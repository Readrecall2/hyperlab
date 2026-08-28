from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import importlib
import ipaddress
import json
import os
import platform
import re
import shutil
import socket
import ssl
import stat
import subprocess
import sys
import sysconfig
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn
from uuid import uuid4

BOUNDARY = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
_MAX_JSON_BYTES = 8 * 1024 * 1024
_NETWORK_TIMEOUT_SECONDS = 3.0
_COMMAND_TIMEOUT_SECONDS = 20.0
_SHA256 = frozenset("0123456789abcdef")
_RUN_SLUG = re.compile(r"^pm-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}$")
_HOME_MOUNT = PurePosixPath("/home")
_INCOMING_PARENT = PurePosixPath("/home/hyperlab/hyperlab-prediction-markets/incoming")
_VOLUME_BASE = PurePosixPath(
    "/mnt/HC_Volume_106716684/hyperlab-prediction-markets"
)
_DISK_RESERVATION = {
    "h1_reserved_bytes": 154_618_822_656,
    "prediction_maximum_raw_bytes": 22_548_578_304,
    "required_free_bytes": 194_347_270_144,
    "safety_margin_bytes": 17_179_869_184,
}
_RUNTIME_SOURCE_MODULES = (
    "hyperlab",
    "ops.prediction_markets_launch_v1.cockpit",
    "ops.prediction_markets_launch_v1.preflight",
    "ops.prediction_markets_launch_v1.runner",
)
_RUNTIME_SOURCE_RELATIVE_FILES = {
    "hyperlab": Path("src/hyperlab/__init__.py"),
    "ops.prediction_markets_launch_v1.cockpit": Path(
        "ops/prediction_markets_launch_v1/cockpit.py"
    ),
    "ops.prediction_markets_launch_v1.preflight": Path(
        "ops/prediction_markets_launch_v1/preflight.py"
    ),
    "ops.prediction_markets_launch_v1.runner": Path(
        "ops/prediction_markets_launch_v1/runner.py"
    ),
}
_RUNTIME_ADMISSION_SCRIPT_RELATIVE = Path(
    "ops/prediction_markets_launch_v1/preflight.py"
)
_RUNTIME_VENV_MODULES = ("fastapi", "requests", "uvicorn", "websocket")
_RUNTIME_HELPERS = {
    "ops.prediction_markets_launch_v1.cockpit": (
        "_validate_venue_state",
        "active_optional_service_is_admissible",
        "classify_monitored_service",
        "complete_service_is_admissible",
        "prepared_state_is_stale",
        "validate_activation_evidence",
    ),
    "ops.prediction_markets_launch_v1.runner": (
        "read_ledger",
        "validate_service_ledger_against_manifest",
    ),
}
_SUPERSEDED_RUNTIME_ADAPTER_ID = "prediction-markets-bcb5280f-runtime-v1"
_SUPERSEDED_RUNTIME_COMMIT = "bcb5280f87393992e2aa4528188009186cd8bdc3"
_SUPERSEDED_RUNTIME_INVENTORY_SHA256 = (
    "573db1e313459d8b153cc6790fd733bd790898eeacaaade4125f48c28a3edf53"
)
_SUPERSEDED_RUNTIME_SLUG = "pm-20260828t024827z-bcb5280f"
_SUPERSEDED_RUNTIME_SOURCE_MODULES = (
    "hyperlab",
    "ops.prediction_markets_launch_v1.cockpit",
    "ops.prediction_markets_launch_v1.preflight",
    "ops.prediction_markets_launch_v1.runner",
)
_SUPERSEDED_RUNTIME_SOURCE_RELATIVE_FILES = dict(_RUNTIME_SOURCE_RELATIVE_FILES)
_SUPERSEDED_RUNTIME_VENV_MODULES = _RUNTIME_VENV_MODULES
_SUPERSEDED_RUNTIME_HELPERS = dict(_RUNTIME_HELPERS)


class PreflightError(RuntimeError):
    """Fail-closed target preflight error."""


class _MountEvidenceError(PreflightError):
    """A mount refusal carrying only bounded, non-secret kernel evidence."""

    def __init__(self, message: str, evidence: Mapping[str, object]) -> None:
        self.evidence = dict(evidence)
        diagnostic = canonical_json_bytes(self.evidence).decode("utf-8")
        super().__init__(f"{message}; observed_mount={diagnostic}")


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _utc_now_text() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _safe_regular_bytes(path: Path, *, maximum_bytes: int = _MAX_JSON_BYTES) -> bytes:
    try:
        before = path.lstat()
    except OSError as error:
        raise PreflightError(f"required file is unreadable: {path}") from error
    if path.is_symlink() or not path.is_file() or before.st_size > maximum_bytes:
        raise PreflightError(f"required file is unsafe: {path}")
    raw = path.read_bytes()
    after = path.lstat()
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise PreflightError(f"file changed while it was authenticated: {path}")
    if len(raw) != before.st_size:
        raise PreflightError(f"short read: {path}")
    return raw


def _object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(_safe_regular_bytes(path).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreflightError(f"invalid JSON object: {path}") from error
    if not isinstance(value, dict):
        raise PreflightError(f"JSON root must be an object: {path}")
    return value


def _validate_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in _SHA256 for char in value):
        raise PreflightError(f"{label} is not a lowercase SHA-256")
    return value


def load_handoff(path: Path) -> dict[str, Any]:
    raw = _safe_regular_bytes(path)
    try:
        handoff = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreflightError("handoff is invalid JSON") from error
    if not isinstance(handoff, dict) or raw != canonical_json_bytes(handoff) + b"\n":
        raise PreflightError("handoff is not canonical JSON with LF")
    pin_path = path.with_name("handoff.sha256")
    pin = _safe_regular_bytes(pin_path, maximum_bytes=256).decode("ascii").strip().split()
    if len(pin) != 2 or pin[1] != path.name:
        raise PreflightError("handoff pin is malformed")
    if sha256_bytes(raw) != pin[0]:
        raise PreflightError("handoff physical SHA-256 diverged")
    if handoff.get("boundary") != BOUNDARY or handoff.get("schema_version") != 1:
        raise PreflightError("handoff boundary or schema diverged")
    return handoff


def _runtime_exact_directory(path: Path, *, label: str) -> Path:
    if not path.is_absolute():
        raise PreflightError(f"{label} is not absolute")
    try:
        identity = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise PreflightError(f"{label} is absent or unreadable") from error
    if path.is_symlink() or not stat.S_ISDIR(identity.st_mode) or resolved != path:
        raise PreflightError(f"{label} is symlinked, special, or non-canonical")
    return resolved


def _runtime_path_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _runtime_git(source_root: Path, *arguments: str) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(source_root), *arguments],
            capture_output=True,
            check=False,
            text=True,
            timeout=_COMMAND_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise PreflightError("runtime source Git authentication could not execute") from error
    if completed.returncode != 0:
        raise PreflightError(
            completed.stderr.strip() or "runtime source Git authentication failed"
        )
    return completed.stdout.strip()


def _runtime_source_inventory(source_root: Path, commit: str) -> dict[str, object]:
    rows: list[dict[str, object]] = []
    output = _runtime_git(source_root, "ls-tree", "-r", "--long", commit)
    for line in output.splitlines():
        metadata, separator, relative_path = line.partition("\t")
        fields = metadata.split()
        if (
            not separator
            or len(fields) != 4
            or fields[1] != "blob"
            or not fields[3].isdigit()
        ):
            raise PreflightError("runtime source Git inventory line is malformed")
        rows.append(
            {
                "blob_sha1": fields[2],
                "mode": fields[0],
                "path": relative_path,
                "size": int(fields[3]),
            }
        )
    if not rows:
        raise PreflightError("runtime source Git inventory is empty")
    body: dict[str, object] = {
        "boundary": BOUNDARY,
        "commit": commit,
        "files": rows,
        "schema_version": 1,
    }
    return {**body, "inventory_sha256": sha256_bytes(canonical_json_bytes(body))}


def _runtime_module_file(module: object, *, name: str) -> Path:
    value = getattr(module, "__file__", None)
    if not isinstance(value, str) or not value:
        raise PreflightError(f"runtime module has no concrete __file__: {name}")
    path = Path(value)
    if not path.is_absolute():
        raise PreflightError(f"runtime module __file__ is not absolute: {name}")
    try:
        identity = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise PreflightError(f"runtime module __file__ is unreadable: {name}") from error
    if path.is_symlink() or not stat.S_ISREG(identity.st_mode) or resolved != path:
        raise PreflightError(f"runtime module __file__ is symlinked or non-canonical: {name}")
    return resolved


def _runtime_reported_file(value: object, *, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise PreflightError(f"{label} is absent")
    path = Path(value)
    if not path.is_absolute():
        raise PreflightError(f"{label} is not absolute")
    try:
        identity = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise PreflightError(f"{label} is unreadable") from error
    if path.is_symlink() or not stat.S_ISREG(identity.st_mode) or resolved != path:
        raise PreflightError(f"{label} is symlinked, special, or non-canonical")
    return resolved


def _runtime_module_class(
    path: Path,
    *,
    source_root: Path,
    venv_root: Path,
    stdlib_roots: Sequence[Path],
    name: str | None = None,
) -> str:
    identity = f"{name}:{path}" if name is not None else str(path)
    if _runtime_path_within(path, venv_root):
        return "venv"
    if _runtime_path_within(path, source_root):
        return "source"
    if "site-packages" in path.parts or "dist-packages" in path.parts:
        raise PreflightError(f"runtime module escaped the venv site-packages: {identity}")
    if any(_runtime_path_within(path, root) for root in stdlib_roots):
        return "stdlib"
    raise PreflightError(
        f"runtime module escaped source, venv, and stdlib roots: {identity}"
    )


def _runtime_admission_script_identity(
    executed_path: Path,
    *,
    source_root: Path,
    source_inventory: Mapping[str, object],
) -> dict[str, object]:
    expected = source_root / _RUNTIME_ADMISSION_SCRIPT_RELATIVE
    executed = _runtime_reported_file(
        str(executed_path), label="runtime admission script"
    )
    if executed != expected:
        raise PreflightError(
            "runtime admission script escaped the authenticated source root"
        )
    rows = source_inventory.get("files")
    if not isinstance(rows, list):
        raise PreflightError("runtime source inventory file list is malformed")
    matches = [
        row
        for row in rows
        if isinstance(row, Mapping)
        and row.get("path") == _RUNTIME_ADMISSION_SCRIPT_RELATIVE.as_posix()
    ]
    if len(matches) != 1:
        raise PreflightError("runtime admission script is not uniquely inventoried")
    row = matches[0]
    blob_sha1 = row.get("blob_sha1")
    mode = row.get("mode")
    size = row.get("size")
    if (
        set(row) != {"blob_sha1", "mode", "path", "size"}
        or not isinstance(blob_sha1, str)
        or len(blob_sha1) != 40
        or any(character not in _SHA256 for character in blob_sha1)
        or mode not in {"100644", "100755"}
        or type(size) is not int
        or size < 1
    ):
        raise PreflightError("runtime admission script inventory row is malformed")
    raw = _safe_regular_bytes(executed)
    git_blob_sha1 = hashlib.sha1(
        f"blob {len(raw)}\0".encode("ascii") + raw
    ).hexdigest()
    if len(raw) != size or git_blob_sha1 != blob_sha1:
        raise PreflightError("runtime admission script diverged from its Git blob")
    return {
        "class": "source",
        "file": str(executed),
        "git_blob_sha1": git_blob_sha1,
        "relative_path": _RUNTIME_ADMISSION_SCRIPT_RELATIVE.as_posix(),
        "sha256": sha256_bytes(raw),
        "size": len(raw),
    }


def validate_runtime_import_admission(
    report: Mapping[str, object],
    *,
    source_root: Path,
    source_commit: str,
    inventory_sha256: str,
) -> None:
    expected_fields = {
        "admission_script",
        "admission_sha256",
        "boundary",
        "inventory_sha256",
        "isolated",
        "loaded_module_files_validated",
        "modules",
        "no_user_site",
        "python_executable",
        "schema_version",
        "source_commit",
        "source_root",
        "terminal_signal",
        "venv_root",
    }
    if set(report) != expected_fields:
        raise PreflightError("runtime import admission fields diverged")
    claimed = _validate_sha256(
        report.get("admission_sha256"), label="runtime import admission hash"
    )
    body = {key: value for key, value in report.items() if key != "admission_sha256"}
    modules = report.get("modules")
    admission_script = report.get("admission_script")
    expected_modules = set(_RUNTIME_SOURCE_MODULES) | set(_RUNTIME_VENV_MODULES)
    loaded_module_files = report.get("loaded_module_files_validated")
    if type(loaded_module_files) is not int:
        raise PreflightError("runtime import admission file count diverged")
    assert isinstance(loaded_module_files, int)
    if (
        sha256_bytes(canonical_json_bytes(body)) != claimed
        or report.get("boundary") != BOUNDARY
        or report.get("inventory_sha256") != inventory_sha256
        or report.get("isolated") is not True
        or report.get("no_user_site") is not True
        or loaded_module_files < len(expected_modules)
        or not isinstance(modules, Mapping)
        or set(modules) != expected_modules
        or not isinstance(admission_script, Mapping)
        or report.get("schema_version") != 1
        or report.get("source_commit") != source_commit
        or report.get("source_root") != str(source_root)
        or report.get("terminal_signal")
        != "PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN"
        or report.get("venv_root") != str(source_root / ".venv")
    ):
        raise PreflightError("runtime import admission binding diverged")
    expected_admission_script = source_root / _RUNTIME_ADMISSION_SCRIPT_RELATIVE
    if (
        set(admission_script)
        != {"class", "file", "git_blob_sha1", "relative_path", "sha256", "size"}
        or admission_script.get("class") != "source"
        or admission_script.get("file") != str(expected_admission_script)
        or admission_script.get("relative_path")
        != _RUNTIME_ADMISSION_SCRIPT_RELATIVE.as_posix()
        or not isinstance(admission_script.get("git_blob_sha1"), str)
        or len(str(admission_script.get("git_blob_sha1"))) != 40
        or any(
            character not in _SHA256
            for character in str(admission_script.get("git_blob_sha1"))
        )
        or not isinstance(admission_script.get("sha256"), str)
        or len(str(admission_script.get("sha256"))) != 64
        or any(
            character not in _SHA256
            for character in str(admission_script.get("sha256"))
        )
        or type(admission_script.get("size")) is not int
        or int(admission_script.get("size", 0)) < 1
    ):
        raise PreflightError("runtime admission script binding diverged")
    authenticated_admission_script = _runtime_reported_file(
        str(expected_admission_script), label="runtime admission script file"
    )
    if authenticated_admission_script != expected_admission_script:
        raise PreflightError("runtime admission script canonical path diverged")
    admission_raw = _safe_regular_bytes(authenticated_admission_script)
    admission_blob_sha1 = hashlib.sha1(
        f"blob {len(admission_raw)}\0".encode("ascii") + admission_raw
    ).hexdigest()
    if (
        admission_script.get("git_blob_sha1") != admission_blob_sha1
        or admission_script.get("sha256") != sha256_bytes(admission_raw)
        or admission_script.get("size") != len(admission_raw)
    ):
        raise PreflightError("runtime admission script content binding diverged")
    venv_root = source_root / ".venv"
    executable = _runtime_reported_file(
        report.get("python_executable"), label="runtime Python executable"
    )
    if not _runtime_path_within(executable, venv_root):
        raise PreflightError("runtime Python executable escaped the admitted venv")
    for name, expected_class in (
        *((name, "source") for name in _RUNTIME_SOURCE_MODULES),
        *((name, "venv") for name in _RUNTIME_VENV_MODULES),
    ):
        row = modules.get(name)
        if (
            not isinstance(row, Mapping)
            or set(row) != {"class", "file"}
            or row.get("class") != expected_class
            or not isinstance(row.get("file"), str)
            or not row.get("file")
        ):
            raise PreflightError(f"runtime import module binding diverged: {name}")
        module_path = _runtime_reported_file(
            row.get("file"), label=f"runtime import module file: {name}"
        )
        if expected_class == "source":
            expected_path = (source_root / _RUNTIME_SOURCE_RELATIVE_FILES[name]).resolve(
                strict=True
            )
            if module_path != expected_path:
                raise PreflightError(f"runtime source module path diverged: {name}")
        elif (
            not _runtime_path_within(module_path, venv_root)
            or "site-packages" not in module_path.parts
        ):
            raise PreflightError(f"runtime dependency escaped venv site-packages: {name}")


def runtime_import_admission(
    handoff_path: Path,
    source_root: Path,
    source_inventory_path: Path,
) -> dict[str, object]:
    """Authenticate an isolated venv and every runtime module origin."""

    handoff = load_handoff(handoff_path)
    slug = handoff.get("run_slug")
    if not isinstance(slug, str) or _RUN_SLUG.fullmatch(slug) is None:
        raise PreflightError("runtime import admission run slug is invalid")
    incoming_root = handoff_path.parent
    volume_mount = Path(str(handoff.get("volume_mount") or ""))
    volume_base = Path(str(handoff.get("volume_base") or ""))
    campaign_root = Path(str(handoff.get("campaign_root") or ""))
    expected_source = volume_base / "sources" / slug
    expected_campaign = volume_base / "campaigns" / slug
    expected_inventory = incoming_root / "source-inventory.json"
    if (
        not handoff_path.is_absolute()
        or handoff_path != incoming_root / "handoff.json"
        or Path(str(handoff.get("incoming_root") or "")) != incoming_root
        or Path(str(handoff.get("source_root") or "")) != source_root
        or source_root != expected_source
        or campaign_root != expected_campaign
        or source_inventory_path != expected_inventory
    ):
        raise PreflightError("runtime import admission path binding diverged")
    source_root = _runtime_exact_directory(source_root, label="runtime source root")
    source_directory = _runtime_exact_directory(
        source_root / "src", label="runtime source package root"
    )
    _runtime_exact_directory(volume_mount, label="runtime volume mount")
    _runtime_exact_directory(volume_base, label="runtime volume base")
    devices = {
        source_root.stat().st_dev,
        source_root.parent.stat().st_dev,
        volume_base.stat().st_dev,
        volume_mount.stat().st_dev,
    }
    if len(devices) != 1:
        raise PreflightError("runtime source root is on the wrong device")

    source_commit = handoff.get("source_commit")
    if (
        not isinstance(source_commit, str)
        or len(source_commit) != 40
        or any(character not in _SHA256 for character in source_commit)
    ):
        raise PreflightError("runtime source commit is invalid")
    inventory_sha256 = _validate_sha256(
        handoff.get("source_inventory_sha256"),
        label="runtime source inventory hash",
    )
    inventory_raw = _safe_regular_bytes(source_inventory_path)
    try:
        expected_inventory_value = json.loads(inventory_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreflightError("runtime source inventory is invalid JSON") from error
    if (
        not isinstance(expected_inventory_value, dict)
        or inventory_raw != canonical_json_bytes(expected_inventory_value) + b"\n"
        or expected_inventory_value.get("inventory_sha256") != inventory_sha256
    ):
        raise PreflightError("runtime source inventory authentication failed")
    if _runtime_git(source_root, "rev-parse", "HEAD") != source_commit:
        raise PreflightError("runtime detached source commit diverged")
    git_top_level = Path(
        _runtime_git(source_root, "rev-parse", "--show-toplevel")
    ).resolve(strict=True)
    if git_top_level != source_root:
        raise PreflightError("runtime source Git top-level diverged")
    if _runtime_git(source_root, "status", "--porcelain"):
        raise PreflightError("runtime source checkout is not clean")
    actual_inventory = _runtime_source_inventory(source_root, source_commit)
    if expected_inventory_value != actual_inventory:
        raise PreflightError("runtime source Git inventory diverged")
    admission_script = _runtime_admission_script_identity(
        Path(__file__),
        source_root=source_root,
        source_inventory=actual_inventory,
    )

    venv_root = _runtime_exact_directory(
        source_root / ".venv", label="runtime virtual environment"
    )
    executable = Path(sys.executable).resolve(strict=True)
    prefix = Path(sys.prefix).resolve(strict=True)
    if (
        not _runtime_path_within(executable, venv_root)
        or prefix != venv_root
        or sys.base_prefix == sys.prefix
        or sys.flags.isolated != 1
        or sys.flags.no_user_site != 1
        or os.environ.get("PYTHONNOUSERSITE") != "1"
    ):
        raise PreflightError("runtime Python is not the expected isolated venv")
    try:
        pyvenv = (venv_root / "pyvenv.cfg").read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise PreflightError("runtime venv configuration is unreadable") from error
    normalized_pyvenv = {
        key.strip().lower(): value.strip().lower()
        for line in pyvenv.splitlines()
        if "=" in line
        for key, value in (line.split("=", 1),)
    }
    if normalized_pyvenv.get("include-system-site-packages") != "false":
        raise PreflightError("runtime venv exposes system site-packages")
    import site

    if site.ENABLE_USER_SITE is not False:
        raise PreflightError("runtime user-site is enabled")
    cwd = Path.cwd().resolve(strict=True)
    for raw_path in sys.path:
        if raw_path in {"", "."}:
            raise PreflightError("runtime isolated sys.path contains an implicit cwd")
        candidate = Path(raw_path).resolve(strict=False)
        if candidate == cwd or (
            _runtime_path_within(candidate, source_root)
            and not _runtime_path_within(candidate, venv_root)
        ):
            raise PreflightError("runtime source or cwd was present before explicit admission")
        if (
            ("site-packages" in candidate.parts or "dist-packages" in candidate.parts)
            and not _runtime_path_within(candidate, venv_root)
        ):
            raise PreflightError("runtime isolated sys.path exposes global site-packages")

    stdlib_roots = tuple(
        {
            Path(value).resolve(strict=True)
            for key in ("stdlib", "platstdlib")
            if (value := sysconfig.get_path(key))
        }
    )
    before_modules = set(sys.modules)
    sys.dont_write_bytecode = True
    sys.path[:0] = [str(source_directory), str(source_root)]
    importlib.invalidate_caches()
    imported: dict[str, object] = {}
    try:
        for name in (*_RUNTIME_VENV_MODULES, *_RUNTIME_SOURCE_MODULES):
            imported[name] = importlib.import_module(name)
    except Exception as error:
        raise PreflightError(
            f"runtime required import failed: {type(error).__name__}:{error}"
        ) from error
    for module_name, helpers in _RUNTIME_HELPERS.items():
        module = imported[module_name]
        if not all(callable(getattr(module, helper, None)) for helper in helpers):
            raise PreflightError(f"runtime helper contract diverged: {module_name}")

    module_records: dict[str, object] = {}
    for name, module in imported.items():
        path = _runtime_module_file(module, name=name)
        actual_class = _runtime_module_class(
            path,
            source_root=source_root,
            venv_root=venv_root,
            stdlib_roots=stdlib_roots,
            name=name,
        )
        expected_class = "source" if name in _RUNTIME_SOURCE_MODULES else "venv"
        if actual_class != expected_class:
            raise PreflightError(f"runtime module origin class diverged: {name}")
        module_records[name] = {"class": actual_class, "file": str(path)}

    validated_files = 0
    for name in sorted(set(sys.modules) - before_modules):
        module = sys.modules.get(name)
        module_file_value = getattr(module, "__file__", None)
        if not isinstance(module_file_value, str) or not module_file_value:
            continue
        path = _runtime_module_file(module, name=name)
        _runtime_module_class(
            path,
            source_root=source_root,
            venv_root=venv_root,
            stdlib_roots=stdlib_roots,
            name=name,
        )
        validated_files += 1
    body: dict[str, object] = {
        "admission_script": admission_script,
        "boundary": BOUNDARY,
        "inventory_sha256": inventory_sha256,
        "isolated": True,
        "loaded_module_files_validated": validated_files,
        "modules": module_records,
        "no_user_site": True,
        "python_executable": str(executable),
        "schema_version": 1,
        "source_commit": source_commit,
        "source_root": str(source_root),
        "terminal_signal": "PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN",
        "venv_root": str(venv_root),
    }
    report = {**body, "admission_sha256": sha256_bytes(canonical_json_bytes(body))}
    validate_runtime_import_admission(
        report,
        source_root=source_root,
        source_commit=source_commit,
        inventory_sha256=inventory_sha256,
    )
    return report


def _runtime_import_subprocess_admission(
    *,
    runtime: Path,
    incoming_root: Path,
    source_root: Path,
    source_commit: str,
    inventory_sha256: str,
    run: CommandRunner,
) -> dict[str, object]:
    completed = run(
        [
            str(runtime),
            "-I",
            str(source_root / _RUNTIME_ADMISSION_SCRIPT_RELATIVE),
            "runtime-import-admission",
            "--handoff",
            str(incoming_root / "handoff.json"),
            "--source-root",
            str(source_root),
            "--source-inventory",
            str(incoming_root / "source-inventory.json"),
        ]
    )
    if completed.returncode != 0:
        raise PreflightError(
            f"isolated runtime import admission failed: {completed.stderr}"
        )
    try:
        report = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise PreflightError("isolated runtime import admission output is invalid") from error
    if not isinstance(report, dict):
        raise PreflightError("isolated runtime import admission output is not an object")
    validate_runtime_import_admission(
        report,
        source_root=source_root,
        source_commit=source_commit,
        inventory_sha256=inventory_sha256,
    )
    return report


def validate_install_layout(
    handoff: Mapping[str, object],
    *,
    handoff_path: Path,
    trusted_source_root: Path | None = None,
) -> dict[str, object]:
    """Bind every mutable operator path/name to the generated attempt slug."""

    slug = handoff.get("run_slug")
    if not isinstance(slug, str) or _RUN_SLUG.fullmatch(slug) is None:
        raise PreflightError("install run slug is invalid")
    volume_base = PurePosixPath(str(handoff.get("volume_base") or ""))
    volume_mount = PurePosixPath(str(handoff.get("volume_mount") or ""))
    incoming = PurePosixPath(str(handoff.get("incoming_root") or ""))
    source = PurePosixPath(str(handoff.get("source_root") or ""))
    campaign = PurePosixPath(str(handoff.get("campaign_root") or ""))
    expected_incoming = _INCOMING_PARENT / slug
    expected_source = _VOLUME_BASE / "sources" / slug
    expected_campaign = _VOLUME_BASE / "campaigns" / slug
    suffix = slug.removeprefix("pm-")
    expected_services = {
        venue: f"hyperlab-pm-{suffix}-{venue}.service"
        for venue in ("polymarket", "kalshi", "dashboard")
    }
    namespace_probe_services = {
        venue: f"hyperlab-pm-{suffix}-{venue}-namespace-probe.service"
        for venue in ("polymarket", "kalshi")
    }
    services = handoff.get("services")
    disk = handoff.get("disk")
    source_commit = handoff.get("source_commit")
    if (
        volume_base != _VOLUME_BASE
        or volume_mount != _VOLUME_BASE.parent
        or incoming != expected_incoming
        or source != expected_source
        or campaign != expected_campaign
        or PurePosixPath(handoff_path.as_posix()) != expected_incoming / "handoff.json"
        or handoff.get("service_user") != "hyperlab"
        or handoff.get("dashboard_port") != 18081
        or services != expected_services
        or disk != _DISK_RESERVATION
        or not isinstance(source_commit, str)
        or len(source_commit) != 40
        or any(character not in _SHA256 for character in source_commit)
    ):
        raise PreflightError("install handoff path, service, disk, or source identity diverged")
    if trusted_source_root is not None and trusted_source_root.as_posix() != source.as_posix():
        raise PreflightError("install script source root diverged from authenticated handoff")
    return {
        "campaign_root": campaign.as_posix(),
        "incoming_root": incoming.as_posix(),
        "run_slug": slug,
        "namespace_probe_services": namespace_probe_services,
        "services": expected_services,
        "source_commit": source_commit,
        "source_root": source.as_posix(),
        "volume_base": volume_base.as_posix(),
        "volume_mount": volume_mount.as_posix(),
    }


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[Sequence[str]], CommandResult]
WriteSurfaceProbe = Callable[[Path], dict[str, object]]


def _command(arguments: Sequence[str]) -> CommandResult:
    completed = subprocess.run(
        list(arguments),
        check=False,
        capture_output=True,
        text=True,
        timeout=_COMMAND_TIMEOUT_SECONDS,
    )
    return CommandResult(completed.returncode, completed.stdout.strip(), completed.stderr.strip())


def _authenticated_runtime_checkout(
    *,
    source_root: Path,
    inventory_path: Path,
    expected_commit: str,
    expected_inventory_sha256: str,
    label: str,
) -> dict[str, object]:
    source_root = _runtime_exact_directory(source_root, label=f"{label} source root")
    _runtime_exact_directory(source_root / "src", label=f"{label} source package root")
    if inventory_path != inventory_path.parent / "source-inventory.json":
        raise PreflightError(f"{label} source inventory path diverged")
    inventory_raw = _safe_regular_bytes(inventory_path)
    try:
        inventory = json.loads(inventory_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreflightError(f"{label} source inventory is invalid JSON") from error
    if (
        not isinstance(inventory, dict)
        or inventory_raw != canonical_json_bytes(inventory) + b"\n"
        or inventory.get("commit") != expected_commit
        or inventory.get("inventory_sha256") != expected_inventory_sha256
    ):
        raise PreflightError(f"{label} source inventory binding diverged")
    if _runtime_git(source_root, "rev-parse", "HEAD") != expected_commit:
        raise PreflightError(f"{label} source commit diverged")
    top_level = Path(
        _runtime_git(source_root, "rev-parse", "--show-toplevel")
    ).resolve(strict=True)
    if top_level != source_root:
        raise PreflightError(f"{label} source Git top-level diverged")
    if _runtime_git(source_root, "status", "--porcelain", "--untracked-files=all"):
        raise PreflightError(f"{label} source checkout is not clean")
    actual = _runtime_source_inventory(source_root, expected_commit)
    if inventory != actual:
        raise PreflightError(f"{label} source Git inventory diverged")
    return actual


def _inventoried_source_file(
    path: Path,
    *,
    source_root: Path,
    inventory: Mapping[str, object],
    relative_path: Path,
    label: str,
) -> dict[str, object]:
    expected = source_root / relative_path
    authenticated = _runtime_reported_file(str(path), label=label)
    if authenticated != expected:
        raise PreflightError(f"{label} escaped its authenticated source root")
    rows = inventory.get("files")
    if not isinstance(rows, list):
        raise PreflightError(f"{label} inventory file list is malformed")
    matches = [
        row
        for row in rows
        if isinstance(row, Mapping) and row.get("path") == relative_path.as_posix()
    ]
    if len(matches) != 1:
        raise PreflightError(f"{label} is not uniquely inventoried")
    row = matches[0]
    if set(row) != {"blob_sha1", "mode", "path", "size"}:
        raise PreflightError(f"{label} inventory row is malformed")
    blob_sha1 = row.get("blob_sha1")
    size = row.get("size")
    if (
        row.get("mode") not in {"100644", "100755"}
        or not isinstance(blob_sha1, str)
        or re.fullmatch(r"[0-9a-f]{40}", blob_sha1) is None
        or type(size) is not int
        or size < 1
    ):
        raise PreflightError(f"{label} inventory identity is malformed")
    raw = _safe_regular_bytes(authenticated)
    actual_blob = hashlib.sha1(
        f"blob {len(raw)}\0".encode("ascii") + raw
    ).hexdigest()
    if len(raw) != size or actual_blob != blob_sha1:
        raise PreflightError(f"{label} diverged from its Git blob")
    return {
        "class": "source",
        "file": str(authenticated),
        "git_blob_sha1": actual_blob,
        "relative_path": relative_path.as_posix(),
        "sha256": sha256_bytes(raw),
        "size": len(raw),
    }


def _superseded_contract(candidate_handoff: Mapping[str, object]) -> dict[str, object]:
    value = candidate_handoff.get("superseded_campaign")
    if not isinstance(value, dict):
        raise PreflightError("superseded runtime contract is absent")
    volume_base = "/mnt/HC_Volume_106716684/hyperlab-prediction-markets"
    suffix = _SUPERSEDED_RUNTIME_SLUG.removeprefix("pm-")
    expected: dict[str, object] = {
        "campaign_root": f"{volume_base}/campaigns/{_SUPERSEDED_RUNTIME_SLUG}",
        "dashboard_port": 18081,
        "incoming_root": (
            "/home/hyperlab/hyperlab-prediction-markets/incoming/"
            f"{_SUPERSEDED_RUNTIME_SLUG}"
        ),
        "namespace_probe_services": {
            venue: f"hyperlab-pm-{suffix}-{venue}-namespace-probe.service"
            for venue in ("polymarket", "kalshi")
        },
        "run_slug": _SUPERSEDED_RUNTIME_SLUG,
        "services": {
            venue: f"hyperlab-pm-{suffix}-{venue}.service"
            for venue in ("polymarket", "kalshi", "dashboard")
        },
        "source_commit": _SUPERSEDED_RUNTIME_COMMIT,
        "source_root": f"{volume_base}/sources/{_SUPERSEDED_RUNTIME_SLUG}",
    }
    if value != expected:
        raise PreflightError("superseded runtime contract is unknown or divergent")
    return expected


def _superseded_runtime_environment(
    *,
    target_source: Path,
) -> tuple[Path, Path, tuple[Path, ...]]:
    venv_root = _runtime_exact_directory(
        target_source / ".venv", label="superseded runtime virtual environment"
    )
    expected_python = target_source / ".venv" / "bin" / "python"
    executable = _runtime_reported_file(
        str(Path(sys.executable)), label="superseded runtime Python executable"
    )
    prefix = Path(sys.prefix).resolve(strict=True)
    if (
        sys.platform != "linux"
        or platform.machine() != "x86_64"
        or sys.version_info[:2] != (3, 12)
        or executable != expected_python
        or prefix != venv_root
        or sys.base_prefix == sys.prefix
        or sys.flags.isolated != 1
        or sys.flags.no_user_site != 1
        or os.environ.get("PYTHONNOUSERSITE") != "1"
    ):
        raise PreflightError("superseded runtime Python isolation diverged")
    try:
        pyvenv = _safe_regular_bytes(
            venv_root / "pyvenv.cfg", maximum_bytes=64 * 1024
        ).decode("utf-8")
    except UnicodeDecodeError as error:
        raise PreflightError("superseded runtime venv configuration is invalid") from error
    normalized = {
        key.strip().lower(): value.strip().lower()
        for line in pyvenv.splitlines()
        if "=" in line
        for key, value in (line.split("=", 1),)
    }
    if normalized.get("include-system-site-packages") != "false":
        raise PreflightError("superseded runtime venv exposes system site-packages")
    import site

    if site.ENABLE_USER_SITE is not False:
        raise PreflightError("superseded runtime user-site is enabled")
    stdlib_roots = tuple(
        {
            Path(value).resolve(strict=True)
            for key in ("stdlib", "platstdlib")
            if (value := sysconfig.get_path(key))
        }
    )
    return venv_root, executable, stdlib_roots


def _superseded_candidate_tool_alias_record(
    *,
    name: str,
    module: object,
    candidate_source_root: Path,
    candidate_inventory: Mapping[str, object],
    candidate_tool: Mapping[str, object],
) -> dict[str, object] | None:
    if name != "__mp_main__":
        return None
    main_module = sys.modules.get("__main__")
    if module is not main_module:
        raise PreflightError(
            "superseded candidate tool alias does not reference __main__"
        )
    path = _runtime_module_file(module, name=name)
    expected = _runtime_reported_file(
        candidate_tool.get("file"), label="authenticated candidate compatibility tool"
    )
    if path != expected:
        raise PreflightError("superseded candidate tool alias file diverged")
    refreshed = _inventoried_source_file(
        path,
        source_root=candidate_source_root,
        inventory=candidate_inventory,
        relative_path=_RUNTIME_ADMISSION_SCRIPT_RELATIVE,
        label="superseded candidate compatibility tool alias",
    )
    refreshed["class"] = "candidate_tool"
    if refreshed != candidate_tool:
        raise PreflightError("superseded candidate tool changed after authentication")
    return {**refreshed, "alias_of": "__main__"}


def superseded_runtime_compatibility(
    candidate_handoff_path: Path,
    candidate_source_root: Path,
    candidate_inventory_path: Path,
) -> dict[str, object]:
    """Verify one versioned historical runtime without calling its newer CLI."""

    candidate_handoff = load_handoff(candidate_handoff_path)
    candidate_layout = validate_install_layout(
        candidate_handoff,
        handoff_path=candidate_handoff_path,
        trusted_source_root=candidate_source_root,
    )
    candidate_commit = str(candidate_layout["source_commit"])
    if candidate_commit == _SUPERSEDED_RUNTIME_COMMIT:
        raise PreflightError("candidate and superseded runtime commits must differ")
    expected_candidate_inventory = _validate_sha256(
        candidate_handoff.get("source_inventory_sha256"),
        label="candidate source inventory hash",
    )
    if candidate_inventory_path != candidate_handoff_path.parent / "source-inventory.json":
        raise PreflightError("candidate source inventory path diverged")
    candidate_inventory = _authenticated_runtime_checkout(
        source_root=candidate_source_root,
        inventory_path=candidate_inventory_path,
        expected_commit=candidate_commit,
        expected_inventory_sha256=expected_candidate_inventory,
        label="candidate tool",
    )
    candidate_tool = _inventoried_source_file(
        Path(__file__),
        source_root=candidate_source_root,
        inventory=candidate_inventory,
        relative_path=_RUNTIME_ADMISSION_SCRIPT_RELATIVE,
        label="candidate compatibility tool",
    )
    candidate_tool["class"] = "candidate_tool"

    target_contract = _superseded_contract(candidate_handoff)
    target_incoming = _runtime_exact_directory(
        Path(str(target_contract["incoming_root"])),
        label="superseded incoming root",
    )
    target_source = _runtime_exact_directory(
        Path(str(target_contract["source_root"])),
        label="superseded source root",
    )
    target_campaign = _runtime_exact_directory(
        Path(str(target_contract["campaign_root"])),
        label="superseded campaign root",
    )
    if target_source == candidate_source_root:
        raise PreflightError("candidate tool and superseded target source roots collide")
    target_handoff_path = target_incoming / "handoff.json"
    target_handoff = load_handoff(target_handoff_path)
    for field in (
        "campaign_root",
        "dashboard_port",
        "incoming_root",
        "run_slug",
        "services",
        "source_commit",
        "source_root",
    ):
        if target_handoff.get(field) != target_contract.get(field):
            raise PreflightError(f"superseded target handoff field diverged: {field}")
    target_inventory_path = target_incoming / "source-inventory.json"
    if (
        target_handoff.get("source_inventory_sha256")
        != _SUPERSEDED_RUNTIME_INVENTORY_SHA256
    ):
        raise PreflightError("superseded target inventory adapter is unsupported")
    target_inventory = _authenticated_runtime_checkout(
        source_root=target_source,
        inventory_path=target_inventory_path,
        expected_commit=_SUPERSEDED_RUNTIME_COMMIT,
        expected_inventory_sha256=_SUPERSEDED_RUNTIME_INVENTORY_SHA256,
        label="superseded target",
    )
    for name, relative_path in _SUPERSEDED_RUNTIME_SOURCE_RELATIVE_FILES.items():
        _inventoried_source_file(
            target_source / relative_path,
            source_root=target_source,
            inventory=target_inventory,
            relative_path=relative_path,
            label=f"superseded runtime source module: {name}",
        )

    venv_root, executable, stdlib_roots = _superseded_runtime_environment(
        target_source=target_source
    )
    cwd = Path.cwd().resolve(strict=True)
    forbidden_roots = (candidate_source_root, target_source, target_incoming)
    for raw_path in sys.path:
        if raw_path in {"", "."}:
            raise PreflightError("superseded isolated sys.path contains an implicit cwd")
        path = Path(raw_path).resolve(strict=False)
        if path == cwd or any(
            _runtime_path_within(path, root)
            and not _runtime_path_within(path, venv_root)
            for root in forbidden_roots
        ):
            raise PreflightError("superseded source, candidate source, or cwd was preloaded")
        if (
            ("site-packages" in path.parts or "dist-packages" in path.parts)
            and not _runtime_path_within(path, venv_root)
        ):
            raise PreflightError("superseded runtime exposes global site-packages")
    if any(name in sys.modules for name in _SUPERSEDED_RUNTIME_SOURCE_MODULES):
        raise PreflightError("superseded source modules were imported before admission")

    before_modules = set(sys.modules)
    if "__mp_main__" in before_modules:
        raise PreflightError("superseded candidate tool alias was preloaded")
    sys.dont_write_bytecode = True
    sys.path[:0] = [str(target_source / "src"), str(target_source)]
    importlib.invalidate_caches()
    imported: dict[str, object] = {}
    try:
        for name in (
            *_SUPERSEDED_RUNTIME_VENV_MODULES,
            *_SUPERSEDED_RUNTIME_SOURCE_MODULES,
        ):
            imported[name] = importlib.import_module(name)
    except Exception as error:
        raise PreflightError(
            f"superseded required import failed: {type(error).__name__}:{error}"
        ) from error
    for module_name, helpers in _SUPERSEDED_RUNTIME_HELPERS.items():
        module = imported[module_name]
        if not all(callable(getattr(module, helper, None)) for helper in helpers):
            raise PreflightError(
                f"superseded runtime helper contract diverged: {module_name}"
            )
    if "__mp_main__" not in set(sys.modules) - before_modules:
        raise PreflightError("superseded candidate tool alias was not created")

    module_records: dict[str, object] = {}
    for name, module in imported.items():
        path = _runtime_module_file(module, name=name)
        actual_class = _runtime_module_class(
            path,
            source_root=target_source,
            venv_root=venv_root,
            stdlib_roots=stdlib_roots,
            name=name,
        )
        expected_class = (
            "source" if name in _SUPERSEDED_RUNTIME_SOURCE_MODULES else "venv"
        )
        if actual_class != expected_class:
            raise PreflightError(f"superseded module origin diverged: {name}")
        if expected_class == "source":
            expected_path = target_source / _SUPERSEDED_RUNTIME_SOURCE_RELATIVE_FILES[name]
            if path != expected_path:
                raise PreflightError(f"superseded source module path diverged: {name}")
        elif "site-packages" not in path.parts:
            raise PreflightError(f"superseded dependency escaped site-packages: {name}")
        module_records[name] = {"class": actual_class, "file": str(path)}

    runner: Any = imported["ops.prediction_markets_launch_v1.runner"]
    envelope: Any = importlib.import_module("hyperlab.research_data.envelope")
    venue_type = getattr(envelope, "Venue", None)
    required_runner_helpers = (
        "_validate_result",
        "canonical_json_bytes",
        "load_campaign_context",
        "read_ledger",
        "sha256_bytes",
        "validate_service_ledger_against_manifest",
    )
    if venue_type is None or not all(
        callable(getattr(runner, name, None)) for name in required_runner_helpers
    ):
        raise PreflightError("superseded campaign evidence adapter contract diverged")
    context = runner.load_campaign_context(target_campaign, target_source)
    ledgers: dict[str, object] = {}
    for venue in (venue_type.POLYMARKET, venue_type.KALSHI):
        rows = runner.read_ledger(target_campaign / venue.value / "ledger.jsonl")
        if not rows or rows[0].get("ordinal") != 0:
            raise PreflightError(
                f"superseded target ledger lacks authenticated ordinal 0: {venue.value}"
            )
        runner.validate_service_ledger_against_manifest(
            rows,
            campaign_manifest=context.manifest,
            venue=venue,
        )
        for row in rows:
            terminal_hash = row.get("terminal_result_sha256")
            if terminal_hash is None:
                continue
            scheduled = datetime.fromisoformat(
                str(row["scheduled_start_utc"]).replace("Z", "+00:00")
            )
            run_root = (
                target_campaign
                / venue.value
                / "runs"
                / f"shard-{row['ordinal']:04d}-{scheduled.strftime('%Y%m%dT%H%M%SZ')}"
            )
            result = runner._validate_result(
                run_root,
                context,
                venue,
                ordinal=row["ordinal"],
            )
            if (
                runner.sha256_bytes(runner.canonical_json_bytes(result))
                != terminal_hash
            ):
                raise PreflightError(
                    f"superseded terminal result and ledger diverged: {venue.value}"
                )
        ledgers[venue.value] = {
            "entries": len(rows),
            "last_entry_sha256": rows[-1].get("entry_sha256") if rows else None,
        }

    validated_files = 0
    for name in sorted(set(sys.modules) - before_modules):
        module = sys.modules.get(name)
        value = getattr(module, "__file__", None)
        if not isinstance(value, str) or not value:
            continue
        path = _runtime_module_file(module, name=name)
        alias_record = _superseded_candidate_tool_alias_record(
            name=name,
            module=module,
            candidate_source_root=candidate_source_root,
            candidate_inventory=candidate_inventory,
            candidate_tool=candidate_tool,
        )
        if alias_record is not None:
            module_records[name] = alias_record
            validated_files += 1
            continue
        module_class = _runtime_module_class(
            path,
            source_root=target_source,
            venv_root=venv_root,
            stdlib_roots=stdlib_roots,
            name=name,
        )
        if module_class == "source":
            _inventoried_source_file(
                path,
                source_root=target_source,
                inventory=target_inventory,
                relative_path=path.relative_to(target_source),
                label=f"superseded loaded source module: {name}",
            )
        validated_files += 1

    manifest_path = target_campaign / "campaign-manifest.json"
    activation_path = target_campaign / "state" / "activation-receipt.json"
    body: dict[str, object] = {
        "adapter_id": _SUPERSEDED_RUNTIME_ADAPTER_ID,
        "boundary": BOUNDARY,
        "candidate_commit": candidate_commit,
        "candidate_inventory_sha256": expected_candidate_inventory,
        "candidate_source_root": str(candidate_source_root),
        "candidate_tool": candidate_tool,
        "isolated": True,
        "ledgers": ledgers,
        "loaded_module_files_validated": validated_files,
        "modules": module_records,
        "no_historical_new_cli_invoked": True,
        "no_user_site": True,
        "schema_version": 1,
        "target_activation_receipt_sha256": sha256_bytes(
            _safe_regular_bytes(activation_path)
        ),
        "target_campaign_manifest_sha256": sha256_bytes(
            _safe_regular_bytes(manifest_path)
        ),
        "target_commit": _SUPERSEDED_RUNTIME_COMMIT,
        "target_inventory_sha256": _SUPERSEDED_RUNTIME_INVENTORY_SHA256,
        "target_python_executable": str(executable),
        "target_source_root": str(target_source),
        "target_venv_root": str(venv_root),
        "terminal_signal": (
            "PREDICTION_SUPERSEDED_RUNTIME_COMPATIBILITY_RUNTIME_GREEN"
        ),
    }
    report = {
        **body,
        "compatibility_sha256": sha256_bytes(canonical_json_bytes(body)),
    }
    return report


def _required_command(name: str) -> str:
    value = shutil.which(name)
    if value is None:
        raise PreflightError(f"required offline command is absent: {name}")
    return value


def _stat_device_major_minor(path: Path) -> str:
    """Return the kernel device identity used by stat(2), not a mount source label."""

    try:
        identity = path.stat()
    except OSError as error:
        raise PreflightError(f"filesystem path is unavailable for device authentication: {path}") from error
    major = getattr(os, "major", None)
    minor = getattr(os, "minor", None)
    if not callable(major) or not callable(minor):
        raise PreflightError("stat device major:minor authentication is unavailable")
    value = f"{major(identity.st_dev)}:{minor(identity.st_dev)}"
    if re.fullmatch(r"[0-9]+:[0-9]+", value) is None:
        raise PreflightError("stat device major:minor identity is invalid")
    return value


def _mount_evidence(
    path: Path,
    *,
    run: CommandRunner,
    label: str,
    expected_target: Path | None = None,
    target_may_be_ancestor: bool = False,
    expected_device: str | None = None,
    expected_fstype: str | None = None,
    expected_fsroot: str | None = None,
    required_mode: str | None = None,
) -> dict[str, object]:
    """Authenticate a mount view with findmnt plus the path's stat(2) device."""

    mounted = run(
        [
            "findmnt",
            "-rn",
            "--raw",
            "-T",
            path.as_posix(),
            "-o",
            "TARGET,SOURCE,FSTYPE,VFS-OPTIONS,MAJ:MIN,FSROOT",
        ]
    )
    fields = mounted.stdout.split(maxsplit=5)
    if mounted.returncode != 0 or len(fields) != 6:
        raise PreflightError(f"{label} mount evidence is unavailable or malformed")
    target_text, source, fstype, options_text, device, fsroot = fields
    if any(len(field) > 1024 for field in fields):
        raise PreflightError(f"{label} mount evidence field is oversized")
    target = PurePosixPath(target_text)
    candidate = PurePosixPath(path.as_posix())
    observed: dict[str, object] = {
        "device_major_minor": device,
        "filesystem": fstype,
        "filesystem_root": fsroot,
        "logical_path": path.as_posix(),
        "mount": target.as_posix(),
        "options": options_text.split(","),
        "source": source,
        "stat_device_major_minor": None,
    }

    def refuse(reason: str) -> NoReturn:
        raise _MountEvidenceError(f"{label} {reason}", observed)

    target_is_absolute = target.is_absolute() or (
        os.name == "nt" and re.fullmatch(r"[A-Za-z]:/.*", target_text) is not None
    )
    if not target_is_absolute or (target != candidate and not target_may_be_ancestor):
        refuse("mount target diverged")
    if target_may_be_ancestor and target != candidate and target not in candidate.parents:
        refuse("mount target is not an authenticated ancestor")
    if expected_target is not None and target != PurePosixPath(expected_target.as_posix()):
        refuse("mount target diverged")
    if re.fullmatch(r"[0-9]+:[0-9]+", device) is None:
        refuse("findmnt device identity is invalid")
    try:
        stat_device = _stat_device_major_minor(path)
    except PreflightError as error:
        refuse(f"stat device authentication failed: {error}")
    observed["stat_device_major_minor"] = stat_device
    if stat_device != device:
        refuse("findmnt and stat device identities diverged")
    if expected_device is not None and device != expected_device:
        refuse("filesystem device diverged")
    if expected_fstype is not None and fstype != expected_fstype:
        refuse("filesystem type diverged")
    if not fsroot.startswith("/") or ".." in PurePosixPath(fsroot).parts:
        refuse("filesystem root is invalid")
    if expected_fsroot is not None and fsroot != expected_fsroot:
        refuse("filesystem root/bind identity diverged")
    options = observed["options"]
    assert isinstance(options, list)
    if required_mode is not None:
        opposite = "ro" if required_mode == "rw" else "rw"
        if required_mode not in options or opposite in options:
            refuse(f"is not mounted {required_mode}")
    return {
        "device_major_minor": device,
        "filesystem": fstype,
        "filesystem_root": fsroot,
        "logical_path": path.as_posix(),
        "mount": target.as_posix(),
        "options": options,
        "source": source,
        "stat_device_major_minor": stat_device,
    }


def _expected_filesystem_root(
    *,
    admitted_root: str,
    volume_mount: Path,
    path: Path,
) -> str:
    try:
        relative = PurePosixPath(path.as_posix()).relative_to(
            PurePosixPath(volume_mount.as_posix())
        )
    except ValueError as error:
        raise PreflightError("namespace path leaves the admitted volume mount") from error
    root = PurePosixPath(admitted_root)
    if not root.is_absolute() or ".." in root.parts:
        raise PreflightError("admitted filesystem root identity is invalid")
    return (root / relative).as_posix()


def _authenticate_volume_namespace_mapping(
    evidence: Mapping[str, object],
    *,
    admitted_root: str,
    allowed_targets: Sequence[Path],
    label: str,
    volume_mount: Path,
) -> None:
    """Authenticate a systemd mount target without assuming every RO path survives."""

    target_text = evidence.get("mount")
    filesystem_root = evidence.get("filesystem_root")
    if not isinstance(target_text, str) or not isinstance(filesystem_root, str):
        raise PreflightError(f"{label} mount mapping evidence is invalid")
    target = PurePosixPath(target_text)
    allowed = {PurePosixPath(path.as_posix()) for path in allowed_targets}
    if target not in allowed:
        raise PreflightError(f"{label} mount target is not allowlisted")
    volume = PurePosixPath(volume_mount.as_posix())
    root = PurePosixPath(admitted_root)
    try:
        relative = target.relative_to(volume)
    except ValueError as error:
        raise PreflightError(f"{label} mount target leaves the admitted volume") from error
    expected_root = (root / relative).as_posix()
    if filesystem_root != expected_root:
        raise PreflightError(f"{label} filesystem root/bind identity diverged")


def _authenticate_incoming_namespace_target(
    evidence: Mapping[str, object],
    *,
    home_evidence: Mapping[str, object],
    incoming: Path,
    label: str,
) -> None:
    logical_path = evidence.get("logical_path")
    target_text = evidence.get("mount")
    filesystem_root = evidence.get("filesystem_root")
    filesystem = evidence.get("filesystem")
    device = evidence.get("device_major_minor")
    stat_device = evidence.get("stat_device_major_minor")
    source = evidence.get("source")
    options = evidence.get("options")
    home_target_text = home_evidence.get("mount")
    home_root_text = home_evidence.get("filesystem_root")
    home_filesystem = home_evidence.get("filesystem")
    home_device = home_evidence.get("device_major_minor")
    home_stat_device = home_evidence.get("stat_device_major_minor")
    home_source = home_evidence.get("source")
    home_options = home_evidence.get("options")
    home_logical_path = home_evidence.get("logical_path")
    if not all(
        isinstance(value, str)
        for value in (
            logical_path,
            target_text,
            filesystem_root,
            filesystem,
            device,
            stat_device,
            source,
            home_target_text,
            home_root_text,
            home_filesystem,
            home_device,
            home_stat_device,
            home_source,
            home_logical_path,
        )
    ) or not all(
        isinstance(value, list) and all(isinstance(item, str) for item in value)
        for value in (options, home_options)
    ):
        raise PreflightError(f"{label} mount mapping evidence is invalid")
    assert isinstance(target_text, str)
    assert isinstance(logical_path, str)
    assert isinstance(filesystem_root, str)
    assert isinstance(filesystem, str)
    assert isinstance(device, str)
    assert isinstance(stat_device, str)
    assert isinstance(source, str)
    assert isinstance(options, list)
    assert isinstance(home_target_text, str)
    assert isinstance(home_root_text, str)
    assert isinstance(home_filesystem, str)
    assert isinstance(home_device, str)
    assert isinstance(home_stat_device, str)
    assert isinstance(home_source, str)
    assert isinstance(home_options, list)
    assert isinstance(home_logical_path, str)
    target = PurePosixPath(target_text)
    home_target = PurePosixPath(home_target_text)
    incoming_path = PurePosixPath(incoming.as_posix())
    filesystem_root_path = PurePosixPath(filesystem_root)
    home_root = PurePosixPath(home_root_text)
    root = PurePosixPath("/")
    if logical_path != incoming_path.as_posix():
        raise PreflightError(f"{label} logical path diverged")
    if home_logical_path != _HOME_MOUNT.as_posix():
        raise PreflightError(f"{label} /home logical path diverged")
    root_backed_home = PurePosixPath("/home") == _HOME_MOUNT and home_target == root
    if home_target != _HOME_MOUNT and not root_backed_home:
        raise PreflightError(f"{label} home mount target is not authenticated")
    try:
        relative_incoming = incoming_path.relative_to(_HOME_MOUNT)
    except ValueError as error:
        raise PreflightError(f"{label} logical path leaves /home") from error
    if not relative_incoming.parts or ".." in relative_incoming.parts:
        raise PreflightError(f"{label} logical path is not a canonical /home descendant")
    if (
        incoming_path.parent != _INCOMING_PARENT
        or _RUN_SLUG.fullmatch(incoming_path.name) is None
    ):
        raise PreflightError(f"{label} logical path is outside the dedicated incoming root")
    allowed: set[PurePosixPath] = {incoming_path}
    cursor = incoming_path
    while cursor != _HOME_MOUNT:
        cursor = cursor.parent
        if cursor == root:
            raise PreflightError(f"{label} ancestor chain reached filesystem root")
        allowed.add(cursor)
    if root_backed_home:
        allowed.add(root)
    if target == root and not root_backed_home:
        raise PreflightError(
            f"{label} root target requires the authenticated home mount target"
        )
    if target not in allowed:
        raise PreflightError(f"{label} mount target is not an authenticated /home ancestor")
    if (
        filesystem != "ext4"
        or home_filesystem != "ext4"
        or device != stat_device
        or home_device != home_stat_device
        or device != home_device
    ):
        raise PreflightError(f"{label} filesystem type or device identity diverged")
    if "ro" not in options or "rw" in options or "ro" not in home_options or "rw" in home_options:
        raise PreflightError(f"{label} is not mounted ro")
    if (
        not home_root.is_absolute()
        or ".." in home_root.parts
        or not filesystem_root_path.is_absolute()
        or ".." in filesystem_root_path.parts
    ):
        raise PreflightError(f"{label} /home filesystem root is invalid")
    if root_backed_home and home_root != root:
        raise PreflightError(f"{label} /home filesystem root mapping diverged")
    try:
        target_relative = target.relative_to(home_target)
    except ValueError as error:
        raise PreflightError(
            f"{label} target precedes the authenticated home mount target"
        ) from error
    expected_root = (home_root / target_relative).as_posix()
    if filesystem_root != expected_root:
        raise PreflightError(f"{label} filesystem root/bind identity diverged")

    def source_parts(value: str) -> tuple[str, str | None]:
        matched = re.fullmatch(r"([^\[\]]+?)(?:\[(/[^\[\]]*)\])?", value)
        if matched is None:
            raise PreflightError(f"{label} mount source identity is invalid")
        return matched.group(1), matched.group(2)

    home_source_base, home_source_root = source_parts(home_source)
    source_base, source_root = source_parts(source)
    if home_source_root is not None and home_source_root != home_root_text:
        raise PreflightError(f"{label} /home source/root identity diverged")
    if home_source_root is None and home_root != root:
        raise PreflightError(f"{label} /home source/root identity is absent")
    if source_base != home_source_base or (
        source_root is not None and source_root != filesystem_root
    ):
        raise PreflightError(f"{label} source/root identity diverged")
    if source_root is None and filesystem_root_path != root:
        raise PreflightError(f"{label} ancestor bind source/root identity is absent")
    chain: list[PurePosixPath] = [_HOME_MOUNT]
    cursor = _HOME_MOUNT
    for part in relative_incoming.parts:
        cursor /= part
        chain.append(cursor)
    for member in chain:
        member_path = Path(member.as_posix())
        _canonical_directory(member_path, label=f"{label} authenticated chain member")
        if _stat_device_major_minor(member_path) != device:
            raise PreflightError(f"{label} authenticated chain member device diverged")


def _durable_write_surface_probe(root: Path) -> dict[str, object]:
    """Prove the sole writable venue surface without touching an existing file."""

    before = root.lstat()
    if root.is_symlink() or not stat.S_ISDIR(before.st_mode) or root.resolve(strict=True) != root:
        raise PreflightError("venue write surface is absent, symlinked, or non-canonical")
    probe_name = f".prediction-write-surface-probe-{uuid4().hex}"
    probe = root / probe_name
    directory_descriptor: int | None = None
    file_descriptor: int | None = None
    created = False
    removed = False
    probe_identity: tuple[int, int, int] | None = None
    primary_error: OSError | PreflightError | None = None
    cleanup_error: OSError | PreflightError | None = None

    def identity(value: os.stat_result) -> tuple[int, int, int]:
        return value.st_dev, value.st_ino, value.st_mode

    def current_probe_identity() -> tuple[int, int, int]:
        if directory_descriptor is not None:
            return identity(
                os.stat(
                    probe_name,
                    dir_fd=directory_descriptor,
                    follow_symlinks=False,
                )
            )
        return identity(probe.lstat())

    try:
        if os.name != "nt":
            directory_descriptor = os.open(
                root,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
            if identity(os.fstat(directory_descriptor)) != identity(before):
                raise PreflightError("venue write surface directory identity diverged")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        if directory_descriptor is not None:
            file_descriptor = os.open(
                probe_name,
                flags,
                0o600,
                dir_fd=directory_descriptor,
            )
        else:
            file_descriptor = os.open(probe, flags, 0o600)
        created = True
        probe_identity = identity(os.fstat(file_descriptor))
        if (
            not stat.S_ISREG(probe_identity[2])
            or probe_identity[0] != before.st_dev
        ):
            raise PreflightError("venue write surface probe identity is invalid")
        with os.fdopen(file_descriptor, "wb", closefd=True) as handle:
            file_descriptor = None
            handle.write(b"PREDICTION_MARKETS_WRITE_SURFACE_PROBE_V1\n")
            handle.flush()
            os.fsync(handle.fileno())
        if directory_descriptor is not None:
            os.fsync(directory_descriptor)
            if current_probe_identity() != probe_identity:
                raise PreflightError("venue write surface probe identity changed before removal")
            os.unlink(probe_name, dir_fd=directory_descriptor)
            removed = True
            os.fsync(directory_descriptor)
        else:
            if current_probe_identity() != probe_identity:
                raise PreflightError("venue write surface probe identity changed before removal")
            probe.unlink()
            removed = True
    except (OSError, PreflightError) as error:
        primary_error = error
    finally:
        if file_descriptor is not None:
            try:
                os.close(file_descriptor)
            except OSError as error:
                cleanup_error = error
        try:
            if created and not removed:
                if probe_identity is None or current_probe_identity() != probe_identity:
                    raise PreflightError(
                        "venue write surface cleanup target identity changed"
                    )
                if directory_descriptor is not None:
                    os.unlink(probe_name, dir_fd=directory_descriptor)
                else:
                    probe.unlink()
                removed = True
                if directory_descriptor is not None:
                    os.fsync(directory_descriptor)
        except (OSError, PreflightError) as error:
            cleanup_error = error
    try:
        after = root.lstat()
        descriptor_after = (
            os.fstat(directory_descriptor)
            if directory_descriptor is not None
            else after
        )
    except OSError as error:
        if cleanup_error is None:
            cleanup_error = error
        after = before
        descriptor_after = before
    if directory_descriptor is not None:
        try:
            os.close(directory_descriptor)
        except OSError as error:
            if cleanup_error is None:
                cleanup_error = error
    if (
        root.is_symlink()
        or not stat.S_ISDIR(after.st_mode)
        or identity(before) != identity(after)
        or identity(before) != identity(descriptor_after)
    ):
        raise PreflightError("venue write surface changed during durable probe")
    if cleanup_error is not None:
        raise PreflightError(
            f"venue write surface probe cleanup was not durable: {cleanup_error}"
        ) from cleanup_error
    if primary_error is not None:
        raise PreflightError(f"venue write surface durable probe failed: {primary_error}") from primary_error
    if not removed or probe.exists() or probe.is_symlink():
        raise PreflightError("venue write surface probe was not removed")
    return {
        "directory_fsync": os.name != "nt",
        "exclusive_create": True,
        "file_fsync": True,
        "probe_removed": True,
        "root": root.as_posix(),
    }


def verify_transfer_inventory(incoming_root: Path, handoff: Mapping[str, object]) -> dict[str, object]:
    if incoming_root.is_symlink() or not incoming_root.is_dir():
        raise PreflightError("incoming root is absent or unsafe")
    if incoming_root.resolve(strict=True) != incoming_root:
        raise PreflightError("incoming root real path differs")
    declared = handoff.get("transfer_inventory_sha256")
    _validate_sha256(declared, label="transfer inventory hash")
    inventory_path = incoming_root / "transfer-inventory.json"
    raw_inventory = _safe_regular_bytes(inventory_path)
    if sha256_bytes(raw_inventory) != declared:
        raise PreflightError("transfer inventory hash diverged")
    try:
        inventory = json.loads(raw_inventory.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreflightError("transfer inventory is invalid") from error
    if (
        not isinstance(inventory, dict)
        or set(inventory) != {"files", "schema_version"}
        or inventory.get("schema_version") != 1
        or not isinstance(inventory.get("files"), list)
    ):
        raise PreflightError("transfer inventory schema diverged")
    checked = 0
    seen_paths: set[str] = set()
    for item in inventory["files"]:
        if not isinstance(item, dict) or set(item) != {"path", "sha256", "size"}:
            raise PreflightError("transfer inventory entry is invalid")
        relative = item.get("path")
        expected = _validate_sha256(item.get("sha256"), label="transfer file hash")
        expected_size = item.get("size")
        if (
            not isinstance(relative, str)
            or not relative
            or "\\" in relative
            or relative in seen_paths
            or type(expected_size) is not int
            or expected_size < 0
        ):
            raise PreflightError("transfer inventory path is invalid")
        seen_paths.add(relative)
        pure = PurePosixPath(relative)
        if pure.is_absolute() or ".." in pure.parts:
            raise PreflightError("transfer inventory path escapes incoming root")
        candidate = incoming_root.joinpath(*pure.parts)
        parent_identities: list[tuple[Path, tuple[int, int, int]]] = []
        parent = incoming_root
        for part in pure.parts[:-1]:
            parent /= part
            try:
                before_parent = parent.lstat()
            except OSError as error:
                raise PreflightError(
                    f"transfer inventory parent directory is unreadable: {parent}"
                ) from error
            if (
                parent.is_symlink()
                or not stat.S_ISDIR(before_parent.st_mode)
                or parent.resolve(strict=True) != parent
            ):
                raise PreflightError(
                    f"transfer inventory parent directory is unsafe: {parent}"
                )
            parent_identities.append(
                (
                    parent,
                    (before_parent.st_dev, before_parent.st_ino, before_parent.st_mode),
                )
            )
        payload = _safe_regular_bytes(candidate, maximum_bytes=512 * 1024 * 1024)
        for parent, before_identity in parent_identities:
            try:
                after_parent = parent.lstat()
            except OSError as error:
                raise PreflightError(
                    f"transfer inventory parent directory changed during read: {parent}"
                ) from error
            after_identity = (
                after_parent.st_dev,
                after_parent.st_ino,
                after_parent.st_mode,
            )
            if (
                parent.is_symlink()
                or not stat.S_ISDIR(after_parent.st_mode)
                or after_identity != before_identity
            ):
                raise PreflightError(
                    f"transfer inventory parent directory changed during read: {parent}"
                )
        if len(payload) != expected_size or sha256_bytes(payload) != expected:
            raise PreflightError(f"transfer file hash diverged: {relative}")
        checked += 1
    return {"files_checked": checked, "inventory_sha256": declared}


def verify_wheelhouse(incoming_root: Path, handoff: Mapping[str, object]) -> dict[str, object]:
    manifest_path = incoming_root / "wheelhouse.sha256"
    raw = _safe_regular_bytes(manifest_path, maximum_bytes=1024 * 1024)
    expected_manifest = _validate_sha256(
        handoff.get("wheelhouse_manifest_sha256"), label="wheelhouse manifest hash"
    )
    if sha256_bytes(raw) != expected_manifest:
        raise PreflightError("wheelhouse manifest hash diverged")
    wheelhouse = incoming_root / "wheelhouse"
    if wheelhouse.is_symlink() or not wheelhouse.is_dir():
        raise PreflightError("wheelhouse is absent or unsafe")
    count = 0
    total = 0
    declared_names: set[str] = set()
    for line in raw.decode("ascii").splitlines():
        fields = line.split("  ", maxsplit=1)
        if len(fields) != 2:
            raise PreflightError("wheelhouse manifest line is malformed")
        expected = _validate_sha256(fields[0], label="wheel hash")
        name = fields[1]
        if not name.endswith(".whl") or Path(name).name != name or name in declared_names:
            raise PreflightError("wheelhouse filename is invalid")
        declared_names.add(name)
        wheel = wheelhouse / name
        payload = _safe_regular_bytes(wheel, maximum_bytes=512 * 1024 * 1024)
        if sha256_bytes(payload) != expected:
            raise PreflightError(f"wheel hash diverged: {name}")
        count += 1
        total += len(payload)
    if count == 0:
        raise PreflightError("wheelhouse is empty")
    entries = list(wheelhouse.iterdir())
    actual_names = {
        item.name for item in entries if item.is_file() and not item.is_symlink()
    }
    if actual_names != declared_names or len(entries) != len(declared_names):
        raise PreflightError("wheelhouse contains undeclared or unsafe entries")
    return {"bytes": total, "files": count, "manifest_sha256": expected_manifest}


def verify_git_bundle(
    incoming_root: Path,
    handoff: Mapping[str, object],
    run: CommandRunner,
) -> dict[str, object]:
    bundle = incoming_root / str(handoff["bundle_filename"])
    expected = _validate_sha256(handoff.get("bundle_sha256"), label="Git bundle hash")
    payload = _safe_regular_bytes(bundle, maximum_bytes=512 * 1024 * 1024)
    if sha256_bytes(payload) != expected:
        raise PreflightError("Git bundle SHA-256 diverged")
    verify_root = incoming_root / f".git-bundle-verify-{uuid4().hex}"
    verify_root.mkdir(mode=0o700, exist_ok=False)
    try:
        initialized = run(["git", "init", "--bare", "--quiet", str(verify_root)])
        if initialized.returncode != 0:
            raise PreflightError(
                f"temporary bare repository initialization failed: {initialized.stderr}"
            )
        verified = run(
            ["git", "-C", str(verify_root), "bundle", "verify", str(bundle)]
        )
        if verified.returncode != 0:
            raise PreflightError(f"Git bundle verification failed: {verified.stderr}")
    finally:
        if (
            verify_root.is_symlink()
            or verify_root.resolve(strict=True) != verify_root
            or verify_root.parent != incoming_root
            or not verify_root.name.startswith(".git-bundle-verify-")
        ):
            raise PreflightError("refusing unsafe Git bundle verification cleanup")
        shutil.rmtree(verify_root)
    return {
        "sha256": expected,
        "temporary_repository_removed": not verify_root.exists(),
        "verified": True,
    }


def _dns(host: str) -> list[str]:
    try:
        completed = subprocess.run(
            ["getent", "ahosts", host],
            check=False,
            capture_output=True,
            text=True,
            timeout=_NETWORK_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as error:
        raise TimeoutError(f"DNS lookup exceeded {_NETWORK_TIMEOUT_SECONDS:g}s") from error
    if completed.returncode != 0:
        raise LookupError(completed.stderr.strip() or "getent returned no address")
    addresses: list[str] = []
    for line in completed.stdout.splitlines():
        fields = line.split()
        if not fields:
            continue
        try:
            addresses.append(str(ipaddress.ip_address(fields[0])))
        except ValueError:
            continue
    addresses = sorted(set(addresses))
    if not addresses:
        raise LookupError("no address returned")
    return addresses[:8]


def _tls(host: str) -> str:
    context = ssl.create_default_context()
    with (
        socket.create_connection((host, 443), timeout=_NETWORK_TIMEOUT_SECONDS) as plain,
        context.wrap_socket(plain, server_hostname=host) as secured,
    ):
        certificate = secured.getpeercert()
        if not certificate:
            raise ssl.SSLError("peer certificate is absent")
        return secured.version() or "TLS_VERSION_UNKNOWN"


def _https_get(url: str) -> int:
    class _NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(
            self,
            req: urllib.request.Request,
            fp: object,
            code: int,
            msg: str,
            headers: object,
            newurl: str,
        ) -> urllib.request.Request | None:
            return None

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "HyperLab-Public-Preflight/1"},
        method="GET",
    )
    try:
        with opener.open(request, timeout=_NETWORK_TIMEOUT_SECONDS) as response:
            response.read(4096)
            return int(response.status)
    except urllib.error.HTTPError as error:
        return int(error.code)


def _wss_handshake(host: str, path: str) -> int:
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    context = ssl.create_default_context()
    with (
        socket.create_connection((host, 443), timeout=_NETWORK_TIMEOUT_SECONDS) as plain,
        context.wrap_socket(plain, server_hostname=host) as secured,
    ):
        secured.settimeout(_NETWORK_TIMEOUT_SECONDS)
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "Connection: Upgrade\r\n"
            "Upgrade: websocket\r\n"
            "Origin: https://polymarket.com\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode("ascii")
        secured.sendall(request)
        response = secured.recv(4096).split(b"\r\n", maxsplit=1)[0]
    fields = response.decode("ascii", errors="replace").split()
    if len(fields) < 2 or not fields[1].isdigit():
        raise ConnectionError("invalid WebSocket HTTP response")
    return int(fields[1])


def probe_venue_connectivity(
    venue: str,
    *,
    dns_probe: Callable[[str], list[str]] = _dns,
    tls_probe: Callable[[str], str] = _tls,
    https_probe: Callable[[str], int] = _https_get,
    wss_probe: Callable[[str, str], int] = _wss_handshake,
) -> dict[str, object]:
    hosts: tuple[str, ...]
    if venue == "polymarket":
        hosts = (
            "gamma-api.polymarket.com",
            "clob.polymarket.com",
            "data-api.polymarket.com",
            "ws-subscriptions-clob.polymarket.com",
        )
        https_url = "https://gamma-api.polymarket.com/markets/keyset?limit=1"
    elif venue == "kalshi":
        hosts = ("external-api.kalshi.com",)
        https_url = "https://external-api.kalshi.com/trade-api/v2/markets?limit=1"
    else:
        raise PreflightError("unknown prediction venue")
    started = time.monotonic()
    errors: list[str] = []
    dns: dict[str, object] = {}
    tls: dict[str, object] = {}
    for host in hosts:
        try:
            dns[host] = dns_probe(host)
        except (LookupError, OSError, socket.gaierror) as error:
            dns[host] = None
            errors.append(f"DNS:{host}:{type(error).__name__}:{error}")
            continue
        try:
            tls[host] = tls_probe(host)
        except (ConnectionError, OSError, ssl.SSLError, TimeoutError) as error:
            tls[host] = None
            errors.append(f"TLS:{host}:{type(error).__name__}:{error}")
    https_status: int | None = None
    if not errors:
        try:
            https_status = https_probe(https_url)
            if https_status in {403, 429}:
                errors.append(f"HTTPS_STATUS_{https_status}")
            elif not 200 <= https_status < 300:
                errors.append(f"HTTPS_UNEXPECTED_STATUS_{https_status}")
        except (ConnectionError, OSError, TimeoutError, urllib.error.URLError) as error:
            errors.append(f"HTTPS:{type(error).__name__}:{error}")
    wss: dict[str, object]
    if venue == "polymarket" and not errors:
        try:
            status = wss_probe("ws-subscriptions-clob.polymarket.com", "/ws/market")
            wss = {"documented_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market", "status": status}
            if status != http.client.SWITCHING_PROTOCOLS:
                errors.append(f"WSS_UNEXPECTED_STATUS_{status}")
        except (ConnectionError, OSError, ssl.SSLError, TimeoutError) as error:
            wss = {"documented_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market", "status": None}
            errors.append(f"WSS:{type(error).__name__}:{error}")
    elif venue == "kalshi":
        wss = {
            "documented_status": "AUTHENTICATED_HANDSHAKE_REQUIRED",
            "probe": "NOT_EXECUTED_CREDENTIALS_FORBIDDEN",
            "status": None,
        }
    else:
        wss = {"status": None}
    return {
        "attempts": 1,
        "dns": dns,
        "elapsed_ms": int((time.monotonic() - started) * 1000),
        "errors": errors,
        "https": {"status": https_status, "url": https_url},
        "max_duration_seconds": 40,
        "proxy_policy": "DIRECT_ONLY_ENVIRONMENT_PROXY_DISABLED",
        "tls": tls,
        "venue": venue,
        "verdict": "NETWORK_PREFLIGHT_GREEN" if not errors else "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT",
        "wss": wss,
    }


def _parse_df_available(output: str) -> int:
    lines = [line for line in output.splitlines() if line.strip()]
    if len(lines) < 2:
        raise PreflightError("df output is incomplete")
    fields = lines[-1].split()
    if len(fields) < 4 or not fields[3].isdigit():
        raise PreflightError("df available bytes are invalid")
    return int(fields[3])


def _systemd_collision(service: str, run: CommandRunner) -> dict[str, object]:
    result = run(
        [
            "systemctl",
            "show",
            service,
            "--property=LoadState,ActiveState,SubState,MainPID",
            "--no-pager",
        ]
    )
    if result.returncode != 0:
        raise PreflightError(
            f"systemd service identity check failed: {service}: {result.stderr or 'no diagnostic'}"
        )
    values: dict[str, str] = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key] = value
    expected = {"LoadState", "ActiveState", "SubState", "MainPID"}
    if set(values) != expected:
        raise PreflightError(f"systemd service identity is incomplete: {service}")
    load = values["LoadState"]
    active = values["ActiveState"]
    sub = values["SubState"]
    main_pid = values["MainPID"]
    if (
        load != "not-found"
        or active != "inactive"
        or sub != "dead"
        or main_pid != "0"
    ):
        raise PreflightError(f"service collision: {service} load={load} active={active}")
    return {"active_state": active, "load_state": load, "service": service}


def _authenticate_superseded_dashboard_port(
    handoff_path: Path,
    handoff: Mapping[str, object],
    run: CommandRunner,
) -> dict[str, object]:
    superseded = handoff.get("superseded_campaign")
    if not isinstance(superseded, Mapping):
        raise PreflightError("superseded campaign contract is absent")
    services = superseded.get("services")
    if (
        superseded.get("run_slug") != "pm-20260828t024827z-bcb5280f"
        or superseded.get("source_commit")
        != "bcb5280f87393992e2aa4528188009186cd8bdc3"
        or superseded.get("dashboard_port") != 18081
        or not isinstance(services, Mapping)
    ):
        raise PreflightError("superseded dashboard identity diverged")
    dashboard = services.get("dashboard")
    if dashboard != "hyperlab-pm-20260828t024827z-bcb5280f-dashboard.service":
        raise PreflightError("superseded dashboard service identity diverged")
    script = handoff_path.parent / "scripts" / "cutover.sh"
    if script.is_symlink() or not script.is_file() or script.resolve(strict=True) != script:
        raise PreflightError("authenticated cutover verifier is absent or unsafe")
    verified = run(["bash", str(script), "verify-old", str(handoff_path)])
    expected_signals = [
        "PREDICTION_OLD_RAW_RECEIPTS_LEDGER_AUTHENTICATED",
        "PREDICTION_OLD_CAMPAIGN_FIVE_UNITS_AUTHENTICATED",
        "PREDICTION_OLD_CAMPAIGN_PREMUTATION_AUTHENTICATED",
    ]
    if verified.returncode != 0 or verified.stdout.splitlines() != expected_signals:
        raise PreflightError(
            "superseded dashboard port owner authentication failed: "
            + (verified.stderr or verified.stdout or "no diagnostic")[:1024]
        )
    return {
        "free": False,
        "host": "127.0.0.1",
        "occupied_by_authenticated_superseded_dashboard": True,
        "owner_service": dashboard,
        "port": 18081,
        "verification_signals": expected_signals,
    }


def host_preflight(
    handoff_path: Path,
    *,
    run: CommandRunner = _command,
    connectivity_probe: Callable[[str], dict[str, object]] = probe_venue_connectivity,
) -> dict[str, object]:
    handoff = load_handoff(handoff_path)
    incoming = handoff_path.parent.resolve(strict=True)
    errors: list[str] = []
    checks: dict[str, object] = {}
    try:
        checks["transfer"] = verify_transfer_inventory(incoming, handoff)
        checks["wheelhouse"] = verify_wheelhouse(incoming, handoff)
        if os.environ.get("USER") != handoff.get("service_user"):
            raise PreflightError("preflight must run as the dedicated service user")
        if Path.home().as_posix() != f"/home/{handoff.get('service_user')}":
            raise PreflightError("service user HOME diverged")
        volume_base = Path(str(handoff["volume_base"]))
        expected_parents = {
            "volume_base": volume_base,
            "source_parent": Path(str(handoff["source_root"])).parent,
            "campaign_parent": Path(str(handoff["campaign_root"])).parent,
        }
        if (
            expected_parents["source_parent"] != volume_base / "sources"
            or expected_parents["campaign_parent"] != volume_base / "campaigns"
        ):
            raise PreflightError("attempt parent roots leave the dedicated Prediction Markets tree")
        for label, candidate in expected_parents.items():
            if candidate.is_symlink() or (
                candidate.exists()
                and (not candidate.is_dir() or candidate.resolve(strict=True) != candidate)
            ):
                raise PreflightError(f"{label} is an unsafe existing path")
        for name in (
            "bash",
            "df",
            "findmnt",
            "getent",
            "git",
            "python3.12",
            "sha256sum",
            "systemctl",
            "timedatectl",
        ):
            _required_command(name)
        python = run(
            [
                "python3.12",
                "-I",
                "-c",
                (
                    "import platform,ssl,sys,venv;"
                    "assert sys.version_info[:2]==(3,12);"
                    "assert platform.machine() in {'x86_64','AMD64'};"
                    "libc,version=platform.libc_ver();"
                    "assert libc=='glibc' and tuple(map(int,version.split('.')[:2]))>=(2,28);"
                    "assert ssl.OPENSSL_VERSION"
                ),
            ]
        )
        if python.returncode != 0:
            raise PreflightError(f"offline CPython preflight failed: {python.stderr}")
        checks["python"] = {
            "implementation": platform.python_implementation(),
            "minimum_glibc": "2.28",
            "required": "CPython 3.12 x86_64",
            "system_command_green": True,
        }
        checks["git_bundle"] = verify_git_bundle(incoming, handoff, run)
        ntp = run(["timedatectl", "show", "--property=NTPSynchronized", "--value"])
        if ntp.returncode != 0 or ntp.stdout != "yes":
            raise PreflightError("NTP is not synchronized")
        checks["ntp"] = {"synchronized": True}
        mount = str(handoff["volume_mount"])
        filesystem = _mount_evidence(
            Path(mount),
            run=run,
            label="campaign host filesystem",
            expected_target=Path(mount),
            expected_fstype="ext4",
            expected_fsroot="/",
            required_mode="rw",
        )
        capacity = run(["df", "-PB1", mount])
        if capacity.returncode != 0:
            raise PreflightError(f"campaign capacity check failed: {capacity.stderr}")
        available = _parse_df_available(capacity.stdout)
        disk = handoff.get("disk")
        if not isinstance(disk, Mapping):
            raise PreflightError("disk reservation contract is absent")
        required = disk.get("required_free_bytes")
        if type(required) is not int or required <= 0:
            raise PreflightError("required capacity is invalid")
        if available < required:
            raise PreflightError(
                "PREDICTION_CAPACITY_REFUSED_COEXISTENCE_NOT_PROVEN "
                f"available={available} required={required}; use a distinct host or ext4 volume"
            )
        checks["filesystem"] = {
            "available_bytes": available,
            "required_free_bytes": required,
            **filesystem,
        }
        for key in ("source_root", "campaign_root"):
            candidate = Path(str(handoff[key]))
            if candidate.exists() or candidate.is_symlink():
                raise PreflightError(f"unique {key} already exists")
        dashboard_port = handoff.get("dashboard_port")
        if type(dashboard_port) is not int or not 1024 <= dashboard_port <= 65535:
            raise PreflightError("dashboard port is invalid")
        checks["loopback_port"] = _authenticate_superseded_dashboard_port(
            handoff_path,
            handoff,
            run,
        )
        services = handoff.get("services")
        if not isinstance(services, Mapping) or set(services) != {"dashboard", "kalshi", "polymarket"}:
            raise PreflightError("service identity map diverged")
        slug = handoff.get("run_slug")
        if not isinstance(slug, str) or _RUN_SLUG.fullmatch(slug) is None:
            raise PreflightError("host preflight run slug is invalid")
        suffix = slug.removeprefix("pm-")
        namespace_probes = {
            venue: f"hyperlab-pm-{suffix}-{venue}-namespace-probe.service"
            for venue in ("polymarket", "kalshi")
        }
        checks["services"] = [
            _systemd_collision(service, run)
            for service in [
                *(str(services[name]) for name in ("polymarket", "kalshi", "dashboard")),
                *(str(namespace_probes[name]) for name in ("polymarket", "kalshi")),
            ]
        ]
    except (OSError, PreflightError, subprocess.SubprocessError) as error:
        errors.append(str(error))
    connectivity: dict[str, dict[str, object]] = {}
    for venue in ("polymarket", "kalshi"):
        try:
            connectivity[venue] = connectivity_probe(venue)
        except (OSError, PreflightError, TimeoutError) as error:
            connectivity[venue] = {
                "errors": [f"PREFLIGHT_INTERNAL:{type(error).__name__}:{error}"],
                "venue": venue,
                "verdict": "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT",
            }
    eligible = [
        venue
        for venue in ("polymarket", "kalshi")
        if connectivity[venue].get("verdict") == "NETWORK_PREFLIGHT_GREEN"
    ]
    return {
        "boundary": BOUNDARY,
        "checks": checks,
        "eligible_venues": eligible,
        "errors": errors,
        "host_admitted": not errors,
        "installation_admissible": not errors,
        "network": connectivity,
        "recorded_at_utc": _utc_now_text(),
        "schema_version": 1,
        "terminal_signal": (
            "PREDICTION_HOST_PREFLIGHT_GREEN"
            if not errors
            else "PREDICTION_HOST_PREFLIGHT_REFUSED"
        ),
    }


def fsync_probe(handoff_path: Path) -> dict[str, object]:
    handoff = load_handoff(handoff_path)
    base = Path(str(handoff["volume_base"]))
    getuid = getattr(os, "getuid", lambda: -1)
    roots = (base, base / "sources", base / "campaigns")
    for root in roots:
        if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
            raise PreflightError(
                "dedicated Prediction Markets parent root is absent or unsafe"
            )
        identity = root.stat()
        if identity.st_uid != getuid() or (identity.st_mode & 0o777) != 0o700:
            raise PreflightError(
                "dedicated Prediction Markets parent ownership or mode diverged"
            )
    probe = base / f".prediction-fsync-probe-{uuid4().hex}"
    descriptor = os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(b"PREDICTION_MARKETS_FSYNC_PROBE_V1\n")
            handle.flush()
            os.fsync(handle.fileno())
        directory = os.open(base, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
            probe.unlink()
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if probe.exists():
            probe.unlink()
    return {
        "boundary": BOUNDARY,
        "filesystem_write_surface": str(base),
        "parent_roots": [str(root) for root in roots],
        "probe_removed": not probe.exists(),
        "recorded_at_utc": _utc_now_text(),
        "schema_version": 1,
        "terminal_signal": "PREDICTION_FILESYSTEM_FSYNC_GREEN",
    }


def resume_preflight(
    handoff_path: Path,
    *,
    run: CommandRunner = _command,
) -> dict[str, object]:
    handoff = load_handoff(handoff_path)
    errors: list[str] = []
    checks: dict[str, object] = {}
    try:
        if os.environ.get("USER") != handoff.get("service_user"):
            raise PreflightError("resume preflight must run as the dedicated service user")
        if Path.home().as_posix() != f"/home/{handoff.get('service_user')}":
            raise PreflightError("resume service user HOME diverged")
        source = Path(str(handoff["source_root"]))
        campaign = Path(str(handoff["campaign_root"]))
        volume_base = Path(str(handoff["volume_base"])).as_posix()
        if (
            source.is_symlink()
            or campaign.is_symlink()
            or source.resolve(strict=True) != source
            or campaign.resolve(strict=True) != campaign
            or not source.as_posix().startswith(f"{volume_base}/sources/")
            or not campaign.as_posix().startswith(f"{volume_base}/campaigns/")
        ):
            raise PreflightError("resume roots are absent or leave the Prediction Markets tree")
        runtime = source / ".venv" / "bin" / "python"
        if runtime.is_symlink() or not runtime.is_file():
            raise PreflightError("resume offline Python is absent or unsafe")
        incoming = Path(str(handoff["incoming_root"]))
        transfer = verify_transfer_inventory(incoming, handoff)
        source_commit = handoff.get("source_commit")
        source_inventory_sha256 = _validate_sha256(
            handoff.get("source_inventory_sha256"),
            label="resume source inventory hash",
        )
        if (
            type(source_commit) is not str
            or len(source_commit) != 40
            or any(character not in _SHA256 for character in source_commit)
        ):
            raise PreflightError("resume source commit is invalid")
        identity_command = run(
            [
                str(runtime),
                "-I",
                str(incoming / "scripts" / "launch_pack.py"),
                "verify-source",
                "--source-root",
                str(source),
                "--inventory",
                str(incoming / "source-inventory.json"),
                "--expected-commit",
                source_commit,
            ]
        )
        if identity_command.returncode != 0:
            raise PreflightError(
                f"resume source identity failed: {identity_command.stderr}"
            )
        try:
            source_identity = json.loads(identity_command.stdout)
        except json.JSONDecodeError as error:
            raise PreflightError("resume source identity output is invalid") from error
        if (
            not isinstance(source_identity, dict)
            or set(source_identity) != {"commit", "files", "inventory_sha256", "status"}
            or source_identity.get("commit") != source_commit
            or type(source_identity.get("files")) is not int
            or int(source_identity["files"]) <= 0
            or source_identity.get("inventory_sha256") != source_inventory_sha256
            or source_identity.get("status") != "PREDICTION_SOURCE_IDENTITY_GREEN"
        ):
            raise PreflightError("resume source identity result diverged")
        runtime_import = _runtime_import_subprocess_admission(
            runtime=runtime,
            incoming_root=incoming,
            source_root=source,
            source_commit=source_commit,
            inventory_sha256=source_inventory_sha256,
            run=run,
        )
        ntp = run(["timedatectl", "show", "--property=NTPSynchronized", "--value"])
        if ntp.returncode != 0 or ntp.stdout != "yes":
            raise PreflightError("NTP is not synchronized for resume")
        mount = str(handoff["volume_mount"])
        target = run(["findmnt", "-rn", "-T", mount, "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"])
        fields = target.stdout.split(maxsplit=3)
        if (
            target.returncode != 0
            or len(fields) != 4
            or fields[0] != mount
            or fields[2] != "ext4"
            or "rw" not in fields[3].split(",")
            or "ro" in fields[3].split(",")
        ):
            raise PreflightError("resume filesystem is not the admitted ext4 rw mount")
        capacity = run(["df", "-PB1", mount])
        if capacity.returncode != 0:
            raise PreflightError(f"resume capacity check failed: {capacity.stderr}")
        available = _parse_df_available(capacity.stdout)
        disk = handoff.get("disk")
        if not isinstance(disk, Mapping) or type(disk.get("required_free_bytes")) is not int:
            raise PreflightError("resume disk reservation contract is absent")
        required = int(disk["required_free_bytes"])
        if available < required:
            raise PreflightError(
                "PREDICTION_CAPACITY_REFUSED_COEXISTENCE_NOT_PROVEN "
                f"available={available} required={required}; use a distinct host or ext4 volume"
            )
        checks = {
            "filesystem": {
                "available_bytes": available,
                "filesystem": "ext4",
                "mount": mount,
                "required_free_bytes": required,
            },
            "ntp": {"synchronized": True},
            "offline_imports": {
                "admission_sha256": runtime_import["admission_sha256"],
                "terminal_signal": runtime_import["terminal_signal"],
                "verified": True,
            },
            "roots": {"campaign": str(campaign), "source": str(source)},
            "source_identity": source_identity,
            "transfer_identity": transfer,
        }
    except (OSError, PreflightError, subprocess.SubprocessError) as error:
        errors.append(str(error))
    return {
        "boundary": BOUNDARY,
        "checks": checks,
        "errors": errors,
        "recorded_at_utc": _utc_now_text(),
        "resume_admissible": not errors,
        "schema_version": 1,
        "terminal_signal": (
            "PREDICTION_RESUME_PREFLIGHT_GREEN"
            if not errors
            else "PREDICTION_RESUME_PREFLIGHT_REFUSED"
        ),
    }


def _canonical_runtime_report(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
    raw = _safe_regular_bytes(path)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreflightError(f"{label} is invalid JSON") from error
    if not isinstance(value, dict) or raw != canonical_json_bytes(value) + b"\n":
        raise PreflightError(f"{label} is not canonical JSON with LF")
    return value, raw


def _live_reservation_checks(
    handoff: Mapping[str, object],
    *,
    run: CommandRunner,
    label: str,
) -> dict[str, object]:
    ntp = run(["timedatectl", "show", "--property=NTPSynchronized", "--value"])
    if ntp.returncode != 0 or ntp.stdout != "yes":
        raise PreflightError(f"NTP is not synchronized for {label}")
    mount = str(handoff["volume_mount"])
    filesystem = _mount_evidence(
        Path(mount),
        run=run,
        label=f"{label} filesystem",
        expected_target=Path(mount),
        expected_fstype="ext4",
        expected_fsroot="/",
        required_mode="rw",
    )
    capacity = run(["df", "-PB1", mount])
    if capacity.returncode != 0:
        raise PreflightError(f"{label} capacity check failed: {capacity.stderr}")
    available = _parse_df_available(capacity.stdout)
    disk = handoff.get("disk")
    if not isinstance(disk, Mapping) or type(disk.get("required_free_bytes")) is not int:
        raise PreflightError(f"{label} disk reservation contract is absent")
    required = int(disk["required_free_bytes"])
    if available < required:
        raise PreflightError(
            "PREDICTION_CAPACITY_REFUSED_COEXISTENCE_NOT_PROVEN "
            f"available={available} required={required}; enlarge or choose another ext4 volume"
        )
    return {
        "capacity": {
            "admitted": True,
            "available_bytes": available,
            "required_free_bytes": required,
        },
        "filesystem": filesystem,
        "ntp": {"synchronized": True},
    }


def install_admission_preflight(
    handoff_path: Path,
    host_report_path: Path,
    fsync_report_path: Path,
    *,
    run: CommandRunner = _command,
) -> dict[str, object]:
    """Re-authenticate static inputs and live reserves immediately before systemd mutation."""

    errors: list[str] = []
    evidence: dict[str, object] = {}
    try:
        handoff = load_handoff(handoff_path)
        layout = validate_install_layout(handoff, handoff_path=handoff_path)
        if os.environ.get("USER") != "hyperlab" or Path.home().as_posix() != "/home/hyperlab":
            raise PreflightError("install admission must run as the dedicated hyperlab user")
        host, host_raw = _canonical_runtime_report(
            host_report_path,
            label="host preflight report",
        )
        network = host.get("network")
        eligible = host.get("eligible_venues")
        host_checks = host.get("checks")
        old_port = host_checks.get("loopback_port") if isinstance(host_checks, Mapping) else None
        if (
            host.get("boundary") != BOUNDARY
            or host.get("schema_version") != 1
            or host.get("terminal_signal") != "PREDICTION_HOST_PREFLIGHT_GREEN"
            or host.get("host_admitted") is not True
            or host.get("installation_admissible") is not True
            or host.get("errors") != []
            or not isinstance(network, Mapping)
            or set(network) != {"polymarket", "kalshi"}
            or not isinstance(eligible, list)
            or eligible
            != [
                venue
                for venue in ("polymarket", "kalshi")
                if isinstance(network.get(venue), Mapping)
                and network[venue].get("verdict") == "NETWORK_PREFLIGHT_GREEN"
            ]
            or not isinstance(old_port, Mapping)
            or old_port.get("host") != "127.0.0.1"
            or old_port.get("port") != 18081
            or old_port.get("free") is not False
            or old_port.get("occupied_by_authenticated_superseded_dashboard") is not True
            or old_port.get("owner_service")
            != "hyperlab-pm-20260828t024827z-bcb5280f-dashboard.service"
        ):
            raise PreflightError("host preflight admission evidence diverged")
        fsync, fsync_raw = _canonical_runtime_report(
            fsync_report_path,
            label="filesystem fsync report",
        )
        expected_parents = [
            str(_VOLUME_BASE),
            str(_VOLUME_BASE / "sources"),
            str(_VOLUME_BASE / "campaigns"),
        ]
        if (
            fsync.get("boundary") != BOUNDARY
            or fsync.get("schema_version") != 1
            or fsync.get("terminal_signal") != "PREDICTION_FILESYSTEM_FSYNC_GREEN"
            or fsync.get("filesystem_write_surface") != str(_VOLUME_BASE)
            or fsync.get("parent_roots") != expected_parents
            or fsync.get("probe_removed") is not True
        ):
            raise PreflightError("filesystem fsync admission evidence diverged")
        resumed = resume_preflight(handoff_path, run=run)
        if resumed.get("resume_admissible") is not True:
            resume_errors = resumed.get("errors")
            if not isinstance(resume_errors, list):
                resume_errors = ["resume error schema diverged"]
            raise PreflightError(
                "install source/reservation revalidation failed: "
                + "; ".join(str(item) for item in resume_errors)
            )
        live = _live_reservation_checks(handoff, run=run, label="install admission")
        host_filesystem = (
            host_checks.get("filesystem") if isinstance(host_checks, Mapping) else None
        )
        live_filesystem = live.get("filesystem")
        if (
            not isinstance(host_filesystem, Mapping)
            or not isinstance(live_filesystem, Mapping)
            or host_filesystem.get("device_major_minor")
            != live_filesystem.get("device_major_minor")
            or host_filesystem.get("stat_device_major_minor")
            != live_filesystem.get("stat_device_major_minor")
            or host_filesystem.get("filesystem") != "ext4"
            or live_filesystem.get("filesystem") != "ext4"
            or host_filesystem.get("filesystem_root") != "/"
            or live_filesystem.get("filesystem_root") != "/"
        ):
            raise PreflightError("install filesystem device diverged from host preflight")
        port = int(handoff["dashboard_port"])
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
            probe.bind(("127.0.0.1", port))
        services = handoff["services"]
        if not isinstance(services, Mapping):
            raise PreflightError("handoff services mapping is invalid")
        namespace_probes = layout["namespace_probe_services"]
        if not isinstance(namespace_probes, Mapping):
            raise PreflightError("namespace probe services mapping is invalid")
        all_services = [
            *(str(services[name]) for name in ("polymarket", "kalshi", "dashboard")),
            *(str(namespace_probes[name]) for name in ("polymarket", "kalshi")),
        ]
        service_checks = [_systemd_collision(service, run) for service in all_services]
        verify_transfer_inventory(handoff_path.parent, handoff)
        inventory = _object(handoff_path.parent / "transfer-inventory.json")
        items = inventory.get("files")
        if not isinstance(items, list):
            raise PreflightError("transfer inventory entries are absent")
        by_path = {
            str(item.get("path")): item
            for item in items
            if isinstance(item, Mapping)
        }
        unit_sha256: dict[str, str] = {}
        for service in all_services:
            item = by_path.get(f"systemd/{service}")
            if not isinstance(item, Mapping):
                raise PreflightError(f"authenticated unit is absent: {service}")
            unit_sha256[service] = _validate_sha256(
                item.get("sha256"),
                label=f"{service} unit hash",
            )
        resume_checks = resumed.get("checks")
        evidence = {
            "filesystem_fsync_report_sha256": sha256_bytes(fsync_raw),
            "handoff_sha256": sha256_bytes(_safe_regular_bytes(handoff_path)),
            "host_preflight_report_sha256": sha256_bytes(host_raw),
            "layout": layout,
            "live": live,
            "loopback_port": {"free": True, "host": "127.0.0.1", "port": port},
            "services": service_checks,
            "source_identity": resume_checks.get("source_identity")
            if isinstance(resume_checks, Mapping)
            else None,
            "transfer_inventory_sha256": handoff["transfer_inventory_sha256"],
            "unit_sha256": unit_sha256,
        }
    except (OSError, PreflightError, subprocess.SubprocessError, ValueError) as error:
        errors.append(str(error))
    return {
        "boundary": BOUNDARY,
        "errors": errors,
        "evidence": evidence,
        "install_admissible": not errors,
        "recorded_at_utc": _utc_now_text(),
        "schema_version": 1,
        "terminal_signal": (
            "PREDICTION_INSTALL_ADMISSION_GREEN"
            if not errors
            else "PREDICTION_INSTALL_ADMISSION_REFUSED"
        ),
    }


def _validated_install_admission(
    handoff_path: Path,
    install_admission_path: Path,
    *,
    operation: str,
) -> tuple[dict[str, Any], dict[str, object], dict[str, object]]:
    handoff = load_handoff(handoff_path)
    layout = validate_install_layout(handoff, handoff_path=handoff_path)
    expected_admission_path = (
        Path(str(layout["campaign_root"]))
        / "state"
        / "install-admission-report.json"
    )
    if install_admission_path != expected_admission_path:
        raise PreflightError(f"{operation} install admission path diverged")
    admission, _admission_raw = _canonical_runtime_report(
        install_admission_path,
        label="install admission report",
    )
    evidence = admission.get("evidence")
    prior_live = evidence.get("live") if isinstance(evidence, Mapping) else None
    prior_filesystem = (
        prior_live.get("filesystem") if isinstance(prior_live, Mapping) else None
    )
    admitted_device = (
        prior_filesystem.get("device_major_minor")
        if isinstance(prior_filesystem, Mapping)
        else None
    )
    if (
        set(admission)
        != {
            "boundary",
            "errors",
            "evidence",
            "install_admissible",
            "recorded_at_utc",
            "schema_version",
            "terminal_signal",
        }
        or admission.get("boundary") != BOUNDARY
        or admission.get("errors") != []
        or admission.get("install_admissible") is not True
        or admission.get("schema_version") != 1
        or admission.get("terminal_signal") != "PREDICTION_INSTALL_ADMISSION_GREEN"
        or not isinstance(evidence, Mapping)
        or evidence.get("handoff_sha256")
        != sha256_bytes(_safe_regular_bytes(handoff_path))
        or evidence.get("layout") != layout
        or not isinstance(prior_filesystem, Mapping)
        or not isinstance(admitted_device, str)
        or re.fullmatch(r"[0-9]+:[0-9]+", admitted_device) is None
        or prior_filesystem.get("stat_device_major_minor") != admitted_device
        or prior_filesystem.get("filesystem") != "ext4"
        or prior_filesystem.get("filesystem_root") != "/"
        or prior_filesystem.get("mount") != layout["volume_mount"]
    ):
        raise PreflightError(f"{operation} install admission binding diverged")
    return handoff, layout, dict(prior_filesystem)


def collector_activation_guard(
    handoff_path: Path,
    install_admission_path: Path,
    *,
    run: CommandRunner = _command,
) -> dict[str, object]:
    """Rebind the admitted device, then refresh reserves before collectors."""

    errors: list[str] = []
    live: dict[str, object] = {}
    try:
        handoff, _layout, admitted_filesystem = _validated_install_admission(
            handoff_path,
            install_admission_path,
            operation="collector guard",
        )
        live = _live_reservation_checks(handoff, run=run, label="collector activation")
        current_filesystem = live.get("filesystem")
        if (
            not isinstance(current_filesystem, Mapping)
            or current_filesystem.get("device_major_minor")
            != admitted_filesystem.get("device_major_minor")
            or current_filesystem.get("stat_device_major_minor")
            != admitted_filesystem.get("stat_device_major_minor")
            or current_filesystem.get("filesystem") != "ext4"
            or current_filesystem.get("filesystem_root") != "/"
        ):
            raise PreflightError("collector activation filesystem device diverged")
    except (OSError, PreflightError, subprocess.SubprocessError, ValueError) as error:
        errors.append(str(error))
    return {
        "boundary": BOUNDARY,
        "errors": errors,
        "live": live,
        "activation_admissible": not errors,
        "recorded_at_utc": _utc_now_text(),
        "schema_version": 1,
        "terminal_signal": (
            "PREDICTION_COLLECTOR_ACTIVATION_GUARD_GREEN"
            if not errors
            else "PREDICTION_COLLECTOR_ACTIVATION_GUARD_REFUSED"
        ),
    }


def _canonical_directory(path: Path, *, label: str) -> tuple[int, int, int]:
    try:
        identity = path.lstat()
    except OSError as error:
        raise PreflightError(f"{label} is absent") from error
    if (
        path.is_symlink()
        or not stat.S_ISDIR(identity.st_mode)
        or path.resolve(strict=True) != path
    ):
        raise PreflightError(f"{label} is symlinked, non-directory, or non-canonical")
    return identity.st_dev, identity.st_ino, identity.st_mode


def _runner_namespace_checks(
    handoff: Mapping[str, object],
    layout: Mapping[str, object],
    admitted_filesystem: Mapping[str, object],
    *,
    venue: str,
    run: CommandRunner,
    write_probe: WriteSurfaceProbe,
    observed_mounts: dict[str, object],
) -> dict[str, object]:
    if venue not in {"polymarket", "kalshi"}:
        raise PreflightError("runner namespace venue is invalid")
    service_user = str(handoff.get("service_user") or "")
    if os.environ.get("USER") != service_user:
        raise PreflightError("runner namespace guard must run as the dedicated service user")
    if Path.home().as_posix() != f"/home/{service_user}":
        raise PreflightError("runner namespace guard service user HOME diverged")
    incoming = Path(str(layout["incoming_root"]))
    volume_mount = Path(str(layout["volume_mount"]))
    volume_base = Path(str(layout["volume_base"]))
    source = Path(str(layout["source_root"]))
    campaign = Path(str(layout["campaign_root"]))
    venue_root = campaign / venue
    roots_before = {
        "incoming": _canonical_directory(incoming, label="runner incoming root"),
        "volume_base": _canonical_directory(
            volume_base, label="runner Prediction Markets volume base"
        ),
        "source": _canonical_directory(source, label="runner source root"),
        "campaign": _canonical_directory(campaign, label="runner campaign root"),
        "venue": _canonical_directory(venue_root, label="runner venue root"),
    }
    device = str(admitted_filesystem.get("device_major_minor") or "")
    admitted_root = str(admitted_filesystem.get("filesystem_root") or "")
    if re.fullmatch(r"[0-9]+:[0-9]+", device) is None or admitted_root != "/":
        raise PreflightError("runner namespace admitted filesystem identity is invalid")
    parent_mount = _mount_evidence(
        volume_mount,
        run=run,
        label="runner namespace volume parent",
        expected_target=volume_mount,
        expected_device=device,
        expected_fstype="ext4",
        expected_fsroot=admitted_root,
        required_mode="ro",
    )
    observed_mounts["volume_parent"] = {
        **parent_mount,
        "logical_path": volume_mount.as_posix(),
    }
    volume_base_mount = _mount_evidence(
        volume_base,
        run=run,
        label="runner Prediction Markets volume base",
        target_may_be_ancestor=True,
        expected_device=device,
        expected_fstype="ext4",
        required_mode="ro",
    )
    observed_mounts["volume_base"] = {
        **volume_base_mount,
        "logical_path": volume_base.as_posix(),
    }
    _authenticate_volume_namespace_mapping(
        volume_base_mount,
        admitted_root=admitted_root,
        allowed_targets=(volume_mount, volume_base),
        label="runner Prediction Markets volume base",
        volume_mount=volume_mount,
    )
    home_mount = _mount_evidence(
        Path(_HOME_MOUNT.as_posix()),
        run=run,
        label="runner namespace /home root",
        target_may_be_ancestor=True,
        expected_fstype="ext4",
        required_mode="ro",
    )
    observed_mounts["home"] = {
        **home_mount,
        "logical_path": _HOME_MOUNT.as_posix(),
    }
    incoming_mount = _mount_evidence(
        incoming,
        run=run,
        label="runner namespace incoming root",
        target_may_be_ancestor=True,
        required_mode="ro",
    )
    observed_mounts["incoming"] = {
        **incoming_mount,
        "logical_path": incoming.as_posix(),
    }
    _authenticate_incoming_namespace_target(
        incoming_mount,
        home_evidence=home_mount,
        incoming=incoming,
        label="runner namespace incoming root",
    )
    readonly_mounts: dict[str, dict[str, object]] = {}
    for label, path in (("source", source), ("campaign", campaign)):
        readonly_mounts[label] = _mount_evidence(
            path,
            run=run,
            label=f"runner namespace {label} root",
            target_may_be_ancestor=True,
            expected_device=device,
            expected_fstype="ext4",
            required_mode="ro",
        )
        observed_mounts[label] = {
            **readonly_mounts[label],
            "logical_path": path.as_posix(),
        }
        _authenticate_volume_namespace_mapping(
            readonly_mounts[label],
            admitted_root=admitted_root,
            allowed_targets=(volume_mount, volume_base, path),
            label=f"runner namespace {label} root",
            volume_mount=volume_mount,
        )
    venue_expected_root = _expected_filesystem_root(
        admitted_root=admitted_root,
        volume_mount=volume_mount,
        path=venue_root,
    )
    venue_mount = _mount_evidence(
        venue_root,
        run=run,
        label="runner namespace venue root",
        expected_target=venue_root,
        expected_device=device,
        expected_fstype="ext4",
        expected_fsroot=venue_expected_root,
        required_mode="rw",
    )
    observed_mounts["venue"] = {
        **venue_mount,
        "logical_path": venue_root.as_posix(),
    }
    write_surface = write_probe(venue_root)
    venue_mount_after = _mount_evidence(
        venue_root,
        run=run,
        label="runner namespace venue root after durable probe",
        expected_target=venue_root,
        expected_device=device,
        expected_fstype="ext4",
        expected_fsroot=venue_expected_root,
        required_mode="rw",
    )
    roots_after = {
        "incoming": _canonical_directory(incoming, label="runner incoming root after probe"),
        "volume_base": _canonical_directory(
            volume_base, label="runner Prediction Markets volume base after probe"
        ),
        "source": _canonical_directory(source, label="runner source root after probe"),
        "campaign": _canonical_directory(campaign, label="runner campaign root after probe"),
        "venue": _canonical_directory(venue_root, label="runner venue root after probe"),
    }
    if roots_after != roots_before or venue_mount_after != venue_mount:
        raise PreflightError("runner namespace roots or venue mount changed during admission")
    return {
        "admitted_device_major_minor": device,
        "incoming_readonly": incoming_mount,
        "parent_mount": parent_mount,
        "readonly_roots": readonly_mounts,
        "volume_base_readonly": volume_base_mount,
        "venue": venue,
        "venue_mount": venue_mount,
        "write_surface": write_surface,
    }


def runner_namespace_admission(
    handoff_path: Path,
    install_admission_path: Path,
    *,
    venue: str,
    run: CommandRunner = _command,
    write_probe: WriteSurfaceProbe = _durable_write_surface_probe,
) -> dict[str, object]:
    """Prove the exact systemd namespace write surface before the collector process."""

    errors: list[str] = []
    checks: dict[str, object] = {}
    observed_mounts: dict[str, object] = {}
    try:
        handoff, layout, admitted_filesystem = _validated_install_admission(
            handoff_path,
            install_admission_path,
            operation="runner namespace guard",
        )
        checks = _runner_namespace_checks(
            handoff,
            layout,
            admitted_filesystem,
            venue=venue,
            run=run,
            write_probe=write_probe,
            observed_mounts=observed_mounts,
        )
    except (OSError, PreflightError, subprocess.SubprocessError, ValueError) as error:
        if isinstance(error, _MountEvidenceError):
            observed_mounts["rejected"] = error.evidence
        errors.append(str(error))
    return {
        "boundary": BOUNDARY,
        "checks": checks,
        "errors": errors,
        "namespace_admissible": not errors,
        "observed_mounts": observed_mounts,
        "recorded_at_utc": _utc_now_text(),
        "schema_version": 1,
        "terminal_signal": (
            "PREDICTION_RUNNER_NAMESPACE_GREEN"
            if not errors
            else "PREDICTION_RUNNER_NAMESPACE_REFUSED"
        ),
        "venue": venue,
    }


def authenticate_namespace_probe_completion(
    *,
    properties_text: str,
    journal_text: str,
    service: str,
    venue: str,
    campaign_root: Path,
    incoming_root: Path,
) -> dict[str, object]:
    """Bind successful oneshot properties to its exact fail-closed JSON receipt."""

    if venue not in {"polymarket", "kalshi"}:
        raise PreflightError("namespace probe venue is invalid")
    campaign = PurePosixPath(campaign_root.as_posix())
    incoming = PurePosixPath(incoming_root.as_posix())
    if (
        not campaign.is_absolute()
        or not incoming.is_absolute()
        or ".." in campaign.parts
        or ".." in incoming.parts
        or campaign.name != incoming.name
        or _RUN_SLUG.fullmatch(campaign.name) is None
    ):
        raise PreflightError("namespace probe attempt roots are invalid or divergent")
    expected_service = (
        f"hyperlab-pm-{campaign.name.removeprefix('pm-')}-"
        f"{venue}-namespace-probe.service"
    )
    if service != expected_service:
        raise PreflightError("namespace probe service identity diverged")
    if len(properties_text.encode("utf-8")) > 4096:
        raise PreflightError("namespace probe properties are oversized")
    expected_properties = {
        "ActiveState",
        "ExecMainCode",
        "ExecMainStatus",
        "FragmentPath",
        "LoadState",
        "MainPID",
        "NRestarts",
        "Result",
        "SubState",
    }
    properties: dict[str, str] = {}
    for line in properties_text.splitlines():
        if "=" not in line:
            raise PreflightError("namespace probe properties are malformed")
        key, value = line.split("=", 1)
        if key in properties:
            raise PreflightError("namespace probe property is duplicated")
        properties[key] = value
    if set(properties) != expected_properties:
        raise PreflightError("namespace probe property set diverged")

    def require_property(key: str, expected: str, label: str) -> None:
        if properties.get(key) != expected:
            raise PreflightError(f"namespace probe {label} diverged")

    require_property("LoadState", "loaded", "load-state")
    require_property("ActiveState", "inactive", "active-state")
    require_property("SubState", "dead", "sub-state")
    require_property("Result", "success", "result")
    require_property("MainPID", "0", "main-pid")
    require_property("NRestarts", "0", "restart-count")
    if properties["ExecMainCode"] not in {"0", "1"}:
        raise PreflightError("namespace probe exit-code-kind diverged")
    require_property("ExecMainStatus", "0", "exit-status")
    require_property(
        "FragmentPath",
        f"/etc/systemd/system/{service}",
        "fragment",
    )

    journal_raw = journal_text.encode("utf-8")
    if not journal_raw:
        raise PreflightError("namespace probe payload is absent")
    if len(journal_raw) > 65_536:
        raise PreflightError("namespace probe journal payload is oversized")
    candidates: list[tuple[str, dict[str, object]]] = []
    for line in journal_text.splitlines():
        try:
            candidate = json.loads(line)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if (
            isinstance(candidate, dict)
            and candidate.get("terminal_signal")
            in {
                "PREDICTION_RUNNER_NAMESPACE_GREEN",
                "PREDICTION_RUNNER_NAMESPACE_REFUSED",
            }
        ):
            candidates.append((line, candidate))
    if not candidates:
        raise PreflightError("namespace probe payload is absent or malformed")
    if len(candidates) != 1:
        raise PreflightError("namespace probe payload is ambiguous")
    payload_line, payload = candidates[0]
    if payload_line != canonical_json_bytes(payload).decode("utf-8"):
        raise PreflightError("namespace probe payload is not canonical JSON")
    expected_payload_fields = {
        "boundary",
        "checks",
        "errors",
        "namespace_admissible",
        "observed_mounts",
        "recorded_at_utc",
        "schema_version",
        "terminal_signal",
        "venue",
    }
    if set(payload) != expected_payload_fields:
        raise PreflightError("namespace probe payload field set diverged")
    if (
        payload.get("boundary") != BOUNDARY
        or payload.get("schema_version") != 1
        or payload.get("venue") != venue
    ):
        raise PreflightError("namespace probe payload identity diverged")
    recorded_at = payload.get("recorded_at_utc")
    if not isinstance(recorded_at, str):
        raise PreflightError("namespace probe payload timestamp is invalid")
    try:
        parsed_at = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise PreflightError("namespace probe payload timestamp is invalid") from error
    if parsed_at.tzinfo is None or parsed_at.utcoffset() != UTC.utcoffset(parsed_at):
        raise PreflightError("namespace probe payload timestamp is not UTC")
    if (
        payload.get("terminal_signal") != "PREDICTION_RUNNER_NAMESPACE_GREEN"
        or payload.get("namespace_admissible") is not True
        or payload.get("errors") != []
    ):
        raise PreflightError("namespace probe payload refused admission")
    checks = payload.get("checks")
    observed = payload.get("observed_mounts")
    if not isinstance(checks, dict) or not isinstance(observed, dict):
        raise PreflightError("namespace probe payload proofs are invalid")
    required_mounts = {
        "campaign",
        "home",
        "incoming",
        "source",
        "venue",
        "volume_base",
        "volume_parent",
    }
    if set(observed) != required_mounts:
        raise PreflightError("namespace probe observed mount set diverged")
    mount_fields = {
        "device_major_minor",
        "filesystem",
        "filesystem_root",
        "logical_path",
        "mount",
        "options",
        "source",
        "stat_device_major_minor",
    }
    for name in sorted(required_mounts):
        row = observed[name]
        if not isinstance(row, dict) or set(row) != mount_fields:
            raise PreflightError(f"namespace probe {name} mount proof is invalid")
        device = row.get("device_major_minor")
        options = row.get("options")
        if (
            row.get("filesystem") != "ext4"
            or not isinstance(device, str)
            or re.fullmatch(r"[0-9]+:[0-9]+", device) is None
            or row.get("stat_device_major_minor") != device
            or not isinstance(options, list)
            or not all(isinstance(item, str) for item in options)
        ):
            raise PreflightError(f"namespace probe {name} mount identity diverged")
        expected_mode = "rw" if name == "venue" else "ro"
        opposite = "ro" if expected_mode == "rw" else "rw"
        if expected_mode not in options or opposite in options:
            raise PreflightError(f"namespace probe {name} mount mode diverged")
    venue_root = (campaign / venue).as_posix()
    if observed["incoming"].get("logical_path") != incoming.as_posix():
        raise PreflightError("namespace probe incoming logical path diverged")
    if observed["venue"].get("logical_path") != venue_root:
        raise PreflightError("namespace probe venue logical path diverged")
    readonly = checks.get("readonly_roots")
    write_surface = checks.get("write_surface")
    if (
        checks.get("admitted_device_major_minor")
        != observed["venue"].get("device_major_minor")
        or checks.get("incoming_readonly") != observed["incoming"]
        or checks.get("parent_mount") != observed["volume_parent"]
        or checks.get("venue") != venue
        or checks.get("venue_mount") != observed["venue"]
        or checks.get("volume_base_readonly") != observed["volume_base"]
        or readonly
        != {
            "campaign": observed["campaign"],
            "source": observed["source"],
        }
        or not isinstance(write_surface, dict)
        or write_surface
        != {
            "directory_fsync": True,
            "exclusive_create": True,
            "file_fsync": True,
            "probe_removed": True,
            "root": venue_root,
        }
    ):
        raise PreflightError("namespace probe authenticated proof graph diverged")
    return {
        "authenticated": True,
        "boundary": BOUNDARY,
        "exec_main_code": properties["ExecMainCode"],
        "payload_sha256": sha256_bytes(canonical_json_bytes(payload)),
        "service": service,
        "terminal_signal": "PREDICTION_NAMESPACE_PROBE_COMPLETION_GREEN",
        "venue": venue,
    }


def runner_startup_admission(
    handoff_path: Path,
    install_admission_path: Path,
    *,
    venue: str,
    run: CommandRunner = _command,
    write_probe: WriteSurfaceProbe = _durable_write_surface_probe,
) -> dict[str, object]:
    """Re-authenticate clock, source, transfer and mount before a service run."""

    errors: list[str] = []
    checks: dict[str, object] = {}
    observed_mounts: dict[str, object] = {}
    try:
        handoff, layout, admitted_filesystem = _validated_install_admission(
            handoff_path,
            install_admission_path,
            operation="runner startup",
        )
        service_user = str(handoff.get("service_user") or "")
        if os.environ.get("USER") != service_user:
            raise PreflightError("runner startup must run as the dedicated service user")
        if Path.home().as_posix() != f"/home/{service_user}":
            raise PreflightError("runner startup service user HOME diverged")
        source = Path(str(layout["source_root"]))
        campaign = Path(str(layout["campaign_root"]))
        if (
            source.is_symlink()
            or campaign.is_symlink()
            or source.resolve(strict=True) != source
            or campaign.resolve(strict=True) != campaign
        ):
            raise PreflightError("runner startup roots are absent, symlinked, or non-canonical")
        runtime = source / ".venv" / "bin" / "python"
        if runtime.is_symlink() or not runtime.is_file():
            raise PreflightError("runner startup offline Python is absent or unsafe")
        transfer = verify_transfer_inventory(handoff_path.parent, handoff)
        source_commit = str(layout["source_commit"])
        source_inventory_sha256 = _validate_sha256(
            handoff.get("source_inventory_sha256"),
            label="runner startup source inventory hash",
        )
        identity_command = run(
            [
                str(runtime),
                "-I",
                str(handoff_path.parent / "scripts" / "launch_pack.py"),
                "verify-source",
                "--source-root",
                str(source),
                "--inventory",
                str(handoff_path.parent / "source-inventory.json"),
                "--expected-commit",
                source_commit,
            ]
        )
        if identity_command.returncode != 0:
            raise PreflightError(
                f"runner startup source identity failed: {identity_command.stderr}"
            )
        try:
            source_identity = json.loads(identity_command.stdout)
        except json.JSONDecodeError as error:
            raise PreflightError("runner startup source identity output is invalid") from error
        if (
            not isinstance(source_identity, dict)
            or set(source_identity) != {"commit", "files", "inventory_sha256", "status"}
            or source_identity.get("commit") != source_commit
            or type(source_identity.get("files")) is not int
            or int(source_identity["files"]) <= 0
            or source_identity.get("inventory_sha256") != source_inventory_sha256
            or source_identity.get("status") != "PREDICTION_SOURCE_IDENTITY_GREEN"
        ):
            raise PreflightError("runner startup source identity result diverged")
        runtime_import = _runtime_import_subprocess_admission(
            runtime=runtime,
            incoming_root=handoff_path.parent,
            source_root=source,
            source_commit=source_commit,
            inventory_sha256=source_inventory_sha256,
            run=run,
        )
        ntp = run(["timedatectl", "show", "--property=NTPSynchronized", "--value"])
        if ntp.returncode != 0 or ntp.stdout != "yes":
            raise PreflightError("NTP is not synchronized for runner startup")
        namespace = _runner_namespace_checks(
            handoff,
            layout,
            admitted_filesystem,
            venue=venue,
            run=run,
            write_probe=write_probe,
            observed_mounts=observed_mounts,
        )
        checks = {
            "capacity": {"deferred_to_ledger_accounted_runner_gate": True},
            "namespace": namespace,
            "ntp": {"synchronized": True},
            "runtime_import_admission_sha256": runtime_import["admission_sha256"],
            "roots": {"campaign": str(campaign), "source": str(source)},
            "source_identity": source_identity,
            "transfer_identity": transfer,
        }
    except (OSError, PreflightError, subprocess.SubprocessError, ValueError) as error:
        if isinstance(error, _MountEvidenceError):
            observed_mounts["rejected"] = error.evidence
        errors.append(str(error))
    return {
        "boundary": BOUNDARY,
        "checks": checks,
        "errors": errors,
        "observed_mounts": observed_mounts,
        "recorded_at_utc": _utc_now_text(),
        "schema_version": 1,
        "startup_admissible": not errors,
        "terminal_signal": (
            "PREDICTION_RUNNER_STARTUP_ADMISSION_GREEN"
            if not errors
            else "PREDICTION_RUNNER_STARTUP_ADMISSION_REFUSED"
        ),
    }


def recovery_initial_admission(handoff_path: Path) -> dict[str, object]:
    """Authenticate why a venue may legitimately have no runtime state yet."""

    handoff = load_handoff(handoff_path)
    campaign_root = Path(str(handoff.get("campaign_root") or ""))
    if not campaign_root.is_absolute():
        raise PreflightError("recovery campaign root is not absolute")
    state_root = campaign_root / "state"
    manifest_path = campaign_root / "campaign-manifest.json"
    manifest_raw = _safe_regular_bytes(manifest_path)
    manifest = _object(manifest_path)
    manifest_pin = (
        _safe_regular_bytes(campaign_root / "campaign-manifest.sha256", maximum_bytes=256)
        .decode("ascii")
        .strip()
        .split()
    )
    claimed_manifest = _validate_sha256(
        manifest.get("manifest_sha256"), label="campaign manifest logical hash"
    )
    manifest_body = {
        key: value for key, value in manifest.items() if key != "manifest_sha256"
    }
    if (
        manifest_raw != canonical_json_bytes(manifest) + b"\n"
        or sha256_bytes(canonical_json_bytes(manifest_body)) != claimed_manifest
        or len(manifest_pin) != 2
        or manifest_pin[1] != "campaign-manifest.json"
        or sha256_bytes(manifest_raw) != manifest_pin[0]
    ):
        raise PreflightError("campaign manifest authentication failed during recovery")
    activation_path = state_root / "activation-receipt.json"
    preflight_path = state_root / "preflight-report.json"
    activation_raw = _safe_regular_bytes(activation_path)
    preflight_raw = _safe_regular_bytes(preflight_path)
    try:
        activation = json.loads(activation_raw.decode("utf-8"))
        preflight = json.loads(preflight_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreflightError("recovery admission JSON is invalid") from error
    if not isinstance(activation, dict) or not isinstance(preflight, dict):
        raise PreflightError("recovery admission roots must be objects")
    if activation_raw != canonical_json_bytes(activation) + b"\n":
        raise PreflightError("activation receipt is not canonical JSON with LF")
    if preflight_raw != canonical_json_bytes(preflight) + b"\n":
        raise PreflightError("initial preflight report is not canonical JSON with LF")
    expected_activation_fields = {
        "boundary",
        "campaign_id",
        "campaign_manifest_sha256",
        "campaign_root",
        "dashboard_port",
        "eligible_venues",
        "economic_evidence_status",
        "h1_actions",
        "preflight_report_sha256",
        "quick_start",
        "receipt_sha256",
        "recorded_at_utc",
        "schema_version",
        "source_commit",
        "starts_at_utc",
    }
    if set(activation) != expected_activation_fields:
        raise PreflightError("activation receipt fields diverged")
    claimed_receipt = _validate_sha256(
        activation.get("receipt_sha256"), label="activation receipt hash"
    )
    activation_body = {
        key: value for key, value in activation.items() if key != "receipt_sha256"
    }
    if sha256_bytes(canonical_json_bytes(activation_body)) != claimed_receipt:
        raise PreflightError("activation receipt self-hash diverged")
    if (
        activation.get("boundary") != BOUNDARY
        or activation.get("schema_version") != 1
        or activation.get("campaign_id") != manifest.get("campaign_id")
        or activation.get("campaign_manifest_sha256") != claimed_manifest
        or activation.get("starts_at_utc") != manifest.get("starts_at_utc")
        or activation.get("campaign_root") != str(campaign_root)
        or activation.get("source_commit") != handoff.get("source_commit")
        or activation.get("dashboard_port") != handoff.get("dashboard_port")
        or activation.get("economic_evidence_status")
        != "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE"
        or activation.get("h1_actions") != "NONE"
        or type(activation.get("quick_start")) is not bool
    ):
        raise PreflightError("activation receipt binding diverged")
    for field in ("recorded_at_utc", "starts_at_utc"):
        value = activation.get(field)
        if not isinstance(value, str):
            raise PreflightError(f"activation {field} is absent")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise PreflightError(f"activation {field} is invalid") from error
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise PreflightError(f"activation {field} is not timezone-aware")
    expected_preflight_sha = _validate_sha256(
        activation.get("preflight_report_sha256"),
        label="activation preflight report hash",
    )
    if sha256_bytes(preflight_raw) != expected_preflight_sha:
        raise PreflightError("initial preflight report hash diverged")
    eligible = activation.get("eligible_venues")
    if (
        not isinstance(eligible, list)
        or any(
            not isinstance(item, str) or item not in {"polymarket", "kalshi"}
            for item in eligible
        )
        or len(set(eligible)) != len(eligible)
        or preflight.get("eligible_venues") != eligible
        or preflight.get("boundary") != BOUNDARY
        or preflight.get("installation_admissible") is not True
        or preflight.get("terminal_signal") != "PREDICTION_HOST_PREFLIGHT_GREEN"
    ):
        raise PreflightError("initial preflight admission fields diverged")
    network = preflight.get("network")
    if not isinstance(network, dict) or set(network) != {"polymarket", "kalshi"}:
        raise PreflightError("initial per-venue network admission is absent")
    admission: dict[str, object] = {}
    for venue in ("polymarket", "kalshi"):
        record = network.get(venue)
        if not isinstance(record, dict) or not isinstance(record.get("verdict"), str):
            raise PreflightError(f"initial {venue} network verdict is invalid")
        verdict = record["verdict"]
        admitted = venue in eligible
        if admitted != (verdict == "NETWORK_PREFLIGHT_GREEN"):
            raise PreflightError(f"initial {venue} eligibility and network verdict diverged")
        if not admitted and verdict != "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT":
            raise PreflightError(f"initial {venue} refusal is not a public-source verdict")
        admission[venue] = {"eligible": admitted, "network_verdict": verdict}
    return {
        "admission_by_venue": admission,
        "boundary": BOUNDARY,
        "campaign_root": str(campaign_root),
        "schema_version": 1,
        "source_commit": handoff["source_commit"],
        "terminal_signal": "PREDICTION_RECOVERY_INITIAL_ADMISSION_AUTHENTICATED",
    }


_RECOVERY_NETWORK_ADMISSION_FIELDS = {
    "boundary",
    "campaign_id",
    "campaign_manifest_sha256",
    "campaign_root",
    "handoff_sha256",
    "initial_preflight_report_sha256",
    "network_report",
    "network_report_sha256",
    "receipt_sha256",
    "recorded_at_utc",
    "schema_version",
    "source_commit",
    "source_root",
    "terminal_signal",
    "venue",
}


def validate_recovery_network_admission(
    record: Mapping[str, object],
    *,
    handoff: Mapping[str, object],
    handoff_sha256: str,
    initial_preflight_sha256: str,
    campaign_id: str,
    campaign_manifest_sha256: str,
    venue: str,
) -> None:
    if set(record) != _RECOVERY_NETWORK_ADMISSION_FIELDS:
        raise PreflightError("recovery network admission fields diverged")
    claimed_receipt = _validate_sha256(
        record.get("receipt_sha256"), label="recovery admission receipt hash"
    )
    body = {key: value for key, value in record.items() if key != "receipt_sha256"}
    network = record.get("network_report")
    network_sha256 = _validate_sha256(
        record.get("network_report_sha256"), label="recovery network report hash"
    )
    recorded_at = record.get("recorded_at_utc")
    try:
        parsed_at = datetime.fromisoformat(str(recorded_at).replace("Z", "+00:00"))
    except ValueError as error:
        raise PreflightError("recovery network admission time is invalid") from error
    if (
        sha256_bytes(canonical_json_bytes(body)) != claimed_receipt
        or not isinstance(network, Mapping)
        or network.get("venue") != venue
        or network.get("verdict") != "NETWORK_PREFLIGHT_GREEN"
        or sha256_bytes(canonical_json_bytes(network) + b"\n") != network_sha256
        or record.get("boundary") != BOUNDARY
        or record.get("campaign_id") != campaign_id
        or record.get("campaign_manifest_sha256") != campaign_manifest_sha256
        or record.get("campaign_root") != handoff.get("campaign_root")
        or record.get("handoff_sha256") != handoff_sha256
        or record.get("initial_preflight_report_sha256") != initial_preflight_sha256
        or record.get("schema_version") != 1
        or record.get("source_commit") != handoff.get("source_commit")
        or record.get("source_root") != handoff.get("source_root")
        or record.get("terminal_signal")
        != "PREDICTION_RECOVERY_NETWORK_ADMISSION_AUTHENTICATED"
        or record.get("venue") != venue
        or not isinstance(recorded_at, str)
        or parsed_at.tzinfo is None
        or parsed_at.utcoffset() is None
    ):
        raise PreflightError("recovery network admission binding diverged")


def recovery_network_admission(
    handoff_path: Path,
    network_report_path: Path,
    *,
    venue: str,
    output_path: Path,
) -> dict[str, object]:
    if venue not in {"polymarket", "kalshi"}:
        raise PreflightError("recovery admission venue is invalid")
    handoff_raw = _safe_regular_bytes(handoff_path)
    handoff = load_handoff(handoff_path)
    campaign_root = Path(str(handoff.get("campaign_root") or "")).resolve(strict=True)
    expected_output = campaign_root / "state" / f"recovery-admission-{venue}.json"
    if output_path != expected_output or output_path.parent.is_symlink():
        raise PreflightError("recovery admission output escaped its fixed campaign state path")
    preflight_path = campaign_root / "state" / "preflight-report.json"
    preflight_raw = _safe_regular_bytes(preflight_path)
    preflight = _object(preflight_path)
    initial_network = preflight.get("network")
    eligible = preflight.get("eligible_venues")
    if (
        not isinstance(initial_network, Mapping)
        or not isinstance(initial_network.get(venue), Mapping)
        or initial_network[venue].get("verdict") != "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT"
        or not isinstance(eligible, list)
        or venue in eligible
    ):
        raise PreflightError("recovery admission is not for an initially unavailable venue")
    network_raw = _safe_regular_bytes(network_report_path)
    network = _object(network_report_path)
    if network_raw != canonical_json_bytes(network) + b"\n":
        raise PreflightError("recovery network report is not canonical JSON with LF")
    if network.get("venue") != venue or network.get("verdict") != "NETWORK_PREFLIGHT_GREEN":
        raise PreflightError("recovery network report is not a GREEN verdict for the venue")
    manifest_path = campaign_root / "campaign-manifest.json"
    manifest = _object(manifest_path)
    claimed_manifest = _validate_sha256(
        manifest.get("manifest_sha256"), label="campaign manifest logical hash"
    )
    manifest_body = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    if sha256_bytes(canonical_json_bytes(manifest_body)) != claimed_manifest:
        raise PreflightError("campaign manifest logical hash diverged during recovery admission")
    handoff_sha256 = sha256_bytes(handoff_raw)
    initial_preflight_sha256 = sha256_bytes(preflight_raw)
    campaign_id = str(manifest.get("campaign_id") or "")
    if output_path.exists() or output_path.is_symlink():
        existing_raw = _safe_regular_bytes(output_path)
        existing = _object(output_path)
        if existing_raw != canonical_json_bytes(existing) + b"\n":
            raise PreflightError("recovery network admission is not canonical JSON with LF")
        validate_recovery_network_admission(
            existing,
            handoff=handoff,
            handoff_sha256=handoff_sha256,
            initial_preflight_sha256=initial_preflight_sha256,
            campaign_id=campaign_id,
            campaign_manifest_sha256=claimed_manifest,
            venue=venue,
        )
        return existing
    body: dict[str, object] = {
        "boundary": BOUNDARY,
        "campaign_id": campaign_id,
        "campaign_manifest_sha256": claimed_manifest,
        "campaign_root": str(campaign_root),
        "handoff_sha256": handoff_sha256,
        "initial_preflight_report_sha256": initial_preflight_sha256,
        "network_report": network,
        "network_report_sha256": sha256_bytes(network_raw),
        "recorded_at_utc": _utc_now_text(),
        "schema_version": 1,
        "source_commit": handoff["source_commit"],
        "source_root": handoff["source_root"],
        "terminal_signal": "PREDICTION_RECOVERY_NETWORK_ADMISSION_AUTHENTICATED",
        "venue": venue,
    }
    record = {**body, "receipt_sha256": sha256_bytes(canonical_json_bytes(body))}
    validate_recovery_network_admission(
        record,
        handoff=handoff,
        handoff_sha256=handoff_sha256,
        initial_preflight_sha256=initial_preflight_sha256,
        campaign_id=campaign_id,
        campaign_manifest_sha256=claimed_manifest,
        venue=venue,
    )
    _atomic_report(output_path, record)
    return record


def _atomic_report(path: Path, report: Mapping[str, object]) -> None:
    if path.exists():
        raise PreflightError("preflight report path must be new")
    payload = canonical_json_bytes(report) + b"\n"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb", closefd=True) as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    if os.name != "nt":
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prediction Markets fail-closed target preflight")
    subparsers = parser.add_subparsers(dest="command", required=True)
    host = subparsers.add_parser("host")
    host.add_argument("--handoff", type=Path, required=True)
    host.add_argument("--report", type=Path, required=True)
    fsync = subparsers.add_parser("fsync")
    fsync.add_argument("--handoff", type=Path, required=True)
    fsync.add_argument("--report", type=Path, required=True)
    network = subparsers.add_parser("network")
    network.add_argument("--venue", choices=("polymarket", "kalshi"), required=True)
    network.add_argument("--report", type=Path, required=True)
    resume = subparsers.add_parser("resume")
    resume.add_argument("--handoff", type=Path, required=True)
    resume.add_argument("--report", type=Path, required=True)
    install_admission = subparsers.add_parser("install-admission")
    install_admission.add_argument("--handoff", type=Path, required=True)
    install_admission.add_argument("--host-report", type=Path, required=True)
    install_admission.add_argument("--fsync-report", type=Path, required=True)
    install_admission.add_argument("--report", type=Path, required=True)
    activation_guard = subparsers.add_parser("collector-activation-guard")
    activation_guard.add_argument("--handoff", type=Path, required=True)
    activation_guard.add_argument("--install-admission-report", type=Path, required=True)
    activation_guard.add_argument("--report", type=Path, required=True)
    namespace_guard = subparsers.add_parser("runner-namespace-guard")
    namespace_guard.add_argument("--handoff", type=Path, required=True)
    namespace_guard.add_argument("--install-admission-report", type=Path, required=True)
    namespace_guard.add_argument(
        "--venue", choices=("polymarket", "kalshi"), required=True
    )
    namespace_completion = subparsers.add_parser(
        "authenticate-namespace-probe-completion"
    )
    namespace_completion.add_argument("--service", required=True)
    namespace_completion.add_argument(
        "--venue", choices=("polymarket", "kalshi"), required=True
    )
    namespace_completion.add_argument("--campaign-root", type=Path, required=True)
    namespace_completion.add_argument("--incoming-root", type=Path, required=True)
    recovery_admission = subparsers.add_parser("recovery-admission")
    recovery_admission.add_argument("--handoff", type=Path, required=True)
    recovery_admission.add_argument("--report", type=Path, required=True)
    recovery_network = subparsers.add_parser("recovery-network-admit")
    recovery_network.add_argument("--handoff", type=Path, required=True)
    recovery_network.add_argument("--network-report", type=Path, required=True)
    recovery_network.add_argument(
        "--venue", choices=("polymarket", "kalshi"), required=True
    )
    recovery_network.add_argument("--report", type=Path, required=True)
    runtime_import = subparsers.add_parser("runtime-import-admission")
    runtime_import.add_argument("--handoff", type=Path, required=True)
    runtime_import.add_argument("--source-root", type=Path, required=True)
    runtime_import.add_argument("--source-inventory", type=Path, required=True)
    runtime_import.add_argument("--report", type=Path)
    superseded_runtime = subparsers.add_parser(
        "superseded-runtime-compatibility"
    )
    superseded_runtime.add_argument("--candidate-handoff", type=Path, required=True)
    superseded_runtime.add_argument("--candidate-source-root", type=Path, required=True)
    superseded_runtime.add_argument(
        "--candidate-source-inventory", type=Path, required=True
    )
    superseded_runtime.add_argument("--report", type=Path)
    return parser


def _fail(message: str) -> NoReturn:
    print(f"PREDICTION_PREFLIGHT_REFUSED:{message}", file=sys.stderr)
    raise SystemExit(4)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "superseded-runtime-compatibility":
            report = superseded_runtime_compatibility(
                arguments.candidate_handoff,
                arguments.candidate_source_root,
                arguments.candidate_source_inventory,
            )
            if arguments.report is not None:
                expected_report = (
                    arguments.candidate_handoff.parent
                    / "superseded-runtime-compatibility.json"
                )
                if arguments.report != expected_report:
                    raise PreflightError(
                        "superseded compatibility report escaped its fixed incoming path"
                    )
                _atomic_report(arguments.report, report)
            print(canonical_json_bytes(report).decode("utf-8"))
            return 0
        if arguments.command == "runtime-import-admission":
            report = runtime_import_admission(
                arguments.handoff,
                arguments.source_root,
                arguments.source_inventory,
            )
            if arguments.report is not None:
                expected_report = arguments.handoff.parent / "runtime-import-admission.json"
                if arguments.report != expected_report:
                    raise PreflightError(
                        "runtime import admission report escaped its fixed incoming path"
                    )
                _atomic_report(arguments.report, report)
            print(canonical_json_bytes(report).decode("utf-8"))
            return 0
        if arguments.command == "recovery-network-admit":
            report = recovery_network_admission(
                arguments.handoff,
                arguments.network_report,
                venue=arguments.venue,
                output_path=arguments.report,
            )
            print(canonical_json_bytes(report).decode("utf-8"))
            return 0
        if arguments.command == "runner-namespace-guard":
            report = runner_namespace_admission(
                arguments.handoff,
                arguments.install_admission_report,
                venue=arguments.venue,
            )
            print(canonical_json_bytes(report).decode("utf-8"))
            return 0 if report["namespace_admissible"] is True else 4
        if arguments.command == "authenticate-namespace-probe-completion":
            report = authenticate_namespace_probe_completion(
                properties_text=os.environ.get(
                    "HYPERLAB_NAMESPACE_PROBE_PROPERTIES", ""
                ),
                journal_text=os.environ.get("HYPERLAB_NAMESPACE_PROBE_JOURNAL", ""),
                service=arguments.service,
                venue=arguments.venue,
                campaign_root=arguments.campaign_root,
                incoming_root=arguments.incoming_root,
            )
            print(canonical_json_bytes(report).decode("utf-8"))
            return 0
        if arguments.command == "install-admission":
            report = install_admission_preflight(
                arguments.handoff,
                arguments.host_report,
                arguments.fsync_report,
            )
        elif arguments.command == "collector-activation-guard":
            report = collector_activation_guard(
                arguments.handoff,
                arguments.install_admission_report,
            )
        elif arguments.command == "host":
            report = host_preflight(arguments.handoff)
        elif arguments.command == "fsync":
            report = fsync_probe(arguments.handoff)
        elif arguments.command == "resume":
            report = resume_preflight(arguments.handoff)
        elif arguments.command == "recovery-admission":
            report = recovery_initial_admission(arguments.handoff)
        else:
            report = probe_venue_connectivity(arguments.venue)
        _atomic_report(arguments.report, report)
        print(canonical_json_bytes(report).decode("utf-8"))
        if arguments.command == "host" and report["installation_admissible"] is not True:
            return 4
        if arguments.command == "network" and report["verdict"] != "NETWORK_PREFLIGHT_GREEN":
            return 4
        if arguments.command == "resume" and report["resume_admissible"] is not True:
            return 4
        if arguments.command == "install-admission" and report["install_admissible"] is not True:
            return 4
        if (
            arguments.command == "collector-activation-guard"
            and report["activation_admissible"] is not True
        ):
            return 4
        return 0
    except (OSError, PreflightError, subprocess.SubprocessError) as error:
        _fail(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
