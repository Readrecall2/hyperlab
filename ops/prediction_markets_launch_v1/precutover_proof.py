from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import secrets
import shutil
import stat
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn

try:
    import pwd
except ImportError:  # pragma: no cover - Windows pack verification path
    pwd = None  # type: ignore[assignment]

BOUNDARY = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
PROOF_ID = "prediction-markets-linux-precutover-proof-v1"
EXPECTED_PROOF_BRANCH = "codex/prediction-markets-v3-independent-audit"
TERMINAL_SIGNAL = "PREDICTION_RUNTIME_PREPARED_BEFORE_CUTOVER"
TRANSFER_SIGNAL = "PREDICTION_LINUX_PRECUTOVER_PROOF_WINDOWS_TRANSFER_VERIFIED"
RETRIEVAL_SIGNAL = (
    "PREDICTION_WINDOWS_LINUX_PRECUTOVER_PROOF_RETRIEVED_AUTHENTICATED"
)
INPUT_INVENTORY = "proof-input-inventory.json"
INPUT_INVENTORY_PIN = "proof-input-inventory.sha256"
PROOF_MANIFEST = "proof-manifest.json"
PROOF_MANIFEST_PIN = "proof-manifest.sha256"
RUNTIME_REPORT = "runtime-import-admission.json"
REPORT = "linux-precutover-proof-report.json"
REPORT_PIN = "linux-precutover-proof-report.sha256"
OUTPUT_INVENTORY = "linux-precutover-proof-output-inventory.json"
OUTPUT_INVENTORY_PIN = "linux-precutover-proof-output-inventory.sha256"
PROGRESS_LOG = "linux-precutover-proof-progress.log"
NEUTRAL_CWD = "linux-precutover-neutral-cwd"
_BUNDLE_NAME = "hyperlab-prediction-markets-runtime-data-quality-v1.bundle"
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_RUN_SLUG = re.compile(r"^pm-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}$")
_SAFE_RELATIVE = re.compile(
    r"^(?!/)(?!.*(?:^|/)\.\.(?:/|$))(?!.*//)[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*$"
)
_SHELL_SHEBANG = b"#!/usr/bin/env bash\n"
_EXPECTED_SOURCE_MODULES = {
    "hyperlab": "src/hyperlab/__init__.py",
    "ops.prediction_markets_launch_v1.cockpit": (
        "ops/prediction_markets_launch_v1/cockpit.py"
    ),
    "ops.prediction_markets_launch_v1.preflight": (
        "ops/prediction_markets_launch_v1/preflight.py"
    ),
    "ops.prediction_markets_launch_v1.runner": (
        "ops/prediction_markets_launch_v1/runner.py"
    ),
}
_EXPECTED_VENV_MODULES = {"fastapi", "requests", "uvicorn", "websocket"}
_FIXED_INPUT_FILES = {
    "README.md",
    _BUNDLE_NAME,
    "handoff.json",
    "handoff.sha256",
    "source-inventory.json",
    "wheelhouse.sha256",
    PROOF_MANIFEST,
    PROOF_MANIFEST_PIN,
    "scripts/bootstrap-offline.sh",
    "scripts/launch_pack.py",
    "scripts/precutover_proof.py",
    "scripts/preflight.py",
    "operator/A-windows-verify-transfer.ps1",
    "operator/B0-linux-precutover-proof.sh",
    "operator/C0-windows-retrieve-authenticate.ps1",
}
_ALLOWED_OUTPUT_FILES = {
    PROGRESS_LOG,
    RUNTIME_REPORT,
    REPORT,
    REPORT_PIN,
    OUTPUT_INVENTORY,
    OUTPUT_INVENTORY_PIN,
}


class ProofError(RuntimeError):
    """Fail-closed Linux pre-cutover proof error."""


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


def sha256_file(path: Path) -> str:
    return sha256_bytes(_safe_regular_bytes(path, maximum_bytes=2 * 1024**3))


def _utc_now_text() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _safe_regular_bytes(path: Path, *, maximum_bytes: int = 16 * 1024**2) -> bytes:
    try:
        before = path.lstat()
    except OSError as error:
        raise ProofError(f"required file is unreadable: {path}") from error
    if path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size > maximum_bytes:
        raise ProofError(f"required file is unsafe or oversized: {path}")
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
    ) or len(raw) != before.st_size:
        raise ProofError(f"file changed while authenticated: {path}")
    return raw


def _canonical_object(path: Path) -> tuple[dict[str, Any], bytes]:
    raw = _safe_regular_bytes(path)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProofError(f"invalid JSON object: {path}") from error
    if not isinstance(value, dict) or raw != canonical_json_bytes(value) + b"\n":
        raise ProofError(f"JSON object is not canonical with final LF: {path}")
    return value, raw


def _pinned_object(path: Path, pin_path: Path) -> tuple[dict[str, Any], bytes, str]:
    value, raw = _canonical_object(path)
    fields = _safe_regular_bytes(pin_path, maximum_bytes=256).decode("ascii").strip().split()
    digest = sha256_bytes(raw)
    if len(fields) != 2 or fields[0] != digest or fields[1] != path.name:
        raise ProofError(f"pinned object diverged: {path}")
    return value, raw, digest


def _write_new(path: Path, payload: bytes, *, mode: int = 0o600) -> None:
    if path.exists() or path.is_symlink():
        raise ProofError(f"output path must be new: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
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


def _write_json_new(path: Path, value: object) -> None:
    _write_new(path, canonical_json_bytes(value) + b"\n")


def _write_pin(path: Path, target: Path) -> None:
    _write_new(path, f"{sha256_file(target)}  {target.name}\n".encode("ascii"))


def _command(
    arguments: Sequence[str],
    *,
    cwd: Path | None = None,
    timeout_seconds: int = 180,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        list(arguments),
        capture_output=True,
        check=False,
        cwd=cwd,
        text=True,
        timeout=timeout_seconds,
    )
    if completed.returncode != 0:
        diagnostic = completed.stderr.strip() or completed.stdout.strip()
        raise ProofError(
            f"command failed ({completed.returncode}): {arguments[0]}:{diagnostic}"
        )
    return completed


def _git(repo_root: Path, *arguments: str) -> str:
    return _command(["git", "-C", str(repo_root), *arguments]).stdout.strip()


def _exact_directory(path: Path, *, label: str) -> Path:
    if not path.is_absolute():
        raise ProofError(f"{label} is not absolute")
    try:
        identity = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise ProofError(f"{label} is absent or unreadable") from error
    if path.is_symlink() or not stat.S_ISDIR(identity.st_mode) or resolved != path:
        raise ProofError(f"{label} is symlinked, special, or non-canonical")
    return resolved


def _exact_file(path: Path, *, label: str) -> Path:
    if not path.is_absolute():
        raise ProofError(f"{label} is not absolute")
    try:
        identity = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise ProofError(f"{label} is absent or unreadable") from error
    if path.is_symlink() or not stat.S_ISREG(identity.st_mode) or resolved != path:
        raise ProofError(f"{label} is symlinked, special, or non-canonical")
    return resolved


def _validate_shell_payload(payload: bytes, *, label: str) -> None:
    if not payload.startswith(_SHELL_SHEBANG):
        raise ProofError(f"shell shebang diverged: {label}")
    if payload.startswith(b"\xef\xbb\xbf") or b"\x00" in payload or b"\r" in payload:
        raise ProofError(f"shell bytes are unsafe: {label}")
    try:
        payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ProofError(f"shell is not strict UTF-8: {label}") from error
    if not payload.endswith(b"\n"):
        raise ProofError(f"shell lacks final LF: {label}")


def _validate_sha(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ProofError(f"{label} is not a lowercase SHA-256")
    return value


def _proof_manifest(root: Path) -> tuple[dict[str, Any], str]:
    manifest, _raw, digest = _pinned_object(
        root / PROOF_MANIFEST,
        root / PROOF_MANIFEST_PIN,
    )
    expected_fields = {
        "boundary",
        "bundle_filename",
        "bundle_ref",
        "bundle_sha256",
        "campaign_root",
        "expected_branch",
        "handoff_sha256",
        "incoming_root",
        "input_inventory_filename",
        "proof_id",
        "run_slug",
        "schema_version",
        "source_commit",
        "source_inventory_file_sha256",
        "source_inventory_sha256",
        "source_root",
        "terminal_signal",
        "volume_base",
        "volume_mount",
        "wheelhouse_manifest_sha256",
    }
    if set(manifest) != expected_fields:
        raise ProofError("proof manifest fields diverged")
    slug = manifest.get("run_slug")
    commit = manifest.get("source_commit")
    branch = manifest.get("expected_branch")
    if (
        manifest.get("boundary") != BOUNDARY
        or manifest.get("proof_id") != PROOF_ID
        or manifest.get("schema_version") != 1
        or manifest.get("terminal_signal") != TERMINAL_SIGNAL
        or manifest.get("bundle_filename") != _BUNDLE_NAME
        or manifest.get("input_inventory_filename") != INPUT_INVENTORY
        or not isinstance(slug, str)
        or _RUN_SLUG.fullmatch(slug) is None
        or not isinstance(commit, str)
        or _COMMIT.fullmatch(commit) is None
        or not isinstance(branch, str)
        or branch != EXPECTED_PROOF_BRANCH
        or manifest.get("bundle_ref") != f"refs/heads/{branch}"
    ):
        raise ProofError("proof manifest identity diverged")
    expected_volume_mount = "/mnt/HC_Volume_106716684"
    expected_volume_base = f"{expected_volume_mount}/hyperlab-prediction-markets"
    if (
        manifest.get("incoming_root")
        != f"/home/hyperlab/hyperlab-prediction-markets/incoming/{slug}"
        or manifest.get("volume_mount") != expected_volume_mount
        or manifest.get("volume_base") != expected_volume_base
        or manifest.get("source_root") != f"{expected_volume_base}/sources/{slug}"
        or manifest.get("campaign_root") != f"{expected_volume_base}/campaigns/{slug}"
    ):
        raise ProofError("proof manifest root layout diverged")
    for field in (
        "bundle_sha256",
        "handoff_sha256",
        "source_inventory_file_sha256",
        "source_inventory_sha256",
        "wheelhouse_manifest_sha256",
    ):
        _validate_sha(manifest.get(field), label=field)
    return manifest, digest


def _inventory_rows(value: Mapping[str, object]) -> list[dict[str, object]]:
    if set(value) != {"files", "schema_version"} or value.get("schema_version") != 1:
        raise ProofError("inventory schema diverged")
    rows = value.get("files")
    if not isinstance(rows, list) or not rows:
        raise ProofError("inventory is empty")
    normalized: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"path", "sha256", "size"}:
            raise ProofError("inventory entry schema diverged")
        relative = row.get("path")
        digest = row.get("sha256")
        size = row.get("size")
        if (
            not isinstance(relative, str)
            or _SAFE_RELATIVE.fullmatch(relative) is None
            or relative in seen
            or not isinstance(digest, str)
            or _SHA256.fullmatch(digest) is None
            or type(size) is not int
            or size < 0
        ):
            raise ProofError("inventory entry is unsafe or invalid")
        seen.add(relative)
        normalized.append(row)
    if [str(row["path"]) for row in normalized] != sorted(seen):
        raise ProofError("inventory paths are not sorted")
    return normalized


def _verify_inventory(
    root: Path,
    inventory_path: Path,
    pin_path: Path,
) -> tuple[list[dict[str, object]], str]:
    value, _raw, digest = _pinned_object(inventory_path, pin_path)
    rows = _inventory_rows(value)
    for row in rows:
        relative = str(row["path"])
        target = root.joinpath(*PurePosixPath(relative).parts)
        try:
            if target.resolve(strict=True) != target:
                raise ProofError(f"inventory path is not canonical: {relative}")
        except OSError as error:
            raise ProofError(f"inventory path is unreadable: {relative}") from error
        payload = _safe_regular_bytes(target, maximum_bytes=2 * 1024**3)
        if len(payload) != row["size"] or sha256_bytes(payload) != row["sha256"]:
            raise ProofError(f"inventory file identity diverged: {relative}")
        if relative.endswith(".sh"):
            _validate_shell_payload(payload, label=relative)
    return rows, digest


def _verify_wheelhouse(root: Path, manifest_digest: str) -> int:
    manifest_path = root / "wheelhouse.sha256"
    raw = _safe_regular_bytes(manifest_path, maximum_bytes=1024**2)
    if sha256_bytes(raw) != manifest_digest:
        raise ProofError("wheelhouse manifest hash diverged")
    declared: set[str] = set()
    for line in raw.decode("ascii").splitlines():
        fields = line.split("  ", 1)
        if len(fields) != 2 or _SHA256.fullmatch(fields[0]) is None:
            raise ProofError("wheelhouse manifest line is malformed")
        name = fields[1]
        if Path(name).name != name or not name.endswith(".whl") or name in declared:
            raise ProofError("wheelhouse manifest filename is unsafe")
        wheel = root / "wheelhouse" / name
        if sha256_file(wheel) != fields[0]:
            raise ProofError(f"wheelhouse file hash diverged: {name}")
        declared.add(name)
    entries = list((root / "wheelhouse").iterdir())
    if any(item.is_symlink() or not item.is_file() for item in entries):
        raise ProofError("wheelhouse contains a linked or non-file entry")
    actual = {item.name for item in entries}
    if not declared or actual != declared:
        raise ProofError("wheelhouse contains missing, extra, or unsafe entries")
    return len(declared)


def _verify_bundle(root: Path, manifest: Mapping[str, object]) -> dict[str, object]:
    bundle = root / str(manifest["bundle_filename"])
    if sha256_file(bundle) != manifest["bundle_sha256"]:
        raise ProofError("Git bundle SHA-256 diverged")
    heads = _command(["git", "bundle", "list-heads", str(bundle)]).stdout.splitlines()
    expected = f"{manifest['source_commit']} {manifest['bundle_ref']}"
    if heads != [expected]:
        raise ProofError("Git bundle ref or commit diverged")
    verify_root = root / f".proof-bundle-verify.{secrets.token_hex(8)}"
    if verify_root.exists() or verify_root.is_symlink():
        raise ProofError("bundle verifier root must be new")
    verify_root.mkdir()
    verify_root = verify_root.resolve(strict=True)
    try:
        _command(["git", "init", "--bare", "--quiet", str(verify_root)])
        _command(["git", "-C", str(verify_root), "bundle", "verify", str(bundle)])
    finally:
        expected_parent = root.resolve(strict=True)
        if verify_root.parent != expected_parent or not verify_root.name.startswith(
            ".proof-bundle-verify."
        ):
            raise ProofError("refusing unsafe bundle verifier cleanup")
        shutil.rmtree(verify_root)
    return {
        "filename": bundle.name,
        "ref": manifest["bundle_ref"],
        "sha256": manifest["bundle_sha256"],
        "verified_in_fresh_bare_repository": True,
    }


def verify_input(root: Path, *, strict_files: bool = True) -> dict[str, object]:
    root = _exact_directory(root, label="proof pack root")
    manifest, manifest_digest = _proof_manifest(root)
    rows, inventory_digest = _verify_inventory(
        root,
        root / INPUT_INVENTORY,
        root / INPUT_INVENTORY_PIN,
    )
    declared = {str(row["path"]) for row in rows}
    wheel_paths = {path for path in declared if path.startswith("wheelhouse/")}
    if not _FIXED_INPUT_FILES.issubset(declared) or not wheel_paths:
        raise ProofError("proof input inventory is incomplete")
    expected_declared = _FIXED_INPUT_FILES | wheel_paths
    if declared != expected_declared:
        raise ProofError("proof input inventory declares unexpected files")
    entries = list(root.rglob("*"))
    if any(path.is_symlink() for path in entries):
        raise ProofError("proof pack contains a symlink")
    actual = {
        path.relative_to(root).as_posix()
        for path in entries
        if path.is_file()
        and not path.relative_to(root).as_posix().startswith(
            ".proof-bundle-verify."
        )
    }
    expected_actual = declared | {INPUT_INVENTORY, INPUT_INVENTORY_PIN}
    if strict_files:
        if actual != expected_actual:
            raise ProofError("proof pack contains undeclared or missing files")
        expected_directories = {"operator", "scripts", "wheelhouse"}
    else:
        if not expected_actual.issubset(actual) or not (
            actual - expected_actual
        ).issubset(_ALLOWED_OUTPUT_FILES):
            raise ProofError("proof pack contains an unexpected mutable output")
        expected_directories = {"operator", "scripts", "wheelhouse"}
        if (root / NEUTRAL_CWD).is_dir():
            expected_directories.add(NEUTRAL_CWD)
    actual_directories = {
        path.relative_to(root).as_posix() for path in entries if path.is_dir()
    }
    if actual_directories != expected_directories:
        raise ProofError("proof pack directory layout diverged")
    handoff, _handoff_raw, handoff_digest = _pinned_object(
        root / "handoff.json", root / "handoff.sha256"
    )
    source_inventory, source_inventory_raw = _canonical_object(
        root / "source-inventory.json"
    )
    if (
        handoff_digest != manifest["handoff_sha256"]
        or sha256_bytes(source_inventory_raw)
        != manifest["source_inventory_file_sha256"]
        or source_inventory.get("inventory_sha256")
        != manifest["source_inventory_sha256"]
        or handoff.get("boundary") != BOUNDARY
        or handoff.get("schema_version") != 1
        or handoff.get("run_slug") != manifest["run_slug"]
        or handoff.get("source_commit") != manifest["source_commit"]
        or handoff.get("incoming_root") != manifest["incoming_root"]
        or handoff.get("source_root") != manifest["source_root"]
        or handoff.get("campaign_root") != manifest["campaign_root"]
        or handoff.get("volume_base") != manifest["volume_base"]
        or handoff.get("volume_mount") != manifest["volume_mount"]
        or handoff.get("bundle_filename") != manifest["bundle_filename"]
        or handoff.get("bundle_sha256") != manifest["bundle_sha256"]
        or handoff.get("source_inventory_sha256")
        != manifest["source_inventory_sha256"]
        or handoff.get("wheelhouse_manifest_sha256")
        != manifest["wheelhouse_manifest_sha256"]
    ):
        raise ProofError("proof manifest, handoff, or source inventory diverged")
    wheels = _verify_wheelhouse(root, str(manifest["wheelhouse_manifest_sha256"]))
    bundle = _verify_bundle(root, manifest)
    return {
        "bundle": bundle,
        "files": len(rows),
        "input_inventory_sha256": inventory_digest,
        "manifest_sha256": manifest_digest,
        "run_slug": manifest["run_slug"],
        "schema_version": 1,
        "source_commit": manifest["source_commit"],
        "terminal_signal": "PREDICTION_LINUX_PRECUTOVER_PROOF_INPUT_AUTHENTICATED",
        "wheels": wheels,
    }


def _os_release() -> dict[str, str]:
    values: dict[str, str] = {}
    for line in _safe_regular_bytes(Path("/etc/os-release"), maximum_bytes=64 * 1024).decode(
        "utf-8"
    ).splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value.strip().strip('"')
    if values.get("ID") != "ubuntu":
        raise ProofError("Linux proof host is not Ubuntu")
    return values


def _mount_evidence(path: Path, *, expected_fstype: str | None = None) -> dict[str, object]:
    completed = _command(
        [
            "findmnt",
            "-rn",
            "--raw",
            "-T",
            str(path),
            "-o",
            "TARGET,SOURCE,FSTYPE,VFS-OPTIONS,MAJ:MIN,FSROOT",
        ]
    )
    fields = completed.stdout.split(maxsplit=5)
    if len(fields) != 6:
        raise ProofError(f"mount evidence is malformed: {path}")
    target, source, fstype, options, device, fsroot = fields
    if expected_fstype is not None and fstype != expected_fstype:
        raise ProofError(f"filesystem type diverged for {path}: {fstype}")
    stat_device = path.stat().st_dev
    return {
        "device_major_minor": device,
        "filesystem_root": fsroot,
        "fstype": fstype,
        "source": source,
        "stat_device": stat_device,
        "target": target,
        "vfs_options": sorted(set(options.split(","))),
    }


def linux_environment(root: Path, *, source_must_be_absent: bool) -> dict[str, object]:
    root = _exact_directory(root, label="Linux proof incoming root")
    manifest, _digest = _proof_manifest(root)
    if root != Path(str(manifest["incoming_root"])):
        raise ProofError("Linux proof incoming root differs from the authenticated handoff")
    if os.name != "posix" or platform.system() != "Linux":
        raise ProofError("Linux proof requires a real Linux kernel")
    if platform.machine() != "x86_64":
        raise ProofError("Linux proof requires x86_64")
    if sys.version_info[:2] != (3, 12):
        raise ProofError("Linux proof requires Python 3.12")
    libc_name, libc_version = platform.libc_ver()
    try:
        libc_tuple = tuple(int(part) for part in libc_version.split(".")[:2])
    except ValueError as error:
        raise ProofError("glibc version is malformed") from error
    if libc_name != "glibc" or libc_tuple < (2, 28):
        raise ProofError("Linux proof requires glibc >= 2.28")
    if pwd is None:
        raise ProofError("Linux pwd database is unavailable")
    user = pwd.getpwuid(os.getuid())
    if user.pw_name != "hyperlab" or os.environ.get("HOME") != "/home/hyperlab":
        raise ProofError("Linux proof must run as hyperlab with canonical HOME")
    release = _os_release()
    volume_mount = _exact_directory(
        Path(str(manifest["volume_mount"])), label="Prediction volume mount"
    )
    volume_base = _exact_directory(
        Path(str(manifest["volume_base"])), label="Prediction volume base"
    )
    sources = _exact_directory(volume_base / "sources", label="Prediction sources root")
    campaigns = _exact_directory(
        volume_base / "campaigns", label="Prediction campaigns root"
    )
    source = Path(str(manifest["source_root"]))
    campaign = Path(str(manifest["campaign_root"]))
    if source.parent != sources or source.name != manifest["run_slug"]:
        raise ProofError("Linux proof source path escaped the authenticated sources root")
    if campaign.parent != volume_base / "campaigns" or campaign.name != manifest["run_slug"]:
        raise ProofError("Linux proof campaign path escaped the authenticated campaigns root")
    if campaign.exists() or campaign.is_symlink():
        raise ProofError("Linux proof campaign root must remain absent")
    if source_must_be_absent:
        if source.exists() or source.is_symlink():
            raise ProofError("Linux proof source root must be new")
    else:
        _exact_directory(source, label="Linux proof source root")
    if not os.access(root, os.W_OK | os.X_OK) or not os.access(
        sources, os.W_OK | os.X_OK
    ):
        raise ProofError("Linux proof roots are not writable by hyperlab")
    device_paths = [volume_mount, volume_base, sources, campaigns]
    if not source_must_be_absent:
        device_paths.append(source)
    devices = {path.stat().st_dev for path in device_paths}
    if len(devices) != 1:
        raise ProofError("Linux proof source and volume paths span devices")
    volume_evidence = _mount_evidence(volume_mount, expected_fstype="ext4")
    source_parent_evidence = _mount_evidence(sources, expected_fstype="ext4")
    if (
        volume_evidence["device_major_minor"]
        != source_parent_evidence["device_major_minor"]
        or volume_evidence["target"] != str(volume_mount)
        or source_parent_evidence["target"] != str(volume_mount)
    ):
        raise ProofError("Linux proof volume mount identity diverged")
    return {
        "architecture": platform.machine(),
        "campaign_root_absent": True,
        "glibc": libc_version,
        "home": os.environ["HOME"],
        "incoming_mount": _mount_evidence(root),
        "kernel": platform.release(),
        "os_id": release["ID"],
        "os_version_id": release.get("VERSION_ID", ""),
        "python": platform.python_version(),
        "source_parent_mount": source_parent_evidence,
        "source_root_state": "absent" if source_must_be_absent else "prepared",
        "user": user.pw_name,
        "volume_mount": volume_evidence,
    }


def _verify_runtime_report(
    path: Path,
    *,
    source_root: Path,
    source_commit: str,
    inventory_sha256: str,
) -> dict[str, object]:
    report, raw = _canonical_object(path)
    expected_fields = {
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
    body = {key: value for key, value in report.items() if key != "admission_sha256"}
    modules = report.get("modules")
    loaded_module_files = report.get("loaded_module_files_validated")
    if (
        set(report) != expected_fields
        or report.get("admission_sha256") != sha256_bytes(canonical_json_bytes(body))
        or report.get("boundary") != BOUNDARY
        or report.get("inventory_sha256") != inventory_sha256
        or report.get("isolated") is not True
        or report.get("no_user_site") is not True
        or report.get("source_commit") != source_commit
        or report.get("source_root") != str(source_root)
        or report.get("venv_root") != str(source_root / ".venv")
        or report.get("terminal_signal") != "PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN"
        or type(loaded_module_files) is not int
        or loaded_module_files < len(_EXPECTED_SOURCE_MODULES) + len(_EXPECTED_VENV_MODULES)
        or not isinstance(modules, dict)
        or set(modules) != set(_EXPECTED_SOURCE_MODULES) | _EXPECTED_VENV_MODULES
    ):
        raise ProofError("runtime import admission report diverged")
    for name, relative in _EXPECTED_SOURCE_MODULES.items():
        row = modules[name]
        expected = (source_root / relative).resolve(strict=True)
        if not isinstance(row, dict) or row != {"class": "source", "file": str(expected)}:
            raise ProofError(f"runtime source module origin diverged: {name}")
    venv = (source_root / ".venv").resolve(strict=True)
    for name in _EXPECTED_VENV_MODULES:
        row = modules[name]
        if (
            not isinstance(row, dict)
            or set(row) != {"class", "file"}
            or row.get("class") != "venv"
        ):
            raise ProofError(f"runtime venv module class diverged: {name}")
        module_path = _exact_file(
            Path(str(row.get("file") or "")), label=f"runtime venv module: {name}"
        )
        if venv not in module_path.parents or "site-packages" not in module_path.parts:
            raise ProofError(f"runtime venv module origin diverged: {name}")
    executable = _exact_file(
        Path(str(report.get("python_executable") or "")),
        label="runtime Python executable",
    )
    if venv not in executable.parents:
        raise ProofError("runtime Python escaped the prepared venv")
    return {
        "admission_sha256": report["admission_sha256"],
        "file_sha256": sha256_bytes(raw),
        "loaded_module_files_validated": report["loaded_module_files_validated"],
        "modules": modules,
        "terminal_signal": report["terminal_signal"],
    }


def _source_verification(root: Path, source_root: Path, commit: str) -> dict[str, object]:
    completed = _command(
        [
            sys.executable,
            "-I",
            str(root / "scripts" / "launch_pack.py"),
            "verify-source",
            "--source-root",
            str(source_root),
            "--inventory",
            str(root / "source-inventory.json"),
            "--expected-commit",
            commit,
        ],
        cwd=Path.cwd(),
    )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise ProofError("source verification output is invalid") from error
    if (
        not isinstance(value, dict)
        or value.get("commit") != commit
        or value.get("status") != "PREDICTION_SOURCE_IDENTITY_GREEN"
    ):
        raise ProofError("source verification did not authenticate the expected commit")
    return value


def _scan_b0_forbidden_commands(root: Path) -> dict[str, object]:
    payload = _safe_regular_bytes(
        root / "operator" / "B0-linux-precutover-proof.sh"
    ).decode("utf-8")
    executable = "\n".join(
        line for line in payload.splitlines() if not line.lstrip().startswith("#")
    )
    forbidden = (
        "sudo",
        "systemctl",
        "systemd-run",
        "verify-old",
        "disarm-old",
        "restore-old",
        "cutover.sh",
        "install.sh",
        "curl",
        "wget",
        "ssh",
        "scp",
        "nc",
    )
    present = [token for token in forbidden if re.search(rf"(?<![A-Za-z0-9_-]){re.escape(token)}(?![A-Za-z0-9_-])", executable)]
    if present:
        raise ProofError(f"B0 contains forbidden executable commands: {present}")
    required = (
        "verify-input",
        "verify-linux-environment",
        "git clone --no-checkout",
        "checkout --detach",
        "verify-source",
        "bootstrap-offline.sh",
        "runtime-import-admission",
        "write-report",
    )
    missing = [token for token in required if token not in executable]
    if missing:
        raise ProofError(f"B0 preparation sequence is incomplete: {missing}")
    return {"forbidden_absent": list(forbidden), "required_present": list(required)}


def write_report(root: Path, source_root: Path, neutral_cwd: Path, runtime_report: Path) -> dict[str, object]:
    root = _exact_directory(root, label="proof report incoming root")
    source_root = _exact_directory(source_root, label="proof report source root")
    neutral_cwd = _exact_directory(neutral_cwd, label="proof neutral cwd")
    if Path.cwd().resolve(strict=True) != neutral_cwd:
        raise ProofError("proof report was not generated from the neutral cwd")
    if neutral_cwd != root / NEUTRAL_CWD or any(neutral_cwd.iterdir()):
        raise ProofError("proof neutral cwd is not exact and empty")
    manifest, manifest_digest = _proof_manifest(root)
    if source_root != Path(str(manifest["source_root"])):
        raise ProofError("proof report source root diverged")
    if runtime_report != root / RUNTIME_REPORT:
        raise ProofError("runtime report escaped the incoming root")
    input_proof = verify_input(root, strict_files=False)
    environment = linux_environment(root, source_must_be_absent=False)
    source = _source_verification(root, source_root, str(manifest["source_commit"]))
    runtime = _verify_runtime_report(
        runtime_report,
        source_root=source_root,
        source_commit=str(manifest["source_commit"]),
        inventory_sha256=str(manifest["source_inventory_sha256"]),
    )
    if _git(source_root, "rev-parse", "--abbrev-ref", "HEAD") != "HEAD":
        raise ProofError("prepared source is not detached")
    if _git(source_root, "status", "--porcelain"):
        raise ProofError("prepared source checkout is not clean")
    command_contract = _scan_b0_forbidden_commands(root)
    body: dict[str, object] = {
        "boundary": BOUNDARY,
        "bundle": input_proof["bundle"],
        "campaign_root_absent": True,
        "command_contract": command_contract,
        "environment": environment,
        "expected_branch": manifest["expected_branch"],
        "input_inventory_sha256": input_proof["input_inventory_sha256"],
        "manifest_sha256": manifest_digest,
        "neutral_cwd": str(neutral_cwd),
        "proof_id": PROOF_ID,
        "recorded_at_utc": _utc_now_text(),
        "run_slug": manifest["run_slug"],
        "runtime_import": runtime,
        "schema_version": 1,
        "source_commit": manifest["source_commit"],
        "source_root": str(source_root),
        "source_verification": source,
        "terminal_signal": TERMINAL_SIGNAL,
    }
    report = {**body, "proof_sha256": sha256_bytes(canonical_json_bytes(body))}
    report_path = root / REPORT
    report_pin = root / REPORT_PIN
    output_inventory_path = root / OUTPUT_INVENTORY
    output_inventory_pin = root / OUTPUT_INVENTORY_PIN
    for path in (report_path, report_pin, output_inventory_path, output_inventory_pin):
        if path.exists() or path.is_symlink():
            raise ProofError(f"proof output must be new: {path}")
    _write_json_new(report_path, report)
    _write_pin(report_pin, report_path)
    output_paths = [REPORT, REPORT_PIN, RUNTIME_REPORT]
    output_inventory = {
        "files": [
            {
                "path": relative,
                "sha256": sha256_file(root / relative),
                "size": (root / relative).stat().st_size,
            }
            for relative in sorted(output_paths)
        ],
        "schema_version": 1,
    }
    _write_json_new(output_inventory_path, output_inventory)
    _write_pin(output_inventory_pin, output_inventory_path)
    return report


def verify_output(proof_root: Path, evidence_root: Path) -> dict[str, object]:
    proof_root = _exact_directory(proof_root, label="local proof pack root")
    evidence_root = _exact_directory(evidence_root, label="retrieved proof evidence root")
    manifest, manifest_digest = _proof_manifest(proof_root)
    rows, inventory_digest = _verify_inventory(
        evidence_root,
        evidence_root / OUTPUT_INVENTORY,
        evidence_root / OUTPUT_INVENTORY_PIN,
    )
    expected_files = {REPORT, REPORT_PIN, RUNTIME_REPORT}
    if {str(row["path"]) for row in rows} != expected_files:
        raise ProofError("retrieved output inventory fields diverged")
    actual = {
        path.relative_to(evidence_root).as_posix()
        for path in evidence_root.rglob("*")
        if path.is_file() and not path.is_symlink()
    }
    if actual != expected_files | {OUTPUT_INVENTORY, OUTPUT_INVENTORY_PIN}:
        raise ProofError("retrieved evidence contains undeclared or missing files")
    report, _raw, report_digest = _pinned_object(
        evidence_root / REPORT,
        evidence_root / REPORT_PIN,
    )
    body = {key: value for key, value in report.items() if key != "proof_sha256"}
    expected_report_fields = {
        "boundary",
        "bundle",
        "campaign_root_absent",
        "command_contract",
        "environment",
        "expected_branch",
        "input_inventory_sha256",
        "manifest_sha256",
        "neutral_cwd",
        "proof_id",
        "proof_sha256",
        "recorded_at_utc",
        "run_slug",
        "runtime_import",
        "schema_version",
        "source_commit",
        "source_root",
        "source_verification",
        "terminal_signal",
    }
    if (
        set(report) != expected_report_fields
        or report.get("proof_sha256") != sha256_bytes(canonical_json_bytes(body))
        or report.get("boundary") != BOUNDARY
        or report.get("proof_id") != PROOF_ID
        or report.get("schema_version") != 1
        or report.get("run_slug") != manifest["run_slug"]
        or report.get("source_commit") != manifest["source_commit"]
        or report.get("expected_branch") != manifest["expected_branch"]
        or report.get("manifest_sha256") != manifest_digest
        or report.get("terminal_signal") != TERMINAL_SIGNAL
        or report.get("campaign_root_absent") is not True
    ):
        raise ProofError("retrieved proof report binding diverged")
    runtime_summary = report.get("runtime_import")
    if (
        not isinstance(runtime_summary, dict)
        or runtime_summary.get("file_sha256")
        != sha256_file(evidence_root / RUNTIME_REPORT)
        or runtime_summary.get("terminal_signal")
        != "PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN"
    ):
        raise ProofError("retrieved runtime import evidence diverged")
    return {
        "output_inventory_sha256": inventory_digest,
        "report_sha256": report_digest,
        "run_slug": manifest["run_slug"],
        "schema_version": 1,
        "source_commit": manifest["source_commit"],
        "terminal_signal": RETRIEVAL_SIGNAL,
    }


def _copy_regular(source: Path, target: Path) -> None:
    payload = _safe_regular_bytes(source, maximum_bytes=2 * 1024**3)
    _write_new(target, payload)


def _git_blob(repo_root: Path, commit: str, relative: str) -> bytes:
    if _COMMIT.fullmatch(commit) is None or _SAFE_RELATIVE.fullmatch(relative) is None:
        raise ProofError("Git blob identity is unsafe")
    completed = subprocess.run(
        ["git", "-C", str(repo_root), "cat-file", "blob", f"{commit}:{relative}"],
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise ProofError(completed.stderr.decode("utf-8", errors="replace").strip())
    return completed.stdout


def render_readme(manifest: Mapping[str, object]) -> str:
    return f"""# Prediction Markets Linux pre-cutover proof v1

Boundary: `{BOUNDARY}`. This pack prepares no campaign and changes no service.

- run slug: `{manifest['run_slug']}`
- source commit: `{manifest['source_commit']}`
- expected branch: `{manifest['expected_branch']}`
- incoming root: `{manifest['incoming_root']}`
- source root: `{manifest['source_root']}`
- campaign root: `{manifest['campaign_root']}` (must remain absent)

Run A on Windows, B0 in Tabby as `hyperlab`, then C0 on Windows. B0 stops at
`{TERMINAL_SIGNAL}`. It is not a cutover, activation, publication, or economic gate.
"""


def render_windows_transfer(manifest: Mapping[str, object]) -> str:
    incoming = manifest["incoming_root"]
    return f"""# Lieu: Windows PowerShell 5.1 local. Durée moyenne: 3-10 min; maximum opérateur: 25 min.
# Prompts: clé hôte SSH ou passphrase de la clé possibles; aucun prompt VPS privilégié.
# Monitoring: sortie de cette console, une ligne par étape. Ctrl+C laisse uniquement
# le nouvel incoming incomplet; ne pas le supprimer ni le réutiliser, régénérer un slug.
# Signal terminal exact: {TRANSFER_SIGNAL}
$ErrorActionPreference = 'Stop'
$OperatorRoot = (Resolve-Path -LiteralPath $PSScriptRoot).Path
$PackRoot = (Resolve-Path -LiteralPath (Join-Path $OperatorRoot '..')).Path
if ((Split-Path -Leaf $OperatorRoot) -cne 'operator') {{ throw 'A must remain in the proof pack operator directory.' }}
$Python = (Get-Command python -ErrorAction Stop).Source
& $Python -I -c "import sys; assert sys.version_info[:2] == (3,12)"
if ($LASTEXITCODE -ne 0) {{ throw 'Local Python 3.12 is required.' }}
& $Python -I (Join-Path $PackRoot 'scripts/precutover_proof.py') verify-input --root $PackRoot
if ($LASTEXITCODE -ne 0) {{ throw 'Local proof-pack authentication failed.' }}
$SshTarget = $env:HYPERLAB_PM_SSH_TARGET
if ([string]::IsNullOrWhiteSpace($SshTarget) -or $SshTarget -notmatch '^[a-z0-9._-]+@[a-z0-9.-]+$') {{ throw 'Set HYPERLAB_PM_SSH_TARGET to user@host.' }}
$SshKeyRaw = $env:HYPERLAB_PM_SSH_KEY
if ([string]::IsNullOrWhiteSpace($SshKeyRaw)) {{ throw 'Set HYPERLAB_PM_SSH_KEY to the dedicated key path.' }}
$SshKey = (Resolve-Path -LiteralPath $SshKeyRaw).Path
if (-not (Test-Path -LiteralPath $SshKey -PathType Leaf)) {{ throw 'SSH key path is not a regular file.' }}
$IncomingRoot = '{incoming}'
Write-Output 'PREDICTION_LINUX_PRECUTOVER_PROOF_TRANSFER_BEGIN'
ssh -i $SshKey $SshTarget "/usr/bin/test ! -e '$IncomingRoot' && /usr/bin/install -d -m 0700 '$IncomingRoot'"
if ($LASTEXITCODE -ne 0) {{ throw 'Unique incoming root creation failed; do not reuse it.' }}
Get-ChildItem -LiteralPath $PackRoot -Force | ForEach-Object {{
    Write-Output ('TRANSFER ' + $_.Name)
    scp -i $SshKey -r -- $_.FullName "${{SshTarget}}:${{IncomingRoot}}/"
    if ($LASTEXITCODE -ne 0) {{ throw ('Transfer failed: ' + $_.Name) }}
}}
$RemoteVerify = "/usr/bin/env -i HOME=/home/hyperlab PATH=/usr/bin:/bin /usr/bin/python3.12 -I '$IncomingRoot/scripts/precutover_proof.py' verify-input --root '$IncomingRoot'"
ssh -i $SshKey $SshTarget $RemoteVerify
if ($LASTEXITCODE -ne 0) {{ throw 'Remote proof-pack authentication failed; do not run B0.' }}
Write-Output '{TRANSFER_SIGNAL}'
"""


def render_b0(manifest: Mapping[str, object]) -> str:
    incoming = manifest["incoming_root"]
    source = manifest["source_root"]
    campaign = manifest["campaign_root"]
    commit = manifest["source_commit"]
    bundle = manifest["bundle_filename"]
    return f"""#!/usr/bin/env bash
# Lieu: Tabby/VPS Ubuntu Bash sous hyperlab. Durée moyenne: 8-20 min; maximum dur: 35 min.
# Prompts: aucun. Monitoring sûr depuis un second onglet:
# tail -F '{incoming}/{PROGRESS_LOG}'
# Ctrl+C/timeout arrête seulement ce processus de validation et conserve incoming,
# clone, venv et preuves partielles; la campagne active reste intacte. Ne pas réutiliser le slug.
# Signal terminal exact: {TERMINAL_SIGNAL}
set -Eeuo pipefail
umask 077
INCOMING_ROOT='{incoming}'
SOURCE_ROOT='{source}'
CAMPAIGN_ROOT='{campaign}'
BUNDLE_PATH="$INCOMING_ROOT/{bundle}"
EXPECTED_COMMIT='{commit}'
EXPECTED_SELF="$INCOMING_ROOT/operator/B0-linux-precutover-proof.sh"
fail() {{ printf 'PREDICTION_LINUX_PRECUTOVER_PROOF_REFUSED:%s\n' "$1" >&2; exit 4; }}
if [[ ${{1:-}} != --bounded-child ]]; then
  (($# == 0)) || fail 'usage: run B0 without arguments'
  [[ $(/usr/bin/readlink -f -- "$0") == "$EXPECTED_SELF" ]] || fail 'B0 path is not canonical'
  exec /usr/bin/timeout --signal=TERM --kill-after=30s 35m \
    /usr/bin/env -i HOME=/home/hyperlab PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
    PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 PIP_CONFIG_FILE=/dev/null \
    GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TERMINAL_PROMPT=0 \
    GIT_ALLOW_PROTOCOL=file /usr/bin/bash "$EXPECTED_SELF" --bounded-child
fi
(($# == 1)) || fail 'bounded child arguments diverged'
[[ $(id -un) == hyperlab ]] || fail 'run as hyperlab'
[[ $HOME == /home/hyperlab ]] || fail 'HOME must be /home/hyperlab'
[[ $(readlink -f -- "$INCOMING_ROOT") == "$INCOMING_ROOT" ]] || fail 'incoming root is not canonical'
for command in python3.12 git bash timeout env findmnt readlink tee id mkdir; do
  [[ $(command -v "$command") == "/usr/bin/$command" ]] || fail "required command is not canonical:$command"
done
python3.12 -I "$INCOMING_ROOT/scripts/precutover_proof.py" verify-input --root "$INCOMING_ROOT"
python3.12 -I "$INCOMING_ROOT/scripts/precutover_proof.py" verify-linux-environment --root "$INCOMING_ROOT"
[[ ! -e $SOURCE_ROOT && ! -L $SOURCE_ROOT ]] || fail 'source root must be new'
[[ ! -e $CAMPAIGN_ROOT && ! -L $CAMPAIGN_ROOT ]] || fail 'campaign root must remain absent'
for output in '{PROGRESS_LOG}' '{RUNTIME_REPORT}' '{REPORT}' '{REPORT_PIN}' '{OUTPUT_INVENTORY}' '{OUTPUT_INVENTORY_PIN}'; do
  [[ ! -e $INCOMING_ROOT/$output && ! -L $INCOMING_ROOT/$output ]] || fail "proof output already exists:$output"
done
exec > >(tee -a "$INCOMING_ROOT/{PROGRESS_LOG}") 2>&1
trap 'printf "PREDICTION_LINUX_PRECUTOVER_PROOF_INTERRUPTED_PRESERVED_NO_CUTOVER\n" >&2; exit 130' HUP INT TERM
printf 'PREDICTION_LINUX_PRECUTOVER_STEP:1:INPUT_AND_LINUX_GREEN\n'
git clone --no-checkout "$BUNDLE_PATH" "$SOURCE_ROOT"
git -C "$SOURCE_ROOT" checkout --detach "$EXPECTED_COMMIT"
if git -C "$SOURCE_ROOT" symbolic-ref -q HEAD >/dev/null 2>&1; then fail 'source checkout is not detached'; fi
python3.12 -I "$INCOMING_ROOT/scripts/launch_pack.py" verify-source --source-root "$SOURCE_ROOT" --inventory "$INCOMING_ROOT/source-inventory.json" --expected-commit "$EXPECTED_COMMIT"
printf 'PREDICTION_LINUX_PRECUTOVER_STEP:2:BUNDLE_CLONE_INVENTORY_GREEN\n'
bash "$INCOMING_ROOT/scripts/bootstrap-offline.sh" "$SOURCE_ROOT" "$INCOMING_ROOT/wheelhouse"
VENV_PYTHON="$SOURCE_ROOT/.venv/bin/python"
[[ -x $VENV_PYTHON && ! -L $VENV_PYTHON ]] || fail 'prepared venv Python is absent or unsafe'
"$VENV_PYTHON" -I "$INCOMING_ROOT/scripts/launch_pack.py" verify-source --source-root "$SOURCE_ROOT" --inventory "$INCOMING_ROOT/source-inventory.json" --expected-commit "$EXPECTED_COMMIT"
printf 'PREDICTION_LINUX_PRECUTOVER_STEP:3:OFFLINE_VENV_BOOTSTRAP_GREEN\n'
NEUTRAL_ROOT="$INCOMING_ROOT/{NEUTRAL_CWD}"
[[ ! -e $NEUTRAL_ROOT && ! -L $NEUTRAL_ROOT ]] || fail 'neutral cwd must be new'
mkdir -m 0700 -- "$NEUTRAL_ROOT"
cd "$NEUTRAL_ROOT"
timeout --signal=TERM --kill-after=5s 180s env PYTHONNOUSERSITE=1 \
  "$VENV_PYTHON" -I "$INCOMING_ROOT/scripts/preflight.py" runtime-import-admission \
  --handoff "$INCOMING_ROOT/handoff.json" \
  --source-root "$SOURCE_ROOT" \
  --source-inventory "$INCOMING_ROOT/source-inventory.json" \
  --report "$INCOMING_ROOT/{RUNTIME_REPORT}"
printf 'PREDICTION_LINUX_PRECUTOVER_STEP:4:ISOLATED_RUNTIME_IMPORT_GREEN\n'
"$VENV_PYTHON" -I "$INCOMING_ROOT/scripts/precutover_proof.py" write-report \
  --root "$INCOMING_ROOT" \
  --source-root "$SOURCE_ROOT" \
  --neutral-cwd "$NEUTRAL_ROOT" \
  --runtime-report "$INCOMING_ROOT/{RUNTIME_REPORT}"
[[ ! -e $CAMPAIGN_ROOT && ! -L $CAMPAIGN_ROOT ]] || fail 'campaign root changed during proof'
printf '{TERMINAL_SIGNAL}\n'
"""


def render_windows_retrieve(manifest: Mapping[str, object]) -> str:
    incoming = manifest["incoming_root"]
    slug = manifest["run_slug"]
    return f"""# Lieu: Windows PowerShell 5.1 local. Durée moyenne: 2-6 min; maximum opérateur: 15 min.
# Prompts: clé hôte SSH ou passphrase possibles; aucun prompt VPS privilégié.
# Monitoring: sortie de cette console. Ctrl+C laisse un nouveau dossier local partiel;
# ne pas le réutiliser. Cette récupération est strictement en lecture côté VPS.
# Signal terminal exact: {RETRIEVAL_SIGNAL}
$ErrorActionPreference = 'Stop'
$OperatorRoot = (Resolve-Path -LiteralPath $PSScriptRoot).Path
$PackRoot = (Resolve-Path -LiteralPath (Join-Path $OperatorRoot '..')).Path
$Python = (Get-Command python -ErrorAction Stop).Source
& $Python -I -c "import sys; assert sys.version_info[:2] == (3,12)"
if ($LASTEXITCODE -ne 0) {{ throw 'Local Python 3.12 is required.' }}
$SshTarget = $env:HYPERLAB_PM_SSH_TARGET
if ([string]::IsNullOrWhiteSpace($SshTarget) -or $SshTarget -notmatch '^[a-z0-9._-]+@[a-z0-9.-]+$') {{ throw 'Set HYPERLAB_PM_SSH_TARGET to user@host.' }}
$SshKeyRaw = $env:HYPERLAB_PM_SSH_KEY
if ([string]::IsNullOrWhiteSpace($SshKeyRaw)) {{ throw 'Set HYPERLAB_PM_SSH_KEY to the dedicated key path.' }}
$SshKey = (Resolve-Path -LiteralPath $SshKeyRaw).Path
$IncomingRoot = '{incoming}'
$EvidenceRoot = Join-Path (Split-Path -Parent $PackRoot) 'linux-precutover-proof-evidence-{slug}'
if (Test-Path -LiteralPath $EvidenceRoot) {{ throw 'Evidence root must be new; do not overwrite or reuse it.' }}
New-Item -ItemType Directory -Path $EvidenceRoot | Out-Null
$Names = @('{RUNTIME_REPORT}','{REPORT}','{REPORT_PIN}','{OUTPUT_INVENTORY}','{OUTPUT_INVENTORY_PIN}')
$RemoteCheck = ($Names | ForEach-Object {{ "/usr/bin/test -f '$IncomingRoot/$_' && /usr/bin/test ! -L '$IncomingRoot/$_'" }}) -join ' && '
ssh -i $SshKey $SshTarget $RemoteCheck
if ($LASTEXITCODE -ne 0) {{ throw 'Remote proof outputs are absent or unsafe.' }}
foreach ($Name in $Names) {{
    Write-Output ('RETRIEVE ' + $Name)
    scp -i $SshKey -- "${{SshTarget}}:${{IncomingRoot}}/$Name" (Join-Path $EvidenceRoot $Name)
    if ($LASTEXITCODE -ne 0) {{ throw ('Retrieval failed: ' + $Name) }}
}}
& $Python -I (Join-Path $PackRoot 'scripts/precutover_proof.py') verify-output --proof-root $PackRoot --evidence-root $EvidenceRoot
if ($LASTEXITCODE -ne 0) {{ throw 'Retrieved Linux proof authentication failed.' }}
Write-Output ('PREDICTION_LINUX_PRECUTOVER_PROOF_EVIDENCE_ROOT=' + $EvidenceRoot)
Write-Output '{RETRIEVAL_SIGNAL}'
"""


def finalize(
    *,
    repo_root: Path,
    runtime_pack: Path,
    output_root: Path,
    source_commit: str,
    expected_branch: str,
) -> dict[str, object]:
    repo_root = _exact_directory(repo_root, label="repository root")
    runtime_pack = _exact_directory(runtime_pack, label="runtime pack root")
    if output_root.exists() or output_root.is_symlink():
        raise ProofError("proof output root must be new")
    if _COMMIT.fullmatch(source_commit) is None:
        raise ProofError("proof source commit is invalid")
    if expected_branch != EXPECTED_PROOF_BRANCH:
        raise ProofError("proof expected branch is not the independent audit branch")
    if _git(repo_root, "rev-parse", "HEAD") != source_commit:
        raise ProofError("repository HEAD differs from proof source commit")
    checked_branch = _git(repo_root, "check-ref-format", "--branch", expected_branch)
    if checked_branch != expected_branch:
        raise ProofError("proof expected branch is invalid")
    if _git(repo_root, "rev-parse", "--verify", f"refs/heads/{expected_branch}") != source_commit:
        raise ProofError("proof expected branch differs from source commit")
    if _git(repo_root, "status", "--porcelain"):
        raise ProofError("repository must be clean before proof finalization")
    runtime_handoff, _runtime_raw, runtime_handoff_digest = _pinned_object(
        runtime_pack / "handoff.json", runtime_pack / "handoff.sha256"
    )
    if runtime_handoff.get("source_commit") != source_commit:
        raise ProofError("runtime pack source commit diverged")
    _command(
        [
            sys.executable,
            "-I",
            str(runtime_pack / "scripts" / "launch_pack.py"),
            "verify-transfer",
            "--incoming-root",
            str(runtime_pack),
            "--handoff",
            str(runtime_pack / "handoff.json"),
        ]
    )
    slug = runtime_handoff.get("run_slug")
    if not isinstance(slug, str) or _RUN_SLUG.fullmatch(slug) is None:
        raise ProofError("runtime pack run slug is invalid")
    output_root.mkdir(parents=True)
    for relative in (
        _BUNDLE_NAME,
        "handoff.json",
        "handoff.sha256",
        "source-inventory.json",
        "wheelhouse.sha256",
        "scripts/bootstrap-offline.sh",
        "scripts/launch_pack.py",
        "scripts/preflight.py",
    ):
        _copy_regular(
            runtime_pack.joinpath(*PurePosixPath(relative).parts),
            output_root.joinpath(*PurePosixPath(relative).parts),
        )
    for wheel in sorted((runtime_pack / "wheelhouse").glob("*.whl")):
        _copy_regular(wheel, output_root / "wheelhouse" / wheel.name)
    proof_script_relative = "ops/prediction_markets_launch_v1/precutover_proof.py"
    _write_new(
        output_root / "scripts" / "precutover_proof.py",
        _git_blob(repo_root, source_commit, proof_script_relative),
    )
    source_inventory, source_inventory_raw = _canonical_object(
        output_root / "source-inventory.json"
    )
    manifest: dict[str, object] = {
        "boundary": BOUNDARY,
        "bundle_filename": _BUNDLE_NAME,
        "bundle_ref": f"refs/heads/{expected_branch}",
        "bundle_sha256": runtime_handoff["bundle_sha256"],
        "campaign_root": runtime_handoff["campaign_root"],
        "expected_branch": expected_branch,
        "handoff_sha256": runtime_handoff_digest,
        "incoming_root": runtime_handoff["incoming_root"],
        "input_inventory_filename": INPUT_INVENTORY,
        "proof_id": PROOF_ID,
        "run_slug": slug,
        "schema_version": 1,
        "source_commit": source_commit,
        "source_inventory_file_sha256": sha256_bytes(source_inventory_raw),
        "source_inventory_sha256": source_inventory["inventory_sha256"],
        "source_root": runtime_handoff["source_root"],
        "terminal_signal": TERMINAL_SIGNAL,
        "volume_base": runtime_handoff["volume_base"],
        "volume_mount": runtime_handoff["volume_mount"],
        "wheelhouse_manifest_sha256": runtime_handoff["wheelhouse_manifest_sha256"],
    }
    _write_json_new(output_root / PROOF_MANIFEST, manifest)
    _write_pin(output_root / PROOF_MANIFEST_PIN, output_root / PROOF_MANIFEST)
    _write_new(output_root / "README.md", render_readme(manifest).encode("utf-8"))
    operator_files = {
        "A-windows-verify-transfer.ps1": render_windows_transfer(manifest),
        "B0-linux-precutover-proof.sh": render_b0(manifest),
        "C0-windows-retrieve-authenticate.ps1": render_windows_retrieve(manifest),
    }
    for name, content in operator_files.items():
        payload = content.encode("utf-8")
        if name.endswith(".sh"):
            _validate_shell_payload(payload, label=name)
        _write_new(output_root / "operator" / name, payload)
    input_paths = [
        path.relative_to(output_root).as_posix()
        for path in output_root.rglob("*")
        if path.is_file() and not path.is_symlink()
    ]
    inventory = {
        "files": [
            {
                "path": relative,
                "sha256": sha256_file(
                    output_root.joinpath(*PurePosixPath(relative).parts)
                ),
                "size": output_root.joinpath(*PurePosixPath(relative).parts).stat().st_size,
            }
            for relative in sorted(input_paths)
        ],
        "schema_version": 1,
    }
    _write_json_new(output_root / INPUT_INVENTORY, inventory)
    _write_pin(output_root / INPUT_INVENTORY_PIN, output_root / INPUT_INVENTORY)
    verified = verify_input(output_root.resolve(strict=True), strict_files=True)
    return {
        **verified,
        "output_root": str(output_root.resolve(strict=True)),
        "terminal_signal": "PREDICTION_MARKETS_LINUX_PRECUTOVER_PROOF_PACK_GREEN_AWAITING_HUMAN_EXECUTION",
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prediction Markets Linux pre-cutover proof")
    subparsers = parser.add_subparsers(dest="command", required=True)
    final = subparsers.add_parser("finalize")
    final.add_argument("--repo-root", type=Path, required=True)
    final.add_argument("--runtime-pack", type=Path, required=True)
    final.add_argument("--output-root", type=Path, required=True)
    final.add_argument("--source-commit", required=True)
    final.add_argument("--expected-branch", required=True)
    verify = subparsers.add_parser("verify-input")
    verify.add_argument("--root", type=Path, required=True)
    environment = subparsers.add_parser("verify-linux-environment")
    environment.add_argument("--root", type=Path, required=True)
    write = subparsers.add_parser("write-report")
    write.add_argument("--root", type=Path, required=True)
    write.add_argument("--source-root", type=Path, required=True)
    write.add_argument("--neutral-cwd", type=Path, required=True)
    write.add_argument("--runtime-report", type=Path, required=True)
    output = subparsers.add_parser("verify-output")
    output.add_argument("--proof-root", type=Path, required=True)
    output.add_argument("--evidence-root", type=Path, required=True)
    return parser


def _fail(message: str) -> NoReturn:
    print(f"PREDICTION_LINUX_PRECUTOVER_PROOF_REFUSED:{message}", file=sys.stderr)
    raise SystemExit(4)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "finalize":
            result = finalize(
                repo_root=arguments.repo_root.resolve(strict=True),
                runtime_pack=arguments.runtime_pack.resolve(strict=True),
                output_root=arguments.output_root.resolve(strict=False),
                source_commit=arguments.source_commit,
                expected_branch=arguments.expected_branch,
            )
        elif arguments.command == "verify-input":
            result = verify_input(arguments.root.resolve(strict=True), strict_files=True)
        elif arguments.command == "verify-linux-environment":
            result = linux_environment(
                arguments.root.resolve(strict=True), source_must_be_absent=True
            )
            result = {
                **result,
                "terminal_signal": "PREDICTION_LINUX_PRECUTOVER_ENVIRONMENT_GREEN",
            }
        elif arguments.command == "write-report":
            result = write_report(
                arguments.root.resolve(strict=True),
                arguments.source_root.resolve(strict=True),
                arguments.neutral_cwd.resolve(strict=True),
                arguments.runtime_report.resolve(strict=True),
            )
        else:
            result = verify_output(
                arguments.proof_root.resolve(strict=True),
                arguments.evidence_root.resolve(strict=True),
            )
        print(canonical_json_bytes(result).decode("utf-8"))
        return 0
    except (OSError, ProofError, subprocess.SubprocessError, ValueError) as error:
        _fail(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
