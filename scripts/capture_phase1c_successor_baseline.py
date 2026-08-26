"""One-shot capture of the acquired Phase 1C verifier baseline bytes.

The failed candidate-05 closure acquired a verifier identity from a mixed-EOL
Windows worktree which cannot be reconstructed from Git blob bytes or from
``git cat-file --filters``.  This helper therefore selects the exact tracked
file namespace from the checkpoint commit while hashing the still-intact
CURRENT worktree bytes.  It refuses any identity or producer dependency
closure other than the independently acquired pins.

This script never imports a Phase 1C runner, worker, workload, writer, or
candidate store.  It writes one immutable canonical witness and nothing else.
"""

from __future__ import annotations

import ast
import ctypes
import hashlib
import json
import os
import stat
import subprocess
from ctypes import wintypes
from pathlib import Path, PurePosixPath
from uuid import uuid4

BASELINE_COMMIT = "f6c34d3c1e37bccf7ae72ef26cb8d8797dda8ed5"
ACQUIRED_GLOBAL_IDENTITY_SHA256 = (
    "fa0e55fb4a42488eaa52a69355909c578f45994c64e2849df3e859a0089c5936"
)
PRODUCER_DEPENDENCY_CLOSURE_SHA256 = (
    "e8bc7f8f4e3fce05bbb5681b95963414cea6d26a0de813d3f22a39a30a0c9bb7"
)
EXPECTED_GLOBAL_FILE_COUNT = 138
EXPECTED_CLOSURE_FILE_COUNT = 104
PHASE1C_CODE_IDENTITY_FORMAT = "hyperlab-storage-v4-phase1c-code-identity-v1"
BASELINE_WITNESS_FORMAT = (
    "HYPERLAB_STORAGE_V4_PHASE1C_ACQUIRED_VERIFIER_BASELINE_V1"
)
PRODUCER_CLOSURE_FORMAT = (
    "hyperlab-storage-v4-phase1c-producer-dependency-closure-v1"
)
PRODUCER_CLOSURE_STATUS = "PRODUCER_DEPENDENCY_CLOSURE_UNCHANGED"
OUTPUT_RELATIVE_PATH = (
    "config/paper/storage-v4-phase1c-successor-baseline-byte-witness.json"
)
FIXED_CODE_PATHS = (
    "pyproject.toml",
    "requirements-runtime.lock",
    "scripts/certify_storage_v4_phase1c.py",
    "scripts/generate_phase12_live_paper_artifacts.py",
)
PRODUCER_ENTRYPOINTS = (
    "hyperlab.paper.storage_v4.capacity_runner",
    "hyperlab.paper.storage_v4.phase1c_workers",
    "hyperlab.paper.storage_v4.phase1c_workloads",
)
EXCLUDED_UNTRACKED_SOURCE_PATHS = (
    "src/hyperlab/paper/storage_v4/_audit_progress.py",
    "src/hyperlab/paper/storage_v4/phase1c_successor.py",
)
_MAX_CODE_BYTES = 4 * 1024 * 1024


class BaselineCaptureError(RuntimeError):
    """The historical baseline can no longer be captured exactly."""


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical_json_bytes(value: object) -> bytes:
    try:
        text = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise BaselineCaptureError("cannot encode canonical JSON") from error
    try:
        return text.encode("utf-8")
    except UnicodeEncodeError as error:
        raise BaselineCaptureError("canonical JSON is not valid UTF-8") from error


def _is_reparse(observed: os.stat_result) -> bool:
    attributes = int(getattr(observed, "st_file_attributes", 0))
    mask = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & mask)


def _stat_identity(observed: os.stat_result) -> tuple[int, int, int, int]:
    return (
        int(observed.st_dev),
        int(observed.st_ino),
        int(observed.st_size),
        int(observed.st_mtime_ns),
    )


def _safe_relative_path(value: str) -> str:
    pure = PurePosixPath(value)
    if (
        not value
        or "\\" in value
        or pure.is_absolute()
        or pure.as_posix() != value
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        raise BaselineCaptureError(f"unsafe repository path: {value!r}")
    return value


def _safe_worktree_path(repository_root: Path, relative_path: str) -> Path:
    _safe_relative_path(relative_path)
    cursor = repository_root
    for part in PurePosixPath(relative_path).parts:
        cursor /= part
        try:
            observed = os.lstat(cursor)
        except OSError as error:
            raise BaselineCaptureError(
                f"baseline path is missing: {relative_path}"
            ) from error
        if stat.S_ISLNK(observed.st_mode) or _is_reparse(observed):
            raise BaselineCaptureError(
                f"baseline path traverses a link/reparse point: {relative_path}"
            )
    resolved = cursor.resolve(strict=True)
    try:
        resolved.relative_to(repository_root)
    except ValueError as error:
        raise BaselineCaptureError("baseline path escapes repository") from error
    if not resolved.is_file():
        raise BaselineCaptureError(f"baseline path is not a file: {relative_path}")
    return resolved


def _read_stable(path: Path) -> bytes:
    try:
        before = os.lstat(path)
        if (
            not stat.S_ISREG(before.st_mode)
            or _is_reparse(before)
            or before.st_size > _MAX_CODE_BYTES
        ):
            raise BaselineCaptureError(f"unsafe or oversized baseline file: {path}")
        flags = os.O_RDONLY
        for name in ("O_BINARY", "O_NOINHERIT", "O_NOFOLLOW"):
            flags |= int(getattr(os, name, 0))
        descriptor = os.open(path, flags)
    except BaselineCaptureError:
        raise
    except OSError as error:
        raise BaselineCaptureError(f"cannot open baseline file: {path}") from error
    try:
        opened_before = os.fstat(descriptor)
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            total += len(chunk)
            if total > _MAX_CODE_BYTES:
                raise BaselineCaptureError(f"baseline file exceeds bound: {path}")
            chunks.append(chunk)
        payload = b"".join(chunks)
        opened_after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        after = os.lstat(path)
    except OSError as error:
        raise BaselineCaptureError(
            f"baseline file disappeared after read: {path}"
        ) from error
    if (
        len(payload) > _MAX_CODE_BYTES
        or not stat.S_ISREG(after.st_mode)
        or _is_reparse(after)
        or len(
            {
                _stat_identity(before),
                _stat_identity(opened_before),
                _stat_identity(opened_after),
                _stat_identity(after),
            }
        )
        != 1
    ):
        raise BaselineCaptureError(f"baseline file changed while read: {path}")
    return payload


def _fsync_directory(path: Path) -> None:
    if os.name != "nt":
        descriptor = os.open(
            path,
            os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0)),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    flush_file_buffers = kernel32.FlushFileBuffers
    flush_file_buffers.argtypes = [wintypes.HANDLE]
    flush_file_buffers.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL
    handle = create_file(
        str(path),
        0x40000000,
        0x00000007,
        None,
        3,
        0x02000000,
        None,
    )
    if handle == wintypes.HANDLE(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        if not flush_file_buffers(handle):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        if not close_handle(handle):
            raise ctypes.WinError(ctypes.get_last_error())


def _safe_new_output_path(repository_root: Path, relative_path: str) -> Path:
    _safe_relative_path(relative_path)
    try:
        root_observed = os.lstat(repository_root)
    except OSError as error:
        raise BaselineCaptureError("repository root is unavailable") from error
    if (
        not stat.S_ISDIR(root_observed.st_mode)
        or stat.S_ISLNK(root_observed.st_mode)
        or _is_reparse(root_observed)
    ):
        raise BaselineCaptureError("repository root is not a direct directory")
    resolved_root = repository_root.resolve(strict=True)
    cursor = resolved_root
    parts = PurePosixPath(relative_path).parts
    for part in parts[:-1]:
        cursor /= part
        try:
            observed = os.lstat(cursor)
        except OSError as error:
            raise BaselineCaptureError("publication parent is unavailable") from error
        if (
            not stat.S_ISDIR(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or _is_reparse(observed)
        ):
            raise BaselineCaptureError(
                "publication path traverses a link/reparse point"
            )
    resolved_parent = cursor.resolve(strict=True)
    try:
        resolved_parent.relative_to(resolved_root)
    except ValueError as error:
        raise BaselineCaptureError("publication path escapes repository") from error
    target = resolved_parent / parts[-1]
    try:
        os.lstat(target)
    except FileNotFoundError:
        return target
    except OSError as error:
        raise BaselineCaptureError("cannot inspect publication target") from error
    raise BaselineCaptureError("baseline witness already exists")


def _publish_immutable(target: Path, data: bytes) -> None:
    if type(data) is not bytes:
        raise TypeError("immutable publication data must be exact bytes")
    temporary = target.parent / f".{target.name}.{uuid4().hex}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    for name in ("O_BINARY", "O_NOINHERIT"):
        flags |= int(getattr(os, name, 0))
    descriptor: int | None = None
    published = False
    try:
        descriptor = os.open(temporary, flags, 0o600)
        view = memoryview(data)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise BaselineCaptureError("temporary artifact write was incomplete")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        if _read_stable(temporary) != data:
            raise BaselineCaptureError("temporary artifact read-back differs")
        try:
            os.link(temporary, target)
        except FileExistsError as error:
            raise BaselineCaptureError("baseline witness already exists") from error
        published = True
        _fsync_directory(target.parent)
        temporary.unlink()
        _fsync_directory(target.parent)
    except BaselineCaptureError:
        raise
    except OSError as error:
        raise BaselineCaptureError("immutable durable publication failed") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary.exists():
            temporary.unlink(missing_ok=True)
        if not published and target.exists():
            raise BaselineCaptureError(
                "publication failed after the authority name became visible"
            )


def _git(repository_root: Path, *arguments: str) -> bytes:
    environment = dict(os.environ)
    for name in tuple(environment):
        if name.startswith("GIT_") or name in {"PYTHONPATH", "PYTHONHOME"}:
            environment.pop(name, None)
    environment.update(
        {
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "nul" if os.name == "nt" else "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_TERMINAL_PROMPT": "0",
        }
    )
    try:
        result = subprocess.run(
            ("git", "-C", str(repository_root), *arguments),
            check=False,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=60,
            env=environment,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise BaselineCaptureError("read-only Git query failed") from error
    if result.returncode != 0:
        raise BaselineCaptureError(
            "read-only Git query failed: "
            + result.stderr.decode("utf-8", errors="replace")[:500]
        )
    return result.stdout


def _tracked_code_paths(repository_root: Path) -> tuple[str, ...]:
    observed_commit = _git(repository_root, "rev-parse", "HEAD").strip().decode("ascii")
    if observed_commit != BASELINE_COMMIT:
        raise BaselineCaptureError("HEAD differs from the acquired baseline checkpoint")
    raw = _git(
        repository_root,
        "ls-tree",
        "-r",
        "--name-only",
        "-z",
        BASELINE_COMMIT,
    )
    tracked = {
        _safe_relative_path(item.decode("utf-8", errors="strict"))
        for item in raw.split(b"\0")
        if item
    }
    selected = {
        path
        for path in tracked
        if path.startswith("src/hyperlab/") and path.endswith(".py")
    }
    selected.update(FIXED_CODE_PATHS)
    if not set(FIXED_CODE_PATHS).issubset(tracked):
        raise BaselineCaptureError("checkpoint is missing a fixed identity input")
    if len(selected) != EXPECTED_GLOBAL_FILE_COUNT:
        raise BaselineCaptureError("acquired global identity file count differs")
    if set(EXCLUDED_UNTRACKED_SOURCE_PATHS) & selected:
        raise BaselineCaptureError("successor additions leaked into baseline selection")
    return tuple(sorted(selected))


def _module_index(payloads: dict[str, bytes]) -> tuple[dict[str, str], set[str]]:
    modules: dict[str, str] = {}
    packages: set[str] = set()
    for path in sorted(payloads):
        if not path.startswith("src/hyperlab/") or not path.endswith(".py"):
            continue
        relative = PurePosixPath(path).relative_to("src")
        if relative.name == "__init__.py":
            module = ".".join(relative.parts[:-1])
            packages.add(module)
        else:
            module = ".".join((*relative.parts[:-1], relative.stem))
        if not module or module in modules:
            raise BaselineCaptureError("ambiguous local Python module namespace")
        modules[module] = path
    return modules, packages


def _relative_import_base(
    *, module: str, is_package: bool, level: int, imported_module: str | None
) -> str:
    if level == 0:
        return imported_module or ""
    package = module.split(".") if is_package else module.split(".")[:-1]
    ascent = level - 1
    if ascent > len(package):
        raise BaselineCaptureError("relative import escapes the local package")
    base = package[: len(package) - ascent]
    if imported_module:
        base.extend(imported_module.split("."))
    return ".".join(base)


def _reject_dynamic_imports(tree: ast.AST, *, path: str) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        name = function.id if isinstance(function, ast.Name) else None
        attribute = function.attr if isinstance(function, ast.Attribute) else None
        if name in {"eval", "exec"}:
            raise BaselineCaptureError(f"dynamic execution in dependency closure: {path}")
        if attribute in {"exec_module", "module_from_spec", "spec_from_file_location"}:
            raise BaselineCaptureError(f"dynamic module loader in dependency closure: {path}")
        if name == "__import__" or attribute == "import_module":
            literal = (
                node.args[0].value
                if node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
                else None
            )
            if literal is None or literal == "hyperlab" or literal.startswith("hyperlab."):
                raise BaselineCaptureError(
                    f"unresolved dynamic local import in dependency closure: {path}"
                )


def _producer_dependency_paths(payloads: dict[str, bytes]) -> tuple[str, ...]:
    modules, packages = _module_index(payloads)
    pending: set[str] = set()
    for entrypoint in PRODUCER_ENTRYPOINTS:
        try:
            pending.add(modules[entrypoint])
        except KeyError as error:
            raise BaselineCaptureError(
                f"producer entrypoint is missing: {entrypoint}"
            ) from error
    selected: set[str] = set()
    while pending:
        path = min(pending)
        pending.remove(path)
        if path in selected:
            continue
        selected.add(path)
        relative = PurePosixPath(path).relative_to("src")
        parent = relative.parent
        while parent.parts and parent.parts[0] == "hyperlab":
            init_path = (PurePosixPath("src") / parent / "__init__.py").as_posix()
            if init_path in payloads and init_path not in selected:
                pending.add(init_path)
            parent = parent.parent
        module = next(name for name, module_path in modules.items() if module_path == path)
        is_package = module in packages
        try:
            tree = compile(payloads[path], path, "exec", ast.PyCF_ONLY_AST)
        except (SyntaxError, ValueError) as error:
            raise BaselineCaptureError(f"cannot parse dependency source: {path}") from error
        _reject_dynamic_imports(tree, path=path)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    dependency = modules.get(alias.name)
                    if dependency is not None:
                        pending.add(dependency)
                    elif alias.name == "hyperlab" or alias.name.startswith("hyperlab."):
                        raise BaselineCaptureError(
                            f"unresolved local import {alias.name!r} in {path}"
                        )
            elif isinstance(node, ast.ImportFrom):
                base = _relative_import_base(
                    module=module,
                    is_package=is_package,
                    level=node.level,
                    imported_module=node.module,
                )
                dependency = modules.get(base)
                if dependency is not None:
                    pending.add(dependency)
                elif base == "hyperlab" or base.startswith("hyperlab."):
                    raise BaselineCaptureError(
                        f"unresolved local import {base!r} in {path}"
                    )
                if base in packages:
                    for alias in node.names:
                        if alias.name == "*":
                            continue
                        child = modules.get(f"{base}.{alias.name}")
                        if child is not None:
                            pending.add(child)
    return tuple(sorted(selected))


def _capture_payload(repository_root: Path) -> dict[str, object]:
    selected_paths = _tracked_code_paths(repository_root)
    payloads = {
        path: _read_stable(_safe_worktree_path(repository_root, path))
        for path in selected_paths
    }
    global_files = {
        path: {"bytes": len(payload), "sha256": _sha256(payload)}
        for path, payload in sorted(payloads.items())
    }
    global_without_sha256 = {
        "files": global_files,
        "format": PHASE1C_CODE_IDENTITY_FORMAT,
        "repository_root": str(repository_root),
    }
    global_sha256 = _sha256(_canonical_json_bytes(global_without_sha256))
    if global_sha256 != ACQUIRED_GLOBAL_IDENTITY_SHA256:
        raise BaselineCaptureError(
            "current tracked worktree bytes no longer reproduce acquired identity fa0e"
        )
    closure_paths = _producer_dependency_paths(payloads)
    closure_files = [
        {"path": path, "sha256": _sha256(payloads[path])}
        for path in closure_paths
    ]
    closure_sha256 = _sha256(_canonical_json_bytes(closure_files))
    if (
        len(closure_files) != EXPECTED_CLOSURE_FILE_COUNT
        or closure_sha256 != PRODUCER_DEPENDENCY_CLOSURE_SHA256
    ):
        raise BaselineCaptureError(
            "current tracked worktree bytes no longer reproduce producer closure e8bc/104"
        )
    return {
        "acquired_verifier_global_identity": {
            **global_without_sha256,
            "sha256": global_sha256,
        },
        "acquisition": {
            "baseline_commit": BASELINE_COMMIT,
            "checkout_filter_context": (
                "MIXED_WORKTREE_BYTES_NOT_REPRODUCIBLE_FROM_GIT_FILTERS"
            ),
            "excluded_untracked_source_paths": list(EXCLUDED_UNTRACKED_SOURCE_PATHS),
            "git_filtered_snapshot_rejected": True,
            "method": "STABLE_LIVE_WORKTREE_BYTES_BEFORE_SUCCESSOR_PATCH",
            "repository_root": str(repository_root),
        },
        "artifact": BASELINE_WITNESS_FORMAT,
        "producer_dependency_closure": {
            "closure_sha256": closure_sha256,
            "entrypoints": list(PRODUCER_ENTRYPOINTS),
            "file_count": len(closure_files),
            "files": closure_files,
            "format": PRODUCER_CLOSURE_FORMAT,
            "status": PRODUCER_CLOSURE_STATUS,
        },
    }


def main() -> int:
    repository_root = Path(__file__).absolute().parents[1]
    output_path = _safe_new_output_path(repository_root, OUTPUT_RELATIVE_PATH)
    payload = _capture_payload(repository_root)
    data = _canonical_json_bytes(payload)
    _publish_immutable(output_path, data)
    observed = _read_stable(output_path)
    if observed != data:
        raise BaselineCaptureError("published baseline witness bytes differ")
    print(
        _canonical_json_bytes(
            {
                "output_path": str(output_path),
                "sha256": _sha256(data),
                "size_bytes": len(data),
                "status": "PHASE1C_SUCCESSOR_BASELINE_BYTE_WITNESS_CAPTURED",
            }
        ).decode("utf-8"),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
