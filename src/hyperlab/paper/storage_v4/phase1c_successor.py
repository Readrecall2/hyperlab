"""Fail-closed read-only successor closure for Phase 1C candidate-05.

The old candidate remains attributed to its original producer identity.  The
successor performs one byte-exact reattestation and publishes a receipt without
``COMPLETE``.  A separate finalizer consumes only that receipt plus durable gate
witnesses, verifies the current successor verifier identity, and publishes the
terminal closure without reopening the producer candidate.

This module intentionally imports no workload, worker, runner, writer, or store.
"""

from __future__ import annotations

import ast
import ctypes
import hashlib
import json
import math
import os
import platform
import sqlite3
import stat
import sys
import time
import zlib
from collections.abc import Callable, Mapping
from ctypes import wintypes
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import cast
from uuid import uuid4

PHASE1C_TARGET_NOT_MET_VERDICT = (
    "STORAGE_V4_PHASE_1C_NATIVE_CAPACITY_CHARACTERIZED_TARGET_NOT_MET"
)
PHASE1C_CODE_IDENTITY_FORMAT = "hyperlab-storage-v4-phase1c-code-identity-v1"
SUCCESSOR_VERIFIER_IDENTITY_FORMAT = (
    "hyperlab-storage-v4-phase1c-successor-verifier-identity-v1"
)
SUCCESSOR_DEPENDENCY_CLOSURE_FORMAT = (
    "hyperlab-storage-v4-phase1c-producer-dependency-closure-v1"
)
SUCCESSOR_BASELINE_WITNESS_FORMAT = (
    "HYPERLAB_STORAGE_V4_PHASE1C_ACQUIRED_VERIFIER_BASELINE_V1"
)
SUCCESSOR_RECEIPT_FORMAT = "hyperlab-storage-v4-phase1c-successor-receipt-v1"
SUCCESSOR_REPORT_FORMAT = "hyperlab-storage-v4-phase1c-successor-report-v1"
SUCCESSOR_MANIFEST_FORMAT = "hyperlab-storage-v4-phase1c-successor-manifest-v1"
SUCCESSOR_PIN_FORMAT = "hyperlab-storage-v4-phase1c-successor-pin-v1"
SUCCESSOR_COMPLETE_FORMAT = "hyperlab-storage-v4-phase1c-successor-complete-v1"
SUCCESSOR_REATTESTED_STATUS = "STORAGE_V4_PHASE_1C_SUCCESSOR_REATTESTED"
SUCCESSOR_CLOSURE_STATUS = "STORAGE_V4_PHASE_1C_SUCCESSOR_CLOSURE_VERIFIED"
SUCCESSOR_ATTRIBUTION = "OLD_PRODUCER_BYTES_REATTESTED_BY_NEW_VERIFIER"
SUCCESSOR_BOUNDARY_ARTIFACT = (
    "STORAGE_V4_PHASE_1C_CUMULATIVE_BOUNDARY_CERTIFICATE_V1"
)
SUCCESSOR_RECEIPT_NAME = "successor-receipt.json"
SUCCESSOR_REPORT_NAME = "successor-report.json"
SUCCESSOR_MANIFEST_NAME = "manifest.json"
SUCCESSOR_PIN_NAME = "pin/certification.pin.json"
SUCCESSOR_COMPLETE_NAME = "COMPLETE"
PRODUCER_DEPENDENCY_CLOSURE_UNCHANGED = (
    "PRODUCER_DEPENDENCY_CLOSURE_UNCHANGED"
)
CURRENT_VERIFIER_CLOSURE_STATUS = (
    "CURRENT_VERIFIER_DEPENDENCY_CLOSURE_VERIFIED_NON_PRODUCER"
)

SYNTHETIC_CAPACITY_WORKLOAD = "SYNTHETIC_CAPACITY_WORKLOAD"
NOT_ECONOMIC_EVIDENCE = "NOT_ECONOMIC_EVIDENCE"
NOT_ALPHA_EVIDENCE = "NOT_ALPHA_EVIDENCE"
PAPER_ONLY = "PAPER_ONLY"
CAPACITY_MARKERS = (
    SYNTHETIC_CAPACITY_WORKLOAD,
    NOT_ECONOMIC_EVIDENCE,
    NOT_ALPHA_EVIDENCE,
    PAPER_ONLY,
)
SUCCESSOR_REQUIRED_MARKERS = (
    "SUCCESSOR_CERTIFIED_FROM_IMMUTABLE_CANDIDATE_05",
    "OLD_PRODUCER_IDENTITY_BOUND",
    "NEW_VERIFIER_IDENTITY_BOUND",
    PRODUCER_DEPENDENCY_CLOSURE_UNCHANGED,
    "COMMITS_INGESTED_DURING_SUCCESSION=0",
    "PREFIX_REINGESTED=0",
    "CANDIDATE_05_UNCHANGED=true",
    "RUN06_COMMITS=0",
)
SUCCESSOR_MARKERS = (
    *CAPACITY_MARKERS,
    "ZERO_REINGESTION",
    "NOT_CURRENT_PRODUCER_EXECUTION",
    SUCCESSOR_ATTRIBUTION,
    *SUCCESSOR_REQUIRED_MARKERS,
)

_PHASE1C_FIXED_CODE_PATHS = (
    "pyproject.toml",
    "requirements-runtime.lock",
    "scripts/capture_phase1c_successor_baseline.py",
    "scripts/certify_storage_v4_phase1c.py",
    "scripts/certify_storage_v4_phase1c_successor.py",
    "scripts/generate_phase05_paper_evidence.py",
    "scripts/generate_phase12_live_paper_artifacts.py",
)
_SUCCESSOR_PRODUCER_ENTRYPOINTS = (
    "hyperlab.paper.storage_v4.capacity_runner",
    "hyperlab.paper.storage_v4.phase1c_workers",
    "hyperlab.paper.storage_v4.phase1c_workloads",
)
_BASELINE_EXCLUDED_ADDITIONS = (
    "src/hyperlab/paper/storage_v4/_audit_progress.py",
    "src/hyperlab/paper/storage_v4/phase1c_successor.py",
)
SUCCESSOR_TARGETED_TEST_PATHS = (
    "tests/storage_v4/test_audit_progress.py",
    "tests/storage_v4/test_full_audit_progress.py",
    "tests/storage_v4/test_phase1c_successor.py",
    "tests/storage_v4/test_phase1c_successor_cli.py",
    "tests/storage_v4/test_phase1c_worker_result_resume.py",
)
SUCCESSOR_TARGETED_WITNESS_NAME = "targeted-tests-witness.json"
SUCCESSOR_CLOSURE_WITNESS_NAME = "closure-witness.json"
SUCCESSOR_V9_RELATIVE_PATH = (
    "config/paper/phase08-v9-historical-attestation.json"
)
SUCCESSOR_V9_SIZE_BYTES = 2_833
SUCCESSOR_V9_SHA256 = (
    "7f3216b97ffeb60d18c05572e5642f08dbb589caebcbc746fa5829b6fa565d33"
)
SUCCESSOR_TARGETED_LOG_NAME = "00-targeted-tests.log"
_CLOSURE_PURPOSES = (
    "V10_CHECK",
    "PHASE05_CHECK",
    "RUFF_GLOBAL_FINAL",
    "MYPY_HYPERLAB_FINAL",
    "PYTEST_GLOBAL_FINAL_SINGLE_RUN",
    "GIT_DIFF_CHECK_FINAL",
)
_CLOSURE_LOG_NAMES = (
    "01-v10-check.log",
    "02-phase05-check.log",
    "03-ruff-global-final.log",
    "04-mypy-hyperlab-final.log",
    "05-pytest-global-final-single-run.log",
    "06-git-diff-check-final.log",
)
_SHA256_LENGTH = 64
_MAX_CERTIFICATE_BYTES = 4 * 1024 * 1024
_MAX_CODE_FILE_BYTES = 4 * 1024 * 1024
_MAX_LOG_BYTES = 128 * 1024 * 1024
_BOUNDARY_KEYS = {
    "artifact",
    "authority",
    "boundary_commit_count",
    "boundary_manifest",
    "boundary_manifest_sha256",
    "evidence",
    "measurement",
    "previous_certificate_sha256",
    "terminal_manifest_sha256",
    "workload_prefix",
}
_MANIFEST_KEYS = {
    "activity_rates",
    "artifact",
    "configuration",
    "expected",
    "generator_version",
    "golden_census_sha256",
    "markers",
    "payload_sizes",
    "profile",
    "seed",
    "strategies",
    "tail_restart_sizes",
    "temporal_cadence",
    "type_distribution",
}
_MEASUREMENT_KEYS = {
    "byte_census",
    "counts",
    "cpu_ns",
    "durations",
    "full_history_audit_ns",
    "markers",
    "metadata_authentication_ns",
    "observed_workload_sha256",
    "retained_bytes_per_raw_input_byte",
    "rss",
    "startup",
    "storage_growth_target",
    "throughput",
    "wall_ns",
    "workload_manifest_sha256",
    "write_amplification",
}
_EVIDENCE_KEYS = {"authority", "batching", "integrity", "scopes", "startup"}
_EVIDENCE_AUTHORITY_KEYS = {
    "candidate_root",
    "code_identity",
    "config_identity",
    "paper_store_id",
    "raw_lake_id",
    "raw_store_id",
    "run_id",
    "runtime_identity",
}
_EVIDENCE_INTEGRITY_KEYS = {
    "alignment_status",
    "audited_candidate_tree",
    "commit_count",
    "final_prefix_root",
    "market_gap_count",
    "oracle_commit_count",
    "oracle_final_prefix_root",
    "oracle_logical_row_count",
    "oracle_workload_sha256",
    "raw_reference_count",
    "raw_reference_prefix_root",
}

ProgressCallback = Callable[[Mapping[str, object]], None]


class Phase1CSuccessorError(RuntimeError):
    """Fail-closed rejection of successor evidence or publication."""


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _require_sha256(value: object, *, label: str) -> str:
    if (
        type(value) is not str
        or len(value) != _SHA256_LENGTH
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise Phase1CSuccessorError(f"{label} must be a lowercase SHA-256")
    return value


def _require_git_commit(value: object, *, label: str) -> str:
    if (
        type(value) is not str
        or len(value) != 40
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise Phase1CSuccessorError(
            f"{label} must be a lowercase 40-hex Git commit"
        )
    return value


def _require_text(value: object, *, label: str) -> str:
    if type(value) is not str or not value:
        raise Phase1CSuccessorError(f"{label} must be non-empty text")
    value.encode("utf-8", errors="strict")
    return value


def _require_positive_int(value: object, *, label: str) -> int:
    if type(value) is not int or value < 1:
        raise Phase1CSuccessorError(f"{label} must be a positive exact integer")
    return value


def _require_non_negative_int(value: object, *, label: str) -> int:
    if type(value) is not int or value < 0:
        raise Phase1CSuccessorError(
            f"{label} must be a non-negative exact integer"
        )
    return value


def _mapping(value: object, *, label: str) -> dict[str, object]:
    if type(value) is not dict:
        raise Phase1CSuccessorError(f"{label} must be an exact JSON object")
    return value


def _sequence(value: object, *, label: str) -> list[object]:
    if type(value) is not list:
        raise Phase1CSuccessorError(f"{label} must be an exact JSON array")
    return value


def _require_exact_keys(
    value: Mapping[str, object], expected: set[str], *, label: str
) -> None:
    if set(value) != expected:
        raise Phase1CSuccessorError(f"{label} fields differ from its contract")


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


def _require_absolute_direct_path(
    path: Path, *, label: str, directory: bool
) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise Phase1CSuccessorError(f"{label} must be an absolute pathlib.Path")
    try:
        resolved = path.resolve(strict=True)
        observed = os.lstat(path)
    except OSError as error:
        raise Phase1CSuccessorError(f"{label} is missing") from error
    if resolved != path or stat.S_ISLNK(observed.st_mode) or _is_reparse(observed):
        raise Phase1CSuccessorError(f"{label} is indirect or unsafe")
    expected_mode = stat.S_ISDIR if directory else stat.S_ISREG
    if not expected_mode(observed.st_mode):
        raise Phase1CSuccessorError(f"{label} has the wrong filesystem type")
    return resolved


def _read_stable_regular_file(
    path: Path, *, label: str, maximum_bytes: int
) -> bytes:
    _require_absolute_direct_path(path, label=label, directory=False)
    try:
        before = os.lstat(path)
        flags = os.O_RDONLY
        for name in ("O_BINARY", "O_NOINHERIT", "O_NOFOLLOW"):
            flags |= int(getattr(os, name, 0))
        descriptor = os.open(path, flags)
    except OSError as error:
        raise Phase1CSuccessorError(f"{label} could not be opened") from error
    try:
        opened_before = os.fstat(descriptor)
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            total += len(chunk)
            if total > maximum_bytes:
                raise Phase1CSuccessorError(f"{label} exceeds its size bound")
            chunks.append(chunk)
        opened_after = os.fstat(descriptor)
    except OSError as error:
        raise Phase1CSuccessorError(f"{label} could not be read") from error
    finally:
        os.close(descriptor)
    try:
        after = os.lstat(path)
    except OSError as error:
        raise Phase1CSuccessorError(f"{label} disappeared") from error
    if (
        not stat.S_ISREG(after.st_mode)
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
        raise Phase1CSuccessorError(f"{label} changed while read")
    return b"".join(chunks)


def _parse_canonical_json(data: bytes, *, label: str) -> dict[str, object]:
    try:
        value = json.loads(data.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise Phase1CSuccessorError(f"{label} is not strict JSON") from error
    mapping = _mapping(value, label=label)
    if canonical_json_bytes(mapping) != data:
        raise Phase1CSuccessorError(f"{label} is not exact canonical JSON")
    return mapping


def _safe_relative_path(value: object, *, label: str) -> str:
    if type(value) is not str:
        raise Phase1CSuccessorError(f"{label} must be text")
    pure = PurePosixPath(value)
    if (
        not value
        or "\\" in value
        or pure.is_absolute()
        or pure.as_posix() != value
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        raise Phase1CSuccessorError(f"{label} is not a safe relative path")
    return value


def _safe_repository_file(repository_root: Path, relative_path: str) -> Path:
    _safe_relative_path(relative_path, label="repository file path")
    cursor = repository_root
    for part in PurePosixPath(relative_path).parts:
        cursor /= part
        try:
            observed = os.lstat(cursor)
        except OSError as error:
            raise Phase1CSuccessorError(
                f"verifier identity file is missing: {relative_path}"
            ) from error
        if stat.S_ISLNK(observed.st_mode) or _is_reparse(observed):
            raise Phase1CSuccessorError(
                f"verifier identity traverses a link/reparse point: {relative_path}"
            )
    resolved = cursor.resolve(strict=True)
    try:
        resolved.relative_to(repository_root)
    except ValueError as error:
        raise Phase1CSuccessorError("verifier identity path escapes repository") from error
    if not resolved.is_file():
        raise Phase1CSuccessorError(
            f"verifier identity path is not a file: {relative_path}"
        )
    return resolved


class CanonicalizationError(ValueError):
    """A value has no permitted canonical JSON representation."""


def _snapshot_canonical_value(
    value: object,
    *,
    path: str,
    active: set[int],
) -> object:
    value_type = type(value)
    if value is None or value_type is bool or value_type is int:
        return value
    if value_type is str:
        try:
            cast(str, value).encode("utf-8", errors="strict")
        except UnicodeEncodeError as error:
            raise CanonicalizationError(
                f"{path} must be strict UTF-8 text"
            ) from error
        return value
    if value_type is float:
        raise CanonicalizationError(
            f"{path} float values are forbidden, including NaN and Infinity"
        )
    if isinstance(value, Decimal):
        raise CanonicalizationError(
            f"{path} Decimal values require an explicit canonical decimal string"
        )
    if value_type is list:
        identity = id(value)
        if identity in active:
            raise CanonicalizationError(f"{path} contains a cyclic list")
        active.add(identity)
        snapshot: list[object] = []
        try:
            for index, item in enumerate(cast(list[object], value)):
                snapshot.append(
                    _snapshot_canonical_value(
                        item,
                        path=f"{path}[{index}]",
                        active=active,
                    )
                )
        finally:
            active.remove(identity)
        return snapshot
    if value_type is dict:
        identity = id(value)
        if identity in active:
            raise CanonicalizationError(f"{path} contains a cyclic object")
        active.add(identity)
        snapshot_object: dict[str, object] = {}
        try:
            for key, item in cast(dict[object, object], value).items():
                if type(key) is not str:
                    raise CanonicalizationError(
                        f"{path} object keys must be text"
                    )
                try:
                    key.encode("utf-8", errors="strict")
                except UnicodeEncodeError as error:
                    raise CanonicalizationError(
                        f"{path} object keys must be strict UTF-8"
                    ) from error
                snapshot_object[key] = _snapshot_canonical_value(
                    item,
                    path=f"{path}.{key}",
                    active=active,
                )
        finally:
            active.remove(identity)
        return snapshot_object
    raise CanonicalizationError(
        f"{path} type {value_type.__module__}.{value_type.__qualname__} "
        "is not canonical"
    )


def canonical_json_bytes(value: object) -> bytes:
    """Encode strict logical JSON as deterministic UTF-8 bytes."""

    try:
        snapshot = _snapshot_canonical_value(value, path="$", active=set())
    except (RecursionError, RuntimeError) as error:
        raise CanonicalizationError(
            "logical value changed or exceeded recursion while being snapshotted"
        ) from error
    try:
        text = json.dumps(
            snapshot,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return text.encode("utf-8", errors="strict")
    except (RecursionError, TypeError, UnicodeEncodeError, ValueError) as error:
        raise CanonicalizationError(
            "logical value could not be encoded as canonical UTF-8 JSON"
        ) from error


class DurabilityError(RuntimeError):
    """A local durable publication invariant could not be established."""


def fsync_directory(path: Path) -> None:
    """Durably flush directory entries on POSIX and Windows or fail closed."""

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
    invalid_handle = wintypes.HANDLE(-1).value
    if handle == invalid_handle:
        raise ctypes.WinError(ctypes.get_last_error())

    flushed = False
    flush_error = 0
    try:
        flushed = bool(flush_file_buffers(handle))
        flush_error = ctypes.get_last_error() if not flushed else 0
    finally:
        closed = bool(close_handle(handle))
        close_error = ctypes.get_last_error() if not closed else 0
    if not flushed:
        raise ctypes.WinError(flush_error)
    if not closed:
        raise ctypes.WinError(close_error)


def _read_immutable_publication_target(path: Path, expected: bytes) -> None:
    try:
        before = os.lstat(path)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(before.st_mode)
            or _is_reparse(before)
        ):
            raise DurabilityError(
                f"immutable target is not a direct regular file: {path.name}"
            )
        flags = os.O_RDONLY
        for name in ("O_BINARY", "O_NOINHERIT", "O_NOFOLLOW"):
            flags |= int(getattr(os, name, 0))
        descriptor = os.open(path, flags)
    except DurabilityError:
        raise
    except OSError as error:
        raise DurabilityError(
            f"immutable target could not be opened: {path.name}"
        ) from error
    try:
        opened_before = os.fstat(descriptor)
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        opened_after = os.fstat(descriptor)
    except OSError as error:
        raise DurabilityError(
            f"immutable target could not be read: {path.name}"
        ) from error
    finally:
        os.close(descriptor)
    try:
        after = os.lstat(path)
    except OSError as error:
        raise DurabilityError(
            f"immutable target disappeared: {path.name}"
        ) from error
    if (
        not stat.S_ISREG(after.st_mode)
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
        raise DurabilityError(
            f"immutable target changed while read: {path.name}"
        )
    if b"".join(chunks) != expected:
        raise DurabilityError(
            f"refusing to overwrite divergent immutable target: {path.name}"
        )


def durable_publish_immutable(target: Path, data: bytes) -> None:
    """Publish immutable bytes exclusively, idempotently, and durably."""

    if not isinstance(target, Path):
        raise TypeError("immutable publication target must be pathlib.Path")
    if type(data) is not bytes:
        raise TypeError("immutable publication data must be exact bytes")
    try:
        parent = target.parent.resolve(strict=True)
        parent_stat = os.lstat(target.parent)
    except OSError as error:
        raise DurabilityError("publication parent does not exist") from error
    if (
        parent != target.parent
        or not stat.S_ISDIR(parent_stat.st_mode)
        or stat.S_ISLNK(parent_stat.st_mode)
        or _is_reparse(parent_stat)
    ):
        raise DurabilityError("publication parent is indirect or unsafe")
    if target.is_symlink():
        raise DurabilityError(
            f"refusing publication through symbolic link: {target}"
        )
    if target.exists():
        _read_immutable_publication_target(target, data)
        fsync_directory(parent)
        return

    temporary: Path | None = None
    for _attempt in range(16):
        candidate = parent / f".{target.name}.{uuid4().hex}.tmp"
        try:
            stream = candidate.open("xb")
        except FileExistsError:
            continue
        temporary = candidate
        try:
            with stream:
                written = stream.write(data)
                if written != len(data):
                    raise DurabilityError(
                        "temporary artifact write was incomplete"
                    )
                stream.flush()
                os.fsync(stream.fileno())
            _read_immutable_publication_target(candidate, data)
        except Exception:
            candidate.unlink(missing_ok=True)
            raise
        break
    if temporary is None:
        raise DurabilityError("could not allocate a fresh UUID temporary file")

    try:
        try:
            os.link(temporary, target)
        except FileExistsError:
            _read_immutable_publication_target(target, data)
            fsync_directory(parent)
            temporary.unlink(missing_ok=True)
            return
        fsync_directory(parent)
        temporary.unlink(missing_ok=True)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


_CANDIDATE_TREE_HEARTBEAT_MIN_SECONDS = 30.0
_CANDIDATE_TREE_HEARTBEAT_MAX_SECONDS = 60.0


class CandidateTreeWitnessError(Phase1CSuccessorError):
    """A candidate tree is unsafe, transient, or changed while witnessed."""


@dataclass(frozen=True, slots=True)
class CandidateFileWitness:
    relative_path: str
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        _safe_relative_path(self.relative_path, label="candidate file path")
        if type(self.size_bytes) is not int or self.size_bytes < 0:
            raise ValueError(
                "candidate file size must be a non-negative exact integer"
            )
        _require_sha256(self.sha256, label="candidate file SHA-256")

    def payload(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class CandidateTreeWitness:
    root: Path
    files: tuple[CandidateFileWitness, ...]
    directories: tuple[str, ...]
    directory_count: int
    total_bytes: int
    tree_sha256: str

    def __post_init__(self) -> None:
        if not self.root.is_absolute() or not self.root.is_dir():
            raise ValueError(
                "candidate tree root must be an existing absolute directory"
            )
        file_paths = tuple(item.relative_path for item in self.files)
        if not self.files or file_paths != tuple(sorted(set(file_paths))):
            raise ValueError(
                "candidate tree files must be non-empty, unique, and sorted"
            )
        if self.directories != tuple(sorted(set(self.directories))):
            raise ValueError(
                "candidate tree directories must be unique and sorted"
            )
        for directory in self.directories:
            _safe_relative_path(directory, label="candidate directory path")
        if self.directory_count != len(self.directories) + 1:
            raise ValueError(
                "candidate tree directory count differs from its manifest"
            )
        if self.total_bytes != sum(item.size_bytes for item in self.files):
            raise ValueError("candidate tree total differs from its files")
        _require_sha256(self.tree_sha256, label="candidate tree SHA-256")
        if self.tree_sha256 != _sha256(
            canonical_json_bytes(self.payload_without_sha256())
        ):
            raise ValueError(
                "candidate tree SHA-256 differs from its manifest"
            )

    def payload_without_sha256(self) -> dict[str, object]:
        return {
            "directories": list(self.directories),
            "directory_count": self.directory_count,
            "file_count": len(self.files),
            "files": [item.payload() for item in self.files],
            "root": str(self.root),
            "total_bytes": self.total_bytes,
        }

    def payload(self) -> dict[str, object]:
        return {
            **self.payload_without_sha256(),
            "tree_sha256": self.tree_sha256,
        }


def _candidate_tree_link_or_reparse(path: Path) -> bool:
    try:
        observed = os.lstat(path)
    except OSError:
        return True
    return stat.S_ISLNK(observed.st_mode) or _is_reparse(observed)


def _validate_candidate_tree_root(root: Path) -> None:
    if not isinstance(root, Path) or not root.is_absolute():
        raise ValueError("candidate root must be an absolute pathlib.Path")
    try:
        resolved = root.resolve(strict=True)
    except OSError as error:
        raise CandidateTreeWitnessError("candidate root is missing") from error
    if resolved != root or not root.is_dir():
        raise CandidateTreeWitnessError(
            "candidate root is not a direct regular directory"
        )
    cursor = root
    while True:
        if _candidate_tree_link_or_reparse(cursor):
            raise CandidateTreeWitnessError(
                f"candidate ancestry contains a link/reparse point: {cursor}"
            )
        if cursor.parent == cursor:
            break
        cursor = cursor.parent


def _enumerate_candidate_tree(
    root: Path,
) -> tuple[tuple[str, ...], tuple[Path, ...]]:
    directories: list[str] = []
    files: list[Path] = []
    try:
        for directory, names, filenames in os.walk(
            root,
            topdown=True,
            followlinks=False,
        ):
            base = Path(directory)
            if _candidate_tree_link_or_reparse(base):
                raise CandidateTreeWitnessError(
                    f"candidate tree contains a link/reparse point: {base}"
                )
            names.sort()
            filenames.sort()
            for name in names:
                candidate = base / name
                observed = os.lstat(candidate)
                if stat.S_ISLNK(observed.st_mode) or _is_reparse(observed):
                    raise CandidateTreeWitnessError(
                        "candidate tree contains a link/reparse point: "
                        f"{candidate}"
                    )
                if not stat.S_ISDIR(observed.st_mode):
                    raise CandidateTreeWitnessError(
                        "candidate tree contains a non-directory entry: "
                        f"{candidate}"
                    )
                directories.append(candidate.relative_to(root).as_posix())
            for name in filenames:
                candidate = base / name
                observed = os.lstat(candidate)
                if stat.S_ISLNK(observed.st_mode) or _is_reparse(observed):
                    raise CandidateTreeWitnessError(
                        "candidate tree contains a link/reparse point: "
                        f"{candidate}"
                    )
                if not stat.S_ISREG(observed.st_mode):
                    raise CandidateTreeWitnessError(
                        "candidate tree contains a non-regular entry: "
                        f"{candidate}"
                    )
                if name == "PENDING" or name.endswith(
                    (".tmp", "-journal", "-shm", "-wal")
                ):
                    raise CandidateTreeWitnessError(
                        f"candidate tree contains a transient sidecar: {candidate}"
                    )
                files.append(candidate)
    except CandidateTreeWitnessError:
        raise
    except OSError as error:
        raise CandidateTreeWitnessError(
            "candidate tree enumeration failed"
        ) from error
    directories.sort()
    files.sort(key=lambda item: item.relative_to(root).as_posix())
    return tuple(directories), tuple(files)


def _hash_candidate_file(path: Path) -> tuple[int, str]:
    try:
        before = os.lstat(path)
        if not stat.S_ISREG(before.st_mode) or _is_reparse(before):
            raise CandidateTreeWitnessError(
                f"candidate contains an unsafe file: {path}"
            )
        flags = os.O_RDONLY
        for name in ("O_BINARY", "O_NOINHERIT", "O_NOFOLLOW"):
            flags |= int(getattr(os, name, 0))
        descriptor = os.open(path, flags)
    except CandidateTreeWitnessError:
        raise
    except OSError as error:
        raise CandidateTreeWitnessError(
            f"candidate file open failed: {path}"
        ) from error
    digest = hashlib.sha256()
    try:
        opened_before = os.fstat(descriptor)
        if not stat.S_ISREG(opened_before.st_mode) or _is_reparse(
            opened_before
        ):
            raise CandidateTreeWitnessError(
                f"candidate descriptor is not a regular file: {path}"
            )
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
        opened_after = os.fstat(descriptor)
    except OSError as error:
        raise CandidateTreeWitnessError(
            f"candidate file hash failed: {path}"
        ) from error
    finally:
        os.close(descriptor)
    try:
        after = os.lstat(path)
    except OSError as error:
        raise CandidateTreeWitnessError(
            f"candidate file disappeared: {path}"
        ) from error
    if (
        not stat.S_ISREG(after.st_mode)
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
        raise CandidateTreeWitnessError(
            f"candidate file changed while hashed: {path}"
        )
    return int(before.st_size), digest.hexdigest()


def witness_candidate_tree(
    root: Path,
    *,
    progress: ProgressCallback | None = None,
    heartbeat_interval_seconds: float = _CANDIDATE_TREE_HEARTBEAT_MIN_SECONDS,
) -> CandidateTreeWitness:
    """Hash one complete immutable candidate without following links."""

    _validate_candidate_tree_root(root)
    if progress is not None and not callable(progress):
        raise TypeError("candidate hash progress callback must be callable or None")
    if (
        type(heartbeat_interval_seconds) not in (int, float)
        or not math.isfinite(float(heartbeat_interval_seconds))
        or not _CANDIDATE_TREE_HEARTBEAT_MIN_SECONDS
        <= float(heartbeat_interval_seconds)
        <= _CANDIDATE_TREE_HEARTBEAT_MAX_SECONDS
    ):
        raise ValueError(
            "candidate hash heartbeat must be between 30 and 60 seconds"
        )

    directories, paths = _enumerate_candidate_tree(root)
    if not paths:
        raise CandidateTreeWitnessError("candidate tree is empty")
    files: list[CandidateFileWitness] = []
    total_bytes = 0
    last_heartbeat = time.monotonic()
    for path in paths:
        size, digest = _hash_candidate_file(path)
        total_bytes += size
        files.append(
            CandidateFileWitness(
                relative_path=path.relative_to(root).as_posix(),
                size_bytes=size,
                sha256=digest,
            )
        )
        now = time.monotonic()
        if now - last_heartbeat >= float(heartbeat_interval_seconds):
            if progress is not None:
                progress(
                    {
                        "bytes_hashed": total_bytes,
                        "candidate_root": str(root),
                        "files_completed": len(files),
                        "files_total": len(paths),
                        "phase": "phase1c_candidate_tree_hash",
                        "status": "RUNNING",
                    }
                )
            last_heartbeat = now

    verification_directories, verification_paths = (
        _enumerate_candidate_tree(root)
    )
    if (
        verification_directories != directories
        or tuple(
            path.relative_to(root).as_posix()
            for path in verification_paths
        )
        != tuple(item.relative_path for item in files)
    ):
        raise CandidateTreeWitnessError(
            "candidate tree changed during enumeration"
        )
    for path, witnessed in zip(verification_paths, files, strict=True):
        size, digest = _hash_candidate_file(path)
        if size != witnessed.size_bytes or digest != witnessed.sha256:
            raise CandidateTreeWitnessError(
                "candidate tree changed after its first hash pass: "
                f"{path}"
            )
    material = {
        "directories": list(directories),
        "directory_count": len(directories) + 1,
        "file_count": len(files),
        "files": [item.payload() for item in files],
        "root": str(root),
        "total_bytes": total_bytes,
    }
    return CandidateTreeWitness(
        root=root,
        files=tuple(files),
        directories=directories,
        directory_count=len(directories) + 1,
        total_bytes=total_bytes,
        tree_sha256=_sha256(canonical_json_bytes(material)),
    )


@dataclass(frozen=True, slots=True)
class SuccessorCodeIdentity:
    repository_root: Path
    identity_format: str
    files: tuple[tuple[str, int, str], ...]
    sha256: str

    def __post_init__(self) -> None:
        if not self.repository_root.is_absolute():
            raise ValueError("code identity repository root must be absolute")
        _require_text(self.identity_format, label="code identity format")
        if not self.files or self.files != tuple(sorted(self.files)):
            raise ValueError("code identity files must be non-empty and sorted")
        if len({path for path, _, _ in self.files}) != len(self.files):
            raise ValueError("code identity paths must be unique")
        for path, size, digest in self.files:
            _safe_relative_path(path, label="code identity path")
            _require_non_negative_int(size, label="code identity file size")
            _require_sha256(digest, label="code identity file SHA-256")
        _require_sha256(self.sha256, label="code identity SHA-256")
        if self.sha256 != _sha256(canonical_json_bytes(self.payload_without_sha256())):
            raise ValueError("code identity SHA-256 differs from its manifest")

    def payload_without_sha256(self) -> dict[str, object]:
        return {
            "files": {
                path: {"bytes": size, "sha256": digest}
                for path, size, digest in self.files
            },
            "format": self.identity_format,
            "repository_root": str(self.repository_root),
        }

    def payload(self) -> dict[str, object]:
        return {**self.payload_without_sha256(), "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class SuccessorDependencyClosureWitness:
    entrypoints: tuple[str, ...]
    files: tuple[tuple[str, str], ...]
    closure_sha256: str
    status: str

    def __post_init__(self) -> None:
        if self.entrypoints != tuple(sorted(set(self.entrypoints))):
            raise ValueError("dependency closure entrypoints must be sorted and unique")
        if not self.files or self.files != tuple(sorted(self.files)):
            raise ValueError("dependency closure files must be non-empty and sorted")
        if len({path for path, _ in self.files}) != len(self.files):
            raise ValueError("dependency closure paths must be unique")
        for path, digest in self.files:
            _safe_relative_path(path, label="dependency closure path")
            _require_sha256(digest, label="dependency closure file SHA-256")
        _require_sha256(self.closure_sha256, label="dependency closure SHA-256")
        _require_text(self.status, label="dependency closure status")
        if self.closure_sha256 != _sha256(
            canonical_json_bytes(
                [{"path": path, "sha256": digest} for path, digest in self.files]
            )
        ):
            raise ValueError("dependency closure SHA-256 differs from its file list")

    def payload(self) -> dict[str, object]:
        return {
            "closure_sha256": self.closure_sha256,
            "entrypoints": list(self.entrypoints),
            "file_count": len(self.files),
            "files": [
                {"path": path, "sha256": digest} for path, digest in self.files
            ],
            "format": SUCCESSOR_DEPENDENCY_CLOSURE_FORMAT,
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class SuccessorVerifierState:
    global_code_identity: SuccessorCodeIdentity
    successor_code_identity: SuccessorCodeIdentity
    dependency_closure: SuccessorDependencyClosureWitness
    runtime_identity: str

    def __post_init__(self) -> None:
        if self.global_code_identity.identity_format != PHASE1C_CODE_IDENTITY_FORMAT:
            raise ValueError("global code identity format differs")
        if (
            self.successor_code_identity.identity_format
            != SUCCESSOR_VERIFIER_IDENTITY_FORMAT
        ):
            raise ValueError("successor verifier identity format differs")
        _require_sha256(self.runtime_identity, label="verifier runtime identity")
        if self.global_code_identity.sha256 == self.successor_code_identity.sha256:
            raise ValueError("global and successor identities must be distinct")

    def payload(self) -> dict[str, object]:
        return {
            "global_phase1c_code_identity": self.global_code_identity.payload(),
            "role": "CURRENT_SUCCESSOR_VERIFIER_ONLY_NOT_PRODUCER",
            "runtime_identity": self.runtime_identity,
            "successor_verifier_code_identity": self.successor_code_identity.payload(),
        }


def _identity_from_payloads(
    *,
    repository_root: Path,
    identity_format: str,
    relative_paths: tuple[str, ...],
    payloads: Mapping[str, bytes],
) -> SuccessorCodeIdentity:
    files = tuple(
        (path, len(payloads[path]), _sha256(payloads[path]))
        for path in relative_paths
    )
    material = {
        "files": {
            path: {"bytes": size, "sha256": digest}
            for path, size, digest in files
        },
        "format": identity_format,
        "repository_root": str(repository_root),
    }
    return SuccessorCodeIdentity(
        repository_root=repository_root,
        identity_format=identity_format,
        files=files,
        sha256=_sha256(canonical_json_bytes(material)),
    )


def _module_index(payloads: Mapping[str, bytes]) -> tuple[dict[str, str], set[str]]:
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
            raise Phase1CSuccessorError("ambiguous local Python module namespace")
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
        raise Phase1CSuccessorError("relative import escapes local package")
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
            raise Phase1CSuccessorError(
                f"dynamic execution exists in dependency closure: {path}"
            )
        if attribute in {"exec_module", "module_from_spec", "spec_from_file_location"}:
            raise Phase1CSuccessorError(
                f"dynamic module loader exists in dependency closure: {path}"
            )
        if name == "__import__" or attribute == "import_module":
            literal = (
                node.args[0].value
                if node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
                else None
            )
            if literal is None or literal == "hyperlab" or literal.startswith("hyperlab."):
                raise Phase1CSuccessorError(
                    f"unresolved dynamic local import exists in dependency closure: {path}"
                )


def _dependency_closure(
    payloads: Mapping[str, bytes], *, status: str
) -> SuccessorDependencyClosureWitness:
    modules, packages = _module_index(payloads)
    pending: set[str] = set()
    for entrypoint in _SUCCESSOR_PRODUCER_ENTRYPOINTS:
        try:
            pending.add(modules[entrypoint])
        except KeyError as error:
            raise Phase1CSuccessorError(
                f"producer dependency entrypoint is missing: {entrypoint}"
            ) from error
    selected: set[str] = set()
    reverse_modules = {path: module for module, path in modules.items()}
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
        module = reverse_modules[path]
        is_package = module in packages
        try:
            tree = compile(payloads[path], path, "exec", ast.PyCF_ONLY_AST)
        except (SyntaxError, ValueError) as error:
            raise Phase1CSuccessorError(
                f"dependency source cannot be parsed: {path}"
            ) from error
        _reject_dynamic_imports(tree, path=path)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    dependency = modules.get(alias.name)
                    if dependency is not None:
                        pending.add(dependency)
                    elif alias.name == "hyperlab" or alias.name.startswith("hyperlab."):
                        raise Phase1CSuccessorError(
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
                    raise Phase1CSuccessorError(
                        f"unresolved local import {base!r} in {path}"
                    )
                if base in packages:
                    for alias in node.names:
                        if alias.name == "*":
                            continue
                        child = modules.get(f"{base}.{alias.name}")
                        if child is not None:
                            pending.add(child)
    files = tuple((path, _sha256(payloads[path])) for path in sorted(selected))
    return SuccessorDependencyClosureWitness(
        entrypoints=_SUCCESSOR_PRODUCER_ENTRYPOINTS,
        files=files,
        closure_sha256=_sha256(
            canonical_json_bytes(
                [{"path": path, "sha256": digest} for path, digest in files]
            )
        ),
        status=status,
    )


def _current_runtime_identity() -> str:
    return _sha256(
        canonical_json_bytes(
            {
                "byteorder": sys.byteorder,
                "implementation": platform.python_implementation(),
                "machine": platform.machine(),
                "os_name": os.name,
                "platform": sys.platform,
                "python": platform.python_version(),
                "sqlite": sqlite3.sqlite_version,
                "zlib": zlib.ZLIB_VERSION,
            }
        )
    )


def compute_phase1c_successor_verifier_state(
    repository_root: Path,
) -> SuccessorVerifierState:
    """Hash current verifier/support bytes and its conservative local closure."""

    root = _require_absolute_direct_path(
        repository_root, label="repository root", directory=True
    )
    source_root = _require_absolute_direct_path(
        root / "src" / "hyperlab", label="HyperLab source root", directory=True
    )
    source_paths: set[str] = set()
    for path in source_root.rglob("*.py"):
        if path.is_file():
            source_paths.add(path.relative_to(root).as_posix())
    global_paths = tuple(sorted(source_paths | set(_PHASE1C_FIXED_CODE_PATHS)))
    payloads: dict[str, bytes] = {}
    for relative_path in global_paths:
        path = _safe_repository_file(root, relative_path)
        payloads[relative_path] = _read_stable_regular_file(
            path,
            label=f"successor verifier code file {relative_path}",
            maximum_bytes=_MAX_CODE_FILE_BYTES,
        )
    global_identity = _identity_from_payloads(
        repository_root=root,
        identity_format=PHASE1C_CODE_IDENTITY_FORMAT,
        relative_paths=global_paths,
        payloads=payloads,
    )
    successor_identity = _identity_from_payloads(
        repository_root=root,
        identity_format=SUCCESSOR_VERIFIER_IDENTITY_FORMAT,
        relative_paths=global_paths,
        payloads=payloads,
    )
    return SuccessorVerifierState(
        global_code_identity=global_identity,
        successor_code_identity=successor_identity,
        dependency_closure=_dependency_closure(
            payloads, status=CURRENT_VERIFIER_CLOSURE_STATUS
        ),
        runtime_identity=_current_runtime_identity(),
    )


def _canonical_targeted_pytest_basetemp(command: tuple[str, ...]) -> Path:
    expected_prefix = (
        sys.executable,
        "-m",
        "pytest",
        *SUCCESSOR_TARGETED_TEST_PATHS,
        "-p",
        "no:cacheprovider",
        "--basetemp",
    )
    if len(command) != len(expected_prefix) + 1 or command[:-1] != expected_prefix:
        raise Phase1CSuccessorError("targeted test command differs from canonical command")
    basetemp = Path(command[-1])
    if not basetemp.is_absolute():
        raise Phase1CSuccessorError("targeted pytest basetemp must be absolute")
    return basetemp


def _canonical_global_pytest_basetemp(command: tuple[str, ...]) -> Path:
    expected_prefix = (
        sys.executable,
        "-m",
        "pytest",
        "-p",
        "no:cacheprovider",
        "--basetemp",
    )
    if len(command) != len(expected_prefix) + 1 or command[:-1] != expected_prefix:
        raise Phase1CSuccessorError("global pytest command differs from canonical command")
    basetemp = Path(command[-1])
    if not basetemp.is_absolute():
        raise Phase1CSuccessorError("global pytest basetemp must be absolute")
    return basetemp


def _canonical_closure_commands(
    global_basetemp: Path,
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if not global_basetemp.is_absolute():
        raise Phase1CSuccessorError("global pytest basetemp must be absolute")
    python = sys.executable
    return (
        (
            "V10_CHECK",
            (python, "scripts/generate_phase12_live_paper_artifacts.py", "--check"),
        ),
        (
            "PHASE05_CHECK",
            (python, "scripts/generate_phase05_paper_evidence.py", "--check"),
        ),
        ("RUFF_GLOBAL_FINAL", (python, "-m", "ruff", "check", ".")),
        ("MYPY_HYPERLAB_FINAL", (python, "-m", "mypy", "src/hyperlab")),
        (
            "PYTEST_GLOBAL_FINAL_SINGLE_RUN",
            (
                python,
                "-m",
                "pytest",
                "-p",
                "no:cacheprovider",
                "--basetemp",
                str(global_basetemp),
            ),
        ),
        (
            "GIT_DIFF_CHECK_FINAL",
            ("git", "-c", "core.whitespace=cr-at-eol", "diff", "--check"),
        ),
    )


@dataclass(frozen=True, slots=True)
class Phase1CSuccessorTestWitness:
    command: tuple[str, ...]
    exit_code: int
    output_sha256: str
    source_files: tuple[tuple[str, str], ...]
    summary: str
    output_log_path: str | None
    output_log_size_bytes: int | None

    def __post_init__(self) -> None:
        if not self.command or any(
            type(item) is not str or not item for item in self.command
        ):
            raise ValueError("targeted test command must be non-empty")
        _canonical_targeted_pytest_basetemp(self.command)
        if type(self.exit_code) is not int or self.exit_code != 0:
            raise Phase1CSuccessorError("targeted successor tests did not pass")
        _require_sha256(self.output_sha256, label="targeted test output SHA-256")
        if not self.source_files or self.source_files != tuple(sorted(self.source_files)):
            raise ValueError("targeted test source witnesses must be sorted")
        if tuple(path for path, _digest in self.source_files) != (
            SUCCESSOR_TARGETED_TEST_PATHS
        ):
            raise Phase1CSuccessorError("targeted test sources differ from canonical list")
        if len({path for path, _ in self.source_files}) != len(self.source_files):
            raise ValueError("targeted test source paths must be unique")
        for path, digest in self.source_files:
            _safe_relative_path(path, label="targeted test source path")
            _require_sha256(digest, label="targeted test source SHA-256")
        _require_text(self.summary, label="targeted test summary")
        if (self.output_log_path is None) != (self.output_log_size_bytes is None):
            raise ValueError("targeted log path and size must both be present")
        if self.output_log_path is not None:
            if not Path(self.output_log_path).is_absolute():
                raise ValueError("targeted output log path must be absolute")
            _require_non_negative_int(
                self.output_log_size_bytes, label="targeted output log size"
            )

    def payload(self) -> dict[str, object]:
        return {
            "command": list(self.command),
            "exit_code": self.exit_code,
            "format": "hyperlab-storage-v4-phase1c-targeted-tests-v1",
            "output_log_path": self.output_log_path,
            "output_log_size_bytes": self.output_log_size_bytes,
            "output_sha256": self.output_sha256,
            "source_files": dict(self.source_files),
            "summary": self.summary,
        }

    @property
    def sha256(self) -> str:
        return _sha256(canonical_json_bytes(self.payload()))


@dataclass(frozen=True, slots=True)
class Phase1CSuccessorCommandWitness:
    purpose: str
    command: tuple[str, ...]
    exit_code: int
    output_sha256: str
    summary: str
    output_log_path: str | None
    output_log_size_bytes: int | None

    def __post_init__(self) -> None:
        _require_text(self.purpose, label="closure purpose")
        if not self.command or any(type(item) is not str or not item for item in self.command):
            raise ValueError("closure command must be non-empty")
        if type(self.exit_code) is not int or self.exit_code != 0:
            raise Phase1CSuccessorError(f"closure command failed: {self.purpose}")
        _require_sha256(self.output_sha256, label="closure output SHA-256")
        _require_text(self.summary, label="closure summary")
        if (self.output_log_path is None) != (self.output_log_size_bytes is None):
            raise ValueError("closure log path and size must both be present")
        if self.output_log_path is not None:
            if not Path(self.output_log_path).is_absolute():
                raise ValueError("closure output log path must be absolute")
            _require_non_negative_int(
                self.output_log_size_bytes, label="closure output log size"
            )

    def payload(self) -> dict[str, object]:
        return {
            "command": list(self.command),
            "exit_code": self.exit_code,
            "output_log_path": self.output_log_path,
            "output_log_size_bytes": self.output_log_size_bytes,
            "output_sha256": self.output_sha256,
            "purpose": self.purpose,
            "summary": self.summary,
        }


@dataclass(frozen=True, slots=True)
class Phase1CSuccessorV9ByteWitness:
    path: str
    size_bytes: int
    before_sha256: str
    after_sha256: str

    def __post_init__(self) -> None:
        _require_text(self.path, label="V9 attestation path")
        _require_positive_int(self.size_bytes, label="V9 attestation size")
        _require_sha256(self.before_sha256, label="V9 before SHA-256")
        _require_sha256(self.after_sha256, label="V9 after SHA-256")
        if self.before_sha256 != self.after_sha256:
            raise Phase1CSuccessorError("V9 changed during successor closure")

    def payload(self) -> dict[str, object]:
        return {
            "after_sha256": self.after_sha256,
            "before_sha256": self.before_sha256,
            "byte_identical": True,
            "path": self.path,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class Phase1CSuccessorClosureWitness:
    commands: tuple[Phase1CSuccessorCommandWitness, ...]
    v9: Phase1CSuccessorV9ByteWitness

    def __post_init__(self) -> None:
        purposes = tuple(item.purpose for item in self.commands)
        if purposes != _CLOSURE_PURPOSES:
            raise Phase1CSuccessorError("closure commands differ from canonical order")
        global_basetemp = _canonical_global_pytest_basetemp(
            self.commands[4].command
        )
        if tuple((item.purpose, item.command) for item in self.commands) != (
            _canonical_closure_commands(global_basetemp)
        ):
            raise Phase1CSuccessorError("closure commands differ from canonical commands")
        if not isinstance(self.v9, Phase1CSuccessorV9ByteWitness):
            raise TypeError("closure V9 witness has wrong type")
        if (
            self.v9.path != SUCCESSOR_V9_RELATIVE_PATH
            or self.v9.size_bytes != SUCCESSOR_V9_SIZE_BYTES
            or self.v9.before_sha256 != SUCCESSOR_V9_SHA256
            or self.v9.after_sha256 != SUCCESSOR_V9_SHA256
        ):
            raise Phase1CSuccessorError(
                "closure V9 witness differs from canonical pinned bytes"
            )

    def payload(self) -> dict[str, object]:
        return {
            "commands": [item.payload() for item in self.commands],
            "format": "hyperlab-storage-v4-phase1c-closure-v1",
            "global_pytest_runs": 1,
            "status": "STORAGE_V4_PHASE_1C_REPOSITORY_CLOSURE_VERIFIED",
            "v9": self.v9.payload(),
        }

    @property
    def sha256(self) -> str:
        return _sha256(canonical_json_bytes(self.payload()))


def parse_phase1c_successor_test_witness(
    value: object,
) -> Phase1CSuccessorTestWitness:
    payload = _mapping(value, label="targeted test witness")
    _require_exact_keys(
        payload,
        {
            "command",
            "exit_code",
            "format",
            "output_log_path",
            "output_log_size_bytes",
            "output_sha256",
            "source_files",
            "summary",
        },
        label="targeted test witness",
    )
    if payload["format"] != "hyperlab-storage-v4-phase1c-targeted-tests-v1":
        raise Phase1CSuccessorError("targeted test witness format differs")
    command = tuple(
        _require_text(item, label="targeted test command item")
        for item in _sequence(payload["command"], label="targeted test command")
    )
    source_mapping = _mapping(payload["source_files"], label="targeted sources")
    source_files = tuple(
        sorted(
            (
                _safe_relative_path(path, label="targeted source path"),
                _require_sha256(digest, label="targeted source SHA-256"),
            )
            for path, digest in source_mapping.items()
        )
    )
    return Phase1CSuccessorTestWitness(
        command=command,
        exit_code=cast(int, payload["exit_code"]),
        output_sha256=_require_sha256(
            payload["output_sha256"], label="targeted output SHA-256"
        ),
        source_files=source_files,
        summary=_require_text(payload["summary"], label="targeted summary"),
        output_log_path=cast(str | None, payload["output_log_path"]),
        output_log_size_bytes=cast(
            int | None, payload["output_log_size_bytes"]
        ),
    )


def parse_phase1c_successor_closure_witness(
    value: object,
) -> Phase1CSuccessorClosureWitness:
    payload = _mapping(value, label="closure witness")
    _require_exact_keys(
        payload,
        {"commands", "format", "global_pytest_runs", "status", "v9"},
        label="closure witness",
    )
    if (
        payload["format"] != "hyperlab-storage-v4-phase1c-closure-v1"
        or payload["global_pytest_runs"] != 1
        or payload["status"]
        != "STORAGE_V4_PHASE_1C_REPOSITORY_CLOSURE_VERIFIED"
    ):
        raise Phase1CSuccessorError("closure witness contract differs")
    commands: list[Phase1CSuccessorCommandWitness] = []
    for item in _sequence(payload["commands"], label="closure commands"):
        command_payload = _mapping(item, label="closure command")
        _require_exact_keys(
            command_payload,
            {
                "command",
                "exit_code",
                "output_log_path",
                "output_log_size_bytes",
                "output_sha256",
                "purpose",
                "summary",
            },
            label="closure command",
        )
        commands.append(
            Phase1CSuccessorCommandWitness(
                purpose=_require_text(
                    command_payload["purpose"], label="closure purpose"
                ),
                command=tuple(
                    _require_text(value, label="closure command item")
                    for value in _sequence(
                        command_payload["command"], label="closure command"
                    )
                ),
                exit_code=cast(int, command_payload["exit_code"]),
                output_sha256=_require_sha256(
                    command_payload["output_sha256"],
                    label="closure output SHA-256",
                ),
                summary=_require_text(
                    command_payload["summary"], label="closure summary"
                ),
                output_log_path=cast(
                    str | None, command_payload["output_log_path"]
                ),
                output_log_size_bytes=cast(
                    int | None, command_payload["output_log_size_bytes"]
                ),
            )
        )
    v9_payload = _mapping(payload["v9"], label="V9 witness")
    _require_exact_keys(
        v9_payload,
        {
            "after_sha256",
            "before_sha256",
            "byte_identical",
            "path",
            "size_bytes",
        },
        label="V9 witness",
    )
    if v9_payload["byte_identical"] is not True:
        raise Phase1CSuccessorError("V9 witness does not prove byte identity")
    return Phase1CSuccessorClosureWitness(
        commands=tuple(commands),
        v9=Phase1CSuccessorV9ByteWitness(
            path=_require_text(v9_payload["path"], label="V9 path"),
            size_bytes=cast(int, v9_payload["size_bytes"]),
            before_sha256=_require_sha256(
                v9_payload["before_sha256"], label="V9 before SHA-256"
            ),
            after_sha256=_require_sha256(
                v9_payload["after_sha256"], label="V9 after SHA-256"
            ),
        ),
    )


def _load_canonical_witness(path: Path, *, label: str) -> dict[str, object]:
    data = _read_stable_regular_file(path, label=label, maximum_bytes=_MAX_LOG_BYTES)
    return _parse_canonical_json(data, label=label)


def load_phase1c_successor_test_witness(path: Path) -> Phase1CSuccessorTestWitness:
    return parse_phase1c_successor_test_witness(
        _load_canonical_witness(path, label="targeted test witness file")
    )


def load_phase1c_successor_closure_witness(
    path: Path,
) -> Phase1CSuccessorClosureWitness:
    return parse_phase1c_successor_closure_witness(
        _load_canonical_witness(path, label="closure witness file")
    )


@dataclass(frozen=True, slots=True)
class Phase1CSuccessorExpectations:
    boundary_commit_counts: tuple[int, ...]
    terminal_certificate_sha256: str
    terminal_manifest_sha256: str
    terminal_tree_sha256: str
    producer_code_identity: str
    producer_runtime_identity: str
    config_identity: str
    producer_stdout_size_bytes: int
    workload_profile: str
    workload_seed: int
    generator_version: str
    baseline_byte_witness_sha256: str
    baseline_byte_witness_size_bytes: int
    acquired_verifier_baseline_identity: str
    acquired_verifier_file_count: int
    baseline_commit: str
    producer_dependency_closure_sha256: str
    producer_dependency_closure_file_count: int
    producer_dependency_entrypoints: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            type(self.boundary_commit_counts) is not tuple
            or len(self.boundary_commit_counts) < 2
            or self.boundary_commit_counts
            != tuple(sorted(set(self.boundary_commit_counts)))
            or any(type(item) is not int or item < 1 for item in self.boundary_commit_counts)
        ):
            raise ValueError("successor boundary counts must be increasing")
        for label, digest_value in (
            ("terminal certificate", self.terminal_certificate_sha256),
            ("terminal manifest", self.terminal_manifest_sha256),
            ("terminal tree", self.terminal_tree_sha256),
            ("producer code identity", self.producer_code_identity),
            ("producer runtime identity", self.producer_runtime_identity),
            ("config identity", self.config_identity),
            ("baseline witness", self.baseline_byte_witness_sha256),
            ("acquired verifier", self.acquired_verifier_baseline_identity),
            ("producer dependency closure", self.producer_dependency_closure_sha256),
        ):
            _require_sha256(digest_value, label=label)
        _require_git_commit(self.baseline_commit, label="baseline commit")
        for label, count_value in (
            ("producer stdout size", self.producer_stdout_size_bytes),
            ("baseline witness size", self.baseline_byte_witness_size_bytes),
            ("acquired verifier file count", self.acquired_verifier_file_count),
            (
                "producer dependency closure file count",
                self.producer_dependency_closure_file_count,
            ),
        ):
            _require_positive_int(count_value, label=label)
        for label, value in (
            ("workload profile", self.workload_profile),
            ("generator version", self.generator_version),
        ):
            _require_text(value, label=label)
        if type(self.workload_seed) is not int or self.workload_seed < 0:
            raise ValueError("workload seed must be non-negative")
        if self.producer_dependency_entrypoints != _SUCCESSOR_PRODUCER_ENTRYPOINTS:
            raise ValueError("producer dependency entrypoints differ from exact scope")


@dataclass(frozen=True, slots=True)
class Phase1CSuccessorConfig:
    repository_root: Path
    baseline_byte_witness_path: Path
    source_mission_root: Path
    capacity_candidate_root: Path
    boundary_certificate_root: Path
    producer_stdout_log: Path
    producer_stdout_sha256: str
    run06_candidate_root: Path
    receipt_root: Path
    expectations: Phase1CSuccessorExpectations

    def __post_init__(self) -> None:
        for label, path in (
            ("repository_root", self.repository_root),
            ("baseline_byte_witness_path", self.baseline_byte_witness_path),
            ("source_mission_root", self.source_mission_root),
            ("capacity_candidate_root", self.capacity_candidate_root),
            ("boundary_certificate_root", self.boundary_certificate_root),
            ("producer_stdout_log", self.producer_stdout_log),
            ("run06_candidate_root", self.run06_candidate_root),
            ("receipt_root", self.receipt_root),
        ):
            if not isinstance(path, Path) or not path.is_absolute():
                raise ValueError(f"{label} must be an absolute pathlib.Path")
        _require_sha256(self.producer_stdout_sha256, label="producer stdout SHA-256")
        if not isinstance(self.expectations, Phase1CSuccessorExpectations):
            raise TypeError("expectations must be Phase1CSuccessorExpectations")
        if self.capacity_candidate_root != self.source_mission_root / "capacity-cumulative":
            raise ValueError("capacity candidate root differs from canonical layout")
        if self.boundary_certificate_root != (
            self.source_mission_root / ".capacity-cumulative.phase1c-boundaries"
        ):
            raise ValueError("boundary root differs from canonical layout")
        if self.run06_candidate_root != self.source_mission_root.parent / "native-capacity-06":
            raise ValueError("run06 candidate root differs from canonical sibling")
        if self.receipt_root == self.source_mission_root or self.receipt_root.is_relative_to(
            self.source_mission_root
        ):
            raise ValueError("successor receipt root must be outside producer mission")
        if self.receipt_root == self.run06_candidate_root or self.receipt_root.is_relative_to(
            self.run06_candidate_root
        ):
            raise ValueError("successor receipt root must not create candidate06")


@dataclass(frozen=True, slots=True)
class SuccessorFileWitness:
    path: Path
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        if not self.path.is_absolute():
            raise ValueError("file witness path must be absolute")
        _require_non_negative_int(self.size_bytes, label="file witness size")
        _require_sha256(self.sha256, label="file witness SHA-256")

    def payload(self) -> dict[str, object]:
        return {
            "path": str(self.path),
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class SuccessorAcquiredBaselineWitness:
    file: SuccessorFileWitness
    baseline_commit: str
    code_identity: SuccessorCodeIdentity
    dependency_closure: SuccessorDependencyClosureWitness
    acquisition: Mapping[str, object]

    def payload(self) -> dict[str, object]:
        return {
            "acquired_verifier_global_identity": self.code_identity.payload(),
            "acquisition": dict(self.acquisition),
            "artifact": SUCCESSOR_BASELINE_WITNESS_FORMAT,
            "file_witness": self.file.payload(),
            "producer_dependency_closure": self.dependency_closure.payload(),
        }


@dataclass(frozen=True, slots=True)
class SuccessorBoundaryCertificate:
    commit_count: int
    manifest_sha256: str
    previous_sha256: str | None
    sha256: str
    path: Path
    payload_mapping: Mapping[str, object]

    def payload(self) -> dict[str, object]:
        return {
            "commit_count": self.commit_count,
            "manifest_sha256": self.manifest_sha256,
            "path": str(self.path),
            "payload": dict(self.payload_mapping),
            "previous_sha256": self.previous_sha256,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class SuccessorMissionEntry:
    relative_path: str
    kind: str
    device: int
    inode: int
    size_bytes: int
    mtime_ns: int

    def payload(self) -> dict[str, object]:
        return {
            "device": self.device,
            "inode": self.inode,
            "kind": self.kind,
            "mtime_ns": self.mtime_ns,
            "relative_path": self.relative_path,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class SuccessorMissionRootWitness:
    root: Path
    root_identity: tuple[int, int, int, int]
    entries: tuple[SuccessorMissionEntry, ...]
    sha256: str

    def payload_without_sha256(self) -> dict[str, object]:
        return {
            "entries": [item.payload() for item in self.entries],
            "entry_count": len(self.entries),
            "root": str(self.root),
            "root_identity": list(self.root_identity),
        }

    def payload(self) -> dict[str, object]:
        return {**self.payload_without_sha256(), "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class SuccessorRun06AbsenceWitness:
    path: Path
    parent: Path
    parent_entries: tuple[str, ...]
    sha256: str

    def payload_without_sha256(self) -> dict[str, object]:
        return {
            "absent": True,
            "parent": str(self.parent),
            "parent_entries": list(self.parent_entries),
            "path": str(self.path),
            "run06_candidate_absent": True,
        }

    def payload(self) -> dict[str, object]:
        return {**self.payload_without_sha256(), "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class Phase1CSuccessorReattestation:
    source_mission_root: Path
    capacity_candidate_root: Path
    boundary_certificate_root: Path
    producer_stdout: SuccessorFileWitness
    acquired_baseline: SuccessorAcquiredBaselineWitness
    boundaries: tuple[SuccessorBoundaryCertificate, ...]
    terminal_tree: CandidateTreeWitness
    producer_code_identity: str
    producer_runtime_identity: str
    current_verifier: SuccessorVerifierState
    config_identity: str
    mission_before: SuccessorMissionRootWitness
    mission_after: SuccessorMissionRootWitness
    run06_before: SuccessorRun06AbsenceWitness
    run06_after: SuccessorRun06AbsenceWitness
    terminal_verdict: str = PHASE1C_TARGET_NOT_MET_VERDICT

    def __post_init__(self) -> None:
        if not self.boundaries:
            raise ValueError("successor reattestation requires boundaries")
        if self.terminal_tree.root != self.capacity_candidate_root:
            raise ValueError("terminal tree differs from candidate")
        if self.mission_before != self.mission_after:
            raise ValueError("producer mission changed during reattestation")
        if self.run06_before != self.run06_after:
            raise ValueError("run06 absence changed during reattestation")
        if (
            self.acquired_baseline.code_identity.sha256
            == self.current_verifier.successor_code_identity.sha256
        ):
            raise ValueError("acquired baseline must not masquerade as current verifier")
        if self.terminal_verdict != PHASE1C_TARGET_NOT_MET_VERDICT:
            raise ValueError("successor must preserve TARGET_NOT_MET")

    def payload(self) -> dict[str, object]:
        return {
            "acquired_verifier_baseline": self.acquired_baseline.payload(),
            "attribution": SUCCESSOR_ATTRIBUTION,
            "boundaries": [item.payload() for item in self.boundaries],
            "boundary_certificate_root": str(self.boundary_certificate_root),
            "capacity_candidate_root": str(self.capacity_candidate_root),
            "config_identity": self.config_identity,
            "current_final_verifier_identity": self.current_verifier.payload(),
            "current_verifier_dependency_closure": (
                self.current_verifier.dependency_closure.payload()
            ),
            "markers": list(SUCCESSOR_MARKERS),
            "mission_root_witness": {
                "after": self.mission_after.payload(),
                "before": self.mission_before.payload(),
                "candidate_05_unchanged": True,
            },
            "producer_dependency_closure": (
                self.acquired_baseline.dependency_closure.payload()
            ),
            "producer_identity": {
                "code_identity": self.producer_code_identity,
                "runtime_identity": self.producer_runtime_identity,
            },
            "producer_stdout": self.producer_stdout.payload(),
            "run06_absence_witness": {
                "after": self.run06_after.payload(),
                "before": self.run06_before.payload(),
                "run06_candidate_absent": True,
            },
            "source_mission_root": str(self.source_mission_root),
            "terminal_tree": self.terminal_tree.payload(),
            "terminal_verdict": self.terminal_verdict,
            "work_accounting": {
                "candidate_05_unchanged": True,
                "commits_ingested_during_succession": 0,
                "prefix_reingested": 0,
                "run06_commits": 0,
            },
        }

    @property
    def sha256(self) -> str:
        return _sha256(canonical_json_bytes(self.payload()))


@dataclass(frozen=True, slots=True)
class Phase1CSuccessorReceipt:
    root: Path
    path: Path
    sha256: str
    size_bytes: int
    reattestation: Phase1CSuccessorReattestation


@dataclass(frozen=True, slots=True)
class Phase1CSuccessorPublication:
    root: Path
    receipt_sha256: str
    report_sha256: str
    manifest_sha256: str
    pin_sha256: str
    complete_sha256: str
    verdict: str


def _parse_code_identity_payload(
    value: object, *, expected_format: str, repository_root: Path
) -> SuccessorCodeIdentity:
    payload = _mapping(value, label="code identity")
    _require_exact_keys(
        payload, {"files", "format", "repository_root", "sha256"}, label="code identity"
    )
    if payload["format"] != expected_format or payload["repository_root"] != str(
        repository_root
    ):
        raise Phase1CSuccessorError("code identity format/root differs")
    files_mapping = _mapping(payload["files"], label="code identity files")
    files: list[tuple[str, int, str]] = []
    for path, item in files_mapping.items():
        file_payload = _mapping(item, label="code identity file")
        _require_exact_keys(
            file_payload, {"bytes", "sha256"}, label="code identity file"
        )
        files.append(
            (
                _safe_relative_path(path, label="code identity path"),
                _require_non_negative_int(
                    file_payload["bytes"], label="code identity size"
                ),
                _require_sha256(
                    file_payload["sha256"], label="code identity file SHA-256"
                ),
            )
        )
    return SuccessorCodeIdentity(
        repository_root=repository_root,
        identity_format=expected_format,
        files=tuple(sorted(files)),
        sha256=_require_sha256(payload["sha256"], label="code identity SHA-256"),
    )


def _parse_dependency_closure(value: object) -> SuccessorDependencyClosureWitness:
    payload = _mapping(value, label="producer dependency closure")
    _require_exact_keys(
        payload,
        {"closure_sha256", "entrypoints", "file_count", "files", "format", "status"},
        label="producer dependency closure",
    )
    if payload["format"] != SUCCESSOR_DEPENDENCY_CLOSURE_FORMAT:
        raise Phase1CSuccessorError("producer dependency closure format differs")
    entrypoints = tuple(
        _require_text(item, label="dependency entrypoint")
        for item in _sequence(payload["entrypoints"], label="dependency entrypoints")
    )
    files: list[tuple[str, str]] = []
    for item in _sequence(payload["files"], label="dependency closure files"):
        file_payload = _mapping(item, label="dependency closure file")
        _require_exact_keys(
            file_payload, {"path", "sha256"}, label="dependency closure file"
        )
        files.append(
            (
                _safe_relative_path(
                    file_payload["path"], label="dependency closure path"
                ),
                _require_sha256(
                    file_payload["sha256"], label="dependency closure file SHA-256"
                ),
            )
        )
    if payload["file_count"] != len(files):
        raise Phase1CSuccessorError("dependency closure file count differs")
    return SuccessorDependencyClosureWitness(
        entrypoints=entrypoints,
        files=tuple(files),
        closure_sha256=_require_sha256(
            payload["closure_sha256"], label="dependency closure SHA-256"
        ),
        status=_require_text(payload["status"], label="dependency closure status"),
    )


def _load_acquired_baseline(
    config: Phase1CSuccessorConfig,
) -> SuccessorAcquiredBaselineWitness:
    data = _read_stable_regular_file(
        config.baseline_byte_witness_path,
        label="acquired verifier baseline byte witness",
        maximum_bytes=_MAX_CERTIFICATE_BYTES,
    )
    expected = config.expectations
    if (
        len(data) != expected.baseline_byte_witness_size_bytes
        or _sha256(data) != expected.baseline_byte_witness_sha256
    ):
        raise Phase1CSuccessorError("acquired baseline witness bytes differ from pin")
    payload = _parse_canonical_json(data, label="acquired verifier baseline witness")
    _require_exact_keys(
        payload,
        {
            "acquired_verifier_global_identity",
            "acquisition",
            "artifact",
            "producer_dependency_closure",
        },
        label="acquired verifier baseline witness",
    )
    if payload["artifact"] != SUCCESSOR_BASELINE_WITNESS_FORMAT:
        raise Phase1CSuccessorError("acquired baseline witness artifact differs")
    acquisition = _mapping(payload["acquisition"], label="baseline acquisition")
    _require_exact_keys(
        acquisition,
        {
            "baseline_commit",
            "checkout_filter_context",
            "excluded_untracked_source_paths",
            "git_filtered_snapshot_rejected",
            "method",
            "repository_root",
        },
        label="baseline acquisition",
    )
    if (
        acquisition["baseline_commit"] != expected.baseline_commit
        or acquisition["repository_root"] != str(config.repository_root)
        or acquisition["method"]
        != "STABLE_LIVE_WORKTREE_BYTES_BEFORE_SUCCESSOR_PATCH"
        or acquisition["checkout_filter_context"]
        != "MIXED_WORKTREE_BYTES_NOT_REPRODUCIBLE_FROM_GIT_FILTERS"
        or acquisition["git_filtered_snapshot_rejected"] is not True
        or acquisition["excluded_untracked_source_paths"]
        != list(_BASELINE_EXCLUDED_ADDITIONS)
    ):
        raise Phase1CSuccessorError("acquired baseline provenance differs")
    code_identity = _parse_code_identity_payload(
        payload["acquired_verifier_global_identity"],
        expected_format=PHASE1C_CODE_IDENTITY_FORMAT,
        repository_root=config.repository_root,
    )
    closure = _parse_dependency_closure(payload["producer_dependency_closure"])
    if (
        code_identity.sha256 != expected.acquired_verifier_baseline_identity
        or len(code_identity.files) != expected.acquired_verifier_file_count
        or closure.entrypoints != expected.producer_dependency_entrypoints
        or closure.closure_sha256
        != expected.producer_dependency_closure_sha256
        or len(closure.files) != expected.producer_dependency_closure_file_count
        or closure.status != PRODUCER_DEPENDENCY_CLOSURE_UNCHANGED
    ):
        raise Phase1CSuccessorError("acquired verifier identity/closure differs")
    global_digests = {path: digest for path, _, digest in code_identity.files}
    if any(global_digests.get(path) != digest for path, digest in closure.files):
        raise Phase1CSuccessorError("producer closure differs from acquired global bytes")
    return SuccessorAcquiredBaselineWitness(
        file=SuccessorFileWitness(
            path=config.baseline_byte_witness_path,
            size_bytes=len(data),
            sha256=_sha256(data),
        ),
        baseline_commit=expected.baseline_commit,
        code_identity=code_identity,
        dependency_closure=closure,
        acquisition=acquisition,
    )


def _validate_tree_payload(value: object, *, expected_root: Path) -> dict[str, object]:
    tree = _mapping(value, label="candidate tree witness")
    _require_exact_keys(
        tree,
        {
            "directories",
            "directory_count",
            "file_count",
            "files",
            "root",
            "total_bytes",
            "tree_sha256",
        },
        label="candidate tree witness",
    )
    if tree["root"] != str(expected_root):
        raise Phase1CSuccessorError("candidate tree root differs from authority")
    directories = tuple(
        _safe_relative_path(item, label="candidate tree directory")
        for item in _sequence(tree["directories"], label="candidate directories")
    )
    if directories != tuple(sorted(set(directories))):
        raise Phase1CSuccessorError("candidate tree directories are ambiguous")
    file_paths: list[str] = []
    total_bytes = 0
    for ordinal, item in enumerate(
        _sequence(tree["files"], label="candidate files"), start=1
    ):
        file_value = _mapping(item, label=f"candidate file {ordinal}")
        _require_exact_keys(
            file_value,
            {"relative_path", "sha256", "size_bytes"},
            label=f"candidate file {ordinal}",
        )
        file_paths.append(
            _safe_relative_path(
                file_value["relative_path"], label=f"candidate file {ordinal} path"
            )
        )
        total_bytes += _require_non_negative_int(
            file_value["size_bytes"], label=f"candidate file {ordinal} size"
        )
        _require_sha256(
            file_value["sha256"], label=f"candidate file {ordinal} SHA-256"
        )
    if not file_paths or tuple(file_paths) != tuple(sorted(set(file_paths))):
        raise Phase1CSuccessorError("candidate tree files are empty or ambiguous")
    if tree["file_count"] != len(file_paths):
        raise Phase1CSuccessorError("candidate tree file count differs")
    if tree["directory_count"] != len(directories) + 1:
        raise Phase1CSuccessorError("candidate tree directory count differs")
    if tree["total_bytes"] != total_bytes:
        raise Phase1CSuccessorError("candidate tree byte total differs")
    observed_sha256 = _require_sha256(
        tree["tree_sha256"], label="candidate tree SHA-256"
    )
    material = dict(tree)
    del material["tree_sha256"]
    if _sha256(canonical_json_bytes(material)) != observed_sha256:
        raise Phase1CSuccessorError("candidate tree SHA-256 differs from manifest")
    return tree


def _manifest_shape(manifest: Mapping[str, object]) -> bytes:
    normalized = dict(manifest)
    configuration = dict(_mapping(manifest["configuration"], label="configuration"))
    configuration["commit_count"] = 0
    normalized["configuration"] = configuration
    activity = dict(_mapping(manifest["activity_rates"], label="activity rates"))
    activity["market_gap_count"] = 0
    normalized["activity_rates"] = activity
    normalized["expected"] = {
        "commit_count": 0,
        "logical_row_count": 0,
        "workload_sha256": "0" * 64,
    }
    return canonical_json_bytes(normalized)


def _validate_manifest(
    value: object,
    *,
    commit_count: int,
    expectations: Phase1CSuccessorExpectations,
) -> tuple[dict[str, object], int, str, int]:
    manifest = _mapping(value, label="boundary workload manifest")
    _require_exact_keys(manifest, _MANIFEST_KEYS, label="boundary workload manifest")
    if manifest["artifact"] != "STORAGE_V4_SYNTHETIC_CAPACITY_WORKLOAD_MANIFEST_V1":
        raise Phase1CSuccessorError("boundary workload artifact differs")
    if (
        manifest["profile"] != expectations.workload_profile
        or manifest["seed"] != expectations.workload_seed
        or manifest["generator_version"] != expectations.generator_version
        or manifest["markers"] != list(CAPACITY_MARKERS)
    ):
        raise Phase1CSuccessorError("boundary workload identity differs")
    configuration = _mapping(manifest["configuration"], label="workload configuration")
    if configuration.get("commit_count") != commit_count:
        raise Phase1CSuccessorError("workload configuration count differs")
    expected = _mapping(manifest["expected"], label="workload expected digest")
    _require_exact_keys(
        expected,
        {"commit_count", "logical_row_count", "workload_sha256"},
        label="workload expected digest",
    )
    if expected["commit_count"] != commit_count:
        raise Phase1CSuccessorError("workload expected commit count differs")
    logical_rows = _require_positive_int(
        expected["logical_row_count"], label="workload logical row count"
    )
    workload_sha256 = _require_sha256(
        expected["workload_sha256"], label="workload SHA-256"
    )
    activity = _mapping(manifest["activity_rates"], label="workload activity rates")
    market_gap_count = _require_non_negative_int(
        activity.get("market_gap_count"), label="workload market gap count"
    )
    if market_gap_count > commit_count:
        raise Phase1CSuccessorError("workload market gaps exceed commits")
    return manifest, logical_rows, workload_sha256, market_gap_count


def _validate_measurement(
    value: object,
    *,
    commit_count: int,
    logical_rows: int,
    manifest_sha256: str,
    workload_sha256: str,
) -> None:
    measurement = _mapping(value, label="boundary measurement")
    _require_exact_keys(measurement, _MEASUREMENT_KEYS, label="boundary measurement")
    counts = _mapping(measurement["counts"], label="measurement counts")
    _require_exact_keys(
        counts,
        {"checkpoints", "commits", "logical_rows", "manifests", "segments"},
        label="measurement counts",
    )
    if counts["commits"] != commit_count or counts["logical_rows"] != logical_rows:
        raise Phase1CSuccessorError("measurement counts differ from workload prefix")
    for name in ("checkpoints", "manifests", "segments"):
        _require_positive_int(counts[name], label=f"measurement {name}")
    if (
        measurement["workload_manifest_sha256"] != manifest_sha256
        or measurement["observed_workload_sha256"] != workload_sha256
        or measurement["markers"] != list(CAPACITY_MARKERS)
    ):
        raise Phase1CSuccessorError("measurement workload binding differs")
    _require_positive_int(measurement["wall_ns"], label="measurement wall time")
    _require_non_negative_int(measurement["cpu_ns"], label="measurement CPU time")
    _require_positive_int(
        measurement["full_history_audit_ns"], label="full-history audit duration"
    )
    census = _mapping(measurement["byte_census"], label="measurement byte census")
    raw_bytes = _require_positive_int(census.get("raw_bytes"), label="raw bytes")
    paper_bytes = _require_positive_int(
        census.get("paper_incremental_bytes"), label="Paper bytes"
    )
    if census.get("total_bytes") != raw_bytes + paper_bytes:
        raise Phase1CSuccessorError("measurement byte census total differs")


def _validate_evidence(
    value: object,
    *,
    candidate_root: Path,
    expectations: Phase1CSuccessorExpectations,
    commit_count: int,
    logical_rows: int,
    workload_sha256: str,
    market_gap_count: int,
) -> dict[str, object]:
    evidence = _mapping(value, label="boundary evidence")
    _require_exact_keys(evidence, _EVIDENCE_KEYS, label="boundary evidence")
    authority = _mapping(evidence["authority"], label="boundary authority")
    _require_exact_keys(
        authority, _EVIDENCE_AUTHORITY_KEYS, label="boundary evidence authority"
    )
    if (
        authority["candidate_root"] != str(candidate_root)
        or authority["code_identity"] != expectations.producer_code_identity
        or authority["runtime_identity"] != expectations.producer_runtime_identity
        or authority["config_identity"] != expectations.config_identity
    ):
        raise Phase1CSuccessorError("boundary producer authority differs")
    integrity = _mapping(evidence["integrity"], label="boundary integrity")
    _require_exact_keys(
        integrity, _EVIDENCE_INTEGRITY_KEYS, label="boundary evidence integrity"
    )
    if (
        integrity["alignment_status"] != "PHASE1C_RAW_PAPER_ALIGNED"
        or integrity["commit_count"] != commit_count
        or integrity["oracle_commit_count"] != commit_count
        or integrity["oracle_logical_row_count"] != logical_rows
        or integrity["oracle_workload_sha256"] != workload_sha256
        or integrity["market_gap_count"] != market_gap_count
        or integrity["oracle_final_prefix_root"] != integrity["final_prefix_root"]
        or integrity["raw_reference_count"] != commit_count
    ):
        raise Phase1CSuccessorError("boundary audit/oracle exactness differs")
    for label in (
        "final_prefix_root",
        "oracle_final_prefix_root",
        "raw_reference_prefix_root",
    ):
        _require_sha256(integrity[label], label=f"boundary {label}")
    return _validate_tree_payload(
        integrity["audited_candidate_tree"], expected_root=candidate_root
    )


def _load_boundary_chain(
    config: Phase1CSuccessorConfig,
) -> tuple[tuple[SuccessorBoundaryCertificate, ...], dict[str, object]]:
    root = _require_absolute_direct_path(
        config.boundary_certificate_root,
        label="boundary certificate root",
        directory=True,
    )
    expected_counts = config.expectations.boundary_commit_counts
    entries = tuple(sorted(root.iterdir(), key=lambda item: item.name))
    if len(entries) != len(expected_counts):
        raise Phase1CSuccessorError("boundary namespace is incomplete or forked")
    certificates: list[SuccessorBoundaryCertificate] = []
    previous: str | None = None
    terminal_tree: dict[str, object] | None = None
    normalized_shape: bytes | None = None
    for expected_count, path in zip(expected_counts, entries, strict=True):
        data = _read_stable_regular_file(
            path,
            label=f"boundary certificate {expected_count}",
            maximum_bytes=_MAX_CERTIFICATE_BYTES,
        )
        value = _parse_canonical_json(data, label=f"boundary certificate {expected_count}")
        _require_exact_keys(value, _BOUNDARY_KEYS, label="boundary certificate")
        if (
            value["artifact"] != SUCCESSOR_BOUNDARY_ARTIFACT
            or value["boundary_commit_count"] != expected_count
        ):
            raise Phase1CSuccessorError("boundary artifact/count differs")
        manifest, logical_rows, workload_sha256, market_gap_count = _validate_manifest(
            value["boundary_manifest"],
            commit_count=expected_count,
            expectations=config.expectations,
        )
        manifest_sha256 = _sha256(canonical_json_bytes(manifest))
        if (
            value["boundary_manifest_sha256"] != manifest_sha256
            or value["terminal_manifest_sha256"]
            != config.expectations.terminal_manifest_sha256
            or path.name != f"{expected_count:016d}-{manifest_sha256}.json"
        ):
            raise Phase1CSuccessorError("boundary manifest/path binding differs")
        prefix = _mapping(value["workload_prefix"], label="workload prefix")
        _require_exact_keys(
            prefix,
            {"commit_count", "logical_row_count", "sha256"},
            label="workload prefix",
        )
        if prefix != {
            "commit_count": expected_count,
            "logical_row_count": logical_rows,
            "sha256": workload_sha256,
        }:
            raise Phase1CSuccessorError("workload prefix differs from manifest")
        _validate_measurement(
            value["measurement"],
            commit_count=expected_count,
            logical_rows=logical_rows,
            manifest_sha256=manifest_sha256,
            workload_sha256=workload_sha256,
        )
        audited_tree = _validate_evidence(
            value["evidence"],
            candidate_root=config.capacity_candidate_root,
            expectations=config.expectations,
            commit_count=expected_count,
            logical_rows=logical_rows,
            workload_sha256=workload_sha256,
            market_gap_count=market_gap_count,
        )
        authority = _mapping(value["authority"], label="boundary authority")
        _require_exact_keys(
            authority,
            {"checkpoint_root", "paper_manifest_root", "raw_manifest_root"},
            label="boundary authority",
        )
        for label, digest in authority.items():
            _require_sha256(digest, label=f"boundary authority {label}")
        if value["previous_certificate_sha256"] != previous:
            raise Phase1CSuccessorError("boundary certificate chain is forked or has a gap")
        digest = _sha256(data)
        certificates.append(
            SuccessorBoundaryCertificate(
                commit_count=expected_count,
                manifest_sha256=manifest_sha256,
                previous_sha256=previous,
                sha256=digest,
                path=path,
                payload_mapping=value,
            )
        )
        previous = digest
        shape = _manifest_shape(manifest)
        if normalized_shape is None:
            normalized_shape = shape
        elif shape != normalized_shape:
            raise Phase1CSuccessorError("cumulative boundaries differ outside prefixes")
        terminal_tree = audited_tree
    terminal = certificates[-1]
    if (
        terminal.sha256 != config.expectations.terminal_certificate_sha256
        or terminal.manifest_sha256 != config.expectations.terminal_manifest_sha256
        or terminal_tree is None
        or terminal_tree["tree_sha256"] != config.expectations.terminal_tree_sha256
    ):
        raise Phase1CSuccessorError("terminal proof differs from pinned authority")
    terminal_measurement = _mapping(
        terminal.payload_mapping["measurement"], label="terminal measurement"
    )
    target = _mapping(
        terminal_measurement["storage_growth_target"], label="terminal target"
    )
    if target.get("status") != "AVAILABLE" or target.get("passed") is not False:
        raise Phase1CSuccessorError("terminal evidence does not prove TARGET_NOT_MET")
    return tuple(certificates), terminal_tree


def _file_witness(
    path: Path, *, expected_sha256: str, expected_size: int, label: str
) -> SuccessorFileWitness:
    data = _read_stable_regular_file(path, label=label, maximum_bytes=_MAX_LOG_BYTES)
    if len(data) != expected_size or _sha256(data) != expected_sha256:
        raise Phase1CSuccessorError(f"{label} differs from pinned authority")
    return SuccessorFileWitness(path=path, size_bytes=len(data), sha256=_sha256(data))


def _witness_mission_root(root: Path) -> SuccessorMissionRootWitness:
    root = _require_absolute_direct_path(root, label="source mission root", directory=True)
    before = os.lstat(root)
    entries: list[SuccessorMissionEntry] = []
    try:
        for directory, names, filenames in os.walk(root, topdown=True, followlinks=False):
            base = Path(directory)
            names.sort()
            filenames.sort()
            for name in names:
                path = base / name
                observed = os.lstat(path)
                if not stat.S_ISDIR(observed.st_mode) or _is_reparse(observed):
                    raise Phase1CSuccessorError("mission root contains unsafe directory")
                entries.append(
                    SuccessorMissionEntry(
                        relative_path=path.relative_to(root).as_posix(),
                        kind="DIRECTORY",
                        device=int(observed.st_dev),
                        inode=int(observed.st_ino),
                        size_bytes=int(observed.st_size),
                        mtime_ns=int(observed.st_mtime_ns),
                    )
                )
            for name in filenames:
                path = base / name
                observed = os.lstat(path)
                if not stat.S_ISREG(observed.st_mode) or _is_reparse(observed):
                    raise Phase1CSuccessorError("mission root contains unsafe file")
                entries.append(
                    SuccessorMissionEntry(
                        relative_path=path.relative_to(root).as_posix(),
                        kind="FILE",
                        device=int(observed.st_dev),
                        inode=int(observed.st_ino),
                        size_bytes=int(observed.st_size),
                        mtime_ns=int(observed.st_mtime_ns),
                    )
                )
    except OSError as error:
        raise Phase1CSuccessorError("source mission namespace is unreadable") from error
    after = os.lstat(root)
    if _stat_identity(before) != _stat_identity(after):
        raise Phase1CSuccessorError("source mission root changed while witnessed")
    entries_tuple = tuple(sorted(entries, key=lambda item: item.relative_path))
    material = {
        "entries": [item.payload() for item in entries_tuple],
        "entry_count": len(entries_tuple),
        "root": str(root),
        "root_identity": list(_stat_identity(after)),
    }
    return SuccessorMissionRootWitness(
        root=root,
        root_identity=_stat_identity(after),
        entries=entries_tuple,
        sha256=_sha256(canonical_json_bytes(material)),
    )


def _witness_run06_absence(path: Path) -> SuccessorRun06AbsenceWitness:
    parent = _require_absolute_direct_path(
        path.parent, label="run06 parent", directory=True
    )
    try:
        os.lstat(path)
    except FileNotFoundError:
        pass
    except OSError as error:
        raise Phase1CSuccessorError("run06 candidate absence is ambiguous") from error
    else:
        raise Phase1CSuccessorError("run06 candidate exists; successor must not use it")
    names = tuple(sorted(item.name for item in parent.iterdir()))
    material = {
        "absent": True,
        "parent": str(parent),
        "parent_entries": list(names),
        "path": str(path),
        "run06_candidate_absent": True,
    }
    return SuccessorRun06AbsenceWitness(
        path=path,
        parent=parent,
        parent_entries=names,
        sha256=_sha256(canonical_json_bytes(material)),
    )


def verify_phase1c_successor(
    config: Phase1CSuccessorConfig,
    *,
    progress: ProgressCallback | None = None,
) -> Phase1CSuccessorReattestation:
    """Reattest candidate-05 read-only without executing a producer."""

    if not isinstance(config, Phase1CSuccessorConfig):
        raise TypeError("config must be Phase1CSuccessorConfig")
    _require_absolute_direct_path(
        config.repository_root, label="repository root", directory=True
    )
    baseline = _load_acquired_baseline(config)
    current_verifier = compute_phase1c_successor_verifier_state(
        config.repository_root
    )
    if (
        current_verifier.global_code_identity.sha256
        == config.expectations.acquired_verifier_baseline_identity
    ):
        raise Phase1CSuccessorError(
            "acquired verifier baseline is not the current final verifier identity"
        )
    run06_before = _witness_run06_absence(config.run06_candidate_root)
    mission_before = _witness_mission_root(config.source_mission_root)
    _require_absolute_direct_path(
        config.capacity_candidate_root, label="capacity candidate root", directory=True
    )
    boundaries, terminal_tree_payload = _load_boundary_chain(config)
    producer_stdout = _file_witness(
        config.producer_stdout_log,
        expected_sha256=config.producer_stdout_sha256,
        expected_size=config.expectations.producer_stdout_size_bytes,
        label="producer stdout log",
    )
    observed_tree = witness_candidate_tree(
        config.capacity_candidate_root, progress=progress
    )
    if observed_tree.payload() != terminal_tree_payload:
        raise Phase1CSuccessorError(
            "current candidate bytes differ from old producer terminal tree"
        )
    mission_after = _witness_mission_root(config.source_mission_root)
    run06_after = _witness_run06_absence(config.run06_candidate_root)
    if mission_before != mission_after:
        raise Phase1CSuccessorError("candidate05 mission changed during reattestation")
    if run06_before != run06_after:
        raise Phase1CSuccessorError("run06 absence changed during reattestation")
    return Phase1CSuccessorReattestation(
        source_mission_root=config.source_mission_root,
        capacity_candidate_root=config.capacity_candidate_root,
        boundary_certificate_root=config.boundary_certificate_root,
        producer_stdout=producer_stdout,
        acquired_baseline=baseline,
        boundaries=boundaries,
        terminal_tree=observed_tree,
        producer_code_identity=config.expectations.producer_code_identity,
        producer_runtime_identity=config.expectations.producer_runtime_identity,
        current_verifier=current_verifier,
        config_identity=config.expectations.config_identity,
        mission_before=mission_before,
        mission_after=mission_after,
        run06_before=run06_before,
        run06_after=run06_after,
    )


def _ensure_absent_root(path: Path) -> Path:
    if not path.is_absolute():
        raise Phase1CSuccessorError("successor receipt root must be absolute")
    parent = path.parent.resolve(strict=True)
    if path.exists():
        raise Phase1CSuccessorError("successor receipt root already exists")
    try:
        path.mkdir()
        fsync_directory(parent)
    except (OSError, DurabilityError) as error:
        raise Phase1CSuccessorError("successor receipt root creation failed") from error
    return path.resolve(strict=True)


def _publish_immutable(path: Path, payload: Mapping[str, object]) -> tuple[str, int]:
    data = canonical_json_bytes(dict(payload))
    try:
        durable_publish_immutable(path, data)
    except (OSError, DurabilityError) as error:
        raise Phase1CSuccessorError(f"immutable publication failed: {path.name}") from error
    observed = _read_stable_regular_file(
        path, label=f"published {path.name}", maximum_bytes=_MAX_LOG_BYTES
    )
    if observed != data:
        raise Phase1CSuccessorError(f"published bytes differ: {path.name}")
    return _sha256(data), len(data)


def reattest_phase1c_successor(
    config: Phase1CSuccessorConfig,
    *,
    progress: ProgressCallback | None = None,
) -> Phase1CSuccessorReceipt:
    result = verify_phase1c_successor(config, progress=progress)
    root = _ensure_absent_root(config.receipt_root)
    payload = {
        "artifact": SUCCESSOR_RECEIPT_FORMAT,
        "markers": list(SUCCESSOR_MARKERS),
        "payload": result.payload(),
        "reattestation_sha256": result.sha256,
        "status": SUCCESSOR_REATTESTED_STATUS,
    }
    path = root / SUCCESSOR_RECEIPT_NAME
    digest, size = _publish_immutable(path, payload)
    if {item.name for item in root.iterdir()} != {SUCCESSOR_RECEIPT_NAME}:
        raise Phase1CSuccessorError("receipt root contains unexpected entries")
    return Phase1CSuccessorReceipt(
        root=root,
        path=path,
        sha256=digest,
        size_bytes=size,
        reattestation=result,
    )


def _path_is_under(path: Path, root: Path) -> bool:
    return path == root or path.is_relative_to(root)


def _verify_witnessed_log(
    path_text: str | None,
    size: int | None,
    digest: str,
    *,
    forbidden_roots: tuple[Path, ...],
    forbidden_paths: tuple[Path, ...],
) -> Path:
    if path_text is None or size is None:
        raise Phase1CSuccessorError("closure and targeted logs must be durable")
    path = Path(path_text)
    resolved = _require_absolute_direct_path(path, label="gate output log", directory=False)
    if resolved in forbidden_paths or any(
        _path_is_under(resolved, root) for root in forbidden_roots
    ):
        raise Phase1CSuccessorError("gate witness attempts to reopen producer evidence")
    data = _read_stable_regular_file(
        resolved, label="closure/test output log", maximum_bytes=_MAX_LOG_BYTES
    )
    if len(data) != size or _sha256(data) != digest:
        raise Phase1CSuccessorError("closure/test output log differs from witness")
    return resolved


def _verify_gate_witnesses(
    repository_root: Path,
    targeted_tests: Phase1CSuccessorTestWitness,
    closure: Phase1CSuccessorClosureWitness,
    *,
    forbidden_roots: tuple[Path, ...],
    forbidden_paths: tuple[Path, ...],
) -> None:
    targeted_log = _verify_witnessed_log(
        targeted_tests.output_log_path,
        targeted_tests.output_log_size_bytes,
        targeted_tests.output_sha256,
        forbidden_roots=forbidden_roots,
        forbidden_paths=forbidden_paths,
    )
    if (
        targeted_log.name != SUCCESSOR_TARGETED_LOG_NAME
        or targeted_log.parent.name != "logs"
    ):
        raise Phase1CSuccessorError("targeted test log path differs from canonical layout")
    gate_root = _require_absolute_direct_path(
        targeted_log.parent.parent, label="successor gate root", directory=True
    )
    for forbidden in forbidden_roots:
        if _path_is_under(gate_root, forbidden) or _path_is_under(
            forbidden, gate_root
        ):
            raise Phase1CSuccessorError("successor gate root overlaps producer evidence")
    targeted_basetemp = _canonical_targeted_pytest_basetemp(targeted_tests.command)
    global_basetemp = _canonical_global_pytest_basetemp(
        closure.commands[4].command
    )
    if targeted_basetemp != gate_root / "pytest" / "targeted" or (
        global_basetemp != gate_root / "pytest" / "global"
    ):
        raise Phase1CSuccessorError("pytest basetemps differ from canonical gate layout")
    _require_absolute_direct_path(
        targeted_basetemp, label="targeted pytest basetemp", directory=True
    )
    _require_absolute_direct_path(
        global_basetemp, label="global pytest basetemp", directory=True
    )
    for relative_path, digest in targeted_tests.source_files:
        safe = _safe_relative_path(relative_path, label="targeted test source")
        path = _safe_repository_file(repository_root, safe)
        data = _read_stable_regular_file(
            path, label="targeted test source", maximum_bytes=_MAX_LOG_BYTES
        )
        if _sha256(data) != digest:
            raise Phase1CSuccessorError("targeted test source changed after gate")
    for command, expected_name in zip(
        closure.commands, _CLOSURE_LOG_NAMES, strict=True
    ):
        observed_log = _verify_witnessed_log(
            command.output_log_path,
            command.output_log_size_bytes,
            command.output_sha256,
            forbidden_roots=forbidden_roots,
            forbidden_paths=forbidden_paths,
        )
        if observed_log != gate_root / "logs" / expected_name:
            raise Phase1CSuccessorError("closure log path differs from canonical layout")
    raw_v9 = repository_root / PurePosixPath(SUCCESSOR_V9_RELATIVE_PATH)
    if raw_v9.is_absolute():
        v9_path = raw_v9
    else:
        relative = _safe_relative_path(closure.v9.path, label="V9 relative path")
        v9_path = repository_root / PurePosixPath(relative)
    resolved_v9 = _require_absolute_direct_path(
        v9_path, label="V9 attestation", directory=False
    )
    if resolved_v9 in forbidden_paths or any(
        _path_is_under(resolved_v9, root) for root in forbidden_roots
    ):
        raise Phase1CSuccessorError("V9 witness attempts to reopen producer evidence")
    data = _read_stable_regular_file(
        resolved_v9, label="V9 attestation", maximum_bytes=_MAX_LOG_BYTES
    )
    if (
        len(data) != closure.v9.size_bytes
        or _sha256(data) != closure.v9.before_sha256
        or closure.v9.before_sha256 != closure.v9.after_sha256
    ):
        raise Phase1CSuccessorError("V9 attestation differs at successor closure")


def _load_receipt(root: Path, expected_sha256: str) -> tuple[dict[str, object], bytes]:
    _require_absolute_direct_path(root, label="successor receipt root", directory=True)
    data = _read_stable_regular_file(
        root / SUCCESSOR_RECEIPT_NAME,
        label="successor receipt",
        maximum_bytes=_MAX_LOG_BYTES,
    )
    if _sha256(data) != expected_sha256:
        raise Phase1CSuccessorError("successor receipt SHA-256 differs")
    value = _parse_canonical_json(data, label="successor receipt")
    _require_exact_keys(
        value,
        {"artifact", "markers", "payload", "reattestation_sha256", "status"},
        label="successor receipt",
    )
    if (
        value["artifact"] != SUCCESSOR_RECEIPT_FORMAT
        or value["markers"] != list(SUCCESSOR_MARKERS)
        or value["status"] != SUCCESSOR_REATTESTED_STATUS
    ):
        raise Phase1CSuccessorError("successor receipt contract differs")
    payload = _mapping(value["payload"], label="successor receipt payload")
    if _sha256(canonical_json_bytes(payload)) != value["reattestation_sha256"]:
        raise Phase1CSuccessorError("successor receipt payload SHA-256 differs")
    if (
        payload.get("markers") != list(SUCCESSOR_MARKERS)
        or payload.get("attribution") != SUCCESSOR_ATTRIBUTION
        or payload.get("terminal_verdict") != PHASE1C_TARGET_NOT_MET_VERDICT
        or payload.get("work_accounting")
        != {
            "candidate_05_unchanged": True,
            "commits_ingested_during_succession": 0,
            "prefix_reingested": 0,
            "run06_commits": 0,
        }
    ):
        raise Phase1CSuccessorError("successor receipt accounting/verdict differs")
    mission = _mapping(payload.get("mission_root_witness"), label="mission witness")
    run06 = _mapping(payload.get("run06_absence_witness"), label="run06 witness")
    if (
        mission.get("candidate_05_unchanged") is not True
        or mission.get("before") != mission.get("after")
        or run06.get("run06_candidate_absent") is not True
        or run06.get("before") != run06.get("after")
    ):
        raise Phase1CSuccessorError("successor immutability/absence proof differs")
    terminal_tree = _mapping(payload.get("terminal_tree"), label="terminal tree")
    _require_sha256(terminal_tree.get("tree_sha256"), label="terminal tree SHA-256")
    producer_closure = _mapping(
        payload.get("producer_dependency_closure"), label="producer closure"
    )
    if producer_closure.get("status") != PRODUCER_DEPENDENCY_CLOSURE_UNCHANGED:
        raise Phase1CSuccessorError("producer dependency closure marker is missing")
    return value, data


def _require_finalization_tree(root: Path) -> None:
    allowed_files = {
        SUCCESSOR_RECEIPT_NAME,
        SUCCESSOR_REPORT_NAME,
        SUCCESSOR_MANIFEST_NAME,
        SUCCESSOR_COMPLETE_NAME,
    }
    for entry in root.iterdir():
        observed = os.lstat(entry)
        if stat.S_ISLNK(observed.st_mode) or _is_reparse(observed):
            raise Phase1CSuccessorError("successor output contains unsafe entry")
        if stat.S_ISREG(observed.st_mode):
            if entry.name not in allowed_files:
                raise Phase1CSuccessorError("successor output contains extra file")
        elif stat.S_ISDIR(observed.st_mode):
            if entry.name != "pin":
                raise Phase1CSuccessorError("successor output contains extra directory")
            if {item.name for item in entry.iterdir()} != {
                "certification.pin.json"
            }:
                raise Phase1CSuccessorError("successor pin directory is ambiguous")
        else:
            raise Phase1CSuccessorError("successor output contains special entry")


def _reattest_finalization_artifact(
    path: Path,
    *,
    expected_payload: Mapping[str, object],
    expected_sha256: str,
    expected_size_bytes: int,
    label: str,
) -> None:
    expected_bytes = canonical_json_bytes(dict(expected_payload))
    if (
        len(expected_bytes) != expected_size_bytes
        or _sha256(expected_bytes) != expected_sha256
    ):
        raise Phase1CSuccessorError(
            f"{label} in-memory authority is internally inconsistent"
        )
    observed = _read_stable_regular_file(
        path,
        label=label,
        maximum_bytes=_MAX_LOG_BYTES,
    )
    observed_payload = _parse_canonical_json(observed, label=label)
    if (
        len(observed) != expected_size_bytes
        or _sha256(observed) != expected_sha256
        or observed != expected_bytes
        or observed_payload != dict(expected_payload)
    ):
        raise Phase1CSuccessorError(
            f"{label} differs before COMPLETE publication"
        )


def finalize_phase1c_successor_closure(
    *,
    repository_root: Path,
    receipt_root: Path,
    expected_receipt_sha256: str,
    targeted_tests: Phase1CSuccessorTestWitness,
    closure: Phase1CSuccessorClosureWitness,
) -> Phase1CSuccessorPublication:
    """Publish closure/COMPLETE without reopening producer candidate bytes."""

    if not isinstance(targeted_tests, Phase1CSuccessorTestWitness):
        raise TypeError("targeted_tests has wrong type")
    if not isinstance(closure, Phase1CSuccessorClosureWitness):
        raise TypeError("closure has wrong type")
    _require_sha256(expected_receipt_sha256, label="expected receipt SHA-256")
    repository_root = _require_absolute_direct_path(
        repository_root, label="repository root", directory=True
    )
    root = _require_absolute_direct_path(
        receipt_root, label="successor receipt root", directory=True
    )
    _require_finalization_tree(root)
    receipt, receipt_bytes = _load_receipt(root, expected_receipt_sha256)
    payload = _mapping(receipt["payload"], label="successor receipt payload")
    stored_verifier = _mapping(
        payload.get("current_final_verifier_identity"), label="current verifier"
    )
    current_verifier = compute_phase1c_successor_verifier_state(repository_root)
    if stored_verifier != current_verifier.payload():
        raise Phase1CSuccessorError("current successor verifier identity changed")
    if payload.get("current_verifier_dependency_closure") != (
        current_verifier.dependency_closure.payload()
    ):
        raise Phase1CSuccessorError("current verifier dependency closure changed")
    source_root = Path(_require_text(payload["source_mission_root"], label="source root"))
    candidate_root = Path(
        _require_text(payload["capacity_candidate_root"], label="candidate root")
    )
    boundary_root = Path(
        _require_text(payload["boundary_certificate_root"], label="boundary root")
    )
    producer_stdout = _mapping(payload["producer_stdout"], label="producer stdout")
    producer_stdout_path = Path(
        _require_text(producer_stdout["path"], label="producer stdout path")
    )
    run06_witness = _mapping(
        payload["run06_absence_witness"], label="run06 witness"
    )
    run06_before = _mapping(run06_witness["before"], label="run06 before witness")
    run06_root = Path(_require_text(run06_before["path"], label="run06 root"))
    forbidden_roots = (
        source_root, candidate_root, boundary_root, run06_root, root
    )
    forbidden_paths = (producer_stdout_path,)
    _verify_gate_witnesses(
        repository_root,
        targeted_tests,
        closure,
        forbidden_roots=forbidden_roots,
        forbidden_paths=forbidden_paths,
    )
    common = {
        "acquired_verifier_baseline": payload["acquired_verifier_baseline"],
        "attribution": SUCCESSOR_ATTRIBUTION,
        "current_final_verifier_identity": stored_verifier,
        "producer_dependency_closure": payload["producer_dependency_closure"],
        "producer_identity": payload["producer_identity"],
        "terminal_tree_sha256": _mapping(
            payload["terminal_tree"], label="terminal tree"
        )["tree_sha256"],
        "terminal_verdict": PHASE1C_TARGET_NOT_MET_VERDICT,
        "work_accounting": payload["work_accounting"],
    }
    report_payload = {
        "artifact": SUCCESSOR_REPORT_FORMAT,
        "markers": list(SUCCESSOR_MARKERS),
        "payload": {
            **common,
            "closure": closure.payload(),
            "closure_sha256": closure.sha256,
            "receipt_sha256": expected_receipt_sha256,
            "targeted_tests": targeted_tests.payload(),
            "targeted_tests_sha256": targeted_tests.sha256,
        },
        "status": SUCCESSOR_CLOSURE_STATUS,
    }
    report_sha256, report_size = _publish_immutable(
        root / SUCCESSOR_REPORT_NAME, report_payload
    )
    manifest_payload = {
        "artifact": SUCCESSOR_MANIFEST_FORMAT,
        "artifacts": {
            SUCCESSOR_RECEIPT_NAME: {
                "sha256": expected_receipt_sha256,
                "size_bytes": len(receipt_bytes),
            },
            SUCCESSOR_REPORT_NAME: {
                "sha256": report_sha256,
                "size_bytes": report_size,
            },
        },
        "markers": list(SUCCESSOR_MARKERS),
        "payload": common,
        "status": SUCCESSOR_CLOSURE_STATUS,
    }
    manifest_sha256, manifest_size = _publish_immutable(
        root / SUCCESSOR_MANIFEST_NAME, manifest_payload
    )
    pin_root = root / "pin"
    if not pin_root.exists():
        try:
            pin_root.mkdir()
            fsync_directory(root)
        except OSError as error:
            raise Phase1CSuccessorError("successor pin directory creation failed") from error
    _require_absolute_direct_path(pin_root, label="successor pin root", directory=True)
    pin_payload = {
        "artifact": SUCCESSOR_PIN_FORMAT,
        "links": {
            SUCCESSOR_MANIFEST_NAME: manifest_sha256,
            SUCCESSOR_RECEIPT_NAME: expected_receipt_sha256,
            SUCCESSOR_REPORT_NAME: report_sha256,
        },
        "manifest_size_bytes": manifest_size,
        "markers": list(SUCCESSOR_MARKERS),
        "payload": common,
        "status": SUCCESSOR_CLOSURE_STATUS,
    }
    pin_sha256, pin_size = _publish_immutable(
        root / PurePosixPath(SUCCESSOR_PIN_NAME), pin_payload
    )
    if compute_phase1c_successor_verifier_state(repository_root) != current_verifier:
        raise Phase1CSuccessorError("successor verifier changed before COMPLETE")
    _verify_gate_witnesses(
        repository_root,
        targeted_tests,
        closure,
        forbidden_roots=forbidden_roots,
        forbidden_paths=forbidden_paths,
    )
    _load_receipt(root, expected_receipt_sha256)
    _require_finalization_tree(root)
    _reattest_finalization_artifact(
        root / SUCCESSOR_REPORT_NAME,
        expected_payload=report_payload,
        expected_sha256=report_sha256,
        expected_size_bytes=report_size,
        label="successor report",
    )
    _reattest_finalization_artifact(
        root / SUCCESSOR_MANIFEST_NAME,
        expected_payload=manifest_payload,
        expected_sha256=manifest_sha256,
        expected_size_bytes=manifest_size,
        label="successor manifest",
    )
    _reattest_finalization_artifact(
        root / PurePosixPath(SUCCESSOR_PIN_NAME),
        expected_payload=pin_payload,
        expected_sha256=pin_sha256,
        expected_size_bytes=pin_size,
        label="successor pin",
    )
    complete_payload = {
        "artifact": SUCCESSOR_COMPLETE_FORMAT,
        "links": {
            SUCCESSOR_MANIFEST_NAME: manifest_sha256,
            SUCCESSOR_PIN_NAME: pin_sha256,
            SUCCESSOR_RECEIPT_NAME: expected_receipt_sha256,
            SUCCESSOR_REPORT_NAME: report_sha256,
        },
        "markers": list(SUCCESSOR_MARKERS),
        "payload": {
            **common,
            "closure_sha256": closure.sha256,
            "successor_status": SUCCESSOR_REATTESTED_STATUS,
        },
        "status": PHASE1C_TARGET_NOT_MET_VERDICT,
    }
    complete_sha256, _ = _publish_immutable(
        root / SUCCESSOR_COMPLETE_NAME, complete_payload
    )
    _require_finalization_tree(root)
    if {item.name for item in root.iterdir()} != {
        SUCCESSOR_RECEIPT_NAME,
        SUCCESSOR_REPORT_NAME,
        SUCCESSOR_MANIFEST_NAME,
        SUCCESSOR_COMPLETE_NAME,
        "pin",
    }:
        raise Phase1CSuccessorError("terminal successor tree is incomplete")
    return Phase1CSuccessorPublication(
        root=root,
        receipt_sha256=expected_receipt_sha256,
        report_sha256=report_sha256,
        manifest_sha256=manifest_sha256,
        pin_sha256=pin_sha256,
        complete_sha256=complete_sha256,
        verdict=PHASE1C_TARGET_NOT_MET_VERDICT,
    )


__all__ = [
    "CAPACITY_MARKERS",
    "CURRENT_VERIFIER_CLOSURE_STATUS",
    "PHASE1C_TARGET_NOT_MET_VERDICT",
    "PRODUCER_DEPENDENCY_CLOSURE_UNCHANGED",
    "SUCCESSOR_ATTRIBUTION",
    "SUCCESSOR_CLOSURE_WITNESS_NAME",
    "SUCCESSOR_COMPLETE_FORMAT",
    "SUCCESSOR_COMPLETE_NAME",
    "SUCCESSOR_MARKERS",
    "SUCCESSOR_REATTESTED_STATUS",
    "SUCCESSOR_RECEIPT_FORMAT",
    "SUCCESSOR_RECEIPT_NAME",
    "SUCCESSOR_TARGETED_LOG_NAME",
    "SUCCESSOR_TARGETED_TEST_PATHS",
    "SUCCESSOR_TARGETED_WITNESS_NAME",
    "SUCCESSOR_V9_RELATIVE_PATH",
    "SUCCESSOR_V9_SHA256",
    "SUCCESSOR_V9_SIZE_BYTES",
    "Phase1CSuccessorClosureWitness",
    "Phase1CSuccessorCommandWitness",
    "Phase1CSuccessorConfig",
    "Phase1CSuccessorError",
    "Phase1CSuccessorExpectations",
    "Phase1CSuccessorPublication",
    "Phase1CSuccessorReattestation",
    "Phase1CSuccessorReceipt",
    "Phase1CSuccessorTestWitness",
    "Phase1CSuccessorV9ByteWitness",
    "SuccessorCodeIdentity",
    "SuccessorDependencyClosureWitness",
    "SuccessorVerifierState",
    "compute_phase1c_successor_verifier_state",
    "finalize_phase1c_successor_closure",
    "load_phase1c_successor_closure_witness",
    "load_phase1c_successor_test_witness",
    "parse_phase1c_successor_closure_witness",
    "parse_phase1c_successor_test_witness",
    "reattest_phase1c_successor",
    "verify_phase1c_successor",
]
