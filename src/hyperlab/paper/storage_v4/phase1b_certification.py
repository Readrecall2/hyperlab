"""Offline Golden V3 equivalence certification for Storage v4 Phase 1B.

The certifier deliberately has no access to the source Paper SQLite database.
Its authority is one already-complete Golden V3 export plus its external pin.
It imports that export in ``V3_COMPATIBILITY_IMPORT`` mode, closes and reopens
the Storage v4 repository, performs a structural checkpointed-startup check,
audits the complete immutable chain, and compares every rematerialized JSONL
record against all thirteen Golden streams before publishing ``COMPLETE``.

``COMPLETE`` is the sole immutable success marker and the final publication.
Progress remains explicitly non-terminal after all gates pass and is closed
before that marker is attempted. A timeout, interruption, integrity error, or
logical divergence therefore leaves a useful partial result directory without
a success marker and never mutates or reuses an existing result.
"""

from __future__ import annotations

import _thread
import hashlib
import json
import os
import stat
import sys
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import IO, Any, Protocol, Self, cast

from hyperlab.environment_authorization import (
    current_paper_release_code_sha256,
    current_paper_runtime_environment_sha256,
)
from hyperlab.paper.golden_v3 import (
    GOLDEN_STREAM_NAMES,
    GoldenVerification,
    iter_golden_stream,
    validate_new_auxiliary_path,
    verify_golden_v3,
)
from hyperlab.paper.storage_v4.anchor import Anchor, LocalAnchor
from hyperlab.paper.storage_v4.canonical import build_commit_logical, canonical_json_bytes
from hyperlab.paper.storage_v4.checkpoint import CheckpointState
from hyperlab.paper.storage_v4.contracts import (
    StorageMode,
    rematerialize_compatibility_record,
)
from hyperlab.paper.storage_v4.durability import durable_publish_immutable, fsync_directory
from hyperlab.paper.storage_v4.exact_decimal import ExactDecimalSum
from hyperlab.paper.storage_v4.golden_import import (
    AssembledGoldenCommit,
    GoldenCommitAssembler,
    GoldenImportExpectations,
)
from hyperlab.paper.storage_v4.manifest import OpaqueIdentity
from hyperlab.paper.storage_v4.overlay import OverlayThresholds
from hyperlab.paper.storage_v4.repository import RepositoryConfig, StorageRepository
from hyperlab.paper.storage_v4.segment import CodecProfile
from hyperlab.paper.storage_v4.types import (
    CommitFrame,
    Hash32,
    RunId,
    StoreId,
    StreamId,
)

PHASE1B_CERTIFICATION_FORMAT = "hyperlab-storage-v4-phase1b-certification-v2"
PHASE1B_COMPLETE_FORMAT = "hyperlab-storage-v4-phase1b-complete-v2"
PHASE1B_CERTIFIER_CONFIGURATION_FORMAT = (
    "hyperlab-storage-v4-phase1b-certifier-configuration-v1"
)
PHASE1B_SUCCESS = "STORAGE_V4_PHASE_1B_GOLDEN_EQUIVALENT"
PHASE1B_DIVERGED = "STORAGE_V4_PHASE_1B_GOLDEN_DIVERGED"
PHASE1B_INTEGRITY_BLOCKED = "STORAGE_V4_PHASE_1B_INTEGRITY_BLOCKED"

GOLDEN_V3_EXPECTED_COMMITS = 252_262
GOLDEN_V3_EXPECTED_ROWS = 1_011_362
GOLDEN_V3_EXPECTED_STREAMS = 13
MAX_SAFETY_SECONDS = 7_200.0
DEFAULT_HEARTBEAT_SECONDS = 45.0
_SHA256_LENGTH = 64
_CERTIFIER_CANDIDATE_ID = "phase08-phase05-multistrategy-paper-v1"
_ALLOWED_NON_BLOCKING_COVERAGE_GAPS = frozenset(
    {
        "PHASE05_PHASE08_DECISIONS_NOT_BOTH_OBSERVED",
        "REPLAY_NOT_PERFORMED",
    }
)


class Phase1BCertificationError(RuntimeError):
    """The offline certification boundary is unsafe, incomplete, or invalid."""


class Phase1BGoldenDivergenceError(Phase1BCertificationError):
    """A rematerialized Storage v4 logical record differs from Golden V3."""


class RepositoryLike(Protocol):
    """Narrow repository surface exercised by the certification pipeline."""

    @property
    def overlay_state(self) -> Any: ...

    @property
    def startup_report(self) -> Any: ...

    def append(self, frame: Any) -> bool: ...

    def seal(
        self,
        *,
        checkpoint_state: CheckpointState,
        cumulative_stream_counts: tuple[tuple[StreamId, int], ...],
        historical_commit_count: int,
    ) -> Any: ...

    def startup(self) -> Any: ...

    def full_audit(self) -> Any: ...

    def iter_historical_frames(self) -> Iterator[Any]: ...

    def close(self) -> None: ...


RepositoryCreate = Callable[[Path, Anchor, "Phase1BCertificationConfig"], RepositoryLike]
RepositoryOpen = Callable[[Path, Anchor, "Phase1BCertificationConfig"], RepositoryLike]
GoldenAssembler = Callable[
    [GoldenVerification], tuple[GoldenImportExpectations, Iterable[AssembledGoldenCommit]]
]
GoldenVerifier = Callable[[Path, Path], GoldenVerification]
GoldenStreamFactory = Callable[[GoldenVerification, str], Iterable[Mapping[str, object]]]


def _require_digest(value: str, *, label: str) -> str:
    if type(value) is not str or len(value) != _SHA256_LENGTH:
        raise ValueError(f"{label} must be a 64-character lowercase SHA-256")
    if value != value.lower() or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a 64-character lowercase SHA-256")
    return value


@dataclass(frozen=True, slots=True)
class _PinnedFileWitness:
    sha256: str
    size_bytes: int
    device: int
    inode: int
    mode: int
    mtime_ns: int

    def report_payload(self) -> dict[str, object]:
        return {
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "stable_during_each_verification": True,
            "stat": {
                "device": self.device,
                "inode": self.inode,
                "mode": self.mode,
                "mtime_ns": self.mtime_ns,
            },
            "unchanged_across_certification": True,
        }


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
    )


def _stable_pin_witness(path: Path) -> _PinnedFileWitness:
    """Hash one regular pin while proving its pathname and open handle stayed stable."""

    try:
        path_before = path.stat(follow_symlinks=False)
        if path.is_symlink() or not stat.S_ISREG(path_before.st_mode):
            raise Phase1BCertificationError("Golden pin must be a regular non-symlink file")
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            handle_before = os.fstat(handle.fileno())
            if _stat_identity(handle_before) != _stat_identity(path_before):
                raise Phase1BCertificationError("Golden pin changed while it was opened")
            while chunk := handle.read(64 * 1024):
                digest.update(chunk)
            handle_after = os.fstat(handle.fileno())
        path_after = path.stat(follow_symlinks=False)
    except Phase1BCertificationError:
        raise
    except OSError as error:
        raise Phase1BCertificationError("Golden pin could not be witnessed") from error
    identity = _stat_identity(path_before)
    if identity != _stat_identity(handle_after) or identity != _stat_identity(path_after):
        raise Phase1BCertificationError("Golden pin changed while it was hashed")
    return _PinnedFileWitness(
        sha256=digest.hexdigest(),
        size_bytes=path_before.st_size,
        device=path_before.st_dev,
        inode=path_before.st_ino,
        mode=path_before.st_mode,
        mtime_ns=path_before.st_mtime_ns,
    )


def _verify_with_stable_pin(
    verifier: GoldenVerifier,
    root: Path,
    pin: Path,
) -> tuple[GoldenVerification, _PinnedFileWitness]:
    before = _stable_pin_witness(pin)
    verification = verifier(root, pin)
    after = _stable_pin_witness(pin)
    if after != before:
        raise Phase1BCertificationError("Golden pin changed during verification")
    return verification, after


@dataclass(frozen=True, slots=True)
class Phase1BCertificationConfig:
    """Explicit, immutable inputs for one new offline certification result."""

    golden_root: Path
    golden_pin: Path
    output_root: Path
    expected_golden_root: str
    expected_source_sha256: str
    expected_run_id: str
    config_hash: str
    release_code_sha256: str
    runtime_environment_sha256: str
    certifier_code_sha256: str
    certifier_runtime_environment_sha256: str
    store_id: str = "golden-v3-storage-v4-phase1b"
    seal_rows: int = 50_000
    seal_bytes: int = 256 * 1024 * 1024
    codec_level: int = 6
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS
    safety_seconds: float = MAX_SAFETY_SECONDS

    def __post_init__(self) -> None:
        for label, path in (
            ("golden_root", self.golden_root),
            ("golden_pin", self.golden_pin),
            ("output_root", self.output_root),
        ):
            if not isinstance(path, Path):
                raise TypeError(f"{label} must be pathlib.Path")
            if not path.is_absolute():
                raise ValueError(f"{label} must be an explicit absolute path")
        _require_digest(self.expected_golden_root, label="expected_golden_root")
        _require_digest(self.expected_source_sha256, label="expected_source_sha256")
        _require_digest(self.expected_run_id, label="expected_run_id")
        _require_digest(self.config_hash, label="config_hash")
        _require_digest(self.release_code_sha256, label="release_code_sha256")
        _require_digest(
            self.runtime_environment_sha256,
            label="runtime_environment_sha256",
        )
        _require_digest(
            self.certifier_code_sha256,
            label="certifier_code_sha256",
        )
        _require_digest(
            self.certifier_runtime_environment_sha256,
            label="certifier_runtime_environment_sha256",
        )
        RunId(self.expected_run_id)
        StoreId(self.store_id)
        for label, value, minimum in (
            ("seal_rows", self.seal_rows, 1),
            ("seal_bytes", self.seal_bytes, 1),
            ("codec_level", self.codec_level, 1),
        ):
            if type(value) is not int or value < minimum:
                raise ValueError(f"{label} must be an integer >= {minimum}")
        if self.codec_level > 9:
            raise ValueError("codec_level must be between 1 and 9")
        if type(self.heartbeat_seconds) is not float or not (
            30.0 <= self.heartbeat_seconds <= 60.0
        ):
            raise ValueError("heartbeat_seconds must be a float between 30 and 60")
        if type(self.safety_seconds) is not float or not (
            0.0 < self.safety_seconds <= MAX_SAFETY_SECONDS
        ):
            raise ValueError("safety_seconds must be a positive float no greater than 7200")

    @property
    def store_root(self) -> Path:
        return self.output_root / "store"

    @property
    def anchor_path(self) -> Path:
        return self.output_root / "anchor.sqlite3"

    @property
    def progress_path(self) -> Path:
        return self.output_root / "progress.jsonl"

    @property
    def report_path(self) -> Path:
        return self.output_root / "report.json"

    @property
    def complete_path(self) -> Path:
        return self.output_root / "COMPLETE"


@dataclass(frozen=True, slots=True)
class _CertificationExpectations:
    commits: int
    rows: int
    streams: int
    market_gap_alert_rows: int


_PRODUCTION_EXPECTATIONS = _CertificationExpectations(
    commits=GOLDEN_V3_EXPECTED_COMMITS,
    rows=GOLDEN_V3_EXPECTED_ROWS,
    streams=GOLDEN_V3_EXPECTED_STREAMS,
    market_gap_alert_rows=1,
)


@dataclass(frozen=True, slots=True)
class Phase1BCertificationResult:
    output_root: Path
    report_path: Path
    complete_path: Path
    report_sha256: str
    final_prefix_root: str
    manifest_root: str
    status: str = PHASE1B_SUCCESS


@dataclass(frozen=True, slots=True)
class _Dependencies:
    verify: GoldenVerifier
    assemble: GoldenAssembler
    stream: GoldenStreamFactory
    create_repository: RepositoryCreate
    open_repository: RepositoryOpen
    anchor_create: Callable[[Path, StoreId], Anchor]
    anchor_open: Callable[[Path, StoreId], Anchor]
    current_certifier_code_sha256: Callable[[], str]
    current_certifier_runtime_environment_sha256: Callable[[], str]


def _default_verify(root: Path, pin: Path) -> GoldenVerification:
    return verify_golden_v3(root, pin_path=pin)


def _default_assemble(
    verification: GoldenVerification,
) -> tuple[GoldenImportExpectations, Iterable[AssembledGoldenCommit]]:
    expectations = GoldenImportExpectations.from_verification(verification)
    return expectations, GoldenCommitAssembler.from_verification(verification)


def _default_stream(
    verification: GoldenVerification,
    name: str,
) -> Iterable[Mapping[str, object]]:
    return cast(
        Iterable[Mapping[str, object]],
        iter_golden_stream(
            verification.export_root,
            name,
            verification=verification,
        ),
    )


def _default_current_certifier_code_sha256() -> str:
    return current_paper_release_code_sha256(candidate_id=_CERTIFIER_CANDIDATE_ID)


def _default_current_certifier_runtime_environment_sha256() -> str:
    return current_paper_runtime_environment_sha256(
        candidate_id=_CERTIFIER_CANDIDATE_ID
    )


def _certifier_configuration_payload(
    config: Phase1BCertificationConfig,
) -> dict[str, object]:
    """Return the exact canonical configuration bound into the V4 chain."""

    return {
        "certifier_code_sha256": config.certifier_code_sha256,
        "certifier_runtime_environment_sha256": (
            config.certifier_runtime_environment_sha256
        ),
        "codec": {"id": 1, "level": config.codec_level},
        "format": PHASE1B_CERTIFIER_CONFIGURATION_FORMAT,
        "golden_source": {
            "config_hash": config.config_hash,
            "golden_root": config.expected_golden_root,
            "release_code_sha256": config.release_code_sha256,
            "run_id": config.expected_run_id,
            "runtime_environment_sha256": config.runtime_environment_sha256,
            "source_sha256": config.expected_source_sha256,
        },
        "heartbeat_seconds": str(config.heartbeat_seconds),
        "mode": StorageMode.V3_COMPATIBILITY_IMPORT.value,
        "safety_seconds": str(config.safety_seconds),
        "seal_thresholds": {
            "bytes": config.seal_bytes,
            "rows": config.seal_rows,
        },
        "store_id": config.store_id,
    }


def _certifier_configuration_sha256(config: Phase1BCertificationConfig) -> str:
    return hashlib.sha256(
        canonical_json_bytes(_certifier_configuration_payload(config))
    ).hexdigest()


def _validate_certifier_provenance(
    config: Phase1BCertificationConfig,
    dependencies: _Dependencies,
) -> None:
    """Fail closed if the executing checkout/runtime differs from explicit inputs."""

    try:
        current_code = _require_digest(
            dependencies.current_certifier_code_sha256(),
            label="current certifier code SHA-256",
        )
        current_runtime = _require_digest(
            dependencies.current_certifier_runtime_environment_sha256(),
            label="current certifier runtime environment SHA-256",
        )
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise Phase1BCertificationError(
            "current certifier provenance could not be authenticated"
        ) from error
    if current_code != config.certifier_code_sha256:
        raise Phase1BCertificationError(
            "current certifier code SHA-256 differs from the explicit expectation"
        )
    if current_runtime != config.certifier_runtime_environment_sha256:
        raise Phase1BCertificationError(
            "current certifier runtime environment SHA-256 differs from the explicit expectation"
        )


def _repository_config(config: Phase1BCertificationConfig) -> RepositoryConfig:
    return RepositoryConfig(
        store_id=StoreId(config.store_id),
        run_id=RunId(config.expected_run_id),
        mode=StorageMode.V3_COMPATIBILITY_IMPORT,
        run_identity=OpaqueIdentity(Hash32.from_hex(config.expected_run_id)),
        config_identity=OpaqueIdentity(
            Hash32.from_hex(_certifier_configuration_sha256(config))
        ),
        code_identity=OpaqueIdentity(Hash32.from_hex(config.certifier_code_sha256)),
        runtime_identity=OpaqueIdentity(
            Hash32.from_hex(config.certifier_runtime_environment_sha256)
        ),
        start_prefix_root=Hash32.from_hex(config.expected_golden_root),
        thresholds=OverlayThresholds(
            seal_rows=config.seal_rows,
            seal_bytes=config.seal_bytes,
        ),
        codec_profile=CodecProfile.zlib(level=config.codec_level),
    )


def _default_create_repository(
    root: Path,
    anchor: Anchor,
    config: Phase1BCertificationConfig,
) -> RepositoryLike:
    return StorageRepository.create(root, anchor=anchor, config=_repository_config(config))


def _default_open_repository(
    root: Path,
    anchor: Anchor,
    config: Phase1BCertificationConfig,
) -> RepositoryLike:
    return StorageRepository.open_existing(
        root,
        anchor=anchor,
        config=_repository_config(config),
    )


DEFAULT_DEPENDENCIES = _Dependencies(
    verify=_default_verify,
    assemble=_default_assemble,
    stream=_default_stream,
    create_repository=_default_create_repository,
    open_repository=_default_open_repository,
    anchor_create=lambda path, store_id: LocalAnchor.create(path, store_id=store_id),
    anchor_open=lambda path, store_id: LocalAnchor.open_existing(
        path,
        store_id=store_id,
    ),
    current_certifier_code_sha256=_default_current_certifier_code_sha256,
    current_certifier_runtime_environment_sha256=(
        _default_current_certifier_runtime_environment_sha256
    ),
)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _json_line(value: Mapping[str, object]) -> bytes:
    try:
        return (
            json.dumps(
                dict(value),
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8", errors="strict")
            + b"\n"
        )
    except (RecursionError, TypeError, UnicodeEncodeError, ValueError) as error:
        raise Phase1BCertificationError(
            "Golden row cannot be canonically compared"
        ) from error


def _canonical_artifact(value: Mapping[str, object]) -> bytes:
    return canonical_json_bytes(cast(Any, dict(value))) + b"\n"


def _validated_output_root(config: Phase1BCertificationConfig) -> Path:
    try:
        return validate_new_auxiliary_path(
            config.output_root,
            forbidden_paths=(config.golden_root, config.golden_pin),
            label="Storage v4 Phase 1B output root",
            required_suffix=None,
        )
    except (OSError, ValueError) as error:
        raise Phase1BCertificationError(str(error)) from error


def _new_output_root(config: Phase1BCertificationConfig) -> None:
    root = _validated_output_root(config)
    root.mkdir(exist_ok=False)
    fsync_directory(root.parent)


def _remove_failed_complete(path: Path) -> None:
    """Remove a success marker if publication raced with a terminal failure."""

    if not path.exists() and not path.is_symlink():
        return
    if path.is_symlink() or not path.is_file():
        raise Phase1BCertificationError(
            "failed COMPLETE cleanup encountered a non-regular path"
        )
    path.unlink()
    fsync_directory(path.parent)


class _Progress:
    """Exclusive, durable JSONL progress plus a synchronized console view."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._handle: IO[bytes] | None = None
        self._latest: dict[str, object] = {}
        self._async_error: BaseException | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.close()

    def emit(self, payload: Mapping[str, object]) -> None:
        with self._lock:
            self._raise_async_error_locked()
            event = dict(payload)
            event.setdefault("timestamp_utc", _utc_now())
            if event.get("event") != "heartbeat":
                self._latest.update(event)
            line = _json_line(event)
            created = False
            if self._handle is None:
                self._handle = self._path.open("xb")
                created = True
            self._handle.write(line)
            self._handle.flush()
            os.fsync(self._handle.fileno())
            if created:
                fsync_directory(self._path.parent)
            print(line.decode("utf-8").rstrip("\n"), flush=True)

    def heartbeat(self, metrics: Mapping[str, object]) -> None:
        with self._lock:
            latest = dict(self._latest)
        self.emit(
            {
                **latest,
                **metrics,
                "event": "heartbeat",
                "status": "RUNNING",
                "timestamp_utc": _utc_now(),
            }
        )

    def record_async_error(self, error: BaseException) -> None:
        with self._lock:
            if self._async_error is None:
                self._async_error = error

    def raise_async_error(self) -> None:
        with self._lock:
            self._raise_async_error_locked()

    def _raise_async_error_locked(self) -> None:
        if self._async_error is not None:
            raise Phase1BCertificationError("durable heartbeat failed") from self._async_error

    def close(self) -> None:
        with self._lock:
            if self._handle is not None:
                self._handle.close()
                self._handle = None


class _Heartbeat:
    def __init__(
        self,
        progress: _Progress,
        state: _Metrics,
        *,
        seconds: float,
    ) -> None:
        self._progress = progress
        self._state = state
        self._seconds = seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="storage-v4-phase1b-heartbeat",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self, *, raise_on_error: bool = True) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=self._seconds + 1.0)
        if self._thread.is_alive():
            self._progress.record_async_error(
                TimeoutError("heartbeat thread did not stop cleanly")
            )
        if raise_on_error:
            self._progress.raise_async_error()

    def _run(self) -> None:
        try:
            while not self._stop.wait(self._seconds):
                snapshot = self._state.snapshot()
                self._progress.heartbeat(snapshot)
        except BaseException as error:
            self._progress.record_async_error(error)


class _Deadline:
    """Interrupt the main thread at the sole configured offline ceiling."""

    def __init__(self, seconds: float) -> None:
        self._seconds = seconds
        self._stop = threading.Event()
        self._expired = threading.Event()
        self._started_at: float | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="storage-v4-phase1b-deadline",
            daemon=True,
        )

    @property
    def expired(self) -> bool:
        return self._expired.is_set()

    def start(self) -> None:
        self._started_at = time.monotonic()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def require_active(self) -> None:
        started_at = self._started_at
        if (
            self.expired
            or started_at is None
            or time.monotonic() - started_at >= self._seconds
        ):
            self._expired.set()
            raise TimeoutError("Storage v4 Phase 1B safety deadline expired")

    def finish_after_complete(self) -> None:
        self.stop()
        self.require_active()

    def _run(self) -> None:
        if not self._stop.wait(self._seconds):
            self._expired.set()
            _thread.interrupt_main()


@dataclass(frozen=True, slots=True)
class _PeakRssMeasurement:
    status: str
    source: str
    value_bytes: int | None = None
    unavailable_reason: str | None = None

    def payload(self) -> dict[str, object]:
        result: dict[str, object] = {
            "peak_rss_source": self.source,
            "peak_rss_status": self.status,
        }
        if self.status == "AVAILABLE":
            if type(self.value_bytes) is not int or self.value_bytes <= 0:
                raise Phase1BCertificationError("available peak RSS measurement is invalid")
            result["peak_rss_bytes"] = self.value_bytes
        elif self.status == "UNAVAILABLE":
            if type(self.unavailable_reason) is not str or not self.unavailable_reason:
                raise Phase1BCertificationError("unavailable peak RSS status lacks a reason")
            result["peak_rss_unavailable_reason"] = self.unavailable_reason
        else:
            raise Phase1BCertificationError("peak RSS status is invalid")
        return result


def _windows_peak_rss_bytes() -> int:
    import ctypes
    from ctypes import wintypes

    class _Counters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    counters = _Counters()
    counters.cb = ctypes.sizeof(counters)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    get_current_process = kernel32.GetCurrentProcess
    get_current_process.restype = wintypes.HANDLE
    get_process_memory_info = psapi.GetProcessMemoryInfo
    get_process_memory_info.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_Counters),
        wintypes.DWORD,
    ]
    get_process_memory_info.restype = wintypes.BOOL
    if not get_process_memory_info(
        get_current_process(),
        ctypes.byref(counters),
        counters.cb,
    ):
        error_code = ctypes.get_last_error()
        raise OSError(error_code, "GetProcessMemoryInfo failed")
    value = int(counters.PeakWorkingSetSize)
    if value <= 0:
        raise OSError("GetProcessMemoryInfo returned a non-positive peak RSS")
    return value


def _peak_rss_measurement() -> _PeakRssMeasurement:
    if sys.platform == "win32":
        try:
            value = _windows_peak_rss_bytes()
        except (AttributeError, OSError, TypeError, ValueError):
            return _PeakRssMeasurement(
                status="UNAVAILABLE",
                source="WINDOWS_PROCESS_MEMORY_COUNTERS",
                unavailable_reason="WINDOWS_PROCESS_MEMORY_QUERY_FAILED",
            )
        return _PeakRssMeasurement(
            status="AVAILABLE",
            source="WINDOWS_PROCESS_MEMORY_COUNTERS",
            value_bytes=value,
        )
    try:
        import resource

        raw_value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        value = raw_value if sys.platform == "darwin" else raw_value * 1024
        if value <= 0:
            raise ValueError("resource peak RSS is non-positive")
    except (ImportError, OSError, TypeError, ValueError):
        return _PeakRssMeasurement(
            status="UNAVAILABLE",
            source="RESOURCE_GETRUSAGE",
            unavailable_reason="RESOURCE_RUSAGE_QUERY_FAILED",
        )
    return _PeakRssMeasurement(
        status="AVAILABLE",
        source="RESOURCE_GETRUSAGE",
        value_bytes=value,
    )


@dataclass(slots=True)
class _Metrics:
    expected_commits: int
    expected_rows: int
    started_monotonic: float = field(default_factory=time.monotonic)
    started_cpu: float = field(default_factory=time.process_time)
    commits: int = 0
    rows: int = 0
    segments: int = 0
    checkpoints: int = 0
    bytes_written: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def imported(self, *, commits: int, rows: int) -> None:
        with self._lock:
            self.commits = commits
            self.rows = rows

    def sealed(self, *, physical_bytes: int) -> None:
        with self._lock:
            self.segments += 1
            self.checkpoints += 1
            self.bytes_written += physical_bytes

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "bytes_written": self.bytes_written,
                "bytes_written_scope": (
                    "SEALED_SEGMENT_PLUS_CHECKPOINT_PLUS_MANIFEST_OBSERVED"
                ),
                "checkpoints": self.checkpoints,
                "commits_completed": self.commits,
                "commits_expected": self.expected_commits,
                "cpu_us": int((time.process_time() - self.started_cpu) * 1_000_000),
                "elapsed_us": int(
                    (time.monotonic() - self.started_monotonic) * 1_000_000
                ),
                **_peak_rss_measurement().payload(),
                "rows_completed": self.rows,
                "rows_expected": self.expected_rows,
                "segments": self.segments,
            }


def _manifest_streams(verification: GoldenVerification) -> dict[str, Mapping[str, object]]:
    raw = verification.manifest.get("streams")
    if type(raw) is not dict:
        raise Phase1BCertificationError("Golden manifest streams are missing")
    result: dict[str, Mapping[str, object]] = {}
    for name in GOLDEN_STREAM_NAMES:
        descriptor = raw.get(name)
        if type(descriptor) is not dict:
            raise Phase1BCertificationError(f"Golden stream {name!r} is missing")
        result[name] = cast(Mapping[str, object], descriptor)
    if set(raw) != set(GOLDEN_STREAM_NAMES):
        raise Phase1BCertificationError("Golden manifest differs from fixed 13 streams")
    return result


def _validate_expectations(
    config: Phase1BCertificationConfig,
    verification: GoldenVerification,
    expectations: GoldenImportExpectations,
    stream_factory: GoldenStreamFactory,
    required: _CertificationExpectations,
) -> None:
    if verification.root_hash != config.expected_golden_root:
        raise Phase1BCertificationError("Golden root differs from the explicit expectation")
    if verification.manifest.get("run_id") != config.expected_run_id:
        raise Phase1BCertificationError("Golden run ID differs from the explicit expectation")
    if expectations.run_id != RunId(config.expected_run_id):
        raise Phase1BCertificationError("Golden importer run ID differs")
    if expectations.export_root != Hash32.from_hex(config.expected_golden_root):
        raise Phase1BCertificationError("Golden importer root differs")
    source = verification.manifest.get("source")
    if type(source) is not dict or source.get("sha256") != config.expected_source_sha256:
        raise Phase1BCertificationError("Golden source SHA-256 differs")
    if expectations.commit_count != required.commits:
        raise Phase1BCertificationError("Golden commit count differs from 252262")
    if expectations.row_count != required.rows:
        raise Phase1BCertificationError("Golden row count differs from 1011362")
    if len(expectations.stream_row_counts) != required.streams:
        raise Phase1BCertificationError("Golden stream count differs from 13")
    run_rows = tuple(stream_factory(verification, "run"))
    if len(run_rows) != 1:
        raise Phase1BCertificationError("Golden run stream must contain exactly one row")
    run = run_rows[0]
    nested = run.get("config")
    if type(nested) is not dict:
        raise Phase1BCertificationError("Golden run has no authenticated config object")
    if run.get("config_hash") != config.config_hash:
        raise Phase1BCertificationError("Golden config hash differs")
    if nested.get("release_code_sha256") != config.release_code_sha256:
        raise Phase1BCertificationError("Golden release code identity differs")
    if nested.get("runtime_environment_sha256") != config.runtime_environment_sha256:
        raise Phase1BCertificationError("Golden runtime environment identity differs")


def _checkpoint_state(commit: AssembledGoldenCommit) -> CheckpointState:
    sections = commit.build_checkpoint_sections()
    return CheckpointState(
        adapter=sections.adapter_state,
        ledger=sections.ledger_state,
        projection=sections.projection_state,
        sessions=sections.sessions_state,
        incidents=sections.incidents_state,
        cursors=sections.cursors,
        stream_heads=sections.stream_heads,
    )


def _checkpoint_state_sha256(state: CheckpointState) -> str:
    if type(state) is not CheckpointState:
        raise TypeError("checkpoint state witness requires CheckpointState")
    digest = hashlib.sha256(b"HL4-PHASE1B-CHECKPOINT-STATE\x00\x01")
    for section in state.canonical_sections:
        digest.update(len(section).to_bytes(8, byteorder="big", signed=False))
        digest.update(section)
    return digest.hexdigest()


def _seal_physical_bytes(result: Any) -> int:
    segment = getattr(result, "segment", None)
    size = getattr(segment, "physical_size", None)
    if type(size) is not int or size < 0:
        raise Phase1BCertificationError("repository seal result has no physical segment size")
    checkpoint_path = getattr(result, "checkpoint_path", None)
    manifest_path = getattr(result, "manifest_path", None)
    extra = 0
    for path in (checkpoint_path, manifest_path):
        if isinstance(path, Path) and path.is_file():
            extra += path.stat().st_size
    return size + extra


def _seal_if_needed(
    repository: RepositoryLike,
    commit: AssembledGoldenCommit,
    metrics: _Metrics,
    *,
    force: bool,
) -> bool:
    state = repository.overlay_state
    tail_count = getattr(state, "tail_commit_count", None)
    required = getattr(state, "seal_required", None)
    if type(tail_count) is not int:
        raise Phase1BCertificationError("repository overlay state has no tail count")
    if tail_count == 0:
        return False
    if not force and required is not True:
        return False
    counts = tuple(
        sorted(
            (
                (StreamId(name), count)
                for name, count in commit.cumulative_stream_counts
                if count > 0
            ),
            key=lambda pair: pair[0].value.encode("utf-8"),
        )
    )
    checkpoint_state = _checkpoint_state(commit)
    result = repository.seal(
        checkpoint_state=checkpoint_state,
        cumulative_stream_counts=counts,
        historical_commit_count=int(commit.frame.commit_sequence),
    )
    published_checkpoint = getattr(result, "checkpoint", None)
    published_state = getattr(published_checkpoint, "state", None)
    if type(published_state) is not CheckpointState or published_state != checkpoint_state:
        raise Phase1BCertificationError(
            "published checkpoint state differs from the requested seal state"
        )
    metrics.sealed(physical_bytes=_seal_physical_bytes(result))
    return True


def _attribute_int(value: object, *names: str) -> int:
    for name in names:
        candidate = getattr(value, name, None)
        if type(candidate) is int:
            return candidate
        if candidate is not None and type(candidate).__module__ == (
            "hyperlab.paper.storage_v4.types"
        ):
            try:
                return int(candidate)
            except (TypeError, ValueError):
                pass
    raise Phase1BCertificationError(f"repository report lacks {'/'.join(names)}")


def _attribute_text(value: object, *names: str) -> str:
    for name in names:
        candidate = getattr(value, name, None)
        if isinstance(candidate, Hash32):
            return candidate.hex()
        if type(candidate) is str and candidate:
            return candidate
    raise Phase1BCertificationError(f"repository report lacks {'/'.join(names)}")


def _verify_structural_startup(
    startup: object,
    *,
    expected_commits: int,
    expected_rows: int,
    expected_checkpoint_state: CheckpointState,
) -> dict[str, object]:
    generation = _attribute_int(startup, "manifest_generation", "selected_generation")
    historical = _attribute_int(
        startup,
        "historical_commits_not_read",
        "base_commit_sequence",
    )
    historical_rows = _attribute_int(startup, "historical_rows_not_read")
    historical_segments = _attribute_int(startup, "historical_segments_not_read")
    segments_read = _attribute_int(startup, "segments_read")
    tail = _attribute_int(startup, "tail_entries_replayed", "tail_commits_replayed")
    checkpoint = getattr(startup, "checkpoint_root", None)
    checkpoint_state = getattr(startup, "checkpoint_state", None)
    checkpoint_used = getattr(startup, "checkpoint_used", None)
    integrity_raw = getattr(startup, "integrity_status", None)
    integrity = getattr(integrity_raw, "value", integrity_raw)
    if (
        generation < 1
        or not isinstance(checkpoint, Hash32)
        or checkpoint_used is not True
    ):
        raise Phase1BCertificationError("startup did not select an authenticated checkpoint")
    if segments_read != 0:
        raise Phase1BCertificationError("normal startup read historical segments")
    if historical != expected_commits:
        raise Phase1BCertificationError("startup historical-not-read counter differs")
    if historical_rows != expected_rows:
        raise Phase1BCertificationError("startup historical-row counter differs")
    if (
        type(checkpoint_state) is not CheckpointState
        or checkpoint_state != expected_checkpoint_state
    ):
        raise Phase1BCertificationError(
            "authenticated checkpoint state differs from independent Golden state"
        )
    if tail < 0:
        raise Phase1BCertificationError("startup tail counter is invalid")
    if integrity != "AUTHENTICATED_CHECKPOINT_PLUS_TAIL":
        raise Phase1BCertificationError("startup integrity result is not authenticated")
    return {
        "checkpoint_state_after_tail_exact": tail == 0,
        "checkpoint_state_exact": True,
        "checkpoint_used": True,
        "historical_commits_not_read": historical,
        "historical_payload_replay_complexity": "O(tail)",
        "historical_rows_not_read": historical_rows,
        "historical_segments_not_read": historical_segments,
        "integrity_result": integrity,
        "manifest_generation": generation,
        "metadata_authentication_complexity": (
            "O(current_manifest + checkpoint + tail)"
        ),
        "segments_read": segments_read,
        "tail_entries_replayed": tail,
    }


def _golden_required_text(
    row: Mapping[str, object],
    key: str,
    *,
    context: str,
) -> str:
    value = row.get(key)
    if type(value) is not str or not value:
        raise Phase1BGoldenDivergenceError(
            f"Golden {context} field {key!r} is not nonempty text"
        )
    return value


def _golden_required_digest(
    row: Mapping[str, object],
    key: str,
    *,
    context: str,
) -> str:
    value = _golden_required_text(row, key, context=context)
    try:
        Hash32.from_hex(value)
    except ValueError as error:
        raise Phase1BGoldenDivergenceError(
            f"Golden {context} field {key!r} is not lowercase SHA-256"
        ) from error
    return value


def _golden_required_int(
    row: Mapping[str, object],
    key: str,
    *,
    context: str,
    minimum: int = 0,
) -> int:
    value = row.get(key)
    if type(value) is not int or value < minimum:
        raise Phase1BGoldenDivergenceError(
            f"Golden {context} field {key!r} is not an integer >= {minimum}"
        )
    return value


def _golden_optional_int(
    row: Mapping[str, object],
    key: str,
    *,
    context: str,
    minimum: int = 0,
) -> int | None:
    value = row.get(key)
    if value is None:
        return None
    if type(value) is not int or value < minimum:
        raise Phase1BGoldenDivergenceError(
            f"Golden {context} field {key!r} is not null or an integer"
        )
    return value


def _owned_one(
    owned: Mapping[str, list[Mapping[str, object]]],
    stream: str,
    *,
    sequence: int,
) -> Mapping[str, object]:
    rows = owned[stream]
    if len(rows) != 1:
        raise Phase1BGoldenDivergenceError(
            f"V4 frame {sequence} must own exactly one {stream!r} row"
        )
    return rows[0]


def _verify_terminal_projection_binding(
    current: Mapping[str, object],
    history: Mapping[str, object],
) -> None:
    """Compare the shared V3 semantics, not the two distinct table envelopes."""

    integer_fields = ("revision", "event_sequence")
    digest_fields = ("event_head_hash", "projection_hash")
    for field_name in integer_fields:
        if _golden_required_int(
            current,
            field_name,
            context="current projection",
        ) != _golden_required_int(history, field_name, context="projection history"):
            raise Phase1BGoldenDivergenceError(
                f"V4 terminal projection {field_name} differs from projection history"
            )
    for field_name in digest_fields:
        if _golden_required_digest(
            current,
            field_name,
            context="current projection",
        ) != _golden_required_digest(history, field_name, context="projection history"):
            raise Phase1BGoldenDivergenceError(
                f"V4 terminal projection {field_name} differs from projection history"
            )
    current_payload = current.get("payload")
    history_payload = history.get("payload")
    if type(current_payload) is not dict or type(history_payload) is not dict:
        raise Phase1BGoldenDivergenceError(
            "Golden terminal projection payload is not an object"
        )
    if _json_line(current_payload) != _json_line(history_payload):
        raise Phase1BGoldenDivergenceError(
            "V4 terminal projection payload differs from projection history"
        )


def _verify_frame_ownership(
    frame: CommitFrame,
    owned: Mapping[str, list[Mapping[str, object]]],
    *,
    expected_commit_count: int,
    expected_stream_counts: Mapping[str, int],
) -> str:
    """Verify V3 identity and commit ownership independently of the importer."""

    sequence = int(frame.commit_sequence)
    commit = _owned_one(owned, "commits", sequence=sequence)
    inbox = _owned_one(owned, "inbox", sequence=sequence)
    if _golden_required_int(
        commit,
        "commit_sequence",
        context="commit",
        minimum=1,
    ) != sequence:
        raise Phase1BGoldenDivergenceError(
            f"V4 frame {sequence} owns another Golden commit sequence"
        )
    commit_hash = _golden_required_digest(commit, "commit_hash", context="commit")
    if frame.legacy_v3_identity != Hash32.from_hex(commit_hash):
        raise Phase1BGoldenDivergenceError(
            f"V4 frame {sequence} legacy V3 identity differs"
        )
    if _golden_required_int(
        inbox,
        "commit_sequence",
        context="inbox",
        minimum=1,
    ) != sequence:
        raise Phase1BGoldenDivergenceError(
            f"V4 frame {sequence} owns another Golden inbox sequence"
        )
    if _golden_required_digest(inbox, "commit_hash", context="inbox") != commit_hash:
        raise Phase1BGoldenDivergenceError(
            f"V4 frame {sequence} inbox/commit hashes differ"
        )
    input_id = _golden_required_text(commit, "input_id", context="commit")
    if _golden_required_text(inbox, "input_id", context="inbox") != input_id:
        raise Phase1BGoldenDivergenceError(
            f"V4 frame {sequence} inbox/commit input identities differ"
        )

    for stream in (
        "ledger_transactions",
        "ledger_entries",
        "alerts",
        "runtime_sessions",
        "incidents",
    ):
        for row in owned[stream]:
            if _golden_required_int(
                row,
                "commit_sequence",
                context=stream,
                minimum=1,
            ) != sequence:
                raise Phase1BGoldenDivergenceError(
                    f"V4 frame {sequence} misowns a {stream!r} row"
                )

    raw_event_hashes = commit.get("event_hashes")
    if type(raw_event_hashes) is not list or any(
        type(value) is not str for value in raw_event_hashes
    ):
        raise Phase1BGoldenDivergenceError(
            f"Golden commit {sequence} event hash witness is malformed"
        )
    event_hashes = cast(list[str], raw_event_hashes)
    first_event = _golden_optional_int(
        commit,
        "first_event_sequence",
        context="commit",
        minimum=1,
    )
    last_event = _golden_optional_int(
        commit,
        "last_event_sequence",
        context="commit",
        minimum=1,
    )
    event_rows = owned["events"]
    if first_event is None or last_event is None:
        if first_event is not None or last_event is not None or event_hashes or event_rows:
            raise Phase1BGoldenDivergenceError(
                f"V4 frame {sequence} empty-event ownership differs"
            )
    else:
        if (
            last_event < first_event
            or len(event_hashes) != last_event - first_event + 1
            or len(event_rows) != len(event_hashes)
        ):
            raise Phase1BGoldenDivergenceError(
                f"V4 frame {sequence} event range ownership differs"
            )
        for event_sequence, expected_hash, event in zip(
            range(first_event, last_event + 1),
            event_hashes,
            event_rows,
            strict=True,
        ):
            if _golden_required_int(
                event,
                "sequence",
                context="event",
                minimum=1,
            ) != event_sequence:
                raise Phase1BGoldenDivergenceError(
                    f"V4 frame {sequence} event sequence ownership differs"
                )
            if _golden_required_digest(event, "event_hash", context="event") != expected_hash:
                raise Phase1BGoldenDivergenceError(
                    f"V4 frame {sequence} event hash ownership differs"
                )
            if _golden_required_text(event, "input_id", context="event") != input_id:
                raise Phase1BGoldenDivergenceError(
                    f"V4 frame {sequence} event input ownership differs"
                )

    projections = owned["projection_history"]
    revisions = tuple(
        _golden_required_int(row, "revision", context="projection")
        for row in projections
    )
    expected_revisions = (0, 1) if sequence == 1 else (sequence,)
    if revisions != expected_revisions:
        raise Phase1BGoldenDivergenceError(
            f"V4 frame {sequence} projection revision ownership differs"
        )
    if _golden_required_int(
        commit,
        "projection_revision",
        context="commit",
    ) != sequence:
        raise Phase1BGoldenDivergenceError(
            f"Golden commit {sequence} projection revision differs"
        )
    if _golden_required_digest(
        projections[-1],
        "projection_hash",
        context="projection",
    ) != _golden_required_digest(commit, "projection_hash", context="commit"):
        raise Phase1BGoldenDivergenceError(
            f"V4 frame {sequence} projection hash ownership differs"
        )

    if sequence == 1:
        if len(owned["schema"]) != expected_stream_counts["schema"]:
            raise Phase1BGoldenDivergenceError(
                "V4 first frame does not own the complete schema snapshot"
            )
    elif owned["schema"]:
        raise Phase1BGoldenDivergenceError("V4 schema rows appear after the first frame")

    final = sequence == expected_commit_count
    for stream in ("run", "projection_current", "heads"):
        expected_count = expected_stream_counts[stream] if final else 0
        if len(owned[stream]) != expected_count:
            raise Phase1BGoldenDivergenceError(
                f"V4 terminal metadata ownership differs for {stream!r}"
            )
    if final:
        current = _owned_one(owned, "projection_current", sequence=sequence)
        _verify_terminal_projection_binding(current, projections[-1])
        for stream in ("run", "heads"):
            terminal = _owned_one(owned, stream, sequence=sequence)
            if _golden_required_int(
                terminal,
                "commit_count",
                context=stream,
                minimum=1,
            ) != expected_commit_count:
                raise Phase1BGoldenDivergenceError(
                    f"Golden terminal {stream!r} commit count differs"
                )
    return commit_hash


def _build_expected_checkpoint_state(
    *,
    export_root: str,
    sequence: int,
    processed_rows: int,
    last_commit_hash: str,
    counts: Mapping[str, int],
    digests: Mapping[str, Any],
    last_line_hash: Mapping[str, str | None],
    ledger_balances: Mapping[str, Mapping[str, ExactDecimalSum]],
    last_entry_hash: str | None,
    last_transaction_hash: str | None,
    latest_projection: Mapping[str, object],
    sessions: list[str],
    incidents: list[str],
) -> CheckpointState:
    return CheckpointState(
        adapter=cast(
            Any,
            {
                "contract": "hyperlab.storage_v4.golden_import.v1",
                "export_root": export_root,
                "last_v3_commit_hash": last_commit_hash,
                "processed_commits": sequence,
                "processed_rows": processed_rows,
            },
        ),
        ledger=cast(
            Any,
            {
                "balances": {
                    account: {
                        unit: amount.text
                        for unit, amount in sorted(by_unit.items())
                    }
                    for account, by_unit in sorted(ledger_balances.items())
                },
                "entry_count": counts["ledger_entries"],
                "last_entry_hash": last_entry_hash,
                "last_transaction_hash": last_transaction_hash,
                "transaction_count": counts["ledger_transactions"],
            },
        ),
        projection=cast(
            Any,
            {
                "canonical_json": _json_line(latest_projection)[:-1].decode("utf-8"),
                "projection_hash": _golden_required_digest(
                    latest_projection,
                    "projection_hash",
                    context="checkpoint projection",
                ),
                "revision": _golden_required_int(
                    latest_projection,
                    "revision",
                    context="checkpoint projection",
                ),
            },
        ),
        sessions=cast(Any, {"records": list(sessions)}),
        incidents=cast(Any, {"records": list(incidents)}),
        cursors=cast(Any, {"stream_row_counts": dict(counts)}),
        stream_heads=cast(
            Any,
            {
                "streams": {
                    name: {
                        "last_line_sha256": last_line_hash[name],
                        "logical_sha256": digests[name].hexdigest(),
                        "row_count": counts[name],
                    }
                    for name in GOLDEN_STREAM_NAMES
                }
            },
        ),
    )


@dataclass(frozen=True, slots=True)
class _GoldenComparisonResult:
    report: dict[str, object]
    checkpoint_state: CheckpointState


def _compare_all_streams(
    repository: RepositoryLike,
    verification: GoldenVerification,
    stream_factory: GoldenStreamFactory,
    checkpoint_state_witnesses: Mapping[int, str],
    *,
    expected_rows: int,
    expected_market_gap_rows: int,
) -> _GoldenComparisonResult:
    if not checkpoint_state_witnesses or any(
        type(sequence) is not int
        or sequence < 1
        or type(witness) is not str
        or len(witness) != 64
        for sequence, witness in checkpoint_state_witnesses.items()
    ):
        raise Phase1BCertificationError(
            "persisted checkpoint state witness set is invalid"
        )
    expected_iterators = {
        name: iter(stream_factory(verification, name)) for name in GOLDEN_STREAM_NAMES
    }
    stream_descriptors = _manifest_streams(verification)
    expected_stream_counts: dict[str, int] = {}
    for name, descriptor in stream_descriptors.items():
        row_count = descriptor.get("row_count")
        if type(row_count) is not int or row_count < 0:
            raise Phase1BCertificationError(
                f"Golden stream {name!r} row count is malformed"
            )
        expected_stream_counts[name] = row_count
    expected_commit_count = expected_stream_counts["commits"]
    expected_run_id = verification.manifest.get("run_id")
    if type(expected_run_id) is not str:
        raise Phase1BCertificationError("Golden manifest run ID is malformed")
    counts = {name: 0 for name in GOLDEN_STREAM_NAMES}
    digests = {name: hashlib.sha256() for name in GOLDEN_STREAM_NAMES}
    last_line_hash: dict[str, str | None] = {
        name: None for name in GOLDEN_STREAM_NAMES
    }
    ledger_balances: dict[str, dict[str, ExactDecimalSum]] = {}
    last_transaction_hash: str | None = None
    last_entry_hash: str | None = None
    sessions: list[str] = []
    incidents: list[str] = []
    latest_projection: Mapping[str, object] | None = None
    last_commit_hash: str | None = None
    market_gap_rows = 0
    total = 0
    prior_sequence = 0
    final_prefix_root: str | None = None
    verified_checkpoint_boundaries: set[int] = set()
    expected_previous_prefix = Hash32.from_hex(verification.root_hash)
    stream_positions = {
        name: position for position, name in enumerate(GOLDEN_STREAM_NAMES)
    }
    for frame in repository.iter_historical_frames():
        if type(frame) is not CommitFrame:
            raise Phase1BGoldenDivergenceError(
                "V4 historical reader emitted a non-CommitFrame value"
            )
        sequence = int(frame.commit_sequence)
        if sequence != prior_sequence + 1:
            raise Phase1BGoldenDivergenceError(
                "V4 historical commit sequence has a gap, overlap, or reorder"
            )
        if frame.run_id.value != expected_run_id:
            raise Phase1BGoldenDivergenceError(
                f"V4 frame {sequence} belongs to another run"
            )
        if frame.previous_prefix_root != expected_previous_prefix:
            raise Phase1BGoldenDivergenceError(
                f"V4 frame {sequence} previous prefix root differs"
            )
        prior_sequence = sequence
        owned: dict[str, list[Mapping[str, object]]] = {
            name: [] for name in GOLDEN_STREAM_NAMES
        }
        local_counts = {name: 0 for name in GOLDEN_STREAM_NAMES}
        prior_stream_position = -1
        for row in frame.rows:
            name = row.stream_id.value
            if name not in expected_iterators:
                raise Phase1BGoldenDivergenceError(
                    f"V4 emitted unexpected stream {name!r}"
                )
            stream_position = stream_positions[name]
            if stream_position < prior_stream_position:
                raise Phase1BGoldenDivergenceError(
                    f"V4 frame {sequence} stream blocks are reordered"
                )
            prior_stream_position = stream_position
            expected_ordinal = local_counts[name]
            if int(row.ordinal) != expected_ordinal:
                raise Phase1BGoldenDivergenceError(
                    f"V4 frame {sequence} {name!r} ordinal differs"
                )
            local_counts[name] += 1
            try:
                expected = next(expected_iterators[name])
            except StopIteration as error:
                raise Phase1BGoldenDivergenceError(
                    f"V4 emitted an extra {name!r} row"
                ) from error
            expected_line = _json_line(expected)
            actual_line = rematerialize_compatibility_record(row)
            ordinal = counts[name]
            if actual_line != expected_line:
                raise Phase1BGoldenDivergenceError(
                    f"V4/Golden bytes differ at {name}[{ordinal}]"
                )
            owned[name].append(expected)
            counts[name] += 1
            total += 1
            digests[name].update(actual_line)
            last_line_hash[name] = hashlib.sha256(actual_line).hexdigest()
            if name == "alerts" and expected.get("code") == "MARKET_GAP":
                market_gap_rows += 1
            elif name == "ledger_transactions":
                last_transaction_hash = _golden_required_digest(
                    expected,
                    "transaction_hash",
                    context="ledger transaction",
                )
            elif name == "ledger_entries":
                last_entry_hash = _golden_required_digest(
                    expected,
                    "entry_hash",
                    context="ledger entry",
                )
                account = _golden_required_text(
                    expected,
                    "account",
                    context="ledger entry",
                )
                unit = _golden_required_text(
                    expected,
                    "unit",
                    context="ledger entry",
                )
                amount_text = _golden_required_text(
                    expected,
                    "amount_text",
                    context="ledger entry",
                )
                try:
                    amount = ExactDecimalSum.from_text(amount_text)
                except ValueError as error:
                    raise Phase1BGoldenDivergenceError(
                        "Golden ledger amount_text is not exact Decimal"
                    ) from error
                by_unit = ledger_balances.setdefault(account, {})
                by_unit[unit] = by_unit.get(unit, ExactDecimalSum()).add(amount)
            elif name == "projection_history":
                latest_projection = expected
            elif name == "runtime_sessions":
                sessions.append(expected_line[:-1].decode("utf-8"))
            elif name == "incidents":
                incidents.append(expected_line[:-1].decode("utf-8"))
        last_commit_hash = _verify_frame_ownership(
            frame,
            owned,
            expected_commit_count=expected_commit_count,
            expected_stream_counts=expected_stream_counts,
        )
        checkpoint_witness = checkpoint_state_witnesses.get(sequence)
        if checkpoint_witness is not None:
            if latest_projection is None:
                raise Phase1BGoldenDivergenceError(
                    f"checkpoint boundary {sequence} lacks projection state"
                )
            expected_checkpoint = _build_expected_checkpoint_state(
                export_root=verification.root_hash,
                sequence=sequence,
                processed_rows=total,
                last_commit_hash=last_commit_hash,
                counts=counts,
                digests=digests,
                last_line_hash=last_line_hash,
                ledger_balances=ledger_balances,
                last_entry_hash=last_entry_hash,
                last_transaction_hash=last_transaction_hash,
                latest_projection=latest_projection,
                sessions=sessions,
                incidents=incidents,
            )
            if _checkpoint_state_sha256(expected_checkpoint) != checkpoint_witness:
                raise Phase1BGoldenDivergenceError(
                    f"checkpoint state differs at seal boundary {sequence}"
                )
            verified_checkpoint_boundaries.add(sequence)
        logical = build_commit_logical(frame)
        expected_previous_prefix = logical.prefix_root
        final_prefix_root = logical.prefix_root.hex()
    if prior_sequence == 0 or final_prefix_root is None:
        raise Phase1BGoldenDivergenceError("V4 historical reader produced no commits")
    if prior_sequence != expected_commit_count:
        raise Phase1BGoldenDivergenceError("V4 historical commit count differs")
    for name, iterator in expected_iterators.items():
        try:
            next(iterator)
        except StopIteration:
            pass
        else:
            raise Phase1BGoldenDivergenceError(f"V4 is missing {name!r} rows")
        descriptor = stream_descriptors[name]
        expected_count = descriptor.get("row_count")
        expected_digest = descriptor.get("logical_sha256")
        if counts[name] != expected_count:
            raise Phase1BGoldenDivergenceError(f"V4 {name!r} row count differs")
        if digests[name].hexdigest() != expected_digest:
            raise Phase1BGoldenDivergenceError(f"V4 {name!r} logical SHA-256 differs")
    if total != expected_rows:
        raise Phase1BGoldenDivergenceError("V4 total row count differs")
    if market_gap_rows != expected_market_gap_rows:
        raise Phase1BGoldenDivergenceError("V4 MARKET_GAP coverage differs")
    if verified_checkpoint_boundaries != set(checkpoint_state_witnesses):
        raise Phase1BGoldenDivergenceError(
            "persisted checkpoint boundary set differs from historical commits"
        )
    if latest_projection is None or last_commit_hash is None:
        raise Phase1BGoldenDivergenceError(
            "V4 historical comparison lacks terminal checkpoint inputs"
        )
    checkpoint_state = _build_expected_checkpoint_state(
        export_root=verification.root_hash,
        sequence=prior_sequence,
        processed_rows=total,
        last_commit_hash=last_commit_hash,
        counts=counts,
        digests=digests,
        last_line_hash=last_line_hash,
        ledger_balances=ledger_balances,
        last_entry_hash=last_entry_hash,
        last_transaction_hash=last_transaction_hash,
        latest_projection=latest_projection,
        sessions=sessions,
        incidents=incidents,
    )
    return _GoldenComparisonResult(
        report={
            "commits": prior_sequence,
            "checkpoint_states_verified": len(verified_checkpoint_boundaries),
            "final_prefix_root": final_prefix_root,
            "frame_ownership_exact": True,
            "legacy_v3_identity_exact": True,
            "market_gap_rows": market_gap_rows,
            "row_order_exact": True,
            "rows": total,
            "streams": {
                name: {
                    "logical_sha256": digests[name].hexdigest(),
                    "row_count": counts[name],
                }
                for name in GOLDEN_STREAM_NAMES
            },
        },
        checkpoint_state=checkpoint_state,
    )


def _tree_bytes(path: Path) -> int:
    total = 0
    for candidate in path.rglob("*"):
        if candidate.is_file() and not candidate.is_symlink():
            total += candidate.stat().st_size
    return total


def _golden_sizes(verification: GoldenVerification) -> dict[str, int]:
    descriptors = _manifest_streams(verification)
    physical = 0
    logical = 0
    for descriptor in descriptors.values():
        logical_size = descriptor.get("logical_size")
        shards = descriptor.get("shards")
        if type(logical_size) is not int or type(shards) is not list:
            raise Phase1BCertificationError("Golden stream size metadata is invalid")
        logical += logical_size
        for shard in shards:
            if type(shard) is not dict or type(shard.get("physical_size")) is not int:
                raise Phase1BCertificationError("Golden shard size metadata is invalid")
            physical += cast(int, shard["physical_size"])
    return {"logical_jsonl_bytes": logical, "physical_shard_bytes": physical}


def _coverage_metadata(
    verification: GoldenVerification,
    *,
    market_gap_alert_count: int,
) -> dict[str, object]:
    census = verification.manifest.get("census")
    if type(census) is not dict:
        raise Phase1BCertificationError("Golden census metadata is missing")
    raw_decisions = census.get("strategy_decision_counts")
    raw_gaps = census.get("coverage_gaps")
    raw_alert_counts = census.get("alert_code_counts")
    if (
        type(raw_decisions) is not dict
        or any(type(key) is not str or type(value) is not int for key, value in raw_decisions.items())
        or type(raw_gaps) is not list
        or any(type(value) is not str for value in raw_gaps)
        or type(raw_alert_counts) is not dict
        or any(
            type(key) is not str or type(value) is not int
            for key, value in raw_alert_counts.items()
        )
    ):
        raise Phase1BCertificationError("Golden coverage metadata is malformed")
    decisions = cast(dict[str, int], raw_decisions)
    alert_counts = cast(dict[str, int], raw_alert_counts)
    gaps = cast(list[str], raw_gaps)
    if len(gaps) != len(set(gaps)):
        raise Phase1BCertificationError("Golden coverage gap list contains duplicates")
    unexpected_gaps = set(gaps) - _ALLOWED_NON_BLOCKING_COVERAGE_GAPS
    if unexpected_gaps:
        rendered = ", ".join(sorted(unexpected_gaps))
        raise Phase1BCertificationError(
            f"Golden contains non-authorized coverage gaps: {rendered}"
        )
    census_market_gap = alert_counts.get("MARKET_GAP", 0)
    if census_market_gap != market_gap_alert_count:
        raise Phase1BCertificationError(
            "Golden census MARKET_GAP count differs from exhaustive comparison"
        )
    return {
        "coverage_gaps": list(gaps),
        "economic_evidence": False,
        "market_gap_alert_count": census_market_gap,
        "non_blocking": True,
        "phase05_decision_coverage": decisions.get("phase05_cash_and_carry", 0) > 0,
        "phase08_decision_coverage": decisions.get("phase08_robust_pairs", 0) > 0,
        "strategy_decision_counts": dict(sorted(decisions.items())),
    }


def _audit_checkpoint_state_witnesses(audit: object) -> dict[int, str]:
    raw = getattr(audit, "checkpoint_state_witnesses", None)
    if type(raw) is not tuple or not raw:
        raise Phase1BCertificationError(
            "full audit lacks persisted checkpoint state witnesses"
        )
    result: dict[int, str] = {}
    previous_sequence = 0
    for witness in raw:
        sequence = _attribute_int(witness, "covered_commit_sequence")
        digest = _attribute_text(witness, "state_sha256")
        _require_digest(digest, label="checkpoint_state_witness.state_sha256")
        if sequence <= previous_sequence or sequence in result:
            raise Phase1BCertificationError(
                "persisted checkpoint state witness boundaries are not strictly ordered"
            )
        result[sequence] = digest
        previous_sequence = sequence
    return result


def _audit_payload(
    audit: object,
    startup: object,
    *,
    checkpoint_state_witnesses: Mapping[int, str],
) -> dict[str, object]:
    integrity_raw = getattr(audit, "integrity_status", None)
    integrity = getattr(integrity_raw, "value", integrity_raw)
    if integrity != "FULL_HISTORY_AUTHENTICATED":
        raise Phase1BCertificationError("full audit integrity status is not authenticated")
    final_prefix = _attribute_text(startup, "base_prefix_root")
    if _attribute_int(startup, "tail_entries_replayed") != 0:
        raise Phase1BCertificationError("final certification startup retained an unsealed tail")
    manifests = _attribute_int(audit, "manifests_read", "manifests")
    checkpoints = _attribute_int(audit, "checkpoints_read", "checkpoints")
    segments = _attribute_int(audit, "segments_read", "segments")
    generation = _attribute_int(audit, "manifest_generation")
    commits = _attribute_int(audit, "commits_read", "commits")
    witness_count = len(checkpoint_state_witnesses)
    if not (
        generation
        == manifests
        == checkpoints
        == segments
        == witness_count
    ):
        raise Phase1BCertificationError(
            "full audit manifest/checkpoint/segment/witness counts differ"
        )
    if max(checkpoint_state_witnesses) != commits:
        raise Phase1BCertificationError(
            "final persisted checkpoint boundary differs from audited commits"
        )
    return {
        "checkpoint_state_witnesses": [
            {
                "covered_commit_sequence": sequence,
                "state_sha256": digest,
            }
            for sequence, digest in checkpoint_state_witnesses.items()
        ],
        "checkpoints": checkpoints,
        "commits": commits,
        "final_prefix_root": final_prefix,
        "integrity_status": integrity,
        "manifests": manifests,
        "manifest_root": _attribute_text(audit, "manifest_root", "selected_manifest_root"),
        "physical_bytes": _attribute_int(
            audit,
            "physical_segment_bytes",
            "physical_bytes",
        ),
        "rows": _attribute_int(audit, "rows_read", "rows"),
        "segments": segments,
    }


def _certify_storage_v4_phase1b(
    config: Phase1BCertificationConfig,
    *,
    dependencies: _Dependencies,
    deadline: _Deadline,
    required: _CertificationExpectations,
) -> Phase1BCertificationResult:
    _validated_output_root(config)
    _validate_certifier_provenance(config, dependencies)
    initial, initial_pin_witness = _verify_with_stable_pin(
        dependencies.verify,
        config.golden_root,
        config.golden_pin,
    )
    expectations, assembled = dependencies.assemble(initial)
    _validate_expectations(config, initial, expectations, dependencies.stream, required)
    _new_output_root(config)

    metrics = _Metrics(required.commits, required.rows)
    repository: RepositoryLike | None = None
    anchor: Anchor | None = None
    last_commit: AssembledGoldenCommit | None = None
    with _Progress(config.progress_path) as progress:
        heartbeat = _Heartbeat(
            progress,
            metrics,
            seconds=config.heartbeat_seconds,
        )
        try:
            heartbeat.start()
            progress.emit(
                {
                    "event": "phase_started",
                    "golden_root": initial.root_hash,
                    "mode": StorageMode.V3_COMPATIBILITY_IMPORT.value,
                    "phase": "streaming_import",
                    "status": "RUNNING",
                }
            )
            anchor = dependencies.anchor_create(
                config.anchor_path,
                StoreId(config.store_id),
            )
            repository = dependencies.create_repository(
                config.store_root,
                anchor,
                config,
            )
            for commit in assembled:
                progress.raise_async_error()
                if not repository.append(commit.frame):
                    raise Phase1BCertificationError(
                        "fresh Golden import unexpectedly reported an idempotent duplicate"
                    )
                last_commit = commit
                metrics.imported(
                    commits=int(commit.frame.commit_sequence),
                    rows=commit.cumulative_rows,
                )
                _seal_if_needed(
                    repository,
                    commit,
                    metrics,
                    force=int(commit.frame.commit_sequence) == required.commits,
                )
            if last_commit is None:
                raise Phase1BCertificationError("Golden assembler produced no commits")
            snapshot = metrics.snapshot()
            if snapshot["commits_completed"] != required.commits:
                raise Phase1BCertificationError("imported commit count differs")
            if snapshot["rows_completed"] != required.rows:
                raise Phase1BCertificationError("imported row count differs")
            if cast(int, snapshot["segments"]) < 2:
                raise Phase1BCertificationError(
                    "Golden import did not exercise multiple seal/checkpoint cycles"
                )
            repository.close()
            repository = None
            close_anchor = getattr(anchor, "close", None)
            if callable(close_anchor):
                close_anchor()
            anchor = None

            progress.emit(
                {
                    **snapshot,
                    "event": "phase_started",
                    "phase": "checkpointed_restart_and_full_audit",
                    "status": "RUNNING",
                }
            )
            anchor = dependencies.anchor_open(
                config.anchor_path,
                StoreId(config.store_id),
            )
            repository = dependencies.open_repository(
                config.store_root,
                anchor,
                config,
            )
            startup = repository.startup()
            audit_report = repository.full_audit()
            checkpoint_state_witnesses = _audit_checkpoint_state_witnesses(
                audit_report
            )
            audit = _audit_payload(
                audit_report,
                startup,
                checkpoint_state_witnesses=checkpoint_state_witnesses,
            )
            if audit["commits"] != required.commits:
                raise Phase1BCertificationError("full audit commit count differs")
            if audit["rows"] != required.rows:
                raise Phase1BCertificationError("full audit row count differs")
            if not (
                len(checkpoint_state_witnesses)
                == cast(int, snapshot["segments"])
                == cast(int, snapshot["checkpoints"])
            ):
                raise Phase1BCertificationError(
                    "persisted checkpoint witness count differs from seal metrics"
                )
            comparison_result = _compare_all_streams(
                repository,
                initial,
                dependencies.stream,
                checkpoint_state_witnesses,
                expected_rows=required.rows,
                expected_market_gap_rows=required.market_gap_alert_rows,
            )
            comparison = comparison_result.report
            startup_payload = _verify_structural_startup(
                startup,
                expected_commits=required.commits,
                expected_rows=required.rows,
                expected_checkpoint_state=comparison_result.checkpoint_state,
            )
            if comparison["commits"] != required.commits:
                raise Phase1BGoldenDivergenceError("comparison commit count differs")
            if comparison["final_prefix_root"] != audit["final_prefix_root"]:
                raise Phase1BGoldenDivergenceError(
                    "logical comparison and full audit final roots differ"
                )

            final, final_pin_witness = _verify_with_stable_pin(
                dependencies.verify,
                config.golden_root,
                config.golden_pin,
            )
            if (
                final.root_hash != initial.root_hash
                or final.export_root != initial.export_root
                or final.manifest != initial.manifest
                or final_pin_witness != initial_pin_witness
            ):
                raise Phase1BCertificationError(
                    "Golden authority or pin changed during Storage v4 certification"
                )
            repository.close()
            repository = None
            close_anchor = getattr(anchor, "close", None)
            if callable(close_anchor):
                close_anchor()
            anchor = None
            deadline.require_active()
            heartbeat.stop()
            deadline.require_active()
            _validate_certifier_provenance(config, dependencies)
            deadline.require_active()
            final_metrics = metrics.snapshot()
            coverage_metadata = _coverage_metadata(
                initial,
                market_gap_alert_count=cast(int, comparison["market_gap_rows"]),
            )
            report: dict[str, object] = {
                "audit": audit,
                "certification_scope": "TECHNICAL_STORAGE_AND_REPLAY_ORACLE",
                "comparison": comparison,
                "coverage_metadata_non_blocking": coverage_metadata,
                "format": PHASE1B_CERTIFICATION_FORMAT,
                "golden": {
                    "certified_source_identity_reaffirmed_from_pinned_manifest": True,
                    "export_reverified_unchanged": True,
                    "pin": initial_pin_witness.report_payload(),
                    "root": initial.root_hash,
                    "run_id": config.expected_run_id,
                    "sizes": _golden_sizes(initial),
                    "source_sha256": config.expected_source_sha256,
                },
                "identities": {
                    "golden_source": {
                        "config_hash": config.config_hash,
                        "release_code_sha256": config.release_code_sha256,
                        "run_id": config.expected_run_id,
                        "runtime_environment_sha256": (
                            config.runtime_environment_sha256
                        ),
                    },
                    "storage_v4_certifier": {
                        "candidate_id": _CERTIFIER_CANDIDATE_ID,
                        "code_sha256": config.certifier_code_sha256,
                        "configuration": _certifier_configuration_payload(config),
                        "configuration_sha256": (
                            _certifier_configuration_sha256(config)
                        ),
                        "runtime_environment_sha256": (
                            config.certifier_runtime_environment_sha256
                        ),
                    },
                },
                "limitations": [
                    "V3_COMPATIBILITY_IMPORT size is not V4_NATIVE capacity evidence",
                    "local witness does not prove Linux root ownership or compromised-admin isolation",
                    "Windows functional fsync testing is not an ext4 durability certification",
                    "raw-lake system integration and native capacity remain future evidence",
                    "peak RSS is either measured with an identified process API or explicitly UNAVAILABLE; no memory capacity claim is inferred",
                    "startup historical payload replay is O(tail), while metadata authentication includes the cumulative current manifest",
                    "this technical result is not economic or real-money authorization",
                ],
                "metrics": final_metrics,
                "mode": StorageMode.V3_COMPATIBILITY_IMPORT.value,
                "paper_only": True,
                "platform": {
                    "os_name": os.name,
                    "sys_platform": sys.platform,
                },
                "sizes": {
                    "anchor_bytes": config.anchor_path.stat().st_size,
                    "capacity_claim": "NONE",
                    "v3_compatibility_import_segment_bytes": audit["physical_bytes"],
                    "v3_compatibility_import_storage_v4_store_bytes": _tree_bytes(
                        config.store_root
                    ),
                },
                "startup": startup_payload,
                "status": PHASE1B_SUCCESS,
            }
            report_bytes = _canonical_artifact(report)
            durable_publish_immutable(config.report_path, report_bytes)
            report_sha256 = hashlib.sha256(report_bytes).hexdigest()
            complete = {
                "certifier_code_sha256": config.certifier_code_sha256,
                "certifier_configuration_sha256": (
                    _certifier_configuration_sha256(config)
                ),
                "certifier_runtime_environment_sha256": (
                    config.certifier_runtime_environment_sha256
                ),
                "format": PHASE1B_COMPLETE_FORMAT,
                "golden_root": initial.root_hash,
                "golden_pin_sha256": initial_pin_witness.sha256,
                "manifest_root": audit["manifest_root"],
                "report_sha256": report_sha256,
                "status": "COMPLETE",
            }
            deadline.require_active()
            progress.emit(
                {
                    "event": "certification_gates_passed",
                    "manifest_root": audit["manifest_root"],
                    "phase": "publication",
                    "report_sha256": report_sha256,
                    "status": "RUNNING",
                }
            )
            progress.close()
            deadline.require_active()
            try:
                durable_publish_immutable(
                    config.complete_path,
                    _canonical_artifact(complete),
                )
                deadline.finish_after_complete()
            except BaseException:
                _remove_failed_complete(config.complete_path)
                raise
            return Phase1BCertificationResult(
                output_root=config.output_root,
                report_path=config.report_path,
                complete_path=config.complete_path,
                report_sha256=report_sha256,
                final_prefix_root=cast(str, audit["final_prefix_root"]),
                manifest_root=cast(str, audit["manifest_root"]),
            )
        finally:
            heartbeat.stop(raise_on_error=False)
            if repository is not None:
                repository.close()
            if anchor is not None:
                close_anchor = getattr(anchor, "close", None)
                if callable(close_anchor):
                    close_anchor()


def certify_storage_v4_phase1b(
    config: Phase1BCertificationConfig,
    *,
    _dependencies: _Dependencies = DEFAULT_DEPENDENCIES,
    _test_expectations: _CertificationExpectations | None = None,
) -> Phase1BCertificationResult:
    """Run one complete, non-resumable, no-overwrite Phase 1B certification.

    Production callers cannot alter the fixed 252262/1011362/13/1 census.
    ``_test_expectations`` is deliberately private and exists only so the same
    orchestration can be exercised with tiny synthetic repositories.
    """

    if type(config) is not Phase1BCertificationConfig:
        raise TypeError("config must be Phase1BCertificationConfig")
    required = (
        _PRODUCTION_EXPECTATIONS
        if _test_expectations is None
        else _test_expectations
    )
    if type(required) is not _CertificationExpectations:
        raise TypeError("private test expectations have an invalid type")
    if (
        type(required.commits) is not int
        or required.commits < 1
        or type(required.rows) is not int
        or required.rows < 1
        or required.streams != len(GOLDEN_STREAM_NAMES)
        or type(required.market_gap_alert_rows) is not int
        or required.market_gap_alert_rows < 0
    ):
        raise ValueError("private test expectations are invalid")
    deadline = _Deadline(config.safety_seconds)
    deadline.start()
    try:
        return _certify_storage_v4_phase1b(
            config,
            dependencies=_dependencies,
            deadline=deadline,
            required=required,
        )
    except KeyboardInterrupt as error:
        if deadline.expired:
            raise TimeoutError(
                "Storage v4 Phase 1B exceeded its offline safety deadline"
            ) from error
        raise
    finally:
        deadline.stop()


def failure_verdict(error: BaseException) -> str:
    """Classify an observed failure without turning platform limits into blocks."""

    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, Phase1BGoldenDivergenceError):
            return PHASE1B_DIVERGED
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
    return PHASE1B_INTEGRITY_BLOCKED


__all__ = [
    "DEFAULT_HEARTBEAT_SECONDS",
    "GOLDEN_V3_EXPECTED_COMMITS",
    "GOLDEN_V3_EXPECTED_ROWS",
    "GOLDEN_V3_EXPECTED_STREAMS",
    "MAX_SAFETY_SECONDS",
    "PHASE1B_DIVERGED",
    "PHASE1B_INTEGRITY_BLOCKED",
    "PHASE1B_SUCCESS",
    "Phase1BCertificationConfig",
    "Phase1BCertificationError",
    "Phase1BCertificationResult",
    "Phase1BGoldenDivergenceError",
    "certify_storage_v4_phase1b",
    "failure_verdict",
]
