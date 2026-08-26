"""Concrete offline Storage V4 Phase 1C synthetic capacity runner.

The runner owns one fresh candidate directory and never publishes outside it.
It streams a frozen synthetic workload through the native raw-reference path,
creates repeated Paper checkpoints, reopens both authorities, and keeps normal
startup timing separate from the exhaustive offline audit.

This module is capacity plumbing only. It exposes no venue, order, wallet,
credential, or live-trading surface.
"""

from __future__ import annotations

import ctypes
import hashlib
import importlib
import json
import os
import stat
import sys
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path

from hyperlab.paper.storage_v4.anchor import LocalAnchor
from hyperlab.paper.storage_v4.candidate_tree import (
    CandidateTreeWitness,
    CandidateTreeWitnessError,
    witness_candidate_tree,
)
from hyperlab.paper.storage_v4.canonical import canonical_json_bytes
from hyperlab.paper.storage_v4.capacity import (
    CapacityBytePaths,
    CapacityMeasurement,
    CapacityWorkloadDigest,
    CapacityWorkloadHasher,
    CapacityWorkloadManifest,
    DurationObservations,
    SyntheticCapacityCommit,
    census_byte_categories,
    iter_capacity_commits,
)
from hyperlab.paper.storage_v4.capacity_adapter import SyntheticCapacityPhase1CAdapter
from hyperlab.paper.storage_v4.capacity_oracle import (
    CapacityOracleReport,
    compare_capacity_native_exact,
)
from hyperlab.paper.storage_v4.contracts import RawLakeId, StorageMode
from hyperlab.paper.storage_v4.durability import (
    DurabilityError,
    durable_publish_immutable,
    fsync_directory,
)
from hyperlab.paper.storage_v4.faults import FaultHook, FaultPoint
from hyperlab.paper.storage_v4.manifest import OpaqueIdentity
from hyperlab.paper.storage_v4.native_journal import (
    audit_native_frames,
    unbind_native_checkpoint_state,
)
from hyperlab.paper.storage_v4.phase1c_pipeline import (
    Phase1CAuthorityStatus,
    Phase1CCertificationReport,
    Phase1CSealResult,
    Phase1CWriter,
    inspect_phase1c_alignment,
)
from hyperlab.paper.storage_v4.phase1c_progress import BoundedAuditProgress
from hyperlab.paper.storage_v4.raw_reference import RawSegmentRef
from hyperlab.paper.storage_v4.raw_segment import (
    RawSegmentThresholds,
    raw_footer_index_physical_bytes,
)
from hyperlab.paper.storage_v4.raw_store import (
    DiskRawResolver,
    RawStore,
    RawStoreConfig,
    RawStorePaths,
)
from hyperlab.paper.storage_v4.repository import (
    RepositoryConfig,
    RepositoryPaths,
    StorageRepository,
)
from hyperlab.paper.storage_v4.startup_trace import (
    StartupFileAccessTrace,
    StartupTracePaths,
    trace_startup_file_access,
)
from hyperlab.paper.storage_v4.types import Hash32, RunId, StoreId

WriteBytesProbe = Callable[[], int | None]
RssProbe = Callable[[], int | None]
ProgressCallback = Callable[[Mapping[str, object]], None]

_GENESIS_PREFIX_ROOT = Hash32(b"\x00" * 32)
_RUNNER_IDENTITY_DOMAIN = b"HL4-PHASE1C-OFFLINE-CAPACITY-RUNNER-V1\x00"
_BOUNDARY_CERTIFICATE_ARTIFACT = (
    "STORAGE_V4_PHASE_1C_CUMULATIVE_BOUNDARY_CERTIFICATE_V1"
)


class OfflineCapacityRunnerErrorCode(StrEnum):
    TYPE_INVALID = "OFFLINE_CAPACITY_TYPE_INVALID"
    PATH_INVALID = "OFFLINE_CAPACITY_PATH_INVALID"
    CANDIDATE_EXISTS = "OFFLINE_CAPACITY_CANDIDATE_EXISTS"
    CANDIDATE_MISSING = "OFFLINE_CAPACITY_CANDIDATE_MISSING"
    WORKLOAD_DIVERGENCE = "OFFLINE_CAPACITY_WORKLOAD_DIVERGENCE"
    INTEGRITY_DIVERGENCE = "OFFLINE_CAPACITY_INTEGRITY_DIVERGENCE"
    MEASUREMENT_INVALID = "OFFLINE_CAPACITY_MEASUREMENT_INVALID"


class OfflineCapacityRunnerError(RuntimeError):
    """Fail-closed runner rejection with a stable machine-readable code."""

    def __init__(self, code: OfflineCapacityRunnerErrorCode, message: str) -> None:
        if type(code) is not OfflineCapacityRunnerErrorCode:
            raise TypeError("offline capacity runner error code is invalid")
        self.code = code
        super().__init__(f"{code.value}: {message}")


def _error(
    code: OfflineCapacityRunnerErrorCode,
    message: str,
) -> OfflineCapacityRunnerError:
    return OfflineCapacityRunnerError(code, message)


def _is_link_or_reparse_point(path: Path) -> bool:
    if path.is_symlink():
        return True
    if os.name != "nt":
        return False
    try:
        path_stat = os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    attributes = int(getattr(path_stat, "st_file_attributes", 0))
    reparse_mask = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & reparse_mask)


def _require_fresh_absolute_root(root: Path) -> None:
    if not isinstance(root, Path) or not root.is_absolute():
        raise _error(
            OfflineCapacityRunnerErrorCode.PATH_INVALID,
            "candidate_root must be an absolute pathlib.Path",
        )
    if root.exists() or _is_link_or_reparse_point(root):
        raise _error(
            OfflineCapacityRunnerErrorCode.CANDIDATE_EXISTS,
            "candidate_root already exists or is a link/reparse point",
        )
    parent = root.parent
    if _is_link_or_reparse_point(parent) or not parent.is_dir():
        raise _error(
            OfflineCapacityRunnerErrorCode.PATH_INVALID,
            "candidate_root parent must be an existing regular directory",
        )
    try:
        resolved_parent = parent.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise _error(
            OfflineCapacityRunnerErrorCode.PATH_INVALID,
            "candidate_root parent cannot be resolved",
        ) from exc
    if os.path.normcase(os.fspath(resolved_parent)) != os.path.normcase(
        os.fspath(parent.absolute())
    ):
        raise _error(
            OfflineCapacityRunnerErrorCode.PATH_INVALID,
            "candidate_root parent must not traverse a link or reparse point",
        )


def _require_existing_absolute_root(root: Path) -> None:
    if not isinstance(root, Path) or not root.is_absolute():
        raise _error(
            OfflineCapacityRunnerErrorCode.PATH_INVALID,
            "candidate_root must be an absolute pathlib.Path",
        )
    if _is_link_or_reparse_point(root) or not root.is_dir():
        raise _error(
            OfflineCapacityRunnerErrorCode.CANDIDATE_MISSING,
            "resume candidate_root is missing or is a link/reparse point",
        )
    try:
        resolved = root.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise _error(
            OfflineCapacityRunnerErrorCode.PATH_INVALID,
            "resume candidate_root cannot be resolved",
        ) from exc
    if os.path.normcase(os.fspath(resolved)) != os.path.normcase(
        os.fspath(root.absolute())
    ):
        raise _error(
            OfflineCapacityRunnerErrorCode.PATH_INVALID,
            "resume candidate_root must not traverse a link or reparse point",
        )
def _derived_hash(manifest: CapacityWorkloadManifest, label: bytes) -> Hash32:
    return Hash32(
        hashlib.sha256(
            _RUNNER_IDENTITY_DOMAIN + label + bytes.fromhex(manifest.sha256)
        ).digest()
    )


def _process_peak_rss_bytes() -> int | None:
    """Best-effort process-lifetime peak RSS from the operating system."""

    if os.name == "nt":
        try:
            from ctypes import wintypes

            class _ProcessMemoryCounters(ctypes.Structure):
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

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            psapi = ctypes.WinDLL("psapi", use_last_error=True)
            get_current_process = kernel32.GetCurrentProcess
            get_current_process.argtypes = []
            get_current_process.restype = wintypes.HANDLE
            get_process_memory_info = psapi.GetProcessMemoryInfo
            get_process_memory_info.argtypes = [
                wintypes.HANDLE,
                ctypes.POINTER(_ProcessMemoryCounters),
                wintypes.DWORD,
            ]
            get_process_memory_info.restype = wintypes.BOOL
            counters = _ProcessMemoryCounters()
            counters.cb = ctypes.sizeof(counters)
            if not get_process_memory_info(
                get_current_process(),
                ctypes.byref(counters),
                counters.cb,
            ):
                return None
            return int(counters.PeakWorkingSetSize)
        except (AttributeError, OSError, ValueError):
            return None

    try:
        resource = importlib.import_module("resource")
        usage = resource.getrusage(resource.RUSAGE_SELF)
        peak = usage.ru_maxrss
        if type(peak) not in (int, float) or peak < 0:
            return None
    except (AttributeError, ImportError, OSError, ValueError):
        return None
    return int(peak) if sys.platform == "darwin" else int(peak * 1024)


def current_process_cumulative_write_bytes() -> int | None:
    """Return Windows process I/O write-transfer bytes when the OS exposes them.

    This counter intentionally remains opt-in because it is process-scoped, not
    candidate-directory-scoped.  A fresh isolated capacity worker can use it to
    calculate an honest, explicitly scoped write-amplification observation.
    """

    if os.name != "nt":
        return None
    try:
        from ctypes import wintypes

        class _IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        get_current_process = kernel32.GetCurrentProcess
        get_current_process.argtypes = []
        get_current_process.restype = wintypes.HANDLE
        get_process_io_counters = kernel32.GetProcessIoCounters
        get_process_io_counters.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(_IoCounters),
        ]
        get_process_io_counters.restype = wintypes.BOOL
        counters = _IoCounters()
        if not get_process_io_counters(
            get_current_process(),
            ctypes.byref(counters),
        ):
            return None
        return int(counters.WriteTransferCount)
    except (AttributeError, OSError, ValueError):
        return None


def _validate_optional_counter(value: int | None, *, label: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < 0:
        raise _error(
            OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
            f"{label} probe must return a non-negative exact integer or None",
        )
    return value


def _transient_bytes(root: Path) -> int:
    """Return exact bytes currently held by recognizable transient files."""

    total = 0
    stack = [root]
    while stack:
        directory = stack.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                path = Path(entry.path)
                entry_stat = os.lstat(path)
                if entry.is_symlink() or _is_link_or_reparse_point(path):
                    raise _error(
                        OfflineCapacityRunnerErrorCode.PATH_INVALID,
                        "candidate acquired a link or reparse point during measurement",
                    )
                if stat.S_ISDIR(entry_stat.st_mode):
                    stack.append(path)
                    continue
                if not stat.S_ISREG(entry_stat.st_mode):
                    raise _error(
                        OfflineCapacityRunnerErrorCode.PATH_INVALID,
                        "candidate acquired a non-regular filesystem entry",
                    )
                if entry.name.endswith((".tmp", "-journal", "-wal", "-shm")):
                    total += entry_stat.st_size
    return total


@dataclass(slots=True)
class _MeasurementObserver:
    candidate_root: Path
    rss_probe: RssProbe
    checkpoint_publications_ns: list[int]
    manifest_publications_ns: list[int]
    scratch_peak_bytes: int = 0
    peak_rss_bytes: int | None = None
    _checkpoint_started_ns: int | None = None
    _paper_manifest_started_ns: int | None = None
    _raw_manifest_started_ns: int | None = None

    def sample_resources(self) -> None:
        observed_rss = _validate_optional_counter(self.rss_probe(), label="RSS")
        if observed_rss is not None:
            self.peak_rss_bytes = (
                observed_rss
                if self.peak_rss_bytes is None
                else max(self.peak_rss_bytes, observed_rss)
            )
        self.scratch_peak_bytes = max(
            self.scratch_peak_bytes,
            _transient_bytes(self.candidate_root),
        )

    def __call__(self, point: FaultPoint, /) -> None:
        if type(point) is not FaultPoint:
            raise TypeError("fault observation requires FaultPoint")
        now = time.perf_counter_ns()
        self.sample_resources()
        if point is FaultPoint.AFTER_SEGMENT_PUBLICATION:
            if self._checkpoint_started_ns is not None:
                raise _error(
                    OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
                    "nested checkpoint publication timing is invalid",
                )
            self._checkpoint_started_ns = now
        elif point is FaultPoint.BEFORE_CHECKPOINT_PUBLICATION:
            if self._checkpoint_started_ns is None:
                raise _error(
                    OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
                    "checkpoint construction lacked a durable segment boundary",
                )
        elif point is FaultPoint.AFTER_CHECKPOINT_PUBLICATION:
            started = self._checkpoint_started_ns
            if started is None:
                raise _error(
                    OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
                    "checkpoint publication ended without a start observation",
                )
            self.checkpoint_publications_ns.append(now - started)
            self._checkpoint_started_ns = None
        elif point is FaultPoint.BEFORE_MANIFEST_PUBLICATION:
            if self._paper_manifest_started_ns is not None:
                raise _error(
                    OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
                    "nested Paper manifest publication timing is invalid",
                )
            self._paper_manifest_started_ns = now
        elif point is FaultPoint.AFTER_MANIFEST_PUBLICATION:
            started = self._paper_manifest_started_ns
            if started is None:
                raise _error(
                    OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
                    "Paper manifest publication ended without a start observation",
                )
            self.manifest_publications_ns.append(now - started)
            self._paper_manifest_started_ns = None
        elif point is FaultPoint.BEFORE_RAW_MANIFEST_PUBLICATION:
            if self._raw_manifest_started_ns is not None:
                raise _error(
                    OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
                    "nested raw manifest publication timing is invalid",
                )
            self._raw_manifest_started_ns = now
        elif point is FaultPoint.AFTER_RAW_MANIFEST_PUBLICATION:
            started = self._raw_manifest_started_ns
            if started is None:
                raise _error(
                    OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
                    "raw manifest publication ended without a start observation",
                )
            self.manifest_publications_ns.append(now - started)
            self._raw_manifest_started_ns = None

    def assert_balanced(self) -> None:
        if any(
            started is not None
            for started in (
                self._checkpoint_started_ns,
                self._paper_manifest_started_ns,
                self._raw_manifest_started_ns,
            )
        ):
            raise _error(
                OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
                "publication timing ended with an unmatched durability boundary",
            )


@dataclass(frozen=True, slots=True)
class OfflineCapacityRunEvidence:
    """Integrity reports and explicit scopes retained beside one measurement."""

    candidate_root: Path
    audited_candidate_tree: CandidateTreeWitness
    raw_store_id: str
    raw_lake_id: str
    paper_store_id: str
    run_id: str
    config_identity: str
    code_identity: str
    runtime_identity: str
    certification: Phase1CCertificationReport
    startup_file_trace: StartupFileAccessTrace
    oracle: CapacityOracleReport
    batch_count: int
    seal_count: int
    max_batch_commits_observed: int
    wall_scope: str
    metadata_scope: str
    scratch_scope: str
    scratch_status: str
    rss_scope: str
    checkpoint_scope: str
    manifest_scope: str
    storage_rate_scope: str

    def __post_init__(self) -> None:
        if not isinstance(self.candidate_root, Path) or not self.candidate_root.is_absolute():
            raise ValueError("candidate_root must be an absolute pathlib.Path")
        if (
            not isinstance(self.audited_candidate_tree, CandidateTreeWitness)
            or self.audited_candidate_tree.root != self.candidate_root
        ):
            raise ValueError("audited candidate tree must bind the exact candidate root")
        for text_label, text_value in (
            ("raw_store_id", self.raw_store_id),
            ("raw_lake_id", self.raw_lake_id),
            ("paper_store_id", self.paper_store_id),
            ("run_id", self.run_id),
        ):
            if type(text_value) is not str or not text_value:
                raise ValueError(f"{text_label} must be non-empty text")
        for digest_label, digest_value in (
            ("config_identity", self.config_identity),
            ("code_identity", self.code_identity),
            ("runtime_identity", self.runtime_identity),
        ):
            if (
                type(digest_value) is not str
                or len(digest_value) != 64
                or digest_value != digest_value.lower()
                or any(character not in "0123456789abcdef" for character in digest_value)
            ):
                raise ValueError(f"{digest_label} must be a lowercase SHA-256")
        if not isinstance(self.certification, Phase1CCertificationReport):
            raise TypeError("certification must be Phase1CCertificationReport")
        if not isinstance(self.startup_file_trace, StartupFileAccessTrace):
            raise TypeError("startup_file_trace must be StartupFileAccessTrace")
        if not isinstance(self.oracle, CapacityOracleReport):
            raise TypeError("oracle must be CapacityOracleReport")
        for label, value in (
            ("batch_count", self.batch_count),
            ("seal_count", self.seal_count),
            ("max_batch_commits_observed", self.max_batch_commits_observed),
        ):
            if type(value) is not int or value < 1:
                raise ValueError(f"{label} must be a positive exact integer")

    def payload(self) -> dict[str, object]:
        certification = self.certification
        native = certification.native_audit
        return {
            "authority": {
                "candidate_root": str(self.candidate_root),
                "code_identity": self.code_identity,
                "config_identity": self.config_identity,
                "paper_store_id": self.paper_store_id,
                "raw_lake_id": self.raw_lake_id,
                "raw_store_id": self.raw_store_id,
                "run_id": self.run_id,
                "runtime_identity": self.runtime_identity,
            },
            "batching": {
                "batch_count": self.batch_count,
                "max_batch_commits_observed": self.max_batch_commits_observed,
                "seal_count": self.seal_count,
            },
            "integrity": {
                "alignment_status": certification.alignment.status.value,
                "audited_candidate_tree": self.audited_candidate_tree.payload(),
                "commit_count": native.commit_count,
                "final_prefix_root": native.final_prefix_root.hex(),
                "market_gap_count": native.market_gap_count,
                "oracle_commit_count": self.oracle.commit_count,
                "oracle_final_prefix_root": self.oracle.final_prefix_root,
                "oracle_logical_row_count": self.oracle.logical_row_count,
                "oracle_workload_sha256": self.oracle.workload_sha256,
                "raw_reference_count": native.raw_reference_count,
                "raw_reference_prefix_root": native.raw_reference_prefix_root.hex(),
            },
            "scopes": {
                "checkpoint": self.checkpoint_scope,
                "manifest": self.manifest_scope,
                "metadata": self.metadata_scope,
                "rss": self.rss_scope,
                "scratch": self.scratch_scope,
                "scratch_status": self.scratch_status,
                "storage_rate": self.storage_rate_scope,
                "wall": self.wall_scope,
            },
            "startup": {
                "file_access_trace": self.startup_file_trace.payload(),
                "paper_checkpoint_used": certification.paper_startup.checkpoint_used,
                "paper_historical_commits_not_read": (
                    certification.paper_startup.historical_commits_not_read
                ),
                "paper_segments_read": certification.paper_startup.segments_read,
                "paper_tail_entries_replayed": (
                    certification.paper_startup.tail_entries_replayed
                ),
                "raw_historical_segments_read": (
                    certification.raw_startup.historical_segments_read
                ),
                "raw_manifest_namespace_entries_scanned": (
                    certification.raw_startup.manifest_namespace_entries_scanned
                ),
                "raw_manifests_opened": certification.raw_startup.manifests_opened,
            },
        }


@dataclass(frozen=True, slots=True)
class CumulativeCapacityAccounting:
    """Exact unique-workload and durable-ingestion accounting for one run."""

    commits_generated: int
    commits_ingested: int
    prefix_commits_reingested: int
    prefix_commits_audited: int
    suffix_commits_reconstructed: int
    raw_commits_reused: int
    raw_seal_count: int
    worker_count: int = 1
    store_count: int = 1
    stream_count: int = 1
    resume_count: int = 0

    def __post_init__(self) -> None:
        for label, value in (
            ("commits_generated", self.commits_generated),
            ("commits_ingested", self.commits_ingested),
            ("prefix_commits_reingested", self.prefix_commits_reingested),
            ("prefix_commits_audited", self.prefix_commits_audited),
            ("suffix_commits_reconstructed", self.suffix_commits_reconstructed),
            ("raw_commits_reused", self.raw_commits_reused),
            ("raw_seal_count", self.raw_seal_count),
            ("resume_count", self.resume_count),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{label} must be a non-negative exact integer")
        for label, value in (
            ("worker_count", self.worker_count),
            ("store_count", self.store_count),
            ("stream_count", self.stream_count),
        ):
            if type(value) is not int or value != 1:
                raise ValueError(f"{label} must be exactly one")
        if self.prefix_commits_reingested != 0:
            raise ValueError("certified prefixes must never be reingested")
        if self.commits_generated != self.commits_ingested:
            raise ValueError("unique generated and durably ingested commits must match")
        if self.raw_commits_reused != self.suffix_commits_reconstructed:
            raise ValueError("raw reuse must equal the reconstructed raw-only suffix")

    def payload(self) -> dict[str, int]:
        return {
            "commits_generated": self.commits_generated,
            "commits_ingested": self.commits_ingested,
            "prefix_commits_audited": self.prefix_commits_audited,
            "prefix_commits_reingested": self.prefix_commits_reingested,
            "raw_commits_reused": self.raw_commits_reused,
            "raw_seal_count": self.raw_seal_count,
            "resume_count": self.resume_count,
            "store_count": self.store_count,
            "stream_count": self.stream_count,
            "suffix_commits_reconstructed": self.suffix_commits_reconstructed,
            "worker_count": self.worker_count,
            "generator_emissions": self.commits_generated + self.raw_commits_reused,
        }


@dataclass(frozen=True, slots=True)
class DurableCapacityBoundaryCertificate:
    commit_count: int
    manifest_sha256: str
    path: Path
    sha256: str
    previous_sha256: str | None
    raw_manifest_root: Hash32
    paper_manifest_root: Hash32
    checkpoint_root: Hash32
    canonical_payload: bytes
    payload_mapping: Mapping[str, object]
    measurement_mapping: Mapping[str, object]
    evidence_mapping: Mapping[str, object]
    typed_measurement: CapacityMeasurement | None = None
    typed_evidence: OfflineCapacityRunEvidence | None = None

    def __post_init__(self) -> None:
        if type(self.commit_count) is not int or self.commit_count < 1:
            raise ValueError("certificate commit_count must be positive")
        for label, digest in (
            ("manifest_sha256", self.manifest_sha256),
            ("sha256", self.sha256),
        ):
            if (
                type(digest) is not str
                or len(digest) != 64
                or digest != digest.lower()
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ValueError(f"certificate {label} must be a lowercase SHA-256")
        if not isinstance(self.path, Path) or not self.path.is_absolute():
            raise ValueError("certificate path must be absolute")
        for root in (
            self.raw_manifest_root,
            self.paper_manifest_root,
            self.checkpoint_root,
        ):
            if type(root) is not Hash32:
                raise TypeError("certificate authority roots must be Hash32")
        if type(self.canonical_payload) is not bytes:
            raise TypeError("certificate canonical_payload must be exact bytes")
        if hashlib.sha256(self.canonical_payload).hexdigest() != self.sha256:
            raise ValueError("certificate SHA-256 differs from canonical payload")
        for label, value in (
            ("payload_mapping", self.payload_mapping),
            ("measurement_mapping", self.measurement_mapping),
            ("evidence_mapping", self.evidence_mapping),
        ):
            if not isinstance(value, Mapping):
                raise TypeError(f"certificate {label} must be a mapping")
        payload = dict(self.payload_mapping)
        if canonical_json_bytes(payload) != self.canonical_payload:
            raise ValueError("certificate mapping differs from canonical payload bytes")
        if (
            payload.get("measurement") != dict(self.measurement_mapping)
            or payload.get("evidence") != dict(self.evidence_mapping)
            or payload.get("authority")
            != {
                "checkpoint_root": self.checkpoint_root.hex(),
                "paper_manifest_root": self.paper_manifest_root.hex(),
                "raw_manifest_root": self.raw_manifest_root.hex(),
            }
        ):
            raise ValueError("certificate mappings or authority roots diverged")
        if (self.typed_measurement is None) != (self.typed_evidence is None):
            raise ValueError("typed measurement and evidence must be present together")
        if self.typed_measurement is not None and (
            self.typed_measurement.payload() != dict(self.measurement_mapping)
            or self.typed_evidence is None
            or self.typed_evidence.payload() != dict(self.evidence_mapping)
        ):
            raise ValueError("typed boundary differs from durable payload mappings")
        if self.previous_sha256 is not None and (
            type(self.previous_sha256) is not str
            or len(self.previous_sha256) != 64
            or self.previous_sha256 != self.previous_sha256.lower()
            or any(
                character not in "0123456789abcdef"
                for character in self.previous_sha256
            )
        ):
            raise ValueError("previous certificate SHA-256 is invalid")


@dataclass(frozen=True, slots=True)
class CumulativeCapacityBoundaryResult:
    """Authenticated certificate for one exact prefix of the terminal workload."""

    manifest: CapacityWorkloadManifest
    workload_prefix: CapacityWorkloadDigest
    measurement: CapacityMeasurement
    evidence: OfflineCapacityRunEvidence
    raw_manifest_root: Hash32
    paper_manifest_root: Hash32
    checkpoint_root: Hash32
    certificate: DurableCapacityBoundaryCertificate

    def __post_init__(self) -> None:
        if not isinstance(self.manifest, CapacityWorkloadManifest):
            raise TypeError("boundary manifest must be CapacityWorkloadManifest")
        if self.workload_prefix != self.manifest.digest:
            raise ValueError("boundary workload prefix differs from its manifest")
        if not isinstance(self.measurement, CapacityMeasurement):
            raise TypeError("boundary measurement must be CapacityMeasurement")
        if not isinstance(self.evidence, OfflineCapacityRunEvidence):
            raise TypeError("boundary evidence must be OfflineCapacityRunEvidence")
        if (
            self.measurement.commit_count != self.manifest.commit_count
            or self.measurement.logical_row_count != self.manifest.logical_row_count
            or self.measurement.observed_workload_sha256
            != self.manifest.workload_sha256
        ):
            raise ValueError("boundary measurement differs from its exact prefix")
        for root in (
            self.raw_manifest_root,
            self.paper_manifest_root,
            self.checkpoint_root,
        ):
            if type(root) is not Hash32:
                raise TypeError("boundary authority roots must be Hash32")
        if not isinstance(self.certificate, DurableCapacityBoundaryCertificate):
            raise TypeError("boundary certificate must be durable")
        if (
            self.certificate.commit_count != self.manifest.commit_count
            or self.certificate.manifest_sha256 != self.manifest.sha256
            or self.certificate.raw_manifest_root != self.raw_manifest_root
            or self.certificate.paper_manifest_root != self.paper_manifest_root
            or self.certificate.checkpoint_root != self.checkpoint_root
            or self.certificate.typed_measurement is not self.measurement
            or self.certificate.typed_evidence is not self.evidence
        ):
            raise ValueError("boundary certificate differs from its manifest")


@dataclass(frozen=True, slots=True)
class CumulativeCapacityRunResult:
    """All authenticated boundaries produced by one store and one input stream."""

    candidate_root: Path
    terminal_manifest: CapacityWorkloadManifest
    boundary_manifests: tuple[CapacityWorkloadManifest, ...]
    boundaries: tuple[DurableCapacityBoundaryCertificate, ...]
    typed_boundaries: tuple[CumulativeCapacityBoundaryResult, ...]
    terminal_shared_candidate_tree: CandidateTreeWitness
    accounting: CumulativeCapacityAccounting

    def __post_init__(self) -> None:
        if not isinstance(self.candidate_root, Path) or not self.candidate_root.is_absolute():
            raise ValueError("candidate_root must be an absolute pathlib.Path")
        if not isinstance(self.terminal_manifest, CapacityWorkloadManifest):
            raise TypeError("terminal_manifest must be CapacityWorkloadManifest")
        if not self.boundaries or not self.typed_boundaries:
            raise ValueError("cumulative result requires authenticated boundaries")
        if (
            type(self.boundary_manifests) is not tuple
            or any(
                not isinstance(manifest, CapacityWorkloadManifest)
                for manifest in self.boundary_manifests
            )
            or self.boundary_manifests[-1] != self.terminal_manifest
        ):
            raise ValueError("boundary manifests differ from terminal manifest")
        if tuple(boundary.commit_count for boundary in self.boundaries) != tuple(
            manifest.commit_count for manifest in self.boundary_manifests
        ) or tuple(boundary.manifest_sha256 for boundary in self.boundaries) != tuple(
            manifest.sha256 for manifest in self.boundary_manifests
        ):
            raise ValueError("durable boundary set differs from requested manifests")
        if self.typed_boundaries[-1].manifest != self.terminal_manifest:
            raise ValueError("terminal boundary differs from terminal manifest")
        if any(
            boundary.evidence.candidate_root != self.candidate_root
            for boundary in self.typed_boundaries
        ):
            raise ValueError("all boundaries must bind the same candidate root")
        if not isinstance(self.accounting, CumulativeCapacityAccounting):
            raise TypeError("accounting must be CumulativeCapacityAccounting")
        if self.accounting.commits_ingested != self.terminal_manifest.commit_count:
            raise ValueError("terminal ingestion count differs from terminal manifest")
        if self.boundaries[-1].commit_count != (
            self.terminal_manifest.commit_count
        ):
            raise ValueError("terminal durable boundary certificate is missing")
        if (
            not isinstance(self.terminal_shared_candidate_tree, CandidateTreeWitness)
            or self.terminal_shared_candidate_tree
            != self.typed_boundaries[-1].evidence.audited_candidate_tree
        ):
            raise ValueError("terminal shared candidate tree differs from terminal evidence")
        previous: str | None = None
        for certificate in self.boundaries:
            if certificate.previous_sha256 != previous:
                raise ValueError("durable boundary certificate chain is broken")
            previous = certificate.sha256
        durable_hashes = {boundary.sha256 for boundary in self.boundaries}
        if any(
            boundary.certificate.sha256 not in durable_hashes
            for boundary in self.typed_boundaries
        ):
            raise ValueError("typed boundary lacks its durable representation")

    @property
    def certificates(self) -> tuple[DurableCapacityBoundaryCertificate, ...]:
        """Backwards-compatible alias for the complete durable boundary set."""

        return self.boundaries


def _validate_cumulative_manifests(
    manifests: tuple[CapacityWorkloadManifest, ...],
) -> CapacityWorkloadManifest:
    if len(manifests) < 2:
        raise _error(
            OfflineCapacityRunnerErrorCode.TYPE_INVALID,
            "cumulative run requires at least two boundary manifests",
        )
    if any(not isinstance(item, CapacityWorkloadManifest) for item in manifests):
        raise _error(
            OfflineCapacityRunnerErrorCode.TYPE_INVALID,
            "cumulative boundaries must be CapacityWorkloadManifest values",
        )
    counts = tuple(item.commit_count for item in manifests)
    if counts != tuple(sorted(set(counts))):
        raise _error(
            OfflineCapacityRunnerErrorCode.WORKLOAD_DIVERGENCE,
            "cumulative boundary commit counts must be strictly increasing",
        )
    terminal = manifests[-1]
    for item in manifests[:-1]:
        if replace(
            item.config,
            commit_count=terminal.config.commit_count,
            market_gap_count=terminal.config.market_gap_count,
        ) != terminal.config:
            raise _error(
                OfflineCapacityRunnerErrorCode.WORKLOAD_DIVERGENCE,
                "cumulative boundaries differ outside exact prefix counts",
            )
    return terminal


def _certificate_path(directory: Path, manifest: CapacityWorkloadManifest) -> Path:
    return directory / f"{manifest.commit_count:016d}-{manifest.sha256}.json"


def _certificate_payload(
    *,
    terminal_manifest: CapacityWorkloadManifest,
    boundary_manifest: CapacityWorkloadManifest,
    workload_prefix: CapacityWorkloadDigest,
    measurement: CapacityMeasurement,
    evidence: OfflineCapacityRunEvidence,
    terminal_seal: Phase1CSealResult,
    previous_sha256: str | None,
) -> dict[str, object]:
    return {
        "artifact": _BOUNDARY_CERTIFICATE_ARTIFACT,
        "authority": {
            "checkpoint_root": terminal_seal.paper_seal.checkpoint.root.hex(),
            "paper_manifest_root": terminal_seal.paper_seal.manifest.identity.root.hex(),
            "raw_manifest_root": terminal_seal.binding.raw_manifest_root.hex(),
        },
        "boundary_commit_count": boundary_manifest.commit_count,
        "boundary_manifest": boundary_manifest.payload(),
        "boundary_manifest_sha256": boundary_manifest.sha256,
        "evidence": evidence.payload(),
        "measurement": measurement.payload(),
        "previous_certificate_sha256": previous_sha256,
        "terminal_manifest_sha256": terminal_manifest.sha256,
        "workload_prefix": {
            "commit_count": workload_prefix.commit_count,
            "logical_row_count": workload_prefix.logical_row_count,
            "sha256": workload_prefix.sha256,
        },
    }


def _publish_boundary_certificate(
    *,
    directory: Path,
    terminal_manifest: CapacityWorkloadManifest,
    boundary_manifest: CapacityWorkloadManifest,
    workload_prefix: CapacityWorkloadDigest,
    measurement: CapacityMeasurement,
    evidence: OfflineCapacityRunEvidence,
    terminal_seal: Phase1CSealResult,
    previous_sha256: str | None,
) -> DurableCapacityBoundaryCertificate:
    payload = _certificate_payload(
        terminal_manifest=terminal_manifest,
        boundary_manifest=boundary_manifest,
        workload_prefix=workload_prefix,
        measurement=measurement,
        evidence=evidence,
        terminal_seal=terminal_seal,
        previous_sha256=previous_sha256,
    )
    encoded = canonical_json_bytes(payload)
    path = _certificate_path(directory, boundary_manifest)
    try:
        durable_publish_immutable(path, encoded)
    except (DurabilityError, OSError, TypeError, ValueError) as exc:
        raise _error(
            OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
            "boundary certificate publication was not durable",
        ) from exc
    return DurableCapacityBoundaryCertificate(
        commit_count=boundary_manifest.commit_count,
        manifest_sha256=boundary_manifest.sha256,
        path=path,
        sha256=hashlib.sha256(encoded).hexdigest(),
        previous_sha256=previous_sha256,
        raw_manifest_root=terminal_seal.binding.raw_manifest_root,
        paper_manifest_root=terminal_seal.paper_seal.manifest.identity.root,
        checkpoint_root=terminal_seal.paper_seal.checkpoint.root,
        canonical_payload=encoded,
        payload_mapping=payload,
        measurement_mapping=measurement.payload(),
        evidence_mapping=evidence.payload(),
        typed_measurement=measurement,
        typed_evidence=evidence,
    )


def _load_boundary_certificates(
    *,
    directory: Path,
    candidate_root: Path,
    manifests: tuple[CapacityWorkloadManifest, ...],
    terminal_manifest: CapacityWorkloadManifest,
    checkpoint_commit_count: int,
) -> tuple[DurableCapacityBoundaryCertificate, ...]:
    if _is_link_or_reparse_point(directory) or not directory.is_dir():
        raise _error(
            OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
            "durable boundary certificate directory is missing or unsafe",
        )
    expected = {
        item.commit_count: item
        for item in manifests
        if item.commit_count <= checkpoint_commit_count
    }
    published: dict[
        int,
        tuple[
            Path,
            bytes,
            dict[str, object],
            Hash32,
            Hash32,
            Hash32,
            dict[str, object],
            dict[str, object],
        ],
    ] = {}
    for path in directory.iterdir():
        if _is_link_or_reparse_point(path) or not path.is_file():
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "boundary certificate namespace contains an unsafe entry",
            )
        if path.name.startswith(".") and path.name.endswith(".tmp"):
            continue
        try:
            encoded = path.read_bytes()
            value = json.loads(encoded.decode("utf-8", errors="strict"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "boundary certificate cannot be decoded",
            ) from exc
        if type(value) is not dict or canonical_json_bytes(value) != encoded:
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "boundary certificate is not exact canonical JSON",
            )
        commit_count = value.get("boundary_commit_count")
        if type(commit_count) is not int or commit_count in published:
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "boundary certificate namespace is forked or ambiguous",
            )
        manifest = expected.get(commit_count)
        evidence = value.get("evidence")
        measurement = value.get("measurement")
        authority = value.get("authority")
        evidence_authority = (
            evidence.get("authority") if type(evidence) is dict else None
        )
        raw_root_value = (
            authority.get("raw_manifest_root") if type(authority) is dict else None
        )
        paper_root_value = (
            authority.get("paper_manifest_root") if type(authority) is dict else None
        )
        checkpoint_root_value = (
            authority.get("checkpoint_root") if type(authority) is dict else None
        )
        if any(
            type(root_value) is not str
            for root_value in (
                raw_root_value,
                paper_root_value,
                checkpoint_root_value,
            )
        ):
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "boundary certificate authority roots are invalid",
            )
        assert isinstance(raw_root_value, str)
        assert isinstance(paper_root_value, str)
        assert isinstance(checkpoint_root_value, str)
        try:
            raw_manifest_root = Hash32.from_hex(raw_root_value)
            paper_manifest_root = Hash32.from_hex(paper_root_value)
            checkpoint_root = Hash32.from_hex(checkpoint_root_value)
        except (TypeError, ValueError) as exc:
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "boundary certificate authority roots are invalid",
            ) from exc
        if (
            manifest is None
            or path != _certificate_path(directory, manifest)
            or value.get("artifact") != _BOUNDARY_CERTIFICATE_ARTIFACT
            or value.get("boundary_manifest_sha256") != manifest.sha256
            or value.get("boundary_manifest") != manifest.payload()
            or value.get("terminal_manifest_sha256") != terminal_manifest.sha256
            or type(evidence_authority) is not dict
            or evidence_authority.get("candidate_root") != str(candidate_root)
            or type(measurement) is not dict
            or type(evidence) is not dict
            or value.get("workload_prefix")
            != {
                "commit_count": manifest.digest.commit_count,
                "logical_row_count": manifest.digest.logical_row_count,
                "sha256": manifest.digest.sha256,
            }
        ):
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "boundary certificate differs from the requested cumulative run",
            )
        published[commit_count] = (
            path,
            encoded,
            value,
            raw_manifest_root,
            paper_manifest_root,
            checkpoint_root,
            measurement,
            evidence,
        )
    if set(published) != set(expected):
        raise _error(
            OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
            "a completed cumulative boundary lacks its durable certificate",
        )
    certificates: list[DurableCapacityBoundaryCertificate] = []
    previous: str | None = None
    for commit_count in sorted(published):
        (
            path,
            encoded,
            value,
            raw_manifest_root,
            paper_manifest_root,
            checkpoint_root,
            measurement,
            evidence,
        ) = published[commit_count]
        if value.get("previous_certificate_sha256") != previous:
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "durable boundary certificate chain is forked or has a gap",
            )
        digest = hashlib.sha256(encoded).hexdigest()
        manifest = expected[commit_count]
        certificates.append(
            DurableCapacityBoundaryCertificate(
                commit_count=commit_count,
                manifest_sha256=manifest.sha256,
                path=path,
                sha256=digest,
                previous_sha256=previous,
                raw_manifest_root=raw_manifest_root,
                paper_manifest_root=paper_manifest_root,
                checkpoint_root=checkpoint_root,
                canonical_payload=encoded,
                payload_mapping=value,
                measurement_mapping=measurement,
                evidence_mapping=evidence,
            )
        )
        previous = digest
    return tuple(certificates)


def _capacity_paths(
    *,
    staging: Path,
    raw_anchor: LocalAnchor,
    paper_anchor: LocalAnchor,
    raw_paths: RawStorePaths,
    paper_paths: RepositoryPaths,
    raw_embedded_index_bytes: tuple[tuple[Path, int], ...],
) -> CapacityBytePaths:
    return CapacityBytePaths(
        raw_segments=(raw_paths.segments,),
        raw_manifests=(raw_paths.manifests,),
        raw_index=(),
        raw_embedded_index_bytes=raw_embedded_index_bytes,
        paper_segments=(paper_paths.segments,),
        paper_overlay=(paper_paths.overlay,),
        paper_checkpoints=(paper_paths.checkpoints,),
        paper_manifests=(paper_paths.manifests,),
        raw_anchors_witnesses=(raw_anchor.path, raw_anchor.writer_lease_path),
        paper_anchors_witnesses=(
            paper_anchor.path,
            paper_anchor.writer_lease_path,
            paper_paths.writer_lease,
        ),
        raw_current_cache=(raw_paths.current,),
        paper_current_cache=(paper_paths.current,),
        scratch=(staging,),
    )


class OfflinePhase1CCapacityRunner:
    """Run one synthetic manifest into one fresh, local, offline candidate."""

    __slots__ = (
        "_batch_size",
        "_candidate_root",
        "_checkpoint_every_batches",
        "_code_identity",
        "_fault_hook",
        "_last_cumulative_result",
        "_last_evidence",
        "_progress",
        "_raw_thresholds",
        "_rss_probe",
        "_runtime_identity",
        "_write_bytes_probe",
    )

    def __init__(
        self,
        *,
        candidate_root: Path,
        code_identity: Hash32,
        runtime_identity: Hash32,
        batch_size: int = 10_000,
        checkpoint_every_batches: int = 1,
        raw_thresholds: RawSegmentThresholds | None = None,
        rss_probe: RssProbe | None = None,
        write_bytes_probe: WriteBytesProbe | None = None,
        progress: ProgressCallback | None = None,
        fault_hook: FaultHook = None,
    ) -> None:
        if not isinstance(candidate_root, Path) or not candidate_root.is_absolute():
            raise _error(
                OfflineCapacityRunnerErrorCode.PATH_INVALID,
                "candidate_root must be an absolute pathlib.Path",
            )
        if type(code_identity) is not Hash32:
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "code_identity must be Hash32",
            )
        if type(runtime_identity) is not Hash32:
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "runtime_identity must be Hash32",
            )
        for label, value in (
            ("batch_size", batch_size),
            ("checkpoint_every_batches", checkpoint_every_batches),
        ):
            if type(value) is not int or value < 1:
                raise _error(
                    OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                    f"{label} must be a positive exact integer",
                )
        if batch_size > 10_000:
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "batch_size exceeds the synthetic adapter bound of 10000 commits",
            )
        if raw_thresholds is not None and type(raw_thresholds) is not RawSegmentThresholds:
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "raw_thresholds must be RawSegmentThresholds or None",
            )
        if rss_probe is not None and not callable(rss_probe):
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "rss_probe must be callable or None",
            )
        if write_bytes_probe is not None and not callable(write_bytes_probe):
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "write_bytes_probe must be callable or None",
            )
        if progress is not None and not callable(progress):
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "progress must be callable or None",
            )
        if fault_hook is not None and not callable(fault_hook):
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "fault_hook must be callable or None",
            )
        self._candidate_root = candidate_root
        self._code_identity = code_identity
        self._runtime_identity = runtime_identity
        self._batch_size = batch_size
        self._checkpoint_every_batches = checkpoint_every_batches
        self._raw_thresholds = raw_thresholds or RawSegmentThresholds()
        self._rss_probe = rss_probe or _process_peak_rss_bytes
        self._write_bytes_probe = write_bytes_probe
        self._progress = progress
        self._fault_hook = fault_hook
        self._last_evidence: OfflineCapacityRunEvidence | None = None
        self._last_cumulative_result: CumulativeCapacityRunResult | None = None

    @property
    def candidate_root(self) -> Path:
        return self._candidate_root

    @property
    def last_evidence(self) -> OfflineCapacityRunEvidence | None:
        return self._last_evidence

    @property
    def last_cumulative_result(self) -> CumulativeCapacityRunResult | None:
        return self._last_cumulative_result

    def _emit(self, **payload: object) -> None:
        if self._progress is not None:
            self._progress(payload)

    def _configs(
        self,
        manifest: CapacityWorkloadManifest,
    ) -> tuple[RawStoreConfig, RepositoryConfig]:
        suffix = manifest.sha256[:16]
        config_identity = _derived_hash(manifest, b"config\x00")
        run_id = RunId(f"SYNTHETIC_STORAGE_V4_PHASE1C/{suffix}/run")
        return (
            RawStoreConfig(
                store_id=StoreId(f"SYNTHETIC_STORAGE_V4_PHASE1C/{suffix}/raw"),
                lake_id=RawLakeId(f"SYNTHETIC_STORAGE_V4_PHASE1C/{suffix}/lake"),
                config_identity=config_identity,
            ),
            RepositoryConfig(
                store_id=StoreId(f"SYNTHETIC_STORAGE_V4_PHASE1C/{suffix}/paper"),
                run_id=run_id,
                mode=StorageMode.V4_NATIVE,
                run_identity=OpaqueIdentity(_derived_hash(manifest, b"run\x00")),
                config_identity=OpaqueIdentity(config_identity),
                code_identity=OpaqueIdentity(self._code_identity),
                runtime_identity=OpaqueIdentity(self._runtime_identity),
                start_prefix_root=_GENESIS_PREFIX_ROOT,
            ),
        )

    def _audit_cumulative_boundary(
        self,
        *,
        manifest: CapacityWorkloadManifest,
        terminal_seal: Phase1CSealResult,
        raw_config: RawStoreConfig,
        paper_config: RepositoryConfig,
        root: Path,
        staging: Path,
        raw_root: Path,
        paper_root: Path,
        raw_paths: RawStorePaths,
        paper_paths: RepositoryPaths,
        raw_anchor: LocalAnchor,
        paper_anchor: LocalAnchor,
        observer: _MeasurementObserver,
        emit_snapshot: Callable[[Mapping[str, object]], None],
        workload_wall_ns: int,
        workload_cpu_ns: int,
        cumulative_bytes_written: int | None,
        seal_durations: list[int],
        batch_count: int,
        max_batch_commits_observed: int,
        resumed: bool,
    ) -> tuple[CapacityMeasurement, OfflineCapacityRunEvidence]:
        observer.assert_balanced()
        observer.sample_resources()
        if workload_wall_ns < 1:
            raise _error(
                OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
                "cumulative workload duration was not positive",
            )
        try:
            audited_candidate_tree = witness_candidate_tree(
                root,
                progress=emit_snapshot,
            )
        except CandidateTreeWitnessError as error:
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                f"candidate tree could not be bound before boundary audit: {error}",
            ) from error

        startup_trace_paths = StartupTracePaths(
            candidate_root=root,
            raw_root=raw_root,
            paper_root=paper_root,
            raw_anchor=raw_anchor.path,
            paper_anchor=paper_anchor.path,
            raw_anchor_writer_lease=raw_anchor.writer_lease_path,
            paper_anchor_writer_lease=paper_anchor.writer_lease_path,
            paper_writer_lease=paper_paths.writer_lease,
        )
        with trace_startup_file_access(startup_trace_paths) as startup_trace_recorder:
            startup_started = time.perf_counter_ns()
            reopened_raw = RawStore.open_existing(
                raw_root,
                anchor=raw_anchor,
                config=raw_config,
            )
            try:
                reopened_paper = StorageRepository.open_existing(
                    paper_root,
                    anchor=paper_anchor,
                    config=paper_config,
                )
                try:
                    startup_ns = time.perf_counter_ns() - startup_started
                    raw_startup = reopened_raw.startup_report
                    paper_startup = reopened_paper.startup_report
                    alignment = inspect_phase1c_alignment(reopened_raw, reopened_paper)
                    metadata_authentication_ns = time.perf_counter_ns() - startup_started
                    if (
                        raw_startup.historical_segments_read != 0
                        or paper_startup.segments_read != 0
                        or not paper_startup.checkpoint_used
                        or paper_startup.tail_entries_replayed != 0
                        or alignment.status is not Phase1CAuthorityStatus.ALIGNED
                        or alignment.binding != terminal_seal.binding
                    ):
                        raise _error(
                            OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                            "boundary reopen exceeded checkpoint plus empty-tail scope",
                        )
                    authenticated_raw_manifest = reopened_raw.manifest
                    if (
                        authenticated_raw_manifest is None
                        or authenticated_raw_manifest.root
                        != terminal_seal.binding.raw_manifest_root
                    ):
                        raise _error(
                            OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                            "boundary raw manifest differs from its Paper binding",
                        )
                    raw_embedded_index_bytes = tuple(
                        (
                            raw_paths.segment_path(descriptor.physical_sha256),
                            raw_footer_index_physical_bytes(descriptor.record_count),
                        )
                        for descriptor in authenticated_raw_manifest.segments
                    )

                    startup_trace_recorder.stop_observing()
                    audit_started = time.perf_counter_ns()

                    def emit_audit_progress(payload: Mapping[str, object]) -> None:
                        boundary_payload: dict[str, object] = {
                            "boundary_commit_count": manifest.commit_count,
                        }
                        boundary_payload.update(payload)
                        emit_snapshot(boundary_payload)

                    audit_progress = BoundedAuditProgress(
                        phase="capacity_full_audit",
                        progress=emit_audit_progress,
                        totals={},
                    )
                    raw_audit = reopened_raw.full_audit(
                        progress=emit_audit_progress,
                    )
                    paper_audit = reopened_paper.full_audit(
                        progress=emit_audit_progress,
                    )
                    resolver = DiskRawResolver(reopened_raw)
                    native_audit = audit_native_frames(
                        reopened_paper.iter_historical_frames(),
                        resolver,
                        terminal_seal.expectations,
                        progress=emit_audit_progress,
                    )
                    oracle = compare_capacity_native_exact(
                        reopened_paper,
                        resolver,
                        manifest,
                        run_id=paper_config.run_id,
                        progress=emit_audit_progress,
                    )
                    full_history_audit_ns = time.perf_counter_ns() - audit_started
                    if (
                        raw_audit.records_read != terminal_seal.binding.raw_record_count
                        or paper_audit.commits_read
                        != terminal_seal.expectations.commit_count
                        or native_audit.raw_reference_prefix_root
                        != terminal_seal.binding.raw_reference_prefix_root
                        or oracle.commit_count != manifest.commit_count
                        or oracle.logical_row_count != manifest.logical_row_count
                        or oracle.workload_sha256 != manifest.workload_sha256
                        or oracle.final_prefix_root
                        != terminal_seal.expectations.final_prefix_root.hex()
                        or oracle.market_gap_count
                        != terminal_seal.expectations.market_gap_count
                    ):
                        raise _error(
                            OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                            "boundary raw, Paper, native, or workload audit diverged",
                        )
                    audit_progress.complete(
                        {},
                        extra={"full_history_audit_ns": full_history_audit_ns},
                    )
                    certification = Phase1CCertificationReport(
                        raw_startup=raw_startup,
                        paper_startup=paper_startup,
                        alignment=alignment,
                        raw_audit=raw_audit,
                        paper_audit=paper_audit,
                        native_audit=native_audit,
                        raw_resolver_physical_hash_passes=resolver.physical_hash_passes,
                    )
                finally:
                    reopened_paper.close()
            finally:
                reopened_raw.close()
        startup_file_trace = startup_trace_recorder.result

        try:
            post_audit_candidate_tree = witness_candidate_tree(
                root,
                progress=emit_snapshot,
            )
        except CandidateTreeWitnessError as error:
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                f"candidate tree could not be rebound after boundary audit: {error}",
            ) from error
        if post_audit_candidate_tree != audited_candidate_tree:
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "candidate tree changed across the read-only boundary audit",
            )

        observer.sample_resources()
        byte_census = census_byte_categories(
            _capacity_paths(
                staging=staging,
                raw_anchor=raw_anchor,
                paper_anchor=paper_anchor,
                raw_paths=raw_paths,
                paper_paths=paper_paths,
                raw_embedded_index_bytes=raw_embedded_index_bytes,
            ),
            scratch_peak_bytes=observer.scratch_peak_bytes,
            candidate_root=root,
        )
        measurement = CapacityMeasurement(
            workload_manifest_sha256=manifest.sha256,
            observed_workload_sha256=oracle.workload_sha256,
            commit_count=manifest.commit_count,
            logical_row_count=manifest.logical_row_count,
            wall_ns=workload_wall_ns,
            cpu_ns=workload_cpu_ns,
            peak_rss_bytes=observer.peak_rss_bytes,
            byte_census=byte_census,
            segment_count=raw_audit.segments_read + paper_audit.segments_read,
            checkpoint_count=paper_audit.checkpoints_read,
            manifest_count=raw_audit.manifests_read + paper_audit.manifests_read,
            startup_ns=startup_ns,
            startup_historical_segments_read=(
                raw_startup.historical_segments_read + paper_startup.segments_read
            ),
            startup_historical_commits_replayed=0,
            startup_tail_entries_replayed=paper_startup.tail_entries_replayed,
            metadata_authentication_ns=metadata_authentication_ns,
            full_history_audit_ns=full_history_audit_ns,
            seal_durations=DurationObservations(tuple(seal_durations)),
            checkpoint_durations=DurationObservations(
                tuple(observer.checkpoint_publications_ns)
            ),
            manifest_publish_durations=DurationObservations(
                tuple(observer.manifest_publications_ns)
            ),
            logical_span_ns=(
                None
                if manifest.config.cadence_ns is None or manifest.commit_count == 1
                else manifest.config.cadence_ns * (manifest.commit_count - 1)
            ),
            raw_input_bytes=raw_audit.logical_payload_bytes,
            cumulative_bytes_written=cumulative_bytes_written,
        )
        evidence = OfflineCapacityRunEvidence(
            candidate_root=root,
            audited_candidate_tree=audited_candidate_tree,
            raw_store_id=raw_config.store_id.value,
            raw_lake_id=raw_config.lake_id.value,
            paper_store_id=paper_config.store_id.value,
            run_id=paper_config.run_id.value,
            config_identity=raw_config.config_identity.hex(),
            code_identity=paper_config.code_identity.digest.hex(),
            runtime_identity=paper_config.runtime_identity.digest.hex(),
            certification=certification,
            startup_file_trace=startup_file_trace,
            oracle=oracle,
            batch_count=batch_count,
            seal_count=paper_audit.checkpoints_read,
            max_batch_commits_observed=max_batch_commits_observed,
            wall_scope=(
                "cumulative active generation, ingestion, and seal time through this "
                "boundary; intermediate read-only certification time excluded"
                + ("; pre-resume process time unavailable" if resumed else "")
            ),
            metadata_scope=(
                "metadata authentication includes raw/Paper boundary reopen and "
                "authority alignment; full-history audit excluded"
            ),
            scratch_scope=(
                "exact maximum bytes of recognized .tmp/-journal/-wal/-shm files at "
                "instrumented transient growth boundaries"
            ),
            scratch_status="EXACT_RECOGNIZED_TRANSIENT_FILE_PEAK_AT_INSTRUMENTED_BOUNDARIES",
            rss_scope=(
                "maximum configured RSS-probe observation; default is the operating-system "
                "process-lifetime peak"
            ),
            checkpoint_scope=(
                "one durable Paper checkpoint per cumulative input batch"
            ),
            manifest_scope="raw and Paper immutable manifest publication boundaries",
            storage_rate_scope=(
                "elapsed first-to-last logical timestamp; unavailable for one commit"
            ),
        )
        return measurement, evidence

    def run_cumulative_capacity_workload(
        self,
        *,
        manifests: tuple[CapacityWorkloadManifest, ...],
        commits: Iterable[SyntheticCapacityCommit],
    ) -> CumulativeCapacityRunResult:
        """Ingest one terminal stream and certify every exact prefix boundary."""

        if type(manifests) is not tuple:
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "cumulative manifests must be an exact tuple",
            )
        if not isinstance(commits, Iterable):
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "terminal cumulative commits must be iterable",
            )
        return self._run_cumulative_capacity_workload(
            manifests=manifests,
            commits=commits,
            resume_existing=False,
            commit_factory=None,
        )

    def resume_cumulative_capacity_workload(
        self,
        *,
        manifests: tuple[CapacityWorkloadManifest, ...],
        commit_factory: Callable[
            [CapacityWorkloadManifest, int, Mapping[str, int]],
            Iterable[SyntheticCapacityCommit],
        ]
        | None = None,
    ) -> CumulativeCapacityRunResult:
        """Resume the same logical run from its authenticated Paper boundary."""

        if type(manifests) is not tuple:
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "cumulative manifests must be an exact tuple",
            )
        if commit_factory is not None and not callable(commit_factory):
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "resume commit_factory must be callable or None",
            )
        return self._run_cumulative_capacity_workload(
            manifests=manifests,
            commits=None,
            resume_existing=True,
            commit_factory=commit_factory,
        )

    def _run_cumulative_capacity_workload(
        self,
        *,
        manifests: tuple[CapacityWorkloadManifest, ...],
        commits: Iterable[SyntheticCapacityCommit] | None,
        resume_existing: bool,
        commit_factory: Callable[
            [CapacityWorkloadManifest, int, Mapping[str, int]],
            Iterable[SyntheticCapacityCommit],
        ]
        | None,
    ) -> CumulativeCapacityRunResult:
        terminal_manifest = _validate_cumulative_manifests(manifests)
        if self._checkpoint_every_batches != 1:
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "cumulative capacity requires one checkpoint per input batch",
            )
        root = self._candidate_root
        certificate_directory = root.parent / f".{root.name}.phase1c-boundaries"
        if resume_existing:
            _require_existing_absolute_root(root)
            if (
                _is_link_or_reparse_point(certificate_directory)
                or not certificate_directory.is_dir()
            ):
                raise _error(
                    OfflineCapacityRunnerErrorCode.CANDIDATE_MISSING,
                    "resume boundary certificate directory is missing or unsafe",
                )
        else:
            _require_fresh_absolute_root(root)
            if certificate_directory.exists() or _is_link_or_reparse_point(
                certificate_directory
            ):
                raise _error(
                    OfflineCapacityRunnerErrorCode.CANDIDATE_EXISTS,
                    "boundary certificate directory already exists or is unsafe",
                )
        self._last_evidence = None
        self._last_cumulative_result = None

        raw_config, paper_config = self._configs(terminal_manifest)
        anchors = root / "anchors"
        staging = root / "staging"
        raw_root = root / "raw"
        paper_root = root / "paper"
        raw_paths = RawStorePaths.from_root(raw_root)
        paper_paths = RepositoryPaths.from_root(paper_root)
        write_start = (
            None
            if self._write_bytes_probe is None
            else _validate_optional_counter(
                self._write_bytes_probe(),
                label="cumulative write bytes",
            )
        )
        initial_workload_wall_started = time.perf_counter_ns()
        initial_workload_cpu_started = time.process_time_ns()

        if not resume_existing:
            root.mkdir()
            anchors.mkdir()
            staging.mkdir()
            certificate_directory.mkdir()
            fsync_directory(certificate_directory.parent)
            raw_anchor = LocalAnchor.create(
                anchors / "raw.sqlite3",
                store_id=raw_config.store_id,
            )
            paper_anchor = LocalAnchor.create(
                anchors / "paper.sqlite3",
                store_id=paper_config.store_id,
            )
        else:
            if not anchors.is_dir() or not staging.is_dir():
                raise _error(
                    OfflineCapacityRunnerErrorCode.CANDIDATE_MISSING,
                    "resume candidate lacks anchors or raw staging directory",
                )
            raw_anchor = LocalAnchor.open_existing(
                anchors / "raw.sqlite3",
                store_id=raw_config.store_id,
            )
            paper_anchor = LocalAnchor.open_existing(
                anchors / "paper.sqlite3",
                store_id=paper_config.store_id,
            )

        observer = _MeasurementObserver(
            candidate_root=root,
            rss_probe=self._rss_probe,
            checkpoint_publications_ns=[],
            manifest_publications_ns=[],
        )

        def observe(point: FaultPoint, /) -> None:
            observer(point)
            if self._fault_hook is not None:
                self._fault_hook(point)

        observer.sample_resources()
        raw: RawStore | None = None
        paper: StorageRepository | None = None
        writer: Phase1CWriter
        hasher: CapacityWorkloadHasher
        adapter: SyntheticCapacityPhase1CAdapter
        suffix_references: tuple[RawSegmentRef, ...]
        certificates: list[DurableCapacityBoundaryCertificate]
        prefix_commits_audited = 0
        raw_commits_reused = 0
        resume_count = 1 if resume_existing else 0
        batch_count = 0
        raw_segment_count = 0
        paper_segment_count = 0
        checkpoint_count = 0
        commits_completed = 0
        logical_rows_completed = 0
        max_batch_commits_observed = 0
        seal_durations: list[int] = []
        terminal_seal: Phase1CSealResult | None = None

        try:
            if not resume_existing:
                raw = RawStore.create(
                    raw_root,
                    anchor=raw_anchor,
                    config=raw_config,
                    fault_hook=observe,
                )
                paper = StorageRepository.create(
                    paper_root,
                    anchor=paper_anchor,
                    config=paper_config,
                    fault_hook=observe,
                )
                writer = Phase1CWriter(
                    raw_store=raw,
                    paper_repository=paper,
                    staging_directory=staging,
                    raw_thresholds=self._raw_thresholds,
                    fault_hook=observe,
                )
                hasher = CapacityWorkloadHasher()
                adapter = SyntheticCapacityPhase1CAdapter(
                    run_id=paper_config.run_id,
                    start_prefix_root=paper_config.start_prefix_root,
                    max_batch_commits=self._batch_size,
                )
                if commits is None:
                    raise AssertionError("fresh cumulative commits disappeared")
                commit_iterator = iter(commits)
                suffix_references = ()
                certificates = []
            else:
                raw = RawStore.open_existing(
                    raw_root,
                    anchor=raw_anchor,
                    config=raw_config,
                    fault_hook=observe,
                )
                paper = StorageRepository.open_existing(
                    paper_root,
                    anchor=paper_anchor,
                    config=paper_config,
                    fault_hook=observe,
                )
                checkpoint = paper.checkpoint
                paper_manifest = paper.manifest
                raw_manifest = raw.manifest
                if checkpoint is None or paper_manifest is None or raw_manifest is None:
                    raise _error(
                        OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                        "resume requires non-empty raw and Paper authorities",
                    )
                checkpoint_state, checkpoint_binding = unbind_native_checkpoint_state(
                    checkpoint.state
                )
                adapter, workload_prefix = (
                    SyntheticCapacityPhase1CAdapter.resume_from_checkpoint(
                        checkpoint_state,
                        expected_run_id=paper_config.run_id,
                        expected_start_prefix_root=paper_config.start_prefix_root,
                        max_batch_commits=self._batch_size,
                    )
                )
                resume = Phase1CWriter.resume_from_authenticated_checkpoint(
                    raw_store=raw,
                    paper_repository=paper,
                    staging_directory=staging,
                    source_prefix_root=adapter.source_prefix_root,
                    raw_thresholds=self._raw_thresholds,
                    fault_hook=observe,
                )
                if (
                    resume.checkpoint_state != checkpoint_state
                    or resume.binding != checkpoint_binding
                    or resume.boundary_audit.commit_count != workload_prefix.commit_count
                    or resume.binding.raw_record_count != workload_prefix.commit_count
                    or not 1
                    <= workload_prefix.commit_count
                    < terminal_manifest.commit_count
                ):
                    raise _error(
                        OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                        "resume checkpoint, adapter prefix, or raw binding diverged",
                    )
                writer = resume.writer
                hasher = CapacityWorkloadHasher.resume_from_prefix(workload_prefix)
                suffix_references = tuple(resume.suffix_references)
                prefix_commits_audited = resume.boundary_audit.commit_count
                commits_completed = workload_prefix.commit_count
                logical_rows_completed = workload_prefix.logical_row_count
                batch_count = len(paper_manifest.segments)
                paper_segment_count = len(paper_manifest.segments)
                checkpoint_count = paper_manifest.generation
                raw_segment_count = len(raw_manifest.segments)
                max_batch_commits_observed = min(
                    self._batch_size,
                    commits_completed,
                )
                certificates = list(
                    _load_boundary_certificates(
                        directory=certificate_directory,
                        candidate_root=root,
                        manifests=manifests,
                        terminal_manifest=terminal_manifest,
                        checkpoint_commit_count=commits_completed,
                    )
                )

                def default_commit_factory(
                    selected_manifest: CapacityWorkloadManifest,
                    start_sequence: int,
                    stream_sequences: Mapping[str, int],
                ) -> Iterable[SyntheticCapacityCommit]:
                    return iter_capacity_commits(
                        selected_manifest.config,
                        start_sequence=start_sequence,
                        initial_stream_sequences=stream_sequences,
                    )

                selected_factory = commit_factory or default_commit_factory
                resumed_commits = selected_factory(
                    terminal_manifest,
                    adapter.next_commit_sequence,
                    adapter.source_stream_sequences,
                )
                if not isinstance(resumed_commits, Iterable):
                    raise _error(
                        OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                        "resume commit_factory must return an iterable",
                    )
                commit_iterator = iter(resumed_commits)

            next_boundary_index = next(
                (
                    index
                    for index, boundary in enumerate(manifests)
                    if boundary.commit_count > commits_completed
                ),
                len(manifests),
            )
            if next_boundary_index == len(manifests):
                raise _error(
                    OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                    "cumulative authority is already at or beyond the terminal boundary",
                )
            pending: list[SyntheticCapacityCommit] = []
            suffix_index = 0
            boundaries: list[CumulativeCapacityBoundaryResult] = []
            active_wall_ns = 0
            active_cpu_ns = 0
            active_wall_started: int | None = (
                time.perf_counter_ns()
                if resume_existing
                else initial_workload_wall_started
            )
            active_cpu_started: int | None = (
                time.process_time_ns()
                if resume_existing
                else initial_workload_cpu_started
            )

            def current_workload_times() -> tuple[int, int]:
                wall = active_wall_ns
                cpu = active_cpu_ns
                if active_wall_started is not None and active_cpu_started is not None:
                    wall += time.perf_counter_ns() - active_wall_started
                    cpu += time.process_time_ns() - active_cpu_started
                return wall, cpu

            def observed_write_delta() -> int | None:
                if self._write_bytes_probe is None:
                    return None
                observed = _validate_optional_counter(
                    self._write_bytes_probe(),
                    label="cumulative write bytes",
                )
                if write_start is None or observed is None:
                    return None
                if observed < write_start:
                    raise _error(
                        OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
                        "cumulative write-byte probe regressed",
                    )
                return observed - write_start

            def emit_snapshot(payload: Mapping[str, object]) -> None:
                if self._progress is None:
                    return
                observer.sample_resources()
                wall, cpu = current_workload_times()
                snapshot: dict[str, object] = {
                    "workload": "SYNTHETIC_CAPACITY_V1",
                    "workload_profile": terminal_manifest.config.profile.value,
                    "workload_id": terminal_manifest.sha256,
                    "workload_manifest_sha256": terminal_manifest.sha256,
                    "workload_sha256": terminal_manifest.workload_sha256,
                    "commits_completed": commits_completed,
                    "commits_total": terminal_manifest.commit_count,
                    "logical_rows_completed": logical_rows_completed,
                    "logical_rows_total": terminal_manifest.logical_row_count,
                    "workload_elapsed_ns": wall,
                    "cpu_ns": cpu,
                    "cpu_status": "ACTIVE_CUMULATIVE_WORKLOAD_CPU",
                    "peak_rss_bytes": observer.peak_rss_bytes,
                    "bytes_written": observed_write_delta(),
                    "raw_segment_count": raw_segment_count,
                    "paper_segment_count": paper_segment_count,
                    "segment_count": raw_segment_count + paper_segment_count,
                    "checkpoint_count": checkpoint_count,
                    "batch_count": batch_count,
                    "prefix_commits_reingested": 0,
                    "raw_commits_reused": raw_commits_reused,
                }
                snapshot.update(payload)
                self._progress(snapshot)

            def close_authorities() -> None:
                nonlocal raw
                nonlocal paper
                try:
                    if paper is not None:
                        paper.close()
                finally:
                    paper = None
                    if raw is not None:
                        raw.close()
                    raw = None

            def restore_certified_boundary(
                expected_prefix: CapacityWorkloadDigest,
            ) -> None:
                nonlocal adapter
                nonlocal hasher
                nonlocal paper
                nonlocal prefix_commits_audited
                nonlocal raw
                nonlocal writer
                raw = RawStore.open_existing(
                    raw_root,
                    anchor=raw_anchor,
                    config=raw_config,
                    fault_hook=observe,
                )
                try:
                    paper = StorageRepository.open_existing(
                        paper_root,
                        anchor=paper_anchor,
                        config=paper_config,
                        fault_hook=observe,
                    )
                except BaseException:
                    raw.close()
                    raw = None
                    raise
                checkpoint = paper.checkpoint
                if checkpoint is None:
                    raise _error(
                        OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                        "certified boundary lost its Paper checkpoint",
                    )
                state, _ = unbind_native_checkpoint_state(checkpoint.state)
                restored_adapter, restored_prefix = (
                    SyntheticCapacityPhase1CAdapter.resume_from_checkpoint(
                        state,
                        expected_run_id=paper_config.run_id,
                        expected_start_prefix_root=paper_config.start_prefix_root,
                        max_batch_commits=self._batch_size,
                    )
                )
                restored = Phase1CWriter.resume_from_authenticated_checkpoint(
                    raw_store=raw,
                    paper_repository=paper,
                    staging_directory=staging,
                    source_prefix_root=restored_adapter.source_prefix_root,
                    raw_thresholds=self._raw_thresholds,
                    fault_hook=observe,
                )
                if (
                    restored_prefix != expected_prefix
                    or restored.suffix_references
                    or restored.boundary_audit.commit_count
                    != expected_prefix.commit_count
                ):
                    raise _error(
                        OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                        "certified boundary failed exact authenticated restore",
                    )
                prefix_commits_audited += restored.boundary_audit.commit_count
                adapter = restored_adapter
                hasher = CapacityWorkloadHasher.resume_from_prefix(restored_prefix)
                writer = restored.writer

            def certify_boundary() -> None:
                nonlocal active_cpu_ns
                nonlocal active_cpu_started
                nonlocal active_wall_ns
                nonlocal active_wall_started
                nonlocal next_boundary_index
                if terminal_seal is None:
                    raise _error(
                        OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                        "boundary lacks a terminal Phase 1C seal",
                    )
                boundary_manifest = manifests[next_boundary_index]
                workload_prefix = hasher.snapshot()
                if workload_prefix != boundary_manifest.digest:
                    raise _error(
                        OfflineCapacityRunnerErrorCode.WORKLOAD_DIVERGENCE,
                        "observed cumulative prefix differs from boundary manifest",
                    )
                if active_wall_started is None or active_cpu_started is None:
                    raise AssertionError("cumulative active timer is not running")
                active_wall_ns += time.perf_counter_ns() - active_wall_started
                active_cpu_ns += time.process_time_ns() - active_cpu_started
                active_wall_started = None
                active_cpu_started = None
                close_authorities()
                measurement, evidence = self._audit_cumulative_boundary(
                    manifest=boundary_manifest,
                    terminal_seal=terminal_seal,
                    raw_config=raw_config,
                    paper_config=paper_config,
                    root=root,
                    staging=staging,
                    raw_root=raw_root,
                    paper_root=paper_root,
                    raw_paths=raw_paths,
                    paper_paths=paper_paths,
                    raw_anchor=raw_anchor,
                    paper_anchor=paper_anchor,
                    observer=observer,
                    emit_snapshot=emit_snapshot,
                    workload_wall_ns=active_wall_ns,
                    workload_cpu_ns=active_cpu_ns,
                    cumulative_bytes_written=observed_write_delta(),
                    seal_durations=seal_durations,
                    batch_count=batch_count,
                    max_batch_commits_observed=max_batch_commits_observed,
                    resumed=resume_existing,
                )
                certificate = _publish_boundary_certificate(
                    directory=certificate_directory,
                    terminal_manifest=terminal_manifest,
                    boundary_manifest=boundary_manifest,
                    workload_prefix=workload_prefix,
                    measurement=measurement,
                    evidence=evidence,
                    terminal_seal=terminal_seal,
                    previous_sha256=(None if not certificates else certificates[-1].sha256),
                )
                certificates.append(certificate)
                boundaries.append(
                    CumulativeCapacityBoundaryResult(
                        manifest=boundary_manifest,
                        workload_prefix=workload_prefix,
                        measurement=measurement,
                        evidence=evidence,
                        raw_manifest_root=terminal_seal.binding.raw_manifest_root,
                        paper_manifest_root=(
                            terminal_seal.paper_seal.manifest.identity.root
                        ),
                        checkpoint_root=terminal_seal.paper_seal.checkpoint.root,
                        certificate=certificate,
                    )
                )
                self._last_evidence = evidence
                emit_snapshot(
                    {
                        "boundary_commit_count": boundary_manifest.commit_count,
                        "boundary_certificate_path": str(certificate.path),
                        "boundary_certificate_sha256": certificate.sha256,
                        "phase": "capacity_boundary_complete",
                        "status": "AUTHENTICATED_DURABLE_PREFIX",
                    }
                )
                next_boundary_index += 1
                if next_boundary_index < len(manifests):
                    restore_certified_boundary(workload_prefix)
                    active_wall_started = time.perf_counter_ns()
                    active_cpu_started = time.process_time_ns()

            def flush_batch() -> None:
                nonlocal batch_count
                nonlocal checkpoint_count
                nonlocal commits_completed
                nonlocal logical_rows_completed
                nonlocal max_batch_commits_observed
                nonlocal paper_segment_count
                nonlocal raw_commits_reused
                nonlocal raw_segment_count
                nonlocal suffix_index
                nonlocal terminal_seal
                if not pending:
                    return
                if raw is None or paper is None:
                    raise AssertionError("cumulative authorities are closed")
                batch = tuple(pending)
                pending.clear()
                max_batch_commits_observed = max(
                    max_batch_commits_observed,
                    len(batch),
                )
                phase1c_batch = adapter.build_phase1c_batch(batch)
                suffix_remaining = len(suffix_references) - suffix_index
                if suffix_remaining:
                    if len(batch) > suffix_remaining:
                        raise _error(
                            OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                            "one batch mixed presealed raw suffix and new raw ingestion",
                        )
                    references = tuple(
                        suffix_references[suffix_index : suffix_index + len(batch)]
                    )
                    batch_result = writer.append_presealed_batch(
                        phase1c_batch,
                        references,
                    )
                    suffix_index += len(batch)
                    raw_commits_reused += len(batch)
                else:
                    batch_result = writer.append_batch(phase1c_batch)
                    raw_segment_count += len(batch_result.raw_seals)
                commits_completed += len(batch)
                logical_rows_completed += sum(len(commit.rows) for commit in batch)
                batch_count += 1
                seal_started = time.perf_counter_ns()
                terminal_seal = writer.seal(
                    adapter.checkpoint_state(workload_prefix=hasher.snapshot())
                )
                seal_durations.append(time.perf_counter_ns() - seal_started)
                paper_segment_count += 1
                checkpoint_count += 1
                observer.sample_resources()
                emit_snapshot(
                    {
                        "phase": "capacity_ingest",
                        "seal_count": checkpoint_count,
                        "status": "RUNNING",
                    }
                )
                if (
                    next_boundary_index < len(manifests)
                    and commits_completed
                    == manifests[next_boundary_index].commit_count
                ):
                    certify_boundary()

            for commit in commit_iterator:
                if commits_completed + len(pending) >= terminal_manifest.commit_count:
                    raise _error(
                        OfflineCapacityRunnerErrorCode.WORKLOAD_DIVERGENCE,
                        "terminal cumulative iterable emitted extra commits",
                    )
                hasher.update(commit)
                pending.append(commit)
                boundary_remaining = (
                    manifests[next_boundary_index].commit_count - commits_completed
                )
                batch_limit = min(self._batch_size, boundary_remaining)
                suffix_remaining = len(suffix_references) - suffix_index
                if suffix_remaining:
                    batch_limit = min(batch_limit, suffix_remaining)
                if len(pending) == batch_limit:
                    flush_batch()
            flush_batch()

            if (
                commits_completed != terminal_manifest.commit_count
                or suffix_index != len(suffix_references)
                or hasher.finalize() != terminal_manifest.digest
                or next_boundary_index != len(manifests)
                or not boundaries
            ):
                raise _error(
                    OfflineCapacityRunnerErrorCode.WORKLOAD_DIVERGENCE,
                    "cumulative terminal stream or authenticated boundaries are incomplete",
                )
            terminal_evidence = boundaries[-1].evidence
            commits_ingested = terminal_evidence.certification.raw_audit.records_read
            final_raw_seals = terminal_evidence.certification.raw_audit.segments_read
            if (
                commits_ingested != terminal_manifest.commit_count
                or final_raw_seals != raw_segment_count
            ):
                raise _error(
                    OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                    "final raw audit differs from exact cumulative ingestion accounting",
                )
            accounting = CumulativeCapacityAccounting(
                commits_generated=terminal_manifest.commit_count,
                commits_ingested=commits_ingested,
                prefix_commits_reingested=0,
                prefix_commits_audited=prefix_commits_audited,
                suffix_commits_reconstructed=raw_commits_reused,
                raw_commits_reused=raw_commits_reused,
                raw_seal_count=final_raw_seals,
                resume_count=resume_count,
            )
            result = CumulativeCapacityRunResult(
                candidate_root=root,
                terminal_manifest=terminal_manifest,
                boundary_manifests=manifests,
                boundaries=tuple(certificates),
                typed_boundaries=tuple(boundaries),
                terminal_shared_candidate_tree=(
                    terminal_evidence.audited_candidate_tree
                ),
                accounting=accounting,
            )
            self._last_cumulative_result = result
            self._last_evidence = terminal_evidence
            emit_snapshot(
                {
                    **accounting.payload(),
                    "terminal_shared_candidate_tree_sha256": (
                        result.terminal_shared_candidate_tree.tree_sha256
                    ),
                    "phase": "capacity_complete",
                    "status": "STORAGE_V4_PHASE_1C_CUMULATIVE_CAPACITY_EXACT",
                }
            )
            return result
        finally:
            try:
                if paper is not None:
                    paper.close()
            finally:
                if raw is not None:
                    raw.close()

    def run_capacity_workload(
        self,
        *,
        manifest: CapacityWorkloadManifest,
        commits: Iterable[SyntheticCapacityCommit],
    ) -> CapacityMeasurement:
        if not isinstance(manifest, CapacityWorkloadManifest):
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "manifest must be CapacityWorkloadManifest",
            )
        if not isinstance(commits, Iterable):
            raise _error(
                OfflineCapacityRunnerErrorCode.TYPE_INVALID,
                "commits must be iterable",
            )
        _require_fresh_absolute_root(self._candidate_root)
        self._last_evidence = None
        self._last_cumulative_result = None

        raw_config, paper_config = self._configs(manifest)
        root = self._candidate_root
        anchors = root / "anchors"
        staging = root / "staging"
        raw_root = root / "raw"
        paper_root = root / "paper"
        raw_paths = RawStorePaths.from_root(raw_root)
        paper_paths = RepositoryPaths.from_root(paper_root)

        write_start = (
            None
            if self._write_bytes_probe is None
            else _validate_optional_counter(
                self._write_bytes_probe(),
                label="cumulative write bytes",
            )
        )
        wall_started = time.perf_counter_ns()
        cpu_started = time.process_time_ns()
        root.mkdir()
        anchors.mkdir()
        staging.mkdir()

        raw_anchor = LocalAnchor.create(anchors / "raw.sqlite3", store_id=raw_config.store_id)
        paper_anchor = LocalAnchor.create(
            anchors / "paper.sqlite3",
            store_id=paper_config.store_id,
        )
        observer = _MeasurementObserver(
            candidate_root=root,
            rss_probe=self._rss_probe,
            checkpoint_publications_ns=[],
            manifest_publications_ns=[],
        )
        observer.sample_resources()
        hasher = CapacityWorkloadHasher()
        adapter = SyntheticCapacityPhase1CAdapter(
            run_id=paper_config.run_id,
            start_prefix_root=paper_config.start_prefix_root,
            max_batch_commits=self._batch_size,
        )
        pending: list[SyntheticCapacityCommit] = []
        batch_count = 0
        batches_since_checkpoint = 0
        seal_durations: list[int] = []
        max_batch_commits_observed = 0
        commits_completed = 0
        logical_rows_completed = 0
        raw_segment_count = 0
        paper_segment_count = 0
        checkpoint_count = 0
        terminal_seal: Phase1CSealResult | None = None

        def emit_snapshot(payload: Mapping[str, object]) -> None:
            if self._progress is None:
                return
            observer.sample_resources()
            observed_write_bytes = (
                None
                if self._write_bytes_probe is None
                else _validate_optional_counter(
                    self._write_bytes_probe(),
                    label="cumulative write bytes",
                )
            )
            if (
                write_start is not None
                and observed_write_bytes is not None
                and observed_write_bytes < write_start
            ):
                raise _error(
                    OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
                    "cumulative write-byte probe regressed during progress",
                )
            elapsed_ns = time.perf_counter_ns() - wall_started
            snapshot: dict[str, object] = {
                "workload": "SYNTHETIC_CAPACITY_V1",
                "workload_profile": manifest.config.profile.value,
                "workload_id": manifest.sha256,
                "workload_manifest_sha256": manifest.sha256,
                "workload_sha256": manifest.workload_sha256,
                "commits_completed": commits_completed,
                "commits_total": manifest.commit_count,
                "logical_rows_completed": logical_rows_completed,
                "logical_rows_total": manifest.logical_row_count,
                "elapsed_ns": elapsed_ns,
                "workload_elapsed_ns": elapsed_ns,
                "cpu_ns": time.process_time_ns() - cpu_started,
                "cpu_status": "CURRENT_WORKER_PROCESS_CPU_SINCE_WORKLOAD_START",
                "peak_rss_bytes": observer.peak_rss_bytes,
                "peak_rss_status": (
                    "PROCESS_LIFETIME_HIGH_WATER_MARK"
                    if observer.peak_rss_bytes is not None
                    else "UNAVAILABLE_RSS_PROBE"
                ),
                "bytes_written": (
                    None
                    if write_start is None or observed_write_bytes is None
                    else observed_write_bytes - write_start
                ),
                "bytes_written_status": (
                    "PROCESS_SCOPED_CUMULATIVE_WRITE_TRANSFER_DELTA"
                    if write_start is not None and observed_write_bytes is not None
                    else "UNAVAILABLE_WRITE_BYTE_PROBE"
                ),
                "raw_segment_count": raw_segment_count,
                "paper_segment_count": paper_segment_count,
                "segment_count": raw_segment_count + paper_segment_count,
                "checkpoint_count": checkpoint_count,
                "segment_checkpoint_status": "EXACT_DURABLE_PUBLICATION_COUNTS",
                "progress_metrics_scope": (
                    "CURRENT_WORKER_PROCESS_SELF_OBSERVED_AT_COMPLETED_PROGRESS_BOUNDARY"
                ),
            }
            snapshot.update(payload)
            self._progress(snapshot)

        raw = RawStore.create(
            raw_root,
            anchor=raw_anchor,
            config=raw_config,
            fault_hook=observer,
        )
        try:
            paper = StorageRepository.create(
                paper_root,
                anchor=paper_anchor,
                config=paper_config,
                fault_hook=observer,
            )
            try:
                writer = Phase1CWriter(
                    raw_store=raw,
                    paper_repository=paper,
                    staging_directory=staging,
                    raw_thresholds=self._raw_thresholds,
                )

                def flush_batch() -> None:
                    nonlocal batch_count
                    nonlocal batches_since_checkpoint
                    nonlocal commits_completed
                    nonlocal logical_rows_completed
                    nonlocal max_batch_commits_observed
                    nonlocal checkpoint_count
                    nonlocal paper_segment_count
                    nonlocal raw_segment_count
                    nonlocal terminal_seal
                    if not pending:
                        return
                    batch = tuple(pending)
                    pending.clear()
                    max_batch_commits_observed = max(
                        max_batch_commits_observed,
                        len(batch),
                    )
                    batch_result = writer.append_batch(adapter.build_phase1c_batch(batch))
                    raw_segment_count += len(batch_result.raw_seals)
                    commits_completed += len(batch)
                    logical_rows_completed += sum(len(commit.rows) for commit in batch)
                    batch_count += 1
                    batches_since_checkpoint += 1
                    observer.sample_resources()
                    if batches_since_checkpoint == self._checkpoint_every_batches:
                        seal_started = time.perf_counter_ns()
                        terminal_seal = writer.seal(
                            adapter.checkpoint_state(workload_prefix=hasher.snapshot())
                        )
                        seal_durations.append(time.perf_counter_ns() - seal_started)
                        paper_segment_count += 1
                        checkpoint_count += 1
                        batches_since_checkpoint = 0
                        observer.sample_resources()
                    emit_snapshot(
                        {
                            "batch_count": batch_count,
                            "phase": "capacity_ingest",
                            "seal_count": len(seal_durations),
                            "status": "RUNNING",
                        }
                    )

                for commit in commits:
                    hasher.update(commit)
                    pending.append(commit)
                    if len(pending) == self._batch_size:
                        flush_batch()
                flush_batch()

                observed = hasher.finalize()
                if (
                    observed.commit_count != manifest.commit_count
                    or observed.logical_row_count != manifest.logical_row_count
                    or observed.sha256 != manifest.workload_sha256
                ):
                    raise _error(
                        OfflineCapacityRunnerErrorCode.WORKLOAD_DIVERGENCE,
                        "observed workload differs from its frozen manifest",
                    )
                if batches_since_checkpoint:
                    seal_started = time.perf_counter_ns()
                    terminal_seal = writer.seal(
                        adapter.checkpoint_state(workload_prefix=hasher.snapshot())
                    )
                    seal_durations.append(time.perf_counter_ns() - seal_started)
                    paper_segment_count += 1
                    checkpoint_count += 1
                    batches_since_checkpoint = 0
                    observer.sample_resources()
                if terminal_seal is None or batch_count < 1:
                    raise _error(
                        OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                        "capacity run did not publish a terminal checkpoint",
                    )
            finally:
                paper.close()
        finally:
            raw.close()

        observer.assert_balanced()
        observer.sample_resources()
        cpu_ns = time.process_time_ns() - cpu_started
        wall_ns = time.perf_counter_ns() - wall_started
        if wall_ns < 1:
            raise _error(
                OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
                "capacity wall duration was not positive",
            )
        write_end = (
            None
            if self._write_bytes_probe is None
            else _validate_optional_counter(
                self._write_bytes_probe(),
                label="cumulative write bytes",
            )
        )
        if write_start is None or write_end is None:
            cumulative_bytes_written = None
        elif write_end < write_start:
            raise _error(
                OfflineCapacityRunnerErrorCode.MEASUREMENT_INVALID,
                "cumulative write-byte probe regressed",
            )
        else:
            cumulative_bytes_written = write_end - write_start

        try:
            audited_candidate_tree = witness_candidate_tree(
                root,
                progress=emit_snapshot,
            )
        except CandidateTreeWitnessError as error:
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                f"candidate tree could not be bound before read-only audits: {error}",
            ) from error

        startup_trace_paths = StartupTracePaths(
            candidate_root=root,
            raw_root=raw_root,
            paper_root=paper_root,
            raw_anchor=raw_anchor.path,
            paper_anchor=paper_anchor.path,
            raw_anchor_writer_lease=raw_anchor.writer_lease_path,
            paper_anchor_writer_lease=paper_anchor.writer_lease_path,
            paper_writer_lease=paper_paths.writer_lease,
        )
        with trace_startup_file_access(startup_trace_paths) as startup_trace_recorder:
            startup_started = time.perf_counter_ns()
            reopened_raw = RawStore.open_existing(
                raw_root,
                anchor=raw_anchor,
                config=raw_config,
            )
            try:
                reopened_paper = StorageRepository.open_existing(
                    paper_root,
                    anchor=paper_anchor,
                    config=paper_config,
                )
                try:
                    startup_ns = time.perf_counter_ns() - startup_started
                    raw_startup = reopened_raw.startup_report
                    paper_startup = reopened_paper.startup_report
                    alignment = inspect_phase1c_alignment(reopened_raw, reopened_paper)
                    metadata_authentication_ns = time.perf_counter_ns() - startup_started
                    if (
                        raw_startup.historical_segments_read != 0
                        or paper_startup.segments_read != 0
                        or not paper_startup.checkpoint_used
                        or paper_startup.tail_entries_replayed != 0
                        or alignment.status is not Phase1CAuthorityStatus.ALIGNED
                        or alignment.binding != terminal_seal.binding
                    ):
                        raise _error(
                            OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                            "normal reopen exceeded checkpoint plus empty-tail scope",
                        )
                    authenticated_raw_manifest = reopened_raw.manifest
                    if (
                        authenticated_raw_manifest is None
                        or authenticated_raw_manifest.root
                        != terminal_seal.binding.raw_manifest_root
                    ):
                        raise _error(
                            OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                            "authenticated raw manifest differs from terminal binding",
                        )
                    raw_embedded_index_bytes = tuple(
                        (
                            raw_paths.segment_path(descriptor.physical_sha256),
                            raw_footer_index_physical_bytes(descriptor.record_count),
                        )
                        for descriptor in authenticated_raw_manifest.segments
                    )

                    startup_trace_recorder.stop_observing()
                    audit_started = time.perf_counter_ns()
                    audit_progress = BoundedAuditProgress(
                        phase="capacity_full_audit",
                        progress=emit_snapshot,
                        totals={},
                    )
                    raw_audit = reopened_raw.full_audit(progress=emit_snapshot)
                    paper_audit = reopened_paper.full_audit(progress=emit_snapshot)
                    resolver = DiskRawResolver(reopened_raw)
                    native_audit = audit_native_frames(
                        reopened_paper.iter_historical_frames(),
                        resolver,
                        terminal_seal.expectations,
                        progress=emit_snapshot,
                    )
                    oracle = compare_capacity_native_exact(
                        reopened_paper,
                        resolver,
                        manifest,
                        run_id=paper_config.run_id,
                        progress=emit_snapshot,
                    )
                    full_history_audit_ns = time.perf_counter_ns() - audit_started
                    if (
                        raw_audit.records_read != terminal_seal.binding.raw_record_count
                        or paper_audit.commits_read
                        != terminal_seal.expectations.commit_count
                        or native_audit.raw_reference_prefix_root
                        != terminal_seal.binding.raw_reference_prefix_root
                        or oracle.commit_count != manifest.commit_count
                        or oracle.logical_row_count != manifest.logical_row_count
                        or oracle.workload_sha256 != manifest.workload_sha256
                        or oracle.final_prefix_root
                        != terminal_seal.expectations.final_prefix_root.hex()
                        or oracle.market_gap_count
                        != terminal_seal.expectations.market_gap_count
                    ):
                        raise _error(
                            OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                            "exhaustive raw, Paper, native, or workload audit diverged",
                        )
                    audit_progress.complete(
                        {},
                        extra={"full_history_audit_ns": full_history_audit_ns},
                    )
                    certification = Phase1CCertificationReport(
                        raw_startup=raw_startup,
                        paper_startup=paper_startup,
                        alignment=alignment,
                        raw_audit=raw_audit,
                        paper_audit=paper_audit,
                        native_audit=native_audit,
                        raw_resolver_physical_hash_passes=(
                            resolver.physical_hash_passes
                        ),
                    )
                finally:
                    reopened_paper.close()
            finally:
                reopened_raw.close()
        startup_file_trace = startup_trace_recorder.result

        try:
            post_audit_candidate_tree = witness_candidate_tree(
                root,
                progress=emit_snapshot,
            )
        except CandidateTreeWitnessError as error:
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                f"candidate tree could not be rebound after read-only audits: {error}",
            ) from error
        if post_audit_candidate_tree != audited_candidate_tree:
            raise _error(
                OfflineCapacityRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "candidate tree changed across read-only audits",
            )

        observer.sample_resources()
        byte_census = census_byte_categories(
            _capacity_paths(
                staging=staging,
                raw_anchor=raw_anchor,
                paper_anchor=paper_anchor,
                raw_paths=raw_paths,
                paper_paths=paper_paths,
                raw_embedded_index_bytes=raw_embedded_index_bytes,
            ),
            scratch_peak_bytes=observer.scratch_peak_bytes,
            candidate_root=root,
        )
        measurement = CapacityMeasurement(
            workload_manifest_sha256=manifest.sha256,
            observed_workload_sha256=oracle.workload_sha256,
            commit_count=observed.commit_count,
            logical_row_count=observed.logical_row_count,
            wall_ns=wall_ns,
            cpu_ns=cpu_ns,
            peak_rss_bytes=observer.peak_rss_bytes,
            byte_census=byte_census,
            segment_count=raw_audit.segments_read + paper_audit.segments_read,
            checkpoint_count=paper_audit.checkpoints_read,
            manifest_count=raw_audit.manifests_read + paper_audit.manifests_read,
            startup_ns=startup_ns,
            startup_historical_segments_read=(
                raw_startup.historical_segments_read + paper_startup.segments_read
            ),
            startup_historical_commits_replayed=0,
            startup_tail_entries_replayed=paper_startup.tail_entries_replayed,
            metadata_authentication_ns=metadata_authentication_ns,
            full_history_audit_ns=full_history_audit_ns,
            seal_durations=DurationObservations(tuple(seal_durations)),
            checkpoint_durations=DurationObservations(
                tuple(observer.checkpoint_publications_ns)
            ),
            manifest_publish_durations=DurationObservations(
                tuple(observer.manifest_publications_ns)
            ),
            logical_span_ns=(
                None
                if manifest.config.cadence_ns is None or manifest.commit_count == 1
                else manifest.config.cadence_ns * (manifest.commit_count - 1)
            ),
            raw_input_bytes=raw_audit.logical_payload_bytes,
            cumulative_bytes_written=cumulative_bytes_written,
        )
        self._last_evidence = OfflineCapacityRunEvidence(
            candidate_root=root,
            audited_candidate_tree=audited_candidate_tree,
            raw_store_id=raw_config.store_id.value,
            raw_lake_id=raw_config.lake_id.value,
            paper_store_id=paper_config.store_id.value,
            run_id=paper_config.run_id.value,
            config_identity=raw_config.config_identity.hex(),
            code_identity=paper_config.code_identity.digest.hex(),
            runtime_identity=paper_config.runtime_identity.digest.hex(),
            certification=certification,
            startup_file_trace=startup_file_trace,
            oracle=oracle,
            batch_count=batch_count,
            seal_count=len(seal_durations),
            max_batch_commits_observed=max_batch_commits_observed,
            wall_scope=(
                "fresh candidate creation through terminal authority close; "
                "manifest precomputation and reopen audit excluded"
            ),
            metadata_scope=(
                "metadata authentication includes raw/Paper reopen and authority alignment; "
                "full-history audit excluded"
            ),
            scratch_scope=(
                "exact maximum bytes of recognized .tmp/-journal/-wal/-shm files at all "
                "instrumented transient growth boundaries; unrelated filesystem allocation "
                "and unrecognized temporary names are outside scope"
            ),
            scratch_status="EXACT_RECOGNIZED_TRANSIENT_FILE_PEAK_AT_INSTRUMENTED_BOUNDARIES",
            rss_scope=(
                "maximum configured RSS-probe observation; default is the operating-system "
                "process-lifetime peak"
            ),
            checkpoint_scope=(
                "durable Paper segment completion through durable checkpoint completion"
            ),
            manifest_scope=(
                "raw and Paper immutable manifest publication boundaries"
            ),
            storage_rate_scope=(
                "elapsed first-to-last logical timestamp; unavailable for one commit"
            ),
        )
        emit_snapshot(
            {
                "phase": "capacity_complete",
                "status": "STORAGE_V4_PHASE_1C_CAPACITY_LEVEL_EXACT",
            }
        )
        return measurement


__all__ = [
    "CumulativeCapacityAccounting",
    "CumulativeCapacityBoundaryResult",
    "CumulativeCapacityRunResult",
    "DurableCapacityBoundaryCertificate",
    "OfflineCapacityRunEvidence",
    "OfflineCapacityRunnerError",
    "OfflineCapacityRunnerErrorCode",
    "OfflinePhase1CCapacityRunner",
    "ProgressCallback",
    "RssProbe",
    "WriteBytesProbe",
    "current_process_cumulative_write_bytes",
]
