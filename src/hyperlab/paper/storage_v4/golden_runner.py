"""Offline production runner for a certified Golden V3 -> V4_NATIVE candidate.

The runner owns one fresh local candidate and never publishes a success marker.
It first builds raw-before-Paper native storage, then reopens and exhaustively
audits it, and finally invokes the independent thirteen-stream Golden oracle.
Evidence publication remains a separate semantic boundary.
"""

from __future__ import annotations

import hashlib
import importlib
import os
import platform
import stat
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hyperlab.paper.golden_v3 import GOLDEN_STREAM_NAMES, GoldenVerification

from .anchor import LocalAnchor
from .candidate_tree import (
    CandidateTreeWitness,
    CandidateTreeWitnessError,
    witness_candidate_tree,
)
from .canonical import canonical_json_bytes
from .capacity import ByteCategoryCensus, CapacityBytePaths, census_byte_categories
from .contracts import RawLakeId, StorageMode
from .faults import FaultPoint
from .golden_import import GoldenCommitAssembler
from .golden_native import (
    GOLDEN_NATIVE_INPUT_TYPE,
    GoldenNativeBatch,
    GoldenNativeDifferentialResult,
    GoldenNativeIngestResult,
    GoldenStreamFactory,
    compare_golden_native_exact,
    ingest_golden_native_batches,
    iter_golden_native_batches,
)
from .manifest import OpaqueIdentity
from .phase1c_pipeline import (
    Phase1CAuthorityStatus,
    Phase1CBatchResult,
    Phase1CCertificationReport,
    Phase1CSealResult,
    Phase1CWriter,
    certify_phase1c_reopen,
    inspect_phase1c_alignment,
)
from .raw_segment import raw_footer_index_physical_bytes
from .raw_store import DiskRawResolver, RawStore, RawStoreConfig, RawStorePaths
from .repository import RepositoryConfig, RepositoryPaths, StorageRepository
from .startup_trace import (
    StartupFileAccessTrace,
    StartupTracePaths,
    trace_startup_file_access,
)
from .types import Hash32, RunId, StoreId

GOLDEN_NATIVE_EXPECTED_COMMITS = 252_262
GOLDEN_NATIVE_EXPECTED_ROWS = 1_011_362
GOLDEN_NATIVE_EXPECTED_STREAMS = 13
GOLDEN_NATIVE_EXPECTED_MARKET_GAPS = 1
GOLDEN_NATIVE_STATUS = "STORAGE_V4_PHASE_1C_GOLDEN_NATIVE_EXACT"

ProgressCallback = Callable[[Mapping[str, object]], None]
AssemblerFactory = Callable[[GoldenVerification], GoldenCommitAssembler]
RssProbe = Callable[[], int | None]
WriteBytesProbe = Callable[[], int | None]


class GoldenNativeRunnerError(RuntimeError):
    """A fresh Golden native candidate is unsafe, divergent, or incomplete."""


def _validate_optional_counter(value: int | None, *, label: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < 0:
        raise GoldenNativeRunnerError(
            f"{label} probe must return a non-negative exact integer or None"
        )
    return value


def _sha256(value: bytes) -> Hash32:
    return Hash32(hashlib.sha256(value).digest())


def _identity_payload(
    verification: GoldenVerification,
    *,
    batch_size: int,
    code_identity: Hash32,
    runtime_identity: Hash32,
) -> bytes:
    return canonical_json_bytes(
        {
            "batch_size": batch_size,
            "code_identity": code_identity.hex(),
            "format": "hyperlab.storage_v4.phase1c.golden_native_config.v1",
            "golden_root": verification.root_hash,
            "run_id": verification.manifest.get("run_id"),
            "runtime_identity": runtime_identity.hex(),
        }
    )


def current_runtime_identity() -> Hash32:
    """Hash the runtime facts that can affect local storage serialization."""

    import sqlite3
    import zlib

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


def _process_peak_rss_bytes() -> int | None:
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        class _Counters(ctypes.Structure):
            _fields_ = (
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
            )

        counters = _Counters()
        counters.cb = ctypes.sizeof(counters)
        handle = ctypes.windll.kernel32.GetCurrentProcess()
        if not ctypes.windll.psapi.GetProcessMemoryInfo(
            handle,
            ctypes.byref(counters),
            counters.cb,
        ):
            return None
        return int(counters.PeakWorkingSetSize)
    try:
        resource = importlib.import_module("resource")
        observed = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except (AttributeError, ImportError, OSError, ValueError):
        return None
    return observed if sys.platform == "darwin" else observed * 1024


def _is_reparse(value: os.stat_result) -> bool:
    return bool(int(getattr(value, "st_file_attributes", 0)) & 0x400)


def _require_fresh_root(root: Path) -> None:
    if not isinstance(root, Path) or not root.is_absolute():
        raise GoldenNativeRunnerError("candidate root must be an absolute pathlib.Path")
    parent = root.parent
    if not parent.is_dir() or parent.is_symlink():
        raise GoldenNativeRunnerError("candidate parent is missing or linked")
    cursor = parent
    while True:
        observed = os.lstat(cursor)
        if stat.S_ISLNK(observed.st_mode) or _is_reparse(observed):
            raise GoldenNativeRunnerError("candidate ancestry contains a link or reparse point")
        if cursor.parent == cursor:
            break
        cursor = cursor.parent
    try:
        os.lstat(root)
    except FileNotFoundError:
        return
    raise GoldenNativeRunnerError("candidate root already exists")


def _transient_bytes(root: Path) -> int:
    total = 0
    if not root.exists():
        return 0
    for directory, names, files in os.walk(root, followlinks=False):
        base = Path(directory)
        for name in (*names, *files):
            entry = base / name
            observed = os.lstat(entry)
            if stat.S_ISLNK(observed.st_mode) or _is_reparse(observed):
                raise GoldenNativeRunnerError("candidate acquired a linked filesystem entry")
            if name.endswith((".tmp", "-journal", "-wal", "-shm")):
                total += int(observed.st_size)
    return total


@dataclass(slots=True)
class _Observer:
    root: Path
    rss_probe: RssProbe
    scratch_peak_bytes: int = 0
    peak_rss_bytes: int | None = None

    def sample(self) -> None:
        observed = self.rss_probe()
        if observed is not None:
            if type(observed) is not int or observed < 0:
                raise GoldenNativeRunnerError("RSS probe returned an invalid counter")
            self.peak_rss_bytes = (
                observed
                if self.peak_rss_bytes is None
                else max(self.peak_rss_bytes, observed)
            )
        self.scratch_peak_bytes = max(self.scratch_peak_bytes, _transient_bytes(self.root))

    def __call__(self, point: FaultPoint, /) -> None:
        if type(point) is not FaultPoint:
            raise TypeError("Golden observer requires FaultPoint")
        self.sample()


@dataclass(frozen=True, slots=True)
class GoldenNativeRunResult:
    candidate_root: Path
    audited_candidate_tree: CandidateTreeWitness
    raw_config: RawStoreConfig
    paper_config: RepositoryConfig
    ingestion: GoldenNativeIngestResult
    certification: Phase1CCertificationReport
    startup_file_trace: StartupFileAccessTrace
    differential: GoldenNativeDifferentialResult
    byte_census: ByteCategoryCensus
    wall_ns: int
    cpu_ns: int
    startup_and_audit_ns: int
    differential_ns: int
    peak_rss_bytes: int | None
    scratch_status: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.audited_candidate_tree, CandidateTreeWitness)
            or self.audited_candidate_tree.root != self.candidate_root
        ):
            raise ValueError("audited candidate tree must bind the exact Golden root")

    def payload(self) -> dict[str, object]:
        native = self.certification.native_audit
        raw_startup = self.certification.raw_startup
        paper_startup = self.certification.paper_startup
        raw_audit = self.certification.raw_audit
        paper_audit = self.certification.paper_audit
        return {
            "artifact": "STORAGE_V4_PHASE1C_GOLDEN_NATIVE_RUN_V1",
            "authority": {
                "config_identity": self.raw_config.config_identity.hex(),
                "paper_store_id": self.paper_config.store_id.value,
                "raw_lake_id": self.raw_config.lake_id.value,
                "raw_store_id": self.raw_config.store_id.value,
                "run_id": self.paper_config.run_id.value,
            },
            "bytes": self.byte_census.payload(),
            "candidate_root": str(self.candidate_root),
            "differential": dict(self.differential.report),
            "integrity": {
                "audited_candidate_tree": self.audited_candidate_tree.payload(),
                "commit_count": native.commit_count,
                "final_prefix_root": native.final_prefix_root.hex(),
                "market_gap_count": native.market_gap_count,
                "raw_reference_count": native.raw_reference_count,
                "raw_reference_prefix_root": native.raw_reference_prefix_root.hex(),
                "streams": [
                    {
                        "logical_sha256": stream.logical_sha256.hex(),
                        "row_count": stream.row_count,
                        "stream_id": stream.stream_id.value,
                    }
                    for stream in native.streams
                ],
            },
            "limitations": [
                "RAW_SOURCE_IS_CERTIFIED_CANONICAL_GOLDEN_INBOX_JSONL_NOT_ORIGINAL_WIRE",
                "PEAK_RSS_IS_PROCESS_LIFETIME_HIGH_WATER_MARK",
                "FULL_AUDIT_IS_OFFLINE_O_N",
                "RAW_CUMULATIVE_MANIFEST_CHAIN_AUTHENTICATION_IS_O_SEGMENTS_SQUARED",
            ],
            "markers": ["PAPER_ONLY", "TECHNICAL_STORAGE_REPLAY_EVIDENCE", "NOT_ECONOMIC_EVIDENCE"],
            "measurements": {
                "cpu_ns": self.cpu_ns,
                "differential_ns": self.differential_ns,
                "peak_rss_bytes": self.peak_rss_bytes,
                "scratch_peak_bytes": self.byte_census.scratch_peak_bytes,
                "scratch_status": self.scratch_status,
                "startup_and_audit_ns": self.startup_and_audit_ns,
                "wall_ns": self.wall_ns,
            },
            "paper_audit": {
                "checkpoints_read": paper_audit.checkpoints_read,
                "commits_read": paper_audit.commits_read,
                "manifests_read": paper_audit.manifests_read,
                "rows_read": paper_audit.rows_read,
                "segments_read": paper_audit.segments_read,
            },
            "raw_audit": {
                "logical_payload_bytes": raw_audit.logical_payload_bytes,
                "manifests_read": raw_audit.manifests_read,
                "physical_segment_bytes": raw_audit.physical_segment_bytes,
                "records_read": raw_audit.records_read,
                "segments_read": raw_audit.segments_read,
                "stored_payload_bytes": raw_audit.stored_payload_bytes,
            },
            "startup": {
                "file_access_trace": self.startup_file_trace.payload(),
                "paper_checkpoint_used": paper_startup.checkpoint_used,
                "paper_historical_commits_not_read": paper_startup.historical_commits_not_read,
                "paper_segments_read": paper_startup.segments_read,
                "paper_tail_entries_replayed": paper_startup.tail_entries_replayed,
                "raw_historical_segments_read": raw_startup.historical_segments_read,
                "raw_manifest_namespace_entries_scanned": (
                    raw_startup.manifest_namespace_entries_scanned
                ),
                "raw_manifests_opened": raw_startup.manifests_opened,
            },
            "status": GOLDEN_NATIVE_STATUS,
        }


class OfflineGoldenNativeRunner:
    """Create and certify one fresh offline Golden native candidate."""

    def __init__(
        self,
        *,
        candidate_root: Path,
        code_identity: Hash32,
        runtime_identity: Hash32 | None = None,
        batch_size: int = 12_000,
        expected_commits: int = GOLDEN_NATIVE_EXPECTED_COMMITS,
        expected_rows: int = GOLDEN_NATIVE_EXPECTED_ROWS,
        expected_streams: int = GOLDEN_NATIVE_EXPECTED_STREAMS,
        expected_market_gaps: int = GOLDEN_NATIVE_EXPECTED_MARKET_GAPS,
        progress: ProgressCallback | None = None,
        assembler_factory: AssemblerFactory = GoldenCommitAssembler.from_verification,
        stream_factory: GoldenStreamFactory | None = None,
        rss_probe: RssProbe = _process_peak_rss_bytes,
        write_bytes_probe: WriteBytesProbe | None = None,
    ) -> None:
        if type(code_identity) is not Hash32:
            raise TypeError("Golden runner code identity must be Hash32")
        if runtime_identity is not None and type(runtime_identity) is not Hash32:
            raise TypeError("Golden runner runtime identity must be Hash32 or None")
        for label, value in (
            ("batch_size", batch_size),
            ("expected_commits", expected_commits),
            ("expected_rows", expected_rows),
            ("expected_streams", expected_streams),
            ("expected_market_gaps", expected_market_gaps),
        ):
            if type(value) is not int or value < 1:
                raise ValueError(f"{label} must be a positive exact integer")
        if batch_size > 50_000:
            raise ValueError("Golden native batch_size exceeds the raw segment record bound")
        if progress is not None and not callable(progress):
            raise TypeError("progress must be callable or None")
        if not callable(assembler_factory) or not callable(rss_probe):
            raise TypeError("Golden runner factories and probes must be callable")
        if write_bytes_probe is not None and not callable(write_bytes_probe):
            raise TypeError("Golden runner write_bytes_probe must be callable or None")
        self._candidate_root = candidate_root
        self._code_identity = code_identity
        self._runtime_identity = runtime_identity or current_runtime_identity()
        self._batch_size = batch_size
        self._expected_commits = expected_commits
        self._expected_rows = expected_rows
        self._expected_streams = expected_streams
        self._expected_market_gaps = expected_market_gaps
        self._progress = progress
        self._assembler_factory = assembler_factory
        self._stream_factory = stream_factory
        self._rss_probe = rss_probe
        self._write_bytes_probe = write_bytes_probe

    def _emit(self, **payload: object) -> None:
        if self._progress is not None:
            self._progress(payload)

    def _configs(self, verification: GoldenVerification) -> tuple[RawStoreConfig, RepositoryConfig]:
        run_value = verification.manifest.get("run_id")
        if type(run_value) is not str or not run_value:
            raise GoldenNativeRunnerError("verified Golden manifest has no run_id")
        try:
            golden_root = Hash32.from_hex(verification.root_hash)
        except ValueError as error:
            raise GoldenNativeRunnerError("verified Golden root is invalid") from error
        config_identity = _sha256(
            _identity_payload(
                verification,
                batch_size=self._batch_size,
                code_identity=self._code_identity,
                runtime_identity=self._runtime_identity,
            )
        )
        suffix = verification.root_hash[:16]
        raw_config = RawStoreConfig(
            store_id=StoreId(f"GOLDEN_V3_PHASE1C/{suffix}/raw"),
            lake_id=RawLakeId(f"GOLDEN_V3_PHASE1C/{suffix}/canonical-inbox"),
            config_identity=config_identity,
        )
        paper_config = RepositoryConfig(
            store_id=StoreId(f"GOLDEN_V3_PHASE1C/{suffix}/paper"),
            run_id=RunId(run_value),
            mode=StorageMode.V4_NATIVE,
            run_identity=OpaqueIdentity(_sha256(b"HL4-GOLDEN-RUN\x00" + run_value.encode())),
            config_identity=OpaqueIdentity(config_identity),
            code_identity=OpaqueIdentity(self._code_identity),
            runtime_identity=OpaqueIdentity(self._runtime_identity),
            start_prefix_root=golden_root,
        )
        return raw_config, paper_config

    def run(self, verification: GoldenVerification) -> GoldenNativeRunResult:
        if type(verification) is not GoldenVerification:
            raise TypeError("Golden native runner requires GoldenVerification")
        _require_fresh_root(self._candidate_root)
        raw_config, paper_config = self._configs(verification)
        root = self._candidate_root
        anchors = root / "anchors"
        staging = root / "staging"
        raw_root = root / "raw"
        paper_root = root / "paper"
        raw_paths = RawStorePaths.from_root(raw_root)
        paper_paths = RepositoryPaths.from_root(paper_root)
        observer = _Observer(root, self._rss_probe)

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
        paper_anchor = LocalAnchor.create(anchors / "paper.sqlite3", store_id=paper_config.store_id)
        observer.sample()
        commits_completed = 0
        logical_rows_completed = 0
        raw_segment_count = 0
        paper_segment_count = 0
        checkpoint_count = 0

        def emit_snapshot(payload: Mapping[str, object]) -> None:
            if self._progress is None:
                return
            observer.sample()
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
                raise GoldenNativeRunnerError(
                    "cumulative write-byte probe regressed during progress"
                )
            elapsed_ns = time.perf_counter_ns() - wall_started
            snapshot: dict[str, object] = {
                "workload": "GOLDEN_V3_NATIVE",
                "workload_profile": GOLDEN_NATIVE_INPUT_TYPE,
                "workload_id": verification.root_hash,
                "golden_root_hash": verification.root_hash,
                "commits_completed": commits_completed,
                "commits_total": self._expected_commits,
                "logical_rows_completed": logical_rows_completed,
                "logical_rows_total": self._expected_rows,
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
        raw = RawStore.create(raw_root, anchor=raw_anchor, config=raw_config, fault_hook=observer)
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
                )
                assembler = self._assembler_factory(verification)
                batches = iter_golden_native_batches(
                    assembler,
                    batch_size=self._batch_size,
                    expected_commit_count=self._expected_commits,
                )

                def observed_batch(
                    boundary: GoldenNativeBatch,
                    batch_result: Phase1CBatchResult,
                    seal_result: Phase1CSealResult,
                ) -> None:
                    nonlocal checkpoint_count
                    nonlocal commits_completed
                    nonlocal logical_rows_completed
                    nonlocal paper_segment_count
                    nonlocal raw_segment_count
                    if not batch_result.raw_seals:
                        raise GoldenNativeRunnerError(
                            "Golden native batch published no raw segment"
                        )
                    commits_completed = int(boundary.boundary_commit_sequence)
                    logical_rows_completed += sum(
                        len(frame.rows) for frame in batch_result.native_frames
                    )
                    raw_segment_count += len(batch_result.raw_seals)
                    paper_segment_count += 1
                    checkpoint_count += 1
                    if (
                        int(seal_result.paper_seal.checkpoint.covered_commit_sequence)
                        != commits_completed
                    ):
                        raise GoldenNativeRunnerError(
                            "Golden progress callback observed another checkpoint boundary"
                        )
                    emit_snapshot(
                        {"phase": "golden_native_ingest", "status": "RUNNING"}
                    )

                ingestion = ingest_golden_native_batches(
                    writer,
                    batches,
                    progress=observed_batch,
                )
            finally:
                paper.close()
        finally:
            raw.close()
        observer.sample()
        cpu_ns = time.process_time_ns() - cpu_started
        wall_ns = time.perf_counter_ns() - wall_started

        try:
            audited_candidate_tree = witness_candidate_tree(
                root,
                progress=emit_snapshot,
            )
        except CandidateTreeWitnessError as error:
            raise GoldenNativeRunnerError(
                f"Golden candidate could not be bound before read-only audits: {error}"
            ) from error

        audit_started = time.perf_counter_ns()
        emit_snapshot({"phase": "golden_native_full_audit", "status": "RUNNING"})
        certification = certify_phase1c_reopen(
            raw_root=raw_root,
            raw_anchor=raw_anchor,
            raw_config=raw_config,
            paper_root=paper_root,
            paper_anchor=paper_anchor,
            paper_config=paper_config,
            binding=ingestion.terminal_seal_result.binding,
            expectations=ingestion.terminal_seal_result.expectations,
        )
        startup_and_audit_ns = time.perf_counter_ns() - audit_started
        differential_started = time.perf_counter_ns()
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
                    startup_alignment = inspect_phase1c_alignment(
                        reopened_raw,
                        reopened_paper,
                    )
                    if (
                        startup_alignment.status is not Phase1CAuthorityStatus.ALIGNED
                        or startup_alignment.binding
                        != ingestion.terminal_seal_result.binding
                    ):
                        raise GoldenNativeRunnerError(
                            "Golden native normal reopen authority is not aligned"
                        )
                    authenticated_raw_manifest = reopened_raw.manifest
                    if (
                        authenticated_raw_manifest is None
                        or authenticated_raw_manifest.root
                        != ingestion.terminal_seal_result.binding.raw_manifest_root
                    ):
                        raise GoldenNativeRunnerError(
                            "Golden native authenticated raw manifest differs"
                        )
                    raw_embedded_index_bytes = tuple(
                        (
                            raw_paths.segment_path(descriptor.physical_sha256),
                            raw_footer_index_physical_bytes(descriptor.record_count),
                        )
                        for descriptor in authenticated_raw_manifest.segments
                    )
                    startup_trace_recorder.stop_observing()
                    resolver = DiskRawResolver(reopened_raw)
                    compare_kwargs: dict[str, Any] = {}
                    if self._stream_factory is not None:
                        compare_kwargs["stream_factory"] = self._stream_factory
                    emit_snapshot(
                        {"phase": "golden_native_differential", "status": "RUNNING"}
                    )
                    differential = compare_golden_native_exact(
                        reopened_paper,
                        resolver,
                        verification,
                        ingestion,
                        **compare_kwargs,
                    )
                finally:
                    reopened_paper.close()
            finally:
                reopened_raw.close()
            differential_ns = time.perf_counter_ns() - differential_started
        startup_file_trace = startup_trace_recorder.result

        try:
            post_audit_candidate_tree = witness_candidate_tree(
                root,
                progress=emit_snapshot,
            )
        except CandidateTreeWitnessError as error:
            raise GoldenNativeRunnerError(
                f"Golden candidate could not be rebound after read-only audits: {error}"
            ) from error
        if post_audit_candidate_tree != audited_candidate_tree:
            raise GoldenNativeRunnerError(
                "Golden candidate tree changed across read-only audits and differential"
            )
        observer.sample()

        native = certification.native_audit
        report = differential.report
        manifest_streams = verification.manifest.get("streams")
        report_streams = report.get("streams")
        if type(manifest_streams) is not dict or type(report_streams) is not dict:
            raise GoldenNativeRunnerError("Golden native stream census is missing")
        nonempty_streams = {
            name
            for name, descriptor in manifest_streams.items()
            if type(descriptor) is dict and descriptor.get("row_count") != 0
        }
        if (
            ingestion.commit_count != self._expected_commits
            or native.commit_count != self._expected_commits
            or sum(stream.row_count for stream in native.streams) != self._expected_rows
            or native.market_gap_count != self._expected_market_gaps
            or report.get("commits") != self._expected_commits
            or report.get("rows") != self._expected_rows
            or report.get("market_gap_rows") != self._expected_market_gaps
            or report.get("checkpoint_states_verified") != len(ingestion.checkpoint_witnesses)
            or len(report_streams) != self._expected_streams
            or set(report_streams) != set(GOLDEN_STREAM_NAMES)
            or {stream.stream_id.value for stream in native.streams} != nonempty_streams
        ):
            raise GoldenNativeRunnerError("Golden native production cardinality gates differ")
        if (
            certification.raw_startup.historical_segments_read != 0
            or certification.raw_startup.manifest_namespace_entries_scanned != 0
            or certification.paper_startup.segments_read != 0
            or certification.paper_startup.tail_entries_replayed != 0
            or not certification.paper_startup.checkpoint_used
        ):
            raise GoldenNativeRunnerError("Golden native startup exceeded checkpoint scope")

        census = census_byte_categories(
            CapacityBytePaths(
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
            ),
            scratch_peak_bytes=observer.scratch_peak_bytes,
            candidate_root=root,
        )
        emit_snapshot(
            {"phase": "golden_native_complete", "status": GOLDEN_NATIVE_STATUS}
        )
        return GoldenNativeRunResult(
            candidate_root=root,
            audited_candidate_tree=audited_candidate_tree,
            raw_config=raw_config,
            paper_config=paper_config,
            ingestion=ingestion,
            certification=certification,
            startup_file_trace=startup_file_trace,
            differential=differential,
            byte_census=census,
            wall_ns=wall_ns,
            cpu_ns=cpu_ns,
            startup_and_audit_ns=startup_and_audit_ns,
            differential_ns=differential_ns,
            peak_rss_bytes=observer.peak_rss_bytes,
            scratch_status=(
                "EXACT_RECOGNIZED_TRANSIENT_FILE_PEAK_AT_INSTRUMENTED_BOUNDARIES"
            ),
        )


__all__ = [
    "GOLDEN_NATIVE_EXPECTED_COMMITS",
    "GOLDEN_NATIVE_EXPECTED_MARKET_GAPS",
    "GOLDEN_NATIVE_EXPECTED_ROWS",
    "GOLDEN_NATIVE_EXPECTED_STREAMS",
    "GOLDEN_NATIVE_STATUS",
    "GoldenNativeRunResult",
    "GoldenNativeRunnerError",
    "OfflineGoldenNativeRunner",
    "current_runtime_identity",
]
