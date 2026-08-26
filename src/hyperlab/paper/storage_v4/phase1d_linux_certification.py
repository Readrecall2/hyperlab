"""Offline Linux/ext4 certification for the Storage V4 native Paper path.

Phase 1D deliberately reuses the already certified Phase 1C logical engine.  It
adds Linux mount, ownership, permission, atomic-rename, interprocess lease, and
real process-signal evidence on fresh isolated stores.  The corpus is bounded
and synthetic; it is filesystem/recovery evidence, never alpha or economic
evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import signal
import stat
import subprocess
import sys
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeVar

from .anchor import AnchorError, AnchorErrorCode, LocalAnchor
from .canonical import canonical_json_bytes
from .capacity import (
    CapacityProfile,
    CapacityTypeSpec,
    CapacityWorkloadConfig,
    CapacityWorkloadHasher,
    CapacityWorkloadManifest,
    SyntheticCapacityCommit,
    build_capacity_workload_manifest,
    iter_capacity_commits,
)
from .capacity_adapter import SyntheticCapacityPhase1CAdapter
from .capacity_oracle import compare_capacity_native_exact
from .checkpoint import CheckpointState
from .contracts import RawLakeId, StorageMode
from .durability import durable_publish_immutable, fsync_directory
from .faults import FaultHook, FaultPoint
from .linux_ext4 import (
    SecureTreeAttestation,
    attest_secure_tree,
    detect_mount,
    read_mountinfo,
    require_ext4_mount,
    secure_mkdir,
)
from .manifest import OpaqueIdentity
from .phase1c_pipeline import (
    Phase1CWriter,
    certify_phase1c_reopen,
)
from .raw_segment import (
    RawRecordMetadata,
    RawSegmentArtifact,
    RawSegmentThresholds,
    RawSegmentWriter,
)
from .raw_store import (
    DiskRawResolver,
    RawStore,
    RawStoreConfig,
    RawStoreError,
    RawStoreErrorCode,
)
from .repository import (
    RepositoryConfig,
    RepositoryError,
    RepositoryErrorCode,
    StorageRepository,
)
from .types import (
    CommitFrame,
    CommitOrdinal,
    CommitSequence,
    EventSequence,
    Hash32,
    LogicalRow,
    RunId,
    StoreId,
    StreamId,
)

PHASE1D_ARTIFACT = "STORAGE_V4_PHASE1D_LINUX_EXT4_OFFLINE_CERTIFICATION_V1"
PHASE1D_READY_VERDICT = "STORAGE_V4_PHASE_1D_READY_FOR_OFFLINE_LINUX_CERTIFICATION"
PHASE1D_CERTIFIED_VERDICT = "STORAGE_V4_PHASE_1D_LINUX_EXT4_CERTIFIED"
PHASE1D_SYNTHETIC_MARKERS = (
    "SYNTHETIC_FILESYSTEM_RECOVERY_CORPUS",
    "NOT_ECONOMIC_EVIDENCE",
    "NOT_ALPHA_EVIDENCE",
    "PAPER_ONLY",
    "NO_NETWORK_NO_ORDER_NO_SECRET",
)
PHASE1D_COMMIT_COUNT = 513
PHASE1D_TAIL_SIZES = (0, 1, 17, 64)
PHASE1D_BATCH_SIZE = 32
PHASE1D_WORKLOAD_SEED = 20260826
PHASE1D_PROGRESS_MIN_SECONDS = 30.0
_SOURCE_IDENTITY_DOMAIN = b"HL4-PHASE1D-SOURCE-IDENTITY-V1\x00"
_IDENTITY_DOMAIN = b"HL4-PHASE1D-IDENTITY-V1\x00"


class Phase1DCertificationError(RuntimeError):
    """A Phase 1D gate could not be established without weakening safety."""


def _sha(value: bytes) -> Hash32:
    return Hash32(hashlib.sha256(value).digest())


def _derived_hash(label: str) -> Hash32:
    return _sha(_IDENTITY_DOMAIN + label.encode("utf-8", errors="strict"))


def _require_source_commit(value: str) -> str:
    if (
        type(value) is not str
        or len(value) != 40
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("source commit must be one lowercase 40-character Git SHA")
    return value


def build_phase1d_workload_manifest(
    *,
    commit_count: int = PHASE1D_COMMIT_COUNT,
) -> CapacityWorkloadManifest:
    """Build the frozen bounded representative Phase 1D filesystem corpus."""

    if type(commit_count) is not int or commit_count < 129:
        raise ValueError("Phase 1D representative corpus requires at least 129 commits")
    config = CapacityWorkloadConfig(
        profile=CapacityProfile.GOLDEN_SHAPED,
        seed=PHASE1D_WORKLOAD_SEED,
        commit_count=commit_count,
        start_time_ns=1_787_680_000_000_000_000,
        cadence_ns=1_000_000_000,
        type_distribution=(
            CapacityTypeSpec(
                record_type="PUBLIC_MARKET_EVENT",
                stream="market_inputs",
                weight=7,
                payload_min_bytes=192,
                payload_max_bytes=768,
                payload_cardinality=commit_count,
            ),
            CapacityTypeSpec(
                record_type="PUBLIC_FUNDING_SETTLEMENT",
                stream="funding_inputs",
                weight=1,
                payload_min_bytes=128,
                payload_max_bytes=384,
                payload_cardinality=max(17, commit_count // 8),
            ),
        ),
        strategies=(
            "phase05_cash_and_carry",
            "phase08_robust_pairs",
        ),
        alert_every_commits=31,
        incident_every_commits=47,
        ledger_every_commits=19,
        projection_every_commits=13,
        market_gap_count=max(1, min(3, commit_count // 129)),
        alert_payload_bytes=192,
        incident_payload_bytes=256,
        ledger_payload_bytes=224,
        market_gap_payload_bytes=320,
        projection_payload_bytes=256,
        golden_census_sha256=hashlib.sha256(b"PHASE1D_BOUNDED_REPRESENTATIVE_NOT_GOLDEN_HISTORY").hexdigest(),
    )
    return build_capacity_workload_manifest(config)


@dataclass(frozen=True, slots=True)
class CrashCase:
    store_kind: str
    fault_point: FaultPoint
    signal_name: str
    case_id: str


def build_crash_plan() -> tuple[CrashCase, ...]:
    """Return the fixed real-process crash matrix for Linux certification."""

    paper_points = (
        FaultPoint.AFTER_OVERLAY_TRANSACTION,
        FaultPoint.AFTER_FILE_FSYNC,
        FaultPoint.AFTER_RENAME,
        FaultPoint.AFTER_DIRECTORY_FSYNC,
        FaultPoint.AFTER_SEGMENT_PUBLICATION,
        FaultPoint.AFTER_CHECKPOINT_PUBLICATION,
        FaultPoint.AFTER_MANIFEST_PUBLICATION,
        FaultPoint.AFTER_ANCHOR_PUBLICATION,
        FaultPoint.AFTER_CURRENT_PUBLICATION,
    )
    raw_points = (
        FaultPoint.AFTER_FILE_FSYNC,
        FaultPoint.AFTER_RENAME,
        FaultPoint.AFTER_DIRECTORY_FSYNC,
        FaultPoint.AFTER_RAW_SEGMENT_PUBLICATION,
        FaultPoint.AFTER_RAW_MANIFEST_PUBLICATION,
        FaultPoint.AFTER_RAW_ANCHOR_PUBLICATION,
        FaultPoint.AFTER_CURRENT_PUBLICATION,
    )
    cases: list[CrashCase] = []
    for signal_name in ("SIGTERM", "SIGKILL"):
        for store_kind, points in (("paper", paper_points), ("raw", raw_points)):
            for point in points:
                cases.append(
                    CrashCase(
                        store_kind=store_kind,
                        fault_point=point,
                        signal_name=signal_name,
                        case_id=f"{store_kind}-{point.value}-{signal_name.lower()}",
                    )
                )
    return tuple(cases)


def decide_phase1d_verdict(*, filesystem_type: str, gates_passed: bool) -> str:
    if type(gates_passed) is not bool:
        raise TypeError("gates_passed must be an exact bool")
    if not gates_passed:
        raise ValueError("Phase 1D gates did not pass")
    return PHASE1D_CERTIFIED_VERDICT if filesystem_type == "ext4" else PHASE1D_READY_VERDICT


@dataclass(frozen=True, slots=True)
class ResourceSnapshot:
    wall_ns: int
    cpu_ns: int
    peak_rss_bytes: int | None
    cumulative_write_bytes: int | None
    child_cpu_ns: int | None
    child_peak_rss_bytes: int | None
    self_output_blocks: int | None
    child_output_blocks: int | None

    def payload(self) -> dict[str, object]:
        return {
            "cpu_ns": self.cpu_ns,
            "cumulative_write_bytes": self.cumulative_write_bytes,
            "child_cpu_ns": self.child_cpu_ns,
            "child_output_blocks": self.child_output_blocks,
            "child_peak_rss_bytes": self.child_peak_rss_bytes,
            "peak_rss_bytes": self.peak_rss_bytes,
            "self_output_blocks": self.self_output_blocks,
            "wall_ns": self.wall_ns,
        }


def _proc_write_bytes() -> int | None:
    try:
        for line in Path("/proc/self/io").read_text(encoding="ascii").splitlines():
            key, separator, value = line.partition(":")
            if separator and key == "write_bytes":
                return int(value.strip())
    except (OSError, UnicodeError, ValueError):
        return None
    return None


def _linux_rusage() -> tuple[int, int, int, int, int] | None:
    if not sys.platform.startswith("linux"):
        return None
    try:
        import resource

        own = resource.getrusage(resource.RUSAGE_SELF)
        children = resource.getrusage(resource.RUSAGE_CHILDREN)
        child_cpu_ns = int((children.ru_utime + children.ru_stime) * 1_000_000_000)
        return (
            int(own.ru_maxrss) * 1024,
            child_cpu_ns,
            int(children.ru_maxrss) * 1024,
            int(own.ru_oublock),
            int(children.ru_oublock),
        )
    except (ImportError, OSError, ValueError):
        return None


def _resources() -> ResourceSnapshot:
    usage = _linux_rusage()
    return ResourceSnapshot(
        wall_ns=time.monotonic_ns(),
        cpu_ns=time.process_time_ns(),
        peak_rss_bytes=None if usage is None else usage[0],
        cumulative_write_bytes=_proc_write_bytes(),
        child_cpu_ns=None if usage is None else usage[1],
        child_peak_rss_bytes=None if usage is None else usage[2],
        self_output_blocks=None if usage is None else usage[3],
        child_output_blocks=None if usage is None else usage[4],
    )


def _delta(before: ResourceSnapshot, after: ResourceSnapshot) -> dict[str, object]:
    write_delta = (
        None
        if before.cumulative_write_bytes is None or after.cumulative_write_bytes is None
        else after.cumulative_write_bytes - before.cumulative_write_bytes
    )
    child_cpu_delta = (
        None
        if before.child_cpu_ns is None or after.child_cpu_ns is None
        else after.child_cpu_ns - before.child_cpu_ns
    )
    self_blocks_delta = (
        None
        if before.self_output_blocks is None or after.self_output_blocks is None
        else after.self_output_blocks - before.self_output_blocks
    )
    child_blocks_delta = (
        None
        if before.child_output_blocks is None or after.child_output_blocks is None
        else after.child_output_blocks - before.child_output_blocks
    )
    return {
        "child_cpu_ns": child_cpu_delta,
        "child_output_blocks": child_blocks_delta,
        "child_peak_rss_bytes": after.child_peak_rss_bytes,
        "cpu_ns": after.cpu_ns - before.cpu_ns,
        "peak_rss_bytes": after.peak_rss_bytes,
        "process_write_bytes": write_delta,
        "self_output_blocks": self_blocks_delta,
        "wall_ns": after.wall_ns - before.wall_ns,
    }


def _configs(label: str) -> tuple[RawStoreConfig, RepositoryConfig]:
    namespace = f"SYNTHETIC_STORAGE_V4_PHASE1D/{label}"
    config_identity = _derived_hash(f"{label}/config")
    run_id = RunId(f"{namespace}/run")
    return (
        RawStoreConfig(
            store_id=StoreId(f"{namespace}/raw"),
            lake_id=RawLakeId(f"{namespace}/lake"),
            config_identity=config_identity,
        ),
        RepositoryConfig(
            store_id=StoreId(f"{namespace}/paper"),
            run_id=run_id,
            mode=StorageMode.V4_NATIVE,
            run_identity=OpaqueIdentity(_derived_hash(f"{label}/run")),
            config_identity=OpaqueIdentity(config_identity),
            code_identity=OpaqueIdentity(_derived_hash(f"{label}/code")),
            runtime_identity=OpaqueIdentity(_derived_hash(f"{label}/runtime")),
            start_prefix_root=Hash32(b"\x00" * 32),
        ),
    )


def _raw_thresholds() -> RawSegmentThresholds:
    return RawSegmentThresholds(
        max_records=32,
        max_logical_payload_bytes=4 * 1024 * 1024,
        max_physical_bytes=8 * 1024 * 1024,
        max_single_payload_bytes=64 * 1024,
    )


def _private_owner(path: Path) -> tuple[int, int]:
    observed = os.lstat(path)
    return int(observed.st_uid), int(observed.st_gid)


def _publish_json(path: Path, payload: Mapping[str, object]) -> str:
    data = canonical_json_bytes(dict(payload)) + b"\n"
    durable_publish_immutable(path, data)
    return hashlib.sha256(data).hexdigest()


def _write_progress(path: Path, payload: Mapping[str, object]) -> None:
    line = canonical_json_bytes(dict(payload)) + b"\n"
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    flags |= int(getattr(os, "O_CLOEXEC", 0))
    descriptor = os.open(path, flags, 0o600)
    try:
        remaining = memoryview(line)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError("progress write made no progress")
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(path.parent)


def _source_identity(repository_root: Path) -> dict[str, object]:
    paths = tuple(
        sorted(
            {
                "AGENTS.md",
                "ops/storage_v4_phase1d/monitor_offline_certification.sh",
                "ops/storage_v4_phase1d/run_offline_certification.sh",
                "pyproject.toml",
                "requirements-runtime.lock",
                "scripts/certify_storage_v4_phase1d_linux.py",
                "tests/storage_v4/test_anchor.py",
                "tests/storage_v4/test_faults_durability.py",
                "tests/storage_v4/test_linux_ext4.py",
                "tests/storage_v4/test_phase1d_linux_certification.py",
                "tests/storage_v4/test_raw_store.py",
                "tests/storage_v4/test_repository.py",
                *(
                    path.relative_to(repository_root).as_posix()
                    for path in sorted(
                        (repository_root / "src" / "hyperlab" / "paper" / "storage_v4").glob("*.py")
                    )
                ),
            }
        )
    )
    files: list[dict[str, object]] = []
    digest = hashlib.sha256(_SOURCE_IDENTITY_DOMAIN)
    for relative in paths:
        path = repository_root / relative
        data = path.read_bytes()
        file_sha = hashlib.sha256(data).hexdigest()
        files.append({"path": relative, "sha256": file_sha, "size_bytes": len(data)})
        encoded = relative.encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
        digest.update(bytes.fromhex(file_sha))
    return {"files": files, "sha256": digest.hexdigest()}


@dataclass(frozen=True, slots=True)
class TailCaseReport:
    tail_entries: int
    checkpoint_commits: int
    startup_ns: int
    startup_tail_entries_replayed: int
    startup_historical_commits_not_read: int
    startup_segments_read: int
    raw_startup_historical_segments_read: int
    full_audit_commits: int
    full_audit_rows: int
    oracle_commits: int
    oracle_rows: int
    oracle_workload_sha256: str
    final_prefix_root: str
    resources: Mapping[str, object]
    tree: SecureTreeAttestation

    def payload(self) -> dict[str, object]:
        return {
            "checkpoint_commits": self.checkpoint_commits,
            "final_prefix_root": self.final_prefix_root,
            "full_audit": {
                "commits": self.full_audit_commits,
                "rows": self.full_audit_rows,
            },
            "independent_oracle": {
                "commits": self.oracle_commits,
                "logical_rows": self.oracle_rows,
                "workload_sha256": self.oracle_workload_sha256,
            },
            "raw_startup_historical_segments_read": self.raw_startup_historical_segments_read,
            "resources": dict(self.resources),
            "startup": {
                "historical_commits_not_read": self.startup_historical_commits_not_read,
                "segments_read": self.startup_segments_read,
                "tail_entries_replayed": self.startup_tail_entries_replayed,
                "wall_ns": self.startup_ns,
            },
            "tail_entries": self.tail_entries,
            "tree": self.tree.payload(),
        }


def _run_tail_case(
    case_root: Path,
    *,
    manifest: CapacityWorkloadManifest,
    tail_entries: int,
) -> TailCaseReport:
    before = _resources()
    secure_mkdir(case_root)
    anchors = secure_mkdir(case_root / "anchors")
    stores = secure_mkdir(case_root / "stores")
    staging = secure_mkdir(case_root / "staging")
    raw_config, paper_config = _configs(f"tail-{tail_entries}")
    raw_anchor = LocalAnchor.create(anchors / "raw-anchor.sqlite3", store_id=raw_config.store_id)
    paper_anchor = LocalAnchor.create(
        anchors / "paper-anchor.sqlite3",
        store_id=paper_config.store_id,
    )
    raw = RawStore.create(stores / "raw", anchor=raw_anchor, config=raw_config)
    paper = StorageRepository.create(stores / "paper", anchor=paper_anchor, config=paper_config)
    adapter = SyntheticCapacityPhase1CAdapter(
        run_id=paper_config.run_id,
        max_batch_commits=PHASE1D_BATCH_SIZE,
    )
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
        raw_thresholds=_raw_thresholds(),
    )
    checkpoint_count = manifest.commit_count - tail_entries
    pending: list[SyntheticCapacityCommit] = []
    hasher = CapacityWorkloadHasher()
    checkpoint = None
    for commit in iter_capacity_commits(manifest.config):
        hasher.update(commit)
        pending.append(commit)
        boundary = len(pending) == PHASE1D_BATCH_SIZE or commit.sequence in {
            checkpoint_count,
            manifest.commit_count,
        }
        if boundary:
            writer.append_batch(adapter.build_phase1c_batch(tuple(pending)))
            pending.clear()
        if commit.sequence == checkpoint_count:
            checkpoint = writer.seal(adapter.checkpoint_state(workload_prefix=hasher.snapshot()))
    observed = hasher.finalize()
    if (
        checkpoint is None
        or observed.sha256 != manifest.workload_sha256
        or observed.commit_count != manifest.commit_count
        or observed.logical_row_count != manifest.logical_row_count
    ):
        raise Phase1DCertificationError("bounded workload or checkpoint boundary diverged")
    terminal_expectations = writer.audit_expectations()
    paper.close()
    raw.close()

    startup_started = time.monotonic_ns()
    with (
        RawStore.open_existing(
            stores / "raw",
            anchor=raw_anchor,
            config=raw_config,
        ) as reopened_raw,
        StorageRepository.open_existing(
            stores / "paper",
            anchor=paper_anchor,
            config=paper_config,
        ) as reopened_paper,
    ):
        raw_startup = reopened_raw.startup_report
        paper_startup = reopened_paper.startup_report
    startup_ns = time.monotonic_ns() - startup_started
    certification = certify_phase1c_reopen(
        raw_root=stores / "raw",
        raw_anchor=raw_anchor,
        raw_config=raw_config,
        paper_root=stores / "paper",
        paper_anchor=paper_anchor,
        paper_config=paper_config,
        binding=checkpoint.binding,
        expectations=terminal_expectations,
        expected_tail_entries=tail_entries,
        checkpoint_expectations=checkpoint.expectations,
    )
    with (
        RawStore.open_existing(
            stores / "raw",
            anchor=raw_anchor,
            config=raw_config,
        ) as oracle_raw,
        StorageRepository.open_existing(
            stores / "paper",
            anchor=paper_anchor,
            config=paper_config,
        ) as oracle_paper,
    ):
        oracle = compare_capacity_native_exact(
            oracle_paper,
            DiskRawResolver(oracle_raw),
            manifest,
            run_id=paper_config.run_id,
            include_tail=tail_entries > 0,
        )
    if (
        paper_startup.tail_entries_replayed != tail_entries
        or paper_startup.segments_read != 0
        or paper_startup.historical_commits_not_read != checkpoint_count
        or raw_startup.historical_segments_read != 0
        or oracle.commit_count != manifest.commit_count
        or oracle.logical_row_count != manifest.logical_row_count
        or oracle.workload_sha256 != manifest.workload_sha256
    ):
        raise Phase1DCertificationError("checkpoint plus bounded tail or oracle diverged")
    owner_uid, owner_gid = _private_owner(case_root)
    tree = attest_secure_tree(
        case_root,
        expected_uid=owner_uid,
        expected_gid=owner_gid,
    )
    after = _resources()
    return TailCaseReport(
        tail_entries=tail_entries,
        checkpoint_commits=checkpoint_count,
        startup_ns=startup_ns,
        startup_tail_entries_replayed=paper_startup.tail_entries_replayed,
        startup_historical_commits_not_read=paper_startup.historical_commits_not_read,
        startup_segments_read=paper_startup.segments_read,
        raw_startup_historical_segments_read=raw_startup.historical_segments_read,
        full_audit_commits=(certification.paper_audit.commits_read + tail_entries),
        full_audit_rows=(
            certification.paper_audit.rows_read + certification.paper_startup.tail_rows_replayed
        ),
        oracle_commits=oracle.commit_count,
        oracle_rows=oracle.logical_row_count,
        oracle_workload_sha256=oracle.workload_sha256,
        final_prefix_root=oracle.final_prefix_root,
        resources=_delta(before, after),
        tree=tree,
    )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _single_frame(config: RepositoryConfig) -> CommitFrame:
    return CommitFrame(
        run_id=config.run_id,
        commit_sequence=CommitSequence(1),
        previous_prefix_root=config.start_prefix_root,
        rows=(
            LogicalRow(
                stream_id=StreamId("events"),
                ordinal=CommitOrdinal(0),
                value={
                    "contract": PHASE1D_ARTIFACT,
                    "paper_only": True,
                    "synthetic": True,
                },
            ),
        ),
    )


def _checkpoint_state() -> CheckpointState:
    return CheckpointState(
        adapter={"contract": PHASE1D_ARTIFACT},
        ledger={"paper_only": True},
        projection={"revision": 1},
        sessions={"closed": []},
        incidents={"open": []},
        cursors={"commit": 1},
        stream_heads={"events": 1},
    )


def _seal_single(repository: StorageRepository) -> None:
    repository.seal(
        checkpoint_state=_checkpoint_state(),
        cumulative_stream_counts=((StreamId("events"), 1),),
        historical_commit_count=1,
    )


def _build_raw_artifact(
    directory: Path,
    *,
    config: RawStoreConfig,
    fault_hook: FaultHook = None,
) -> RawSegmentArtifact:
    writer = RawSegmentWriter(
        directory,
        lake_id=config.lake_id,
        thresholds=_raw_thresholds(),
        fault_hook=fault_hook,
    )
    writer.append(
        b'{"paper_only":true,"synthetic":true}',
        RawRecordMetadata(
            record_id="phase1d-record-1",
            source_id="hyperlab.storage-v4.phase1d.synthetic",
            venue_id="SYNTHETIC",
            input_type="PUBLIC_MARKET_EVENT",
            source_stream_id=StreamId("wire"),
            source_first_sequence=EventSequence(1),
            source_last_sequence=EventSequence(1),
            arrival_sequence=EventSequence(1),
            source_timestamp="2026-08-26T00:00:00Z",
            received_timestamp="2026-08-26T00:00:00Z",
        ),
    )
    return writer.seal()


@dataclass(slots=True)
class _SignalFaultHook:
    point: FaultPoint
    signal_number: int
    seen: int = 0

    def __call__(self, observed: FaultPoint, /) -> None:
        if observed is not self.point:
            return None
        self.seen += 1
        os.kill(os.getpid(), self.signal_number)
        raise AssertionError("process survived its selected crash signal")


def _signal_number(name: str) -> int:
    if name == "SIGTERM":
        return int(signal.SIGTERM)
    if name == "SIGKILL" and hasattr(signal, "SIGKILL"):
        return int(signal.SIGKILL)
    raise Phase1DCertificationError(f"unsupported crash signal: {name}")


def _crash_worker(case_root: Path, case: CrashCase) -> int:
    """Create one fresh case and self-terminate at its selected real boundary."""

    secure_mkdir(case_root)
    anchors = secure_mkdir(case_root / "anchors")
    stores = secure_mkdir(case_root / "stores")
    staging = secure_mkdir(case_root / "staging")
    raw_config, paper_config = _configs(case.case_id)
    hook = _SignalFaultHook(case.fault_point, _signal_number(case.signal_name))
    if case.store_kind == "paper":
        anchor = LocalAnchor.create(
            anchors / "paper-anchor.sqlite3",
            store_id=paper_config.store_id,
        )
        repository = StorageRepository.create(
            stores / "paper",
            anchor=anchor,
            config=paper_config,
        )
        frame = _single_frame(paper_config)
        if case.fault_point is FaultPoint.AFTER_OVERLAY_TRANSACTION:
            repository.set_fault_hook(hook)
            repository.append(frame)
        else:
            repository.append(frame)
            repository.set_fault_hook(hook)
            _seal_single(repository)
    elif case.store_kind == "raw":
        anchor = LocalAnchor.create(
            anchors / "raw-anchor.sqlite3",
            store_id=raw_config.store_id,
        )
        store = RawStore.create(
            stores / "raw",
            anchor=anchor,
            config=raw_config,
        )
        if case.fault_point is FaultPoint.AFTER_FILE_FSYNC:
            _build_raw_artifact(
                staging,
                config=raw_config,
                fault_hook=hook,
            )
        else:
            artifact = _build_raw_artifact(staging, config=raw_config)
            store.set_fault_hook(hook)
            store.seal(artifact)
    else:
        raise Phase1DCertificationError(f"unknown crash store kind: {case.store_kind}")
    raise Phase1DCertificationError(f"selected crash boundary was not reached: {case.case_id}")


def _safe_cleanup_temporary_orphans(root: Path) -> tuple[dict[str, object], ...]:
    owner_uid, owner_gid = _private_owner(root)
    removed: list[dict[str, object]] = []
    for directory, names, filenames in os.walk(root, topdown=True, followlinks=False):
        names.sort()
        for name in sorted(filenames):
            if not (name.startswith(".") and name.endswith(".tmp")):
                continue
            path = Path(directory) / name
            observed = os.lstat(path)
            if (
                stat.S_ISLNK(observed.st_mode)
                or not stat.S_ISREG(observed.st_mode)
                or int(observed.st_nlink) != 1
                or int(observed.st_uid) != owner_uid
                or int(observed.st_gid) != owner_gid
                or stat.S_IMODE(observed.st_mode) != 0o600
            ):
                raise Phase1DCertificationError(f"unsafe temporary crash orphan cannot be cleaned: {path}")
            data = path.read_bytes()
            removed.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "size_bytes": len(data),
                }
            )
            path.unlink()
            fsync_directory(path.parent)
    return tuple(removed)


@dataclass(frozen=True, slots=True)
class CrashCaseReport:
    case: CrashCase
    returncode: int
    recovered_generation: int
    recovered_items: int
    first_startup: Mapping[str, object]
    repeated_startup: Mapping[str, object]
    removed_temporary_orphans: tuple[Mapping[str, object], ...]
    tree: SecureTreeAttestation
    resources: Mapping[str, object]

    def payload(self) -> dict[str, object]:
        return {
            "case_id": self.case.case_id,
            "fault_point": self.case.fault_point.value,
            "first_startup": dict(self.first_startup),
            "real_process_returncode": self.returncode,
            "recovered_generation": self.recovered_generation,
            "recovered_items": self.recovered_items,
            "removed_temporary_orphans": [dict(item) for item in self.removed_temporary_orphans],
            "repeated_startup": dict(self.repeated_startup),
            "resources": dict(self.resources),
            "signal": self.case.signal_name,
            "store_kind": self.case.store_kind,
            "tree": self.tree.payload(),
        }


def _paper_startup_payload(repository: StorageRepository) -> dict[str, object]:
    report = repository.startup_report
    return {
        "current_cache_status": report.current_cache_status.value,
        "historical_commits_not_read": report.historical_commits_not_read,
        "integrity_status": report.integrity_status.value,
        "manifest_generation": report.manifest_generation,
        "segments_read": report.segments_read,
        "tail_entries_replayed": report.tail_entries_replayed,
    }


def _raw_startup_payload(store: RawStore) -> dict[str, object]:
    report = store.startup_report
    return {
        "adopted_direct_successor": report.adopted_direct_successor,
        "current_status": report.current_status.value,
        "generation": report.generation,
        "historical_segments_read": report.historical_segments_read,
        "pending_status": report.pending_status.value,
    }


def _recover_crash_case(
    case_root: Path,
    *,
    case: CrashCase,
    repository_root: Path,
) -> CrashCaseReport:
    before = _resources()
    command = (
        sys.executable,
        "-m",
        "hyperlab.paper.storage_v4.phase1d_linux_certification",
        "--internal-crash-worker",
        "--case-root",
        str(case_root),
        "--case-id",
        case.case_id,
        "--store-kind",
        case.store_kind,
        "--fault-point",
        case.fault_point.value,
        "--signal-name",
        case.signal_name,
    )
    completed = subprocess.run(
        command,
        cwd=repository_root,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60.0,
        check=False,
    )
    expected_returncode = -_signal_number(case.signal_name)
    if completed.returncode != expected_returncode:
        raise Phase1DCertificationError(
            "real crash worker did not terminate at the expected signal boundary: "
            f"{case.case_id}; returncode={completed.returncode}; "
            f"stderr={completed.stderr[-2000:]}"
        )
    raw_config, paper_config = _configs(case.case_id)
    if case.store_kind == "paper":
        paper_anchor = LocalAnchor.open_existing(
            case_root / "anchors" / "paper-anchor.sqlite3",
            store_id=paper_config.store_id,
        )
        repository = StorageRepository.open_existing(
            case_root / "stores" / "paper",
            anchor=paper_anchor,
            config=paper_config,
        )
        first = _paper_startup_payload(repository)
        if repository.manifest is None:
            _seal_single(repository)
        frames = tuple(repository.iter_historical_frames())
        paper_audit = repository.full_audit()
        if len(frames) != 1 or frames[0] != _single_frame(paper_config) or paper_audit.commits_read != 1:
            raise Phase1DCertificationError(f"Paper recovery diverged after {case.case_id}")
        generation = repository.manifest.generation if repository.manifest is not None else 0
        repository.close()
        removed = _safe_cleanup_temporary_orphans(case_root)
        repeated_paper = StorageRepository.open_existing(
            case_root / "stores" / "paper",
            anchor=paper_anchor,
            config=paper_config,
        )
        repeated_payload = _paper_startup_payload(repeated_paper)
        if repeated_paper.full_audit().commits_read != 1:
            raise Phase1DCertificationError("repeated Paper recovery audit diverged")
        repeated_paper.close()
        recovered_items = 1
    else:
        raw_anchor = LocalAnchor.open_existing(
            case_root / "anchors" / "raw-anchor.sqlite3",
            store_id=raw_config.store_id,
        )
        store = RawStore.open_existing(
            case_root / "stores" / "raw",
            anchor=raw_anchor,
            config=raw_config,
        )
        first = _raw_startup_payload(store)
        if store.manifest is None:
            artifact = _build_raw_artifact(
                case_root / "staging",
                config=raw_config,
            )
            store.seal(artifact)
        raw_audit = store.full_audit()
        if raw_audit.records_read != 1 or store.manifest is None:
            raise Phase1DCertificationError(f"raw recovery diverged after {case.case_id}")
        generation = store.manifest.generation
        store.close()
        removed = _safe_cleanup_temporary_orphans(case_root)
        repeated_raw = RawStore.open_existing(
            case_root / "stores" / "raw",
            anchor=raw_anchor,
            config=raw_config,
        )
        repeated_payload = _raw_startup_payload(repeated_raw)
        if repeated_raw.full_audit().records_read != 1:
            raise Phase1DCertificationError("repeated raw recovery audit diverged")
        repeated_raw.close()
        recovered_items = 1
    owner_uid, owner_gid = _private_owner(case_root)
    tree = attest_secure_tree(
        case_root,
        expected_uid=owner_uid,
        expected_gid=owner_gid,
    )
    after = _resources()
    return CrashCaseReport(
        case=case,
        returncode=completed.returncode,
        recovered_generation=generation,
        recovered_items=recovered_items,
        first_startup=first,
        repeated_startup=repeated_payload,
        removed_temporary_orphans=removed,
        tree=tree,
        resources=_delta(before, after),
    )


def _lease_worker(
    *,
    case_root: Path,
    label: str,
    store_kind: str,
    anchor_path: Path,
) -> int:
    raw_config, paper_config = _configs(label)
    try:
        if store_kind.startswith("paper"):
            anchor = LocalAnchor.open_existing(
                anchor_path,
                store_id=paper_config.store_id,
            )
            repository = StorageRepository.open_existing(
                case_root / "stores" / "paper",
                anchor=anchor,
                config=paper_config,
            )
            repository.close()
        elif store_kind == "raw-anchor":
            anchor = LocalAnchor.open_existing(
                anchor_path,
                store_id=raw_config.store_id,
            )
            store = RawStore.open_existing(
                case_root / "stores" / "raw",
                anchor=anchor,
                config=raw_config,
            )
            store.close()
        else:
            raise Phase1DCertificationError(f"invalid lease worker kind: {store_kind}")
    except RepositoryError as error:
        if error.code is RepositoryErrorCode.WRITER_LEASE_HELD:
            print(
                canonical_json_bytes(
                    {"code": error.code.value, "status": "REFUSED", "store_kind": store_kind}
                ).decode("utf-8")
            )
            return 0
        raise
    except RawStoreError as error:
        if error.code is RawStoreErrorCode.WRITER_LEASE_HELD:
            print(
                canonical_json_bytes(
                    {"code": error.code.value, "status": "REFUSED", "store_kind": store_kind}
                ).decode("utf-8")
            )
            return 0
        raise
    print(
        canonical_json_bytes(
            {"code": "UNEXPECTEDLY_OPENED", "status": "FAILED", "store_kind": store_kind}
        ).decode("utf-8")
    )
    return 71


def _run_lease_probes(case_root: Path, *, repository_root: Path) -> dict[str, object]:
    secure_mkdir(case_root)
    anchors = secure_mkdir(case_root / "anchors")
    stores = secure_mkdir(case_root / "stores")
    label = "interprocess-leases"
    raw_config, paper_config = _configs(label)
    raw_anchor = LocalAnchor.create(
        anchors / "raw-anchor.sqlite3",
        store_id=raw_config.store_id,
    )
    paper_anchor = LocalAnchor.create(
        anchors / "paper-anchor.sqlite3",
        store_id=paper_config.store_id,
    )
    secondary_paper_anchor = LocalAnchor.create(
        anchors / "paper-secondary-anchor.sqlite3",
        store_id=paper_config.store_id,
    )
    raw = RawStore.create(stores / "raw", anchor=raw_anchor, config=raw_config)
    paper = StorageRepository.create(
        stores / "paper",
        anchor=paper_anchor,
        config=paper_config,
    )
    probes = (
        ("paper-anchor", paper_anchor.path),
        ("paper-store", secondary_paper_anchor.path),
        ("raw-anchor", raw_anchor.path),
    )
    reports: list[dict[str, object]] = []
    try:
        for store_kind, anchor_path in probes:
            completed = subprocess.run(
                (
                    sys.executable,
                    "-m",
                    "hyperlab.paper.storage_v4.phase1d_linux_certification",
                    "--internal-lease-worker",
                    "--case-root",
                    str(case_root),
                    "--case-id",
                    label,
                    "--store-kind",
                    store_kind,
                    "--anchor-path",
                    str(anchor_path),
                ),
                cwd=repository_root,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=30.0,
                check=False,
            )
            if completed.returncode != 0:
                raise Phase1DCertificationError(
                    f"interprocess lease probe failed for {store_kind}: {completed.stderr[-2000:]}"
                )
            payload = json.loads(completed.stdout)
            if payload.get("status") != "REFUSED":
                raise Phase1DCertificationError(f"interprocess lease was not refused for {store_kind}")
            reports.append(dict(payload))
    finally:
        paper.close()
        raw.close()
    owner_uid, owner_gid = _private_owner(case_root)
    tree = attest_secure_tree(
        case_root,
        expected_uid=owner_uid,
        expected_gid=owner_gid,
    )
    return {
        "paper_anchor_lease": reports[0],
        "paper_store_lease": reports[1],
        "raw_anchor_lease": reports[2],
        "tree": tree.payload(),
    }


def _expect_anchor_hardlink_refusal(anchor_path: Path, *, store_id: StoreId) -> str:
    sibling = anchor_path.parent / "anchor-hardlink-probe.sqlite3"
    os.link(anchor_path, sibling)
    try:
        try:
            LocalAnchor.open_existing(anchor_path, store_id=store_id)
        except AnchorError as error:
            if error.code is not AnchorErrorCode.CORRUPT:
                raise
            return error.code.value
        raise Phase1DCertificationError("hardlinked external anchor was accepted")
    finally:
        sibling.unlink(missing_ok=True)
        fsync_directory(sibling.parent)


def _run_security_probes(case_root: Path) -> dict[str, object]:
    secure_mkdir(case_root)
    anchors = secure_mkdir(case_root / "anchors")
    stores = secure_mkdir(case_root / "stores")
    staging = secure_mkdir(case_root / "staging")
    label = "security-probes"
    raw_config, paper_config = _configs(label)
    raw_anchor = LocalAnchor.create(
        anchors / "raw-anchor.sqlite3",
        store_id=raw_config.store_id,
    )
    paper_anchor = LocalAnchor.create(
        anchors / "paper-anchor.sqlite3",
        store_id=paper_config.store_id,
    )
    raw = RawStore.create(stores / "raw", anchor=raw_anchor, config=raw_config)
    raw.seal(_build_raw_artifact(staging, config=raw_config))
    raw.close()
    paper = StorageRepository.create(
        stores / "paper",
        anchor=paper_anchor,
        config=paper_config,
    )
    paper.append(_single_frame(paper_config))
    _seal_single(paper)
    paper.close()

    paper_current = stores / "paper" / "CURRENT"
    paper_current.unlink()
    fsync_directory(paper_current.parent)
    reopened_paper = StorageRepository.open_existing(
        stores / "paper",
        anchor=paper_anchor,
        config=paper_config,
    )
    paper_current_status = reopened_paper.startup_report.current_cache_status.value
    if paper_current_status != "CURRENT_ABSENT_REPAIRED":
        raise Phase1DCertificationError("Paper CURRENT was not repaired from authority")
    if reopened_paper.full_audit().commits_read != 1:
        raise Phase1DCertificationError("Paper CURRENT repair changed authoritative history")
    reopened_paper.close()

    raw_current = stores / "raw" / "CURRENT"
    raw_current.unlink()
    fsync_directory(raw_current.parent)
    reopened_raw = RawStore.open_existing(
        stores / "raw",
        anchor=raw_anchor,
        config=raw_config,
    )
    raw_current_status = reopened_raw.startup_report.current_status.value
    if raw_current_status != "RAW_CURRENT_ABSENT_REPAIRED":
        raise Phase1DCertificationError("raw CURRENT was not repaired from authority")
    if reopened_raw.full_audit().records_read != 1:
        raise Phase1DCertificationError("raw CURRENT repair changed authoritative history")
    reopened_raw.close()

    paper_link = stores / "paper-symlink-probe"
    paper_link.symlink_to(stores / "paper", target_is_directory=True)
    try:
        try:
            StorageRepository.open_existing(
                paper_link,
                anchor=paper_anchor,
                config=paper_config,
            )
        except RepositoryError as error:
            paper_symlink_code = error.code.value
        else:
            raise Phase1DCertificationError("symlinked Paper root was accepted")
    finally:
        paper_link.unlink(missing_ok=True)
        fsync_directory(paper_link.parent)

    anchor_hardlink_code = _expect_anchor_hardlink_refusal(
        paper_anchor.path,
        store_id=paper_config.store_id,
    )

    paper_segment = next((stores / "paper" / "segments").glob("*.hl4s"))
    paper_segment_probe = anchors / "paper-segment-hardlink-probe.hl4s"
    os.link(paper_segment, paper_segment_probe)
    paper_hardlink_code: str | None = None
    hardlinked_paper: StorageRepository | None = None
    try:
        hardlinked_paper = StorageRepository.open_existing(
            stores / "paper",
            anchor=paper_anchor,
            config=paper_config,
        )
        try:
            hardlinked_paper.full_audit()
        except RepositoryError as error:
            paper_hardlink_code = error.code.value
    finally:
        if hardlinked_paper is not None:
            hardlinked_paper.close()
        paper_segment_probe.unlink(missing_ok=True)
        fsync_directory(paper_segment_probe.parent)
    if paper_hardlink_code is None:
        raise Phase1DCertificationError("hardlinked Paper segment was accepted")

    raw_segment = next((stores / "raw" / "segments").glob("*.hl4r"))
    raw_segment_probe = anchors / "raw-segment-hardlink-probe.hl4r"
    os.link(raw_segment, raw_segment_probe)
    raw_hardlink_code: str | None = None
    hardlinked_raw: RawStore | None = None
    try:
        hardlinked_raw = RawStore.open_existing(
            stores / "raw",
            anchor=raw_anchor,
            config=raw_config,
        )
        try:
            hardlinked_raw.full_audit()
        except RawStoreError as error:
            raw_hardlink_code = error.code.value
    finally:
        if hardlinked_raw is not None:
            hardlinked_raw.close()
        raw_segment_probe.unlink(missing_ok=True)
        fsync_directory(raw_segment_probe.parent)
    if raw_hardlink_code is None:
        raise Phase1DCertificationError("hardlinked raw segment was accepted")

    with StorageRepository.open_existing(
        stores / "paper",
        anchor=paper_anchor,
        config=paper_config,
    ) as final_paper:
        if final_paper.full_audit().commits_read != 1:
            raise Phase1DCertificationError("Paper authority did not recover after probes")
    with RawStore.open_existing(
        stores / "raw",
        anchor=raw_anchor,
        config=raw_config,
    ) as final_raw:
        if final_raw.full_audit().records_read != 1:
            raise Phase1DCertificationError("raw authority did not recover after probes")

    owner_uid, owner_gid = _private_owner(case_root)
    tree = attest_secure_tree(
        case_root,
        expected_uid=owner_uid,
        expected_gid=owner_gid,
    )
    return {
        "anchor_external_to_paper_store": not paper_anchor.path.is_relative_to(stores / "paper"),
        "anchor_hardlink_refusal": anchor_hardlink_code,
        "paper_current_repair": paper_current_status,
        "paper_hardlink_refusal": paper_hardlink_code,
        "paper_symlink_refusal": paper_symlink_code,
        "raw_current_repair": raw_current_status,
        "raw_hardlink_refusal": raw_hardlink_code,
        "tree": tree.payload(),
    }


def _foreign_handles(candidate: Path) -> dict[str, object]:
    hits: list[dict[str, object]] = []
    unreadable = 0
    proc = Path("/proc")
    if not proc.is_dir():
        return {"available": False, "hits": hits, "unreadable_processes": unreadable}
    selected = candidate.absolute()
    for process in sorted(proc.iterdir(), key=lambda item: item.name):
        if not process.name.isdigit() or int(process.name) == os.getpid():
            continue
        fd_root = process / "fd"
        try:
            descriptors = tuple(fd_root.iterdir())
        except OSError:
            unreadable += 1
            continue
        for descriptor in descriptors:
            try:
                target = Path(os.readlink(descriptor))
            except OSError:
                continue
            try:
                target.absolute().relative_to(selected)
            except ValueError:
                continue
            hits.append({"fd": descriptor.name, "pid": int(process.name), "target": str(target)})
    return {"available": True, "hits": hits, "unreadable_processes": unreadable}


def _systemd_inspection(candidate: Path, explicit_services: Iterable[str]) -> dict[str, object]:
    services = {service for service in explicit_services if service}
    discovery_status = "AVAILABLE"
    try:
        listed = subprocess.run(
            (
                "systemctl",
                "list-units",
                "--type=service",
                "--all",
                "--no-legend",
                "--no-pager",
            ),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=15.0,
            check=False,
        )
        if listed.returncode == 0:
            for line in listed.stdout.splitlines():
                fields = line.split()
                if not fields:
                    continue
                name = fields[0]
                lowered = name.lower()
                if "hyperlab" in lowered or "paper" in lowered:
                    services.add(name)
        else:
            discovery_status = f"UNAVAILABLE_RETURN_{listed.returncode}"
    except (FileNotFoundError, subprocess.TimeoutExpired):
        discovery_status = "UNAVAILABLE"
    inspected: list[dict[str, object]] = []
    candidate_text = str(candidate.absolute())
    for service in sorted(services):
        completed = subprocess.run(
            (
                "systemctl",
                "show",
                service,
                "--no-pager",
                "--property=LoadState,ActiveState,SubState,FragmentPath,ExecStart,Environment,EnvironmentFiles,WorkingDirectory,RootDirectory",
            ),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=15.0,
            check=False,
        )
        mentions_candidate = candidate_text in completed.stdout
        inspected.append(
            {
                "mentions_candidate_path": mentions_candidate,
                "returncode": completed.returncode,
                "service": service,
                "show_sha256": hashlib.sha256(completed.stdout.encode("utf-8", errors="strict")).hexdigest(),
                "state_lines": [
                    line
                    for line in completed.stdout.splitlines()
                    if line.startswith(("LoadState=", "ActiveState=", "SubState="))
                ],
            }
        )
    return {
        "discovery_status": discovery_status,
        "inspected_services": inspected,
        "matching_service_count": len(inspected),
    }


def _verify_git_source(repository_root: Path, source_commit: str) -> dict[str, object]:
    def run(*arguments: str) -> str:
        completed = subprocess.run(
            ("git", *arguments),
            cwd=repository_root,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=20.0,
            check=False,
        )
        if completed.returncode != 0:
            raise Phase1DCertificationError(
                f"Git source verification failed: {' '.join(arguments)}: {completed.stderr}"
            )
        return completed.stdout.strip()

    observed = run("rev-parse", "HEAD")
    if observed != source_commit:
        raise Phase1DCertificationError(
            f"source HEAD {observed} differs from required commit {source_commit}"
        )
    tracked_status = run("status", "--porcelain", "--untracked-files=no")
    if tracked_status:
        raise Phase1DCertificationError("tracked source tree or index is not clean")
    return {"head": observed, "tracked_and_index_clean": True}


def _host_payload(*, uid: int, gid: int) -> dict[str, object]:
    umask = None
    try:
        for line in Path("/proc/self/status").read_text(encoding="ascii").splitlines():
            if line.startswith("Umask:"):
                umask = line.partition(":")[2].strip()
                break
    except (OSError, UnicodeError):
        pass
    return {
        "gid": gid,
        "kernel_release": platform.release(),
        "kernel_version": platform.version(),
        "machine": platform.machine(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "python_executable": sys.executable,
        "uid": uid,
        "umask_from_proc": umask,
    }


def run_phase1d_linux_certification(
    *,
    workspace: Path,
    repository_root: Path,
    source_commit: str,
    paper_services: Iterable[str] = (),
) -> dict[str, object]:
    """Run every bounded Phase 1D gate on one fresh offline ext4 workspace."""

    if not sys.platform.startswith("linux"):
        raise Phase1DCertificationError("Phase 1D execution requires a Linux kernel")
    source_commit = _require_source_commit(source_commit)
    selected_workspace = workspace.absolute()
    selected_repository = repository_root.absolute()
    if selected_workspace.exists() or selected_workspace.is_symlink():
        raise Phase1DCertificationError("certification workspace must be fresh and absent")
    if not selected_workspace.parent.is_dir():
        raise Phase1DCertificationError("certification workspace parent is missing")
    if selected_workspace.is_relative_to(selected_repository):
        raise Phase1DCertificationError("certification workspace must be outside the source tree")

    git_witness = _verify_git_source(selected_repository, source_commit)
    mountinfo_path = Path("/proc/self/mountinfo")
    mountinfo_bytes = mountinfo_path.read_bytes()
    entries = read_mountinfo(mountinfo_path)
    mount = require_ext4_mount(detect_mount(selected_workspace.parent, entries))
    handles = _foreign_handles(selected_workspace)
    if handles["hits"]:
        raise Phase1DCertificationError("another process already holds a handle into the candidate workspace")
    systemd = _systemd_inspection(selected_workspace, paper_services)
    inspected_services = systemd["inspected_services"]
    if not isinstance(inspected_services, list) or any(
        isinstance(item, dict) and bool(item.get("mentions_candidate_path")) for item in inspected_services
    ):
        raise Phase1DCertificationError(
            "a Paper/HyperLab service configuration references the candidate workspace"
        )

    total_before = _resources()
    secure_mkdir(selected_workspace)
    progress_path = selected_workspace / "progress.jsonl"
    _write_progress(
        progress_path,
        {"phase": "PREFLIGHT", "status": "PASS", "timestamp": _utc_now()},
    )
    source_identity = _source_identity(selected_repository)
    manifest = build_phase1d_workload_manifest()

    tail_root = secure_mkdir(selected_workspace / "tail-matrix")
    tail_reports: list[TailCaseReport] = []
    for tail_entries in PHASE1D_TAIL_SIZES:
        report = _run_tail_case(
            tail_root / f"tail-{tail_entries}",
            manifest=manifest,
            tail_entries=tail_entries,
        )
        tail_reports.append(report)
        _write_progress(
            progress_path,
            {
                "phase": "TAIL_MATRIX",
                "status": "PASS",
                "tail_entries": tail_entries,
                "timestamp": _utc_now(),
            },
        )

    crash_root = secure_mkdir(selected_workspace / "crash-matrix")
    crash_reports: list[CrashCaseReport] = []
    crash_plan = build_crash_plan()
    for index, case in enumerate(crash_plan, start=1):
        report = _recover_crash_case(
            crash_root / case.case_id,
            case=case,
            repository_root=selected_repository,
        )
        crash_reports.append(report)
        _write_progress(
            progress_path,
            {
                "case": case.case_id,
                "completed": index,
                "phase": "CRASH_MATRIX",
                "status": "PASS",
                "timestamp": _utc_now(),
                "total": len(crash_plan),
            },
        )

    lease_report = _run_lease_probes(
        selected_workspace / "lease-probes",
        repository_root=selected_repository,
    )
    _write_progress(
        progress_path,
        {"phase": "LEASE_PROBES", "status": "PASS", "timestamp": _utc_now()},
    )
    security_report = _run_security_probes(selected_workspace / "security-probes")
    _write_progress(
        progress_path,
        {"phase": "SECURITY_PROBES", "status": "PASS", "timestamp": _utc_now()},
    )

    owner_uid, owner_gid = _private_owner(selected_workspace)
    final_tree = attest_secure_tree(
        selected_workspace,
        expected_uid=owner_uid,
        expected_gid=owner_gid,
    )
    total_after = _resources()
    verdict = decide_phase1d_verdict(filesystem_type=mount.filesystem_type, gates_passed=True)
    result: dict[str, object] = {
        "artifact": PHASE1D_ARTIFACT,
        "completed_at": _utc_now(),
        "crash_matrix": {
            "case_count": len(crash_reports),
            "cases": [report.payload() for report in crash_reports],
            "signals": ["SIGTERM", "SIGKILL"],
        },
        "filesystem": {
            "mount": mount.payload(),
            "mountinfo_sha256": hashlib.sha256(mountinfo_bytes).hexdigest(),
            "required": "ext4",
        },
        "fresh_isolated_paper_only_integration": {
            "adapter": "SyntheticCapacityPhase1CAdapter",
            "network_used": False,
            "orders_created": 0,
            "paper_only": True,
            "secret_inputs": 0,
            "strategies": list(manifest.config.strategies),
            "workload_markers": list(PHASE1D_SYNTHETIC_MARKERS),
        },
        "git": git_witness,
        "host": _host_payload(uid=owner_uid, gid=owner_gid),
        "lease_probes": lease_report,
        "markers": list(PHASE1D_SYNTHETIC_MARKERS),
        "permissions_and_ownership": {
            "expected_directory_mode": "0700",
            "expected_file_mode": "0600",
            "gid": owner_gid,
            "hardlinks": final_tree.hardlink_count,
            "tree": final_tree.payload(),
            "uid": owner_uid,
        },
        "preflight": {
            "foreign_handles": handles,
            "systemd": systemd,
            "workspace_was_absent": True,
        },
        "resources": _delta(total_before, total_after),
        "security_probes": security_report,
        "source_commit": source_commit,
        "source_identity": source_identity,
        "tail_restart": {
            "cases": [report.payload() for report in tail_reports],
            "proof": "authenticated checkpoint plus bounded overlay; zero historical segments opened",
            "tail_sizes": list(PHASE1D_TAIL_SIZES),
        },
        "verdict": verdict,
        "workload": {
            "batch_size": PHASE1D_BATCH_SIZE,
            "commit_count": manifest.commit_count,
            "logical_row_count": manifest.logical_row_count,
            "profile": manifest.config.profile.value,
            "workload_sha256": manifest.workload_sha256,
        },
    }
    report_path = selected_workspace / "phase1d-report.json"
    report_sha256 = _publish_json(report_path, result)
    complete_payload = {
        "artifact": "STORAGE_V4_PHASE1D_COMPLETE_V1",
        "completed_at": result["completed_at"],
        "report": report_path.name,
        "report_sha256": report_sha256,
        "source_commit": source_commit,
        "verdict": verdict,
    }
    _publish_json(selected_workspace / "COMPLETE.json", complete_payload)
    _write_progress(
        progress_path,
        {"phase": "COMPLETE", "status": verdict, "timestamp": _utc_now()},
    )
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--repository-root", type=Path)
    parser.add_argument("--source-commit")
    parser.add_argument("--paper-service", action="append", default=[])
    parser.add_argument("--internal-crash-worker", action="store_true")
    parser.add_argument("--internal-lease-worker", action="store_true")
    parser.add_argument("--case-root", type=Path)
    parser.add_argument("--case-id")
    parser.add_argument("--store-kind")
    parser.add_argument("--fault-point", choices=[point.value for point in FaultPoint])
    parser.add_argument("--signal-name", choices=("SIGTERM", "SIGKILL"))
    parser.add_argument("--anchor-path", type=Path)
    return parser


_RequiredT = TypeVar("_RequiredT")


def _required(value: _RequiredT | None, *, label: str) -> _RequiredT:
    if value is None:
        raise Phase1DCertificationError(f"missing required argument: {label}")
    return value


def main(arguments: Iterable[str] | None = None) -> int:
    namespace = _parser().parse_args(None if arguments is None else tuple(arguments))
    if namespace.internal_crash_worker:
        case_root = _required(namespace.case_root, label="case-root")
        case_id = _required(namespace.case_id, label="case-id")
        store_kind = _required(namespace.store_kind, label="store-kind")
        fault_point = _required(namespace.fault_point, label="fault-point")
        signal_name = _required(namespace.signal_name, label="signal-name")
        return _crash_worker(
            case_root,
            CrashCase(
                store_kind=store_kind,
                fault_point=FaultPoint(fault_point),
                signal_name=signal_name,
                case_id=case_id,
            ),
        )
    if namespace.internal_lease_worker:
        return _lease_worker(
            case_root=_required(namespace.case_root, label="case-root"),
            label=_required(namespace.case_id, label="case-id"),
            store_kind=_required(namespace.store_kind, label="store-kind"),
            anchor_path=_required(namespace.anchor_path, label="anchor-path"),
        )
    workspace = _required(namespace.workspace, label="workspace")
    repository_root = _required(namespace.repository_root, label="repository-root")
    source_commit = _required(namespace.source_commit, label="source-commit")
    result = run_phase1d_linux_certification(
        workspace=workspace,
        repository_root=repository_root,
        source_commit=source_commit,
        paper_services=namespace.paper_service,
    )
    print(canonical_json_bytes({"verdict": result["verdict"]}).decode("utf-8"))
    return 0


__all__ = [
    "PHASE1D_ARTIFACT",
    "PHASE1D_CERTIFIED_VERDICT",
    "PHASE1D_READY_VERDICT",
    "CrashCase",
    "Phase1DCertificationError",
    "build_crash_plan",
    "build_phase1d_workload_manifest",
    "decide_phase1d_verdict",
    "main",
    "run_phase1d_linux_certification",
]


if __name__ == "__main__":
    raise SystemExit(main())
