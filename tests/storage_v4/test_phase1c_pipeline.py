from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from hyperlab.paper.storage_v4.anchor import LocalAnchor
from hyperlab.paper.storage_v4.canonical import build_commit_logical, canonical_json_bytes
from hyperlab.paper.storage_v4.capacity import (
    CapacityProfile,
    CapacityTypeSpec,
    CapacityWorkloadConfig,
    SyntheticCapacityCommit,
    build_capacity_workload_manifest,
    iter_capacity_commits,
)
from hyperlab.paper.storage_v4.checkpoint import CheckpointState
from hyperlab.paper.storage_v4.contracts import CompatibilityRecord, RawLakeId, StorageMode
from hyperlab.paper.storage_v4.faults import (
    DeterministicFaultInjector,
    FaultPoint,
    InjectedCrash,
)
from hyperlab.paper.storage_v4.manifest import OpaqueIdentity
from hyperlab.paper.storage_v4.phase1c_pipeline import (
    NativeRawRecord,
    Phase1CAuthorityStatus,
    Phase1CBatch,
    Phase1CPipelineError,
    Phase1CPipelineErrorCode,
    Phase1CWriter,
    certify_phase1c_reopen,
    ingest_capacity_workload,
    inspect_phase1c_alignment,
)
from hyperlab.paper.storage_v4.raw_segment import (
    RawRecordMetadata,
    RawSegmentError,
    RawSegmentErrorCode,
    RawSegmentThresholds,
    RawSegmentWriter,
)
from hyperlab.paper.storage_v4.raw_store import RawStore, RawStoreConfig
from hyperlab.paper.storage_v4.repository import RepositoryConfig, StorageRepository
from hyperlab.paper.storage_v4.types import (
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

_RUN = RunId("SYNTHETIC_STORAGE_V4_PHASE1C/pipeline-run")
_PAPER_STORE = StoreId("SYNTHETIC_STORAGE_V4_PHASE1C/paper-store")
_RAW_STORE = StoreId("SYNTHETIC_STORAGE_V4_PHASE1C/raw-store")
_LAKE = RawLakeId("SYNTHETIC_STORAGE_V4_PHASE1C/raw-lake")
_START = Hash32(b"\x00" * 32)


def _sha(value: bytes) -> Hash32:
    return Hash32(hashlib.sha256(value).digest())


def _payload(
    sequence: int,
    *,
    marker: str | None = None,
    record_id: str | None = None,
) -> bytes:
    return (
        canonical_json_bytes(
            {
                "arrival_sequence": sequence,
                "commit_sequence": sequence,
                "input_id": record_id or f"input-{sequence}",
                "payload": {"marker": marker or f"public-{sequence}"},
                "run_id": _RUN.value,
            }
        )
        + b"\n"
    )


def _metadata(sequence: int, *, record_id: str | None = None) -> RawRecordMetadata:
    return RawRecordMetadata(
        record_id=record_id or f"input-{sequence}",
        source_id="synthetic-public-source",
        venue_id="SYNTHETIC",
        input_type="PUBLIC_MARKET_DATA",
        source_stream_id=StreamId("public-wire"),
        source_first_sequence=EventSequence(sequence),
        source_last_sequence=EventSequence(sequence),
        arrival_sequence=EventSequence(sequence),
        source_timestamp=f"2026-08-25T12:00:{sequence:02d}Z",
        received_timestamp=f"2026-08-25T12:00:{sequence:02d}Z",
    )


def _raw_record(
    sequence: int,
    *,
    marker: str | None = None,
    record_id: str | None = None,
) -> NativeRawRecord:
    return NativeRawRecord(
        commit_sequence=sequence,
        payload=_payload(sequence, marker=marker, record_id=record_id),
        metadata=_metadata(sequence, record_id=record_id),
    )


def _frames(count: int = 3) -> tuple[CommitFrame, ...]:
    previous = _START
    frames: list[CommitFrame] = []
    for sequence in range(1, count + 1):
        rows = [
            CompatibilityRecord.from_jsonl_bytes(_payload(sequence)).to_logical_row(
                StreamId("inbox"), CommitOrdinal(0)
            ),
            LogicalRow(
                StreamId("projection_history"),
                CommitOrdinal(0),
                {"revision": sequence, "run_id": _RUN.value},
            ),
        ]
        if sequence == 2:
            rows.append(
                LogicalRow(
                    StreamId("alerts"),
                    CommitOrdinal(0),
                    {"code": "MARKET_GAP", "run_id": _RUN.value},
                )
            )
        frame = CommitFrame(
            run_id=_RUN,
            commit_sequence=CommitSequence(sequence),
            previous_prefix_root=previous,
            rows=tuple(rows),
            legacy_v3_identity=_sha(f"legacy-{sequence}".encode()),
        )
        previous = build_commit_logical(frame).prefix_root
        frames.append(frame)
    return tuple(frames)


def _frames_with_record_ids(record_ids: tuple[str, ...]) -> tuple[CommitFrame, ...]:
    previous = _START
    frames: list[CommitFrame] = []
    for sequence, record_id in enumerate(record_ids, start=1):
        frame = CommitFrame(
            run_id=_RUN,
            commit_sequence=CommitSequence(sequence),
            previous_prefix_root=previous,
            rows=(
                CompatibilityRecord.from_jsonl_bytes(_payload(sequence, record_id=record_id)).to_logical_row(
                    StreamId("inbox"), CommitOrdinal(0)
                ),
            ),
            legacy_v3_identity=_sha(f"legacy-{sequence}".encode()),
        )
        previous = build_commit_logical(frame).prefix_root
        frames.append(frame)
    return tuple(frames)


def _state(revision: int) -> CheckpointState:
    return CheckpointState(
        adapter={"paper_cursor": revision},
        ledger={"cash": "100.00"},
        projection={"revision": revision},
        sessions={"closed": []},
        incidents={"resolved": ["MARKET_GAP"] if revision >= 2 else []},
        cursors={"public": revision},
        stream_heads={"inbox": revision},
    )


def _configs() -> tuple[RawStoreConfig, RepositoryConfig]:
    config_identity = _sha(b"phase1c-config")
    raw_config = RawStoreConfig(
        store_id=_RAW_STORE,
        lake_id=_LAKE,
        config_identity=config_identity,
    )
    paper_config = RepositoryConfig(
        store_id=_PAPER_STORE,
        run_id=_RUN,
        mode=StorageMode.V4_NATIVE,
        run_identity=OpaqueIdentity(_sha(b"run")),
        config_identity=OpaqueIdentity(config_identity),
        code_identity=OpaqueIdentity(_sha(b"code")),
        runtime_identity=OpaqueIdentity(_sha(b"runtime")),
        start_prefix_root=_START,
    )
    return raw_config, paper_config


def _systems(
    tmp_path: Path,
    *,
    raw_fault_hook: DeterministicFaultInjector | None = None,
) -> tuple[
    RawStore,
    LocalAnchor,
    RawStoreConfig,
    StorageRepository,
    LocalAnchor,
    RepositoryConfig,
    Path,
]:
    raw_config, paper_config = _configs()
    raw_anchor = LocalAnchor.create(tmp_path / "raw-anchor.sqlite3", store_id=_RAW_STORE)
    paper_anchor = LocalAnchor.create(tmp_path / "paper-anchor.sqlite3", store_id=_PAPER_STORE)
    raw = RawStore.create(
        tmp_path / "raw",
        anchor=raw_anchor,
        config=raw_config,
        fault_hook=raw_fault_hook,
    )
    paper = StorageRepository.create(tmp_path / "paper", anchor=paper_anchor, config=paper_config)
    staging = tmp_path / "staging"
    staging.mkdir()
    return raw, raw_anchor, raw_config, paper, paper_anchor, paper_config, staging


def _one_record_raw_thresholds(
    records: tuple[NativeRawRecord, ...],
) -> RawSegmentThresholds:
    maximum_payload = max(len(record.payload) for record in records)
    return RawSegmentThresholds(
        max_records=100,
        max_logical_payload_bytes=maximum_payload,
        max_physical_bytes=1024 * 1024,
        max_single_payload_bytes=maximum_payload,
    )


def test_one_paper_batch_rotates_multiple_raw_segments_and_audits_exactly(
    tmp_path: Path,
) -> None:
    raw, raw_anchor, raw_config, paper, paper_anchor, paper_config, staging = _systems(tmp_path)
    source = _frames()
    records = tuple(_raw_record(sequence) for sequence in range(1, 4))
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
        raw_thresholds=_one_record_raw_thresholds(records),
    )

    batch = writer.append_batch(Phase1CBatch(source_frames=source, raw_records=records))

    assert len(batch.raw_seals) == 3
    assert batch.raw_seal is batch.raw_seals[-1]
    assert [seal.manifest.generation for seal in batch.raw_seals] == [1, 2, 3]
    assert [seal.descriptor.record_count for seal in batch.raw_seals] == [1, 1, 1]
    assert [int(seal.references[0].arrival_sequence) for seal in batch.raw_seals] == [1, 2, 3]
    assert batch.cursor.raw_manifest_roots == tuple(seal.manifest.root for seal in batch.raw_seals)
    assert batch.cursor.raw_reference_count == 3
    assert paper.overlay_state.tail_commit_count == 3
    assert raw.manifest is not None
    assert raw.manifest.generation == 3
    assert raw.manifest.total_record_count == 3
    assert len(raw.manifest.segments) == 3
    assert list(staging.iterdir()) == []

    terminal = writer.seal(_state(3))
    assert terminal.binding.raw_generation == 3
    assert terminal.expectations.raw_manifest_roots == batch.cursor.raw_manifest_roots
    paper.close()
    raw.close()

    report = certify_phase1c_reopen(
        raw_root=tmp_path / "raw",
        raw_anchor=raw_anchor,
        raw_config=raw_config,
        paper_root=tmp_path / "paper",
        paper_anchor=paper_anchor,
        paper_config=paper_config,
        binding=terminal.binding,
        expectations=terminal.expectations,
    )
    assert report.alignment.status is Phase1CAuthorityStatus.ALIGNED
    assert report.raw_audit.segments_read == 3
    assert report.raw_audit.records_read == 3
    assert report.paper_audit.commits_read == 3
    assert report.native_audit.commit_count == 3
    assert report.native_audit.final_prefix_root == batch.cursor.native_prefix_root
    assert report.native_audit.raw_manifest_roots == batch.cursor.raw_manifest_roots


def test_single_raw_record_limit_remains_fail_closed(tmp_path: Path) -> None:
    raw, _, _, paper, _, _, staging = _systems(tmp_path)
    record = _raw_record(1)
    thresholds = RawSegmentThresholds(
        max_records=100,
        max_logical_payload_bytes=len(record.payload),
        max_physical_bytes=1024 * 1024,
        max_single_payload_bytes=len(record.payload) - 1,
    )
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
        raw_thresholds=thresholds,
    )
    batch = Phase1CBatch(source_frames=_frames()[:1], raw_records=(record,))

    with pytest.raises(Phase1CPipelineError) as caught:
        writer.append_batch(batch)

    assert caught.value.code is Phase1CPipelineErrorCode.RAW_PUBLICATION_FAILED
    cause = caught.value.__cause__
    assert isinstance(cause, RawSegmentError)
    assert cause.code is RawSegmentErrorCode.LIMIT
    assert raw.manifest is None
    assert paper.manifest is None
    assert paper.overlay_state.tail_commit_count == 0
    assert writer.cursor.next_commit_sequence == 1
    assert list(staging.iterdir()) == []
    with pytest.raises(Phase1CPipelineError) as poisoned:
        writer.append_batch(batch)
    assert poisoned.value.code is Phase1CPipelineErrorCode.STATE_INVALID
    paper.close()
    raw.close()


def test_failure_after_first_rotated_raw_seal_preserves_authenticated_prefix(
    tmp_path: Path,
) -> None:
    injector = DeterministicFaultInjector(
        FaultPoint.BEFORE_RAW_SEGMENT_PUBLICATION,
        occurrence=2,
    )
    raw, _, _, paper, _, _, staging = _systems(
        tmp_path,
        raw_fault_hook=injector,
    )
    records = tuple(_raw_record(sequence) for sequence in range(1, 4))
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
        raw_thresholds=_one_record_raw_thresholds(records),
    )

    with pytest.raises(InjectedCrash) as caught:
        writer.append_batch(Phase1CBatch(source_frames=_frames(), raw_records=records))

    assert caught.value.point is FaultPoint.BEFORE_RAW_SEGMENT_PUBLICATION
    assert caught.value.occurrence == 2
    assert injector.seen == 2
    assert injector.triggered is True
    assert raw.manifest is not None
    assert raw.manifest.generation == 1
    assert raw.manifest.total_record_count == 1
    assert len(raw.manifest.segments) == 1
    assert paper.manifest is None
    assert paper.overlay_state.tail_commit_count == 0
    assert writer.cursor.next_commit_sequence == 1
    assert inspect_phase1c_alignment(raw, paper).status is Phase1CAuthorityStatus.RAW_VALID_PAPER_ABSENT
    paper.close()
    raw.close()


def test_resume_discards_unsealed_paper_tail_and_reuses_authenticated_raw_suffix(
    tmp_path: Path,
) -> None:
    raw, raw_anchor, raw_config, paper, paper_anchor, paper_config, staging = _systems(
        tmp_path
    )
    frames = _frames(4)
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
    )
    first_batch = Phase1CBatch(
        source_frames=frames[:2],
        raw_records=tuple(_raw_record(sequence) for sequence in range(1, 3)),
    )
    writer.append_batch(first_batch)
    writer.seal(_state(2))
    suffix_batch = Phase1CBatch(
        source_frames=frames[2:],
        raw_records=tuple(_raw_record(sequence) for sequence in range(3, 5)),
    )
    writer.append_batch(suffix_batch)
    assert paper.overlay_state.tail_commit_count == 2
    assert raw.manifest is not None
    raw_generation_before_resume = raw.manifest.generation
    raw.close()
    paper.close()

    reopened_raw = RawStore.open_existing(
        tmp_path / "raw",
        anchor=raw_anchor,
        config=raw_config,
    )
    reopened_paper = StorageRepository.open_existing(
        tmp_path / "paper",
        anchor=paper_anchor,
        config=paper_config,
    )
    resume = Phase1CWriter.resume_from_authenticated_checkpoint(
        raw_store=reopened_raw,
        paper_repository=reopened_paper,
        staging_directory=staging,
        source_prefix_root=build_commit_logical(frames[1]).prefix_root,
    )
    assert resume.boundary_audit.commit_count == 2
    assert resume.raw_suffix.suffix_records_read == 2
    assert resume.discarded_tail.discarded_commit_count == 2
    assert reopened_paper.overlay_state.tail_commit_count == 0
    assert tuple(
        int(reference.arrival_sequence) for reference in resume.suffix_references
    ) == (3, 4)

    rebuilt = resume.writer.append_presealed_batch(
        suffix_batch,
        resume.suffix_references,
    )
    assert rebuilt.raw_seals == ()
    assert reopened_raw.manifest is not None
    assert reopened_raw.manifest.generation == raw_generation_before_resume
    terminal = resume.writer.seal(_state(4))
    assert terminal.expectations.commit_count == 4
    assert terminal.expectations.final_prefix_root == rebuilt.cursor.native_prefix_root
    assert inspect_phase1c_alignment(
        reopened_raw,
        reopened_paper,
    ).status is Phase1CAuthorityStatus.ALIGNED
    reopened_raw.close()
    reopened_paper.close()


def test_two_raw_first_batches_two_checkpoint_cycles_reopen_and_full_audit(
    tmp_path: Path,
) -> None:
    raw, raw_anchor, raw_config, paper, paper_anchor, paper_config, staging = _systems(tmp_path)
    source = _frames()
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
    )

    first_batch = writer.append_batch(Phase1CBatch(source_frames=source[:1], raw_records=(_raw_record(1),)))
    first_seal = writer.seal(_state(1))
    second_batch = writer.append_batch(Phase1CBatch(source_frames=source[1:], raw_records=(_raw_record(3),)))
    terminal = writer.seal(_state(3))

    assert first_batch.raw_seal is not None
    assert first_batch.raw_seal.manifest.generation == 1
    assert first_seal.paper_seal.manifest.generation == 1
    assert second_batch.raw_seal is not None
    assert second_batch.raw_seal.manifest.generation == 2
    assert terminal.paper_seal.manifest.generation == 2
    assert terminal.binding.raw_generation == 2
    assert terminal.binding.raw_record_count == 2
    assert writer.cursor.next_commit_sequence == 4
    assert not hasattr(writer.cursor, "seen_raw_records")
    assert source[1].previous_prefix_root == build_commit_logical(source[0]).prefix_root
    assert (
        second_batch.native_frames[0].previous_prefix_root
        == build_commit_logical(first_batch.native_frames[0]).prefix_root
    )
    assert inspect_phase1c_alignment(raw, paper).status is Phase1CAuthorityStatus.ALIGNED

    paper.close()
    raw.close()
    report = certify_phase1c_reopen(
        raw_root=tmp_path / "raw",
        raw_anchor=raw_anchor,
        raw_config=raw_config,
        paper_root=tmp_path / "paper",
        paper_anchor=paper_anchor,
        paper_config=paper_config,
        binding=terminal.binding,
        expectations=terminal.expectations,
    )

    assert report.raw_startup.historical_segments_read == 0
    assert report.paper_startup.segments_read == 0
    assert report.paper_startup.checkpoint_used is True
    assert report.raw_audit.segments_read == 2
    assert report.raw_audit.records_read == 2
    assert report.paper_audit.manifests_read == 2
    assert report.paper_audit.checkpoints_read == 2
    assert report.paper_audit.segments_read == 2
    assert report.paper_audit.commits_read == 3
    assert report.native_audit.raw_reference_count == 2
    assert report.native_audit.market_gap_count == 1
    assert report.raw_resolver_physical_hash_passes == 2


def test_reopen_certifies_authenticated_checkpoint_plus_native_tail(tmp_path: Path) -> None:
    raw, raw_anchor, raw_config, paper, paper_anchor, paper_config, staging = _systems(tmp_path)
    source = _frames(4)
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
    )
    writer.append_batch(Phase1CBatch(source_frames=source[:1], raw_records=(_raw_record(1),)))
    checkpoint = writer.seal(_state(1))
    writer.append_batch(
        Phase1CBatch(
            source_frames=source[1:],
            raw_records=tuple(_raw_record(sequence) for sequence in range(2, 5)),
        )
    )
    terminal_expectations = writer.audit_expectations()
    paper.close()
    raw.close()

    report = certify_phase1c_reopen(
        raw_root=tmp_path / "raw",
        raw_anchor=raw_anchor,
        raw_config=raw_config,
        paper_root=tmp_path / "paper",
        paper_anchor=paper_anchor,
        paper_config=paper_config,
        binding=checkpoint.binding,
        expectations=terminal_expectations,
        expected_tail_entries=3,
        checkpoint_expectations=checkpoint.expectations,
    )

    assert report.paper_startup.segments_read == 0
    assert report.paper_startup.tail_entries_replayed == 3
    assert report.paper_startup.historical_commits_not_read == 1
    assert report.alignment.status is Phase1CAuthorityStatus.RAW_AHEAD_OF_PAPER
    assert report.paper_audit.commits_read == 1
    assert report.native_audit.commit_count == 4
    assert report.raw_audit.records_read == 4


def test_cross_batch_duplicate_reference_fails_terminal_tail_certification(
    tmp_path: Path,
) -> None:
    raw, raw_anchor, raw_config, paper, paper_anchor, paper_config, staging = _systems(tmp_path)
    source = _frames_with_record_ids(("duplicate-input", "duplicate-input"))
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
    )
    writer.append_batch(
        Phase1CBatch(
            source_frames=source[:1],
            raw_records=(_raw_record(1, record_id="duplicate-input"),),
        )
    )
    checkpoint = writer.seal(_state(1))
    writer.append_batch(
        Phase1CBatch(
            source_frames=source[1:],
            raw_records=(_raw_record(2, record_id="duplicate-input"),),
        )
    )
    terminal_expectations = writer.audit_expectations()
    paper.close()
    raw.close()

    with pytest.raises(Phase1CPipelineError) as caught:
        certify_phase1c_reopen(
            raw_root=tmp_path / "raw",
            raw_anchor=raw_anchor,
            raw_config=raw_config,
            paper_root=tmp_path / "paper",
            paper_anchor=paper_anchor,
            paper_config=paper_config,
            binding=checkpoint.binding,
            expectations=terminal_expectations,
            expected_tail_entries=1,
            checkpoint_expectations=checkpoint.expectations,
        )

    assert caught.value.code is Phase1CPipelineErrorCode.AUDIT_FAILED


def test_raw_valid_paper_absent_is_explicit_recovery_state(tmp_path: Path) -> None:
    raw, _, _, paper, _, _, staging = _systems(tmp_path)
    with RawSegmentWriter(staging, lake_id=_LAKE) as segment_writer:
        segment_writer.append(_payload(1), _metadata(1))
        raw.seal(segment_writer.seal())

    report = inspect_phase1c_alignment(raw, paper)

    assert report.status is Phase1CAuthorityStatus.RAW_VALID_PAPER_ABSENT
    assert report.raw_generation == 1
    assert report.raw_record_count == 1
    assert report.paper_generation == 0
    assert report.paper_tail_commit_count == 0
    paper.close()
    raw.close()


def test_crash_after_raw_before_paper_append_is_quarantined_on_reopen(
    tmp_path: Path,
) -> None:
    raw, raw_anchor, raw_config, paper, paper_anchor, paper_config, staging = _systems(tmp_path)
    injector = DeterministicFaultInjector(FaultPoint.AFTER_RAW_BEFORE_PAPER_APPEND)
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
        fault_hook=injector,
    )

    with pytest.raises(InjectedCrash) as caught:
        writer.append_batch(
            Phase1CBatch(
                source_frames=_frames()[:1],
                raw_records=(_raw_record(1),),
            )
        )

    assert caught.value.point is FaultPoint.AFTER_RAW_BEFORE_PAPER_APPEND
    assert injector.triggered is True
    assert raw.manifest is not None
    assert raw.manifest.generation == 1
    assert paper.manifest is None
    assert paper.overlay_state.tail_commit_count == 0
    assert inspect_phase1c_alignment(raw, paper).status is Phase1CAuthorityStatus.RAW_VALID_PAPER_ABSENT
    with pytest.raises(Phase1CPipelineError) as poisoned:
        writer.audit_expectations()
    assert poisoned.value.code is Phase1CPipelineErrorCode.STATE_INVALID
    paper.close()
    raw.close()

    for _ in range(2):
        reopened_raw = RawStore.open_existing(
            tmp_path / "raw",
            anchor=raw_anchor,
            config=raw_config,
        )
        reopened_paper = StorageRepository.open_existing(
            tmp_path / "paper",
            anchor=paper_anchor,
            config=paper_config,
        )
        assert (
            inspect_phase1c_alignment(reopened_raw, reopened_paper).status
            is Phase1CAuthorityStatus.RAW_VALID_PAPER_ABSENT
        )
        with pytest.raises(Phase1CPipelineError) as quarantined:
            Phase1CWriter(
                raw_store=reopened_raw,
                paper_repository=reopened_paper,
                staging_directory=staging,
            )
        assert quarantined.value.code is Phase1CPipelineErrorCode.STATE_INVALID
        reopened_paper.close()
        reopened_raw.close()


def test_valid_tail_commit_with_missing_raw_segment_is_refused_idempotently(
    tmp_path: Path,
) -> None:
    raw, raw_anchor, raw_config, paper, paper_anchor, paper_config, staging = _systems(tmp_path)
    source = _frames(2)
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
    )
    writer.append_batch(Phase1CBatch(source_frames=source[:1], raw_records=(_raw_record(1),)))
    checkpoint = writer.seal(_state(1))
    tail = writer.append_batch(Phase1CBatch(source_frames=source[1:], raw_records=(_raw_record(2),)))
    expectations = writer.audit_expectations()
    assert tail.raw_seal is not None
    missing = tail.raw_seal.segment_path
    paper.close()
    raw.close()
    missing.unlink()

    for _ in range(2):
        with pytest.raises(Phase1CPipelineError) as refused:
            certify_phase1c_reopen(
                raw_root=tmp_path / "raw",
                raw_anchor=raw_anchor,
                raw_config=raw_config,
                paper_root=tmp_path / "paper",
                paper_anchor=paper_anchor,
                paper_config=paper_config,
                binding=checkpoint.binding,
                expectations=expectations,
                expected_tail_entries=1,
                checkpoint_expectations=checkpoint.expectations,
            )
        assert refused.value.code is Phase1CPipelineErrorCode.AUDIT_FAILED


@pytest.mark.parametrize("mutation", ("absent", "truncated"))
def test_checkpoint_with_unresolvable_raw_reference_is_refused_idempotently(
    tmp_path: Path,
    mutation: str,
) -> None:
    raw, raw_anchor, raw_config, paper, paper_anchor, paper_config, staging = _systems(tmp_path)
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
    )
    batch = writer.append_batch(Phase1CBatch(source_frames=_frames()[:1], raw_records=(_raw_record(1),)))
    terminal = writer.seal(_state(1))
    assert batch.raw_seal is not None
    segment = batch.raw_seal.segment_path
    paper.close()
    raw.close()
    if mutation == "absent":
        segment.unlink()
    else:
        segment.write_bytes(b"truncated")

    for _ in range(2):
        with pytest.raises(Phase1CPipelineError) as refused:
            certify_phase1c_reopen(
                raw_root=tmp_path / "raw",
                raw_anchor=raw_anchor,
                raw_config=raw_config,
                paper_root=tmp_path / "paper",
                paper_anchor=paper_anchor,
                paper_config=paper_config,
                binding=terminal.binding,
                expectations=terminal.expectations,
            )
        assert refused.value.code is Phase1CPipelineErrorCode.AUDIT_FAILED


def test_raw_payload_divergence_fails_before_raw_or_paper_publication(tmp_path: Path) -> None:
    raw, _, _, paper, _, _, staging = _systems(tmp_path)
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
    )

    with pytest.raises(Phase1CPipelineError) as caught:
        writer.append_batch(
            Phase1CBatch(
                source_frames=_frames()[:1],
                raw_records=(_raw_record(1, marker="different"),),
            )
        )

    assert caught.value.code is Phase1CPipelineErrorCode.RAW_INPUT_INVALID
    assert raw.manifest is None
    assert paper.manifest is None
    assert paper.overlay_state.tail_commit_count == 0
    assert writer.cursor.next_commit_sequence == 1
    paper.close()
    raw.close()


def _capacity_config() -> CapacityWorkloadConfig:
    return CapacityWorkloadConfig(
        profile=CapacityProfile.GOLDEN_SHAPED,
        seed=71,
        commit_count=3,
        start_time_ns=1_700_000_000_000_000_000,
        cadence_ns=1_000_000_000,
        type_distribution=(
            CapacityTypeSpec(
                record_type="PUBLIC_BBO",
                stream="inbox",
                weight=1,
                payload_min_bytes=1,
                payload_max_bytes=7,
                payload_cardinality=3,
            ),
        ),
        strategies=("phase05_cash_and_carry",),
        alert_every_commits=None,
        incident_every_commits=None,
        ledger_every_commits=None,
        market_gap_count=0,
        alert_payload_bytes=1,
        incident_payload_bytes=1,
        ledger_payload_bytes=1,
        market_gap_payload_bytes=1,
        golden_census_sha256="a" * 64,
        bounded_tail_max=None,
    )


class _CapacityAdapter:
    def __init__(self) -> None:
        self._previous = _START

    def build_phase1c_batch(
        self,
        commits: tuple[SyntheticCapacityCommit, ...],
    ) -> Phase1CBatch:
        frames: list[CommitFrame] = []
        records: list[NativeRawRecord] = []
        for commit in commits:
            sequence = commit.sequence
            payload = (
                canonical_json_bytes(
                    {
                        "arrival_sequence": sequence,
                        "commit_sequence": sequence,
                        "input_id": f"input-{sequence}",
                        "payload": {
                            "synthetic_capacity_descriptor": commit.descriptor(),
                        },
                        "run_id": _RUN.value,
                    }
                )
                + b"\n"
            )
            frame = CommitFrame(
                run_id=_RUN,
                commit_sequence=CommitSequence(sequence),
                previous_prefix_root=self._previous,
                rows=(
                    CompatibilityRecord.from_jsonl_bytes(payload).to_logical_row(
                        StreamId("inbox"), CommitOrdinal(0)
                    ),
                ),
            )
            frames.append(frame)
            self._previous = build_commit_logical(frame).prefix_root
            records.append(
                NativeRawRecord(
                    commit_sequence=sequence,
                    payload=payload,
                    metadata=_metadata(sequence),
                )
            )
        return Phase1CBatch(tuple(frames), tuple(records))


def test_capacity_protocol_ingests_three_batches_with_bounded_cursor(tmp_path: Path) -> None:
    raw, _, _, paper, _, _, staging = _systems(tmp_path)
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
    )
    config = _capacity_config()
    manifest = build_capacity_workload_manifest(config)

    report = ingest_capacity_workload(
        writer=writer,
        manifest=manifest,
        commits=iter_capacity_commits(config),
        adapter=_CapacityAdapter(),
        batch_size=1,
    )

    assert report.batch_count == 3
    assert report.commit_count == 3
    assert report.logical_row_count == manifest.logical_row_count
    assert report.observed_workload_sha256 == manifest.workload_sha256
    assert report.cursor.next_commit_sequence == 4
    assert report.cursor.raw_reference_count == 3
    assert not hasattr(report.cursor, "seen_raw_records")
    assert raw.manifest is not None
    assert raw.manifest.generation == 3
    assert paper.overlay_state.tail_commit_count == 3
    paper.close()
    raw.close()
