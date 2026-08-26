from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from hyperlab.paper.storage_v4.anchor import LocalAnchor
from hyperlab.paper.storage_v4.canonical import build_commit_logical
from hyperlab.paper.storage_v4.capacity import (
    CAPACITY_MARKERS,
    CapacityProfile,
    CapacityTypeSpec,
    CapacityWorkloadConfig,
    CapacityWorkloadHasher,
    SyntheticCapacityCommit,
    build_capacity_workload_manifest,
    iter_capacity_commits,
)
from hyperlab.paper.storage_v4.capacity_adapter import (
    PAPER_DIRECT_OWNERSHIP,
    RAW_NATIVE_INBOX_OWNERSHIP,
    SYNTHETIC_CAPACITY_ADAPTER_CONTRACT,
    SYNTHETIC_CAPACITY_ROW_CONTRACT,
    SYNTHETIC_CAPACITY_SOURCE_ID,
    SYNTHETIC_CAPACITY_VENUE_ID,
    SyntheticCapacityAdapterError,
    SyntheticCapacityAdapterErrorCode,
    SyntheticCapacityPhase1CAdapter,
)
from hyperlab.paper.storage_v4.capacity_oracle import compare_capacity_native_exact
from hyperlab.paper.storage_v4.contracts import CompatibilityRecord, RawLakeId, StorageMode
from hyperlab.paper.storage_v4.manifest import OpaqueIdentity
from hyperlab.paper.storage_v4.phase1c_pipeline import (
    Phase1CBatch,
    Phase1CCapacityBatchAdapter,
    Phase1CWriter,
)
from hyperlab.paper.storage_v4.raw_store import DiskRawResolver, RawStore, RawStoreConfig
from hyperlab.paper.storage_v4.repository import RepositoryConfig, StorageRepository
from hyperlab.paper.storage_v4.types import Hash32, RunId, StoreId

_RUN = RunId("SYNTHETIC_CAPACITY_WORKLOAD/adapter-run")
_START = Hash32(b"\x00" * 32)


def _sha(value: bytes) -> Hash32:
    return Hash32(hashlib.sha256(value).digest())


def _config(*, commit_count: int = 8) -> CapacityWorkloadConfig:
    return CapacityWorkloadConfig(
        profile=CapacityProfile.GOLDEN_SHAPED,
        seed=41,
        commit_count=commit_count,
        start_time_ns=1_700_000_000_000_000_000,
        cadence_ns=250_000_000,
        type_distribution=(
            CapacityTypeSpec(
                record_type="PUBLIC_BBO",
                stream="inbox",
                weight=2,
                payload_min_bytes=3,
                payload_max_bytes=13,
                payload_cardinality=3,
            ),
            CapacityTypeSpec(
                record_type="PUBLIC_FUNDING",
                stream="events",
                weight=1,
                payload_min_bytes=5,
                payload_max_bytes=17,
                payload_cardinality=5,
            ),
        ),
        strategies=("phase05_cash_and_carry", "phase08_lead_lag"),
        alert_every_commits=3,
        incident_every_commits=4,
        ledger_every_commits=2,
        market_gap_count=2,
        alert_payload_bytes=7,
        incident_payload_bytes=9,
        ledger_payload_bytes=11,
        market_gap_payload_bytes=13,
        golden_census_sha256="a" * 64,
    )


def _decoded_payload(record: bytes) -> dict[str, object]:
    decoded = json.loads(record)
    assert type(decoded) is dict
    return decoded


def _assert_payload(value: object, expected: bytes) -> None:
    assert type(value) is dict
    assert value["size_bytes"] == len(expected)
    assert value["content_sha256"] == hashlib.sha256(expected).hexdigest()
    encoded = value["content_base64"]
    assert type(encoded) is str
    assert base64.b64decode(encoded, validate=True) == expected


def test_multibatch_adapter_preserves_source_chain_row_ownership_and_payloads() -> None:
    commits = tuple(iter_capacity_commits(_config()))
    adapter = SyntheticCapacityPhase1CAdapter(run_id=_RUN, max_batch_commits=3)

    assert isinstance(adapter, Phase1CCapacityBatchAdapter)
    batches = (
        adapter.build_phase1c_batch(commits[:3]),
        adapter.build_phase1c_batch(commits[3:6]),
        adapter.build_phase1c_batch(commits[6:]),
    )

    previous = _START
    flattened_frames = tuple(frame for batch in batches for frame in batch.source_frames)
    flattened_records = tuple(record for batch in batches for record in batch.raw_records)
    assert len(flattened_frames) == len(flattened_records) == len(commits)

    for commit, frame, record in zip(
        commits, flattened_frames, flattened_records, strict=True
    ):
        assert frame.previous_prefix_root == previous
        assert int(frame.commit_sequence) == commit.sequence
        assert len(frame.rows) == len(commit.rows)
        previous = build_commit_logical(frame).prefix_root

        assert record.commit_sequence == commit.sequence
        assert record.metadata.record_id.startswith("synthetic-capacity-v1:")
        assert record.metadata.source_id == SYNTHETIC_CAPACITY_SOURCE_ID
        assert record.metadata.venue_id == SYNTHETIC_CAPACITY_VENUE_ID
        assert record.metadata.source_stream_id.value == commit.rows[0].stream
        assert int(record.metadata.source_first_sequence) == commit.rows[0].source_sequence
        assert int(record.metadata.source_last_sequence) == commit.rows[0].source_sequence
        assert int(record.metadata.arrival_sequence) == commit.sequence
        assert record.metadata.source_timestamp == f"unix-ns:{commit.logical_time_ns}"
        assert record.metadata.received_timestamp == record.metadata.source_timestamp

        assert CompatibilityRecord.from_logical_row(frame.rows[0]).jsonl_bytes == record.payload
        raw_value = _decoded_payload(record.payload)
        assert raw_value["contract"] == SYNTHETIC_CAPACITY_ROW_CONTRACT
        assert raw_value["markers"] == list(CAPACITY_MARKERS)
        assert raw_value["ownership"] == RAW_NATIVE_INBOX_OWNERSHIP
        assert raw_value["commit_sequence"] == commit.sequence
        assert raw_value["row_ordinal"] == 0
        assert raw_value["source_sequence"] == commit.rows[0].source_sequence
        assert raw_value["stream"] == commit.rows[0].stream
        assert raw_value["input_id"] == record.metadata.record_id
        assert raw_value["run_id"] == _RUN.value
        _assert_payload(raw_value["payload"], commit.rows[0].payload.to_bytes())

        local_ordinals = {"inbox": 1}
        for logical, synthetic in zip(frame.rows[1:], commit.rows[1:], strict=True):
            expected_ordinal = local_ordinals.get(synthetic.stream, 0)
            assert logical.stream_id.value == synthetic.stream
            assert int(logical.ordinal) == expected_ordinal
            local_ordinals[synthetic.stream] = expected_ordinal + 1
            value = logical.value
            assert type(value) is dict
            assert value["contract"] == SYNTHETIC_CAPACITY_ROW_CONTRACT
            assert value["markers"] == list(CAPACITY_MARKERS)
            assert value["ownership"] == PAPER_DIRECT_OWNERSHIP
            assert value["commit_sequence"] == commit.sequence
            assert value["row_ordinal"] == synthetic.row_ordinal
            assert value["source_sequence"] == synthetic.source_sequence
            _assert_payload(value["payload"], synthetic.payload.to_bytes())

    assert adapter.next_commit_sequence == len(commits) + 1
    assert adapter.source_prefix_root == previous
    assert adapter.commit_count == len(commits)
    assert adapter.logical_row_count == sum(len(commit.rows) for commit in commits)
    assert adapter.raw_record_count == len(commits)


def test_checkpoint_state_is_repeatable_bounded_and_failures_are_atomic() -> None:
    commits = tuple(iter_capacity_commits(_config(commit_count=20)))
    adapter = SyntheticCapacityPhase1CAdapter(
        run_id=_RUN,
        max_batch_commits=5,
        max_tracked_streams=8,
    )

    adapter.build_phase1c_batch(commits[:5])
    first_state = adapter.checkpoint_state()
    assert adapter.checkpoint_state().canonical_sections == first_state.canonical_sections
    first_size = sum(map(len, first_state.canonical_sections))

    for offset in range(5, len(commits), 5):
        adapter.build_phase1c_batch(commits[offset : offset + 5])
    terminal = adapter.checkpoint_state()
    terminal_size = sum(map(len, terminal.canonical_sections))

    assert terminal.adapter["contract"] == SYNTHETIC_CAPACITY_ADAPTER_CONTRACT
    assert terminal.adapter["markers"] == list(CAPACITY_MARKERS)
    assert terminal.adapter["commit_count"] == 20
    assert terminal.adapter["logical_row_count"] == sum(len(item.rows) for item in commits)
    assert terminal.cursors["covered_commit_sequence"] == 20
    assert terminal.cursors["next_commit_sequence"] == 21
    assert terminal.cursors["raw_record_count"] == 20
    assert terminal.incidents["market_gap_count"] == 2
    assert adapter.tracked_stream_count <= 8
    assert terminal_size < first_size + 256
    assert not any("seen" in slot or "record_ids" in slot for slot in adapter.__slots__)

    before_failure = terminal.canonical_sections
    with pytest.raises(SyntheticCapacityAdapterError) as caught:
        adapter.build_phase1c_batch(commits[:1])
    assert caught.value.code is SyntheticCapacityAdapterErrorCode.COMMIT_DIVERGENCE
    assert adapter.checkpoint_state().canonical_sections == before_failure

    bad_row = replace(
        commits[-1].rows[0],
        commit_sequence=21,
        source_sequence=commits[-1].rows[0].source_sequence + 2,
    )
    bad_commit = SyntheticCapacityCommit(
        sequence=21,
        logical_time_ns=commits[-1].logical_time_ns,
        strategy=commits[-1].strategy,
        rows=(bad_row,),
    )
    with pytest.raises(SyntheticCapacityAdapterError) as caught:
        adapter.build_phase1c_batch((bad_commit,))
    assert caught.value.code is SyntheticCapacityAdapterErrorCode.STREAM_DIVERGENCE
    assert adapter.checkpoint_state().canonical_sections == before_failure

    with pytest.raises(SyntheticCapacityAdapterError) as caught:
        adapter.build_phase1c_batch(commits[:6])
    assert caught.value.code is SyntheticCapacityAdapterErrorCode.BATCH_LIMIT


def test_checkpoint_resume_skips_certified_prefix_and_matches_continuous_state() -> None:
    config = _config(commit_count=20)
    commits = tuple(iter_capacity_commits(config))
    continuous = SyntheticCapacityPhase1CAdapter(run_id=_RUN, max_batch_commits=5)
    continuous_hasher = CapacityWorkloadHasher()
    for offset in range(0, 20, 5):
        batch = commits[offset : offset + 5]
        for commit in batch:
            continuous_hasher.update(commit)
        continuous.build_phase1c_batch(batch)

    prefix = SyntheticCapacityPhase1CAdapter(run_id=_RUN, max_batch_commits=5)
    hasher = CapacityWorkloadHasher()
    for commit in commits[:5]:
        hasher.update(commit)
    prefix.build_phase1c_batch(commits[:5])
    restored, restored_digest = SyntheticCapacityPhase1CAdapter.resume_from_checkpoint(
        prefix.checkpoint_state(workload_prefix=hasher.snapshot()),
        expected_run_id=_RUN,
        expected_start_prefix_root=_START,
        max_batch_commits=5,
    )
    resumed_hasher = CapacityWorkloadHasher.resume_from_prefix(restored_digest)
    suffix = tuple(
        iter_capacity_commits(
            config,
            start_sequence=6,
            initial_stream_sequences=restored.source_stream_sequences,
        )
    )
    assert tuple(commit.sequence for commit in suffix) == tuple(range(6, 21))
    for offset in range(0, len(suffix), 5):
        batch = suffix[offset : offset + 5]
        for commit in batch:
            resumed_hasher.update(commit)
        restored.build_phase1c_batch(batch)

    assert restored.checkpoint_state(
        workload_prefix=resumed_hasher.finalize()
    ).canonical_sections == continuous.checkpoint_state(
        workload_prefix=continuous_hasher.finalize()
    ).canonical_sections


def test_adapter_batches_are_accepted_by_writer_across_repeated_seals(tmp_path: Path) -> None:
    paper_store_id = StoreId("SYNTHETIC_CAPACITY_WORKLOAD/paper-store")
    raw_store_id = StoreId("SYNTHETIC_CAPACITY_WORKLOAD/raw-store")
    config_identity = _sha(b"capacity-adapter-config")
    raw_config = RawStoreConfig(
        store_id=raw_store_id,
        lake_id=RawLakeId("SYNTHETIC_CAPACITY_WORKLOAD/raw-lake"),
        config_identity=config_identity,
    )
    paper_config = RepositoryConfig(
        store_id=paper_store_id,
        run_id=_RUN,
        mode=StorageMode.V4_NATIVE,
        run_identity=OpaqueIdentity(_sha(b"run")),
        config_identity=OpaqueIdentity(config_identity),
        code_identity=OpaqueIdentity(_sha(b"code")),
        runtime_identity=OpaqueIdentity(_sha(b"runtime")),
        start_prefix_root=_START,
    )
    raw_anchor = LocalAnchor.create(tmp_path / "raw-anchor.sqlite3", store_id=raw_store_id)
    paper_anchor = LocalAnchor.create(
        tmp_path / "paper-anchor.sqlite3", store_id=paper_store_id
    )
    raw = RawStore.create(
        tmp_path / "raw",
        anchor=raw_anchor,
        config=raw_config,
    )
    paper = StorageRepository.create(
        tmp_path / "paper",
        anchor=paper_anchor,
        config=paper_config,
    )
    staging = tmp_path / "staging"
    staging.mkdir()
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
    )
    adapter = SyntheticCapacityPhase1CAdapter(run_id=_RUN, max_batch_commits=2)
    workload_config = _config(commit_count=4)
    workload_manifest = build_capacity_workload_manifest(workload_config)
    commits = tuple(iter_capacity_commits(workload_config))

    first = adapter.build_phase1c_batch(commits[:2])
    assert type(first) is Phase1CBatch
    writer.append_batch(first)
    first_seal = writer.seal(adapter.checkpoint_state())
    writer.append_batch(adapter.build_phase1c_batch(commits[2:]))
    terminal = writer.seal(adapter.checkpoint_state())

    assert first_seal.paper_seal.manifest.generation == 1
    assert terminal.paper_seal.manifest.generation == 2
    assert terminal.binding.raw_record_count == 4
    assert writer.cursor.next_commit_sequence == 5
    assert paper.overlay_state.tail_commit_count == 0
    paper.close()
    raw.close()

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
    baseline = compare_capacity_native_exact(
        reopened_paper,
        DiskRawResolver(reopened_raw),
        workload_manifest,
        run_id=_RUN,
    )
    progress: list[dict[str, object]] = []
    oracle = compare_capacity_native_exact(
        reopened_paper,
        DiskRawResolver(reopened_raw),
        workload_manifest,
        run_id=_RUN,
        progress=lambda payload: progress.append(dict(payload)),
    )
    assert oracle == baseline
    assert oracle.commit_count == 4
    assert oracle.logical_row_count == workload_manifest.logical_row_count
    assert oracle.workload_sha256 == workload_manifest.workload_sha256
    assert oracle.market_gap_count == workload_config.market_gap_count
    assert [item["audit_event"] for item in progress] == ["STARTED", "COMPLETE"]
    assert [item["audited_commits"] for item in progress] == [0, 4]
    assert [item["audited_rows"] for item in progress] == [
        0,
        workload_manifest.logical_row_count,
    ]
    assert all(
        item["audit_progress_authority"] == "NON_AUTHORITATIVE_OBSERVABILITY_ONLY"
        for item in progress
    )
    reopened_paper.close()
    reopened_raw.close()
