from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pytest

import hyperlab.paper.storage_v4.phase1c_progress as audit_progress_module
from hyperlab.paper.storage_v4.anchor import LocalAnchor
from hyperlab.paper.storage_v4.canonical import build_commit_logical
from hyperlab.paper.storage_v4.checkpoint import CheckpointState
from hyperlab.paper.storage_v4.contracts import RawLakeId, StorageMode
from hyperlab.paper.storage_v4.manifest import OpaqueIdentity
from hyperlab.paper.storage_v4.phase1c_progress import AUDIT_PROGRESS_AUTHORITY
from hyperlab.paper.storage_v4.raw_segment import (
    RawRecordMetadata,
    RawSegmentArtifact,
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

SYNTHETIC_STORAGE_V4_WORKLOAD = True


def _raw_artifact(
    tmp_path: Path,
    *,
    lake_id: RawLakeId,
    sequence: int,
) -> RawSegmentArtifact:
    staging = tmp_path / f"raw-staging-{sequence}"
    staging.mkdir()
    writer = RawSegmentWriter(staging, lake_id=lake_id)
    writer.append(
        f'{{"sequence":{sequence},"synthetic":true}}'.encode(),
        RawRecordMetadata(
            record_id=f"input-{sequence}",
            source_id="synthetic-source",
            venue_id="SYNTHETIC",
            input_type="PUBLIC_MARKET_EVENT",
            source_stream_id=StreamId("wire"),
            source_first_sequence=EventSequence(sequence),
            source_last_sequence=EventSequence(sequence),
            arrival_sequence=EventSequence(sequence),
            source_timestamp=f"2026-01-01T00:00:{sequence:02d}Z",
            received_timestamp=f"2026-01-01T00:00:{sequence:02d}Z",
        ),
    )
    return writer.seal()


def _paper_state(sequence: int) -> CheckpointState:
    return CheckpointState(
        adapter={"sequence": sequence},
        ledger={"synthetic": True},
        projection={"sequence": sequence},
        sessions={"closed": []},
        incidents={"open": []},
        cursors={"commit": sequence},
        stream_heads={"events": sequence},
    )


def _append_paper(repository: StorageRepository, sequence: int) -> None:
    frame = CommitFrame(
        run_id=repository.config.run_id,
        commit_sequence=CommitSequence(sequence),
        previous_prefix_root=repository.overlay_state.head_prefix_root,
        rows=(
            LogicalRow(
                stream_id=StreamId("events"),
                ordinal=CommitOrdinal(0),
                value={"sequence": sequence, "synthetic": True},
            ),
        ),
    )
    assert repository.append(frame)
    assert repository.overlay_state.head_prefix_root == build_commit_logical(frame).prefix_root


def _assert_common_progress(events: list[dict[str, object]], *, phase: str) -> None:
    assert [event["audit_event"] for event in events] == [
        "STARTED",
        "HEARTBEAT",
        "HEARTBEAT",
        "COMPLETE",
    ]
    assert [event["heartbeat_sequence"] for event in events] == [0, 1, 2, 3]
    assert [event["phase_elapsed_ns"] for event in events] == [
        0,
        31_000_000_000,
        62_000_000_000,
        93_000_000_000,
    ]
    assert all(event["phase"] == phase for event in events)
    assert all(event["audit_progress_authority"] == AUDIT_PROGRESS_AUTHORITY for event in events)
    assert all(type(event["phase_started_at_unix_ns"]) is int for event in events)
    assert events[-1]["status"] == "COMPLETE"


def test_raw_full_audit_progress_is_bounded_monotone_and_result_neutral(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_id = StoreId("SYNTHETIC_AUDIT_PROGRESS/raw")
    lake_id = RawLakeId("SYNTHETIC_AUDIT_PROGRESS/lake")
    config = RawStoreConfig(
        store_id=store_id,
        lake_id=lake_id,
        config_identity=Hash32(b"\x41" * 32),
    )
    anchor = LocalAnchor.create(tmp_path / "raw-anchor.sqlite3", store_id=store_id)
    store = RawStore.create(tmp_path / "raw", anchor=anchor, config=config)
    store.seal(_raw_artifact(tmp_path, lake_id=lake_id, sequence=1))
    store.seal(_raw_artifact(tmp_path, lake_id=lake_id, sequence=2))
    baseline = store.full_audit()
    ticks = iter((0, 31_000_000_000, 62_000_000_000, 93_000_000_000))
    monkeypatch.setattr(audit_progress_module, "monotonic_ns", lambda: next(ticks))
    events: list[dict[str, object]] = []

    observed = store.full_audit(progress=lambda payload: events.append(dict(payload)))

    assert observed == baseline
    _assert_common_progress(events, phase="raw_full_audit")
    assert [event["audited_segments"] for event in events] == [0, 1, 2, 2]
    assert [event["audited_records"] for event in events] == [0, 1, 2, 2]
    assert all(event["segments_total"] == 2 for event in events)
    assert all(event["records_total"] == 2 for event in events)
    store.close()


@pytest.mark.parametrize(
    ("failure_event", "expected_calls"),
    [
        ("STARTED", ["STARTED"]),
        ("HEARTBEAT", ["STARTED", "HEARTBEAT"]),
    ],
)
def test_raw_full_audit_progress_callback_failure_is_result_neutral(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_event: str,
    expected_calls: list[str],
) -> None:
    store_id = StoreId(
        f"SYNTHETIC_AUDIT_PROGRESS/fail-soft/{failure_event}"
    )
    lake_id = RawLakeId(
        f"SYNTHETIC_AUDIT_PROGRESS/lake/{failure_event}"
    )
    config = RawStoreConfig(
        store_id=store_id,
        lake_id=lake_id,
        config_identity=Hash32(b"\x42" * 32),
    )
    anchor = LocalAnchor.create(tmp_path / "raw-anchor.sqlite3", store_id=store_id)
    store = RawStore.create(tmp_path / "raw", anchor=anchor, config=config)
    store.seal(_raw_artifact(tmp_path, lake_id=lake_id, sequence=1))
    store.seal(_raw_artifact(tmp_path, lake_id=lake_id, sequence=2))
    baseline = store.full_audit()
    ticks = iter((0, 31_000_000_000))
    monkeypatch.setattr(audit_progress_module, "monotonic_ns", lambda: next(ticks))
    calls: list[str] = []

    def fail_once(payload: Mapping[str, object]) -> None:
        event = str(payload["audit_event"])
        calls.append(event)
        if event == failure_event:
            raise RuntimeError("synthetic telemetry sink failure")

    observed = store.full_audit(progress=fail_once)

    assert observed == baseline
    assert calls == expected_calls
    store.close()


def test_paper_full_audit_progress_is_bounded_monotone_and_result_neutral(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_id = StoreId("SYNTHETIC_AUDIT_PROGRESS/paper")
    config = RepositoryConfig(
        store_id=store_id,
        run_id=RunId("SYNTHETIC_AUDIT_PROGRESS/run"),
        mode=StorageMode.V4_NATIVE,
        run_identity=OpaqueIdentity(Hash32(b"\x51" * 32)),
        config_identity=OpaqueIdentity(Hash32(b"\x52" * 32)),
        code_identity=OpaqueIdentity(Hash32(b"\x53" * 32)),
        runtime_identity=OpaqueIdentity(Hash32(b"\x54" * 32)),
        start_prefix_root=Hash32(b"\x00" * 32),
    )
    anchor = LocalAnchor.create(tmp_path / "paper-anchor.sqlite3", store_id=store_id)
    repository = StorageRepository.create(
        tmp_path / "paper",
        anchor=anchor,
        config=config,
    )
    for sequence in (1, 2):
        _append_paper(repository, sequence)
        repository.seal(
            checkpoint_state=_paper_state(sequence),
            cumulative_stream_counts=((StreamId("events"), sequence),),
            historical_commit_count=sequence,
        )
    baseline = repository.full_audit()
    ticks = iter((0, 31_000_000_000, 62_000_000_000, 93_000_000_000))
    monkeypatch.setattr(audit_progress_module, "monotonic_ns", lambda: next(ticks))
    events: list[dict[str, object]] = []

    observed = repository.full_audit(
        progress=lambda payload: events.append(dict(payload)),
    )

    assert observed == baseline
    _assert_common_progress(events, phase="paper_full_audit")
    assert [event["audited_segments"] for event in events] == [0, 1, 2, 2]
    assert [event["audited_commits"] for event in events] == [0, 1, 2, 2]
    assert [event["audited_rows"] for event in events] == [0, 1, 2, 2]
    assert all(event["segments_total"] == 2 for event in events)
    assert all(event["commits_total"] == 2 for event in events)
    assert all(event["rows_total"] == 2 for event in events)
    repository.close()

