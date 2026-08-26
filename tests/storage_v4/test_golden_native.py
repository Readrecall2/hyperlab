from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from pathlib import Path

import pytest

import hyperlab.paper.storage_v4.golden_runner as golden_runner_module
from hyperlab.paper.golden_v3 import GOLDEN_STREAM_NAMES, GoldenVerification
from hyperlab.paper.storage_v4.anchor import LocalAnchor
from hyperlab.paper.storage_v4.contracts import RawLakeId, StorageMode
from hyperlab.paper.storage_v4.golden_import import GoldenCommitAssembler, GoldenImportExpectations
from hyperlab.paper.storage_v4.golden_native import (
    GOLDEN_NATIVE_INPUT_TYPE,
    GOLDEN_NATIVE_SOURCE_ID,
    GoldenNativeBatch,
    GoldenNativeError,
    RematerializedGoldenRepository,
    compare_golden_native_exact,
    ingest_golden_native_batches,
    iter_golden_native_batches,
)
from hyperlab.paper.storage_v4.golden_runner import (
    GOLDEN_NATIVE_STATUS,
    OfflineGoldenNativeRunner,
)
from hyperlab.paper.storage_v4.manifest import OpaqueIdentity
from hyperlab.paper.storage_v4.phase1c_pipeline import (
    Phase1CBatchResult,
    Phase1CSealResult,
    Phase1CWriter,
)
from hyperlab.paper.storage_v4.raw_store import DiskRawResolver, RawStore, RawStoreConfig
from hyperlab.paper.storage_v4.repository import RepositoryConfig, StorageRepository
from hyperlab.paper.storage_v4.startup_trace import (
    STARTUP_TRACE_STATUS,
    StartupFileCategory,
)
from hyperlab.paper.storage_v4.types import Hash32, RunId, StoreId

SYNTHETIC_STORAGE_V4_WORKLOAD = True

_RUN = RunId("SYNTHETIC_STORAGE_V4_PHASE1C/golden-native")
_PAPER_STORE = StoreId("SYNTHETIC_STORAGE_V4_PHASE1C/golden-native-paper")
_RAW_STORE = StoreId("SYNTHETIC_STORAGE_V4_PHASE1C/golden-native-raw")
_RAW_LAKE = RawLakeId("SYNTHETIC_STORAGE_V4_PHASE1C/golden-native-lake")
_EXPORT_ROOT = Hash32(b"\x91" * 32)


def _hash(marker: int) -> str:
    return f"{marker:064x}"


def _canonical(row: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            dict(row),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def _fixture() -> dict[str, list[dict[str, object]]]:
    commit_hashes = (_hash(101), _hash(102), _hash(103))
    projection_hashes = (_hash(201), _hash(202), _hash(203), _hash(204))
    event_head_hashes = (_hash(301), _hash(302), _hash(303), _hash(304))
    input_ids = ("golden-input-1", "golden-input-2", "golden-input-3")
    projections = [
        {
            "event_head_hash": event_head_hashes[revision],
            "event_sequence": revision,
            "payload": {"state": "INITIAL" if revision == 0 else f"REVISION-{revision}"},
            "projection_hash": projection_hashes[revision],
            "revision": revision,
            "run_id": _RUN.value,
        }
        for revision in range(4)
    ]
    commits = [
        {
            "commit_hash": commit_hashes[index],
            "commit_sequence": index + 1,
            "event_hashes": [],
            "first_event_sequence": None,
            "input_id": input_ids[index],
            "last_event_sequence": None,
            "projection_hash": projection_hashes[index + 1],
            "projection_revision": index + 1,
            "run_id": _RUN.value,
        }
        for index in range(3)
    ]
    inbox = [
        {
            "commit_hash": commit_hashes[index],
            "commit_sequence": index + 1,
            "input_id": input_ids[index],
            "payload": {"kind": "RUN_START" if index == 0 else "TIMER"},
            "run_id": _RUN.value,
        }
        for index in range(3)
    ]
    terminal_projection = dict(projections[-1])
    terminal_projection.update(
        {
            "effective_status": "RUNNING",
            "status": "RUNNING",
            "updated_at": "2026-08-25T12:00:03Z",
        }
    )
    return {
        "schema": [{"kind": "metadata", "name": "paper_schema"}],
        "run": [{"commit_count": 3, "run_id": _RUN.value}],
        "inbox": inbox,
        "events": [],
        "ledger_transactions": [],
        "ledger_entries": [],
        "alerts": [{"code": "MARKET_GAP", "commit_sequence": 2, "run_id": _RUN.value}],
        "commits": commits,
        "projection_history": projections,
        "projection_current": [terminal_projection],
        "runtime_sessions": [],
        "incidents": [],
        "heads": [{"commit_count": 3, "run_id": _RUN.value}],
    }


def _expectations(streams: dict[str, list[dict[str, object]]]) -> GoldenImportExpectations:
    counts = tuple((name, len(streams[name])) for name in GOLDEN_STREAM_NAMES)
    return GoldenImportExpectations(
        run_id=_RUN,
        export_root=_EXPORT_ROOT,
        commit_count=3,
        row_count=sum(count for _, count in counts),
        stream_row_counts=counts,
    )


def _verification(
    tmp_path: Path,
    streams: dict[str, list[dict[str, object]]],
) -> GoldenVerification:
    descriptors = {
        name: {
            "logical_sha256": hashlib.sha256(
                b"".join(_canonical(row) for row in streams[name])
            ).hexdigest(),
            "row_count": len(streams[name]),
        }
        for name in GOLDEN_STREAM_NAMES
    }
    return GoldenVerification(
        export_root=tmp_path / "synthetic-golden-not-read",
        root_hash=_EXPORT_ROOT.hex(),
        manifest={"run_id": _RUN.value, "streams": descriptors},
    )


def _systems(
    tmp_path: Path,
) -> tuple[RawStore, StorageRepository, Path]:
    config_identity = Hash32(hashlib.sha256(b"golden-native-config").digest())
    raw_anchor = LocalAnchor.create(tmp_path / "raw-anchor.sqlite3", store_id=_RAW_STORE)
    paper_anchor = LocalAnchor.create(tmp_path / "paper-anchor.sqlite3", store_id=_PAPER_STORE)
    raw = RawStore.create(
        tmp_path / "raw",
        anchor=raw_anchor,
        config=RawStoreConfig(
            store_id=_RAW_STORE,
            lake_id=_RAW_LAKE,
            config_identity=config_identity,
        ),
    )
    paper = StorageRepository.create(
        tmp_path / "paper",
        anchor=paper_anchor,
        config=RepositoryConfig(
            store_id=_PAPER_STORE,
            run_id=_RUN,
            mode=StorageMode.V4_NATIVE,
            run_identity=OpaqueIdentity(Hash32(hashlib.sha256(b"run").digest())),
            config_identity=OpaqueIdentity(config_identity),
            code_identity=OpaqueIdentity(Hash32(hashlib.sha256(b"code").digest())),
            runtime_identity=OpaqueIdentity(Hash32(hashlib.sha256(b"runtime").digest())),
            start_prefix_root=_EXPORT_ROOT,
        ),
    )
    staging = tmp_path / "staging"
    staging.mkdir()
    return raw, paper, staging


def test_golden_native_batches_ingest_and_independent_13_stream_differential(
    tmp_path: Path,
) -> None:
    streams = _fixture()
    batches = tuple(
        iter_golden_native_batches(
            GoldenCommitAssembler(streams, _expectations(streams)),
            batch_size=2,
        )
    )
    assert [len(boundary.batch.source_frames) for boundary in batches] == [2, 1]
    assert [int(boundary.boundary_commit_sequence) for boundary in batches] == [2, 3]
    assert batches[0].checkpoint_state.adapter["processed_commits"] == 2
    assert batches[1].checkpoint_state.adapter["processed_commits"] == 3
    assert all(
        record.metadata.source_id == GOLDEN_NATIVE_SOURCE_ID
        and record.metadata.input_type == GOLDEN_NATIVE_INPUT_TYPE
        and record.metadata.venue_id is None
        for boundary in batches
        for record in boundary.batch.raw_records
    )

    raw, paper, staging = _systems(tmp_path)
    writer = Phase1CWriter(
        raw_store=raw,
        paper_repository=paper,
        staging_directory=staging,
    )
    callbacks: list[tuple[int, int, int]] = []

    def progress(
        boundary: GoldenNativeBatch,
        batch_result: Phase1CBatchResult,
        seal_result: Phase1CSealResult,
    ) -> None:
        callbacks.append(
            (
                int(boundary.boundary_commit_sequence),
                int(batch_result.cursor.next_commit_sequence) - 1,
                int(seal_result.paper_seal.checkpoint.covered_commit_sequence),
            )
        )

    ingestion = ingest_golden_native_batches(writer, iter(batches), progress=progress)
    assert callbacks == [(2, 2, 2), (3, 3, 3)]
    assert ingestion.commit_count == 3
    assert [int(w.commit_sequence) for w in ingestion.checkpoint_witnesses] == [2, 3]
    assert all(
        witness.bound_state_sha256 != witness.unbound_state_sha256
        for witness in ingestion.checkpoint_witnesses
    )
    assert raw.full_audit().records_read == 3
    assert paper.full_audit().checkpoints_read == 2

    verification = _verification(tmp_path, streams)

    def stream_factory(
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        return streams[name]

    resolver = DiskRawResolver(raw)
    differential = compare_golden_native_exact(
        paper,
        resolver,
        verification,
        ingestion,
        stream_factory=stream_factory,
    )
    assert differential.report["commits"] == 3
    assert differential.report["rows"] == sum(len(rows) for rows in streams.values())
    assert differential.report["market_gap_rows"] == 1
    assert differential.report["checkpoint_states_verified"] == 2
    assert differential.terminal_unbound_checkpoint_state == batches[-1].checkpoint_state

    divergent_streams = {name: list(rows) for name, rows in streams.items()}
    divergent_streams["alerts"] = [
        {**divergent_streams["alerts"][0], "code": "MUTATED_MARKET_GAP"}
    ]

    def divergent_stream_factory(
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        return divergent_streams[name]

    with pytest.raises(GoldenNativeError):
        compare_golden_native_exact(
            paper,
            resolver,
            verification,
            ingestion,
            stream_factory=divergent_stream_factory,
        )

    view = RematerializedGoldenRepository(
        paper,
        resolver,
        golden_export_root=_EXPORT_ROOT,
    )
    rematerialized = tuple(view.iter_historical_frames())
    assert rematerialized[0].previous_prefix_root == _EXPORT_ROOT
    assert sum(len(frame.rows) for frame in rematerialized) == differential.report["rows"]

    paper.close()
    raw.close()


def test_generic_golden_native_iterable_requires_an_exact_terminal_count() -> None:
    with pytest.raises(ValueError, match="require expected_commit_count"):
        tuple(iter_golden_native_batches(iter(()), batch_size=2))

    with pytest.raises(GoldenNativeError, match="ended before expected_commit_count"):
        tuple(
            iter_golden_native_batches(
                iter(()),
                batch_size=2,
                expected_commit_count=1,
            )
        )


def test_offline_golden_runner_closes_reopen_differential_and_census(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    streams = _fixture()
    verification = _verification(tmp_path, streams)
    expected_rows = sum(len(rows) for rows in streams.values())
    progress: list[dict[str, object]] = []
    overlay_journal_sizes: list[int] = []
    original_transient_bytes = golden_runner_module._transient_bytes

    def witness_transient_bytes(root: Path) -> int:
        observed = original_transient_bytes(root)
        for journal in root.rglob("overlay.sqlite3-journal"):
            if journal.is_file():
                size = journal.stat().st_size
                if size > 0:
                    overlay_journal_sizes.append(size)
        return observed

    monkeypatch.setattr(
        golden_runner_module,
        "_transient_bytes",
        witness_transient_bytes,
    )

    def assembler_factory(ignored: GoldenVerification) -> GoldenCommitAssembler:
        del ignored
        return GoldenCommitAssembler(streams, _expectations(streams))

    def stream_factory(
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        return streams[name]

    runner = OfflineGoldenNativeRunner(
        candidate_root=(tmp_path / "golden-candidate").absolute(),
        code_identity=Hash32(hashlib.sha256(b"test-code").digest()),
        batch_size=2,
        expected_commits=3,
        expected_rows=expected_rows,
        expected_streams=13,
        expected_market_gaps=1,
        assembler_factory=assembler_factory,
        stream_factory=stream_factory,
        rss_probe=lambda: 123_456,
        progress=lambda payload: progress.append(dict(payload)),
    )

    result = runner.run(verification)
    payload = result.payload()

    assert payload["status"] == GOLDEN_NATIVE_STATUS
    assert result.certification.paper_startup.segments_read == 0
    assert result.certification.paper_startup.tail_entries_replayed == 0
    assert result.certification.raw_startup.historical_segments_read == 0
    assert result.startup_file_trace.status == STARTUP_TRACE_STATUS
    assert result.startup_file_trace.historical_segment_open_count == 0
    assert {
        StartupFileCategory.RAW_MANIFEST,
        StartupFileCategory.PAPER_MANIFEST,
        StartupFileCategory.PAPER_CHECKPOINT,
        StartupFileCategory.PAPER_OVERLAY,
        StartupFileCategory.RAW_ANCHOR,
        StartupFileCategory.PAPER_ANCHOR,
    }.issubset({item.category for item in result.startup_file_trace.opens})
    assert all(
        "/segments/" not in item.relative_path
        for item in result.startup_file_trace.opens
    )
    assert payload["startup"]["file_access_trace"] == (
        result.startup_file_trace.payload()
    )
    assert result.certification.native_audit.commit_count == 3
    assert result.differential.report["rows"] == expected_rows
    assert result.audited_candidate_tree.root == result.candidate_root
    assert payload["integrity"]["audited_candidate_tree"] == (
        result.audited_candidate_tree.payload()
    )
    assert result.byte_census.raw_bytes > 0
    assert result.byte_census.raw_index_bytes > 0
    assert result.byte_census.raw_segments_bytes > 0
    assert result.byte_census.paper_incremental_bytes > 0
    assert result.byte_census.scratch_current_bytes == 0
    assert overlay_journal_sizes
    assert result.byte_census.scratch_peak_bytes >= max(overlay_journal_sizes)
    assert result.peak_rss_bytes == 123_456
    assert result.scratch_status == (
        "EXACT_RECOGNIZED_TRANSIENT_FILE_PEAK_AT_INSTRUMENTED_BOUNDARIES"
    )
    ingest_progress = [
        item for item in progress if item["phase"] == "golden_native_ingest"
    ]
    assert [item["commits_completed"] for item in ingest_progress] == [2, 3]
    assert all(item["commits_total"] == 3 for item in ingest_progress)
    assert all(item["workload"] == "GOLDEN_V3_NATIVE" for item in ingest_progress)
    assert all(
        item["workload_profile"] == GOLDEN_NATIVE_INPUT_TYPE
        for item in ingest_progress
    )
    assert all(
        item["workload_id"] == verification.root_hash for item in ingest_progress
    )
    assert [item["raw_segment_count"] for item in ingest_progress] == [1, 2]
    assert [item["paper_segment_count"] for item in ingest_progress] == [1, 2]
    assert [item["segment_count"] for item in ingest_progress] == [2, 4]
    assert [item["checkpoint_count"] for item in ingest_progress] == [1, 2]
    assert ingest_progress[-1]["logical_rows_completed"] == expected_rows
    assert all(item["logical_rows_total"] == expected_rows for item in ingest_progress)
    assert all(item["bytes_written"] is None for item in ingest_progress)
    assert all(
        item["bytes_written_status"] == "UNAVAILABLE_WRITE_BYTE_PROBE"
        for item in ingest_progress
    )
    assert progress[-1]["phase"] == "golden_native_complete"
    assert progress[-1]["segment_count"] == 4
    assert progress[-1]["checkpoint_count"] == 2
