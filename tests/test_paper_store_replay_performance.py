from __future__ import annotations

import gc
import hashlib
import io
import json
import os
import queue
import shutil
import signal
import sqlite3
import subprocess
import sys
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

import hyperlab.paper.engine as engine_module
import hyperlab.paper.store as store_module
from hyperlab.paper import (
    ConcurrentWriteError,
    IntegrityError,
    MarketEvent,
    PaperEngine,
    PaperEvent,
    PaperEventType,
    PaperExecutionConfig,
    PaperProjection,
    PaperRiskLimits,
    PaperRunConfig,
    PaperStore,
)
from hyperlab.paper.runtime import replay_paper_run
from hyperlab.paper.store import IntegrityIssue
from scripts import diagnose_paper_replay_copy as replay_diagnostic

_START = datetime(2026, 8, 21, 12, tzinfo=UTC)
_INSTRUMENT = "HYPERLIQUID:BTC:perp"


def _config() -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="replay_connection_fixture",
        strategy_hash="a" * 64,
        parameters={"fixture": "SYNTHETIC_REPLAY_CONNECTION"},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(
            calibration_status="SYNTHETIC",
            source="deterministic-test-fixture",
        ),
        risk=PaperRiskLimits(),
        seed=17,
        initial_cash=Decimal("100000"),
        validation_started_at=_START,
        data_calibration_status="SYNTHETIC",
        data_source="deterministic-test-fixture",
    )


def _market(sequence: int) -> MarketEvent:
    return MarketEvent.create(
        received_at=_START + timedelta(seconds=sequence),
        instrument=_INSTRUMENT,
        bid_price=Decimal("100"),
        ask_price=Decimal("101"),
        bid_depth=Decimal("100"),
        ask_depth=Decimal("100"),
        source_sequence=sequence,
    )


def _build_journal(
    path: Path,
    *,
    historical_replay_only: bool,
    temporary_directory: TemporaryDirectory[str] | None = None,
) -> tuple[PaperStore, PaperRunConfig]:
    config = _config()
    if historical_replay_only:
        if temporary_directory is None:
            raise ValueError("historical replay fixture requires TemporaryDirectory ownership")
        store = PaperStore._create_temporary_historical_replay(
            temporary_directory,
            filename=path.name,
        )
        engine = PaperEngine._for_historical_replay(store, config)
    else:
        store = PaperStore(path)
        engine = PaperEngine(store, config)
    engine.start()
    for sequence in range(1, 5):
        engine.process_market(_market(sequence))
    engine.post_funding(
        instrument=_INSTRUMENT,
        amount=Decimal("0"),
        occurred_at=_START + timedelta(seconds=5),
        source_event_id="c" * 64,
    )
    engine.pause(
        as_of=_START + timedelta(seconds=6),
        reason="synthetic replay connection fixture",
        operator_artifact_hash="d" * 64,
    )
    return store, config


def _input_identity(store: PaperStore, run_id: str) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            record.input_id,
            record.payload,
            record.payload_hash,
            record.first_event_sequence,
            record.last_event_sequence,
            record.commit_sequence,
            record.commit_hash,
        )
        for record in store.iter_inputs(run_id)
    )


def test_projection_canonical_record_hashes_the_existing_serialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, config = _build_journal(
        tmp_path / "canonical-record-source.sqlite3",
        historical_replay_only=False,
    )
    try:
        projection = store.get_projection(config.run_id)
    finally:
        store.close()
    expected_payload = projection.to_dict()
    expected_json = store_module.canonical_json(expected_payload)
    expected_hash = hashlib.sha256(expected_json.encode("utf-8")).hexdigest()
    canonical_json_calls = 0
    original_canonical_json = store_module.canonical_json

    def count_canonical_json(value: object) -> str:
        nonlocal canonical_json_calls
        canonical_json_calls += 1
        return original_canonical_json(value)

    def forbid_recanonicalization(_value: object) -> str:
        raise AssertionError("canonical record hash must reuse its existing JSON bytes")

    monkeypatch.setattr(store_module, "canonical_json", count_canonical_json)
    monkeypatch.setattr(store_module, "canonical_sha256", forbid_recanonicalization)

    payload, payload_json, payload_hash = store_module._canonical_record(
        projection,
        label="projection",
    )

    assert canonical_json_calls == 1
    assert payload == expected_payload
    assert payload_json == expected_json
    assert payload_hash == expected_hash


def test_disposable_historical_replay_store_reuses_memory_connection_and_closes(
    tmp_path: Path,
) -> None:
    temporary_directory = TemporaryDirectory(dir=tmp_path)
    database = Path(temporary_directory.name) / "disposable-replay.sqlite3"
    store, config = _build_journal(
        database,
        historical_replay_only=True,
        temporary_directory=temporary_directory,
    )
    connection = store._connect()

    assert store._connect() is connection
    assert store._read_connection() is connection
    assert int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) == 1
    assert str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower() == "memory"
    assert int(connection.execute("PRAGMA synchronous").fetchone()[0]) == 0
    assert int(connection.execute("PRAGMA query_only").fetchone()[0]) == 0
    assert store.inspect_integrity_readonly(config.run_id).ok is True

    store.close()
    store.close()
    with pytest.raises(RuntimeError, match="temporary historical replay store is closed"):
        store._connect()
    with pytest.raises(sqlite3.ProgrammingError):
        connection.execute("SELECT 1")
    database.unlink()
    assert not database.exists()

    temporary_directory.cleanup()


def test_direct_historical_flag_cannot_target_a_persistent_path(tmp_path: Path) -> None:
    database = tmp_path / "must-not-be-created.sqlite3"
    with pytest.raises(
        ValueError,
        match="restricted to the internal temporary-store factory",
    ):
        PaperStore(database, historical_replay_only=True)
    assert not database.exists()


def test_temporary_replay_store_cleanup_preserves_primary_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "source.sqlite3"
    source_store, config = _build_journal(
        database,
        historical_replay_only=False,
    )
    engine = PaperEngine(source_store, config)
    original_iter_inputs = source_store.iter_inputs
    original_close = PaperStore.close
    replay_connections: list[sqlite3.Connection] = []
    replay_directories: list[Path] = []
    directory_cleanup_calls = 0

    class CleanupFailureTemporaryDirectory(TemporaryDirectory[str]):
        def cleanup(self) -> None:
            nonlocal directory_cleanup_calls
            super().cleanup()
            directory_cleanup_calls += 1
            if directory_cleanup_calls == 1:
                raise OSError("synthetic temporary directory cleanup failure")

    def malformed_inputs(run_id: str) -> Iterator[object]:
        yield from original_iter_inputs(run_id)
        raise ValueError("malformed historical input fixture")

    def close_then_report_failure(store: PaperStore) -> None:
        if store._historical_replay_only:
            connection = store._replay_connection
            assert connection is not None
            replay_connections.append(connection)
            replay_directories.append(store.path.parent)
            original_close(store)
            raise OSError("synthetic replay close failure")
        original_close(store)

    monkeypatch.setattr(source_store, "iter_inputs", malformed_inputs)
    monkeypatch.setattr(PaperStore, "close", close_then_report_failure)
    monkeypatch.setattr(
        engine_module,
        "TemporaryDirectory",
        CleanupFailureTemporaryDirectory,
    )

    try:
        with pytest.raises(
            ValueError,
            match="malformed historical input fixture",
        ) as failure:
            engine.verify_input_replay()

        assert len(replay_connections) == 1
        assert directory_cleanup_calls == 1
        with pytest.raises(sqlite3.ProgrammingError):
            replay_connections[0].execute("SELECT 1")
        assert replay_directories
        assert all(not directory.exists() for directory in replay_directories)
        assert any(
            "temporary replay-store cleanup also failed: OSError: synthetic replay close failure" in note
            for note in getattr(failure.value, "__notes__", ())
        )
        assert any(
            "temporary replay-directory cleanup also failed: "
            "OSError: synthetic temporary directory cleanup failure" in note
            for note in getattr(failure.value, "__notes__", ())
        )
    finally:
        source_store.close()


def test_normal_store_keeps_delete_full_durability_and_query_only_reads(
    tmp_path: Path,
) -> None:
    database = tmp_path / "normal.sqlite3"
    store, config = _build_journal(database, historical_replay_only=False)
    expected_projection = store.get_projection(config.run_id).to_dict()
    writer = store._connect()
    reader = store._read_connection()
    try:
        assert int(writer.execute("PRAGMA foreign_keys").fetchone()[0]) == 1
        assert str(writer.execute("PRAGMA journal_mode").fetchone()[0]).lower() == "delete"
        assert int(writer.execute("PRAGMA synchronous").fetchone()[0]) == 2
        assert int(reader.execute("PRAGMA query_only").fetchone()[0]) == 1
        with pytest.raises(sqlite3.OperationalError):
            reader.execute("UPDATE paper_schema SET version=version")
    finally:
        reader.close()
        writer.close()

    reopened = PaperStore(database, initialize=False)
    assert reopened.get_projection(config.run_id).to_dict() == expected_projection
    assert reopened.inspect_integrity_readonly(config.run_id).ok is True


def test_replay_connection_mode_preserves_exact_journal_and_projection(
    tmp_path: Path,
) -> None:
    normal, config = _build_journal(
        tmp_path / "normal-equivalence.sqlite3",
        historical_replay_only=False,
    )
    temporary_directory = TemporaryDirectory(dir=tmp_path)
    replay, replay_config = _build_journal(
        Path(temporary_directory.name) / "replay-equivalence.sqlite3",
        historical_replay_only=True,
        temporary_directory=temporary_directory,
    )
    assert replay_config == config
    assert normal.inspect_integrity_readonly(config.run_id).ok is True
    assert replay.inspect_integrity_readonly(config.run_id).ok is True
    assert replay.get_run(config.run_id).head_identity == normal.get_run(config.run_id).head_identity
    assert replay.get_projection(config.run_id).to_dict() == normal.get_projection(config.run_id).to_dict()
    assert _input_identity(replay, config.run_id) == _input_identity(normal, config.run_id)
    assert tuple(
        (event.hash_payload(), event.event_hash) for event in replay.iter_events(config.run_id)
    ) == tuple((event.hash_payload(), event.event_hash) for event in normal.iter_events(config.run_id))
    assert tuple(
        (entry.entry, entry.entry_hash) for entry in replay.iter_ledger_entries(config.run_id)
    ) == tuple((entry.entry, entry.entry_hash) for entry in normal.iter_ledger_entries(config.run_id))
    replay.close()
    temporary_directory.cleanup()


def test_attested_input_replay_does_not_rescan_each_reconcile_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, config = _build_journal(
        tmp_path / "reconcile-source.sqlite3",
        historical_replay_only=False,
    )
    PaperEngine(source, config).reconcile(as_of=_START + timedelta(seconds=7))
    original_verify = PaperEngine._verify_durable_state
    replay_prefixes: list[int] = []

    def counted_verify(
        engine: PaperEngine,
        *,
        should_stop: object = None,
    ) -> object:
        if engine.store.historical_replay_only:
            replay_prefixes.append(engine.store.get_run(engine.run_id).commit_sequence)
        return original_verify(engine, should_stop=should_stop)  # type: ignore[arg-type]

    monkeypatch.setattr(PaperEngine, "_verify_durable_state", counted_verify)

    verified = replay_paper_run(source, config.run_id)

    assert verified.projection_hash == source.get_projection(config.run_id).canonical_hash
    # The fresh replay engine verifies RUN_START normally. The later durable
    # RECONCILE must consume the already-complete source integrity attestation
    # instead of recursively replaying its complete prefix.
    assert replay_prefixes == [1]

    replay_prefixes.clear()
    assert (
        PaperEngine(source, config).verify_input_replay().to_dict()
        == source.get_projection(config.run_id).to_dict()
    )
    # Standalone callers have no source attestation, so retain the original
    # full-prefix verification contract.
    assert len(replay_prefixes) == 2
    assert replay_prefixes[0] == 1
    assert replay_prefixes[1] > replay_prefixes[0]


def test_attested_replay_finally_rejects_structurally_valid_ledger_semantic_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    original_transaction_ledger = engine_module.transaction_ledger_amounts

    def corrupt_successful_reconcile(
        projection: PaperProjection,
        event: PaperEvent,
    ) -> tuple[tuple[str, Decimal], ...]:
        entries = original_transaction_ledger(projection, event)
        if event.event_type is PaperEventType.RECONCILIATION_SUCCEEDED:
            return (
                *entries,
                ("asset:cash", Decimal("1")),
                ("equity:synthetic_semantic_corruption", Decimal("-1")),
            )
        return entries

    monkeypatch.setattr(
        engine_module,
        "transaction_ledger_amounts",
        corrupt_successful_reconcile,
    )
    monkeypatch.setattr(
        store_module,
        "transaction_ledger_amounts",
        corrupt_successful_reconcile,
    )

    reference_directory = TemporaryDirectory(dir=tmp_path)
    reference_store = PaperStore._create_temporary_historical_replay(
        reference_directory,
        filename="reference-semantic-drift.sqlite3",
    )
    reference_engine = PaperEngine._for_historical_replay(
        reference_store,
        config,
    )
    reference_engine.start()
    reference_engine._reconcile(
        as_of=_START + timedelta(seconds=7),
        verification=reference_engine._verified_historical_replay_prefix(),
    )
    assert reference_store.inspect_integrity_readonly(config.run_id).ok is True
    assert reference_engine._ledger_reconciliation_errors(reference_engine.projection()) == (
        "ledger cash differs from the replayed projection",
    )

    reference_engine.reconcile(as_of=_START + timedelta(seconds=8))
    assert any(
        event.event.event_type is PaperEventType.RECONCILIATION_FAILED
        for event in reference_store.iter_events(config.run_id)
    )
    reference_store.close()
    reference_directory.cleanup()

    optimized_directory = TemporaryDirectory(dir=tmp_path)
    optimized_store = PaperStore._create_temporary_historical_replay(
        optimized_directory,
        filename="optimized-semantic-drift.sqlite3",
    )
    optimized_engine = PaperEngine._for_historical_replay(
        optimized_store,
        config,
    )
    optimized_engine.start()
    for seconds in (7, 8):
        optimized_engine._reconcile(
            as_of=_START + timedelta(seconds=seconds),
            verification=optimized_engine._verified_historical_replay_prefix(),
        )
    assert optimized_store.inspect_integrity_readonly(config.run_id).ok is True
    assert optimized_engine._ledger_reconciliation_errors(optimized_engine.projection()) == (
        "ledger cash differs from the replayed projection",
    )
    source_path = optimized_store.path
    optimized_store.close()

    source = PaperStore(source_path, initialize=False)
    original_reconciliation = PaperEngine._ledger_reconciliation_errors
    final_checks: list[tuple[int, tuple[str, ...]]] = []

    def counted_reconciliation(
        engine: PaperEngine,
        projection: PaperProjection,
        *,
        should_stop: object = None,
    ) -> tuple[str, ...]:
        errors = original_reconciliation(
            engine,
            projection,
            should_stop=should_stop,  # type: ignore[arg-type]
        )
        if engine.store.historical_replay_only:
            final_checks.append(
                (
                    engine.store.get_run(engine.run_id).commit_sequence,
                    errors,
                )
            )
        return errors

    monkeypatch.setattr(
        PaperEngine,
        "_ledger_reconciliation_errors",
        counted_reconciliation,
    )
    with pytest.raises(
        ValueError,
        match="ledger cash differs from the replayed projection",
    ):
        replay_paper_run(source, config.run_id)

    assert final_checks[-1] == (
        3,
        ("ledger cash differs from the replayed projection",),
    )
    source.close()
    del source, optimized_engine, optimized_store
    gc.collect()
    optimized_directory.cleanup()


def test_disposable_engine_projection_result_is_copy_safe_and_exact(tmp_path: Path) -> None:
    temporary_directory = TemporaryDirectory(dir=tmp_path)
    store, config = _build_journal(
        Path(temporary_directory.name) / "cached-replay.sqlite3",
        historical_replay_only=True,
        temporary_directory=temporary_directory,
    )
    with pytest.raises(
        ValueError,
        match="requires the internal replay engine factory",
    ):
        PaperEngine(store, config)
    cached = PaperEngine._for_historical_replay(store, config)
    first = cached.projection()
    durable = store.get_projection(config.run_id)

    assert first.to_dict() == durable.to_dict()
    first.cash += Decimal("1")
    assert cached.projection().to_dict() == durable.to_dict()
    store.close()
    temporary_directory.cleanup()


def test_disposable_replay_full_integrity_failure_is_not_skipped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, config = _build_journal(
        tmp_path / "final-integrity-source.sqlite3",
        historical_replay_only=False,
    )
    inspect_integrity = PaperStore.inspect_integrity_readonly
    replay_integrity_calls = 0

    def corrupted_replay_integrity(
        store: PaperStore,
        run_id: str,
    ) -> object:
        nonlocal replay_integrity_calls
        report = inspect_integrity(store, run_id)
        if not store.historical_replay_only:
            return report
        replay_integrity_calls += 1
        return replace(
            report,
            ok=False,
            issues=(
                IntegrityIssue(
                    "SYNTHETIC_REPLAY_HISTORY_CORRUPTION",
                    "targeted final replay-store integrity fixture",
                ),
            ),
        )

    monkeypatch.setattr(
        PaperStore,
        "inspect_integrity_readonly",
        corrupted_replay_integrity,
    )

    with pytest.raises(IntegrityError) as failure:
        replay_paper_run(source, config.run_id)

    assert replay_integrity_calls == 1
    assert {issue.code for issue in failure.value.report.issues} == {"SYNTHETIC_REPLAY_HISTORY_CORRUPTION"}


def test_final_replay_integrity_failure_rechecks_stable_source_head(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, config = _build_journal(
        tmp_path / "final-integrity-concurrent-source.sqlite3",
        historical_replay_only=False,
    )
    source_engine = PaperEngine(source, config)
    inspect_integrity = PaperStore.inspect_integrity_readonly
    advanced = False

    def advance_source_before_failure(
        store: PaperStore,
        run_id: str,
    ) -> object:
        nonlocal advanced
        report = inspect_integrity(store, run_id)
        if not store.historical_replay_only:
            return report
        source_engine.process_market(_market(99))
        advanced = True
        return replace(
            report,
            ok=False,
            issues=(
                IntegrityIssue(
                    "SYNTHETIC_REPLAY_HISTORY_CORRUPTION",
                    "targeted concurrent final replay-store fixture",
                ),
            ),
        )

    monkeypatch.setattr(
        PaperStore,
        "inspect_integrity_readonly",
        advance_source_before_failure,
    )

    with pytest.raises(ConcurrentWriteError, match="durable head changed"):
        replay_paper_run(source, config.run_id)

    assert advanced is True


def test_reconcile_benchmark_exercises_reference_and_optimized_paths() -> None:
    repository = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [
            sys.executable,
            str(repository / "scripts" / "benchmark_paper_replay_store.py"),
            "--commits",
            "1000",
            "--reconcile-every",
            "250",
            "--ledger-every",
            "10",
            "--mode",
            "both",
            "--worker-order",
            "optimized-first",
        ],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    report = json.loads(completed.stdout)
    workers = {worker["mode"]: worker for worker in report["workers"]}

    assert report["source"]["reconcile_input_count"] == 4
    assert report["source"]["ledger_entry_count"] > 100
    assert report["worker_order"] == ["optimized", "reference"]
    assert workers["optimized"]["instrumentation"] == {
        "bounded_prefix_certification_count": 4,
        "final_full_integrity_count": 1,
        "historical_ledger_reconciliation_count": 2,
        "reference_full_prefix_rescan_count": 0,
        "reference_prefix_commit_work": 0,
    }
    reference = workers["reference"]["instrumentation"]
    assert reference["bounded_prefix_certification_count"] == 0
    assert reference["final_full_integrity_count"] == 1
    assert reference["historical_ledger_reconciliation_count"] == 6
    assert reference["reference_full_prefix_rescan_count"] == 4
    assert reference["reference_prefix_commit_work"] > 0
    assert all(workers["optimized"]["exact_target_equality"].values())
    assert all(workers["reference"]["exact_target_equality"].values())
    assert workers["optimized"]["source_sha256"] == workers["reference"]["source_sha256"]
    assert workers["optimized"]["target_head_identity"] == workers["reference"]["target_head_identity"]


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _diagnostic_command(
    *,
    repository: Path,
    database_copy: Path,
    forbidden_original: Path,
    scratch_root: Path,
    run_id: str,
    expected_sha256: str,
    progress_every_rows: int = 1,
    instrumentation_mode: str = "V2",
) -> list[str]:
    return [
        sys.executable,
        str(repository / "scripts" / "diagnose_paper_replay_copy.py"),
        "--database-copy",
        str(database_copy),
        "--forbid-original",
        str(forbidden_original),
        "--scratch-root",
        str(scratch_root),
        "--run-id",
        run_id,
        "--expected-sha256",
        expected_sha256,
        "--wall-limit-seconds",
        "60",
        "--progress-every-rows",
        str(progress_every_rows),
        "--instrumentation-mode",
        instrumentation_mode,
    ]


_FAKE_REPLAY_INPUT_TYPES = (
    "RUN_START",
    "PUBLIC_MARKET_EVENT",
    "PUBLIC_MARKET_EVENT",
    "PUBLIC_MARKET_EVENT",
    "PUBLIC_MARKET_EVENT",
    "PUBLIC_FUNDING_SETTLEMENT",
    "OPERATOR_PAUSE",
    "RECONCILE",
)


def _fake_replay_timing_v2(*, completed_target_commits: int) -> dict[str, object]:
    expected_target_commits = len(_FAKE_REPLAY_INPUT_TYPES)
    if not 0 <= completed_target_commits <= expected_target_commits:
        raise ValueError("fake completed commit count is outside its source prefix")
    type_counts: dict[str, int] = {}
    for input_type in _FAKE_REPLAY_INPUT_TYPES[:completed_target_commits]:
        type_counts[input_type] = type_counts.get(input_type, 0) + 1
    source_tail = [
        {"commit_sequence": sequence, "input_type": input_type}
        for sequence, input_type in enumerate(_FAKE_REPLAY_INPUT_TYPES, start=1)
    ]
    completed_input_tail: list[dict[str, object]] = []
    operation_counts: dict[str, int] = {}
    for sequence, input_type in enumerate(
        _FAKE_REPLAY_INPUT_TYPES[:completed_target_commits],
        start=1,
    ):
        operations = {
            "input_dispatch": {
                "span_count": 1,
                "thread_cpu_seconds": 0.0,
                "wall_seconds": 0.0,
            }
        }
        if input_type == "RECONCILE":
            for operation in (
                "historical_head_integrity",
                "historical_prefix_certification",
                "historical_reconcile",
            ):
                operations[operation] = {
                    "span_count": 1,
                    "thread_cpu_seconds": 0.0,
                    "wall_seconds": 0.0,
                }
        for operation, timing in operations.items():
            operation_counts[operation] = operation_counts.get(operation, 0) + int(
                timing["span_count"]
            )
        completed_input_tail.append(
            {
                "commit_sequence": sequence,
                "input_type": input_type,
                "operations": operations,
                "projection_characters": 1,
                "thread_cpu_seconds": 0.0,
                "wall_seconds": 0.0,
                "zlib_bytes": 1,
            }
        )
    active_commit_sequence = (
        completed_target_commits + 1
        if completed_target_commits < expected_target_commits
        else None
    )
    return {
        "active_input": (
            {
                "commit_sequence": active_commit_sequence,
                "input_type": _FAKE_REPLAY_INPUT_TYPES[active_commit_sequence - 1],
                "wall_seconds": 1.0,
            }
            if active_commit_sequence is not None
            else None
        ),
        "active_operation": (
            {"name": "historical_head_integrity", "wall_seconds": 1.0}
            if active_commit_sequence is not None
            else None
        ),
        "completed_input_tail": completed_input_tail,
        "completed_target_commits": completed_target_commits,
        "expected_target_commits": expected_target_commits,
        "input_type_timings": {
            input_type: {
                "max_wall_seconds": 0.0,
                "span_count": count,
                "thread_cpu_seconds": 0.0,
                "wall_seconds": 0.0,
            }
            for input_type, count in type_counts.items()
        },
        "last_completed_input": (
            source_tail[completed_target_commits - 1]
            if completed_target_commits
            else None
        ),
        "operation_timings": {
            operation: {
                "max_wall_seconds": 0.0,
                "span_count": count,
                "thread_cpu_seconds": 0.0,
                "wall_seconds": 0.0,
            }
            for operation, count in operation_counts.items()
        },
        "projection_history_sizes": {
            "latest_commit_sequence": completed_target_commits,
            "latest_projection_characters": 1,
            "latest_zlib_bytes": 1,
            "max_projection_characters": 1,
            "max_zlib_bytes": 1,
        },
        "remaining_target_commits": expected_target_commits - completed_target_commits,
        "slowest_completed_inputs": [
            {
                "commit_sequence": item["commit_sequence"],
                "input_type": item["input_type"],
                "thread_cpu_seconds": 0.0,
                "wall_seconds": 0.0,
            }
            for item in completed_input_tail
        ],
        "source_first_input": source_tail[0],
        "source_tail": source_tail,
        "target_database_bytes": 1,
        "version": 2,
    }


def _fake_replay_progress(
    *,
    completed_target_commits: int,
) -> dict[str, object]:
    timing = _fake_replay_timing_v2(
        completed_target_commits=completed_target_commits
    )
    active_timing = timing["active_input"]
    active = (
        {
            "commit_sequence": active_timing["commit_sequence"],
            "input_type": active_timing["input_type"],
            "wall_nanoseconds": int(
                float(active_timing["wall_seconds"]) * 1_000_000_000
            ),
        }
        if isinstance(active_timing, dict)
        else None
    )
    type_counts: dict[str, int] = {}
    for input_type in _FAKE_REPLAY_INPUT_TYPES[:completed_target_commits]:
        type_counts[input_type] = type_counts.get(input_type, 0) + 1
    return {
        "active_input": active,
        "completed_target_commits": completed_target_commits,
        "expected_target_commits": len(_FAKE_REPLAY_INPUT_TYPES),
        "input_type_counts": type_counts,
        "last_completed_input": timing["last_completed_input"],
        "projection_history_sizes": timing["projection_history_sizes"],
        "remaining_target_commits": timing["remaining_target_commits"],
        "source_first_input": timing["source_first_input"],
        "source_tail": timing["source_tail"],
        "target_database_bytes": timing["target_database_bytes"],
    }

def _valid_fake_worker_result(token: str, run_id: str) -> dict[str, object]:
    config_hash = "b" * 64
    event_head_hash = "c" * 64
    commit_head_hash = "d" * 64
    projection_hash = "e" * 64
    head_identity: list[object] = [
        run_id,
        config_hash,
        "FLAT",
        8,
        event_head_hash,
        8,
        commit_head_hash,
        8,
        projection_hash,
    ]
    phases = (
        "source_integrity",
        "event_replay",
        "canonical_input_replay_total",
        "replay_store_generation",
        "target_integrity",
        "target_ledger_reconciliation",
        "final_exact_comparisons",
    )
    return {
        "_worker_protocol_token": token,
        "authorizes_real_money": False,
        "bounded_historical_prefix_certification_count": 1,
        "config_hash": config_hash,
        "event": "worker_result",
        "event_count": 8,
        "event_head_hash": event_head_hash,
        "historical_ledger_reconciliation_count": 2,
        "mode": "PAPER_ONLY",
        "orders_enabled": False,
        "peak_rss_bytes": 0,
        "peak_rss_source": "resource-getrusage",
        "profile": {
            "counters": {},
            "instrumentation_mode": "V2",
            "replay_progress": _fake_replay_progress(
                completed_target_commits=8
            ),
            "replay_timing_v2": _fake_replay_timing_v2(
                completed_target_commits=8
            ),
            "logical_row_counts_note": (
                "integrity row hooks and append arguments; not physical SQLite scans"
            ),
            "phase_counters": {},
            "phase_timings": {
                phase: {
                    "counters": {},
                    "cpu_seconds": 0.0,
                    "peak_rss_bytes": 0,
                    "peak_rss_source": "resource-getrusage",
                    "status": "completed",
                    "wall_seconds": 0.0,
                }
                for phase in phases
            },
            "sqlite_sql_text_tracing": ("disabled_to_avoid_expanded_payload_materialization"),
        },
        "projection_hash": projection_hash,
        "projection_history_decode_count": 0,
        "replay_cpu_seconds": 0.0,
        "replay_wall_seconds": 0.0,
        "run_id": run_id,
        "source_head_identity": head_identity,
        "source_open_mode": "sqlite-mode=ro;immutable=1;query_only=ON",
        "source_projection_hash": projection_hash,
        "source_query_only_verified": True,
        "source_sqlite_connection_count": 1,
        "source_write_connection_attempts": 0,
        "status": "WORKER_COMPLETE",
        "target_database_bytes": 1,
        "target_initial_identity": "f" * 64,
        "target_head_identity": head_identity,
        "target_logical_transaction_counts": {"append_atomic": 8, "create_run": 1},
        "target_projection_hash": projection_hash,
        "target_paper_store_sqlite_connection_count": 1,
        "target_sqlite_connection_count": 1,
    }


class _CompletedProtocolProcess:
    def __init__(self, lines: list[str]) -> None:
        self.stdout = io.StringIO(chr(10).join(lines) + chr(10))

    def poll(self) -> int:
        return 0

    def terminate(self) -> None:
        raise AssertionError("completed fake worker must not be terminated")

    def kill(self) -> None:
        raise AssertionError("completed fake worker must not be killed")

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return 0


class _InlineProtocolThread:
    def __init__(
        self,
        *,
        target: Callable[..., object],
        args: tuple[object, ...],
        daemon: bool,
    ) -> None:
        del daemon
        self._target = target
        self._args = args

    def start(self) -> None:
        self._target(*self._args)

    def join(self, timeout: float | None = None) -> None:
        del timeout


def _run_fake_worker_protocol(
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    line_factory: Callable[[str, str], list[str]],
) -> tuple[int, list[dict[str, object]], str, Path, bytes, Path]:
    original = tmp_path / "paper-original.sqlite3"
    source, config = _build_journal(original, historical_replay_only=False)
    source.close()
    database_copy = tmp_path / "paper-explicit-copy.sqlite3"
    shutil.copy2(original, database_copy)
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    before = database_copy.read_bytes()
    args = replay_diagnostic._parser().parse_args(
        _diagnostic_command(
            repository=Path(__file__).resolve().parents[1],
            database_copy=database_copy,
            forbidden_original=original,
            scratch_root=scratch_root,
            run_id=config.run_id,
            expected_sha256=_file_sha256(database_copy),
        )[2:]
    )

    def fake_popen(command: list[str], **kwargs: object) -> _CompletedProtocolProcess:
        assert kwargs["stderr"] is subprocess.STDOUT
        assert kwargs["stdout"] is subprocess.PIPE
        token = command[command.index("--_worker-token") + 1]
        return _CompletedProtocolProcess(line_factory(token, config.run_id))

    monkeypatch.setattr(replay_diagnostic.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(replay_diagnostic.threading, "Thread", _InlineProtocolThread)

    return_code = replay_diagnostic._supervise_locked(args, "delete")
    output = capsys.readouterr().out
    records = [json.loads(line) for line in output.splitlines() if line.strip()]
    return return_code, records, output, database_copy, before, scratch_root


def test_explicit_sqlite_copy_diagnostic_refuses_the_forbidden_original(
    tmp_path: Path,
) -> None:
    repository = Path(__file__).resolve().parents[1]
    database = tmp_path / "paper-original.sqlite3"
    store, config = _build_journal(database, historical_replay_only=False)
    store.close()
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    before = database.read_bytes()

    completed = subprocess.run(
        _diagnostic_command(
            repository=repository,
            database_copy=database,
            forbidden_original=database,
            scratch_root=scratch_root,
            run_id=config.run_id,
            expected_sha256=_file_sha256(database),
        ),
        cwd=repository,
        capture_output=True,
        text=True,
        timeout=90,
    )

    records = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    assert completed.returncode == 2
    assert records[-1]["status"] == "REFUSED_COPY_MATCHES_FORBIDDEN_ORIGINAL"
    assert database.read_bytes() == before
    assert not tuple(scratch_root.iterdir())


def test_explicit_sqlite_copy_diagnostic_refuses_hardlink_to_forbidden_original(
    tmp_path: Path,
) -> None:
    repository = Path(__file__).resolve().parents[1]
    original = tmp_path / "paper-original.sqlite3"
    store, config = _build_journal(original, historical_replay_only=False)
    store.close()
    database_copy = tmp_path / "paper-hardlink.sqlite3"
    try:
        os.link(original, database_copy)
    except OSError as error:
        pytest.skip(f"hardlinks unavailable: {type(error).__name__}")
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    before = original.read_bytes()
    before_stat = original.stat()

    completed = subprocess.run(
        _diagnostic_command(
            repository=repository,
            database_copy=database_copy,
            forbidden_original=original,
            scratch_root=scratch_root,
            run_id=config.run_id,
            expected_sha256=_file_sha256(database_copy),
        ),
        cwd=repository,
        capture_output=True,
        text=True,
        timeout=90,
    )

    records = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    assert completed.returncode == 2
    assert records[-1]["status"] == "REFUSED_COPY_MATCHES_FORBIDDEN_ORIGINAL"
    assert original.read_bytes() == before
    after_stat = original.stat()
    assert after_stat.st_size == before_stat.st_size
    assert after_stat.st_mtime_ns == before_stat.st_mtime_ns
    assert not tuple(scratch_root.iterdir())


def test_explicit_sqlite_copy_diagnostic_refuses_existing_sidecar_unchanged(
    tmp_path: Path,
) -> None:
    repository = Path(__file__).resolve().parents[1]
    original = tmp_path / "paper-original.sqlite3"
    store, config = _build_journal(original, historical_replay_only=False)
    store.close()
    database_copy = tmp_path / "paper-explicit-copy.sqlite3"
    shutil.copy2(original, database_copy)
    sidecar = Path(f"{database_copy}-wal")
    sidecar.write_bytes(b"PREEXISTING_SIDECAR")
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    copy_before = database_copy.read_bytes()
    sidecar_before = sidecar.read_bytes()

    completed = subprocess.run(
        _diagnostic_command(
            repository=repository,
            database_copy=database_copy,
            forbidden_original=original,
            scratch_root=scratch_root,
            run_id=config.run_id,
            expected_sha256=_file_sha256(database_copy),
        ),
        cwd=repository,
        capture_output=True,
        text=True,
        timeout=90,
    )

    records = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    assert completed.returncode == 2
    assert records[-1]["status"] == "REFUSED_COPY_HAS_SQLITE_SIDECAR"
    assert database_copy.read_bytes() == copy_before
    assert sidecar.read_bytes() == sidecar_before
    assert not tuple(scratch_root.iterdir())


def test_explicit_sqlite_copy_diagnostic_profiles_exact_replay_without_source_writes(
    tmp_path: Path,
) -> None:
    repository = Path(__file__).resolve().parents[1]
    original = tmp_path / "paper-original.sqlite3"
    source, config = _build_journal(original, historical_replay_only=False)
    PaperEngine(source, config).reconcile(as_of=_START + timedelta(seconds=7))
    source.close()
    database_copy = tmp_path / "paper-explicit-copy.sqlite3"
    shutil.copy2(original, database_copy)
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    original_before = original.read_bytes()
    copy_before = database_copy.read_bytes()
    copy_stat_before = database_copy.stat()
    expected_sha256 = _file_sha256(database_copy)

    completed = subprocess.run(
        _diagnostic_command(
            repository=repository,
            database_copy=database_copy,
            forbidden_original=original,
            scratch_root=scratch_root,
            run_id=config.run_id,
            expected_sha256=expected_sha256,
            progress_every_rows=10_000,
        ),
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
        timeout=90,
    )

    records = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    started_phases = [str(record["phase"]) for record in records if record.get("event") == "phase_started"]
    finished_records = [record for record in records if record.get("event") == "phase_finished"]
    finished_phases = [str(record["phase"]) for record in finished_records]
    result = records[-1]
    assert result["event"] == "diagnostic_result"
    assert result["status"] == "REPLAY_EXACT"
    assert result["authorizes_real_money"] is False
    assert result["mode"] == "PAPER_ONLY"
    assert result["orders_enabled"] is False
    assert result["source_open_mode"] == "sqlite-mode=ro;immutable=1;query_only=ON"
    assert result["source_query_only_verified"] is True
    assert result["source_journal_mode"] == "delete"
    assert result["source_lock_mode"] == "sqlite-shared-read-transaction"
    assert result["source_sidecars_observed"] == []
    assert result["source_write_connection_attempts"] == 0
    assert result["source_sha256_before"] == expected_sha256
    assert result["source_sha256_after"] == expected_sha256
    assert result["source_sha256_unchanged"] is True
    assert result["source_stat_unchanged"] is True
    assert result["projection_history_decode_count"] == 21
    assert result["bounded_historical_prefix_certification_count"] == 1
    assert result["historical_ledger_reconciliation_count"] == 2
    assert result["target_logical_transaction_counts"] == {
        "append_atomic": 8,
        "create_run": 1,
    }
    assert result["source_sqlite_connection_count"] > 0
    assert result["target_paper_store_sqlite_connection_count"] == 1
    assert result["target_sqlite_connection_count"] == 1
    profile = result["profile"]
    replay_timing = profile["replay_timing_v2"]
    assert replay_timing["version"] == 2
    assert replay_timing["expected_target_commits"] == 8
    assert replay_timing["completed_target_commits"] == 8
    assert replay_timing["remaining_target_commits"] == 0
    assert replay_timing["active_input"] is None
    assert replay_timing["active_operation"] is None
    assert replay_timing["source_first_input"] == {
        "commit_sequence": 1,
        "input_type": "RUN_START",
    }
    expected_replay_inputs = list(enumerate(_FAKE_REPLAY_INPUT_TYPES, start=1))
    assert [
        (item["commit_sequence"], item["input_type"])
        for item in replay_timing["source_tail"]
    ] == expected_replay_inputs
    assert [
        (item["commit_sequence"], item["input_type"])
        for item in replay_timing["completed_input_tail"]
    ] == expected_replay_inputs
    assert all(item["operations"] for item in replay_timing["completed_input_tail"])
    assert {
        "historical_head_integrity",
        "historical_prefix_certification",
        "historical_reconcile",
    } <= set(
        replay_timing["completed_input_tail"][-1]["operations"]
    )
    assert all(
        item["projection_characters"] > 0 and item["zlib_bytes"] > 0
        for item in replay_timing["completed_input_tail"]
    )
    assert replay_timing["last_completed_input"] == {
        "commit_sequence": 8,
        "input_type": "RECONCILE",
    }
    assert {
        input_type: timing["span_count"]
        for input_type, timing in replay_timing["input_type_timings"].items()
    } == {
        "OPERATOR_PAUSE": 1,
        "PUBLIC_FUNDING_SETTLEMENT": 1,
        "PUBLIC_MARKET_EVENT": 4,
        "RECONCILE": 1,
        "RUN_START": 1,
    }
    assert {
        "append_events_insert",
        "append_input_canonicalization",
        "append_ledger_insert",
        "append_prepare_alerts",
        "append_prepare_events",
        "append_prepare_ledger",
        "append_prepare_through_inbox",
        "append_projection_and_commit_rows",
        "append_projection_canonicalization",
        "append_projection_history_storage",
        "append_record_canonicalization",
        "append_replay_validation",
        "append_result_build",
        "append_sqlite_commit",
        "create_run",
        "engine_commit_prepare",
        "historical_full_integrity_verification",
        "historical_head_integrity",
        "historical_prefix_certification",
        "historical_reconcile",
        "input_dispatch",
        "replay_store_post_inputs",
        "replay_store_setup",
        "source_input_fetch",
    } <= set(replay_timing["operation_timings"])
    projection_sizes = replay_timing["projection_history_sizes"]
    assert projection_sizes["latest_commit_sequence"] == 8
    assert projection_sizes["latest_projection_characters"] > 0
    assert projection_sizes["latest_zlib_bytes"] > 0
    assert projection_sizes["max_projection_characters"] >= (
        projection_sizes["latest_projection_characters"]
    )
    assert projection_sizes["max_zlib_bytes"] >= projection_sizes["latest_zlib_bytes"]
    assert replay_timing["target_database_bytes"] == result["target_database_bytes"]
    assert len(replay_timing["slowest_completed_inputs"]) == 8
    serialized_replay_timing = json.dumps(replay_timing, sort_keys=True)
    assert '"input_id"' not in serialized_replay_timing
    assert '"payload"' not in serialized_replay_timing
    assert "SELECT * FROM" not in serialized_replay_timing
    assert profile["sqlite_sql_text_tracing"] == ("disabled_to_avoid_expanded_payload_materialization")
    counters = profile["counters"]
    assert {
        key: counters[key]
        for key in (
            "target_rows_written.paper_alerts",
            "target_rows_written.paper_commits",
            "target_rows_written.paper_events",
            "target_rows_written.paper_inbox",
            "target_rows_written.paper_ledger_entries",
            "target_rows_written.paper_ledger_transactions",
            "target_rows_written.paper_projection_history",
            "target_rows_written.paper_projections",
            "target_rows_written.paper_runs",
            "target_rows_updated.paper_projections",
            "target_rows_updated.paper_runs",
        )
    } == {
        "target_rows_written.paper_alerts": 1,
        "target_rows_written.paper_commits": 8,
        "target_rows_written.paper_events": 9,
        "target_rows_written.paper_inbox": 8,
        "target_rows_written.paper_ledger_entries": 4,
        "target_rows_written.paper_ledger_transactions": 2,
        "target_rows_written.paper_projection_history": 9,
        "target_rows_written.paper_projections": 1,
        "target_rows_written.paper_runs": 1,
        "target_rows_updated.paper_projections": 8,
        "target_rows_updated.paper_runs": 8,
    }
    expected_integrity_rows = {
        "integrity_rows.alert_row": 1,
        "integrity_rows.commit_row": 8,
        "integrity_rows.event_row": 9,
        "integrity_rows.inbox_row": 8,
        "integrity_rows.ledger_entry_row": 4,
        "integrity_rows.ledger_transaction_row": 2,
        "integrity_rows.projection_history_row": 9,
    }
    for phase in ("source_integrity", "target_integrity"):
        phase_counters = profile["phase_timings"][phase]["counters"]
        assert {key: phase_counters[key] for key in expected_integrity_rows} == expected_integrity_rows
    expected_phases = {
        "source_integrity",
        "event_replay",
        "canonical_input_replay_total",
        "replay_store_generation",
        "target_integrity",
        "target_ledger_reconciliation",
        "final_exact_comparisons",
    }
    assert started_phases == [
        "source_integrity",
        "event_replay",
        "canonical_input_replay_total",
        "replay_store_generation",
        "target_integrity",
        "target_ledger_reconciliation",
        "final_exact_comparisons",
    ]
    assert finished_phases == [
        "source_integrity",
        "event_replay",
        "replay_store_generation",
        "target_integrity",
        "target_ledger_reconciliation",
        "final_exact_comparisons",
        "canonical_input_replay_total",
    ]
    assert set(started_phases) == expected_phases
    assert set(finished_phases) == expected_phases
    assert all(record["status"] == "completed" for record in finished_records)
    assert set(profile["phase_timings"]) == expected_phases
    assert len(counters) < 100
    assert not any(key.startswith("json_decode_label.") for key in counters)
    assert original.read_bytes() == original_before
    assert database_copy.read_bytes() == copy_before
    copy_stat_after = database_copy.stat()
    assert copy_stat_after.st_size == copy_stat_before.st_size
    assert copy_stat_after.st_mtime_ns == copy_stat_before.st_mtime_ns
    assert not tuple(scratch_root.iterdir())
    assert not Path(f"{database_copy}-journal").exists()
    assert not Path(f"{database_copy}-wal").exists()
    assert not Path(f"{database_copy}-shm").exists()


def test_explicit_sqlite_copy_diagnostic_refuses_unsupervised_worker(
    tmp_path: Path,
) -> None:
    repository = Path(__file__).resolve().parents[1]
    original = tmp_path / "paper-original.sqlite3"
    database_copy = tmp_path / "paper-explicit-copy.sqlite3"
    original.write_bytes(b"forbidden-original")
    database_copy.write_bytes(b"explicit-copy")
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    before = database_copy.read_bytes()
    command = _diagnostic_command(
        repository=repository,
        database_copy=database_copy,
        forbidden_original=original,
        scratch_root=scratch_root,
        run_id="a" * 64,
        expected_sha256=_file_sha256(database_copy),
    )
    command.insert(2, "--_worker")
    environment = os.environ.copy()
    environment.pop("HYPERLAB_REPLAY_DIAGNOSTIC_WORKER_TOKEN", None)

    completed = subprocess.run(
        command,
        cwd=repository,
        capture_output=True,
        env=environment,
        text=True,
        timeout=90,
    )

    records = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    assert completed.returncode == 2
    assert records == [
        {
            "event": "worker_failed",
            "status": "REFUSED_UNSUPERVISED_WORKER",
        }
    ]
    assert database_copy.read_bytes() == before
    assert not tuple(scratch_root.iterdir())


def test_explicit_sqlite_copy_diagnostic_refuses_wal_header_without_creating_sidecars(
    tmp_path: Path,
) -> None:
    repository = Path(__file__).resolve().parents[1]
    original = tmp_path / "paper-original.sqlite3"
    source, config = _build_journal(original, historical_replay_only=False)
    source.close()
    database_copy = tmp_path / "paper-explicit-copy.sqlite3"
    shutil.copy2(original, database_copy)
    connection = sqlite3.connect(database_copy, isolation_level=None)
    try:
        assert str(connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower() == "wal"
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        connection.close()
    sidecars = tuple(Path(f"{database_copy}{suffix}") for suffix in ("-journal", "-shm", "-wal"))
    assert database_copy.read_bytes()[18:20] == bytes((2, 2))
    assert not any(path.exists() for path in sidecars)
    before = database_copy.read_bytes()
    before_stat = database_copy.stat()
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()

    completed = subprocess.run(
        _diagnostic_command(
            repository=repository,
            database_copy=database_copy,
            forbidden_original=original,
            scratch_root=scratch_root,
            run_id=config.run_id,
            expected_sha256=_file_sha256(database_copy),
        ),
        cwd=repository,
        capture_output=True,
        text=True,
        timeout=90,
    )

    records = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    assert completed.returncode == 2
    assert records[-1]["status"] == "REFUSED_SOURCE_JOURNAL_MODE"
    assert database_copy.read_bytes() == before
    after_stat = database_copy.stat()
    assert after_stat.st_size == before_stat.st_size
    assert after_stat.st_mtime_ns == before_stat.st_mtime_ns
    assert not any(path.exists() for path in sidecars)
    assert not tuple(scratch_root.iterdir())


def test_immutable_copy_store_enforces_query_only_at_sqlite_level(tmp_path: Path) -> None:
    database = tmp_path / "paper.sqlite3"
    source, _config_value = _build_journal(database, historical_replay_only=False)
    source.close()
    before = database.read_bytes()
    profiler = replay_diagnostic.ReplayProfiler(progress_every_rows=1_000)
    immutable = replay_diagnostic.ImmutableCopyPaperStore(database, profiler)
    connection = immutable._read_connection()
    try:
        assert int(connection.execute("PRAGMA query_only").fetchone()[0]) == 1
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("CREATE TABLE forbidden_write(value INTEGER)")
    finally:
        connection.close()
        immutable.close()

    assert profiler.source_query_only_verified is True
    assert profiler.source_write_connection_attempts == 0
    assert database.read_bytes() == before
    assert not Path(f"{database}-journal").exists()
    assert not Path(f"{database}-wal").exists()
    assert not Path(f"{database}-shm").exists()


def test_supervisor_refuses_sidecar_observed_after_final_fingerprint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    original = tmp_path / "paper-original.sqlite3"
    source, config = _build_journal(original, historical_replay_only=False)
    source.close()
    database_copy = tmp_path / "paper-explicit-copy.sqlite3"
    shutil.copy2(original, database_copy)
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    before = database_copy.read_bytes()
    args = replay_diagnostic._parser().parse_args(
        _diagnostic_command(
            repository=Path(__file__).resolve().parents[1],
            database_copy=database_copy,
            forbidden_original=original,
            scratch_root=scratch_root,
            run_id=config.run_id,
            expected_sha256=_file_sha256(database_copy),
        )[2:]
    )
    state = {"after_final_fingerprint": False, "fingerprints": 0}
    original_fingerprint = replay_diagnostic._fingerprint

    def observed_fingerprint(
        path: Path,
        *,
        deadline: float | None = None,
    ) -> replay_diagnostic.Fingerprint:
        result = original_fingerprint(path, deadline=deadline)
        state["fingerprints"] += 1
        if state["fingerprints"] == 2:
            state["after_final_fingerprint"] = True
        return result

    def observed_sidecars(path: Path) -> tuple[Path, ...]:
        if state["after_final_fingerprint"]:
            return (Path(f"{path}-wal"),)
        return ()

    class FakeProcess:
        def __init__(self, token: str) -> None:
            self.stdout = io.StringIO(json.dumps(_valid_fake_worker_result(token, config.run_id)) + chr(10))

        def poll(self) -> int:
            return 0

        def terminate(self) -> None:
            raise AssertionError("completed fake worker must not be terminated")

        def kill(self) -> None:
            raise AssertionError("completed fake worker must not be killed")

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            return 0

    class InlineThread:
        def __init__(
            self,
            *,
            target: Callable[..., object],
            args: tuple[object, ...],
            daemon: bool,
        ) -> None:
            del daemon
            self._target = target
            self._args = args

        def start(self) -> None:
            self._target(*self._args)

        def join(self, timeout: float | None = None) -> None:
            del timeout

    def fake_popen(command: list[str], **kwargs: object) -> FakeProcess:
        assert kwargs["stderr"] is subprocess.STDOUT
        token = command[command.index("--_worker-token") + 1]
        return FakeProcess(token)

    monkeypatch.setattr(replay_diagnostic, "_fingerprint", observed_fingerprint)
    monkeypatch.setattr(replay_diagnostic, "_sqlite_sidecars", observed_sidecars)
    monkeypatch.setattr(replay_diagnostic.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(replay_diagnostic.threading, "Thread", InlineThread)

    return_code = replay_diagnostic._supervise_locked(args, "delete")

    records = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    result = records[-1]
    assert return_code == 1
    assert result["event"] == "diagnostic_result"
    assert result["status"] == "SOURCE_COPY_SQLITE_SIDECAR_APPEARED"
    assert result["source_sidecars_observed"] == ["-wal"]
    assert not any(
        record.get("event") == "diagnostic_result" and record.get("status") == "REPLAY_EXACT"
        for record in records
    )
    assert database_copy.read_bytes() == before
    assert not tuple(scratch_root.iterdir())


def test_supervisor_final_fingerprint_refusal_preserves_last_replay_timing_v2(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    original_fingerprint = replay_diagnostic._fingerprint
    fingerprint_calls = 0

    def refuse_final_fingerprint(
        path: Path,
        *,
        deadline: float | None = None,
    ) -> replay_diagnostic.Fingerprint:
        nonlocal fingerprint_calls
        fingerprint_calls += 1
        if fingerprint_calls == 2:
            raise replay_diagnostic.DiagnosticRefusal(
                "REFUSED_FINGERPRINT_DEADLINE",
                "final fingerprint exceeded its reserved deadline",
            )
        return original_fingerprint(path, deadline=deadline)

    def heartbeat_then_terminal(token: str, run_id: str) -> list[str]:
        replay_timing = _fake_replay_timing_v2(completed_target_commits=7)
        return [
            json.dumps(
                {
                    "_worker_protocol_token": token,
                    "elapsed_seconds": 1.0,
                    "event": "phase_heartbeat",
                    "peak_rss_bytes": 0,
                    "peak_rss_source": "resource-getrusage",
                    "phase": "replay_store_generation",
                    "phase_cpu_seconds": 1.0,
                    "phase_wall_seconds": 1.0,
                    "replay_progress": _fake_replay_progress(
                        completed_target_commits=7
                    ),
                    "replay_timing_v2": replay_timing,
                    "rows_observed": 1,
                    "sequence": 1,
                    "target_commits": 7,
                }
            ),
            json.dumps(_valid_fake_worker_result(token, run_id)),
        ]

    monkeypatch.setattr(replay_diagnostic, "_fingerprint", refuse_final_fingerprint)
    return_code, records, _output, database_copy, before, scratch_root = (
        _run_fake_worker_protocol(
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            capsys=capsys,
            line_factory=heartbeat_then_terminal,
        )
    )

    result = records[-1]
    assert fingerprint_calls == 2
    assert return_code == 1
    assert result["event"] == "diagnostic_result"
    assert result["status"] == "REFUSED_FINGERPRINT_DEADLINE"
    assert result["authorizes_real_money"] is False
    assert result["mode"] == "PAPER_ONLY"
    assert result["orders_enabled"] is False
    assert result["last_worker_phase"] == "replay_store_generation"
    assert result["last_worker_sequence"] == 1
    assert result["last_replay_timing_v2"] == _fake_replay_timing_v2(
        completed_target_commits=7
    )
    assert result["source_sha256_before"] == _file_sha256(database_copy)
    assert result["source_sidecars_observed"] == []
    assert database_copy.read_bytes() == before
    assert not tuple(scratch_root.iterdir())

def test_supervisor_timeout_preserves_last_phase_and_cleans_worker_scratch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    original = tmp_path / "paper-original.sqlite3"
    source, config = _build_journal(original, historical_replay_only=False)
    source.close()
    database_copy = tmp_path / "paper-explicit-copy.sqlite3"
    shutil.copy2(original, database_copy)
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    before = database_copy.read_bytes()
    before_stat = database_copy.stat()
    args = replay_diagnostic._parser().parse_args(
        _diagnostic_command(
            repository=Path(__file__).resolve().parents[1],
            database_copy=database_copy,
            forbidden_original=original,
            scratch_root=scratch_root,
            run_id=config.run_id,
            expected_sha256=_file_sha256(database_copy),
        )[2:]
    )
    clock: dict[str, object] = {"armed": False, "checks": 0}
    process_holder: list[FakeProcess] = []

    class FakeProcess:
        def __init__(self, token: str) -> None:
            self.stdout = io.StringIO(
                chr(10).join(
                    (
                        json.dumps(
                            {
                                "_worker_protocol_token": token,
                                "elapsed_seconds": 1.0,
                                "event": "phase_started",
                                "phase": "replay_store_generation",
                                "sequence": 1,
                            }
                        ),
                        json.dumps(
                            {
                                "_worker_protocol_token": token,
                                "elapsed_seconds": 6.0,
                                "event": "phase_heartbeat",
                                "peak_rss_bytes": 0,
                                "peak_rss_source": "resource-getrusage",
                                "phase": "replay_store_generation",
                                "phase_cpu_seconds": 4.0,
                                "phase_wall_seconds": 5.0,
                                "rows_observed": 10,
                                "sequence": 2,
                                "target_commits": 7,
                                "replay_progress": _fake_replay_progress(
                                    completed_target_commits=7
                                ),
                                "replay_timing_v2": _fake_replay_timing_v2(
                                    completed_target_commits=7
                                ),                            }
                        ),
                    )
                )
                + chr(10)
            )
            self.return_code: int | None = None
            self.terminated = False

        def poll(self) -> int | None:
            return self.return_code

        def terminate(self) -> None:
            self.terminated = True
            self.return_code = -15
            clock["armed"] = False

        def kill(self) -> None:
            self.return_code = -9
            clock["armed"] = False

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            assert self.return_code is not None
            return self.return_code

    class InlineThread:
        def __init__(
            self,
            *,
            target: Callable[..., object],
            args: tuple[object, ...],
            daemon: bool,
        ) -> None:
            del daemon
            self._target = target
            self._args = args

        def start(self) -> None:
            self._target(*self._args)
            clock["armed"] = True

        def join(self, timeout: float | None = None) -> None:
            del timeout

    def fake_popen(command: list[str], **kwargs: object) -> FakeProcess:
        assert kwargs["stderr"] is subprocess.STDOUT
        token = command[command.index("--_worker-token") + 1]
        owned_scratch = Path(command[command.index("--scratch-root") + 1])
        (owned_scratch / "worker-residue.tmp").write_bytes(b"residue")
        assert (owned_scratch / "worker-residue.tmp").exists()
        process = FakeProcess(token)
        process_holder.append(process)
        return process

    def fake_perf_counter() -> float:
        if not bool(clock["armed"]):
            return 0.0
        clock["checks"] = int(clock["checks"]) + 1
        return 100.0 if int(clock["checks"]) >= 2 else 0.0

    monkeypatch.setattr(replay_diagnostic, "perf_counter", fake_perf_counter)
    monkeypatch.setattr(replay_diagnostic.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(replay_diagnostic.threading, "Thread", InlineThread)

    return_code = replay_diagnostic._supervise_locked(args, "delete")

    records = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    events = [str(record["event"]) for record in records]
    result = records[-1]
    heartbeat = next(record for record in records if record["event"] == "phase_heartbeat")
    worker_timeout = next(record for record in records if record["event"] == "worker_timeout")
    assert return_code == 124
    assert events.index("phase_started") < events.index("phase_heartbeat")
    assert events.index("phase_heartbeat") < events.index("worker_timeout")
    assert events.index("worker_timeout") < len(events) - 1
    assert result["event"] == "diagnostic_result"
    assert result["status"] == "DIAGNOSTIC_TIMEOUT"
    assert result["last_worker_phase"] == "replay_store_generation"
    assert result["last_worker_sequence"] == 2
    assert heartbeat["phase_wall_seconds"] == 5.0
    assert heartbeat["phase_cpu_seconds"] == 4.0
    assert heartbeat["replay_timing_v2"] == worker_timeout["last_replay_timing_v2"]
    assert worker_timeout["last_replay_timing_v2"] == result["last_replay_timing_v2"]
    assert result["last_replay_timing_v2"]["active_input"] == {
        "commit_sequence": 8,
        "input_type": "RECONCILE",
        "wall_seconds": 1.0,
    }
    assert process_holder[0].terminated is True
    assert database_copy.read_bytes() == before
    after_stat = database_copy.stat()
    assert after_stat.st_size == before_stat.st_size
    assert after_stat.st_mtime_ns == before_stat.st_mtime_ns
    assert not tuple(scratch_root.iterdir())


def test_replay_timing_v2_bootstrap_uses_source_first_outside_bounded_tail() -> None:
    replay_timing: dict[str, object] = {
        "active_input": None,
        "active_operation": None,
        "completed_input_tail": [],
        "completed_target_commits": 1,
        "expected_target_commits": 20,
        "input_type_timings": {},
        "last_completed_input": None,
        "operation_timings": {
            "create_run": {
                "max_wall_seconds": 0.0,
                "span_count": 1,
                "thread_cpu_seconds": 0.0,
                "wall_seconds": 0.0,
            }
        },
        "projection_history_sizes": {
            "latest_commit_sequence": 1,
            "latest_projection_characters": 1,
            "latest_zlib_bytes": 1,
            "max_projection_characters": 1,
            "max_zlib_bytes": 1,
        },
        "remaining_target_commits": 19,
        "slowest_completed_inputs": [],
        "source_first_input": {
            "commit_sequence": 1,
            "input_type": "RUN_START",
        },
        "source_tail": [
            {
                "commit_sequence": sequence,
                "input_type": "PUBLIC_MARKET_EVENT",
            }
            for sequence in range(5, 21)
        ],
        "target_database_bytes": 1,
        "version": 2,
    }

    assert replay_diagnostic._is_replay_timing_v2(
        replay_timing,
        terminal=False,
        target_commits=1,
        expected_commits=20,
    )

    contradictory_active = {
        **replay_timing,
        "active_input": {
            "commit_sequence": 1,
            "input_type": "PUBLIC_MARKET_EVENT",
            "wall_seconds": 0.0,
        },
    }
    assert not replay_diagnostic._is_replay_timing_v2(
        contradictory_active,
        terminal=False,
        target_commits=1,
        expected_commits=20,
    )

    empty_terminal = {
        **replay_timing,
        "completed_target_commits": 0,
        "expected_target_commits": 0,
        "operation_timings": {},
        "projection_history_sizes": {
            "latest_commit_sequence": 0,
            "latest_projection_characters": 0,
            "latest_zlib_bytes": 0,
            "max_projection_characters": 0,
            "max_zlib_bytes": 0,
        },
        "remaining_target_commits": 0,
        "source_first_input": None,
        "source_tail": [],
    }
    assert not replay_diagnostic._is_replay_timing_v2(
        empty_terminal,
        terminal=True,
        target_commits=0,
        expected_commits=0,
    )


def test_worker_protocol_refuses_authenticated_incomplete_terminal_without_forwarding(
    capsys: pytest.CaptureFixture[str],
) -> None:
    token = "a" * 64
    run_id = "b" * 64
    line = json.dumps(
        {
            "_worker_protocol_token": token,
            "event": "worker_result",
            "status": "WORKER_COMPLETE",
        }
    )

    record, failure = replay_diagnostic._decode_worker_line(
        line,
        expected_run_id=run_id,
        expected_token=token,
    )

    assert record is None
    assert failure == {
        "detail": "worker terminal result failed fail-closed schema validation",
        "status": "DIAGNOSTIC_WORKER_PROTOCOL_FAILURE",
    }
    assert capsys.readouterr().out == ""


def test_worker_protocol_refuses_payload_inside_replay_timing_v2(
    capsys: pytest.CaptureFixture[str],
) -> None:
    token = "a" * 64
    run_id = "b" * 64
    result = _valid_fake_worker_result(token, run_id)
    profile = result["profile"]
    assert isinstance(profile, dict)
    replay_timing = profile["replay_timing_v2"]
    assert isinstance(replay_timing, dict)
    source_first = replay_timing["source_first_input"]
    assert isinstance(source_first, dict)
    source_first["payload"] = {"forbidden": "SECRET_INPUT_PAYLOAD"}

    record, failure = replay_diagnostic._decode_worker_line(
        json.dumps(result),
        expected_run_id=run_id,
        expected_token=token,
    )

    assert record is None
    assert failure == {
        "detail": "worker terminal result failed fail-closed schema validation",
        "status": "DIAGNOSTIC_WORKER_PROTOCOL_FAILURE",
    }
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    "case",
    (
        "empty_operations",
        "zero_projection_characters",
        "zero_zlib_bytes",
        "terminal_other_type",
        "mismatched_type_aggregate",
        "missing_operation_aggregate",
    ),
)
def test_worker_protocol_refuses_impossible_replay_timing_v2(
    case: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    token = "a" * 64
    run_id = "b" * 64
    result = _valid_fake_worker_result(token, run_id)
    profile = result["profile"]
    assert isinstance(profile, dict)
    replay_timing = profile["replay_timing_v2"]
    assert isinstance(replay_timing, dict)
    completed_tail = replay_timing["completed_input_tail"]
    assert isinstance(completed_tail, list)
    first_completed = completed_tail[0]
    assert isinstance(first_completed, dict)
    input_type_timings = replay_timing["input_type_timings"]
    operation_timings = replay_timing["operation_timings"]
    assert isinstance(input_type_timings, dict)
    assert isinstance(operation_timings, dict)

    if case == "empty_operations":
        first_completed["operations"] = {}
    elif case == "zero_projection_characters":
        first_completed["projection_characters"] = 0
    elif case == "zero_zlib_bytes":
        first_completed["zlib_bytes"] = 0
    elif case == "terminal_other_type":
        input_type_timings["OTHER"] = input_type_timings.pop("OPERATOR_PAUSE")
        source_tail = replay_timing["source_tail"]
        slowest = replay_timing["slowest_completed_inputs"]
        assert isinstance(source_tail, list)
        assert isinstance(slowest, list)
        source_tail[6]["input_type"] = "OTHER"
        completed_tail[6]["input_type"] = "OTHER"
        for item in slowest:
            assert isinstance(item, dict)
            if item["commit_sequence"] == 7:
                item["input_type"] = "OTHER"
                break
        else:
            raise AssertionError("fake slowest inputs omitted commit sequence 7")
    elif case == "mismatched_type_aggregate":
        input_type_timings.pop("OPERATOR_PAUSE")
        market_timing = input_type_timings["PUBLIC_MARKET_EVENT"]
        assert isinstance(market_timing, dict)
        market_timing["span_count"] = int(market_timing["span_count"]) + 1
    elif case == "missing_operation_aggregate":
        operation_timings.pop("historical_head_integrity")
    else:
        raise AssertionError(f"unhandled mutation case: {case}")

    record, failure = replay_diagnostic._decode_worker_line(
        json.dumps(result),
        expected_run_id=run_id,
        expected_token=token,
    )

    assert record is None
    assert failure == {
        "detail": "worker terminal result failed fail-closed schema validation",
        "status": "DIAGNOSTIC_WORKER_PROTOCOL_FAILURE",
    }
    assert capsys.readouterr().out == ""



def test_supervisor_never_trusts_success_status_on_worker_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def forged_failure(token: str, _run_id: str) -> list[str]:
        return [
            json.dumps(
                {
                    "_worker_protocol_token": token,
                    "detail": "SECRET_FAILURE_PAYLOAD",
                    "event": "worker_failed",
                    "status": "REPLAY_EXACT",
                }
            )
        ]

    return_code, records, output, database_copy, before, scratch_root = _run_fake_worker_protocol(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        line_factory=forged_failure,
    )

    assert return_code == 1
    assert records[-1]["status"] == "DIAGNOSTIC_WORKER_PROTOCOL_FAILURE"
    assert not any(record.get("status") == "REPLAY_EXACT" for record in records)
    assert "SECRET_FAILURE_PAYLOAD" not in output
    assert database_copy.read_bytes() == before
    assert not tuple(scratch_root.iterdir())


def test_supervisor_rejects_every_record_after_valid_worker_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def terminal_then_phase(token: str, run_id: str) -> list[str]:
        phase_after_terminal = {
            "_worker_protocol_token": token,
            "elapsed_seconds": 1.0,
            "event": "phase_started",
            "phase": "event_replay",
            "sequence": 1,
        }
        return [
            json.dumps(_valid_fake_worker_result(token, run_id)),
            json.dumps(phase_after_terminal),
        ]

    return_code, records, _output, database_copy, before, scratch_root = _run_fake_worker_protocol(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        line_factory=terminal_then_phase,
    )

    assert return_code == 1
    assert records[-1]["status"] == "DIAGNOSTIC_WORKER_PROTOCOL_FAILURE"
    assert not any(
        record.get("event") == "diagnostic_result" and record.get("status") == "REPLAY_EXACT"
        for record in records
    )
    assert not any(record.get("event") == "phase_started" for record in records)
    assert database_copy.read_bytes() == before
    assert not tuple(scratch_root.iterdir())


def test_supervisor_rejects_extra_payload_and_filters_merged_stderr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def result_with_payload(token: str, run_id: str) -> list[str]:
        result = _valid_fake_worker_result(token, run_id)
        result["payload"] = "SECRET_SQL_PAYLOAD"
        return [json.dumps(result)]

    return_code, records, output, database_copy, before, scratch_root = _run_fake_worker_protocol(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        line_factory=result_with_payload,
    )

    assert return_code == 1
    assert records[-1]["status"] == "DIAGNOSTIC_WORKER_PROTOCOL_FAILURE"
    assert "SECRET_SQL_PAYLOAD" not in output
    assert database_copy.read_bytes() == before
    assert not tuple(scratch_root.iterdir())


def test_worker_stdout_reader_bounds_one_unterminated_line() -> None:
    stream = io.StringIO("x" * (replay_diagnostic._MAX_WORKER_LINE_CHARACTERS + 10))
    messages: queue.Queue[str | None] = queue.Queue()

    replay_diagnostic._forward_stdout(stream, messages)

    assert messages.get_nowait() == replay_diagnostic._WORKER_LINE_TOO_LONG
    assert messages.get_nowait() is None
    with pytest.raises(queue.Empty):
        messages.get_nowait()


def test_worker_stdout_reader_fails_closed_when_queue_capacity_is_exceeded() -> None:
    stream = io.StringIO("first\nsecond\nthird\n")
    messages: queue.Queue[str | None] = queue.Queue(maxsize=2)

    replay_diagnostic._forward_stdout(stream, messages)

    overflow = messages.get_nowait()
    assert overflow == replay_diagnostic._WORKER_OUTPUT_QUEUE_FULL
    assert messages.get_nowait() is None
    with pytest.raises(queue.Empty):
        messages.get_nowait()
    record, failure = replay_diagnostic._decode_worker_line(
        overflow,
        expected_run_id="a" * 64,
        expected_token="b" * 64,
    )
    assert record is None
    assert failure == {
        "detail": "worker output exceeded the bounded supervisor queue capacity",
        "status": "DIAGNOSTIC_WORKER_PROTOCOL_FAILURE",
    }


@pytest.mark.skipif(not hasattr(signal, "SIGTERM"), reason="SIGTERM is unavailable")
def test_supervisor_sigterm_reaps_worker_attests_source_and_restores_handler(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    original = tmp_path / "paper-original.sqlite3"
    source, config = _build_journal(original, historical_replay_only=False)
    source.close()
    database_copy = tmp_path / "paper-explicit-copy.sqlite3"
    shutil.copy2(original, database_copy)
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    expected_sha256 = _file_sha256(database_copy)
    before = database_copy.read_bytes()
    before_stat = database_copy.stat()
    args = replay_diagnostic._parser().parse_args(
        _diagnostic_command(
            repository=Path(__file__).resolve().parents[1],
            database_copy=database_copy,
            forbidden_original=original,
            scratch_root=scratch_root,
            run_id=config.run_id,
            expected_sha256=expected_sha256,
        )[2:]
    )
    previous_handler = signal.getsignal(signal.SIGTERM)
    process_holder: list[FakeProcess] = []

    class FakeProcess:
        def __init__(self, token: str) -> None:
            self.stdout = io.StringIO(
                chr(10).join(
                    (
                        json.dumps(
                            {
                                "_worker_protocol_token": token,
                                "elapsed_seconds": 1.0,
                                "event": "phase_started",
                                "phase": "replay_store_generation",
                                "sequence": 1,
                            }
                        ),
                        json.dumps(
                            {
                                "_worker_protocol_token": token,
                                "elapsed_seconds": 6.0,
                                "event": "phase_heartbeat",
                                "peak_rss_bytes": 0,
                                "peak_rss_source": "resource-getrusage",
                                "phase": "replay_store_generation",
                                "phase_cpu_seconds": 4.0,
                                "phase_wall_seconds": 5.0,
                                "rows_observed": 10,
                                "sequence": 2,
                                "target_commits": 7,
                                "replay_progress": _fake_replay_progress(
                                    completed_target_commits=7
                                ),
                                "replay_timing_v2": _fake_replay_timing_v2(
                                    completed_target_commits=7
                                ),                            }
                        ),
                    )
                )
                + chr(10)
            )
            self.return_code: int | None = None
            self.signalled = False
            self.terminated = False

        def poll(self) -> int | None:
            if not self.signalled:
                self.signalled = True
                handler = signal.getsignal(signal.SIGTERM)
                assert callable(handler)
                handler(signal.SIGTERM, None)
            return self.return_code

        def terminate(self) -> None:
            self.terminated = True
            self.return_code = -15

        def kill(self) -> None:
            self.return_code = -9

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            assert self.return_code is not None
            return self.return_code

    class InlineThread:
        def __init__(
            self,
            *,
            target: Callable[..., object],
            args: tuple[object, ...],
            daemon: bool,
        ) -> None:
            del daemon
            self._target = target
            self._args = args

        def start(self) -> None:
            self._target(*self._args)

        def join(self, timeout: float | None = None) -> None:
            del timeout

    def fake_popen(command: list[str], **kwargs: object) -> FakeProcess:
        assert kwargs["stderr"] is subprocess.STDOUT
        token = command[command.index("--_worker-token") + 1]
        owned_scratch = Path(command[command.index("--scratch-root") + 1])
        (owned_scratch / "worker-residue.tmp").write_bytes(b"residue")
        process = FakeProcess(token)
        process_holder.append(process)
        return process

    monkeypatch.setattr(replay_diagnostic.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(replay_diagnostic.threading, "Thread", InlineThread)

    return_code = replay_diagnostic._supervise(args)

    records = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    result = records[-1]
    assert return_code == 130
    assert result["event"] == "diagnostic_result"
    assert result["status"] == "DIAGNOSTIC_INTERRUPTED"
    assert result["last_worker_phase"] == "replay_store_generation"
    assert result["last_worker_sequence"] == 2
    assert any(record.get("event") == "phase_heartbeat" for record in records)
    assert result["source_sha256_before"] == expected_sha256
    assert result["source_sha256_after"] == expected_sha256
    assert result["source_sha256_unchanged"] is True
    assert result["source_stat_unchanged"] is True
    assert process_holder[0].terminated is True
    assert database_copy.read_bytes() == before
    after_stat = database_copy.stat()
    assert after_stat.st_size == before_stat.st_size
    assert after_stat.st_mtime_ns == before_stat.st_mtime_ns
    assert not tuple(scratch_root.iterdir())
    assert signal.getsignal(signal.SIGTERM) == previous_handler


@pytest.mark.skipif(not hasattr(signal, "SIGTERM"), reason="SIGTERM is unavailable")
def test_supervisor_sigterm_during_final_fingerprint_keeps_after_attestation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    original = tmp_path / "paper-original.sqlite3"
    source, config = _build_journal(original, historical_replay_only=False)
    source.close()
    database_copy = tmp_path / "paper-explicit-copy.sqlite3"
    shutil.copy2(original, database_copy)
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    expected_sha256 = _file_sha256(database_copy)
    before = database_copy.read_bytes()
    before_stat = database_copy.stat()
    args = replay_diagnostic._parser().parse_args(
        _diagnostic_command(
            repository=Path(__file__).resolve().parents[1],
            database_copy=database_copy,
            forbidden_original=original,
            scratch_root=scratch_root,
            run_id=config.run_id,
            expected_sha256=expected_sha256,
        )[2:]
    )
    previous_handler = signal.getsignal(signal.SIGTERM)
    original_fingerprint = replay_diagnostic._fingerprint
    fingerprint_calls = 0

    def signal_after_fingerprint(
        path: Path,
        *,
        deadline: float | None = None,
    ) -> replay_diagnostic.Fingerprint:
        nonlocal fingerprint_calls
        result = original_fingerprint(path, deadline=deadline)
        fingerprint_calls += 1
        if fingerprint_calls == 2:
            handler = signal.getsignal(signal.SIGTERM)
            assert callable(handler)
            handler(signal.SIGTERM, None)
        return result

    def fake_popen(command: list[str], **kwargs: object) -> _CompletedProtocolProcess:
        assert kwargs["stderr"] is subprocess.STDOUT
        token = command[command.index("--_worker-token") + 1]
        owned_scratch = Path(command[command.index("--scratch-root") + 1])
        (owned_scratch / "worker-residue.tmp").write_bytes(b"residue")
        return _CompletedProtocolProcess([json.dumps(_valid_fake_worker_result(token, config.run_id))])

    monkeypatch.setattr(replay_diagnostic, "_fingerprint", signal_after_fingerprint)
    monkeypatch.setattr(replay_diagnostic.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(replay_diagnostic.threading, "Thread", _InlineProtocolThread)

    return_code = replay_diagnostic._supervise(args)

    records = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    result = records[-1]
    assert fingerprint_calls == 2
    assert return_code == 130
    assert result["status"] == "DIAGNOSTIC_INTERRUPTED"
    assert result["source_sha256_before"] == expected_sha256
    assert result["source_sha256_after"] == expected_sha256
    assert result["source_sha256_unchanged"] is True
    assert result["source_stat_unchanged"] is True
    assert database_copy.read_bytes() == before
    after_stat = database_copy.stat()
    assert after_stat.st_size == before_stat.st_size
    assert after_stat.st_mtime_ns == before_stat.st_mtime_ns
    assert not tuple(scratch_root.iterdir())
    assert signal.getsignal(signal.SIGTERM) == previous_handler


def test_supervisor_initial_filesystem_error_is_fixed_jsonl_without_path_leak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = replay_diagnostic._parser().parse_args(
        [
            "--database-copy",
            str(tmp_path / "copy.sqlite3"),
            "--forbid-original",
            str(tmp_path / "original.sqlite3"),
            "--scratch-root",
            str(tmp_path),
            "--run-id",
            "a" * 64,
            "--expected-sha256",
            "b" * 64,
        ]
    )

    def fail_resolve(_args: object) -> tuple[Path, Path, Path]:
        raise OSError("SECRET_PATH_OR_PAYLOAD")

    monkeypatch.setattr(replay_diagnostic, "_resolve_inputs", fail_resolve)

    return_code = replay_diagnostic._supervise(args)

    captured = capsys.readouterr()
    records = [json.loads(line) for line in captured.out.splitlines() if line.strip()]
    assert return_code == 1
    assert records == [
        {
            "authorizes_real_money": False,
            "detail": "diagnostic supervisor raised an exception before source attestation",
            "event": "diagnostic_result",
            "exception_type": "OSError",
            "mode": "PAPER_ONLY",
            "orders_enabled": False,
            "status": "DIAGNOSTIC_SUPERVISOR_FAILED",
        }
    ]
    assert "SECRET_PATH_OR_PAYLOAD" not in captured.out
    assert "SECRET_PATH_OR_PAYLOAD" not in captured.err
    assert "Traceback" not in captured.err
