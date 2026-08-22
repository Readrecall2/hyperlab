from __future__ import annotations

import gc
import json
import sqlite3
import subprocess
import sys
from collections.abc import Iterator
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
