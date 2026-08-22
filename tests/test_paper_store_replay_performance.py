from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

import hyperlab.paper.engine as engine_module
from hyperlab.paper import (
    MarketEvent,
    PaperEngine,
    PaperExecutionConfig,
    PaperRiskLimits,
    PaperRunConfig,
    PaperStore,
)

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
) -> tuple[PaperStore, PaperRunConfig]:
    config = _config()
    store = PaperStore(path, historical_replay_only=historical_replay_only)
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
    database = tmp_path / "disposable-replay.sqlite3"
    store, config = _build_journal(database, historical_replay_only=True)
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
    with pytest.raises(sqlite3.ProgrammingError):
        connection.execute("SELECT 1")
    database.unlink()
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
            "temporary replay-store cleanup also failed: "
            "OSError: synthetic replay close failure" in note
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
    replay, replay_config = _build_journal(
        tmp_path / "replay-equivalence.sqlite3",
        historical_replay_only=True,
    )
    assert replay_config == config
    assert normal.inspect_integrity_readonly(config.run_id).ok is True
    assert replay.inspect_integrity_readonly(config.run_id).ok is True
    assert replay.get_run(config.run_id).head_identity == normal.get_run(config.run_id).head_identity
    assert replay.get_projection(config.run_id).to_dict() == normal.get_projection(
        config.run_id
    ).to_dict()
    assert _input_identity(replay, config.run_id) == _input_identity(normal, config.run_id)
    assert tuple(
        (event.hash_payload(), event.event_hash)
        for event in replay.iter_events(config.run_id)
    ) == tuple(
        (event.hash_payload(), event.event_hash)
        for event in normal.iter_events(config.run_id)
    )
    assert tuple(
        (entry.entry, entry.entry_hash)
        for entry in replay.iter_ledger_entries(config.run_id)
    ) == tuple(
        (entry.entry, entry.entry_hash)
        for entry in normal.iter_ledger_entries(config.run_id)
    )
    replay.close()
