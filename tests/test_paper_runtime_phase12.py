from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from hyperlab.backtest.protocol import canonical_json
from hyperlab.cli import app
from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.models import (
    DecisionIntent,
    MarketEvent,
    PaperEventType,
    PaperExecutionConfig,
    PaperRiskLimits,
    PaperRunConfig,
    PaperState,
    deterministic_id,
)
from hyperlab.paper.runner import PaperStrategyView
from hyperlab.paper.runtime import (
    PaperAdmissionError,
    PaperRuntime,
    PaperRuntimeConfig,
    PaperRuntimeStepKind,
    PublicSourceDescriptor,
    replay_paper_run,
)
from hyperlab.paper.store import IdempotencyConflictError, PaperStore

_START = datetime(2026, 8, 13, 10, tzinfo=UTC)
_STRATEGY_HASH = "a" * 64
_DATA_HASH = "b" * 64
_SOURCE = "phase12-public-normalized-test"
_INSTRUMENT = "HYPERLIQUID:BTC:perp"


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class HoldStrategy:
    strategy_name = "phase12_runtime_fixture"

    def __init__(self) -> None:
        self.strategy_hash = _STRATEGY_HASH
        self.calls = 0

    def decide(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None:
        assert markets
        assert view.config_hash
        self.calls += 1
        return None


class QueuePublicSource:
    def __init__(
        self,
        frames: list[Mapping[str, MarketEvent] | None],
        *,
        descriptor: PublicSourceDescriptor | None = None,
        before_poll: object | None = None,
    ) -> None:
        self._descriptor = descriptor or PublicSourceDescriptor(_SOURCE, _DATA_HASH)
        self.frames = list(frames)
        self.before_poll = before_poll
        self.poll_calls = 0
        self.stop_calls = 0
        self.close_calls = 0
        self.timeouts: list[float] = []

    @property
    def descriptor(self) -> PublicSourceDescriptor:
        return self._descriptor

    @descriptor.setter
    def descriptor(self, value: PublicSourceDescriptor) -> None:
        self._descriptor = value

    def poll(self, *, timeout_seconds: float) -> Mapping[str, MarketEvent] | None:
        self.poll_calls += 1
        self.timeouts.append(timeout_seconds)
        if callable(self.before_poll):
            self.before_poll()
        return self.frames.pop(0) if self.frames else None

    def stop(self) -> None:
        self.stop_calls += 1

    def close(self) -> None:
        self.close_calls += 1


def _config() -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="phase12_runtime_fixture",
        strategy_hash=_STRATEGY_HASH,
        parameters={"threshold": "frozen", "version": 1},
        data_hash=_DATA_HASH,
        execution=PaperExecutionConfig(),
        risk=PaperRiskLimits(),
        seed=12,
        initial_cash=Decimal("100000"),
        validation_started_at=_START,
        run_kind="DEMO",
        data_calibration_status="SYNTHETIC",
        data_source=_SOURCE,
    )


def _market(label: str, received_at: datetime) -> MarketEvent:
    return MarketEvent.create(
        received_at=received_at,
        instrument=_INSTRUMENT,
        bid_price=Decimal("100"),
        ask_price=Decimal("101"),
        bid_depth=Decimal("10"),
        ask_depth=Decimal("11"),
        source_sequence=int(deterministic_id("phase12_runtime_sequence", label)[:8], 16),
    )


def _runtime(
    database: Path,
    source: QueuePublicSource,
    strategy: HoldStrategy,
    clock: MutableClock,
    *,
    timer_interval_seconds: float = 1.0,
) -> PaperRuntime:
    engine = PaperEngine(PaperStore(database), _config())
    return PaperRuntime(
        engine,
        strategy,
        source,
        config=PaperRuntimeConfig(
            timer_interval_seconds=timer_interval_seconds,
            source_poll_timeout_seconds=0.1,
        ),
        clock=clock,
    )


def test_runtime_reconciles_before_first_public_source_poll(tmp_path: Path) -> None:
    database = tmp_path / "paper.sqlite3"
    clock = MutableClock(_START + timedelta(seconds=2))
    strategy = HoldStrategy()
    market = _market("first", clock.value)
    observed_event_types: list[PaperEventType] = []

    def assert_reconciled_before_poll() -> None:
        store = PaperStore(database, initialize=False)
        projection = store.get_projection(_config().run_id)
        assert projection.reconciled is True
        observed_event_types.extend(
            event.event.event_type for event in store.get_events(_config().run_id)
        )

    source = QueuePublicSource([{_INSTRUMENT: market}], before_poll=assert_reconciled_before_poll)
    runtime = _runtime(database, source, strategy, clock)

    step = runtime.run_once()

    assert step.kind is PaperRuntimeStepKind.MARKET
    assert strategy.calls == 1
    assert source.poll_calls == 1
    assert observed_event_types[:2] == [
        PaperEventType.RUN_STARTED,
        PaperEventType.RECONCILIATION_SUCCEEDED,
    ]


def test_runtime_uses_injected_clock_for_durable_timer_and_never_polls_when_due(
    tmp_path: Path,
) -> None:
    clock = MutableClock(_START + timedelta(seconds=2))
    source = QueuePublicSource([None])
    runtime = _runtime(tmp_path / "paper.sqlite3", source, HoldStrategy(), clock)
    runtime.start()
    clock.value += timedelta(seconds=3)

    step = runtime.run_once()

    assert step.kind is PaperRuntimeStepKind.TIMER
    assert step.timer_result is not None
    assert source.poll_calls == 0
    assert step.timer_result.append.idempotent is False


def test_runtime_rejects_source_or_strategy_drift_before_admission(tmp_path: Path) -> None:
    clock = MutableClock(_START + timedelta(seconds=2))
    source = QueuePublicSource([None])
    strategy = HoldStrategy()
    runtime = _runtime(tmp_path / "paper.sqlite3", source, strategy, clock)
    runtime.start()
    strategy.strategy_hash = "c" * 64

    with pytest.raises(PaperAdmissionError, match="strategy identity changed"):
        runtime.run_once()
    assert source.poll_calls == 0

    strategy.strategy_hash = _STRATEGY_HASH
    source.descriptor = PublicSourceDescriptor(_SOURCE, "d" * 64)
    with pytest.raises(PaperAdmissionError, match="source identity changed"):
        runtime.run_once()
    assert source.poll_calls == 0


def test_runtime_rejects_unfrozen_strategy_at_construction(tmp_path: Path) -> None:
    clock = MutableClock(_START + timedelta(seconds=2))
    source = QueuePublicSource([None])
    strategy = HoldStrategy()
    strategy.strategy_hash = "c" * 64

    with pytest.raises(PaperAdmissionError, match="differs from the frozen paper configuration"):
        _runtime(tmp_path / "paper.sqlite3", source, strategy, clock)
    assert source.poll_calls == 0


def test_runtime_rejects_clock_rollback_before_source_poll(tmp_path: Path) -> None:
    clock = MutableClock(_START + timedelta(seconds=2))
    source = QueuePublicSource([None])
    runtime = _runtime(tmp_path / "paper.sqlite3", source, HoldStrategy(), clock)
    runtime.start()
    clock.value -= timedelta(seconds=1)

    with pytest.raises(PaperAdmissionError, match="clock moved backwards"):
        runtime.run_once()
    assert source.poll_calls == 0


def test_restart_clock_rollback_fails_before_appending_a_start_event(tmp_path: Path) -> None:
    database = tmp_path / "paper.sqlite3"
    first_clock = MutableClock(_START + timedelta(seconds=5))
    first_runtime = _runtime(database, QueuePublicSource([None]), HoldStrategy(), first_clock)
    first_runtime.start()
    before = database.read_bytes()

    restart_clock = MutableClock(_START + timedelta(seconds=4))
    restarted = _runtime(database, QueuePublicSource([None]), HoldStrategy(), restart_clock)
    with pytest.raises(PaperAdmissionError, match="precedes durable paper state"):
        restarted.start()
    assert database.read_bytes() == before


def test_restart_filters_a_durable_redelivery_before_strategy_decision(tmp_path: Path) -> None:
    database = tmp_path / "paper.sqlite3"
    first_clock = MutableClock(_START + timedelta(seconds=2))
    market = _market("redelivery", first_clock.value)
    first_strategy = HoldStrategy()
    first_runtime = _runtime(
        database,
        QueuePublicSource([{_INSTRUMENT: market}]),
        first_strategy,
        first_clock,
    )
    assert first_runtime.run_once().kind is PaperRuntimeStepKind.MARKET
    first_runtime.close()
    assert first_strategy.calls == 1

    restart_clock = MutableClock(_START + timedelta(seconds=3))
    restart_strategy = HoldStrategy()
    restart_source = QueuePublicSource([{_INSTRUMENT: market}])
    restarted = _runtime(database, restart_source, restart_strategy, restart_clock)

    step = restarted.run_once()

    assert step.kind is PaperRuntimeStepKind.DUPLICATE
    assert step.duplicate_event_ids == (market.event_id,)
    assert restart_strategy.calls == 0
    restart_events = PaperStore(database, initialize=False).get_events(_config().run_id)
    restart_exercises = [
        event
        for event in restart_events
        if event.event.event_type is PaperEventType.RESILIENCE_EXERCISE_RECORDED
    ]
    assert len(restart_exercises) == 1
    assert restart_exercises[0].event.payload["exercise"] == "RESTART"


def test_restart_rejects_divergent_redelivery_and_latches_manual_review(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    original_at = _START + timedelta(seconds=2)
    original = _market("divergent-redelivery", original_at)
    first_clock = MutableClock(original_at)
    first = _runtime(
        database,
        QueuePublicSource([{_INSTRUMENT: original}]),
        HoldStrategy(),
        first_clock,
    )
    assert first.run_once().kind is PaperRuntimeStepKind.MARKET

    divergent = replace(original, bid_price=Decimal("99"))
    restart_clock = MutableClock(_START + timedelta(seconds=3))
    restarted = _runtime(
        database,
        QueuePublicSource([{_INSTRUMENT: divergent}]),
        HoldStrategy(),
        restart_clock,
    )

    with pytest.raises(IdempotencyConflictError, match="different payload"):
        restarted.run_once()
    assert PaperStore(database, initialize=False).get_projection(
        _config().run_id
    ).state is PaperState.MANUAL_REVIEW


def test_runtime_rejects_market_older_than_frozen_staleness_limit(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    clock = MutableClock(_START + timedelta(seconds=2))
    stale_market = _market("runtime-stale", _START + timedelta(seconds=5))
    strategy = HoldStrategy()
    runtime = _runtime(
        database,
        QueuePublicSource([{_INSTRUMENT: stale_market}]),
        strategy,
        clock,
        timer_interval_seconds=60,
    )
    runtime.start()
    clock.value = _START + timedelta(seconds=40)

    with pytest.raises(PaperAdmissionError, match="stale_after_seconds"):
        runtime.run_once()
    assert strategy.calls == 0
    durable = PaperStore(database, initialize=False)
    assert not durable.contains_input(
        _config().run_id,
        stale_market.event_id,
    )
    assert durable.get_projection(_config().run_id).state is PaperState.PAUSED
    assert any(
        alert.code == "STALE_MARKET_DATA"
        for alert in durable.get_alerts(_config().run_id)
    )


def test_repeated_start_on_same_runtime_does_not_duplicate_restart_exercise(tmp_path: Path) -> None:
    database = tmp_path / "paper.sqlite3"
    first_clock = MutableClock(_START + timedelta(seconds=2))
    first = _runtime(database, QueuePublicSource([None]), HoldStrategy(), first_clock)
    first.start()
    first.close()

    restart_clock = MutableClock(_START + timedelta(seconds=3))
    restarted = _runtime(database, QueuePublicSource([None]), HoldStrategy(), restart_clock)
    first_startup = restarted.start()
    second_startup = restarted.start()

    assert first_startup is second_startup
    restart_events = [
        event
        for event in PaperStore(database, initialize=False).get_events(_config().run_id)
        if event.event.event_type is PaperEventType.RESILIENCE_EXERCISE_RECORDED
    ]
    assert len(restart_events) == 1


def test_unseen_out_of_order_public_event_fails_closed(tmp_path: Path) -> None:
    clock = MutableClock(_START + timedelta(seconds=10))
    old_market = _market("unseen-old", _START + timedelta(seconds=1))
    source = QueuePublicSource([{_INSTRUMENT: old_market}])
    runtime = _runtime(tmp_path / "paper.sqlite3", source, HoldStrategy(), clock)

    with pytest.raises(PaperAdmissionError, match="precedes durable paper state"):
        runtime.run_once()


def test_run_forever_stops_and_closes_source_cleanly(tmp_path: Path) -> None:
    clock = MutableClock(_START + timedelta(seconds=2))
    source = QueuePublicSource([None])
    runtime = _runtime(tmp_path / "paper.sqlite3", source, HoldStrategy(), clock)

    projection = runtime.run_forever(max_steps=1)

    assert projection.reconciled is True
    assert runtime.stopped is True
    assert source.stop_calls == 1
    assert source.close_calls == 1


def test_read_only_replay_does_not_change_store_bytes(tmp_path: Path) -> None:
    database = tmp_path / "paper.sqlite3"
    clock = MutableClock(_START + timedelta(seconds=2))
    runtime = _runtime(database, QueuePublicSource([None]), HoldStrategy(), clock)
    runtime.start()
    before = database.read_bytes()

    result = replay_paper_run(PaperStore(database, initialize=False), _config().run_id)

    assert result.event_count == 2
    assert result.to_dict()["status"] == "REPLAY_EXACT"
    assert database.read_bytes() == before


def test_paper_cli_exposes_only_explicit_paper_subcommands() -> None:
    result = CliRunner().invoke(app, ["paper", "--help"], env={"COLUMNS": "160"})

    assert result.exit_code == 0
    for command in ("status", "replay", "reconcile", "run"):
        assert command in result.stdout


def test_paper_cli_status_and_replay_are_byte_for_byte_read_only(tmp_path: Path) -> None:
    database = tmp_path / "paper.sqlite3"
    clock = MutableClock(_START + timedelta(seconds=2))
    runtime = _runtime(database, QueuePublicSource([None]), HoldStrategy(), clock)
    runtime.start()
    before = database.read_bytes()
    runner = CliRunner()

    status = runner.invoke(
        app,
        ["paper", "status", "--database", str(database), "--run-id", _config().run_id],
    )
    replay = runner.invoke(
        app,
        ["paper", "replay", _config().run_id, "--database", str(database)],
    )

    assert status.exit_code == 0, status.output
    assert replay.exit_code == 0, replay.output
    assert json.loads(status.stdout)["orders_enabled"] is False
    assert json.loads(replay.stdout)["status"] == "REPLAY_EXACT"
    assert database.read_bytes() == before


def test_paper_cli_status_masks_corruption_without_mutating_store(tmp_path: Path) -> None:
    database = tmp_path / "paper.sqlite3"
    clock = MutableClock(_START + timedelta(seconds=2))
    _runtime(database, QueuePublicSource([None]), HoldStrategy(), clock).start()
    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER paper_events_no_update")
        connection.execute(
            "UPDATE paper_events SET payload_json = ? WHERE run_id = ? AND sequence = 1",
            ("{}", _config().run_id),
        )
    corrupt_bytes = database.read_bytes()

    result = CliRunner().invoke(
        app,
        ["paper", "status", "--database", str(database), "--run-id", _config().run_id],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["integrity"] == "FAILED_READONLY"
    assert payload["status"] == "MANUAL_REVIEW"
    assert payload["projection"] is None
    assert payload["orders_enabled"] is False
    assert database.read_bytes() == corrupt_bytes


def test_paper_cli_run_fails_closed_without_static_approval(tmp_path: Path) -> None:
    artifact = tmp_path / "paper-config.json"
    artifact.write_text(canonical_json(_config().to_dict()), encoding="utf-8")
    database = tmp_path / "must-not-exist.sqlite3"

    result = CliRunner().invoke(
        app,
        ["paper", "run", str(artifact), "--database", str(database)],
    )

    assert result.exit_code == 2
    assert "Aucune liaison figée" in result.output
    assert not database.exists()
