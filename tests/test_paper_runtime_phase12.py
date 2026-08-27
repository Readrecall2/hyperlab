from __future__ import annotations

import json
import signal
import sqlite3
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal, localcontext
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from typer.testing import CliRunner

import hyperlab.cli as cli_module
from hyperlab.backtest.protocol import canonical_json, canonical_sha256
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
    utc_text,
)
from hyperlab.paper.public_source import PublicFundingSettlement
from hyperlab.paper.runner import PaperStrategyView
from hyperlab.paper.runtime import (
    PaperAdmissionError,
    PaperRuntime,
    PaperRuntimeConfig,
    PaperRuntimeLease,
    PaperRuntimeStepKind,
    PaperStartupInterrupted,
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


class RestoreHoldStrategy(HoldStrategy):
    def __init__(self) -> None:
        super().__init__()
        self.restore_calls = 0
        self.restored_event_ids: list[str] = []

    def restore(
        self,
        markets: Iterable[MarketEvent],
        view: PaperStrategyView,
    ) -> None:
        assert view.config_hash
        self.restore_calls += 1
        self.restored_event_ids = [market.event_id for market in markets]


class SimulatedHardCrash(BaseException):
    pass


class RaisingStrategy(HoldStrategy):
    def __init__(
        self,
        error: BaseException,
        *,
        before_raise: Callable[[], None] | None = None,
    ) -> None:
        super().__init__()
        self.error = error
        self.before_raise = before_raise

    def decide(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None:
        assert markets
        assert view.config_hash
        if self.before_raise is not None:
            self.before_raise()
        raise self.error


class QueuePublicSource:
    def __init__(
        self,
        frames: list[object | None],
        *,
        descriptor: PublicSourceDescriptor | None = None,
        before_start: object | None = None,
        before_poll: object | None = None,
    ) -> None:
        self._descriptor = descriptor or PublicSourceDescriptor(_SOURCE, _DATA_HASH)
        self.frames = list(frames)
        self.before_start = before_start
        self.before_poll = before_poll
        self.start_calls = 0
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

    def start(self) -> None:
        self.start_calls += 1
        if callable(self.before_start):
            self.before_start()

    def poll(self, *, timeout_seconds: float) -> object | None:
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
        runtime_source_poll_timeout_seconds=0.1,
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
    run_config: PaperRunConfig | None = None,
) -> PaperRuntime:
    selected_config = run_config or _config()
    if selected_config.runtime_timer_interval_seconds != timer_interval_seconds:
        selected_config = replace(
            selected_config,
            runtime_timer_interval_seconds=timer_interval_seconds,
        )
    engine = PaperEngine(PaperStore(database), selected_config)
    return PaperRuntime(
        engine,
        strategy,
        source,
        config=PaperRuntimeConfig(
            timer_interval_seconds=timer_interval_seconds,
            source_poll_timeout_seconds=selected_config.runtime_source_poll_timeout_seconds,
        ),
        clock=clock,
    )


def test_runtime_rejects_temporary_historical_replay_store(tmp_path: Path) -> None:
    temporary_directory = TemporaryDirectory(dir=tmp_path)
    store = PaperStore._create_temporary_historical_replay(
        temporary_directory,
        filename="runtime-rejected.sqlite3",
    )
    engine = PaperEngine._for_historical_replay(store, _config())

    with pytest.raises(
        PaperAdmissionError,
        match="rejects temporary historical replay-only stores",
    ):
        PaperRuntime(
            engine,
            HoldStrategy(),
            QueuePublicSource([]),
        )
    store.close()
    temporary_directory.cleanup()


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
        observed_event_types.extend(event.event.event_type for event in store.get_events(_config().run_id))

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


def test_runtime_starts_lazy_source_after_reconcile_and_strategy_restore(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    clock = MutableClock(_START + timedelta(seconds=2))
    strategy = RestoreHoldStrategy()

    def assert_safe_start_order() -> None:
        projection = PaperStore(database, initialize=False).get_projection(_config().run_id)
        assert projection.reconciled is True
        assert strategy.restore_calls == 1

    source = QueuePublicSource([None], before_start=assert_safe_start_order)
    runtime = _runtime(database, source, strategy, clock)

    runtime.start()

    assert source.start_calls == 1
    assert source.poll_calls == 0
    assert strategy.restored_event_ids == []


def test_public_source_poll_failure_is_durably_paused_and_replay_exact(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    clock = MutableClock(_START + timedelta(seconds=2))

    def fail_poll() -> None:
        raise RuntimeError("fixture detail must not be persisted")

    runtime = _runtime(
        database,
        QueuePublicSource([None], before_poll=fail_poll),
        HoldStrategy(),
        clock,
    )

    with pytest.raises(PaperAdmissionError, match="public source failed closed"):
        runtime.run_once()

    store = PaperStore(database, initialize=False)
    projection = store.get_projection(_config().run_id)
    assert projection.state is PaperState.PAUSED
    source_failures = list(store.iter_inputs(_config().run_id, input_type="PUBLIC_SOURCE_FAILURE"))
    assert len(source_failures) == 1
    assert source_failures[0].payload["reason"] == ("terminal public source failure: RuntimeError")
    assert "fixture detail" not in json.dumps(source_failures[0].payload)
    replay = replay_paper_run(store, _config().run_id)
    assert replay.projection_hash == projection.canonical_hash
    assert replay.to_dict()["status"] == "REPLAY_EXACT"


@pytest.mark.parametrize(
    ("item", "error_match"),
    [
        (object(), "unsupported item"),
        ({}, "non-empty normalized market frame"),
        (
            {_INSTRUMENT: _market("future-returned-bbo", _START + timedelta(seconds=5))},
            "ahead of the runtime clock",
        ),
        (
            PublicFundingSettlement(
                event_id="c" * 64,
                instrument=_INSTRUMENT,
                funding_time=_START + timedelta(seconds=4),
                received_at=_START + timedelta(seconds=5),
                funding_rate=Decimal("0.0001"),
                funding_interval_seconds=3_600,
                rate_kind="hyperliquid-hourly-settlement",
                mark_price=None,
                oracle_price=None,
                source_observation_id="future-returned-funding",
            ),
            "ahead of the runtime clock",
        ),
    ],
    ids=("unsupported", "malformed-frame", "future-bbo", "future-funding"),
)
def test_invalid_returned_source_item_is_durably_paused_before_original_error(
    tmp_path: Path,
    item: object,
    error_match: str,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = replace(_config(), required_instruments=(_INSTRUMENT,))
    runtime = _runtime(
        database,
        QueuePublicSource([item]),
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=2)),
        run_config=config,
    )

    with pytest.raises(PaperAdmissionError, match=error_match):
        runtime.run_once()

    store = PaperStore(database, initialize=False)
    projection = store.get_projection(config.run_id)
    failures = list(store.iter_inputs(config.run_id, input_type="PUBLIC_SOURCE_FAILURE"))
    assert projection.state is PaperState.PAUSED
    assert len(failures) == 1
    assert failures[0].payload["reason"] == ("terminal public source failure: PaperAdmissionError")
    assert replay_paper_run(store, config.run_id).projection_hash == (projection.canonical_hash)
    runtime.close()


def test_public_source_poll_failure_is_durable_while_runtime_already_paused(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    clock = MutableClock(_START + timedelta(seconds=2))

    def fail_poll() -> None:
        raise RuntimeError("paused runtime source failure fixture")

    runtime = _runtime(
        database,
        QueuePublicSource([None], before_poll=fail_poll),
        HoldStrategy(),
        clock,
        timer_interval_seconds=60,
    )
    runtime.start()
    clock.value += timedelta(seconds=1)
    paused = runtime.engine.pause(
        as_of=clock.value,
        reason="operator inspection fixture",
        operator_artifact_hash="5" * 64,
    )
    assert paused.projection.state is PaperState.PAUSED
    run_id = runtime.engine.run_id
    clock.value += timedelta(seconds=1)

    with pytest.raises(PaperAdmissionError, match="public source failed closed"):
        runtime.run_once()

    store = PaperStore(database, initialize=False)
    assert store.get_projection(run_id).state is PaperState.PAUSED
    failures = list(store.iter_inputs(run_id, input_type="PUBLIC_SOURCE_FAILURE"))
    assert len(failures) == 1


def test_restart_streams_durable_markets_into_strategy_before_new_poll(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    first_at = _START + timedelta(seconds=2)
    first_market = _market("restore-first", first_at)
    first = _runtime(
        database,
        QueuePublicSource([{_INSTRUMENT: first_market}]),
        HoldStrategy(),
        MutableClock(first_at),
    )
    assert first.run_once().kind is PaperRuntimeStepKind.MARKET
    first.close()

    second_at = _START + timedelta(seconds=3)
    second_market = _market("restore-second", second_at)
    strategy = RestoreHoldStrategy()
    restarted = _runtime(
        database,
        QueuePublicSource([{_INSTRUMENT: second_market}]),
        strategy,
        MutableClock(second_at),
    )

    step = restarted.run_once()

    assert step.kind is PaperRuntimeStepKind.MARKET
    assert strategy.restore_calls == 1
    assert strategy.restored_event_ids == [first_market.event_id]
    assert strategy.calls == 1


def test_restart_reuses_one_integrity_scan_and_one_event_replay_before_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "single-verified-restart.sqlite3"
    first = _runtime(
        database,
        QueuePublicSource([{_INSTRUMENT: _market("single-pass", _START + timedelta(seconds=1))}]),
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=1)),
    )
    first.run_once()
    first.close()
    restarted = _runtime(
        database,
        QueuePublicSource([None]),
        RestoreHoldStrategy(),
        MutableClock(_START + timedelta(seconds=2)),
    )
    integrity_calls = 0
    replay_calls = 0
    original_integrity = restarted.engine.store.verify_integrity
    original_replay = restarted.engine.replay

    def counted_integrity(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal integrity_calls
        integrity_calls += 1
        return original_integrity(*args, **kwargs)

    def counted_replay(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal replay_calls
        replay_calls += 1
        return original_replay(*args, **kwargs)

    monkeypatch.setattr(restarted.engine.store, "verify_integrity", counted_integrity)
    monkeypatch.setattr(restarted.engine, "replay", counted_replay)
    startup = restarted.start()
    assert integrity_calls == 1
    assert replay_calls == 1
    assert startup.projection.runtime_session_generation == 2
    restarted.close()


@pytest.mark.parametrize("signal_name", ["SIGINT", "SIGTERM"])
@pytest.mark.parametrize("interrupt_phase", ["integrity", "strategy_restore"])
def test_signal_interrupts_startup_without_new_durable_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    signal_name: str,
    interrupt_phase: str,
) -> None:
    database = tmp_path / f"interrupt-{signal_name.casefold()}.sqlite3"
    first = _runtime(
        database,
        QueuePublicSource([{_INSTRUMENT: _market("interrupt-history", _START + timedelta(seconds=1))}]),
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=1)),
    )
    first.run_once()
    first.close()
    before_store = PaperStore(database, initialize=False)
    before = before_store.get_run(_config().run_id)
    before_projection = before_store.get_projection(_config().run_id)
    before_store.close()
    source = QueuePublicSource([None])
    strategy = RestoreHoldStrategy()
    restarted = _runtime(
        database,
        source,
        strategy,
        MutableClock(_START + timedelta(seconds=2)),
    )
    requested = False
    signal_requested_at: float | None = None
    original_observer = restarted.engine.store._observe_integrity_buffer
    original_restore = strategy.restore

    def raise_requested_signal() -> None:
        nonlocal requested, signal_requested_at
        requested = True
        signal_requested_at = time.perf_counter()
        handler = signal.getsignal(getattr(signal, signal_name))
        assert callable(handler)
        signal.raise_signal(getattr(signal, signal_name))

    def request_signal(name: str, size: int) -> None:
        original_observer(name, size)
        if not requested and name == "event_row":
            raise_requested_signal()

    def interrupting_restore(
        markets: Iterable[MarketEvent],
        view: PaperStrategyView,
    ) -> None:
        def interrupting_markets() -> Iterable[MarketEvent]:
            for market in markets:
                if not requested:
                    raise_requested_signal()
                yield market

        original_restore(interrupting_markets(), view)

    if interrupt_phase == "integrity":
        monkeypatch.setattr(
            restarted.engine.store,
            "_observe_integrity_buffer",
            request_signal,
        )
    else:
        monkeypatch.setattr(strategy, "restore", interrupting_restore)
    with (
        cli_module._cooperative_signal_handlers(restarted.stop),
        pytest.raises(PaperStartupInterrupted, match="startup interrupted"),
    ):
        restarted.start()
    assert signal_requested_at is not None
    signal_response_seconds = time.perf_counter() - signal_requested_at
    after_store = PaperStore(database, initialize=False)
    after = after_store.get_run(_config().run_id)
    after_projection = after_store.get_projection(_config().run_id)
    after_store.close()
    assert requested is True
    assert signal_response_seconds < 1.0
    assert after.head_identity == before.head_identity
    assert after_projection.to_dict() == before_projection.to_dict()
    assert after_projection.runtime_session_generation == 1
    assert after_projection.runtime_session_active is False
    assert source.start_calls == 0
    with PaperRuntimeLease(database, _config().run_id):
        pass


def test_public_funding_is_durable_idempotent_and_conflicts_fail_closed(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = replace(_config(), required_instruments=(_INSTRUMENT,))
    received_at = _START + timedelta(seconds=2)
    settlement = PublicFundingSettlement(
        event_id=deterministic_id("phase12_runtime_funding", "hour-1"),
        instrument=_INSTRUMENT,
        funding_time=_START + timedelta(seconds=1),
        received_at=received_at,
        funding_rate=Decimal("0.0001"),
        funding_interval_seconds=3600,
        rate_kind="hyperliquid-hourly-settlement",
        mark_price=None,
        oracle_price=None,
        source_observation_id="fixture-observation-hour-1",
    )
    first = _runtime(
        database,
        QueuePublicSource([settlement]),
        HoldStrategy(),
        MutableClock(received_at),
        run_config=config,
    )

    first_step = first.run_once()
    first.close()

    assert first_step.kind is PaperRuntimeStepKind.FUNDING
    assert first_step.funding_event_id == settlement.event_id
    assert first_step.projection.realized_pnl == 0
    funding_input_id = deterministic_id("paper_funding_input", config.run_id, settlement.event_id)
    durable = PaperStore(database, initialize=False)
    funding_input = durable.get_input(config.run_id, funding_input_id)
    assert funding_input is not None
    assert funding_input.payload["mark_source"] == "FLAT_NO_MARK"
    assert funding_input.payload["applicability"] == "PRE_ACTIVATION_IGNORED"
    assert "mark_price" not in funding_input.payload

    refetched = replace(
        settlement,
        received_at=_START + timedelta(seconds=3),
        source_observation_id="fixture-observation-hour-1-refetch",
    )
    duplicate = _runtime(
        database,
        QueuePublicSource([refetched]),
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=3)),
        run_config=config,
    )
    duplicate_step = duplicate.run_once()
    duplicate.close()

    assert duplicate_step.kind is PaperRuntimeStepKind.DUPLICATE
    assert duplicate_step.duplicate_event_ids == (settlement.event_id,)
    assert (
        len(
            [
                event
                for event in durable.get_events(config.run_id)
                if event.event.event_type is PaperEventType.FUNDING_POSTED
            ]
        )
        == 1
    )
    assert replay_paper_run(durable, config.run_id).run_id == config.run_id

    corrected = replace(
        settlement,
        received_at=_START + timedelta(seconds=4),
        funding_rate=Decimal("0.0002"),
        source_observation_id="fixture-observation-hour-1-correction",
    )
    conflict = _runtime(
        database,
        QueuePublicSource([corrected]),
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=4)),
        run_config=config,
    )
    with pytest.raises(IdempotencyConflictError, match="different payload"):
        conflict.run_once()
    assert durable.get_projection(config.run_id).state is PaperState.MANUAL_REVIEW


def test_runtime_v2_funding_amount_is_independent_of_ambient_decimal_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "ambient-funding.sqlite3"
    config = replace(_config(), required_instruments=(_INSTRUMENT,))
    clock = MutableClock(_START)
    runtime = _runtime(
        database,
        QueuePublicSource([]),
        HoldStrategy(),
        clock,
        run_config=config,
    )
    runtime.start()
    quantity = Decimal("-1.60824649096141304")
    mark = Decimal("25627.3357")
    rate = Decimal("0.0000097375")
    expected_amount = Decimal("0.4013317705351950009472354289")
    historical = runtime.engine.projection().clone()
    historical.positions[_INSTRUMENT] = quantity
    monkeypatch.setattr(
        runtime.engine.store,
        "get_projection_before_received_at",
        lambda *_args, **_kwargs: historical,
    )
    received_at = _START + timedelta(seconds=2)
    settlement = PublicFundingSettlement(
        event_id=deterministic_id("phase12_runtime_funding", "ambient-context"),
        instrument=_INSTRUMENT,
        funding_time=_START + timedelta(seconds=1),
        received_at=received_at,
        funding_rate=rate,
        funding_interval_seconds=3600,
        rate_kind="synthetic-hourly-settlement",
        mark_price=mark,
        oracle_price=None,
        source_observation_id="ambient-context-funding",
    )

    with localcontext() as context:
        context.prec = 9
        context.rounding = ROUND_DOWN
        step = runtime._funding_step(settlement, received_at)

    funding_input = runtime.engine.store.get_input(
        config.run_id,
        deterministic_id("paper_funding_input", config.run_id, settlement.event_id),
    )
    runtime.close()

    assert step.kind is PaperRuntimeStepKind.FUNDING
    assert step.funding_result is not None
    assert funding_input is not None
    assert funding_input.payload["amount"] == str(expected_amount)
    assert step.projection.realized_pnl == expected_amount


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
    first_runtime.close()
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
    first.close()

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
    durable = PaperStore(database, initialize=False)
    assert durable.get_projection(_config().run_id).state is PaperState.MANUAL_REVIEW
    # The terminal store latch forbids a second failure append; the original
    # idempotency error and its durable MANUAL_REVIEW transition are authoritative.
    assert list(durable.iter_inputs(_config().run_id, input_type="PUBLIC_SOURCE_FAILURE")) == []


def test_fresh_producer_fifo_backlog_reaches_old_event_and_fails_closed(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    clock = MutableClock(_START + timedelta(seconds=2))
    stale_market = _market("runtime-stale-fifo-head", _START + timedelta(seconds=20))
    producer_latest = _market(
        "runtime-fresh-fifo-tail",
        _START + timedelta(seconds=30, milliseconds=900),
    )
    strategy = HoldStrategy()
    config = replace(
        _config(),
        risk=replace(_config().risk, stale_after_seconds=10),
        runtime_timer_interval_seconds=60,
    )
    source = QueuePublicSource(
        [
            {_INSTRUMENT: stale_market},
            {_INSTRUMENT: producer_latest},
        ]
    )
    runtime = _runtime(
        database,
        source,
        strategy,
        clock,
        timer_interval_seconds=60,
        run_config=config,
    )
    runtime.start()
    clock.value = _START + timedelta(seconds=31)

    with pytest.raises(PaperAdmissionError, match="stale_after_seconds"):
        runtime.run_once()
    assert strategy.calls == 0
    assert source.frames == [{_INSTRUMENT: producer_latest}]
    assert (clock.value - producer_latest.received_at).total_seconds() == pytest.approx(0.1)
    assert config.risk.stale_after_seconds == 10
    durable = PaperStore(database, initialize=False)
    assert not durable.contains_input(
        config.run_id,
        stale_market.event_id,
    )
    assert durable.get_projection(config.run_id).state is PaperState.PAUSED
    assert any(alert.code == "STALE_MARKET_DATA" for alert in durable.get_alerts(config.run_id))
    failures = list(durable.iter_inputs(config.run_id, input_type="PUBLIC_SOURCE_FAILURE"))
    assert len(failures) == 1


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


def test_pre_activation_market_is_observed_without_strategy_or_economic_effect(
    tmp_path: Path,
) -> None:
    clock = MutableClock(_START + timedelta(seconds=10))
    old_market = _market("unseen-old", _START + timedelta(seconds=1))
    source = QueuePublicSource([{_INSTRUMENT: old_market}])
    strategy = HoldStrategy()
    database = tmp_path / "paper.sqlite3"
    runtime = _runtime(database, source, strategy, clock)

    step = runtime.run_once()

    assert step.kind is PaperRuntimeStepKind.MARKET
    assert step.runner_result is not None
    assert step.runner_result.decision_result is None
    assert strategy.calls == 0
    durable = PaperStore(database, initialize=False)
    projection = durable.get_projection(_config().run_id)
    assert projection.marks[_INSTRUMENT] == Decimal("100.5")
    assert projection.last_market_received_at_by_instrument[_INSTRUMENT] == old_market.received_at
    assert projection.last_received_at == clock.value
    source_input = durable.get_input(_config().run_id, old_market.event_id)
    assert source_input is not None
    assert source_input.payload["processed_at"] == utc_text(clock.value)
    assert source_input.payload["execution_policy"] == "SOURCE_CHRONOLOGY_OBSERVE_ONLY"


def test_run_forever_stops_and_closes_source_cleanly(tmp_path: Path) -> None:
    clock = MutableClock(_START + timedelta(seconds=2))
    source = QueuePublicSource([None])
    runtime = _runtime(tmp_path / "paper.sqlite3", source, HoldStrategy(), clock)

    projection = runtime.run_forever(max_steps=1)

    assert projection.reconciled is True
    assert runtime.stopped is True
    assert source.stop_calls == 1
    assert source.close_calls == 1


def test_clean_runtime_session_stops_and_restarts_without_failure(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    first = _runtime(
        database,
        QueuePublicSource([None]),
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=2)),
    )

    started = first.start().projection
    assert started.runtime_session_active is True
    assert started.runtime_session_generation == 1
    first.close()

    durable = PaperStore(database, initialize=False)
    stopped = durable.get_projection(_config().run_id)
    assert stopped.runtime_session_active is False
    assert stopped.runtime_session_stopped_at == _START + timedelta(seconds=2)
    assert list(durable.iter_inputs(_config().run_id, input_type="PAPER_RUNTIME_FAILURE")) == []
    assert replay_paper_run(durable, _config().run_id).projection_hash == (stopped.canonical_hash)

    second = _runtime(
        database,
        QueuePublicSource([None]),
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=3)),
    )
    restarted = second.start().projection
    assert restarted.runtime_session_active is True
    assert restarted.runtime_session_generation == 2
    second.close()


def test_strategy_exception_is_one_durable_runtime_failure_and_clean_session_stop(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    clock = MutableClock(_START + timedelta(seconds=2))
    market = _market("strategy-runtime-failure", clock.value)
    runtime = _runtime(
        database,
        QueuePublicSource([{_INSTRUMENT: market}]),
        RaisingStrategy(RuntimeError("secret strategy detail")),
        clock,
    )

    with pytest.raises(RuntimeError, match="secret strategy detail"):
        runtime.run_once()
    runtime.close()

    durable = PaperStore(database, initialize=False)
    projection = durable.get_projection(_config().run_id)
    failures = list(durable.iter_inputs(_config().run_id, input_type="PAPER_RUNTIME_FAILURE"))
    assert durable.contains_input(_config().run_id, market.event_id)
    assert projection.state is PaperState.PAUSED
    assert projection.runtime_session_active is False
    assert len(failures) == 1
    assert failures[0].payload["reason"] == (
        "terminal paper runtime failure: MARKET_EVALUATION: RuntimeError"
    )
    assert "secret strategy detail" not in json.dumps(failures[0].payload)
    assert replay_paper_run(durable, _config().run_id).projection_hash == (projection.canonical_hash)


def test_runtime_failure_latches_an_unreadable_projection_without_fake_journal(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    clock = MutableClock(_START + timedelta(seconds=2))
    market = _market("unreadable-projection-runtime-failure", clock.value)
    run_id = _config().run_id
    anchors: dict[str, object] = {}

    def corrupt_after_market_commit() -> None:
        inspection = PaperStore(database, initialize=False)
        assert inspection.contains_input(run_id, market.event_id)
        assert inspection.latch_unreadable_projection(run_id) is False
        run = inspection.get_run(run_id)
        payload = inspection.get_projection_payload(run_id)
        assert payload["runtime_session_generation"] == 1
        assert payload["runtime_session_id"] is not None
        assert payload["runtime_session_started_at"] is not None
        assert payload["runtime_session_stopped_at"] is None
        anchors.update(
            {
                "event_sequence": run.event_sequence,
                "event_head_hash": run.event_head_hash,
                "commit_sequence": run.commit_sequence,
                "commit_head_hash": run.commit_head_hash,
                "projection_revision": run.projection_revision,
                "projection_hash": run.projection_hash,
                "event_types": tuple(event.event.event_type for event in inspection.iter_events(run_id)),
                "runtime_session_id": payload["runtime_session_id"],
                "runtime_session_generation": payload["runtime_session_generation"],
                "runtime_session_started_at": payload["runtime_session_started_at"],
                "runtime_session_stopped_at": payload["runtime_session_stopped_at"],
            }
        )
        invalid_payload = dict(payload)
        invalid_payload["archived_order_count"] = -1
        with sqlite3.connect(database) as connection:
            connection.execute(
                "UPDATE paper_projections SET payload_json=? WHERE run_id=?",
                (canonical_json(invalid_payload), run_id),
            )

    runtime = _runtime(
        database,
        QueuePublicSource([{_INSTRUMENT: market}]),
        RaisingStrategy(
            RuntimeError("original evaluation failure"),
            before_raise=corrupt_after_market_commit,
        ),
        clock,
    )

    with pytest.raises(RuntimeError, match="original evaluation failure") as failure:
        runtime.run_once()
    assert all(
        "paper runtime failure persistence also failed" not in note
        for note in getattr(failure.value, "__notes__", ())
    )
    runtime.close()

    durable = PaperStore(database, initialize=False)
    after_run = durable.get_run(run_id)
    alerts = tuple(
        alert for alert in durable.get_alerts(run_id) if alert.code == "PAPER_STORE_INTEGRITY_FAILURE"
    )
    assert after_run.status == PaperState.MANUAL_REVIEW.value
    assert after_run.event_sequence == anchors["event_sequence"]
    assert after_run.event_head_hash == anchors["event_head_hash"]
    assert after_run.commit_sequence == anchors["commit_sequence"]
    assert after_run.commit_head_hash == anchors["commit_head_hash"]
    assert after_run.projection_revision == anchors["projection_revision"]
    assert after_run.projection_hash == anchors["projection_hash"]
    assert len(alerts) == 1
    assert alerts[0].commit_sequence is None
    assert alerts[0].alert["detail"] == ("durable projection cannot be reconstructed by PaperProjection")
    assert alerts[0].alert["issues"] == [
        {
            "code": "CURRENT_PROJECTION_MODEL_INVALID",
            "detail": ("ValueError: projection archived_order_count must be a non-negative integer"),
        }
    ]
    assert durable.contains_input(run_id, market.event_id)
    assert list(durable.iter_inputs(run_id, input_type="PAPER_RUNTIME_FAILURE")) == []
    assert tuple(event.event.event_type for event in durable.iter_events(run_id)) == anchors["event_types"]
    with sqlite3.connect(database) as connection:
        row = connection.execute(
            "SELECT payload_json, effective_status FROM paper_projections WHERE run_id=?",
            (run_id,),
        ).fetchone()
    assert row is not None
    raw_payload = json.loads(str(row[0]))
    assert row[1] == PaperState.MANUAL_REVIEW.value
    assert raw_payload["archived_order_count"] == -1
    for field in (
        "runtime_session_id",
        "runtime_session_generation",
        "runtime_session_started_at",
        "runtime_session_stopped_at",
    ):
        assert raw_payload[field] == anchors[field]
    assert raw_payload["runtime_session_generation"] > 0
    assert raw_payload["runtime_session_id"] is not None
    assert raw_payload["runtime_session_stopped_at"] is None

    blocked_source = QueuePublicSource([None])
    blocked = _runtime(database, blocked_source, HoldStrategy(), clock)
    with pytest.raises(
        PaperAdmissionError,
        match="model-invalid; MANUAL_REVIEW latched",
    ):
        blocked.start()
    assert blocked_source.start_calls == 0
    assert (
        len(
            tuple(
                alert
                for alert in PaperStore(database, initialize=False).get_alerts(run_id)
                if alert.code == "PAPER_STORE_INTEGRITY_FAILURE"
            )
        )
        == 1
    )


def test_hard_crash_requires_one_offline_review_then_replacement_session(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _config()
    clock = MutableClock(_START + timedelta(seconds=2))
    market = _market("hard-crash-after-market", clock.value)
    crashed = _runtime(
        database,
        QueuePublicSource([{_INSTRUMENT: market}]),
        RaisingStrategy(SimulatedHardCrash("after market before decision")),
        clock,
        run_config=config,
    )

    with pytest.raises(SimulatedHardCrash, match="after market"):
        crashed.run_once()
    crashed.close()

    durable = PaperStore(database, initialize=False)
    ambiguous = durable.get_projection(config.run_id)
    assert durable.contains_input(config.run_id, market.event_id)
    assert ambiguous.runtime_session_active is True
    assert list(durable.iter_inputs(config.run_id, input_type="PAPER_RUNTIME_FAILURE")) == []

    blocked_source = QueuePublicSource([None])
    blocked = _runtime(
        database,
        blocked_source,
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=3)),
        run_config=config,
    )
    with pytest.raises(PaperAdmissionError, match="explicit reviewed recovery"):
        blocked.start()
    assert blocked_source.start_calls == 0

    paused = durable.get_projection(config.run_id)
    failures = list(durable.iter_inputs(config.run_id, input_type="PAPER_RUNTIME_FAILURE"))
    assert paused.state is PaperState.PAUSED
    assert paused.runtime_session_active is True
    assert len(failures) == 1
    assert failures[0].payload["reason"] == (
        "terminal paper runtime failure: UNCLOSED_RUNTIME_SESSION: UnclosedRuntimeSessionError"
    )
    assert replay_paper_run(durable, config.run_id).projection_hash == (paused.canonical_hash)

    review_engine = PaperEngine(PaperStore(database, initialize=False), config)
    with PaperRuntimeLease(database, config.run_id):
        reviewed = review_engine.resume_from_pause(
            as_of=_START + timedelta(seconds=4),
            review_artifact_hash="9" * 64,
            reviewed_critical_incident_count=paused.critical_incident_count,
            reviewed_last_critical_incident_at=paused.last_critical_incident_at,
            recovery_mode="OFFLINE_UNCLOSED_SESSION",
        ).projection
    assert reviewed.state is PaperState.FLAT
    assert reviewed.runtime_session_active is True

    recovered_source = QueuePublicSource([None])
    recovered = _runtime(
        database,
        recovered_source,
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=5)),
        run_config=config,
    )
    replacement = recovered.start().projection
    assert replacement.runtime_session_active is True
    assert replacement.runtime_session_generation == 2
    assert recovered_source.start_calls == 1
    recovered.close()


def test_read_only_replay_does_not_change_store_bytes(tmp_path: Path) -> None:
    database = tmp_path / "paper.sqlite3"
    clock = MutableClock(_START + timedelta(seconds=2))
    runtime = _runtime(database, QueuePublicSource([None]), HoldStrategy(), clock)
    runtime.start()
    before = database.read_bytes()

    result = replay_paper_run(PaperStore(database, initialize=False), _config().run_id)

    assert result.event_count == 3
    assert result.to_dict()["status"] == "REPLAY_EXACT"
    assert database.read_bytes() == before
    runtime.close()


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
    live_bytes = database.read_bytes()
    runner = CliRunner()

    status = runner.invoke(
        app,
        ["paper", "status", "--database", str(database), "--run-id", _config().run_id],
    )
    active_replay = runner.invoke(
        app,
        ["paper", "replay", _config().run_id, "--database", str(database)],
    )

    assert status.exit_code == 0, status.output
    assert json.loads(status.stdout)["orders_enabled"] is False
    assert active_replay.exit_code != 0
    assert "paper runtime already active" in active_replay.output
    assert database.read_bytes() == live_bytes

    runtime.close()
    stopped_bytes = database.read_bytes()
    replay = runner.invoke(
        app,
        ["paper", "replay", _config().run_id, "--database", str(database)],
    )

    assert replay.exit_code == 0, replay.output
    assert json.loads(replay.stdout)["status"] == "REPLAY_EXACT"
    assert database.read_bytes() == stopped_bytes


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
    assert payload["integrity"] == "HEAD_ANCHORS_FAILED_READONLY"
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


def test_runtime_exclusive_lease_rejects_second_writer_then_releases_on_close(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    first_source = QueuePublicSource([None])
    config = _config()
    second_source = QueuePublicSource([None])
    first = _runtime(
        database,
        first_source,
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=2)),
        run_config=config,
    )
    second = _runtime(
        database,
        second_source,
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=3)),
        run_config=config,
    )

    first.start()
    with pytest.raises(PaperAdmissionError, match="already active"):
        PaperRuntimeLease(database, config.run_id)
    with pytest.raises(PaperAdmissionError, match="already active"):
        second.start()
    assert second_source.start_calls == 0

    first.close()
    with PaperRuntimeLease(database, config.run_id):
        pass
    second.start()
    assert second_source.start_calls == 1
    second.close()


def test_runtime_admission_failure_releases_exact_lease(tmp_path: Path) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _config()

    def fail_source_start() -> None:
        raise RuntimeError("post-acquisition source start fixture")

    failed_source = QueuePublicSource([None], before_start=fail_source_start)
    failing = _runtime(
        database,
        failed_source,
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=2)),
        run_config=config,
    )

    with pytest.raises(PaperAdmissionError, match="failed during admission"):
        failing.start()

    with PaperRuntimeLease(database, config.run_id):
        pass

    recovered_source = QueuePublicSource([None])
    recovered = _runtime(
        database,
        recovered_source,
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=3)),
        run_config=config,
    )
    startup = recovered.start()
    assert startup.projection.state is PaperState.PAUSED
    assert recovered_source.start_calls == 1
    recovered.close()


def test_runtime_rejects_cadence_or_release_drift_before_source_and_lease(
    tmp_path: Path,
) -> None:
    cadence_database = tmp_path / "cadence.sqlite3"
    config = _config()
    cadence_source = QueuePublicSource([None])
    with pytest.raises(PaperAdmissionError, match="cadence differs"):
        PaperRuntime(
            PaperEngine(PaperStore(cadence_database), config),
            HoldStrategy(),
            cadence_source,
            config=PaperRuntimeConfig(
                timer_interval_seconds=config.runtime_timer_interval_seconds,
                source_poll_timeout_seconds=(config.runtime_source_poll_timeout_seconds * 2),
            ),
            clock=MutableClock(_START + timedelta(seconds=2)),
        )
    assert cadence_source.start_calls == 0
    assert list(tmp_path.glob(".cadence.sqlite3.paper-runtime-*.lock")) == []

    release_database = tmp_path / "release.sqlite3"
    release_source = QueuePublicSource([None])
    mismatched_release = replace(config, release_code_sha256="0" * 64)
    with pytest.raises(PaperAdmissionError, match="release code differs"):
        PaperRuntime(
            PaperEngine(PaperStore(release_database), mismatched_release),
            HoldStrategy(),
            release_source,
            config=PaperRuntimeConfig(
                timer_interval_seconds=(mismatched_release.runtime_timer_interval_seconds),
                source_poll_timeout_seconds=(mismatched_release.runtime_source_poll_timeout_seconds),
            ),
            clock=MutableClock(_START + timedelta(seconds=2)),
        )
    assert release_source.start_calls == 0
    assert list(tmp_path.glob(".release.sqlite3.paper-runtime-*.lock")) == []


def test_funding_receipt_then_older_source_market_keeps_durable_time_monotonic(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = replace(
        _config(),
        required_instruments=(_INSTRUMENT,),
        runtime_timer_interval_seconds=60,
    )

    def lineage_market(
        label: str,
        received_at: datetime,
        *,
        event_kind: str = "bbo",
        tradable: bool = True,
    ) -> MarketEvent:
        return MarketEvent.create(
            received_at=received_at,
            instrument=_INSTRUMENT,
            bid_price=Decimal("100"),
            ask_price=Decimal("101"),
            bid_depth=Decimal("10"),
            ask_depth=Decimal("11"),
            source_sequence=int(
                deterministic_id("phase12_runtime_sequence", label)[:8],
                16,
            ),
            tradable=tradable,
            source_event_kind=event_kind,
            source_connection_id="chronology-connection-1",
            source_connection_epoch=1,
        )

    connect_health = lineage_market(
        "chronology-connect",
        _START + timedelta(seconds=1),
        event_kind="connect",
        tradable=False,
    )
    bootstrap_market = lineage_market(
        "chronology-bootstrap",
        _START + timedelta(seconds=2),
    )
    funding_received_at = _START + timedelta(seconds=10)
    settlement = PublicFundingSettlement(
        event_id=deterministic_id("phase12_runtime_funding", "chronology"),
        instrument=_INSTRUMENT,
        funding_time=_START + timedelta(seconds=9),
        received_at=funding_received_at,
        funding_rate=Decimal("0.0001"),
        funding_interval_seconds=3600,
        rate_kind="hyperliquid-hourly-settlement",
        mark_price=None,
        oracle_price=None,
        source_observation_id="chronology-funding-observation",
    )
    older_market = lineage_market(
        "after-funding-older-source",
        _START + timedelta(seconds=5),
    )
    source = QueuePublicSource(
        [
            {_INSTRUMENT: connect_health},
            {_INSTRUMENT: bootstrap_market},
            settlement,
            {_INSTRUMENT: older_market},
        ]
    )
    strategy = HoldStrategy()
    clock = MutableClock(_START)
    runtime = _runtime(
        database,
        source,
        strategy,
        clock,
        timer_interval_seconds=60,
        run_config=config,
    )
    runtime.start()

    clock.value = _START + timedelta(seconds=1)
    assert runtime.run_once().kind is PaperRuntimeStepKind.MARKET
    clock.value = _START + timedelta(seconds=2)
    assert runtime.run_once().kind is PaperRuntimeStepKind.MARKET
    assert strategy.calls == 0

    clock.value = funding_received_at
    funding_step = runtime.run_once()
    assert funding_step.kind is PaperRuntimeStepKind.FUNDING
    clock.value = _START + timedelta(seconds=11)
    market_step = runtime.run_once()
    assert market_step.kind is PaperRuntimeStepKind.MARKET
    assert strategy.calls == 0
    runtime.close()

    durable = PaperStore(database, initialize=False)
    projection = durable.get_projection(config.run_id)
    assert projection.last_received_at == clock.value
    assert projection.last_market_received_at_by_instrument[_INSTRUMENT] == older_market.received_at
    assert projection.last_public_source_received_at == funding_received_at
    market_input = durable.get_input(config.run_id, older_market.event_id)
    assert market_input is not None
    assert market_input.payload["processed_at"] == utc_text(clock.value)
    assert market_input.payload["execution_policy"] == "SOURCE_CHRONOLOGY_OBSERVE_ONLY"
    durable_times = [event.event.received_at for event in durable.iter_events(config.run_id)]
    assert durable_times == sorted(durable_times)
    assert replay_paper_run(durable, config.run_id).to_dict()["status"] == "REPLAY_EXACT"

    restarted = _runtime(
        database,
        QueuePublicSource([{_INSTRUMENT: older_market}]),
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=12)),
        timer_interval_seconds=60,
        run_config=config,
    )
    duplicate = restarted.run_once()
    assert duplicate.kind is PaperRuntimeStepKind.DUPLICATE
    restarted.close()


def test_replay_rejects_coherently_tampered_run_start_inbox_readonly(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _config()
    runtime = _runtime(
        database,
        QueuePublicSource([None]),
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=2)),
        run_config=config,
    )
    runtime.start()
    runtime.close()
    input_id = deterministic_id("paper_input_run_started", config.run_id)
    tampered_payload = {
        "config_hash": "0" * 64,
        "input_type": "RUN_START",
        "run_id": config.run_id,
    }
    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER paper_inbox_no_update")
        updated = connection.execute(
            "UPDATE paper_inbox SET payload_json=?, payload_hash=? WHERE run_id=? AND input_id=?",
            (
                canonical_json(tampered_payload),
                canonical_sha256(tampered_payload),
                config.run_id,
                input_id,
            ),
        )
    before = database.read_bytes()
    assert updated.rowcount == 1

    with pytest.raises(PaperAdmissionError, match="full readonly integrity"):
        replay_paper_run(PaperStore(database, initialize=False), config.run_id)
    assert database.read_bytes() == before


def test_restart_and_replay_use_streaming_history_iterators(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _config()
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()
    for ordinal in range(256):
        engine.process_market(
            _market(
                f"streaming-history-{ordinal}",
                _START + timedelta(seconds=1, milliseconds=ordinal),
            )
        )
    store.close()

    def forbid_materialized_history(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("restart/replay must not materialize complete history")

    monkeypatch.setattr(PaperStore, "get_events", forbid_materialized_history)
    monkeypatch.setattr(PaperStore, "get_inputs", forbid_materialized_history)
    monkeypatch.setattr(
        PaperStore,
        "get_ledger_entries",
        forbid_materialized_history,
    )

    restarted_store = PaperStore(database, initialize=False)
    restarted = PaperEngine(restarted_store, config)
    restored = restarted.start().projection
    assert restored.last_sequence >= 257
    replay = replay_paper_run(restarted_store, config.run_id)

    assert replay.event_count == restored.last_sequence
    assert replay.projection_hash == restored.canonical_hash


def test_runtime_environment_mismatch_rejects_before_lease_store_or_source(
    tmp_path: Path,
) -> None:
    database = tmp_path / "environment-mismatch.sqlite3"
    config = replace(_config(), runtime_environment_sha256="0" * 64)
    source = QueuePublicSource([None])
    runtime = _runtime(
        database,
        source,
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=2)),
        run_config=config,
    )

    with pytest.raises(PaperAdmissionError, match="runtime environment differs"):
        runtime.start()

    assert source.start_calls == 0
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM paper_runs").fetchone() == (0,)
    assert list(tmp_path.glob(".environment-mismatch.sqlite3.paper-runtime-*.lock")) == []


def test_runtime_environment_drift_at_pre_source_recheck_fails_closed_and_releases_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hyperlab.environment_authorization as authorization_module

    database = tmp_path / "environment-drift.sqlite3"
    config = _config()
    source = QueuePublicSource([None])
    runtime = _runtime(
        database,
        source,
        HoldStrategy(),
        MutableClock(_START + timedelta(seconds=2)),
        run_config=config,
    )
    calls = 0

    def drifting_environment_digest(*, candidate_id: str) -> str:
        nonlocal calls
        assert candidate_id == "phase08-robust-pairs-btc-eth-paper-v1"
        calls += 1
        return config.runtime_environment_sha256 if calls == 1 else "0" * 64

    monkeypatch.setattr(
        authorization_module,
        "current_paper_runtime_environment_sha256",
        drifting_environment_digest,
    )

    with pytest.raises(PaperAdmissionError, match="runtime environment differs"):
        runtime.start()

    assert calls == 2
    assert source.start_calls == 0
    assert runtime.engine.projection().runtime_session_active is True
    with PaperRuntimeLease(database, config.run_id):
        pass
