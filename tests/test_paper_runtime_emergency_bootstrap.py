from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from hyperlab.backtest.costs import SlippageModel
from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.models import (
    DecisionAction,
    DecisionIntent,
    MarketEvent,
    OrderIntent,
    OrderSide,
    OrderStatus,
    PaperEventType,
    PaperExecutionConfig,
    PaperOrderType,
    PaperRiskLimits,
    PaperRunConfig,
    PaperState,
    TimeInForce,
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
from hyperlab.paper.store import PaperStore

_START = datetime(2026, 8, 17, 9, tzinfo=UTC)
_SOURCE = "phase12-bootstrap-runtime-fixture"
_DATA_HASH = "b" * 64
_STRATEGY_HASH = "a" * 64
_BTC = "HYPERLIQUID:BTC:perp"
_ETH = "HYPERLIQUID:ETH:perp"
_AUTO_FLATTEN_REASON = "runtime automatic unhedged-timeout emergency flatten"


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class RestoringHoldStrategy:
    strategy_name = "phase12_bootstrap_runtime_fixture"
    strategy_hash = _STRATEGY_HASH

    def __init__(self) -> None:
        self.restore_calls = 0
        self.restored_event_ids: list[str] = []
        self.decide_calls = 0

    def restore(
        self,
        markets: Iterable[MarketEvent],
        view: PaperStrategyView,
    ) -> None:
        assert view.config_hash
        self.restore_calls += 1
        self.restored_event_ids = [market.event_id for market in markets]

    def decide(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None:
        assert markets
        assert view.config_hash
        self.decide_calls += 1
        return None


class QueuePublicSource:
    def __init__(
        self,
        items: list[object | None],
        *,
        bootstrap_timeout_seconds: float = 120.0,
    ) -> None:
        self.items = list(items)
        self._descriptor = PublicSourceDescriptor(
            _SOURCE,
            _DATA_HASH,
            bootstrap_timeout_seconds=bootstrap_timeout_seconds,
        )

    @property
    def descriptor(self) -> PublicSourceDescriptor:
        return self._descriptor

    def start(self) -> None:
        return None

    def poll(self, *, timeout_seconds: float) -> object | None:
        assert timeout_seconds >= 0
        return self.items.pop(0) if self.items else None

    def stop(self) -> None:
        return None

    def close(self) -> None:
        return None


def _config(
    *,
    required_instruments: tuple[str, ...] = (_BTC, _ETH),
    unhedged_timeout_seconds: int = 2,
    runtime_timer_interval_seconds: float = 1.0,
    runtime_source_poll_timeout_seconds: float = 0.1,
    risk: PaperRiskLimits | None = None,
) -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="phase12_bootstrap_runtime_fixture",
        strategy_hash=_STRATEGY_HASH,
        parameters={"fixture": "bootstrap-emergency", "version": 1},
        data_hash=_DATA_HASH,
        execution=PaperExecutionConfig(
            slippage=SlippageModel(max_participation=0.25),
            calibration_status="SYNTHETIC",
            source="deterministic-bootstrap-fixture",
        ),
        risk=risk
        or PaperRiskLimits(
            stale_after_seconds=10,
            unhedged_timeout_seconds=unhedged_timeout_seconds,
        ),
        seed=12,
        initial_cash=Decimal("100000"),
        validation_started_at=_START,
        run_kind="DEMO",
        data_calibration_status="SYNTHETIC",
        data_source=_SOURCE,
        required_instruments=required_instruments,
        runtime_timer_interval_seconds=runtime_timer_interval_seconds,
        runtime_source_poll_timeout_seconds=runtime_source_poll_timeout_seconds,
    )


def _market(
    label: str,
    instrument: str,
    received_at: datetime,
    *,
    depth: str = "10",
    bid: str = "100",
    ask: str = "101",
    tradable: bool = True,
    stale: bool = False,
    gap: bool = False,
    event_kind: str = "bbo",
    connection_id: str = "fixture-connection-1",
    connection_epoch: int = 1,
    capture_ordinal: int = 0,
) -> MarketEvent:
    return MarketEvent.create(
        received_at=received_at,
        instrument=instrument,
        bid_price=Decimal(bid),
        ask_price=Decimal(ask),
        bid_depth=Decimal(depth),
        ask_depth=Decimal(depth),
        source_sequence=int(
            deterministic_id("phase12_bootstrap_runtime_sequence", label)[:8],
            16,
        ),
        capture_ordinal=capture_ordinal,
        tradable=tradable,
        stale=stale,
        gap=gap,
        source_event_kind=event_kind,
        source_connection_id=connection_id,
        source_connection_epoch=connection_epoch,
    )


def _connect_frame(
    received_at: datetime,
    *,
    instruments: tuple[str, ...] = (_BTC, _ETH),
) -> dict[str, MarketEvent]:
    return {
        instrument: _market(
            f"connect-{instrument}",
            instrument,
            received_at,
            tradable=False,
            event_kind="connect",
            capture_ordinal=index,
        )
        for index, instrument in enumerate(instruments, start=1)
    }


def _runtime(
    database: Path,
    config: PaperRunConfig,
    source: QueuePublicSource,
    strategy: RestoringHoldStrategy,
    clock: MutableClock,
    *,
    timer_interval_seconds: float = 1.0,
) -> PaperRuntime:
    return PaperRuntime(
        PaperEngine(PaperStore(database), config),
        strategy,
        source,
        config=PaperRuntimeConfig(
            timer_interval_seconds=timer_interval_seconds,
            source_poll_timeout_seconds=0.1,
        ),
        clock=clock,
    )


def _entry_decision(
    config: PaperRunConfig,
    market: MarketEvent,
) -> DecisionIntent:
    decision_id = DecisionIntent.identifier(
        run_id=config.run_id,
        market_event_id=market.event_id,
        action=DecisionAction.ENTRY,
        ordinal=0,
    )
    order = OrderIntent.create(
        decision_id=decision_id,
        run_id=config.run_id,
        instrument=market.instrument,
        side=OrderSide.BUY,
        quantity=Decimal("2"),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.IOC,
        created_at=market.received_at,
        ordinal=0,
    )
    return DecisionIntent(
        decision_id=decision_id,
        run_id=config.run_id,
        strategy_name=config.strategy_name,
        action=DecisionAction.ENTRY,
        decided_at=market.received_at,
        received_at=market.received_at,
        market_event_id=market.event_id,
        observed_event_ids=(market.event_id,),
        orders=(order,),
    )


def test_rest_bbos_and_connect_health_cannot_arm_before_lineage_complete_bbos(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _config()
    rest_btc = _market(
        "rest-btc",
        _BTC,
        _START + timedelta(seconds=1, milliseconds=100),
        capture_ordinal=1,
    )
    rest_eth = _market(
        "rest-eth",
        _ETH,
        _START + timedelta(seconds=1, milliseconds=100),
        capture_ordinal=2,
    )
    connect_frame = _connect_frame(_START + timedelta(seconds=2))
    websocket_btc = _market(
        "websocket-btc",
        _BTC,
        _START + timedelta(seconds=3),
    )
    websocket_eth = _market(
        "websocket-eth",
        _ETH,
        _START + timedelta(seconds=4),
    )
    steady_btc = _market(
        "steady-btc",
        _BTC,
        _START + timedelta(seconds=4, milliseconds=500),
    )
    source = QueuePublicSource(
        [
            {_BTC: rest_btc, _ETH: rest_eth},
            connect_frame,
            {_BTC: websocket_btc},
            {_ETH: websocket_eth},
            {_BTC: steady_btc},
        ]
    )
    strategy = RestoringHoldStrategy()
    clock = MutableClock(_START + timedelta(seconds=1, milliseconds=100))
    runtime = _runtime(database, config, source, strategy, clock)

    rest_step = runtime.run_once()
    assert rest_step.kind is PaperRuntimeStepKind.MARKET
    assert rest_step.projection.state is PaperState.FLAT
    assert strategy.restore_calls == 0
    assert strategy.decide_calls == 0

    durable = PaperStore(database, initialize=False)
    assert durable.get_input(config.run_id, rest_btc.event_id) is not None
    assert durable.get_input(config.run_id, rest_eth.event_id) is not None
    assert list(durable.iter_inputs(config.run_id, input_type="TIMER")) == []

    clock.value = _START + timedelta(seconds=2)
    connected = runtime.run_once()
    assert connected.kind is PaperRuntimeStepKind.MARKET
    assert strategy.restore_calls == 0
    assert strategy.decide_calls == 0
    assert all(
        durable.get_input(config.run_id, event.event_id) is not None for event in connect_frame.values()
    )
    assert list(durable.iter_inputs(config.run_id, input_type="TIMER")) == []

    clock.value = _START + timedelta(seconds=3)
    one_live_bbo = runtime.run_once()
    assert one_live_bbo.kind is PaperRuntimeStepKind.MARKET
    assert strategy.restore_calls == 0
    assert strategy.decide_calls == 0
    assert list(durable.iter_inputs(config.run_id, input_type="TIMER")) == []

    clock.value = _START + timedelta(seconds=4)
    complete = runtime.run_once()
    assert complete.kind is PaperRuntimeStepKind.MARKET
    assert strategy.restore_calls == 1
    assert strategy.restored_event_ids == [
        rest_btc.event_id,
        rest_eth.event_id,
        connect_frame[_BTC].event_id,
        connect_frame[_ETH].event_id,
        websocket_btc.event_id,
        websocket_eth.event_id,
    ]
    assert strategy.decide_calls == 0
    assert list(durable.iter_inputs(config.run_id, input_type="TIMER")) == []

    clock.value = _START + timedelta(seconds=4, milliseconds=500)
    steady = runtime.run_once()
    assert steady.kind is PaperRuntimeStepKind.MARKET
    assert strategy.restore_calls == 1
    assert strategy.decide_calls == 1


def test_epoch_two_reconnect_during_bootstrap_admits_exact_later_lineage(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _config(required_instruments=(_BTC,))
    connection_id = "fixture-connection-2"
    connected_at = _START + timedelta(seconds=2)
    connect = _market(
        "epoch-two-connect",
        _BTC,
        connected_at,
        tradable=False,
        gap=True,
        event_kind="connect",
        connection_id=connection_id,
        connection_epoch=2,
    )
    fresh = _market(
        "epoch-two-bbo",
        _BTC,
        connected_at + timedelta(seconds=1),
        connection_id=connection_id,
        connection_epoch=2,
    )
    strategy = RestoringHoldStrategy()
    clock = MutableClock(connected_at)
    runtime = _runtime(
        database,
        config,
        QueuePublicSource([{_BTC: connect}, {_BTC: fresh}]),
        strategy,
        clock,
    )

    health = runtime.run_once()
    assert health.kind is PaperRuntimeStepKind.MARKET
    assert strategy.restore_calls == 0

    clock.value = fresh.received_at
    admitted = runtime.run_once()
    assert admitted.kind is PaperRuntimeStepKind.MARKET
    assert strategy.restore_calls == 1
    assert strategy.decide_calls == 0

    clock.value += timedelta(seconds=1)
    runtime.close()
    projection = PaperStore(database, initialize=False).get_projection(config.run_id)
    assert projection.runtime_session_active is False
    assert (
        replay_paper_run(
            PaperStore(database, initialize=False),
            config.run_id,
        ).projection_hash
        == projection.canonical_hash
    )


def test_bootstrap_gap_health_is_durable_and_cannot_complete_admission(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _config(required_instruments=(_BTC,))
    gap_btc = _market(
        "gap-btc",
        _BTC,
        _START + timedelta(seconds=1),
        tradable=False,
        gap=True,
        event_kind="gap",
    )
    connect_frame = _connect_frame(
        _START + timedelta(seconds=2),
        instruments=(_BTC,),
    )
    fresh_btc = _market("recovered-btc", _BTC, _START + timedelta(seconds=3))
    strategy = RestoringHoldStrategy()
    clock = MutableClock(_START + timedelta(seconds=1))
    runtime = _runtime(
        database,
        config,
        QueuePublicSource(
            [
                {_BTC: gap_btc},
                connect_frame,
                {_BTC: fresh_btc},
            ]
        ),
        strategy,
        clock,
    )

    gap_step = runtime.run_once()
    assert gap_step.kind is PaperRuntimeStepKind.MARKET
    assert gap_step.projection.state is PaperState.PAUSED
    assert strategy.restore_calls == 0

    store = PaperStore(database, initialize=False)
    durable_gap = store.get_input(config.run_id, gap_btc.event_id)
    assert durable_gap is not None
    assert durable_gap.payload["market"]["gap"] is True
    assert durable_gap.payload["market"]["tradable"] is False
    assert list(store.iter_inputs(config.run_id, input_type="TIMER")) == []

    clock.value = _START + timedelta(seconds=2)
    connected = runtime.run_once()
    assert connected.kind is PaperRuntimeStepKind.MARKET
    assert connected.projection.state is PaperState.PAUSED
    assert strategy.restore_calls == 0

    clock.value = _START + timedelta(seconds=3)
    recovered = runtime.run_once()
    assert recovered.kind is PaperRuntimeStepKind.MARKET
    assert recovered.projection.state is PaperState.PAUSED
    assert strategy.restore_calls == 1
    assert strategy.restored_event_ids == [
        gap_btc.event_id,
        connect_frame[_BTC].event_id,
        fresh_btc.event_id,
    ]
    assert replay_paper_run(store, config.run_id).projection_hash == (recovered.projection.canonical_hash)


def test_bootstrap_deadline_persists_source_failure_without_early_stale_timer(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _config()
    strategy = RestoringHoldStrategy()
    clock = MutableClock(_START + timedelta(seconds=1))
    runtime = _runtime(
        database,
        config,
        QueuePublicSource([], bootstrap_timeout_seconds=120.0),
        strategy,
        clock,
    )

    idle = runtime.run_once()
    assert idle.kind is PaperRuntimeStepKind.IDLE
    assert idle.projection.state is PaperState.FLAT

    store = PaperStore(database, initialize=False)
    assert list(store.iter_inputs(config.run_id, input_type="TIMER")) == []
    assert list(store.iter_inputs(config.run_id, input_type="PUBLIC_SOURCE_FAILURE")) == []

    clock.value = _START + timedelta(seconds=121)
    with pytest.raises(PaperAdmissionError, match="bootstrap deadline expired"):
        runtime.run_once()

    projection = store.get_projection(config.run_id)
    assert projection.state is PaperState.PAUSED
    failures = list(store.iter_inputs(config.run_id, input_type="PUBLIC_SOURCE_FAILURE"))
    assert len(failures) == 1
    assert failures[0].payload["reason"] == "terminal public source failure: TimeoutError"
    assert strategy.restore_calls == 0
    assert strategy.decide_calls == 0
    assert replay_paper_run(store, config.run_id).projection_hash == projection.canonical_hash


def test_stale_unhedged_timeout_waits_for_fresh_bbo_before_automatic_flatten(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _config(
        required_instruments=(_BTC,),
        unhedged_timeout_seconds=2,
        runtime_timer_interval_seconds=11.0,
    )
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()

    entry_market = _market(
        "stale-wait-entry-decision",
        _BTC,
        _START + timedelta(seconds=1),
        depth="2",
    )
    engine.submit_decision(_entry_decision(config, entry_market), entry_market)
    partial = engine.process_market(
        _market(
            "stale-wait-entry-partial-fill",
            _BTC,
            _START + timedelta(seconds=2),
            depth="2",
        )
    )
    assert partial.projection.state is PaperState.HEDGE_PENDING
    assert partial.projection.positions == {_BTC: Decimal("0.5")}

    connect_frame = _connect_frame(
        _START + timedelta(seconds=3),
        instruments=(_BTC,),
    )
    bootstrap_btc = _market(
        "stale-wait-bootstrap-btc",
        _BTC,
        _START + timedelta(seconds=3, milliseconds=500),
    )
    fresh_btc = _market(
        "stale-wait-fresh-btc",
        _BTC,
        _START + timedelta(seconds=15),
    )
    source = QueuePublicSource(
        [
            connect_frame,
            {_BTC: bootstrap_btc},
            {_BTC: fresh_btc},
        ]
    )
    strategy = RestoringHoldStrategy()
    clock = MutableClock(_START + timedelta(seconds=3))
    runtime = PaperRuntime(
        engine,
        strategy,
        source,
        config=PaperRuntimeConfig(
            timer_interval_seconds=11.0,
            source_poll_timeout_seconds=0.1,
        ),
        clock=clock,
    )

    connected = runtime.run_once()
    assert connected.kind is PaperRuntimeStepKind.MARKET
    assert strategy.restore_calls == 0

    clock.value = _START + timedelta(seconds=3, milliseconds=600)
    bootstrapped = runtime.run_once()
    assert bootstrapped.kind is PaperRuntimeStepKind.MARKET
    assert strategy.restore_calls == 1

    clock.value = _START + timedelta(seconds=14, milliseconds=600)
    timed_out = runtime.run_once()
    assert timed_out.kind is PaperRuntimeStepKind.TIMER
    assert timed_out.projection.state is PaperState.EMERGENCY_FLATTEN
    assert timed_out.projection.current_exit_decision_id is None
    assert all(not order.intent.reduce_only for order in timed_out.projection.orders.values())
    assert [
        record
        for record in store.iter_inputs(config.run_id, input_type="STRATEGY_DECISION")
        if record.payload["decision"]["action"] == "EXIT"
    ] == []

    clock.value = _START + timedelta(seconds=15, milliseconds=100)
    planned = runtime.run_once()
    assert planned.kind is PaperRuntimeStepKind.MARKET
    assert planned.projection.state is PaperState.EMERGENCY_FLATTEN
    exit_id = planned.projection.current_exit_decision_id
    assert exit_id is not None
    emergency_orders = [
        order for order in planned.projection.orders.values() if order.intent.decision_id == exit_id
    ]
    assert len(emergency_orders) == 1
    assert emergency_orders[0].intent.reduce_only is True
    exit_inputs = [
        record
        for record in store.iter_inputs(config.run_id, input_type="STRATEGY_DECISION")
        if record.payload["decision"]["action"] == "EXIT"
    ]
    assert len(exit_inputs) == 1
    assert clock.value - fresh_btc.received_at == timedelta(milliseconds=100)
    duplicate = engine.emergency_flatten(
        {_BTC: fresh_btc},
        decided_at=clock.value,
        reason=_AUTO_FLATTEN_REASON,
    )
    assert duplicate.append.idempotent is True
    assert (
        len(
            [
                record
                for record in store.iter_inputs(
                    config.run_id,
                    input_type="STRATEGY_DECISION",
                )
                if record.payload["decision"]["action"] == "EXIT"
            ]
        )
        == 1
    )

    assert replay_paper_run(store, config.run_id).projection_hash == (planned.projection.canonical_hash)


def test_unhedged_bootstrap_timeout_automatically_flattens_and_replays_exactly(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _config(
        unhedged_timeout_seconds=2,
        runtime_timer_interval_seconds=10.0,
    )
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()

    entry_market = _market(
        "entry-decision",
        _BTC,
        _START + timedelta(seconds=1),
        depth="2",
    )
    engine.submit_decision(_entry_decision(config, entry_market), entry_market)
    partial = engine.process_market(
        _market(
            "entry-partial-fill",
            _BTC,
            _START + timedelta(seconds=2),
            depth="2",
        )
    )
    assert partial.projection.state is PaperState.HEDGE_PENDING
    assert partial.projection.positions == {_BTC: Decimal("0.5")}

    connect_frame = _connect_frame(_START + timedelta(seconds=3))
    bootstrap_btc = _market(
        "bootstrap-btc",
        _BTC,
        _START + timedelta(seconds=3, milliseconds=500),
    )
    bootstrap_eth = _market(
        "bootstrap-eth",
        _ETH,
        _START + timedelta(seconds=5),
    )
    emergency_btc = _market(
        "emergency-btc",
        _BTC,
        _START + timedelta(seconds=6),
    )
    fill_btc = _market(
        "emergency-fill",
        _BTC,
        _START + timedelta(seconds=7),
    )
    source = QueuePublicSource(
        [
            connect_frame,
            {_BTC: bootstrap_btc},
            {_ETH: bootstrap_eth},
            {_BTC: emergency_btc},
            {_BTC: fill_btc},
        ]
    )
    strategy = RestoringHoldStrategy()
    clock = MutableClock(_START + timedelta(seconds=3))
    runtime = PaperRuntime(
        engine,
        strategy,
        source,
        config=PaperRuntimeConfig(
            timer_interval_seconds=10.0,
            source_poll_timeout_seconds=0.1,
        ),
        clock=clock,
    )

    connected = runtime.run_once()
    assert connected.kind is PaperRuntimeStepKind.MARKET
    assert strategy.restore_calls == 0

    clock.value = _START + timedelta(seconds=3, milliseconds=500)
    one_live_bbo = runtime.run_once()
    assert one_live_bbo.kind is PaperRuntimeStepKind.MARKET
    assert strategy.restore_calls == 0

    clock.value = _START + timedelta(seconds=4)
    timed_out = runtime.run_once()
    assert timed_out.kind is PaperRuntimeStepKind.TIMER
    assert timed_out.projection.state is PaperState.EMERGENCY_FLATTEN
    exit_id = timed_out.projection.current_exit_decision_id
    assert exit_id is not None
    emergency_orders = [
        order for order in timed_out.projection.orders.values() if order.intent.decision_id == exit_id
    ]
    assert len(emergency_orders) == 1
    assert emergency_orders[0].intent.reduce_only is True
    assert emergency_orders[0].intent.order_type is PaperOrderType.TAKER
    assert emergency_orders[0].intent.time_in_force is TimeInForce.IOC

    duplicate = engine.emergency_flatten(
        {_BTC: bootstrap_btc},
        decided_at=clock.value,
        reason=_AUTO_FLATTEN_REASON,
    )
    assert duplicate.append.idempotent is True
    exit_inputs = [
        record
        for record in store.iter_inputs(config.run_id, input_type="STRATEGY_DECISION")
        if record.payload["decision"]["action"] == "EXIT"
    ]
    assert len(exit_inputs) == 1

    clock.value = _START + timedelta(seconds=5)
    complete = runtime.run_once()
    assert complete.kind is PaperRuntimeStepKind.MARKET
    assert complete.projection.state is PaperState.EMERGENCY_FLATTEN
    assert complete.projection.current_exit_decision_id == exit_id
    assert strategy.restore_calls == 1
    assert strategy.decide_calls == 0

    clock.value = _START + timedelta(seconds=6)
    flattened = runtime.run_once()
    assert flattened.kind is PaperRuntimeStepKind.MARKET
    assert flattened.projection.state is PaperState.FLAT
    assert flattened.projection.positions == {}

    clock.value = _START + timedelta(seconds=7)
    post_flat = runtime.run_once()
    assert post_flat.kind is PaperRuntimeStepKind.MARKET
    assert post_flat.projection.state is PaperState.FLAT
    assert post_flat.projection.positions == {}
    assert (
        len(
            [
                record
                for record in store.iter_inputs(
                    config.run_id,
                    input_type="STRATEGY_DECISION",
                )
                if record.payload["decision"]["action"] == "EXIT"
            ]
        )
        == 1
    )

    replay = replay_paper_run(store, config.run_id)
    assert replay.projection_hash == post_flat.projection.canonical_hash
    assert replay.to_dict()["status"] == "REPLAY_EXACT"


def test_restart_bootstrap_suppresses_existing_order_until_bilateral_lineage(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _config(
        unhedged_timeout_seconds=60,
        runtime_timer_interval_seconds=60,
    )
    initial_market = _market(
        "pre-restart-order-market",
        _BTC,
        _START + timedelta(milliseconds=100),
    )
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()
    engine.process_market(initial_market)
    decision = _entry_decision(config, initial_market)
    submitted = engine.submit_decision(decision, initial_market)
    order_id = decision.orders[0].order_id
    assert submitted.projection.orders[order_id].status is OrderStatus.ACKED
    assert submitted.projection.positions == {}
    store.close()

    connect_frame = _connect_frame(_START + timedelta(seconds=1))
    websocket_btc = _market(
        "restart-bootstrap-btc",
        _BTC,
        _START + timedelta(seconds=2),
    )
    websocket_eth = _market(
        "restart-bootstrap-eth",
        _ETH,
        _START + timedelta(seconds=3),
    )
    steady_btc = _market(
        "restart-steady-btc",
        _BTC,
        _START + timedelta(seconds=4),
    )
    source = QueuePublicSource(
        [
            connect_frame,
            {_BTC: websocket_btc},
            {_ETH: websocket_eth},
            {_BTC: steady_btc},
        ]
    )
    strategy = RestoringHoldStrategy()
    clock = MutableClock(_START + timedelta(seconds=1))
    runtime = _runtime(
        database,
        config,
        source,
        strategy,
        clock,
        timer_interval_seconds=60,
    )

    connected = runtime.run_once()
    assert connected.projection.positions == {}
    assert connected.projection.orders[order_id].status is OrderStatus.ACKED
    clock.value = _START + timedelta(seconds=2)
    first_leg = runtime.run_once()
    assert first_leg.projection.positions == {}
    assert first_leg.projection.orders[order_id].status is OrderStatus.ACKED
    clock.value = _START + timedelta(seconds=3)
    bilateral = runtime.run_once()
    assert bilateral.projection.positions == {}
    assert bilateral.projection.orders[order_id].status is OrderStatus.ACKED
    assert strategy.decide_calls == 0

    clock.value = _START + timedelta(seconds=4)
    executed = runtime.run_once()
    assert executed.projection.positions[_BTC] == Decimal("2")


def test_marked_cap_protective_flatten_retries_partial_across_restart(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    risk = PaperRiskLimits(
        max_gross_notional=Decimal("225"),
        max_net_notional=Decimal("225"),
        max_instrument_notional=Decimal("225"),
        stale_after_seconds=10,
        unhedged_timeout_seconds=60,
    )
    config = _config(
        required_instruments=(_BTC,),
        runtime_timer_interval_seconds=60.0,
        risk=risk,
    )
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()
    entry_market = _market(
        "protective-entry",
        _BTC,
        _START + timedelta(seconds=1),
        depth="2",
    )
    engine.submit_decision(_entry_decision(config, entry_market), entry_market)
    opened = engine.process_market(
        _market(
            "protective-entry-partial",
            _BTC,
            _START + timedelta(seconds=2),
            depth="2",
        )
    ).projection
    assert opened.positions == {_BTC: Decimal("0.5")}

    first_clock = MutableClock(_START + timedelta(seconds=3))
    first_runtime = PaperRuntime(
        engine,
        RestoringHoldStrategy(),
        QueuePublicSource(
            [
                _connect_frame(
                    _START + timedelta(seconds=3),
                    instruments=(_BTC,),
                ),
                {
                    _BTC: _market(
                        "protective-mark-jump",
                        _BTC,
                        _START + timedelta(seconds=3, milliseconds=500),
                        bid="500",
                        ask="501",
                        depth="10",
                    )
                },
                {
                    _BTC: _market(
                        "protective-partial-exit",
                        _BTC,
                        _START + timedelta(seconds=4),
                        bid="500",
                        ask="501",
                        depth="1",
                    )
                },
            ]
        ),
        config=PaperRuntimeConfig(
            timer_interval_seconds=60.0,
            source_poll_timeout_seconds=0.1,
        ),
        clock=first_clock,
    )

    first_runtime.run_once()
    first_clock.value = _START + timedelta(seconds=3, milliseconds=500)
    protected = first_runtime.run_once()
    assert protected.projection.state is PaperState.REDUCE_ONLY
    assert protected.projection.positions == {_BTC: Decimal("0.5")}
    first_exit_id = protected.projection.current_exit_decision_id
    assert first_exit_id is not None

    first_clock.value = _START + timedelta(seconds=4)
    partial = first_runtime.run_once()
    assert partial.projection.state is PaperState.REDUCE_ONLY
    assert partial.projection.positions == {_BTC: Decimal("0.25")}
    retry_after_partial = partial.projection.current_exit_decision_id
    assert retry_after_partial is not None and retry_after_partial != first_exit_id
    first_runtime.close()

    second_clock = MutableClock(_START + timedelta(seconds=5))
    second_runtime = PaperRuntime(
        PaperEngine(PaperStore(database), config),
        RestoringHoldStrategy(),
        QueuePublicSource(
            [
                _connect_frame(
                    _START + timedelta(seconds=5),
                    instruments=(_BTC,),
                ),
                {
                    _BTC: _market(
                        "protective-restart-bootstrap",
                        _BTC,
                        _START + timedelta(seconds=5, milliseconds=500),
                        bid="500",
                        ask="501",
                        depth="10",
                    )
                },
                {
                    _BTC: _market(
                        "protective-full-fill-after-restart",
                        _BTC,
                        _START + timedelta(seconds=6),
                        bid="500",
                        ask="501",
                        depth="10",
                    )
                },
            ]
        ),
        config=PaperRuntimeConfig(
            timer_interval_seconds=60.0,
            source_poll_timeout_seconds=0.1,
        ),
        clock=second_clock,
    )

    second_runtime.run_once()
    second_clock.value = _START + timedelta(seconds=5, milliseconds=500)
    bootstrapped = second_runtime.run_once()
    assert bootstrapped.projection.positions == {_BTC: Decimal("0.25")}
    assert bootstrapped.projection.current_exit_decision_id == retry_after_partial

    second_clock.value = _START + timedelta(seconds=6)
    flattened = second_runtime.run_once()
    assert flattened.projection.state is PaperState.FLAT
    assert flattened.projection.positions == {}
    second_runtime.close()
    durable_after_close = second_runtime.engine.projection()

    event_types = [stored.event.event_type for stored in store.iter_events(config.run_id)]
    assert PaperEventType.ORDER_PARTIALLY_FILLED in event_types
    assert PaperEventType.ORDER_EXPIRED in event_types
    assert PaperEventType.ORDER_FILLED in event_types
    assert replay_paper_run(store, config.run_id).projection_hash == (durable_after_close.canonical_hash)


def test_marked_cap_protective_no_fill_stays_reduce_only_and_retries(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    base_config = _config(
        required_instruments=(_BTC,),
        runtime_timer_interval_seconds=60.0,
        risk=PaperRiskLimits(
            max_gross_notional=Decimal("60"),
            max_net_notional=Decimal("60"),
            max_instrument_notional=Decimal("60"),
            stale_after_seconds=10,
            unhedged_timeout_seconds=60,
        ),
    )
    config = replace(
        base_config,
        execution=replace(
            base_config.execution,
            ioc_fill_probability=Decimal(0),
        ),
    )
    store = PaperStore(database)
    engine = PaperEngine(store, config)
    engine.start()
    entry_market = _market(
        "protective-no-fill-entry",
        _BTC,
        _START + timedelta(seconds=1),
    )
    decision_id = DecisionIntent.identifier(
        run_id=config.run_id,
        market_event_id=entry_market.event_id,
        action=DecisionAction.ENTRY,
        ordinal=0,
    )
    entry_order = OrderIntent.create(
        decision_id=decision_id,
        run_id=config.run_id,
        instrument=_BTC,
        side=OrderSide.BUY,
        quantity=Decimal("0.5"),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=entry_market.received_at,
        ordinal=0,
    )
    engine.submit_decision(
        DecisionIntent(
            decision_id=decision_id,
            run_id=config.run_id,
            strategy_name=config.strategy_name,
            action=DecisionAction.ENTRY,
            decided_at=entry_market.received_at,
            received_at=entry_market.received_at,
            market_event_id=entry_market.event_id,
            observed_event_ids=(entry_market.event_id,),
            orders=(entry_order,),
        ),
        entry_market,
    )
    opened = engine.process_market(
        _market(
            "protective-no-fill-entry-fill",
            _BTC,
            _START + timedelta(seconds=2),
        )
    ).projection
    assert opened.positions == {_BTC: Decimal("0.5")}

    clock = MutableClock(_START + timedelta(seconds=3))
    runtime = PaperRuntime(
        engine,
        RestoringHoldStrategy(),
        QueuePublicSource(
            [
                _connect_frame(
                    _START + timedelta(seconds=3),
                    instruments=(_BTC,),
                ),
                {
                    _BTC: _market(
                        "protective-no-fill-mark-jump",
                        _BTC,
                        _START + timedelta(seconds=3, milliseconds=500),
                        bid="200",
                        ask="201",
                    )
                },
                {
                    _BTC: _market(
                        "protective-no-fill-attempt-one",
                        _BTC,
                        _START + timedelta(seconds=4),
                        bid="200",
                        ask="201",
                    )
                },
                {
                    _BTC: _market(
                        "protective-no-fill-attempt-two",
                        _BTC,
                        _START + timedelta(seconds=5),
                        bid="200",
                        ask="201",
                    )
                },
            ]
        ),
        config=PaperRuntimeConfig(
            timer_interval_seconds=60.0,
            source_poll_timeout_seconds=0.1,
        ),
        clock=clock,
    )

    runtime.run_once()
    clock.value = _START + timedelta(seconds=3, milliseconds=500)
    protected = runtime.run_once()
    first_exit_id = protected.projection.current_exit_decision_id
    assert first_exit_id is not None

    clock.value = _START + timedelta(seconds=4)
    first_miss = runtime.run_once()
    retry_one = first_miss.projection.current_exit_decision_id
    assert first_miss.projection.state is PaperState.REDUCE_ONLY
    assert first_miss.projection.positions == {_BTC: Decimal("0.5")}
    assert retry_one is not None and retry_one != first_exit_id

    clock.value = _START + timedelta(seconds=5)
    second_miss = runtime.run_once()
    retry_two = second_miss.projection.current_exit_decision_id
    assert second_miss.projection.state is PaperState.REDUCE_ONLY
    assert second_miss.projection.positions == {_BTC: Decimal("0.5")}
    assert retry_two is not None and retry_two != retry_one
    runtime.close()

    no_fills = [
        stored
        for stored in store.iter_events(config.run_id)
        if stored.event.event_type is PaperEventType.ORDER_NO_FILL
    ]
    assert len(no_fills) == 2
    durable = runtime.engine.projection()
    assert replay_paper_run(store, config.run_id).projection_hash == durable.canonical_hash
