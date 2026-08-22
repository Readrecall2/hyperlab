from __future__ import annotations

import gc
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal, localcontext
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest

import hyperlab.paper.engine as paper_engine_module
import hyperlab.paper.reducer as paper_reducer_module
import hyperlab.paper.store as paper_store_module
from hyperlab.backtest.protocol import canonical_json
from hyperlab.environment_authorization import (
    current_paper_release_code_sha256,
    current_paper_runtime_environment_sha256,
    paper_release_identity_candidate,
)
from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.models import (
    DecisionAction,
    DecisionIntent,
    MarketEvent,
    MarketExecutionPolicy,
    OrderIntent,
    OrderSide,
    OrderStatus,
    PaperEvent,
    PaperEventType,
    PaperExecutionConfig,
    PaperOrder,
    PaperOrderType,
    PaperProjection,
    PaperRiskLimits,
    PaperRunConfig,
    PaperState,
    PaperStrategyConfig,
    PaperStrategyProjection,
    TimeInForce,
    deterministic_id,
    paper_accounting_add,
    paper_attributed_cash,
    paper_attributed_fees,
    paper_attributed_positions,
)
from hyperlab.paper.reducer import (
    PAPER_CASH_MATH_VERSION,
    apply_event,
    transaction_ledger_amounts,
)
from hyperlab.paper.reporting import build_paper_report
from hyperlab.paper.runner import PaperStrategyView, PortfolioRunner
from hyperlab.paper.runtime import (
    PaperAdmissionError,
    PaperRuntime,
    PaperRuntimeConfig,
    PaperRuntimeStepKind,
    PublicSourceDescriptor,
    replay_paper_run,
)
from hyperlab.paper.store import AppendConflictError, PaperStore

_START = datetime(2026, 8, 18, 8, tzinfo=UTC)
_BTC = "HYPERLIQUID:BTC:perp"
_ETH = "HYPERLIQUID:ETH:perp"
_DATA_HASH = "d" * 64


def _live_cash_projection(
    *,
    aggregate_cash: str,
    phase08_cash: str = "0.291214466292888563049853327",
) -> PaperProjection:
    strategies = {
        strategy_id: PaperStrategyProjection(
            strategy_id=strategy_id,
            strategy_name=strategy_id,
            strategy_hash=deterministic_id("live_decimal_strategy", strategy_id),
            strategy_config_hash=deterministic_id(
                "live_decimal_strategy_config",
                strategy_id,
            ),
            cash=Decimal(cash),
        )
        for strategy_id, cash in (
            ("phase05_sentiment_btc", "0"),
            ("phase08_robust_pairs", phase08_cash),
        )
    }
    return PaperProjection(
        run_id=deterministic_id("live_decimal_run"),
        config_hash=deterministic_id("live_decimal_config"),
        initial_cash=Decimal("2000"),
        cash=Decimal(aggregate_cash),
        strategy_projections=strategies,
    )


def _strategy_config(
    strategy_id: str,
    *,
    instrument: str,
    max_order_notional: str = "100000",
) -> PaperStrategyConfig:
    return PaperStrategyConfig(
        strategy_id=strategy_id,
        strategy_name=f"synthetic_{strategy_id}",
        strategy_hash=deterministic_id("synthetic_strategy", strategy_id),
        parameters={"fixture": "SYNTHETIC", "instrument": instrument},
        risk=PaperRiskLimits(max_order_notional=Decimal(max_order_notional)),
        required_instruments=(instrument,),
    )


def _portfolio_config(
    strategies: tuple[PaperStrategyConfig, ...],
    *,
    portfolio_risk: PaperRiskLimits | None = None,
) -> PaperRunConfig:
    candidate_id = paper_release_identity_candidate(
        config_schema_version=3,
    )
    primary = sorted(strategies, key=lambda item: item.strategy_id)[0]
    return PaperRunConfig(
        strategy_name=primary.strategy_name,
        strategy_hash=primary.strategy_hash,
        parameters=primary.parameters,
        data_hash=_DATA_HASH,
        execution=PaperExecutionConfig(
            taker_fee_bps=Decimal("10"),
            calibration_status="SYNTHETIC",
            source="deterministic-multistrategy-fixture",
        ),
        risk=portfolio_risk or PaperRiskLimits(),
        seed=7,
        initial_cash=Decimal("100000"),
        validation_started_at=_START,
        run_kind="DEMO",
        data_calibration_status="SYNTHETIC",
        data_source="deterministic-multistrategy-fixture",
        required_instruments=tuple(
            sorted({instrument for item in strategies for instrument in item.required_instruments})
        ),
        schema_version=3,
        strategies=strategies,
        release_code_sha256=current_paper_release_code_sha256(
            candidate_id=candidate_id,
        ),
        runtime_environment_sha256=current_paper_runtime_environment_sha256(
            candidate_id=candidate_id,
        ),
    )


def _market(
    label: str,
    at: datetime,
    *,
    instrument: str = _BTC,
    bid: str = "100",
    ask: str = "100",
    bid_depth: str = "1000",
    ask_depth: str = "1000",
    tradable: bool = True,
    source_event_kind: str | None = None,
    source_connection_id: str | None = None,
    source_connection_epoch: int | None = None,
) -> MarketEvent:
    return MarketEvent.create(
        received_at=at,
        instrument=instrument,
        bid_price=Decimal(bid),
        ask_price=Decimal(ask),
        bid_depth=Decimal(bid_depth),
        ask_depth=Decimal(ask_depth),
        source_sequence=int(deterministic_id("multistrategy_market", label)[:8], 16),
        capture_ordinal=0 if instrument == _BTC else 1,
        tradable=tradable,
        source_event_kind=source_event_kind,
        source_connection_id=source_connection_id,
        source_connection_epoch=source_connection_epoch,
    )


def _strategy_decision(
    config: PaperRunConfig,
    strategy: PaperStrategyConfig,
    markets: Mapping[str, MarketEvent],
    *,
    action: DecisionAction,
    order_specs: tuple[tuple[str, OrderSide, str], ...],
    decided_at: datetime,
    ordinal: int,
) -> DecisionIntent:
    primary = markets[order_specs[0][0]]
    decision_id = DecisionIntent.identifier(
        run_id=config.run_id,
        strategy_id=strategy.strategy_id,
        market_event_id=primary.event_id,
        action=action,
        ordinal=ordinal,
    )
    return DecisionIntent(
        decision_id=decision_id,
        run_id=config.run_id,
        strategy_id=strategy.strategy_id,
        strategy_name=strategy.strategy_name,
        strategy_hash=strategy.strategy_hash,
        strategy_config_hash=strategy.strategy_config_hash,
        action=action,
        decided_at=decided_at,
        received_at=max(market.received_at for market in markets.values()),
        market_event_id=primary.event_id,
        observed_event_ids=tuple(markets[instrument].event_id for instrument in sorted(markets)),
        orders=tuple(
            OrderIntent.create(
                decision_id=decision_id,
                run_id=config.run_id,
                strategy_id=strategy.strategy_id,
                instrument=instrument,
                side=side,
                quantity=Decimal(quantity),
                order_type=PaperOrderType.TAKER,
                time_in_force=TimeInForce.IOC,
                created_at=decided_at,
                ordinal=index,
                reduce_only=action is DecisionAction.EXIT,
                hedge_group_id=(
                    deterministic_id(
                        "phase08_incident_hedge_group",
                        decision_id,
                    )
                    if action is DecisionAction.ENTRY and len(order_specs) > 1
                    else None
                ),
                leg_number=index + 1,
            )
            for index, (instrument, side, quantity) in enumerate(order_specs)
        ),
        ordinal=ordinal,
    )


class OneShotStrategy:
    def __init__(
        self,
        config: PaperStrategyConfig,
        *,
        side: OrderSide,
        quantity: str,
        calls: list[str] | None = None,
        fail: bool = False,
        wait_evaluations: int = 0,
    ) -> None:
        self.strategy_id = config.strategy_id
        self.strategy_name = config.strategy_name
        self.strategy_hash = config.strategy_hash
        self._config_hash = config.strategy_config_hash
        self._instrument = config.required_instruments[0]
        self._side = side
        self._quantity = Decimal(quantity)
        self._calls = calls
        self._fail = fail
        self._wait_evaluations = wait_evaluations
        self._decided = False
        self.seen_event_ids: list[str] = []

    @property
    def strategy_config_hash(self) -> str:
        return self._config_hash

    def decide(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None:
        self.seen_event_ids.extend(item.event_id for item in markets.values())
        if self._calls is not None:
            self._calls.append(self.strategy_id)
        if self._fail:
            raise ValueError(f"synthetic failure for {self.strategy_id}")
        if self._wait_evaluations:
            self._wait_evaluations -= 1
            return None
        if self._decided or view.state is not PaperState.FLAT:
            return None
        self._decided = True
        market = markets[self._instrument]
        decision_id = DecisionIntent.identifier(
            run_id=view.run_id,
            strategy_id=view.strategy_id,
            market_event_id=market.event_id,
            action=DecisionAction.ENTRY,
            ordinal=0,
        )
        order = OrderIntent.create(
            decision_id=decision_id,
            run_id=view.run_id,
            strategy_id=view.strategy_id,
            instrument=self._instrument,
            side=self._side,
            quantity=self._quantity,
            order_type=PaperOrderType.TAKER,
            time_in_force=TimeInForce.GTC,
            created_at=market.received_at,
            ordinal=0,
        )
        return DecisionIntent(
            decision_id=decision_id,
            run_id=view.run_id,
            strategy_id=view.strategy_id,
            strategy_name=view.strategy_name,
            strategy_hash=view.strategy_hash,
            strategy_config_hash=view.strategy_config_hash,
            action=DecisionAction.ENTRY,
            decided_at=market.received_at,
            received_at=max(item.received_at for item in markets.values()),
            market_event_id=market.event_id,
            observed_event_ids=tuple(
                item.event_id for item in sorted(markets.values(), key=lambda item: item.instrument)
            ),
            orders=(order,),
        )


class RoundTripStrategy(OneShotStrategy):
    def decide(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None:
        self.seen_event_ids.extend(item.event_id for item in markets.values())
        market = markets[self._instrument]
        position = view.positions.get(self._instrument, Decimal(0))
        if not self._decided and view.state is PaperState.FLAT:
            self._decided = True
            action = DecisionAction.ENTRY
            side = self._side
            quantity = self._quantity
            reduce_only = False
            ordinal = 0
        elif position != 0 and view.state is PaperState.HEDGED:
            action = DecisionAction.EXIT
            side = OrderSide.SELL if position > 0 else OrderSide.BUY
            quantity = abs(position)
            reduce_only = True
            ordinal = 1
        else:
            return None
        decision_id = DecisionIntent.identifier(
            run_id=view.run_id,
            strategy_id=view.strategy_id,
            market_event_id=market.event_id,
            action=action,
            ordinal=ordinal,
        )
        order = OrderIntent.create(
            decision_id=decision_id,
            run_id=view.run_id,
            strategy_id=view.strategy_id,
            instrument=self._instrument,
            side=side,
            quantity=quantity,
            order_type=PaperOrderType.TAKER,
            time_in_force=TimeInForce.GTC,
            created_at=market.received_at,
            ordinal=0,
            reduce_only=reduce_only,
        )
        return DecisionIntent(
            decision_id=decision_id,
            run_id=view.run_id,
            strategy_id=view.strategy_id,
            strategy_name=view.strategy_name,
            strategy_hash=view.strategy_hash,
            strategy_config_hash=view.strategy_config_hash,
            action=action,
            decided_at=market.received_at,
            received_at=max(item.received_at for item in markets.values()),
            market_event_id=market.event_id,
            observed_event_ids=tuple(
                item.event_id for item in sorted(markets.values(), key=lambda item: item.instrument)
            ),
            orders=(order,),
            ordinal=ordinal,
        )


class RestoreHoldStrategy:
    def __init__(self, config: PaperStrategyConfig) -> None:
        self.strategy_id = config.strategy_id
        self.strategy_name = config.strategy_name
        self.strategy_hash = config.strategy_hash
        self._config_hash = config.strategy_config_hash
        self.restore_calls = 0
        self.restored_event_ids: list[str] = []
        self.seen_event_ids: list[str] = []

    @property
    def strategy_config_hash(self) -> str:
        return self._config_hash

    def restore(
        self,
        markets: Iterable[MarketEvent],
        view: PaperStrategyView,
    ) -> None:
        assert view.strategy_id == self.strategy_id
        self.restore_calls += 1
        self.restored_event_ids = [market.event_id for market in markets]

    def decide(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None:
        assert view.strategy_id == self.strategy_id
        self.seen_event_ids.extend(market.event_id for market in markets.values())
        return None


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class QueuePublicSource:
    def __init__(
        self,
        frames: list[object | None],
        *,
        before_poll: object | None = None,
    ) -> None:
        self.frames = list(frames)
        self.before_poll = before_poll
        self.descriptor = PublicSourceDescriptor(
            "deterministic-multistrategy-fixture",
            _DATA_HASH,
        )

    def start(self) -> None:
        return None

    def poll(self, *, timeout_seconds: float) -> object | None:
        del timeout_seconds
        if callable(self.before_poll):
            self.before_poll()
        return self.frames.pop(0) if self.frames else None

    def stop(self) -> None:
        return None

    def close(self) -> None:
        return None


def _runtime(
    database: Path,
    config: PaperRunConfig,
    strategies: tuple[RestoreHoldStrategy, ...],
    source: QueuePublicSource,
    clock: MutableClock,
) -> PaperRuntime:
    return PaperRuntime(
        PaperEngine(PaperStore(database), config),
        strategies,
        source,
        config=PaperRuntimeConfig(
            timer_interval_seconds=config.runtime_timer_interval_seconds,
            source_poll_timeout_seconds=config.runtime_source_poll_timeout_seconds,
        ),
        clock=clock,
    )


def _run_two_frames(
    tmp_path: Path,
    config: PaperRunConfig,
    strategies: tuple[OneShotStrategy, ...],
    frames: tuple[dict[str, MarketEvent], dict[str, MarketEvent]],
) -> tuple[PaperEngine, PortfolioRunner]:
    engine = PaperEngine(PaperStore(tmp_path / "paper.sqlite3"), config)
    engine.start()
    runner = PortfolioRunner(engine, strategies)
    runner.process_frame(frames[0], processed_at=_START)
    runner.process_frame(frames[1], processed_at=_START + timedelta(seconds=1))
    return engine, runner


def _phase08_asymmetric_exit_stall(
    database: Path,
) -> tuple[
    PaperRunConfig,
    PaperEngine,
    PaperStrategyConfig,
    PaperStrategyConfig,
    PaperProjection,
]:
    phase08 = replace(
        _strategy_config("phase08_robust_pairs", instrument=_ETH),
        required_instruments=(_BTC, _ETH),
    )
    other = _strategy_config("phase05_hold", instrument=_BTC)
    config = _portfolio_config((phase08, other))
    engine = PaperEngine(PaperStore(database), config)
    engine.start()

    entry_markets = {
        _BTC: _market("phase08-entry-btc", _START + timedelta(seconds=1)),
        _ETH: _market(
            "phase08-entry-eth",
            _START + timedelta(seconds=1),
            instrument=_ETH,
        ),
    }
    for market in sorted(
        entry_markets.values(),
        key=lambda item: (item.received_at, item.capture_ordinal),
    ):
        engine.process_market(
            market,
            execution_policy=MarketExecutionPolicy.BOOTSTRAP_OBSERVE_ONLY,
        )
    engine.submit_decision(
        _strategy_decision(
            config,
            phase08,
            entry_markets,
            action=DecisionAction.ENTRY,
            order_specs=(
                (_ETH, OrderSide.BUY, "0.051"),
                (_BTC, OrderSide.SELL, "0.00155"),
            ),
            decided_at=_START + timedelta(seconds=1),
            ordinal=0,
        ),
        entry_markets,
    )
    engine.submit_decision(
        _strategy_decision(
            config,
            other,
            {_BTC: entry_markets[_BTC]},
            action=DecisionAction.ENTRY,
            order_specs=((_BTC, OrderSide.BUY, "0.01"),),
            decided_at=_START + timedelta(seconds=1),
            ordinal=0,
        ),
        {_BTC: entry_markets[_BTC]},
    )
    engine.process_market(
        _market(
            "phase08-entry-fill-eth",
            _START + timedelta(seconds=2),
            instrument=_ETH,
        )
    )
    hedged = engine.process_market(
        _market("phase08-entry-fill-btc", _START + timedelta(seconds=2))
    ).projection
    assert hedged.strategy_projections[phase08.strategy_id].state is PaperState.HEDGED
    assert hedged.strategy_projections[other.strategy_id].state is PaperState.HEDGED
    assert hedged.state is PaperState.HEDGED

    exit_markets = {
        _BTC: _market("phase08-exit-btc", _START + timedelta(seconds=3)),
        _ETH: _market(
            "phase08-exit-eth",
            _START + timedelta(seconds=3),
            instrument=_ETH,
        ),
    }
    engine.submit_decision(
        _strategy_decision(
            config,
            phase08,
            exit_markets,
            action=DecisionAction.EXIT,
            order_specs=(
                (_ETH, OrderSide.SELL, "0.051"),
                (_BTC, OrderSide.BUY, "0.00155"),
            ),
            decided_at=_START + timedelta(seconds=3),
            ordinal=1,
        ),
        exit_markets,
    )
    engine.process_market(
        _market(
            "phase08-exit-fill-eth",
            _START + timedelta(seconds=4),
            instrument=_ETH,
        )
    )
    stalled = engine.process_market(
        _market(
            "phase08-exit-partial-btc",
            _START + timedelta(seconds=4),
            bid="77981",
            ask="77981",
            ask_depth="0.0022",
        )
    ).projection
    ticked = engine.process_timer(as_of=_START + timedelta(seconds=4, milliseconds=500))
    assert ticked.projection.to_dict() != stalled.to_dict()
    assert ticked.projection.state is PaperState.HEDGED
    assert ticked.projection.strategy_projections[phase08.strategy_id].state is (PaperState.EMERGENCY_FLATTEN)
    assert ticked.projection.strategy_projections[phase08.strategy_id].positions == {
        _BTC: Decimal("-0.001329999999999999974352726946")
    }
    return config, engine, phase08, other, ticked.projection


def test_legacy_schema_v2_config_and_identity_are_unchanged() -> None:
    legacy = PaperRunConfig(
        strategy_name="legacy_fixture",
        strategy_hash="a" * 64,
        parameters={"version": 1},
        data_hash=_DATA_HASH,
        execution=PaperExecutionConfig(),
        risk=PaperRiskLimits(),
        seed=1,
        initial_cash=Decimal("1000"),
        validation_started_at=_START,
    )

    payload = legacy.to_dict()

    assert payload["schema_version"] == 2
    assert "strategies" not in payload
    assert PaperRunConfig.from_dict(payload).to_dict() == payload
    assert PaperRunConfig.from_dict(payload).run_id == legacy.run_id


def test_two_strategies_share_observations_and_evaluate_in_strategy_id_order(
    tmp_path: Path,
) -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    zeta = _strategy_config("zeta", instrument=_ETH)
    config = _portfolio_config((zeta, alpha))
    calls: list[str] = []
    strategies = (
        OneShotStrategy(zeta, side=OrderSide.BUY, quantity="1", calls=calls),
        OneShotStrategy(alpha, side=OrderSide.BUY, quantity="1", calls=calls),
    )
    first = {
        _BTC: _market("btc-1", _START, instrument=_BTC),
        _ETH: _market("eth-1", _START, instrument=_ETH),
    }
    second = {
        _BTC: _market("btc-2", _START + timedelta(seconds=1), instrument=_BTC),
        _ETH: _market("eth-2", _START + timedelta(seconds=1), instrument=_ETH),
    }

    engine, _ = _run_two_frames(tmp_path, config, strategies, (first, second))
    projection = engine.projection()

    assert calls[:2] == ["alpha", "zeta"]
    assert set(strategies[0].seen_event_ids[:2]) == {
        first[_BTC].event_id,
        first[_ETH].event_id,
    }
    assert set(strategies[1].seen_event_ids[:2]) == {
        first[_BTC].event_id,
        first[_ETH].event_id,
    }
    assert projection.strategy_projections["alpha"].positions == {_BTC: Decimal("1")}
    assert projection.strategy_projections["zeta"].positions == {_ETH: Decimal("1")}


def test_opposite_same_instrument_positions_preserve_attribution_and_account_net(
    tmp_path: Path,
) -> None:
    long_config = _strategy_config("long", instrument=_BTC)
    short_config = _strategy_config("short", instrument=_BTC)
    config = _portfolio_config((short_config, long_config))
    first = {_BTC: _market("same-1", _START)}
    second = {_BTC: _market("same-2", _START + timedelta(seconds=1))}

    engine, _ = _run_two_frames(
        tmp_path,
        config,
        (
            OneShotStrategy(short_config, side=OrderSide.SELL, quantity="0.06"),
            OneShotStrategy(long_config, side=OrderSide.BUY, quantity="0.10"),
        ),
        (first, second),
    )
    projection = engine.projection()

    assert projection.strategy_projections["long"].positions[_BTC] == Decimal("0.10")
    assert projection.strategy_projections["short"].positions[_BTC] == Decimal("-0.06")
    assert projection.positions[_BTC] == Decimal("0.04")
    assert projection.fees == sum(item.fees for item in projection.strategy_projections.values())
    assert engine.replay().to_dict() == projection.to_dict()
    assert engine.verify_input_replay().to_dict() == projection.to_dict()
    assert engine.reconcile(as_of=_START + timedelta(seconds=2)).projection.reconciled is True


def test_cross_strategy_internal_netting_keeps_distinct_realized_views(
    tmp_path: Path,
) -> None:
    long_config = _strategy_config("long", instrument=_BTC)
    short_config = _strategy_config("short", instrument=_BTC)
    config = _portfolio_config((long_config, short_config))
    engine = PaperEngine(PaperStore(tmp_path / "paper.sqlite3"), config)
    engine.start()
    runner = PortfolioRunner(
        engine,
        (
            OneShotStrategy(long_config, side=OrderSide.BUY, quantity="1"),
            OneShotStrategy(
                short_config,
                side=OrderSide.SELL,
                quantity="0.5",
                wait_evaluations=1,
            ),
        ),
    )
    runner.process_frame({_BTC: _market("netting-1", _START)}, processed_at=_START)
    runner.process_frame(
        {_BTC: _market("netting-2", _START + timedelta(seconds=1), bid="110", ask="110")},
        processed_at=_START + timedelta(seconds=1),
    )
    projection = runner.process_frame(
        {_BTC: _market("netting-3", _START + timedelta(seconds=2), bid="90", ask="90")},
        processed_at=_START + timedelta(seconds=2),
    ).projection

    assert projection.positions == {_BTC: Decimal("0.5")}
    assert projection.strategy_projections["long"].positions == {_BTC: Decimal("1")}
    assert projection.strategy_projections["short"].positions == {_BTC: Decimal("-0.5")}
    assert projection.realized_pnl != sum(
        strategy.realized_pnl for strategy in projection.strategy_projections.values()
    )
    assert engine.verify_input_replay().to_dict() == projection.to_dict()
    assert engine.reconcile(as_of=_START + timedelta(seconds=3)).projection.reconciled


def test_strategy_risk_rejection_does_not_block_other_strategy(tmp_path: Path) -> None:
    rejected = _strategy_config("a_rejected", instrument=_BTC, max_order_notional="50")
    accepted = _strategy_config("b_accepted", instrument=_BTC)
    config = _portfolio_config((accepted, rejected))
    first = {_BTC: _market("risk-1", _START)}
    second = {_BTC: _market("risk-2", _START + timedelta(seconds=1))}

    engine, _ = _run_two_frames(
        tmp_path,
        config,
        (
            OneShotStrategy(accepted, side=OrderSide.BUY, quantity="1"),
            OneShotStrategy(rejected, side=OrderSide.BUY, quantity="1"),
        ),
        (first, second),
    )
    projection = engine.projection()

    assert projection.strategy_projections["a_rejected"].positions == {}
    assert projection.strategy_projections["b_accepted"].positions == {_BTC: Decimal("1")}


def test_portfolio_risk_rejection_dominates_every_strategy(tmp_path: Path) -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    beta = _strategy_config("beta", instrument=_BTC)
    config = _portfolio_config(
        (alpha, beta),
        portfolio_risk=PaperRiskLimits(max_gross_notional=Decimal("50")),
    )
    first = {_BTC: _market("portfolio-risk-1", _START)}
    second = {_BTC: _market("portfolio-risk-2", _START + timedelta(seconds=1))}

    engine, _ = _run_two_frames(
        tmp_path,
        config,
        (
            OneShotStrategy(alpha, side=OrderSide.BUY, quantity="1"),
            OneShotStrategy(beta, side=OrderSide.BUY, quantity="1"),
        ),
        (first, second),
    )

    assert engine.projection().positions == {}


def test_phase08_strategy_level_emergency_flatten_retries_only_residual_owner(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config, engine, phase08, other, stalled = _phase08_asymmetric_exit_stall(database)
    phase08_stalled = stalled.strategy_projections[phase08.strategy_id]
    other_before = stalled.strategy_projections[other.strategy_id].to_dict()
    original_exit_id = phase08_stalled.current_exit_decision_id
    assert original_exit_id is not None
    original_btc_order = next(
        order
        for order in stalled.orders.values()
        if order.intent.decision_id == original_exit_id and order.intent.instrument == _BTC
    )

    residual = Decimal("-0.001329999999999999974352726946")
    assert original_btc_order.status is OrderStatus.EXPIRED
    assert original_btc_order.filled_quantity == Decimal("0.0002200000000000000256472730537")
    assert phase08_stalled.positions == {_BTC: residual}
    assert phase08_stalled.state is PaperState.EMERGENCY_FLATTEN
    assert stalled.strategy_projections[other.strategy_id].positions == {_BTC: Decimal("0.01")}
    assert stalled.state is PaperState.HEDGED
    assert not any(order.status.active for order in stalled.orders.values())
    assert not any(alert.code == "UNHEDGED_TIMEOUT" for alert in engine.store.get_alerts(config.run_id))
    engine.store.close()

    connection_id = "phase08-emergency-runtime-1"
    clock = MutableClock(_START + timedelta(seconds=5))
    connect_markets = {
        instrument: _market(
            f"phase08-emergency-connect-{instrument}",
            clock.value,
            instrument=instrument,
            tradable=False,
            source_event_kind="connect",
            source_connection_id=connection_id,
            source_connection_epoch=1,
        )
        for instrument in (_BTC, _ETH)
    }
    plan_at = _START + timedelta(seconds=5, milliseconds=500)
    plan_markets = {
        instrument: _market(
            f"phase08-emergency-plan-{instrument}",
            plan_at,
            instrument=instrument,
            source_event_kind="bbo",
            source_connection_id=connection_id,
            source_connection_epoch=1,
        )
        for instrument in (_BTC, _ETH)
    }
    runtime = _runtime(
        database,
        config,
        (RestoreHoldStrategy(phase08), RestoreHoldStrategy(other)),
        QueuePublicSource(
            [
                connect_markets,
                plan_markets,
                {
                    instrument: _market(
                        f"phase08-emergency-fill-{instrument}",
                        _START + timedelta(seconds=6, milliseconds=500),
                        instrument=instrument,
                        source_event_kind="bbo",
                        source_connection_id=connection_id,
                        source_connection_epoch=1,
                    )
                    for instrument in (_BTC, _ETH)
                },
                {
                    instrument: _market(
                        f"phase08-emergency-post-flat-{instrument}",
                        _START + timedelta(seconds=7, milliseconds=500),
                        instrument=instrument,
                        source_event_kind="bbo",
                        source_connection_id=connection_id,
                        source_connection_epoch=1,
                    )
                    for instrument in (_BTC, _ETH)
                },
            ]
        ),
        clock,
    )

    connected = runtime.run_once()
    assert connected.kind is PaperRuntimeStepKind.MARKET
    assert (
        connected.projection.strategy_projections[phase08.strategy_id].current_exit_decision_id
        == original_exit_id
    )
    clock.value = plan_at
    planned = runtime.run_once()
    assert planned.kind is PaperRuntimeStepKind.MARKET
    planned_phase08 = planned.projection.strategy_projections[phase08.strategy_id]
    emergency_exit_id = planned_phase08.current_exit_decision_id
    assert emergency_exit_id is not None
    assert emergency_exit_id != original_exit_id
    emergency_orders = [
        order for order in planned.projection.orders.values() if order.intent.decision_id == emergency_exit_id
    ]
    assert len(emergency_orders) == 1
    emergency_order = emergency_orders[0]
    assert emergency_order.intent.strategy_id == phase08.strategy_id
    assert emergency_order.intent.instrument == _BTC
    assert emergency_order.intent.side is OrderSide.BUY
    assert emergency_order.intent.quantity == abs(residual)
    assert emergency_order.intent.reduce_only is True
    assert emergency_order.intent.order_type is PaperOrderType.TAKER
    assert emergency_order.intent.time_in_force is TimeInForce.IOC
    assert planned.projection.strategy_projections[other.strategy_id].to_dict() == other_before

    clock.value = _START + timedelta(seconds=6, milliseconds=500)
    pending_timer = runtime.run_once()
    assert pending_timer.kind is PaperRuntimeStepKind.TIMER
    assert pending_timer.projection.strategy_projections[phase08.strategy_id].state is (
        PaperState.EMERGENCY_FLATTEN
    )
    assert pending_timer.projection.strategy_projections[phase08.strategy_id].positions == {_BTC: residual}
    flattened = runtime.run_once()
    assert flattened.kind is PaperRuntimeStepKind.MARKET
    flattened_phase08 = flattened.projection.strategy_projections[phase08.strategy_id]
    assert flattened_phase08.state is PaperState.FLAT
    assert flattened_phase08.positions == {}
    assert flattened.projection.strategy_projections[other.strategy_id].to_dict() == other_before
    assert flattened.projection.positions == {_BTC: Decimal("0.01")}
    assert flattened.projection.positions == paper_attributed_positions(
        {
            strategy_id: strategy.positions
            for strategy_id, strategy in flattened.projection.strategy_projections.items()
        }
    )
    filled_emergency_order = flattened.projection.orders[emergency_order.intent.order_id]
    assert filled_emergency_order.status is OrderStatus.FILLED
    assert filled_emergency_order.filled_quantity == abs(residual)

    exit_inputs = [
        record
        for record in runtime.engine.store.iter_inputs(
            config.run_id,
            input_type="STRATEGY_DECISION",
        )
        if record.payload["decision"]["action"] == "EXIT"
        and record.payload["decision"]["strategy_id"] == phase08.strategy_id
    ]
    assert len(exit_inputs) == 2

    clock.value = _START + timedelta(seconds=7, milliseconds=500)
    flat_timer = runtime.run_once()
    assert flat_timer.kind is PaperRuntimeStepKind.TIMER
    assert flat_timer.projection.positions == {_BTC: Decimal("0.01")}
    assert flat_timer.projection.strategy_projections[phase08.strategy_id].positions == {}
    post_flat = runtime.run_once()
    assert post_flat.kind is PaperRuntimeStepKind.MARKET
    assert post_flat.projection.positions == {_BTC: Decimal("0.01")}
    assert post_flat.projection.strategy_projections[phase08.strategy_id].positions == {}
    assert (
        len(
            [
                record
                for record in runtime.engine.store.iter_inputs(
                    config.run_id,
                    input_type="STRATEGY_DECISION",
                )
                if record.payload["decision"]["action"] == "EXIT"
                and record.payload["decision"]["strategy_id"] == phase08.strategy_id
            ]
        )
        == 2
    )
    runtime.close()

    recovered_store = PaperStore(database, initialize=False)
    recovered = PaperEngine(recovered_store, config)
    reconciled = recovered.reconcile(as_of=_START + timedelta(seconds=8)).projection
    assert recovered.replay().to_dict() == reconciled.to_dict()
    assert recovered.verify_input_replay().to_dict() == reconciled.to_dict()
    assert replay_paper_run(recovered_store, config.run_id).projection_hash == (reconciled.canonical_hash)
    recovered_store.close()


@pytest.mark.parametrize("market_mode", ("missing", "stale"))
def test_strategy_level_emergency_flatten_owned_market_failure_stays_fail_closed(
    tmp_path: Path,
    market_mode: str,
) -> None:
    database = tmp_path / f"{market_mode}.sqlite3"
    config, engine, phase08, other, stalled = _phase08_asymmetric_exit_stall(database)
    before_positions = stalled.positions
    before_strategy_positions = {
        strategy_id: dict(strategy.positions)
        for strategy_id, strategy in stalled.strategy_projections.items()
    }
    before_exit_inputs = [
        record
        for record in engine.store.iter_inputs(
            config.run_id,
            input_type="STRATEGY_DECISION",
        )
        if record.payload["decision"]["action"] == "EXIT"
    ]
    runtime = PaperRuntime(
        engine,
        (RestoreHoldStrategy(phase08), RestoreHoldStrategy(other)),
        QueuePublicSource([]),
        config=PaperRuntimeConfig(
            timer_interval_seconds=config.runtime_timer_interval_seconds,
            source_poll_timeout_seconds=config.runtime_source_poll_timeout_seconds,
        ),
        clock=MutableClock(_START + timedelta(seconds=5)),
    )

    if market_mode == "missing":
        market = _market(
            "phase08-emergency-unrelated-eth",
            _START + timedelta(seconds=5),
            instrument=_ETH,
        )
        engine.process_market(
            market,
            processed_at=market.received_at,
            execution_policy=MarketExecutionPolicy.BOOTSTRAP_OBSERVE_ONLY,
        )
        runtime._latest_markets[_ETH] = market
        as_of = market.received_at
    else:
        market = _market(
            "phase08-emergency-stale-btc",
            _START + timedelta(seconds=5),
        )
        engine.process_market(
            market,
            processed_at=market.received_at,
            execution_policy=MarketExecutionPolicy.BOOTSTRAP_OBSERVE_ONLY,
        )
        runtime._latest_markets[_BTC] = market
        as_of = market.received_at + timedelta(seconds=config.risk.stale_after_seconds + 1)

    result = runtime._ensure_automatic_emergency_flatten(
        engine.projection(),
        as_of=as_of,
    )
    after = engine.projection()
    assert result is None
    assert after.positions == before_positions
    assert {
        strategy_id: dict(strategy.positions) for strategy_id, strategy in after.strategy_projections.items()
    } == before_strategy_positions
    assert after.strategy_projections[phase08.strategy_id].state is (PaperState.EMERGENCY_FLATTEN)
    assert after.strategy_projections[other.strategy_id].state is PaperState.HEDGED
    assert [
        record
        for record in engine.store.iter_inputs(
            config.run_id,
            input_type="STRATEGY_DECISION",
        )
        if record.payload["decision"]["action"] == "EXIT"
    ] == before_exit_inputs
    assert engine.verify_input_replay().to_dict() == after.to_dict()
    engine.store.close()


def test_strategy_evaluation_failure_is_local_and_durable(tmp_path: Path) -> None:
    broken = _strategy_config("broken", instrument=_BTC)
    healthy = _strategy_config("healthy", instrument=_BTC)
    config = _portfolio_config((healthy, broken))
    first = {_BTC: _market("failure-1", _START)}
    second = {_BTC: _market("failure-2", _START + timedelta(seconds=1))}

    engine, _ = _run_two_frames(
        tmp_path,
        config,
        (
            OneShotStrategy(healthy, side=OrderSide.BUY, quantity="1"),
            OneShotStrategy(broken, side=OrderSide.BUY, quantity="1", fail=True),
        ),
        (first, second),
    )
    projection = engine.projection()

    assert projection.strategy_projections["broken"].state is PaperState.PAUSED
    assert projection.strategy_projections["healthy"].positions == {_BTC: Decimal("1")}
    assert projection.state is not PaperState.PAUSED
    assert engine.replay().to_dict() == projection.to_dict()
    assert engine.verify_input_replay().to_dict() == projection.to_dict()


def test_strategy_failure_with_owned_exposure_escalates_to_portfolio_protection(
    tmp_path: Path,
) -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    beta = _strategy_config("beta", instrument=_BTC)
    config = _portfolio_config((alpha, beta))
    engine, _ = _run_two_frames(
        tmp_path,
        config,
        (
            OneShotStrategy(alpha, side=OrderSide.BUY, quantity="0.1"),
            OneShotStrategy(beta, side=OrderSide.BUY, quantity="0.1"),
        ),
        (
            {_BTC: _market("exposed-failure-1", _START)},
            {_BTC: _market("exposed-failure-2", _START + timedelta(seconds=1))},
        ),
    )

    second_market = _market("exposed-failure-2", _START + timedelta(seconds=1))
    failed = engine.record_strategy_failure(
        strategy_id="alpha",
        as_of=_START + timedelta(seconds=2),
        phase="EVALUATION",
        error_type="RuntimeError",
        market_event_ids=(second_market.event_id,),
    ).projection

    assert failed.strategy_projections["alpha"].state is PaperState.PAUSED
    assert failed.strategy_projections["beta"].state is PaperState.HEDGED
    assert failed.state is PaperState.REDUCE_ONLY
    assert engine.verify_input_replay().to_dict() == failed.to_dict()

    engine.emergency_flatten(
        {_BTC: second_market},
        decided_at=_START + timedelta(seconds=2),
        reason="synthetic exposed strategy failure",
    )
    flattened = engine.process_market(
        _market("exposed-failure-3", _START + timedelta(seconds=3)),
        processed_at=_START + timedelta(seconds=3),
    ).projection

    assert flattened.positions == {}
    assert all(strategy.positions == {} for strategy in flattened.strategy_projections.values())
    assert flattened.state is PaperState.FLAT
    assert engine.verify_input_replay().to_dict() == flattened.to_dict()
    assert engine.reconcile(as_of=_START + timedelta(seconds=4)).projection.reconciled


def test_live_v3_decimal_paths_diverge_by_one_ulp_and_remain_rejected() -> None:
    fill_quantity = Decimal("0.00005098909090909090909090909091")
    fill_price = Decimal("73312")
    prior_strategy_cash = Decimal("4.029326699020161290322580600")
    prior_aggregate_cash = Decimal("2004.029326699020161290322581")

    with localcontext() as context:
        context.prec = 28
        fill_notional = fill_quantity * fill_price
        strategy_cash = prior_strategy_cash - fill_notional
        aggregate_cash = prior_aggregate_cash - fill_notional
        attributed_cash = Decimal("2000") + strategy_cash

    assert fill_notional == Decimal("3.738112232727272727272727273")
    assert strategy_cash == Decimal("0.291214466292888563049853327")
    assert aggregate_cash == Decimal("2000.291214466292888563049854")
    assert attributed_cash == Decimal("2000.291214466292888563049853")
    assert aggregate_cash - attributed_cash == Decimal("1E-24")
    with localcontext() as context:
        context.prec = 28
        assert context.next_plus(attributed_cash) == aggregate_cash

    # The historical head remains fail-closed. The fix prevents future v2 cash
    # events from creating it rather than weakening the exact model invariant.
    with localcontext() as context:
        context.prec = 9
        context.rounding = ROUND_DOWN
        with pytest.raises(
            ValueError,
            match="aggregate cash differs from durable strategy attribution",
        ):
            _live_cash_projection(aggregate_cash=str(aggregate_cash))


@pytest.mark.parametrize(
    "aggregate_cash",
    (
        "2000.291214466292888563049855",
        "2000.301214466292888563049853",
    ),
)
def test_cash_attribution_rejects_two_ulps_and_real_divergence(
    aggregate_cash: str,
) -> None:
    with pytest.raises(
        ValueError,
        match="aggregate cash differs from durable strategy attribution",
    ):
        _live_cash_projection(aggregate_cash=aggregate_cash)


def test_v2_repeated_live_fills_keep_cash_and_both_ledgers_exact() -> None:
    fill_quantity = Decimal("0.00005098909090909090909090909091")
    fill_price = Decimal("73312")
    prior_strategy_cash = Decimal("4.029326699020161290322580600")
    prior_aggregate_cash = Decimal("2004.029326699020161290322581")
    projection = _live_cash_projection(
        aggregate_cash=str(prior_aggregate_cash),
        phase08_cash=str(prior_strategy_cash),
    )
    decision_id = deterministic_id("v2_repeated_live_fill_decision")
    intent = OrderIntent.create(
        decision_id=decision_id,
        run_id=projection.run_id,
        instrument=_BTC,
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=_START,
        ordinal=0,
        strategy_id="phase08_robust_pairs",
    )
    projection.orders[intent.order_id] = PaperOrder(
        intent=intent,
        action=DecisionAction.ENTRY,
        status=OrderStatus.ACKED,
        active_at=_START,
    )
    ledger_cash = {
        "asset:cash": prior_aggregate_cash,
        "strategy:phase05_sentiment_btc:asset:cash": Decimal(0),
        "strategy:phase08_robust_pairs:asset:cash": prior_strategy_cash,
    }

    legacy_aggregate = prior_aggregate_cash
    legacy_strategy = prior_strategy_cash
    legacy_gaps: list[Decimal] = []
    with localcontext() as context:
        context.prec = 28
        fill_notional = fill_quantity * fill_price
        for _ in range(8):
            legacy_aggregate -= fill_notional
            legacy_strategy -= fill_notional
            legacy_gaps.append(legacy_aggregate - (Decimal("2000") + legacy_strategy))
    assert legacy_gaps[4] == Decimal("2E-24")
    assert legacy_gaps[7] == Decimal("3E-24")

    for index in range(8):
        at = _START + timedelta(seconds=index + 1)
        event = PaperEvent.create(
            run_id=projection.run_id,
            event_type=PaperEventType.ORDER_PARTIALLY_FILLED,
            occurred_at=at,
            received_at=at,
            causation_id=decision_id,
            correlation_id=decision_id,
            payload={
                "cash_math_version": PAPER_CASH_MATH_VERSION,
                "fee": "0",
                "fill_price": str(fill_price),
                "fill_quantity": str(fill_quantity),
                "order_id": intent.order_id,
                "strategy_id": "phase08_robust_pairs",
            },
            ordinal=index,
        )
        entries = transaction_ledger_amounts(projection, event)
        for entry_account, amount in entries:
            if entry_account in ledger_cash:
                ledger_cash[entry_account] = paper_accounting_add(
                    ledger_cash[entry_account],
                    amount,
                )
        apply_event(projection, event)
        projection.last_sequence += 1
        projection.last_event_hash = deterministic_id(
            "v2_repeated_live_fill_head",
            index,
        )
        assert projection.cash == paper_attributed_cash(
            projection.initial_cash,
            {strategy_id: strategy.cash for strategy_id, strategy in projection.strategy_projections.items()},
        )
        assert ledger_cash["asset:cash"] == projection.cash
        assert (
            ledger_cash["strategy:phase08_robust_pairs:asset:cash"]
            == projection.strategy_projections["phase08_robust_pairs"].cash
        )
        assert projection.clone().to_dict() == projection.to_dict()


def test_v2_fill_rounding_residual_is_applied_after_economic_cash_net() -> None:
    initial_cash = Decimal("3657.574928767318381788322355")
    strategy_cash = {
        "alpha": Decimal("884.5688696506178981696443430"),
        "beta": Decimal("-0.08972487665279251186597936499"),
    }
    strategies = {
        strategy_id: PaperStrategyProjection(
            strategy_id=strategy_id,
            strategy_name=strategy_id,
            strategy_hash=deterministic_id("v2_residual_strategy", strategy_id),
            strategy_config_hash=deterministic_id(
                "v2_residual_strategy_config",
                strategy_id,
            ),
            cash=cash,
        )
        for strategy_id, cash in strategy_cash.items()
    }
    prior_cash = paper_attributed_cash(
        initial_cash,
        {strategy_id: strategy.cash for strategy_id, strategy in strategies.items()},
    )
    assert prior_cash == Decimal("4542.054073541283487446100719")
    projection = PaperProjection(
        run_id=deterministic_id("v2_residual_run"),
        config_hash=deterministic_id("v2_residual_config"),
        initial_cash=initial_cash,
        cash=prior_cash,
        strategy_projections=strategies,
    )
    decision_id = deterministic_id("v2_residual_decision")
    intent = OrderIntent.create(
        decision_id=decision_id,
        run_id=projection.run_id,
        instrument=_BTC,
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=_START,
        ordinal=0,
        strategy_id="alpha",
    )
    projection.orders[intent.order_id] = PaperOrder(
        intent=intent,
        action=DecisionAction.ENTRY,
        status=OrderStatus.ACKED,
        active_at=_START,
    )
    event = PaperEvent.create(
        run_id=projection.run_id,
        event_type=PaperEventType.ORDER_FILLED,
        occurred_at=_START,
        received_at=_START,
        causation_id=decision_id,
        correlation_id=decision_id,
        payload={
            "cash_math_version": PAPER_CASH_MATH_VERSION,
            "fee": "0",
            "fill_price": "11795.31229246265339643428535",
            "fill_quantity": "1",
            "order_id": intent.order_id,
            "strategy_id": "alpha",
        },
    )

    entries = transaction_ledger_amounts(projection, event)
    assert ("asset:cash", Decimal("-4E-24")) in entries
    assert (
        "equity:cash_attribution_rounding",
        Decimal("4E-24"),
    ) in entries
    economic_cash = paper_accounting_add(
        prior_cash,
        Decimal("-11795.31229246265339643428535"),
    )
    ledger_cash = paper_accounting_add(economic_cash, Decimal("-4E-24"))

    apply_event(projection, event)

    expected_cash = Decimal("-7253.258218921369908988184635")
    assert economic_cash == Decimal("-7253.258218921369908988184631")
    assert ledger_cash == expected_cash
    assert projection.cash == expected_cash
    assert projection.clone().to_dict() == projection.to_dict()


def test_v2_single_strategy_fill_rounding_reconciles_split_close_and_open() -> None:
    price = Decimal("0.33333333333333333333333333335")
    prior_cash = Decimal("0.1")
    projection = PaperProjection(
        run_id=deterministic_id("v2_single_residual_run"),
        config_hash=deterministic_id("v2_single_residual_config"),
        initial_cash=prior_cash,
        cash=prior_cash,
        positions={_BTC: Decimal(1)},
        cost_basis={_BTC: price},
        inventory_value={_BTC: price},
    )
    decision_id = deterministic_id("v2_single_residual_decision")
    intent = OrderIntent.create(
        decision_id=decision_id,
        run_id=projection.run_id,
        instrument=_BTC,
        side=OrderSide.SELL,
        quantity=Decimal(2),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=_START,
        ordinal=0,
    )
    projection.orders[intent.order_id] = PaperOrder(
        intent=intent,
        action=DecisionAction.ENTRY,
        status=OrderStatus.ACKED,
        active_at=_START,
    )
    event = PaperEvent.create(
        run_id=projection.run_id,
        event_type=PaperEventType.ORDER_FILLED,
        occurred_at=_START,
        received_at=_START,
        causation_id=decision_id,
        correlation_id=decision_id,
        payload={
            "cash_math_version": PAPER_CASH_MATH_VERSION,
            "fee": "0",
            "fill_price": str(price),
            "fill_quantity": "2",
            "order_id": intent.order_id,
        },
    )

    entries = transaction_ledger_amounts(projection, event)
    correction = ("asset:cash", Decimal("-1E-28"))
    correction_index = entries.index(correction)
    assert entries[correction_index + 1] == (
        "equity:cash_attribution_rounding",
        Decimal("1E-28"),
    )
    economic_cash = Decimal(0)
    for account, amount in entries[:correction_index]:
        if account == "asset:cash":
            economic_cash = paper_accounting_add(economic_cash, amount)
    ledger_cash = paper_accounting_add(prior_cash, economic_cash)
    ledger_cash = paper_accounting_add(ledger_cash, correction[1])

    apply_event(projection, event)

    expected_cash = Decimal("0.7666666666666666666666666667")
    assert ledger_cash == expected_cash
    assert projection.cash == expected_cash
    assert projection.positions == {_BTC: Decimal(-1)}
    assert projection.clone().to_dict() == projection.to_dict()


def test_v2_fill_canonicalizes_position_and_fee_strategy_attribution() -> None:
    attributed = {
        "alpha": Decimal("9.435147383574709800817284726"),
        "zeta": Decimal("0.847524082815169828990388003"),
    }
    prior_total = Decimal("10.28267146638987962980767273")
    fill_quantity = Decimal("0.0153562739336565822727381152")
    strategies = {
        strategy_id: PaperStrategyProjection(
            strategy_id=strategy_id,
            strategy_name=strategy_id,
            strategy_hash=deterministic_id(
                "v2_position_fee_strategy",
                strategy_id,
            ),
            strategy_config_hash=deterministic_id(
                "v2_position_fee_strategy_config",
                strategy_id,
            ),
            fees=value,
            realized_pnl=-value,
            positions={_BTC: value},
            cost_basis={_BTC: Decimal(1)},
            inventory_value={_BTC: value},
        )
        for strategy_id, value in attributed.items()
    }
    projection = PaperProjection(
        run_id=deterministic_id("v2_position_fee_run"),
        config_hash=deterministic_id("v2_position_fee_config"),
        initial_cash=Decimal(1000),
        cash=Decimal(1000),
        fees=prior_total,
        realized_pnl=-prior_total,
        positions={_BTC: prior_total},
        cost_basis={_BTC: Decimal(1)},
        inventory_value={_BTC: prior_total},
        strategy_projections=strategies,
    )
    decision_id = deterministic_id("v2_position_fee_decision")
    intent = OrderIntent.create(
        decision_id=decision_id,
        run_id=projection.run_id,
        instrument=_BTC,
        side=OrderSide.BUY,
        quantity=fill_quantity,
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=_START,
        ordinal=0,
        strategy_id="alpha",
    )
    projection.orders[intent.order_id] = PaperOrder(
        intent=intent,
        action=DecisionAction.ENTRY,
        status=OrderStatus.ACKED,
        active_at=_START,
    )
    event = PaperEvent.create(
        run_id=projection.run_id,
        event_type=PaperEventType.ORDER_FILLED,
        occurred_at=_START,
        received_at=_START,
        causation_id=decision_id,
        correlation_id=decision_id,
        payload={
            "cash_math_version": PAPER_CASH_MATH_VERSION,
            "fee": str(fill_quantity),
            "fill_price": "1",
            "fill_quantity": str(fill_quantity),
            "order_id": intent.order_id,
            "strategy_id": "alpha",
        },
    )

    with localcontext() as context:
        context.prec = 28
        legacy_aggregate = prior_total + fill_quantity
        legacy_owner = attributed["alpha"] + fill_quantity
        canonical_total = legacy_owner + attributed["zeta"]
    assert legacy_aggregate == Decimal("10.29802774032353621208041085")
    assert canonical_total == Decimal("10.29802774032353621208041084")
    assert legacy_aggregate - canonical_total == Decimal("1E-26")

    entries = transaction_ledger_amounts(projection, event)
    fee_correction = ("expense:fees", Decimal("-1E-26"))
    fee_correction_index = entries.index(fee_correction)
    assert entries[fee_correction_index + 1] == (
        "equity:fee_attribution_rounding",
        Decimal("1E-26"),
    )

    initial_ledger = (
        ("asset:cash", Decimal(1000)),
        (f"asset:inventory:{_BTC}", prior_total),
        ("expense:fees", prior_total),
        (
            f"strategy:alpha:asset:inventory:{_BTC}",
            attributed["alpha"],
        ),
        ("strategy:alpha:expense:fees", attributed["alpha"]),
        (
            f"strategy:zeta:asset:inventory:{_BTC}",
            attributed["zeta"],
        ),
        ("strategy:zeta:expense:fees", attributed["zeta"]),
    )
    ledger_entries = [
        SimpleNamespace(transaction_id="fixture", account=account, amount=amount)
        for account, amount in initial_ledger
    ]
    ledger_entries.extend(
        SimpleNamespace(transaction_id="fill", account=account, amount=amount) for account, amount in entries
    )

    apply_event(projection, event)

    assert projection.positions == {_BTC: canonical_total}
    assert projection.fees == canonical_total
    assert projection.positions == paper_attributed_positions(
        {strategy_id: strategy.positions for strategy_id, strategy in projection.strategy_projections.items()}
    )
    assert projection.fees == paper_attributed_fees(
        {strategy_id: strategy.fees for strategy_id, strategy in projection.strategy_projections.items()}
    )
    assert projection.inventory_value[_BTC] == legacy_aggregate
    assert projection.clone().to_dict() == projection.to_dict()

    engine = object.__new__(PaperEngine)
    engine.config = SimpleNamespace(run_id=projection.run_id)
    engine.store = SimpleNamespace(iter_ledger_entries=lambda run_id: iter(ledger_entries))
    with localcontext() as context:
        context.prec = 9
        context.rounding = ROUND_DOWN
        assert engine._ledger_reconciliation_errors(projection) == ()


def test_v2_position_attribution_zero_topology_is_atomic_and_reconciled() -> None:
    attributed = {
        "c": Decimal("0.02509536853868544525094444620"),
        "b": Decimal("-7.713600717237376694254423198"),
        "a": Decimal("8.084264546566182011050842913"),
    }
    prior_total = Decimal("0.3957591978674907620473641612")
    strategies = {
        strategy_id: PaperStrategyProjection(
            strategy_id=strategy_id,
            strategy_name=strategy_id,
            strategy_hash=deterministic_id(
                "v2_position_topology_strategy",
                strategy_id,
            ),
            strategy_config_hash=deterministic_id(
                "v2_position_topology_strategy_config",
                strategy_id,
            ),
            positions={_BTC: value},
            cost_basis={_BTC: Decimal(1)},
            inventory_value={_BTC: value},
        )
        for strategy_id, value in attributed.items()
    }
    projection = PaperProjection(
        run_id=deterministic_id("v2_position_topology_run"),
        config_hash=deterministic_id("v2_position_topology_config"),
        initial_cash=Decimal(1000),
        cash=Decimal(1000),
        positions={_BTC: prior_total},
        cost_basis={_BTC: Decimal(1)},
        inventory_value={_BTC: prior_total},
        strategy_projections=strategies,
    )
    decision_id = deterministic_id("v2_position_topology_decision")
    intent = OrderIntent.create(
        decision_id=decision_id,
        run_id=projection.run_id,
        instrument=_BTC,
        side=OrderSide.SELL,
        quantity=prior_total,
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=_START,
        ordinal=0,
        strategy_id="a",
    )
    projection.orders[intent.order_id] = PaperOrder(
        intent=intent,
        action=DecisionAction.EXIT,
        status=OrderStatus.ACKED,
        active_at=_START,
    )
    event = PaperEvent.create(
        run_id=projection.run_id,
        event_type=PaperEventType.ORDER_FILLED,
        occurred_at=_START,
        received_at=_START,
        causation_id=decision_id,
        correlation_id=decision_id,
        payload={
            "cash_math_version": PAPER_CASH_MATH_VERSION,
            "fee": "0",
            "fill_price": "1",
            "fill_quantity": str(prior_total),
            "order_id": intent.order_id,
            "strategy_id": "a",
        },
    )

    entries = transaction_ledger_amounts(projection, event)
    inventory_correction = (
        f"asset:inventory:{_BTC}",
        Decimal("2E-28"),
    )
    correction_index = entries.index(inventory_correction)
    assert entries[correction_index + 1] == (
        f"equity:inventory_accounting_rounding:{_BTC}",
        Decimal("-2E-28"),
    )

    apply_event(projection, event)

    assert projection.positions == {_BTC: Decimal("2E-28")}
    assert projection.cost_basis == {_BTC: Decimal(1)}
    assert projection.inventory_value == {_BTC: Decimal("2E-28")}
    assert projection.clone().to_dict() == projection.to_dict()


def test_v2_position_attribution_removes_rounding_only_zero_position() -> None:
    attributed = {
        "alpha": Decimal("9.913007250937819729355250391"),
        "beta": Decimal("4.173035717830006939709408891"),
        "zeta": Decimal("-9.475268937434469263176177160"),
    }
    prior_total = Decimal("4.610774031333357405888482120")
    fill_quantity = Decimal("4.610774031333357405888482122")
    basis = Decimal("1.7")
    strategies = {
        strategy_id: PaperStrategyProjection(
            strategy_id=strategy_id,
            strategy_name=strategy_id,
            strategy_hash=deterministic_id(
                "v2_position_remove_strategy",
                strategy_id,
            ),
            strategy_config_hash=deterministic_id(
                "v2_position_remove_strategy_config",
                strategy_id,
            ),
            positions={_BTC: value},
            cost_basis={_BTC: basis},
            inventory_value={_BTC: value * basis},
        )
        for strategy_id, value in attributed.items()
    }
    projection = PaperProjection(
        run_id=deterministic_id("v2_position_remove_run"),
        config_hash=deterministic_id("v2_position_remove_config"),
        initial_cash=Decimal(1000),
        cash=Decimal(1000),
        positions={_BTC: prior_total},
        cost_basis={_BTC: basis},
        inventory_value={_BTC: prior_total * basis},
        strategy_projections=strategies,
    )
    decision_id = deterministic_id("v2_position_remove_decision")
    intent = OrderIntent.create(
        decision_id=decision_id,
        run_id=projection.run_id,
        instrument=_BTC,
        side=OrderSide.SELL,
        quantity=fill_quantity,
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=_START,
        ordinal=0,
        strategy_id="alpha",
    )
    projection.orders[intent.order_id] = PaperOrder(
        intent=intent,
        action=DecisionAction.EXIT,
        status=OrderStatus.ACKED,
        active_at=_START,
    )
    event = PaperEvent.create(
        run_id=projection.run_id,
        event_type=PaperEventType.ORDER_FILLED,
        occurred_at=_START,
        received_at=_START,
        causation_id=decision_id,
        correlation_id=decision_id,
        payload={
            "cash_math_version": PAPER_CASH_MATH_VERSION,
            "fee": "0",
            "fill_price": "3",
            "fill_quantity": str(fill_quantity),
            "order_id": intent.order_id,
            "strategy_id": "alpha",
        },
    )

    entries = transaction_ledger_amounts(projection, event)
    inventory_correction = (
        f"asset:inventory:{_BTC}",
        Decimal("6E-27"),
    )
    correction_index = entries.index(inventory_correction)
    assert entries[correction_index + 1] == (
        f"equity:inventory_accounting_rounding:{_BTC}",
        Decimal("-6E-27"),
    )

    apply_event(projection, event)

    assert projection.positions == {}
    assert projection.cost_basis == {}
    assert projection.inventory_value == {}
    assert projection.clone().to_dict() == projection.to_dict()


def test_v2_strategy_reversal_inventory_rounding_reconciles_exactly() -> None:
    prior_inventory = Decimal("884.5688696506178981696443430")
    fill_quantity = Decimal("1.08972487665279251186597936499")
    strategy = PaperStrategyProjection(
        strategy_id="alpha",
        strategy_name="alpha",
        strategy_hash=deterministic_id("v2_reversal_strategy"),
        strategy_config_hash=deterministic_id("v2_reversal_strategy_config"),
        positions={_BTC: Decimal(1)},
        cost_basis={_BTC: prior_inventory},
        inventory_value={_BTC: prior_inventory},
    )
    projection = PaperProjection(
        run_id=deterministic_id("v2_reversal_run"),
        config_hash=deterministic_id("v2_reversal_config"),
        initial_cash=Decimal(1000),
        cash=Decimal(1000),
        positions={_BTC: Decimal(1)},
        cost_basis={_BTC: prior_inventory},
        inventory_value={_BTC: prior_inventory},
        strategy_projections={"alpha": strategy},
    )
    decision_id = deterministic_id("v2_reversal_decision")
    intent = OrderIntent.create(
        decision_id=decision_id,
        run_id=projection.run_id,
        instrument=_BTC,
        side=OrderSide.SELL,
        quantity=Decimal(2),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=_START,
        ordinal=0,
        strategy_id="alpha",
    )
    projection.orders[intent.order_id] = PaperOrder(
        intent=intent,
        action=DecisionAction.EXIT,
        status=OrderStatus.ACKED,
        active_at=_START,
    )
    event = PaperEvent.create(
        run_id=projection.run_id,
        event_type=PaperEventType.ORDER_PARTIALLY_FILLED,
        occurred_at=_START,
        received_at=_START,
        causation_id=decision_id,
        correlation_id=decision_id,
        payload={
            "cash_math_version": PAPER_CASH_MATH_VERSION,
            "fee": "0",
            "fill_price": "1",
            "fill_quantity": str(fill_quantity),
            "order_id": intent.order_id,
            "strategy_id": "alpha",
        },
    )

    entries = transaction_ledger_amounts(projection, event)
    strategy_inventory_account = f"strategy:alpha:asset:inventory:{_BTC}"
    correction_entries = [
        (index, amount)
        for index, (account, amount) in enumerate(entries)
        if account == strategy_inventory_account and abs(amount) < Decimal("1E-20")
    ]
    assert correction_entries == [(12, Decimal("3.5E-26"))]
    correction_index, correction = correction_entries[0]
    assert entries[correction_index + 1] == (
        f"strategy:alpha:equity:inventory_accounting_rounding:{_BTC}",
        correction.copy_negate(),
    )
    initial_ledger = (
        ("asset:cash", Decimal(1000)),
        (f"asset:inventory:{_BTC}", prior_inventory),
        (strategy_inventory_account, prior_inventory),
    )
    ledger_entries = [
        SimpleNamespace(transaction_id="fixture", account=account, amount=amount)
        for account, amount in initial_ledger
    ]
    ledger_entries.extend(
        SimpleNamespace(transaction_id="fill", account=account, amount=amount) for account, amount in entries
    )

    apply_event(projection, event)

    engine = object.__new__(PaperEngine)
    engine.config = SimpleNamespace(run_id=projection.run_id)
    engine.store = SimpleNamespace(iter_ledger_entries=lambda run_id: iter(ledger_entries))
    assert projection.strategy_projections["alpha"].inventory_value == {
        _BTC: Decimal("-0.089724876652792511865979365")
    }
    assert engine._ledger_reconciliation_errors(projection) == ()


def test_strategy_realized_pnl_reconciliation_preserves_transaction_order() -> None:
    funding_then_fee = Decimal("884.5688696506178981696443430")
    final_funding = Decimal("-0.08972487665279251186597936499")
    strategy = PaperStrategyProjection(
        strategy_id="alpha",
        strategy_name="alpha",
        strategy_hash=deterministic_id("v2_realized_order_strategy"),
        strategy_config_hash=deterministic_id("v2_realized_order_strategy_config"),
        cash=final_funding,
        fees=funding_then_fee,
        realized_pnl=final_funding,
    )
    projection = PaperProjection(
        run_id=deterministic_id("v2_realized_order_run"),
        config_hash=deterministic_id("v2_realized_order_config"),
        initial_cash=Decimal(1000),
        cash=paper_attributed_cash(
            Decimal(1000),
            {"alpha": final_funding},
        ),
        fees=funding_then_fee,
        realized_pnl=final_funding,
        strategy_projections={"alpha": strategy},
    )
    ledger_transactions = (
        (
            "initial",
            (
                ("asset:cash", Decimal(1000)),
                ("equity:initial_capital", Decimal(-1000)),
            ),
        ),
        (
            "funding-1",
            (
                ("asset:cash", funding_then_fee),
                ("income:funding", funding_then_fee.copy_negate()),
                ("strategy:alpha:asset:cash", funding_then_fee),
                (
                    "strategy:alpha:income:funding",
                    funding_then_fee.copy_negate(),
                ),
            ),
        ),
        (
            "fee",
            (
                ("asset:cash", funding_then_fee.copy_negate()),
                ("expense:fees", funding_then_fee),
                (
                    "strategy:alpha:asset:cash",
                    funding_then_fee.copy_negate(),
                ),
                ("strategy:alpha:expense:fees", funding_then_fee),
            ),
        ),
        (
            "funding-2",
            (
                ("asset:cash", final_funding),
                ("income:funding", final_funding.copy_negate()),
                ("strategy:alpha:asset:cash", final_funding),
                (
                    "strategy:alpha:income:funding",
                    final_funding.copy_negate(),
                ),
            ),
        ),
    )
    ledger_entries = [
        SimpleNamespace(transaction_id=transaction_id, account=account, amount=amount)
        for transaction_id, entries in ledger_transactions
        for account, amount in entries
    ]
    engine = object.__new__(PaperEngine)
    engine.config = SimpleNamespace(run_id=projection.run_id)
    engine.store = SimpleNamespace(iter_ledger_entries=lambda run_id: iter(ledger_entries))

    assert engine._ledger_reconciliation_errors(projection) == ()


@pytest.mark.parametrize(
    "mutation",
    ("cash", "fees", "aggregate_inventory", "strategy_inventory"),
)
def test_v2_rounding_accounts_reject_material_accounting_mutation(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    projection = _live_cash_projection(
        aggregate_cash="2004.029326699020161290322581",
        phase08_cash="4.029326699020161290322580600",
    )
    fill_quantity = Decimal("0.00005098909090909090909090909091")
    decision_id = deterministic_id("v2_material_mutation_decision")
    intent = OrderIntent.create(
        decision_id=decision_id,
        run_id=projection.run_id,
        instrument=_BTC,
        side=OrderSide.BUY,
        quantity=fill_quantity,
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=_START,
        ordinal=0,
        strategy_id="phase08_robust_pairs",
    )
    projection.orders[intent.order_id] = PaperOrder(
        intent=intent,
        action=DecisionAction.ENTRY,
        status=OrderStatus.ACKED,
        active_at=_START,
    )
    event = PaperEvent.create(
        run_id=projection.run_id,
        event_type=PaperEventType.ORDER_FILLED,
        occurred_at=_START,
        received_at=_START,
        causation_id=decision_id,
        correlation_id=decision_id,
        payload={
            "cash_math_version": PAPER_CASH_MATH_VERSION,
            "fee": "0.01",
            "fill_price": "73312",
            "fill_quantity": str(fill_quantity),
            "order_id": intent.order_id,
            "strategy_id": "phase08_robust_pairs",
        },
    )
    real_apply_event = paper_reducer_module.apply_event

    def apply_with_material_mutation(
        working: PaperProjection,
        applied_event: PaperEvent,
    ) -> PaperProjection:
        updated = real_apply_event(working, applied_event)
        owner = updated.strategy_projection("phase08_robust_pairs")
        if mutation == "cash":
            owner.cash = paper_accounting_add(owner.cash, Decimal(100))
            updated.cash = paper_accounting_add(updated.cash, Decimal(100))
        elif mutation == "fees":
            owner.fees = paper_accounting_add(owner.fees, Decimal(100))
            owner.realized_pnl = paper_accounting_add(
                owner.realized_pnl,
                Decimal(-100),
            )
            updated.fees = paper_accounting_add(
                updated.fees,
                Decimal(100),
            )
            updated.realized_pnl = paper_accounting_add(
                updated.realized_pnl,
                Decimal(-100),
            )
        elif mutation == "aggregate_inventory":
            updated.inventory_value[_BTC] = paper_accounting_add(
                updated.inventory_value[_BTC],
                Decimal(100),
            )
            updated.cost_basis[_BTC] = abs(updated.inventory_value[_BTC] / updated.positions[_BTC])
        else:
            owner.inventory_value[_BTC] = paper_accounting_add(
                owner.inventory_value[_BTC],
                Decimal(100),
            )
            owner.cost_basis[_BTC] = abs(owner.inventory_value[_BTC] / owner.positions[_BTC])
        return updated

    with monkeypatch.context() as patch:
        patch.setattr(
            paper_reducer_module,
            "apply_event",
            apply_with_material_mutation,
        )
        with pytest.raises(
            AssertionError,
            match="differs from its economic transition",
        ):
            paper_reducer_module.transaction_ledger_amounts(
                projection,
                event,
            )


def test_v2_rounding_accounts_reject_balanced_but_false_ordinary_ledger() -> None:
    strategy = PaperStrategyProjection(
        strategy_id="alpha",
        strategy_name="alpha",
        strategy_hash=deterministic_id("v2_false_ledger_strategy"),
        strategy_config_hash=deterministic_id("v2_false_ledger_strategy_config"),
    )
    projection = PaperProjection(
        run_id=deterministic_id("v2_false_ledger_run"),
        config_hash=deterministic_id("v2_false_ledger_config"),
        initial_cash=Decimal(100),
        cash=Decimal(100),
        strategy_projections={"alpha": strategy},
    )
    decision_id = deterministic_id("v2_false_ledger_decision")
    intent = OrderIntent.create(
        decision_id=decision_id,
        run_id=projection.run_id,
        instrument=_BTC,
        side=OrderSide.BUY,
        quantity=Decimal(10),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=_START,
        ordinal=0,
        strategy_id="alpha",
    )
    projection.orders[intent.order_id] = PaperOrder(
        intent=intent,
        action=DecisionAction.ENTRY,
        status=OrderStatus.ACKED,
        active_at=_START,
    )
    event = PaperEvent.create(
        run_id=projection.run_id,
        event_type=PaperEventType.ORDER_FILLED,
        occurred_at=_START,
        received_at=_START,
        causation_id=decision_id,
        correlation_id=decision_id,
        payload={
            "cash_math_version": PAPER_CASH_MATH_VERSION,
            "fee": "0",
            "fill_price": "1",
            "fill_quantity": "10",
            "order_id": intent.order_id,
            "strategy_id": "alpha",
        },
    )
    false_but_balanced = [
        ("asset:cash", Decimal(-110)),
        (f"asset:inventory:{_BTC}", Decimal(110)),
        ("strategy:alpha:asset:cash", Decimal(-110)),
        (
            f"strategy:alpha:asset:inventory:{_BTC}",
            Decimal(110),
        ),
    ]

    with pytest.raises(
        AssertionError,
        match="ordinary ledger entries differ from economic postings",
    ):
        paper_reducer_module._with_attribution_rounding_entries(
            projection,
            event,
            false_but_balanced,
        )


def test_v2_fill_projection_is_independent_of_ambient_decimal_context() -> None:
    strategies = {
        strategy_id: PaperStrategyProjection(
            strategy_id=strategy_id,
            strategy_name=strategy_id,
            strategy_hash=deterministic_id("v2_context_strategy", strategy_id),
            strategy_config_hash=deterministic_id(
                "v2_context_strategy_config",
                strategy_id,
            ),
        )
        for strategy_id in ("alpha", "beta")
    }
    base = PaperProjection(
        run_id=deterministic_id("v2_context_run"),
        config_hash=deterministic_id("v2_context_config"),
        initial_cash=Decimal(1000),
        cash=Decimal(1000),
        strategy_projections=strategies,
    )
    decision_id = deterministic_id("v2_context_decision")
    intent = OrderIntent.create(
        decision_id=decision_id,
        run_id=base.run_id,
        instrument=_BTC,
        side=OrderSide.BUY,
        quantity=Decimal(1),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=_START,
        ordinal=0,
        strategy_id="alpha",
    )
    base.orders[intent.order_id] = PaperOrder(
        intent=intent,
        action=DecisionAction.ENTRY,
        status=OrderStatus.ACKED,
        active_at=_START,
    )
    event = PaperEvent.create(
        run_id=base.run_id,
        event_type=PaperEventType.ORDER_FILLED,
        occurred_at=_START,
        received_at=_START,
        causation_id=decision_id,
        correlation_id=decision_id,
        payload={
            "cash_math_version": PAPER_CASH_MATH_VERSION,
            "fee": "0.1234567890123456789012345678",
            "fill_price": "1.234567890123456789012345678",
            "fill_quantity": "1",
            "order_id": intent.order_id,
            "strategy_id": "alpha",
        },
    )
    expected = base.clone()
    observed = base.clone()

    expected_entries = transaction_ledger_amounts(expected, event)
    apply_event(expected, event)
    with localcontext() as context:
        context.prec = 9
        context.rounding = ROUND_DOWN
        observed_entries = transaction_ledger_amounts(observed, event)
        apply_event(observed, event)

    assert observed_entries == expected_entries
    assert observed.to_dict() == expected.to_dict()
    assert observed.fees == Decimal("0.1234567890123456789012345678")
    assert observed.inventory_value[_BTC] == Decimal("1.234567890123456789012345678")
    assert observed.realized_pnl == Decimal("-0.1234567890123456789012345678")


def test_v2_market_input_replay_is_independent_of_ambient_decimal_context(
    tmp_path: Path,
) -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    beta = _strategy_config("beta", instrument=_BTC)
    config = _portfolio_config((alpha, beta))
    store = PaperStore(tmp_path / "ambient-context-replay.sqlite3")
    engine = PaperEngine(store, config)
    engine.start()
    first = _market(
        "ambient-context-first",
        _START,
        bid="1.234567890123456789012345678",
        ask="1.234567890123456789012345678",
    )
    observed = engine.process_market(first).projection
    strategy = OneShotStrategy(
        alpha,
        side=OrderSide.BUY,
        quantity="0.9876543210987654321098765432",
    )
    decision = strategy.decide(
        {_BTC: first},
        PaperStrategyView.from_projection(observed, alpha),
    )
    assert decision is not None
    engine.submit_decision(decision, {_BTC: first})
    second = _market(
        "ambient-context-fill",
        _START + timedelta(seconds=1),
        bid="1.234567890123456789012345678",
        ask="1.234567890123456789012345678",
    )

    with localcontext() as context:
        context.prec = 9
        context.rounding = ROUND_DOWN
        filled = engine.process_market(second).projection

    event_hashes = tuple(event.event_hash for event in store.iter_events(config.run_id))
    replayed = engine.verify_input_replay()

    assert replayed.to_dict() == filled.to_dict()
    assert tuple(event.event_hash for event in store.iter_events(config.run_id)) == event_hashes
    assert observed.clone().to_dict() == observed.to_dict()


def test_v2_multistrategy_funding_uses_canonical_cash_and_rounding_ledger() -> None:
    prior_strategy_cash = Decimal("4.029326699020161290322580600")
    prior_aggregate_cash = Decimal("2004.029326699020161290322581")
    phase05_amount = Decimal("0.1234567890123456789012345678")
    phase08_amount = Decimal("-3.861569021739618406173961841")
    total_amount = Decimal("-3.738112232727272727272727273")
    projection = _live_cash_projection(
        aggregate_cash=str(prior_aggregate_cash),
        phase08_cash=str(prior_strategy_cash),
    )
    event = PaperEvent.create(
        run_id=projection.run_id,
        event_type=PaperEventType.FUNDING_POSTED,
        occurred_at=_START,
        received_at=_START,
        causation_id=deterministic_id("v2_funding_source"),
        correlation_id=deterministic_id("v2_funding_input"),
        payload={
            "amount": str(total_amount),
            "cash_math_version": PAPER_CASH_MATH_VERSION,
            "strategy_amounts": {
                "phase05_sentiment_btc": str(phase05_amount),
                "phase08_robust_pairs": str(phase08_amount),
            },
        },
    )
    ledger_cash = {
        "asset:cash": prior_aggregate_cash,
        "strategy:phase05_sentiment_btc:asset:cash": Decimal(0),
        "strategy:phase08_robust_pairs:asset:cash": prior_strategy_cash,
    }

    # Cash v2 remains deterministic even if unrelated caller code changed the
    # ambient Decimal context before invoking the reducer.
    with localcontext() as context:
        context.prec = 9
        context.rounding = ROUND_DOWN
        entries = transaction_ledger_amounts(projection, event)
        apply_event(projection, event)

    for entry_account, amount in entries:
        if entry_account in ledger_cash:
            ledger_cash[entry_account] = paper_accounting_add(
                ledger_cash[entry_account],
                amount,
            )

    expected_cash = Decimal("2000.291214466292888563049853")
    assert projection.cash == expected_cash
    assert projection.cash == paper_attributed_cash(
        projection.initial_cash,
        {strategy_id: strategy.cash for strategy_id, strategy in projection.strategy_projections.items()},
    )
    assert ledger_cash["asset:cash"] == expected_cash
    assert ledger_cash["strategy:phase05_sentiment_btc:asset:cash"] == (
        projection.strategy_projections["phase05_sentiment_btc"].cash
    )
    assert ledger_cash["strategy:phase08_robust_pairs:asset:cash"] == (
        projection.strategy_projections["phase08_robust_pairs"].cash
    )
    assert ("asset:cash", Decimal("-1E-24")) in entries
    assert (
        "equity:cash_attribution_rounding",
        Decimal("1E-24"),
    ) in entries
    assert projection.clone().to_dict() == projection.to_dict()


def test_v2_funding_rejects_inconsistent_rounding_metadata() -> None:
    projection = _live_cash_projection(
        aggregate_cash="2004.029326699020161290322581",
        phase08_cash="4.029326699020161290322580600",
    )
    event = PaperEvent.create(
        run_id=projection.run_id,
        event_type=PaperEventType.FUNDING_POSTED,
        occurred_at=_START,
        received_at=_START,
        causation_id=deterministic_id("invalid_funding_rounding_source"),
        correlation_id=deterministic_id("invalid_funding_rounding_input"),
        payload={
            "amount": "0",
            "cash_math_version": PAPER_CASH_MATH_VERSION,
            "strategy_amounts": {
                "phase05_sentiment_btc": "0",
                "phase08_robust_pairs": "0",
            },
            "strategy_funding_rounding": {
                "allocated_amount": "0",
                "raw_amount": "0.01",
                "residual": "0.02",
                "strategy_id": "phase08_robust_pairs",
            },
        },
    )

    with pytest.raises(
        ValueError,
        match="strategy funding rounding metadata is inconsistent",
    ):
        transaction_ledger_amounts(projection, event)


def test_v2_funding_realized_pnl_uses_the_booked_strategy_amount() -> None:
    prior_realized = Decimal("-0.4109615443561567624369907676")
    strategies = {
        strategy_id: PaperStrategyProjection(
            strategy_id=strategy_id,
            strategy_name=strategy_id,
            strategy_hash=deterministic_id("v2_funding_pnl_strategy", strategy_id),
            strategy_config_hash=deterministic_id(
                "v2_funding_pnl_strategy_config",
                strategy_id,
            ),
            cash=Decimal(0),
            realized_pnl=(prior_realized if strategy_id == "zeta" else Decimal(0)),
        )
        for strategy_id in ("alpha", "zeta")
    }
    projection = PaperProjection(
        run_id=deterministic_id("v2_funding_pnl_run"),
        config_hash=deterministic_id("v2_funding_pnl_config"),
        initial_cash=Decimal(1000),
        cash=Decimal(1000),
        realized_pnl=prior_realized,
        strategy_projections=strategies,
    )
    account_amount = Decimal("0.4013317705351950009472354289")
    allocated_zeta = Decimal("1.1717386981716144432164247357")
    booked_zeta = Decimal("1.171738698171614443216424736")
    event = PaperEvent.create(
        run_id=projection.run_id,
        event_type=PaperEventType.FUNDING_POSTED,
        occurred_at=_START,
        received_at=_START,
        causation_id=deterministic_id("v2_funding_pnl_source"),
        correlation_id=deterministic_id("v2_funding_pnl_input"),
        payload={
            "amount": str(account_amount),
            "cash_math_version": PAPER_CASH_MATH_VERSION,
            "strategy_amounts": {
                "alpha": "-0.7704069276364194422691893068",
                "zeta": str(allocated_zeta),
            },
            "strategy_funding_rounding": {
                "allocated_amount": str(allocated_zeta),
                "raw_amount": "1.171738698171614443216424736",
                "residual": "-0.0000000000000000000000000003",
                "strategy_id": "zeta",
            },
        },
    )

    entries = transaction_ledger_amounts(projection, event)
    assert (
        "strategy:zeta:income:funding",
        booked_zeta.copy_negate(),
    ) in entries
    apply_event(projection, event)

    assert projection.strategy_projections["zeta"].realized_pnl == Decimal("0.7607771538154576807794339684")
    assert projection.realized_pnl == paper_accounting_add(
        prior_realized,
        account_amount,
    )
    assert projection.clone().to_dict() == projection.to_dict()


def test_v2_funding_allocates_and_records_non_distributive_rounding_residual(
    tmp_path: Path,
) -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    zeta = _strategy_config("zeta", instrument=_BTC)
    config = _portfolio_config((zeta, alpha))
    store = PaperStore(tmp_path / "funding-residual.sqlite3")
    engine = PaperEngine(store, config)
    engine.start()
    runner = PortfolioRunner(
        engine,
        (
            OneShotStrategy(
                zeta,
                side=OrderSide.SELL,
                quantity="4.69547837477505620",
            ),
            OneShotStrategy(
                alpha,
                side=OrderSide.BUY,
                quantity="3.08723188381364316",
            ),
        ),
    )
    runner.process_frame(
        {_BTC: _market("funding-residual-plan", _START, bid="1", ask="1")},
        processed_at=_START,
    )
    exposed = runner.process_frame(
        {
            _BTC: _market(
                "funding-residual-fill",
                _START + timedelta(seconds=1),
                bid="1",
                ask="1",
            )
        },
        processed_at=_START + timedelta(seconds=1),
    )
    aggregate_position = Decimal("-1.60824649096141304")
    assert exposed.projection.positions == {_BTC: aggregate_position}
    assert {
        strategy_id: strategy.positions
        for strategy_id, strategy in exposed.projection.strategy_projections.items()
    } == {
        "alpha": {_BTC: Decimal("3.08723188381364316")},
        "zeta": {_BTC: Decimal("-4.69547837477505620")},
    }

    mark = Decimal("25627.3357")
    rate = Decimal("0.0000097375")
    account_amount = Decimal("0.4013317705351950009472354289")
    with localcontext() as context:
        context.prec = 9
        context.rounding = ROUND_DOWN
        funded = engine.post_funding(
            instrument=_BTC,
            amount=account_amount,
            occurred_at=_START + timedelta(seconds=2),
            source_event_id=deterministic_id("v2_funding_rounding_source"),
            funding_rate=rate,
            funding_interval_seconds=3600,
            rate_kind="synthetic-hourly-settlement",
            mark_price=mark,
            source_mark_price=mark,
            position_quantity=aggregate_position,
            mark_source="PUBLIC_SETTLEMENT_MARK",
            source_observation_id="v2-funding-rounding-observation",
            received_at=_START + timedelta(seconds=2),
            processed_at=_START + timedelta(seconds=2),
        ).projection
    before_duplicate_run = store.get_run(config.run_id)
    before_duplicate_events = tuple(store.iter_events(config.run_id))
    before_duplicate_ledger = tuple(store.iter_ledger_entries(config.run_id))
    duplicate = engine.post_funding(
        instrument=_BTC,
        amount=account_amount,
        occurred_at=_START + timedelta(seconds=2),
        source_event_id=deterministic_id("v2_funding_rounding_source"),
        funding_rate=rate,
        funding_interval_seconds=3600,
        rate_kind="synthetic-hourly-settlement",
        mark_price=mark,
        source_mark_price=mark,
        position_quantity=aggregate_position,
        mark_source="PUBLIC_SETTLEMENT_MARK",
        source_observation_id="v2-funding-rounding-observation",
        received_at=_START + timedelta(seconds=2),
        processed_at=_START + timedelta(seconds=2),
    )
    assert duplicate.append.idempotent
    assert store.get_run(config.run_id) == before_duplicate_run
    assert tuple(store.iter_events(config.run_id)) == before_duplicate_events
    assert tuple(store.iter_ledger_entries(config.run_id)) == before_duplicate_ledger

    funding_inputs = tuple(
        store.iter_inputs(
            config.run_id,
            input_type="PUBLIC_FUNDING_SETTLEMENT",
        )
    )
    assert len(funding_inputs) == 1
    assert "strategy_amounts" not in funding_inputs[0].payload
    assert "strategy_funding_rounding" not in funding_inputs[0].payload
    funding_events = tuple(
        stored.event
        for stored in store.iter_events(config.run_id)
        if stored.event.event_type is PaperEventType.FUNDING_POSTED
    )
    assert len(funding_events) == 1
    payload = funding_events[0].payload
    strategy_amounts = payload["strategy_amounts"]
    rounding = payload["strategy_funding_rounding"]
    assert isinstance(strategy_amounts, Mapping)
    assert isinstance(rounding, Mapping)
    assert strategy_amounts == {
        "alpha": "-0.7704069276364194422691893068",
        "zeta": "1.1717386981716144432164247357",
    }
    assert rounding["strategy_id"] == "zeta"
    assert Decimal(str(rounding["raw_amount"])) == Decimal("1.171738698171614443216424736")
    assert Decimal(str(rounding["allocated_amount"])) == Decimal(strategy_amounts["zeta"])
    assert Decimal(str(rounding["residual"])) == Decimal("-3E-28")
    funding_ledger = tuple(
        entry.amount
        for entry in store.iter_ledger_entries(config.run_id)
        if entry.event_id == funding_events[0].event_id
    )
    with localcontext() as context:
        context.prec = 50
        assert sum(funding_ledger, Decimal(0)) == 0
    assert funded.cash == paper_attributed_cash(
        funded.initial_cash,
        {strategy_id: strategy.cash for strategy_id, strategy in funded.strategy_projections.items()},
    )
    assert engine.replay().to_dict() == funded.to_dict()

    reconciled = engine.reconcile(
        as_of=_START + timedelta(seconds=3),
    ).projection
    assert reconciled.reconciled
    assert engine.verify_input_replay().to_dict() == reconciled.to_dict()


def test_historical_v1_cash_input_replay_preserves_events_and_ledger(
    tmp_path: Path,
) -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    beta = _strategy_config("beta", instrument=_BTC)
    config = replace(
        _portfolio_config((alpha, beta)),
        release_code_sha256=("9085bb4a11095e478f3056feba8eb81d93dffd014d1c72e409f7f24c2657c35d"),
        runtime_environment_sha256=("9c41d224d2a2ae66a47617906f3f9ad8bb9df4a7de259af0c116d585274df732"),
    )
    assert config.config_hash == ("8a3c601e69ce529a2a7200c93b97ca4d9c248711a1cdde4b8e89d6d9a90648c8")
    assert config.run_id == ("b091fd6478ef246f9df6ce51bd27e07c0bf111a3fd899f8b86837c184cb4cf47")
    temporary_directory = TemporaryDirectory(dir=tmp_path)
    database = Path(temporary_directory.name) / "historical-v1.sqlite3"
    fixture_store = PaperStore._create_temporary_historical_replay(
        temporary_directory,
        filename=database.name,
    )
    fixture_engine = PaperEngine._for_historical_replay(fixture_store, config)
    fixture_engine.start()
    first = _market("historical-v1-first", _START)
    observed = fixture_engine.process_market(
        first,
        _cash_math_version=1,
    ).projection
    strategy = OneShotStrategy(alpha, side=OrderSide.BUY, quantity="1")
    decision = strategy.decide(
        {_BTC: first},
        PaperStrategyView.from_projection(observed, alpha),
    )
    assert decision is not None
    fixture_engine.submit_decision(decision, {_BTC: first})
    second = _market(
        "historical-v1-second",
        _START + timedelta(seconds=1),
    )
    fixture_engine.process_market(
        second,
        _cash_math_version=1,
    )
    durable_projection = fixture_engine.post_funding(
        instrument=_BTC,
        amount=Decimal("-0.1"),
        occurred_at=_START + timedelta(seconds=2),
        source_event_id=deterministic_id("historical_v1_funding_source"),
        funding_rate=Decimal("0.001"),
        funding_interval_seconds=3600,
        rate_kind="historical-v1-fixture",
        mark_price=Decimal(100),
        source_mark_price=Decimal(100),
        position_quantity=Decimal(1),
        mark_source="PUBLIC_SETTLEMENT_MARK",
        source_observation_id="historical-v1-funding-observation",
        received_at=_START + timedelta(seconds=2),
        processed_at=_START + timedelta(seconds=2),
        _cash_math_version=1,
    ).projection

    v1_inputs = tuple(fixture_store.iter_inputs(config.run_id, input_type="PUBLIC_MARKET_EVENT"))
    v1_funding_inputs = tuple(
        fixture_store.iter_inputs(
            config.run_id,
            input_type="PUBLIC_FUNDING_SETTLEMENT",
        )
    )
    fills = tuple(
        event
        for event in fixture_store.iter_events(config.run_id)
        if event.event.event_type
        in {
            PaperEventType.ORDER_PARTIALLY_FILLED,
            PaperEventType.ORDER_FILLED,
        }
    )
    assert len(v1_inputs) == 2
    assert all("cash_math_version" not in item.payload for item in v1_inputs)
    assert len(v1_funding_inputs) == 1
    assert "cash_math_version" not in v1_funding_inputs[0].payload
    assert "strategy_amounts" in v1_funding_inputs[0].payload
    assert len(fills) == 1
    assert "cash_math_version" not in fills[0].event.payload
    v1_funding_events = tuple(
        event
        for event in fixture_store.iter_events(config.run_id)
        if event.event.event_type is PaperEventType.FUNDING_POSTED
    )
    assert len(v1_funding_events) == 1
    assert "cash_math_version" not in v1_funding_events[0].event.payload
    assert "strategy_funding_rounding" not in v1_funding_events[0].event.payload
    event_hashes = tuple(event.event_hash for event in fixture_store.iter_events(config.run_id))
    ledger_hashes = tuple(entry.entry_hash for entry in fixture_store.iter_ledger_entries(config.run_id))
    assert event_hashes == (
        "2b214c395658c69c99daa943416bf51b14b2c3c56654e2bfeb9f4dd474155992",
        "72d4f2dd00a2006a5fc407081b8d8017205782e704d1d1fabd96a0da2bee8efc",
        "1861cf08c114575051033b3b3e68d85417aaf7326708b86e84639bb019d286d3",
        "c96946d1f80dc0aa20de5a449027e9c43b06ccd5a748252c80a38ed74479b3c9",
        "0aa655ba9f96c3015616de8e1be1d9b752a52bba77c2618d8241405e87968228",
        "0fc7c67b5db3ba4c05138ae21621c60b168655a5122d6ca0da410bd64c68f90d",
        "87bb5b622eacbe656ed30f9df59e78bd1640565c65b825a43170974c31df4e96",
        "8e8ff3526de919aeb1c0d93e15cf2c9e3b1fb9974a9135eb6d8479708eb004b6",
        "bcec7f24e791ccc2e5fad4e5827314a7b5827863ee6e47ad190188ecf0e1f6a5",
        "2de21e3bb1de339d2d0a8f9548b7cec1f41879274f21052f317fbfdcdaf4585c",
        "33d66e247572f940b856d8a6dc1687283459434339357d53cdb447f8451ed9e3",
        "703b9d3861cee801c2477f453a436ba55fbf813da720ce7fd7c50172f3f63b6d",
    )
    assert ledger_hashes == (
        "048e4c0d6d536db0629ba5a9f054d6002e851880cf48ef5cd730604e78168161",
        "ffaa38182adcab90721c38e48f8ec17b24928a25c98368ca1c287e9b30af3ba5",
        "98026e5567c1d5cbb3a4018db8d4c88c6bc015a7a0f1f58ad90cbae3acc9b50d",
        "59661b37f4faed37545115ad54683f8cc7fbfb5df8b0f3c34fc5bf5e9e37b50d",
        "6fad114f09f1afb890e4e37689b2c51232a4de525d5c8f81986abedb09dc4089",
        "a33a5e0114b8cf6031c2b98d699c58d3262468584a4cf7d4a077b195d5a5c677",
        "1086cedc05d4889e0d5ec5e12c55d27b8e2d421e42e9625424a8d5491c25085f",
        "08ceafd98885d2da9cb783c6507012cf3f56630795154174cb566fdcc6a84fb9",
        "f0d4f059d5bbf54530e3c6d9ade431ba5cf541837520bb2cd3ad3c1736210765",
        "845ac50137a778f07a0dee7c7257684886ad3111924b944ef1613ba16b73bc93",
        "037c5eca682effa3bde2a6b52d288c620fed27037782cce076ccc1ec4fee384a",
        "062c74ef1345466819efb6dc10b60bb080c71609eb0c430580e7498636c543ff",
        "fb640bc15f28fa76a4a84d4e27ce04fa8f9502f53bb4635536d3cfc9e8aa0832",
        "dded91e6b0a2e8e091489199bed0d762c2d9f85fc09a0bf0c3d529416262ec41",
        "4c989a6474c1859eaf277c43b5a8fb16d074a94364d4c330ccf691961e184661",
        "d0ed24d2f66c40c157a5b1e71c43fcc8972841fd145235932fd352c53114dfc8",
    )
    assert fixture_store.get_run(config.run_id).projection_hash == (
        "cf6e86f2fc47f672684dd2d14d1104ffa44707c6c1d7698699d5336f9f06d044"
    )

    fixture_store.close()
    with pytest.raises(
        ValueError,
        match="restricted to the internal temporary-store factory",
    ):
        PaperStore(database, historical_replay_only=True)
    ordinary_store = PaperStore(database, initialize=False)
    ordinary_engine = PaperEngine(ordinary_store, config)
    replayed = ordinary_engine.verify_input_replay()

    assert replayed.to_dict() == durable_projection.to_dict()
    assert tuple(event.event_hash for event in ordinary_store.iter_events(config.run_id)) == event_hashes
    assert (
        tuple(entry.entry_hash for entry in ordinary_store.iter_ledger_entries(config.run_id))
        == ledger_hashes
    )
    ordinary_store.close()
    del fixture_engine, fixture_store, ordinary_engine, ordinary_store
    gc.collect()
    temporary_directory.cleanup()


def test_append_rejects_model_invalid_cash_before_any_durable_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    beta = _strategy_config("beta", instrument=_BTC)
    config = _portfolio_config((alpha, beta))
    store = PaperStore(tmp_path / "paper.sqlite3")
    engine = PaperEngine(store, config)
    engine.start()
    market = _market("invalid-post-event-cash", _START + timedelta(seconds=1))
    before_run = store.get_run(config.run_id)
    before_projection = store.get_projection(config.run_id).to_dict()
    before_events = tuple(store.iter_events(config.run_id))
    before_inputs = tuple(store.iter_inputs(config.run_id))
    before_ledger = tuple(store.iter_ledger_entries(config.run_id))
    before_alerts = store.get_alerts(config.run_id)
    with sqlite3.connect(store.path) as connection:
        before_counts = tuple(
            int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            for table in (
                "paper_commits",
                "paper_projection_history",
            )
        )
    real_apply_event = paper_engine_module.apply_event

    def apply_with_invalid_cash(
        projection: PaperProjection,
        event: PaperEvent,
    ) -> PaperProjection:
        updated = real_apply_event(projection, event)
        updated.cash += Decimal("0.01")
        return updated

    with monkeypatch.context() as patch:
        patch.setattr(paper_engine_module, "apply_event", apply_with_invalid_cash)
        patch.setattr(paper_store_module, "apply_event", apply_with_invalid_cash)
        with pytest.raises(
            AppendConflictError,
            match="post-event projection violates PaperProjection invariants",
        ):
            engine.process_market(market)

    assert store.get_run(config.run_id) == before_run
    assert store.get_projection(config.run_id).to_dict() == before_projection
    assert tuple(store.iter_events(config.run_id)) == before_events
    assert tuple(store.iter_inputs(config.run_id)) == before_inputs
    assert tuple(store.iter_ledger_entries(config.run_id)) == before_ledger
    assert store.get_alerts(config.run_id) == before_alerts
    with sqlite3.connect(store.path) as connection:
        assert (
            tuple(
                int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
                for table in (
                    "paper_commits",
                    "paper_projection_history",
                )
            )
            == before_counts
        )
    assert store.contains_input(config.run_id, market.event_id) is False
    assert store.verify_integrity(config.run_id).ok


def test_append_rejects_cash_input_event_version_mismatch_before_any_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    beta = _strategy_config("beta", instrument=_BTC)
    config = _portfolio_config((alpha, beta))
    store = PaperStore(tmp_path / "cash-version-mismatch.sqlite3")
    engine = PaperEngine(store, config)
    engine.start()
    first = _market("cash-version-first", _START)
    observed = engine.process_market(first).projection
    strategy = OneShotStrategy(alpha, side=OrderSide.BUY, quantity="1")
    decision = strategy.decide(
        {_BTC: first},
        PaperStrategyView.from_projection(observed, alpha),
    )
    assert decision is not None
    engine.submit_decision(decision, {_BTC: first})
    second = _market(
        "cash-version-fill",
        _START + timedelta(seconds=1),
    )
    captured: dict[str, object] = {}
    real_append = store.append_atomic

    def capture_append(
        run_id: str,
        input_id: str,
        input_payload: object,
        events: object,
        ledger_entries: object,
        projection: object,
        *,
        alerts: object = (),
        expected_sequence: int | None = None,
    ) -> object:
        captured.update(
            {
                "alerts": alerts,
                "events": events,
                "expected_sequence": expected_sequence,
                "input_id": input_id,
                "input_payload": input_payload,
                "ledger_entries": ledger_entries,
                "projection": projection,
                "run_id": run_id,
            }
        )
        raise RuntimeError("capture valid v2 append")

    with monkeypatch.context() as patch:
        patch.setattr(store, "append_atomic", capture_append)
        with pytest.raises(RuntimeError, match="capture valid v2 append"):
            engine.process_market(second)

    input_payload = captured["input_payload"]
    events = captured["events"]
    ledger_entries = captured["ledger_entries"]
    alerts = captured["alerts"]
    expected_sequence = captured["expected_sequence"]
    assert isinstance(input_payload, dict)
    assert isinstance(events, tuple)
    assert isinstance(ledger_entries, list)
    assert isinstance(alerts, list)
    assert isinstance(expected_sequence, int)
    assert input_payload["cash_math_version"] == PAPER_CASH_MATH_VERSION
    assert any(
        event.event_type
        in {
            PaperEventType.ORDER_PARTIALLY_FILLED,
            PaperEventType.ORDER_FILLED,
        }
        for event in events
    )
    before_run = store.get_run(config.run_id)
    before_projection = store.get_projection(config.run_id).to_dict()
    before_events = tuple(store.iter_events(config.run_id))
    before_inputs = tuple(store.iter_inputs(config.run_id))
    before_ledger = tuple(store.iter_ledger_entries(config.run_id))
    before_alerts = store.get_alerts(config.run_id)
    with sqlite3.connect(store.path) as connection:
        before_counts = tuple(
            int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            for table in (
                "paper_commits",
                "paper_projection_history",
            )
        )

    for bad_version in (None, 1):
        bad_input = dict(input_payload)
        if bad_version is None:
            bad_input.pop("cash_math_version")
        else:
            bad_input["cash_math_version"] = bad_version
        with pytest.raises(
            AppendConflictError,
            match="cash event version differs from its durable input version",
        ):
            real_append(
                str(captured["run_id"]),
                str(captured["input_id"]),
                bad_input,
                events,
                ledger_entries,
                captured["projection"],
                alerts=alerts,
                expected_sequence=expected_sequence,
            )

    assert store.get_run(config.run_id) == before_run
    assert store.get_projection(config.run_id).to_dict() == before_projection
    assert tuple(store.iter_events(config.run_id)) == before_events
    assert tuple(store.iter_inputs(config.run_id)) == before_inputs
    assert tuple(store.iter_ledger_entries(config.run_id)) == before_ledger
    assert store.get_alerts(config.run_id) == before_alerts
    with sqlite3.connect(store.path) as connection:
        assert (
            tuple(
                int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
                for table in (
                    "paper_commits",
                    "paper_projection_history",
                )
            )
            == before_counts
        )
    assert store.verify_integrity(config.run_id).ok


def test_decimal_overflow_projection_is_latched_without_journal_append(
    tmp_path: Path,
) -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    beta = _strategy_config("beta", instrument=_BTC)
    config = _portfolio_config((alpha, beta))
    store = PaperStore(tmp_path / "overflow.sqlite3")
    engine = PaperEngine(store, config)
    engine.start()
    before_run = store.get_run(config.run_id)
    before_inputs = tuple(store.iter_inputs(config.run_id))
    before_events = tuple(store.iter_events(config.run_id))
    before_ledger = tuple(store.iter_ledger_entries(config.run_id))
    with sqlite3.connect(store.path) as connection:
        before_history_count = int(
            connection.execute("SELECT count(*) FROM paper_projection_history").fetchone()[0]
        )

    invalid = store.get_projection_payload(config.run_id)
    raw_strategies = invalid["strategy_projections"]
    assert isinstance(raw_strategies, dict)
    for raw_strategy in raw_strategies.values():
        assert isinstance(raw_strategy, dict)
        raw_strategy["cash"] = "9E+999999"
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE paper_projections SET payload_json=? WHERE run_id=?",
            (canonical_json(invalid), config.run_id),
        )

    readonly = store.inspect_integrity_readonly(config.run_id)
    assert any(
        issue.code == "CURRENT_PROJECTION_MODEL_INVALID" and issue.detail.startswith("Overflow:")
        for issue in readonly.issues
    )
    assert store.latch_unreadable_projection(config.run_id) is True

    after_run = store.get_run(config.run_id)
    alerts = tuple(
        alert for alert in store.get_alerts(config.run_id) if alert.code == "PAPER_STORE_INTEGRITY_FAILURE"
    )
    assert after_run.status == PaperState.MANUAL_REVIEW.value
    assert after_run.event_sequence == before_run.event_sequence
    assert after_run.event_head_hash == before_run.event_head_hash
    assert after_run.commit_sequence == before_run.commit_sequence
    assert after_run.commit_head_hash == before_run.commit_head_hash
    assert after_run.projection_revision == before_run.projection_revision
    assert after_run.projection_hash == before_run.projection_hash
    assert tuple(store.iter_inputs(config.run_id)) == before_inputs
    assert tuple(store.iter_events(config.run_id)) == before_events
    assert tuple(store.iter_ledger_entries(config.run_id)) == before_ledger
    assert len(alerts) == 1
    assert alerts[0].commit_sequence is None
    assert alerts[0].alert["issues"][0]["detail"].startswith("Overflow:")
    with sqlite3.connect(store.path) as connection:
        assert (
            int(connection.execute("SELECT count(*) FROM paper_projection_history").fetchone()[0])
            == before_history_count
        )


def test_round_trip_realized_pnl_fees_and_portfolio_totals_are_attributed(
    tmp_path: Path,
) -> None:
    long_config = _strategy_config("long", instrument=_BTC)
    short_config = _strategy_config("short", instrument=_BTC)
    config = _portfolio_config((short_config, long_config))
    engine = PaperEngine(PaperStore(tmp_path / "paper.sqlite3"), config)
    engine.start()
    runner = PortfolioRunner(
        engine,
        (
            RoundTripStrategy(short_config, side=OrderSide.SELL, quantity="0.5"),
            RoundTripStrategy(long_config, side=OrderSide.BUY, quantity="1"),
        ),
    )

    runner.process_frame({_BTC: _market("round-1", _START)}, processed_at=_START)
    runner.process_frame(
        {_BTC: _market("round-2", _START + timedelta(seconds=1), bid="110", ask="110")},
        processed_at=_START + timedelta(seconds=1),
    )
    runner.process_frame(
        {_BTC: _market("round-3", _START + timedelta(seconds=2), bid="105", ask="105")},
        processed_at=_START + timedelta(seconds=2),
    )
    projection = engine.projection()
    local = projection.strategy_projections

    assert local["long"].positions == {}
    assert local["short"].positions == {}
    assert local["long"].realized_pnl == Decimal("-5.215")
    assert local["short"].realized_pnl == Decimal("2.3925")
    assert local["long"].fees == Decimal("0.215")
    assert local["short"].fees == Decimal("0.1075")
    assert projection.realized_pnl == sum(item.realized_pnl for item in local.values())
    assert projection.fees == sum(item.fees for item in local.values())
    assert projection.cash - projection.initial_cash == sum(item.cash for item in local.values())
    assert engine.verify_input_replay().to_dict() == projection.to_dict()
    assert engine.reconcile(as_of=_START + timedelta(seconds=3)).projection.reconciled


def test_deterministic_ids_are_strategy_scoped_on_the_same_observation(
    tmp_path: Path,
) -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    beta = _strategy_config("beta", instrument=_BTC)
    config = _portfolio_config((beta, alpha))
    market = {_BTC: _market("same-observation", _START)}

    observed: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
    for name in ("first.sqlite3", "second.sqlite3"):
        engine = PaperEngine(PaperStore(tmp_path / name), config)
        engine.start()
        PortfolioRunner(
            engine,
            (
                OneShotStrategy(beta, side=OrderSide.BUY, quantity="0.1"),
                OneShotStrategy(alpha, side=OrderSide.BUY, quantity="0.1"),
            ),
        ).process_frame(market, processed_at=_START)
        projection = engine.projection()
        decisions = tuple(
            sorted(item.current_entry_decision_id or "" for item in projection.strategy_projections.values())
        )
        orders = tuple(sorted(projection.orders))
        assert len(set(decisions)) == 2
        assert len(set(orders)) == 2
        observed.append((decisions, orders))

    assert observed[0] == observed[1]


def test_multi_strategy_report_exposes_portfolio_and_bounded_attribution(
    tmp_path: Path,
) -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    beta = _strategy_config("beta", instrument=_BTC)
    config = _portfolio_config((alpha, beta))
    engine, _ = _run_two_frames(
        tmp_path,
        config,
        (
            OneShotStrategy(alpha, side=OrderSide.BUY, quantity="0.1"),
            OneShotStrategy(beta, side=OrderSide.SELL, quantity="0.05"),
        ),
        (
            {_BTC: _market("report-1", _START)},
            {_BTC: _market("report-2", _START + timedelta(seconds=1))},
        ),
    )

    report = build_paper_report(engine.store, config.run_id)

    assert report["schema_version"] == 2
    assert report["portfolio"]["gross_notional"] == "15"
    assert report["account"]["gross_notional"] == "5"
    assert report["identity"]["portfolio_id"] == config.portfolio_id
    assert report["identity"]["strategy_count"] == 2
    assert report["orders_enabled"] is False
    strategies = report["strategies"]
    assert isinstance(strategies, dict)
    assert list(strategies) == ["alpha", "beta"]
    assert strategies["alpha"]["accounting"]["positions"][0]["quantity"] == "0.1"
    assert strategies["beta"]["accounting"]["positions"][0]["quantity"] == "-0.05"
    assert strategies["alpha"]["identity"]["strategy_config_hash"] == alpha.strategy_config_hash
    assert strategies["alpha"]["risk"]["limits"] == alpha.risk.to_dict()


def test_runtime_restart_restores_both_strategies_and_replays_exactly(
    tmp_path: Path,
) -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    beta = _strategy_config("beta", instrument=_BTC)
    config = _portfolio_config((alpha, beta))
    database = tmp_path / "paper.sqlite3"
    first_market = _market("restart-1", _START + timedelta(seconds=1))
    first = _runtime(
        database,
        config,
        (RestoreHoldStrategy(beta), RestoreHoldStrategy(alpha)),
        QueuePublicSource([{_BTC: first_market}]),
        MutableClock(_START + timedelta(seconds=1)),
    )
    assert first.run_once().kind is PaperRuntimeStepKind.MARKET
    first.close()

    second_market = _market("restart-2", _START + timedelta(seconds=2))
    alpha_restarted = RestoreHoldStrategy(alpha)
    beta_restarted = RestoreHoldStrategy(beta)
    restarted = _runtime(
        database,
        config,
        (beta_restarted, alpha_restarted),
        QueuePublicSource([{_BTC: second_market}]),
        MutableClock(_START + timedelta(seconds=2)),
    )
    assert restarted.run_once().kind is PaperRuntimeStepKind.MARKET

    assert alpha_restarted.restore_calls == 1
    assert beta_restarted.restore_calls == 1
    assert alpha_restarted.restored_event_ids == [first_market.event_id]
    assert beta_restarted.restored_event_ids == [first_market.event_id]
    store = PaperStore(database, initialize=False)
    projection = store.get_projection(config.run_id)
    replay = replay_paper_run(store, config.run_id)
    assert replay.projection_hash == projection.canonical_hash
    assert replay.to_dict()["status"] == "REPLAY_EXACT"


def test_global_public_source_failure_pauses_portfolio_not_one_strategy(
    tmp_path: Path,
) -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    beta = _strategy_config("beta", instrument=_BTC)
    config = _portfolio_config((alpha, beta))
    database = tmp_path / "paper.sqlite3"

    def fail_poll() -> None:
        raise RuntimeError("synthetic private detail")

    runtime = _runtime(
        database,
        config,
        (RestoreHoldStrategy(alpha), RestoreHoldStrategy(beta)),
        QueuePublicSource([None], before_poll=fail_poll),
        MutableClock(_START + timedelta(seconds=1)),
    )

    with pytest.raises(PaperAdmissionError, match="public source failed closed"):
        runtime.run_once()

    store = PaperStore(database, initialize=False)
    projection = store.get_projection(config.run_id)
    assert projection.state is PaperState.PAUSED
    assert all(strategy.state is PaperState.FLAT for strategy in projection.strategy_projections.values())
    failures = list(store.iter_inputs(config.run_id, input_type="PUBLIC_SOURCE_FAILURE"))
    assert len(failures) == 1
    assert "synthetic private detail" not in str(failures[0].payload)
    assert replay_paper_run(store, config.run_id).projection_hash == projection.canonical_hash


def test_strategy_membership_and_budget_changes_create_new_run_identity() -> None:
    alpha = _strategy_config("alpha", instrument=_BTC)
    beta = _strategy_config("beta", instrument=_BTC)
    beta_smaller = _strategy_config("beta", instrument=_BTC, max_order_notional="99")

    one = _portfolio_config((alpha,))
    two = _portfolio_config((alpha, beta))
    changed_budget = _portfolio_config((alpha, beta_smaller))

    assert len({one.config_hash, two.config_hash, changed_budget.config_hash}) == 3
    assert len({one.run_id, two.run_id, changed_budget.run_id}) == 3


def test_legacy_single_strategy_durable_replay_stays_exact(tmp_path: Path) -> None:
    legacy = PaperRunConfig(
        strategy_name="legacy_fixture",
        strategy_hash="a" * 64,
        parameters={"version": 1},
        data_hash=_DATA_HASH,
        execution=PaperExecutionConfig(),
        risk=PaperRiskLimits(),
        seed=1,
        initial_cash=Decimal("1000"),
        validation_started_at=_START,
    )
    engine = PaperEngine(PaperStore(tmp_path / "legacy.sqlite3"), legacy)
    started = engine.start().projection

    assert started.strategy_projections == {}
    assert started.to_dict()["schema_version"] == 3
    assert engine.replay().to_dict() == started.to_dict()
    assert engine.verify_input_replay().to_dict() == started.to_dict()
