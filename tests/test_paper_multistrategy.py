from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

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
    OrderIntent,
    OrderSide,
    PaperExecutionConfig,
    PaperOrderType,
    PaperRiskLimits,
    PaperRunConfig,
    PaperState,
    PaperStrategyConfig,
    TimeInForce,
    deterministic_id,
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
from hyperlab.paper.store import PaperStore

_START = datetime(2026, 8, 18, 8, tzinfo=UTC)
_BTC = "HYPERLIQUID:BTC:perp"
_ETH = "HYPERLIQUID:ETH:perp"
_DATA_HASH = "d" * 64


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
) -> MarketEvent:
    return MarketEvent.create(
        received_at=at,
        instrument=instrument,
        bid_price=Decimal(bid),
        ask_price=Decimal(ask),
        bid_depth=Decimal("1000"),
        ask_depth=Decimal("1000"),
        source_sequence=int(deterministic_id("multistrategy_market", label)[:8], 16),
        capture_ordinal=0 if instrument == _BTC else 1,
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
