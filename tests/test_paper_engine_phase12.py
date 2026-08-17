from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from hyperlab.backtest.costs import CostRule, CostSchedule, SlippageModel
from hyperlab.backtest.execution import MakerFillModel
from hyperlab.paper import (
    DecisionAction,
    DecisionIntent,
    IdempotencyConflictError,
    IntegrityError,
    MarketEvent,
    OrderIntent,
    OrderSide,
    OrderStatus,
    PaperEngine,
    PaperEvent,
    PaperEventType,
    PaperExecutionConfig,
    PaperGateEvidence,
    PaperGateStatus,
    PaperOrderType,
    PaperProjection,
    PaperRiskLimits,
    PaperRunConfig,
    PaperState,
    PaperStore,
    RunConflictError,
    TimeInForce,
    deterministic_id,
    evaluate_order_risk,
    evaluate_paper_gate,
    keyed_uniform,
)
from hyperlab.paper.models import PaperOrder, legal_transition, require_transition
from hyperlab.paper.reducer import apply_event

_START = datetime(2026, 8, 13, 10, tzinfo=UTC)
_INSTRUMENT = "HYPERLIQUID:BTC:perp"
_HEDGE_INSTRUMENT = "HYPERLIQUID:ETH:perp"
_STRATEGY_HASH = "a" * 64
_DATA_HASH = "b" * 64
_EXECUTION_EVIDENCE_HASH = "c" * 64
_DATA_EVIDENCE_HASH = "d" * 64
_PREREQUISITES_EVIDENCE_HASH = "e" * 64


def _execution_config(
    *,
    maker_probability: float = 1.0,
    max_participation: float = 1.0,
    maker_timeout_ms: int = 1_000,
    ioc_fill_probability: Decimal = Decimal("1"),
    calibrated: bool = False,
) -> PaperExecutionConfig:
    if calibrated:
        maker = MakerFillModel(
            base_probability=maker_probability,
            participation_decay=0.0,
            calibration_id="public-l2-calibration-2026q2",
            calibration_status="CALIBRATED",
            calibration_evidence_hash=_EXECUTION_EVIDENCE_HASH,
        )
        return PaperExecutionConfig(
            maker_fill=maker,
            slippage=SlippageModel(max_participation=max_participation),
            cost_schedule=CostSchedule(
                rules=(
                    CostRule(
                        instrument="HYPERLIQUID:*:perp",
                        maker_fee_bps=1.0,
                        taker_fee_bps=2.0,
                        slippage=SlippageModel(max_participation=max_participation),
                        effective_from=_START,
                        source="public-versioned-fee-schedule-2026q2",
                    ),
                ),
                calibration_status="CALIBRATED",
                calibration_evidence_hash=_EXECUTION_EVIDENCE_HASH,
            ),
            maker_fee_bps=Decimal("1"),
            taker_fee_bps=Decimal("2"),
            ioc_fill_probability=ioc_fill_probability,
            maker_timeout_ms=maker_timeout_ms,
            calibration_status="CALIBRATED",
            calibration_evidence_hash=_EXECUTION_EVIDENCE_HASH,
            source="public-versioned-calibration-2026q2",
        )
    return PaperExecutionConfig(
        maker_fill=MakerFillModel(
            base_probability=maker_probability,
            participation_decay=0.0,
            calibration_id="deterministic-test-fixture",
            calibration_status="SYNTHETIC",
        ),
        slippage=SlippageModel(max_participation=max_participation),
        maker_fee_bps=Decimal("1"),
        taker_fee_bps=Decimal("2"),
        ioc_fill_probability=ioc_fill_probability,
        maker_timeout_ms=maker_timeout_ms,
        calibration_status="SYNTHETIC",
        source="deterministic-test-fixture",
    )


def _config(
    *,
    execution: PaperExecutionConfig | None = None,
    risk: PaperRiskLimits | None = None,
    calibrated: bool = False,
    parameters: dict[str, object] | None = None,
) -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="phase12_fixture",
        strategy_hash=_STRATEGY_HASH,
        parameters=parameters or {"entry_threshold": "1.5", "version": 1},
        data_hash=_DATA_HASH,
        execution=execution or _execution_config(calibrated=calibrated),
        risk=risk or PaperRiskLimits(),
        seed=42,
        initial_cash=Decimal("100000"),
        validation_started_at=_START,
        run_kind="VALIDATION" if calibrated else "DEMO",
        data_calibration_status="CALIBRATED" if calibrated else "SYNTHETIC",
        data_calibration_evidence_hash=_DATA_EVIDENCE_HASH if calibrated else None,
        data_source="public-versioned-feed-2026q2" if calibrated else "deterministic-test-fixture",
        economic_prerequisites_satisfied=calibrated,
        economic_prerequisites_evidence_hash=(_PREREQUISITES_EVIDENCE_HASH if calibrated else None),
        required_instruments=(_INSTRUMENT,) if calibrated else (),
    )


def _market(
    label: str,
    at: datetime,
    *,
    instrument: str = _INSTRUMENT,
    bid: str = "100",
    ask: str = "101",
    bid_depth: str = "100",
    ask_depth: str = "100",
    trade_price: str | None = None,
    trade_quantity: str | None = None,
    aggressor_side: OrderSide | None = None,
    stale: bool = False,
    gap: bool = False,
    tradable: bool = True,
) -> MarketEvent:
    return MarketEvent.create(
        received_at=at,
        instrument=instrument,
        bid_price=Decimal(bid),
        ask_price=Decimal(ask),
        bid_depth=Decimal(bid_depth),
        ask_depth=Decimal(ask_depth),
        source_sequence=int(deterministic_id("phase12_test_sequence", label)[:8], 16),
        trade_price=Decimal(trade_price) if trade_price is not None else None,
        trade_quantity=Decimal(trade_quantity) if trade_quantity is not None else None,
        aggressor_side=aggressor_side,
        stale=stale,
        gap=gap,
        tradable=tradable,
    )


def _decision(
    config: PaperRunConfig,
    market: MarketEvent,
    *,
    action: DecisionAction,
    side: OrderSide,
    quantity: str = "1",
    order_type: PaperOrderType = PaperOrderType.TAKER,
    time_in_force: TimeInForce = TimeInForce.GTC,
    limit_price: str | None = None,
    ordinal: int = 0,
    reduce_only: bool | None = None,
) -> DecisionIntent:
    decision_id = DecisionIntent.identifier(
        run_id=config.run_id,
        market_event_id=market.event_id,
        action=action,
        ordinal=ordinal,
    )
    order = OrderIntent.create(
        decision_id=decision_id,
        run_id=config.run_id,
        instrument=market.instrument,
        side=side,
        quantity=Decimal(quantity),
        order_type=order_type,
        time_in_force=time_in_force,
        created_at=market.received_at,
        ordinal=0,
        limit_price=Decimal(limit_price) if limit_price is not None else None,
        reduce_only=action is DecisionAction.EXIT if reduce_only is None else reduce_only,
    )
    return DecisionIntent(
        decision_id=decision_id,
        run_id=config.run_id,
        strategy_name=config.strategy_name,
        action=action,
        ordinal=ordinal,
        decided_at=market.received_at,
        received_at=market.received_at,
        market_event_id=market.event_id,
        observed_event_ids=(market.event_id,),
        orders=(order,),
    )


def _mixed_entry_decision(
    config: PaperRunConfig,
    market: MarketEvent,
) -> DecisionIntent:
    decision_id = DecisionIntent.identifier(
        run_id=config.run_id,
        market_event_id=market.event_id,
        action=DecisionAction.ENTRY,
        ordinal=0,
    )
    first_leg = OrderIntent.create(
        decision_id=decision_id,
        run_id=config.run_id,
        instrument=market.instrument,
        side=OrderSide.BUY,
        quantity=Decimal(1),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=market.received_at,
        ordinal=0,
        leg_number=1,
    )
    pending_entry = OrderIntent.create(
        decision_id=decision_id,
        run_id=config.run_id,
        instrument=market.instrument,
        side=OrderSide.BUY,
        quantity=Decimal("0.5"),
        order_type=PaperOrderType.MAKER,
        time_in_force=TimeInForce.GTC,
        created_at=market.received_at,
        ordinal=1,
        limit_price=Decimal(100),
        leg_number=2,
    )
    return DecisionIntent(
        decision_id=decision_id,
        run_id=config.run_id,
        strategy_name=config.strategy_name,
        action=DecisionAction.ENTRY,
        ordinal=0,
        decided_at=market.received_at,
        received_at=market.received_at,
        market_event_id=market.event_id,
        observed_event_ids=(market.event_id,),
        orders=(first_leg, pending_entry),
    )


def _started_engine(path: Path, config: PaperRunConfig) -> tuple[PaperStore, PaperEngine]:
    store = PaperStore(path)
    engine = PaperEngine(store, config)
    result = engine.start()
    assert result.projection.state is PaperState.FLAT
    return store, engine


def _event_types(store: PaperStore, run_id: str) -> list[PaperEventType]:
    return [stored.event.event_type for stored in store.get_events(run_id)]


def _run_complete_cycle(
    path: Path,
) -> tuple[PaperRunConfig, PaperStore, PaperProjection]:
    config = _config()
    store, engine = _started_engine(path, config)

    entry_market = _market("entry-decision", _START + timedelta(seconds=1))
    entry = _decision(
        config,
        entry_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    engine.submit_decision(entry, entry_market)
    engine.process_market(_market("entry-fill", _START + timedelta(seconds=2)))
    assert engine.projection().state is PaperState.HEDGED

    engine.post_funding(
        instrument=_INSTRUMENT,
        amount=Decimal("0.5"),
        occurred_at=_START + timedelta(seconds=3),
        source_event_id=deterministic_id("phase12_test_funding", "cycle-1"),
    )

    exit_market = _market("exit-decision", _START + timedelta(seconds=4))
    exit_decision = _decision(
        config,
        exit_market,
        action=DecisionAction.EXIT,
        side=OrderSide.SELL,
    )
    engine.submit_decision(exit_decision, exit_market)
    completed = engine.process_market(
        _market("exit-fill", _START + timedelta(seconds=5), bid="100", ask="101")
    )
    assert completed.projection.state is PaperState.FLAT
    assert completed.projection.completed_cycles == 1
    reconciled = engine.reconcile(as_of=_START + timedelta(seconds=6)).projection
    assert reconciled.reconciled is True
    return config, store, reconciled


def test_state_machine_declares_all_eleven_states_and_only_documented_transitions() -> None:
    expected = {
        PaperState.FLAT: {PaperState.ENTRY_PLANNED, PaperState.PAUSED, PaperState.MANUAL_REVIEW},
        PaperState.ENTRY_PLANNED: {
            PaperState.LEG_1_PENDING,
            PaperState.FLAT,
            PaperState.PAUSED,
            PaperState.MANUAL_REVIEW,
        },
        PaperState.LEG_1_PENDING: {
            PaperState.HEDGE_PENDING,
            PaperState.HEDGED,
            PaperState.FLAT,
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        },
        PaperState.HEDGE_PENDING: {
            PaperState.HEDGED,
            PaperState.FLAT,
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        },
        PaperState.HEDGED: {
            PaperState.EXIT_PLANNED,
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        },
        PaperState.EXIT_PLANNED: {
            PaperState.EXIT_PENDING,
            PaperState.HEDGED,
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        },
        PaperState.EXIT_PENDING: {
            PaperState.FLAT,
            PaperState.HEDGED,
            PaperState.PAUSED,
            PaperState.REDUCE_ONLY,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        },
        PaperState.PAUSED: {
            PaperState.FLAT,
            PaperState.LEG_1_PENDING,
            PaperState.HEDGE_PENDING,
            PaperState.HEDGED,
            PaperState.EXIT_PENDING,
            PaperState.REDUCE_ONLY,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        },
        PaperState.REDUCE_ONLY: {
            PaperState.EXIT_PLANNED,
            PaperState.EXIT_PENDING,
            PaperState.FLAT,
            PaperState.PAUSED,
            PaperState.MANUAL_REVIEW,
            PaperState.EMERGENCY_FLATTEN,
        },
        PaperState.MANUAL_REVIEW: set(),
        PaperState.EMERGENCY_FLATTEN: {
            PaperState.FLAT,
            PaperState.PAUSED,
            PaperState.MANUAL_REVIEW,
        },
    }

    assert set(PaperState) == set(expected)
    assert len(PaperState) == 11
    for source in PaperState:
        for target in PaperState:
            assert legal_transition(source, target) is (source is target or target in expected[source])
    with pytest.raises(ValueError, match="illegal paper state transition"):
        require_transition(PaperState.FLAT, PaperState.HEDGED)


def test_identifiers_config_hashes_and_seeded_draws_are_deterministic() -> None:
    first = deterministic_id(
        "phase12_contract",
        {"z": 2, "a": Decimal("1.00")},
        _START,
    )
    reordered = deterministic_id(
        "phase12_contract",
        {"a": Decimal("1.00"), "z": 2},
        _START,
    )
    config_a = _config(parameters={"z": 2, "a": "1.00"})
    config_b = _config(parameters={"a": "1.00", "z": 2})

    assert first == reordered
    assert len(first) == 64
    assert config_a.config_hash == config_b.config_hash
    assert config_a.run_id == config_b.run_id
    assert keyed_uniform(42, purpose="fill", identity=first) == keyed_uniform(
        42, purpose="fill", identity=first
    )
    assert keyed_uniform(42, purpose="fill", identity=first) != keyed_uniform(
        42, purpose="fill", identity=first, attempt=1
    )
    assert replace(config_a, seed=43).run_id != config_a.run_id


def test_v1_snapshot_identity_and_legacy_positional_run_kind_remain_compatible() -> None:
    legacy = replace(_config(), schema_version=1)
    payload = legacy.to_dict()

    assert payload["schema_version"] == 1
    assert "environment" not in payload
    assert legacy.config_hash == "6c5c6061764468f93131a9e54bc3d96edcbcb2f440d27456824e377550e25790"
    assert legacy.run_id == "8ffd2193e39e62474f0919c95fb19bc9e1352ca4273d81ceea06916120eb5d7f"
    assert PaperRunConfig.from_dict(payload) == legacy

    positional = PaperRunConfig(
        "phase12_fixture",
        _STRATEGY_HASH,
        {"version": 1},
        _DATA_HASH,
        _execution_config(),
        PaperRiskLimits(),
        42,
        Decimal("100000"),
        _START,
        "DEMO",
    )
    assert positional.run_kind == "DEMO"
    assert positional.schema_version == 2
    assert positional.environment == "PAPER"


def test_forged_decision_and_order_identifiers_are_rejected_at_the_boundary() -> None:
    config = _config()
    market = _market("identifier-boundary", _START + timedelta(seconds=1))
    decision = _decision(
        config,
        market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
        ordinal=7,
    )
    order = decision.orders[0]

    assert decision.ordinal == 7
    assert MarketEvent.from_dict(market.to_dict()) == market
    assert DecisionIntent.from_dict(decision.to_dict()) == decision
    assert OrderIntent.from_dict(order.to_dict()) == order
    with pytest.raises(ValueError, match="market event_id does not match"):
        replace(
            market,
            event_id=deterministic_id("phase12_forged_market_id", market.event_id),
        )
    with pytest.raises(ValueError, match="order_id does not match"):
        replace(
            order,
            order_id=deterministic_id("phase12_forged_order_id", order.order_id),
        )

    forged_decision_id = deterministic_id(
        "phase12_forged_decision_id",
        decision.decision_id,
    )
    forged_bound_order = OrderIntent.create(
        decision_id=forged_decision_id,
        run_id=config.run_id,
        instrument=market.instrument,
        side=OrderSide.BUY,
        quantity=Decimal(1),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=market.received_at,
        ordinal=0,
    )
    with pytest.raises(ValueError, match="decision_id does not match"):
        replace(
            decision,
            decision_id=forged_decision_id,
            orders=(forged_bound_order,),
        )


def test_configuration_rejects_a_divergent_execution_engine_build_hash() -> None:
    config = _config()
    divergent_hash = "f" * 64 if config.engine_build_hash != "f" * 64 else "e" * 64

    with pytest.raises(ValueError, match="different execution-engine build"):
        replace(config, engine_build_hash=divergent_hash)

    snapshot = config.to_dict()
    snapshot["engine_build_hash"] = divergent_hash
    with pytest.raises(ValueError, match="different execution-engine build"):
        PaperRunConfig.from_dict(snapshot)


def test_calibrated_data_rejects_placeholder_provenance() -> None:
    with pytest.raises(ValueError, match=r"(?i)(placeholder|source)"):
        PaperRunConfig(
            strategy_name="phase12_fixture",
            strategy_hash=_STRATEGY_HASH,
            parameters={"version": 1},
            data_hash=_DATA_HASH,
            execution=_execution_config(calibrated=True),
            risk=PaperRiskLimits(),
            seed=42,
            initial_cash=Decimal("100000"),
            validation_started_at=_START,
            run_kind="VALIDATION",
            data_calibration_status="CALIBRATED",
            data_calibration_evidence_hash=_DATA_EVIDENCE_HASH,
            data_source="research-placeholder",
            economic_prerequisites_satisfied=True,
            economic_prerequisites_evidence_hash=_PREREQUISITES_EVIDENCE_HASH,
        )


def test_validation_prerequisites_and_cycle_floor_require_durable_evidence() -> None:
    calibrated = _config(calibrated=True)

    with pytest.raises(ValueError, match="Gate B/C evidence hash"):
        replace(calibrated, economic_prerequisites_evidence_hash=None)
    with pytest.raises(ValueError, match="at least 30"):
        replace(calibrated, minimum_validation_cycles=29)


def test_technical_paper_run_is_explicitly_paper_and_does_not_require_gate_bc() -> None:
    technical = PaperRunConfig(
        strategy_name="phase12_fixture",
        strategy_hash=_STRATEGY_HASH,
        parameters={"version": 1},
        data_hash=_DATA_HASH,
        execution=_execution_config(calibrated=False),
        risk=PaperRiskLimits(),
        seed=42,
        initial_cash=Decimal("100000"),
        validation_started_at=_START,
        run_kind="TECHNICAL",
        data_calibration_status="UNCALIBRATED",
        data_source="public-normalized-source-v1",
        required_instruments=(_INSTRUMENT,),
        minimum_validation_cycles=1,
    )

    assert technical.environment == "PAPER"
    assert technical.schema_version == 2
    assert technical.economic_prerequisites_satisfied is False
    assert technical.economically_eligible is False
    assert PaperRunConfig.from_dict(technical.to_dict()) == technical
    with pytest.raises(ValueError, match="environment must be PAPER"):
        replace(technical, environment="TESTNET")


def test_missing_point_in_time_cost_rule_is_rejected_before_simulated_ack(
    tmp_path: Path,
) -> None:
    calibrated = _execution_config(calibrated=True)
    assert calibrated.cost_schedule is not None
    expired_schedule = CostSchedule(
        rules=(
            CostRule(
                instrument="HYPERLIQUID:*:perp",
                maker_fee_bps=1.0,
                taker_fee_bps=2.0,
                slippage=SlippageModel(),
                effective_from=_START - timedelta(days=2),
                effective_to=_START - timedelta(days=1),
                source="public-versioned-expired-fee-schedule",
            ),
        ),
        calibration_status="CALIBRATED",
        calibration_evidence_hash=_EXECUTION_EVIDENCE_HASH,
    )
    config = _config(
        execution=replace(calibrated, cost_schedule=expired_schedule),
        calibrated=True,
    )
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    market = _market("missing-cost-rule", _START + timedelta(seconds=1))
    decision = _decision(
        config,
        market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )

    result = engine.submit_decision(decision, market)

    assert result.projection.orders[decision.orders[0].order_id].status is OrderStatus.REJECTED
    types = _event_types(store, config.run_id)
    assert PaperEventType.RISK_REJECTED in types
    assert PaperEventType.ORDER_ACKED not in types
    assert any(
        "point-in-time cost schedule unavailable" in str(alert.alert.get("message"))
        for alert in store.get_alerts(config.run_id)
    )


def test_cost_rule_expiry_after_ack_is_terminal_audited_and_pauses(
    tmp_path: Path,
) -> None:
    calibrated = _execution_config(calibrated=True)
    expiring_schedule = CostSchedule(
        rules=(
            CostRule(
                instrument="HYPERLIQUID:*:perp",
                maker_fee_bps=1.0,
                taker_fee_bps=2.0,
                slippage=SlippageModel(),
                effective_from=_START,
                effective_to=_START + timedelta(seconds=2),
                source="public-versioned-expiring-fee-schedule",
            ),
        ),
        calibration_status="CALIBRATED",
        calibration_evidence_hash=_EXECUTION_EVIDENCE_HASH,
    )
    config = _config(
        execution=replace(calibrated, cost_schedule=expiring_schedule),
        calibrated=True,
    )
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    market = _market("expiring-cost-accept", _START + timedelta(seconds=1))
    decision = _decision(
        config,
        market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    accepted = engine.submit_decision(decision, market).projection
    assert accepted.orders[decision.orders[0].order_id].status is OrderStatus.ACKED

    result = engine.process_market(_market("expired-cost-match", _START + timedelta(seconds=3))).projection

    assert result.orders[decision.orders[0].order_id].status is OrderStatus.EXPIRED
    assert result.state is PaperState.PAUSED
    assert any(
        alert.code == "COST_SCHEDULE_GAP" and alert.severity == "CRITICAL"
        for alert in store.get_alerts(config.run_id)
    )
    assert engine.replay().to_dict() == result.to_dict()


def test_exact_zero_cash_survives_serialization_clone_restart_and_replay(
    tmp_path: Path,
) -> None:
    execution = replace(
        _execution_config(),
        maker_fee_bps=Decimal(0),
        taker_fee_bps=Decimal(0),
    )
    config = replace(_config(execution=execution), initial_cash=Decimal("101"))
    database = tmp_path / "paper.sqlite3"
    _store, engine = _started_engine(database, config)
    decision_market = _market("zero-cash-decision", _START + timedelta(seconds=1))
    decision = _decision(
        config,
        decision_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    engine.submit_decision(decision, decision_market)
    filled = engine.process_market(_market("zero-cash-fill", _START + timedelta(seconds=2))).projection

    assert filled.cash == Decimal(0)
    assert filled.to_dict()["cash"] == "0"
    assert PaperProjection.from_dict(filled.to_dict()).cash == Decimal(0)
    assert filled.clone().cash == Decimal(0)

    restarted = PaperEngine(PaperStore(database), config)
    restarted.start()
    assert restarted.projection().cash == Decimal(0)
    assert restarted.replay().cash == Decimal(0)


def test_happy_lifecycle_is_traceable_balanced_and_exactly_replayable(tmp_path: Path) -> None:
    config, store, projection = _run_complete_cycle(tmp_path / "first.sqlite3")
    types = _event_types(store, config.run_id)

    required_order = [
        PaperEventType.DECISION_RECORDED,
        PaperEventType.ORDER_PLANNED,
        PaperEventType.RISK_ACCEPTED,
        PaperEventType.ORDER_ACKED,
        PaperEventType.ORDER_FILLED,
    ]
    cursor = -1
    for event_type in required_order:
        cursor = types.index(event_type, cursor + 1)
    assert PaperEventType.FUNDING_POSTED in types
    assert PaperEventType.CYCLE_COMPLETED in types
    assert PaperEventType.RECONCILIATION_SUCCEEDED in types
    assert projection.positions == {}
    assert projection.cash == config.initial_cash + projection.realized_pnl
    assert projection.net_pnl == projection.realized_pnl
    assert projection.fees > 0

    transaction_totals: dict[str, Decimal] = defaultdict(Decimal)
    account_balances: dict[str, Decimal] = defaultdict(Decimal)
    for entry in store.get_ledger_entries(config.run_id):
        transaction_totals[entry.transaction_id] += entry.amount
        account_balances[entry.account] += entry.amount
    assert transaction_totals
    assert set(transaction_totals.values()) == {Decimal(0)}
    assert account_balances["asset:cash"] == projection.cash
    assert account_balances["expense:fees"] == projection.fees
    assert not {
        account
        for account, amount in account_balances.items()
        if account.startswith("asset:inventory:") and amount != 0
    }

    restarted = PaperEngine(PaperStore(tmp_path / "first.sqlite3"), config)
    restart_result = restarted.start()
    assert restart_result.append.idempotent is True
    assert restarted.replay().to_dict() == projection.to_dict()

    second_config, second_store, second_projection = _run_complete_cycle(tmp_path / "second.sqlite3")
    first_events = store.get_events(config.run_id)
    second_events = second_store.get_events(second_config.run_id)
    assert [event.event.unsigned_dict() for event in first_events] == [
        event.event.unsigned_dict() for event in second_events
    ]
    assert [event.event_hash for event in first_events] == [event.event_hash for event in second_events]
    assert projection.to_dict() == second_projection.to_dict()


def test_canonical_input_replay_exactly_reproduces_cycle_funding_and_reconciliation(
    tmp_path: Path,
) -> None:
    config, store, projection = _run_complete_cycle(tmp_path / "paper.sqlite3")
    input_types = [record.payload["input_type"] for record in store.get_inputs(config.run_id)]
    assert "STRATEGY_DECISION" in input_types
    assert "PUBLIC_MARKET_EVENT" in input_types
    assert "PUBLIC_FUNDING_SETTLEMENT" in input_types
    assert "RECONCILE" in input_types

    engine = PaperEngine(store, config)
    replayed = engine.verify_input_replay()
    assert replayed.to_dict() == projection.to_dict()
    assert replayed.last_event_hash == projection.last_event_hash


def test_timer_without_market_persists_critical_stale_pause_idempotently(
    tmp_path: Path,
) -> None:
    config = _config(risk=replace(PaperRiskLimits(), stale_after_seconds=5))
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    as_of = _START + timedelta(seconds=6)

    first = engine.process_timer(as_of=as_of)
    assert first.append.idempotent is False
    assert first.projection.state is PaperState.PAUSED
    assert _event_types(store, config.run_id).count(PaperEventType.TIMER_TICKED) == 1
    stale_alerts = [alert for alert in store.get_alerts(config.run_id) if alert.code == "STALE_MARKET_DATA"]
    assert len(stale_alerts) == 1
    assert stale_alerts[0].severity == "CRITICAL"

    durable = first.projection.to_dict()
    event_count = len(store.get_events(config.run_id))
    duplicate = engine.process_timer(as_of=as_of)
    assert duplicate.append.idempotent is True
    assert duplicate.projection.to_dict() == durable
    assert len(store.get_events(config.run_id)) == event_count
    assert _event_types(store, config.run_id).count(PaperEventType.TIMER_TICKED) == 1


def test_stale_timer_duplicate_alert_uses_indexed_lookup_not_lifetime_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(risk=replace(PaperRiskLimits(), stale_after_seconds=5))
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)

    first = engine.process_timer(as_of=_START + timedelta(seconds=6))
    assert first.projection.state is PaperState.PAUSED
    assert _event_types(store, config.run_id).count(PaperEventType.ALERT_RAISED) == 1

    def forbid_lifetime_alert_scan(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("duplicate alert detection must use the indexed point lookup")

    monkeypatch.setattr(store, "get_alerts", forbid_lifetime_alert_scan)
    second = engine.process_timer(as_of=_START + timedelta(seconds=7))

    assert second.append.idempotent is False
    assert second.projection.state is PaperState.PAUSED
    assert _event_types(store, config.run_id).count(PaperEventType.ALERT_RAISED) == 1
    assert _event_types(store, config.run_id).count(PaperEventType.TIMER_TICKED) == 2


def test_fresh_channel_cannot_mask_a_stale_instrument_with_exposure(
    tmp_path: Path,
) -> None:
    config = _config(risk=replace(PaperRiskLimits(), stale_after_seconds=5))
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    eth_market = _market(
        "per-channel-eth-entry",
        _START + timedelta(seconds=1),
        instrument=_HEDGE_INSTRUMENT,
    )
    eth_entry = _decision(
        config,
        eth_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    engine.submit_decision(eth_entry, eth_market)
    exposed = engine.process_market(
        _market(
            "per-channel-eth-fill",
            _START + timedelta(seconds=2),
            instrument=_HEDGE_INSTRUMENT,
        )
    ).projection
    assert exposed.positions == {_HEDGE_INSTRUMENT: Decimal(1)}

    engine.process_market(_market("per-channel-btc-fresh", _START + timedelta(seconds=10)))
    paused = engine.process_timer(as_of=_START + timedelta(seconds=10)).projection

    assert paused.state is PaperState.PAUSED
    assert paused.last_market_received_at_by_instrument[_INSTRUMENT] == (_START + timedelta(seconds=10))
    assert paused.last_market_received_at_by_instrument[_HEDGE_INSTRUMENT] == (_START + timedelta(seconds=2))
    assert any(
        alert.code == "STALE_MARKET_DATA" and _HEDGE_INSTRUMENT in str(alert.alert.get("message"))
        for alert in store.get_alerts(config.run_id)
    )


def test_funding_source_is_idempotent_and_divergent_amount_conflicts(
    tmp_path: Path,
) -> None:
    config = _config()
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    entry_market = _market("funding-entry", _START + timedelta(seconds=1))
    entry = _decision(
        config,
        entry_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    engine.submit_decision(entry, entry_market)
    engine.process_market(_market("funding-entry-fill", _START + timedelta(seconds=2)))
    source_event_id = deterministic_id("phase12_public_funding_source", "settlement-1")
    occurred_at = _START + timedelta(seconds=3)

    first = engine.post_funding(
        instrument=_INSTRUMENT,
        amount=Decimal("0.5"),
        occurred_at=occurred_at,
        source_event_id=source_event_id,
    )
    duplicate = engine.post_funding(
        instrument=_INSTRUMENT,
        amount=Decimal("0.5"),
        occurred_at=occurred_at,
        source_event_id=source_event_id,
    )
    assert duplicate.append.idempotent is True
    cash_after_first = first.projection.cash

    with pytest.raises(IdempotencyConflictError):
        engine.post_funding(
            instrument=_INSTRUMENT,
            amount=Decimal("0.6"),
            occurred_at=occurred_at,
            source_event_id=source_event_id,
        )
    assert store.get_projection(config.run_id).cash == cash_after_first
    assert _event_types(store, config.run_id).count(PaperEventType.FUNDING_POSTED) == 1


def test_detailed_funding_uses_position_before_settlement_and_ignores_bootstrap_history(
    tmp_path: Path,
) -> None:
    config = _config()
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    engine.reconcile(as_of=_START)
    entry_market = _market("detailed-funding-entry", _START + timedelta(seconds=1))
    entry = _decision(
        config,
        entry_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    engine.submit_decision(entry, entry_market)
    engine.process_market(_market("detailed-funding-entry-fill", _START + timedelta(seconds=2)))

    exit_at = _START + timedelta(seconds=3, milliseconds=500)
    exit_market = _market("detailed-funding-exit", exit_at)
    exit_decision = _decision(
        config,
        exit_market,
        action=DecisionAction.EXIT,
        side=OrderSide.SELL,
    )
    engine.submit_decision(exit_decision, exit_market)
    flat = engine.process_market(
        _market(
            "detailed-funding-exit-fill",
            _START + timedelta(seconds=3, milliseconds=600),
        )
    ).projection
    assert flat.positions == {}

    before_funding = flat.realized_pnl
    applied = engine.post_funding(
        instrument=_INSTRUMENT,
        amount=Decimal("-0.1"),
        occurred_at=_START + timedelta(seconds=3),
        source_event_id=deterministic_id("detailed_funding", "applied"),
        funding_rate=Decimal("0.001"),
        funding_interval_seconds=3600,
        rate_kind="hyperliquid-hourly-settlement",
        mark_price=Decimal("100"),
        source_mark_price=Decimal("100"),
        position_quantity=Decimal("1"),
        mark_source="PUBLIC_SETTLEMENT_MARK",
        source_observation_id="detailed-funding-applied",
        received_at=_START + timedelta(seconds=4),
        applicability="APPLIED",
        source_activation_cutoff=_START,
    ).projection

    assert applied.positions == {}
    assert applied.realized_pnl == before_funding - Decimal("0.1")

    ignored = engine.post_funding(
        instrument=_INSTRUMENT,
        amount=Decimal("0"),
        occurred_at=_START - timedelta(hours=1),
        source_event_id=deterministic_id("detailed_funding", "pre-activation"),
        funding_rate=Decimal("0.001"),
        funding_interval_seconds=3600,
        rate_kind="hyperliquid-hourly-settlement",
        position_quantity=Decimal("0"),
        mark_source="FLAT_NO_MARK",
        source_observation_id="detailed-funding-pre-activation",
        received_at=_START + timedelta(seconds=5),
        applicability="PRE_ACTIVATION_IGNORED",
        source_activation_cutoff=_START,
    ).projection

    assert ignored.realized_pnl == applied.realized_pnl
    ignored_input = store.get_input(
        config.run_id,
        deterministic_id(
            "paper_funding_input",
            config.run_id,
            deterministic_id("detailed_funding", "pre-activation"),
        ),
    )
    assert ignored_input is not None
    assert ignored_input.payload["applicability"] == "PRE_ACTIVATION_IGNORED"
    assert engine.verify_input_replay().to_dict() == ignored.to_dict()


def test_explicitly_stale_frame_does_not_change_mark_equity_or_peak(tmp_path: Path) -> None:
    config = _config()
    _store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    entry_market = _market("stale-mark-entry", _START + timedelta(seconds=1))
    entry = _decision(
        config,
        entry_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    engine.submit_decision(entry, entry_market)
    reliable = engine.process_market(_market("stale-mark-reliable", _START + timedelta(seconds=2))).projection
    before_mark = reliable.marks[_INSTRUMENT]
    before_equity = reliable.equity
    before_peak = reliable.peak_equity

    stale = engine.process_market(
        _market(
            "stale-mark-untrusted",
            _START + timedelta(seconds=3),
            bid="1",
            ask="2",
            stale=True,
        )
    ).projection
    assert stale.state is PaperState.PAUSED
    assert stale.marks[_INSTRUMENT] == before_mark
    assert stale.equity == before_equity
    assert stale.peak_equity == before_peak


def test_nontradable_health_frame_is_persisted_without_mark_ack_or_fill(
    tmp_path: Path,
) -> None:
    config = _config(
        execution=replace(_execution_config(), ack_latency_ms=500),
    )
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    entry_market = _market("health-entry", _START + timedelta(seconds=1))
    decision = _decision(
        config,
        entry_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    accepted = engine.submit_decision(decision, entry_market).projection
    order_id = decision.orders[0].order_id
    assert accepted.orders[order_id].status is OrderStatus.RISK_ACCEPTED

    health = _market(
        "connect-health-only",
        _START + timedelta(seconds=2),
        bid="1",
        ask="2",
        tradable=False,
    )
    observed = engine.process_market(health).projection

    assert store.get_input(config.run_id, health.event_id) is not None
    assert observed.orders[order_id].status is OrderStatus.RISK_ACCEPTED
    assert observed.positions == {}
    assert _INSTRUMENT not in observed.marks
    types = _event_types(store, config.run_id)
    assert PaperEventType.ORDER_ACKED not in types
    assert PaperEventType.ORDER_FILLED not in types
    assert PaperEventType.MARK_RECORDED not in types
    assert PaperEventType.PUBLIC_SOURCE_HEALTH_RECORDED in types

    recovered = engine.process_market(
        _market("health-next-tradable", _START + timedelta(seconds=3))
    ).projection
    assert recovered.orders[order_id].status is OrderStatus.FILLED
    assert recovered.positions[_INSTRUMENT] == Decimal("1")


def test_public_source_failure_is_critical_and_durable_while_already_paused(
    tmp_path: Path,
) -> None:
    config = _config()
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    paused = engine.pause(
        as_of=_START + timedelta(seconds=1),
        reason="operator inspection fixture",
        operator_artifact_hash="1" * 64,
    )
    assert paused.projection.state is PaperState.PAUSED

    failed = engine.pause(
        as_of=_START + timedelta(seconds=2),
        reason="terminal public source failure: TimeoutError",
        operator_artifact_hash="2" * 64,
        origin="PUBLIC_SOURCE_FAILURE",
    )

    assert failed.projection.state is PaperState.PAUSED
    failures = tuple(store.iter_inputs(config.run_id, input_type="PUBLIC_SOURCE_FAILURE"))
    assert len(failures) == 1
    source_alerts = [
        alert for alert in store.get_alerts(config.run_id) if alert.code == "PUBLIC_SOURCE_FAILURE"
    ]
    assert len(source_alerts) == 1
    assert source_alerts[0].severity == "CRITICAL"
    paused_transitions = [
        event
        for event in store.get_events(config.run_id)
        if event.event.event_type is PaperEventType.STATE_TRANSITIONED
        and event.event.payload.get("target") == PaperState.PAUSED.value
    ]
    assert len(paused_transitions) == 1


def test_risk_rejection_is_terminal_and_precedes_any_ack(tmp_path: Path) -> None:
    risk = PaperRiskLimits(max_order_notional=Decimal("50"))
    config = _config(risk=risk)
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    market = _market("oversized", _START + timedelta(seconds=1))
    decision = _decision(
        config,
        market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )

    result = engine.submit_decision(decision, market)
    types = _event_types(store, config.run_id)

    assert result.projection.state is PaperState.FLAT
    assert result.projection.orders[decision.orders[0].order_id].status is OrderStatus.REJECTED
    assert PaperEventType.RISK_REJECTED in types
    assert PaperEventType.ORDER_ACKED not in types
    assert PaperEventType.ORDER_REJECTED not in types
    assert any(alert.code == "RISK_REJECTED" for alert in store.get_alerts(config.run_id))


def test_quantity_and_concurrent_order_limits_are_enforced_before_ack(
    tmp_path: Path,
) -> None:
    quantity_limits = replace(
        PaperRiskLimits(),
        max_order_quantity=Decimal("0.5"),
        max_position_quantity=Decimal("0.75"),
    )
    quantity_config = _config(risk=quantity_limits)
    _quantity_store, quantity_engine = _started_engine(tmp_path / "quantity.sqlite3", quantity_config)
    market = _market("quantity-cap", _START + timedelta(seconds=1))
    oversized = _decision(
        quantity_config,
        market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
        quantity="1",
    )

    quantity_result = quantity_engine.submit_decision(oversized, market).projection

    assert quantity_result.orders[oversized.orders[0].order_id].status is OrderStatus.REJECTED

    concurrency_limits = replace(PaperRiskLimits(), max_concurrent_orders=1)
    concurrency_config = _config(risk=concurrency_limits)
    _concurrency_store, concurrency_engine = _started_engine(
        tmp_path / "concurrency.sqlite3", concurrency_config
    )
    mixed_market = _market("concurrent-cap", _START + timedelta(seconds=1))
    mixed = _mixed_entry_decision(concurrency_config, mixed_market)

    concurrent_result = concurrency_engine.submit_decision(mixed, mixed_market).projection

    statuses = [concurrent_result.orders[order.order_id].status for order in mixed.orders]
    assert sum(status is OrderStatus.REJECTED for status in statuses) == 1
    assert len(concurrent_result.active_orders) == 1


def test_true_reduce_only_order_can_escape_quantity_caps(tmp_path: Path) -> None:
    config = _config()
    _store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    entry_market = _market("quantity-reduce-entry", _START + timedelta(seconds=1))
    entry = _decision(
        config,
        entry_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
        quantity="1",
    )
    engine.submit_decision(entry, entry_market)
    opened = engine.process_market(_market("quantity-reduce-fill", _START + timedelta(seconds=2))).projection
    assert opened.positions[_INSTRUMENT] == Decimal(1)

    exit_market = _market("quantity-reduce-exit", _START + timedelta(seconds=3))
    exit_intent = _decision(
        config,
        exit_market,
        action=DecisionAction.EXIT,
        side=OrderSide.SELL,
        quantity="1",
    ).orders[0]
    strict_limits = replace(
        PaperRiskLimits(),
        max_order_quantity=Decimal("0.1"),
        max_position_quantity=Decimal("0.5"),
    )

    risk = evaluate_order_risk(opened, exit_intent, exit_market, strict_limits)

    assert risk.accepted is True
    assert risk.risk_reducing is True
    assert risk.projected_instrument_quantity > strict_limits.max_position_quantity


def test_decision_older_than_frozen_staleness_limit_is_rejected_before_ack(
    tmp_path: Path,
) -> None:
    config = _config(risk=replace(PaperRiskLimits(), stale_after_seconds=5))
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    observed = _market("too-old-observation", _START + timedelta(seconds=1))
    decided_at = observed.received_at + timedelta(seconds=6)
    decision_id = DecisionIntent.identifier(
        run_id=config.run_id,
        market_event_id=observed.event_id,
        action=DecisionAction.ENTRY,
        ordinal=0,
    )
    order = OrderIntent.create(
        decision_id=decision_id,
        run_id=config.run_id,
        instrument=_INSTRUMENT,
        side=OrderSide.BUY,
        quantity=Decimal(1),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=decided_at,
        ordinal=0,
    )
    decision = DecisionIntent(
        decision_id=decision_id,
        run_id=config.run_id,
        strategy_name=config.strategy_name,
        action=DecisionAction.ENTRY,
        ordinal=0,
        decided_at=decided_at,
        received_at=observed.received_at,
        market_event_id=observed.event_id,
        observed_event_ids=(observed.event_id,),
        orders=(order,),
    )

    result = engine.submit_decision(decision, observed)
    assert result.projection.state is PaperState.FLAT
    assert result.projection.orders[order.order_id].status is OrderStatus.REJECTED
    order_events = [
        stored.event.event_type
        for stored in store.get_events(config.run_id)
        if stored.event.payload.get("order_id") == order.order_id
    ]
    assert PaperEventType.RISK_REJECTED in order_events
    assert PaperEventType.ORDER_ACKED not in order_events


def test_concurrent_reduce_only_orders_cannot_overreserve_or_invert_a_position(
    tmp_path: Path,
) -> None:
    config = _config()
    _store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    entry_market = _market("reduce-reservation-entry", _START + timedelta(seconds=1))
    entry = _decision(
        config,
        entry_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    engine.submit_decision(entry, entry_market)
    opened = engine.process_market(
        _market("reduce-reservation-entry-fill", _START + timedelta(seconds=2))
    ).projection
    assert opened.positions == {_INSTRUMENT: Decimal(1)}
    assert opened.state is PaperState.HEDGED

    exit_market = _market("reduce-reservation-exit", _START + timedelta(seconds=3))
    decision_id = DecisionIntent.identifier(
        run_id=config.run_id,
        market_event_id=exit_market.event_id,
        action=DecisionAction.EXIT,
        ordinal=0,
    )
    exit_orders = tuple(
        OrderIntent.create(
            decision_id=decision_id,
            run_id=config.run_id,
            instrument=_INSTRUMENT,
            side=OrderSide.SELL,
            quantity=Decimal("0.75"),
            order_type=PaperOrderType.TAKER,
            time_in_force=TimeInForce.GTC,
            created_at=exit_market.received_at,
            ordinal=ordinal,
            reduce_only=True,
        )
        for ordinal in range(2)
    )
    exit_decision = DecisionIntent(
        decision_id=decision_id,
        run_id=config.run_id,
        strategy_name=config.strategy_name,
        action=DecisionAction.EXIT,
        ordinal=0,
        decided_at=exit_market.received_at,
        received_at=exit_market.received_at,
        market_event_id=exit_market.event_id,
        observed_event_ids=(exit_market.event_id,),
        orders=exit_orders,
    )
    planned = engine.submit_decision(exit_decision, exit_market).projection

    active_reduce_quantity = sum(
        (
            order.remaining_quantity
            for order in planned.active_orders
            if order.intent.reduce_only and order.intent.instrument == _INSTRUMENT
        ),
        Decimal(0),
    )
    assert active_reduce_quantity <= Decimal(1)
    assert sum(order.status is OrderStatus.REJECTED for order in planned.orders.values()) >= 1

    matched = engine.process_market(
        _market("reduce-reservation-fill", _START + timedelta(seconds=4))
    ).projection
    assert matched.positions.get(_INSTRUMENT, Decimal(0)) >= 0
    assert sum(
        (
            order.filled_quantity
            for order in matched.orders.values()
            if order.intent.reduce_only and order.intent.instrument == _INSTRUMENT
        ),
        Decimal(0),
    ) <= Decimal(1)


def test_reducer_rejects_a_reduce_only_fill_that_crosses_through_zero() -> None:
    config = _config()
    decision_id = deterministic_id("phase12_reduce_only_decision", config.run_id)
    intent = OrderIntent.create(
        decision_id=decision_id,
        run_id=config.run_id,
        instrument=_INSTRUMENT,
        side=OrderSide.SELL,
        quantity=Decimal(2),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=_START,
        ordinal=0,
        reduce_only=True,
    )
    projection = PaperProjection(
        run_id=config.run_id,
        config_hash=config.config_hash,
        initial_cash=config.initial_cash,
        state=PaperState.EXIT_PENDING,
        state_since=_START,
        positions={_INSTRUMENT: Decimal(1)},
        cost_basis={_INSTRUMENT: Decimal(100)},
        inventory_value={_INSTRUMENT: Decimal(100)},
        marks={_INSTRUMENT: Decimal(100)},
        orders={
            intent.order_id: PaperOrder(
                intent=intent,
                action=DecisionAction.EXIT,
                status=OrderStatus.ACKED,
                active_at=_START,
            )
        },
    )
    event = PaperEvent.create(
        run_id=config.run_id,
        event_type=PaperEventType.ORDER_FILLED,
        occurred_at=_START + timedelta(seconds=1),
        received_at=_START + timedelta(seconds=1),
        causation_id=deterministic_id("phase12_reduce_only_market", "cross-zero"),
        correlation_id=decision_id,
        payload={
            "fee": "0",
            "fill_id": deterministic_id("phase12_reduce_only_fill", "cross-zero"),
            "fill_price": "100",
            "fill_quantity": "2",
            "liquidity": "TAKER",
            "order_id": intent.order_id,
            "slippage_bps": "0",
        },
    )
    before = projection.to_dict()

    with pytest.raises(ValueError, match="reduce_only"):
        apply_event(projection, event)
    assert projection.to_dict() == before


def test_maker_non_fill_expires_without_an_implicit_ioc_chase(tmp_path: Path) -> None:
    execution = _execution_config(maker_probability=0.0, maker_timeout_ms=1_000)
    config = _config(execution=execution)
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    market = _market("maker-decision", _START + timedelta(seconds=1))
    decision = _decision(
        config,
        market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
        order_type=PaperOrderType.MAKER,
        limit_price="100",
    )
    engine.submit_decision(decision, market)
    engine.process_market(
        _market(
            "maker-eligible-trade",
            _START + timedelta(milliseconds=1_500),
            trade_price="100",
            trade_quantity="10",
            aggressor_side=OrderSide.SELL,
        )
    )
    result = engine.process_market(_market("maker-timeout", _START + timedelta(seconds=3)))

    order = result.projection.orders[decision.orders[0].order_id]
    assert result.projection.state is PaperState.FLAT
    assert order.status is OrderStatus.NO_FILL
    assert order.filled_quantity == 0
    assert all(item.intent.order_type is PaperOrderType.MAKER for item in result.projection.orders.values())
    types = _event_types(store, config.run_id)
    assert PaperEventType.ORDER_NO_FILL in types
    assert PaperEventType.ORDER_PARTIALLY_FILLED not in types
    assert PaperEventType.ORDER_FILLED not in types


def test_maker_miss_on_one_frame_can_fill_on_a_later_independent_draw(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, int]] = []

    def controlled_draw(
        seed: int,
        *,
        purpose: str,
        identity: str,
        attempt: int = 0,
    ) -> float:
        assert seed == 42
        calls.append((purpose, identity, attempt))
        return 0.9 if len(calls) == 1 else 0.1

    monkeypatch.setattr("hyperlab.paper.engine.keyed_uniform", controlled_draw)
    execution = _execution_config(maker_probability=0.5, maker_timeout_ms=10_000)
    config = _config(execution=execution)
    _store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    decision_market = _market("maker-frame-decision", _START + timedelta(seconds=1))
    decision = _decision(
        config,
        decision_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
        order_type=PaperOrderType.MAKER,
        limit_price="100",
    )
    engine.submit_decision(decision, decision_market)
    first = engine.process_market(
        _market(
            "maker-frame-miss",
            _START + timedelta(seconds=2),
            trade_price="100",
            trade_quantity="1",
            aggressor_side=OrderSide.SELL,
        )
    ).projection
    assert first.orders[decision.orders[0].order_id].status is OrderStatus.ACKED
    assert first.orders[decision.orders[0].order_id].filled_quantity == 0

    second = engine.process_market(
        _market(
            "maker-frame-fill",
            _START + timedelta(seconds=3),
            trade_price="100",
            trade_quantity="1",
            aggressor_side=OrderSide.SELL,
        )
    ).projection
    assert second.orders[decision.orders[0].order_id].status is OrderStatus.FILLED
    assert len(calls) == 2
    assert calls[0] != calls[1]


def test_terminal_order_from_an_old_cycle_cannot_block_the_next_cycle(
    tmp_path: Path,
) -> None:
    execution = _execution_config(maker_probability=0.0, maker_timeout_ms=1_000)
    config = _config(execution=execution)
    _store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    first_market = _market("old-cycle-maker", _START + timedelta(seconds=1))
    old_entry = _decision(
        config,
        first_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
        order_type=PaperOrderType.MAKER,
        limit_price="100",
    )
    engine.submit_decision(old_entry, first_market)
    expired = engine.process_market(_market("old-cycle-terminal", _START + timedelta(seconds=3))).projection
    assert expired.orders[old_entry.orders[0].order_id].status is OrderStatus.NO_FILL
    assert expired.state is PaperState.FLAT

    next_market = _market("new-cycle-entry", _START + timedelta(seconds=4))
    next_entry = _decision(
        config,
        next_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    engine.submit_decision(next_entry, next_market)
    filled = engine.process_market(_market("new-cycle-fill", _START + timedelta(seconds=5))).projection
    assert filled.orders[next_entry.orders[0].order_id].status is OrderStatus.FILLED
    assert filled.positions == {_INSTRUMENT: Decimal(1)}
    assert filled.state is PaperState.HEDGED


def test_one_public_frame_has_a_shared_depth_budget_across_orders(tmp_path: Path) -> None:
    config = _config()
    _store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    decision_market = _market(
        "shared-depth-decision",
        _START + timedelta(seconds=1),
        ask_depth="1.5",
    )
    decision_id = DecisionIntent.identifier(
        run_id=config.run_id,
        market_event_id=decision_market.event_id,
        action=DecisionAction.ENTRY,
        ordinal=0,
    )
    orders = tuple(
        OrderIntent.create(
            decision_id=decision_id,
            run_id=config.run_id,
            instrument=_INSTRUMENT,
            side=OrderSide.BUY,
            quantity=Decimal(1),
            order_type=PaperOrderType.TAKER,
            time_in_force=TimeInForce.GTC,
            created_at=decision_market.received_at,
            ordinal=ordinal,
        )
        for ordinal in range(2)
    )
    decision = DecisionIntent(
        decision_id=decision_id,
        run_id=config.run_id,
        strategy_name=config.strategy_name,
        action=DecisionAction.ENTRY,
        ordinal=0,
        decided_at=decision_market.received_at,
        received_at=decision_market.received_at,
        market_event_id=decision_market.event_id,
        observed_event_ids=(decision_market.event_id,),
        orders=orders,
    )
    engine.submit_decision(decision, decision_market)
    matched = engine.process_market(
        _market(
            "shared-depth-fill",
            _START + timedelta(seconds=2),
            ask_depth="1.5",
        )
    ).projection

    total_filled = sum(
        (matched.orders[order.order_id].filled_quantity for order in orders),
        Decimal(0),
    )
    assert total_filled <= Decimal("1.5")
    assert matched.positions.get(_INSTRUMENT, Decimal(0)) == total_filled


def test_delayed_second_leg_ioc_cannot_overhedge_a_partial_first_leg(
    tmp_path: Path,
) -> None:
    execution = replace(
        _execution_config(max_participation=1.0),
        leg_delay_ms=1_000,
    )
    config = _config(execution=execution)
    _store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    decision_at = _START + timedelta(seconds=1)
    first_market = _market(
        "partial-hedge-leg-1-decision",
        decision_at,
        instrument=_INSTRUMENT,
        ask_depth="0.5",
    )
    hedge_market = _market(
        "partial-hedge-leg-2-decision",
        decision_at,
        instrument=_HEDGE_INSTRUMENT,
    )
    decision_id = DecisionIntent.identifier(
        run_id=config.run_id,
        market_event_id=first_market.event_id,
        action=DecisionAction.ENTRY,
        ordinal=0,
    )
    group_id = "phase12-pair-hedge"
    first_leg = OrderIntent.create(
        decision_id=decision_id,
        run_id=config.run_id,
        instrument=_INSTRUMENT,
        side=OrderSide.BUY,
        quantity=Decimal(1),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.IOC,
        created_at=decision_at,
        ordinal=0,
        hedge_group_id=group_id,
        leg_number=1,
    )
    second_leg = OrderIntent.create(
        decision_id=decision_id,
        run_id=config.run_id,
        instrument=_HEDGE_INSTRUMENT,
        side=OrderSide.SELL,
        quantity=Decimal(1),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.IOC,
        created_at=decision_at,
        ordinal=1,
        hedge_group_id=group_id,
        leg_number=2,
    )
    decision = DecisionIntent(
        decision_id=decision_id,
        run_id=config.run_id,
        strategy_name=config.strategy_name,
        action=DecisionAction.ENTRY,
        ordinal=0,
        decided_at=decision_at,
        received_at=decision_at,
        market_event_id=first_market.event_id,
        observed_event_ids=(first_market.event_id, hedge_market.event_id),
        orders=(first_leg, second_leg),
    )
    engine.submit_decision(
        decision,
        {_INSTRUMENT: first_market, _HEDGE_INSTRUMENT: hedge_market},
    )
    after_first = engine.process_market(
        _market(
            "partial-hedge-leg-1-fill",
            _START + timedelta(seconds=2),
            instrument=_INSTRUMENT,
            ask_depth="0.5",
        )
    ).projection
    assert after_first.orders[first_leg.order_id].filled_quantity == Decimal("0.5")

    after_hedge = engine.process_market(
        _market(
            "partial-hedge-leg-2-fill",
            _START + timedelta(seconds=3),
            instrument=_HEDGE_INSTRUMENT,
            bid="50",
            ask="51",
            bid_depth="100",
            ask_depth="100",
        )
    ).projection
    first_filled = after_hedge.orders[first_leg.order_id].filled_quantity
    hedge_filled = after_hedge.orders[second_leg.order_id].filled_quantity
    assert hedge_filled <= first_filled
    assert abs(after_hedge.positions.get(_HEDGE_INSTRUMENT, Decimal(0))) <= first_filled


def test_paused_rejects_unacked_entries_but_accepts_a_true_reduce_only_exit(
    tmp_path: Path,
) -> None:
    execution = replace(
        _execution_config(),
        leg_delay_ms=5_000,
        cancel_latency_ms=2_000,
        maker_timeout_ms=10_000,
    )
    config = _config(execution=execution)
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    entry_market = _market("paused-entry", _START + timedelta(seconds=1))
    entry = _mixed_entry_decision(config, entry_market)
    pending_entry_id = entry.orders[1].order_id
    planned = engine.submit_decision(entry, entry_market).projection
    assert planned.orders[pending_entry_id].status is OrderStatus.RISK_ACCEPTED

    exposed = engine.process_market(
        _market("paused-first-leg-fill", _START + timedelta(seconds=2))
    ).projection
    assert exposed.positions == {_INSTRUMENT: Decimal(1)}
    assert exposed.state is PaperState.HEDGE_PENDING

    paused = engine.process_market(
        _market(
            "paused-stale-protection",
            _START + timedelta(seconds=3),
            stale=True,
        )
    ).projection
    assert paused.state is PaperState.PAUSED
    assert paused.orders[pending_entry_id].status is OrderStatus.REJECTED

    exit_market = _market("paused-exit", _START + timedelta(seconds=4))
    exit_decision = _decision(
        config,
        exit_market,
        action=DecisionAction.EXIT,
        side=OrderSide.SELL,
    )
    exit_planned = engine.submit_decision(exit_decision, exit_market).projection
    assert exit_planned.state is PaperState.EXIT_PENDING
    assert exit_planned.orders[exit_decision.orders[0].order_id].status is OrderStatus.ACKED

    flattened = engine.process_market(_market("paused-exit-fill", _START + timedelta(seconds=5))).projection
    assert flattened.state is PaperState.FLAT
    assert flattened.positions == {}

    after_old_ack_due = engine.process_market(
        _market(
            "paused-old-entry-must-stay-dead",
            _START + timedelta(seconds=7),
            trade_price="100",
            trade_quantity="1",
            aggressor_side=OrderSide.SELL,
        )
    ).projection
    assert after_old_ack_due.orders[pending_entry_id].status is OrderStatus.REJECTED
    assert after_old_ack_due.orders[pending_entry_id].filled_quantity == 0
    pending_events = [
        stored.event.event_type
        for stored in store.get_events(config.run_id)
        if stored.event.payload.get("order_id") == pending_entry_id
    ]
    assert PaperEventType.ORDER_ACKED not in pending_events
    assert PaperEventType.ORDER_FILLED not in pending_events
    assert PaperEventType.ORDER_PARTIALLY_FILLED not in pending_events


def test_reduce_only_protection_cancels_active_entries_then_allows_exit(
    tmp_path: Path,
) -> None:
    execution = replace(
        _execution_config(),
        cancel_latency_ms=2_000,
        maker_timeout_ms=10_000,
    )
    risk = replace(PaperRiskLimits(), max_drawdown=Decimal(10))
    config = _config(execution=execution, risk=risk)
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    entry_market = _market("reduce-only-entry", _START + timedelta(seconds=1))
    entry = _mixed_entry_decision(config, entry_market)
    pending_entry_id = entry.orders[1].order_id
    engine.submit_decision(entry, entry_market)
    exposed = engine.process_market(
        _market("reduce-only-first-leg-fill", _START + timedelta(seconds=2))
    ).projection
    assert exposed.orders[pending_entry_id].status is OrderStatus.ACKED
    assert exposed.positions == {_INSTRUMENT: Decimal(1)}

    protected = engine.process_market(
        _market(
            "reduce-only-drawdown",
            _START + timedelta(seconds=3),
            bid="49",
            ask="50",
        )
    ).projection
    assert protected.state is PaperState.REDUCE_ONLY
    assert protected.orders[pending_entry_id].status is OrderStatus.CANCEL_PENDING
    assert protected.orders[pending_entry_id].cancel_effective_at == _START + timedelta(seconds=5)

    exit_market = _market(
        "reduce-only-exit",
        _START + timedelta(seconds=4),
        bid="49",
        ask="50",
    )
    exit_decision = _decision(
        config,
        exit_market,
        action=DecisionAction.EXIT,
        side=OrderSide.SELL,
    )
    exit_planned = engine.submit_decision(exit_decision, exit_market).projection
    assert exit_planned.state is PaperState.REDUCE_ONLY
    assert exit_planned.orders[exit_decision.orders[0].order_id].status is OrderStatus.ACKED

    flattened = engine.process_market(
        _market(
            "reduce-only-cancel-effective-and-exit-fill",
            _START + timedelta(seconds=6),
            bid="49",
            ask="50",
            trade_price="100",
            trade_quantity="1",
            aggressor_side=OrderSide.SELL,
        )
    ).projection
    assert flattened.orders[pending_entry_id].status is OrderStatus.CANCELLED
    assert flattened.orders[pending_entry_id].filled_quantity == 0
    assert flattened.positions == {}
    assert flattened.state is PaperState.FLAT
    pending_fill_events = [
        stored.event.event_type
        for stored in store.get_events(config.run_id)
        if stored.event.payload.get("order_id") == pending_entry_id
        and stored.event.received_at >= _START + timedelta(seconds=5)
    ]
    assert PaperEventType.ORDER_FILLED not in pending_fill_events
    assert PaperEventType.ORDER_PARTIALLY_FILLED not in pending_fill_events


def test_public_mark_jump_breaches_cap_before_pending_entry_can_increase_risk(
    tmp_path: Path,
) -> None:
    execution = replace(
        _execution_config(),
        cancel_latency_ms=2_000,
        maker_timeout_ms=10_000,
    )
    risk = replace(
        PaperRiskLimits(),
        max_gross_notional=Decimal("175"),
        max_net_notional=Decimal("175"),
        max_instrument_notional=Decimal("175"),
    )
    config = _config(execution=execution, risk=risk)
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    entry_market = _market("marked-cap-entry", _START + timedelta(seconds=1))
    entry = _mixed_entry_decision(config, entry_market)
    pending_entry_id = entry.orders[1].order_id
    engine.submit_decision(entry, entry_market)
    exposed = engine.process_market(
        _market("marked-cap-first-leg-fill", _START + timedelta(seconds=2))
    ).projection
    assert exposed.positions == {_INSTRUMENT: Decimal(1)}
    assert exposed.orders[pending_entry_id].status is OrderStatus.ACKED

    protected = engine.process_market(
        _market(
            "marked-cap-jump",
            _START + timedelta(seconds=3),
            bid="200",
            ask="201",
            trade_price="100",
            trade_quantity="1",
            aggressor_side=OrderSide.SELL,
        )
    ).projection

    assert protected.state is PaperState.REDUCE_ONLY
    assert protected.positions == {_INSTRUMENT: Decimal(1)}
    assert protected.orders[pending_entry_id].status is OrderStatus.CANCEL_PENDING
    assert protected.orders[pending_entry_id].filled_quantity == 0
    assert any(alert.code == "MARKED_GROSS_NOTIONAL_LIMIT" for alert in store.get_alerts(config.run_id))
    assert engine.replay().to_dict() == protected.to_dict()


def test_entry_cannot_fill_in_protective_state_during_cancel_latency(
    tmp_path: Path,
) -> None:
    execution = replace(
        _execution_config(),
        cancel_latency_ms=2_000,
        maker_timeout_ms=10_000,
    )
    risk = replace(PaperRiskLimits(), max_drawdown=Decimal(10))
    config = _config(execution=execution, risk=risk)
    _store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    entry_market = _market("cancel-latency-entry", _START + timedelta(seconds=1))
    entry = _mixed_entry_decision(config, entry_market)
    pending_entry_id = entry.orders[1].order_id
    engine.submit_decision(entry, entry_market)
    engine.process_market(_market("cancel-latency-first-leg-fill", _START + timedelta(seconds=2)))
    protected = engine.process_market(
        _market(
            "cancel-latency-drawdown",
            _START + timedelta(seconds=3),
            bid="49",
            ask="50",
        )
    ).projection
    assert protected.orders[pending_entry_id].status is OrderStatus.CANCEL_PENDING

    during_latency = engine.process_market(
        _market(
            "cancel-latency-eligible-fill",
            _START + timedelta(seconds=4),
            bid="49",
            ask="50",
            trade_price="100",
            trade_quantity="0.5",
            aggressor_side=OrderSide.SELL,
        )
    ).projection
    assert during_latency.orders[pending_entry_id].status is OrderStatus.CANCEL_PENDING
    assert during_latency.orders[pending_entry_id].filled_quantity == 0

    after_effective = engine.process_market(
        _market(
            "cancel-latency-after-effective",
            _START + timedelta(seconds=6),
            bid="49",
            ask="50",
            trade_price="100",
            trade_quantity="0.5",
            aggressor_side=OrderSide.SELL,
        )
    ).projection
    assert after_effective.orders[pending_entry_id].status is OrderStatus.CANCELLED
    assert after_effective.orders[pending_entry_id].filled_quantity == 0


def test_partial_ioc_times_out_unhedged_then_emergency_flattens(tmp_path: Path) -> None:
    execution = _execution_config(max_participation=0.25)
    risk = PaperRiskLimits(unhedged_timeout_seconds=2)
    config = _config(execution=execution, risk=risk)
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    entry_market = _market(
        "partial-entry-decision",
        _START + timedelta(seconds=1),
        bid_depth="2",
        ask_depth="2",
    )
    entry = _decision(
        config,
        entry_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
        quantity="2",
        time_in_force=TimeInForce.IOC,
    )
    engine.submit_decision(entry, entry_market)
    partial = engine.process_market(
        _market(
            "partial-entry-fill",
            _START + timedelta(seconds=2),
            bid_depth="2",
            ask_depth="2",
        )
    )

    entry_order = partial.projection.orders[entry.orders[0].order_id]
    assert partial.projection.state is PaperState.HEDGE_PENDING
    assert entry_order.status is OrderStatus.EXPIRED
    assert entry_order.filled_quantity == Decimal("0.5")
    assert partial.projection.positions == {_INSTRUMENT: Decimal("0.5")}

    paused = engine.process_market(
        _market(
            "partial-entry-source-gap",
            _START + timedelta(seconds=3),
            gap=True,
        )
    )
    assert paused.projection.state is PaperState.PAUSED
    assert paused.projection.suspended_from is PaperState.HEDGE_PENDING

    timed_out = engine.process_timer(as_of=_START + timedelta(seconds=6))
    assert timed_out.projection.state is PaperState.EMERGENCY_FLATTEN
    assert any(alert.code == "UNHEDGED_TIMEOUT" for alert in store.get_alerts(config.run_id))

    gap_while_emergency = engine.process_market(
        _market(
            "emergency-source-gap",
            _START + timedelta(seconds=6, milliseconds=100),
            gap=True,
        )
    )
    assert gap_while_emergency.projection.state is PaperState.EMERGENCY_FLATTEN

    emergency_market = _market(
        "emergency-decision",
        _START + timedelta(seconds=6, milliseconds=200),
        bid_depth="10",
        ask_depth="10",
    )
    planned = engine.emergency_flatten(
        {_INSTRUMENT: emergency_market},
        decided_at=emergency_market.received_at,
        reason="unhedged timeout fixture",
    )
    emergency_orders = [
        order for order in planned.projection.orders.values() if order.action is DecisionAction.EXIT
    ]
    assert len(emergency_orders) == 1
    assert emergency_orders[0].intent.reduce_only is True
    assert emergency_orders[0].intent.time_in_force is TimeInForce.IOC

    flattened = engine.process_market(
        _market(
            "emergency-fill",
            _START + timedelta(seconds=7),
            bid_depth="10",
            ask_depth="10",
        )
    )
    assert flattened.projection.state is PaperState.FLAT
    assert flattened.projection.positions == {}
    assert flattened.projection.completed_cycles == 1
    types = _event_types(store, config.run_id)
    assert PaperEventType.ORDER_PARTIALLY_FILLED in types
    assert PaperEventType.ORDER_EXPIRED in types


def test_duplicate_inputs_are_noops_but_divergent_reuse_fails_closed(tmp_path: Path) -> None:
    config = _config()
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    market = _market("idempotent-decision", _START + timedelta(seconds=1))
    decision = _decision(
        config,
        market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    first = engine.submit_decision(decision, market)
    duplicate = engine.submit_decision(decision, market)

    assert first.append.idempotent is False
    assert duplicate.append.idempotent is True
    assert _event_types(store, config.run_id).count(PaperEventType.DECISION_RECORDED) == 1

    divergent_market = replace(
        market,
        bid_price=Decimal("99"),
        ask_price=Decimal("102"),
    )
    with pytest.raises(IdempotencyConflictError, match="different payload"):
        engine.submit_decision(decision, divergent_market)
    assert store.get_run(config.run_id).status == PaperState.MANUAL_REVIEW.value
    assert any(alert.code == "INBOX_IDEMPOTENCY_CONFLICT" for alert in store.get_alerts(config.run_id))


def test_runtime_duplicate_lookup_uses_one_targeted_input_not_the_full_inbox(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    market = _market("targeted-duplicate", _START + timedelta(seconds=1))
    decision = _decision(
        config,
        market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    targeted_calls: list[tuple[str, str]] = []
    get_input = store.get_input

    def targeted_get_input(run_id: str, input_id: str) -> object:
        targeted_calls.append((run_id, input_id))
        return get_input(run_id, input_id)

    def forbidden_full_inbox(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("runtime idempotency must not materialize the full inbox")

    monkeypatch.setattr(store, "get_input", targeted_get_input)
    monkeypatch.setattr(store, "get_inputs", forbidden_full_inbox)

    first = engine.submit_decision(decision, market)
    duplicate = engine.submit_decision(decision, market)

    assert first.append.idempotent is False
    assert duplicate.append.idempotent is True
    assert targeted_calls == [
        (config.run_id, decision.decision_id),
        (config.run_id, decision.decision_id),
    ]


def test_targeted_input_lookup_sql_shape_is_constant_after_inbox_growth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    read_connection = store._read_connection

    def traced_lookup(input_id: str) -> tuple[object, list[str]]:
        statements: list[str] = []

        def traced_read_connection() -> sqlite3.Connection:
            connection = read_connection()
            connection.set_trace_callback(statements.append)
            return connection

        monkeypatch.setattr(store, "_read_connection", traced_read_connection)
        try:
            record = store.get_input(config.run_id, input_id)
        finally:
            monkeypatch.setattr(store, "_read_connection", read_connection)
        return record, statements

    run_start_id = deterministic_id("paper_input_run_started", config.run_id)
    initial_record, initial_trace = traced_lookup(run_start_id)
    assert initial_record is not None

    latest_market: MarketEvent | None = None
    for ordinal in range(24):
        latest_market = _market(
            f"inbox-growth-{ordinal}",
            _START + timedelta(seconds=ordinal + 1),
        )
        engine.process_market(latest_market)
    assert latest_market is not None
    grown_record, grown_trace = traced_lookup(latest_market.event_id)
    assert grown_record is not None

    for trace in (initial_trace, grown_trace):
        inbox_queries = [statement.upper() for statement in trace if "PAPER_INBOX" in statement.upper()]
        assert len(inbox_queries) == 1
        assert "WHERE RUN_ID=" in inbox_queries[0]
        assert "AND INPUT_ID=" in inbox_queries[0]
        assert "ORDER BY" not in inbox_queries[0]


def test_append_hot_path_uses_incremental_guard_without_a_full_history_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    collect_append_head_issues = store._collect_append_head_issues
    incremental_guard_calls = 0

    def forbidden_full_scan(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("append hot path must not run the full history audit")

    def counted_incremental_guard(*args: object, **kwargs: object) -> object:
        nonlocal incremental_guard_calls
        incremental_guard_calls += 1
        return collect_append_head_issues(*args, **kwargs)

    monkeypatch.setattr(store, "_collect_integrity_issues", forbidden_full_scan)
    monkeypatch.setattr(store, "_collect_append_head_issues", counted_incremental_guard)

    result = engine.process_market(_market("bounded-append-guard", _START + timedelta(seconds=1)))

    assert result.append.appended_event_count > 0
    assert result.append.idempotent is False
    assert incremental_guard_calls == 1


@pytest.mark.parametrize(
    ("corruption", "expected_issue"),
    [
        ("event_head", "EVENT_HEAD_MISMATCH"),
        ("last_commit", "COMMIT_HASH"),
        ("projection", "PROJECTION_HASH"),
    ],
)
def test_incremental_append_guard_detects_corrupt_durable_heads_without_full_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
    expected_issue: str,
) -> None:
    database = tmp_path / f"{corruption}.sqlite3"
    config = _config()
    store, engine = _started_engine(database, config)
    with sqlite3.connect(database) as connection:
        if corruption == "event_head":
            connection.execute(
                "UPDATE paper_runs SET event_head_hash = ? WHERE run_id = ?",
                ("0" * 64, config.run_id),
            )
        elif corruption == "last_commit":
            connection.execute("DROP TRIGGER paper_commits_no_update")
            connection.execute(
                """
                UPDATE paper_commits SET commit_hash = ?
                WHERE run_id = ? AND commit_sequence = (
                    SELECT MAX(commit_sequence) FROM paper_commits WHERE run_id = ?
                )
                """,
                ("1" * 64, config.run_id, config.run_id),
            )
        else:
            connection.execute(
                "UPDATE paper_projections SET projection_hash = ? WHERE run_id = ?",
                ("2" * 64, config.run_id),
            )

    def forbidden_full_scan(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("corruption guard must remain bounded to durable heads")

    monkeypatch.setattr(store, "_collect_integrity_issues", forbidden_full_scan)
    with pytest.raises(IntegrityError) as captured:
        engine.process_market(
            _market(
                f"append-after-{corruption}-corruption",
                _START + timedelta(seconds=1),
            )
        )

    issue_codes = {issue.code for issue in captured.value.report.issues}
    assert expected_issue in issue_codes
    assert store.get_run(config.run_id).status == PaperState.MANUAL_REVIEW.value
    assert any(alert.code == "PAPER_STORE_INTEGRITY_FAILURE" for alert in store.get_alerts(config.run_id))


def test_config_drift_creates_a_new_identity_and_cannot_rewrite_a_run(tmp_path: Path) -> None:
    config = _config()
    store, _engine = _started_engine(tmp_path / "paper.sqlite3", config)
    drifted = replace(config, seed=config.seed + 1)

    assert drifted.config_hash != config.config_hash
    assert drifted.run_id != config.run_id
    with pytest.raises(RunConflictError, match="different immutable configuration"):
        store.create_run(
            config.run_id,
            drifted,
            config_hash=drifted.config_hash,
            seed=drifted.seed,
        )
    assert store.get_run(config.run_id).status == PaperState.MANUAL_REVIEW.value


@pytest.mark.parametrize(
    ("fault_stage", "durable_after_crash"),
    [
        ("after_input", False),
        ("after_events", False),
        ("after_ledger", False),
        ("before_commit", False),
        ("after_commit", True),
    ],
)
def test_crash_boundaries_restart_without_loss_or_duplicate_effects(
    tmp_path: Path,
    fault_stage: str,
    durable_after_crash: bool,
) -> None:
    database = tmp_path / f"{fault_stage}.sqlite3"
    config = _config()
    _store, _engine = _started_engine(database, config)
    market = _market(f"crash-{fault_stage}", _START + timedelta(seconds=1))
    decision = _decision(
        config,
        market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )

    def inject(stage: str) -> None:
        if stage == fault_stage:
            raise RuntimeError(f"simulated crash at {stage}")

    crashing = PaperEngine(PaperStore(database, fault_injector=inject), config)
    with pytest.raises(RuntimeError, match="simulated crash"):
        crashing.submit_decision(decision, market)

    restarted_store = PaperStore(database)
    restarted = PaperEngine(restarted_store, config)
    restarted.start()
    assert restarted_store.contains_input(config.run_id, decision.decision_id) is durable_after_crash

    recovered = restarted.submit_decision(decision, market)
    assert recovered.append.idempotent is durable_after_crash
    assert _event_types(restarted_store, config.run_id).count(PaperEventType.DECISION_RECORDED) == 1
    assert restarted.replay().to_dict() == restarted.projection().to_dict()
    assert restarted_store.verify_integrity(config.run_id).ok is True


def test_event_tampering_latches_manual_review_and_blocks_restart(tmp_path: Path) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _config()
    store, _engine = _started_engine(database, config)
    with sqlite3.connect(database) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE paper_events SET payload_json = ? WHERE run_id = ? AND sequence = 1",
                ("{}", config.run_id),
            )
        connection.rollback()
        # Emulate out-of-band file tampering by an actor able to alter the SQLite schema.
        connection.execute("DROP TRIGGER paper_events_no_update")
        connection.execute(
            "UPDATE paper_events SET payload_json = ? WHERE run_id = ? AND sequence = 1",
            ("{}", config.run_id),
        )

    with pytest.raises(IntegrityError) as captured:
        store.verify_integrity(config.run_id)
    assert captured.value.report.ok is False
    assert store.get_run(config.run_id).status == PaperState.MANUAL_REVIEW.value
    assert any(alert.code == "PAPER_STORE_INTEGRITY_FAILURE" for alert in store.get_alerts(config.run_id))
    with pytest.raises(IntegrityError):
        PaperEngine(PaperStore(database), config).start()


def _complete_gate_cycle(
    engine: PaperEngine,
    config: PaperRunConfig,
    *,
    cycle_ordinal: int,
) -> None:
    base_seconds = 1 + cycle_ordinal * 4
    entry_market = _market(
        f"gate-entry-{cycle_ordinal}",
        _START + timedelta(seconds=base_seconds),
    )
    entry = _decision(
        config,
        entry_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    engine.submit_decision(entry, entry_market)
    engine.process_market(
        _market(
            f"gate-entry-fill-{cycle_ordinal}",
            _START + timedelta(seconds=base_seconds + 1),
        )
    )
    exit_market = _market(
        f"gate-exit-{cycle_ordinal}",
        _START + timedelta(seconds=base_seconds + 2),
    )
    exit_decision = _decision(
        config,
        exit_market,
        action=DecisionAction.EXIT,
        side=OrderSide.SELL,
    )
    engine.submit_decision(exit_decision, exit_market)
    completed = engine.process_market(
        _market(
            f"gate-exit-fill-{cycle_ordinal}",
            _START + timedelta(seconds=base_seconds + 3),
        )
    ).projection
    assert completed.completed_cycles == cycle_ordinal + 1


def _persist_gate_evidence(
    engine: PaperEngine,
    config: PaperRunConfig,
    *,
    as_of: datetime,
    stressed_net_pnl: Decimal = Decimal(1),
) -> None:
    engine.process_market(_market(f"gate-terminal-{as_of.isoformat()}", as_of))
    for ordinal, exercise in enumerate(("RESTART", "DISCONNECT", "PARTIAL_FILL", "CRASH_RECOVERY")):
        engine.record_resilience_exercise(
            exercise=exercise,
            artifact_hash=deterministic_id("phase12_gate_exercise", exercise, ordinal),
            exercised_at=as_of,
        )
    engine.record_stress_result(
        artifact_hash=deterministic_id("phase12_gate_stress", stressed_net_pnl),
        stressed_net_pnl=stressed_net_pnl,
        evaluated_at=as_of,
    )
    engine.record_observation_coverage(
        artifact_hash=deterministic_id("phase12_gate_coverage", as_of),
        window_start=config.validation_started_at,
        window_end=as_of,
        continuous=True,
        recorded_at=as_of,
    )


def _gate_run(
    path: Path,
    *,
    days: int = 42,
    minimum_cycles: int = 30,
    completed_cycles: int = 30,
    stressed_net_pnl: Decimal = Decimal(1),
    recent_incident: bool = False,
) -> tuple[PaperRunConfig, PaperStore, PaperGateEvidence]:
    config = replace(
        _config(calibrated=True),
        minimum_validation_cycles=minimum_cycles,
    )
    store, engine = _started_engine(path, config)
    for cycle_ordinal in range(completed_cycles):
        _complete_gate_cycle(engine, config, cycle_ordinal=cycle_ordinal)
    as_of = config.validation_started_at + timedelta(days=days)
    if recent_incident:
        incident_at = as_of - timedelta(days=13, hours=23)
        engine.process_market(_market("gate-recent-incident", incident_at, stale=True))
    _persist_gate_evidence(
        engine,
        config,
        as_of=as_of,
        stressed_net_pnl=stressed_net_pnl,
    )
    return config, store, PaperGateEvidence(as_of=as_of)


def test_gate_d_is_store_bound_enforces_thresholds_and_blocks_demo(
    tmp_path: Path,
) -> None:
    demo = _config()
    demo_store, demo_engine = _started_engine(tmp_path / "demo.sqlite3", demo)
    demo_as_of = demo.validation_started_at + timedelta(days=56)
    _persist_gate_evidence(
        demo_engine,
        demo,
        as_of=demo_as_of,
        stressed_net_pnl=Decimal("1000000"),
    )
    demo_result = evaluate_paper_gate(
        demo_store,
        demo.run_id,
        PaperGateEvidence(as_of=demo_as_of),
    )
    assert demo_result.status is PaperGateStatus.BLOCKED_PRECONDITIONS
    assert demo_result.eligible is False
    assert demo_result.checks["validation_run"] is False
    assert demo_result.checks["calibrated_models"] is False

    validation, store, evidence = _gate_run(tmp_path / "pass.sqlite3")
    non_authorizing = evaluate_paper_gate(store, validation.run_id, evidence)
    assert non_authorizing.status is PaperGateStatus.BLOCKED_PRECONDITIONS
    assert non_authorizing.eligible is False
    production_checks = {
        "paper_readiness_receipt_bound",
        "durable_runtime_source_attestation",
        "gate_d_artifact_bytes_verified",
    }
    assert all(not non_authorizing.checks[name] for name in production_checks)
    assert all(value for name, value in non_authorizing.checks.items() if name not in production_checks)

    long_config, long_store, long_evidence = _gate_run(
        tmp_path / "long-window.sqlite3",
        days=57,
    )
    long_result = evaluate_paper_gate(long_store, long_config.run_id, long_evidence)
    assert long_result.status is PaperGateStatus.BLOCKED_PRECONDITIONS
    assert long_result.eligible is False

    with pytest.raises(ValueError, match="precedes the durable paper state"):
        evaluate_paper_gate(
            store,
            validation.run_id,
            PaperGateEvidence(as_of=evidence.as_of - timedelta(microseconds=1)),
        )

    short_config, short_store, short_evidence = _gate_run(
        tmp_path / "short.sqlite3",
        days=41,
    )
    assert (
        evaluate_paper_gate(short_store, short_config.run_id, short_evidence).status
        is PaperGateStatus.BLOCKED_OBSERVATION_WINDOW
    )

    default_threshold = _config(calibrated=True)
    assert default_threshold.minimum_validation_cycles == 30
    cycle_config, cycle_store, cycle_evidence = _gate_run(
        tmp_path / "cycles.sqlite3",
        minimum_cycles=30,
        completed_cycles=29,
    )
    cycle_result = evaluate_paper_gate(cycle_store, cycle_config.run_id, cycle_evidence)
    assert cycle_result.status is PaperGateStatus.BLOCKED_INSUFFICIENT_CYCLES
    assert cycle_result.checks["minimum_cycles"] is False

    incident_config, incident_store, incident_evidence = _gate_run(
        tmp_path / "incident.sqlite3",
        recent_incident=True,
    )
    incident_result = evaluate_paper_gate(
        incident_store,
        incident_config.run_id,
        incident_evidence,
    )
    assert incident_result.status is PaperGateStatus.BLOCKED_PRECONDITIONS
    assert incident_result.checks["operational_state"] is False
    assert incident_result.checks["incident_free_14_days"] is False

    loss_config, loss_store, loss_evidence = _gate_run(
        tmp_path / "loss.sqlite3",
        stressed_net_pnl=Decimal(0),
    )
    assert (
        evaluate_paper_gate(loss_store, loss_config.run_id, loss_evidence).status
        is PaperGateStatus.BLOCKED_STRESSED_RESULT
    )


def test_gate_ignores_a_detached_fabricated_projection(tmp_path: Path) -> None:
    config, store, evidence = _gate_run(
        tmp_path / "durable.sqlite3",
        minimum_cycles=30,
        completed_cycles=0,
    )
    forged = PaperProjection(
        run_id=config.run_id,
        config_hash=config.config_hash,
        initial_cash=config.initial_cash,
        completed_cycles=1_000_000,
    )
    assert forged.completed_cycles > config.minimum_validation_cycles
    assert store.get_projection(config.run_id).completed_cycles == 0

    result = evaluate_paper_gate(store, config.run_id, evidence)
    assert result.status is PaperGateStatus.BLOCKED_INSUFFICIENT_CYCLES
    assert result.eligible is False
    assert result.checks["minimum_cycles"] is False


def test_gate_rejects_a_stress_result_that_predates_later_economic_events(
    tmp_path: Path,
) -> None:
    config = replace(_config(calibrated=True), minimum_validation_cycles=30)
    store, engine = _started_engine(tmp_path / "stale-stress.sqlite3", config)
    for cycle_ordinal in range(30):
        _complete_gate_cycle(engine, config, cycle_ordinal=cycle_ordinal)
    as_of = config.validation_started_at + timedelta(days=42)
    engine.record_stress_result(
        artifact_hash=deterministic_id("phase12_stale_stress", "before-final-market"),
        stressed_net_pnl=Decimal(1),
        evaluated_at=as_of - timedelta(seconds=1),
    )
    engine.process_market(_market("after-stress-economic-market", as_of))
    for ordinal, exercise in enumerate(("RESTART", "DISCONNECT", "PARTIAL_FILL", "CRASH_RECOVERY")):
        engine.record_resilience_exercise(
            exercise=exercise,
            artifact_hash=deterministic_id("phase12_stale_stress_exercise", ordinal),
            exercised_at=as_of,
        )
    engine.record_observation_coverage(
        artifact_hash=deterministic_id("phase12_stale_stress_coverage", as_of),
        window_start=config.validation_started_at,
        window_end=as_of,
        continuous=True,
        recorded_at=as_of,
    )

    result = evaluate_paper_gate(
        store,
        config.run_id,
        PaperGateEvidence(as_of=as_of),
    )

    assert result.status is PaperGateStatus.BLOCKED_STRESSED_RESULT
    assert result.checks["positive_stressed_net_pnl"] is False
    assert any("final economic event" in reason for reason in result.reasons)


def test_fill_keeps_public_bbo_valuation_and_funding_mark_separate(
    tmp_path: Path,
) -> None:
    execution = replace(
        _execution_config(max_participation=1.0),
        maker_fee_bps=Decimal(0),
        taker_fee_bps=Decimal(0),
        slippage=SlippageModel(base_bps=100.0, max_participation=1.0),
    )
    config = _config(execution=execution)
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    decision_market = _market("valuation-entry", _START + timedelta(seconds=1))
    engine.reconcile(as_of=_START)
    decision = _decision(
        config,
        decision_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
        quantity="1",
    )
    engine.submit_decision(decision, decision_market)
    fill_market = _market("valuation-fill", _START + timedelta(seconds=2))
    filled = engine.process_market(fill_market).projection

    public_mid = Decimal("100.5")
    assert filled.public_bbo_mids[_INSTRUMENT] == public_mid
    assert filled.marks[_INSTRUMENT] == public_mid
    assert filled.cost_basis[_INSTRUMENT] > public_mid
    assert filled.equity < config.initial_cash
    assert filled.net_pnl == filled.equity - config.initial_cash

    funding_rate = Decimal("0.0001")
    funding_amount = -(Decimal(1) * public_mid * funding_rate)
    funded = engine.post_funding(
        instrument=_INSTRUMENT,
        amount=funding_amount,
        occurred_at=_START + timedelta(seconds=3),
        source_event_id=deterministic_id("phase12_funding", "public-bbo-not-fill"),
        funding_rate=funding_rate,
        funding_interval_seconds=3600,
        rate_kind="hyperliquid-hourly-settlement",
        mark_price=public_mid,
        source_mark_price=None,
        oracle_price=None,
        position_quantity=Decimal(1),
        mark_source="DURABLE_PUBLIC_BBO_MID",
        source_observation_id="public-bbo-not-fill-observation",
        received_at=_START + timedelta(seconds=3),
        processed_at=_START + timedelta(seconds=3),
        source_activation_cutoff=_START,
    ).projection

    assert funded.public_bbo_mids[_INSTRUMENT] == public_mid
    funding_inputs = tuple(store.iter_inputs(config.run_id, input_type="PUBLIC_FUNDING_SETTLEMENT"))
    assert len(funding_inputs) == 1
    assert funding_inputs[0].payload["mark_price"] == "100.5"
    assert funding_inputs[0].payload["mark_source"] == "DURABLE_PUBLIC_BBO_MID"
    assert engine.replay().to_dict() == funded.to_dict()


def test_hour_old_public_bbo_cannot_value_funding(tmp_path: Path) -> None:
    config = _config(
        risk=replace(PaperRiskLimits(), stale_after_seconds=5),
    )
    _store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    engine.reconcile(as_of=_START)
    entry_market = _market("old-funding-mark-entry", _START + timedelta(seconds=1))
    entry = _decision(
        config,
        entry_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    engine.submit_decision(entry, entry_market)
    engine.process_market(_market("old-funding-mark-fill", _START + timedelta(seconds=2)))

    with pytest.raises(ValueError, match="stale at funding_time"):
        engine.post_funding(
            instrument=_INSTRUMENT,
            amount=Decimal("-0.01005"),
            occurred_at=_START + timedelta(seconds=10),
            source_event_id=deterministic_id("phase12_funding", "stale-public-bbo"),
            funding_rate=Decimal("0.0001"),
            funding_interval_seconds=3600,
            rate_kind="hyperliquid-hourly-settlement",
            mark_price=Decimal("100.5"),
            source_mark_price=None,
            oracle_price=None,
            position_quantity=Decimal(1),
            mark_source="DURABLE_PUBLIC_BBO_MID",
            source_observation_id="stale-public-bbo-observation",
            received_at=_START + timedelta(seconds=11),
            processed_at=_START + timedelta(seconds=11),
            source_activation_cutoff=_START,
        )


def test_adverse_slippage_crossing_hard_cap_has_no_economic_effect(
    tmp_path: Path,
) -> None:
    execution = replace(
        _execution_config(max_participation=1.0),
        maker_fee_bps=Decimal(0),
        taker_fee_bps=Decimal(0),
        slippage=SlippageModel(base_bps=100.0, max_participation=1.0),
    )
    risk = replace(
        PaperRiskLimits(),
        max_order_notional=Decimal("101"),
        max_gross_notional=Decimal("101"),
        max_net_notional=Decimal("101"),
        max_instrument_notional=Decimal("101"),
    )
    config = _config(execution=execution, risk=risk)
    store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    decision_market = _market("hard-cap-decision", _START + timedelta(seconds=1))
    decision = _decision(
        config,
        decision_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
    )
    accepted = engine.submit_decision(decision, decision_market).projection
    assert accepted.orders[decision.orders[0].order_id].status is OrderStatus.ACKED

    rejected = engine.process_market(_market("hard-cap-fill", _START + timedelta(seconds=2))).projection

    assert rejected.positions == {}
    assert rejected.cash == config.initial_cash
    assert rejected.fees == 0
    assert rejected.state is PaperState.PAUSED
    assert any(alert.code == "FILL_HARD_CAP_REJECTED" for alert in store.get_alerts(config.run_id))


def test_reduce_only_exit_remains_allowed_above_entry_hard_caps(tmp_path: Path) -> None:
    execution = replace(
        _execution_config(max_participation=1.0),
        maker_fee_bps=Decimal(0),
        taker_fee_bps=Decimal(0),
        slippage=SlippageModel(base_bps=100.0, max_participation=1.0),
    )
    risk = replace(
        PaperRiskLimits(),
        max_order_notional=Decimal("50"),
        max_gross_notional=Decimal("50"),
        max_net_notional=Decimal("50"),
        max_instrument_notional=Decimal("50"),
    )
    config = _config(execution=execution, risk=risk)
    _store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    entry_market = _market("reduce-cap-entry", _START + timedelta(seconds=1))
    entry = _decision(
        config,
        entry_market,
        action=DecisionAction.ENTRY,
        side=OrderSide.BUY,
        quantity="0.4",
    )
    engine.submit_decision(entry, entry_market)
    opened = engine.process_market(_market("reduce-cap-entry-fill", _START + timedelta(seconds=2))).projection
    assert opened.positions[_INSTRUMENT] == Decimal("0.4")

    exit_market = _market(
        "reduce-cap-exit",
        _START + timedelta(seconds=3),
        bid="200",
        ask="201",
    )
    exit_decision = _decision(
        config,
        exit_market,
        action=DecisionAction.EXIT,
        side=OrderSide.SELL,
        quantity="0.4",
    )
    accepted = engine.submit_decision(exit_decision, exit_market).projection
    assert accepted.orders[exit_decision.orders[0].order_id].status is OrderStatus.ACKED
    flattened = engine.process_market(
        _market(
            "reduce-cap-exit-fill",
            _START + timedelta(seconds=4),
            bid="200",
            ask="201",
        )
    ).projection

    assert flattened.positions == {}
    assert flattened.state is PaperState.FLAT


def test_resume_requires_bilateral_fresh_public_bbos(tmp_path: Path) -> None:
    config = replace(
        _config(risk=replace(PaperRiskLimits(), stale_after_seconds=5)),
        required_instruments=(_INSTRUMENT, _HEDGE_INSTRUMENT),
    )
    _store, engine = _started_engine(tmp_path / "paper.sqlite3", config)
    engine.process_market(_market("resume-btc-initial", _START + timedelta(seconds=1)))
    engine.process_market(
        _market(
            "resume-eth-initial",
            _START + timedelta(seconds=1),
            instrument=_HEDGE_INSTRUMENT,
        )
    )
    engine.pause(
        as_of=_START + timedelta(seconds=2),
        reason="bilateral freshness fixture",
        operator_artifact_hash="1" * 64,
    )
    engine.process_market(_market("resume-btc-fresh", _START + timedelta(seconds=3)))

    review_hash = "2" * 64
    reviewed = engine.projection()
    with pytest.raises(ValueError, match="every required instrument"):
        engine.resume_from_pause(
            as_of=_START + timedelta(seconds=7),
            review_artifact_hash=review_hash,
            reviewed_critical_incident_count=reviewed.critical_incident_count,
            reviewed_last_critical_incident_at=reviewed.last_critical_incident_at,
            recovery_mode="STANDARD",
        )

    engine.process_market(
        _market(
            "resume-eth-fresh",
            _START + timedelta(seconds=7),
            instrument=_HEDGE_INSTRUMENT,
        )
    )
    resumed = engine.resume_from_pause(
        as_of=_START + timedelta(seconds=8),
        review_artifact_hash=review_hash,
        reviewed_critical_incident_count=reviewed.critical_incident_count,
        reviewed_last_critical_incident_at=reviewed.last_critical_incident_at,
        recovery_mode="STANDARD",
    ).projection

    assert resumed.state is PaperState.FLAT
    assert engine.replay().to_dict() == resumed.to_dict()
