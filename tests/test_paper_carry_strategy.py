from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from hyperlab.backtest.execution import MakerFillModel
from hyperlab.backtest.protocol import canonical_sha256
from hyperlab.collector.models import ParsedRecord
from hyperlab.data.schema import RecordType, latest_schema_for
from hyperlab.paper.carry_strategy import (
    PHASE05_CARRY_STRATEGY_ID,
    FrozenCashAndCarryPaperConfig,
    FrozenCashAndCarryPaperStrategy,
    make_phase05_paper_strategy_config,
)
from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.models import (
    DecisionIntent,
    MarketEvent,
    OrderSide,
    OrderStatus,
    PaperExecutionConfig,
    PaperRiskLimits,
    PaperRunConfig,
    PaperState,
    TimeInForce,
)
from hyperlab.paper.public_source import (
    PublicFundingSettlement,
    PublicRecordAdapterError,
    PublicRecordMarketEventAdapter,
)
from hyperlab.paper.runner import PaperStrategyView
from hyperlab.paper.store import PaperStore

_START = datetime(2026, 1, 1, tzinfo=UTC)
_SPOT = "HL:BTC:spot"
_PERP = "HL:BTC:perp"
_SPOT_IDENTITY = "1" * 64
_PERP_IDENTITY = "2" * 64


def _config() -> FrozenCashAndCarryPaperConfig:
    return FrozenCashAndCarryPaperConfig(
        spot_instrument=_SPOT,
        perp_instrument=_PERP,
        spot_product_identity_sha256=_SPOT_IDENTITY,
        perp_product_identity_sha256=_PERP_IDENTITY,
        retained_hours=96,
        maximum_gross_notional=Decimal("500"),
        spot_quantity_step=Decimal("0.0001"),
        perp_quantity_step=Decimal("0.0001"),
        spot_max_quantity=Decimal("10"),
        perp_max_quantity=Decimal("10"),
    )


def _risk() -> PaperRiskLimits:
    return PaperRiskLimits(
        max_gross_notional=Decimal("500"),
        max_net_notional=Decimal("300"),
        max_instrument_notional=Decimal("250"),
        max_order_notional=Decimal("250"),
        max_position_quantity=Decimal("10"),
        max_order_quantity=Decimal("10"),
        max_concurrent_orders=2,
        max_daily_loss=Decimal("100"),
        max_drawdown=Decimal("200"),
        stale_after_seconds=10,
        unhedged_timeout_seconds=20,
    )


def _bound_strategy() -> tuple[FrozenCashAndCarryPaperStrategy, str, str]:
    config = _config()
    identity = make_phase05_paper_strategy_config(config=config, risk=_risk())
    strategy = FrozenCashAndCarryPaperStrategy(config=config, strategy_config=identity)
    return strategy, identity.strategy_hash, identity.strategy_config_hash


def _market(
    instrument: str,
    received_at: datetime,
    *,
    mid: Decimal,
    capture_ordinal: int,
) -> MarketEvent:
    is_spot = instrument.endswith(":spot")
    identity = _SPOT_IDENTITY if is_spot else _PERP_IDENTITY
    spread = Decimal("0.01")
    return MarketEvent.create(
        received_at=received_at,
        instrument=instrument,
        bid_price=mid - spread / 2,
        ask_price=mid + spread / 2,
        bid_depth=Decimal("5000"),
        ask_depth=Decimal("5000"),
        capture_ordinal=capture_ordinal,
        context={
            "instrument_kind": "spot" if is_spot else "perp",
            "notional_volume_24h": "100000000",
            "observation_id": canonical_sha256(
                {"instrument": instrument, "received_at": received_at.isoformat()}
            ),
            "open_interest_notional": None if is_spot else "1000000000",
            "product_identity_sha256": identity,
            "received_at": received_at,
            "source_asset": "@107" if is_spot else "BTC",
        },
    )


def _funding(hour: int, *, rate: Decimal = Decimal("0.0002")) -> PublicFundingSettlement:
    funding_time = _START + timedelta(hours=hour)
    return PublicFundingSettlement(
        event_id=canonical_sha256({"funding_time": funding_time.isoformat(), "instrument": _PERP}),
        instrument=_PERP,
        funding_time=funding_time,
        received_at=funding_time,
        funding_rate=rate,
        funding_interval_seconds=3600,
        rate_kind="hyperliquid-hourly-settlement",
        mark_price=Decimal("100.5"),
        oracle_price=Decimal("100.4"),
        source_observation_id=f"funding-{hour}",
    )


def _view(
    strategy: FrozenCashAndCarryPaperStrategy,
    strategy_hash: str,
    strategy_config_hash: str,
    *,
    state: PaperState = PaperState.FLAT,
    positions: dict[str, Decimal] | None = None,
    run_id: str = "a" * 64,
    config_hash: str = "b" * 64,
) -> PaperStrategyView:
    return PaperStrategyView(
        run_id=run_id,
        config_hash=config_hash,
        state=state,
        positions=positions or {},
        marks={},
        completed_cycles=0,
        strategy_id=strategy.strategy_id,
        strategy_name=strategy.strategy_name,
        strategy_hash=strategy_hash,
        strategy_config_hash=strategy_config_hash,
    )


def _warm_to_entry(
    strategy: FrozenCashAndCarryPaperStrategy,
    strategy_hash: str,
    strategy_config_hash: str,
    *,
    run_id: str = "a" * 64,
    config_hash: str = "b" * 64,
) -> DecisionIntent | None:
    view = _view(
        strategy,
        strategy_hash,
        strategy_config_hash,
        run_id=run_id,
        config_hash=config_hash,
    )
    decision = None
    for hour in range(81):
        if hour:
            strategy.observe_funding(_funding(hour))
        spot_mid = Decimal("100") + Decimal(hour) / Decimal("1000")
        basis_bps = Decimal("80") - Decimal(hour) / Decimal("2")
        perp_mid = spot_mid * (Decimal("1") + basis_bps / Decimal("10000"))
        frame = {
            _SPOT: _market(
                _SPOT,
                _START + timedelta(hours=hour),
                mid=spot_mid,
                capture_ordinal=hour * 2 + 1,
            ),
            _PERP: _market(
                _PERP,
                _START + timedelta(hours=hour),
                mid=perp_mid,
                capture_ordinal=hour * 2 + 2,
            ),
        }
        candidate = strategy.decide(frame, view)
        if candidate is not None:
            decision = candidate
    return decision


def _common_row(record_type: RecordType, received_at: datetime, *, asset: str) -> dict[str, object]:
    row: dict[str, object] = {name: None for name in latest_schema_for(record_type).schema.names}
    row.update(
        {
            "asset": asset,
            "connection_id": "connection-1",
            "event_time": received_at,
            "exchange_time": None,
            "received_time": received_at,
            "record_type": record_type.value,
            "schema_version": latest_schema_for(record_type).version,
            "source_sequence": 1,
            "venue": "hyperliquid",
        }
    )
    return row


def test_phase05_adapter_identity_is_stable_and_risk_bound() -> None:
    config = _config()
    first = make_phase05_paper_strategy_config(config=config, risk=_risk())
    second = make_phase05_paper_strategy_config(config=config, risk=_risk())

    assert first == second
    assert first.strategy_id == PHASE05_CARRY_STRATEGY_ID
    assert first.required_instruments == (_PERP, _SPOT)
    assert FrozenCashAndCarryPaperStrategy(config=config).strategy_hash == first.strategy_hash
    assert first.parameters["economic_status"] == "TECHNICAL_ONLY_UNCALIBRATED"

    tighter = make_phase05_paper_strategy_config(
        config=config,
        risk=replace(_risk(), max_gross_notional=Decimal("499")),
    )
    assert tighter.strategy_config_hash != first.strategy_config_hash
    with pytest.raises(ValueError, match="local risk budget is below"):
        FrozenCashAndCarryPaperStrategy(config=config, strategy_config=tighter)


def test_market_event_context_round_trips_without_changing_legacy_payloads() -> None:
    contextual = _market(_SPOT, _START, mid=Decimal("100"), capture_ordinal=1)
    restored = MarketEvent.from_dict(contextual.to_dict())

    assert restored == contextual
    assert restored.context["product_identity_sha256"] == _SPOT_IDENTITY

    legacy = MarketEvent.create(
        received_at=_START,
        instrument=_PERP,
        bid_price=Decimal("99"),
        ask_price=Decimal("101"),
        bid_depth=Decimal("1"),
        ask_depth=Decimal("1"),
        capture_ordinal=2,
    )
    assert "context" not in legacy.to_dict()

    malformed = contextual.to_dict()
    malformed["context"] = "not-a-mapping"
    with pytest.raises(TypeError, match="context payload must be a mapping"):
        MarketEvent.from_dict(malformed)


def test_opt_in_shared_source_attaches_normalized_context_with_spot_alias() -> None:
    adapter = PublicRecordMarketEventAdapter(
        instruments={
            ("hyperliquid", "@107"): _SPOT,
            ("hyperliquid", "BTC"): _PERP,
        },
        queue_capacity=16,
        include_market_context=True,
        product_identity_hashes={
            _SPOT: _SPOT_IDENTITY,
            _PERP: _PERP_IDENTITY,
        },
    )
    context_row = _common_row(RecordType.MARKET_CONTEXT, _START, asset="@107")
    context_row.update(
        {
            "instrument_kind": "spot",
            "instrument_id": "HL:@107:spot",
            "mark_price": Decimal("100"),
            "mid_price": Decimal("100"),
            "notional_volume_24h": Decimal("100000000"),
            "observation_id": "spot-context-1",
        }
    )
    mismatched = dict(context_row)
    mismatched["instrument_id"] = "HL:OTHER:spot"
    with pytest.raises(
        PublicRecordAdapterError,
        match="instrument_id differs from the frozen source route",
    ):
        adapter.adapt(ParsedRecord(RecordType.MARKET_CONTEXT, "@107", mismatched))
    assert adapter.adapt(ParsedRecord(RecordType.MARKET_CONTEXT, "@107", context_row)) is None

    bbo_row = _common_row(RecordType.BBO, _START + timedelta(seconds=1), asset="@107")
    bbo_row.update(
        {
            "ask_price": Decimal("100.1"),
            "ask_quantity": Decimal("1000"),
            "bid_price": Decimal("99.9"),
            "bid_quantity": Decimal("1000"),
            "update_id": "spot-bbo-1",
        }
    )
    frame = adapter.adapt(ParsedRecord(RecordType.BBO, "@107", bbo_row))

    assert frame is not None
    event = frame[_SPOT]
    assert event.context["source_asset"] == "@107"
    assert event.context["instrument_kind"] == "spot"
    assert event.context["product_identity_sha256"] == _SPOT_IDENTITY
    assert b"market_context" in adapter.identity_artifact_bytes


def test_phase05_signal_and_two_leg_maker_entry_are_deterministic() -> None:
    first, first_hash, first_config_hash = _bound_strategy()
    second, second_hash, second_config_hash = _bound_strategy()

    first_decision = _warm_to_entry(first, first_hash, first_config_hash)
    second_decision = _warm_to_entry(second, second_hash, second_config_hash)

    assert first_decision is not None
    assert first_decision == second_decision
    assert first_decision.strategy_id == PHASE05_CARRY_STRATEGY_ID
    assert len(first_decision.orders) == 2
    assert [order.instrument for order in first_decision.orders] == [_SPOT, _PERP]
    assert [order.side.value for order in first_decision.orders] == ["BUY", "SELL"]
    assert [order.order_type.value for order in first_decision.orders] == ["MAKER", "MAKER"]
    assert [order.leg_number for order in first_decision.orders] == [1, 2]
    assert len({order.hedge_group_id for order in first_decision.orders}) == 1
    assert all(order.strategy_id == PHASE05_CARRY_STRATEGY_ID for order in first_decision.orders)


def test_phase05_signal_waits_for_both_post_bar_execution_quotes() -> None:
    strategy, strategy_hash, strategy_config_hash = _bound_strategy()
    view = _view(strategy, strategy_hash, strategy_config_hash)
    last_frame: dict[str, MarketEvent] = {}
    for hour in range(80):
        if hour:
            strategy.observe_funding(_funding(hour))
        spot_mid = Decimal("100") + Decimal(hour) / Decimal("1000")
        basis_bps = Decimal("80") - Decimal(hour) / Decimal("2")
        perp_mid = spot_mid * (Decimal("1") + basis_bps / Decimal("10000"))
        last_frame = {
            _SPOT: _market(
                _SPOT,
                _START + timedelta(hours=hour),
                mid=spot_mid,
                capture_ordinal=hour * 2 + 1,
            ),
            _PERP: _market(
                _PERP,
                _START + timedelta(hours=hour),
                mid=perp_mid,
                capture_ordinal=hour * 2 + 2,
            ),
        }
        strategy.decide(last_frame, view)

    strategy.observe_funding(_funding(80))
    received_at = _START + timedelta(hours=80)
    spot = _market(
        _SPOT,
        received_at,
        mid=Decimal("100.08"),
        capture_ordinal=161,
    )
    perp = _market(
        _PERP,
        received_at,
        mid=Decimal("100.482"),
        capture_ordinal=162,
    )

    assert strategy.decide({_SPOT: spot, _PERP: last_frame[_PERP]}, view) is None
    assert strategy.diagnostic_snapshot["status"] == ("WAITING_FOR_COMPLETE_POST_BAR_EXECUTION_FRAME")
    decision = strategy.decide({_SPOT: spot, _PERP: perp}, view)

    assert decision is not None
    assert decision.market_event_id == perp.event_id
    assert decision.observed_event_ids[-2:] == (spot.event_id, perp.event_id)


def test_phase05_restoration_reconstructs_signal_state_without_reemitting_history() -> None:
    live, strategy_hash, strategy_config_hash = _bound_strategy()
    inputs: list[MarketEvent | PublicFundingSettlement] = []
    view = _view(live, strategy_hash, strategy_config_hash)
    for hour in range(81):
        if hour:
            settlement = _funding(hour)
            live.observe_funding(settlement)
            inputs.append(settlement)
        spot_mid = Decimal("100") + Decimal(hour) / Decimal("1000")
        basis_bps = Decimal("80") - Decimal(hour) / Decimal("2")
        perp_mid = spot_mid * (Decimal("1") + basis_bps / Decimal("10000"))
        frame = {
            _SPOT: _market(
                _SPOT,
                _START + timedelta(hours=hour),
                mid=spot_mid,
                capture_ordinal=hour * 2 + 1,
            ),
            _PERP: _market(
                _PERP,
                _START + timedelta(hours=hour),
                mid=perp_mid,
                capture_ordinal=hour * 2 + 2,
            ),
        }
        inputs.extend(frame.values())
        live.decide(frame, view)

    restored, restored_hash, restored_config_hash = _bound_strategy()
    restored.restore_public_inputs(inputs, _view(restored, restored_hash, restored_config_hash))

    assert restored.diagnostic_snapshot["status"] == "RESTORED"
    assert restored.diagnostic_snapshot["signal_input_hash"] == live.diagnostic_snapshot["signal_input_hash"]
    duplicate_frame = {
        _SPOT: inputs[-2],
        _PERP: inputs[-1],
    }
    assert restored.decide(duplicate_frame, _view(restored, restored_hash, restored_config_hash)) is None


def test_phase05_two_leg_exit_is_reduce_only_and_deterministic() -> None:
    strategy, strategy_hash, strategy_config_hash = _bound_strategy()
    entry = _warm_to_entry(strategy, strategy_hash, strategy_config_hash)
    assert entry is not None
    positions = {order.instrument: order.quantity * order.side.sign for order in entry.orders}
    view = _view(
        strategy,
        strategy_hash,
        strategy_config_hash,
        state=PaperState.HEDGED,
        positions=positions,
    )

    exit_intent = None
    for hour in range(81, 89):
        strategy.observe_funding(_funding(hour))
        spot_mid = Decimal("100") + Decimal(hour) / Decimal("1000")
        basis_bps = Decimal("250") if hour >= 87 else Decimal("40")
        perp_mid = spot_mid * (Decimal("1") + basis_bps / Decimal("10000"))
        frame = {
            _SPOT: _market(
                _SPOT,
                _START + timedelta(hours=hour),
                mid=spot_mid,
                capture_ordinal=hour * 2 + 1,
            ),
            _PERP: _market(
                _PERP,
                _START + timedelta(hours=hour),
                mid=perp_mid,
                capture_ordinal=hour * 2 + 2,
            ),
        }
        candidate = strategy.decide(frame, view)
        if candidate is not None:
            exit_intent = candidate

    assert exit_intent is not None
    assert exit_intent.action.value == "EXIT"
    assert len(exit_intent.orders) == 2
    assert all(order.reduce_only for order in exit_intent.orders)
    assert all(order.time_in_force is TimeInForce.GTC for order in exit_intent.orders)
    assert [order.instrument for order in exit_intent.orders] == [_SPOT, _PERP]
    for order in exit_intent.orders:
        assert order.quantity == abs(positions[order.instrument])
        assert order.side.sign == (-1 if positions[order.instrument] > 0 else 1)


def test_phase05_partial_or_missing_second_leg_times_out_and_flattens_exactly(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    strategy_config = _config()
    hedge_risk = replace(
        _risk(),
        max_instrument_notional=Decimal("251"),
        max_order_notional=Decimal("251"),
    )
    identity = make_phase05_paper_strategy_config(
        config=strategy_config,
        risk=hedge_risk,
    )
    execution = PaperExecutionConfig(
        maker_fill=MakerFillModel(
            base_probability=1.0,
            participation_decay=0.0,
            calibration_id="synthetic-phase05-hedge-fixture",
            calibration_status="SYNTHETIC",
        ),
        maker_fee_bps=Decimal("1"),
        taker_fee_bps=Decimal("2"),
        ioc_fill_probability=Decimal("1"),
        maker_timeout_ms=1_000,
        calibration_status="SYNTHETIC",
        source="SYNTHETIC_TECHNICAL_ONLY_FIXTURE",
    )
    run_config = PaperRunConfig(
        strategy_name=identity.strategy_name,
        strategy_hash=identity.strategy_hash,
        parameters=identity.parameters,
        data_hash="d" * 64,
        execution=execution,
        risk=hedge_risk,
        seed=505,
        initial_cash=Decimal("10000"),
        validation_started_at=_START,
        run_kind="DEMO",
        data_calibration_status="SYNTHETIC",
        data_source="SYNTHETIC_PHASE05_HEDGE_FIXTURE",
        required_instruments=identity.required_instruments,
        schema_version=3,
        strategies=(identity,),
    )

    for label, second_trade_fraction in (
        ("second-no-fill", None),
        ("second-partial", Decimal("0.5")),
    ):
        strategy = FrozenCashAndCarryPaperStrategy(
            config=strategy_config,
            strategy_config=identity,
        )
        entry = _warm_to_entry(
            strategy,
            identity.strategy_hash,
            identity.strategy_config_hash,
            run_id=run_config.run_id,
            config_hash=run_config.config_hash,
        )
        assert entry is not None
        entry_at = _START + timedelta(hours=80)
        spot_mid = Decimal("100.08")
        perp_mid = spot_mid * (Decimal("1") + Decimal("40") / Decimal("10000"))
        frame = {
            _SPOT: _market(
                _SPOT,
                entry_at,
                mid=spot_mid,
                capture_ordinal=161,
            ),
            _PERP: _market(
                _PERP,
                entry_at,
                mid=perp_mid,
                capture_ordinal=162,
            ),
        }
        database = tmp_path / f"{label}.sqlite3"
        engine = PaperEngine(PaperStore(database), run_config)
        engine.start()
        planned = engine.submit_decision(entry, frame).projection
        spot_order = next(order for order in planned.orders.values() if order.intent.instrument == _SPOT)
        perp_order = next(order for order in planned.orders.values() if order.intent.instrument == _PERP)
        assert spot_order.intent.hedge_group_id == perp_order.intent.hedge_group_id

        spot_fill = MarketEvent.create(
            received_at=entry_at + timedelta(milliseconds=500),
            instrument=_SPOT,
            bid_price=frame[_SPOT].bid_price,
            ask_price=frame[_SPOT].ask_price,
            bid_depth=Decimal("100"),
            ask_depth=Decimal("100"),
            source_sequence=900_001,
            trade_price=spot_order.intent.limit_price,
            trade_quantity=spot_order.intent.quantity,
            aggressor_side=OrderSide.SELL,
        )
        after_first = engine.process_market(spot_fill).projection
        assert after_first.strategy_projection(PHASE05_CARRY_STRATEGY_ID).state is PaperState.HEDGE_PENDING
        assert after_first.positions[_SPOT] == spot_order.intent.quantity

        if second_trade_fraction is not None:
            perp_partial = MarketEvent.create(
                received_at=entry_at + timedelta(milliseconds=750),
                instrument=_PERP,
                bid_price=frame[_PERP].bid_price,
                ask_price=frame[_PERP].ask_price,
                bid_depth=Decimal("100"),
                ask_depth=Decimal("100"),
                source_sequence=900_002,
                trade_price=perp_order.intent.limit_price,
                trade_quantity=perp_order.intent.quantity * second_trade_fraction,
                aggressor_side=OrderSide.BUY,
            )
            partial = engine.process_market(perp_partial).projection
            assert partial.orders[perp_order.intent.order_id].status is OrderStatus.PARTIALLY_FILLED

        terminal_market = MarketEvent.create(
            received_at=entry_at + timedelta(seconds=2),
            instrument=_PERP,
            bid_price=frame[_PERP].bid_price,
            ask_price=frame[_PERP].ask_price,
            bid_depth=Decimal("100"),
            ask_depth=Decimal("100"),
            source_sequence=900_003,
        )
        unhedged = engine.process_market(terminal_market).projection
        expected_status = OrderStatus.NO_FILL if second_trade_fraction is None else OrderStatus.EXPIRED
        assert unhedged.orders[perp_order.intent.order_id].status is expected_status
        assert unhedged.strategy_projection(PHASE05_CARRY_STRATEGY_ID).state is PaperState.HEDGE_PENDING

        restarted = PaperEngine(PaperStore(database), run_config)
        assert restarted.projection().to_dict() == unhedged.to_dict()
        timed_out = restarted.process_timer(as_of=entry_at + timedelta(seconds=23)).projection
        assert timed_out.state is PaperState.EMERGENCY_FLATTEN
        timeout_alerts = [
            alert
            for alert in restarted.store.get_alerts(run_config.run_id)
            if alert.code == "UNHEDGED_TIMEOUT"
        ]
        assert timeout_alerts
        assert timeout_alerts[-1].alert["strategy_id"] == PHASE05_CARRY_STRATEGY_ID

        emergency_at = entry_at + timedelta(seconds=24)
        emergency_markets = {
            instrument: MarketEvent.create(
                received_at=emergency_at,
                instrument=instrument,
                bid_price=frame[instrument].bid_price,
                ask_price=frame[instrument].ask_price,
                bid_depth=Decimal("100"),
                ask_depth=Decimal("100"),
                source_sequence=910_000 + ordinal,
                capture_ordinal=ordinal,
            )
            for ordinal, instrument in enumerate(sorted(timed_out.positions), start=1)
        }
        planned_exit = restarted.emergency_flatten(
            emergency_markets,
            decided_at=emergency_at,
            reason="synthetic Phase 05 unhedged timeout fixture",
        ).projection
        exit_orders = [
            order
            for order in planned_exit.orders.values()
            if order.intent.reduce_only and order.status.active
        ]
        assert exit_orders
        assert all(order.intent.time_in_force is TimeInForce.IOC for order in exit_orders)
        for order in sorted(exit_orders, key=lambda item: item.intent.instrument):
            market = emergency_markets[order.intent.instrument]
            restarted.process_market(
                replace(
                    market,
                    received_at=emergency_at + timedelta(seconds=1),
                    event_id=MarketEvent.identifier(
                        instrument=market.instrument,
                        received_at=emergency_at + timedelta(seconds=1),
                        source_sequence=market.source_sequence,
                        capture_ordinal=market.capture_ordinal,
                    ),
                )
            )

        flattened = restarted.projection()
        assert flattened.positions == {}
        assert flattened.strategy_projection(PHASE05_CARRY_STRATEGY_ID).state is PaperState.FLAT
        assert restarted.replay().to_dict() == flattened.to_dict()
        assert restarted.verify_input_replay().to_dict() == flattened.to_dict()
        assert restarted.reconcile(as_of=emergency_at + timedelta(seconds=2)).projection.reconciled
