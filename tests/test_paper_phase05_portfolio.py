from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from hyperlab.backtest.execution import MakerFillModel
from hyperlab.backtest.protocol import canonical_sha256
from hyperlab.collector.bootstrap import parse_funding_history
from hyperlab.collector.models import WireEnvelope
from hyperlab.environment_authorization import (
    current_paper_release_code_sha256,
    current_paper_runtime_environment_sha256,
    paper_release_identity_candidate,
)
from hyperlab.paper.carry_strategy import (
    PHASE05_CARRY_STRATEGY_ID,
    FrozenCashAndCarryPaperStrategy,
)
from hyperlab.paper.collector_source import (
    PHASE12_PHASE05_PRODUCT_IDENTITY_HASHES,
    PHASE12_PHASE05_PUBLIC_ASSETS,
    PHASE12_PHASE05_PUBLIC_INSTRUMENTS,
    PHASE12_PHASE05_PUBLIC_SOURCE_NAME,
)
from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.models import (
    DecisionAction,
    DecisionIntent,
    MarketEvent,
    OrderIntent,
    OrderSide,
    OrderStatus,
    PaperExecutionConfig,
    PaperOrderType,
    PaperState,
    TimeInForce,
    decimal_text,
    utc_text,
)
from hyperlab.paper.pairs_strategy import (
    PHASE08_PAIRS_STRATEGY_ID,
    FrozenRobustPairsPaperStrategy,
)
from hyperlab.paper.phase05_portfolio import (
    Phase05Phase08PaperFoundation,
    build_phase05_phase08_paper_foundation,
    default_phase05_phase08_risk_allocation,
)
from hyperlab.paper.public_source import (
    PublicFundingSettlement,
    PublicRecordMarketEventAdapter,
)
from hyperlab.paper.reporting import build_paper_report
from hyperlab.paper.runner import PaperStrategyView, PortfolioRunner
from hyperlab.paper.runtime import (
    PaperAdmissionError,
    PaperRuntime,
    PaperRuntimeConfig,
    PublicSourceDescriptor,
    replay_paper_run,
)
from hyperlab.paper.store import PaperStore

_START = datetime(2026, 1, 1, tzinfo=UTC)
_SPOT = "HL:HYPE:spot"
_HYPE_PERP = "HL:HYPE:perp"
_BTC = "HL:BTC:perp"
_ETH = "HL:ETH:perp"


def _execution() -> PaperExecutionConfig:
    return PaperExecutionConfig(
        maker_fill=MakerFillModel(
            base_probability=1.0,
            participation_decay=0.0,
            calibration_id="synthetic-phase05-portfolio-fixture",
            calibration_status="SYNTHETIC",
        ),
        maker_fee_bps=Decimal("1"),
        taker_fee_bps=Decimal("2"),
        ioc_fill_probability=Decimal("1"),
        maker_timeout_ms=1_000,
        calibration_status="UNCALIBRATED",
        source="SYNTHETIC_TECHNICAL_ONLY_FIXTURE",
    )


def _foundation(tmp_path: Path) -> Phase05Phase08PaperFoundation:
    candidate_id = paper_release_identity_candidate(
        config_schema_version=3,
    )
    return build_phase05_phase08_paper_foundation(
        runtime_status_path=tmp_path / "collector-status.json",
        validation_started_at=_START,
        execution=_execution(),
        release_code_sha256=current_paper_release_code_sha256(
            candidate_id=candidate_id,
        ),
        runtime_environment_sha256=current_paper_runtime_environment_sha256(
            candidate_id=candidate_id,
        ),
    )


def _carry_event(
    instrument: str,
    received_at: datetime,
    *,
    mid: Decimal,
    capture_ordinal: int,
    source_sequence: int,
    trade_price: Decimal | None = None,
    trade_quantity: Decimal | None = None,
    aggressor_side: OrderSide | None = None,
) -> MarketEvent:
    is_spot = instrument == _SPOT
    half_spread = Decimal("0.005")
    return MarketEvent.create(
        received_at=received_at,
        instrument=instrument,
        bid_price=mid - half_spread,
        ask_price=mid + half_spread,
        bid_depth=Decimal("5000"),
        ask_depth=Decimal("5000"),
        source_sequence=source_sequence,
        capture_ordinal=capture_ordinal,
        trade_price=trade_price,
        trade_quantity=trade_quantity,
        aggressor_side=aggressor_side,
        context={
            "instrument_kind": "spot" if is_spot else "perp",
            "notional_volume_24h": "100000000",
            "observation_id": canonical_sha256(
                {
                    "instrument": instrument,
                    "received_at": received_at.isoformat(),
                    "source_sequence": source_sequence,
                }
            ),
            "open_interest_notional": None if is_spot else "1000000000",
            "product_identity_sha256": PHASE12_PHASE05_PRODUCT_IDENTITY_HASHES[
                instrument
            ],
            "received_at": received_at,
            "source_asset": "@107" if is_spot else "HYPE",
        },
    )


def _carry_frame(hour: int, *, capture_base: int = 0) -> dict[str, MarketEvent]:
    received_at = _START + timedelta(hours=hour)
    spot_mid = Decimal("100") + Decimal(hour) / Decimal("1000")
    basis_bps = Decimal("80") - Decimal(hour) / Decimal("2")
    perp_mid = spot_mid * (Decimal("1") + basis_bps / Decimal("10000"))
    return {
        _SPOT: _carry_event(
            _SPOT,
            received_at,
            mid=spot_mid,
            capture_ordinal=capture_base + 1,
            source_sequence=hour * 10 + 1,
        ),
        _HYPE_PERP: _carry_event(
            _HYPE_PERP,
            received_at,
            mid=perp_mid,
            capture_ordinal=capture_base + 2,
            source_sequence=hour * 10 + 2,
        ),
    }


def _pair_frame(
    index: int,
    spread: float,
    *,
    pair_start: datetime,
    capture_base: int = 0,
) -> dict[str, MarketEvent]:
    received_at = pair_start + timedelta(seconds=index * 30 + 10)
    btc_mid = Decimal(str(100_000.0 + 150.0 * index))
    eth_mid = Decimal(str(float(btc_mid) * math.exp(spread)))

    def event(instrument: str, mid: Decimal, ordinal: int) -> MarketEvent:
        return MarketEvent.create(
            received_at=received_at,
            instrument=instrument,
            bid_price=mid - Decimal("0.01"),
            ask_price=mid + Decimal("0.01"),
            bid_depth=Decimal("100"),
            ask_depth=Decimal("100"),
            source_sequence=100_000 + index * 10 + ordinal,
            capture_ordinal=capture_base + ordinal,
        )

    return {
        _ETH: event(_ETH, eth_mid, 1),
        _BTC: event(_BTC, btc_mid, 2),
    }


def _funding(hour: int) -> PublicFundingSettlement:
    funding_time = _START + timedelta(hours=hour)
    return PublicFundingSettlement(
        event_id=canonical_sha256(
            {"funding_time": funding_time.isoformat(), "instrument": _HYPE_PERP}
        ),
        instrument=_HYPE_PERP,
        funding_time=funding_time,
        received_at=funding_time,
        funding_rate=Decimal("0.0002"),
        funding_interval_seconds=3600,
        rate_kind="hyperliquid-hourly-settlement",
        mark_price=Decimal("100.5"),
        oracle_price=Decimal("100.4"),
        source_observation_id=f"synthetic-funding-{hour}",
    )


def _realistic_hype_funding_settlement(
    *,
    offset_ms: int = 30,
) -> tuple[datetime, PublicFundingSettlement]:
    funding_hour = _START + timedelta(hours=1)
    received_at = funding_hour + timedelta(seconds=2)
    raw_time_ms = int(funding_hour.timestamp() * 1_000) + offset_ms
    envelope = WireEnvelope(
        raw_message=(
            '[{"coin":"HYPE","fundingRate":"0.0000125",'
            f'"premium":"-0.0002319741","time":{raw_time_ms}}}]'
        ),
        received_time=received_at,
        connection_id="rest-refresh-7-fixture",
        connection_epoch=7,
        arrival_sequence=42,
    )
    records = parse_funding_history(
        [
            {
                "coin": "HYPE",
                "fundingRate": "0.0000125",
                "premium": "-0.0002319741",
                "time": raw_time_ms,
            }
        ],
        envelope,
    )
    assert len(records) == 1
    adapter = PublicRecordMarketEventAdapter(
        instruments=PHASE12_PHASE05_PUBLIC_INSTRUMENTS,
        queue_capacity=16,
        include_market_context=True,
        product_identity_hashes=PHASE12_PHASE05_PRODUCT_IDENTITY_HASHES,
    )
    settlement = adapter.adapt(records[0])
    assert isinstance(settlement, PublicFundingSettlement)
    return funding_hour, settlement


def _post_flat_funding(
    engine: PaperEngine,
    strategy: FrozenCashAndCarryPaperStrategy,
    hour: int,
) -> None:
    settlement = _funding(hour)
    engine.post_funding(
        instrument=settlement.instrument,
        amount=Decimal(0),
        occurred_at=settlement.funding_time,
        source_event_id=settlement.event_id,
        funding_rate=settlement.funding_rate,
        funding_interval_seconds=settlement.funding_interval_seconds,
        rate_kind=settlement.rate_kind,
        mark_price=settlement.mark_price,
        source_mark_price=settlement.mark_price,
        oracle_price=settlement.oracle_price,
        position_quantity=Decimal(0),
        mark_source="PUBLIC_SETTLEMENT_MARK",
        source_observation_id=settlement.source_observation_id,
        received_at=settlement.received_at,
        processed_at=settlement.received_at,
    )
    strategy.observe_funding(settlement)


def _real_adapters(
    foundation: Phase05Phase08PaperFoundation,
) -> tuple[FrozenCashAndCarryPaperStrategy, FrozenRobustPairsPaperStrategy]:
    carry = next(
        strategy
        for strategy in foundation.strategies
        if isinstance(strategy, FrozenCashAndCarryPaperStrategy)
    )
    pairs = next(
        strategy
        for strategy in foundation.strategies
        if isinstance(strategy, FrozenRobustPairsPaperStrategy)
    )
    return carry, pairs


def _warm_frame_without_decision(
    engine: PaperEngine,
    foundation: Phase05Phase08PaperFoundation,
    frame: Mapping[str, MarketEvent],
    *,
    processed_at: datetime,
) -> None:
    for market in sorted(
        frame.values(),
        key=lambda item: (item.received_at, item.capture_ordinal, item.event_id),
    ):
        engine.process_market(market, processed_at=processed_at)
    for strategy_config in foundation.config.strategy_configs:
        adapter = next(
            item
            for item in foundation.strategies
            if item.strategy_id == strategy_config.strategy_id
        )
        view = PaperStrategyView.from_projection(engine.projection(), strategy_config)
        assert adapter.decide(frame, replace(view, state=PaperState.PAUSED)) is None


class _OfflinePublicSource:
    def __init__(self, descriptor: PublicSourceDescriptor) -> None:
        self.descriptor = descriptor
        self.started = False

    def start(self) -> None:
        self.started = True

    def poll(self, *, timeout_seconds: float) -> None:
        del timeout_seconds
        return None

    def stop(self) -> None:
        return None

    def close(self) -> None:
        return None


def test_foundation_is_lazy_public_only_uncalibrated_and_budget_bound(
    tmp_path: Path,
) -> None:
    foundation = _foundation(tmp_path)
    try:
        allocation = default_phase05_phase08_risk_allocation()
        config = foundation.config
        identity = json.loads(foundation.source.identity_artifact_bytes)

        assert config.schema_version == 3
        assert config.environment == "PAPER"
        assert config.run_kind == "TECHNICAL"
        assert config.data_calibration_status == "UNCALIBRATED"
        assert config.economic_prerequisites_satisfied is False
        assert config.data_source == PHASE12_PHASE05_PUBLIC_SOURCE_NAME
        assert config.required_instruments == (_BTC, _ETH, _HYPE_PERP, _SPOT)
        assert [item.strategy_id for item in config.strategy_configs] == [
            PHASE05_CARRY_STRATEGY_ID,
            PHASE08_PAIRS_STRATEGY_ID,
        ]
        assert config.risk == allocation.portfolio
        assert config.strategy_config(PHASE05_CARRY_STRATEGY_ID).risk == allocation.phase05
        assert config.strategy_config(PHASE08_PAIRS_STRATEGY_ID).risk == allocation.phase08
        assert foundation.source.started is False
        assert foundation.source.collector_config.assets == PHASE12_PHASE05_PUBLIC_ASSETS
        assert foundation.source.collector_config.subscription_channels == (
            "activeAssetCtx",
            "bbo",
        )
        assert foundation.source.collector_config.history_lookback_hours == 72
        assert identity["public_only"] is True
        assert identity["pending_bbo_coalescing"].startswith("LATEST_PER_INSTRUMENT")
        assert identity["product_identity_hashes"] == dict(
            PHASE12_PHASE05_PRODUCT_IDENTITY_HASHES
        )
        assert identity["normalized_order_policy"].endswith("CONNECTION_EPOCH_V2")
        assert identity["transport"]["rest_connection_id_policy"] == (
            "SYNCHRONOUS_BOOTSTRAP_RESYNC_PRODUCER_SCOPED_V1"
        )
        assert identity["transport"]["credential_scope"] == "NONE"
        assert identity["transport"]["orders_enabled"] is False
        assert identity["transport"]["execution_routes_present"] is False
        assert identity["transport"]["wallet_or_signer_present"] is False
    finally:
        foundation.source.close()


def test_real_hyperliquid_funding_history_restores_at_exact_hour(
    tmp_path: Path,
) -> None:
    funding_hour, settlement = _realistic_hype_funding_settlement()
    assert settlement.funding_time == funding_hour
    assert settlement.received_at == funding_hour + timedelta(seconds=2)

    foundation = _foundation(tmp_path)
    database = tmp_path / "real-funding-restore.sqlite3"
    engine = PaperEngine(PaperStore(database), foundation.config)
    engine.start()
    initial_source = _OfflinePublicSource(foundation.source.descriptor)
    initial_runtime = PaperRuntime(
        engine,
        foundation.strategies,
        initial_source,
        config=PaperRuntimeConfig(
            timer_interval_seconds=foundation.config.runtime_timer_interval_seconds,
            source_poll_timeout_seconds=foundation.config.runtime_source_poll_timeout_seconds,
        ),
        clock=lambda: _START,
    )
    initial_runtime.start()
    initial_runtime.close()

    engine.post_funding(
        instrument=settlement.instrument,
        amount=Decimal(0),
        occurred_at=settlement.funding_time,
        source_event_id=settlement.event_id,
        funding_rate=settlement.funding_rate,
        funding_interval_seconds=settlement.funding_interval_seconds,
        rate_kind=settlement.rate_kind,
        mark_price=None,
        source_mark_price=settlement.mark_price,
        oracle_price=settlement.oracle_price,
        position_quantity=Decimal(0),
        mark_source="FLAT_NO_MARK",
        source_observation_id=settlement.source_observation_id,
        received_at=settlement.received_at,
        processed_at=settlement.received_at,
        source_activation_cutoff=_START,
    )
    durable = next(
        record
        for record in engine.store.iter_inputs(
            engine.run_id,
            input_type="PUBLIC_FUNDING_SETTLEMENT",
        )
    )
    assert durable.payload["occurred_at"] == utc_text(funding_hour)
    foundation.source.close()

    restarted_foundation = _foundation(tmp_path)
    restarted_source = _OfflinePublicSource(restarted_foundation.source.descriptor)
    restarted = PaperRuntime(
        PaperEngine(PaperStore(database), restarted_foundation.config),
        restarted_foundation.strategies,
        restarted_source,
        config=PaperRuntimeConfig(
            timer_interval_seconds=restarted_foundation.config.runtime_timer_interval_seconds,
            source_poll_timeout_seconds=(restarted_foundation.config.runtime_source_poll_timeout_seconds),
        ),
        clock=lambda: settlement.received_at + timedelta(seconds=1),
    )
    try:
        recovered = next(
            item for item in restarted._durable_public_inputs() if isinstance(item, PublicFundingSettlement)
        )
        assert recovered.funding_time == funding_hour
        startup = restarted.start()
        assert startup.projection.reconciled
        restarted_carry, _restarted_pairs = _real_adapters(restarted_foundation)
        assert restarted_carry.diagnostic_snapshot["status"] == "RESTORED"
    finally:
        restarted.close()
        restarted_foundation.source.close()


def test_malformed_finalized_funding_time_still_fails_closed(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="first 60 seconds of its UTC hour"):
        _realistic_hype_funding_settlement(offset_ms=120_000)

    foundation = _foundation(tmp_path)
    carry, _pairs = _real_adapters(foundation)
    malformed_time = _START + timedelta(hours=1, milliseconds=30)
    malformed = PublicFundingSettlement(
        event_id=canonical_sha256({"funding_time": malformed_time.isoformat(), "instrument": _HYPE_PERP}),
        instrument=_HYPE_PERP,
        funding_time=malformed_time,
        received_at=malformed_time + timedelta(seconds=2),
        funding_rate=Decimal("0.0000125"),
        funding_interval_seconds=3600,
        rate_kind="hyperliquid-hourly-settlement",
        mark_price=None,
        oracle_price=None,
        source_observation_id="malformed-finalized-funding",
    )
    try:
        with pytest.raises(ValueError, match="exact UTC hour"):
            carry.restore_incremental_public_inputs((malformed,))
    finally:
        foundation.source.close()


def test_incremental_restore_failure_releases_cursor_before_failure_persistence(
    tmp_path: Path,
) -> None:
    foundation = _foundation(tmp_path)
    database = tmp_path / "restore-failure-lock.sqlite3"
    engine = PaperEngine(PaperStore(database, timeout_seconds=0.05), foundation.config)
    engine.start()
    initial_runtime = PaperRuntime(
        engine,
        foundation.strategies,
        _OfflinePublicSource(foundation.source.descriptor),
        config=PaperRuntimeConfig(
            timer_interval_seconds=foundation.config.runtime_timer_interval_seconds,
            source_poll_timeout_seconds=foundation.config.runtime_source_poll_timeout_seconds,
        ),
        clock=lambda: _START,
    )
    initial_runtime.start()
    initial_runtime.close()

    malformed_time = _START + timedelta(hours=1, milliseconds=30)
    received_at = malformed_time + timedelta(seconds=2)
    engine.post_funding(
        instrument=_HYPE_PERP,
        amount=Decimal(0),
        occurred_at=malformed_time,
        source_event_id=canonical_sha256({"malformed-funding": malformed_time.isoformat()}),
        funding_rate=Decimal("0.0000125"),
        funding_interval_seconds=3600,
        rate_kind="hyperliquid-hourly-settlement",
        mark_price=None,
        source_mark_price=None,
        oracle_price=None,
        position_quantity=Decimal(0),
        mark_source="FLAT_NO_MARK",
        source_observation_id="malformed-durable-funding",
        received_at=received_at,
        processed_at=received_at,
        source_activation_cutoff=_START,
    )
    engine.reconcile(as_of=received_at + timedelta(seconds=1))
    foundation.source.close()

    restarted_foundation = _foundation(tmp_path)
    runtime = PaperRuntime(
        PaperEngine(
            PaperStore(database, timeout_seconds=0.05),
            restarted_foundation.config,
        ),
        restarted_foundation.strategies,
        _OfflinePublicSource(restarted_foundation.source.descriptor),
        config=PaperRuntimeConfig(
            timer_interval_seconds=restarted_foundation.config.runtime_timer_interval_seconds,
            source_poll_timeout_seconds=(restarted_foundation.config.runtime_source_poll_timeout_seconds),
        ),
        clock=lambda: received_at + timedelta(seconds=2),
    )
    try:
        with pytest.raises(PaperAdmissionError, match="phase05_cash_and_carry") as failure:
            runtime._restore_strategy(runtime.engine.projection(), incremental=True)
        assert "exact UTC hour" in str(failure.value.__cause__)
        persisted = runtime._persist_runtime_failure(
            as_of=received_at + timedelta(seconds=2),
            phase="MARKET_EVALUATION",
            error_type=type(failure.value).__name__,
            failure_key="incremental-restore-fixture",
        )
        assert persisted is not None
        assert persisted.projection.state is PaperState.PAUSED
    finally:
        runtime.close()
        restarted_foundation.source.close()


def test_real_phase05_phase08_same_observation_funding_restart_and_replay(
    tmp_path: Path,
) -> None:
    foundation = _foundation(tmp_path)
    database = tmp_path / "real-portfolio.sqlite3"
    engine = PaperEngine(PaperStore(database), foundation.config)
    engine.start()
    initial_source = _OfflinePublicSource(foundation.source.descriptor)
    initial_runtime = PaperRuntime(
        engine,
        foundation.strategies,
        initial_source,
        config=PaperRuntimeConfig(
            timer_interval_seconds=foundation.config.runtime_timer_interval_seconds,
            source_poll_timeout_seconds=foundation.config.runtime_source_poll_timeout_seconds,
        ),
        clock=lambda: _START,
    )
    initial_runtime.start()
    initial_runtime.close()
    assert initial_source.started
    runner = PortfolioRunner(engine, foundation.strategies)
    carry, pairs = _real_adapters(foundation)

    for hour in range(80):
        if hour:
            _post_flat_funding(engine, carry, hour)
        frame = _carry_frame(hour)
        _warm_frame_without_decision(
            engine,
            foundation,
            frame,
            processed_at=_START + timedelta(hours=hour),
        )

    pair_start = _START + timedelta(hours=79, minutes=40, seconds=20)
    for index in range(38):
        frame = _pair_frame(
            index,
            0.006 * math.sin(index * 0.71),
            pair_start=pair_start,
        )
        _warm_frame_without_decision(
            engine,
            foundation,
            frame,
            processed_at=max(item.received_at for item in frame.values()),
        )
    shock = _pair_frame(38, 0.02, pair_start=pair_start)
    _warm_frame_without_decision(
        engine,
        foundation,
        shock,
        processed_at=max(item.received_at for item in shock.values()),
    )

    _post_flat_funding(engine, carry, 80)
    final_frame = {
        **_carry_frame(80),
        **_pair_frame(
            39,
            0.006 * math.sin(39 * 0.71),
            pair_start=pair_start,
            capture_base=2,
        ),
    }
    shared = runner.process_frame(
        final_frame,
        processed_at=_START + timedelta(hours=80),
    )

    assert [item.strategy_id for item in shared.strategy_results] == [
        PHASE05_CARRY_STRATEGY_ID,
        PHASE08_PAIRS_STRATEGY_ID,
    ]
    assert [item.status for item in shared.strategy_results] == ["DECISION", "DECISION"]
    projection = shared.projection
    assert projection.strategy_projection(PHASE05_CARRY_STRATEGY_ID).decisions == 1
    assert projection.strategy_projection(PHASE08_PAIRS_STRATEGY_ID).decisions == 1
    final_ids = {event.event_id for event in final_frame.values()}
    admitted_final_ids = []
    for record in engine.store.iter_inputs(
        engine.run_id,
        input_type="PUBLIC_MARKET_EVENT",
    ):
        raw_market = record.payload.get("market")
        if isinstance(raw_market, Mapping) and raw_market.get("event_id") in final_ids:
            admitted_final_ids.append(str(raw_market["event_id"]))
    assert sorted(admitted_final_ids) == sorted(final_ids)

    for order in sorted(
        projection.orders.values(),
        key=lambda item: (item.intent.strategy_id or "", item.intent.leg_number),
    ):
        market = final_frame[order.intent.instrument]
        if order.intent.instrument in {_BTC, _ETH}:
            fill = MarketEvent.create(
                received_at=market.received_at + timedelta(milliseconds=500),
                instrument=market.instrument,
                bid_price=market.bid_price,
                ask_price=market.ask_price,
                bid_depth=Decimal("100"),
                ask_depth=Decimal("100"),
                source_sequence=900_000 + order.intent.leg_number,
                capture_ordinal=10 + order.intent.leg_number,
            )
        else:
            assert order.intent.limit_price is not None
            fill = _carry_event(
                order.intent.instrument,
                market.received_at + timedelta(milliseconds=500),
                mid=(market.bid_price + market.ask_price) / Decimal(2),
                capture_ordinal=20 + order.intent.leg_number,
                source_sequence=910_000 + order.intent.leg_number,
                trade_price=order.intent.limit_price,
                trade_quantity=Decimal("100"),
                aggressor_side=(
                    OrderSide.SELL
                    if order.intent.side is OrderSide.BUY
                    else OrderSide.BUY
                ),
            )
        engine.process_market(fill, processed_at=fill.received_at)

    filled = engine.projection()
    assert filled.strategy_projection(PHASE05_CARRY_STRATEGY_ID).state is PaperState.HEDGED
    assert filled.strategy_projection(PHASE08_PAIRS_STRATEGY_ID).state is PaperState.HEDGED
    assert all(item.fees > 0 for item in filled.strategy_projections.values())
    assert filled.fees == sum(item.fees for item in filled.strategy_projections.values())

    funding_at = _START + timedelta(hours=81)
    hype_position = filled.positions[_HYPE_PERP]
    funding_amount = -(hype_position * Decimal("100") * Decimal("0.0002"))
    engine.post_funding(
        instrument=_HYPE_PERP,
        amount=funding_amount,
        occurred_at=funding_at,
        source_event_id=canonical_sha256({"funding": funding_at.isoformat()}),
        funding_rate=Decimal("0.0002"),
        funding_interval_seconds=3600,
        rate_kind="hyperliquid-hourly-settlement",
        mark_price=Decimal("100"),
        source_mark_price=Decimal("100"),
        oracle_price=Decimal("100"),
        position_quantity=hype_position,
        mark_source="PUBLIC_SETTLEMENT_MARK",
        source_observation_id="synthetic-applied-funding-81",
        received_at=funding_at,
        processed_at=funding_at,
    )
    report = build_paper_report(engine.store, engine.run_id)
    strategies = report["strategies"]
    assert isinstance(strategies, Mapping)
    assert strategies[PHASE05_CARRY_STRATEGY_ID]["accounting"]["funding_net"] == decimal_text(funding_amount)
    assert strategies[PHASE08_PAIRS_STRATEGY_ID]["accounting"]["funding_net"] == "0"
    assert report["account"]["funding_net"] == decimal_text(funding_amount)
    assert engine.replay().to_dict() == engine.projection().to_dict()
    assert engine.verify_input_replay().to_dict() == engine.projection().to_dict()
    assert engine.reconcile(as_of=funding_at + timedelta(seconds=1)).projection.reconciled

    original_hashes = {
        PHASE05_CARRY_STRATEGY_ID: carry.diagnostic_snapshot.get("signal_input_hash"),
        PHASE08_PAIRS_STRATEGY_ID: pairs.diagnostic_snapshot.get("signal_input_hash"),
    }
    foundation.source.close()

    restarted_foundation = _foundation(tmp_path)
    offline_source = _OfflinePublicSource(restarted_foundation.source.descriptor)
    runtime = PaperRuntime(
        PaperEngine(PaperStore(database), restarted_foundation.config),
        restarted_foundation.strategies,
        offline_source,
        config=PaperRuntimeConfig(
            timer_interval_seconds=restarted_foundation.config.runtime_timer_interval_seconds,
            source_poll_timeout_seconds=(
                restarted_foundation.config.runtime_source_poll_timeout_seconds
            ),
        ),
        clock=lambda: funding_at + timedelta(seconds=2),
    )
    try:
        startup = runtime.start()
        assert startup.projection.reconciled
        restarted_carry, restarted_pairs = _real_adapters(restarted_foundation)
        assert restarted_carry.diagnostic_snapshot["status"] == "RESTORED"
        assert restarted_pairs.diagnostic_snapshot["status"] == "RESTORED"
        assert restarted_carry.diagnostic_snapshot.get("signal_input_hash") == original_hashes[
            PHASE05_CARRY_STRATEGY_ID
        ]
        assert restarted_pairs.diagnostic_snapshot.get("signal_input_hash") == original_hashes[
            PHASE08_PAIRS_STRATEGY_ID
        ]
        assert offline_source.started
    finally:
        runtime.close()
        restarted_foundation.source.close()

    store = PaperStore(database, initialize=False)
    projection = store.get_projection(foundation.config.run_id)
    replay = replay_paper_run(store, foundation.config.run_id)
    assert replay.projection_hash == projection.canonical_hash
    assert replay.to_dict()["status"] == "REPLAY_EXACT"


def test_phase08_proportional_terminal_partial_iocs_are_hedged_without_timeout(
    tmp_path: Path,
) -> None:
    foundation = _foundation(tmp_path)
    engine = PaperEngine(PaperStore(tmp_path / "proportional-partials.sqlite3"), foundation.config)
    engine.start()
    strategy = foundation.config.strategy_config(PHASE08_PAIRS_STRATEGY_ID)
    decided_at = _START + timedelta(seconds=1)

    def market(instrument: str, ordinal: int) -> MarketEvent:
        return MarketEvent.create(
            received_at=decided_at,
            instrument=instrument,
            bid_price=Decimal("1"),
            ask_price=Decimal("1"),
            bid_depth=Decimal("1"),
            ask_depth=Decimal("1"),
            source_sequence=800_000 + ordinal,
            capture_ordinal=ordinal,
        )

    markets = {_ETH: market(_ETH, 1), _BTC: market(_BTC, 2)}
    anchor = markets[_BTC]
    signal = {"fixture": "proportional-terminal-partial-iocs"}
    decision_id = DecisionIntent.identifier(
        run_id=foundation.config.run_id,
        strategy_id=strategy.strategy_id,
        market_event_id=anchor.event_id,
        action=DecisionAction.ENTRY,
        ordinal=0,
        signal=signal,
    )
    orders = (
        OrderIntent.create(
            decision_id=decision_id,
            run_id=foundation.config.run_id,
            strategy_id=strategy.strategy_id,
            instrument=_ETH,
            side=OrderSide.BUY,
            quantity=Decimal("0.2"),
            order_type=PaperOrderType.TAKER,
            time_in_force=TimeInForce.IOC,
            created_at=decided_at,
            ordinal=0,
            hedge_group_id="phase08-proportional-partial-fixture",
            leg_number=1,
        ),
        OrderIntent.create(
            decision_id=decision_id,
            run_id=foundation.config.run_id,
            strategy_id=strategy.strategy_id,
            instrument=_BTC,
            side=OrderSide.SELL,
            quantity=Decimal("0.01"),
            order_type=PaperOrderType.TAKER,
            time_in_force=TimeInForce.IOC,
            created_at=decided_at,
            ordinal=1,
            hedge_group_id="phase08-proportional-partial-fixture",
            leg_number=2,
        ),
    )
    decision = DecisionIntent(
        decision_id=decision_id,
        run_id=foundation.config.run_id,
        strategy_id=strategy.strategy_id,
        strategy_name=strategy.strategy_name,
        strategy_hash=strategy.strategy_hash,
        strategy_config_hash=strategy.strategy_config_hash,
        action=DecisionAction.ENTRY,
        decided_at=decided_at,
        received_at=decided_at,
        market_event_id=anchor.event_id,
        observed_event_ids=tuple(item.event_id for item in markets.values()),
        orders=orders,
        ordinal=0,
        signal=signal,
    )
    engine.process_market(markets[_ETH])
    engine.process_market(markets[_BTC])
    engine.submit_decision(decision, markets)

    def fill_market(instrument: str, ordinal: int, offset_seconds: int) -> MarketEvent:
        return MarketEvent.create(
            received_at=decided_at + timedelta(seconds=offset_seconds),
            instrument=instrument,
            bid_price=Decimal("1"),
            ask_price=Decimal("1"),
            bid_depth=Decimal("1"),
            ask_depth=Decimal("1"),
            source_sequence=800_000 + ordinal,
            capture_ordinal=ordinal,
        )

    engine.process_market(fill_market(_ETH, 3, 1))
    filled = engine.process_market(fill_market(_BTC, 4, 2)).projection
    owned = filled.strategy_projection(PHASE08_PAIRS_STRATEGY_ID)

    assert [filled.orders[order.order_id].filled_quantity for order in orders] == [
        Decimal("0.1"),
        Decimal("0.005"),
    ]
    assert owned.state is PaperState.HEDGED
    after_timeout = engine.process_timer(as_of=decided_at + timedelta(seconds=30)).projection
    assert after_timeout.strategy_projection(PHASE08_PAIRS_STRATEGY_ID).state is PaperState.HEDGED
    assert not any(alert.code == "UNHEDGED_TIMEOUT" for alert in engine.store.get_alerts(engine.run_id))
    assert engine.replay().to_dict() == after_timeout.to_dict()
    foundation.source.close()


def test_phase08_second_ioc_waits_for_earlier_leg_fill_before_hedging(
    tmp_path: Path,
) -> None:
    """A later IOC leg must not NO_FILL before an earlier sibling has had a fill."""

    foundation = _foundation(tmp_path)
    engine = PaperEngine(
        PaperStore(tmp_path / "phase08-delayed-hedge-sequencing.sqlite3"),
        foundation.config,
    )
    engine.start()

    strategy = foundation.config.strategy_config(PHASE08_PAIRS_STRATEGY_ID)
    decided_at = _START + timedelta(seconds=1)

    def market(
        instrument: str,
        ordinal: int,
        *,
        offset_ms: int = 0,
        depth: str = "1",
    ) -> MarketEvent:
        return MarketEvent.create(
            received_at=decided_at + timedelta(milliseconds=offset_ms),
            instrument=instrument,
            bid_price=Decimal("1"),
            ask_price=Decimal("1"),
            bid_depth=Decimal(depth),
            ask_depth=Decimal(depth),
            source_sequence=900_000 + ordinal,
            capture_ordinal=ordinal,
        )

    initial_markets = {
        _ETH: market(_ETH, 1),
        _BTC: market(_BTC, 2),
    }
    anchor = initial_markets[_BTC]
    signal = {"fixture": "phase08-delayed-hedge-sequencing"}

    decision_id = DecisionIntent.identifier(
        run_id=foundation.config.run_id,
        strategy_id=strategy.strategy_id,
        market_event_id=anchor.event_id,
        action=DecisionAction.ENTRY,
        ordinal=0,
        signal=signal,
    )

    eth_order = OrderIntent.create(
        decision_id=decision_id,
        run_id=foundation.config.run_id,
        strategy_id=strategy.strategy_id,
        instrument=_ETH,
        side=OrderSide.SELL,
        quantity=Decimal("0.036"),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.IOC,
        created_at=decided_at,
        ordinal=0,
        hedge_group_id="phase08-delayed-hedge-fixture",
        leg_number=1,
    )
    btc_order = OrderIntent.create(
        decision_id=decision_id,
        run_id=foundation.config.run_id,
        strategy_id=strategy.strategy_id,
        instrument=_BTC,
        side=OrderSide.BUY,
        quantity=Decimal("0.0024"),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.IOC,
        created_at=decided_at,
        ordinal=1,
        hedge_group_id="phase08-delayed-hedge-fixture",
        leg_number=2,
    )

    decision = DecisionIntent(
        decision_id=decision_id,
        run_id=foundation.config.run_id,
        strategy_id=strategy.strategy_id,
        strategy_name=strategy.strategy_name,
        strategy_hash=strategy.strategy_hash,
        strategy_config_hash=strategy.strategy_config_hash,
        action=DecisionAction.ENTRY,
        decided_at=decided_at,
        received_at=decided_at,
        market_event_id=anchor.event_id,
        observed_event_ids=tuple(item.event_id for item in initial_markets.values()),
        orders=(eth_order, btc_order),
        ordinal=0,
        signal=signal,
    )

    engine.process_market(initial_markets[_ETH])
    engine.process_market(initial_markets[_BTC])
    engine.submit_decision(decision, initial_markets)

    # Reproduce the live ordering:
    # BTC gets a fresh executable BBO before ETH leg 1 has produced a durable fill.
    btc_early = engine.process_market(
        market(_BTC, 3, offset_ms=100)
    ).projection

    assert btc_early.orders[eth_order.order_id].status is OrderStatus.ACKED
    assert btc_early.orders[eth_order.order_id].filled_quantity == Decimal("0")
    assert btc_early.orders[btc_order.order_id].status is OrderStatus.ACKED
    assert btc_early.orders[btc_order.order_id].filled_quantity == Decimal("0")

    # Critically: the second IOC was deferred, not terminalized as NO_FILL.
    assert not any(
        alert.code == "UNHEDGED_TIMEOUT"
        for alert in engine.store.get_alerts(engine.run_id)
    )

    # Leg 1 now fills completely.
    eth_filled = engine.process_market(
        market(_ETH, 4, offset_ms=200)
    ).projection

    owned = eth_filled.strategy_projection(PHASE08_PAIRS_STRATEGY_ID)

    assert eth_filled.orders[eth_order.order_id].status is OrderStatus.FILLED
    assert eth_filled.orders[eth_order.order_id].filled_quantity == Decimal("0.036")
    assert eth_filled.orders[btc_order.order_id].status is OrderStatus.ACKED

    # This assertion also locks the reducer bug found from the live snapshot:
    # a real strategy-level unhedged position must propagate to the portfolio.
    assert owned.state is PaperState.HEDGE_PENDING
    assert owned.positions == {_ETH: Decimal("-0.036")}
    assert eth_filled.state is PaperState.HEDGE_PENDING

    # A subsequent BTC BBO may now match leg 2 using the durable leg-1 fill
    # to derive its hedge quantity cap.
    hedged = engine.process_market(
        market(_BTC, 5, offset_ms=300)
    ).projection

    owned = hedged.strategy_projection(PHASE08_PAIRS_STRATEGY_ID)

    assert hedged.orders[btc_order.order_id].status is OrderStatus.FILLED
    assert hedged.orders[btc_order.order_id].filled_quantity == Decimal("0.0024")
    assert owned.state is PaperState.HEDGED
    assert hedged.state is PaperState.HEDGED

    # Crossing the frozen timeout must not create a false unhedged incident.
    after_timeout = engine.process_timer(
        as_of=decided_at + timedelta(seconds=30)
    ).projection

    assert (
        after_timeout.strategy_projection(PHASE08_PAIRS_STRATEGY_ID).state
        is PaperState.HEDGED
    )
    assert not any(
        alert.code == "UNHEDGED_TIMEOUT"
        for alert in engine.store.get_alerts(engine.run_id)
    )

    # Event-sourced determinism remains exact.
    assert engine.replay().to_dict() == after_timeout.to_dict()

    foundation.source.close()
