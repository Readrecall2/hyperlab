from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import hyperlab.paper.models as paper_models
from hyperlab.backtest.costs import SlippageModel
from hyperlab.backtest.execution import MakerFillModel
from hyperlab.backtest.protocol import canonical_json, canonical_sha256
from hyperlab.paper import (
    AlertSeverity,
    DecisionAction,
    DecisionIntent,
    MarketEvent,
    OrderIntent,
    OrderSide,
    OrderStatus,
    PaperEngine,
    PaperEvent,
    PaperEventType,
    PaperExecutionConfig,
    PaperGateEvidence,
    PaperOrderType,
    PaperProjection,
    PaperRiskLimits,
    PaperRunConfig,
    PaperState,
    PaperStore,
    TimeInForce,
    deterministic_id,
    evaluate_paper_gate,
)
from hyperlab.paper.models import PaperOrder, StoredPaperEvent, utc_text
from hyperlab.paper.reducer import apply_event, apply_stored_event, replay_projection
from hyperlab.paper.reporting import build_paper_report

_START = datetime(2026, 8, 17, 12, tzinfo=UTC)
_INSTRUMENT = "HYPERLIQUID:BTC:perp"
_RUN_ID = deterministic_id("paper_projection_compaction_run")
_CONFIG_HASH = deterministic_id("paper_projection_compaction_config")


def _decision_id(
    *,
    run_id: str,
    label: str,
    action: DecisionAction = DecisionAction.ENTRY,
) -> tuple[str, str]:
    market_event_id = deterministic_id("paper_projection_compaction_market", label)
    return (
        DecisionIntent.identifier(
            run_id=run_id,
            market_event_id=market_event_id,
            action=action,
            ordinal=0,
        ),
        market_event_id,
    )


def _order_intent(
    *,
    run_id: str,
    decision_id: str,
    created_at: datetime,
    ordinal: int,
    action: DecisionAction = DecisionAction.ENTRY,
) -> OrderIntent:
    return OrderIntent.create(
        decision_id=decision_id,
        run_id=run_id,
        instrument=_INSTRUMENT,
        side=OrderSide.BUY if action is DecisionAction.ENTRY else OrderSide.SELL,
        quantity=Decimal(1),
        order_type=PaperOrderType.TAKER,
        time_in_force=TimeInForce.GTC,
        created_at=created_at,
        ordinal=ordinal,
        reduce_only=action is DecisionAction.EXIT,
    )


def _event(
    *,
    run_id: str,
    event_type: PaperEventType,
    at: datetime,
    correlation_id: str,
    payload: dict[str, object],
    causation_id: str | None = None,
    ordinal: int = 0,
) -> PaperEvent:
    return PaperEvent.create(
        run_id=run_id,
        event_type=event_type,
        occurred_at=at,
        received_at=at,
        causation_id=causation_id,
        correlation_id=correlation_id,
        payload=payload,
        ordinal=ordinal,
    )


def _stored_event(
    event: PaperEvent,
    *,
    sequence: int,
    previous_event_hash: str,
) -> StoredPaperEvent:
    event_hash = canonical_sha256(
        {
            **event.unsigned_dict(),
            "previous_event_hash": previous_event_hash,
            "sequence": sequence,
        }
    )
    return StoredPaperEvent(
        event=event,
        sequence=sequence,
        previous_event_hash=previous_event_hash,
        event_hash=event_hash,
    )


def test_projection_v3_round_trip_preserves_new_runtime_fields_and_legacy_incidents() -> None:
    projection = PaperProjection(
        run_id=_RUN_ID,
        config_hash=_CONFIG_HASH,
        initial_cash=Decimal("1000"),
        archived_order_count=7,
        critical_incident_count=2,
        last_critical_incident_at=_START,
        marks={_INSTRUMENT: Decimal("100.5")},
        public_bbo_mids={_INSTRUMENT: Decimal("100.5")},
        public_bbo_received_at_by_instrument={_INSTRUMENT: _START},
        last_received_at=_START,
        last_public_source_received_at=_START,
        last_market_received_at=_START,
        last_market_received_at_by_instrument={_INSTRUMENT: _START},
    )

    payload = projection.to_dict()

    assert payload["schema_version"] == 3
    assert "critical_incidents" not in payload
    assert PaperProjection.from_dict(payload).to_dict() == payload

    legacy = dict(payload)
    legacy["schema_version"] = 1
    legacy.pop("archived_order_count")
    legacy.pop("critical_incident_count")
    legacy.pop("last_critical_incident_at")
    for field_name in (
        "runtime_session_generation",
        "runtime_session_id",
        "runtime_session_started_at",
        "runtime_session_stopped_at",
    ):
        legacy.pop(field_name)
    legacy["critical_incidents"] = [
        utc_text(_START - timedelta(days=1)),
        utc_text(_START),
    ]

    parsed_legacy = PaperProjection.from_dict(legacy)
    assert parsed_legacy.archived_order_count == 0
    assert parsed_legacy.critical_incident_count == 2
    assert parsed_legacy.last_critical_incident_at == _START


def test_schema_v2_config_parse_is_explicit_and_does_not_resolve_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _engine_config().to_dict()

    def unexpected_release_lookup() -> str:
        raise AssertionError("explicit schema-v2 parsing must not inspect the checkout")

    monkeypatch.setattr(
        paper_models,
        "_current_release_code_sha256",
        unexpected_release_lookup,
    )

    parsed = PaperRunConfig.from_dict(payload)

    assert parsed.to_dict() == payload
    for field_name in (
        "release_code_sha256",
        "runtime_source_poll_timeout_seconds",
        "runtime_timer_interval_seconds",
    ):
        incomplete = dict(payload)
        incomplete.pop(field_name)
        with pytest.raises(ValueError, match=field_name):
            PaperRunConfig.from_dict(incomplete)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"archived_order_count": -1}, "archived_order_count"),
        ({"archived_order_count": True}, "archived_order_count"),
        ({"critical_incident_count": -1}, "critical_incident_count"),
        (
            {"critical_incident_count": 1, "last_critical_incident_at": None},
            "last_critical_incident_at",
        ),
        (
            {"critical_incident_count": 0, "last_critical_incident_at": _START},
            "last_critical_incident_at",
        ),
    ],
)
def test_projection_rejects_invalid_bounded_summary_state(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        PaperProjection(
            run_id=_RUN_ID,
            config_hash=_CONFIG_HASH,
            initial_cash=Decimal("1000"),
            **kwargs,
        )


def test_order_planning_archives_only_unrelated_terminal_orders_atomically() -> None:
    old_decision_id, _ = _decision_id(run_id=_RUN_ID, label="old")
    active_decision_id, _ = _decision_id(run_id=_RUN_ID, label="active")
    entry_decision_id, entry_market_id = _decision_id(run_id=_RUN_ID, label="entry")
    exit_decision_id, _ = _decision_id(
        run_id=_RUN_ID,
        label="exit",
        action=DecisionAction.EXIT,
    )
    old_terminal = _order_intent(
        run_id=_RUN_ID,
        decision_id=old_decision_id,
        created_at=_START,
        ordinal=0,
    )
    old_active = _order_intent(
        run_id=_RUN_ID,
        decision_id=active_decision_id,
        created_at=_START,
        ordinal=0,
    )
    current_entry_terminal = _order_intent(
        run_id=_RUN_ID,
        decision_id=entry_decision_id,
        created_at=_START,
        ordinal=0,
    )
    current_exit_terminal = _order_intent(
        run_id=_RUN_ID,
        decision_id=exit_decision_id,
        created_at=_START,
        ordinal=0,
        action=DecisionAction.EXIT,
    )
    incoming = _order_intent(
        run_id=_RUN_ID,
        decision_id=entry_decision_id,
        created_at=_START,
        ordinal=1,
    )
    projection = PaperProjection(
        run_id=_RUN_ID,
        config_hash=_CONFIG_HASH,
        initial_cash=Decimal("1000"),
        orders={
            old_terminal.order_id: PaperOrder(
                intent=old_terminal,
                action=DecisionAction.ENTRY,
                status=OrderStatus.REJECTED,
            ),
            old_active.order_id: PaperOrder(
                intent=old_active,
                action=DecisionAction.ENTRY,
                status=OrderStatus.ACKED,
            ),
            current_entry_terminal.order_id: PaperOrder(
                intent=current_entry_terminal,
                action=DecisionAction.ENTRY,
                status=OrderStatus.FILLED,
                filled_quantity=Decimal(1),
                average_fill_price=Decimal(100),
            ),
            current_exit_terminal.order_id: PaperOrder(
                intent=current_exit_terminal,
                action=DecisionAction.EXIT,
                status=OrderStatus.CANCELLED,
            ),
        },
        archived_order_count=5,
        current_entry_decision_id=entry_decision_id,
        current_exit_decision_id=exit_decision_id,
    )
    planned = _event(
        run_id=_RUN_ID,
        event_type=PaperEventType.ORDER_PLANNED,
        at=_START,
        causation_id=entry_market_id,
        correlation_id=entry_decision_id,
        payload={"action": DecisionAction.ENTRY.value, "order": incoming.to_dict()},
    )

    apply_event(projection, planned)

    assert set(projection.orders) == {
        old_active.order_id,
        current_entry_terminal.order_id,
        current_exit_terminal.order_id,
        incoming.order_id,
    }
    assert projection.archived_order_count == 6

    another_old_decision, _ = _decision_id(run_id=_RUN_ID, label="another-old")
    another_old = _order_intent(
        run_id=_RUN_ID,
        decision_id=another_old_decision,
        created_at=_START,
        ordinal=0,
    )
    projection.orders[another_old.order_id] = PaperOrder(
        intent=another_old,
        action=DecisionAction.ENTRY,
        status=OrderStatus.REJECTED,
    )
    before_duplicate = projection.to_dict()

    with pytest.raises(ValueError, match="duplicates an existing order"):
        apply_event(projection, planned)

    assert projection.to_dict() == before_duplicate

    foreign_decision_id, foreign_market_id = _decision_id(
        run_id=_RUN_ID,
        label="foreign",
    )
    foreign = _order_intent(
        run_id=_RUN_ID,
        decision_id=foreign_decision_id,
        created_at=_START,
        ordinal=0,
    )
    foreign_planned = _event(
        run_id=_RUN_ID,
        event_type=PaperEventType.ORDER_PLANNED,
        at=_START,
        causation_id=foreign_market_id,
        correlation_id=foreign_decision_id,
        payload={"action": DecisionAction.ENTRY.value, "order": foreign.to_dict()},
    )
    before_foreign = projection.to_dict()

    with pytest.raises(ValueError, match="current ENTRY decision"):
        apply_event(projection, foreign_planned)

    assert projection.to_dict() == before_foreign


def test_large_reducer_replay_keeps_projection_cardinality_and_bytes_bounded() -> None:
    cycles = 1_500
    projection = PaperProjection(
        run_id=_RUN_ID,
        config_hash=_CONFIG_HASH,
        initial_cash=Decimal("1000"),
    )
    previous_event_hash = projection.last_event_hash
    assert previous_event_hash is not None
    stored_events: list[StoredPaperEvent] = []
    sequence = 0
    early_size = 0
    last_incident_at: datetime | None = None

    warning = _event(
        run_id=_RUN_ID,
        event_type=PaperEventType.ALERT_RAISED,
        at=_START + timedelta(microseconds=1),
        correlation_id=deterministic_id("projection_compaction_warning"),
        payload={"severity": AlertSeverity.WARNING.value},
    )
    sequence += 1
    stored = _stored_event(
        warning,
        sequence=sequence,
        previous_event_hash=previous_event_hash,
    )
    stored_events.append(stored)
    apply_stored_event(projection, stored)
    previous_event_hash = stored.event_hash
    assert projection.critical_incident_count == 0

    for cycle in range(cycles):
        decision_at = _START + timedelta(microseconds=sequence + 1)
        decision_id, market_event_id = _decision_id(
            run_id=_RUN_ID,
            label=f"cycle-{cycle}",
        )
        order = _order_intent(
            run_id=_RUN_ID,
            decision_id=decision_id,
            created_at=decision_at,
            ordinal=0,
        )
        decision = DecisionIntent(
            decision_id=decision_id,
            run_id=_RUN_ID,
            strategy_name="projection_compaction_fixture",
            action=DecisionAction.ENTRY,
            decided_at=decision_at,
            received_at=decision_at,
            market_event_id=market_event_id,
            observed_event_ids=(market_event_id,),
            orders=(order,),
        )
        events = (
            _event(
                run_id=_RUN_ID,
                event_type=PaperEventType.DECISION_RECORDED,
                at=decision_at,
                causation_id=market_event_id,
                correlation_id=decision_id,
                payload={"decision": decision.to_dict()},
            ),
            _event(
                run_id=_RUN_ID,
                event_type=PaperEventType.ORDER_PLANNED,
                at=_START + timedelta(microseconds=sequence + 2),
                causation_id=market_event_id,
                correlation_id=decision_id,
                payload={
                    "action": DecisionAction.ENTRY.value,
                    "order": order.to_dict(),
                },
            ),
            _event(
                run_id=_RUN_ID,
                event_type=PaperEventType.RISK_REJECTED,
                at=_START + timedelta(microseconds=sequence + 3),
                causation_id=market_event_id,
                correlation_id=decision_id,
                payload={"order_id": order.order_id},
            ),
            _event(
                run_id=_RUN_ID,
                event_type=PaperEventType.CYCLE_COMPLETED,
                at=_START + timedelta(microseconds=sequence + 4),
                causation_id=market_event_id,
                correlation_id=decision_id,
                payload={
                    "completed_at": utc_text(
                        _START + timedelta(microseconds=sequence + 4)
                    )
                },
            ),
            _event(
                run_id=_RUN_ID,
                event_type=PaperEventType.ALERT_RAISED,
                at=_START + timedelta(microseconds=sequence + 5),
                causation_id=market_event_id,
                correlation_id=decision_id,
                payload={"severity": AlertSeverity.CRITICAL.value},
            ),
        )
        for event in events:
            sequence += 1
            stored = _stored_event(
                event,
                sequence=sequence,
                previous_event_hash=previous_event_hash,
            )
            stored_events.append(stored)
            apply_stored_event(projection, stored)
            previous_event_hash = stored.event_hash
            if event.event_type is PaperEventType.ALERT_RAISED:
                last_incident_at = event.received_at
        if cycle == 9:
            early_size = len(canonical_json(projection.to_dict()))

    replayed = replay_projection(
        run_id=_RUN_ID,
        config_hash=_CONFIG_HASH,
        initial_cash=Decimal("1000"),
        events=tuple(stored_events),
    )
    final_payload = projection.to_dict()

    assert replayed.to_dict() == final_payload
    assert projection.archived_order_count == cycles - 1
    assert len(projection.orders) == 1
    assert projection.archived_order_count + len(projection.orders) == cycles
    assert projection.critical_incident_count == cycles
    assert projection.last_critical_incident_at == last_incident_at
    assert "critical_incidents" not in final_payload
    assert len(canonical_json(final_payload)) - early_size < 128


def _engine_config() -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="projection_compaction_fixture",
        strategy_hash=deterministic_id("projection_compaction_strategy"),
        parameters={"synthetic_fixture": True},
        data_hash=deterministic_id("projection_compaction_data"),
        execution=PaperExecutionConfig(
            maker_fill=MakerFillModel(
                base_probability=1.0,
                participation_decay=0.0,
                calibration_id="projection-compaction-fixture",
                calibration_status="SYNTHETIC",
            ),
            slippage=SlippageModel(max_participation=1.0),
            maker_fee_bps=Decimal("1"),
            taker_fee_bps=Decimal("2"),
            calibration_status="SYNTHETIC",
            source="synthetic-projection-compaction-fixture",
        ),
        risk=PaperRiskLimits(),
        seed=17,
        initial_cash=Decimal("100000"),
        validation_started_at=_START,
        run_kind="DEMO",
        data_calibration_status="SYNTHETIC",
        data_source="synthetic-projection-compaction-fixture",
    )


def _market(label: str, at: datetime, *, source_sequence: int) -> MarketEvent:
    return MarketEvent.create(
        received_at=at,
        instrument=_INSTRUMENT,
        bid_price=Decimal("100"),
        ask_price=Decimal("101"),
        bid_depth=Decimal("100"),
        ask_depth=Decimal("100"),
        source_sequence=source_sequence,
    )


def _engine_decision(
    config: PaperRunConfig,
    market: MarketEvent,
    *,
    action: DecisionAction,
) -> DecisionIntent:
    decision_id = DecisionIntent.identifier(
        run_id=config.run_id,
        market_event_id=market.event_id,
        action=action,
        ordinal=0,
    )
    order = _order_intent(
        run_id=config.run_id,
        decision_id=decision_id,
        created_at=market.received_at,
        ordinal=0,
        action=action,
    )
    return DecisionIntent(
        decision_id=decision_id,
        run_id=config.run_id,
        strategy_name=config.strategy_name,
        action=action,
        decided_at=market.received_at,
        received_at=market.received_at,
        market_event_id=market.event_id,
        observed_event_ids=(market.event_id,),
        orders=(order,),
    )


def test_compacted_engine_projection_preserves_replay_ledger_gate_and_report(
    tmp_path: Path,
) -> None:
    config = _engine_config()
    store = PaperStore(tmp_path / "paper.sqlite3")
    engine = PaperEngine(store, config)
    engine.start()
    source_sequence = 0
    last_at = _START

    for cycle in range(4):
        entry_at = _START + timedelta(seconds=(cycle * 4) + 1)
        source_sequence += 1
        entry_market = _market(
            f"entry-{cycle}",
            entry_at,
            source_sequence=source_sequence,
        )
        engine.submit_decision(
            _engine_decision(config, entry_market, action=DecisionAction.ENTRY),
            entry_market,
        )
        source_sequence += 1
        engine.process_market(
            _market(
                f"entry-fill-{cycle}",
                entry_at + timedelta(seconds=1),
                source_sequence=source_sequence,
            )
        )

        exit_at = entry_at + timedelta(seconds=2)
        source_sequence += 1
        exit_market = _market(
            f"exit-{cycle}",
            exit_at,
            source_sequence=source_sequence,
        )
        engine.submit_decision(
            _engine_decision(config, exit_market, action=DecisionAction.EXIT),
            exit_market,
        )
        source_sequence += 1
        completed = engine.process_market(
            _market(
                f"exit-fill-{cycle}",
                exit_at + timedelta(seconds=1),
                source_sequence=source_sequence,
            )
        )
        assert completed.projection.state is PaperState.FLAT
        last_at = exit_at + timedelta(seconds=1)

    projection = engine.reconcile(as_of=last_at + timedelta(seconds=1)).projection
    assert projection.positions == {}
    assert projection.cash == config.initial_cash + projection.realized_pnl
    assert projection.archived_order_count == 6
    assert len(projection.orders) == 2
    assert projection.archived_order_count + len(projection.orders) == 8

    transaction_balances: defaultdict[str, Decimal] = defaultdict(Decimal)
    for entry in store.iter_ledger_entries(config.run_id):
        transaction_balances[entry.transaction_id] += entry.amount
    assert transaction_balances
    assert set(transaction_balances.values()) == {Decimal(0)}
    assert engine.replay().to_dict() == projection.to_dict()
    assert store.verify_integrity(config.run_id).ok is True

    killed_at = last_at + timedelta(seconds=2)
    killed = engine.kill(
        as_of=killed_at,
        reason="synthetic bounded incident fixture",
        operator_artifact_hash=deterministic_id("projection_compaction_kill_artifact"),
    ).projection
    assert killed.critical_incident_count == 1
    assert killed.last_critical_incident_at == killed_at
    assert engine.replay().to_dict() == killed.to_dict()
    assert engine.verify_input_replay().to_dict() == killed.to_dict()
    assert store.verify_integrity(config.run_id).ok is True

    report = build_paper_report(store, config.run_id)
    assert report["account"]["archived_order_count"] == 6
    assert report["risk"]["critical_incident_count"] == 1
    assert report["risk"]["last_critical_incident_at"] == utc_text(killed_at)

    gate = evaluate_paper_gate(
        store,
        config.run_id,
        PaperGateEvidence(as_of=killed_at + timedelta(seconds=1)),
    )
    assert gate.snapshot.critical_incident_count == 1
    assert gate.snapshot.incident_free_days_completed == 0
    assert gate.checks["incident_free_14_days"] is False
