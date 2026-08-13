from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import cast

from hyperlab.paper.models import (
    AlertSeverity,
    OrderSide,
    OrderStatus,
    PaperEvent,
    PaperEventType,
    PaperOrder,
    PaperProjection,
    PaperState,
    StoredPaperEvent,
    decimal_value,
    parse_utc,
    require_transition,
)


def _payload_string(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _order(projection: PaperProjection, event: PaperEvent) -> PaperOrder:
    order_id = _payload_string(event.payload, "order_id")
    try:
        return projection.orders[order_id]
    except KeyError as error:
        raise ValueError(
            f"event {cast(PaperEventType, event.event_type).value} references unknown order {order_id}"
        ) from error


def _transition(projection: PaperProjection, event: PaperEvent) -> None:
    source = PaperState(_payload_string(event.payload, "source"))
    target = PaperState(_payload_string(event.payload, "target"))
    if projection.state is not source:
        raise ValueError(
            f"state transition expected {source.value}, projection is {projection.state.value}"
        )
    require_transition(source, target)
    if target in {
        PaperState.PAUSED,
        PaperState.REDUCE_ONLY,
        PaperState.MANUAL_REVIEW,
        PaperState.EMERGENCY_FLATTEN,
    } and source not in {
        PaperState.PAUSED,
        PaperState.REDUCE_ONLY,
        PaperState.MANUAL_REVIEW,
        PaperState.EMERGENCY_FLATTEN,
    }:
        projection.suspended_from = source
    elif target not in {
        PaperState.PAUSED,
        PaperState.REDUCE_ONLY,
        PaperState.MANUAL_REVIEW,
        PaperState.EMERGENCY_FLATTEN,
    }:
        projection.suspended_from = None
    projection.state = target
    projection.state_since = event.received_at


def _apply_fill(projection: PaperProjection, event: PaperEvent, *, full: bool) -> None:
    order = _order(projection, event)
    if order.status not in {
        OrderStatus.ACKED,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.CANCEL_PENDING,
    }:
        raise ValueError(f"cannot fill order in status {order.status.value}")
    prior_status = order.status
    fill_quantity = decimal_value(
        _payload_string(event.payload, "fill_quantity"),
        label="fill_quantity",
        positive=True,
    )
    price = decimal_value(
        _payload_string(event.payload, "fill_price"),
        label="fill_price",
        positive=True,
    )
    fee = decimal_value(
        _payload_string(event.payload, "fee"),
        label="fill fee",
    )
    if fill_quantity > order.remaining_quantity:
        raise ValueError("fill quantity exceeds the open order remainder")
    if full and fill_quantity != order.remaining_quantity:
        raise ValueError("ORDER_FILLED must consume the entire open remainder")
    if not full and fill_quantity >= order.remaining_quantity:
        raise ValueError("ORDER_PARTIALLY_FILLED must leave a positive remainder")

    prior_filled = order.filled_quantity
    new_filled = prior_filled + fill_quantity
    if order.average_fill_price is None:
        average = price
    else:
        average = (order.average_fill_price * prior_filled + price * fill_quantity) / new_filled
    order.filled_quantity = new_filled
    order.average_fill_price = average
    order.fees += fee
    order.fill_attempts += 1
    order.status = (
        OrderStatus.FILLED
        if full
        else (
            OrderStatus.CANCEL_PENDING
            if prior_status is OrderStatus.CANCEL_PENDING
            else OrderStatus.PARTIALLY_FILLED
        )
    )

    instrument = order.intent.instrument
    fill_signed = cast(OrderSide, order.intent.side).sign * fill_quantity
    prior_quantity = projection.positions.get(instrument, Decimal(0))
    prior_basis = projection.cost_basis.get(instrument, Decimal(0))
    prior_inventory_value = projection.inventory_value.get(instrument, Decimal(0))
    new_quantity = prior_quantity + fill_signed
    realized = Decimal(0)
    inventory_change = fill_signed * price

    if prior_quantity == 0 or (prior_quantity > 0) == (fill_signed > 0):
        new_inventory_value = prior_inventory_value + inventory_change
        new_basis = abs(new_inventory_value / new_quantity)
    else:
        closed = min(abs(prior_quantity), abs(fill_signed))
        current_sign = Decimal(1) if prior_quantity > 0 else Decimal(-1)
        cash_close = current_sign * closed * price
        inventory_close = (
            -prior_inventory_value
            if closed == abs(prior_quantity)
            else -current_sign * closed * prior_basis
        )
        realized = cash_close + inventory_close
        opening = abs(fill_signed) - closed
        opening_change = (
            (Decimal(1) if fill_signed > 0 else Decimal(-1)) * opening * price
        )
        new_inventory_value = prior_inventory_value + inventory_close + opening_change
        new_basis = (
            Decimal(0) if new_quantity == 0 else abs(new_inventory_value / new_quantity)
        )

    projection.cash -= fill_signed * price + fee
    projection.fees += fee
    projection.realized_pnl += realized - fee
    if new_quantity == 0:
        projection.positions.pop(instrument, None)
        projection.cost_basis.pop(instrument, None)
        projection.inventory_value.pop(instrument, None)
    else:
        projection.positions[instrument] = new_quantity
        projection.cost_basis[instrument] = new_basis
        projection.inventory_value[instrument] = new_inventory_value
    projection.marks[instrument] = price
    projection.peak_equity = max(projection.peak_equity, projection.equity)


def apply_event(projection: PaperProjection, event: PaperEvent) -> PaperProjection:
    """Apply one validated domain event without I/O or implicit time."""

    if event.run_id != projection.run_id:
        raise ValueError("paper event belongs to another run")
    event_type = event.event_type
    if projection.last_received_at is not None and event.received_at < projection.last_received_at:
        raise ValueError("paper events must be ordered by non-decreasing received_at")
    # Validate reduce-only fills before *any* projection mutation (including a
    # daily-session rollover).  A forged or stale fill must fail closed without
    # leaving a partially mutated in-memory projection.
    if event_type in {
        PaperEventType.ORDER_PARTIALLY_FILLED,
        PaperEventType.ORDER_FILLED,
    }:
        order = _order(projection, event)
        if order.intent.reduce_only:
            fill_quantity = decimal_value(
                _payload_string(event.payload, "fill_quantity"),
                label="fill_quantity",
                positive=True,
            )
            current = projection.positions.get(order.intent.instrument, Decimal(0))
            signed_fill = cast(OrderSide, order.intent.side).sign * fill_quantity
            remaining = current + signed_fill
            if (
                current == 0
                or (current > 0) == (signed_fill > 0)
                or abs(remaining) > abs(current)
                or (remaining != 0 and (remaining > 0) != (current > 0))
            ):
                raise ValueError("reduce_only fill would cross zero or increase exposure")
    event_date = event.received_at.date().isoformat()
    if projection.session_date != event_date:
        projection.session_start_equity = projection.equity
        projection.session_date = event_date

    if event_type is PaperEventType.RUN_STARTED:
        if projection.last_sequence != 0:
            raise ValueError("RUN_STARTED can only be the first event")
        config_hash = _payload_string(event.payload, "config_hash")
        if config_hash != projection.config_hash:
            raise ValueError("RUN_STARTED config hash differs from the frozen projection")
        projection.state_since = event.received_at
    elif event_type is PaperEventType.STATE_TRANSITIONED:
        _transition(projection, event)
    elif event_type is PaperEventType.DECISION_RECORDED:
        projection.decisions += 1
        raw_decision = event.payload.get("decision")
        if not isinstance(raw_decision, Mapping):
            raise ValueError("DECISION_RECORDED lacks a decision object")
        decision_id = _payload_string(raw_decision, "decision_id")
        action = _payload_string(raw_decision, "action")
        if action == "ENTRY":
            projection.current_entry_decision_id = decision_id
            projection.current_exit_decision_id = None
        elif action == "EXIT":
            projection.current_exit_decision_id = decision_id
    elif event_type is PaperEventType.ORDER_PLANNED:
        raw_order = event.payload.get("order")
        if not isinstance(raw_order, Mapping):
            raise ValueError("ORDER_PLANNED lacks an order object")
        order = PaperOrder.from_dict(
            {
                "action": _payload_string(event.payload, "action"),
                "filled_quantity": "0",
                "fees": "0",
                "intent": raw_order,
                "status": OrderStatus.PLANNED.value,
            }
        )
        if order.intent.order_id in projection.orders:
            raise ValueError("ORDER_PLANNED duplicates an existing order")
        projection.orders[order.intent.order_id] = order
    elif event_type is PaperEventType.RISK_ACCEPTED:
        order = _order(projection, event)
        if order.status is not OrderStatus.PLANNED:
            raise ValueError("risk can only accept a planned order")
        order.status = OrderStatus.RISK_ACCEPTED
        order.active_at = parse_utc(_payload_string(event.payload, "ack_due_at"))
    elif event_type is PaperEventType.RISK_REJECTED:
        order = _order(projection, event)
        if order.status is not OrderStatus.PLANNED:
            raise ValueError("risk can only reject a planned order")
        order.status = OrderStatus.REJECTED
    elif event_type is PaperEventType.ORDER_ACKED:
        order = _order(projection, event)
        if order.status is not OrderStatus.RISK_ACCEPTED:
            raise ValueError("only a risk-accepted order may be acknowledged")
        order.status = OrderStatus.ACKED
        order.active_at = parse_utc(_payload_string(event.payload, "active_at"))
        expires_at = event.payload.get("expires_at")
        order.expires_at = parse_utc(str(expires_at)) if expires_at is not None else None
    elif event_type is PaperEventType.ORDER_REJECTED:
        order = _order(projection, event)
        if order.status not in {OrderStatus.RISK_ACCEPTED, OrderStatus.ACKED}:
            raise ValueError("only an accepted or acknowledged order may be rejected")
        order.status = OrderStatus.REJECTED
    elif event_type is PaperEventType.CANCEL_REQUESTED:
        order = _order(projection, event)
        if order.status not in {OrderStatus.ACKED, OrderStatus.PARTIALLY_FILLED}:
            raise ValueError("only an active order may be cancelled")
        order.status = OrderStatus.CANCEL_PENDING
        order.cancel_effective_at = parse_utc(
            _payload_string(event.payload, "cancel_effective_at")
        )
    elif event_type is PaperEventType.ORDER_PARTIALLY_FILLED:
        _apply_fill(projection, event, full=False)
    elif event_type is PaperEventType.ORDER_FILLED:
        _apply_fill(projection, event, full=True)
    elif event_type is PaperEventType.ORDER_CANCELLED:
        order = _order(projection, event)
        if order.status is not OrderStatus.CANCEL_PENDING:
            raise ValueError("only a cancel-pending order may become cancelled")
        order.status = OrderStatus.CANCELLED
    elif event_type is PaperEventType.ORDER_EXPIRED:
        order = _order(projection, event)
        if order.status not in {
            OrderStatus.ACKED,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.CANCEL_PENDING,
        }:
            raise ValueError("only an active order may expire")
        order.status = OrderStatus.EXPIRED
    elif event_type is PaperEventType.ORDER_NO_FILL:
        order = _order(projection, event)
        if order.status not in {
            OrderStatus.ACKED,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.CANCEL_PENDING,
        }:
            raise ValueError("only an active order may record a non-fill")
        order.status = OrderStatus.NO_FILL
        order.fill_attempts += 1
    elif event_type is PaperEventType.MARK_RECORDED:
        instrument = _payload_string(event.payload, "instrument")
        projection.marks[instrument] = decimal_value(
            _payload_string(event.payload, "price"), label="mark price", positive=True
        )
        projection.peak_equity = max(projection.peak_equity, projection.equity)
        projection.last_market_received_at_by_instrument[instrument] = event.received_at
        projection.last_market_received_at = event.received_at
    elif event_type is PaperEventType.FUNDING_POSTED:
        amount = decimal_value(_payload_string(event.payload, "amount"), label="funding amount")
        projection.cash += amount
        projection.realized_pnl += amount
        projection.peak_equity = max(projection.peak_equity, projection.equity)
    elif event_type is PaperEventType.CYCLE_COMPLETED:
        if projection.positions:
            raise ValueError("a cycle cannot complete with an open position")
        projection.completed_cycles += 1
        projection.current_entry_decision_id = None
        projection.current_exit_decision_id = None
    elif event_type is PaperEventType.ALERT_RAISED:
        if AlertSeverity(_payload_string(event.payload, "severity")) is AlertSeverity.CRITICAL:
            projection.critical_incidents.append(event.received_at)
    elif event_type is PaperEventType.RECONCILIATION_SUCCEEDED:
        projection.reconciled = True
    elif event_type is PaperEventType.RECONCILIATION_FAILED:
        projection.reconciled = False
    elif event_type in {
        PaperEventType.TIMER_TICKED,
        PaperEventType.STRESS_RESULT_RECORDED,
        PaperEventType.RESILIENCE_EXERCISE_RECORDED,
        PaperEventType.OBSERVATION_COVERAGE_RECORDED,
    }:
        pass
    else:  # pragma: no cover - exhaustive StrEnum guard
        raise ValueError(f"unsupported paper event type: {event_type}")

    projection.last_received_at = event.received_at
    return projection


def apply_stored_event(projection: PaperProjection, stored: StoredPaperEvent) -> PaperProjection:
    if stored.sequence != projection.last_sequence + 1:
        raise ValueError("stored paper event sequence is not contiguous")
    if stored.previous_event_hash != projection.last_event_hash:
        raise ValueError("stored paper event hash chain is not contiguous")
    if not stored.verify_hash():
        raise ValueError("stored paper event hash is invalid")
    apply_event(projection, stored.event)
    projection.last_sequence = stored.sequence
    projection.last_event_hash = stored.event_hash
    return projection


def replay_projection(
    *,
    run_id: str,
    config_hash: str,
    initial_cash: Decimal,
    events: tuple[StoredPaperEvent, ...],
) -> PaperProjection:
    projection = PaperProjection(
        run_id=run_id,
        config_hash=config_hash,
        initial_cash=initial_cash,
    )
    for stored in events:
        apply_stored_event(projection, stored)
    return projection


def transaction_ledger_amounts(
    projection: PaperProjection,
    event: PaperEvent,
) -> tuple[tuple[str, Decimal], ...]:
    """Return exact debit-positive entries for a fill, balanced to zero."""

    if event.event_type not in {
        PaperEventType.ORDER_PARTIALLY_FILLED,
        PaperEventType.ORDER_FILLED,
        PaperEventType.FUNDING_POSTED,
    }:
        return ()
    if event.event_type is PaperEventType.FUNDING_POSTED:
        amount = decimal_value(_payload_string(event.payload, "amount"), label="funding amount")
        return (("asset:cash", amount), ("income:funding", -amount))
    order = _order(projection, event)
    quantity = decimal_value(
        _payload_string(event.payload, "fill_quantity"), label="fill_quantity", positive=True
    )
    price = decimal_value(
        _payload_string(event.payload, "fill_price"), label="fill_price", positive=True
    )
    fee = decimal_value(
        _payload_string(event.payload, "fee"), label="fee"
    )
    instrument = order.intent.instrument
    signed_fill = cast(OrderSide, order.intent.side).sign * quantity
    current = projection.positions.get(instrument, Decimal(0))
    basis = projection.cost_basis.get(instrument, Decimal(0))
    inventory_value = projection.inventory_value.get(instrument, Decimal(0))
    entries: list[tuple[str, Decimal]] = []

    closing = Decimal(0)
    if current != 0 and (current > 0) != (signed_fill > 0):
        closing = min(abs(current), abs(signed_fill))
        cash_close = (Decimal(1) if current > 0 else Decimal(-1)) * closing * price
        inventory_close = (
            -inventory_value
            if closing == abs(current)
            else -(Decimal(1) if current > 0 else Decimal(-1)) * closing * basis
        )
        realized = -(cash_close + inventory_close)
        entries.extend(
            [
                ("asset:cash", cash_close),
                (f"asset:inventory:{instrument}", inventory_close),
                ("income:realized_pnl", realized),
            ]
        )

    opening = abs(signed_fill) - closing
    if opening:
        signed_opening = (Decimal(1) if signed_fill > 0 else Decimal(-1)) * opening
        entries.extend(
            [
                ("asset:cash", -signed_opening * price),
                (f"asset:inventory:{instrument}", signed_opening * price),
            ]
        )
    if fee:
        entries.extend([("asset:cash", -fee), ("expense:fees", fee)])
    if sum((amount for _, amount in entries), Decimal(0)) != 0:
        raise AssertionError("generated fill ledger transaction is not exactly balanced")
    return tuple(entries)


__all__ = [
    "apply_event",
    "apply_stored_event",
    "replay_projection",
    "transaction_ledger_amounts",
]
