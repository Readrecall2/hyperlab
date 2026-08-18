from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from decimal import Decimal
from typing import cast

from hyperlab.paper.models import (
    AlertSeverity,
    DecisionAction,
    OrderSide,
    OrderStatus,
    PaperEvent,
    PaperEventType,
    PaperOrder,
    PaperProjection,
    PaperState,
    PaperStrategyProjection,
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


def _payload_digest(payload: Mapping[str, object], key: str) -> str:
    value = _payload_string(payload, key)
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{key} must be a lowercase SHA-256 digest")
    return value


def _event_strategy_id(event: PaperEvent) -> str | None:
    raw = event.payload.get("strategy_id")
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw or any(character.isspace() for character in raw):
        raise ValueError("strategy_id must be a stable non-empty identifier")
    return raw


def _strategy_projection(
    projection: PaperProjection,
    event: PaperEvent,
) -> PaperStrategyProjection | None:
    strategy_id = _event_strategy_id(event)
    if strategy_id is None:
        return None
    try:
        return projection.strategy_projections[strategy_id]
    except KeyError as error:
        raise ValueError(f"event references unknown strategy_id {strategy_id}") from error


def _source_received_at(event: PaperEvent) -> datetime:
    raw = event.payload.get("source_received_at")
    source_received_at = parse_utc(raw) if isinstance(raw, str) else event.received_at
    if source_received_at > event.received_at:
        raise ValueError("public source time cannot exceed durable processing time")
    return source_received_at


def _observe_public_source_time(
    projection: PaperProjection,
    event: PaperEvent,
    *,
    instrument: str | None = None,
) -> datetime:
    source_received_at = _source_received_at(event)
    previous = projection.last_public_source_received_at
    if previous is None or source_received_at > previous:
        projection.last_public_source_received_at = source_received_at
    if instrument is not None:
        prior_market = projection.last_market_received_at_by_instrument.get(instrument)
        if prior_market is None or source_received_at > prior_market:
            projection.last_market_received_at_by_instrument[instrument] = source_received_at
        projection.last_market_received_at = max(projection.last_market_received_at_by_instrument.values())
    return source_received_at


def _order(projection: PaperProjection, event: PaperEvent) -> PaperOrder:
    order_id = _payload_string(event.payload, "order_id")
    try:
        order = projection.orders[order_id]
    except KeyError as error:
        raise ValueError(
            f"event {cast(PaperEventType, event.event_type).value} references unknown order {order_id}"
        ) from error
    if _event_strategy_id(event) != order.intent.strategy_id:
        raise ValueError("order event strategy_id differs from its durable order")
    return order


def _archive_unrelated_terminal_orders(
    projection: PaperProjection,
    *,
    incoming_decision_id: str,
    strategy_id: str | None = None,
) -> None:
    retained_decision_ids = {incoming_decision_id}
    if strategy_id is None:
        if projection.current_entry_decision_id is not None:
            retained_decision_ids.add(projection.current_entry_decision_id)
        if projection.current_exit_decision_id is not None:
            retained_decision_ids.add(projection.current_exit_decision_id)
    else:
        for strategy in projection.strategy_projections.values():
            if strategy.current_entry_decision_id is not None:
                retained_decision_ids.add(strategy.current_entry_decision_id)
            if strategy.current_exit_decision_id is not None:
                retained_decision_ids.add(strategy.current_exit_decision_id)
    archived_order_ids = tuple(
        sorted(
            order_id
            for order_id, order in projection.orders.items()
            if not order.status.active and order.intent.decision_id not in retained_decision_ids
        )
    )
    for order_id in archived_order_ids:
        del projection.orders[order_id]
    projection.archived_order_count += len(archived_order_ids)


def _transition(projection: PaperProjection, event: PaperEvent) -> None:
    source = PaperState(_payload_string(event.payload, "source"))
    target = PaperState(_payload_string(event.payload, "target"))
    if projection.state is not source:
        raise ValueError(f"state transition expected {source.value}, projection is {projection.state.value}")
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
    if target is PaperState.MANUAL_REVIEW:
        projection.reconciled = False
    projection.state_since = event.received_at


def _strategy_transition(
    projection: PaperProjection,
    strategy: PaperStrategyProjection,
    event: PaperEvent,
) -> None:
    source = PaperState(_payload_string(event.payload, "source"))
    target = PaperState(_payload_string(event.payload, "target"))
    if strategy.state is not source:
        raise ValueError(
            f"strategy state transition expected {source.value}, projection is {strategy.state.value}"
        )
    require_transition(source, target)
    protective = {
        PaperState.PAUSED,
        PaperState.REDUCE_ONLY,
        PaperState.MANUAL_REVIEW,
        PaperState.EMERGENCY_FLATTEN,
    }
    if target in protective and source not in protective:
        strategy.suspended_from = source
    elif target not in protective:
        strategy.suspended_from = None
    strategy.state = target
    strategy.state_since = event.received_at
    _sync_portfolio_state(projection, at=event.received_at)


def _sync_portfolio_state(
    projection: PaperProjection,
    *,
    at: datetime,
) -> None:
    if not projection.strategy_projections or projection.state in {
        PaperState.PAUSED,
        PaperState.REDUCE_ONLY,
        PaperState.MANUAL_REVIEW,
        PaperState.EMERGENCY_FLATTEN,
    }:
        return
    target = (
        PaperState.HEDGED
        if any(strategy.positions for strategy in projection.strategy_projections.values())
        else PaperState.FLAT
    )
    if projection.state is not target:
        projection.state = target
        projection.state_since = at


def _apply_strategy_fill(
    projection: PaperProjection,
    strategy: PaperStrategyProjection,
    *,
    instrument: str,
    fill_signed: Decimal,
    price: Decimal,
    fee: Decimal,
) -> None:
    prior_quantity = strategy.positions.get(instrument, Decimal(0))
    prior_basis = strategy.cost_basis.get(instrument, Decimal(0))
    prior_inventory_value = strategy.inventory_value.get(instrument, Decimal(0))
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
            -prior_inventory_value if closed == abs(prior_quantity) else -current_sign * closed * prior_basis
        )
        realized = cash_close + inventory_close
        opening = abs(fill_signed) - closed
        opening_change = (Decimal(1) if fill_signed > 0 else Decimal(-1)) * opening * price
        new_inventory_value = prior_inventory_value + inventory_close + opening_change
        new_basis = Decimal(0) if new_quantity == 0 else abs(new_inventory_value / new_quantity)

    strategy.cash -= fill_signed * price + fee
    strategy.fees += fee
    strategy.realized_pnl += realized - fee
    if new_quantity == 0:
        strategy.positions.pop(instrument, None)
        strategy.cost_basis.pop(instrument, None)
        strategy.inventory_value.pop(instrument, None)
    else:
        strategy.positions[instrument] = new_quantity
        strategy.cost_basis[instrument] = new_basis
        strategy.inventory_value[instrument] = new_inventory_value
    strategy.peak_equity = max(
        strategy.peak_equity,
        strategy.equity(projection.marks),
    )


def _apply_fill(projection: PaperProjection, event: PaperEvent, *, full: bool) -> None:
    order = _order(projection, event)
    event_strategy_id = _event_strategy_id(event)
    if event_strategy_id != order.intent.strategy_id:
        raise ValueError("fill strategy_id differs from its durable order")
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
            -prior_inventory_value if closed == abs(prior_quantity) else -current_sign * closed * prior_basis
        )
        realized = cash_close + inventory_close
        opening = abs(fill_signed) - closed
        opening_change = (Decimal(1) if fill_signed > 0 else Decimal(-1)) * opening * price
        new_inventory_value = prior_inventory_value + inventory_close + opening_change
        new_basis = Decimal(0) if new_quantity == 0 else abs(new_inventory_value / new_quantity)

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
    if order.intent.strategy_id is not None:
        strategy = projection.strategy_projection(order.intent.strategy_id)
        _apply_strategy_fill(
            projection,
            strategy,
            instrument=instrument,
            fill_signed=fill_signed,
            price=price,
            fee=fee,
        )
        _sync_portfolio_state(projection, at=event.received_at)
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
            current = (
                projection.strategy_projection(order.intent.strategy_id).positions.get(
                    order.intent.instrument,
                    Decimal(0),
                )
                if order.intent.strategy_id is not None
                else projection.positions.get(order.intent.instrument, Decimal(0))
            )
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
    strategy = _strategy_projection(projection, event)
    for owned in projection.strategy_projections.values():
        if owned.session_date != event_date:
            owned.session_start_equity = owned.equity(projection.marks)
            owned.session_date = event_date

    if event_type is PaperEventType.RUN_STARTED:
        if projection.last_sequence != 0:
            raise ValueError("RUN_STARTED can only be the first event")
        config_hash = _payload_string(event.payload, "config_hash")
        if config_hash != projection.config_hash:
            raise ValueError("RUN_STARTED config hash differs from the frozen projection")
        raw_strategies = event.payload.get("strategies")
        if raw_strategies is not None:
            if projection.strategy_projections:
                raise ValueError("RUN_STARTED cannot replace strategy projections")
            if not isinstance(raw_strategies, tuple) or not raw_strategies:
                raise ValueError("RUN_STARTED strategies must be a non-empty array")
            parsed: dict[str, PaperStrategyProjection] = {}
            for raw_strategy in raw_strategies:
                if not isinstance(raw_strategy, Mapping):
                    raise ValueError("RUN_STARTED strategy identity must be an object")
                strategy_projection = PaperStrategyProjection(
                    strategy_id=_payload_string(raw_strategy, "strategy_id"),
                    strategy_name=_payload_string(raw_strategy, "strategy_name"),
                    strategy_hash=_payload_digest(raw_strategy, "strategy_hash"),
                    strategy_config_hash=_payload_digest(
                        raw_strategy,
                        "strategy_config_hash",
                    ),
                    state_since=event.received_at,
                    session_date=event_date,
                )
                if strategy_projection.strategy_id in parsed:
                    raise ValueError("RUN_STARTED contains duplicate strategy_id")
                parsed[strategy_projection.strategy_id] = strategy_projection
            projection.strategy_projections = parsed
        projection.state_since = event.received_at
    elif event_type is PaperEventType.RUNTIME_SESSION_STARTED:
        session_id = _payload_digest(event.payload, "session_id")
        raw_generation = event.payload.get("generation")
        if (
            isinstance(raw_generation, bool)
            or not isinstance(raw_generation, int)
            or raw_generation != projection.runtime_session_generation + 1
        ):
            raise ValueError("runtime session generation must advance by exactly one")
        raw_replaced = event.payload.get("replaces_unclosed_session_id")
        replaced_session_id = (
            None if raw_replaced is None else _payload_digest(event.payload, "replaces_unclosed_session_id")
        )
        active_session_id = projection.runtime_session_id if projection.runtime_session_active else None
        if replaced_session_id != active_session_id:
            raise ValueError("runtime session replacement differs from the active durable session")
        started_at = parse_utc(_payload_string(event.payload, "started_at"))
        if started_at != event.received_at:
            raise ValueError("runtime session start time must equal durable processing time")
        projection.runtime_session_generation = raw_generation
        projection.runtime_session_id = session_id
        projection.runtime_session_started_at = started_at
        projection.runtime_session_stopped_at = None
    elif event_type is PaperEventType.RUNTIME_SESSION_STOPPED:
        session_id = _payload_digest(event.payload, "session_id")
        raw_generation = event.payload.get("generation")
        if (
            isinstance(raw_generation, bool)
            or not isinstance(raw_generation, int)
            or not projection.runtime_session_active
            or session_id != projection.runtime_session_id
            or raw_generation != projection.runtime_session_generation
        ):
            raise ValueError("runtime session stop differs from the active durable session")
        stopped_at = parse_utc(_payload_string(event.payload, "stopped_at"))
        if stopped_at != event.received_at:
            raise ValueError("runtime session stop time must equal durable processing time")
        reason = _payload_string(event.payload, "reason")
        if reason not in {"NORMAL_COMPLETION", "COOPERATIVE_STOP"}:
            raise ValueError("unsupported clean runtime session stop reason")
        projection.runtime_session_stopped_at = stopped_at
    elif event_type is PaperEventType.STATE_TRANSITIONED:
        if strategy is None:
            _transition(projection, event)
        else:
            _strategy_transition(projection, strategy, event)
    elif event_type is PaperEventType.DECISION_RECORDED:
        projection.decisions += 1
        raw_decision = event.payload.get("decision")
        if not isinstance(raw_decision, Mapping):
            raise ValueError("DECISION_RECORDED lacks a decision object")
        decision_id = _payload_string(raw_decision, "decision_id")
        action = _payload_string(raw_decision, "action")
        if strategy is None:
            if action == "ENTRY":
                projection.current_entry_decision_id = decision_id
                projection.current_exit_decision_id = None
            elif action == "EXIT":
                projection.current_exit_decision_id = decision_id
        else:
            if raw_decision.get("strategy_id") != strategy.strategy_id:
                raise ValueError("DECISION_RECORDED strategy identity differs from its event")
            strategy.decisions += 1
            if action == "ENTRY":
                strategy.current_entry_decision_id = decision_id
                strategy.current_exit_decision_id = None
            elif action == "EXIT":
                strategy.current_exit_decision_id = decision_id
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
        if order.intent.run_id != projection.run_id:
            raise ValueError("ORDER_PLANNED order belongs to another run")
        if order.intent.strategy_id != _event_strategy_id(event):
            raise ValueError("ORDER_PLANNED strategy identity differs from its event")
        if order.action is DecisionAction.ENTRY:
            current_decision_id = (
                strategy.current_entry_decision_id
                if strategy is not None
                else projection.current_entry_decision_id
            )
        elif order.action is DecisionAction.EXIT:
            current_decision_id = (
                strategy.current_exit_decision_id
                if strategy is not None
                else projection.current_exit_decision_id
            )
        else:
            raise ValueError("ORDER_PLANNED requires an ENTRY or EXIT action")
        if current_decision_id != order.intent.decision_id:
            raise ValueError(f"ORDER_PLANNED must belong to the current {order.action.value} decision")
        if order.intent.order_id in projection.orders:
            raise ValueError("ORDER_PLANNED duplicates an existing order")
        _archive_unrelated_terminal_orders(
            projection,
            incoming_decision_id=order.intent.decision_id,
            strategy_id=order.intent.strategy_id,
        )
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
        order.cancel_effective_at = parse_utc(_payload_string(event.payload, "cancel_effective_at"))
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
        price = decimal_value(_payload_string(event.payload, "price"), label="mark price", positive=True)
        source_received_at = _observe_public_source_time(
            projection,
            event,
            instrument=instrument,
        )
        projection.marks[instrument] = price
        projection.public_bbo_mids[instrument] = price
        projection.public_bbo_received_at_by_instrument[instrument] = source_received_at
        projection.peak_equity = max(projection.peak_equity, projection.equity)
        for owned in projection.strategy_projections.values():
            owned.peak_equity = max(
                owned.peak_equity,
                owned.equity(projection.marks),
            )
    elif event_type is PaperEventType.FUNDING_POSTED:
        _observe_public_source_time(projection, event)
        amount = decimal_value(_payload_string(event.payload, "amount"), label="funding amount")
        projection.cash += amount
        projection.realized_pnl += amount
        raw_strategy_amounts = event.payload.get("strategy_amounts")
        if raw_strategy_amounts is not None:
            if not isinstance(raw_strategy_amounts, Mapping):
                raise ValueError("strategy_amounts must be an object")
            parsed_amounts = {
                str(strategy_id): decimal_value(
                    str(strategy_amount),
                    label=f"strategy funding {strategy_id}",
                )
                for strategy_id, strategy_amount in raw_strategy_amounts.items()
            }
            if sum(parsed_amounts.values(), Decimal(0)) != amount:
                raise ValueError("strategy funding attribution does not sum to account funding")
            for strategy_id, strategy_amount in parsed_amounts.items():
                owned = projection.strategy_projection(strategy_id)
                owned.cash += strategy_amount
                owned.realized_pnl += strategy_amount
                owned.peak_equity = max(
                    owned.peak_equity,
                    owned.equity(projection.marks),
                )
        projection.peak_equity = max(projection.peak_equity, projection.equity)
    elif event_type is PaperEventType.PUBLIC_SOURCE_HEALTH_RECORDED:
        instrument = _payload_string(event.payload, "instrument")
        _observe_public_source_time(projection, event, instrument=instrument)
        if bool(event.payload.get("gap")) or bool(event.payload.get("stale")):
            projection.public_bbo_mids.pop(instrument, None)
            projection.public_bbo_received_at_by_instrument.pop(instrument, None)
    elif event_type is PaperEventType.CYCLE_COMPLETED:
        cycle_positions = strategy.positions if strategy is not None else projection.positions
        if cycle_positions:
            raise ValueError("a cycle cannot complete with an open position")
        projection.completed_cycles += 1
        if strategy is None:
            projection.current_entry_decision_id = None
            projection.current_exit_decision_id = None
        else:
            strategy.completed_cycles += 1
            strategy.current_entry_decision_id = None
            strategy.current_exit_decision_id = None
            _sync_portfolio_state(projection, at=event.received_at)
    elif event_type is PaperEventType.ALERT_RAISED:
        if AlertSeverity(_payload_string(event.payload, "severity")) is AlertSeverity.CRITICAL:
            projection.critical_incident_count += 1
            projection.last_critical_incident_at = event.received_at
            if strategy is not None:
                strategy.critical_incident_count += 1
                strategy.last_critical_incident_at = event.received_at
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
    events: Iterable[StoredPaperEvent],
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
        entries: list[tuple[str, Decimal]] = [
            ("asset:cash", amount),
            ("income:funding", -amount),
        ]
        raw_strategy_amounts = event.payload.get("strategy_amounts")
        if raw_strategy_amounts is not None:
            if not isinstance(raw_strategy_amounts, Mapping):
                raise ValueError("strategy_amounts must be an object")
            for strategy_id, raw_amount in sorted(raw_strategy_amounts.items()):
                strategy_amount = decimal_value(
                    str(raw_amount),
                    label=f"strategy funding {strategy_id}",
                )
                entries.extend(
                    [
                        (f"strategy:{strategy_id}:asset:cash", strategy_amount),
                        (f"strategy:{strategy_id}:income:funding", -strategy_amount),
                    ]
                )
        return tuple(entries)
    order = _order(projection, event)
    quantity = decimal_value(
        _payload_string(event.payload, "fill_quantity"), label="fill_quantity", positive=True
    )
    price = decimal_value(_payload_string(event.payload, "fill_price"), label="fill_price", positive=True)
    fee = decimal_value(_payload_string(event.payload, "fee"), label="fee")
    instrument = order.intent.instrument
    signed_fill = cast(OrderSide, order.intent.side).sign * quantity

    def attributed_entries(
        *,
        current: Decimal,
        basis: Decimal,
        inventory_value: Decimal,
        prefix: str = "",
    ) -> list[tuple[str, Decimal]]:
        attributed: list[tuple[str, Decimal]] = []
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
            attributed.extend(
                [
                    (prefix + "asset:cash", cash_close),
                    (prefix + f"asset:inventory:{instrument}", inventory_close),
                    (prefix + "income:realized_pnl", realized),
                ]
            )

        opening = abs(signed_fill) - closing
        if opening:
            signed_opening = (Decimal(1) if signed_fill > 0 else Decimal(-1)) * opening
            attributed.extend(
                [
                    (prefix + "asset:cash", -signed_opening * price),
                    (
                        prefix + f"asset:inventory:{instrument}",
                        signed_opening * price,
                    ),
                ]
            )
        if fee:
            attributed.extend(
                [
                    (prefix + "asset:cash", -fee),
                    (prefix + "expense:fees", fee),
                ]
            )
        return attributed

    entries = attributed_entries(
        current=projection.positions.get(instrument, Decimal(0)),
        basis=projection.cost_basis.get(instrument, Decimal(0)),
        inventory_value=projection.inventory_value.get(instrument, Decimal(0)),
    )
    if order.intent.strategy_id is not None:
        strategy = projection.strategy_projection(order.intent.strategy_id)
        entries.extend(
            attributed_entries(
                current=strategy.positions.get(instrument, Decimal(0)),
                basis=strategy.cost_basis.get(instrument, Decimal(0)),
                inventory_value=strategy.inventory_value.get(instrument, Decimal(0)),
                prefix=f"strategy:{order.intent.strategy_id}:",
            )
        )
    if sum((amount for _, amount in entries), Decimal(0)) != 0:
        raise AssertionError("generated fill ledger transaction is not exactly balanced")
    return tuple(entries)


__all__ = [
    "apply_event",
    "apply_stored_event",
    "replay_projection",
    "transaction_ledger_amounts",
]
