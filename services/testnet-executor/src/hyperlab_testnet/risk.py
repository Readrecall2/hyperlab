"""Fail-closed, Testnet-specific pre-action risk controls."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from .canonical import decimal_text, decimal_value, parse_utc, utc_text
from .config import TestnetRiskLimits
from .models import (
    ActionKind,
    OrderSide,
    RuntimeState,
    TestnetOrder,
    TestnetOrderIntent,
)


@dataclass(frozen=True, slots=True)
class RiskDecision:
    accepted: bool
    reasons: tuple[str, ...]
    risk_reducing: bool
    order_notional: Decimal
    projected_gross_notional: Decimal
    projected_position_notional: Decimal
    projected_position_quantity: Decimal
    reserved_gross_notional: Decimal
    worst_long_notional: Decimal
    worst_long_quantity: Decimal
    worst_short_notional: Decimal
    worst_short_quantity: Decimal

    def to_dict(self) -> dict[str, object]:
        return {
            "accepted": self.accepted,
            "order_notional": decimal_text(self.order_notional),
            "projected_gross_notional": decimal_text(self.projected_gross_notional),
            "projected_position_notional": decimal_text(
                self.projected_position_notional
            ),
            "projected_position_quantity": decimal_text(
                self.projected_position_quantity
            ),
            "reasons": list(self.reasons),
            "reserved_gross_notional": decimal_text(self.reserved_gross_notional),
            "risk_reducing": self.risk_reducing,
            "worst_long_notional": decimal_text(self.worst_long_notional),
            "worst_long_quantity": decimal_text(self.worst_long_quantity),
            "worst_short_notional": decimal_text(self.worst_short_notional),
            "worst_short_quantity": decimal_text(self.worst_short_quantity),
        }


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    accepted: bool
    action: ActionKind
    observed_requests: int
    limit: int

    @property
    def reason(self) -> str | None:
        if self.accepted:
            return None
        return f"{self.action.value.lower()} request rate limit reached"


def _utc(value: datetime, *, label: str) -> datetime:
    return parse_utc(utc_text(value), label=label)


def _request_limit(action: ActionKind, limits: TestnetRiskLimits) -> int:
    if action is ActionKind.SUBMIT:
        return limits.submit_requests_per_minute
    if action is ActionKind.CANCEL or action is ActionKind.SCHEDULE_CANCEL:
        return limits.cancel_requests_per_minute
    return limits.replace_requests_per_minute


def evaluate_action_rate(
    action: ActionKind | str,
    *,
    requests_in_last_minute: int,
    limits: TestnetRiskLimits,
) -> RateLimitDecision:
    normalized = ActionKind(action)
    if (
        isinstance(requests_in_last_minute, bool)
        or not isinstance(requests_in_last_minute, int)
        or requests_in_last_minute < 0
    ):
        raise ValueError("requests_in_last_minute must be a non-negative integer")
    limit = _request_limit(normalized, limits)
    return RateLimitDecision(
        accepted=requests_in_last_minute < limit,
        action=normalized,
        observed_requests=requests_in_last_minute,
        limit=limit,
    )


def reconciliation_is_fresh(
    *,
    now: datetime,
    last_reconciled_at: datetime | None,
    limits: TestnetRiskLimits,
) -> bool:
    current = _utc(now, label="now")
    if last_reconciled_at is None:
        return False
    reconciled = _utc(last_reconciled_at, label="last_reconciled_at")
    return reconciled <= current and current - reconciled <= timedelta(
        seconds=limits.reconciliation_stale_after_seconds
    )


def market_is_fresh(
    *,
    now: datetime,
    market_received_at: datetime,
    limits: TestnetRiskLimits,
) -> bool:
    current = _utc(now, label="now")
    received = _utc(market_received_at, label="market_received_at")
    return received <= current and current - received <= timedelta(
        seconds=limits.market_stale_after_seconds
    )


def _active_reservations(
    open_orders: Sequence[TestnetOrder],
    *,
    replaced_order_id: str | None,
) -> tuple[TestnetOrder, ...]:
    for order in open_orders:
        if not isinstance(order, TestnetOrder):
            raise TypeError("open_orders must contain TestnetOrder values")
    active = tuple(order for order in open_orders if order.status.reserves_exposure)
    if replaced_order_id is None:
        return active
    if (
        not isinstance(replaced_order_id, str)
        or len(replaced_order_id) != 64
        or any(character not in "0123456789abcdef" for character in replaced_order_id)
    ):
        raise ValueError("replaced_order_id must be a lowercase SHA-256 order identity")
    matches = tuple(
        order for order in active if order.intent.order_id == replaced_order_id
    )
    if len(matches) != 1:
        raise ValueError(
            "replaced_order_id must identify one exposure-reserving original order"
        )
    return tuple(
        order for order in active if order.intent.order_id != replaced_order_id
    )


def _is_risk_reducing(
    intent: TestnetOrderIntent,
    *,
    current_positions: Mapping[str, Decimal],
    active_orders: Sequence[TestnetOrder],
) -> bool:
    current = decimal_value(
        current_positions.get(intent.instrument, Decimal(0)),
        label="current position",
    )
    if current == 0:
        return False
    reducing_side = OrderSide.SELL if current > 0 else OrderSide.BUY
    if OrderSide(intent.side) is not reducing_side:
        return False
    already_reserved = sum(
        (
            order.reserved_quantity
            for order in active_orders
            if order.intent.instrument == intent.instrument
            and order.intent.reduce_only
            and OrderSide(order.intent.side) is reducing_side
        ),
        Decimal(0),
    )
    return already_reserved + intent.quantity <= abs(current)


def is_risk_reducing(
    intent: TestnetOrderIntent,
    *,
    current_positions: Mapping[str, Decimal],
    open_orders: Sequence[TestnetOrder],
    replaced_order_id: str | None = None,
) -> bool:
    return _is_risk_reducing(
        intent,
        current_positions=current_positions,
        active_orders=_active_reservations(
            open_orders,
            replaced_order_id=replaced_order_id,
        ),
    )


def evaluate_order_risk(
    intent: TestnetOrderIntent,
    *,
    now: datetime,
    market_received_at: datetime,
    last_reconciled_at: datetime | None,
    runtime_state: RuntimeState | str,
    current_positions: Mapping[str, Decimal],
    marks: Mapping[str, Decimal],
    open_orders: Sequence[TestnetOrder],
    submit_requests_in_last_minute: int,
    limits: TestnetRiskLimits,
    replaced_order_id: str | None = None,
) -> RiskDecision:
    """Evaluate a persisted intent using worst-case open/unknown reservations."""

    if not isinstance(intent, TestnetOrderIntent):
        raise TypeError("intent must be a TestnetOrderIntent")
    state = RuntimeState(runtime_state)
    raw_mark = marks.get(intent.instrument)
    mark: Decimal | None = None
    if raw_mark is not None:
        mark = decimal_value(raw_mark, label=f"mark {intent.instrument}", positive=True)
    valuation_price = max(mark, intent.limit_price) if mark is not None else intent.limit_price
    order_notional = intent.quantity * valuation_price
    active_orders = _active_reservations(
        open_orders,
        replaced_order_id=replaced_order_id,
    )
    reducing = _is_risk_reducing(
        intent,
        current_positions=current_positions,
        active_orders=active_orders,
    )
    reasons: list[str] = []

    if state in {RuntimeState.KILLED, RuntimeState.MANUAL_REVIEW}:
        reasons.append(f"runtime state {state.value} blocks every new order action")
    elif state in {RuntimeState.STOPPED, RuntimeState.STARTING}:
        reasons.append(f"runtime state {state.value} is not admitted for order actions")
    elif state is RuntimeState.PAUSED and not reducing:
        reasons.append("PAUSED runtime permits risk reduction only")
    if intent.reduce_only and not reducing:
        reasons.append("reduce_only intent would not reduce the reserved position")
    if not market_is_fresh(
        now=now,
        market_received_at=market_received_at,
        limits=limits,
    ):
        reasons.append("market observation is stale or ahead of the runtime clock")
    if not reconciliation_is_fresh(
        now=now,
        last_reconciled_at=last_reconciled_at,
        limits=limits,
    ):
        reasons.append("authoritative Testnet reconciliation is missing or stale")
    if mark is None:
        reasons.append(f"missing mark for order instrument {intent.instrument}")
    rate = evaluate_action_rate(
        ActionKind.SUBMIT,
        requests_in_last_minute=submit_requests_in_last_minute,
        limits=limits,
    )
    if not rate.accepted:
        assert rate.reason is not None
        reasons.append(rate.reason)
    if intent.quantity > limits.max_order_quantity and not reducing:
        reasons.append("order quantity exceeds max_order_quantity")
    if order_notional > limits.max_order_notional and not reducing:
        reasons.append("order notional exceeds max_order_notional")

    if len(active_orders) >= limits.max_concurrent_orders and not reducing:
        reasons.append("max_concurrent_orders is already reserved")

    current_gross = Decimal(0)
    for instrument, raw_quantity in current_positions.items():
        quantity = decimal_value(raw_quantity, label=f"position {instrument}")
        position_mark = marks.get(instrument)
        if position_mark is None:
            reasons.append(f"missing mark for existing position {instrument}")
            continue
        normalized_mark = decimal_value(
            position_mark, label=f"mark {instrument}", positive=True
        )
        current_gross += abs(quantity * normalized_mark)

    reserved_gross = Decimal(0)
    current_quantity = decimal_value(
        current_positions.get(intent.instrument, Decimal(0)),
        label="current instrument position",
    )
    worst_long_quantity = current_quantity
    worst_short_quantity = current_quantity
    worst_long_price = valuation_price
    worst_short_price = valuation_price
    for order in active_orders:
        reserved_mark = marks.get(order.intent.instrument)
        if reserved_mark is None:
            reasons.append(f"missing mark for reserved order {order.intent.instrument}")
            conservative_reserved_price = order.intent.limit_price
        else:
            normalized_mark = decimal_value(
                reserved_mark,
                label=f"reserved mark {order.intent.instrument}",
                positive=True,
            )
            conservative_reserved_price = max(
                normalized_mark,
                order.intent.limit_price,
            )
        reserved_gross += (
            order.reserved_quantity * conservative_reserved_price
        )
        if order.intent.instrument == intent.instrument:
            if OrderSide(order.intent.side) is OrderSide.BUY:
                worst_long_quantity += order.reserved_quantity
                worst_long_price = max(
                    worst_long_price,
                    conservative_reserved_price,
                )
            else:
                worst_short_quantity -= order.reserved_quantity
                worst_short_price = max(
                    worst_short_price,
                    conservative_reserved_price,
                )
    if OrderSide(intent.side) is OrderSide.BUY:
        worst_long_quantity += intent.quantity
        worst_long_price = max(worst_long_price, valuation_price)
    else:
        worst_short_quantity -= intent.quantity
        worst_short_price = max(worst_short_price, valuation_price)
    worst_long_notional = abs(worst_long_quantity) * worst_long_price
    worst_short_notional = abs(worst_short_quantity) * worst_short_price
    projected_position_quantity = (
        worst_long_quantity
        if abs(worst_long_quantity) >= abs(worst_short_quantity)
        else worst_short_quantity
    )
    projected_position_notional = max(worst_long_notional, worst_short_notional)
    projected_gross = current_gross + reserved_gross + order_notional

    if not reducing:
        if max(abs(worst_long_quantity), abs(worst_short_quantity)) > (
            limits.max_position_quantity
        ):
            reasons.append("worst-case position quantity exceeds max_position_quantity")
        if projected_position_notional > limits.max_position_notional:
            reasons.append("worst-case position notional exceeds max_position_notional")
        if projected_gross > limits.max_gross_notional:
            reasons.append("worst-case gross notional exceeds max_gross_notional")

    return RiskDecision(
        accepted=not reasons,
        reasons=tuple(reasons),
        risk_reducing=reducing,
        order_notional=order_notional,
        projected_gross_notional=projected_gross,
        projected_position_notional=projected_position_notional,
        projected_position_quantity=projected_position_quantity,
        reserved_gross_notional=reserved_gross,
        worst_long_notional=worst_long_notional,
        worst_long_quantity=worst_long_quantity,
        worst_short_notional=worst_short_notional,
        worst_short_quantity=worst_short_quantity,
    )


__all__ = [
    "RateLimitDecision",
    "RiskDecision",
    "TestnetRiskLimits",
    "evaluate_action_rate",
    "evaluate_order_risk",
    "is_risk_reducing",
    "market_is_fresh",
    "reconciliation_is_fresh",
]
