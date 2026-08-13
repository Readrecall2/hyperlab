from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import cast

from hyperlab.paper.models import (
    MarketEvent,
    OrderIntent,
    OrderSide,
    PaperProjection,
    PaperRiskLimits,
    PaperState,
    decimal_text,
)


@dataclass(frozen=True, slots=True)
class RiskDecision:
    accepted: bool
    reasons: tuple[str, ...]
    order_notional: Decimal
    projected_gross_notional: Decimal
    projected_net_notional: Decimal
    projected_instrument_notional: Decimal
    risk_reducing: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "accepted": self.accepted,
            "order_notional": decimal_text(self.order_notional),
            "projected_gross_notional": decimal_text(self.projected_gross_notional),
            "projected_instrument_notional": decimal_text(self.projected_instrument_notional),
            "projected_net_notional": decimal_text(self.projected_net_notional),
            "reasons": list(self.reasons),
            "risk_reducing": self.risk_reducing,
        }


def _price_for_order(order: OrderIntent, market: MarketEvent) -> Decimal:
    if order.limit_price is not None:
        return order.limit_price
    return market.ask_price if order.side is OrderSide.BUY else market.bid_price


def is_risk_reducing(projection: PaperProjection, order: OrderIntent) -> bool:
    current = projection.positions.get(order.instrument, Decimal(0))
    if current == 0:
        return False
    # Reserve already accepted reduce-only remainders before accepting another
    # one.  Without this, concurrent exits can each appear safe in isolation
    # and together cross zero into a new position.
    reserved = sum(
        (
            cast(OrderSide, pending.intent.side).sign * pending.remaining_quantity
            for pending in projection.active_orders
            if pending.intent.instrument == order.instrument
            and pending.intent.reduce_only
        ),
        Decimal(0),
    )
    available = current + reserved
    if available == 0 or (available > 0) != (current > 0):
        return False
    signed = cast(OrderSide, order.side).sign * order.quantity
    projected = available + signed
    return abs(projected) <= abs(available) and (
        projected == 0 or (projected > 0) == (available > 0)
    )


def evaluate_order_risk(
    projection: PaperProjection,
    order: OrderIntent,
    market: MarketEvent,
    limits: PaperRiskLimits,
) -> RiskDecision:
    """Fail-closed pre-acceptance check including worst-case open-order reservations."""

    if order.run_id != projection.run_id:
        raise ValueError("risk check order belongs to another run")
    if market.instrument != order.instrument:
        raise ValueError("risk check market event does not match the order instrument")
    price = _price_for_order(order, market)
    order_notional = order.quantity * price
    reducing = is_risk_reducing(projection, order)
    reasons: list[str] = []

    if projection.state is PaperState.MANUAL_REVIEW:
        reasons.append("manual review blocks every new simulated order")
    if order.reduce_only and not reducing:
        reasons.append("reduce_only order would not reduce the existing position")
    if projection.state in {
        PaperState.PAUSED,
        PaperState.REDUCE_ONLY,
        PaperState.EMERGENCY_FLATTEN,
    } and not reducing:
        reasons.append(f"state {projection.state.value} permits risk reduction only")
    if order.created_at - market.received_at > timedelta(
        seconds=limits.stale_after_seconds
    ) and not reducing:
        reasons.append("public market observation is older than stale_after_seconds")
    if (market.stale or market.gap) and not reducing:
        reasons.append("stale or gapped market data blocks new exposure")
    if not market.tradable and not reducing:
        reasons.append("non-tradable instrument permits flattening only")
    if order_notional > limits.max_order_notional and not reducing:
        reasons.append("order notional exceeds max_order_notional")

    current_equity = projection.equity
    if projection.session_start_equity - current_equity >= limits.max_daily_loss and not reducing:
        reasons.append("daily loss limit permits risk reduction only")
    if projection.peak_equity - current_equity >= limits.max_drawdown and not reducing:
        reasons.append("drawdown limit permits risk reduction only")

    current_gross = Decimal(0)
    current_net = Decimal(0)
    for instrument, quantity in projection.positions.items():
        mark = price if instrument == order.instrument else projection.marks.get(instrument)
        if mark is None:
            reasons.append(f"missing mark for existing position {instrument}")
            continue
        notional = quantity * mark
        current_gross += abs(notional)
        current_net += notional

    reserved_gross = Decimal(0)
    reserved_net = Decimal(0)
    reserved_instrument = Decimal(0)
    for pending in projection.active_orders:
        pending_price = (
            price
            if pending.intent.instrument == order.instrument
            else pending.intent.limit_price or projection.marks.get(pending.intent.instrument)
        )
        if pending_price is None:
            reasons.append(f"missing mark for pending order {pending.intent.order_id}")
            continue
        pending_notional = pending.remaining_quantity * pending_price
        reserved_gross += pending_notional
        reserved_net += cast(OrderSide, pending.intent.side).sign * pending_notional
        if pending.intent.instrument == order.instrument:
            reserved_instrument += pending_notional

    existing_instrument = abs(projection.positions.get(order.instrument, Decimal(0)) * price)
    projected_gross = current_gross + reserved_gross + order_notional
    projected_net = abs(
        current_net + reserved_net + cast(OrderSide, order.side).sign * order_notional
    )
    projected_instrument = existing_instrument + reserved_instrument + order_notional

    if not reducing:
        if projected_gross > limits.max_gross_notional:
            reasons.append("worst-case gross notional exceeds max_gross_notional")
        if projected_net > limits.max_net_notional:
            reasons.append("worst-case net notional exceeds max_net_notional")
        if projected_instrument > limits.max_instrument_notional:
            reasons.append("worst-case instrument notional exceeds max_instrument_notional")

    return RiskDecision(
        accepted=not reasons,
        reasons=tuple(reasons),
        order_notional=order_notional,
        projected_gross_notional=projected_gross,
        projected_net_notional=projected_net,
        projected_instrument_notional=projected_instrument,
        risk_reducing=reducing,
    )


__all__ = ["RiskDecision", "evaluate_order_risk", "is_risk_reducing"]
