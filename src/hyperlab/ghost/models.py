from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from hyperlab.research_data.canonical import CanonicalValue, canonical_json_bytes

BOUNDARY = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
MODEL_VERSION = "BASE_REALISM_GHOST_V1"
AUTHENTICATED_PUBLIC_RESEARCH_LABEL = "AUTHENTICATED_PUBLIC_RESEARCH"
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$")
_ZERO = Decimal("0")
_BPS = Decimal("10000")
_NS_PER_SECOND = Decimal("1000000000")


def _identifier(value: str, *, label: str) -> str:
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{label} is not a canonical identifier")
    return value


def exact_decimal(value: object, *, label: str, positive: bool = False) -> Decimal:
    if not isinstance(value, (str, int, Decimal)) or isinstance(value, bool):
        raise TypeError(f"{label} must be an exact decimal string or integer")
    result = Decimal(value)
    if not result.is_finite() or (positive and result <= 0):
        raise ValueError(f"{label} must be finite" + (" and positive" if positive else ""))
    return result


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"

    @property
    def sign(self) -> Decimal:
        return Decimal("1") if self is Side.BUY else Decimal("-1")


class TimeInForce(StrEnum):
    POST_ONLY = "POST_ONLY"
    ALO = "ALO"
    IOC = "IOC"

    @property
    def is_maker(self) -> bool:
        return self in {TimeInForce.POST_ONLY, TimeInForce.ALO}


@dataclass(frozen=True, slots=True)
class ExecutionMechanismVersion:
    mechanism_id: str
    venue: str
    instrument_id: str
    effective_from_ns: int
    effective_to_ns: int | None
    supported_time_in_force: tuple[TimeInForce, ...]
    maker_fill_requires_aggressor_flow: bool
    supports_partial_fills: bool
    cancel_replace_loses_priority: bool

    def __post_init__(self) -> None:
        _identifier(self.mechanism_id, label="execution mechanism id")
        _identifier(self.venue, label="venue")
        _identifier(self.instrument_id, label="instrument id")
        if self.effective_from_ns < 0 or (
            self.effective_to_ns is not None
            and self.effective_to_ns <= self.effective_from_ns
        ):
            raise ValueError("execution mechanism interval is invalid")
        if not self.supported_time_in_force:
            raise ValueError("execution mechanism requires supported time in force")
        if len(set(self.supported_time_in_force)) != len(self.supported_time_in_force):
            raise ValueError("execution mechanism time in force values must be unique")
        if self.maker_fill_requires_aggressor_flow is not True:
            raise ValueError("MAKER_CONTACT_FILL_IS_FORBIDDEN")
        if self.supports_partial_fills is not True:
            raise ValueError("PARTIAL_FILL_MODEL_IS_REQUIRED")
        if self.cancel_replace_loses_priority is not True:
            raise ValueError("CANCEL_REPLACE_MUST_LOSE_PRIORITY")

    def assert_effective_at(self, timestamp_ns: int) -> None:
        if timestamp_ns < self.effective_from_ns or (
            self.effective_to_ns is not None and timestamp_ns >= self.effective_to_ns
        ):
            raise ValueError("EXECUTION_MECHANISM_NOT_EFFECTIVE")




class VenueHealth(StrEnum):
    FRESH = "FRESH"
    STALE = "STALE"
    GAP = "GAP"
    RECONNECT = "RECONNECT"
    OUTAGE = "OUTAGE"


class QueueScenario(StrEnum):
    PESSIMISTIC = "PESSIMISTIC"
    CONSERVATIVE = "CONSERVATIVE"
    SENSITIVITY = "SENSITIVITY"


@dataclass(frozen=True, slots=True)
class ClockedObservation:
    source_ns: int | None
    receive_ns: int
    clock_uncertainty_ns: int

    def __post_init__(self) -> None:
        for value, label in (
            (self.receive_ns, "receive time"),
            (self.clock_uncertainty_ns, "clock uncertainty"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{label} must be a non-negative integer")
        if self.source_ns is not None and (type(self.source_ns) is not int or self.source_ns < 0):
            raise ValueError("source time must be absent or non-negative")

    def assert_known_at(self, decision_ns: int, *, decision_clock_uncertainty_ns: int) -> None:
        if self.receive_ns > decision_ns:
            raise ValueError("LOOKAHEAD_RECEIVE_AFTER_DECISION")
        if decision_clock_uncertainty_ns < 0:
            raise ValueError("decision clock uncertainty cannot be negative")
        if self.receive_ns + self.clock_uncertainty_ns > decision_ns - decision_clock_uncertainty_ns:
            raise ValueError("CLOCK_UNCERTAINTY_OVERLAP")


@dataclass(frozen=True, slots=True)
class InstrumentGridVersion:
    grid_id: str
    venue: str
    instrument_id: str
    effective_from_ns: int
    effective_to_ns: int | None
    tick_size: Decimal
    lot_size: Decimal

    def __post_init__(self) -> None:
        _identifier(self.grid_id, label="grid id")
        _identifier(self.venue, label="venue")
        _identifier(self.instrument_id, label="instrument id")
        if self.effective_from_ns < 0:
            raise ValueError("grid effective_from_ns cannot be negative")
        if self.effective_to_ns is not None and self.effective_to_ns <= self.effective_from_ns:
            raise ValueError("grid effective interval is invalid")
        if self.tick_size <= 0 or self.lot_size <= 0:
            raise ValueError("tick and lot sizes must be positive")

    def assert_effective_at(self, timestamp_ns: int) -> None:
        if timestamp_ns < self.effective_from_ns or (
            self.effective_to_ns is not None and timestamp_ns >= self.effective_to_ns
        ):
            raise ValueError("GRID_VERSION_NOT_EFFECTIVE")

    def assert_price(self, value: Decimal) -> None:
        if value <= 0 or value % self.tick_size != 0:
            raise ValueError("PRICE_OFF_TICK_GRID")

    def assert_quantity(self, value: Decimal) -> None:
        if value <= 0 or value % self.lot_size != 0:
            raise ValueError("QUANTITY_OFF_LOT_GRID")


@dataclass(frozen=True, slots=True)
class BookLevel:
    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        if self.price <= 0 or self.quantity <= 0:
            raise ValueError("book levels require positive price and quantity")


@dataclass(frozen=True, slots=True)
class DepthConsumption:
    requested_quantity: Decimal
    filled_quantity: Decimal
    unfilled_quantity: Decimal
    notional: Decimal
    levels: int
    fills: tuple[BookLevel, ...]
    midpoint_used: bool = False

    @property
    def average_price(self) -> Decimal | None:
        if self.filled_quantity == 0:
            return None
        return self.notional / self.filled_quantity


@dataclass(frozen=True, slots=True)
class ExecutableBook:
    venue: str
    instrument_id: str
    observation: ClockedObservation
    health: VenueHealth
    grid: InstrumentGridVersion
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]

    def __post_init__(self) -> None:
        _identifier(self.venue, label="venue")
        _identifier(self.instrument_id, label="instrument id")
        if self.grid.venue != self.venue or self.grid.instrument_id != self.instrument_id:
            raise ValueError("BOOK_GRID_IDENTITY_MISMATCH")
        self.grid.assert_effective_at(self.observation.receive_ns)
        if not self.bids or not self.asks:
            raise ValueError("BOOK_REQUIRES_TWO_SIDED_FINITE_DEPTH")
        if any(left.price <= right.price for left, right in zip(self.bids, self.bids[1:], strict=False)):
            raise ValueError("BIDS_NOT_STRICTLY_DESCENDING")
        if any(left.price >= right.price for left, right in zip(self.asks, self.asks[1:], strict=False)):
            raise ValueError("ASKS_NOT_STRICTLY_ASCENDING")
        for level in self.bids + self.asks:
            self.grid.assert_price(level.price)
            self.grid.assert_quantity(level.quantity)
        if self.best_bid.price >= self.best_ask.price:
            raise ValueError("BOOK_CROSSED")

    @property
    def best_bid(self) -> BookLevel:
        return self.bids[0]

    @property
    def best_ask(self) -> BookLevel:
        return self.asks[0]

    @property
    def spread(self) -> Decimal:
        return self.best_ask.price - self.best_bid.price

    def consume(self, side: Side, quantity: Decimal, limit_price: Decimal) -> DepthConsumption:
        self.grid.assert_quantity(quantity)
        self.grid.assert_price(limit_price)
        selected = self.asks if side is Side.BUY else self.bids
        remaining = quantity
        fills: list[BookLevel] = []
        for level in selected:
            allowed = level.price <= limit_price if side is Side.BUY else level.price >= limit_price
            if not allowed or remaining == 0:
                break
            filled = min(remaining, level.quantity)
            fills.append(BookLevel(level.price, filled))
            remaining -= filled
        filled_quantity = quantity - remaining
        return DepthConsumption(
            requested_quantity=quantity,
            filled_quantity=filled_quantity,
            unfilled_quantity=remaining,
            notional=sum((item.price * item.quantity for item in fills), _ZERO),
            levels=len(fills),
            fills=tuple(fills),
            midpoint_used=False,
        )

    def after(self, side: Side, consumption: DepthConsumption) -> ExecutableBook | None:
        selected = list(self.asks if side is Side.BUY else self.bids)
        for fill in consumption.fills:
            if not selected or selected[0].price != fill.price or selected[0].quantity < fill.quantity:
                raise ValueError("DEPTH_CONSUMPTION_DOES_NOT_MATCH_BOOK")
            remaining = selected[0].quantity - fill.quantity
            if remaining == 0:
                selected.pop(0)
            else:
                selected[0] = BookLevel(selected[0].price, remaining)
        bids = self.bids if side is Side.BUY else tuple(selected)
        asks = tuple(selected) if side is Side.BUY else self.asks
        if not bids or not asks:
            return None
        return ExecutableBook(
            self.venue,
            self.instrument_id,
            self.observation,
            self.health,
            self.grid,
            bids,
            asks,
        )


@dataclass(frozen=True, slots=True)
class LatencyModelVersion:
    model_id: str
    decision_ns: int
    transit_ns: int
    admission_ns: int
    ack_ns: int
    cancel_ns: int
    clock_uncertainty_ns: int

    def __post_init__(self) -> None:
        _identifier(self.model_id, label="latency model id")
        for value in (
            self.decision_ns,
            self.transit_ns,
            self.admission_ns,
            self.ack_ns,
            self.cancel_ns,
            self.clock_uncertainty_ns,
        ):
            if type(value) is not int or value < 0:
                raise ValueError("latency values must be non-negative integers")

    def timeline(self, decision_ns: int, cancel_request_ns: int | None) -> dict[str, int | None]:
        decision_complete = decision_ns + self.decision_ns
        transit = decision_complete + self.transit_ns
        admission = transit + self.admission_ns
        acknowledgement = admission + self.ack_ns
        cancel_ack = None if cancel_request_ns is None else cancel_request_ns + self.cancel_ns
        if cancel_request_ns is not None and cancel_request_ns < decision_ns:
            raise ValueError("CANCEL_PRECEDES_DECISION")
        return {
            "decision_ns": decision_ns,
            "decision_complete_ns": decision_complete,
            "transit_ns": transit,
            "admission_ns": admission,
            "ack_ns": acknowledgement,
            "cancel_request_ns": cancel_request_ns,
            "cancel_ack_ns": cancel_ack,
        }


@dataclass(frozen=True, slots=True)
class QueueModelVersion:
    model_id: str
    primary: QueueScenario
    pessimistic_ahead_multiplier: Decimal
    conservative_ahead_multiplier: Decimal
    sensitivity_ahead_multiplier: Decimal

    def __post_init__(self) -> None:
        _identifier(self.model_id, label="queue model id")
        if self.primary is not QueueScenario.PESSIMISTIC:
            raise ValueError("PRIMARY_QUEUE_SCENARIO_MUST_BE_PESSIMISTIC")
        for value in self.multipliers().values():
            if value < 0:
                raise ValueError("queue multipliers cannot be negative")
        if not (
            self.pessimistic_ahead_multiplier
            >= self.conservative_ahead_multiplier
            >= self.sensitivity_ahead_multiplier
        ):
            raise ValueError("queue scenarios must be ordered from pessimistic to sensitivity")

    def multipliers(self) -> dict[QueueScenario, Decimal]:
        return {
            QueueScenario.PESSIMISTIC: self.pessimistic_ahead_multiplier,
            QueueScenario.CONSERVATIVE: self.conservative_ahead_multiplier,
            QueueScenario.SENSITIVITY: self.sensitivity_ahead_multiplier,
        }


@dataclass(frozen=True, slots=True)
class CostScheduleVersion:
    schedule_id: str
    venue: str
    instrument_id: str
    effective_from_ns: int
    effective_to_ns: int | None
    maker_fee_bps: Decimal
    taker_fee_bps: Decimal
    funding_bps: Decimal
    hedge_fee_bps: Decimal
    opportunity_cost_bps_per_second: Decimal

    def __post_init__(self) -> None:
        _identifier(self.schedule_id, label="cost schedule id")
        _identifier(self.venue, label="venue")
        _identifier(self.instrument_id, label="instrument id")
        if self.effective_from_ns < 0 or (
            self.effective_to_ns is not None and self.effective_to_ns <= self.effective_from_ns
        ):
            raise ValueError("cost schedule interval is invalid")
        for value in (
            self.maker_fee_bps,
            self.taker_fee_bps,
            self.hedge_fee_bps,
            self.opportunity_cost_bps_per_second,
        ):
            if value < 0:
                raise ValueError("primary costs cannot contain rebates or negative charges")

    @property
    def primary_rebate(self) -> Decimal:
        return _ZERO

    @property
    def primary_reward(self) -> Decimal:
        return _ZERO

    def assert_effective_at(self, timestamp_ns: int) -> None:
        if timestamp_ns < self.effective_from_ns or (
            self.effective_to_ns is not None and timestamp_ns >= self.effective_to_ns
        ):
            raise ValueError("COST_SCHEDULE_NOT_EFFECTIVE")

    def fee(self, *, notional: Decimal, maker: bool, hedge: bool) -> Decimal:
        bps = self.maker_fee_bps if maker else self.taker_fee_bps
        if hedge:
            bps += self.hedge_fee_bps
        return notional * bps / _BPS

    def opportunity_cost(self, *, capital_notional_ns: Decimal) -> Decimal:
        return (
            capital_notional_ns
            / _NS_PER_SECOND
            * self.opportunity_cost_bps_per_second
            / _BPS
        )


@dataclass(frozen=True, slots=True)
class ReplayProvenance:
    adapter_id: str
    fixture_sha256: str
    configuration_sha256: str
    raw_manifest_sha256: str | None
    raw_root_sha256: str | None
    segment_sha256s: tuple[str, ...]
    synthetic: bool

    def to_dict(self) -> dict[str, CanonicalValue]:
        return {
            "adapter_id": self.adapter_id,
            "configuration_sha256": self.configuration_sha256,
            "fixture_sha256": self.fixture_sha256,
            "raw_manifest_sha256": self.raw_manifest_sha256,
            "raw_root_sha256": self.raw_root_sha256,
            "segment_sha256s": list(self.segment_sha256s),
            "synthetic": self.synthetic,
        }


@dataclass(frozen=True, slots=True)
class OrderReport:
    order_id: str
    venue: str
    instrument_id: str
    side: Side
    time_in_force: TimeInForce
    role: str
    status: str
    reason: str
    requested_quantity: Decimal
    admitted_quantity: Decimal
    filled_quantity: Decimal
    unfilled_quantity: Decimal
    notional: Decimal
    average_price: Decimal | None
    fee: Decimal
    fill_timestamp_ns: int | None
    level_count: int
    timeline: dict[str, int | None]
    queue_sensitivity: dict[str, str]
    cancel_fill_race_observed: bool
    priority_generation: int
    depends_on_order_id: str | None
    depends_on_filled_quantity: Decimal | None
    group_id: str | None
    leg_index: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "admitted_quantity": self.admitted_quantity,
            "average_price": self.average_price,
            "cancel_fill_race_observed": self.cancel_fill_race_observed,
            "depends_on_filled_quantity": self.depends_on_filled_quantity,
            "depends_on_order_id": self.depends_on_order_id,
            "fee": self.fee,
            "filled_quantity": self.filled_quantity,
            "fill_timestamp_ns": self.fill_timestamp_ns,
            "group_id": self.group_id,
            "instrument_id": self.instrument_id,
            "leg_index": self.leg_index,
            "level_count": self.level_count,
            "notional": self.notional,
            "order_id": self.order_id,
            "priority_generation": self.priority_generation,
            "queue_sensitivity": self.queue_sensitivity,
            "reason": self.reason,
            "requested_quantity": self.requested_quantity,
            "role": self.role,
            "side": self.side.value,
            "status": self.status,
            "time_in_force": self.time_in_force.value,
            "timeline": self.timeline,
            "unfilled_quantity": self.unfilled_quantity,
            "venue": self.venue,
        }


@dataclass(frozen=True, slots=True)
class FillReport:
    order_id: str
    venue: str
    instrument_id: str
    side: Side
    quantity: Decimal
    price: Decimal
    notional: Decimal
    fee: Decimal
    timestamp_ns: int
    role: str
    forced: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "fee": self.fee,
            "forced": self.forced,
            "instrument_id": self.instrument_id,
            "notional": self.notional,
            "order_id": self.order_id,
            "price": self.price,
            "quantity": self.quantity,
            "role": self.role,
            "side": self.side.value,
            "timestamp_ns": self.timestamp_ns,
            "venue": self.venue,
        }


@dataclass(frozen=True, slots=True)
class GroupReport:
    group_id: str
    status: str
    order_ids: tuple[str, ...]
    worst_leg_fill_ratio: Decimal
    residual_inventory: dict[str, Decimal]
    timeout_ns: int
    repair_attempted: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "group_id": self.group_id,
            "order_ids": list(self.order_ids),
            "repair_attempted": self.repair_attempted,
            "residual_inventory": self.residual_inventory,
            "status": self.status,
            "timeout_ns": self.timeout_ns,
            "worst_leg_fill_ratio": self.worst_leg_fill_ratio,
        }


@dataclass(frozen=True, slots=True)
class PnlReport:
    spread: Decimal
    signal: Decimal
    fees: Decimal
    adverse_selection: Decimal
    inventory: Decimal
    hedge: Decimal
    funding: Decimal
    opportunity_cost: Decimal
    forced_close: Decimal
    reward: Decimal
    rebate: Decimal
    net: Decimal
    reconciliation_difference: Decimal

    def to_dict(self) -> dict[str, Decimal]:
        return {
            "adverse_selection": self.adverse_selection,
            "fees": self.fees,
            "forced_close": self.forced_close,
            "funding": self.funding,
            "hedge": self.hedge,
            "inventory": self.inventory,
            "net": self.net,
            "opportunity_cost": self.opportunity_cost,
            "rebate": self.rebate,
            "reconciliation_difference": self.reconciliation_difference,
            "reward": self.reward,
            "signal": self.signal,
            "spread": self.spread,
        }


@dataclass(frozen=True, slots=True)
class ExposureReport:
    positions: dict[str, Decimal]
    gross_filled_notional: Decimal
    capital_immobilized_notional_ns: Decimal
    unresolved_closeout: dict[str, Decimal]
    reconciliation_difference: Decimal

    def to_dict(self) -> dict[str, Any]:
        return {
            "capital_immobilized_notional_ns": self.capital_immobilized_notional_ns,
            "gross_filled_notional": self.gross_filled_notional,
            "positions": self.positions,
            "reconciliation_difference": self.reconciliation_difference,
            "unresolved_closeout": self.unresolved_closeout,
        }


@dataclass(frozen=True, slots=True)
class GhostReport:
    scenario_id: str
    fixture_label: str
    provenance: ReplayProvenance
    model_version: str
    latency_model_id: str
    queue_model_id: str
    grid_version_ids: tuple[str, ...]
    cost_schedule_ids: tuple[str, ...]
    mechanism_version_ids: tuple[str, ...]
    closeout_model_id: str
    fills: tuple[FillReport, ...]
    orders: tuple[OrderReport, ...]
    groups: tuple[GroupReport, ...]
    pnl: PnlReport
    exposure: ExposureReport
    no_trade_reasons: tuple[str, ...]
    observability: tuple[dict[str, Any], ...]
    limitations: tuple[str, ...]
    boundary: str = BOUNDARY
    schema_version: int = 1
    economic_claim: str = "NONE_RESEARCH_MECHANISM_ONLY"

    def _body(self) -> dict[str, Any]:
        return {
            "boundary": self.boundary,
            "cost_schedule_ids": list(self.cost_schedule_ids),
            "economic_claim": self.economic_claim,
            "exposure": self.exposure.to_dict(),
            "fills": [item.to_dict() for item in self.fills],
            "fixture_label": self.fixture_label,
            "grid_version_ids": list(self.grid_version_ids),
            "groups": [item.to_dict() for item in self.groups],
            "latency_model_id": self.latency_model_id,
            "closeout_model_id": self.closeout_model_id,
            "mechanism_version_ids": list(self.mechanism_version_ids),
            "limitations": list(self.limitations),
            "model_version": self.model_version,
            "no_trade_reasons": list(self.no_trade_reasons),
            "observability": list(self.observability),
            "orders": [item.to_dict() for item in self.orders],
            "pnl": self.pnl.to_dict(),
            "provenance": self.provenance.to_dict(),
            "queue_model_id": self.queue_model_id,
            "scenario_id": self.scenario_id,
            "schema_version": self.schema_version,
        }

    @property
    def report_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self._body())).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {**self._body(), "report_sha256": self.report_sha256}

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())


__all__ = [
    "AUTHENTICATED_PUBLIC_RESEARCH_LABEL",
    "BOUNDARY",
    "MODEL_VERSION",
    "BookLevel",
    "ClockedObservation",
    "CostScheduleVersion",
    "DepthConsumption",
    "ExecutableBook",
    "ExecutionMechanismVersion",
    "ExposureReport",
    "FillReport",
    "GhostReport",
    "GroupReport",
    "InstrumentGridVersion",
    "LatencyModelVersion",
    "OrderReport",
    "PnlReport",
    "QueueModelVersion",
    "QueueScenario",
    "ReplayProvenance",
    "Side",
    "TimeInForce",
    "VenueHealth",
    "exact_decimal",
]
