"""Deterministic, Testnet-only execution domain models.

The Paper engine remains a separate simulation domain.  This module reuses its
canonical identity and exact-decimal primitives, but owns a distinct order FSM
whose states are driven only by Hyperliquid Testnet observations.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import cast

from .canonical import (
    JsonValue,
    decimal_text,
    decimal_value,
    deterministic_id,
    parse_instrument,
    parse_utc,
    utc_text,
)

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_CLOID_RE = re.compile(r"0x[0-9a-f]{32}\Z")


class RuntimeState(StrEnum):
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    KILLED = "KILLED"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"

    @property
    def sign(self) -> Decimal:
        return Decimal(1) if self is OrderSide.BUY else Decimal(-1)


class OrderType(StrEnum):
    LIMIT = "LIMIT"


class TimeInForce(StrEnum):
    GTC = "GTC"
    IOC = "IOC"
    ALO = "ALO"


class OrderStatus(StrEnum):
    REQUESTED = "REQUESTED"
    SUBMITTED = "SUBMITTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    INVALID = "INVALID"
    UNKNOWN = "UNKNOWN"

    @property
    def terminal(self) -> bool:
        return self in {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
            OrderStatus.INVALID,
        }

    @property
    def reserves_exposure(self) -> bool:
        return self in {
            OrderStatus.REQUESTED,
            OrderStatus.SUBMITTED,
            OrderStatus.ACKNOWLEDGED,
            OrderStatus.OPEN,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.CANCEL_REQUESTED,
            OrderStatus.UNKNOWN,
        }


_ORDER_TRANSITIONS: Mapping[OrderStatus, frozenset[OrderStatus]] = MappingProxyType(
    {
        OrderStatus.REQUESTED: frozenset(
            {
                OrderStatus.SUBMITTED,
                OrderStatus.ACKNOWLEDGED,
                OrderStatus.OPEN,
                OrderStatus.REJECTED,
                OrderStatus.INVALID,
                OrderStatus.UNKNOWN,
            }
        ),
        OrderStatus.SUBMITTED: frozenset(
            {
                OrderStatus.ACKNOWLEDGED,
                OrderStatus.OPEN,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
                OrderStatus.CANCEL_REQUESTED,
                OrderStatus.REJECTED,
                OrderStatus.EXPIRED,
                OrderStatus.INVALID,
                OrderStatus.UNKNOWN,
            }
        ),
        OrderStatus.ACKNOWLEDGED: frozenset(
            {
                OrderStatus.OPEN,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
                OrderStatus.CANCEL_REQUESTED,
                OrderStatus.REJECTED,
                OrderStatus.EXPIRED,
                OrderStatus.UNKNOWN,
            }
        ),
        OrderStatus.OPEN: frozenset(
            {
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
                OrderStatus.CANCEL_REQUESTED,
                OrderStatus.CANCELLED,
                OrderStatus.EXPIRED,
                OrderStatus.UNKNOWN,
            }
        ),
        OrderStatus.PARTIALLY_FILLED: frozenset(
            {
                OrderStatus.FILLED,
                OrderStatus.CANCEL_REQUESTED,
                OrderStatus.CANCELLED,
                OrderStatus.EXPIRED,
                OrderStatus.UNKNOWN,
            }
        ),
        OrderStatus.CANCEL_REQUESTED: frozenset(
            {
                OrderStatus.OPEN,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
                OrderStatus.CANCELLED,
                OrderStatus.REJECTED,
                OrderStatus.EXPIRED,
                OrderStatus.UNKNOWN,
            }
        ),
        OrderStatus.UNKNOWN: frozenset(
            {
                OrderStatus.SUBMITTED,
                OrderStatus.ACKNOWLEDGED,
                OrderStatus.OPEN,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
                OrderStatus.CANCEL_REQUESTED,
                OrderStatus.CANCELLED,
                OrderStatus.REJECTED,
                OrderStatus.EXPIRED,
                OrderStatus.INVALID,
            }
        ),
        OrderStatus.FILLED: frozenset(),
        OrderStatus.CANCELLED: frozenset(),
        OrderStatus.REJECTED: frozenset(),
        OrderStatus.EXPIRED: frozenset(),
        OrderStatus.INVALID: frozenset(),
    }
)


def legal_order_transition(source: OrderStatus | str, target: OrderStatus | str) -> bool:
    current = OrderStatus(source)
    candidate = OrderStatus(target)
    return current is candidate or candidate in _ORDER_TRANSITIONS[current]


def require_order_transition(source: OrderStatus | str, target: OrderStatus | str) -> None:
    current = OrderStatus(source)
    candidate = OrderStatus(target)
    if not legal_order_transition(current, candidate):
        raise ValueError(
            f"illegal Testnet order transition: {current.value} -> {candidate.value}"
        )


class ActionKind(StrEnum):
    SUBMIT = "SUBMIT"
    CANCEL = "CANCEL"
    REPLACE = "REPLACE"
    SCHEDULE_CANCEL = "SCHEDULE_CANCEL"


class ActionAttemptStatus(StrEnum):
    AMBIGUOUS = "AMBIGUOUS"
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"
    RESOLVED_NOT_SENT = "RESOLVED_NOT_SENT"

    @property
    def resolved(self) -> bool:
        return self is not ActionAttemptStatus.AMBIGUOUS


def _sha256(value: str, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return value


def deterministic_cloid(*components: object) -> str:
    """Return a Hyperliquid CLOID with an exact 16-byte lowercase-hex payload."""

    digest = deterministic_id("hyperliquid_testnet_cloid_v1", *components)
    return f"0x{digest[:32]}"


def validate_cloid(value: str) -> str:
    if not isinstance(value, str) or _CLOID_RE.fullmatch(value) is None:
        raise ValueError("cloid must be 0x followed by exactly 16 lowercase-hex bytes")
    return value


def validate_testnet_instrument(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("instrument must be a string")
    exchange, coin, kind = parse_instrument(value)
    if (
        exchange != "HL"
        or kind != "perp"
        or not coin
        or any(
            character == ":"
            or character.isspace()
            or unicodedata.category(character).startswith("C")
            for character in coin
        )
        or value != f"HL:{coin}:perp"
    ):
        raise ValueError(
            "Testnet instrument must be exact canonical HL:<venue coin>:perp"
        )
    return value


@dataclass(frozen=True, slots=True)
class TestnetOrderIntent:
    order_id: str
    cloid: str
    run_id: str
    decision_id: str
    instrument: str
    side: OrderSide | str
    quantity: Decimal
    limit_price: Decimal
    time_in_force: TimeInForce | str
    reduce_only: bool
    created_at: datetime
    ordinal: int = 0
    order_type: OrderType | str = OrderType.LIMIT

    def __post_init__(self) -> None:
        object.__setattr__(self, "order_id", _sha256(self.order_id, label="order_id"))
        object.__setattr__(self, "run_id", _sha256(self.run_id, label="run_id"))
        object.__setattr__(self, "decision_id", _sha256(self.decision_id, label="decision_id"))
        object.__setattr__(self, "cloid", validate_cloid(self.cloid))
        object.__setattr__(
            self,
            "instrument",
            validate_testnet_instrument(self.instrument),
        )
        object.__setattr__(self, "side", OrderSide(self.side))
        object.__setattr__(self, "order_type", OrderType(self.order_type))
        object.__setattr__(self, "time_in_force", TimeInForce(self.time_in_force))
        object.__setattr__(
            self,
            "quantity",
            decimal_value(self.quantity, label="quantity", positive=True),
        )
        object.__setattr__(
            self,
            "limit_price",
            decimal_value(self.limit_price, label="limit_price", positive=True),
        )
        if not isinstance(self.reduce_only, bool):
            raise TypeError("reduce_only must be a boolean")
        if isinstance(self.ordinal, bool) or not isinstance(self.ordinal, int) or self.ordinal < 0:
            raise ValueError("ordinal must be a non-negative integer")
        # parse_utc owns the strict timezone/UTC checks used by Paper.
        normalized_created_at = parse_utc(utc_text(self.created_at), label="created_at")
        object.__setattr__(self, "created_at", normalized_created_at)
        expected_order_id = self.identifier(
            run_id=self.run_id,
            decision_id=self.decision_id,
            instrument=self.instrument,
            side=cast(OrderSide, self.side),
            quantity=self.quantity,
            limit_price=self.limit_price,
            time_in_force=cast(TimeInForce, self.time_in_force),
            reduce_only=self.reduce_only,
            ordinal=self.ordinal,
        )
        if self.order_id != expected_order_id:
            raise ValueError("order_id does not match the deterministic Testnet intent")
        if self.cloid != deterministic_cloid(self.run_id, self.order_id):
            raise ValueError("cloid does not match the deterministic Testnet order identity")

    @staticmethod
    def identifier(
        *,
        run_id: str,
        decision_id: str,
        instrument: str,
        side: OrderSide | str,
        quantity: Decimal | str | int,
        limit_price: Decimal | str | int,
        time_in_force: TimeInForce | str,
        reduce_only: bool,
        ordinal: int,
    ) -> str:
        return deterministic_id(
            "hyperliquid_testnet_order_v1",
            run_id,
            decision_id,
            ordinal,
            instrument,
            str(side),
            decimal_text(decimal_value(quantity, label="quantity", positive=True)),
            decimal_text(decimal_value(limit_price, label="limit_price", positive=True)),
            str(time_in_force),
            reduce_only,
        )

    @classmethod
    def create(
        cls,
        *,
        run_id: str,
        decision_id: str,
        instrument: str,
        side: OrderSide | str,
        quantity: Decimal | str | int,
        limit_price: Decimal | str | int,
        time_in_force: TimeInForce | str,
        reduce_only: bool,
        created_at: datetime,
        ordinal: int = 0,
    ) -> TestnetOrderIntent:
        normalized_quantity = decimal_value(quantity, label="quantity", positive=True)
        normalized_price = decimal_value(limit_price, label="limit_price", positive=True)
        order_id = cls.identifier(
            run_id=run_id,
            decision_id=decision_id,
            instrument=instrument,
            side=side,
            quantity=normalized_quantity,
            limit_price=normalized_price,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            ordinal=ordinal,
        )
        return cls(
            order_id=order_id,
            cloid=deterministic_cloid(run_id, order_id),
            run_id=run_id,
            decision_id=decision_id,
            instrument=instrument,
            side=side,
            quantity=normalized_quantity,
            limit_price=normalized_price,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            created_at=created_at,
            ordinal=ordinal,
        )

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "cloid": self.cloid,
            "created_at": utc_text(self.created_at),
            "decision_id": self.decision_id,
            "instrument": self.instrument,
            "limit_price": decimal_text(self.limit_price),
            "order_id": self.order_id,
            "order_type": cast(OrderType, self.order_type).value,
            "ordinal": self.ordinal,
            "quantity": decimal_text(self.quantity),
            "reduce_only": self.reduce_only,
            "run_id": self.run_id,
            "side": cast(OrderSide, self.side).value,
            "time_in_force": cast(TimeInForce, self.time_in_force).value,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> TestnetOrderIntent:
        expected_keys = frozenset(
            {
                "cloid",
                "created_at",
                "decision_id",
                "instrument",
                "limit_price",
                "order_id",
                "order_type",
                "ordinal",
                "quantity",
                "reduce_only",
                "run_id",
                "side",
                "time_in_force",
            }
        )
        if frozenset(value) != expected_keys:
            missing = sorted(expected_keys - frozenset(value))
            extra = sorted(frozenset(value) - expected_keys)
            raise ValueError(
                f"invalid Testnet order intent keys: missing={missing}; extra={extra}"
            )
        reduce_only = value["reduce_only"]
        if not isinstance(reduce_only, bool):
            raise TypeError("reduce_only must be a boolean")
        ordinal = value["ordinal"]
        if isinstance(ordinal, bool) or not isinstance(ordinal, int):
            raise TypeError("ordinal must be an integer")
        return cls(
            order_id=str(value["order_id"]),
            cloid=str(value["cloid"]),
            run_id=str(value["run_id"]),
            decision_id=str(value["decision_id"]),
            instrument=str(value["instrument"]),
            side=str(value["side"]),
            quantity=decimal_value(str(value["quantity"]), label="quantity", positive=True),
            limit_price=decimal_value(
                str(value["limit_price"]), label="limit_price", positive=True
            ),
            time_in_force=str(value["time_in_force"]),
            reduce_only=reduce_only,
            created_at=parse_utc(str(value["created_at"]), label="created_at"),
            ordinal=ordinal,
            order_type=str(value["order_type"]),
        )


@dataclass(frozen=True, slots=True)
class TestnetOrder:
    intent: TestnetOrderIntent
    status: OrderStatus
    filled_quantity: Decimal = Decimal(0)
    average_fill_price: Decimal | None = None
    venue_order_id: str | None = None
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.intent, TestnetOrderIntent):
            raise TypeError("intent must be a TestnetOrderIntent")
        object.__setattr__(self, "status", OrderStatus(self.status))
        filled = decimal_value(
            self.filled_quantity,
            label="filled_quantity",
            non_negative=True,
        )
        if filled > self.intent.quantity:
            raise ValueError("filled_quantity cannot exceed requested quantity")
        object.__setattr__(self, "filled_quantity", filled)
        if self.average_fill_price is not None:
            object.__setattr__(
                self,
                "average_fill_price",
                decimal_value(
                    self.average_fill_price,
                    label="average_fill_price",
                    positive=True,
                ),
            )
        status = self.status
        if filled == 0 and self.average_fill_price is not None:
            raise ValueError("average_fill_price requires a positive filled_quantity")
        if filled > 0 and self.average_fill_price is None:
            raise ValueError("positive filled_quantity requires average_fill_price")
        if status is OrderStatus.FILLED and filled != self.intent.quantity:
            raise ValueError("FILLED orders require filled_quantity equal to quantity")
        if status is OrderStatus.PARTIALLY_FILLED and not (
            Decimal(0) < filled < self.intent.quantity
        ):
            raise ValueError(
                "PARTIALLY_FILLED orders require 0 < filled_quantity < quantity"
            )
        if status in {
            OrderStatus.REQUESTED,
            OrderStatus.SUBMITTED,
            OrderStatus.ACKNOWLEDGED,
            OrderStatus.OPEN,
            OrderStatus.REJECTED,
            OrderStatus.INVALID,
        } and filled != 0:
            raise ValueError(f"{status.value} orders cannot carry a fill projection")
        if self.venue_order_id is not None:
            value = self.venue_order_id.strip()
            if not value or any(character.isspace() for character in value):
                raise ValueError("venue_order_id must be a stable non-empty identifier")
            object.__setattr__(self, "venue_order_id", value)
        if self.updated_at is not None:
            object.__setattr__(
                self,
                "updated_at",
                parse_utc(utc_text(self.updated_at), label="updated_at"),
            )

    @property
    def remaining_quantity(self) -> Decimal:
        return self.intent.quantity - self.filled_quantity

    @property
    def reserved_quantity(self) -> Decimal:
        return self.remaining_quantity if self.status.reserves_exposure else Decimal(0)


__all__ = [
    "ActionAttemptStatus",
    "ActionKind",
    "OrderSide",
    "OrderStatus",
    "OrderType",
    "RuntimeState",
    "TestnetOrder",
    "TestnetOrderIntent",
    "TimeInForce",
    "deterministic_cloid",
    "legal_order_transition",
    "require_order_transition",
    "validate_cloid",
    "validate_testnet_instrument",
]
