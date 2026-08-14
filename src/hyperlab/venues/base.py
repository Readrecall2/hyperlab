from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

from hyperlab.collector.models import ParsedMessage, WireEnvelope


def _utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{label} must be an explicit UTC timestamp")
    return value


@dataclass(frozen=True, slots=True)
class NormalizedInstrument:
    """Explicit mapping from one venue contract to HyperLab base-asset units."""

    venue: str
    source_symbol: str
    asset: str
    base_asset: str
    quote_asset: str
    instrument_kind: str
    contract_kind: str
    quantity_multiplier: Decimal
    price_tick: Decimal
    quantity_step: Decimal
    status: str

    def __post_init__(self) -> None:
        if self.instrument_kind != "perp":
            raise ValueError("reference venue instruments must be perpetuals")
        if self.contract_kind != "linear":
            raise ValueError("only explicitly linear contracts are supported")
        if self.quantity_multiplier <= 0 or self.price_tick <= 0 or self.quantity_step <= 0:
            raise ValueError("instrument multipliers and increments must be positive")

    def normalize_quantity(self, source_quantity: object) -> Decimal:
        quantity = Decimal(str(source_quantity)) * self.quantity_multiplier
        if not quantity.is_finite() or quantity < 0:
            raise ValueError(f"invalid source quantity: {source_quantity!r}")
        return quantity


@dataclass(frozen=True, slots=True)
class ClockMeasurement:
    venue: str
    request_sent_time: datetime
    response_received_time: datetime
    server_time: datetime
    round_trip_latency_ms: Decimal
    estimated_clock_drift_ms: Decimal
    drift_uncertainty_ms: Decimal

    def adjusted_one_way_latency_ms(self, source_time: datetime, received_time: datetime) -> Decimal:
        """Return signed latency; negative values expose bad clocks instead of being clipped."""

        _utc(source_time, label="source_time")
        _utc(received_time, label="received_time")
        apparent = Decimal(str((received_time - source_time).total_seconds() * 1_000))
        return apparent + self.estimated_clock_drift_ms


def measure_clock(
    venue: str,
    *,
    request_sent_time: datetime,
    response_received_time: datetime,
    server_time: datetime,
) -> ClockMeasurement:
    sent = _utc(request_sent_time, label="request_sent_time")
    received = _utc(response_received_time, label="response_received_time")
    server = _utc(server_time, label="server_time")
    if received < sent:
        raise ValueError("clock response cannot precede the request")
    round_trip_ms = Decimal(str((received - sent).total_seconds() * 1_000))
    midpoint = sent + (received - sent) / 2
    # local - server: add this value to apparent receive-minus-source latency.
    drift_ms = Decimal(str((midpoint - server).total_seconds() * 1_000))
    return ClockMeasurement(
        venue=venue,
        request_sent_time=sent,
        response_received_time=received,
        server_time=server,
        round_trip_latency_ms=round_trip_ms,
        estimated_clock_drift_ms=drift_ms,
        drift_uncertainty_ms=round_trip_ms / 2,
    )


class PublicVenueConnector(Protocol):
    """Replay-safe parsing boundary implemented by every public venue adapter."""

    venue: str

    def parse_message(self, envelope: WireEnvelope) -> ParsedMessage: ...

    def instrument_for_asset(self, asset: str) -> NormalizedInstrument: ...
