from __future__ import annotations

import hashlib
import html
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, fields
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from types import MappingProxyType
from typing import Any

from hyperlab.collector.models import ParsedRecord
from hyperlab.data.lake import (
    PartitionManifest,
    discover_partitions,
    read_hashed_table,
    validate_partition,
)
from hyperlab.data.schema import RecordType


class MarketMakingDataError(ValueError):
    """Raised when an event replay cannot preserve causal L2 state."""


@dataclass(frozen=True, slots=True)
class AdaptiveMarketMakerConfig:
    """Frozen assumptions for an offline, read-only L2 replay.

    ``CALIBRATED`` is accepted only with a SHA-256 evidence hash. Even then this
    model is a research replay: it contains no client, signer or order route.
    """

    target_venue: str
    asset: str
    initial_cash: float = 100_000.0
    order_size: float = 0.01
    max_order_size: float = 0.05
    max_inventory: float = 0.10
    maker_fee_bps: float = 1.5
    taker_fee_bps: float = 4.5
    minimum_half_spread_bps: float = 2.0
    toxicity_spread_bps: float = 8.0
    inventory_skew_bps: float = 6.0
    quote_latency_ms: int = 25
    cancel_latency_ms: int = 25
    replace_threshold_bps: float = 0.5
    queue_ahead_fraction: float = 1.0
    toxicity_limit: float = 0.75
    toxicity_ewma_alpha: float = 0.20
    order_flow_scale: float = 1.0
    size_toxicity_sensitivity: float = 0.75
    max_book_age_ms: int = 1_000
    spike_threshold_bps: float = 25.0
    markout_horizons_ms: tuple[int, ...] = (100, 1_000, 5_000)
    venue_weights: Mapping[str, float] = field(default_factory=dict)
    hedge_venue: str | None = None
    hedge_trigger_inventory: float | None = None
    calibration_status: str = "UNCALIBRATED"
    calibration_evidence_hash: str | None = None
    data_label: str = "UNVERIFIED_REPLAY"

    def __post_init__(self) -> None:
        if not self.target_venue or not self.asset:
            raise ValueError("target_venue and asset must not be empty")
        positive = {
            "initial_cash": self.initial_cash,
            "order_size": self.order_size,
            "max_order_size": self.max_order_size,
            "max_inventory": self.max_inventory,
            "order_flow_scale": self.order_flow_scale,
        }
        if any(not math.isfinite(value) or value <= 0.0 for value in positive.values()):
            raise ValueError("cash, sizes, inventory and order-flow scale must be finite and positive")
        non_negative = {
            "maker_fee_bps": self.maker_fee_bps,
            "taker_fee_bps": self.taker_fee_bps,
            "minimum_half_spread_bps": self.minimum_half_spread_bps,
            "toxicity_spread_bps": self.toxicity_spread_bps,
            "inventory_skew_bps": self.inventory_skew_bps,
            "replace_threshold_bps": self.replace_threshold_bps,
            "spike_threshold_bps": self.spike_threshold_bps,
        }
        if any(not math.isfinite(value) or value < 0.0 for value in non_negative.values()):
            raise ValueError("fees, spreads, skew and thresholds must be finite and non-negative")
        if self.order_size > self.max_order_size:
            raise ValueError("order_size cannot exceed max_order_size")
        if not 0.0 <= self.queue_ahead_fraction <= 1.0:
            raise ValueError("queue_ahead_fraction must be in [0, 1]")
        if not 0.0 < self.toxicity_ewma_alpha <= 1.0:
            raise ValueError("toxicity_ewma_alpha must be in (0, 1]")
        if not 0.0 <= self.size_toxicity_sensitivity <= 1.0:
            raise ValueError("size_toxicity_sensitivity must be in [0, 1]")
        if not math.isfinite(self.toxicity_limit) or self.toxicity_limit < 0.0:
            raise ValueError("toxicity_limit must be finite and non-negative")
        if self.quote_latency_ms < 0 or self.cancel_latency_ms < 0 or self.max_book_age_ms <= 0:
            raise ValueError("latencies must be non-negative and max book age must be positive")
        if not self.markout_horizons_ms or any(value <= 0 for value in self.markout_horizons_ms):
            raise ValueError("markout horizons must be non-empty and positive")
        if tuple(sorted(set(self.markout_horizons_ms))) != self.markout_horizons_ms:
            raise ValueError("markout horizons must be strictly increasing and unique")
        if self.hedge_venue == self.target_venue:
            raise ValueError("hedge venue must differ from target venue")
        if (self.hedge_venue is None) != (self.hedge_trigger_inventory is None):
            raise ValueError("hedge venue and trigger must be configured together")
        if self.hedge_trigger_inventory is not None and (
            not math.isfinite(self.hedge_trigger_inventory) or self.hedge_trigger_inventory <= 0.0
        ):
            raise ValueError("hedge trigger inventory must be finite and positive")
        if any(
            not venue or not math.isfinite(weight) or weight <= 0.0
            for venue, weight in self.venue_weights.items()
        ):
            raise ValueError("venue weights must have non-empty names and positive finite values")
        object.__setattr__(
            self,
            "venue_weights",
            MappingProxyType(dict(sorted(self.venue_weights.items()))),
        )
        status = self.calibration_status.upper()
        if status not in {"UNCALIBRATED", "CALIBRATED"}:
            raise ValueError("calibration_status must be UNCALIBRATED or CALIBRATED")
        if status == "CALIBRATED" and not _is_sha256(self.calibration_evidence_hash):
            raise ValueError("CALIBRATED market making requires a SHA-256 evidence hash")
        if not self.data_label:
            raise ValueError("data_label must not be empty")
        if status == "CALIBRATED" and any(
            marker in self.data_label.casefold()
            for marker in ("synthetic", "toy", "unverified", "default", "placeholder")
        ):
            raise ValueError("synthetic, toy or placeholder data cannot be declared CALIBRATED")


@dataclass(frozen=True, slots=True)
class MarketObservation:
    received_time: datetime
    target_mid: float
    microprice: float
    imbalance: float
    fair_value: float
    order_flow: float
    toxicity: float
    quoted_bid: float | None
    quoted_ask: float | None
    quoted_bid_size: float | None
    quoted_ask_size: float | None


@dataclass(slots=True)
class MarketMakingFill:
    received_time: datetime
    venue: str
    side: str
    price: float
    quantity: float
    is_maker: bool
    fee: float
    reason: str
    fair_value_at_fill: float
    spread_capture: float
    quote_created_at: datetime | None = None
    queue_ahead_before: float = 0.0
    markouts: dict[int, float | None] = field(default_factory=dict)
    adverse_selection: dict[int, float | None] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MarketMakingMetrics:
    spread_pnl: float
    markout_pnl: dict[int, float]
    adverse_selection_pnl: dict[int, float]
    net_pnl: float
    fee_pnl: float
    hedge_pnl: float
    max_inventory: float
    maker_fills: int
    taker_fills: int
    partial_fills: int
    filled_units: float
    quoted_units: float
    fill_ratio: float
    maker_rate: float
    taker_rate: float
    cancel_count: int
    replace_count: int
    cancel_to_fill: float
    toxic_withdrawals: int
    minimum_quoted_half_spread_bps: float
    minimum_quote_size: float
    maximum_quote_size: float
    hedge_count: int
    observed_venues: tuple[str, ...]
    target_sequence_observable: bool
    sequence_gaps: int
    resynchronizations: int
    outage_count: int
    abandoned_quotes: int
    unresolved_trade_throughs: int
    post_only_rejects: int
    quote_state_known: bool
    spike_count: int
    losses_during_spikes: float


@dataclass(frozen=True, slots=True)
class MarketMakingReplayResult:
    status: str
    simulation_label: str
    config_hash: str
    fills: tuple[MarketMakingFill, ...]
    observations: tuple[MarketObservation, ...]
    metrics: MarketMakingMetrics
    ending_cash: float
    ending_inventory: float
    final_bid: float | None
    final_ask: float | None
    limitations: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["fills"] = [
            {
                **asdict(fill),
                "received_time": fill.received_time.isoformat(),
                "quote_created_at": (
                    None if fill.quote_created_at is None else fill.quote_created_at.isoformat()
                ),
            }
            for fill in self.fills
        ]
        payload["observations"] = [
            {**asdict(item), "received_time": item.received_time.isoformat()} for item in self.observations
        ]
        return payload


@dataclass(frozen=True, slots=True)
class MarketMakingDataAudit:
    asset: str
    target_venue: str
    venues: tuple[str, ...]
    first_received_time: datetime | None
    last_received_time: datetime | None
    event_count: int
    checks: dict[str, bool]
    reasons: tuple[str, ...]
    manifest_hashes: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return all(self.checks.values())

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["first_received_time"] = (
            None if self.first_received_time is None else self.first_received_time.isoformat()
        )
        payload["last_received_time"] = (
            None if self.last_received_time is None else self.last_received_time.isoformat()
        )
        payload["passed"] = self.passed
        return payload


@dataclass(slots=True)
class _Book:
    venue: str
    asset: str
    epoch: str | None = None
    last_sequence: int | None = None
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    received_time: datetime | None = None
    synchronized: bool = False
    resyncing: bool = False

    @property
    def best_bid(self) -> tuple[float, float]:
        if not self.bids:
            raise MarketMakingDataError(f"{self.venue} book has no bid")
        price = max(self.bids)
        return price, self.bids[price]

    @property
    def best_ask(self) -> tuple[float, float]:
        if not self.asks:
            raise MarketMakingDataError(f"{self.venue} book has no ask")
        price = min(self.asks)
        return price, self.asks[price]

    @property
    def mid(self) -> float:
        return (self.best_bid[0] + self.best_ask[0]) / 2.0

    @property
    def imbalance(self) -> float:
        bid_quantity = self.best_bid[1]
        ask_quantity = self.best_ask[1]
        total = bid_quantity + ask_quantity
        return 0.0 if total <= 0.0 else (bid_quantity - ask_quantity) / total

    @property
    def microprice(self) -> float:
        bid_price, bid_quantity = self.best_bid
        ask_price, ask_quantity = self.best_ask
        total = bid_quantity + ask_quantity
        if total <= 0.0:
            return (bid_price + ask_price) / 2.0
        return (ask_price * bid_quantity + bid_price * ask_quantity) / total

    def validate(self) -> None:
        bid = self.best_bid[0]
        ask = self.best_ask[0]
        if bid >= ask:
            raise MarketMakingDataError(f"crossed or locked {self.venue} book: bid={bid} ask={ask}")


@dataclass(slots=True)
class _Quote:
    side: str
    price: float
    quantity: float
    remaining: float
    queue_ahead: float
    created_at: datetime
    active_at: datetime
    cancel_effective_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class _AtomicEvent:
    received_time: datetime
    first_position: int
    records: tuple[ParsedRecord, ...]


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdefABCDEF" for character in value)
    )


def _number(value: object, *, label: str, positive: bool = False) -> float:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise MarketMakingDataError(f"{label} must be numeric") from exc
    if not math.isfinite(result) or (positive and result <= 0.0):
        qualifier = "positive and finite" if positive else "finite"
        raise MarketMakingDataError(f"{label} must be {qualifier}")
    return result


def _time(row: Mapping[str, Any], name: str = "received_time") -> datetime:
    value = row.get(name)
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise MarketMakingDataError(f"{name} must be timezone-aware")
    if value.utcoffset() != UTC.utcoffset(value):
        raise MarketMakingDataError(f"{name} must use UTC")
    return value


def _atomic_events(records: Sequence[ParsedRecord]) -> tuple[_AtomicEvent, ...]:
    if not records:
        raise MarketMakingDataError("market-making replay cannot be empty")
    prior: datetime | None = None
    grouped: dict[tuple[object, ...], list[tuple[int, ParsedRecord]]] = {}
    singles: list[_AtomicEvent] = []
    for position, record in enumerate(records):
        received = _time(record.row)
        if prior is not None and received < prior:
            raise MarketMakingDataError("received_time must be non-decreasing in capture order")
        prior = received
        row = record.row
        key: tuple[object, ...]
        if record.record_type in {RecordType.L2_BOOK_STATE, RecordType.L2_SNAPSHOT}:
            key = (
                "l2_snapshot_frame",
                row.get("venue"),
                record.asset,
                received,
                row.get("snapshot_id"),
            )
            grouped.setdefault(key, []).append((position, record))
        elif record.record_type == RecordType.L2_DELTA:
            key = (
                record.record_type,
                row.get("venue"),
                record.asset,
                received,
                row.get("update_id"),
            )
            grouped.setdefault(key, []).append((position, record))
        elif record.record_type == RecordType.TRADE:
            key = (
                record.record_type,
                row.get("venue"),
                record.asset,
                received,
                row.get("connection_id"),
            )
            grouped.setdefault(key, []).append((position, record))
        else:
            singles.append(_AtomicEvent(received, position, (record,)))
    atomic = singles + [
        _AtomicEvent(_time(items[0][1].row), min(item[0] for item in items), tuple(item[1] for item in items))
        for items in grouped.values()
    ]
    atomic.sort(key=lambda event: (event.received_time, event.first_position))
    return tuple(atomic)


class L2MarketMakingReplay:
    """Deterministic event-by-event simulator with no network or order capability."""

    def __init__(self, config: AdaptiveMarketMakerConfig) -> None:
        self.config = config

    def run(self, records: Iterable[ParsedRecord]) -> MarketMakingReplayResult:
        sequence = tuple(records)
        events = _atomic_events(sequence)
        state = _ReplayState(self.config)
        for event in events:
            state.process(event)
        return state.finish()


class _ReplayState:
    def __init__(self, config: AdaptiveMarketMakerConfig) -> None:
        self.config = config
        self.books: dict[str, _Book] = {}
        self.quotes: dict[str, _Quote] = {}
        self.pending_desired: dict[str, tuple[float, float, datetime]] = {}
        self.cash = config.initial_cash
        self.inventory = 0.0
        self.max_inventory = 0.0
        self.order_flow = 0.0
        self.fills: list[MarketMakingFill] = []
        self.observations: list[MarketObservation] = []
        self.quote_state_known = True
        self.target_available = True
        self.sequence_gaps = 0
        self.resynchronizations = 0
        self.outage_count = 0
        self.abandoned_quotes = 0
        self.unresolved_trade_throughs = 0
        self.post_only_rejects = 0
        self.cancel_count = 0
        self.replace_count = 0
        self.toxic_withdrawals = 0
        self.quoted_units = 0.0
        self.partial_fills = 0
        self.fees_paid = 0.0
        self.hedge_pnl = 0.0
        self.hedge_count = 0
        self.spike_count = 0
        self.losses_during_spikes = 0.0
        self.minimum_half_spreads: list[float] = []
        self.submitted_sizes: list[float] = []
        self.last_target_mid: float | None = None
        self.last_equity: float = config.initial_cash
        self.last_time: datetime | None = None
        self.coverage_gap = False
        self.observed_venues: set[str] = set()
        self.target_sequence_observable = True
        self.target_l2_events = 0

    def process(self, event: _AtomicEvent) -> None:
        self.last_time = event.received_time
        self._complete_cancels(event.received_time)
        record_types = {record.record_type for record in event.records}
        if RecordType.L2_SNAPSHOT in record_types and record_types.issubset(
            {RecordType.L2_BOOK_STATE, RecordType.L2_SNAPSHOT}
        ):
            self._snapshot(event.records, event.received_time)
        elif RecordType.L2_BOOK_STATE in record_types:
            raise MarketMakingDataError("L2 book-state header has no matching snapshot levels")
        elif len(record_types) != 1:
            raise MarketMakingDataError("an atomic event cannot mix record types")
        elif RecordType.TRADE in record_types:
            self._trade_frame(event.records, event.received_time)
        elif RecordType.L2_DELTA in record_types:
            self._delta(event.records, event.received_time)
        elif RecordType.CONNECTION_EVENT in record_types:
            self._connection(event.records[0], event.received_time)
        else:
            return
        self._withdraw_if_stale(event.received_time)
        self._maybe_hedge(event.received_time)
        self._refresh_quotes(event.received_time)
        self._observe(event.received_time)

    def _book(self, venue: str, asset: str) -> _Book:
        if asset != self.config.asset:
            raise MarketMakingDataError(f"replay record asset {asset!r} does not match {self.config.asset!r}")
        return self.books.setdefault(venue, _Book(venue=venue, asset=asset))

    def _snapshot(self, records: tuple[ParsedRecord, ...], at: datetime) -> None:
        headers = [record for record in records if record.record_type == RecordType.L2_BOOK_STATE]
        levels = [record for record in records if record.record_type == RecordType.L2_SNAPSHOT]
        if len(headers) != 1 or not levels:
            raise MarketMakingDataError("L2 snapshot requires one book-state header and its levels")
        header = headers[0]
        first = levels[0]
        venue = str(first.row.get("venue", ""))
        book = self._book(venue, first.asset)
        snapshot_id = first.row.get("snapshot_id")
        epoch = str(first.row.get("book_epoch_id", ""))
        sequence_value = first.row.get("last_sequence")
        last_sequence = None if sequence_value is None else int(str(sequence_value))
        if last_sequence is not None and last_sequence < 0:
            raise MarketMakingDataError("L2 snapshot sequence cannot be negative")
        header_row = header.row
        if (
            header.asset != first.asset
            or header_row.get("venue") != venue
            or header_row.get("snapshot_id") != snapshot_id
            or str(header_row.get("book_epoch_id", "")) != epoch
            or _time(header_row) != at
        ):
            raise MarketMakingDataError("L2 book-state header does not match its snapshot")
        expected_counts = {
            "bid": int(str(header_row.get("bid_level_count"))),
            "ask": int(str(header_row.get("ask_level_count"))),
        }
        if any(value <= 0 for value in expected_counts.values()):
            raise MarketMakingDataError("L2 snapshot must contain at least one level per side")
        bids: dict[float, float] = {}
        asks: dict[float, float] = {}
        observed_levels: dict[str, set[int]] = {"bid": set(), "ask": set()}
        for record in levels:
            row = record.row
            if (
                row.get("venue") != venue
                or row.get("snapshot_id") != snapshot_id
                or str(row.get("book_epoch_id", "")) != epoch
                or row.get("last_sequence") != sequence_value
                or _time(row) != at
            ):
                raise MarketMakingDataError("inconsistent L2 snapshot metadata")
            side = str(row.get("side"))
            if side not in {"bid", "ask"}:
                raise MarketMakingDataError(f"invalid L2 side: {side}")
            level = int(str(row.get("level")))
            if level < 0 or level in observed_levels[side]:
                raise MarketMakingDataError("L2 snapshot levels must be unique and non-negative")
            observed_levels[side].add(level)
            price = _number(row.get("price"), label="L2 snapshot price", positive=True)
            quantity = _number(row.get("quantity"), label="L2 snapshot quantity")
            if quantity < 0.0:
                raise MarketMakingDataError("L2 snapshot quantity must be non-negative")
            target = bids if side == "bid" else asks
            if price in target:
                raise MarketMakingDataError("duplicate price inside L2 snapshot")
            if quantity > 0.0:
                target[price] = quantity
        if any(observed_levels[side] != set(range(expected_counts[side])) for side in ("bid", "ask")):
            raise MarketMakingDataError("L2 snapshot level count does not match its book-state header")
        if not epoch:
            raise MarketMakingDataError("L2 snapshot epoch must not be empty")
        if book.epoch is not None and epoch != book.epoch and not book.resyncing:
            raise MarketMakingDataError("L2 epoch changed without explicit resynchronization")
        if (
            book.epoch == epoch
            and last_sequence is not None
            and book.last_sequence is not None
            and last_sequence <= book.last_sequence
        ):
            raise MarketMakingDataError("non-increasing L2 snapshot sequence")
        book.epoch = epoch
        book.last_sequence = last_sequence
        book.bids = bids
        book.asks = asks
        book.received_time = at
        book.validate()
        self._reconcile_queue_after_book(book)
        self.observed_venues.add(venue)
        if venue == self.config.target_venue:
            self.target_l2_events += 1
            self.target_sequence_observable = self.target_sequence_observable and last_sequence is not None
        if not book.resyncing:
            book.synchronized = True
            if venue == self.config.target_venue:
                self.target_available = True

    def _delta(self, records: tuple[ParsedRecord, ...], at: datetime) -> None:
        first = records[0]
        venue = str(first.row.get("venue", ""))
        book = self._book(venue, first.asset)
        update_id = first.row.get("update_id")
        epoch = str(first.row.get("book_epoch_id", ""))
        first_sequence = _optional_int(first.row.get("first_sequence"))
        last_sequence = _optional_int(first.row.get("last_sequence"))
        if (first_sequence is not None and first_sequence < 0) or (
            last_sequence is not None and last_sequence < 0
        ):
            raise MarketMakingDataError("L2 delta sequences cannot be negative")
        if not book.synchronized or book.epoch != epoch:
            self._gap(venue, at)
            return
        if first_sequence is not None and book.last_sequence is not None:
            if last_sequence is not None and last_sequence <= book.last_sequence:
                raise MarketMakingDataError("non-increasing L2 delta sequence")
            if first_sequence > book.last_sequence + 1:
                self._gap(venue, at)
                return
        if last_sequence is not None and first_sequence is not None and last_sequence < first_sequence:
            raise MarketMakingDataError("L2 delta sequence range is reversed")
        for record in records:
            row = record.row
            if (
                row.get("venue") != venue
                or row.get("update_id") != update_id
                or str(row.get("book_epoch_id", "")) != epoch
                or _optional_int(row.get("first_sequence")) != first_sequence
                or _optional_int(row.get("last_sequence")) != last_sequence
                or _time(row) != at
            ):
                raise MarketMakingDataError("inconsistent L2 delta metadata")
            side = str(row.get("side"))
            if side not in {"bid", "ask"}:
                raise MarketMakingDataError(f"invalid L2 side: {side}")
            price = _number(row.get("price"), label="L2 delta price", positive=True)
            quantity = _number(row.get("quantity"), label="L2 delta quantity")
            action = str(row.get("action"))
            target = book.bids if side == "bid" else book.asks
            if action == "delete" or quantity == 0.0:
                target.pop(price, None)
            elif action == "set" and quantity > 0.0:
                target[price] = quantity
            else:
                raise MarketMakingDataError("invalid L2 delta action or quantity")
        book.last_sequence = last_sequence if last_sequence is not None else book.last_sequence
        book.received_time = at
        book.validate()
        self._reconcile_queue_after_book(book)
        self.observed_venues.add(venue)
        if venue == self.config.target_venue:
            self.target_l2_events += 1
            self.target_sequence_observable = (
                self.target_sequence_observable and first_sequence is not None and last_sequence is not None
            )

    def _trade_frame(self, records: tuple[ParsedRecord, ...], at: datetime) -> None:
        target_signed_flow = 0.0
        for record in records:
            venue = str(record.row.get("venue", ""))
            if record.asset != self.config.asset:
                continue
            side = str(record.row.get("aggressor_side"))
            if side not in {"buy", "sell", "unknown"}:
                raise MarketMakingDataError(f"invalid trade aggressor side: {side}")
            price = _number(record.row.get("price"), label="trade price", positive=True)
            quantity = _number(record.row.get("quantity"), label="trade quantity", positive=True)
            if venue != self.config.target_venue:
                continue
            if self._can_match_quotes() and side != "unknown":
                quote_side = "ask" if side == "buy" else "bid"
                self._consume_quote(quote_side, price, quantity, at)
            target_signed_flow += quantity if side == "buy" else -quantity if side == "sell" else 0.0
        normalized = target_signed_flow / self.config.order_flow_scale
        alpha = self.config.toxicity_ewma_alpha
        self.order_flow = alpha * normalized + (1.0 - alpha) * self.order_flow

    def _consume_quote(self, side: str, trade_price: float, flow: float, at: datetime) -> None:
        quote = self.quotes.get(side)
        if quote is None or quote.active_at > at or quote.created_at >= at:
            return
        if side == "bid" and trade_price > quote.price:
            return
        if side == "ask" and trade_price < quote.price:
            return
        queue_before = quote.queue_ahead
        consumed_queue = min(flow, quote.queue_ahead)
        quote.queue_ahead -= consumed_queue
        executable = flow - consumed_queue
        if executable <= 0.0:
            return
        quantity = min(quote.remaining, executable)
        if quantity <= 0.0:
            return
        fair = self._fair_value(at)
        maker_fee = quantity * quote.price * self.config.maker_fee_bps / 10_000.0
        fill_side = "buy" if side == "bid" else "sell"
        sign = 1.0 if fill_side == "buy" else -1.0
        if fill_side == "buy":
            self.inventory += quantity
            self.cash -= quantity * quote.price + maker_fee
        else:
            self.inventory -= quantity
            self.cash += quantity * quote.price - maker_fee
        self.fees_paid += maker_fee
        quote.remaining -= quantity
        self.partial_fills += int(quantity < quote.quantity or quote.remaining > 1e-12)
        spread_capture = sign * (fair - quote.price) * quantity
        self.fills.append(
            MarketMakingFill(
                received_time=at,
                venue=self.config.target_venue,
                side=fill_side,
                price=quote.price,
                quantity=quantity,
                is_maker=True,
                fee=maker_fee,
                reason="queue_trade_through",
                fair_value_at_fill=fair,
                spread_capture=spread_capture,
                quote_created_at=quote.created_at,
                queue_ahead_before=queue_before,
            )
        )
        if quote.remaining <= 1e-12:
            self.quotes.pop(side, None)
        self.max_inventory = max(self.max_inventory, abs(self.inventory))

    def _reconcile_queue_after_book(self, book: _Book) -> None:
        if book.venue != self.config.target_venue:
            return
        for side, quote in tuple(self.quotes.items()):
            crossed = (side == "bid" and quote.price >= book.best_ask[0]) or (
                side == "ask" and quote.price <= book.best_bid[0]
            )
            if crossed:
                self.quotes.pop(side, None)
                self.pending_desired.pop(side, None)
                if book.received_time is not None and quote.active_at > book.received_time:
                    self.post_only_rejects += 1
                else:
                    self.unresolved_trade_throughs += 1
                    self.coverage_gap = True
                    self.quote_state_known = False
                continue
            displayed = book.bids.get(quote.price, 0.0) if side == "bid" else book.asks.get(quote.price, 0.0)
            quote.queue_ahead = min(quote.queue_ahead, displayed)

    def _connection(self, record: ParsedRecord, at: datetime) -> None:
        venue = str(record.row.get("venue", ""))
        if record.asset not in {self.config.asset, "GLOBAL"}:
            return
        kind = str(record.row.get("event_kind"))
        book = self._book(venue, self.config.asset)
        epoch_value = record.row.get("book_epoch_id")
        epoch = None if epoch_value is None else str(epoch_value)
        if kind == "disconnect":
            if venue == self.config.target_venue:
                self.outage_count += 1
                self.abandoned_quotes += len(self.quotes) + len(self.pending_desired)
                self.quotes.clear()
                self.pending_desired.clear()
                self.quote_state_known = False
                self.target_available = False
            book.synchronized = False
        elif kind == "gap":
            self._gap(venue, at)
        elif kind == "resync_start":
            book.resyncing = True
            book.synchronized = False
            if epoch:
                book.epoch = epoch
                book.last_sequence = None
            if venue == self.config.target_venue:
                self._request_all_cancels(at, replace=False)
                self.target_available = False
        elif kind == "resync_complete":
            if book.received_time is None or (epoch and book.epoch != epoch):
                raise MarketMakingDataError("resync completed without a matching L2 snapshot")
            book.resyncing = False
            book.synchronized = True
            self.resynchronizations += 1
            if venue == self.config.target_venue:
                self.target_available = True
                self.quote_state_known = True
        elif kind == "connect":
            book.synchronized = False
            if venue == self.config.target_venue:
                self.target_available = False

    def _gap(self, venue: str, at: datetime) -> None:
        book = self.books.get(venue)
        if book is not None:
            book.synchronized = False
        self.sequence_gaps += 1
        self.coverage_gap = True
        if venue == self.config.target_venue:
            self._request_all_cancels(at, replace=False)
            self.target_available = False

    def _target_ready(self, at: datetime) -> bool:
        target = self.books.get(self.config.target_venue)
        return (
            self.target_available
            and self.quote_state_known
            and target is not None
            and target.synchronized
            and target.received_time is not None
            and (at - target.received_time) <= timedelta(milliseconds=self.config.max_book_age_ms)
        )

    def _can_match_quotes(self) -> bool:
        target = self.books.get(self.config.target_venue)
        return self.target_available and target is not None and target.synchronized

    def _withdraw_if_stale(self, at: datetime) -> None:
        if self.quotes and not self._target_ready(at):
            self._request_all_cancels(at, replace=False)

    def _fair_value(self, at: datetime) -> float:
        values: list[tuple[float, float]] = []
        configured = dict(self.config.venue_weights)
        if not configured:
            configured = {self.config.target_venue: 1.0}
        elif self.config.target_venue not in configured:
            configured[self.config.target_venue] = 1.0
        for venue, weight in configured.items():
            book = self.books.get(venue)
            if (
                book is None
                or not book.synchronized
                or book.received_time is None
                or at - book.received_time > timedelta(milliseconds=self.config.max_book_age_ms)
            ):
                raise MarketMakingDataError(
                    f"configured fair-value venue {venue!r} is missing, stale or unsynchronized"
                )
            values.append((book.microprice, weight))
        total_weight = sum(weight for _, weight in values)
        return sum(value * weight for value, weight in values) / total_weight

    def _toxicity(self) -> float:
        return abs(self.order_flow)

    def _desired_quotes(self, at: datetime) -> dict[str, tuple[float, float]]:
        if not self._target_ready(at):
            return {}
        toxicity = self._toxicity()
        if toxicity > self.config.toxicity_limit:
            return {}
        target = self.books[self.config.target_venue]
        try:
            fair = self._fair_value(at)
        except MarketMakingDataError:
            return {}
        inventory_ratio = self.inventory / self.config.max_inventory
        reservation = fair - target.mid * self.config.inventory_skew_bps / 10_000.0 * inventory_ratio
        half_spread_bps = max(
            self.config.minimum_half_spread_bps,
            self.config.maker_fee_bps + self.config.toxicity_spread_bps * toxicity,
        )
        half_spread = target.mid * half_spread_bps / 10_000.0
        bid = min(reservation - half_spread, target.best_bid[0])
        ask = max(reservation + half_spread, target.best_ask[0])
        if bid <= 0.0 or bid >= ask:
            raise MarketMakingDataError("quote model produced invalid passive prices")
        size_multiplier = 1.0 - self.config.size_toxicity_sensitivity * min(toxicity, 1.0)
        base_size = min(self.config.max_order_size, self.config.order_size * size_multiplier)
        buy_size = min(base_size, max(self.config.max_inventory - self.inventory, 0.0))
        sell_size = min(base_size, max(self.config.max_inventory + self.inventory, 0.0))
        desired: dict[str, tuple[float, float]] = {}
        if buy_size > 1e-12:
            desired["bid"] = (bid, buy_size)
            self.minimum_half_spreads.append(abs(fair - bid) / target.mid * 10_000.0)
        if sell_size > 1e-12:
            desired["ask"] = (ask, sell_size)
            self.minimum_half_spreads.append(abs(ask - fair) / target.mid * 10_000.0)
        return desired

    def _refresh_quotes(self, at: datetime) -> None:
        target = self.books.get(self.config.target_venue)
        if target is None:
            return
        desired = self._desired_quotes(at)
        if not desired:
            if self.quotes:
                active_cancels = any(quote.cancel_effective_at is None for quote in self.quotes.values())
                if active_cancels and self._toxicity() > self.config.toxicity_limit:
                    self.toxic_withdrawals += 1
                self._request_all_cancels(at, replace=False)
            return
        for side in ("bid", "ask"):
            wanted = desired.get(side)
            current = self.quotes.get(side)
            if wanted is None:
                if current is not None:
                    self._request_cancel(side, at, replace=False)
                continue
            price, quantity = wanted
            if current is None:
                self._place(side, price, quantity, at)
                continue
            price_change = abs(price / current.price - 1.0) * 10_000.0
            size_changed = not math.isclose(quantity, current.quantity, rel_tol=1e-12, abs_tol=1e-12)
            if price_change > self.config.replace_threshold_bps or size_changed:
                self.pending_desired[side] = (price, quantity, at)
                self._request_cancel(side, at, replace=True)

    def _place(self, side: str, price: float, quantity: float, at: datetime) -> None:
        target = self.books[self.config.target_venue]
        displayed = target.bids.get(price, 0.0) if side == "bid" else target.asks.get(price, 0.0)
        queue_ahead = displayed * self.config.queue_ahead_fraction
        self.quotes[side] = _Quote(
            side=side,
            price=price,
            quantity=quantity,
            remaining=quantity,
            queue_ahead=queue_ahead,
            created_at=at,
            active_at=at + timedelta(milliseconds=self.config.quote_latency_ms),
        )
        self.quoted_units += quantity
        self.submitted_sizes.append(quantity)

    def _request_cancel(self, side: str, at: datetime, *, replace: bool) -> None:
        quote = self.quotes.get(side)
        if quote is None or quote.cancel_effective_at is not None:
            return
        self.cancel_count += 1
        self.replace_count += int(replace)
        quote.cancel_effective_at = at + timedelta(milliseconds=self.config.cancel_latency_ms)
        if self.config.cancel_latency_ms == 0:
            self._complete_cancels(at)

    def _request_all_cancels(self, at: datetime, *, replace: bool) -> None:
        for side in tuple(self.quotes):
            self._request_cancel(side, at, replace=replace)
        if not replace:
            self.pending_desired.clear()

    def _complete_cancels(self, at: datetime) -> None:
        completed = [
            side
            for side, quote in self.quotes.items()
            if quote.cancel_effective_at is not None and quote.cancel_effective_at <= at
        ]
        for side in completed:
            self.quotes.pop(side, None)
            pending = self.pending_desired.pop(side, None)
            if pending is not None:
                price, quantity, requested_at = pending
                created_at = max(at, requested_at)
                self._place(side, price, quantity, created_at)

    def _maybe_hedge(self, at: datetime) -> None:
        venue = self.config.hedge_venue
        trigger = self.config.hedge_trigger_inventory
        if venue is None or trigger is None or abs(self.inventory) < trigger:
            return
        book = self.books.get(venue)
        if (
            book is None
            or not book.synchronized
            or book.received_time is None
            or at - book.received_time > timedelta(milliseconds=self.config.max_book_age_ms)
        ):
            return
        quantity = abs(self.inventory)
        if self.inventory > 0.0:
            side = "sell"
            price = book.best_bid[0]
            execution_pnl = (price - book.mid) * quantity
            self.inventory -= quantity
            self.cash += quantity * price
        else:
            side = "buy"
            price = book.best_ask[0]
            execution_pnl = (book.mid - price) * quantity
            self.inventory += quantity
            self.cash -= quantity * price
        fee = quantity * price * self.config.taker_fee_bps / 10_000.0
        self.cash -= fee
        self.fees_paid += fee
        self.hedge_pnl += execution_pnl
        self.hedge_count += 1
        self.fills.append(
            MarketMakingFill(
                received_time=at,
                venue=venue,
                side=side,
                price=price,
                quantity=quantity,
                is_maker=False,
                fee=fee,
                reason="inventory_hedge",
                fair_value_at_fill=book.mid,
                spread_capture=0.0,
            )
        )

    def _observe(self, at: datetime) -> None:
        target = self.books.get(self.config.target_venue)
        if target is None or not target.synchronized or target.received_time is None:
            return
        try:
            fair = self._fair_value(at)
        except MarketMakingDataError:
            return
        bid = self.quotes.get("bid")
        ask = self.quotes.get("ask")
        self.observations.append(
            MarketObservation(
                received_time=at,
                target_mid=target.mid,
                microprice=target.microprice,
                imbalance=target.imbalance,
                fair_value=fair,
                order_flow=self.order_flow,
                toxicity=self._toxicity(),
                quoted_bid=None if bid is None else bid.price,
                quoted_ask=None if ask is None else ask.price,
                quoted_bid_size=None if bid is None else bid.remaining,
                quoted_ask_size=None if ask is None else ask.remaining,
            )
        )
        equity = self.cash + self.inventory * fair
        if self.last_target_mid is not None:
            move_bps = abs(target.mid / self.last_target_mid - 1.0) * 10_000.0
            if move_bps >= self.config.spike_threshold_bps:
                self.spike_count += 1
                self.losses_during_spikes += min(equity - self.last_equity, 0.0)
        self.last_target_mid = target.mid
        self.last_equity = equity
        self.max_inventory = max(self.max_inventory, abs(self.inventory))

    def finish(self) -> MarketMakingReplayResult:
        if not self.observations:
            raise MarketMakingDataError("replay never established a synchronized target book")
        self._compute_markouts()
        maker_fills = sum(fill.is_maker for fill in self.fills)
        taker_fills = len(self.fills) - maker_fills
        filled_units = sum(fill.quantity for fill in self.fills if fill.is_maker)
        fill_ratio = filled_units / self.quoted_units if self.quoted_units > 0.0 else 0.0
        maker_rate = maker_fills / len(self.fills) if self.fills else 0.0
        taker_rate = taker_fills / len(self.fills) if self.fills else 0.0
        cancel_to_fill = self.cancel_count / maker_fills if maker_fills else float(self.cancel_count)
        spread_pnl = sum(fill.spread_capture for fill in self.fills if fill.is_maker)
        markout_pnl = {
            horizon: sum(
                value
                for fill in self.fills
                if fill.is_maker
                for value in [fill.markouts.get(horizon)]
                if value is not None
            )
            for horizon in self.config.markout_horizons_ms
        }
        adverse_pnl = {
            horizon: sum(
                value
                for fill in self.fills
                if fill.is_maker
                for value in [fill.adverse_selection.get(horizon)]
                if value is not None
            )
            for horizon in self.config.markout_horizons_ms
        }
        final_fair = self.observations[-1].fair_value
        net_pnl = self.cash + self.inventory * final_fair - self.config.initial_cash
        status = self._status()
        metrics = MarketMakingMetrics(
            spread_pnl=spread_pnl,
            markout_pnl=markout_pnl,
            adverse_selection_pnl=adverse_pnl,
            net_pnl=net_pnl,
            fee_pnl=-self.fees_paid,
            hedge_pnl=self.hedge_pnl,
            max_inventory=self.max_inventory,
            maker_fills=maker_fills,
            taker_fills=taker_fills,
            partial_fills=self.partial_fills,
            filled_units=filled_units,
            quoted_units=self.quoted_units,
            fill_ratio=fill_ratio,
            maker_rate=maker_rate,
            taker_rate=taker_rate,
            cancel_count=self.cancel_count,
            replace_count=self.replace_count,
            cancel_to_fill=cancel_to_fill,
            toxic_withdrawals=self.toxic_withdrawals,
            minimum_quoted_half_spread_bps=(
                min(self.minimum_half_spreads) if self.minimum_half_spreads else 0.0
            ),
            minimum_quote_size=min(self.submitted_sizes) if self.submitted_sizes else 0.0,
            maximum_quote_size=max(self.submitted_sizes) if self.submitted_sizes else 0.0,
            hedge_count=self.hedge_count,
            observed_venues=tuple(sorted(self.observed_venues)),
            target_sequence_observable=(self.target_l2_events > 0 and self.target_sequence_observable),
            sequence_gaps=self.sequence_gaps,
            resynchronizations=self.resynchronizations,
            outage_count=self.outage_count,
            abandoned_quotes=self.abandoned_quotes,
            unresolved_trade_throughs=self.unresolved_trade_throughs,
            post_only_rejects=self.post_only_rejects,
            quote_state_known=self.quote_state_known,
            spike_count=self.spike_count,
            losses_during_spikes=self.losses_during_spikes,
        )
        return MarketMakingReplayResult(
            status=status,
            simulation_label="EVENT_REPLAY_RESEARCH_ONLY",
            config_hash=_config_hash(self.config),
            fills=tuple(self.fills),
            observations=tuple(self.observations),
            metrics=metrics,
            ending_cash=self.cash,
            ending_inventory=self.inventory,
            final_bid=None if "bid" not in self.quotes else self.quotes["bid"].price,
            final_ask=None if "ask" not in self.quotes else self.quotes["ask"].price,
            limitations=(
                "Queue position is estimated from displayed L2 quantity, not order-level identity.",
                "Hidden liquidity, exchange rejects and private acknowledgements are not observable.",
                "Replay uses receive-time causality and never authorizes testnet or live trading.",
            ),
        )

    def _compute_markouts(self) -> None:
        for fill in self.fills:
            if not fill.is_maker:
                continue
            sign = 1.0 if fill.side == "buy" else -1.0
            for horizon in self.config.markout_horizons_ms:
                threshold = fill.received_time + timedelta(milliseconds=horizon)
                observation = next(
                    (item for item in self.observations if item.received_time >= threshold),
                    None,
                )
                if observation is None:
                    fill.markouts[horizon] = None
                    fill.adverse_selection[horizon] = None
                    continue
                markout = sign * (observation.fair_value - fill.price) * fill.quantity
                fill.markouts[horizon] = markout
                fill.adverse_selection[horizon] = markout - fill.spread_capture

    def _status(self) -> str:
        if not self.quote_state_known:
            return "BLOCKED_UNRECONCILED_QUOTES"
        if self.coverage_gap:
            return "BLOCKED_DATA_GAPS"
        if self.target_l2_events == 0 or not self.target_sequence_observable:
            return "BLOCKED_SEQUENCE_UNOBSERVABLE"
        configured_references = set(self.config.venue_weights) - {self.config.target_venue}
        if not configured_references or not configured_references.issubset(self.observed_venues):
            return "BLOCKED_SINGLE_VENUE"
        if self.config.calibration_status.upper() != "CALIBRATED":
            return "BLOCKED_UNCALIBRATED"
        return "RESEARCH_REPLAY_COMPLETE"


def _optional_int(value: object) -> int | None:
    return None if value is None else int(str(value))


def _config_hash(config: AdaptiveMarketMakerConfig) -> str:
    payload = {
        item.name: (
            dict(config.venue_weights) if item.name == "venue_weights" else getattr(config, item.name)
        )
        for item in fields(config)
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def load_market_making_records(
    root: Path,
    *,
    asset: str,
    venues: Iterable[str],
) -> tuple[tuple[ParsedRecord, ...], tuple[PartitionManifest, ...]]:
    """Load validated immutable lake artifacts needed by the event replay."""

    venue_set = set(venues)
    if not venue_set:
        raise ValueError("at least one venue is required")
    wanted_types = {
        RecordType.L2_BOOK_STATE,
        RecordType.L2_SNAPSHOT,
        RecordType.L2_DELTA,
        RecordType.TRADE,
        RecordType.CONNECTION_EVENT,
    }
    records: list[tuple[datetime, int, ParsedRecord]] = []
    manifests: list[PartitionManifest] = []
    position = 0
    for manifest_path in discover_partitions(root):
        manifest = validate_partition(manifest_path)
        key = manifest.partition
        if key.venue not in venue_set or key.asset not in {asset, "GLOBAL"}:
            continue
        record_type = RecordType(key.record_type)
        if record_type not in wanted_types:
            continue
        table = read_hashed_table(root, manifest)
        manifests.append(manifest)
        for row in table.to_pylist():
            row_asset = str(row.get("asset", key.asset))
            if row_asset not in {asset, "GLOBAL"}:
                continue
            records.append(
                (
                    _time(row),
                    position,
                    ParsedRecord(record_type, row_asset, row),
                )
            )
            position += 1
    records.sort(key=lambda item: (item[0], item[1]))
    return tuple(item[2] for item in records), tuple(manifests)


def audit_market_making_records(
    records: Iterable[ParsedRecord],
    *,
    asset: str,
    target_venue: str,
    minimum_events: int = 10_000,
    calibration_evidence_hash: str | None = None,
    manifest_hashes: Iterable[str] = (),
) -> MarketMakingDataAudit:
    """Fail closed on missing venue, L2, trades, receive times, sequences or resyncs."""

    if minimum_events <= 0:
        raise ValueError("minimum_events must be positive")
    selected = [record for record in records if record.asset in {asset, "GLOBAL"}]
    times = [_time(record.row) for record in selected]
    venues = tuple(sorted({str(record.row.get("venue", "")) for record in selected}))
    types_by_venue: dict[str, set[RecordType]] = {}
    target_l2: list[ParsedRecord] = []
    snapshot_headers: dict[tuple[str, str], tuple[int, int]] = {}
    snapshot_counts: dict[tuple[str, str], dict[str, int]] = {}
    gaps = 0
    completed_resyncs = 0
    frame_keys_by_receive: dict[datetime, set[tuple[object, ...]]] = {}
    for record in selected:
        venue = str(record.row.get("venue", ""))
        frame_key: tuple[object, ...]
        types_by_venue.setdefault(venue, set()).add(record.record_type)
        if venue == target_venue and record.record_type in {
            RecordType.L2_SNAPSHOT,
            RecordType.L2_DELTA,
        }:
            target_l2.append(record)
        if record.record_type == RecordType.L2_BOOK_STATE:
            snapshot_id = str(record.row.get("snapshot_id", ""))
            snapshot_headers[(venue, snapshot_id)] = (
                int(str(record.row.get("bid_level_count"))),
                int(str(record.row.get("ask_level_count"))),
            )
        elif record.record_type == RecordType.L2_SNAPSHOT:
            snapshot_id = str(record.row.get("snapshot_id", ""))
            side = str(record.row.get("side", ""))
            counts = snapshot_counts.setdefault((venue, snapshot_id), {"bid": 0, "ask": 0})
            if side in counts:
                counts[side] += 1
        if record.record_type == RecordType.CONNECTION_EVENT:
            kind = str(record.row.get("event_kind"))
            gaps += int(kind in {"gap", "disconnect"})
            completed_resyncs += int(venue == target_venue and kind == "resync_complete")
        if record.record_type in {RecordType.L2_BOOK_STATE, RecordType.L2_SNAPSHOT}:
            frame_key = (venue, "l2_snapshot", record.row.get("snapshot_id"))
        elif record.record_type == RecordType.L2_DELTA:
            frame_key = (venue, record.record_type, record.row.get("update_id"))
        elif record.record_type == RecordType.TRADE:
            frame_key = (venue, record.record_type, record.row.get("connection_id"))
        else:
            frame_key = (
                venue,
                record.record_type,
                record.row.get("event_kind"),
                record.row.get("channel"),
            )
        frame_keys_by_receive.setdefault(_time(record.row), set()).add(frame_key)
    target_types = types_by_venue.get(target_venue, set())
    reference_venues = [venue for venue in venues if venue != target_venue]
    sequences = [record.row.get("last_sequence", record.row.get("source_sequence")) for record in target_l2]
    snapshots_complete = bool(snapshot_counts) and snapshot_counts.keys() == snapshot_headers.keys()
    if snapshots_complete:
        snapshots_complete = all(
            snapshot_headers[key] == (counts["bid"], counts["ask"]) for key, counts in snapshot_counts.items()
        )
    checks = {
        "minimum_event_count": len(selected) >= minimum_events,
        "target_l2_snapshots": RecordType.L2_SNAPSHOT in target_types,
        "target_l2_snapshot_headers": RecordType.L2_BOOK_STATE in target_types,
        "target_trades": RecordType.TRADE in target_types,
        "multi_venue_l2": any(
            RecordType.L2_SNAPSHOT in types_by_venue.get(venue, set()) for venue in reference_venues
        ),
        "receive_timestamps": bool(times) and all(earlier <= later for earlier, later in pairwise(times)),
        "receive_order_unambiguous": all(
            len(frame_keys) == 1 for frame_keys in frame_keys_by_receive.values()
        ),
        "snapshot_headers_complete": snapshots_complete,
        "target_sequences_observable": bool(sequences) and all(value is not None for value in sequences),
        "no_declared_gaps": gaps == 0,
        "resynchronization_observable": completed_resyncs > 0,
        "calibration_evidence": _is_sha256(calibration_evidence_hash),
    }
    reasons = tuple(name for name, passed in checks.items() if not passed)
    return MarketMakingDataAudit(
        asset=asset,
        target_venue=target_venue,
        venues=venues,
        first_received_time=min(times) if times else None,
        last_received_time=max(times) if times else None,
        event_count=len(selected),
        checks=checks,
        reasons=reasons,
        manifest_hashes=tuple(sorted(manifest_hashes)),
    )


def write_market_making_report(
    result: MarketMakingReplayResult,
    *,
    output_dir: Path,
    audit: MarketMakingDataAudit | None = None,
) -> Path:
    """Write deterministic JSON and HTML artifacts for independent review."""

    output_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {"result": result.as_dict()}
    if audit is not None:
        payload["audit"] = audit.as_dict()
    summary = output_dir / "market_making_summary.json"
    temporary = summary.with_name(f".{summary.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(summary)
    metrics = result.metrics
    rows = "".join(
        f"<tr><th>{html.escape(label)}</th><td>{html.escape(str(value))}</td></tr>"
        for label, value in (
            ("Status", result.status),
            ("Simulation", result.simulation_label),
            ("PnL net", metrics.net_pnl),
            ("PnL spread", metrics.spread_pnl),
            ("Markout 100 ms", metrics.markout_pnl.get(100, 0.0)),
            ("Markout 1 s", metrics.markout_pnl.get(1_000, 0.0)),
            ("Markout 5 s", metrics.markout_pnl.get(5_000, 0.0)),
            ("Inventaire maximal", metrics.max_inventory),
            ("Taux maker", metrics.maker_rate),
            ("Taux taker", metrics.taker_rate),
            ("Fill ratio", metrics.fill_ratio),
            ("Cancel-to-fill", metrics.cancel_to_fill),
            ("Pertes pendant spikes", metrics.losses_during_spikes),
            ("Quotes abandonnées", metrics.abandoned_quotes),
            ("Trade-throughs non résolus", metrics.unresolved_trade_throughs),
            ("Rejets post-only simulés", metrics.post_only_rejects),
        )
    )
    warning = f"{result.status} — RESEARCH ONLY — NO ORDER ROUTE"
    document = f"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><title>Phase 11 market making</title>
<style>body{{font-family:system-ui;max-width:960px;margin:2rem auto;padding:0 1rem}}
.warning{{background:#7a1f1f;color:white;padding:1rem;font-weight:700}}table{{border-collapse:collapse}}
th,td{{border:1px solid #bbb;padding:.5rem;text-align:left}}</style></head>
<body><h1>Phase 11 — replay L2 market making</h1><p class="warning">{warning}</p>
<table>{rows}</table><h2>Limites</h2><ul>{"".join(f"<li>{html.escape(item)}</li>" for item in result.limitations)}</ul>
<p>Configuration hashée : <code>{result.config_hash}</code></p></body></html>"""
    report = output_dir / "market_making_report.html"
    report.write_text(document, encoding="utf-8")
    return report
