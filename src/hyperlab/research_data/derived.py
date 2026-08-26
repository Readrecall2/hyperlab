from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from .canonical import canonical_json_bytes
from .envelope import PublicDataEnvelope, Venue
from .segments import ManifestRecord, SegmentDescriptor


def _decimal(value: object | None) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"invalid exact decimal {value!r}") from error
    if not result.is_finite():
        raise ValueError("non-finite decimal is forbidden")
    return result


def _mapping(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


@dataclass(frozen=True, slots=True)
class BookLevel:
    side: str
    price: Decimal
    quantity: Decimal
    order_count: int | None
    level: int

    def __post_init__(self) -> None:
        if self.side not in {"BID", "ASK"}:
            raise ValueError("book side must be BID or ASK")
        if self.price <= 0 or self.quantity < 0 or self.level < 0:
            raise ValueError("book level price, quantity, or index is invalid")
        if self.order_count is not None and self.order_count < 0:
            raise ValueError("book order count cannot be negative")


@dataclass(frozen=True, slots=True)
class BboView:
    venue: Venue
    instrument_id: str
    source_timestamp_ns: int | None
    receive_timestamp_utc_ns: int
    arrival_sequence: int
    bid_price: Decimal | None
    bid_quantity: Decimal | None
    ask_price: Decimal | None
    ask_quantity: Decimal | None
    spread: Decimal | None
    imbalance: Decimal | None


@dataclass(frozen=True, slots=True)
class L2SnapshotView:
    venue: Venue
    instrument_id: str
    source_timestamp_ns: int | None
    receive_timestamp_utc_ns: int
    arrival_sequence: int
    levels: tuple[BookLevel, ...]
    is_snapshot: bool = True


@dataclass(frozen=True, slots=True)
class TradeView:
    venue: Venue
    instrument_id: str
    source_timestamp_ns: int
    receive_timestamp_utc_ns: int
    arrival_sequence: int
    source_event_id: str
    price: Decimal
    quantity: Decimal
    aggressor_side: str | None
    causal_liquidation_label: bool | None = None


@dataclass(frozen=True, slots=True)
class ActiveContextView:
    venue: Venue
    instrument_id: str
    source_timestamp_ns: int | None
    receive_timestamp_utc_ns: int
    arrival_sequence: int
    mark_price: Decimal | None
    mid_price: Decimal | None
    oracle_price: Decimal | None
    funding: Decimal | None
    open_interest: Decimal | None
    day_notional_volume: Decimal | None


@dataclass(frozen=True, slots=True)
class HyperliquidDerivedViews:
    bbo: tuple[BboView, ...]
    l2_snapshots: tuple[L2SnapshotView, ...]
    trades: tuple[TradeView, ...]
    active_context: tuple[ActiveContextView, ...]


def _bbo(envelope: PublicDataEnvelope, data: dict[str, Any]) -> BboView:
    sides = data.get("bbo")
    if not isinstance(sides, list) or len(sides) != 2:
        raise ValueError("Hyperliquid BBO must contain bid and ask slots")
    bid = None if sides[0] is None else _mapping(sides[0], label="BBO bid")
    ask = None if sides[1] is None else _mapping(sides[1], label="BBO ask")
    bid_price = None if bid is None else _decimal(bid.get("px"))
    bid_quantity = None if bid is None else _decimal(bid.get("sz"))
    ask_price = None if ask is None else _decimal(ask.get("px"))
    ask_quantity = None if ask is None else _decimal(ask.get("sz"))
    spread = None if bid_price is None or ask_price is None else ask_price - bid_price
    total_quantity = (
        None
        if bid_quantity is None or ask_quantity is None
        else bid_quantity + ask_quantity
    )
    if total_quantity is None or total_quantity <= 0:
        imbalance = None
    else:
        assert bid_quantity is not None and ask_quantity is not None
        imbalance = (bid_quantity - ask_quantity) / total_quantity
    assert envelope.instrument_id is not None
    return BboView(
        venue=envelope.venue,
        instrument_id=envelope.instrument_id,
        source_timestamp_ns=envelope.source_timestamp_ns,
        receive_timestamp_utc_ns=envelope.receive_timestamp_utc_ns,
        arrival_sequence=envelope.arrival_sequence,
        bid_price=bid_price,
        bid_quantity=bid_quantity,
        ask_price=ask_price,
        ask_quantity=ask_quantity,
        spread=spread,
        imbalance=imbalance,
    )


def _l2(envelope: PublicDataEnvelope, data: dict[str, Any]) -> L2SnapshotView:
    sides = data.get("levels")
    if not isinstance(sides, list) or len(sides) != 2:
        raise ValueError("Hyperliquid L2 snapshot must contain two sides")
    levels: list[BookLevel] = []
    for side, raw_levels in zip(("BID", "ASK"), sides, strict=True):
        if not isinstance(raw_levels, list):
            raise ValueError("Hyperliquid L2 side must be an array")
        for index, raw_level in enumerate(raw_levels):
            level = _mapping(raw_level, label="L2 level")
            price = _decimal(level.get("px"))
            quantity = _decimal(level.get("sz"))
            if price is None or quantity is None:
                raise ValueError("Hyperliquid L2 price and quantity are required")
            count = None if level.get("n") is None else int(str(level["n"]))
            levels.append(BookLevel(side, price, quantity, count, index))
    assert envelope.instrument_id is not None
    return L2SnapshotView(
        venue=envelope.venue,
        instrument_id=envelope.instrument_id,
        source_timestamp_ns=envelope.source_timestamp_ns,
        receive_timestamp_utc_ns=envelope.receive_timestamp_utc_ns,
        arrival_sequence=envelope.arrival_sequence,
        levels=tuple(levels),
        is_snapshot=True,
    )


def build_hyperliquid_views(
    envelopes: tuple[PublicDataEnvelope, ...] | list[PublicDataEnvelope],
) -> HyperliquidDerivedViews:
    """Regenerate minimal H1/H3/H4 views without mutating the raw lake."""

    bbo: list[BboView] = []
    l2: list[L2SnapshotView] = []
    trades: list[TradeView] = []
    contexts: list[ActiveContextView] = []
    for envelope in envelopes:
        if envelope.venue is not Venue.HYPERLIQUID:
            continue
        decoded = json.loads(envelope.raw_payload.decode("utf-8"))
        payload = _mapping(decoded, label="Hyperliquid raw frame")
        data = payload.get("data")
        if envelope.feed_type == "bbo":
            bbo.append(_bbo(envelope, _mapping(data, label="BBO data")))
        elif envelope.feed_type == "l2_book":
            l2.append(_l2(envelope, _mapping(data, label="L2 data")))
        elif envelope.feed_type == "trades":
            if not isinstance(data, list):
                raise ValueError("Hyperliquid trades data must be an array")
            assert envelope.instrument_id is not None
            for item in data:
                trade = _mapping(item, label="trade")
                price = _decimal(trade.get("px"))
                quantity = _decimal(trade.get("sz"))
                if price is None or quantity is None:
                    raise ValueError("trade price and quantity are required")
                source_timestamp_ns = int(str(trade["time"])) * 1_000_000
                source_event_id = f"{trade['time']}:{trade['coin']}:{trade['tid']}"
                aggressor = {"B": "BUY", "A": "SELL"}.get(str(trade.get("side")))
                trades.append(
                    TradeView(
                        venue=envelope.venue,
                        instrument_id=envelope.instrument_id,
                        source_timestamp_ns=source_timestamp_ns,
                        receive_timestamp_utc_ns=envelope.receive_timestamp_utc_ns,
                        arrival_sequence=envelope.arrival_sequence,
                        source_event_id=source_event_id,
                        price=price,
                        quantity=quantity,
                        aggressor_side=aggressor,
                        causal_liquidation_label=None,
                    )
                )
        elif envelope.feed_type == "active_asset_context":
            context_payload = _mapping(data, label="active context data")
            context = _mapping(context_payload.get("ctx"), label="active context")
            assert envelope.instrument_id is not None
            contexts.append(
                ActiveContextView(
                    venue=envelope.venue,
                    instrument_id=envelope.instrument_id,
                    source_timestamp_ns=envelope.source_timestamp_ns,
                    receive_timestamp_utc_ns=envelope.receive_timestamp_utc_ns,
                    arrival_sequence=envelope.arrival_sequence,
                    mark_price=_decimal(context.get("markPx")),
                    mid_price=_decimal(context.get("midPx")),
                    oracle_price=_decimal(context.get("oraclePx")),
                    funding=_decimal(context.get("funding")),
                    open_interest=_decimal(context.get("openInterest")),
                    day_notional_volume=_decimal(context.get("dayNtlVlm")),
                )
            )
    return HyperliquidDerivedViews(tuple(bbo), tuple(l2), tuple(trades), tuple(contexts))


@dataclass(frozen=True, slots=True)
class PaperSegmentReference:
    """Compact Paper-journal boundary: one reference per segment, never per tick."""

    manifest_sha256: str
    segment_sha256: str
    segment_index: int
    frame_count: int
    first_arrival_sequence: int
    last_arrival_sequence: int
    raw_summary_sha256: str


def paper_reference_for_segment(
    manifest: ManifestRecord, descriptor: SegmentDescriptor
) -> PaperSegmentReference:
    if descriptor not in manifest.segments:
        raise ValueError("segment is not authenticated by the manifest")
    summary = canonical_json_bytes(
        {
            "collection_id": descriptor.collection_id,
            "first_arrival_sequence": descriptor.first_arrival_sequence,
            "frame_count": descriptor.frame_count,
            "last_arrival_sequence": descriptor.last_arrival_sequence,
            "segment_sha256": descriptor.physical_sha256,
        }
    )
    return PaperSegmentReference(
        manifest_sha256=manifest.manifest_sha256,
        segment_sha256=descriptor.physical_sha256,
        segment_index=descriptor.segment_index,
        frame_count=descriptor.frame_count,
        first_arrival_sequence=descriptor.first_arrival_sequence,
        last_arrival_sequence=descriptor.last_arrival_sequence,
        raw_summary_sha256=hashlib.sha256(summary).hexdigest(),
    )


def paper_references(manifest: ManifestRecord) -> tuple[PaperSegmentReference, ...]:
    return tuple(paper_reference_for_segment(manifest, item) for item in manifest.segments)


@dataclass(frozen=True, slots=True)
class DerivedDatasetIdentity:
    raw_manifest_sha256: str
    raw_root_sha256: str
    model_version: str
    parameters_sha256: str

    @classmethod
    def build(
        cls,
        *,
        manifest: ManifestRecord,
        model_version: str,
        parameters: object,
    ) -> DerivedDatasetIdentity:
        if not model_version:
            raise ValueError("derived dataset model version is required")
        return cls(
            raw_manifest_sha256=manifest.manifest_sha256,
            raw_root_sha256=manifest.root_sha256,
            model_version=model_version,
            parameters_sha256=hashlib.sha256(canonical_json_bytes(parameters)).hexdigest(),
        )


__all__ = [
    "ActiveContextView",
    "BboView",
    "BookLevel",
    "DerivedDatasetIdentity",
    "HyperliquidDerivedViews",
    "L2SnapshotView",
    "PaperSegmentReference",
    "TradeView",
    "build_hyperliquid_views",
    "paper_reference_for_segment",
    "paper_references",
]
