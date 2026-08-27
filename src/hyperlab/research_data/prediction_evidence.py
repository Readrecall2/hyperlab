from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, cast
from urllib.parse import urlsplit

from .adapters import KALSHI_METADATA_VERSION, POLYMARKET_METADATA_VERSION
from .canonical import canonical_json_bytes
from .envelope import PublicDataEnvelope, Venue
from .segments import ResearchSegmentReader

if TYPE_CHECKING:
    from .prediction_contracts import OfficialPublicContract

_SHA256_LENGTH = 64


class _EndpointContract(Protocol):
    @property
    def endpoint_id(self) -> str: ...

    @property
    def method(self) -> str: ...

    @property
    def url(self) -> str: ...


_EXPECTED_METADATA_VERSION = {
    Venue.POLYMARKET: POLYMARKET_METADATA_VERSION,
    Venue.KALSHI: KALSHI_METADATA_VERSION,
}

_FEED_ENDPOINTS: dict[Venue, dict[str, frozenset[str]]] = {
    Venue.POLYMARKET: {
        "events": frozenset({"gamma-event", "gamma-events-keyset"}),
        "fees": frozenset({"clob-fee-rate"}),
        "heartbeat": frozenset({"market-websocket"}),
        "best_bid_ask": frozenset({"market-websocket"}),
        "last_trade_price": frozenset({"clob-last-trade-price", "market-websocket"}),
        "market_batch": frozenset({"market-websocket"}),
        "market_lifecycle": frozenset({"market-websocket"}),
        "metadata": frozenset(
            {
                "clob-market-by-token",
                "clob-market-info",
                "gamma-market",
                "gamma-markets-keyset",
            }
        ),
        "order_book": frozenset({"clob-order-book", "market-websocket"}),
        "public_trades": frozenset({"data-trades"}),
        "price_change": frozenset({"market-websocket"}),
        "tick_size": frozenset({"clob-tick-size"}),
        "tick_size_change": frozenset({"market-websocket"}),
    },
    Venue.KALSHI: {
        "block_trades": frozenset({"trades"}),
        "event_fee_changes": frozenset({"event-fee-changes"}),
        "event_metadata": frozenset({"event-metadata"}),
        "events": frozenset({"event", "events"}),
        "exchange_schedule": frozenset({"exchange-schedule"}),
        "exchange_status": frozenset({"exchange-status"}),
        "fee_changes": frozenset({"series-fee-changes"}),
        "historical_cutoff": frozenset({"historical-cutoff"}),
        "historical_markets": frozenset({"historical-market", "historical-markets"}),
        "historical_trades": frozenset({"historical-trades"}),
        "incentives": frozenset({"incentives"}),
        "markets": frozenset({"market", "markets"}),
        "order_book": frozenset({"single-orderbook"}),
        "series": frozenset({"series", "series-list"}),
        "trades": frozenset({"trades"}),
    },
}


def _endpoint_matches(endpoint: _EndpointContract, source_url: str, transport: str) -> bool:
    expected_transport = "PUBLIC_WEBSOCKET" if endpoint.method == "WSS" else "PUBLIC_HTTP"
    if transport != expected_transport:
        return False
    actual = urlsplit(source_url)
    template = urlsplit(endpoint.url)
    if actual.scheme != template.scheme or actual.hostname != template.hostname:
        return False
    actual_parts = tuple(item for item in actual.path.split("/") if item)
    template_parts = tuple(item for item in template.path.split("/") if item)
    if len(actual_parts) != len(template_parts):
        return False
    return all(
        template_item == actual_item
        or (template_item.startswith("{") and template_item.endswith("}"))
        for template_item, actual_item in zip(template_parts, actual_parts, strict=True)
    )


def _validate_official_provenance(
    envelope: PublicDataEnvelope,
    contract: OfficialPublicContract,
) -> None:
    if contract.venue is not envelope.venue:
        raise ValueError("prediction raw contract venue diverged")
    expected_version = _EXPECTED_METADATA_VERSION[envelope.venue]
    if envelope.source_metadata_version != expected_version:
        raise ValueError("prediction raw source metadata version is not the frozen adapter contract")
    if envelope.provenance.transport == "FIXTURE":
        if (
            envelope.provenance.fixture_label != "SYNTHETIC/FIXTURE"
            or not envelope.provenance.source_url.startswith("fixture://")
        ):
            raise ValueError("prediction fixture provenance is not explicit")
        return
    allowed_ids = _FEED_ENDPOINTS[envelope.venue].get(envelope.feed_type)
    if (
        allowed_ids is None
        and envelope.venue is Venue.POLYMARKET
        and envelope.feed_type.startswith("unknown_")
    ):
        allowed_ids = frozenset({"market-websocket"})
    if not allowed_ids:
        raise ValueError("prediction raw feed has no official endpoint contract")
    endpoints = tuple(item for item in contract.endpoints if item.endpoint_id in allowed_ids)
    if not endpoints or not any(
        _endpoint_matches(endpoint, envelope.provenance.source_url, envelope.provenance.transport)
        for endpoint in endpoints
    ):
        raise ValueError("prediction raw provenance does not match its official endpoint contract")


@dataclass(frozen=True, slots=True)
class PredictionRawRecordRef:
    content_sha256: str
    arrival_sequence: int
    raw_record_index: int
    raw_record_sha256: str

    def __post_init__(self) -> None:
        for value in (self.content_sha256, self.raw_record_sha256):
            if len(value) != _SHA256_LENGTH or any(
                character not in "0123456789abcdef" for character in value
            ):
                raise ValueError("prediction raw record reference requires SHA-256 identities")
        if self.arrival_sequence <= 0 or self.raw_record_index < 0:
            raise ValueError("prediction raw record reference counters are invalid")

    def to_dict(self) -> dict[str, int | str]:
        return {
            "arrival_sequence": self.arrival_sequence,
            "content_sha256": self.content_sha256,
            "raw_record_index": self.raw_record_index,
            "raw_record_sha256": self.raw_record_sha256,
        }


def _mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return cast(Mapping[str, Any], value)


def prediction_raw_records(envelope: PublicDataEnvelope) -> tuple[Mapping[str, Any], ...]:
    try:
        decoded = json.loads(envelope.raw_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("prediction raw evidence must be strict UTF-8 JSON") from error
    if isinstance(decoded, list):
        return tuple(_mapping(item, label="prediction raw array record") for item in decoded)
    root = _mapping(decoded, label="prediction raw payload")
    # Gamma market records embed an ``events`` array and event records embed a
    # ``markets`` array.  Those are identity edges, not page wrappers.  Resolve
    # direct records before consulting wrapper keys so a market cannot be
    # silently replaced by its first embedded event (or conversely).
    if envelope.feed_type == "metadata" and any(
        key in root for key in ("conditionId", "condition_id", "fd", "questionID")
    ):
        return (root,)
    if envelope.feed_type == "events" and (
        ("id" in root and "markets" in root) or "event_ticker" in root
    ):
        return (root,)
    if envelope.feed_type == "event_metadata":
        if "event_metadata" in root:
            raise ValueError("Kalshi event metadata wrapper is not part of the current contract")
        return (root,)
    singular_keys = {
        "events": ("event",),
        "historical_markets": ("market",),
        "markets": ("market",),
        "metadata": ("market", "event"),
        "order_book": ("orderbook_fp",),
        "series": ("series",),
    }
    for key in singular_keys.get(envelope.feed_type, ()):
        record = root.get(key)
        if isinstance(record, Mapping):
            return (cast(Mapping[str, Any], record),)
    sequence_keys = {
        "block_trades": ("trades",),
        "event_fee_changes": ("fee_changes", "event_fee_changes"),
        "events": ("events",),
        "fee_changes": ("fee_changes",),
        "historical_markets": ("markets",),
        "historical_trades": ("trades",),
        "incentives": ("incentive_programs", "incentives"),
        "markets": ("markets",),
        "metadata": ("markets", "events"),
        "public_trades": ("trades",),
        "series": ("series",),
        "trades": ("trades",),
    }
    sequence_wrappers = tuple(
        (key, records)
        for key in sequence_keys.get(envelope.feed_type, ())
        if isinstance((records := root.get(key)), list)
    )
    if len(sequence_wrappers) > 1:
        raise ValueError("prediction raw payload contains ambiguous sequence wrappers")
    if sequence_wrappers:
        key, records = sequence_wrappers[0]
        return tuple(_mapping(item, label=f"prediction {key} record") for item in records)
    return (root,)


def prediction_raw_record_ref(
    envelope: PublicDataEnvelope,
    raw_record_index: int,
) -> PredictionRawRecordRef:
    records = prediction_raw_records(envelope)
    if raw_record_index < 0 or raw_record_index >= len(records):
        raise ValueError("prediction raw record index is outside the payload")
    record_sha256 = hashlib.sha256(canonical_json_bytes(records[raw_record_index])).hexdigest()
    return PredictionRawRecordRef(
        content_sha256=envelope.content_sha256,
        arrival_sequence=envelope.arrival_sequence,
        raw_record_index=raw_record_index,
        raw_record_sha256=record_sha256,
    )


class PredictionRawEvidenceIndex:
    def __init__(
        self,
        reader: ResearchSegmentReader,
        *,
        contracts: Mapping[Venue, OfficialPublicContract] | None = None,
    ) -> None:
        envelopes = reader.replay()
        if not envelopes:
            raise ValueError("prediction raw evidence index requires a non-empty manifest")
        selected_contracts = {} if contracts is None else dict(contracts)
        for envelope in envelopes:
            if envelope.venue not in {Venue.POLYMARKET, Venue.KALSHI}:
                continue
            contract = selected_contracts.get(envelope.venue)
            if contract is None:
                if envelope.provenance.transport != "FIXTURE":
                    raise ValueError("public prediction raw evidence requires an official contract")
                if envelope.source_metadata_version != _EXPECTED_METADATA_VERSION[envelope.venue]:
                    raise ValueError("prediction fixture metadata version diverged")
                continue
            _validate_official_provenance(envelope, contract)
        self.manifest_sha256 = reader.manifest.manifest_sha256
        self.root_sha256 = reader.manifest.root_sha256
        self._by_identity = {(item.content_sha256, item.arrival_sequence): item for item in envelopes}
        if len(self._by_identity) != len(envelopes):
            raise ValueError("prediction raw evidence envelope identity is ambiguous")

    @property
    def envelopes(self) -> tuple[PublicDataEnvelope, ...]:
        return tuple(sorted(self._by_identity.values(), key=lambda item: item.arrival_sequence))

    def require_envelope(
        self,
        reference: PredictionRawRecordRef,
        *,
        venue: Venue,
        allowed_feeds: Sequence[str],
    ) -> PublicDataEnvelope:
        envelope = self._by_identity.get((reference.content_sha256, reference.arrival_sequence))
        if envelope is None:
            raise ValueError("prediction raw record is absent from authenticated manifest")
        if envelope.venue is not venue or envelope.feed_type not in set(allowed_feeds):
            raise ValueError("prediction raw record venue or feed is not admissible")
        expected = prediction_raw_record_ref(envelope, reference.raw_record_index)
        if expected != reference:
            raise ValueError("prediction raw record reference diverged from payload")
        return envelope

    def require_record(
        self,
        reference: PredictionRawRecordRef,
        *,
        venue: Venue,
        allowed_feeds: Sequence[str],
    ) -> tuple[PublicDataEnvelope, Mapping[str, Any]]:
        envelope = self.require_envelope(
            reference,
            venue=venue,
            allowed_feeds=allowed_feeds,
        )
        return envelope, prediction_raw_records(envelope)[reference.raw_record_index]


__all__ = [
    "PredictionRawEvidenceIndex",
    "PredictionRawRecordRef",
    "prediction_raw_record_ref",
    "prediction_raw_records",
]
