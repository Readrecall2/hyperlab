from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType

from hyperlab.collector.models import ParsedRecord
from hyperlab.data.schema import RecordType, instrument, latest_schema_for, parse_instrument
from hyperlab.paper.models import MarketEvent
from hyperlab.paper.runtime import PUBLIC_MARKET_SCHEMA_VERSION, PublicSourceDescriptor


class PublicRecordAdapterError(ValueError):
    """A supported normalized record cannot be admitted without guessing."""


class PublicRecordQueueFull(RuntimeError):
    """The bounded paper-source queue saturated and the source stopped fail-closed."""


class PublicRecordSourceClosed(RuntimeError):
    """Records cannot be fed after the paper source has stopped or closed."""


class _StreamStatus(StrEnum):
    READY = "READY"
    AWAITING_BOOK = "AWAITING_BOOK"
    GAPPED = "GAPPED"
    STALE = "STALE"


@dataclass(frozen=True, slots=True)
class _BookState:
    instrument: str
    bid_price: Decimal
    ask_price: Decimal
    bid_depth: Decimal
    ask_depth: Decimal


@dataclass(frozen=True, slots=True)
class PublicFundingSettlement:
    """Immutable normalized public funding settlement with stable economic identity."""

    event_id: str
    instrument: str
    funding_time: datetime
    received_at: datetime
    funding_rate: Decimal
    funding_interval_seconds: int
    rate_kind: str
    mark_price: Decimal | None
    oracle_price: Decimal | None
    source_observation_id: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.event_id, str)
            or len(self.event_id) != 64
            or any(character not in "0123456789abcdef" for character in self.event_id)
        ):
            raise ValueError("public funding event_id must be a lowercase SHA-256 digest")
        _required_text(self.instrument, label="funding instrument")
        funding_time = _utc(self.funding_time, label="funding_time")
        received_at = _utc(self.received_at, label="funding received_at")
        if funding_time > received_at:
            raise ValueError("public funding settlement cannot be received before funding_time")
        object.__setattr__(self, "funding_time", funding_time)
        object.__setattr__(self, "received_at", received_at)
        if not isinstance(self.funding_rate, Decimal) or not self.funding_rate.is_finite():
            raise ValueError("public funding rate must be a finite Decimal")
        if (
            isinstance(self.funding_interval_seconds, bool)
            or not isinstance(self.funding_interval_seconds, int)
            or self.funding_interval_seconds <= 0
        ):
            raise ValueError("public funding interval must be a positive integer")
        _required_text(self.rate_kind, label="funding rate_kind")
        _required_text(self.source_observation_id, label="funding source_observation_id")
        for label, value in (
            ("funding mark_price", self.mark_price),
            ("funding oracle_price", self.oracle_price),
        ):
            if value is not None and (not isinstance(value, Decimal) or not value.is_finite() or value <= 0):
                raise ValueError(f"{label} must be a finite positive Decimal when present")


PublicSourceItem = Mapping[str, MarketEvent] | PublicFundingSettlement


_BboCoalesceKey = tuple[str, int]


@dataclass(frozen=True, slots=True)
class _QueuedPublicSourceItem:
    item: PublicSourceItem
    received_at: datetime
    bbo_coalesce_key: _BboCoalesceKey | None


_SourceKey = tuple[str, str]
_SUPPORTED_RECORD_TYPES = frozenset({RecordType.BBO, RecordType.CONNECTION_EVENT, RecordType.FUNDING})
_GAP_EVENTS = frozenset({"disconnect", "gap", "resync_start"})
_AWAITING_BOOK_EVENTS = frozenset({"connect", "resync_complete"})
_ADAPTER_SCHEMA_VERSION = 9
_PENDING_BBO_COALESCING = "LATEST_PER_INSTRUMENT_PER_UTC_MINUTE_BETWEEN_CONTROL_BARRIERS_V1"
_FEED_CONTRACT = "SOLE_COLLECTOR_NORMALIZED_BBO_CONNECTION_FUNDING_BOUNDED_PENDING_BBO_LATEST_VALUE_V9"
_GLOBAL_CONNECTION_POLICY = (
    "MULTI_INSTRUMENT_GLOBAL_EVENT_SORTED_ORDINAL_INITIAL_BOOTSTRAP_CONNECT_HEALTH_ONLY_V4"
)
_BBO_TRADABILITY_POLICY = (
    "REST_BOOTSTRAP_NONTRADABLE_POST_CONNECT_EXACT_WEBSOCKET_LINEAGE_REQUIRED_MALFORMED_TERMINAL_V2"
)
_MALFORMED_BBO_POLICY = "TERMINAL_SOURCE_FAILURE_RESTART_AND_RESYNC_REQUIRED_NO_SILENT_DROP_V1"
_INSTRUMENT_ROUTE_POLICY = "EXPLICIT_MAPPING_REQUIRES_SEPARATE_METADATA_REVIEW_V1"
_SOURCE_VENUE_TO_PAPER_EXCHANGE: Mapping[str, str] = MappingProxyType({"hyperliquid": "HL"})
_UINT64_MAX = (1 << 64) - 1
_LINEAGE_FIELDS = (
    "update_id",
    "source_sequence",
    "connection_id",
    "connection_epoch",
    "arrival_sequence",
    "capture_epoch_id",
    "snapshot_id",
    "resync_snapshot_id",
    "event_kind",
    "socket_role",
)


def _utc(value: object, *, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise PublicRecordAdapterError(f"{label} must be a datetime")
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise PublicRecordAdapterError(f"{label} must be an explicit UTC timestamp")
    return value.astimezone(UTC)


def _required_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PublicRecordAdapterError(f"{label} must be a non-empty string")
    if value != value.strip():
        raise PublicRecordAdapterError(f"{label} must already be normalized")
    return value


def _positive_decimal(value: object) -> Decimal | None:
    if not isinstance(value, Decimal):
        return None
    if not value.is_finite() or value <= 0:
        return None
    return value


def _optional_schema_decimal(value: object, *, label: str) -> None:
    if value is None:
        return
    if not isinstance(value, Decimal) or not value.is_finite():
        raise PublicRecordAdapterError(f"{label} must be a finite Decimal when present")


def _optional_uint64(value: object, *, label: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > _UINT64_MAX:
        raise PublicRecordAdapterError(f"{label} must be an unsigned 64-bit integer when present")


def _lineage_text(value: object, *, label: str) -> str:
    if isinstance(value, bool):
        raise PublicRecordAdapterError(f"{label} cannot be boolean")
    if isinstance(value, datetime):
        return _utc(value, label=label).isoformat(timespec="microseconds")
    if isinstance(value, (str, int, Decimal)):
        text = str(value)
        if not text:
            raise PublicRecordAdapterError(f"{label} cannot be empty")
        return text
    raise PublicRecordAdapterError(f"{label} has an unsupported lineage value")


class PublicRecordMarketEventAdapter:
    """Pure, stateful projection from normalized public records to paper events.

    Instrument resolution is explicit because a venue/asset pair alone does not
    prove whether the normalized stream represents spot or perpetual exposure.
    The mapping is bound into the descriptor but remains an admission-time
    metadata assertion, not self-proving economic identity. The adapter owns no
    transport and writes no lake data.
    """

    def __init__(
        self,
        *,
        instruments: Mapping[_SourceKey, str],
        queue_capacity: int,
        funding_dedupe_capacity: int = 4_096,
        identity_context: Mapping[str, object] | None = None,
        include_market_context: bool = False,
        product_identity_hashes: Mapping[str, str] | None = None,
    ) -> None:
        if not instruments:
            raise ValueError("at least one explicit public instrument mapping is required")
        if isinstance(queue_capacity, bool) or not isinstance(queue_capacity, int) or queue_capacity < 1:
            raise ValueError("queue_capacity must be a positive integer")
        if (
            isinstance(funding_dedupe_capacity, bool)
            or not isinstance(funding_dedupe_capacity, int)
            or funding_dedupe_capacity < 1
        ):
            raise ValueError("funding_dedupe_capacity must be a positive integer")
        if identity_context is not None and not isinstance(identity_context, Mapping):
            raise TypeError("identity_context must be a mapping when present")
        raw_identity_context = {} if identity_context is None else identity_context
        if any(not isinstance(key, str) or not key for key in raw_identity_context):
            raise ValueError("identity_context keys must be non-empty strings")
        try:
            transport_identity = json.loads(
                json.dumps(
                    dict(raw_identity_context),
                    allow_nan=False,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("identity_context must be canonical JSON data") from exc

        normalized: dict[_SourceKey, str] = {}
        seen_instruments: set[str] = set()
        for raw_key, raw_instrument in instruments.items():
            if not isinstance(raw_key, tuple) or len(raw_key) != 2:
                raise ValueError("instrument mapping keys must be (venue, asset) tuples")
            venue = _required_text(raw_key[0], label="mapping venue")
            asset = _required_text(raw_key[1], label="mapping asset")
            instrument = _required_text(raw_instrument, label="paper instrument")
            if venue != venue.casefold():
                raise ValueError("public source venue must use its normalized lowercase name")
            if asset != asset.upper():
                raise ValueError("public source asset must use its normalized uppercase name")
            expected_exchange = _SOURCE_VENUE_TO_PAPER_EXCHANGE.get(venue)
            if expected_exchange is None:
                raise ValueError(f"unsupported public source venue: {venue}")
            exchange, instrument_asset, _kind = parse_instrument(instrument)
            if not exchange or not instrument_asset:
                raise ValueError("paper instrument exchange and asset must not be empty")
            if exchange != expected_exchange or (instrument_asset != asset and not include_market_context):
                raise ValueError("public source venue/asset must match the canonical HL paper instrument")
            if instrument in seen_instruments:
                raise ValueError("each paper instrument must have exactly one public source")
            normalized[(venue, asset)] = instrument
            seen_instruments.add(instrument)

        if not isinstance(include_market_context, bool):
            raise TypeError("include_market_context must be a boolean")
        if not include_market_context and product_identity_hashes is not None:
            raise ValueError("product_identity_hashes require include_market_context=True")
        product_identities: dict[str, str] = {}
        if include_market_context:
            if not isinstance(product_identity_hashes, Mapping):
                raise ValueError("market context projection requires product_identity_hashes")
            if set(product_identity_hashes) != seen_instruments:
                raise ValueError("product_identity_hashes must exactly cover mapped instruments")
            for instrument, digest in product_identity_hashes.items():
                if (
                    not isinstance(digest, str)
                    or len(digest) != 64
                    or any(character not in "0123456789abcdef" for character in digest)
                ):
                    raise ValueError(f"product identity for {instrument} must be a lowercase SHA-256 digest")
                product_identities[instrument] = digest

        self._instruments = MappingProxyType(normalized)
        self._include_market_context = include_market_context
        self._product_identity_hashes = MappingProxyType(product_identities)
        self._market_contexts: dict[_SourceKey, Mapping[str, object]] = {}
        self._queue_capacity = queue_capacity
        self._books: dict[_SourceKey, _BookState] = {}
        self._funding_dedupe_capacity = funding_dedupe_capacity
        self._status = {key: _StreamStatus.READY for key in normalized}
        self._admitted_websocket_lineage: dict[_SourceKey, tuple[str, int] | None] = {
            key: None for key in normalized
        }
        self._last_received_at: datetime | None = None
        self._last_arrival_sequence_by_connection: dict[
            tuple[str, int], tuple[int, str, str, str]
        ] = {}
        self._last_connection_epoch_by_route: dict[_SourceKey, int] = {}
        self._funding_signatures: OrderedDict[
            str,
            tuple[Decimal, int, Decimal | None, Decimal | None],
        ] = OrderedDict()
        identity: dict[str, object] = {
            "adapter_schema_version": _ADAPTER_SCHEMA_VERSION,
            "bbo_tradability_policy": _BBO_TRADABILITY_POLICY,
            "feed_contract": _FEED_CONTRACT,
            "funding_dedupe_capacity_settlements": funding_dedupe_capacity,
            "global_connection_policy": _GLOBAL_CONNECTION_POLICY,
            "malformed_bbo_policy": _MALFORMED_BBO_POLICY,
            "pending_bbo_coalescing": _PENDING_BBO_COALESCING,
            "instrument_route_policy": _INSTRUMENT_ROUTE_POLICY,
            "instruments": [
                {
                    "asset": asset,
                    "instrument": normalized[(venue, asset)],
                    "venue": venue,
                }
                for venue, asset in sorted(normalized)
            ],
            "normalized_record_schema_versions": {
                record_type.value: latest_schema_for(record_type).version
                for record_type in sorted(
                    _SUPPORTED_RECORD_TYPES,
                    key=lambda item: item.value,
                )
            },
            "paper_market_schema_version": PUBLIC_MARKET_SCHEMA_VERSION,
            "public_only": True,
            "queue_capacity_frames": queue_capacity,
            "source_venue_aliases": [
                {
                    "paper_exchange": paper_exchange,
                    "source_venue": source_venue,
                }
                for source_venue, paper_exchange in sorted(_SOURCE_VENUE_TO_PAPER_EXCHANGE.items())
            ],
            "trade_projection": "BLOCKED_RESTART_DURABLE_IDENTITY_UNAVAILABLE",
            "transport": transport_identity,
        }
        if include_market_context:
            schema_versions = identity["normalized_record_schema_versions"]
            if not isinstance(schema_versions, dict):
                raise AssertionError("normalized schema identity must be mutable during construction")
            schema_versions[RecordType.MARKET_CONTEXT.value] = latest_schema_for(
                RecordType.MARKET_CONTEXT
            ).version
            identity.update(
                {
                    "adapter_schema_version": 10,
                    "feed_contract": (
                        "SOLE_COLLECTOR_NORMALIZED_BBO_CONNECTION_FUNDING_MARKET_CONTEXT_"
                        "BOUNDED_PENDING_BBO_LATEST_VALUE_V10"
                    ),
                    "funding_time_policy": "HYPERLIQUID_FUNDING_HISTORY_FIRST_60_SECONDS_CANONICAL_EXACT_UTC_HOUR_V1",
                    "instrument_route_policy": "EXPLICIT_MAPPING_PRODUCT_IDENTITY_BOUND_V2",
                    "market_context_policy": "LATEST_CAUSAL_CONTEXT_ATTACHED_TO_BBO_V1",
                    "normalized_order_policy": (
                        "COLLECTOR_FIFO_PRODUCER_SCOPED_CONNECTION_ID_"
                        "PER_CONNECTION_ARRIVAL_SEQUENCE_PER_ROUTE_CONNECTION_EPOCH_V2"
                    ),
                    "product_identity_hashes": dict(sorted(product_identities.items())),
                }
            )
        self._identity_artifact_bytes = json.dumps(
            identity,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        self._identity_hash = hashlib.sha256(self._identity_artifact_bytes).hexdigest()

    @property
    def instruments(self) -> Mapping[_SourceKey, str]:
        return self._instruments

    @property
    def identity_artifact_bytes(self) -> bytes:
        return self._identity_artifact_bytes

    @property
    def identity_hash(self) -> str:
        return self._identity_hash

    @property
    def queue_capacity(self) -> int:
        return self._queue_capacity

    def adapt(self, record: ParsedRecord) -> PublicSourceItem | None:
        """Adapt one already-normalized record into an immutable Paper source item."""

        if not isinstance(record, ParsedRecord):
            raise TypeError("record must be a ParsedRecord")
        if not isinstance(record.record_type, RecordType):
            raise PublicRecordAdapterError("ParsedRecord.record_type must be a RecordType")
        if record.record_type is RecordType.TRADE:
            raise PublicRecordAdapterError(
                "trade projection is disabled until restart-durable venue trade "
                "identity is persisted and restored"
            )
        supported_record_types = _SUPPORTED_RECORD_TYPES
        if self._include_market_context:
            supported_record_types = supported_record_types | {RecordType.MARKET_CONTEXT}
        if record.record_type not in supported_record_types:
            return None
        if not isinstance(record.row, Mapping):
            raise PublicRecordAdapterError("ParsedRecord.row must be a mapping")
        self._validate_normalized_row(record)

        venue, asset = self._record_identity(record)
        received_at = _utc(record.row.get("received_time"), label="received_time")
        if record.record_type is not RecordType.FUNDING:
            if self._include_market_context:
                self._observe_v10_order(
                    record,
                    venue=venue,
                    asset=asset,
                    received_at=received_at,
                )
            else:
                self._observe_order(received_at)

        if record.record_type is RecordType.CONNECTION_EVENT:
            return self._adapt_connection(record, venue=venue, asset=asset, received_at=received_at)

        key = (venue, asset)
        if key not in self._instruments:
            return None
        if record.record_type is RecordType.MARKET_CONTEXT:
            self._adapt_market_context(record, key=key, received_at=received_at)
            return None
        if record.record_type is RecordType.FUNDING:
            return self._adapt_funding(record, key=key, received_at=received_at)
        return self._adapt_bbo(record, key=key, received_at=received_at)

    @staticmethod
    def _validate_normalized_row(record: ParsedRecord) -> None:
        row = record.row
        spec = latest_schema_for(record.record_type)
        expected = set(spec.schema.names)
        if any(not isinstance(name, str) for name in row):
            raise PublicRecordAdapterError("normalized row keys must all be strings")
        actual = set(row)
        if actual != expected:
            missing = sorted(expected - actual)
            unexpected = sorted(actual - expected)
            raise PublicRecordAdapterError(
                "normalized row does not exactly match the claimed schema; "
                f"missing={missing}, unexpected={unexpected}"
            )
        schema_version = row.get("schema_version")
        if type(schema_version) is not int or schema_version != spec.version:
            raise PublicRecordAdapterError(
                f"schema_version must equal latest {record.record_type.value} v{spec.version}"
            )
        if row.get("record_type") != record.record_type.value:
            raise PublicRecordAdapterError("row record_type does not match ParsedRecord.record_type")
        _utc(row.get("event_time"), label="event_time")
        exchange_time = row.get("exchange_time")
        if exchange_time is not None:
            _utc(exchange_time, label="exchange_time")
        _optional_uint64(row.get("source_sequence"), label="source_sequence")
        connection_id = row.get("connection_id")
        if connection_id is not None:
            _required_text(connection_id, label="connection_id")
        if record.record_type is RecordType.BBO:
            _required_text(row.get("update_id"), label="update_id")
            for name in (
                "bid_price",
                "bid_quantity",
                "ask_price",
                "ask_quantity",
            ):
                _optional_schema_decimal(row.get(name), label=name)
            return
        if record.record_type is RecordType.MARKET_CONTEXT:
            kind = _required_text(row.get("instrument_kind"), label="instrument_kind")
            if kind not in {"spot", "perp"}:
                raise PublicRecordAdapterError("market context kind must be spot or perp")
            _required_text(row.get("instrument_id"), label="instrument_id")
            _required_text(row.get("observation_id"), label="observation_id")
            for name in (
                "mark_price",
                "oracle_price",
                "mid_price",
                "current_funding_rate",
                "open_interest_quantity",
                "open_interest_notional",
                "base_volume_24h",
                "notional_volume_24h",
                "previous_day_price",
                "circulating_supply",
            ):
                _optional_schema_decimal(row.get(name), label=name)
            for name in ("mark_price", "oracle_price", "mid_price"):
                value = row.get(name)
                if isinstance(value, Decimal) and value <= 0:
                    raise PublicRecordAdapterError(f"{name} must be positive when present")
            for name in (
                "open_interest_quantity",
                "open_interest_notional",
                "base_volume_24h",
                "notional_volume_24h",
                "previous_day_price",
                "circulating_supply",
            ):
                value = row.get(name)
                if isinstance(value, Decimal) and value < 0:
                    raise PublicRecordAdapterError(f"{name} must be non-negative when present")
            return
        if record.record_type is RecordType.FUNDING:
            funding_time = _utc(row.get("funding_time"), label="funding_time")
            if row.get("event_time") != funding_time or exchange_time != funding_time:
                raise PublicRecordAdapterError("funding event_time and exchange_time must equal funding_time")
            funding_rate = row.get("funding_rate")
            if not isinstance(funding_rate, Decimal) or not funding_rate.is_finite():
                raise PublicRecordAdapterError("funding_rate must be a finite Decimal")
            interval = row.get("funding_interval_seconds")
            if isinstance(interval, bool) or not isinstance(interval, int) or interval <= 0:
                raise PublicRecordAdapterError("funding_interval_seconds must be a positive integer")
            _required_text(row.get("rate_kind"), label="rate_kind")
            _required_text(row.get("observation_id"), label="observation_id")
            for name in ("mark_price", "oracle_price"):
                _optional_schema_decimal(row.get(name), label=name)
                price = row.get(name)
                if isinstance(price, Decimal) and price <= 0:
                    raise PublicRecordAdapterError(f"{name} must be positive when present")
            return

        for name in (
            "channel",
            "book_epoch_id",
            "reason",
            "resync_snapshot_id",
            "capture_epoch_id",
            "socket_role",
        ):
            value = row.get(name)
            if value is not None:
                _required_text(value, label=name)
        for name in (
            "expected_sequence",
            "observed_sequence",
            "connection_epoch",
        ):
            _optional_uint64(row.get(name), label=name)
        _required_text(row.get("event_kind"), label="event_kind")
        if connection_id is None or (
            row.get("connection_epoch") is None and row.get("capture_epoch_id") is None
        ):
            raise PublicRecordAdapterError("connection event lacks stable connection epoch lineage")

    def _record_identity(self, record: ParsedRecord) -> _SourceKey:
        venue = _required_text(record.row.get("venue"), label="venue")
        row_asset = _required_text(record.row.get("asset"), label="asset")
        record_asset = _required_text(record.asset, label="ParsedRecord.asset")
        if row_asset != record_asset:
            raise PublicRecordAdapterError("ParsedRecord.asset does not match row asset")
        return venue, row_asset

    @staticmethod
    def _causal_arrival_lineage(record: ParsedRecord) -> tuple[str, int, int] | None:
        row = record.row
        connection_id = row.get("connection_id")
        if not isinstance(connection_id, str) or not connection_id:
            return None
        if record.record_type is RecordType.BBO:
            identity = row.get("update_id")
            suffix_prefix = f":{connection_id}"
        elif record.record_type is RecordType.MARKET_CONTEXT:
            identity = row.get("observation_id")
            suffix_prefix = connection_id
        else:
            return None
        if not isinstance(identity, str):
            return None
        parts = identity.rsplit(":", 2)
        if len(parts) != 3:
            return None
        if record.record_type is RecordType.BBO:
            if not parts[0].endswith(suffix_prefix):
                return None
        elif parts[0] != suffix_prefix:
            return None
        epoch_text, arrival_text = parts[1:]
        if not epoch_text.isdecimal() or not arrival_text.isdecimal():
            return None
        epoch = int(epoch_text)
        arrival_sequence = int(arrival_text)
        if not 1 <= epoch <= _UINT64_MAX or not 1 <= arrival_sequence <= _UINT64_MAX:
            return None
        return connection_id, epoch, arrival_sequence

    def _observe_v10_order(
        self,
        record: ParsedRecord,
        *,
        venue: str,
        asset: str,
        received_at: datetime,
    ) -> None:
        lineage = self._causal_arrival_lineage(record)
        raw_epoch = record.row.get("connection_epoch")
        epoch = (
            lineage[1]
            if lineage is not None
            else raw_epoch
            if isinstance(raw_epoch, int)
            and not isinstance(raw_epoch, bool)
            and 1 <= raw_epoch <= _UINT64_MAX
            else None
        )
        routes = (
            tuple(key for key in self._instruments if key[0] == venue)
            if asset == "GLOBAL"
            else ((venue, asset),)
            if (venue, asset) in self._instruments
            else ()
        )
        if epoch is not None:
            for route in routes:
                previous_epoch = self._last_connection_epoch_by_route.get(route)
                if previous_epoch is not None and epoch < previous_epoch:
                    raise PublicRecordAdapterError(
                        "normalized records regress per-route connection_epoch"
                    )
        current_arrival = (
            lineage[2],
            record.record_type.value,
            venue,
            asset,
        ) if lineage is not None else None
        if lineage is not None and current_arrival is not None:
            connection_key = lineage[:2]
            previous_arrival = self._last_arrival_sequence_by_connection.get(connection_key)
            if previous_arrival is not None and current_arrival[0] < previous_arrival[0]:
                previous_lineage = (*connection_key, *previous_arrival)
                current_lineage = (*connection_key, *current_arrival)
                raise PublicRecordAdapterError(
                    "normalized records regress per-producer connection arrival_sequence; "
                    f"previous={previous_lineage!r}; current={current_lineage!r}"
                )
        elif epoch is None:
            self._observe_order(received_at)

        if epoch is not None:
            for route in routes:
                self._last_connection_epoch_by_route[route] = epoch
        if lineage is not None and current_arrival is not None:
            self._last_arrival_sequence_by_connection[lineage[:2]] = current_arrival

    def _observe_order(self, received_at: datetime) -> None:
        if self._last_received_at is not None and received_at < self._last_received_at:
            raise PublicRecordAdapterError("normalized records are out of received_time order")
        self._last_received_at = received_at

    def _source_sequence(
        self,
        record: ParsedRecord,
        *,
        venue: str,
        asset: str,
    ) -> int:
        row = record.row
        if record.record_type is RecordType.BBO:
            has_identity = row.get("update_id") is not None
        else:
            has_identity = (
                row.get("connection_id") is not None
                and row.get("event_kind") is not None
                and (row.get("connection_epoch") is not None or row.get("capture_epoch_id") is not None)
            )
        if not has_identity:
            raise PublicRecordAdapterError(f"{record.record_type.value} lacks stable public lineage")

        parts = [
            ("record_type", record.record_type.value),
            ("venue", venue),
            ("asset", asset),
        ]
        for field in _LINEAGE_FIELDS:
            value = row.get(field)
            if value is not None:
                parts.append((field, _lineage_text(value, label=field)))
        payload = "\n".join(f"{name}:{len(value)}:{value}" for name, value in parts)
        return int.from_bytes(hashlib.sha256(payload.encode("utf-8")).digest()[:16], "big")

    def _adapt_market_context(
        self,
        record: ParsedRecord,
        *,
        key: _SourceKey,
        received_at: datetime,
    ) -> None:
        row = record.row
        expected_kind = parse_instrument(self._instruments[key])[2]
        observed_kind = _required_text(row.get("instrument_kind"), label="instrument_kind")
        if observed_kind != expected_kind:
            raise PublicRecordAdapterError("market context kind differs from the frozen canonical instrument")
        observed_instrument_id = _required_text(
            row.get("instrument_id"),
            label="instrument_id",
        )
        expected_source_instrument_id = instrument(key[0], key[1], expected_kind)
        if observed_instrument_id != expected_source_instrument_id:
            raise PublicRecordAdapterError(
                "market context instrument_id differs from the frozen source route"
            )
        context = {
            "base_volume_24h": row.get("base_volume_24h"),
            "circulating_supply": row.get("circulating_supply"),
            "current_funding_rate": row.get("current_funding_rate"),
            "instrument_id": self._instruments[key],
            "instrument_kind": observed_kind,
            "mark_price": row.get("mark_price"),
            "mid_price": row.get("mid_price"),
            "notional_volume_24h": row.get("notional_volume_24h"),
            "observation_id": _required_text(row.get("observation_id"), label="observation_id"),
            "open_interest_notional": row.get("open_interest_notional"),
            "open_interest_quantity": row.get("open_interest_quantity"),
            "oracle_price": row.get("oracle_price"),
            "previous_day_price": row.get("previous_day_price"),
            "product_identity_sha256": self._product_identity_hashes[self._instruments[key]],
            "received_at": received_at,
            "source_asset": key[1],
            "source_venue": key[0],
        }
        self._market_contexts[key] = MappingProxyType(context)
        return None

    def _adapt_funding(
        self,
        record: ParsedRecord,
        *,
        key: _SourceKey,
        received_at: datetime,
    ) -> PublicFundingSettlement | None:
        row = record.row
        funding_time = _utc(row.get("funding_time"), label="funding_time")
        if funding_time > received_at:
            raise PublicRecordAdapterError("funding settlement cannot be received before funding_time")
        funding_rate_value = row.get("funding_rate")
        if not isinstance(funding_rate_value, Decimal) or not funding_rate_value.is_finite():
            raise PublicRecordAdapterError("funding_rate must be a finite Decimal")
        interval_value = row.get("funding_interval_seconds")
        if isinstance(interval_value, bool) or not isinstance(interval_value, int) or interval_value <= 0:
            raise PublicRecordAdapterError("funding_interval_seconds must be a positive integer")
        rate_kind = _required_text(row.get("rate_kind"), label="rate_kind")
        if rate_kind != "hyperliquid-hourly-settlement":
            raise PublicRecordAdapterError(
                "only finalized Hyperliquid hourly funding settlements are supported"
            )
        observation_id = _required_text(
            row.get("observation_id"),
            label="observation_id",
        )
        mark_value = row.get("mark_price")
        oracle_value = row.get("oracle_price")
        mark_price = mark_value if isinstance(mark_value, Decimal) else None
        oracle_price = oracle_value if isinstance(oracle_value, Decimal) else None
        identity_payload = json.dumps(
            {
                "funding_time": funding_time.isoformat(timespec="microseconds"),
                "instrument": self._instruments[key],
                "rate_kind": rate_kind,
                "schema": "paper-public-funding-settlement-v1",
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        event_id = hashlib.sha256(identity_payload).hexdigest()
        signature = (
            funding_rate_value,
            interval_value,
            mark_price,
            oracle_price,
        )
        previous = self._funding_signatures.get(event_id)
        if previous is not None:
            if previous != signature:
                raise PublicRecordAdapterError(
                    "public funding settlement correction conflicts with an admitted identity"
                )
            self._funding_signatures.move_to_end(event_id)
            return None
        self._funding_signatures[event_id] = signature
        if len(self._funding_signatures) > self._funding_dedupe_capacity:
            self._funding_signatures.popitem(last=False)
        return PublicFundingSettlement(
            event_id=event_id,
            instrument=self._instruments[key],
            funding_time=funding_time,
            received_at=received_at,
            funding_rate=funding_rate_value,
            funding_interval_seconds=interval_value,
            rate_kind=rate_kind,
            mark_price=mark_price,
            oracle_price=oracle_price,
            source_observation_id=observation_id,
        )

    def _adapt_bbo(
        self,
        record: ParsedRecord,
        *,
        key: _SourceKey,
        received_at: datetime,
    ) -> Mapping[str, MarketEvent] | None:
        row = record.row
        bid_price = _positive_decimal(row.get("bid_price"))
        ask_price = _positive_decimal(row.get("ask_price"))
        bid_depth = _positive_decimal(row.get("bid_quantity"))
        ask_depth = _positive_decimal(row.get("ask_quantity"))
        if bid_price is None or ask_price is None or bid_depth is None or ask_depth is None:
            raise PublicRecordAdapterError(
                "supported BBO must be bilateral with finite positive Decimal prices and quantities"
            )
        if bid_price > ask_price:
            raise PublicRecordAdapterError("supported BBO must not be crossed")

        if self._status[key] in {_StreamStatus.AWAITING_BOOK, _StreamStatus.STALE}:
            self._status[key] = _StreamStatus.READY

        source_lineage = self._market_source_lineage(record)
        lineage_admitted = source_lineage == self._admitted_websocket_lineage[key]
        book = _BookState(
            instrument=self._instruments[key],
            bid_price=bid_price,
            ask_price=ask_price,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
        )
        self._books[key] = book
        blocked = (
            self._status[key] is not _StreamStatus.READY or source_lineage[0] is None or not lineage_admitted
        )
        event = self._market_event(
            record,
            key=key,
            received_at=received_at,
            book=book,
            stale=self._status[key] is _StreamStatus.STALE,
            gap=self._status[key] in {_StreamStatus.GAPPED, _StreamStatus.AWAITING_BOOK},
            tradable=not blocked,
        )
        return MappingProxyType({event.instrument: event})

    def _adapt_connection(
        self,
        record: ParsedRecord,
        *,
        venue: str,
        asset: str,
        received_at: datetime,
    ) -> Mapping[str, MarketEvent] | None:
        event_kind = _required_text(record.row.get("event_kind"), label="event_kind")
        if event_kind not in _GAP_EVENTS | _AWAITING_BOOK_EVENTS:
            raise PublicRecordAdapterError(f"unsupported connection event_kind: {event_kind}")

        if asset == "GLOBAL":
            affected = sorted(
                (key for key in self._instruments if key[0] == venue),
                key=lambda key: self._instruments[key],
            )
        else:
            key = (venue, asset)
            affected = [key] if key in self._instruments else []
        if not affected:
            return None

        connection_epoch = record.row.get("connection_epoch")
        initial_bootstrap_connect = (
            event_kind == "connect"
            and isinstance(connection_epoch, int)
            and not isinstance(connection_epoch, bool)
            and connection_epoch > 0
            and all(self._status[key] is _StreamStatus.READY and key in self._books for key in affected)
        )
        if not initial_bootstrap_connect:
            target_status = _StreamStatus.GAPPED if event_kind in _GAP_EVENTS else _StreamStatus.AWAITING_BOOK
            for key in affected:
                self._status[key] = target_status
        connection_lineage = self._market_source_lineage(record)
        if (
            event_kind == "connect"
            and connection_lineage[0] is not None
            and connection_lineage[1] is not None
        ):
            admitted_lineage = (connection_lineage[0], connection_lineage[1])
        else:
            admitted_lineage = None
        for key in affected:
            self._admitted_websocket_lineage[key] = admitted_lineage
        events: dict[str, MarketEvent] = {}
        for index, key in enumerate(affected):
            capture_ordinal = index + 1 if len(affected) > 1 else 0
            book = self._books.get(key)
            if book is None:
                continue
            event = self._market_event(
                record,
                key=key,
                received_at=received_at,
                book=book,
                stale=False,
                gap=not initial_bootstrap_connect,
                tradable=False,
                capture_ordinal=capture_ordinal,
            )
            events[event.instrument] = event
        return MappingProxyType(events) if events else None

    @staticmethod
    def _market_source_lineage(record: ParsedRecord) -> tuple[str | None, int | None]:
        row = record.row
        connection_id = row.get("connection_id")
        if record.record_type is not RecordType.BBO:
            connection_epoch = row.get("connection_epoch")
            if (
                isinstance(connection_id, str)
                and isinstance(connection_epoch, int)
                and not isinstance(connection_epoch, bool)
                and connection_id
                and 1 <= connection_epoch <= _UINT64_MAX
            ):
                return connection_id, connection_epoch
            return None, None
        if not isinstance(connection_id, str):
            return None, None
        update_id = row.get("update_id")
        if not isinstance(update_id, str) or update_id.startswith("rest:"):
            return None, None
        parts = update_id.rsplit(":", 2)
        if len(parts) != 3 or not parts[0].endswith(f":{connection_id}"):
            return None, None
        epoch_text, arrival_text = parts[1:]
        if not epoch_text.isdecimal() or not arrival_text.isdecimal():
            return None, None
        epoch = int(epoch_text)
        arrival_sequence = int(arrival_text)
        if not 1 <= epoch <= _UINT64_MAX or not 1 <= arrival_sequence <= _UINT64_MAX:
            return None, None
        return connection_id, epoch

    def _market_event(
        self,
        record: ParsedRecord,
        *,
        key: _SourceKey,
        received_at: datetime,
        book: _BookState,
        stale: bool,
        gap: bool,
        tradable: bool,
        capture_ordinal: int = 0,
    ) -> MarketEvent:
        source_venue, source_asset = self._record_identity(record)
        source_event_kind = (
            _required_text(record.row.get("event_kind"), label="event_kind")
            if record.record_type is RecordType.CONNECTION_EVENT
            else record.record_type.value
        )
        source_connection_id, source_connection_epoch = self._market_source_lineage(record)
        return MarketEvent.create(
            received_at=received_at,
            instrument=book.instrument,
            bid_price=book.bid_price,
            ask_price=book.ask_price,
            bid_depth=book.bid_depth,
            ask_depth=book.ask_depth,
            source_sequence=self._source_sequence(
                record,
                venue=source_venue,
                asset=source_asset,
            ),
            source_event_kind=source_event_kind,
            source_connection_id=source_connection_id,
            source_connection_epoch=source_connection_epoch,
            capture_ordinal=capture_ordinal,
            stale=stale,
            gap=gap,
            tradable=tradable,
            context=(self._market_contexts.get(key, {}) if self._include_market_context else {}),
        )


class BoundedPublicRecordSource:
    """Bounded source with latest-value semantics for still-pending BBOs.

    Saturation and malformed supported input are terminal. Continuing after
    either condition would make the paper stream silently incomplete. Only
    BBOs for the same instrument and UTC minute may replace an older BBO that
    has not yet been polled. Funding and connection events are causal barriers,
    and minute boundaries remain FIFO so the frozen strategy retains every
    completed close it can safely evaluate.
    """

    def __init__(
        self,
        *,
        descriptor: PublicSourceDescriptor,
        adapter: PublicRecordMarketEventAdapter,
        capacity: int,
    ) -> None:
        if not isinstance(descriptor, PublicSourceDescriptor):
            raise TypeError("descriptor must be a PublicSourceDescriptor")
        if not isinstance(adapter, PublicRecordMarketEventAdapter):
            raise TypeError("adapter must be a PublicRecordMarketEventAdapter")
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        if capacity != adapter.queue_capacity:
            raise ValueError("capacity must equal the queue capacity frozen in the source identity")
        if descriptor.data_hash != adapter.identity_hash:
            raise ValueError("public source descriptor data_hash must equal the canonical source identity")
        self._descriptor = descriptor
        self._adapter = adapter
        self._capacity = capacity
        self._queue: deque[_QueuedPublicSourceItem] = deque()
        self._queue_condition = threading.Condition()
        self._high_water = 0
        self._adapted_items = 0
        self._enqueued_items = 0
        self._polled_items = 0
        self._coalesced_bbo_frames = 0
        self._latest_adapted_received_at: datetime | None = None
        self._feed_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._stopped = threading.Event()
        self._closed = False
        self._fatal: Exception | None = None

    @property
    def descriptor(self) -> PublicSourceDescriptor:
        return self._descriptor

    @property
    def identity_artifact_bytes(self) -> bytes:
        return self._adapter.identity_artifact_bytes

    @property
    def pending_count(self) -> int:
        with self._queue_condition:
            return len(self._queue)

    @property
    def high_water(self) -> int:
        with self._queue_condition:
            return self._high_water

    @staticmethod
    def _received_at(item: PublicSourceItem) -> datetime:
        if isinstance(item, PublicFundingSettlement):
            return item.received_at
        received = tuple(event.received_at for event in item.values())
        if not received:
            raise PublicRecordAdapterError("public source item must not be empty")
        return min(received)

    @staticmethod
    def _bbo_coalesce_key(
        record: ParsedRecord,
        item: PublicSourceItem,
    ) -> _BboCoalesceKey | None:
        if record.record_type is not RecordType.BBO:
            return None
        if not isinstance(item, Mapping) or len(item) != 1:
            raise PublicRecordAdapterError("one normalized BBO must produce exactly one instrument")
        event = next(iter(item.values()))
        minute = int(event.received_at.timestamp()) // 60
        return event.instrument, minute

    @staticmethod
    def _timestamp_text(value: datetime | None) -> str | None:
        return None if value is None else value.isoformat(timespec="microseconds")

    def queue_snapshot(self, *, as_of: datetime) -> dict[str, object]:
        observed_at = _utc(as_of, label="queue snapshot as_of")
        with self._queue_condition:
            oldest = self._queue[0].received_at if self._queue else None
            newest = self._queue[-1].received_at if self._queue else None
            latest = self._latest_adapted_received_at
            return {
                "capacity_frames": self._capacity,
                "pending_frames": len(self._queue),
                "high_water_frames": self._high_water,
                "adapted_items": self._adapted_items,
                "enqueued_items": self._enqueued_items,
                "polled_items": self._polled_items,
                "coalesced_bbo_frames": self._coalesced_bbo_frames,
                "oldest_pending_received_at": self._timestamp_text(oldest),
                "newest_pending_received_at": self._timestamp_text(newest),
                "oldest_pending_age_seconds": (
                    None if oldest is None else max((observed_at - oldest).total_seconds(), 0.0)
                ),
                "latest_adapted_received_at": self._timestamp_text(latest),
                "latest_adapted_age_seconds": (
                    None if latest is None else max((observed_at - latest).total_seconds(), 0.0)
                ),
                "pending_bbo_coalescing": _PENDING_BBO_COALESCING,
            }

    def feed(self, record: ParsedRecord) -> bool:
        """Feed one record; return whether it produced an enqueued paper frame."""

        with self._feed_lock:
            self._raise_if_unavailable()
            try:
                frame = self._adapter.adapt(record)
            except PublicRecordAdapterError as exc:
                self._latch(exc)
                raise
            if frame is None:
                return False
            received_at = self._received_at(frame)
            coalesce_key = self._bbo_coalesce_key(record, frame)
            with self._queue_condition:
                self._adapted_items += 1
                self._latest_adapted_received_at = received_at
                if coalesce_key is not None:
                    for index in range(len(self._queue) - 1, -1, -1):
                        queued = self._queue[index]
                        if queued.bbo_coalesce_key is None:
                            break
                        if queued.bbo_coalesce_key == coalesce_key:
                            del self._queue[index]
                            self._coalesced_bbo_frames += 1
                            break
                if len(self._queue) >= self._capacity:
                    full = True
                else:
                    self._queue.append(
                        _QueuedPublicSourceItem(
                            item=frame,
                            received_at=received_at,
                            bbo_coalesce_key=coalesce_key,
                        )
                    )
                    self._enqueued_items += 1
                    self._high_water = max(self._high_water, len(self._queue))
                    self._queue_condition.notify()
                    full = False
            if full:
                error = PublicRecordQueueFull(
                    "public paper source queue saturated; normalized coverage is incomplete"
                )
                self._latch(error)
                raise error
            return True

    def feed_many(self, records: Iterable[ParsedRecord]) -> int:
        enqueued = 0
        for record in records:
            enqueued += int(self.feed(record))
        return enqueued

    def poll(self, *, timeout_seconds: float) -> PublicSourceItem | None:
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(float(timeout_seconds))
            or timeout_seconds < 0
        ):
            raise ValueError("timeout_seconds must be finite and non-negative")
        self._raise_if_fatal()
        if self._stopped.is_set():
            return None
        deadline = time.monotonic() + float(timeout_seconds)
        queued: _QueuedPublicSourceItem | None = None
        while queued is None:
            self._raise_if_fatal()
            if self._stopped.is_set():
                return None
            with self._queue_condition:
                if self._queue:
                    queued = self._queue.popleft()
                    self._polled_items += 1
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._queue_condition.wait(timeout=remaining)
        self._raise_if_fatal()
        return queued.item

    def fail(self, error: Exception) -> None:
        self._latch(error)

    def stop(self) -> None:
        self._stopped.set()
        with self._queue_condition:
            self._queue_condition.notify_all()

    def close(self) -> None:
        with self._state_lock:
            self._closed = True
            self._stopped.set()
        with self._queue_condition:
            self._queue_condition.notify_all()

    def _latch(self, error: Exception) -> None:
        with self._state_lock:
            if self._fatal is None:
                self._fatal = error
            self._stopped.set()
        with self._queue_condition:
            self._queue_condition.notify_all()

    def _raise_if_fatal(self) -> None:
        with self._state_lock:
            error = self._fatal
        if error is not None:
            raise error

    def _raise_if_unavailable(self) -> None:
        self._raise_if_fatal()
        with self._state_lock:
            unavailable = self._closed or self._stopped.is_set()
        if unavailable:
            raise PublicRecordSourceClosed("public paper source is stopped or closed")
