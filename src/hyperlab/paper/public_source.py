from __future__ import annotations

import hashlib
import json
import math
import queue
import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType

from hyperlab.collector.models import ParsedRecord
from hyperlab.data.schema import RecordType, latest_schema_for, parse_instrument
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


_SourceKey = tuple[str, str]
_SUPPORTED_RECORD_TYPES = frozenset({RecordType.BBO, RecordType.CONNECTION_EVENT})
_GAP_EVENTS = frozenset({"disconnect", "gap", "resync_start"})
_AWAITING_BOOK_EVENTS = frozenset({"connect", "resync_complete"})
_ADAPTER_SCHEMA_VERSION = 2
_FEED_CONTRACT = "SOLE_COLLECTOR_NORMALIZED_BBO_CONNECTION_BOUNDED_FIFO_V2"
_GLOBAL_CONNECTION_POLICY = "MULTI_INSTRUMENT_GLOBAL_EVENT_TERMINAL_V1"
_INSTRUMENT_ROUTE_POLICY = "EXPLICIT_MAPPING_REQUIRES_SEPARATE_METADATA_REVIEW_V1"
_SOURCE_VENUE_TO_PAPER_EXCHANGE: Mapping[str, str] = MappingProxyType(
    {"hyperliquid": "HL"}
)
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
        raise PublicRecordAdapterError(
            f"{label} must be a finite Decimal when present"
        )


def _optional_uint64(value: object, *, label: str) -> None:
    if value is None:
        return
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        or value > _UINT64_MAX
    ):
        raise PublicRecordAdapterError(
            f"{label} must be an unsigned 64-bit integer when present"
        )


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
    ) -> None:
        if not instruments:
            raise ValueError("at least one explicit public instrument mapping is required")
        if (
            isinstance(queue_capacity, bool)
            or not isinstance(queue_capacity, int)
            or queue_capacity < 1
        ):
            raise ValueError("queue_capacity must be a positive integer")

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
            exchange, instrument_asset, kind = parse_instrument(instrument)
            if not exchange or not instrument_asset:
                raise ValueError("paper instrument exchange and asset must not be empty")
            if instrument != f"{expected_exchange}:{asset}:{kind}":
                raise ValueError(
                    "public source venue/asset must match the canonical HL paper instrument"
                )
            if instrument in seen_instruments:
                raise ValueError("each paper instrument must have exactly one public source")
            normalized[(venue, asset)] = instrument
            seen_instruments.add(instrument)

        self._instruments = MappingProxyType(normalized)
        self._queue_capacity = queue_capacity
        self._books: dict[_SourceKey, _BookState] = {}
        self._status = {key: _StreamStatus.READY for key in normalized}
        self._last_received_at: datetime | None = None
        identity = {
            "adapter_schema_version": _ADAPTER_SCHEMA_VERSION,
            "feed_contract": _FEED_CONTRACT,
            "global_connection_policy": _GLOBAL_CONNECTION_POLICY,
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
                for source_venue, paper_exchange in sorted(
                    _SOURCE_VENUE_TO_PAPER_EXCHANGE.items()
                )
            ],
            "trade_projection": "BLOCKED_RESTART_DURABLE_IDENTITY_UNAVAILABLE",
        }
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

    def adapt(self, record: ParsedRecord) -> Mapping[str, MarketEvent] | None:
        """Adapt one already-normalized record, preserving input arrival order."""

        if not isinstance(record, ParsedRecord):
            raise TypeError("record must be a ParsedRecord")
        if not isinstance(record.record_type, RecordType):
            raise PublicRecordAdapterError("ParsedRecord.record_type must be a RecordType")
        if record.record_type is RecordType.TRADE:
            raise PublicRecordAdapterError(
                "trade projection is disabled until restart-durable venue trade "
                "identity is persisted and restored"
            )
        if record.record_type not in _SUPPORTED_RECORD_TYPES:
            return None
        if not isinstance(record.row, Mapping):
            raise PublicRecordAdapterError("ParsedRecord.row must be a mapping")
        self._validate_normalized_row(record)

        venue, asset = self._record_identity(record)
        received_at = _utc(record.row.get("received_time"), label="received_time")
        self._observe_order(received_at)

        if record.record_type is RecordType.CONNECTION_EVENT:
            return self._adapt_connection(record, venue=venue, asset=asset, received_at=received_at)

        key = (venue, asset)
        if key not in self._instruments:
            return None
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
            raise PublicRecordAdapterError(
                "row record_type does not match ParsedRecord.record_type"
            )
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
            row.get("connection_epoch") is None
            and row.get("capture_epoch_id") is None
        ):
            raise PublicRecordAdapterError(
                "connection event lacks stable connection epoch lineage"
            )

    def _record_identity(self, record: ParsedRecord) -> _SourceKey:
        venue = _required_text(record.row.get("venue"), label="venue")
        row_asset = _required_text(record.row.get("asset"), label="asset")
        record_asset = _required_text(record.asset, label="ParsedRecord.asset")
        if row_asset != record_asset:
            raise PublicRecordAdapterError("ParsedRecord.asset does not match row asset")
        return venue, row_asset

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
                and (
                    row.get("connection_epoch") is not None
                    or row.get("capture_epoch_id") is not None
                )
            )
        if not has_identity:
            raise PublicRecordAdapterError(
                f"{record.record_type.value} lacks stable public lineage"
            )

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
        if (
            bid_price is None
            or ask_price is None
            or bid_depth is None
            or ask_depth is None
            or bid_price > ask_price
        ):
            if self._status[key] is not _StreamStatus.GAPPED:
                self._status[key] = _StreamStatus.STALE
            return None

        if self._status[key] in {_StreamStatus.AWAITING_BOOK, _StreamStatus.STALE}:
            self._status[key] = _StreamStatus.READY

        book = _BookState(
            instrument=self._instruments[key],
            bid_price=bid_price,
            ask_price=ask_price,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
        )
        self._books[key] = book
        blocked = self._status[key] is not _StreamStatus.READY
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

        target_status = (
            _StreamStatus.GAPPED
            if event_kind in _GAP_EVENTS
            else _StreamStatus.AWAITING_BOOK
        )
        for key in affected:
            self._status[key] = target_status
        if asset == "GLOBAL" and len(affected) > 1:
            raise PublicRecordAdapterError(
                "global connection event spans multiple instruments and cannot be "
                "crash-atomically represented by the current paper runtime"
            )

        for key in affected:
            book = self._books.get(key)
            if book is None:
                continue
            event = self._market_event(
                record,
                key=key,
                received_at=received_at,
                book=book,
                stale=False,
                gap=True,
                tradable=False,
            )
            return MappingProxyType({event.instrument: event})
        return None

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
    ) -> MarketEvent:
        source_venue, source_asset = self._record_identity(record)
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
            stale=stale,
            gap=gap,
            tradable=tradable,
        )


class BoundedPublicRecordSource:
    """Bounded FIFO source a sole public collector can feed in-process.

    Saturation and malformed supported input are terminal. Continuing after
    either condition would make the paper stream silently incomplete.
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
            raise ValueError(
                "capacity must equal the queue capacity frozen in the source identity"
            )
        if descriptor.data_hash != adapter.identity_hash:
            raise ValueError(
                "public source descriptor data_hash must equal the canonical source identity"
            )
        self._descriptor = descriptor
        self._adapter = adapter
        self._queue: queue.Queue[Mapping[str, MarketEvent]] = queue.Queue(maxsize=capacity)
        self._feed_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._stopped = threading.Event()
        self._closed = False
        self._fatal: Exception | None = None

    @property
    def descriptor(self) -> PublicSourceDescriptor:
        return self._descriptor

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
            try:
                self._queue.put_nowait(frame)
            except queue.Full as exc:
                error = PublicRecordQueueFull(
                    "public paper source queue saturated; normalized coverage is incomplete"
                )
                self._latch(error)
                raise error from exc
            return True

    def feed_many(self, records: Iterable[ParsedRecord]) -> int:
        enqueued = 0
        for record in records:
            enqueued += int(self.feed(record))
        return enqueued

    def poll(self, *, timeout_seconds: float) -> Mapping[str, MarketEvent] | None:
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
        try:
            frame = self._queue.get(timeout=float(timeout_seconds))
        except queue.Empty:
            self._raise_if_fatal()
            return None
        self._raise_if_fatal()
        return frame

    def stop(self) -> None:
        self._stopped.set()

    def close(self) -> None:
        with self._state_lock:
            self._closed = True
            self._stopped.set()

    def _latch(self, error: Exception) -> None:
        with self._state_lock:
            if self._fatal is None:
                self._fatal = error
            self._stopped.set()

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
