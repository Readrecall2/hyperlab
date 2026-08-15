from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from itertools import pairwise
from threading import Lock, local
from typing import Any, Protocol

import requests

from hyperlab.collector.models import ParsedMessage, ParsedRecord, WireEnvelope
from hyperlab.data.schema import RecordType, instrument, latest_schema_for
from hyperlab.venues.base import (
    ClockMeasurement,
    HttpRequestDiagnostics,
    NormalizedInstrument,
    measure_clock,
)
from hyperlab.venues.http_observability import (
    HttpPathObservation,
    HttpPeerPathObserver,
    Resolver,
    diagnose_http_paths,
)

VENUE = "binance_usdm"
REST_BASE_URL = "https://fapi.binance.com"
WS_PUBLIC_BASE_URL = "wss://fstream.binance.com/public/stream?streams="
WS_MARKET_BASE_URL = "wss://fstream.binance.com/market/stream?streams="

# This is an executable safety boundary, not merely documentation. Paths for
# accounts, keys, orders, positions, listen keys, transfers, or signing cannot
# pass through BinancePublicRestClient.
PUBLIC_GET_PATHS = frozenset(
    {
        "/fapi/v1/time",
        "/fapi/v1/exchangeInfo",
        "/fapi/v1/fundingRate",
        "/fapi/v1/fundingInfo",
        "/fapi/v1/klines",
        "/fapi/v1/ticker/bookTicker",
        "/fapi/v1/aggTrades",
    }
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _datetime_ms(value: object) -> datetime:
    try:
        milliseconds = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid Binance millisecond timestamp: {value!r}") from exc
    if milliseconds < 0:
        raise ValueError("Binance millisecond timestamp cannot be negative")
    return datetime.fromtimestamp(milliseconds / 1_000, tz=UTC)


def _decimal(value: object, *, required: bool = True) -> Decimal | None:
    if value is None or value == "":
        if required:
            raise ValueError("required Binance numeric value is missing")
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid Binance numeric value: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"non-finite Binance numeric value: {value!r}")
    return result


def _required_decimal(value: object) -> Decimal:
    result = _decimal(value)
    assert result is not None
    return result


def _mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"Binance {label} must be an object")
    return value


def _sequence(value: object, *, label: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"Binance {label} must be an array")
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _common(
    record_type: RecordType,
    asset: str,
    envelope: WireEnvelope,
    *,
    event_time: datetime,
    exchange_time: datetime | None,
    source_sequence: int | None,
) -> dict[str, object]:
    return {
        "schema_version": latest_schema_for(record_type).version,
        "record_type": record_type.value,
        "venue": VENUE,
        "asset": asset,
        "event_time": event_time,
        "exchange_time": exchange_time,
        "received_time": envelope.received_time,
        "source_sequence": source_sequence,
        "connection_id": envelope.connection_id,
    }


def _observation_id(envelope: WireEnvelope) -> str:
    return f"{envelope.connection_id}:{envelope.connection_epoch}:{envelope.arrival_sequence}"


def _filter(symbol: Mapping[str, Any], filter_type: str, field: str) -> Decimal:
    filters = _sequence(symbol.get("filters"), label="symbol filters")
    for raw_filter in filters:
        item = _mapping(raw_filter, label="symbol filter")
        if item.get("filterType") == filter_type:
            return _required_decimal(item.get(field))
    raise ValueError(f"Binance symbol is missing {filter_type}.{field}")


def normalize_exchange_info(
    payload: object,
    requested_assets: tuple[str, ...],
) -> dict[str, NormalizedInstrument]:
    """Admit only reviewed linear USDT perpetual identities and base-unit sizes."""

    root = _mapping(payload, label="exchange information")
    symbols = _sequence(root.get("symbols"), label="exchange symbols")
    by_asset: dict[str, NormalizedInstrument] = {}
    requested = {asset.upper() for asset in requested_assets}
    if not requested or len(requested) != len(requested_assets):
        raise ValueError("requested assets must be a non-empty unique list")
    for raw_symbol in symbols:
        symbol = _mapping(raw_symbol, label="exchange symbol")
        base_asset = str(symbol.get("baseAsset", "")).upper()
        if base_asset not in requested:
            continue
        source_symbol = str(symbol.get("symbol", "")).upper()
        if (
            source_symbol != f"{base_asset}USDT"
            or str(symbol.get("quoteAsset", "")).upper() != "USDT"
            or str(symbol.get("marginAsset", "")).upper() != "USDT"
            or str(symbol.get("contractType", "")) != "PERPETUAL"
        ):
            continue
        if base_asset in by_asset:
            raise ValueError(f"multiple Binance contracts map to {base_asset}")
        by_asset[base_asset] = NormalizedInstrument(
            venue=VENUE,
            source_symbol=source_symbol,
            asset=base_asset,
            base_asset=base_asset,
            quote_asset="USDT",
            instrument_kind="perp",
            contract_kind="linear",
            # Binance USD-M quantity is expressed in the base asset. Keeping the
            # multiplier explicit prevents silently reusing this rule for COIN-M.
            quantity_multiplier=Decimal("1"),
            price_tick=_filter(symbol, "PRICE_FILTER", "tickSize"),
            quantity_step=_filter(symbol, "LOT_SIZE", "stepSize"),
            status=str(symbol.get("status", "UNKNOWN")),
        )
    missing = sorted(requested - set(by_asset))
    if missing:
        raise ValueError(f"no reviewed Binance USD-M perpetual identity for: {', '.join(missing)}")
    return by_asset


class BinancePublicConnector:
    venue = VENUE

    def __init__(self, instruments: Mapping[str, NormalizedInstrument]) -> None:
        if not instruments:
            raise ValueError("Binance connector requires normalized instruments")
        self._by_asset = {asset.upper(): item for asset, item in instruments.items()}
        self._by_symbol = {item.source_symbol: item for item in self._by_asset.values()}

    @classmethod
    def from_exchange_info(
        cls,
        payload: object,
        requested_assets: tuple[str, ...],
    ) -> BinancePublicConnector:
        return cls(normalize_exchange_info(payload, requested_assets))

    def instrument_for_asset(self, asset: str) -> NormalizedInstrument:
        try:
            return self._by_asset[asset.upper()]
        except KeyError:
            raise ValueError(f"asset is not configured for Binance: {asset}") from None

    def websocket_urls(
        self,
        assets: tuple[str, ...],
        candle_intervals: tuple[str, ...],
    ) -> dict[str, str]:
        public_streams: list[str] = []
        market_streams: list[str] = []
        for asset in assets:
            symbol = self.instrument_for_asset(asset).source_symbol.lower()
            public_streams.append(f"{symbol}@depth20@100ms")
            market_streams.extend(
                (
                    f"{symbol}@aggTrade",
                    f"{symbol}@markPrice@1s",
                )
            )
            market_streams.extend(
                f"{symbol}@kline_{interval}" for interval in candle_intervals
            )
        return {
            "public": WS_PUBLIC_BASE_URL + "/".join(public_streams),
            "market": WS_MARKET_BASE_URL + "/".join(market_streams),
        }

    def parse_message(self, envelope: WireEnvelope) -> ParsedMessage:
        return parse_binance_message(envelope, self._by_symbol)

    def metadata_records(self, received_time: datetime) -> tuple[ParsedRecord, ...]:
        records: list[ParsedRecord] = []
        for asset, item in sorted(self._by_asset.items()):
            metadata = {
                "contract_kind": item.contract_kind,
                "quantity_multiplier": str(item.quantity_multiplier),
                "price_tick": str(item.price_tick),
                "quantity_step": str(item.quantity_step),
                "status": item.status,
                "source_quantity_unit": item.base_asset,
            }
            metadata_json = _canonical_json(metadata)
            row = {
                "schema_version": latest_schema_for(RecordType.INSTRUMENT_METADATA).version,
                "record_type": RecordType.INSTRUMENT_METADATA.value,
                "venue": VENUE,
                "asset": asset,
                "event_time": received_time,
                "exchange_time": None,
                "received_time": received_time,
                "source_sequence": None,
                "connection_id": None,
                "instrument_kind": "perp",
                "instrument_id": instrument(VENUE, asset, "perp"),
                "source_symbol": item.source_symbol,
                "source_index": None,
                "base_token": item.base_asset,
                "quote_token": item.quote_asset,
                "sz_decimals": None,
                "wei_decimals": None,
                "max_leverage": None,
                "margin_table_id": None,
                "is_canonical": True,
                "full_name": f"Binance USD-M {item.source_symbol} perpetual",
                "metadata_sha256": hashlib.sha256(metadata_json.encode()).hexdigest(),
                "metadata_json": metadata_json,
            }
            records.append(ParsedRecord(RecordType.INSTRUMENT_METADATA, asset, row))
        return tuple(records)


def _wire_record(
    envelope: WireEnvelope,
    *,
    channel: str | None,
    asset: str | None,
    is_json: bool,
) -> ParsedRecord:
    row = _common(
        RecordType.WIRE_MESSAGE,
        "GLOBAL",
        envelope,
        event_time=envelope.received_time,
        exchange_time=None,
        source_sequence=None,
    )
    row.update(
        {
            "connection_epoch": envelope.connection_epoch,
            "arrival_sequence": envelope.arrival_sequence,
            "capture_epoch_id": envelope.capture_epoch_id,
            "channel": channel,
            "message_asset": asset,
            "raw_message": envelope.raw_message,
            "is_json": is_json,
            "payload_sha256": hashlib.sha256(envelope.raw_message.encode()).hexdigest(),
        }
    )
    return ParsedRecord(RecordType.WIRE_MESSAGE, "GLOBAL", row)


def parse_binance_message(
    envelope: WireEnvelope,
    instruments_by_symbol: Mapping[str, NormalizedInstrument],
) -> ParsedMessage:
    """Parse one combined public stream frame without hidden state or I/O."""

    try:
        root = json.loads(envelope.raw_message)
    except json.JSONDecodeError:
        wire = _wire_record(envelope, channel=None, asset=None, is_json=False)
        return ParsedMessage(None, (wire,), ("invalid_json",))
    if not isinstance(root, Mapping):
        wire = _wire_record(envelope, channel=None, asset=None, is_json=True)
        return ParsedMessage(None, (wire,), ("json_root_not_object",))
    stream = root.get("stream")
    channel = str(stream) if isinstance(stream, str) and stream else None
    raw_data = root.get("data")
    data = raw_data if isinstance(raw_data, Mapping) else None
    source_symbol = "" if data is None else str(data.get("s", "")).upper()
    normalized = instruments_by_symbol.get(source_symbol)
    asset = None if normalized is None else normalized.asset
    records: list[ParsedRecord] = [
        _wire_record(envelope, channel=channel, asset=asset, is_json=True)
    ]
    issues: list[str] = []
    stream_types: list[tuple[str, object]] = []
    if "st" in root:
        stream_types.append(("wrapper", root["st"]))
    if data is not None and "st" in data:
        stream_types.append(("data", data["st"]))
    if any(
        not (
            (isinstance(value, int) and not isinstance(value, bool) and value == 1)
            or (isinstance(value, str) and value == "1")
        )
        for _, value in stream_types
    ):
        observed = ",".join(
            f"{location}={value!r}" for location, value in stream_types
        )
        issues.append(f"invalid_stream_type:expected=1:{observed}")
        return ParsedMessage(channel, tuple(records), tuple(issues))
    if channel is None or data is None:
        issues.append("invalid_combined_stream_envelope")
        return ParsedMessage(channel, tuple(records), tuple(issues))
    if normalized is None:
        issues.append(f"unknown_symbol:{source_symbol or 'missing'}")
        return ParsedMessage(channel, tuple(records), tuple(issues))
    channel_symbol, separator, _stream_kind = channel.partition("@")
    if not separator or channel_symbol.upper() != source_symbol:
        issues.append(
            "stream_symbol_mismatch:"
            f"channel={channel_symbol or 'missing'}:payload={source_symbol or 'missing'}"
        )
        return ParsedMessage(channel, tuple(records), tuple(issues))
    try:
        event = str(data.get("e", ""))
        if channel.endswith("@bookTicker"):
            if event != "bookTicker":
                raise ValueError(
                    f"event/channel mismatch: expected bookTicker, observed {event or 'missing'}"
                )
            records.append(_parse_bbo(data, envelope, normalized))
        elif "@depth20" in channel:
            if event != "depthUpdate":
                raise ValueError(
                    f"event/channel mismatch: expected depthUpdate, observed {event or 'missing'}"
                )
            records.append(_parse_bbo_from_depth(data, envelope, normalized))
            records.extend(_parse_l2_snapshot(data, envelope, normalized))
        elif channel.endswith("@aggTrade"):
            if event != "aggTrade":
                raise ValueError(
                    f"event/channel mismatch: expected aggTrade, observed {event or 'missing'}"
                )
            records.append(_parse_trade(data, envelope, normalized))
        elif "@kline_" in channel:
            if event != "kline":
                raise ValueError(
                    f"event/channel mismatch: expected kline, observed {event or 'missing'}"
                )
            records.append(_parse_candle(data, envelope, normalized))
        elif "@markPrice" in channel:
            if event != "markPriceUpdate":
                raise ValueError(
                    "event/channel mismatch: "
                    f"expected markPriceUpdate, observed {event or 'missing'}"
                )
            records.append(_parse_mark_price(data, envelope, normalized))
        else:
            issues.append(f"unknown_channel:{channel}")
    except (KeyError, TypeError, ValueError) as exc:
        issues.append(f"invalid_{channel}:{type(exc).__name__}:{exc}")
    return ParsedMessage(channel, tuple(records), tuple(issues))


def _parse_bbo(
    data: Mapping[str, Any],
    envelope: WireEnvelope,
    normalized: NormalizedInstrument,
) -> ParsedRecord:
    transaction_time = _datetime_ms(data.get("T", data["E"]))
    exchange_time = _datetime_ms(data["E"])
    update_id = int(str(data["u"]))
    row = _common(
        RecordType.BBO,
        normalized.asset,
        envelope,
        event_time=transaction_time,
        exchange_time=exchange_time,
        source_sequence=update_id,
    )
    row.update(
        {
            "update_id": f"{normalized.source_symbol}:{update_id}",
            "bid_price": _required_decimal(data["b"]),
            "bid_quantity": normalized.normalize_quantity(data["B"]),
            "ask_price": _required_decimal(data["a"]),
            "ask_quantity": normalized.normalize_quantity(data["A"]),
        }
    )
    return ParsedRecord(RecordType.BBO, normalized.asset, row)


def _parse_bbo_from_depth(
    data: Mapping[str, Any],
    envelope: WireEnvelope,
    normalized: NormalizedInstrument,
) -> ParsedRecord:
    """Derive exact BBO from the first levels of one complete top-20 frame."""

    bids = _sequence(data["b"], label="partial depth bids")
    asks = _sequence(data["a"], label="partial depth asks")
    if not bids or not asks:
        raise ValueError("Binance partial depth BBO requires non-empty bid and ask sides")
    bid = _sequence(bids[0], label="partial depth best bid")
    ask = _sequence(asks[0], label="partial depth best ask")
    if len(bid) != 2 or len(ask) != 2:
        raise ValueError("Binance partial depth BBO levels must contain price and quantity")
    transaction_time = _datetime_ms(data.get("T", data["E"]))
    exchange_time = _datetime_ms(data["E"])
    update_id = int(str(data["u"]))
    if update_id < 0:
        raise ValueError("Binance L2 update sequence cannot be negative")
    row = _common(
        RecordType.BBO,
        normalized.asset,
        envelope,
        event_time=transaction_time,
        exchange_time=exchange_time,
        source_sequence=update_id,
    )
    row.update(
        {
            "update_id": f"{normalized.source_symbol}:{update_id}",
            "bid_price": _required_decimal(bid[0]),
            "bid_quantity": normalized.normalize_quantity(bid[1]),
            "ask_price": _required_decimal(ask[0]),
            "ask_quantity": normalized.normalize_quantity(ask[1]),
        }
    )
    return ParsedRecord(RecordType.BBO, normalized.asset, row)


def _parse_l2_snapshot(
    data: Mapping[str, Any],
    envelope: WireEnvelope,
    normalized: NormalizedInstrument,
) -> list[ParsedRecord]:
    """Normalize one complete top-20 public book frame without delta inference."""

    transaction_time = _datetime_ms(data.get("T", data["E"]))
    exchange_time = _datetime_ms(data["E"])
    last_sequence = int(str(data["u"]))
    if last_sequence < 0:
        raise ValueError("Binance L2 update sequence cannot be negative")
    bids = _sequence(data["b"], label="partial depth bids")
    asks = _sequence(data["a"], label="partial depth asks")
    if len(bids) > 20 or len(asks) > 20:
        raise ValueError("Binance partial depth frame exceeds its configured top-20 contract")

    snapshot_id = (
        f"ws:{envelope.connection_id}:{envelope.connection_epoch}:"
        f"{envelope.arrival_sequence}:{normalized.source_symbol}:{last_sequence}"
    )
    book_epoch_id = f"{envelope.connection_id}:{envelope.connection_epoch}"
    header = _common(
        RecordType.L2_BOOK_STATE,
        normalized.asset,
        envelope,
        event_time=transaction_time,
        exchange_time=exchange_time,
        source_sequence=None,
    )
    header.update(
        {
            "snapshot_id": snapshot_id,
            "book_epoch_id": book_epoch_id,
            "bid_level_count": len(bids),
            "ask_level_count": len(asks),
        }
    )
    records = [ParsedRecord(RecordType.L2_BOOK_STATE, normalized.asset, header)]
    for side, raw_levels in (("bid", bids), ("ask", asks)):
        for level_number, raw_level in enumerate(raw_levels):
            level = _sequence(raw_level, label=f"partial depth {side} level")
            if len(level) != 2:
                raise ValueError("Binance partial depth level must contain price and quantity")
            row = _common(
                RecordType.L2_SNAPSHOT,
                normalized.asset,
                envelope,
                event_time=transaction_time,
                exchange_time=exchange_time,
                source_sequence=None,
            )
            row.update(
                {
                    "snapshot_id": snapshot_id,
                    "book_epoch_id": book_epoch_id,
                    "last_sequence": last_sequence,
                    "side": side,
                    "level": level_number,
                    "price": _required_decimal(level[0]),
                    "quantity": normalized.normalize_quantity(level[1]),
                    "order_count": None,
                }
            )
            records.append(ParsedRecord(RecordType.L2_SNAPSHOT, normalized.asset, row))
    return records


def _parse_trade(
    data: Mapping[str, Any],
    envelope: WireEnvelope,
    normalized: NormalizedInstrument,
) -> ParsedRecord:
    trade_time = _datetime_ms(data["T"])
    exchange_time = _datetime_ms(data["E"])
    aggregate_id = int(str(data["a"]))
    price = _required_decimal(data["p"])
    quantity = normalized.normalize_quantity(data["q"])
    buyer_is_maker = data["m"]
    if aggregate_id < 0:
        raise ValueError("Binance aggregate trade ID cannot be negative")
    if price <= 0:
        raise ValueError("Binance aggregate trade price must be positive")
    if quantity <= 0:
        raise ValueError("Binance aggregate trade quantity must be positive")
    if not isinstance(buyer_is_maker, bool):
        raise ValueError("Binance aggregate trade maker flag must be boolean")
    row = _common(
        RecordType.TRADE,
        normalized.asset,
        envelope,
        event_time=trade_time,
        exchange_time=exchange_time,
        source_sequence=aggregate_id,
    )
    row.update(
        {
            "trade_id": f"{normalized.source_symbol}:agg:{aggregate_id}",
            "aggressor_side": "sell" if buyer_is_maker else "buy",
            "price": price,
            "quantity": quantity,
            "quote_quantity": price * quantity,
            "is_liquidation": None,
            "connection_epoch": envelope.connection_epoch,
            "arrival_sequence": envelope.arrival_sequence,
        }
    )
    return ParsedRecord(RecordType.TRADE, normalized.asset, row)


def _parse_candle(
    data: Mapping[str, Any],
    envelope: WireEnvelope,
    normalized: NormalizedInstrument,
) -> ParsedRecord:
    candle = _mapping(data["k"], label="kline")
    open_time = _datetime_ms(candle["t"])
    close_time = _datetime_ms(candle["T"])
    source_sequence = int(str(candle["L"])) if candle.get("L") is not None else None
    row = _common(
        RecordType.CANDLE,
        normalized.asset,
        envelope,
        event_time=open_time,
        exchange_time=_datetime_ms(data["E"]),
        source_sequence=source_sequence,
    )
    row.update(
        {
            "interval": str(candle["i"]),
            "open_time": open_time,
            "close_time": close_time,
            "open": _required_decimal(candle["o"]),
            "high": _required_decimal(candle["h"]),
            "low": _required_decimal(candle["l"]),
            "close": _required_decimal(candle["c"]),
            "base_volume": normalized.normalize_quantity(candle["v"]),
            "quote_volume": _decimal(candle.get("q"), required=False),
            "trade_count": int(str(candle["n"])),
            "is_final": bool(candle["x"]),
            "observation_id": _observation_id(envelope),
        }
    )
    return ParsedRecord(RecordType.CANDLE, normalized.asset, row)


def _parse_mark_price(
    data: Mapping[str, Any],
    envelope: WireEnvelope,
    normalized: NormalizedInstrument,
) -> ParsedRecord:
    event_time = _datetime_ms(data["E"])
    row = _common(
        RecordType.MARKET_CONTEXT,
        normalized.asset,
        envelope,
        event_time=event_time,
        exchange_time=event_time,
        source_sequence=None,
    )
    row.update(
        {
            "instrument_kind": "perp",
            "instrument_id": instrument(VENUE, normalized.asset, "perp"),
            "mark_price": _required_decimal(data["p"]),
            # Binance `i` is an index price. It is intentionally retained only
            # in the raw wire frame and never relabelled as an oracle price.
            "oracle_price": None,
            "mid_price": None,
            "current_funding_rate": _decimal(data.get("r"), required=False),
            "open_interest_quantity": None,
            "open_interest_notional": None,
            "base_volume_24h": None,
            "notional_volume_24h": None,
            "previous_day_price": None,
            "circulating_supply": None,
            "observation_id": _observation_id(envelope),
        }
    )
    return ParsedRecord(RecordType.MARKET_CONTEXT, normalized.asset, row)


class JsonGetTransport(Protocol):
    def get_json(self, url: str, params: Mapping[str, object], timeout_seconds: float) -> Any: ...


@dataclass(slots=True)
class _PendingHttpDiagnostics:
    url: str
    urllib3_counters_before: tuple[int, int] | None
    counters_before_completion_sequence: int
    requests_session_reused: bool
    diagnostic_prepare_ms: float
    counter_baseline_current: bool = False
    lock_requested_at: float | None = None
    lock_acquired_at: float | None = None
    get_started_at: float | None = None
    get_completed_at: float | None = None
    decode_started_at: float | None = None
    decode_completed_at: float | None = None
    response: Any | None = None
    outcome: str = "success"
    failure_stage: str | None = None
    exception_type: str | None = None
    completion_sequence: int | None = None
    path_observation: HttpPathObservation | None = None


@dataclass(frozen=True, slots=True)
class _TransportIdentityEvidence:
    urllib3_connection_identity: str | None
    tls_socket_identity: str | None
    tls_session_reused: bool | None


class BinancePublicHttpRequestError(RuntimeError):
    """Public HTTP failure carrying timings captured outside clock validity logic."""

    def __init__(
        self,
        original_exception: Exception,
        *,
        request_sent_time: datetime,
        response_received_time: datetime,
        http_diagnostics: HttpRequestDiagnostics,
    ) -> None:
        self.original_exception = original_exception
        self.request_sent_time = request_sent_time
        self.response_received_time = response_received_time
        self.http_diagnostics = http_diagnostics
        detail = str(original_exception).strip()
        reason = type(original_exception).__name__ if not detail else (
            f"{type(original_exception).__name__}: {detail}"
        )
        super().__init__(f"Binance public HTTP request failed: {reason}")


class _TransportDiagnosticsState(local):
    value: _PendingHttpDiagnostics | None

    def __init__(self) -> None:
        self.value = None


class RequestsJsonTransport:
    """Keyless GET transport with one reusable HTTPS connection pool."""

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        monotonic: Callable[[], float] = time.perf_counter,
    ) -> None:
        self._session = session or requests.Session()
        self._session.trust_env = False
        self._session.auth = None
        self._session.headers.clear()
        self._session.cookies.clear()
        self._session.params.clear()
        self._session.proxies.clear()
        self._session.cert = None
        self._session.verify = True
        self._lock = Lock()
        self._closed = False
        self._monotonic = monotonic
        self._diagnostics = _TransportDiagnosticsState()
        self._diagnostics_lock = Lock()
        self._session_requests_prepared = 0
        self._last_urllib3_connection_identity: str | None = None
        self._last_tls_socket_identity: str | None = None
        self._completed_request_sequence = 0
        self._peer_path_observer = HttpPeerPathObserver()

    def _pool_snapshot(self, url: str) -> tuple[int, int] | None:
        """Return urllib3 connection-object-created and request-started counters."""
        try:
            adapter = self._session.get_adapter(url)
            manager = getattr(adapter, "poolmanager", None)
            pools = getattr(manager, "pools", None)
            container = getattr(pools, "_container", None)
            lock = getattr(pools, "lock", None)
            if container is None or lock is None:
                return None
            with lock:
                pool_values = tuple(container.values())
            return (
                sum(int(pool.num_connections) for pool in pool_values),
                sum(int(pool.num_requests) for pool in pool_values),
            )
        except Exception:
            return None

    @staticmethod
    def _elapsed_ms(response: Any) -> float | None:
        try:
            elapsed = response.elapsed
            value = float(elapsed.total_seconds()) * 1_000
        except (AttributeError, TypeError, ValueError, OverflowError):
            return None
        return value if value >= 0 else None

    def prepare_diagnostics(self, url: str) -> None:
        """Capture optional pool state before the authoritative request timestamp."""

        prepare_started_at = self._monotonic()
        with self._lock:
            counters_before_completion_sequence = self._completed_request_sequence
            counters_before = self._pool_snapshot(url)
        with self._diagnostics_lock:
            session_reused = self._session_requests_prepared > 0
            self._session_requests_prepared += 1
        prepare_completed_at = self._monotonic()
        self._diagnostics.value = _PendingHttpDiagnostics(
            url=url,
            urllib3_counters_before=counters_before,
            counters_before_completion_sequence=counters_before_completion_sequence,
            requests_session_reused=session_reused,
            diagnostic_prepare_ms=max(prepare_completed_at - prepare_started_at, 0.0) * 1_000,
        )

    @staticmethod
    def _mark_failure(
        pending: _PendingHttpDiagnostics,
        *,
        stage: str,
        error: Exception,
    ) -> None:
        pending.outcome = "failure"
        pending.failure_stage = stage
        pending.exception_type = type(error).__name__

    @staticmethod
    def _duration_ms(started: float | None, completed: float | None) -> float | None:
        if started is None or completed is None:
            return None
        return max(completed - started, 0.0) * 1_000

    @staticmethod
    def _opaque_identity(value: object | None) -> str | None:
        if value is None:
            return None
        value_type = type(value)
        return f"{value_type.__module__}.{value_type.__qualname__}:{id(value):x}"

    @staticmethod
    def _connection_from_pool_queue(raw: Any) -> object | None:
        """Read one unambiguous idle connection without removing it from the pool."""

        try:
            pool = getattr(raw, "_pool", None)
            connection_queue = getattr(pool, "pool", None)
            mutex = getattr(connection_queue, "mutex", None)
            queue_values = getattr(connection_queue, "queue", None)
            if mutex is None or queue_values is None:
                return None
            with mutex:
                connections = tuple(
                    connection for connection in queue_values if connection is not None
                )
            if len(connections) == 1:
                return connections[0]
        except Exception:
            return None
        return None

    @classmethod
    def _transport_identity_evidence(cls, response: Any | None) -> _TransportIdentityEvidence:
        """Inspect retained response objects after the authoritative receive timestamp."""

        try:
            raw = getattr(response, "raw", None)
            connection = cls._connection_from_pool_queue(raw)
            if connection is None:
                connection = getattr(raw, "connection", None)
            if connection is None:
                connection = getattr(raw, "_connection", None)
            socket = getattr(connection, "sock", None)
            if socket is None:
                original_response = getattr(raw, "_original_response", None)
                file_pointer = getattr(original_response, "fp", None)
                raw_file_pointer = getattr(file_pointer, "raw", None)
                socket = getattr(raw_file_pointer, "_sock", None)
            session_reused = getattr(socket, "session_reused", None)
            if not isinstance(session_reused, bool):
                session_reused = None
            return _TransportIdentityEvidence(
                urllib3_connection_identity=cls._opaque_identity(connection),
                tls_socket_identity=cls._opaque_identity(socket),
                tls_session_reused=session_reused,
            )
        except Exception:
            return _TransportIdentityEvidence(None, None, None)

    def _connection_reuse_evidence(
        self,
        evidence: _TransportIdentityEvidence,
    ) -> tuple[bool | None, bool | None]:
        with self._diagnostics_lock:
            connection_reused = (
                None
                if evidence.urllib3_connection_identity is None
                or self._last_urllib3_connection_identity is None
                else evidence.urllib3_connection_identity
                == self._last_urllib3_connection_identity
            )
            socket_reused = (
                None
                if evidence.tls_socket_identity is None
                or self._last_tls_socket_identity is None
                else evidence.tls_socket_identity == self._last_tls_socket_identity
            )
            self._last_urllib3_connection_identity = evidence.urllib3_connection_identity
            self._last_tls_socket_identity = evidence.tls_socket_identity
        return connection_reused, socket_reused

    def _capture_response_path(
        self,
        pending: _PendingHttpDiagnostics,
        response: Any,
    ) -> Any:
        try:
            observation = self._peer_path_observer.observe_response(response)
        except Exception:
            return response
        pending.path_observation = observation
        return response

    def _response_hook(
        self,
        pending: _PendingHttpDiagnostics,
    ) -> Callable[..., Any]:
        def observe(response: Any, *_args: Any, **_kwargs: Any) -> Any:
            return self._capture_response_path(pending, response)

        return observe

    def _get_json_under_transport_lock(
        self,
        pending: _PendingHttpDiagnostics,
        url: str,
        params: Mapping[str, object],
        timeout_seconds: float,
    ) -> Any:
        pending.lock_acquired_at = self._monotonic()
        pending.counter_baseline_current = (
            pending.counters_before_completion_sequence
            == self._completed_request_sequence
        )
        if self._closed:
            error = RuntimeError("Binance public HTTP transport is closed")
            self._mark_failure(pending, stage="transport_lock", error=error)
            raise error

        pending.get_started_at = self._monotonic()
        try:
            response = self._session.get(
                url,
                params={key: str(value) for key, value in params.items()},
                timeout=timeout_seconds,
                headers={"User-Agent": "HyperLab/0.2 public-market-data"},
                allow_redirects=False,
                hooks={'response': [self._response_hook(pending)]},
            )
        except Exception as exc:
            pending.get_completed_at = self._monotonic()
            self._mark_failure(pending, stage="session_get", error=exc)
            raise
        pending.get_completed_at = self._monotonic()
        if pending.path_observation is None:
            response = self._capture_response_path(pending, response)
        pending.response = response

        try:
            if 300 <= response.status_code < 400:
                error = RuntimeError(
                    f"Binance public HTTP redirect refused: {response.status_code}"
                )
                self._mark_failure(pending, stage="http_status", error=error)
                raise error
            response.raise_for_status()
        except requests.HTTPError as exc:
            error = RuntimeError(f"Binance public HTTP error {response.status_code}")
            self._mark_failure(pending, stage="http_status", error=error)
            raise error from exc
        except Exception as exc:
            if pending.outcome != "failure":
                self._mark_failure(pending, stage="http_status", error=exc)
            raise

        pending.decode_started_at = self._monotonic()
        try:
            payload = response.json()
        except Exception as exc:
            pending.decode_completed_at = self._monotonic()
            self._mark_failure(pending, stage="json_decode", error=exc)
            raise
        pending.decode_completed_at = self._monotonic()
        return payload

    def get_json(self, url: str, params: Mapping[str, object], timeout_seconds: float) -> Any:
        pending = self._diagnostics.value
        if (
            pending is None
            or pending.url != url
            or pending.lock_requested_at is not None
        ):
            # Direct transport callers have no authoritative clock boundary.
            self.prepare_diagnostics(url)
            pending = self._diagnostics.value
        if pending is None:
            raise RuntimeError("HTTP diagnostics preparation failed")

        pending.lock_requested_at = self._monotonic()
        try:
            with self._lock:
                try:
                    return self._get_json_under_transport_lock(
                        pending,
                        url,
                        params,
                        timeout_seconds,
                    )
                finally:
                    self._completed_request_sequence += 1
                    pending.completion_sequence = self._completed_request_sequence
        except Exception as exc:
            if pending.outcome != "failure":
                self._mark_failure(pending, stage="transport_lock", error=exc)
            raise

    def consume_diagnostics(self) -> HttpRequestDiagnostics | None:
        """Finalize diagnostics after the authoritative receive timestamp."""

        pending = self._diagnostics.value
        self._diagnostics.value = None
        if pending is None:
            return None
        finalize_started_at = self._monotonic()

        with self._lock:
            finalization_completion_sequence = self._completed_request_sequence
            post_request_observation_current = (
                pending.completion_sequence is not None
                and pending.completion_sequence == finalization_completion_sequence
            )
            if post_request_observation_current:
                counters_after = self._pool_snapshot(pending.url)
                path_observation = pending.path_observation
                fallback_evidence = self._transport_identity_evidence(pending.response)
                if path_observation is None:
                    evidence = fallback_evidence
                else:
                    evidence = _TransportIdentityEvidence(
                        urllib3_connection_identity=(
                            path_observation.urllib3_connection_identity
                            or fallback_evidence.urllib3_connection_identity
                        ),
                        tls_socket_identity=(
                            path_observation.tls_socket_identity
                            or fallback_evidence.tls_socket_identity
                        ),
                        tls_session_reused=(
                            path_observation.tls_session_reused
                            if path_observation.tls_session_reused is not None
                            else fallback_evidence.tls_session_reused
                        ),
                    )
                connection_reused, socket_reused = self._connection_reuse_evidence(
                    evidence
                )
            else:
                counters_after = None
                evidence = _TransportIdentityEvidence(None, None, None)
                connection_reused, socket_reused = None, None

        counters_before = pending.urllib3_counters_before
        counter_window_current = (
            post_request_observation_current and pending.counter_baseline_current
        )
        connection_objects_created_delta = (
            counters_after[0] - counters_before[0]
            if counter_window_current
            and counters_before is not None
            and counters_after is not None
            else None
        )
        requests_started_delta = (
            counters_after[1] - counters_before[1]
            if counter_window_current
            and counters_before is not None
            and counters_after is not None
            else None
        )
        new_connection_object = (
            None
            if connection_objects_created_delta is None
            or connection_objects_created_delta < 0
            else connection_objects_created_delta > 0
        )
        tls_session_reused = (
            evidence.tls_session_reused if socket_reused is not True else None
        )
        transport_lock_wait_ms = self._duration_ms(
            pending.lock_requested_at,
            pending.lock_acquired_at,
        )
        adapter_header_elapsed_ms = self._elapsed_ms(pending.response)
        session_get_total_ms = self._duration_ms(
            pending.get_started_at,
            pending.get_completed_at,
        )
        json_decode_ms = self._duration_ms(
            pending.decode_started_at,
            pending.decode_completed_at,
        )
        path_observation = pending.path_observation
        finalize_completed_at = self._monotonic()
        return HttpRequestDiagnostics(
            transport_lock_wait_ms=transport_lock_wait_ms,
            requests_adapter_header_elapsed_ms=adapter_header_elapsed_ms,
            session_get_total_ms=session_get_total_ms,
            json_decode_ms=json_decode_ms,
            pool_connections_before=(
                None if counters_before is None else counters_before[0]
            ),
            pool_connections_after=(
                None if counters_after is None else counters_after[0]
            ),
            pool_connection_delta=connection_objects_created_delta,
            pool_requests_before=(
                None if counters_before is None else counters_before[1]
            ),
            pool_requests_after=(
                None if counters_after is None else counters_after[1]
            ),
            pool_request_delta=requests_started_delta,
            new_pool_connection_created=new_connection_object,
            outcome=pending.outcome,
            failure_stage=pending.failure_stage,
            exception_type=pending.exception_type,
            requests_session_reused=pending.requests_session_reused,
            urllib3_connection_identity=evidence.urllib3_connection_identity,
            urllib3_connection_reused=connection_reused,
            tls_socket_identity=evidence.tls_socket_identity,
            tls_socket_reused=socket_reused,
            tls_session_reused=tls_session_reused,
            diagnostic_prepare_ms=pending.diagnostic_prepare_ms,
            diagnostic_finalize_ms=max(
                finalize_completed_at - finalize_started_at,
                0.0,
            )
            * 1_000,
            request_completion_sequence=pending.completion_sequence,
            finalization_completion_sequence=finalization_completion_sequence,
            post_request_observation_current=post_request_observation_current,
            peer_ip=None if path_observation is None else path_observation.peer_ip,
            peer_port=None if path_observation is None else path_observation.peer_port,
            socket_family=(
                None if path_observation is None else path_observation.socket_family
            ),
            response_cloudfront_pop=(
                None
                if path_observation is None
                else path_observation.response_cloudfront_pop
            ),
            response_cache=(
                None if path_observation is None else path_observation.response_cache
            ),
        )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._session.close()


@dataclass(frozen=True, slots=True)
class TimedPayload:
    payload: Any
    request_sent_time: datetime
    response_received_time: datetime
    http_diagnostics: HttpRequestDiagnostics | None = None


class BinancePublicRestClient:
    """GET-only, keyless Binance USD-M market-data client."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 15.0,
        transport: JsonGetTransport | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.timeout_seconds = timeout_seconds
        self.transport = transport or RequestsJsonTransport()
        self._closed = False
        self.clock = clock

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        close = getattr(self.transport, "close", None)
        if callable(close):
            close()

    @staticmethod
    def _attach_failure_diagnostics(
        error: Exception,
        *,
        request_sent_time: datetime,
        response_received_time: datetime,
        http_diagnostics: HttpRequestDiagnostics,
    ) -> bool:
        """Preserve the transport exception type when it accepts annotations."""

        try:
            setattr(error, "request_sent_time", request_sent_time)  # noqa: B010
            setattr(error, "response_received_time", response_received_time)  # noqa: B010
            setattr(error, "http_diagnostics", http_diagnostics)  # noqa: B010
        except Exception:
            return False
        return True

    def _get(self, path: str, params: Mapping[str, object] | None = None) -> TimedPayload:
        if self._closed:
            raise RuntimeError("Binance public REST client is closed")
        if path not in PUBLIC_GET_PATHS:
            raise ValueError(f"Binance endpoint is outside the public market-data allowlist: {path}")
        url = REST_BASE_URL + path
        prepare_diagnostics = getattr(self.transport, "prepare_diagnostics", None)
        if callable(prepare_diagnostics):
            prepare_diagnostics(url)

        sent = self.clock()
        try:
            payload = self.transport.get_json(
                url,
                {} if params is None else params,
                self.timeout_seconds,
            )
        except Exception as exc:
            received = self.clock()
            diagnostics = None
            consume_diagnostics = getattr(self.transport, "consume_diagnostics", None)
            if callable(consume_diagnostics):
                diagnostics = consume_diagnostics()
            if diagnostics is None:
                raise
            if self._attach_failure_diagnostics(
                exc,
                request_sent_time=sent,
                response_received_time=received,
                http_diagnostics=diagnostics,
            ):
                raise
            raise BinancePublicHttpRequestError(
                exc,
                request_sent_time=sent,
                response_received_time=received,
                http_diagnostics=diagnostics,
            ) from exc

        received = self.clock()
        diagnostics = None
        consume_diagnostics = getattr(self.transport, "consume_diagnostics", None)
        if callable(consume_diagnostics):
            diagnostics = consume_diagnostics()
        return TimedPayload(payload, sent, received, diagnostics)

    def exchange_info(self) -> TimedPayload:
        return self._get("/fapi/v1/exchangeInfo")

    def clock_measurement(self) -> ClockMeasurement:
        timed = self._get("/fapi/v1/time")
        server = _mapping(timed.payload, label="server time")
        return measure_clock(
            VENUE,
            request_sent_time=timed.request_sent_time,
            response_received_time=timed.response_received_time,
            server_time=_datetime_ms(server["serverTime"]),
            http_diagnostics=timed.http_diagnostics,
        )

    def funding_history(self, symbol: str, start_ms: int, end_ms: int) -> TimedPayload:
        if start_ms < 0 or end_ms < start_ms:
            raise ValueError("invalid Binance funding time range")
        return self._get(
            "/fapi/v1/fundingRate",
            {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000},
        )

    def funding_info(self) -> TimedPayload:
        return self._get("/fapi/v1/fundingInfo")

    def klines(self, symbol: str, interval: str, start_ms: int, end_ms: int) -> TimedPayload:
        if start_ms < 0 or end_ms < start_ms:
            raise ValueError("invalid Binance kline time range")
        cursor = start_ms
        rows: list[Any] = []
        first_sent: datetime | None = None
        last_received: datetime | None = None
        while cursor <= end_ms:
            page = self._get(
                "/fapi/v1/klines",
                {
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 1500,
                },
            )
            if first_sent is None:
                first_sent = page.request_sent_time
            last_received = page.response_received_time
            items = list(_sequence(page.payload, label="klines page"))
            rows.extend(items)
            if len(items) < 1500:
                break
            last = _sequence(items[-1], label="last kline")
            next_cursor = int(str(last[0])) + 1
            if next_cursor <= cursor:
                raise RuntimeError("Binance kline pagination made no progress")
            cursor = next_cursor
        assert first_sent is not None and last_received is not None
        return TimedPayload(rows, first_sent, last_received)

    def book_ticker(self, symbol: str) -> TimedPayload:
        return self._get("/fapi/v1/ticker/bookTicker", {"symbol": symbol})

    def aggregate_trades(self, symbol: str, start_ms: int, end_ms: int) -> TimedPayload:
        if start_ms < 0 or end_ms < start_ms:
            raise ValueError("invalid Binance aggregate trade time range")
        return self._get(
            "/fapi/v1/aggTrades",
            {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000},
        )


def diagnose_binance_http_paths(
    *,
    samples: int = 10,
    fresh_sample_count: int = 1,
    interval_seconds: float = 1.0,
    timeout_seconds: float = 15.0,
    client_factory: Callable[[], Any] | None = None,
    resolver: Resolver | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    runtime_status: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if client_factory is None:

        def factory() -> BinancePublicRestClient:
            return BinancePublicRestClient(timeout_seconds=timeout_seconds)

    else:
        factory = client_factory
    arguments: dict[str, Any] = {
        "samples": samples,
        "fresh_sample_count": fresh_sample_count,
        "interval_seconds": interval_seconds,
        "client_factory": factory,
        "sleeper": sleeper,
        "runtime_status": runtime_status,
    }
    if resolver is not None:
        arguments["resolver"] = resolver
    return diagnose_http_paths(**arguments)


def _positive_milliseconds(value: timedelta, *, label: str) -> int:
    milliseconds = Decimal(str(value.total_seconds())) * 1_000
    if milliseconds <= 0 or milliseconds != milliseconds.to_integral_value():
        raise ValueError(f"{label} must be a positive whole number of milliseconds")
    return int(milliseconds)


def _optional_non_negative_milliseconds(
    value: Decimal | float | None,
    *,
    label: str,
) -> Decimal | None:
    result = _decimal(value, required=False)
    if result is not None and result < 0:
        raise ValueError(f"{label} must be non-negative")
    if result is None:
        return None
    try:
        return result.quantize(Decimal("0.000000000000000001"))
    except InvalidOperation as exc:
        raise ValueError(f"{label} exceeds the durable decimal range") from exc


def clock_record(
    measurement: ClockMeasurement,
    observation_id: str,
    *,
    connection_id: str | None = None,
    connection_epoch: int | None = None,
    capture_epoch_id: str | None = None,
    sampling_interval: timedelta = timedelta(seconds=10),
    max_age: timedelta = timedelta(seconds=15),
    max_uncertainty_ms: Decimal = Decimal("50"),
    clock_schedule_overdue_ms: Decimal | float | None = None,
    single_flight_blocked_ms: Decimal | float | None = None,
    executor_submit_to_worker_start_ms: Decimal | float | None = None,
    worker_completion_to_supervisor_drain_ms: Decimal | float | None = None,
) -> ParsedRecord:
    sampling_interval_ms = _positive_milliseconds(
        sampling_interval,
        label="clock sampling interval",
    )
    max_age_ms = _positive_milliseconds(max_age, label="clock maximum age")
    if max_age < sampling_interval:
        raise ValueError("clock maximum age cannot be shorter than the sampling interval")
    if not max_uncertainty_ms.is_finite() or max_uncertainty_ms < 0:
        raise ValueError("clock maximum uncertainty must be finite and non-negative")
    if connection_epoch is not None and connection_epoch < 1:
        raise ValueError("connection_epoch must be positive when present")
    if connection_id is not None and not connection_id.strip():
        raise ValueError("connection_id must be non-empty when present")
    if capture_epoch_id is not None and not capture_epoch_id.strip():
        raise ValueError("capture_epoch_id must be non-empty when present")
    diagnostics = measurement.http_diagnostics

    missing_identity = (
        connection_id is None or connection_epoch is None or capture_epoch_id is None
    )
    uncertainty_exceeded = measurement.drift_uncertainty_ms > max_uncertainty_ms
    if missing_identity:
        sample_status = "invalid"
        invalid_reason = "missing active capture identity"
    elif uncertainty_exceeded:
        sample_status = "invalid"
        invalid_reason = (
            "clock uncertainty exceeds threshold: "
            f"{measurement.drift_uncertainty_ms}ms > {max_uncertainty_ms}ms"
        )
    else:
        sample_status = "valid"
        invalid_reason = None
    causal_valid_from = (
        measurement.response_received_time if sample_status == "valid" else None
    )
    causal_valid_until = (
        measurement.response_received_time + max_age if sample_status == "valid" else None
    )
    row = {
        "schema_version": latest_schema_for(RecordType.CLOCK_SYNC).version,
        "record_type": RecordType.CLOCK_SYNC.value,
        "venue": measurement.venue,
        "asset": "GLOBAL",
        "event_time": measurement.server_time,
        "exchange_time": measurement.server_time,
        "received_time": measurement.response_received_time,
        "source_sequence": None,
        "connection_id": connection_id,
        "request_sent_time": measurement.request_sent_time,
        "response_received_time": measurement.response_received_time,
        "server_time": measurement.server_time,
        "round_trip_latency_ms": measurement.round_trip_latency_ms,
        "estimated_clock_drift_ms": measurement.estimated_clock_drift_ms,
        "drift_uncertainty_ms": measurement.drift_uncertainty_ms,
        "observation_id": observation_id,
        "connection_epoch": connection_epoch,
        "capture_epoch_id": capture_epoch_id,
        "causal_valid_from": causal_valid_from,
        "causal_valid_until": causal_valid_until,
        "sample_status": sample_status,
        "invalid_reason": invalid_reason,
        "sampling_interval_ms": sampling_interval_ms,
        "max_age_ms": max_age_ms,
        "max_uncertainty_ms": max_uncertainty_ms,
        "clock_schedule_overdue_ms": _optional_non_negative_milliseconds(
            clock_schedule_overdue_ms,
            label="clock schedule overdue",
        ),
        "single_flight_blocked_ms": _optional_non_negative_milliseconds(
            single_flight_blocked_ms,
            label="single flight blocked",
        ),
        "executor_submit_to_worker_start_ms": _optional_non_negative_milliseconds(
            executor_submit_to_worker_start_ms,
            label="executor submit to worker start",
        ),
        "worker_completion_to_supervisor_drain_ms": _optional_non_negative_milliseconds(
            worker_completion_to_supervisor_drain_ms,
            label="worker completion to supervisor drain",
        ),
        "transport_lock_wait_ms": _optional_non_negative_milliseconds(
            None if diagnostics is None else diagnostics.transport_lock_wait_ms,
            label="transport lock wait",
        ),
        "requests_adapter_header_elapsed_ms": _optional_non_negative_milliseconds(
            None
            if diagnostics is None
            else diagnostics.requests_adapter_header_elapsed_ms,
            label="Requests adapter header elapsed",
        ),
        "session_get_total_ms": _optional_non_negative_milliseconds(
            None if diagnostics is None else diagnostics.session_get_total_ms,
            label="session get total",
        ),
        "json_decode_ms": _optional_non_negative_milliseconds(
            None if diagnostics is None else diagnostics.json_decode_ms,
            label="JSON decode",
        ),
        "diagnostic_prepare_ms": _optional_non_negative_milliseconds(
            None if diagnostics is None else diagnostics.diagnostic_prepare_ms,
            label="diagnostic prepare",
        ),
        "diagnostic_finalize_ms": _optional_non_negative_milliseconds(
            None if diagnostics is None else diagnostics.diagnostic_finalize_ms,
            label="diagnostic finalize",
        ),
        "new_urllib3_connection_object_created": (
            None if diagnostics is None else diagnostics.new_urllib3_connection_object_created
        ),
        "requests_session_reused": (
            None if diagnostics is None else diagnostics.requests_session_reused
        ),
        "urllib3_connection_identity": (
            None if diagnostics is None else diagnostics.urllib3_connection_identity
        ),
        "urllib3_connection_reused": (
            None if diagnostics is None else diagnostics.urllib3_connection_reused
        ),
        "tls_socket_identity": (
            None if diagnostics is None else diagnostics.tls_socket_identity
        ),
        "tls_socket_reused": None if diagnostics is None else diagnostics.tls_socket_reused,
        "tls_session_reused": None if diagnostics is None else diagnostics.tls_session_reused,
        "post_request_observation_current": (
            None if diagnostics is None else diagnostics.post_request_observation_current
        ),
        "peer_ip": None if diagnostics is None else diagnostics.peer_ip,
        "peer_port": None if diagnostics is None else diagnostics.peer_port,
        "socket_family": None if diagnostics is None else diagnostics.socket_family,
        "response_cloudfront_pop": (
            None if diagnostics is None else diagnostics.response_cloudfront_pop
        ),
        "response_cache": None if diagnostics is None else diagnostics.response_cache,
    }
    return ParsedRecord(RecordType.CLOCK_SYNC, "GLOBAL", row)


def parse_funding_history(
    payload: object,
    *,
    received_time: datetime,
    normalized: NormalizedInstrument,
    expected_interval_seconds: int | None,
) -> tuple[ParsedRecord, ...]:
    """Preserve settlements under an explicitly sourced venue schedule."""

    if expected_interval_seconds is not None and expected_interval_seconds <= 0:
        raise ValueError("expected funding interval must be positive")
    items = [_mapping(item, label="funding item") for item in _sequence(payload, label="funding history")]
    items.sort(key=lambda item: int(str(item["fundingTime"])))
    timestamps = [int(str(item["fundingTime"])) for item in items]
    if expected_interval_seconds is None:
        differences = [
            (current - previous) // 1_000
            for previous, current in pairwise(timestamps)
            if current > previous and (current - previous) % 1_000 == 0
        ]
        if not differences:
            raise ValueError("at least two ordered funding settlements are required to infer cadence")
        expected_interval_seconds = min(differences)
    records: list[ParsedRecord] = []
    for index, item in enumerate(items):
        funding_time = _datetime_ms(timestamps[index])
        observation_id = f"rest:{normalized.source_symbol}:{timestamps[index]}:{int(received_time.timestamp() * 1_000)}"
        row = {
            "schema_version": latest_schema_for(RecordType.FUNDING).version,
            "record_type": RecordType.FUNDING.value,
            "venue": VENUE,
            "asset": normalized.asset,
            "event_time": funding_time,
            "exchange_time": funding_time,
            "received_time": received_time,
            "source_sequence": None,
            "connection_id": None,
            "funding_time": funding_time,
            "funding_rate": _required_decimal(item["fundingRate"]),
            "funding_interval_seconds": expected_interval_seconds,
            "rate_kind": "realized",
            "mark_price": _decimal(item.get("markPrice"), required=False),
            "oracle_price": None,
            "observation_id": observation_id,
        }
        records.append(ParsedRecord(RecordType.FUNDING, normalized.asset, row))
    return tuple(records)


def funding_intervals(payload: object) -> dict[str, int]:
    """Return adjusted public schedules; absence must never imply eight hours."""

    result: dict[str, int] = {}
    for raw_item in _sequence(payload, label="funding information"):
        item = _mapping(raw_item, label="funding information item")
        symbol = str(item.get("symbol", "")).upper()
        interval_hours = int(str(item.get("fundingIntervalHours", "0")))
        if not symbol or interval_hours <= 0:
            raise ValueError("Binance funding information has an invalid symbol or interval")
        result[symbol] = interval_hours * 3_600
    return result


def parse_klines(
    payload: object,
    *,
    received_time: datetime,
    normalized: NormalizedInstrument,
    interval: str,
) -> tuple[ParsedRecord, ...]:
    records: list[ParsedRecord] = []
    for index, raw_item in enumerate(_sequence(payload, label="klines")):
        item = _sequence(raw_item, label="kline")
        if len(item) < 9:
            raise ValueError("Binance kline must contain at least nine fields")
        open_time = _datetime_ms(item[0])
        close_time = _datetime_ms(item[6])
        observation_id = (
            f"rest:{normalized.source_symbol}:{interval}:{int(str(item[0]))}:"
            f"{int(received_time.timestamp() * 1_000)}:{index}"
        )
        row = {
            "schema_version": latest_schema_for(RecordType.CANDLE).version,
            "record_type": RecordType.CANDLE.value,
            "venue": VENUE,
            "asset": normalized.asset,
            "event_time": open_time,
            "exchange_time": None,
            "received_time": received_time,
            "source_sequence": None,
            "connection_id": None,
            "interval": interval,
            "open_time": open_time,
            "close_time": close_time,
            "open": _required_decimal(item[1]),
            "high": _required_decimal(item[2]),
            "low": _required_decimal(item[3]),
            "close": _required_decimal(item[4]),
            "base_volume": normalized.normalize_quantity(item[5]),
            "quote_volume": _required_decimal(item[7]),
            "trade_count": int(str(item[8])),
            # REST klines may include the still-open interval. Wall-clock
            # comparison would fabricate source finality, so preserve unknown.
            "is_final": None,
            "observation_id": observation_id,
        }
        records.append(ParsedRecord(RecordType.CANDLE, normalized.asset, row))
    return tuple(records)
