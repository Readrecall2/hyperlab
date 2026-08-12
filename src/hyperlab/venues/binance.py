from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from itertools import pairwise
from typing import Any, Protocol

from hyperlab.collector.models import ParsedMessage, ParsedRecord, WireEnvelope
from hyperlab.data.schema import RecordType, instrument, latest_schema_for
from hyperlab.venues.base import ClockMeasurement, NormalizedInstrument, measure_clock

VENUE = "binance_usdm"
REST_BASE_URL = "https://fapi.binance.com"
WS_BASE_URL = "wss://fstream.binance.com/stream?streams="

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

    def websocket_url(self, assets: tuple[str, ...], candle_intervals: tuple[str, ...]) -> str:
        streams: list[str] = []
        for asset in assets:
            symbol = self.instrument_for_asset(asset).source_symbol.lower()
            streams.extend((f"{symbol}@bookTicker", f"{symbol}@aggTrade", f"{symbol}@markPrice@1s"))
            streams.extend(f"{symbol}@kline_{interval}" for interval in candle_intervals)
        return WS_BASE_URL + "/".join(streams)

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
    if channel is None or data is None:
        issues.append("invalid_combined_stream_envelope")
        return ParsedMessage(channel, tuple(records), tuple(issues))
    if normalized is None:
        issues.append(f"unknown_symbol:{source_symbol or 'missing'}")
        return ParsedMessage(channel, tuple(records), tuple(issues))
    try:
        event = str(data.get("e", ""))
        if event == "bookTicker" or channel.endswith("@bookTicker"):
            records.append(_parse_bbo(data, envelope, normalized))
        elif event == "aggTrade" or channel.endswith("@aggTrade"):
            records.append(_parse_trade(data, envelope, normalized))
        elif event == "kline" or "@kline_" in channel:
            records.append(_parse_candle(data, envelope, normalized))
        elif event == "markPriceUpdate" or "@markPrice" in channel:
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
            "aggressor_side": "sell" if bool(data["m"]) else "buy",
            "price": price,
            "quantity": quantity,
            "quote_quantity": price * quantity,
            "is_liquidation": None,
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


class UrllibJsonTransport:
    def get_json(self, url: str, params: Mapping[str, object], timeout_seconds: float) -> Any:
        query = urllib.parse.urlencode({key: str(value) for key, value in params.items()})
        request_url = url if not query else f"{url}?{query}"
        request = urllib.request.Request(
            request_url,
            method="GET",
            headers={"User-Agent": "HyperLab/0.2 public-market-data"},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"Binance public HTTP error {exc.code}") from exc


@dataclass(frozen=True, slots=True)
class TimedPayload:
    payload: Any
    request_sent_time: datetime
    response_received_time: datetime


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
        self.transport = transport or UrllibJsonTransport()
        self.clock = clock

    def _get(self, path: str, params: Mapping[str, object] | None = None) -> TimedPayload:
        if path not in PUBLIC_GET_PATHS:
            raise ValueError(f"Binance endpoint is outside the public market-data allowlist: {path}")
        sent = self.clock()
        payload = self.transport.get_json(
            REST_BASE_URL + path,
            {} if params is None else params,
            self.timeout_seconds,
        )
        received = self.clock()
        return TimedPayload(payload, sent, received)

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


def clock_record(measurement: ClockMeasurement, observation_id: str) -> ParsedRecord:
    row = {
        "schema_version": latest_schema_for(RecordType.CLOCK_SYNC).version,
        "record_type": RecordType.CLOCK_SYNC.value,
        "venue": measurement.venue,
        "asset": "GLOBAL",
        "event_time": measurement.server_time,
        "exchange_time": measurement.server_time,
        "received_time": measurement.response_received_time,
        "source_sequence": None,
        "connection_id": None,
        "request_sent_time": measurement.request_sent_time,
        "response_received_time": measurement.response_received_time,
        "server_time": measurement.server_time,
        "round_trip_latency_ms": measurement.round_trip_latency_ms,
        "estimated_clock_drift_ms": measurement.estimated_clock_drift_ms,
        "drift_uncertainty_ms": measurement.drift_uncertainty_ms,
        "observation_id": observation_id,
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
