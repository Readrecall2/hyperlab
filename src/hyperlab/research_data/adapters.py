from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Final, cast
from urllib.parse import unquote, urlsplit

from .canonical import CanonicalValue, canonical_json_bytes
from .envelope import CaptureProvenance, PublicDataEnvelope, SessionEnvelopeFactory, Venue
from .prediction_time import prediction_rfc3339_to_ns

HYPERLIQUID_PUBLIC_HTTP_URL: Final = "https://api.hyperliquid.xyz/info"
HYPERLIQUID_PUBLIC_WEBSOCKET_URL: Final = "wss://api.hyperliquid.xyz/ws"
POLYMARKET_GAMMA_PUBLIC_URL: Final = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_PUBLIC_URL: Final = "https://clob.polymarket.com"
POLYMARKET_DATA_PUBLIC_URL: Final = "https://data-api.polymarket.com"
POLYMARKET_PUBLIC_WEBSOCKET_URL: Final = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
KALSHI_PUBLIC_HTTP_URL: Final = "https://external-api.kalshi.com/trade-api/v2"

HYPERLIQUID_METADATA_VERSION: Final = "hyperliquid-official-public-api-2026-08-26"
POLYMARKET_METADATA_VERSION: Final = "polymarket-official-public-api-2026-08-27"
KALSHI_METADATA_VERSION_V1: Final = "kalshi-official-public-rest-2026-08-27"
KALSHI_METADATA_VERSION: Final = "kalshi-official-public-rest-2026-08-28"
KALSHI_SUPPORTED_METADATA_VERSIONS: Final = frozenset(
    {KALSHI_METADATA_VERSION_V1, KALSHI_METADATA_VERSION}
)

_PRIVATE_PATH_SEGMENTS = {
    "account",
    "auth",
    "cancel",
    "exchange",
    "fill",
    "orders",
    "portfolio",
    "positions",
    "rfq",
    "user",
    "wallet",
}
_PUBLIC_HTTP_HOSTS = {
    "api.hyperliquid.xyz",
    "clob.polymarket.com",
    "data-api.polymarket.com",
    "external-api.kalshi.com",
    "gamma-api.polymarket.com",
    "mainnet.zklighter.elliot.ai",
}
_PUBLIC_WEBSOCKET_HOSTS = {
    "api.hyperliquid.xyz",
    "mainnet.zklighter.elliot.ai",
    "ws-subscriptions-clob.polymarket.com",
}
_PUBLIC_WEBSOCKET_URLS = {
    HYPERLIQUID_PUBLIC_WEBSOCKET_URL,
    POLYMARKET_PUBLIC_WEBSOCKET_URL,
    "wss://mainnet.zklighter.elliot.ai/stream",
    "wss://mainnet.zklighter.elliot.ai/stream?readonly=true",
}
_LIGHTER_PUBLIC_HTTP_PATH = "/api/v1/orderBooks"
_LIGHTER_PUBLIC_WEBSOCKET_PATH = "/stream"
_LIGHTER_PUBLIC_WEBSOCKET_QUERIES = {"", "readonly=true"}
_LIGHTER_PUBLIC_CHANNEL = re.compile(r"^(?:order_book|ticker|market_stats|trade)/[0-9]+$")
_PATH_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@+-]{0,511}$")
_KALSHI_PRICE_DOLLARS = re.compile(r"^(?:0(?:\.\d{1,4})?|1(?:\.0{1,4})?)$")
_KALSHI_COUNT_FP = re.compile(r"^(?:0|[1-9]\d*)\.\d{2}$")

# Kalshi REST V2 documents date-time response fields as RFC3339 text.  Numeric
# epochs currently appear only in request filters, not in any activated response
# field.  Keeping this allowlist empty prevents a JSON number from being guessed
# as seconds, milliseconds, microseconds, or nanoseconds.
_KALSHI_NUMERIC_EPOCH_RESPONSE_FIELDS: Final[dict[tuple[str, str], str]] = {}
_KALSHI_SOURCE_TIMESTAMP_FIELDS: Final = {
    "series": "last_updated_ts",
    "events": "last_updated_ts",
    "markets": "updated_time",
    "trades": "created_time",
    "block_trades": "created_time",
    "historical_markets": "updated_time",
    "historical_trades": "created_time",
}
_KALSHI_CURSOR_FIELDS: Final = {
    "events": "cursor",
    "markets": "cursor",
    "trades": "cursor",
    "block_trades": "cursor",
    "incentives": "next_cursor",
    "event_fee_changes": "cursor",
    "historical_markets": "cursor",
    "historical_trades": "cursor",
}


def _documented_public_get_route(host: str, path: str) -> bool:
    static_routes = {
        "clob.polymarket.com": {
            "/book",
            "/fee-rate",
            "/last-trade-price",
            "/tick-size",
        },
        "data-api.polymarket.com": {"/trades"},
        "external-api.kalshi.com": {
            "/trade-api/v2/events",
            "/trade-api/v2/events/fee_changes",
            "/trade-api/v2/exchange/schedule",
            "/trade-api/v2/exchange/status",
            "/trade-api/v2/historical/cutoff",
            "/trade-api/v2/historical/markets",
            "/trade-api/v2/historical/trades",
            "/trade-api/v2/incentive_programs",
            "/trade-api/v2/markets",
            "/trade-api/v2/markets/trades",
            "/trade-api/v2/series",
            "/trade-api/v2/series/fee_changes",
        },
        "gamma-api.polymarket.com": {"/events/keyset", "/markets/keyset"},
        "mainnet.zklighter.elliot.ai": {_LIGHTER_PUBLIC_HTTP_PATH},
    }
    if path in static_routes.get(host, set()):
        return True
    dynamic_patterns = {
        "clob.polymarket.com": (r"/(?:clob-markets|markets-by-token)/[A-Za-z0-9._:@+-]+",),
        "external-api.kalshi.com": (
            r"/trade-api/v2/events/[A-Za-z0-9._:@+-]+",
            r"/trade-api/v2/events/[A-Za-z0-9._:@+-]+/metadata",
            r"/trade-api/v2/historical/markets/[A-Za-z0-9._:@+-]+",
            r"/trade-api/v2/markets/[A-Za-z0-9._:@+-]+",
            r"/trade-api/v2/markets/[A-Za-z0-9._:@+-]+/orderbook",
            r"/trade-api/v2/series/[A-Za-z0-9._:@+-]+",
        ),
        "gamma-api.polymarket.com": (r"/(?:events|markets)/[A-Za-z0-9._:@+-]+",),
    }
    return any(re.fullmatch(pattern, path) for pattern in dynamic_patterns.get(host, ()))


def _mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _sequence(value: object, *, label: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{label} must be an array")
    return value


def _json(raw_payload: bytes) -> object:
    try:
        return json.loads(
            raw_payload.decode("utf-8"), parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value))
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("public venue payload is not strict UTF-8 JSON") from error


def _path_component(value: str, *, label: str) -> str:
    if _PATH_COMPONENT.fullmatch(value) is None:
        raise ValueError(f"{label} is not a safe public path component")
    return value


def _opaque_query_value(value: str, *, label: str) -> str:
    if not value or len(value) > 4_096 or any(ord(character) < 32 for character in value):
        raise ValueError(f"{label} is not a bounded opaque query value")
    return value


def _milliseconds_to_ns(value: object | None) -> int | None:
    if value is None or value == "":
        return None
    milliseconds = int(str(value))
    if milliseconds < 0:
        raise ValueError("source millisecond timestamp cannot be negative")
    return milliseconds * 1_000_000


def _iso_to_ns(value: object | None) -> int | None:
    if value is None or value == "":
        return None
    return prediction_rfc3339_to_ns(value, label="source timestamp")


def _kalshi_rfc3339_to_ns(value: object, *, feed_type: str, field: str) -> int:
    timestamp_ns = prediction_rfc3339_to_ns(
        value,
        label=f"Kalshi {feed_type}.{field}",
    )
    if timestamp_ns < 0:
        raise ValueError(
            f"Kalshi {feed_type}.{field} must normalize to a non-negative UTC epoch"
        )
    return timestamp_ns


def _kalshi_optional_timestamp(
    record: Mapping[str, Any],
    *,
    feed_type: str,
) -> int | None:
    field = _KALSHI_SOURCE_TIMESTAMP_FIELDS[feed_type]
    if field not in record or record[field] is None:
        return None
    numeric_unit = _KALSHI_NUMERIC_EPOCH_RESPONSE_FIELDS.get((feed_type, field))
    if numeric_unit is not None:
        value = record[field]
        if type(value) is not int or value < 0:
            raise ValueError(
                f"Kalshi {feed_type}.{field} must be a non-negative {numeric_unit} epoch integer"
            )
        multiplier = {
            "seconds": 1_000_000_000,
            "milliseconds": 1_000_000,
            "microseconds": 1_000,
            "nanoseconds": 1,
        }[numeric_unit]
        return value * multiplier
    return _kalshi_rfc3339_to_ns(record[field], feed_type=feed_type, field=field)


def _kalshi_cursor(decoded: Mapping[str, Any], *, feed_type: str) -> str | None:
    field = _KALSHI_CURSOR_FIELDS.get(feed_type)
    if field is None or field not in decoded:
        return None
    value = decoded[field]
    if value is None or value == "":
        return None
    if type(value) is not str:
        raise ValueError(f"Kalshi {feed_type} cursor must be absent, null, or exact text")
    return _opaque_query_value(value, label=f"Kalshi {feed_type} cursor")


def _kalshi_text(value: object, *, label: str) -> str:
    if type(value) is not str:
        raise ValueError(f"{label} must be exact text")
    return _path_component(value, label=label)


def _kalshi_price(value: object, *, label: str) -> Decimal:
    if type(value) is not str or _KALSHI_PRICE_DOLLARS.fullmatch(value) is None:
        raise ValueError(f"{label} must be a fixed-point dollar string with one to four decimals")
    try:
        price = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"{label} is not a finite fixed-point decimal") from error
    if not price.is_finite() or price < 0 or price > 1:
        raise ValueError(f"{label} is outside binary price bounds")
    return price


def _kalshi_count(value: object, *, label: str, allow_zero: bool = False) -> Decimal:
    if type(value) is not str or _KALSHI_COUNT_FP.fullmatch(value) is None:
        raise ValueError(f"{label} must be a fixed-point count string with two decimals")
    try:
        count = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"{label} is not a finite fixed-point decimal") from error
    if not count.is_finite() or count < 0 or (not allow_zero and count == 0):
        raise ValueError(f"{label} must be a positive finite quantity")
    return count


def _kalshi_mapping_records(
    decoded: Mapping[str, Any],
    *,
    feed_type: str,
    singular_key: str,
    plural_key: str,
) -> tuple[Mapping[str, Any], ...]:
    if singular_key == plural_key and singular_key in decoded:
        wrapped = decoded[singular_key]
        if isinstance(wrapped, Mapping):
            return (_mapping(wrapped, label=f"Kalshi {feed_type} record"),)
        return tuple(
            _mapping(item, label=f"Kalshi {feed_type} record")
            for item in _sequence(wrapped, label=f"Kalshi {feed_type} records")
        )
    singular_present = singular_key in decoded
    plural_present = plural_key in decoded
    if singular_present and plural_present:
        raise ValueError(f"Kalshi {feed_type} payload has ambiguous wrappers")
    if singular_present:
        return (_mapping(decoded[singular_key], label=f"Kalshi {feed_type} record"),)
    if plural_present:
        return tuple(
            _mapping(item, label=f"Kalshi {feed_type} record")
            for item in _sequence(decoded[plural_key], label=f"Kalshi {feed_type} records")
        )
    raise ValueError(f"Kalshi {feed_type} payload omitted its documented wrapper")


def _kalshi_validate_market_record(record: Mapping[str, Any], *, feed_type: str) -> str:
    ticker = _kalshi_text(record.get("ticker"), label="Kalshi market ticker")
    event_ticker = record.get("event_ticker")
    if event_ticker is not None:
        _kalshi_text(event_ticker, label="Kalshi event ticker")
    for field in (
        "yes_bid_dollars",
        "yes_ask_dollars",
        "no_bid_dollars",
        "no_ask_dollars",
        "last_price_dollars",
        "previous_yes_bid_dollars",
        "previous_yes_ask_dollars",
        "previous_price_dollars",
        "settlement_value_dollars",
    ):
        if field in record and record[field] is not None:
            _kalshi_price(record[field], label=f"Kalshi market {field}")
    for field in (
        "yes_bid_size_fp",
        "yes_ask_size_fp",
        "open_interest_fp",
        "volume_fp",
        "volume_24h_fp",
    ):
        if field in record and record[field] is not None:
            _kalshi_count(
                record[field],
                label=f"Kalshi market {field}",
                allow_zero=True,
            )
    return ticker


def _kalshi_validate_orderbook(decoded: Mapping[str, Any]) -> None:
    orderbook = _mapping(decoded.get("orderbook_fp"), label="Kalshi orderbook_fp")
    for side in ("yes_dollars", "no_dollars"):
        levels = _sequence(orderbook.get(side), label=f"Kalshi orderbook {side}")
        previous: Decimal | None = None
        for raw_level in levels:
            level = _sequence(raw_level, label=f"Kalshi orderbook {side} level")
            if len(level) != 2:
                raise ValueError("Kalshi orderbook level must contain price and quantity")
            price = _kalshi_price(level[0], label="Kalshi orderbook price")
            _kalshi_count(level[1], label="Kalshi orderbook quantity")
            if previous is not None and price <= previous:
                raise ValueError("Kalshi orderbook prices must be strictly increasing")
            previous = price


def _kalshi_validate_trade_record(
    record: Mapping[str, Any],
    *,
    expected_block_trade: bool | None,
) -> tuple[str, str]:
    trade_id = _kalshi_text(record.get("trade_id"), label="Kalshi trade id")
    ticker = _kalshi_text(record.get("ticker"), label="Kalshi trade ticker")
    block_trade = record.get("is_block_trade")
    if type(block_trade) is not bool:
        raise ValueError("Kalshi trade is_block_trade must be a boolean")
    if expected_block_trade is not None and block_trade is not expected_block_trade:
        raise ValueError("Kalshi block-trade feed classification diverged")
    count = _kalshi_count(record.get("count_fp"), label="Kalshi trade count_fp")
    if count <= 0:
        raise ValueError("Kalshi trade count must be positive")
    _kalshi_price(record.get("yes_price_dollars"), label="Kalshi YES trade price")
    _kalshi_price(record.get("no_price_dollars"), label="Kalshi NO trade price")
    if record.get("taker_outcome_side") not in {"yes", "no"}:
        raise ValueError("Kalshi taker outcome side is invalid")
    if record.get("taker_book_side") not in {"bid", "ask"}:
        raise ValueError("Kalshi taker book side is invalid")
    _kalshi_rfc3339_to_ns(
        record.get("created_time"),
        feed_type="trades",
        field="created_time",
    )
    return trade_id, ticker


@dataclass(frozen=True, slots=True)
class PublicHttpRequest:
    method: str
    url: str
    query: tuple[tuple[str, str], ...] = ()
    json_body: Mapping[str, CanonicalValue] | None = None

    def __post_init__(self) -> None:
        if self.method not in {"GET", "POST"}:
            raise ValueError("public adapters support GET and Hyperliquid info POST only")
        parsed = urlsplit(self.url)
        if (
            parsed.scheme != "https"
            or parsed.hostname not in _PUBLIC_HTTP_HOSTS
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or parsed.port not in {None, 443}
        ):
            raise ValueError("public HTTP request must use HTTPS")
        segments = {part.lower() for part in unquote(parsed.path).split("/") if part}
        exact_public_exchange_route = parsed.path in {
            "/trade-api/v2/exchange/schedule",
            "/trade-api/v2/exchange/status",
        }
        if segments & _PRIVATE_PATH_SEGMENTS and not exact_public_exchange_route:
            raise ValueError("private/account/order route is forbidden")
        if self.method == "POST" and self.url != HYPERLIQUID_PUBLIC_HTTP_URL:
            raise ValueError("POST is restricted to the public Hyperliquid info surface")
        if parsed.hostname == "mainnet.zklighter.elliot.ai" and (
            self.method != "GET"
            or parsed.path != _LIGHTER_PUBLIC_HTTP_PATH
            or self.json_body is not None
            or set(dict(self.query)) - {"filter", "market_id"}
        ):
            raise ValueError("Lighter HTTP is restricted to documented public market metadata")
        if self.method == "GET" and not _documented_public_get_route(parsed.hostname or "", parsed.path):
            raise ValueError("GET route is not in the documented public allowlist")
        if self.method == "GET" and self.json_body is not None:
            raise ValueError("GET public requests cannot carry a JSON body")
        if any(type(key) is not str or not key or type(value) is not str for key, value in self.query):
            raise ValueError("public HTTP query fields must be explicit text")
        if self.json_body is not None:
            canonical_json_bytes(self.json_body)


@dataclass(frozen=True, slots=True)
class PublicWebsocketSubscription:
    url: str
    payload: Mapping[str, CanonicalValue]

    def __post_init__(self) -> None:
        parsed = urlsplit(self.url)
        if (
            parsed.scheme != "wss"
            or parsed.hostname not in _PUBLIC_WEBSOCKET_HOSTS
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or parsed.port not in {None, 443}
            or self.url not in _PUBLIC_WEBSOCKET_URLS
        ):
            raise ValueError("public WebSocket request must use WSS")
        canonical_json_bytes(self.payload)
        serialized = canonical_json_bytes(self.payload).decode("utf-8").lower()
        forbidden_markers = ('"user"', '"orders"', '"fills"', '"twapstates"')
        if any(marker in serialized for marker in forbidden_markers):
            raise ValueError("credentialed or user-scoped WebSocket subscription is forbidden")
        if parsed.hostname == "mainnet.zklighter.elliot.ai" and (
            parsed.path != _LIGHTER_PUBLIC_WEBSOCKET_PATH
            or parsed.query not in _LIGHTER_PUBLIC_WEBSOCKET_QUERIES
            or set(self.payload) != {"channel", "type"}
            or self.payload.get("type") != "subscribe"
            or type(self.payload.get("channel")) is not str
            or _LIGHTER_PUBLIC_CHANNEL.fullmatch(cast(str, self.payload["channel"])) is None
        ):
            raise ValueError("Lighter WebSocket subscription is not an allowlisted public channel")


def canonical_hyperliquid_instrument(coin: str) -> str:
    cleaned = coin.strip()
    if not cleaned:
        raise ValueError("Hyperliquid coin cannot be empty")
    kind = "spot" if cleaned.startswith("@") or "/" in cleaned else "perp"
    return f"HL:{cleaned}:{kind}"


def canonical_polymarket_instrument(asset_id: str) -> str:
    if not asset_id:
        raise ValueError("Polymarket asset id cannot be empty")
    return f"PM:{asset_id}"


def canonical_kalshi_market(ticker: str) -> str:
    cleaned = ticker.strip().upper()
    if not cleaned:
        raise ValueError("Kalshi market ticker cannot be empty")
    return f"KALSHI:{cleaned}"


def canonical_kalshi_event(ticker: str) -> str:
    cleaned = ticker.strip().upper()
    if not cleaned:
        raise ValueError("Kalshi event ticker cannot be empty")
    return f"KALSHI_EVENT:{cleaned}"


def canonical_kalshi_series(ticker: str) -> str:
    cleaned = ticker.strip().upper()
    if not cleaned:
        raise ValueError("Kalshi series ticker cannot be empty")
    return f"KALSHI_SERIES:{cleaned}"


class HyperliquidPublicAdapter:
    venue = Venue.HYPERLIQUID
    metadata_version = HYPERLIQUID_METADATA_VERSION
    supported_feeds = frozenset({"bbo", "l2_book", "trades", "all_mids", "active_asset_context", "metadata"})
    unavailable_public_global_labels = (
        "TWAP_GLOBAL_PUBLIC_SOURCE_UNVERIFIED",
        "LIQUIDATION_GLOBAL_PUBLIC_SOURCE_UNVERIFIED",
    )

    def public_http_requests(
        self, *, feeds: Sequence[str], instruments: Sequence[str]
    ) -> tuple[PublicHttpRequest, ...]:
        requests: list[PublicHttpRequest] = []
        selected = set(feeds)
        self._validate_feeds(selected)
        if "metadata" in selected or "active_asset_context" in selected:
            requests.append(
                PublicHttpRequest(
                    method="POST",
                    url=HYPERLIQUID_PUBLIC_HTTP_URL,
                    json_body={"type": "metaAndAssetCtxs"},
                )
            )
            requests.append(
                PublicHttpRequest(
                    method="POST",
                    url=HYPERLIQUID_PUBLIC_HTTP_URL,
                    json_body={"type": "spotMetaAndAssetCtxs"},
                )
            )
        if "l2_book" in selected:
            for coin in instruments:
                requests.append(
                    PublicHttpRequest(
                        method="POST",
                        url=HYPERLIQUID_PUBLIC_HTTP_URL,
                        json_body={"coin": coin, "type": "l2Book"},
                    )
                )
        return tuple(requests)

    def websocket_subscriptions(
        self, *, feeds: Sequence[str], instruments: Sequence[str]
    ) -> tuple[PublicWebsocketSubscription, ...]:
        selected = set(feeds)
        self._validate_feeds(selected)
        subscriptions: list[PublicWebsocketSubscription] = []
        if "all_mids" in selected:
            subscriptions.append(
                PublicWebsocketSubscription(
                    HYPERLIQUID_PUBLIC_WEBSOCKET_URL,
                    {"method": "subscribe", "subscription": {"type": "allMids"}},
                )
            )
        mapping = {
            "bbo": "bbo",
            "l2_book": "l2Book",
            "trades": "trades",
            "active_asset_context": "activeAssetCtx",
        }
        for feed, subscription_type in mapping.items():
            if feed not in selected:
                continue
            for coin in instruments:
                subscriptions.append(
                    PublicWebsocketSubscription(
                        HYPERLIQUID_PUBLIC_WEBSOCKET_URL,
                        {
                            "method": "subscribe",
                            "subscription": {"coin": coin, "type": subscription_type},
                        },
                    )
                )
        return tuple(subscriptions)

    def _validate_feeds(self, feeds: set[str]) -> None:
        unknown = feeds - self.supported_feeds
        if unknown:
            raise ValueError(f"unsupported Hyperliquid public feeds: {sorted(unknown)}")

    def envelope_from_websocket(
        self,
        raw_payload: bytes,
        *,
        factory: SessionEnvelopeFactory,
        receive_timestamp_utc_ns: int,
        receive_monotonic_ns: int,
        provenance: CaptureProvenance | None = None,
    ) -> PublicDataEnvelope:
        payload = _mapping(_json(raw_payload), label="Hyperliquid WebSocket payload")
        channel = str(payload.get("channel") or "unknown")
        feed_map = {
            "activeAssetCtx": "active_asset_context",
            "activeSpotAssetCtx": "active_asset_context",
            "allMids": "all_mids",
            "bbo": "bbo",
            "l2Book": "l2_book",
            "trades": "trades",
            "subscriptionResponse": "subscription_ack",
            "pong": "heartbeat",
        }
        feed_type = feed_map.get(channel, f"unknown_{channel}")
        data = payload.get("data")
        coin: str | None = None
        source_timestamp_ns: int | None = None
        source_event_id: str | None = None
        if isinstance(data, Mapping):
            if data.get("coin") is not None:
                coin = str(data["coin"])
            source_timestamp_ns = _milliseconds_to_ns(data.get("time"))
        elif channel == "trades" and isinstance(data, Sequence) and not isinstance(data, str):
            trades = [item for item in data if isinstance(item, Mapping)]
            if trades:
                first = trades[0]
                coin = None if first.get("coin") is None else str(first["coin"])
                if len(trades) == 1:
                    source_timestamp_ns = _milliseconds_to_ns(first.get("time"))
                    if all(first.get(key) is not None for key in ("time", "coin", "tid")):
                        source_event_id = f"{first['time']}:{first['coin']}:{first['tid']}"
        instrument_id = None if coin is None else canonical_hyperliquid_instrument(coin)
        if instrument_id is None:
            instrument_id = "HL:GLOBAL:public"
        return factory.make(
            feed_type=feed_type,
            instrument_id=instrument_id,
            market_id=None,
            source_timestamp_ns=source_timestamp_ns,
            receive_timestamp_utc_ns=receive_timestamp_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            raw_payload=raw_payload,
            source_sequence=None,
            source_event_id=source_event_id,
            provenance=provenance,
        )

    def envelope_from_http(
        self,
        raw_payload: bytes,
        *,
        feed_type: str,
        instrument: str | None,
        factory: SessionEnvelopeFactory,
        receive_timestamp_utc_ns: int,
        receive_monotonic_ns: int,
        provenance: CaptureProvenance | None = None,
    ) -> PublicDataEnvelope:
        self._validate_feeds({feed_type})
        _json(raw_payload)
        return factory.make(
            feed_type=feed_type,
            instrument_id=(
                "HL:GLOBAL:public" if instrument is None else canonical_hyperliquid_instrument(instrument)
            ),
            market_id=None,
            source_timestamp_ns=None,
            receive_timestamp_utc_ns=receive_timestamp_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            raw_payload=raw_payload,
            source_sequence=None,
            provenance=provenance,
        )


class PolymarketPublicAdapter:
    venue = Venue.POLYMARKET
    metadata_version = POLYMARKET_METADATA_VERSION
    supported_feeds = frozenset(
        {
            "metadata",
            "events",
            "order_book",
            "public_trades",
            "last_trade_price",
            "tick_size",
            "fees",
            "price_change",
            "best_bid_ask",
            "tick_size_change",
            "market_lifecycle",
        }
    )

    def market_census_request(self, *, limit: int, after_cursor: str | None = None) -> PublicHttpRequest:
        if limit <= 0 or limit > 100:
            raise ValueError("Polymarket census limit must be within 1..100")
        query = [("closed", "false"), ("limit", str(limit))]
        if after_cursor is not None:
            query.append(
                (
                    "after_cursor",
                    _opaque_query_value(after_cursor, label="Polymarket market cursor"),
                )
            )
        return PublicHttpRequest(
            method="GET",
            url=f"{POLYMARKET_GAMMA_PUBLIC_URL}/markets/keyset",
            query=tuple(query),
        )

    def event_census_request(self, *, limit: int, after_cursor: str | None = None) -> PublicHttpRequest:
        if limit <= 0 or limit > 500:
            raise ValueError("Polymarket event census limit must be within 1..500")
        query = [("closed", "false"), ("limit", str(limit))]
        if after_cursor is not None:
            query.append(
                (
                    "after_cursor",
                    _opaque_query_value(after_cursor, label="Polymarket event cursor"),
                )
            )
        return PublicHttpRequest(
            method="GET",
            url=f"{POLYMARKET_GAMMA_PUBLIC_URL}/events/keyset",
            query=tuple(query),
        )

    def market_metadata_by_token_request(self, token_id: str) -> PublicHttpRequest:
        return PublicHttpRequest(
            method="GET",
            url=f"{POLYMARKET_GAMMA_PUBLIC_URL}/markets/keyset",
            query=(
                (
                    "clob_token_ids",
                    _path_component(token_id, label="Polymarket token id"),
                ),
                ("limit", "1"),
            ),
        )

    def market_metadata_by_condition_request(self, condition_id: str) -> PublicHttpRequest:
        return PublicHttpRequest(
            method="GET",
            url=f"{POLYMARKET_GAMMA_PUBLIC_URL}/markets/keyset",
            query=(
                (
                    "condition_ids",
                    _path_component(condition_id, label="Polymarket condition id"),
                ),
                ("limit", "1"),
            ),
        )

    def market_metadata_request(self, market_id: str) -> PublicHttpRequest:
        return PublicHttpRequest(
            method="GET",
            url=(
                f"{POLYMARKET_GAMMA_PUBLIC_URL}/markets/"
                f"{_path_component(market_id, label='Polymarket market id')}"
            ),
        )

    def event_metadata_request(self, event_id: str) -> PublicHttpRequest:
        return PublicHttpRequest(
            method="GET",
            url=(
                f"{POLYMARKET_GAMMA_PUBLIC_URL}/events/"
                f"{_path_component(event_id, label='Polymarket event id')}"
            ),
        )

    def market_by_token_request(self, token_id: str) -> PublicHttpRequest:
        return PublicHttpRequest(
            method="GET",
            url=(
                f"{POLYMARKET_CLOB_PUBLIC_URL}/markets-by-token/"
                f"{_path_component(token_id, label='Polymarket token id')}"
            ),
        )

    def clob_market_request(self, condition_id: str) -> PublicHttpRequest:
        return PublicHttpRequest(
            method="GET",
            url=(
                f"{POLYMARKET_CLOB_PUBLIC_URL}/clob-markets/"
                f"{_path_component(condition_id, label='Polymarket condition id')}"
            ),
        )

    def public_trade_request(
        self,
        condition_ids: Sequence[str],
        *,
        limit: int = 100,
        offset: int = 0,
        start: int | None = None,
        end: int | None = None,
        taker_only: bool = True,
    ) -> PublicHttpRequest:
        if not condition_ids or limit <= 0 or limit > 10_000 or offset < 0 or offset > 10_000:
            raise ValueError("Polymarket public trades need markets and a limit within 1..10000")
        if (start is None) != (end is None) or (
            start is not None and (start < 0 or end is None or end <= start)
        ):
            raise ValueError("Polymarket trade time window must be a positive start/end pair")
        markets = ",".join(_path_component(item, label="Polymarket condition id") for item in condition_ids)
        query = [
            ("limit", str(limit)),
            ("market", markets),
        ]
        if offset:
            query.append(("offset", str(offset)))
        if not taker_only:
            query.append(("takerOnly", "false"))
        if start is not None and end is not None:
            query.extend((("start", str(start)), ("end", str(end))))
        return PublicHttpRequest(
            method="GET",
            url=f"{POLYMARKET_DATA_PUBLIC_URL}/trades",
            query=tuple(query),
        )

    def token_parameter_requests(
        self, token_ids: Sequence[str], *, feeds: Sequence[str]
    ) -> tuple[tuple[str, PublicHttpRequest], ...]:
        selected = set(feeds)
        allowed = {"order_book", "last_trade_price", "tick_size", "fees"}
        unknown = selected - allowed
        if unknown:
            raise ValueError(f"unsupported Polymarket token parameter feeds: {sorted(unknown)}")
        paths = {
            "fees": "fee-rate",
            "last_trade_price": "last-trade-price",
            "order_book": "book",
            "tick_size": "tick-size",
        }
        return tuple(
            (
                feed,
                PublicHttpRequest(
                    method="GET",
                    url=f"{POLYMARKET_CLOB_PUBLIC_URL}/{paths[feed]}",
                    query=(("token_id", token_id),),
                ),
            )
            for token_id in token_ids
            for feed in sorted(selected)
        )

    def order_book_requests(self, token_ids: Sequence[str]) -> tuple[PublicHttpRequest, ...]:
        return tuple(
            request for _, request in self.token_parameter_requests(token_ids, feeds=("order_book",))
        )

    def websocket_subscription(self, token_ids: Sequence[str]) -> PublicWebsocketSubscription:
        if not token_ids:
            raise ValueError("Polymarket subscription requires explicit token ids")
        if any(type(item) is not str or not item for item in token_ids):
            raise ValueError("Polymarket subscription token ids must be non-empty text")
        return PublicWebsocketSubscription(
            POLYMARKET_PUBLIC_WEBSOCKET_URL,
            {
                "assets_ids": list(token_ids),
                "custom_feature_enabled": True,
                "initial_dump": True,
                "type": "market",
            },
        )

    def envelope_from_websocket(
        self,
        raw_payload: bytes,
        *,
        factory: SessionEnvelopeFactory,
        receive_timestamp_utc_ns: int,
        receive_monotonic_ns: int,
        provenance: CaptureProvenance | None = None,
    ) -> PublicDataEnvelope:
        decoded = _json(raw_payload)
        if isinstance(decoded, list):
            if not decoded or any(not isinstance(item, Mapping) for item in decoded):
                raise ValueError("Polymarket WebSocket batch records must be objects")
            return factory.make(
                feed_type="market_batch",
                instrument_id=canonical_polymarket_instrument("GLOBAL"),
                market_id="PM:GLOBAL",
                source_timestamp_ns=None,
                receive_timestamp_utc_ns=receive_timestamp_utc_ns,
                receive_monotonic_ns=receive_monotonic_ns,
                raw_payload=raw_payload,
                source_sequence=None,
                source_event_id=hashlib.sha256(raw_payload).hexdigest(),
                provenance=provenance,
            )
        payload = _mapping(decoded, label="Polymarket WebSocket payload")
        event_type = str(payload.get("event_type") or "unknown")
        feed_map = {
            "book": "order_book",
            "price_change": "price_change",
            "last_trade_price": "last_trade_price",
            "best_bid_ask": "best_bid_ask",
            "tick_size_change": "tick_size_change",
            "new_market": "market_lifecycle",
            "market_resolved": "market_lifecycle",
        }
        feed_type = feed_map.get(event_type, f"unknown_{event_type}")
        asset_id = str(payload.get("asset_id") or "GLOBAL")
        market = str(payload.get("market") or "UNKNOWN")
        source_event_id: str | None = None
        for key in ("transaction_hash", "hash"):
            if payload.get(key):
                source_event_id = str(payload[key])
                break
        if source_event_id is None and event_type == "price_change":
            changes = payload.get("price_changes")
            if (
                isinstance(changes, list)
                and len(changes) == 1
                and isinstance(changes[0], Mapping)
                and changes[0].get("hash")
            ):
                source_event_id = str(changes[0]["hash"])
        return factory.make(
            feed_type=feed_type,
            instrument_id=canonical_polymarket_instrument(asset_id),
            market_id=f"PM:{market}",
            source_timestamp_ns=(
                _milliseconds_to_ns(payload.get("timestamp"))
                if event_type in {"book", "last_trade_price", "price_change"}
                else None
            ),
            receive_timestamp_utc_ns=receive_timestamp_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            raw_payload=raw_payload,
            source_sequence=None,
            source_event_id=source_event_id,
            provenance=provenance,
        )

    def envelope_from_http(
        self,
        raw_payload: bytes,
        *,
        feed_type: str,
        token_id: str | None,
        market_id: str,
        factory: SessionEnvelopeFactory,
        receive_timestamp_utc_ns: int,
        receive_monotonic_ns: int,
        provenance: CaptureProvenance | None = None,
    ) -> PublicDataEnvelope:
        if feed_type not in {
            "events",
            "fees",
            "last_trade_price",
            "metadata",
            "order_book",
            "public_trades",
            "tick_size",
        }:
            raise ValueError("unsupported Polymarket public HTTP feed")
        decoded = _json(raw_payload)
        source_timestamp_ns = None
        source_cursor = None
        source_event_id = None
        if isinstance(decoded, Mapping):
            if decoded.get("next_cursor"):
                source_cursor = str(decoded["next_cursor"])
            if feed_type != "order_book" and decoded.get("timestamp") is not None:
                source_timestamp_ns = _milliseconds_to_ns(decoded.get("timestamp"))
            else:
                for field in ("updatedAt", "createdAt", "endDate", "startDate"):
                    if decoded.get(field):
                        source_timestamp_ns = _iso_to_ns(decoded[field])
                        break
            if decoded.get("hash"):
                source_event_id = str(decoded["hash"])
        return factory.make(
            feed_type=feed_type,
            instrument_id=canonical_polymarket_instrument(token_id or "GLOBAL"),
            market_id=f"PM:{market_id}",
            source_timestamp_ns=source_timestamp_ns,
            receive_timestamp_utc_ns=receive_timestamp_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            raw_payload=raw_payload,
            source_sequence=None,
            source_cursor=source_cursor,
            source_event_id=source_event_id,
            provenance=provenance,
        )


class KalshiPublicAdapter:
    venue = Venue.KALSHI
    metadata_version = KALSHI_METADATA_VERSION
    websocket_public_without_credentials = False
    websocket_limitation = "PUBLIC_CHANNELS_REQUIRE_AUTHENTICATED_WEBSOCKET_HANDSHAKE"
    supported_feeds = frozenset(
        {
            "series",
            "events",
            "markets",
            "order_book",
            "trades",
            "block_trades",
            "incentives",
            "fee_changes",
            "event_fee_changes",
            "event_metadata",
            "exchange_status",
            "exchange_schedule",
            "historical_cutoff",
            "historical_markets",
            "historical_trades",
        }
    )

    def websocket_subscriptions(self, *_: object, **__: object) -> tuple[PublicWebsocketSubscription, ...]:
        return ()

    def market_census_request(self, *, limit: int, cursor: str | None = None) -> PublicHttpRequest:
        if limit <= 0 or limit > 1000:
            raise ValueError("Kalshi census limit must be within 1..1000")
        query = [("limit", str(limit)), ("status", "open")]
        if cursor is not None:
            query.append(("cursor", _opaque_query_value(cursor, label="Kalshi market cursor")))
        return PublicHttpRequest(
            method="GET",
            url=f"{KALSHI_PUBLIC_HTTP_URL}/markets",
            query=tuple(query),
        )

    def event_census_request(self, *, limit: int, cursor: str | None = None) -> PublicHttpRequest:
        if limit <= 0 or limit > 200:
            raise ValueError("Kalshi event census limit must be within 1..200")
        query = [("limit", str(limit)), ("status", "open")]
        if cursor is not None:
            query.append(("cursor", _opaque_query_value(cursor, label="Kalshi event cursor")))
        return PublicHttpRequest(
            method="GET",
            url=f"{KALSHI_PUBLIC_HTTP_URL}/events",
            query=tuple(query),
        )

    def series_census_request(self) -> PublicHttpRequest:
        return PublicHttpRequest(method="GET", url=f"{KALSHI_PUBLIC_HTTP_URL}/series")

    def trade_request(
        self,
        ticker: str,
        *,
        block_trade: bool,
        limit: int = 1000,
        cursor: str | None = None,
    ) -> PublicHttpRequest:
        if limit <= 0 or limit > 1000:
            raise ValueError("Kalshi trade limit must be within 1..1000")
        query = [
            ("is_block_trade", "true" if block_trade else "false"),
            ("limit", str(limit)),
            ("ticker", _path_component(ticker, label="Kalshi market ticker")),
        ]
        if cursor is not None:
            query.append(("cursor", _opaque_query_value(cursor, label="Kalshi trade cursor")))
        return PublicHttpRequest(
            method="GET",
            url=f"{KALSHI_PUBLIC_HTTP_URL}/markets/trades",
            query=tuple(query),
        )

    def incentive_request(
        self, *, limit: int = 1000, cursor: str | None = None
    ) -> PublicHttpRequest:
        if limit <= 0 or limit > 1000:
            raise ValueError("Kalshi incentive limit must be within 1..1000")
        query = [("limit", str(limit))]
        if cursor is not None:
            query.append(
                ("cursor", _opaque_query_value(cursor, label="Kalshi incentive cursor"))
            )
        return PublicHttpRequest(
            method="GET",
            url=f"{KALSHI_PUBLIC_HTTP_URL}/incentive_programs",
            query=tuple(query),
        )

    def event_fee_changes_request(
        self,
        event_ticker: str,
        *,
        limit: int = 1000,
        cursor: str | None = None,
    ) -> PublicHttpRequest:
        if limit <= 0 or limit > 1000:
            raise ValueError("Kalshi event fee-change limit must be within 1..1000")
        query = [
            (
                "event_ticker",
                _path_component(event_ticker, label="Kalshi event ticker"),
            ),
            ("limit", str(limit)),
        ]
        if cursor is not None:
            query.append(("cursor", _opaque_query_value(cursor, label="Kalshi fee cursor")))
        return PublicHttpRequest(
            method="GET",
            url=f"{KALSHI_PUBLIC_HTTP_URL}/events/fee_changes",
            query=tuple(query),
        )

    def requests_for_market(
        self,
        *,
        ticker: str,
        event_ticker: str | None,
        series_ticker: str | None,
        feeds: Sequence[str],
    ) -> tuple[PublicHttpRequest, ...]:
        ticker = _path_component(ticker, label="Kalshi market ticker")
        if event_ticker is not None:
            event_ticker = _path_component(event_ticker, label="Kalshi event ticker")
        if series_ticker is not None:
            series_ticker = _path_component(series_ticker, label="Kalshi series ticker")
        selected = set(feeds)
        unknown = selected - self.supported_feeds
        if unknown:
            raise ValueError(f"unsupported Kalshi public feeds: {sorted(unknown)}")
        requests: list[PublicHttpRequest] = []
        if "markets" in selected:
            requests.append(PublicHttpRequest(method="GET", url=f"{KALSHI_PUBLIC_HTTP_URL}/markets/{ticker}"))
        if "order_book" in selected:
            requests.append(
                PublicHttpRequest(method="GET", url=f"{KALSHI_PUBLIC_HTTP_URL}/markets/{ticker}/orderbook")
            )
        if "trades" in selected:
            requests.append(
                PublicHttpRequest(
                    method="GET",
                    url=f"{KALSHI_PUBLIC_HTTP_URL}/markets/trades",
                    query=(
                        ("is_block_trade", "false"),
                        ("limit", "1000"),
                        ("ticker", ticker),
                    ),
                )
            )
        if "block_trades" in selected:
            requests.append(
                PublicHttpRequest(
                    method="GET",
                    url=f"{KALSHI_PUBLIC_HTTP_URL}/markets/trades",
                    query=(
                        ("is_block_trade", "true"),
                        ("limit", "1000"),
                        ("ticker", ticker),
                    ),
                )
            )
        if "events" in selected and event_ticker:
            requests.append(
                PublicHttpRequest(method="GET", url=f"{KALSHI_PUBLIC_HTTP_URL}/events/{event_ticker}")
            )
        if "event_metadata" in selected and event_ticker:
            requests.append(
                PublicHttpRequest(
                    method="GET",
                    url=f"{KALSHI_PUBLIC_HTTP_URL}/events/{event_ticker}/metadata",
                )
            )
        if "series" in selected and series_ticker:
            requests.append(
                PublicHttpRequest(method="GET", url=f"{KALSHI_PUBLIC_HTTP_URL}/series/{series_ticker}")
            )
        if "incentives" in selected:
            requests.append(
                PublicHttpRequest(
                    method="GET",
                    url=f"{KALSHI_PUBLIC_HTTP_URL}/incentive_programs",
                    query=(("limit", "1000"),),
                )
            )
        if "fee_changes" in selected and series_ticker:
            requests.append(
                PublicHttpRequest(
                    method="GET",
                    url=f"{KALSHI_PUBLIC_HTTP_URL}/series/fee_changes",
                    query=(("series_ticker", series_ticker), ("show_historical", "true")),
                )
            )
        if "event_fee_changes" in selected and event_ticker:
            requests.append(
                PublicHttpRequest(
                    method="GET",
                    url=f"{KALSHI_PUBLIC_HTTP_URL}/events/fee_changes",
                    query=(
                        ("event_ticker", event_ticker),
                        ("limit", "1000"),
                    ),
                )
            )
        if "exchange_status" in selected:
            requests.append(PublicHttpRequest(method="GET", url=f"{KALSHI_PUBLIC_HTTP_URL}/exchange/status"))
        if "exchange_schedule" in selected:
            requests.append(
                PublicHttpRequest(method="GET", url=f"{KALSHI_PUBLIC_HTTP_URL}/exchange/schedule")
            )
        return tuple(requests)

    def historical_cutoff_request(self) -> PublicHttpRequest:
        return PublicHttpRequest(method="GET", url=f"{KALSHI_PUBLIC_HTTP_URL}/historical/cutoff")

    def historical_market_census_request(self, *, limit: int, cursor: str | None = None) -> PublicHttpRequest:
        if limit <= 0 or limit > 1000:
            raise ValueError("Kalshi historical census limit must be within 1..1000")
        query = [("limit", str(limit))]
        if cursor is not None:
            query.append(("cursor", _opaque_query_value(cursor, label="Kalshi historical cursor")))
        return PublicHttpRequest(
            method="GET",
            url=f"{KALSHI_PUBLIC_HTTP_URL}/historical/markets",
            query=tuple(query),
        )

    def historical_market_request(self, ticker: str) -> PublicHttpRequest:
        return PublicHttpRequest(
            method="GET",
            url=(
                f"{KALSHI_PUBLIC_HTTP_URL}/historical/markets/"
                f"{_path_component(ticker, label='Kalshi historical ticker')}"
            ),
        )

    def historical_trade_request(
        self, ticker: str, *, limit: int = 1000, cursor: str | None = None
    ) -> PublicHttpRequest:
        if limit <= 0 or limit > 1000:
            raise ValueError("Kalshi historical trade limit must be within 1..1000")
        query = [
            ("limit", str(limit)),
            ("ticker", _path_component(ticker, label="Kalshi market ticker")),
        ]
        if cursor is not None:
            query.append(("cursor", _opaque_query_value(cursor, label="Kalshi historical trade cursor")))
        return PublicHttpRequest(
            method="GET",
            url=f"{KALSHI_PUBLIC_HTTP_URL}/historical/trades",
            query=tuple(query),
        )

    def envelope_from_http(
        self,
        raw_payload: bytes,
        *,
        feed_type: str,
        ticker: str | None,
        factory: SessionEnvelopeFactory,
        receive_timestamp_utc_ns: int,
        receive_monotonic_ns: int,
        provenance: CaptureProvenance | None = None,
    ) -> PublicDataEnvelope:
        if feed_type not in self.supported_feeds:
            raise ValueError("unsupported Kalshi public feed")
        decoded = _mapping(_json(raw_payload), label=f"Kalshi {feed_type} payload")
        source_cursor = _kalshi_cursor(decoded, feed_type=feed_type)
        source_timestamp_ns: int | None = None
        source_event_id: str | None = None
        detected_ticker = ticker
        if feed_type == "series":
            records = _kalshi_mapping_records(
                decoded,
                feed_type=feed_type,
                singular_key="series",
                plural_key="series",
            )
            for record in records:
                _kalshi_text(record.get("ticker"), label="Kalshi series ticker")
            if isinstance(decoded.get("series"), Mapping):
                detected_ticker = _kalshi_text(
                    records[0].get("ticker"), label="Kalshi series ticker"
                )
                source_timestamp_ns = _kalshi_optional_timestamp(records[0], feed_type=feed_type)
        elif feed_type == "events":
            records = _kalshi_mapping_records(
                decoded,
                feed_type=feed_type,
                singular_key="event",
                plural_key="events",
            )
            for record in records:
                _kalshi_text(record.get("event_ticker"), label="Kalshi event ticker")
                if record.get("series_ticker") is not None:
                    _kalshi_text(record["series_ticker"], label="Kalshi series ticker")
            if "event" in decoded:
                detected_ticker = _kalshi_text(
                    records[0].get("event_ticker"), label="Kalshi event ticker"
                )
                source_timestamp_ns = _kalshi_optional_timestamp(records[0], feed_type=feed_type)
        elif feed_type in {"markets", "historical_markets"}:
            records = _kalshi_mapping_records(
                decoded,
                feed_type=feed_type,
                singular_key="market",
                plural_key="markets",
            )
            tickers = tuple(
                _kalshi_validate_market_record(record, feed_type=feed_type)
                for record in records
            )
            if "market" in decoded:
                detected_ticker = tickers[0]
                source_timestamp_ns = _kalshi_optional_timestamp(records[0], feed_type=feed_type)
        elif feed_type == "order_book":
            _kalshi_validate_orderbook(decoded)
        elif feed_type in {"trades", "block_trades", "historical_trades"}:
            records = tuple(
                _mapping(item, label=f"Kalshi {feed_type} trade")
                for item in _sequence(decoded.get("trades"), label=f"Kalshi {feed_type} trades")
            )
            expected_block_trade = {"trades": False, "block_trades": True}.get(feed_type)
            identities = tuple(
                _kalshi_validate_trade_record(
                    record,
                    expected_block_trade=expected_block_trade,
                )
                for record in records
            )
            if len(records) == 1:
                source_event_id, detected_ticker = identities[0]
                source_timestamp_ns = _kalshi_optional_timestamp(records[0], feed_type=feed_type)
        elif feed_type == "incentives":
            records = tuple(
                _mapping(item, label="Kalshi incentive record")
                for item in _sequence(
                    decoded.get("incentive_programs"),
                    label="Kalshi incentive_programs",
                )
            )
            for record in records:
                if record.get("id") is not None:
                    _kalshi_text(record["id"], label="Kalshi incentive id")
                if record.get("market_ticker") is not None:
                    _kalshi_text(record["market_ticker"], label="Kalshi incentive market ticker")
                for field in ("start_date", "end_date"):
                    if record.get(field) is not None:
                        _kalshi_rfc3339_to_ns(record[field], feed_type=feed_type, field=field)
        elif feed_type == "fee_changes":
            records = tuple(
                _mapping(item, label="Kalshi series fee-change record")
                for item in _sequence(
                    decoded.get("series_fee_change_arr"),
                    label="Kalshi series_fee_change_arr",
                )
            )
            for record in records:
                if record.get("id") is not None:
                    _kalshi_text(record["id"], label="Kalshi series fee-change id")
                _kalshi_text(record.get("series_ticker"), label="Kalshi series ticker")
                _kalshi_rfc3339_to_ns(
                    record.get("scheduled_ts"), feed_type=feed_type, field="scheduled_ts"
                )
        elif feed_type == "event_fee_changes":
            records = tuple(
                _mapping(item, label="Kalshi event fee-change record")
                for item in _sequence(
                    decoded.get("event_fee_changes"),
                    label="Kalshi event_fee_changes",
                )
            )
            for record in records:
                if record.get("id") is not None:
                    _kalshi_text(record["id"], label="Kalshi event fee-change id")
                _kalshi_text(record.get("event_ticker"), label="Kalshi event ticker")
                multiplier = record.get("fee_multiplier_override")
                fee_type_override = record.get("fee_type_override")
                if (multiplier is None) != (fee_type_override is None):
                    raise ValueError("Kalshi event fee overrides must both be null or both be present")
                _kalshi_rfc3339_to_ns(
                    record.get("scheduled_ts"), feed_type=feed_type, field="scheduled_ts"
                )
        elif feed_type == "event_metadata":
            for field in ("market_details", "settlement_sources"):
                metadata_records = _sequence(
                    decoded.get(field), label=f"Kalshi event metadata {field}"
                )
                if any(not isinstance(item, Mapping) for item in metadata_records):
                    raise ValueError(f"Kalshi event metadata {field} records must be objects")
        elif feed_type == "exchange_status":
            for field in ("exchange_active", "trading_active"):
                if type(decoded.get(field)) is not bool:
                    raise ValueError(f"Kalshi exchange status {field} must be a boolean")
            resume = decoded.get("exchange_estimated_resume_time")
            if resume is not None:
                _kalshi_rfc3339_to_ns(resume, feed_type=feed_type, field="resume_time")
        elif feed_type == "exchange_schedule":
            _mapping(decoded.get("schedule"), label="Kalshi exchange schedule")
        elif feed_type == "historical_cutoff":
            for field in ("market_settled_ts", "orders_updated_ts", "trades_created_ts"):
                _kalshi_rfc3339_to_ns(decoded.get(field), feed_type=feed_type, field=field)
        canonical_ticker = detected_ticker or "GLOBAL"
        if feed_type in {"events", "event_metadata", "event_fee_changes"}:
            canonical_id = canonical_kalshi_event(canonical_ticker)
        elif feed_type in {"series", "fee_changes"}:
            canonical_id = canonical_kalshi_series(canonical_ticker)
        else:
            canonical_id = canonical_kalshi_market(canonical_ticker)
        return factory.make(
            feed_type=feed_type,
            instrument_id=None,
            market_id=canonical_id,
            source_timestamp_ns=source_timestamp_ns,
            receive_timestamp_utc_ns=receive_timestamp_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            raw_payload=raw_payload,
            source_sequence=None,
            source_cursor=source_cursor,
            source_event_id=source_event_id,
            provenance=provenance,
        )


def all_public_route_specs() -> tuple[PublicHttpRequest | PublicWebsocketSubscription, ...]:
    from .lighter import LighterPublicAdapter

    hyperliquid = HyperliquidPublicAdapter()
    lighter = LighterPublicAdapter()
    polymarket = PolymarketPublicAdapter()
    kalshi = KalshiPublicAdapter()
    return (
        *hyperliquid.public_http_requests(feeds=("metadata", "l2_book"), instruments=("BTC",)),
        *hyperliquid.websocket_subscriptions(
            feeds=("bbo", "l2_book", "trades", "all_mids", "active_asset_context"),
            instruments=("BTC",),
        ),
        *lighter.public_http_requests(feeds=("metadata",), market_indices=(0,)),
        *lighter.websocket_subscriptions(
            feeds=("order_book", "ticker", "market_stats", "trades"),
            market_indices=(0,),
        ),
        *lighter.websocket_subscriptions(
            feeds=("order_book", "ticker", "market_stats", "trades"),
            market_indices=(0,),
            websocket_url="wss://mainnet.zklighter.elliot.ai/stream?readonly=true",
        ),
        polymarket.market_census_request(limit=1),
        polymarket.event_census_request(limit=1),
        polymarket.market_by_token_request("fixture-token"),
        polymarket.market_metadata_by_token_request("fixture-token"),
        polymarket.market_metadata_by_condition_request("fixture-condition"),
        polymarket.clob_market_request("fixture-condition"),
        polymarket.order_book_requests(("fixture-token",))[0],
        polymarket.public_trade_request(("fixture-condition",), limit=1),
        *(
            request
            for _, request in polymarket.token_parameter_requests(
                ("fixture-token",), feeds=("last_trade_price", "tick_size", "fees")
            )
        ),
        polymarket.websocket_subscription(("fixture-token",)),
        kalshi.market_census_request(limit=1),
        *kalshi.requests_for_market(
            ticker="FIXTURE-MARKET",
            event_ticker="FIXTURE-EVENT",
            series_ticker="FIXTURE-SERIES",
            feeds=("series", "events", "markets", "order_book", "trades", "incentives", "fee_changes"),
        ),
    )


__all__ = [
    "HYPERLIQUID_METADATA_VERSION",
    "HYPERLIQUID_PUBLIC_HTTP_URL",
    "HYPERLIQUID_PUBLIC_WEBSOCKET_URL",
    "KALSHI_METADATA_VERSION",
    "KALSHI_METADATA_VERSION_V1",
    "KALSHI_PUBLIC_HTTP_URL",
    "KALSHI_SUPPORTED_METADATA_VERSIONS",
    "POLYMARKET_CLOB_PUBLIC_URL",
    "POLYMARKET_DATA_PUBLIC_URL",
    "POLYMARKET_GAMMA_PUBLIC_URL",
    "POLYMARKET_METADATA_VERSION",
    "POLYMARKET_PUBLIC_WEBSOCKET_URL",
    "HyperliquidPublicAdapter",
    "KalshiPublicAdapter",
    "PolymarketPublicAdapter",
    "PublicHttpRequest",
    "PublicWebsocketSubscription",
    "all_public_route_specs",
    "canonical_hyperliquid_instrument",
    "canonical_kalshi_event",
    "canonical_kalshi_market",
    "canonical_kalshi_series",
    "canonical_polymarket_instrument",
]
