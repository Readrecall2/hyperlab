from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, cast
from urllib.parse import unquote, urlsplit

from .canonical import CanonicalValue, canonical_json_bytes
from .envelope import CaptureProvenance, PublicDataEnvelope, SessionEnvelopeFactory, Venue

HYPERLIQUID_PUBLIC_HTTP_URL: Final = "https://api.hyperliquid.xyz/info"
HYPERLIQUID_PUBLIC_WEBSOCKET_URL: Final = "wss://api.hyperliquid.xyz/ws"
POLYMARKET_GAMMA_PUBLIC_URL: Final = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_PUBLIC_URL: Final = "https://clob.polymarket.com"
POLYMARKET_DATA_PUBLIC_URL: Final = "https://data-api.polymarket.com"
POLYMARKET_PUBLIC_WEBSOCKET_URL: Final = (
    "wss://ws-subscriptions-clob.polymarket.com/ws/market"
)
KALSHI_PUBLIC_HTTP_URL: Final = "https://external-api.kalshi.com/trade-api/v2"

HYPERLIQUID_METADATA_VERSION: Final = "hyperliquid-official-public-api-2026-08-26"
POLYMARKET_METADATA_VERSION: Final = "polymarket-official-public-api-2026-08-26"
KALSHI_METADATA_VERSION: Final = "kalshi-official-public-rest-2026-08-26"

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
        return json.loads(raw_payload.decode("utf-8"), parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("public venue payload is not strict UTF-8 JSON") from error


def _path_component(value: str, *, label: str) -> str:
    if _PATH_COMPONENT.fullmatch(value) is None:
        raise ValueError(f"{label} is not a safe public path component")
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
    text = str(value).replace("Z", "+00:00")
    timestamp = datetime.fromisoformat(text)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("source timestamp must be timezone-aware")
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = timestamp.astimezone(UTC) - epoch
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


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
        if segments & _PRIVATE_PATH_SEGMENTS:
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


class HyperliquidPublicAdapter:
    venue = Venue.HYPERLIQUID
    metadata_version = HYPERLIQUID_METADATA_VERSION
    supported_feeds = frozenset(
        {"bbo", "l2_book", "trades", "all_mids", "active_asset_context", "metadata"}
    )
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
                "HL:GLOBAL:public"
                if instrument is None
                else canonical_hyperliquid_instrument(instrument)
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
            "tick_size_change",
            "market_lifecycle",
        }
    )

    def market_census_request(self, *, limit: int) -> PublicHttpRequest:
        if limit <= 0 or limit > 100:
            raise ValueError("Polymarket census limit must be within 1..100")
        return PublicHttpRequest(
            method="GET",
            url=f"{POLYMARKET_GAMMA_PUBLIC_URL}/markets",
            query=(("active", "true"), ("closed", "false"), ("limit", str(limit))),
        )

    def event_census_request(self, *, limit: int) -> PublicHttpRequest:
        if limit <= 0 or limit > 100:
            raise ValueError("Polymarket event census limit must be within 1..100")
        return PublicHttpRequest(
            method="GET",
            url=f"{POLYMARKET_GAMMA_PUBLIC_URL}/events",
            query=(("active", "true"), ("closed", "false"), ("limit", str(limit))),
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

    def public_trade_request(
        self, condition_ids: Sequence[str], *, limit: int = 100
    ) -> PublicHttpRequest:
        if not condition_ids or limit <= 0 or limit > 10_000:
            raise ValueError("Polymarket public trades need markets and a limit within 1..10000")
        markets = ",".join(
            _path_component(item, label="Polymarket condition id")
            for item in condition_ids
        )
        return PublicHttpRequest(
            method="GET",
            url=f"{POLYMARKET_DATA_PUBLIC_URL}/trades",
            query=(("market", markets), ("limit", str(limit))),
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
            request
            for _, request in self.token_parameter_requests(
                token_ids, feeds=("order_book",)
            )
        )

    def websocket_subscription(
        self, token_ids: Sequence[str]
    ) -> PublicWebsocketSubscription:
        if not token_ids:
            raise ValueError("Polymarket subscription requires explicit token ids")
        if any(type(item) is not str or not item for item in token_ids):
            raise ValueError("Polymarket subscription token ids must be non-empty text")
        return PublicWebsocketSubscription(
            POLYMARKET_PUBLIC_WEBSOCKET_URL,
            {
                "assets_ids": list(token_ids),
                "custom_feature_enabled": True,
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
            return factory.make(
                feed_type="market_batch",
                instrument_id="PM:GLOBAL",
                market_id="PM:GLOBAL",
                source_timestamp_ns=None,
                receive_timestamp_utc_ns=receive_timestamp_utc_ns,
                receive_monotonic_ns=receive_monotonic_ns,
                raw_payload=raw_payload,
                source_sequence=None,
                provenance=provenance,
            )
        payload = _mapping(decoded, label="Polymarket WebSocket payload")
        event_type = str(payload.get("event_type") or "unknown")
        feed_map = {
            "book": "order_book",
            "price_change": "price_change",
            "last_trade_price": "last_trade_price",
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
            source_timestamp_ns=_milliseconds_to_ns(payload.get("timestamp")),
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
        source_event_id = None
        if isinstance(decoded, Mapping):
            if decoded.get("timestamp") is not None:
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
            source_event_id=source_event_id,
            provenance=provenance,
        )


class KalshiPublicAdapter:
    venue = Venue.KALSHI
    metadata_version = KALSHI_METADATA_VERSION
    websocket_public_without_credentials = False
    websocket_limitation = "PUBLIC_CHANNELS_REQUIRE_AUTHENTICATED_WEBSOCKET_HANDSHAKE"
    supported_feeds = frozenset(
        {"series", "events", "markets", "order_book", "trades", "incentives", "fee_changes"}
    )

    def websocket_subscriptions(self, *_: object, **__: object) -> tuple[PublicWebsocketSubscription, ...]:
        return ()

    def market_census_request(self, *, limit: int) -> PublicHttpRequest:
        if limit <= 0 or limit > 1000:
            raise ValueError("Kalshi census limit must be within 1..1000")
        return PublicHttpRequest(
            method="GET",
            url=f"{KALSHI_PUBLIC_HTTP_URL}/markets",
            query=(("limit", str(limit)), ("status", "open")),
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
            requests.append(
                PublicHttpRequest(method="GET", url=f"{KALSHI_PUBLIC_HTTP_URL}/markets/{ticker}")
            )
        if "order_book" in selected:
            requests.append(
                PublicHttpRequest(
                    method="GET", url=f"{KALSHI_PUBLIC_HTTP_URL}/markets/{ticker}/orderbook"
                )
            )
        if "trades" in selected:
            requests.append(
                PublicHttpRequest(
                    method="GET",
                    url=f"{KALSHI_PUBLIC_HTTP_URL}/markets/trades",
                    query=(("ticker", ticker), ("limit", "1000")),
                )
            )
        if "events" in selected and event_ticker:
            requests.append(
                PublicHttpRequest(
                    method="GET", url=f"{KALSHI_PUBLIC_HTTP_URL}/events/{event_ticker}"
                )
            )
        if "series" in selected and series_ticker:
            requests.append(
                PublicHttpRequest(
                    method="GET", url=f"{KALSHI_PUBLIC_HTTP_URL}/series/{series_ticker}"
                )
            )
        if "incentives" in selected:
            requests.append(
                PublicHttpRequest(
                    method="GET",
                    url=f"{KALSHI_PUBLIC_HTTP_URL}/incentive_programs",
                    query=(("market_ticker", ticker),),
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
        return tuple(requests)

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
        decoded = _json(raw_payload)
        source_cursor: str | None = None
        source_timestamp_ns: int | None = None
        source_event_id: str | None = None
        detected_ticker = ticker
        if isinstance(decoded, Mapping):
            if decoded.get("cursor"):
                source_cursor = str(decoded["cursor"])
            record: Mapping[str, Any] | None = None
            singular_keys = {
                "series": "series",
                "events": "event",
                "markets": "market",
                "order_book": "orderbook_fp",
            }
            singular_key = singular_keys.get(feed_type)
            if singular_key and isinstance(decoded.get(singular_key), Mapping):
                record = cast(Mapping[str, Any], decoded[singular_key])
            if record is not None:
                for key in ("ticker", "market_ticker"):
                    if record.get(key):
                        detected_ticker = str(record[key])
                for key in ("updated_time", "last_updated_ts", "created_time"):
                    if record.get(key):
                        source_timestamp_ns = _iso_to_ns(record[key])
                        break
            if feed_type == "trades":
                trades = decoded.get("trades")
                if isinstance(trades, list) and len(trades) == 1 and isinstance(trades[0], Mapping):
                    trade = trades[0]
                    detected_ticker = str(trade.get("ticker") or detected_ticker or "GLOBAL")
                    source_timestamp_ns = _iso_to_ns(trade.get("created_time"))
                    if trade.get("trade_id"):
                        source_event_id = str(trade["trade_id"])
        canonical_ticker = detected_ticker or "GLOBAL"
        return factory.make(
            feed_type=feed_type,
            instrument_id=None,
            market_id=canonical_kalshi_market(canonical_ticker),
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
        polymarket.order_book_requests(("fixture-token",))[0],
        polymarket.public_trade_request(("fixture-condition",), limit=1),
        *(request for _, request in polymarket.token_parameter_requests(
            ("fixture-token",), feeds=("last_trade_price", "tick_size", "fees")
        )),
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
    "KALSHI_PUBLIC_HTTP_URL",
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
    "canonical_kalshi_market",
    "canonical_polymarket_instrument",
]
