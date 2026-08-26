from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Final

from .adapters import PublicHttpRequest, PublicWebsocketSubscription
from .canonical import CanonicalValue
from .envelope import CaptureProvenance, PublicDataEnvelope, SessionEnvelopeFactory, Venue

LIGHTER_PUBLIC_HTTP_URL: Final = "https://mainnet.zklighter.elliot.ai/api/v1"
LIGHTER_PUBLIC_WEBSOCKET_URL: Final = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_METADATA_VERSION: Final = "lighter-official-public-api-2026-08-26-v1"

LIGHTER_DOCUMENTARY_CONTRACT: dict[str, CanonicalValue] = {
    "account_types": {
        "plus": {
            "documented_maker_cancel_latency_ms": 200,
            "documented_maker_taker_fee_bps": "0.5",
            "documented_taker_latency_ms": 300,
        },
        "premium": {
            "documented_maker_cancel_latency_ms": 0,
            "tiers": [
                {"lit": 0, "maker_fee_percent": "0.0040", "taker_fee_percent": "0.0280", "taker_latency_ms": 200},
                {"lit": 1000, "maker_fee_percent": "0.0039", "taker_fee_percent": "0.0273", "taker_latency_ms": 195},
                {"lit": 3000, "maker_fee_percent": "0.0038", "taker_fee_percent": "0.0266", "taker_latency_ms": 190},
                {"lit": 10000, "maker_fee_percent": "0.0036", "taker_fee_percent": "0.0252", "taker_latency_ms": 180},
                {"lit": 30000, "maker_fee_percent": "0.0034", "taker_fee_percent": "0.0238", "taker_latency_ms": 170},
                {"lit": 100000, "maker_fee_percent": "0.0032", "taker_fee_percent": "0.0224", "taker_latency_ms": 160},
                {"lit": 300000, "maker_fee_percent": "0.0030", "taker_fee_percent": "0.0210", "taker_latency_ms": 150},
                {"lit": 500000, "maker_fee_percent": "0.0028", "taker_fee_percent": "0.0196", "taker_latency_ms": 140},
            ],
        },
        "standard": {
            "documented_maker_cancel_latency_ms": 200,
            "documented_maker_fee_percent": "0",
            "documented_taker_fee_percent": "0",
            "documented_taker_latency_ms": 300,
        },
    },
    "capabilities": {
        "order_book": {
            "documented_batch_interval_ms": 50,
            "continuity": "CURRENT_BEGIN_NONCE_EQUALS_PREVIOUS_NONCE",
            "offset_semantics": "API_SERVER_CURSOR_INCREASES_BUT_NEED_NOT_BE_CONTIGUOUS",
        },
        "public_rest": ["orderBooks", "orderBookDetails", "recentTrades"],
        "public_websocket": ["order_book", "ticker", "market_stats", "trade"],
    },
    "capture_date": "2026-08-26",
    "comparable_scenarios_ms": [100, 250, 500, 1000],
    "document_pages": [
        {
            "page_update_label_at_capture": "Updated 15 days ago",
            "url": "https://apidocs.lighter.xyz/docs/websocket-reference",
        },
        {
            "page_update_label_at_capture": "Updated 3 months ago",
            "url": "https://apidocs.lighter.xyz/reference/orderbooks",
        },
        {
            "page_update_label_at_capture": "Updated 3 months ago",
            "url": "https://apidocs.lighter.xyz/reference/orderbookdetails",
        },
        {
            "page_update_label_at_capture": "Updated 3 months ago",
            "url": "https://apidocs.lighter.xyz/reference/recenttrades",
        },
        {
            "page_update_label_at_capture": "Updated about 2 hours ago",
            "url": "https://apidocs.lighter.xyz/docs/account-types",
        },
        {
            "page_update_label_at_capture": "Updated about 1 hour ago",
            "url": "https://apidocs.lighter.xyz/docs/rate-limits",
        },
        {
            "page_update_label_at_capture": "current page retrieved 2026-08-26",
            "url": "https://docs.lighter.xyz/trading/trading-fees",
        },
    ],
    "forbidden_surfaces": [
        "API_KEY",
        "ACCOUNT_CHANNEL",
        "AUTH_TOKEN",
        "PRIVATE_ORDER_POSITION_CHANNEL",
        "SIGNER_OR_SIGNING_SDK",
        "SEND_TX_OR_SEND_TX_BATCH",
        "CREATE_CANCEL_MODIFY_ORDER",
        "PROXY_OR_READONLY_REGION_BYPASS",
    ],
    "metadata_version": LIGHTER_METADATA_VERSION,
    "rate_limits_documentary": {
        "standard_rest_requests_per_rolling_minute": 60,
        "websocket_connections_per_ip": 255,
        "websocket_max_client_messages_per_minute": 200,
        "websocket_max_inflight_messages": 50,
        "websocket_subscriptions_per_connection": 500,
    },
    "scenario_classification": "VERSIONED_HYPOTHETICAL_NOT_ACCOUNT_OBSERVATION",
    "schemas": {
        "market_stats_websocket_required": [
            "channel",
            "market_stats.market_id",
            "timestamp",
        ],
        "order_book_websocket_required": [
            "channel",
            "order_book.asks",
            "order_book.begin_nonce",
            "order_book.bids",
            "order_book.code",
            "order_book.last_updated_at",
            "order_book.nonce",
            "order_book.offset",
        ],
        "order_books_http_market_required": [
            "maker_fee",
            "market_id",
            "market_type",
            "min_base_amount",
            "min_quote_amount",
            "status",
            "supported_price_decimals",
            "supported_quote_decimals",
            "supported_size_decimals",
            "symbol",
            "taker_fee",
        ],
        "ticker_websocket_required": [
            "channel",
            "last_updated_at",
            "nonce",
            "ticker",
        ],
        "trade_websocket_required": [
            "channel",
            "liquidation_trades",
            "nonce",
            "trades",
        ],
    },
    "scope": "DOCUMENTARY_NOT_OBSERVED_ACCOUNT_ACCESS",
    "schema_version": 1,
    "timestamp_interpretation": {
        "last_updated_at": "DOCUMENTED_EXAMPLES_INTERPRETED_AS_UNIX_MICROSECONDS",
        "timestamp": "DOCUMENTED_EXAMPLES_INTERPRETED_AS_UNIX_MILLISECONDS",
    },
}

_CHANNEL = re.compile(r"^(order_book|ticker|market_stats|trade):(\d+)$")
_DECIMAL = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")


def _mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _sequence(value: object, *, label: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{label} must be an array")
    return value


def _strict_json(raw_payload: bytes) -> object:
    try:
        return json.loads(
            raw_payload.decode("utf-8"),
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Lighter public payload is not strict UTF-8 JSON") from error


def _required_int(value: object, *, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _decimal_text(value: object, *, label: str) -> str:
    if type(value) is not str or _DECIMAL.fullmatch(value) is None:
        raise ValueError(f"{label} must be an exact decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"{label} must be an exact decimal string") from error
    if not parsed.is_finite():
        raise ValueError(f"{label} must be finite")
    return value


def _milliseconds_to_ns(value: object, *, label: str) -> int:
    return _required_int(value, label=label) * 1_000_000


def _microseconds_to_ns(value: object, *, label: str) -> int:
    return _required_int(value, label=label) * 1_000


def canonical_lighter_market(market_id: int) -> str:
    if type(market_id) is not int or market_id < 0 or market_id > 65_535:
        raise ValueError("Lighter market id must be within 0..65535")
    return f"LIGHTER:MARKET:{market_id}"


@dataclass(frozen=True, slots=True)
class LighterMarketMetadata:
    market_id: int
    symbol: str
    market_type: str
    status: str
    supported_price_decimals: int
    supported_size_decimals: int
    supported_quote_decimals: int
    min_base_amount: str
    min_quote_amount: str
    maker_fee: str
    taker_fee: str

    def __post_init__(self) -> None:
        canonical_lighter_market(self.market_id)
        if not self.symbol or not self.symbol.isascii():
            raise ValueError("Lighter market symbol must be non-empty ASCII")
        if self.market_type not in {"perp", "spot"}:
            raise ValueError("Lighter market type must be perp or spot")
        if not self.status:
            raise ValueError("Lighter market status is required")
        for value in (
            self.supported_price_decimals,
            self.supported_size_decimals,
            self.supported_quote_decimals,
        ):
            if type(value) is not int or value < 0 or value > 18:
                raise ValueError("Lighter precision must be within 0..18")
        for text_value, label in (
            (self.min_base_amount, "minimum base amount"),
            (self.min_quote_amount, "minimum quote amount"),
            (self.maker_fee, "maker fee"),
            (self.taker_fee, "taker fee"),
        ):
            _decimal_text(text_value, label=label)

    def to_dict(self) -> dict[str, CanonicalValue]:
        return {
            "maker_fee": self.maker_fee,
            "market_id": self.market_id,
            "market_type": self.market_type,
            "min_base_amount": self.min_base_amount,
            "min_quote_amount": self.min_quote_amount,
            "status": self.status,
            "supported_price_decimals": self.supported_price_decimals,
            "supported_quote_decimals": self.supported_quote_decimals,
            "supported_size_decimals": self.supported_size_decimals,
            "symbol": self.symbol,
            "taker_fee": self.taker_fee,
        }


def lighter_market_census(raw_payload: bytes, *, limit: int) -> tuple[LighterMarketMetadata, ...]:
    if limit <= 0 or limit > 100:
        raise ValueError("Lighter market census limit must be within 1..100")
    payload = _mapping(_strict_json(raw_payload), label="Lighter orderBooks payload")
    if payload.get("code") != 200:
        raise ValueError("Lighter orderBooks payload did not return code 200")
    rows = _sequence(payload.get("order_books"), label="Lighter order books")
    markets: list[LighterMarketMetadata] = []
    for raw in rows:
        item = _mapping(raw, label="Lighter market metadata")
        markets.append(
            LighterMarketMetadata(
                market_id=_required_int(item.get("market_id"), label="Lighter market id"),
                symbol=str(item.get("symbol") or ""),
                market_type=str(item.get("market_type") or ""),
                status=str(item.get("status") or ""),
                supported_price_decimals=_required_int(
                    item.get("supported_price_decimals"), label="Lighter price precision"
                ),
                supported_size_decimals=_required_int(
                    item.get("supported_size_decimals"), label="Lighter size precision"
                ),
                supported_quote_decimals=_required_int(
                    item.get("supported_quote_decimals"), label="Lighter quote precision"
                ),
                min_base_amount=_decimal_text(
                    item.get("min_base_amount"), label="Lighter minimum base amount"
                ),
                min_quote_amount=_decimal_text(
                    item.get("min_quote_amount"), label="Lighter minimum quote amount"
                ),
                maker_fee=_decimal_text(item.get("maker_fee"), label="Lighter maker fee"),
                taker_fee=_decimal_text(item.get("taker_fee"), label="Lighter taker fee"),
            )
        )
    if not markets:
        raise ValueError("Lighter orderBooks returned no markets")
    ordered = sorted(markets, key=lambda item: item.market_id)
    if len({item.market_id for item in ordered}) != len(ordered):
        raise ValueError("Lighter orderBooks returned duplicate market ids")
    return tuple(ordered[:limit])


class LighterPublicAdapter:
    venue = Venue.LIGHTER
    metadata_version = LIGHTER_METADATA_VERSION
    supported_feeds = frozenset({"metadata", "order_book", "ticker", "market_stats", "trades"})

    def __init__(self) -> None:
        self._book_state: dict[int, tuple[int, int]] = {}
        self._seen_book_payloads: set[str] = set()
        self._frozen_markets: set[int] = set()

    @property
    def frozen_markets(self) -> frozenset[int]:
        return frozenset(self._frozen_markets)

    def begin_connection_epoch(self) -> None:
        self._book_state.clear()

    def _validate_feeds(self, feeds: set[str]) -> None:
        unknown = feeds - self.supported_feeds
        if unknown:
            raise ValueError(f"unsupported Lighter public feeds: {sorted(unknown)}")

    def public_http_requests(
        self, *, feeds: Sequence[str], market_indices: Sequence[int]
    ) -> tuple[PublicHttpRequest, ...]:
        del market_indices
        selected = set(feeds)
        self._validate_feeds(selected)
        if "metadata" not in selected:
            return ()
        return (
            PublicHttpRequest(
                method="GET",
                url=f"{LIGHTER_PUBLIC_HTTP_URL}/orderBooks",
                query=(("filter", "all"),),
            ),
        )

    def websocket_subscriptions(
        self, *, feeds: Sequence[str], market_indices: Sequence[int]
    ) -> tuple[PublicWebsocketSubscription, ...]:
        selected = set(feeds)
        self._validate_feeds(selected)
        if any(type(item) is not int or item < 0 or item > 65_535 for item in market_indices):
            raise ValueError("Lighter market indices must be within 0..65535")
        channel_names = {
            "order_book": "order_book",
            "ticker": "ticker",
            "market_stats": "market_stats",
            "trades": "trade",
        }
        return tuple(
            PublicWebsocketSubscription(
                url=LIGHTER_PUBLIC_WEBSOCKET_URL,
                payload={"channel": f"{channel_names[feed]}/{market_id}", "type": "subscribe"},
            )
            for market_id in market_indices
            for feed in ("order_book", "ticker", "market_stats", "trades")
            if feed in selected
        )

    def envelope_from_http(
        self,
        raw_payload: bytes,
        *,
        factory: SessionEnvelopeFactory,
        receive_timestamp_utc_ns: int,
        receive_monotonic_ns: int,
        provenance: CaptureProvenance | None = None,
    ) -> PublicDataEnvelope:
        lighter_market_census(raw_payload, limit=100)
        return factory.make(
            feed_type="metadata",
            instrument_id="LIGHTER:GLOBAL",
            market_id=None,
            source_timestamp_ns=None,
            receive_timestamp_utc_ns=receive_timestamp_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            raw_payload=raw_payload,
            provenance=provenance,
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
        payload = _mapping(_strict_json(raw_payload), label="Lighter WebSocket payload")
        if payload.get("type") == "pong":
            return factory.make(
                feed_type="heartbeat",
                instrument_id="LIGHTER:GLOBAL",
                market_id=None,
                source_timestamp_ns=None,
                receive_timestamp_utc_ns=receive_timestamp_utc_ns,
                receive_monotonic_ns=receive_monotonic_ns,
                raw_payload=raw_payload,
                provenance=provenance,
            )
        channel = payload.get("channel")
        if type(channel) is not str:
            raise ValueError("Lighter WebSocket payload omitted its public channel")
        match = _CHANNEL.fullmatch(channel)
        if match is None:
            raise ValueError("Lighter WebSocket payload used a non-allowlisted channel")
        channel_kind, market_text = match.groups()
        market_id = int(market_text)
        canonical_market = canonical_lighter_market(market_id)
        feed_type = "trades" if channel_kind == "trade" else channel_kind
        source_timestamp_ns: int | None = None
        source_sequence: int | None = None
        source_cursor: str | None = None
        source_event_id: str | None = None
        explicit_gap = False
        explicit_gap_reason: str | None = None

        if channel_kind == "order_book":
            if market_id in self._frozen_markets:
                raise ValueError(f"LIGHTER_CONTINUITY_FROZEN:{market_id}")
            book = _mapping(payload.get("order_book"), label="Lighter order book")
            if book.get("code") != 0:
                raise ValueError("Lighter order book returned a non-zero code")
            nonce = _required_int(book.get("nonce"), label="Lighter order book nonce")
            begin_nonce = _required_int(
                book.get("begin_nonce"), label="Lighter order book begin_nonce"
            )
            offset = _required_int(book.get("offset"), label="Lighter order book offset")
            outer_offset = payload.get("offset")
            if outer_offset is not None and _required_int(
                outer_offset, label="Lighter outer order book offset"
            ) != offset:
                raise ValueError("Lighter inner and outer order book offsets disagree")
            source_sequence = nonce
            source_cursor = f"begin_nonce={begin_nonce};offset={offset}"
            updated_at = book.get("last_updated_at", payload.get("last_updated_at"))
            source_timestamp_ns = _microseconds_to_ns(
                updated_at, label="Lighter order book last_updated_at"
            )
            fingerprint = hashlib.sha256(raw_payload).hexdigest()
            duplicate = fingerprint in self._seen_book_payloads
            self._seen_book_payloads.add(fingerprint)
            previous = self._book_state.get(market_id)
            if not duplicate and previous is not None:
                previous_nonce, previous_offset = previous
                if begin_nonce != previous_nonce:
                    explicit_gap = True
                    explicit_gap_reason = "LIGHTER_BEGIN_NONCE_MISMATCH"
                elif offset <= previous_offset:
                    explicit_gap = True
                    explicit_gap_reason = "LIGHTER_OFFSET_NOT_INCREASING"
            if not duplicate:
                self._book_state[market_id] = (nonce, offset)
            if explicit_gap:
                self._frozen_markets.add(market_id)
        elif channel_kind == "ticker":
            _mapping(payload.get("ticker"), label="Lighter ticker")
            source_sequence = _required_int(payload.get("nonce"), label="Lighter ticker nonce")
            source_timestamp_ns = _microseconds_to_ns(
                payload.get("last_updated_at"), label="Lighter ticker last_updated_at"
            )
        elif channel_kind == "market_stats":
            stats = _mapping(payload.get("market_stats"), label="Lighter market stats")
            if _required_int(stats.get("market_id"), label="Lighter market stats id") != market_id:
                raise ValueError("Lighter market stats id differs from its channel")
            source_timestamp_ns = _milliseconds_to_ns(
                payload.get("timestamp"), label="Lighter market stats timestamp"
            )
        else:
            trades = _sequence(payload.get("trades"), label="Lighter trades")
            _sequence(payload.get("liquidation_trades", ()), label="Lighter liquidation trades")
            source_sequence = _required_int(payload.get("nonce"), label="Lighter trade nonce")
            trade_rows = [_mapping(item, label="Lighter trade") for item in trades]
            if trade_rows:
                timestamps = [
                    _milliseconds_to_ns(item.get("timestamp"), label="Lighter trade timestamp")
                    for item in trade_rows
                ]
                source_timestamp_ns = max(timestamps)
                if len(trade_rows) == 1 and trade_rows[0].get("trade_id") is not None:
                    source_event_id = str(trade_rows[0]["trade_id"])

        return factory.make(
            feed_type=feed_type,
            instrument_id=canonical_market,
            market_id=canonical_market,
            source_timestamp_ns=source_timestamp_ns,
            receive_timestamp_utc_ns=receive_timestamp_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            raw_payload=raw_payload,
            source_sequence=source_sequence,
            source_cursor=source_cursor,
            source_event_id=source_event_id,
            provenance=provenance,
            infer_source_sequence_continuity=False,
            explicit_gap_detected=explicit_gap,
            explicit_gap_reason=explicit_gap_reason,
        )


__all__ = [
    "LIGHTER_DOCUMENTARY_CONTRACT",
    "LIGHTER_METADATA_VERSION",
    "LIGHTER_PUBLIC_HTTP_URL",
    "LIGHTER_PUBLIC_WEBSOCKET_URL",
    "LighterMarketMetadata",
    "LighterPublicAdapter",
    "canonical_lighter_market",
    "lighter_market_census",
]
