from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import urlencode
from uuid import uuid4

from .adapters import (
    HYPERLIQUID_METADATA_VERSION,
    HYPERLIQUID_PUBLIC_HTTP_URL,
    HYPERLIQUID_PUBLIC_WEBSOCKET_URL,
    KALSHI_METADATA_VERSION,
    KALSHI_PUBLIC_HTTP_URL,
    POLYMARKET_GAMMA_PUBLIC_URL,
    POLYMARKET_METADATA_VERSION,
    POLYMARKET_PUBLIC_WEBSOCKET_URL,
    HyperliquidPublicAdapter,
    KalshiPublicAdapter,
    PolymarketPublicAdapter,
    PublicHttpRequest,
)
from .canonical import CanonicalValue, canonical_json_bytes
from .envelope import CaptureProvenance, SessionEnvelopeFactory, Venue
from .lighter import (
    LIGHTER_METADATA_VERSION,
    LIGHTER_PUBLIC_HTTP_URL,
    LIGHTER_PUBLIC_READONLY_WEBSOCKET_URL,
    LIGHTER_PUBLIC_WEBSOCKET_URL,
    LighterPublicAdapter,
    lighter_market_census,
)
from .prediction_contracts import (
    BoundedCursorPager,
    normalize_kalshi_event_metadata,
    normalize_polymarket_clob_v2_market,
    polymarket_gamma_token_outcomes,
)
from .prediction_time import prediction_rfc3339_to_ns
from .segments import (
    SEGMENT_SUFFIX,
    ManifestRecord,
    ResearchDataCapacityError,
    ResearchSegmentWriter,
    decode_segment,
)

_TERMINAL_ERROR_MAX_UTF8_BYTES = 2_048


def _bounded_terminal_error(value: str) -> str:
    """Return deterministic non-empty UTF-8 text within the receipt contract."""

    normalized = value.strip() or "UNSPECIFIED_PUBLIC_COLLECTION_ERROR"
    raw = normalized.encode("utf-8")
    if len(raw) <= _TERMINAL_ERROR_MAX_UTF8_BYTES:
        return normalized
    suffix = "...[TRUNCATED]"
    prefix = raw[: _TERMINAL_ERROR_MAX_UTF8_BYTES - len(suffix)]
    while prefix:
        try:
            return prefix.decode("utf-8") + suffix
        except UnicodeDecodeError as error:
            prefix = prefix[: error.start]
    return suffix


class HttpResponse(Protocol):
    status_code: int
    content: bytes

    def iter_content(self, chunk_size: int) -> Iterator[bytes]: ...
    def close(self) -> None: ...


class HttpSession(Protocol):
    def get(self, url: str, **kwargs: object) -> HttpResponse: ...
    def post(self, url: str, **kwargs: object) -> HttpResponse: ...
    def close(self) -> None: ...


def _request_source_url(request: PublicHttpRequest) -> str:
    if not request.query:
        return request.url
    return f"{request.url}?{urlencode(request.query)}"


def _request_provenance(factory: SessionEnvelopeFactory, request: PublicHttpRequest) -> CaptureProvenance:
    return CaptureProvenance(
        factory.provenance.collection_id,
        _request_source_url(request),
        "PUBLIC_HTTP",
    )


@dataclass(frozen=True, slots=True)
class ProbeConfig:
    output_root: Path
    venue: Venue
    feeds: tuple[str, ...]
    instruments: tuple[str, ...]
    census_limit: int
    duration_seconds: int
    max_bytes: int
    max_segment_bytes: int
    rotation_seconds: float
    progress_interval_seconds: float
    collection_id: str | None = None
    max_frames: int = 50_000
    max_segments: int = 100
    max_network_calls: int = 100
    campaign_manifest_sha256: str | None = None
    official_contract_sha256: str | None = None
    candidate_config_sha256: str | None = None
    collection_cutoff_utc_ns_exclusive: int | None = None

    def __post_init__(self) -> None:
        if not self.feeds:
            raise ValueError("probe feeds are required")
        if len(self.feeds) != len(set(self.feeds)) or any(not item for item in self.feeds):
            raise ValueError("probe feeds must be unique non-empty names")
        if (
            len(self.instruments) > 100
            or len(self.instruments) != len(set(self.instruments))
            or any(not item for item in self.instruments)
        ):
            raise ValueError("probe instruments must be unique, non-empty, and bounded to 100")
        if not self.instruments and self.census_limit <= 0:
            raise ValueError("explicit instruments/markets or a bounded census is required")
        if self.census_limit < 0 or self.census_limit > 100:
            raise ValueError("probe census limit must be within 0..100")
        duration_limit = 600 if self.venue is Venue.LIGHTER else 300
        if self.duration_seconds <= 0 or self.duration_seconds > duration_limit:
            raise ValueError(f"{self.venue.value} probe duration must be within 1..{duration_limit} seconds")
        if self.max_bytes < 4_096 or self.max_segment_bytes < 1_024:
            raise ValueError("probe and segment byte bounds are too small")
        if self.max_segment_bytes > self.max_bytes:
            raise ValueError("segment byte bound cannot exceed total byte bound")
        if self.rotation_seconds <= 0 or self.progress_interval_seconds <= 0:
            raise ValueError("probe rotation and progress intervals must be positive")
        if self.max_frames <= 0 or self.max_frames > 50_000:
            raise ValueError("probe frame bound must be within 1..50000")
        if self.max_segments <= 0 or self.max_segments > 100:
            raise ValueError("probe segment bound must be within 1..100")
        if self.max_network_calls <= 0 or self.max_network_calls > 1_000:
            raise ValueError("probe network-call bound must be within 1..1000")
        for value, label in (
            (self.campaign_manifest_sha256, "campaign manifest"),
            (self.official_contract_sha256, "official contract"),
            (self.candidate_config_sha256, "candidate config"),
        ):
            if value is not None and (
                len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ValueError(f"{label} binding must be a SHA-256")
        binding_presence = tuple(
            value is not None
            for value in (
                self.campaign_manifest_sha256,
                self.official_contract_sha256,
                self.candidate_config_sha256,
            )
        )
        if any(binding_presence) and not all(binding_presence):
            raise ValueError("prediction probe bindings must be all present or all absent")
        if self.collection_cutoff_utc_ns_exclusive is not None and (
            type(self.collection_cutoff_utc_ns_exclusive) is not int
            or self.collection_cutoff_utc_ns_exclusive <= 0
            or not all(binding_presence)
            or self.venue not in {Venue.POLYMARKET, Venue.KALSHI}
        ):
            raise ValueError("prediction collection cutoff requires a bound positive UTC nanosecond")
        if all(binding_presence) and self.collection_cutoff_utc_ns_exclusive is None:
            raise ValueError("campaign-bound prediction probe requires its authenticated slot cutoff")
        supported = {
            Venue.HYPERLIQUID: HyperliquidPublicAdapter.supported_feeds,
            Venue.LIGHTER: LighterPublicAdapter.supported_feeds,
            Venue.POLYMARKET: PolymarketPublicAdapter.supported_feeds,
            Venue.KALSHI: KalshiPublicAdapter.supported_feeds,
        }[self.venue]
        unknown = set(self.feeds) - supported
        if unknown:
            raise ValueError(f"unsupported {self.venue.value} public feeds: {sorted(unknown)}")
        if self.venue is Venue.KALSHI and set(self.feeds) & {
            "historical_markets",
            "historical_trades",
        }:
            required_historical_graph = {
                "event_metadata",
                "events",
                "historical_cutoff",
                "historical_markets",
                "series",
            }
            if not required_historical_graph.issubset(self.feeds):
                raise ValueError(
                    "Kalshi historical collection requires cutoff, market, event metadata and series"
                )


@dataclass(frozen=True, slots=True)
class ProbeReport:
    schema_version: int
    boundary: str
    venue: str
    terminal_health: str
    collection_id: str
    requested_duration_seconds: int
    elapsed_ms: int
    frames: int
    segments: int
    bytes: int
    gaps: int
    duplicates: int
    reconnects: int
    queue_high_water: int
    source_timestamp_min_ns: int | None
    source_timestamp_max_ns: int | None
    manifest_sha256: str | None
    root_sha256: str | None
    network_calls: int
    limitations: tuple[str, ...]
    error: str | None
    probe_binding_sha256: str | None = None
    campaign_manifest_sha256: str | None = None
    official_contract_sha256: str | None = None
    candidate_config_sha256: str | None = None
    connection_attempts: tuple[dict[str, CanonicalValue], ...] = ()

    def to_dict(self) -> dict[str, CanonicalValue]:
        return {
            "boundary": self.boundary,
            "bytes": self.bytes,
            "campaign_manifest_sha256": self.campaign_manifest_sha256,
            "candidate_config_sha256": self.candidate_config_sha256,
            "collection_id": self.collection_id,
            "connection_attempts": list(self.connection_attempts),
            "duplicates": self.duplicates,
            "elapsed_ms": self.elapsed_ms,
            "error": self.error,
            "frames": self.frames,
            "gaps": self.gaps,
            "limitations": list(self.limitations),
            "manifest_sha256": self.manifest_sha256,
            "network_calls": self.network_calls,
            "official_contract_sha256": self.official_contract_sha256,
            "probe_binding_sha256": self.probe_binding_sha256,
            "queue_high_water": self.queue_high_water,
            "reconnects": self.reconnects,
            "requested_duration_seconds": self.requested_duration_seconds,
            "root_sha256": self.root_sha256,
            "schema_version": self.schema_version,
            "segments": self.segments,
            "source_timestamp_max_ns": self.source_timestamp_max_ns,
            "source_timestamp_min_ns": self.source_timestamp_min_ns,
            "terminal_health": self.terminal_health,
            "venue": self.venue,
        }


def _probe_binding_payload(
    config: ProbeConfig,
    *,
    collection_id: str,
) -> dict[str, CanonicalValue]:
    return {
        "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        "campaign_manifest_sha256": config.campaign_manifest_sha256,
        "candidate_config_sha256": config.candidate_config_sha256,
        "census_limit": config.census_limit,
        "collection_id": collection_id,
        "collection_cutoff_utc_ns_exclusive": config.collection_cutoff_utc_ns_exclusive,
        "duration_seconds": config.duration_seconds,
        "feeds": list(config.feeds),
        "instruments": list(config.instruments),
        "max_bytes": config.max_bytes,
        "max_frames": config.max_frames,
        "max_network_calls": config.max_network_calls,
        "max_segment_bytes": config.max_segment_bytes,
        "max_segments": config.max_segments,
        "official_contract_sha256": config.official_contract_sha256,
        "progress_interval_seconds": format(config.progress_interval_seconds, "g"),
        "proxy_policy": "DIRECT_ONLY_ENVIRONMENT_PROXY_DISABLED",
        "rotation_seconds": format(config.rotation_seconds, "g"),
        "schema_version": 1,
        "venue": config.venue.value,
    }


def _probe_binding_payload_from_mapping(
    value: Mapping[str, object],
) -> dict[str, CanonicalValue]:
    expected = {
        "boundary",
        "campaign_manifest_sha256",
        "candidate_config_sha256",
        "census_limit",
        "collection_id",
        "collection_cutoff_utc_ns_exclusive",
        "duration_seconds",
        "feeds",
        "instruments",
        "max_bytes",
        "max_frames",
        "max_network_calls",
        "max_segment_bytes",
        "max_segments",
        "official_contract_sha256",
        "progress_interval_seconds",
        "proxy_policy",
        "rotation_seconds",
        "schema_version",
        "venue",
    }
    legacy_expected = expected - {"collection_cutoff_utc_ns_exclusive"}
    observed = frozenset(set(value) - {"probe_binding_sha256"})
    if observed not in {frozenset(expected), frozenset(legacy_expected)}:
        raise ValueError("recovery probe binding fields differ from schema v1")
    bindings = tuple(
        value.get(key)
        for key in (
            "campaign_manifest_sha256",
            "official_contract_sha256",
            "candidate_config_sha256",
        )
    )
    if any(item is not None for item in bindings) != all(item is not None for item in bindings):
        raise ValueError("recovery prediction bindings must be all present or all absent")
    for item in bindings:
        if item is not None and (
            type(item) is not str
            or len(item) != 64
            or any(character not in "0123456789abcdef" for character in item)
        ):
            raise ValueError("recovery prediction binding is not a SHA-256")
    selected_fields = expected if "collection_cutoff_utc_ns_exclusive" in value else legacy_expected
    canonical = json.loads(canonical_json_bytes({key: value[key] for key in selected_fields}))
    if not isinstance(canonical, dict):
        raise AssertionError("probe binding payload must remain an object")
    return cast(dict[str, CanonicalValue], canonical)


def _probe_binding_sha256(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class _Counters:
    def __init__(self) -> None:
        self.gaps = 0
        self.duplicates = 0
        self.reconnects = 0
        self.queue_high_water = 0
        self.source_timestamps: list[int] = []

    def observe(self, envelope: Any) -> None:
        self.gaps += int(envelope.state.gap_detected)
        self.duplicates += int(envelope.state.duplicate)
        if envelope.source_timestamp_ns is not None:
            self.source_timestamps.append(envelope.source_timestamp_ns)

    def begin_reconnect(self, factory: SessionEnvelopeFactory) -> None:
        self.reconnects += 1
        factory.begin_reconnect()


class _ProbeBoundaryReached(RuntimeError):
    def __init__(self, terminal_health: str) -> None:
        super().__init__(terminal_health)
        self.terminal_health = terminal_health


@dataclass(slots=True)
class _NetworkBudget:
    maximum_calls: int
    calls: int = 0

    def consume(self) -> None:
        if self.calls >= self.maximum_calls:
            raise _ProbeBoundaryReached("MAX_NETWORK_CALLS_REACHED")
        self.calls += 1


def _atomic_json(path: Path, body: Mapping[str, object]) -> None:
    value = canonical_json_bytes(body)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("xb") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _execute_http(
    session: HttpSession,
    request: PublicHttpRequest,
    *,
    deadline: float,
    max_response_bytes: int,
    budget: _NetworkBudget | None = None,
) -> bytes:
    if max_response_bytes <= 0:
        raise ValueError("HTTP response bound must be positive")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise _ProbeBoundaryReached("MAX_DURATION_REACHED")
    if budget is not None:
        budget.consume()
    phase_timeout = max(0.05, min(5.0, remaining / 2))
    kwargs: dict[str, object] = {
        "allow_redirects": False,
        "params": dict(request.query),
        "stream": True,
        "timeout": (phase_timeout, phase_timeout),
    }
    if request.method == "POST":
        kwargs["json"] = request.json_body
        response = session.post(request.url, **kwargs)
    else:
        response = session.get(request.url, **kwargs)
    try:
        if response.status_code != 200:
            raise ConnectionError(
                f"public source returned HTTP {response.status_code} for {_request_source_url(request)}"
            )
        iterator = getattr(response, "iter_content", None)
        chunks = iterator(chunk_size=64 * 1024) if callable(iterator) else iter((bytes(response.content),))
        body = bytearray()
        for chunk in chunks:
            body.extend(chunk)
            if len(body) > max_response_bytes:
                raise ResearchDataCapacityError("public HTTP response exceeds its raw byte bound")
            if time.monotonic() > deadline:
                raise _ProbeBoundaryReached("MAX_DURATION_REACHED")
        return bytes(body)
    finally:
        close = getattr(response, "close", None)
        if callable(close):
            close()


def _http_response_bound(config: ProbeConfig) -> int:
    envelope_safe = max(1, (config.max_segment_bytes - 1_024) * 3 // 4)
    return min(config.max_bytes, envelope_safe)


def _default_http_session() -> HttpSession:
    import requests

    session = requests.Session()
    session.trust_env = False
    session.headers.update({"User-Agent": "HyperLab-Research-Data-Plane-V1-PUBLIC-DIRECT-ONLY"})
    return cast(HttpSession, session)


def _default_lighter_http_session() -> HttpSession:
    import requests

    session = requests.Session()
    session.trust_env = False
    session.headers.update({"User-Agent": "HyperLab-Lighter-Public-Probe-V1-DIRECT-ONLY"})
    return cast(HttpSession, session)


def _append(writer: ResearchSegmentWriter, counters: _Counters, envelope: Any) -> None:
    writer.append(envelope)
    counters.observe(envelope)


def _append_probe_bounded(
    writer: ResearchSegmentWriter,
    counters: _Counters,
    envelope: Any,
    config: ProbeConfig,
    *,
    allow_polymarket_reconnect_control: bool = False,
) -> None:
    if (
        config.collection_cutoff_utc_ns_exclusive is not None
        and envelope.receive_timestamp_utc_ns >= config.collection_cutoff_utc_ns_exclusive
    ):
        raise _ProbeBoundaryReached("MAX_DURATION_REACHED")
    if writer.segment_count >= config.max_segments:
        raise _ProbeBoundaryReached("MAX_SEGMENTS_REACHED")
    if writer.segment_count == config.max_segments - 1 and writer.would_rotate(envelope):
        writer.flush()
        raise _ProbeBoundaryReached("MAX_SEGMENTS_REACHED")
    _append(writer, counters, envelope)
    if writer.frame_count >= config.max_frames:
        raise _ProbeBoundaryReached("MAX_FRAMES_REACHED")
    if envelope.state.gap_detected:
        raise _ProbeBoundaryReached("CONTINUITY_BROKEN_FROZEN")
    allowed_reconnect_control = (
        allow_polymarket_reconnect_control
        and config.venue is Venue.POLYMARKET
        and envelope.feed_type == "heartbeat"
        and envelope.instrument_id == "PM:GLOBAL"
        and envelope.market_id == "PM:GLOBAL"
        and envelope.state.reconnect
        and envelope.state.reason == "RECONNECT_BOUNDARY"
        and envelope.provenance.transport == "PUBLIC_WEBSOCKET"
        and envelope.provenance.source_url == POLYMARKET_PUBLIC_WEBSOCKET_URL
    )
    if (
        config.venue in {Venue.POLYMARKET, Venue.KALSHI}
        and envelope.state.reconnect
        and not allowed_reconnect_control
    ):
        raise _ProbeBoundaryReached("CONTINUITY_UNKNOWN_AFTER_RECONNECT_FROZEN")


def _append_lighter_bounded(
    writer: ResearchSegmentWriter,
    counters: _Counters,
    envelope: Any,
    config: ProbeConfig,
) -> None:
    _append_probe_bounded(writer, counters, envelope, config)


def _raw_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _require_polymarket_websocket_selection(
    envelope: Any,
    selected_feeds: set[str],
) -> None:
    if envelope.feed_type == "heartbeat":
        return
    if envelope.feed_type == "market_batch":
        decoded = json.loads(envelope.raw_payload.decode("utf-8"))
        if not isinstance(decoded, list):
            raise ValueError("Polymarket WebSocket batch must remain an array")
        event_feeds = {
            "best_bid_ask": "best_bid_ask",
            "book": "order_book",
            "last_trade_price": "last_trade_price",
            "market_resolved": "market_lifecycle",
            "new_market": "market_lifecycle",
            "price_change": "price_change",
            "tick_size_change": "tick_size_change",
        }
        observed = tuple(
            event_feeds.get(str(item.get("event_type") or ""))
            if isinstance(item, Mapping)
            else None
            for item in decoded
        )
        if any(item is None or item not in selected_feeds for item in observed):
            raise ValueError("Polymarket WebSocket batch escaped the frozen feed selection")
        return
    if envelope.feed_type not in selected_feeds:
        raise ValueError("Polymarket WebSocket frame escaped the frozen feed selection")


def _datetime_to_ns(value: datetime) -> int:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("collector receive timestamp must be timezone-aware")
    delta = value.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return delta.days * 86_400_000_000_000 + delta.seconds * 1_000_000_000 + delta.microseconds * 1_000


def _polymarket_tokens(census_payload: bytes, *, limit: int) -> tuple[tuple[str, str], ...]:
    decoded = json.loads(census_payload.decode("utf-8"))
    if not isinstance(decoded, Mapping) or not isinstance(decoded.get("markets"), list):
        raise ValueError("Polymarket keyset market census must contain markets")
    selected: list[tuple[str, str]] = []
    for market in decoded["markets"]:
        if not isinstance(market, Mapping):
            continue
        market_id = str(market.get("conditionId") or "")
        if (
            not market_id
            or not market.get("questionID")
        ):
            raise ValueError("Polymarket census market identity graph is incomplete")
        for token, _outcome in polymarket_gamma_token_outcomes(market):
            selected.append((token, market_id))
            if len(selected) >= limit:
                return tuple(selected)
    return tuple(selected)


def _polymarket_keyset_markets(payload: bytes) -> tuple[Mapping[str, object], ...]:
    decoded = json.loads(payload.decode("utf-8"))
    if not isinstance(decoded, Mapping) or not isinstance(decoded.get("markets"), list):
        raise ValueError("Polymarket keyset response must contain markets")
    markets = decoded["markets"]
    if any(not isinstance(item, Mapping) for item in markets):
        raise ValueError("Polymarket keyset markets must be objects")
    return tuple(cast(Mapping[str, object], item) for item in markets)


def _next_cursor(payload: bytes, *, key: str) -> str | None:
    decoded = json.loads(payload.decode("utf-8"))
    if not isinstance(decoded, Mapping):
        raise ValueError("cursor page must be an object")
    value = decoded.get(key)
    if value is None or value == "":
        return None
    if type(value) is not str:
        raise ValueError("cursor value must be absent, null, or exact text")
    if len(value) > 4_096 or any(ord(character) < 32 for character in value):
        raise ValueError("cursor value must be bounded opaque text")
    return value


def _polymarket_event_id(market: Mapping[str, object]) -> str | None:
    if market.get("eventId"):
        return str(market["eventId"])
    events = market.get("events")
    if isinstance(events, list) and len(events) == 1 and isinstance(events[0], Mapping):
        event_id = events[0].get("id")
        return None if event_id is None else str(event_id)
    return None


def _hyperliquid_instruments(census_payload: bytes, *, limit: int) -> tuple[str, ...]:
    decoded = json.loads(census_payload.decode("utf-8"))
    if not isinstance(decoded, list) or not decoded or not isinstance(decoded[0], Mapping):
        raise ValueError("Hyperliquid metaAndAssetCtxs census is invalid")
    universe = decoded[0].get("universe")
    if not isinstance(universe, list):
        raise ValueError("Hyperliquid census omitted universe")
    names = sorted({str(item["name"]) for item in universe if isinstance(item, Mapping) and item.get("name")})
    if not names:
        raise LookupError("Hyperliquid census returned no instruments")
    return tuple(names[:limit])


def _kalshi_markets(census_payload: bytes, *, limit: int) -> tuple[tuple[str, str | None, str | None], ...]:
    decoded = json.loads(census_payload.decode("utf-8"))
    if not isinstance(decoded, Mapping):
        raise ValueError("Kalshi market census is invalid")
    markets: list[Mapping[str, object]]
    if isinstance(decoded.get("market"), Mapping):
        markets = [cast(Mapping[str, object], decoded["market"])]
    elif isinstance(decoded.get("markets"), list):
        raw_markets = decoded["markets"]
        if any(not isinstance(item, Mapping) for item in raw_markets):
            raise ValueError("Kalshi market census records must be objects")
        markets = [cast(Mapping[str, object], item) for item in raw_markets]
    else:
        raise ValueError("Kalshi market census is invalid")
    identities: list[tuple[str, str | None, str | None]] = []
    for market in markets[:limit]:
        ticker = market.get("ticker")
        if type(ticker) is not str or not ticker:
            raise ValueError("Kalshi market census omitted ticker")
        event = market.get("event_ticker")
        if event is not None and (type(event) is not str or not event):
            raise ValueError("Kalshi market census event ticker must be exact text")
        identities.append((ticker, event, None))
    return tuple(identities)


def _kalshi_series(event_payload: bytes, *, expected_event_ticker: str | None = None) -> str:
    decoded = json.loads(event_payload.decode("utf-8"))
    if not isinstance(decoded, Mapping) or not isinstance(decoded.get("event"), Mapping):
        raise ValueError("Kalshi event metadata is invalid")
    event = cast(Mapping[str, object], decoded["event"])
    observed_event_ticker = event.get("event_ticker")
    if expected_event_ticker is not None and observed_event_ticker != expected_event_ticker:
        raise ValueError("Kalshi event payload returned another event ticker")
    series = event.get("series_ticker")
    if type(series) is not str or not series:
        raise ValueError("Kalshi event metadata omitted series_ticker")
    return series


def _kalshi_event_metadata_record(
    payload: bytes,
    *,
    provenance: CaptureProvenance,
    expected_event_ticker: str,
    expected_market_ticker: str,
) -> Mapping[str, object]:
    decoded = json.loads(payload.decode("utf-8"))
    if not isinstance(decoded, Mapping) or "event_metadata" in decoded:
        raise ValueError("Kalshi event metadata payload is invalid")
    return normalize_kalshi_event_metadata(
        cast(Mapping[str, object], decoded),
        provenance=provenance,
        expected_event_ticker=expected_event_ticker,
        expected_market_ticker=expected_market_ticker,
    )


def _kalshi_series_record(
    payload: bytes,
    *,
    expected_series_ticker: str,
) -> Mapping[str, object]:
    decoded = json.loads(payload.decode("utf-8"))
    if not isinstance(decoded, Mapping) or not isinstance(decoded.get("series"), Mapping):
        raise ValueError("Kalshi series payload is invalid")
    series = cast(Mapping[str, object], decoded["series"])
    observed = series.get("ticker") or series.get("series_ticker")
    if type(observed) is not str or observed != expected_series_ticker:
        raise ValueError("Kalshi series payload returned another series ticker")
    return series


def _kalshi_market_record(
    payload: bytes,
    *,
    expected_ticker: str,
    expected_event_ticker: str | None = None,
) -> Mapping[str, object]:
    decoded = json.loads(payload.decode("utf-8"))
    if not isinstance(decoded, Mapping) or not isinstance(decoded.get("market"), Mapping):
        raise ValueError("Kalshi market payload is invalid")
    market = cast(Mapping[str, object], decoded["market"])
    if type(market.get("ticker")) is not str or market.get("ticker") != expected_ticker:
        raise ValueError("Kalshi market payload returned another ticker")
    if expected_event_ticker is not None and market.get("event_ticker") != expected_event_ticker:
        raise ValueError("Kalshi market payload returned another event ticker")
    return market


def _kalshi_trade_ids(
    records: object,
    *,
    expected_ticker: str,
    expected_block_trade: bool | None,
) -> tuple[str, ...]:
    if not isinstance(records, list):
        raise ValueError("Kalshi trade records must be an array")
    identities: list[str] = []
    for raw_record in records:
        if not isinstance(raw_record, Mapping):
            raise ValueError("Kalshi trade record must be an object")
        trade_id = raw_record.get("trade_id")
        ticker = raw_record.get("ticker")
        if (
            type(trade_id) is not str
            or not trade_id.strip()
            or type(ticker) is not str
            or ticker != expected_ticker
        ):
            raise ValueError("Kalshi trade identity or ticker is missing")
        block_trade = raw_record.get("is_block_trade")
        if type(block_trade) is not bool:
            raise ValueError("Kalshi block-trade classification must be an exact boolean")
        if expected_block_trade is not None and block_trade is not expected_block_trade:
            raise ValueError("Kalshi block-trade feed classification diverged")
        identities.append(trade_id)
    return tuple(identities)


def _admitted_kalshi_scopes(
    page_scopes: Sequence[tuple[str, str | None, str | None]],
    admitted_ids: Sequence[str],
) -> tuple[tuple[str, str | None, str | None], ...]:
    remaining = set(admitted_ids)
    selected: list[tuple[str, str | None, str | None]] = []
    for scope in page_scopes:
        if scope[0] not in remaining:
            continue
        selected.append(scope)
        remaining.remove(scope[0])
    return tuple(selected)


def _polymarket_token_parameter_plan(
    adapter: PolymarketPublicAdapter,
    token_markets: Sequence[tuple[str, str]],
    selected_feeds: set[str],
) -> tuple[tuple[str, str, str, PublicHttpRequest], ...]:
    phases = ("fees", "tick_size", "order_book", "last_trade_price")
    return tuple(
        (token, market, feed_type, request)
        for feed_type in phases
        if feed_type in selected_feeds
        for token, market in token_markets
        for returned_feed, request in adapter.token_parameter_requests(
            (token,),
            feeds=(feed_type,),
        )
        if returned_feed == feed_type
    )


def _hyperliquid_probe(
    config: ProbeConfig,
    *,
    factory: SessionEnvelopeFactory,
    writer: ResearchSegmentWriter,
    counters: _Counters,
    deadline: float,
    stop_requested: Callable[[], bool],
    progress: Callable[[int], None],
    session: HttpSession,
    budget: _NetworkBudget | None = None,
) -> tuple[str, ...]:
    import websocket

    from hyperlab.collector.websocket import UrlWebsocketClientFactory

    if budget is None:
        budget = _NetworkBudget(config.max_network_calls)
    adapter = HyperliquidPublicAdapter()
    http_provenance = CaptureProvenance(
        factory.provenance.collection_id,
        HYPERLIQUID_PUBLIC_HTTP_URL,
        "PUBLIC_HTTP",
    )
    ws_provenance = CaptureProvenance(
        factory.provenance.collection_id,
        HYPERLIQUID_PUBLIC_WEBSOCKET_URL,
        "PUBLIC_WEBSOCKET",
    )
    instruments = config.instruments
    census_request_body: Mapping[str, CanonicalValue] | None = None
    if not instruments:
        census_request = PublicHttpRequest(
            method="POST",
            url=HYPERLIQUID_PUBLIC_HTTP_URL,
            json_body={"type": "metaAndAssetCtxs"},
        )
        raw = _execute_http(
            session,
            census_request,
            deadline=deadline,
            max_response_bytes=_http_response_bound(config),
            budget=budget,
        )
        instruments = _hyperliquid_instruments(raw, limit=config.census_limit)
        census_request_body = census_request.json_body
        envelope = adapter.envelope_from_http(
            raw,
            feed_type="metadata",
            instrument=None,
            factory=factory,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            provenance=http_provenance,
        )
        _append_probe_bounded(writer, counters, envelope, config)
    for request in adapter.public_http_requests(feeds=config.feeds, instruments=instruments):
        if request.json_body is None:
            continue
        if census_request_body is not None and request.json_body == census_request_body:
            continue
        raw = _execute_http(
            session,
            request,
            deadline=deadline,
            max_response_bytes=_http_response_bound(config),
            budget=budget,
        )
        feed = "metadata" if request.json_body.get("type") != "l2Book" else "l2_book"
        instrument = None if feed == "metadata" else str(request.json_body.get("coin"))
        envelope = adapter.envelope_from_http(
            raw,
            feed_type=feed,
            instrument=instrument,
            factory=factory,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            provenance=http_provenance,
        )
        _append_probe_bounded(writer, counters, envelope, config)
    subscriptions = adapter.websocket_subscriptions(feeds=config.feeds, instruments=instruments)
    if not subscriptions:
        return adapter.unavailable_public_global_labels
    connector = UrlWebsocketClientFactory(
        HYPERLIQUID_PUBLIC_WEBSOCKET_URL,
        queue_capacity=1_024,
        venue="hyperliquid",
        socket_role="research-data-plane-v1",
        allow_environment_proxy=False,
    )
    delays = (0.25, 0.5, 1.0, 2.0, 4.0)
    attempt = 0
    connected_once = False
    last_connection_error: BaseException | None = None
    last_ping = time.monotonic()
    last_progress = last_ping
    socket: Any = None
    try:
        while time.monotonic() < deadline and not stop_requested():
            if socket is None:
                try:
                    budget.consume()
                    socket = connector.connect("public", max(0.05, min(10.0, deadline - time.monotonic())))
                    for subscription in subscriptions:
                        socket.send_json(dict(subscription.payload))
                    attempt = 0
                    connected_once = True
                    last_connection_error = None
                except (
                    ConnectionError,
                    OSError,
                    TimeoutError,
                    websocket.WebSocketException,
                ) as caught:
                    last_connection_error = caught
                    if time.monotonic() >= deadline:
                        raise ConnectionError("Hyperliquid public WebSocket remained unavailable") from caught
                    counters.begin_reconnect(factory)
                    remaining = max(0.0, deadline - time.monotonic())
                    if remaining > 0:
                        time.sleep(min(delays[min(attempt, len(delays) - 1)], remaining))
                    attempt += 1
                    continue
            try:
                received = socket.receive(timeout_seconds=1.0)
                snapshot = socket.telemetry_snapshot()
                counters.queue_high_water = max(
                    counters.queue_high_water, int(snapshot.get("queue_high_water", 0))
                )
                now = time.monotonic()
                if received is not None:
                    raw_message = received.raw_message.encode("utf-8")
                    if len(raw_message) > _http_response_bound(config):
                        raise ResearchDataCapacityError("public WebSocket frame exceeds its raw byte bound")
                    envelope = adapter.envelope_from_websocket(
                        raw_message,
                        factory=factory,
                        receive_timestamp_utc_ns=_datetime_to_ns(received.received_time),
                        receive_monotonic_ns=(
                            time.monotonic_ns()
                            if received.received_monotonic_ns is None
                            else received.received_monotonic_ns
                        ),
                        provenance=ws_provenance,
                    )
                    _append_probe_bounded(writer, counters, envelope, config)
                if now - last_ping >= 20.0:
                    socket.send_json({"method": "ping"})
                    last_ping = now
                if now - last_progress >= config.progress_interval_seconds:
                    progress(writer.frame_count)
                    last_progress = now
            except BufferError:
                snapshot = socket.telemetry_snapshot()
                counters.queue_high_water = max(
                    counters.queue_high_water, int(snapshot.get("queue_high_water", 0))
                )
                raise
            except (ConnectionError, OSError, TimeoutError, websocket.WebSocketException):
                socket.close()
                socket = None
                counters.begin_reconnect(factory)
    finally:
        if socket is not None:
            socket.close()
    if not connected_once and last_connection_error is not None and not stop_requested():
        raise ConnectionError("Hyperliquid public WebSocket never connected") from last_connection_error
    return adapter.unavailable_public_global_labels


def _lighter_probe(
    config: ProbeConfig,
    *,
    factory: SessionEnvelopeFactory,
    writer: ResearchSegmentWriter,
    counters: _Counters,
    deadline: float,
    stop_requested: Callable[[], bool],
    progress: Callable[[int], None],
    session: HttpSession,
    connection_attempts: list[dict[str, CanonicalValue]],
    budget: _NetworkBudget | None = None,
) -> tuple[str, ...]:
    import websocket

    from hyperlab.collector.websocket import UrlWebsocketClientFactory

    if budget is None:
        budget = _NetworkBudget(config.max_network_calls)
    adapter = LighterPublicAdapter()
    limitations: list[str] = []
    market_indices: tuple[int, ...]
    if config.instruments:
        try:
            market_indices = tuple(int(item) for item in config.instruments)
        except ValueError as error:
            raise ValueError("Lighter instruments must be decimal market indices") from error
        if "metadata" in config.feeds:
            limitations.append("LIGHTER_METADATA_NOT_OBSERVED_EXPLICIT_MARKET_INDEX")
    else:
        metadata_requests = adapter.public_http_requests(
            feeds=config.feeds,
            market_indices=(),
        )
        if not metadata_requests:
            raise ValueError("Lighter census requires the public metadata feed")
        http_provenance = CaptureProvenance(
            factory.provenance.collection_id,
            f"{LIGHTER_PUBLIC_HTTP_URL}/orderBooks",
            "PUBLIC_HTTP",
        )
        raw_metadata = _execute_http(
            session,
            metadata_requests[0],
            deadline=deadline,
            max_response_bytes=_http_response_bound(config),
            budget=budget,
        )
        catalog = lighter_market_census(raw_metadata, limit=100)
        metadata_envelope = adapter.envelope_from_http(
            raw_metadata,
            factory=factory,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            provenance=http_provenance,
        )
        _append_lighter_bounded(writer, counters, metadata_envelope, config)
        active = tuple(item.market_id for item in catalog if item.status.lower() == "active")
        market_indices = active[: config.census_limit]
        if not market_indices:
            raise LookupError("Lighter metadata exposed no active public market in census")

    websocket_modes = (
        ("normal", LIGHTER_PUBLIC_WEBSOCKET_URL),
        ("readonly", LIGHTER_PUBLIC_READONLY_WEBSOCKET_URL),
    )
    last_connection_error: BaseException | None = None
    for mode, websocket_url in websocket_modes:
        subscriptions = adapter.websocket_subscriptions(
            feeds=config.feeds,
            market_indices=market_indices,
            websocket_url=websocket_url,
        )
        if not subscriptions:
            return (*limitations, "LIGHTER_WEBSOCKET_NOT_REQUESTED")
        connector = UrlWebsocketClientFactory(
            websocket_url,
            queue_capacity=1_024,
            venue="lighter",
            socket_role=f"lighter-public-probe-v1-{mode}",
            allow_environment_proxy=False,
        )
        attempt_started_ns = time.monotonic_ns()
        socket: Any = None
        try:
            budget.consume()
            socket = connector.connect_paused("public", max(0.05, min(10.0, deadline - time.monotonic())))
        except (
            ConnectionError,
            OSError,
            TimeoutError,
            websocket.WebSocketException,
        ) as caught:
            last_connection_error = caught
            status = getattr(caught, "status_code", None)
            message = str(caught).strip()
            connection_attempts.append(
                {
                    "duration_ms": max(0, (time.monotonic_ns() - attempt_started_ns) // 1_000_000),
                    "error_message": message[:4096] or type(caught).__name__,
                    "error_type": type(caught).__name__,
                    "handshake_result": "FAILED_BEFORE_COLLECTION",
                    "http_status": status if type(status) is int else None,
                    "logical_url": websocket_url,
                    "mode": mode,
                }
            )
            continue

        connection_attempts.append(
            {
                "duration_ms": max(0, (time.monotonic_ns() - attempt_started_ns) // 1_000_000),
                "error_message": None,
                "error_type": None,
                "handshake_result": "HTTP_101",
                "http_status": 101,
                "logical_url": websocket_url,
                "mode": mode,
            }
        )
        ws_provenance = CaptureProvenance(
            factory.provenance.collection_id,
            websocket_url,
            "PUBLIC_WEBSOCKET",
        )
        last_ping = time.monotonic()
        last_progress = last_ping
        try:
            for subscription in subscriptions:
                socket.send_json(dict(subscription.payload))
            socket.start_receiving()
            while time.monotonic() < deadline and not stop_requested():
                received = socket.receive(timeout_seconds=1.0)
                snapshot = socket.telemetry_snapshot()
                counters.queue_high_water = max(
                    counters.queue_high_water, int(snapshot.get("queue_high_water", 0))
                )
                now = time.monotonic()
                if received is not None:
                    raw_message = received.raw_message.encode("utf-8")
                    if len(raw_message) > _http_response_bound(config):
                        raise ResearchDataCapacityError("public WebSocket frame exceeds its raw byte bound")
                    envelope = adapter.envelope_from_websocket(
                        raw_message,
                        factory=factory,
                        receive_timestamp_utc_ns=_datetime_to_ns(received.received_time),
                        receive_monotonic_ns=(
                            time.monotonic_ns()
                            if received.received_monotonic_ns is None
                            else received.received_monotonic_ns
                        ),
                        provenance=ws_provenance,
                    )
                    _append_lighter_bounded(writer, counters, envelope, config)
                if now - last_ping >= 60.0:
                    socket.send_json({"type": "ping"})
                    last_ping = now
                if now - last_progress >= config.progress_interval_seconds:
                    progress(writer.frame_count)
                    last_progress = now
        except BufferError:
            snapshot = socket.telemetry_snapshot()
            counters.queue_high_water = max(
                counters.queue_high_water, int(snapshot.get("queue_high_water", 0))
            )
            raise
        except (ConnectionError, OSError, TimeoutError, websocket.WebSocketException) as caught:
            raise ConnectionError(
                "Lighter public WebSocket disconnected after successful handshake; "
                "automatic retry is disabled"
            ) from caught
        finally:
            socket.close()
        if not stop_requested() and time.monotonic() >= deadline:
            raise _ProbeBoundaryReached("MAX_DURATION_REACHED")
        return tuple(limitations)

    raise ConnectionError(
        "Lighter official normal and readonly public WebSocket handshakes failed"
    ) from last_connection_error


def _polymarket_rebootstrap_selected_markets(
    *,
    adapter: PolymarketPublicAdapter,
    token_markets: Sequence[tuple[str, str]],
    resolved_event_ids: set[str],
    selected_feeds: set[str],
    factory: SessionEnvelopeFactory,
    writer: ResearchSegmentWriter,
    counters: _Counters,
    config: ProbeConfig,
    deadline: float,
    stop_requested: Callable[[], bool],
    session: HttpSession,
    budget: _NetworkBudget,
) -> None:
    def require_continuation() -> None:
        if stop_requested():
            raise _ProbeBoundaryReached("INTERRUPTED_RECOVERABLE")

    tokens_by_market: dict[str, set[str]] = {}
    for token, market in token_markets:
        tokens_by_market.setdefault(market, set()).add(token)
    current_event_ids: set[str] = set()
    for market, expected_tokens in sorted(tokens_by_market.items()):
        require_continuation()
        representative_token = sorted(expected_tokens)[0]
        gamma_request = adapter.market_metadata_by_token_request(representative_token)
        gamma_raw = _execute_http(
            session,
            gamma_request,
            deadline=deadline,
            max_response_bytes=_http_response_bound(config),
            budget=budget,
        )
        gamma_markets = _polymarket_keyset_markets(gamma_raw)
        if len(gamma_markets) != 1:
            raise ValueError("Polymarket rebootstrap requires one Gamma market")
        gamma_market = gamma_markets[0]
        gamma_token_outcomes = polymarket_gamma_token_outcomes(gamma_market)
        gamma_tokens = {item[0] for item in gamma_token_outcomes}
        if (
            str(gamma_market.get("conditionId") or "") != market
            or not gamma_tokens.issuperset(expected_tokens)
        ):
            raise ValueError("Polymarket rebootstrap Gamma identity graph diverged")
        event_id = _polymarket_event_id(gamma_market)
        if event_id is not None:
            current_event_ids.add(event_id)
        gamma_envelope = adapter.envelope_from_http(
            gamma_raw,
            feed_type="metadata",
            token_id=representative_token,
            market_id=market,
            factory=factory,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            provenance=_request_provenance(factory, gamma_request),
        )
        _append_probe_bounded(
            writer,
            counters,
            gamma_envelope,
            config,
        )

        require_continuation()
        clob_request = adapter.clob_market_request(market)
        clob_raw = _execute_http(
            session,
            clob_request,
            deadline=deadline,
            max_response_bytes=_http_response_bound(config),
            budget=budget,
        )
        clob_provenance = _request_provenance(factory, clob_request)
        clob_envelope = adapter.envelope_from_http(
            clob_raw,
            feed_type="metadata",
            token_id=None,
            market_id=market,
            factory=factory,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            provenance=clob_provenance,
        )
        _append_probe_bounded(writer, counters, clob_envelope, config)
        clob_decoded = json.loads(clob_raw.decode("utf-8"))
        if not isinstance(clob_decoded, Mapping):
            raise ValueError("Polymarket rebootstrap CLOB market-info must be an object")
        normalize_polymarket_clob_v2_market(
            cast(Mapping[str, object], clob_decoded),
            provenance=clob_provenance,
            expected_condition_id=market,
            expected_token_outcomes=gamma_token_outcomes,
        )

    if "events" in selected_feeds:
        event_ids = current_event_ids | resolved_event_ids
        if not event_ids:
            raise ValueError("Polymarket rebootstrap event identity is unavailable")
        for event_id in sorted(event_ids):
            require_continuation()
            event_request = adapter.event_metadata_request(event_id)
            event_raw = _execute_http(
                session,
                event_request,
                deadline=deadline,
                max_response_bytes=_http_response_bound(config),
                budget=budget,
            )
            event_envelope = adapter.envelope_from_http(
                event_raw,
                feed_type="events",
                token_id=None,
                market_id=f"EVENT:{event_id}",
                factory=factory,
                receive_timestamp_utc_ns=time.time_ns(),
                receive_monotonic_ns=time.monotonic_ns(),
                provenance=_request_provenance(factory, event_request),
            )
            _append_probe_bounded(writer, counters, event_envelope, config)

    rebootstrap_feeds = selected_feeds & {"fees", "tick_size"}
    for token, known_market, feed_type, request in _polymarket_token_parameter_plan(
        adapter,
        token_markets,
        rebootstrap_feeds,
    ):
        require_continuation()
        raw = _execute_http(
            session,
            request,
            deadline=deadline,
            max_response_bytes=_http_response_bound(config),
            budget=budget,
        )
        decoded = json.loads(raw.decode("utf-8"))
        market = known_market
        if isinstance(decoded, Mapping) and decoded.get("market"):
            market = str(decoded["market"])
        envelope = adapter.envelope_from_http(
            raw,
            feed_type=feed_type,
            token_id=token,
            market_id=market,
            factory=factory,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            provenance=_request_provenance(factory, request),
        )
        _append_probe_bounded(writer, counters, envelope, config)


def _polymarket_probe(
    config: ProbeConfig,
    *,
    factory: SessionEnvelopeFactory,
    writer: ResearchSegmentWriter,
    counters: _Counters,
    deadline: float,
    stop_requested: Callable[[], bool],
    progress: Callable[[int], None],
    session: HttpSession,
    budget: _NetworkBudget | None = None,
) -> tuple[str, ...]:
    import websocket

    if budget is None:
        budget = _NetworkBudget(config.max_network_calls)
    adapter = PolymarketPublicAdapter()
    selected_feeds = set(config.feeds)
    limitations: list[str] = []
    market_token_outcomes: dict[str, tuple[tuple[str, str], ...]] = {}
    if config.instruments:
        resolved: list[tuple[str, str]] = []
        resolved_event_ids: set[str] = set()
        for token in config.instruments:
            request = adapter.market_by_token_request(token)
            raw = _execute_http(
                session,
                request,
                deadline=deadline,
                max_response_bytes=_http_response_bound(config),
                budget=budget,
            )
            decoded = json.loads(raw.decode("utf-8"))
            if not isinstance(decoded, Mapping) or not decoded.get("condition_id"):
                raise ValueError("Polymarket token metadata omitted condition_id")
            market = str(decoded["condition_id"])
            resolved.append((token, market))
            envelope = adapter.envelope_from_http(
                raw,
                feed_type="metadata",
                token_id=token,
                market_id=market,
                factory=factory,
                receive_timestamp_utc_ns=time.time_ns(),
                receive_monotonic_ns=time.monotonic_ns(),
                provenance=_request_provenance(factory, request),
            )
            _append_probe_bounded(writer, counters, envelope, config)
            gamma_request = adapter.market_metadata_by_token_request(token)
            gamma_raw = _execute_http(
                session,
                gamma_request,
                deadline=deadline,
                max_response_bytes=_http_response_bound(config),
                budget=budget,
            )
            gamma_markets = _polymarket_keyset_markets(gamma_raw)
            if len(gamma_markets) != 1:
                raise ValueError("Polymarket token must resolve to exactly one Gamma market")
            gamma_market = gamma_markets[0]
            gamma_condition = str(gamma_market.get("conditionId") or "")
            gamma_token_outcomes = polymarket_gamma_token_outcomes(gamma_market)
            gamma_tokens = {item[0] for item in gamma_token_outcomes}
            if (
                gamma_condition != market
                or token not in gamma_tokens
            ):
                raise ValueError("Polymarket CLOB/Gamma token identity graph diverged")
            previous = market_token_outcomes.setdefault(market, gamma_token_outcomes)
            if previous != gamma_token_outcomes:
                raise ValueError("Polymarket Gamma token/outcome graph changed during census")
            gamma_envelope = adapter.envelope_from_http(
                gamma_raw,
                feed_type="metadata",
                token_id=token,
                market_id=market,
                factory=factory,
                receive_timestamp_utc_ns=time.time_ns(),
                receive_monotonic_ns=time.monotonic_ns(),
                provenance=_request_provenance(factory, gamma_request),
            )
            _append_probe_bounded(writer, counters, gamma_envelope, config)
            event_id = _polymarket_event_id(gamma_market)
            if event_id is not None:
                resolved_event_ids.add(event_id)
        token_markets = tuple(resolved)
    else:
        resolved_event_ids = set()
        pager = BoundedCursorPager(
            max_pages=max(1, min(config.max_network_calls, config.census_limit)),
            max_items=config.census_limit,
        )
        cursor: str | None = None
        selected_token_markets: list[tuple[str, str]] = []
        while pager.items < config.census_limit:
            page_limit = min(20, config.census_limit - pager.items)
            census_request = adapter.market_census_request(limit=page_limit, after_cursor=cursor)
            census = _execute_http(
                session,
                census_request,
                deadline=deadline,
                max_response_bytes=_http_response_bound(config),
                budget=budget,
            )
            census_envelope = adapter.envelope_from_http(
                census,
                feed_type="metadata",
                token_id=None,
                market_id="CENSUS",
                factory=factory,
                receive_timestamp_utc_ns=time.time_ns(),
                receive_monotonic_ns=time.monotonic_ns(),
                provenance=_request_provenance(factory, census_request),
            )
            _append_probe_bounded(writer, counters, census_envelope, config)
            markets = _polymarket_keyset_markets(census)
            next_cursor = _next_cursor(census, key="next_cursor")
            market_ids = [str(item.get("conditionId") or "") for item in markets]
            admitted = set(
                pager.admit(
                    requested_cursor=cursor,
                    next_cursor=next_cursor,
                    item_ids=market_ids,
                )
            )
            for census_market in markets:
                if str(census_market.get("conditionId") or "") not in admitted:
                    continue
                event_id = _polymarket_event_id(census_market)
                if event_id is not None:
                    resolved_event_ids.add(event_id)
                single_page = _raw_json({"markets": [census_market], "next_cursor": None})
                selected_token_markets.extend(_polymarket_tokens(single_page, limit=2))
                token_outcomes = polymarket_gamma_token_outcomes(census_market)
                previous = market_token_outcomes.setdefault(
                    str(census_market["conditionId"]), token_outcomes
                )
                if previous != token_outcomes:
                    raise ValueError("Polymarket Gamma token/outcome graph is duplicated")
            if next_cursor is None or pager.items >= config.census_limit:
                break
            cursor = next_cursor
        token_markets = tuple(selected_token_markets)
    if not token_markets:
        raise LookupError("Polymarket census returned no public CLOB token")
    for market in sorted({item[1] for item in token_markets}):
        expected_token_outcomes = market_token_outcomes.get(market)
        if expected_token_outcomes is None:
            raise ValueError("Polymarket Gamma token/outcome graph is absent")
        clob_request = adapter.clob_market_request(market)
        clob_raw = _execute_http(
            session,
            clob_request,
            deadline=deadline,
            max_response_bytes=_http_response_bound(config),
            budget=budget,
        )
        clob_provenance = _request_provenance(factory, clob_request)
        clob_envelope = adapter.envelope_from_http(
            clob_raw,
            feed_type="metadata",
            token_id=None,
            market_id=market,
            factory=factory,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            provenance=clob_provenance,
        )
        _append_probe_bounded(writer, counters, clob_envelope, config)
        clob_decoded = json.loads(clob_raw.decode("utf-8"))
        if not isinstance(clob_decoded, Mapping):
            raise ValueError("Polymarket CLOB market-info must be an object")
        normalize_polymarket_clob_v2_market(
            cast(Mapping[str, object], clob_decoded),
            provenance=clob_provenance,
            expected_condition_id=market,
            expected_token_outcomes=expected_token_outcomes,
        )
    if "events" in selected_feeds:
        if resolved_event_ids:
            for event_id in sorted(resolved_event_ids):
                event_request = adapter.event_metadata_request(event_id)
                event_raw = _execute_http(
                    session,
                    event_request,
                    deadline=deadline,
                    max_response_bytes=_http_response_bound(config),
                    budget=budget,
                )
                event_envelope = adapter.envelope_from_http(
                    event_raw,
                    feed_type="events",
                    token_id=None,
                    market_id=f"EVENT:{event_id}",
                    factory=factory,
                    receive_timestamp_utc_ns=time.time_ns(),
                    receive_monotonic_ns=time.monotonic_ns(),
                    provenance=_request_provenance(factory, event_request),
                )
                _append_probe_bounded(writer, counters, event_envelope, config)
        else:
            limitations.append("POLYMARKET_EVENT_ID_UNKNOWN_NOT_OBSERVED")
    token_http_feeds = selected_feeds & {
        "fees",
        "last_trade_price",
        "order_book",
        "tick_size",
    }
    for token, known_market, feed_type, request in _polymarket_token_parameter_plan(
        adapter,
        token_markets,
        token_http_feeds,
    ):
        raw = _execute_http(
            session,
            request,
            deadline=deadline,
            max_response_bytes=_http_response_bound(config),
            budget=budget,
        )
        decoded = json.loads(raw.decode("utf-8"))
        market = known_market
        if isinstance(decoded, Mapping) and decoded.get("market"):
            market = str(decoded["market"])
        envelope = adapter.envelope_from_http(
            raw,
            feed_type=feed_type,
            token_id=token,
            market_id=market,
            factory=factory,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            provenance=_request_provenance(factory, request),
        )
        _append_probe_bounded(writer, counters, envelope, config)
    if "public_trades" in selected_feeds:
        for market in sorted({market for _, market in token_markets}):
            offset = 0
            max_trade_pages = max(1, min(5, config.max_network_calls))
            for _page_index in range(max_trade_pages):
                request = adapter.public_trade_request((market,), limit=100, offset=offset)
                raw = _execute_http(
                    session,
                    request,
                    deadline=deadline,
                    max_response_bytes=_http_response_bound(config),
                    budget=budget,
                )
                decoded_trades = json.loads(raw.decode("utf-8"))
                if not isinstance(decoded_trades, list):
                    raise ValueError("Polymarket public trades page must be an array")
                envelope = adapter.envelope_from_http(
                    raw,
                    feed_type="public_trades",
                    token_id=None,
                    market_id=market,
                    factory=factory,
                    receive_timestamp_utc_ns=time.time_ns(),
                    receive_monotonic_ns=time.monotonic_ns(),
                    provenance=_request_provenance(factory, request),
                )
                _append_probe_bounded(writer, counters, envelope, config)
                if len(decoded_trades) < 100:
                    break
                offset += 100
            else:
                limitations.append("POLYMARKET_TRADES_PAGE_CAP_REACHED")
    token_ids = tuple(token for token, _ in token_markets)
    websocket_feeds = selected_feeds & {
        "best_bid_ask",
        "last_trade_price",
        "market_lifecycle",
        "order_book",
        "price_change",
        "tick_size_change",
    }
    if not websocket_feeds:
        return tuple(limitations)
    subscription = adapter.websocket_subscription(token_ids)
    ws_provenance = CaptureProvenance(
        factory.provenance.collection_id,
        POLYMARKET_PUBLIC_WEBSOCKET_URL,
        "PUBLIC_WEBSOCKET",
    )
    delays = (0.25, 0.5, 1.0, 2.0, 4.0)
    attempt = 0
    connected_once = False
    needs_rebootstrap = False
    last_connection_error: BaseException | None = None
    last_progress = time.monotonic()
    last_ping = last_progress
    connection: Any = None
    try:
        while time.monotonic() < deadline and not stop_requested():
            if connection is None:
                try:
                    budget.consume()
                    connection = websocket.create_connection(
                        POLYMARKET_PUBLIC_WEBSOCKET_URL,
                        timeout=max(0.05, min(10.0, deadline - time.monotonic())),
                        enable_multithread=False,
                        redirect_limit=0,
                        http_no_proxy=["*"],
                    )
                    getstatus = getattr(connection, "getstatus", None)
                    if not callable(getstatus) or getstatus() != 101:
                        raise ConnectionError("Polymarket public WebSocket did not return HTTP 101")
                    if needs_rebootstrap:
                        reconnect_control = factory.make(
                            feed_type="heartbeat",
                            instrument_id="PM:GLOBAL",
                            market_id="PM:GLOBAL",
                            source_timestamp_ns=None,
                            receive_timestamp_utc_ns=time.time_ns(),
                            receive_monotonic_ns=time.monotonic_ns(),
                            raw_payload=canonical_json_bytes(
                                {
                                    "control": "RECONNECT_BOUNDARY",
                                    "session_identity": factory.session_identity,
                                }
                            ),
                            source_sequence=None,
                            source_event_id=f"{factory.session_identity}:reconnect-boundary",
                            provenance=ws_provenance,
                            infer_source_sequence_continuity=False,
                        )
                        _append_probe_bounded(
                            writer,
                            counters,
                            reconnect_control,
                            config,
                            allow_polymarket_reconnect_control=True,
                        )
                        _polymarket_rebootstrap_selected_markets(
                            adapter=adapter,
                            token_markets=token_markets,
                            resolved_event_ids=resolved_event_ids,
                            selected_feeds=selected_feeds,
                            factory=factory,
                            writer=writer,
                            counters=counters,
                            config=config,
                            deadline=deadline,
                            stop_requested=stop_requested,
                            session=session,
                            budget=budget,
                        )
                        needs_rebootstrap = False
                    if stop_requested():
                        raise _ProbeBoundaryReached("INTERRUPTED_RECOVERABLE")
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise _ProbeBoundaryReached("MAX_DURATION_REACHED")
                    connection.settimeout(max(0.001, min(1.0, remaining)))
                    connection.send(canonical_json_bytes(subscription.payload).decode("utf-8"))
                    attempt = 0
                    connected_once = True
                    last_connection_error = None
                except (
                    ConnectionError,
                    OSError,
                    TimeoutError,
                    websocket.WebSocketException,
                ) as caught:
                    last_connection_error = caught
                    if connection is not None:
                        connection.close()
                        connection = None
                    counters.begin_reconnect(factory)
                    needs_rebootstrap = True
                    remaining = max(0.0, deadline - time.monotonic())
                    if remaining > 0:
                        time.sleep(min(delays[min(attempt, len(delays) - 1)], remaining))
                    attempt += 1
                    continue
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise _ProbeBoundaryReached("MAX_DURATION_REACHED")
                connection.settimeout(max(0.001, min(1.0, remaining)))
                message = connection.recv()
                if time.monotonic() > deadline:
                    raise _ProbeBoundaryReached("MAX_DURATION_REACHED")
                now_mono_ns = time.monotonic_ns()
                now_utc_ns = time.time_ns()
                if isinstance(message, bytes):
                    raw = message
                elif isinstance(message, str):
                    raw = message.encode("utf-8")
                else:
                    raise TypeError("Polymarket public WebSocket returned a non-text frame")
                record_envelopes: tuple[Any, ...]
                if raw in {b"PING", b"PONG"}:
                    record_envelopes = (
                        factory.make(
                            feed_type="heartbeat",
                            instrument_id="PM:GLOBAL",
                            market_id="PM:GLOBAL",
                            source_timestamp_ns=None,
                            receive_timestamp_utc_ns=now_utc_ns,
                            receive_monotonic_ns=now_mono_ns,
                            raw_payload=raw,
                            source_sequence=None,
                            provenance=ws_provenance,
                        ),
                    )
                    if raw == b"PING":
                        connection.send("PONG")
                else:
                    if not raw:
                        raise ConnectionError("Polymarket public WebSocket closed")
                    if len(raw) > _http_response_bound(config):
                        raise ResearchDataCapacityError("public WebSocket frame exceeds its raw byte bound")
                    record_envelopes = (
                        adapter.envelope_from_websocket(
                            raw,
                            factory=factory,
                            receive_timestamp_utc_ns=now_utc_ns,
                            receive_monotonic_ns=now_mono_ns,
                            provenance=ws_provenance,
                        ),
                    )
                for envelope in record_envelopes:
                    _require_polymarket_websocket_selection(envelope, websocket_feeds)
                    _append_probe_bounded(writer, counters, envelope, config)
                counters.queue_high_water = max(counters.queue_high_water, 1)
            except websocket.WebSocketTimeoutException:
                pass
            except (ConnectionError, OSError, websocket.WebSocketException):
                connection.close()
                connection = None
                counters.begin_reconnect(factory)
                needs_rebootstrap = True
            now = time.monotonic()
            if connection is not None and now - last_ping >= 10.0:
                try:
                    connection.send("PING")
                    last_ping = now
                except (ConnectionError, OSError, websocket.WebSocketException):
                    connection.close()
                    connection = None
                    counters.begin_reconnect(factory)
                    needs_rebootstrap = True
            if now - last_progress >= config.progress_interval_seconds:
                progress(writer.frame_count)
                last_progress = now
    finally:
        if connection is not None:
            connection.close()
    if not connected_once and last_connection_error is not None and not stop_requested():
        raise ConnectionError(
            "Polymarket public WebSocket never connected: "
            f"{type(last_connection_error).__name__}: {last_connection_error}"
        ) from last_connection_error
    if not stop_requested() and connected_once and (needs_rebootstrap or connection is None):
        raise _ProbeBoundaryReached("CONTINUITY_UNKNOWN_AFTER_RECONNECT_FROZEN")
    return tuple(limitations)


def _kalshi_probe(
    config: ProbeConfig,
    *,
    factory: SessionEnvelopeFactory,
    writer: ResearchSegmentWriter,
    counters: _Counters,
    deadline: float,
    stop_requested: Callable[[], bool],
    progress: Callable[[int], None],
    session: HttpSession,
    budget: _NetworkBudget | None = None,
) -> tuple[str, ...]:
    if budget is None:
        budget = _NetworkBudget(config.max_network_calls)
    adapter = KalshiPublicAdapter()
    selected = set(config.feeds)
    limitations = [
        adapter.websocket_limitation,
        "KALSHI_REST_BOOK_HAS_NO_DOCUMENTED_SOURCE_TIMESTAMP_OR_SEQUENCE",
        "KALSHI_CONTINUITY_CANNOT_BE_PROVEN_BY_GAPS_ZERO",
        "KALSHI_EXACT_FEES_REQUIRE_SERIES_AND_EVENT_SCHEDULE_BINDING",
    ]

    def capture(request: PublicHttpRequest, *, feed_type: str, identity: str) -> bytes:
        raw = _execute_http(
            session,
            request,
            deadline=deadline,
            max_response_bytes=_http_response_bound(config),
            budget=budget,
        )
        envelope = adapter.envelope_from_http(
            raw,
            feed_type=feed_type,
            ticker=identity,
            factory=factory,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            provenance=_request_provenance(factory, request),
        )
        _append_probe_bounded(writer, counters, envelope, config)
        return raw

    def capture_fee_histories(
        *,
        ticker: str,
        event_ticker: str,
        series_ticker: str,
    ) -> None:
        if "fee_changes" in selected:
            fee_request = adapter.requests_for_market(
                ticker=ticker,
                event_ticker=event_ticker,
                series_ticker=series_ticker,
                feeds=("fee_changes",),
            )[0]
            capture(fee_request, feed_type="fee_changes", identity=series_ticker)
        if "event_fee_changes" not in selected:
            return
        fee_cursor: str | None = None
        fee_pager = BoundedCursorPager(max_pages=5, max_items=5_000)
        while True:
            fee_request = adapter.event_fee_changes_request(event_ticker, cursor=fee_cursor)
            fee_raw = capture(
                fee_request,
                feed_type="event_fee_changes",
                identity=event_ticker,
            )
            decoded_fee = json.loads(fee_raw.decode("utf-8"))
            if not isinstance(decoded_fee, Mapping):
                raise ValueError("Kalshi event fee-change page must be an object")
            fee_items = decoded_fee.get("event_fee_changes")
            if not isinstance(fee_items, list):
                raise ValueError("Kalshi event fee-change records must be an array")
            if any(not isinstance(item, Mapping) for item in fee_items):
                raise ValueError("Kalshi event fee-change records must be objects")
            next_fee_cursor = _next_cursor(fee_raw, key="cursor")
            if next_fee_cursor is None:
                next_fee_cursor = _next_cursor(fee_raw, key="next_cursor")
            fee_pager.admit(
                requested_cursor=fee_cursor,
                next_cursor=next_fee_cursor,
                item_ids=[
                    hashlib.sha256(_raw_json(item)).hexdigest()
                    for item in fee_items
                ],
            )
            if next_fee_cursor is None:
                break
            if fee_pager.pages >= fee_pager.max_pages:
                limitations.append("KALSHI_EVENT_FEE_PAGE_CAP_REACHED")
                break
            fee_cursor = next_fee_cursor

    for global_feed in ("exchange_status", "exchange_schedule"):
        if global_feed not in selected:
            continue
        global_request = adapter.requests_for_market(
            ticker="GLOBAL",
            event_ticker=None,
            series_ticker=None,
            feeds=(global_feed,),
        )[0]
        capture(global_request, feed_type=global_feed, identity="GLOBAL")

    if "incentives" in selected:
        incentive_cursor: str | None = None
        incentive_pager = BoundedCursorPager(max_pages=5, max_items=5_000)
        while True:
            incentive_request = adapter.incentive_request(limit=1000, cursor=incentive_cursor)
            incentive_raw = capture(incentive_request, feed_type="incentives", identity="GLOBAL")
            decoded_incentives = json.loads(incentive_raw.decode("utf-8"))
            if not isinstance(decoded_incentives, Mapping):
                raise ValueError("Kalshi incentives page must be an object")
            programs = decoded_incentives.get("incentive_programs")
            if not isinstance(programs, list):
                raise ValueError("Kalshi incentives page omitted its records")
            if any(not isinstance(item, Mapping) for item in programs):
                raise ValueError("Kalshi incentive records must be objects")
            item_ids: list[str] = []
            for item in programs:
                assert isinstance(item, Mapping)
                identity = item.get("id") or item.get("incentive_id")
                if identity is None:
                    identity = hashlib.sha256(_raw_json(item)).hexdigest()
                if type(identity) is not str or not identity:
                    raise ValueError("Kalshi incentive identity must be exact text")
                item_ids.append(identity)
            next_incentive_cursor = _next_cursor(incentive_raw, key="next_cursor")
            incentive_pager.admit(
                requested_cursor=incentive_cursor,
                next_cursor=next_incentive_cursor,
                item_ids=item_ids,
            )
            if next_incentive_cursor is None:
                break
            if incentive_pager.pages >= incentive_pager.max_pages:
                limitations.append("KALSHI_INCENTIVES_PAGE_CAP_REACHED")
                break
            incentive_cursor = next_incentive_cursor

    scopes: list[tuple[str, str | None, str | None]] = []
    current_specific_feeds = {
        "block_trades",
        "event_fee_changes",
        "fee_changes",
        "markets",
        "order_book",
        "trades",
    }
    historical_requested = bool(
        selected & {"historical_cutoff", "historical_markets", "historical_trades"}
    )
    shared_graph_feeds = {"event_metadata", "events", "series"}
    historical_only = historical_requested and not bool(
        selected & {"block_trades", "markets", "order_book", "trades"}
    )
    current_scope_requested = bool(
        selected & current_specific_feeds and not historical_only
    ) or bool(not historical_requested and selected & shared_graph_feeds)
    if config.instruments and current_scope_requested:
        for requested_ticker in config.instruments:
            market_request = adapter.requests_for_market(
                ticker=requested_ticker,
                event_ticker=None,
                series_ticker=None,
                feeds=("markets",),
            )[0]
            market_raw = capture(market_request, feed_type="markets", identity=requested_ticker)
            scope = _kalshi_markets(market_raw, limit=1)[0]
            if scope[0] != requested_ticker:
                raise ValueError("Kalshi explicit market metadata returned another ticker")
            scopes.append(scope)
    elif current_scope_requested:
        market_pager = BoundedCursorPager(
            max_pages=max(1, min(config.max_network_calls, config.census_limit)),
            max_items=config.census_limit,
        )
        market_cursor: str | None = None
        while market_pager.items < config.census_limit:
            market_request = adapter.market_census_request(
                limit=min(50, config.census_limit - market_pager.items),
                cursor=market_cursor,
            )
            census = capture(market_request, feed_type="markets", identity="CENSUS")
            page_scopes = _kalshi_markets(census, limit=config.census_limit)
            next_market_cursor = _next_cursor(census, key="cursor")
            if next_market_cursor is None:
                next_market_cursor = _next_cursor(census, key="next_cursor")
            admitted = market_pager.admit(
                requested_cursor=market_cursor,
                next_cursor=next_market_cursor,
                item_ids=[item[0] for item in page_scopes],
            )
            scopes.extend(_admitted_kalshi_scopes(page_scopes, admitted))
            if next_market_cursor is None or market_pager.items >= config.census_limit:
                break
            market_cursor = next_market_cursor

    historical_scopes: list[tuple[str, str | None, str | None]] = []
    if selected & {"historical_cutoff", "historical_markets", "historical_trades"}:
        cutoff_request = adapter.historical_cutoff_request()
        capture(cutoff_request, feed_type="historical_cutoff", identity="GLOBAL")
    if selected & {"historical_markets", "historical_trades"}:
        if config.instruments:
            historical_scopes.extend((ticker, None, None) for ticker in config.instruments)
        else:
            historical_pager = BoundedCursorPager(
                max_pages=max(1, min(5, config.max_network_calls)),
                max_items=max(1, config.census_limit),
            )
            historical_cursor: str | None = None
            while historical_pager.items < max(1, config.census_limit):
                historical_request = adapter.historical_market_census_request(
                    limit=min(50, max(1, config.census_limit) - historical_pager.items),
                    cursor=historical_cursor,
                )
                historical_raw = capture(
                    historical_request,
                    feed_type="historical_markets",
                    identity="HISTORICAL_CENSUS",
                )
                page_scopes = _kalshi_markets(historical_raw, limit=max(1, config.census_limit))
                next_historical_cursor = _next_cursor(historical_raw, key="cursor")
                if next_historical_cursor is None:
                    next_historical_cursor = _next_cursor(historical_raw, key="next_cursor")
                admitted = historical_pager.admit(
                    requested_cursor=historical_cursor,
                    next_cursor=next_historical_cursor,
                    item_ids=[item[0] for item in page_scopes],
                )
                historical_scopes.extend(_admitted_kalshi_scopes(page_scopes, admitted))
                if next_historical_cursor is None or historical_pager.items >= max(1, config.census_limit):
                    break
                historical_cursor = next_historical_cursor

    resolved_scopes: list[tuple[str, str | None, str | None]] = []
    for ticker, event_ticker, series_ticker in scopes:
        graph_required = bool(
            selected
            & {
                "block_trades",
                "event_fee_changes",
                "event_metadata",
                "events",
                "fee_changes",
                "markets",
                "order_book",
                "series",
                "trades",
            }
        )
        if event_ticker and graph_required:
            event_request = adapter.requests_for_market(
                ticker=ticker,
                event_ticker=event_ticker,
                series_ticker=None,
                feeds=("events",),
            )[0]
            event_raw = capture(event_request, feed_type="events", identity=event_ticker)
            series_ticker = _kalshi_series(
                event_raw,
                expected_event_ticker=event_ticker,
            )
            metadata_request = adapter.requests_for_market(
                ticker=ticker,
                event_ticker=event_ticker,
                series_ticker=series_ticker,
                feeds=("event_metadata",),
            )[0]
            metadata_raw = capture(
                metadata_request,
                feed_type="event_metadata",
                identity=event_ticker,
            )
            _kalshi_event_metadata_record(
                metadata_raw,
                provenance=_request_provenance(factory, metadata_request),
                expected_event_ticker=event_ticker,
                expected_market_ticker=ticker,
            )
            series_request = adapter.requests_for_market(
                ticker=ticker,
                event_ticker=event_ticker,
                series_ticker=series_ticker,
                feeds=("series",),
            )[0]
            series_raw = capture(series_request, feed_type="series", identity=series_ticker)
            _kalshi_series_record(
                series_raw,
                expected_series_ticker=series_ticker,
            )
        if graph_required and event_ticker is None:
            raise ValueError(f"KALSHI_GRAPH_EVENT_TICKER_UNAVAILABLE:{ticker}")
        if graph_required and series_ticker is None:
            raise ValueError(f"KALSHI_GRAPH_SERIES_TICKER_UNAVAILABLE:{ticker}")
        resolved_scopes.append((ticker, event_ticker, series_ticker))

        if event_ticker is not None and series_ticker is not None:
            capture_fee_histories(
                ticker=ticker,
                event_ticker=event_ticker,
                series_ticker=series_ticker,
            )

    for ticker, event_ticker, series_ticker in historical_scopes:
        historical_market_request = adapter.historical_market_request(ticker)
        historical_market_raw = capture(
            historical_market_request,
            feed_type="historical_markets",
            identity=ticker,
        )
        historical_scope = _kalshi_markets(historical_market_raw, limit=1)[0]
        if historical_scope[0] != ticker:
            raise ValueError("Kalshi historical market returned another ticker")
        event_ticker = historical_scope[1] or event_ticker
        if event_ticker is not None:
            event_request = adapter.requests_for_market(
                ticker=ticker,
                event_ticker=event_ticker,
                series_ticker=None,
                feeds=("events",),
            )[0]
            event_raw = capture(event_request, feed_type="events", identity=event_ticker)
            series_ticker = _kalshi_series(
                event_raw,
                expected_event_ticker=event_ticker,
            )
            metadata_request = adapter.requests_for_market(
                ticker=ticker,
                event_ticker=event_ticker,
                series_ticker=series_ticker,
                feeds=("event_metadata",),
            )[0]
            metadata_raw = capture(
                metadata_request,
                feed_type="event_metadata",
                identity=event_ticker,
            )
            _kalshi_event_metadata_record(
                metadata_raw,
                provenance=_request_provenance(factory, metadata_request),
                expected_event_ticker=event_ticker,
                expected_market_ticker=ticker,
            )
            series_request = adapter.requests_for_market(
                ticker=ticker,
                event_ticker=event_ticker,
                series_ticker=series_ticker,
                feeds=("series",),
            )[0]
            series_raw = capture(series_request, feed_type="series", identity=series_ticker)
            _kalshi_series_record(
                series_raw,
                expected_series_ticker=series_ticker,
            )
        else:
            raise ValueError(f"KALSHI_HISTORICAL_EVENT_TICKER_UNAVAILABLE:{ticker}")
        assert event_ticker is not None and series_ticker is not None
        capture_fee_histories(
            ticker=ticker,
            event_ticker=event_ticker,
            series_ticker=series_ticker,
        )
        if "historical_trades" not in selected:
            continue
        historical_trade_cursor: str | None = None
        historical_trade_pager = BoundedCursorPager(max_pages=5, max_items=5_000)
        while True:
            historical_trade_request = adapter.historical_trade_request(
                ticker, cursor=historical_trade_cursor
            )
            historical_trade_raw = capture(
                historical_trade_request,
                feed_type="historical_trades",
                identity=ticker,
            )
            decoded_historical_trades = json.loads(historical_trade_raw.decode("utf-8"))
            if not isinstance(decoded_historical_trades, Mapping) or not isinstance(
                decoded_historical_trades.get("trades"), list
            ):
                raise ValueError("Kalshi historical trades page is invalid")
            historical_trades = decoded_historical_trades["trades"]
            next_historical_trade_cursor = _next_cursor(historical_trade_raw, key="cursor")
            if next_historical_trade_cursor is None:
                next_historical_trade_cursor = _next_cursor(historical_trade_raw, key="next_cursor")
            historical_trade_pager.admit(
                requested_cursor=historical_trade_cursor,
                next_cursor=next_historical_trade_cursor,
                item_ids=_kalshi_trade_ids(
                    historical_trades,
                    expected_ticker=ticker,
                    expected_block_trade=None,
                ),
            )
            if next_historical_trade_cursor is None:
                break
            if historical_trade_pager.pages >= historical_trade_pager.max_pages:
                limitations.append("KALSHI_HISTORICAL_TRADES_PAGE_CAP_REACHED")
                break
            historical_trade_cursor = next_historical_trade_cursor

    last_progress = time.monotonic()
    first_poll = True
    lifecycle_next_poll = {ticker: 0.0 for ticker, _event, _series in resolved_scopes}
    lifecycle_terminal: set[str] = set()
    while time.monotonic() < deadline and not stop_requested():
        for ticker, event_ticker, series_ticker in resolved_scopes:
            now = time.monotonic()
            if (
                "markets" in selected
                and ticker not in lifecycle_terminal
                and now >= lifecycle_next_poll[ticker]
            ):
                lifecycle_request = adapter.requests_for_market(
                    ticker=ticker,
                    event_ticker=event_ticker,
                    series_ticker=series_ticker,
                    feeds=("markets",),
                )[0]
                lifecycle_raw = capture(
                    lifecycle_request,
                    feed_type="markets",
                    identity=ticker,
                )
                lifecycle_market = _kalshi_market_record(
                    lifecycle_raw,
                    expected_ticker=ticker,
                    expected_event_ticker=event_ticker,
                )
                lifecycle_next_poll[ticker] = now + max(
                    10.0,
                    config.progress_interval_seconds,
                )
                if str(lifecycle_market.get("status") or "").lower() == "finalized":
                    result = str(lifecycle_market.get("result") or "").lower()
                    settlement_value = lifecycle_market.get("settlement_value_dollars")
                    settlement_ts = lifecycle_market.get("settlement_ts")
                    try:
                        settlement_decimal = Decimal(str(settlement_value))
                    except Exception:
                        settlement_decimal = Decimal("NaN")
                    try:
                        settlement_time_ns = prediction_rfc3339_to_ns(
                            settlement_ts,
                            label="Kalshi settlement_ts",
                        )
                    except ValueError:
                        settlement_time_ns = None
                    if (
                        result in {"yes", "no"}
                        and settlement_decimal.is_finite()
                        and settlement_decimal
                        == (Decimal("1") if result == "yes" else Decimal("0"))
                        and settlement_time_ns is not None
                    ):
                        lifecycle_terminal.add(ticker)
                    else:
                        limitations.append(
                            f"KALSHI_FINALIZED_WITHOUT_RESULT_OR_SETTLEMENT:{ticker}"
                        )
            selected_poll_feeds = (
                config.feeds
                if first_poll
                else tuple(feed for feed in config.feeds if feed in {"block_trades", "order_book", "trades"})
            )
            selected_poll_feeds = tuple(
                feed
                for feed in selected_poll_feeds
                if feed
                not in {
                    "event_fee_changes",
                    "event_metadata",
                    "events",
                    "exchange_schedule",
                    "exchange_status",
                    "fee_changes",
                    "historical_cutoff",
                    "historical_markets",
                    "historical_trades",
                    "incentives",
                    "markets",
                    "series",
                }
            )
            for trade_feed, is_block_trade in (
                ("trades", False),
                ("block_trades", True),
            ):
                if trade_feed not in selected_poll_feeds:
                    continue
                trade_cursor: str | None = None
                trade_pager = BoundedCursorPager(max_pages=5, max_items=5_000)
                while True:
                    trade_request = adapter.trade_request(
                        ticker,
                        block_trade=is_block_trade,
                        cursor=trade_cursor,
                    )
                    trade_raw = capture(trade_request, feed_type=trade_feed, identity=ticker)
                    decoded_trades = json.loads(trade_raw.decode("utf-8"))
                    if not isinstance(decoded_trades, Mapping) or not isinstance(
                        decoded_trades.get("trades"), list
                    ):
                        raise ValueError("Kalshi trades page is invalid")
                    trade_items = decoded_trades["trades"]
                    next_trade_cursor = _next_cursor(trade_raw, key="cursor")
                    if next_trade_cursor is None:
                        next_trade_cursor = _next_cursor(trade_raw, key="next_cursor")
                    trade_pager.admit(
                        requested_cursor=trade_cursor,
                        next_cursor=next_trade_cursor,
                        item_ids=_kalshi_trade_ids(
                            trade_items,
                            expected_ticker=ticker,
                            expected_block_trade=is_block_trade,
                        ),
                    )
                    if next_trade_cursor is None:
                        break
                    if trade_pager.pages >= trade_pager.max_pages:
                        limitations.append(f"KALSHI_{trade_feed.upper()}_PAGE_CAP_REACHED")
                        break
                    trade_cursor = next_trade_cursor
            selected_poll_feeds = tuple(
                feed for feed in selected_poll_feeds if feed not in {"block_trades", "trades"}
            )
            requests = adapter.requests_for_market(
                ticker=ticker,
                event_ticker=event_ticker,
                series_ticker=series_ticker,
                feeds=selected_poll_feeds,
            )
            for request in requests:
                path = request.url.removeprefix(KALSHI_PUBLIC_HTTP_URL).strip("/")
                if "orderbook" in path:
                    feed_type = "order_book"
                elif path == "markets/trades":
                    feed_type = (
                        "block_trades" if dict(request.query).get("is_block_trade") == "true" else "trades"
                    )
                elif path == "events/fee_changes":
                    feed_type = "event_fee_changes"
                elif path.endswith("/metadata") and path.startswith("events/"):
                    feed_type = "event_metadata"
                elif path.startswith("events/"):
                    feed_type = "events"
                elif path.startswith("series/fee_changes"):
                    feed_type = "fee_changes"
                elif path.startswith("series/"):
                    feed_type = "series"
                elif path.startswith("incentive_programs"):
                    feed_type = "incentives"
                elif path == "exchange/status":
                    feed_type = "exchange_status"
                elif path == "exchange/schedule":
                    feed_type = "exchange_schedule"
                else:
                    feed_type = "markets"
                identity = ticker
                if feed_type in {"events", "event_fee_changes", "event_metadata"}:
                    identity = event_ticker or ticker
                elif feed_type in {"fee_changes", "series"}:
                    identity = series_ticker or ticker
                capture(request, feed_type=feed_type, identity=identity)
        first_poll = False
        lifecycle_pending = bool(
            "markets" in selected
            and resolved_scopes
            and lifecycle_terminal
            != {ticker for ticker, _event, _series in resolved_scopes}
        )
        if not (selected & {"block_trades", "order_book", "trades"}) and not lifecycle_pending:
            return tuple(dict.fromkeys(limitations))
        now = time.monotonic()
        if now - last_progress >= config.progress_interval_seconds:
            progress(writer.frame_count)
            last_progress = now
        remaining = deadline - now
        if remaining > 0 and not stop_requested():
            time.sleep(min(2.0, remaining))
    return tuple(dict.fromkeys(limitations))


def run_public_probe(
    config: ProbeConfig,
    *,
    stop_requested: Callable[[], bool] = lambda: False,
    progress: Callable[[int], None] = lambda _count: None,
    http_session_factory: Callable[[], HttpSession] = _default_http_session,
) -> ProbeReport:
    """Run one bounded, credential-free probe and publish immutable raw evidence."""

    if config.output_root.exists():
        raise FileExistsError("probe output root must be new")
    config.output_root.mkdir(parents=True)
    reports_root = config.output_root / "reports"
    reports_root.mkdir()
    raw_root = config.output_root / "raw"
    collection_id = config.collection_id or f"rdp-{config.venue.value}-{uuid4().hex}"
    probe_binding_payload = _probe_binding_payload(config, collection_id=collection_id)
    probe_binding_sha256 = _probe_binding_sha256(probe_binding_payload)
    source_versions = {
        Venue.HYPERLIQUID: HYPERLIQUID_METADATA_VERSION,
        Venue.LIGHTER: LIGHTER_METADATA_VERSION,
        Venue.POLYMARKET: POLYMARKET_METADATA_VERSION,
        Venue.KALSHI: KALSHI_METADATA_VERSION,
    }
    source_urls = {
        Venue.HYPERLIQUID: HYPERLIQUID_PUBLIC_HTTP_URL,
        Venue.LIGHTER: LIGHTER_PUBLIC_HTTP_URL,
        Venue.POLYMARKET: POLYMARKET_GAMMA_PUBLIC_URL,
        Venue.KALSHI: KALSHI_PUBLIC_HTTP_URL,
    }
    factory = SessionEnvelopeFactory(
        venue=config.venue,
        collector_identity="hyperlab-research-data-plane-v1",
        session_identity=f"probe-binding-{probe_binding_sha256}",
        source_metadata_version=source_versions[config.venue],
        provenance=CaptureProvenance(
            collection_id,
            source_urls[config.venue],
            "PUBLIC_HTTP",
        ),
    )
    counters = _Counters()
    budget = _NetworkBudget(config.max_network_calls)
    started = time.monotonic()
    terminal = "COMPLETE"
    error: str | None = None
    limitations: tuple[str, ...] = ()
    connection_attempts: list[dict[str, CanonicalValue]] = []
    manifest: ManifestRecord | None = None
    selected_http_session_factory = (
        _default_lighter_http_session
        if config.venue is Venue.LIGHTER and http_session_factory is _default_http_session
        else http_session_factory
    )
    session = selected_http_session_factory()
    writer = ResearchSegmentWriter(
        raw_root,
        collection_id=collection_id,
        max_segment_bytes=config.max_segment_bytes,
        rotation_seconds=config.rotation_seconds,
        max_total_bytes=config.max_bytes,
    )
    deadline = started + config.duration_seconds
    if config.collection_cutoff_utc_ns_exclusive is not None:
        cutoff_remaining_ns = config.collection_cutoff_utc_ns_exclusive - time.time_ns()
        deadline = min(deadline, started + max(cutoff_remaining_ns, 0) / 1_000_000_000)
    collection_stopped = started

    def publish_running(frame_count: int) -> None:
        timestamps = counters.source_timestamps
        running: dict[str, object] = {
            "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
            "bytes": writer.stored_segment_bytes,
            "binding_status": "PLANNED_UNAUTHENTICATED",
            "campaign_manifest_sha256": None,
            "candidate_config_sha256": None,
            "collection_id": collection_id,
            "connection_attempts": list(connection_attempts),
            "duplicates": counters.duplicates,
            "elapsed_ms": int((time.monotonic() - started) * 1_000),
            "error": None,
            "frames": frame_count,
            "gaps": counters.gaps,
            "limitations": [],
            "manifest_sha256": None,
            "network_calls": budget.calls,
            "official_contract_sha256": None,
            "planned_campaign_manifest_sha256": config.campaign_manifest_sha256,
            "planned_candidate_config_sha256": config.candidate_config_sha256,
            "planned_official_contract_sha256": config.official_contract_sha256,
            "planned_probe_binding_sha256": probe_binding_sha256,
            "probe_binding_sha256": None,
            "max_frames": config.max_frames,
            "max_network_calls": config.max_network_calls,
            "max_segments": config.max_segments,
            "queue_high_water": counters.queue_high_water,
            "reconnects": counters.reconnects,
            "requested_duration_seconds": config.duration_seconds,
            "root_sha256": None,
            "schema_version": 1,
            "segments": writer.segment_count,
            "source_timestamp_max_ns": None if not timestamps else max(timestamps),
            "source_timestamp_min_ns": None if not timestamps else min(timestamps),
            "terminal_health": "RUNNING",
            "venue": config.venue.value,
        }
        _atomic_json(reports_root / "health.json", running)
        progress(frame_count)

    publish_running(0)
    _atomic_json(
        reports_root / "probe-config.json",
        {**probe_binding_payload, "probe_binding_sha256": probe_binding_sha256},
    )
    try:
        try:
            if config.venue is Venue.HYPERLIQUID:
                limitations = _hyperliquid_probe(
                    config,
                    factory=factory,
                    writer=writer,
                    counters=counters,
                    deadline=deadline,
                    stop_requested=stop_requested,
                    progress=publish_running,
                    session=session,
                    budget=budget,
                )
            elif config.venue is Venue.LIGHTER:
                limitations = _lighter_probe(
                    config,
                    factory=factory,
                    writer=writer,
                    counters=counters,
                    deadline=deadline,
                    stop_requested=stop_requested,
                    progress=publish_running,
                    session=session,
                    connection_attempts=connection_attempts,
                    budget=budget,
                )
            elif config.venue is Venue.POLYMARKET:
                limitations = _polymarket_probe(
                    config,
                    factory=factory,
                    writer=writer,
                    counters=counters,
                    deadline=deadline,
                    stop_requested=stop_requested,
                    progress=publish_running,
                    session=session,
                    budget=budget,
                )
            else:
                limitations = _kalshi_probe(
                    config,
                    factory=factory,
                    writer=writer,
                    counters=counters,
                    deadline=deadline,
                    stop_requested=stop_requested,
                    progress=publish_running,
                    session=session,
                    budget=budget,
                )
        finally:
            collection_stopped = time.monotonic()
        if stop_requested():
            terminal = "INTERRUPTED_RECOVERABLE"
        manifest = writer.close()
    except KeyboardInterrupt:
        terminal = "INTERRUPTED_RECOVERABLE"
        manifest = writer.close()
    except _ProbeBoundaryReached as caught:
        terminal = caught.terminal_health
        manifest = writer.close()
    except ResearchDataCapacityError as caught:
        terminal = "MAX_BYTES_REACHED"
        error = _bounded_terminal_error(str(caught))
        manifest = writer.close()
    except BufferError as caught:
        terminal = "BACKPRESSURE_LIMIT_REACHED"
        counters.gaps += 1
        error = _bounded_terminal_error(f"{type(caught).__name__}:{caught}")
        manifest = writer.close()
    except (ConnectionError, LookupError, TimeoutError, OSError) as caught:
        terminal = "PUBLIC_SOURCE_UNAVAILABLE"
        error = _bounded_terminal_error(f"{type(caught).__name__}:{caught}")
        manifest = writer.close()
    except ValueError as caught:
        terminal = "PUBLIC_SOURCE_INVALID"
        error = _bounded_terminal_error(f"{type(caught).__name__}:{caught}")
        manifest = writer.close()
    except BaseException:
        writer.abort()
        raise
    finally:
        session.close()
    elapsed_ms = int((min(collection_stopped, deadline) - started) * 1_000)
    timestamps = counters.source_timestamps
    venue_limitations = {
        Venue.HYPERLIQUID: (),
        Venue.LIGHTER: (),
        Venue.POLYMARKET: (
            "POLYMARKET_WS_HAS_NO_DOCUMENTED_SOURCE_SEQUENCE",
            "POLYMARKET_CONTINUITY_CANNOT_BE_PROVEN_BY_GAPS_ZERO",
            "POLYMARKET_EXACT_FEES_REQUIRE_POINT_IN_TIME_SCHEDULE",
        ),
        Venue.KALSHI: (
            KalshiPublicAdapter.websocket_limitation,
            "KALSHI_REST_BOOK_HAS_NO_DOCUMENTED_SOURCE_TIMESTAMP_OR_SEQUENCE",
            "KALSHI_CONTINUITY_CANNOT_BE_PROVEN_BY_GAPS_ZERO",
            "KALSHI_EXACT_FEES_REQUIRE_SERIES_AND_EVENT_SCHEDULE_BINDING",
        ),
    }[config.venue]
    binding_is_authenticated = manifest is not None and manifest.frame_count > 0
    if not binding_is_authenticated:
        limitations = (*limitations, "NO_AUTHENTICATED_RAW_FRAME")
    limitations = tuple(dict.fromkeys((*venue_limitations, *limitations)))
    report = ProbeReport(
        schema_version=1,
        boundary="PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        venue=config.venue.value,
        terminal_health=terminal,
        collection_id=collection_id,
        requested_duration_seconds=config.duration_seconds,
        elapsed_ms=elapsed_ms,
        frames=0 if manifest is None else manifest.frame_count,
        segments=0 if manifest is None else len(manifest.segments),
        bytes=0 if manifest is None else manifest.stored_segment_bytes,
        gaps=counters.gaps,
        duplicates=counters.duplicates,
        reconnects=counters.reconnects,
        queue_high_water=counters.queue_high_water,
        source_timestamp_min_ns=None if not timestamps else min(timestamps),
        source_timestamp_max_ns=None if not timestamps else max(timestamps),
        manifest_sha256=None if manifest is None else manifest.manifest_sha256,
        root_sha256=None if manifest is None else manifest.root_sha256,
        network_calls=budget.calls,
        probe_binding_sha256=(probe_binding_sha256 if binding_is_authenticated else None),
        limitations=limitations,
        error=error,
        campaign_manifest_sha256=(
            config.campaign_manifest_sha256 if binding_is_authenticated else None
        ),
        official_contract_sha256=(
            config.official_contract_sha256 if binding_is_authenticated else None
        ),
        candidate_config_sha256=(
            config.candidate_config_sha256 if binding_is_authenticated else None
        ),
        connection_attempts=tuple(connection_attempts),
    )
    _atomic_json(reports_root / "health.json", report.to_dict())
    _atomic_json(reports_root / "result.json", report.to_dict())
    return report


def recover_public_probe_output(
    output_root: Path,
    *,
    venue: Venue,
    requested_duration_seconds: int,
    terminal_health: str,
    error: str,
    limitations: Sequence[str] = (),
) -> ProbeReport:
    """Finalize only already-published raw bytes after a process-level probe error."""

    raw_root = output_root / "raw"
    reports_root = output_root / "reports"
    if terminal_health not in {
        "INTERRUPTED_RECOVERED",
        "PUBLIC_SOURCE_UNAVAILABLE_RECOVERED",
        "RECOVERED_AFTER_PROCESS_ERROR",
    }:
        raise ValueError("recovery terminal health cannot claim a normal or green run")
    if not error:
        raise ValueError("recovery requires the preserved process error")
    error = _bounded_terminal_error(error)
    if (reports_root / "result.json").exists():
        raise ValueError("recovery refuses an output that already has a terminal result")
    try:
        probe_config = json.loads((reports_root / "probe-config.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as caught:
        raise ValueError("recovery requires the original probe-config.json") from caught
    if not isinstance(probe_config, Mapping):
        raise ValueError("recovery probe config must be an object")
    probe_binding_payload = _probe_binding_payload_from_mapping(probe_config)
    probe_binding_sha256 = _probe_binding_sha256(probe_binding_payload)
    if probe_config.get("probe_binding_sha256") != probe_binding_sha256:
        raise ValueError("recovery probe config binding hash diverged")
    if (
        probe_config.get("venue") != venue.value
        or probe_config.get("duration_seconds") != requested_duration_seconds
    ):
        raise ValueError("recovery venue or duration diverges from original config")
    segment_paths = sorted((raw_root / "segments").glob(f"*{SEGMENT_SUFFIX}"))
    if not segment_paths:
        raise LookupError("recoverable probe output has no published segment")
    artifacts = [
        decode_segment(
            path.read_bytes(),
            expected_physical_sha256=path.name.removesuffix(SEGMENT_SUFFIX),
        )
        for path in segment_paths
    ]
    collection_ids = {artifact.descriptor.collection_id for artifact in artifacts}
    if len(collection_ids) != 1:
        raise ValueError("recoverable probe segments mix collection identities")
    collection_id = next(iter(collection_ids))
    stored_bytes = sum(artifact.descriptor.stored_bytes for artifact in artifacts)
    writer = ResearchSegmentWriter(
        raw_root,
        collection_id=collection_id,
        max_segment_bytes=max(artifact.descriptor.logical_bytes for artifact in artifacts),
        rotation_seconds=300.0,
        max_total_bytes=max(stored_bytes + 4_096, 1_048_576),
    )
    manifest = writer.close()
    if manifest is None:
        raise ValueError("recoverable probe did not authenticate a manifest")
    from .segments import ResearchSegmentReader

    envelopes = ResearchSegmentReader(raw_root, manifest_sha256=manifest.manifest_sha256).replay()
    if {item.venue for item in envelopes} != {venue}:
        raise ValueError("recoverable probe envelopes do not match requested venue")
    expected_session_prefix = f"probe-binding-{probe_binding_sha256}:"
    if any(not item.session_identity.startswith(expected_session_prefix) for item in envelopes):
        raise ValueError("recoverable probe raw envelopes do not bind the probe configuration")
    counters = _Counters()
    for envelope in envelopes:
        counters.observe(envelope)
    monotonic_values = [item.receive_monotonic_ns for item in envelopes]
    elapsed_ms = 0 if not monotonic_values else (max(monotonic_values) - min(monotonic_values)) // 1_000_000
    timestamps = counters.source_timestamps
    recovered_reconnects = sum(int(item.state.reconnect) for item in envelopes)
    combined_limitations = (
        *limitations,
        "QUEUE_HIGH_WATER_NOT_RECOVERABLE_AFTER_PROCESS_ERROR",
        "NETWORK_CALL_COUNT_NOT_RECOVERABLE_AFTER_PROCESS_ERROR",
        "CONNECTION_ATTEMPTS_NOT_RECOVERABLE_AFTER_PROCESS_ERROR",
        "UNPUBLISHED_IN_MEMORY_TAIL_NOT_CLAIMED",
    )
    report = ProbeReport(
        schema_version=1,
        boundary="PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        venue=venue.value,
        terminal_health=terminal_health,
        collection_id=collection_id,
        requested_duration_seconds=requested_duration_seconds,
        elapsed_ms=elapsed_ms,
        frames=len(envelopes),
        segments=len(manifest.segments),
        bytes=manifest.stored_segment_bytes,
        gaps=counters.gaps,
        duplicates=counters.duplicates,
        reconnects=recovered_reconnects,
        queue_high_water=0,
        source_timestamp_min_ns=None if not timestamps else min(timestamps),
        source_timestamp_max_ns=None if not timestamps else max(timestamps),
        manifest_sha256=manifest.manifest_sha256,
        root_sha256=manifest.root_sha256,
        network_calls=0,
        probe_binding_sha256=probe_binding_sha256,
        limitations=combined_limitations,
        error=error,
        campaign_manifest_sha256=cast(str | None, probe_config.get("campaign_manifest_sha256")),
        official_contract_sha256=cast(str | None, probe_config.get("official_contract_sha256")),
        candidate_config_sha256=cast(str | None, probe_config.get("candidate_config_sha256")),
        connection_attempts=(),
    )
    _atomic_json(reports_root / "health.json", report.to_dict())
    _atomic_json(reports_root / "result.json", report.to_dict())
    return report


__all__ = [
    "ProbeConfig",
    "ProbeReport",
    "recover_public_probe_output",
    "run_public_probe",
]
