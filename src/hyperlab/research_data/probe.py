from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

from .adapters import (
    HYPERLIQUID_METADATA_VERSION,
    HYPERLIQUID_PUBLIC_HTTP_URL,
    HYPERLIQUID_PUBLIC_WEBSOCKET_URL,
    KALSHI_METADATA_VERSION,
    KALSHI_PUBLIC_HTTP_URL,
    POLYMARKET_CLOB_PUBLIC_URL,
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
    LIGHTER_PUBLIC_WEBSOCKET_URL,
    LighterPublicAdapter,
    lighter_market_census,
)
from .segments import (
    SEGMENT_SUFFIX,
    ManifestRecord,
    ResearchDataCapacityError,
    ResearchSegmentWriter,
    decode_segment,
)


class HttpResponse(Protocol):
    status_code: int
    content: bytes

    def iter_content(self, chunk_size: int) -> Iterator[bytes]: ...
    def close(self) -> None: ...


class HttpSession(Protocol):
    def get(self, url: str, **kwargs: object) -> HttpResponse: ...
    def post(self, url: str, **kwargs: object) -> HttpResponse: ...
    def close(self) -> None: ...


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
            raise ValueError(
                f"{self.venue.value} probe duration must be within 1..{duration_limit} seconds"
            )
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
        supported = {
            Venue.HYPERLIQUID: HyperliquidPublicAdapter.supported_feeds,
            Venue.LIGHTER: LighterPublicAdapter.supported_feeds,
            Venue.POLYMARKET: PolymarketPublicAdapter.supported_feeds,
            Venue.KALSHI: KalshiPublicAdapter.supported_feeds,
        }[self.venue]
        unknown = set(self.feeds) - supported
        if unknown:
            raise ValueError(
                f"unsupported {self.venue.value} public feeds: {sorted(unknown)}"
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
    limitations: tuple[str, ...]
    error: str | None

    def to_dict(self) -> dict[str, CanonicalValue]:
        return {
            "boundary": self.boundary,
            "bytes": self.bytes,
            "collection_id": self.collection_id,
            "duplicates": self.duplicates,
            "elapsed_ms": self.elapsed_ms,
            "error": self.error,
            "frames": self.frames,
            "gaps": self.gaps,
            "limitations": list(self.limitations),
            "manifest_sha256": self.manifest_sha256,
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
) -> bytes:
    if max_response_bytes <= 0:
        raise ValueError("HTTP response bound must be positive")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("bounded public probe deadline expired before HTTP request")
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
                f"public source returned HTTP {response.status_code} for {request.url}"
            )
        iterator = getattr(response, "iter_content", None)
        chunks = (
            iterator(chunk_size=64 * 1024)
            if callable(iterator)
            else iter((bytes(response.content),))
        )
        body = bytearray()
        for chunk in chunks:
            body.extend(chunk)
            if len(body) > max_response_bytes:
                raise ResearchDataCapacityError("public HTTP response exceeds its raw byte bound")
            if time.monotonic() > deadline:
                raise TimeoutError("bounded public probe deadline expired during HTTP response")
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
    session.headers.update({"User-Agent": "HyperLab-Research-Data-Plane-V1-PUBLIC-ONLY"})
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


def _append_lighter_bounded(
    writer: ResearchSegmentWriter,
    counters: _Counters,
    envelope: Any,
    config: ProbeConfig,
) -> None:
    if writer.segment_count >= config.max_segments:
        raise _ProbeBoundaryReached("MAX_SEGMENTS_REACHED")
    if (
        writer.segment_count == config.max_segments - 1
        and writer.would_rotate(envelope)
    ):
        writer.flush()
        raise _ProbeBoundaryReached("MAX_SEGMENTS_REACHED")
    _append(writer, counters, envelope)
    if envelope.state.gap_detected:
        raise _ProbeBoundaryReached("CONTINUITY_BROKEN_FROZEN")
    if writer.frame_count >= config.max_frames:
        raise _ProbeBoundaryReached("MAX_FRAMES_REACHED")


def _raw_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _datetime_to_ns(value: datetime) -> int:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("collector receive timestamp must be timezone-aware")
    delta = value.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _polymarket_tokens(census_payload: bytes, *, limit: int) -> tuple[tuple[str, str], ...]:
    decoded = json.loads(census_payload.decode("utf-8"))
    if not isinstance(decoded, list):
        raise ValueError("Polymarket market census must be an array")
    selected: list[tuple[str, str]] = []
    for market in decoded:
        if not isinstance(market, Mapping):
            continue
        market_id = str(market.get("conditionId") or market.get("id") or "")
        raw_tokens = market.get("clobTokenIds")
        if isinstance(raw_tokens, str):
            try:
                raw_tokens = json.loads(raw_tokens)
            except json.JSONDecodeError:
                raw_tokens = None
        if not market_id or not isinstance(raw_tokens, list):
            continue
        for token in raw_tokens:
            if token:
                selected.append((str(token), market_id))
                if len(selected) >= limit:
                    return tuple(selected)
    return tuple(selected)


def _hyperliquid_instruments(census_payload: bytes, *, limit: int) -> tuple[str, ...]:
    decoded = json.loads(census_payload.decode("utf-8"))
    if not isinstance(decoded, list) or not decoded or not isinstance(decoded[0], Mapping):
        raise ValueError("Hyperliquid metaAndAssetCtxs census is invalid")
    universe = decoded[0].get("universe")
    if not isinstance(universe, list):
        raise ValueError("Hyperliquid census omitted universe")
    names = sorted(
        {
            str(item["name"])
            for item in universe
            if isinstance(item, Mapping) and item.get("name")
        }
    )
    if not names:
        raise LookupError("Hyperliquid census returned no instruments")
    return tuple(names[:limit])


def _kalshi_markets(
    census_payload: bytes, *, limit: int
) -> tuple[tuple[str, str | None, str | None], ...]:
    decoded = json.loads(census_payload.decode("utf-8"))
    if not isinstance(decoded, Mapping):
        raise ValueError("Kalshi market census is invalid")
    markets: list[Mapping[str, object]]
    if isinstance(decoded.get("market"), Mapping):
        markets = [cast(Mapping[str, object], decoded["market"])]
    elif isinstance(decoded.get("markets"), list):
        raw_markets = decoded["markets"]
        if not raw_markets or any(not isinstance(item, Mapping) for item in raw_markets):
            raise LookupError("Kalshi market census returned no active market")
        markets = [cast(Mapping[str, object], item) for item in raw_markets]
    else:
        raise ValueError("Kalshi market census is invalid")
    identities: list[tuple[str, str | None, str | None]] = []
    for market in markets[:limit]:
        ticker = str(market.get("ticker") or "")
        if not ticker:
            raise ValueError("Kalshi market census omitted ticker")
        event = None if market.get("event_ticker") is None else str(market["event_ticker"])
        identities.append((ticker, event, None))
    return tuple(identities)


def _kalshi_series(event_payload: bytes) -> str:
    decoded = json.loads(event_payload.decode("utf-8"))
    if not isinstance(decoded, Mapping) or not isinstance(decoded.get("event"), Mapping):
        raise ValueError("Kalshi event metadata is invalid")
    series = decoded["event"].get("series_ticker")
    if not series:
        raise ValueError("Kalshi event metadata omitted series_ticker")
    return str(series)


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
) -> tuple[str, ...]:
    import websocket

    from hyperlab.collector.websocket import UrlWebsocketClientFactory

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
        _append(writer, counters, envelope)
    for request in adapter.public_http_requests(
        feeds=config.feeds, instruments=instruments
    ):
        if request.json_body is None:
            continue
        if census_request_body is not None and request.json_body == census_request_body:
            continue
        raw = _execute_http(
            session,
            request,
            deadline=deadline,
            max_response_bytes=_http_response_bound(config),
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
        _append(writer, counters, envelope)
    subscriptions = adapter.websocket_subscriptions(
        feeds=config.feeds, instruments=instruments
    )
    if not subscriptions:
        return adapter.unavailable_public_global_labels
    connector = UrlWebsocketClientFactory(
        HYPERLIQUID_PUBLIC_WEBSOCKET_URL,
        queue_capacity=1_024,
        venue="hyperliquid",
        socket_role="research-data-plane-v1",
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
                    socket = connector.connect(
                        "public", max(0.05, min(10.0, deadline - time.monotonic()))
                    )
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
                        raise ConnectionError(
                            "Hyperliquid public WebSocket remained unavailable"
                        ) from caught
                    counters.begin_reconnect(factory)
                    time.sleep(delays[min(attempt, len(delays) - 1)])
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
                        raise ResearchDataCapacityError(
                            "public WebSocket frame exceeds its raw byte bound"
                        )
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
                    _append(writer, counters, envelope)
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
) -> tuple[str, ...]:
    import websocket

    from hyperlab.collector.websocket import UrlWebsocketClientFactory

    adapter = LighterPublicAdapter()
    http_provenance = CaptureProvenance(
        factory.provenance.collection_id,
        f"{LIGHTER_PUBLIC_HTTP_URL}/orderBooks",
        "PUBLIC_HTTP",
    )
    ws_provenance = CaptureProvenance(
        factory.provenance.collection_id,
        LIGHTER_PUBLIC_WEBSOCKET_URL,
        "PUBLIC_WEBSOCKET",
    )
    metadata_requests = adapter.public_http_requests(
        feeds=config.feeds,
        market_indices=(),
    )
    market_indices: tuple[int, ...]
    if metadata_requests:
        raw_metadata = _execute_http(
            session,
            metadata_requests[0],
            deadline=deadline,
            max_response_bytes=_http_response_bound(config),
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
        by_id = {item.market_id: item for item in catalog}
        if config.instruments:
            try:
                market_indices = tuple(int(item) for item in config.instruments)
            except ValueError as error:
                raise ValueError("Lighter instruments must be decimal market indices") from error
            missing = sorted(set(market_indices) - set(by_id))
            if missing:
                raise ValueError(f"Lighter metadata omitted requested market indices: {missing}")
        else:
            active = tuple(item.market_id for item in catalog if item.status.lower() == "active")
            market_indices = active[: config.census_limit]
            if not market_indices:
                raise LookupError("Lighter metadata exposed no active public market in census")
    else:
        if not config.instruments:
            raise ValueError("Lighter census requires the public metadata feed")
        try:
            market_indices = tuple(int(item) for item in config.instruments)
        except ValueError as error:
            raise ValueError("Lighter instruments must be decimal market indices") from error

    subscriptions = adapter.websocket_subscriptions(
        feeds=config.feeds,
        market_indices=market_indices,
    )
    if not subscriptions:
        return ("LIGHTER_WEBSOCKET_NOT_REQUESTED",)
    connector = UrlWebsocketClientFactory(
        LIGHTER_PUBLIC_WEBSOCKET_URL,
        queue_capacity=1_024,
        venue="lighter",
        socket_role="lighter-public-probe-v1",
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
                    socket = connector.connect(
                        "public", max(0.05, min(10.0, deadline - time.monotonic()))
                    )
                    for subscription in subscriptions:
                        socket.send_json(dict(subscription.payload))
                    attempt = 0
                    last_connection_error = None
                    connected_once = True
                except (
                    ConnectionError,
                    OSError,
                    TimeoutError,
                    websocket.WebSocketException,
                ) as caught:
                    last_connection_error = caught
                    if socket is not None:
                        socket.close()
                        socket = None
                    if time.monotonic() >= deadline:
                        raise ConnectionError(
                            "Lighter public WebSocket remained unavailable"
                        ) from caught
                    time.sleep(min(delays[min(attempt, len(delays) - 1)], max(deadline - time.monotonic(), 0)))
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
                        raise ResearchDataCapacityError(
                            "public WebSocket frame exceeds its raw byte bound"
                        )
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
            except (ConnectionError, OSError, TimeoutError, websocket.WebSocketException):
                socket.close()
                socket = None
                counters.begin_reconnect(factory)
                adapter.begin_connection_epoch()
    finally:
        if socket is not None:
            socket.close()
    if not connected_once and last_connection_error is not None and not stop_requested():
        raise ConnectionError("Lighter public WebSocket never connected") from last_connection_error
    if not stop_requested() and time.monotonic() >= deadline:
        raise _ProbeBoundaryReached("MAX_DURATION_REACHED")
    return ()


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
) -> tuple[str, ...]:
    import websocket

    adapter = PolymarketPublicAdapter()
    selected_feeds = set(config.feeds)
    limitations: list[str] = []
    http_provenance = CaptureProvenance(
        factory.provenance.collection_id,
        POLYMARKET_GAMMA_PUBLIC_URL,
        "PUBLIC_HTTP",
    )
    if config.instruments:
        resolved: list[tuple[str, str]] = []
        for token in config.instruments:
            request = adapter.market_by_token_request(token)
            raw = _execute_http(
                session,
                request,
                deadline=deadline,
                max_response_bytes=_http_response_bound(config),
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
                provenance=CaptureProvenance(
                    factory.provenance.collection_id,
                    request.url,
                    "PUBLIC_HTTP",
                ),
            )
            _append(writer, counters, envelope)
        token_markets = tuple(resolved)
        if "metadata" in selected_feeds:
            limitations.append(
                "POLYMARKET_GAMMA_RULE_METADATA_NOT_RESOLVED_FROM_TOKEN_ID"
            )
    else:
        census = _execute_http(
            session,
            adapter.market_census_request(limit=max(config.census_limit, 1)),
            deadline=deadline,
            max_response_bytes=_http_response_bound(config),
        )
        census_envelope = adapter.envelope_from_http(
            census,
            feed_type="metadata",
            token_id=None,
            market_id="CENSUS",
            factory=factory,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            provenance=http_provenance,
        )
        _append(writer, counters, census_envelope)
        token_markets = _polymarket_tokens(census, limit=config.census_limit)
    if not token_markets:
        raise LookupError("Polymarket census returned no public CLOB token")
    if "events" in selected_feeds:
        limitations.append(
            "POLYMARKET_EVENT_METADATA_REQUIRES_EXPLICIT_EVENT_ID_OUTSIDE_TOKEN_SCOPE"
        )
    clob_provenance = CaptureProvenance(
        factory.provenance.collection_id,
        POLYMARKET_CLOB_PUBLIC_URL,
        "PUBLIC_HTTP",
    )
    token_http_feeds = selected_feeds & {
        "fees",
        "last_trade_price",
        "order_book",
        "tick_size",
    }
    for token, known_market in token_markets:
        for feed_type, request in adapter.token_parameter_requests(
            (token,), feeds=tuple(token_http_feeds)
        ):
            raw = _execute_http(
                session,
                request,
                deadline=deadline,
                max_response_bytes=_http_response_bound(config),
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
                provenance=clob_provenance,
            )
            _append(writer, counters, envelope)
    if "public_trades" in selected_feeds:
        for market in sorted({market for _, market in token_markets}):
            request = adapter.public_trade_request((market,), limit=100)
            raw = _execute_http(
                session,
                request,
                deadline=deadline,
                max_response_bytes=_http_response_bound(config),
            )
            envelope = adapter.envelope_from_http(
                raw,
                feed_type="public_trades",
                token_id=None,
                market_id=market,
                factory=factory,
                receive_timestamp_utc_ns=time.time_ns(),
                receive_monotonic_ns=time.monotonic_ns(),
                provenance=CaptureProvenance(
                    factory.provenance.collection_id,
                    request.url,
                    "PUBLIC_HTTP",
                ),
            )
            _append(writer, counters, envelope)
    token_ids = tuple(token for token, _ in token_markets)
    websocket_feeds = selected_feeds & {
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
    last_connection_error: BaseException | None = None
    last_progress = time.monotonic()
    last_ping = last_progress
    connection: Any = None
    try:
        while time.monotonic() < deadline and not stop_requested():
            if connection is None:
                try:
                    connection = websocket.create_connection(
                        POLYMARKET_PUBLIC_WEBSOCKET_URL,
                        timeout=max(0.05, min(10.0, deadline - time.monotonic())),
                        enable_multithread=False,
                        redirect_limit=0,
                    )
                    getstatus = getattr(connection, "getstatus", None)
                    if not callable(getstatus) or getstatus() != 101:
                        raise ConnectionError("Polymarket public WebSocket did not return HTTP 101")
                    connection.settimeout(1.0)
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
                    time.sleep(delays[min(attempt, len(delays) - 1)])
                    attempt += 1
                    continue
            try:
                message = connection.recv()
                now_mono_ns = time.monotonic_ns()
                now_utc_ns = time.time_ns()
                if isinstance(message, bytes):
                    raw = message
                elif isinstance(message, str):
                    raw = message.encode("utf-8")
                else:
                    raise TypeError("Polymarket public WebSocket returned a non-text frame")
                if raw in {b"PING", b"PONG"}:
                    envelope = factory.make(
                        feed_type="heartbeat",
                        instrument_id="PM:GLOBAL",
                        market_id="PM:GLOBAL",
                        source_timestamp_ns=None,
                        receive_timestamp_utc_ns=now_utc_ns,
                        receive_monotonic_ns=now_mono_ns,
                        raw_payload=raw,
                        source_sequence=None,
                        provenance=ws_provenance,
                    )
                    if raw == b"PING":
                        connection.send("PONG")
                else:
                    if not raw:
                        raise ConnectionError("Polymarket public WebSocket closed")
                    if len(raw) > _http_response_bound(config):
                        raise ResearchDataCapacityError(
                            "public WebSocket frame exceeds its raw byte bound"
                        )
                    envelope = adapter.envelope_from_websocket(
                        raw,
                        factory=factory,
                        receive_timestamp_utc_ns=now_utc_ns,
                        receive_monotonic_ns=now_mono_ns,
                        provenance=ws_provenance,
                    )
                _append(writer, counters, envelope)
                counters.queue_high_water = max(counters.queue_high_water, 1)
            except websocket.WebSocketTimeoutException:
                pass
            except (ConnectionError, OSError, websocket.WebSocketException):
                connection.close()
                connection = None
                counters.begin_reconnect(factory)
            now = time.monotonic()
            if connection is not None and now - last_ping >= 10.0:
                try:
                    connection.send("PING")
                    last_ping = now
                except (ConnectionError, OSError, websocket.WebSocketException):
                    connection.close()
                    connection = None
                    counters.begin_reconnect(factory)
            if now - last_progress >= config.progress_interval_seconds:
                progress(writer.frame_count)
                last_progress = now
    finally:
        if connection is not None:
            connection.close()
    if not connected_once and last_connection_error is not None and not stop_requested():
        raise ConnectionError("Polymarket public WebSocket never connected") from last_connection_error
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
) -> tuple[str, ...]:
    adapter = KalshiPublicAdapter()
    limitations = [adapter.websocket_limitation]
    provenance = CaptureProvenance(
        factory.provenance.collection_id,
        KALSHI_PUBLIC_HTTP_URL,
        "PUBLIC_HTTP",
    )
    scopes: list[tuple[str, str | None, str | None]] = []
    if config.instruments:
        for requested_ticker in config.instruments:
            market_request = adapter.requests_for_market(
                ticker=requested_ticker,
                event_ticker=None,
                series_ticker=None,
                feeds=("markets",),
            )[0]
            market_raw = _execute_http(
                session,
                market_request,
                deadline=deadline,
                max_response_bytes=_http_response_bound(config),
            )
            scope = _kalshi_markets(market_raw, limit=1)[0]
            if scope[0].upper() != requested_ticker.upper():
                raise ValueError("Kalshi explicit market metadata returned another ticker")
            scopes.append(scope)
            market_envelope = adapter.envelope_from_http(
                market_raw,
                feed_type="markets",
                ticker=scope[0],
                factory=factory,
                receive_timestamp_utc_ns=time.time_ns(),
                receive_monotonic_ns=time.monotonic_ns(),
                provenance=provenance,
            )
            _append(writer, counters, market_envelope)
    else:
        census = _execute_http(
            session,
            adapter.market_census_request(limit=max(config.census_limit, 1)),
            deadline=deadline,
            max_response_bytes=_http_response_bound(config),
        )
        census_envelope = adapter.envelope_from_http(
            census,
            feed_type="markets",
            ticker="CENSUS",
            factory=factory,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            provenance=provenance,
        )
        _append(writer, counters, census_envelope)
        scopes.extend(_kalshi_markets(census, limit=config.census_limit))
    resolved_scopes: list[tuple[str, str | None, str | None]] = []
    for ticker, event_ticker, series_ticker in scopes:
        if event_ticker and ({"series", "fee_changes"} & set(config.feeds)):
            event_request = adapter.requests_for_market(
                ticker=ticker,
                event_ticker=event_ticker,
                series_ticker=None,
                feeds=("events",),
            )[0]
            event_raw = _execute_http(
                session,
                event_request,
                deadline=deadline,
                max_response_bytes=_http_response_bound(config),
            )
            series_ticker = _kalshi_series(event_raw)
            event_envelope = adapter.envelope_from_http(
                event_raw,
                feed_type="events",
                ticker=ticker,
                factory=factory,
                receive_timestamp_utc_ns=time.time_ns(),
                receive_monotonic_ns=time.monotonic_ns(),
                provenance=provenance,
            )
            _append(writer, counters, event_envelope)
        if "events" in config.feeds and event_ticker is None:
            limitations.append(f"KALSHI_EVENT_TICKER_UNAVAILABLE:{ticker}")
        if {"series", "fee_changes"} & set(config.feeds) and series_ticker is None:
            limitations.append(f"KALSHI_SERIES_TICKER_UNAVAILABLE:{ticker}")
        resolved_scopes.append((ticker, event_ticker, series_ticker))
    last_progress = time.monotonic()
    while time.monotonic() < deadline and not stop_requested():
        for ticker, event_ticker, series_ticker in resolved_scopes:
            requests = adapter.requests_for_market(
                ticker=ticker,
                event_ticker=event_ticker,
                series_ticker=series_ticker,
                feeds=config.feeds,
            )
            for request in requests:
                raw = _execute_http(
                    session,
                    request,
                    deadline=deadline,
                    max_response_bytes=_http_response_bound(config),
                )
                path = request.url.removeprefix(KALSHI_PUBLIC_HTTP_URL).strip("/")
                if "orderbook" in path:
                    feed_type = "order_book"
                elif path == "markets/trades":
                    feed_type = "trades"
                elif path.startswith("events/"):
                    feed_type = "events"
                elif path.startswith("series/fee_changes"):
                    feed_type = "fee_changes"
                elif path.startswith("series/"):
                    feed_type = "series"
                elif path.startswith("incentive_programs"):
                    feed_type = "incentives"
                else:
                    feed_type = "markets"
                envelope = adapter.envelope_from_http(
                    raw,
                    feed_type=feed_type,
                    ticker=ticker,
                    factory=factory,
                    receive_timestamp_utc_ns=time.time_ns(),
                    receive_monotonic_ns=time.monotonic_ns(),
                    provenance=provenance,
                )
                _append(writer, counters, envelope)
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
        session_identity=f"probe-{config.venue.value}-{collection_id}",
        source_metadata_version=source_versions[config.venue],
        provenance=CaptureProvenance(
            collection_id,
            source_urls[config.venue],
            "PUBLIC_HTTP",
        ),
    )
    counters = _Counters()
    started = time.monotonic()
    terminal = "COMPLETE"
    error: str | None = None
    limitations: tuple[str, ...] = ()
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

    def publish_running(frame_count: int) -> None:
        timestamps = counters.source_timestamps
        running: dict[str, object] = {
            "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
            "bytes": writer.stored_segment_bytes,
            "collection_id": collection_id,
            "duplicates": counters.duplicates,
            "elapsed_ms": int((time.monotonic() - started) * 1_000),
            "error": None,
            "frames": frame_count,
            "gaps": counters.gaps,
            "limitations": [],
            "manifest_sha256": None,
            "max_frames": config.max_frames,
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
        {
            "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
            "census_limit": config.census_limit,
            "duration_seconds": config.duration_seconds,
            "feeds": list(config.feeds),
            "instruments": list(config.instruments),
            "max_bytes": config.max_bytes,
            "max_frames": config.max_frames,
            "max_segment_bytes": config.max_segment_bytes,
            "max_segments": config.max_segments,
            "progress_interval_seconds": str(config.progress_interval_seconds),
            "proxy_policy": (
                "DIRECT_ONLY_ENVIRONMENT_PROXY_DISABLED"
                if config.venue is Venue.LIGHTER
                else "VENUE_DEFAULT"
            ),
            "rotation_seconds": str(config.rotation_seconds),
            "schema_version": 1,
            "venue": config.venue.value,
        },
    )
    try:
        deadline = started + config.duration_seconds
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
            )
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
        error = str(caught)
        manifest = writer.close()
    except BufferError as caught:
        terminal = "BACKPRESSURE_LIMIT_REACHED"
        counters.gaps += 1
        error = f"{type(caught).__name__}:{caught}"
        manifest = writer.close()
    except (ConnectionError, LookupError, TimeoutError, OSError) as caught:
        terminal = "PUBLIC_SOURCE_UNAVAILABLE"
        error = f"{type(caught).__name__}:{caught}"
        manifest = writer.close()
    except ValueError as caught:
        terminal = "PUBLIC_SOURCE_INVALID"
        error = f"{type(caught).__name__}:{caught}"
        manifest = writer.close()
    except BaseException:
        writer.abort()
        raise
    finally:
        session.close()
    elapsed_ms = int((time.monotonic() - started) * 1_000)
    timestamps = counters.source_timestamps
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
        limitations=limitations,
        error=error,
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

    envelopes = ResearchSegmentReader(
        raw_root, manifest_sha256=manifest.manifest_sha256
    ).replay()
    counters = _Counters()
    for envelope in envelopes:
        counters.observe(envelope)
    monotonic_values = [item.receive_monotonic_ns for item in envelopes]
    elapsed_ms = (
        0
        if not monotonic_values
        else (max(monotonic_values) - min(monotonic_values)) // 1_000_000
    )
    timestamps = counters.source_timestamps
    combined_limitations = (
        *limitations,
        "QUEUE_HIGH_WATER_NOT_RECOVERABLE_AFTER_PROCESS_ERROR",
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
        reconnects=counters.reconnects,
        queue_high_water=0,
        source_timestamp_min_ns=None if not timestamps else min(timestamps),
        source_timestamp_max_ns=None if not timestamps else max(timestamps),
        manifest_sha256=manifest.manifest_sha256,
        root_sha256=manifest.root_sha256,
        limitations=combined_limitations,
        error=error,
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
