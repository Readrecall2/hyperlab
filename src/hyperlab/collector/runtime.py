from __future__ import annotations

import random
import sys
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from hyperlab.api.public import HyperliquidPublicClient, PublicBootstrap
from hyperlab.collector.bootstrap import (
    historical_envelope,
    parse_bbo_from_l2,
    parse_bootstrap,
    parse_candles,
    parse_funding_history,
    parse_l2_snapshot,
)
from hyperlab.collector.models import (
    CollectorConfig,
    CollectorMetrics,
    CollectorState,
    ParsedMessage,
    ParsedRecord,
    PublicSubscription,
    WireEnvelope,
)
from hyperlab.collector.parser import parse_websocket_message
from hyperlab.collector.storage import (
    BatchingLakeSink,
    CoordinatedWriterError,
    FlushResult,
    LakeSink,
)
from hyperlab.collector.telemetry import (
    MonotonicTimingSummary,
    ProcessRuntimeTelemetry,
)
from hyperlab.collector.websocket import (
    PublicSocket,
    PublicSocketFactory,
    ReceivedWireMessage,
    WebsocketConsumerBackpressure,
    WebsocketQueueOverflow,
)
from hyperlab.data.lake import DataLakeError
from hyperlab.data.schema import RecordType, latest_schema_for
from hyperlab.storage.sqlite import write_runtime_status

_GENERATION_REASON_HISTORY_CAPACITY = 32


class PublicRestClient(Protocol):
    def bootstrap(self, *, observed_at_ms: int | None = None) -> PublicBootstrap: ...

    def funding_history(
        self,
        asset: str,
        start_ms: int,
        end_ms: int | None = None,
    ) -> Any: ...

    def candles(self, asset: str, interval: str, start_ms: int, end_ms: int) -> Any: ...

    def l2_snapshot(
        self,
        asset: str,
        *,
        n_sig_figs: int | None = None,
        mantissa: int | None = None,
    ) -> Any: ...


class BoundedBackoff:
    def __init__(
        self,
        initial_seconds: float,
        maximum_seconds: float,
        jitter_ratio: float,
        random_value: Callable[[], float],
    ) -> None:
        if initial_seconds <= 0 or maximum_seconds < initial_seconds:
            raise ValueError("invalid backoff bounds")
        if not 0 <= jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between 0 and 1")
        self.initial_seconds = initial_seconds
        self.maximum_seconds = maximum_seconds
        self.jitter_ratio = jitter_ratio
        self.random_value = random_value
        self.attempt = 0

    def reset(self) -> None:
        self.attempt = 0

    def next_delay(self) -> float:
        base = min(
            self.maximum_seconds,
            self.initial_seconds * (2 ** min(self.attempt, 30)),
        )
        self.attempt += 1
        sample = min(max(self.random_value(), 0.0), 1.0)
        factor = 1.0 + self.jitter_ratio * (2.0 * sample - 1.0)
        return min(max(base * factor, min(self.initial_seconds, 1.0)), self.maximum_seconds)


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _candle_interval_seconds(interval: str) -> int:
    amount = int(interval[:-1])
    multiplier = {
        "m": 60,
        "h": 3_600,
        "d": 86_400,
        "w": 604_800,
    }[interval[-1]]
    return amount * multiplier


class PublicCollector:
    """Supervise one public socket and explicit REST bootstrap/resynchronization."""

    def __init__(
        self,
        config: CollectorConfig,
        *,
        rest: PublicRestClient,
        socket_factory: PublicSocketFactory,
        sink: LakeSink,
        runtime_status_path: Path,
        clock: Callable[[], datetime] = _utc_now,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        random_value: Callable[[], float] = random.random,
        connection_id_factory: Callable[[], str] = lambda: uuid.uuid4().hex,
        producer_scoped_rest_connection_ids: bool = False,
    ) -> None:
        if not isinstance(producer_scoped_rest_connection_ids, bool):
            raise TypeError("producer_scoped_rest_connection_ids must be a boolean")
        self.config = config
        subscriptions = config.subscriptions()
        critical_channels = {"activeAssetCtx", "l2Book"}
        if critical_channels.isdisjoint(config.subscription_channels):
            critical_channels.add("bbo")
        self._critical_stream_keys = frozenset(
            subscription.key for subscription in subscriptions if subscription.channel in critical_channels
        )
        if config.critical_funding_history:
            self._critical_stream_keys = frozenset(
                set(self._critical_stream_keys)
                | {
                    f"fundingHistory:{asset}"
                    for asset in config.assets
                    if not asset.startswith("@") and "/" not in asset
                }
            )
        self._candle_intervals_by_stream_key = {
            subscription.key: subscription.interval
            for subscription in subscriptions
            if subscription.channel == "candle" and subscription.interval is not None
        }
        self.rest = rest
        self.socket_factory = socket_factory
        self.sink = sink
        self.runtime_status_path = runtime_status_path
        self.clock = clock
        self.monotonic = monotonic
        self.sleeper = sleeper
        self.connection_id_factory = connection_id_factory
        self._producer_scoped_rest_connection_ids = producer_scoped_rest_connection_ids
        self.metrics = CollectorMetrics()
        self._stop_requested = False
        self._runtime_telemetry = ProcessRuntimeTelemetry(
            monotonic_ns=lambda: round(self.monotonic() * 1_000_000_000),
            auto_start=False,
        )
        self._generation_failures: deque[dict[str, object]] = deque(
            maxlen=_GENERATION_REASON_HISTORY_CAPACITY
        )
        self._generation_failures_seen = 0
        self._normalization_timing = MonotonicTimingSummary()
        self._sink_enqueue_timing = MonotonicTimingSummary()
        self._rest_bootstrap_materialization_timing = MonotonicTimingSummary()
        self._rest_refresh_worker_materialization_timing = MonotonicTimingSummary()
        self._rest_refresh_supervisor_apply_timing = MonotonicTimingSummary()
        self._rest_bootstrap_rows_total = 0
        self._rest_refresh_rows_materialized_total = 0
        self._rest_refresh_rows_applied_total = 0
        self._last_rest_bootstrap_rows = 0
        self._last_rest_refresh_rows_materialized = 0
        self._last_rest_refresh_rows_applied = 0
        self._wire_monotonic_legacy_fallbacks = 0
        self._wire_monotonic_domain_fallbacks = 0
        self._active_socket: PublicSocket | None = None
        self._active_connection_id: str | None = None
        self._active_generation: int | None = None
        self._last_closed_socket_telemetry: dict[str, object] | None = None
        self._backlog_liveness_deferrals = 0
        self._backlog_liveness_active = False
        self._rest_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="hyperlab-rest")
        self._rest_future: Future[tuple[ParsedRecord, ...]] | None = None
        self._rest_refresh_counter = 0
        self._closed = False
        self._flush_failure: Exception | None = None
        self._flush_attempts = 0
        self._successful_flush_attempts = 0
        self._connection_attempts: deque[float] = deque()
        self._live_since: float | None = None
        self._backoff = BoundedBackoff(
            config.backoff_initial_seconds,
            config.backoff_max_seconds,
            config.backoff_jitter_ratio,
            random_value,
        )

    @classmethod
    def create_default(
        cls,
        config: CollectorConfig,
        *,
        data_dir: Path,
        request_timeout_seconds: float,
        socket_factory: PublicSocketFactory,
        sink: LakeSink | None = None,
        validate_storage_integrity: bool = False,
    ) -> PublicCollector:
        rest = HyperliquidPublicClient(
            network=config.network,
            timeout_seconds=request_timeout_seconds,
        )
        resolved_sink = (
            BatchingLakeSink(
                data_dir / "lake",
                batch_size=config.batch_size,
                queue_capacity=config.queue_capacity,
                validate_integrity=validate_storage_integrity,
            )
            if sink is None
            else sink
        )
        return cls(
            config,
            rest=rest,
            socket_factory=socket_factory,
            sink=resolved_sink,
            runtime_status_path=data_dir / "runtime_status.json",
        )

    def stop(self) -> None:
        self._stop_requested = True
        cancel_rest = getattr(self.rest, "cancel", None)
        if callable(cancel_rest):
            cancel_rest()

    def close(self) -> None:
        """Stop, flush and release public network resources."""

        if self._closed:
            return
        self._closed = True
        flush_attempts_before_close = self._flush_attempts
        successful_flushes_before_close = self._successful_flush_attempts
        try:
            self.stop()
            if self._active_socket is not None:
                self._close_socket_with_telemetry(
                    self._active_socket,
                    reason="collector close requested",
                )
            future = self._rest_future
            self._rest_future = None
            if future is not None:
                if self._flush_failure is None and future.done():
                    self._apply_rest_future(future)
                else:
                    future.cancel()
            self.metrics.connection_alive = False
            self.metrics.state = CollectorState.STOPPED
            self._update_staleness(self.clock())
            if self._flush_failure is not None:
                with suppress(Exception):
                    self._publish_status(error=f"{type(self._flush_failure).__name__}: {self._flush_failure}")
            else:
                try:
                    self._flush()
                except (DataLakeError, OSError) as exc:
                    with suppress(Exception):
                        self._publish_status(error=f"{type(exc).__name__}: {exc}")
                    raise
                self._publish_status()
        finally:
            primary_error = sys.exception()
            cleanup_errors: list[tuple[str, BaseException]] = []
            close_flush_attempts = self._flush_attempts - flush_attempts_before_close
            close_successful_flushes = self._successful_flush_attempts - successful_flushes_before_close
            if self._flush_failure is None and (
                close_flush_attempts == 0
                or (close_flush_attempts == close_successful_flushes and self.sink.pending_count)
            ):
                try:
                    self._flush()
                except BaseException as exc:
                    cleanup_errors.append(("terminal lake flush", exc))
            close_rest = getattr(self.rest, "close", None)
            cleanup_actions: list[tuple[str, Callable[[], None]]] = [
                (
                    "REST executor shutdown",
                    lambda: self._rest_executor.shutdown(wait=False, cancel_futures=True),
                ),
                ("lake sink close", self.sink.close),
                ("runtime telemetry close", self._runtime_telemetry.close),
            ]
            if callable(close_rest):
                cleanup_actions.append(("public REST client close", close_rest))
            for label, action in cleanup_actions:
                try:
                    action()
                except BaseException as exc:
                    cleanup_errors.append((label, exc))
            if primary_error is not None:
                for label, cleanup_error in cleanup_errors:
                    primary_error.add_note(
                        f"{label} also failed: {type(cleanup_error).__name__}: {cleanup_error}"
                    )
            elif cleanup_errors:
                first_label, first_error = cleanup_errors[0]
                for label, cleanup_error in cleanup_errors[1:]:
                    first_error.add_note(
                        f"{label} also failed: {type(cleanup_error).__name__}: {cleanup_error}"
                    )
                first_error.add_note(f"cleanup action: {first_label}")
                raise first_error

    def run(
        self,
        *,
        max_messages: int = 0,
        duration_seconds: float | None = None,
    ) -> CollectorMetrics:
        if max_messages < 0 or (duration_seconds is not None and duration_seconds <= 0):
            raise ValueError("max_messages and duration_seconds must be non-negative")
        self._runtime_telemetry.watchdog.start()
        started = self.monotonic()
        initial_bootstrap_complete = False
        socket: PublicSocket | None = None

        while not self._should_stop(started, max_messages, duration_seconds):
            connection_id = f"ws-{self.metrics.connection_epoch + 1}-{self.connection_id_factory()}"
            connected = False
            generation_liveness: dict[str, object] = {
                "connected_at_monotonic": None,
                "live_started_monotonic": None,
                "last_ping_baseline_monotonic": None,
                "last_ping_sent_monotonic": None,
                "last_pong_baseline_monotonic": None,
                "last_pong_received_monotonic": None,
                "pending_ack_subscriptions": set(),
                "live": False,
            }
            try:
                next_epoch = self.metrics.connection_epoch + 1
                self._drain_rest_refresh(wait=True)
                if not initial_bootstrap_complete:
                    self.metrics.state = CollectorState.BOOTSTRAPPING
                    self._publish_status()
                    self._collect_rest(
                        connection_id=connection_id,
                        connection_epoch=next_epoch,
                    )
                    self._flush()
                    initial_bootstrap_complete = True
                else:
                    self.metrics.state = CollectorState.RESYNCING
                    self._publish_status()
                    for asset in self.config.assets:
                        self._add_connection_event(
                            connection_id,
                            next_epoch,
                            asset=asset,
                            event_kind="resync_start",
                            reason="pre-connection REST state restoration",
                        )
                    self._collect_rest(
                        connection_id=connection_id,
                        connection_epoch=next_epoch,
                    )
                    for asset in self.config.assets:
                        self._add_connection_event(
                            connection_id,
                            next_epoch,
                            asset=asset,
                            event_kind="resync_complete",
                            reason="state snapshots restored; trade coverage remains unknown",
                        )
                    self.metrics.resyncs += 1
                    self._flush()

                self.metrics.state = CollectorState.CONNECTING
                self._publish_status()
                if not self._limit_connection_rate():
                    break
                socket = self.socket_factory.connect(
                    self.config.network, self.config.ws_connect_timeout_seconds
                )
                if self._should_stop(started, max_messages, duration_seconds):
                    self._close_socket(socket)
                    socket = None
                    break
                connect_received_at = ReceivedWireMessage(
                    "",
                    socket.connected_at,
                ).received_time
                connected = True
                self.metrics.connection_epoch = next_epoch
                self.metrics.connections += 1
                self.metrics.connection_alive = True
                self._active_socket = socket
                self._active_connection_id = connection_id
                self._active_generation = next_epoch
                if self.metrics.connections > 1:
                    self.metrics.reconnects += 1
                epoch = self.metrics.connection_epoch
                self._add_connection_event(
                    connection_id,
                    epoch,
                    event_kind="connect",
                    reason=None,
                    at=connect_received_at,
                )

                subscriptions = self.config.subscriptions()
                pending = {subscription.key for subscription in subscriptions}
                generation_liveness["pending_ack_subscriptions"] = pending
                for subscription in subscriptions:
                    socket.send_json(
                        {
                            "method": "subscribe",
                            "subscription": subscription.payload(),
                        }
                    )
                self.metrics.state = CollectorState.SUBSCRIBING
                connected_at = self.monotonic()
                last_ping = connected_at
                last_pong = connected_at
                generation_liveness.update(
                    {
                        "connected_at_monotonic": connected_at,
                        "last_ping_baseline_monotonic": last_ping,
                        "last_pong_baseline_monotonic": last_pong,
                    }
                )
                last_flush = connected_at
                last_status_publish = connected_at
                last_rest_refresh = connected_at
                live = False
                arrival_sequence = 0
                stale_baseline = self.clock()
                for subscription in subscriptions:
                    if subscription.channel in {
                        "activeAssetCtx",
                        "bbo",
                        "l2Book",
                        "trades",
                        "candle",
                    }:
                        self.metrics.last_event_by_channel[subscription.key] = stale_baseline

                while not self._should_stop(started, max_messages, duration_seconds):
                    self._backlog_liveness_active = False
                    received = socket.receive(1.0)
                    observed_mono = self.monotonic()
                    observed_at = self.clock()
                    wire_received_mono = observed_mono
                    if received is not None:
                        if isinstance(received, ReceivedWireMessage):
                            raw_message = received.raw_message
                            received_at = received.received_time
                            wire_received_mono = self._wire_received_monotonic_seconds(
                                received,
                                observed_monotonic=observed_mono,
                                observed_at=observed_at,
                            )
                        else:
                            raw_message = received
                            received_at = observed_at
                        arrival_sequence += 1
                        parsed = self._handle_message(
                            raw_message,
                            received_at,
                            connection_id,
                            epoch,
                            arrival_sequence,
                        )
                        if parsed.is_pong:
                            last_pong = wire_received_mono
                            generation_liveness["last_pong_baseline_monotonic"] = (
                                wire_received_mono
                            )
                            generation_liveness["last_pong_received_monotonic"] = (
                                wire_received_mono
                            )
                            self.metrics.pongs_received += 1
                        if parsed.acknowledged_subscription is not None:
                            self.metrics.subscription_acks += 1
                            pending.discard(self._subscription_key(parsed.acknowledged_subscription))

                    if not pending and not live:
                        self.metrics.state = CollectorState.LIVE
                        self.metrics.last_recovered_at = self.clock()
                        if self.metrics.last_error is not None:
                            self.metrics.last_error = None
                            self.metrics.last_error_at = None
                            self._publish_status()
                        self.metrics.current_backoff_seconds = 0.0
                        live = True
                        generation_liveness["live"] = True
                        generation_liveness["live_started_monotonic"] = observed_mono
                        self._live_since = observed_mono
                        last_flush = observed_mono

                    if (
                        live
                        and self._live_since is not None
                        and observed_mono - self._live_since >= self.config.backoff_reset_after_seconds
                    ):
                        self._backoff.reset()
                        self._live_since = None

                    if observed_mono - last_ping >= self.config.heartbeat_interval_seconds:
                        self._runtime_telemetry.record_worker_scheduling_lag(
                            round((last_ping + self.config.heartbeat_interval_seconds) * 1_000_000_000),
                            observed_monotonic_ns=round(observed_mono * 1_000_000_000),
                        )
                        socket.send_json({"method": "ping"})
                        self.metrics.pings_sent += 1
                        last_ping = observed_mono
                        generation_liveness["last_ping_baseline_monotonic"] = observed_mono
                        generation_liveness["last_ping_sent_monotonic"] = observed_mono
                    if observed_mono - last_pong > self.config.pong_timeout_seconds:
                        backlog_state, _snapshot = self._socket_backlog_state(
                            socket,
                            max_age_seconds=self.config.pong_timeout_seconds,
                        )
                        if backlog_state == "exhausted":
                            raise WebsocketConsumerBackpressure(
                                "Hyperliquid public websocket oldest queued message exceeded "
                                "the unchanged pong deadline; local consumer capacity exhausted"
                            )
                        raise TimeoutError("Hyperliquid pong deadline exceeded")
                    if pending and observed_mono - connected_at > self.config.pong_timeout_seconds:
                        backlog_state, _snapshot = self._socket_backlog_state(
                            socket,
                            max_age_seconds=self.config.pong_timeout_seconds,
                        )
                        if backlog_state == "exhausted":
                            raise WebsocketConsumerBackpressure(
                                "Hyperliquid public websocket oldest queued message exceeded "
                                "the unchanged subscription acknowledgement deadline; local "
                                "consumer capacity exhausted"
                            )
                        raise TimeoutError(f"subscription acknowledgements missing: {sorted(pending)}")

                    self._update_staleness(observed_at)
                    critical_stale = tuple(
                        key for key in self.metrics.stale_channels if key in self._critical_stream_keys
                    )
                    if live and critical_stale:
                        backlog_state, _snapshot = self._socket_backlog_state(
                            socket,
                            max_age_seconds=self.config.stale_after_seconds,
                        )
                        if backlog_state == "exhausted":
                            raise WebsocketConsumerBackpressure(
                                "Hyperliquid public websocket oldest queued message exceeded "
                                "the unchanged critical-stream stale deadline; local consumer "
                                "capacity exhausted"
                            )
                        raise TimeoutError(f"stale critical public streams: {list(critical_stale)}")
                    if (
                        live
                        and self._rest_future is None
                        and self.config.rest_refresh_enabled
                        and (observed_mono - last_rest_refresh >= self.config.rest_refresh_interval_seconds)
                    ):
                        self._schedule_rest_refresh(connection_id, epoch, observed_at)
                        last_rest_refresh = observed_mono
                    self._drain_rest_refresh(wait=False)
                    if live and (
                        self.sink.should_flush
                        or observed_mono - last_flush >= self.config.flush_interval_seconds
                    ):
                        self._flush(durable=False)
                        self._publish_status()
                        last_flush = observed_mono
                        last_status_publish = observed_mono
                    elif observed_mono - last_status_publish >= self.config.flush_interval_seconds:
                        self._publish_status()
                        last_status_publish = observed_mono

                self._add_connection_event(
                    connection_id,
                    epoch,
                    event_kind="disconnect",
                    reason="collector stop requested or bounded run completed",
                )
                self._flush()
                self._close_socket_with_telemetry(
                    socket,
                    reason="collector stop requested or bounded run completed",
                )
                self.metrics.connection_alive = False
                socket = None
            except CoordinatedWriterError as exc:
                self._record_generation_failure(
                    socket=socket,
                    connection_id=connection_id,
                    generation=next_epoch,
                    connected=connected,
                    error=exc,
                    will_reconnect=False,
                    liveness=generation_liveness,
                )
                self.metrics.connection_alive = False
                if socket is not None:
                    self._close_socket_with_telemetry(
                        socket,
                        reason=f"{type(exc).__name__}: {exc}",
                    )
                    socket = None
                with suppress(Exception):
                    self._publish_status(
                        error=f"{type(exc).__name__}: {exc}",
                    )
                raise
            except Exception as exc:
                if self._flush_failure is not None:
                    with suppress(Exception):
                        self._publish_status(error=f"{type(exc).__name__}: {exc}")
                    raise
                if isinstance(exc, InterruptedError) and self._stop_requested:
                    if connected:
                        self._add_connection_event(
                            connection_id,
                            self.metrics.connection_epoch,
                            event_kind="disconnect",
                            reason="collector stop requested or bounded run completed",
                        )
                    if self.sink.pending_count:
                        self._flush()
                    self.metrics.connection_alive = False
                    if socket is not None:
                        self._close_socket_with_telemetry(
                            socket,
                            reason="collector stop requested",
                        )
                        socket = None
                    break
                fatal_local_capacity = isinstance(
                    exc,
                    (WebsocketQueueOverflow, WebsocketConsumerBackpressure),
                )
                self._record_generation_failure(
                    socket=socket,
                    connection_id=connection_id,
                    generation=next_epoch,
                    connected=connected,
                    error=exc,
                    will_reconnect=not fatal_local_capacity,
                    liveness=generation_liveness,
                )
                self.metrics.connection_alive = False
                if socket is not None:
                    self._close_socket_with_telemetry(
                        socket,
                        reason=f"{type(exc).__name__}: {exc}",
                    )
                    socket = None
                if connected:
                    if self.sink.pending_count:
                        self._flush()
                    self._record_disconnect_gap(connection_id, f"{type(exc).__name__}: {exc}")
                    self._flush()
                self._publish_status(error=f"{type(exc).__name__}: {exc}")
                if fatal_local_capacity:
                    raise
                if self._should_stop(started, max_messages, duration_seconds):
                    break
                self.metrics.state = CollectorState.BACKOFF
                delay = self._backoff.next_delay()
                self.metrics.current_backoff_seconds = delay
                self._publish_status()
                self._interruptible_sleep(delay)

        self.metrics.state = CollectorState.STOPPED
        self.metrics.connection_alive = False
        if self._rest_future is not None and self._rest_future.done():
            self._drain_rest_refresh(wait=False)
        self._update_staleness(self.clock())
        self._flush()
        self._publish_status()
        return self.metrics

    def _should_stop(
        self,
        started: float,
        max_messages: int,
        duration_seconds: float | None,
    ) -> bool:
        if self._stop_requested:
            return True
        if max_messages and self.metrics.messages_received >= max_messages:
            return True
        return duration_seconds is not None and self.monotonic() - started >= duration_seconds

    def _interruptible_sleep(self, delay: float) -> bool:
        remaining = max(delay, 0.0)
        while remaining > 0 and not self._stop_requested:
            step = min(remaining, 0.25)
            self.sleeper(step)
            remaining -= step
        return not self._stop_requested

    def _limit_connection_rate(self) -> bool:
        while not self._stop_requested:
            now = self.monotonic()
            while self._connection_attempts and now - self._connection_attempts[0] >= 60:
                self._connection_attempts.popleft()
            if len(self._connection_attempts) < 29:
                self._connection_attempts.append(now)
                return True
            delay = max(60 - (now - self._connection_attempts[0]), 0.001)
            if not self._interruptible_sleep(delay):
                return False
        return False

    @staticmethod
    def _close_socket(socket: PublicSocket) -> None:
        # Cleanup must not hide the original disconnect or interrupt.
        with suppress(Exception):
            socket.close()

    def _close_socket_with_telemetry(
        self,
        socket: PublicSocket,
        *,
        reason: str,
    ) -> None:
        before_close = self._socket_telemetry(socket)
        self._close_socket(socket)
        after_close = self._socket_telemetry(socket)
        self._last_closed_socket_telemetry = {
            "generation": self._active_generation,
            "connection_id": self._active_connection_id,
            "closed_at": self.clock().isoformat(),
            "reason": reason,
            "telemetry_before_close": before_close,
            "telemetry_after_close": after_close,
        }
        if self._active_socket is socket:
            self._active_socket = None
            self._active_connection_id = None
            self._active_generation = None

    @staticmethod
    def _socket_telemetry(socket: PublicSocket | None) -> dict[str, object] | None:
        if socket is None:
            return None
        snapshot = getattr(socket, "telemetry_snapshot", None)
        if not callable(snapshot):
            return None
        try:
            value = snapshot()
        except Exception as exc:
            return {
                "telemetry_error": f"{type(exc).__name__}: {exc}",
            }
        if not isinstance(value, Mapping):
            return {
                "telemetry_error": "socket telemetry snapshot is not a mapping",
            }
        return dict(value)

    def _wire_received_monotonic_seconds(
        self,
        received: ReceivedWireMessage,
        *,
        observed_monotonic: float,
        observed_at: datetime,
    ) -> float:
        wall_age = max((observed_at - received.received_time).total_seconds(), 0.0)
        expected_candidate = max(observed_monotonic - wall_age, 0.0)
        received_monotonic_ns = received.received_monotonic_ns
        if received_monotonic_ns is None:
            self._wire_monotonic_legacy_fallbacks += 1
            return expected_candidate
        candidate = received_monotonic_ns / 1_000_000_000
        domain_tolerance = max(
            self.config.pong_timeout_seconds,
            self.config.stale_after_seconds,
            1.0,
        )
        if abs(candidate - expected_candidate) > domain_tolerance:
            self._wire_monotonic_domain_fallbacks += 1
            return expected_candidate
        return min(candidate, observed_monotonic)

    def _socket_backlog_state(
        self,
        socket: PublicSocket,
        *,
        max_age_seconds: float,
    ) -> tuple[str, dict[str, object] | None]:
        snapshot = self._socket_telemetry(socket)
        if snapshot is None:
            return "none", None
        depth = snapshot.get("queue_depth")
        oldest_age_ms = snapshot.get("oldest_message_age_ms")
        latest_age_ms = snapshot.get("latest_message_received_age_ms")
        if (
            snapshot.get("reader_alive") is not True
            or snapshot.get("terminal_exception_type") is not None
            or snapshot.get("terminal_reason") is not None
            or isinstance(depth, bool)
            or not isinstance(depth, int)
            or depth <= 0
            or isinstance(oldest_age_ms, bool)
            or not isinstance(oldest_age_ms, (int, float))
            or isinstance(latest_age_ms, bool)
            or not isinstance(latest_age_ms, (int, float))
        ):
            return "none", snapshot
        threshold_ms = max_age_seconds * 1_000
        if oldest_age_ms > threshold_ms:
            return "exhausted", snapshot
        if latest_age_ms <= threshold_ms:
            return "draining", snapshot
        return "none", snapshot

    @staticmethod
    def _safe_mapping_snapshot(
        snapshot: Callable[[], object],
        *,
        label: str,
    ) -> dict[str, object]:
        try:
            value = snapshot()
        except Exception as exc:
            return {"telemetry_error": f"{label}: {type(exc).__name__}: {exc}"}
        if not isinstance(value, Mapping):
            return {"telemetry_error": f"{label} snapshot is not a mapping"}
        return dict(value)

    @staticmethod
    def _monotonic_age_ms(observed: float | None, candidate: object) -> float | None:
        if (
            observed is None
            or isinstance(candidate, bool)
            or not isinstance(candidate, (int, float))
        ):
            return None
        return max(observed - float(candidate), 0.0) * 1_000

    def _record_generation_failure(
        self,
        *,
        socket: PublicSocket | None,
        connection_id: str,
        generation: int,
        connected: bool,
        error: BaseException,
        will_reconnect: bool,
        liveness: Mapping[str, object] | None = None,
    ) -> None:
        detail = str(error).strip()
        reason = type(error).__name__ if not detail else f"{type(error).__name__}: {detail}"
        recorded_at = self.clock()
        try:
            failure_observed_monotonic = self.monotonic()
        except Exception:
            failure_observed_monotonic = None
        context: Mapping[str, object] = {} if liveness is None else liveness
        pending_value = context.get("pending_ack_subscriptions")
        if isinstance(pending_value, (set, frozenset, list, tuple)):
            pending_ack_subscriptions = sorted(str(value) for value in pending_value)
        else:
            pending_ack_subscriptions = []
        process_snapshot = self._safe_mapping_snapshot(
            self._runtime_telemetry.snapshot,
            label="process runtime telemetry",
        )
        writer_metrics = getattr(self.sink, "metrics_snapshot", None)
        writer_snapshot = (
            self._safe_mapping_snapshot(writer_metrics, label="writer telemetry")
            if callable(writer_metrics)
            else None
        )
        failure_snapshot: dict[str, object] = {
            "collector": self.metrics.as_dict(recorded_at),
            "process": process_snapshot,
            "writer": writer_snapshot,
            "liveness": {
                "collector_state": self.metrics.state.value,
                "connected": connected,
                "live": context.get("live") is True,
                "connection_age_ms": self._monotonic_age_ms(
                    failure_observed_monotonic,
                    context.get("connected_at_monotonic"),
                ),
                "live_duration_ms": self._monotonic_age_ms(
                    failure_observed_monotonic,
                    context.get("live_started_monotonic"),
                ),
                "pending_ack_count": len(pending_ack_subscriptions),
                "pending_ack_subscriptions": pending_ack_subscriptions,
                "ping": {
                    "deadline_baseline_age_ms": self._monotonic_age_ms(
                        failure_observed_monotonic,
                        context.get("last_ping_baseline_monotonic"),
                    ),
                    "last_sent_age_ms": self._monotonic_age_ms(
                        failure_observed_monotonic,
                        context.get("last_ping_sent_monotonic"),
                    ),
                },
                "pong": {
                    "deadline_baseline_age_ms": self._monotonic_age_ms(
                        failure_observed_monotonic,
                        context.get("last_pong_baseline_monotonic"),
                    ),
                    "last_received_age_ms": self._monotonic_age_ms(
                        failure_observed_monotonic,
                        context.get("last_pong_received_monotonic"),
                    ),
                },
            },
        }
        self._generation_failures_seen += 1
        self._generation_failures.append(
            {
                "generation": generation if connected else None,
                "connection_attempt": generation,
                "connection_id": connection_id,
                "connected": connected,
                "recorded_at": recorded_at.isoformat(),
                "reason": reason,
                "socket": self._socket_telemetry(socket),
                "failure_snapshot": failure_snapshot,
                "will_reconnect": will_reconnect,
            }
        )

    @staticmethod
    def _subscription_key(subscription: Mapping[str, Any]) -> str:
        return PublicSubscription.from_payload(subscription).key

    def _handle_message(
        self,
        raw_message: str,
        observed_at: datetime,
        connection_id: str,
        connection_epoch: int,
        arrival_sequence: int,
    ) -> ParsedMessage:
        envelope = WireEnvelope(
            raw_message=raw_message,
            received_time=observed_at,
            connection_id=connection_id,
            connection_epoch=connection_epoch,
            arrival_sequence=arrival_sequence,
            capture_epoch_id=(f"hyperliquid-capture-{connection_epoch}-{connection_id}"),
        )
        normalization_started = self.monotonic()
        try:
            parsed = parse_websocket_message(envelope)
        finally:
            self._normalization_timing.observe_seconds(max(self.monotonic() - normalization_started, 0.0))
        self.metrics.messages_received += 1
        self.metrics.last_received_at = observed_at
        self.metrics.records_parsed += len(parsed.records)
        self.metrics.normalization_issues += len(parsed.issues)
        if parsed.is_pong:
            self.metrics.last_pong_at = observed_at
        if parsed.channel is not None:
            assets = {
                record.asset for record in parsed.records if record.record_type != RecordType.WIRE_MESSAGE
            }
            for asset in assets:
                channel = "activeAssetCtx" if parsed.channel == "activeSpotAssetCtx" else parsed.channel
                if channel == "candle":
                    metric_keys = {
                        PublicSubscription(
                            channel=channel,
                            coin=asset,
                            interval=str(record.row["interval"]),
                        ).key
                        for record in parsed.records
                        if record.asset == asset and record.record_type == RecordType.CANDLE
                    }
                else:
                    metric_keys = {PublicSubscription(channel=channel, coin=asset).key}
                for metric_key in metric_keys:
                    self.metrics.last_event_by_channel[metric_key] = observed_at
                latest: datetime | None = None
                for record in parsed.records:
                    if record.record_type == RecordType.WIRE_MESSAGE or record.asset != asset:
                        continue
                    exchange_time = record.row.get("exchange_time")
                    if isinstance(exchange_time, datetime) and (latest is None or exchange_time > latest):
                        latest = exchange_time
                if latest is not None:
                    for metric_key in metric_keys:
                        current = self.metrics.last_exchange_event_by_channel.get(metric_key)
                        if current is None or latest > current:
                            self.metrics.last_exchange_event_by_channel[metric_key] = latest
        self._store_records(parsed.records)
        return parsed

    def _store_record(self, record: ParsedRecord) -> None:
        self._store_records((record,))

    def _store_records(self, records: Iterable[ParsedRecord]) -> None:
        batch = tuple(records)
        for record in batch:
            if record.record_type == RecordType.FUNDING:
                funding_time = record.row.get("funding_time")
                if isinstance(funding_time, datetime):
                    current = self.metrics.last_funding_by_asset.get(record.asset)
                    if current is None or funding_time > current:
                        self.metrics.last_funding_by_asset[record.asset] = funding_time
        enqueue_started = self.monotonic()
        try:
            self.sink.add_many(batch)
        finally:
            self._sink_enqueue_timing.observe_seconds(max(self.monotonic() - enqueue_started, 0.0))
        self._collect_completed()
        self.metrics.queue_high_water = max(
            self.metrics.queue_high_water,
            self.sink.high_water,
        )
        if self.sink.should_flush:
            self._flush()

    def _collect_completed(self) -> bool:
        collect_completed = getattr(self.sink, "collect_completed", None)
        if not callable(collect_completed):
            return False
        self._apply_flush_result(collect_completed())
        return True

    def _flush(self, *, durable: bool = True) -> None:
        if not durable:
            self._collect_completed()
            request_flush = getattr(self.sink, "request_flush", None)
            if callable(request_flush):
                request_flush()
                return
        self._flush_attempts += 1
        try:
            result = self.sink.flush()
        except (DataLakeError, OSError) as exc:
            self._flush_failure = exc
            raise
        self._successful_flush_attempts += 1
        self._apply_flush_result(result)

    def _apply_flush_result(self, result: FlushResult) -> None:
        self.metrics.duplicates_suppressed += result.duplicate_count
        if result.row_count:
            self.metrics.batches_written += 1
            self.metrics.rows_written += result.row_count

    def _update_staleness(self, now: datetime) -> None:
        stale: list[str] = []
        for key, observed_at in self.metrics.last_event_by_channel.items():
            age = (now - observed_at).total_seconds()
            if key in self._critical_stream_keys:
                if age > self.config.stale_after_seconds:
                    stale.append(key)
            elif key in self._candle_intervals_by_stream_key:
                interval = self._candle_intervals_by_stream_key[key]
                if age > _candle_interval_seconds(interval) + self.config.stale_after_seconds:
                    stale.append(key)
        if self.config.collect_funding_history:
            expected_funding = (now - timedelta(seconds=self.config.funding_grace_seconds)).replace(
                minute=0,
                second=0,
                microsecond=0,
            )
            for asset in self.config.assets:
                if asset.startswith("@") or "/" in asset:
                    continue
                latest = self.metrics.last_funding_by_asset.get(asset)
                if latest is None or latest < expected_funding - timedelta(seconds=60):
                    stale.append(f"fundingHistory:{asset}")
        self.metrics.stale_channels = tuple(sorted(stale))

    def _publish_status(self, *, error: str | None = None) -> None:
        now = self.clock()
        if error is not None:
            self.metrics.last_error = error
            self.metrics.last_error_at = now
            self.metrics.last_failure = error
            self.metrics.last_failure_at = now
        payload: dict[str, object] = {
            "schema_version": 1,
            "ok": (
                self.metrics.state == CollectorState.LIVE
                and self.metrics.connection_alive
                and error is None
                and self.metrics.last_error is None
                and not self.metrics.stale_channels
                and not self._backlog_liveness_active
            ),
            "mode": "readonly",
            "orders_enabled": False,
            "network": self.config.network,
            "updated_at": now.isoformat(),
            "pending_rows": self.sink.pending_count,
            "unclean_restart_detected": bool(
                getattr(self.sink, "unclean_restart_detected", False)
            ),
            "metrics": self.metrics.as_dict(now),
        }
        observability: dict[str, object] = {
            "process": self._runtime_telemetry.snapshot(),
            "reconnect_reasons_by_generation": list(self._generation_failures),
            "generation_reason_history": {
                "capacity": _GENERATION_REASON_HISTORY_CAPACITY,
                "seen": self._generation_failures_seen,
                "retained": len(self._generation_failures),
                "truncated": max(
                    self._generation_failures_seen - len(self._generation_failures),
                    0,
                ),
            },
            "liveness": {
                "fresh_backlog_deferrals": self._backlog_liveness_deferrals,
                "fresh_backlog_active": self._backlog_liveness_active,
                "wire_monotonic_legacy_fallbacks": self._wire_monotonic_legacy_fallbacks,
                "wire_monotonic_domain_fallbacks": self._wire_monotonic_domain_fallbacks,
            },
            "worker_phases": {
                "normalization_ms": self._normalization_timing.as_dict(),
                "sink_enqueue_ms": self._sink_enqueue_timing.as_dict(),
                "rest": {
                    "bootstrap_materialization_ms": (self._rest_bootstrap_materialization_timing.as_dict()),
                    "refresh_worker_materialization_ms": (
                        self._rest_refresh_worker_materialization_timing.as_dict()
                    ),
                    "refresh_supervisor_apply_ms": (self._rest_refresh_supervisor_apply_timing.as_dict()),
                    "bootstrap_rows_total": self._rest_bootstrap_rows_total,
                    "refresh_rows_materialized_total": (self._rest_refresh_rows_materialized_total),
                    "refresh_rows_applied_total": self._rest_refresh_rows_applied_total,
                    "last_bootstrap_rows": self._last_rest_bootstrap_rows,
                    "last_refresh_rows_materialized": (self._last_rest_refresh_rows_materialized),
                    "last_refresh_rows_applied": self._last_rest_refresh_rows_applied,
                },
            },
        }
        socket_telemetry = self._socket_telemetry(self._active_socket)
        if socket_telemetry is not None:
            observability["socket"] = socket_telemetry
        if self._last_closed_socket_telemetry is not None:
            observability["last_closed_socket"] = dict(self._last_closed_socket_telemetry)
        writer_metrics = getattr(self.sink, "metrics_snapshot", None)
        if callable(writer_metrics):
            observability["writer"] = writer_metrics()
        source_queue_metrics = getattr(self.sink, "source_queue_snapshot", None)
        if callable(source_queue_metrics):
            observability["source_queue"] = source_queue_metrics(as_of=now)
        payload["observability"] = observability
        if self.metrics.last_error is not None:
            payload["error"] = self.metrics.last_error
        write_runtime_status(self.runtime_status_path, payload)

    def _collect_rest(
        self,
        *,
        connection_id: str,
        connection_epoch: int,
    ) -> None:
        materialization_started = self.monotonic()
        rest_connection_id = (
            f"rest-bootstrap-{connection_epoch}-{connection_id}"
            if self._producer_scoped_rest_connection_ids
            else connection_id
        )
        try:
            records = tuple(
                self._iter_rest_records(
                    connection_id=rest_connection_id,
                    connection_epoch=connection_epoch,
                    history_hours=self.config.history_lookback_hours,
                    include_l2=True,
                    query_end=self.clock(),
                )
            )
        finally:
            self._rest_bootstrap_materialization_timing.observe_seconds(
                max(self.monotonic() - materialization_started, 0.0)
            )
        self._last_rest_bootstrap_rows = len(records)
        self._rest_bootstrap_rows_total += len(records)
        if records:
            self._store_records(records)

    def _iter_rest_records(
        self,
        *,
        connection_id: str,
        connection_epoch: int,
        history_hours: int,
        include_l2: bool,
        query_end: datetime,
    ) -> Iterator[ParsedRecord]:
        end_ms = int(query_end.timestamp() * 1_000)
        raw_bootstrap = self.rest.bootstrap()
        bootstrap_received = self.clock()
        bootstrap = PublicBootstrap(
            observed_at_ms=int(bootstrap_received.timestamp() * 1_000),
            perp_payload=raw_bootstrap.perp_payload,
            spot_payload=raw_bootstrap.spot_payload,
        )
        bootstrap_records = parse_bootstrap(
            bootstrap,
            connection_id=connection_id,
            connection_epoch=connection_epoch,
        )
        requested_assets = frozenset(self.config.assets)
        yield from (
            record for record in bootstrap_records if record.asset in requested_assets
        )

        start_ms = end_ms - history_hours * 3_600_000
        if self._stop_requested:
            return
        rest_sequence = 2
        for asset in self.config.assets:
            if self._stop_requested:
                return
            if self.config.collect_funding_history and not asset.startswith("@") and "/" not in asset:
                funding_pages = getattr(self.rest, "funding_history_pages", None)
                if callable(funding_pages):
                    pages = funding_pages(asset, start_ms, end_ms)
                else:
                    pages = (self.rest.funding_history(asset, start_ms, end_ms),)
                for page in pages:
                    if self._stop_requested:
                        return
                    received_at = self.clock()
                    envelope = historical_envelope(
                        received_at,
                        connection_id=connection_id,
                        connection_epoch=connection_epoch,
                        arrival_sequence=rest_sequence,
                    )
                    rest_sequence += 1
                    yield from parse_funding_history(page, envelope)

            for interval in self.config.candle_intervals:
                candle_pages = getattr(self.rest, "candle_pages", None)
                if callable(candle_pages):
                    pages = candle_pages(asset, interval, start_ms, end_ms)
                else:
                    pages = (self.rest.candles(asset, interval, start_ms, end_ms),)
                for page in pages:
                    if self._stop_requested:
                        return
                    received_at = self.clock()
                    envelope = historical_envelope(
                        received_at,
                        connection_id=connection_id,
                        connection_epoch=connection_epoch,
                        arrival_sequence=rest_sequence,
                    )
                    rest_sequence += 1
                    yield from parse_candles(page, envelope)

            if not include_l2:
                continue
            if self._stop_requested:
                return
            l2_payload = self.rest.l2_snapshot(asset)
            l2_received = self.clock()
            l2_envelope = historical_envelope(
                l2_received,
                connection_id=connection_id,
                connection_epoch=connection_epoch,
                arrival_sequence=rest_sequence,
            )
            rest_sequence += 1
            if "l2Book" in self.config.subscription_channels:
                yield from parse_l2_snapshot(l2_payload, l2_envelope)
            if "bbo" in self.config.subscription_channels:
                yield from parse_bbo_from_l2(l2_payload, l2_envelope)

    def _schedule_rest_refresh(
        self,
        connection_id: str,
        connection_epoch: int,
        query_end: datetime,
    ) -> None:
        if self._rest_future is not None:
            return
        self._rest_refresh_counter += 1
        refresh_id = f"rest-refresh-{connection_epoch}-{self._rest_refresh_counter}-{connection_id}"
        longest_interval_hours = max(
            (_candle_interval_seconds(interval) // 3_600 for interval in self.config.candle_intervals),
            default=0,
        )
        history_hours = max(2, longest_interval_hours + 1)
        self._rest_future = self._rest_executor.submit(
            self._materialize_rest_refresh,
            connection_id=refresh_id,
            connection_epoch=connection_epoch,
            history_hours=history_hours,
            query_end=query_end,
        )

    def _materialize_rest_refresh(
        self,
        *,
        connection_id: str,
        connection_epoch: int,
        history_hours: int,
        query_end: datetime,
    ) -> tuple[ParsedRecord, ...]:
        materialization_started = self.monotonic()
        try:
            return tuple(
                self._iter_rest_records(
                    connection_id=connection_id,
                    connection_epoch=connection_epoch,
                    history_hours=history_hours,
                    include_l2=False,
                    query_end=query_end,
                )
            )
        finally:
            self._rest_refresh_worker_materialization_timing.observe_seconds(
                max(self.monotonic() - materialization_started, 0.0)
            )

    def _drain_rest_refresh(self, *, wait: bool) -> None:
        future = self._rest_future
        if future is None or (not wait and not future.done()):
            return
        self._rest_future = None
        self._apply_rest_future(future)

    def _apply_rest_future(self, future: Future[tuple[ParsedRecord, ...]]) -> None:
        apply_started = self.monotonic()
        try:
            try:
                records = future.result()
            except Exception as exc:
                error = f"REST refresh failed: {type(exc).__name__}: {exc}"
                if isinstance(exc, InterruptedError) and self._stop_requested:
                    return
                self.metrics.gaps += 1
                self.metrics.last_error = error
                self.metrics.last_error_at = self.clock()
                self._add_connection_event(
                    f"rest-refresh-{self.metrics.connection_epoch}",
                    max(self.metrics.connection_epoch, 1),
                    event_kind="gap",
                    reason=f"coverage_unknown:{error}",
                )
                self._publish_status(error=error)
                if self.config.reconnect_on_rest_refresh_failure:
                    raise RuntimeError(error) from exc
                return
            row_count = len(records)
            self._last_rest_refresh_rows_materialized = row_count
            self._rest_refresh_rows_materialized_total += row_count
            if records:
                self._store_records(records)
            self._last_rest_refresh_rows_applied = row_count
            self._rest_refresh_rows_applied_total += row_count
            self.metrics.rest_refreshes += 1
            if self.metrics.last_error is not None and self.metrics.last_error.startswith(
                "REST refresh failed:"
            ):
                self.metrics.last_error = None
                self.metrics.last_error_at = None
                self.metrics.last_recovered_at = self.clock()
            self._publish_status()
        finally:
            self._rest_refresh_supervisor_apply_timing.observe_seconds(
                max(self.monotonic() - apply_started, 0.0)
            )

    def _add_connection_event(
        self,
        connection_id: str,
        connection_epoch: int,
        *,
        asset: str = "GLOBAL",
        event_kind: str,
        reason: str | None,
        at: datetime | None = None,
    ) -> None:
        now = self.clock() if at is None else at
        row: dict[str, object] = {
            "schema_version": latest_schema_for(RecordType.CONNECTION_EVENT).version,
            "record_type": RecordType.CONNECTION_EVENT.value,
            "venue": "hyperliquid",
            "asset": asset,
            "event_time": now,
            "exchange_time": None,
            "received_time": now,
            "source_sequence": None,
            "connection_id": connection_id,
            "event_kind": event_kind,
            "channel": None,
            "book_epoch_id": f"{connection_id}:{connection_epoch}",
            "reason": reason,
            "expected_sequence": None,
            "observed_sequence": None,
            "resync_snapshot_id": None,
            "connection_epoch": connection_epoch,
            "capture_epoch_id": (f"hyperliquid-capture-{connection_epoch}-{connection_id}"),
            "socket_role": "public",
        }
        self._store_record(
            ParsedRecord(
                RecordType.CONNECTION_EVENT,
                asset,
                row,
            )
        )

    def _record_disconnect_gap(self, connection_id: str, reason: str) -> None:
        epoch = self.metrics.connection_epoch
        self._add_connection_event(
            connection_id,
            epoch,
            event_kind="disconnect",
            reason=reason,
        )
        self._add_connection_event(
            connection_id,
            epoch,
            event_kind="gap",
            reason=(
                f"coverage_unknown:no public server sequence; state resync will follow; disconnect={reason}"
            ),
        )
        self.metrics.gaps += 1
