from __future__ import annotations

import random
import sys
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterator, Mapping
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
    WireEnvelope,
)
from hyperlab.collector.parser import parse_websocket_message
from hyperlab.collector.storage import BatchingLakeSink
from hyperlab.collector.websocket import (
    PublicSocket,
    PublicSocketFactory,
    ReceivedWireMessage,
)
from hyperlab.data.lake import PartitionValidationError
from hyperlab.data.schema import RecordType
from hyperlab.storage.sqlite import write_runtime_status


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
        sink: BatchingLakeSink,
        runtime_status_path: Path,
        clock: Callable[[], datetime] = _utc_now,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        random_value: Callable[[], float] = random.random,
        connection_id_factory: Callable[[], str] = lambda: uuid.uuid4().hex,
    ) -> None:
        self.config = config
        self.rest = rest
        self.socket_factory = socket_factory
        self.sink = sink
        self.runtime_status_path = runtime_status_path
        self.clock = clock
        self.monotonic = monotonic
        self.sleeper = sleeper
        self.connection_id_factory = connection_id_factory
        self.metrics = CollectorMetrics()
        self._stop_requested = False
        self._active_socket: PublicSocket | None = None
        self._rest_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="hyperlab-rest")
        self._rest_future: Future[tuple[ParsedRecord, ...]] | None = None
        self._rest_refresh_counter = 0
        self._closed = False
        self._flush_failure: PartitionValidationError | None = None
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
    ) -> PublicCollector:
        rest = HyperliquidPublicClient(
            network=config.network,
            timeout_seconds=request_timeout_seconds,
        )
        sink = BatchingLakeSink(
            data_dir / "lake",
            batch_size=config.batch_size,
            queue_capacity=config.queue_capacity,
        )
        return cls(
            config,
            rest=rest,
            socket_factory=socket_factory,
            sink=sink,
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
                self._close_socket(self._active_socket)
                self._active_socket = None
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
                except PartitionValidationError as exc:
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
        started = self.monotonic()
        initial_bootstrap_complete = False
        socket: PublicSocket | None = None

        while not self._should_stop(started, max_messages, duration_seconds):
            connection_id = f"ws-{self.metrics.connection_epoch + 1}-{self.connection_id_factory()}"
            connected = False
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
                connected = True
                self.metrics.connection_epoch = next_epoch
                self.metrics.connections += 1
                self.metrics.connection_alive = True
                self._active_socket = socket
                if self.metrics.connections > 1:
                    self.metrics.reconnects += 1
                epoch = self.metrics.connection_epoch
                self._add_connection_event(
                    connection_id,
                    epoch,
                    event_kind="connect",
                    reason=None,
                )

                subscriptions = self.config.subscriptions()
                pending = {subscription.key for subscription in subscriptions}
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
                last_flush = connected_at
                last_rest_refresh = connected_at
                live = False
                arrival_sequence = 0
                stale_baseline = self.clock()
                for subscription in subscriptions:
                    if subscription.channel in {"activeAssetCtx", "l2Book", "candle"}:
                        self.metrics.last_event_by_channel[subscription.key] = stale_baseline

                while not self._should_stop(started, max_messages, duration_seconds):
                    received = socket.receive(1.0)
                    observed_mono = self.monotonic()
                    observed_at = self.clock()
                    if received is not None:
                        if isinstance(received, ReceivedWireMessage):
                            raw_message = received.raw_message
                            received_at = received.received_time
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
                            last_pong = observed_mono
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
                        socket.send_json({"method": "ping"})
                        self.metrics.pings_sent += 1
                        last_ping = observed_mono
                    if observed_mono - last_pong > self.config.pong_timeout_seconds:
                        raise TimeoutError("Hyperliquid pong deadline exceeded")
                    if pending and observed_mono - connected_at > self.config.pong_timeout_seconds:
                        raise TimeoutError(f"subscription acknowledgements missing: {sorted(pending)}")

                    self._update_staleness(observed_at)
                    critical_stale = tuple(
                        key
                        for key in self.metrics.stale_channels
                        if key.startswith(("activeAssetCtx:", "l2Book:"))
                    )
                    if live and critical_stale:
                        raise TimeoutError(f"stale critical public streams: {list(critical_stale)}")
                    if (
                        live
                        and self._rest_future is None
                        and (observed_mono - last_rest_refresh >= self.config.rest_refresh_interval_seconds)
                    ):
                        self._schedule_rest_refresh(connection_id, epoch, observed_at)
                        last_rest_refresh = observed_mono
                    self._drain_rest_refresh(wait=False)
                    if live and (
                        self.sink.should_flush
                        or observed_mono - last_flush >= self.config.flush_interval_seconds
                    ):
                        self._flush()
                        self._publish_status()
                        last_flush = observed_mono

                self._add_connection_event(
                    connection_id,
                    epoch,
                    event_kind="disconnect",
                    reason="collector stop requested or bounded run completed",
                )
                self._flush()
                self._close_socket(socket)
                self._active_socket = None
                self.metrics.connection_alive = False
                socket = None
            except Exception as exc:
                if self._flush_failure is not None:
                    with suppress(Exception):
                        self._publish_status(error=f"{type(exc).__name__}: {exc}")
                    raise
                if isinstance(exc, InterruptedError) and self._stop_requested:
                    self.metrics.connection_alive = False
                    if socket is not None:
                        self._close_socket(socket)
                        self._active_socket = None
                        socket = None
                    if self.sink.pending_count:
                        self._flush()
                    break
                self.metrics.connection_alive = False
                if socket is not None:
                    self._close_socket(socket)
                    self._active_socket = None
                    socket = None
                if connected:
                    if self.sink.pending_count:
                        self._flush()
                    self._record_disconnect_gap(connection_id, str(exc))
                    self._flush()
                self._publish_status(error=f"{type(exc).__name__}: {exc}")
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

    @staticmethod
    def _subscription_key(subscription: Mapping[str, Any]) -> str:
        channel = str(subscription.get("type", ""))
        coin = str(subscription.get("coin", ""))
        interval = subscription.get("interval")
        suffix = f":{interval}" if interval is not None else ""
        return f"{channel}:{coin}{suffix}"

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
        )
        parsed = parse_websocket_message(envelope)
        self.metrics.messages_received += 1
        self.metrics.last_received_at = observed_at
        self.metrics.records_parsed += len(parsed.records)
        self.metrics.normalization_issues += len(parsed.issues)
        if parsed.is_pong:
            self.metrics.last_pong_at = observed_at
        if parsed.channel is not None:
            self.metrics.last_event_by_channel[parsed.channel] = observed_at
            assets = {
                record.asset for record in parsed.records if record.record_type != RecordType.WIRE_MESSAGE
            }
            for asset in assets:
                channel = "activeAssetCtx" if parsed.channel == "activeSpotAssetCtx" else parsed.channel
                metric_keys = [f"{channel}:{asset}"]
                if channel == "candle":
                    metric_keys.extend(
                        f"{channel}:{asset}:{record.row['interval']}"
                        for record in parsed.records
                        if record.asset == asset and record.record_type == RecordType.CANDLE
                    )
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
        for record in parsed.records:
            self._store_record(record)
        return parsed

    def _store_record(self, record: ParsedRecord) -> None:
        if record.record_type == RecordType.FUNDING:
            funding_time = record.row.get("funding_time")
            if isinstance(funding_time, datetime):
                current = self.metrics.last_funding_by_asset.get(record.asset)
                if current is None or funding_time > current:
                    self.metrics.last_funding_by_asset[record.asset] = funding_time
        self.sink.add(record)
        self.metrics.queue_high_water = max(
            self.metrics.queue_high_water,
            self.sink.high_water,
        )
        if self.sink.should_flush:
            self._flush()

    def _flush(self) -> None:
        self._flush_attempts += 1
        try:
            result = self.sink.flush()
        except PartitionValidationError as exc:
            self._flush_failure = exc
            raise
        self._successful_flush_attempts += 1
        self.metrics.duplicates_suppressed += result.duplicate_count
        if result.row_count:
            self.metrics.batches_written += 1
            self.metrics.rows_written += result.row_count

    def _update_staleness(self, now: datetime) -> None:
        stale: list[str] = []
        for key, observed_at in self.metrics.last_event_by_channel.items():
            age = (now - observed_at).total_seconds()
            if key.startswith(("activeAssetCtx:", "l2Book:")):
                if age > self.config.stale_after_seconds:
                    stale.append(key)
            elif key.startswith("candle:"):
                interval = key.rsplit(":", 1)[-1]
                if age > _candle_interval_seconds(interval) + self.config.stale_after_seconds:
                    stale.append(key)
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
            ),
            "mode": "readonly",
            "orders_enabled": False,
            "network": self.config.network,
            "updated_at": now.isoformat(),
            "pending_rows": self.sink.pending_count,
            "metrics": self.metrics.as_dict(now),
        }
        if self.metrics.last_error is not None:
            payload["error"] = self.metrics.last_error
        write_runtime_status(self.runtime_status_path, payload)

    def _collect_rest(
        self,
        *,
        connection_id: str,
        connection_epoch: int,
    ) -> None:
        for record in self._iter_rest_records(
            connection_id=connection_id,
            connection_epoch=connection_epoch,
            history_hours=self.config.history_lookback_hours,
            include_l2=True,
            query_end=self.clock(),
        ):
            self._store_record(record)

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
        yield from parse_bootstrap(
            bootstrap,
            connection_id=connection_id,
            connection_epoch=connection_epoch,
        )

        start_ms = end_ms - history_hours * 3_600_000
        if self._stop_requested:
            return
        rest_sequence = 2
        for asset in self.config.assets:
            if self._stop_requested:
                return
            if not asset.startswith("@") and "/" not in asset:
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
            yield from parse_l2_snapshot(l2_payload, l2_envelope)
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
        longest_interval_hours = (
            max(_candle_interval_seconds(interval) for interval in self.config.candle_intervals) // 3_600
        )
        history_hours = max(2, longest_interval_hours + 1)
        self._rest_future = self._rest_executor.submit(
            lambda: tuple(
                self._iter_rest_records(
                    connection_id=refresh_id,
                    connection_epoch=connection_epoch,
                    history_hours=history_hours,
                    include_l2=False,
                    query_end=query_end,
                )
            )
        )

    def _drain_rest_refresh(self, *, wait: bool) -> None:
        future = self._rest_future
        if future is None or (not wait and not future.done()):
            return
        self._rest_future = None
        self._apply_rest_future(future)

    def _apply_rest_future(self, future: Future[tuple[ParsedRecord, ...]]) -> None:
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
            return
        for record in records:
            self._store_record(record)
        self.metrics.rest_refreshes += 1
        if self.metrics.last_error is not None and self.metrics.last_error.startswith("REST refresh failed:"):
            self.metrics.last_error = None
            self.metrics.last_error_at = None
            self.metrics.last_recovered_at = self.clock()
        self._publish_status()

    def _add_connection_event(
        self,
        connection_id: str,
        connection_epoch: int,
        *,
        asset: str = "GLOBAL",
        event_kind: str,
        reason: str | None,
    ) -> None:
        now = self.clock()
        row: dict[str, object] = {
            "schema_version": 1,
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
