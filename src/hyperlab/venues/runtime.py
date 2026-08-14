from __future__ import annotations

import time
import uuid
from collections import deque
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from math import ceil
from pathlib import Path
from statistics import median

from hyperlab.collector.models import ParsedMessage, ParsedRecord, WireEnvelope
from hyperlab.collector.storage import BatchingLakeSink, CoordinatedWriterError, LakeSink
from hyperlab.collector.telemetry import (
    MonotonicTimingSummary,
    ProcessRuntimeTelemetry,
)
from hyperlab.collector.websocket import (
    PublicSocket,
    UrlWebsocketClientFactory,
    WebsocketConsumerBackpressure,
    WebsocketQueueOverflow,
)
from hyperlab.data.schema import RecordType, latest_schema_for
from hyperlab.storage.sqlite import write_runtime_status
from hyperlab.venues.base import ClockMeasurement
from hyperlab.venues.binance import (
    VENUE,
    BinancePublicConnector,
    BinancePublicRestClient,
    clock_record,
    funding_intervals,
    parse_funding_history,
    parse_klines,
)

_CLOCK_OBSERVABILITY_CAPACITY = 256
_GENERATION_REASON_HISTORY_CAPACITY = 32
_OBSERVABILITY_ERROR_MESSAGE_LIMIT = 512
_MAX_CONSECUTIVE_REJECTED_CLOCK_PROBES = 1
_STRICT_MAX_CLOCK_SAMPLING_INTERVAL_SECONDS = 10.0
_STRICT_MAX_CLOCK_AGE_SECONDS = 15.0
_STRICT_MAX_CLOCK_UNCERTAINTY_MS = Decimal("50")


@dataclass(slots=True)
class _ClockFutureContext:
    capture_epoch_id: str
    connection_id: str
    connection_epoch: int
    submitted_at: float
    worker_started_at: float | None = None
    worker_completed_at: float | None = None


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class ReferenceCollectorConfig:
    assets: tuple[str, ...] = ("BTC", "ETH")
    candle_intervals: tuple[str, ...] = ("1m",)
    history_lookback_hours: int = 24
    batch_size: int = 500
    queue_capacity: int = 10_000
    connect_timeout_seconds: float = 15.0
    stale_after_seconds: float = 30.0
    clock_sampling_interval_seconds: float = 5.0
    clock_max_age_seconds: float = 15.0
    clock_max_uncertainty_ms: Decimal = Decimal("50")

    def __post_init__(self) -> None:
        if not self.assets or len(self.assets) != len(set(self.assets)):
            raise ValueError("assets must be a non-empty unique list")
        if not self.candle_intervals or len(self.candle_intervals) != len(set(self.candle_intervals)):
            raise ValueError("candle intervals must be a non-empty unique list")
        if any(value <= 0 for value in (self.history_lookback_hours, self.batch_size, self.queue_capacity)):
            raise ValueError("reference collector limits must be positive")
        if self.queue_capacity < self.batch_size:
            raise ValueError("queue capacity cannot be smaller than batch size")
        if (
            self.connect_timeout_seconds <= 0
            or self.stale_after_seconds <= 0
            or self.clock_sampling_interval_seconds <= 0
            or self.clock_max_age_seconds <= 0
        ):
            raise ValueError("reference collector timeouts must be positive")
        if self.clock_max_age_seconds < self.clock_sampling_interval_seconds:
            raise ValueError("clock maximum age cannot be shorter than its sampling interval")
        if self.clock_sampling_interval_seconds > _STRICT_MAX_CLOCK_SAMPLING_INTERVAL_SECONDS:
            raise ValueError("clock sampling interval cannot exceed the strict 10 second gate")
        if self.clock_max_age_seconds > _STRICT_MAX_CLOCK_AGE_SECONDS:
            raise ValueError("clock maximum age cannot exceed the strict 15 second gate")
        if not self.clock_max_uncertainty_ms.is_finite() or self.clock_max_uncertainty_ms < 0:
            raise ValueError("clock maximum uncertainty must be finite and non-negative")
        if self.clock_max_uncertainty_ms > _STRICT_MAX_CLOCK_UNCERTAINTY_MS:
            raise ValueError("clock maximum uncertainty cannot exceed the strict 50 ms gate")


class _SocketRoleFailure(ConnectionError):
    def __init__(
        self,
        socket_role: str,
        error: BaseException,
        *,
        operation: str = "receive",
    ) -> None:
        self.socket_role = socket_role
        self.original_error = error
        self.original_type = type(error).__name__
        self.operation = operation
        detail = str(error).strip()
        reason = self.original_type if not detail else f"{self.original_type}: {detail}"
        super().__init__(f"Binance {socket_role} websocket {operation} failed: {reason}")

    def as_diagnostic(self) -> dict[str, str]:
        detail = str(self.original_error).strip()
        reason = self.original_type if not detail else f"{self.original_type}: {detail}"
        return {
            "socket_role": self.socket_role,
            "operation": self.operation,
            "exception_type": self.original_type,
            "reason": reason,
        }


class _PairedSocketHandshakeFailure(_SocketRoleFailure):
    def __init__(self, failures: tuple[_SocketRoleFailure, ...]) -> None:
        if not failures:
            raise ValueError("paired handshake failure requires at least one socket failure")
        self.failures = failures
        primary = failures[0]
        super().__init__(primary.socket_role, primary.original_error, operation="handshake")
        summary = "; ".join(
            f"{failure.socket_role}={failure.as_diagnostic()['reason']}" for failure in failures
        )
        self.args = (f"Binance paired websocket handshake failed: {summary}",)


class BinanceReferenceCollector:
    """Small supervised collector limited to Binance public market data."""

    def __init__(
        self,
        config: ReferenceCollectorConfig,
        *,
        rest: BinancePublicRestClient,
        sink: LakeSink,
        runtime_status_path: Path,
        clock: Callable[[], datetime] = _utc_now,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.rest = rest
        self.sink = sink
        self.runtime_status_path = runtime_status_path
        self.clock = clock
        self.monotonic = monotonic
        self._stop = False
        self._closed = False
        self._sockets: dict[str, PublicSocket] = {}
        self._active_connection_epoch: int | None = None
        self._active_capture_epoch_id: str | None = None
        self._last_closed_socket_telemetry: dict[str, object] | None = None
        self._connector: BinancePublicConnector | None = None
        self._critical_stream_last_received: dict[str, datetime] = {}
        self._critical_stream_expectations: dict[
            str,
            tuple[str, RecordType, str],
        ] = {}
        self._critical_stream_seen: set[str] = set()
        self._valid_clock_until: datetime | None = None
        self._clock_rejection_streak = 0
        self._clock_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="binance-public-clock",
        )
        self._clock_future: Future[ClockMeasurement] | None = None
        self._clock_future_context: _ClockFutureContext | None = None
        self._clock_observations: deque[dict[str, object]] = deque(maxlen=_CLOCK_OBSERVABILITY_CAPACITY)
        self._clock_observations_seen = 0
        self._clock_schedule_overdue = MonotonicTimingSummary()
        self._clock_single_flight_blocked = MonotonicTimingSummary()
        self._runtime_telemetry = ProcessRuntimeTelemetry(
            monotonic_ns=lambda: round(self.monotonic() * 1_000_000_000),
            auto_start=False,
        )
        self._generation_failures: deque[dict[str, object]] = deque(
            maxlen=_GENERATION_REASON_HISTORY_CAPACITY
        )
        self._generation_failures_seen = 0
        self._backlog_liveness_deferrals = 0
        self._normalization_timing = MonotonicTimingSummary()
        self._sink_enqueue_timing = MonotonicTimingSummary()
        self.metrics: dict[str, object] = {
            "venue": VENUE,
            "state": "stopped",
            "messages_received": 0,
            "records_parsed": 0,
            "rows_written": 0,
            "normalization_issues": 0,
            "connections": 0,
            "physical_connections": 0,
            "reconnects": 0,
            "clock_samples": 0,
            "clock_samples_valid": 0,
            "clock_samples_invalid": 0,
            "clock_consecutive_rejected_probes": 0,
            "clock_rejected_probe_streak_high_water": 0,
            "clock_sample_failures": 0,
            "capture_ready": False,
            "missing_required_streams": [],
            "pending_l2_resync_assets": list(config.assets),
            "clock_sync_valid": False,
            "clock_observability": self._clock_observability_payload(),
            "maintenance_or_absence_detected": False,
            "last_error": None,
        }

    def _counter(self, name: str) -> int:
        return int(str(self.metrics[name]))

    @classmethod
    def create_default(
        cls,
        config: ReferenceCollectorConfig,
        *,
        data_dir: Path,
        request_timeout_seconds: float,
        sink: LakeSink | None = None,
    ) -> BinanceReferenceCollector:
        return cls(
            config,
            rest=BinancePublicRestClient(timeout_seconds=request_timeout_seconds),
            sink=(
                BatchingLakeSink(
                    data_dir / "lake",
                    batch_size=config.batch_size,
                    queue_capacity=config.queue_capacity,
                )
                if sink is None
                else sink
            ),
            runtime_status_path=data_dir / "runtime_status_binance_usdm.json",
        )

    def stop(self) -> None:
        self._stop = True
        self._close_sockets(reason="collector stop requested")

    def _close_sockets(self, *, reason: str) -> None:
        sockets = tuple(sorted(self._sockets.items()))
        connection_epoch = self._active_connection_epoch
        capture_epoch_id = self._active_capture_epoch_id
        self._sockets.clear()
        if not sockets:
            self._active_connection_epoch = None
            self._active_capture_epoch_id = None
            return
        before_close = {role: self._socket_telemetry(socket) for role, socket in sockets}
        for _role, socket in sockets:
            with suppress(Exception):
                socket.close()
        after_close = {role: self._socket_telemetry(socket) for role, socket in sockets}
        self._last_closed_socket_telemetry = {
            "generation": connection_epoch,
            "capture_epoch_id": capture_epoch_id,
            "closed_at": self.clock().isoformat(),
            "reason": reason,
            "telemetry_before_close": before_close,
            "telemetry_after_close": after_close,
        }
        self._active_connection_epoch = None
        self._active_capture_epoch_id = None

    def _interruptible_sleep(self, delay_seconds: float) -> None:
        deadline = self.monotonic() + max(delay_seconds, 0.0)
        while not self._stop:
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(remaining, 0.25))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.stop()
        errors: list[tuple[str, BaseException]] = []
        try:
            self._clock_executor.shutdown(wait=True, cancel_futures=True)
        except BaseException as exc:
            errors.append(("clock sampler shutdown", exc))
        try:
            self._finalize_clock_after_generation_close()
        except BaseException as exc:
            errors.append(("clock sample finalization", exc))
        close_rest = getattr(self.rest, "close", None)
        if callable(close_rest):
            try:
                close_rest()
            except BaseException as exc:
                errors.append(("Binance REST transport close", exc))
        try:
            result = self.sink.flush()
            self.metrics["rows_written"] = self._counter("rows_written") + result.row_count
        except BaseException as exc:
            errors.append(("terminal lake flush", exc))
        try:
            self.sink.close()
        except BaseException as exc:
            errors.append(("lake sink close", exc))
        self.metrics["state"] = "failed" if errors else "stopped"
        try:
            self._publish()
        except BaseException as exc:
            errors.append(("runtime status publish", exc))
        try:
            self._runtime_telemetry.close()
        except BaseException as exc:
            errors.append(("runtime telemetry close", exc))
        if not errors:
            return
        first_label, primary = errors[0]
        primary.add_note(f"cleanup action: {first_label}")
        for label, secondary in errors[1:]:
            primary.add_note(f"{label} also failed: {type(secondary).__name__}: {secondary}")
        raise primary

    def _collect_completed_rows(self) -> None:
        collect_completed = getattr(self.sink, "collect_completed", None)
        if not callable(collect_completed):
            return
        result = collect_completed()
        self.metrics["rows_written"] = self._counter("rows_written") + result.row_count

    def _add(self, record: ParsedRecord) -> None:
        enqueue_started = self.monotonic()
        try:
            accepted = self.sink.add(record)
        finally:
            self._sink_enqueue_timing.observe_seconds(max(self.monotonic() - enqueue_started, 0.0))
        if accepted:
            self.metrics["records_parsed"] = self._counter("records_parsed") + 1
        self._collect_completed_rows()
        if self.sink.should_flush:
            result = self.sink.flush()
            self.metrics["rows_written"] = self._counter("rows_written") + result.row_count

    def _add_many(self, records: Iterable[ParsedRecord]) -> None:
        enqueue_started = self.monotonic()
        try:
            accepted = self.sink.add_many(records)
        finally:
            self._sink_enqueue_timing.observe_seconds(max(self.monotonic() - enqueue_started, 0.0))
        self.metrics["records_parsed"] = self._counter("records_parsed") + accepted
        self._collect_completed_rows()
        if self.sink.should_flush:
            result = self.sink.flush()
            self.metrics["rows_written"] = self._counter("rows_written") + result.row_count

    def _connection_event(
        self,
        event_kind: str,
        *,
        connection_id: str,
        connection_epoch: int | None = None,
        capture_epoch_id: str | None = None,
        socket_role: str | None = None,
        asset: str = "GLOBAL",
        channel: str | None = None,
        book_epoch_id: str | None = None,
        reason: str | None = None,
        at: datetime | None = None,
        received_at: datetime | None = None,
        resync_snapshot_id: str | None = None,
    ) -> None:
        event_time = self.clock() if at is None else at
        received_time = event_time if received_at is None else received_at
        row = {
            "schema_version": latest_schema_for(RecordType.CONNECTION_EVENT).version,
            "record_type": RecordType.CONNECTION_EVENT.value,
            "venue": VENUE,
            "asset": asset,
            "event_time": event_time,
            "exchange_time": None,
            "received_time": received_time,
            "source_sequence": None,
            "connection_id": connection_id,
            "event_kind": event_kind,
            "channel": channel,
            "book_epoch_id": book_epoch_id,
            "reason": reason,
            "expected_sequence": None,
            "observed_sequence": None,
            "resync_snapshot_id": resync_snapshot_id,
            "connection_epoch": connection_epoch,
            "capture_epoch_id": capture_epoch_id,
            "socket_role": socket_role,
        }
        self._add(ParsedRecord(RecordType.CONNECTION_EVENT, asset, row))

    def _initialize_critical_streams(
        self,
        connector: BinancePublicConnector,
        *,
        at: datetime,
    ) -> None:
        channels: list[str] = []
        expectations: dict[str, tuple[str, RecordType, str]] = {}
        for asset in self.config.assets:
            symbol = connector.instrument_for_asset(asset).source_symbol.lower()
            for channel, record_type, socket_role in (
                (f"{symbol}@depth20@100ms", RecordType.L2_BOOK_STATE, "public"),
                (f"{symbol}@aggTrade", RecordType.TRADE, "market"),
            ):
                channels.append(channel)
                expectations[channel] = (asset, record_type, socket_role)
        self._critical_stream_last_received = {channel: at for channel in channels}
        self._critical_stream_expectations = expectations
        self._critical_stream_seen = set()

    def _refresh_capture_readiness(
        self,
        *,
        at: datetime,
        pending_l2_resync_assets: set[str],
    ) -> None:
        before = (
            self.metrics["state"],
            self.metrics["capture_ready"],
            self.metrics["missing_required_streams"],
            self.metrics["pending_l2_resync_assets"],
            self.metrics["clock_sync_valid"],
        )
        missing_streams = sorted(set(self._critical_stream_last_received) - self._critical_stream_seen)
        clock_valid = self._valid_clock_until is not None and at < self._valid_clock_until
        stale_streams = self._stale_critical_streams(at=at)
        ready = not missing_streams and not pending_l2_resync_assets and not stale_streams and clock_valid
        self.metrics["capture_ready"] = ready
        self.metrics["missing_required_streams"] = missing_streams
        self.metrics["pending_l2_resync_assets"] = sorted(pending_l2_resync_assets)
        self.metrics["clock_sync_valid"] = clock_valid
        if ready:
            self.metrics["state"] = "live"
        elif self.metrics["state"] not in {"stale", "backoff", "failed"}:
            self.metrics["state"] = "warming"
        after = (
            self.metrics["state"],
            self.metrics["capture_ready"],
            self.metrics["missing_required_streams"],
            self.metrics["pending_l2_resync_assets"],
            self.metrics["clock_sync_valid"],
        )
        if after != before:
            self._publish()

    def _stale_critical_streams(self, *, at: datetime) -> tuple[str, ...]:
        threshold = timedelta(seconds=self.config.stale_after_seconds)
        return tuple(
            channel
            for channel, last_received in sorted(self._critical_stream_last_received.items())
            if at - last_received > threshold
        )

    def _observe_critical_stream(
        self,
        parsed: ParsedMessage,
        *,
        received_time: datetime,
        socket_role: str,
    ) -> None:
        channel = parsed.channel
        if channel not in self._critical_stream_last_received:
            return
        assert channel is not None
        expected_asset, expected_type, expected_role = self._critical_stream_expectations[channel]
        if socket_role != expected_role:
            return
        if not any(
            record.record_type == expected_type and record.asset == expected_asset
            for record in parsed.records
        ):
            return
        self._critical_stream_last_received[channel] = received_time
        self._critical_stream_seen.add(channel)

    def _require_critical_streams_fresh(
        self,
        *,
        at: datetime,
    ) -> None:
        stale_channels = self._stale_critical_streams(at=at)
        if not stale_channels:
            return
        stale_by_role: dict[str, list[str]] = {}
        for channel in stale_channels:
            _asset, _record_type, role = self._critical_stream_expectations[channel]
            stale_by_role.setdefault(role, []).append(channel)
        backlog_states = {
            role: self._socket_backlog_state(
                role,
                max_age_seconds=self.config.stale_after_seconds,
            )[0]
            for role in stale_by_role
        }
        exhausted_roles = sorted(role for role, state in backlog_states.items() if state == "exhausted")
        if exhausted_roles:
            role = exhausted_roles[0]
            raise _SocketRoleFailure(
                role,
                WebsocketConsumerBackpressure(
                    "Binance websocket oldest queued message exceeded the unchanged "
                    "critical-stream stale deadline; local consumer capacity exhausted; "
                    f"channels={stale_by_role[role]}"
                ),
            )
        self.metrics["capture_ready"] = False
        reason = "required Binance depth-derived BBO/L2 or trade streams stale: " + ", ".join(stale_channels)
        self.metrics["maintenance_or_absence_detected"] = True
        self.metrics["state"] = "stale"
        self._publish()
        raise TimeoutError(reason)

    def _record_l2_resync_if_needed(
        self,
        parsed: ParsedMessage,
        *,
        pending_assets: set[str],
        connection_id: str,
        connection_epoch: int,
        capture_epoch_id: str | None = None,
    ) -> None:
        states = [
            record
            for record in parsed.records
            if record.record_type == RecordType.L2_BOOK_STATE and record.asset in pending_assets
        ]
        if not states:
            return
        if len(states) != 1:
            raise RuntimeError("one Binance partial-depth frame produced multiple L2 states")
        state = states[0]
        expected_epoch = f"{connection_id}:{connection_epoch}"
        observed_epoch = str(state.row["book_epoch_id"])
        if observed_epoch != expected_epoch:
            raise RuntimeError(
                "Binance L2 book epoch is incompatible with the active connection: "
                f"expected {expected_epoch!r}, observed {observed_epoch!r}"
            )
        event_time = state.row["event_time"]
        received_time = state.row["received_time"]
        snapshot_id = str(state.row["snapshot_id"])
        if not isinstance(event_time, datetime) or not isinstance(received_time, datetime):
            raise RuntimeError("Binance L2 state has invalid event or receive timestamps")
        reason = "complete Binance top-20 snapshot received"
        for event_kind in ("resync_start", "resync_complete"):
            self._connection_event(
                event_kind,
                connection_id=connection_id,
                connection_epoch=connection_epoch,
                capture_epoch_id=capture_epoch_id,
                socket_role="public",
                asset=state.asset,
                channel=str(parsed.channel),
                book_epoch_id=expected_epoch,
                reason=reason,
                at=event_time,
                received_at=received_time,
                resync_snapshot_id=(snapshot_id if event_kind == "resync_complete" else None),
            )
        pending_assets.remove(state.asset)

    def _bootstrap(self) -> BinancePublicConnector:
        self.metrics["state"] = "bootstrapping"
        exchange_info = self.rest.exchange_info()
        connector = BinancePublicConnector.from_exchange_info(
            exchange_info.payload,
            self.config.assets,
        )
        for record in connector.metadata_records(exchange_info.response_received_time):
            self._add(record)
        unavailable = [
            item.source_symbol
            for asset in self.config.assets
            if (item := connector.instrument_for_asset(asset)).status != "TRADING"
        ]
        if unavailable:
            self.metrics["maintenance_or_absence_detected"] = True
            self.metrics["state"] = "unavailable"
            self.metrics["last_error"] = "Binance instruments are not TRADING: " + ", ".join(
                sorted(unavailable)
            )
            self._publish()
            raise RuntimeError(str(self.metrics["last_error"]))
        raw_schedule = self.rest.funding_info()
        schedules = funding_intervals(raw_schedule.payload)
        end_ms = int(self.clock().timestamp() * 1_000)
        start_ms = end_ms - self.config.history_lookback_hours * 3_600_000
        for asset in self.config.assets:
            normalized = connector.instrument_for_asset(asset)
            funding = self.rest.funding_history(normalized.source_symbol, start_ms, end_ms)
            for record in parse_funding_history(
                funding.payload,
                received_time=funding.response_received_time,
                normalized=normalized,
                expected_interval_seconds=schedules.get(normalized.source_symbol),
            ):
                self._add(record)
            for interval in self.config.candle_intervals:
                klines = self.rest.klines(normalized.source_symbol, interval, start_ms, end_ms)
                for record in parse_klines(
                    klines.payload,
                    received_time=klines.response_received_time,
                    normalized=normalized,
                    interval=interval,
                ):
                    self._add(record)
        result = self.sink.flush()
        self.metrics["rows_written"] = self._counter("rows_written") + result.row_count
        self._connector = connector
        return connector

    @staticmethod
    def _latency_summary(values: list[float]) -> dict[str, int | float | None]:
        if not values:
            return {
                "count": 0,
                "min": None,
                "median": None,
                "p95": None,
                "p99": None,
                "max": None,
            }
        ordered = sorted(values)

        def percentile(fraction: float) -> float:
            return ordered[max(ceil(fraction * len(ordered)) - 1, 0)]

        return {
            "count": len(ordered),
            "min": round(ordered[0], 6),
            "median": round(float(median(ordered)), 6),
            "p95": round(percentile(0.95), 6),
            "p99": round(percentile(0.99), 6),
            "max": round(ordered[-1], 6),
        }

    def _clock_in_flight_payload(self) -> dict[str, object]:
        future = self._clock_future
        context = self._clock_future_context
        if future is None:
            return {
                "pending": False,
                "state": "idle",
                "future_done": False,
                "future_cancelled": False,
                "pending_age_ms": None,
                "queued_not_started_age_ms": None,
                "running_age_ms": None,
                "completed_awaiting_drain_age_ms": None,
                "capture_epoch_id": None,
                "connection_id": None,
                "connection_epoch": None,
            }
        if context is None:
            return {
                "pending": True,
                "state": "identity_missing",
                "future_done": future.done(),
                "future_cancelled": future.cancelled(),
                "pending_age_ms": None,
                "queued_not_started_age_ms": None,
                "running_age_ms": None,
                "completed_awaiting_drain_age_ms": None,
                "capture_epoch_id": None,
                "connection_id": None,
                "connection_epoch": None,
            }

        observed_at = self.monotonic()
        pending_age_seconds = max(observed_at - context.submitted_at, 0.0)
        queued_age_seconds: float | None = None
        running_age_seconds: float | None = None
        completed_age_seconds: float | None = None
        if context.worker_started_at is None:
            state = "completed_without_worker_start" if future.done() else "queued_not_started"
            if not future.done():
                queued_age_seconds = pending_age_seconds
        elif context.worker_completed_at is None:
            state = "running"
            running_age_seconds = max(observed_at - context.worker_started_at, 0.0)
        else:
            state = "completed_awaiting_drain"
            completed_age_seconds = max(observed_at - context.worker_completed_at, 0.0)

        def milliseconds(value: float | None) -> float | None:
            return None if value is None else round(value * 1_000, 6)

        return {
            "pending": True,
            "state": state,
            "future_done": future.done(),
            "future_cancelled": future.cancelled(),
            "pending_age_ms": milliseconds(pending_age_seconds),
            "queued_not_started_age_ms": milliseconds(queued_age_seconds),
            "running_age_ms": milliseconds(running_age_seconds),
            "completed_awaiting_drain_age_ms": milliseconds(completed_age_seconds),
            "capture_epoch_id": context.capture_epoch_id,
            "connection_id": context.connection_id,
            "connection_epoch": context.connection_epoch,
        }

    def _clock_observability_payload(self) -> dict[str, object]:
        timing_fields = (
            ("authoritative_clock_round_trip", "authoritative_clock_round_trip_ms"),
            ("failed_request_boundary_duration", "failed_request_boundary_duration_ms"),
            ("executor_submit_to_worker_start", "executor_submit_to_worker_start_ms"),
            ("transport_lock_wait", "transport_lock_wait_ms"),
            (
                "requests_adapter_header_elapsed",
                "requests_adapter_header_elapsed_ms",
            ),
            ("session_get_total", "session_get_total_ms"),
            ("json_decode", "json_decode_ms"),
            ("diagnostic_prepare", "diagnostic_prepare_ms"),
            ("diagnostic_finalize", "diagnostic_finalize_ms"),
            (
                "worker_completion_to_supervisor_drain",
                "worker_completion_to_supervisor_drain_ms",
            ),
        )
        latency_ms: dict[str, object] = {}
        for label, field in timing_fields:
            values = [
                float(value)
                for observation in self._clock_observations
                if isinstance((value := observation.get(field)), (int, float)) and not isinstance(value, bool)
            ]
            latency_ms[label] = self._latency_summary(values)

        connection_deltas = [
            int(value)
            for observation in self._clock_observations
            if isinstance(
                (value := observation.get("urllib3_connection_objects_created_delta")),
                int,
            )
            and not isinstance(value, bool)
        ]
        request_deltas = [
            int(value)
            for observation in self._clock_observations
            if isinstance(
                (value := observation.get("urllib3_requests_started_delta")),
                int,
            )
            and not isinstance(value, bool)
        ]
        new_connection_flags = [
            observation.get("new_urllib3_connection_object_created")
            for observation in self._clock_observations
        ]
        current_observation_flags = [
            observation.get("post_request_observation_current") for observation in self._clock_observations
        ]
        return {
            "capacity": _CLOCK_OBSERVABILITY_CAPACITY,
            "samples_seen": self._clock_observations_seen,
            "samples_retained": len(self._clock_observations),
            "latest": (dict(self._clock_observations[-1]) if self._clock_observations else None),
            "in_flight": self._clock_in_flight_payload(),
            "outcomes": {
                "success": sum(
                    observation.get("outcome") == "success" for observation in self._clock_observations
                ),
                "error": sum(
                    observation.get("outcome") == "error" for observation in self._clock_observations
                ),
                "discarded_after_generation_close": sum(
                    observation.get("outcome") == "discarded_after_generation_close"
                    for observation in self._clock_observations
                ),
            },
            "clock_schedule_overdue_ms": self._clock_schedule_overdue.as_dict(),
            "single_flight_blocked_ms": self._clock_single_flight_blocked.as_dict(),
            "latency_ms": latency_ms,
            "http_pool": {
                "urllib3_connection_objects_created_delta_total": sum(connection_deltas),
                "urllib3_requests_started_delta_total": sum(request_deltas),
                "new_urllib3_connection_object_created_samples": (new_connection_flags.count(True)),
                "no_new_urllib3_connection_object_created_samples": (new_connection_flags.count(False)),
                "new_urllib3_connection_object_indeterminate_samples": (new_connection_flags.count(None)),
                "post_request_observation_current_samples": (current_observation_flags.count(True)),
                "post_request_observation_contaminated_samples": (current_observation_flags.count(False)),
                "post_request_observation_indeterminate_samples": (current_observation_flags.count(None)),
            },
        }

    def _observe_clock_execution(
        self,
        context: _ClockFutureContext,
        measurement: ClockMeasurement | None,
        *,
        drained_at: float,
        error: BaseException | None = None,
        outcome: str | None = None,
    ) -> None:
        diagnostics = (
            measurement.http_diagnostics
            if measurement is not None
            else getattr(error, "http_diagnostics", None)
        )
        raw_error_message = None if error is None else str(error).strip()
        error_message = (
            None if raw_error_message is None else raw_error_message[:_OBSERVABILITY_ERROR_MESSAGE_LIMIT]
        )
        request_sent_time = getattr(error, "request_sent_time", None)
        response_received_time = getattr(error, "response_received_time", None)
        failed_request_boundary_duration_ms: float | None = None
        if isinstance(request_sent_time, datetime) and isinstance(
            response_received_time,
            datetime,
        ):
            try:
                boundary_duration = response_received_time - request_sent_time
            except TypeError:
                pass
            else:
                if boundary_duration >= timedelta(0):
                    failed_request_boundary_duration_ms = round(
                        boundary_duration.total_seconds() * 1_000,
                        6,
                    )
        observation: dict[str, object] = {
            "outcome": outcome or ("error" if error is not None else "success"),
            "exception_type": None if error is None else type(error).__name__,
            "exception_message": error_message,
            "exception_message_truncated": (
                False
                if raw_error_message is None
                else len(raw_error_message) > _OBSERVABILITY_ERROR_MESSAGE_LIMIT
            ),
            "capture_epoch_id": context.capture_epoch_id,
            "connection_id": context.connection_id,
            "connection_epoch": context.connection_epoch,
            "executor_submit_to_worker_start_ms": (
                None
                if context.worker_started_at is None
                else round(
                    max(context.worker_started_at - context.submitted_at, 0.0) * 1_000,
                    6,
                )
            ),
            "authoritative_clock_round_trip_ms": (
                None if measurement is None else float(measurement.round_trip_latency_ms)
            ),
            "failed_request_boundary_duration_ms": (failed_request_boundary_duration_ms),
            "worker_completion_to_supervisor_drain_ms": (
                None
                if context.worker_completed_at is None
                else round(
                    max(drained_at - context.worker_completed_at, 0.0) * 1_000,
                    6,
                )
            ),
            "transport_lock_wait_ms": (None if diagnostics is None else diagnostics.transport_lock_wait_ms),
            "requests_adapter_header_elapsed_ms": (
                None if diagnostics is None else diagnostics.requests_adapter_header_elapsed_ms
            ),
            "session_get_total_ms": (None if diagnostics is None else diagnostics.session_get_total_ms),
            "json_decode_ms": (None if diagnostics is None else diagnostics.json_decode_ms),
            "diagnostic_prepare_ms": (None if diagnostics is None else diagnostics.diagnostic_prepare_ms),
            "diagnostic_finalize_ms": (None if diagnostics is None else diagnostics.diagnostic_finalize_ms),
            "urllib3_connection_objects_created_total_before": (
                None if diagnostics is None else diagnostics.urllib3_connection_objects_created_total_before
            ),
            "urllib3_connection_objects_created_total_after": (
                None if diagnostics is None else diagnostics.urllib3_connection_objects_created_total_after
            ),
            "urllib3_connection_objects_created_delta": (
                None if diagnostics is None else diagnostics.urllib3_connection_objects_created_delta
            ),
            "new_urllib3_connection_object_created": (
                None if diagnostics is None else diagnostics.new_urllib3_connection_object_created
            ),
            "urllib3_requests_started_total_before": (
                None if diagnostics is None else diagnostics.urllib3_requests_started_total_before
            ),
            "urllib3_requests_started_total_after": (
                None if diagnostics is None else diagnostics.urllib3_requests_started_total_after
            ),
            "urllib3_requests_started_delta": (
                None if diagnostics is None else diagnostics.urllib3_requests_started_delta
            ),
            "request_completion_sequence": (
                None if diagnostics is None else diagnostics.request_completion_sequence
            ),
            "finalization_completion_sequence": (
                None if diagnostics is None else diagnostics.finalization_completion_sequence
            ),
            "post_request_observation_current": (
                None if diagnostics is None else diagnostics.post_request_observation_current
            ),
            "http_outcome": None if diagnostics is None else diagnostics.outcome,
            "http_failure_stage": (None if diagnostics is None else diagnostics.failure_stage),
            "http_exception_type": (None if diagnostics is None else diagnostics.exception_type),
            "requests_session_reused": (None if diagnostics is None else diagnostics.requests_session_reused),
            "urllib3_connection_identity": (
                None if diagnostics is None else diagnostics.urllib3_connection_identity
            ),
            "urllib3_connection_reused": (
                None if diagnostics is None else diagnostics.urllib3_connection_reused
            ),
            "tls_socket_identity": (None if diagnostics is None else diagnostics.tls_socket_identity),
            "tls_socket_reused": (None if diagnostics is None else diagnostics.tls_socket_reused),
            "tls_session_reused": (None if diagnostics is None else diagnostics.tls_session_reused),
            "peer_ip": None if diagnostics is None else diagnostics.peer_ip,
            "peer_port": None if diagnostics is None else diagnostics.peer_port,
            "socket_family": None if diagnostics is None else diagnostics.socket_family,
            "response_cloudfront_pop": (
                None if diagnostics is None else diagnostics.response_cloudfront_pop
            ),
            "response_cache": None if diagnostics is None else diagnostics.response_cache,
        }
        self._clock_observations.append(observation)
        self._clock_observations_seen += 1
        self.metrics["clock_observability"] = self._clock_observability_payload()

    def _finalize_clock_after_generation_close(self) -> None:
        future = self._clock_future
        context = self._clock_future_context
        self._clock_future = None
        self._clock_future_context = None
        if future is None:
            return
        if context is None:
            raise RuntimeError("clock future is missing its capture identity during close")
        if not future.done():
            raise RuntimeError("clock future remained pending after executor shutdown")
        try:
            measurement = future.result()
        except Exception as exc:
            self._observe_clock_execution(
                context,
                None,
                drained_at=self.monotonic(),
                error=exc,
                outcome="discarded_after_generation_close",
            )
        else:
            self._observe_clock_execution(
                context,
                measurement,
                drained_at=self.monotonic(),
                outcome="discarded_after_generation_close",
            )

    def _run_clock_sample(self, context: _ClockFutureContext) -> ClockMeasurement:
        context.worker_started_at = self.monotonic()
        try:
            return self.rest.clock_measurement()
        finally:
            context.worker_completed_at = self.monotonic()

    def _schedule_clock_sample(
        self,
        *,
        connection_id: str,
        connection_epoch: int,
        capture_epoch_id: str,
    ) -> None:
        if self._clock_future is not None:
            return
        context = _ClockFutureContext(
            capture_epoch_id=capture_epoch_id,
            connection_id=connection_id,
            connection_epoch=connection_epoch,
            submitted_at=self.monotonic(),
        )
        self._clock_future_context = context
        try:
            self._clock_future = self._clock_executor.submit(
                self._run_clock_sample,
                context,
            )
        except BaseException:
            self._clock_future_context = None
            raise

    def _drain_clock_sample(
        self,
        *,
        active_capture_epoch_id: str | None,
        wait: bool,
    ) -> bool:
        future = self._clock_future
        context = self._clock_future_context
        if future is None:
            return False
        if not wait and not future.done():
            return False
        self._clock_future = None
        self._clock_future_context = None
        if context is None:
            raise RuntimeError("clock future is missing its capture identity")
        capture_epoch_id = context.capture_epoch_id
        connection_id = context.connection_id
        connection_epoch = context.connection_epoch
        try:
            measurement = future.result()
        except Exception as exc:
            self._observe_clock_execution(
                context,
                None,
                drained_at=self.monotonic(),
                error=exc,
            )
            reason = f"clock_sync request failed: {type(exc).__name__}: {exc}"
            self.metrics["clock_sample_failures"] = self._counter("clock_sample_failures") + 1
            self.metrics["maintenance_or_absence_detected"] = True
            self._valid_clock_until = None
            self.metrics["capture_ready"] = False
            self.metrics["clock_sync_valid"] = False
            self._connection_event(
                "gap",
                connection_id=f"{connection_id}:clock",
                connection_epoch=connection_epoch,
                capture_epoch_id=capture_epoch_id,
                socket_role="clock",
                channel="clock_sync",
                reason=f"coverage_unknown:{reason}",
            )
            return True
        self._observe_clock_execution(
            context,
            measurement,
            drained_at=self.monotonic(),
        )
        if active_capture_epoch_id != capture_epoch_id:
            self.metrics["clock_sample_failures"] = self._counter("clock_sample_failures") + 1
            self._valid_clock_until = None
            self.metrics["capture_ready"] = False
            self.metrics["clock_sync_valid"] = False
            return True
        record = clock_record(
            measurement,
            f"clock:{capture_epoch_id}:{uuid.uuid4().hex}",
            connection_id=connection_id,
            connection_epoch=connection_epoch,
            capture_epoch_id=capture_epoch_id,
            sampling_interval=timedelta(seconds=self.config.clock_sampling_interval_seconds),
            max_age=timedelta(seconds=self.config.clock_max_age_seconds),
            max_uncertainty_ms=self.config.clock_max_uncertainty_ms,
        )
        self._add(record)
        self.metrics["clock_samples"] = self._counter("clock_samples") + 1
        status = str(record.row["sample_status"])
        counter = "clock_samples_valid" if status == "valid" else "clock_samples_invalid"
        self.metrics[counter] = self._counter(counter) + 1
        valid_until = record.row["causal_valid_until"]
        if status == "valid" and isinstance(valid_until, datetime):
            self._clock_rejection_streak = 0
            self.metrics["clock_consecutive_rejected_probes"] = 0
            self._valid_clock_until = valid_until
            self.metrics["clock_sync_valid"] = True
        else:
            invalid_reason = record.row["invalid_reason"]
            measured_uncertainty = Decimal(
                str(record.row["drift_uncertainty_ms"])
            )
            expected_high_uncertainty_rejection = (
                isinstance(invalid_reason, str)
                and measured_uncertainty > _STRICT_MAX_CLOCK_UNCERTAINTY_MS
                and invalid_reason.startswith("clock uncertainty exceeds threshold:")
            )
            if expected_high_uncertainty_rejection:
                self._clock_rejection_streak += 1
                self.metrics["clock_consecutive_rejected_probes"] = (
                    self._clock_rejection_streak
                )
                self.metrics["clock_rejected_probe_streak_high_water"] = max(
                    self._counter("clock_rejected_probe_streak_high_water"),
                    self._clock_rejection_streak,
                )
            else:
                self._clock_rejection_streak = 0
                self.metrics["clock_consecutive_rejected_probes"] = 0
            prior_interval_live = (
                expected_high_uncertainty_rejection
                and self._clock_rejection_streak
                <= _MAX_CONSECUTIVE_REJECTED_CLOCK_PROBES
                and self._valid_clock_until is not None
                and self.clock() < self._valid_clock_until
            )
            if not prior_interval_live:
                self._valid_clock_until = None
                self.metrics["capture_ready"] = False
            self.metrics["clock_sync_valid"] = prior_interval_live
            self.metrics["maintenance_or_absence_detected"] = True
        return True

    def _observe_clock_schedule(
        self,
        *,
        expected_at: float,
        observed_at: float,
        prior_context: _ClockFutureContext | None,
    ) -> None:
        overdue_seconds = max(observed_at - expected_at, 0.0)
        self._clock_schedule_overdue.observe_seconds(overdue_seconds)
        scheduling_expected_at = expected_at
        if prior_context is not None:
            completed_at = prior_context.worker_completed_at
            blocked_until = observed_at if completed_at is None else min(completed_at, observed_at)
            blocked_seconds = max(blocked_until - expected_at, 0.0)
            if blocked_seconds > 0:
                self._clock_single_flight_blocked.observe_seconds(blocked_seconds)
            scheduling_expected_at = max(expected_at, blocked_until)
        self._runtime_telemetry.record_worker_scheduling_lag(
            round(scheduling_expected_at * 1_000_000_000),
            observed_monotonic_ns=round(observed_at * 1_000_000_000),
        )
        self.metrics["clock_observability"] = self._clock_observability_payload()

    def _abandon_clock_sample(self) -> None:
        future = self._clock_future
        if future is None:
            self._clock_future_context = None
            return
        if future.cancel():
            self._clock_future = None
            self._clock_future_context = None

    def _record_generation_end(
        self,
        *,
        connection_ids: dict[str, str],
        connected_roles: Iterable[str],
        connection_epoch: int,
        capture_epoch_id: str,
        reason: str,
        include_gap: bool,
    ) -> None:
        roles = tuple(connected_roles)
        for role in roles:
            self._connection_event(
                "disconnect",
                connection_id=connection_ids[role],
                connection_epoch=connection_epoch,
                capture_epoch_id=capture_epoch_id,
                socket_role=role,
                channel=role,
                reason=reason,
            )
            if include_gap:
                self._connection_event(
                    "gap",
                    connection_id=connection_ids[role],
                    connection_epoch=connection_epoch,
                    capture_epoch_id=capture_epoch_id,
                    socket_role=role,
                    channel=role,
                    reason=f"coverage_unknown:{reason}",
                )
        if include_gap and set(roles) != {"public", "market"}:
            self._connection_event(
                "gap",
                connection_id=f"{capture_epoch_id}:supervisor",
                connection_epoch=connection_epoch,
                capture_epoch_id=capture_epoch_id,
                socket_role="supervisor",
                channel="capture_generation",
                reason=(
                    "coverage_unknown:required Binance socket pair was incomplete;"
                    f" connected_roles={','.join(roles) or 'none'}; failure={reason}"
                ),
            )
        self._valid_clock_until = None
        self.metrics["capture_ready"] = False
        self.metrics["clock_sync_valid"] = False

    def run(self, *, duration_seconds: float | None = None, max_messages: int | None = None) -> None:
        if duration_seconds is not None and duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive when provided")
        if max_messages is not None and max_messages <= 0:
            raise ValueError("max_messages must be positive when provided")
        self._runtime_telemetry.watchdog.start()
        connector = self._bootstrap()
        websocket_urls = connector.websocket_urls(
            self.config.assets,
            self.config.candle_intervals,
        )
        required_roles = ("public", "market")
        if set(websocket_urls) != set(required_roles):
            raise RuntimeError("Binance connector must provide public and market websocket URLs")
        factories = {
            role: UrlWebsocketClientFactory(
                websocket_urls[role],
                queue_capacity=self.config.queue_capacity,
                clock=self.clock,
                monotonic_ns=lambda: round(self.monotonic() * 1_000_000_000),
                venue=VENUE,
                socket_role=role,
            )
            for role in required_roles
        }

        def open_paused(role: str) -> PublicSocket:
            return factories[role].connect_paused(
                "public",
                self.config.connect_timeout_seconds,
            )

        started = self.monotonic()
        epoch = 0
        while not self._stop:
            if duration_seconds is not None and self.monotonic() - started >= duration_seconds:
                break
            if max_messages is not None and self._counter("messages_received") >= max_messages:
                break
            epoch += 1
            capture_epoch_id = f"binance-capture-{epoch}-{uuid.uuid4().hex}"
            self._active_connection_epoch = epoch
            self._active_capture_epoch_id = capture_epoch_id
            connection_ids = {role: f"binance-{role}-{uuid.uuid4().hex}" for role in required_roles}
            arrival_sequences = {role: 0 for role in required_roles}
            connected_roles: list[str] = []
            if epoch > 1:
                self.metrics["reconnects"] = self._counter("reconnects") + 1
            try:
                self.metrics["state"] = "connecting"
                self.metrics["capture_ready"] = False
                self.metrics["clock_sync_valid"] = False
                self._valid_clock_until = None
                self._clock_rejection_streak = 0
                self.metrics["clock_consecutive_rejected_probes"] = 0
                self.metrics["capture_epoch_id"] = capture_epoch_id
                self.metrics["physical_connection_ids"] = dict(connection_ids)
                open_errors: dict[str, _SocketRoleFailure] = {}
                with ThreadPoolExecutor(
                    max_workers=len(required_roles),
                    thread_name_prefix="binance-public-connect",
                ) as connect_executor:
                    connect_futures: dict[Future[PublicSocket], str] = {
                        connect_executor.submit(open_paused, role): role for role in required_roles
                    }
                    for future in as_completed(connect_futures):
                        role = connect_futures[future]
                        try:
                            socket = future.result()
                        except Exception as exc:
                            open_errors[role] = _SocketRoleFailure(
                                role,
                                exc,
                                operation="handshake",
                            )
                            continue
                        self._sockets[role] = socket
                        self.metrics["physical_connections"] = self._counter("physical_connections") + 1
                if open_errors:
                    ordered_failures = tuple(
                        open_errors[role] for role in required_roles if role in open_errors
                    )
                    raise _PairedSocketHandshakeFailure(ordered_failures)
                if self._stop or (
                    duration_seconds is not None and self.monotonic() - started >= duration_seconds
                ):
                    self._close_sockets(reason="bounded run completed during paired handshake")
                    break

                activation_at = self.clock()
                self._initialize_critical_streams(connector, at=activation_at)
                for role in required_roles:
                    self._connection_event(
                        "connect",
                        connection_id=connection_ids[role],
                        connection_epoch=epoch,
                        capture_epoch_id=capture_epoch_id,
                        socket_role=role,
                        channel=role,
                        at=activation_at,
                        received_at=activation_at,
                    )
                    connected_roles.append(role)
                self.metrics["connections"] = self._counter("connections") + 1
                for role in required_roles:
                    self._sockets[role].start_receiving()
                self.metrics["state"] = "warming"
                pending_l2_resync_assets = set(self.config.assets)
                next_clock_sample_at = self.monotonic()
                self._drain_clock_sample(
                    active_capture_epoch_id=capture_epoch_id,
                    wait=False,
                )
                if self._clock_future is None:
                    self._schedule_clock_sample(
                        connection_id=connection_ids["public"],
                        connection_epoch=epoch,
                        capture_epoch_id=capture_epoch_id,
                    )
                    next_clock_sample_at += self.config.clock_sampling_interval_seconds
                self._publish()
                next_status_publish_at = self.monotonic() + self.config.clock_sampling_interval_seconds
                while not self._stop:
                    if duration_seconds is not None and self.monotonic() - started >= duration_seconds:
                        break
                    if max_messages is not None and (self._counter("messages_received") >= max_messages):
                        break
                    observed_mono = self.monotonic()
                    prior_clock_context = self._clock_future_context
                    self._drain_clock_sample(
                        active_capture_epoch_id=capture_epoch_id,
                        wait=False,
                    )
                    observed_mono = self.monotonic()
                    self._refresh_capture_readiness(
                        at=self.clock(),
                        pending_l2_resync_assets=pending_l2_resync_assets,
                    )
                    if self._clock_future is None and observed_mono >= next_clock_sample_at:
                        self._observe_clock_schedule(
                            expected_at=next_clock_sample_at,
                            observed_at=observed_mono,
                            prior_context=prior_clock_context,
                        )
                        self._schedule_clock_sample(
                            connection_id=connection_ids["public"],
                            connection_epoch=epoch,
                            capture_epoch_id=capture_epoch_id,
                        )
                        next_clock_sample_at = observed_mono + self.config.clock_sampling_interval_seconds
                    for role in required_roles:
                        try:
                            received = self._sockets[role].receive(0.01)
                        except Exception as exc:
                            raise _SocketRoleFailure(role, exc) from exc
                        if received is None:
                            continue
                        arrival_sequences[role] += 1
                        envelope = WireEnvelope(
                            received.raw_message,
                            received.received_time,
                            connection_ids[role],
                            epoch,
                            arrival_sequences[role],
                            capture_epoch_id,
                        )
                        normalization_started = self.monotonic()
                        try:
                            try:
                                parsed = connector.parse_message(envelope)
                            except CoordinatedWriterError:
                                raise
                            except Exception as exc:
                                raise _SocketRoleFailure(
                                    role,
                                    exc,
                                    operation="normalization",
                                ) from exc
                        finally:
                            self._normalization_timing.observe_seconds(
                                max(self.monotonic() - normalization_started, 0.0)
                            )
                        self._observe_critical_stream(
                            parsed,
                            received_time=received.received_time,
                            socket_role=role,
                        )
                        self.metrics["messages_received"] = self._counter("messages_received") + 1
                        self.metrics["normalization_issues"] = self._counter("normalization_issues") + len(
                            parsed.issues
                        )
                        if role == "public":
                            try:
                                self._record_l2_resync_if_needed(
                                    parsed,
                                    pending_assets=pending_l2_resync_assets,
                                    connection_id=connection_ids[role],
                                    connection_epoch=epoch,
                                    capture_epoch_id=capture_epoch_id,
                                )
                            except CoordinatedWriterError:
                                raise
                            except Exception as exc:
                                raise _SocketRoleFailure(
                                    role,
                                    exc,
                                    operation="l2_resync",
                                ) from exc
                        self._add_many(parsed.records)
                        if max_messages is not None and (self._counter("messages_received") >= max_messages):
                            break
                    now = self.clock()
                    self._require_critical_streams_fresh(
                        at=now,
                    )
                    self._refresh_capture_readiness(
                        at=now,
                        pending_l2_resync_assets=pending_l2_resync_assets,
                    )
                    status_observed_mono = self.monotonic()
                    if status_observed_mono >= next_status_publish_at:
                        self._publish()
                        next_status_publish_at = (
                            status_observed_mono + self.config.clock_sampling_interval_seconds
                        )
                self._drain_clock_sample(
                    active_capture_epoch_id=capture_epoch_id,
                    wait=False,
                )
                self._close_sockets(reason="collector stop requested or bounded run completed")
                self._record_generation_end(
                    connection_ids=connection_ids,
                    connected_roles=connected_roles,
                    connection_epoch=epoch,
                    capture_epoch_id=capture_epoch_id,
                    reason="collector stop requested or bounded run completed",
                    include_gap=False,
                )
                self._abandon_clock_sample()
                break
            except CoordinatedWriterError as exc:
                self._record_generation_failure(
                    connection_epoch=epoch,
                    capture_epoch_id=capture_epoch_id,
                    connected_roles=connected_roles,
                    error=exc,
                    will_reconnect=False,
                )
                self._abandon_clock_sample()
                self.metrics["last_error"] = f"{type(exc).__name__}: {exc}"
                self.metrics["maintenance_or_absence_detected"] = True
                self.metrics["capture_ready"] = False
                self.metrics["state"] = "failed"
                with suppress(Exception):
                    self._publish()
                raise
            except Exception as exc:
                fatal_local_capacity = self._is_fatal_local_capacity_error(exc)
                if not self._stop:
                    self._record_generation_failure(
                        connection_epoch=epoch,
                        capture_epoch_id=capture_epoch_id,
                        connected_roles=connected_roles,
                        error=exc,
                        will_reconnect=not fatal_local_capacity,
                    )
                self._close_sockets(reason=f"{type(exc).__name__}: {exc}")
                self._abandon_clock_sample()
                if self._stop:
                    self._record_generation_end(
                        connection_ids=connection_ids,
                        connected_roles=connected_roles,
                        connection_epoch=epoch,
                        capture_epoch_id=capture_epoch_id,
                        reason="collector stop requested",
                        include_gap=False,
                    )
                    break
                self.metrics["last_error"] = f"{type(exc).__name__}: {exc}"
                self.metrics["maintenance_or_absence_detected"] = True
                self.metrics["capture_ready"] = False
                self.metrics["state"] = "failed" if fatal_local_capacity else "backoff"
                try:
                    self._record_generation_end(
                        connection_ids=connection_ids,
                        connected_roles=connected_roles,
                        connection_epoch=epoch,
                        capture_epoch_id=capture_epoch_id,
                        reason=str(self.metrics["last_error"]),
                        include_gap=True,
                    )
                except CoordinatedWriterError:
                    self.metrics["state"] = "failed"
                    with suppress(Exception):
                        self._publish()
                    raise
                self._publish()
                if fatal_local_capacity:
                    raise
                self._interruptible_sleep(min(2 ** min(epoch, 5), 30))
            finally:
                self._close_sockets(reason="generation finalization")
        self.metrics["state"] = "stopped"
        self._publish()

    @staticmethod
    def _socket_telemetry(socket: PublicSocket) -> dict[str, object] | None:
        snapshot = getattr(socket, "telemetry_snapshot", None)
        if not callable(snapshot):
            return None
        try:
            value = snapshot()
        except Exception as exc:
            return {"telemetry_error": f"{type(exc).__name__}: {exc}"}
        if not isinstance(value, dict):
            return {"telemetry_error": "socket telemetry snapshot is not a dictionary"}
        return dict(value)

    def _socket_backlog_state(
        self,
        role: str,
        *,
        max_age_seconds: float,
    ) -> tuple[str, dict[str, object] | None]:
        socket = self._sockets.get(role)
        if socket is None:
            return "none", None
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
    def _is_fatal_local_capacity_error(error: BaseException) -> bool:
        original = error.original_error if isinstance(error, _SocketRoleFailure) else error
        return isinstance(
            original,
            (WebsocketQueueOverflow, WebsocketConsumerBackpressure),
        )

    def _socket_telemetry_by_role(self) -> dict[str, object]:
        snapshots: dict[str, object] = {}
        for role, socket in sorted(self._sockets.items()):
            snapshot = self._socket_telemetry(socket)
            if snapshot is not None:
                snapshots[role] = snapshot
        return snapshots

    def _writer_telemetry(self) -> dict[str, object] | None:
        snapshot = getattr(self.sink, "metrics_snapshot", None)
        if not callable(snapshot):
            return None
        try:
            value = snapshot()
        except Exception as exc:
            return {"telemetry_error": f"{type(exc).__name__}: {exc}"}
        if not isinstance(value, dict):
            return {"telemetry_error": "writer telemetry snapshot is not a dictionary"}
        return dict(value)

    def _safe_pending_rows(self) -> int | None:
        try:
            return self.sink.pending_count
        except Exception:
            return None

    def _record_generation_failure(
        self,
        *,
        connection_epoch: int,
        capture_epoch_id: str,
        connected_roles: Iterable[str],
        error: BaseException,
        will_reconnect: bool,
    ) -> None:
        roles = tuple(connected_roles)
        opened_roles = tuple(sorted(self._sockets))
        if isinstance(error, _SocketRoleFailure):
            initiating_role = error.socket_role
        elif isinstance(error, CoordinatedWriterError):
            initiating_role = "writer"
        else:
            initiating_role = "supervisor"
        detail = str(error).strip()
        reason = type(error).__name__ if not detail else f"{type(error).__name__}: {detail}"
        paired_handshake_failures = (
            [failure.as_diagnostic() for failure in error.failures]
            if isinstance(error, _PairedSocketHandshakeFailure)
            else []
        )
        self._generation_failures_seen += 1
        self._generation_failures.append(
            {
                "generation": connection_epoch if roles else None,
                "connection_attempt": connection_epoch,
                "capture_epoch_id": capture_epoch_id,
                "connected_socket_roles": list(roles),
                "opened_socket_roles": list(opened_roles),
                "initiating_role": initiating_role,
                "collateral_socket_roles": sorted(set(opened_roles) - {initiating_role}),
                "reason": reason,
                "paired_handshake_failures": paired_handshake_failures,
                "recorded_at": self.clock().isoformat(),
                "socket_telemetry": self._socket_telemetry_by_role(),
                "writer_pending_rows": self._safe_pending_rows(),
                "clock_future_pending": self._clock_future is not None,
                "will_reconnect": will_reconnect,
            }
        )

    def _publish(self) -> None:
        self.metrics["clock_observability"] = self._clock_observability_payload()
        now = self.clock()
        self.metrics["critical_stream_lag_seconds"] = {
            channel: max((now - received_at).total_seconds(), 0.0)
            for channel, received_at in sorted(self._critical_stream_last_received.items())
        }
        payload = dict(self.metrics)
        payload["ok"] = bool(self.metrics["state"] == "live" and self.metrics["capture_ready"])
        payload["updated_at"] = now.isoformat()
        payload["network_scope"] = "public market data only"
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
            },
            "sockets": self._socket_telemetry_by_role(),
            "worker_phases": {
                "normalization_ms": self._normalization_timing.as_dict(),
                "sink_enqueue_ms": self._sink_enqueue_timing.as_dict(),
            },
        }
        if self._last_closed_socket_telemetry is not None:
            observability["last_closed_sockets"] = dict(self._last_closed_socket_telemetry)
        writer_telemetry = self._writer_telemetry()
        if writer_telemetry is not None:
            observability["writer"] = writer_telemetry
        payload["observability"] = observability
        write_runtime_status(self.runtime_status_path, payload)
