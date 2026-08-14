from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from hyperlab.collector.models import ParsedMessage, ParsedRecord, WireEnvelope
from hyperlab.collector.storage import BatchingLakeSink, CoordinatedWriterError, LakeSink
from hyperlab.collector.websocket import PublicSocket, UrlWebsocketClientFactory
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
        if (
            not self.clock_max_uncertainty_ms.is_finite()
            or self.clock_max_uncertainty_ms < 0
        ):
            raise ValueError("clock maximum uncertainty must be finite and non-negative")


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
        self._connector: BinancePublicConnector | None = None
        self._critical_stream_last_received: dict[str, datetime] = {}
        self._critical_stream_expectations: dict[
            str,
            tuple[str, RecordType, str],
        ] = {}
        self._critical_stream_seen: set[str] = set()
        self._valid_clock_until: datetime | None = None
        self._clock_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="binance-public-clock",
        )
        self._clock_future: Future[ClockMeasurement] | None = None
        self._clock_future_context: tuple[str, str, int] | None = None
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
            "clock_sample_failures": 0,
            "capture_ready": False,
            "missing_required_streams": [],
            "pending_l2_resync_assets": list(config.assets),
            "clock_sync_valid": False,
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
        for socket in tuple(self._sockets.values()):
            with suppress(Exception):
                socket.close()

    def _close_sockets(self) -> None:
        sockets = tuple(self._sockets.values())
        self._sockets.clear()
        for socket in sockets:
            with suppress(Exception):
                socket.close()

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
        if not errors:
            return
        first_label, primary = errors[0]
        primary.add_note(f"cleanup action: {first_label}")
        for label, secondary in errors[1:]:
            primary.add_note(f"{label} also failed: {type(secondary).__name__}: {secondary}")
        raise primary

    def _add(self, record: ParsedRecord) -> None:
        if self.sink.add(record):
            self.metrics["records_parsed"] = self._counter("records_parsed") + 1
        if self.sink.should_flush:
            result = self.sink.flush()
            self.metrics["rows_written"] = self._counter("rows_written") + result.row_count

    def _add_many(self, records: Iterable[ParsedRecord]) -> None:
        accepted = self.sink.add_many(records)
        self.metrics["records_parsed"] = self._counter("records_parsed") + accepted
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
        missing_streams = sorted(
            set(self._critical_stream_last_received) - self._critical_stream_seen
        )
        clock_valid = self._valid_clock_until is not None and at < self._valid_clock_until
        stale_streams = self._stale_critical_streams(at=at)
        ready = (
            not missing_streams
            and not pending_l2_resync_assets
            and not stale_streams
            and clock_valid
        )
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
        expected_asset, expected_type, expected_role = self._critical_stream_expectations[
            channel
        ]
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
        reason = "required Binance depth-derived BBO/L2 or trade streams stale: " + ", ".join(stale_channels)
        self.metrics["maintenance_or_absence_detected"] = True
        self.metrics["capture_ready"] = False
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

    def _schedule_clock_sample(
        self,
        *,
        connection_id: str,
        connection_epoch: int,
        capture_epoch_id: str,
    ) -> None:
        if self._clock_future is not None:
            return
        self._clock_future_context = (
            capture_epoch_id,
            connection_id,
            connection_epoch,
        )
        self._clock_future = self._clock_executor.submit(self.rest.clock_measurement)

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
        capture_epoch_id, connection_id, connection_epoch = context
        try:
            measurement = future.result()
        except Exception as exc:
            reason = f"clock_sync request failed: {type(exc).__name__}: {exc}"
            self.metrics["clock_sample_failures"] = self._counter(
                "clock_sample_failures"
            ) + 1
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
        if active_capture_epoch_id != capture_epoch_id:
            self.metrics["clock_sample_failures"] = self._counter(
                "clock_sample_failures"
            ) + 1
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
            sampling_interval=timedelta(
                seconds=self.config.clock_sampling_interval_seconds
            ),
            max_age=timedelta(seconds=self.config.clock_max_age_seconds),
            max_uncertainty_ms=self.config.clock_max_uncertainty_ms,
        )
        self._add(record)
        self.metrics["clock_samples"] = self._counter("clock_samples") + 1
        status = str(record.row["sample_status"])
        counter = (
            "clock_samples_valid" if status == "valid" else "clock_samples_invalid"
        )
        self.metrics[counter] = self._counter(counter) + 1
        valid_until = record.row["causal_valid_until"]
        if status == "valid" and isinstance(valid_until, datetime):
            self._valid_clock_until = valid_until
            self.metrics["clock_sync_valid"] = True
        else:
            self._valid_clock_until = None
            self.metrics["capture_ready"] = False
            self.metrics["clock_sync_valid"] = False
            self.metrics["maintenance_or_absence_detected"] = True
        return True

    def _abandon_clock_sample(self) -> None:
        future = self._clock_future
        self._clock_future = None
        self._clock_future_context = None
        if future is not None:
            future.cancel()

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
            connection_ids = {
                role: f"binance-{role}-{uuid.uuid4().hex}" for role in required_roles
            }
            arrival_sequences = {role: 0 for role in required_roles}
            connected_roles: list[str] = []
            if epoch > 1:
                self.metrics["reconnects"] = self._counter("reconnects") + 1
            try:
                self.metrics["state"] = "connecting"
                self.metrics["capture_ready"] = False
                self.metrics["clock_sync_valid"] = False
                self._valid_clock_until = None
                self.metrics["capture_epoch_id"] = capture_epoch_id
                self.metrics["physical_connection_ids"] = dict(connection_ids)
                open_errors: list[Exception] = []
                with ThreadPoolExecutor(
                    max_workers=len(required_roles),
                    thread_name_prefix="binance-public-connect",
                ) as connect_executor:
                    connect_futures: dict[Future[PublicSocket], str] = {
                        connect_executor.submit(open_paused, role): role
                        for role in required_roles
                    }
                    for future in as_completed(connect_futures):
                        role = connect_futures[future]
                        try:
                            socket = future.result()
                        except Exception as exc:
                            open_errors.append(exc)
                            continue
                        self._sockets[role] = socket
                        self.metrics["physical_connections"] = self._counter(
                            "physical_connections"
                        ) + 1
                if open_errors:
                    primary = open_errors[0]
                    for secondary in open_errors[1:]:
                        primary.add_note(
                            "paired Binance socket handshake also failed: "
                            f"{type(secondary).__name__}: {secondary}"
                        )
                    raise primary
                if self._stop or (
                    duration_seconds is not None
                    and self.monotonic() - started >= duration_seconds
                ):
                    self._close_sockets()
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
                while not self._stop:
                    if duration_seconds is not None and self.monotonic() - started >= duration_seconds:
                        break
                    if max_messages is not None and (
                        self._counter("messages_received") >= max_messages
                    ):
                        break
                    observed_mono = self.monotonic()
                    self._drain_clock_sample(
                        active_capture_epoch_id=capture_epoch_id,
                        wait=False,
                    )
                    self._refresh_capture_readiness(
                        at=self.clock(),
                        pending_l2_resync_assets=pending_l2_resync_assets,
                    )
                    if (
                        self._clock_future is None
                        and observed_mono >= next_clock_sample_at
                    ):
                        self._schedule_clock_sample(
                            connection_id=connection_ids["public"],
                            connection_epoch=epoch,
                            capture_epoch_id=capture_epoch_id,
                        )
                        next_clock_sample_at = (
                            observed_mono
                            + self.config.clock_sampling_interval_seconds
                        )
                    for role in required_roles:
                        received = self._sockets[role].receive(0.01)
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
                        parsed = connector.parse_message(envelope)
                        self._observe_critical_stream(
                            parsed,
                            received_time=received.received_time,
                            socket_role=role,
                        )
                        self.metrics["messages_received"] = self._counter(
                            "messages_received"
                        ) + 1
                        self.metrics["normalization_issues"] = self._counter(
                            "normalization_issues"
                        ) + len(parsed.issues)
                        if role == "public":
                            self._record_l2_resync_if_needed(
                                parsed,
                                pending_assets=pending_l2_resync_assets,
                                connection_id=connection_ids[role],
                                connection_epoch=epoch,
                                capture_epoch_id=capture_epoch_id,
                            )
                        self._add_many(parsed.records)
                        if max_messages is not None and (
                            self._counter("messages_received") >= max_messages
                        ):
                            break
                    now = self.clock()
                    self._require_critical_streams_fresh(
                        at=now,
                    )
                    self._refresh_capture_readiness(
                        at=now,
                        pending_l2_resync_assets=pending_l2_resync_assets,
                    )
                self._drain_clock_sample(
                    active_capture_epoch_id=capture_epoch_id,
                    wait=False,
                )
                self._close_sockets()
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
                self._abandon_clock_sample()
                self.metrics["last_error"] = f"{type(exc).__name__}: {exc}"
                self.metrics["maintenance_or_absence_detected"] = True
                self.metrics["capture_ready"] = False
                self.metrics["state"] = "failed"
                with suppress(Exception):
                    self._publish()
                raise
            except Exception as exc:
                self._close_sockets()
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
                self.metrics["state"] = "backoff"
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
                self._interruptible_sleep(min(2 ** min(epoch, 5), 30))
            finally:
                self._close_sockets()
        self.metrics["state"] = "stopped"
        self._publish()

    def _publish(self) -> None:
        now = self.clock()
        self.metrics["critical_stream_lag_seconds"] = {
            channel: max((now - received_at).total_seconds(), 0.0)
            for channel, received_at in sorted(self._critical_stream_last_received.items())
        }
        payload = dict(self.metrics)
        payload["ok"] = bool(
            self.metrics["state"] == "live" and self.metrics["capture_ready"]
        )
        payload["updated_at"] = now.isoformat()
        payload["network_scope"] = "public market data only"
        write_runtime_status(self.runtime_status_path, payload)
