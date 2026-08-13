from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from hyperlab.collector.models import ParsedMessage, ParsedRecord, WireEnvelope
from hyperlab.collector.storage import BatchingLakeSink, CoordinatedWriterError, LakeSink
from hyperlab.collector.websocket import PublicSocket, UrlWebsocketClientFactory
from hyperlab.data.schema import RecordType
from hyperlab.storage.sqlite import write_runtime_status
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

    def __post_init__(self) -> None:
        if not self.assets or len(self.assets) != len(set(self.assets)):
            raise ValueError("assets must be a non-empty unique list")
        if not self.candle_intervals or len(self.candle_intervals) != len(set(self.candle_intervals)):
            raise ValueError("candle intervals must be a non-empty unique list")
        if any(value <= 0 for value in (self.history_lookback_hours, self.batch_size, self.queue_capacity)):
            raise ValueError("reference collector limits must be positive")
        if self.queue_capacity < self.batch_size:
            raise ValueError("queue capacity cannot be smaller than batch size")
        if self.connect_timeout_seconds <= 0 or self.stale_after_seconds <= 0:
            raise ValueError("reference collector timeouts must be positive")


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
        self._socket: PublicSocket | None = None
        self._connector: BinancePublicConnector | None = None
        self._critical_stream_last_received: dict[str, datetime] = {}
        self.metrics: dict[str, object] = {
            "venue": VENUE,
            "state": "stopped",
            "messages_received": 0,
            "records_parsed": 0,
            "rows_written": 0,
            "normalization_issues": 0,
            "connections": 0,
            "reconnects": 0,
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
        if self._socket is not None:
            self._socket.close()

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
        asset: str = "GLOBAL",
        channel: str = "combined_market_stream",
        book_epoch_id: str | None = None,
        reason: str | None = None,
        at: datetime | None = None,
        received_at: datetime | None = None,
        resync_snapshot_id: str | None = None,
    ) -> None:
        event_time = self.clock() if at is None else at
        received_time = event_time if received_at is None else received_at
        row = {
            "schema_version": 1,
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
        }
        self._add(ParsedRecord(RecordType.CONNECTION_EVENT, asset, row))

    def _initialize_critical_streams(
        self,
        connector: BinancePublicConnector,
        *,
        at: datetime,
    ) -> None:
        channels: list[str] = []
        for asset in self.config.assets:
            symbol = connector.instrument_for_asset(asset).source_symbol.lower()
            channels.extend((f"{symbol}@bookTicker", f"{symbol}@depth20@100ms"))
        self._critical_stream_last_received = {channel: at for channel in channels}

    def _stale_critical_streams(self, *, at: datetime) -> tuple[str, ...]:
        threshold = timedelta(seconds=self.config.stale_after_seconds)
        return tuple(
            channel
            for channel, last_received in sorted(self._critical_stream_last_received.items())
            if at - last_received > threshold
        )

    def _require_critical_streams_fresh(
        self,
        *,
        at: datetime,
        connection_id: str,
    ) -> None:
        stale_channels = self._stale_critical_streams(at=at)
        if not stale_channels:
            return
        reason = "required Binance BBO/L2 streams stale: " + ", ".join(stale_channels)
        self.metrics["maintenance_or_absence_detected"] = True
        self.metrics["state"] = "stale"
        self._connection_event(
            "gap",
            connection_id=connection_id,
            channel=",".join(stale_channels),
            reason=reason,
            at=at,
        )
        self._publish()
        raise TimeoutError(reason)

    def _record_l2_resync_if_needed(
        self,
        parsed: ParsedMessage,
        *,
        pending_assets: set[str],
        connection_id: str,
        connection_epoch: int,
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
        clock_sample = self.rest.clock_measurement()
        self._add(clock_record(clock_sample, f"clock:{uuid.uuid4().hex}"))
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

    def run(self, *, duration_seconds: float | None = None, max_messages: int | None = None) -> None:
        if duration_seconds is not None and duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive when provided")
        if max_messages is not None and max_messages <= 0:
            raise ValueError("max_messages must be positive when provided")
        connector = self._bootstrap()
        factory = UrlWebsocketClientFactory(
            connector.websocket_url(self.config.assets, self.config.candle_intervals),
            queue_capacity=self.config.queue_capacity,
            clock=self.clock,
        )
        started = self.monotonic()
        epoch = 0
        while not self._stop:
            if duration_seconds is not None and self.monotonic() - started >= duration_seconds:
                break
            if max_messages is not None and self._counter("messages_received") >= max_messages:
                break
            epoch += 1
            connection_id = uuid.uuid4().hex
            arrival_sequence = 0
            if epoch > 1:
                self.metrics["reconnects"] = self._counter("reconnects") + 1
            try:
                self.metrics["state"] = "connecting"
                self._socket = factory.connect("public", self.config.connect_timeout_seconds)
                self.metrics["connections"] = self._counter("connections") + 1
                self.metrics["state"] = "live"
                self._connection_event("connect", connection_id=connection_id)
                connection_started = self.clock()
                self._initialize_critical_streams(connector, at=connection_started)
                pending_l2_resync_assets = set(self.config.assets)
                self._publish()
                while not self._stop:
                    if duration_seconds is not None and self.monotonic() - started >= duration_seconds:
                        break
                    received = self._socket.receive(1.0)
                    now = self.clock()
                    if received is None:
                        self._require_critical_streams_fresh(
                            at=now,
                            connection_id=connection_id,
                        )
                        continue
                    arrival_sequence += 1
                    envelope = WireEnvelope(
                        received.raw_message,
                        received.received_time,
                        connection_id,
                        epoch,
                        arrival_sequence,
                    )
                    parsed = connector.parse_message(envelope)
                    if parsed.channel in self._critical_stream_last_received:
                        self._critical_stream_last_received[parsed.channel] = received.received_time
                    self.metrics["messages_received"] = self._counter("messages_received") + 1
                    self.metrics["normalization_issues"] = self._counter("normalization_issues") + len(
                        parsed.issues
                    )
                    self._record_l2_resync_if_needed(
                        parsed,
                        pending_assets=pending_l2_resync_assets,
                        connection_id=connection_id,
                        connection_epoch=epoch,
                    )
                    self._add_many(parsed.records)
                    self._require_critical_streams_fresh(
                        at=now,
                        connection_id=connection_id,
                    )
                    if max_messages is not None and self._counter("messages_received") >= max_messages:
                        break
                if (
                    self._stop
                    or (duration_seconds is not None and self.monotonic() - started >= duration_seconds)
                    or (max_messages is not None and self._counter("messages_received") >= max_messages)
                ):
                    break
            except CoordinatedWriterError as exc:
                self.metrics["last_error"] = f"{type(exc).__name__}: {exc}"
                self.metrics["maintenance_or_absence_detected"] = True
                self.metrics["state"] = "failed"
                with suppress(Exception):
                    self._publish()
                raise
            except Exception as exc:
                if self._stop:
                    break
                self.metrics["last_error"] = f"{type(exc).__name__}: {exc}"
                self.metrics["maintenance_or_absence_detected"] = True
                self.metrics["state"] = "backoff"
                self._connection_event(
                    "disconnect",
                    connection_id=connection_id,
                    reason=str(self.metrics["last_error"]),
                )
                self._publish()
                self._interruptible_sleep(min(2 ** min(epoch, 5), 30))
            finally:
                if self._socket is not None:
                    self._socket.close()
                    self._socket = None
        self.metrics["state"] = "stopped"
        self._publish()

    def _publish(self) -> None:
        now = self.clock()
        self.metrics["critical_stream_lag_seconds"] = {
            channel: max((now - received_at).total_seconds(), 0.0)
            for channel, received_at in sorted(self._critical_stream_last_received.items())
        }
        payload = dict(self.metrics)
        payload["updated_at"] = now.isoformat()
        payload["network_scope"] = "public market data only"
        write_runtime_status(self.runtime_status_path, payload)
