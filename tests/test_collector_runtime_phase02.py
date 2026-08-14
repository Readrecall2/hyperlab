from __future__ import annotations

import json
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from hyperlab.api.public import PublicBootstrap
from hyperlab.collector.models import (
    CollectorConfig,
    CollectorState,
    ParsedRecord,
    PublicSubscription,
    WireEnvelope,
)
from hyperlab.collector.parser import parse_websocket_message
from hyperlab.collector.runtime import PublicCollector
from hyperlab.collector.storage import BatchingLakeSink, FlushResult
from hyperlab.data.lake import PartitionValidationError
from hyperlab.data.schema import RecordType

BASE_TIME = datetime(2026, 8, 12, 12, tzinfo=UTC)


@dataclass
class ControlledTime:
    elapsed: float = 0.0

    def monotonic(self) -> float:
        return self.elapsed

    def now(self) -> datetime:
        return BASE_TIME + timedelta(seconds=self.elapsed)

    def advance(self, seconds: float) -> None:
        self.elapsed += seconds


class FakeRest:
    def __init__(self) -> None:
        self.bootstrap_calls = 0
        self.funding_calls: list[tuple[str, int, int | None]] = []
        self.close_calls = 0
        self.candle_calls: list[tuple[str, str, int, int]] = []
        self.l2_calls: list[str] = []

    def bootstrap(self, *, observed_at_ms: int | None = None) -> PublicBootstrap:
        self.bootstrap_calls += 1
        observed_at_ms = observed_at_ms or int(BASE_TIME.timestamp() * 1_000)
        return PublicBootstrap(
            observed_at_ms=observed_at_ms,
            perp_payload=[
                {
                    "universe": [
                        {
                            "name": "BTC",
                            "szDecimals": 5,
                            "maxLeverage": 50,
                            "marginTableId": 20,
                        }
                    ]
                },
                [
                    {
                        "dayBaseVlm": "10",
                        "dayNtlVlm": "500000",
                        "funding": "0.00001",
                        "markPx": "50000",
                        "midPx": "50000.5",
                        "openInterest": "2",
                        "oraclePx": "50001",
                        "prevDayPx": "49000",
                    }
                ],
            ],
            spot_payload=[{"tokens": [], "universe": []}, []],
        )

    def funding_history(
        self,
        asset: str,
        start_ms: int,
        end_ms: int | None = None,
    ) -> object:
        self.funding_calls.append((asset, start_ms, end_ms))
        assert end_ms is not None
        return [
            {
                "coin": asset,
                "fundingRate": "0.00001",
                "premium": "0",
                "time": end_ms - 3_600_000,
            }
        ]

    def candles(self, asset: str, interval: str, start_ms: int, end_ms: int) -> object:
        self.candle_calls.append((asset, interval, start_ms, end_ms))
        return [
            {
                "T": end_ms - 60_000,
                "c": "50000",
                "h": "50010",
                "i": interval,
                "l": "49990",
                "n": 3,
                "o": "49995",
                "s": asset,
                "t": end_ms - 119_999,
                "v": "1.5",
            }
        ]

    def l2_snapshot(
        self,
        asset: str,
        *,
        n_sig_figs: int | None = None,
        mantissa: int | None = None,
    ) -> object:
        assert n_sig_figs is None
        assert mantissa is None
        self.l2_calls.append(asset)
        return {
            "coin": asset,
            "levels": [
                [{"n": 1, "px": "49999", "sz": "2"}],
                [{"n": 1, "px": "50001", "sz": "3"}],
            ],
            "time": int(BASE_TIME.timestamp() * 1_000),
        }

    def close(self) -> None:
        self.close_calls += 1


SocketItem = tuple[str | BaseException | None, float]


class FakeSocket:
    def __init__(self, timer: ControlledTime, items: list[SocketItem]) -> None:
        self.timer = timer
        self.items = list(items)
        self.sent: list[dict[str, object]] = []
        self.closed = False

    def send_json(self, payload: dict[str, object]) -> None:
        self.sent.append(payload)

    def receive(self, timeout_seconds: float) -> str | None:
        assert timeout_seconds == 1.0
        if not self.items:
            raise AssertionError("fake socket script exhausted")
        item, advance_seconds = self.items.pop(0)
        self.timer.advance(advance_seconds)
        if isinstance(item, BaseException):
            raise item
        return item

    def close(self) -> None:
        self.closed = True


class FakeSocketFactory:
    def __init__(self, sockets: list[FakeSocket]) -> None:
        self.sockets = list(sockets)
        self.connect_calls: list[tuple[str, float]] = []

    def connect(self, network: str, timeout_seconds: float) -> FakeSocket:
        self.connect_calls.append((network, timeout_seconds))
        if not self.sockets:
            raise AssertionError("fake socket factory exhausted")
        return self.sockets.pop(0)


class StateRecordingSink(BatchingLakeSink):
    def __init__(self, root: Path) -> None:
        super().__init__(root, batch_size=1_000, queue_capacity=2_000)
        self.state_provider: Callable[[], CollectorState] | None = None
        self.flush_states: list[CollectorState] = []

    def flush(self) -> FlushResult:
        if self.state_provider is not None:
            self.flush_states.append(self.state_provider())
        return super().flush()


def _ack_messages(config: CollectorConfig) -> list[SocketItem]:
    return [
        (
            json.dumps(
                {
                    "channel": "subscriptionResponse",
                    "data": {
                        "method": "subscribe",
                        "subscription": subscription.payload(),
                    },
                },
                separators=(",", ":"),
            ),
            0.0,
        )
        for subscription in config.subscriptions()
    ]


def _candle_message(asset: str, interval: str, *, close_offset_seconds: int = 0) -> str:
    close_time_ms = int((BASE_TIME + timedelta(seconds=close_offset_seconds)).timestamp() * 1_000)
    interval_seconds = {"1m": 60, "5m": 300}[interval]
    return json.dumps(
        {
            "channel": "candle",
            "data": {
                "t": close_time_ms - interval_seconds * 1_000 + 1,
                "T": close_time_ms,
                "s": asset,
                "i": interval,
                "o": "50000",
                "c": "50001",
                "h": "50002",
                "l": "49999",
                "v": "1.5",
                "n": 3,
            },
        },
        separators=(",", ":"),
    )


def _config(**overrides: Any) -> CollectorConfig:
    values: dict[str, Any] = {
        "assets": ("BTC",),
        "candle_intervals": ("1m",),
        "batch_size": 1_000,
        "queue_capacity": 2_000,
        "flush_interval_seconds": 5.0,
        "heartbeat_interval_seconds": 2.0,
        "pong_timeout_seconds": 10.0,
        "stale_after_seconds": 20.0,
        "backoff_initial_seconds": 2.0,
        "backoff_max_seconds": 3.0,
        "backoff_jitter_ratio": 1.0,
        "history_lookback_hours": 1,
    }
    values.update(overrides)
    return CollectorConfig(**values)


def _collector(
    tmp_path: Path,
    config: CollectorConfig,
    timer: ControlledTime,
    sockets: list[FakeSocket],
    *,
    sleeper: Callable[[float], None] = lambda _delay: None,
) -> tuple[PublicCollector, FakeRest, FakeSocketFactory, StateRecordingSink]:
    rest = FakeRest()
    factory = FakeSocketFactory(sockets)
    sink = StateRecordingSink(tmp_path / "lake")
    connection_ids = iter(("first", "second", "third"))
    collector = PublicCollector(
        config,
        rest=rest,
        socket_factory=factory,
        sink=sink,
        runtime_status_path=tmp_path / "runtime_status.json",
        clock=timer.now,
        monotonic=timer.monotonic,
        sleeper=sleeper,
        random_value=lambda: 1.0,
        connection_id_factory=lambda: next(connection_ids),
    )
    sink.state_provider = lambda: collector.metrics.state
    return collector, rest, factory, sink


def _parquet_rows(root: Path, record_type: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for path in root.rglob("*.parquet"):
        if f"type={record_type}" in path.as_posix():
            rows.extend(pq.ParquetFile(path).read().to_pylist())
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("event_time")),
            str(row.get("connection_id")),
            str(row.get("event_kind")),
        ),
    )


def _assert_all_source_sequences_are_null(root: Path) -> None:
    paths = list(root.rglob("*.parquet"))
    assert paths
    for path in paths:
        table = pq.ParquetFile(path).read()
        assert table.column("source_sequence").null_count == table.num_rows, path


def test_rest_bootstrap_precedes_ws_acks_and_live_flush(tmp_path: Path) -> None:
    timer = ControlledTime()
    config = _config()
    socket = FakeSocket(timer, _ack_messages(config))
    collector, rest, factory, sink = _collector(tmp_path, config, timer, [socket])

    metrics = collector.run(max_messages=config.subscription_count)

    assert metrics.state == CollectorState.STOPPED
    assert (metrics.connections, metrics.reconnects, metrics.resyncs, metrics.gaps) == (1, 0, 0, 0)
    assert rest.bootstrap_calls == 1
    assert len(rest.funding_calls) == len(rest.candle_calls) == len(rest.l2_calls) == 1
    assert factory.connect_calls == [("mainnet", config.ws_connect_timeout_seconds)]
    assert socket.sent == [
        {"method": "subscribe", "subscription": subscription.payload()}
        for subscription in config.subscriptions()
    ]
    bbo_rows = _parquet_rows(sink.root, "bbo")
    assert len(bbo_rows) == 1
    assert str(bbo_rows[0]["bid_price"]) == "49999.000000000000000000"
    assert bbo_rows[0]["source_sequence"] is None
    assert CollectorState.BOOTSTRAPPING in sink.flush_states
    assert CollectorState.LIVE in sink.flush_states
    connection_events = _parquet_rows(sink.root, "connection_event")
    assert [row["event_kind"] for row in connection_events] == [
        "connect",
        "disconnect",
    ]
    assert all(row["schema_version"] == 2 for row in connection_events)
    assert all(row["connection_epoch"] == 1 for row in connection_events)
    assert all(row["capture_epoch_id"] for row in connection_events)
    assert all(row["socket_role"] == "public" for row in connection_events)
    _assert_all_source_sequences_are_null(sink.root)

    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    assert status["mode"] == "readonly"
    assert status["orders_enabled"] is False
    assert status["metrics"]["state"] == "stopped"


def test_heartbeat_ping_and_pong_are_observed_without_real_time(tmp_path: Path) -> None:
    timer = ControlledTime()
    config = _config()
    socket = FakeSocket(
        timer,
        [
            *_ack_messages(config),
            (None, 3.0),
            ('{"channel":"pong"}', 0.0),
        ],
    )
    collector, _rest, _factory, _sink = _collector(tmp_path, config, timer, [socket])

    metrics = collector.run(max_messages=config.subscription_count + 1)

    assert [payload for payload in socket.sent if payload == {"method": "ping"}] == [{"method": "ping"}]
    assert (metrics.pings_sent, metrics.pongs_received) == (1, 1)
    assert metrics.last_pong_at == BASE_TIME + timedelta(seconds=3)
    assert metrics.messages_received == config.subscription_count + 1


def test_disconnect_records_unknown_coverage_then_reconnects_with_bounded_backoff(
    tmp_path: Path,
) -> None:
    timer = ControlledTime()
    config = _config()
    first = FakeSocket(
        timer,
        [*_ack_messages(config), (ConnectionError("wire cut"), 0.0)],
    )
    second = FakeSocket(timer, _ack_messages(config))
    sleeps: list[float] = []
    collector, rest, factory, sink = _collector(
        tmp_path,
        config,
        timer,
        [first, second],
        sleeper=sleeps.append,
    )

    metrics = collector.run(max_messages=config.subscription_count * 2)

    assert sum(sleeps) == config.backoff_max_seconds
    assert max(sleeps) <= 0.25
    assert metrics.connections == 2
    assert metrics.reconnects == 1
    assert metrics.resyncs == 1
    assert metrics.gaps == 1
    assert metrics.last_error is None
    assert metrics.last_failure is not None
    assert "wire cut" in metrics.last_failure
    assert metrics.last_recovered_at is not None
    assert rest.bootstrap_calls == 2
    assert factory.connect_calls == [
        ("mainnet", config.ws_connect_timeout_seconds),
        ("mainnet", config.ws_connect_timeout_seconds),
    ]
    assert first.closed is True
    assert second.closed is True
    reasons = [str(row["reason"]) for row in _parquet_rows(sink.root, "connection_event")]
    assert any(reason.startswith("coverage_unknown:no public server sequence") for reason in reasons)
    _assert_all_source_sequences_are_null(sink.root)


def test_backoff_resets_only_after_a_stable_live_window(tmp_path: Path) -> None:
    timer = ControlledTime()
    config = _config(
        backoff_jitter_ratio=0.0,
        backoff_reset_after_seconds=60.0,
        heartbeat_interval_seconds=10.0,
        pong_timeout_seconds=120.0,
        stale_after_seconds=1_000.0,
    )
    first = FakeSocket(
        timer,
        [
            *_ack_messages(config),
            (None, 61.0),
            (ConnectionError("cut after stable live"), 0.0),
        ],
    )
    second = FakeSocket(timer, _ack_messages(config))
    sleeps: list[float] = []
    collector, _rest, _factory, _sink = _collector(
        tmp_path,
        config,
        timer,
        [first, second],
        sleeper=sleeps.append,
    )
    collector._backoff.attempt = 4

    collector.run(max_messages=config.subscription_count * 2)

    assert sum(sleeps) == config.backoff_initial_seconds
    assert max(sleeps) <= 0.25


def test_backoff_and_connection_rate_waits_are_interruptible(tmp_path: Path) -> None:
    timer = ControlledTime()
    config = _config()
    collector, _rest, _factory, _sink = _collector(tmp_path, config, timer, [])
    sleeps: list[float] = []

    def stop_on_first_slice(delay: float) -> None:
        sleeps.append(delay)
        collector.stop()

    collector.sleeper = stop_on_first_slice
    assert collector._interruptible_sleep(30.0) is False
    assert sleeps == [0.25]

    collector._stop_requested = False
    sleeps.clear()
    collector._connection_attempts.extend([0.0] * 29)
    assert collector._limit_connection_rate() is False
    assert sleeps == [0.25]
    collector.close()


def test_candle_observation_is_stored_without_fabricated_finality(tmp_path: Path) -> None:
    timer = ControlledTime()
    config = _config()
    collector, _rest, _factory, sink = _collector(tmp_path, config, timer, [])
    fixture = Path(__file__).parent / "fixtures" / "hyperliquid" / "ws_candle.json"
    raw_message = fixture.read_text(encoding="utf-8")
    wire = json.loads(raw_message)
    close_time = datetime.fromtimestamp(int(wire["data"]["T"]) / 1_000, tz=UTC)
    parsed = parse_websocket_message(
        WireEnvelope(
            raw_message=raw_message,
            received_time=close_time - timedelta(milliseconds=1),
            connection_id="candle-test",
            connection_epoch=1,
            arrival_sequence=1,
        )
    )
    candle = next(record for record in parsed.records if record.record_type.value == "candle")

    collector._store_record(candle)

    assert sink.pending_count == 1
    sink.flush()
    rows = _parquet_rows(sink.root, "candle")
    assert len(rows) == 1
    assert rows[0]["is_final"] is None
    assert rows[0]["received_time"] == close_time - timedelta(milliseconds=1)


def test_stale_critical_channels_are_visible_and_force_disconnect(tmp_path: Path) -> None:
    timer = ControlledTime()
    config = _config(
        heartbeat_interval_seconds=10.0,
        pong_timeout_seconds=20.0,
        stale_after_seconds=2.0,
    )
    socket = FakeSocket(timer, [*_ack_messages(config), (None, 3.0)])
    collector, _rest, _factory, sink = _collector(tmp_path, config, timer, [socket])

    metrics = collector.run(duration_seconds=2.5)

    assert metrics.gaps == 1
    assert metrics.stale_channels == (
        "activeAssetCtx:BTC",
        "bbo:BTC",
        "l2Book:BTC",
        "trades:BTC",
    )
    assert socket.closed is True
    reasons = [str(row["reason"]) for row in _parquet_rows(sink.root, "connection_event")]
    assert any("stale critical public streams" in reason for reason in reasons)
    assert any(reason.startswith("coverage_unknown:") for reason in reasons)
    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    assert status["ok"] is False
    assert "stale critical public streams" in status["metrics"]["last_error"]


def test_trade_silence_is_critical_while_bbo_context_and_l2_remain_fresh(
    tmp_path: Path,
) -> None:
    timer = ControlledTime()
    config = _config(
        heartbeat_interval_seconds=100.0,
        pong_timeout_seconds=200.0,
        stale_after_seconds=2.0,
    )
    fixture_root = Path(__file__).parent / "fixtures" / "hyperliquid"
    active_context = (fixture_root / "ws_active_asset_ctx.json").read_text(
        encoding="utf-8"
    )
    bbo = (fixture_root / "ws_bbo.json").read_text(encoding="utf-8")
    l2_book = (fixture_root / "ws_l2_book.json").read_text(encoding="utf-8")
    trade = (fixture_root / "ws_trades.json").read_text(encoding="utf-8")
    socket = FakeSocket(
        timer,
        [
            *_ack_messages(config),
            (trade, 0.0),
            (active_context, 0.0),
            (bbo, 0.0),
            (l2_book, 0.0),
            (active_context, 1.5),
            (bbo, 0.0),
            (l2_book, 0.0),
            (active_context, 1.0),
        ],
    )
    collector, _rest, _factory, sink = _collector(
        tmp_path,
        config,
        timer,
        [socket],
    )

    metrics = collector.run(duration_seconds=2.4)

    assert metrics.gaps == 1
    assert metrics.stale_channels == ("trades:BTC",)
    assert socket.closed is True
    reasons = [
        str(row["reason"])
        for row in _parquet_rows(sink.root, "connection_event")
    ]
    assert any("trades:BTC" in reason for reason in reasons)
    collector.close()


def test_invalid_batch_flush_is_terminal_and_close_does_not_retry_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timer = ControlledTime()
    collector, _rest, _factory, sink = _collector(
        tmp_path,
        _config(),
        timer,
        [],
        sleeper=timer.advance,
    )
    invalid_raw = json.dumps(
        {
            "channel": "activeSpotAssetCtx",
            "data": {
                "coin": "@71",
                "ctx": {
                    "markPx": "1.0",
                    "midPx": None,
                    "prevDayPx": "-1.0",
                    "dayNtlVlm": "0.0",
                    "circulatingSupply": "0.0",
                },
            },
        },
        separators=(",", ":"),
    )
    parsed = parse_websocket_message(
        WireEnvelope(
            raw_message=invalid_raw,
            received_time=timer.now(),
            connection_id="invalid-batch",
            connection_epoch=1,
            arrival_sequence=1,
        )
    )
    invalid_context = next(
        record for record in parsed.records if record.record_type.value == "market_context"
    )
    sink.add(invalid_context)
    original_flush = sink.flush
    attempts = 0

    def count_invalid_batch_attempt() -> FlushResult:
        nonlocal attempts
        attempts += 1
        return original_flush()

    monkeypatch.setattr(sink, "flush", count_invalid_batch_attempt)
    with pytest.raises(PartitionValidationError, match="previous_day_price must be non-negative"):
        try:
            collector.run(duration_seconds=0.5)
        finally:
            collector.close()
    assert attempts == 1
    assert sink._closed is True
    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    assert status["error"] == "PartitionValidationError: previous_day_price must be non-negative"

    collector.close()
    assert attempts == 1


def test_close_releases_resources_when_completed_rest_refresh_application_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timer = ControlledTime()
    collector, rest, _factory, sink = _collector(tmp_path, _config(), timer, [])
    completed: Future[tuple[ParsedRecord, ...]] = Future()
    completed.set_result(())
    collector._rest_future = completed

    valid = parse_websocket_message(
        WireEnvelope(
            raw_message=(
                '{"channel":"activeSpotAssetCtx","data":{"coin":"@71","ctx":'
                '{"markPx":"1.0","midPx":null,"prevDayPx":"0.0",'
                '"dayNtlVlm":"0.0","circulatingSupply":"0.0"}}}'
            ),
            received_time=timer.now(),
            connection_id="pending-valid-batch",
            connection_epoch=1,
            arrival_sequence=1,
        )
    )
    valid_context = next(
        record for record in valid.records if record.record_type == RecordType.MARKET_CONTEXT
    )
    sink.add(valid_context)

    def fail_refresh_application(_future: Future[tuple[ParsedRecord, ...]]) -> None:
        raise RuntimeError("refresh application failed")

    monkeypatch.setattr(collector, "_apply_rest_future", fail_refresh_application)

    with pytest.raises(RuntimeError, match="refresh application failed"):
        collector.close()

    assert sink._closed is True
    assert rest.close_calls == 1
    assert sink.pending_count == 0
    assert len(_parquet_rows(sink.root, "market_context")) == 1
    collector.close()
    assert rest.close_calls == 1


def test_close_flushes_remainder_after_successful_internal_flush(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timer = ControlledTime()
    collector, rest, _factory, sink = _collector(tmp_path, _config(), timer, [])
    records: list[ParsedRecord] = []
    for sequence in range(1, 4):
        parsed = parse_websocket_message(
            WireEnvelope(
                raw_message=(
                    '{"channel":"activeSpotAssetCtx","data":{"coin":"@71","ctx":'
                    '{"markPx":"1.0","midPx":null,"prevDayPx":"0.0",'
                    '"dayNtlVlm":"0.0","circulatingSupply":"0.0"}}}'
                ),
                received_time=timer.now(),
                connection_id="successful-internal-flush",
                connection_epoch=1,
                arrival_sequence=sequence,
            )
        )
        records.append(
            next(record for record in parsed.records if record.record_type == RecordType.MARKET_CONTEXT)
        )
    completed: Future[tuple[ParsedRecord, ...]] = Future()
    completed.set_result(tuple(records))
    collector._rest_future = completed
    sink.batch_size = 2

    def fail_status(*, error: str | None = None) -> None:
        del error
        raise OSError("status write failed")

    monkeypatch.setattr(collector, "_publish_status", fail_status)

    with pytest.raises(OSError, match="status write failed"):
        collector.close()

    assert sink.pending_count == 0
    assert len(_parquet_rows(sink.root, "market_context")) == 3
    assert sink._closed is True
    assert rest.close_calls == 1


def test_close_runs_remaining_cleanup_after_base_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timer = ControlledTime()
    collector, rest, _factory, sink = _collector(tmp_path, _config(), timer, [])
    sink_close_calls = 0

    def interrupt_sink_close() -> None:
        nonlocal sink_close_calls
        sink_close_calls += 1
        raise KeyboardInterrupt("second stop signal")

    monkeypatch.setattr(sink, "close", interrupt_sink_close)

    with pytest.raises(KeyboardInterrupt, match="second stop signal"):
        collector.close()

    assert sink_close_calls == 1
    assert rest.close_calls == 1
    collector.close()
    assert rest.close_calls == 1


def test_close_does_not_retry_flush_interrupted_while_applying_rest_future(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timer = ControlledTime()
    collector, rest, _factory, sink = _collector(tmp_path, _config(), timer, [])
    valid = parse_websocket_message(
        WireEnvelope(
            raw_message=(
                '{"channel":"activeSpotAssetCtx","data":{"coin":"@71","ctx":'
                '{"markPx":"1.0","midPx":null,"prevDayPx":"0.0",'
                '"dayNtlVlm":"0.0","circulatingSupply":"0.0"}}}'
            ),
            received_time=timer.now(),
            connection_id="interrupted-future-flush",
            connection_epoch=1,
            arrival_sequence=1,
        )
    )
    valid_context = next(
        record for record in valid.records if record.record_type == RecordType.MARKET_CONTEXT
    )
    completed: Future[tuple[ParsedRecord, ...]] = Future()
    completed.set_result((valid_context,))
    collector._rest_future = completed
    sink.batch_size = 1
    flush_calls = 0

    def interrupt_flush() -> FlushResult:
        nonlocal flush_calls
        flush_calls += 1
        raise KeyboardInterrupt("flush interrupted")

    monkeypatch.setattr(sink, "flush", interrupt_flush)

    with pytest.raises(KeyboardInterrupt, match="flush interrupted"):
        collector.close()

    assert flush_calls == 1
    assert sink._closed is True
    assert rest.close_calls == 1
    collector.close()
    assert flush_calls == 1
    assert rest.close_calls == 1


@pytest.mark.parametrize(
    ("asset", "interval", "expected_key"),
    [
        ("BTC", "1m", "candle:BTC:1m"),
        ("ETH", "1m", "candle:ETH:1m"),
        ("BTC", "5m", "candle:BTC:5m"),
        ("ETH", "5m", "candle:ETH:5m"),
    ],
)
def test_candle_subscription_key_is_canonical(
    asset: str,
    interval: str,
    expected_key: str,
) -> None:
    subscription = PublicSubscription(channel="candle", coin=asset, interval=interval)

    assert subscription.key == expected_key
    assert PublicCollector._subscription_key(subscription.payload()) == expected_key



def test_subscription_key_rejects_ambiguous_components() -> None:
    with pytest.raises(ValueError, match="stream key separator"):
        PublicSubscription(channel="candle", coin="BTC:1m", interval="1m")

    with pytest.raises(ValueError, match="unsupported candle interval"):
        PublicSubscription(channel="candle", coin="BTC", interval="1m:BTC")


def test_multi_asset_multi_interval_candle_staleness_uses_canonical_keys(tmp_path: Path) -> None:
    timer = ControlledTime()
    config = _config(assets=("BTC", "ETH"), candle_intervals=("1m", "5m"))
    collector, _rest, _factory, _sink = _collector(tmp_path, config, timer, [])
    expected_keys = {
        "candle:BTC:1m",
        "candle:BTC:5m",
        "candle:ETH:1m",
        "candle:ETH:5m",
    }

    try:
        sequence = 0
        for asset in config.assets:
            for interval in config.candle_intervals:
                sequence += 1
                collector._handle_message(
                    _candle_message(asset, interval),
                    timer.now(),
                    "canonical-candles",
                    1,
                    sequence,
                )
        collector.metrics.last_funding_by_asset = {asset: timer.now() for asset in config.assets}

        assert set(collector.metrics.last_event_by_channel) == expected_keys

        collector._update_staleness(timer.now() + timedelta(seconds=81))

        assert collector.metrics.stale_channels == ("candle:BTC:1m", "candle:ETH:1m")
    finally:
        collector.metrics.last_event_by_channel = {
            key: value
            for key, value in collector.metrics.last_event_by_channel.items()
            if key in expected_keys
        }
        collector.close()


def test_duration_stop_and_cleanup_are_normal_with_multiple_candle_streams(tmp_path: Path) -> None:
    timer = ControlledTime()
    config = _config(
        assets=("BTC", "ETH"),
        candle_intervals=("1m", "5m"),
        heartbeat_interval_seconds=100.0,
        pong_timeout_seconds=200.0,
        stale_after_seconds=20.0,
    )
    messages = [
        *_ack_messages(config),
        *[
            (_candle_message(asset, interval), 0.0)
            for asset in config.assets
            for interval in config.candle_intervals
        ],
        (None, 3.0),
    ]
    socket = FakeSocket(timer, messages)
    collector, rest, _factory, sink = _collector(tmp_path, config, timer, [socket])

    try:
        metrics = collector.run(duration_seconds=2.5)

        assert metrics.state == CollectorState.STOPPED
        assert metrics.messages_received == config.subscription_count + 4
        assert metrics.stale_channels == ()
        assert socket.closed is True
        collector.close()
        collector.close()
        assert sink._closed is True
        assert rest.close_calls == 1
    finally:
        if not collector._closed:
            collector.metrics.last_event_by_channel = {
                key: value
                for key, value in collector.metrics.last_event_by_channel.items()
                if not key.startswith("candle:") or key.count(":") == 2
            }
            collector.close()
