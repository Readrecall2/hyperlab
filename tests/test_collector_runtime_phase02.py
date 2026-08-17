from __future__ import annotations

import hashlib
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
from hyperlab.collector.storage import (
    BatchingLakeSink,
    CoordinatedWriterError,
    FlushResult,
)
from hyperlab.collector.websocket import (
    ReceivedWireMessage,
    WebsocketConsumerBackpressure,
    WebsocketQueueOverflow,
)
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


class SyntheticFullUniverseRest(FakeRest):
    '''Production-shaped synthetic bootstrap; never economic or live evidence.'''

    BTC_SOURCE_INDEX = 21
    ETH_SOURCE_INDEX = 44

    def __init__(self, *, misalign_unrelated_perp: bool = False) -> None:
        super().__init__()
        self.misalign_unrelated_perp = misalign_unrelated_perp

    def bootstrap(self, *, observed_at_ms: int | None = None) -> PublicBootstrap:
        self.bootstrap_calls += 1
        observed_at_ms = observed_at_ms or int(BASE_TIME.timestamp() * 1_000)
        unrelated = [
            {
                'name': f'SYNTHETIC_UNRELATED_{index:03d}',
                'szDecimals': index % 6,
                'maxLeverage': 3 + index % 48,
                'marginTableId': index,
            }
            for index in range(64)
        ]
        perp_universe = [
            *unrelated[: self.BTC_SOURCE_INDEX],
            {
                'name': 'BTC',
                'szDecimals': 5,
                'maxLeverage': 50,
                'marginTableId': 20,
            },
            *unrelated[self.BTC_SOURCE_INDEX : self.ETH_SOURCE_INDEX - 1],
            {
                'name': 'ETH',
                'szDecimals': 4,
                'maxLeverage': 50,
                'marginTableId': 20,
            },
            *unrelated[self.ETH_SOURCE_INDEX - 1 :],
        ]
        perp_contexts = [
            {
                'dayBaseVlm': str(index + 1),
                'dayNtlVlm': str((index + 1) * 1_000),
                'funding': '0.00001',
                'markPx': str(10_000 + index),
                'midPx': str(10_000.5 + index),
                'openInterest': '2',
                'oraclePx': str(10_001 + index),
                'prevDayPx': str(9_900 + index),
            }
            for index in range(len(perp_universe))
        ]
        if self.misalign_unrelated_perp:
            perp_contexts.pop()

        spot_tokens = [
            {
                'name': 'USDC',
                'szDecimals': 8,
                'weiDecimals': 8,
                'index': 0,
            },
            {
                'name': 'BTC',
                'szDecimals': 5,
                'weiDecimals': 8,
                'index': 77,
                'fullName': 'Bitcoin',
            },
            *[
                {
                    'name': f'SYNTHETIC_SPOT_{index:03d}',
                    'szDecimals': index % 6,
                    'weiDecimals': 8,
                    'index': 1_000 + index,
                }
                for index in range(16)
            ],
        ]
        spot_universe = [
            {
                'name': 'BTC/USDC',
                'tokens': [77, 0],
                'index': 107,
                'isCanonical': True,
            },
            *[
                {
                    'name': f'SYNTHETIC_SPOT_{index:03d}/USDC',
                    'tokens': [1_000 + index, 0],
                    'index': 1_000 + index,
                    'isCanonical': True,
                }
                for index in range(16)
            ],
        ]
        spot_contexts = [
            {
                'coin': '@' + str(pair['index']),
                'dayNtlVlm': str((index + 1) * 100),
                'markPx': str(1_000 + index),
                'midPx': str(1_000.5 + index),
                'prevDayPx': str(990 + index),
                'circulatingSupply': str(1_000_000 + index),
            }
            for index, pair in enumerate(spot_universe)
        ]
        return PublicBootstrap(
            observed_at_ms=observed_at_ms,
            perp_payload=[{'universe': perp_universe}, perp_contexts],
            spot_payload=[
                {'tokens': spot_tokens, 'universe': spot_universe},
                spot_contexts,
            ],
        )


SocketItem = tuple[str | ReceivedWireMessage | BaseException | None, float]


class FakeSocket:
    def __init__(
        self,
        timer: ControlledTime,
        items: list[SocketItem],
        *,
        telemetry_queue_depth: int = 0,
        telemetry_oldest_age_ms: float | None = None,
        telemetry_latest_age_ms: float | None = None,
    ) -> None:
        self.timer = timer
        self.connected_at = timer.now()
        self.items = list(items)
        self.sent: list[dict[str, object]] = []
        self.closed = False
        self.telemetry_queue_depth = telemetry_queue_depth
        self.telemetry_oldest_age_ms = telemetry_oldest_age_ms
        self.telemetry_latest_age_ms = telemetry_latest_age_ms
        self.terminal_error: BaseException | None = None

    def send_json(self, payload: dict[str, object]) -> None:
        self.sent.append(payload)

    def receive(self, timeout_seconds: float) -> str | ReceivedWireMessage | None:
        assert timeout_seconds == 1.0
        if not self.items:
            raise AssertionError("fake socket script exhausted")
        item, advance_seconds = self.items.pop(0)
        self.timer.advance(advance_seconds)
        if isinstance(item, BaseException):
            self.terminal_error = item
            raise item
        return item

    def telemetry_snapshot(self) -> dict[str, object]:
        error = self.terminal_error
        detail = None if error is None else str(error).strip()
        return {
            "reader_alive": not self.closed,
            "closed": self.closed,
            "queue_capacity": 2_000,
            "queue_depth": self.telemetry_queue_depth,
            "queue_high_water": self.telemetry_queue_depth,
            "oldest_message_age_ms": self.telemetry_oldest_age_ms,
            "latest_message_received_age_ms": self.telemetry_latest_age_ms,
            "terminal_exception_type": None if error is None else type(error).__name__,
            "terminal_reason": (
                None
                if error is None
                else type(error).__name__
                if not detail
                else f"{type(error).__name__}: {detail}"
            ),
        }

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


class AsyncFlushRequestRecordingSink(StateRecordingSink):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.flush_requests = 0

    def collect_completed(self) -> FlushResult:
        return FlushResult((), 0, 0)

    def request_flush(self) -> bool:
        self.flush_requests += 1
        return True


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
    rest: FakeRest | None = None,
    sleeper: Callable[[float], None] = lambda _delay: None,
) -> tuple[PublicCollector, FakeRest, FakeSocketFactory, StateRecordingSink]:
    resolved_rest = FakeRest() if rest is None else rest
    factory = FakeSocketFactory(sockets)
    sink = StateRecordingSink(tmp_path / "lake")
    connection_ids = iter(("first", "second", "third"))
    collector = PublicCollector(
        config,
        rest=resolved_rest,
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
    return collector, resolved_rest, factory, sink


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


def test_synthetic_non_economic_full_universe_bootstrap_persists_only_requested_assets(
    tmp_path: Path,
) -> None:
    timer = ControlledTime()
    config = _config(assets=('BTC', 'ETH'))
    socket = FakeSocket(timer, _ack_messages(config))
    rest = SyntheticFullUniverseRest()
    collector, _rest, _factory, sink = _collector(
        tmp_path,
        config,
        timer,
        [socket],
        rest=rest,
    )

    metrics = collector.run(max_messages=config.subscription_count)

    assert metrics.state == CollectorState.STOPPED
    assert rest.bootstrap_calls == 1
    metadata_rows = _parquet_rows(sink.root, 'instrument_metadata')
    context_rows = _parquet_rows(sink.root, 'market_context')
    assert {str(row['asset']) for row in metadata_rows} == {'BTC', 'ETH'}
    assert {str(row['asset']) for row in context_rows} == {'BTC', 'ETH'}
    assert len(metadata_rows) == len(context_rows) == 2

    metadata_by_asset = {str(row['asset']): row for row in metadata_rows}
    assert {
        asset: row['source_index'] for asset, row in metadata_by_asset.items()
    } == {
        'BTC': SyntheticFullUniverseRest.BTC_SOURCE_INDEX,
        'ETH': SyntheticFullUniverseRest.ETH_SOURCE_INDEX,
    }
    assert {
        asset: json.loads(str(row['metadata_json']))['name']
        for asset, row in metadata_by_asset.items()
    } == {'BTC': 'BTC', 'ETH': 'ETH'}
    for row in metadata_rows:
        metadata_json = str(row['metadata_json'])
        assert row['metadata_sha256'] == hashlib.sha256(metadata_json.encode()).hexdigest()
        assert row['connection_id'] == 'ws-1-first'
        assert row['source_sequence'] is None
    assert {str(row['observation_id']) for row in context_rows} == {
        'ws-1-first:1:1'
    }
    assert all(row['source_sequence'] is None for row in context_rows)

    status = json.loads((tmp_path / 'runtime_status.json').read_text(encoding='utf-8'))
    rest_status = status['observability']['worker_phases']['rest']
    assert rest_status['last_bootstrap_rows'] == 16
    assert rest_status['bootstrap_rows_total'] == 16


def test_bootstrap_scope_preserves_exact_encoded_spot_identity(tmp_path: Path) -> None:
    timer = ControlledTime()
    rest = SyntheticFullUniverseRest()
    collector, _rest, _factory, _sink = _collector(
        tmp_path,
        _config(assets=('@107',)),
        timer,
        [],
        rest=rest,
    )
    try:
        records = tuple(
            collector._iter_rest_records(
                connection_id='spot-bootstrap',
                connection_epoch=7,
                history_hours=1,
                include_l2=False,
                query_end=BASE_TIME,
            )
        )
    finally:
        collector.close()

    bootstrap_records = tuple(
        record
        for record in records
        if record.record_type in {RecordType.INSTRUMENT_METADATA, RecordType.MARKET_CONTEXT}
    )
    assert [(record.record_type, record.asset) for record in bootstrap_records] == [
        (RecordType.INSTRUMENT_METADATA, '@107'),
        (RecordType.MARKET_CONTEXT, '@107'),
    ]
    metadata, context = bootstrap_records
    assert metadata.row['source_symbol'] == '@107'
    assert metadata.row['source_index'] == 107
    assert metadata.row['base_token'] == 'BTC'
    assert metadata.row['quote_token'] == 'USDC'
    metadata_json = str(metadata.row['metadata_json'])
    assert metadata.row['metadata_sha256'] == hashlib.sha256(metadata_json.encode()).hexdigest()
    assert context.row['instrument_kind'] == 'spot'
    assert context.row['observation_id'] == 'spot-bootstrap:7:1'


def test_unrequested_malformed_bootstrap_entry_still_fails_closed(tmp_path: Path) -> None:
    timer = ControlledTime()
    rest = SyntheticFullUniverseRest(misalign_unrelated_perp=True)
    collector, _rest, _factory, _sink = _collector(
        tmp_path,
        _config(assets=('BTC',)),
        timer,
        [],
        rest=rest,
    )
    try:
        with pytest.raises(ValueError, match='perp metadata and contexts are not aligned'):
            tuple(
                collector._iter_rest_records(
                    connection_id='malformed-bootstrap',
                    connection_epoch=1,
                    history_hours=1,
                    include_l2=False,
                    query_end=BASE_TIME,
                )
            )
        assert rest.funding_calls == []
        assert rest.candle_calls == []
        assert rest.l2_calls == []
    finally:
        collector.close()


def test_rest_bootstrap_precedes_ws_acks_and_live_flush(tmp_path: Path) -> None:
    timer = ControlledTime()
    config = _config()
    socket = FakeSocket(timer, _ack_messages(config))
    socket.connected_at = BASE_TIME - timedelta(milliseconds=1)
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
    assert str(bbo_rows[0]["update_id"]).startswith("rest:")
    l2_headers = _parquet_rows(sink.root, "l2_book_state")
    l2_levels = _parquet_rows(sink.root, "l2_snapshot")
    assert len(l2_headers) == 1
    assert len(l2_levels) == 2
    rest_snapshot_id = str(l2_headers[0]["snapshot_id"])
    assert rest_snapshot_id.startswith(f"rest:{l2_headers[0]['connection_id']}:1:")
    assert {str(row["snapshot_id"]) for row in l2_levels} == {rest_snapshot_id}
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
    assert connection_events[0]["received_time"] == socket.connected_at
    _assert_all_source_sequences_are_null(sink.root)

    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    assert status["mode"] == "readonly"
    assert status["orders_enabled"] is False
    assert status["metrics"]["state"] == "stopped"
    observability = status["observability"]
    assert "socket" not in observability
    assert observability["generation_reason_history"] == {
        "capacity": 32,
        "seen": 0,
        "retained": 0,
        "truncated": 0,
    }
    closed_socket = observability["last_closed_socket"]
    assert closed_socket["generation"] == 1
    assert closed_socket["connection_id"] == "ws-1-first"
    assert closed_socket["telemetry_before_close"]["closed"] is False
    assert closed_socket["telemetry_after_close"]["closed"] is True
    assert "queue_high_water" in closed_socket["telemetry_before_close"]


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
    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    observability = status["observability"]
    assert "process_cpu" in observability["process"]
    failures = observability["reconnect_reasons_by_generation"]
    assert len(failures) == 1
    assert failures[0]["generation"] == 1
    assert failures[0]["connected"] is True
    assert failures[0]["reason"] == "ConnectionError: wire cut"
    assert failures[0]["socket"]["terminal_exception_type"] == "ConnectionError"
    assert failures[0]["will_reconnect"] is True
    failure_snapshot = failures[0]["failure_snapshot"]
    assert failure_snapshot["collector"]["state"] == "live"
    assert failure_snapshot["collector"]["connection_alive"] is True
    assert "process_cpu" in failure_snapshot["process"]
    assert failure_snapshot["writer"]["queue"]["capacity_rows"] == config.queue_capacity
    failure_liveness = failure_snapshot["liveness"]
    assert failure_liveness["live"] is True
    assert failure_liveness["connection_age_ms"] == 0.0
    assert failure_liveness["live_duration_ms"] == 0.0
    assert failure_liveness["pending_ack_count"] == 0
    assert failure_liveness["pending_ack_subscriptions"] == []
    assert failure_liveness["ping"] == {
        "deadline_baseline_age_ms": 0.0,
        "last_sent_age_ms": None,
    }
    assert failure_liveness["pong"] == {
        "deadline_baseline_age_ms": 0.0,
        "last_received_age_ms": None,
    }
    assert observability["generation_reason_history"] == {
        "capacity": 32,
        "seen": 1,
        "retained": 1,
        "truncated": 0,
    }
    assert observability["last_closed_socket"]["generation"] == 2
    worker_phases = observability["worker_phases"]
    assert worker_phases["normalization_ms"]["count"] > 0
    assert worker_phases["sink_enqueue_ms"]["count"] > 0


def test_stop_requested_interrupt_after_connect_persists_clean_disconnect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timer = ControlledTime()
    config = _config()
    socket = FakeSocket(timer, _ack_messages(config))
    collector, _rest, factory, sink = _collector(tmp_path, config, timer, [socket])
    scripted_receive = socket.receive

    def stop_then_interrupt(
        timeout_seconds: float,
    ) -> str | ReceivedWireMessage | None:
        if socket.items:
            return scripted_receive(timeout_seconds)
        collector.stop()
        raise InterruptedError("collector stop requested")

    monkeypatch.setattr(socket, "receive", stop_then_interrupt)
    try:
        metrics = collector.run()
    finally:
        collector.close()

    assert metrics.state == CollectorState.STOPPED
    assert (metrics.connections, metrics.reconnects, metrics.gaps) == (1, 0, 0)
    assert metrics.last_failure is None
    assert factory.connect_calls == [("mainnet", config.ws_connect_timeout_seconds)]
    assert socket.closed is True
    events = _parquet_rows(sink.root, "connection_event")
    assert [row["event_kind"] for row in events] == ["connect", "disconnect"]
    assert events[-1]["reason"] == "collector stop requested or bounded run completed"
    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    assert status["observability"]["generation_reason_history"]["seen"] == 0


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
        "l2Book:BTC",
    )
    assert socket.closed is True
    reasons = [str(row["reason"]) for row in _parquet_rows(sink.root, "connection_event")]
    assert any("stale critical public streams" in reason for reason in reasons)
    assert any(reason.startswith("coverage_unknown:") for reason in reasons)
    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    assert status["ok"] is False
    assert "stale critical public streams" in status["metrics"]["last_error"]


def test_event_driven_bbo_and_trade_silence_does_not_disconnect_healthy_state_streams(
    tmp_path: Path,
) -> None:
    timer = ControlledTime()
    config = _config(
        heartbeat_interval_seconds=100.0,
        pong_timeout_seconds=200.0,
        stale_after_seconds=2.0,
    )
    fixture_root = Path(__file__).parent / "fixtures" / "hyperliquid"
    active_context = (fixture_root / "ws_active_asset_ctx.json").read_text(encoding="utf-8")
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

    assert (metrics.connections, metrics.reconnects, metrics.gaps) == (1, 0, 0)
    assert metrics.stale_channels == ()
    assert socket.closed is True
    reasons = [str(row["reason"]) for row in _parquet_rows(sink.root, "connection_event")]
    assert not any("stale critical public streams" in reason for reason in reasons)
    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    ingest_age = status["metrics"]["channel_ingest_age_seconds"]
    assert ingest_age["bbo:BTC"] > config.stale_after_seconds
    assert ingest_age["trades:BTC"] > config.stale_after_seconds
    assert ingest_age["activeAssetCtx:BTC"] <= config.stale_after_seconds
    assert ingest_age["l2Book:BTC"] <= config.stale_after_seconds
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


def test_disk_full_flush_is_terminal_and_close_does_not_retry_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timer = ControlledTime()
    collector, _rest, _factory, sink = _collector(tmp_path, _config(), timer, [])
    attempts = 0

    def fail_disk_full() -> FlushResult:
        nonlocal attempts
        attempts += 1
        raise OSError("disk full")

    monkeypatch.setattr(sink, "flush", fail_disk_full)

    with pytest.raises(OSError, match="disk full"):
        collector._flush()
    collector.close()

    assert attempts == 1
    assert sink._closed is True
    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    assert status["error"] == "OSError: disk full"


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


def test_fresh_unrelated_reader_backlog_does_not_defer_stale_disconnect(
    tmp_path: Path,
) -> None:
    timer = ControlledTime()
    config = _config(
        heartbeat_interval_seconds=10.0,
        pong_timeout_seconds=20.0,
        stale_after_seconds=2.0,
    )
    socket = FakeSocket(
        timer,
        [*_ack_messages(config), (None, 3.0)],
        telemetry_queue_depth=2,
        telemetry_oldest_age_ms=1_000.0,
        telemetry_latest_age_ms=100.0,
    )
    collector, _rest, factory, _sink = _collector(tmp_path, config, timer, [socket])

    metrics = collector.run(duration_seconds=2.5)

    assert metrics.connections == 1
    assert metrics.reconnects == 0
    assert metrics.gaps == 1
    assert factory.connect_calls == [("mainnet", config.ws_connect_timeout_seconds)]
    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    assert status["ok"] is False
    assert status["observability"]["liveness"] == {
        "fresh_backlog_deferrals": 0,
        "fresh_backlog_active": False,
        "wire_monotonic_legacy_fallbacks": 0,
        "wire_monotonic_domain_fallbacks": 0,
    }
    failure = status["observability"]["reconnect_reasons_by_generation"][0]
    assert "stale critical public streams" in failure["reason"]


def test_aged_reader_backlog_is_fatal_local_capacity_without_reconnect(
    tmp_path: Path,
) -> None:
    timer = ControlledTime()
    config = _config(
        heartbeat_interval_seconds=10.0,
        pong_timeout_seconds=20.0,
        stale_after_seconds=2.0,
    )
    socket = FakeSocket(
        timer,
        [*_ack_messages(config), (None, 3.0)],
        telemetry_queue_depth=2,
        telemetry_oldest_age_ms=2_500.0,
        telemetry_latest_age_ms=100.0,
    )
    collector, _rest, factory, _sink = _collector(tmp_path, config, timer, [socket])

    try:
        with pytest.raises(
            WebsocketConsumerBackpressure,
            match="local consumer capacity exhausted",
        ):
            collector.run(duration_seconds=30.0)
    finally:
        collector.close()

    assert collector.metrics.connections == 1
    assert collector.metrics.reconnects == 0
    assert collector.metrics.gaps == 1
    assert factory.connect_calls == [("mainnet", config.ws_connect_timeout_seconds)]
    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    failure = status["observability"]["reconnect_reasons_by_generation"][0]
    assert failure["will_reconnect"] is False
    assert failure["reason"].startswith("WebsocketConsumerBackpressure:")


def test_terminal_websocket_overflow_is_fatal_without_reconnect(tmp_path: Path) -> None:
    timer = ControlledTime()
    config = _config()
    socket = FakeSocket(
        timer,
        [
            *_ack_messages(config),
            (
                WebsocketQueueOverflow("bounded public websocket queue is full; local capacity exhausted"),
                0.0,
            ),
        ],
        telemetry_queue_depth=config.queue_capacity,
        telemetry_oldest_age_ms=1.0,
        telemetry_latest_age_ms=0.0,
    )
    collector, _rest, factory, _sink = _collector(tmp_path, config, timer, [socket])

    try:
        with pytest.raises(WebsocketQueueOverflow, match="local capacity exhausted"):
            collector.run(duration_seconds=30.0)
    finally:
        collector.close()

    assert collector.metrics.connections == 1
    assert collector.metrics.reconnects == 0
    assert factory.connect_calls == [("mainnet", config.ws_connect_timeout_seconds)]
    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    observability = status["observability"]
    failure = observability["reconnect_reasons_by_generation"][0]
    assert failure["will_reconnect"] is False
    assert failure["socket"]["terminal_exception_type"] == "WebsocketQueueOverflow"
    assert (
        observability["last_closed_socket"]["telemetry_before_close"]["terminal_exception_type"]
        == "WebsocketQueueOverflow"
    )


def test_generation_reason_history_reports_truncation(tmp_path: Path) -> None:
    timer = ControlledTime()
    collector, _rest, _factory, _sink = _collector(tmp_path, _config(), timer, [])
    try:
        for generation in range(1, 36):
            collector._record_generation_failure(
                socket=None,
                connection_id=f"connection-{generation}",
                generation=generation,
                connected=True,
                error=ConnectionError(f"failure-{generation}"),
                will_reconnect=True,
            )
        collector._publish_status()
        status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
        observability = status["observability"]
        assert observability["generation_reason_history"] == {
            "capacity": 32,
            "seen": 35,
            "retained": 32,
            "truncated": 3,
        }
        retained = observability["reconnect_reasons_by_generation"]
        assert retained[0]["generation"] == 4
        assert retained[-1]["generation"] == 35
    finally:
        collector.close()


def test_pong_liveness_uses_wire_receive_monotonic_not_consumer_time(
    tmp_path: Path,
) -> None:
    timer = ControlledTime()
    config = _config(
        heartbeat_interval_seconds=2.0,
        pong_timeout_seconds=5.0,
        stale_after_seconds=1_000.0,
    )
    socket = FakeSocket(
        timer,
        [
            *_ack_messages(config),
            (
                ReceivedWireMessage(
                    '{"channel":"pong"}',
                    BASE_TIME,
                    received_monotonic_ns=0,
                ),
                6.0,
            ),
        ],
        telemetry_queue_depth=3,
        telemetry_oldest_age_ms=1_000.0,
        telemetry_latest_age_ms=50.0,
    )
    collector, _rest, factory, _sink = _collector(tmp_path, config, timer, [socket])

    metrics = collector.run(duration_seconds=5.5)

    assert metrics.pongs_received == 1
    assert metrics.gaps == 1
    assert metrics.reconnects == 0
    assert factory.connect_calls == [("mainnet", config.ws_connect_timeout_seconds)]
    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    failure = status["observability"]["reconnect_reasons_by_generation"][0]
    assert "Hyperliquid pong deadline exceeded" in failure["reason"]


def test_wire_receive_monotonic_has_safe_legacy_and_foreign_domain_fallbacks(
    tmp_path: Path,
) -> None:
    timer = ControlledTime(elapsed=6.0)
    collector, _rest, _factory, _sink = _collector(tmp_path, _config(), timer, [])
    try:
        legacy = ReceivedWireMessage('{"channel":"pong"}', BASE_TIME)
        assert collector._wire_received_monotonic_seconds(
            legacy,
            observed_monotonic=timer.monotonic(),
            observed_at=timer.now(),
        ) == pytest.approx(0.0)

        foreign_domain = ReceivedWireMessage(
            '{"channel":"pong"}',
            timer.now(),
            received_monotonic_ns=1_000_000_000_000,
        )
        assert collector._wire_received_monotonic_seconds(
            foreign_domain,
            observed_monotonic=timer.monotonic(),
            observed_at=timer.now(),
        ) == pytest.approx(6.0)
        assert collector._wire_monotonic_legacy_fallbacks == 1
        assert collector._wire_monotonic_domain_fallbacks == 1
    finally:
        collector.close()


@pytest.mark.parametrize(
    ("oldest_age_ms", "expect_fatal"),
    (
        (1_000.0, False),
        (2_500.0, True),
    ),
)
def test_subscription_ack_timeout_distinguishes_fresh_and_aged_backlog(
    tmp_path: Path,
    oldest_age_ms: float,
    expect_fatal: bool,
) -> None:
    timer = ControlledTime()
    config = _config(
        heartbeat_interval_seconds=1.0,
        pong_timeout_seconds=2.0,
        stale_after_seconds=1_000.0,
    )
    socket = FakeSocket(
        timer,
        [
            *_ack_messages(config)[:-1],
            (
                ReceivedWireMessage(
                    '{"channel":"pong"}',
                    BASE_TIME + timedelta(seconds=3),
                    received_monotonic_ns=3_000_000_000,
                ),
                3.0,
            ),
        ],
        telemetry_queue_depth=1,
        telemetry_oldest_age_ms=oldest_age_ms,
        telemetry_latest_age_ms=100.0,
    )
    collector, _rest, factory, _sink = _collector(tmp_path, config, timer, [socket])

    try:
        if expect_fatal:
            with pytest.raises(
                WebsocketConsumerBackpressure,
                match="subscription acknowledgement deadline",
            ):
                collector.run(duration_seconds=2.5)
        else:
            metrics = collector.run(duration_seconds=2.5)
            assert metrics.gaps == 1
            assert collector._backlog_liveness_deferrals == 0
            status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
            failure = status["observability"]["reconnect_reasons_by_generation"][0]
            assert "subscription acknowledgements missing" in failure["reason"]
    finally:
        collector.close()

    assert collector.metrics.reconnects == 0
    assert factory.connect_calls == [("mainnet", config.ws_connect_timeout_seconds)]
    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    failure = status["observability"]["reconnect_reasons_by_generation"][0]
    failure_liveness = failure["failure_snapshot"]["liveness"]
    assert failure_liveness["live"] is False
    assert failure_liveness["live_duration_ms"] is None
    assert failure_liveness["pending_ack_count"] == 1
    assert failure_liveness["pending_ack_subscriptions"] == [
        config.subscriptions()[-1].key
    ]
    if expect_fatal:
        assert failure["will_reconnect"] is False


@pytest.mark.parametrize("method", (None, "unsubscribe"))
def test_non_subscribe_subscription_response_remains_pending_until_ack_timeout(
    tmp_path: Path,
    method: str | None,
) -> None:
    timer = ControlledTime()
    config = _config(
        heartbeat_interval_seconds=1.0,
        pong_timeout_seconds=2.0,
        stale_after_seconds=1_000.0,
    )
    subscriptions = config.subscriptions()
    rejected = subscriptions[-1]
    response_data: dict[str, object] = {"subscription": rejected.payload()}
    if method is not None:
        response_data["method"] = method
    non_subscribe_response = json.dumps(
        {"channel": "subscriptionResponse", "data": response_data},
        separators=(",", ":"),
    )
    socket = FakeSocket(
        timer,
        [
            *_ack_messages(config)[:-1],
            (non_subscribe_response, 0.0),
            (
                ReceivedWireMessage(
                    '{"channel":"pong"}',
                    BASE_TIME + timedelta(seconds=3),
                    received_monotonic_ns=3_000_000_000,
                ),
                3.0,
            ),
        ],
    )
    collector, _rest, _factory, _sink = _collector(tmp_path, config, timer, [socket])

    try:
        metrics = collector.run(duration_seconds=2.5)
        assert (metrics.connections, metrics.reconnects, metrics.gaps) == (1, 0, 1)
        assert metrics.subscription_acks == config.subscription_count - 1
        status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
        failure = status["observability"]["reconnect_reasons_by_generation"][0]
        assert "subscription acknowledgements missing" in failure["reason"]
        assert rejected.key in failure["reason"]
    finally:
        collector.close()


def test_duration_expiry_during_connect_creates_no_spurious_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timer = ControlledTime()
    config = _config()
    socket = FakeSocket(timer, [])
    collector, _rest, factory, sink = _collector(tmp_path, config, timer, [socket])
    original_connect = factory.connect

    def connect_after_deadline(network: str, timeout_seconds: float) -> FakeSocket:
        connected = original_connect(network, timeout_seconds)
        timer.advance(2.0)
        return connected

    monkeypatch.setattr(factory, "connect", connect_after_deadline)

    metrics = collector.run(duration_seconds=1.0)

    assert metrics.connections == 0
    assert metrics.connection_epoch == 0
    assert socket.sent == []
    assert socket.closed is True
    assert _parquet_rows(sink.root, "connection_event") == []


def test_writer_failure_records_generation_reason_without_reconnect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timer = ControlledTime()
    config = _config()
    socket = FakeSocket(timer, [])
    collector, _rest, factory, _sink = _collector(tmp_path, config, timer, [socket])

    def fail_connection_event(*_args: object, **_kwargs: object) -> None:
        raise CoordinatedWriterError("simulated coordinated writer failure")

    monkeypatch.setattr(collector, "_add_connection_event", fail_connection_event)
    try:
        with pytest.raises(
            CoordinatedWriterError,
            match="simulated coordinated writer failure",
        ):
            collector.run(duration_seconds=30.0)
    finally:
        collector.close()

    assert collector.metrics.connections == 1
    assert collector.metrics.reconnects == 0
    assert factory.connect_calls == [("mainnet", config.ws_connect_timeout_seconds)]
    status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
    failure = status["observability"]["reconnect_reasons_by_generation"][0]
    assert failure["generation"] == 1
    assert failure["will_reconnect"] is False
    assert failure["reason"].startswith("CoordinatedWriterError:")


def test_rest_refresh_is_materialized_off_supervisor_and_applied_as_one_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timer = ControlledTime()
    collector, _rest, _factory, sink = _collector(tmp_path, _config(), timer, [])
    batch_sizes: list[int] = []
    original_add_many = sink.add_many

    def record_batch(records: Any) -> int:
        batch = tuple(records)
        batch_sizes.append(len(batch))
        return original_add_many(batch)

    monkeypatch.setattr(sink, "add_many", record_batch)
    try:
        collector._schedule_rest_refresh(
            "refresh-connection",
            1,
            timer.now(),
        )
        collector._drain_rest_refresh(wait=True)
        collector._publish_status()

        assert len(batch_sizes) == 1
        assert batch_sizes[0] > 1
        status = json.loads((tmp_path / "runtime_status.json").read_text(encoding="utf-8"))
        rest_metrics = status["observability"]["worker_phases"]["rest"]
        assert rest_metrics["refresh_worker_materialization_ms"]["count"] == 1
        assert rest_metrics["refresh_supervisor_apply_ms"]["count"] == 1
        assert rest_metrics["last_refresh_rows_materialized"] == batch_sizes[0]
        assert rest_metrics["last_refresh_rows_applied"] == batch_sizes[0]
        assert rest_metrics["refresh_rows_materialized_total"] == batch_sizes[0]
        assert rest_metrics["refresh_rows_applied_total"] == batch_sizes[0]
    finally:
        collector.close()


def test_periodic_flush_requests_async_durability_without_blocking_runtime(
    tmp_path: Path,
) -> None:
    timer = ControlledTime()
    config = _config(flush_interval_seconds=5.0)
    socket = FakeSocket(
        timer,
        [
            *_ack_messages(config),
            (_candle_message("BTC", "1m"), 6.0),
        ],
    )
    collector, _rest, _factory, original_sink = _collector(
        tmp_path,
        config,
        timer,
        [socket],
    )
    original_sink.close()
    sink = AsyncFlushRequestRecordingSink(tmp_path / "async-lake")
    collector.sink = sink
    sink.state_provider = lambda: collector.metrics.state
    try:
        metrics = collector.run(
            max_messages=config.subscription_count + 1,
        )
        assert metrics.state == CollectorState.STOPPED
        assert sink.flush_requests == 1
    finally:
        collector.close()

class _FailSecondFundingCallRest(FakeRest):
    def __init__(self) -> None:
        super().__init__()
        self.funding_attempts = 0

    def funding_history(
        self,
        asset: str,
        start_ms: int,
        end_ms: int | None = None,
    ) -> object:
        self.funding_attempts += 1
        if self.funding_attempts == 2:
            self.funding_calls.append((asset, start_ms, end_ms))
            raise ConnectionError("fixture refresh failure")
        return super().funding_history(asset, start_ms, end_ms)


class _MissingInitialFundingRest(FakeRest):
    def __init__(self) -> None:
        super().__init__()
        self.funding_attempts = 0

    def funding_history(
        self,
        asset: str,
        start_ms: int,
        end_ms: int | None = None,
    ) -> object:
        self.funding_attempts += 1
        if self.funding_attempts == 1:
            self.funding_calls.append((asset, start_ms, end_ms))
            return []
        return super().funding_history(asset, start_ms, end_ms)


def _paper_policy_config(**overrides: Any) -> CollectorConfig:
    values: dict[str, Any] = {
        "candle_intervals": (),
        "subscription_channels": ("bbo",),
        "collect_funding_history": True,
        "reconnect_on_rest_refresh_failure": True,
        "critical_funding_history": True,
        "rest_refresh_interval_seconds": 1.0,
        "heartbeat_interval_seconds": 100.0,
        "pong_timeout_seconds": 200.0,
        "stale_after_seconds": 100.0,
        "backoff_initial_seconds": 0.1,
        "backoff_max_seconds": 0.1,
        "backoff_jitter_ratio": 0.0,
    }
    values.update(overrides)
    return _config(**values)


def test_paper_rest_refresh_failure_forces_exact_reconnect_and_resync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timer = ControlledTime()
    config = _paper_policy_config()
    rest = _FailSecondFundingCallRest()
    first = FakeSocket(timer, [*_ack_messages(config), (None, 1.1)])
    second = FakeSocket(timer, _ack_messages(config))
    collector, _resolved_rest, factory, sink = _collector(
        tmp_path,
        config,
        timer,
        [first, second],
        rest=rest,
        sleeper=lambda _delay: None,
    )

    def materialize_synchronously(
        connection_id: str,
        connection_epoch: int,
        query_end: datetime,
    ) -> None:
        future: Future[tuple[ParsedRecord, ...]] = Future()
        try:
            future.set_result(
                collector._materialize_rest_refresh(
                    connection_id=f"sync-{connection_id}",
                    connection_epoch=connection_epoch,
                    history_hours=2,
                    query_end=query_end,
                )
            )
        except BaseException as exc:
            future.set_exception(exc)
        collector._rest_future = future

    monkeypatch.setattr(collector, "_schedule_rest_refresh", materialize_synchronously)
    try:
        metrics = collector.run(max_messages=2)
    finally:
        collector.close()

    assert rest.funding_attempts == 3
    assert (metrics.connections, metrics.reconnects, metrics.resyncs) == (2, 1, 1)
    assert metrics.gaps == 2
    assert metrics.last_error is None
    assert first.closed is True
    assert second.closed is True
    assert factory.connect_calls == [
        ("mainnet", config.ws_connect_timeout_seconds),
        ("mainnet", config.ws_connect_timeout_seconds),
    ]
    events = _parquet_rows(sink.root, "connection_event")
    assert {
        int(row["connection_epoch"])
        for row in events
        if row["event_kind"] == "resync_start"
    } == {2}
    assert {int(row["connection_epoch"]) for row in events if row["event_kind"] == "resync_complete"} == {2}
    assert any("REST refresh failed" in str(row["reason"]) for row in events)
    assert {int(row["connection_epoch"]) for row in events if row["event_kind"] == "connect"} == {
        1,
        2,
    }


def test_paper_missing_funding_is_critical_even_with_fresh_bbo_and_recovers_after_reconnect(
    tmp_path: Path,
) -> None:
    timer = ControlledTime()
    config = _paper_policy_config(rest_refresh_interval_seconds=300.0)
    rest = _MissingInitialFundingRest()
    fresh_bbo = (
        Path(__file__).parent / "fixtures" / "hyperliquid" / "ws_bbo.json"
    ).read_text(encoding="utf-8")
    first = FakeSocket(timer, [(fresh_bbo, 0.0), *_ack_messages(config)])
    second = FakeSocket(timer, _ack_messages(config))
    collector, _resolved_rest, factory, sink = _collector(
        tmp_path,
        config,
        timer,
        [first, second],
        rest=rest,
        sleeper=lambda _delay: None,
    )
    try:
        metrics = collector.run(max_messages=3)
    finally:
        collector.close()

    assert rest.funding_attempts == 2
    assert (metrics.connections, metrics.reconnects, metrics.resyncs) == (2, 1, 1)
    assert metrics.gaps == 1
    assert metrics.stale_channels == ()
    assert metrics.last_error is None
    assert factory.connect_calls == [
        ("mainnet", config.ws_connect_timeout_seconds),
        ("mainnet", config.ws_connect_timeout_seconds),
    ]
    events = _parquet_rows(sink.root, "connection_event")
    first_failure = next(
        row
        for row in events
        if row["event_kind"] == "disconnect"
        and "stale critical public streams" in str(row["reason"])
    )
    assert "fundingHistory:BTC" in str(first_failure["reason"])
    second_connect = next(
        row for row in events if row["event_kind"] == "connect" and row["connection_epoch"] == 2
    )
    assert second_connect["connection_id"] != first_failure["connection_id"]
