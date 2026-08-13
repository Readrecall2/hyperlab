from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from hyperlab.collector.models import ParsedMessage, ParsedRecord, WireEnvelope
from hyperlab.collector.storage import BatchingLakeSink, CoordinatedWriterError
from hyperlab.data.lake import inventory_partitions
from hyperlab.data.schema import RecordType
from hyperlab.venues.base import NormalizedInstrument, measure_clock
from hyperlab.venues.binance import (
    PUBLIC_GET_PATHS,
    BinancePublicConnector,
    BinancePublicRestClient,
    clock_record,
    funding_intervals,
    normalize_exchange_info,
    parse_funding_history,
)
from hyperlab.venues.replay import replay_synchronized
from hyperlab.venues.runtime import BinanceReferenceCollector, ReferenceCollectorConfig

BASE = datetime(2026, 8, 12, 12, tzinfo=UTC)


def _exchange_info() -> dict[str, object]:
    return {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "pair": "BTCUSDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                ],
            }
        ]
    }


def _connector() -> BinancePublicConnector:
    return BinancePublicConnector.from_exchange_info(_exchange_info(), ("BTC",))


def _envelope(payload: object, sequence: int = 1, received: datetime = BASE) -> WireEnvelope:
    return WireEnvelope(
        json.dumps(payload, separators=(",", ":")),
        received,
        "binance-fixture",
        1,
        sequence,
    )


def _combined(stream: str, data: dict[str, object]) -> dict[str, object]:
    return {"stream": stream, "data": data}


def test_exchange_info_normalizes_symbol_identity_and_base_sizes_explicitly() -> None:
    instruments = normalize_exchange_info(_exchange_info(), ("BTC",))
    btc = instruments["BTC"]

    assert btc.source_symbol == "BTCUSDT"
    assert btc.contract_kind == "linear"
    assert btc.quantity_multiplier == Decimal("1")
    assert btc.normalize_quantity("0.125") == Decimal("0.125")
    assert btc.price_tick == Decimal("0.10")
    assert btc.quantity_step == Decimal("0.001")

    inverse = _exchange_info()
    symbol = inverse["symbols"][0]  # type: ignore[index]
    symbol["symbol"] = "BTCUSD_PERP"  # type: ignore[index]
    symbol["quoteAsset"] = "USD"  # type: ignore[index]
    with pytest.raises(ValueError, match="no reviewed"):
        normalize_exchange_info(inverse, ("BTC",))


def test_binance_connector_collects_bbo_l2_trade_candle_and_funding_context() -> None:
    connector = _connector()
    frames = [
        _combined(
            "btcusdt@bookTicker",
            {
                "e": "bookTicker",
                "E": 1_786_492_800_010,
                "T": 1_786_492_800_009,
                "s": "BTCUSDT",
                "u": 41,
                "b": "60000.1",
                "B": "1.25",
                "a": "60000.2",
                "A": "2.5",
            },
        ),
        _combined(
            "btcusdt@depth20@100ms",
            {
                "e": "depthUpdate",
                "E": 1_786_492_800_015,
                "T": 1_786_492_800_014,
                "s": "BTCUSDT",
                "U": 40,
                "u": 43,
                "pu": 39,
                "b": [["60000.1", "1.25"], ["59999.9", "3.0"]],
                "a": [["60000.2", "2.5"], ["60000.4", "4.0"]],
            },
        ),
        _combined(
            "btcusdt@aggTrade",
            {
                "e": "aggTrade",
                "E": 1_786_492_800_020,
                "T": 1_786_492_800_019,
                "s": "BTCUSDT",
                "a": 42,
                "p": "60000.2",
                "q": "0.01",
                "m": False,
            },
        ),
        _combined(
            "btcusdt@kline_1m",
            {
                "e": "kline",
                "E": 1_786_492_800_030,
                "s": "BTCUSDT",
                "k": {
                    "t": 1_786_492_800_000,
                    "T": 1_786_492_859_999,
                    "i": "1m",
                    "L": 55,
                    "o": "60000",
                    "h": "60010",
                    "l": "59990",
                    "c": "60005",
                    "v": "12.5",
                    "q": "750000",
                    "n": 88,
                    "x": False,
                },
            },
        ),
        _combined(
            "btcusdt@markPrice@1s",
            {
                "e": "markPriceUpdate",
                "E": 1_786_492_800_040,
                "s": "BTCUSDT",
                "p": "60002",
                "i": "60001",
                "r": "0.0001",
                "T": 1_786_521_600_000,
            },
        ),
    ]
    parsed = [connector.parse_message(_envelope(frame, sequence=index)) for index, frame in enumerate(frames, 1)]
    normalized = [message.records[1] for message in parsed if "@depth20" not in str(message.channel)]
    depth = parsed[1]

    assert [record.record_type for record in normalized] == [
        RecordType.BBO,
        RecordType.TRADE,
        RecordType.CANDLE,
        RecordType.MARKET_CONTEXT,
    ]
    assert normalized[0].row["source_sequence"] == 41
    assert normalized[0].row["bid_quantity"] == Decimal("1.25")
    assert normalized[1].row["aggressor_side"] == "buy"
    assert normalized[2].row["is_final"] is False
    assert normalized[3].row["mark_price"] == Decimal("60002")
    assert normalized[3].row["oracle_price"] is None
    assert '"i":"60001"' in parsed[4].records[0].row["raw_message"]

    assert [record.record_type for record in depth.records] == [
        RecordType.WIRE_MESSAGE,
        RecordType.L2_BOOK_STATE,
        RecordType.L2_SNAPSHOT,
        RecordType.L2_SNAPSHOT,
        RecordType.L2_SNAPSHOT,
        RecordType.L2_SNAPSHOT,
    ]
    header = depth.records[1]
    levels = depth.records[2:]
    assert header.row["venue"] == "binance_usdm"
    assert header.row["received_time"] == BASE
    assert header.row["bid_level_count"] == 2
    assert header.row["ask_level_count"] == 2
    assert {record.row["last_sequence"] for record in levels} == {43}
    assert [(record.row["side"], record.row["level"]) for record in levels] == [
        ("bid", 0),
        ("bid", 1),
        ("ask", 0),
        ("ask", 1),
    ]
    assert levels[0].row["price"] == Decimal("60000.1")
    assert levels[0].row["quantity"] == Decimal("1.25")

def test_binance_websocket_url_includes_replayable_top_20_l2_for_every_asset() -> None:
    connector = BinancePublicConnector.from_exchange_info(
        {
            "symbols": [
                _exchange_info()["symbols"][0],  # type: ignore[index]
                {
                    **_exchange_info()["symbols"][0],  # type: ignore[index]
                    "symbol": "ETHUSDT",
                    "pair": "ETHUSDT",
                    "baseAsset": "ETH",
                },
            ]
        },
        ("BTC", "ETH"),
    )

    url = connector.websocket_url(("BTC", "ETH"), ("1m",))

    assert "btcusdt@depth20@100ms" in url
    assert "ethusdt@depth20@100ms" in url



def test_clock_midpoint_drift_latency_and_uncertainty_are_preserved(tmp_path: Path) -> None:
    measurement = measure_clock(
        "binance_usdm",
        request_sent_time=BASE,
        response_received_time=BASE + timedelta(milliseconds=40),
        server_time=BASE + timedelta(milliseconds=15),
    )

    assert measurement.round_trip_latency_ms == Decimal("40.0")
    assert measurement.estimated_clock_drift_ms == Decimal("5.0")
    assert measurement.drift_uncertainty_ms == Decimal("20.0")
    assert measurement.adjusted_one_way_latency_ms(
        BASE + timedelta(milliseconds=100), BASE + timedelta(milliseconds=125)
    ) == Decimal("30.0")
    record = clock_record(measurement, "clock-1")
    assert record.record_type == RecordType.CLOCK_SYNC
    sink = BatchingLakeSink(tmp_path, persistent_dedup=False)
    try:
        assert sink.add(record)
        result = sink.flush()
    finally:
        sink.close()
    assert result.row_count == 1


class FakeTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object], float]] = []

    def get_json(self, url: str, params: Mapping[str, object], timeout_seconds: float) -> Any:
        self.calls.append((url, dict(params), timeout_seconds))
        return {"serverTime": 1_786_492_800_000}


def test_rest_client_is_get_only_keyless_and_rejects_every_unlisted_path() -> None:
    transport = FakeTransport()
    client = BinancePublicRestClient(transport=transport)
    client.exchange_info()

    assert transport.calls[0][0].endswith("/fapi/v1/exchangeInfo")
    assert transport.calls[0][1] == {}
    assert all("order" not in path.lower() and "account" not in path.lower() for path in PUBLIC_GET_PATHS)
    with pytest.raises(ValueError, match="outside the public market-data allowlist"):
        client._get("/fapi/v1/order")


class KlinePagingTransport:
    def __init__(self) -> None:
        self.starts: list[int] = []

    def get_json(self, url: str, params: Mapping[str, object], timeout_seconds: float) -> Any:
        del url, timeout_seconds
        start = int(str(params["startTime"]))
        self.starts.append(start)
        page_size = 1500 if len(self.starts) == 1 else 1
        return [
            [start + index * 60_000, "1", "1", "1", "1", "1", start, "1", 1]
            for index in range(page_size)
        ]


def test_rest_kline_history_paginates_without_silent_truncation() -> None:
    transport = KlinePagingTransport()
    client = BinancePublicRestClient(transport=transport)
    response = client.klines("BTCUSDT", "1m", 1_000, 100_000_000)

    assert len(response.payload) == 1501
    assert transport.starts == [1_000, 1_000 + 1499 * 60_000 + 1]


def test_funding_schedule_is_explicit_and_missing_settlements_remain_detectable() -> None:
    normalized = _connector().instrument_for_asset("BTC")
    schedule = funding_intervals([{"symbol": "BTCUSDT", "fundingIntervalHours": 8}])
    records = parse_funding_history(
        [
            {"symbol": "BTCUSDT", "fundingTime": 1_786_492_800_000, "fundingRate": "0.0001"},
            {"symbol": "BTCUSDT", "fundingTime": 1_786_550_400_000, "fundingRate": "0.0002"},
        ],
        received_time=BASE,
        normalized=normalized,
        expected_interval_seconds=schedule["BTCUSDT"],
    )

    assert len(records) == 2
    assert {record.row["funding_interval_seconds"] for record in records} == {28_800}
    assert records[0].row["oracle_price"] is None
    assert records[1].row["event_time"] - records[0].row["event_time"] == timedelta(hours=16)


class StubConnector:
    def __init__(self, venue: str) -> None:
        self.venue = venue

    def websocket_url(self, assets: tuple[str, ...], candle_intervals: tuple[str, ...]) -> str:
        return "wss://invalid.example"

    def instrument_for_asset(self, asset: str) -> NormalizedInstrument:
        raise NotImplementedError

    def parse_message(self, envelope: WireEnvelope) -> ParsedMessage:
        root = json.loads(envelope.raw_message)
        event_time = datetime.fromisoformat(root["event_time"])
        return ParsedMessage(
            channel="bbo",
            records=(
                ParsedRecord(RecordType.BBO, "BTC", {"event_time": event_time}),
            ),
        )


def _replay_envelope(received: datetime, source: datetime, sequence: int) -> WireEnvelope:
    return _envelope({"event_time": source.isoformat()}, sequence, received)


def test_multi_venue_replay_detects_desync_absence_and_source_out_of_order() -> None:
    result = replay_synchronized(
        {"a": StubConnector("a"), "b": StubConnector("b")},
        {
            "a": [
                _replay_envelope(BASE, BASE, 1),
                _replay_envelope(BASE + timedelta(seconds=7), BASE - timedelta(milliseconds=1), 2),
            ],
            "b": [_replay_envelope(BASE + timedelta(milliseconds=400), BASE, 1)],
        },
        max_clock_skew=timedelta(milliseconds=250),
        venue_absent_after=timedelta(seconds=5),
    )

    kinds = {issue.kind for issue in result.issues}
    assert "venues_desynchronized" in kinds
    assert "venue_absent" in kinds
    assert "venue_absence_gap" in kinds
    assert "source_time_out_of_order" in kinds
    assert [event.venue for event in result.events] == ["a", "b", "a"]


def test_sink_partitions_external_records_by_their_actual_venue(tmp_path: Path) -> None:
    connector = _connector()
    frame = _combined(
        "btcusdt@bookTicker",
        {
            "e": "bookTicker",
            "E": 1_786_492_800_010,
            "T": 1_786_492_800_009,
            "s": "BTCUSDT",
            "u": 41,
            "b": "60000.1",
            "B": "1.25",
            "a": "60000.2",
            "A": "2.5",
        },
    )
    parsed = connector.parse_message(_envelope(frame))
    sink = BatchingLakeSink(tmp_path, persistent_dedup=False)
    try:
        for record in parsed.records:
            sink.add(record)
        result = sink.flush()
    finally:
        sink.close()

    assert {manifest.partition.venue for manifest in result.manifests} == {"binance_usdm"}
    data_paths = [tmp_path / manifest.relative_data_path for manifest in result.manifests]
    assert all("venue=binance_usdm" in str(path) for path in data_paths)
    assert sum(pq.ParquetFile(path).metadata.num_rows for path in data_paths) == 2


def _depth_frame(event_ms: int, *, last_sequence: int) -> dict[str, object]:
    return _combined(
        "btcusdt@depth20@100ms",
        {
            "e": "depthUpdate",
            "E": event_ms,
            "T": event_ms,
            "s": "BTCUSDT",
            "U": last_sequence,
            "u": last_sequence,
            "pu": last_sequence - 1,
            "b": [["60000.1", "1.25"]],
            "a": [["60000.2", "2.5"]],
        },
    )


def _reference_collector(tmp_path: Path, sink: BatchingLakeSink) -> BinanceReferenceCollector:
    return BinanceReferenceCollector(
        ReferenceCollectorConfig(
            assets=("BTC",),
            candle_intervals=("1m",),
            batch_size=100,
            queue_capacity=200,
        ),
        rest=BinancePublicRestClient(transport=FakeTransport()),
        sink=sink,
        runtime_status_path=tmp_path / "runtime-status.json",
        clock=lambda: BASE,
    )


def test_binance_complete_l2_snapshot_marks_each_connection_resync(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    sink = BatchingLakeSink(root, batch_size=100, queue_capacity=500)
    collector = _reference_collector(tmp_path, sink)
    connector = _connector()
    base_ms = int(BASE.timestamp() * 1_000)
    try:
        for epoch, offset_ms in ((1, 0), (2, 1_000)):
            connection_id = f"connection-{epoch}"
            received_time = BASE + timedelta(milliseconds=offset_ms + 25)
            envelope = WireEnvelope(
                json.dumps(
                    _depth_frame(base_ms + offset_ms, last_sequence=40 + epoch),
                    separators=(",", ":"),
                ),
                received_time,
                connection_id,
                epoch,
                1,
            )
            parsed = connector.parse_message(envelope)
            pending = {"BTC"}
            collector._record_l2_resync_if_needed(
                parsed,
                pending_assets=pending,
                connection_id=connection_id,
                connection_epoch=epoch,
            )
            assert pending == set()
            for record in parsed.records:
                collector._add(record)
    finally:
        collector.close()

    report = inventory_partitions(root)
    assert not [
        gap
        for partition, gap in report.cross_segment_gaps
        if partition.record_type == RecordType.L2_SNAPSHOT
        and gap.kind == "l2_resync_missing"
    ]
    events: list[dict[str, object]] = []
    for manifest in report.partitions:
        if manifest.partition.record_type != RecordType.CONNECTION_EVENT:
            continue
        events.extend(
            pq.ParquetFile(root / manifest.relative_data_path).read().to_pylist()
        )
    resyncs = [row for row in events if str(row["event_kind"]).startswith("resync_")]
    assert [row["event_kind"] for row in resyncs].count("resync_start") == 2
    assert [row["event_kind"] for row in resyncs].count("resync_complete") == 2
    assert all(row["venue"] == "binance_usdm" for row in resyncs)
    assert all(row["asset"] == "BTC" for row in resyncs)
    assert all(row["received_time"] is not None for row in resyncs)
    assert all(
        row["resync_snapshot_id"] is not None
        for row in resyncs
        if row["event_kind"] == "resync_complete"
    )


def test_binance_staleness_is_checked_per_required_bbo_and_l2_stream(
    tmp_path: Path,
) -> None:
    sink = BatchingLakeSink(tmp_path / "lake", batch_size=10, queue_capacity=20)
    collector = _reference_collector(tmp_path, sink)
    try:
        collector._initialize_critical_streams(_connector(), at=BASE)
        collector._critical_stream_last_received["btcusdt@bookTicker"] = BASE + timedelta(
            seconds=29
        )
        assert collector._stale_critical_streams(at=BASE + timedelta(seconds=31)) == (
            "btcusdt@depth20@100ms",
        )
    finally:
        collector.close()


class FatalWriterSink:
    high_water = 0
    pending_count = 0
    should_flush = False

    def add(self, record: ParsedRecord) -> bool:
        del record
        raise CoordinatedWriterError("simulated coordinated writer incompatibility")

    def flush(self) -> object:
        raise AssertionError("fatal writer must not be flushed and retried")

    def close(self) -> None:
        pass


class NoMessageSocket:
    def __init__(self) -> None:
        self.closed = False

    def receive(self, timeout_seconds: float) -> None:
        del timeout_seconds
        raise AssertionError("writer failed before the first socket receive")

    def close(self) -> None:
        self.closed = True


class FatalWriterSocketFactory:
    socket = NoMessageSocket()

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def connect(self, network: str, timeout_seconds: float) -> NoMessageSocket:
        del network, timeout_seconds
        return self.socket


def test_binance_coordinated_writer_error_is_fatal_without_reconnect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collector = BinanceReferenceCollector(
        ReferenceCollectorConfig(assets=("BTC",), candle_intervals=("1m",)),
        rest=BinancePublicRestClient(transport=FakeTransport()),
        sink=FatalWriterSink(),  # type: ignore[arg-type]
        runtime_status_path=tmp_path / "runtime-status.json",
        clock=lambda: BASE,
    )
    monkeypatch.setattr(collector, "_bootstrap", _connector)
    monkeypatch.setattr(
        "hyperlab.venues.runtime.UrlWebsocketClientFactory",
        FatalWriterSocketFactory,
    )
    FatalWriterSocketFactory.socket = NoMessageSocket()

    with pytest.raises(
        CoordinatedWriterError,
        match="simulated coordinated writer incompatibility",
    ):
        collector.run(max_messages=1)

    assert collector.metrics["state"] == "failed"
    assert collector.metrics["connections"] == 1
    assert collector.metrics["reconnects"] == 0
    assert FatalWriterSocketFactory.socket.closed is True


class FailingTerminalFlushSink:
    high_water = 0
    pending_count = 1
    should_flush = False

    def __init__(self) -> None:
        self.closed = False

    def add(self, record: ParsedRecord) -> bool:
        del record
        return True

    def flush(self) -> object:
        raise CoordinatedWriterError("simulated terminal flush failure")

    def close(self) -> None:
        self.closed = True


def test_binance_close_releases_sink_after_terminal_flush_failure(
    tmp_path: Path,
) -> None:
    sink = FailingTerminalFlushSink()
    collector = BinanceReferenceCollector(
        ReferenceCollectorConfig(assets=("BTC",), candle_intervals=("1m",)),
        rest=BinancePublicRestClient(transport=FakeTransport()),
        sink=sink,  # type: ignore[arg-type]
        runtime_status_path=tmp_path / "runtime-status.json",
        clock=lambda: BASE,
    )

    with pytest.raises(
        CoordinatedWriterError,
        match="simulated terminal flush failure",
    ):
        collector.close()

    assert sink.closed is True
    assert collector.metrics["state"] == "failed"
    collector.close()
