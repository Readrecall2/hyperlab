from __future__ import annotations

import json
import threading
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar

import pyarrow.parquet as pq
import pytest

from hyperlab.collector.models import ParsedMessage, ParsedRecord, WireEnvelope
from hyperlab.collector.storage import BatchingLakeSink, CoordinatedWriterError
from hyperlab.collector.websocket import ReceivedWireMessage
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


def _exchange_info_for_btc_and_eth() -> dict[str, object]:
    btc = _exchange_info()["symbols"][0]  # type: ignore[index]
    return {
        "symbols": [
            btc,
            {
                **btc,  # type: ignore[arg-type]
                "symbol": "ETHUSDT",
                "pair": "ETHUSDT",
                "baseAsset": "ETH",
            },
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

def test_binance_connector_splits_book_and_market_streams_on_official_public_urls() -> None:
    connector = BinancePublicConnector.from_exchange_info(
        _exchange_info_for_btc_and_eth(),
        ("BTC", "ETH"),
    )

    urls = connector.websocket_urls(("BTC", "ETH"), ("1m",))

    public_prefix = "wss://fstream.binance.com/public/stream?streams="
    market_prefix = "wss://fstream.binance.com/market/stream?streams="
    assert set(urls) == {"public", "market"}
    assert urls["public"].startswith(public_prefix)
    assert urls["market"].startswith(market_prefix)
    assert set(urls["public"].removeprefix(public_prefix).split("/")) == {
        "btcusdt@bookTicker",
        "btcusdt@depth20@100ms",
        "ethusdt@bookTicker",
        "ethusdt@depth20@100ms",
    }
    assert set(urls["market"].removeprefix(market_prefix).split("/")) == {
        "btcusdt@aggTrade",
        "btcusdt@markPrice@1s",
        "btcusdt@kline_1m",
        "ethusdt@aggTrade",
        "ethusdt@markPrice@1s",
        "ethusdt@kline_1m",
    }


def test_binance_agg_trade_preserves_wire_and_normalized_lineage_in_lake(
    tmp_path: Path,
) -> None:
    connector = _connector()
    received_time = BASE + timedelta(milliseconds=27)
    event_ms = 1_786_492_800_019
    exchange_ms = 1_786_492_800_020
    frame = _combined(
        "btcusdt@aggTrade",
        {
            "e": "aggTrade",
            "E": exchange_ms,
            "T": event_ms,
            "s": "BTCUSDT",
            "a": 42,
            "p": "60000.2",
            "q": "0.01",
            "m": False,
        },
    )
    envelope = WireEnvelope(
        json.dumps(frame, separators=(",", ":")),
        received_time,
        "physical-market-connection",
        7,
        19,
    )

    parsed = connector.parse_message(envelope)

    assert parsed.issues == ()
    assert [record.record_type for record in parsed.records] == [
        RecordType.WIRE_MESSAGE,
        RecordType.TRADE,
    ]
    wire, trade = parsed.records
    assert wire.row["raw_message"] == envelope.raw_message
    assert json.loads(str(wire.row["raw_message"]))["data"]["T"] == event_ms
    assert json.loads(str(wire.row["raw_message"]))["data"]["E"] == exchange_ms
    assert wire.row["received_time"] == received_time
    assert wire.row["connection_id"] == "physical-market-connection"
    assert wire.row["connection_epoch"] == 7
    assert wire.row["arrival_sequence"] == 19
    assert trade.row["event_time"] == datetime.fromtimestamp(event_ms / 1_000, tz=UTC)
    assert trade.row["exchange_time"] == datetime.fromtimestamp(exchange_ms / 1_000, tz=UTC)
    assert trade.row["received_time"] == received_time
    assert trade.row["connection_id"] == "physical-market-connection"
    assert trade.row["connection_epoch"] == 7
    assert trade.row["arrival_sequence"] == 19
    assert trade.row["source_sequence"] == 42

    root = tmp_path / "lake"
    sink = BatchingLakeSink(root, persistent_dedup=False)
    try:
        assert sink.add_many(parsed.records) == 2
        sink.flush()
    finally:
        sink.close()

    persisted: dict[RecordType, list[dict[str, object]]] = {}
    for manifest in inventory_partitions(root).partitions:
        persisted.setdefault(manifest.partition.record_type, []).extend(
            pq.ParquetFile(root / manifest.relative_data_path).read().to_pylist()
        )
    persisted_wire = persisted[RecordType.WIRE_MESSAGE][0]
    persisted_trade = persisted[RecordType.TRADE][0]
    assert len(persisted[RecordType.WIRE_MESSAGE]) == 1
    assert len(persisted[RecordType.TRADE]) == 1
    assert persisted_wire["raw_message"] == envelope.raw_message
    assert persisted_wire["connection_id"] == "physical-market-connection"
    assert persisted_wire["connection_epoch"] == 7
    assert persisted_wire["arrival_sequence"] == 19
    assert persisted_trade["event_time"] == trade.row["event_time"]
    assert persisted_trade["exchange_time"] == trade.row["exchange_time"]
    assert persisted_trade["received_time"] == received_time
    assert persisted_trade["connection_id"] == "physical-market-connection"
    assert persisted_trade["connection_epoch"] == 7
    assert persisted_trade["arrival_sequence"] == 19


def test_binance_malformed_agg_trade_retains_exact_raw_wire_frame() -> None:
    raw_message = json.dumps(
        _combined(
            "btcusdt@aggTrade",
            {
                "e": "aggTrade",
                "E": 1_786_492_800_020,
                "T": 1_786_492_800_019,
                "s": "BTCUSDT",
                "a": 42,
                "p": "not-a-price",
                "q": "0.01",
                "m": False,
            },
        ),
        separators=(",", ":"),
    )
    envelope = WireEnvelope(
        raw_message,
        BASE + timedelta(milliseconds=27),
        "malformed-market-connection",
        3,
        11,
    )
    parsed = _connector().parse_message(envelope)

    assert len(parsed.records) == 1
    wire = parsed.records[0]
    assert wire.record_type == RecordType.WIRE_MESSAGE
    assert wire.row["raw_message"] == raw_message
    assert wire.row["channel"] == "btcusdt@aggTrade"
    assert wire.row["message_asset"] == "BTC"
    assert wire.row["connection_id"] == "malformed-market-connection"
    assert wire.row["connection_epoch"] == 3
    assert wire.row["arrival_sequence"] == 11
    assert len(parsed.issues) == 1
    assert str(parsed.issues[0]).startswith("invalid_btcusdt@aggTrade:ValueError:")


@pytest.mark.parametrize(
    ("field", "value", "issue_fragment"),
    [
        ("a", -1, "aggregate trade ID cannot be negative"),
        ("p", "0", "aggregate trade price must be positive"),
        ("q", "0", "aggregate trade quantity must be positive"),
        ("m", "false", "aggregate trade maker flag must be boolean"),
    ],
)
def test_binance_invalid_agg_trade_values_remain_raw_only(
    field: str,
    value: object,
    issue_fragment: str,
) -> None:
    frame = _trade_frame(1_786_492_800_019, sequence=42)
    data = frame["data"]
    assert isinstance(data, dict)
    data[field] = value
    raw_message = json.dumps(frame, separators=(",", ":"))

    parsed = _connector().parse_message(
        WireEnvelope(raw_message, BASE, "market-connection", 1, 1)
    )

    assert [record.record_type for record in parsed.records] == [
        RecordType.WIRE_MESSAGE
    ]
    assert parsed.records[0].row["raw_message"] == raw_message
    assert len(parsed.issues) == 1
    assert issue_fragment in parsed.issues[0]


def test_non_boolean_maker_flag_cannot_arm_or_refresh_trade_health(
    tmp_path: Path,
) -> None:
    collector = _reference_collector(
        tmp_path,
        BatchingLakeSink(tmp_path / "lake", batch_size=10, queue_capacity=20),
    )
    connector = _connector()
    collector._initialize_critical_streams(connector, at=BASE)
    channel = "btcusdt@aggTrade"

    def parsed_trade(maker: object, *, seconds: int, sequence: int) -> ParsedMessage:
        frame = _trade_frame(
            int((BASE + timedelta(seconds=seconds)).timestamp() * 1_000),
            sequence=sequence,
        )
        data = frame["data"]
        assert isinstance(data, dict)
        data["m"] = maker
        return connector.parse_message(
            WireEnvelope(
                json.dumps(frame, separators=(",", ":")),
                BASE + timedelta(seconds=seconds),
                "market-connection",
                1,
                sequence,
            )
        )

    try:
        malformed = parsed_trade("false", seconds=10, sequence=1)
        assert [record.record_type for record in malformed.records] == [
            RecordType.WIRE_MESSAGE
        ]
        collector._observe_critical_stream(
            malformed,
            received_time=BASE + timedelta(seconds=10),
            socket_role="market",
        )
        assert channel not in collector._critical_stream_seen
        assert collector._critical_stream_last_received[channel] == BASE

        valid = parsed_trade(False, seconds=11, sequence=2)
        collector._observe_critical_stream(
            valid,
            received_time=BASE + timedelta(seconds=11),
            socket_role="market",
        )
        assert channel in collector._critical_stream_seen
        assert collector._critical_stream_last_received[channel] == BASE + timedelta(
            seconds=11
        )

        malformed_again = parsed_trade("false", seconds=12, sequence=3)
        collector._observe_critical_stream(
            malformed_again,
            received_time=BASE + timedelta(seconds=12),
            socket_role="market",
        )
        assert collector._critical_stream_last_received[channel] == BASE + timedelta(
            seconds=11
        )
    finally:
        collector.close()


@pytest.mark.parametrize(
    ("stream_type", "expect_trade"),
    [
        (1, True),
        ("1", True),
        (2, False),
        ("2", False),
        ("invalid", False),
        (True, False),
        (None, False),
        (1.0, False),
    ],
)
def test_binance_usdm_stream_type_discriminator_is_fail_closed(
    stream_type: object,
    *,
    expect_trade: bool,
) -> None:
    frame = _trade_frame(1_786_492_800_019, sequence=42)
    data = frame["data"]
    assert isinstance(data, dict)
    data["st"] = stream_type
    raw_message = json.dumps(frame, separators=(",", ":"))

    parsed = _connector().parse_message(
        WireEnvelope(raw_message, BASE, "market-connection", 1, 1)
    )

    assert parsed.records[0].record_type == RecordType.WIRE_MESSAGE
    assert parsed.records[0].row["raw_message"] == raw_message
    if expect_trade:
        assert parsed.issues == ()
        assert [record.record_type for record in parsed.records] == [
            RecordType.WIRE_MESSAGE,
            RecordType.TRADE,
        ]
    else:
        assert [record.record_type for record in parsed.records] == [
            RecordType.WIRE_MESSAGE
        ]
        assert parsed.issues == (
            f"invalid_stream_type:expected=1:data={stream_type!r}",
        )


@pytest.mark.parametrize(
    ("wrapper_stream_type", "data_stream_type"),
    [(1, 2), (2, 1), ("1", True)],
)
def test_binance_usdm_wrapper_and_payload_stream_type_conflicts_fail_closed(
    wrapper_stream_type: object,
    data_stream_type: object,
) -> None:
    frame = _trade_frame(1_786_492_800_019, sequence=42)
    frame["st"] = wrapper_stream_type
    data = frame["data"]
    assert isinstance(data, dict)
    data["st"] = data_stream_type

    parsed = _connector().parse_message(
        WireEnvelope(
            json.dumps(frame, separators=(",", ":")),
            BASE,
            "market-connection",
            1,
            1,
        )
    )

    assert [record.record_type for record in parsed.records] == [
        RecordType.WIRE_MESSAGE
    ]
    assert parsed.issues == (
        "invalid_stream_type:expected=1:"
        f"wrapper={wrapper_stream_type!r},data={data_stream_type!r}",
    )


def test_non_usdm_stream_type_cannot_arm_or_refresh_trade_health(
    tmp_path: Path,
) -> None:
    collector = _reference_collector(
        tmp_path,
        BatchingLakeSink(tmp_path / "lake", batch_size=10, queue_capacity=20),
    )
    connector = _connector()
    collector._initialize_critical_streams(connector, at=BASE)
    channel = "btcusdt@aggTrade"

    def parsed_trade(stream_type: object, *, seconds: int, sequence: int) -> ParsedMessage:
        frame = _trade_frame(
            int((BASE + timedelta(seconds=seconds)).timestamp() * 1_000),
            sequence=sequence,
        )
        data = frame["data"]
        assert isinstance(data, dict)
        data["st"] = stream_type
        return connector.parse_message(
            WireEnvelope(
                json.dumps(frame, separators=(",", ":")),
                BASE + timedelta(seconds=seconds),
                "market-connection",
                1,
                sequence,
            )
        )

    try:
        coin_m = parsed_trade(2, seconds=10, sequence=1)
        assert [record.record_type for record in coin_m.records] == [
            RecordType.WIRE_MESSAGE
        ]
        collector._observe_critical_stream(
            coin_m,
            received_time=BASE + timedelta(seconds=10),
            socket_role="market",
        )
        assert channel not in collector._critical_stream_seen
        assert collector._critical_stream_last_received[channel] == BASE

        usdm = parsed_trade("1", seconds=11, sequence=2)
        collector._observe_critical_stream(
            usdm,
            received_time=BASE + timedelta(seconds=11),
            socket_role="market",
        )
        assert channel in collector._critical_stream_seen
        assert collector._critical_stream_last_received[channel] == BASE + timedelta(
            seconds=11
        )

        malformed = parsed_trade(True, seconds=12, sequence=3)
        assert [record.record_type for record in malformed.records] == [
            RecordType.WIRE_MESSAGE
        ]
        collector._observe_critical_stream(
            malformed,
            received_time=BASE + timedelta(seconds=12),
            socket_role="market",
        )
        assert collector._critical_stream_last_received[channel] == BASE + timedelta(
            seconds=11
        )
    finally:
        collector.close()


def test_malformed_agg_trade_cannot_arm_or_refresh_required_trade_health(
    tmp_path: Path,
) -> None:
    collector = _reference_collector(
        tmp_path,
        BatchingLakeSink(tmp_path / "lake", batch_size=10, queue_capacity=20),
    )
    connector = _connector()
    collector._initialize_critical_streams(connector, at=BASE)
    channel = "btcusdt@aggTrade"
    malformed = connector.parse_message(
        WireEnvelope(
            json.dumps(
                _combined(
                    channel,
                    {
                        "e": "aggTrade",
                        "E": 1_786_492_800_020,
                        "T": 1_786_492_800_019,
                        "s": "BTCUSDT",
                        "a": 42,
                        "p": "not-a-price",
                        "q": "0.01",
                        "m": False,
                    },
                ),
                separators=(",", ":"),
            ),
            BASE + timedelta(seconds=10),
            "market-connection",
            1,
            1,
        )
    )
    try:
        collector._observe_critical_stream(
            malformed,
            received_time=BASE + timedelta(seconds=10),
            socket_role="market",
        )
        assert channel not in collector._critical_stream_seen
        assert collector._critical_stream_last_received[channel] == BASE

        valid = connector.parse_message(
            WireEnvelope(
                json.dumps(
                    _trade_frame(
                        int((BASE + timedelta(seconds=11)).timestamp() * 1_000),
                        sequence=43,
                    ),
                    separators=(",", ":"),
                ),
                BASE + timedelta(seconds=11),
                "market-connection",
                1,
                2,
            )
        )
        collector._observe_critical_stream(
            valid,
            received_time=BASE + timedelta(seconds=11),
            socket_role="market",
        )
        assert channel in collector._critical_stream_seen
        assert collector._critical_stream_last_received[channel] == BASE + timedelta(
            seconds=11
        )
    finally:
        collector.close()


def test_stream_symbol_or_event_mismatch_cannot_arm_trade_health(
    tmp_path: Path,
) -> None:
    connector = BinancePublicConnector.from_exchange_info(
        _exchange_info_for_btc_and_eth(),
        ("BTC", "ETH"),
    )
    collector = BinanceReferenceCollector(
        ReferenceCollectorConfig(
            assets=("BTC", "ETH"),
            candle_intervals=("1m",),
            batch_size=10,
            queue_capacity=20,
        ),
        rest=BinancePublicRestClient(transport=FakeTransport()),
        sink=BatchingLakeSink(tmp_path / "lake", batch_size=10, queue_capacity=20),
        runtime_status_path=tmp_path / "runtime-status.json",
        clock=lambda: BASE,
    )
    collector._initialize_critical_streams(connector, at=BASE)
    channel = "btcusdt@aggTrade"
    payload = {
        "e": "aggTrade",
        "E": 1_786_492_800_020,
        "T": 1_786_492_800_019,
        "s": "ETHUSDT",
        "a": 42,
        "p": "3000.1",
        "q": "0.01",
        "m": False,
    }
    symbol_mismatch = connector.parse_message(
        WireEnvelope(
            json.dumps(_combined(channel, payload), separators=(",", ":")),
            BASE + timedelta(seconds=10),
            "market-connection",
            1,
            1,
        )
    )
    event_mismatch = connector.parse_message(
        WireEnvelope(
            json.dumps(
                _combined(
                    channel,
                    {
                        **payload,
                        "e": "bookTicker",
                        "s": "BTCUSDT",
                    },
                ),
                separators=(",", ":"),
            ),
            BASE + timedelta(seconds=11),
            "market-connection",
            1,
            2,
        )
    )
    try:
        assert [record.record_type for record in symbol_mismatch.records] == [
            RecordType.WIRE_MESSAGE
        ]
        assert symbol_mismatch.issues == (
            "stream_symbol_mismatch:channel=btcusdt:payload=ETHUSDT",
        )
        assert [record.record_type for record in event_mismatch.records] == [
            RecordType.WIRE_MESSAGE
        ]
        assert len(event_mismatch.issues) == 1
        assert "event/channel mismatch" in event_mismatch.issues[0]

        for parsed, received_time in (
            (symbol_mismatch, BASE + timedelta(seconds=10)),
            (event_mismatch, BASE + timedelta(seconds=11)),
        ):
            collector._observe_critical_stream(
                parsed,
                received_time=received_time,
                socket_role="market",
            )
        assert channel not in collector._critical_stream_seen
        assert collector._critical_stream_last_received[channel] == BASE
    finally:
        collector.close()


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
        collector._critical_stream_last_received["btcusdt@aggTrade"] = BASE + timedelta(
            seconds=29
        )
        assert collector._stale_critical_streams(at=BASE + timedelta(seconds=31)) == (
            "btcusdt@depth20@100ms",
        )
    finally:
        collector.close()


def test_binance_agg_trade_is_a_required_stream_for_each_asset(tmp_path: Path) -> None:
    sink = BatchingLakeSink(tmp_path / "lake", batch_size=10, queue_capacity=20)
    collector = BinanceReferenceCollector(
        ReferenceCollectorConfig(
            assets=("BTC", "ETH"),
            candle_intervals=("1m",),
            batch_size=10,
            queue_capacity=20,
        ),
        rest=BinancePublicRestClient(transport=FakeTransport()),
        sink=sink,
        runtime_status_path=tmp_path / "runtime-status.json",
        clock=lambda: BASE,
    )
    connector = BinancePublicConnector.from_exchange_info(
        _exchange_info_for_btc_and_eth(),
        ("BTC", "ETH"),
    )
    try:
        collector._initialize_critical_streams(connector, at=BASE)
        fresh = BASE + timedelta(seconds=29)
        for channel in collector._critical_stream_last_received:
            collector._critical_stream_last_received[channel] = fresh
        collector._critical_stream_last_received["btcusdt@aggTrade"] = BASE
        collector._critical_stream_last_received["ethusdt@aggTrade"] = BASE
        assert collector._stale_critical_streams(at=BASE + timedelta(seconds=31)) == (
            "btcusdt@aggTrade",
            "ethusdt@aggTrade",
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


class OneFrameSocket:
    def __init__(self, raw_message: str) -> None:
        self.messages = [ReceivedWireMessage(raw_message, BASE)]
        self.closed = False

    def receive(self, timeout_seconds: float) -> ReceivedWireMessage | None:
        del timeout_seconds
        return self.messages.pop(0) if self.messages else None

    def close(self) -> None:
        self.closed = True


class SplitSocketFactory:
    sockets: ClassVar[dict[str, OneFrameSocket]] = {}
    urls: ClassVar[dict[str, str]] = {}

    def __init__(self, url: str, *_args: object, **_kwargs: object) -> None:
        if "/public/" in url:
            self.role = "public"
        elif "/market/" in url:
            self.role = "market"
        else:
            raise AssertionError(f"unexpected Binance websocket URL: {url}")
        self.urls[self.role] = url

    def connect(self, network: str, timeout_seconds: float) -> OneFrameSocket:
        assert network == "public"
        assert timeout_seconds > 0
        return self.sockets[self.role]


def test_binance_runtime_supervises_split_sockets_with_one_capture_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    public_frame = _combined(
        "btcusdt@bookTicker",
        {
            "e": "bookTicker",
            "E": int(BASE.timestamp() * 1_000),
            "T": int(BASE.timestamp() * 1_000),
            "s": "BTCUSDT",
            "u": 1,
            "b": "60000.1",
            "B": "1.0",
            "a": "60000.2",
            "A": "2.0",
        },
    )
    market_frame = _combined(
        "btcusdt@aggTrade",
        {
            "e": "aggTrade",
            "E": int(BASE.timestamp() * 1_000),
            "T": int(BASE.timestamp() * 1_000),
            "s": "BTCUSDT",
            "a": 2,
            "p": "60000.2",
            "q": "0.01",
            "m": False,
        },
    )
    SplitSocketFactory.sockets = {
        "public": OneFrameSocket(json.dumps(public_frame, separators=(",", ":"))),
        "market": OneFrameSocket(json.dumps(market_frame, separators=(",", ":"))),
    }
    SplitSocketFactory.urls = {}
    root = tmp_path / "lake"
    sink = BatchingLakeSink(root, batch_size=100, queue_capacity=200)
    collector = _reference_collector(tmp_path, sink)
    monkeypatch.setattr(collector, "_bootstrap", _connector)
    monkeypatch.setattr(
        "hyperlab.venues.runtime.UrlWebsocketClientFactory",
        SplitSocketFactory,
    )

    try:
        collector.run(max_messages=2)
    finally:
        collector.close()

    assert set(SplitSocketFactory.urls) == {"public", "market"}
    assert all(socket.closed for socket in SplitSocketFactory.sockets.values())
    rows: dict[RecordType, list[dict[str, object]]] = {}
    for manifest in inventory_partitions(root).partitions:
        rows.setdefault(manifest.partition.record_type, []).extend(
            pq.ParquetFile(root / manifest.relative_data_path).read().to_pylist()
        )
    wire_rows = rows[RecordType.WIRE_MESSAGE]
    assert len(wire_rows) == 2
    assert {row["connection_id"] for row in wire_rows} == {
        row["connection_id"] for row in rows[RecordType.CONNECTION_EVENT]
    }
    assert len({row["connection_id"] for row in wire_rows}) == 2
    assert {row["connection_epoch"] for row in wire_rows} == {1}
    assert {row["arrival_sequence"] for row in wire_rows} == {1}
    capture_ids = {row["capture_epoch_id"] for row in wire_rows}
    assert len(capture_ids) == 1
    trade = rows[RecordType.TRADE][0]
    market_wire = next(row for row in wire_rows if row["channel"] == "btcusdt@aggTrade")
    assert trade["connection_id"] == market_wire["connection_id"]
    assert trade["connection_epoch"] == market_wire["connection_epoch"]
    assert trade["arrival_sequence"] == market_wire["arrival_sequence"]
    clock = rows[RecordType.CLOCK_SYNC][0]
    assert clock["capture_epoch_id"] in capture_ids
    assert clock["sample_status"] == "valid"


class ScriptedClockRest:
    def __init__(self, measurements: list[object]) -> None:
        self.measurements = measurements
        self.ready = [threading.Event() for _ in measurements]
        self.calls = 0

    def clock_measurement(self) -> object:
        index = self.calls
        self.calls += 1
        measurement = self.measurements[index]
        self.ready[index].set()
        return measurement


class ScriptedSocket:
    def __init__(
        self,
        actions: list[dict[str, object] | BaseException],
        *,
        clock: Any,
        ready: list[threading.Event | None] | None = None,
        advance: Any = None,
    ) -> None:
        self.actions = actions
        self.clock = clock
        self.ready = ready or [None] * len(actions)
        self.advance = advance
        self.receive_count = 0
        self.closed = False

    def receive(self, timeout_seconds: float) -> ReceivedWireMessage | None:
        assert timeout_seconds > 0
        if not self.actions:
            return None
        wait_for = self.ready[self.receive_count]
        if wait_for is not None:
            assert wait_for.wait(timeout=1)
        action = self.actions.pop(0)
        self.receive_count += 1
        if self.advance is not None:
            self.advance()
        if isinstance(action, BaseException):
            raise action
        return ReceivedWireMessage(
            json.dumps(action, separators=(",", ":")),
            self.clock(),
        )

    def close(self) -> None:
        self.closed = True


class ReconnectingSocketFactory:
    runs: ClassVar[dict[str, list[ScriptedSocket]]] = {}

    def __init__(self, url: str, *_args: object, **_kwargs: object) -> None:
        self.role = "public" if "/public/" in url else "market"

    def connect(self, network: str, timeout_seconds: float) -> ScriptedSocket:
        assert network == "public"
        assert timeout_seconds > 0
        return self.runs[self.role].pop(0)


def _clock_measurement_at(offset_seconds: int, rtt_ms: int) -> object:
    sent = BASE + timedelta(seconds=offset_seconds)
    return measure_clock(
        "binance_usdm",
        request_sent_time=sent,
        response_received_time=sent + timedelta(milliseconds=rtt_ms),
        server_time=sent + timedelta(milliseconds=rtt_ms / 2),
    )


def _bbo_frame(event_ms: int, *, sequence: int) -> dict[str, object]:
    return _combined(
        "btcusdt@bookTicker",
        {
            "e": "bookTicker",
            "E": event_ms,
            "T": event_ms,
            "s": "BTCUSDT",
            "u": sequence,
            "b": "60000.1",
            "B": "1.0",
            "a": "60000.2",
            "A": "2.0",
        },
    )


def _trade_frame(event_ms: int, *, sequence: int) -> dict[str, object]:
    return _combined(
        "btcusdt@aggTrade",
        {
            "e": "aggTrade",
            "E": event_ms,
            "T": event_ms,
            "s": "BTCUSDT",
            "a": sequence,
            "p": "60000.2",
            "q": "0.01",
            "m": False,
        },
    )


def test_market_socket_failure_closes_pair_and_reconnects_with_fresh_lineage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rest = ScriptedClockRest(
        [
            _clock_measurement_at(0, 20),
            _clock_measurement_at(2, 20),
        ]
    )
    base_ms = int(BASE.timestamp() * 1_000)
    first_public = ScriptedSocket(
        [_depth_frame(base_ms, last_sequence=1)],
        clock=lambda: BASE,
        ready=[rest.ready[0]],
    )
    first_market = ScriptedSocket(
        [ConnectionError("simulated market socket failure")],
        clock=lambda: BASE,
    )
    second_public = ScriptedSocket(
        [
            _depth_frame(base_ms + 1_000, last_sequence=2),
            _bbo_frame(base_ms + 1_001, sequence=3),
        ],
        clock=lambda: BASE + timedelta(seconds=2),
        ready=[rest.ready[1], None],
    )
    second_market = ScriptedSocket(
        [_trade_frame(base_ms + 1_000, sequence=4)],
        clock=lambda: BASE + timedelta(seconds=2),
    )
    all_sockets = [first_public, first_market, second_public, second_market]
    ReconnectingSocketFactory.runs = {
        "public": [first_public, second_public],
        "market": [first_market, second_market],
    }
    root = tmp_path / "lake"
    collector = BinanceReferenceCollector(
        ReferenceCollectorConfig(assets=("BTC",), candle_intervals=("1m",)),
        rest=rest,  # type: ignore[arg-type]
        sink=BatchingLakeSink(root, batch_size=100, queue_capacity=500),
        runtime_status_path=tmp_path / "runtime-status.json",
        clock=lambda: BASE + timedelta(seconds=2),
    )
    monkeypatch.setattr(collector, "_bootstrap", _connector)
    monkeypatch.setattr(collector, "_interruptible_sleep", lambda _delay: None)
    monkeypatch.setattr(
        "hyperlab.venues.runtime.UrlWebsocketClientFactory",
        ReconnectingSocketFactory,
    )

    try:
        collector.run(max_messages=4)
    finally:
        collector.close()

    assert all(socket.closed for socket in all_sockets)
    assert collector.metrics["connections"] == 2
    assert collector.metrics["physical_connections"] == 4
    assert collector.metrics["reconnects"] == 1
    rows: dict[RecordType, list[dict[str, object]]] = {}
    for manifest in inventory_partitions(root).partitions:
        rows.setdefault(manifest.partition.record_type, []).extend(
            pq.ParquetFile(root / manifest.relative_data_path).read().to_pylist()
        )
    wire_by_epoch: dict[int, list[dict[str, object]]] = {}
    for row in rows[RecordType.WIRE_MESSAGE]:
        wire_by_epoch.setdefault(int(str(row["connection_epoch"])), []).append(row)
    assert set(wire_by_epoch) == {1, 2}
    assert {
        row["connection_id"] for row in wire_by_epoch[1]
    }.isdisjoint({row["connection_id"] for row in wire_by_epoch[2]})
    second_public_wire = sorted(
        (
            row
            for row in wire_by_epoch[2]
            if row["channel"] != "btcusdt@aggTrade"
        ),
        key=lambda row: int(str(row["arrival_sequence"])),
    )
    assert [row["arrival_sequence"] for row in second_public_wire] == [1, 2]
    second_market_wire = next(
        row for row in wire_by_epoch[2] if row["channel"] == "btcusdt@aggTrade"
    )
    assert second_market_wire["arrival_sequence"] == 1
    second_capture = second_market_wire["capture_epoch_id"]
    assert any(
        row["event_kind"] == "resync_complete"
        and row["capture_epoch_id"] == second_capture
        for row in rows[RecordType.CONNECTION_EVENT]
    )
    assert any(
        row["sample_status"] == "valid"
        and row["capture_epoch_id"] == second_capture
        for row in rows[RecordType.CLOCK_SYNC]
    )


class ManualTime:
    def __init__(self) -> None:
        self.seconds = 0.0

    def monotonic(self) -> float:
        return self.seconds

    def now(self) -> datetime:
        return BASE + timedelta(seconds=self.seconds)

    def advance(self) -> None:
        self.seconds += 2.5


class TimedSocketFactory:
    sockets: ClassVar[dict[str, ScriptedSocket]] = {}

    def __init__(self, url: str, *_args: object, **_kwargs: object) -> None:
        self.role = "public" if "/public/" in url else "market"

    def connect(self, network: str, timeout_seconds: float) -> ScriptedSocket:
        assert network == "public"
        assert timeout_seconds > 0
        return self.sockets[self.role]


def test_reference_clock_sampling_default_is_five_seconds() -> None:
    config = ReferenceCollectorConfig()

    assert config.clock_sampling_interval_seconds == 5.0
    assert config.clock_max_age_seconds == 15.0


def test_clock_sampling_continues_and_invalid_sample_breaks_readiness_and_coverage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manual = ManualTime()
    rest = ScriptedClockRest(
        [
            _clock_measurement_at(0, 20),
            _clock_measurement_at(5, 102),
            _clock_measurement_at(10, 102),
            _clock_measurement_at(15, 102),
            _clock_measurement_at(20, 20),
        ]
    )
    base_ms = int(BASE.timestamp() * 1_000)
    public_actions = [
        _depth_frame(base_ms, last_sequence=1),
        *[
            _bbo_frame(base_ms + index, sequence=index)
            for index in range(2, 6)
        ],
    ]
    market_actions = [
        _trade_frame(base_ms + index, sequence=index)
        for index in range(1, 6)
    ]
    public = ScriptedSocket(
        public_actions,
        clock=manual.now,
        ready=rest.ready,
        advance=manual.advance,
    )
    market = ScriptedSocket(
        market_actions,
        clock=manual.now,
        advance=manual.advance,
    )
    TimedSocketFactory.sockets = {"public": public, "market": market}
    root = tmp_path / "lake"
    collector = BinanceReferenceCollector(
        ReferenceCollectorConfig(assets=("BTC",), candle_intervals=("1m",)),
        rest=rest,  # type: ignore[arg-type]
        sink=BatchingLakeSink(root, batch_size=100, queue_capacity=500),
        runtime_status_path=tmp_path / "runtime-status.json",
        clock=manual.now,
        monotonic=manual.monotonic,
    )
    readiness: list[bool] = []
    original_refresh = collector._refresh_capture_readiness

    def record_readiness(**kwargs: Any) -> None:
        original_refresh(**kwargs)
        readiness.append(bool(collector.metrics["capture_ready"]))

    monkeypatch.setattr(collector, "_bootstrap", _connector)
    monkeypatch.setattr(collector, "_refresh_capture_readiness", record_readiness)
    monkeypatch.setattr(
        "hyperlab.venues.runtime.UrlWebsocketClientFactory",
        TimedSocketFactory,
    )

    try:
        collector.run(max_messages=10)
    finally:
        collector.close()

    assert rest.calls == 5
    assert True in readiness
    first_ready = readiness.index(True)
    assert False in readiness[first_ready + 1 :]
    clock_rows: list[dict[str, object]] = []
    for manifest in inventory_partitions(root).partitions:
        if manifest.partition.record_type == RecordType.CLOCK_SYNC:
            clock_rows.extend(
                pq.ParquetFile(root / manifest.relative_data_path).read().to_pylist()
            )
    clock_rows.sort(key=lambda row: row["response_received_time"])
    assert [row["sample_status"] for row in clock_rows] == [
        "valid",
        "invalid",
        "invalid",
        "invalid",
        "valid",
    ]
    assert clock_rows[1]["causal_valid_from"] is None
    assert clock_rows[1]["causal_valid_until"] is None
    assert clock_rows[0]["causal_valid_until"] < clock_rows[4]["causal_valid_from"]


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
