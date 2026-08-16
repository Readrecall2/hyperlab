from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from hyperlab.collector.models import ParsedRecord, WireEnvelope
from hyperlab.collector.parser import parse_websocket_message
from hyperlab.collector.storage import BatchingLakeSink
from hyperlab.data.lake import PartitionKey, PartitionValidationError
from hyperlab.data.schema import RecordType

NOW = datetime(2026, 8, 12, 12, tzinfo=UTC)
OPEN_TIME = datetime(2026, 8, 12, 11, 59, tzinfo=UTC)


def _parsed(raw_message: str, sequence: int) -> tuple[ParsedRecord, ...]:
    return parse_websocket_message(
        WireEnvelope(
            raw_message=raw_message,
            received_time=NOW,
            connection_id="interleaved-test",
            connection_epoch=1,
            arrival_sequence=sequence,
        )
    ).records


def _bbo_message(asset: str, sequence: int, bid: str = "1") -> tuple[ParsedRecord, ...]:
    return _parsed(
        (
            '{"channel":"bbo","data":{"coin":"'
            f'{asset}","time":1786536000000,"bbo":['
            f'{{"px":"{bid}","sz":"2","n":1}},'
            '{"px":"2","sz":"3","n":1}]}}'
        ),
        sequence,
    )


def _trade_message(asset: str, sequence: int) -> tuple[ParsedRecord, ...]:
    return _parsed(
        (
            '{"channel":"trades","data":[{"coin":"'
            f'{asset}","side":"B","px":"1.5","sz":"1",'
            '"time":1786536000000,"tid":12345}]}'
        ),
        sequence,
    )


def _candle_record(
    observation_id: str,
    *,
    two_decimal_places: bool,
    corrected_close: bool = False,
    received_time: datetime = NOW,
) -> ParsedRecord:
    values = {
        "open": "1.00" if two_decimal_places else "1.0",
        "high": "1.20" if two_decimal_places else "1.2",
        "low": "0.90" if two_decimal_places else "0.9",
        "close": "1.10" if two_decimal_places else "1.1",
        "base_volume": "10.00" if two_decimal_places else "10.0",
        "quote_volume": "11.00" if two_decimal_places else "11.0",
    }
    if corrected_close:
        values["close"] = "1.15"
    row: dict[str, object] = {
        "schema_version": 2,
        "record_type": RecordType.CANDLE.value,
        "venue": "hyperliquid",
        "asset": "BTC",
        "event_time": OPEN_TIME,
        "exchange_time": OPEN_TIME,
        "received_time": received_time,
        "source_sequence": None,
        "connection_id": "rest-bootstrap",
        "interval": "1m",
        "open_time": OPEN_TIME,
        "close_time": OPEN_TIME + timedelta(minutes=1) - timedelta(milliseconds=1),
        "trade_count": 5,
        "is_final": True,
        "observation_id": observation_id,
        **{name: Decimal(value) for name, value in values.items()},
    }
    return ParsedRecord(RecordType.CANDLE, "BTC", row)


def test_partition_asset_encoding_is_injective_and_round_trips() -> None:
    assets = ("@107", "A/B", "A_B", "%")
    keys = {
        asset: PartitionKey("hyperliquid", date(2026, 8, 12), asset, RecordType.TRADE) for asset in assets
    }
    encoded = {asset: key.relative_path.parts[2].removeprefix("asset=") for asset, key in keys.items()}

    assert encoded == {
        "@107": "%40107",
        "A/B": "A%2FB",
        "A_B": "A_B",
        "%": "%25",
    }
    assert len(set(encoded.values())) == len(assets)
    for key in keys.values():
        assert PartitionKey.from_leaf(key.relative_path) == key


def test_partition_asset_decode_rejects_noncanonical_percent_encoding() -> None:
    noncanonical = Path("venue=hyperliquid/date=2026-08-12/asset=A%2fB/type=trade")

    with pytest.raises(PartitionValidationError, match="encoding is not canonical"):
        PartitionKey.from_leaf(noncanonical)


def test_interleaved_wire_messages_share_global_partition_and_keep_message_asset(
    tmp_path: Path,
) -> None:
    btc_first = _bbo_message("BTC", 1)[0]
    eth = _trade_message("ETH", 2)[0]
    btc_second = _bbo_message("BTC", 3, bid="1.1")[0]
    records = (btc_first, eth, btc_second)
    assert [record.record_type for record in records] == [RecordType.WIRE_MESSAGE] * 3
    assert [record.asset for record in records] == ["GLOBAL"] * 3
    assert [record.row["message_asset"] for record in records] == ["BTC", "ETH", "BTC"]

    sink = BatchingLakeSink(
        tmp_path / "lake",
        batch_size=3,
        queue_capacity=3,
        persistent_dedup=False,
    )
    try:
        assert all(sink.add(record) for record in records)
        result = sink.flush()
    finally:
        sink.close()

    assert result.row_count == 3
    assert len(result.manifests) == 1
    manifest = result.manifests[0]
    assert manifest.partition.asset == "GLOBAL"
    table = pq.ParquetFile(sink.root / manifest.relative_data_path).read()
    assert table.column("asset").to_pylist() == ["GLOBAL", "GLOBAL", "GLOBAL"]
    assert table.column("arrival_sequence").to_pylist() == [1, 2, 3]
    assert table.column("message_asset").to_pylist() == ["BTC", "ETH", "BTC"]


def test_overflow_for_a_new_group_does_not_leave_an_empty_group(tmp_path: Path) -> None:
    wire = _bbo_message("BTC", 1)[0]
    trade = next(record for record in _trade_message("ETH", 2) if record.record_type == RecordType.TRADE)
    sink = BatchingLakeSink(
        tmp_path / "lake",
        batch_size=1,
        queue_capacity=1,
        persistent_dedup=False,
    )
    try:
        assert sink.add(wire) is True
        groups_before = tuple(sink._groups)

        with pytest.raises(BufferError, match="no record was dropped"):
            sink.add(trade)

        assert tuple(sink._groups) == groups_before
        assert len(sink._groups) == 1
        assert all(group for group in sink._groups.values())
        assert sink.pending_count == 1
        result = sink.flush()
    finally:
        sink.close()

    assert result.row_count == 1
    assert result.manifests[0].partition.record_type == RecordType.WIRE_MESSAGE


def test_persistent_dedup_only_rejects_consecutive_payload_across_restarts(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    observations = (
        _candle_record(
            "a-first",
            two_decimal_places=False,
            received_time=NOW,
        ),
        _candle_record(
            "b-correction",
            two_decimal_places=False,
            corrected_close=True,
            received_time=NOW + timedelta(seconds=1),
        ),
        _candle_record(
            "a-restored",
            two_decimal_places=False,
            received_time=NOW + timedelta(seconds=2),
        ),
    )

    for record in observations:
        sink = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
        try:
            assert sink.add(record) is True
            assert sink.flush().row_count == 1
        finally:
            sink.close()

    (root / ".collector-observations.sqlite3").unlink()
    duplicate_sink = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    try:
        assert (
            duplicate_sink.add(
                _candle_record(
                    "a-duplicate",
                    two_decimal_places=True,
                    received_time=NOW + timedelta(seconds=3),
                )
            )
            is False
        )
        assert (
            duplicate_sink.add(
                _candle_record(
                    "b-after-a",
                    two_decimal_places=True,
                    corrected_close=True,
                    received_time=NOW + timedelta(seconds=4),
                )
            )
            is True
        )
        rebuilt_flush = duplicate_sink.flush()
        assert (rebuilt_flush.row_count, rebuilt_flush.duplicate_count) == (1, 1)
    finally:
        duplicate_sink.close()

    closes = [
        row["close"]
        for parquet_path in root.rglob("*.parquet")
        for row in pq.ParquetFile(parquet_path).read().to_pylist()
    ]
    assert closes.count(Decimal("1.100000000000000000")) == 2
    assert closes.count(Decimal("1.150000000000000000")) == 2


def test_persistent_dedup_rebuild_normalizes_decimals_and_allows_correction(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    first_sink = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    try:
        assert first_sink.add(_candle_record("first", two_decimal_places=False)) is True
        first_flush = first_sink.flush()
    finally:
        first_sink.close()

    assert first_flush.row_count == 1
    index_path = root / ".collector-observations.sqlite3"
    assert index_path.is_file()
    index_path.unlink()
    with sqlite3.connect(index_path) as legacy_index:
        legacy_index.executescript(
            """
            CREATE TABLE observations (
                record_type TEXT NOT NULL,
                logical_key_sha256 TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                PRIMARY KEY (record_type, logical_key_sha256, payload_sha256)
            ) WITHOUT ROWID;
            CREATE TABLE indexed_manifests (
                data_file TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                PRIMARY KEY (data_file, sha256)
            ) WITHOUT ROWID;
            """
        )

    rebuilt_sink = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    try:
        assert rebuilt_sink.add(_candle_record("same-value-new-scale", two_decimal_places=True)) is False
        assert rebuilt_sink.pending_count == 0

        assert (
            rebuilt_sink.add(
                _candle_record(
                    "source-correction",
                    two_decimal_places=True,
                    corrected_close=True,
                )
            )
            is True
        )
        corrected_flush = rebuilt_sink.flush()
    finally:
        rebuilt_sink.close()

    assert corrected_flush.row_count == 1
    assert corrected_flush.duplicate_count == 1
    assert len(corrected_flush.manifests) == 1
    closes = sorted(
        row["close"]
        for parquet_path in root.rglob("*.parquet")
        for row in pq.ParquetFile(parquet_path).read().to_pylist()
    )
    with sqlite3.connect(index_path) as rebuilt_index:
        assert rebuilt_index.execute("PRAGMA user_version").fetchone() == (4,)
        assert (
            rebuilt_index.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'observations'"
            ).fetchone()
            is None
        )
        assert rebuilt_index.execute("SELECT COUNT(*) FROM observation_heads").fetchone() == (1,)
    assert closes == [
        Decimal("1.100000000000000000"),
        Decimal("1.150000000000000000"),
    ]
