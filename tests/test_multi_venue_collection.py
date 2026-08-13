from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from hyperlab.collector.models import ParsedRecord
from hyperlab.collector.multivenue import MultiVenueCollector
from hyperlab.collector.storage import (
    BatchingLakeSink,
    CoordinatedLakeSink,
    CoordinatedLakeWriter,
    CoordinatedWriterError,
)
from hyperlab.data.lake import discover_partitions, validate_partition
from hyperlab.data.schema import RecordType

BASE = datetime(2026, 8, 13, 0, tzinfo=UTC)
VENUES = ("hyperliquid", "binance_usdm")


def _trade(venue: str, index: int) -> ParsedRecord:
    timestamp = BASE + timedelta(microseconds=index)
    return ParsedRecord(
        RecordType.TRADE,
        "BTC",
        {
            "schema_version": 1,
            "record_type": RecordType.TRADE.value,
            "venue": venue,
            "asset": "BTC",
            "event_time": timestamp,
            "exchange_time": timestamp,
            "received_time": timestamp + timedelta(milliseconds=1),
            "source_sequence": index,
            "connection_id": f"{venue}-connection",
            # Deliberately identical across venues: venue must prevent collision.
            "trade_id": f"shared-trade-{index}",
            "aggressor_side": "buy",
            "price": Decimal("60000"),
            "quantity": Decimal("0.001"),
            "quote_quantity": Decimal("60"),
            "is_liquidation": None,
        },
    )


def test_coordinated_writer_serializes_concurrent_venues_without_dedup_collision(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    writer = CoordinatedLakeWriter(
        root,
        venues=VENUES,
        batch_size=17,
        queue_capacity=500,
    )
    hyperliquid = writer.client("hyperliquid")
    binance = writer.client("binance_usdm")

    with pytest.raises(RuntimeError, match="active writer"):
        BatchingLakeSink(root, batch_size=1, queue_capacity=2)

    def ingest(venue: str, sink: CoordinatedLakeSink) -> tuple[int, int]:
        accepted = 0
        rows_written = 0
        for index in range(100):
            accepted += int(sink.add(_trade(venue, index)))
            if index % 7 == 0:
                rows_written += sink.flush().row_count
        rows_written += sink.flush().row_count
        return accepted, rows_written

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = tuple(
                future.result()
                for future in (
                    executor.submit(ingest, "hyperliquid", hyperliquid),
                    executor.submit(ingest, "binance_usdm", binance),
                )
            )
        assert results == ((100, 100), (100, 100))
        assert writer.pending_count == 0
    finally:
        hyperliquid.close()
        binance.close()
        writer.close()

    manifests = [validate_partition(path) for path in discover_partitions(root)]
    assert {manifest.partition.venue for manifest in manifests} == set(VENUES)
    assert sum(manifest.row_count for manifest in manifests) == 200
    for venue in VENUES:
        trade_ids: list[str] = []
        for manifest in manifests:
            if manifest.partition.venue != venue:
                continue
            table = pq.ParquetFile(root / manifest.relative_data_path).read(columns=["trade_id", "venue"])
            assert set(table.column("venue").to_pylist()) == {venue}
            trade_ids.extend(str(value) for value in table.column("trade_id").to_pylist())
        assert sorted(trade_ids) == sorted(f"shared-trade-{index}" for index in range(100))

    # The coordinator released the unchanged root lock after its single writer closed.
    reopened = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    reopened.close()


def test_coordinated_writer_fails_closed_on_wrong_or_incompatible_venue(tmp_path: Path) -> None:
    writer = CoordinatedLakeWriter(
        tmp_path / "lake",
        venues=VENUES,
        batch_size=10,
        queue_capacity=20,
    )
    hyperliquid = writer.client("hyperliquid")
    try:
        with pytest.raises(ValueError, match="not configured"):
            writer.client("unexpected")
        with pytest.raises(CoordinatedWriterError, match="venue mismatch"):
            hyperliquid.add(_trade("binance_usdm", 1))
        assert writer.pending_count == 0
    finally:
        hyperliquid.close()
        writer.close()


class _FakeCollector:
    def __init__(self, *, failure: BaseException | None = None) -> None:
        self.failure = failure
        self.started = threading.Event()
        self.stopped = threading.Event()
        self.stop_calls = 0
        self.close_calls = 0

    def run(self, **_kwargs: object) -> None:
        self.started.set()
        if self.failure is not None:
            raise self.failure
        assert self.stopped.wait(timeout=5), "peer failure did not stop this collector"

    def stop(self) -> None:
        self.stop_calls += 1
        self.stopped.set()

    def close(self) -> None:
        self.close_calls += 1


class _FakeWriter:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


def test_multi_venue_runtime_starts_both_and_stops_peer_on_failure() -> None:
    failure = CoordinatedWriterError("simulated incompatible writer failure")
    hyperliquid = _FakeCollector(failure=failure)
    binance = _FakeCollector()
    writer = _FakeWriter()
    runtime = MultiVenueCollector(hyperliquid=hyperliquid, binance=binance, writer=writer)

    with pytest.raises(RuntimeError, match="simulated incompatible writer failure"):
        runtime.run(duration_seconds=60)
    runtime.close()

    assert hyperliquid.started.is_set()
    assert binance.started.is_set()
    assert hyperliquid.stop_calls >= 1
    assert binance.stop_calls >= 1
    assert hyperliquid.close_calls == 1
    assert binance.close_calls == 1
    assert writer.close_calls == 1
