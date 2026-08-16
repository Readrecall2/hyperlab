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
from hyperlab.collector.writer_worker import CoordinatedWriterWorker, WriterWorkerSink
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


def _binance_l2_snapshot_level(level: int) -> ParsedRecord:
    event_time = BASE + timedelta(seconds=1)
    return ParsedRecord(
        RecordType.L2_SNAPSHOT,
        "ETH",
        {
            "schema_version": 1,
            "record_type": RecordType.L2_SNAPSHOT.value,
            "venue": "binance_usdm",
            "asset": "ETH",
            "event_time": event_time,
            "exchange_time": event_time,
            "received_time": event_time + timedelta(milliseconds=500),
            "source_sequence": None,
            "connection_id": "binance-connection",
            "snapshot_id": "ws:binance-connection:1:40862:ETHUSDT:11274459157841",
            "book_epoch_id": "binance-connection:1",
            "last_sequence": 11274459157841,
            "side": "ask" if level >= 20 else "bid",
            "level": level % 20,
            "price": Decimal("1880") + Decimal(level) / Decimal("100"),
            "quantity": Decimal("1"),
            "order_count": None,
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


def test_writer_worker_drains_concurrent_multirow_frames_with_exact_lineage(
    tmp_path: Path,
) -> None:
    root = tmp_path / "worker-lake"
    writer = CoordinatedWriterWorker(
        root,
        venues=VENUES,
        batch_size=200,
        queue_capacity=500,
    )
    hyperliquid = writer.client("hyperliquid")
    binance = writer.client("binance_usdm")
    producers_ready = threading.Barrier(2)

    def ingest(venue: str, sink: WriterWorkerSink) -> tuple[int, int]:
        admitted = 0
        for start in range(0, 100, 4):
            frame = tuple(_trade(venue, index) for index in range(start, start + 4))
            admitted += sink.add_many(frame)
        producers_ready.wait(timeout=5)
        return admitted, sink.flush().row_count

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
        snapshot = writer.metrics_snapshot()
        assert snapshot["outstanding_rows"] == 0
        venues = snapshot["venues"]
        assert isinstance(venues, dict)
        assert venues["hyperliquid"]["frames_processed"] == 25
        assert venues["binance_usdm"]["frames_processed"] == 25
        assert venues["hyperliquid"]["durable_rows"] == 100
        assert venues["binance_usdm"]["durable_rows"] == 100
    finally:
        hyperliquid.close()
        binance.close()
        writer.close()

    manifests = [validate_partition(path) for path in discover_partitions(root)]
    assert sum(manifest.row_count for manifest in manifests) == 200
    observed = {
        (str(row["venue"]), str(row["trade_id"]))
        for manifest in manifests
        for row in pq.ParquetFile(root / manifest.relative_data_path).read().to_pylist()
    }
    assert observed == {(venue, f"shared-trade-{index}") for venue in VENUES for index in range(100)}


def test_coordinated_writer_does_not_flush_inside_one_source_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    writer = CoordinatedLakeWriter(
        root,
        venues=VENUES,
        batch_size=1,
        queue_capacity=100,
    )
    hyperliquid = writer.client("hyperliquid")
    binance = writer.client("binance_usdm")
    records = tuple(_binance_l2_snapshot_level(level) for level in range(40))
    first_add_entered = threading.Event()
    allow_batch_to_finish = threading.Event()
    flush_attempted = threading.Event()
    original_add = writer._sink._add
    add_calls = 0

    def blocking_add(record: ParsedRecord, *, journal: object) -> bool:
        nonlocal add_calls
        accepted = original_add(record, journal=journal)
        add_calls += 1
        if add_calls == 1:
            first_add_entered.set()
            assert allow_batch_to_finish.wait(timeout=5)
        return accepted

    monkeypatch.setattr(writer._sink, "_add", blocking_add)

    def peer_flush() -> object:
        flush_attempted.set()
        return hyperliquid.flush()

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            batch_future = pool.submit(binance.add_many, records)
            assert first_add_entered.wait(timeout=5)
            flush_future = pool.submit(peer_flush)
            assert flush_attempted.wait(timeout=5)
            assert not flush_future.done()
            allow_batch_to_finish.set()
            assert batch_future.result(timeout=5) == 40
            flush_future.result(timeout=5)

        result = binance.flush()
        l2_manifests = [
            manifest
            for manifest in result.manifests
            if manifest.partition.record_type == RecordType.L2_SNAPSHOT
        ]
        assert len(l2_manifests) == 1
        assert l2_manifests[0].row_count == 40
    finally:
        allow_batch_to_finish.set()
        hyperliquid.close()
        binance.close()
        writer.close()


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
