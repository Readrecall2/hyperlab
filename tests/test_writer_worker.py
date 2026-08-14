from __future__ import annotations

import threading
import time
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pyarrow.parquet as pq
import pytest

from hyperlab.collector.models import ParsedRecord
from hyperlab.collector.storage import (
    BatchingLakeSink,
    CoordinatedLakeWriter,
    CoordinatedWriterError,
    FlushResult,
)
from hyperlab.collector.writer_worker import (
    CoordinatedWriterWorker,
    WriterQueueCapacityError,
    WriterWorkerError,
)
from hyperlab.data.lake import PartitionManifest, discover_partitions, validate_partition
from hyperlab.data.schema import RecordType

BASE = datetime(2026, 8, 14, 0, tzinfo=UTC)
VENUES = ("hyperliquid", "binance_usdm")


def _trade(venue: str, sequence: int) -> ParsedRecord:
    event_time = BASE + timedelta(microseconds=sequence)
    return ParsedRecord(
        RecordType.TRADE,
        "BTC",
        {
            "schema_version": 2,
            "record_type": RecordType.TRADE.value,
            "venue": venue,
            "asset": "BTC",
            "event_time": event_time,
            "exchange_time": event_time,
            "received_time": event_time + timedelta(milliseconds=1),
            "source_sequence": sequence,
            "connection_id": f"{venue}-connection",
            "trade_id": f"{venue}-trade-{sequence}",
            "aggressor_side": "buy",
            "price": Decimal("60000"),
            "quantity": Decimal("0.001"),
            "quote_quantity": Decimal("60"),
            "is_liquidation": None,
            "connection_epoch": 1,
            "arrival_sequence": sequence + 1,
        },
    )


def test_batch_readiness_uses_one_exact_group_instead_of_global_pending_rows(
    tmp_path: Path,
) -> None:
    sink = BatchingLakeSink(
        tmp_path / "lake",
        batch_size=3,
        queue_capacity=12,
    )
    try:
        assert sink.add(_trade("hyperliquid", 1)) is True
        assert sink.add(_trade("binance_usdm", 2)) is True
        assert sink.add(_trade("hyperliquid", 3)) is True
        assert sink.add(_trade("binance_usdm", 4)) is True

        assert sink.pending_count == 4
        assert sink.should_flush is False
        assert sink.metrics_snapshot()["coalescing"] == {
            "readiness": "exact_group",
            "pending_groups": 2,
            "ready_groups": 0,
            "max_group_rows": 2,
        }

        assert sink.add(_trade("hyperliquid", 5)) is True
        assert sink.pending_count == 5
        assert sink.should_flush is True
        assert sink.metrics_snapshot()["coalescing"] == {
            "readiness": "exact_group",
            "pending_groups": 2,
            "ready_groups": 1,
            "max_group_rows": 3,
        }

        ready = sink.flush_ready()
        assert ready.row_count == 3
        assert ready.duplicate_count == 0
        assert len(ready.manifests) == 1
        assert ready.manifests[0].partition.venue == "hyperliquid"
        assert sink.pending_count == 2
        assert sink.should_flush is False

        residual = sink.flush()
        assert residual.row_count == 2
        assert residual.duplicate_count == 0
        assert len(residual.manifests) == 1
        assert residual.manifests[0].partition.venue == "binance_usdm"
    finally:
        sink.close()

    reopened = BatchingLakeSink(
        tmp_path / "lake",
        batch_size=3,
        queue_capacity=12,
    )
    try:
        assert reopened.add(_trade("hyperliquid", 1)) is False
        assert reopened.add(_trade("binance_usdm", 2)) is False
        duplicates = reopened.flush()
        assert duplicates.row_count == 0
        assert duplicates.duplicate_count == 2
    finally:
        reopened.close()


def test_coordinated_writer_exposes_physical_exact_group_readiness(
    tmp_path: Path,
) -> None:
    writer = CoordinatedLakeWriter(
        tmp_path / "lake",
        venues=VENUES,
        batch_size=3,
        queue_capacity=12,
    )
    hyperliquid = writer.client("hyperliquid")
    binance = writer.client("binance_usdm")
    try:
        assert hyperliquid.add_many((_trade("hyperliquid", 1), _trade("hyperliquid", 2))) == 2
        assert binance.add_many((_trade("binance_usdm", 3), _trade("binance_usdm", 4))) == 2

        assert writer.pending_count == 4
        assert writer.should_flush is False

        assert hyperliquid.add(_trade("hyperliquid", 5)) is True
        assert writer.should_flush is True

        ready = writer.flush_ready_all()
        assert ready["hyperliquid"].row_count == 3
        assert ready["hyperliquid"].duplicate_count == 0
        assert len(ready["hyperliquid"].manifests) == 1
        assert ready["binance_usdm"] == FlushResult((), 0, 0)
        assert writer.pending_count == 2
        assert writer.should_flush is False

        residual = writer.flush_all()
        assert residual["hyperliquid"] == FlushResult((), 0, 0)
        assert residual["binance_usdm"].row_count == 2
        assert residual["binance_usdm"].duplicate_count == 0
        assert len(residual["binance_usdm"].manifests) == 1
        assert writer.pending_count == 0
    finally:
        hyperliquid.close()
        binance.close()
        writer.close()


def test_blocked_physical_flush_keeps_enqueue_nonblocking_and_capacity_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    worker = CoordinatedWriterWorker(
        root,
        venues=VENUES,
        batch_size=1,
        queue_capacity=2,
    )
    hyperliquid = worker.client("hyperliquid")
    binance = worker.client("binance_usdm")
    assert hyperliquid.should_flush is False
    assert binance.should_flush is False
    physical_sink = worker._writer._sink
    original_flush = physical_sink.flush_ready
    flush_entered = threading.Event()
    allow_flush = threading.Event()

    def blocked_flush() -> object:
        flush_entered.set()
        assert allow_flush.wait(timeout=5), "test did not release the physical flush"
        return original_flush()

    monkeypatch.setattr(physical_sink, "flush_ready", blocked_flush)
    enqueue_completed = threading.Event()
    enqueue_errors: list[BaseException] = []

    def enqueue_peer() -> None:
        try:
            assert binance.add(_trade("binance_usdm", 2)) is True
        except BaseException as exc:
            enqueue_errors.append(exc)
        finally:
            enqueue_completed.set()

    try:
        assert hyperliquid.add(_trade("hyperliquid", 1)) is True
        assert flush_entered.wait(timeout=5), "worker never entered the physical flush"

        producer = threading.Thread(target=enqueue_peer, name="test-peer-producer")
        producer.start()
        assert enqueue_completed.wait(timeout=1), "producer blocked behind physical storage"
        producer.join(timeout=1)
        assert not producer.is_alive()
        assert enqueue_errors == []

        with pytest.raises(WriterQueueCapacityError, match="no record was admitted"):
            hyperliquid.add(_trade("hyperliquid", 3))

        blocked = worker.metrics_snapshot()
        assert blocked["outstanding_rows"] == 2
        assert worker.pending_count == 2
        assert hyperliquid.pending_count == 1
        assert binance.pending_count == 1
        blocked_venues = blocked["venues"]
        assert isinstance(blocked_venues, dict)
        assert blocked_venues["hyperliquid"]["high_water_rows"] == 1
        assert blocked_venues["binance_usdm"]["high_water_rows"] == 1
        assert blocked_venues["hyperliquid"]["capacity_rejections"] == 1

        allow_flush.set()
        hyperliquid_result = hyperliquid.flush()
        binance_result = binance.collect_completed()

        assert hyperliquid_result.row_count == 1
        assert binance_result.row_count == 1
        assert binance.collect_completed() == type(binance_result)((), 0, 0)
        assert worker.pending_count == 0

        snapshot = worker.metrics_snapshot()
        assert snapshot["failure"] is None
        assert snapshot["outstanding_rows"] == 0
        venues = snapshot["venues"]
        assert isinstance(venues, dict)
        for venue in VENUES:
            assert venues[venue]["frames_enqueued"] == 1
            assert venues[venue]["frames_processed"] == 1
            assert venues[venue]["durable_rows"] == 1
            assert venues[venue]["enqueue_delay_ms"]["count"] >= 1
            assert venues[venue]["queue_residence_ms"]["count"] == 1
            assert venues[venue]["add_ms"]["count"] == 1
            assert venues[venue]["write_ms"]["count"] == 1
        assert snapshot["flush_ms"]["count"] >= 3

        hyperliquid.close()
        binance.close()
        worker.close()
    finally:
        allow_flush.set()
        if worker._thread.is_alive():
            worker.close()

    manifests = [validate_partition(path) for path in discover_partitions(root)]
    assert sum(manifest.row_count for manifest in manifests) == 2
    observed = {
        (str(row["venue"]), str(row["trade_id"]))
        for manifest in manifests
        for row in pq.ParquetFile(root / manifest.relative_data_path).read().to_pylist()
    }
    assert observed == {
        ("hyperliquid", "hyperliquid-trade-1"),
        ("binance_usdm", "binance_usdm-trade-2"),
    }


def test_async_flush_failure_surfaces_and_releases_the_single_root_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    worker = CoordinatedWriterWorker(
        root,
        venues=VENUES,
        batch_size=1,
        queue_capacity=2,
    )
    hyperliquid = worker.client("hyperliquid")
    flush_failed = threading.Event()

    def fail_flush() -> object:
        flush_failed.set()
        raise OSError("simulated fsync failure")

    monkeypatch.setattr(worker._writer._sink, "flush_ready", fail_flush)
    monkeypatch.setattr(worker._writer._sink, "flush", fail_flush)

    assert hyperliquid.add(_trade("hyperliquid", 1)) is True
    assert flush_failed.wait(timeout=5), "worker never attempted its automatic flush"

    with pytest.raises(
        WriterWorkerError,
        match=r"auto_flush.*CoordinatedWriterError",
    ) as raised:
        hyperliquid.flush()
    physical_failure = raised.value.__cause__
    assert physical_failure is not None
    assert isinstance(physical_failure.__cause__, OSError)
    assert str(physical_failure.__cause__) == "simulated fsync failure"

    consumed = False

    def must_not_be_consumed() -> Iterator[ParsedRecord]:
        nonlocal consumed
        consumed = True
        yield _trade("binance_usdm", 2)

    with pytest.raises(
        WriterWorkerError,
        match=r"auto_flush.*CoordinatedWriterError",
    ):
        hyperliquid.add_many(must_not_be_consumed())
    assert consumed is False

    snapshot = worker.metrics_snapshot()
    assert snapshot["failure"] == {
        "phase": "auto_flush",
        "type": "CoordinatedWriterError",
        "message": "coordinated lake ready-group flush failed for all venues",
    }
    assert snapshot["outstanding_rows"] == 1
    assert snapshot["flush_ms"]["count"] >= 1

    with pytest.raises(
        WriterWorkerError,
        match=r"auto_flush.*CoordinatedWriterError",
    ):
        worker.close()
    assert not worker._thread.is_alive()
    assert list(root.rglob("*.parquet")) == []

    # Failure remains visible, but deterministic cleanup released the sole
    # process/root lock so a new fail-closed writer can start.
    reopened = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    reopened.close()


def test_async_failure_landing_during_freeze_wins_over_venue_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    worker = CoordinatedWriterWorker(
        root,
        venues=VENUES,
        batch_size=1,
        queue_capacity=2,
    )
    hyperliquid = worker.client("hyperliquid")
    flush_entered = threading.Event()
    allow_failure = threading.Event()

    def fail_flush() -> object:
        flush_entered.set()
        assert allow_failure.wait(timeout=5), "test did not release the failed flush"
        raise OSError("simulated fsync race")

    monkeypatch.setattr(worker._writer._sink, "flush_ready", fail_flush)
    monkeypatch.setattr(worker._writer._sink, "flush", fail_flush)
    assert hyperliquid.add(_trade("hyperliquid", 1)) is True
    assert flush_entered.wait(timeout=5), "worker never entered automatic flush"

    original_freeze = worker._freeze_frame

    def freeze_after_failure(
        records: Iterable[ParsedRecord],
    ) -> tuple[ParsedRecord, ...]:
        frozen = original_freeze(records)
        allow_failure.set()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if worker.metrics_snapshot()["failure"] is not None:
                return frozen
            time.sleep(0.001)
        raise AssertionError("worker did not record the async failure")

    monkeypatch.setattr(worker, "_freeze_frame", freeze_after_failure)
    try:
        with pytest.raises(
            WriterWorkerError,
            match=r"auto_flush.*CoordinatedWriterError",
        ):
            hyperliquid.add(_trade("binance_usdm", 2))
    finally:
        allow_failure.set()

    with pytest.raises(
        WriterWorkerError,
        match=r"auto_flush.*CoordinatedWriterError",
    ):
        worker.close()
    assert not worker._thread.is_alive()


def test_idle_worker_reports_waiting_and_terminal_phase(tmp_path: Path) -> None:
    worker = CoordinatedWriterWorker(
        tmp_path / "lake",
        venues=("hyperliquid",),
    )
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if worker.metrics_snapshot()["active_phase"] == "waiting":
                break
            time.sleep(0.001)
        else:
            raise AssertionError("worker did not report its condition wait")
    finally:
        worker.close()

    assert worker.metrics_snapshot()["active_phase"] == "closed"


def test_record_flush_validation_is_atomic_before_credit_mutation(
    tmp_path: Path,
) -> None:
    writer = CoordinatedLakeWriter(
        tmp_path / "lake",
        venues=VENUES,
        batch_size=10,
        queue_capacity=20,
    )
    valid_manifest = cast(
        PartitionManifest,
        SimpleNamespace(
            partition=SimpleNamespace(venue="hyperliquid"),
            row_count=1,
        ),
    )
    incompatible_manifest = cast(
        PartitionManifest,
        SimpleNamespace(
            partition=SimpleNamespace(venue="unexpected"),
            row_count=1,
        ),
    )
    before_manifests = {venue: tuple(manifests) for venue, manifests in writer._manifest_credit.items()}
    before_rows = writer._row_credit.copy()
    before_duplicates = writer._duplicate_credit.copy()
    before_pending = writer._pending_by_venue.copy()
    before_duplicates_since_flush = writer._duplicates_since_flush.copy()

    try:
        with pytest.raises(
            CoordinatedWriterError,
            match="published an incompatible venue",
        ):
            writer._record_flush(
                FlushResult(
                    (valid_manifest, incompatible_manifest),
                    row_count=2,
                    duplicate_count=0,
                ),
                full_barrier=True,
            )

        assert {
            venue: tuple(manifests) for venue, manifests in writer._manifest_credit.items()
        } == before_manifests
        assert writer._row_credit == before_rows
        assert writer._duplicate_credit == before_duplicates
        assert writer._pending_by_venue == before_pending
        assert writer._duplicates_since_flush == before_duplicates_since_flush
    finally:
        writer.close()


def test_coordinated_flush_all_uses_one_physical_flush_and_drains_every_venue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = CoordinatedLakeWriter(
        tmp_path / "lake",
        venues=VENUES,
        batch_size=10,
        queue_capacity=20,
    )
    hyperliquid = writer.client("hyperliquid")
    binance = writer.client("binance_usdm")
    original_flush = writer._sink.flush
    physical_flushes = 0

    def counted_flush() -> FlushResult:
        nonlocal physical_flushes
        physical_flushes += 1
        return original_flush()

    monkeypatch.setattr(writer._sink, "flush", counted_flush)
    try:
        assert hyperliquid.add(_trade("hyperliquid", 1)) is True
        assert binance.add(_trade("binance_usdm", 2)) is True

        results = writer.flush_all()

        assert list(results) == list(VENUES)
        assert results["hyperliquid"].row_count == 1
        assert results["binance_usdm"].row_count == 1
        assert physical_flushes == 1
        assert writer.pending_count == 0
    finally:
        hyperliquid.close()
        binance.close()
        writer.close()


def test_default_venue_budgets_reserve_peer_capacity_and_report_high_water(
    tmp_path: Path,
) -> None:
    worker = CoordinatedWriterWorker(
        tmp_path / "lake",
        venues=VENUES,
        batch_size=4,
        queue_capacity=8,
    )
    hyperliquid = worker.client("hyperliquid")
    binance = worker.client("binance_usdm")
    try:
        assert (
            hyperliquid.add_many(
                (
                    _trade("hyperliquid", 1),
                    _trade("hyperliquid", 2),
                )
            )
            == 2
        )
        with pytest.raises(
            WriterQueueCapacityError,
            match=r"venue outstanding-row capacity exceeded.*hyperliquid",
        ):
            hyperliquid.add_many(
                (
                    _trade("hyperliquid", 3),
                    _trade("hyperliquid", 4),
                    _trade("hyperliquid", 5),
                )
            )

        assert (
            binance.add_many(
                (
                    _trade("binance_usdm", 6),
                    _trade("binance_usdm", 7),
                )
            )
            == 2
        )

        hyperliquid_result = hyperliquid.flush()
        binance_result = binance.collect_completed()
        assert hyperliquid_result.row_count == 2
        assert binance_result.row_count == 2

        snapshot = worker.metrics_snapshot()
        assert snapshot["outstanding_high_water_rows"] == 4
        venues = snapshot["venues"]
        assert isinstance(venues, dict)
        assert venues["hyperliquid"]["capacity_rows"] == 4
        assert venues["binance_usdm"]["capacity_rows"] == 4
        assert venues["hyperliquid"]["capacity_rejections"] == 1
        storage = snapshot["storage"]
        assert isinstance(storage, dict)
        flushes = storage["flushes"]
        assert isinstance(flushes, dict)
        assert flushes["attempted"] == 1
    finally:
        hyperliquid.close()
        binance.close()
        worker.close()


def test_explicit_venue_budgets_must_exactly_partition_global_capacity(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="exactly the configured venues"):
        CoordinatedWriterWorker(
            tmp_path / "missing-venue",
            venues=VENUES,
            queue_capacity=4,
            batch_size=4,
            venue_capacity_rows={"hyperliquid": 4},
        )
    with pytest.raises(ValueError, match="sum exactly"):
        CoordinatedWriterWorker(
            tmp_path / "wrong-total",
            venues=VENUES,
            queue_capacity=4,
            batch_size=4,
            venue_capacity_rows={
                "hyperliquid": 1,
                "binance_usdm": 1,
            },
        )


@pytest.mark.parametrize(
    "row_counts",
    (
        {"hyperliquid": 0, "binance_usdm": 1},
        {"hyperliquid": 1, "binance_usdm": 2},
    ),
    ids=("first-venue-undercount", "later-venue-overcount"),
)
def test_worker_flush_validates_all_results_before_any_credit_or_barrier_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    row_counts: dict[str, int],
) -> None:
    worker = CoordinatedWriterWorker(
        tmp_path / "lake",
        venues=VENUES,
        batch_size=4,
        queue_capacity=4,
    )
    hyperliquid = worker.client("hyperliquid")
    binance = worker.client("binance_usdm")

    def invalid_flush_all() -> dict[str, FlushResult]:
        return {venue: FlushResult((), row_counts[venue], 0) for venue in VENUES}

    monkeypatch.setattr(worker._writer, "flush_all", invalid_flush_all)
    assert hyperliquid.add(_trade("hyperliquid", 1)) is True
    assert binance.add(_trade("binance_usdm", 2)) is True

    with pytest.raises(
        WriterWorkerError,
        match=r"barrier_flush.*did not match accepted pending rows",
    ):
        hyperliquid.flush()

    assert worker._accepted_pending_by_venue == {
        "hyperliquid": 1,
        "binance_usdm": 1,
    }
    assert worker._durable_rows_by_venue == {
        "hyperliquid": 0,
        "binance_usdm": 0,
    }
    assert worker._outstanding_rows == 2
    for credits in worker._completed.values():
        assert credits.manifests == []
        assert credits.row_count == 0
        assert credits.duplicate_count == 0

    with pytest.raises(
        WriterWorkerError,
        match=r"barrier_flush.*did not match accepted pending rows",
    ):
        worker.close()
    assert not worker._thread.is_alive()


def test_nonblocking_flush_requests_cover_frames_added_after_older_barrier(
    tmp_path: Path,
) -> None:
    worker = CoordinatedWriterWorker(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=4,
        queue_capacity=4,
    )
    sink = worker.client("hyperliquid")
    try:
        assert sink.add(_trade("hyperliquid", 1)) is True
        assert sink.request_flush() is True
        assert sink.add(_trade("hyperliquid", 2)) is True
        assert sink.request_flush() is True

        deadline = time.monotonic() + 5
        durable_rows = 0
        while time.monotonic() < deadline:
            durable_rows += sink.collect_completed().row_count
            if durable_rows == 2:
                break
            time.sleep(0.001)
        else:
            raise AssertionError("nonblocking worker barriers did not flush")

        assert worker.pending_count == 0
        assert sink.request_flush() is False
    finally:
        sink.close()
        worker.close()
