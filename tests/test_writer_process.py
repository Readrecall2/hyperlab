from __future__ import annotations

import contextlib
import multiprocessing as mp
import os
import signal
import threading
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from hyperlab.collector.models import ParsedRecord
from hyperlab.collector.storage import BatchingLakeSink, FlushResult
from hyperlab.collector.writer_process import (
    CoordinatedWriterProcess,
    ProcessWriterError,
)
from hyperlab.collector.writer_worker import WriterQueueCapacityError
from hyperlab.data.lake import discover_partitions, validate_partition
from hyperlab.data.schema import RecordType

BASE = datetime(2026, 8, 14, tzinfo=UTC)
VENUES = ("hyperliquid", "binance_usdm")


def _parent_holding_writer(root: str, ready_queue: Any) -> None:
    worker = CoordinatedWriterProcess(
        Path(root),
        venues=("hyperliquid",),
        batch_size=2,
        queue_capacity=2,
    )
    child = worker.metrics_snapshot()["child_process"]
    assert isinstance(child, dict)
    ready_queue.put(child["pid"])
    while True:
        time.sleep(0.1)


def _l2_frame(
    venue: str,
    snapshot_index: int,
    *,
    levels_per_side: int = 20,
) -> tuple[ParsedRecord, ...]:
    event_time = BASE + timedelta(microseconds=snapshot_index)
    snapshot_id = f"{venue}-snapshot-{snapshot_index}"
    records: list[ParsedRecord] = []
    for side in ("bid", "ask"):
        for level in range(levels_per_side):
            offset = Decimal(level) / Decimal("10")
            price = Decimal("60000") - offset if side == "bid" else Decimal("60000") + offset
            records.append(
                ParsedRecord(
                    RecordType.L2_SNAPSHOT,
                    "BTC",
                    {
                        "schema_version": 1,
                        "record_type": RecordType.L2_SNAPSHOT.value,
                        "venue": venue,
                        "asset": "BTC",
                        "event_time": event_time,
                        "exchange_time": event_time,
                        "received_time": event_time + timedelta(milliseconds=1),
                        "source_sequence": snapshot_index,
                        "connection_id": f"{venue}-connection",
                        "snapshot_id": snapshot_id,
                        "book_epoch_id": f"{venue}-book-1",
                        "last_sequence": snapshot_index,
                        "side": side,
                        "level": level,
                        "price": price,
                        "quantity": Decimal("1.25"),
                        "order_count": None,
                    },
                )
            )
    return tuple(records)


def test_process_writer_owns_root_lock_and_preserves_multi_venue_l2_frames(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    worker = CoordinatedWriterProcess(
        root,
        venues=VENUES,
        batch_size=80,
        queue_capacity=80,
        venue_capacity_rows={
            "hyperliquid": 40,
            "binance_usdm": 40,
        },
    )
    hyperliquid = worker.client("hyperliquid")
    binance = worker.client("binance_usdm")
    try:
        snapshot = worker.metrics_snapshot()
        child = snapshot["child_process"]
        assert isinstance(child, dict)
        assert child["pid"] != os.getpid()
        assert child["start_method"] == "spawn"
        assert snapshot["isolation"] == "spawned_process"

        with pytest.raises(
            RuntimeError,
            match="collector lake already has an active writer",
        ):
            BatchingLakeSink(root, batch_size=1, queue_capacity=1)

        assert hyperliquid.add_many(_l2_frame("hyperliquid", 1)) == 40
        assert binance.add_many(_l2_frame("binance_usdm", 2)) == 40
        hyperliquid_result = hyperliquid.flush()
        binance_result = binance.collect_completed()
        assert hyperliquid_result.row_count == 40
        assert binance_result.row_count == 40
        assert worker.pending_count == 0

        hyperliquid.close()
        binance.close()
        worker.close()
    finally:
        if worker.metrics_snapshot()["failure"] is None:
            worker.close()

    final = worker.metrics_snapshot()
    assert final["closed"] is True
    assert final["accounting_status"] == "exact"
    child = final["child_process"]
    assert isinstance(child, dict)
    assert child["alive"] is False
    assert child["exitcode"] == 0
    assert child["telemetry"] is not None
    assert final["storage"] is not None

    manifests = [validate_partition(path) for path in discover_partitions(root)]
    rows = [
        row
        for manifest in manifests
        for row in pq.ParquetFile(root / manifest.relative_data_path).read().to_pylist()
    ]
    assert len(rows) == 80
    assert {(row["venue"], row["snapshot_id"]) for row in rows} == {
        ("hyperliquid", "hyperliquid-snapshot-1"),
        ("binance_usdm", "binance_usdm-snapshot-2"),
    }

    reopened = BatchingLakeSink(root, batch_size=1, queue_capacity=1)
    reopened.close()


def test_process_writer_rejects_oversized_frame_before_admission(
    tmp_path: Path,
) -> None:
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=4,
        queue_capacity=4,
    )
    sink = worker.client("hyperliquid")
    try:
        with pytest.raises(
            WriterQueueCapacityError,
            match="no record was admitted",
        ):
            sink.add_many(
                _l2_frame(
                    "hyperliquid",
                    1,
                    levels_per_side=3,
                )
            )
        snapshot = worker.metrics_snapshot()
        assert snapshot["outstanding_rows"] == 0
        venues = snapshot["venues"]
        assert isinstance(venues, dict)
        assert venues["hyperliquid"]["capacity_rejections"] == 1
    finally:
        sink.close()
        worker.close()


def test_abrupt_writer_child_death_is_fail_closed_and_releases_root_lock(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    worker = CoordinatedWriterProcess(
        root,
        venues=("hyperliquid",),
        batch_size=40,
        queue_capacity=40,
    )
    sink = worker.client("hyperliquid")
    assert (
        sink.add_many(
            _l2_frame(
                "hyperliquid",
                1,
                levels_per_side=1,
            )
        )
        == 2
    )

    worker._process.terminate()
    worker._process.join(timeout=5)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if worker.metrics_snapshot()["failure"] is not None:
            break
        time.sleep(0.01)
    else:
        raise AssertionError("parent monitor did not detect abrupt child death")

    with pytest.raises(ProcessWriterError, match="child_exit"):
        sink.flush()
    snapshot = worker.metrics_snapshot()
    assert snapshot["accounting_status"] == "indeterminate"
    assert snapshot["outstanding_rows"] >= 0

    with pytest.raises(ProcessWriterError, match="child_exit"):
        worker.close()

    reopened = BatchingLakeSink(root, batch_size=1, queue_capacity=1)
    reopened.close()


def test_cpu_heavy_l2_flush_runs_outside_parent_python_process(
    tmp_path: Path,
) -> None:
    frame = tuple(
        record
        for snapshot_index in range(1, 51)
        for record in _l2_frame(
            "hyperliquid",
            snapshot_index,
            levels_per_side=20,
        )
    )
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=len(frame),
        queue_capacity=len(frame),
    )
    sink = worker.client("hyperliquid")
    assert sink.add_many(frame) == len(frame)

    completed = threading.Event()
    failures: list[BaseException] = []

    def durable_barrier() -> None:
        try:
            result = sink.flush()
            assert result.row_count == len(frame)
        except BaseException as exc:
            failures.append(exc)
        finally:
            completed.set()

    waiter = threading.Thread(target=durable_barrier)
    waiter.start()
    ticks: list[float] = []
    previous = time.monotonic()
    while not completed.wait(0.005):
        observed = time.monotonic()
        ticks.append(observed - previous)
        previous = observed
    waiter.join(timeout=5)

    assert failures == []
    assert not waiter.is_alive()
    assert worker.metrics_snapshot()["child_process"]["pid"] != os.getpid()
    assert ticks
    assert max(ticks) < 0.25

    sink.close()
    worker.close()
    for path in discover_partitions(tmp_path / "lake"):
        validate_partition(path)


def test_result_transport_failure_is_fail_closed_without_hanging_waiters(
    tmp_path: Path,
) -> None:
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=2,
        queue_capacity=2,
    )
    sink = worker.client("hyperliquid")
    try:
        worker._result_queue.close()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            failure = worker.metrics_snapshot()["failure"]
            if failure is not None:
                break
            time.sleep(0.01)
        else:
            raise AssertionError("result transport failure was not observed")

        assert isinstance(failure, dict)
        assert failure["phase"] == "result_transport"
        with pytest.raises(ProcessWriterError, match="result_transport"):
            sink.flush()
        with pytest.raises(ProcessWriterError, match="result_transport"):
            worker.close()
    finally:
        if worker._process.is_alive():
            worker._cleanup_failed_process()


def test_writer_child_exits_and_releases_root_lock_after_parent_is_killed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    context = mp.get_context("spawn")
    ready_queue = context.Queue()
    parent = context.Process(
        target=_parent_holding_writer,
        args=(str(root), ready_queue),
        daemon=False,
    )
    child_pid: int | None = None
    lock_reacquired = False
    try:
        parent.start()
        child_pid = ready_queue.get(timeout=30)
        assert isinstance(child_pid, int)
        with pytest.raises(
            RuntimeError,
            match="collector lake already has an active writer",
        ):
            BatchingLakeSink(root, batch_size=1, queue_capacity=1)

        parent.terminate()
        parent.join(timeout=5)
        if parent.is_alive():
            parent.kill()
            parent.join(timeout=5)
        assert not parent.is_alive()

        deadline = time.monotonic() + 20
        while True:
            try:
                reopened = BatchingLakeSink(
                    root,
                    batch_size=1,
                    queue_capacity=1,
                )
            except RuntimeError as exc:
                if "active writer" not in str(exc) or time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
                continue
            reopened.close()
            lock_reacquired = True
            break
    finally:
        if parent.is_alive():
            parent.kill()
            parent.join(timeout=5)
        with contextlib.suppress(AttributeError, OSError, ValueError):
            ready_queue.cancel_join_thread()
        with contextlib.suppress(AttributeError, OSError, ValueError):
            ready_queue.close()
        if child_pid is not None and not lock_reacquired:
            kill_signal = getattr(signal, "SIGKILL", signal.SIGTERM)
            with contextlib.suppress(OSError, ProcessLookupError):
                os.kill(child_pid, kill_signal)


def test_process_writer_metrics_are_isolated_and_staleness_is_bounded(
    tmp_path: Path,
) -> None:
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=2,
        queue_capacity=2,
    )
    try:
        with worker._condition:
            snapshot = worker.metrics_snapshot()
            child = snapshot["child_process"]
            assert isinstance(child, dict)
            telemetry = child["telemetry"]
            assert isinstance(telemetry, dict)
            telemetry["caller_mutation"] = True
            cached = worker._child_cache
            assert isinstance(cached, dict)
            cached_process = cached["process"]
            assert isinstance(cached_process, dict)
            assert "caller_mutation" not in cached_process

            worker._child_cache_received_ns = worker._monotonic_ns() - 3_000_000_000
            stale = worker.metrics_snapshot()["child_process"]
            assert isinstance(stale, dict)
            assert stale["cache_current"] is False
            assert stale["cache_stale"] is True
    finally:
        worker.close()


def test_process_writer_releases_duplicate_capacity_and_credits_once(
    tmp_path: Path,
) -> None:
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=80,
        queue_capacity=80,
    )
    sink = worker.client("hyperliquid")
    frame = _l2_frame("hyperliquid", 1)
    try:
        assert sink.add_many(frame) == 40
        assert sink.add_many(frame) == 40
        result = sink.flush()
        assert result.row_count == 40
        assert result.duplicate_count == 40
        assert worker.pending_count == 0
        assert sink.collect_completed() == FlushResult((), 0, 0)
    finally:
        sink.close()
        worker.close()


def test_process_writer_startup_fails_closed_when_root_lock_is_held(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    owner = BatchingLakeSink(root, batch_size=1, queue_capacity=1)
    try:
        with pytest.raises(
            ProcessWriterError,
            match="collector lake already has an active writer",
        ):
            CoordinatedWriterProcess(
                root,
                venues=("hyperliquid",),
                batch_size=2,
                queue_capacity=2,
                startup_timeout_seconds=5,
            )
    finally:
        owner.close()


def test_post_ack_child_join_timeout_is_bounded_and_accounting_stays_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=2,
        queue_capacity=2,
    )
    original_join = worker._process.join
    original_is_alive = worker._process.is_alive
    original_kill = worker._process.kill
    forced_join_seen = False
    synthetic_alive_reported = False

    def bounded_join(timeout: float | None = None) -> None:
        nonlocal forced_join_seen
        if timeout == 5.0 and not forced_join_seen:
            forced_join_seen = True
            return
        original_join(timeout=timeout)

    def synthetic_is_alive() -> bool:
        nonlocal synthetic_alive_reported
        if forced_join_seen and not synthetic_alive_reported:
            synthetic_alive_reported = True
            return True
        return original_is_alive()

    monkeypatch.setattr(worker._process, "join", bounded_join)
    monkeypatch.setattr(worker._process, "is_alive", synthetic_is_alive)
    started = time.monotonic()
    try:
        with pytest.raises(ProcessWriterError, match="child_shutdown"):
            worker.close()
        assert time.monotonic() - started < 10
        snapshot = worker.metrics_snapshot()
        assert snapshot["accounting_status"] == "exact"
        failure = snapshot["failure"]
        assert isinstance(failure, dict)
        assert failure["phase"] == "child_shutdown"
    finally:
        if original_is_alive():
            original_kill()
            original_join(timeout=5)


def test_nonblocking_flush_request_makes_sub_batch_rows_durable(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    worker = CoordinatedWriterProcess(
        root,
        venues=("hyperliquid",),
        batch_size=40,
        queue_capacity=40,
    )
    sink = worker.client("hyperliquid")
    try:
        assert (
            sink.add_many(
                _l2_frame(
                    "hyperliquid",
                    1,
                    levels_per_side=1,
                )
            )
            == 2
        )
        assert sink.request_flush() is True
        assert (
            sink.add_many(
                _l2_frame(
                    "hyperliquid",
                    2,
                    levels_per_side=1,
                )
            )
            == 2
        )
        assert sink.request_flush() is True

        deadline = time.monotonic() + 10
        durable_rows = 0
        durable_duplicates = 0
        while time.monotonic() < deadline:
            observed = sink.collect_completed()
            durable_rows += observed.row_count
            durable_duplicates += observed.duplicate_count
            if durable_rows == 4:
                break
            time.sleep(0.01)
        else:
            raise AssertionError("nonblocking barrier did not become durable")

        assert durable_rows == 4
        assert durable_duplicates == 0
        assert worker.pending_count == 0
        assert sink.request_flush() is False
        manifests = [validate_partition(path) for path in discover_partitions(root)]
        assert sum(manifest.row_count for manifest in manifests) == 4
    finally:
        sink.close()
        worker.close()
