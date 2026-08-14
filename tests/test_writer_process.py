from __future__ import annotations

import contextlib
import json
import multiprocessing as mp
import os
import signal
import threading
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from hyperlab.collector.models import ParsedRecord, WireEnvelope
from hyperlab.collector.storage import BatchingLakeSink, FlushResult
from hyperlab.collector.writer_process import (
    CoordinatedWriterProcess,
    ProcessWriterError,
)
from hyperlab.collector.writer_worker import WriterQueueCapacityError
from hyperlab.data.lake import discover_partitions, validate_partition
from hyperlab.data.schema import RecordType
from hyperlab.venues.base import measure_clock
from hyperlab.venues.binance import BinancePublicConnector, clock_record

BASE = datetime(2026, 8, 14, tzinfo=UTC)
VENUES = ("hyperliquid", "binance_usdm")
BINANCE_STRESS_CAPTURE = "binance-stress-capture-1"


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
    asset: str = "BTC",
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
                    asset,
                    {
                        "schema_version": 1,
                        "record_type": RecordType.L2_SNAPSHOT.value,
                        "venue": venue,
                        "asset": asset,
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
            "connection_id": f"{venue}-trade-connection",
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


def _binance_connector() -> BinancePublicConnector:
    symbols = []
    for asset in ("BTC", "ETH"):
        symbols.append(
            {
                "symbol": f"{asset}USDT",
                "pair": f"{asset}USDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "baseAsset": asset,
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                ],
            }
        )
    return BinancePublicConnector.from_exchange_info({"symbols": symbols}, ("BTC", "ETH"))


def _binance_depth_frame(
    connector: BinancePublicConnector,
    snapshot_index: int,
    asset: str,
) -> tuple[ParsedRecord, ...]:
    symbol = f"{asset}USDT"
    event_ms = int(BASE.timestamp() * 1_000) + snapshot_index
    last_sequence = 10_000 + snapshot_index
    payload = {
        "stream": f"{asset.lower()}usdt@depth20@100ms",
        "data": {
            "e": "depthUpdate",
            "E": event_ms,
            "T": event_ms,
            "s": symbol,
            "U": last_sequence,
            "u": last_sequence,
            "pu": last_sequence - 1,
            "b": [[str(60_000 - level), "1.25"] for level in range(20)],
            "a": [[str(60_001 + level), "1.25"] for level in range(20)],
        },
    }
    received_time = BASE + timedelta(microseconds=snapshot_index)
    envelope = WireEnvelope(
        json.dumps(payload, separators=(",", ":")),
        received_time,
        "binance-public-depth-stress",
        1,
        snapshot_index + 1,
        BINANCE_STRESS_CAPTURE,
    )
    parsed = connector.parse_message(envelope)
    assert len(parsed.records) == 43
    return parsed.records


def _binance_clock_sample(sample_index: int) -> ParsedRecord:
    request_sent_time = BASE + timedelta(seconds=sample_index * 5)
    response_received_time = request_sent_time + timedelta(milliseconds=80)
    measurement = measure_clock(
        "binance_usdm",
        request_sent_time=request_sent_time,
        response_received_time=response_received_time,
        server_time=request_sent_time + timedelta(milliseconds=40),
    )
    return clock_record(
        measurement,
        f"binance-clock-{sample_index}",
        connection_id="binance-public-depth-stress",
        connection_epoch=1,
        capture_epoch_id=BINANCE_STRESS_CAPTURE,
        sampling_interval=timedelta(seconds=5),
    )


def test_process_writer_coalesces_sustained_multi_group_frames_before_publication(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    worker = CoordinatedWriterProcess(
        root,
        venues=VENUES,
        batch_size=500,
        queue_capacity=20_000,
        venue_capacity_rows={
            "hyperliquid": 10_000,
            "binance_usdm": 10_000,
        },
    )
    hyperliquid = worker.client("hyperliquid")
    binance = worker.client("binance_usdm")
    assert hyperliquid.should_flush is False
    assert binance.should_flush is False
    connector = _binance_connector()
    try:
        for snapshot_index in range(452):
            asset = "BTC" if snapshot_index % 2 == 0 else "ETH"
            assert hyperliquid.add(_trade("hyperliquid", snapshot_index)) is True
            assert (
                binance.add_many(
                    _binance_depth_frame(
                        connector,
                        snapshot_index,
                        asset,
                    )
                )
                == 43
            )
            if snapshot_index % 100 == 0:
                assert binance.add(_binance_clock_sample(snapshot_index // 100)) is True
            if (snapshot_index + 1) % 100 == 0:
                # At the nominal two-symbol depth20@100ms rate, 100 frames are
                # five seconds. Exercise the live collector's unchanged FIFO
                # durability barrier while keeping test ingress accelerated.
                assert binance.request_flush() is True
            # Two depth20@100ms symbols nominally produce 20 logical frames/s;
            # 10 ms pacing sustains five times that ingress rate.
            time.sleep(0.01)

        result = binance.flush()
        peer_result = hyperliquid.collect_completed()
        assert result.row_count == 19_441
        assert peer_result.row_count == 452

        snapshot = worker.metrics_snapshot()
        assert snapshot["failure"] is None
        assert snapshot["outstanding_rows"] == 0
        assert snapshot["outstanding_high_water_rows"] < 8_000
        venues = snapshot["venues"]
        assert isinstance(venues, dict)
        assert venues["binance_usdm"]["capacity_rows"] == 10_000
        assert venues["binance_usdm"]["capacity_rejections"] == 0
        assert venues["binance_usdm"]["high_water_rows"] < 7_000
        assert venues["binance_usdm"]["frames_processed"] == 457
        assert venues["binance_usdm"]["durable_rows"] == 19_441
        assert venues["binance_usdm"]["queue_residence_ms"]["count"] == 457
        assert venues["binance_usdm"]["queue_residence_ms"]["max_ms"] < 5_000
        assert venues["hyperliquid"]["capacity_rows"] == 10_000
        assert venues["hyperliquid"]["capacity_rejections"] == 0
        assert venues["hyperliquid"]["frames_processed"] == 452
        assert venues["hyperliquid"]["durable_rows"] == 452
        storage = snapshot["storage"]
        assert isinstance(storage, dict)
        assert storage["coalescing"] == {
            "readiness": "exact_group",
            "pending_groups": 0,
            "ready_groups": 0,
            "max_group_rows": 0,
        }
        assert storage["written"] == {
            "rows": 19_893,
            "partitions": 71,
        }
    finally:
        hyperliquid.close()
        binance.close()
        worker.close()

    manifests = [validate_partition(path) for path in discover_partitions(root)]
    assert len(manifests) == 71
    assert sum(manifest.row_count for manifest in manifests) == 19_893
    manifest_groups = Counter(
        (
            manifest.partition.venue,
            manifest.partition.record_type,
            manifest.partition.asset,
        )
        for manifest in manifests
    )
    assert manifest_groups == Counter(
        {
            ("binance_usdm", RecordType.L2_SNAPSHOT, "BTC"): 18,
            ("binance_usdm", RecordType.L2_SNAPSHOT, "ETH"): 18,
            ("binance_usdm", RecordType.WIRE_MESSAGE, "GLOBAL"): 5,
            ("binance_usdm", RecordType.BBO, "BTC"): 5,
            ("binance_usdm", RecordType.BBO, "ETH"): 5,
            ("binance_usdm", RecordType.L2_BOOK_STATE, "BTC"): 5,
            ("binance_usdm", RecordType.L2_BOOK_STATE, "ETH"): 5,
            ("binance_usdm", RecordType.CLOCK_SYNC, "GLOBAL"): 5,
            ("hyperliquid", RecordType.TRADE, "BTC"): 5,
        }
    )

    rows_by_type: dict[RecordType, list[dict[str, object]]] = {}
    for manifest in manifests:
        rows_by_type.setdefault(manifest.partition.record_type, []).extend(
            pq.ParquetFile(root / manifest.relative_data_path).read().to_pylist()
        )
    assert {record_type: len(rows) for record_type, rows in rows_by_type.items()} == {
        RecordType.WIRE_MESSAGE: 452,
        RecordType.BBO: 452,
        RecordType.L2_BOOK_STATE: 452,
        RecordType.L2_SNAPSHOT: 18_080,
        RecordType.TRADE: 452,
        RecordType.CLOCK_SYNC: 5,
    }

    clock_rows = rows_by_type[RecordType.CLOCK_SYNC]
    expected_binance_lineage = {
        ("binance-public-depth-stress", 1, BINANCE_STRESS_CAPTURE)
    }
    assert {
        (row["connection_id"], row["connection_epoch"], row["capture_epoch_id"])
        for row in rows_by_type[RecordType.WIRE_MESSAGE]
    } == expected_binance_lineage
    assert {
        (row["connection_id"], row["connection_epoch"], row["capture_epoch_id"])
        for row in clock_rows
    } == expected_binance_lineage
    assert {row["observation_id"] for row in clock_rows} == {
        f"binance-clock-{sample_index}" for sample_index in range(5)
    }
    assert {row["sample_status"] for row in clock_rows} == {"valid"}
    assert {row["drift_uncertainty_ms"] for row in clock_rows} == {Decimal("40")}
    assert all(
        row["causal_valid_from"] == row["response_received_time"]
        and row["causal_valid_until"]
        == row["response_received_time"] + timedelta(seconds=15)
        for row in clock_rows
    )

    def lineage(row: dict[str, object]) -> tuple[object, object]:
        return (
            row["connection_id"],
            row["received_time"],
        )

    wire_asset_by_lineage = {
        lineage(row): row["message_asset"] for row in rows_by_type[RecordType.WIRE_MESSAGE]
    }
    assert len(wire_asset_by_lineage) == 452
    assert {row["arrival_sequence"] for row in rows_by_type[RecordType.WIRE_MESSAGE]} == set(range(1, 453))
    assert {
        key: row["asset"]
        for record_type in (RecordType.BBO, RecordType.L2_BOOK_STATE)
        for row in rows_by_type[record_type]
        for key in (lineage(row),)
    } == wire_asset_by_lineage
    assert {
        lineage(row): row["asset"] for row in rows_by_type[RecordType.L2_SNAPSHOT]
    } == wire_asset_by_lineage

    state_snapshot_ids = {str(row["snapshot_id"]) for row in rows_by_type[RecordType.L2_BOOK_STATE]}
    level_counts = Counter(str(row["snapshot_id"]) for row in rows_by_type[RecordType.L2_SNAPSHOT])
    side_counts = Counter(
        (str(row["snapshot_id"]), str(row["side"])) for row in rows_by_type[RecordType.L2_SNAPSHOT]
    )
    assert set(level_counts) == state_snapshot_ids
    assert set(level_counts.values()) == {40}
    assert set(side_counts.values()) == {20}
    bbo_sequence_by_lineage = {lineage(row): row["source_sequence"] for row in rows_by_type[RecordType.BBO]}
    level_sequence_by_lineage = {
        lineage(row): row["last_sequence"] for row in rows_by_type[RecordType.L2_SNAPSHOT]
    }
    assert level_sequence_by_lineage == bbo_sequence_by_lineage


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
