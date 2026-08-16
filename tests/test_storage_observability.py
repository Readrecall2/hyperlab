from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import BinaryIO

import pytest

import hyperlab.collector.storage as storage_module
import hyperlab.data.lake as lake_module
from hyperlab.collector.models import ParsedRecord
from hyperlab.collector.storage import BatchingLakeSink, CoordinatedLakeWriter
from hyperlab.collector.writer_worker import CoordinatedWriterWorker
from hyperlab.data.schema import RecordType

BASE = datetime(2026, 8, 14, 0, tzinfo=UTC)
STAGES = (
    "flush_total",
    "sort",
    "arrow_build",
    "partition_analysis",
    "partition_directory_fsync",
    "parquet_write",
    "parquet_fsync",
    "parquet_hash",
    "partition_publish",
    "data_directory_fsync",
    "manifest_fsync",
    "manifest_directory_fsync",
    "immediate_validation",
    "sqlite_commit",
)


class _StepClock:
    def __init__(self) -> None:
        self._now_ns = 0

    def __call__(self) -> int:
        self._now_ns += 1_000_000
        return self._now_ns


def _trade(sequence: int) -> ParsedRecord:
    event_time = BASE + timedelta(microseconds=sequence)
    return ParsedRecord(
        RecordType.TRADE,
        "BTC",
        {
            "schema_version": 2,
            "record_type": RecordType.TRADE.value,
            "venue": "hyperliquid",
            "asset": "BTC",
            "event_time": event_time,
            "exchange_time": event_time,
            "received_time": event_time + timedelta(milliseconds=1),
            "source_sequence": sequence,
            "connection_id": "storage-observability-test",
            "trade_id": f"trade-{sequence}",
            "aggressor_side": "buy",
            "price": Decimal("60000"),
            "quantity": Decimal("0.001"),
            "quote_quantity": Decimal("60"),
            "is_liquidation": None,
            "connection_epoch": 1,
            "arrival_sequence": sequence,
        },
    )


def test_batching_sink_reports_bounded_json_safe_stage_timings(tmp_path: Path) -> None:
    sink = BatchingLakeSink(
        tmp_path / "lake",
        batch_size=2,
        queue_capacity=4,
        monotonic_ns=_StepClock(),
    )
    try:
        assert sink.add(_trade(1)) is True
        result = sink.flush()
        snapshot = sink.metrics_snapshot()
    finally:
        sink.close()

    assert result.row_count == 1
    assert snapshot["schema_version"] == 1
    assert snapshot["queue"] == {
        "batch_size_rows": 2,
        "capacity_rows": 4,
        "pending_rows": 0,
        "high_water_rows": 1,
    }
    assert snapshot["flushes"] == {
        "attempted": 1,
        "succeeded": 1,
        "failed": 0,
        "in_progress": 0,
    }
    assert snapshot["written"] == {"rows": 1, "partitions": 1}
    timings = snapshot["timings_ms"]
    assert isinstance(timings, dict)
    expected_counts = dict.fromkeys(STAGES, 1)
    expected_counts["sort"] = 2
    expected_counts["partition_publish"] = 2
    for stage, expected_count in expected_counts.items():
        summary = timings[stage]
        assert isinstance(summary, dict)
        assert summary["count"] == expected_count
        assert summary["window_count"] == expected_count
        assert summary["window_capacity"] == 4_096
        assert summary["window_truncated"] is False
        assert summary["min_ms"] is not None

    json.dumps(snapshot, allow_nan=False)


def test_new_lake_root_entry_is_persisted_before_writer_use(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[Path] = []
    original_fsync_directory = storage_module._fsync_lake_directory

    def observe_fsync_directory(path: Path) -> None:
        observed.append(path)
        original_fsync_directory(path)

    monkeypatch.setattr(
        storage_module,
        "_fsync_lake_directory",
        observe_fsync_directory,
    )
    sink = BatchingLakeSink(tmp_path / "lake")
    sink.close()

    assert observed == [tmp_path]


def test_partition_durability_barriers_follow_publication_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[str] = []
    syncing_created_directories = False
    original_flush_and_fsync = lake_module._flush_and_fsync
    original_fsync_directory = lake_module._fsync_directory
    original_fsync_created = lake_module._fsync_created_directory_entries
    original_publish = lake_module._publish_exclusive

    def observe_flush_and_fsync(stream: BinaryIO) -> None:
        name = Path(stream.name).name
        if name.endswith(".parquet.tmp"):
            observed.append("parquet_fsync")
        elif name.endswith(".manifest.tmp"):
            observed.append("manifest_fsync")
        original_flush_and_fsync(stream)

    def observe_publish(
        temporary: Path,
        target: Path,
        *,
        expected_bytes: bytes | None = None,
    ) -> None:
        observed.append("data_publish" if target.name.endswith(".parquet") else "manifest_publish")
        original_publish(
            temporary,
            target,
            expected_bytes=expected_bytes,
        )

    def observe_fsync_created(created: tuple[Path, ...]) -> None:
        nonlocal syncing_created_directories
        observed.append("partition_directory_fsync")
        syncing_created_directories = True
        try:
            original_fsync_created(created)
        finally:
            syncing_created_directories = False

    def observe_fsync_directory(path: Path) -> None:
        if not syncing_created_directories:
            observed.append(
                "manifest_directory_fsync" if list(path.glob("*.manifest.json")) else "data_directory_fsync"
            )
        original_fsync_directory(path)

    monkeypatch.setattr(lake_module, "_flush_and_fsync", observe_flush_and_fsync)
    monkeypatch.setattr(lake_module, "_publish_exclusive", observe_publish)
    monkeypatch.setattr(
        lake_module,
        "_fsync_created_directory_entries",
        observe_fsync_created,
    )
    monkeypatch.setattr(lake_module, "_fsync_directory", observe_fsync_directory)

    sink = BatchingLakeSink(
        tmp_path / "lake",
        batch_size=2,
        queue_capacity=4,
    )
    try:
        assert sink.add(_trade(1)) is True
        result = sink.flush()
    finally:
        sink.close()

    assert result.row_count == 1
    assert observed == [
        "partition_directory_fsync",
        "parquet_fsync",
        "data_publish",
        "data_directory_fsync",
        "manifest_fsync",
        "manifest_publish",
        "manifest_directory_fsync",
    ]


def test_recovery_manifest_publish_is_fsynced_before_directory_fault(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    sink = BatchingLakeSink(
        root,
        batch_size=2,
        queue_capacity=4,
    )
    try:
        assert sink.add(_trade(1)) is True
        manifest = sink.flush().manifests[0]
    finally:
        sink.close()

    data_path = root / manifest.relative_data_path
    manifest_path = root / manifest.relative_manifest_path
    manifest_path.unlink()
    observed: list[str] = []
    original_flush_and_fsync = lake_module._flush_and_fsync
    original_publish = lake_module._publish_exclusive
    original_fsync_directory = lake_module._fsync_directory

    def observe_flush_and_fsync(stream: BinaryIO) -> None:
        if Path(stream.name).name.endswith(".manifest.tmp"):
            observed.append("manifest_fsync")
        original_flush_and_fsync(stream)

    def observe_publish(
        temporary: Path,
        target: Path,
        *,
        expected_bytes: bytes | None = None,
    ) -> None:
        observed.append("manifest_publish")
        original_publish(
            temporary,
            target,
            expected_bytes=expected_bytes,
        )

    def fail_directory_fsync(_path: Path) -> None:
        observed.append("manifest_directory_fsync")
        raise OSError("simulated recovery directory fsync failure")

    with monkeypatch.context() as context:
        context.setattr(
            lake_module,
            "_flush_and_fsync",
            observe_flush_and_fsync,
        )
        context.setattr(lake_module, "_publish_exclusive", observe_publish)
        context.setattr(
            lake_module,
            "_fsync_directory",
            fail_directory_fsync,
        )
        with pytest.raises(
            OSError,
            match="recovery directory fsync failure",
        ):
            lake_module.recover_partition_manifest(root, data_path)

    assert observed == [
        "manifest_fsync",
        "manifest_publish",
        "manifest_directory_fsync",
    ]
    assert manifest_path.is_file()
    assert list(manifest_path.parent.glob(".*.manifest.tmp")) == []
    retry_fsync_paths: list[Path] = []

    def observe_retry_fsync(path: Path) -> None:
        retry_fsync_paths.append(path)
        original_fsync_directory(path)

    with monkeypatch.context() as context:
        context.setattr(
            lake_module,
            "_fsync_directory",
            observe_retry_fsync,
        )
        assert lake_module.recover_partition_manifest(root, data_path) == manifest
    assert retry_fsync_paths == [manifest_path.parent]


def test_sink_restart_validates_existing_manifests_and_fsyncs_each_leaf_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    sink = BatchingLakeSink(
        root,
        batch_size=2,
        queue_capacity=4,
        persistent_dedup=False,
    )
    try:
        assert sink.add(_trade(1)) is True
        assert sink.flush().row_count == 1
        assert sink.add(_trade(2)) is True
        assert sink.flush().row_count == 1
    finally:
        sink.close()

    manifest_paths = sorted(root.rglob("*.manifest.json"), key=lambda path: path.as_posix())
    data_paths = sorted(root.rglob("part-*.parquet"), key=lambda path: path.as_posix())
    assert len(manifest_paths) == 2
    assert len({path.parent for path in manifest_paths}) == 1
    immutable_before = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (*data_paths, *manifest_paths)
    }
    original_validate = storage_module.validate_partition
    original_fsync_directory = storage_module._fsync_lake_directory
    validated_on_failed_start: list[Path] = []
    failed_fsync_paths: list[Path] = []

    def observe_validation_before_fault(path: Path) -> object:
        validated_on_failed_start.append(path)
        return original_validate(path)

    def fail_directory_fsync(path: Path) -> None:
        failed_fsync_paths.append(path)
        raise OSError("simulated startup manifest directory fsync failure")

    with monkeypatch.context() as context:
        context.setattr(
            storage_module,
            "validate_partition",
            observe_validation_before_fault,
        )
        context.setattr(
            storage_module,
            "_fsync_lake_directory",
            fail_directory_fsync,
        )
        with pytest.raises(
            OSError,
            match="startup manifest directory fsync failure",
        ):
            BatchingLakeSink(root, persistent_dedup=False)

    leaf = manifest_paths[0].parent
    assert validated_on_failed_start == manifest_paths
    assert failed_fsync_paths == [leaf]
    assert {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in immutable_before
    } == immutable_before

    validated_on_retry: list[Path] = []
    retry_fsync_paths: list[Path] = []

    def observe_retry_validation(path: Path) -> object:
        validated_on_retry.append(path)
        return original_validate(path)

    def observe_retry_fsync(path: Path) -> None:
        retry_fsync_paths.append(path)
        original_fsync_directory(path)

    with monkeypatch.context() as context:
        context.setattr(storage_module, "validate_partition", observe_retry_validation)
        context.setattr(storage_module, "_fsync_lake_directory", observe_retry_fsync)
        reopened = BatchingLakeSink(root, persistent_dedup=False)
        reopened.close()

    assert validated_on_retry == manifest_paths
    assert retry_fsync_paths == [leaf]
    assert {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in immutable_before
    } == immutable_before


def test_failed_immediate_validation_is_counted_without_clearing_pending_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_validation(_manifest_path: Path) -> object:
        raise RuntimeError("synthetic immediate validation failure")

    monkeypatch.setattr(lake_module, "validate_partition", fail_validation)
    sink = BatchingLakeSink(
        tmp_path / "lake",
        batch_size=2,
        queue_capacity=4,
        monotonic_ns=_StepClock(),
    )
    try:
        assert sink.add(_trade(1)) is True
        with pytest.raises(RuntimeError, match="synthetic immediate validation failure"):
            sink.flush()
        snapshot = sink.metrics_snapshot()
    finally:
        sink.close()

    assert snapshot["queue"]["pending_rows"] == 1
    assert snapshot["flushes"] == {
        "attempted": 1,
        "succeeded": 0,
        "failed": 1,
        "in_progress": 0,
    }
    assert snapshot["written"] == {"rows": 0, "partitions": 0}
    assert snapshot["timings_ms"]["immediate_validation"]["count"] == 1
    assert snapshot["timings_ms"]["sqlite_commit"]["count"] == 0


def test_coordinated_writer_and_worker_forward_storage_snapshot(tmp_path: Path) -> None:
    writer = CoordinatedLakeWriter(
        tmp_path / "direct-lake",
        venues=("hyperliquid",),
        monotonic_ns=_StepClock(),
    )
    try:
        direct = writer.metrics_snapshot()
        assert direct["schema_version"] == 1
        json.dumps(direct, allow_nan=False)
    finally:
        writer.close()

    worker = CoordinatedWriterWorker(
        tmp_path / "worker-lake",
        venues=("hyperliquid",),
        monotonic_ns=_StepClock(),
    )
    try:
        worker_snapshot = worker.metrics_snapshot()
        assert worker_snapshot["storage"] == worker._writer.metrics_snapshot()
        json.dumps(worker_snapshot, allow_nan=False)
    finally:
        worker.close()
