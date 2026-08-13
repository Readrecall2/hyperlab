from __future__ import annotations

import multiprocessing
import shutil
import sqlite3
from collections import namedtuple
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import pytest

import hyperlab.collector.storage as storage_module
import hyperlab.storage.sqlite as sqlite_storage_module
from hyperlab.collector.models import ParsedRecord, WireEnvelope
from hyperlab.collector.parser import parse_websocket_message
from hyperlab.collector.storage import (
    BatchingLakeSink,
    LakeWriterActiveError,
    StorageCapacityError,
)
from hyperlab.data.lake import discover_partitions, validate_partition
from hyperlab.storage.sqlite import write_runtime_status

NOW = datetime(2026, 8, 13, 12, tzinfo=UTC)


class _EventLike(Protocol):
    def set(self) -> None: ...


def _wire_record(sequence: int) -> ParsedRecord:
    parsed = parse_websocket_message(
        WireEnvelope(
            raw_message='{"channel":"pong"}',
            received_time=NOW,
            connection_id="phase15-storage-test",
            connection_epoch=1,
            arrival_sequence=sequence,
        )
    )
    assert len(parsed.records) == 1
    return parsed.records[0]


def _hold_writer(root: str, ready: _EventLike) -> None:
    sink = BatchingLakeSink(
        Path(root),
        batch_size=1,
        queue_capacity=2,
        min_free_bytes=0,
        min_free_percent=0,
    )
    assert sink.pending_count == 0
    ready.set()
    # An abrupt process termination intentionally bypasses sink.close().
    multiprocessing.Event().wait()


def test_interprocess_writer_lock_releases_after_abrupt_process_termination(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    process = context.Process(target=_hold_writer, args=(str(root), ready))
    process.start()
    try:
        assert ready.wait(15), "child writer did not acquire its lock"
        with pytest.raises(LakeWriterActiveError, match="active writer"):
            BatchingLakeSink(
                root,
                batch_size=1,
                queue_capacity=2,
                min_free_bytes=0,
                min_free_percent=0,
            )
    finally:
        process.terminate()
        process.join(15)
    assert process.exitcode is not None and process.exitcode != 0

    recovered = BatchingLakeSink(
        root,
        batch_size=1,
        queue_capacity=2,
        min_free_bytes=0,
        min_free_percent=0,
    )
    assert recovered.unclean_restart_detected is True
    recovered.close()
    assert not (root / ".collector-session.json").exists()


def test_new_writer_root_directory_chain_is_durably_synced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "nested" / "lake"
    synced: list[Path] = []
    monkeypatch.setattr(storage_module, "_fsync_parent", lambda path: synced.append(path))

    sink = BatchingLakeSink(
        root,
        batch_size=1,
        queue_capacity=2,
        min_free_bytes=0,
        min_free_percent=0,
    )
    sink.close()

    assert root.parent in synced
    assert root in synced


def test_runtime_status_is_fsynced_before_atomic_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []
    monkeypatch.setattr(sqlite_storage_module.os, "fsync", lambda descriptor: calls.append(descriptor))
    target = tmp_path / "runtime" / "runtime_status.json"

    write_runtime_status(target, {"ok": True, "orders_enabled": False})

    assert calls
    assert target.read_text(encoding="utf-8").startswith("{")
    assert not target.with_suffix(".tmp").exists()


def test_disk_reserve_failure_keeps_pending_rows_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    sink = BatchingLakeSink(
        root,
        batch_size=1,
        queue_capacity=2,
        min_free_bytes=0,
        min_free_percent=0,
    )
    sink.add(_wire_record(1))
    usage = namedtuple("DiskUsage", "total used free")(10_000, 9_999, 1)
    monkeypatch.setattr(storage_module.shutil, "disk_usage", lambda _path: usage)
    sink._min_free_bytes = 2
    try:
        with pytest.raises(StorageCapacityError, match="reserve exhausted"):
            sink.flush()
        assert sink.pending_count == 1
        assert discover_partitions(root) == ()
    finally:
        sink.close()


def test_valid_interrupted_parquet_is_recovered_and_invalid_temp_fails_startup(tmp_path: Path) -> None:
    source = tmp_path / "source"
    writer = BatchingLakeSink(
        source,
        batch_size=1,
        queue_capacity=2,
        min_free_bytes=0,
        min_free_percent=0,
    )
    writer.add(_wire_record(1))
    manifest = writer.flush().manifests[0]
    writer.close()

    target = tmp_path / "target"
    relative_leaf = manifest.relative_data_path.parent
    leaf = target / relative_leaf
    leaf.mkdir(parents=True)
    shutil.copyfile(source / manifest.relative_data_path, leaf / ".interrupted.parquet.tmp")

    recovered = BatchingLakeSink(
        target,
        batch_size=1,
        queue_capacity=2,
        min_free_bytes=0,
        min_free_percent=0,
    )
    recovered.close()
    recovered_manifests = discover_partitions(target)
    assert len(recovered_manifests) == 1
    assert validate_partition(recovered_manifests[0]).sha256 == manifest.sha256
    assert list(target.rglob(".*.parquet.tmp")) == []

    corrupt = tmp_path / "corrupt"
    corrupt.mkdir()
    (corrupt / ".broken.parquet.tmp").write_bytes(b"not parquet")
    with pytest.raises(ValueError, match="invalid interrupted Parquet"):
        BatchingLakeSink(
            corrupt,
            batch_size=1,
            queue_capacity=2,
            min_free_bytes=0,
            min_free_percent=0,
        )


def test_failed_clean_marker_removal_forces_unclean_restart_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    sink = BatchingLakeSink(
        root,
        batch_size=1,
        queue_capacity=2,
        min_free_bytes=0,
        min_free_percent=0,
    )
    original_clear = storage_module._clear_session_marker
    monkeypatch.setattr(
        storage_module,
        "_clear_session_marker",
        lambda _root: (_ for _ in ()).throw(OSError("simulated disk failure")),
    )

    with pytest.raises(OSError, match="simulated disk failure"):
        sink.close()
    monkeypatch.setattr(storage_module, "_clear_session_marker", original_clear)
    recovered = BatchingLakeSink(
        root,
        batch_size=1,
        queue_capacity=2,
        min_free_bytes=0,
        min_free_percent=0,
    )
    assert recovered.unclean_restart_detected is True
    recovered.close()
    assert not (root / ".collector-session.json").exists()


def test_corrupt_derived_observation_index_is_rebuilt_from_valid_parquet(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    writer = BatchingLakeSink(
        root,
        batch_size=1,
        queue_capacity=2,
        min_free_bytes=0,
        min_free_percent=0,
    )
    writer.add(_wire_record(1))
    writer.flush()
    writer.close()

    index_path = root / ".collector-observations.sqlite3"
    index_path.write_bytes(b"corrupt derived cache")
    rebuilt = BatchingLakeSink(
        root,
        batch_size=1,
        queue_capacity=2,
        min_free_bytes=0,
        min_free_percent=0,
        validate_integrity=True,
    )
    rebuilt.close()

    with sqlite3.connect(index_path) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
