from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from functools import wraps
from pathlib import Path
from typing import Any, BinaryIO, Protocol

import pyarrow as pa

from hyperlab.collector.models import ParsedRecord
from hyperlab.collector.telemetry import MonotonicTimingSummary
from hyperlab.data.lake import (
    PartitionKey,
    PartitionManifest,
    discover_partitions,
    read_hashed_table,
    recover_partition_manifest,
    validate_partition,
    write_partition,
)
from hyperlab.data.lake import _fsync_directory as _fsync_lake_directory
from hyperlab.data.schema import RecordType, SchemaSpec, latest_schema_for, schema_for


@dataclass(frozen=True, slots=True)
class FlushResult:
    manifests: tuple[PartitionManifest, ...]
    row_count: int
    duplicate_count: int


class CoordinatedWriterError(RuntimeError):
    """Fatal coordinated-writer incompatibility or storage failure."""


class LakeSink(Protocol):
    """Minimal collector sink contract, including coordinated venue views."""

    @property
    def high_water(self) -> int: ...

    @property
    def pending_count(self) -> int: ...

    @property
    def should_flush(self) -> bool: ...

    def add(self, record: ParsedRecord) -> bool: ...
    def add_many(self, records: Iterable[ParsedRecord]) -> int: ...

    def flush(self) -> FlushResult: ...

    def close(self) -> None: ...


_GroupKey = tuple[str, RecordType, str, str, str]
_ObservationSignature = tuple[str, str, str]
_ObservationHeadKey = tuple[str, str]
_StablePrimaryKey = tuple[str, str]
_OBSERVATION_INDEX_VERSION = 4
_PERSISTENT_PRIMARY_KEY_TYPES = frozenset({RecordType.TRADE})
_PARTIAL_FLUSH_BARRIER_ONLY_TYPES = frozenset(
    {
        RecordType.CANDLE,
        RecordType.FUNDING,
    }
)
_RecentKey = tuple[RecordType, tuple[object, ...]]
_STORAGE_TIMING_WINDOW = 4_096
_STORAGE_TIMING_STAGES = (
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


class _StorageMetrics:
    """Bounded monotonic timings plus small lifetime counters."""

    def __init__(self) -> None:
        self._timings = {
            stage: MonotonicTimingSummary(window_capacity=_STORAGE_TIMING_WINDOW)
            for stage in _STORAGE_TIMING_STAGES
        }
        self._lock = threading.Lock()
        self._flush_attempts = 0
        self._flush_succeeded = 0
        self._flush_failed = 0
        self._rows_written = 0
        self._partitions_written = 0

    def observe(self, stage: str, duration_ns: int) -> None:
        timing = self._timings.get(stage)
        if timing is not None:
            timing.observe_ns(max(duration_ns, 0))

    def begin_flush(self) -> None:
        with self._lock:
            self._flush_attempts += 1

    def end_flush(
        self,
        *,
        succeeded: bool,
        row_count: int,
        partition_count: int,
    ) -> None:
        with self._lock:
            if succeeded:
                self._flush_succeeded += 1
                self._rows_written += row_count
                self._partitions_written += partition_count
            else:
                self._flush_failed += 1

    def snapshot(
        self,
        *,
        batch_size: int,
        queue_capacity: int,
        pending_count: int,
        high_water: int,
        pending_group_count: int,
        ready_group_count: int,
        max_group_rows: int,
    ) -> dict[str, object]:
        with self._lock:
            attempts = self._flush_attempts
            succeeded = self._flush_succeeded
            failed = self._flush_failed
            rows_written = self._rows_written
            partitions_written = self._partitions_written
        return {
            "schema_version": 1,
            "queue": {
                "batch_size_rows": batch_size,
                "capacity_rows": queue_capacity,
                "pending_rows": pending_count,
                "high_water_rows": high_water,
            },
            "coalescing": {
                "readiness": "exact_group",
                "pending_groups": pending_group_count,
                "ready_groups": ready_group_count,
                "max_group_rows": max_group_rows,
            },
            "flushes": {
                "attempted": attempts,
                "succeeded": succeeded,
                "failed": failed,
                "in_progress": attempts - succeeded - failed,
            },
            "written": {
                "rows": rows_written,
                "partitions": partitions_written,
            },
            "timings_ms": {stage: self._timings[stage].as_dict() for stage in _STORAGE_TIMING_STAGES},
        }


def _instrument_flush(
    method: Callable[[Any], FlushResult],
) -> Callable[[Any], FlushResult]:
    """Measure every flush attempt without changing its control flow."""

    @wraps(method)
    def measured(sink: Any) -> FlushResult:
        started_ns = sink._monotonic_ns()
        sink._metrics.begin_flush()
        try:
            result = method(sink)
        except BaseException:
            sink._metrics.observe(
                "flush_total",
                sink._monotonic_ns() - started_ns,
            )
            sink._metrics.end_flush(
                succeeded=False,
                row_count=0,
                partition_count=0,
            )
            raise
        sink._metrics.observe(
            "flush_total",
            sink._monotonic_ns() - started_ns,
        )
        sink._metrics.end_flush(
            succeeded=True,
            row_count=result.row_count,
            partition_count=len(result.manifests),
        )
        return result

    return measured


@dataclass(frozen=True, slots=True)
class _GroupMutation:
    group_key: _GroupKey
    primary_key: tuple[object, ...]
    primary_existed: bool
    previous_row: dict[str, object] | None
    group_created: bool


@dataclass(slots=True)
class _RecentBatch:
    """Transactional LRU view whose work scales with touches and evictions."""

    recent: OrderedDict[_RecentKey, None]
    capacity: int
    touched: OrderedDict[_RecentKey, None] = field(default_factory=OrderedDict)
    removed_global: set[_RecentKey] = field(default_factory=set)
    size: int = field(init=False)
    oldest: Iterator[_RecentKey] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.size = len(self.recent)
        self.oldest = iter(self.recent)

    def contains(self, key: _RecentKey) -> bool:
        return key in self.touched or (key in self.recent and key not in self.removed_global)

    def touch(self, key: _RecentKey) -> None:
        """Mark an existing key most-recent without mutating global history."""

        self.touched.pop(key, None)
        self.touched[key] = None

    def insert(self, key: _RecentKey) -> None:
        if self.contains(key):
            raise AssertionError("recent-key batch insertion must be unique")
        self.touched[key] = None
        self.size += 1
        if self.size > self.capacity:
            self._evict_oldest()

    def commit(self) -> None:
        for key in self.removed_global:
            self.recent.pop(key, None)
        for key in self.touched:
            if key in self.recent:
                self.recent.move_to_end(key)
            else:
                self.recent[key] = None

    def _evict_oldest(self) -> None:
        for candidate in self.oldest:
            if candidate in self.removed_global or candidate in self.touched:
                continue
            self.removed_global.add(candidate)
            self.size -= 1
            return

        candidate, _ = self.touched.popitem(last=False)
        if candidate in self.recent:
            self.removed_global.add(candidate)
        self.size -= 1


@dataclass(slots=True)
class _ObservationBatch:
    """Transactional payload updates plus exact LRU order."""

    observations: OrderedDict[_ObservationHeadKey, str]
    capacity: int
    touched: OrderedDict[_ObservationHeadKey, str] = field(default_factory=OrderedDict)
    removed_global: set[_ObservationHeadKey] = field(default_factory=set)
    size: int = field(init=False)
    oldest: Iterator[_ObservationHeadKey] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.size = len(self.observations)
        self.oldest = iter(self.observations)

    def get(self, key: _ObservationHeadKey) -> str | None:
        if key in self.touched:
            return self.touched[key]
        if key in self.removed_global:
            return None
        return self.observations.get(key)

    def set(self, key: _ObservationHeadKey, payload: str) -> None:
        existed = self.get(key) is not None
        self.touched.pop(key, None)
        self.touched[key] = payload
        if existed:
            return
        self.size += 1
        if self.size > self.capacity:
            self._evict_oldest()

    def commit(self) -> None:
        for key in self.removed_global:
            self.observations.pop(key, None)
        for key, payload in self.touched.items():
            self.observations[key] = payload
            self.observations.move_to_end(key)

    def _evict_oldest(self) -> None:
        for candidate in self.oldest:
            if candidate in self.removed_global or candidate in self.touched:
                continue
            self.removed_global.add(candidate)
            self.size -= 1
            return

        candidate, _ = self.touched.popitem(last=False)
        if candidate in self.observations:
            self.removed_global.add(candidate)
        self.size -= 1


@dataclass(slots=True)
class _PendingObservationBatch:
    pending: OrderedDict[_ObservationHeadKey, _ObservationSignature]
    touched: OrderedDict[_ObservationHeadKey, _ObservationSignature] = field(default_factory=OrderedDict)

    def set(
        self,
        key: _ObservationHeadKey,
        signature: _ObservationSignature,
    ) -> None:
        self.touched.pop(key, None)
        self.touched[key] = signature

    def commit(self) -> None:
        for key, signature in self.touched.items():
            self.pending[key] = signature
            self.pending.move_to_end(key)


@dataclass(slots=True)
class _BatchMutationJournal:
    pending_count: int
    duplicate_count: int
    high_water: int
    recent: _RecentBatch
    observations: _ObservationBatch
    pending_observations: _PendingObservationBatch
    group_mutations: list[_GroupMutation] = field(default_factory=list)
    stable_keys_added: list[_StablePrimaryKey] = field(default_factory=list)

    def rollback(self, sink: BatchingLakeSink) -> None:
        for group_mutation in reversed(self.group_mutations):
            group = sink._groups[group_mutation.group_key]
            if group_mutation.primary_existed:
                assert group_mutation.previous_row is not None
                group[group_mutation.primary_key] = group_mutation.previous_row
            else:
                group.pop(group_mutation.primary_key, None)
            if group_mutation.group_created and not group:
                sink._groups.pop(group_mutation.group_key, None)

        for stable_key in reversed(self.stable_keys_added):
            sink._pending_stable_primary_keys.discard(stable_key)

        sink._pending_count = self.pending_count
        sink._duplicate_count = self.duplicate_count
        sink.high_water = self.high_water


def _canonical_scalar(value: object) -> object:
    if isinstance(value, Decimal):
        if value.is_zero():
            return "0"
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("observation datetime must be timezone-aware")
        return value.astimezone(UTC).isoformat(timespec="microseconds")
    if isinstance(value, (list, tuple)):
        return [_canonical_scalar(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _canonical_scalar(item) for key, item in value.items()}
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(
        _canonical_scalar(value),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _observation_signature(
    record_type: RecordType,
    row: dict[str, object],
) -> _ObservationSignature | None:
    payload_fields: tuple[str, ...]
    if record_type == RecordType.CANDLE:
        logical_fields = ("venue", "asset", "interval", "open_time")
        payload_fields = (
            "close_time",
            "open",
            "high",
            "low",
            "close",
            "base_volume",
            "quote_volume",
            "trade_count",
            "is_final",
        )
    elif record_type == RecordType.FUNDING:
        logical_fields = ("venue", "asset", "funding_time", "rate_kind")
        payload_fields = (
            "funding_rate",
            "funding_interval_seconds",
            "mark_price",
            "oracle_price",
        )
    else:
        return None
    logical = _canonical_json([row.get(name) for name in logical_fields])
    payload = _canonical_json([row.get(name) for name in payload_fields])
    return (
        f"{record_type.value}:v{int(str(row['schema_version']))}",
        hashlib.sha256(logical.encode()).hexdigest(),
        hashlib.sha256(payload.encode()).hexdigest(),
    )


def _stable_primary_key(
    record_type: RecordType,
    schema_version: int,
    primary_key: tuple[object, ...],
) -> _StablePrimaryKey | None:
    if record_type not in _PERSISTENT_PRIMARY_KEY_TYPES:
        return None
    del schema_version
    canonical = _canonical_json(primary_key)
    return (
        f"{record_type.value}:compatible-primary-key",
        hashlib.sha256(canonical.encode()).hexdigest(),
    )


def _rebuild_order(
    row: dict[str, object],
    manifest: PartitionManifest,
    row_number: int,
) -> tuple[str, str, str, str, int]:
    return (
        _canonical_json(row.get("received_time")),
        _canonical_json(row.get("connection_id")),
        _canonical_json(row.get("observation_id")),
        manifest.relative_data_path.as_posix(),
        row_number,
    )


class _RootWriterLock:
    """Process-scoped, non-blocking writer lock retained for the sink lifetime."""

    def __init__(self, root: Path) -> None:
        root_missing = not root.exists()
        root.mkdir(parents=True, exist_ok=True)
        if root_missing:
            _fsync_lake_directory(root.parent)
        self.path = root / ".collector-writer.lock"
        stream = self.path.open("a+b")
        try:
            self._lock(stream)
        except OSError:
            stream.close()
            raise RuntimeError(f"collector lake already has an active writer: {root}") from None
        self._stream: BinaryIO | None = stream

    @staticmethod
    def _lock(stream: BinaryIO) -> None:
        stream.seek(0)
        if stream.read(1) == b"":
            stream.write(b"\0")
            stream.flush()
        stream.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            return
        import fcntl

        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)  # type: ignore[attr-defined]

    def close(self) -> None:
        stream = self._stream
        if stream is None:
            return
        self._stream = None
        stream.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)  # type: ignore[attr-defined]
        stream.close()


def _recover_orphans(root: Path) -> None:
    """Prove existing manifests durable, then rebuild manifests for orphan data."""

    existing_manifest_directories: set[Path] = set()
    recovery_root = root / ".recovery"
    for manifest_path in sorted(
        root.rglob("part-*.manifest.json"),
        key=lambda path: path.as_posix(),
    ):
        if recovery_root in manifest_path.parents:
            continue
        validate_partition(manifest_path)
        existing_manifest_directories.add(manifest_path.parent)

    for data_path in sorted(root.rglob("part-*.parquet"), key=lambda path: path.as_posix()):
        manifest_path = data_path.with_name(f"{data_path.stem}.manifest.json")
        if recovery_root in data_path.parents or manifest_path.exists():
            continue
        recover_partition_manifest(root, data_path)

    for directory in sorted(existing_manifest_directories, key=lambda path: path.as_posix()):
        _fsync_lake_directory(directory)


class _PersistentObservationIndex:
    """Derived cache: immutable manifests remain the source of truth."""

    def __init__(
        self,
        root: Path,
        *,
        serialized_cross_thread_access: bool = False,
    ) -> None:
        root.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            root / ".collector-observations.sqlite3",
            check_same_thread=not serialized_cross_thread_access,
        )
        try:
            self._connection.execute("PRAGMA journal_mode=DELETE")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._migrate()
            self._reconcile(root)
        except BaseException:
            self._connection.close()
            raise

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        head_columns = tuple(
            str(row[1]) for row in self._connection.execute("PRAGMA table_info(observation_heads)")
        )
        manifest_columns = tuple(
            str(row[1]) for row in self._connection.execute("PRAGMA table_info(indexed_manifests)")
        )
        stable_columns = tuple(
            str(row[1]) for row in self._connection.execute("PRAGMA table_info(stable_primary_keys)")
        )
        schema_is_current = (
            version == _OBSERVATION_INDEX_VERSION
            and head_columns == ("record_type", "logical_key_sha256", "payload_sha256")
            and manifest_columns == ("data_file", "sha256")
            and stable_columns == ("record_type", "primary_key_sha256")
        )
        if schema_is_current:
            return
        with self._connection:
            self._connection.execute("DROP TABLE IF EXISTS observations")
            self._connection.execute("DROP TABLE IF EXISTS observation_heads")
            self._connection.execute("DROP TABLE IF EXISTS indexed_manifests")
            self._connection.execute("DROP TABLE IF EXISTS stable_primary_keys")
            self._create_tables()
            self._connection.execute(f"PRAGMA user_version = {_OBSERVATION_INDEX_VERSION}")

    def _create_tables(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE observation_heads (
                record_type TEXT NOT NULL,
                logical_key_sha256 TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                PRIMARY KEY (record_type, logical_key_sha256)
            ) WITHOUT ROWID
            """
        )
        self._connection.execute(
            """
            CREATE TABLE indexed_manifests (
                data_file TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                PRIMARY KEY (data_file, sha256)
            ) WITHOUT ROWID
            """
        )

        self._connection.execute(
            """
            CREATE TABLE stable_primary_keys (
                record_type TEXT NOT NULL,
                primary_key_sha256 TEXT NOT NULL,
                PRIMARY KEY (record_type, primary_key_sha256)
            ) WITHOUT ROWID
            """
        )

    def _reconcile(self, root: Path) -> None:
        observations: list[tuple[tuple[str, str, str, str, int], _ObservationSignature]] = []
        stable_primary_keys: set[_StablePrimaryKey] = set()
        pending_manifests: list[PartitionManifest] = []
        for manifest_path in discover_partitions(root):
            manifest = validate_partition(manifest_path)
            record_type = RecordType(manifest.partition.record_type)
            if record_type not in {
                RecordType.CANDLE,
                RecordType.FUNDING,
                *_PERSISTENT_PRIMARY_KEY_TYPES,
            }:
                continue
            table = read_hashed_table(root, manifest)
            rows = table.to_pylist()
            spec = schema_for(record_type, manifest.schema_version)
            for row_number, row in enumerate(rows):
                observation = _observation_signature(record_type, row)
                if observation is not None:
                    observations.append((_rebuild_order(row, manifest, row_number), observation))
                stable = _stable_primary_key(
                    record_type,
                    spec.version,
                    tuple(row[name] for name in spec.primary_key),
                )
                if stable is not None:
                    stable_primary_keys.add(stable)
            pending_manifests.append(manifest)

        with self._connection:
            self._connection.execute("DELETE FROM observation_heads")
            self._connection.execute("DELETE FROM stable_primary_keys")
            self._connection.execute("DELETE FROM indexed_manifests")
            self.add_many(
                observation
                for _order, observation in sorted(
                    observations,
                    key=lambda item: item[0],
                )
            )
            self.add_stable_many(sorted(stable_primary_keys))
            self._connection.executemany(
                "INSERT OR IGNORE INTO indexed_manifests VALUES (?, ?)",
                [(manifest.data_file, manifest.sha256) for manifest in pending_manifests],
            )

    def contains(self, signature: _ObservationSignature) -> bool:
        return (
            self._connection.execute(
                """
                SELECT 1 FROM observation_heads
                WHERE record_type = ? AND logical_key_sha256 = ? AND payload_sha256 = ?
                """,
                signature,
            ).fetchone()
            is not None
        )

    def contains_stable(self, key: _StablePrimaryKey) -> bool:
        return (
            self._connection.execute(
                """
                SELECT 1 FROM stable_primary_keys
                WHERE record_type = ? AND primary_key_sha256 = ?
                """,
                key,
            ).fetchone()
            is not None
        )

    def add_many(self, signatures: Any) -> None:
        self._connection.executemany(
            """
            INSERT INTO observation_heads VALUES (?, ?, ?)
            ON CONFLICT(record_type, logical_key_sha256)
            DO UPDATE SET payload_sha256 = excluded.payload_sha256
            """,
            signatures,
        )

    def add_stable_many(self, keys: Any) -> None:
        self._connection.executemany(
            "INSERT OR IGNORE INTO stable_primary_keys VALUES (?, ?)",
            keys,
        )

    def commit(
        self,
        signatures: list[_ObservationSignature],
        stable_primary_keys: list[_StablePrimaryKey],
        manifests: list[PartitionManifest],
    ) -> None:
        relevant = [
            manifest
            for manifest in manifests
            if RecordType(manifest.partition.record_type)
            in {
                RecordType.CANDLE,
                RecordType.FUNDING,
                *_PERSISTENT_PRIMARY_KEY_TYPES,
            }
        ]
        with self._connection:
            self.add_many(signatures)
            self.add_stable_many(stable_primary_keys)
            self._connection.executemany(
                "INSERT OR IGNORE INTO indexed_manifests VALUES (?, ?)",
                [(manifest.data_file, manifest.sha256) for manifest in relevant],
            )

    def close(self) -> None:
        self._connection.close()


class BatchingLakeSink:
    """Bounded in-memory batcher publishing immutable Parquet artifacts."""

    def __init__(
        self,
        root: Path,
        *,
        batch_size: int = 500,
        queue_capacity: int = 10_000,
        recent_key_capacity: int = 100_000,
        persistent_dedup: bool = True,
        _serialized_cross_thread_access: bool = False,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if batch_size <= 0 or queue_capacity < batch_size or recent_key_capacity <= 0:
            raise ValueError("invalid batch or queue capacity")
        self.root = root
        self._writer_lock = _RootWriterLock(root)
        self._observation_index: _PersistentObservationIndex | None = None
        self.batch_size = batch_size
        self.queue_capacity = queue_capacity
        self.recent_key_capacity = recent_key_capacity
        self._monotonic_ns = monotonic_ns
        self._metrics = _StorageMetrics()
        self._groups: dict[_GroupKey, OrderedDict[tuple[object, ...], dict[str, object]]] = {}
        self._recent: OrderedDict[tuple[RecordType, tuple[object, ...]], None] = OrderedDict()
        self._observations: OrderedDict[_ObservationHeadKey, str] = OrderedDict()
        self._pending_observations: OrderedDict[_ObservationHeadKey, _ObservationSignature] = OrderedDict()
        self._pending_stable_primary_keys: set[_StablePrimaryKey] = set()
        try:
            _recover_orphans(root)
            if persistent_dedup:
                self._observation_index = _PersistentObservationIndex(
                    root,
                    serialized_cross_thread_access=_serialized_cross_thread_access,
                )
        except BaseException:
            self._writer_lock.close()
            raise
        self._closed = False
        self._pending_count = 0
        self._duplicate_count = 0
        self.high_water = 0

    @property
    def pending_count(self) -> int:
        return self._pending_count

    @property
    def should_flush(self) -> bool:
        return any(
            group_key[1] not in _PARTIAL_FLUSH_BARRIER_ONLY_TYPES and len(group) >= self.batch_size
            for group_key, group in self._groups.items()
        )

    def metrics_snapshot(self) -> dict[str, object]:
        group_sizes = tuple(len(group) for group in self._groups.values())
        return self._metrics.snapshot(
            batch_size=self.batch_size,
            queue_capacity=self.queue_capacity,
            pending_count=self._pending_count,
            high_water=self.high_water,
            pending_group_count=len(group_sizes),
            ready_group_count=sum(
                group_key[1] not in _PARTIAL_FLUSH_BARRIER_ONLY_TYPES and len(group) >= self.batch_size
                for group_key, group in self._groups.items()
            ),
            max_group_rows=max(group_sizes, default=0),
        )

    def add(self, record: ParsedRecord) -> bool:
        return self._add(record, journal=None)

    def _add(
        self,
        record: ParsedRecord,
        *,
        journal: _BatchMutationJournal | None,
    ) -> bool:
        spec = latest_schema_for(record.record_type)
        row = dict(record.row)
        row["schema_version"] = spec.version
        partition_asset = record.asset
        primary_key = self._primary_key(spec, row)
        recent_key: _RecentKey = (record.record_type, primary_key)
        observation = _observation_signature(record.record_type, row)
        stable_primary_key = _stable_primary_key(
            record.record_type,
            spec.version,
            primary_key,
        )
        if (
            stable_primary_key is not None
            and self._observation_index is not None
            and self._observation_index.contains_stable(stable_primary_key)
        ):
            self._duplicate_count += 1
            return False
        if observation is not None:
            cached_head_key: _ObservationHeadKey = (observation[0], observation[1])
            if journal is None:
                cached_payload = self._observations.get(cached_head_key)
            else:
                cached_payload = journal.observations.get(cached_head_key)
            if cached_payload == observation[2] or (
                cached_payload is None
                and self._observation_index is not None
                and self._observation_index.contains(observation)
            ):
                self._duplicate_count += 1
                return False
        if journal is None:
            recent_duplicate = recent_key in self._recent
        else:
            recent_duplicate = journal.recent.contains(recent_key)
        if recent_duplicate:
            if journal is None:
                self._recent.move_to_end(recent_key)
            else:
                journal.recent.touch(recent_key)
            self._duplicate_count += 1
            return False

        group_key = self._group_key(record.record_type, partition_asset, row)
        group = self._groups.get(group_key)
        if group is not None and primary_key in group:
            if journal is not None:
                journal.group_mutations.append(
                    _GroupMutation(
                        group_key,
                        primary_key,
                        True,
                        group[primary_key],
                        False,
                    )
                )
            group[primary_key] = row
            self._duplicate_count += 1
            return False
        if self._pending_count >= self.queue_capacity:
            raise BufferError("collector queue capacity exceeded; no record was dropped")

        group_created = group is None
        if group is None:
            group = OrderedDict()
        if journal is not None:
            journal.group_mutations.append(
                _GroupMutation(
                    group_key,
                    primary_key,
                    False,
                    None,
                    group_created,
                )
            )
        if group_created:
            self._groups[group_key] = group

        group[primary_key] = row
        self._pending_count += 1
        self.high_water = max(self.high_water, self._pending_count)
        if journal is None:
            self._recent[recent_key] = None
            if len(self._recent) > self.recent_key_capacity:
                self._recent.popitem(last=False)
        else:
            journal.recent.insert(recent_key)

        if observation is not None:
            head_key: _ObservationHeadKey = (observation[0], observation[1])
            if journal is None:
                self._observations[head_key] = observation[2]
                self._observations.move_to_end(head_key)
                if len(self._observations) > self.recent_key_capacity:
                    self._observations.popitem(last=False)
                self._pending_observations[head_key] = observation
                self._pending_observations.move_to_end(head_key)
            else:
                journal.observations.set(head_key, observation[2])
                journal.pending_observations.set(head_key, observation)

        if stable_primary_key is not None and stable_primary_key not in self._pending_stable_primary_keys:
            self._pending_stable_primary_keys.add(stable_primary_key)
            if journal is not None:
                journal.stable_keys_added.append(stable_primary_key)
        return True

    def add_many(self, records: Iterable[ParsedRecord]) -> int:
        """Atomically add one frame with work bounded by that frame's mutations."""

        batch = tuple(records)
        if len(batch) > self.queue_capacity - self._pending_count:
            raise BufferError("collector queue capacity exceeded before atomic batch; no record was added")
        journal = _BatchMutationJournal(
            pending_count=self._pending_count,
            duplicate_count=self._duplicate_count,
            high_water=self.high_water,
            recent=_RecentBatch(
                self._recent,
                self.recent_key_capacity,
            ),
            observations=_ObservationBatch(
                self._observations,
                self.recent_key_capacity,
            ),
            pending_observations=_PendingObservationBatch(
                self._pending_observations,
            ),
        )
        accepted = 0
        try:
            for record in batch:
                accepted += int(self._add(record, journal=journal))
        except BaseException:
            journal.rollback(self)
            raise
        journal.recent.commit()
        journal.observations.commit()
        journal.pending_observations.commit()
        return accepted

    @_instrument_flush
    def flush_ready(self) -> FlushResult:
        ready = tuple(
            group_key
            for group_key, group in self._groups.items()
            if group_key[1] not in _PARTIAL_FLUSH_BARRIER_ONLY_TYPES and len(group) >= self.batch_size
        )
        return self._flush_groups(ready, final_barrier=False)

    @_instrument_flush
    def flush(self) -> FlushResult:
        return self._flush_groups(tuple(self._groups), final_barrier=True)

    def _flush_groups(
        self,
        selected_group_keys: tuple[_GroupKey, ...],
        *,
        final_barrier: bool,
    ) -> FlushResult:
        if not selected_group_keys:
            if not final_barrier:
                return FlushResult((), 0, 0)
            duplicates = self._duplicate_count
            self._duplicate_count = 0
            return FlushResult((), 0, duplicates)

        manifests: list[PartitionManifest] = []
        selected_stable_primary_keys: set[_StablePrimaryKey] = set()
        written = 0
        sort_started_ns = self._monotonic_ns()
        try:
            group_keys = sorted(
                selected_group_keys,
                key=lambda item: (item[3], item[2], item[0], item[1].value, item[4]),
            )
        finally:
            self._metrics.observe(
                "sort",
                self._monotonic_ns() - sort_started_ns,
            )
        for group_key in group_keys:
            venue, record_type, asset, day, _stream = group_key
            spec = latest_schema_for(record_type)
            rows = list(self._groups[group_key].values())
            if not final_barrier and record_type in _PERSISTENT_PRIMARY_KEY_TYPES:
                for row in rows:
                    stable = _stable_primary_key(
                        record_type,
                        spec.version,
                        self._primary_key(spec, row),
                    )
                    if stable is not None:
                        selected_stable_primary_keys.add(stable)
            sort_started_ns = self._monotonic_ns()
            try:
                rows.sort(key=lambda row: self._order_key(spec, row))
            finally:
                self._metrics.observe(
                    "sort",
                    self._monotonic_ns() - sort_started_ns,
                )
            arrow_started_ns = self._monotonic_ns()
            try:
                table = pa.Table.from_pylist(rows, schema=spec.schema)
            finally:
                self._metrics.observe(
                    "arrow_build",
                    self._monotonic_ns() - arrow_started_ns,
                )
            manifest = write_partition(
                self.root,
                PartitionKey(
                    venue=venue,
                    date=day,
                    asset=asset,
                    record_type=record_type,
                ),
                table,
                _timing_observer=self._metrics.observe,
                _monotonic_ns=self._monotonic_ns,
            )
            manifests.append(manifest)
            written += len(rows)

        if not selected_stable_primary_keys.issubset(self._pending_stable_primary_keys):
            raise RuntimeError("partial flush selected an untracked stable primary key")
        should_commit_index = final_barrier or bool(selected_stable_primary_keys)
        if self._observation_index is not None and should_commit_index:
            sqlite_started_ns = self._monotonic_ns()
            try:
                self._observation_index.commit(
                    list(self._pending_observations.values()) if final_barrier else [],
                    (
                        sorted(self._pending_stable_primary_keys)
                        if final_barrier
                        else sorted(selected_stable_primary_keys)
                    ),
                    manifests,
                )
            finally:
                self._metrics.observe(
                    "sqlite_commit",
                    self._monotonic_ns() - sqlite_started_ns,
                )

        for group_key in group_keys:
            del self._groups[group_key]
        self._pending_count -= written
        if self._pending_count < 0:
            raise RuntimeError("partial flush produced negative pending-row accounting")

        if final_barrier:
            self._pending_observations.clear()
            self._pending_stable_primary_keys.clear()
            duplicates = self._duplicate_count
            self._duplicate_count = 0
            if self._groups or self._pending_count:
                raise RuntimeError("full flush left pending groups or rows")
        else:
            self._pending_stable_primary_keys.difference_update(selected_stable_primary_keys)
            duplicates = 0
        return FlushResult(tuple(manifests), written, duplicates)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._observation_index is not None:
                self._observation_index.close()
        finally:
            self._writer_lock.close()

    @staticmethod
    def _primary_key(spec: SchemaSpec, row: dict[str, object]) -> tuple[object, ...]:
        try:
            values = tuple(row[name] for name in spec.primary_key)
        except KeyError as exc:
            raise ValueError(f"record is missing primary key field {exc.args[0]!r}") from None
        if any(value is None for value in values):
            raise ValueError("record primary key cannot contain null")
        return values

    @staticmethod
    def _order_key(spec: SchemaSpec, row: dict[str, object]) -> tuple[tuple[int, Any], ...]:
        return tuple((1, "") if row.get(name) is None else (0, row[name]) for name in spec.order_key)

    @staticmethod
    def _group_key(
        record_type: RecordType,
        asset: str,
        row: dict[str, object],
    ) -> _GroupKey:
        venue = row.get("venue")
        if not isinstance(venue, str) or not venue:
            raise ValueError("record venue must be a non-empty string")
        event_time = row.get("event_time")
        if not isinstance(event_time, datetime):
            raise ValueError("record event_time must be a datetime")
        day = event_time.date().isoformat()
        if record_type == RecordType.CANDLE:
            stream = f"interval={row.get('interval')}"
        elif record_type == RecordType.FUNDING:
            stream = f"kind={row.get('rate_kind')};seconds={row.get('funding_interval_seconds')}"
        else:
            stream = "default"
        return venue, record_type, asset, day, stream


class CoordinatedLakeSink:
    """Venue-scoped view over one process-wide, serialized lake writer."""

    def __init__(self, owner: CoordinatedLakeWriter, venue: str) -> None:
        self._owner = owner
        self.venue = venue
        self._closed = False

    @property
    def pending_count(self) -> int:
        return self._owner._client_pending(self)

    @property
    def should_flush(self) -> bool:
        return self._owner._client_should_flush(self)

    @property
    def high_water(self) -> int:
        return self._owner._client_high_water(self)

    def add(self, record: ParsedRecord) -> bool:
        return self._owner._client_add(self, record)

    def add_many(self, records: Iterable[ParsedRecord]) -> int:
        return self._owner._client_add_many(self, records)

    def flush(self) -> FlushResult:
        return self._owner._client_flush(self)

    def close(self) -> None:
        self._owner._close_client(self)


class CoordinatedLakeWriter:
    """Own exactly one root lock/index while serializing venue-scoped clients."""

    def __init__(
        self,
        root: Path,
        *,
        venues: tuple[str, ...],
        batch_size: int = 500,
        queue_capacity: int = 10_000,
        recent_key_capacity: int = 100_000,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if not venues or len(venues) != len(set(venues)) or any(not venue for venue in venues):
            raise ValueError("coordinated writer venues must be non-empty and unique")
        self.root = root
        self._venue_order = venues
        self._venues = frozenset(venues)
        self._lock = threading.RLock()
        self._sink = BatchingLakeSink(
            root,
            batch_size=batch_size,
            queue_capacity=queue_capacity,
            recent_key_capacity=recent_key_capacity,
            _serialized_cross_thread_access=True,
            monotonic_ns=monotonic_ns,
        )
        self._clients: dict[str, CoordinatedLakeSink] = {}
        self._pending_by_venue = {venue: 0 for venue in venues}
        self._high_water_by_venue = {venue: 0 for venue in venues}
        self._duplicates_since_flush = {venue: 0 for venue in venues}
        self._manifest_credit: dict[str, list[PartitionManifest]] = {venue: [] for venue in venues}
        self._row_credit = {venue: 0 for venue in venues}
        self._duplicate_credit = {venue: 0 for venue in venues}
        self._closed = False

    @property
    def pending_count(self) -> int:
        with self._lock:
            return self._sink.pending_count

    @property
    def should_flush(self) -> bool:
        with self._lock:
            return self._sink.should_flush

    def metrics_snapshot(self) -> dict[str, object]:
        return self._sink.metrics_snapshot()

    def client(self, venue: str) -> CoordinatedLakeSink:
        with self._lock:
            if self._closed:
                raise RuntimeError("coordinated lake writer is closed")
            if venue not in self._venues:
                raise ValueError(f"venue {venue!r} is not configured for this coordinated writer")
            if venue in self._clients:
                raise RuntimeError(f"coordinated lake writer already has a client for venue {venue!r}")
            client = CoordinatedLakeSink(self, venue)
            self._clients[venue] = client
            return client

    def _require_active(self, client: CoordinatedLakeSink) -> None:
        if self._closed:
            raise CoordinatedWriterError("coordinated lake writer is closed")
        if client._closed or self._clients.get(client.venue) is not client:
            raise CoordinatedWriterError(f"coordinated lake client for {client.venue!r} is closed")

    def _client_pending(self, client: CoordinatedLakeSink) -> int:
        with self._lock:
            self._require_active(client)
            return self._pending_by_venue[client.venue]

    def _client_should_flush(self, client: CoordinatedLakeSink) -> bool:
        with self._lock:
            self._require_active(client)
            return self._sink.should_flush

    def _client_high_water(self, client: CoordinatedLakeSink) -> int:
        with self._lock:
            self._require_active(client)
            return self._high_water_by_venue[client.venue]

    def _client_add(self, client: CoordinatedLakeSink, record: ParsedRecord) -> bool:
        with self._lock:
            self._require_active(client)
            row_venue = record.row.get("venue")
            if row_venue != client.venue:
                raise CoordinatedWriterError(
                    "coordinated lake client venue mismatch: "
                    f"expected {client.venue!r}, observed {row_venue!r}"
                )
            try:
                accepted = self._sink.add(record)
            except Exception as exc:
                raise CoordinatedWriterError(
                    f"coordinated lake add failed for venue {client.venue!r}"
                ) from exc
            if accepted:
                pending = self._pending_by_venue[client.venue] + 1
                self._pending_by_venue[client.venue] = pending
                self._high_water_by_venue[client.venue] = max(
                    self._high_water_by_venue[client.venue],
                    pending,
                )
            else:
                self._duplicates_since_flush[client.venue] += 1
            return accepted

    def _client_add_many(
        self,
        client: CoordinatedLakeSink,
        records: Iterable[ParsedRecord],
    ) -> int:
        batch = tuple(records)
        with self._lock:
            self._require_active(client)
            mismatched = sorted(
                {str(record.row.get("venue")) for record in batch if record.row.get("venue") != client.venue}
            )
            if mismatched:
                raise CoordinatedWriterError(
                    "coordinated lake client venue mismatch: "
                    f"expected {client.venue!r}, observed {mismatched!r}"
                )
            try:
                if batch and self._sink.should_flush:
                    self._record_flush(self._sink.flush_ready(), full_barrier=False)
                accepted = self._sink.add_many(batch)
            except Exception as exc:
                raise CoordinatedWriterError(
                    f"coordinated lake atomic add failed for venue {client.venue!r}"
                ) from exc
            duplicates = len(batch) - accepted
            pending = self._pending_by_venue[client.venue] + accepted
            self._pending_by_venue[client.venue] = pending
            self._high_water_by_venue[client.venue] = max(
                self._high_water_by_venue[client.venue],
                pending,
            )
            self._duplicates_since_flush[client.venue] += duplicates
            return accepted

    def _record_flush(self, result: FlushResult, *, full_barrier: bool) -> None:
        expected_duplicates = sum(self._duplicates_since_flush.values())
        if full_barrier and result.duplicate_count != expected_duplicates:
            raise CoordinatedWriterError(
                "coordinated writer duplicate accounting mismatch: "
                f"sink={result.duplicate_count}, clients={expected_duplicates}"
            )
        if not full_barrier and result.duplicate_count != 0:
            raise CoordinatedWriterError("coordinated writer partial flush returned duplicate credit")
        rows_by_venue = {venue: 0 for venue in self._venues}
        manifests_by_venue: dict[str, list[PartitionManifest]] = {venue: [] for venue in self._venues}
        for manifest in result.manifests:
            venue = manifest.partition.venue
            if venue not in self._venues:
                raise CoordinatedWriterError(f"coordinated writer published an incompatible venue: {venue!r}")
            rows_by_venue[venue] += manifest.row_count
            manifests_by_venue[venue].append(manifest)
        if sum(rows_by_venue.values()) != result.row_count:
            raise CoordinatedWriterError(
                "coordinated writer flush row-count mismatch: "
                f"manifests={sum(rows_by_venue.values())}, result={result.row_count}"
            )
        for venue in self._venues:
            pending = self._pending_by_venue[venue]
            if (full_barrier and rows_by_venue[venue] != pending) or (
                not full_barrier and rows_by_venue[venue] > pending
            ):
                raise CoordinatedWriterError(
                    "coordinated writer row accounting mismatch for "
                    f"{venue}: manifests={rows_by_venue[venue]}, "
                    f"pending={pending}, full_barrier={full_barrier}"
                )

        for venue in self._venues:
            self._manifest_credit[venue].extend(manifests_by_venue[venue])
            self._row_credit[venue] += rows_by_venue[venue]
            self._pending_by_venue[venue] -= rows_by_venue[venue]
            if full_barrier:
                self._duplicate_credit[venue] += self._duplicates_since_flush[venue]
                self._duplicates_since_flush[venue] = 0

    def _drain_all_credits(self) -> dict[str, FlushResult]:
        results = {
            venue: FlushResult(
                tuple(self._manifest_credit[venue]),
                self._row_credit[venue],
                self._duplicate_credit[venue],
            )
            for venue in self._venue_order
        }
        for venue in self._venue_order:
            self._manifest_credit[venue].clear()
            self._row_credit[venue] = 0
            self._duplicate_credit[venue] = 0
        return results

    def flush_ready_all(self) -> dict[str, FlushResult]:
        """Publish ready exact groups and retain sparse groups for a full barrier."""

        with self._lock:
            if self._closed:
                raise CoordinatedWriterError("coordinated lake writer is closed")
            try:
                physical_result = self._sink.flush_ready()
            except Exception as exc:
                raise CoordinatedWriterError(
                    "coordinated lake ready-group flush failed for all venues"
                ) from exc
            self._record_flush(physical_result, full_barrier=False)
            return self._drain_all_credits()

    def flush_all(self) -> dict[str, FlushResult]:
        """Physically flush once and atomically drain credits for every venue."""

        with self._lock:
            if self._closed:
                raise CoordinatedWriterError("coordinated lake writer is closed")
            try:
                physical_result = self._sink.flush()
            except Exception as exc:
                raise CoordinatedWriterError("coordinated lake flush failed for all venues") from exc
            self._record_flush(physical_result, full_barrier=True)
            return self._drain_all_credits()

    def _client_flush(self, client: CoordinatedLakeSink) -> FlushResult:
        with self._lock:
            self._require_active(client)
            try:
                physical_result = self._sink.flush()
            except Exception as exc:
                raise CoordinatedWriterError(
                    f"coordinated lake flush failed for venue {client.venue!r}"
                ) from exc
            self._record_flush(physical_result, full_barrier=True)
            venue = client.venue
            result = FlushResult(
                tuple(self._manifest_credit[venue]),
                self._row_credit[venue],
                self._duplicate_credit[venue],
            )
            self._manifest_credit[venue].clear()
            self._row_credit[venue] = 0
            self._duplicate_credit[venue] = 0
            return result

    def _close_client(self, client: CoordinatedLakeSink) -> None:
        with self._lock:
            if client._closed:
                return
            if self._clients.get(client.venue) is not client:
                raise RuntimeError("coordinated lake client does not belong to this writer")
            client._closed = True

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._record_flush(self._sink.flush(), full_barrier=True)
            finally:
                self._sink.close()
