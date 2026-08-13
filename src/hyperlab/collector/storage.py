from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, BinaryIO, Protocol

import pyarrow as pa

from hyperlab.collector.models import ParsedRecord
from hyperlab.data.lake import (
    PartitionKey,
    PartitionManifest,
    discover_partitions,
    read_hashed_table,
    recover_partition_manifest,
    validate_partition,
    write_partition,
)
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
_OBSERVATION_INDEX_VERSION = 3
_PERSISTENT_PRIMARY_KEY_TYPES = frozenset({RecordType.TRADE})


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
    canonical = _canonical_json(primary_key)
    return (
        f"{record_type.value}:v{schema_version}",
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
        root.mkdir(parents=True, exist_ok=True)
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
    for data_path in sorted(root.rglob("part-*.parquet"), key=lambda path: path.as_posix()):
        manifest_path = data_path.with_name(f"{data_path.stem}.manifest.json")
        if manifest_path.exists() or (root / ".recovery") in data_path.parents:
            continue
        recover_partition_manifest(root, data_path)


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
    ) -> None:
        if batch_size <= 0 or queue_capacity < batch_size or recent_key_capacity <= 0:
            raise ValueError("invalid batch or queue capacity")
        self.root = root
        self._writer_lock = _RootWriterLock(root)
        self._observation_index: _PersistentObservationIndex | None = None
        self.batch_size = batch_size
        self.queue_capacity = queue_capacity
        self.recent_key_capacity = recent_key_capacity
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
        return self._pending_count >= self.batch_size

    def add(self, record: ParsedRecord) -> bool:
        spec = latest_schema_for(record.record_type)
        row = dict(record.row)
        row["schema_version"] = spec.version
        partition_asset = record.asset
        primary_key = self._primary_key(spec, row)
        recent_key = (record.record_type, primary_key)
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
            head_key = observation[:2]
            cached_payload = self._observations.get(head_key)
            if cached_payload == observation[2] or (
                cached_payload is None
                and self._observation_index is not None
                and self._observation_index.contains(observation)
            ):
                self._duplicate_count += 1
                return False
        if recent_key in self._recent:
            self._recent.move_to_end(recent_key)
            self._duplicate_count += 1
            return False

        group_key = self._group_key(record.record_type, partition_asset, row)
        group = self._groups.get(group_key)
        if group is not None and primary_key in group:
            group[primary_key] = row
            self._duplicate_count += 1
            return False
        if self._pending_count >= self.queue_capacity:
            raise BufferError("collector queue capacity exceeded; no record was dropped")
        group = self._groups.setdefault(group_key, OrderedDict())

        group[primary_key] = row
        self._pending_count += 1
        self.high_water = max(self.high_water, self._pending_count)
        self._recent[recent_key] = None
        if len(self._recent) > self.recent_key_capacity:
            self._recent.popitem(last=False)
        if observation is not None:
            head_key = observation[:2]
            self._observations[head_key] = observation[2]
            self._observations.move_to_end(head_key)
            self._pending_observations[head_key] = observation
            self._pending_observations.move_to_end(head_key)
            if len(self._observations) > self.recent_key_capacity:
                self._observations.popitem(last=False)
        if stable_primary_key is not None:
            self._pending_stable_primary_keys.add(stable_primary_key)
        return True

    def add_many(self, records: Iterable[ParsedRecord]) -> int:
        """Add one logical source batch without exposing a partial batch to a flush."""

        batch = tuple(records)
        if len(batch) > self.queue_capacity - self._pending_count:
            raise BufferError("collector queue capacity exceeded before atomic batch; no record was added")
        groups = {key: group.copy() for key, group in self._groups.items()}
        recent = self._recent.copy()
        observations = self._observations.copy()
        pending_observations = self._pending_observations.copy()
        pending_stable_primary_keys = self._pending_stable_primary_keys.copy()
        pending_count = self._pending_count
        duplicate_count = self._duplicate_count
        high_water = self.high_water
        try:
            return sum(self.add(record) for record in batch)
        except BaseException:
            self._groups = groups
            self._recent = recent
            self._observations = observations
            self._pending_observations = pending_observations
            self._pending_stable_primary_keys = pending_stable_primary_keys
            self._pending_count = pending_count
            self._duplicate_count = duplicate_count
            self.high_water = high_water
            raise

    def flush(self) -> FlushResult:
        if not self._groups:
            duplicates = self._duplicate_count
            self._duplicate_count = 0
            return FlushResult((), 0, duplicates)

        manifests: list[PartitionManifest] = []
        written = 0
        for group_key in sorted(
            self._groups,
            key=lambda item: (item[3], item[2], item[0], item[1].value, item[4]),
        ):
            venue, record_type, asset, day, _stream = group_key
            spec = latest_schema_for(record_type)
            rows = list(self._groups[group_key].values())
            rows.sort(key=lambda row: self._order_key(spec, row))
            table = pa.Table.from_pylist(rows, schema=spec.schema)
            manifest = write_partition(
                self.root,
                PartitionKey(
                    venue=venue,
                    date=day,
                    asset=asset,
                    record_type=record_type,
                ),
                table,
            )
            manifests.append(manifest)
            written += len(rows)

        if self._observation_index is not None:
            self._observation_index.commit(
                list(self._pending_observations.values()),
                sorted(self._pending_stable_primary_keys),
                manifests,
            )
        self._pending_observations.clear()
        self._pending_stable_primary_keys.clear()
        duplicates = self._duplicate_count
        self._groups.clear()
        self._pending_count = 0
        self._duplicate_count = 0
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
    ) -> None:
        if not venues or len(venues) != len(set(venues)) or any(not venue for venue in venues):
            raise ValueError("coordinated writer venues must be non-empty and unique")
        self.root = root
        self._venues = frozenset(venues)
        self._lock = threading.RLock()
        self._sink = BatchingLakeSink(
            root,
            batch_size=batch_size,
            queue_capacity=queue_capacity,
            recent_key_capacity=recent_key_capacity,
            _serialized_cross_thread_access=True,
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
                    self._record_flush(self._sink.flush())
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

    def _record_flush(self, result: FlushResult) -> None:
        expected_duplicates = sum(self._duplicates_since_flush.values())
        if result.duplicate_count != expected_duplicates:
            raise CoordinatedWriterError(
                "coordinated writer duplicate accounting mismatch: "
                f"sink={result.duplicate_count}, clients={expected_duplicates}"
            )
        rows_by_venue = {venue: 0 for venue in self._venues}
        for manifest in result.manifests:
            venue = manifest.partition.venue
            if venue not in self._venues:
                raise CoordinatedWriterError(f"coordinated writer published an incompatible venue: {venue!r}")
            rows_by_venue[venue] += manifest.row_count
            self._manifest_credit[venue].append(manifest)
        for venue in self._venues:
            if rows_by_venue[venue] != self._pending_by_venue[venue]:
                raise CoordinatedWriterError(
                    "coordinated writer row accounting mismatch for "
                    f"{venue}: manifests={rows_by_venue[venue]}, "
                    f"pending={self._pending_by_venue[venue]}"
                )
            self._row_credit[venue] += rows_by_venue[venue]
            self._duplicate_credit[venue] += self._duplicates_since_flush[venue]
            self._pending_by_venue[venue] = 0
            self._duplicates_since_flush[venue] = 0

    def _client_flush(self, client: CoordinatedLakeSink) -> FlushResult:
        with self._lock:
            self._require_active(client)
            try:
                physical_result = self._sink.flush()
            except Exception as exc:
                raise CoordinatedWriterError(
                    f"coordinated lake flush failed for venue {client.venue!r}"
                ) from exc
            self._record_flush(physical_result)
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
                self._record_flush(self._sink.flush())
            finally:
                self._sink.close()
