from __future__ import annotations

import base64
import os
import pickle
import shutil
import sqlite3
import tempfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Generic, TypeVar, cast

import pandas as pd

from hyperlab.data.schema import RecordType

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_UINT64_MAX = (1 << 64) - 1
SQLITE_CACHE_LIMIT_BYTES = 32 * 1024 * 1024
SQLITE_COMMIT_INTERVAL_ROWS = 8_192
_SpillT = TypeVar("_SpillT", bound=tuple[object, ...])


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("continuity row-store timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp_ns(value: datetime) -> int:
    delta = _utc(value) - _EPOCH
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
        + int(getattr(value, "nanosecond", 0))
    )


def _from_timestamp_ns(value: int) -> datetime:
    return pd.Timestamp(value, unit="ns", tz=UTC)


def _uint64_sort_text(value: object) -> str:
    normalized = int(str(value))
    if not 0 <= normalized <= _UINT64_MAX:
        raise ValueError("continuity scratch uint64 value is out of range")
    return f"{normalized:020d}"


def _encode_order_key(value: tuple[tuple[int, object], ...]) -> str:
    payload = pickle.dumps(value, protocol=5)
    return base64.b85encode(payload).decode("ascii")


def _decode_order_key(value: str) -> tuple[tuple[int, object], ...]:
    decoded = pickle.loads(base64.b85decode(value.encode("ascii")))
    if not isinstance(decoded, tuple):
        raise ValueError("invalid continuity integrity order key")
    return cast(tuple[tuple[int, object], ...], decoded)


def _compare_encoded_order_keys(left: str, right: str) -> int:
    left_key = _decode_order_key(left)
    right_key = _decode_order_key(right)
    return (left_key > right_key) - (left_key < right_key)


@dataclass(slots=True)
class StreamingMetrics:
    """Deterministic counters plus explicitly non-semantic elapsed timings."""

    manifest_files_discovered: int = 0
    manifest_files_validated: int = 0
    manifest_files_selected: int = 0
    manifest_files_pruned: int = 0
    rows_validated: int = 0
    parquet_file_scan_operations: int = 0
    unique_parquet_files_scanned: int = 0
    rows_scanned: int = 0
    semantic_rows_scanned: int = 0
    rows_staged: int = 0
    rows_scanned_by_record_type: dict[str, int] = field(default_factory=dict)
    record_batches_scanned: int = 0
    max_record_batch_rows: int = 0
    max_file_rows: int = 0
    max_file_size_bytes: int = 0
    max_python_rows_per_batch: int = 0
    max_boundary_candidates: int = 0
    wire_identity_keys: int = 0
    integrity_primary_keys_spilled: int = 0
    integrity_l2_metadata_keys_spilled: int = 0
    integrity_cadence_rows_spilled: int = 0
    spilled_timestamp_rows: int = 0
    spilled_sequence_rows: int = 0
    spilled_set_keys: int = 0
    peak_scratch_bytes: int = 0
    sqlite_commits: int = 0
    max_uncommitted_rows: int = 0
    phase_elapsed_seconds: dict[str, float] = field(default_factory=dict)

    def observe_batch(
        self,
        record_type: RecordType,
        rows: int,
        *,
        semantic_scan: bool,
    ) -> None:
        self.record_batches_scanned += 1
        self.rows_scanned += rows
        self.max_record_batch_rows = max(self.max_record_batch_rows, rows)
        self.max_python_rows_per_batch = max(self.max_python_rows_per_batch, rows)
        if semantic_scan:
            self.semantic_rows_scanned += rows
        key = record_type.value
        self.rows_scanned_by_record_type[key] = self.rows_scanned_by_record_type.get(key, 0) + rows

    def observe_phase(self, phase: str, elapsed_seconds: float) -> None:
        self.phase_elapsed_seconds[phase] = round(max(elapsed_seconds, 0.0), 6)

    def as_dict(self) -> dict[str, object]:
        return {
            "semantic": False,
            "files": {
                "manifest_files_discovered": self.manifest_files_discovered,
                "manifest_files_validated": self.manifest_files_validated,
                "manifest_files_selected": self.manifest_files_selected,
                "manifest_files_pruned": self.manifest_files_pruned,
                "unique_parquet_files_scanned": self.unique_parquet_files_scanned,
                "parquet_file_scan_operations": self.parquet_file_scan_operations,
            },
            "rows": {
                "validated_total": self.rows_validated,
                "scanned_total": self.rows_scanned,
                "semantic_scanned_total": self.semantic_rows_scanned,
                "staged_total": self.rows_staged,
                "scanned_by_record_type": dict(sorted(self.rows_scanned_by_record_type.items())),
            },
            "bounded_state": {
                "record_batches_scanned": self.record_batches_scanned,
                "max_record_batch_rows": self.max_record_batch_rows,
                "max_file_rows": self.max_file_rows,
                "max_file_size_bytes": self.max_file_size_bytes,
                "max_python_rows_per_batch": self.max_python_rows_per_batch,
                "max_boundary_candidates": self.max_boundary_candidates,
                "wire_identity_keys": self.wire_identity_keys,
                "integrity_primary_keys_spilled": (
                    self.integrity_primary_keys_spilled
                ),
                "integrity_l2_metadata_keys_spilled": (
                    self.integrity_l2_metadata_keys_spilled
                ),
                "integrity_cadence_rows_spilled": (
                    self.integrity_cadence_rows_spilled
                ),
                "sqlite_cache_limit_bytes": SQLITE_CACHE_LIMIT_BYTES,
                "sqlite_commit_interval_rows": SQLITE_COMMIT_INTERVAL_ROWS,
                "sqlite_commits": self.sqlite_commits,
                "max_uncommitted_rows": self.max_uncommitted_rows,
                "sqlite_mmap_bytes": 0,
                "spilled_timestamp_rows": self.spilled_timestamp_rows,
                "spilled_sequence_rows": self.spilled_sequence_rows,
                "spilled_set_keys": self.spilled_set_keys,
                "peak_scratch_bytes": self.peak_scratch_bytes,
            },
            "elapsed_seconds_by_phase": dict(sorted(self.phase_elapsed_seconds.items())),
        }


class BoundedRowStore:
    """Disk-backed projected rows with a fixed SQLite page-cache budget.

    The store is scratch state only. It never lives inside the immutable lake and
    is removed after the audit. Row payloads contain tuples for one projected
    Arrow schema; column names are interned once per encoding instead of repeated
    for every row.
    """

    def __init__(
        self,
        metrics: StreamingMetrics,
        *,
        scratch_parent: Path | None = None,
    ) -> None:
        self.metrics = metrics
        parent = None if scratch_parent is None else str(scratch_parent)
        self.directory = Path(tempfile.mkdtemp(prefix="hyperlab-continuity-", dir=parent))
        self.path = self.directory / "rows.sqlite3"
        self.connection = sqlite3.connect(self.path)
        self.connection.create_collation(
            "HYPERLAB_PYTHON_ORDER",
            _compare_encoded_order_keys,
        )
        self.connection.execute("PRAGMA journal_mode=OFF")
        self.connection.execute("PRAGMA synchronous=OFF")
        self.connection.execute("PRAGMA temp_store=FILE")
        self.connection.execute("PRAGMA mmap_size=0")
        self.connection.execute(f"PRAGMA cache_size=-{SQLITE_CACHE_LIMIT_BYTES // 1024}")
        self.connection.executescript(
            """
            CREATE TABLE encodings (
                encoding_id INTEGER PRIMARY KEY,
                columns_blob BLOB NOT NULL UNIQUE
            );
            CREATE TABLE rows (
                row_id INTEGER PRIMARY KEY,
                venue TEXT NOT NULL,
                record_type TEXT NOT NULL,
                asset TEXT NOT NULL,
                population TEXT NOT NULL,
                received_ns INTEGER NOT NULL,
                connection_id TEXT NOT NULL,
                connection_epoch TEXT,
                capture_epoch_id TEXT NOT NULL,
                message_asset TEXT NOT NULL,
                channel TEXT NOT NULL,
                snapshot_id TEXT NOT NULL,
                event_kind TEXT NOT NULL,
                source_sequence_text TEXT NOT NULL,
                source_sequence_sort TEXT NOT NULL,
                arrival_sequence TEXT,
                manifest_order INTEGER NOT NULL,
                row_order INTEGER NOT NULL,
                encoding_id INTEGER NOT NULL,
                payload BLOB NOT NULL
            );
            CREATE TABLE timestamps (
                series_id INTEGER NOT NULL,
                timestamp_ns INTEGER NOT NULL,
                ordinal INTEGER NOT NULL,
                PRIMARY KEY (series_id, timestamp_ns, ordinal)
            ) WITHOUT ROWID;
            CREATE TABLE spill_sequences (
                sequence_id INTEGER NOT NULL,
                ordinal INTEGER NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY (sequence_id, ordinal)
            ) WITHOUT ROWID;
            CREATE TABLE spill_sets (
                set_id INTEGER NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY (set_id, payload)
            ) WITHOUT ROWID;
            CREATE TABLE integrity_datasets (
                dataset_id INTEGER PRIMARY KEY,
                venue TEXT NOT NULL,
                asset TEXT NOT NULL,
                record_type TEXT NOT NULL,
                compatibility_family INTEGER NOT NULL,
                UNIQUE(venue, asset, record_type, compatibility_family)
            );
            CREATE TABLE integrity_primary_keys (
                dataset_id INTEGER NOT NULL,
                primary_key BLOB NOT NULL,
                PRIMARY KEY(dataset_id, primary_key)
            ) WITHOUT ROWID;
            CREATE TABLE integrity_l2_metadata (
                dataset_id INTEGER NOT NULL,
                identifier TEXT NOT NULL,
                metadata BLOB NOT NULL,
                PRIMARY KEY(dataset_id, identifier)
            ) WITHOUT ROWID;
            CREATE TABLE integrity_streams (
                stream_id INTEGER PRIMARY KEY,
                venue TEXT NOT NULL,
                asset TEXT NOT NULL,
                record_type TEXT NOT NULL,
                compatibility_family INTEGER NOT NULL,
                stream_key TEXT NOT NULL,
                expected_interval_ns INTEGER,
                expected_interval_is_null INTEGER NOT NULL,
                UNIQUE(
                    venue, asset, record_type, compatibility_family, stream_key
                )
            );
            CREATE TABLE integrity_cadence_rows (
                stream_id INTEGER NOT NULL,
                order_key TEXT COLLATE HYPERLAB_PYTHON_ORDER NOT NULL,
                manifest_order INTEGER NOT NULL,
                row_order INTEGER NOT NULL,
                cadence_ns INTEGER NOT NULL,
                manifest_id TEXT NOT NULL,
                partition_path TEXT NOT NULL,
                PRIMARY KEY(stream_id, order_key, manifest_order, row_order)
            ) WITHOUT ROWID;
            CREATE TABLE artifacts (
                relative_path TEXT PRIMARY KEY,
                stem_key TEXT NOT NULL,
                kind TEXT NOT NULL
            );
            CREATE INDEX artifacts_stem_idx ON artifacts(stem_key, kind);
            """
        )
        self._encoding_ids: dict[tuple[str, ...], int] = {}
        self._encoding_columns: dict[int, tuple[str, ...]] = {}
        self._next_series_id = 1
        self._next_sequence_id = 1
        self._next_set_id = 1
        self._next_spill_ordinal = 1
        self._uncommitted_rows = 0
        self._indexes_ready = False
        self._closed = False

    def _observe_mutations(self, rows: int) -> None:
        if rows <= 0:
            return
        self._uncommitted_rows += rows
        self.metrics.max_uncommitted_rows = max(
            self.metrics.max_uncommitted_rows,
            self._uncommitted_rows,
        )
        if self._uncommitted_rows >= SQLITE_COMMIT_INTERVAL_ROWS:
            self._commit_pending()

    def _commit_pending(self) -> None:
        if self._uncommitted_rows <= 0:
            return
        self.connection.commit()
        self._uncommitted_rows = 0
        self.metrics.sqlite_commits += 1
        self.observe_scratch_size()

    def _encoding_id(self, columns: Sequence[str]) -> int:
        key = tuple(columns)
        existing = self._encoding_ids.get(key)
        if existing is not None:
            return existing
        payload = pickle.dumps(key, protocol=5)
        cursor = self.connection.execute(
            "INSERT INTO encodings(columns_blob) VALUES (?)",
            (sqlite3.Binary(payload),),
        )
        lastrowid = cursor.lastrowid
        if lastrowid is None:
            raise ValueError("continuity scratch encoding insert returned no row id")
        encoding_id = lastrowid
        self._encoding_ids[key] = encoding_id
        self._encoding_columns[encoding_id] = key
        self._observe_mutations(1)
        return encoding_id

    def insert_batch(
        self,
        *,
        venue: str,
        record_type: RecordType,
        asset: str,
        population: str,
        manifest_order: int,
        first_row_order: int,
        columns: Sequence[str],
        rows: Sequence[Mapping[str, object]],
        row_orders: Sequence[int] | None = None,
    ) -> None:
        if population not in {"in", "boundary", "cadence"}:
            raise ValueError(f"invalid continuity row population: {population}")
        encoding_id = self._encoding_id(columns)
        encoded: list[tuple[object, ...]] = []
        if row_orders is not None and len(row_orders) != len(rows):
            raise ValueError("continuity row_orders must match rows")
        for offset, row in enumerate(rows):
            received = row.get("received_time")
            if not isinstance(received, datetime):
                raise ValueError("continuity row received_time must be a timestamp")
            connection_epoch = row.get("connection_epoch")
            arrival_sequence = row.get("arrival_sequence")
            source_sequence = row.get("source_sequence")
            values = tuple(row.get(column) for column in columns)
            encoded.append(
                (
                    venue,
                    record_type.value,
                    asset,
                    population,
                    _timestamp_ns(received),
                    str(row.get("connection_id") or ""),
                    (
                        None
                        if connection_epoch is None
                        else _uint64_sort_text(connection_epoch)
                    ),
                    str(row.get("capture_epoch_id") or ""),
                    str(row.get("message_asset") or ""),
                    str(row.get("channel") or ""),
                    str(row.get("snapshot_id") or ""),
                    str(row.get("event_kind") or ""),
                    "" if source_sequence is None else str(source_sequence),
                    (
                        ""
                        if source_sequence is None
                        else _uint64_sort_text(source_sequence)
                    ),
                    (
                        None
                        if arrival_sequence is None
                        else _uint64_sort_text(arrival_sequence)
                    ),
                    manifest_order,
                    (first_row_order + offset if row_orders is None else int(row_orders[offset])),
                    encoding_id,
                    sqlite3.Binary(pickle.dumps(values, protocol=5)),
                )
            )
        self.connection.executemany(
            """
            INSERT INTO rows(
                venue, record_type, asset, population, received_ns,
                connection_id, connection_epoch, capture_epoch_id,
                message_asset, channel, snapshot_id, event_kind,
                source_sequence_text, source_sequence_sort, arrival_sequence, manifest_order,
                row_order, encoding_id, payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            encoded,
        )
        self.metrics.rows_staged += len(encoded)
        self._observe_mutations(len(encoded))
        self.observe_scratch_size()

    def finalize_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._commit_pending()
        self.connection.executescript(
            """
            CREATE INDEX rows_selection_idx ON rows(
                venue, record_type, asset, population, received_ns
            );
            CREATE INDEX rows_frame_idx ON rows(
                venue, record_type, asset, connection_id, received_ns
            );
            CREATE INDEX rows_snapshot_idx ON rows(
                venue, record_type, asset, population, snapshot_id,
                received_ns, connection_id, manifest_order, row_order
            );
            CREATE INDEX rows_wire_sequence_idx ON rows(
                venue, record_type, connection_id, connection_epoch,
                arrival_sequence
            );
            CREATE INDEX rows_wire_frame_idx ON rows(
                venue, record_type, population, connection_id, received_ns,
                message_asset, manifest_order, row_order
            );
            """
        )
        self.connection.commit()
        self._indexes_ready = True
        self.observe_scratch_size()

    def _columns_for_encoding(self, encoding_id: int) -> tuple[str, ...]:
        cached = self._encoding_columns.get(encoding_id)
        if cached is not None:
            return cached
        row = self.connection.execute(
            "SELECT columns_blob FROM encodings WHERE encoding_id = ?",
            (encoding_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"missing continuity row encoding {encoding_id}")
        decoded = pickle.loads(bytes(row[0]))
        if not isinstance(decoded, tuple) or not all(isinstance(value, str) for value in decoded):
            raise ValueError(f"invalid continuity row encoding {encoding_id}")
        self._encoding_columns[encoding_id] = decoded
        return decoded

    def iter_rows(
        self,
        venue: str,
        record_type: RecordType,
        asset: str | None = None,
        *,
        with_boundaries: bool = False,
        population: str | None = None,
        filters: Mapping[str, object] | None = None,
        order_by: str = "reference",
    ) -> Iterator[dict[str, object]]:
        clauses = ["venue = ?", "record_type = ?"]
        parameters: list[object] = [venue, record_type.value]
        if asset is not None:
            clauses.append("asset = ?")
            parameters.append(asset)
        if population is not None:
            clauses.append("population = ?")
            parameters.append(population)
        elif with_boundaries:
            clauses.append("population IN ('in', 'boundary')")
        else:
            clauses.append("population = 'in'")
        filter_columns = {
            "connection_id",
            "connection_epoch",
            "capture_epoch_id",
            "message_asset",
            "channel",
            "snapshot_id",
            "event_kind",
            "arrival_sequence",
        }
        for name, value in (filters or {}).items():
            if name == "received_time":
                if not isinstance(value, datetime):
                    raise TypeError("received_time row filter must be a timestamp")
                clauses.append("received_ns = ?")
                parameters.append(_timestamp_ns(value))
            elif name in filter_columns:
                clauses.append(f"{name} = ?")
                parameters.append(
                    _uint64_sort_text(value)
                    if name in {"connection_epoch", "arrival_sequence"}
                    else value
                )
            else:
                raise ValueError(f"unsupported continuity row filter: {name}")
        if order_by == "received_arrival":
            ordering = """
                received_ns, COALESCE(arrival_sequence, ''),
                connection_id, manifest_order, row_order
            """
        elif order_by == "received_source":
            ordering = """
                received_ns, source_sequence_sort,
                connection_id, manifest_order, row_order
            """
        elif order_by == "snapshot_group":
            ordering = """
                snapshot_id, received_ns, connection_id,
                manifest_order, row_order
            """
        elif order_by != "reference":
            raise ValueError(f"unsupported continuity row ordering: {order_by}")
        elif with_boundaries:
            ordering = """
                received_ns,
                CASE population WHEN 'in' THEN 0 ELSE 1 END,
                asset,
                CASE population WHEN 'in' THEN connection_id ELSE '' END,
                CASE population WHEN 'in' THEN source_sequence_text ELSE '' END,
                manifest_order,
                row_order
            """
        else:
            ordering = """
                received_ns, asset, connection_id, source_sequence_text,
                manifest_order, row_order
            """
        cursor = self.connection.execute(
            "SELECT encoding_id, payload FROM rows WHERE " + " AND ".join(clauses) + " ORDER BY " + ordering,
            parameters,
        )
        for encoding_id, payload in cursor:
            columns = self._columns_for_encoding(int(encoding_id))
            values = pickle.loads(bytes(payload))
            if not isinstance(values, tuple) or len(values) != len(columns):
                raise ValueError("invalid continuity scratch row payload")
            yield dict(zip(columns, values, strict=True))

    def new_timestamp_series(self) -> SpilledTimestampSeries:
        series_id = self._next_series_id
        self._next_series_id += 1
        return SpilledTimestampSeries(self, series_id)

    def new_sequence(self) -> SpilledSequence[_SpillT]:
        sequence_id = self._next_sequence_id
        self._next_sequence_id += 1
        return SpilledSequence(self, sequence_id)

    def new_set(self) -> SpilledSet:
        set_id = self._next_set_id
        self._next_set_id += 1
        return SpilledSet(self, set_id)

    def integrity_dataset_id(
        self,
        venue: str,
        asset: str,
        record_type: str,
        compatibility_family: int,
    ) -> int:
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO integrity_datasets(
                venue, asset, record_type, compatibility_family
            ) VALUES (?, ?, ?, ?)
            """,
            (venue, asset, record_type, compatibility_family),
        )
        row = self.connection.execute(
            """
            SELECT dataset_id FROM integrity_datasets
            WHERE venue = ? AND asset = ? AND record_type = ?
              AND compatibility_family = ?
            """,
            (venue, asset, record_type, compatibility_family),
        ).fetchone()
        if row is None:
            raise ValueError("continuity integrity dataset insert returned no row")
        del cursor
        self._observe_mutations(1)
        return int(row[0])

    def integrity_stream_id(
        self,
        venue: str,
        asset: str,
        record_type: str,
        compatibility_family: int,
        stream_key: str,
        expected_interval_ns: int | None,
    ) -> tuple[int, bool]:
        self.connection.execute(
            """
            INSERT OR IGNORE INTO integrity_streams(
                venue, asset, record_type, compatibility_family, stream_key,
                expected_interval_ns, expected_interval_is_null
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                venue,
                asset,
                record_type,
                compatibility_family,
                stream_key,
                expected_interval_ns,
                int(expected_interval_ns is None),
            ),
        )
        row = self.connection.execute(
            """
            SELECT stream_id, expected_interval_ns, expected_interval_is_null
            FROM integrity_streams
            WHERE venue = ? AND asset = ? AND record_type = ?
              AND compatibility_family = ? AND stream_key = ?
            """,
            (venue, asset, record_type, compatibility_family, stream_key),
        ).fetchone()
        if row is None:
            raise ValueError("continuity integrity stream insert returned no row")
        observed_interval = None if bool(row[2]) else int(row[1])
        self._observe_mutations(1)
        return int(row[0]), observed_interval == expected_interval_ns

    def add_integrity_primary_keys(
        self,
        dataset_id: int,
        values: Sequence[tuple[object, ...]],
    ) -> bool:
        encoded = [(dataset_id, sqlite3.Binary(pickle.dumps(value, protocol=5))) for value in values]
        try:
            self.connection.executemany(
                """
                INSERT INTO integrity_primary_keys(dataset_id, primary_key)
                VALUES (?, ?)
                """,
                encoded,
            )
        except sqlite3.IntegrityError:
            return False
        self.metrics.integrity_primary_keys_spilled += len(encoded)
        self._observe_mutations(len(encoded))
        return True

    def add_integrity_l2_metadata(
        self,
        dataset_id: int,
        values: Mapping[str, tuple[object, ...]],
    ) -> str | None:
        for identifier, metadata in values.items():
            encoded = sqlite3.Binary(pickle.dumps(metadata, protocol=5))
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO integrity_l2_metadata(
                    dataset_id, identifier, metadata
                ) VALUES (?, ?, ?)
                """,
                (dataset_id, identifier, encoded),
            )
            if cursor.rowcount:
                self.metrics.integrity_l2_metadata_keys_spilled += 1
                continue
            row = self.connection.execute(
                """
                SELECT metadata FROM integrity_l2_metadata
                WHERE dataset_id = ? AND identifier = ?
                """,
                (dataset_id, identifier),
            ).fetchone()
            if row is None or bytes(row[0]) != bytes(encoded):
                return identifier
        self._observe_mutations(len(values))
        return None

    def add_integrity_cadence_rows(
        self,
        stream_id: int,
        rows: Sequence[tuple[tuple[tuple[int, object], ...], int, int, int, str, str]],
    ) -> None:
        encoded = [
            (
                stream_id,
                _encode_order_key(order_key),
                manifest_order,
                row_order,
                cadence_ns,
                manifest_id,
                partition_path,
            )
            for (
                order_key,
                manifest_order,
                row_order,
                cadence_ns,
                manifest_id,
                partition_path,
            ) in rows
        ]
        self.connection.executemany(
            """
            INSERT INTO integrity_cadence_rows(
                stream_id, order_key, manifest_order, row_order, cadence_ns,
                manifest_id, partition_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            encoded,
        )
        self.metrics.integrity_cadence_rows_spilled += len(encoded)
        self._observe_mutations(len(encoded))

    def iter_integrity_cadence_rows(
        self,
    ) -> Iterator[tuple[int, str, str, int, int, str, str]]:
        cursor = self.connection.execute(
            """
            SELECT rows.stream_id, streams.record_type, streams.asset,
                   streams.expected_interval_ns, rows.cadence_ns,
                   rows.manifest_id, rows.partition_path
            FROM integrity_cadence_rows AS rows
            JOIN integrity_streams AS streams
              ON streams.stream_id = rows.stream_id
            ORDER BY rows.stream_id,
                     rows.order_key COLLATE HYPERLAB_PYTHON_ORDER,
                     rows.manifest_order,
                     rows.row_order
            """
        )
        for row in cursor:
            yield (
                int(row[0]),
                str(row[1]),
                str(row[2]),
                int(row[3]),
                int(row[4]),
                str(row[5]),
                str(row[6]),
            )

    def add_artifacts(
        self,
        artifacts: Sequence[tuple[str, str, str]],
    ) -> None:
        self.connection.executemany(
            """
            INSERT INTO artifacts(relative_path, stem_key, kind)
            VALUES (?, ?, ?)
            """,
            artifacts,
        )
        self._observe_mutations(len(artifacts))

    def orphan_parquet_path(self) -> str | None:
        row = self.connection.execute(
            """
            SELECT data.relative_path
            FROM artifacts AS data
            LEFT JOIN artifacts AS manifest
              ON manifest.stem_key = data.stem_key
             AND manifest.kind = 'manifest'
            WHERE data.kind = 'parquet' AND manifest.relative_path IS NULL
            ORDER BY data.relative_path
            LIMIT 1
            """
        ).fetchone()
        return None if row is None else str(row[0])

    def iter_manifest_relative_paths(self) -> Iterator[str]:
        cursor = self.connection.execute(
            """
            SELECT relative_path FROM artifacts
            WHERE kind = 'manifest' ORDER BY relative_path
            """
        )
        for (relative_path,) in cursor:
            yield str(relative_path)

    def manifest_artifact_count(self) -> int:
        row = self.connection.execute("SELECT COUNT(*) FROM artifacts WHERE kind = 'manifest'").fetchone()
        assert row is not None
        return int(row[0])

    def next_spill_ordinal(self) -> int:
        value = self._next_spill_ordinal
        self._next_spill_ordinal += 1
        return value

    def observe_scratch_size(self) -> int:
        total = 0
        for path in self.directory.iterdir():
            try:
                total += path.stat().st_size
            except FileNotFoundError:
                continue
        self.metrics.peak_scratch_bytes = max(
            self.metrics.peak_scratch_bytes,
            total,
        )
        return total

    def close(self) -> None:
        if self._closed:
            return
        self.observe_scratch_size()
        self.connection.close()
        with suppress(FileNotFoundError):
            shutil.rmtree(self.directory)
        self._closed = True

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            return


class SpilledTimestampSeries(Iterable[datetime]):
    def __init__(self, store: BoundedRowStore, series_id: int) -> None:
        self.store = store
        self.series_id = series_id
        self._count = 0

    def append(self, value: datetime) -> None:
        ordinal = self.store.next_spill_ordinal()
        self.store.connection.execute(
            "INSERT INTO timestamps(series_id, timestamp_ns, ordinal) VALUES (?, ?, ?)",
            (self.series_id, _timestamp_ns(value), ordinal),
        )
        self._count += 1
        self.store.metrics.spilled_timestamp_rows += 1
        self.store._observe_mutations(1)

    def __iter__(self) -> Iterator[datetime]:
        cursor = self.store.connection.execute(
            """
            SELECT timestamp_ns FROM timestamps
            WHERE series_id = ? ORDER BY timestamp_ns, ordinal
            """,
            (self.series_id,),
        )
        for (timestamp_ns,) in cursor:
            yield _from_timestamp_ns(int(timestamp_ns))

    def __bool__(self) -> bool:
        return self._count > 0

    def __len__(self) -> int:
        return self._count


class SpilledSequence(Iterable[_SpillT], Generic[_SpillT]):
    def __init__(self, store: BoundedRowStore, sequence_id: int) -> None:
        self.store = store
        self.sequence_id = sequence_id
        self._count = 0

    def append(self, value: _SpillT) -> None:
        ordinal = self.store.next_spill_ordinal()
        self.store.connection.execute(
            """
            INSERT INTO spill_sequences(sequence_id, ordinal, payload)
            VALUES (?, ?, ?)
            """,
            (self.sequence_id, ordinal, sqlite3.Binary(pickle.dumps(value, protocol=5))),
        )
        self._count += 1
        self.store.metrics.spilled_sequence_rows += 1
        self.store._observe_mutations(1)

    def __iter__(self) -> Iterator[_SpillT]:
        cursor = self.store.connection.execute(
            """
            SELECT payload FROM spill_sequences
            WHERE sequence_id = ? ORDER BY ordinal
            """,
            (self.sequence_id,),
        )
        for (payload,) in cursor:
            value = pickle.loads(bytes(payload))
            if not isinstance(value, tuple):
                raise ValueError("invalid continuity spilled sequence payload")
            yield cast(_SpillT, value)

    def __bool__(self) -> bool:
        return self._count > 0

    def __len__(self) -> int:
        return self._count


class SpilledSet:
    def __init__(self, store: BoundedRowStore, set_id: int) -> None:
        self.store = store
        self.set_id = set_id
        self._count = 0

    def add(self, value: tuple[object, ...]) -> None:
        payload = pickle.dumps(value, protocol=5)
        cursor = self.store.connection.execute(
            "INSERT OR IGNORE INTO spill_sets(set_id, payload) VALUES (?, ?)",
            (self.set_id, sqlite3.Binary(payload)),
        )
        if cursor.rowcount:
            self._count += 1
            self.store.metrics.spilled_set_keys += 1
            self.store._observe_mutations(1)

    def __contains__(self, value: object) -> bool:
        if not isinstance(value, tuple):
            return False
        payload = pickle.dumps(value, protocol=5)
        row = self.store.connection.execute(
            "SELECT 1 FROM spill_sets WHERE set_id = ? AND payload = ?",
            (self.set_id, sqlite3.Binary(payload)),
        ).fetchone()
        return row is not None

    def __len__(self) -> int:
        return self._count


def configured_scratch_parent() -> Path | None:
    raw = os.environ.get("HYPERLAB_CONTINUITY_SCRATCH")
    if raw is None or not raw.strip():
        return None
    path = Path(raw).expanduser().resolve()
    if not path.is_dir():
        raise ValueError("HYPERLAB_CONTINUITY_SCRATCH must name an existing directory")
    return path


__all__ = [
    "BoundedRowStore",
    "SpilledSequence",
    "SpilledSet",
    "SpilledTimestampSeries",
    "StreamingMetrics",
    "configured_scratch_parent",
]
