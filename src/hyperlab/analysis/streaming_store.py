from __future__ import annotations

import hashlib
import json
import math
import os
import pickle
import sqlite3
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Literal, TypeAlias, cast

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SqlValue: TypeAlias = str | int | float | bytes | None
EventFieldCategory: TypeAlias = Literal[
    "bool", "int", "float", "timestamp", "str"
]

_SQLITE_CACHE_KIB = 32 * 1024
_SQLITE_COMMIT_ROWS = 2_048
_NULL_SORT_TEXT = "\U0010ffff"
_NULL_SORT_INTEGER = -(2**63)

_EVENT_NUMERIC_COLUMNS = frozenset(
    {
        "response_bps",
        "negative_lag_response_bps",
        "first_move_delay_ms",
        "net_execution_bps",
        "gross_execution_bps",
        "break_even_move_bps",
        "fill_adjusted_net_bps",
        "fill_adjusted_gross_bps",
        "before_funding_execution_bps",
        "entry_fee_bps_applied",
        "exit_fee_bps_applied",
        "entry_spread_cost_bps",
        "exit_spread_cost_bps",
        "entry_slippage_cost_bps",
        "exit_slippage_cost_bps",
        "adverse_exit_cost_bps",
        "unclosed_exposure_fraction",
        "matched_fill_fraction",
    }
)

_EVENT_FILTER_COLUMNS = frozenset(
    {
        "row_kind",
        "signal_role",
        "asset",
        "signal_family",
        "horizon_ms",
        "time_bucket",
        "time_bucket_ns",
        "execution_scenario",
        "execution_model",
        "execution_status",
        "evaluable",
        "randomization_block",
    }
)

_EVENT_SELECT_COLUMNS = frozenset(
    {
        "event_id",
        "signal_time_ns",
        "row_kind",
        "signal_role",
        "asset",
        "signal_family",
        "horizon_ms",
        "time_bucket",
        "time_bucket_ns",
        "execution_scenario",
        "execution_model",
        "execution_status",
        "evaluable",
        "classification",
        "first_move_direction",
        "randomization_block",
        *_EVENT_NUMERIC_COLUMNS,
    }
)

# The production event artifact is a versioned table, not an Arrow inference
# result.  Keep this order identical to ``LeadLagAnalysis.events`` from the
# pandas oracle.  Information/control rows legitimately omit the execution-only
# suffix; the fixed schema supplies typed nulls for those cells.
_EVENT_FIELD_DEFINITIONS: tuple[tuple[str, EventFieldCategory], ...] = (
    ("signal_id", "str"),
    ("signal_venue", "str"),
    ("asset", "str"),
    ("signal_family", "str"),
    ("signal_time", "timestamp"),
    ("signal_value", "float"),
    ("signal_strength", "float"),
    ("signal_direction", "int"),
    ("signal_role", "str"),
    ("time_axis", "str"),
    ("source_time_status", "str"),
    ("horizon_ms", "int"),
    ("target_time", "timestamp"),
    ("time_bucket", "timestamp"),
    ("interval_tag", "str"),
    ("interval_id", "str"),
    ("interval_start", "timestamp"),
    ("interval_end", "timestamp"),
    ("evaluable", "bool"),
    ("exclusion_reason", "str"),
    ("baseline_time", "timestamp"),
    ("response_state_time", "timestamp"),
    ("baseline_mid", "float"),
    ("response_mid", "float"),
    ("response_bps", "float"),
    ("negative_lag_response_bps", "float"),
    ("first_move_delay_ms", "float"),
    ("first_move_direction", "str"),
    ("classification", "str"),
    ("baseline_bid", "float"),
    ("baseline_ask", "float"),
    ("baseline_bid_quantity", "float"),
    ("baseline_ask_quantity", "float"),
    ("response_bid", "float"),
    ("response_ask", "float"),
    ("randomization_block", "str"),
    ("row_kind", "str"),
    ("execution_scenario", "str"),
    ("execution_model", "str"),
    ("execution_calibration_status", "str"),
    ("execution_status", "str"),
    ("execution_source", "str"),
    ("economic_scope", "str"),
    ("funding_status", "str"),
    ("economic_admissibility", "str"),
    ("net_execution_scope", "str"),
    ("latency_ms_assumption", "float"),
    ("exit_latency_ms_assumption", "float"),
    ("maker_timeout_ms_assumption", "float"),
    ("maker_fee_bps_assumption", "float"),
    ("taker_fee_bps_assumption", "float"),
    ("slippage_bps_assumption", "float"),
    ("adverse_exit_bps_assumption", "float"),
    ("queue_ahead_multiplier_assumption", "float"),
    ("max_participation_assumption", "float"),
    ("spread_source", "str"),
    ("entry_time", "timestamp"),
    ("exit_time", "timestamp"),
    ("entry_price", "float"),
    ("exit_price", "float"),
    ("requested_notional_usd", "float"),
    ("entry_fill_fraction", "float"),
    ("exit_fill_fraction", "float"),
    ("matched_fill_fraction", "float"),
    ("unclosed_exposure_fraction", "float"),
    ("fill_fraction", "float"),
    ("gross_execution_bps", "float"),
    ("net_execution_bps", "float"),
    ("before_funding_execution_bps", "float"),
    ("before_cost_mid_move_bps", "float"),
    ("entry_fee_bps_applied", "float"),
    ("exit_fee_bps_applied", "float"),
    ("entry_spread_cost_bps", "float"),
    ("exit_spread_cost_bps", "float"),
    ("entry_slippage_cost_bps", "float"),
    ("exit_slippage_cost_bps", "float"),
    ("adverse_exit_cost_bps", "float"),
    ("fill_adjusted_gross_bps", "float"),
    ("fill_adjusted_net_bps", "float"),
    ("break_even_move_bps", "float"),
)

PHASE10_EVENT_COLUMNS = tuple(name for name, _category in _EVENT_FIELD_DEFINITIONS)
_EVENT_FIELD_NAME_SET = frozenset(PHASE10_EVENT_COLUMNS)
_EVENT_FIELD_CATEGORIES = dict(_EVENT_FIELD_DEFINITIONS)

PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS = (
    "artifact_schema_version",
    "streaming_resource_model_version",
    "research_status",
    "source_time_lead_status",
    "config_sha256",
    "gate_report_sha256",
    "semantic_gate_sha256",
    "semantic_gate_canonicalizer_version",
    "semantic_gate_excluded_json_pointers",
    "manifest_fingerprint",
    "selected_manifests_sha256",
    "selected_manifest_count",
)


def _fixed_arrow_type(category: EventFieldCategory) -> pa.DataType:
    if category == "bool":
        return pa.bool_()
    if category == "int":
        return pa.int64()
    if category == "float":
        return pa.float64()
    if category == "timestamp":
        return pa.timestamp("ns", tz="UTC")
    return pa.string()


PHASE10_EVENT_PARQUET_SCHEMA = pa.schema(
    [
        *(
            pa.field(name, _fixed_arrow_type(category), nullable=True)
            for name, category in _EVENT_FIELD_DEFINITIONS
        ),
        *(
            pa.field(name, pa.string(), nullable=False)
            for name in PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS
        ),
    ]
)


class StreamingStoreError(ValueError):
    """Raised when a bounded scratch spool cannot preserve the data contract."""


@dataclass(frozen=True, slots=True)
class ExactTimestampNs:
    """Trusted internal UTC epoch-nanosecond timestamp.

    This deliberately narrow type distinguishes causal-kernel timestamps from
    arbitrary integral or string values at the fixed-schema publication seam.
    """

    value: int

    def __post_init__(self) -> None:
        if isinstance(self.value, bool) or not isinstance(self.value, int):
            raise StreamingStoreError("ExactTimestampNs.value must be an integer")
        if not -(2**63) <= self.value < 2**63:
            raise StreamingStoreError("ExactTimestampNs.value is outside int64")


def timestamp_ns(value: object, *, label: str) -> int:
    """Convert one timezone-aware timestamp-like value to an exact epoch nanosecond."""

    if isinstance(value, ExactTimestampNs):
        return value.value
    if not isinstance(value, (str, int, float, datetime, np.datetime64, pd.Timestamp)):
        raise StreamingStoreError(f"{label} must be timestamp-like")
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        raise StreamingStoreError(f"{label} must be timestamp-like") from None
    if timestamp.tzinfo is None:
        raise StreamingStoreError(f"{label} must be timezone-aware")
    return int(timestamp.tz_convert("UTC").value)


def datetime_from_ns(value: int) -> datetime:
    """Return a UTC Python datetime while retaining nanoseconds in pandas callers."""

    return pd.Timestamp(value, tz="UTC").to_pydatetime()


def _is_missing(value: object) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, (float, np.floating)):
        return not math.isfinite(float(value))
    if isinstance(value, np.datetime64):
        return bool(np.isnat(value))
    return False


def _optional_float(value: object) -> float | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        raise StreamingStoreError("boolean value cannot be stored as an event float")
    try:
        result = float(str(value))
    except (TypeError, ValueError):
        raise StreamingStoreError(f"event value {value!r} is not numeric") from None
    return result if math.isfinite(result) else None


def _optional_integer(value: object) -> int | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        raise StreamingStoreError("boolean value cannot be stored as an event integer")
    try:
        return int(str(value))
    except (TypeError, ValueError):
        raise StreamingStoreError(f"event value {value!r} is not integer-like") from None


def _optional_text(value: object) -> str | None:
    return None if _is_missing(value) else str(value)


def _canonical_json_text(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _json_compatible(value: object) -> object:
    if _is_missing(value):
        return None
    if isinstance(value, ExactTimestampNs):
        return (
            pd.Timestamp(value.value, tz="UTC").isoformat().replace("+00:00", "Z")
        )
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, (float, np.floating)):
        result = float(value)
        return result if math.isfinite(result) else None
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, (pd.Timestamp, datetime, np.datetime64)):
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        return timestamp.tz_convert("UTC").isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Enum):
        return _json_compatible(value.value)
    if isinstance(value, Mapping):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_compatible(item) for item in value]
    item = getattr(value, "item", None)
    if callable(item):
        return _json_compatible(item())
    raise StreamingStoreError(f"unsupported event value type: {type(value).__name__}")


class _SqliteSpool:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path)
        self._connection.execute("PRAGMA journal_mode=DELETE")
        self._connection.execute("PRAGMA synchronous=OFF")
        self._connection.execute("PRAGMA temp_store=FILE")
        self._connection.execute(f"PRAGMA cache_size=-{_SQLITE_CACHE_KIB}")
        self._connection.execute("PRAGMA mmap_size=0")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._closed = False
        self.rows_since_commit = 0
        self.commit_count = 0
        self.max_uncommitted_rows = 0

    @property
    def connection(self) -> sqlite3.Connection:
        if self._closed:
            raise StreamingStoreError("scratch spool is closed")
        return self._connection

    def observe_mutations(self, rows: int) -> None:
        self.rows_since_commit += rows
        self.max_uncommitted_rows = max(self.max_uncommitted_rows, self.rows_since_commit)
        if self.rows_since_commit >= _SQLITE_COMMIT_ROWS:
            self.commit()

    def commit(self) -> None:
        if self._closed or self.rows_since_commit == 0:
            return
        self._connection.commit()
        self.rows_since_commit = 0
        self.commit_count += 1

    def scratch_bytes(self) -> int:
        total = 0
        for path in self.path.parent.rglob("*"):
            if path.is_file():
                try:
                    total += path.stat().st_size
                except FileNotFoundError:
                    continue
        return total

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.commit()
        finally:
            self._connection.close()
            self._closed = True

    def __enter__(self) -> _SqliteSpool:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()


class SourceRowSpool(_SqliteSpool):
    """Disk-backed deterministic ordering for projected Phase 10 source rows."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.connection.executescript(
            """
            CREATE TABLE source_rows (
                row_id INTEGER PRIMARY KEY,
                kind TEXT NOT NULL,
                venue TEXT NOT NULL,
                asset TEXT NOT NULL,
                received_ns INTEGER NOT NULL,
                connection_id TEXT NOT NULL,
                arrival_sequence INTEGER NOT NULL,
                source_sequence INTEGER NOT NULL,
                logical_identity TEXT NOT NULL,
                manifest_order INTEGER NOT NULL,
                row_order INTEGER NOT NULL,
                payload BLOB NOT NULL
            );
            CREATE UNIQUE INDEX source_physical_order
            ON source_rows(kind, manifest_order, row_order);
            CREATE INDEX source_asset_time
            ON source_rows(asset, received_ns, kind, venue, connection_id,
                           arrival_sequence, source_sequence, logical_identity,
                           manifest_order, row_order);
            CREATE INDEX source_kind_asset_time
            ON source_rows(kind, asset, received_ns, venue, connection_id,
                           arrival_sequence, source_sequence, logical_identity,
                           manifest_order, row_order);
            """
        )
        self.total_rows = 0

    @staticmethod
    def _arrival_sequence(row: Mapping[str, object]) -> int:
        explicit = row.get("arrival_sequence")
        if explicit is not None:
            return int(str(explicit))
        snapshot_id = row.get("snapshot_id")
        book_epoch_id = row.get("book_epoch_id")
        if isinstance(snapshot_id, str) and isinstance(book_epoch_id, str):
            prefix = f"ws:{book_epoch_id}:"
            if snapshot_id.startswith(prefix):
                candidate = snapshot_id[len(prefix) :].partition(":")[0]
                try:
                    return int(candidate)
                except ValueError:
                    return -1
        update_id = row.get("update_id")
        if row.get("source_sequence") is None and isinstance(update_id, str):
            try:
                return int(update_id.rpartition(":")[2])
            except ValueError:
                return -1
        return -1

    @staticmethod
    def _identity(row: Mapping[str, object]) -> str:
        for name in ("snapshot_id", "update_id", "trade_id", "observation_id"):
            value = row.get(name)
            if value is not None:
                return str(value)
        return ""

    def add_rows(
        self,
        *,
        kind: str,
        rows: Sequence[Mapping[str, object]],
        manifest_order: int,
        first_row_order: int,
        row_orders: Sequence[int] | None = None,
    ) -> None:
        if not rows:
            return
        if row_orders is not None and len(row_orders) != len(rows):
            raise StreamingStoreError("source row_orders length does not match rows")
        values: list[tuple[SqlValue, ...]] = []
        for offset, row in enumerate(rows):
            received = timestamp_ns(row.get("received_time"), label="source received_time")
            source_sequence = row.get("source_sequence")
            values.append(
                (
                    kind,
                    str(row.get("venue") or ""),
                    str(row.get("asset") or ""),
                    received,
                    str(row.get("connection_id") or ""),
                    self._arrival_sequence(row),
                    _NULL_SORT_INTEGER
                    if source_sequence is None
                    else int(str(source_sequence)),
                    self._identity(row),
                    manifest_order,
                    (
                        first_row_order + offset
                        if row_orders is None
                        else int(row_orders[offset])
                    ),
                    pickle.dumps(dict(row), protocol=5),
                )
            )
        self.connection.executemany(
            """
            INSERT INTO source_rows(
                kind, venue, asset, received_ns, connection_id,
                arrival_sequence, source_sequence, logical_identity,
                manifest_order, row_order, payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
        self.total_rows += len(values)
        self.observe_mutations(len(values))

    def count_rows(self, *, asset: str, start_ns: int, end_ns: int) -> int:
        row = self.connection.execute(
            """
            SELECT COUNT(*) FROM source_rows
            WHERE asset = ? AND received_ns >= ? AND received_ns < ?
            """,
            (asset, start_ns, end_ns),
        ).fetchone()
        return 0 if row is None else int(row[0])

    def iter_receive_batches(
        self, *, asset: str, start_ns: int, end_ns: int
    ) -> Iterator[tuple[int, int]]:
        cursor = self.connection.execute(
            """
            SELECT received_ns, COUNT(*)
            FROM source_rows
            WHERE asset = ? AND received_ns >= ? AND received_ns < ?
            GROUP BY received_ns
            ORDER BY received_ns
            """,
            (asset, start_ns, end_ns),
        )
        for received_ns, row_count in cursor:
            yield int(received_ns), int(row_count)

    def iter_ordered_batches(
        self,
        *,
        asset: str,
        start_ns: int,
        end_ns: int,
        fetch_rows: int = 1_024,
    ) -> Iterator[tuple[int, tuple[tuple[str, Mapping[str, object]], ...]]]:
        """Yield complete receive-time batches from one deterministic SQL cursor."""

        if isinstance(fetch_rows, bool) or fetch_rows <= 0:
            raise StreamingStoreError("source fetch_rows must be positive")
        cursor = self.connection.execute(
            """
            SELECT received_ns, kind, payload FROM source_rows
            WHERE asset = ? AND received_ns >= ? AND received_ns < ?
            ORDER BY received_ns, kind, venue, asset, connection_id,
                     arrival_sequence, source_sequence, logical_identity,
                     manifest_order, row_order
            """,
            (asset, start_ns, end_ns),
        )
        batch_time: int | None = None
        rows: list[tuple[str, Mapping[str, object]]] = []
        while fetched := cursor.fetchmany(fetch_rows):
            for raw_received_ns, raw_kind, payload in fetched:
                received_ns = int(raw_received_ns)
                if batch_time is not None and received_ns != batch_time:
                    yield batch_time, tuple(rows)
                    rows.clear()
                value = pickle.loads(payload)
                if not isinstance(value, dict):
                    raise StreamingStoreError("source scratch payload is not a row object")
                batch_time = received_ns
                rows.append(
                    (
                        str(raw_kind),
                        {str(key): item for key, item in value.items()},
                    )
                )
        if batch_time is not None:
            yield batch_time, tuple(rows)

    def midpoint_batch_timestamp(
        self, *, asset: str, start_ns: int, end_ns: int
    ) -> int | None:
        count_row = self.connection.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT DISTINCT received_ns
                FROM source_rows
                WHERE asset = ? AND received_ns >= ? AND received_ns < ?
            )
            """,
            (asset, start_ns, end_ns),
        ).fetchone()
        count = 0 if count_row is None else int(count_row[0])
        if count <= 1:
            return None
        row = self.connection.execute(
            """
            SELECT DISTINCT received_ns
            FROM source_rows
            WHERE asset = ? AND received_ns >= ? AND received_ns < ?
            ORDER BY received_ns
            LIMIT 1 OFFSET ?
            """,
            (asset, start_ns, end_ns, count // 2),
        ).fetchone()
        return None if row is None else int(row[0])

    def iter_rows(
        self,
        *,
        kind: str,
        asset: str,
        start_ns: int,
        end_ns: int,
        fetch_rows: int = 1_024,
    ) -> Iterator[dict[str, object]]:
        cursor = self.connection.execute(
            """
            SELECT payload FROM source_rows
            WHERE kind = ? AND asset = ?
              AND received_ns >= ? AND received_ns < ?
            ORDER BY received_ns, venue, asset, connection_id,
                     arrival_sequence, source_sequence, logical_identity,
                     manifest_order, row_order
            """,
            (kind, asset, start_ns, end_ns),
        )
        while batch := cursor.fetchmany(fetch_rows):
            for (payload,) in batch:
                value = pickle.loads(payload)
                if not isinstance(value, dict):
                    raise StreamingStoreError("source scratch payload is not a row object")
                yield {str(key): item for key, item in value.items()}

    def dataframe(
        self,
        *,
        kind: str,
        asset: str,
        start_ns: int,
        end_ns: int,
        columns: Sequence[str],
        maximum_rows: int,
    ) -> pd.DataFrame:
        records: list[dict[str, object]] = []
        for row in self.iter_rows(kind=kind, asset=asset, start_ns=start_ns, end_ns=end_ns):
            records.append(row)
            if len(records) > maximum_rows:
                raise StreamingStoreError(
                    "projected source chunk exceeded max_source_rows_per_chunk"
                )
        extra = sorted({key for row in records for key in row}.difference(columns))
        return pd.DataFrame.from_records(records, columns=[*columns, *extra])


class EventSpool(_SqliteSpool):
    """Disk-backed canonical event spool plus narrow exact-statistic samples."""

    def __init__(self, path: Path, *, quantile_run_rows: int = 250_000) -> None:
        if (
            isinstance(quantile_run_rows, bool)
            or not isinstance(quantile_run_rows, int)
            or quantile_run_rows <= 0
        ):
            raise StreamingStoreError("quantile_run_rows must be a positive integer")
        super().__init__(path)
        self.quantile_run_rows = quantile_run_rows
        self.max_quantile_buffer_rows = 0
        self.connection.executescript(
            """
            CREATE TABLE event_rows (
                event_id INTEGER PRIMARY KEY,
                signal_time_ns INTEGER NOT NULL,
                asset TEXT NOT NULL,
                signal_family TEXT NOT NULL,
                horizon_ms INTEGER NOT NULL,
                signal_id TEXT NOT NULL,
                row_kind TEXT NOT NULL,
                scenario_sort TEXT NOT NULL,
                model_sort TEXT NOT NULL,
                time_bucket_ns INTEGER,
                signal_role TEXT,
                execution_scenario TEXT,
                execution_model TEXT,
                execution_status TEXT,
                evaluable INTEGER,
                classification TEXT,
                first_move_direction TEXT,
                randomization_block TEXT,
                response_bps REAL,
                negative_lag_response_bps REAL,
                first_move_delay_ms REAL,
                net_execution_bps REAL,
                gross_execution_bps REAL,
                break_even_move_bps REAL,
                fill_adjusted_net_bps REAL,
                fill_adjusted_gross_bps REAL,
                before_funding_execution_bps REAL,
                entry_fee_bps_applied REAL,
                exit_fee_bps_applied REAL,
                entry_spread_cost_bps REAL,
                exit_spread_cost_bps REAL,
                entry_slippage_cost_bps REAL,
                exit_slippage_cost_bps REAL,
                adverse_exit_cost_bps REAL,
                unclosed_exposure_fraction REAL,
                matched_fill_fraction REAL,
                payload BLOB NOT NULL
            );
            CREATE INDEX event_canonical_order
            ON event_rows(signal_time_ns, asset, signal_family, horizon_ms,
                          signal_id, row_kind, scenario_sort, model_sort, event_id);
            CREATE UNIQUE INDEX event_logical_identity
            ON event_rows(signal_time_ns, asset, signal_family, horizon_ms,
                          signal_id, row_kind, scenario_sort, model_sort);
            CREATE INDEX event_group
            ON event_rows(row_kind, signal_role, asset, signal_family, horizon_ms,
                          execution_scenario, execution_model, time_bucket_ns, event_id);
            CREATE INDEX event_blocks
            ON event_rows(row_kind, signal_role, evaluable, randomization_block,
                          asset, signal_family, horizon_ms, event_id);
            CREATE TABLE quantile_values (
                metric TEXT NOT NULL,
                time_bucket_ns INTEGER,
                execution_scenario TEXT,
                execution_model TEXT,
                asset TEXT NOT NULL,
                signal_family TEXT NOT NULL,
                horizon_ms INTEGER NOT NULL,
                value REAL NOT NULL,
                event_id INTEGER NOT NULL
            );
            CREATE INDEX quantile_aggregate_order
            ON quantile_values(metric, execution_scenario, execution_model,
                               asset, signal_family, horizon_ms, value, event_id);
            CREATE INDEX quantile_bucket_order
            ON quantile_values(metric, time_bucket_ns, execution_scenario,
                               execution_model, asset, signal_family, horizon_ms,
                               value, event_id);
            """
        )
        self.total_rows = 0
        self.rows_by_kind: dict[str, int] = {}

    @staticmethod
    def _required_event_value(row: Mapping[str, object], name: str) -> object:
        value = row.get(name)
        if _is_missing(value):
            raise StreamingStoreError(f"streaming event row requires {name}")
        return value

    def add_frame(self, frame: pd.DataFrame) -> None:
        columns = [str(value) for value in frame.columns]
        if len(set(columns)) != len(columns):
            raise StreamingStoreError("streaming event frame has duplicate columns")
        unknown = sorted(set(columns).difference(_EVENT_FIELD_NAME_SET))
        if unknown:
            raise StreamingStoreError(
                "streaming event frame has unknown columns: " + ", ".join(unknown)
            )
        self.add_rows(
            dict(zip(columns, values, strict=True))
            for values in frame.itertuples(index=False, name=None)
        )

    def add_rows(self, rows: Iterable[Mapping[str, object]]) -> None:
        """Spool a mapping stream using fixed-schema, bounded internal batches."""

        pending: list[tuple[SqlValue, ...]] = []
        quantiles: list[tuple[SqlValue, ...]] = []
        for raw_row in rows:
            if not isinstance(raw_row, Mapping):
                raise StreamingStoreError("streaming event row must be a mapping")
            row: dict[str, object] = {}
            for raw_name, value in raw_row.items():
                name = str(raw_name)
                if name in row:
                    raise StreamingStoreError(
                        f"streaming event row has duplicate normalized column: {name}"
                    )
                row[name] = value
            unknown = sorted(set(row).difference(_EVENT_FIELD_NAME_SET))
            if unknown:
                raise StreamingStoreError(
                    "streaming event row has unknown columns: " + ", ".join(unknown)
                )
            for name in row:
                self._arrow_value(row.get(name), _EVENT_FIELD_CATEGORIES[name])
            signal_time_ns = timestamp_ns(
                self._required_event_value(row, "signal_time"), label="event signal_time"
            )
            bucket_value = row.get("time_bucket")
            bucket_ns = None if _is_missing(bucket_value) else timestamp_ns(
                bucket_value, label="event time_bucket"
            )
            event_id = self.total_rows + len(pending) + 1
            scenario = _optional_text(row.get("execution_scenario"))
            model = _optional_text(row.get("execution_model"))
            event_values: list[SqlValue] = [
                event_id,
                signal_time_ns,
                str(self._required_event_value(row, "asset")),
                str(self._required_event_value(row, "signal_family")),
                int(str(self._required_event_value(row, "horizon_ms"))),
                str(self._required_event_value(row, "signal_id")),
                str(self._required_event_value(row, "row_kind")),
                _NULL_SORT_TEXT if scenario is None else scenario,
                _NULL_SORT_TEXT if model is None else model,
                bucket_ns,
                _optional_text(row.get("signal_role")),
                scenario,
                model,
                _optional_text(row.get("execution_status")),
                None if _is_missing(row.get("evaluable")) else int(bool(row.get("evaluable"))),
                _optional_text(row.get("classification")),
                _optional_text(row.get("first_move_direction")),
                _optional_text(row.get("randomization_block")),
            ]
            event_values.extend(
                _optional_float(row.get(name)) for name in sorted(_EVENT_NUMERIC_COLUMNS)
            )
            event_values.append(pickle.dumps(row, protocol=5))
            pending.append(tuple(event_values))

            row_kind = str(row.get("row_kind"))
            signal_role = _optional_text(row.get("signal_role"))
            metric_values: list[tuple[str, float]] = []
            response = _optional_float(row.get("response_bps"))
            if row_kind == "information" and signal_role == "primary" and bool(row.get("evaluable")):
                if response is not None:
                    metric_values.append(("information_response", response))
                first_delay = _optional_float(row.get("first_move_delay_ms"))
                if first_delay is not None and str(row.get("first_move_direction")) in {"same", "opposite"}:
                    metric_values.append(("information_first_move_delay", first_delay))
            if row_kind == "execution" and str(row.get("execution_status")) in {"FILLED", "PARTIAL"}:
                net = _optional_float(row.get("net_execution_bps"))
                if net is not None:
                    metric_values.append(("execution_net", net))
            for metric, value in metric_values:
                quantiles.append(
                    (
                        metric,
                        bucket_ns,
                        scenario,
                        model,
                        str(row["asset"]),
                        str(row["signal_family"]),
                        int(str(row["horizon_ms"])),
                        value,
                        event_id,
                    )
                )
            self.max_quantile_buffer_rows = max(
                self.max_quantile_buffer_rows, len(quantiles)
            )

            if len(pending) >= min(_SQLITE_COMMIT_ROWS, self.quantile_run_rows):
                self._insert_event_rows(pending, quantiles)
                pending.clear()
                quantiles.clear()
        if pending:
            self._insert_event_rows(pending, quantiles)

    def _insert_event_rows(
        self,
        rows: Sequence[tuple[SqlValue, ...]],
        quantiles: Sequence[tuple[SqlValue, ...]],
    ) -> None:
        numeric_names = sorted(_EVENT_NUMERIC_COLUMNS)
        placeholders = ", ".join("?" for _ in range(19 + len(numeric_names)))
        columns = ", ".join(
            (
                "event_id, signal_time_ns, asset, signal_family, horizon_ms, signal_id, "
                "row_kind, scenario_sort, model_sort, time_bucket_ns, signal_role, "
                "execution_scenario, execution_model, execution_status, evaluable, "
                "classification, first_move_direction, randomization_block",
                ", ".join(numeric_names),
                "payload",
            )
        )
        try:
            self.connection.executemany(
                f"INSERT INTO event_rows({columns}) VALUES ({placeholders})",
                rows,
            )
            if quantiles:
                self.connection.executemany(
                    """
                    INSERT INTO quantile_values(
                        metric, time_bucket_ns, execution_scenario, execution_model,
                        asset, signal_family, horizon_ms, value, event_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    quantiles,
                )
        except sqlite3.IntegrityError as exc:
            raise StreamingStoreError(
                f"duplicate or conflicting logical event row: {exc}"
            ) from None
        for row in rows:
            kind = str(row[6])
            self.rows_by_kind[kind] = self.rows_by_kind.get(kind, 0) + 1
        self.total_rows += len(rows)
        self.observe_mutations(len(rows) + len(quantiles))

    @staticmethod
    def _normalized_filters(filters: object) -> dict[str, object]:
        if isinstance(filters, Mapping):
            return {str(name): value for name, value in filters.items()}
        equals = getattr(filters, "equals", None)
        one_of = getattr(filters, "one_of", None)
        if not isinstance(equals, Sequence) or not isinstance(one_of, Sequence):
            raise StreamingStoreError("event filters must be a mapping or EventFilter")
        normalized: dict[str, object] = {}
        for name, value in equals:
            normalized[str(name)] = value
        for name, values in one_of:
            key = str(name)
            if key in normalized:
                raise StreamingStoreError(f"duplicate event filter column: {key}")
            normalized[key] = tuple(values)
        return normalized

    @staticmethod
    def _sql_filter_value(name: str, value: object) -> SqlValue:
        if name in {"time_bucket", "time_bucket_ns"} and value is not None:
            if name == "time_bucket":
                return timestamp_ns(value, label="event time_bucket filter")
            return int(str(value))
        if isinstance(value, (np.bool_, bool)):
            return int(bool(value))
        return cast(SqlValue, value)

    @classmethod
    def _where(
        cls, filters: object, *, table_alias: str = ""
    ) -> tuple[str, list[SqlValue]]:
        prefix = "" if not table_alias else f"{table_alias}."
        clauses: list[str] = []
        parameters: list[SqlValue] = []
        normalized = cls._normalized_filters(filters)
        for name in sorted(normalized):
            if name not in _EVENT_FILTER_COLUMNS:
                raise StreamingStoreError(f"unsupported event filter column: {name}")
            value = normalized[name]
            sql_name = "time_bucket_ns" if name == "time_bucket" else name
            if value is None:
                clauses.append(f"{prefix}{sql_name} IS NULL")
            elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                sequence = list(value)
                if not sequence:
                    clauses.append("0")
                else:
                    clauses.append(
                        f"{prefix}{sql_name} IN ({', '.join('?' for _ in sequence)})"
                    )
                    parameters.extend(cls._sql_filter_value(name, item) for item in sequence)
            else:
                clauses.append(f"{prefix}{sql_name} = ?")
                parameters.append(cls._sql_filter_value(name, value))
        return (" AND ".join(clauses) if clauses else "1"), parameters

    def iter_rows(
        self,
        *,
        filters: object,
        columns: Sequence[str],
        order_by: Sequence[str] = ("event_id",),
        fetch_rows: int = 1_024,
    ) -> Iterator[tuple[object, ...]]:
        if not columns or any(name not in _EVENT_SELECT_COLUMNS for name in columns):
            raise StreamingStoreError("unsupported event select columns")
        if any(name not in _EVENT_SELECT_COLUMNS for name in order_by):
            raise StreamingStoreError("unsupported event order columns")
        where, parameters = self._where(filters)
        selected = ", ".join(
            "time_bucket_ns" if name == "time_bucket" else name for name in columns
        )
        ordering = ", ".join(
            "time_bucket_ns" if name == "time_bucket" else name for name in order_by
        )
        cursor = self.connection.execute(
            f"SELECT {selected} FROM event_rows WHERE {where} ORDER BY {ordering}",
            parameters,
        )
        while batch := cursor.fetchmany(fetch_rows):
            yield from batch

    def count(self, *, filters: object) -> int:
        where, parameters = self._where(filters)
        row = self.connection.execute(
            f"SELECT COUNT(*) FROM event_rows WHERE {where}",
            parameters,
        ).fetchone()
        return 0 if row is None else int(row[0])

    def distinct_randomization_blocks(self) -> tuple[str, ...]:
        rows = self.connection.execute(
            """
            SELECT DISTINCT randomization_block
            FROM event_rows
            WHERE row_kind = 'information' AND signal_role = 'primary'
              AND evaluable = 1 AND randomization_block IS NOT NULL
            ORDER BY randomization_block
            """
        )
        return tuple(str(row[0]) for row in rows)

    def exact_quantile(
        self,
        *,
        filters: object,
        quantile: float,
        column: str | None = None,
        metric: str | None = None,
    ) -> float:
        metric_by_column = {
            "response_bps": "information_response",
            "first_move_delay_ms": "information_first_move_delay",
            "net_execution_bps": "execution_net",
        }
        resolved_metric = metric
        if column is not None:
            try:
                resolved_metric = metric_by_column[column]
            except KeyError:
                raise StreamingStoreError(
                    f"unsupported exact quantile column: {column}"
                ) from None
        if resolved_metric not in {
            "information_response",
            "information_first_move_delay",
            "execution_net",
        }:
            raise StreamingStoreError(
                f"unsupported exact quantile metric: {resolved_metric}"
            )
        if not 0.0 <= quantile <= 1.0:
            raise StreamingStoreError("quantile must be in [0, 1]")
        clauses = ["metric = ?"]
        parameters: list[SqlValue] = [resolved_metric]
        allowed = {
            "time_bucket",
            "time_bucket_ns",
            "execution_scenario",
            "execution_model",
            "asset",
            "signal_family",
            "horizon_ms",
        }
        normalized_filters = self._normalized_filters(filters)
        implicit: dict[str, object]
        if resolved_metric == "information_response":
            implicit = {"row_kind": "information", "evaluable": True}
        elif resolved_metric == "information_first_move_delay":
            implicit = {
                "row_kind": "information",
                "evaluable": True,
                "first_move_direction": ("same", "opposite"),
            }
        else:
            implicit = {
                "row_kind": "execution",
                "execution_status": ("FILLED", "PARTIAL"),
            }
        for name, expected in implicit.items():
            if name not in normalized_filters:
                continue
            actual = normalized_filters.pop(name)
            if isinstance(expected, tuple):
                actual_values = (
                    tuple(actual)
                    if isinstance(actual, Sequence)
                    and not isinstance(actual, (str, bytes, bytearray))
                    else (actual,)
                )
                if actual_values != expected:
                    raise StreamingStoreError(
                        f"exact quantile filter {name} conflicts with metric population"
                    )
            elif self._sql_filter_value(name, actual) != self._sql_filter_value(
                name, expected
            ):
                raise StreamingStoreError(
                    f"exact quantile filter {name} conflicts with metric population"
                )
        for name in sorted(normalized_filters):
            if name not in allowed:
                raise StreamingStoreError(f"unsupported quantile filter column: {name}")
            value = normalized_filters[name]
            sql_name = "time_bucket_ns" if name == "time_bucket" else name
            if isinstance(value, Sequence) and not isinstance(
                value, (str, bytes, bytearray)
            ):
                sequence = list(value)
                if not sequence:
                    clauses.append("0")
                else:
                    clauses.append(
                        f"{sql_name} IN ({', '.join('?' for _ in sequence)})"
                    )
                    parameters.extend(
                        self._sql_filter_value(name, item) for item in sequence
                    )
                continue
            if value is None:
                clauses.append(f"{sql_name} IS NULL")
            else:
                clauses.append(f"{sql_name} = ?")
                parameters.append(self._sql_filter_value(name, value))
        where = " AND ".join(clauses)
        count_row = self.connection.execute(
            f"SELECT COUNT(*) FROM quantile_values WHERE {where}",
            parameters,
        ).fetchone()
        count = 0 if count_row is None else int(count_row[0])
        if count == 0:
            return math.nan
        position = quantile * (count - 1)
        lower_position = math.floor(position)
        upper_position = math.ceil(position)

        def at(offset: int) -> float:
            row = self.connection.execute(
                f"""
                SELECT value FROM quantile_values
                WHERE {where}
                ORDER BY value, event_id
                LIMIT 1 OFFSET ?
                """,
                [*parameters, offset],
            ).fetchone()
            if row is None:
                raise StreamingStoreError("exact quantile order statistic disappeared")
            return float(row[0])

        lower = at(lower_position)
        upper = at(upper_position)
        fraction = position - lower_position
        return lower + (upper - lower) * fraction

    @property
    def event_columns(self) -> tuple[str, ...]:
        return PHASE10_EVENT_COLUMNS

    def _arrow_value(self, value: object, category: EventFieldCategory) -> object:
        if _is_missing(value):
            return None
        if category == "timestamp":
            if isinstance(value, ExactTimestampNs):
                return pd.Timestamp(value.value, tz="UTC")
            if not isinstance(
                value, (datetime, np.datetime64, pd.Timestamp)
            ):
                raise StreamingStoreError("fixed-schema event timestamp has a type conflict")
            timestamp = pd.Timestamp(value)
            if timestamp.tzinfo is None:
                raise StreamingStoreError(
                    "fixed-schema event timestamp must be timezone-aware"
                )
            # Arrow accepts pandas Timestamp directly and therefore retains the
            # nanosecond component that Python's datetime cannot represent.
            return timestamp.tz_convert("UTC")
        if category == "bool":
            if not isinstance(value, (bool, np.bool_)):
                raise StreamingStoreError("fixed-schema event bool has a type conflict")
            return bool(value)
        if category == "int":
            if isinstance(value, (bool, np.bool_)) or not isinstance(
                value, (int, np.integer)
            ):
                raise StreamingStoreError("fixed-schema event integer has a type conflict")
            return int(value)
        if category == "float":
            if isinstance(value, (bool, np.bool_)) or not isinstance(
                value, (int, float, np.integer, np.floating, Decimal)
            ):
                raise StreamingStoreError("fixed-schema event float has a type conflict")
            return float(value)
        if not isinstance(value, str):
            raise StreamingStoreError("fixed-schema event string has a type conflict")
        return value

    def write_parquet(
        self,
        path: Path,
        *,
        bindings: Mapping[str, str],
        row_group_rows: int,
        writer_buffer_rows: int | None = None,
        compression: str = "zstd",
    ) -> tuple[int, int, str]:
        if isinstance(row_group_rows, bool) or row_group_rows <= 0:
            raise StreamingStoreError("Parquet row_group_rows must be positive")
        buffer_limit = row_group_rows if writer_buffer_rows is None else writer_buffer_rows
        if isinstance(buffer_limit, bool) or buffer_limit <= 0:
            raise StreamingStoreError("Parquet writer_buffer_rows must be positive")
        if buffer_limit > row_group_rows:
            raise StreamingStoreError(
                "Parquet writer_buffer_rows cannot exceed row_group_rows"
            )
        expected_bindings = frozenset(PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS)
        actual_bindings = frozenset(str(name) for name in bindings)
        if actual_bindings != expected_bindings:
            missing = sorted(expected_bindings.difference(actual_bindings))
            unknown = sorted(actual_bindings.difference(expected_bindings))
            raise StreamingStoreError(
                "fixed-schema event bindings differ"
                f"; missing={missing}; unknown={unknown}"
            )
        normalized_bindings: dict[str, str] = {}
        for name in PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS:
            value = bindings[name]
            if not isinstance(value, str) or not value:
                raise StreamingStoreError(
                    f"fixed-schema event binding {name} must be a non-empty string"
                )
            normalized_bindings[name] = value
        columns = list(PHASE10_EVENT_PARQUET_SCHEMA.names)
        schema = PHASE10_EVENT_PARQUET_SCHEMA
        cursor = self.connection.execute(
            """
            SELECT payload FROM event_rows
            ORDER BY signal_time_ns, asset, signal_family, horizon_ms,
                     signal_id, row_kind, scenario_sort, model_sort, event_id
            """
        )
        writer = pq.ParquetWriter(
            path,
            schema,
            compression=compression,
            use_dictionary=True,
            write_statistics=True,
        )
        logical_hash = hashlib.sha256()
        written = 0
        buffer: list[dict[str, object]] = []
        batches: list[pa.RecordBatch] = []
        batched_rows = 0

        def write_complete_groups() -> None:
            nonlocal batches, batched_rows, written
            while batched_rows >= row_group_rows:
                table = pa.Table.from_batches(batches, schema=schema)
                group = table.slice(0, row_group_rows)
                writer.write_table(group, row_group_size=row_group_rows)
                written += group.num_rows
                remainder = table.slice(row_group_rows)
                batches = remainder.to_batches(max_chunksize=buffer_limit)
                batched_rows = remainder.num_rows

        def flush_buffer() -> None:
            nonlocal batched_rows
            if not buffer:
                return
            batch = pa.RecordBatch.from_pylist(buffer, schema=schema)
            batches.append(batch)
            batched_rows += batch.num_rows
            buffer.clear()
            write_complete_groups()

        try:
            for (payload,) in cursor:
                raw = pickle.loads(payload)
                if not isinstance(raw, dict):
                    raise StreamingStoreError("event scratch payload is not a row object")
                normalized: dict[str, object] = {}
                for name in PHASE10_EVENT_COLUMNS:
                    normalized[name] = self._arrow_value(
                        raw.get(name), _EVENT_FIELD_CATEGORIES[name]
                    )
                for name, value in normalized_bindings.items():
                    existing = raw.get(name)
                    if not _is_missing(existing) and str(existing) != value:
                        raise StreamingStoreError(
                            f"event row conflicts with evidence binding {name}"
                        )
                    normalized[name] = value
                logical_hash.update(
                    _canonical_json_text(
                        {name: _json_compatible(normalized.get(name)) for name in columns}
                    ).encode("utf-8")
                )
                logical_hash.update(b"\n")
                buffer.append(normalized)
                if len(buffer) == buffer_limit:
                    flush_buffer()
            flush_buffer()
            if batched_rows:
                table = pa.Table.from_batches(batches, schema=schema)
                writer.write_table(table, row_group_size=row_group_rows)
                written += table.num_rows
        finally:
            writer.close()
        with path.open("r+b") as handle:
            os.fsync(handle.fileno())
        return written, path.stat().st_size, logical_hash.hexdigest()


__all__ = [
    "PHASE10_EVENT_COLUMNS",
    "PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS",
    "PHASE10_EVENT_PARQUET_SCHEMA",
    "EventSpool",
    "ExactTimestampNs",
    "SourceRowSpool",
    "StreamingStoreError",
    "datetime_from_ns",
    "timestamp_ns",
]
