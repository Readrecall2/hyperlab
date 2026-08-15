from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from datetime import date as Date
from itertools import pairwise
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import quote, unquote

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from hyperlab.data.schema import (
    BREAKING_SCHEMA_TRANSITIONS,
    SCHEMA_VERSION_METADATA,
    RecordType,
    SchemaSpec,
    schema_fingerprint,
    schema_for,
)

MANIFEST_FORMAT_VERSION = 1
_SLUG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_ASSET = re.compile(r"^[A-Za-z0-9@%][A-Za-z0-9@%._/+:-]{0,127}$")
_CANDLE_INTERVAL = re.compile(r"^([1-9][0-9]*)(s|m|h|d|w)$")
_PARQUET_FILE = re.compile(r"^part-[0-9a-f]{64}\.parquet$")
_MANIFEST_FILE = re.compile(r"^part-[0-9a-f]{64}\.manifest\.json$")
_SCHEMA_VERSION_VALUE = re.compile(rb"^[1-9][0-9]*$")
_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
_FUNDING_BUCKET_TOLERANCE_NS = 60 * 1_000_000_000
_TIMESTAMP_COLUMNS = ("event_time", "exchange_time", "received_time")
_INTERVAL_UNIT_NS = {
    "s": 1_000_000_000,
    "m": 60 * 1_000_000_000,
    "h": 3_600 * 1_000_000_000,
    "d": 86_400 * 1_000_000_000,
    "w": 7 * 86_400 * 1_000_000_000,
}
_L2_METADATA_DEFINITIONS = {
    RecordType.L2_SNAPSHOT: (
        "snapshot_id",
        (
            "book_epoch_id",
            "connection_id",
            "last_sequence",
            "event_time",
            "exchange_time",
            "received_time",
            "source_sequence",
        ),
        "snapshot",
    ),
    RecordType.L2_DELTA: (
        "update_id",
        (
            "book_epoch_id",
            "connection_id",
            "first_sequence",
            "last_sequence",
            "event_time",
            "exchange_time",
            "received_time",
            "source_sequence",
        ),
        "delta",
    ),
}


class DataLakeError(ValueError):
    """Base error for immutable data-lake operations."""


class PartitionValidationError(DataLakeError):
    """Raised when a partition cannot be trusted."""


class PartitionExistsError(DataLakeError):
    """Raised instead of overwriting an existing immutable artifact."""


@dataclass(frozen=True, slots=True)
class PartitionKey:
    venue: str
    date: Date | str
    asset: str
    record_type: RecordType | str

    def __post_init__(self) -> None:
        if not _SLUG.fullmatch(self.venue):
            raise ValueError(f"invalid venue partition slug: {self.venue!r}")
        if not _ASSET.fullmatch(self.asset):
            raise ValueError(f"invalid asset partition value: {self.asset!r}")
        parsed_date: Date
        if isinstance(self.date, str):
            try:
                parsed_date = Date.fromisoformat(self.date)
            except ValueError:
                raise ValueError(f"invalid partition date: {self.date!r}") from None
        else:
            parsed_date = self.date
        try:
            parsed_type = (
                self.record_type if isinstance(self.record_type, RecordType) else RecordType(self.record_type)
            )
        except ValueError:
            raise ValueError(f"invalid record type partition: {self.record_type!r}") from None
        object.__setattr__(self, "date", parsed_date)
        object.__setattr__(self, "record_type", parsed_type)

    def path(self, root: Path) -> Path:
        return root / self.relative_path

    @property
    def relative_path(self) -> Path:
        record_type = _record_type_value(self.record_type)
        partition_date = _date_value(self.date)
        return Path(
            f"venue={self.venue}",
            f"date={partition_date.isoformat()}",
            f"asset={quote(self.asset, safe='-._~')}",
            f"type={record_type}",
        )

    def as_dict(self) -> dict[str, str]:
        return {
            "venue": self.venue,
            "date": _date_value(self.date).isoformat(),
            "asset": self.asset,
            "record_type": _record_type_value(self.record_type),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> PartitionKey:
        try:
            return cls(
                venue=str(payload["venue"]),
                date=str(payload["date"]),
                asset=str(payload["asset"]),
                record_type=str(payload["record_type"]),
            )
        except KeyError as exc:
            raise PartitionValidationError(f"manifest partition is missing {exc.args[0]!r}") from None

    @classmethod
    def from_leaf(cls, leaf: Path) -> PartitionKey:
        parts = (leaf.parent.parent.parent, leaf.parent.parent, leaf.parent, leaf)
        expected = ("venue", "date", "asset", "type")
        values: dict[str, str] = {}
        for directory, label in zip(parts, expected, strict=True):
            prefix = f"{label}="
            if not directory.name.startswith(prefix):
                raise PartitionValidationError(
                    "invalid partition layout: expected "
                    "venue=<venue>/date=YYYY-MM-DD/asset=<asset>/type=<record-type>"
                )
            values[label] = directory.name[len(prefix) :]
        encoded_asset = values["asset"]
        values["asset"] = unquote(encoded_asset)
        if quote(values["asset"], safe="-._~") != encoded_asset:
            raise PartitionValidationError("asset partition encoding is not canonical")
        try:
            return cls(
                venue=values["venue"],
                date=values["date"],
                asset=values["asset"],
                record_type=values["type"],
            )
        except ValueError as exc:
            raise PartitionValidationError(str(exc)) from None


@dataclass(frozen=True, slots=True)
class Gap:
    kind: str
    start: str
    end: str
    missing_count: int
    connection_id: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "start": self.start,
            "end": self.end,
            "missing_count": self.missing_count,
            "connection_id": self.connection_id,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> Gap:
        return cls(
            kind=str(payload["kind"]),
            start=str(payload["start"]),
            end=str(payload["end"]),
            missing_count=int(str(payload["missing_count"])),
            connection_id=(None if payload.get("connection_id") is None else str(payload["connection_id"])),
        )


@dataclass(frozen=True, slots=True)
class PartitionManifest:
    partition: PartitionKey
    data_file: str
    sha256: str
    size_bytes: int
    row_count: int
    timestamp_bounds: dict[str, dict[str, str | None]]
    schema_name: str
    schema_version: int
    schema_fingerprint: str
    stream_key: str
    sequence_min: int | None
    sequence_max: int | None
    duplicates: int
    out_of_order: int
    gaps: tuple[Gap, ...]
    gap_detection: str
    null_counts: dict[str, int]
    quality: str
    expected_interval_ns: int | None = None
    format_version: int = MANIFEST_FORMAT_VERSION

    @property
    def manifest_file(self) -> str:
        return f"{Path(self.data_file).stem}.manifest.json"

    @property
    def relative_data_path(self) -> Path:
        return self.partition.relative_path / self.data_file

    @property
    def relative_manifest_path(self) -> Path:
        return self.partition.relative_path / self.manifest_file

    def as_dict(self) -> dict[str, object]:
        return {
            "format_version": self.format_version,
            "partition": self.partition.as_dict(),
            "data_file": self.data_file,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "row_count": self.row_count,
            "timestamp_bounds": self.timestamp_bounds,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "schema_fingerprint": self.schema_fingerprint,
            "stream_key": self.stream_key,
            "sequence_min": self.sequence_min,
            "sequence_max": self.sequence_max,
            "duplicates": self.duplicates,
            "out_of_order": self.out_of_order,
            "gaps": [gap.as_dict() for gap in self.gaps],
            "gap_detection": self.gap_detection,
            "null_counts": self.null_counts,
            "quality": self.quality,
            "expected_interval_ns": self.expected_interval_ns,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> PartitionManifest:
        try:
            format_version = int(str(payload["format_version"]))
            if format_version != MANIFEST_FORMAT_VERSION:
                raise PartitionValidationError(f"unsupported manifest format version: {format_version}")
            partition_payload = payload["partition"]
            bounds_payload = payload["timestamp_bounds"]
            gaps_payload = payload["gaps"]
            nulls_payload = payload["null_counts"]
            if not isinstance(partition_payload, dict):
                raise TypeError("partition must be an object")
            if not isinstance(bounds_payload, dict):
                raise TypeError("timestamp_bounds must be an object")
            if not isinstance(gaps_payload, list):
                raise TypeError("gaps must be an array")
            if not isinstance(nulls_payload, dict):
                raise TypeError("null_counts must be an object")
            timestamp_bounds: dict[str, dict[str, str | None]] = {}
            for name, value in bounds_payload.items():
                if not isinstance(value, dict):
                    raise TypeError(f"timestamp bound {name!r} must be an object")
                timestamp_bounds[str(name)] = {
                    "min": None if value.get("min") is None else str(value["min"]),
                    "max": None if value.get("max") is None else str(value["max"]),
                }
            gaps = tuple(Gap.from_dict(value) for value in gaps_payload if isinstance(value, dict))
            if len(gaps) != len(gaps_payload):
                raise TypeError("every gap must be an object")
            expected_interval_ns = (
                None
                if payload.get("expected_interval_ns") is None
                else int(str(payload["expected_interval_ns"]))
            )
            if expected_interval_ns is not None and expected_interval_ns <= 0:
                raise PartitionValidationError("expected_interval_ns must be positive")
            return cls(
                format_version=format_version,
                partition=PartitionKey.from_dict(partition_payload),
                data_file=str(payload["data_file"]),
                sha256=str(payload["sha256"]),
                size_bytes=int(str(payload["size_bytes"])),
                row_count=int(str(payload["row_count"])),
                timestamp_bounds=timestamp_bounds,
                schema_name=str(payload["schema_name"]),
                schema_version=int(str(payload["schema_version"])),
                schema_fingerprint=str(payload["schema_fingerprint"]),
                stream_key=str(payload["stream_key"]),
                sequence_min=(None if payload["sequence_min"] is None else int(str(payload["sequence_min"]))),
                sequence_max=(None if payload["sequence_max"] is None else int(str(payload["sequence_max"]))),
                duplicates=int(str(payload["duplicates"])),
                out_of_order=int(str(payload["out_of_order"])),
                gaps=gaps,
                gap_detection=str(payload["gap_detection"]),
                null_counts={str(name): int(str(value)) for name, value in nulls_payload.items()},
                quality=str(payload["quality"]),
                expected_interval_ns=expected_interval_ns,
            )
        except KeyError as exc:
            raise PartitionValidationError(f"manifest is missing {exc.args[0]!r}") from None
        except (TypeError, ValueError) as exc:
            if isinstance(exc, PartitionValidationError):
                raise
            raise PartitionValidationError(f"invalid manifest: {exc}") from None


@dataclass(frozen=True, slots=True)
class InventoryReport:
    partitions: tuple[PartitionManifest, ...]
    total_rows: int
    venues: tuple[str, ...]
    assets: tuple[str, ...]
    record_types: tuple[str, ...]
    dates: tuple[str, ...]
    delisted_assets: tuple[str, ...]
    cross_segment_gaps: tuple[tuple[PartitionKey, Gap], ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "partition_count": len(self.partitions),
            "row_count": self.total_rows,
            "venues": list(self.venues),
            "assets": list(self.assets),
            "record_types": list(self.record_types),
            "dates": list(self.dates),
            "delisted_assets": list(self.delisted_assets),
            "cross_segment_gaps": [
                {"partition": key.as_dict(), **gap.as_dict()} for key, gap in self.cross_segment_gaps
            ],
            "partitions": [manifest.as_dict() for manifest in self.partitions],
        }


@dataclass(frozen=True, slots=True)
class _Analysis:
    timestamp_bounds: dict[str, dict[str, str | None]]
    sequence_min: int | None
    sequence_max: int | None
    duplicates: int
    out_of_order: int
    gaps: tuple[Gap, ...]
    gap_detection: str
    null_counts: dict[str, int]
    quality: str


def _record_type_value(record_type: RecordType | str) -> str:
    return record_type.value if isinstance(record_type, RecordType) else str(record_type)


def _date_value(value: Date | str) -> Date:
    return Date.fromisoformat(value) if isinstance(value, str) else value


def _require_under_root(canonical_root: Path, path: Path, *, label: str) -> Path:
    resolved = path.resolve()
    try:
        resolved.relative_to(canonical_root)
    except ValueError:
        raise PartitionValidationError(f"{label} resolves outside data lake root: {resolved}") from None
    return resolved


def _canonical_json(payload: dict[str, object]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_hashed_path(
    data_path: Path,
    manifest: PartitionManifest,
    *,
    columns: list[str] | None = None,
) -> pa.Table:
    try:
        payload = data_path.read_bytes()
    except FileNotFoundError:
        raise PartitionValidationError(f"partition data file not found: {data_path.name}") from None
    actual_hash = hashlib.sha256(payload).hexdigest()
    if actual_hash != manifest.sha256:
        raise PartitionValidationError(
            "CORRUPT_PARTITION [hash_mismatch] "
            f"partition={manifest.relative_data_path.as_posix()} "
            f"expected_sha256={manifest.sha256} actual_sha256={actual_hash}"
        )
    if len(payload) != manifest.size_bytes:
        raise PartitionValidationError(f"size mismatch for {manifest.data_file}")
    try:
        return pq.ParquetFile(pa.BufferReader(payload)).read(columns=columns)
    except Exception as exc:
        raise PartitionValidationError(f"invalid Parquet file {data_path.name}: {exc}") from None


def read_hashed_table(
    root: Path,
    manifest: PartitionManifest,
    *,
    columns: list[str] | None = None,
) -> pa.Table:
    """Hash and decode the same immutable payload bytes to reduce TOCTOU exposure."""

    return _read_hashed_path(
        root / manifest.relative_data_path,
        manifest,
        columns=columns,
    )


def _timestamp_iso(epoch_ns: int) -> str:
    seconds, nanoseconds = divmod(epoch_ns, 1_000_000_000)
    base = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{nanoseconds:09d}Z"


def _timestamp_bounds(table: pa.Table, name: str) -> dict[str, str | None]:
    values = pc.cast(table.column(name).combine_chunks(), pa.int64())
    minimum = pc.min(values).as_py()
    maximum = pc.max(values).as_py()
    return {
        "min": None if minimum is None else _timestamp_iso(int(minimum)),
        "max": None if maximum is None else _timestamp_iso(int(maximum)),
    }


def _normalized_row(value: tuple[object, ...]) -> tuple[tuple[int, object], ...]:
    return tuple((1, "") if item is None else (0, item) for item in value)


def _rows_for(table: pa.Table, columns: tuple[str, ...]) -> list[tuple[object, ...]]:
    values = [table.column(name).to_pylist() for name in columns]
    return list(zip(*values, strict=True))


def _interval_ns(expected_interval: timedelta | None) -> int | None:
    if expected_interval is None:
        return None
    value = (
        expected_interval.days * 86_400 * 1_000_000_000
        + expected_interval.seconds * 1_000_000_000
        + expected_interval.microseconds * 1_000
    )
    if value <= 0:
        raise ValueError("expected_interval must be positive")
    return value


def _declared_candle_interval_ns(table: pa.Table) -> int:
    intervals = set(table.column("interval").to_pylist())
    if len(intervals) != 1:
        raise PartitionValidationError("one candle interval is required per immutable Parquet file")
    interval = str(next(iter(intervals)))
    match = _CANDLE_INTERVAL.fullmatch(interval)
    if match is None:
        raise PartitionValidationError(f"unsupported declared candle interval: {interval!r}")
    return int(match.group(1)) * _INTERVAL_UNIT_NS[match.group(2)]


def _stream_definition(
    table: pa.Table,
    key: PartitionKey,
    requested_interval_ns: int | None,
) -> tuple[int | None, str]:
    if key.record_type == RecordType.CANDLE:
        declared_interval_ns = _declared_candle_interval_ns(table)
        if requested_interval_ns is not None and requested_interval_ns != declared_interval_ns:
            raise PartitionValidationError("expected_interval does not match the declared candle interval")
        interval = str(table.column("interval")[0].as_py())
        return declared_interval_ns, f"candle:{interval}"
    if key.record_type == RecordType.FUNDING:
        definitions = set(
            zip(
                table.column("rate_kind").to_pylist(),
                table.column("funding_interval_seconds").to_pylist(),
                strict=True,
            )
        )
        if len(definitions) != 1:
            raise PartitionValidationError(
                "one rate_kind and funding interval are required per immutable Parquet file"
            )
        raw_rate_kind, raw_seconds = next(iter(definitions))
        rate_kind = str(raw_rate_kind)
        interval_seconds = int(str(raw_seconds))
        if interval_seconds <= 0:
            raise PartitionValidationError("declared funding interval must be positive")
        declared_interval_ns = interval_seconds * 1_000_000_000
        if requested_interval_ns is not None and requested_interval_ns != declared_interval_ns:
            raise PartitionValidationError("expected_interval does not match the declared funding interval")
        stream_key = json.dumps(
            {
                "funding_interval_seconds": interval_seconds,
                "rate_kind": rate_kind,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return declared_interval_ns, f"funding:{stream_key}"
    return requested_interval_ns, "default"


def _sequence_gaps(table: pa.Table) -> list[Gap]:
    sequences = table.column("source_sequence").to_pylist()
    connections = table.column("connection_id").to_pylist()
    previous: dict[str | None, int] = {}
    gaps: list[Gap] = []
    for connection, raw_sequence in zip(connections, sequences, strict=True):
        if raw_sequence is None:
            continue
        sequence = int(raw_sequence)
        prior = previous.get(connection)
        if prior is not None and sequence > prior + 1:
            gaps.append(
                Gap(
                    kind="sequence",
                    start=str(prior),
                    end=str(sequence),
                    missing_count=sequence - prior - 1,
                    connection_id=connection,
                )
            )
        elif prior is not None and sequence < prior:
            gaps.append(
                Gap(
                    kind="sequence_regression",
                    start=str(prior),
                    end=str(sequence),
                    missing_count=0,
                    connection_id=connection,
                )
            )
        if prior != sequence:
            previous[connection] = sequence
    return gaps


def _funding_bucket(epoch_ns: int, interval_ns: int) -> int:
    tolerance_ns = min(
        _FUNDING_BUCKET_TOLERANCE_NS,
        max(interval_ns // 2 - 1, 0),
    )
    return (epoch_ns + tolerance_ns) // interval_ns


def _time_gaps(
    table: pa.Table,
    record_type: RecordType | str,
    expected_interval_ns: int | None,
) -> list[Gap]:
    if expected_interval_ns is None:
        return []
    is_funding = record_type == RecordType.FUNDING
    timestamp_name = "funding_time" if is_funding else "event_time"
    raw_times = pc.cast(
        table.column(timestamp_name).combine_chunks(),
        pa.int64(),
    ).to_pylist()
    times = list(dict.fromkeys(int(value) for value in raw_times if value is not None))
    gaps: list[Gap] = []
    for previous, current in pairwise(times):
        if is_funding:
            previous_bucket = _funding_bucket(previous, expected_interval_ns)
            current_bucket = _funding_bucket(current, expected_interval_ns)
            difference = current_bucket - previous_bucket
            if difference > 1:
                gaps.append(
                    Gap(
                        kind="funding_bucket",
                        start=_timestamp_iso(previous),
                        end=_timestamp_iso(current),
                        missing_count=difference - 1,
                    )
                )
            continue
        difference = current - previous
        if difference > expected_interval_ns:
            gaps.append(
                Gap(
                    kind="time",
                    start=_timestamp_iso(previous),
                    end=_timestamp_iso(current),
                    missing_count=max(difference // expected_interval_ns - 1, 1),
                )
            )
    return gaps


def _arrival_sequence_gaps(table: pa.Table) -> list[Gap]:
    connections = table.column("connection_id").to_pylist()
    epochs = table.column("connection_epoch").to_pylist()
    sequences = table.column("arrival_sequence").to_pylist()
    previous: dict[tuple[str, int], int] = {}
    gaps: list[Gap] = []
    for raw_connection, raw_epoch, raw_sequence in zip(
        connections,
        epochs,
        sequences,
        strict=True,
    ):
        connection = str(raw_connection)
        key = (connection, int(raw_epoch))
        sequence = int(raw_sequence)
        prior = previous.get(key)
        if prior is not None and sequence > prior + 1:
            gaps.append(
                Gap(
                    kind="arrival_sequence",
                    start=str(prior),
                    end=str(sequence),
                    missing_count=sequence - prior - 1,
                    connection_id=connection,
                )
            )
        elif prior is not None and sequence < prior:
            gaps.append(
                Gap(
                    kind="arrival_sequence_regression",
                    start=str(prior),
                    end=str(sequence),
                    missing_count=0,
                    connection_id=connection,
                )
            )
        if prior != sequence:
            previous[key] = sequence
    return gaps


def _require_partition_value(table: pa.Table, name: str, expected: object) -> None:
    observed = set(table.column(name).to_pylist())
    if observed != {expected}:
        raise PartitionValidationError(
            f"partition {name} mismatch: expected {expected!r}, observed {sorted(map(str, observed))}"
        )


def _duplicate_primary_keys(table: pa.Table, spec: SchemaSpec) -> int:
    seen: set[tuple[object, ...]] = set()
    duplicates = 0
    for row in _rows_for(table, spec.primary_key):
        if row in seen:
            duplicates += 1
        else:
            seen.add(row)
    return duplicates


def _out_of_order_rows(table: pa.Table, spec: SchemaSpec) -> int:
    order_rows = [_normalized_row(row) for row in _rows_for(table, spec.order_key)]
    return sum(current < previous for previous, current in pairwise(order_rows))


def _require_positive_prices(table: pa.Table, *names: str) -> None:
    for name in names:
        if any(value is not None and value <= 0 for value in table.column(name).to_pylist()):
            raise PartitionValidationError(f"{name} must be a positive price")


def _require_non_negative(table: pa.Table, *names: str) -> None:
    for name in names:
        if any(value is not None and value < 0 for value in table.column(name).to_pylist()):
            raise PartitionValidationError(f"{name} must be non-negative")


def _require_positive_when_present(table: pa.Table, *names: str) -> None:
    for name in names:
        if any(value is not None and value <= 0 for value in table.column(name).to_pylist()):
            raise PartitionValidationError(f"{name} must be positive when present")


def _require_closed_vocabulary(table: pa.Table, name: str, allowed: frozenset[str]) -> None:
    observed = {str(value) for value in table.column(name).to_pylist()}
    invalid = sorted(observed - allowed)
    if invalid:
        raise PartitionValidationError(f"{name} contains unsupported values: {', '.join(invalid)}")


def _require_non_empty_strings(table: pa.Table, *names: str) -> None:
    for name in names:
        if any(value is None or not str(value).strip() for value in table.column(name).to_pylist()):
            raise PartitionValidationError(f"{name} must be non-empty")


def _require_optional_non_empty_strings(table: pa.Table, *names: str) -> None:
    for name in names:
        if any(value is not None and not str(value).strip() for value in table.column(name).to_pylist()):
            raise PartitionValidationError(f"{name} must be non-empty when present")


def _validate_text_hash(table: pa.Table, text_name: str, hash_name: str) -> None:
    for raw_text, raw_hash in zip(
        table.column(text_name).to_pylist(),
        table.column(hash_name).to_pylist(),
        strict=True,
    ):
        declared_hash = str(raw_hash)
        if _SHA256_HEX.fullmatch(declared_hash) is None:
            raise PartitionValidationError(f"{hash_name} must be lowercase SHA-256 hex")
        actual_hash = hashlib.sha256(str(raw_text).encode("utf-8")).hexdigest()
        if actual_hash != declared_hash:
            raise PartitionValidationError(f"{hash_name} does not match the exact UTF-8 {text_name}")


def _require_valid_json(table: pa.Table, name: str) -> None:
    for raw_value in table.column(name).to_pylist():
        try:
            json.loads(str(raw_value))
        except json.JSONDecodeError:
            raise PartitionValidationError(f"{name} must contain valid JSON") from None


def _validate_wire_json_flags(table: pa.Table) -> None:
    for raw_message, declared_is_json in zip(
        table.column("raw_message").to_pylist(),
        table.column("is_json").to_pylist(),
        strict=True,
    ):
        try:
            json.loads(str(raw_message))
            actual_is_json = True
        except json.JSONDecodeError:
            actual_is_json = False
        if bool(declared_is_json) != actual_is_json:
            raise PartitionValidationError("is_json must describe whether raw_message is valid JSON")


def _validate_l2_metadata(table: pa.Table, key: PartitionKey) -> None:
    definition = _L2_METADATA_DEFINITIONS.get(RecordType(_record_type_value(key.record_type)))
    if definition is None:
        return
    identifier_name, metadata_names, label = definition
    identifiers = table.column(identifier_name).to_pylist()
    metadata_columns = [table.column(name).to_pylist() for name in metadata_names]
    expected_by_identifier: dict[str, tuple[object, ...]] = {}
    for identifier, *metadata in zip(identifiers, *metadata_columns, strict=True):
        normalized_identifier = str(identifier)
        observed_metadata = tuple(metadata)
        previous = expected_by_identifier.setdefault(normalized_identifier, observed_metadata)
        if previous != observed_metadata:
            raise PartitionValidationError(
                f"inconsistent L2 {label} metadata for {identifier_name} {normalized_identifier!r}"
            )


def _validate_semantics(table: pa.Table, key: PartitionKey, spec: SchemaSpec) -> None:
    record_type = key.record_type
    if record_type == RecordType.INSTRUMENT_METADATA:
        _require_closed_vocabulary(
            table,
            "instrument_kind",
            frozenset({"spot", "perp"}),
        )
        _require_non_empty_strings(
            table,
            "instrument_id",
            "source_symbol",
            "metadata_sha256",
            "metadata_json",
        )
        _require_optional_non_empty_strings(
            table,
            "base_token",
            "quote_token",
            "full_name",
        )
        if any(value is not None and value <= 0 for value in table.column("max_leverage").to_pylist()):
            raise PartitionValidationError("max_leverage must be positive when present")
        _validate_text_hash(table, "metadata_json", "metadata_sha256")
        _require_valid_json(table, "metadata_json")
    elif record_type == RecordType.MARKET_CONTEXT:
        _require_closed_vocabulary(
            table,
            "instrument_kind",
            frozenset({"spot", "perp"}),
        )
        _require_non_empty_strings(table, "instrument_id")
        _require_positive_prices(
            table,
            "mark_price",
            "oracle_price",
            "mid_price",
        )
        _require_non_negative(
            table,
            "previous_day_price",
            "open_interest_quantity",
            "open_interest_notional",
            "base_volume_24h",
            "notional_volume_24h",
            "circulating_supply",
        )
    elif record_type == RecordType.WIRE_MESSAGE:
        _require_non_empty_strings(
            table,
            "connection_id",
            "raw_message",
            "payload_sha256",
        )
        _require_optional_non_empty_strings(table, "channel")
        _require_positive_when_present(
            table,
            "connection_epoch",
            "arrival_sequence",
        )
        if spec.version >= 2:
            _require_optional_non_empty_strings(table, "capture_epoch_id")
        if table.column("source_sequence").null_count != table.num_rows:
            raise PartitionValidationError(
                "wire_message source_sequence must remain null; use arrival_sequence"
            )
        _validate_text_hash(table, "raw_message", "payload_sha256")
        _validate_wire_json_flags(table)
    elif record_type == RecordType.CANDLE:
        _require_positive_prices(table, "open", "high", "low", "close")
        _require_non_negative(table, "base_volume", "quote_volume")
        for open_time, close_time, open_price, high, low, close in zip(
            table.column("open_time").to_pylist(),
            table.column("close_time").to_pylist(),
            table.column("open").to_pylist(),
            table.column("high").to_pylist(),
            table.column("low").to_pylist(),
            table.column("close").to_pylist(),
            strict=True,
        ):
            if open_time >= close_time:
                raise PartitionValidationError("open_time must be before close_time")
            if low > min(open_price, close) or high < max(open_price, close) or low > high:
                raise PartitionValidationError(
                    "invalid OHLC relationship: low <= open/close <= high is required"
                )
    elif record_type == RecordType.BBO:
        _require_positive_prices(table, "bid_price", "ask_price")
        _require_non_negative(table, "bid_quantity", "ask_quantity")
        for bid_price, bid_quantity, ask_price, ask_quantity in zip(
            table.column("bid_price").to_pylist(),
            table.column("bid_quantity").to_pylist(),
            table.column("ask_price").to_pylist(),
            table.column("ask_quantity").to_pylist(),
            strict=True,
        ):
            if (bid_price is None) != (bid_quantity is None):
                raise PartitionValidationError("BBO bid price and quantity must be present together")
            if (ask_price is None) != (ask_quantity is None):
                raise PartitionValidationError("BBO ask price and quantity must be present together")
            if bid_price is not None and ask_price is not None and bid_price > ask_price:
                raise PartitionValidationError("bid_price must not exceed ask_price")
    elif record_type in {RecordType.L2_SNAPSHOT, RecordType.L2_DELTA}:
        _require_closed_vocabulary(table, "side", frozenset({"bid", "ask"}))
        _require_positive_prices(table, "price")
        _require_non_negative(table, "quantity")
        if record_type == RecordType.L2_DELTA:
            _require_closed_vocabulary(table, "action", frozenset({"set", "delete"}))
            for first, last in zip(
                table.column("first_sequence").to_pylist(),
                table.column("last_sequence").to_pylist(),
                strict=True,
            ):
                if first is not None and last is not None and first > last:
                    raise PartitionValidationError("first_sequence must not exceed last_sequence")
    elif record_type == RecordType.TRADE:
        _require_closed_vocabulary(
            table,
            "aggressor_side",
            frozenset({"buy", "sell", "unknown"}),
        )
        _require_positive_prices(table, "price")
        if any(value <= 0 for value in table.column("quantity").to_pylist()):
            raise PartitionValidationError("trade quantity must be positive")
        _require_non_negative(table, "quote_quantity")
        if spec.version >= 2:
            _require_positive_when_present(
                table,
                "connection_epoch",
                "arrival_sequence",
            )
    elif record_type == RecordType.FUNDING:
        _require_non_empty_strings(table, "rate_kind")
        _require_positive_prices(table, "mark_price", "oracle_price")
    elif record_type == RecordType.OPEN_INTEREST:
        _require_non_negative(
            table,
            "open_interest_quantity",
            "open_interest_notional",
        )
        _require_positive_prices(table, "mark_price")
    elif record_type == RecordType.FEE:
        _require_non_empty_strings(table, "scope")
        _require_closed_vocabulary(table, "instrument_kind", frozenset({"spot", "perp"}))
        for start, end in zip(
            table.column("effective_from").to_pylist(),
            table.column("effective_to").to_pylist(),
            strict=True,
        ):
            if end is not None and start > end:
                raise PartitionValidationError("effective_from must not be after effective_to")
    elif record_type == RecordType.CONNECTION_EVENT:
        _require_closed_vocabulary(
            table,
            "event_kind",
            frozenset({"connect", "disconnect", "gap", "resync_start", "resync_complete"}),
        )
        if spec.version >= 2:
            _require_positive_when_present(table, "connection_epoch")
            _require_optional_non_empty_strings(
                table,
                "capture_epoch_id",
                "socket_role",
            )
    elif record_type == RecordType.INSTRUMENT_LIFECYCLE:
        _require_closed_vocabulary(table, "instrument_kind", frozenset({"spot", "perp"}))
        _require_closed_vocabulary(
            table,
            "status",
            frozenset({"listed", "renamed", "delisted"}),
        )
        for start, end in zip(
            table.column("valid_from").to_pylist(),
            table.column("valid_to").to_pylist(),
            strict=True,
        ):
            if end is not None and start > end:
                raise PartitionValidationError("valid_from must not be after valid_to")
    elif record_type == RecordType.CLOCK_SYNC:
        _require_non_negative(
            table,
            "round_trip_latency_ms",
            "drift_uncertainty_ms",
        )
        for sent, received, uncertainty, round_trip in zip(
            table.column("request_sent_time").to_pylist(),
            table.column("response_received_time").to_pylist(),
            table.column("drift_uncertainty_ms").to_pylist(),
            table.column("round_trip_latency_ms").to_pylist(),
            strict=True,
        ):
            if sent > received:
                raise PartitionValidationError("clock sync request must be sent before its response")
            if uncertainty * 2 != round_trip:
                raise PartitionValidationError("clock drift uncertainty must equal half the round trip")
        if spec.version >= 2:
            _validate_clock_sync_v2(table)
        if spec.version >= 3:
            _validate_clock_sync_v3(table)


def _validate_clock_sync_v2(table: pa.Table) -> None:
    _require_closed_vocabulary(
        table,
        "sample_status",
        frozenset({"valid", "invalid"}),
    )
    _require_optional_non_empty_strings(
        table,
        "capture_epoch_id",
        "invalid_reason",
    )
    _require_non_negative(table, "max_uncertainty_ms")
    _require_positive_when_present(table, "connection_epoch")
    columns = {
        name: table.column(name).to_pylist()
        for name in (
            "connection_id",
            "connection_epoch",
            "capture_epoch_id",
            "received_time",
            "response_received_time",
            "drift_uncertainty_ms",
            "causal_valid_from",
            "causal_valid_until",
            "sample_status",
            "invalid_reason",
            "sampling_interval_ms",
            "max_age_ms",
            "max_uncertainty_ms",
        )
    }
    for values in zip(*columns.values(), strict=True):
        row = dict(zip(columns, values, strict=True))
        sampling_interval_ms = row["sampling_interval_ms"]
        max_age_ms = row["max_age_ms"]
        max_uncertainty_ms = row["max_uncertainty_ms"]
        if sampling_interval_ms is None or int(sampling_interval_ms) <= 0:
            raise PartitionValidationError("sampling_interval_ms must be positive")
        if max_age_ms is None or int(max_age_ms) <= 0:
            raise PartitionValidationError("max_age_ms must be positive")
        if int(sampling_interval_ms) > int(max_age_ms):
            raise PartitionValidationError(
                "sampling_interval_ms must not exceed max_age_ms"
            )
        if max_uncertainty_ms is None:
            raise PartitionValidationError("max_uncertainty_ms must be present")
        if row["received_time"] != row["response_received_time"]:
            raise PartitionValidationError(
                "received_time must equal response_received_time for clock sync"
            )

        if row["sample_status"] == "valid":
            for identity in ("connection_id", "connection_epoch", "capture_epoch_id"):
                if row[identity] is None or (
                    isinstance(row[identity], str) and not row[identity].strip()
                ):
                    raise PartitionValidationError(
                        f"{identity} must be present for valid clock coverage"
                    )
            if row["drift_uncertainty_ms"] > max_uncertainty_ms:
                raise PartitionValidationError(
                    "valid clock uncertainty exceeds max_uncertainty_ms"
                )
            if row["invalid_reason"] is not None:
                raise PartitionValidationError(
                    "invalid_reason must be null for valid clock coverage"
                )
            expected_from = row["response_received_time"]
            if row["causal_valid_from"] != expected_from:
                raise PartitionValidationError(
                    "causal_valid_from must equal response_received_time"
                )
            expected_until = expected_from + timedelta(
                milliseconds=int(max_age_ms)
            )
            if row["causal_valid_until"] != expected_until:
                raise PartitionValidationError(
                    "causal_valid_until must equal causal_valid_from plus max_age_ms"
                )
            continue

        if row["causal_valid_from"] is not None or row["causal_valid_until"] is not None:
            raise PartitionValidationError(
                "invalid clock sample must not claim a causal validity interval"
            )
        if row["invalid_reason"] is None or not str(row["invalid_reason"]).strip():
            raise PartitionValidationError(
                "invalid_reason must be present for invalid clock sample"
            )


def _validate_clock_sync_v3(table: pa.Table) -> None:
    _require_non_negative(
        table,
        "clock_schedule_overdue_ms",
        "single_flight_blocked_ms",
        "executor_submit_to_worker_start_ms",
        "worker_completion_to_supervisor_drain_ms",
        "transport_lock_wait_ms",
        "requests_adapter_header_elapsed_ms",
        "session_get_total_ms",
        "json_decode_ms",
        "diagnostic_prepare_ms",
        "diagnostic_finalize_ms",
    )
    _require_positive_when_present(table, "peer_port")
    _require_optional_non_empty_strings(
        table,
        "urllib3_connection_identity",
        "tls_socket_identity",
        "peer_ip",
        "socket_family",
        "response_cloudfront_pop",
        "response_cache",
    )


def _analyze_table(
    table: pa.Table,
    key: PartitionKey,
    spec: SchemaSpec,
    *,
    expected_interval_ns: int | None,
) -> _Analysis:
    if table.num_rows == 0:
        raise PartitionValidationError("empty partitions are not allowed")
    if not table.schema.equals(spec.schema, check_metadata=True):
        raise PartitionValidationError(
            f"schema mismatch for {_record_type_value(key.record_type)} version {spec.version}"
        )

    null_counts = {name: table.column(name).null_count for name in table.column_names}
    required_nulls = [
        field.name for field in spec.schema if not field.nullable and null_counts[field.name] > 0
    ]
    if required_nulls:
        raise PartitionValidationError(f"non-nullable columns contain nulls: {', '.join(required_nulls)}")

    _require_partition_value(table, "schema_version", spec.version)
    _require_partition_value(table, "record_type", _record_type_value(key.record_type))
    _require_partition_value(table, "venue", key.venue)
    _require_partition_value(table, "asset", key.asset)
    _validate_semantics(table, key, spec)
    _validate_l2_metadata(table, key)

    event_dates = {
        value.date().isoformat() for value in table.column("event_time").to_pylist() if value is not None
    }
    expected_date = _date_value(key.date).isoformat()
    if event_dates != {expected_date}:
        raise PartitionValidationError(
            f"partition date mismatch: expected {expected_date!r}, observed {sorted(event_dates)}"
        )

    duplicates = _duplicate_primary_keys(table, spec)
    if duplicates:
        raise PartitionValidationError(f"duplicate primary keys: {duplicates}")

    out_of_order = _out_of_order_rows(table, spec)
    if out_of_order:
        raise PartitionValidationError(f"out-of-order rows: {out_of_order}")

    raw_sequences = table.column("source_sequence").to_pylist()
    sequence_values = [int(value) for value in raw_sequences if value is not None]
    gap_sources = [
        *_sequence_gaps(table),
        *_time_gaps(
            table,
            key.record_type,
            expected_interval_ns,
        ),
    ]
    if key.record_type == RecordType.WIRE_MESSAGE:
        gap_sources.extend(_arrival_sequence_gaps(table))
    gaps = tuple(
        sorted(
            gap_sources,
            key=lambda item: (item.kind, item.connection_id or "", item.start, item.end),
        )
    )
    complete_sequence = len(sequence_values) == table.num_rows
    if key.record_type == RecordType.WIRE_MESSAGE:
        gap_detection = "arrival_sequence"
    elif key.record_type == RecordType.FUNDING and expected_interval_ns is not None:
        gap_detection = "funding_bucket_and_sequence" if complete_sequence else "funding_bucket"
    elif expected_interval_ns is not None and complete_sequence:
        gap_detection = "cadence_and_sequence"
    elif expected_interval_ns is not None:
        gap_detection = "cadence"
    elif complete_sequence:
        gap_detection = "sequence"
    else:
        gap_detection = "not_observable"
    return _Analysis(
        timestamp_bounds={name: _timestamp_bounds(table, name) for name in _TIMESTAMP_COLUMNS},
        sequence_min=min(sequence_values) if sequence_values else None,
        sequence_max=max(sequence_values) if sequence_values else None,
        duplicates=duplicates,
        out_of_order=out_of_order,
        gaps=gaps,
        gap_detection=gap_detection,
        null_counts=null_counts,
        quality=("degraded" if gaps else "unobservable" if gap_detection == "not_observable" else "ok"),
    )


def _manifest_for(
    key: PartitionKey,
    spec: SchemaSpec,
    analysis: _Analysis,
    *,
    data_file: str,
    sha256: str,
    size_bytes: int,
    row_count: int,
    expected_interval_ns: int | None,
    stream_key: str,
) -> PartitionManifest:
    return PartitionManifest(
        partition=key,
        data_file=data_file,
        sha256=sha256,
        size_bytes=size_bytes,
        row_count=row_count,
        timestamp_bounds=analysis.timestamp_bounds,
        schema_name=spec.record_type.value,
        schema_version=spec.version,
        schema_fingerprint=schema_fingerprint(spec),
        stream_key=stream_key,
        sequence_min=analysis.sequence_min,
        sequence_max=analysis.sequence_max,
        duplicates=analysis.duplicates,
        out_of_order=analysis.out_of_order,
        gaps=analysis.gaps,
        gap_detection=analysis.gap_detection,
        null_counts=analysis.null_counts,
        quality=analysis.quality,
        expected_interval_ns=expected_interval_ns,
    )


def _publish_exclusive(temporary: Path, target: Path, *, expected_bytes: bytes | None = None) -> None:
    try:
        os.link(temporary, target)
    except FileExistsError:
        if expected_bytes is not None:
            identical = target.read_bytes() == expected_bytes
        else:
            identical = _sha256(target) == _sha256(temporary)
        if not identical:
            raise PartitionExistsError(f"refusing to overwrite immutable file: {target.name}") from None
    finally:
        temporary.unlink(missing_ok=True)


def _flush_and_fsync(stream: BinaryIO) -> None:
    """Flush Python buffers and force one regular file to stable storage."""

    stream.flush()
    os.fsync(stream.fileno())


def _fsync_file(path: Path) -> None:
    """Force a closed producer's file contents and metadata to stable storage."""

    with path.open("r+b") as stream:
        _flush_and_fsync(stream)


def _fsync_directory(path: Path) -> None:
    """Persist directory-entry changes where directory fsync is supported."""

    if os.name == "nt":
        # Python cannot portably open a Windows directory for os.fsync(). The
        # regular files are still fsynced; Linux production additionally gets
        # the directory durability barrier below.
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _missing_directories(root: Path, leaf: Path) -> tuple[Path, ...]:
    """Return missing directories from leaf upward, bounded by root."""

    missing: list[Path] = []
    current = leaf
    while True:
        if not current.exists():
            missing.append(current)
        if current == root:
            return tuple(missing)
        current = current.parent


def _fsync_created_directory_entries(created: tuple[Path, ...]) -> None:
    """Persist each newly created directory entry in its parent directory."""

    for directory in created:
        _fsync_directory(directory.parent)


def _schema_spec_for_write(key: PartitionKey, table: pa.Table) -> SchemaSpec:
    raw_version = (table.schema.metadata or {}).get(SCHEMA_VERSION_METADATA)
    if raw_version is None:
        raise PartitionValidationError("missing hyperlab.schema_version metadata")
    if _SCHEMA_VERSION_VALUE.fullmatch(raw_version) is None:
        raise PartitionValidationError(f"invalid hyperlab.schema_version metadata: {raw_version!r}")
    try:
        version = int(raw_version)
    except ValueError:
        raise PartitionValidationError(f"invalid hyperlab.schema_version metadata: {raw_version!r}") from None
    try:
        return schema_for(key.record_type, version)
    except ValueError as exc:
        raise PartitionValidationError(str(exc)) from None


_TimingObserver = Callable[[str, int], None]


def _observe_timing(
    observer: _TimingObserver | None,
    stage: str,
    started_ns: int,
    monotonic_ns: Callable[[], int],
) -> None:
    if observer is None:
        return
    try:
        observer(stage, max(monotonic_ns() - started_ns, 0))
    except Exception:
        # Storage observability must never change immutable publication semantics.
        return


def write_partition(
    root: Path,
    key: PartitionKey,
    table: pa.Table,
    expected_interval: timedelta | None = None,
    *,
    _timing_observer: _TimingObserver | None = None,
    _monotonic_ns: Callable[[], int] = time.monotonic_ns,
) -> PartitionManifest:
    """Validate and atomically publish one immutable, content-addressed Parquet file."""

    analysis_started_ns = _monotonic_ns()
    try:
        spec = _schema_spec_for_write(key, table)
        expected_interval_ns, stream_key = _stream_definition(
            table,
            key,
            _interval_ns(expected_interval),
        )
        analysis = _analyze_table(
            table,
            key,
            spec,
            expected_interval_ns=expected_interval_ns,
        )
    finally:
        _observe_timing(
            _timing_observer,
            "partition_analysis",
            analysis_started_ns,
            _monotonic_ns,
        )
    canonical_root = root.resolve()
    leaf = _require_under_root(
        canonical_root,
        key.path(root),
        label="partition leaf",
    )
    missing_directories = _missing_directories(canonical_root, leaf)
    leaf.mkdir(parents=True, exist_ok=True)
    leaf = _require_under_root(canonical_root, leaf, label="partition leaf")
    directory_fsync_started_ns = _monotonic_ns()
    try:
        _fsync_created_directory_entries(missing_directories)
    finally:
        _observe_timing(
            _timing_observer,
            "partition_directory_fsync",
            directory_fsync_started_ns,
            _monotonic_ns,
        )
    temporary = leaf / f".{uuid.uuid4().hex}.parquet.tmp"
    _require_under_root(canonical_root, temporary, label="temporary Parquet")
    try:
        parquet_started_ns = _monotonic_ns()
        try:
            pq.write_table(
                table.combine_chunks(),
                temporary,
                compression="zstd",
                version="2.6",
                data_page_version="2.0",
                use_dictionary=False,
                write_statistics=True,
                row_group_size=65_536,
                store_schema=True,
            )
        finally:
            _observe_timing(
                _timing_observer,
                "parquet_write",
                parquet_started_ns,
                _monotonic_ns,
            )
        parquet_fsync_started_ns = _monotonic_ns()
        try:
            _fsync_file(temporary)
        finally:
            _observe_timing(
                _timing_observer,
                "parquet_fsync",
                parquet_fsync_started_ns,
                _monotonic_ns,
            )
        hash_started_ns = _monotonic_ns()
        try:
            digest = _sha256(temporary)
        finally:
            _observe_timing(
                _timing_observer,
                "parquet_hash",
                hash_started_ns,
                _monotonic_ns,
            )
        data_file = f"part-{digest}.parquet"
        data_path = leaf / data_file
        _require_under_root(canonical_root, data_path, label="partition data")
        size_bytes = temporary.stat().st_size
        manifest = _manifest_for(
            key,
            spec,
            analysis,
            data_file=data_file,
            sha256=digest,
            size_bytes=size_bytes,
            row_count=table.num_rows,
            expected_interval_ns=expected_interval_ns,
            stream_key=stream_key,
        )
        publish_started_ns = _monotonic_ns()
        try:
            _publish_exclusive(temporary, data_path)
        finally:
            _observe_timing(
                _timing_observer,
                "partition_publish",
                publish_started_ns,
                _monotonic_ns,
            )
        directory_fsync_started_ns = _monotonic_ns()
        try:
            _fsync_directory(leaf)
        finally:
            _observe_timing(
                _timing_observer,
                "data_directory_fsync",
                directory_fsync_started_ns,
                _monotonic_ns,
            )
    finally:
        temporary.unlink(missing_ok=True)

    manifest_bytes = _canonical_json(manifest.as_dict())
    manifest_path = leaf / manifest.manifest_file
    _require_under_root(canonical_root, manifest_path, label="partition manifest")
    manifest_temporary = leaf / f".{uuid.uuid4().hex}.manifest.tmp"
    _require_under_root(canonical_root, manifest_temporary, label="temporary manifest")
    try:
        with manifest_temporary.open("xb") as stream:
            stream.write(manifest_bytes)
            fsync_started_ns = _monotonic_ns()
            try:
                _flush_and_fsync(stream)
            finally:
                _observe_timing(
                    _timing_observer,
                    "manifest_fsync",
                    fsync_started_ns,
                    _monotonic_ns,
                )
        publish_started_ns = _monotonic_ns()
        try:
            _publish_exclusive(
                manifest_temporary,
                manifest_path,
                expected_bytes=manifest_bytes,
            )
        finally:
            _observe_timing(
                _timing_observer,
                "partition_publish",
                publish_started_ns,
                _monotonic_ns,
            )
        directory_fsync_started_ns = _monotonic_ns()
        try:
            _fsync_directory(leaf)
        finally:
            _observe_timing(
                _timing_observer,
                "manifest_directory_fsync",
                directory_fsync_started_ns,
                _monotonic_ns,
            )
    finally:
        manifest_temporary.unlink(missing_ok=True)
    validation_started_ns = _monotonic_ns()
    try:
        return validate_partition(manifest_path)
    finally:
        _observe_timing(
            _timing_observer,
            "immediate_validation",
            validation_started_ns,
            _monotonic_ns,
        )


def recover_partition_manifest(root: Path, data_path: Path) -> PartitionManifest:
    """Rebuild and atomically publish the manifest for one valid orphan Parquet."""

    canonical_root = root.resolve()
    resolved_data_path = _require_under_root(
        canonical_root,
        data_path,
        label="orphan partition data",
    )
    if not resolved_data_path.is_file() or _PARQUET_FILE.fullmatch(resolved_data_path.name) is None:
        raise PartitionValidationError(f"invalid orphan Parquet path: {data_path.name}")
    manifest_path = resolved_data_path.with_name(f"{resolved_data_path.stem}.manifest.json")
    if manifest_path.exists():
        existing = validate_partition(manifest_path)
        _fsync_directory(manifest_path.parent)
        return existing

    payload = resolved_data_path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    expected_data_file = f"part-{digest}.parquet"
    if resolved_data_path.name != expected_data_file:
        raise PartitionValidationError(
            f"orphan Parquet content-address mismatch: expected {expected_data_file}"
        )
    try:
        table = pq.ParquetFile(pa.BufferReader(payload)).read()
    except Exception as exc:
        raise PartitionValidationError(
            f"invalid orphan Parquet file {resolved_data_path.name}: {exc}"
        ) from None

    key = PartitionKey.from_leaf(resolved_data_path.parent)
    spec = _schema_spec_for_write(key, table)
    expected_interval_ns, stream_key = _stream_definition(table, key, None)
    analysis = _analyze_table(
        table,
        key,
        spec,
        expected_interval_ns=expected_interval_ns,
    )
    manifest = _manifest_for(
        key,
        spec,
        analysis,
        data_file=resolved_data_path.name,
        sha256=digest,
        size_bytes=len(payload),
        row_count=table.num_rows,
        expected_interval_ns=expected_interval_ns,
        stream_key=stream_key,
    )
    manifest_bytes = _canonical_json(manifest.as_dict())
    temporary = resolved_data_path.parent / f".{uuid.uuid4().hex}.manifest.tmp"
    _require_under_root(canonical_root, temporary, label="temporary recovery manifest")
    try:
        with temporary.open("xb") as stream:
            stream.write(manifest_bytes)
            _flush_and_fsync(stream)
        _publish_exclusive(temporary, manifest_path, expected_bytes=manifest_bytes)
        _fsync_directory(manifest_path.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return validate_partition(manifest_path)


def _paths_for_validation(path: Path) -> tuple[Path, Path]:
    if path.name.endswith(".manifest.json"):
        manifest_path = path
        payload = _read_manifest_payload(manifest_path)
        data_file = payload.get("data_file")
        if not isinstance(data_file, str) or Path(data_file).name != data_file:
            raise PartitionValidationError("manifest data_file must be a plain file name")
        return manifest_path, manifest_path.parent / data_file
    if path.suffix == ".parquet":
        return path.with_name(f"{path.stem}.manifest.json"), path
    raise PartitionValidationError(f"expected a .parquet or .manifest.json path: {path.name}")


def _read_manifest_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise PartitionValidationError(f"manifest not found: {path.name}") from None
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PartitionValidationError(f"invalid manifest JSON {path.name}: {exc}") from None
    if not isinstance(payload, dict):
        raise PartitionValidationError(f"invalid manifest JSON {path.name}: root must be an object")
    return payload


def validate_partition(path: Path) -> PartitionManifest:
    """Validate manifest, layout, raw hash, Parquet schema and all recorded statistics."""

    manifest_path, data_path = _paths_for_validation(path)
    manifest = PartitionManifest.from_dict(_read_manifest_payload(manifest_path))
    if manifest_path.read_bytes() != _canonical_json(manifest.as_dict()):
        raise PartitionValidationError(f"manifest is not canonical: {manifest_path.name}")
    if manifest_path.name != manifest.manifest_file:
        raise PartitionValidationError(f"manifest file name mismatch: expected {manifest.manifest_file}")
    actual_key = PartitionKey.from_leaf(data_path.parent)
    if actual_key != manifest.partition:
        raise PartitionValidationError(
            f"manifest partition does not match physical layout for {data_path.name}"
        )
    if data_path.name != manifest.data_file:
        raise PartitionValidationError(f"manifest data_file mismatch for {data_path.name}")
    expected_data_file = f"part-{manifest.sha256}.parquet"
    if manifest.data_file != expected_data_file:
        raise PartitionValidationError(f"content-addressed file name mismatch: expected {expected_data_file}")
    if not data_path.is_file():
        raise PartitionValidationError(f"partition data file not found: {data_path.name}")

    try:
        spec = schema_for(actual_key.record_type, manifest.schema_version)
    except ValueError as exc:
        raise PartitionValidationError(str(exc)) from None
    if manifest.schema_name != spec.record_type.value:
        raise PartitionValidationError(f"schema name mismatch for {data_path.name}")
    if manifest.schema_fingerprint != schema_fingerprint(spec):
        raise PartitionValidationError(f"schema fingerprint mismatch for {data_path.name}")
    # Hash, size-check and decode one immutable byte payload. A concurrent replacement
    # after read_bytes cannot change the bytes passed to the Parquet decoder.
    table = _read_hashed_path(data_path, manifest)
    expected_interval_ns, stream_key = _stream_definition(
        table,
        actual_key,
        manifest.expected_interval_ns,
    )
    analysis = _analyze_table(
        table,
        actual_key,
        spec,
        expected_interval_ns=expected_interval_ns,
    )
    candidate = _manifest_for(
        actual_key,
        spec,
        analysis,
        data_file=data_path.name,
        sha256=manifest.sha256,
        size_bytes=manifest.size_bytes,
        row_count=table.num_rows,
        expected_interval_ns=expected_interval_ns,
        stream_key=stream_key,
    )
    expected = manifest.as_dict()
    observed = candidate.as_dict()
    for field in sorted(expected):
        if expected[field] != observed[field]:
            raise PartitionValidationError(f"manifest statistics mismatch for {data_path.name}: {field}")
    return manifest


def _physical_partition_date(root: Path, path: Path) -> Date | None:
    try:
        parts = path.relative_to(root).parts
    except ValueError:
        return None
    if len(parts) < 5:
        return None
    venue, partition_date, asset, record_type = parts[-5:-1]
    if not (
        venue.startswith("venue=")
        and partition_date.startswith("date=")
        and asset.startswith("asset=")
        and record_type.startswith("type=")
    ):
        return None
    try:
        return Date.fromisoformat(partition_date.removeprefix("date="))
    except ValueError:
        return None


def discover_partitions(
    root: Path,
    *,
    through_date: Date | str | None = None,
) -> tuple[Path, ...]:
    if not root.exists():
        return ()
    cutoff = None if through_date is None else _date_value(through_date)
    canonical_root = root.resolve()
    for artifact in root.rglob("*"):
        _require_under_root(canonical_root, artifact, label="discovered artifact")

    def selected(path: Path) -> bool:
        physical_date = _physical_partition_date(root, path)
        return cutoff is None or physical_date is None or physical_date <= cutoff

    parquet_files = tuple(
        path for path in sorted(root.rglob("*.parquet"), key=lambda path: path.as_posix()) if selected(path)
    )
    noncanonical = [path for path in parquet_files if _PARQUET_FILE.fullmatch(path.name) is None]
    if noncanonical:
        relative = noncanonical[0].relative_to(root).as_posix()
        raise PartitionValidationError(f"non-canonical Parquet file: {relative}")
    manifest_candidates = tuple(
        path
        for path in sorted(root.rglob("*.manifest.json"), key=lambda path: path.as_posix())
        if selected(path)
    )
    noncanonical_manifests = [
        path for path in manifest_candidates if _MANIFEST_FILE.fullmatch(path.name) is None
    ]
    if noncanonical_manifests:
        relative = noncanonical_manifests[0].relative_to(root).as_posix()
        raise PartitionValidationError(f"non-canonical manifest file: {relative}")
    manifests = manifest_candidates
    manifest_set = set(manifests)
    orphaned = [
        path for path in parquet_files if path.with_name(f"{path.stem}.manifest.json") not in manifest_set
    ]
    if orphaned:
        relative = orphaned[0].relative_to(root).as_posix()
        raise PartitionValidationError(f"orphan Parquet without manifest: {relative}")
    return manifests


def _delisted_assets(
    root: Path,
    manifests: tuple[PartitionManifest, ...],
    *,
    as_of: Date | None = None,
) -> tuple[str, ...]:
    start_of_day = None if as_of is None else datetime(as_of.year, as_of.month, as_of.day, tzinfo=UTC)
    cutoff_ns = (
        None
        if start_of_day is None
        else int((start_of_day + timedelta(days=1)).timestamp()) * 1_000_000_000 - 1
    )
    latest: dict[tuple[str, str, str], tuple[int, str]] = {}
    for manifest in manifests:
        if manifest.partition.record_type != RecordType.INSTRUMENT_LIFECYCLE:
            continue
        if as_of is not None and _date_value(manifest.partition.date) > as_of:
            continue
        table = read_hashed_table(
            root,
            manifest,
            columns=["venue", "asset", "instrument_id", "valid_from", "status"],
        )
        valid_from = pc.cast(table.column("valid_from"), pa.int64()).to_pylist()
        for venue, asset, instrument_id, timestamp, status in zip(
            table.column("venue").to_pylist(),
            table.column("asset").to_pylist(),
            table.column("instrument_id").to_pylist(),
            valid_from,
            table.column("status").to_pylist(),
            strict=True,
        ):
            if cutoff_ns is not None and int(timestamp) > cutoff_ns:
                continue
            key = (str(venue), str(asset), str(instrument_id))
            candidate = (int(timestamp), str(status).lower())
            if key not in latest or candidate[0] > latest[key][0]:
                latest[key] = candidate
    return tuple(
        sorted(
            {f"{venue}:{asset}" for (venue, asset, _), (_, status) in latest.items() if status == "delisted"}
        )
    )


def delisted_assets_as_of(
    root: Path,
    manifests: tuple[PartitionManifest, ...],
    date: Date,
) -> tuple[str, ...]:
    return _delisted_assets(root, manifests, as_of=date)


def _resync_events(
    root: Path,
    manifests: tuple[PartitionManifest, ...],
) -> dict[tuple[str, str, str | None, str | None], dict[str, list[int]]]:
    events: dict[tuple[str, str, str | None, str | None], dict[str, list[int]]] = {}
    for manifest in manifests:
        if manifest.partition.record_type != RecordType.CONNECTION_EVENT:
            continue
        table = read_hashed_table(
            root,
            manifest,
            columns=[
                "venue",
                "asset",
                "connection_id",
                "book_epoch_id",
                "event_kind",
                "event_time",
            ],
        )
        timestamps = pc.cast(table.column("event_time"), pa.int64()).to_pylist()
        for venue, asset, connection, epoch, kind, timestamp in zip(
            table.column("venue").to_pylist(),
            table.column("asset").to_pylist(),
            table.column("connection_id").to_pylist(),
            table.column("book_epoch_id").to_pylist(),
            table.column("event_kind").to_pylist(),
            timestamps,
            strict=True,
        ):
            normalized_kind = str(kind)
            if normalized_kind not in {"resync_start", "resync_complete"}:
                continue
            key = (
                str(venue),
                str(asset),
                None if epoch is None else str(epoch),
                None if connection is None else str(connection),
            )
            events.setdefault(key, {}).setdefault(normalized_kind, []).append(int(timestamp))
    return events


def _has_completed_resync(
    events: dict[tuple[str, str, str | None, str | None], dict[str, list[int]]],
    key: tuple[str, str, str | None, str | None],
    *,
    after_ns: int,
    before_ns: int,
) -> bool:
    matching = events.get(key, {})
    starts = matching.get("resync_start", [])
    completes = matching.get("resync_complete", [])
    return any(
        after_ns < start <= complete <= before_ns or after_ns < start <= before_ns <= complete
        for start in starts
        for complete in completes
    )


@dataclass(frozen=True, slots=True)
class _CrossSegmentRow:
    manifest: PartitionManifest
    order_key: tuple[tuple[int, object], ...]
    event_ns: int
    cadence_ns: int
    source_sequence: int | None
    connection_id: str | None
    wire_epoch: int | None
    arrival_sequence: int | None
    l2_identifier: str | None
    l2_epoch: str | None
    l2_first_sequence: int | None
    l2_last_sequence: int | None


def _schema_compatibility_family(record_type: RecordType | str, version: int) -> int:
    normalized = RecordType(_record_type_value(record_type))
    breaking_candidates = [
        candidate
        for (transition_type, _previous, candidate) in BREAKING_SCHEMA_TRANSITIONS
        if transition_type == normalized and candidate <= version
    ]
    return max(breaking_candidates, default=1)


def _cross_segment_gaps(
    root: Path,
    manifests: tuple[PartitionManifest, ...],
) -> tuple[tuple[PartitionKey, Gap], ...]:
    """Merge immutable sorted runs before validating dataset-wide invariants.

    A flush boundary is not a semantic ordering boundary: independently sorted Parquet
    segments may overlap or split one logical source message. Each segment has already
    been validated in isolation, so this pass performs a stable merge by the declared
    schema order key and keeps exact duplicate, cadence, sequence and L2 checks.
    """

    grouped: dict[
        tuple[str, str, RecordType | str, int, str],
        list[PartitionManifest],
    ] = {}
    for manifest in manifests:
        stream = (
            manifest.partition.venue,
            manifest.partition.asset,
            manifest.partition.record_type,
            _schema_compatibility_family(
                manifest.partition.record_type,
                manifest.schema_version,
            ),
            manifest.stream_key,
        )
        grouped.setdefault(stream, []).append(manifest)

    resyncs = _resync_events(root, manifests)
    result: list[tuple[PartitionKey, Gap]] = []
    primary_keys_by_dataset: dict[
        tuple[str, str, RecordType | str, int],
        set[tuple[object, ...]],
    ] = {}
    l2_metadata_by_dataset: dict[
        tuple[str, str, RecordType | str, int, str],
        tuple[object, ...],
    ] = {}
    for group in grouped.values():
        intervals = {manifest.expected_interval_ns for manifest in group}
        sample = group[0].partition
        if len(intervals) != 1:
            raise PartitionValidationError(
                "inconsistent gap cadence in stream: "
                f"{sample.venue}/{sample.asset}/{_record_type_value(sample.record_type)}"
            )
        expected_interval_ns = next(iter(intervals))
        compatibility_family = _schema_compatibility_family(
            sample.record_type,
            group[0].schema_version,
        )
        dataset_key = (
            sample.venue,
            sample.asset,
            sample.record_type,
            compatibility_family,
        )
        seen_primary_keys = primary_keys_by_dataset.setdefault(dataset_key, set())
        is_l2 = sample.record_type in {
            RecordType.L2_SNAPSHOT,
            RecordType.L2_DELTA,
        }
        l2_identifier_name = "update_id" if sample.record_type == RecordType.L2_DELTA else "snapshot_id"
        stream_rows: list[_CrossSegmentRow] = []

        for manifest in group:
            spec = schema_for(sample.record_type, manifest.schema_version)
            table = read_hashed_table(root, manifest)
            l2_definition = _L2_METADATA_DEFINITIONS.get(RecordType(_record_type_value(sample.record_type)))
            if l2_definition is not None:
                identifier_name, metadata_names, label = l2_definition
                identifiers = table.column(identifier_name).to_pylist()
                metadata_columns = [table.column(name).to_pylist() for name in metadata_names]
                for identifier, *metadata in zip(
                    identifiers,
                    *metadata_columns,
                    strict=True,
                ):
                    normalized_identifier = str(identifier)
                    metadata_key = (*dataset_key, normalized_identifier)
                    observed_metadata = tuple(metadata)
                    previous_metadata = l2_metadata_by_dataset.setdefault(
                        metadata_key,
                        observed_metadata,
                    )
                    if previous_metadata != observed_metadata:
                        raise PartitionValidationError(
                            f"inconsistent L2 {label} metadata for "
                            f"{identifier_name} {normalized_identifier!r} across partitions"
                        )

            primary_rows = _rows_for(table, spec.primary_key)
            order_rows = _rows_for(table, spec.order_key)
            event_ns = pc.cast(table.column("event_time"), pa.int64()).to_pylist()
            cadence_ns = (
                pc.cast(table.column("funding_time"), pa.int64()).to_pylist()
                if sample.record_type == RecordType.FUNDING
                else event_ns
            )
            sequences = table.column("source_sequence").to_pylist()
            connections = table.column("connection_id").to_pylist()
            wire_epochs = (
                table.column("connection_epoch").to_pylist()
                if sample.record_type == RecordType.WIRE_MESSAGE
                else [None] * table.num_rows
            )
            wire_arrivals = (
                table.column("arrival_sequence").to_pylist()
                if sample.record_type == RecordType.WIRE_MESSAGE
                else [None] * table.num_rows
            )
            l2_identifiers = (
                table.column(l2_identifier_name).to_pylist() if is_l2 else [None] * table.num_rows
            )
            l2_epochs = table.column("book_epoch_id").to_pylist() if is_l2 else [None] * table.num_rows
            l2_first = (
                table.column("first_sequence").to_pylist()
                if sample.record_type == RecordType.L2_DELTA
                else [None] * table.num_rows
            )
            l2_last = table.column("last_sequence").to_pylist() if is_l2 else [None] * table.num_rows

            for index in range(table.num_rows):
                primary_key = primary_rows[index]
                if primary_key in seen_primary_keys:
                    raise PartitionValidationError("duplicate primary keys: 1")
                seen_primary_keys.add(primary_key)
                raw_sequence = sequences[index]
                raw_wire_epoch = wire_epochs[index]
                raw_arrival_sequence = wire_arrivals[index]
                raw_l2_first = l2_first[index]
                raw_l2_last = l2_last[index]
                stream_rows.append(
                    _CrossSegmentRow(
                        manifest=manifest,
                        order_key=_normalized_row(order_rows[index]),
                        event_ns=int(event_ns[index]),
                        cadence_ns=int(cadence_ns[index]),
                        source_sequence=(None if raw_sequence is None else int(raw_sequence)),
                        connection_id=(None if connections[index] is None else str(connections[index])),
                        wire_epoch=(None if raw_wire_epoch is None else int(raw_wire_epoch)),
                        arrival_sequence=(
                            None if raw_arrival_sequence is None else int(raw_arrival_sequence)
                        ),
                        l2_identifier=(None if l2_identifiers[index] is None else str(l2_identifiers[index])),
                        l2_epoch=(None if l2_epochs[index] is None else str(l2_epochs[index])),
                        l2_first_sequence=(None if raw_l2_first is None else int(str(raw_l2_first))),
                        l2_last_sequence=(None if raw_l2_last is None else int(str(raw_l2_last))),
                    )
                )

        stream_rows.sort(key=lambda item: item.order_key)
        previous_cadence: tuple[int, int, str] | None = None
        previous_sequences: dict[str | None, tuple[int, str]] = {}
        previous_arrival_sequences: dict[tuple[str, int], tuple[int, str]] = {}
        previous_l2: tuple[str, str | None, str | None, int | None, int] | None = None

        for row in stream_rows:
            manifest_id = row.manifest.data_file
            if expected_interval_ns is None:
                current_cadence_value = row.cadence_ns
                cadence_step: int | None = None
            elif sample.record_type == RecordType.FUNDING:
                current_cadence_value = _funding_bucket(
                    row.cadence_ns,
                    expected_interval_ns,
                )
                cadence_step = 1
            else:
                current_cadence_value = row.cadence_ns
                cadence_step = expected_interval_ns
            if previous_cadence is not None and cadence_step is not None:
                previous_value, previous_time_ns, previous_manifest = previous_cadence
                difference = current_cadence_value - previous_value
                if difference > cadence_step and previous_manifest != manifest_id:
                    is_funding = sample.record_type == RecordType.FUNDING
                    result.append(
                        (
                            row.manifest.partition,
                            Gap(
                                kind="funding_bucket" if is_funding else "time",
                                start=_timestamp_iso(previous_time_ns),
                                end=_timestamp_iso(row.cadence_ns),
                                missing_count=(
                                    difference - 1 if is_funding else max(difference // cadence_step - 1, 1)
                                ),
                            ),
                        )
                    )
            previous_cadence = (
                current_cadence_value,
                row.cadence_ns,
                manifest_id,
            )

            if row.source_sequence is not None:
                sequence = row.source_sequence
                prior = previous_sequences.get(row.connection_id)
                if prior is not None and prior[1] != manifest_id:
                    prior_sequence = prior[0]
                    if sequence > prior_sequence + 1:
                        result.append(
                            (
                                row.manifest.partition,
                                Gap(
                                    kind="sequence",
                                    start=str(prior_sequence),
                                    end=str(sequence),
                                    missing_count=sequence - prior_sequence - 1,
                                    connection_id=row.connection_id,
                                ),
                            )
                        )
                    elif sequence < prior_sequence:
                        result.append(
                            (
                                row.manifest.partition,
                                Gap(
                                    kind="sequence_regression",
                                    start=str(prior_sequence),
                                    end=str(sequence),
                                    missing_count=0,
                                    connection_id=row.connection_id,
                                ),
                            )
                        )
                previous_sequences[row.connection_id] = (sequence, manifest_id)

            if sample.record_type == RecordType.WIRE_MESSAGE:
                assert row.connection_id is not None
                assert row.wire_epoch is not None
                assert row.arrival_sequence is not None
                arrival_key = (row.connection_id, row.wire_epoch)
                prior_arrival = previous_arrival_sequences.get(arrival_key)
                if prior_arrival is not None and prior_arrival[1] != manifest_id:
                    prior_sequence = prior_arrival[0]
                    if row.arrival_sequence > prior_sequence + 1:
                        result.append(
                            (
                                row.manifest.partition,
                                Gap(
                                    kind="arrival_sequence",
                                    start=str(prior_sequence),
                                    end=str(row.arrival_sequence),
                                    missing_count=row.arrival_sequence - prior_sequence - 1,
                                    connection_id=row.connection_id,
                                ),
                            )
                        )
                    elif row.arrival_sequence < prior_sequence:
                        result.append(
                            (
                                row.manifest.partition,
                                Gap(
                                    kind="arrival_sequence_regression",
                                    start=str(prior_sequence),
                                    end=str(row.arrival_sequence),
                                    missing_count=0,
                                    connection_id=row.connection_id,
                                ),
                            )
                        )
                previous_arrival_sequences[arrival_key] = (
                    row.arrival_sequence,
                    manifest_id,
                )

            if not is_l2:
                continue
            assert row.l2_identifier is not None
            if previous_l2 is not None and row.l2_identifier != previous_l2[0]:
                (
                    prior_identifier,
                    prior_epoch,
                    prior_connection,
                    prior_last,
                    prior_event_ns,
                ) = previous_l2
                reset = (
                    sample.record_type == RecordType.L2_DELTA
                    and row.l2_first_sequence is not None
                    and prior_last is not None
                    and row.l2_first_sequence <= prior_last
                )
                transitioned = row.l2_epoch != prior_epoch or row.connection_id != prior_connection or reset
                resync_key = (
                    sample.venue,
                    sample.asset,
                    row.l2_epoch,
                    row.connection_id,
                )
                if transitioned and not _has_completed_resync(
                    resyncs,
                    resync_key,
                    after_ns=prior_event_ns,
                    before_ns=row.event_ns,
                ):
                    if sample.record_type == RecordType.L2_SNAPSHOT:
                        prior_cursor = f"snapshot={prior_identifier}"
                        current_cursor = f"snapshot={row.l2_identifier}"
                    else:
                        prior_cursor = f"sequence={prior_last}"
                        current_cursor = f"sequence={row.l2_first_sequence}"
                    result.append(
                        (
                            row.manifest.partition,
                            Gap(
                                kind="l2_resync_missing",
                                start=(f"epoch={prior_epoch},connection={prior_connection},{prior_cursor}"),
                                end=(f"epoch={row.l2_epoch},connection={row.connection_id},{current_cursor}"),
                                missing_count=0,
                                connection_id=row.connection_id,
                            ),
                        )
                    )
            previous_l2 = (
                row.l2_identifier,
                row.l2_epoch,
                row.connection_id,
                row.l2_last_sequence,
                row.event_ns,
            )

    return tuple(
        sorted(
            result,
            key=lambda item: (
                item[0].relative_path.as_posix(),
                item[1].kind,
                item[1].start,
                item[1].end,
            ),
        )
    )


def inventory_partitions(
    root: Path,
    *,
    through_date: Date | str | None = None,
) -> InventoryReport:
    validated: list[tuple[PartitionManifest, Path]] = []
    canonical_root = root.resolve()
    for path in discover_partitions(root, through_date=through_date):
        manifest = validate_partition(path)
        _require_under_root(canonical_root, path, label="partition manifest")
        _require_under_root(
            canonical_root,
            path.parent / manifest.data_file,
            label="partition data",
        )
        expected_path = (canonical_root / manifest.relative_manifest_path).resolve()
        if path.resolve() != expected_path:
            raise PartitionValidationError(
                f"manifest is outside canonical root layout: {path.relative_to(root).as_posix()}"
            )
        validated.append((manifest, path))
    manifests = tuple(
        manifest
        for manifest, _ in sorted(
            validated,
            key=lambda item: item[0].relative_data_path.as_posix(),
        )
    )
    return InventoryReport(
        partitions=manifests,
        total_rows=sum(manifest.row_count for manifest in manifests),
        venues=tuple(sorted({manifest.partition.venue for manifest in manifests})),
        assets=tuple(sorted({manifest.partition.asset for manifest in manifests})),
        record_types=tuple(
            sorted({_record_type_value(manifest.partition.record_type) for manifest in manifests})
        ),
        dates=tuple(sorted({_date_value(manifest.partition.date).isoformat() for manifest in manifests})),
        delisted_assets=_delisted_assets(root, manifests),
        cross_segment_gaps=_cross_segment_gaps(root, manifests),
    )
