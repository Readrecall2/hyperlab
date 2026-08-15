from __future__ import annotations

import hashlib
import json
import math
import os
import pickle
import re
import shutil
import sqlite3
import tempfile
import time
from bisect import bisect_right
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import TypeAlias, cast

from hyperlab.analysis.gate_binding import (
    SavedPhase10Gate,
    canonical_json_bytes,
    compare_saved_and_fresh_phase10_gate,
    load_saved_phase10_gate,
    verify_saved_phase10_gate_unchanged,
)
from hyperlab.analysis.lead_lag import StrictInterval
from hyperlab.analysis.streaming_store import (
    SourceRowSpool,
    StreamingStoreError,
    timestamp_ns,
)
from hyperlab.data.continuity import audit_phase10_continuity
from hyperlab.data.lake import (
    PartitionManifest,
    PartitionValidationError,
    iter_hashed_batches,
)
from hyperlab.data.schema import RecordType, schema_fingerprint, schema_for

_ASSETS = frozenset({"BTC", "ETH"})
_VENUES = frozenset({"binance_usdm", "hyperliquid"})
_MARKET_TYPES = frozenset(
    {
        RecordType.BBO,
        RecordType.TRADE,
        RecordType.L2_BOOK_STATE,
        RecordType.L2_SNAPSHOT,
    }
)
_SELECTED_TYPES = frozenset({*_MARKET_TYPES, RecordType.CLOCK_SYNC})
_PARQUET_FILE = re.compile(r"^part-[0-9a-f]{64}\.parquet$")
_MANIFEST_FILE = re.compile(r"^part-[0-9a-f]{64}\.manifest\.json$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MAX_MANIFEST_BYTES = 4 * 1024 * 1024
_MAX_DISCOVERY_DEPTH = 32
_DEFAULT_BATCH_ROWS = 4_096
_DEFAULT_MAX_L2_ROWS_PER_RECEIVE = 100_000
_SQLITE_CACHE_KIB = 32 * 1024
_HASH_BLOCK_BYTES = 1024 * 1024
_SOURCE_SPOOL_COMPRESSED_SIZE_MULTIPLIER = 8
_SOURCE_SPOOL_BYTES_PER_SELECTED_ROW = 2_048

_REQUIRED_PROJECTION_COLUMNS: Mapping[RecordType, frozenset[str]] = {
    RecordType.BBO: frozenset(
        {
            "record_type",
            "venue",
            "asset",
            "received_time",
            "source_sequence",
            "connection_id",
            "update_id",
            "bid_price",
            "bid_quantity",
            "ask_price",
            "ask_quantity",
        }
    ),
    RecordType.TRADE: frozenset(
        {
            "record_type",
            "venue",
            "asset",
            "received_time",
            "source_sequence",
            "connection_id",
            "trade_id",
            "aggressor_side",
            "price",
            "quantity",
            "quote_quantity",
        }
    ),
    RecordType.L2_BOOK_STATE: frozenset(
        {
            "record_type",
            "venue",
            "asset",
            "event_time",
            "exchange_time",
            "received_time",
            "source_sequence",
            "connection_id",
            "snapshot_id",
            "book_epoch_id",
            "bid_level_count",
            "ask_level_count",
        }
    ),
    RecordType.L2_SNAPSHOT: frozenset(
        {
            "record_type",
            "venue",
            "asset",
            "event_time",
            "exchange_time",
            "received_time",
            "source_sequence",
            "connection_id",
            "snapshot_id",
            "book_epoch_id",
            "last_sequence",
            "side",
            "level",
            "price",
            "quantity",
            "order_count",
        }
    ),
    RecordType.CLOCK_SYNC: frozenset(
        {
            "record_type",
            "venue",
            "asset",
            "received_time",
            "causal_valid_from",
            "causal_valid_until",
            "sample_status",
        }
    ),
}
_OPTIONAL_PROJECTION_COLUMNS: Mapping[RecordType, frozenset[str]] = {
    RecordType.TRADE: frozenset({"arrival_sequence"}),
}

SqlValue: TypeAlias = int | float | str | bytes | None
_ADMISSION_BINDING_TOKEN = object()


class StreamingLakeValidationError(ValueError):
    """Raised when bounded Phase 10 input admission cannot be trusted."""


@dataclass(frozen=True, slots=True)
class BoundedLeadLagGateAdmission:
    """Small, manifest-free admission produced by the saved/fresh gate check."""

    root: Path
    start: datetime
    end: datetime
    assets: tuple[str, ...]
    intervals: tuple[StrictInterval, ...]
    saved_gate: SavedPhase10Gate
    gate_report_sha256: str
    semantic_gate_sha256: str
    semantic_gate_canonicalizer_version: str
    excluded_json_pointers: tuple[str, ...]
    clock_lookback: timedelta = field(repr=False)
    _binding_token: object = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class BoundedLeadLagWindow:
    """Bounded source descriptor and its disk-backed causal row spool."""

    root: Path
    start: datetime
    end: datetime
    assets: tuple[str, ...]
    intervals: tuple[StrictInterval, ...]
    saved_gate: SavedPhase10Gate
    gate_report_sha256: str
    semantic_gate_sha256: str
    semantic_gate_canonicalizer_version: str
    excluded_json_pointers: tuple[str, ...]
    manifest_fingerprint: str
    selected_manifests_sha256: str
    selected_manifests_count: int
    source_spool: SourceRowSpool
    observability: Mapping[str, object]
    _catalog_path: Path = field(repr=False)
    _selected_manifests_path: Path = field(repr=False)
    _inventory_manifest_fingerprint: str = field(repr=False)
    _inventory_partition_count: int = field(repr=False)
    _inventory_row_count: int = field(repr=False)
    _clock_lookback: timedelta = field(repr=False)

    @property
    def selected_manifests_path(self) -> Path:
        return self._selected_manifests_path

    @property
    def selected_manifest_count(self) -> int:
        """Legacy publication field name for the streamed JSONL line count."""

        return self.selected_manifests_count

    def close(self) -> None:
        self.source_spool.close()

    def __enter__(self) -> BoundedLeadLagWindow:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()


@dataclass(slots=True)
class _ClockDiagnostics:
    row_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0
    causally_usable_count: int = 0
    min_received_ns: int | None = None
    max_received_ns: int | None = None
    usable_min_received_ns: int | None = None
    usable_max_received_ns: int | None = None

    def observe(
        self,
        received_ns: int,
        *,
        sample_valid: bool,
        causally_usable: bool,
    ) -> None:
        self.row_count += 1
        self.min_received_ns = (
            received_ns if self.min_received_ns is None else min(self.min_received_ns, received_ns)
        )
        self.max_received_ns = (
            received_ns if self.max_received_ns is None else max(self.max_received_ns, received_ns)
        )
        if sample_valid:
            self.valid_count += 1
        else:
            self.invalid_count += 1
        if causally_usable:
            self.causally_usable_count += 1
            self.usable_min_received_ns = (
                received_ns
                if self.usable_min_received_ns is None
                else min(self.usable_min_received_ns, received_ns)
            )
            self.usable_max_received_ns = (
                received_ns
                if self.usable_max_received_ns is None
                else max(self.usable_max_received_ns, received_ns)
            )

    def legacy_dict(self) -> dict[str, object]:
        return {
            "row_count": self.causally_usable_count,
            "usage": "DIAGNOSTIC_ONLY_STRICT_INTERVALS_DEFINE_CAUSAL_VALIDITY",
            "received_time_min": _iso_from_ns(self.usable_min_received_ns),
            "received_time_max": _iso_from_ns(self.usable_max_received_ns),
        }

    def scan_dict(self) -> dict[str, object]:
        return {
            "row_count": self.row_count,
            "received_time_min": _iso_from_ns(self.min_received_ns),
            "received_time_max": _iso_from_ns(self.max_received_ns),
            "reported_valid_row_count": self.valid_count,
            "reported_invalid_row_count": self.invalid_count,
            "causally_usable_row_count": self.causally_usable_count,
        }


class _Catalog:
    def __init__(self, path: Path, *, create: bool = True) -> None:
        self.path = Path(path)
        if create and self.path.exists():
            raise StreamingLakeValidationError(f"refusing to overwrite bounded catalog: {self.path}")
        self.connection = sqlite3.connect(self.path)
        self.connection.execute("PRAGMA journal_mode=DELETE")
        self.connection.execute("PRAGMA synchronous=OFF")
        self.connection.execute("PRAGMA temp_store=FILE")
        self.connection.execute(f"PRAGMA cache_size=-{_SQLITE_CACHE_KIB}")
        self.connection.execute("PRAGMA mmap_size=0")
        self.connection.execute("PRAGMA foreign_keys=ON")
        if create:
            self.connection.executescript(
                """
                CREATE TABLE data_files (
                    relative_path TEXT PRIMARY KEY,
                    size_bytes INTEGER NOT NULL
                );
                CREATE TABLE manifests (
                    relative_manifest_path TEXT PRIMARY KEY,
                    relative_data_path TEXT NOT NULL UNIQUE,
                    manifest_bytes_sha256 TEXT NOT NULL,
                    entry_json BLOB NOT NULL,
                    venue TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    record_type TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    received_min_ns INTEGER NOT NULL,
                    received_max_ns INTEGER NOT NULL,
                    row_count INTEGER NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    selected INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX manifest_selection
                ON manifests(record_type, venue, asset, received_min_ns,
                             received_max_ns, relative_data_path);
                CREATE TABLE strict_intervals (
                    start_ns INTEGER NOT NULL,
                    end_ns INTEGER NOT NULL,
                    tag TEXT NOT NULL,
                    PRIMARY KEY(start_ns, end_ns, tag)
                );
                CREATE TABLE l2_source (
                    row_id INTEGER PRIMARY KEY,
                    record_type TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    received_ns INTEGER NOT NULL,
                    manifest_order INTEGER NOT NULL,
                    row_order INTEGER NOT NULL,
                    payload BLOB NOT NULL,
                    UNIQUE(record_type, manifest_order, row_order)
                );
                CREATE INDEX l2_source_frame
                ON l2_source(asset, received_ns, venue, record_type,
                             manifest_order, row_order);
                """
            )
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.connection.commit()
        finally:
            self.connection.close()
            self._closed = True


def _mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise StreamingLakeValidationError(f"{label} must be an object")
    return cast(Mapping[str, object], value)


def _integer(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise StreamingLakeValidationError(f"{label} must be an integer greater than or equal to {minimum}")
    return value


def _parse_utc(value: object, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise StreamingLakeValidationError(f"{label} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise StreamingLakeValidationError(f"{label} is not a valid timestamp") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise StreamingLakeValidationError(f"{label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _gate_window(
    saved: SavedPhase10Gate,
) -> tuple[
    datetime,
    datetime,
    tuple[str, ...],
    tuple[StrictInterval, ...],
    timedelta,
]:
    report = saved.report
    requested = _mapping(report.get("requested_window"), label="requested_window")
    start = _parse_utc(requested.get("start"), label="requested_window.start")
    end = _parse_utc(requested.get("end"), label="requested_window.end")
    raw_assets = report.get("assets")
    if not isinstance(raw_assets, list) or not all(isinstance(asset, str) for asset in raw_assets):
        raise StreamingLakeValidationError("assets must be a string array")
    assets = tuple(sorted(str(asset).strip().upper() for asset in raw_assets))
    if frozenset(assets) != _ASSETS or len(assets) != len(_ASSETS):
        raise StreamingLakeValidationError("gate assets must be exactly BTC and ETH")
    overlap = _mapping(report.get("strict_phase_10_overlap"), label="strict_phase_10_overlap")
    raw_intervals = overlap.get("intervals")
    if not isinstance(raw_intervals, list):
        raise StreamingLakeValidationError("strict_phase_10_overlap.intervals must be an array")
    intervals: list[StrictInterval] = []
    for index, raw in enumerate(raw_intervals):
        item = _mapping(raw, label=f"strict_phase_10_overlap.intervals[{index}]")
        tag = item.get("capture_epoch_id")
        if not isinstance(tag, str):
            raise StreamingLakeValidationError(
                f"strict_phase_10_overlap.intervals[{index}].capture_epoch_id must be a string"
            )
        intervals.append(
            StrictInterval(
                start=_parse_utc(
                    item.get("start"),
                    label=f"strict_phase_10_overlap.intervals[{index}].start",
                ),
                end=_parse_utc(
                    item.get("end"),
                    label=f"strict_phase_10_overlap.intervals[{index}].end",
                ),
                tag=tag,
            )
        )
    ordered = tuple(sorted(intervals))
    if not ordered:
        raise StreamingLakeValidationError("strict interval population is empty")
    clock = _mapping(report.get("clock_sync"), label="clock_sync")
    strict_max_age_ms = clock.get("strict_max_age_ms")
    if isinstance(strict_max_age_ms, bool) or not isinstance(strict_max_age_ms, (int, float)):
        raise StreamingLakeValidationError("clock_sync.strict_max_age_ms must be numeric")
    if not math.isfinite(float(strict_max_age_ms)) or float(strict_max_age_ms) <= 0:
        raise StreamingLakeValidationError("clock_sync.strict_max_age_ms must be positive and finite")
    return start, end, assets, ordered, timedelta(milliseconds=float(strict_max_age_ms))


def _build_admission(
    *,
    root: Path,
    start: datetime,
    end: datetime,
    assets: tuple[str, ...],
    intervals: tuple[StrictInterval, ...],
    saved_gate: SavedPhase10Gate,
    gate_report_sha256: str,
    semantic_gate_sha256: str,
    semantic_gate_canonicalizer_version: str,
    excluded_json_pointers: tuple[str, ...],
    clock_lookback: timedelta,
) -> BoundedLeadLagGateAdmission:
    return BoundedLeadLagGateAdmission(
        root=root,
        start=start,
        end=end,
        assets=assets,
        intervals=intervals,
        saved_gate=saved_gate,
        gate_report_sha256=gate_report_sha256,
        semantic_gate_sha256=semantic_gate_sha256,
        semantic_gate_canonicalizer_version=semantic_gate_canonicalizer_version,
        excluded_json_pointers=excluded_json_pointers,
        clock_lookback=clock_lookback,
        _binding_token=_ADMISSION_BINDING_TOKEN,
    )


def validate_bounded_lead_lag_gate(
    root: Path,
    gate_report_path: Path,
) -> BoundedLeadLagGateAdmission:
    """Revalidate semantic PASS before any analysis-owned manifest staging."""

    requested_root = Path(root)
    if not requested_root.is_dir():
        raise StreamingLakeValidationError(f"lake root is not a directory: {requested_root}")
    canonical_root = requested_root.resolve()
    saved = load_saved_phase10_gate(Path(gate_report_path))
    start, end, assets, intervals, clock_lookback = _gate_window(saved)
    fresh = audit_phase10_continuity(
        canonical_root,
        assets=assets,
        start=start,
        end=end,
    )
    compare_saved_and_fresh_phase10_gate(saved, fresh)
    return _build_admission(
        root=canonical_root,
        start=start,
        end=end,
        assets=assets,
        intervals=intervals,
        saved_gate=saved,
        gate_report_sha256=saved.gate_report_sha256,
        semantic_gate_sha256=saved.semantic_gate_sha256,
        semantic_gate_canonicalizer_version=saved.canonicalizer_version,
        excluded_json_pointers=saved.excluded_json_pointers,
        clock_lookback=clock_lookback,
    )


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _prepare_output_paths(
    admission: BoundedLeadLagGateAdmission,
    scratch_dir: Path,
    selected_manifests_path: Path,
) -> tuple[Path, Path, Path]:
    scratch = Path(scratch_dir).resolve()
    selected = Path(selected_manifests_path).resolve()
    if _is_within(scratch, admission.root) or _is_within(selected, admission.root):
        raise StreamingLakeValidationError(
            "analysis scratch and selected-manifest evidence must be outside the lake"
        )
    scratch.mkdir(parents=True, exist_ok=True)
    selected.parent.mkdir(parents=True, exist_ok=True)
    if not scratch.is_dir():
        raise StreamingLakeValidationError(f"scratch path is not a directory: {scratch}")
    catalog_path = scratch / "bounded-lead-lag-catalog.sqlite3"
    source_path = scratch / "bounded-lead-lag-source.sqlite3"
    for path in (catalog_path, source_path, selected):
        if path.exists():
            raise StreamingLakeValidationError(f"refusing to overwrite bounded staging artifact: {path}")
    return catalog_path, source_path, selected


def _canonical_manifest_bytes(manifest: PartitionManifest) -> bytes:
    return canonical_json_bytes(manifest.as_dict()) + b"\n"


def _decode_manifest(payload: bytes, *, relative_path: str) -> PartitionManifest:
    if not payload:
        raise StreamingLakeValidationError(f"empty manifest: {relative_path}")
    if len(payload) > _MAX_MANIFEST_BYTES:
        raise StreamingLakeValidationError(f"manifest exceeds bounded byte limit: {relative_path}")

    def reject_constant(value: str) -> object:
        raise StreamingLakeValidationError(f"manifest contains non-finite JSON {value}: {relative_path}")

    try:
        decoded = json.loads(payload.decode("utf-8"), parse_constant=reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StreamingLakeValidationError(f"invalid UTF-8 JSON manifest {relative_path}: {exc}") from None
    if not isinstance(decoded, dict):
        raise StreamingLakeValidationError(f"manifest is not an object: {relative_path}")
    try:
        manifest = PartitionManifest.from_dict(decoded)
    except (PartitionValidationError, TypeError, ValueError) as exc:
        raise StreamingLakeValidationError(f"invalid manifest {relative_path}: {exc}") from None
    if payload != _canonical_manifest_bytes(manifest):
        raise StreamingLakeValidationError(f"manifest is not exact canonical JSON: {relative_path}")
    return manifest


def _manifest_bounds(manifest: PartitionManifest) -> tuple[int, int]:
    received = manifest.timestamp_bounds.get("received_time")
    if not isinstance(received, dict):
        raise StreamingLakeValidationError(
            f"manifest lacks received_time bounds: {manifest.relative_manifest_path.as_posix()}"
        )
    minimum = received.get("min")
    maximum = received.get("max")
    if minimum is None or maximum is None:
        raise StreamingLakeValidationError(
            f"manifest has null received_time bounds: {manifest.relative_manifest_path.as_posix()}"
        )
    minimum_ns = timestamp_ns(minimum, label="manifest received_time.min")
    maximum_ns = timestamp_ns(maximum, label="manifest received_time.max")
    if maximum_ns < minimum_ns:
        raise StreamingLakeValidationError(
            f"manifest has reversed received_time bounds: {manifest.relative_manifest_path.as_posix()}"
        )
    return minimum_ns, maximum_ns


def _validate_manifest_identity(
    manifest: PartitionManifest,
    *,
    relative_manifest_path: str,
) -> tuple[int, int, bytes]:
    expected_manifest_path = manifest.relative_manifest_path.as_posix()
    if relative_manifest_path != expected_manifest_path:
        raise StreamingLakeValidationError(
            "manifest partition/path mismatch: "
            f"expected={expected_manifest_path} actual={relative_manifest_path}"
        )
    if not _SHA256.fullmatch(manifest.sha256):
        raise StreamingLakeValidationError(f"manifest has invalid data SHA-256: {relative_manifest_path}")
    if manifest.data_file != f"part-{manifest.sha256}.parquet":
        raise StreamingLakeValidationError(
            f"manifest data filename is not content-addressed: {relative_manifest_path}"
        )
    if manifest.row_count <= 0 or manifest.size_bytes <= 0:
        raise StreamingLakeValidationError(
            f"manifest row_count and size_bytes must be positive: {relative_manifest_path}"
        )
    try:
        spec = schema_for(manifest.partition.record_type, manifest.schema_version)
    except ValueError as exc:
        raise StreamingLakeValidationError(
            f"manifest references an unknown schema: {relative_manifest_path}: {exc}"
        ) from None
    record_type = RecordType(manifest.partition.record_type)
    if manifest.schema_name != record_type.value:
        raise StreamingLakeValidationError(
            f"manifest schema_name does not match partition: {relative_manifest_path}"
        )
    if manifest.schema_fingerprint != schema_fingerprint(spec):
        raise StreamingLakeValidationError(f"manifest schema fingerprint mismatch: {relative_manifest_path}")
    minimum_ns, maximum_ns = _manifest_bounds(manifest)
    entry = manifest.as_dict()
    entry["relative_data_path"] = manifest.relative_data_path.as_posix()
    entry["relative_manifest_path"] = relative_manifest_path
    return minimum_ns, maximum_ns, canonical_json_bytes(entry)


def _safe_relative(root: Path, path: Path) -> str:
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(root)
    except ValueError:
        raise StreamingLakeValidationError(
            f"lake artifact resolves outside the canonical root: {path}"
        ) from None
    if resolved != root / relative:
        raise StreamingLakeValidationError(f"non-canonical lake artifact path: {path}")
    return relative.as_posix()


def _iter_lake_files(root: Path) -> Iterator[tuple[Path, str]]:
    stack: list[tuple[Iterator[os.DirEntry[str]], int]] = []
    first = os.scandir(root)
    stack.append((first, 0))
    try:
        while stack:
            iterator, depth = stack[-1]
            try:
                entry = next(iterator)
            except StopIteration:
                close = getattr(iterator, "close", None)
                if callable(close):
                    close()
                stack.pop()
                continue
            path = Path(entry.path)
            try:
                if entry.is_symlink():
                    raise StreamingLakeValidationError(
                        f"symbolic links are not permitted in the lake: {path}"
                    )
                if entry.is_dir(follow_symlinks=False):
                    if depth >= _MAX_DISCOVERY_DEPTH:
                        raise StreamingLakeValidationError(
                            f"lake discovery exceeds {_MAX_DISCOVERY_DEPTH} directories: {path}"
                        )
                    _safe_relative(root, path)
                    stack.append((os.scandir(path), depth + 1))
                    continue
                if entry.is_file(follow_symlinks=False):
                    yield path, _safe_relative(root, path)
            except OSError as exc:
                raise StreamingLakeValidationError(
                    f"lake artifact could not be inspected: {path}: {exc}"
                ) from None
    finally:
        for iterator, _depth in reversed(stack):
            close = getattr(iterator, "close", None)
            if callable(close):
                close()


def _read_manifest_bounded(path: Path, *, expected_size: int) -> bytes:
    if expected_size > _MAX_MANIFEST_BYTES:
        raise StreamingLakeValidationError(f"manifest exceeds bounded byte limit: {path}")
    try:
        with path.open("rb") as stream:
            payload = stream.read(_MAX_MANIFEST_BYTES + 1)
    except OSError as exc:
        raise StreamingLakeValidationError(f"manifest could not be read: {path}: {exc}") from None
    if len(payload) != expected_size:
        raise StreamingLakeValidationError(f"manifest size changed while reading: {path}")
    return payload


def _insert_manifest(
    catalog: _Catalog,
    path: Path,
    relative_path: str,
    *,
    size_bytes: int,
) -> None:
    payload = _read_manifest_bounded(path, expected_size=size_bytes)
    manifest = _decode_manifest(payload, relative_path=relative_path)
    minimum_ns, maximum_ns, entry_bytes = _validate_manifest_identity(
        manifest,
        relative_manifest_path=relative_path,
    )
    record_type = RecordType(manifest.partition.record_type)
    try:
        catalog.connection.execute(
            """
            INSERT INTO manifests(
                relative_manifest_path, relative_data_path,
                manifest_bytes_sha256, entry_json, venue, asset,
                record_type, schema_version, received_min_ns,
                received_max_ns, row_count, size_bytes, sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                relative_path,
                manifest.relative_data_path.as_posix(),
                hashlib.sha256(payload).hexdigest(),
                entry_bytes,
                manifest.partition.venue,
                manifest.partition.asset,
                record_type.value,
                manifest.schema_version,
                minimum_ns,
                maximum_ns,
                manifest.row_count,
                manifest.size_bytes,
                manifest.sha256,
            ),
        )
    except sqlite3.IntegrityError as exc:
        raise StreamingLakeValidationError(
            f"duplicate or conflicting manifest evidence: {relative_path}: {exc}"
        ) from None


def _catalog_lake(root: Path, catalog: _Catalog) -> tuple[int, int, int]:
    files_discovered = 0
    manifest_files = 0
    parquet_files = 0
    for path, relative_path in _iter_lake_files(root):
        files_discovered += 1
        name = path.name
        try:
            size_bytes = path.stat().st_size
        except OSError as exc:
            raise StreamingLakeValidationError(f"lake artifact could not be stated: {path}: {exc}") from None
        if name.endswith(".parquet"):
            if _PARQUET_FILE.fullmatch(name) is None:
                raise StreamingLakeValidationError(f"non-canonical Parquet file: {relative_path}")
            parquet_files += 1
            catalog.connection.execute(
                "INSERT INTO data_files(relative_path, size_bytes) VALUES (?, ?)",
                (relative_path, size_bytes),
            )
        elif name.endswith(".manifest.json"):
            if _MANIFEST_FILE.fullmatch(name) is None:
                raise StreamingLakeValidationError(f"non-canonical manifest file: {relative_path}")
            manifest_files += 1
            _insert_manifest(
                catalog,
                path,
                relative_path,
                size_bytes=size_bytes,
            )
    catalog.connection.commit()
    return files_discovered, manifest_files, parquet_files


def _single_text(catalog: _Catalog, query: str) -> str | None:
    row = catalog.connection.execute(query).fetchone()
    return None if row is None else str(row[0])


def _validate_catalog_inventory(
    catalog: _Catalog,
    saved: SavedPhase10Gate,
) -> tuple[int, int]:
    orphan = _single_text(
        catalog,
        """
        SELECT d.relative_path FROM data_files AS d
        LEFT JOIN manifests AS m ON m.relative_data_path = d.relative_path
        WHERE m.relative_data_path IS NULL
        ORDER BY d.relative_path LIMIT 1
        """,
    )
    if orphan is not None:
        raise StreamingLakeValidationError(f"orphan Parquet without manifest: {orphan}")
    missing = _single_text(
        catalog,
        """
        SELECT m.relative_data_path FROM manifests AS m
        LEFT JOIN data_files AS d ON d.relative_path = m.relative_data_path
        WHERE d.relative_path IS NULL
        ORDER BY m.relative_data_path LIMIT 1
        """,
    )
    if missing is not None:
        raise StreamingLakeValidationError(f"manifest data file is missing: {missing}")
    size_mismatch = _single_text(
        catalog,
        """
        SELECT m.relative_data_path FROM manifests AS m
        JOIN data_files AS d ON d.relative_path = m.relative_data_path
        WHERE d.size_bytes != m.size_bytes
        ORDER BY m.relative_data_path LIMIT 1
        """,
    )
    if size_mismatch is not None:
        raise StreamingLakeValidationError(f"manifest data size mismatch: {size_mismatch}")
    count_row = catalog.connection.execute(
        "SELECT COUNT(*), COALESCE(SUM(row_count), 0) FROM manifests"
    ).fetchone()
    if count_row is None:
        raise StreamingLakeValidationError("catalog inventory count disappeared")
    partition_count, row_count = int(count_row[0]), int(count_row[1])
    validation = _mapping(saved.report.get("validation"), label="validation")
    expected_partitions = _integer(
        validation.get("inventory_partition_count"),
        label="validation.inventory_partition_count",
    )
    expected_rows = _integer(
        validation.get("inventory_row_count"),
        label="validation.inventory_row_count",
    )
    if partition_count != expected_partitions or row_count != expected_rows:
        raise StreamingLakeValidationError(
            "lake inventory changed after the saved/fresh continuity gate: "
            f"expected_partitions={expected_partitions} actual_partitions={partition_count} "
            f"expected_rows={expected_rows} actual_rows={row_count}"
        )
    return partition_count, row_count


def _insert_intervals(
    catalog: _Catalog,
    intervals: Sequence[StrictInterval],
) -> None:
    catalog.connection.executemany(
        "INSERT INTO strict_intervals(start_ns, end_ns, tag) VALUES (?, ?, ?)",
        [
            (
                timestamp_ns(item.start, label="strict interval start"),
                timestamp_ns(item.end, label="strict interval end"),
                item.tag,
            )
            for item in intervals
        ],
    )


def _mark_selected(
    catalog: _Catalog,
    admission: BoundedLeadLagGateAdmission,
) -> None:
    _insert_intervals(catalog, admission.intervals)
    lookback_ns = int(admission.clock_lookback.total_seconds() * 1_000_000_000)
    market_placeholders = ",".join("?" for _ in _MARKET_TYPES)
    venue_placeholders = ",".join("?" for _ in _VENUES)
    asset_placeholders = ",".join("?" for _ in admission.assets)
    parameters: list[SqlValue] = [
        *(item.value for item in sorted(_MARKET_TYPES, key=lambda item: item.value)),
        *sorted(_VENUES),
        *admission.assets,
        RecordType.CLOCK_SYNC.value,
        *sorted(_VENUES),
        lookback_ns,
    ]
    catalog.connection.execute(
        f"""
        UPDATE manifests AS m SET selected = 1
        WHERE (
            m.record_type IN ({market_placeholders})
            AND m.venue IN ({venue_placeholders})
            AND m.asset IN ({asset_placeholders})
            AND EXISTS (
                SELECT 1 FROM strict_intervals AS i
                WHERE m.received_max_ns >= i.start_ns
                  AND m.received_min_ns < i.end_ns
            )
        ) OR (
            m.record_type = ?
            AND m.venue IN ({venue_placeholders})
            AND m.asset = 'GLOBAL'
            AND m.schema_version >= 2
            AND EXISTS (
                SELECT 1 FROM strict_intervals AS i
                WHERE m.received_max_ns >= i.start_ns - ?
                  AND m.received_min_ns < i.end_ns
            )
        )
        """,
        parameters,
    )
    catalog.connection.commit()


def _inventory_manifest_fingerprint(catalog: _Catalog) -> str:
    digest = hashlib.sha256()
    digest.update(b"[")
    first = True
    cursor = catalog.connection.execute(
        """
        SELECT relative_manifest_path, manifest_bytes_sha256, entry_json
        FROM manifests ORDER BY relative_manifest_path
        """
    )
    for relative_path, manifest_hash, entry_json in cursor:
        if not first:
            digest.update(b",")
        first = False
        digest.update(
            canonical_json_bytes(
                {
                    "relative_manifest_path": str(relative_path),
                    "manifest_bytes_sha256": str(manifest_hash),
                    "entry": json.loads(bytes(entry_json)),
                }
            )
        )
    digest.update(b"]")
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class _SelectedEvidence:
    manifest_fingerprint: str
    jsonl_sha256: str
    count: int


def _write_selected_manifest_evidence(
    catalog: _Catalog,
    selected_path: Path,
) -> _SelectedEvidence:
    array_digest = hashlib.sha256()
    jsonl_digest = hashlib.sha256()
    array_digest.update(b"[")
    count = 0
    try:
        with selected_path.open("xb") as stream:
            cursor = catalog.connection.execute(
                """
                SELECT entry_json FROM manifests
                WHERE selected = 1 ORDER BY relative_data_path
                """
            )
            for (raw_entry,) in cursor:
                entry_bytes = bytes(raw_entry)
                if count:
                    array_digest.update(b",")
                array_digest.update(entry_bytes)
                line = entry_bytes + b"\n"
                stream.write(line)
                jsonl_digest.update(line)
                count += 1
            array_digest.update(b"]")
            stream.flush()
            os.fsync(stream.fileno())
    except OSError as exc:
        raise StreamingLakeValidationError(
            f"selected manifest evidence could not be published: {selected_path}: {exc}"
        ) from None
    if count == 0:
        raise StreamingLakeValidationError("validated window contains no selected normalized manifests")
    return _SelectedEvidence(
        manifest_fingerprint=array_digest.hexdigest(),
        jsonl_sha256=jsonl_digest.hexdigest(),
        count=count,
    )


def _source_spool_preflight(
    catalog: _Catalog,
    *,
    scratch_low_watermark_bytes: int,
    scratch_reserve_bytes: int,
) -> dict[str, int]:
    selected = catalog.connection.execute(
        """
        SELECT COALESCE(SUM(size_bytes), 0), COALESCE(SUM(row_count), 0)
        FROM manifests WHERE selected = 1
        """
    ).fetchone()
    if selected is None:
        raise StreamingLakeValidationError("selected source sizing evidence disappeared")
    selected_size_bytes = int(selected[0])
    selected_row_count = int(selected[1])
    estimated_bytes = (
        selected_size_bytes * _SOURCE_SPOOL_COMPRESSED_SIZE_MULTIPLIER
        + selected_row_count * _SOURCE_SPOOL_BYTES_PER_SELECTED_ROW
    )
    available_bytes = shutil.disk_usage(catalog.path.parent).free
    required_remaining = max(scratch_low_watermark_bytes, scratch_reserve_bytes)
    projected_remaining = available_bytes - estimated_bytes
    if projected_remaining < required_remaining:
        raise StreamingLakeValidationError(
            "bounded source-spool disk preflight failed before Parquet ingest: "
            f"available={available_bytes} estimated_source_spool={estimated_bytes} "
            f"required_remaining={required_remaining}"
        )
    return {
        "available_bytes": available_bytes,
        "selected_parquet_bytes": selected_size_bytes,
        "selected_manifest_rows": selected_row_count,
        "compressed_size_multiplier": _SOURCE_SPOOL_COMPRESSED_SIZE_MULTIPLIER,
        "bytes_per_selected_row": _SOURCE_SPOOL_BYTES_PER_SELECTED_ROW,
        "estimated_source_spool_bytes": estimated_bytes,
        "projected_remaining_bytes": projected_remaining,
        "required_low_watermark_bytes": scratch_low_watermark_bytes,
        "required_reserve_bytes": scratch_reserve_bytes,
    }


def _manifest_from_entry(entry_bytes: bytes) -> PartitionManifest:
    decoded = json.loads(entry_bytes)
    if not isinstance(decoded, dict):
        raise StreamingLakeValidationError("catalog manifest entry is not an object")
    manifest_payload = dict(decoded)
    manifest_payload.pop("relative_data_path", None)
    manifest_payload.pop("relative_manifest_path", None)
    try:
        return PartitionManifest.from_dict(manifest_payload)
    except (PartitionValidationError, TypeError, ValueError) as exc:
        raise StreamingLakeValidationError(f"catalog manifest entry is invalid: {exc}") from None


def _required_utc(value: object, *, label: str) -> datetime:
    try:
        value_ns = timestamp_ns(value, label=label)
    except StreamingStoreError as exc:
        raise StreamingLakeValidationError(str(exc)) from None
    return datetime.fromtimestamp(value_ns / 1_000_000_000, tz=UTC)


def _interval_arrays(
    intervals: Sequence[StrictInterval],
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    return (
        tuple(timestamp_ns(item.start, label="strict interval start") for item in intervals),
        tuple(timestamp_ns(item.end, label="strict interval end") for item in intervals),
    )


def _in_strict_intervals(
    value_ns: int,
    starts: tuple[int, ...],
    ends: tuple[int, ...],
) -> bool:
    index = bisect_right(starts, value_ns) - 1
    return index >= 0 and value_ns < ends[index]


def _clock_overlaps_intervals(
    row: Mapping[str, object],
    starts: tuple[int, ...],
    ends: tuple[int, ...],
) -> bool:
    try:
        valid_from = timestamp_ns(row.get("causal_valid_from"), label="clock causal_valid_from")
        valid_until = timestamp_ns(row.get("causal_valid_until"), label="clock causal_valid_until")
    except StreamingStoreError:
        return False
    if valid_until <= valid_from:
        return False
    index = max(bisect_right(ends, valid_from), 0)
    return index < len(starts) and valid_from < ends[index] and valid_until > starts[index]


def _is_contemporaneous_market_row(row: Mapping[str, object]) -> bool:
    connection_id = str(row.get("connection_id") or "").casefold()
    if connection_id.startswith(("rest", "bootstrap", "history", "historical")):
        return False
    for name in ("snapshot_id", "update_id"):
        value = row.get(name)
        if isinstance(value, str) and value.casefold().startswith("rest:"):
            return False
    return True


def _validate_partition_row(
    row: Mapping[str, object],
    manifest: PartitionManifest,
) -> int:
    expected_type = RecordType(manifest.partition.record_type).value
    if row.get("record_type") != expected_type:
        raise StreamingLakeValidationError(
            f"row record_type does not match {manifest.relative_data_path.as_posix()}"
        )
    if row.get("venue") != manifest.partition.venue:
        raise StreamingLakeValidationError(
            f"row venue does not match {manifest.relative_data_path.as_posix()}"
        )
    if row.get("asset") != manifest.partition.asset:
        raise StreamingLakeValidationError(
            f"row asset does not match {manifest.relative_data_path.as_posix()}"
        )
    try:
        return timestamp_ns(row.get("received_time"), label="received_time")
    except StreamingStoreError as exc:
        raise StreamingLakeValidationError(str(exc)) from None


def _l2_add_rows(
    catalog: _Catalog,
    *,
    record_type: RecordType,
    rows: Sequence[Mapping[str, object]],
    received_ns: Sequence[int],
    manifest_order: int,
    row_orders: Sequence[int],
) -> None:
    if not rows:
        return
    catalog.connection.executemany(
        """
        INSERT INTO l2_source(
            record_type, venue, asset, received_ns,
            manifest_order, row_order, payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                record_type.value,
                str(row.get("venue") or ""),
                str(row.get("asset") or ""),
                received,
                manifest_order,
                row_order,
                pickle.dumps(dict(row), protocol=5),
            )
            for row, received, row_order in zip(rows, received_ns, row_orders, strict=True)
        ],
    )


def _projection(record_type: RecordType, schema_version: int) -> tuple[str, ...]:
    available = tuple(schema_for(record_type, schema_version).schema.names)
    available_set = frozenset(available)
    try:
        required = _REQUIRED_PROJECTION_COLUMNS[record_type]
    except KeyError:
        raise StreamingLakeValidationError(
            f"no bounded projection contract for {record_type.value}"
        ) from None
    missing = sorted(required - available_set)
    if missing:
        raise StreamingLakeValidationError(
            f"schema {record_type.value} v{schema_version} lacks required projected columns: {missing!r}"
        )
    selected = required | _OPTIONAL_PROJECTION_COLUMNS.get(record_type, frozenset())
    return tuple(name for name in available if name in selected)


@dataclass(slots=True)
class _ScanMetrics:
    selected_files_scanned: int = 0
    source_rows_scanned: int = 0
    record_batches_scanned: int = 0
    max_record_batch_rows: int = 0
    market_rows_retained: int = 0
    l2_raw_rows_staged: int = 0
    rows_scanned_by_dimension: dict[tuple[str, str, str], int] = field(default_factory=dict)


def _scan_selected_manifests(
    admission: BoundedLeadLagGateAdmission,
    catalog: _Catalog,
    source_spool: SourceRowSpool,
    *,
    batch_rows: int,
) -> tuple[_ScanMetrics, _ClockDiagnostics, dict[tuple[str, str, str], int]]:
    starts, ends = _interval_arrays(admission.intervals)
    metrics = _ScanMetrics()
    clock = _ClockDiagnostics()
    retained: dict[tuple[str, str, str], int] = {}
    cursor = catalog.connection.execute(
        """
        SELECT entry_json FROM manifests
        WHERE selected = 1 ORDER BY relative_data_path
        """
    )
    for manifest_order, (raw_entry,) in enumerate(cursor):
        manifest = _manifest_from_entry(bytes(raw_entry))
        record_type = RecordType(manifest.partition.record_type)
        if record_type not in _SELECTED_TYPES:
            raise StreamingLakeValidationError("catalog selected an unsupported record type")
        projection = _projection(record_type, manifest.schema_version)
        raw_row_count = 0
        for batch in iter_hashed_batches(
            admission.root,
            manifest,
            columns=projection,
            batch_size=batch_rows,
        ):
            metrics.record_batches_scanned += 1
            metrics.max_record_batch_rows = max(metrics.max_record_batch_rows, batch.num_rows)
            metrics.source_rows_scanned += batch.num_rows
            scan_dimension = (
                manifest.partition.venue,
                manifest.partition.asset,
                record_type.value,
            )
            metrics.rows_scanned_by_dimension[scan_dimension] = (
                metrics.rows_scanned_by_dimension.get(scan_dimension, 0) + batch.num_rows
            )
            rows = [{str(key): value for key, value in raw.items()} for raw in batch.to_pylist()]
            retained_rows: list[Mapping[str, object]] = []
            retained_orders: list[int] = []
            retained_received: list[int] = []
            for batch_offset, row in enumerate(rows):
                row_order = raw_row_count + batch_offset
                received_ns = _validate_partition_row(row, manifest)
                if record_type == RecordType.CLOCK_SYNC:
                    sample_valid = row.get("sample_status") == "valid"
                    usable = sample_valid and _clock_overlaps_intervals(row, starts, ends)
                    clock.observe(
                        received_ns,
                        sample_valid=sample_valid,
                        causally_usable=usable,
                    )
                    continue
                if not _in_strict_intervals(received_ns, starts, ends):
                    continue
                if not _is_contemporaneous_market_row(row):
                    continue
                retained_rows.append(row)
                retained_orders.append(row_order)
                retained_received.append(received_ns)
                dimension = (
                    manifest.partition.venue,
                    manifest.partition.asset,
                    record_type.value,
                )
                retained[dimension] = retained.get(dimension, 0) + 1
            if record_type in {RecordType.BBO, RecordType.TRADE}:
                source_spool.add_rows(
                    kind=record_type.value,
                    rows=retained_rows,
                    manifest_order=manifest_order,
                    first_row_order=raw_row_count,
                    row_orders=retained_orders,
                )
                metrics.market_rows_retained += len(retained_rows)
            elif record_type in {
                RecordType.L2_BOOK_STATE,
                RecordType.L2_SNAPSHOT,
            }:
                _l2_add_rows(
                    catalog,
                    record_type=record_type,
                    rows=retained_rows,
                    received_ns=retained_received,
                    manifest_order=manifest_order,
                    row_orders=retained_orders,
                )
                metrics.l2_raw_rows_staged += len(retained_rows)
            raw_row_count += batch.num_rows
            del rows
        if raw_row_count != manifest.row_count:
            raise StreamingLakeValidationError(
                f"row count changed for {manifest.relative_data_path.as_posix()}: "
                f"expected={manifest.row_count} actual={raw_row_count}"
            )
        metrics.selected_files_scanned += 1
        catalog.connection.commit()
    source_spool.commit()
    return metrics, clock, retained


def _max_complete_simultaneous_source_batch(
    catalog: _Catalog,
    source_spool: SourceRowSpool,
) -> int:
    """Return the disk-backed max BBO/trade/raw-L2 rows at one asset receive time."""

    source_spool.commit()
    catalog.connection.commit()
    alias = "bounded_source_input"
    catalog.connection.execute(
        f"ATTACH DATABASE ? AS {alias}",
        (str(source_spool.path),),
    )
    try:
        cursor = catalog.connection.execute(
            f"""
            SELECT COALESCE(MAX(total_rows), 0)
            FROM (
                SELECT asset, received_ns, SUM(row_count) AS total_rows
                FROM (
                    SELECT asset, received_ns, COUNT(*) AS row_count
                    FROM {alias}.source_rows
                    WHERE kind IN ('bbo', 'trade')
                    GROUP BY asset, received_ns
                    UNION ALL
                    SELECT asset, received_ns, COUNT(*) AS row_count
                    FROM l2_source
                    GROUP BY asset, received_ns
                )
                GROUP BY asset, received_ns
            )
            """
        )
        try:
            row = cursor.fetchone()
        finally:
            cursor.close()
        return 0 if row is None else int(row[0])
    finally:
        catalog.connection.execute(f"DETACH DATABASE {alias}")


def _as_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool):
        raise StreamingLakeValidationError(f"{label} must be integer-like")
    try:
        return int(str(value))
    except (TypeError, ValueError):
        raise StreamingLakeValidationError(f"{label} must be integer-like") from None


def _l2_key(row: Mapping[str, object]) -> tuple[str, str, str]:
    snapshot_id = row.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not snapshot_id:
        raise StreamingLakeValidationError("L2 row requires a snapshot_id")
    return str(row.get("venue")), str(row.get("asset")), snapshot_id


def _same_l2_lineage(header: Mapping[str, object], level: Mapping[str, object]) -> bool:
    return all(
        header.get(name) == level.get(name)
        for name in (
            "venue",
            "asset",
            "event_time",
            "exchange_time",
            "received_time",
            "source_sequence",
            "connection_id",
            "snapshot_id",
            "book_epoch_id",
        )
    )


def _reconstruct_l2_batch(
    headers: Sequence[dict[str, object]],
    levels: Sequence[dict[str, object]],
) -> list[dict[str, object]]:
    grouped_headers: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    grouped_levels: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    for header in headers:
        grouped_headers.setdefault(_l2_key(header), []).append(header)
    for level in levels:
        grouped_levels.setdefault(_l2_key(level), []).append(level)
    orphan_levels = sorted(set(grouped_levels) - set(grouped_headers))
    if orphan_levels:
        raise StreamingLakeValidationError(f"L2 atomic frame mismatch: orphan_levels={orphan_levels!r}")
    frames: list[dict[str, object]] = []
    for snapshot_key in sorted(grouped_headers):
        matching_headers = grouped_headers[snapshot_key]
        if len(matching_headers) != 1:
            raise StreamingLakeValidationError(
                f"L2 snapshot {snapshot_key!r} has {len(matching_headers)} headers"
            )
        header = matching_headers[0]
        bid_count = _as_integer(header.get("bid_level_count"), label="bid_level_count")
        ask_count = _as_integer(header.get("ask_level_count"), label="ask_level_count")
        if bid_count < 0 or ask_count < 0:
            raise StreamingLakeValidationError(f"L2 snapshot {snapshot_key!r} has invalid level counts")
        by_side: dict[str, list[dict[str, object]]] = {"bid": [], "ask": []}
        last_sequences: set[int | None] = set()
        for level in grouped_levels.get(snapshot_key, []):
            if not _same_l2_lineage(header, level):
                raise StreamingLakeValidationError(
                    f"L2 snapshot {snapshot_key!r} crosses lineage or timestamps"
                )
            side = level.get("side")
            if not isinstance(side, str) or side not in by_side:
                raise StreamingLakeValidationError(f"L2 snapshot {snapshot_key!r} has invalid side {side!r}")
            by_side[side].append(level)
            raw_sequence = level.get("last_sequence")
            last_sequences.add(
                None if raw_sequence is None else _as_integer(raw_sequence, label="last_sequence")
            )
        if len(by_side["bid"]) != bid_count or len(by_side["ask"]) != ask_count:
            raise StreamingLakeValidationError(
                f"L2 snapshot {snapshot_key!r} does not match header level counts"
            )
        if len(last_sequences) > 1:
            raise StreamingLakeValidationError(f"L2 snapshot {snapshot_key!r} has inconsistent last_sequence")
        compact_levels: dict[str, tuple[tuple[int, Decimal, Decimal, int | None], ...]] = {}
        depth: dict[str, Decimal] = {}
        for side, expected_count in (("bid", bid_count), ("ask", ask_count)):
            side_rows = sorted(
                by_side[side],
                key=lambda item: _as_integer(item.get("level"), label="level"),
            )
            indices = [_as_integer(item.get("level"), label="level") for item in side_rows]
            if indices != list(range(expected_count)):
                raise StreamingLakeValidationError(
                    f"L2 snapshot {snapshot_key!r} has non-contiguous {side} levels"
                )
            normalized: list[tuple[int, Decimal, Decimal, int | None]] = []
            for item in side_rows:
                price = item.get("price")
                quantity = item.get("quantity")
                if not isinstance(price, Decimal) or not isinstance(quantity, Decimal):
                    raise StreamingLakeValidationError(
                        f"L2 snapshot {snapshot_key!r} price and quantity must be Decimal"
                    )
                if price <= 0 or quantity < 0:
                    raise StreamingLakeValidationError(
                        f"L2 snapshot {snapshot_key!r} has invalid price or quantity"
                    )
                order_count = item.get("order_count")
                normalized.append(
                    (
                        _as_integer(item.get("level"), label="level"),
                        price,
                        quantity,
                        (None if order_count is None else _as_integer(order_count, label="order_count")),
                    )
                )
            compact_levels[side] = tuple(normalized)
            depth[side] = sum((item[2] for item in normalized), start=Decimal(0))
        total_depth = depth["bid"] + depth["ask"]
        imbalance = 0.0 if total_depth == 0 else float((depth["bid"] - depth["ask"]) / total_depth)
        frames.append(
            {
                **header,
                "last_sequence": next(iter(last_sequences), None),
                "bid_depth": depth["bid"],
                "ask_depth": depth["ask"],
                "imbalance": imbalance,
                "bids": compact_levels["bid"],
                "asks": compact_levels["ask"],
            }
        )
    return frames


@dataclass(frozen=True, slots=True)
class _L2Metrics:
    frame_count: int
    receive_batches: int
    max_rows_per_receive_batch: int
    max_frames_per_receive_batch: int


def _reconstruct_l2_to_spool(
    catalog: _Catalog,
    source_spool: SourceRowSpool,
    *,
    selected_manifest_count: int,
    max_l2_rows_per_receive: int,
) -> _L2Metrics:
    frame_count = 0
    receive_batches = 0
    max_rows = 0
    max_frames = 0
    batch_cursor = catalog.connection.execute(
        """
        SELECT asset, received_ns, COUNT(*)
        FROM l2_source
        GROUP BY asset, received_ns
        ORDER BY asset, received_ns
        """
    )
    try:
        for asset, received_ns, row_count in batch_cursor:
            count = int(row_count)
            if count > max_l2_rows_per_receive:
                raise StreamingLakeValidationError(
                    "L2 atomic receive batch exceeds bounded state: "
                    f"asset={asset} received_ns={received_ns} rows={count} "
                    f"limit={max_l2_rows_per_receive}"
                )
            rows = catalog.connection.execute(
                """
                SELECT record_type, payload FROM l2_source
                WHERE asset = ? AND received_ns = ?
                ORDER BY venue, record_type, manifest_order, row_order
                """,
                (str(asset), int(received_ns)),
            ).fetchall()
            headers: list[dict[str, object]] = []
            levels: list[dict[str, object]] = []
            for raw_type, payload in rows:
                decoded = pickle.loads(payload)
                if not isinstance(decoded, dict):
                    raise StreamingLakeValidationError("L2 scratch payload is not a row")
                if raw_type == RecordType.L2_BOOK_STATE.value:
                    headers.append(decoded)
                elif raw_type == RecordType.L2_SNAPSHOT.value:
                    levels.append(decoded)
                else:
                    raise StreamingLakeValidationError(f"unexpected L2 scratch record type: {raw_type}")
            frames = _reconstruct_l2_batch(headers, levels)
            if frames:
                source_spool.add_rows(
                    kind="l2",
                    rows=frames,
                    manifest_order=selected_manifest_count,
                    first_row_order=frame_count,
                )
            frame_count += len(frames)
            receive_batches += 1
            max_rows = max(max_rows, count)
            max_frames = max(max_frames, len(frames))
            del rows, headers, levels, frames
    finally:
        batch_cursor.close()
    source_spool.commit()
    return _L2Metrics(
        frame_count=frame_count,
        receive_batches=receive_batches,
        max_rows_per_receive_batch=max_rows,
        max_frames_per_receive_batch=max_frames,
    )


def _validate_required_populations(
    source_spool: SourceRowSpool,
    clock: _ClockDiagnostics,
    assets: Sequence[str],
) -> None:
    connection = source_spool.connection
    for kind in ("bbo", "trade", "l2"):
        for asset in assets:
            for venue in sorted(_VENUES):
                row = connection.execute(
                    """
                    SELECT COUNT(*) FROM source_rows
                    WHERE kind = ? AND asset = ? AND venue = ?
                    """,
                    (kind, asset, venue),
                ).fetchone()
                count = 0 if row is None else int(row[0])
                if count == 0:
                    raise StreamingLakeValidationError(
                        "validated window lacks required contemporaneous rows: "
                        f"kind={kind} asset={asset} venue={venue}"
                    )
    if clock.causally_usable_count == 0:
        raise StreamingLakeValidationError("validated window lacks causally valid clock diagnostics")


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _cleanup_exact_files(paths: Sequence[Path]) -> None:
    for path in paths:
        for candidate in (path, Path(f"{path}-journal"), Path(f"{path}-wal"), Path(f"{path}-shm")):
            try:
                candidate.unlink(missing_ok=True)
            except OSError:
                continue


def _staging_bytes(paths: Sequence[Path]) -> int:
    total = 0
    for path in paths:
        for candidate in (
            path,
            Path(f"{path}-journal"),
            Path(f"{path}-wal"),
            Path(f"{path}-shm"),
        ):
            total += _file_size(candidate)
    return total


def load_bounded_lead_lag_window(
    admission: BoundedLeadLagGateAdmission,
    scratch_dir: Path,
    selected_manifests_path: Path,
    *,
    batch_rows: int = _DEFAULT_BATCH_ROWS,
    max_l2_rows_per_receive: int = _DEFAULT_MAX_L2_ROWS_PER_RECEIVE,
    scratch_low_watermark_bytes: int | None = None,
    scratch_reserve_bytes: int | None = None,
) -> BoundedLeadLagWindow:
    """Stage projected causal rows without materializing the lake population."""

    if not isinstance(admission, BoundedLeadLagGateAdmission):
        raise TypeError("admission must come from validate_bounded_lead_lag_gate")
    if admission._binding_token is not _ADMISSION_BINDING_TOKEN:
        raise StreamingLakeValidationError("admission was not produced by validate_bounded_lead_lag_gate")
    if isinstance(batch_rows, bool) or not isinstance(batch_rows, int) or batch_rows <= 0:
        raise ValueError("batch_rows must be a positive integer")
    if (
        isinstance(max_l2_rows_per_receive, bool)
        or not isinstance(max_l2_rows_per_receive, int)
        or max_l2_rows_per_receive <= 0
    ):
        raise ValueError("max_l2_rows_per_receive must be a positive integer")
    normalized_disk_bounds: dict[str, int] = {}
    for name, value in (
        ("scratch_low_watermark_bytes", scratch_low_watermark_bytes),
        ("scratch_reserve_bytes", scratch_reserve_bytes),
    ):
        if value is None:
            normalized_disk_bounds[name] = 0
        elif isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a nonnegative integer or None")
        else:
            normalized_disk_bounds[name] = value
    verify_saved_phase10_gate_unchanged(admission.saved_gate)
    catalog_path, source_path, selected_path = _prepare_output_paths(
        admission, scratch_dir, selected_manifests_path
    )
    catalog: _Catalog | None = None
    source_spool: SourceRowSpool | None = None
    succeeded = False
    created = (catalog_path, source_path, selected_path)
    total_started = time.perf_counter()
    phase_timings: dict[str, float] = {}
    scratch_high_water = 0
    try:
        phase_started = time.perf_counter()
        catalog = _Catalog(catalog_path)
        source_spool = SourceRowSpool(source_path)
        files_discovered, manifest_files, parquet_files = _catalog_lake(admission.root, catalog)
        inventory_count, inventory_rows = _validate_catalog_inventory(catalog, admission.saved_gate)
        inventory_fingerprint = _inventory_manifest_fingerprint(catalog)
        phase_timings["manifest_catalog_and_inventory"] = time.perf_counter() - phase_started
        scratch_high_water = max(scratch_high_water, _staging_bytes(created))

        phase_started = time.perf_counter()
        _mark_selected(catalog, admission)
        evidence = _write_selected_manifest_evidence(catalog, selected_path)
        source_spool_preflight = _source_spool_preflight(
            catalog,
            scratch_low_watermark_bytes=normalized_disk_bounds["scratch_low_watermark_bytes"],
            scratch_reserve_bytes=normalized_disk_bounds["scratch_reserve_bytes"],
        )
        phase_timings["manifest_selection_and_evidence"] = time.perf_counter() - phase_started
        scratch_high_water = max(scratch_high_water, _staging_bytes(created))

        phase_started = time.perf_counter()
        scan_metrics, clock, retained = _scan_selected_manifests(
            admission,
            catalog,
            source_spool,
            batch_rows=batch_rows,
        )
        max_complete_simultaneous_source_batch = _max_complete_simultaneous_source_batch(
            catalog,
            source_spool,
        )
        if max_complete_simultaneous_source_batch > max_l2_rows_per_receive:
            raise StreamingLakeValidationError(
                "complete simultaneous source batch exceeds bounded state: "
                f"rows={max_complete_simultaneous_source_batch} "
                f"limit={max_l2_rows_per_receive}"
            )
        phase_timings["projected_scan_and_spool"] = time.perf_counter() - phase_started
        scratch_high_water = max(scratch_high_water, _staging_bytes(created))

        phase_started = time.perf_counter()
        l2_metrics = _reconstruct_l2_to_spool(
            catalog,
            source_spool,
            selected_manifest_count=evidence.count,
            max_l2_rows_per_receive=max_l2_rows_per_receive,
        )
        phase_timings["l2_atomic_reconstruction"] = time.perf_counter() - phase_started
        scratch_high_water = max(scratch_high_water, _staging_bytes(created))

        phase_started = time.perf_counter()
        _validate_required_populations(source_spool, clock, admission.assets)
        source_spool.commit()
        catalog.connection.commit()
        verify_saved_phase10_gate_unchanged(admission.saved_gate)
        rows_by_kind = {
            str(kind): int(count)
            for kind, count in source_spool.connection.execute(
                "SELECT kind, COUNT(*) FROM source_rows GROUP BY kind ORDER BY kind"
            )
        }
        phase_timings["final_validation_and_binding"] = time.perf_counter() - phase_started
        phase_timings["total"] = time.perf_counter() - total_started
        scratch_high_water = max(scratch_high_water, _staging_bytes(created))
        observability: dict[str, object] = {
            "files_discovered": files_discovered,
            "manifest_files_validated": manifest_files,
            "parquet_files_cataloged": parquet_files,
            "inventory_partition_count": inventory_count,
            "inventory_row_count": inventory_rows,
            "selected_manifests_count": evidence.count,
            "selected_files_scanned": scan_metrics.selected_files_scanned,
            "source_rows_scanned": scan_metrics.source_rows_scanned,
            "record_batches_scanned": scan_metrics.record_batches_scanned,
            "max_record_batch_rows": scan_metrics.max_record_batch_rows,
            "market_rows_retained": scan_metrics.market_rows_retained,
            "l2_raw_rows_staged": scan_metrics.l2_raw_rows_staged,
            "max_complete_simultaneous_source_batch_rows": (max_complete_simultaneous_source_batch),
            "l2_frames_reconstructed": l2_metrics.frame_count,
            "l2_receive_batches": l2_metrics.receive_batches,
            "max_l2_rows_per_receive_batch": l2_metrics.max_rows_per_receive_batch,
            "max_l2_frames_per_receive_batch": l2_metrics.max_frames_per_receive_batch,
            "rows_spooled_by_kind": rows_by_kind,
            "rows_scanned_by_venue_asset_type": {
                f"{venue}|{asset}|{record_type}": count
                for (venue, asset, record_type), count in sorted(
                    scan_metrics.rows_scanned_by_dimension.items()
                )
            },
            "rows_retained_by_venue_asset_type": {
                f"{venue}|{asset}|{record_type}": count
                for (venue, asset, record_type), count in sorted(retained.items())
            },
            "output_rows_spooled": source_spool.total_rows,
            "clock_sync_diagnostics": clock.legacy_dict(),
            "clock_input_diagnostics": clock.scan_dict(),
            "source_spool_preflight": source_spool_preflight,
            "catalog_bytes": _file_size(catalog_path),
            "source_spool_bytes": _file_size(source_path),
            "selected_manifests_bytes": _file_size(selected_path),
            "scratch_bytes": source_spool.scratch_bytes(),
            "scratch_size_high_water_bytes": scratch_high_water,
            "source_spool_commits": source_spool.commit_count,
            "source_spool_max_uncommitted_rows": source_spool.max_uncommitted_rows,
            "batch_rows_limit": batch_rows,
            "l2_rows_per_receive_limit": max_l2_rows_per_receive,
            "phase_timings_seconds": phase_timings,
        }
        catalog.close()
        catalog = None
        window = BoundedLeadLagWindow(
            root=admission.root,
            start=admission.start,
            end=admission.end,
            assets=admission.assets,
            intervals=admission.intervals,
            saved_gate=admission.saved_gate,
            gate_report_sha256=admission.gate_report_sha256,
            semantic_gate_sha256=admission.semantic_gate_sha256,
            semantic_gate_canonicalizer_version=(admission.semantic_gate_canonicalizer_version),
            excluded_json_pointers=admission.excluded_json_pointers,
            manifest_fingerprint=evidence.manifest_fingerprint,
            selected_manifests_sha256=evidence.jsonl_sha256,
            selected_manifests_count=evidence.count,
            source_spool=source_spool,
            observability=MappingProxyType(observability),
            _catalog_path=catalog_path,
            _selected_manifests_path=selected_path,
            _inventory_manifest_fingerprint=inventory_fingerprint,
            _inventory_partition_count=inventory_count,
            _inventory_row_count=inventory_rows,
            _clock_lookback=admission.clock_lookback,
        )
        succeeded = True
        return window
    except (
        OSError,
        PartitionValidationError,
        sqlite3.Error,
        StreamingStoreError,
    ) as exc:
        raise StreamingLakeValidationError(str(exc)) from None
    finally:
        if catalog is not None:
            catalog.close()
        if not succeeded:
            if source_spool is not None:
                source_spool.close()
            _cleanup_exact_files(created)


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size_bytes = 0
    try:
        with path.open("rb") as stream:
            while block := stream.read(_HASH_BLOCK_BYTES):
                digest.update(block)
                size_bytes += len(block)
    except OSError as exc:
        raise StreamingLakeValidationError(f"immutable input could not be rechecked: {path}: {exc}") from None
    return digest.hexdigest(), size_bytes


def _hash_selected_jsonl(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    count = 0
    try:
        with path.open("rb") as stream:
            for line in stream:
                digest.update(line)
                count += 1
    except OSError as exc:
        raise StreamingLakeValidationError(
            f"selected manifest evidence could not be rechecked: {path}: {exc}"
        ) from None
    return digest.hexdigest(), count


def _temporary_catalog_path(parent: Path) -> Path:
    with tempfile.NamedTemporaryFile(
        mode="wb",
        prefix="bounded-lead-lag-verify-",
        suffix=".sqlite3",
        dir=parent,
        delete=False,
    ) as handle:
        path = Path(handle.name)
    path.unlink()
    return path


def _selected_evidence_from_catalog(catalog: _Catalog) -> _SelectedEvidence:
    array_digest = hashlib.sha256()
    jsonl_digest = hashlib.sha256()
    array_digest.update(b"[")
    count = 0
    cursor = catalog.connection.execute(
        "SELECT entry_json FROM manifests WHERE selected = 1 ORDER BY relative_data_path"
    )
    for (raw_entry,) in cursor:
        entry = bytes(raw_entry)
        if count:
            array_digest.update(b",")
        array_digest.update(entry)
        jsonl_digest.update(entry)
        jsonl_digest.update(b"\n")
        count += 1
    array_digest.update(b"]")
    return _SelectedEvidence(
        manifest_fingerprint=array_digest.hexdigest(),
        jsonl_sha256=jsonl_digest.hexdigest(),
        count=count,
    )


def verify_immutable_inputs_unchanged(window: BoundedLeadLagWindow) -> None:
    """Recheck raw gate bytes, every manifest, and selected data bytes."""

    verify_saved_phase10_gate_unchanged(window.saved_gate)
    observed_jsonl_hash, observed_jsonl_count = _hash_selected_jsonl(window._selected_manifests_path)
    if (
        observed_jsonl_hash != window.selected_manifests_sha256
        or observed_jsonl_count != window.selected_manifests_count
    ):
        raise StreamingLakeValidationError("selected manifest JSONL evidence changed after bounded staging")
    verify_path = _temporary_catalog_path(window._catalog_path.parent)
    verify_catalog: _Catalog | None = None
    try:
        verify_catalog = _Catalog(verify_path)
        _catalog_lake(window.root, verify_catalog)
        partition_count, row_count = _validate_catalog_inventory(verify_catalog, window.saved_gate)
        if (
            partition_count != window._inventory_partition_count
            or row_count != window._inventory_row_count
            or _inventory_manifest_fingerprint(verify_catalog) != window._inventory_manifest_fingerprint
        ):
            raise StreamingLakeValidationError("lake manifest inventory changed after bounded staging")
        admission = _build_admission(
            root=window.root,
            start=window.start,
            end=window.end,
            assets=window.assets,
            intervals=window.intervals,
            saved_gate=window.saved_gate,
            gate_report_sha256=window.gate_report_sha256,
            semantic_gate_sha256=window.semantic_gate_sha256,
            semantic_gate_canonicalizer_version=(window.semantic_gate_canonicalizer_version),
            excluded_json_pointers=window.excluded_json_pointers,
            clock_lookback=window._clock_lookback,
        )
        _mark_selected(verify_catalog, admission)
        fresh_evidence = _selected_evidence_from_catalog(verify_catalog)
        if fresh_evidence != _SelectedEvidence(
            manifest_fingerprint=window.manifest_fingerprint,
            jsonl_sha256=window.selected_manifests_sha256,
            count=window.selected_manifests_count,
        ):
            raise StreamingLakeValidationError(
                "selected immutable manifest set changed after bounded staging"
            )
        cursor = verify_catalog.connection.execute(
            """
            SELECT relative_data_path, sha256, size_bytes
            FROM manifests WHERE selected = 1 ORDER BY relative_data_path
            """
        )
        for relative_path, expected_hash, expected_size in cursor:
            data_path = window.root / Path(str(relative_path))
            actual_hash, actual_size = _hash_file(data_path)
            if actual_hash != str(expected_hash) or actual_size != int(expected_size):
                raise StreamingLakeValidationError(f"selected immutable data changed: {relative_path}")
        verify_saved_phase10_gate_unchanged(window.saved_gate)
    finally:
        if verify_catalog is not None:
            verify_catalog.close()
        _cleanup_exact_files((verify_path,))


def _iso_from_ns(value: int | None) -> str | None:
    if value is None:
        return None
    seconds, nanoseconds = divmod(value, 1_000_000_000)
    base = datetime.fromtimestamp(seconds, tz=UTC)
    if nanoseconds == 0:
        fraction = ""
    elif nanoseconds % 1_000 == 0:
        fraction = f"{nanoseconds // 1_000:06d}"
    else:
        fraction = f"{nanoseconds:09d}"
    suffix = "" if not fraction else f".{fraction}"
    return base.strftime("%Y-%m-%dT%H:%M:%S") + suffix + "Z"


__all__ = [
    "BoundedLeadLagGateAdmission",
    "BoundedLeadLagWindow",
    "StreamingLakeValidationError",
    "load_bounded_lead_lag_window",
    "validate_bounded_lead_lag_gate",
    "verify_immutable_inputs_unchanged",
]
