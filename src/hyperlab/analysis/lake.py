from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias, cast

import pandas as pd

from hyperlab.data.continuity import audit_phase10_continuity
from hyperlab.data.lake import (
    InventoryReport,
    PartitionManifest,
    inventory_partitions,
    read_hashed_table,
)
from hyperlab.data.schema import RecordType, latest_schema_for

if TYPE_CHECKING:
    from hyperlab.analysis.lead_lag import LeadLagDataset, StrictInterval


JsonValue: TypeAlias = (
    bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"] | None
)

_ASSETS = ("BTC", "ETH")
_ASSET_SET = frozenset(_ASSETS)
_VENUES = frozenset({"binance_usdm", "hyperliquid"})
_MARKET_TYPES = frozenset(
    {
        RecordType.BBO,
        RecordType.TRADE,
        RecordType.L2_BOOK_STATE,
        RecordType.L2_SNAPSHOT,
    }
)
_SELECTED_TYPES = _MARKET_TYPES | {RecordType.CLOCK_SYNC}


class LeadLagLakeValidationError(ValueError):
    """Raised when a saved gate or immutable lake snapshot is not trustworthy."""


@dataclass(frozen=True, slots=True)
class _ParsedInterval:
    start: datetime
    end: datetime
    tag: str


@dataclass(frozen=True, slots=True)
class _ValidatedGate:
    report: dict[str, object]
    assets: tuple[str, ...]
    start: datetime
    end: datetime
    intervals: tuple[_ParsedInterval, ...]
    clock_lookback: timedelta


@dataclass(frozen=True, slots=True)
class ValidatedLeadLagWindow:
    dataset: LeadLagDataset
    intervals: tuple[StrictInterval, ...]
    gate_report: dict[str, object]
    gate_report_sha256: str
    canonical_gate_sha256: str
    manifest_fingerprint: str
    selected_manifest_entries: tuple[dict[str, object], ...]
    start: datetime
    end: datetime
    assets: tuple[str, ...]
    root: Path


def _core_types() -> tuple[type[LeadLagDataset], type[StrictInterval]]:
    # The import stays lazy so this read-only boundary does not pull analysis
    # machinery into collector, writer, or continuity-gate processes.
    from hyperlab.analysis.lead_lag import LeadLagDataset, StrictInterval

    return LeadLagDataset, StrictInterval


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise LeadLagLakeValidationError(
                f"saved continuity gate contains duplicate JSON key {key!r}"
            )
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise LeadLagLakeValidationError(
        f"saved continuity gate contains non-finite JSON value {value}"
    )


def _normalize_json(value: object, *, label: str) -> JsonValue:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise LeadLagLakeValidationError(f"{label} contains NaN or infinity")
        return value
    if isinstance(value, Mapping):
        result: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise LeadLagLakeValidationError(
                    f"{label} contains a non-string mapping key"
                )
            result[key] = _normalize_json(item, label=f"{label}.{key}")
        return result
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        return [
            _normalize_json(item, label=f"{label}[{index}]")
            for index, item in enumerate(value)
        ]
    raise LeadLagLakeValidationError(
        f"{label} contains unsupported value type {type(value).__name__}"
    )


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        _normalize_json(value, label="canonical JSON"),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _as_mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(
        isinstance(key, str) for key in value
    ):
        raise LeadLagLakeValidationError(f"{label} must be a JSON object")
    return cast(dict[str, object], value)


def _as_list(value: object, *, label: str) -> list[object]:
    if not isinstance(value, list):
        raise LeadLagLakeValidationError(f"{label} must be a JSON array")
    return value


def _as_number(value: object, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise LeadLagLakeValidationError(f"{label} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise LeadLagLakeValidationError(f"{label} must be finite")
    return result


def _as_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or value is None:
        raise LeadLagLakeValidationError(f"{label} must be an integer")
    try:
        parsed = int(str(value))
    except ValueError:
        raise LeadLagLakeValidationError(f"{label} must be an integer") from None
    return parsed


def _parse_utc(value: object, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise LeadLagLakeValidationError(f"{label} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise LeadLagLakeValidationError(
            f"{label} must be a valid ISO-8601 timestamp"
        ) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LeadLagLakeValidationError(f"{label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _required_utc(value: object, *, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise LeadLagLakeValidationError(f"{label} must be a timestamp")
    if value.tzinfo is None or value.utcoffset() is None:
        raise LeadLagLakeValidationError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _load_saved_gate(path: Path) -> tuple[dict[str, object], bytes]:
    try:
        payload = path.read_bytes()
    except FileNotFoundError:
        raise LeadLagLakeValidationError(
            f"saved continuity gate does not exist: {path}"
        ) from None
    if not payload:
        raise LeadLagLakeValidationError("saved continuity gate is empty")
    try:
        decoded: object = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except UnicodeDecodeError:
        raise LeadLagLakeValidationError(
            "saved continuity gate must be UTF-8 JSON"
        ) from None
    except json.JSONDecodeError as exc:
        raise LeadLagLakeValidationError(
            f"saved continuity gate is invalid JSON: {exc.msg}"
        ) from None
    report = _as_mapping(decoded, label="saved continuity gate")
    _normalize_json(report, label="saved continuity gate")
    return report, payload


def _validate_saved_gate(report: dict[str, object]) -> _ValidatedGate:
    if report.get("technical_capture_gate") != "PASS":
        raise LeadLagLakeValidationError(
            "saved continuity gate must have technical_capture_gate=PASS"
        )
    failures = _as_list(
        report.get("failure_reasons"), label="failure_reasons"
    )
    if failures:
        raise LeadLagLakeValidationError(
            "saved continuity gate must have failure_reasons=[]"
        )

    raw_assets = _as_list(report.get("assets"), label="assets")
    if not all(isinstance(asset, str) for asset in raw_assets):
        raise LeadLagLakeValidationError("assets must contain only strings")
    assets = tuple(str(asset).strip().upper() for asset in raw_assets)
    if len(assets) != len(set(assets)) or frozenset(assets) != _ASSET_SET:
        raise LeadLagLakeValidationError(
            "saved continuity gate must cover exactly BTC and ETH"
        )

    requested = _as_mapping(
        report.get("requested_window"), label="requested_window"
    )
    start = _parse_utc(requested.get("start"), label="requested_window.start")
    end = _parse_utc(requested.get("end"), label="requested_window.end")
    if end <= start:
        raise LeadLagLakeValidationError(
            "requested_window.end must be after requested_window.start"
        )

    policy = _as_mapping(report.get("policy"), label="policy")
    if policy.get("interval_semantics") != "half_open_received_time_causal":
        raise LeadLagLakeValidationError(
            "continuity gate must use half_open_received_time_causal intervals"
        )

    clock = _as_mapping(report.get("clock_sync"), label="clock_sync")
    if clock.get("coverage_continuous") is not True:
        raise LeadLagLakeValidationError(
            "saved continuity gate requires continuous clock coverage"
        )
    if clock.get("causal_coverage_continuous") is not True:
        raise LeadLagLakeValidationError(
            "saved continuity gate requires causal clock coverage"
        )
    if (
        _as_number(
            clock.get("uncovered_seconds"),
            label="clock_sync.uncovered_seconds",
        )
        != 0.0
    ):
        raise LeadLagLakeValidationError(
            "saved continuity gate clock coverage must have zero uncovered seconds"
        )
    without_clock = _as_list(
        clock.get("market_active_without_valid_clock"),
        label="clock_sync.market_active_without_valid_clock",
    )
    if without_clock:
        raise LeadLagLakeValidationError(
            "market-active capture generations must all have valid clock coverage"
        )
    max_age_ms = _as_number(
        clock.get("strict_max_age_ms"), label="clock_sync.strict_max_age_ms"
    )
    if max_age_ms <= 0:
        raise LeadLagLakeValidationError(
            "clock_sync.strict_max_age_ms must be positive"
        )

    overlap = _as_mapping(
        report.get("strict_phase_10_overlap"),
        label="strict_phase_10_overlap",
    )
    raw_intervals = _as_list(
        overlap.get("intervals"), label="strict_phase_10_overlap.intervals"
    )
    intervals: list[_ParsedInterval] = []
    for index, raw_interval in enumerate(raw_intervals):
        item = _as_mapping(
            raw_interval,
            label=f"strict_phase_10_overlap.intervals[{index}]",
        )
        tag = item.get("capture_epoch_id")
        if not isinstance(tag, str) or not tag.strip():
            raise LeadLagLakeValidationError(
                "every strict interval requires a non-empty capture_epoch_id"
            )
        interval_start = _parse_utc(
            item.get("start"),
            label=f"strict_phase_10_overlap.intervals[{index}].start",
        )
        interval_end = _parse_utc(
            item.get("end"),
            label=f"strict_phase_10_overlap.intervals[{index}].end",
        )
        if interval_end <= interval_start:
            raise LeadLagLakeValidationError(
                "every strict interval must have positive duration"
            )
        if interval_start < start or interval_end > end:
            raise LeadLagLakeValidationError(
                "strict intervals must be contained by the requested window"
            )
        duration = _as_number(
            item.get("duration_seconds"),
            label=f"strict_phase_10_overlap.intervals[{index}].duration_seconds",
        )
        if duration <= 0 or not math.isclose(
            duration,
            (interval_end - interval_start).total_seconds(),
            abs_tol=1e-6,
        ):
            raise LeadLagLakeValidationError(
                "strict interval duration does not match its timestamps"
            )
        intervals.append(
            _ParsedInterval(
                start=interval_start,
                end=interval_end,
                tag=tag,
            )
        )

    ordered = tuple(sorted(intervals, key=lambda item: (item.start, item.end, item.tag)))
    if not ordered:
        raise LeadLagLakeValidationError(
            "saved continuity gate requires positive strict overlap intervals"
        )
    for previous, current in pairwise(ordered):
        if current.start < previous.end:
            raise LeadLagLakeValidationError(
                "strict overlap intervals must not overlap"
            )
    if raw_intervals != [
        {
            "capture_epoch_id": item.tag,
            "start": item.start.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "end": item.end.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "duration_seconds": round((item.end - item.start).total_seconds(), 6),
        }
        for item in ordered
    ]:
        raise LeadLagLakeValidationError(
            "strict overlap intervals are not in canonical gate order"
        )
    reported_count = overlap.get("interval_count")
    if isinstance(reported_count, bool) or not isinstance(reported_count, int):
        raise LeadLagLakeValidationError(
            "strict_phase_10_overlap.interval_count must be an integer"
        )
    if reported_count != len(ordered):
        raise LeadLagLakeValidationError(
            "strict overlap interval_count does not match intervals"
        )
    reported_duration = _as_number(
        overlap.get("duration_seconds"),
        label="strict_phase_10_overlap.duration_seconds",
    )
    total_duration = sum(
        (item.end - item.start).total_seconds() for item in ordered
    )
    if reported_duration <= 0 or not math.isclose(
        reported_duration, total_duration, abs_tol=1e-6
    ):
        raise LeadLagLakeValidationError(
            "strict overlap duration must be positive and match its intervals"
        )

    return _ValidatedGate(
        report=report,
        assets=assets,
        start=start,
        end=end,
        intervals=ordered,
        clock_lookback=timedelta(milliseconds=max_age_ms),
    )


def _manifest_received_bounds(
    manifest: PartitionManifest,
) -> tuple[datetime, datetime]:
    received = manifest.timestamp_bounds.get("received_time")
    if not isinstance(received, dict):
        raise LeadLagLakeValidationError(
            f"manifest {manifest.relative_data_path.as_posix()} lacks received_time bounds"
        )
    minimum = _parse_utc(
        received.get("min"),
        label=f"{manifest.relative_data_path.as_posix()} received_time.min",
    )
    maximum = _parse_utc(
        received.get("max"),
        label=f"{manifest.relative_data_path.as_posix()} received_time.max",
    )
    if maximum < minimum:
        raise LeadLagLakeValidationError(
            f"manifest {manifest.relative_data_path.as_posix()} has reversed bounds"
        )
    return minimum, maximum


def _record_type(manifest: PartitionManifest) -> RecordType:
    value = manifest.partition.record_type
    return value if isinstance(value, RecordType) else RecordType(value)


def _manifest_overlaps(
    manifest: PartitionManifest,
    intervals: tuple[_ParsedInterval, ...],
    *,
    clock_lookback: timedelta,
) -> bool:
    minimum, maximum = _manifest_received_bounds(manifest)
    record_type = _record_type(manifest)
    for interval in intervals:
        lower = (
            interval.start - clock_lookback
            if record_type == RecordType.CLOCK_SYNC
            else interval.start
        )
        if maximum >= lower and minimum < interval.end:
            return True
    return False


def _select_manifests(
    inventory: InventoryReport,
    *,
    assets: tuple[str, ...],
    intervals: tuple[_ParsedInterval, ...],
    clock_lookback: timedelta,
) -> tuple[PartitionManifest, ...]:
    selected: list[PartitionManifest] = []
    asset_set = frozenset(assets)
    for manifest in inventory.partitions:
        venue = manifest.partition.venue
        record_type = _record_type(manifest)
        asset = manifest.partition.asset
        if venue not in _VENUES or record_type not in _SELECTED_TYPES:
            continue
        if record_type == RecordType.CLOCK_SYNC:
            if asset != "GLOBAL" or manifest.schema_version < 2:
                continue
        elif asset not in asset_set:
            continue
        if _manifest_overlaps(
            manifest,
            intervals,
            clock_lookback=clock_lookback,
        ):
            selected.append(manifest)
    return tuple(
        sorted(selected, key=lambda item: item.relative_data_path.as_posix())
    )


def _manifest_entries(
    manifests: tuple[PartitionManifest, ...],
) -> tuple[dict[str, object], ...]:
    entries: list[dict[str, object]] = []
    for manifest in manifests:
        entry = manifest.as_dict()
        entry["relative_data_path"] = manifest.relative_data_path.as_posix()
        entry["relative_manifest_path"] = manifest.relative_manifest_path.as_posix()
        entries.append(entry)
    return tuple(entries)


def _inventory_matches_gate(
    inventory: InventoryReport, report: Mapping[str, object]
) -> None:
    validation = _as_mapping(report.get("validation"), label="validation")
    count = validation.get("inventory_partition_count")
    rows = validation.get("inventory_row_count")
    if isinstance(count, bool) or not isinstance(count, int):
        raise LeadLagLakeValidationError(
            "validation.inventory_partition_count must be an integer"
        )
    if isinstance(rows, bool) or not isinstance(rows, int):
        raise LeadLagLakeValidationError(
            "validation.inventory_row_count must be an integer"
        )
    if count != len(inventory.partitions) or rows != inventory.total_rows:
        raise LeadLagLakeValidationError(
            "lake inventory changed after the canonical continuity re-audit"
        )


def _in_intervals(
    timestamp: datetime, intervals: tuple[_ParsedInterval, ...]
) -> bool:
    return any(item.start <= timestamp < item.end for item in intervals)


def _clock_overlaps_intervals(
    row: Mapping[str, object], intervals: tuple[_ParsedInterval, ...]
) -> bool:
    valid_from = row.get("causal_valid_from")
    valid_until = row.get("causal_valid_until")
    if not isinstance(valid_from, datetime) or not isinstance(valid_until, datetime):
        return False
    start = _required_utc(valid_from, label="clock causal_valid_from")
    end = _required_utc(valid_until, label="clock causal_valid_until")
    if end <= start:
        return False
    return any(start < item.end and end > item.start for item in intervals)


def _is_contemporaneous_market_row(row: Mapping[str, object]) -> bool:
    connection_id = str(row.get("connection_id") or "").casefold()
    if connection_id.startswith(("rest", "bootstrap", "history", "historical")):
        return False
    for name in ("snapshot_id", "update_id"):
        value = row.get(name)
        if isinstance(value, str) and value.casefold().startswith("rest:"):
            return False
    return True


def _row_identity(row: Mapping[str, object]) -> str:
    for name in ("snapshot_id", "update_id", "trade_id", "observation_id"):
        value = row.get(name)
        if value is not None:
            return str(value)
    return ""


def _encoded_arrival_sequence(row: Mapping[str, object]) -> int:
    explicit = row.get("arrival_sequence")
    if explicit is not None:
        return _as_integer(explicit, label="arrival_sequence")
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
        candidate = update_id.rpartition(":")[2]
        try:
            return int(candidate)
        except ValueError:
            return -1
    return -1


def _row_sort_key(row: Mapping[str, object]) -> tuple[object, ...]:
    received = _required_utc(row.get("received_time"), label="received_time")
    return (
        pd.Timestamp(received).value,
        str(row.get("venue") or ""),
        str(row.get("asset") or ""),
        str(row.get("connection_id") or ""),
        _encoded_arrival_sequence(row),
        (
            -1
            if row.get("source_sequence") is None
            else _as_integer(row["source_sequence"], label="source_sequence")
        ),
        _row_identity(row),
    )


def _validate_partition_row(
    row: dict[str, object], manifest: PartitionManifest
) -> None:
    record_type = _record_type(manifest)
    if row.get("record_type") != record_type.value:
        raise LeadLagLakeValidationError(
            f"row record_type does not match {manifest.relative_data_path.as_posix()}"
        )
    if row.get("venue") != manifest.partition.venue:
        raise LeadLagLakeValidationError(
            f"row venue does not match {manifest.relative_data_path.as_posix()}"
        )
    if row.get("asset") != manifest.partition.asset:
        raise LeadLagLakeValidationError(
            f"row asset does not match {manifest.relative_data_path.as_posix()}"
        )
    _required_utc(row.get("received_time"), label="received_time")


def _read_selected_rows(
    root: Path,
    manifests: tuple[PartitionManifest, ...],
    intervals: tuple[_ParsedInterval, ...],
) -> dict[RecordType, list[dict[str, object]]]:
    rows: dict[RecordType, list[dict[str, object]]] = {
        record_type: [] for record_type in _SELECTED_TYPES
    }
    for manifest in manifests:
        record_type = _record_type(manifest)
        table = read_hashed_table(root, manifest)
        if table.num_rows != manifest.row_count:
            raise LeadLagLakeValidationError(
                f"row count changed for {manifest.relative_data_path.as_posix()}"
            )
        for raw_row in table.to_pylist():
            row = {str(key): value for key, value in raw_row.items()}
            _validate_partition_row(row, manifest)
            received = _required_utc(row["received_time"], label="received_time")
            if record_type == RecordType.CLOCK_SYNC:
                if (
                    row.get("sample_status") == "valid"
                    and _clock_overlaps_intervals(row, intervals)
                ):
                    rows[record_type].append(row)
                continue
            if _in_intervals(received, intervals) and _is_contemporaneous_market_row(row):
                rows[record_type].append(row)
    for values in rows.values():
        values.sort(key=_row_sort_key)
    return rows


def _same_l2_lineage(
    header: Mapping[str, object], level: Mapping[str, object]
) -> bool:
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


def _reconstruct_l2(
    headers: list[dict[str, object]], levels: list[dict[str, object]]
) -> list[dict[str, object]]:
    grouped_headers: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    grouped_levels: dict[tuple[str, str, str], list[dict[str, object]]] = {}

    def key(row: Mapping[str, object]) -> tuple[str, str, str]:
        snapshot_id = row.get("snapshot_id")
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise LeadLagLakeValidationError("L2 row requires a snapshot_id")
        return (str(row.get("venue")), str(row.get("asset")), snapshot_id)

    for header in headers:
        grouped_headers.setdefault(key(header), []).append(header)
    for level in levels:
        grouped_levels.setdefault(key(level), []).append(level)

    if not set(grouped_levels).issubset(grouped_headers):
        orphan_levels = sorted(set(grouped_levels) - set(grouped_headers))
        raise LeadLagLakeValidationError(
            "L2 atomic frame mismatch: "
            f"orphan_levels={orphan_levels!r}"
        )

    frames: list[dict[str, object]] = []
    for snapshot_key in sorted(grouped_headers):
        matching_headers = grouped_headers[snapshot_key]
        if len(matching_headers) != 1:
            raise LeadLagLakeValidationError(
                f"L2 snapshot {snapshot_key!r} has {len(matching_headers)} headers"
            )
        header = matching_headers[0]
        bid_count = _as_integer(
            header.get("bid_level_count"), label="bid_level_count"
        )
        ask_count = _as_integer(
            header.get("ask_level_count"), label="ask_level_count"
        )
        if bid_count < 0 or ask_count < 0:
            raise LeadLagLakeValidationError(
                f"L2 snapshot {snapshot_key!r} has invalid level counts"
            )
        by_side: dict[str, list[dict[str, object]]] = {"bid": [], "ask": []}
        last_sequences: set[int | None] = set()
        for level in grouped_levels.get(snapshot_key, []):
            if not _same_l2_lineage(header, level):
                raise LeadLagLakeValidationError(
                    f"L2 snapshot {snapshot_key!r} crosses lineage or timestamps"
                )
            side = level.get("side")
            if not isinstance(side, str) or side not in by_side:
                raise LeadLagLakeValidationError(
                    f"L2 snapshot {snapshot_key!r} has invalid side {side!r}"
                )
            by_side[side].append(level)
            raw_sequence = level.get("last_sequence")
            last_sequences.add(
                None
                if raw_sequence is None
                else _as_integer(raw_sequence, label="last_sequence")
            )
        if len(by_side["bid"]) != bid_count or len(by_side["ask"]) != ask_count:
            raise LeadLagLakeValidationError(
                f"L2 snapshot {snapshot_key!r} does not match header level counts"
            )
        if len(last_sequences) > 1:
            raise LeadLagLakeValidationError(
                f"L2 snapshot {snapshot_key!r} has inconsistent last_sequence"
            )

        compact_levels: dict[
            str, tuple[tuple[int, Decimal, Decimal, int | None], ...]
        ] = {}
        depth: dict[str, Decimal] = {}
        for side, expected_count in (("bid", bid_count), ("ask", ask_count)):
            side_rows = sorted(
                by_side[side],
                key=lambda item: _as_integer(item["level"], label="level"),
            )
            indices = [
                _as_integer(item["level"], label="level") for item in side_rows
            ]
            if indices != list(range(expected_count)):
                raise LeadLagLakeValidationError(
                    f"L2 snapshot {snapshot_key!r} has non-contiguous {side} levels"
                )
            normalized: list[tuple[int, Decimal, Decimal, int | None]] = []
            for item in side_rows:
                price = item.get("price")
                quantity = item.get("quantity")
                if not isinstance(price, Decimal) or not isinstance(quantity, Decimal):
                    raise LeadLagLakeValidationError(
                        f"L2 snapshot {snapshot_key!r} price and quantity must be Decimal"
                    )
                if price <= 0 or quantity < 0:
                    raise LeadLagLakeValidationError(
                        f"L2 snapshot {snapshot_key!r} has invalid price or quantity"
                    )
                order_count = item.get("order_count")
                normalized.append(
                    (
                        _as_integer(item["level"], label="level"),
                        price,
                        quantity,
                        (
                            None
                            if order_count is None
                            else _as_integer(order_count, label="order_count")
                        ),
                    )
                )
            compact_levels[side] = tuple(normalized)
            depth[side] = sum(
                (item[2] for item in normalized), start=Decimal(0)
            )

        total_depth = depth["bid"] + depth["ask"]
        imbalance = (
            0.0
            if total_depth == 0
            else float((depth["bid"] - depth["ask"]) / total_depth)
        )
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
    frames.sort(key=_row_sort_key)
    return frames


def _dataframe(
    rows: list[dict[str, object]], *, record_type: RecordType | None
) -> pd.DataFrame:
    if record_type is None:
        base_columns = [
            "schema_version",
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
            "last_sequence",
            "bid_depth",
            "ask_depth",
            "imbalance",
            "bids",
            "asks",
        ]
    else:
        base_columns = list(latest_schema_for(record_type).schema.names)
    extra_columns = sorted(
        {name for row in rows for name in row}.difference(base_columns)
    )
    frame = pd.DataFrame.from_records(rows, columns=[*base_columns, *extra_columns])
    if not frame.empty:
        frame = frame.sort_values(
            ["received_time", "venue", "asset"],
            kind="mergesort",
            ignore_index=True,
        )
    frame.attrs["causal_time"] = "received_time"
    frame.attrs["equal_received_time_semantics"] = "simultaneous_batch"
    return frame


def load_validated_lead_lag_window(
    root: Path, gate_report_path: Path
) -> ValidatedLeadLagWindow:
    """Load an immutable, independently gated Phase 10 analysis window.

    The saved report is treated as evidence, not authority: the unchanged
    continuity audit must reproduce the exact canonical JSON object before any
    adapter-owned economic table is loaded. Selected immutable manifests are
    then snapshotted by content hash and checked again after all reads.
    """

    root = Path(root)
    gate_report_path = Path(gate_report_path)
    if not root.is_dir():
        raise LeadLagLakeValidationError(f"lake root is not a directory: {root}")
    root = root.resolve()

    saved_report, raw_gate = _load_saved_gate(gate_report_path)
    gate = _validate_saved_gate(saved_report)
    live_report = audit_phase10_continuity(
        root,
        assets=gate.assets,
        start=gate.start,
        end=gate.end,
    )
    if _canonical_bytes(live_report) != _canonical_bytes(saved_report):
        raise LeadLagLakeValidationError(
            "saved continuity gate does not equal the canonical fresh re-audit"
        )

    inventory = inventory_partitions(root)
    _inventory_matches_gate(inventory, live_report)
    selected = _select_manifests(
        inventory,
        assets=gate.assets,
        intervals=gate.intervals,
        clock_lookback=gate.clock_lookback,
    )
    if not selected:
        raise LeadLagLakeValidationError(
            "validated window contains no selected normalized manifests"
        )
    selected_entries = _manifest_entries(selected)
    manifest_fingerprint = _sha256(_canonical_bytes(selected_entries))

    rows = _read_selected_rows(root, selected, gate.intervals)
    l2_rows = _reconstruct_l2(
        rows[RecordType.L2_BOOK_STATE], rows[RecordType.L2_SNAPSHOT]
    )
    bbo = _dataframe(rows[RecordType.BBO], record_type=RecordType.BBO)
    trades = _dataframe(rows[RecordType.TRADE], record_type=RecordType.TRADE)
    l2 = _dataframe(l2_rows, record_type=None)
    clock_sync = _dataframe(
        rows[RecordType.CLOCK_SYNC], record_type=RecordType.CLOCK_SYNC
    )
    if bbo.empty or trades.empty or l2.empty or clock_sync.empty:
        raise LeadLagLakeValidationError(
            "validated window must contain BBO, trade, complete L2, and valid clock rows"
        )

    final_inventory = inventory_partitions(root)
    final_selected = _select_manifests(
        final_inventory,
        assets=gate.assets,
        intervals=gate.intervals,
        clock_lookback=gate.clock_lookback,
    )
    if _manifest_entries(final_selected) != selected_entries:
        raise LeadLagLakeValidationError(
            "selected immutable manifest/hash set changed while loading"
        )
    if gate_report_path.read_bytes() != raw_gate:
        raise LeadLagLakeValidationError(
            "saved continuity gate changed while loading"
        )

    dataset_type, interval_type = _core_types()
    intervals = tuple(
        interval_type(start=item.start, end=item.end, tag=item.tag)
        for item in gate.intervals
    )
    dataset = dataset_type(
        bbo=bbo,
        trades=trades,
        l2=l2,
        clock_sync=clock_sync,
        provenance={
            "kind": "REAL",
            "assets": list(gate.assets),
            "requested_window": {
                "start": gate.start.isoformat(timespec="microseconds").replace(
                    "+00:00", "Z"
                ),
                "end": gate.end.isoformat(timespec="microseconds").replace(
                    "+00:00", "Z"
                ),
            },
            "gate_report_sha256": _sha256(raw_gate),
            "canonical_gate_sha256": _sha256(_canonical_bytes(saved_report)),
            "manifest_fingerprint": manifest_fingerprint,
            "selected_manifest_count": len(selected_entries),
        },
        source_fingerprint=manifest_fingerprint,
    )
    return ValidatedLeadLagWindow(
        root=root,
        dataset=dataset,
        intervals=intervals,
        gate_report=dict(saved_report),
        gate_report_sha256=_sha256(raw_gate),
        canonical_gate_sha256=_sha256(_canonical_bytes(saved_report)),
        manifest_fingerprint=manifest_fingerprint,
        selected_manifest_entries=selected_entries,
        start=gate.start,
        end=gate.end,
        assets=gate.assets,
    )
