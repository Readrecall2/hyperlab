from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import date as Date
from pathlib import Path

from hyperlab.data.lake import (
    InventoryReport,
    PartitionManifest,
    delisted_assets_as_of,
    inventory_partitions,
    read_hashed_table,
)
from hyperlab.data.schema import RecordType

QUALITY_REPORT_VERSION = 1


def _date_value(value: Date | str) -> Date:
    if isinstance(value, Date):
        return value
    try:
        return Date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"invalid quality report date: {value!r}") from None


def _connection_event_counts(
    root: Path,
    manifests: tuple[PartitionManifest, ...],
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for manifest in manifests:
        if manifest.partition.record_type != RecordType.CONNECTION_EVENT:
            continue
        table = read_hashed_table(root, manifest, columns=["event_kind"])
        counts.update(str(value) for value in table.column("event_kind").to_pylist())
    return dict(sorted(counts.items()))


def _aggregate_nulls(manifests: tuple[PartitionManifest, ...]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for manifest in manifests:
        counts.update(manifest.null_counts)
    return dict(sorted(counts.items()))


def daily_quality_report(root: Path, date: Date | str) -> dict[str, object]:
    """Return a deterministic, JSON-serializable quality report for one UTC day."""

    report_date = _date_value(date)
    inventory: InventoryReport = inventory_partitions(root, through_date=report_date)
    manifests = tuple(
        manifest
        for manifest in inventory.partitions
        if manifest.partition.date == report_date
    )
    degraded = sum(manifest.quality == "degraded" for manifest in manifests)
    unobservable = sum(manifest.quality == "unobservable" for manifest in manifests)
    gaps = [
        {
            "partition": manifest.relative_data_path.as_posix(),
            **gap.as_dict(),
        }
        for manifest in manifests
        for gap in manifest.gaps
    ]
    gaps.extend(
        {
            "partition": key.relative_path.as_posix(),
            "boundary": True,
            **gap.as_dict(),
        }
        for key, gap in inventory.cross_segment_gaps
        if key.date == report_date
    )
    gaps.sort(
        key=lambda item: (
            str(item["partition"]),
            str(item["kind"]),
            str(item["start"]),
            str(item["end"]),
        )
    )
    dependency_manifests = tuple(
        manifest
        for manifest in inventory.partitions
        if _date_value(manifest.partition.date) <= report_date
    )
    manifest_set = json.dumps(
        [manifest.as_dict() for manifest in dependency_manifests],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    report_quality = (
        "missing"
        if not manifests
        else "degraded"
        if degraded or gaps
        else "unobservable"
        if unobservable
        else "ok"
    )
    return {
        "report_version": QUALITY_REPORT_VERSION,
        "date": report_date.isoformat(),
        "quality": report_quality,
        "partition_count": len(manifests),
        "degraded_partition_count": degraded,
        "unobservable_partition_count": unobservable,
        "row_count": sum(manifest.row_count for manifest in manifests),
        "duplicate_count": sum(manifest.duplicates for manifest in manifests),
        "out_of_order_count": sum(manifest.out_of_order for manifest in manifests),
        "gap_count": len(gaps),
        "gaps": gaps,
        "connection_events": _connection_event_counts(root, manifests),
        "null_counts": _aggregate_nulls(manifests),
        "delisted_assets": list(
            delisted_assets_as_of(root, inventory.partitions, report_date)
        ),
        "manifest_set_sha256": hashlib.sha256(manifest_set).hexdigest(),
        "partitions": [
            {
                "path": manifest.relative_data_path.as_posix(),
                "sha256": manifest.sha256,
                "row_count": manifest.row_count,
                "quality": manifest.quality,
                "gap_detection": manifest.gap_detection,
                "schema_name": manifest.schema_name,
                "schema_version": manifest.schema_version,
                "schema_fingerprint": manifest.schema_fingerprint,
                "stream_key": manifest.stream_key,
                "timestamp_bounds": manifest.timestamp_bounds,
                "sequence_min": manifest.sequence_min,
                "sequence_max": manifest.sequence_max,
            }
            for manifest in manifests
        ],
    }
