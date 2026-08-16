from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Never

import pandas as pd
import pyarrow as pa
import pytest
import test_phase10_continuity as legacy_continuity

import hyperlab.data.continuity as continuity_module
from hyperlab.collector.models import ParsedRecord
from hyperlab.data import cli as data_cli
from hyperlab.data.continuity import (
    Interval,
    _load_lake,
    _state_intervals,
    audit_phase10_continuity,
)
from hyperlab.data.continuity_store import (
    SQLITE_COMMIT_INTERVAL_ROWS,
    BoundedRowStore,
    StreamingMetrics,
)
from hyperlab.data.lake import (
    PartitionKey,
    PartitionManifest,
    PartitionValidationError,
    discover_partitions,
    inventory_partitions,
    read_hashed_table,
    validate_partition,
    write_partition,
)
from hyperlab.data.schema import RecordType, latest_schema_for

_STRESS_MANIFEST_COUNT = 60_001
_STRESS_INTEGRITY_ROW_COUNT = 1_000_001
_STRESS_TIMESTAMP_COUNT = 5_000_001


def test_clock_forensic_evidence_is_row_bounded_after_60001_rejections() -> None:
    accumulator = continuity_module._ClockForensicAccumulator()
    for index in range(_STRESS_MANIFEST_COUNT):
        observed_at = legacy_continuity.BASE + timedelta(microseconds=index)
        row: dict[str, object] = {
            "schema_version": 4,
            "observation_id": f"clock-stress-{index:05d}",
            "capture_epoch_id": "clock-stress-capture",
            "connection_id": "clock-stress-public",
            "connection_epoch": 1,
            "request_sent_time": observed_at - timedelta(milliseconds=102),
            "response_received_time": observed_at,
            "server_time": observed_at - timedelta(milliseconds=51),
            "received_time": observed_at,
            "round_trip_latency_ms": Decimal("102"),
            "estimated_clock_drift_ms": Decimal("0"),
            "drift_uncertainty_ms": Decimal("51"),
            "sample_status": "invalid",
            "invalid_reason": "clock uncertainty exceeds threshold: 51ms > 50ms",
            "causal_valid_from": None,
            "causal_valid_until": None,
            "sampling_interval_ms": 10_000,
            "max_age_ms": 15_000,
            "max_uncertainty_ms": Decimal("50"),
        }
        sample = accumulator.observe(row, "rejected")
        accumulator.observe_rejection_streak(
            "clock-stress-capture",
            index + 1,
            sample,
        )

    payload = accumulator.as_dict()

    assert payload["schema_version_counts"] == {"4": _STRESS_MANIFEST_COUNT}
    assert payload["outcomes"] == {
        "valid": 0,
        "rejected": _STRESS_MANIFEST_COUNT,
        "hard_invalid": 0,
    }
    rejected = payload["rejected_evidence"]
    assert rejected["sample_limit"] == 64
    assert rejected["total"] == _STRESS_MANIFEST_COUNT
    assert rejected["retained"] == 64
    assert rejected["truncated"] == _STRESS_MANIFEST_COUNT - 64
    assert len(rejected["samples"]) == 64
    consecutive = payload["consecutive_rejection_evidence"]
    assert consecutive["total"] == 1
    assert consecutive["retained"] == 1
    assert consecutive["truncated"] == 0
    json.dumps(payload, sort_keys=True)


def test_clock_forensic_pair_evidence_is_bounded_across_40_captures() -> None:
    def forensic_payload(capture_order: Iterator[int]) -> dict[str, object]:
        accumulator = continuity_module._ClockForensicAccumulator()
        for capture_index in capture_order:
            capture = f"clock-pair-capture-{capture_index:02d}"
            for streak in (1, 2):
                observed_at = legacy_continuity.BASE + timedelta(
                    seconds=capture_index,
                    microseconds=streak,
                )
                row: dict[str, object] = {
                    "schema_version": 4,
                    "observation_id": f"clock-pair-{capture_index:02d}-{streak}",
                    "capture_epoch_id": capture,
                    "connection_id": f"clock-pair-public-{capture_index:02d}",
                    "connection_epoch": 1,
                    "request_sent_time": observed_at - timedelta(milliseconds=102),
                    "response_received_time": observed_at,
                    "server_time": observed_at - timedelta(milliseconds=51),
                    "received_time": observed_at,
                    "round_trip_latency_ms": Decimal("102"),
                    "estimated_clock_drift_ms": Decimal("0"),
                    "drift_uncertainty_ms": Decimal("51"),
                    "sample_status": "invalid",
                    "invalid_reason": (
                        "clock uncertainty exceeds threshold: 51ms > 50ms"
                    ),
                    "causal_valid_from": None,
                    "causal_valid_until": None,
                    "sampling_interval_ms": 10_000,
                    "max_age_ms": 15_000,
                    "max_uncertainty_ms": Decimal("50"),
                }
                sample = accumulator.observe(row, "rejected")
                accumulator.observe_rejection_streak(
                    capture,
                    streak,
                    sample,
                )
        return accumulator.as_dict()

    forward = forensic_payload(iter(range(40)))
    reverse = forensic_payload(iter(reversed(range(40))))
    assert forward == reverse
    consecutive = forward["consecutive_rejection_evidence"]
    assert consecutive["total"] == 40
    assert consecutive["retained"] == 32
    assert consecutive["truncated"] == 8
    assert [
        pair["capture_epoch_id"]
        for pair in consecutive["pairs"]
    ] == [f"clock-pair-capture-{index:02d}" for index in range(32)]


def test_scratch_store_preserves_and_orders_nanosecond_timestamps(
    tmp_path: Path,
) -> None:
    store = BoundedRowStore(StreamingMetrics(), scratch_parent=tmp_path)
    earlier = pd.Timestamp("2026-08-15T12:00:00.000000123Z")
    later = pd.Timestamp("2026-08-15T12:00:00.000000987Z")
    try:
        series = store.new_timestamp_series()
        series.append(later)
        series.append(earlier)
        assert [value.value for value in series] == [earlier.value, later.value]

        columns = (
            "received_time",
            "connection_id",
            "source_sequence",
            "snapshot_id",
        )
        store.insert_batch(
            venue="binance_usdm",
            record_type=RecordType.BBO,
            asset="BTC",
            population="in",
            manifest_order=0,
            first_row_order=0,
            columns=columns,
            rows=(
                {
                    "received_time": later,
                    "connection_id": "public-1",
                    "source_sequence": 2,
                    "snapshot_id": "snapshot-1",
                },
                {
                    "received_time": earlier,
                    "connection_id": "public-1",
                    "source_sequence": 1,
                    "snapshot_id": "snapshot-1",
                },
            ),
        )
        high_uint64 = 1 << 63
        max_uint64 = (1 << 64) - 1
        store.insert_batch(
            venue="binance_usdm",
            record_type=RecordType.TRADE,
            asset="BTC",
            population="in",
            manifest_order=1,
            first_row_order=0,
            columns=(
                "received_time",
                "connection_id",
                "connection_epoch",
                "arrival_sequence",
                "source_sequence",
            ),
            rows=(
                {
                    "received_time": earlier,
                    "connection_id": "market-1",
                    "connection_epoch": max_uint64,
                    "arrival_sequence": max_uint64,
                    "source_sequence": max_uint64,
                },
                {
                    "received_time": earlier,
                    "connection_id": "market-1",
                    "connection_epoch": max_uint64,
                    "arrival_sequence": high_uint64,
                    "source_sequence": high_uint64,
                },
            ),
        )
        store.finalize_indexes()
        ordered = list(store.iter_rows("binance_usdm", RecordType.BBO, "BTC"))
        assert [row["received_time"].value for row in ordered] == [
            earlier.value,
            later.value,
        ]
        exact = list(
            store.iter_rows(
                "binance_usdm",
                RecordType.BBO,
                "BTC",
                filters={"received_time": earlier},
            )
        )
        assert len(exact) == 1
        assert exact[0]["received_time"].value == earlier.value

        plan = store.connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT encoding_id, payload FROM rows
            WHERE venue = ? AND record_type = ? AND asset = ?
              AND population = 'in' AND snapshot_id = ?
            ORDER BY received_ns, asset, connection_id, source_sequence_text,
                     manifest_order, row_order
            """,
            ("binance_usdm", RecordType.BBO.value, "BTC", "snapshot-1"),
        ).fetchall()
        assert any("USING INDEX rows_snapshot_idx" in str(row[3]) for row in plan)

        ordered_trades = list(
            store.iter_rows(
                "binance_usdm",
                RecordType.TRADE,
                "BTC",
                order_by="received_source",
            )
        )
        assert [row["source_sequence"] for row in ordered_trades] == [
            high_uint64,
            max_uint64,
        ]
        exact_trade = list(
            store.iter_rows(
                "binance_usdm",
                RecordType.TRADE,
                "BTC",
                filters={
                    "connection_epoch": max_uint64,
                    "arrival_sequence": max_uint64,
                },
            )
        )
        assert len(exact_trade) == 1
        assert exact_trade[0]["source_sequence"] == max_uint64
    finally:
        store.close()


def _semantic_payload(payload: dict[str, object]) -> dict[str, object]:
    result = dict(payload)
    observability = result.pop("observability")
    assert isinstance(observability, dict)
    assert observability["semantic"] is False
    return result


def _canonical_semantic_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(
        _semantic_payload(payload),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()


@pytest.mark.parametrize(
    ("case", "fixture_options", "expected_sha256"),
    (
        (
            "pass",
            {},
            "3844e43cbab83cd039a1748e0d53a3d676d984baf591dae2656df8632777f16a",
        ),
        (
            "missing-raw-trade",
            {"omit_binance_raw_trade": True},
            "1eb65d3633acd2e019ae05407b7302f92672b83d34c086f3e3edda0d146266c7",
        ),
        (
            "missing-resync",
            {"omit_binance_resync": True},
            "4fb966afe747bf368e2ceec5eea19317c674fb6d1a4789203de59a3b53f76a1d",
        ),
        (
            "isolated-rejection",
            {"invalid_clock_at": legacy_continuity.BASE + timedelta(seconds=5.5)},
            "93a8b2a6dc51d63d71347bc625c3ca72c8b71f050dbeacd50f8f47faca66bf38",
        ),
        (
            "delayed-trade",
            {"delayed_hyperliquid_trade": True},
            "bf278b30285f70201fed0b6db6574336b6bed9edbe27aeeeb45c1be29736a196",
        ),
        (
            "rest-bootstrap",
            {"add_hyperliquid_rest_bootstrap": True},
            "e2f4174bc286882f8f12024b1239391df6d78cce08f5a4fa7460ce9bf0e2e6cd",
        ),
    ),
)
def test_streaming_report_matches_frozen_legacy_semantics(
    tmp_path: Path,
    case: str,
    fixture_options: dict[str, object],
    expected_sha256: str,
) -> None:
    lake = tmp_path / case
    legacy_continuity._write_continuity_lake(lake, **fixture_options)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=legacy_continuity.BASE,
        end=legacy_continuity.BASE + timedelta(seconds=30),
    )

    digest = hashlib.sha256(_canonical_semantic_bytes(payload)).hexdigest()
    assert digest == expected_sha256


def test_semantics_are_invariant_to_physical_file_fragmentation(
    tmp_path: Path,
) -> None:
    baseline_lake = tmp_path / "baseline"
    fragmented_lake = tmp_path / "fragmented"
    legacy_continuity._write_continuity_lake(baseline_lake)
    shutil.copytree(baseline_lake, fragmented_lake)

    for manifest_path in discover_partitions(fragmented_lake):
        manifest = validate_partition(manifest_path)
        if manifest.row_count <= 1:
            continue
        table = read_hashed_table(fragmented_lake, manifest)
        (fragmented_lake / manifest.relative_manifest_path).unlink()
        (fragmented_lake / manifest.relative_data_path).unlink()
        expected_interval = (
            None
            if manifest.expected_interval_ns is None
            else timedelta(microseconds=manifest.expected_interval_ns / 1_000)
        )
        for row_index in range(table.num_rows):
            write_partition(
                fragmented_lake,
                manifest.partition,
                table.slice(row_index, 1),
                expected_interval,
            )

    baseline = audit_phase10_continuity(
        baseline_lake,
        assets=("BTC", "ETH"),
        start=legacy_continuity.BASE,
        end=legacy_continuity.BASE + timedelta(seconds=30),
    )
    fragmented = audit_phase10_continuity(
        fragmented_lake,
        assets=("BTC", "ETH"),
        start=legacy_continuity.BASE,
        end=legacy_continuity.BASE + timedelta(seconds=30),
    )
    baseline_semantics = _semantic_payload(baseline)
    fragmented_semantics = _semantic_payload(fragmented)
    baseline_validation = dict(baseline_semantics["validation"])
    fragmented_validation = dict(fragmented_semantics["validation"])
    baseline_count = baseline_validation.pop("inventory_partition_count")
    fragmented_count = fragmented_validation.pop("inventory_partition_count")
    baseline_semantics["validation"] = baseline_validation
    fragmented_semantics["validation"] = fragmented_validation

    assert fragmented_count > baseline_count
    assert fragmented_semantics == baseline_semantics


def _write_single_record_partition(
    root: Path,
    record: ParsedRecord,
    *,
    expected_interval: timedelta | None = None,
) -> PartitionManifest:
    spec = latest_schema_for(record.record_type)
    row = {**record.row, "schema_version": spec.version}
    venue = row["venue"]
    event_time = row["event_time"]
    assert isinstance(venue, str)
    assert isinstance(event_time, datetime)
    table = pa.Table.from_pylist([row], schema=spec.schema)
    return write_partition(
        root,
        PartitionKey(
            venue=venue,
            date=event_time.date(),
            asset=record.asset,
            record_type=record.record_type,
        ),
        table,
        expected_interval,
    )


def _binance_bbo_record(
    received: datetime,
    *,
    arrival: int = 1,
    source_sequence: int = 100,
) -> ParsedRecord:
    records = legacy_continuity._binance_frame(
        legacy_continuity._binance_connector(),
        asset="BTC",
        kind="bbo",
        received=received,
        connection_id="binance-public-regression",
        arrival=arrival,
        source_sequence=source_sequence,
        capture="binance-capture-regression",
    )
    return next(record for record in records if record.record_type == RecordType.BBO)


def test_audit_rejects_duplicate_primary_key_split_across_valid_files(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    original = _binance_bbo_record(legacy_continuity.BASE + timedelta(seconds=1))
    first = _write_single_record_partition(lake, original)
    changed_quantity = Decimal(str(original.row["ask_quantity"])) + Decimal("1")
    conflicting = ParsedRecord(
        original.record_type,
        original.asset,
        {**original.row, "ask_quantity": changed_quantity},
    )
    second = _write_single_record_partition(lake, conflicting)
    assert first.data_file != second.data_file

    with pytest.raises(PartitionValidationError) as raised:
        audit_phase10_continuity(
            lake,
            assets=("BTC",),
            start=legacy_continuity.BASE,
            end=legacy_continuity.BASE + timedelta(seconds=30),
        )

    assert str(raised.value) == "duplicate primary keys: 1"


def test_audit_rejects_cross_file_l2_snapshot_metadata_conflict(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    frame = legacy_continuity._binance_frame(
        legacy_continuity._binance_connector(),
        asset="BTC",
        kind="l2",
        received=legacy_continuity.BASE + timedelta(seconds=1),
        connection_id="binance-public-regression",
        arrival=1,
        source_sequence=100,
        capture="binance-capture-regression",
    )
    levels = [
        record
        for record in frame
        if record.record_type == RecordType.L2_SNAPSHOT
    ]
    assert len(levels) == 2
    first = _write_single_record_partition(lake, levels[0])
    conflicting = ParsedRecord(
        levels[1].record_type,
        levels[1].asset,
        {
            **levels[1].row,
            "book_epoch_id": f"{levels[1].row['book_epoch_id']}:conflict",
        },
    )
    second = _write_single_record_partition(lake, conflicting)
    assert first.data_file != second.data_file
    snapshot_id = str(levels[0].row["snapshot_id"])

    with pytest.raises(PartitionValidationError) as raised:
        audit_phase10_continuity(
            lake,
            assets=("BTC",),
            start=legacy_continuity.BASE,
            end=legacy_continuity.BASE + timedelta(seconds=30),
        )

    assert str(raised.value) == (
        "inconsistent L2 snapshot metadata for "
        f"snapshot_id {snapshot_id!r} across partitions"
    )


def test_audit_rejects_canonical_partition_pair_under_extra_root_prefix(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    nested = lake / "unexpected-prefix"
    manifest = _write_single_record_partition(
        nested,
        _binance_bbo_record(legacy_continuity.BASE + timedelta(seconds=1)),
    )
    misplaced = (Path("unexpected-prefix") / manifest.relative_manifest_path).as_posix()
    expected = f"manifest is outside canonical root layout: {misplaced}"

    with pytest.raises(
        PartitionValidationError,
        match=rf"^{re.escape(expected)}$",
    ):
        audit_phase10_continuity(
            lake,
            assets=("BTC",),
            start=legacy_continuity.BASE,
            end=legacy_continuity.BASE + timedelta(seconds=30),
        )


def test_post_load_semantic_exception_closes_retained_scratch_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = tmp_path / "lake"
    scratch_parent = tmp_path / "scratch"
    scratch_parent.mkdir()
    legacy_continuity._write_continuity_lake(lake)
    original_load = continuity_module._load_lake
    retained_stores: list[BoundedRowStore] = []

    def retain_loaded_store(
        root: Path,
        start: datetime,
        end: datetime,
        assets: tuple[str, ...],
    ) -> continuity_module._LoadedLake:
        loaded = original_load(root, start, end, assets)
        retained_stores.append(loaded.store)
        return loaded

    def fail_semantic_validation(*args: object, **kwargs: object) -> Never:
        del args, kwargs
        raise RuntimeError("synthetic post-load semantic failure")

    monkeypatch.setattr(continuity_module, "configured_scratch_parent", lambda: scratch_parent)
    monkeypatch.setattr(continuity_module, "_load_lake", retain_loaded_store)
    monkeypatch.setattr(continuity_module, "_connection_lineage", fail_semantic_validation)

    with pytest.raises(RuntimeError, match=r"^synthetic post-load semantic failure$"):
        audit_phase10_continuity(
            lake,
            assets=("BTC", "ETH"),
            start=legacy_continuity.BASE,
            end=legacy_continuity.BASE + timedelta(seconds=30),
        )

    assert len(retained_stores) == 1
    scratch = retained_stores[0].directory
    assert not scratch.exists()
    assert list(scratch_parent.iterdir()) == []


def test_load_interruption_closes_scratch_before_public_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = tmp_path / "lake"
    scratch_parent = tmp_path / "scratch"
    scratch_parent.mkdir()
    _write_single_record_partition(
        lake,
        _binance_bbo_record(legacy_continuity.BASE + timedelta(seconds=1)),
    )

    def interrupt_scan(*args: object, **kwargs: object) -> Iterator[pa.RecordBatch]:
        del args, kwargs
        raise KeyboardInterrupt("synthetic load interruption")
        yield  # pragma: no cover - marks this as an iterator

    monkeypatch.setattr(continuity_module, "configured_scratch_parent", lambda: scratch_parent)
    monkeypatch.setattr(continuity_module, "iter_hashed_batches", interrupt_scan)

    with pytest.raises(KeyboardInterrupt, match="synthetic load interruption"):
        _load_lake(
            lake,
            legacy_continuity.BASE,
            legacy_continuity.BASE + timedelta(seconds=30),
            ("BTC",),
        )

    assert list(scratch_parent.iterdir()) == []


def test_pruned_partition_data_hash_is_validated_before_pruning(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    manifest = _write_single_record_partition(
        lake,
        _binance_bbo_record(legacy_continuity.BASE - timedelta(minutes=2)),
    )
    data_path = lake / manifest.relative_data_path
    corrupted = bytearray(data_path.read_bytes())
    corrupted[len(corrupted) // 2] ^= 1
    data_path.write_bytes(corrupted)
    actual_hash = hashlib.sha256(corrupted).hexdigest()
    expected = (
        "CORRUPT_PARTITION [hash_mismatch] "
        f"partition={manifest.relative_data_path.as_posix()} "
        f"expected_sha256={manifest.sha256} actual_sha256={actual_hash}"
    )

    with pytest.raises(
        PartitionValidationError,
        match=rf"^{re.escape(expected)}$",
    ):
        audit_phase10_continuity(
            lake,
            assets=("BTC",),
            start=legacy_continuity.BASE,
            end=legacy_continuity.BASE + timedelta(seconds=30),
        )


def test_tampered_manifest_bounds_are_validated_before_they_can_prune(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    manifest = _write_single_record_partition(
        lake,
        _binance_bbo_record(legacy_continuity.BASE + timedelta(seconds=1)),
    )
    manifest_path = lake / manifest.relative_manifest_path
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    false_bound = (legacy_continuity.BASE - timedelta(minutes=2)).isoformat()
    payload["timestamp_bounds"]["received_time"] = {
        "min": false_bound,
        "max": false_bound,
    }
    manifest_path.write_bytes(
        (
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode()
    )
    expected = (
        f"manifest statistics mismatch for {manifest.data_file}: timestamp_bounds"
    )

    with pytest.raises(
        PartitionValidationError,
        match=rf"^{re.escape(expected)}$",
    ):
        audit_phase10_continuity(
            lake,
            assets=("BTC",),
            start=legacy_continuity.BASE,
            end=legacy_continuity.BASE + timedelta(seconds=30),
        )


def test_required_bbo_cross_file_cadence_gap_matches_inventory_and_forces_fail(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    expected_interval = timedelta(seconds=1)
    _write_single_record_partition(
        lake,
        _binance_bbo_record(
            legacy_continuity.BASE + timedelta(seconds=1),
            arrival=1,
            source_sequence=100,
        ),
        expected_interval=expected_interval,
    )
    _write_single_record_partition(
        lake,
        _binance_bbo_record(
            legacy_continuity.BASE + timedelta(seconds=3),
            arrival=2,
            source_sequence=101,
        ),
        expected_interval=expected_interval,
    )
    inventory = inventory_partitions(lake)
    assert len(inventory.cross_segment_gaps) == 1
    key, gap = inventory.cross_segment_gaps[0]
    left = datetime.fromisoformat(gap.start.replace("Z", "+00:00"))
    right = datetime.fromisoformat(gap.end.replace("Z", "+00:00"))
    assert right - left < timedelta(seconds=30)
    inventory_derived = {
        "partition": key.relative_path.as_posix(),
        "kind": gap.kind,
        "start": gap.start,
        "end": gap.end,
        "missing_count": gap.missing_count,
        "connection_id": gap.connection_id,
        "cross_segment": True,
    }

    payload = audit_phase10_continuity(
        lake,
        assets=("BTC",),
        start=legacy_continuity.BASE,
        end=legacy_continuity.BASE + timedelta(seconds=10),
    )

    validation = payload["validation"]
    assert isinstance(validation, dict)
    assert validation["relevant_gap_count"] == 1
    assert validation["relevant_gaps"] == [inventory_derived]
    assert payload["technical_capture_gate"] == "FAIL"
    failure_reasons = payload["failure_reasons"]
    assert isinstance(failure_reasons, list)
    assert "bounded_lake_gaps_present" in failure_reasons


def _fake_manifest(
    *,
    venue: str,
    asset: str,
    record_type: RecordType,
    minimum: datetime,
    maximum: datetime,
    row_count: int,
) -> PartitionManifest:
    spec = latest_schema_for(record_type)
    digest = hashlib.sha256(
        f"{venue}:{asset}:{record_type.value}:{row_count}".encode()
    ).hexdigest()
    return PartitionManifest(
        partition=PartitionKey(
            venue,
            legacy_continuity.BASE.date(),
            asset,
            record_type,
        ),
        data_file=f"part-{digest}.parquet",
        sha256=digest,
        size_bytes=1,
        row_count=row_count,
        timestamp_bounds={
            "received_time": {
                "min": minimum.isoformat(),
                "max": maximum.isoformat(),
            }
        },
        schema_name=record_type.value,
        schema_version=spec.version,
        schema_fingerprint="streaming-regression-fixture",
        stream_key="streaming-regression-fixture",
        sequence_min=None,
        sequence_max=None,
        duplicates=0,
        out_of_order=0,
        gaps=(),
        gap_detection="not_applicable",
        null_counts={name: 0 for name in spec.schema.names},
        quality="PASS",
    )


def test_load_lake_validates_60001_lazy_mixed_manifests_with_bounded_scans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start = legacy_continuity.BASE
    end = start + timedelta(seconds=30)
    descriptors = (
        _fake_manifest(
            venue="other",
            asset="BTC",
            record_type=RecordType.TRADE,
            minimum=start,
            maximum=start,
            row_count=3,
        ),
        _fake_manifest(
            venue="binance_usdm",
            asset="SOL",
            record_type=RecordType.BBO,
            minimum=start,
            maximum=start,
            row_count=5,
        ),
        _fake_manifest(
            venue="hyperliquid",
            asset="DOGE",
            record_type=RecordType.L2_BOOK_STATE,
            minimum=start,
            maximum=start,
            row_count=7,
        ),
        _fake_manifest(
            venue="binance_usdm",
            asset="BTC",
            record_type=RecordType.CANDLE,
            minimum=start,
            maximum=start,
            row_count=11,
        ),
        _fake_manifest(
            venue="hyperliquid",
            asset="GLOBAL",
            record_type=RecordType.CONNECTION_EVENT,
            minimum=end,
            maximum=end + timedelta(seconds=1),
            row_count=13,
        ),
        _fake_manifest(
            venue="hyperliquid",
            asset="ETH",
            record_type=RecordType.L2_SNAPSHOT,
            minimum=start - timedelta(minutes=1),
            maximum=start - timedelta(microseconds=1),
            row_count=17,
        ),
    )
    progress = {"validated": 0, "scanned": 0}

    def lazy_paths(
        root: Path,
        store: BoundedRowStore,
    ) -> Iterator[Path]:
        metrics = store.metrics
        metrics.manifest_files_discovered = _STRESS_MANIFEST_COUNT
        for index in range(_STRESS_MANIFEST_COUNT):
            # If the caller materializes this generator before validating each
            # yielded path, the assertion fails at the second item.
            assert progress["validated"] == index
            assert progress["scanned"] == index
            descriptor = descriptors[index % len(descriptors)]
            yield root / descriptor.relative_manifest_path

    def validate(path: Path) -> PartitionManifest:
        del path
        index = progress["validated"]
        progress["validated"] += 1
        return descriptors[index % len(descriptors)]

    def bounded_empty_scan(*args: object, **kwargs: object) -> Iterator[object]:
        del args, kwargs
        progress["scanned"] += 1
        return iter(())

    monkeypatch.setattr(
        continuity_module,
        "_discover_partition_paths_bounded",
        lazy_paths,
    )
    monkeypatch.setattr(continuity_module, "validate_partition", validate)
    monkeypatch.setattr(continuity_module, "iter_hashed_batches", bounded_empty_scan)
    monkeypatch.setattr(
        continuity_module,
        "configured_scratch_parent",
        lambda: tmp_path,
    )

    loaded = _load_lake(
        tmp_path / "unused-lake",
        start,
        end,
        ("BTC", "ETH"),
    )
    scratch = loaded.store.directory
    try:
        metrics = loaded.metrics
        assert progress["validated"] == _STRESS_MANIFEST_COUNT
        assert progress["scanned"] == _STRESS_MANIFEST_COUNT
        assert metrics.manifest_files_discovered == _STRESS_MANIFEST_COUNT
        assert metrics.manifest_files_validated == _STRESS_MANIFEST_COUNT
        assert metrics.manifest_files_selected == 0
        assert metrics.manifest_files_pruned == _STRESS_MANIFEST_COUNT
        assert metrics.parquet_file_scan_operations == _STRESS_MANIFEST_COUNT
        assert metrics.unique_parquet_files_scanned == _STRESS_MANIFEST_COUNT
        assert metrics.rows_scanned == 0
        assert metrics.rows_staged == 0
        assert metrics.record_batches_scanned == 0
        assert loaded.inventory_partition_count == _STRESS_MANIFEST_COUNT
        assert loaded.inventory.partitions == ()
    finally:
        loaded.store.close()

    assert not scratch.exists()


def test_integrity_primary_keys_spill_1000001_rows_in_bounded_batches(
    tmp_path: Path,
) -> None:
    metrics = StreamingMetrics()
    store = BoundedRowStore(metrics, scratch_parent=tmp_path)
    datasets = (
        ("binance_usdm", "BTC", RecordType.BBO),
        ("binance_usdm", "ETH", RecordType.TRADE),
        ("binance_usdm", "GLOBAL", RecordType.WIRE_MESSAGE),
        ("hyperliquid", "BTC", RecordType.L2_BOOK_STATE),
        ("hyperliquid", "ETH", RecordType.L2_SNAPSHOT),
        ("hyperliquid", "GLOBAL", RecordType.CLOCK_SYNC),
    )
    dataset_ids = tuple(
        store.integrity_dataset_id(venue, asset, record_type.value, 1)
        for venue, asset, record_type in datasets
    )
    batch_size = 1_024
    max_batch = 0
    scratch = store.directory
    try:
        for batch_number, first in enumerate(
            range(0, _STRESS_INTEGRITY_ROW_COUNT, batch_size)
        ):
            count = min(batch_size, _STRESS_INTEGRITY_ROW_COUNT - first)
            keys = tuple((first + offset,) for offset in range(count))
            max_batch = max(max_batch, len(keys))
            assert store.add_integrity_primary_keys(
                dataset_ids[batch_number % len(dataset_ids)],
                keys,
            )
        scratch_bytes = store.observe_scratch_size()
        assert metrics.integrity_primary_keys_spilled == _STRESS_INTEGRITY_ROW_COUNT
        assert max_batch == batch_size
        assert metrics.sqlite_commits > 0
        assert metrics.max_uncommitted_rows <= SQLITE_COMMIT_INTERVAL_ROWS + batch_size
        assert scratch_bytes > 0
    finally:
        store.close()

    assert not scratch.exists()


class _ObservedTimestamp(datetime):
    __slots__ = ("ordinal", "owner")

    owner: _LazyContinuousTimestamps
    ordinal: int

    def __new__(
        cls,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
        second: int,
        microsecond: int,
        *,
        owner: _LazyContinuousTimestamps,
        ordinal: int,
    ) -> _ObservedTimestamp:
        value = super().__new__(
            cls,
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
            tzinfo=UTC,
        )
        value.owner = owner
        value.ordinal = ordinal
        return value

    def _plain(self) -> datetime:
        return datetime(
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            self.second,
            self.microsecond,
            tzinfo=UTC,
        )

    def __add__(self, value: timedelta) -> datetime:
        self.owner.processed = self.ordinal + 1
        return self._plain() + value

    def astimezone(self, tz: object = None) -> datetime:
        return self._plain().astimezone(tz)


class _LazyContinuousTimestamps:
    def __init__(self, count: int) -> None:
        self.count = count
        self.processed = 0
        self.yielded = 0
        self.started = False

    def __iter__(self) -> Iterator[datetime]:
        if self.started:
            raise AssertionError("timestamp population must be consumed once")
        self.started = True
        for ordinal in range(self.count):
            # `_state_intervals` marks each item from its arithmetic operation.
            # list()/sorted() materialization would request item two first.
            assert self.processed == ordinal
            seconds, microsecond = divmod(ordinal, 1_000_000)
            value = _ObservedTimestamp(
                2026,
                8,
                13,
                12,
                0,
                seconds,
                microsecond,
                owner=self,
                ordinal=ordinal,
            )
            self.yielded += 1
            yield value


def test_state_intervals_streams_5000001_continuous_timestamps() -> None:
    start = legacy_continuity.BASE
    end = start + timedelta(seconds=5, microseconds=2)
    timestamps = _LazyContinuousTimestamps(_STRESS_TIMESTAMP_COUNT)

    result = _state_intervals(
        timestamps,
        "capture-1",
        timedelta(microseconds=2),
        start,
        end,
        (),
    )

    assert timestamps.yielded == _STRESS_TIMESTAMP_COUNT
    assert timestamps.processed == _STRESS_TIMESTAMP_COUNT
    assert result == (Interval(start, end, "capture-1"),)


def test_observability_is_non_semantic_and_semantics_are_repeatable(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    legacy_continuity._write_continuity_lake(lake)

    first = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=legacy_continuity.BASE,
        end=legacy_continuity.BASE + timedelta(seconds=30),
    )
    second = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=legacy_continuity.BASE,
        end=legacy_continuity.BASE + timedelta(seconds=30),
    )

    first_semantic = _canonical_semantic_bytes(first)
    second_semantic = _canonical_semantic_bytes(second)
    assert first_semantic == second_semantic
    assert hashlib.sha256(first_semantic).hexdigest() == (
        "3844e43cbab83cd039a1748e0d53a3d676d984baf591dae2656df8632777f16a"
    )

    first_observability = first["observability"]
    second_observability = second["observability"]
    assert isinstance(first_observability, dict)
    assert isinstance(second_observability, dict)
    assert first_observability["semantic"] is False
    assert second_observability["semantic"] is False
    first_deterministic = dict(first_observability)
    second_deterministic = dict(second_observability)
    first_elapsed = first_deterministic.pop("elapsed_seconds_by_phase")
    second_elapsed = second_deterministic.pop("elapsed_seconds_by_phase")
    assert first_deterministic == second_deterministic
    assert isinstance(first_elapsed, dict)
    assert isinstance(second_elapsed, dict)
    assert set(first_elapsed) == set(second_elapsed)
    assert all(float(value) >= 0 for value in first_elapsed.values())
    assert all(float(value) >= 0 for value in second_elapsed.values())
