from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import hyperlab.data.lake as lake_module
import hyperlab.data.quality as quality_module
import hyperlab.data.schema as data_schema
from hyperlab.data.catalog import build_catalog, export_dataset
from hyperlab.data.lake import (
    DataLakeError,
    PartitionKey,
    PartitionValidationError,
    inventory_partitions,
    validate_partition,
    write_partition,
)
from hyperlab.data.quality import daily_quality_report
from hyperlab.data.schema import (
    SCHEMA_NAME_METADATA,
    SCHEMA_VERSION_METADATA,
    RecordType,
    SchemaSpec,
    schema_for,
)

DAY = date(2026, 8, 11)


def _utc(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 8, 11, hour, minute, tzinfo=UTC)


def test_new_partition_directory_chain_is_durably_synced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "new-lake"
    synced: list[Path] = []
    monkeypatch.setattr(lake_module, "_fsync_directory", lambda path: synced.append(path))
    key = PartitionKey("hyperliquid", DAY, "BTC", RecordType.CANDLE)

    manifest = write_partition(root, key, _candle_table())

    leaf = (root / manifest.relative_data_path).parent
    expected_creation_parents = {
        root.parent,
        root,
        root / "venue=hyperliquid",
        root / "venue=hyperliquid" / f"date={DAY.isoformat()}",
        root / "venue=hyperliquid" / f"date={DAY.isoformat()}" / "asset=BTC",
    }
    assert expected_creation_parents <= set(synced)
    assert leaf in synced


def _directory_link(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target, target_is_directory=True)
        return
    except OSError as exc:
        if os.name != "nt":
            pytest.skip(f"directory symlinks unavailable: {exc}")
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "$ErrorActionPreference='Stop'; "
            "New-Item -ItemType Junction -Path $env:HYPERLAB_TEST_LINK "
            "-Target $env:HYPERLAB_TEST_TARGET | Out-Null",
        ],
        capture_output=True,
        check=False,
        env={
            **os.environ,
            "HYPERLAB_TEST_LINK": str(link),
            "HYPERLAB_TEST_TARGET": str(target),
        },
        text=True,
    )
    if completed.returncode != 0:
        pytest.skip(f"directory junctions unavailable: {completed.stderr.strip()}")


def _candle_table(
    *,
    times: tuple[datetime, ...] = (_utc(0), _utc(1)),
    sequences: tuple[int | None, ...] = (10, 11),
    venue: str = "hyperliquid",
    asset: str = "BTC",
    interval: str = "1h",
    interval_hours: int = 1,
) -> pa.Table:
    spec = schema_for("candle")
    rows = []
    for index, (event_time, sequence) in enumerate(zip(times, sequences, strict=True)):
        price = Decimal(50_000 + index)
        rows.append(
            {
                "schema_version": 1,
                "record_type": "candle",
                "venue": venue,
                "asset": asset,
                "event_time": event_time,
                "exchange_time": None if index == 0 else event_time,
                "received_time": event_time + timedelta(milliseconds=10),
                "source_sequence": sequence,
                "connection_id": "connection-1",
                "interval": interval,
                "open_time": event_time - timedelta(hours=interval_hours),
                "close_time": event_time,
                "open": price,
                "high": price + 1,
                "low": price - 1,
                "close": price,
                "base_volume": Decimal("12.5"),
                "quote_volume": None,
                "trade_count": 42,
                "is_final": True,
            }
        )
    return pa.Table.from_pylist(rows, schema=spec.schema)


def _lifecycle_table(
    *,
    status: str = "delisted",
    event_time: datetime | None = None,
    instrument_id: str = "hyperliquid:OLD:perp",
    source_symbol: str = "OLD",
) -> pa.Table:
    spec = schema_for("instrument_lifecycle")
    observed_at = event_time or _utc(2)
    return pa.Table.from_pylist(
        [
            {
                "schema_version": 1,
                "record_type": "instrument_lifecycle",
                "venue": "hyperliquid",
                "asset": "OLD",
                "event_time": observed_at,
                "exchange_time": None,
                "received_time": observed_at + timedelta(milliseconds=1),
                "source_sequence": None,
                "connection_id": None,
                "source_symbol": source_symbol,
                "instrument_id": instrument_id,
                "instrument_kind": "perp",
                "status": status,
                "valid_from": observed_at,
                "valid_to": None,
            }
        ],
        schema=spec.schema,
    )


def _trade_table(
    *,
    event_time: datetime,
    trade_id: str,
    sequence: int | None,
    asset: str = "BTC",
) -> pa.Table:
    spec = schema_for("trade")
    return pa.Table.from_pylist(
        [
            {
                "schema_version": 1,
                "record_type": "trade",
                "venue": "hyperliquid",
                "asset": asset,
                "event_time": event_time,
                "exchange_time": event_time,
                "received_time": event_time + timedelta(milliseconds=1),
                "source_sequence": sequence,
                "connection_id": "connection-1",
                "trade_id": trade_id,
                "aggressor_side": "buy",
                "price": Decimal("50000"),
                "quantity": Decimal("0.1"),
                "quote_quantity": None,
                "is_liquidation": None,
            }
        ],
        schema=spec.schema,
    )


def _wire_table(
    observations: tuple[tuple[datetime, int, int], ...],
    *,
    connection_id: str = "connection-1",
) -> pa.Table:
    spec = schema_for("wire_message")
    rows = []
    for event_time, connection_epoch, arrival_sequence in observations:
        raw_message = json.dumps(
            {"channel": "pong", "arrival": arrival_sequence},
            separators=(",", ":"),
        )
        rows.append(
            {
                "schema_version": 1,
                "record_type": "wire_message",
                "venue": "hyperliquid",
                "asset": "CONNECTION",
                "event_time": event_time,
                "exchange_time": None,
                "received_time": event_time,
                "source_sequence": None,
                "connection_id": connection_id,
                "connection_epoch": connection_epoch,
                "arrival_sequence": arrival_sequence,
                "channel": "pong",
                "raw_message": raw_message,
                "is_json": True,
                "payload_sha256": hashlib.sha256(raw_message.encode()).hexdigest(),
            }
        )
    return pa.Table.from_pylist(rows, schema=spec.schema)


def _resync_events_table() -> pa.Table:
    spec = schema_for("connection_event")
    rows = []
    for minute, event_kind in enumerate(("resync_start", "resync_complete")):
        event_time = _utc(3, minute)
        rows.append(
            {
                "schema_version": 1,
                "record_type": "connection_event",
                "venue": "hyperliquid",
                "asset": "BTC",
                "event_time": event_time,
                "exchange_time": None,
                "received_time": event_time,
                "source_sequence": None,
                "connection_id": "connection-1",
                "event_kind": event_kind,
                "channel": "l2Book",
                "book_epoch_id": "epoch-2",
                "reason": "sequence_gap" if event_kind == "resync_start" else None,
                "expected_sequence": 101,
                "observed_sequence": 103,
                "resync_snapshot_id": "snapshot-2",
            }
        )
    return pa.Table.from_pylist(rows, schema=spec.schema)


def _l2_delta_transition_table() -> pa.Table:
    spec = schema_for("l2_delta")
    rows = []
    for hour, epoch, connection, sequence in (
        (0, "epoch-1", "connection-1", 100),
        (1, "epoch-2", "connection-2", 1),
    ):
        event_time = _utc(hour)
        rows.append(
            {
                "schema_version": 1,
                "record_type": "l2_delta",
                "venue": "hyperliquid",
                "asset": "BTC",
                "event_time": event_time,
                "exchange_time": event_time,
                "received_time": event_time + timedelta(milliseconds=1),
                "source_sequence": sequence,
                "connection_id": connection,
                "update_id": f"update-{sequence}",
                "book_epoch_id": epoch,
                "first_sequence": sequence,
                "last_sequence": sequence,
                "side": "bid",
                "price": Decimal("50000"),
                "quantity": Decimal("1"),
                "action": "set",
            }
        )
    return pa.Table.from_pylist(rows, schema=spec.schema)


def _l2_snapshot_transition_table() -> pa.Table:
    spec = schema_for("l2_snapshot")
    rows = []
    for hour, epoch, connection, snapshot_id in (
        (0, "epoch-1", "connection-1", "snapshot-1"),
        (1, "epoch-2", "connection-2", "snapshot-2"),
    ):
        event_time = _utc(hour)
        rows.append(
            {
                "schema_version": 1,
                "record_type": "l2_snapshot",
                "venue": "hyperliquid",
                "asset": "BTC",
                "event_time": event_time,
                "exchange_time": event_time,
                "received_time": event_time + timedelta(milliseconds=1),
                "source_sequence": None,
                "connection_id": connection,
                "snapshot_id": snapshot_id,
                "book_epoch_id": epoch,
                "last_sequence": None,
                "side": "bid",
                "level": 0,
                "price": Decimal("50000"),
                "quantity": Decimal("1"),
                "order_count": 1,
            }
        )
    return pa.Table.from_pylist(rows, schema=spec.schema)


def _epoch_two_resync_events_table() -> pa.Table:
    spec = schema_for("connection_event")
    rows = []
    for minute, event_kind in ((30, "resync_start"), (45, "resync_complete")):
        event_time = _utc(0, minute)
        rows.append(
            {
                "schema_version": 1,
                "record_type": "connection_event",
                "venue": "hyperliquid",
                "asset": "BTC",
                "event_time": event_time,
                "exchange_time": None,
                "received_time": event_time,
                "source_sequence": None,
                "connection_id": "connection-2",
                "event_kind": event_kind,
                "channel": "l2Book",
                "book_epoch_id": "epoch-2",
                "reason": "connection_reset" if event_kind == "resync_start" else None,
                "expected_sequence": 1,
                "observed_sequence": 1,
                "resync_snapshot_id": "snapshot-2",
            }
        )
    return pa.Table.from_pylist(rows, schema=spec.schema)


def _funding_table(
    *,
    times: tuple[datetime, ...],
    sequences: tuple[int, ...],
    rate_kind: str,
    interval_seconds: int,
) -> pa.Table:
    spec = schema_for("funding")
    rows = []
    for event_time, sequence in zip(times, sequences, strict=True):
        rows.append(
            {
                "schema_version": 1,
                "record_type": "funding",
                "venue": "hyperliquid",
                "asset": "BTC",
                "event_time": event_time,
                "exchange_time": event_time,
                "received_time": event_time + timedelta(milliseconds=1),
                "source_sequence": sequence,
                "connection_id": "connection-1",
                "funding_time": event_time,
                "funding_rate": Decimal("0.00001"),
                "funding_interval_seconds": interval_seconds,
                "rate_kind": rate_kind,
                "mark_price": None,
                "oracle_price": None,
            }
        )
    return pa.Table.from_pylist(rows, schema=spec.schema)


def test_partition_key_rejects_ambiguous_or_unsafe_layout_values() -> None:
    with pytest.raises(ValueError, match="venue"):
        PartitionKey("../venue", DAY, "BTC", "candle")
    pair = PartitionKey("hyperliquid", DAY, "BTC/USDC", "candle")
    assert "asset=BTC%2FUSDC" in pair.relative_path.as_posix()
    with pytest.raises(ValueError, match="asset"):
        PartitionKey("hyperliquid", DAY, "../BTC", "candle")
    with pytest.raises(ValueError, match="record type"):
        PartitionKey("hyperliquid", DAY, "BTC", "l2")


def test_write_validate_and_repeat_are_content_addressed_and_idempotent(tmp_path: Path) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "candle")
    table = _candle_table()

    first = write_partition(tmp_path, key, table, expected_interval=timedelta(hours=1))
    second = write_partition(tmp_path, key, table, expected_interval=timedelta(hours=1))

    assert first == second
    assert first.data_file == f"part-{first.sha256}.parquet"
    assert first.row_count == 2
    assert first.timestamp_bounds["event_time"] == {
        "min": "2026-08-11T00:00:00.000000000Z",
        "max": "2026-08-11T01:00:00.000000000Z",
    }
    assert first.null_counts["exchange_time"] == 1
    assert first.gap_detection == "cadence_and_sequence"
    assert first.quality == "ok"

    leaf = key.path(tmp_path)
    assert len(list(leaf.glob("*.parquet"))) == 1
    manifest_path = leaf / first.manifest_file
    assert manifest_path.read_bytes().endswith(b"\n")
    assert json.loads(manifest_path.read_text(encoding="utf-8")) == first.as_dict()
    assert validate_partition(manifest_path) == first


def test_validation_hashes_before_attempting_to_read_corrupted_parquet(tmp_path: Path) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "candle")
    manifest = write_partition(tmp_path, key, _candle_table())
    data_path = key.path(tmp_path) / manifest.data_file
    corrupted = bytearray(data_path.read_bytes())
    corrupted[len(corrupted) // 2] ^= 0xFF
    data_path.write_bytes(corrupted)

    with pytest.raises(
        PartitionValidationError,
        match=(
            r"^CORRUPT_PARTITION \[hash_mismatch\] "
            r"partition=venue=hyperliquid/date=2026-08-11/asset=BTC/type=candle/"
            rf"{manifest.data_file} expected_sha256={manifest.sha256} actual_sha256="
        ),
    ):
        validate_partition(data_path)


@pytest.mark.parametrize(
    ("table", "message"),
    [
        (
            _candle_table(times=(_utc(0), _utc(0)), sequences=(10, 10)),
            "duplicate primary keys",
        ),
        (
            _candle_table(times=(_utc(1), _utc(0)), sequences=(11, 10)),
            "out-of-order rows",
        ),
        (_candle_table(venue="other"), "venue"),
        (_candle_table(times=(_utc(0), datetime(2026, 8, 12, tzinfo=UTC))), "date"),
    ],
)
def test_write_rejects_invalid_partition_content(tmp_path: Path, table: pa.Table, message: str) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "candle")
    with pytest.raises(PartitionValidationError, match=message):
        write_partition(tmp_path, key, table)


def test_write_rejects_non_utc_schema_without_casting(tmp_path: Path) -> None:
    spec = schema_for("candle")
    table = _candle_table()
    fields = [
        pa.field(field.name, pa.timestamp("ns") if field.name == "event_time" else field.type)
        for field in spec.schema
    ]
    naive_schema = pa.schema(fields, metadata=spec.schema.metadata)
    naive_table = table.cast(naive_schema)

    with pytest.raises(PartitionValidationError, match="schema mismatch"):
        write_partition(
            tmp_path,
            PartitionKey("hyperliquid", DAY, "BTC", "candle"),
            naive_table,
        )


def test_write_selects_the_registered_schema_version_from_arrow_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    v1 = schema_for(RecordType.CANDLE)
    v2_schema = v1.schema.append(pa.field("source_note", pa.string(), nullable=True))
    v2_schema = v2_schema.with_metadata(
        {
            SCHEMA_NAME_METADATA: RecordType.CANDLE.value.encode(),
            SCHEMA_VERSION_METADATA: b"2",
        }
    )
    v2 = SchemaSpec(
        record_type=RecordType.CANDLE,
        version=2,
        schema=v2_schema,
        primary_key=v1.primary_key,
        order_key=v1.order_key,
    )
    monkeypatch.setitem(data_schema._SCHEMA_REGISTRY, (RecordType.CANDLE, 2), v2)

    v1_table = _candle_table()
    v2_table = v1_table.set_column(
        v1_table.schema.get_field_index("schema_version"),
        v2_schema.field("schema_version"),
        pa.array([2] * v1_table.num_rows, type=pa.uint16()),
    )
    v2_table = v2_table.append_column(
        v2_schema.field("source_note"),
        pa.array([None] * v2_table.num_rows, type=pa.string()),
    ).replace_schema_metadata(v2_schema.metadata)
    key = PartitionKey("hyperliquid", DAY, "BTC", "candle")

    v1_manifest = write_partition(tmp_path, key, v1_table)
    v2_manifest = write_partition(tmp_path, key, v2_table)

    assert v1_manifest.schema_version == 1
    assert v2_manifest.schema_version == 2
    assert v2_manifest.schema_fingerprint != v1_manifest.schema_fingerprint
    inventory = inventory_partitions(tmp_path)
    assert sorted(manifest.schema_version for manifest in inventory.partitions) == [1, 2]
    assert inventory.total_rows == v1_table.num_rows + v2_table.num_rows


@pytest.mark.parametrize(
    ("raw_version", "message"),
    [
        (None, "missing hyperlab.schema_version metadata"),
        (b"version-one", "invalid hyperlab.schema_version metadata"),
        (b"0", "invalid hyperlab.schema_version metadata"),
        (b"99", "unknown schema version 99"),
    ],
)
def test_write_rejects_missing_invalid_or_unknown_arrow_schema_version(
    tmp_path: Path,
    raw_version: bytes | None,
    message: str,
) -> None:
    table = _candle_table()
    metadata = dict(table.schema.metadata or {})
    if raw_version is None:
        metadata.pop(SCHEMA_VERSION_METADATA)
    else:
        metadata[SCHEMA_VERSION_METADATA] = raw_version
    table = table.replace_schema_metadata(metadata)

    with pytest.raises(PartitionValidationError, match=message):
        write_partition(
            tmp_path,
            PartitionKey("hyperliquid", DAY, "BTC", "candle"),
            table,
        )


def test_sequence_and_time_gaps_are_explicit_and_degrade_quality(tmp_path: Path) -> None:
    table = _candle_table(
        times=(_utc(0), _utc(3)),
        sequences=(10, 13),
    )
    manifest = write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "candle"),
        table,
        expected_interval=timedelta(hours=1),
    )

    assert manifest.quality == "degraded"
    assert manifest.gap_detection == "cadence_and_sequence"
    assert {(gap.kind, gap.missing_count) for gap in manifest.gaps} == {
        ("sequence", 2),
        ("time", 2),
    }
    assert validate_partition(tmp_path / manifest.relative_data_path) == manifest


def test_gap_detection_is_explicit_when_a_stream_is_not_observable(tmp_path: Path) -> None:
    manifest = write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "trade"),
        _trade_table(event_time=_utc(0), trade_id="trade-1", sequence=None),
    )

    assert manifest.gap_detection == "not_observable"
    assert manifest.quality == "unobservable"
    report = daily_quality_report(tmp_path, DAY)
    assert report["quality"] == "unobservable"
    assert report["unobservable_partition_count"] == 1


def test_l2_snapshots_and_deltas_cannot_share_a_physical_partition(tmp_path: Path) -> None:
    snapshot_spec = schema_for("l2_snapshot")
    event_time = _utc(0)
    snapshot = pa.Table.from_pylist(
        [
            {
                "schema_version": 1,
                "record_type": "l2_snapshot",
                "venue": "hyperliquid",
                "asset": "BTC",
                "event_time": event_time,
                "exchange_time": event_time,
                "received_time": event_time + timedelta(milliseconds=1),
                "source_sequence": 100,
                "connection_id": "c1",
                "snapshot_id": "s1",
                "book_epoch_id": "e1",
                "last_sequence": 100,
                "side": "bid",
                "level": 0,
                "price": Decimal("50000"),
                "quantity": Decimal("1"),
                "order_count": None,
            }
        ],
        schema=snapshot_spec.schema,
    )

    with pytest.raises(PartitionValidationError, match=r"schema mismatch|record_type"):
        write_partition(
            tmp_path,
            PartitionKey("hyperliquid", DAY, "BTC", "l2_delta"),
            snapshot,
        )


def test_inventory_and_daily_report_retain_delisted_assets_deterministically(
    tmp_path: Path,
) -> None:
    candle = write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "candle"),
        _candle_table(),
    )
    lifecycle = write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "OLD", "instrument_lifecycle"),
        _lifecycle_table(),
    )

    inventory = inventory_partitions(tmp_path)
    assert len(inventory.partitions) == 2
    assert inventory.total_rows == 3
    assert inventory.delisted_assets == ("hyperliquid:OLD",)
    assert {item.sha256 for item in inventory.partitions} == {candle.sha256, lifecycle.sha256}

    first = daily_quality_report(tmp_path, DAY)
    second = daily_quality_report(tmp_path, "2026-08-11")
    assert first == second
    assert first["delisted_assets"] == ["hyperliquid:OLD"]
    assert first["row_count"] == 3
    encoded = json.dumps(first, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert encoded == json.dumps(second, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def test_inventory_detects_gaps_and_duplicates_across_segment_boundaries(
    tmp_path: Path,
) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "candle")
    write_partition(
        tmp_path,
        key,
        _candle_table(times=(_utc(0), _utc(1)), sequences=(10, 11)),
        expected_interval=timedelta(hours=1),
    )
    write_partition(
        tmp_path,
        key,
        _candle_table(times=(_utc(3), _utc(4)), sequences=(13, 14)),
        expected_interval=timedelta(hours=1),
    )

    report = daily_quality_report(tmp_path, DAY)
    boundary_gaps = [gap for gap in report["gaps"] if gap.get("boundary")]
    assert {(gap["kind"], gap["missing_count"]) for gap in boundary_gaps} == {
        ("sequence", 1),
        ("time", 1),
    }
    assert report["quality"] == "degraded"

    duplicate_root = tmp_path / "duplicates"
    write_partition(
        duplicate_root,
        key,
        _candle_table(times=(_utc(0), _utc(1)), sequences=(10, 11)),
    )
    write_partition(
        duplicate_root,
        key,
        _candle_table(times=(_utc(1), _utc(2)), sequences=(11, 12)),
    )
    with pytest.raises(PartitionValidationError, match="duplicate primary keys"):
        inventory_partitions(duplicate_root)


def test_inventory_merges_real_binance_snapshot_split_across_sorted_segments(
    tmp_path: Path,
) -> None:
    """Regression for ETH wire arrival 40862 observed on 2026-08-13."""

    spec = schema_for("l2_snapshot")
    connection = "fbf0cbd8eb444a2d944e5877bef9c3c6"
    epoch = f"{connection}:1"
    event_time = datetime(2026, 8, 13, 0, 34, 15, 69_000, tzinfo=UTC)
    received_time = datetime(2026, 8, 13, 0, 34, 15, 602_230, tzinfo=UTC)
    snapshot = f"ws:{connection}:1:40862:ETHUSDT:11274459157841"

    def row(
        row_event: datetime,
        row_received: datetime,
        snapshot_id: str,
        last_sequence: int,
        side: str,
        level: int,
    ) -> dict[str, object]:
        return {
            "schema_version": 1,
            "record_type": "l2_snapshot",
            "venue": "binance_usdm",
            "asset": "ETH",
            "event_time": row_event,
            "exchange_time": row_event + timedelta(milliseconds=4),
            "received_time": row_received,
            "source_sequence": None,
            "connection_id": connection,
            "snapshot_id": snapshot_id,
            "book_epoch_id": epoch,
            "last_sequence": last_sequence,
            "side": side,
            "level": level,
            "price": Decimal("1880") + Decimal(level) / Decimal("100"),
            "quantity": Decimal("1"),
            "order_count": None,
        }

    earlier_event = datetime(2026, 8, 13, 0, 34, 12, 612_000, tzinfo=UTC)
    first_rows = [
        row(
            earlier_event,
            earlier_event + timedelta(milliseconds=899),
            f"ws:{connection}:1:37481:ETHUSDT:11274458515044",
            11274458515044,
            "ask",
            0,
        ),
        *[row(event_time, received_time, snapshot, 11274459157841, "ask", level) for level in range(5)],
        *[row(event_time, received_time, snapshot, 11274459157841, "bid", level) for level in range(20)],
    ]
    second_rows = [
        row(event_time, received_time, snapshot, 11274459157841, "ask", level) for level in range(5, 20)
    ]
    key = PartitionKey("binance_usdm", date(2026, 8, 13), "ETH", "l2_snapshot")
    first = write_partition(tmp_path, key, pa.Table.from_pylist(first_rows, schema=spec.schema))
    second = write_partition(tmp_path, key, pa.Table.from_pylist(second_rows, schema=spec.schema))

    assert validate_partition(tmp_path / first.relative_manifest_path) == first
    assert validate_partition(tmp_path / second.relative_manifest_path) == second
    inventory = inventory_partitions(tmp_path)
    assert inventory.total_rows == 41
    assert inventory.cross_segment_gaps == ()


def test_inventory_rejects_a_canonical_parquet_without_its_manifest(tmp_path: Path) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "candle")
    manifest = write_partition(tmp_path, key, _candle_table())
    (key.path(tmp_path) / manifest.manifest_file).unlink()

    with pytest.raises(PartitionValidationError, match="orphan Parquet without manifest"):
        inventory_partitions(tmp_path)


def test_inventory_rejects_every_noncanonical_parquet_under_the_lake(tmp_path: Path) -> None:
    (tmp_path / "export.parquet").write_bytes(b"not a lake partition")

    with pytest.raises(PartitionValidationError, match="non-canonical Parquet file"):
        inventory_partitions(tmp_path)


def test_cross_date_gaps_are_attributed_to_the_day_of_the_current_row(tmp_path: Path) -> None:
    next_day = date(2026, 8, 12)
    previous = datetime(2026, 8, 11, 23, tzinfo=UTC)
    current = datetime(2026, 8, 12, 1, tzinfo=UTC)
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "candle"),
        _candle_table(times=(previous,), sequences=(23,)),
        expected_interval=timedelta(hours=1),
    )
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", next_day, "BTC", "candle"),
        _candle_table(times=(current,), sequences=(25,)),
        expected_interval=timedelta(hours=1),
    )

    previous_report = daily_quality_report(tmp_path, DAY)
    current_report = daily_quality_report(tmp_path, next_day)
    assert previous_report["gap_count"] == 0
    assert {(gap["kind"], gap["missing_count"], gap["partition"]) for gap in current_report["gaps"]} == {
        (
            "sequence",
            1,
            "venue=hyperliquid/date=2026-08-12/asset=BTC/type=candle",
        ),
        ("time", 1, "venue=hyperliquid/date=2026-08-12/asset=BTC/type=candle"),
    }

    isolated = tmp_path / "isolated"
    write_partition(
        isolated,
        PartitionKey("hyperliquid", next_day, "BTC", "candle"),
        _candle_table(times=(current,), sequences=(25,)),
        expected_interval=timedelta(hours=1),
    )
    assert daily_quality_report(isolated, next_day)["gap_count"] == 0


def test_primary_keys_remain_unique_across_date_partitions(tmp_path: Path) -> None:
    next_day = date(2026, 8, 12)
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "trade"),
        _trade_table(event_time=_utc(23), trade_id="duplicate-id", sequence=10),
    )
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", next_day, "BTC", "trade"),
        _trade_table(
            event_time=datetime(2026, 8, 12, 0, tzinfo=UTC),
            trade_id="duplicate-id",
            sequence=11,
        ),
    )

    with pytest.raises(PartitionValidationError, match="duplicate primary keys"):
        inventory_partitions(tmp_path)


def test_manifest_file_name_must_match_its_content_address(tmp_path: Path) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "candle")
    manifest = write_partition(tmp_path, key, _candle_table())
    original = key.path(tmp_path) / manifest.manifest_file
    renamed = key.path(tmp_path) / "part-renamed.manifest.json"
    original.rename(renamed)

    with pytest.raises(PartitionValidationError, match="manifest file name mismatch"):
        validate_partition(renamed)


def test_manifest_rejects_a_non_positive_expected_interval(tmp_path: Path) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "candle")
    manifest = write_partition(
        tmp_path,
        key,
        _candle_table(),
        expected_interval=timedelta(hours=1),
    )
    manifest_path = key.path(tmp_path) / manifest.manifest_file
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["expected_interval_ns"] = -1
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(PartitionValidationError, match="expected_interval_ns must be positive"):
        validate_partition(manifest_path)


@pytest.mark.parametrize(
    ("arrivals", "expected_kind", "missing_count"),
    [
        ((1, 3), "arrival_sequence", 1),
        ((3, 1), "arrival_sequence_regression", 0),
    ],
)
def test_wire_arrival_sequence_quality_is_distinct_from_source_sequence(
    tmp_path: Path,
    arrivals: tuple[int, int],
    expected_kind: str,
    missing_count: int,
) -> None:
    manifest = write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "CONNECTION", "wire_message"),
        _wire_table(
            (
                (_utc(0), 1, arrivals[0]),
                (_utc(1), 1, arrivals[1]),
            )
        ),
    )

    assert manifest.sequence_min is None
    assert manifest.sequence_max is None
    assert manifest.gap_detection == "arrival_sequence"
    assert [(gap.kind, gap.missing_count) for gap in manifest.gaps] == [(expected_kind, missing_count)]


def test_wire_arrival_sequence_may_restart_in_a_new_connection_epoch(
    tmp_path: Path,
) -> None:
    manifest = write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "CONNECTION", "wire_message"),
        _wire_table(
            (
                (_utc(0), 1, 3),
                (_utc(1), 2, 1),
            )
        ),
    )

    assert manifest.gaps == ()


def test_wire_arrival_gap_is_detected_across_parquet_segments(tmp_path: Path) -> None:
    key = PartitionKey("hyperliquid", DAY, "CONNECTION", "wire_message")
    write_partition(tmp_path, key, _wire_table(((_utc(0), 1, 1),)))
    write_partition(tmp_path, key, _wire_table(((_utc(1), 1, 3),)))

    gaps = inventory_partitions(tmp_path).cross_segment_gaps
    assert [(gap.kind, gap.missing_count) for _, gap in gaps] == [("arrival_sequence", 1)]


def test_daily_report_makes_l2_resynchronization_events_visible(tmp_path: Path) -> None:
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "connection_event"),
        _resync_events_table(),
    )

    report = daily_quality_report(tmp_path, DAY)
    assert report["connection_events"] == {
        "resync_complete": 1,
        "resync_start": 1,
    }


def test_inventory_rejects_a_valid_partition_below_an_unexpected_prefix(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    prefixed_root = lake / "unexpected-prefix"
    write_partition(
        prefixed_root,
        PartitionKey("hyperliquid", DAY, "BTC", "candle"),
        _candle_table(),
    )

    with pytest.raises(PartitionValidationError, match="canonical root layout"):
        inventory_partitions(lake)


def test_empty_daily_report_is_missing_not_ok(tmp_path: Path) -> None:
    report = daily_quality_report(tmp_path, DAY)

    assert report["quality"] == "missing"
    assert report["partition_count"] == 0
    assert report["row_count"] == 0


def test_daily_delisted_assets_are_computed_as_of_the_report_day(tmp_path: Path) -> None:
    next_day = date(2026, 8, 12)
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "OLD", "instrument_lifecycle"),
        _lifecycle_table(status="listed", event_time=_utc(2)),
    )
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", next_day, "OLD", "instrument_lifecycle"),
        _lifecycle_table(
            status="delisted",
            event_time=datetime(2026, 8, 12, 2, tzinfo=UTC),
        ),
    )

    assert daily_quality_report(tmp_path, DAY)["delisted_assets"] == []
    assert daily_quality_report(tmp_path, next_day)["delisted_assets"] == ["hyperliquid:OLD"]


def test_delisted_assets_are_deduplicated_across_instrument_ids(tmp_path: Path) -> None:
    key = PartitionKey("hyperliquid", DAY, "OLD", "instrument_lifecycle")
    first = _lifecycle_table(instrument_id="hyperliquid:OLD:spot", source_symbol="OLD-SPOT")
    second = _lifecycle_table(
        event_time=_utc(3),
        instrument_id="hyperliquid:OLD:perp",
        source_symbol="OLD-PERP",
    )
    write_partition(tmp_path, key, first)
    write_partition(tmp_path, key, second)

    assert inventory_partitions(tmp_path).delisted_assets == ("hyperliquid:OLD",)
    assert daily_quality_report(tmp_path, DAY)["delisted_assets"] == ["hyperliquid:OLD"]


def test_l2_snapshot_epoch_transition_without_resync_is_visible_without_sequence(
    tmp_path: Path,
) -> None:
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "l2_snapshot"),
        _l2_snapshot_transition_table(),
    )

    report = daily_quality_report(tmp_path, DAY)
    assert report["quality"] == "degraded"
    assert len(report["gaps"]) == 1
    gap = report["gaps"][0]
    assert gap["kind"] == "l2_resync_missing"
    assert "snapshot=snapshot-1" in gap["start"]
    assert "snapshot=snapshot-2" in gap["end"]
    assert "sequence=" not in gap["start"] + gap["end"]


def test_l2_snapshot_epoch_transition_with_explicit_resync_is_accepted(
    tmp_path: Path,
) -> None:
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "l2_snapshot"),
        _l2_snapshot_transition_table(),
    )
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "connection_event"),
        _epoch_two_resync_events_table(),
    )

    report = daily_quality_report(tmp_path, DAY)
    assert report["gap_count"] == 0
    assert report["connection_events"] == {
        "resync_complete": 1,
        "resync_start": 1,
    }


def test_l2_epoch_transition_without_explicit_resync_is_degraded(tmp_path: Path) -> None:
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "l2_delta"),
        _l2_delta_transition_table(),
    )

    report = daily_quality_report(tmp_path, DAY)
    assert report["quality"] == "degraded"
    assert [gap["kind"] for gap in report["gaps"]] == ["l2_resync_missing"]


def test_l2_epoch_transition_with_explicit_resync_is_accepted(tmp_path: Path) -> None:
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "l2_delta"),
        _l2_delta_transition_table(),
    )
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "connection_event"),
        _epoch_two_resync_events_table(),
    )

    report = daily_quality_report(tmp_path, DAY)
    assert report["gap_count"] == 0
    assert report["connection_events"] == {
        "resync_complete": 1,
        "resync_start": 1,
    }


def test_connection_report_rehashes_the_exact_bytes_it_decodes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "connection_event"),
        _resync_events_table(),
    )
    original_inventory = quality_module.inventory_partitions

    def inventory_then_corrupt(
        root: Path,
        *,
        through_date: date | str | None = None,
    ) -> object:
        inventory = original_inventory(root, through_date=through_date)
        data_path = root / manifest.relative_data_path
        corrupted = bytearray(data_path.read_bytes())
        corrupted[len(corrupted) // 2] ^= 0xFF
        data_path.write_bytes(corrupted)
        return inventory

    monkeypatch.setattr(quality_module, "inventory_partitions", inventory_then_corrupt)
    with pytest.raises(PartitionValidationError, match=r"CORRUPT_PARTITION \[hash_mismatch\]"):
        daily_quality_report(tmp_path, DAY)


def test_validation_hashes_and_decodes_one_payload_when_file_is_replaced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "candle")
    manifest = write_partition(tmp_path, key, _candle_table())
    data_path = (tmp_path / manifest.relative_data_path).resolve()
    original_read_bytes = Path.read_bytes

    def read_then_replace(path: Path) -> bytes:
        payload = original_read_bytes(path)
        if path.resolve() == data_path:
            corrupted = bytearray(payload)
            corrupted[len(corrupted) // 2] ^= 0xFF
            path.write_bytes(corrupted)
        return payload

    monkeypatch.setattr(Path, "read_bytes", read_then_replace)
    assert validate_partition(data_path) == manifest
    monkeypatch.undo()
    with pytest.raises(PartitionValidationError, match=r"CORRUPT_PARTITION \[hash_mismatch\]"):
        validate_partition(data_path)


def test_candle_cadences_coexist_without_cross_stream_gaps(tmp_path: Path) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "candle")
    one_hour = write_partition(
        tmp_path,
        key,
        _candle_table(times=(_utc(0), _utc(1)), sequences=(10, 11)),
    )
    four_hour = write_partition(
        tmp_path,
        key,
        _candle_table(
            times=(_utc(0), _utc(4)),
            sequences=(100, 101),
            interval="4h",
            interval_hours=4,
        ),
    )

    assert one_hour.expected_interval_ns == 3_600_000_000_000
    assert four_hour.expected_interval_ns == 14_400_000_000_000
    assert daily_quality_report(tmp_path, DAY)["gap_count"] == 0

    with pytest.raises(PartitionValidationError, match="declared candle interval"):
        write_partition(
            tmp_path / "mismatch",
            key,
            _candle_table(),
            expected_interval=timedelta(hours=2),
        )


def test_funding_kinds_and_cadences_are_distinct_logical_streams(tmp_path: Path) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "funding")
    hourly = write_partition(
        tmp_path,
        key,
        _funding_table(
            times=(_utc(0), _utc(1)),
            sequences=(10, 11),
            rate_kind="predicted",
            interval_seconds=3_600,
        ),
    )
    settled = write_partition(
        tmp_path,
        key,
        _funding_table(
            times=(_utc(0), _utc(8)),
            sequences=(100, 101),
            rate_kind="settled",
            interval_seconds=28_800,
        ),
    )

    assert hourly.expected_interval_ns == 3_600_000_000_000
    assert settled.expected_interval_ns == 28_800_000_000_000
    assert hourly.stream_key != settled.stream_key
    assert daily_quality_report(tmp_path, DAY)["gap_count"] == 0

    with pytest.raises(PartitionValidationError, match="declared funding interval"):
        write_partition(
            tmp_path / "mismatch",
            key,
            _funding_table(
                times=(_utc(0),),
                sequences=(10,),
                rate_kind="predicted",
                interval_seconds=3_600,
            ),
            expected_interval=timedelta(hours=8),
        )


def test_funding_hour_buckets_tolerate_millisecond_jitter(tmp_path: Path) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "funding")
    manifest = write_partition(
        tmp_path,
        key,
        _funding_table(
            times=(
                _utc(0) + timedelta(milliseconds=76),
                _utc(1) + timedelta(milliseconds=123),
            ),
            sequences=(10, 11),
            rate_kind="settled",
            interval_seconds=3_600,
        ),
    )

    assert manifest.gaps == ()
    assert manifest.gap_detection == "funding_bucket_and_sequence"


def test_funding_hour_buckets_still_report_a_missing_hour(tmp_path: Path) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "funding")
    manifest = write_partition(
        tmp_path,
        key,
        _funding_table(
            times=(
                _utc(0) + timedelta(milliseconds=76),
                _utc(2) + timedelta(milliseconds=123),
            ),
            sequences=(10, 11),
            rate_kind="settled",
            interval_seconds=3_600,
        ),
    )

    assert [(gap.kind, gap.missing_count) for gap in manifest.gaps] == [("funding_bucket", 1)]


def test_funding_tolerance_assigns_just_before_hour_to_next_bucket(
    tmp_path: Path,
) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "funding")
    manifest = write_partition(
        tmp_path,
        key,
        _funding_table(
            times=(
                _utc(1) - timedelta(milliseconds=50),
                _utc(2) - timedelta(milliseconds=25),
            ),
            sequences=(10, 11),
            rate_kind="settled",
            interval_seconds=3_600,
        ),
    )

    assert manifest.gaps == ()


def test_funding_bucket_tolerance_is_used_across_parquet_segments(
    tmp_path: Path,
) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "funding")
    for event_time, sequence in (
        (_utc(0) + timedelta(milliseconds=76), 10),
        (_utc(1) + timedelta(milliseconds=123), 11),
    ):
        write_partition(
            tmp_path,
            key,
            _funding_table(
                times=(event_time,),
                sequences=(sequence,),
                rate_kind="settled",
                interval_seconds=3_600,
            ),
        )

    assert inventory_partitions(tmp_path).cross_segment_gaps == ()


def test_funding_primary_keys_remain_unique_across_historical_cadences(
    tmp_path: Path,
) -> None:
    key = PartitionKey("hyperliquid", DAY, "BTC", "funding")
    write_partition(
        tmp_path,
        key,
        _funding_table(
            times=(_utc(0),),
            sequences=(10,),
            rate_kind="settled",
            interval_seconds=3_600,
        ),
    )
    write_partition(
        tmp_path,
        key,
        _funding_table(
            times=(_utc(0),),
            sequences=(11,),
            rate_kind="settled",
            interval_seconds=28_800,
        ),
    )

    with pytest.raises(PartitionValidationError, match="duplicate primary keys"):
        inventory_partitions(tmp_path)


def test_daily_manifest_set_hash_is_causal_and_covers_prior_dependencies(tmp_path: Path) -> None:
    report_day = date(2026, 8, 12)
    report_time = datetime(2026, 8, 12, 1, tzinfo=UTC)
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", report_day, "BTC", "trade"),
        _trade_table(event_time=report_time, trade_id="report", sequence=2),
    )
    baseline = daily_quality_report(tmp_path, report_day)["manifest_set_sha256"]

    future_day = date(2026, 8, 13)
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", future_day, "FUTURE", "trade"),
        _trade_table(
            event_time=datetime(2026, 8, 13, 1, tzinfo=UTC),
            trade_id="future",
            sequence=1,
            asset="FUTURE",
        ),
    )
    assert daily_quality_report(tmp_path, report_day)["manifest_set_sha256"] == baseline

    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "PAST", "trade"),
        _trade_table(event_time=_utc(1), trade_id="past", sequence=1, asset="PAST"),
    )
    assert daily_quality_report(tmp_path, report_day)["manifest_set_sha256"] != baseline


def test_write_rejects_a_leaf_symlink_escape_before_publishing_bytes(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    outside = tmp_path / "outside"
    lake.mkdir()
    outside.mkdir()
    venue_link = lake / "venue=hyperliquid"
    _directory_link(venue_link, outside)

    with pytest.raises(PartitionValidationError, match="outside data lake root"):
        write_partition(
            lake,
            PartitionKey("hyperliquid", DAY, "BTC", "candle"),
            _candle_table(),
        )

    assert list(outside.iterdir()) == []


def test_inventory_rejects_a_discovered_symlink_escape(tmp_path: Path) -> None:
    source = tmp_path / "source"
    write_partition(
        source,
        PartitionKey("hyperliquid", DAY, "BTC", "candle"),
        _candle_table(),
    )
    lake = tmp_path / "lake"
    lake.mkdir()
    _directory_link(lake / "venue=hyperliquid", source / "venue=hyperliquid")

    with pytest.raises(PartitionValidationError, match="outside data lake root"):
        inventory_partitions(lake)


def test_l2_delta_rejects_inconsistent_metadata_inside_one_update(tmp_path: Path) -> None:
    spec = schema_for("l2_delta")
    event_time = _utc(0)
    rows = []
    for side, price, epoch in (
        ("bid", Decimal("50000"), "epoch-1"),
        ("ask", Decimal("50001"), "epoch-2"),
    ):
        rows.append(
            {
                "schema_version": 1,
                "record_type": "l2_delta",
                "venue": "hyperliquid",
                "asset": "BTC",
                "event_time": event_time,
                "exchange_time": event_time,
                "received_time": event_time,
                "source_sequence": 100,
                "connection_id": "connection-1",
                "update_id": "same-update",
                "book_epoch_id": epoch,
                "first_sequence": 100,
                "last_sequence": 100,
                "side": side,
                "price": price,
                "quantity": Decimal("1"),
                "action": "set",
            }
        )
    table = pa.Table.from_pylist(rows, schema=spec.schema)

    with pytest.raises(PartitionValidationError, match="inconsistent L2 delta metadata"):
        write_partition(
            tmp_path,
            PartitionKey("hyperliquid", DAY, "BTC", "l2_delta"),
            table,
        )
    assert list(tmp_path.rglob("*.parquet")) == []


def test_past_daily_report_ignores_a_future_duplicate_partition(tmp_path: Path) -> None:
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "trade"),
        _trade_table(event_time=_utc(1), trade_id="same-id", sequence=1),
    )
    baseline = daily_quality_report(tmp_path, DAY)
    next_day = date(2026, 8, 12)
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", next_day, "BTC", "trade"),
        _trade_table(
            event_time=datetime(2026, 8, 12, 1, tzinfo=UTC),
            trade_id="same-id",
            sequence=2,
        ),
    )

    assert daily_quality_report(tmp_path, DAY) == baseline
    with pytest.raises(PartitionValidationError, match="duplicate primary keys"):
        inventory_partitions(tmp_path)


def test_past_daily_report_ignores_a_corrupted_future_partition(tmp_path: Path) -> None:
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", "trade"),
        _trade_table(event_time=_utc(1), trade_id="past", sequence=1),
    )
    baseline = daily_quality_report(tmp_path, DAY)
    next_day = date(2026, 8, 12)
    future = write_partition(
        tmp_path,
        PartitionKey("hyperliquid", next_day, "BTC", "trade"),
        _trade_table(
            event_time=datetime(2026, 8, 12, 1, tzinfo=UTC),
            trade_id="future",
            sequence=2,
        ),
    )
    future_path = tmp_path / future.relative_data_path
    corrupted = bytearray(future_path.read_bytes())
    corrupted[len(corrupted) // 2] ^= 0xFF
    future_path.write_bytes(corrupted)

    assert daily_quality_report(tmp_path, DAY) == baseline
    with pytest.raises(PartitionValidationError, match=r"CORRUPT_PARTITION \[hash_mismatch\]"):
        inventory_partitions(tmp_path)


def test_duckdb_catalog_has_separate_views_and_export_is_reproducible(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    table = _candle_table()
    manifest = write_partition(
        lake,
        PartitionKey("hyperliquid", DAY, "BTC", "candle"),
        table,
    )
    database = build_catalog(lake, tmp_path / "catalog.duckdb")

    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM candle").fetchone() == (2,)
        views = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.views WHERE table_schema = 'main'"
            ).fetchall()
        }
        assert "candle" in views
        assert "l2_snapshot" not in views
        assert "l2_delta" not in views

    output_a = tmp_path / "export-a.parquet"
    output_b = tmp_path / "export-b.parquet"
    first = export_dataset(
        lake,
        output_a,
        output_format="parquet",
        record_type="candle",
    )
    second = export_dataset(
        lake,
        output_b,
        output_format="parquet",
        record_type="candle",
    )
    assert first.sha256 == second.sha256
    assert first.source_hashes == second.source_hashes == (manifest.sha256,)
    exported = pq.read_table(output_a)
    assert exported.column("exchange_time").null_count == 1
    assert exported.column("quote_volume").null_count == 2

    csv_result = export_dataset(
        lake,
        tmp_path / "export.csv",
        output_format="csv",
        record_type="candle",
    )
    with csv_result.output.open(encoding="utf-8", newline="") as stream:
        csv_rows = list(csv.DictReader(stream))
    assert csv_rows[0]["exchange_time"] == ""
    assert csv_rows[0]["quote_volume"] == ""

    with pytest.raises(DataLakeError, match="already exists"):
        export_dataset(lake, output_a, output_format="parquet", record_type="candle")


def test_catalog_cannot_replace_an_immutable_lake_artifact(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    manifest = write_partition(
        lake,
        PartitionKey("hyperliquid", DAY, "BTC", "candle"),
        _candle_table(),
    )
    data_path = lake / manifest.relative_data_path
    original = data_path.read_bytes()

    with pytest.raises(DataLakeError, match="immutable lake artifact"):
        build_catalog(lake, data_path)

    assert data_path.read_bytes() == original
    assert validate_partition(data_path) == manifest
