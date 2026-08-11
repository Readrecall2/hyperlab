from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import hyperlab.data.catalog as catalog_module
from hyperlab.data.catalog import build_catalog, export_dataset
from hyperlab.data.lake import DataLakeError, InventoryReport, PartitionKey, write_partition
from hyperlab.data.schema import schema_for

DAY = date(2026, 8, 11)


def _candle_table(
    *,
    venue: str = "hyperliquid",
    asset: str = "BTC",
    times: tuple[datetime, ...],
    sequences: tuple[int, ...],
    price_offset: int = 0,
) -> pa.Table:
    rows: list[dict[str, object]] = []
    for index, (event_time, sequence) in enumerate(zip(times, sequences, strict=True)):
        price = Decimal(50_000 + price_offset + index)
        rows.append(
            {
                "schema_version": 1,
                "record_type": "candle",
                "venue": venue,
                "asset": asset,
                "event_time": event_time,
                "exchange_time": event_time,
                "received_time": event_time + timedelta(milliseconds=1),
                "source_sequence": sequence,
                "connection_id": "connection-1",
                "interval": "1h",
                "open_time": event_time - timedelta(hours=1),
                "close_time": event_time,
                "open": price,
                "high": price + 1,
                "low": price - 1,
                "close": price,
                "base_volume": Decimal("1"),
                "quote_volume": None,
                "trade_count": 1,
                "is_final": True,
            }
        )
    return pa.Table.from_pylist(rows, schema=schema_for("candle").schema)


def _time(day: date, hour: int) -> datetime:
    return datetime(day.year, day.month, day.day, hour, tzinfo=UTC)


def test_export_refuses_every_resolved_destination_inside_the_lake(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    event_time = _time(DAY, 0)
    write_partition(
        lake,
        PartitionKey("hyperliquid", DAY, "BTC", "candle"),
        _candle_table(times=(event_time,), sequences=(1,)),
    )
    destination = lake / "exports" / "nested" / ".." / "candles.parquet"

    with pytest.raises(DataLakeError, match="inside immutable data lake"):
        export_dataset(lake, destination, record_type="candle")

    assert not (lake / "exports").exists()


def test_multisegment_export_has_explicit_dimension_and_record_order(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    next_day = date(2026, 8, 12)
    expected_rows = [
        ("alpha", DAY.isoformat(), "BTC", _time(DAY, 0)),
        ("alpha", DAY.isoformat(), "BTC", _time(DAY, 2)),
        ("alpha", DAY.isoformat(), "SOL", _time(DAY, 1)),
        ("alpha", next_day.isoformat(), "ETH", _time(next_day, 0)),
        ("zeta", DAY.isoformat(), "BTC", _time(DAY, 0)),
    ]
    inputs = [
        ("zeta", DAY, "BTC", _time(DAY, 0), 50),
        ("alpha", next_day, "ETH", _time(next_day, 0), 40),
        ("alpha", DAY, "SOL", _time(DAY, 1), 30),
        # Same physical partition, deliberately written in reverse event order.
        ("alpha", DAY, "BTC", _time(DAY, 2), 20),
        ("alpha", DAY, "BTC", _time(DAY, 0), 10),
    ]
    manifests = []
    for venue, partition_date, asset, event_time, sequence in inputs:
        manifests.append(
            write_partition(
                lake,
                PartitionKey(venue, partition_date, asset, "candle"),
                _candle_table(
                    venue=venue,
                    asset=asset,
                    times=(event_time,),
                    sequences=(sequence,),
                    price_offset=sequence,
                ),
            )
        )

    first = export_dataset(
        lake,
        tmp_path / "first.parquet",
        record_type="candle",
        schema_version=1,
    )
    second = export_dataset(
        lake,
        tmp_path / "second.parquet",
        record_type="candle",
        schema_version=1,
    )

    exported = pq.read_table(first.output)
    observed_rows = [
        (venue, event_time.date().isoformat(), asset, event_time)
        for venue, asset, event_time in zip(
            exported.column("venue").to_pylist(),
            exported.column("asset").to_pylist(),
            exported.column("event_time").to_pylist(),
            strict=True,
        )
    ]
    assert observed_rows == expected_rows
    assert first.sha256 == second.sha256
    assert first.source_hashes == second.source_hashes == tuple(
        sorted(manifest.sha256 for manifest in manifests)
    )
    assert first.filters["schema_version"] == 1


def test_export_hashes_the_exact_bytes_it_decodes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = tmp_path / "lake"
    event_time = _time(DAY, 0)
    manifest = write_partition(
        lake,
        PartitionKey("hyperliquid", DAY, "BTC", "candle"),
        _candle_table(times=(event_time,), sequences=(1,), price_offset=0),
    )
    source = lake / manifest.relative_data_path
    original_bytes = source.read_bytes()

    replacement_path = tmp_path / "replacement.parquet"
    pq.write_table(
        _candle_table(times=(event_time,), sequences=(1,), price_offset=999),
        replacement_path,
    )
    replacement_bytes = replacement_path.read_bytes()
    assert hashlib.sha256(replacement_bytes).hexdigest() != manifest.sha256

    real_read_bytes = Path.read_bytes
    real_inventory = catalog_module.inventory_partitions
    inventory_finished = False
    read_source = False

    def inventory_then_enable_race(root: Path) -> InventoryReport:
        nonlocal inventory_finished
        result = real_inventory(root)
        inventory_finished = True
        return result

    def read_then_replace(path: Path) -> bytes:
        nonlocal read_source
        payload = real_read_bytes(path)
        if inventory_finished and not read_source and path.resolve() == source.resolve():
            read_source = True
            source.write_bytes(replacement_bytes)
        return payload

    monkeypatch.setattr(catalog_module, "inventory_partitions", inventory_then_enable_race)
    monkeypatch.setattr(Path, "read_bytes", read_then_replace)

    result = export_dataset(lake, tmp_path / "export.parquet", record_type="candle")

    assert read_source is True
    assert result.source_hashes == (hashlib.sha256(original_bytes).hexdigest(),)
    assert result.filters["schema_version"] == 1
    assert pq.read_table(result.output).column("open").to_pylist() == [Decimal("50000")]
    assert source.read_bytes() == replacement_bytes


def test_schema_version_is_an_explicit_export_filter(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    event_time = _time(DAY, 0)
    write_partition(
        lake,
        PartitionKey("hyperliquid", DAY, "BTC", "candle"),
        _candle_table(times=(event_time,), sequences=(1,)),
    )

    with pytest.raises(DataLakeError, match="no validated partitions"):
        export_dataset(
            lake,
            tmp_path / "unknown-version.parquet",
            record_type="candle",
            schema_version=2,
        )
    with pytest.raises(DataLakeError, match="schema_version must be positive"):
        export_dataset(
            lake,
            tmp_path / "invalid-version.parquet",
            record_type="candle",
            schema_version=0,
        )


def test_catalog_materializes_the_verified_bytes_instead_of_lazy_source_paths(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    event_time = _time(DAY, 0)
    manifest = write_partition(
        lake,
        PartitionKey("hyperliquid", DAY, "BTC", "candle"),
        _candle_table(times=(event_time,), sequences=(1,), price_offset=0),
    )
    database = build_catalog(lake, tmp_path / "catalog.duckdb")

    # Keep a valid Parquet schema but replace the audited content after the build.
    pq.write_table(
        _candle_table(times=(event_time,), sequences=(1,), price_offset=999),
        lake / manifest.relative_data_path,
    )

    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.execute('SELECT open FROM "candle"').fetchall() == [
            (Decimal("50000"),)
        ]
        assert connection.execute(
            "SELECT sha256 FROM hyperlab_partitions"
        ).fetchall() == [(manifest.sha256,)]
        assert connection.execute(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = 'candle_v1'"
        ).fetchone() == ("BASE TABLE",)
