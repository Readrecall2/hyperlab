from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

import hyperlab.data.lake as lake_module
from hyperlab.data.lake import (
    PartitionKey,
    PartitionValidationError,
    iter_hashed_batches,
    write_partition,
)
from hyperlab.data.schema import RecordType, schema_for

DAY = date(2026, 8, 15)
START = datetime(2026, 8, 15, 12, tzinfo=UTC)


def _bbo_table(row_count: int) -> pa.Table:
    rows = []
    for index in range(row_count):
        observed_at = START + timedelta(microseconds=index)
        sequence = index + 1
        rows.append(
            {
                "schema_version": 2,
                "record_type": RecordType.BBO.value,
                "venue": "binance_usdm",
                "asset": "BTC",
                "event_time": observed_at,
                "exchange_time": observed_at,
                "received_time": observed_at + timedelta(microseconds=1),
                "source_sequence": sequence,
                "connection_id": "public-1",
                "update_id": f"BTCUSDT:{sequence}",
                "bid_price": Decimal("100"),
                "bid_quantity": Decimal("2"),
                "ask_price": Decimal("101"),
                "ask_quantity": Decimal("3"),
            }
        )
    return pa.Table.from_pylist(rows, schema=schema_for(RecordType.BBO, version=2).schema)


def _write_bbo_partition(root: Path, *, row_count: int = 7):
    return write_partition(
        root,
        PartitionKey("binance_usdm", DAY, "BTC", RecordType.BBO),
        _bbo_table(row_count),
    )


def test_iter_hashed_batches_projects_columns_and_bounds_each_batch(tmp_path: Path) -> None:
    manifest = _write_bbo_partition(tmp_path)

    batches = list(
        iter_hashed_batches(
            tmp_path,
            manifest,
            columns=("received_time", "update_id"),
            batch_size=3,
        )
    )

    assert [batch.num_rows for batch in batches] == [3, 3, 1]
    assert all(batch.schema.names == ["received_time", "update_id"] for batch in batches)
    assert pa.Table.from_batches(batches).column("update_id").to_pylist() == [
        f"BTCUSDT:{sequence}" for sequence in range(1, 8)
    ]


@pytest.mark.parametrize("batch_size", [0, -1, True])
def test_iter_hashed_batches_rejects_non_positive_or_boolean_batch_size(
    tmp_path: Path,
    batch_size: int,
) -> None:
    manifest = _write_bbo_partition(tmp_path, row_count=1)

    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        list(iter_hashed_batches(tmp_path, manifest, batch_size=batch_size))


def test_iter_hashed_batches_fails_closed_on_hash_mismatch(tmp_path: Path) -> None:
    manifest = _write_bbo_partition(tmp_path, row_count=2)
    data_path = tmp_path / manifest.relative_data_path
    payload = bytearray(data_path.read_bytes())
    payload[len(payload) // 2] ^= 1
    data_path.write_bytes(payload)

    with pytest.raises(PartitionValidationError, match=r"CORRUPT_PARTITION \[hash_mismatch\]"):
        list(iter_hashed_batches(tmp_path, manifest, batch_size=1))


def test_iter_hashed_batches_fails_closed_on_size_mismatch(tmp_path: Path) -> None:
    manifest = _write_bbo_partition(tmp_path, row_count=2)
    wrong_size = replace(manifest, size_bytes=manifest.size_bytes + 1)

    with pytest.raises(PartitionValidationError, match="size mismatch"):
        list(iter_hashed_batches(tmp_path, wrong_size, batch_size=1))


def test_iter_hashed_batches_uses_one_descriptor_and_closes_it_early(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _write_bbo_partition(tmp_path, row_count=2)
    data_path = tmp_path / manifest.relative_data_path
    original_open = Path.open
    original_parquet_file = lake_module.pq.ParquetFile
    opened = []
    parquet_sources = []

    def tracking_open(path: Path, *args: object, **kwargs: object):
        stream = original_open(path, *args, **kwargs)
        if path == data_path:
            opened.append(stream)
        return stream

    def tracking_parquet_file(source: object, **kwargs: object):
        parquet_sources.append(source)
        return original_parquet_file(source, **kwargs)

    monkeypatch.setattr(Path, "open", tracking_open)
    monkeypatch.setattr(lake_module.pq, "ParquetFile", tracking_parquet_file)
    iterator = iter_hashed_batches(tmp_path, manifest, batch_size=1)

    first = next(iterator)
    assert first.num_rows == 1
    assert len(opened) == 1
    assert parquet_sources == opened
    assert not opened[0].closed

    iterator.close()
    assert opened[0].closed


def test_iter_hashed_batches_closes_parquet_file_when_batch_setup_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _write_bbo_partition(tmp_path, row_count=1)
    sources = []
    close_forces = []

    class FailingParquetFile:
        def __init__(self, source: object, **_kwargs: object) -> None:
            sources.append(source)

        def iter_batches(self, **_kwargs: object):
            raise RuntimeError("synthetic batch setup failure")

        def close(self, *, force: bool = False) -> None:
            close_forces.append(force)

    monkeypatch.setattr(lake_module.pq, "ParquetFile", FailingParquetFile)

    with pytest.raises(
        PartitionValidationError,
        match=r"invalid Parquet file .*synthetic batch setup failure",
    ):
        list(iter_hashed_batches(tmp_path, manifest, batch_size=1))

    assert len(sources) == 1
    assert close_forces == [True]
