from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import hyperlab.collector.storage as storage_module
import hyperlab.data.lake as lake_module
from hyperlab.api.public import PublicBootstrap
from hyperlab.collector.bootstrap import parse_bootstrap
from hyperlab.collector.models import ParsedRecord, WireEnvelope
from hyperlab.collector.parser import parse_websocket_message
from hyperlab.collector.storage import BatchingLakeSink
from hyperlab.data.lake import (
    PartitionKey,
    discover_partitions,
    inventory_partitions,
    validate_partition,
    write_partition,
)
from hyperlab.data.schema import RecordType, schema_for

NOW = datetime(2026, 8, 12, 12, tzinfo=UTC)


def _wire_record(sequence: int) -> ParsedRecord:
    parsed = parse_websocket_message(
        WireEnvelope(
            raw_message='{"channel":"pong"}',
            received_time=NOW,
            connection_id="local-test-connection",
            connection_epoch=1,
            arrival_sequence=sequence,
        )
    )
    assert len(parsed.records) == 1
    return parsed.records[0]


def test_real_zero_previous_day_reference_flushes_to_parquet(tmp_path: Path) -> None:
    fixture = Path(__file__).parent / "fixtures" / "hyperliquid" / "rest_spot_context_prev_day_zero.json"
    records = parse_bootstrap(
        PublicBootstrap(
            observed_at_ms=int(NOW.timestamp() * 1_000),
            perp_payload=[{"universe": []}, []],
            spot_payload=json.loads(fixture.read_text(encoding="utf-8")),
        )
    )
    sink = BatchingLakeSink(tmp_path / "lake", batch_size=10, queue_capacity=20)
    try:
        for record in records:
            sink.add(record)
        result = sink.flush()
    finally:
        sink.close()

    assert result.row_count == 2
    context_paths = list((tmp_path / "lake").rglob("type=market_context/*.parquet"))
    assert len(context_paths) == 1
    rows = pq.ParquetFile(context_paths[0]).read().to_pylist()
    assert len(rows) == 1
    assert rows[0]["asset"] == "@189"
    assert rows[0]["mark_price"] == Decimal("620.000000000000000000")
    assert rows[0]["previous_day_price"] == Decimal("0E-18")


def _bbo_record() -> ParsedRecord:
    parsed = parse_websocket_message(
        WireEnvelope(
            raw_message=(
                '{"channel":"bbo","data":{"coin":"BTC","time":1786536000000,'
                '"bbo":[{"px":"49999","sz":"2","n":1},'
                '{"px":"50001","sz":"3","n":1}]}}'
            ),
            received_time=NOW,
            connection_id="local-test-connection",
            connection_epoch=1,
            arrival_sequence=4,
        )
    )
    assert len(parsed.records) == 2
    return parsed.records[1]


def _trade_record(sequence: int) -> ParsedRecord:
    parsed = parse_websocket_message(
        WireEnvelope(
            raw_message=(
                '{"channel":"trades","data":[{"coin":"BTC","side":"B",'
                '"px":"50000","sz":"0.1","time":1786536000000,"tid":12345}]}'
            ),
            received_time=NOW,
            connection_id="local-test-connection",
            connection_epoch=1,
            arrival_sequence=sequence,
        )
    )
    records = [record for record in parsed.records if record.record_type.value == "trade"]
    assert len(records) == 1
    return records[0]


def test_batch_is_not_visible_before_flush_and_partition_publish_is_atomic(tmp_path: Path) -> None:
    sink = BatchingLakeSink(tmp_path / "lake", batch_size=2, queue_capacity=4)
    assert sink.add(_wire_record(2)) is True
    assert sink.add(_wire_record(1)) is True

    assert sink.should_flush is True
    assert list(tmp_path.rglob("*.parquet")) == []

    result = sink.flush()

    assert result.row_count == 2
    assert result.duplicate_count == 0
    assert len(result.manifests) == 1
    assert sink.pending_count == 0
    data_path = sink.root / result.manifests[0].relative_data_path
    assert validate_partition(data_path) == result.manifests[0]
    assert [path for path in tmp_path.rglob("*") if ".tmp" in path.name] == []

    table = pq.ParquetFile(data_path).read()
    assert table.column("arrival_sequence").to_pylist() == [1, 2]
    assert table.column("source_sequence").null_count == table.num_rows


def test_duplicate_keys_are_rejected_within_and_across_batches(tmp_path: Path) -> None:
    sink = BatchingLakeSink(tmp_path / "lake", batch_size=2, queue_capacity=4)
    record = _wire_record(1)

    assert sink.add(record) is True
    assert sink.add(record) is False
    first = sink.flush()
    assert (first.row_count, first.duplicate_count) == (1, 1)

    assert sink.add(record) is False
    second = sink.flush()
    assert (second.row_count, second.duplicate_count) == (0, 1)
    assert len(list(tmp_path.rglob("*.parquet"))) == 1


def test_bbo_without_a_public_server_sequence_can_be_batched(tmp_path: Path) -> None:
    sink = BatchingLakeSink(tmp_path / "lake", batch_size=2, queue_capacity=4)
    record = _bbo_record()
    assert record.row["source_sequence"] is None

    assert sink.add(record) is True
    result = sink.flush()

    assert result.row_count == 1
    table = pq.ParquetFile(sink.root / result.manifests[0].relative_data_path).read()
    assert table.column("source_sequence").null_count == 1


def test_queue_overflow_is_explicit_and_does_not_drop_pending_records(tmp_path: Path) -> None:
    sink = BatchingLakeSink(tmp_path / "lake", batch_size=2, queue_capacity=2)
    assert sink.add(_wire_record(1)) is True
    assert sink.add(_wire_record(2)) is True

    with pytest.raises(BufferError, match="no record was dropped"):
        sink.add(_wire_record(3))

    assert sink.pending_count == 2
    result = sink.flush()
    assert result.row_count == 2
    assert sink.pending_count == 0


def test_failed_atomic_publish_keeps_the_batch_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = BatchingLakeSink(tmp_path / "lake", batch_size=2, queue_capacity=2)
    sink.add(_wire_record(1))
    sink.add(_wire_record(2))

    def fail_publish(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("simulated publish failure")

    monkeypatch.setattr(storage_module, "write_partition", fail_publish)
    with pytest.raises(RuntimeError, match="simulated publish failure"):
        sink.flush()

    assert sink.pending_count == 2
    assert list(tmp_path.rglob("*.parquet")) == []


@pytest.mark.parametrize("persistent_dedup", [True, False])
def test_restart_recovers_manifest_and_inventories_parquet_after_publish_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    persistent_dedup: bool,
) -> None:
    root = tmp_path / "lake"
    sink = BatchingLakeSink(
        root,
        batch_size=2,
        queue_capacity=4,
        persistent_dedup=persistent_dedup,
    )
    sink.add(_wire_record(1))
    original_publish = lake_module._publish_exclusive
    calls = 0

    def fail_manifest_publish(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated crash before manifest publication")
        original_publish(*args, **kwargs)

    with monkeypatch.context() as context:
        context.setattr(lake_module, "_publish_exclusive", fail_manifest_publish)
        with pytest.raises(RuntimeError, match="before manifest"):
            sink.flush()

    assert len(list(root.rglob("part-*.parquet"))) == 1
    assert list(root.rglob("*.manifest.json")) == []
    sink.close()

    restarted = BatchingLakeSink(
        root,
        batch_size=2,
        queue_capacity=4,
        persistent_dedup=persistent_dedup,
    )
    try:
        recovered_data = list(root.rglob("part-*.parquet"))
        manifests = discover_partitions(root)
        assert len(recovered_data) == 1
        assert len(manifests) == 1
        manifest = validate_partition(manifests[0])
        assert manifest.row_count == 1
        assert pq.ParquetFile(recovered_data[0]).read().column("arrival_sequence").to_pylist() == [1]
        assert len(inventory_partitions(root).partitions) == 1
        assert not (root / ".recovery").exists()
        manifest_bytes = manifests[0].read_bytes()
    finally:
        restarted.close()

    restarted_again = BatchingLakeSink(
        root,
        batch_size=2,
        queue_capacity=4,
        persistent_dedup=persistent_dedup,
    )
    try:
        assert discover_partitions(root)[0].read_bytes() == manifest_bytes
    finally:
        restarted_again.close()


def test_root_writer_lock_is_non_blocking_and_released_on_close(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    first = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    try:
        with pytest.raises(RuntimeError, match="active writer"):
            BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    finally:
        first.close()

    second = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    second.close()


def test_failed_index_initialization_closes_sqlite_and_releases_writer_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"

    def fail_reconcile(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("simulated reconcile failure")

    with monkeypatch.context() as context:
        context.setattr(
            storage_module._PersistentObservationIndex,
            "_reconcile",
            fail_reconcile,
        )
        with pytest.raises(RuntimeError, match="reconcile failure"):
            BatchingLakeSink(root, batch_size=1, queue_capacity=2)

    index_path = root / ".collector-observations.sqlite3"
    index_path.unlink()
    recovered = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    recovered.close()


def test_reconcile_validates_indexed_manifest_data_pairs(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    sink = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    try:
        assert sink.add(_wire_record(1)) is True
        manifest = sink.flush().manifests[0]
    finally:
        sink.close()

    data_path = root / manifest.relative_data_path
    original = data_path.read_bytes()
    data_path.write_bytes(original + b"corrupt")
    with pytest.raises(ValueError, match="hash_mismatch"):
        BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    data_path.write_bytes(original)

    data_path.unlink()
    with pytest.raises(ValueError, match="partition data file not found"):
        BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    data_path.write_bytes(original)

    recovered = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    recovered.close()


def test_trade_primary_key_is_deduplicated_after_restart_and_index_rebuild(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    first = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    try:
        assert first.add(_trade_record(1)) is True
        first_flush = first.flush()
        assert first_flush.row_count == 1
    finally:
        first.close()

    second = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    try:
        assert second.add(_trade_record(2)) is False
    finally:
        second.close()

    (root / ".collector-observations.sqlite3").unlink()
    rebuilt = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    try:
        assert rebuilt.add(_trade_record(3)) is False
    finally:
        rebuilt.close()

    manifest = first_flush.manifests[0]
    (root / manifest.relative_manifest_path).unlink()
    (root / manifest.relative_data_path).unlink()
    empty_source = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    try:
        assert empty_source.add(_trade_record(4)) is True
        assert empty_source.flush().row_count == 1
    finally:
        empty_source.close()


def test_trade_primary_key_remains_deduplicated_across_v1_to_v2_migration(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    current = _trade_record(1)
    legacy_row = dict(current.row)
    legacy_row["schema_version"] = 1
    legacy_row.pop("connection_epoch")
    legacy_row.pop("arrival_sequence")
    write_partition(
        root,
        PartitionKey("hyperliquid", NOW.date(), "BTC", RecordType.TRADE),
        pa.Table.from_pylist(
            [legacy_row],
            schema=schema_for(RecordType.TRADE, version=1).schema,
        ),
    )

    sink = BatchingLakeSink(root, batch_size=1, queue_capacity=2)
    try:
        assert sink.add(_trade_record(2)) is False
        assert sink.flush().row_count == 0
    finally:
        sink.close()
