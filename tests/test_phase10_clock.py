from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from hyperlab.data.lake import (
    PartitionKey,
    PartitionValidationError,
    inventory_partitions,
    write_partition,
)
from hyperlab.data.schema import RecordType, schema_for
from hyperlab.venues.base import ClockMeasurement, measure_clock
from hyperlab.venues.binance import clock_record

BASE = datetime(2026, 8, 13, 12, tzinfo=UTC)
SAMPLING_INTERVAL = timedelta(seconds=10)
MAX_AGE = timedelta(seconds=15)
MAX_UNCERTAINTY_MS = Decimal('50')


def _measurement(
    *,
    response_delay: timedelta,
    request_sent_time: datetime = BASE,
) -> ClockMeasurement:
    response_received_time = request_sent_time + response_delay
    return measure_clock(
        'binance_usdm',
        request_sent_time=request_sent_time,
        response_received_time=response_received_time,
        server_time=request_sent_time + response_delay / 2,
    )


def _record(
    measurement: ClockMeasurement,
    observation_id: str,
    *,
    connection_id: str = 'binance-connection-7',
    connection_epoch: int = 7,
    capture_epoch_id: str = 'capture-epoch-7',
) -> dict[str, object]:
    return dict(
        clock_record(
            measurement,
            observation_id,
            connection_id=connection_id,
            connection_epoch=connection_epoch,
            capture_epoch_id=capture_epoch_id,
            sampling_interval=SAMPLING_INTERVAL,
            max_age=MAX_AGE,
            max_uncertainty_ms=MAX_UNCERTAINTY_MS,
        ).row
    )


def _table(row: dict[str, object], *, version: int = 2) -> pa.Table:
    return pa.Table.from_pylist(
        [row],
        schema=schema_for(RecordType.CLOCK_SYNC, version=version).schema,
    )


def _trade_table(*, version: int, trade_id: str, sequence: int) -> pa.Table:
    event_time = BASE + timedelta(seconds=sequence)
    row = {
        'schema_version': version,
        'record_type': RecordType.TRADE.value,
        'venue': 'binance_usdm',
        'asset': 'BTC',
        'event_time': event_time,
        'exchange_time': event_time,
        'received_time': event_time + timedelta(milliseconds=1),
        'source_sequence': sequence,
        'connection_id': 'market-connection',
        'trade_id': trade_id,
        'aggressor_side': 'buy',
        'price': Decimal('60000'),
        'quantity': Decimal('0.01'),
        'quote_quantity': Decimal('600'),
        'is_liquidation': False,
    }
    if version == 2:
        row.update(connection_epoch=3, arrival_sequence=sequence)
    return pa.Table.from_pylist(
        [row],
        schema=schema_for(RecordType.TRADE, version=version).schema,
    )


def test_uncertainty_at_50_ms_has_a_response_causal_bounded_interval() -> None:
    measurement = _measurement(response_delay=timedelta(milliseconds=100))

    row = _record(measurement, 'clock-at-threshold')

    assert measurement.drift_uncertainty_ms == MAX_UNCERTAINTY_MS
    assert row['schema_version'] == 2
    assert row['connection_id'] == 'binance-connection-7'
    assert row['connection_epoch'] == 7
    assert row['capture_epoch_id'] == 'capture-epoch-7'
    assert row['sample_status'] == 'valid'
    assert row['invalid_reason'] is None
    assert row['causal_valid_from'] == measurement.response_received_time
    assert row['causal_valid_until'] == measurement.response_received_time + MAX_AGE
    assert row['causal_valid_from'] != measurement.request_sent_time
    assert row['sampling_interval_ms'] == 10_000
    assert row['max_age_ms'] == 15_000
    assert row['max_uncertainty_ms'] == MAX_UNCERTAINTY_MS


def test_uncertainty_above_50_ms_is_explicitly_invalid_without_coverage() -> None:
    measurement = _measurement(response_delay=timedelta(milliseconds=102))

    row = _record(measurement, 'clock-over-threshold')

    assert measurement.drift_uncertainty_ms == Decimal('51')
    assert row['sample_status'] == 'invalid'
    assert row['causal_valid_from'] is None
    assert row['causal_valid_until'] is None
    assert row['invalid_reason']
    assert 'uncertainty' in str(row['invalid_reason']).lower()
    assert row['connection_id'] == 'binance-connection-7'
    assert row['connection_epoch'] == 7
    assert row['capture_epoch_id'] == 'capture-epoch-7'


def test_clock_validity_never_interpolates_across_stale_or_new_epoch_periods() -> None:
    first = _record(
        _measurement(response_delay=timedelta(milliseconds=20)),
        'clock-epoch-7',
    )
    second_measurement = _measurement(
        request_sent_time=BASE + timedelta(seconds=30),
        response_delay=timedelta(milliseconds=20),
    )
    second = _record(
        second_measurement,
        'clock-epoch-8',
        connection_id='binance-connection-8',
        connection_epoch=8,
        capture_epoch_id='capture-epoch-8',
    )

    assert first['causal_valid_until'] < second['causal_valid_from']
    assert first['connection_epoch'] != second['connection_epoch']
    assert first['capture_epoch_id'] != second['capture_epoch_id']
    uncovered = second['causal_valid_from'] - first['causal_valid_until']
    assert uncovered == timedelta(seconds=15)


def test_clock_sync_v1_remains_valid_beside_v2_partitions(tmp_path: Path) -> None:
    measurement = _measurement(response_delay=timedelta(milliseconds=20))
    v1_row = {
        'schema_version': 1,
        'record_type': RecordType.CLOCK_SYNC.value,
        'venue': 'binance_usdm',
        'asset': 'GLOBAL',
        'event_time': measurement.server_time,
        'exchange_time': measurement.server_time,
        'received_time': measurement.response_received_time,
        'source_sequence': None,
        'connection_id': None,
        'request_sent_time': measurement.request_sent_time,
        'response_received_time': measurement.response_received_time,
        'server_time': measurement.server_time,
        'round_trip_latency_ms': measurement.round_trip_latency_ms,
        'estimated_clock_drift_ms': measurement.estimated_clock_drift_ms,
        'drift_uncertainty_ms': measurement.drift_uncertainty_ms,
        'observation_id': 'legacy-clock-v1',
    }
    key = PartitionKey('binance_usdm', BASE.date(), 'GLOBAL', RecordType.CLOCK_SYNC)

    v1_manifest = write_partition(tmp_path, key, _table(v1_row, version=1))
    v2_manifest = write_partition(
        tmp_path,
        key,
        _table(_record(measurement, 'causal-clock-v2')),
    )

    assert v1_manifest.schema_version == 1
    assert v2_manifest.schema_version == 2
    inventory = inventory_partitions(tmp_path)
    assert sorted(item.schema_version for item in inventory.partitions) == [1, 2]
    assert inventory.total_rows == 2


def test_inventory_rejects_trade_duplicate_hidden_at_v1_v2_boundary(
    tmp_path: Path,
) -> None:
    key = PartitionKey('binance_usdm', BASE.date(), 'BTC', RecordType.TRADE)
    write_partition(
        tmp_path,
        key,
        _trade_table(version=1, trade_id='duplicate-trade', sequence=1),
    )
    write_partition(
        tmp_path,
        key,
        _trade_table(version=2, trade_id='duplicate-trade', sequence=2),
    )

    with pytest.raises(PartitionValidationError, match='duplicate primary keys'):
        inventory_partitions(tmp_path)


def test_inventory_reports_trade_sequence_gap_across_v1_v2_boundary(
    tmp_path: Path,
) -> None:
    key = PartitionKey('binance_usdm', BASE.date(), 'BTC', RecordType.TRADE)
    write_partition(
        tmp_path,
        key,
        _trade_table(version=1, trade_id='trade-1', sequence=1),
    )
    write_partition(
        tmp_path,
        key,
        _trade_table(version=2, trade_id='trade-3', sequence=3),
    )

    inventory = inventory_partitions(tmp_path)

    assert [
        (gap.kind, gap.start, gap.end, gap.missing_count)
        for _, gap in inventory.cross_segment_gaps
    ] == [('sequence', '1', '3', 1)]


def test_lake_accepts_threshold_valid_and_over_threshold_invalid_rows(tmp_path: Path) -> None:
    key = PartitionKey('binance_usdm', BASE.date(), 'GLOBAL', RecordType.CLOCK_SYNC)
    at_threshold = _record(
        _measurement(response_delay=timedelta(milliseconds=100)),
        'clock-valid-50ms',
    )
    over_threshold = _record(
        _measurement(response_delay=timedelta(milliseconds=102)),
        'clock-invalid-51ms',
    )

    valid_manifest = write_partition(tmp_path, key, _table(at_threshold))
    invalid_manifest = write_partition(tmp_path, key, _table(over_threshold))

    assert valid_manifest.row_count == 1
    assert invalid_manifest.row_count == 1
    assert inventory_partitions(tmp_path).total_rows == 2


@pytest.mark.parametrize(
    ('updates', 'message'),
    [
        (
            {
                'round_trip_latency_ms': Decimal('102'),
                'drift_uncertainty_ms': Decimal('51'),
            },
            'uncertainty',
        ),
        ({'causal_valid_from': BASE}, 'causal_valid_from'),
        (
            {'causal_valid_until': BASE + timedelta(seconds=99)},
            'causal_valid_until',
        ),
        ({'invalid_reason': 'fabricated failure'}, 'invalid_reason'),
        ({'connection_id': None}, 'connection_id'),
        ({'connection_epoch': None}, 'connection_epoch'),
        ({'connection_epoch': 0}, 'connection_epoch'),
        ({'capture_epoch_id': None}, 'capture_epoch_id'),
    ],
)
def test_lake_rejects_inconsistent_valid_clock_coverage(
    tmp_path: Path,
    updates: dict[str, object],
    message: str,
) -> None:
    row = _record(
        _measurement(response_delay=timedelta(milliseconds=20)),
        'malformed-valid-clock',
    )
    row.update(updates)

    with pytest.raises(PartitionValidationError, match=message):
        write_partition(
            tmp_path,
            PartitionKey('binance_usdm', BASE.date(), 'GLOBAL', RecordType.CLOCK_SYNC),
            _table(row),
        )


@pytest.mark.parametrize(
    ('updates', 'message'),
    [
        (
            {
                'causal_valid_from': BASE + timedelta(milliseconds=102),
                'causal_valid_until': BASE + timedelta(seconds=15, milliseconds=102),
            },
            'invalid',
        ),
        ({'invalid_reason': None}, 'invalid_reason'),
        ({'sample_status': 'unknown'}, 'sample_status'),
    ],
)
def test_lake_rejects_inconsistent_invalid_clock_coverage(
    tmp_path: Path,
    updates: dict[str, object],
    message: str,
) -> None:
    row = _record(
        _measurement(response_delay=timedelta(milliseconds=102)),
        'malformed-invalid-clock',
    )
    row.update(updates)

    with pytest.raises(PartitionValidationError, match=message):
        write_partition(
            tmp_path,
            PartitionKey('binance_usdm', BASE.date(), 'GLOBAL', RecordType.CLOCK_SYNC),
            _table(row),
        )
