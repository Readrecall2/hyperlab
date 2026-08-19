from __future__ import annotations

import contextlib
import json
import multiprocessing as mp
import os
import signal
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pyarrow.parquet as pq
import pytest

from hyperlab.api.public import PublicBootstrap
from hyperlab.collector.bootstrap import (
    historical_envelope,
    parse_bbo_from_l2,
    parse_bootstrap,
    parse_candles,
    parse_funding_history,
    parse_l2_snapshot,
)
from hyperlab.collector.models import ParsedRecord, WireEnvelope
from hyperlab.collector.storage import (
    BatchingLakeSink,
    CoordinatedWriterError,
    FlushResult,
)
from hyperlab.collector.writer_process import (
    CoordinatedWriterProcess,
    ProcessWriterError,
)
from hyperlab.collector.writer_worker import WriterQueueCapacityError
from hyperlab.data.lake import (
    PartitionKey,
    PartitionManifest,
    discover_partitions,
    validate_partition,
)
from hyperlab.data.schema import RecordType
from hyperlab.venues.base import measure_clock
from hyperlab.venues.binance import BinancePublicConnector, clock_record

BASE = datetime(2026, 8, 14, tzinfo=UTC)
VENUES = ("hyperliquid", "binance_usdm")
BINANCE_STRESS_CAPTURE = "binance-stress-capture-1"
HL_BOOTSTRAP_CONNECTION_ID = "hyperliquid-synthetic-rest-bootstrap"
HL_PERSISTED_ASSETS = ("BTC", "ETH")
SYNTHETIC_NON_ECONOMIC_PERP_MARKETS = 300
SYNTHETIC_NON_ECONOMIC_SPOT_MARKETS = 250
SYNTHETIC_HISTORY_MINUTES = 60
_DURATION_SUMMARY_FIELDS = {
    "count",
    "window_count",
    "min_ms",
    "mean_ms",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
}


def _group_diagnostics(snapshot: dict[str, object]) -> list[dict[str, object]]:
    diagnostics = snapshot["group_diagnostics"]
    assert isinstance(diagnostics, dict)
    assert set(diagnostics) == {
        "schema_version",
        "dimensions",
        "accounting_status",
        "capacity",
        "semantics",
        "groups",
    }
    assert diagnostics["schema_version"] == 1
    assert diagnostics["dimensions"] == ["venue", "asset", "record_type"]
    assert diagnostics["accounting_status"] == snapshot["accounting_status"]
    capacity = diagnostics["capacity"]
    assert isinstance(capacity, dict)
    assert set(capacity) == {"max_groups", "current_groups", "rejections"}
    assert capacity["max_groups"] == snapshot["queue_capacity_rows"]
    assert isinstance(capacity["current_groups"], int)
    assert isinstance(capacity["rejections"], int)
    assert diagnostics["semantics"] == {
        "rows": {
            "enqueued": "parent_admitted",
            "acknowledged": "child_processed_including_duplicates",
            "durable": "parent_observed_manifest_rows",
        },
        "duplicate_attribution": "venue_only",
        "flushes": (
            "parent_observed_flush_events_with_at_least_one_output_manifest_for_group"
        ),
        "queue_residence": {
            "interval": "parent_enqueue_to_child_dequeue",
            "row_scope": "child_processed_including_duplicates",
            "frame_samples": "one_per_group_per_child_processed_frame",
            "lifetime_fields": ["count", "min_ms", "mean_ms", "max_ms"],
            "windowed_fields": ["p50_ms", "p95_ms", "p99_ms"],
            "percentile_window_samples": 4_096,
        },
    }
    groups = diagnostics["groups"]
    assert isinstance(groups, list)
    assert all(isinstance(group, dict) for group in groups)
    typed_groups = [group for group in groups if isinstance(group, dict)]
    keys = [
        (
            str(group["venue"]),
            str(group["asset"]),
            str(group["record_type"]),
        )
        for group in typed_groups
    ]
    assert keys == sorted(keys)
    assert capacity["current_groups"] == len(typed_groups)
    return typed_groups


def _assert_group_contribution_consistency(group: dict[str, object]) -> None:
    rows = group["rows"]
    assert isinstance(rows, dict)
    assert set(rows) == {"enqueued", "acknowledged", "durable"}
    assert all(isinstance(value, int) and not isinstance(value, bool) for value in rows.values())

    output_files = group["output_files"]
    assert isinstance(output_files, int) and not isinstance(output_files, bool)
    average = group["average_rows_per_output_file"]
    if output_files == 0:
        assert average is None
    else:
        assert isinstance(average, int | float) and not isinstance(average, bool)
        assert average == pytest.approx(rows["durable"] / output_files)

    flush = group["flush_contribution"]
    assert isinstance(flush, dict)
    assert set(flush) == {
        "flushes",
        "rows",
        "output_files",
        "row_fraction",
        "file_fraction",
    }
    assert flush["rows"] == rows["durable"]
    assert flush["output_files"] == output_files

    residence = group["queue_residence_contribution"]
    assert isinstance(residence, dict)
    assert set(residence) == {
        "frames",
        "rows",
        "row_milliseconds",
        "row_weighted_mean_ms",
        "frame_residence_ms",
    }
    assert residence["rows"] == rows["acknowledged"]
    assert isinstance(residence["row_milliseconds"], int | float)
    assert isinstance(residence["row_weighted_mean_ms"], int | float)
    assert residence["row_milliseconds"] == pytest.approx(
        residence["rows"] * residence["row_weighted_mean_ms"]
    )
    frame_summary = residence["frame_residence_ms"]
    assert isinstance(frame_summary, dict)
    assert set(frame_summary) == _DURATION_SUMMARY_FIELDS
    assert frame_summary["count"] == residence["frames"]


def _diagnostic_manifest(
    *,
    venue: str,
    asset: str = "BTC",
    row_count: int,
) -> PartitionManifest:
    return PartitionManifest(
        partition=PartitionKey(
            venue=venue,
            date=BASE.date(),
            asset=asset,
            record_type=RecordType.L2_SNAPSHOT,
        ),
        data_file="synthetic-protocol-test.parquet",
        sha256="0" * 64,
        size_bytes=1,
        row_count=row_count,
        timestamp_bounds={},
        schema_name=RecordType.L2_SNAPSHOT.value,
        schema_version=1,
        schema_fingerprint="0" * 64,
        stream_key="synthetic-protocol-test",
        sequence_min=None,
        sequence_max=None,
        duplicates=0,
        out_of_order=0,
        gaps=(),
        gap_detection="not_applicable",
        null_counts={},
        quality="ok",
    )


def _parent_holding_writer(root: str, ready_queue: Any) -> None:
    worker = CoordinatedWriterProcess(
        Path(root),
        venues=("hyperliquid",),
        batch_size=2,
        queue_capacity=2,
    )
    child = worker.metrics_snapshot()["child_process"]
    assert isinstance(child, dict)
    ready_queue.put(child["pid"])
    while True:
        time.sleep(0.1)


def _l2_frame(
    venue: str,
    snapshot_index: int,
    *,
    asset: str = "BTC",
    levels_per_side: int = 20,
) -> tuple[ParsedRecord, ...]:
    event_time = BASE + timedelta(microseconds=snapshot_index)
    snapshot_id = f"{venue}-snapshot-{snapshot_index}"
    records: list[ParsedRecord] = []
    for side in ("bid", "ask"):
        for level in range(levels_per_side):
            offset = Decimal(level) / Decimal("10")
            price = Decimal("60000") - offset if side == "bid" else Decimal("60000") + offset
            records.append(
                ParsedRecord(
                    RecordType.L2_SNAPSHOT,
                    asset,
                    {
                        "schema_version": 1,
                        "record_type": RecordType.L2_SNAPSHOT.value,
                        "venue": venue,
                        "asset": asset,
                        "event_time": event_time,
                        "exchange_time": event_time,
                        "received_time": event_time + timedelta(milliseconds=1),
                        "source_sequence": snapshot_index,
                        "connection_id": f"{venue}-connection",
                        "snapshot_id": snapshot_id,
                        "book_epoch_id": f"{venue}-book-1",
                        "last_sequence": snapshot_index,
                        "side": side,
                        "level": level,
                        "price": price,
                        "quantity": Decimal("1.25"),
                        "order_count": None,
                    },
                )
            )
    return tuple(records)


def _trade(venue: str, sequence: int) -> ParsedRecord:
    event_time = BASE + timedelta(microseconds=sequence)
    return ParsedRecord(
        RecordType.TRADE,
        "BTC",
        {
            "schema_version": 2,
            "record_type": RecordType.TRADE.value,
            "venue": venue,
            "asset": "BTC",
            "event_time": event_time,
            "exchange_time": event_time,
            "received_time": event_time + timedelta(milliseconds=1),
            "source_sequence": sequence,
            "connection_id": f"{venue}-trade-connection",
            "trade_id": f"{venue}-trade-{sequence}",
            "aggressor_side": "buy",
            "price": Decimal("60000"),
            "quantity": Decimal("0.001"),
            "quote_quantity": Decimal("60"),
            "is_liquidation": None,
            "connection_epoch": 1,
            "arrival_sequence": sequence + 1,
        },
    )


def _binance_connector() -> BinancePublicConnector:
    symbols = []
    for asset in ("BTC", "ETH"):
        symbols.append(
            {
                "symbol": f"{asset}USDT",
                "pair": f"{asset}USDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "baseAsset": asset,
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                ],
            }
        )
    return BinancePublicConnector.from_exchange_info({"symbols": symbols}, ("BTC", "ETH"))


def _binance_depth_frame(
    connector: BinancePublicConnector,
    snapshot_index: int,
    asset: str,
) -> tuple[ParsedRecord, ...]:
    symbol = f"{asset}USDT"
    event_ms = int(BASE.timestamp() * 1_000) + snapshot_index
    last_sequence = 10_000 + snapshot_index
    payload = {
        "stream": f"{asset.lower()}usdt@depth20@100ms",
        "data": {
            "e": "depthUpdate",
            "E": event_ms,
            "T": event_ms,
            "s": symbol,
            "U": last_sequence,
            "u": last_sequence,
            "pu": last_sequence - 1,
            "b": [[str(60_000 - level), "1.25"] for level in range(20)],
            "a": [[str(60_001 + level), "1.25"] for level in range(20)],
        },
    }
    received_time = BASE + timedelta(microseconds=snapshot_index)
    envelope = WireEnvelope(
        json.dumps(payload, separators=(",", ":")),
        received_time,
        "binance-public-depth-stress",
        1,
        snapshot_index + 1,
        BINANCE_STRESS_CAPTURE,
    )
    parsed = connector.parse_message(envelope)
    assert len(parsed.records) == 43
    return parsed.records


def _binance_clock_sample(sample_index: int) -> ParsedRecord:
    request_sent_time = BASE + timedelta(seconds=sample_index * 5)
    response_received_time = request_sent_time + timedelta(milliseconds=80)
    measurement = measure_clock(
        "binance_usdm",
        request_sent_time=request_sent_time,
        response_received_time=response_received_time,
        server_time=request_sent_time + timedelta(milliseconds=40),
    )
    return clock_record(
        measurement,
        f"binance-clock-{sample_index}",
        connection_id="binance-public-depth-stress",
        connection_epoch=1,
        capture_epoch_id=BINANCE_STRESS_CAPTURE,
        sampling_interval=timedelta(seconds=5),
    )


def _synthetic_hyperliquid_full_universe_bootstrap() -> tuple[ParsedRecord, ...]:
    """Parse a production-shaped, explicitly synthetic/non-economic venue universe."""

    perp_assets = (
        *HL_PERSISTED_ASSETS,
        *(
            f"SYNTHETIC_NON_ECONOMIC_PERP_{index:03d}"
            for index in range(SYNTHETIC_NON_ECONOMIC_PERP_MARKETS - len(HL_PERSISTED_ASSETS))
        ),
    )
    perp_universe = [
        {
            "name": asset,
            "szDecimals": 5,
            "maxLeverage": 1,
            "marginTableId": 0,
        }
        for asset in perp_assets
    ]
    perp_contexts = [
        {
            "dayBaseVlm": "0",
            "dayNtlVlm": "0",
            "funding": "0",
            "markPx": "1",
            "midPx": "1",
            "openInterest": "0",
            "oraclePx": "1",
            "prevDayPx": "1",
        }
        for _asset in perp_assets
    ]

    spot_tokens: list[dict[str, object]] = [
        {
            "name": "USDC",
            "szDecimals": 8,
            "weiDecimals": 8,
            "index": 0,
            "tokenId": "0x00000000000000000000000000000000",
            "isCanonical": True,
            "evmContract": None,
            "fullName": "Synthetic USD",
        }
    ]
    spot_universe: list[dict[str, object]] = []
    spot_contexts: list[dict[str, object]] = []
    for offset in range(SYNTHETIC_NON_ECONOMIC_SPOT_MARKETS):
        token_index = offset + 1
        pair_index = 1_000 + offset
        token_name = f"SYNTHETIC_NON_ECONOMIC_SPOT_{offset:03d}"
        spot_tokens.append(
            {
                "name": token_name,
                "szDecimals": 5,
                "weiDecimals": 8,
                "index": token_index,
                "tokenId": f"0x{token_index:032x}",
                "isCanonical": False,
                "evmContract": None,
                "fullName": token_name,
            }
        )
        spot_universe.append(
            {
                "name": f"{token_name}/USDC",
                "tokens": [token_index, 0],
                "index": pair_index,
                "isCanonical": False,
            }
        )
        spot_contexts.append(
            {
                "coin": f"@{pair_index}",
                "dayBaseVlm": "0",
                "dayNtlVlm": "0",
                "markPx": "1",
                "midPx": "1",
                "prevDayPx": "1",
                "circulatingSupply": "0",
            }
        )

    bootstrap = PublicBootstrap(
        observed_at_ms=int(BASE.timestamp() * 1_000),
        perp_payload=[{"universe": perp_universe}, perp_contexts],
        spot_payload=[
            {"tokens": spot_tokens, "universe": spot_universe},
            spot_contexts,
        ],
    )
    return parse_bootstrap(
        bootstrap,
        connection_id=HL_BOOTSTRAP_CONNECTION_ID,
        connection_epoch=1,
    )


def _synthetic_hyperliquid_one_hour_history() -> tuple[ParsedRecord, ...]:
    """Build public REST history through production parsers; values are non-economic."""

    records: list[ParsedRecord] = []
    arrival_sequence = 2
    for asset_index, asset in enumerate(HL_PERSISTED_ASSETS):
        funding_received = BASE + timedelta(milliseconds=arrival_sequence)
        funding_envelope = historical_envelope(
            funding_received,
            connection_id=HL_BOOTSTRAP_CONNECTION_ID,
            connection_epoch=1,
            arrival_sequence=arrival_sequence,
        )
        arrival_sequence += 1
        records.extend(
            parse_funding_history(
                [
                    {
                        "coin": asset,
                        "fundingRate": "0",
                        "premium": "0",
                        "time": int((BASE - timedelta(hours=1)).timestamp() * 1_000),
                    }
                ],
                funding_envelope,
            )
        )

        candle_received = BASE + timedelta(milliseconds=arrival_sequence)
        candle_envelope = historical_envelope(
            candle_received,
            connection_id=HL_BOOTSTRAP_CONNECTION_ID,
            connection_epoch=1,
            arrival_sequence=arrival_sequence,
        )
        arrival_sequence += 1
        candle_payload = []
        for minute in range(SYNTHETIC_HISTORY_MINUTES):
            open_time = BASE - timedelta(minutes=SYNTHETIC_HISTORY_MINUTES - minute)
            open_ms = int(open_time.timestamp() * 1_000)
            candle_payload.append(
                {
                    "t": open_ms,
                    "T": open_ms + 59_999,
                    "s": asset,
                    "i": "1m",
                    "o": "1",
                    "c": "1",
                    "h": "1",
                    "l": "1",
                    "v": "0",
                    "n": 0,
                }
            )
        records.extend(parse_candles(candle_payload, candle_envelope))

        l2_received = BASE + timedelta(
            milliseconds=arrival_sequence + asset_index,
        )
        l2_envelope = historical_envelope(
            l2_received,
            connection_id=HL_BOOTSTRAP_CONNECTION_ID,
            connection_epoch=1,
            arrival_sequence=arrival_sequence,
        )
        arrival_sequence += 1
        l2_payload = {
            "coin": asset,
            "time": int(l2_received.timestamp() * 1_000),
            "levels": [
                [
                    {
                        "px": str(100 - level),
                        "sz": "1",
                        "n": 1,
                    }
                    for level in range(20)
                ],
                [
                    {
                        "px": str(101 + level),
                        "sz": "1",
                        "n": 1,
                    }
                    for level in range(20)
                ],
            ],
        }
        records.extend(parse_l2_snapshot(l2_payload, l2_envelope))
        records.extend(parse_bbo_from_l2(l2_payload, l2_envelope))
    return tuple(records)


def test_process_writer_coalesces_sustained_multi_group_frames_before_publication(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    worker = CoordinatedWriterProcess(
        root,
        venues=VENUES,
        batch_size=500,
        queue_capacity=20_000,
        venue_capacity_rows={
            "hyperliquid": 10_000,
            "binance_usdm": 10_000,
        },
    )
    hyperliquid = worker.client("hyperliquid")
    binance = worker.client("binance_usdm")
    assert hyperliquid.should_flush is False
    assert binance.should_flush is False
    connector = _binance_connector()
    full_universe_bootstrap = _synthetic_hyperliquid_full_universe_bootstrap()
    assert len(full_universe_bootstrap) == 1_100
    assert len({record.asset for record in full_universe_bootstrap}) == 550
    persisted_bootstrap = tuple(
        record
        for record in full_universe_bootstrap
        if record.asset in HL_PERSISTED_ASSETS
    )
    assert Counter(record.record_type for record in persisted_bootstrap) == Counter(
        {RecordType.INSTRUMENT_METADATA: 2, RecordType.MARKET_CONTEXT: 2}
    )
    assert {record.asset for record in persisted_bootstrap} == set(HL_PERSISTED_ASSETS)
    history = _synthetic_hyperliquid_one_hour_history()
    assert len(history) == 206
    startup_records = (*persisted_bootstrap, *history)
    assert len(startup_records) == 210

    try:
        assert hyperliquid.add_many(startup_records) == 210
        # Production issues a full FIFO durability barrier immediately after
        # REST bootstrap. It is deliberately queued before either live producer.
        assert hyperliquid.request_flush() is True
        cadence = threading.Barrier(2)

        def ingest_hyperliquid() -> int:
            admitted = 0
            try:
                for snapshot_index in range(452):
                    cadence.wait(timeout=10)
                    admitted += int(hyperliquid.add(_trade("hyperliquid", snapshot_index)))
                    cadence.wait(timeout=10)
            except BaseException:
                cadence.abort()
                raise
            return admitted

        def ingest_binance() -> int:
            admitted = 0
            try:
                for snapshot_index in range(452):
                    cadence.wait(timeout=10)
                    asset = "BTC" if snapshot_index % 2 == 0 else "ETH"
                    admitted += binance.add_many(
                        _binance_depth_frame(connector, snapshot_index, asset)
                    )
                    if snapshot_index % 100 == 0:
                        admitted += int(
                            binance.add(_binance_clock_sample(snapshot_index // 100))
                        )
                    if (snapshot_index + 1) % 100 == 0:
                        assert binance.request_flush() is True
                    cadence.wait(timeout=10)
                    time.sleep(0.01)
            except BaseException:
                cadence.abort()
                raise
            return admitted

        with ThreadPoolExecutor(
            max_workers=2,
            thread_name_prefix="cold-start-ingress",
        ) as executor:
            hyperliquid_future = executor.submit(ingest_hyperliquid)
            binance_future = executor.submit(ingest_binance)
            assert hyperliquid_future.result(timeout=30) == 452
            assert binance_future.result(timeout=30) == 19_441

        result = binance.flush()
        peer_result = hyperliquid.collect_completed()
        assert result.row_count == 19_441
        assert peer_result.row_count == 662

        snapshot = worker.metrics_snapshot()
        assert snapshot["failure"] is None
        assert snapshot["outstanding_rows"] == 0
        assert snapshot["outstanding_high_water_rows"] < 8_000
        venues = snapshot["venues"]
        assert isinstance(venues, dict)
        assert venues["binance_usdm"]["capacity_rows"] == 10_000
        assert venues["binance_usdm"]["capacity_rejections"] == 0
        assert venues["binance_usdm"]["high_water_rows"] < 7_000
        assert venues["binance_usdm"]["frames_processed"] == 457
        assert venues["binance_usdm"]["durable_rows"] == 19_441
        assert venues["binance_usdm"]["queue_residence_ms"]["count"] == 457
        assert venues["binance_usdm"]["queue_residence_ms"]["max_ms"] < 5_000
        assert venues["hyperliquid"]["capacity_rows"] == 10_000
        assert venues["hyperliquid"]["capacity_rejections"] == 0
        assert venues["hyperliquid"]["frames_processed"] == 453
        assert venues["hyperliquid"]["durable_rows"] == 662
        storage = snapshot["storage"]
        assert isinstance(storage, dict)
        assert storage["coalescing"] == {
            "readiness": "exact_group",
            "pending_groups": 0,
            "ready_groups": 0,
            "max_group_rows": 0,
        }
        assert storage["written"] == {
            "rows": 20_103,
            "partitions": 85,
        }
        diagnostic_groups = _group_diagnostics(snapshot)
        expected_diagnostics = {
            ("binance_usdm", "BTC", RecordType.BBO.value): (226, 5, 226),
            ("binance_usdm", "BTC", RecordType.L2_BOOK_STATE.value): (226, 5, 226),
            ("binance_usdm", "BTC", RecordType.L2_SNAPSHOT.value): (9_040, 18, 226),
            ("binance_usdm", "ETH", RecordType.BBO.value): (226, 5, 226),
            ("binance_usdm", "ETH", RecordType.L2_BOOK_STATE.value): (226, 5, 226),
            ("binance_usdm", "ETH", RecordType.L2_SNAPSHOT.value): (9_040, 18, 226),
            ("binance_usdm", "GLOBAL", RecordType.CLOCK_SYNC.value): (5, 5, 5),
            ("binance_usdm", "GLOBAL", RecordType.WIRE_MESSAGE.value): (452, 5, 452),
            ("hyperliquid", "BTC", RecordType.BBO.value): (1, 1, 1),
            ("hyperliquid", "BTC", RecordType.CANDLE.value): (60, 1, 1),
            ("hyperliquid", "BTC", RecordType.FUNDING.value): (1, 1, 1),
            ("hyperliquid", "BTC", RecordType.INSTRUMENT_METADATA.value): (1, 1, 1),
            ("hyperliquid", "BTC", RecordType.L2_BOOK_STATE.value): (1, 1, 1),
            ("hyperliquid", "BTC", RecordType.L2_SNAPSHOT.value): (40, 1, 1),
            ("hyperliquid", "BTC", RecordType.MARKET_CONTEXT.value): (1, 1, 1),
            ("hyperliquid", "BTC", RecordType.TRADE.value): (452, 5, 452),
            ("hyperliquid", "ETH", RecordType.BBO.value): (1, 1, 1),
            ("hyperliquid", "ETH", RecordType.CANDLE.value): (60, 1, 1),
            ("hyperliquid", "ETH", RecordType.FUNDING.value): (1, 1, 1),
            ("hyperliquid", "ETH", RecordType.INSTRUMENT_METADATA.value): (1, 1, 1),
            ("hyperliquid", "ETH", RecordType.L2_BOOK_STATE.value): (1, 1, 1),
            ("hyperliquid", "ETH", RecordType.L2_SNAPSHOT.value): (40, 1, 1),
            ("hyperliquid", "ETH", RecordType.MARKET_CONTEXT.value): (1, 1, 1),
        }
        assert len(diagnostic_groups) == len(expected_diagnostics)
        observed_row_fraction = 0.0
        observed_file_fraction = 0.0
        for group in diagnostic_groups:
            _assert_group_contribution_consistency(group)
            key = (
                str(group["venue"]),
                str(group["asset"]),
                str(group["record_type"]),
            )
            expected_rows, expected_files, expected_frames = expected_diagnostics[key]
            assert group["rows"] == {
                "enqueued": expected_rows,
                "acknowledged": expected_rows,
                "durable": expected_rows,
            }
            assert group["output_files"] == expected_files
            assert group["average_rows_per_output_file"] == pytest.approx(
                expected_rows / expected_files
            )

            flush = group["flush_contribution"]
            assert isinstance(flush, dict)
            assert flush["flushes"] == expected_files
            assert flush["rows"] == expected_rows
            assert flush["output_files"] == expected_files
            assert flush["row_fraction"] == pytest.approx(expected_rows / 20_103)
            assert flush["file_fraction"] == pytest.approx(expected_files / 85)
            observed_row_fraction += float(flush["row_fraction"])
            observed_file_fraction += float(flush["file_fraction"])

            residence = group["queue_residence_contribution"]
            assert isinstance(residence, dict)
            assert residence["frames"] == expected_frames
            assert residence["rows"] == expected_rows
            frame_summary = residence["frame_residence_ms"]
            assert isinstance(frame_summary, dict)
            assert frame_summary["count"] == expected_frames
            assert frame_summary["window_count"] == expected_frames
            assert residence["row_milliseconds"] == pytest.approx(
                expected_rows * residence["row_weighted_mean_ms"]
            )
        assert observed_row_fraction == pytest.approx(1.0)
        assert observed_file_fraction == pytest.approx(1.0)
    finally:
        hyperliquid.close()
        binance.close()
        worker.close()

    manifests = [validate_partition(path) for path in discover_partitions(root)]
    assert len(manifests) == 85
    assert sum(manifest.row_count for manifest in manifests) == 20_103
    manifest_groups = Counter(
        (
            manifest.partition.venue,
            manifest.partition.record_type,
            manifest.partition.asset,
        )
        for manifest in manifests
    )
    assert manifest_groups == Counter(
        {
            ("binance_usdm", RecordType.L2_SNAPSHOT, "BTC"): 18,
            ("binance_usdm", RecordType.L2_SNAPSHOT, "ETH"): 18,
            ("binance_usdm", RecordType.WIRE_MESSAGE, "GLOBAL"): 5,
            ("binance_usdm", RecordType.BBO, "BTC"): 5,
            ("binance_usdm", RecordType.BBO, "ETH"): 5,
            ("binance_usdm", RecordType.L2_BOOK_STATE, "BTC"): 5,
            ("binance_usdm", RecordType.L2_BOOK_STATE, "ETH"): 5,
            ("binance_usdm", RecordType.CLOCK_SYNC, "GLOBAL"): 5,
            ("hyperliquid", RecordType.BBO, "BTC"): 1,
            ("hyperliquid", RecordType.BBO, "ETH"): 1,
            ("hyperliquid", RecordType.CANDLE, "BTC"): 1,
            ("hyperliquid", RecordType.CANDLE, "ETH"): 1,
            ("hyperliquid", RecordType.FUNDING, "BTC"): 1,
            ("hyperliquid", RecordType.FUNDING, "ETH"): 1,
            ("hyperliquid", RecordType.INSTRUMENT_METADATA, "BTC"): 1,
            ("hyperliquid", RecordType.INSTRUMENT_METADATA, "ETH"): 1,
            ("hyperliquid", RecordType.L2_BOOK_STATE, "BTC"): 1,
            ("hyperliquid", RecordType.L2_BOOK_STATE, "ETH"): 1,
            ("hyperliquid", RecordType.L2_SNAPSHOT, "BTC"): 1,
            ("hyperliquid", RecordType.L2_SNAPSHOT, "ETH"): 1,
            ("hyperliquid", RecordType.MARKET_CONTEXT, "BTC"): 1,
            ("hyperliquid", RecordType.MARKET_CONTEXT, "ETH"): 1,
            ("hyperliquid", RecordType.TRADE, "BTC"): 5,
        }
    )

    rows_by_type: dict[RecordType, list[dict[str, object]]] = {}
    for manifest in manifests:
        rows_by_type.setdefault(manifest.partition.record_type, []).extend(
            pq.ParquetFile(root / manifest.relative_data_path).read().to_pylist()
        )
    assert {record_type: len(rows) for record_type, rows in rows_by_type.items()} == {
        RecordType.WIRE_MESSAGE: 452,
        RecordType.BBO: 454,
        RecordType.CANDLE: 120,
        RecordType.FUNDING: 2,
        RecordType.INSTRUMENT_METADATA: 2,
        RecordType.L2_BOOK_STATE: 454,
        RecordType.L2_SNAPSHOT: 18_160,
        RecordType.MARKET_CONTEXT: 2,
        RecordType.TRADE: 452,
        RecordType.CLOCK_SYNC: 5,
    }

    startup_types = {
        RecordType.BBO,
        RecordType.CANDLE,
        RecordType.FUNDING,
        RecordType.INSTRUMENT_METADATA,
        RecordType.L2_BOOK_STATE,
        RecordType.L2_SNAPSHOT,
        RecordType.MARKET_CONTEXT,
    }
    hyperliquid_startup_rows = [
        row
        for record_type in startup_types
        for row in rows_by_type[record_type]
        if row["venue"] == "hyperliquid"
    ]
    assert len(hyperliquid_startup_rows) == 210
    assert {row["asset"] for row in hyperliquid_startup_rows} == {
        "BTC",
        "ETH",
    }
    assert {row["connection_id"] for row in hyperliquid_startup_rows} == {
        HL_BOOTSTRAP_CONNECTION_ID
    }
    assert all(row["source_sequence"] is None for row in hyperliquid_startup_rows)
    assert {
        row["asset"]: row["source_index"]
        for row in rows_by_type[RecordType.INSTRUMENT_METADATA]
    } == {"BTC": 0, "ETH": 1}

    clock_rows = rows_by_type[RecordType.CLOCK_SYNC]
    expected_binance_lineage = {
        ("binance-public-depth-stress", 1, BINANCE_STRESS_CAPTURE)
    }
    assert {
        (row["connection_id"], row["connection_epoch"], row["capture_epoch_id"])
        for row in rows_by_type[RecordType.WIRE_MESSAGE]
    } == expected_binance_lineage
    assert {
        (row["connection_id"], row["connection_epoch"], row["capture_epoch_id"])
        for row in clock_rows
    } == expected_binance_lineage
    assert {row["observation_id"] for row in clock_rows} == {
        f"binance-clock-{sample_index}" for sample_index in range(5)
    }
    assert {row["sample_status"] for row in clock_rows} == {"valid"}
    assert {row["drift_uncertainty_ms"] for row in clock_rows} == {Decimal("40")}
    assert all(
        row["causal_valid_from"] == row["response_received_time"]
        and row["causal_valid_until"]
        == row["response_received_time"] + timedelta(seconds=15)
        for row in clock_rows
    )

    def lineage(row: dict[str, object]) -> tuple[object, object]:
        return (
            row["connection_id"],
            row["received_time"],
        )

    wire_asset_by_lineage = {
        lineage(row): row["message_asset"] for row in rows_by_type[RecordType.WIRE_MESSAGE]
    }
    assert len(wire_asset_by_lineage) == 452
    assert {row["arrival_sequence"] for row in rows_by_type[RecordType.WIRE_MESSAGE]} == set(range(1, 453))
    assert {
        key: row["asset"]
        for record_type in (RecordType.BBO, RecordType.L2_BOOK_STATE)
        for row in rows_by_type[record_type]
        if row["venue"] == "binance_usdm"
        for key in (lineage(row),)
    } == wire_asset_by_lineage
    assert {
        lineage(row): row["asset"]
        for row in rows_by_type[RecordType.L2_SNAPSHOT]
        if row["venue"] == "binance_usdm"
    } == wire_asset_by_lineage

    state_snapshot_ids = {str(row["snapshot_id"]) for row in rows_by_type[RecordType.L2_BOOK_STATE]}
    level_counts = Counter(str(row["snapshot_id"]) for row in rows_by_type[RecordType.L2_SNAPSHOT])
    side_counts = Counter(
        (str(row["snapshot_id"]), str(row["side"])) for row in rows_by_type[RecordType.L2_SNAPSHOT]
    )
    assert set(level_counts) == state_snapshot_ids
    assert set(level_counts.values()) == {40}
    assert set(side_counts.values()) == {20}
    bbo_sequence_by_lineage = {lineage(row): row["source_sequence"] for row in rows_by_type[RecordType.BBO]}
    level_sequence_by_lineage = {
        lineage(row): row["last_sequence"] for row in rows_by_type[RecordType.L2_SNAPSHOT]
    }
    assert level_sequence_by_lineage == bbo_sequence_by_lineage


def test_process_writer_owns_root_lock_and_preserves_multi_venue_l2_frames(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    worker = CoordinatedWriterProcess(
        root,
        venues=VENUES,
        batch_size=80,
        queue_capacity=80,
        venue_capacity_rows={
            "hyperliquid": 40,
            "binance_usdm": 40,
        },
    )
    hyperliquid = worker.client("hyperliquid")
    binance = worker.client("binance_usdm")
    try:
        snapshot = worker.metrics_snapshot()
        child = snapshot["child_process"]
        assert isinstance(child, dict)
        assert child["pid"] != os.getpid()
        assert child["start_method"] == "spawn"
        assert snapshot["isolation"] == "spawned_process"

        with pytest.raises(
            RuntimeError,
            match="collector lake already has an active writer",
        ):
            BatchingLakeSink(root, batch_size=1, queue_capacity=1)

        assert hyperliquid.add_many(_l2_frame("hyperliquid", 1)) == 40
        assert binance.add_many(_l2_frame("binance_usdm", 2)) == 40
        hyperliquid_result = hyperliquid.flush()
        binance_result = binance.collect_completed()
        assert hyperliquid_result.row_count == 40
        assert binance_result.row_count == 40
        assert worker.pending_count == 0

        hyperliquid.close()
        binance.close()
        worker.close()
    finally:
        if worker.metrics_snapshot()["failure"] is None:
            worker.close()

    final = worker.metrics_snapshot()
    assert final["closed"] is True
    assert final["accounting_status"] == "exact"
    child = final["child_process"]
    assert isinstance(child, dict)
    assert child["alive"] is False
    assert child["exitcode"] == 0
    assert child["telemetry"] is not None
    assert final["storage"] is not None
    diagnostic_groups = _group_diagnostics(final)
    assert [
        (group["venue"], group["asset"], group["record_type"])
        for group in diagnostic_groups
    ] == [
        ("binance_usdm", "BTC", RecordType.L2_SNAPSHOT.value),
        ("hyperliquid", "BTC", RecordType.L2_SNAPSHOT.value),
    ]
    for group in diagnostic_groups:
        _assert_group_contribution_consistency(group)
        assert group["rows"] == {
            "enqueued": 40,
            "acknowledged": 40,
            "durable": 40,
        }
        assert group["output_files"] == 1
        assert group["average_rows_per_output_file"] == pytest.approx(40.0)
        flush = group["flush_contribution"]
        assert isinstance(flush, dict)
        assert flush == {
            "flushes": 1,
            "rows": 40,
            "output_files": 1,
            "row_fraction": pytest.approx(0.5),
            "file_fraction": pytest.approx(0.5),
        }
        residence = group["queue_residence_contribution"]
        assert isinstance(residence, dict)
        assert residence["frames"] == 1
        assert residence["rows"] == 40
        assert residence["row_milliseconds"] == pytest.approx(
            40 * residence["row_weighted_mean_ms"]
        )
        frame_summary = residence["frame_residence_ms"]
        assert isinstance(frame_summary, dict)
        assert frame_summary["count"] == 1
        assert frame_summary["window_count"] == 1

    manifests = [validate_partition(path) for path in discover_partitions(root)]
    rows = [
        row
        for manifest in manifests
        for row in pq.ParquetFile(root / manifest.relative_data_path).read().to_pylist()
    ]
    assert len(rows) == 80
    assert {(row["venue"], row["snapshot_id"]) for row in rows} == {
        ("hyperliquid", "hyperliquid-snapshot-1"),
        ("binance_usdm", "binance_usdm-snapshot-2"),
    }

    reopened = BatchingLakeSink(root, batch_size=1, queue_capacity=1)
    reopened.close()


def test_malformed_flush_manifest_attribution_is_rejected_before_mutation(
    tmp_path: Path,
) -> None:
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=80,
        queue_capacity=80,
    )
    sink = worker.client("hyperliquid")
    try:
        assert sink.add_many(_l2_frame("hyperliquid", 1)) == 40
        deadline = time.monotonic() + 5
        while True:
            before = worker.metrics_snapshot()
            venues = before["venues"]
            assert isinstance(venues, dict)
            if venues["hyperliquid"]["frames_processed"] == 1:
                break
            if time.monotonic() >= deadline:
                pytest.fail("writer did not acknowledge the setup frame")
            time.sleep(0.01)

        before_diagnostics = before["group_diagnostics"]
        malformed_results = (
            (
                FlushResult(
                    (_diagnostic_manifest(venue="hyperliquid", row_count=39),),
                    40,
                    0,
                ),
                "manifest rows did not match",
            ),
            (
                FlushResult(
                    (
                        _diagnostic_manifest(venue="hyperliquid", row_count=20),
                        _diagnostic_manifest(venue="binance_usdm", row_count=20),
                    ),
                    40,
                    0,
                ),
                "manifest venue did not match",
            ),
            (
                FlushResult(
                    (
                        _diagnostic_manifest(venue="hyperliquid", row_count=20),
                        _diagnostic_manifest(
                            venue="hyperliquid",
                            asset="ETH",
                            row_count=20,
                        ),
                    ),
                    40,
                    0,
                ),
                "durable group diagnostics were not admitted",
            ),
            (
                FlushResult(
                    (
                        _diagnostic_manifest(venue="hyperliquid", row_count=20),
                        cast(PartitionManifest, object()),
                    ),
                    40,
                    0,
                ),
                "invalid manifest",
            ),
        )
        for malformed_result, expected_error in malformed_results:
            with (
                worker._condition,
                pytest.raises(CoordinatedWriterError, match=expected_error),
            ):
                worker._apply_flush_locked(
                    {
                        "command_id": None,
                        "full_barrier": False,
                        "results": {"hyperliquid": malformed_result},
                        "flush_duration_ns": 1,
                    },
                    expected_kind="barrier",
                )
            after = worker.metrics_snapshot()
            assert after["group_diagnostics"] == before_diagnostics
            assert after["outstanding_rows"] == 40
            after_venues = after["venues"]
            assert isinstance(after_venues, dict)
            assert after_venues["hyperliquid"]["durable_rows"] == 0

        durable = sink.flush()
        assert durable.row_count == 40
        assert durable.duplicate_count == 0
    finally:
        sink.close()
        worker.close()


def test_process_writer_rejects_oversized_frame_before_admission(
    tmp_path: Path,
) -> None:
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=4,
        queue_capacity=4,
    )
    sink = worker.client("hyperliquid")
    try:
        with pytest.raises(
            WriterQueueCapacityError,
            match="no record was admitted",
        ):
            sink.add_many(
                _l2_frame(
                    "hyperliquid",
                    1,
                    levels_per_side=3,
                )
            )
        snapshot = worker.metrics_snapshot()
        assert snapshot["outstanding_rows"] == 0
        venues = snapshot["venues"]
        assert isinstance(venues, dict)
        assert venues["hyperliquid"]["capacity_rejections"] == 1
    finally:
        sink.close()
        worker.close()


def test_process_writer_bounds_lifetime_group_diagnostics_before_admission(
    tmp_path: Path,
) -> None:
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=2,
        queue_capacity=2,
    )
    sink = worker.client("hyperliquid")

    def trade_for_asset(asset: str, sequence: int) -> ParsedRecord:
        source = _trade("hyperliquid", sequence)
        row = dict(source.row)
        row["asset"] = asset
        row["trade_id"] = f"diagnostic-cap-{asset}-{sequence}"
        return ParsedRecord(RecordType.TRADE, asset, row)

    try:
        for sequence, asset in enumerate(("BTC", "ETH")):
            assert sink.add(trade_for_asset(asset, sequence)) is True
            assert sink.flush().row_count == 1

        with pytest.raises(
            WriterQueueCapacityError,
            match=r"lifetime diagnostic-group capacity exceeded.*no record was admitted",
        ):
            sink.add(trade_for_asset("SOL", 2))

        snapshot = worker.metrics_snapshot()
        assert snapshot["outstanding_rows"] == 0
        venues = snapshot["venues"]
        assert isinstance(venues, dict)
        assert venues["hyperliquid"]["capacity_rejections"] == 0
        assert venues["hyperliquid"]["durable_rows"] == 2
        diagnostics = snapshot["group_diagnostics"]
        assert isinstance(diagnostics, dict)
        assert diagnostics["capacity"] == {
            "max_groups": 2,
            "current_groups": 2,
            "rejections": 1,
        }
        assert {
            (group["venue"], group["asset"], group["record_type"])
            for group in _group_diagnostics(snapshot)
        } == {
            ("hyperliquid", "BTC", RecordType.TRADE.value),
            ("hyperliquid", "ETH", RecordType.TRADE.value),
        }
    finally:
        sink.close()
        worker.close()


def test_abrupt_writer_child_death_is_fail_closed_and_releases_root_lock(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    worker = CoordinatedWriterProcess(
        root,
        venues=("hyperliquid",),
        batch_size=40,
        queue_capacity=40,
    )
    sink = worker.client("hyperliquid")
    assert (
        sink.add_many(
            _l2_frame(
                "hyperliquid",
                1,
                levels_per_side=1,
            )
        )
        == 2
    )

    worker._process.terminate()
    worker._process.join(timeout=5)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if worker.metrics_snapshot()["failure"] is not None:
            break
        time.sleep(0.01)
    else:
        raise AssertionError("parent monitor did not detect abrupt child death")

    with pytest.raises(ProcessWriterError, match="child_exit"):
        sink.flush()
    snapshot = worker.metrics_snapshot()
    assert snapshot["accounting_status"] == "indeterminate"
    assert snapshot["outstanding_rows"] >= 0

    with pytest.raises(ProcessWriterError, match="child_exit"):
        worker.close()

    reopened = BatchingLakeSink(root, batch_size=1, queue_capacity=1)
    reopened.close()


def test_cpu_heavy_l2_flush_runs_outside_parent_python_process(
    tmp_path: Path,
) -> None:
    frame = tuple(
        record
        for snapshot_index in range(1, 51)
        for record in _l2_frame(
            "hyperliquid",
            snapshot_index,
            levels_per_side=20,
        )
    )
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=len(frame),
        queue_capacity=len(frame),
    )
    sink = worker.client("hyperliquid")
    assert sink.add_many(frame) == len(frame)

    completed = threading.Event()
    failures: list[BaseException] = []

    def durable_barrier() -> None:
        try:
            result = sink.flush()
            assert result.row_count == len(frame)
        except BaseException as exc:
            failures.append(exc)
        finally:
            completed.set()

    waiter = threading.Thread(target=durable_barrier)
    waiter.start()
    ticks: list[float] = []
    previous = time.monotonic()
    while not completed.wait(0.005):
        observed = time.monotonic()
        ticks.append(observed - previous)
        previous = observed
    waiter.join(timeout=5)

    assert failures == []
    assert not waiter.is_alive()
    assert worker.metrics_snapshot()["child_process"]["pid"] != os.getpid()
    assert ticks
    assert max(ticks) < 0.25

    sink.close()
    worker.close()
    for path in discover_partitions(tmp_path / "lake"):
        validate_partition(path)


def test_result_transport_failure_is_fail_closed_without_hanging_waiters(
    tmp_path: Path,
) -> None:
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=2,
        queue_capacity=2,
    )
    sink = worker.client("hyperliquid")
    try:
        worker._result_queue.close()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            failure = worker.metrics_snapshot()["failure"]
            if failure is not None:
                break
            time.sleep(0.01)
        else:
            raise AssertionError("result transport failure was not observed")

        assert isinstance(failure, dict)
        assert failure["phase"] == "result_transport"
        with pytest.raises(ProcessWriterError, match="result_transport"):
            sink.flush()
        with pytest.raises(ProcessWriterError, match="result_transport"):
            worker.close()
    finally:
        if worker._process.is_alive():
            worker._cleanup_failed_process()


def test_writer_child_exits_and_releases_root_lock_after_parent_is_killed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    context = mp.get_context("spawn")
    ready_queue = context.Queue()
    parent = context.Process(
        target=_parent_holding_writer,
        args=(str(root), ready_queue),
        daemon=False,
    )
    child_pid: int | None = None
    lock_reacquired = False
    try:
        parent.start()
        child_pid = ready_queue.get(timeout=30)
        assert isinstance(child_pid, int)
        with pytest.raises(
            RuntimeError,
            match="collector lake already has an active writer",
        ):
            BatchingLakeSink(root, batch_size=1, queue_capacity=1)

        parent.terminate()
        parent.join(timeout=5)
        if parent.is_alive():
            parent.kill()
            parent.join(timeout=5)
        assert not parent.is_alive()

        deadline = time.monotonic() + 20
        while True:
            try:
                reopened = BatchingLakeSink(
                    root,
                    batch_size=1,
                    queue_capacity=1,
                )
            except RuntimeError as exc:
                if "active writer" not in str(exc) or time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
                continue
            reopened.close()
            lock_reacquired = True
            break
    finally:
        if parent.is_alive():
            parent.kill()
            parent.join(timeout=5)
        with contextlib.suppress(AttributeError, OSError, ValueError):
            ready_queue.cancel_join_thread()
        with contextlib.suppress(AttributeError, OSError, ValueError):
            ready_queue.close()
        if child_pid is not None and not lock_reacquired:
            kill_signal = getattr(signal, "SIGKILL", signal.SIGTERM)
            with contextlib.suppress(OSError, ProcessLookupError):
                os.kill(child_pid, kill_signal)


def test_process_writer_metrics_are_isolated_and_staleness_is_bounded(
    tmp_path: Path,
) -> None:
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=2,
        queue_capacity=2,
    )
    try:
        with worker._condition:
            snapshot = worker.metrics_snapshot()
            child = snapshot["child_process"]
            assert isinstance(child, dict)
            telemetry = child["telemetry"]
            assert isinstance(telemetry, dict)
            telemetry["caller_mutation"] = True
            cached = worker._child_cache
            assert isinstance(cached, dict)
            cached_process = cached["process"]
            assert isinstance(cached_process, dict)
            assert "caller_mutation" not in cached_process

            worker._child_cache_received_ns = worker._monotonic_ns() - 3_000_000_000
            stale = worker.metrics_snapshot()["child_process"]
            assert isinstance(stale, dict)
            assert stale["cache_current"] is False
            assert stale["cache_stale"] is True
    finally:
        worker.close()


def test_process_writer_releases_duplicate_capacity_and_credits_once(
    tmp_path: Path,
) -> None:
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=80,
        queue_capacity=80,
    )
    sink = worker.client("hyperliquid")
    frame = _l2_frame("hyperliquid", 1)
    try:
        assert sink.add_many(frame) == 40
        assert sink.add_many(frame) == 40
        result = sink.flush()
        assert result.row_count == 40
        assert result.duplicate_count == 40
        assert worker.pending_count == 0
        assert sink.collect_completed() == FlushResult((), 0, 0)
        diagnostic_groups = _group_diagnostics(worker.metrics_snapshot())
        assert len(diagnostic_groups) == 1
        duplicate_group = diagnostic_groups[0]
        _assert_group_contribution_consistency(duplicate_group)
        assert (
            duplicate_group["venue"],
            duplicate_group["asset"],
            duplicate_group["record_type"],
        ) == ("hyperliquid", "BTC", RecordType.L2_SNAPSHOT.value)
        assert duplicate_group["rows"] == {
            "enqueued": 80,
            "acknowledged": 80,
            "durable": 40,
        }
        assert duplicate_group["output_files"] == 1
        flush = duplicate_group["flush_contribution"]
        assert isinstance(flush, dict)
        assert flush == {
            "flushes": 1,
            "rows": 40,
            "output_files": 1,
            "row_fraction": pytest.approx(1.0),
            "file_fraction": pytest.approx(1.0),
        }
        residence = duplicate_group["queue_residence_contribution"]
        assert isinstance(residence, dict)
        assert residence["frames"] == 2
        assert residence["rows"] == 80
    finally:
        sink.close()
        worker.close()


def test_process_writer_startup_fails_closed_when_root_lock_is_held(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    owner = BatchingLakeSink(root, batch_size=1, queue_capacity=1)
    try:
        with pytest.raises(
            ProcessWriterError,
            match="collector lake already has an active writer",
        ):
            CoordinatedWriterProcess(
                root,
                venues=("hyperliquid",),
                batch_size=2,
                queue_capacity=2,
                startup_timeout_seconds=5,
            )
    finally:
        owner.close()


def test_post_ack_child_join_timeout_is_bounded_and_accounting_stays_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = CoordinatedWriterProcess(
        tmp_path / "lake",
        venues=("hyperliquid",),
        batch_size=2,
        queue_capacity=2,
    )
    original_join = worker._process.join
    original_is_alive = worker._process.is_alive
    original_kill = worker._process.kill
    forced_join_seen = False
    synthetic_alive_reported = False

    def bounded_join(timeout: float | None = None) -> None:
        nonlocal forced_join_seen
        if timeout == 5.0 and not forced_join_seen:
            forced_join_seen = True
            return
        original_join(timeout=timeout)

    def synthetic_is_alive() -> bool:
        nonlocal synthetic_alive_reported
        if forced_join_seen and not synthetic_alive_reported:
            synthetic_alive_reported = True
            return True
        return original_is_alive()

    monkeypatch.setattr(worker._process, "join", bounded_join)
    monkeypatch.setattr(worker._process, "is_alive", synthetic_is_alive)
    started = time.monotonic()
    try:
        with pytest.raises(ProcessWriterError, match="child_shutdown"):
            worker.close()
        assert time.monotonic() - started < 10
        snapshot = worker.metrics_snapshot()
        assert snapshot["accounting_status"] == "exact"
        failure = snapshot["failure"]
        assert isinstance(failure, dict)
        assert failure["phase"] == "child_shutdown"
    finally:
        if original_is_alive():
            original_kill()
            original_join(timeout=5)


def test_nonblocking_flush_request_makes_sub_batch_rows_durable(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    worker = CoordinatedWriterProcess(
        root,
        venues=("hyperliquid",),
        batch_size=40,
        queue_capacity=40,
    )
    sink = worker.client("hyperliquid")
    try:
        assert (
            sink.add_many(
                _l2_frame(
                    "hyperliquid",
                    1,
                    levels_per_side=1,
                )
            )
            == 2
        )
        assert sink.request_flush() is True
        assert (
            sink.add_many(
                _l2_frame(
                    "hyperliquid",
                    2,
                    levels_per_side=1,
                )
            )
            == 2
        )
        assert sink.request_flush() is True

        deadline = time.monotonic() + 10
        durable_rows = 0
        durable_duplicates = 0
        while time.monotonic() < deadline:
            observed = sink.collect_completed()
            durable_rows += observed.row_count
            durable_duplicates += observed.duplicate_count
            if durable_rows == 4:
                break
            time.sleep(0.01)
        else:
            raise AssertionError("nonblocking barrier did not become durable")

        assert durable_rows == 4
        assert durable_duplicates == 0
        assert worker.pending_count == 0
        assert sink.request_flush() is False
        manifests = [validate_partition(path) for path in discover_partitions(root)]
        assert sum(manifest.row_count for manifest in manifests) == 4
    finally:
        sink.close()
        worker.close()
