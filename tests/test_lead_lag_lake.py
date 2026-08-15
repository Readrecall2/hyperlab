from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from hyperlab.analysis import lake as lead_lag_lake
from hyperlab.data.lake import InventoryReport, PartitionKey, PartitionManifest
from hyperlab.data.schema import RecordType, latest_schema_for

BASE = datetime(2026, 8, 15, tzinfo=UTC)
STRICT_START = BASE + timedelta(seconds=1)
STRICT_END = BASE + timedelta(seconds=9)


@dataclass(frozen=True)
class _LakeFixture:
    inventory: InventoryReport
    tables: dict[str, pa.Table]
    report: dict[str, object]


def _iso(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _common(
    venue: str,
    asset: str,
    record_type: RecordType,
    received_time: datetime,
    *,
    connection_id: str = "ws-public",
) -> dict[str, object]:
    return {
        "schema_version": latest_schema_for(record_type).version,
        "record_type": record_type.value,
        "venue": venue,
        "asset": asset,
        "event_time": received_time - timedelta(milliseconds=2),
        "exchange_time": received_time - timedelta(milliseconds=2),
        "received_time": received_time,
        "source_sequence": 1,
        "connection_id": connection_id,
    }


def _bbo_row(
    venue: str,
    asset: str,
    received_time: datetime,
    *,
    update_id: str | None = None,
    connection_id: str = "ws-public",
) -> dict[str, object]:
    row = _common(
        venue,
        asset,
        RecordType.BBO,
        received_time,
        connection_id=connection_id,
    )
    row.update(
        {
            "update_id": update_id or f"ws:{venue}:{asset}",
            "bid_price": Decimal("100"),
            "bid_quantity": Decimal("2"),
            "ask_price": Decimal("101"),
            "ask_quantity": Decimal("3"),
        }
    )
    return row


def _trade_row(
    venue: str, asset: str, received_time: datetime
) -> dict[str, object]:
    row = _common(venue, asset, RecordType.TRADE, received_time)
    row.update(
        {
            "trade_id": f"{venue}:{asset}:trade",
            "aggressor_side": "buy",
            "price": Decimal("100.5"),
            "quantity": Decimal("0.25"),
            "quote_quantity": Decimal("25.125"),
            "is_liquidation": False,
            "connection_epoch": 1,
            "arrival_sequence": 2,
        }
    )
    return row


def _l2_rows(
    venue: str,
    asset: str,
    received_time: datetime,
    *,
    snapshot_prefix: str = "ws",
    connection_id: str = "ws-public",
) -> tuple[dict[str, object], list[dict[str, object]]]:
    snapshot_id = f"{snapshot_prefix}:{venue}:{asset}:snapshot"
    book_epoch_id = f"{connection_id}:1"
    header = _common(
        venue,
        asset,
        RecordType.L2_BOOK_STATE,
        received_time,
        connection_id=connection_id,
    )
    header["source_sequence"] = None
    header.update(
        {
            "snapshot_id": snapshot_id,
            "book_epoch_id": book_epoch_id,
            "bid_level_count": 2,
            "ask_level_count": 1,
        }
    )
    levels: list[dict[str, object]] = []
    for side, values in (
        ("bid", (("100", "2"), ("99", "1"))),
        ("ask", (("101", "3"),)),
    ):
        for level_number, (price, quantity) in enumerate(values):
            row = _common(
                venue,
                asset,
                RecordType.L2_SNAPSHOT,
                received_time,
                connection_id=connection_id,
            )
            row["source_sequence"] = None
            row.update(
                {
                    "snapshot_id": snapshot_id,
                    "book_epoch_id": book_epoch_id,
                    "last_sequence": 10 if venue == "binance_usdm" else None,
                    "side": side,
                    "level": level_number,
                    "price": Decimal(price),
                    "quantity": Decimal(quantity),
                    "order_count": None if venue == "binance_usdm" else 1,
                }
            )
            levels.append(row)
    return header, levels


def _clock_row(received_time: datetime) -> dict[str, object]:
    row = _common(
        "binance_usdm",
        "GLOBAL",
        RecordType.CLOCK_SYNC,
        received_time,
        connection_id="clock-public",
    )
    row.update(
        {
            "request_sent_time": received_time - timedelta(milliseconds=20),
            "response_received_time": received_time,
            "server_time": received_time - timedelta(milliseconds=5),
            "round_trip_latency_ms": Decimal("20"),
            "estimated_clock_drift_ms": Decimal("5"),
            "drift_uncertainty_ms": Decimal("10"),
            "observation_id": "clock:1",
            "connection_epoch": 1,
            "capture_epoch_id": "capture-1",
            "causal_valid_from": received_time,
            "causal_valid_until": STRICT_END,
            "sample_status": "valid",
            "invalid_reason": None,
            "sampling_interval_ms": 1_000,
            "max_age_ms": 15_000,
            "max_uncertainty_ms": Decimal("50"),
        }
    )
    return row


def _manifest(
    venue: str,
    asset: str,
    record_type: RecordType,
    rows: list[dict[str, object]],
    *,
    suffix: str = "0001",
) -> PartitionManifest:
    received_times = [pd.Timestamp(row["received_time"]).to_pydatetime() for row in rows]
    token = f"{venue}:{asset}:{record_type.value}:{suffix}"
    return PartitionManifest(
        partition=PartitionKey(venue, BASE.date(), asset, record_type),
        data_file=f"part-{suffix}.parquet",
        sha256=hashlib.sha256(token.encode()).hexdigest(),
        size_bytes=1,
        row_count=len(rows),
        timestamp_bounds={
            "received_time": {
                "min": _iso(min(received_times)),
                "max": _iso(max(received_times)),
            }
        },
        schema_name=record_type.value,
        schema_version=latest_schema_for(record_type).version,
        schema_fingerprint=hashlib.sha256(record_type.value.encode()).hexdigest(),
        stream_key=f"{venue}:{asset}:{record_type.value}",
        sequence_min=None,
        sequence_max=None,
        duplicates=0,
        out_of_order=0,
        gaps=(),
        gap_detection="test",
        null_counts={},
        quality="PASS",
    )


def _gate_report(inventory: InventoryReport) -> dict[str, object]:
    return {
        "audit_version": 1,
        "phase_10_status": "BLOCKED_PRECONDITION_NOT_MET",
        "technical_capture_gate": "PASS",
        "assets": ["BTC", "ETH"],
        "requested_window": {
            "start": _iso(BASE),
            "end": _iso(BASE + timedelta(seconds=10)),
            "duration_seconds": 10.0,
        },
        "policy": {"interval_semantics": "half_open_received_time_causal"},
        "clock_sync": {
            "coverage_continuous": True,
            "causal_coverage_continuous": True,
            "uncovered_seconds": 0.0,
            "market_active_without_valid_clock": [],
            "strict_max_age_ms": 15_000,
        },
        "strict_phase_10_overlap": {
            "duration_seconds": 8.0,
            "interval_count": 1,
            "by_asset": {
                "BTC": {"duration_seconds": 8.0, "interval_count": 1},
                "ETH": {"duration_seconds": 8.0, "interval_count": 1},
            },
            "intervals": [
                {
                    "capture_epoch_id": "capture-1",
                    "start": _iso(STRICT_START),
                    "end": _iso(STRICT_END),
                    "duration_seconds": 8.0,
                }
            ],
        },
        "validation": {
            "inventory_partition_count": len(inventory.partitions),
            "inventory_row_count": inventory.total_rows,
        },
        "failure_reasons": [],
    }


def _fixture(*, incomplete_l2: bool = False) -> _LakeFixture:
    rows_by_key: list[
        tuple[str, str, RecordType, list[dict[str, object]], str]
    ] = []
    simultaneous = STRICT_START + timedelta(seconds=1)
    for venue in ("binance_usdm", "hyperliquid"):
        for asset_index, asset in enumerate(("BTC", "ETH")):
            received = simultaneous + timedelta(milliseconds=asset_index)
            bbo_rows = [_bbo_row(venue, asset, received)]
            if venue == "hyperliquid" and asset == "BTC":
                bbo_rows.append(
                    _bbo_row(
                        venue,
                        asset,
                        received + timedelta(milliseconds=1),
                        update_id="rest:bootstrap-bbo",
                        connection_id="rest-bootstrap",
                    )
                )
            rows_by_key.append((venue, asset, RecordType.BBO, bbo_rows, "bbo"))
            rows_by_key.append(
                (
                    venue,
                    asset,
                    RecordType.TRADE,
                    [_trade_row(venue, asset, received + timedelta(milliseconds=10))],
                    "trade",
                )
            )
            header, levels = _l2_rows(
                venue,
                asset,
                received + timedelta(milliseconds=20),
            )
            if incomplete_l2 and venue == "binance_usdm" and asset == "BTC":
                levels = levels[:-1]
            header_rows = [header]
            if venue == "hyperliquid" and asset == "BTC":
                rest_header, rest_levels = _l2_rows(
                    venue,
                    asset,
                    received + timedelta(milliseconds=30),
                    snapshot_prefix="rest",
                    connection_id="rest-bootstrap",
                )
                header_rows.append(rest_header)
                levels.extend(rest_levels)
            rows_by_key.append(
                (
                    venue,
                    asset,
                    RecordType.L2_BOOK_STATE,
                    header_rows,
                    "state",
                )
            )
            rows_by_key.append(
                (venue, asset, RecordType.L2_SNAPSHOT, levels, "levels")
            )

    clock = _clock_row(STRICT_START)
    rows_by_key.append(
        (
            "binance_usdm",
            "GLOBAL",
            RecordType.CLOCK_SYNC,
            [clock],
            "clock",
        )
    )

    manifests: list[PartitionManifest] = []
    tables: dict[str, pa.Table] = {}
    for venue, asset, record_type, rows, suffix in rows_by_key:
        manifest = _manifest(
            venue,
            asset,
            record_type,
            rows,
            suffix=suffix,
        )
        manifests.append(manifest)
        tables[manifest.relative_data_path.as_posix()] = pa.Table.from_pylist(rows)
    inventory = InventoryReport(
        partitions=tuple(reversed(manifests)),
        total_rows=sum(manifest.row_count for manifest in manifests),
        venues=("binance_usdm", "hyperliquid"),
        assets=("BTC", "ETH", "GLOBAL"),
        record_types=tuple(sorted(record_type.value for record_type in RecordType)),
        dates=(BASE.date().isoformat(),),
        delisted_assets=(),
        cross_segment_gaps=(),
    )
    return _LakeFixture(
        inventory=inventory,
        tables=tables,
        report=_gate_report(inventory),
    )


def _write_gate(path: Path, report: dict[str, object]) -> bytes:
    payload = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
    path.write_bytes(payload)
    return payload


def _install_lake_mocks(
    monkeypatch: pytest.MonkeyPatch,
    fixture: _LakeFixture,
    *,
    inventories: list[InventoryReport] | None = None,
    live_report: dict[str, object] | None = None,
) -> None:
    inventory_values = iter(inventories or [fixture.inventory, fixture.inventory])
    monkeypatch.setattr(
        lead_lag_lake,
        "audit_phase10_continuity",
        lambda *_args, **_kwargs: copy.deepcopy(live_report or fixture.report),
    )
    monkeypatch.setattr(
        lead_lag_lake,
        "inventory_partitions",
        lambda _root: next(inventory_values),
    )
    monkeypatch.setattr(
        lead_lag_lake,
        "read_hashed_table",
        lambda _root, manifest: fixture.tables[
            manifest.relative_data_path.as_posix()
        ],
    )


def test_loads_only_strict_contemporaneous_rows_and_reconstructs_l2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture()
    root = tmp_path / "lake"
    root.mkdir()
    gate_path = tmp_path / "saved-gate.json"
    raw_gate = _write_gate(gate_path, fixture.report)
    _install_lake_mocks(monkeypatch, fixture)

    window = lead_lag_lake.load_validated_lead_lag_window(root, gate_path)

    assert window.root == root.resolve()
    assert window.assets == ("BTC", "ETH")
    assert window.gate_report_sha256 == hashlib.sha256(raw_gate).hexdigest()
    canonical_gate = json.dumps(
        fixture.report,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert window.canonical_gate_sha256 == hashlib.sha256(canonical_gate).hexdigest()
    assert window.dataset.source_fingerprint == window.manifest_fingerprint
    assert window.dataset.provenance["kind"] == "REAL"
    assert len(window.dataset.bbo) == 4
    assert len(window.dataset.trades) == 4
    assert len(window.dataset.l2) == 4
    assert len(window.dataset.clock_sync) == 1
    assert not window.dataset.bbo["update_id"].str.startswith("rest:").any()
    assert not window.dataset.l2["snapshot_id"].str.startswith("rest:").any()
    assert window.dataset.l2["bid_depth"].tolist() == [Decimal("3")] * 4
    assert window.dataset.l2["ask_depth"].tolist() == [Decimal("3")] * 4
    assert window.dataset.l2["imbalance"].tolist() == [0.0] * 4
    assert all(len(levels) == 2 for levels in window.dataset.l2["bids"])
    assert all(len(levels) == 1 for levels in window.dataset.l2["asks"])
    assert window.dataset.bbo["received_time"].is_monotonic_increasing
    simultaneous_rows = window.dataset.bbo[
        window.dataset.bbo["received_time"] == pd.Timestamp(STRICT_START + timedelta(seconds=1))
    ]
    assert set(simultaneous_rows["venue"]) == {"binance_usdm", "hyperliquid"}
    assert (
        window.dataset.bbo.attrs["equal_received_time_semantics"]
        == "simultaneous_batch"
    )
    paths = [
        str(entry["relative_data_path"])
        for entry in window.selected_manifest_entries
    ]
    assert paths == sorted(paths)
    canonical_manifests = json.dumps(
        window.selected_manifest_entries,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert window.manifest_fingerprint == hashlib.sha256(
        canonical_manifests
    ).hexdigest()


def test_rejects_non_passing_saved_gate_before_any_lake_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture()
    report = copy.deepcopy(fixture.report)
    report["technical_capture_gate"] = "FAIL"
    report["failure_reasons"] = ["clock_sync_not_continuous"]
    root = tmp_path / "lake"
    root.mkdir()
    gate_path = tmp_path / "saved-gate.json"
    _write_gate(gate_path, report)
    monkeypatch.setattr(
        lead_lag_lake,
        "audit_phase10_continuity",
        lambda *_args, **_kwargs: pytest.fail("fresh audit must not run"),
    )
    monkeypatch.setattr(
        lead_lag_lake,
        "inventory_partitions",
        lambda *_args, **_kwargs: pytest.fail("inventory must not run"),
    )

    with pytest.raises(
        lead_lag_lake.LeadLagLakeValidationError,
        match="technical_capture_gate=PASS",
    ):
        lead_lag_lake.load_validated_lead_lag_window(root, gate_path)


def test_rejects_saved_gate_that_fresh_audit_does_not_reproduce(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture()
    root = tmp_path / "lake"
    root.mkdir()
    gate_path = tmp_path / "saved-gate.json"
    _write_gate(gate_path, fixture.report)
    live_report = copy.deepcopy(fixture.report)
    live_report["validation"] = {
        "inventory_partition_count": len(fixture.inventory.partitions),
        "inventory_row_count": fixture.inventory.total_rows + 1,
    }
    monkeypatch.setattr(
        lead_lag_lake,
        "audit_phase10_continuity",
        lambda *_args, **_kwargs: live_report,
    )
    monkeypatch.setattr(
        lead_lag_lake,
        "inventory_partitions",
        lambda *_args, **_kwargs: pytest.fail("inventory must not run"),
    )

    with pytest.raises(
        lead_lag_lake.LeadLagLakeValidationError,
        match="canonical fresh re-audit",
    ):
        lead_lag_lake.load_validated_lead_lag_window(root, gate_path)


def test_rejects_incomplete_atomic_l2_frame(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(incomplete_l2=True)
    root = tmp_path / "lake"
    root.mkdir()
    gate_path = tmp_path / "saved-gate.json"
    _write_gate(gate_path, fixture.report)
    _install_lake_mocks(monkeypatch, fixture)

    with pytest.raises(
        lead_lag_lake.LeadLagLakeValidationError,
        match="does not match header level counts",
    ):
        lead_lag_lake.load_validated_lead_lag_window(root, gate_path)


def test_rejects_selected_manifest_set_drift_during_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture()
    extra_row = _bbo_row(
        "binance_usdm",
        "BTC",
        STRICT_START + timedelta(seconds=3),
        update_id="ws:late-publication",
    )
    extra = _manifest(
        "binance_usdm",
        "BTC",
        RecordType.BBO,
        [extra_row],
        suffix="late",
    )
    final_inventory = replace(
        fixture.inventory,
        partitions=(*fixture.inventory.partitions, extra),
        total_rows=fixture.inventory.total_rows + 1,
    )
    root = tmp_path / "lake"
    root.mkdir()
    gate_path = tmp_path / "saved-gate.json"
    _write_gate(gate_path, fixture.report)
    _install_lake_mocks(
        monkeypatch,
        fixture,
        inventories=[fixture.inventory, final_inventory],
    )

    with pytest.raises(
        lead_lag_lake.LeadLagLakeValidationError,
        match="manifest/hash set changed",
    ):
        lead_lag_lake.load_validated_lead_lag_window(root, gate_path)


def test_rejects_duplicate_keys_in_independently_saved_gate(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    gate_path = tmp_path / "saved-gate.json"
    gate_path.write_text(
        '{"technical_capture_gate":"PASS","technical_capture_gate":"PASS"}',
        encoding="utf-8",
    )

    with pytest.raises(
        lead_lag_lake.LeadLagLakeValidationError,
        match="duplicate JSON key",
    ):
        lead_lag_lake.load_validated_lead_lag_window(root, gate_path)


def test_equal_time_l2_frames_keep_numeric_wire_arrival_order() -> None:
    received = STRICT_START + timedelta(seconds=2)
    headers: list[dict[str, object]] = []
    levels: list[dict[str, object]] = []
    for arrival in (10, 2):
        header, frame_levels = _l2_rows("hyperliquid", "BTC", received)
        snapshot_id = f"ws:ws-public:1:{arrival}:1786752000000"
        header["snapshot_id"] = snapshot_id
        for level in frame_levels:
            level["snapshot_id"] = snapshot_id
        headers.append(header)
        levels.extend(frame_levels)

    frames = lead_lag_lake._reconstruct_l2(headers, levels)

    assert [str(frame["snapshot_id"]) for frame in frames] == [
        "ws:ws-public:1:2:1786752000000",
        "ws:ws-public:1:10:1786752000000",
    ]
