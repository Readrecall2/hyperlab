from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pyarrow as pa
import pytest

from hyperlab.analysis import streaming_lake
from hyperlab.analysis.gate_binding import SavedPhase10Gate
from hyperlab.analysis.lead_lag import StrictInterval
from hyperlab.analysis.streaming_lake import (
    BoundedLeadLagGateAdmission,
    StreamingLakeValidationError,
    load_bounded_lead_lag_window,
    validate_bounded_lead_lag_gate,
    verify_immutable_inputs_unchanged,
)
from hyperlab.data.lake import PartitionKey, PartitionManifest, write_partition
from hyperlab.data.schema import RecordType, schema_for

BASE = datetime(2026, 8, 15, 12, tzinfo=UTC)
STRICT_START = BASE + timedelta(seconds=1)
STRICT_END = BASE + timedelta(seconds=9)
VENUES = ("binance_usdm", "hyperliquid")
ASSETS = ("BTC", "ETH")


def _common(
    venue: str,
    asset: str,
    record_type: RecordType,
    received_time: datetime,
    *,
    schema_version: int,
    connection_id: str = "ws-public",
) -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "record_type": record_type.value,
        "venue": venue,
        "asset": asset,
        "event_time": received_time - timedelta(milliseconds=2),
        "exchange_time": received_time - timedelta(milliseconds=2),
        "received_time": received_time,
        "source_sequence": 1,
        "connection_id": connection_id,
    }


def _bbo(
    venue: str,
    asset: str,
    received_time: datetime,
    *,
    connection_id: str = "ws-public",
    update_id: str | None = None,
) -> dict[str, object]:
    row = _common(
        venue,
        asset,
        RecordType.BBO,
        received_time,
        schema_version=2,
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


def _trade(venue: str, asset: str, received_time: datetime) -> dict[str, object]:
    row = _common(
        venue,
        asset,
        RecordType.TRADE,
        received_time,
        schema_version=2,
    )
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


def _l2(
    venue: str,
    asset: str,
    received_time: datetime,
    *,
    connection_id: str = "ws-public",
    snapshot_prefix: str = "ws",
) -> tuple[dict[str, object], list[dict[str, object]]]:
    snapshot_id = f"{snapshot_prefix}:{venue}:{asset}:snapshot"
    book_epoch_id = f"{connection_id}:1"
    header = _common(
        venue,
        asset,
        RecordType.L2_BOOK_STATE,
        received_time,
        schema_version=1,
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
        ("ask", (("101", "3"),)),
        ("bid", (("100", "2"), ("99", "1"))),
    ):
        for level, (price, quantity) in enumerate(values):
            row = _common(
                venue,
                asset,
                RecordType.L2_SNAPSHOT,
                received_time,
                schema_version=1,
                connection_id=connection_id,
            )
            row["source_sequence"] = None
            row.update(
                {
                    "snapshot_id": snapshot_id,
                    "book_epoch_id": book_epoch_id,
                    "last_sequence": 10 if venue == "binance_usdm" else None,
                    "side": side,
                    "level": level,
                    "price": Decimal(price),
                    "quantity": Decimal(quantity),
                    "order_count": None if venue == "binance_usdm" else 1,
                }
            )
            levels.append(row)
    return header, levels


def _clock(received_time: datetime, *, valid: bool = True) -> dict[str, object]:
    row = _common(
        "binance_usdm",
        "GLOBAL",
        RecordType.CLOCK_SYNC,
        received_time,
        schema_version=2,
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
            "observation_id": f"clock:{int(valid)}",
            "connection_epoch": 1,
            "capture_epoch_id": "capture-1",
            "causal_valid_from": received_time if valid else None,
            "causal_valid_until": (received_time + timedelta(milliseconds=15_000) if valid else None),
            "sample_status": "valid" if valid else "invalid",
            "invalid_reason": None if valid else "uncertainty",
            "sampling_interval_ms": 1_000,
            "max_age_ms": 15_000,
            "max_uncertainty_ms": Decimal("50"),
        }
    )
    return row


def _write(
    root: Path,
    venue: str,
    asset: str,
    record_type: RecordType,
    rows: list[dict[str, object]],
    *,
    version: int,
) -> PartitionManifest:
    table = pa.Table.from_pylist(rows, schema=schema_for(record_type, version).schema)
    return write_partition(
        root,
        PartitionKey(venue, BASE.date(), asset, record_type),
        table,
    )


def _lake(root: Path, *, incomplete_l2: bool = False) -> tuple[PartitionManifest, ...]:
    manifests: list[PartitionManifest] = []
    for venue in VENUES:
        for asset_index, asset in enumerate(ASSETS):
            observed = STRICT_START + timedelta(seconds=1, milliseconds=asset_index)
            bbo_rows = [_bbo(venue, asset, observed)]
            header, levels = _l2(venue, asset, observed + timedelta(milliseconds=20))
            headers = [header]
            if venue == "hyperliquid" and asset == "BTC":
                bbo_rows.append(
                    _bbo(
                        venue,
                        asset,
                        observed + timedelta(milliseconds=1),
                        connection_id="rest-bootstrap",
                        update_id="rest:bootstrap",
                    )
                )
                rest_header, rest_levels = _l2(
                    venue,
                    asset,
                    observed + timedelta(milliseconds=30),
                    connection_id="rest-bootstrap",
                    snapshot_prefix="rest",
                )
                headers.append(rest_header)
                levels.extend(rest_levels)
            if incomplete_l2 and venue == "binance_usdm" and asset == "BTC":
                levels.pop()
            manifests.extend(
                (
                    _write(root, venue, asset, RecordType.BBO, bbo_rows, version=2),
                    _write(
                        root,
                        venue,
                        asset,
                        RecordType.TRADE,
                        [_trade(venue, asset, observed + timedelta(milliseconds=10))],
                        version=2,
                    ),
                    _write(
                        root,
                        venue,
                        asset,
                        RecordType.L2_BOOK_STATE,
                        headers,
                        version=1,
                    ),
                    _write(
                        root,
                        venue,
                        asset,
                        RecordType.L2_SNAPSHOT,
                        levels,
                        version=1,
                    ),
                )
            )
    manifests.append(
        _write(
            root,
            "binance_usdm",
            "GLOBAL",
            RecordType.CLOCK_SYNC,
            [_clock(STRICT_START), _clock(STRICT_START + timedelta(milliseconds=1), valid=False)],
            version=2,
        )
    )
    return tuple(manifests)


def _fake_saved(
    partition_count: int,
    row_count: int,
) -> SavedPhase10Gate:
    return cast(
        SavedPhase10Gate,
        SimpleNamespace(
            report={
                "validation": {
                    "inventory_partition_count": partition_count,
                    "inventory_row_count": row_count,
                }
            },
            gate_report_sha256="1" * 64,
            semantic_gate_sha256="2" * 64,
            canonicalizer_version="phase10_semantic_gate_payload_v1",
            excluded_json_pointers=("/observability",),
        ),
    )


def _admission(manifests: tuple[PartitionManifest, ...], root: Path) -> BoundedLeadLagGateAdmission:
    saved = _fake_saved(len(manifests), sum(manifest.row_count for manifest in manifests))
    return streaming_lake._build_admission(
        root=root.resolve(),
        start=BASE,
        end=BASE + timedelta(seconds=10),
        assets=ASSETS,
        intervals=(StrictInterval(STRICT_START, STRICT_END, "capture-1"),),
        saved_gate=saved,
        gate_report_sha256=saved.gate_report_sha256,
        semantic_gate_sha256=saved.semantic_gate_sha256,
        semantic_gate_canonicalizer_version=saved.canonicalizer_version,
        excluded_json_pointers=saved.excluded_json_pointers,
        clock_lookback=timedelta(seconds=15),
    )


@pytest.fixture(autouse=True)
def _stable_fake_gate_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        streaming_lake,
        "verify_saved_phase10_gate_unchanged",
        lambda _saved: None,
    )


def test_gate_admission_fails_before_analysis_manifest_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    fake_saved = cast(
        SavedPhase10Gate,
        SimpleNamespace(
            report={
                "assets": ["BTC", "ETH"],
                "requested_window": {
                    "start": BASE.isoformat(),
                    "end": (BASE + timedelta(seconds=10)).isoformat(),
                },
                "strict_phase_10_overlap": {
                    "intervals": [
                        {
                            "capture_epoch_id": "capture-1",
                            "start": STRICT_START.isoformat(),
                            "end": STRICT_END.isoformat(),
                        }
                    ]
                },
                "clock_sync": {"strict_max_age_ms": 15_000},
            },
            gate_report_sha256="1" * 64,
            semantic_gate_sha256="2" * 64,
            canonicalizer_version="phase10_semantic_gate_payload_v1",
            excluded_json_pointers=("/observability",),
        ),
    )
    monkeypatch.setattr(streaming_lake, "load_saved_phase10_gate", lambda _path: fake_saved)
    monkeypatch.setattr(streaming_lake, "audit_phase10_continuity", lambda *_a, **_k: {})
    monkeypatch.setattr(
        streaming_lake,
        "compare_saved_and_fresh_phase10_gate",
        lambda *_a, **_k: (_ for _ in ()).throw(ValueError("semantic mismatch")),
    )
    monkeypatch.setattr(
        streaming_lake,
        "_catalog_lake",
        lambda *_a, **_k: pytest.fail("staging must not start"),
    )

    with pytest.raises(ValueError, match="semantic mismatch"):
        validate_bounded_lead_lag_gate(root, tmp_path / "gate.json")


def test_projected_bounded_load_reconstructs_atomic_l2_and_writes_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    manifests = _lake(root)
    admission = _admission(manifests, root)
    real_iter = streaming_lake.iter_hashed_batches
    projections: dict[RecordType, set[tuple[str, ...]]] = {}

    def projected(*args: object, **kwargs: object):
        columns = kwargs.get("columns")
        assert isinstance(columns, tuple)
        manifest = cast(PartitionManifest, args[1])
        record_type = RecordType(manifest.partition.record_type)
        projections.setdefault(record_type, set()).add(columns)
        return real_iter(*args, **kwargs)

    monkeypatch.setattr(streaming_lake, "iter_hashed_batches", projected)
    selected = tmp_path / "evidence" / "selected_manifests.jsonl"
    with load_bounded_lead_lag_window(
        admission,
        tmp_path / "scratch",
        selected,
        batch_rows=2,
    ) as window:
        assert projections and all(projections.values())
        assert all("schema_version" not in columns for values in projections.values() for columns in values)
        for columns in projections[RecordType.BBO]:
            assert {"event_time", "exchange_time"}.isdisjoint(columns)
        for columns in projections[RecordType.TRADE]:
            assert {
                "event_time",
                "exchange_time",
                "is_liquidation",
                "connection_epoch",
            }.isdisjoint(columns)
        for columns in projections[RecordType.CLOCK_SYNC]:
            assert {
                "request_sent_time",
                "response_received_time",
                "server_time",
            }.isdisjoint(columns)
        assert window.assets == ASSETS
        assert window.excluded_json_pointers == ("/observability",)
        assert window.source_spool.total_rows == 12
        for kind in ("bbo", "trade", "l2"):
            for asset in ASSETS:
                rows = list(
                    window.source_spool.iter_rows(
                        kind=kind,
                        asset=asset,
                        start_ns=int(STRICT_START.timestamp() * 1_000_000_000),
                        end_ns=int(STRICT_END.timestamp() * 1_000_000_000),
                    )
                )
                assert len(rows) == 2
                assert {str(row["venue"]) for row in rows} == set(VENUES)
                assert not any(str(row.get("connection_id", "")).startswith("rest") for row in rows)
        l2 = list(
            window.source_spool.iter_rows(
                kind="l2",
                asset="BTC",
                start_ns=int(STRICT_START.timestamp() * 1_000_000_000),
                end_ns=int(STRICT_END.timestamp() * 1_000_000_000),
            )
        )
        assert all(row["bid_depth"] == Decimal("3") for row in l2)
        assert all(row["ask_depth"] == Decimal("3") for row in l2)
        assert all(len(cast(tuple[object, ...], row["bids"])) == 2 for row in l2)
        assert window.observability["max_record_batch_rows"] <= 2
        assert window.selected_manifest_count == window.selected_manifests_count
        assert window.observability["clock_sync_diagnostics"] == {
            "row_count": 1,
            "usage": "DIAGNOSTIC_ONLY_STRICT_INTERVALS_DEFINE_CAUSAL_VALIDITY",
            "received_time_min": "2026-08-15T12:00:01Z",
            "received_time_max": "2026-08-15T12:00:01Z",
        }
        assert window.observability["clock_input_diagnostics"] == {
            "row_count": 2,
            "received_time_min": "2026-08-15T12:00:01Z",
            "received_time_max": "2026-08-15T12:00:01.001000Z",
            "reported_valid_row_count": 1,
            "reported_invalid_row_count": 1,
            "causally_usable_row_count": 1,
        }
        assert window.observability["max_complete_simultaneous_source_batch_rows"] == 8
        dimensions = cast(dict[str, int], window.observability["rows_scanned_by_venue_asset_type"])
        assert dimensions["binance_usdm|BTC|bbo"] == 1
        assert dimensions["hyperliquid|BTC|bbo"] == 2
        retained_dimensions = cast(dict[str, int], window.observability["rows_retained_by_venue_asset_type"])
        assert retained_dimensions["hyperliquid|BTC|bbo"] == 1
        assert "estimated_source_spool_bytes" in cast(
            dict[str, int], window.observability["source_spool_preflight"]
        )
        lines = selected.read_bytes().splitlines()
        entries = [json.loads(line) for line in lines]
        assert len(entries) == window.selected_manifests_count
        expected_array = json.dumps(
            entries,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        assert window.manifest_fingerprint == hashlib.sha256(expected_array).hexdigest()
        assert window.selected_manifests_sha256 == hashlib.sha256(selected.read_bytes()).hexdigest()
        verify_immutable_inputs_unchanged(window)


def test_inventory_mismatch_fails_and_removes_partial_staging(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    manifests = _lake(root)
    admission = _admission(manifests, root)
    changed_saved = _fake_saved(len(manifests), sum(manifest.row_count for manifest in manifests) + 1)
    admission = streaming_lake._build_admission(
        root=admission.root,
        start=admission.start,
        end=admission.end,
        assets=admission.assets,
        intervals=admission.intervals,
        saved_gate=changed_saved,
        gate_report_sha256=changed_saved.gate_report_sha256,
        semantic_gate_sha256=changed_saved.semantic_gate_sha256,
        semantic_gate_canonicalizer_version=changed_saved.canonicalizer_version,
        excluded_json_pointers=changed_saved.excluded_json_pointers,
        clock_lookback=admission.clock_lookback,
    )
    scratch = tmp_path / "scratch"
    evidence = tmp_path / "selected.jsonl"

    with pytest.raises(StreamingLakeValidationError, match="lake inventory changed"):
        load_bounded_lead_lag_window(admission, scratch, evidence)

    assert not evidence.exists()
    assert not (scratch / "bounded-lead-lag-catalog.sqlite3").exists()
    assert not (scratch / "bounded-lead-lag-source.sqlite3").exists()


def test_incomplete_l2_fails_atomically_and_removes_evidence(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    manifests = _lake(root, incomplete_l2=True)
    admission = _admission(manifests, root)
    scratch = tmp_path / "scratch"
    evidence = tmp_path / "selected.jsonl"

    with pytest.raises(StreamingLakeValidationError, match="header level counts"):
        load_bounded_lead_lag_window(admission, scratch, evidence)

    assert not evidence.exists()
    assert not (scratch / "bounded-lead-lag-catalog.sqlite3").exists()
    assert not (scratch / "bounded-lead-lag-source.sqlite3").exists()


def test_prepublication_recheck_detects_selected_data_mutation(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    manifests = _lake(root)
    admission = _admission(manifests, root)
    window = load_bounded_lead_lag_window(
        admission,
        tmp_path / "scratch",
        tmp_path / "selected.jsonl",
    )
    try:
        selected_manifest = next(
            manifest for manifest in manifests if manifest.partition.record_type == RecordType.BBO
        )
        data_path = root / selected_manifest.relative_data_path
        payload = bytearray(data_path.read_bytes())
        payload[len(payload) // 2] ^= 1
        data_path.write_bytes(payload)

        with pytest.raises(StreamingLakeValidationError, match="selected immutable data changed"):
            verify_immutable_inputs_unchanged(window)
    finally:
        window.close()


def test_deterministic_bounded_reruns_publish_identical_evidence_and_rows(
    tmp_path: Path,
) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    manifests = _lake(root)
    admission = _admission(manifests, root)
    first = load_bounded_lead_lag_window(
        admission,
        tmp_path / "scratch-1",
        tmp_path / "selected-1.jsonl",
        batch_rows=1,
    )
    second = load_bounded_lead_lag_window(
        admission,
        tmp_path / "scratch-2",
        tmp_path / "selected-2.jsonl",
        batch_rows=3,
    )
    try:
        assert first.manifest_fingerprint == second.manifest_fingerprint
        assert first.selected_manifests_sha256 == second.selected_manifests_sha256
        assert first.selected_manifest_count == second.selected_manifest_count
        for kind in ("bbo", "trade", "l2"):
            for asset in ASSETS:
                bounds = {
                    "kind": kind,
                    "asset": asset,
                    "start_ns": int(STRICT_START.timestamp() * 1_000_000_000),
                    "end_ns": int(STRICT_END.timestamp() * 1_000_000_000),
                }
                assert list(first.source_spool.iter_rows(**bounds)) == list(
                    second.source_spool.iter_rows(**bounds)
                )
    finally:
        first.close()
        second.close()


def test_l2_receive_batch_bound_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    manifests = _lake(root)
    admission = _admission(manifests, root)
    evidence = tmp_path / "selected.jsonl"

    with pytest.raises(
        StreamingLakeValidationError,
        match="complete simultaneous source batch exceeds bounded state",
    ):
        load_bounded_lead_lag_window(
            admission,
            tmp_path / "scratch",
            evidence,
            max_l2_rows_per_receive=1,
        )

    assert not evidence.exists()


def test_source_spool_disk_preflight_fails_before_any_parquet_ingest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    manifests = _lake(root)
    admission = _admission(manifests, root)
    monkeypatch.setattr(
        streaming_lake.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(total=1, used=0, free=1),
    )
    monkeypatch.setattr(
        streaming_lake,
        "iter_hashed_batches",
        lambda *_a, **_k: pytest.fail("Parquet ingest must not start"),
    )
    evidence = tmp_path / "selected.jsonl"

    with pytest.raises(
        StreamingLakeValidationError,
        match="source-spool disk preflight failed before Parquet ingest",
    ):
        load_bounded_lead_lag_window(
            admission,
            tmp_path / "scratch",
            evidence,
            scratch_low_watermark_bytes=1,
            scratch_reserve_bytes=1,
        )

    assert not evidence.exists()


def test_clock_selection_is_restricted_to_the_two_gate_venues(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    manifests = list(_lake(root))
    unrelated_clock = _clock(STRICT_START + timedelta(milliseconds=2))
    unrelated_clock["venue"] = "coinbase"
    unrelated_clock["observation_id"] = "clock:coinbase"
    manifests.append(
        _write(
            root,
            "coinbase",
            "GLOBAL",
            RecordType.CLOCK_SYNC,
            [unrelated_clock],
            version=2,
        )
    )
    window = load_bounded_lead_lag_window(
        _admission(tuple(manifests), root),
        tmp_path / "scratch",
        tmp_path / "selected.jsonl",
    )
    try:
        clock_input = cast(dict[str, int], window.observability["clock_input_diagnostics"])
        assert clock_input["row_count"] == 2
        entries = [json.loads(line) for line in window.selected_manifests_path.read_bytes().splitlines()]
        assert not any(entry["partition"]["venue"] == "coinbase" for entry in entries)
    finally:
        window.close()


def test_lazy_catalog_shape_exceeds_sixty_thousand_files_without_a_file_list(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    root.mkdir()
    marker = root / f"part-{'0' * 64}.manifest.json"
    marker.write_bytes(b"{}\n")
    yielded = 0
    inserted = 0

    def lazy_files(_root: Path):
        nonlocal yielded
        for index in range(60_001):
            yielded += 1
            yield marker, f"synthetic-shape/{index}/part-{'0' * 64}.manifest.json"

    def counting_insert(*_args: object, **_kwargs: object) -> None:
        nonlocal inserted
        inserted += 1

    monkeypatch.setattr(streaming_lake, "_iter_lake_files", lazy_files)
    monkeypatch.setattr(streaming_lake, "_insert_manifest", counting_insert)
    catalog = streaming_lake._Catalog(tmp_path / "shape.sqlite3")
    try:
        counts = streaming_lake._catalog_lake(root.resolve(), catalog)
    finally:
        catalog.close()

    assert counts == (60_001, 60_001, 0)
    assert yielded == inserted == 60_001


def test_physical_fragmentation_changes_evidence_but_not_logical_spooled_rows(
    tmp_path: Path,
) -> None:
    combined_root = tmp_path / "combined"
    fragmented_root = tmp_path / "fragmented"
    combined_root.mkdir()
    fragmented_root.mkdir()
    combined_manifests = list(_lake(combined_root))
    fragmented_manifests = list(_lake(fragmented_root))
    extra_rows = [
        _bbo(
            "binance_usdm",
            "BTC",
            STRICT_START + timedelta(seconds=2, milliseconds=offset),
            update_id=f"ws:extra:{offset}",
        )
        for offset in (100, 200)
    ]
    for sequence, row in enumerate(extra_rows, start=2):
        row["source_sequence"] = sequence
    combined_manifests.append(
        _write(
            combined_root,
            "binance_usdm",
            "BTC",
            RecordType.BBO,
            extra_rows,
            version=2,
        )
    )
    for row in extra_rows:
        fragmented_manifests.append(
            _write(
                fragmented_root,
                "binance_usdm",
                "BTC",
                RecordType.BBO,
                [row],
                version=2,
            )
        )
    combined = load_bounded_lead_lag_window(
        _admission(tuple(combined_manifests), combined_root),
        tmp_path / "scratch-combined",
        tmp_path / "selected-combined.jsonl",
    )
    fragmented = load_bounded_lead_lag_window(
        _admission(tuple(fragmented_manifests), fragmented_root),
        tmp_path / "scratch-fragmented",
        tmp_path / "selected-fragmented.jsonl",
    )
    try:
        assert combined.manifest_fingerprint != fragmented.manifest_fingerprint
        bounds = {
            "kind": "bbo",
            "asset": "BTC",
            "start_ns": int(STRICT_START.timestamp() * 1_000_000_000),
            "end_ns": int(STRICT_END.timestamp() * 1_000_000_000),
        }
        assert list(combined.source_spool.iter_rows(**bounds)) == list(
            fragmented.source_spool.iter_rows(**bounds)
        )
    finally:
        combined.close()
        fragmented.close()
