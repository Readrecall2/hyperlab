from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from pathlib import Path

import pytest

import hyperlab.paper.storage_v4.capacity_shape as capacity_shape_module
from hyperlab.backtest.protocol import canonical_json, canonical_sha256
from hyperlab.paper.golden_v3 import GOLDEN_STREAM_NAMES, GoldenVerification
from hyperlab.paper.storage_v4.canonical import CanonicalizationError, canonical_json_bytes
from hyperlab.paper.storage_v4.capacity import CAPACITY_MARKERS, CapacityProfile
from hyperlab.paper.storage_v4.capacity_shape import (
    GOLDEN_PAYLOAD_SIZE_BASIS,
    GoldenCapacityShapeError,
    derive_golden_capacity_shape,
)
from hyperlab.paper.storage_v4.phase1c_progress import Phase1CHeartbeatWindow


def _fixture(tmp_path: Path) -> tuple[GoldenVerification, dict[str, list[dict[str, object]]]]:
    payloads = (
        {"input_type": "TYPE_A", "value": 1},
        {"input_type": "TYPE_A", "value": 1},
        {"input_type": "TYPE_B", "value": "longer"},
    )
    times = (
        "2026-08-25T00:00:00.000000Z",
        "2026-08-25T00:00:01.000000Z",
        "2026-08-25T00:00:03.000000Z",
    )
    streams = {name: [] for name in GOLDEN_STREAM_NAMES}
    streams["inbox"] = [
        {
            "created_at": times[index],
            "payload": payload,
            "payload_hash": hashlib.sha256(canonical_json_bytes(payload)).hexdigest(),
        }
        for index, payload in enumerate(payloads)
    ]
    streams["alerts"] = [
        {"code": "MARKET_GAP", "value": 1},
        {"code": "RISK_REJECTED", "value": 2},
    ]
    streams["incidents"] = [{"incident": "one"}]
    streams["ledger_transactions"] = [{"transaction": "one"}]
    streams["commits"] = [{}, {}, {}]
    streams["projection_history"] = [{}, {}, {}, {}]
    manifest = {
        "census": {
            "commit_count": 3,
            "input_type_counts": {"TYPE_A": 2, "TYPE_B": 1},
            "strategy_ids": ["phase05_cash_and_carry", "phase08_robust_pairs"],
        },
        "source": {"sha256": "b" * 64},
        "streams": {
            name: {"row_count": len(streams[name])} for name in GOLDEN_STREAM_NAMES
        },
    }
    verification = GoldenVerification(
        export_root=tmp_path / "not-read",
        root_hash="a" * 64,
        manifest=manifest,
    )
    return verification, streams


def test_golden_shape_is_fact_derived_and_builds_visible_synthetic_config(
    tmp_path: Path,
) -> None:
    verification, streams = _fixture(tmp_path)
    observed_cardinality_stores: list[Path] = []

    def stream_factory(
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        if name == "inbox":
            observed_cardinality_stores.extend(
                tmp_path.rglob("payload-cardinality.sqlite3")
            )
        return streams[name]

    shape = derive_golden_capacity_shape(
        verification,
        stream_factory=stream_factory,
        scratch_parent=tmp_path,
    )
    assert len(observed_cardinality_stores) == 1
    assert observed_cardinality_stores[0].is_absolute()
    assert list(tmp_path.iterdir()) == []
    assert [item.record_type for item in shape.type_observations] == ["TYPE_A", "TYPE_B"]
    assert shape.type_observations[0].count == 2
    assert shape.type_observations[0].distinct_payload_hashes == 1
    assert shape.cadence_ns == 1_500_000_000
    assert shape.market_gap_count == 1
    assert shape.payload()["payload_size_basis"] == GOLDEN_PAYLOAD_SIZE_BASIS
    assert shape.payload()["markers"] == list(CAPACITY_MARKERS)
    assert shape.sha256 == hashlib.sha256(shape.canonical_bytes).hexdigest()

    config = shape.workload_config(commit_count=5, seed=23)
    assert config.profile is CapacityProfile.GOLDEN_SHAPED
    assert config.golden_census_sha256 == shape.sha256
    assert config.market_gap_count == 2
    assert config.type_distribution[0].weight == 2
    assert config.type_distribution[0].stream == "inbox"
    assert config.strategies == (
        "phase05_cash_and_carry",
        "phase08_robust_pairs",
    )


def test_golden_shape_uses_golden_v3_finite_float_encoding_without_relaxing_native(
    tmp_path: Path,
) -> None:
    verification, streams = _fixture(tmp_path)
    payload = {
        "decision": {"signal": {"hedge_ratio": 0.125}},
        "input_type": "TYPE_A",
    }
    streams["inbox"][0]["payload"] = payload
    streams["inbox"][0]["payload_hash"] = canonical_sha256(payload)
    streams["alerts"][0]["value"] = 0.25
    streams["incidents"][0]["value"] = 1.5
    streams["ledger_transactions"][0]["value"] = 2.5
    expected_payload_bytes = (
        b'{"decision":{"signal":{"hedge_ratio":0.125}},"input_type":"TYPE_A"}'
    )

    with pytest.raises(CanonicalizationError, match="float values are forbidden"):
        canonical_json_bytes(payload)
    assert canonical_json(payload).encode("utf-8") == expected_payload_bytes

    def stream_factory(
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        return streams[name]

    shape = derive_golden_capacity_shape(
        verification,
        stream_factory=stream_factory,
        scratch_parent=tmp_path,
    )
    type_a = next(item for item in shape.type_observations if item.record_type == "TYPE_A")
    expected_type_a_sizes = (
        len(expected_payload_bytes),
        len(canonical_json(streams["inbox"][1]["payload"]).encode("utf-8")),
    )
    assert type_a.payload_min_bytes == min(expected_type_a_sizes)
    assert type_a.payload_max_bytes == max(expected_type_a_sizes)
    assert type_a.distinct_payload_hashes == 2
    assert shape.market_gap_payload_bytes == len(
        canonical_json(streams["alerts"][0]).encode("utf-8")
    ) + 1
    assert shape.incident_payload_bytes == len(
        canonical_json(streams["incidents"][0]).encode("utf-8")
    ) + 1
    assert shape.ledger_payload_bytes == len(
        canonical_json(streams["ledger_transactions"][0]).encode("utf-8")
    ) + 1
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("invalid", (float("nan"), float("inf"), float("-inf")))
def test_golden_shape_rejects_non_finite_golden_payload_floats(
    tmp_path: Path,
    invalid: float,
) -> None:
    verification, streams = _fixture(tmp_path)
    streams["inbox"][0]["payload"] = {
        "decision": {"signal": {"hedge_ratio": invalid}},
        "input_type": "TYPE_A",
    }

    def stream_factory(
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        return streams[name]

    with pytest.raises(
        GoldenCapacityShapeError,
        match="Golden inbox payload is not valid Golden V3 canonical JSON",
    ) as exc_info:
        derive_golden_capacity_shape(
            verification,
            stream_factory=stream_factory,
            scratch_parent=tmp_path,
        )
    assert isinstance(exc_info.value.__cause__, ValueError)
    assert list(tmp_path.iterdir()) == []


def test_golden_shape_rejects_well_formed_payload_hash_mismatch(tmp_path: Path) -> None:
    verification, streams = _fixture(tmp_path)
    streams["inbox"][0]["payload_hash"] = "c" * 64

    def stream_factory(
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        return streams[name]

    with pytest.raises(
        GoldenCapacityShapeError,
        match="payload hash differs from canonical payload",
    ):
        derive_golden_capacity_shape(
            verification,
            stream_factory=stream_factory,
            scratch_parent=tmp_path,
        )
    assert list(tmp_path.iterdir()) == []


def test_golden_shape_progress_is_streaming_and_result_is_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    verification, streams = _fixture(tmp_path)
    payload = {"input_type": "TYPE_A", "value": 1}
    payload_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
    streams["inbox"] = [
        {
            "created_at": (
                "2026-08-25T00:00:00.000000Z"
                if index == 0
                else "2026-08-25T00:00:01.000000Z"
            ),
            "payload": payload,
            "payload_hash": payload_hash,
        }
        for index in range(4_097)
    ]
    verification.manifest["census"] = {
        "commit_count": 4_097,
        "input_type_counts": {"TYPE_A": 4_097},
        "strategy_ids": ["phase05_cash_and_carry", "phase08_robust_pairs"],
    }
    verification.manifest["streams"]["inbox"]["row_count"] = 4_097  # type: ignore[index]

    def stream_factory(
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        return streams[name]

    baseline = derive_golden_capacity_shape(
        verification,
        stream_factory=stream_factory,
        scratch_parent=tmp_path,
    )
    clock = iter(range(10_000, 11_000, 100))
    monkeypatch.setattr(capacity_shape_module, "perf_counter_ns", lambda: next(clock))
    events: list[dict[str, object]] = []
    observed = derive_golden_capacity_shape(
        verification,
        stream_factory=stream_factory,
        scratch_parent=tmp_path,
        progress=lambda event: events.append(dict(event)),
    )

    assert observed == baseline
    assert observed.canonical_bytes == baseline.canonical_bytes
    assert observed.sha256 == baseline.sha256
    assert observed.logical_row_count == 4_108
    assert [
        (
            event["stream"],
            event["status"],
            event["rows_completed"],
            event["rows_total"],
            event["logical_row_offset"],
            event["commits_completed"],
            event["logical_rows_completed"],
            event["elapsed_ns"],
        )
        for event in events
    ] == [
        ("inbox", "started", 0, 4_097, 0, 0, 0, 100),
        ("inbox", "running", 4_096, 4_097, 0, 4_096, 4_096, 200),
        ("inbox", "complete", 4_097, 4_097, 0, 4_097, 4_097, 300),
        ("alerts", "started", 0, 2, 4_097, 4_097, 4_097, 400),
        ("alerts", "complete", 2, 2, 4_097, 4_097, 4_099, 500),
        ("incidents", "started", 0, 1, 4_099, 4_097, 4_099, 600),
        ("incidents", "complete", 1, 1, 4_099, 4_097, 4_100, 700),
        (
            "ledger_transactions",
            "started",
            0,
            1,
            4_100,
            4_097,
            4_100,
            800,
        ),
        (
            "ledger_transactions",
            "complete",
            1,
            1,
            4_100,
            4_097,
            4_101,
            900,
        ),
    ]
    assert [event["workload_elapsed_ns"] for event in events] == list(
        range(100, 1_000, 100)
    )
    assert all(
        event["phase"] == "golden_capacity_shape_scan"
        and event["workload"] == "GOLDEN_V3_CAPACITY_SHAPE_SCAN"
        and event["workload_profile"] == "GOLDEN_SHAPED"
        and event["workload_id"] == f"golden-shape:{'a' * 64}"
        and event["golden_root_hash"] == "a" * 64
        and event["commits_total"] == 4_097
        and event["logical_rows_total"] == 4_101
        and event["raw_segment_count"] == 0
        and event["paper_segment_count"] == 0
        and event["segment_count"] == 0
        and event["checkpoint_count"] == 0
        and event["segment_checkpoint_status"]
        == "EXACT_ZERO_NOT_APPLICABLE_READ_ONLY_GOLDEN_CENSUS"
        and event["progress_metrics_scope"]
        == "AUTHENTICATED_GOLDEN_DESCRIPTOR_ROWS_FOR_SCANNED_STREAMS_ONLY"
        and event["rows_completed_scope"] == "CURRENT_STREAM_ONLY"
        for event in events
    )
    window = Phase1CHeartbeatWindow()
    rendered = [
        window.render(
            event,
            observed_elapsed_ns=int(event["workload_elapsed_ns"]),
        )
        for event in events
    ]
    assert any(
        item["recent_throughput_status"] == "AVAILABLE_SAME_WORKLOAD_WINDOW"
        and item["conservative_eta_ns"] is not None
        for item in rendered
    )


@pytest.mark.parametrize(
    "stream",
    ("inbox", "alerts", "incidents", "ledger_transactions"),
)
def test_golden_shape_refuses_scanned_stream_descriptor_count_divergence(
    tmp_path: Path,
    stream: str,
) -> None:
    verification, streams = _fixture(tmp_path)
    descriptor = verification.manifest["streams"][stream]  # type: ignore[index]
    descriptor["row_count"] += 1  # type: ignore[index,operator]

    def stream_factory(
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        return streams[name]

    with pytest.raises(GoldenCapacityShapeError, match="descriptor"):
        derive_golden_capacity_shape(
            verification,
            stream_factory=stream_factory,
            scratch_parent=tmp_path,
        )
    assert list(tmp_path.iterdir()) == []


def test_golden_shape_refuses_manifest_inbox_count_divergence(tmp_path: Path) -> None:
    verification, streams = _fixture(tmp_path)
    verification.manifest["census"]["commit_count"] = 4  # type: ignore[index]

    def stream_factory(
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        return streams[name]

    with pytest.raises(GoldenCapacityShapeError, match="differs"):
        derive_golden_capacity_shape(verification, stream_factory=stream_factory)


def test_golden_shape_removes_disk_cardinality_store_after_error(
    tmp_path: Path,
) -> None:
    verification, streams = _fixture(tmp_path)
    streams["inbox"][1]["payload_hash"] = "invalid"

    def stream_factory(
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        return streams[name]

    with pytest.raises(GoldenCapacityShapeError, match="payload_hash"):
        derive_golden_capacity_shape(
            verification,
            stream_factory=stream_factory,
            scratch_parent=tmp_path,
        )
    assert list(tmp_path.iterdir()) == []


def test_golden_shape_rejects_unknown_types_without_unbounded_aggregation(
    tmp_path: Path,
) -> None:
    verification, streams = _fixture(tmp_path)
    consumed = 0

    def unknown_rows() -> Iterable[Mapping[str, object]]:
        nonlocal consumed
        for index in range(10_000):
            consumed += 1
            payload = {"input_type": f"UNKNOWN_{index}", "value": index}
            yield {
                "created_at": "2026-08-25T00:00:00.000000Z",
                "payload": payload,
                "payload_hash": hashlib.sha256(
                    canonical_json_bytes(payload)
                ).hexdigest(),
            }

    def stream_factory(
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        return unknown_rows() if name == "inbox" else streams[name]

    with pytest.raises(GoldenCapacityShapeError, match="absent from manifest census"):
        derive_golden_capacity_shape(
            verification,
            stream_factory=stream_factory,
            scratch_parent=tmp_path,
        )
    assert consumed == 1
    assert list(tmp_path.iterdir()) == []
