from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from hyperlab.analysis.gate_binding import (
    PHASE10_SEMANTIC_GATE_CANONICALIZER_VERSION,
    PHASE10_SEMANTIC_GATE_EXCLUDED_JSON_POINTERS,
    Phase10GateBindingError,
    compare_saved_and_fresh_phase10_gate,
    load_saved_phase10_gate,
    parse_saved_phase10_gate_json,
    phase10_semantic_gate_payload_v1,
    semantic_phase10_gate_v1,
    verify_saved_phase10_gate_unchanged,
)

_PHASES = (
    "cross_segment_integrity",
    "scratch_index_build",
    "manifest_validation",
    "projected_row_scan_and_spool",
    "connection_lineage",
    "connection_events_and_outages",
    "raw_normalized_lineage",
    "orphan_lineage",
    "market_coverage_intervals",
    "clock_causal_coverage",
    "strict_overlap",
    "bounded_gap_validation",
    "semantic_validation",
    "total",
)


def _observability() -> dict[str, object]:
    return {
        "semantic": False,
        "files": {
            "manifest_files_discovered": 8,
            "manifest_files_validated": 8,
            "manifest_files_selected": 8,
            "manifest_files_pruned": 0,
            "unique_parquet_files_scanned": 8,
            "parquet_file_scan_operations": 8,
        },
        "rows": {
            "validated_total": 32,
            "scanned_total": 32,
            "semantic_scanned_total": 24,
            "staged_total": 24,
            "scanned_by_record_type": {"bbo": 8, "trade": 8},
        },
        "bounded_state": {
            "record_batches_scanned": 8,
            "max_record_batch_rows": 4,
            "max_file_rows": 4,
            "max_file_size_bytes": 1024,
            "max_python_rows_per_batch": 4,
            "max_boundary_candidates": 2,
            "wire_identity_keys": 4,
            "integrity_primary_keys_spilled": 8,
            "integrity_l2_metadata_keys_spilled": 4,
            "integrity_cadence_rows_spilled": 4,
            "sqlite_cache_limit_bytes": 33_554_432,
            "sqlite_commit_interval_rows": 8192,
            "sqlite_commits": 1,
            "max_uncommitted_rows": 24,
            "sqlite_mmap_bytes": 0,
            "spilled_timestamp_rows": 24,
            "spilled_sequence_rows": 4,
            "spilled_set_keys": 8,
            "peak_scratch_bytes": 4096,
        },
        "elapsed_seconds_by_phase": {phase: 0.001 for phase in _PHASES},
    }


def _gate_report() -> dict[str, object]:
    trade_counts = {
        asset: {
            "normalized_count": 1,
            "normalized_with_raw_lineage_count": 1,
            "raw_agg_trade_count": 1,
            "raw_agg_trade_with_role_lineage_count": 1,
        }
        for asset in ("BTC", "ETH")
    }
    empty_lineage = {
        "eligible_capture_generations": ["capture-1"],
        "market_active_invalid_capture_generations": [],
        "incomplete_capture_generations": [],
        "ambiguous_or_wrong_role_connect_identities": 0,
        "unbound_connect_events": 0,
        "normalized_market_lineage_rejections": 0,
    }
    return {
        "audit_version": 1,
        "phase_10_status": "BLOCKED_PRECONDITION_NOT_MET",
        "technical_capture_gate": "PASS",
        "assets": ["BTC", "ETH"],
        "requested_window": {
            "start": "2026-08-15T00:00:00.000000Z",
            "end": "2026-08-15T00:00:10.000000Z",
            "duration_seconds": 10.0,
            "leading_unassessed_seconds": 0.0,
            "trailing_unassessed_seconds": 0.0,
            "max_unassessed_margin_ms": 15_000,
            "leading_margin_within_limit": True,
            "trailing_margin_within_limit": True,
            "trailing_terminal_roles_complete": False,
        },
        "policy": {
            "interval_semantics": "half_open_received_time_causal",
            "state_ttl_ms": 30_000,
            "trade_semantics": "point_event_causal_freshness_no_interpolation",
            "trade_freshness_ms": 30_000,
            "binance_l2_requires_v2_resync_complete": True,
            "clock_legacy_v1_usable": False,
            "clock_max_sampling_interval_ms": 10_000,
            "clock_max_age_ms": 15_000,
            "clock_max_uncertainty_ms": 50.0,
            "clock_actual_sample_spacing_enforced": True,
            "clock_sample_spacing_population": ("all_persisted_identity_bound_v2_clock_sync_attempts"),
            "clock_sample_spacing_timestamp": "request_sent_time",
            "clock_sample_spacing_bounds": ("active_generation_clipped_to_requested_window"),
            "clock_identity_requires_v2_wire_lineage": True,
            "clock_offset_uncertainty_bands_must_overlap": True,
            "physical_connection_roles_required": {
                "binance_usdm": ["market", "public"],
                "hyperliquid": ["public"],
            },
            "market_lineage_requires_exact_raw_payload": True,
            "assessed_span_starts_at_market_readiness_or_initial_clock": True,
            "initial_clock_acquisition_max_delay_ms": 15_000,
            "interpolate_across_capture_generations": False,
            "phase_10_may_be_unblocked_by_this_audit": False,
        },
        "binance_trades": {
            "normalized_total": 2,
            "normalized_with_raw_lineage_total": 2,
            "raw_agg_trade_total": 2,
            "raw_agg_trade_with_role_lineage_total": 2,
            "by_asset": trade_counts,
        },
        "connection_lineage": {
            "binance_usdm": copy.deepcopy(empty_lineage),
            "hyperliquid": {
                **copy.deepcopy(empty_lineage),
                "multiple_active_capture_generations": False,
            },
        },
        "connection_events": {
            venue: {
                "unbound_gap_or_disconnect_events": 0,
                "unbound_resync_events": 0,
                "in_window_gap_events": 0,
                "unclean_in_window_disconnect_events": 0,
                "event_active_capture_generations": ["capture-1"],
                "failure_events_by_capture_generation": {},
            }
            for venue in ("binance_usdm", "hyperliquid")
        },
        "required_wire_lineage": {
            "orphan_required_wire_total": 0,
            "by_venue_asset": {venue: {"BTC": 0, "ETH": 0} for venue in ("binance_usdm", "hyperliquid")},
        },
        "normalized_l2_level_lineage": {
            "orphan_level_total": 0,
            "by_venue_asset": {venue: {"BTC": 0, "ETH": 0} for venue in ("binance_usdm", "hyperliquid")},
        },
        "binance_l2_resync": {"missing_count": 0, "missing": []},
        "clock_sync": {
            "legacy_v1_ignored": 0,
            "valid_v2_samples": 2,
            "invalid_v2_samples": 0,
            "rejected_probe_samples": 0,
            "hard_invalid_v2_samples": 0,
            "failure_events": 0,
            "strict_policy_rejections": 0,
            "wire_identity_rejections": 0,
            "unbound_invalid_events": 0,
            "in_window_invalid_events": 0,
            "in_window_rejected_probe_events": 0,
            "in_window_hard_invalid_events": 0,
            "in_window_failure_events": 0,
            "consecutive_rejection_violations": 0,
            "consecutive_rejection_violation_capture_generations": [],
            "consecutive_rejection_outages": [],
            "max_consecutive_rejected_probes": 0,
            "strict_max_consecutive_rejected_probes": 1,
            "sample_spacing_violations": 0,
            "sample_spacing_violation_capture_generations": [],
            "sample_spacing_population": ("all_persisted_identity_bound_v2_clock_sync_attempts"),
            "sample_spacing_timestamp": "request_sent_time",
            "sample_spacing_bounds": "active_generation_clipped_to_requested_window",
            "offset_discontinuities": 0,
            "offset_discontinuity_capture_generations": [],
            "actual_max_sample_gap_ms": 1000.0,
            "actual_max_cadence_gap_ms": 1000.0,
            "strict_max_sampling_interval_ms": 10_000,
            "strict_max_age_ms": 15_000,
            "strict_max_uncertainty_ms": 50.0,
            "eligible_capture_generations": ["capture-1"],
            "market_active_capture_generations": ["capture-1"],
            "market_active_without_valid_clock": [],
            "assessed_capture_generations": ["capture-1"],
            "market_ready_at_by_capture": {"capture-1": "2026-08-15T00:00:00.000000Z"},
            "initial_acquisition_delay_ms_by_capture": {"capture-1": 0.0},
            "initial_acquisition_delay_violations": [],
            "assessed_span": {
                "start": "2026-08-15T00:00:00.000000Z",
                "end": "2026-08-15T00:00:10.000000Z",
            },
            "causal_coverage_continuous": True,
            "coverage_continuous": True,
            "valid_duration_seconds": 10.0,
            "uncovered_seconds": 0.0,
            "internal_gap_count": 0,
            "generation_gap_count": 0,
            "requested_window_leading_gap_seconds": 0.0,
            "requested_window_trailing_gap_seconds": 0.0,
            "intervals": [],
        },
        "strict_phase_10_overlap": {
            "duration_seconds": 8.0,
            "interval_count": 1,
            "by_asset": {
                "BTC": {"interval_count": 1, "duration_seconds": 8.0},
                "ETH": {"interval_count": 1, "duration_seconds": 8.0},
            },
            "intervals": [
                {
                    "capture_epoch_id": "capture-1",
                    "start": "2026-08-15T00:00:01.000000Z",
                    "end": "2026-08-15T00:00:09.000000Z",
                    "duration_seconds": 8.0,
                }
            ],
        },
        "validation": {
            "inventory_partition_count": 8,
            "inventory_row_count": 32,
            "relevant_gap_count": 0,
            "relevant_gaps": [],
        },
        "failure_reasons": [],
        "observability": _observability(),
    }


def _write_report(
    path: Path,
    report: dict[str, object],
    *,
    indent: int | None = 2,
) -> bytes:
    payload = (json.dumps(report, ensure_ascii=False, allow_nan=False, indent=indent) + "\n").encode()
    path.write_bytes(payload)
    return payload


def _set_path(report: dict[str, object], path: tuple[str, ...], value: object) -> None:
    current: dict[str, object] = report
    for key in path[:-1]:
        candidate = current[key]
        assert isinstance(candidate, dict)
        current = candidate
    current[path[-1]] = value


def test_load_binds_exact_bytes_and_versioned_semantic_payload(tmp_path: Path) -> None:
    report = _gate_report()
    nested = report["policy"]
    assert isinstance(nested, dict)
    nested["observability"] = {"semantic_evidence": "retained"}
    path = tmp_path / "gate.json"
    exact = _write_report(path, report)

    saved = load_saved_phase10_gate(path)

    assert saved.exact_bytes == exact
    assert saved.gate_report_sha256 == hashlib.sha256(exact).hexdigest()
    assert saved.semantic_gate_sha256 == hashlib.sha256(saved.semantic.canonical_bytes).hexdigest()
    assert saved.canonicalizer_version == (PHASE10_SEMANTIC_GATE_CANONICALIZER_VERSION)
    assert saved.excluded_json_pointers == (PHASE10_SEMANTIC_GATE_EXCLUDED_JSON_POINTERS)
    assert "observability" not in saved.semantic.payload
    semantic_policy = saved.semantic.payload["policy"]
    assert isinstance(semantic_policy, dict)
    assert semantic_policy["observability"] == {"semantic_evidence": "retained"}


def test_top_level_observability_is_the_only_excluded_pointer() -> None:
    baseline = _gate_report()
    telemetry_changed = copy.deepcopy(baseline)
    _set_path(
        telemetry_changed,
        ("observability", "files", "manifest_files_discovered"),
        999,
    )
    _set_path(
        telemetry_changed,
        ("observability", "elapsed_seconds_by_phase", "total"),
        123.5,
    )

    assert phase10_semantic_gate_payload_v1(baseline) == (phase10_semantic_gate_payload_v1(telemetry_changed))

    nested_changed = copy.deepcopy(baseline)
    baseline_policy = baseline["policy"]
    changed_policy = nested_changed["policy"]
    assert isinstance(baseline_policy, dict)
    assert isinstance(changed_policy, dict)
    baseline_policy["observability"] = {"value": 1}
    changed_policy["observability"] = {"value": 2}
    assert phase10_semantic_gate_payload_v1(baseline) != (phase10_semantic_gate_payload_v1(nested_changed))


def test_saved_and_fresh_comparison_allows_only_observability_drift(
    tmp_path: Path,
) -> None:
    saved_report = _gate_report()
    path = tmp_path / "gate.json"
    _write_report(path, saved_report)
    saved = load_saved_phase10_gate(path)
    fresh = copy.deepcopy(saved_report)
    _set_path(
        fresh,
        ("observability", "elapsed_seconds_by_phase", "total"),
        99.0,
    )

    compared = compare_saved_and_fresh_phase10_gate(saved, fresh)

    assert compared.semantic_gate_sha256 == saved.semantic_gate_sha256

    fresh["new_semantic_evidence"] = {"count": 1}
    with pytest.raises(Phase10GateBindingError, match="semantic fresh re-audit"):
        compare_saved_and_fresh_phase10_gate(saved, fresh)


@pytest.mark.parametrize(
    "raw",
    (
        b'{"audit_version":1,"audit_version":1}',
        b'{"outer":{"key":1,"key":2}}',
        b'{"value":NaN}',
        b'{"value":Infinity}',
        b'{"value":1e400}',
    ),
)
def test_saved_json_rejects_duplicate_keys_and_nonfinite_numbers(raw: bytes) -> None:
    with pytest.raises(Phase10GateBindingError):
        parse_saved_phase10_gate_json(raw)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    (
        (("observability", "semantic"), True, "semantic must be false"),
        (
            ("observability", "files", "manifest_files_discovered"),
            True,
            "JSON integer",
        ),
        (
            ("observability", "files", "manifest_files_discovered"),
            -1,
            "nonnegative",
        ),
        (
            ("observability", "files", "manifest_files_discovered"),
            1.5,
            "JSON integer",
        ),
        (
            ("observability", "elapsed_seconds_by_phase", "total"),
            -0.1,
            "nonnegative",
        ),
        (
            ("observability", "rows", "scanned_by_record_type", "unknown"),
            1,
            "unknown record types",
        ),
    ),
)
def test_observability_rejects_invalid_values(path: tuple[str, ...], value: object, message: str) -> None:
    report = _gate_report()
    _set_path(report, path, value)

    with pytest.raises(Phase10GateBindingError, match=message):
        phase10_semantic_gate_payload_v1(report)


def test_observability_requires_exact_known_schema() -> None:
    missing = _gate_report()
    observability = missing["observability"]
    assert isinstance(observability, dict)
    del observability["rows"]
    with pytest.raises(Phase10GateBindingError, match="invalid schema"):
        phase10_semantic_gate_payload_v1(missing)

    extra = _gate_report()
    extra_observability = extra["observability"]
    assert isinstance(extra_observability, dict)
    extra_observability["new_section"] = {}
    with pytest.raises(Phase10GateBindingError, match=r"unknown=.*new_section"):
        phase10_semantic_gate_payload_v1(extra)

    unknown_phase = _gate_report()
    elapsed = unknown_phase["observability"]
    assert isinstance(elapsed, dict)
    phase_map = elapsed["elapsed_seconds_by_phase"]
    assert isinstance(phase_map, dict)
    phase_map["new_phase"] = 0.0
    with pytest.raises(Phase10GateBindingError, match=r"unknown=.*new_phase"):
        phase10_semantic_gate_payload_v1(unknown_phase)


@pytest.mark.parametrize(
    ("path", "value"),
    (
        (("policy", "state_ttl_ms"), 30_001),
        (("policy", "clock_max_sampling_interval_ms"), 10_001),
        (("policy", "clock_max_age_ms"), 15_001),
        (("policy", "clock_max_uncertainty_ms"), 50.1),
        (("policy", "interpolate_across_capture_generations"), True),
        (("policy", "phase_10_may_be_unblocked_by_this_audit"), True),
        (("clock_sync", "strict_max_consecutive_rejected_probes"), 2),
        (("clock_sync", "strict_max_age_ms"), 15_001),
        (("clock_sync", "strict_max_uncertainty_ms"), 50.1),
    ),
)
def test_versioned_contract_rejects_weakened_thresholds(path: tuple[str, ...], value: object) -> None:
    report = _gate_report()
    _set_path(report, path, value)

    with pytest.raises(Phase10GateBindingError):
        phase10_semantic_gate_payload_v1(report)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    (
        (
            (
                "connection_lineage",
                "binance_usdm",
                "ambiguous_or_wrong_role_connect_identities",
            ),
            1,
            "invalid binance_usdm connection lineage",
        ),
        (
            ("connection_events", "hyperliquid", "event_active_capture_generations"),
            [],
            "fatal hyperliquid connection events",
        ),
        (
            ("strict_phase_10_overlap", "by_asset", "ETH", "duration_seconds"),
            7.0,
            "must match the common strict intervals",
        ),
    ),
)
def test_passing_gate_rejects_internally_inconsistent_lineage_and_overlap(
    path: tuple[str, ...], value: object, message: str
) -> None:
    report = _gate_report()
    _set_path(report, path, value)

    with pytest.raises(Phase10GateBindingError, match=message):
        phase10_semantic_gate_payload_v1(report)


def test_passing_gate_rejects_embedded_connection_failures() -> None:
    report = _gate_report()
    _set_path(
        report,
        (
            "connection_events",
            "binance_usdm",
            "failure_events_by_capture_generation",
        ),
        {"capture-1": [{"reason": "synthetic failure"}]},
    )

    with pytest.raises(
        Phase10GateBindingError,
        match="fatal binance_usdm connection events",
    ):
        phase10_semantic_gate_payload_v1(report)


def test_unsupported_or_nonpassing_gate_is_rejected() -> None:
    unsupported = _gate_report()
    unsupported["audit_version"] = 2
    with pytest.raises(Phase10GateBindingError, match="unsupported"):
        phase10_semantic_gate_payload_v1(unsupported)

    bool_version = _gate_report()
    bool_version["audit_version"] = True
    with pytest.raises(Phase10GateBindingError, match="JSON integer"):
        phase10_semantic_gate_payload_v1(bool_version)

    failed = _gate_report()
    failed["technical_capture_gate"] = "FAIL"
    failed["failure_reasons"] = ["clock_sync_not_continuous"]
    with pytest.raises(Phase10GateBindingError, match="technical_capture_gate=PASS"):
        phase10_semantic_gate_payload_v1(failed)


def test_raw_serialization_changes_hash_but_not_semantic_hash(tmp_path: Path) -> None:
    report = _gate_report()
    compact_path = tmp_path / "compact.json"
    pretty_path = tmp_path / "pretty.json"
    compact = _write_report(compact_path, report, indent=None)
    pretty = _write_report(pretty_path, report, indent=4)

    compact_gate = load_saved_phase10_gate(compact_path)
    pretty_gate = load_saved_phase10_gate(pretty_path)

    assert compact != pretty
    assert compact_gate.gate_report_sha256 != pretty_gate.gate_report_sha256
    assert compact_gate.semantic_gate_sha256 == pretty_gate.semantic_gate_sha256
    assert semantic_phase10_gate_v1(report).semantic_gate_sha256 == (compact_gate.semantic_gate_sha256)


def test_exact_saved_bytes_are_rechecked_before_publication(tmp_path: Path) -> None:
    report = _gate_report()
    path = tmp_path / "gate.json"
    _write_report(path, report)
    saved = load_saved_phase10_gate(path)
    verify_saved_phase10_gate_unchanged(saved)

    _write_report(path, report, indent=4)

    with pytest.raises(Phase10GateBindingError, match="exact bytes changed"):
        verify_saved_phase10_gate_unchanged(saved)
