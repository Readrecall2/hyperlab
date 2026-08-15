from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import TypeAlias, cast

JsonValue: TypeAlias = bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"] | None

PHASE10_GATE_AUDIT_VERSION = 1
PHASE10_SEMANTIC_GATE_CANONICALIZER_VERSION = "phase10_semantic_gate_payload_v1"
PHASE10_SEMANTIC_GATE_EXCLUDED_JSON_POINTERS = ("/observability",)

_PHASE_10_STATUS = "BLOCKED_PRECONDITION_NOT_MET"
_ASSETS = frozenset({"BTC", "ETH"})
_VENUES = frozenset({"binance_usdm", "hyperliquid"})

# These values are deliberately frozen in the analysis-side gate contract. A
# continuity implementation that changes them must introduce a reviewed gate
# and semantic-canonicalizer version instead of silently changing admission.
_STATE_TTL_MS = 30_000
_CLOCK_MAX_SAMPLING_INTERVAL_MS = 10_000
_CLOCK_MAX_AGE_MS = 15_000
_CLOCK_MAX_UNCERTAINTY_MS = 50.0
_MAX_CONSECUTIVE_REJECTED_CLOCK_PROBES = 1

_SAMPLE_POPULATION = "all_persisted_identity_bound_v2_clock_sync_attempts"
_SAMPLE_TIMESTAMP = "request_sent_time"
_SAMPLE_BOUNDS = "active_generation_clipped_to_requested_window"

_OBSERVABILITY_KEYS = frozenset(
    {
        "semantic",
        "files",
        "rows",
        "bounded_state",
        "elapsed_seconds_by_phase",
    }
)
_OBSERVABILITY_FILE_KEYS = frozenset(
    {
        "manifest_files_discovered",
        "manifest_files_validated",
        "manifest_files_selected",
        "manifest_files_pruned",
        "unique_parquet_files_scanned",
        "parquet_file_scan_operations",
    }
)
_OBSERVABILITY_ROW_KEYS = frozenset(
    {
        "validated_total",
        "scanned_total",
        "semantic_scanned_total",
        "staged_total",
        "scanned_by_record_type",
    }
)
_OBSERVABILITY_BOUNDED_STATE_KEYS = frozenset(
    {
        "record_batches_scanned",
        "max_record_batch_rows",
        "max_file_rows",
        "max_file_size_bytes",
        "max_python_rows_per_batch",
        "max_boundary_candidates",
        "wire_identity_keys",
        "integrity_primary_keys_spilled",
        "integrity_l2_metadata_keys_spilled",
        "integrity_cadence_rows_spilled",
        "sqlite_cache_limit_bytes",
        "sqlite_commit_interval_rows",
        "sqlite_commits",
        "max_uncommitted_rows",
        "sqlite_mmap_bytes",
        "spilled_timestamp_rows",
        "spilled_sequence_rows",
        "spilled_set_keys",
        "peak_scratch_bytes",
    }
)
_OBSERVABILITY_PHASE_KEYS = frozenset(
    {
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
    }
)
_RECORD_TYPE_VALUES = frozenset(
    {
        "instrument_metadata",
        "market_context",
        "wire_message",
        "candle",
        "bbo",
        "l2_book_state",
        "l2_snapshot",
        "l2_delta",
        "trade",
        "funding",
        "open_interest",
        "fee",
        "connection_event",
        "instrument_lifecycle",
        "clock_sync",
    }
)

_REQUIRED_TOP_LEVEL_OBJECTS = (
    "requested_window",
    "policy",
    "binance_trades",
    "connection_lineage",
    "connection_events",
    "required_wire_lineage",
    "normalized_l2_level_lineage",
    "binance_l2_resync",
    "clock_sync",
    "strict_phase_10_overlap",
    "validation",
    "observability",
)


class Phase10GateBindingError(ValueError):
    """Raised when Phase 10 gate evidence cannot be trusted or reproduced."""


@dataclass(frozen=True, slots=True)
class Phase10SemanticGate:
    payload: dict[str, JsonValue]
    canonical_bytes: bytes
    semantic_gate_sha256: str
    canonicalizer_version: str = PHASE10_SEMANTIC_GATE_CANONICALIZER_VERSION
    excluded_json_pointers: tuple[str, ...] = PHASE10_SEMANTIC_GATE_EXCLUDED_JSON_POINTERS


@dataclass(frozen=True, slots=True)
class SavedPhase10Gate:
    path: Path
    exact_bytes: bytes
    report: dict[str, JsonValue]
    gate_report_sha256: str
    semantic: Phase10SemanticGate

    @property
    def semantic_gate_sha256(self) -> str:
        return self.semantic.semantic_gate_sha256

    @property
    def canonicalizer_version(self) -> str:
        return self.semantic.canonicalizer_version

    @property
    def excluded_json_pointers(self) -> tuple[str, ...]:
        return self.semantic.excluded_json_pointers


def sha256_exact_bytes(payload: bytes) -> str:
    """Return the SHA-256 digest of bytes without JSON reserialization."""

    return hashlib.sha256(payload).hexdigest()


def read_exact_gate_report_bytes(path: Path) -> bytes:
    """Read the exact saved report bytes and reject missing or empty evidence."""

    path = Path(path)
    try:
        payload = path.read_bytes()
    except FileNotFoundError:
        raise Phase10GateBindingError(f"saved continuity gate does not exist: {path}") from None
    except OSError as exc:
        raise Phase10GateBindingError(f"saved continuity gate could not be read: {path}: {exc}") from None
    if not payload:
        raise Phase10GateBindingError("saved continuity gate is empty")
    return payload


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise Phase10GateBindingError(f"saved continuity gate contains duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise Phase10GateBindingError(f"saved continuity gate contains non-finite JSON value {value}")


def _normalize_json(value: object, *, label: str) -> JsonValue:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise Phase10GateBindingError(f"{label} contains NaN or infinity")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise Phase10GateBindingError(f"{label} contains a non-string mapping key")
            normalized[key] = _normalize_json(item, label=f"{label}.{key}")
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize_json(item, label=f"{label}[{index}]") for index, item in enumerate(value)]
    raise Phase10GateBindingError(f"{label} contains unsupported value type {type(value).__name__}")


def parse_saved_phase10_gate_json(payload: bytes) -> dict[str, JsonValue]:
    """Parse exact saved bytes with recursive duplicate-key and finite checks."""

    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise Phase10GateBindingError("saved continuity gate must be UTF-8 JSON") from None
    try:
        decoded: object = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except json.JSONDecodeError as exc:
        raise Phase10GateBindingError(f"saved continuity gate is invalid JSON: {exc.msg}") from None
    normalized = _normalize_json(decoded, label="saved continuity gate")
    if not isinstance(normalized, dict):
        raise Phase10GateBindingError("saved continuity gate must be a JSON object")
    return normalized


def canonical_json_bytes(value: object) -> bytes:
    """Serialize a finite JSON value using the Phase 10 canonical encoding."""

    return json.dumps(
        _normalize_json(value, label="canonical JSON"),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _mapping(
    value: JsonValue | None,
    *,
    label: str,
) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise Phase10GateBindingError(f"{label} must be a JSON object")
    return value


def _array(value: JsonValue | None, *, label: str) -> list[JsonValue]:
    if not isinstance(value, list):
        raise Phase10GateBindingError(f"{label} must be a JSON array")
    return value


def _strict_integer(value: JsonValue | None, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise Phase10GateBindingError(f"{label} must be a JSON integer")
    return value


def _nonnegative_integer(value: JsonValue | None, *, label: str) -> int:
    parsed = _strict_integer(value, label=label)
    if parsed < 0:
        raise Phase10GateBindingError(f"{label} must be nonnegative")
    return parsed


def _finite_number(value: JsonValue | None, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise Phase10GateBindingError(f"{label} must be a JSON number")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise Phase10GateBindingError(f"{label} must be finite")
    return parsed


def _nonnegative_number(value: JsonValue | None, *, label: str) -> float:
    parsed = _finite_number(value, label=label)
    if parsed < 0:
        raise Phase10GateBindingError(f"{label} must be nonnegative")
    return parsed


def _boolean(value: JsonValue | None, *, label: str) -> bool:
    if not isinstance(value, bool):
        raise Phase10GateBindingError(f"{label} must be a JSON boolean")
    return value


def _string(value: JsonValue | None, *, label: str) -> str:
    if not isinstance(value, str):
        raise Phase10GateBindingError(f"{label} must be a JSON string")
    return value


def _string_array(value: JsonValue | None, *, label: str) -> list[str]:
    values = _array(value, label=label)
    if not all(isinstance(item, str) for item in values):
        raise Phase10GateBindingError(f"{label} must contain only strings")
    return cast(list[str], values)


def _utc_timestamp(value: JsonValue | None, *, label: str) -> datetime:
    text = _string(value, label=label)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise Phase10GateBindingError(f"{label} must be a valid ISO-8601 timestamp") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Phase10GateBindingError(f"{label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _require_exact_keys(
    value: Mapping[str, JsonValue],
    expected: frozenset[str],
    *,
    label: str,
) -> None:
    actual = frozenset(value)
    if actual == expected:
        return
    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)
    raise Phase10GateBindingError(f"{label} has invalid schema: missing={missing!r} unknown={unknown!r}")


def _require_value(
    mapping: Mapping[str, JsonValue],
    key: str,
    expected: JsonValue,
    *,
    label: str,
) -> None:
    actual = mapping.get(key)
    if actual != expected or type(actual) is not type(expected):
        raise Phase10GateBindingError(f"{label}.{key} must equal {expected!r}")


def _validate_observability_v1(report: Mapping[str, JsonValue]) -> None:
    observability = _mapping(report.get("observability"), label="observability")
    _require_exact_keys(
        observability,
        _OBSERVABILITY_KEYS,
        label="observability",
    )
    if observability.get("semantic") is not False:
        raise Phase10GateBindingError("observability.semantic must be false")

    files = _mapping(observability.get("files"), label="observability.files")
    _require_exact_keys(files, _OBSERVABILITY_FILE_KEYS, label="observability.files")
    for key in sorted(_OBSERVABILITY_FILE_KEYS):
        _nonnegative_integer(files.get(key), label=f"observability.files.{key}")

    rows = _mapping(observability.get("rows"), label="observability.rows")
    _require_exact_keys(rows, _OBSERVABILITY_ROW_KEYS, label="observability.rows")
    for key in sorted(_OBSERVABILITY_ROW_KEYS - {"scanned_by_record_type"}):
        _nonnegative_integer(rows.get(key), label=f"observability.rows.{key}")
    scanned_by_type = _mapping(
        rows.get("scanned_by_record_type"),
        label="observability.rows.scanned_by_record_type",
    )
    unknown_record_types = sorted(set(scanned_by_type) - _RECORD_TYPE_VALUES)
    if unknown_record_types:
        raise Phase10GateBindingError(
            "observability.rows.scanned_by_record_type contains unknown record "
            f"types: {unknown_record_types!r}"
        )
    for key, value in scanned_by_type.items():
        _nonnegative_integer(
            value,
            label=f"observability.rows.scanned_by_record_type.{key}",
        )

    bounded = _mapping(
        observability.get("bounded_state"),
        label="observability.bounded_state",
    )
    _require_exact_keys(
        bounded,
        _OBSERVABILITY_BOUNDED_STATE_KEYS,
        label="observability.bounded_state",
    )
    for key in sorted(_OBSERVABILITY_BOUNDED_STATE_KEYS):
        _nonnegative_integer(
            bounded.get(key),
            label=f"observability.bounded_state.{key}",
        )

    elapsed = _mapping(
        observability.get("elapsed_seconds_by_phase"),
        label="observability.elapsed_seconds_by_phase",
    )
    _require_exact_keys(
        elapsed,
        _OBSERVABILITY_PHASE_KEYS,
        label="observability.elapsed_seconds_by_phase",
    )
    for key in sorted(_OBSERVABILITY_PHASE_KEYS):
        _nonnegative_number(
            elapsed.get(key),
            label=f"observability.elapsed_seconds_by_phase.{key}",
        )


def _validate_requested_window(
    report: Mapping[str, JsonValue],
    *,
    require_pass: bool,
) -> tuple[datetime, datetime]:
    requested = _mapping(report.get("requested_window"), label="requested_window")
    start = _utc_timestamp(requested.get("start"), label="requested_window.start")
    end = _utc_timestamp(requested.get("end"), label="requested_window.end")
    if end <= start:
        raise Phase10GateBindingError("requested_window.end must be after requested_window.start")
    duration = _finite_number(
        requested.get("duration_seconds"),
        label="requested_window.duration_seconds",
    )
    if not math.isclose(
        duration,
        (end - start).total_seconds(),
        rel_tol=0.0,
        abs_tol=1e-6,
    ):
        raise Phase10GateBindingError("requested_window.duration_seconds does not match its timestamps")
    _nonnegative_number(
        requested.get("leading_unassessed_seconds"),
        label="requested_window.leading_unassessed_seconds",
    )
    trailing_unassessed = _nonnegative_number(
        requested.get("trailing_unassessed_seconds"),
        label="requested_window.trailing_unassessed_seconds",
    )
    if (
        _strict_integer(
            requested.get("max_unassessed_margin_ms"),
            label="requested_window.max_unassessed_margin_ms",
        )
        != _CLOCK_MAX_AGE_MS
    ):
        raise Phase10GateBindingError("requested_window.max_unassessed_margin_ms must equal 15000")
    leading_ok = _boolean(
        requested.get("leading_margin_within_limit"),
        label="requested_window.leading_margin_within_limit",
    )
    trailing_ok = _boolean(
        requested.get("trailing_margin_within_limit"),
        label="requested_window.trailing_margin_within_limit",
    )
    terminal_complete = _boolean(
        requested.get("trailing_terminal_roles_complete"),
        label="requested_window.trailing_terminal_roles_complete",
    )
    if require_pass and (
        not leading_ok or not trailing_ok or (trailing_unassessed > 0 and not terminal_complete)
    ):
        raise Phase10GateBindingError(
            "passing gate requires assessed margins and terminal roles to be complete"
        )
    return start, end


def _validate_policy(report: Mapping[str, JsonValue]) -> None:
    policy = _mapping(report.get("policy"), label="policy")
    expected: dict[str, JsonValue] = {
        "interval_semantics": "half_open_received_time_causal",
        "state_ttl_ms": _STATE_TTL_MS,
        "trade_semantics": "point_event_causal_freshness_no_interpolation",
        "trade_freshness_ms": _STATE_TTL_MS,
        "binance_l2_requires_v2_resync_complete": True,
        "clock_legacy_v1_usable": False,
        "clock_max_sampling_interval_ms": _CLOCK_MAX_SAMPLING_INTERVAL_MS,
        "clock_max_age_ms": _CLOCK_MAX_AGE_MS,
        "clock_max_uncertainty_ms": _CLOCK_MAX_UNCERTAINTY_MS,
        "clock_actual_sample_spacing_enforced": True,
        "clock_sample_spacing_population": _SAMPLE_POPULATION,
        "clock_sample_spacing_timestamp": _SAMPLE_TIMESTAMP,
        "clock_sample_spacing_bounds": _SAMPLE_BOUNDS,
        "clock_identity_requires_v2_wire_lineage": True,
        "clock_offset_uncertainty_bands_must_overlap": True,
        "market_lineage_requires_exact_raw_payload": True,
        "assessed_span_starts_at_market_readiness_or_initial_clock": True,
        "initial_clock_acquisition_max_delay_ms": _CLOCK_MAX_AGE_MS,
        "interpolate_across_capture_generations": False,
        "phase_10_may_be_unblocked_by_this_audit": False,
    }
    for key, value in expected.items():
        _require_value(policy, key, value, label="policy")
    roles = _mapping(
        policy.get("physical_connection_roles_required"),
        label="policy.physical_connection_roles_required",
    )
    if roles != {
        "binance_usdm": ["market", "public"],
        "hyperliquid": ["public"],
    }:
        raise Phase10GateBindingError("policy.physical_connection_roles_required does not match v1")


def _validate_trade_and_lineage_sections(
    report: Mapping[str, JsonValue],
    *,
    require_pass: bool,
) -> None:
    trades = _mapping(report.get("binance_trades"), label="binance_trades")
    totals: dict[str, int] = {}
    for key in (
        "normalized_total",
        "normalized_with_raw_lineage_total",
        "raw_agg_trade_total",
        "raw_agg_trade_with_role_lineage_total",
    ):
        totals[key] = _nonnegative_integer(trades.get(key), label=f"binance_trades.{key}")
    by_asset = _mapping(trades.get("by_asset"), label="binance_trades.by_asset")
    if frozenset(by_asset) != _ASSETS:
        raise Phase10GateBindingError("binance_trades.by_asset must cover exactly BTC and ETH")
    fields = (
        "normalized_count",
        "normalized_with_raw_lineage_count",
        "raw_agg_trade_count",
        "raw_agg_trade_with_role_lineage_count",
    )
    sums = dict.fromkeys(fields, 0)
    for asset in sorted(_ASSETS):
        counts = _mapping(by_asset.get(asset), label=f"binance_trades.by_asset.{asset}")
        for field in fields:
            value = _nonnegative_integer(
                counts.get(field),
                label=f"binance_trades.by_asset.{asset}.{field}",
            )
            sums[field] += value
            if require_pass and value == 0:
                raise Phase10GateBindingError(f"passing gate requires positive {asset} {field}")
    expected_totals = {
        "normalized_total": sums["normalized_count"],
        "normalized_with_raw_lineage_total": sums["normalized_with_raw_lineage_count"],
        "raw_agg_trade_total": sums["raw_agg_trade_count"],
        "raw_agg_trade_with_role_lineage_total": sums["raw_agg_trade_with_role_lineage_count"],
    }
    if totals != expected_totals:
        raise Phase10GateBindingError("binance_trades totals do not match by_asset counts")

    required_wire = _mapping(report.get("required_wire_lineage"), label="required_wire_lineage")
    orphan_wire = _nonnegative_integer(
        required_wire.get("orphan_required_wire_total"),
        label="required_wire_lineage.orphan_required_wire_total",
    )
    wire_by_venue_total = _validate_venue_asset_counts(
        required_wire.get("by_venue_asset"),
        label="required_wire_lineage.by_venue_asset",
    )
    if orphan_wire != wire_by_venue_total:
        raise Phase10GateBindingError("required_wire_lineage total does not match by_venue_asset")

    normalized_l2 = _mapping(
        report.get("normalized_l2_level_lineage"),
        label="normalized_l2_level_lineage",
    )
    orphan_l2 = _nonnegative_integer(
        normalized_l2.get("orphan_level_total"),
        label="normalized_l2_level_lineage.orphan_level_total",
    )
    l2_by_venue_total = _validate_venue_asset_counts(
        normalized_l2.get("by_venue_asset"),
        label="normalized_l2_level_lineage.by_venue_asset",
    )
    if orphan_l2 != l2_by_venue_total:
        raise Phase10GateBindingError("normalized_l2_level_lineage total does not match by_venue_asset")

    resync = _mapping(report.get("binance_l2_resync"), label="binance_l2_resync")
    missing_count = _nonnegative_integer(resync.get("missing_count"), label="binance_l2_resync.missing_count")
    missing = _array(resync.get("missing"), label="binance_l2_resync.missing")
    if missing_count != len(missing):
        raise Phase10GateBindingError("binance_l2_resync.missing_count does not match missing")
    for index, item in enumerate(missing):
        entry = _mapping(item, label=f"binance_l2_resync.missing[{index}]")
        _string(
            entry.get("capture_epoch_id"),
            label=f"binance_l2_resync.missing[{index}].capture_epoch_id",
        )
        asset = _string(entry.get("asset"), label=f"binance_l2_resync.missing[{index}].asset")
        if asset not in _ASSETS:
            raise Phase10GateBindingError(f"binance_l2_resync.missing[{index}].asset is unsupported")
    if require_pass and (orphan_wire or orphan_l2 or missing_count):
        raise Phase10GateBindingError("passing gate requires zero lineage or L2 resync omissions")


def _validate_venue_asset_counts(
    value: JsonValue | None,
    *,
    label: str,
) -> int:
    venues = _mapping(value, label=label)
    if frozenset(venues) != _VENUES:
        raise Phase10GateBindingError(f"{label} must cover exactly binance_usdm and hyperliquid")
    total = 0
    for venue in sorted(_VENUES):
        assets = _mapping(venues.get(venue), label=f"{label}.{venue}")
        if frozenset(assets) != _ASSETS:
            raise Phase10GateBindingError(f"{label}.{venue} must cover exactly BTC and ETH")
        for asset in sorted(_ASSETS):
            total += _nonnegative_integer(assets.get(asset), label=f"{label}.{venue}.{asset}")
    return total


def _validate_clock_sync(
    report: Mapping[str, JsonValue],
    *,
    require_pass: bool,
) -> None:
    clock = _mapping(report.get("clock_sync"), label="clock_sync")
    integer_fields = (
        "legacy_v1_ignored",
        "valid_v2_samples",
        "invalid_v2_samples",
        "rejected_probe_samples",
        "hard_invalid_v2_samples",
        "failure_events",
        "strict_policy_rejections",
        "wire_identity_rejections",
        "unbound_invalid_events",
        "in_window_invalid_events",
        "in_window_rejected_probe_events",
        "in_window_hard_invalid_events",
        "in_window_failure_events",
        "consecutive_rejection_violations",
        "sample_spacing_violations",
        "offset_discontinuities",
        "max_consecutive_rejected_probes",
        "internal_gap_count",
        "generation_gap_count",
    )
    integer_values = {
        key: _nonnegative_integer(clock.get(key), label=f"clock_sync.{key}") for key in integer_fields
    }
    exact_values: dict[str, JsonValue] = {
        "strict_max_consecutive_rejected_probes": (_MAX_CONSECUTIVE_REJECTED_CLOCK_PROBES),
        "sample_spacing_population": _SAMPLE_POPULATION,
        "sample_spacing_timestamp": _SAMPLE_TIMESTAMP,
        "sample_spacing_bounds": _SAMPLE_BOUNDS,
        "strict_max_sampling_interval_ms": _CLOCK_MAX_SAMPLING_INTERVAL_MS,
        "strict_max_age_ms": _CLOCK_MAX_AGE_MS,
        "strict_max_uncertainty_ms": _CLOCK_MAX_UNCERTAINTY_MS,
    }
    for key, value in exact_values.items():
        _require_value(clock, key, value, label="clock_sync")
    if (
        require_pass
        and _strict_integer(
            clock.get("max_consecutive_rejected_probes"),
            label="clock_sync.max_consecutive_rejected_probes",
        )
        > _MAX_CONSECUTIVE_REJECTED_CLOCK_PROBES
    ):
        raise Phase10GateBindingError("passing gate exceeds strict consecutive rejected probe policy")
    capture_arrays = (
        "consecutive_rejection_violation_capture_generations",
        "sample_spacing_violation_capture_generations",
        "offset_discontinuity_capture_generations",
        "eligible_capture_generations",
        "market_active_capture_generations",
        "market_active_without_valid_clock",
        "assessed_capture_generations",
        "initial_acquisition_delay_violations",
    )
    capture_values = {key: _string_array(clock.get(key), label=f"clock_sync.{key}") for key in capture_arrays}
    _validate_interval_payload_array(
        clock.get("consecutive_rejection_outages"),
        label="clock_sync.consecutive_rejection_outages",
    )
    _validate_interval_payload_array(clock.get("intervals"), label="clock_sync.intervals")
    for mapping_name in (
        "market_ready_at_by_capture",
        "initial_acquisition_delay_ms_by_capture",
    ):
        mapping = _mapping(clock.get(mapping_name), label=f"clock_sync.{mapping_name}")
        for capture, value in mapping.items():
            if not capture:
                raise Phase10GateBindingError(f"clock_sync.{mapping_name} contains an empty capture key")
            if mapping_name == "market_ready_at_by_capture":
                _utc_timestamp(value, label=f"clock_sync.{mapping_name}.{capture}")
            else:
                _nonnegative_number(value, label=f"clock_sync.{mapping_name}.{capture}")
    assessed_span = clock.get("assessed_span")
    if assessed_span is not None:
        span = _mapping(assessed_span, label="clock_sync.assessed_span")
        span_start = _utc_timestamp(span.get("start"), label="clock_sync.assessed_span.start")
        span_end = _utc_timestamp(span.get("end"), label="clock_sync.assessed_span.end")
        if span_end <= span_start:
            raise Phase10GateBindingError("clock_sync.assessed_span must have positive duration")
    for key in ("actual_max_sample_gap_ms", "actual_max_cadence_gap_ms"):
        value = clock.get(key)
        if value is not None:
            _nonnegative_number(value, label=f"clock_sync.{key}")
    for key in (
        "valid_duration_seconds",
        "uncovered_seconds",
        "requested_window_leading_gap_seconds",
        "requested_window_trailing_gap_seconds",
    ):
        _nonnegative_number(clock.get(key), label=f"clock_sync.{key}")
    coverage = _boolean(clock.get("coverage_continuous"), label="clock_sync.coverage_continuous")
    causal = _boolean(
        clock.get("causal_coverage_continuous"),
        label="clock_sync.causal_coverage_continuous",
    )
    without_clock = _array(
        clock.get("market_active_without_valid_clock"),
        label="clock_sync.market_active_without_valid_clock",
    )
    uncovered = _finite_number(clock.get("uncovered_seconds"), label="clock_sync.uncovered_seconds")
    if require_pass and (not coverage or not causal or without_clock or uncovered != 0.0):
        raise Phase10GateBindingError("passing gate requires continuous causal clock coverage")
    if require_pass:
        fatal_counts = (
            "hard_invalid_v2_samples",
            "failure_events",
            "wire_identity_rejections",
            "unbound_invalid_events",
            "in_window_hard_invalid_events",
            "in_window_failure_events",
            "consecutive_rejection_violations",
            "sample_spacing_violations",
            "offset_discontinuities",
            "internal_gap_count",
            "generation_gap_count",
        )
        if integer_values["valid_v2_samples"] == 0 or any(integer_values[key] for key in fatal_counts):
            raise Phase10GateBindingError("passing gate contains fatal clock evidence")
        fatal_capture_arrays = (
            "consecutive_rejection_violation_capture_generations",
            "sample_spacing_violation_capture_generations",
            "offset_discontinuity_capture_generations",
            "market_active_without_valid_clock",
            "initial_acquisition_delay_violations",
        )
        if any(capture_values[key] for key in fatal_capture_arrays):
            raise Phase10GateBindingError("passing gate contains fatal clock capture generations")
        if not capture_values["eligible_capture_generations"]:
            raise Phase10GateBindingError("passing gate requires eligible clock capture generations")


def _validate_interval_payload_array(
    value: JsonValue | None,
    *,
    label: str,
) -> None:
    raw_intervals = _array(value, label=label)
    for index, raw in enumerate(raw_intervals):
        item = _mapping(raw, label=f"{label}[{index}]")
        tag = _string(
            item.get("capture_epoch_id"),
            label=f"{label}[{index}].capture_epoch_id",
        )
        if not tag:
            raise Phase10GateBindingError(f"{label}[{index}].capture_epoch_id must be non-empty")
        start = _utc_timestamp(item.get("start"), label=f"{label}[{index}].start")
        end = _utc_timestamp(item.get("end"), label=f"{label}[{index}].end")
        if end <= start:
            raise Phase10GateBindingError(f"{label}[{index}] must have positive duration")
        duration = _nonnegative_number(
            item.get("duration_seconds"),
            label=f"{label}[{index}].duration_seconds",
        )
        if not math.isclose(
            duration,
            (end - start).total_seconds(),
            rel_tol=0.0,
            abs_tol=1e-6,
        ):
            raise Phase10GateBindingError(f"{label}[{index}].duration_seconds does not match timestamps")


def _validate_strict_intervals(
    report: Mapping[str, JsonValue],
    *,
    window_start: datetime,
    window_end: datetime,
    require_pass: bool,
) -> None:
    overlap = _mapping(
        report.get("strict_phase_10_overlap"),
        label="strict_phase_10_overlap",
    )
    intervals_raw = _array(overlap.get("intervals"), label="strict_phase_10_overlap.intervals")
    intervals: list[tuple[datetime, datetime, str]] = []
    for index, raw in enumerate(intervals_raw):
        item = _mapping(raw, label=f"strict_phase_10_overlap.intervals[{index}]")
        _require_exact_keys(
            item,
            frozenset({"capture_epoch_id", "start", "end", "duration_seconds"}),
            label=f"strict_phase_10_overlap.intervals[{index}]",
        )
        tag = _string(
            item.get("capture_epoch_id"),
            label=f"strict_phase_10_overlap.intervals[{index}].capture_epoch_id",
        )
        if not tag:
            raise Phase10GateBindingError("strict interval capture_epoch_id is empty")
        start = _utc_timestamp(
            item.get("start"),
            label=f"strict_phase_10_overlap.intervals[{index}].start",
        )
        end = _utc_timestamp(
            item.get("end"),
            label=f"strict_phase_10_overlap.intervals[{index}].end",
        )
        if end <= start or start < window_start or end > window_end:
            raise Phase10GateBindingError(
                "strict intervals must be positive and contained by requested_window"
            )
        duration = _finite_number(
            item.get("duration_seconds"),
            label=f"strict_phase_10_overlap.intervals[{index}].duration_seconds",
        )
        if not math.isclose(
            duration,
            (end - start).total_seconds(),
            rel_tol=0.0,
            abs_tol=1e-6,
        ):
            raise Phase10GateBindingError("strict interval duration does not match its timestamps")
        intervals.append((start, end, tag))
    ordered = sorted(intervals)
    if intervals != ordered:
        raise Phase10GateBindingError("strict intervals are not in canonical order")
    for previous, current in pairwise(ordered):
        if current[0] < previous[1]:
            raise Phase10GateBindingError("strict intervals must not overlap")
    reported_count = _nonnegative_integer(
        overlap.get("interval_count"),
        label="strict_phase_10_overlap.interval_count",
    )
    if reported_count != len(intervals):
        raise Phase10GateBindingError("strict overlap interval_count does not match intervals")
    reported_duration = _nonnegative_number(
        overlap.get("duration_seconds"),
        label="strict_phase_10_overlap.duration_seconds",
    )
    total_duration = sum((end - start).total_seconds() for start, end, _ in intervals)
    if not math.isclose(
        reported_duration,
        total_duration,
        rel_tol=0.0,
        abs_tol=1e-6,
    ):
        raise Phase10GateBindingError("strict overlap duration does not match intervals")
    by_asset = _mapping(overlap.get("by_asset"), label="strict_phase_10_overlap.by_asset")
    if frozenset(by_asset) != _ASSETS:
        raise Phase10GateBindingError("strict_phase_10_overlap.by_asset must cover exactly BTC and ETH")
    for asset in sorted(_ASSETS):
        item = _mapping(by_asset.get(asset), label=f"strict_phase_10_overlap.by_asset.{asset}")
        count = _nonnegative_integer(
            item.get("interval_count"),
            label=f"strict_phase_10_overlap.by_asset.{asset}.interval_count",
        )
        duration = _nonnegative_number(
            item.get("duration_seconds"),
            label=f"strict_phase_10_overlap.by_asset.{asset}.duration_seconds",
        )
        if count != reported_count or not math.isclose(
            duration,
            reported_duration,
            rel_tol=0.0,
            abs_tol=1e-6,
        ):
            raise Phase10GateBindingError(
                "strict_phase_10_overlap.by_asset must match the common strict intervals"
            )
        if require_pass and (count == 0 or duration <= 0):
            raise Phase10GateBindingError(f"passing gate requires positive strict overlap for {asset}")
    if require_pass and (not intervals or reported_duration <= 0):
        raise Phase10GateBindingError("passing gate requires positive strict overlap intervals")


def _validate_remaining_schema(
    report: Mapping[str, JsonValue],
    *,
    require_pass: bool,
) -> None:
    lineages = _mapping(report.get("connection_lineage"), label="connection_lineage")
    if frozenset(lineages) != _VENUES:
        raise Phase10GateBindingError("connection_lineage must cover exactly binance_usdm and hyperliquid")
    for venue in sorted(_VENUES):
        lineage = _mapping(lineages.get(venue), label=f"connection_lineage.{venue}")
        eligible = _string_array(
            lineage.get("eligible_capture_generations"),
            label=f"connection_lineage.{venue}.eligible_capture_generations",
        )
        invalid = _string_array(
            lineage.get("market_active_invalid_capture_generations"),
            label=(f"connection_lineage.{venue}.market_active_invalid_capture_generations"),
        )
        incomplete = _string_array(
            lineage.get("incomplete_capture_generations"),
            label=f"connection_lineage.{venue}.incomplete_capture_generations",
        )
        ambiguous = _nonnegative_integer(
            lineage.get("ambiguous_or_wrong_role_connect_identities"),
            label=(f"connection_lineage.{venue}.ambiguous_or_wrong_role_connect_identities"),
        )
        unbound = _nonnegative_integer(
            lineage.get("unbound_connect_events"),
            label=f"connection_lineage.{venue}.unbound_connect_events",
        )
        rejections = _nonnegative_integer(
            lineage.get("normalized_market_lineage_rejections"),
            label=(f"connection_lineage.{venue}.normalized_market_lineage_rejections"),
        )
        multiple = False
        if venue == "hyperliquid":
            multiple = _boolean(
                lineage.get("multiple_active_capture_generations"),
                label=("connection_lineage.hyperliquid.multiple_active_capture_generations"),
            )
        if require_pass and (
            not eligible
            or invalid
            or incomplete
            or ambiguous
            or unbound
            or rejections
            or multiple
        ):
            raise Phase10GateBindingError(f"passing gate contains invalid {venue} connection lineage")

    events = _mapping(report.get("connection_events"), label="connection_events")
    if frozenset(events) != _VENUES:
        raise Phase10GateBindingError("connection_events must cover exactly binance_usdm and hyperliquid")
    for venue in sorted(_VENUES):
        event_report = _mapping(events.get(venue), label=f"connection_events.{venue}")
        counter_names = (
            "unbound_gap_or_disconnect_events",
            "unbound_resync_events",
            "in_window_gap_events",
            "unclean_in_window_disconnect_events",
        )
        counters = {
            key: _nonnegative_integer(event_report.get(key), label=f"connection_events.{venue}.{key}")
            for key in counter_names
        }
        active_generations = _string_array(
            event_report.get("event_active_capture_generations"),
            label=f"connection_events.{venue}.event_active_capture_generations",
        )
        failures_by_capture = _mapping(
            event_report.get("failure_events_by_capture_generation"),
            label=(f"connection_events.{venue}.failure_events_by_capture_generation"),
        )
        for capture, raw_failures in failures_by_capture.items():
            if not capture:
                raise Phase10GateBindingError(f"connection_events.{venue} contains an empty capture key")
            failures = _array(
                raw_failures,
                label=(f"connection_events.{venue}.failure_events_by_capture_generation.{capture}"),
            )
            for index, failure in enumerate(failures):
                _mapping(
                    failure,
                    label=(
                        f"connection_events.{venue}.failure_events_by_capture_generation.{capture}[{index}]"
                    ),
                )
        failure_count = 0
        for raw_failures in failures_by_capture.values():
            failures = _array(raw_failures, label="connection event failures")
            failure_count += len(failures)
        if require_pass and (
            not active_generations or any(counters.values()) or failure_count
        ):
            raise Phase10GateBindingError(f"passing gate contains fatal {venue} connection events")

    validation = _mapping(report.get("validation"), label="validation")
    partition_count = _nonnegative_integer(
        validation.get("inventory_partition_count"),
        label="validation.inventory_partition_count",
    )
    row_count = _nonnegative_integer(
        validation.get("inventory_row_count"),
        label="validation.inventory_row_count",
    )
    gap_count = _nonnegative_integer(
        validation.get("relevant_gap_count"),
        label="validation.relevant_gap_count",
    )
    gaps = _array(validation.get("relevant_gaps"), label="validation.relevant_gaps")
    for index, gap in enumerate(gaps):
        _mapping(gap, label=f"validation.relevant_gaps[{index}]")
    if gap_count != len(gaps):
        raise Phase10GateBindingError("validation.relevant_gap_count does not match relevant_gaps")
    if require_pass and (partition_count == 0 or row_count == 0 or gap_count):
        raise Phase10GateBindingError("passing gate requires non-empty inventory and zero relevant gaps")


def validate_phase10_gate_report_v1(
    report: Mapping[str, object],
    *,
    require_pass: bool = True,
) -> dict[str, JsonValue]:
    """Validate and defensively copy a Phase 10 continuity report v1.

    Unknown fields outside ``/observability`` are intentionally retained. They
    remain semantic evidence and therefore participate in canonical equality.
    ``observability`` is the sole exact-schema, nonsemantic top-level member.
    """

    normalized_value = _normalize_json(report, label="continuity gate")
    if not isinstance(normalized_value, dict):
        raise Phase10GateBindingError("continuity gate must be a JSON object")
    normalized = normalized_value

    audit_version = _strict_integer(normalized.get("audit_version"), label="audit_version")
    if audit_version != PHASE10_GATE_AUDIT_VERSION:
        raise Phase10GateBindingError(f"unsupported Phase 10 gate audit_version: {audit_version}")
    if normalized.get("phase_10_status") != _PHASE_10_STATUS:
        raise Phase10GateBindingError(f"phase_10_status must equal {_PHASE_10_STATUS}")
    gate = _string(
        normalized.get("technical_capture_gate"),
        label="technical_capture_gate",
    )
    if gate not in {"PASS", "FAIL"}:
        raise Phase10GateBindingError("technical_capture_gate must be PASS or FAIL")
    failures = _string_array(normalized.get("failure_reasons"), label="failure_reasons")
    if failures != sorted(set(failures)):
        raise Phase10GateBindingError("failure_reasons must be sorted and contain no duplicates")
    if gate == "PASS" and failures:
        raise Phase10GateBindingError("technical_capture_gate=PASS requires failure_reasons=[]")
    if gate == "FAIL" and not failures:
        raise Phase10GateBindingError("technical_capture_gate=FAIL requires failure reasons")
    if require_pass and gate != "PASS":
        raise Phase10GateBindingError("saved continuity gate must have technical_capture_gate=PASS")

    assets = _string_array(normalized.get("assets"), label="assets")
    normalized_assets = [asset.strip().upper() for asset in assets]
    if len(normalized_assets) != len(set(normalized_assets)) or frozenset(normalized_assets) != _ASSETS:
        raise Phase10GateBindingError("continuity gate must cover exactly BTC and ETH")

    for key in _REQUIRED_TOP_LEVEL_OBJECTS:
        _mapping(normalized.get(key), label=key)

    window_start, window_end = _validate_requested_window(
        normalized,
        require_pass=require_pass,
    )
    _validate_policy(normalized)
    _validate_trade_and_lineage_sections(normalized, require_pass=require_pass)
    _validate_clock_sync(normalized, require_pass=require_pass)
    _validate_strict_intervals(
        normalized,
        window_start=window_start,
        window_end=window_end,
        require_pass=require_pass,
    )
    _validate_remaining_schema(normalized, require_pass=require_pass)
    _validate_observability_v1(normalized)
    return normalized


def phase10_semantic_gate_payload_v1(
    report: Mapping[str, object],
    *,
    require_pass: bool = True,
) -> dict[str, JsonValue]:
    """Return validated semantic evidence excluding only ``/observability``."""

    normalized = validate_phase10_gate_report_v1(
        report,
        require_pass=require_pass,
    )
    semantic = dict(normalized)
    del semantic["observability"]
    return semantic


def semantic_phase10_gate_v1(
    report: Mapping[str, object],
    *,
    require_pass: bool = True,
) -> Phase10SemanticGate:
    payload = phase10_semantic_gate_payload_v1(
        report,
        require_pass=require_pass,
    )
    canonical = canonical_json_bytes(payload)
    return Phase10SemanticGate(
        payload=payload,
        canonical_bytes=canonical,
        semantic_gate_sha256=sha256_exact_bytes(canonical),
    )


def load_saved_phase10_gate(path: Path) -> SavedPhase10Gate:
    """Load, validate, and bind an independently saved passing gate report."""

    resolved = Path(path)
    exact_bytes = read_exact_gate_report_bytes(resolved)
    report = parse_saved_phase10_gate_json(exact_bytes)
    semantic = semantic_phase10_gate_v1(report)
    return SavedPhase10Gate(
        path=resolved,
        exact_bytes=exact_bytes,
        report=report,
        gate_report_sha256=sha256_exact_bytes(exact_bytes),
        semantic=semantic,
    )


def compare_saved_and_fresh_phase10_gate(
    saved: SavedPhase10Gate,
    fresh_report: Mapping[str, object],
) -> Phase10SemanticGate:
    """Validate a fresh re-audit and require exact canonical semantic equality."""

    if (
        saved.canonicalizer_version != PHASE10_SEMANTIC_GATE_CANONICALIZER_VERSION
        or saved.excluded_json_pointers != PHASE10_SEMANTIC_GATE_EXCLUDED_JSON_POINTERS
    ):
        raise Phase10GateBindingError("saved continuity gate uses an unsupported semantic contract")
    fresh = semantic_phase10_gate_v1(fresh_report)
    if fresh.canonical_bytes != saved.semantic.canonical_bytes:
        raise Phase10GateBindingError(
            "saved continuity gate does not equal the versioned semantic fresh re-audit"
        )
    return fresh


def verify_saved_phase10_gate_unchanged(saved: SavedPhase10Gate) -> None:
    """Fail if saved evidence bytes changed since their initial binding."""

    current = read_exact_gate_report_bytes(saved.path)
    if current != saved.exact_bytes:
        raise Phase10GateBindingError("saved continuity gate exact bytes changed after validation")


__all__ = [
    "PHASE10_GATE_AUDIT_VERSION",
    "PHASE10_SEMANTIC_GATE_CANONICALIZER_VERSION",
    "PHASE10_SEMANTIC_GATE_EXCLUDED_JSON_POINTERS",
    "Phase10GateBindingError",
    "Phase10SemanticGate",
    "SavedPhase10Gate",
    "canonical_json_bytes",
    "compare_saved_and_fresh_phase10_gate",
    "load_saved_phase10_gate",
    "parse_saved_phase10_gate_json",
    "phase10_semantic_gate_payload_v1",
    "read_exact_gate_report_bytes",
    "semantic_phase10_gate_v1",
    "sha256_exact_bytes",
    "validate_phase10_gate_report_v1",
    "verify_saved_phase10_gate_unchanged",
]
