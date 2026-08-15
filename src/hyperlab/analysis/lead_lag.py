from __future__ import annotations

import hashlib
import json
import math
import re
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Literal, cast

import numpy as np
import pandas as pd

REFERENCE_VENUE = "binance_usdm"
EXECUTION_VENUE = "hyperliquid"
SIGNAL_FAMILIES = (
    "agg_trade",
    "trade_imbalance",
    "bbo_change",
    "l2_imbalance",
    "mid_price_change",
    "microprice_change",
    "short_term_momentum",
    "signed_flow",
)
SOURCE_TIME_STATUS = "NOT_ADMISSIBLE_NO_SYMMETRIC_HL_CLOCK_CALIBRATION"
STREAMING_RESOURCE_MODEL_VERSION = "BOUNDED_STREAMING_V1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_INFORMATION_EVENT_ROW_BASE_BYTES = 4_096
_EXECUTION_EVENT_ROW_BASE_BYTES = 8_192
_EVENT_MATERIALIZATION_PEAK_MULTIPLIER = 2
_STUDY_CONFIG_FIELDS = frozenset(
    {
        "assets",
        "horizons_ms",
        "trade_window_ms",
        "momentum_window_ms",
        "l2_levels",
        "max_book_age_ms",
        "minimum_move_bps",
        "bucket_minutes",
        "randomization_resamples",
        "randomization_block_ms",
        "randomization_seed",
        "minimum_events",
        "max_event_rows",
        "max_estimated_event_bytes",
        "streaming_resource_model_version",
        "max_source_rows_per_chunk",
        "max_simultaneous_batch_rows",
        "max_l2_frame_levels",
        "max_l2_levels_per_chunk",
        "max_pending_response_states",
        "max_pending_execution_states",
        "external_merge_fan_in",
        "quantile_sort_run_rows",
        "parquet_row_group_rows",
        "writer_buffer_rows",
        "scratch_low_watermark_bytes",
        "scratch_reserve_bytes",
        "reference_venue",
        "execution_venue",
    }
)
_STUDY_CONFIG_REQUIRED_FIELDS = _STUDY_CONFIG_FIELDS - {
    "reference_venue",
    "execution_venue",
}
_EXECUTION_CONFIG_REQUIRED_FIELDS = frozenset(
    {
        "name",
        "latency_ms",
        "exit_latency_ms",
        "notional_usd",
        "maker_fee_bps",
        "taker_fee_bps",
        "slippage_bps",
        "adverse_exit_bps",
        "queue_ahead_multiplier",
        "maker_timeout_ms",
        "max_participation",
        "calibration_status",
        "source",
    }
)
_EXECUTION_CONFIG_FIELDS = _EXECUTION_CONFIG_REQUIRED_FIELDS | {
    "calibration_evidence_hash"
}


def _utc(value: datetime | pd.Timestamp, *, label: str) -> datetime:
    timestamp = value.to_pydatetime() if isinstance(value, pd.Timestamp) else value
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return timestamp.astimezone(UTC)


def _timestamp_value(value: object, *, label: str) -> pd.Timestamp:
    if isinstance(value, pd.Timestamp):
        return value
    if isinstance(value, (datetime, str, int, float, np.datetime64)):
        return pd.Timestamp(value)
    raise TypeError(f"{label} must be timestamp-like")


def _integer_value(value: object, *, label: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{label} must be integer-like")
    if isinstance(value, (int, np.integer, str)):
        return int(value)
    raise TypeError(f"{label} must be integer-like")


def _float_value(value: object, *, label: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{label} must be numeric")
    if isinstance(value, (int, float, np.integer, np.floating, str)):
        return float(value)
    raise TypeError(f"{label} must be numeric")


def _nanoseconds(values: pd.Series) -> np.ndarray:
    return values.astype("int64").to_numpy(dtype=np.int64)


@dataclass(frozen=True, slots=True, order=True)
class StrictInterval:
    start: datetime
    end: datetime
    tag: str

    def __post_init__(self) -> None:
        start = _utc(self.start, label="interval start")
        end = _utc(self.end, label="interval end")
        if end <= start:
            raise ValueError("strict interval end must be after start")
        if not self.tag.strip():
            raise ValueError("strict interval tag cannot be empty")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)

    def contains_window(self, start: datetime, end: datetime) -> bool:
        first = _utc(start, label="window start")
        last = _utc(end, label="window end")
        return self.start <= first <= last < self.end


@dataclass(frozen=True, slots=True)
class LeadLagDataset:
    bbo: pd.DataFrame
    trades: pd.DataFrame
    l2: pd.DataFrame
    clock_sync: pd.DataFrame
    provenance: Mapping[str, object]
    source_fingerprint: str

    def __post_init__(self) -> None:
        for name in ("bbo", "trades", "l2", "clock_sync"):
            if not isinstance(getattr(self, name), pd.DataFrame):
                raise TypeError(f"{name} must be a pandas DataFrame")
        if not self.source_fingerprint.strip():
            raise ValueError("source_fingerprint cannot be empty")


@dataclass(frozen=True, slots=True)
class ExecutionAssumptions:
    name: str = "baseline"
    latency_ms: int = 100
    exit_latency_ms: int = 100
    notional_usd: float = 1_000.0
    maker_fee_bps: float = 0.0
    taker_fee_bps: float = 0.0
    slippage_bps: float = 0.0
    adverse_exit_bps: float = 0.0
    queue_ahead_multiplier: float = 1.0
    maker_timeout_ms: int = 1_000
    max_participation: float = 1.0
    calibration_status: str = "UNCALIBRATED"
    calibration_evidence_hash: str | None = None
    source: str = "explicit-research-assumption"

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("execution scenario name cannot be empty")
        for name in ("latency_ms", "exit_latency_ms", "maker_timeout_ms"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        for name in (
            "notional_usd",
            "slippage_bps",
            "adverse_exit_bps",
            "queue_ahead_multiplier",
            "max_participation",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(f"{name} must be finite and non-negative")
        if self.notional_usd <= 0.0:
            raise ValueError("notional_usd must be positive")
        if self.slippage_bps + self.adverse_exit_bps >= 10_000.0:
            raise ValueError(
                "combined slippage_bps and adverse_exit_bps must be below 10000 bps"
            )
        if not 0.0 < self.max_participation <= 1.0:
            raise ValueError("max_participation must be in (0, 1]")
        for name in ("maker_fee_bps", "taker_fee_bps"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or abs(value) >= 10_000.0:
                raise ValueError(f"{name} must be finite")
        status = self.calibration_status.upper()
        if status not in {"CALIBRATED", "UNCALIBRATED", "SYNTHETIC"}:
            raise ValueError("unknown execution calibration_status")
        if not self.source.strip():
            raise ValueError("execution source cannot be empty")
        evidence = self.calibration_evidence_hash
        if evidence is not None and _SHA256_RE.fullmatch(evidence) is None:
            raise ValueError("calibration_evidence_hash must be lowercase SHA-256 hex")
        if status == "CALIBRATED" and evidence is None:
            raise ValueError("CALIBRATED execution assumptions require an evidence hash")
        if status == "CALIBRATED" and any(
            marker in self.source.casefold()
            for marker in ("placeholder", "default", "synthetic", "uncalibrated")
        ):
            raise ValueError("CALIBRATED execution assumptions require a non-placeholder source")
        object.__setattr__(self, "calibration_status", status)


def _default_execution_scenarios() -> tuple[ExecutionAssumptions, ...]:
    return (
        ExecutionAssumptions(name="baseline"),
        ExecutionAssumptions(
            name="adverse",
            latency_ms=250,
            exit_latency_ms=250,
            slippage_bps=1.0,
            adverse_exit_bps=1.0,
        ),
    )


@dataclass(frozen=True, slots=True)
class LeadLagConfig:
    assets: tuple[str, ...] = ("BTC", "ETH")
    horizons_ms: tuple[int, ...] = (50, 100, 250, 500, 1_000, 2_000, 5_000)
    trade_window_ms: int = 1_000
    momentum_window_ms: int = 1_000
    l2_levels: int = 5
    max_book_age_ms: int = 1_000
    minimum_move_bps: float = 0.0
    bucket_minutes: int = 60
    randomization_resamples: int = 199
    randomization_block_ms: int = 60_000
    randomization_seed: int = 42
    minimum_events: int = 30
    max_event_rows: int = 5_000_000
    max_estimated_event_bytes: int = 8_000_000_000
    streaming_resource_model_version: str = STREAMING_RESOURCE_MODEL_VERSION
    max_source_rows_per_chunk: int = 100_000
    max_simultaneous_batch_rows: int = 25_000
    max_l2_frame_levels: int = 10_000
    max_l2_levels_per_chunk: int = 100_000
    max_pending_response_states: int = 500_000
    max_pending_execution_states: int = 1_000_000
    external_merge_fan_in: int = 32
    quantile_sort_run_rows: int = 250_000
    parquet_row_group_rows: int = 65_536
    writer_buffer_rows: int = 16_384
    scratch_low_watermark_bytes: int = 4_000_000_000
    scratch_reserve_bytes: int = 2_000_000_000
    execution_scenarios: tuple[ExecutionAssumptions, ...] = field(
        default_factory=_default_execution_scenarios
    )
    reference_venue: str = REFERENCE_VENUE
    execution_venue: str = EXECUTION_VENUE

    def __post_init__(self) -> None:
        assets = tuple(dict.fromkeys(asset.strip().upper() for asset in self.assets if asset.strip()))
        if not assets or len(assets) != len(self.assets):
            raise ValueError("assets must be non-empty and unique")
        if not {"BTC", "ETH"}.issubset(assets):
            raise ValueError("Phase 10-2 assets must include BTC and ETH")
        if any(isinstance(value, bool) or not isinstance(value, int) for value in self.horizons_ms):
            raise ValueError("horizons_ms must contain integers")
        horizons = tuple(self.horizons_ms)
        if not horizons or any(value <= 0 for value in horizons) or len(set(horizons)) != len(horizons):
            raise ValueError("horizons_ms must be positive and unique")
        if tuple(sorted(horizons)) != horizons:
            raise ValueError("horizons_ms must be sorted")
        for name in (
            "trade_window_ms",
            "momentum_window_ms",
            "l2_levels",
            "max_book_age_ms",
            "bucket_minutes",
            "randomization_resamples",
            "randomization_block_ms",
            "minimum_events",
            "max_event_rows",
            "max_estimated_event_bytes",
            "max_source_rows_per_chunk",
            "max_simultaneous_batch_rows",
            "max_l2_frame_levels",
            "max_l2_levels_per_chunk",
            "max_pending_response_states",
            "max_pending_execution_states",
            "external_merge_fan_in",
            "quantile_sort_run_rows",
            "parquet_row_group_rows",
            "writer_buffer_rows",
            "scratch_low_watermark_bytes",
            "scratch_reserve_bytes",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.randomization_resamples < 19:
            raise ValueError("randomization_resamples must be at least 19")
        if self.streaming_resource_model_version != STREAMING_RESOURCE_MODEL_VERSION:
            raise ValueError(
                "streaming_resource_model_version must remain "
                f"{STREAMING_RESOURCE_MODEL_VERSION}"
            )
        if self.external_merge_fan_in < 2:
            raise ValueError("external_merge_fan_in must be at least 2")
        if self.writer_buffer_rows > self.parquet_row_group_rows:
            raise ValueError(
                "writer_buffer_rows must not exceed parquet_row_group_rows"
            )
        if self.max_l2_frame_levels > self.max_l2_levels_per_chunk:
            raise ValueError(
                "max_l2_frame_levels must not exceed max_l2_levels_per_chunk"
            )
        scenarios = tuple(self.execution_scenarios)
        if not scenarios:
            raise ValueError("at least one execution scenario is required")
        if any(not isinstance(item, ExecutionAssumptions) for item in scenarios):
            raise TypeError("execution_scenarios must contain ExecutionAssumptions")
        exclusion_ms = max(
            *horizons,
            self.trade_window_ms,
            self.momentum_window_ms,
            *(
                scenario.latency_ms
                + scenario.exit_latency_ms
                + scenario.maker_timeout_ms
                for scenario in scenarios
            ),
        )
        if self.randomization_block_ms <= exclusion_ms:
            raise ValueError(
                "randomization_block_ms must exceed every horizon, feature window, and "
                "execution lifecycle"
            )
        if (
            isinstance(self.randomization_seed, bool)
            or not isinstance(self.randomization_seed, int)
            or self.randomization_seed < 0
        ):
            raise ValueError("randomization_seed must be a non-negative integer")
        if not math.isfinite(self.minimum_move_bps) or self.minimum_move_bps < 0.0:
            raise ValueError("minimum_move_bps must be finite and non-negative")
        names = tuple(item.name for item in scenarios)
        if len(set(names)) != len(names):
            raise ValueError("execution scenario names must be unique")
        if self.reference_venue != REFERENCE_VENUE:
            raise ValueError("reference_venue must remain binance_usdm")
        if self.execution_venue != EXECUTION_VENUE:
            raise ValueError("execution_venue must remain hyperliquid")
        object.__setattr__(self, "assets", assets)
        object.__setattr__(self, "horizons_ms", horizons)
        object.__setattr__(self, "execution_scenarios", scenarios)

    def as_dict(self) -> dict[str, object]:
        return {
            "assets": list(self.assets),
            "horizons_ms": list(self.horizons_ms),
            "trade_window_ms": self.trade_window_ms,
            "momentum_window_ms": self.momentum_window_ms,
            "l2_levels": self.l2_levels,
            "max_book_age_ms": self.max_book_age_ms,
            "minimum_move_bps": self.minimum_move_bps,
            "bucket_minutes": self.bucket_minutes,
            "randomization_resamples": self.randomization_resamples,
            "randomization_block_ms": self.randomization_block_ms,
            "randomization_seed": self.randomization_seed,
            "minimum_events": self.minimum_events,
            "max_event_rows": self.max_event_rows,
            "max_estimated_event_bytes": self.max_estimated_event_bytes,
            "streaming_resource_model_version": self.streaming_resource_model_version,
            "max_source_rows_per_chunk": self.max_source_rows_per_chunk,
            "max_simultaneous_batch_rows": self.max_simultaneous_batch_rows,
            "max_l2_frame_levels": self.max_l2_frame_levels,
            "max_l2_levels_per_chunk": self.max_l2_levels_per_chunk,
            "max_pending_response_states": self.max_pending_response_states,
            "max_pending_execution_states": self.max_pending_execution_states,
            "external_merge_fan_in": self.external_merge_fan_in,
            "quantile_sort_run_rows": self.quantile_sort_run_rows,
            "parquet_row_group_rows": self.parquet_row_group_rows,
            "writer_buffer_rows": self.writer_buffer_rows,
            "scratch_low_watermark_bytes": self.scratch_low_watermark_bytes,
            "scratch_reserve_bytes": self.scratch_reserve_bytes,
            "execution_scenarios": [asdict(value) for value in self.execution_scenarios],
            "reference_venue": self.reference_venue,
            "execution_venue": self.execution_venue,
        }

    @property
    def config_hash(self) -> str:
        payload = json.dumps(
            self.as_dict(), ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_lead_lag_config(path: Path) -> LeadLagConfig:
    with path.open("rb") as stream:
        payload = tomllib.load(stream)
    unknown_top_level = set(payload) - {"study", "execution_scenarios"}
    if unknown_top_level:
        raise ValueError(
            "unknown lead-lag TOML top-level keys: "
            + ", ".join(sorted(unknown_top_level))
        )
    if "study" not in payload:
        raise ValueError("lead-lag TOML requires a [study] table")
    if "execution_scenarios" not in payload:
        raise ValueError("lead-lag TOML requires [[execution_scenarios]]")
    study = payload["study"]
    scenarios = payload["execution_scenarios"]
    if not isinstance(study, dict):
        raise ValueError("[study] must be a TOML table")
    unknown_study = set(study) - _STUDY_CONFIG_FIELDS
    if unknown_study:
        raise ValueError("unknown [study] keys: " + ", ".join(sorted(unknown_study)))
    missing_study = _STUDY_CONFIG_REQUIRED_FIELDS - set(study)
    if missing_study:
        raise ValueError(
            "[study] missing explicit keys: " + ", ".join(sorted(missing_study))
        )
    if (
        not isinstance(scenarios, list)
        or not scenarios
        or any(not isinstance(item, dict) for item in scenarios)
    ):
        raise ValueError("[[execution_scenarios]] must contain at least one TOML table")
    kwargs: dict[str, object] = dict(study)
    if "assets" in kwargs:
        values = kwargs["assets"]
        if not isinstance(values, list):
            raise ValueError("study.assets must be a TOML array")
        kwargs["assets"] = tuple(str(value) for value in values)
    if "horizons_ms" in kwargs:
        values = kwargs["horizons_ms"]
        if not isinstance(values, list):
            raise ValueError("study.horizons_ms must be a TOML array")
        kwargs["horizons_ms"] = tuple(int(str(value)) for value in values)
    parsed_scenarios: list[ExecutionAssumptions] = []
    for position, item in enumerate(scenarios):
        assert isinstance(item, dict)
        unknown = set(item) - _EXECUTION_CONFIG_FIELDS
        missing = _EXECUTION_CONFIG_REQUIRED_FIELDS - set(item)
        if unknown:
            raise ValueError(
                f"unknown execution_scenarios[{position}] keys: "
                + ", ".join(sorted(unknown))
            )
        if missing:
            raise ValueError(
                f"execution_scenarios[{position}] missing explicit keys: "
                + ", ".join(sorted(missing))
            )
        if str(item["calibration_status"]).upper() != "UNCALIBRATED":
            raise ValueError(
                "file-loaded execution scenarios must remain UNCALIBRATED; "
                "no independent calibration-evidence binding is available"
            )
        try:
            parsed_scenarios.append(
                ExecutionAssumptions(
                    **{str(key): value for key, value in item.items()}
                )
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"invalid execution_scenarios[{position}]: {exc}"
            ) from None
    kwargs["execution_scenarios"] = tuple(parsed_scenarios)
    try:
        return LeadLagConfig(**kwargs)  # type: ignore[arg-type]
    except TypeError as exc:
        raise ValueError(f"invalid lead-lag configuration: {exc}") from None


@dataclass(frozen=True, slots=True)
class LeadLagAnalysis:
    summary: Mapping[str, object]
    metrics: pd.DataFrame
    bucket_metrics: pd.DataFrame
    events: pd.DataFrame
    controls: pd.DataFrame

    def as_dict(self) -> dict[str, object]:
        normalized_summary = _json_value(dict(self.summary))
        if not isinstance(normalized_summary, dict):
            raise TypeError("lead-lag summary must serialize as an object")
        return {
            "summary": normalized_summary,
            "metrics": _records(self.metrics),
            "bucket_metrics": _records(self.bucket_metrics),
            "controls": _records(self.controls),
            "event_row_count": len(self.events),
        }


@dataclass(frozen=True, slots=True)
class LeadLagChunkLimits:
    """Hard limits for one already-projected asset/interval analysis chunk."""

    max_source_rows_per_chunk: int
    max_simultaneous_batch_rows: int
    max_l2_frame_levels: int
    max_l2_levels_per_chunk: int
    max_pending_response_states: int
    max_pending_execution_states: int

    def __post_init__(self) -> None:
        for name in (
            "max_source_rows_per_chunk",
            "max_simultaneous_batch_rows",
            "max_l2_frame_levels",
            "max_l2_levels_per_chunk",
            "max_pending_response_states",
            "max_pending_execution_states",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")

    @classmethod
    def from_config(cls, config: LeadLagConfig) -> LeadLagChunkLimits:
        return cls(
            max_source_rows_per_chunk=config.max_source_rows_per_chunk,
            max_simultaneous_batch_rows=config.max_simultaneous_batch_rows,
            max_l2_frame_levels=config.max_l2_frame_levels,
            max_l2_levels_per_chunk=config.max_l2_levels_per_chunk,
            max_pending_response_states=config.max_pending_response_states,
            max_pending_execution_states=config.max_pending_execution_states,
        )


@dataclass(frozen=True, slots=True)
class LeadLagChunkResult:
    """Oracle-equivalent event frames and deterministic bounds for one chunk."""

    resource_model_version: str
    asset: str
    interval_id: str
    core_start: pd.Timestamp
    core_end: pd.Timestamp
    halo_start: pd.Timestamp
    halo_end: pd.Timestamp
    source_row_count: int
    peak_simultaneous_batch_rows: int
    peak_l2_frame_levels: int
    projected_response_states: int
    projected_execution_states: int
    primary_signal_count: int
    reverse_signal_count: int
    information_events: pd.DataFrame
    reverse_events: pd.DataFrame
    execution_events: pd.DataFrame

    @property
    def output_event_row_count(self) -> int:
        return (
            len(self.information_events)
            + len(self.reverse_events)
            + len(self.execution_events)
        )


def _json_value(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, np.generic):
        return _json_value(value.item())
    if isinstance(value, (pd.Timestamp, datetime)):
        return _utc(pd.Timestamp(value), label="JSON timestamp").isoformat().replace(
            "+00:00", "Z"
        )
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item) for item in value]
    return str(value)


def _records(frame: pd.DataFrame) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for row in frame.to_dict(orient="records"):
        normalized: dict[str, object] = {}
        for key, value in row.items():
            normalized[str(key)] = _json_value(value)
        result.append(normalized)
    return result


def _timestamp_series(values: pd.Series, *, label: str) -> pd.Series:
    normalized: list[pd.Timestamp] = []
    for value in values:
        try:
            timestamp = pd.Timestamp(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{label} contains an invalid timestamp") from exc
        if timestamp.tz is None:
            raise ValueError(f"{label} must contain timezone-aware timestamps")
        normalized.append(timestamp.tz_convert("UTC"))
    return pd.Series(normalized, index=values.index, dtype="datetime64[ns, UTC]")


def _require_columns(frame: pd.DataFrame, required: set[str], *, label: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing columns: {', '.join(missing)}")


def _numeric_column(
    frame: pd.DataFrame,
    name: str,
    *,
    label: str,
    positive: bool = False,
    non_negative: bool = False,
) -> pd.Series:
    values = pd.to_numeric(frame[name], errors="coerce").astype(float)
    finite = np.isfinite(values.to_numpy(dtype=float))
    if not bool(finite.all()):
        raise ValueError(f"{label}.{name} must be finite")
    if positive and bool((values <= 0.0).any()):
        raise ValueError(f"{label}.{name} must be positive")
    if non_negative and bool((values < 0.0).any()):
        raise ValueError(f"{label}.{name} must be non-negative")
    return values


def _prepare_bbo(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "venue",
        "asset",
        "received_time",
        "bid_price",
        "ask_price",
        "bid_quantity",
        "ask_quantity",
    }
    _require_columns(frame, required, label="bbo")
    result = frame.copy()
    result["_ordinal"] = np.arange(len(result), dtype=np.int64)
    result["venue"] = result["venue"].astype(str).str.strip().str.casefold()
    result["asset"] = result["asset"].astype(str).str.strip().str.upper()
    result["received_time"] = _timestamp_series(result["received_time"], label="bbo.received_time")
    for name in ("bid_price", "ask_price"):
        result[name] = _numeric_column(result, name, label="bbo", positive=True)
    for name in ("bid_quantity", "ask_quantity"):
        result[name] = _numeric_column(result, name, label="bbo", non_negative=True)
    if bool((result["bid_price"] > result["ask_price"]).any()):
        raise ValueError("bbo contains crossed prices")
    result = result.sort_values(
        ["venue", "asset", "received_time", "_ordinal"], kind="mergesort"
    )
    # A timestamp is one simultaneous venue batch. Its terminal state is available
    # as a whole; persisted row order must never create cross-venue precedence.
    result = result.groupby(["venue", "asset", "received_time"], sort=False).tail(1).copy()
    result["mid"] = (result["bid_price"] + result["ask_price"]) / 2.0
    total_quantity = result["bid_quantity"] + result["ask_quantity"]
    result["microprice"] = np.where(
        total_quantity > 0.0,
        (
            result["ask_price"] * result["bid_quantity"]
            + result["bid_price"] * result["ask_quantity"]
        )
        / total_quantity,
        result["mid"],
    )
    return result.reset_index(drop=True)


def _prepare_trades(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "venue",
        "asset",
        "received_time",
        "price",
        "quantity",
        "aggressor_side",
    }
    if frame.empty and not required.issubset(frame.columns):
        return pd.DataFrame(columns=sorted(required | {"quote_quantity", "_ordinal", "signed_quote"}))
    _require_columns(frame, required, label="trades")
    result = frame.copy()
    result["_ordinal"] = np.arange(len(result), dtype=np.int64)
    result["venue"] = result["venue"].astype(str).str.strip().str.casefold()
    result["asset"] = result["asset"].astype(str).str.strip().str.upper()
    result["received_time"] = _timestamp_series(
        result["received_time"], label="trades.received_time"
    )
    result["price"] = _numeric_column(result, "price", label="trades", positive=True)
    result["quantity"] = _numeric_column(result, "quantity", label="trades", positive=True)
    side = result["aggressor_side"].astype(str).str.strip().str.casefold()
    if not bool(side.isin(["buy", "sell"]).all()):
        raise ValueError("trades.aggressor_side must be buy or sell")
    result["aggressor_side"] = side
    if "quote_quantity" in result:
        quote = pd.to_numeric(result["quote_quantity"], errors="coerce").astype(float)
        fallback = result["price"] * result["quantity"]
        quote = quote.where(np.isfinite(quote) & quote.gt(0.0), fallback)
    else:
        quote = result["price"] * result["quantity"]
    result["quote_quantity"] = quote
    result["signed_quote"] = np.where(side.eq("buy"), quote, -quote)
    return result.sort_values(
        ["venue", "asset", "received_time", "_ordinal"], kind="mergesort"
    ).reset_index(drop=True)


def _prepare_l2(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"venue", "asset", "received_time", "side", "price", "quantity"}
    atomic_required = {"venue", "asset", "received_time", "bids", "asks"}
    if atomic_required.issubset(frame.columns) and not required.issubset(frame.columns):
        exploded: list[dict[str, object]] = []
        for ordinal, item in enumerate(frame.to_dict(orient="records")):
            snapshot_id = str(item.get("snapshot_id", item["received_time"]))
            for side, column in (("bid", "bids"), ("ask", "asks")):
                levels = item[column]
                if not isinstance(levels, (list, tuple)):
                    raise ValueError(f"l2.{column} must contain an atomic level sequence")
                for fallback_level, raw_level in enumerate(levels):
                    if isinstance(raw_level, Mapping):
                        level = raw_level.get("level", fallback_level)
                        price = raw_level.get("price")
                        quantity = raw_level.get("quantity")
                    elif isinstance(raw_level, (list, tuple)) and len(raw_level) >= 3:
                        level, price, quantity = raw_level[:3]
                    else:
                        raise ValueError(f"l2.{column} contains an invalid level")
                    exploded.append(
                        {
                            "venue": item["venue"],
                            "asset": item["asset"],
                            "received_time": item["received_time"],
                            "snapshot_id": snapshot_id,
                            "side": side,
                            "level": level,
                            "price": price,
                            "quantity": quantity,
                            "_source_ordinal": ordinal,
                        }
                    )
        frame = pd.DataFrame(
            exploded,
            columns=[
                "venue",
                "asset",
                "received_time",
                "snapshot_id",
                "side",
                "level",
                "price",
                "quantity",
                "_source_ordinal",
            ],
        )
    if frame.empty and not required.issubset(frame.columns):
        return pd.DataFrame(columns=sorted(required | {"snapshot_id", "level", "_ordinal"}))
    _require_columns(frame, required, label="l2")
    result = frame.copy()
    if "_source_ordinal" in result:
        result["_ordinal"] = pd.to_numeric(result["_source_ordinal"], errors="raise").astype(int)
    else:
        result["_ordinal"] = np.arange(len(result), dtype=np.int64)
    result["venue"] = result["venue"].astype(str).str.strip().str.casefold()
    result["asset"] = result["asset"].astype(str).str.strip().str.upper()
    result["received_time"] = _timestamp_series(result["received_time"], label="l2.received_time")
    result["price"] = _numeric_column(result, "price", label="l2", positive=True)
    result["quantity"] = _numeric_column(result, "quantity", label="l2", non_negative=True)
    sides = result["side"].astype(str).str.strip().str.casefold().replace(
        {"b": "bid", "buy": "bid", "a": "ask", "sell": "ask"}
    )
    if not bool(sides.isin(["bid", "ask"]).all()):
        raise ValueError("l2.side must be bid or ask")
    result["side"] = sides
    if "snapshot_id" not in result:
        result["snapshot_id"] = result["received_time"].astype(str)
    else:
        result["snapshot_id"] = result["snapshot_id"].astype(str)
    if "level" not in result:
        result["level"] = result.groupby(
            ["venue", "asset", "received_time", "snapshot_id", "side"], sort=False
        ).cumcount()
    result["level"] = pd.to_numeric(result["level"], errors="raise").astype(int)
    if bool((result["level"] < 0).any()):
        raise ValueError("l2.level must be non-negative")
    return result.sort_values(
        ["venue", "asset", "received_time", "snapshot_id", "side", "level", "_ordinal"],
        kind="mergesort",
    ).reset_index(drop=True)


def _validated_intervals(intervals: Sequence[StrictInterval]) -> tuple[StrictInterval, ...]:
    normalized = tuple(sorted(intervals, key=lambda item: (item.start, item.end, item.tag)))
    if not normalized:
        raise ValueError("lead-lag analysis requires strict gate intervals")
    for previous, current in pairwise(normalized):
        if current.start < previous.end:
            raise ValueError("strict gate intervals cannot overlap")
    return normalized


def _interval_id(interval: StrictInterval) -> str:
    payload = "|".join(
        (
            interval.tag,
            interval.start.isoformat(timespec="microseconds"),
            interval.end.isoformat(timespec="microseconds"),
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _interval_for(timestamp: pd.Timestamp, intervals: Sequence[StrictInterval]) -> StrictInterval | None:
    value = timestamp.to_pydatetime()
    for interval in intervals:
        if value < interval.start:
            return None
        if interval.start <= value < interval.end:
            return interval
    return None


def _partition_by_interval(
    frame: pd.DataFrame, intervals: Sequence[StrictInterval]
) -> pd.DataFrame:
    if frame.empty:
        result = frame.copy()
        result["_interval_tag"] = pd.Series(dtype=str)
        result["_interval_id"] = pd.Series(dtype=str)
        return result
    assigned = [
        _interval_for(pd.Timestamp(value), intervals) for value in frame["received_time"]
    ]
    result = frame.copy()
    result["_interval_tag"] = [
        None if interval is None else interval.tag for interval in assigned
    ]
    result["_interval_id"] = [
        None if interval is None else _interval_id(interval) for interval in assigned
    ]
    return result.loc[result["_interval_id"].notna()].copy()


def _signal_id(venue: str, asset: str, family: str, timestamp: pd.Timestamp) -> str:
    payload = f"{venue.casefold()}|{asset}|{family}|{timestamp.isoformat()}".encode()
    return hashlib.sha256(payload).hexdigest()[:24]


def _append_signal(
    rows: list[dict[str, object]],
    *,
    venue: str,
    asset: str,
    family: str,
    timestamp: pd.Timestamp,
    value: float,
) -> None:
    if not math.isfinite(value) or abs(value) <= 1e-15:
        return
    rows.append(
        {
            "signal_id": _signal_id(venue, asset, family, timestamp),
            "signal_venue": venue.casefold(),
            "asset": asset,
            "signal_family": family,
            "signal_time": timestamp,
            "signal_value": value,
            "signal_strength": abs(value),
            "signal_direction": 1 if value > 0.0 else -1,
        }
    )


def _build_bbo_signals(
    bbo: pd.DataFrame,
    *,
    venue: str,
    assets: Sequence[str],
    config: LeadLagConfig,
    intervals: Sequence[StrictInterval],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    maximum_age_ns = config.max_book_age_ms * 1_000_000
    momentum_ns = config.momentum_window_ms * 1_000_000
    selected = bbo.loc[bbo["venue"].eq(venue.casefold()) & bbo["asset"].isin(assets)]
    selected = _partition_by_interval(selected, intervals)
    for (asset, _interval_identity), frame in selected.groupby(
        ["asset", "_interval_id"], sort=True
    ):
        ordered = frame.sort_values(["received_time", "_ordinal"], kind="mergesort").reset_index(
            drop=True
        )
        times = _nanoseconds(ordered["received_time"])
        for position in range(1, len(ordered)):
            current = ordered.iloc[position]
            previous = ordered.iloc[position - 1]
            if times[position] - times[position - 1] > maximum_age_ns:
                continue
            timestamp = pd.Timestamp(current["received_time"])
            previous_mid = float(previous["mid"])
            current_mid = float(current["mid"])
            mid_change = math.log(current_mid / previous_mid) * 10_000.0
            _append_signal(
                rows,
                venue=venue,
                asset=str(asset),
                family="mid_price_change",
                timestamp=timestamp,
                value=mid_change,
            )
            previous_micro = float(previous["microprice"])
            current_micro = float(current["microprice"])
            _append_signal(
                rows,
                venue=venue,
                asset=str(asset),
                family="microprice_change",
                timestamp=timestamp,
                value=math.log(current_micro / previous_micro) * 10_000.0,
            )
            bid_flow = (
                float(current["bid_quantity"])
                if float(current["bid_price"]) > float(previous["bid_price"])
                else (
                    -float(previous["bid_quantity"])
                    if float(current["bid_price"]) < float(previous["bid_price"])
                    else float(current["bid_quantity"] - previous["bid_quantity"])
                )
            )
            ask_flow = (
                -float(current["ask_quantity"])
                if float(current["ask_price"]) < float(previous["ask_price"])
                else (
                    float(previous["ask_quantity"])
                    if float(current["ask_price"]) > float(previous["ask_price"])
                    else -float(current["ask_quantity"] - previous["ask_quantity"])
                )
            )
            scale = max(
                float(
                    current["bid_quantity"]
                    + current["ask_quantity"]
                    + previous["bid_quantity"]
                    + previous["ask_quantity"]
                ),
                1e-12,
            )
            _append_signal(
                rows,
                venue=venue,
                asset=str(asset),
                family="bbo_change",
                timestamp=timestamp,
                value=(bid_flow + ask_flow) / scale,
            )

            target = times[position] - momentum_ns
            baseline = int(np.searchsorted(times, target, side="right") - 1)
            if baseline >= 0 and target - times[baseline] <= maximum_age_ns:
                past_mid = float(ordered.iloc[baseline]["mid"])
                _append_signal(
                    rows,
                    venue=venue,
                    asset=str(asset),
                    family="short_term_momentum",
                    timestamp=timestamp,
                    value=math.log(current_mid / past_mid) * 10_000.0,
                )
    return rows


def _build_trade_signals(
    trades: pd.DataFrame,
    *,
    venue: str,
    assets: Sequence[str],
    config: LeadLagConfig,
    intervals: Sequence[StrictInterval],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    selected = trades.loc[trades["venue"].eq(venue.casefold()) & trades["asset"].isin(assets)]
    selected = _partition_by_interval(selected, intervals)
    for (asset, _interval_identity), frame in selected.groupby(
        ["asset", "_interval_id"], sort=True
    ):
        grouped = (
            frame.groupby("received_time", sort=True)
            .agg(
                signed_quote=("signed_quote", "sum"),
                total_quote=("quote_quantity", "sum"),
            )
            .reset_index()
        )
        if grouped.empty:
            continue
        times = _nanoseconds(grouped["received_time"])
        signed = grouped["signed_quote"].to_numpy(dtype=float)
        total = grouped["total_quote"].to_numpy(dtype=float)
        signed_prefix = np.concatenate(([0.0], np.cumsum(signed)))
        total_prefix = np.concatenate(([0.0], np.cumsum(total)))
        window_ns = config.trade_window_ms * 1_000_000
        grouped_records = cast(
            list[dict[str, object]], grouped.to_dict(orient="records")
        )
        for position, item in enumerate(grouped_records):
            timestamp = _timestamp_value(
                item["received_time"], label="trade signal received_time"
            )
            _append_signal(
                rows,
                venue=venue,
                asset=str(asset),
                family="agg_trade",
                timestamp=timestamp,
                value=_float_value(item["signed_quote"], label="signed_quote"),
            )
            left = int(np.searchsorted(times, times[position] - window_ns, side="left"))
            signed_flow = float(signed_prefix[position + 1] - signed_prefix[left])
            total_flow = float(total_prefix[position + 1] - total_prefix[left])
            _append_signal(
                rows,
                venue=venue,
                asset=str(asset),
                family="signed_flow",
                timestamp=timestamp,
                value=signed_flow,
            )
            if total_flow > 0.0:
                _append_signal(
                    rows,
                    venue=venue,
                    asset=str(asset),
                    family="trade_imbalance",
                    timestamp=timestamp,
                    value=signed_flow / total_flow,
                )
    return rows


def _build_l2_signals(
    l2: pd.DataFrame,
    *,
    venue: str,
    assets: Sequence[str],
    config: LeadLagConfig,
    intervals: Sequence[StrictInterval],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    selected = l2.loc[l2["venue"].eq(venue.casefold()) & l2["asset"].isin(assets)]
    selected = _partition_by_interval(selected, intervals)
    snapshots: list[dict[str, object]] = []
    keys = ["asset", "received_time", "snapshot_id"]
    for (asset, timestamp, snapshot_id), frame in selected.groupby(keys, sort=True):
        bids = frame.loc[frame["side"].eq("bid")].sort_values(
            ["level", "price"], ascending=[True, False], kind="mergesort"
        )
        asks = frame.loc[frame["side"].eq("ask")].sort_values(
            ["level", "price"], ascending=[True, True], kind="mergesort"
        )
        if bids.empty or asks.empty:
            continue
        bid_quantity = float(bids.head(config.l2_levels)["quantity"].sum())
        ask_quantity = float(asks.head(config.l2_levels)["quantity"].sum())
        total = bid_quantity + ask_quantity
        if total <= 0.0:
            continue
        snapshots.append(
            {
                "asset": str(asset),
                "received_time": _timestamp_value(
                    timestamp, label="l2 snapshot received_time"
                ),
                "snapshot_id": str(snapshot_id),
                "ordinal": int(frame["_ordinal"].max()),
                "value": (bid_quantity - ask_quantity) / total,
            }
        )
    if snapshots:
        snapshot_frame = pd.DataFrame(snapshots).sort_values(
            ["asset", "received_time", "ordinal"], kind="mergesort"
        )
        snapshot_frame = snapshot_frame.groupby(["asset", "received_time"], sort=False).tail(1)
        for item in snapshot_frame.to_dict(orient="records"):
            _append_signal(
                rows,
                venue=venue,
                asset=str(item["asset"]),
                family="l2_imbalance",
                timestamp=pd.Timestamp(item["received_time"]),
                value=float(item["value"]),
            )
    return rows


def _build_signals(
    bbo: pd.DataFrame,
    trades: pd.DataFrame,
    l2: pd.DataFrame,
    *,
    venue: str,
    assets: Sequence[str],
    config: LeadLagConfig,
    intervals: Sequence[StrictInterval],
) -> pd.DataFrame:
    rows = [
        *_build_bbo_signals(
            bbo, venue=venue, assets=assets, config=config, intervals=intervals
        ),
        *_build_trade_signals(
            trades, venue=venue, assets=assets, config=config, intervals=intervals
        ),
        *_build_l2_signals(
            l2, venue=venue, assets=assets, config=config, intervals=intervals
        ),
    ]
    columns = [
        "signal_id",
        "signal_venue",
        "asset",
        "signal_family",
        "signal_time",
        "signal_value",
        "signal_strength",
        "signal_direction",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns).sort_values(
        ["signal_time", "asset", "signal_family", "signal_id"], kind="mergesort"
    ).reset_index(drop=True)


class LeadLagCapacityError(ValueError):
    """Raised before long-form event materialization exceeds preregistered limits."""


class LeadLagChunkCapacityError(ValueError):
    """Raised before a bounded chunk exceeds a preregistered hard limit."""


@dataclass(frozen=True, slots=True)
class _CapacityEstimate:
    primary_information_rows: int
    reverse_control_rows: int
    execution_rows: int
    total_event_rows: int
    information_row_bytes: int
    execution_row_bytes: int
    estimated_event_bytes: int

    def as_dict(self, config: LeadLagConfig) -> dict[str, object]:
        return {
            "status": "PASS",
            "method": "CONSERVATIVE_LONG_FORM_V1",
            "scope": "EVENT_TABLE_MATERIALIZATION_INCLUDING_PEAK_DUPLICATION",
            "primary_information_rows_upper_bound": self.primary_information_rows,
            "reverse_control_rows_upper_bound": self.reverse_control_rows,
            "execution_rows_upper_bound": self.execution_rows,
            "total_event_rows_upper_bound": self.total_event_rows,
            "information_row_bytes_bound": self.information_row_bytes,
            "execution_row_bytes_bound": self.execution_row_bytes,
            "materialization_peak_multiplier": _EVENT_MATERIALIZATION_PEAK_MULTIPLIER,
            "estimated_event_bytes_upper_bound": self.estimated_event_bytes,
            "max_event_rows": config.max_event_rows,
            "max_estimated_event_bytes": config.max_estimated_event_bytes,
        }


def _estimate_event_capacity(
    primary_signals: pd.DataFrame,
    reverse_signals: pd.DataFrame,
    intervals: Sequence[StrictInterval],
    config: LeadLagConfig,
) -> _CapacityEstimate:
    horizon_count = len(config.horizons_ms)
    primary_rows = len(primary_signals) * horizon_count
    reverse_rows = len(reverse_signals) * horizon_count
    execution_rows = (
        primary_rows * len(config.execution_scenarios) * 2
    )  # taker + maker for every scenario
    total_rows = primary_rows + reverse_rows + execution_rows
    maximum_asset_bytes = max(
        (len(asset.encode("utf-8")) for asset in config.assets), default=0
    )
    maximum_interval_tag_bytes = max(
        (len(interval.tag.encode("utf-8")) for interval in intervals), default=0
    )
    maximum_scenario_bytes = max(
        (
            len(scenario.name.encode("utf-8"))
            + len(scenario.source.encode("utf-8"))
            + len(scenario.calibration_status.encode("utf-8"))
            for scenario in config.execution_scenarios
        ),
        default=0,
    )
    shared_variable_bytes = maximum_asset_bytes + maximum_interval_tag_bytes
    information_row_bytes = _INFORMATION_EVENT_ROW_BASE_BYTES + shared_variable_bytes
    execution_row_bytes = (
        _EXECUTION_EVENT_ROW_BASE_BYTES
        + shared_variable_bytes
        + maximum_scenario_bytes
    )
    final_table_bytes = (
        (primary_rows + reverse_rows) * information_row_bytes
        + execution_rows * execution_row_bytes
    )
    estimated_bytes = final_table_bytes * _EVENT_MATERIALIZATION_PEAK_MULTIPLIER
    return _CapacityEstimate(
        primary_information_rows=primary_rows,
        reverse_control_rows=reverse_rows,
        execution_rows=execution_rows,
        total_event_rows=total_rows,
        information_row_bytes=information_row_bytes,
        execution_row_bytes=execution_row_bytes,
        estimated_event_bytes=estimated_bytes,
    )


def _enforce_event_capacity(
    estimate: _CapacityEstimate, config: LeadLagConfig
) -> None:
    failures: list[str] = []
    if estimate.total_event_rows > config.max_event_rows:
        failures.append(
            f"estimated_event_rows={estimate.total_event_rows} "
            f"exceeds max_event_rows={config.max_event_rows}"
        )
    if estimate.estimated_event_bytes > config.max_estimated_event_bytes:
        failures.append(
            f"estimated_event_bytes={estimate.estimated_event_bytes} "
            "exceeds "
            f"max_estimated_event_bytes={config.max_estimated_event_bytes}"
        )
    if failures:
        raise LeadLagCapacityError(
            "lead-lag capacity preflight refused event materialization: "
            + "; ".join(failures)
            + "; no response or execution event rows were materialized"
        )


@dataclass(frozen=True, slots=True)
class _BboSeries:
    frame: pd.DataFrame
    times_ns: np.ndarray
    mids: np.ndarray
    bids: np.ndarray
    asks: np.ndarray
    bid_quantities: np.ndarray
    ask_quantities: np.ndarray


@dataclass(frozen=True, slots=True)
class _BboState:
    received_time_ns: int
    bid_price: float
    ask_price: float
    bid_quantity: float
    ask_quantity: float

    @property
    def mid(self) -> float:
        return (self.bid_price + self.ask_price) / 2.0


def _bbo_state(series: _BboSeries, position: int) -> _BboState:
    return _BboState(
        received_time_ns=int(series.times_ns[position]),
        bid_price=float(series.bids[position]),
        ask_price=float(series.asks[position]),
        bid_quantity=float(series.bid_quantities[position]),
        ask_quantity=float(series.ask_quantities[position]),
    )


def _bbo_series_by_asset(bbo: pd.DataFrame, *, venue: str) -> dict[str, _BboSeries]:
    result: dict[str, _BboSeries] = {}
    selected = bbo.loc[bbo["venue"].eq(venue.casefold())]
    for asset, frame in selected.groupby("asset", sort=True):
        ordered = frame.sort_values(["received_time", "_ordinal"], kind="mergesort").reset_index(
            drop=True
        )
        result[str(asset)] = _BboSeries(
            frame=ordered,
            times_ns=_nanoseconds(ordered["received_time"]),
            mids=ordered["mid"].to_numpy(dtype=float),
            bids=ordered["bid_price"].to_numpy(dtype=float),
            asks=ordered["ask_price"].to_numpy(dtype=float),
            bid_quantities=ordered["bid_quantity"].to_numpy(dtype=float),
            ask_quantities=ordered["ask_quantity"].to_numpy(dtype=float),
        )
    return result


def _classification(value: float, minimum_move_bps: float) -> str:
    if value > minimum_move_bps:
        return "same_direction"
    if value < -minimum_move_bps:
        return "adverse"
    return "neutral"


def _response_rows(
    signals: pd.DataFrame,
    response_bbo: pd.DataFrame,
    intervals: Sequence[StrictInterval],
    config: LeadLagConfig,
    *,
    response_venue: str,
    signal_role: Literal["primary", "reverse"],
) -> pd.DataFrame:
    series_by_asset = _bbo_series_by_asset(response_bbo, venue=response_venue)
    rows: list[dict[str, object]] = []
    maximum_age_ns = config.max_book_age_ms * 1_000_000
    bucket_frequency = f"{config.bucket_minutes}min"
    signal_records = cast(
        list[dict[str, object]], signals.to_dict(orient="records")
    )
    for signal in signal_records:
        timestamp = _timestamp_value(signal["signal_time"], label="signal_time")
        timestamp_ns = int(timestamp.value)
        interval = _interval_for(timestamp, intervals)
        series = series_by_asset.get(str(signal["asset"]))
        for horizon_ms in config.horizons_ms:
            target = timestamp + pd.Timedelta(milliseconds=horizon_ms)
            base: dict[str, object] = {
                **signal,
                "signal_role": signal_role,
                "time_axis": "received_time",
                "source_time_status": SOURCE_TIME_STATUS,
                "horizon_ms": horizon_ms,
                "target_time": target,
                "time_bucket": timestamp.floor(bucket_frequency),
                "interval_tag": None if interval is None else interval.tag,
                "interval_id": None if interval is None else _interval_id(interval),
                "interval_start": pd.NaT
                if interval is None
                else pd.Timestamp(interval.start),
                "interval_end": pd.NaT if interval is None else pd.Timestamp(interval.end),
                "evaluable": False,
                "exclusion_reason": None,
                "baseline_time": pd.NaT,
                "response_state_time": pd.NaT,
                "baseline_mid": math.nan,
                "response_mid": math.nan,
                "response_bps": math.nan,
                "negative_lag_response_bps": math.nan,
                "first_move_delay_ms": math.nan,
                "first_move_direction": "none",
                "classification": "not_evaluable",
            }
            if interval is None:
                base["exclusion_reason"] = "signal_outside_strict_interval"
                rows.append(base)
                continue
            if not interval.contains_window(timestamp.to_pydatetime(), target.to_pydatetime()):
                base["exclusion_reason"] = "horizon_crosses_strict_interval"
                rows.append(base)
                continue
            if series is None or len(series.frame) == 0:
                base["exclusion_reason"] = "missing_response_bbo"
                rows.append(base)
                continue

            # Apply the complete simultaneous t batch before taking the baseline.
            # A move at exactly t is therefore absorbed and cannot be credited to
            # the Binance signal at t.
            baseline_position = int(np.searchsorted(series.times_ns, timestamp_ns, side="right") - 1)
            if baseline_position < 0:
                base["exclusion_reason"] = "missing_baseline_bbo"
                rows.append(base)
                continue
            baseline_time_ns = int(series.times_ns[baseline_position])
            if baseline_time_ns < int(pd.Timestamp(interval.start).value):
                base["exclusion_reason"] = "baseline_outside_strict_interval"
                rows.append(base)
                continue
            if timestamp_ns - baseline_time_ns > maximum_age_ns:
                base["exclusion_reason"] = "stale_baseline_bbo"
                rows.append(base)
                continue

            target_ns = int(target.value)
            response_position = int(np.searchsorted(series.times_ns, target_ns, side="right") - 1)
            if response_position < baseline_position:
                base["exclusion_reason"] = "missing_response_state"
                rows.append(base)
                continue
            response_time_ns = int(series.times_ns[response_position])
            if target_ns - response_time_ns > maximum_age_ns:
                base["exclusion_reason"] = "stale_response_bbo"
                rows.append(base)
                continue

            baseline_mid = float(series.mids[baseline_position])
            response_mid = float(series.mids[response_position])
            direction = _integer_value(
                signal["signal_direction"], label="signal_direction"
            )
            response_bps = direction * math.log(response_mid / baseline_mid) * 10_000.0
            first_delay = math.nan
            first_direction = "none"
            for position in range(baseline_position + 1, response_position + 1):
                move_bps = math.log(float(series.mids[position]) / baseline_mid) * 10_000.0
                if abs(move_bps) > config.minimum_move_bps:
                    first_delay = (int(series.times_ns[position]) - timestamp_ns) / 1_000_000.0
                    first_direction = "same" if direction * move_bps > 0.0 else "opposite"
                    break

            past_target = timestamp - pd.Timedelta(milliseconds=horizon_ms)
            negative_response = math.nan
            if interval.start <= past_target.to_pydatetime():
                past_target_ns = int(past_target.value)
                past_position = int(
                    np.searchsorted(series.times_ns, past_target_ns, side="right") - 1
                )
                past_state_ns = (
                    int(series.times_ns[past_position]) if past_position >= 0 else -1
                )
                if (
                    past_position >= 0
                    and past_state_ns >= int(pd.Timestamp(interval.start).value)
                    and past_target_ns - past_state_ns <= maximum_age_ns
                ):
                    past_mid = float(series.mids[past_position])
                    negative_response = direction * math.log(baseline_mid / past_mid) * 10_000.0

            block_number = int(
                (timestamp_ns - int(pd.Timestamp(interval.start).value))
                // (config.randomization_block_ms * 1_000_000)
            )
            base.update(
                {
                    "evaluable": True,
                    "baseline_time": pd.Timestamp(baseline_time_ns, tz="UTC"),
                    "response_state_time": pd.Timestamp(response_time_ns, tz="UTC"),
                    "baseline_mid": baseline_mid,
                    "baseline_bid": float(series.bids[baseline_position]),
                    "baseline_ask": float(series.asks[baseline_position]),
                    "baseline_bid_quantity": float(
                        series.bid_quantities[baseline_position]
                    ),
                    "baseline_ask_quantity": float(
                        series.ask_quantities[baseline_position]
                    ),
                    "response_mid": response_mid,
                    "response_bid": float(series.bids[response_position]),
                    "response_ask": float(series.asks[response_position]),
                    "response_bps": response_bps,
                    "negative_lag_response_bps": negative_response,
                    "first_move_delay_ms": first_delay,
                    "first_move_direction": first_direction,
                    "classification": _classification(response_bps, config.minimum_move_bps),
                    "randomization_block": (
                        f"{_interval_id(interval)}|{block_number:012d}"
                    ),
                }
            )
            rows.append(base)
    return pd.DataFrame(rows)


def _safe_mean(values: pd.Series) -> float:
    return float(values.mean()) if len(values) else math.nan


def _safe_quantile(values: pd.Series, quantile: float) -> float:
    return float(values.quantile(quantile)) if len(values) else math.nan


def _metric_row(
    frame: pd.DataFrame,
    *,
    asset: str,
    family: str,
    horizon_ms: int,
    minimum_events: int,
) -> dict[str, object]:
    evaluable = frame.loc[frame["evaluable"].eq(True)] if not frame.empty else frame
    response = pd.to_numeric(evaluable.get("response_bps", pd.Series(dtype=float)), errors="coerce").dropna()
    classifications = evaluable.get("classification", pd.Series(dtype=str))
    same = int(classifications.eq("same_direction").sum())
    neutral = int(classifications.eq("neutral").sum())
    adverse = int(classifications.eq("adverse").sum())
    count = len(response)
    negative = pd.to_numeric(
        evaluable.get("negative_lag_response_bps", pd.Series(dtype=float)), errors="coerce"
    ).dropna()
    first_move = evaluable.loc[
        evaluable.get("first_move_direction", pd.Series(dtype=str)).isin(
            ["same", "opposite"]
        )
    ]
    first_directions = first_move.get("first_move_direction", pd.Series(dtype=str))
    first_delays = pd.to_numeric(
        first_move.get("first_move_delay_ms", pd.Series(dtype=float)), errors="coerce"
    ).dropna()
    first_same = int(first_directions.eq("same").sum())
    first_opposite = int(first_directions.eq("opposite").sum())
    first_count = first_same + first_opposite
    return {
        "analysis_kind": "information",
        "asset": asset,
        "signal_family": family,
        "horizon_ms": horizon_ms,
        "execution_scenario": None,
        "execution_model": None,
        "signal_count": len(frame),
        "evaluable_count": count,
        "excluded_count": len(frame) - count,
        "same_direction_count": same,
        "neutral_count": neutral,
        "adverse_count": adverse,
        "same_direction_rate": same / count if count else math.nan,
        "false_positive_rate": (neutral + adverse) / count if count else math.nan,
        "adverse_rate": adverse / count if count else math.nan,
        "expected_move_bps": _safe_mean(response),
        "median_move_bps": _safe_quantile(response, 0.5),
        "q10_move_bps": _safe_quantile(response, 0.1),
        "q90_move_bps": _safe_quantile(response, 0.9),
        "negative_lag_expected_move_bps": _safe_mean(negative),
        "first_move_observed_count": first_count,
        "first_move_same_direction_count": first_same,
        "first_move_opposite_count": first_opposite,
        "first_move_same_direction_rate": first_same / first_count
        if first_count
        else math.nan,
        "first_move_opposite_rate": first_opposite / first_count
        if first_count
        else math.nan,
        "median_first_move_delay_ms": _safe_quantile(first_delays, 0.5),
        "minimum_events_met": count >= minimum_events,
        "inference_status": (
            "PENDING_RANDOMIZATION"
            if count >= minimum_events
            else "NOT_ADMISSIBLE_MINIMUM_EVENTS"
        ),
        "empirical_p_value": math.nan,
        "fwer_p_value": math.nan,
        "fdr_q_value": math.nan,
        "source_time_status": SOURCE_TIME_STATUS,
    }


def _information_metrics(events: pd.DataFrame, config: LeadLagConfig) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    empty = events.iloc[0:0]
    grouped = (
        {
            (
                str(asset),
                str(family),
                _integer_value(horizon, label="horizon_ms"),
            ): frame
            for (asset, family, horizon), frame in events.groupby(
                ["asset", "signal_family", "horizon_ms"], sort=False
            )
        }
        if not events.empty
        else {}
    )
    for asset in config.assets:
        for family in SIGNAL_FAMILIES:
            for horizon_ms in config.horizons_ms:
                selected = grouped.get((asset, family, horizon_ms), empty)
                rows.append(
                    _metric_row(
                        selected,
                        asset=asset,
                        family=family,
                        horizon_ms=horizon_ms,
                        minimum_events=config.minimum_events,
                    )
                )
    return pd.DataFrame(rows)


def _bucket_starts(
    intervals: Sequence[StrictInterval], bucket_minutes: int
) -> tuple[pd.Timestamp, ...]:
    frequency = pd.Timedelta(minutes=bucket_minutes)
    starts: set[pd.Timestamp] = set()
    for interval in intervals:
        cursor = pd.Timestamp(interval.start).floor(f"{bucket_minutes}min")
        stop = pd.Timestamp(interval.end)
        while cursor < stop:
            if cursor + frequency > pd.Timestamp(interval.start):
                starts.add(cursor)
            cursor += frequency
    return tuple(sorted(starts))


def _bucket_metrics(
    events: pd.DataFrame,
    intervals: Sequence[StrictInterval],
    config: LeadLagConfig,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    empty = events.iloc[0:0]
    grouped = (
        {
            (
                _timestamp_value(bucket, label="time_bucket"),
                str(asset),
                str(family),
                _integer_value(horizon, label="horizon_ms"),
            ): frame
            for (bucket, asset, family, horizon), frame in events.groupby(
                ["time_bucket", "asset", "signal_family", "horizon_ms"], sort=False
            )
        }
        if not events.empty
        else {}
    )
    for bucket in _bucket_starts(intervals, config.bucket_minutes):
        for asset in config.assets:
            for family in SIGNAL_FAMILIES:
                for horizon_ms in config.horizons_ms:
                    selected = grouped.get(
                        (bucket, asset, family, horizon_ms), empty
                    )
                    row = _metric_row(
                        selected,
                        asset=asset,
                        family=family,
                        horizon_ms=horizon_ms,
                        minimum_events=config.minimum_events,
                    )
                    row["time_bucket"] = bucket
                    if bool(row["minimum_events_met"]):
                        row["inference_status"] = "DESCRIPTIVE_BUCKET_NO_INFERENCE"
                    rows.append(row)
    return pd.DataFrame(rows)


def _benjamini_hochberg(values: Sequence[float]) -> list[float]:
    if not values:
        return []
    raw = np.asarray(values, dtype=float)
    order = np.argsort(raw, kind="mergesort")
    adjusted = np.empty(len(raw), dtype=float)
    running = 1.0
    for reverse_rank in range(len(raw) - 1, -1, -1):
        position = int(order[reverse_rank])
        rank = reverse_rank + 1
        candidate = min(1.0, float(raw[position]) * len(raw) / rank)
        running = min(running, candidate)
        adjusted[position] = running
    return [float(value) for value in adjusted]


def _randomized_controls(
    events: pd.DataFrame,
    metrics: pd.DataFrame,
    reverse_events: pd.DataFrame,
    config: LeadLagConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    evaluable = events.loc[events["evaluable"].eq(True)].copy() if not events.empty else events
    empty_evaluable = evaluable.iloc[0:0]
    grouped_evaluable = (
        {
            (
                str(asset),
                str(family),
                _integer_value(horizon, label="horizon_ms"),
            ): frame
            for (asset, family, horizon), frame in evaluable.groupby(
                ["asset", "signal_family", "horizon_ms"], sort=False
            )
        }
        if not evaluable.empty
        else {}
    )
    reverse_evaluable = (
        reverse_events.loc[reverse_events["evaluable"].eq(True)]
        if not reverse_events.empty
        else reverse_events
    )
    empty_reverse = reverse_evaluable.iloc[0:0]
    grouped_reverse = (
        {
            (
                str(asset),
                str(family),
                _integer_value(horizon, label="horizon_ms"),
            ): frame
            for (asset, family, horizon), frame in reverse_evaluable.groupby(
                ["asset", "signal_family", "horizon_ms"], sort=False
            )
        }
        if not reverse_evaluable.empty
        else {}
    )
    hypothesis_keys: list[tuple[str, str, int]] = []
    samples: list[np.ndarray] = []
    observed_statistics: list[float] = []
    null_statistics: list[np.ndarray] = []

    if not evaluable.empty:
        unique_blocks = sorted(str(value) for value in evaluable["randomization_block"].unique())
        block_location = {value: index for index, value in enumerate(unique_blocks)}
        rng = np.random.default_rng(config.randomization_seed)
        signs = rng.choice(
            np.array([-1.0, 1.0]),
            size=(config.randomization_resamples, len(unique_blocks)),
            replace=True,
        )
        for asset in config.assets:
            for family in SIGNAL_FAMILIES:
                for horizon_ms in config.horizons_ms:
                    selected = grouped_evaluable.get(
                        (asset, family, horizon_ms), empty_evaluable
                    )
                    if len(selected) < config.minimum_events:
                        continue
                    response = selected["response_bps"].to_numpy(dtype=float)
                    locations = np.asarray(
                        [block_location[str(value)] for value in selected["randomization_block"]],
                        dtype=np.int64,
                    )
                    denominator = float(np.sqrt(np.square(response).sum()) / len(response))
                    denominator = max(denominator, 1e-12)
                    observed = float(response.mean() / denominator)
                    randomized_means = (signs[:, locations] * response[np.newaxis, :]).mean(axis=1)
                    randomized = randomized_means / denominator
                    hypothesis_keys.append((asset, family, horizon_ms))
                    samples.append(randomized_means)
                    observed_statistics.append(observed)
                    null_statistics.append(randomized)

    controls: list[dict[str, object]] = []
    raw_p_values: list[float] = []
    fwer_values: list[float] = []
    if null_statistics:
        null_matrix = np.column_stack(null_statistics)
        maximum_null = np.max(np.abs(null_matrix), axis=1)
        for key, null_means, observed in zip(
            hypothesis_keys, samples, observed_statistics, strict=True
        ):
            selected = grouped_evaluable[(key[0], key[1], key[2])]
            observed_mean = float(selected["response_bps"].mean())
            empirical = (
                1.0 + float(np.count_nonzero(np.abs(null_means) >= abs(observed_mean)))
            ) / (config.randomization_resamples + 1.0)
            fwer = (
                1.0 + float(np.count_nonzero(maximum_null >= abs(observed)))
            ) / (config.randomization_resamples + 1.0)
            raw_p_values.append(empirical)
            fwer_values.append(fwer)
        q_values = _benjamini_hochberg(raw_p_values)
    else:
        q_values = []

    metric_result = metrics.copy()
    lookup = {
        key: (raw_p_values[position], fwer_values[position], q_values[position])
        for position, key in enumerate(hypothesis_keys)
    }
    for index, row in metric_result.iterrows():
        key = (str(row["asset"]), str(row["signal_family"]), int(row["horizon_ms"]))
        if key in lookup:
            empirical, fwer, q_value = lookup[key]
            metric_result.at[index, "empirical_p_value"] = empirical
            metric_result.at[index, "fwer_p_value"] = fwer
            metric_result.at[index, "fdr_q_value"] = q_value
            metric_result.at[index, "inference_status"] = "ADMISSIBLE_RANDOMIZATION"

    null_lookup = {
        key: samples[position] for position, key in enumerate(hypothesis_keys)
    }

    for asset in config.assets:
        for family in SIGNAL_FAMILIES:
            for horizon_ms in config.horizons_ms:
                selected = grouped_evaluable.get(
                    (asset, family, horizon_ms), empty_evaluable
                )
                key = (asset, family, horizon_ms)
                inference = lookup.get(key)
                hypothesis_null_means = null_lookup.get(key)
                controls.append(
                    {
                        "control_type": "block_sign_randomization",
                        "asset": asset,
                        "signal_family": family,
                        "horizon_ms": horizon_ms,
                        "sample_count": len(selected),
                        "observed_expected_move_bps": _safe_mean(
                            pd.to_numeric(
                                selected.get(
                                    "response_bps", pd.Series(dtype=float)
                                ),
                                errors="coerce",
                            ).dropna()
                        ),
                        "control_expected_move_bps": float(hypothesis_null_means.mean())
                        if hypothesis_null_means is not None
                        else math.nan,
                        "control_std_bps": float(hypothesis_null_means.std(ddof=1))
                        if hypothesis_null_means is not None
                        else math.nan,
                        "empirical_p_value": inference[0]
                        if inference is not None
                        else math.nan,
                        "fwer_p_value": inference[1]
                        if inference is not None
                        else math.nan,
                        "fdr_q_value": inference[2]
                        if inference is not None
                        else math.nan,
                        "resamples": config.randomization_resamples
                        if inference is not None
                        else 0,
                        "block_ms": config.randomization_block_ms,
                        "seed": config.randomization_seed,
                        "inference_status": "ADMISSIBLE_RANDOMIZATION"
                        if inference is not None
                        else "NOT_ADMISSIBLE_MINIMUM_EVENTS",
                    }
                )
                negative = pd.to_numeric(
                    selected.get("negative_lag_response_bps", pd.Series(dtype=float)),
                    errors="coerce",
                ).dropna()
                controls.append(
                    {
                        "control_type": "negative_lag",
                        "asset": asset,
                        "signal_family": family,
                        "horizon_ms": horizon_ms,
                        "sample_count": len(negative),
                        "observed_expected_move_bps": _safe_mean(
                            pd.to_numeric(selected.get("response_bps", pd.Series(dtype=float)))
                        ),
                        "control_expected_move_bps": _safe_mean(negative),
                        "control_std_bps": float(negative.std(ddof=1))
                        if len(negative) > 1
                        else math.nan,
                        "empirical_p_value": math.nan,
                        "fwer_p_value": math.nan,
                        "fdr_q_value": math.nan,
                        "resamples": 0,
                        "block_ms": None,
                        "seed": None,
                        "inference_status": "DESCRIPTIVE_CONTROL_ONLY",
                    }
                )
                reverse = grouped_reverse.get(
                    (asset, family, horizon_ms), empty_reverse
                )
                reverse_response = pd.to_numeric(
                    reverse.get("response_bps", pd.Series(dtype=float)), errors="coerce"
                ).dropna()
                controls.append(
                    {
                        "control_type": "reverse_hyperliquid_to_binance",
                        "asset": asset,
                        "signal_family": family,
                        "horizon_ms": horizon_ms,
                        "sample_count": len(reverse_response),
                        "observed_expected_move_bps": _safe_mean(
                            pd.to_numeric(selected.get("response_bps", pd.Series(dtype=float)))
                        ),
                        "control_expected_move_bps": _safe_mean(reverse_response),
                        "control_std_bps": float(reverse_response.std(ddof=1))
                        if len(reverse_response) > 1
                        else math.nan,
                        "empirical_p_value": math.nan,
                        "fwer_p_value": math.nan,
                        "fdr_q_value": math.nan,
                        "resamples": 0,
                        "block_ms": None,
                        "seed": None,
                        "inference_status": "DESCRIPTIVE_CONTROL_ONLY",
                    }
                )
    return metric_result, pd.DataFrame(controls)


@dataclass(frozen=True, slots=True)
class _Fill:
    average_price: float
    base_quantity: float
    fraction: float


@dataclass(frozen=True, slots=True)
class _BookSnapshot:
    received_time_ns: int
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]


@dataclass(frozen=True, slots=True)
class _L2Series:
    times_ns: np.ndarray
    snapshots: tuple[_BookSnapshot, ...]


def _compact_snapshot(timestamp_ns: int, frame: pd.DataFrame) -> _BookSnapshot:
    bids: list[tuple[int, float, float]] = []
    asks: list[tuple[int, float, float]] = []
    for side, level, price, quantity in frame[
        ["side", "level", "price", "quantity"]
    ].itertuples(index=False, name=None):
        target = bids if str(side) == "bid" else asks
        target.append((int(level), float(price), float(quantity)))
    bids.sort(key=lambda item: (item[0], -item[1]))
    asks.sort(key=lambda item: (item[0], item[1]))
    return _BookSnapshot(
        received_time_ns=timestamp_ns,
        bids=tuple((price, quantity) for _level, price, quantity in bids),
        asks=tuple((price, quantity) for _level, price, quantity in asks),
    )


def _l2_series_by_asset(l2: pd.DataFrame, *, venue: str) -> dict[str, _L2Series]:
    result: dict[str, _L2Series] = {}
    selected = l2.loc[l2["venue"].eq(venue.casefold())]
    for asset, asset_frame in selected.groupby("asset", sort=True):
        snapshots: list[tuple[int, int, pd.DataFrame]] = []
        for (timestamp, _snapshot_id), frame in asset_frame.groupby(
            ["received_time", "snapshot_id"], sort=True
        ):
            snapshots.append(
                (
                    int(
                        _timestamp_value(
                            timestamp, label="l2 received_time"
                        ).value
                    ),
                    int(frame["_ordinal"].max()),
                    frame.copy(),
                )
            )
        # If more than one complete snapshot exists at one receive timestamp, use
        # the terminal snapshot of that simultaneous venue batch.
        terminal: dict[int, tuple[int, pd.DataFrame]] = {}
        for timestamp_ns, ordinal, frame in snapshots:
            previous = terminal.get(timestamp_ns)
            if previous is None or ordinal >= previous[0]:
                terminal[timestamp_ns] = (ordinal, frame)
        ordered = sorted(terminal.items())
        result[str(asset)] = _L2Series(
            times_ns=np.asarray([item[0] for item in ordered], dtype=np.int64),
            snapshots=tuple(
                _compact_snapshot(timestamp_ns, item[1])
                for timestamp_ns, item in ordered
            ),
        )
    return result


def _fresh_l2_snapshot(
    series: _L2Series | None,
    timestamp_ns: int,
    maximum_age_ns: int,
) -> _BookSnapshot | None:
    if series is None or len(series.times_ns) == 0:
        return None
    position = int(np.searchsorted(series.times_ns, timestamp_ns, side="right") - 1)
    if position < 0 or timestamp_ns - int(series.times_ns[position]) > maximum_age_ns:
        return None
    return series.snapshots[position]


def _book_fill(
    *,
    side: Literal["buy", "sell"],
    requested_base_quantity: float,
    bbo_row: _BboState,
    snapshot: _BookSnapshot | None,
    max_participation: float,
) -> _Fill:
    if requested_base_quantity <= 0.0:
        return _Fill(math.nan, 0.0, 0.0)
    book_side = "ask" if side == "buy" else "bid"
    if snapshot is not None:
        snapshot_is_consistent = bool(snapshot.bids and snapshot.asks)
        if snapshot_is_consistent:
            top_bid = snapshot.bids[0][0]
            top_ask = snapshot.asks[0][0]
            snapshot_is_consistent = (
                snapshot.received_time_ns
                == bbo_row.received_time_ns
                and
                top_bid <= top_ask
                and math.isclose(
                    top_bid,
                    bbo_row.bid_price,
                    rel_tol=0.0,
                    abs_tol=1e-12,
                )
                and math.isclose(
                    top_ask,
                    bbo_row.ask_price,
                    rel_tol=0.0,
                    abs_tol=1e-12,
                )
            )
        if not snapshot_is_consistent:
            snapshot = None
    if snapshot is not None:
        levels = snapshot.asks if book_side == "ask" else snapshot.bids
        price_quantity = [
            (price, quantity * max_participation) for price, quantity in levels
        ]
    else:
        price_quantity = [
            (
                bbo_row.ask_price if side == "buy" else bbo_row.bid_price,
                (bbo_row.ask_quantity if side == "buy" else bbo_row.bid_quantity)
                * max_participation,
            )
        ]
    remaining = requested_base_quantity
    filled = 0.0
    quote = 0.0
    for price, available in price_quantity:
        if price <= 0.0 or available <= 0.0:
            continue
        quantity = min(remaining, available)
        filled += quantity
        quote += quantity * price
        remaining -= quantity
        if remaining <= 1e-15:
            break
    if filled <= 0.0:
        return _Fill(math.nan, 0.0, 0.0)
    return _Fill(
        average_price=quote / filled,
        base_quantity=filled,
        fraction=min(1.0, filled / requested_base_quantity),
    )


def _first_bbo_at_or_after(
    series: _BboSeries | None,
    timestamp: pd.Timestamp,
    *,
    interval: StrictInterval,
    maximum_age_ns: int,
) -> tuple[int, _BboState] | None:
    if series is None:
        return None
    timestamp_ns = int(timestamp.value)
    position = int(np.searchsorted(series.times_ns, timestamp_ns, side="left"))
    if position >= len(series.times_ns):
        return None
    observed_ns = int(series.times_ns[position])
    if observed_ns - timestamp_ns > maximum_age_ns:
        return None
    observed = pd.Timestamp(observed_ns, tz="UTC").to_pydatetime()
    if not interval.start <= observed < interval.end:
        return None
    return position, _bbo_state(series, position)


def _adjust_execution_price(
    price: float,
    *,
    side: Literal["buy", "sell"],
    slippage_bps: float,
    adverse_bps: float = 0.0,
) -> float:
    adjustment = (slippage_bps + adverse_bps) / 10_000.0
    return price * (1.0 + adjustment if side == "buy" else 1.0 - adjustment)


def _economic_values(
    *,
    direction: int,
    baseline_mid: float,
    entry_price: float,
    exit_price: float,
    entry_fee_bps: float,
    exit_fee_bps: float,
    exit_slippage_bps: float,
    adverse_exit_bps: float,
) -> tuple[float, float, float]:
    entry_fee = entry_fee_bps / 10_000.0
    exit_fee = exit_fee_bps / 10_000.0
    if direction > 0:
        gross = exit_price / entry_price - 1.0
        net = (exit_price * (1.0 - exit_fee) - entry_price * (1.0 + entry_fee)) / entry_price
        required_adjusted_exit = entry_price * (1.0 + entry_fee) / (1.0 - exit_fee)
        exit_factor = 1.0 - (exit_slippage_bps + adverse_exit_bps) / 10_000.0
        required_raw_exit = required_adjusted_exit / exit_factor
        break_even = (required_raw_exit / baseline_mid - 1.0) * 10_000.0
    else:
        gross = 1.0 - exit_price / entry_price
        net = (entry_price * (1.0 - entry_fee) - exit_price * (1.0 + exit_fee)) / entry_price
        required_adjusted_exit = entry_price * (1.0 - entry_fee) / (1.0 + exit_fee)
        exit_factor = 1.0 + (exit_slippage_bps + adverse_exit_bps) / 10_000.0
        required_raw_exit = required_adjusted_exit / exit_factor
        break_even = (1.0 - required_raw_exit / baseline_mid) * 10_000.0
    return gross * 10_000.0, net * 10_000.0, break_even


def _execution_base(
    event: Mapping[str, object],
    scenario: ExecutionAssumptions,
    model: Literal["taker", "maker"],
) -> dict[str, object]:
    return {
        **event,
        "row_kind": "execution",
        "execution_scenario": scenario.name,
        "execution_model": model,
        "execution_calibration_status": scenario.calibration_status,
        "execution_source": scenario.source,
        "execution_status": "NOT_ATTEMPTED",
        "economic_scope": "BEFORE_FUNDING",
        "funding_status": "NOT_EVALUATED",
        "economic_admissibility": "NOT_ADMISSIBLE_FUNDING_NOT_EVALUATED",
        "net_execution_scope": "FEES_SPREAD_SLIPPAGE_BEFORE_FUNDING",
        "latency_ms_assumption": scenario.latency_ms,
        "exit_latency_ms_assumption": scenario.exit_latency_ms,
        "maker_timeout_ms_assumption": scenario.maker_timeout_ms,
        "maker_fee_bps_assumption": scenario.maker_fee_bps,
        "taker_fee_bps_assumption": scenario.taker_fee_bps,
        "slippage_bps_assumption": scenario.slippage_bps,
        "adverse_exit_bps_assumption": scenario.adverse_exit_bps,
        "queue_ahead_multiplier_assumption": scenario.queue_ahead_multiplier,
        "max_participation_assumption": scenario.max_participation,
        "spread_source": "OBSERVED_HYPERLIQUID_BOOK_AT_FILL_EVENT",
        "entry_time": pd.NaT,
        "exit_time": pd.NaT,
        "entry_price": math.nan,
        "exit_price": math.nan,
        "requested_notional_usd": scenario.notional_usd,
        "entry_fill_fraction": 0.0,
        "exit_fill_fraction": 0.0,
        "matched_fill_fraction": 0.0,
        "unclosed_exposure_fraction": 0.0,
        "fill_fraction": 0.0,
        "gross_execution_bps": math.nan,
        "net_execution_bps": math.nan,
        "before_funding_execution_bps": math.nan,
        "before_cost_mid_move_bps": math.nan,
        "entry_fee_bps_applied": math.nan,
        "exit_fee_bps_applied": math.nan,
        "entry_spread_cost_bps": math.nan,
        "exit_spread_cost_bps": math.nan,
        "entry_slippage_cost_bps": math.nan,
        "exit_slippage_cost_bps": math.nan,
        "adverse_exit_cost_bps": math.nan,
        "fill_adjusted_gross_bps": math.nan,
        "fill_adjusted_net_bps": math.nan,
        "break_even_move_bps": math.nan,
    }


def _complete_execution(
    result: dict[str, object],
    *,
    direction: int,
    baseline_mid: float,
    entry_reference_mid: float,
    exit_reference_mid: float,
    entry_time: pd.Timestamp,
    exit_time: pd.Timestamp,
    entry_fill: _Fill,
    exit_fill: _Fill,
    entry_fee_bps: float,
    exit_fee_bps: float,
    scenario: ExecutionAssumptions,
    entry_side: Literal["buy", "sell"],
    entry_has_slippage: bool,
) -> dict[str, object]:
    exit_side: Literal["buy", "sell"] = "sell" if direction > 0 else "buy"
    entry_price = _adjust_execution_price(
        entry_fill.average_price,
        side=entry_side,
        slippage_bps=scenario.slippage_bps if entry_has_slippage else 0.0,
    )
    exit_price = _adjust_execution_price(
        exit_fill.average_price,
        side=exit_side,
        slippage_bps=scenario.slippage_bps,
        adverse_bps=scenario.adverse_exit_bps,
    )
    gross, net, break_even = _economic_values(
        direction=direction,
        baseline_mid=baseline_mid,
        entry_price=entry_price,
        exit_price=exit_price,
        entry_fee_bps=entry_fee_bps,
        exit_fee_bps=exit_fee_bps,
        exit_slippage_bps=scenario.slippage_bps,
        adverse_exit_bps=scenario.adverse_exit_bps,
    )
    matched_fraction = entry_fill.fraction * exit_fill.fraction
    unclosed_fraction = entry_fill.fraction * (1.0 - exit_fill.fraction)
    status = "FILLED" if matched_fraction >= 1.0 - 1e-12 else "PARTIAL"
    entry_spread = (
        direction * (entry_fill.average_price / entry_reference_mid - 1.0) * 10_000.0
    )
    exit_spread = (
        -direction * (exit_fill.average_price / exit_reference_mid - 1.0) * 10_000.0
    )
    before_cost_mid_move = (
        direction * math.log(exit_reference_mid / entry_reference_mid) * 10_000.0
    )
    result.update(
        {
            "execution_status": status,
            "entry_time": entry_time,
            "exit_time": exit_time,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "entry_fill_fraction": entry_fill.fraction,
            "exit_fill_fraction": exit_fill.fraction,
            "matched_fill_fraction": matched_fraction,
            "unclosed_exposure_fraction": unclosed_fraction,
            "fill_fraction": matched_fraction,
            "gross_execution_bps": gross,
            "net_execution_bps": net,
            "before_funding_execution_bps": net,
            "before_cost_mid_move_bps": before_cost_mid_move,
            "entry_fee_bps_applied": entry_fee_bps,
            "exit_fee_bps_applied": exit_fee_bps,
            "entry_spread_cost_bps": entry_spread,
            "exit_spread_cost_bps": exit_spread,
            "entry_slippage_cost_bps": scenario.slippage_bps
            if entry_has_slippage
            else 0.0,
            "exit_slippage_cost_bps": scenario.slippage_bps,
            "adverse_exit_cost_bps": scenario.adverse_exit_bps,
            "fill_adjusted_gross_bps": gross * matched_fraction,
            "fill_adjusted_net_bps": net * matched_fraction,
            "break_even_move_bps": break_even,
        }
    )
    return result


def _simulate_taker(
    event: Mapping[str, object],
    scenario: ExecutionAssumptions,
    *,
    interval: StrictInterval,
    bbo: _BboSeries | None,
    l2: _L2Series | None,
    maximum_age_ns: int,
) -> dict[str, object]:
    result = _execution_base(event, scenario, "taker")
    signal_time = _timestamp_value(event["signal_time"], label="signal_time")
    target_time = _timestamp_value(event["target_time"], label="target_time")
    entry_due = signal_time + pd.Timedelta(milliseconds=scenario.latency_ms)
    if entry_due >= target_time:
        result["execution_status"] = "MISSED_LATENCY"
        return result
    entry_state = _first_bbo_at_or_after(
        bbo, entry_due, interval=interval, maximum_age_ns=maximum_age_ns
    )
    if entry_state is None:
        result["execution_status"] = "MISSED_ENTRY_BOOK"
        return result
    assert bbo is not None
    entry_position, entry_row = entry_state
    entry_observed_time = pd.Timestamp(entry_row.received_time_ns, tz="UTC")
    if entry_observed_time >= target_time:
        result["execution_status"] = "MISSED_ENTRY_AFTER_HORIZON"
        return result
    direction = _integer_value(event["signal_direction"], label="signal_direction")
    entry_side: Literal["buy", "sell"] = "buy" if direction > 0 else "sell"
    reference_price = entry_row.ask_price if direction > 0 else entry_row.bid_price
    requested_base = scenario.notional_usd / reference_price
    entry_snapshot = _fresh_l2_snapshot(l2, int(bbo.times_ns[entry_position]), maximum_age_ns)
    entry_fill = _book_fill(
        side=entry_side,
        requested_base_quantity=requested_base,
        bbo_row=entry_row,
        snapshot=entry_snapshot,
        max_participation=scenario.max_participation,
    )
    if entry_fill.fraction <= 0.0:
        result["execution_status"] = "MISSED_ENTRY_LIQUIDITY"
        return result

    exit_due = target_time + pd.Timedelta(milliseconds=scenario.exit_latency_ms)
    exit_state = _first_bbo_at_or_after(
        bbo, exit_due, interval=interval, maximum_age_ns=maximum_age_ns
    )
    if exit_state is None:
        result["execution_status"] = "UNRESOLVED_EXIT_BOOK"
        result["entry_time"] = entry_observed_time
        result["entry_price"] = _adjust_execution_price(
            entry_fill.average_price,
            side=entry_side,
            slippage_bps=scenario.slippage_bps,
        )
        result["entry_fill_fraction"] = entry_fill.fraction
        result["unclosed_exposure_fraction"] = entry_fill.fraction
        return result
    exit_position, exit_row = exit_state
    exit_side: Literal["buy", "sell"] = "sell" if direction > 0 else "buy"
    exit_snapshot = _fresh_l2_snapshot(l2, int(bbo.times_ns[exit_position]), maximum_age_ns)
    exit_fill = _book_fill(
        side=exit_side,
        requested_base_quantity=entry_fill.base_quantity,
        bbo_row=exit_row,
        snapshot=exit_snapshot,
        max_participation=scenario.max_participation,
    )
    if exit_fill.fraction <= 0.0:
        result["execution_status"] = "UNRESOLVED_EXIT_LIQUIDITY"
        result["entry_time"] = entry_observed_time
        result["entry_price"] = _adjust_execution_price(
            entry_fill.average_price,
            side=entry_side,
            slippage_bps=scenario.slippage_bps,
        )
        result["entry_fill_fraction"] = entry_fill.fraction
        result["unclosed_exposure_fraction"] = entry_fill.fraction
        return result
    return _complete_execution(
        result,
        direction=direction,
        baseline_mid=_float_value(event["baseline_mid"], label="baseline_mid"),
        entry_reference_mid=entry_row.mid,
        exit_reference_mid=exit_row.mid,
        entry_time=entry_observed_time,
        exit_time=pd.Timestamp(exit_row.received_time_ns, tz="UTC"),
        entry_fill=entry_fill,
        exit_fill=exit_fill,
        entry_fee_bps=scenario.taker_fee_bps,
        exit_fee_bps=scenario.taker_fee_bps,
        scenario=scenario,
        entry_side=entry_side,
        entry_has_slippage=True,
    )


@dataclass(frozen=True, slots=True)
class _TradeSeries:
    times_ns: np.ndarray
    prices: np.ndarray
    quantities: np.ndarray
    directions: np.ndarray


def _trade_series_by_asset(trades: pd.DataFrame, *, venue: str) -> dict[str, _TradeSeries]:
    result: dict[str, _TradeSeries] = {}
    selected = trades.loc[trades["venue"].eq(venue.casefold())]
    for asset, frame in selected.groupby("asset", sort=True):
        ordered = frame.sort_values(
            ["received_time", "_ordinal"], kind="mergesort"
        ).reset_index(drop=True)
        result[str(asset)] = _TradeSeries(
            times_ns=_nanoseconds(ordered["received_time"]),
            prices=ordered["price"].to_numpy(dtype=float),
            quantities=ordered["quantity"].to_numpy(dtype=float),
            directions=np.where(
                ordered["aggressor_side"].eq("buy"), 1, -1
            ).astype(np.int8),
        )
    return result


def _simulate_maker(
    event: Mapping[str, object],
    scenario: ExecutionAssumptions,
    *,
    interval: StrictInterval,
    bbo: _BboSeries | None,
    trades: _TradeSeries | None,
    l2: _L2Series | None,
    maximum_age_ns: int,
) -> dict[str, object]:
    result = _execution_base(event, scenario, "maker")
    signal_time = _timestamp_value(event["signal_time"], label="signal_time")
    target_time = _timestamp_value(event["target_time"], label="target_time")
    entry_due = signal_time + pd.Timedelta(milliseconds=scenario.latency_ms)
    if entry_due >= target_time:
        result["execution_status"] = "MISSED_LATENCY"
        return result
    entry_state = _first_bbo_at_or_after(
        bbo, entry_due, interval=interval, maximum_age_ns=maximum_age_ns
    )
    if entry_state is None:
        result["execution_status"] = "MISSED_ENTRY_BOOK"
        return result
    assert bbo is not None
    _entry_position, entry_row = entry_state
    entry_observed_time = pd.Timestamp(entry_row.received_time_ns, tz="UTC")
    if entry_observed_time >= target_time:
        result["execution_status"] = "MISSED_ENTRY_AFTER_HORIZON"
        return result
    direction = _integer_value(event["signal_direction"], label="signal_direction")
    entry_side: Literal["buy", "sell"] = "buy" if direction > 0 else "sell"
    maker_price = entry_row.bid_price if direction > 0 else entry_row.ask_price
    displayed_quantity = (
        entry_row.bid_quantity if direction > 0 else entry_row.ask_quantity
    )
    requested_base = scenario.notional_usd / maker_price
    queue_ahead = displayed_quantity * scenario.queue_ahead_multiplier
    deadline = min(
        target_time,
        entry_due + pd.Timedelta(milliseconds=scenario.maker_timeout_ms),
    )
    if trades is None:
        result["execution_status"] = "MISSED_NO_PUBLIC_TRADE"
        result["entry_time"] = entry_observed_time
        return result
    left = int(
        np.searchsorted(trades.times_ns, int(entry_observed_time.value), side="right")
    )
    right = int(np.searchsorted(trades.times_ns, int(deadline.value), side="right"))
    remaining = requested_base
    filled = 0.0
    fill_time: pd.Timestamp | None = None
    eligible_found = False
    required_trade_direction = -1 if direction > 0 else 1
    for position in range(left, right):
        trade_price = float(trades.prices[position])
        if int(trades.directions[position]) != required_trade_direction:
            continue
        if direction > 0 and trade_price > maker_price:
            continue
        if direction < 0 and trade_price < maker_price:
            continue
        eligible_found = True
        quantity = float(trades.quantities[position])
        if queue_ahead > 0.0:
            consumed = min(queue_ahead, quantity)
            queue_ahead -= consumed
            quantity -= consumed
        if quantity <= 0.0:
            continue
        executed = min(remaining, quantity)
        filled += executed
        remaining -= executed
        fill_time = pd.Timestamp(int(trades.times_ns[position]), tz="UTC")
        if remaining <= 1e-15:
            break
    if filled <= 0.0:
        result["execution_status"] = (
            "MISSED_QUEUE" if eligible_found else "MISSED_NO_PUBLIC_TRADE"
        )
        result["entry_time"] = entry_observed_time
        return result

    entry_fill = _Fill(
        average_price=maker_price,
        base_quantity=filled,
        fraction=min(1.0, filled / requested_base),
    )
    exit_due = target_time + pd.Timedelta(milliseconds=scenario.exit_latency_ms)
    exit_state = _first_bbo_at_or_after(
        bbo, exit_due, interval=interval, maximum_age_ns=maximum_age_ns
    )
    if exit_state is None:
        result.update(
            execution_status="UNRESOLVED_EXIT_BOOK",
            entry_time=fill_time,
            entry_price=maker_price,
            entry_fill_fraction=entry_fill.fraction,
            unclosed_exposure_fraction=entry_fill.fraction,
        )
        return result
    exit_position, exit_row = exit_state
    exit_side: Literal["buy", "sell"] = "sell" if direction > 0 else "buy"
    exit_snapshot = _fresh_l2_snapshot(l2, int(bbo.times_ns[exit_position]), maximum_age_ns)
    exit_fill = _book_fill(
        side=exit_side,
        requested_base_quantity=entry_fill.base_quantity,
        bbo_row=exit_row,
        snapshot=exit_snapshot,
        max_participation=scenario.max_participation,
    )
    if exit_fill.fraction <= 0.0:
        result.update(
            execution_status="UNRESOLVED_EXIT_LIQUIDITY",
            entry_time=fill_time,
            entry_price=maker_price,
            entry_fill_fraction=entry_fill.fraction,
            unclosed_exposure_fraction=entry_fill.fraction,
        )
        return result
    assert fill_time is not None
    return _complete_execution(
        result,
        direction=direction,
        baseline_mid=_float_value(event["baseline_mid"], label="baseline_mid"),
        entry_reference_mid=entry_row.mid,
        exit_reference_mid=exit_row.mid,
        entry_time=fill_time,
        exit_time=pd.Timestamp(exit_row.received_time_ns, tz="UTC"),
        entry_fill=entry_fill,
        exit_fill=exit_fill,
        entry_fee_bps=scenario.maker_fee_bps,
        exit_fee_bps=scenario.taker_fee_bps,
        scenario=scenario,
        entry_side=entry_side,
        entry_has_slippage=False,
    )


def _execution_rows(
    information_events: pd.DataFrame,
    bbo: pd.DataFrame,
    trades: pd.DataFrame,
    l2: pd.DataFrame,
    intervals: Sequence[StrictInterval],
    config: LeadLagConfig,
) -> pd.DataFrame:
    if information_events.empty:
        return pd.DataFrame()
    causal_candidates = information_events.loc[
        information_events["interval_id"].notna()
        & information_events["target_time"].lt(information_events["interval_end"])
    ]
    if causal_candidates.empty:
        return pd.DataFrame()
    bbo_series = _bbo_series_by_asset(bbo, venue=config.execution_venue)
    l2_series = _l2_series_by_asset(l2, venue=config.execution_venue)
    hl_trades = _trade_series_by_asset(trades, venue=config.execution_venue)
    interval_by_id = {_interval_id(interval): interval for interval in intervals}
    maximum_age_ns = config.max_book_age_ms * 1_000_000
    rows: list[dict[str, object]] = []
    event_records = cast(
        list[dict[str, object]], causal_candidates.to_dict(orient="records")
    )
    for event in event_records:
        interval = interval_by_id[str(event["interval_id"])]
        asset = str(event["asset"])
        asset_trades = hl_trades.get(asset)
        for scenario in config.execution_scenarios:
            rows.append(
                _simulate_taker(
                    event,
                    scenario,
                    interval=interval,
                    bbo=bbo_series.get(asset),
                    l2=l2_series.get(asset),
                    maximum_age_ns=maximum_age_ns,
                )
            )
            rows.append(
                _simulate_maker(
                    event,
                    scenario,
                    interval=interval,
                    bbo=bbo_series.get(asset),
                    trades=asset_trades,
                    l2=l2_series.get(asset),
                    maximum_age_ns=maximum_age_ns,
                )
            )
    return pd.DataFrame(rows)


def _execution_metrics(events: pd.DataFrame, config: LeadLagConfig) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    empty = events.iloc[0:0]
    grouped = (
        {
            (
                str(scenario),
                str(model),
                str(asset),
                str(family),
                _integer_value(horizon, label="horizon_ms"),
            ): frame
            for (scenario, model, asset, family, horizon), frame in events.groupby(
                [
                    "execution_scenario",
                    "execution_model",
                    "asset",
                    "signal_family",
                    "horizon_ms",
                ],
                sort=False,
            )
        }
        if not events.empty
        else {}
    )
    for scenario in config.execution_scenarios:
        for model in ("taker", "maker"):
            for asset in config.assets:
                for family in SIGNAL_FAMILIES:
                    for horizon_ms in config.horizons_ms:
                        selected = grouped.get(
                            (scenario.name, model, asset, family, horizon_ms), empty
                        )
                        status = selected.get("execution_status", pd.Series(dtype=str))
                        completed = selected.loc[status.isin(["FILLED", "PARTIAL"])]
                        net = pd.to_numeric(
                            completed.get("net_execution_bps", pd.Series(dtype=float)),
                            errors="coerce",
                        ).dropna()
                        gross = pd.to_numeric(
                            completed.get("gross_execution_bps", pd.Series(dtype=float)),
                            errors="coerce",
                        ).dropna()
                        break_even = pd.to_numeric(
                            completed.get("break_even_move_bps", pd.Series(dtype=float)),
                            errors="coerce",
                        ).dropna()
                        adjusted_net = pd.to_numeric(
                            completed.get(
                                "fill_adjusted_net_bps", pd.Series(dtype=float)
                            ),
                            errors="coerce",
                        ).dropna()
                        adjusted_gross = pd.to_numeric(
                            completed.get(
                                "fill_adjusted_gross_bps", pd.Series(dtype=float)
                            ),
                            errors="coerce",
                        ).dropna()
                        before_funding = pd.to_numeric(
                            completed.get(
                                "before_funding_execution_bps",
                                pd.Series(dtype=float),
                            ),
                            errors="coerce",
                        ).dropna()
                        entry_fees = pd.to_numeric(
                            completed.get(
                                "entry_fee_bps_applied", pd.Series(dtype=float)
                            ),
                            errors="coerce",
                        ).dropna()
                        exit_fees = pd.to_numeric(
                            completed.get(
                                "exit_fee_bps_applied", pd.Series(dtype=float)
                            ),
                            errors="coerce",
                        ).dropna()
                        entry_spread = pd.to_numeric(
                            completed.get(
                                "entry_spread_cost_bps", pd.Series(dtype=float)
                            ),
                            errors="coerce",
                        ).dropna()
                        exit_spread = pd.to_numeric(
                            completed.get(
                                "exit_spread_cost_bps", pd.Series(dtype=float)
                            ),
                            errors="coerce",
                        ).dropna()
                        entry_slippage = pd.to_numeric(
                            completed.get(
                                "entry_slippage_cost_bps", pd.Series(dtype=float)
                            ),
                            errors="coerce",
                        ).dropna()
                        exit_slippage = pd.to_numeric(
                            completed.get(
                                "exit_slippage_cost_bps", pd.Series(dtype=float)
                            ),
                            errors="coerce",
                        ).dropna()
                        adverse_exit = pd.to_numeric(
                            completed.get(
                                "adverse_exit_cost_bps", pd.Series(dtype=float)
                            ),
                            errors="coerce",
                        ).dropna()
                        unresolved = status.str.startswith("UNRESOLVED_")
                        residual = pd.to_numeric(
                            selected.get(
                                "unclosed_exposure_fraction", pd.Series(dtype=float)
                            ),
                            errors="coerce",
                        ).fillna(0.0)
                        residual_count = int(residual.gt(1e-15).sum())
                        matched = pd.to_numeric(
                            selected.get(
                                "matched_fill_fraction", pd.Series(dtype=float)
                            ),
                            errors="coerce",
                        ).fillna(0.0)
                        attempt_weighted_net = (
                            math.nan
                            if bool(unresolved.any()) or residual_count > 0
                            else (
                                float(adjusted_net.sum()) / len(selected)
                                if len(selected)
                                else math.nan
                            )
                        )
                        economically_unresolved = unresolved | residual.gt(1e-15)
                        rows.append(
                            {
                                "analysis_kind": "execution",
                                "asset": asset,
                                "signal_family": family,
                                "horizon_ms": horizon_ms,
                                "execution_scenario": scenario.name,
                                "execution_model": model,
                                "signal_count": len(selected),
                                "evaluable_count": len(completed),
                                "excluded_count": len(selected) - len(completed),
                                "same_direction_count": int((net > 0.0).sum()),
                                "neutral_count": int((net == 0.0).sum()),
                                "adverse_count": int((net < 0.0).sum()),
                                "same_direction_rate": float((net > 0.0).mean())
                                if len(net)
                                else math.nan,
                                "false_positive_rate": float((net <= 0.0).mean())
                                if len(net)
                                else math.nan,
                                "adverse_rate": float((net < 0.0).mean())
                                if len(net)
                                else math.nan,
                                "expected_move_bps": _safe_mean(net),
                                "median_move_bps": _safe_quantile(net, 0.5),
                                "q10_move_bps": _safe_quantile(net, 0.1),
                                "q90_move_bps": _safe_quantile(net, 0.9),
                                "negative_lag_expected_move_bps": math.nan,
                                "minimum_events_met": len(completed) >= config.minimum_events,
                                "inference_status": "NOT_APPLICABLE_EXECUTION_SCENARIO",
                                "empirical_p_value": math.nan,
                                "fwer_p_value": math.nan,
                                "fdr_q_value": math.nan,
                                "source_time_status": SOURCE_TIME_STATUS,
                                "attempt_count": len(selected),
                                "filled_count": int(status.eq("FILLED").sum()),
                                "partial_count": int(status.eq("PARTIAL").sum()),
                                "missed_or_unresolved_count": int(
                                    (~status.isin(["FILLED", "PARTIAL"])).sum()
                                ),
                                "unresolved_count": int(unresolved.sum()),
                                "economically_unresolved_count": int(
                                    economically_unresolved.sum()
                                ),
                                "residual_exposure_count": residual_count,
                                "residual_exposure_rate": residual_count / len(selected)
                                if len(selected)
                                else math.nan,
                                "any_fill_rate": len(completed) / len(selected)
                                if len(selected)
                                else math.nan,
                                "fill_rate": float(matched.mean())
                                if len(selected)
                                else math.nan,
                                "mean_matched_fill_fraction": float(matched.mean())
                                if len(selected)
                                else math.nan,
                                "gross_expected_move_bps": _safe_mean(gross),
                                "fill_adjusted_expected_gross_bps": _safe_mean(
                                    adjusted_gross
                                ),
                                "fill_adjusted_expected_net_bps": _safe_mean(
                                    adjusted_net
                                ),
                                "attempt_weighted_expected_net_bps": attempt_weighted_net,
                                "minimum_profitable_move_bps": _safe_mean(break_even),
                                "minimum_profitable_move_scope": "BEFORE_FUNDING",
                                "expected_before_funding_execution_bps": _safe_mean(
                                    before_funding
                                ),
                                "expected_entry_fee_bps": _safe_mean(entry_fees),
                                "expected_exit_fee_bps": _safe_mean(exit_fees),
                                "expected_entry_spread_cost_bps": _safe_mean(
                                    entry_spread
                                ),
                                "expected_exit_spread_cost_bps": _safe_mean(
                                    exit_spread
                                ),
                                "expected_entry_slippage_cost_bps": _safe_mean(
                                    entry_slippage
                                ),
                                "expected_exit_slippage_cost_bps": _safe_mean(
                                    exit_slippage
                                ),
                                "expected_adverse_exit_cost_bps": _safe_mean(
                                    adverse_exit
                                ),
                                "calibration_status": scenario.calibration_status,
                                "economic_claim": "NOT_CLAIMED",
                                "economic_scope": "BEFORE_FUNDING",
                                "funding_status": "NOT_EVALUATED",
                                "economic_admissibility": (
                                    "NOT_ADMISSIBLE_FUNDING_NOT_EVALUATED"
                                ),
                            }
                        )
    return pd.DataFrame(rows)


def _execution_bucket_metrics(
    events: pd.DataFrame,
    intervals: Sequence[StrictInterval],
    config: LeadLagConfig,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for bucket in _bucket_starts(intervals, config.bucket_minutes):
        selected = (
            events
            if events.empty
            else events.loc[events["time_bucket"].eq(bucket)]
        )
        metrics = _execution_metrics(selected, config)
        metrics["time_bucket"] = bucket
        rows.append(metrics)
    return pd.concat(rows, ignore_index=True, sort=False) if rows else pd.DataFrame()


def _add_decay_fields(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for name in (
        "earliest_admissible_horizon_ms",
        "expected_move_at_earliest_horizon_bps",
        "expected_move_decay_from_earliest_bps",
        "expected_move_retention_vs_earliest",
        "peak_absolute_expected_move_bps",
        "absolute_retention_vs_peak",
    ):
        result[name] = math.nan
    if result.empty:
        return result
    group_columns = [
        name
        for name in (
            "time_bucket",
            "analysis_kind",
            "asset",
            "signal_family",
            "execution_scenario",
            "execution_model",
        )
        if name in result.columns
    ]
    for _key, positions in result.groupby(group_columns, dropna=False, sort=False).groups.items():
        ordered = result.loc[list(positions)].sort_values("horizon_ms", kind="mergesort")
        expected = pd.to_numeric(ordered["expected_move_bps"], errors="coerce")
        counts = pd.to_numeric(ordered["evaluable_count"], errors="coerce").fillna(0)
        admissible = expected.notna() & counts.gt(0)
        if not bool(admissible.any()):
            continue
        earliest_index = admissible[admissible].index[0]
        earliest_horizon = _integer_value(
            result.at[earliest_index, "horizon_ms"], label="horizon_ms"
        )
        earliest_value = _float_value(
            result.at[earliest_index, "expected_move_bps"],
            label="expected_move_bps",
        )
        peak = float(expected.loc[admissible].abs().max())
        for index in ordered.index:
            value = _float_value(
                result.at[index, "expected_move_bps"], label="expected_move_bps"
            )
            if not math.isfinite(value):
                continue
            result.at[index, "earliest_admissible_horizon_ms"] = earliest_horizon
            result.at[index, "expected_move_at_earliest_horizon_bps"] = earliest_value
            result.at[index, "expected_move_decay_from_earliest_bps"] = (
                value - earliest_value
            )
            if abs(earliest_value) > 1e-15:
                result.at[index, "expected_move_retention_vs_earliest"] = (
                    value / earliest_value
                )
            result.at[index, "peak_absolute_expected_move_bps"] = peak
            if peak > 1e-15:
                result.at[index, "absolute_retention_vs_peak"] = abs(value) / peak
    return result


def _sort_frame(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    if frame.empty:
        return frame.reset_index(drop=True)
    available = [column for column in columns if column in frame.columns]
    return frame.sort_values(available, kind="mergesort").reset_index(drop=True)


def _clock_diagnostics(clock_sync: pd.DataFrame) -> dict[str, object]:
    diagnostics: dict[str, object] = {
        "row_count": len(clock_sync),
        "usage": "DIAGNOSTIC_ONLY_STRICT_INTERVALS_DEFINE_CAUSAL_VALIDITY",
    }
    if clock_sync.empty:
        diagnostics["received_time_min"] = None
        diagnostics["received_time_max"] = None
        return diagnostics
    if "received_time" in clock_sync:
        received = _timestamp_series(
            clock_sync["received_time"], label="clock_sync.received_time"
        )
        diagnostics["received_time_min"] = received.min().isoformat().replace(
            "+00:00", "Z"
        )
        diagnostics["received_time_max"] = received.max().isoformat().replace(
            "+00:00", "Z"
        )
    else:
        diagnostics["received_time_min"] = None
        diagnostics["received_time_max"] = None
    for candidate in ("valid", "clock_valid", "is_valid"):
        if candidate in clock_sync:
            values = clock_sync[candidate].astype(bool)
            diagnostics["reported_valid_row_count"] = int(values.sum())
            diagnostics["reported_invalid_row_count"] = int((~values).sum())
            break
    return diagnostics


def _required_venue_assets(
    bbo: pd.DataFrame,
    *,
    venue: str,
    assets: Sequence[str],
) -> tuple[str, ...]:
    available = set(
        bbo.loc[bbo["venue"].eq(venue.casefold()), "asset"].astype(str).tolist()
    )
    return tuple(asset for asset in assets if asset not in available)


def _chunk_timestamp(value: datetime | pd.Timestamp, *, label: str) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tz is None:
        raise ValueError(f"{label} must be timezone-aware")
    return timestamp.tz_convert("UTC")


def _chunk_halo_bounds(
    *,
    interval: StrictInterval,
    core_start: pd.Timestamp,
    core_end: pd.Timestamp,
    config: LeadLagConfig,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    left_halo_ms = max(
        config.momentum_window_ms + config.max_book_age_ms,
        config.trade_window_ms,
        max(config.horizons_ms) + config.max_book_age_ms,
    )
    right_halo_ms = (
        max(config.horizons_ms)
        + max(scenario.exit_latency_ms for scenario in config.execution_scenarios)
        + config.max_book_age_ms
    )
    interval_start = pd.Timestamp(interval.start)
    interval_end = pd.Timestamp(interval.end)
    return (
        max(interval_start, core_start - pd.Timedelta(milliseconds=left_halo_ms)),
        min(interval_end, core_end + pd.Timedelta(milliseconds=right_halo_ms)),
    )


def _slice_chunk_frame(
    frame: pd.DataFrame,
    *,
    label: str,
    asset: str,
    halo_start: pd.Timestamp,
    halo_end: pd.Timestamp,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    _require_columns(frame, {"asset", "received_time"}, label=label)
    received = _timestamp_series(frame["received_time"], label=f"{label}.received_time")
    assets = frame["asset"].astype(str).str.strip().str.upper()
    selected = assets.eq(asset) & received.ge(halo_start) & received.lt(halo_end)
    result = frame.loc[selected].copy()
    result["received_time"] = received.loc[selected]
    return result.reset_index(drop=True)


def _peak_simultaneous_source_rows(frames: Sequence[pd.DataFrame]) -> int:
    received = [
        frame["received_time"]
        for frame in frames
        if not frame.empty and "received_time" in frame
    ]
    if not received:
        return 0
    counts = pd.concat(received, ignore_index=True).value_counts(sort=False)
    return int(counts.max()) if len(counts) else 0


def _peak_l2_frame_levels(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    exploded_required = {"venue", "asset", "received_time", "side", "price", "quantity"}
    atomic_required = {"venue", "asset", "received_time", "bids", "asks"}
    if atomic_required.issubset(frame.columns) and not exploded_required.issubset(
        frame.columns
    ):
        maximum = 0
        for bids, asks in frame[["bids", "asks"]].itertuples(index=False, name=None):
            if not isinstance(bids, (list, tuple)):
                raise ValueError("l2.bids must contain an atomic level sequence")
            if not isinstance(asks, (list, tuple)):
                raise ValueError("l2.asks must contain an atomic level sequence")
            maximum = max(maximum, len(bids) + len(asks))
        return maximum
    _require_columns(frame, exploded_required, label="l2")
    keys = ["venue", "asset", "received_time"]
    if "snapshot_id" in frame:
        keys.append("snapshot_id")
    sizes = frame.groupby(keys, sort=False, dropna=False).size()
    return int(sizes.max()) if len(sizes) else 0


def _l2_level_count(frame: pd.DataFrame) -> int:
    """Count the exact level rows that ``_prepare_l2`` would materialize."""

    if frame.empty:
        return 0
    exploded_required = {"venue", "asset", "received_time", "side", "price", "quantity"}
    atomic_required = {"venue", "asset", "received_time", "bids", "asks"}
    if atomic_required.issubset(frame.columns) and not exploded_required.issubset(
        frame.columns
    ):
        total = 0
        for bids, asks in frame[["bids", "asks"]].itertuples(index=False, name=None):
            if not isinstance(bids, (list, tuple)):
                raise ValueError("l2.bids must contain an atomic level sequence")
            if not isinstance(asks, (list, tuple)):
                raise ValueError("l2.asks must contain an atomic level sequence")
            total += len(bids) + len(asks)
        return total
    _require_columns(frame, exploded_required, label="l2")
    return len(frame)


def _enforce_chunk_bound(
    observed: int,
    limit: int,
    *,
    observed_name: str,
    limit_name: str,
) -> None:
    if observed > limit:
        raise LeadLagChunkCapacityError(
            "lead-lag bounded chunk refused materialization: "
            f"{observed_name}={observed} exceeds {limit_name}={limit}"
        )


def _signals_in_core(
    signals: pd.DataFrame,
    *,
    core_start: pd.Timestamp,
    core_end: pd.Timestamp,
) -> pd.DataFrame:
    if signals.empty:
        return signals.reset_index(drop=True)
    selected = signals["signal_time"].ge(core_start) & signals["signal_time"].lt(
        core_end
    )
    return signals.loc[selected].reset_index(drop=True)


def _response_output_rows(
    events: pd.DataFrame, *, row_kind: Literal["information", "control"]
) -> pd.DataFrame:
    result = events.copy()
    result["row_kind"] = row_kind
    result["execution_scenario"] = None
    result["execution_model"] = None
    result["execution_calibration_status"] = None
    result["execution_status"] = "NOT_APPLICABLE"
    return _sort_frame(
        result,
        (
            "signal_time",
            "asset",
            "signal_family",
            "horizon_ms",
            "signal_id",
            "row_kind",
            "execution_scenario",
            "execution_model",
        ),
    )


def analyze_lead_lag_chunk(
    dataset: LeadLagDataset,
    interval: StrictInterval,
    config: LeadLagConfig,
    *,
    asset: str,
    core_start: datetime | pd.Timestamp,
    core_end: datetime | pd.Timestamp,
    limits: LeadLagChunkLimits | None = None,
) -> LeadLagChunkResult:
    """Build oracle-equivalent event rows for one bounded asset/interval core.

    The supplied dataset may contain more than the requested core. Only the
    deterministic causal halo, clipped to ``interval``, is prepared. Halo
    signals are feature state only and are never emitted.
    """

    if not isinstance(dataset, LeadLagDataset):
        raise TypeError("dataset must be a LeadLagDataset")
    if not isinstance(interval, StrictInterval):
        raise TypeError("interval must be a StrictInterval")
    if not isinstance(config, LeadLagConfig):
        raise TypeError("config must be a LeadLagConfig")
    active_limits = LeadLagChunkLimits.from_config(config) if limits is None else limits
    if not isinstance(active_limits, LeadLagChunkLimits):
        raise TypeError("limits must be a LeadLagChunkLimits")

    normalized_asset = asset.strip().upper()
    if normalized_asset not in config.assets:
        raise ValueError("chunk asset must be one of the configured assets")
    start = _chunk_timestamp(core_start, label="chunk core_start")
    end = _chunk_timestamp(core_end, label="chunk core_end")
    interval_start = pd.Timestamp(interval.start)
    interval_end = pd.Timestamp(interval.end)
    if not interval_start <= start < end <= interval_end:
        raise ValueError(
            "chunk core must be a non-empty half-open window inside one strict interval"
        )
    halo_start, halo_end = _chunk_halo_bounds(
        interval=interval,
        core_start=start,
        core_end=end,
        config=config,
    )

    raw_bbo = _slice_chunk_frame(
        dataset.bbo,
        label="bbo",
        asset=normalized_asset,
        halo_start=halo_start,
        halo_end=halo_end,
    )
    raw_trades = _slice_chunk_frame(
        dataset.trades,
        label="trades",
        asset=normalized_asset,
        halo_start=halo_start,
        halo_end=halo_end,
    )
    raw_l2 = _slice_chunk_frame(
        dataset.l2,
        label="l2",
        asset=normalized_asset,
        halo_start=halo_start,
        halo_end=halo_end,
    )
    source_row_count = len(raw_bbo) + len(raw_trades) + len(raw_l2)
    _enforce_chunk_bound(
        source_row_count,
        active_limits.max_source_rows_per_chunk,
        observed_name="source_rows",
        limit_name="max_source_rows_per_chunk",
    )
    peak_simultaneous = _peak_simultaneous_source_rows(
        (raw_bbo, raw_trades, raw_l2)
    )
    _enforce_chunk_bound(
        peak_simultaneous,
        active_limits.max_simultaneous_batch_rows,
        observed_name="simultaneous_batch_rows",
        limit_name="max_simultaneous_batch_rows",
    )
    peak_l2_levels = _peak_l2_frame_levels(raw_l2)
    _enforce_chunk_bound(
        peak_l2_levels,
        active_limits.max_l2_frame_levels,
        observed_name="l2_frame_levels",
        limit_name="max_l2_frame_levels",
    )
    l2_level_count = _l2_level_count(raw_l2)
    _enforce_chunk_bound(
        l2_level_count,
        active_limits.max_l2_levels_per_chunk,
        observed_name="l2_levels_per_chunk",
        limit_name="max_l2_levels_per_chunk",
    )

    bbo = _prepare_bbo(raw_bbo)
    trades = _prepare_trades(raw_trades)
    l2 = _prepare_l2(raw_l2)
    missing_reference = _required_venue_assets(
        bbo, venue=config.reference_venue, assets=(normalized_asset,)
    )
    missing_execution = _required_venue_assets(
        bbo, venue=config.execution_venue, assets=(normalized_asset,)
    )
    if missing_reference or missing_execution:
        missing = [
            venue
            for venue, values in (
                (config.reference_venue, missing_reference),
                (config.execution_venue, missing_execution),
            )
            if values
        ]
        raise ValueError(
            "missing required BBO venue/assets for chunk: " + ", ".join(missing)
        )

    intervals = (interval,)
    primary_signals = _signals_in_core(
        _build_signals(
            bbo,
            trades,
            l2,
            venue=config.reference_venue,
            assets=(normalized_asset,),
            config=config,
            intervals=intervals,
        ),
        core_start=start,
        core_end=end,
    )
    reverse_signals = _signals_in_core(
        _build_signals(
            bbo,
            trades,
            l2,
            venue=config.execution_venue,
            assets=(normalized_asset,),
            config=config,
            intervals=intervals,
        ),
        core_start=start,
        core_end=end,
    )
    projected_response_states = (
        len(primary_signals) + len(reverse_signals)
    ) * len(config.horizons_ms)
    projected_execution_states = (
        len(primary_signals)
        * len(config.horizons_ms)
        * len(config.execution_scenarios)
        * 2
    )
    _enforce_chunk_bound(
        projected_response_states,
        active_limits.max_pending_response_states,
        observed_name="projected_response_states",
        limit_name="max_pending_response_states",
    )
    _enforce_chunk_bound(
        projected_execution_states,
        active_limits.max_pending_execution_states,
        observed_name="projected_execution_states",
        limit_name="max_pending_execution_states",
    )

    information = _response_rows(
        primary_signals,
        bbo,
        intervals,
        config,
        response_venue=config.execution_venue,
        signal_role="primary",
    )
    reverse = _response_rows(
        reverse_signals,
        bbo,
        intervals,
        config,
        response_venue=config.reference_venue,
        signal_role="reverse",
    )
    execution = _execution_rows(
        information,
        bbo,
        trades,
        l2,
        intervals,
        config,
    )
    execution = _sort_frame(
        execution,
        (
            "signal_time",
            "asset",
            "signal_family",
            "horizon_ms",
            "signal_id",
            "row_kind",
            "execution_scenario",
            "execution_model",
        ),
    )
    return LeadLagChunkResult(
        resource_model_version=config.streaming_resource_model_version,
        asset=normalized_asset,
        interval_id=_interval_id(interval),
        core_start=start,
        core_end=end,
        halo_start=halo_start,
        halo_end=halo_end,
        source_row_count=source_row_count,
        peak_simultaneous_batch_rows=peak_simultaneous,
        peak_l2_frame_levels=peak_l2_levels,
        projected_response_states=projected_response_states,
        projected_execution_states=projected_execution_states,
        primary_signal_count=len(primary_signals),
        reverse_signal_count=len(reverse_signals),
        information_events=_response_output_rows(
            information, row_kind="information"
        ),
        reverse_events=_response_output_rows(reverse, row_kind="control"),
        execution_events=execution,
    )


def analyze_lead_lag(
    dataset: LeadLagDataset,
    intervals: Sequence[StrictInterval],
    config: LeadLagConfig,
) -> LeadLagAnalysis:
    """Run a deterministic, received-time-only cross-venue event study.

    ``intervals`` must be the unchanged strict intervals emitted by the
    independent technical capture gate.  Clock rows are retained as diagnostics;
    they are not used to manufacture source-time ordering.
    """

    if not isinstance(dataset, LeadLagDataset):
        raise TypeError("dataset must be a LeadLagDataset")
    if not isinstance(config, LeadLagConfig):
        raise TypeError("config must be a LeadLagConfig")
    strict_intervals = _validated_intervals(intervals)
    bbo = _prepare_bbo(dataset.bbo)
    trades = _prepare_trades(dataset.trades)
    l2 = _prepare_l2(dataset.l2)

    missing_reference = _required_venue_assets(
        bbo, venue=config.reference_venue, assets=config.assets
    )
    missing_execution = _required_venue_assets(
        bbo, venue=config.execution_venue, assets=config.assets
    )
    if missing_reference or missing_execution:
        details: list[str] = []
        if missing_reference:
            details.append(
                f"{config.reference_venue}: {', '.join(missing_reference)}"
            )
        if missing_execution:
            details.append(
                f"{config.execution_venue}: {', '.join(missing_execution)}"
            )
        raise ValueError("missing required BBO venue/assets: " + "; ".join(details))

    primary_signals = _build_signals(
        bbo,
        trades,
        l2,
        venue=config.reference_venue,
        assets=config.assets,
        config=config,
        intervals=strict_intervals,
    )
    reverse_signals = _build_signals(
        bbo,
        trades,
        l2,
        venue=config.execution_venue,
        assets=config.assets,
        config=config,
        intervals=strict_intervals,
    )
    capacity_estimate = _estimate_event_capacity(
        primary_signals,
        reverse_signals,
        strict_intervals,
        config,
    )
    _enforce_event_capacity(capacity_estimate, config)
    information_events = _response_rows(
        primary_signals,
        bbo,
        strict_intervals,
        config,
        response_venue=config.execution_venue,
        signal_role="primary",
    )
    reverse_events = _response_rows(
        reverse_signals,
        bbo,
        strict_intervals,
        config,
        response_venue=config.reference_venue,
        signal_role="reverse",
    )

    information_metrics = _information_metrics(information_events, config)
    information_metrics, controls = _randomized_controls(
        information_events,
        information_metrics,
        reverse_events,
        config,
    )
    information_bucket_metrics = _bucket_metrics(
        information_events, strict_intervals, config
    )
    execution_events = _execution_rows(
        information_events,
        bbo,
        trades,
        l2,
        strict_intervals,
        config,
    )
    execution_metrics = _execution_metrics(execution_events, config)
    execution_bucket_metrics = _execution_bucket_metrics(
        execution_events, strict_intervals, config
    )

    information_output = information_events.copy()
    information_output["row_kind"] = "information"
    information_output["execution_scenario"] = None
    information_output["execution_model"] = None
    information_output["execution_calibration_status"] = None
    information_output["execution_status"] = "NOT_APPLICABLE"
    reverse_output = reverse_events.copy()
    reverse_output["row_kind"] = "control"
    reverse_output["execution_scenario"] = None
    reverse_output["execution_model"] = None
    reverse_output["execution_calibration_status"] = None
    reverse_output["execution_status"] = "NOT_APPLICABLE"
    events = pd.concat(
        [information_output, reverse_output, execution_events],
        ignore_index=True,
        sort=False,
    )
    events = _sort_frame(
        events,
        (
            "signal_time",
            "asset",
            "signal_family",
            "horizon_ms",
            "signal_id",
            "row_kind",
            "execution_scenario",
            "execution_model",
        ),
    )
    metrics = _add_decay_fields(
        _sort_frame(
            pd.concat(
                [information_metrics, execution_metrics], ignore_index=True, sort=False
            ),
            (
                "analysis_kind",
                "asset",
                "signal_family",
                "horizon_ms",
                "execution_scenario",
                "execution_model",
            ),
        )
    )
    bucket_metrics = _add_decay_fields(
        _sort_frame(
            pd.concat(
                [information_bucket_metrics, execution_bucket_metrics],
                ignore_index=True,
                sort=False,
            ),
            (
                "time_bucket",
                "analysis_kind",
                "asset",
                "signal_family",
                "horizon_ms",
                "execution_scenario",
                "execution_model",
            ),
        )
    )
    controls = _sort_frame(
        controls,
        ("control_type", "asset", "signal_family", "horizon_ms"),
    )

    evaluable_count = (
        int(information_events["evaluable"].eq(True).sum())
        if not information_events.empty
        else 0
    )
    exclusion_counts = (
        {
            str(reason): int(count)
            for reason, count in information_events.loc[
                information_events["evaluable"].eq(False), "exclusion_reason"
            ]
            .fillna("unspecified")
            .value_counts()
            .sort_index()
            .items()
        }
        if not information_events.empty
        else {}
    )
    synthetic_warning = "SYNTHETIC_DETECTOR_VALIDATION_ONLY"
    provenance_kind = str(dataset.provenance.get("kind", "UNKNOWN")).upper()
    is_synthetic = provenance_kind == "SYNTHETIC" or synthetic_warning in str(
        dataset.provenance
    )
    interval_duration_seconds = sum(
        (interval.end - interval.start).total_seconds()
        for interval in strict_intervals
    )
    calibration_statuses = sorted(
        {scenario.calibration_status for scenario in config.execution_scenarios}
    )
    warnings = [
        "Economic profitability is not claimed; execution results are scenario outputs.",
        "Execution economics are BEFORE_FUNDING; funding is NOT_EVALUATED.",
        "Source/exchange-time lead is not admissible without symmetric Hyperliquid clock calibration.",
        "The independently saved technical capture gate must PASS before real-data results are admissible.",
    ]
    if is_synthetic:
        warnings.insert(0, synthetic_warning)
    if any(status != "CALIBRATED" for status in calibration_statuses):
        warnings.append(
            "One or more execution scenarios are uncalibrated and cannot support an economic claim."
        )
    summary: dict[str, object] = {
        "research_status": synthetic_warning if is_synthetic else "EVENT_REPLAY_RESEARCH_ONLY",
        "economic_claim": "NOT_CLAIMED",
        "economic_scope": "BEFORE_FUNDING",
        "funding_status": "NOT_EVALUATED",
        "economic_admissibility": "NOT_ADMISSIBLE_FUNDING_NOT_EVALUATED",
        "technical_gate_requirement": "INDEPENDENT_TECHNICAL_CAPTURE_GATE_PASS_REQUIRED",
        "technical_gate_modified": False,
        "time_axis": "received_time",
        "equal_received_time_semantics": "simultaneous_batch_baseline_includes_ties",
        "source_time_lead_status": SOURCE_TIME_STATUS,
        "local_horizon_status": "ADMISSIBLE_ONLY_FOR_EVALUABLE_ROWS_WITHIN_ONE_STRICT_INTERVAL",
        "source_fingerprint": dataset.source_fingerprint,
        "config_sha256": config.config_hash,
        "assets": list(config.assets),
        "horizons_ms": list(config.horizons_ms),
        "signal_families": list(SIGNAL_FAMILIES),
        "strict_interval_count": len(strict_intervals),
        "strict_interval_duration_seconds": interval_duration_seconds,
        "primary_signal_count": len(primary_signals),
        "reverse_signal_count": len(reverse_signals),
        "capacity_preflight_status": "PASS",
        "estimated_event_rows_upper_bound": capacity_estimate.total_event_rows,
        "estimated_event_bytes_upper_bound": capacity_estimate.estimated_event_bytes,
        "max_event_rows": config.max_event_rows,
        "max_estimated_event_bytes": config.max_estimated_event_bytes,
        "capacity_preflight": capacity_estimate.as_dict(config),
        "reverse_control_event_row_count": len(reverse_events),
        "information_event_row_count": len(information_events),
        "evaluable_information_event_count": evaluable_count,
        "excluded_information_event_count": len(information_events) - evaluable_count,
        "exclusion_counts": exclusion_counts,
        "execution_event_row_count": len(execution_events),
        "persisted_event_row_count": len(events),
        "event_expansion_factor": len(events) / len(information_events)
        if len(information_events)
        else 0.0,
        "event_table_estimated_memory_bytes": int(
            events.memory_usage(index=True, deep=True).sum()
        ),
        "resource_model": "IN_MEMORY_LONG_FORM_EVENT_TABLE_SIZE_BEFORE_REAL_RUN",
        "all_configured_hypotheses_reported": True,
        "randomization_method": "deterministic_interval_block_sign_flip_max_t_and_bh_fdr",
        "randomization_resamples": config.randomization_resamples,
        "randomization_block_ms": config.randomization_block_ms,
        "execution_calibration_statuses": calibration_statuses,
        "false_positive_definition": (
            "information: endpoint neutral_or_adverse; execution: resolved matched-slice net_bps<=0"
        ),
        "clock_sync_diagnostics": _clock_diagnostics(dataset.clock_sync),
        "provenance": dict(dataset.provenance),
        "warnings": warnings,
    }
    return LeadLagAnalysis(
        summary=summary,
        metrics=metrics,
        bucket_metrics=bucket_metrics,
        events=events,
        controls=controls,
    )
