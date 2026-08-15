from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import sys
import time
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pyarrow

from hyperlab.analysis.lead_lag import SIGNAL_FAMILIES, LeadLagConfig, StrictInterval
from hyperlab.analysis.streaming_aggregates import aggregate_streaming_events
from hyperlab.analysis.streaming_kernel import (
    EventRow,
    StreamingKernelHighWater,
    run_streaming_kernel,
)
from hyperlab.analysis.streaming_store import (
    PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS,
    EventSpool,
    ExactTimestampNs,
)

_VENUES = ("binance_usdm", "hyperliquid")
_ASSETS = ("BTC", "ETH")
_KERNEL_RECORD_TYPES = ("bbo", "l2", "trade")
_HORIZONS_MS = (50, 100, 250, 500, 1_000, 2_000, 5_000)
_MANIFEST_EVIDENCE_COUNT = 60_001
_MANIFEST_EVIDENCE_TEST = (
    "tests/test_phase10_streaming_lake.py::"
    "test_lazy_catalog_shape_exceeds_sixty_thousand_files_without_a_file_list"
)
_BASE = datetime(2026, 8, 15, 0, 0, tzinfo=UTC)
_SOURCE_STEP_NS = 200_000_000
_SYNTHETIC_SOURCE_BATCH_ROWS = 64
_CONSERVATIVE_ROWS_PER_SIGNAL_BATCH = 420


@dataclass(frozen=True, slots=True)
class ProductionComponentStressResult:
    platform: str
    python_version: str
    pyarrow_version: str
    sampler: str
    manifest_catalog_evidence_test: str
    manifest_catalog_evidence_count: int
    requested_source_rows: int
    source_rows_scanned: int
    source_batches_processed: int
    output_event_row_count: int
    spooled_kernel_event_rows: int
    source_sha256: str
    event_sha256: str
    event_rows_by_kind: dict[str, int]
    observed_venues: tuple[str, ...]
    observed_assets: tuple[str, ...]
    observed_record_types: tuple[str, ...]
    observed_signal_families: tuple[str, ...]
    observed_horizons_ms: tuple[int, ...]
    observed_scenarios: tuple[str, ...]
    observed_models: tuple[str, ...]
    peak_source_batch_rows: int
    peak_sink_batch_rows: int
    peak_retained_source_rows: int
    peak_bbo_history_rows: int
    peak_public_trade_history_rows: int
    peak_trade_window_batches: int
    peak_l2_history_frames: int
    peak_retained_l2_levels: int
    peak_pending_response_states: int
    peak_pending_execution_states: int
    peak_completed_output_rows: int
    kernel_spool_peak_quantile_buffer_rows: int
    kernel_spool_max_uncommitted_rows: int
    kernel_parquet_rows: int
    kernel_parquet_bytes: int
    kernel_parquet_sha256: str
    kernel_parquet_logical_sha256: str
    exact_aggregate_scope: str
    integration_event_rows: int
    integration_metric_rows: int
    integration_bucket_rows: int
    integration_control_rows: int
    integration_exact_median: float
    integration_parquet_rows: int
    integration_parquet_bytes: int
    integration_parquet_sha256: str
    integration_logical_sha256: str
    integration_peak_quantile_buffer_rows: int
    scratch_peak_bytes: int
    measured_peak_rss_bytes: int | None
    elapsed_seconds_by_phase: dict[str, float]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)

    def deterministic_dict(self) -> dict[str, object]:
        payload = self.as_dict()
        payload.pop("platform")
        payload.pop("python_version")
        payload.pop("pyarrow_version")
        payload.pop("sampler")
        payload.pop("measured_peak_rss_bytes")
        payload.pop("elapsed_seconds_by_phase")
        return payload


LazyShapeStressResult = ProductionComponentStressResult


def _peak_rss() -> tuple[int | None, str]:
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.GetCurrentProcess.argtypes = []
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCounters),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        process = kernel32.GetCurrentProcess()
        ok = psapi.GetProcessMemoryInfo(
            process, ctypes.byref(counters), counters.cb
        )
        if not ok:
            return None, "Windows GetProcessMemoryInfo unavailable"
        return (
            int(counters.PeakWorkingSetSize),
            "Windows GetProcessMemoryInfo PeakWorkingSetSize",
        )
    try:
        import resource

        resource_api = cast(Any, resource)
        usage = resource_api.getrusage(resource_api.RUSAGE_SELF)
        peak = int(usage.ru_maxrss)
        multiplier = 1 if sys.platform == "darwin" else 1024
        return peak * multiplier, "resource.getrusage(RUSAGE_SELF).ru_maxrss"
    except (ImportError, OSError):
        return None, "platform peak-RSS sampler unavailable"


def _json_value(value: object) -> object:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, ExactTimestampNs):
        return pd.Timestamp(value.value, tz="UTC").isoformat().replace(
            "+00:00", "Z"
        )
    if isinstance(value, (pd.Timestamp, datetime)):
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            raise ValueError("stress timestamp must be timezone-aware")
        return timestamp.tz_convert("UTC").isoformat().replace("+00:00", "Z")
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        return [_json_value(item) for item in value]
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        return value
    item = getattr(value, "item", None)
    if callable(item):
        return _json_value(item())
    raise TypeError(f"unsupported stress value: {type(value).__name__}")


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            _json_value(value),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _directory_bytes(root: Path) -> int:
    total = 0
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            total += path.stat().st_size
        except FileNotFoundError:
            continue
    return total


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1_048_576):
            digest.update(block)
    return digest.hexdigest()


class _LazyKernelSource:
    """Lazily generate actual kernel batches without retaining the duration."""

    def __init__(self, *, requested_rows: int, minimum_output_events: int) -> None:
        per_asset_rows = math.ceil(requested_rows / len(_ASSETS))
        self.batch_count = max(
            2,
            math.ceil(per_asset_rows / _SYNTHETIC_SOURCE_BATCH_ROWS),
        )
        desired_signal_batches = max(
            2,
            math.ceil(
                minimum_output_events
                / (len(_ASSETS) * _CONSERVATIVE_ROWS_PER_SIGNAL_BATCH)
            ),
        )
        self.signal_period = max(1, self.batch_count // desired_signal_batches)
        self.rows_scanned = 0
        self.batches_processed = 0
        self.peak_batch_rows = 0
        self.observed_venues: set[str] = set()
        self.observed_assets: set[str] = set()
        self.observed_record_types: set[str] = set()
        self.source_hash = hashlib.sha256()
        self._consumed_assets: set[str] = set()

    @property
    def interval(self) -> StrictInterval:
        duration = timedelta(
            microseconds=(self.batch_count * _SOURCE_STEP_NS) // 1_000,
            milliseconds=20_000,
        )
        return StrictInterval(_BASE, _BASE + duration, "synthetic-component-stress")

    def _batch(
        self, asset: str, index: int
    ) -> tuple[int, tuple[tuple[str, Mapping[str, object]], ...]]:
        timestamp = int(pd.Timestamp(_BASE).value) + index * _SOURCE_STEP_NS
        received_time = ExactTimestampNs(timestamp)
        signal = (index + 1) % self.signal_period == 0
        direction = 1 if (index // self.signal_period) % 2 == 0 else -1
        base = 100_000.0 if asset == "BTC" else 4_000.0
        shift = direction * 0.75 if signal else 0.0
        rows: list[tuple[str, Mapping[str, object]]] = []
        for venue in _VENUES:
            bid_quantity = 3.0 if signal and direction > 0 else 2.0
            ask_quantity = 3.0 if signal and direction < 0 else 2.0
            rows.append(
                (
                    "bbo",
                    {
                        "venue": venue,
                        "asset": asset,
                        "received_time": received_time,
                        "bid_price": base + shift - 0.5,
                        "ask_price": base + shift + 0.5,
                        "bid_quantity": bid_quantity,
                        "ask_quantity": ask_quantity,
                    },
                )
            )
            l2_bid_quantity = 4.0 if signal and direction > 0 else 2.0
            l2_ask_quantity = 4.0 if signal and direction < 0 else 2.0
            rows.append(
                (
                    "l2",
                    {
                        "venue": venue,
                        "asset": asset,
                        "received_time": received_time,
                        "bids": (
                            (0, base - 0.5, l2_bid_quantity),
                            (1, base - 1.0, 1.0),
                        ),
                        "asks": (
                            (0, base + 0.5, l2_ask_quantity),
                            (1, base + 1.0, 1.0),
                        ),
                    },
                )
            )
            if signal:
                price = base + shift
                rows.append(
                    (
                        "trade",
                        {
                            "venue": venue,
                            "asset": asset,
                            "received_time": received_time,
                            "price": price,
                            "quantity": 0.01,
                            "quote_quantity": price * 0.01,
                            "aggressor_side": (
                                "buy" if direction > 0 else "sell"
                            ),
                        },
                    )
                )
        filler = 0
        while len(rows) < _SYNTHETIC_SOURCE_BATCH_ROWS:
            venue = _VENUES[filler % len(_VENUES)]
            rows.append(
                (
                    "bbo",
                    {
                        "venue": venue,
                        "asset": asset,
                        "received_time": received_time,
                        "bid_price": base + shift - 0.5,
                        "ask_price": base + shift + 0.5,
                        "bid_quantity": (
                            3.0 if signal and direction > 0 else 2.0
                        ),
                        "ask_quantity": (
                            3.0 if signal and direction < 0 else 2.0
                        ),
                        "update_id": (
                            f"stress-{asset}-{venue}-{index}-{filler}"
                        ),
                    },
                )
            )
            filler += 1
        return timestamp, tuple(rows)

    def iter_ordered_batches(
        self,
        *,
        asset: str,
        start_ns: int,
        end_ns: int,
        fetch_rows: int = 1_024,
    ) -> Iterator[tuple[int, tuple[tuple[str, Mapping[str, object]], ...]]]:
        del fetch_rows
        if asset in self._consumed_assets:
            raise AssertionError(f"stress source asset consumed twice: {asset}")
        if asset not in _ASSETS:
            raise AssertionError(f"unexpected stress asset: {asset}")
        self._consumed_assets.add(asset)
        for index in range(self.batch_count):
            timestamp, batch = self._batch(asset, index)
            if not start_ns <= timestamp < end_ns:
                continue
            self.batches_processed += 1
            self.rows_scanned += len(batch)
            self.peak_batch_rows = max(self.peak_batch_rows, len(batch))
            self.observed_assets.add(asset)
            for kind, row in batch:
                self.observed_venues.add(str(row["venue"]))
                self.observed_record_types.add(kind)
                self.source_hash.update(_canonical_bytes((timestamp, kind, row)))
            yield timestamp, batch


class _CountingHashSink:
    """Bounded sink that spools, counts, and hashes each real kernel row."""

    def __init__(self, maximum_batch_rows: int, *, spool: EventSpool) -> None:
        self.maximum_batch_rows = maximum_batch_rows
        self.spool = spool
        self.total_rows = 0
        self.peak_batch_rows = 0
        self.rows_by_kind: Counter[str] = Counter()
        self.families: set[str] = set()
        self.horizons: set[int] = set()
        self.scenarios: set[str] = set()
        self.models: set[str] = set()
        self.digest = hashlib.sha256()

    def __call__(self, rows: Sequence[EventRow]) -> None:
        if len(rows) > self.maximum_batch_rows:
            raise AssertionError("kernel sink batch exceeded writer_buffer_rows")
        self.spool.add_rows(rows)
        self.peak_batch_rows = max(self.peak_batch_rows, len(rows))
        for row in rows:
            self.total_rows += 1
            self.rows_by_kind[str(row.get("row_kind"))] += 1
            family = row.get("signal_family")
            if isinstance(family, str):
                self.families.add(family)
            horizon = row.get("horizon_ms")
            if isinstance(horizon, int) and not isinstance(horizon, bool):
                self.horizons.add(horizon)
            scenario = row.get("execution_scenario")
            if isinstance(scenario, str):
                self.scenarios.add(scenario)
            model = row.get("execution_model")
            if isinstance(model, str):
                self.models.add(model)
            self.digest.update(_canonical_bytes(row))


def _maximum_kernel_high_water(
    high_waters: Sequence[StreamingKernelHighWater],
) -> StreamingKernelHighWater:
    return StreamingKernelHighWater(
        batches_processed=sum(item.batches_processed for item in high_waters),
        rows_scanned=sum(item.rows_scanned for item in high_waters),
        simultaneous_batch_rows=max(item.simultaneous_batch_rows for item in high_waters),
        retained_source_rows=max(item.retained_source_rows for item in high_waters),
        bbo_history_rows=max(item.bbo_history_rows for item in high_waters),
        public_trade_history_rows=max(
            item.public_trade_history_rows for item in high_waters
        ),
        trade_window_batches=max(item.trade_window_batches for item in high_waters),
        l2_history_frames=max(item.l2_history_frames for item in high_waters),
        retained_l2_levels=max(item.retained_l2_levels for item in high_waters),
        pending_response_states=max(item.pending_response_states for item in high_waters),
        pending_execution_states=max(item.pending_execution_states for item in high_waters),
        completed_output_rows=max(item.completed_output_rows for item in high_waters),
    )


def _integration_event_rows(
    config: LeadLagConfig, interval: StrictInterval
) -> Iterator[dict[str, object]]:
    ordinal = 0
    for asset in config.assets:
        for family in SIGNAL_FAMILIES:
            for horizon_ms in config.horizons_ms:
                for sample in range(2):
                    timestamp = pd.Timestamp(interval.start) + pd.Timedelta(
                        milliseconds=ordinal + 1
                    )
                    ordinal += 1
                    direction = 1 if sample == 0 else -1
                    response = float(direction * (horizon_ms / 1_000.0 + 0.25))
                    signal_id = f"integration-{asset}-{family}-{horizon_ms}-{sample}"
                    common: dict[str, object] = {
                        "signal_id": signal_id,
                        "signal_venue": config.reference_venue,
                        "asset": asset,
                        "signal_family": family,
                        "signal_time": timestamp,
                        "signal_value": float(direction),
                        "signal_strength": 1.0,
                        "signal_direction": direction,
                        "signal_role": "primary",
                        "time_axis": "received_time",
                        "source_time_status": "NOT_ADMISSIBLE",
                        "horizon_ms": horizon_ms,
                        "target_time": timestamp
                        + pd.Timedelta(milliseconds=horizon_ms),
                        "time_bucket": pd.Timestamp(interval.start),
                        "interval_tag": interval.tag,
                        "interval_id": "integration-interval",
                        "interval_start": pd.Timestamp(interval.start),
                        "interval_end": pd.Timestamp(interval.end),
                        "evaluable": True,
                        "exclusion_reason": None,
                        "response_bps": response,
                        "negative_lag_response_bps": -response,
                        "first_move_delay_ms": float(horizon_ms / 2),
                        "first_move_direction": (
                            "same" if response > 0.0 else "opposite"
                        ),
                        "classification": (
                            "same_direction" if response > 0.0 else "adverse"
                        ),
                        "randomization_block": (
                            f"integration-interval|{sample:012d}"
                        ),
                    }
                    yield {
                        **common,
                        "row_kind": "information",
                        "execution_scenario": None,
                        "execution_model": None,
                        "execution_calibration_status": None,
                        "execution_status": "NOT_APPLICABLE",
                    }
                    yield {
                        **common,
                        "signal_id": f"{signal_id}-reverse",
                        "signal_venue": config.execution_venue,
                        "signal_role": "reverse",
                        "row_kind": "control",
                        "execution_scenario": None,
                        "execution_model": None,
                        "execution_calibration_status": None,
                        "execution_status": "NOT_APPLICABLE",
                    }
                    for scenario in config.execution_scenarios:
                        for model in ("maker", "taker"):
                            net = response - (0.5 if model == "maker" else 1.0)
                            yield {
                                **common,
                                "row_kind": "execution",
                                "execution_scenario": scenario.name,
                                "execution_model": model,
                                "execution_calibration_status": (
                                    scenario.calibration_status
                                ),
                                "execution_status": "FILLED",
                                "execution_source": "synthetic-stress",
                                "economic_scope": "BEFORE_FUNDING",
                                "funding_status": "NOT_EVALUATED",
                                "economic_admissibility": "NOT_ADMISSIBLE",
                                "net_execution_scope": "MATCHED_FILLED_SLICE_ONLY",
                                "gross_execution_bps": response,
                                "net_execution_bps": net,
                                "before_funding_execution_bps": net,
                                "fill_adjusted_gross_bps": response,
                                "fill_adjusted_net_bps": net,
                                "break_even_move_bps": abs(response - net),
                                "entry_fee_bps_applied": 0.1,
                                "exit_fee_bps_applied": 0.1,
                                "entry_spread_cost_bps": 0.1,
                                "exit_spread_cost_bps": 0.1,
                                "entry_slippage_cost_bps": 0.1,
                                "exit_slippage_cost_bps": 0.1,
                                "adverse_exit_cost_bps": 0.0,
                                "matched_fill_fraction": 1.0,
                                "unclosed_exposure_fraction": 0.0,
                            }


def _integration_bindings(config: LeadLagConfig) -> dict[str, str]:
    values = {
        "artifact_schema_version": "2",
        "streaming_resource_model_version": config.streaming_resource_model_version,
        "research_status": "EVENT_REPLAY_RESEARCH_ONLY",
        "source_time_lead_status": "NOT_ADMISSIBLE",
        "config_sha256": config.config_hash,
        "gate_report_sha256": hashlib.sha256(b"stress-gate").hexdigest(),
        "semantic_gate_sha256": hashlib.sha256(
            b"stress-semantic-gate"
        ).hexdigest(),
        "semantic_gate_canonicalizer_version": (
            "phase10_semantic_gate_payload_v1"
        ),
        "semantic_gate_excluded_json_pointers": '["/observability"]',
        "manifest_fingerprint": hashlib.sha256(b"stress-manifests").hexdigest(),
        "selected_manifests_sha256": hashlib.sha256(
            b"stress-selected-manifests"
        ).hexdigest(),
        "selected_manifest_count": "1",
    }
    if tuple(values) != PHASE10_EVENT_EVIDENCE_BINDING_COLUMNS:
        raise AssertionError("stress bindings drifted from the fixed event schema")
    return values


@dataclass(frozen=True, slots=True)
class _IntegrationResult:
    event_rows: int
    metric_rows: int
    bucket_rows: int
    control_rows: int
    exact_median: float
    parquet_rows: int
    parquet_bytes: int
    parquet_sha256: str
    logical_sha256: str
    peak_quantile_buffer_rows: int
    scratch_bytes: int


def _run_spool_integration(
    scratch_dir: Path, *, config: LeadLagConfig
) -> _IntegrationResult:
    integration = scratch_dir / "real-event-spool"
    integration.mkdir(parents=True, exist_ok=False)
    interval = StrictInterval(_BASE, _BASE + timedelta(hours=1), "integration")
    parquet_path = integration / "events.parquet"
    spool = EventSpool(
        integration / "events.sqlite3",
        quantile_run_rows=config.quantile_sort_run_rows,
    )
    try:
        spool.add_rows(_integration_event_rows(config, interval))
        aggregates = aggregate_streaming_events(spool, (interval,), config)
        exact_median = spool.exact_quantile(
            metric="information_response",
            filters={
                "asset": "BTC",
                "signal_family": SIGNAL_FAMILIES[0],
                "horizon_ms": config.horizons_ms[0],
                "execution_scenario": None,
                "execution_model": None,
            },
            quantile=0.5,
        )
        parquet_rows, parquet_bytes, logical_sha256 = spool.write_parquet(
            parquet_path,
            bindings=_integration_bindings(config),
            row_group_rows=config.parquet_row_group_rows,
            writer_buffer_rows=config.writer_buffer_rows,
        )
        peak_quantile = spool.max_quantile_buffer_rows
        event_rows = spool.total_rows
    finally:
        spool.close()
    parquet_sha256 = _file_sha256(parquet_path)
    return _IntegrationResult(
        event_rows=event_rows,
        metric_rows=len(aggregates.metrics),
        bucket_rows=len(aggregates.bucket_metrics),
        control_rows=len(aggregates.controls),
        exact_median=exact_median,
        parquet_rows=parquet_rows,
        parquet_bytes=parquet_bytes,
        parquet_sha256=parquet_sha256,
        logical_sha256=logical_sha256,
        peak_quantile_buffer_rows=peak_quantile,
        scratch_bytes=_directory_bytes(integration),
    )


def run_production_component_stress(
    scratch_dir: Path,
    *,
    manifest_count: int = _MANIFEST_EVIDENCE_COUNT,
    source_rows: int = 2_000_000,
    minimum_output_events: int = 2_000_000,
    writer_buffer_rows: int = 16_384,
    quantile_run_rows: int = 250_000,
) -> ProductionComponentStressResult:
    """Benchmark the real causal kernel and bounded publication components."""

    for name, value in {
        "manifest_count": manifest_count,
        "source_rows": source_rows,
        "minimum_output_events": minimum_output_events,
        "writer_buffer_rows": writer_buffer_rows,
        "quantile_run_rows": quantile_run_rows,
    }.items():
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if manifest_count < _MANIFEST_EVIDENCE_COUNT:
        raise ValueError(
            "manifest_count must reference the validated 60,001-file loader shape"
        )
    scratch_dir = Path(scratch_dir)
    scratch_dir.mkdir(parents=True, exist_ok=False)
    total_started = time.perf_counter()
    timings: dict[str, float] = {}
    scratch_peak = _directory_bytes(scratch_dir)
    config = LeadLagConfig(
        randomization_resamples=19,
        minimum_events=2,
        writer_buffer_rows=writer_buffer_rows,
        parquet_row_group_rows=max(writer_buffer_rows, 1_024),
        quantile_sort_run_rows=quantile_run_rows,
        scratch_low_watermark_bytes=1,
        scratch_reserve_bytes=1,
    )

    kernel_artifacts = scratch_dir / "kernel-event-spool"
    kernel_artifacts.mkdir()
    kernel_parquet_path = kernel_artifacts / "events.parquet"
    kernel_spool = EventSpool(
        kernel_artifacts / "events.sqlite3",
        quantile_run_rows=config.quantile_sort_run_rows,
    )
    try:
        started = time.perf_counter()
        source = _LazyKernelSource(
            requested_rows=source_rows,
            minimum_output_events=minimum_output_events,
        )
        sink = _CountingHashSink(
            config.writer_buffer_rows,
            spool=kernel_spool,
        )
        high_waters: list[StreamingKernelHighWater] = []
        for asset in config.assets:
            result = run_streaming_kernel(
                source,
                asset=asset,
                interval=source.interval,
                config=config,
                sink=sink,
                include_execution=True,
            )
            high_waters.append(result.high_water)
        kernel_high_water = _maximum_kernel_high_water(high_waters)
        timings["causal_kernel_and_event_spool"] = (
            time.perf_counter() - started
        )
        scratch_peak = max(scratch_peak, _directory_bytes(scratch_dir))

        expected_families = tuple(sorted(SIGNAL_FAMILIES))
        expected_scenarios = tuple(
            sorted(item.name for item in config.execution_scenarios)
        )
        if source.rows_scanned < source_rows:
            raise AssertionError(
                "lazy kernel source did not reach requested source-row shape"
            )
        if sink.total_rows < minimum_output_events:
            raise AssertionError(
                "causal kernel did not reach requested output-event shape: "
                f"observed={sink.total_rows} required={minimum_output_events}"
            )
        if kernel_spool.total_rows != sink.total_rows:
            raise AssertionError("not every real kernel row traversed EventSpool")
        observed_venues = tuple(sorted(source.observed_venues))
        observed_assets = tuple(sorted(source.observed_assets))
        observed_record_types = tuple(sorted(source.observed_record_types))
        observed_families = tuple(sorted(sink.families))
        observed_horizons = tuple(sorted(sink.horizons))
        observed_scenarios = tuple(sorted(sink.scenarios))
        observed_models = tuple(sorted(sink.models))
        coverage: dict[str, object] = {
            "venues": observed_venues,
            "assets": observed_assets,
            "record_types": observed_record_types,
            "families": observed_families,
            "horizons": observed_horizons,
            "scenarios": observed_scenarios,
            "models": observed_models,
        }
        expected_coverage = {
            "venues": _VENUES,
            "assets": _ASSETS,
            "record_types": _KERNEL_RECORD_TYPES,
            "families": expected_families,
            "horizons": _HORIZONS_MS,
            "scenarios": expected_scenarios,
            "models": ("maker", "taker"),
        }
        if coverage != expected_coverage:
            raise AssertionError(
                "real kernel stress coverage mismatch: "
                f"{coverage!r} != {expected_coverage!r}"
            )

        started = time.perf_counter()
        (
            kernel_parquet_rows,
            kernel_parquet_bytes,
            kernel_parquet_logical_sha256,
        ) = kernel_spool.write_parquet(
            kernel_parquet_path,
            bindings=_integration_bindings(config),
            row_group_rows=config.parquet_row_group_rows,
            writer_buffer_rows=config.writer_buffer_rows,
        )
        timings["full_fixed_schema_parquet"] = time.perf_counter() - started
        if kernel_parquet_rows != sink.total_rows:
            raise AssertionError("fixed-schema Parquet omitted real kernel rows")
        kernel_parquet_sha256 = _file_sha256(kernel_parquet_path)
        kernel_spooled_rows = kernel_spool.total_rows
        kernel_spool_peak_quantile = kernel_spool.max_quantile_buffer_rows
        kernel_spool_max_uncommitted = kernel_spool.max_uncommitted_rows
        scratch_peak = max(scratch_peak, _directory_bytes(scratch_dir))
    finally:
        kernel_spool.close()

    started = time.perf_counter()
    integration = _run_spool_integration(scratch_dir, config=config)
    timings["representative_exact_aggregates_and_parquet"] = (
        time.perf_counter() - started
    )
    scratch_peak = max(
        scratch_peak,
        integration.scratch_bytes,
        _directory_bytes(scratch_dir),
    )
    timings["total"] = time.perf_counter() - total_started
    peak_rss, sampler = _peak_rss()
    return ProductionComponentStressResult(
        platform=platform.platform(),
        python_version=platform.python_version(),
        pyarrow_version=pyarrow.__version__,
        sampler=sampler,
        manifest_catalog_evidence_test=_MANIFEST_EVIDENCE_TEST,
        manifest_catalog_evidence_count=manifest_count,
        requested_source_rows=source_rows,
        source_rows_scanned=source.rows_scanned,
        source_batches_processed=source.batches_processed,
        output_event_row_count=sink.total_rows,
        spooled_kernel_event_rows=kernel_spooled_rows,
        source_sha256=source.source_hash.hexdigest(),
        event_sha256=sink.digest.hexdigest(),
        event_rows_by_kind=dict(sorted(sink.rows_by_kind.items())),
        observed_venues=observed_venues,
        observed_assets=observed_assets,
        observed_record_types=observed_record_types,
        observed_signal_families=observed_families,
        observed_horizons_ms=observed_horizons,
        observed_scenarios=observed_scenarios,
        observed_models=observed_models,
        peak_source_batch_rows=source.peak_batch_rows,
        peak_sink_batch_rows=sink.peak_batch_rows,
        peak_retained_source_rows=kernel_high_water.retained_source_rows,
        peak_bbo_history_rows=kernel_high_water.bbo_history_rows,
        peak_public_trade_history_rows=kernel_high_water.public_trade_history_rows,
        peak_trade_window_batches=kernel_high_water.trade_window_batches,
        peak_l2_history_frames=kernel_high_water.l2_history_frames,
        peak_retained_l2_levels=kernel_high_water.retained_l2_levels,
        peak_pending_response_states=kernel_high_water.pending_response_states,
        peak_pending_execution_states=kernel_high_water.pending_execution_states,
        peak_completed_output_rows=kernel_high_water.completed_output_rows,
        kernel_spool_peak_quantile_buffer_rows=kernel_spool_peak_quantile,
        kernel_spool_max_uncommitted_rows=kernel_spool_max_uncommitted,
        kernel_parquet_rows=kernel_parquet_rows,
        kernel_parquet_bytes=kernel_parquet_bytes,
        kernel_parquet_sha256=kernel_parquet_sha256,
        kernel_parquet_logical_sha256=kernel_parquet_logical_sha256,
        exact_aggregate_scope=(
            "separate_configured_grid_all_assets_families_horizons_"
            "scenarios_models"
        ),
        integration_event_rows=integration.event_rows,
        integration_metric_rows=integration.metric_rows,
        integration_bucket_rows=integration.bucket_rows,
        integration_control_rows=integration.control_rows,
        integration_exact_median=integration.exact_median,
        integration_parquet_rows=integration.parquet_rows,
        integration_parquet_bytes=integration.parquet_bytes,
        integration_parquet_sha256=integration.parquet_sha256,
        integration_logical_sha256=integration.logical_sha256,
        integration_peak_quantile_buffer_rows=integration.peak_quantile_buffer_rows,
        scratch_peak_bytes=scratch_peak,
        measured_peak_rss_bytes=peak_rss,
        elapsed_seconds_by_phase=timings,
    )


def run_lazy_shape_stress(
    scratch_dir: Path, **kwargs: int
) -> ProductionComponentStressResult:
    """Compatibility entry point for the real production-component benchmark."""

    return run_production_component_stress(scratch_dir, **kwargs)


__all__ = [
    "LazyShapeStressResult",
    "ProductionComponentStressResult",
    "run_lazy_shape_stress",
    "run_production_component_stress",
]
