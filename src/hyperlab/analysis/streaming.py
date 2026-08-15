from __future__ import annotations

import hashlib
import shutil
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Protocol

from hyperlab.analysis.lead_lag import (
    SIGNAL_FAMILIES,
    LeadLagConfig,
    StrictInterval,
)
from hyperlab.analysis.streaming_aggregates import aggregate_streaming_events
from hyperlab.analysis.streaming_kernel import (
    StreamingKernelResult,
    run_streaming_kernel,
)
from hyperlab.analysis.streaming_reporting import (
    StreamingEventArtifact,
    StreamingLeadLagAnalysis,
    cleanup_streaming_staging,
    evidence_bindings,
    finalize_streaming_publication,
    make_streaming_staging,
    validate_streaming_destination,
    write_streaming_metadata_artifacts,
)
from hyperlab.analysis.streaming_store import EventSpool, SourceRowSpool

_PRODUCTION_ASSETS = ("BTC", "ETH")
_PRODUCTION_HORIZONS_MS = (50, 100, 250, 500, 1_000, 2_000, 5_000)
_EVENT_SCRATCH_BYTES_PER_ROW = 4_096
_EVENT_OUTPUT_BYTES_PER_ROW = 2_048


class BoundedLeadLagError(ValueError):
    """Raised when the production bounded resource contract cannot be met."""


class BoundedWindow(Protocol):
    @property
    def root(self) -> Path: ...

    @property
    def start(self) -> datetime: ...

    @property
    def end(self) -> datetime: ...

    @property
    def assets(self) -> Sequence[str]: ...

    @property
    def intervals(self) -> Sequence[StrictInterval]: ...

    @property
    def manifest_fingerprint(self) -> str: ...

    @property
    def source_spool(self) -> SourceRowSpool: ...

    @property
    def observability(self) -> Mapping[str, object]: ...


@dataclass(slots=True)
class _CountPass:
    kernels: int = 0
    batches_processed: int = 0
    rows_scanned: int = 0
    primary_signals: int = 0
    reverse_signals: int = 0
    information_rows: int = 0
    reverse_rows: int = 0
    execution_rows: int = 0
    peak_source_rows: int = 0
    peak_simultaneous_rows: int = 0
    peak_l2_levels: int = 0
    peak_pending_responses: int = 0
    peak_pending_executions: int = 0
    peak_completed_bundle_rows: int = 0
    peak_bbo_history_rows: int = 0
    peak_public_trade_history_rows: int = 0
    peak_trade_window_batches: int = 0
    peak_l2_history_frames: int = 0

    @property
    def total_rows(self) -> int:
        return self.information_rows + self.reverse_rows + self.execution_rows

    def add(self, result: StreamingKernelResult) -> None:
        self.kernels += 1
        counts = result.counts
        high_water = result.high_water
        self.batches_processed += high_water.batches_processed
        self.rows_scanned += high_water.rows_scanned
        self.primary_signals += counts.primary_signals
        self.reverse_signals += counts.reverse_signals
        self.information_rows += counts.information_rows
        self.reverse_rows += counts.control_rows
        self.execution_rows += counts.execution_rows
        self.peak_source_rows = max(
            self.peak_source_rows, high_water.retained_source_rows
        )
        self.peak_simultaneous_rows = max(
            self.peak_simultaneous_rows, high_water.simultaneous_batch_rows
        )
        self.peak_l2_levels = max(
            self.peak_l2_levels, high_water.retained_l2_levels
        )
        self.peak_pending_responses = max(
            self.peak_pending_responses, high_water.pending_response_states
        )
        self.peak_pending_executions = max(
            self.peak_pending_executions, high_water.pending_execution_states
        )
        self.peak_completed_bundle_rows = max(
            self.peak_completed_bundle_rows, high_water.completed_output_rows
        )
        self.peak_bbo_history_rows = max(
            self.peak_bbo_history_rows, high_water.bbo_history_rows
        )
        self.peak_public_trade_history_rows = max(
            self.peak_public_trade_history_rows,
            high_water.public_trade_history_rows,
        )
        self.peak_trade_window_batches = max(
            self.peak_trade_window_batches, high_water.trade_window_batches
        )
        self.peak_l2_history_frames = max(
            self.peak_l2_history_frames, high_water.l2_history_frames
        )


@dataclass(slots=True)
class _ResourceMetrics:
    deterministic: dict[str, object] = field(default_factory=dict)
    timings: dict[str, float] = field(default_factory=dict)
    scratch_peak_bytes: int = 0

    def observe_scratch(self, *spools: object) -> None:
        sizes: list[int] = []
        for spool in spools:
            scratch_bytes = getattr(spool, "scratch_bytes", None)
            if callable(scratch_bytes):
                sizes.append(int(scratch_bytes()))
        self.scratch_peak_bytes = max(self.scratch_peak_bytes, *sizes, 0)

    def timed(self, name: str, started: float) -> None:
        self.timings[name] = time.perf_counter() - started

    def as_dict(self) -> dict[str, object]:
        return {
            "semantic": False,
            **self.deterministic,
            "scratch_peak_bytes": self.scratch_peak_bytes,
            "elapsed_seconds_by_phase": {
                key: self.timings[key] for key in sorted(self.timings)
            },
        }


def _validate_production_config(config: LeadLagConfig) -> None:
    if tuple(config.assets) != _PRODUCTION_ASSETS:
        raise BoundedLeadLagError("bounded production assets must remain exactly BTC and ETH")
    if tuple(config.horizons_ms) != _PRODUCTION_HORIZONS_MS:
        raise BoundedLeadLagError(
            "bounded production horizons must remain exactly 50,100,250,500,1000,2000,5000 ms"
        )
    if tuple(SIGNAL_FAMILIES) != (
        "agg_trade",
        "trade_imbalance",
        "bbo_change",
        "l2_imbalance",
        "mid_price_change",
        "microprice_change",
        "short_term_momentum",
        "signed_flow",
    ):
        raise BoundedLeadLagError("Phase 10-2 signal-family contract changed")


def _run_count_pass(window: BoundedWindow, config: LeadLagConfig) -> _CountPass:
    counts = _CountPass()
    for asset in config.assets:
        for interval in sorted(
            window.intervals, key=lambda item: (item.start, item.end, item.tag)
        ):
            counts.add(
                run_streaming_kernel(
                    window.source_spool,
                    asset=asset,
                    interval=interval,
                    config=config,
                    sink=lambda _rows: None,
                )
            )
    return counts


def _disk_preflight(staging: Path, counts: _CountPass, config: LeadLagConfig) -> dict[str, int]:
    usage = shutil.disk_usage(staging)
    scratch_estimate = counts.total_rows * _EVENT_SCRATCH_BYTES_PER_ROW
    output_estimate = counts.total_rows * _EVENT_OUTPUT_BYTES_PER_ROW
    required = scratch_estimate + output_estimate
    retained_free = usage.free - required
    required_remaining = max(
        config.scratch_low_watermark_bytes, config.scratch_reserve_bytes
    )
    if retained_free < required_remaining:
        raise BoundedLeadLagError(
            "bounded scratch/output preflight failed: "
            f"available={usage.free} projected_required={required} "
            f"required_remaining={required_remaining}"
        )
    return {
        "available_bytes": usage.free,
        "projected_event_scratch_bytes": scratch_estimate,
        "projected_event_output_bytes": output_estimate,
        "projected_required_bytes": required,
        "projected_remaining_bytes": retained_free,
        "required_low_watermark_bytes": config.scratch_low_watermark_bytes,
        "required_reserve_bytes": config.scratch_reserve_bytes,
    }


def _deterministic_disk_preflight(
    disk_preflight: Mapping[str, int],
) -> dict[str, int]:
    """Return only preregistered/input-derived disk sizing evidence."""

    return {
        key: int(value)
        for key, value in disk_preflight.items()
        if key not in {"available_bytes", "projected_remaining_bytes"}
    }


def _spool_events(
    window: BoundedWindow,
    config: LeadLagConfig,
    event_spool: EventSpool,
    expected: _CountPass,
) -> tuple[_CountPass, Counter[str]]:
    actual = _CountPass()
    exclusions: Counter[str] = Counter()
    for asset in config.assets:
        for interval in sorted(
            window.intervals, key=lambda item: (item.start, item.end, item.tag)
        ):
            result = run_streaming_kernel(
                window.source_spool,
                asset=asset,
                interval=interval,
                config=config,
                sink=event_spool.add_rows,
            )
            actual.add(result)
            exclusions.update(dict(result.counts.exclusions))
    if actual != expected:
        raise BoundedLeadLagError(
            "streaming count and publication passes produced different deterministic counts"
        )
    if event_spool.total_rows != expected.total_rows:
        raise BoundedLeadLagError("event spool row count differs from count pass")
    return actual, exclusions


def _clock_diagnostics(window: BoundedWindow) -> object:
    value = window.observability.get("clock_sync_diagnostics")
    if value is None:
        value = window.observability.get("clock_diagnostics")
    return value if value is not None else {
        "usage": "DIAGNOSTIC_ONLY_STRICT_INTERVALS_DEFINE_CAUSAL_VALIDITY"
    }


def _summary(
    *,
    window: BoundedWindow,
    config: LeadLagConfig,
    counts: _CountPass,
    exclusions: Counter[str],
    event_size_bytes: int,
    disk_preflight: Mapping[str, int],
) -> dict[str, object]:
    interval_duration = sum(
        (interval.end - interval.start).total_seconds()
        for interval in window.intervals
    )
    evaluable = counts.information_rows - sum(exclusions.values())
    statuses = sorted(
        {scenario.calibration_status for scenario in config.execution_scenarios}
    )
    warnings = [
        "Economic profitability is not claimed; execution results are scenario outputs.",
        "Execution economics are BEFORE_FUNDING; funding is NOT_EVALUATED.",
        "Source/exchange-time lead is not admissible without symmetric Hyperliquid clock calibration.",
        "The independently saved technical capture gate must PASS before real-data results are admissible.",
    ]
    if any(status != "CALIBRATED" for status in statuses):
        warnings.append(
            "One or more execution scenarios are uncalibrated and cannot support an economic claim."
        )
    return {
        "research_status": "EVENT_REPLAY_RESEARCH_ONLY",
        "economic_claim": "NOT_CLAIMED",
        "economic_scope": "BEFORE_FUNDING",
        "funding_status": "NOT_EVALUATED",
        "economic_admissibility": "NOT_ADMISSIBLE_FUNDING_NOT_EVALUATED",
        "technical_gate_requirement": "INDEPENDENT_TECHNICAL_CAPTURE_GATE_PASS_REQUIRED",
        "technical_gate_modified": False,
        "time_axis": "received_time",
        "equal_received_time_semantics": "simultaneous_batch_baseline_includes_ties",
        "source_time_lead_status": "NOT_ADMISSIBLE",
        "local_horizon_status": "ADMISSIBLE_ONLY_FOR_EVALUABLE_ROWS_WITHIN_ONE_STRICT_INTERVAL",
        "source_fingerprint": window.manifest_fingerprint,
        "config_sha256": config.config_hash,
        "assets": list(config.assets),
        "horizons_ms": list(config.horizons_ms),
        "signal_families": list(SIGNAL_FAMILIES),
        "strict_interval_count": len(window.intervals),
        "strict_interval_duration_seconds": interval_duration,
        "primary_signal_count": counts.primary_signals,
        "reverse_signal_count": counts.reverse_signals,
        "capacity_preflight_status": "PASS_BOUNDED_STREAMING_DISK",
        "estimated_event_rows_upper_bound": counts.total_rows,
        "estimated_event_bytes_upper_bound": disk_preflight[
            "projected_event_output_bytes"
        ],
        "capacity_preflight": _deterministic_disk_preflight(disk_preflight),
        "reverse_control_event_row_count": counts.reverse_rows,
        "information_event_row_count": counts.information_rows,
        "evaluable_information_event_count": evaluable,
        "excluded_information_event_count": sum(exclusions.values()),
        "exclusion_counts": dict(sorted(exclusions.items())),
        "execution_event_row_count": counts.execution_rows,
        "persisted_event_row_count": counts.total_rows,
        "event_expansion_factor": counts.total_rows / counts.information_rows
        if counts.information_rows
        else 0.0,
        "event_table_estimated_memory_bytes": 0,
        "event_parquet_bytes": event_size_bytes,
        "resource_model": config.streaming_resource_model_version,
        "legacy_in_memory_limits_used": False,
        "all_configured_hypotheses_reported": True,
        "randomization_method": "deterministic_interval_block_sign_flip_max_t_and_bh_fdr",
        "randomization_resamples": config.randomization_resamples,
        "randomization_block_ms": config.randomization_block_ms,
        "execution_calibration_statuses": statuses,
        "false_positive_definition": (
            "information: endpoint neutral_or_adverse; execution: resolved matched-slice net_bps<=0"
        ),
        "clock_sync_diagnostics": _clock_diagnostics(window),
        "warnings": warnings,
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def run_bounded_analysis_in_staging(
    *,
    window: BoundedWindow,
    config: LeadLagConfig,
    staging: Path,
) -> tuple[StreamingLeadLagAnalysis, StreamingEventArtifact, dict[str, object]]:
    """Execute the two-pass bounded engine for an already admitted immutable window."""

    _validate_production_config(config)
    metrics = _ResourceMetrics()
    started = time.perf_counter()
    counts = _run_count_pass(window, config)
    metrics.timed("exact_count_pass", started)
    metrics.observe_scratch(window.source_spool)

    disk = _disk_preflight(staging, counts, config)
    metrics.deterministic["disk_preflight"] = disk
    event_spool = EventSpool(
        staging / ".scratch" / "events.sqlite3",
        quantile_run_rows=config.quantile_sort_run_rows,
    )
    try:
        started = time.perf_counter()
        actual, exclusions = _spool_events(window, config, event_spool, counts)
        metrics.timed("causal_event_pass", started)
        metrics.observe_scratch(window.source_spool, event_spool)

        started = time.perf_counter()
        aggregates = aggregate_streaming_events(
            event_spool, tuple(window.intervals), config
        )
        metrics.timed("exact_aggregates_and_controls", started)
        metrics.observe_scratch(window.source_spool, event_spool)

        bindings = evidence_bindings(window, config)
        events_path = staging / "events.parquet"
        started = time.perf_counter()
        written, size_bytes, logical_hash = event_spool.write_parquet(
            events_path,
            bindings=bindings,
            row_group_rows=config.parquet_row_group_rows,
            writer_buffer_rows=config.writer_buffer_rows,
        )
        metrics.timed("streaming_parquet", started)
        if written != counts.total_rows:
            raise BoundedLeadLagError("streaming Parquet row count differs from plan")
        event_artifact = StreamingEventArtifact(
            row_count=written,
            size_bytes=size_bytes,
            logical_sha256=logical_hash,
            file_sha256=_sha256_file(events_path),
        )
        metrics.deterministic.update(
            {
                "chunks_processed": actual.kernels,
                "causal_asset_interval_kernels_processed": actual.kernels,
                "causal_batches_processed": actual.batches_processed,
                "causal_rows_scanned": actual.rows_scanned,
                "analysis_passes": 2,
                "projected_primary_signals": actual.primary_signals,
                "projected_reverse_signals": actual.reverse_signals,
                "output_rows_by_kind": {
                    "information": actual.information_rows,
                    "control": actual.reverse_rows,
                    "execution": actual.execution_rows,
                },
                "output_rows_written": written,
                "output_event_bytes": size_bytes,
                "peak_retained_source_rows": actual.peak_source_rows,
                "peak_simultaneous_batch_rows": actual.peak_simultaneous_rows,
                "peak_retained_l2_levels": actual.peak_l2_levels,
                "peak_bbo_history_rows": actual.peak_bbo_history_rows,
                "peak_public_trade_history_rows": (
                    actual.peak_public_trade_history_rows
                ),
                "peak_trade_window_batches": actual.peak_trade_window_batches,
                "peak_l2_history_frames": actual.peak_l2_history_frames,
                "peak_pending_response_states": actual.peak_pending_responses,
                "peak_pending_execution_states": actual.peak_pending_executions,
                "peak_completed_bundle_rows": actual.peak_completed_bundle_rows,
                "peak_writer_python_rows": min(
                    config.writer_buffer_rows, config.parquet_row_group_rows
                ),
                "parquet_row_group_rows": config.parquet_row_group_rows,
                "external_merge_fan_in": config.external_merge_fan_in,
                "peak_external_merge_fan_in": 1,
                "external_ordering_backend": "SQLITE_DISK_BTREE_SINGLE_CURSOR",
                "quantile_sort_run_rows": config.quantile_sort_run_rows,
                "peak_quantile_buffer_rows": event_spool.max_quantile_buffer_rows,
                "quantile_spool_upper_bound_bytes": event_spool.scratch_bytes(),
                "source": dict(window.observability),
            }
        )
        metrics.observe_scratch(window.source_spool, event_spool)
        summary = _summary(
            window=window,
            config=config,
            counts=actual,
            exclusions=exclusions,
            event_size_bytes=size_bytes,
            disk_preflight=disk,
        )
        analysis = StreamingLeadLagAnalysis(
            summary=summary,
            metrics=aggregates.metrics,
            bucket_metrics=aggregates.bucket_metrics,
            controls=aggregates.controls,
            event_row_count=written,
        )
        return analysis, event_artifact, metrics.as_dict()
    finally:
        event_spool.close()


def run_bounded_lead_lag_study(
    root: Path,
    gate_report_path: Path,
    config: LeadLagConfig,
    output: Path,
) -> Mapping[str, Path]:
    """Run the production-only bounded Phase 10-2 pipeline and publish atomically."""

    from hyperlab.analysis.streaming_lake import (
        BoundedLeadLagWindow,
        load_bounded_lead_lag_window,
        validate_bounded_lead_lag_gate,
        verify_immutable_inputs_unchanged,
    )

    validate_streaming_destination(root, output)
    _validate_production_config(config)
    admission = validate_bounded_lead_lag_gate(root, gate_report_path)
    staging = make_streaming_staging(output)
    window: BoundedLeadLagWindow | None = None
    try:
        window = load_bounded_lead_lag_window(
            admission,
            scratch_dir=staging / ".scratch",
            selected_manifests_path=staging / "selected_manifests.jsonl",
            batch_rows=min(
                config.writer_buffer_rows,
                config.max_simultaneous_batch_rows,
            ),
            max_l2_rows_per_receive=config.max_simultaneous_batch_rows,
            scratch_low_watermark_bytes=config.scratch_low_watermark_bytes,
            scratch_reserve_bytes=config.scratch_reserve_bytes,
        )
        analysis, event_artifact, observability = run_bounded_analysis_in_staging(
            window=window,
            config=config,
            staging=staging,
        )
        close = getattr(window, "close", None)
        if callable(close):
            close()
        else:
            window.source_spool.close()
        write_streaming_metadata_artifacts(
            staging=staging,
            analysis=analysis,
            window=window,
            config=config,
            event_artifact=event_artifact,
            resource_observability=observability,
        )
        return finalize_streaming_publication(
            staging=staging,
            output=output,
            root=root,
            verify_inputs_unchanged=lambda: verify_immutable_inputs_unchanged(window),
        )
    except BaseException:
        if window is not None:
            close = getattr(window, "close", None)
            if callable(close):
                close()
            else:
                window.source_spool.close()
        cleanup_streaming_staging(staging)
        raise


__all__ = [
    "BoundedLeadLagError",
    "run_bounded_analysis_in_staging",
    "run_bounded_lead_lag_study",
]
