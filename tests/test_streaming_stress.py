from __future__ import annotations

import inspect
from pathlib import Path

from hyperlab.analysis.lead_lag import SIGNAL_FAMILIES
from hyperlab.analysis.streaming_stress import (
    ProductionComponentStressResult,
    run_production_component_stress,
)

_HORIZONS_MS = (50, 100, 250, 500, 1_000, 2_000, 5_000)
_MANIFEST_TEST = (
    "tests/test_phase10_streaming_lake.py::"
    "test_lazy_catalog_shape_exceeds_sixty_thousand_files_without_a_file_list"
)


def _run_fast(
    target: Path,
    *,
    source_rows: int = 4_000,
    output_rows: int = 2_000,
) -> ProductionComponentStressResult:
    return run_production_component_stress(
        target,
        manifest_count=60_001,
        source_rows=source_rows,
        minimum_output_events=output_rows,
        writer_buffer_rows=128,
        quantile_run_rows=256,
    )


def test_real_kernel_spool_aggregate_and_parquet_coverage(tmp_path: Path) -> None:
    result = _run_fast(tmp_path / "stress")

    assert result.manifest_catalog_evidence_count == 60_001
    assert result.manifest_catalog_evidence_test == _MANIFEST_TEST
    assert Path(_MANIFEST_TEST.partition("::")[0]).is_file()
    assert result.source_rows_scanned >= result.requested_source_rows
    assert result.output_event_row_count >= 2_000
    assert result.spooled_kernel_event_rows == result.output_event_row_count
    assert result.kernel_parquet_rows == result.output_event_row_count
    assert set(result.event_rows_by_kind) == {
        "control",
        "execution",
        "information",
    }
    assert result.observed_venues == ("binance_usdm", "hyperliquid")
    assert result.observed_assets == ("BTC", "ETH")
    assert result.observed_record_types == ("bbo", "l2", "trade")
    assert result.observed_signal_families == tuple(sorted(SIGNAL_FAMILIES))
    assert result.observed_horizons_ms == _HORIZONS_MS
    assert result.observed_scenarios == ("adverse", "baseline")
    assert result.observed_models == ("maker", "taker")

    assert result.peak_source_batch_rows == 64
    assert 0 < result.peak_sink_batch_rows <= 128
    assert result.peak_retained_source_rows > 0
    assert result.peak_bbo_history_rows > 0
    assert result.peak_public_trade_history_rows > 0
    assert result.peak_trade_window_batches > 0
    assert result.peak_l2_history_frames > 0
    assert result.peak_retained_l2_levels > 0
    assert result.peak_pending_response_states > 0
    assert result.peak_pending_execution_states > 0
    assert result.peak_completed_output_rows <= 128
    assert result.kernel_spool_peak_quantile_buffer_rows <= 128
    assert result.kernel_spool_max_uncommitted_rows <= 2_048 + 2 * 128

    assert result.exact_aggregate_scope.startswith("separate_configured_grid")
    assert result.integration_event_rows == 1_344
    assert result.integration_metric_rows > 0
    assert result.integration_bucket_rows > 0
    assert result.integration_control_rows > 0
    assert result.integration_exact_median == 0.0
    assert result.integration_parquet_rows == result.integration_event_rows
    assert result.scratch_peak_bytes >= (
        result.kernel_parquet_bytes + result.integration_parquet_bytes
    )
    assert result.measured_peak_rss_bytes is not None
    assert result.measured_peak_rss_bytes > 0
    assert set(result.elapsed_seconds_by_phase) == {
        "causal_kernel_and_event_spool",
        "full_fixed_schema_parquet",
        "representative_exact_aggregates_and_parquet",
        "total",
    }
    for digest in (
        result.source_sha256,
        result.event_sha256,
        result.kernel_parquet_sha256,
        result.kernel_parquet_logical_sha256,
        result.integration_parquet_sha256,
        result.integration_logical_sha256,
    ):
        assert len(digest) == 64


def test_production_component_stress_is_deterministic(tmp_path: Path) -> None:
    first = _run_fast(tmp_path / "first")
    second = _run_fast(tmp_path / "second")

    assert first.deterministic_dict() == second.deterministic_dict()


def test_real_kernel_state_high_waters_are_stable_as_duration_scales(
    tmp_path: Path,
) -> None:
    shorter = _run_fast(
        tmp_path / "shorter",
        source_rows=20_000,
        output_rows=4_000,
    )
    longer = _run_fast(
        tmp_path / "longer",
        source_rows=40_000,
        output_rows=8_000,
    )

    assert longer.source_rows_scanned > shorter.source_rows_scanned
    assert longer.output_event_row_count > shorter.output_event_row_count
    assert longer.kernel_parquet_rows > shorter.kernel_parquet_rows
    state_fields = (
        "peak_source_batch_rows",
        "peak_sink_batch_rows",
        "peak_retained_source_rows",
        "peak_bbo_history_rows",
        "peak_public_trade_history_rows",
        "peak_trade_window_batches",
        "peak_l2_history_frames",
        "peak_retained_l2_levels",
        "peak_pending_response_states",
        "peak_pending_execution_states",
        "peak_completed_output_rows",
    )
    for field in state_fields:
        assert getattr(longer, field) == getattr(shorter, field), field
    for result in (shorter, longer):
        assert result.kernel_spool_peak_quantile_buffer_rows <= 128
        assert result.kernel_spool_max_uncommitted_rows <= 2_048 + 2 * 128


def test_standalone_defaults_are_sixty_thousand_and_multi_million_shape() -> None:
    parameters = inspect.signature(run_production_component_stress).parameters

    assert parameters["manifest_count"].default == 60_001
    assert parameters["source_rows"].default == 2_000_000
    assert parameters["minimum_output_events"].default == 2_000_000
