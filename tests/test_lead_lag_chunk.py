from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

import hyperlab.analysis.lead_lag as lead_lag_module
from hyperlab.analysis.lead_lag import (
    STREAMING_RESOURCE_MODEL_VERSION,
    ExecutionAssumptions,
    LeadLagChunkCapacityError,
    LeadLagChunkLimits,
    LeadLagChunkResult,
    LeadLagConfig,
    analyze_lead_lag,
    analyze_lead_lag_chunk,
    load_lead_lag_config,
)
from hyperlab.analysis.synthetic import generate_synthetic_lead_lag_dataset

EVENT_ORDER = (
    "signal_time",
    "asset",
    "signal_family",
    "horizon_ms",
    "signal_id",
    "row_kind",
    "execution_scenario",
    "execution_model",
)


def _config() -> LeadLagConfig:
    return LeadLagConfig(
        horizons_ms=(100, 250, 500),
        randomization_resamples=19,
        execution_scenarios=(
            ExecutionAssumptions(
                name="chunk",
                latency_ms=10,
                exit_latency_ms=10,
                maker_timeout_ms=50,
                taker_fee_bps=1.0,
                slippage_bps=0.5,
            ),
        ),
    )


def _combined(result: LeadLagChunkResult) -> pd.DataFrame:
    frame = pd.concat(
        (
            result.information_events,
            result.reverse_events,
            result.execution_events,
        ),
        ignore_index=True,
        sort=False,
    )
    return frame.sort_values(list(EVENT_ORDER), kind="mergesort").reset_index(
        drop=True
    )


def test_chunk_matches_pandas_oracle_and_ignores_legacy_materialization_caps() -> None:
    fixture = generate_synthetic_lead_lag_dataset(
        event_count=32,
        injected_lag_ms=250,
    )
    core_start = pd.Timestamp(fixture.signal_times[8])
    core_end = pd.Timestamp(fixture.signal_times[12])
    oracle_config = _config()
    oracle = analyze_lead_lag(
        fixture.dataset,
        fixture.intervals,
        oracle_config,
    )
    bounded_config = replace(
        oracle_config,
        max_event_rows=1,
        max_estimated_event_bytes=1,
    )

    result = analyze_lead_lag_chunk(
        fixture.dataset,
        fixture.intervals[0],
        bounded_config,
        asset="btc",
        core_start=core_start,
        core_end=core_end,
    )

    expected = oracle.events.loc[
        oracle.events["asset"].eq("BTC")
        & oracle.events["signal_time"].ge(core_start)
        & oracle.events["signal_time"].lt(core_end)
    ].reset_index(drop=True)
    pd.testing.assert_frame_equal(_combined(result), expected)
    assert result.asset == "BTC"
    assert result.resource_model_version == STREAMING_RESOURCE_MODEL_VERSION
    assert result.output_event_row_count == len(expected)
    assert result.information_events["row_kind"].eq("information").all()
    assert result.reverse_events["row_kind"].eq("control").all()
    assert result.execution_events["row_kind"].eq("execution").all()
    for frame in (
        result.information_events,
        result.reverse_events,
        result.execution_events,
    ):
        assert frame["signal_time"].ge(core_start).all()
        assert frame["signal_time"].lt(core_end).all()


def test_chunk_halo_is_formula_driven_and_clipped_to_strict_interval() -> None:
    fixture = generate_synthetic_lead_lag_dataset(event_count=32)
    config = _config()
    interval = fixture.intervals[0]
    core_start = pd.Timestamp(interval.start) + pd.Timedelta(milliseconds=500)
    core_end = pd.Timestamp(interval.start) + pd.Timedelta(milliseconds=1_000)

    result = analyze_lead_lag_chunk(
        fixture.dataset,
        interval,
        config,
        asset="BTC",
        core_start=core_start,
        core_end=core_end,
    )

    assert result.halo_start == pd.Timestamp(interval.start)
    assert result.halo_end == core_end + pd.Timedelta(milliseconds=1_510)
    assert result.primary_signal_count == 0
    assert result.reverse_signal_count == 0
    assert result.output_event_row_count == 0


@pytest.mark.parametrize(
    ("field", "message"),
    (
        ("max_source_rows_per_chunk", "max_source_rows_per_chunk"),
        ("max_simultaneous_batch_rows", "max_simultaneous_batch_rows"),
        ("max_l2_frame_levels", "max_l2_frame_levels"),
        ("max_l2_levels_per_chunk", "max_l2_levels_per_chunk"),
        ("max_pending_response_states", "max_pending_response_states"),
        ("max_pending_execution_states", "max_pending_execution_states"),
    ),
)
def test_chunk_fails_closed_before_each_bounded_population(
    field: str,
    message: str,
) -> None:
    fixture = generate_synthetic_lead_lag_dataset(event_count=32)
    core_start = pd.Timestamp(fixture.signal_times[8])
    core_end = pd.Timestamp(fixture.signal_times[9])
    generous = LeadLagChunkLimits(
        max_source_rows_per_chunk=100_000,
        max_simultaneous_batch_rows=100_000,
        max_l2_frame_levels=100_000,
        max_l2_levels_per_chunk=100_000,
        max_pending_response_states=100_000,
        max_pending_execution_states=100_000,
    )
    limits = replace(generous, **{field: 1})

    with pytest.raises(LeadLagChunkCapacityError, match=message):
        analyze_lead_lag_chunk(
            fixture.dataset,
            fixture.intervals[0],
            _config(),
            asset="BTC",
            core_start=core_start,
            core_end=core_end,
            limits=limits,
        )


@pytest.mark.parametrize(
    "field",
    (
        "max_source_rows_per_chunk",
        "max_simultaneous_batch_rows",
        "max_l2_frame_levels",
        "max_l2_levels_per_chunk",
        "max_pending_response_states",
        "max_pending_execution_states",
        "quantile_sort_run_rows",
        "parquet_row_group_rows",
        "writer_buffer_rows",
        "scratch_low_watermark_bytes",
        "scratch_reserve_bytes",
    ),
)
def test_streaming_resource_controls_require_positive_non_boolean_integers(
    field: str,
) -> None:
    config = _config()
    with pytest.raises(ValueError, match=field):
        replace(config, **{field: 0})
    with pytest.raises(ValueError, match=field):
        replace(config, **{field: True})


def test_streaming_resource_model_and_production_toml_are_explicit(
    tmp_path: Path,
) -> None:
    config_path = Path("config/lead_lag_phase10.toml")
    config = load_lead_lag_config(config_path)
    payload = config.as_dict()

    assert config.streaming_resource_model_version == STREAMING_RESOURCE_MODEL_VERSION
    assert config.assets == ("BTC", "ETH")
    assert config.horizons_ms == (50, 100, 250, 500, 1_000, 2_000, 5_000)
    assert len(config.execution_scenarios) == 2
    for field in (
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
    ):
        assert field in payload
    assert LeadLagChunkLimits.from_config(config).max_source_rows_per_chunk == (
        config.max_source_rows_per_chunk
    )
    assert replace(
        config, writer_buffer_rows=config.writer_buffer_rows + 1
    ).config_hash != config.config_hash
    with pytest.raises(ValueError, match="streaming_resource_model_version"):
        replace(config, streaming_resource_model_version="UNKNOWN")
    with pytest.raises(ValueError, match="external_merge_fan_in"):
        replace(config, external_merge_fan_in=1)
    with pytest.raises(ValueError, match="writer_buffer_rows"):
        replace(
            config,
            writer_buffer_rows=config.parquet_row_group_rows + 1,
        )
    with pytest.raises(ValueError, match="max_l2_frame_levels"):
        replace(
            config,
            max_l2_frame_levels=config.max_l2_levels_per_chunk + 1,
        )

    missing_control = tmp_path / "missing-control.toml"
    missing_control.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "max_source_rows_per_chunk = 100000\n", ""
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="missing explicit keys"):
        load_lead_lag_config(missing_control)


def test_chunk_rejects_aggregate_l2_levels_before_explosion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = generate_synthetic_lead_lag_dataset(event_count=32)
    core_start = pd.Timestamp(fixture.signal_times[8])
    core_end = pd.Timestamp(fixture.signal_times[9])
    selected = fixture.dataset.l2.loc[
        fixture.dataset.l2["asset"].eq("BTC")
        & fixture.dataset.l2["received_time"].ge(core_start - pd.Timedelta(seconds=2))
        & fixture.dataset.l2["received_time"].lt(core_end + pd.Timedelta(seconds=2))
    ]
    assert len(selected) > 1
    level_counts = selected.apply(
        lambda row: len(row["bids"]) + len(row["asks"]), axis=1
    )
    assert int(level_counts.max()) == 6

    def forbidden_prepare(_frame: pd.DataFrame) -> pd.DataFrame:
        raise AssertionError("L2 explosion began before the aggregate bound")

    monkeypatch.setattr(lead_lag_module, "_prepare_l2", forbidden_prepare)
    limits = LeadLagChunkLimits(
        max_source_rows_per_chunk=100_000,
        max_simultaneous_batch_rows=100_000,
        max_l2_frame_levels=6,
        max_l2_levels_per_chunk=6,
        max_pending_response_states=100_000,
        max_pending_execution_states=100_000,
    )
    with pytest.raises(
        LeadLagChunkCapacityError,
        match=r"l2_levels_per_chunk=\d+ exceeds max_l2_levels_per_chunk=6",
    ):
        analyze_lead_lag_chunk(
            fixture.dataset,
            fixture.intervals[0],
            _config(),
            asset="BTC",
            core_start=core_start,
            core_end=core_end,
            limits=limits,
        )


def test_chunk_rejects_non_half_open_core_or_unconfigured_asset() -> None:
    fixture = generate_synthetic_lead_lag_dataset(event_count=32)
    interval = fixture.intervals[0]
    start = pd.Timestamp(interval.start)

    with pytest.raises(ValueError, match="half-open"):
        analyze_lead_lag_chunk(
            fixture.dataset,
            interval,
            _config(),
            asset="BTC",
            core_start=start,
            core_end=start,
        )
    with pytest.raises(ValueError, match="configured assets"):
        analyze_lead_lag_chunk(
            fixture.dataset,
            interval,
            _config(),
            asset="SOL",
            core_start=start,
            core_end=start + pd.Timedelta(seconds=1),
        )
