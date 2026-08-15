from __future__ import annotations

import math
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from hyperlab.analysis.lead_lag import (
    SIGNAL_FAMILIES,
    LeadLagConfig,
    StrictInterval,
    analyze_lead_lag,
)
from hyperlab.analysis.streaming_aggregates import (
    RandomizationDiagnostics,
    aggregate_streaming_events,
)
from hyperlab.analysis.streaming_store import EventSpool
from hyperlab.analysis.synthetic import generate_synthetic_lead_lag_dataset


class _FrameEventSource:
    """Small-fixture adapter; production sources remain disk-backed."""

    def __init__(self, frame: pd.DataFrame, *, reverse_blocks: bool = False) -> None:
        self._rows = frame.to_dict(orient="records")
        self._reverse_blocks = reverse_blocks
        self.iteration_queries: list[tuple[dict[str, object], tuple[str, ...]]] = []
        self.quantile_queries: list[tuple[str, dict[str, object], float]] = []

    @staticmethod
    def _value(row: Mapping[str, object], name: str) -> object:
        if name != "time_bucket_ns":
            return row.get(name)
        value = row.get("time_bucket")
        if value is None or value is pd.NA or value is pd.NaT:
            return None
        return int(pd.Timestamp(value).value)

    @classmethod
    def _matches(
        cls, row: Mapping[str, object], filters: Mapping[str, object]
    ) -> bool:
        for name, expected in filters.items():
            actual = cls._value(row, name)
            if isinstance(expected, Sequence) and not isinstance(
                expected, (str, bytes, bytearray)
            ):
                if actual not in expected:
                    return False
            elif actual != expected:
                return False
        return True

    def iter_rows(
        self,
        *,
        filters: Mapping[str, object],
        columns: Sequence[str],
    ) -> Iterator[tuple[object, ...]]:
        projected = tuple(columns)
        assert projected
        self.iteration_queries.append((dict(filters), projected))
        for row in self._rows:
            if self._matches(row, filters):
                yield tuple(self._value(row, name) for name in projected)

    def exact_quantile(
        self,
        *,
        metric: str,
        filters: Mapping[str, object],
        quantile: float,
    ) -> float:
        self.quantile_queries.append((metric, dict(filters), quantile))
        column = {
            "information_response": "response_bps",
            "information_first_move_delay": "first_move_delay_ms",
            "execution_net": "net_execution_bps",
        }[metric]

        def eligible(row: Mapping[str, object]) -> bool:
            if not self._matches(row, filters):
                return False
            if metric == "information_response":
                return (
                    row.get("row_kind") == "information"
                    and row.get("signal_role") == "primary"
                    and bool(row.get("evaluable"))
                )
            if metric == "information_first_move_delay":
                return (
                    row.get("row_kind") == "information"
                    and row.get("signal_role") == "primary"
                    and bool(row.get("evaluable"))
                    and row.get("first_move_direction") in {"same", "opposite"}
                )
            return (
                row.get("row_kind") == "execution"
                and row.get("execution_status") in {"FILLED", "PARTIAL"}
            )

        values = pd.to_numeric(
            pd.Series(
                [
                    row.get(column)
                    for row in self._rows
                    if eligible(row)
                ],
                dtype=object,
            ),
            errors="coerce",
        ).dropna()
        return float(values.quantile(quantile, interpolation="linear"))

    def distinct_randomization_blocks(self) -> tuple[str, ...]:
        values = sorted(
            {
                str(row["randomization_block"])
                for row in self._rows
                if row.get("row_kind") == "information"
                and row.get("signal_role") == "primary"
                and bool(row.get("evaluable"))
            },
            reverse=self._reverse_blocks,
        )
        return tuple(values)


def _config() -> LeadLagConfig:
    return LeadLagConfig(
        randomization_resamples=19,
        minimum_events=2,
    )


def _assert_oracle_equal(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    pd.testing.assert_frame_equal(
        actual,
        expected,
        check_dtype=False,
        check_exact=False,
        rtol=1e-12,
        atol=1e-12,
    )


@pytest.mark.parametrize("null", [False, True])
def test_streaming_aggregates_match_pandas_oracle_to_1e_12(
    null: bool, tmp_path: Path
) -> None:
    fixture = generate_synthetic_lead_lag_dataset(
        event_count=32,
        seed=20_260_815 + int(null),
        null=null,
    )
    config = _config()
    oracle = analyze_lead_lag(fixture.dataset, fixture.intervals, config)
    source = _FrameEventSource(oracle.events)

    actual = aggregate_streaming_events(source, fixture.intervals, config)

    _assert_oracle_equal(actual.metrics, oracle.metrics)
    _assert_oracle_equal(actual.bucket_metrics, oracle.bucket_metrics)
    _assert_oracle_equal(actual.controls, oracle.controls)
    assert set(actual.metrics["horizon_ms"]) == set(config.horizons_ms)
    execution = actual.metrics.loc[actual.metrics["analysis_kind"].eq("execution")]
    assert set(execution["execution_scenario"].dropna()) == {
        scenario.name for scenario in config.execution_scenarios
    }
    assert set(execution["execution_model"].dropna()) == {"maker", "taker"}
    assert len(source.iteration_queries) == 4
    assert source.quantile_queries
    assert actual.metrics["inference_status"].eq("ADMISSIBLE_RANDOMIZATION").any()

    if not null:
        with EventSpool(tmp_path / "events.sqlite") as spool:
            spool.add_frame(oracle.events)
            spooled = aggregate_streaming_events(spool, fixture.intervals, config)
        _assert_oracle_equal(spooled.metrics, oracle.metrics)
        _assert_oracle_equal(spooled.bucket_metrics, oracle.bucket_metrics)
        _assert_oracle_equal(spooled.controls, oracle.controls)


def test_randomization_uses_the_globally_sorted_block_set() -> None:
    fixture = generate_synthetic_lead_lag_dataset(event_count=32, seed=91)
    config = _config()
    oracle = analyze_lead_lag(fixture.dataset, fixture.intervals, config)

    ascending = aggregate_streaming_events(
        _FrameEventSource(oracle.events), fixture.intervals, config
    )
    descending = aggregate_streaming_events(
        _FrameEventSource(oracle.events, reverse_blocks=True),
        fixture.intervals,
        config,
    )

    _assert_oracle_equal(descending.metrics, ascending.metrics)
    _assert_oracle_equal(descending.controls, ascending.controls)


def test_randomization_state_scales_with_blocks_and_hypotheses_not_events() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    interval = StrictInterval(
        start=start,
        end=start + timedelta(minutes=30),
        tag="randomization-scaling",
    )
    base = pd.DataFrame(
        [
            {
                "row_kind": "information",
                "signal_role": "primary",
                "asset": "BTC",
                "signal_family": "agg_trade",
                "horizon_ms": 50,
                "time_bucket": pd.Timestamp(start),
                "evaluable": True,
                "classification": "same_direction",
                "response_bps": value,
                "negative_lag_response_bps": -value,
                "first_move_direction": "same",
                "first_move_delay_ms": float(position + 1),
                "randomization_block": f"block-{position % 2}",
            }
            for position, value in enumerate((1.0, -0.5, 0.25, 2.0))
        ]
    )
    config = _config()
    small = RandomizationDiagnostics()
    large = RandomizationDiagnostics()

    aggregate_streaming_events(
        _FrameEventSource(base),
        (interval,),
        config,
        randomization_diagnostics=small,
    )
    aggregate_streaming_events(
        _FrameEventSource(pd.concat([base] * 250, ignore_index=True)),
        (interval,),
        config,
        randomization_diagnostics=large,
    )

    assert small.evaluable_response_rows_scanned == 4
    assert large.evaluable_response_rows_scanned == 1_000
    for field in (
        "block_accumulator_cells",
        "eligible_hypotheses",
        "globally_used_blocks",
        "sign_matrix_values",
        "randomized_output_values",
        "block_matrix_applications",
        "per_event_resample_vector_updates",
    ):
        assert getattr(small, field) == getattr(large, field)
    assert large.block_accumulator_cells == 2
    assert large.eligible_hypotheses == 1
    assert large.globally_used_blocks == 2
    assert large.sign_matrix_values == config.randomization_resamples * 2
    assert large.randomized_output_values == config.randomization_resamples
    assert large.block_matrix_applications == 1
    assert large.per_event_resample_vector_updates == 0


def test_block_reconstruction_preserves_null_boundary_reduction_order() -> None:
    values = [
        4.999999999999795,
        5.000000000000907,
        4.999999999999795,
        -4.999999999999795,
        -4.999999999999133,
        4.999999999999133,
        -4.999999999999795,
        -4.999999999999795,
        5.000000000000907,
        4.999999999998685,
        5.000000000000907,
        -4.999999999998685,
        -4.999999999999133,
        5.000000000001352,
        4.999999999999133,
        4.999999999998685,
        -4.999999999999133,
        -4.999999999998685,
        4.999999999999133,
        5.000000000001352,
        4.999999999999133,
        -4.999999999999133,
        4.999999999999795,
        -4.999999999999133,
        -5.000000000001352,
        -4.999999999999133,
        -4.999999999999133,
        -4.999999999998685,
        4.999999999999133,
        -4.999999999998685,
        -4.999999999999795,
        4.999999999999133,
    ]
    locations = np.asarray(
        [0] * 8 + [1] * 9 + [2] * 8 + [3] * 7,
        dtype=np.int64,
    )
    start = datetime(2025, 1, 1, tzinfo=UTC)
    rows = pd.DataFrame(
        [
            {
                "row_kind": "information",
                "signal_role": "primary",
                "asset": "BTC",
                "signal_family": "agg_trade",
                "horizon_ms": 50,
                "time_bucket": pd.Timestamp(start),
                "evaluable": True,
                "classification": "same_direction" if value > 0 else "adverse",
                "response_bps": value,
                "negative_lag_response_bps": -value,
                "first_move_direction": "same" if value > 0 else "opposite",
                "first_move_delay_ms": float(position + 1),
                "randomization_block": f"block-{locations[position]}",
            }
            for position, value in enumerate(values)
        ]
    )
    config = _config()
    signs = np.random.default_rng(config.randomization_seed).choice(
        np.array([-1.0, 1.0]),
        size=(config.randomization_resamples, 4),
        replace=True,
    )
    response = np.asarray(values, dtype=np.float64)
    oracle_null = (signs[:, locations] * response[np.newaxis, :]).mean(axis=1)
    oracle_observed = float(response.mean())
    expected_p = (
        1.0
        + float(np.count_nonzero(np.abs(oracle_null) >= abs(oracle_observed)))
    ) / (config.randomization_resamples + 1.0)

    result = aggregate_streaming_events(
        _FrameEventSource(rows),
        (
            StrictInterval(
                start=start,
                end=start + timedelta(minutes=30),
                tag="null-boundary",
            ),
        ),
        config,
    )
    selected = result.metrics.loc[
        result.metrics["analysis_kind"].eq("information")
        & result.metrics["asset"].eq("BTC")
        & result.metrics["signal_family"].eq("agg_trade")
        & result.metrics["horizon_ms"].eq(50)
    ].iloc[0]

    assert expected_p == 0.4
    assert selected["empirical_p_value"] == expected_p
    assert selected["fwer_p_value"] == expected_p
    assert selected["fdr_q_value"] == expected_p


def test_empty_source_reports_every_configured_cell_without_quantile_calls() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    intervals = (
        StrictInterval(
            start=start,
            end=start + timedelta(minutes=30),
            tag="empty",
        ),
    )
    config = _config()
    source = _FrameEventSource(pd.DataFrame())

    result = aggregate_streaming_events(source, intervals, config)

    information_count = len(config.assets) * len(SIGNAL_FAMILIES) * len(
        config.horizons_ms
    )
    execution_count = (
        information_count * len(config.execution_scenarios) * 2
    )
    assert len(result.metrics) == information_count + execution_count
    assert len(result.bucket_metrics) == information_count + execution_count
    assert len(result.controls) == information_count * 3
    assert result.metrics["signal_count"].eq(0).all()
    assert result.metrics["evaluable_count"].eq(0).all()
    assert result.controls["sample_count"].eq(0).all()
    assert result.controls["empirical_p_value"].isna().all()
    assert source.quantile_queries == []


def test_queries_match_the_event_spool_projection_contract() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    intervals = (
        StrictInterval(start=start, end=start + timedelta(minutes=30), tag="empty"),
    )
    source = _FrameEventSource(pd.DataFrame())

    aggregate_streaming_events(source, intervals, _config())

    assert [query for query, _columns in source.iteration_queries] == [
        {"row_kind": "information", "signal_role": "primary"},
        {"row_kind": "control", "signal_role": "reverse"},
        {"row_kind": "execution"},
    ]
    bucketed_projections = (
        source.iteration_queries[0][1],
        source.iteration_queries[2][1],
    )
    assert all("time_bucket_ns" in columns for columns in bucketed_projections)
    assert all("time_bucket" not in columns for _query, columns in source.iteration_queries)


def test_exact_quantile_path_preserves_pandas_linear_interpolation() -> None:
    values = [0.0, 1.0, 5.0, 9.0]
    rows = pd.DataFrame(
        [
            {
                "row_kind": "information",
                "signal_role": "primary",
                "asset": "BTC",
                "signal_family": "agg_trade",
                "horizon_ms": 50,
                "time_bucket": pd.Timestamp("2025-01-01T00:00:00Z"),
                "evaluable": True,
                "classification": "same_direction",
                "response_bps": value,
                "negative_lag_response_bps": math.nan,
                "first_move_direction": "same",
                "first_move_delay_ms": value,
                "randomization_block": "block-000",
            }
            for value in values
        ]
    )
    source = _FrameEventSource(rows)
    start = datetime(2025, 1, 1, tzinfo=UTC)
    intervals = (
        StrictInterval(start=start, end=start + timedelta(minutes=30), tag="q"),
    )
    config = _config()

    result = aggregate_streaming_events(source, intervals, config)
    selected = result.metrics.loc[
        result.metrics["analysis_kind"].eq("information")
        & result.metrics["asset"].eq("BTC")
        & result.metrics["signal_family"].eq("agg_trade")
        & result.metrics["horizon_ms"].eq(50)
    ].iloc[0]

    assert selected["q10_move_bps"] == pytest.approx(0.3, abs=1e-12)
    assert selected["median_move_bps"] == pytest.approx(3.0, abs=1e-12)
    assert selected["q90_move_bps"] == pytest.approx(7.8, abs=1e-12)
