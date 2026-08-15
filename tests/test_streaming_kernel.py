from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from hyperlab.analysis.lead_lag import LeadLagConfig, StrictInterval, analyze_lead_lag
from hyperlab.analysis.streaming_kernel import (
    StreamingKernelError,
    _iso_timestamp,
    run_streaming_kernel,
)
from hyperlab.analysis.streaming_store import PHASE10_EVENT_COLUMNS, ExactTimestampNs, SourceRowSpool
from hyperlab.analysis.synthetic import generate_synthetic_lead_lag_dataset

_EVENT_ORDER = (
    "signal_time",
    "asset",
    "signal_family",
    "horizon_ms",
    "signal_id",
    "row_kind",
    "execution_scenario",
    "execution_model",
)
_TIMESTAMP_COLUMNS = (
    "signal_time",
    "target_time",
    "time_bucket",
    "interval_start",
    "interval_end",
    "baseline_time",
    "response_state_time",
    "entry_time",
    "exit_time",
)


def _spool(path: Path, fixture: object) -> SourceRowSpool:
    dataset = fixture.dataset  # type: ignore[attr-defined]
    spool = SourceRowSpool(path)
    for manifest_order, (kind, frame) in enumerate(
        (("bbo", dataset.bbo), ("trade", dataset.trades), ("l2", dataset.l2))
    ):
        spool.add_rows(
            kind=kind,
            rows=frame.to_dict(orient="records"),
            manifest_order=manifest_order,
            first_row_order=0,
        )
    return spool


def _canonical(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.reindex(columns=PHASE10_EVENT_COLUMNS).copy()
    for name in _TIMESTAMP_COLUMNS:
        result[name] = result[name].map(
            lambda value: (
                pd.Timestamp(value.value, tz="UTC")
                if isinstance(value, ExactTimestampNs)
                else value
            )
        )
        result[name] = pd.to_datetime(result[name], utc=True, format="mixed")
    return result.sort_values(list(_EVENT_ORDER), kind="mergesort").reset_index(
        drop=True
    )


@pytest.mark.parametrize("null", [False, True])
def test_received_time_streaming_kernel_matches_full_pandas_oracle(
    tmp_path: Path, null: bool
) -> None:
    fixture = generate_synthetic_lead_lag_dataset(
        event_count=32,
        injected_lag_ms=250,
        seed=20260815,
        null=null,
    )
    config = LeadLagConfig(
        randomization_resamples=19,
        max_source_rows_per_chunk=10_000,
        max_l2_levels_per_chunk=100_000,
        writer_buffer_rows=17,
    )
    oracle = analyze_lead_lag(fixture.dataset, fixture.intervals, config)
    expected = _canonical(oracle.events)
    rows: list[dict[str, object]] = []
    spool = _spool(tmp_path / "source.sqlite3", fixture)
    try:
        results = [
            run_streaming_kernel(
                spool,
                asset=asset,
                interval=fixture.intervals[0],
                config=config,
                sink=lambda batch: rows.extend(dict(row) for row in batch),
                include_execution=True,
            )
            for asset in config.assets
        ]
    finally:
        spool.close()
    actual = _canonical(pd.DataFrame(rows))

    assert len(actual) == len(expected)
    for name in PHASE10_EVENT_COLUMNS:
        observed = actual[name]
        wanted = expected[name]
        if pd.api.types.is_float_dtype(wanted.dtype):
            pd.testing.assert_series_equal(
                observed,
                wanted,
                check_dtype=False,
                check_exact=False,
                rtol=1e-12,
                atol=1e-12,
            )
        else:
            pd.testing.assert_series_equal(
                observed, wanted, check_dtype=False, check_exact=True
            )
    assert sum(result.counts.information_rows for result in results) == int(
        expected["row_kind"].eq("information").sum()
    )
    assert sum(result.counts.control_rows for result in results) == int(
        expected["row_kind"].eq("control").sum()
    )
    assert sum(result.counts.execution_rows for result in results) == int(
        expected["row_kind"].eq("execution").sum()
    )
    assert all(
        result.high_water.completed_output_rows <= config.writer_buffer_rows
        for result in results
    )


def test_kernel_preserves_nanosecond_timestamp_and_signal_id_input() -> None:
    assert _iso_timestamp(1_735_689_600_000_000_000) == "2025-01-01T00:00:00+00:00"
    assert _iso_timestamp(1_735_689_600_050_000_000) == (
        "2025-01-01T00:00:00.050000+00:00"
    )


def test_complete_equal_time_batch_is_absorbed_into_signal_baseline(
    tmp_path: Path,
) -> None:
    start = pd.Timestamp("2025-02-01T00:00:00Z")
    signal_time = start + pd.Timedelta(milliseconds=100)
    rows: list[dict[str, object]] = []
    for venue in ("binance_usdm", "hyperliquid"):
        for timestamp, multiplier in (
            (start, 1.0),
            (signal_time, 1.001 if venue == "hyperliquid" else 1.0),
            (
                signal_time + pd.Timedelta(milliseconds=50),
                1.001 if venue == "hyperliquid" else 1.0,
            ),
            (
                signal_time + pd.Timedelta(milliseconds=250),
                1.001 if venue == "hyperliquid" else 1.0,
            ),
        ):
            mid = 60_000.0 * multiplier
            rows.append(
                {
                    "venue": venue,
                    "asset": "BTC",
                    "received_time": timestamp,
                    "bid_price": mid - 0.5,
                    "ask_price": mid + 0.5,
                    "bid_quantity": 10.0,
                    "ask_quantity": 10.0,
                }
            )
    spool = SourceRowSpool(tmp_path / "source.sqlite3")
    spool.add_rows(kind="bbo", rows=rows, manifest_order=0, first_row_order=0)
    spool.add_rows(
        kind="trade",
        rows=(
            {
                "venue": "binance_usdm",
                "asset": "BTC",
                "received_time": signal_time,
                "price": 60_000.0,
                "quantity": 1.0,
                "aggressor_side": "buy",
            },
        ),
        manifest_order=1,
        first_row_order=0,
    )
    interval = StrictInterval(
        start=start.to_pydatetime(),
        end=(signal_time + pd.Timedelta(milliseconds=400)).to_pydatetime(),
        tag="equal-time-boundary",
    )
    config = LeadLagConfig(horizons_ms=(100,), randomization_resamples=19)
    emitted: list[dict[str, object]] = []
    try:
        run_streaming_kernel(
            spool,
            asset="BTC",
            interval=interval,
            config=config,
            sink=lambda batch: emitted.extend(dict(row) for row in batch),
            include_execution=False,
        )
    finally:
        spool.close()
    selected = [
        row
        for row in emitted
        if row["row_kind"] == "information"
        and row["signal_family"] == "agg_trade"
        and int(row["horizon_ms"]) == 100
    ]
    assert len(selected) == 1
    assert selected[0]["baseline_time"] == ExactTimestampNs(int(signal_time.value))
    assert abs(float(selected[0]["response_bps"])) < 1e-10
    assert _iso_timestamp(1_735_689_600_000_000_123) == (
        "2025-01-01T00:00:00.000000123+00:00"
    )


def test_kernel_fails_on_actual_pending_response_bound(tmp_path: Path) -> None:
    fixture = generate_synthetic_lead_lag_dataset(event_count=32, seed=11)
    spool = _spool(tmp_path / "source.sqlite3", fixture)
    config = replace(
        LeadLagConfig(randomization_resamples=19),
        max_pending_response_states=1,
    )
    try:
        with pytest.raises(StreamingKernelError, match="pending response states"):
            run_streaming_kernel(
                spool,
                asset="BTC",
                interval=fixture.intervals[0],
                config=config,
                sink=lambda _rows: None,
                include_execution=False,
            )
    finally:
        spool.close()


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"max_source_rows_per_chunk": 100}, "retained rolling source state"),
        (
            {"max_l2_frame_levels": 6, "max_l2_levels_per_chunk": 6},
            "retained rolling L2 levels",
        ),
    ),
)
def test_kernel_fails_on_actual_retained_state_bounds(
    tmp_path: Path, changes: dict[str, int], message: str
) -> None:
    fixture = generate_synthetic_lead_lag_dataset(event_count=32, seed=23)
    spool = _spool(tmp_path / "source.sqlite3", fixture)
    config = replace(
        LeadLagConfig(randomization_resamples=19),
        max_simultaneous_batch_rows=1_000,
        **changes,
    )
    try:
        with pytest.raises(StreamingKernelError, match=message):
            run_streaming_kernel(
                spool,
                asset="BTC",
                interval=fixture.intervals[0],
                config=config,
                sink=lambda _rows: None,
                include_execution=False,
            )
    finally:
        spool.close()


def test_kernel_fails_closed_when_required_bbo_venue_is_absent(
    tmp_path: Path,
) -> None:
    fixture = generate_synthetic_lead_lag_dataset(event_count=32, seed=29)
    spool = _spool(tmp_path / "source.sqlite3", fixture)
    spool.connection.execute(
        "DELETE FROM source_rows WHERE kind = 'bbo' AND venue = 'hyperliquid'"
    )
    config = LeadLagConfig(randomization_resamples=19)
    try:
        with pytest.raises(StreamingKernelError, match="missing required BBO"):
            run_streaming_kernel(
                spool,
                asset="BTC",
                interval=fixture.intervals[0],
                config=config,
                sink=lambda _rows: None,
                include_execution=False,
            )
    finally:
        spool.close()
