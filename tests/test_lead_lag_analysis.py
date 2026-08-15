from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import hyperlab.analysis.lead_lag as lead_lag_module
from hyperlab.analysis.lead_lag import (
    SIGNAL_FAMILIES,
    ExecutionAssumptions,
    LeadLagAnalysis,
    LeadLagCapacityError,
    LeadLagConfig,
    LeadLagDataset,
    StrictInterval,
    _BboSeries,
    _BboState,
    _book_fill,
    _BookSnapshot,
    _build_bbo_signals,
    _build_trade_signals,
    _complete_execution,
    _economic_values,
    _execution_metrics,
    _Fill,
    _prepare_bbo,
    _prepare_trades,
    _response_rows,
    _simulate_taker,
    analyze_lead_lag,
    load_lead_lag_config,
)
from hyperlab.analysis.synthetic import (
    SYNTHETIC_WARNING,
    SyntheticLeadLagFixture,
    generate_synthetic_lead_lag_dataset,
)


def _config(horizons_ms: tuple[int, ...] = (100, 250, 500)) -> LeadLagConfig:
    return LeadLagConfig(
        horizons_ms=horizons_ms,
        randomization_resamples=19,
        execution_scenarios=(
            ExecutionAssumptions(
                name="unit",
                latency_ms=10,
                exit_latency_ms=10,
                maker_timeout_ms=50,
                taker_fee_bps=1.0,
                maker_fee_bps=0.0,
                slippage_bps=0.5,
            ),
        ),
    )


@pytest.fixture(scope="module")
def injected_fixture() -> SyntheticLeadLagFixture:
    return generate_synthetic_lead_lag_dataset(event_count=64, injected_lag_ms=250)


@pytest.fixture(scope="module")
def injected_analysis(injected_fixture: SyntheticLeadLagFixture) -> LeadLagAnalysis:
    return analyze_lead_lag(
        injected_fixture.dataset, injected_fixture.intervals, _config()
    )


@pytest.fixture(scope="module")
def null_analysis() -> LeadLagAnalysis:
    fixture = generate_synthetic_lead_lag_dataset(
        event_count=32, injected_lag_ms=250, null=True
    )
    return analyze_lead_lag(fixture.dataset, fixture.intervals, _config())


def _information_metric(
    analysis: LeadLagAnalysis, asset: str, family: str, horizon_ms: int
) -> pd.Series:
    selected = analysis.metrics.loc[
        analysis.metrics["analysis_kind"].eq("information")
        & analysis.metrics["asset"].eq(asset)
        & analysis.metrics["signal_family"].eq(family)
        & analysis.metrics["horizon_ms"].eq(horizon_ms)
    ]
    assert len(selected) == 1
    return selected.iloc[0]


def test_synthetic_detector_finds_injected_receive_time_lag(
    injected_analysis: LeadLagAnalysis,
) -> None:
    for asset in ("BTC", "ETH"):
        before = _information_metric(injected_analysis, asset, "agg_trade", 100)
        at_lag = _information_metric(injected_analysis, asset, "agg_trade", 250)
        after = _information_metric(injected_analysis, asset, "agg_trade", 500)
        assert before["expected_move_bps"] == pytest.approx(0.0, abs=1e-9)
        assert at_lag["expected_move_bps"] == pytest.approx(5.0, abs=1e-8)
        assert after["expected_move_bps"] == pytest.approx(5.0, abs=1e-8)
        assert at_lag["same_direction_rate"] == 1.0
        assert at_lag["first_move_same_direction_rate"] == 1.0
        assert at_lag["median_first_move_delay_ms"] == 250.0
        assert at_lag["empirical_p_value"] <= 0.1
        assert at_lag["fwer_p_value"] <= 0.1
        assert before["absolute_retention_vs_peak"] == pytest.approx(0.0)
        assert at_lag["absolute_retention_vs_peak"] == pytest.approx(1.0)


def test_synthetic_null_has_no_directional_discovery(
    null_analysis: LeadLagAnalysis,
) -> None:
    selected = null_analysis.metrics.loc[
        null_analysis.metrics["analysis_kind"].eq("information")
        & null_analysis.metrics["signal_family"].eq("agg_trade")
        & null_analysis.metrics["horizon_ms"].isin([250, 500])
    ]
    assert len(selected) == 4
    assert selected["expected_move_bps"].abs().max() < 1e-8
    assert selected["same_direction_rate"].eq(0.5).all()
    assert selected["fwer_p_value"].ge(0.1).all()


def test_output_is_deterministic_and_json_omits_event_detail(
    injected_fixture: SyntheticLeadLagFixture,
    injected_analysis: LeadLagAnalysis,
) -> None:
    repeated = analyze_lead_lag(
        injected_fixture.dataset, injected_fixture.intervals, _config()
    )
    pd.testing.assert_frame_equal(injected_analysis.metrics, repeated.metrics)
    pd.testing.assert_frame_equal(injected_analysis.controls, repeated.controls)
    assert (
        injected_analysis.summary["capacity_preflight"]
        == repeated.summary["capacity_preflight"]
    )
    capacity = injected_analysis.summary["capacity_preflight"]
    assert isinstance(capacity, dict)
    assert capacity["status"] == "PASS"
    assert capacity["method"] == "CONSERVATIVE_LONG_FORM_V1"
    assert capacity["materialization_peak_multiplier"] == 2
    assert capacity["information_row_bytes_bound"] >= 4_096
    assert capacity["execution_row_bytes_bound"] >= 8_192
    assert capacity["execution_rows_upper_bound"] == (
        capacity["primary_information_rows_upper_bound"]
        * len(_config().execution_scenarios)
        * 2
    )
    assert capacity["total_event_rows_upper_bound"] == sum(
        capacity[key]
        for key in (
            "primary_information_rows_upper_bound",
            "reverse_control_rows_upper_bound",
            "execution_rows_upper_bound",
        )
    )
    assert capacity["total_event_rows_upper_bound"] >= len(injected_analysis.events)
    assert capacity["estimated_event_bytes_upper_bound"] > 0
    payload = injected_analysis.as_dict()
    assert "events" not in payload
    assert payload["event_row_count"] == len(injected_analysis.events)
    json.dumps(payload, allow_nan=False, sort_keys=True)


def test_all_variants_and_reverse_event_evidence_are_retained(
    injected_analysis: LeadLagAnalysis,
) -> None:
    information = injected_analysis.metrics.loc[
        injected_analysis.metrics["analysis_kind"].eq("information")
    ]
    assert len(information) == 2 * len(SIGNAL_FAMILIES) * 3
    reverse_rows = injected_analysis.events.loc[
        injected_analysis.events["row_kind"].eq("control")
        & injected_analysis.events["signal_role"].eq("reverse")
    ]
    assert not reverse_rows.empty
    assert reverse_rows["interval_id"].notna().all()


def _simple_dataset(*, simultaneous_move: bool, include_trade: bool = True) -> tuple[
    LeadLagDataset, tuple[StrictInterval, ...], pd.Timestamp
]:
    start = pd.Timestamp("2025-02-01T00:00:00Z")
    signal_time = start + pd.Timedelta(milliseconds=100)
    rows: list[dict[str, object]] = []
    for asset, base in (("BTC", 60_000.0), ("ETH", 3_000.0)):
        for venue in ("binance_usdm", "hyperliquid"):
            for timestamp, multiplier in (
                (start, 1.0),
                (
                    signal_time,
                    1.001 if simultaneous_move and venue == "hyperliquid" else 1.0,
                ),
                (
                    signal_time + pd.Timedelta(milliseconds=50),
                    1.001 if simultaneous_move and venue == "hyperliquid" else 1.0,
                ),
                (
                    signal_time + pd.Timedelta(milliseconds=250),
                    1.001 if simultaneous_move and venue == "hyperliquid" else 1.0,
                ),
            ):
                mid = base * multiplier
                rows.append(
                    {
                        "venue": venue,
                        "asset": asset,
                        "received_time": timestamp,
                        "bid_price": mid - 0.5,
                        "ask_price": mid + 0.5,
                        "bid_quantity": 10.0,
                        "ask_quantity": 10.0,
                    }
                )
    trade_rows = []
    if include_trade:
        trade_rows = [
            {
                "venue": "binance_usdm",
                "asset": asset,
                "received_time": signal_time,
                "price": price,
                "quantity": 1.0,
                "aggressor_side": "buy",
            }
            for asset, price in (("BTC", 60_000.0), ("ETH", 3_000.0))
        ]
    trade_columns = [
        "venue",
        "asset",
        "received_time",
        "price",
        "quantity",
        "aggressor_side",
    ]
    dataset = LeadLagDataset(
        bbo=pd.DataFrame(rows),
        trades=pd.DataFrame(trade_rows, columns=trade_columns),
        l2=pd.DataFrame(
            columns=["venue", "asset", "received_time", "side", "price", "quantity"]
        ),
        clock_sync=pd.DataFrame({"received_time": [start], "valid": [True]}),
        provenance={"kind": "SYNTHETIC", "warning": SYNTHETIC_WARNING},
        source_fingerprint="1" * 64,
    )
    interval = StrictInterval(
        start=start.to_pydatetime(),
        end=(signal_time + pd.Timedelta(milliseconds=400)).to_pydatetime(),
        tag="simple",
    )
    return dataset, (interval,), signal_time


def test_same_receive_time_hyperliquid_move_is_absorbed_in_baseline() -> None:
    dataset, intervals, signal_time = _simple_dataset(simultaneous_move=True)
    analysis = analyze_lead_lag(dataset, intervals, _config((100,)))
    events = analysis.events.loc[
        analysis.events["row_kind"].eq("information")
        & analysis.events["signal_family"].eq("agg_trade")
        & analysis.events["signal_time"].eq(signal_time)
    ]
    assert len(events) == 2
    assert events["baseline_time"].eq(signal_time).all()
    assert events["response_bps"].abs().max() < 1e-10


def test_constant_dataset_has_schema_complete_empty_result() -> None:
    dataset, intervals, _signal_time = _simple_dataset(
        simultaneous_move=False, include_trade=False
    )
    analysis = analyze_lead_lag(dataset, intervals, _config((100,)))
    assert analysis.events.empty
    information = analysis.metrics.loc[
        analysis.metrics["analysis_kind"].eq("information")
    ]
    assert len(information) == 2 * len(SIGNAL_FAMILIES)
    assert information["signal_count"].eq(0).all()
    assert information["inference_status"].eq(
        "NOT_ADMISSIBLE_MINIMUM_EVENTS"
    ).all()
    block_controls = analysis.controls.loc[
        analysis.controls["control_type"].eq("block_sign_randomization")
    ]
    assert len(block_controls) == 2 * len(SIGNAL_FAMILIES)
    assert block_controls["inference_status"].eq(
        "NOT_ADMISSIBLE_MINIMUM_EVENTS"
    ).all()
    assert block_controls["empirical_p_value"].isna().all()
    assert analysis.as_dict()["event_row_count"] == 0


def test_horizon_crossing_interval_is_excluded() -> None:
    dataset, _intervals, signal_time = _simple_dataset(simultaneous_move=False)
    short_interval = StrictInterval(
        start=datetime(2025, 2, 1, tzinfo=UTC),
        end=(signal_time + pd.Timedelta(milliseconds=100)).to_pydatetime(),
        tag="short",
    )
    analysis = analyze_lead_lag(dataset, (short_interval,), _config((250,)))
    selected = analysis.events.loc[
        analysis.events["row_kind"].eq("information")
        & analysis.events["signal_family"].eq("agg_trade")
    ]
    assert len(selected) == 2
    assert selected["evaluable"].eq(False).all()
    assert selected["exclusion_reason"].eq("horizon_crosses_strict_interval").all()
    assert analysis.events["row_kind"].ne("execution").all()


def test_execution_attempt_is_retained_when_only_future_response_is_invalid() -> None:
    dataset, intervals, _signal_time = _simple_dataset(simultaneous_move=False)
    config = replace(_config((100,)), max_book_age_ms=10)
    analysis = analyze_lead_lag(dataset, intervals, config)
    information = analysis.events.loc[
        analysis.events["row_kind"].eq("information")
        & analysis.events["signal_family"].eq("agg_trade")
    ]
    assert information["evaluable"].eq(False).all()
    assert information["exclusion_reason"].eq("stale_response_bbo").all()
    execution = analysis.events.loc[
        analysis.events["row_kind"].eq("execution")
        & analysis.events["signal_family"].eq("agg_trade")
    ]
    assert len(execution) == 4
    assert execution["execution_status"].ne("NOT_ATTEMPTED").all()


def test_prefix_invariance_for_completed_event_windows(
    injected_fixture: SyntheticLeadLagFixture,
    injected_analysis: LeadLagAnalysis,
) -> None:
    cutoff = pd.Timestamp(injected_fixture.signal_times[16])
    source = injected_fixture.dataset
    prefix = LeadLagDataset(
        bbo=source.bbo.loc[source.bbo["received_time"].lt(cutoff)].copy(),
        trades=source.trades.loc[source.trades["received_time"].lt(cutoff)].copy(),
        l2=source.l2.loc[source.l2["received_time"].lt(cutoff)].copy(),
        clock_sync=source.clock_sync.loc[
            source.clock_sync["received_time"].lt(cutoff)
        ].copy(),
        provenance={**source.provenance, "prefix": True},
        source_fingerprint="2" * 64,
    )
    prefix_interval = StrictInterval(
        start=injected_fixture.intervals[0].start,
        end=cutoff.to_pydatetime(),
        tag=injected_fixture.intervals[0].tag,
    )
    prefix_analysis = analyze_lead_lag(prefix, (prefix_interval,), _config((250,)))
    columns = [
        "asset",
        "signal_family",
        "signal_time",
        "signal_value",
        "response_bps",
        "classification",
        "baseline_time",
        "response_state_time",
    ]
    earlier = cutoff - pd.Timedelta(milliseconds=250)
    full_rows = injected_analysis.events.loc[
        injected_analysis.events["row_kind"].eq("information")
        & injected_analysis.events["horizon_ms"].eq(250)
        & injected_analysis.events["signal_time"].lt(earlier),
        columns,
    ].sort_values(["asset", "signal_family", "signal_time"], kind="mergesort")
    prefix_rows = prefix_analysis.events.loc[
        prefix_analysis.events["row_kind"].eq("information")
        & prefix_analysis.events["signal_time"].lt(earlier),
        columns,
    ].sort_values(["asset", "signal_family", "signal_time"], kind="mergesort")
    pd.testing.assert_frame_equal(
        full_rows.reset_index(drop=True), prefix_rows.reset_index(drop=True)
    )


def test_maker_requires_public_trade_and_observed_queue_depletion(
    injected_fixture: SyntheticLeadLagFixture,
    injected_analysis: LeadLagAnalysis,
) -> None:
    no_trade = injected_analysis.events.loc[
        injected_analysis.events["row_kind"].eq("execution")
        & injected_analysis.events["execution_model"].eq("maker")
        & injected_analysis.events["signal_family"].eq("agg_trade")
        & injected_analysis.events["horizon_ms"].eq(250)
    ]
    assert no_trade["execution_status"].eq("MISSED_NO_PUBLIC_TRADE").all()

    source = injected_fixture.dataset
    first_time = pd.Timestamp(injected_fixture.signal_times[0])
    added: list[dict[str, object]] = []
    for asset, quantity in (("BTC", 1.0), ("ETH", 100.0)):
        signal = source.trades.loc[
            source.trades["venue"].eq("binance_usdm")
            & source.trades["asset"].eq(asset)
            & source.trades["received_time"].eq(first_time)
        ].iloc[0]
        entry_time = first_time + pd.Timedelta(milliseconds=50)
        state = source.bbo.loc[
            source.bbo["venue"].eq("hyperliquid")
            & source.bbo["asset"].eq(asset)
            & source.bbo["received_time"].eq(entry_time)
        ].iloc[0]
        positive = signal["aggressor_side"] == "buy"
        added.append(
            {
                "venue": "hyperliquid",
                "asset": asset,
                "received_time": first_time + pd.Timedelta(milliseconds=55),
                "price": state["bid_price"] if positive else state["ask_price"],
                "quantity": quantity,
                "quote_quantity": quantity * float(state["bid_price"]),
                "aggressor_side": "sell" if positive else "buy",
            }
        )
    evidence_dataset = replace(
        source,
        trades=pd.concat([source.trades, pd.DataFrame(added)], ignore_index=True),
        source_fingerprint="3" * 64,
    )
    evidence = analyze_lead_lag(
        evidence_dataset, injected_fixture.intervals, _config((250,))
    )
    first_maker = evidence.events.loc[
        evidence.events["row_kind"].eq("execution")
        & evidence.events["execution_model"].eq("maker")
        & evidence.events["signal_family"].eq("agg_trade")
        & evidence.events["signal_time"].eq(first_time)
    ]
    assert first_maker.loc[first_maker["asset"].eq("BTC"), "execution_status"].eq(
        "MISSED_QUEUE"
    ).all()
    assert first_maker.loc[first_maker["asset"].eq("ETH"), "execution_status"].isin(
        ["FILLED", "PARTIAL"]
    ).all()


def test_partial_entry_and_exit_accounting_preserves_residual_risk() -> None:
    scenario = ExecutionAssumptions(name="partial")
    result = _complete_execution(
        {},
        direction=1,
        baseline_mid=100.0,
        entry_reference_mid=100.0,
        exit_reference_mid=101.0,
        entry_time=pd.Timestamp("2025-01-01T00:00:00Z"),
        exit_time=pd.Timestamp("2025-01-01T00:00:01Z"),
        entry_fill=_Fill(average_price=100.0, base_quantity=0.5, fraction=0.5),
        exit_fill=_Fill(average_price=101.0, base_quantity=0.25, fraction=0.5),
        entry_fee_bps=0.0,
        exit_fee_bps=0.0,
        scenario=scenario,
        entry_side="buy",
        entry_has_slippage=False,
    )
    assert result["execution_status"] == "PARTIAL"
    assert result["matched_fill_fraction"] == pytest.approx(0.25)
    assert result["unclosed_exposure_fraction"] == pytest.approx(0.25)
    assert result["fill_adjusted_net_bps"] == pytest.approx(
        float(result["net_execution_bps"]) * 0.25
    )
    event = {
        "execution_scenario": "unit",
        "execution_model": "maker",
        "asset": "BTC",
        "signal_family": "agg_trade",
        "horizon_ms": 250,
        "execution_status": "PARTIAL",
        "net_execution_bps": result["net_execution_bps"],
        "gross_execution_bps": result["gross_execution_bps"],
        "fill_adjusted_net_bps": result["fill_adjusted_net_bps"],
        "fill_adjusted_gross_bps": result["fill_adjusted_gross_bps"],
        "break_even_move_bps": result["break_even_move_bps"],
        "unclosed_exposure_fraction": result["unclosed_exposure_fraction"],
        "matched_fill_fraction": result["matched_fill_fraction"],
    }
    metrics = _execution_metrics(pd.DataFrame([event]), _config((250,)))
    selected = metrics.loc[
        metrics["execution_scenario"].eq("unit")
        & metrics["execution_model"].eq("maker")
        & metrics["asset"].eq("BTC")
        & metrics["signal_family"].eq("agg_trade")
    ].iloc[0]
    assert selected["residual_exposure_count"] == 1
    assert math.isnan(float(selected["attempt_weighted_expected_net_bps"]))


def test_stale_favorable_l2_depth_is_not_used_for_execution() -> None:
    bbo = _BboState(
        received_time_ns=1_000,
        bid_price=100.0,
        ask_price=101.0,
        bid_quantity=0.5,
        ask_quantity=0.5,
    )
    stale = _BookSnapshot(
        received_time_ns=900,
        bids=((100.0, 10.0),),
        asks=((101.0, 10.0),),
    )
    fill = _book_fill(
        side="buy",
        requested_base_quantity=2.0,
        bbo_row=bbo,
        snapshot=stale,
        max_participation=1.0,
    )
    assert fill.average_price == 101.0
    assert fill.fraction == pytest.approx(0.25)


def test_cost_and_break_even_are_directionally_consistent() -> None:
    long_gross, long_net, long_break_even = _economic_values(
        direction=1,
        baseline_mid=100.0,
        entry_price=100.5,
        exit_price=102.0,
        entry_fee_bps=2.0,
        exit_fee_bps=3.0,
        exit_slippage_bps=1.0,
        adverse_exit_bps=2.0,
    )
    short_gross, short_net, short_break_even = _economic_values(
        direction=-1,
        baseline_mid=100.0,
        entry_price=99.5,
        exit_price=98.0,
        entry_fee_bps=2.0,
        exit_fee_bps=3.0,
        exit_slippage_bps=1.0,
        adverse_exit_bps=2.0,
    )
    assert long_net < long_gross
    assert short_net < short_gross
    assert long_break_even > 0.0
    assert short_break_even > 0.0


def test_execution_economics_are_componentized_and_before_funding(
    injected_analysis: LeadLagAnalysis,
) -> None:
    completed = injected_analysis.events.loc[
        injected_analysis.events["row_kind"].eq("execution")
        & injected_analysis.events["execution_model"].eq("taker")
        & injected_analysis.events["execution_status"].isin(["FILLED", "PARTIAL"])
    ]
    assert not completed.empty
    row = completed.iloc[0]
    assert row["economic_scope"] == "BEFORE_FUNDING"
    assert row["funding_status"] == "NOT_EVALUATED"
    assert row["economic_admissibility"] == (
        "NOT_ADMISSIBLE_FUNDING_NOT_EVALUATED"
    )
    assert row["before_funding_execution_bps"] == row["net_execution_bps"]
    for field in (
        "entry_fee_bps_applied",
        "exit_fee_bps_applied",
        "entry_spread_cost_bps",
        "exit_spread_cost_bps",
        "entry_slippage_cost_bps",
        "exit_slippage_cost_bps",
        "adverse_exit_cost_bps",
    ):
        assert math.isfinite(float(row[field]))
    metric = injected_analysis.metrics.loc[
        injected_analysis.metrics["analysis_kind"].eq("execution")
        & injected_analysis.metrics["execution_model"].eq("taker")
        & injected_analysis.metrics["asset"].eq(row["asset"])
        & injected_analysis.metrics["signal_family"].eq(row["signal_family"])
        & injected_analysis.metrics["horizon_ms"].eq(row["horizon_ms"])
    ].iloc[0]
    assert metric["funding_status"] == "NOT_EVALUATED"
    assert metric["economic_admissibility"] == (
        "NOT_ADMISSIBLE_FUNDING_NOT_EVALUATED"
    )


def _execution_bbo_series(
    timestamps: list[pd.Timestamp], mids: list[float]
) -> _BboSeries:
    times_ns = np.asarray([int(timestamp.value) for timestamp in timestamps], dtype=np.int64)
    values = np.asarray(mids, dtype=float)
    return _BboSeries(
        frame=pd.DataFrame(),
        times_ns=times_ns,
        mids=values,
        bids=values - 0.5,
        asks=values + 0.5,
        bid_quantities=np.full(len(values), 10.0),
        ask_quantities=np.full(len(values), 10.0),
    )


def test_actual_entry_after_horizon_is_missed_and_exit_cannot_cross_interval() -> None:
    start = pd.Timestamp("2025-04-01T00:00:00Z")
    target = start + pd.Timedelta(milliseconds=100)
    interval = StrictInterval(
        start=start.to_pydatetime(),
        end=(start + pd.Timedelta(milliseconds=200)).to_pydatetime(),
        tag="entry",
    )
    late_entry = _execution_bbo_series([start, target], [100.0, 101.0])
    event = {
        "signal_time": start,
        "target_time": target,
        "signal_direction": 1,
        "baseline_mid": 100.0,
    }
    missed = _simulate_taker(
        event,
        ExecutionAssumptions(
            name="late", latency_ms=10, exit_latency_ms=10, maker_timeout_ms=10
        ),
        interval=interval,
        bbo=late_entry,
        l2=None,
        maximum_age_ns=1_000_000_000,
    )
    assert missed["execution_status"] == "MISSED_ENTRY_AFTER_HORIZON"

    short_interval = StrictInterval(
        start=start.to_pydatetime(),
        end=(start + pd.Timedelta(milliseconds=105)).to_pydatetime(),
        tag="exit",
    )
    crossing = _execution_bbo_series(
        [
            start,
            start + pd.Timedelta(milliseconds=20),
            start + pd.Timedelta(milliseconds=80),
            start + pd.Timedelta(milliseconds=110),
        ],
        [100.0, 100.0, 101.0, 101.0],
    )
    unresolved = _simulate_taker(
        {
            **event,
            "target_time": start + pd.Timedelta(milliseconds=80),
        },
        ExecutionAssumptions(
            name="cross", latency_ms=10, exit_latency_ms=30, maker_timeout_ms=10
        ),
        interval=short_interval,
        bbo=crossing,
        l2=None,
        maximum_age_ns=1_000_000_000,
    )
    assert unresolved["execution_status"] == "UNRESOLVED_EXIT_BOOK"
    assert unresolved["entry_fill_fraction"] == 1.0
    assert unresolved["unclosed_exposure_fraction"] == 1.0


def test_feature_windows_never_bridge_disjoint_same_tag_intervals() -> None:
    start = pd.Timestamp("2025-03-01T00:00:00Z")
    intervals = (
        StrictInterval(
            start=start.to_pydatetime(),
            end=(start + pd.Timedelta(milliseconds=500)).to_pydatetime(),
            tag="same-epoch",
        ),
        StrictInterval(
            start=(start + pd.Timedelta(milliseconds=1_000)).to_pydatetime(),
            end=(start + pd.Timedelta(milliseconds=2_000)).to_pydatetime(),
            tag="same-epoch",
        ),
    )
    bbo = _prepare_bbo(
        pd.DataFrame(
            [
                {
                    "venue": "binance_usdm",
                    "asset": "BTC",
                    "received_time": timestamp,
                    "bid_price": mid - 0.5,
                    "ask_price": mid + 0.5,
                    "bid_quantity": 10.0,
                    "ask_quantity": 10.0,
                }
                for timestamp, mid in (
                    (start + pd.Timedelta(milliseconds=400), 100.0),
                    (start + pd.Timedelta(milliseconds=1_100), 110.0),
                    (start + pd.Timedelta(milliseconds=1_200), 110.0),
                )
            ]
        )
    )
    config = _config((250,))
    bbo_signals = _build_bbo_signals(
        bbo,
        venue="binance_usdm",
        assets=("BTC",),
        config=config,
        intervals=intervals,
    )
    assert not bbo_signals

    trades = _prepare_trades(
        pd.DataFrame(
            [
                {
                    "venue": "binance_usdm",
                    "asset": "BTC",
                    "received_time": start + pd.Timedelta(milliseconds=400),
                    "price": 100.0,
                    "quantity": 100.0,
                    "aggressor_side": "buy",
                },
                {
                    "venue": "binance_usdm",
                    "asset": "BTC",
                    "received_time": start + pd.Timedelta(milliseconds=1_100),
                    "price": 110.0,
                    "quantity": 1.0,
                    "aggressor_side": "sell",
                },
            ]
        )
    )
    trade_signals = _build_trade_signals(
        trades,
        venue="binance_usdm",
        assets=("BTC",),
        config=config,
        intervals=intervals,
    )
    second_imbalance = [
        row
        for row in trade_signals
        if row["signal_family"] == "trade_imbalance"
        and row["signal_time"] == start + pd.Timedelta(milliseconds=1_100)
    ]
    assert len(second_imbalance) == 1
    assert second_imbalance[0]["signal_value"] == -1.0


def test_negative_lag_state_never_crosses_strict_interval_gap() -> None:
    start = pd.Timestamp("2025-03-02T00:00:00Z")
    intervals = (
        StrictInterval(
            start=start.to_pydatetime(),
            end=(start + pd.Timedelta(milliseconds=500)).to_pydatetime(),
            tag="same-epoch",
        ),
        StrictInterval(
            start=(start + pd.Timedelta(milliseconds=1_000)).to_pydatetime(),
            end=(start + pd.Timedelta(milliseconds=1_500)).to_pydatetime(),
            tag="same-epoch",
        ),
    )
    rows = []
    for timestamp, mid in (
        (start + pd.Timedelta(milliseconds=400), 100.0),
        (start + pd.Timedelta(milliseconds=1_050), 101.0),
        (start + pd.Timedelta(milliseconds=1_200), 102.0),
    ):
        rows.append(
            {
                "venue": "hyperliquid",
                "asset": "BTC",
                "received_time": timestamp,
                "bid_price": mid - 0.5,
                "ask_price": mid + 0.5,
                "bid_quantity": 10.0,
                "ask_quantity": 10.0,
            }
        )
    signal_time = start + pd.Timedelta(milliseconds=1_100)
    signals = pd.DataFrame(
        [
            {
                "signal_id": "stable",
                "signal_venue": "binance_usdm",
                "asset": "BTC",
                "signal_family": "agg_trade",
                "signal_time": signal_time,
                "signal_value": 1.0,
                "signal_strength": 1.0,
                "signal_direction": 1,
            }
        ]
    )
    events = _response_rows(
        signals,
        _prepare_bbo(pd.DataFrame(rows)),
        intervals,
        _config((100,)),
        response_venue="hyperliquid",
        signal_role="primary",
    )
    assert bool(events.iloc[0]["evaluable"])
    assert math.isnan(float(events.iloc[0]["negative_lag_response_bps"]))


def test_configuration_fail_closed_contracts() -> None:
    with pytest.raises(ValueError, match="BTC and ETH"):
        LeadLagConfig(assets=("BTC",))
    with pytest.raises(ValueError, match="reference_venue"):
        LeadLagConfig(reference_venue="other")
    with pytest.raises(ValueError, match="execution_venue"):
        LeadLagConfig(execution_venue="other")
    with pytest.raises(ValueError, match="combined"):
        ExecutionAssumptions(slippage_bps=6_000.0, adverse_exit_bps=4_000.0)
    with pytest.raises(ValueError, match="evidence hash"):
        ExecutionAssumptions(calibration_status="CALIBRATED")
    calibrated = ExecutionAssumptions(
        calibration_status="CALIBRATED",
        calibration_evidence_hash="a" * 64,
        source="measured-public-replay-study",
    )
    assert calibrated.calibration_status == "CALIBRATED"


def test_synthetic_generator_rejects_unobservable_lag_and_marks_provenance() -> None:
    fixture = generate_synthetic_lead_lag_dataset(event_count=32, seed=7)
    assert fixture.warning == SYNTHETIC_WARNING
    assert fixture.dataset.provenance["warning"] == SYNTHETIC_WARNING
    assert len(fixture.dataset.source_fingerprint) == 64
    null_first = generate_synthetic_lead_lag_dataset(event_count=32, seed=17, null=True)
    null_second = generate_synthetic_lead_lag_dataset(event_count=32, seed=17, null=True)
    assert null_first.dataset.source_fingerprint == null_second.dataset.source_fingerprint
    pd.testing.assert_frame_equal(null_first.dataset.trades, null_second.dataset.trades)
    with pytest.raises(ValueError, match="observable"):
        generate_synthetic_lead_lag_dataset(event_count=32, injected_lag_ms=5_001)
    with pytest.raises(ValueError, match="unique"):
        generate_synthetic_lead_lag_dataset(assets=("BTC", "ETH", "BTC"))


def test_execution_assumption_hash_serialization_is_stable() -> None:
    first = _config()
    second = _config()
    assert first.as_dict() == second.as_dict()
    assert first.config_hash == second.config_hash
    assert len(first.config_hash) == 64
    assert all(character in "0123456789abcdef" for character in first.config_hash)
    assert replace(first, max_event_rows=first.max_event_rows + 1).config_hash != (
        first.config_hash
    )
    assert replace(
        first,
        max_estimated_event_bytes=first.max_estimated_event_bytes + 1,
    ).config_hash != first.config_hash


def test_capacity_limits_require_positive_non_boolean_integers() -> None:
    with pytest.raises(ValueError, match="max_event_rows must be a positive integer"):
        LeadLagConfig(max_event_rows=True)
    with pytest.raises(ValueError, match="max_event_rows must be a positive integer"):
        LeadLagConfig(max_event_rows=0)
    with pytest.raises(
        ValueError, match="max_estimated_event_bytes must be a positive integer"
    ):
        LeadLagConfig(max_estimated_event_bytes=True)
    with pytest.raises(
        ValueError, match="max_estimated_event_bytes must be a positive integer"
    ):
        LeadLagConfig(max_estimated_event_bytes=0)


@pytest.mark.parametrize(
    ("limits", "message"),
    (
        (
            {"max_event_rows": 1, "max_estimated_event_bytes": 10**15},
            r"estimated_event_rows=.*exceeds max_event_rows=1",
        ),
        (
            {"max_event_rows": 10**12, "max_estimated_event_bytes": 1},
            r"estimated_event_bytes=.*exceeds max_estimated_event_bytes=1",
        ),
    ),
)
def test_capacity_preflight_fails_before_long_form_materialization(
    monkeypatch: pytest.MonkeyPatch,
    limits: dict[str, int],
    message: str,
) -> None:
    dataset, intervals, _signal_time = _simple_dataset(simultaneous_move=False)
    allocations: list[str] = []

    def forbidden_allocation(*args: object, **kwargs: object) -> pd.DataFrame:
        allocations.append("called")
        raise AssertionError("long-form allocation began before capacity preflight")

    monkeypatch.setattr(lead_lag_module, "_response_rows", forbidden_allocation)
    monkeypatch.setattr(lead_lag_module, "_execution_rows", forbidden_allocation)
    config = replace(_config((100,)), **limits)
    with pytest.raises(
        LeadLagCapacityError,
        match=message + r".*no response or execution event rows were materialized",
    ):
        analyze_lead_lag(dataset, intervals, config)
    assert allocations == []


def _valid_toml() -> str:
    return "\n".join(
        (
            "[study]",
            'assets = ["BTC", "ETH"]',
            "horizons_ms = [50, 100, 250]",
            "trade_window_ms = 1000",
            "momentum_window_ms = 1000",
            "l2_levels = 5",
            "max_book_age_ms = 1000",
            "minimum_move_bps = 0.0",
            "bucket_minutes = 60",
            "randomization_resamples = 19",
            "randomization_block_ms = 60000",
            "randomization_seed = 42",
            "minimum_events = 30",
            "max_event_rows = 5000000",
            "max_estimated_event_bytes = 8000000000",
            "[[execution_scenarios]]",
            'name = "explicit"',
            "latency_ms = 20",
            "exit_latency_ms = 20",
            "notional_usd = 1000.0",
            "maker_fee_bps = 0.0",
            "taker_fee_bps = 1.0",
            "slippage_bps = 0.5",
            "adverse_exit_bps = 1.0",
            "queue_ahead_multiplier = 1.0",
            "maker_timeout_ms = 100",
            "max_participation = 0.5",
            'calibration_status = "UNCALIBRATED"',
            'source = "test-explicit-assumption"',
        )
    )


def test_toml_loader_supports_study_and_execution_scenarios(tmp_path: Path) -> None:
    path = tmp_path / "study.toml"
    valid = _valid_toml()
    path.write_text(valid, encoding="utf-8")
    loaded = load_lead_lag_config(path)
    assert loaded.horizons_ms == (50, 100, 250)
    assert loaded.execution_scenarios[0].name == "explicit"

    invalid_cases = (
        (valid + "\n[unexpected]\nvalue = 1", "top-level"),
        (valid.replace("minimum_events = 30\n", ""), "missing explicit keys"),
        (valid.replace("horizons_ms = [50, 100, 250]", "horizonz_ms = [250]"), "unknown"),
        (valid.replace("max_participation = 0.5\n", ""), "missing explicit keys"),
        (valid + "\nmisspelled_fee = 1.0", "unknown execution"),
        (valid.split("[[execution_scenarios]]", maxsplit=1)[0], "requires"),
        (
            valid.replace(
                'calibration_status = "UNCALIBRATED"',
                'calibration_status = "CALIBRATED"\ncalibration_evidence_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"',
            ),
            "must remain UNCALIBRATED",
        ),
    )
    for position, (content, match) in enumerate(invalid_cases):
        invalid = tmp_path / f"invalid-{position}.toml"
        invalid.write_text(content, encoding="utf-8")
        with pytest.raises(ValueError, match=match):
            load_lead_lag_config(invalid)


def test_strict_interval_rejects_naive_or_empty_windows() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        StrictInterval(
            start=datetime(2025, 1, 1),
            end=datetime(2025, 1, 1) + timedelta(seconds=1),
            tag="bad",
        )
    with pytest.raises(ValueError, match="after start"):
        StrictInterval(
            start=datetime(2025, 1, 1, tzinfo=UTC),
            end=datetime(2025, 1, 1, tzinfo=UTC),
            tag="bad",
        )
    assert math.isfinite(_config().minimum_move_bps)
