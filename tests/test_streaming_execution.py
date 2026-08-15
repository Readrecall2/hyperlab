from __future__ import annotations

import math
from dataclasses import dataclass
from numbers import Real

import numpy as np
import pandas as pd
import pytest

import hyperlab.analysis.lead_lag as oracle
from hyperlab.analysis.lead_lag import (
    ExecutionAssumptions,
    LeadLagConfig,
    StrictInterval,
)
from hyperlab.analysis.streaming_execution import (
    BboSample,
    L2Snapshot,
    StreamingExecutionError,
    TradeSample,
    TupleExecutionTimelines,
    model_execution_events,
    model_scenario_execution_events,
)
from hyperlab.analysis.streaming_store import PHASE10_EVENT_COLUMNS, ExactTimestampNs

_START = pd.Timestamp("2025-06-01T00:00:00Z")
_SIGNAL = _START + pd.Timedelta(seconds=1, nanoseconds=123)
_TARGET = _SIGNAL + pd.Timedelta(milliseconds=100)
_INTERVAL_END = _START + pd.Timedelta(seconds=5)
_MAXIMUM_AGE_NS = 1_000_000_000


def _information_event(*, direction: int) -> dict[str, object]:
    baseline_mid = 100.5
    return {
        "signal_id": "BTC:agg_trade:exact-ns",
        "signal_venue": "binance_usdm",
        "asset": "BTC",
        "signal_family": "agg_trade",
        "signal_time": _SIGNAL,
        "signal_value": float(direction),
        "signal_strength": 1.0,
        "signal_direction": direction,
        "signal_role": "primary",
        "time_axis": "received_time",
        "source_time_status": "NOT_ADMISSIBLE_NO_SYMMETRIC_HL_CLOCK_CALIBRATION",
        "horizon_ms": 100,
        "target_time": _TARGET,
        "time_bucket": _SIGNAL.floor("1h"),
        "interval_tag": "fixture",
        "interval_id": "fixture-id",
        "interval_start": _START,
        "interval_end": _INTERVAL_END,
        "evaluable": True,
        "exclusion_reason": None,
        "baseline_time": _SIGNAL,
        "response_state_time": _TARGET,
        "baseline_mid": baseline_mid,
        "response_mid": 101.0,
        "response_bps": 49.627175655,
        "negative_lag_response_bps": 0.0,
        "first_move_delay_ms": 10.0,
        "first_move_direction": "same_direction",
        "classification": "same_direction",
        "baseline_bid": 100.0,
        "baseline_ask": 101.0,
        "baseline_bid_quantity": 5.0,
        "baseline_ask_quantity": 5.0,
        "response_bid": 100.5,
        "response_ask": 101.5,
        "randomization_block": "fixture-id:0",
        "row_kind": "information",
    }


@dataclass(frozen=True)
class _Case:
    direction: int
    bbo: tuple[BboSample, ...]
    l2: tuple[L2Snapshot, ...]
    trades: tuple[TradeSample, ...]
    scenario: ExecutionAssumptions


def _case(name: str) -> _Case:
    entry_ns = int((_SIGNAL + pd.Timedelta(milliseconds=10)).value)
    exit_ns = int((_TARGET + pd.Timedelta(milliseconds=10)).value)
    if name == "filled":
        bbo = (
            BboSample(entry_ns, 100.0, 101.0, 2.0, 2.0),
            BboSample(exit_ns, 102.0, 103.0, 2.0, 2.0),
        )
        l2 = (
            L2Snapshot(
                entry_ns,
                bids=((100.0, 20.0), (99.0, 20.0)),
                asks=((101.0, 20.0), (102.0, 20.0)),
            ),
            L2Snapshot(
                exit_ns,
                bids=((102.0, 20.0), (101.0, 20.0)),
                asks=((103.0, 20.0), (104.0, 20.0)),
            ),
        )
        trades = (
            TradeSample(entry_ns + 1, 100.0, 0.5, -1),
            TradeSample(entry_ns + 20_000_000, 100.0, 20.0, -1),
        )
        scenario = ExecutionAssumptions(
            name=name,
            latency_ms=10,
            exit_latency_ms=10,
            notional_usd=1_000.0,
            maker_fee_bps=-0.2,
            taker_fee_bps=1.5,
            slippage_bps=0.7,
            adverse_exit_bps=0.3,
            queue_ahead_multiplier=0.25,
            maker_timeout_ms=80,
            max_participation=0.75,
        )
        return _Case(1, bbo, l2, trades, scenario)
    if name == "partial":
        bbo = (
            BboSample(entry_ns, 100.0, 101.0, 2.0, 2.0),
            BboSample(exit_ns, 97.0, 98.0, 1.0, 1.0),
        )
        trades = (TradeSample(entry_ns + 20_000_000, 101.0, 2.0, 1),)
        scenario = ExecutionAssumptions(
            name=name,
            latency_ms=10,
            exit_latency_ms=10,
            notional_usd=1_000.0,
            maker_fee_bps=0.0,
            taker_fee_bps=1.0,
            slippage_bps=0.5,
            adverse_exit_bps=0.25,
            queue_ahead_multiplier=0.0,
            maker_timeout_ms=80,
            max_participation=1.0,
        )
        return _Case(-1, bbo, (), trades, scenario)
    if name == "missed":
        scenario = ExecutionAssumptions(
            name=name,
            latency_ms=10,
            exit_latency_ms=10,
            maker_timeout_ms=80,
        )
        return _Case(1, (), (), (), scenario)
    if name == "unresolved":
        bbo = (BboSample(entry_ns, 100.0, 101.0, 20.0, 20.0),)
        trades = (TradeSample(entry_ns + 20_000_000, 100.0, 20.0, -1),)
        scenario = ExecutionAssumptions(
            name=name,
            latency_ms=10,
            exit_latency_ms=10,
            maker_timeout_ms=80,
            queue_ahead_multiplier=0.0,
        )
        return _Case(1, bbo, (), trades, scenario)
    raise AssertionError(f"unknown case {name}")


def _oracle_inputs(
    case: _Case,
) -> tuple[object, object, object]:
    bbo = None
    if case.bbo:
        bids = np.asarray([item.bid_price for item in case.bbo], dtype=float)
        asks = np.asarray([item.ask_price for item in case.bbo], dtype=float)
        bbo = oracle._BboSeries(
            frame=pd.DataFrame(),
            times_ns=np.asarray(
                [item.received_time_ns for item in case.bbo], dtype=np.int64
            ),
            mids=(bids + asks) / 2.0,
            bids=bids,
            asks=asks,
            bid_quantities=np.asarray(
                [item.bid_quantity for item in case.bbo], dtype=float
            ),
            ask_quantities=np.asarray(
                [item.ask_quantity for item in case.bbo], dtype=float
            ),
        )
    l2 = None
    if case.l2:
        l2 = oracle._L2Series(
            times_ns=np.asarray(
                [item.received_time_ns for item in case.l2], dtype=np.int64
            ),
            snapshots=tuple(
                oracle._BookSnapshot(
                    received_time_ns=item.received_time_ns,
                    bids=item.bids,
                    asks=item.asks,
                )
                for item in case.l2
            ),
        )
    trades = None
    if case.trades:
        trades = oracle._TradeSeries(
            times_ns=np.asarray(
                [item.received_time_ns for item in case.trades], dtype=np.int64
            ),
            prices=np.asarray([item.price for item in case.trades], dtype=float),
            quantities=np.asarray(
                [item.quantity for item in case.trades], dtype=float
            ),
            directions=np.asarray(
                [item.direction for item in case.trades], dtype=np.int8
            ),
        )
    return bbo, l2, trades


def _assert_scalar_row_equal(
    actual: dict[str, object], expected: dict[str, object]
) -> None:
    assert set(actual) == set(expected) == set(PHASE10_EVENT_COLUMNS)
    for key in PHASE10_EVENT_COLUMNS:
        left = actual[key]
        right = expected[key]
        if pd.isna(left) and pd.isna(right):
            continue
        if key in {
            "signal_time",
            "target_time",
            "time_bucket",
            "interval_start",
            "interval_end",
            "baseline_time",
            "response_state_time",
            "entry_time",
            "exit_time",
        }:
            left_ns = left.value if isinstance(left, ExactTimestampNs) else pd.Timestamp(left).value
            right_ns = (
                right.value
                if isinstance(right, ExactTimestampNs)
                else pd.Timestamp(right).value
            )
            assert int(left_ns) == int(right_ns), key
            continue
        if isinstance(left, Real) and isinstance(right, Real):
            assert float(left) == pytest.approx(
                float(right), rel=1e-12, abs=1e-12
            ), key
        else:
            assert left == right, key


@pytest.mark.parametrize("case_name", ["filled", "partial", "missed", "unresolved"])
def test_scalar_execution_matches_pandas_oracle_for_every_lifecycle(
    case_name: str,
) -> None:
    case = _case(case_name)
    event = _information_event(direction=case.direction)
    timelines = TupleExecutionTimelines(case.bbo, case.l2, case.trades)
    actual = model_scenario_execution_events(
        event,
        timelines,
        int(_START.value),
        int(_INTERVAL_END.value),
        case.scenario,
        maximum_age_ns=_MAXIMUM_AGE_NS,
    )
    oracle_bbo, oracle_l2, oracle_trades = _oracle_inputs(case)
    interval = StrictInterval(
        start=_START.to_pydatetime(),
        end=_INTERVAL_END.to_pydatetime(),
        tag="fixture",
    )
    expected = (
        oracle._simulate_taker(
            event,
            case.scenario,
            interval=interval,
            bbo=oracle_bbo,
            l2=oracle_l2,
            maximum_age_ns=_MAXIMUM_AGE_NS,
        ),
        oracle._simulate_maker(
            event,
            case.scenario,
            interval=interval,
            bbo=oracle_bbo,
            trades=oracle_trades,
            l2=oracle_l2,
            maximum_age_ns=_MAXIMUM_AGE_NS,
        ),
    )
    assert [row["execution_status"] for row in actual] == [
        row["execution_status"] for row in expected
    ]
    for actual_row, expected_row in zip(actual, expected, strict=True):
        _assert_scalar_row_equal(actual_row, expected_row)


def test_exact_nanosecond_fill_times_and_maker_trade_boundaries_match_oracle() -> None:
    entry_ns = int((_SIGNAL + pd.Timedelta(milliseconds=10)).value)
    deadline_ns = int((_SIGNAL + pd.Timedelta(milliseconds=60)).value)
    exit_ns = int((_TARGET + pd.Timedelta(milliseconds=10)).value)
    scenario = ExecutionAssumptions(
        name="boundaries",
        latency_ms=10,
        exit_latency_ms=10,
        notional_usd=100.0,
        queue_ahead_multiplier=1.0,
        maker_timeout_ms=50,
    )
    case = _Case(
        direction=1,
        bbo=(
            BboSample(entry_ns, 100.0, 101.0, 2.0, 20.0),
            BboSample(exit_ns, 102.0, 103.0, 20.0, 20.0),
        ),
        l2=(),
        trades=(
            # A same-time trade is not public evidence after the entry book.
            TradeSample(entry_ns, 100.0, 100.0, -1),
            # This consumes the displayed queue but cannot fill the order.
            TradeSample(entry_ns + 1, 100.0, 2.0, -1),
            # The maker deadline is inclusive and this trade supplies the fill.
            TradeSample(deadline_ns, 100.0, 2.0, -1),
        ),
        scenario=scenario,
    )
    event = _information_event(direction=1)
    rows = model_scenario_execution_events(
        event,
        TupleExecutionTimelines(case.bbo, case.l2, case.trades),
        int(_START.value),
        int(_INTERVAL_END.value),
        scenario,
        maximum_age_ns=_MAXIMUM_AGE_NS,
    )
    maker = rows[1]
    assert maker["execution_status"] == "FILLED"
    assert maker["entry_time"] == ExactTimestampNs(deadline_ns)
    assert rows[0]["entry_time"] == ExactTimestampNs(entry_ns)
    assert entry_ns % 1_000 == 123


def test_configured_scenario_order_is_taker_then_maker_without_population_state() -> None:
    filled = _case("filled")
    second = ExecutionAssumptions(
        name="second",
        latency_ms=100,
        exit_latency_ms=10,
        maker_timeout_ms=10,
    )
    config = LeadLagConfig(
        horizons_ms=(100,),
        randomization_resamples=19,
        execution_scenarios=(filled.scenario, second),
    )
    rows = model_execution_events(
        _information_event(direction=1),
        TupleExecutionTimelines(filled.bbo, filled.l2, filled.trades),
        int(_START.value),
        int(_INTERVAL_END.value),
        config,
    )
    assert [
        (row["execution_scenario"], row["execution_model"]) for row in rows
    ] == [
        ("filled", "taker"),
        ("filled", "maker"),
        ("second", "taker"),
        ("second", "maker"),
    ]
    assert [row["execution_status"] for row in rows[2:]] == [
        "MISSED_LATENCY",
        "MISSED_LATENCY",
    ]


def test_state_machine_epoch_nanoseconds_are_accepted_without_timezone_loss() -> None:
    filled = _case("filled")
    event = _information_event(direction=1)
    event["signal_time"] = int(_SIGNAL.value)
    event["target_time"] = int(_TARGET.value)
    rows = model_scenario_execution_events(
        event,
        TupleExecutionTimelines(filled.bbo, filled.l2, filled.trades),
        int(_START.value),
        int(_INTERVAL_END.value),
        filled.scenario,
        maximum_age_ns=_MAXIMUM_AGE_NS,
    )
    assert rows[0]["signal_time"] == int(_SIGNAL.value)
    assert rows[0]["entry_time"] == ExactTimestampNs(
        int((_SIGNAL + pd.Timedelta(milliseconds=10)).value)
    )


@pytest.mark.parametrize(
    "exclusion_reason",
    ["missing_baseline_bbo", "stale_baseline_bbo"],
)
def test_missing_or_stale_information_baseline_does_not_cancel_execution_oracle(
    exclusion_reason: str,
) -> None:
    case = _case("filled")
    event = _information_event(direction=case.direction)
    event.update(
        evaluable=False,
        exclusion_reason=exclusion_reason,
        baseline_time=None,
        baseline_mid=math.nan,
        response_state_time=None,
        response_mid=math.nan,
        response_bps=math.nan,
        classification="not_evaluable",
    )
    actual = model_scenario_execution_events(
        event,
        TupleExecutionTimelines(case.bbo, case.l2, case.trades),
        int(_START.value),
        int(_INTERVAL_END.value),
        case.scenario,
        maximum_age_ns=_MAXIMUM_AGE_NS,
    )
    oracle_bbo, oracle_l2, oracle_trades = _oracle_inputs(case)
    interval = StrictInterval(
        start=_START.to_pydatetime(),
        end=_INTERVAL_END.to_pydatetime(),
        tag="fixture",
    )
    expected = (
        oracle._simulate_taker(
            event,
            case.scenario,
            interval=interval,
            bbo=oracle_bbo,
            l2=oracle_l2,
            maximum_age_ns=_MAXIMUM_AGE_NS,
        ),
        oracle._simulate_maker(
            event,
            case.scenario,
            interval=interval,
            bbo=oracle_bbo,
            trades=oracle_trades,
            l2=oracle_l2,
            maximum_age_ns=_MAXIMUM_AGE_NS,
        ),
    )
    for actual_row, expected_row in zip(actual, expected, strict=True):
        _assert_scalar_row_equal(actual_row, expected_row)
        assert actual_row["execution_status"] == "FILLED"
        assert math.isnan(float(actual_row["break_even_move_bps"]))
        assert math.isfinite(float(actual_row["before_cost_mid_move_bps"]))


@pytest.mark.parametrize("baseline_mid", [None, math.nan, math.inf, -math.inf])
def test_nullable_nonfinite_baseline_only_nulls_baseline_dependent_economics(
    baseline_mid: object,
) -> None:
    case = _case("filled")
    event = _information_event(direction=case.direction)
    event["baseline_mid"] = baseline_mid
    rows = model_scenario_execution_events(
        event,
        TupleExecutionTimelines(case.bbo, case.l2, case.trades),
        int(_START.value),
        int(_INTERVAL_END.value),
        case.scenario,
        maximum_age_ns=_MAXIMUM_AGE_NS,
    )
    for row in rows:
        assert row["execution_status"] == "FILLED"
        assert math.isnan(float(row["break_even_move_bps"]))
        assert math.isfinite(float(row["before_cost_mid_move_bps"]))
        assert math.isfinite(float(row["net_execution_bps"]))


def test_fail_closed_on_cross_interval_event_or_lookahead_protocol() -> None:
    filled = _case("filled")
    event = _information_event(direction=1)
    with pytest.raises(
        StreamingExecutionError,
        match="lifecycle must fit",
    ):
        model_scenario_execution_events(
            event,
            TupleExecutionTimelines(filled.bbo, filled.l2, filled.trades),
            int(_START.value),
            int(_TARGET.value),
            filled.scenario,
            maximum_age_ns=_MAXIMUM_AGE_NS,
        )

    class _LookaheadTimeline:
        def first_bbo_at_or_after(
            self, received_time_ns: int, max_age_ns: int, interval_end_ns: int
        ) -> BboSample | None:
            del max_age_ns, interval_end_ns
            return BboSample(received_time_ns - 1, 100.0, 101.0, 1.0, 1.0)

        def l2_at_or_before(
            self, received_time_ns: int, max_age_ns: int
        ) -> L2Snapshot | None:
            del received_time_ns, max_age_ns
            return None

        def iter_trades(
            self, start_exclusive_ns: int, end_inclusive_ns: int
        ) -> object:
            del start_exclusive_ns, end_inclusive_ns
            return iter(())

    with pytest.raises(StreamingExecutionError, match="before its query"):
        model_scenario_execution_events(
            event,
            _LookaheadTimeline(),  # type: ignore[arg-type]
            int(_START.value),
            int(_INTERVAL_END.value),
            filled.scenario,
            maximum_age_ns=_MAXIMUM_AGE_NS,
        )


def test_tuple_timelines_reject_ambiguous_nonterminal_bbo_batches() -> None:
    timestamp_ns = int(_SIGNAL.value)
    with pytest.raises(StreamingExecutionError, match="strictly increasing"):
        TupleExecutionTimelines(
            bbo=(
                BboSample(timestamp_ns, 100.0, 101.0, 1.0, 1.0),
                BboSample(timestamp_ns, 101.0, 102.0, 1.0, 1.0),
            )
        )


def test_all_float_nulls_remain_exact_oracle_null_semantics() -> None:
    missed = _case("missed")
    rows = model_scenario_execution_events(
        _information_event(direction=missed.direction),
        TupleExecutionTimelines(),
        int(_START.value),
        int(_INTERVAL_END.value),
        missed.scenario,
        maximum_age_ns=_MAXIMUM_AGE_NS,
    )
    for row in rows:
        assert row["execution_status"] == "MISSED_ENTRY_BOOK"
        assert math.isnan(float(row["entry_price"]))
        assert pd.isna(row["entry_time"])
