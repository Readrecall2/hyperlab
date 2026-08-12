from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from hyperlab.backtest.attribution import (
    AttributionReconciliationError,
    aggregate_pnl,
    causal_regimes,
)
from hyperlab.backtest.bootstrap import (
    InsufficientBootstrapSampleWarning,
    block_bootstrap_ci,
    moving_block_indices,
)
from hyperlab.backtest.report import write_comparison_report
from hyperlab.models import BacktestMetrics, BacktestResult


def test_moving_block_indices_are_contiguous_and_seed_reproducible() -> None:
    first = moving_block_indices(11, block_size=4, n_resamples=8, seed=17)
    second = moving_block_indices(11, block_size=4, n_resamples=8, seed=17)
    different = moving_block_indices(11, block_size=4, n_resamples=8, seed=18)

    assert np.array_equal(first, second)
    assert not np.array_equal(first, different)
    assert first.shape == (8, 11)
    assert int(first.min()) >= 0
    assert int(first.max()) < 11
    for row in first:
        for start in range(0, len(row), 4):
            block = row[start : start + 4]
            assert np.array_equal(np.diff(block), np.ones(max(len(block) - 1, 0), dtype=int))


def test_block_bootstrap_is_reproducible_and_confidence_is_configurable() -> None:
    values = pd.Series(
        np.sin(np.arange(80, dtype=float) / 4.0),
        index=pd.date_range("2026-01-01", periods=80, freq="1h", tz="UTC"),
    )
    narrow = block_bootstrap_ci(
        values,
        block_size=5,
        n_resamples=500,
        confidence_level=0.50,
        seed=91,
    )
    wide = block_bootstrap_ci(
        values,
        block_size=5,
        n_resamples=500,
        confidence_level=0.95,
        seed=91,
    )
    repeat = block_bootstrap_ci(
        values,
        block_size=5,
        n_resamples=500,
        confidence_level=0.95,
        seed=91,
    )

    assert wide.bootstrap_statistics == repeat.bootstrap_statistics
    assert wide.lower == repeat.lower
    assert wide.upper == repeat.upper
    assert wide.lower <= narrow.lower <= narrow.upper <= wide.upper
    assert wide.lower <= wide.estimate <= wide.upper


def test_constant_sample_has_a_degenerate_interval() -> None:
    values = pd.Series(
        np.full(30, -2.5),
        index=pd.date_range("2026-01-01", periods=30, freq="1h", tz="UTC"),
    )
    result = block_bootstrap_ci(
        values,
        block_size=5,
        n_resamples=100,
        confidence_level=0.90,
        seed=3,
    )

    assert result.estimate == pytest.approx(-2.5)
    assert result.lower == pytest.approx(-2.5)
    assert result.upper == pytest.approx(-2.5)
    assert result.standard_error == pytest.approx(0.0)
    assert set(result.bootstrap_statistics) == {-2.5}


def test_small_effective_sample_is_explicitly_warned() -> None:
    with pytest.warns(InsufficientBootstrapSampleWarning, match="insufficient"):
        values = pd.Series(
            np.arange(8, dtype=float),
            index=pd.date_range("2026-01-01", periods=8, freq="1h", tz="UTC"),
        )
        result = block_bootstrap_ci(
            values,
            block_size=4,
            n_resamples=50,
            seed=2,
        )

    assert result.insufficient_sample is True
    assert result.effective_blocks == pytest.approx(2.0)


def test_block_bootstrap_verifies_a_regular_ordered_utc_index() -> None:
    index = pd.date_range("2026-01-01", periods=24, freq="1h", tz="UTC")
    values = pd.Series(np.arange(24, dtype=float), index=index)

    result = block_bootstrap_ci(values, block_size=4, n_resamples=50, seed=4)

    assert result.time_index_verified is True
    assert result.cadence == "0 days 01:00:00"

    with pytest.raises(ValueError, match="requires an explicit UTC time index"):
        block_bootstrap_ci(values.to_numpy(), block_size=4, n_resamples=50, seed=4)


@pytest.mark.parametrize(
    ("index", "message"),
    [
        (pd.date_range("2026-01-01", periods=10, freq="1h"), "must use UTC"),
        (
            pd.DatetimeIndex(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T02:00:00Z",
                    "2026-01-01T01:00:00Z",
                ]
            ),
            "strictly increasing",
        ),
        (
            pd.DatetimeIndex(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T01:00:00Z",
                    "2026-01-01T03:00:00Z",
                ]
            ),
            "gaps or an irregular cadence",
        ),
    ],
)
def test_block_bootstrap_rejects_non_causal_time_axes(
    index: pd.DatetimeIndex,
    message: str,
) -> None:
    values = pd.Series(np.arange(len(index), dtype=float), index=index)
    with pytest.raises(ValueError, match=message):
        block_bootstrap_ci(values, block_size=1, n_resamples=20, seed=4)


def _report_result(*, oos: bool) -> BacktestResult:
    index = pd.date_range("2026-01-01", periods=24, freq="1h", tz="UTC")
    net = pd.Series(np.sin(np.arange(24, dtype=float)) / 1_000.0, index=index)
    returns = pd.DataFrame({"net_return": net}, index=index)
    equity = (1.0 + net).cumprod().rename("report-test")
    weights = pd.DataFrame({"HL:BTC:perp": 0.0}, index=index)
    metrics = BacktestMetrics(
        total_return=float(equity.iloc[-1] - 1.0),
        annualized_return=0.0,
        annualized_volatility=0.0,
        sharpe=0.0,
        max_drawdown=0.0,
        calmar=0.0,
        win_day_rate=0.0,
        worst_day=0.0,
        turnover=0.0,
        time_in_market=0.0,
        max_gross_leverage=0.0,
        max_net_exposure=0.0,
        price_contribution=0.0,
        funding_contribution=0.0,
        cost_contribution=0.0,
    )
    diagnostics: dict[str, object] = {"audit_status": "SYNTHETIC", "warnings": []}
    if oos:
        diagnostics["evaluation_split"] = "walk_forward_oos"
    return BacktestResult(
        "report-test",
        "test",
        returns,
        equity,
        weights,
        metrics,
        diagnostics,
    )


def test_report_refuses_bootstrap_interval_without_explicit_oos_tag(tmp_path: Path) -> None:
    result = _report_result(oos=False)
    report_path = write_comparison_report(
        [result],
        tmp_path,
        bootstrap_block_size=4,
        bootstrap_resamples=50,
        bootstrap_seed=12,
    )

    assert result.uncertainty["available"] is False
    assert result.uncertainty["status"] == "UNAVAILABLE_NOT_OOS"
    assert result.uncertainty["seed"] == 12
    assert "non étiqueté OOS" in report_path.read_text(encoding="utf-8")
    summary = json.loads((tmp_path / "latest_summary.json").read_text(encoding="utf-8"))
    assert summary["strategies"][0]["uncertainty"]["lower"] is None


def test_report_bootstraps_only_an_explicit_regular_oos_series(tmp_path: Path) -> None:
    result = _report_result(oos=True)
    write_comparison_report(
        [result],
        tmp_path,
        bootstrap_block_size=4,
        bootstrap_resamples=50,
        bootstrap_seed=12,
    )

    assert result.uncertainty["available"] is True
    assert result.uncertainty["status"] == "AVAILABLE_OOS"
    assert result.uncertainty["time_index_verified"] is True
    assert result.uncertainty["cadence"] == "0 days 01:00:00"


@pytest.mark.parametrize(
    ("values", "kwargs", "error"),
    [
        ([], {"block_size": 1}, ValueError),
        ([1.0, math.nan], {"block_size": 1}, ValueError),
        ([[1.0], [2.0]], {"block_size": 1}, ValueError),
        ([1.0, 2.0], {"block_size": 0}, ValueError),
        ([1.0, 2.0], {"block_size": 3}, ValueError),
        ([1.0, 2.0], {"block_size": 1, "n_resamples": 1}, ValueError),
        ([1.0, 2.0], {"block_size": 1, "confidence_level": 1.0}, ValueError),
        ([1.0, 2.0], {"block_size": 1, "seed": -1}, ValueError),
        ([1.0, 2.0], {"block_size": True}, TypeError),
    ],
)
def test_block_bootstrap_rejects_invalid_inputs(
    values: object,
    kwargs: dict[str, object],
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        block_bootstrap_ci(values, **kwargs)  # type: ignore[arg-type]


def _ledger() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": [
                "2026-01-31T23:30:00+00:00",
                "2026-01-31T23:30:00+00:00",
                "2026-01-31T23:30:00-02:00",
                "2026-01-31T23:30:00-02:00",
                "2026-02-15T12:00:00+00:00",
                "2026-02-15T12:00:00+00:00",
            ],
            "asset": ["BTC", "BTC", "ETH", "ETH", "BTC", "BTC"],
            "regime": ["calm", "calm", "chaos", "chaos", "trend_up", "trend_up"],
            "size_usd": [500.0, 500.0, 15_000.0, 15_000.0, 150_000.0, 150_000.0],
            "component": ["price", "fees", "price", "fees", "price", "fees"],
            "pnl": [10.0, -1.0, -4.0, -2.0, 20.0, -3.0],
        }
    )


def test_long_ledger_aggregations_reconcile_by_every_required_dimension() -> None:
    report = aggregate_pnl(_ledger())

    assert report.total_pnl == pytest.approx(20.0)
    assert report.row_count == 6
    assert report.component_totals.to_dict() == {"fees": -6.0, "price": 26.0}
    assert report.by_asset.at["BTC", "total_pnl"] == pytest.approx(26.0)
    assert report.by_asset.at["ETH", "total_pnl"] == pytest.approx(-6.0)
    assert report.by_month.at["2026-01", "total_pnl"] == pytest.approx(9.0)
    assert report.by_month.at["2026-02", "total_pnl"] == pytest.approx(11.0)
    assert report.by_regime.at["chaos", "total_pnl"] == pytest.approx(-6.0)
    assert report.by_size_bucket.at["small", "total_pnl"] == pytest.approx(9.0)
    assert report.by_size_bucket.at["medium", "total_pnl"] == pytest.approx(-6.0)
    assert report.by_size_bucket.at["large", "total_pnl"] == pytest.approx(17.0)
    report.assert_reconciled()


def test_attribution_reconciliation_detects_a_tampered_component() -> None:
    report = aggregate_pnl(_ledger())
    report.by_asset.at["BTC", "price"] += 1.0

    with pytest.raises(AttributionReconciliationError, match="does not reconcile"):
        report.assert_reconciled()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda frame: frame.drop(columns="component"), "missing required columns"),
        (lambda frame: frame.assign(pnl=math.nan), "pnl must contain only finite"),
        (lambda frame: frame.assign(size_usd=-1.0), "size_usd must be non-negative"),
        (
            lambda frame: frame.assign(timestamp="2026-01-01T00:00:00"),
            "must be timezone-aware",
        ),
        (lambda frame: frame.assign(regime=""), "regime.*non-empty"),
    ],
)
def test_long_ledger_rejects_unobservable_or_invalid_dimensions(
    mutation: object,
    message: str,
) -> None:
    mutate = mutation
    assert callable(mutate)
    with pytest.raises(ValueError, match=message):
        aggregate_pnl(mutate(_ledger()))


def test_causal_regimes_ignore_current_and_future_observations() -> None:
    index = pd.date_range("2026-01-01", periods=20, freq="1h", tz="UTC")
    baseline = pd.Series(np.linspace(-0.004, 0.006, len(index)), index=index)
    changed = baseline.copy()
    changed.iloc[14:] = [0.9, -0.8, 0.7, -0.6, 0.5, -0.4]

    baseline_regimes = causal_regimes(
        baseline,
        lookback=4,
        calm_volatility=0.0001,
        chaos_volatility=0.05,
        trend_threshold=0.0005,
    )
    changed_regimes = causal_regimes(
        changed,
        lookback=4,
        calm_volatility=0.0001,
        chaos_volatility=0.05,
        trend_threshold=0.0005,
    )

    pd.testing.assert_series_equal(baseline_regimes.iloc[:15], changed_regimes.iloc[:15])
    assert list(baseline_regimes.iloc[:4]) == ["warmup"] * 4
    assert baseline_regimes.name == "regime"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"lookback": 1}, "lookback"),
        ({"calm_volatility": -0.1}, "calm_volatility"),
        ({"calm_volatility": 0.2, "chaos_volatility": 0.1}, "lower"),
        ({"trend_threshold": 0.0}, "positive"),
    ],
)
def test_causal_regimes_reject_invalid_parameters(
    kwargs: dict[str, object],
    message: str,
) -> None:
    returns = pd.Series(
        [0.0, 0.1, -0.1],
        index=pd.date_range("2026-01-01", periods=3, freq="1h", tz="UTC"),
    )
    with pytest.raises(ValueError, match=message):
        causal_regimes(returns, **kwargs)  # type: ignore[arg-type]
