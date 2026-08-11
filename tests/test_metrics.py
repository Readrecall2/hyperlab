from __future__ import annotations

import math

import pandas as pd
import pytest

from hyperlab.backtest.metrics import _annualize, compute_metrics


def test_annualized_return_reports_numeric_overflow_without_capping() -> None:
    assert _annualize(1.001, 1_000_000.0) == math.inf


def test_drawdown_and_worst_day_include_initial_capital() -> None:
    index = pd.date_range("2026-01-01", periods=2, freq="1D", tz="UTC")
    components = pd.DataFrame(
        {
            "price_return": [-0.2, 0.125],
            "funding_return": [0.0, 0.0],
            "cost_return": [0.0, 0.0],
            "net_return": [-0.2, 0.125],
        },
        index=index,
    )
    equity = (1.0 + components["net_return"]).cumprod()
    weights = pd.DataFrame({"HL:BTC:perp": [0.0, 0.0]}, index=index)

    metrics = compute_metrics(components, equity, weights)

    assert metrics.max_drawdown == pytest.approx(-0.2)
    assert metrics.worst_day == pytest.approx(-0.2)
    assert metrics.win_day_rate == pytest.approx(0.5)


def test_turnover_includes_the_initial_position_opening() -> None:
    index = pd.date_range("2026-01-01", periods=2, freq="1h", tz="UTC")
    components = pd.DataFrame(
        {
            "price_return": [0.0, 0.0],
            "funding_return": [0.0, 0.0],
            "cost_return": [0.0, 0.0],
            "net_return": [0.0, 0.0],
        },
        index=index,
    )
    equity = pd.Series([1.0, 1.0], index=index)
    weights = pd.DataFrame({"HL:BTC:perp": [1.0, 1.0]}, index=index)

    metrics = compute_metrics(components, equity, weights)

    assert metrics.turnover == pytest.approx(1.0)
