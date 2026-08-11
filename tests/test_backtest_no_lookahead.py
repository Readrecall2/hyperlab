from __future__ import annotations

import math

import pandas as pd
import pytest

from hyperlab.backtest.engine import PanelBacktester
from hyperlab.models import CostModel, MarketPanel, RiskLimits, StrategyOutput


def test_signal_after_jump_does_not_capture_same_jump() -> None:
    index = pd.date_range("2026-01-01", periods=3, freq="1h", tz="UTC")
    prices = pd.DataFrame({"HL:BTC:perp": [100.0, 200.0, 200.0]}, index=index)
    zero = pd.DataFrame(0.0, index=index, columns=prices.columns)
    panel = MarketPanel(prices=prices, funding=zero, spreads_bps=zero, volume_usd=zero)
    weights = pd.DataFrame({"HL:BTC:perp": [0.0, 1.0, 0.0]}, index=index)
    output = StrategyOutput("test", "test", weights)
    result = PanelBacktester(
        costs=CostModel(0.0, 0.0, 0.0, 0.0),
        risk_limits=RiskLimits(1.0, 1.0, 1.0),
    ).run(panel, output)
    assert abs(result.metrics.total_return) < 1e-12


def test_half_spread_cost_is_applied_and_stressed() -> None:
    index = pd.date_range("2026-01-01", periods=2, freq="1h", tz="UTC")
    prices = pd.DataFrame({"HL:BTC:perp": [100.0, 100.0]}, index=index)
    zero = pd.DataFrame(0.0, index=index, columns=prices.columns)
    spreads = pd.DataFrame(20.0, index=index, columns=prices.columns)
    panel = MarketPanel(prices=prices, funding=zero, spreads_bps=spreads, volume_usd=zero)
    weights = pd.DataFrame({"HL:BTC:perp": [1.0, 1.0]}, index=index)
    output = StrategyOutput("test", "test", weights)

    result = PanelBacktester(
        costs=CostModel(
            spot_fee_bps=0.0,
            perp_fee_bps=0.0,
            external_perp_fee_bps=0.0,
            base_slippage_bps=0.0,
            stress_multiplier=2.0,
        ),
        risk_limits=RiskLimits(1.0, 1.0, 1.0),
    ).run(panel, output)

    assert result.returns["cost_return"].iloc[0] == pytest.approx(-0.002)
    assert result.metrics.total_return == pytest.approx(-0.002)


def test_loss_below_minus_one_is_reported_without_clipping() -> None:
    index = pd.date_range("2026-01-01", periods=2, freq="1h", tz="UTC")
    prices = pd.DataFrame({"HL:BTC:perp": [100.0, 40.0]}, index=index)
    zero = pd.DataFrame(0.0, index=index, columns=prices.columns)
    panel = MarketPanel(prices=prices, funding=zero, spreads_bps=zero, volume_usd=zero)
    weights = pd.DataFrame({"HL:BTC:perp": [2.0, 0.0]}, index=index)
    output = StrategyOutput("test", "test", weights)

    result = PanelBacktester(
        costs=CostModel(0.0, 0.0, 0.0, 0.0),
        risk_limits=RiskLimits(2.0, 2.0, 2.0),
    ).run(panel, output)

    assert result.returns["net_return"].iloc[-1] == pytest.approx(-1.2)
    assert result.metrics.total_return == pytest.approx(-1.2)
    assert math.isnan(result.metrics.annualized_return)


@pytest.mark.parametrize(
    ("missing_field", "message"),
    [
        ("prices", "price data"),
        ("funding", "funding"),
        ("spreads_bps", "spread"),
    ],
)
def test_active_position_never_silently_fills_missing_data(
    missing_field: str,
    message: str,
) -> None:
    index = pd.date_range("2026-01-01", periods=3, freq="1h", tz="UTC")
    prices = pd.DataFrame({"HL:BTC:perp": [100.0, 101.0, 102.0]}, index=index)
    zero = pd.DataFrame(0.0, index=index, columns=prices.columns)
    panel = MarketPanel(
        prices=prices.copy(),
        funding=zero.copy(),
        spreads_bps=zero.copy(),
        volume_usd=zero.copy(),
    )
    weights = pd.DataFrame({"HL:BTC:perp": [1.0, 1.0, 0.0]}, index=index)
    if missing_field == "spreads_bps":
        panel.spreads_bps.iloc[0, 0] = math.nan
    else:
        getattr(panel, missing_field).iloc[1, 0] = math.nan

    with pytest.raises(ValueError, match=message):
        PanelBacktester(
            costs=CostModel(0.0, 0.0, 0.0, 0.0),
            risk_limits=RiskLimits(1.0, 1.0, 1.0),
        ).run(panel, StrategyOutput("test", "test", weights))


def test_opening_position_requires_a_current_price() -> None:
    index = pd.date_range("2026-01-01", periods=3, freq="1h", tz="UTC")
    prices = pd.DataFrame({"HL:BTC:perp": [100.0, math.nan, 102.0]}, index=index)
    zero = pd.DataFrame(0.0, index=index, columns=prices.columns)
    panel = MarketPanel(prices=prices, funding=zero, spreads_bps=zero, volume_usd=zero)
    weights = pd.DataFrame({"HL:BTC:perp": [0.0, 1.0, 0.0]}, index=index)

    with pytest.raises(ValueError, match="price data"):
        PanelBacktester(
            costs=CostModel(0.0, 0.0, 0.0, 0.0),
            risk_limits=RiskLimits(1.0, 1.0, 1.0),
        ).run(panel, StrategyOutput("test", "test", weights))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("prices", 0.0, "price data must be positive"),
        ("spreads_bps", -20.0, "spread data must be non-negative"),
    ],
)
def test_trades_reject_impossible_price_or_spread_data(
    field: str,
    value: float,
    message: str,
) -> None:
    index = pd.date_range("2026-01-01", periods=2, freq="1h", tz="UTC")
    prices = pd.DataFrame({"HL:BTC:perp": [100.0, 100.0]}, index=index)
    zero = pd.DataFrame(0.0, index=index, columns=prices.columns)
    panel = MarketPanel(
        prices=prices.copy(),
        funding=zero.copy(),
        spreads_bps=zero.copy(),
        volume_usd=zero.copy(),
    )
    getattr(panel, field).iloc[0, 0] = value
    weights = pd.DataFrame({"HL:BTC:perp": [1.0, 0.0]}, index=index)

    with pytest.raises(ValueError, match=message):
        PanelBacktester(
            costs=CostModel(0.0, 0.0, 0.0, 0.0),
            risk_limits=RiskLimits(1.0, 1.0, 1.0),
        ).run(panel, StrategyOutput("test", "test", weights))
