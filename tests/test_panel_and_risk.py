from __future__ import annotations

import math

import pandas as pd
import pytest

from hyperlab.backtest.risk import apply_risk_limits
from hyperlab.data.synthetic import generate_demo_panel
from hyperlab.models import CostModel, RiskLimits


def test_synthetic_panel_is_aligned() -> None:
    panel = generate_demo_panel(hours=800, seed=1)
    panel.validate()
    assert panel.prices.index.equals(panel.funding.index)
    assert list(panel.prices.columns) == list(panel.spreads_bps.columns)


def test_market_panel_rejects_naive_timestamps() -> None:
    index = pd.date_range("2026-01-01", periods=2, freq="1h")
    prices = pd.DataFrame({"HL:BTC:perp": [100.0, 101.0]}, index=index)
    zero = pd.DataFrame(0.0, index=index, columns=prices.columns)
    panel = generate_demo_panel(hours=800, seed=1)
    panel.prices = prices
    panel.funding = zero.copy()
    panel.spreads_bps = zero.copy()
    panel.volume_usd = zero.copy()

    with pytest.raises(ValueError, match="UTC"):
        panel.validate()


def test_risk_limits_do_not_create_new_positions() -> None:
    index = pd.date_range("2026-01-01", periods=2, freq="1h", tz="UTC")
    weights = pd.DataFrame({"A": [0.8, 0.0], "B": [0.0, -0.8], "C": [0.0, 0.0]}, index=index)
    limited = apply_risk_limits(
        weights,
        RiskLimits(max_gross_leverage=0.5, max_net_exposure=0.1, max_instrument_weight=0.5),
    )
    assert (limited["C"] == 0.0).all()
    assert limited.abs().sum(axis=1).max() <= 0.5 + 1e-12
    assert limited.sum(axis=1).abs().max() <= 0.1 + 1e-12


@pytest.mark.parametrize(
    "field",
    ["max_gross_leverage", "max_net_exposure", "max_instrument_weight"],
)
@pytest.mark.parametrize("value", [-0.01, math.nan, math.inf, -math.inf])
def test_risk_limits_reject_negative_or_non_finite_bounds(field: str, value: float) -> None:
    values = {
        "max_gross_leverage": 1.0,
        "max_net_exposure": 1.0,
        "max_instrument_weight": 0.5,
    }
    values[field] = value

    with pytest.raises(ValueError, match=field):
        RiskLimits(**values)


def test_risk_limits_accept_zero_bounds() -> None:
    assert RiskLimits(
        max_gross_leverage=0.0,
        max_net_exposure=0.0,
        max_instrument_weight=0.0,
    ) == RiskLimits(0.0, 0.0, 0.0)


@pytest.mark.parametrize(
    "field",
    [
        "spot_fee_bps",
        "perp_fee_bps",
        "external_perp_fee_bps",
        "base_slippage_bps",
        "stress_multiplier",
    ],
)
@pytest.mark.parametrize("value", [-0.01, math.nan, math.inf, -math.inf])
def test_cost_model_rejects_negative_or_non_finite_values(field: str, value: float) -> None:
    values = {
        "spot_fee_bps": 4.0,
        "perp_fee_bps": 1.5,
        "external_perp_fee_bps": 2.0,
        "base_slippage_bps": 1.0,
        "stress_multiplier": 1.0,
    }
    values[field] = value

    with pytest.raises(ValueError, match=field):
        CostModel(**values)


def test_cost_model_rejects_a_zero_stress_multiplier() -> None:
    with pytest.raises(ValueError, match="stress_multiplier"):
        CostModel(stress_multiplier=0.0)
