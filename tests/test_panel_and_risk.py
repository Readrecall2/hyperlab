from __future__ import annotations

import pandas as pd

from hyperlab.backtest.risk import apply_risk_limits
from hyperlab.data.synthetic import generate_demo_panel
from hyperlab.models import RiskLimits


def test_synthetic_panel_is_aligned() -> None:
    panel = generate_demo_panel(hours=800, seed=1)
    panel.validate()
    assert panel.prices.index.equals(panel.funding.index)
    assert list(panel.prices.columns) == list(panel.spreads_bps.columns)


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
