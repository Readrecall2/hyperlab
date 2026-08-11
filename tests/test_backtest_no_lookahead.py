from __future__ import annotations

import pandas as pd

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
