from __future__ import annotations

import math

from hyperlab.backtest.engine import PanelBacktester
from hyperlab.data.synthetic import generate_demo_panel, generate_microstructure_demo
from hyperlab.models import CostModel, RiskLimits
from hyperlab.strategies.market_making import InventoryAwareMarketMaker
from hyperlab.strategies.registry import STRATEGY_FACTORIES, create_strategy


def test_all_panel_strategies_run() -> None:
    panel = generate_demo_panel(hours=1_200, seed=11)
    engine = PanelBacktester(costs=CostModel(), risk_limits=RiskLimits())
    for name in STRATEGY_FACTORIES:
        output = create_strategy(name).generate(panel)
        assert output.weights.shape == panel.prices.shape
        assert not output.weights.isna().any(axis=None)
        result = engine.run(panel, output)
        assert math.isfinite(result.metrics.total_return)
        assert result.target_weights is not None
        assert float(result.target_weights.abs().sum(axis=1).max()) <= 1.0 + 1e-12
        assert math.isfinite(result.metrics.max_gross_leverage)


def test_market_maker_demo_runs_and_flattens() -> None:
    data = generate_microstructure_demo(events=2_000, seed=3)
    result = InventoryAwareMarketMaker(seed=4).run(data.events)
    assert len(result.equity) == 2_000
    assert math.isfinite(result.metrics.total_return)
    assert abs(float(result.weights.iloc[-1, 0])) < 1e-12
