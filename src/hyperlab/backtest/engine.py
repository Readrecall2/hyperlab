from __future__ import annotations

import numpy as np
import pandas as pd

from hyperlab.backtest.metrics import compute_metrics
from hyperlab.backtest.risk import apply_risk_limits
from hyperlab.models import BacktestResult, CostModel, MarketPanel, RiskLimits, StrategyOutput


class PanelBacktester:
    """Portfolio simulator for bar/funding strategies.

    A target produced at timestamp ``t`` earns price and funding PnL from ``t`` to ``t+1``.
    This is enforced by using prior-period weights against current-period returns.
    """

    def __init__(self, *, costs: CostModel, risk_limits: RiskLimits) -> None:
        self.costs = costs
        self.risk_limits = risk_limits

    def run(self, panel: MarketPanel, output: StrategyOutput) -> BacktestResult:
        panel.validate()
        weights = output.weights.reindex(index=panel.prices.index, columns=panel.prices.columns)
        weights = apply_risk_limits(weights, self.risk_limits).fillna(0.0)

        asset_returns = panel.prices.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
        held = weights.shift(1).fillna(0.0)
        price_return = (held * asset_returns).sum(axis=1)
        funding_return = (-held * panel.funding.fillna(0.0)).sum(axis=1)

        delta = weights.diff().fillna(weights).abs()
        rate = pd.Series(
            {column: self.costs.one_way_bps(column) / 10_000.0 for column in weights.columns}
        )
        cost_return = -(delta * rate).sum(axis=1)

        net_return = price_return + funding_return + cost_return
        net_return = net_return.clip(lower=-0.99)
        equity = (1.0 + net_return).cumprod()
        equity.name = output.name

        components = pd.DataFrame(
            {
                "price_return": price_return,
                "funding_return": funding_return,
                "cost_return": cost_return,
                "net_return": net_return,
            },
            index=panel.prices.index,
        )
        metrics = compute_metrics(components, equity, weights)
        return BacktestResult(
            strategy_name=output.name,
            risk_tier=output.risk_tier,
            returns=components,
            equity=equity,
            weights=weights,
            metrics=metrics,
            diagnostics=output.diagnostics,
        )
