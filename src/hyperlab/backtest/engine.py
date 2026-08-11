from __future__ import annotations

import numpy as np
import pandas as pd

from hyperlab.backtest.metrics import compute_metrics
from hyperlab.backtest.risk import apply_risk_limits
from hyperlab.models import BacktestResult, CostModel, MarketPanel, RiskLimits, StrategyOutput


def _require_active_data(values: pd.DataFrame, activity: pd.DataFrame, label: str) -> None:
    if bool((values.isna() & activity.ne(0.0)).any(axis=None)):
        raise ValueError(f"{label} data is missing or non-finite for an active position")


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
        delta = weights.diff().fillna(weights).abs()
        held = weights.shift(1).fillna(0.0)

        prices = panel.prices.replace([np.inf, -np.inf], np.nan)
        active_price = delta.add(held.abs())
        _require_active_data(prices, active_price, "price")
        if bool(((prices <= 0.0) & active_price.ne(0.0)).any(axis=None)):
            raise ValueError("price data must be positive for an active or traded position")
        asset_returns = prices.pct_change(fill_method=None).replace([np.inf, -np.inf], np.nan)
        _require_active_data(asset_returns, held, "price return")
        price_return = (held * asset_returns.fillna(0.0)).sum(axis=1)

        funding = panel.funding.replace([np.inf, -np.inf], np.nan)
        _require_active_data(funding, held, "funding")
        funding_return = (-held * funding.fillna(0.0)).sum(axis=1)

        fixed_cost_rate = pd.Series(
            {column: self.costs.one_way_bps(column) / 10_000.0 for column in weights.columns}
        )
        spreads_bps = panel.spreads_bps.replace([np.inf, -np.inf], np.nan)
        _require_active_data(spreads_bps, delta, "spread")
        if bool(((spreads_bps < 0.0) & delta.ne(0.0)).any(axis=None)):
            raise ValueError("spread data must be non-negative for a traded position")
        half_spread_rate = spreads_bps.fillna(0.0) * (
            0.5 * self.costs.stress_multiplier / 10_000.0
        )
        cost_return = -(delta * half_spread_rate.add(fixed_cost_rate, axis="columns")).sum(axis=1)

        net_return = price_return + funding_return + cost_return
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
