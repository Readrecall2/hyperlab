from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from hyperlab.models import MarketPanel, StrategyOutput
from hyperlab.strategies.helpers import columns_by, empty_weights, rebalance_mask


@dataclass(slots=True)
class FundingBasketStrategy:
    """Long low-funding perps and short high-funding perps on Hyperliquid."""

    name: str = "funding_basket"
    risk_tier: str = "2 — équilibré"
    lookback_hours: int = 12
    momentum_hours: int = 24
    legs_per_side: int = 2
    min_funding_spread_hourly: float = 0.000008
    rebalance_hours: int = 4
    squeeze_guard_return: float = 0.08

    def generate(self, panel: MarketPanel) -> StrategyOutput:
        weights = empty_weights(panel)
        perps = columns_by(panel, exchange="HL", kind="perp")
        funding_score = panel.funding[perps].rolling(
            self.lookback_hours,
            min_periods=self.lookback_hours,
        ).mean()
        momentum = panel.prices[perps].pct_change(self.momentum_hours)
        rebalance = rebalance_mask(panel.prices.index, self.rebalance_hours)
        current = pd.Series(0.0, index=panel.prices.columns)

        for timestamp in panel.prices.index:
            if bool(rebalance.loc[timestamp]):
                row = funding_score.loc[timestamp].dropna().sort_values()
                current = pd.Series(0.0, index=panel.prices.columns)
                if len(row) >= 2 * self.legs_per_side:
                    longs = list(row.index[: self.legs_per_side])
                    short_candidates = list(row.index[-(self.legs_per_side * 3) :][::-1])
                    shorts = [
                        column
                        for column in short_candidates
                        if float(momentum.at[timestamp, column]) <= self.squeeze_guard_return
                    ][: self.legs_per_side]
                    if len(shorts) == self.legs_per_side:
                        spread = float(row[shorts].mean() - row[longs].mean())
                        if spread >= self.min_funding_spread_hourly:
                            for column in longs:
                                current[column] = 0.5 / len(longs)
                            for column in shorts:
                                current[column] = -0.5 / len(shorts)
            weights.loc[timestamp] = current

        return StrategyOutput(
            name=self.name,
            risk_tier=self.risk_tier,
            weights=weights,
            diagnostics={
                "logic": "long bottom funding / short top funding",
                "legs_per_side": self.legs_per_side,
                "squeeze_guard": self.squeeze_guard_return,
            },
        )
