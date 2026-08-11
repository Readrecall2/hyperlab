from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from hyperlab.models import MarketPanel, StrategyOutput
from hyperlab.strategies.helpers import empty_weights


@dataclass(slots=True)
class PairsMeanReversionStrategy:
    """Rolling-beta mean reversion between two Hyperliquid perps."""

    name: str = "pairs_mean_reversion"
    risk_tier: str = "3 — offensif"
    asset_a: str = "ETH"
    asset_b: str = "BTC"
    lookback_hours: int = 240
    enter_z: float = 2.0
    exit_z: float = 0.5
    stop_z: float = 4.0

    def generate(self, panel: MarketPanel) -> StrategyOutput:
        weights = empty_weights(panel)
        column_a = f"HL:{self.asset_a.upper()}:perp"
        column_b = f"HL:{self.asset_b.upper()}:perp"
        if column_a not in panel.prices.columns or column_b not in panel.prices.columns:
            return StrategyOutput(self.name, self.risk_tier, weights, {"disabled": "missing pair"})

        log_a = np.log(panel.prices[column_a])
        log_b = np.log(panel.prices[column_b])
        ret_a = log_a.diff()
        ret_b = log_b.diff()
        beta = (
            ret_a.rolling(self.lookback_hours, min_periods=self.lookback_hours).cov(ret_b)
            / ret_b.rolling(self.lookback_hours, min_periods=self.lookback_hours).var()
        ).clip(lower=0.25, upper=4.0)
        spread = log_a - beta * log_b
        mean = spread.rolling(self.lookback_hours, min_periods=self.lookback_hours).mean()
        std = spread.rolling(self.lookback_hours, min_periods=self.lookback_hours).std()
        zscore = (spread - mean) / std.replace(0.0, np.nan)

        state = 0
        for timestamp in panel.prices.index:
            z = float(zscore.loc[timestamp])
            b = float(beta.loc[timestamp])
            if pd.isna(z) or pd.isna(b):
                continue
            if state == 0:
                if z >= self.enter_z:
                    state = -1
                elif z <= -self.enter_z:
                    state = 1
            else:
                if abs(z) <= self.exit_z or abs(z) >= self.stop_z:
                    state = 0

            if state != 0:
                gross = 1.0 + abs(b)
                weights.at[timestamp, column_a] = state / gross
                weights.at[timestamp, column_b] = -state * b / gross

        return StrategyOutput(
            name=self.name,
            risk_tier=self.risk_tier,
            weights=weights,
            diagnostics={
                "pair": f"{self.asset_a}/{self.asset_b}",
                "enter_z": self.enter_z,
                "stop_z": self.stop_z,
            },
        )
