from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from hyperlab.models import MarketPanel, StrategyOutput
from hyperlab.strategies.helpers import columns_by, empty_weights, rebalance_mask


@dataclass(slots=True)
class MomentumRegimeStrategy:
    """Directional time-series momentum with volatility sizing and funding penalty."""

    name: str = "momentum_regime"
    risk_tier: str = "3 — offensif"
    lookback_hours: int = 72
    volatility_hours: int = 72
    assets_to_trade: int = 3
    minimum_signal: float = 0.25
    funding_penalty: float = 2_000.0
    rebalance_hours: int = 4

    def generate(self, panel: MarketPanel) -> StrategyOutput:
        weights = empty_weights(panel)
        perps = columns_by(panel, exchange="HL", kind="perp")
        returns = panel.prices[perps].pct_change()
        momentum = panel.prices[perps].pct_change(self.lookback_hours)
        volatility = returns.rolling(
            self.volatility_hours,
            min_periods=self.volatility_hours,
        ).std().replace(0.0, np.nan)
        funding_mean = panel.funding[perps].rolling(24, min_periods=24).mean()
        raw_score = momentum / (volatility * np.sqrt(self.lookback_hours))
        score = raw_score - np.sign(raw_score) * funding_mean * self.funding_penalty
        rebalance = rebalance_mask(panel.prices.index, self.rebalance_hours)
        current = pd.Series(0.0, index=panel.prices.columns)

        for timestamp in panel.prices.index:
            if bool(rebalance.loc[timestamp]):
                row = score.loc[timestamp].dropna()
                row = row[row.abs() >= self.minimum_signal]
                selected = list(row.abs().sort_values(ascending=False).index[: self.assets_to_trade])
                current = pd.Series(0.0, index=panel.prices.columns)
                if selected:
                    inverse_vol = 1.0 / volatility.loc[timestamp, selected]
                    inverse_vol = inverse_vol.replace([np.inf, -np.inf], np.nan).dropna()
                    if not inverse_vol.empty:
                        signed = np.sign(row[inverse_vol.index]) * inverse_vol
                        normalizer = float(signed.abs().sum())
                        if normalizer > 0:
                            current.loc[signed.index] = signed / normalizer
            weights.loc[timestamp] = current

        return StrategyOutput(
            name=self.name,
            risk_tier=self.risk_tier,
            weights=weights,
            diagnostics={
                "logic": "directional momentum + volatility sizing",
                "lookback_hours": self.lookback_hours,
            },
        )
