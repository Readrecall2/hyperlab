from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from hyperlab.models import MarketPanel, StrategyOutput
from hyperlab.strategies.helpers import asset_from, columns_by, empty_weights, rebalance_mask


@dataclass(slots=True)
class CashAndCarryStrategy:
    """Long spot / short perp when positive funding is persistent."""

    name: str = "cash_and_carry"
    risk_tier: str = "1 — défensif"
    lookback_hours: int = 24
    min_mean_funding_hourly: float = 0.000005
    min_positive_share: float = 0.70
    max_abs_basis_bps: float = 150.0
    max_positions: int = 3
    rebalance_hours: int = 12

    def generate(self, panel: MarketPanel) -> StrategyOutput:
        weights = empty_weights(panel)
        perp_columns = columns_by(panel, exchange="HL", kind="perp")
        funding_mean = panel.funding[perp_columns].rolling(self.lookback_hours, min_periods=self.lookback_hours).mean()
        positive_share = (
            (panel.funding[perp_columns] > 0)
            .rolling(self.lookback_hours, min_periods=self.lookback_hours)
            .mean()
        )
        rebalance = rebalance_mask(panel.prices.index, self.rebalance_hours)

        current = pd.Series(0.0, index=panel.prices.columns)
        for timestamp in panel.prices.index:
            if bool(rebalance.loc[timestamp]):
                candidates: list[tuple[float, str, str]] = []
                for perp in perp_columns:
                    asset = asset_from(perp)
                    spot = f"HL:{asset}:spot"
                    if spot not in panel.prices.columns:
                        continue
                    spot_px = float(panel.prices.at[timestamp, spot])
                    perp_px = float(panel.prices.at[timestamp, perp])
                    if spot_px <= 0 or perp_px <= 0:
                        continue
                    basis_bps = (perp_px / spot_px - 1.0) * 10_000.0
                    mean_funding = float(funding_mean.at[timestamp, perp])
                    share = float(positive_share.at[timestamp, perp])
                    if pd.isna(mean_funding) or pd.isna(share):
                        continue
                    if mean_funding < self.min_mean_funding_hourly:
                        continue
                    if share < self.min_positive_share:
                        continue
                    if abs(basis_bps) > self.max_abs_basis_bps:
                        continue
                    score = mean_funding * 10_000.0 - abs(basis_bps) * 0.0005
                    candidates.append((score, spot, perp))

                candidates.sort(reverse=True)
                selected = candidates[: self.max_positions]
                current = pd.Series(0.0, index=panel.prices.columns)
                if selected:
                    leg_weight = 0.5 / len(selected)
                    for _, spot, perp in selected:
                        current[spot] = leg_weight
                        current[perp] = -leg_weight
            weights.loc[timestamp] = current

        return StrategyOutput(
            name=self.name,
            risk_tier=self.risk_tier,
            weights=weights,
            diagnostics={
                "logic": "long spot + short perp",
                "lookback_hours": self.lookback_hours,
                "max_positions": self.max_positions,
            },
        )
