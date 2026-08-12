from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from hyperlab.models import MarketPanel, StrategyOutput
from hyperlab.strategies.helpers import (
    asset_from,
    columns_by,
    empty_weights,
    rebalance_mask,
    scalar_float,
)


@dataclass(slots=True)
class CrossExchangeFundingStrategy:
    """Long the lower-funding venue and short the higher-funding venue."""

    name: str = "cross_exchange_funding"
    risk_tier: str = "2 — équilibré"
    external_exchange: str = "REF"
    lookback_hours: int = 12
    min_abs_diff_hourly: float = 0.000010
    max_positions: int = 3
    rebalance_hours: int = 4

    def generate(self, panel: MarketPanel) -> StrategyOutput:
        weights = empty_weights(panel)
        hl_perps = columns_by(panel, exchange="HL", kind="perp")
        funding_mean = panel.funding.rolling(
            self.lookback_hours,
            min_periods=self.lookback_hours,
        ).mean()
        rebalance = rebalance_mask(panel.prices.index, self.rebalance_hours)
        current = pd.Series(0.0, index=panel.prices.columns)

        for timestamp in panel.prices.index:
            if bool(rebalance.loc[timestamp]):
                candidates: list[tuple[float, str, str, float]] = []
                for hl in hl_perps:
                    asset = asset_from(hl)
                    external = f"{self.external_exchange.upper()}:{asset}:perp"
                    if external not in panel.prices.columns:
                        continue
                    hl_funding = scalar_float(funding_mean.at[timestamp, hl])
                    external_funding = scalar_float(funding_mean.at[timestamp, external])
                    diff = hl_funding - external_funding
                    if pd.isna(diff) or abs(diff) < self.min_abs_diff_hourly:
                        continue
                    candidates.append((abs(diff), hl, external, diff))
                candidates.sort(reverse=True)
                selected = candidates[: self.max_positions]
                current = pd.Series(0.0, index=panel.prices.columns)
                if selected:
                    leg_weight = 0.5 / len(selected)
                    for _, hl, external, diff in selected:
                        if diff > 0:
                            current[hl] = -leg_weight
                            current[external] = leg_weight
                        else:
                            current[hl] = leg_weight
                            current[external] = -leg_weight
            weights.loc[timestamp] = current

        return StrategyOutput(
            name=self.name,
            risk_tier=self.risk_tier,
            weights=weights,
            diagnostics={
                "logic": "long lower funding venue / short higher funding venue",
                "external_exchange": self.external_exchange,
            },
            hedge_groups={
                f"cross_venue:{asset_from(hl)}": (
                    hl,
                    f"{self.external_exchange.upper()}:{asset_from(hl)}:perp",
                )
                for hl in hl_perps
                if f"{self.external_exchange.upper()}:{asset_from(hl)}:perp" in panel.prices.columns
            },
        )
