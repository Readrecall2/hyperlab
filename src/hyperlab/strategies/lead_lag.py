from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from hyperlab.models import MarketPanel, StrategyOutput
from hyperlab.strategies.helpers import asset_from, columns_by, empty_weights


@dataclass(slots=True)
class LeadLagStrategy:
    """Predict next Hyperliquid return from a faster reference venue.

    This bar-level implementation is a research baseline. A deployable version needs
    synchronized sub-second timestamps, latency measurement, and order-book replay.
    """

    name: str = "lead_lag"
    risk_tier: str = "4 — agressif"
    reference_exchange: str = "REF"
    estimation_hours: int = 240
    minimum_prediction: float = 0.0004
    max_assets: int = 3

    def generate(self, panel: MarketPanel) -> StrategyOutput:
        weights = empty_weights(panel)
        hl_perps = columns_by(panel, exchange="HL", kind="perp")
        predictions = pd.DataFrame(index=panel.prices.index)

        for hl in hl_perps:
            asset = asset_from(hl)
            reference = f"{self.reference_exchange.upper()}:{asset}:perp"
            if reference not in panel.prices.columns:
                continue
            hl_return = panel.prices[hl].pct_change()
            reference_return = panel.prices[reference].pct_change()
            lagged_reference = reference_return.shift(1)
            beta = (
                hl_return.rolling(self.estimation_hours, min_periods=self.estimation_hours)
                .cov(lagged_reference)
                / lagged_reference.rolling(
                    self.estimation_hours,
                    min_periods=self.estimation_hours,
                ).var()
            ).clip(lower=-3.0, upper=3.0)
            predictions[hl] = beta * reference_return

        for timestamp in panel.prices.index:
            if predictions.empty:
                break
            row = predictions.loc[timestamp].dropna()
            row = row[row.abs() >= self.minimum_prediction]
            selected = list(row.abs().sort_values(ascending=False).index[: self.max_assets])
            if not selected:
                continue
            signed = np.sign(row[selected]) * row[selected].abs()
            normalizer = float(signed.abs().sum())
            if normalizer > 0:
                weights.loc[timestamp, selected] = signed / normalizer

        return StrategyOutput(
            name=self.name,
            risk_tier=self.risk_tier,
            weights=weights,
            diagnostics={
                "warning": "requires synchronized sub-second data for real validation",
                "reference_exchange": self.reference_exchange,
            },
        )
