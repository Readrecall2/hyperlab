from __future__ import annotations

import numpy as np
import pandas as pd

from hyperlab.models import RiskLimits


def _limit_row(row: pd.Series, limits: RiskLimits) -> pd.Series:
    limited = row.clip(
        lower=-limits.max_instrument_weight,
        upper=limits.max_instrument_weight,
    ).astype(float)
    net = float(limited.sum())
    if net > limits.max_net_exposure:
        positive = limited.clip(lower=0.0)
        total = float(positive.sum())
        if total > 0:
            limited = limited - positive / total * (net - limits.max_net_exposure)
    elif net < -limits.max_net_exposure:
        negative_magnitude = (-limited.clip(upper=0.0)).astype(float)
        total = float(negative_magnitude.sum())
        if total > 0:
            limited = limited + negative_magnitude / total * (-limits.max_net_exposure - net)

    gross = float(limited.abs().sum())
    if gross > limits.max_gross_leverage and gross > 0:
        limited *= limits.max_gross_leverage / gross
    return limited.where(row.ne(0.0), 0.0)


def apply_risk_limits(weights: pd.DataFrame, limits: RiskLimits) -> pd.DataFrame:
    """Clip positions without creating instruments that the strategy did not request."""
    if weights.empty:
        return weights.copy()
    clean = weights.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return clean.apply(lambda row: _limit_row(row, limits), axis=1)
