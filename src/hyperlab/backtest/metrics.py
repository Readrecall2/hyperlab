from __future__ import annotations

import math

import pandas as pd

from hyperlab.models import BacktestMetrics


def infer_periods_per_year(index: pd.Index) -> float:
    datetime_index = pd.DatetimeIndex(index)
    if len(datetime_index) < 2:
        return 365.0
    deltas = datetime_index.to_series().diff().dropna().dt.total_seconds()
    median_seconds = float(deltas.median())
    if median_seconds <= 0:
        return 365.0
    return (365.25 * 24 * 3600) / median_seconds


def drawdown_series(equity: pd.Series) -> pd.Series:
    """Compute drawdown against normalized initial capital as well as later peaks."""
    running_max = equity.cummax().clip(lower=1.0)
    return equity / running_max - 1.0


def _annualize(final_equity: float, exponent: float) -> float:
    if final_equity <= 0.0:
        return math.nan
    try:
        return math.expm1(math.log(final_equity) * exponent)
    except OverflowError:
        return math.inf


def compute_metrics(
    components: pd.DataFrame,
    equity: pd.Series,
    weights: pd.DataFrame,
) -> BacktestMetrics:
    periods_per_year = infer_periods_per_year(equity.index)
    net = components["net_return"].fillna(0.0)
    n_periods = max(len(net), 1)
    final_equity = float(equity.iloc[-1])
    total_return = final_equity - 1.0
    annualized_return = _annualize(final_equity, periods_per_year / n_periods)

    std = float(net.std(ddof=1)) if len(net) > 1 else 0.0
    annualized_volatility = std * math.sqrt(periods_per_year)
    sharpe = float(net.mean()) / std * math.sqrt(periods_per_year) if std > 1e-15 else 0.0

    max_drawdown = float(drawdown_series(equity).min())
    calmar = annualized_return / abs(max_drawdown) if max_drawdown < -1e-12 else 0.0

    daily_equity = equity.resample("1D").last().dropna()
    daily_returns = daily_equity.pct_change()
    if not daily_returns.empty:
        daily_returns.iloc[0] = float(daily_equity.iloc[0]) - 1.0
    daily_returns = daily_returns.dropna()
    win_day_rate = float((daily_returns > 0).mean()) if not daily_returns.empty else 0.0
    worst_day = float(daily_returns.min()) if not daily_returns.empty else 0.0

    turnover = float(weights.diff().fillna(weights).abs().sum(axis=1).sum())
    gross = weights.abs().sum(axis=1)
    net_exposure = weights.sum(axis=1).abs()
    time_in_market = float((gross > 1e-12).mean())

    return BacktestMetrics(
        total_return=total_return,
        annualized_return=annualized_return,
        annualized_volatility=annualized_volatility,
        sharpe=sharpe,
        max_drawdown=max_drawdown,
        calmar=calmar,
        win_day_rate=win_day_rate,
        worst_day=worst_day,
        turnover=turnover,
        time_in_market=time_in_market,
        max_gross_leverage=float(gross.max()),
        max_net_exposure=float(net_exposure.max()),
        price_contribution=float(components["price_return"].sum()),
        funding_contribution=float(components["funding_return"].sum()),
        cost_contribution=float(components["cost_return"].sum()),
    )
