from __future__ import annotations

import math

import numpy as np
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
    *,
    benchmark: pd.Series | None = None,
    fills: pd.DataFrame | None = None,
) -> BacktestMetrics:
    net = components["net_return"].fillna(0.0)
    final_equity = float(equity.iloc[-1])
    total_return = final_equity - 1.0
    datetime_index = pd.DatetimeIndex(equity.index)
    if len(datetime_index) > 1:
        deltas = datetime_index.to_series().diff().dropna().dt.total_seconds()
        representative_seconds = float(deltas.median())
        elapsed_seconds = float((datetime_index[-1] - datetime_index[0]).total_seconds())
        elapsed_seconds += representative_seconds
    else:
        elapsed_seconds = 86_400.0
    elapsed_years = elapsed_seconds / (365.25 * 24.0 * 3600.0)
    annualized_return = _annualize(final_equity, 1.0 / max(elapsed_years, 1e-12))

    max_drawdown = float(drawdown_series(equity).min())
    calmar = annualized_return / abs(max_drawdown) if max_drawdown < -1e-12 else 0.0

    daily_equity = equity.resample("1D").last().dropna()
    daily_returns = daily_equity.pct_change()
    if not daily_returns.empty:
        daily_returns.iloc[0] = float(daily_equity.iloc[0]) - 1.0
    daily_returns = daily_returns.dropna()
    win_day_rate = float((daily_returns > 0).mean()) if not daily_returns.empty else 0.0
    worst_day = float(daily_returns.min()) if not daily_returns.empty else 0.0
    daily_std = float(daily_returns.std(ddof=1)) if len(daily_returns) > 1 else 0.0
    annualized_volatility = daily_std * math.sqrt(365.25)
    sharpe = float(daily_returns.mean()) / daily_std * math.sqrt(365.25) if daily_std > 1e-15 else 0.0
    hourly_returns = net.resample("1h").apply(
        lambda values: float(np.prod(1.0 + values.to_numpy(dtype=float)) - 1.0)
    )
    worst_hour = float(hourly_returns.min()) if not hourly_returns.empty else 0.0

    turnover = float(weights.diff().fillna(weights).abs().sum(axis=1).sum())
    gross = weights.abs().sum(axis=1)
    net_exposure = weights.sum(axis=1).abs()
    time_in_market = float((gross > 1e-12).mean())

    capital_at_start = equity.shift(1, fill_value=1.0)

    def contribution(column: str) -> float:
        if column not in components:
            return 0.0
        return float((components[column].fillna(0.0) * capital_at_start).sum())

    benchmark_return = 0.0
    if benchmark is not None:
        aligned = benchmark.reindex(equity.index)
        if aligned.isna().any():
            raise ValueError("benchmark must cover the complete backtest index")
        benchmark_return = float(aligned.iloc[-1] / aligned.iloc[0] - 1.0)

    fill_rate = 1.0
    if fills is not None and not fills.empty and "status" in fills:
        attempted = fills.loc[~fills["status"].isin(["CANCELLED", "EXPIRED"])]
        if not attempted.empty:
            fill_rate = float(attempted["filled_weight"].abs().gt(1e-12).mean())

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
        price_contribution=contribution("price_return"),
        funding_contribution=contribution("funding_return"),
        cost_contribution=contribution("cost_return"),
        basis_contribution=contribution("basis_return"),
        spread_contribution=contribution("spread_return"),
        fee_contribution=contribution("fee_return"),
        slippage_contribution=contribution("slippage_return"),
        hedge_contribution=contribution("hedge_return"),
        worst_hour=worst_hour,
        benchmark_return=benchmark_return,
        excess_vs_benchmark=total_return - benchmark_return,
        fill_rate=fill_rate,
    )
