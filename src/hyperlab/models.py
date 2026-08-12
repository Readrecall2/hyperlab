from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True, slots=True)
class RiskLimits:
    """Portfolio-level limits applied to target weights before simulation."""

    max_gross_leverage: float = 1.0
    max_net_exposure: float = 1.0
    max_instrument_weight: float = 0.50

    def __post_init__(self) -> None:
        for name, value in (
            ("max_gross_leverage", self.max_gross_leverage),
            ("max_net_exposure", self.max_net_exposure),
            ("max_instrument_weight", self.max_instrument_weight),
        ):
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(f"{name} must be finite and non-negative")


@dataclass(frozen=True, slots=True)
class CostModel:
    """Configurable one-way cost assumptions in basis points.

    These are research placeholders. A live-capable system must fetch account-specific
    rates from Hyperliquid's ``userFees`` endpoint and model venue-specific slippage.
    """

    spot_fee_bps: float = 4.0
    perp_fee_bps: float = 1.5
    external_perp_fee_bps: float = 2.0
    base_slippage_bps: float = 1.0
    stress_multiplier: float = 1.0

    def __post_init__(self) -> None:
        for name, value in (
            ("spot_fee_bps", self.spot_fee_bps),
            ("perp_fee_bps", self.perp_fee_bps),
            ("external_perp_fee_bps", self.external_perp_fee_bps),
            ("base_slippage_bps", self.base_slippage_bps),
        ):
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(f"{name} must be finite and non-negative")
        if not math.isfinite(self.stress_multiplier) or self.stress_multiplier <= 0.0:
            raise ValueError("stress_multiplier must be finite and positive")

    def one_way_bps(self, instrument: str) -> float:
        if instrument.endswith(":spot"):
            fee = self.spot_fee_bps
        elif instrument.startswith("HL:") and instrument.endswith(":perp"):
            fee = self.perp_fee_bps
        else:
            fee = self.external_perp_fee_bps
        return (fee + self.base_slippage_bps) * self.stress_multiplier


@dataclass(slots=True)
class MarketPanel:
    """Aligned market matrices for slow-to-medium frequency research.

    ``prices`` and ``funding`` share the same DatetimeIndex and instrument columns.
    Positive funding means a long perp pays and a short perp receives.
    """

    prices: pd.DataFrame
    funding: pd.DataFrame
    spreads_bps: pd.DataFrame
    volume_usd: pd.DataFrame
    metadata: dict[str, Any] = field(default_factory=dict)
    depth_usd: pd.DataFrame | None = None
    available_at: pd.DataFrame | None = None
    finality: pd.DataFrame | None = None
    tradable: pd.DataFrame | None = None
    regimes: pd.Series | None = None

    def validate(self) -> None:
        if self.prices.empty:
            raise ValueError("prices cannot be empty")
        if not isinstance(self.prices.index, pd.DatetimeIndex):
            raise TypeError("prices index must be a DatetimeIndex")
        if self.prices.index.tz is None or str(self.prices.index.tz).upper() not in {
            "UTC",
            "UTC+00:00",
        }:
            raise ValueError("panel timestamps must use UTC")
        if not self.prices.index.is_monotonic_increasing:
            raise ValueError("panel index must be sorted")
        if self.prices.index.has_duplicates:
            raise ValueError("panel index cannot contain duplicates")
        for name, frame in {
            "funding": self.funding,
            "spreads_bps": self.spreads_bps,
            "volume_usd": self.volume_usd,
        }.items():
            if not frame.index.equals(self.prices.index):
                raise ValueError(f"{name} index differs from prices")
            if list(frame.columns) != list(self.prices.columns):
                raise ValueError(f"{name} columns differ from prices")
        for optional_name, optional_frame in {
            "depth_usd": self.depth_usd,
            "available_at": self.available_at,
            "finality": self.finality,
            "tradable": self.tradable,
        }.items():
            if optional_frame is None:
                continue
            if not optional_frame.index.equals(self.prices.index):
                raise ValueError(f"{optional_name} index differs from prices")
            if list(optional_frame.columns) != list(self.prices.columns):
                raise ValueError(f"{optional_name} columns differ from prices")
        if self.regimes is not None and not self.regimes.index.equals(self.prices.index):
            raise ValueError("regimes index differs from prices")
        if self.prices.isna().all(axis=None):
            raise ValueError("prices are entirely missing")
        if self.depth_usd is not None:
            numeric_depth = self.depth_usd.apply(pd.to_numeric, errors="coerce")
            supplied = self.depth_usd.notna()
            if bool((supplied & (~numeric_depth.map(math.isfinite) | numeric_depth.le(0.0))).any(axis=None)):
                raise ValueError("depth_usd values must be finite and positive when supplied")
        if self.available_at is not None:
            for column in self.available_at.columns:
                for value in self.available_at[column].dropna():
                    timestamp = pd.Timestamp(value)
                    if timestamp.tz is None:
                        raise ValueError("available_at timestamps must use UTC")
                    if str(timestamp.tz).upper() not in {"UTC", "UTC+00:00"}:
                        raise ValueError("available_at timestamps must use UTC")
        for boolean_name, boolean_frame in {
            "finality": self.finality,
            "tradable": self.tradable,
        }.items():
            if boolean_frame is None:
                continue
            valid = boolean_frame.map(lambda value: isinstance(value, bool) or pd.isna(value))
            if not bool(valid.all(axis=None)):
                raise ValueError(f"{boolean_name} values must be boolean or missing")


@dataclass(slots=True)
class StrategyOutput:
    name: str
    risk_tier: str
    weights: pd.DataFrame
    diagnostics: dict[str, Any] = field(default_factory=dict)
    order_types: pd.DataFrame | None = None
    hedge_groups: dict[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass(slots=True)
class BacktestMetrics:
    total_return: float
    annualized_return: float
    annualized_volatility: float
    sharpe: float
    max_drawdown: float
    calmar: float
    win_day_rate: float
    worst_day: float
    turnover: float
    time_in_market: float
    max_gross_leverage: float
    max_net_exposure: float
    price_contribution: float
    funding_contribution: float
    cost_contribution: float
    basis_contribution: float = 0.0
    spread_contribution: float = 0.0
    fee_contribution: float = 0.0
    slippage_contribution: float = 0.0
    hedge_contribution: float = 0.0
    worst_hour: float = 0.0
    benchmark_return: float = 0.0
    excess_vs_benchmark: float = 0.0
    fill_rate: float = 1.0

    def as_dict(self) -> dict[str, float]:
        return {
            "total_return": self.total_return,
            "annualized_return": self.annualized_return,
            "annualized_volatility": self.annualized_volatility,
            "sharpe": self.sharpe,
            "max_drawdown": self.max_drawdown,
            "calmar": self.calmar,
            "win_day_rate": self.win_day_rate,
            "worst_day": self.worst_day,
            "turnover": self.turnover,
            "time_in_market": self.time_in_market,
            "max_gross_leverage": self.max_gross_leverage,
            "max_net_exposure": self.max_net_exposure,
            "price_contribution": self.price_contribution,
            "funding_contribution": self.funding_contribution,
            "cost_contribution": self.cost_contribution,
            "basis_contribution": self.basis_contribution,
            "spread_contribution": self.spread_contribution,
            "fee_contribution": self.fee_contribution,
            "slippage_contribution": self.slippage_contribution,
            "hedge_contribution": self.hedge_contribution,
            "worst_hour": self.worst_hour,
            "benchmark_return": self.benchmark_return,
            "excess_vs_benchmark": self.excess_vs_benchmark,
            "fill_rate": self.fill_rate,
        }


@dataclass(slots=True)
class BacktestResult:
    strategy_name: str
    risk_tier: str
    returns: pd.DataFrame
    equity: pd.Series
    weights: pd.DataFrame
    metrics: BacktestMetrics
    diagnostics: dict[str, Any] = field(default_factory=dict)
    report_path: Path | None = None
    target_weights: pd.DataFrame | None = None
    fills: pd.DataFrame = field(default_factory=pd.DataFrame)
    attribution: pd.DataFrame = field(default_factory=pd.DataFrame)
    benchmark: pd.Series | None = None
    breakdowns: dict[str, pd.DataFrame] = field(default_factory=dict)
    uncertainty: dict[str, Any] = field(default_factory=dict)
