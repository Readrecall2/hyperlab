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
        if self.prices.isna().all(axis=None):
            raise ValueError("prices are entirely missing")


@dataclass(slots=True)
class StrategyOutput:
    name: str
    risk_tier: str
    weights: pd.DataFrame
    diagnostics: dict[str, Any] = field(default_factory=dict)


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
