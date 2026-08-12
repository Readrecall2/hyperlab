from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Literal, cast

import numpy as np
import pandas as pd

from hyperlab.models import MarketPanel, StrategyOutput
from hyperlab.strategies.helpers import empty_weights

HedgeMethod = Literal["rolling", "kalman", "cointegration"]


@dataclass(frozen=True, slots=True)
class PairModel:
    """A pair and hedge model frozen before the final-test period."""

    asset_a: str
    asset_b: str
    method: HedgeMethod
    hedge_ratio: float
    intercept: float
    lookback_bars: int
    train_score: float
    validation_score: float
    kalman_process_variance: float = 1e-5
    kalman_observation_variance: float = 1e-3

    def __post_init__(self) -> None:
        if self.asset_a == self.asset_b or not self.asset_a or not self.asset_b:
            raise ValueError("a pair requires two distinct instruments")
        if self.method not in {"rolling", "kalman", "cointegration"}:
            raise ValueError("unsupported hedge method")
        if self.lookback_bars < 12:
            raise ValueError("lookback_bars must be at least 12")
        for name, value in (
            ("hedge_ratio", self.hedge_ratio),
            ("intercept", self.intercept),
            ("train_score", self.train_score),
            ("validation_score", self.validation_score),
            ("kalman_process_variance", self.kalman_process_variance),
            ("kalman_observation_variance", self.kalman_observation_variance),
        ):
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
        if self.hedge_ratio <= 0.0:
            raise ValueError("hedge_ratio must be positive")
        if self.kalman_process_variance <= 0.0 or self.kalman_observation_variance <= 0.0:
            raise ValueError("Kalman variances must be positive")

    @property
    def pair_id(self) -> str:
        return f"{self.asset_a}|{self.asset_b}"


@dataclass(slots=True)
class RobustPairsStrategy:
    """Causal multi-pair mean reversion with bounded, volatility-based sizing."""

    models: tuple[PairModel, ...]
    name: str = "pairs_mean_reversion_phase08"
    risk_tier: str = "3 — offensif"
    enter_z: float = 2.0
    exit_z: float = 0.5
    stop_z: float = 4.0
    max_holding_bars: int = 168
    cooldown_bars: int = 24
    volatility_lookback_bars: int = 72
    target_spread_volatility: float = 0.01
    maximum_pair_gross: float = 0.50
    trade_start: pd.Timestamp | None = None

    def __post_init__(self) -> None:
        if not self.models:
            raise ValueError("at least one frozen pair model is required")
        instruments = [instrument for model in self.models for instrument in (model.asset_a, model.asset_b)]
        if len(set(instruments)) != len(instruments):
            raise ValueError("selected pairs must be instrument-disjoint")
        if not 0.0 <= self.exit_z < self.enter_z < self.stop_z:
            raise ValueError("require 0 <= exit_z < enter_z < stop_z")
        if self.max_holding_bars < 1 or self.cooldown_bars < 0:
            raise ValueError("holding and cooldown bars must be non-negative and bounded")
        if self.volatility_lookback_bars < 12:
            raise ValueError("volatility_lookback_bars must be at least 12")
        if not 0.0 < self.target_spread_volatility <= 1.0:
            raise ValueError("target_spread_volatility must be in (0, 1]")
        if not 0.0 < self.maximum_pair_gross <= 1.0:
            raise ValueError("maximum_pair_gross must be in (0, 1]")
        if self.trade_start is not None:
            start = pd.Timestamp(self.trade_start)
            if start.tz is None or start.utcoffset() != pd.Timedelta(0):
                raise ValueError("trade_start must be UTC")

    @staticmethod
    def _kalman_coefficients(
        log_a: pd.Series,
        log_b: pd.Series,
        model: PairModel,
    ) -> tuple[pd.Series, pd.Series]:
        beta = pd.Series(np.nan, index=log_a.index, dtype=float)
        intercept = pd.Series(np.nan, index=log_a.index, dtype=float)
        state = np.array([model.intercept, model.hedge_ratio], dtype=float)
        covariance = np.eye(2, dtype=float)
        process = np.eye(2, dtype=float) * model.kalman_process_variance
        observation_variance = model.kalman_observation_variance
        for timestamp in log_a.index:
            x = float(log_b.loc[timestamp])
            y = float(log_a.loc[timestamp])
            if not math.isfinite(x) or not math.isfinite(y):
                continue
            covariance = covariance + process
            design = np.array([1.0, x], dtype=float)
            innovation_variance = float(design @ covariance @ design + observation_variance)
            gain = covariance @ design / innovation_variance
            state = state + gain * (y - float(design @ state))
            covariance = covariance - np.outer(gain, design) @ covariance
            state[1] = float(np.clip(state[1], 0.10, 10.0))
            intercept.at[timestamp] = state[0]
            beta.at[timestamp] = state[1]
        return beta, intercept

    def _features(self, panel: MarketPanel, model: PairModel) -> pd.DataFrame:
        log_a = np.log(panel.prices[model.asset_a])
        log_b = np.log(panel.prices[model.asset_b])
        if model.method == "rolling":
            beta = (
                log_a.rolling(model.lookback_bars, min_periods=model.lookback_bars).cov(log_b)
                / log_b.rolling(model.lookback_bars, min_periods=model.lookback_bars).var()
            ).clip(0.10, 10.0)
            intercept = (
                log_a.rolling(model.lookback_bars, min_periods=model.lookback_bars).mean()
                - beta * log_b.rolling(model.lookback_bars, min_periods=model.lookback_bars).mean()
            )
        elif model.method == "kalman":
            beta, intercept = self._kalman_coefficients(log_a, log_b, model)
        else:
            beta = pd.Series(model.hedge_ratio, index=log_a.index, dtype=float)
            intercept = pd.Series(model.intercept, index=log_a.index, dtype=float)
        spread = log_a - intercept - beta * log_b
        # Baseline moments are delayed one bar. The observation at t may trigger a
        # decision at t but cannot alter its own normalisation statistics.
        history = spread.shift(1)
        mean = history.rolling(model.lookback_bars, min_periods=model.lookback_bars).mean()
        standard_deviation = history.rolling(
            model.lookback_bars, min_periods=model.lookback_bars
        ).std()
        zscore = (spread - mean) / standard_deviation.replace(0.0, np.nan)
        spread_volatility = (
            spread.diff().shift(1).rolling(
                self.volatility_lookback_bars,
                min_periods=self.volatility_lookback_bars,
            ).std()
        )
        return pd.DataFrame(
            {
                "hedge_ratio": beta,
                "intercept": intercept,
                "spread": spread,
                "zscore": zscore,
                "spread_volatility": spread_volatility,
            },
            index=panel.prices.index,
        )

    def features(self, panel: MarketPanel) -> dict[str, pd.DataFrame]:
        """Expose causal diagnostics without embedding large frames in reports."""

        panel.validate()
        return {model.pair_id: self._features(panel, model) for model in self.models}

    def generate(self, panel: MarketPanel) -> StrategyOutput:
        panel.validate()
        weights = empty_weights(panel)
        features_by_pair = self.features(panel)
        events = {
            "entries": 0,
            "mean_reversion_exits": 0,
            "spread_stops": 0,
            "time_stops": 0,
            "cooldown_blocked_entries": 0,
        }
        per_pair_cap = min(self.maximum_pair_gross, 1.0 / len(self.models))
        hedge_groups: dict[str, tuple[str, ...]] = {}
        for model in self.models:
            if model.asset_a not in panel.prices or model.asset_b not in panel.prices:
                raise ValueError(f"selected pair {model.pair_id} is absent from the panel")
            features = features_by_pair[model.pair_id]
            state = 0
            holding_bars = 0
            cooldown_remaining = 0
            for timestamp in panel.prices.index:
                zscore = float(cast(float, features.at[timestamp, "zscore"]))
                beta = float(cast(float, features.at[timestamp, "hedge_ratio"]))
                volatility = float(cast(float, features.at[timestamp, "spread_volatility"]))
                after_start = self.trade_start is None or timestamp >= self.trade_start
                tradable = True
                if panel.tradable is not None:
                    values = panel.tradable.loc[timestamp, [model.asset_a, model.asset_b]]
                    tradable = bool(values.eq(True).all())
                finite = all(math.isfinite(value) for value in (zscore, beta, volatility))
                if state != 0:
                    holding_bars += 1
                    reason: str | None = None
                    if not after_start or not tradable or not finite or abs(zscore) >= self.stop_z:
                        reason = "spread_stops"
                    elif holding_bars >= self.max_holding_bars:
                        reason = "time_stops"
                    elif abs(zscore) <= self.exit_z:
                        reason = "mean_reversion_exits"
                    if reason is not None:
                        state = 0
                        holding_bars = 0
                        cooldown_remaining = self.cooldown_bars
                        events[reason] += 1
                if state == 0:
                    if cooldown_remaining > 0:
                        if after_start and tradable and finite and self.enter_z <= abs(zscore) < self.stop_z:
                            events["cooldown_blocked_entries"] += 1
                        cooldown_remaining -= 1
                    elif after_start and tradable and finite and self.enter_z <= abs(zscore) < self.stop_z:
                        state = -1 if zscore > 0.0 else 1
                        holding_bars = 0
                        events["entries"] += 1
                if state == 0 or not finite:
                    continue
                volatility_scale = min(1.0, self.target_spread_volatility / max(volatility, 1e-12))
                gross = per_pair_cap * volatility_scale
                denominator = 1.0 + abs(beta)
                current_a = float(cast(Any, weights.at[timestamp, model.asset_a]))
                current_b = float(cast(Any, weights.at[timestamp, model.asset_b]))
                weights.at[timestamp, model.asset_a] = current_a + state * gross / denominator
                weights.at[timestamp, model.asset_b] = (
                    current_b - state * gross * beta / denominator
                )
            hedge_groups[model.pair_id] = (model.asset_a, model.asset_b)
        return StrategyOutput(
            name=self.name,
            risk_tier=self.risk_tier,
            weights=weights,
            diagnostics={
                "method": "frozen_pair_selection_causal_dynamic_hedges",
                "selected_pairs": [model.pair_id for model in self.models],
                "hedge_methods": {model.pair_id: model.method for model in self.models},
                "events": events,
                "sizing": "inverse_spread_volatility_capped",
                "loss_scaling": False,
                "unbounded_averaging_down": False,
                "trade_start": self.trade_start.isoformat() if self.trade_start is not None else None,
            },
            hedge_groups=hedge_groups,
        )


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
            if abs(z) >= self.stop_z:
                state = 0
            elif state == 0:
                if z >= self.enter_z:
                    state = -1
                elif z <= -self.enter_z:
                    state = 1
            elif abs(z) <= self.exit_z:
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
            hedge_groups={"statistical_pair": (column_a, column_b)},
        )
