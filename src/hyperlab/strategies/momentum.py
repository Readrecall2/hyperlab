from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Literal, cast

import numpy as np
import pandas as pd

from hyperlab.models import MarketPanel, StrategyOutput
from hyperlab.strategies.helpers import columns_by, empty_weights, rebalance_mask

SignalVariant = Literal["time_series", "breakout", "combined"]


def _as_float(value: object) -> float:
    return float(cast(Any, value))


@dataclass(frozen=True, slots=True)
class MomentumRegimeConfig:
    lookback_bars: int = 72
    baseline_bars: int = 30 * 24
    trend_strength: float = 1.0
    calm_volatility_ratio: float = 0.70
    chaos_volatility_ratio: float = 1.25

    def __post_init__(self) -> None:
        if self.lookback_bars < 12 or self.baseline_bars <= self.lookback_bars:
            raise ValueError("regime windows require baseline_bars > lookback_bars >= 12")
        if not math.isfinite(self.trend_strength) or self.trend_strength <= 0.0:
            raise ValueError("trend_strength must be finite and positive")
        if not 0.0 < self.calm_volatility_ratio < 1.0:
            raise ValueError("calm_volatility_ratio must be in (0, 1)")
        if not math.isfinite(self.chaos_volatility_ratio) or self.chaos_volatility_ratio <= 1.0:
            raise ValueError("chaos_volatility_ratio must be greater than one")


def classify_market_regimes(
    panel: MarketPanel,
    config: MomentumRegimeConfig,
) -> pd.Series:
    """Classify calm, directional and chaotic markets from observations before ``t``."""

    perps = columns_by(panel, exchange="HL", kind="perp")
    if not perps:
        raise ValueError("momentum regimes require at least one Hyperliquid perp")
    market_return = panel.prices[perps].pct_change(fill_method=None).mean(axis=1).fillna(0.0)
    past = market_return.shift(1)
    trailing_mean = past.rolling(config.lookback_bars, min_periods=config.lookback_bars).mean()
    trailing_vol = past.rolling(config.lookback_bars, min_periods=config.lookback_bars).std(ddof=0)
    baseline_vol = past.rolling(config.baseline_bars, min_periods=config.baseline_bars).std(ddof=0)
    strength = trailing_mean.abs() * math.sqrt(config.lookback_bars) / trailing_vol.replace(
        0.0, np.nan
    )
    ready = trailing_mean.notna() & trailing_vol.notna() & baseline_vol.gt(0.0)
    regimes = pd.Series("warmup", index=panel.prices.index, dtype="string", name="regime")
    regimes.loc[ready] = "neutral"
    chaos = ready & trailing_vol.ge(baseline_vol * config.chaos_volatility_ratio)
    trending = ready & ~chaos & strength.ge(config.trend_strength)
    regimes.loc[trending & trailing_mean.gt(0.0)] = "trend_up"
    regimes.loc[trending & trailing_mean.lt(0.0)] = "trend_down"
    calm = (
        ready
        & ~chaos
        & ~trending
        & trailing_vol.le(baseline_vol * config.calm_volatility_ratio)
    )
    regimes.loc[calm] = "calm"
    regimes.loc[chaos] = "chaos"
    return regimes


@dataclass(slots=True)
class RobustMomentumStrategy:
    """Directional Phase-09 strategy with explicit regimes and deployable risk caps."""

    name: str = "momentum_regime"
    risk_tier: str = "3 - offensif directionnel"
    signal_variant: SignalVariant = "combined"
    horizons: tuple[int, ...] = (24, 72, 168)
    breakout_lookback_bars: int = 30 * 24
    volatility_lookback_bars: int = 72
    regime_lookback_bars: int = 72
    regime_baseline_bars: int = 30 * 24
    regime_trend_strength: float = 1.0
    calm_volatility_ratio: float = 0.70
    chaos_volatility_ratio: float = 1.25
    target_annual_volatility: float = 0.15
    minimum_signal: float = 0.20
    volume_confirmation_weight: float = 0.20
    oi_confirmation_weight: float = 2.0
    funding_penalty: float = 2_000.0
    assets_to_trade: int = 3
    maximum_gross_exposure: float = 1.0
    maximum_asset_weight: float = 0.35
    maximum_pairwise_correlation: float = 0.80
    correlation_lookback_bars: int = 168
    stop_volatility_multiple: float = 4.0
    stop_cooldown_bars: int = 12
    liquidation_lookback_bars: int = 168
    liquidation_spike_z: float = 4.0
    liquidation_spike_multiple: float = 10.0
    liquidation_cooldown_bars: int = 12
    rebalance_bars: int = 4
    trade_start: pd.Timestamp | None = None

    def __post_init__(self) -> None:
        if self.signal_variant not in {"time_series", "breakout", "combined"}:
            raise ValueError("unknown momentum signal_variant")
        if not self.horizons or len(set(self.horizons)) != len(self.horizons):
            raise ValueError("horizons must be non-empty and unique")
        if any(horizon < 2 for horizon in self.horizons):
            raise ValueError("momentum horizons must be at least two bars")
        for name, value, minimum in (
            ("breakout_lookback_bars", self.breakout_lookback_bars, 12),
            ("volatility_lookback_bars", self.volatility_lookback_bars, 12),
            ("correlation_lookback_bars", self.correlation_lookback_bars, 12),
            ("liquidation_lookback_bars", self.liquidation_lookback_bars, 12),
            ("rebalance_bars", self.rebalance_bars, 1),
        ):
            if value < minimum:
                raise ValueError(f"{name} must be at least {minimum}")
        if self.assets_to_trade < 1:
            raise ValueError("assets_to_trade must be positive")
        if not 0.0 < self.target_annual_volatility <= 1.0:
            raise ValueError("target_annual_volatility must be in (0, 1]")
        if not math.isfinite(self.minimum_signal) or self.minimum_signal < 0.0:
            raise ValueError("minimum_signal must be non-negative")
        for name, risk_value in (
            ("volume_confirmation_weight", self.volume_confirmation_weight),
            ("oi_confirmation_weight", self.oi_confirmation_weight),
            ("funding_penalty", self.funding_penalty),
            ("stop_volatility_multiple", self.stop_volatility_multiple),
            ("liquidation_spike_z", self.liquidation_spike_z),
        ):
            if not math.isfinite(risk_value) or risk_value < 0.0:
                raise ValueError(f"{name} must be finite and non-negative")
        if not math.isfinite(self.liquidation_spike_multiple) or self.liquidation_spike_multiple <= 1.0:
            raise ValueError("liquidation_spike_multiple must be greater than one")
        if not 0.0 < self.maximum_gross_exposure <= 1.0:
            raise ValueError("maximum_gross_exposure must be in (0, 1x]")
        if not 0.0 < self.maximum_asset_weight <= self.maximum_gross_exposure:
            raise ValueError("maximum_asset_weight must be positive and no larger than gross")
        if not 0.0 <= self.maximum_pairwise_correlation <= 1.0:
            raise ValueError("maximum_pairwise_correlation must be in [0, 1]")
        if self.stop_cooldown_bars < 0 or self.liquidation_cooldown_bars < 0:
            raise ValueError("cooldowns must be non-negative")
        if self.trade_start is not None:
            timestamp = pd.Timestamp(self.trade_start)
            if timestamp.tz is None:
                raise ValueError("trade_start must be timezone-aware")
            self.trade_start = timestamp.tz_convert("UTC")
        _ = self.regime_config

    @property
    def regime_config(self) -> MomentumRegimeConfig:
        return MomentumRegimeConfig(
            lookback_bars=self.regime_lookback_bars,
            baseline_bars=self.regime_baseline_bars,
            trend_strength=self.regime_trend_strength,
            calm_volatility_ratio=self.calm_volatility_ratio,
            chaos_volatility_ratio=self.chaos_volatility_ratio,
        )

    def features(self, panel: MarketPanel) -> dict[str, pd.DataFrame]:
        panel.validate()
        perps = columns_by(panel, exchange="HL", kind="perp")
        if not perps:
            raise ValueError("momentum requires Hyperliquid perpetual instruments")
        prices = panel.prices[perps].astype(float)
        returns = prices.pct_change(fill_method=None)
        volatility = returns.rolling(
            self.volatility_lookback_bars,
            min_periods=self.volatility_lookback_bars,
        ).std(ddof=0).replace(0.0, np.nan)
        horizon_scores = [
            prices.pct_change(horizon, fill_method=None)
            .div(volatility * math.sqrt(horizon))
            .clip(-3.0, 3.0)
            for horizon in self.horizons
        ]
        time_series = sum(horizon_scores[1:], horizon_scores[0].copy()) / len(horizon_scores)
        prior_high = prices.shift(1).rolling(
            self.breakout_lookback_bars,
            min_periods=self.breakout_lookback_bars,
        ).max()
        prior_low = prices.shift(1).rolling(
            self.breakout_lookback_bars,
            min_periods=self.breakout_lookback_bars,
        ).min()
        breakout = pd.DataFrame(
            np.where(prices.gt(prior_high), 1.0, np.where(prices.lt(prior_low), -1.0, 0.0)),
            index=prices.index,
            columns=prices.columns,
        ).where(prior_high.notna() & prior_low.notna())
        if self.signal_variant == "time_series":
            raw_signal = time_series
        elif self.signal_variant == "breakout":
            raw_signal = breakout
        else:
            raw_signal = 0.65 * time_series + 0.35 * breakout

        volume_baseline = panel.volume_usd[perps].shift(1).rolling(
            self.volatility_lookback_bars,
            min_periods=self.volatility_lookback_bars,
        ).mean()
        volume_confirmation = (
            panel.volume_usd[perps].div(volume_baseline).replace(0.0, np.nan).map(np.log).clip(-1.0, 1.0)
        )
        if panel.open_interest_usd is None:
            oi_confirmation = pd.DataFrame(np.nan, index=prices.index, columns=prices.columns)
        else:
            oi_confirmation = panel.open_interest_usd[perps].pct_change(
                min(self.horizons),
                fill_method=None,
            ).clip(-0.25, 0.25)
        funding_cost = panel.funding[perps].rolling(24, min_periods=24).mean()
        direction = raw_signal.map(lambda value: float(np.sign(value)))
        magnitude = (
            raw_signal.abs()
            + self.volume_confirmation_weight * volume_confirmation
            + self.oi_confirmation_weight * oi_confirmation
            - direction * funding_cost * self.funding_penalty
        ).clip(lower=0.0)
        score = direction * magnitude
        return {
            "time_series": time_series,
            "breakout": breakout,
            "volume_confirmation": volume_confirmation,
            "oi_confirmation": oi_confirmation,
            "funding_cost": funding_cost,
            "realized_volatility": volatility,
            "score": score,
        }

    def _liquidation_spikes(self, panel: MarketPanel, perps: list[str]) -> pd.Series:
        if panel.liquidation_usd is None:
            return pd.Series(False, index=panel.prices.index)
        total = panel.liquidation_usd[perps].sum(axis=1, min_count=1)
        past_mean = total.shift(1).rolling(
            self.liquidation_lookback_bars,
            min_periods=self.liquidation_lookback_bars,
        ).mean()
        past_std = total.shift(1).rolling(
            self.liquidation_lookback_bars,
            min_periods=self.liquidation_lookback_bars,
        ).std(ddof=0)
        zscore = (total - past_mean).div(past_std.replace(0.0, np.nan))
        ratio_spike = total.ge(past_mean * self.liquidation_spike_multiple) & past_mean.gt(0.0)
        return (zscore.ge(self.liquidation_spike_z) | ratio_spike).fillna(False)

    def generate(self, panel: MarketPanel) -> StrategyOutput:
        feature_set = self.features(panel)
        score = feature_set["score"]
        volatility = feature_set["realized_volatility"]
        perps = list(score.columns)
        returns = panel.prices[perps].pct_change(fill_method=None)
        regimes = classify_market_regimes(panel, self.regime_config)
        spikes = self._liquidation_spikes(panel, perps)
        rebalance = rebalance_mask(panel.prices.index, self.rebalance_bars)
        weights = empty_weights(panel)
        current = pd.Series(0.0, index=panel.prices.columns)
        entry_price: dict[str, float] = {}
        extreme_price: dict[str, float] = {}
        stop_blocked_until: dict[str, int] = {}
        global_blocked_until = -1
        events = {
            "entries": 0,
            "volatility_stops": 0,
            "stop_cooldown_blocked": 0,
            "liquidation_spikes": 0,
            "liquidation_cooldown_bars": 0,
            "correlation_rejections": 0,
            "chaos_flat_bars": 0,
        }
        target_hourly_volatility = self.target_annual_volatility / math.sqrt(365.0 * 24.0)

        for row, timestamp in enumerate(panel.prices.index):
            for instrument in perps:
                held = float(current[instrument])
                if held == 0.0:
                    continue
                if panel.tradable is not None:
                    tradable_value = panel.tradable.at[timestamp, instrument]
                    if pd.isna(tradable_value) or not bool(tradable_value):
                        current[instrument] = 0.0
                        entry_price.pop(instrument, None)
                        extreme_price.pop(instrument, None)
                        continue
                price = _as_float(panel.prices.at[timestamp, instrument])
                if held > 0.0:
                    extreme_price[instrument] = max(extreme_price[instrument], price)
                    move = math.log(price / extreme_price[instrument])
                else:
                    extreme_price[instrument] = min(extreme_price[instrument], price)
                    move = math.log(extreme_price[instrument] / price)
                realized = _as_float(volatility.at[timestamp, instrument])
                stop_distance = self.stop_volatility_multiple * realized
                if math.isfinite(stop_distance) and stop_distance > 0.0 and move <= -stop_distance:
                    current[instrument] = 0.0
                    entry_price.pop(instrument, None)
                    extreme_price.pop(instrument, None)
                    stop_blocked_until[instrument] = row + self.stop_cooldown_bars
                    events["volatility_stops"] += 1

            if bool(spikes.loc[timestamp]):
                events["liquidation_spikes"] += 1
                global_blocked_until = max(global_blocked_until, row + self.liquidation_cooldown_bars)
                current[:] = 0.0
                entry_price.clear()
                extreme_price.clear()

            before_start = self.trade_start is not None and timestamp < self.trade_start
            regime = str(regimes.loc[timestamp])
            if before_start or row <= global_blocked_until or regime in {"warmup", "chaos"}:
                if row <= global_blocked_until:
                    events["liquidation_cooldown_bars"] += 1
                if regime == "chaos":
                    events["chaos_flat_bars"] += 1
                current[:] = 0.0
                entry_price.clear()
                extreme_price.clear()
                weights.loc[timestamp] = current
                continue

            if bool(rebalance.loc[timestamp]):
                desired = pd.Series(0.0, index=panel.prices.columns)
                candidates = score.loc[timestamp].dropna()
                candidates = candidates[candidates.abs().ge(self.minimum_signal)]
                if panel.tradable is not None:
                    tradable_now = panel.tradable.loc[timestamp, candidates.index].eq(True)
                    candidates = candidates.loc[tradable_now]
                if regime == "trend_up":
                    candidates = candidates[candidates.gt(0.0)]
                elif regime == "trend_down":
                    candidates = candidates[candidates.lt(0.0)]
                start = max(0, row - self.correlation_lookback_bars + 1)
                correlations = returns.iloc[start : row + 1].corr()
                selected: list[str] = []
                for instrument in candidates.abs().sort_values(ascending=False).index:
                    if row <= stop_blocked_until.get(str(instrument), -1):
                        events["stop_cooldown_blocked"] += 1
                        continue
                    if any(
                        abs(_as_float(correlations.at[instrument, prior]))
                        > self.maximum_pairwise_correlation
                        for prior in selected
                        if instrument in correlations.index and prior in correlations.columns
                    ):
                        events["correlation_rejections"] += 1
                        continue
                    selected.append(str(instrument))
                    if len(selected) >= self.assets_to_trade:
                        break
                regime_scale = 0.50 if regime == "calm" else 0.75 if regime == "neutral" else 1.0
                for instrument in selected:
                    realized = _as_float(volatility.at[timestamp, instrument])
                    if not math.isfinite(realized) or realized <= 0.0:
                        continue
                    risk_weight = min(
                        self.maximum_asset_weight,
                        target_hourly_volatility / realized,
                    )
                    strength = min(1.0, max(0.25, abs(_as_float(candidates[instrument]))))
                    desired[instrument] = (
                        math.copysign(risk_weight * strength * regime_scale, candidates[instrument])
                    )
                gross = float(desired.abs().sum())
                if gross > self.maximum_gross_exposure:
                    desired *= self.maximum_gross_exposure / gross
                previous = current.copy()
                current = desired
                for instrument in perps:
                    old = float(previous[instrument])
                    new = float(current[instrument])
                    if new == 0.0:
                        entry_price.pop(instrument, None)
                        extreme_price.pop(instrument, None)
                    elif old == 0.0 or math.copysign(1.0, old) != math.copysign(1.0, new):
                        price = _as_float(panel.prices.at[timestamp, instrument])
                        entry_price[instrument] = price
                        extreme_price[instrument] = price
                        events["entries"] += 1
            weights.loc[timestamp] = current

        return StrategyOutput(
            name=self.name,
            risk_tier=self.risk_tier,
            weights=weights,
            diagnostics={
                "logic": "directional momentum/breakout with causal regimes",
                "signal_variant": self.signal_variant,
                "horizons": list(self.horizons),
                "target_annual_volatility": self.target_annual_volatility,
                "deployable_leverage_cap": self.maximum_gross_exposure,
                "maximum_asset_weight": self.maximum_asset_weight,
                "maximum_pairwise_correlation": self.maximum_pairwise_correlation,
                "funding_is_cost_and_confirmation": True,
                "volume_and_oi_confirmation": True,
                "liquidation_data_available": panel.liquidation_usd is not None,
                "events": events,
                "martingale": False,
                "loss_scaling": False,
            },
        )


@dataclass(slots=True)
class MomentumRegimeStrategy:
    """Legacy compact baseline retained for backwards-compatible demos."""

    name: str = "momentum_regime"
    risk_tier: str = "3 - offensif"
    lookback_hours: int = 72
    volatility_hours: int = 72
    assets_to_trade: int = 3
    minimum_signal: float = 0.25
    funding_penalty: float = 2_000.0
    rebalance_hours: int = 4

    def generate(self, panel: MarketPanel) -> StrategyOutput:
        weights = empty_weights(panel)
        perps = columns_by(panel, exchange="HL", kind="perp")
        returns = panel.prices[perps].pct_change(fill_method=None)
        momentum = panel.prices[perps].pct_change(self.lookback_hours, fill_method=None)
        volatility = returns.rolling(
            self.volatility_hours,
            min_periods=self.volatility_hours,
        ).std().replace(0.0, np.nan)
        funding_mean = panel.funding[perps].rolling(24, min_periods=24).mean()
        raw_score = momentum / (volatility * np.sqrt(self.lookback_hours))
        score = raw_score - funding_mean * self.funding_penalty
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


__all__ = [
    "MomentumRegimeConfig",
    "MomentumRegimeStrategy",
    "RobustMomentumStrategy",
    "SignalVariant",
    "classify_market_regimes",
]
