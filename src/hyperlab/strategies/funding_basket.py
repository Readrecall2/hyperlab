from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal, cast

import numpy as np
import pandas as pd

from hyperlab.models import MarketPanel, StrategyOutput
from hyperlab.strategies.helpers import columns_by, empty_weights, rebalance_mask

_EPSILON = 1e-12


@dataclass(frozen=True, slots=True)
class FundingBasketFeatures:
    """Causal cross-sectional inputs used at each decision timestamp."""

    funding_score: pd.DataFrame
    persistence: pd.DataFrame
    annualized_volatility: pd.DataFrame
    volume_usd: pd.DataFrame
    depth_usd: pd.DataFrame
    market_age_hours: pd.DataFrame
    momentum: pd.DataFrame
    beta_btc: pd.DataFrame
    beta_eth: pd.DataFrame
    eligible: pd.DataFrame
    short_allowed: pd.DataFrame


def shrunk_covariance(returns: pd.DataFrame, *, shrinkage: float) -> np.ndarray:
    """Return a finite diagonal-target shrinkage covariance matrix.

    Shrinking toward the sample diagonal preserves asset-specific variance while
    damping unstable cross-asset correlations. A tiny diagonal floor makes the
    result numerically positive definite without hiding observed returns.
    """

    if not 0.0 <= shrinkage <= 1.0 or not math.isfinite(shrinkage):
        raise ValueError("shrinkage must be finite and in [0, 1]")
    if returns.empty or returns.shape[1] == 0:
        raise ValueError("returns cannot be empty")
    clean = returns.replace([np.inf, -np.inf], np.nan).dropna(axis=0, how="any")
    if len(clean) < 2:
        raise ValueError("at least two complete return observations are required")
    sample = np.atleast_2d(np.cov(clean.to_numpy(dtype=float), rowvar=False, ddof=1))
    diagonal = np.diag(np.diag(sample))
    covariance = (1.0 - shrinkage) * sample + shrinkage * diagonal
    floor = max(float(np.trace(covariance)) / max(len(covariance), 1) * 1e-9, 1e-15)
    covariance = (covariance + covariance.T) * 0.5 + np.eye(len(covariance)) * floor
    if not np.isfinite(covariance).all():
        raise ValueError("covariance contains non-finite values")
    return covariance


def _empty_feature_frame(panel: MarketPanel, value: float = math.nan) -> pd.DataFrame:
    return pd.DataFrame(value, index=panel.prices.index, columns=panel.prices.columns)


def _expand(panel: MarketPanel, frame: pd.DataFrame, value: float = math.nan) -> pd.DataFrame:
    expanded = _empty_feature_frame(panel, value)
    expanded.loc[:, frame.columns] = frame
    return expanded


def _rolling_factor_betas(
    returns: pd.DataFrame,
    *,
    btc_column: str,
    eth_column: str,
    lookback: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    btc = returns[btc_column]
    eth = returns[eth_column]
    variance_btc = btc.rolling(lookback, min_periods=lookback).var()
    variance_eth = eth.rolling(lookback, min_periods=lookback).var()
    covariance_factors = btc.rolling(lookback, min_periods=lookback).cov(eth)
    determinant = variance_btc * variance_eth - covariance_factors**2
    valid = determinant.abs().gt(_EPSILON)
    beta_btc = pd.DataFrame(math.nan, index=returns.index, columns=returns.columns)
    beta_eth = beta_btc.copy()
    for column in returns:
        covariance_btc = returns[column].rolling(lookback, min_periods=lookback).cov(btc)
        covariance_eth = returns[column].rolling(lookback, min_periods=lookback).cov(eth)
        beta_btc[column] = (
            (covariance_btc * variance_eth - covariance_eth * covariance_factors) / determinant
        ).where(valid)
        beta_eth[column] = (
            (covariance_eth * variance_btc - covariance_btc * covariance_factors) / determinant
        ).where(valid)
    return beta_btc, beta_eth


def _nullspace(matrix: np.ndarray) -> np.ndarray:
    _left, singular_values, right = np.linalg.svd(matrix, full_matrices=True)
    tolerance = max(matrix.shape) * max(float(singular_values.max(initial=0.0)), 1.0) * 1e-10
    rank = int(np.sum(singular_values > tolerance))
    return right[rank:].T.copy()


def _capped_side_weights(
    volatility: pd.Series,
    *,
    total: float,
    cap: float,
) -> pd.Series:
    inverse = 1.0 / volatility.clip(lower=1e-8)
    result = pd.Series(0.0, index=volatility.index)
    remaining = list(volatility.index)
    budget = total
    while remaining and budget > _EPSILON:
        allocation = inverse[remaining] / float(inverse[remaining].sum()) * budget
        capped = allocation[allocation.gt(cap)]
        if capped.empty:
            result.loc[remaining] = allocation
            break
        for column in capped.index:
            result[column] = cap
            budget -= cap
            remaining.remove(column)
    return result


@dataclass(slots=True)
class FundingBasketStrategy:
    """Causal Hyperliquid funding spread basket with explicit neutralization."""

    name: str = "funding_basket"
    risk_tier: str = "2 — équilibré"
    mode: Literal["optimized", "ranking"] = "optimized"
    lookback_hours: int = 24
    volatility_lookback_hours: int = 168
    beta_lookback_hours: int = 336
    liquidity_lookback_hours: int = 24
    momentum_hours: int = 24
    legs_per_side: int = 2
    min_funding_spread_hourly: float = 0.000004
    min_volume_usd: float = 10_000_000.0
    min_depth_usd: float = 250_000.0
    min_market_age_hours: int = 24 * 30
    rebalance_hours: int = 8
    squeeze_guard_return: float = 0.08
    covariance_shrinkage: float = 0.35
    risk_aversion: float = 1.0
    turnover_penalty: float = 2.0
    target_gross_leverage: float = 1.0
    max_asset_weight: float = 0.25
    excluded_assets: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.mode not in {"optimized", "ranking"}:
            raise ValueError("mode must be optimized or ranking")
        for name in (
            "lookback_hours",
            "volatility_lookback_hours",
            "beta_lookback_hours",
            "liquidity_lookback_hours",
            "momentum_hours",
            "legs_per_side",
            "min_market_age_hours",
            "rebalance_hours",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name in (
            "min_funding_spread_hourly",
            "min_volume_usd",
            "min_depth_usd",
            "risk_aversion",
            "turnover_penalty",
            "target_gross_leverage",
            "max_asset_weight",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(f"{name} must be finite and non-negative")
        if not 0.0 <= self.covariance_shrinkage <= 1.0:
            raise ValueError("covariance_shrinkage must be in [0, 1]")
        if not math.isfinite(self.squeeze_guard_return) or self.squeeze_guard_return <= 0.0:
            raise ValueError("squeeze_guard_return must be finite and positive")
        if len(self.excluded_assets) != len(set(self.excluded_assets)) or any(
            not isinstance(asset, str) or not asset.strip() for asset in self.excluded_assets
        ):
            raise ValueError("excluded_assets must contain unique non-empty strings")

    def features(self, panel: MarketPanel) -> FundingBasketFeatures:
        panel.validate()
        perps = columns_by(panel, exchange="HL", kind="perp")
        btc_column = "HL:BTC:perp"
        eth_column = "HL:ETH:perp"
        if btc_column not in perps or eth_column not in perps:
            raise ValueError("funding basket requires BTC and ETH Hyperliquid perp references")

        funding = panel.funding[perps].astype(float)
        rolling = funding.rolling(self.lookback_hours, min_periods=self.lookback_hours)
        funding_mean = rolling.mean()
        persistence = rolling.apply(
            lambda values: abs(float(np.sign(values).mean())),
            raw=True,
        )
        funding_score = funding_mean * (0.5 + 0.5 * persistence)
        returns = panel.prices[perps].astype(float).map(math.log).diff()
        volatility = (
            returns.rolling(
                self.volatility_lookback_hours,
                min_periods=self.volatility_lookback_hours,
            ).std()
            * math.sqrt(24.0 * 365.0)
        )
        volume = panel.volume_usd[perps].rolling(
            self.liquidity_lookback_hours,
            min_periods=self.liquidity_lookback_hours,
        ).median()
        if panel.depth_usd is None:
            depth = pd.DataFrame(math.nan, index=panel.prices.index, columns=perps)
        else:
            depth = panel.depth_usd[perps].rolling(
                self.liquidity_lookback_hours,
                min_periods=self.liquidity_lookback_hours,
            ).median()
        observable = panel.prices[perps].notna() & funding.notna()
        if panel.available_at is not None:
            for column in perps:
                available = pd.to_datetime(panel.available_at[column], utc=True, errors="coerce")
                observable[column] &= available.le(panel.prices.index.to_series())
        market_age = observable.cumsum().astype(float)
        momentum = panel.prices[perps].pct_change(self.momentum_hours, fill_method=None)
        beta_btc, beta_eth = _rolling_factor_betas(
            returns,
            btc_column=btc_column,
            eth_column=eth_column,
            lookback=self.beta_lookback_hours,
        )

        eligible = (
            funding_score.notna()
            & volatility.notna()
            & volatility.gt(0.0)
            & beta_btc.notna()
            & beta_eth.notna()
            & volume.ge(self.min_volume_usd)
            & depth.ge(self.min_depth_usd)
            & market_age.ge(self.min_market_age_hours)
        )
        if panel.finality is not None:
            eligible &= panel.finality[perps].eq(True)
        if panel.tradable is not None:
            eligible &= panel.tradable[perps].eq(True)
        excluded = {asset.upper() for asset in self.excluded_assets}
        for column in perps:
            asset = column.split(":")[1].upper()
            if asset in excluded:
                eligible[column] = False
        short_allowed = momentum.le(self.squeeze_guard_return) & eligible
        return FundingBasketFeatures(
            funding_score=_expand(panel, funding_score),
            persistence=_expand(panel, persistence),
            annualized_volatility=_expand(panel, volatility),
            volume_usd=_expand(panel, volume),
            depth_usd=_expand(panel, depth),
            market_age_hours=_expand(panel, market_age, 0.0),
            momentum=_expand(panel, momentum),
            beta_btc=_expand(panel, beta_btc, 0.0),
            beta_eth=_expand(panel, beta_eth, 0.0),
            eligible=_expand(panel, eligible.astype(float), 0.0).astype(bool),
            short_allowed=_expand(panel, short_allowed.astype(float), 0.0).astype(bool),
        )

    def _ranking_weights(
        self,
        timestamp: pd.Timestamp,
        features: FundingBasketFeatures,
        candidates: list[str],
    ) -> pd.Series:
        result = pd.Series(0.0, index=features.funding_score.columns)
        funding_row = cast(pd.Series, features.funding_score.loc[timestamp])
        ranked = funding_row.reindex(candidates).astype(float)
        ranked = ranked.sort_values(kind="stable")
        longs = list(ranked.index[: self.legs_per_side])
        short_pool = [
            column
            for column in reversed(ranked.index.tolist())
            if bool(features.short_allowed.at[timestamp, column]) and column not in longs
        ]
        shorts = short_pool[: self.legs_per_side]
        if len(longs) != self.legs_per_side or len(shorts) != self.legs_per_side:
            return result
        spread = float(ranked.loc[shorts].mean() - ranked.loc[longs].mean())
        if spread < self.min_funding_spread_hourly:
            return result
        side_total = self.target_gross_leverage * 0.5
        volatility_row = cast(pd.Series, features.annualized_volatility.loc[timestamp])
        long_volatility = volatility_row.reindex(longs).astype(float)
        short_volatility = volatility_row.reindex(shorts).astype(float)
        result.loc[longs] = _capped_side_weights(
            long_volatility,
            total=side_total,
            cap=self.max_asset_weight,
        )
        result.loc[shorts] = -_capped_side_weights(
            short_volatility,
            total=side_total,
            cap=self.max_asset_weight,
        )
        return result

    def _optimized_weights(
        self,
        panel: MarketPanel,
        timestamp: pd.Timestamp,
        features: FundingBasketFeatures,
        candidates: list[str],
        previous: pd.Series,
    ) -> pd.Series:
        result = pd.Series(0.0, index=panel.prices.columns)
        safe_candidates = [
            column for column in candidates if bool(features.short_allowed.at[timestamp, column])
        ]
        if len(safe_candidates) < 4:
            return result
        score = features.funding_score.loc[timestamp].reindex(safe_candidates).astype(float)
        spread = float(score.max() - score.min())
        if spread < self.min_funding_spread_hourly:
            return result
        volatility = (
            features.annualized_volatility.loc[timestamp].reindex(safe_candidates).astype(float)
        )
        alpha = -(score - float(score.mean())) / volatility.clip(lower=1e-8)
        alpha_scale = float(alpha.abs().max())
        if alpha_scale <= _EPSILON:
            return result
        alpha = alpha / alpha_scale

        row = panel.prices.index.get_loc(timestamp)
        start = max(0, cast(int, row) - self.beta_lookback_hours)
        history = (
            panel.prices[safe_candidates]
            .iloc[start : cast(int, row) + 1]
            .astype(float)
            .map(math.log)
            .diff()
        )
        try:
            covariance = shrunk_covariance(history, shrinkage=self.covariance_shrinkage)
        except ValueError:
            return result
        average_variance = max(float(np.trace(covariance)) / len(covariance), 1e-12)
        covariance /= average_variance
        beta_btc = features.beta_btc.loc[timestamp].reindex(safe_candidates).to_numpy(dtype=float)
        beta_eth = features.beta_eth.loc[timestamp].reindex(safe_candidates).to_numpy(dtype=float)
        constraints = np.vstack((np.ones(len(safe_candidates)), beta_btc, beta_eth))
        nullspace = _nullspace(constraints)
        if nullspace.shape[1] == 0:
            return result

        previous_values = previous.loc[safe_candidates].to_numpy(dtype=float)
        identity = np.eye(len(safe_candidates))
        quadratic = self.risk_aversion * covariance + self.turnover_penalty * identity
        linear = alpha.to_numpy(dtype=float) + self.turnover_penalty * previous_values
        reduced_quadratic = nullspace.T @ quadratic @ nullspace
        reduced_linear = nullspace.T @ linear
        coordinates = np.linalg.solve(
            reduced_quadratic + np.eye(reduced_quadratic.shape[0]) * 1e-10,
            reduced_linear,
        )
        values = nullspace @ coordinates
        gross = float(np.abs(values).sum())
        if gross <= _EPSILON:
            return result
        if gross > self.target_gross_leverage:
            values *= self.target_gross_leverage / gross
        largest = float(np.abs(values).max())
        if largest > self.max_asset_weight:
            values *= self.max_asset_weight / largest
        values[np.abs(values) < 1e-12] = 0.0
        result.loc[safe_candidates] = values
        return result

    def generate(self, panel: MarketPanel) -> StrategyOutput:
        features = self.features(panel)
        weights = empty_weights(panel)
        perps = columns_by(panel, exchange="HL", kind="perp")
        scheduled = rebalance_mask(panel.prices.index, self.rebalance_hours)
        current = pd.Series(0.0, index=panel.prices.columns)
        rebalance_count = 0
        risk_rebalances = 0

        for timestamp in panel.prices.index:
            candidates = [column for column in perps if bool(features.eligible.at[timestamp, column])]
            unavailable_position = any(
                abs(float(current[column])) > _EPSILON and column not in candidates for column in perps
            )
            guarded_short = any(
                float(current[column]) < -_EPSILON
                and not bool(features.short_allowed.at[timestamp, column])
                for column in perps
            )
            should_rebalance = bool(scheduled.loc[timestamp]) or unavailable_position or guarded_short
            if should_rebalance:
                if unavailable_position or guarded_short:
                    risk_rebalances += 1
                if self.mode == "ranking":
                    current = self._ranking_weights(timestamp, features, candidates)
                else:
                    current = self._optimized_weights(
                        panel,
                        timestamp,
                        features,
                        candidates,
                        current,
                    )
                rebalance_count += 1
            weights.loc[timestamp] = current

        active = weights.abs().sum(axis=1).gt(_EPSILON)
        beta_btc_exposure = (weights * features.beta_btc).sum(axis=1).where(active, 0.0)
        beta_eth_exposure = (weights * features.beta_eth).sum(axis=1).where(active, 0.0)
        method = (
            "constrained_shrunk_covariance"
            if self.mode == "optimized"
            else "simple_inverse_vol_ranking"
        )
        return StrategyOutput(
            name=self.name,
            risk_tier=self.risk_tier,
            weights=weights,
            diagnostics={
                "logic": "persistent cross-sectional funding spread",
                "method": method,
                "funding_score": "rolling_mean_x_directional_persistence",
                "inverse_volatility": True,
                "dollar_neutral": True,
                "btc_eth_beta_neutral": self.mode == "optimized",
                "covariance_shrinkage": self.covariance_shrinkage,
                "turnover_penalty": self.turnover_penalty,
                "max_asset_weight": self.max_asset_weight,
                "squeeze_guard_return": self.squeeze_guard_return,
                "rebalance_count": rebalance_count,
                "risk_rebalance_count": risk_rebalances,
                "max_abs_dollar_exposure": float(weights.sum(axis=1).abs().max()),
                "max_abs_btc_beta_exposure": float(beta_btc_exposure.abs().max()),
                "max_abs_eth_beta_exposure": float(beta_eth_exposure.abs().max()),
                "excluded_assets": list(self.excluded_assets),
            },
        )


__all__ = ["FundingBasketFeatures", "FundingBasketStrategy", "shrunk_covariance"]
