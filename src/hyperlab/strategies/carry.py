from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

from hyperlab.models import MarketPanel, StrategyOutput
from hyperlab.strategies.helpers import asset_from, columns_by, empty_weights, rebalance_mask

_HOURS_PER_YEAR = 365.25 * 24.0
_FUNDING_WINDOWS = (8, 24, 72)


def _rolling_sum(values: pd.Series, hours: int) -> pd.Series:
    return values.rolling(hours, min_periods=hours).sum()


def _rolling_mean(values: pd.Series, hours: int) -> pd.Series:
    return values.rolling(hours, min_periods=hours).mean()


@dataclass(slots=True)
class CashAndCarryStrategy:
    """Causal defensive long-spot/short-perp carry selection.

    Target weights are fractions of total research capital. The sizing denominator
    is spot notional plus a conservative perp margin reserve; cash left outside
    ``capital_fraction`` remains uncommitted. Orders are maker attempts and the
    backtester owns the explicit timeout/IOC hedge policy.
    """

    name: str = "cash_and_carry"
    risk_tier: str = "1 — défensif"
    lookback_hours: int = 72
    min_mean_funding_hourly: float = 0.000005
    min_positive_share: float = 0.70
    max_abs_basis_bps: float = 150.0
    min_depth_usd: float = 100_000.0
    min_volume_usd: float = 1_000_000.0
    min_open_interest_usd: float = 5_000_000.0
    max_annualized_volatility: float = 1.50
    max_positions: int = 1
    rebalance_hours: int = 8
    basis_speed_lookback_hours: int = 8
    capital_fraction: float = 0.50
    perp_margin_fraction: float = 1.0
    round_trip_fees_bps: float = 11.0
    estimated_round_trip_slippage_bps: float = 4.0
    benchmark_annual_rate: float = 0.045
    minimum_net_edge_bps: float = 0.0

    def __post_init__(self) -> None:
        for name, value in (
            ("min_mean_funding_hourly", self.min_mean_funding_hourly),
            ("min_depth_usd", self.min_depth_usd),
            ("min_volume_usd", self.min_volume_usd),
            ("min_open_interest_usd", self.min_open_interest_usd),
            ("max_annualized_volatility", self.max_annualized_volatility),
            ("round_trip_fees_bps", self.round_trip_fees_bps),
            ("estimated_round_trip_slippage_bps", self.estimated_round_trip_slippage_bps),
            ("minimum_net_edge_bps", self.minimum_net_edge_bps),
        ):
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(f"{name} must be finite and non-negative")
        if self.lookback_hours < 72:
            raise ValueError("lookback_hours must cover the 72-hour funding signal")
        if self.basis_speed_lookback_hours <= 0 or self.rebalance_hours <= 0:
            raise ValueError("basis speed and rebalance windows must be positive")
        if self.max_positions <= 0:
            raise ValueError("max_positions must be positive")
        if not 0.0 <= self.min_positive_share <= 1.0:
            raise ValueError("min_positive_share must be in [0, 1]")
        if not math.isfinite(self.max_abs_basis_bps) or self.max_abs_basis_bps <= 0.0:
            raise ValueError("max_abs_basis_bps must be finite and positive")
        if not 0.0 < self.capital_fraction <= 1.0:
            raise ValueError("capital_fraction must be in (0, 1]")
        if not math.isfinite(self.perp_margin_fraction) or self.perp_margin_fraction <= 0.0:
            raise ValueError("perp_margin_fraction must be finite and positive")
        if not math.isfinite(self.benchmark_annual_rate) or self.benchmark_annual_rate <= -1.0:
            raise ValueError("benchmark_annual_rate must be finite and greater than -1")

    def features(self, panel: MarketPanel) -> dict[str, pd.DataFrame]:
        """Build per-asset features from observations available at or before each row."""

        panel.validate()
        if panel.depth_usd is None:
            raise ValueError("cash-and-carry requires executable spot and perp depth")
        if panel.open_interest_usd is None:
            raise ValueError("cash-and-carry requires point-in-time perp open interest")

        result: dict[str, pd.DataFrame] = {}
        for perp in columns_by(panel, exchange="HL", kind="perp"):
            asset = asset_from(perp)
            spot = f"HL:{asset}:spot"
            if spot not in panel.prices.columns:
                continue
            spot_price = panel.prices[spot].astype(float)
            perp_price = panel.prices[perp].astype(float)
            funding = panel.funding[perp].astype(float)
            basis = (perp_price / spot_price - 1.0) * 10_000.0
            funding_8h_mean = _rolling_mean(funding, 8)
            funding_24h_mean = _rolling_mean(funding, 24)
            funding_trend = funding_8h_mean - funding_24h_mean
            lag = self.basis_speed_lookback_hours
            # Long spot / short perp benefits only when the signed basis falls.
            # Using |basis| would incorrectly reward convergence of a negative basis.
            basis_speed = (basis.shift(lag) - basis) / float(lag)
            spot_vol = spot_price.pct_change(fill_method=None).rolling(24, min_periods=24).std()
            perp_vol = perp_price.pct_change(fill_method=None).rolling(24, min_periods=24).std()
            annualized_volatility = pd.concat([spot_vol, perp_vol], axis=1).max(axis=1) * math.sqrt(
                _HOURS_PER_YEAR
            )
            frame = pd.DataFrame(index=panel.prices.index)
            for hours in _FUNDING_WINDOWS:
                frame[f"funding_{hours}h"] = _rolling_sum(funding, hours)
            frame["positive_funding_share_72h"] = (
                funding.gt(0.0).rolling(72, min_periods=72).mean()
            )
            frame["funding_trend_hourly"] = funding_trend
            frame["basis_bps"] = basis
            frame["basis_convergence_bps_per_hour"] = basis_speed
            frame["spot_depth_usd"] = panel.depth_usd[spot].astype(float)
            frame["perp_depth_usd"] = panel.depth_usd[perp].astype(float)
            frame["spot_volume_usd"] = panel.volume_usd[spot].astype(float)
            frame["perp_volume_usd"] = panel.volume_usd[perp].astype(float)
            frame["annualized_volatility"] = annualized_volatility
            frame["open_interest_usd"] = panel.open_interest_usd[perp].astype(float)
            projected_funding = funding_8h_mean + funding_trend
            entry_exit_spread_bps = panel.spreads_bps[spot].astype(float) + panel.spreads_bps[
                perp
            ].astype(float)
            fixed_cost_bps = (
                self.round_trip_fees_bps
                + self.estimated_round_trip_slippage_bps
                + entry_exit_spread_bps
            )
            for hours in _FUNDING_WINDOWS:
                funding_edge = projected_funding * hours * 10_000.0
                convergence_edge = (basis_speed.clip(lower=0.0) * hours).clip(
                    upper=basis.clip(lower=0.0)
                )
                opportunity_bps = (
                    math.expm1(math.log1p(self.benchmark_annual_rate) * hours / _HOURS_PER_YEAR)
                    * 10_000.0
                )
                frame[f"edge_net_{hours}h_bps"] = (
                    funding_edge + convergence_edge - fixed_cost_bps - opportunity_bps
                )
            result[asset] = frame
        return result

    def generate(self, panel: MarketPanel) -> StrategyOutput:
        feature_sets = self.features(panel)
        weights = empty_weights(panel)
        order_types = pd.DataFrame("maker", index=weights.index, columns=weights.columns)
        rebalance = rebalance_mask(panel.prices.index, self.rebalance_hours)
        current = pd.Series(0.0, index=panel.prices.columns)
        pair_leg_weight = self.capital_fraction / (1.0 + self.perp_margin_fraction)
        gate_names = (
            "funding",
            "positive_share",
            "basis",
            "depth",
            "volume",
            "open_interest",
            "volatility",
            "edge",
        )
        candidate_observations = 0
        complete_candidate_observations = 0
        gate_failure_counts = dict.fromkeys(gate_names, 0)
        gate_survivor_counts = dict.fromkeys(gate_names, 0)

        for timestamp in panel.prices.index:
            if bool(rebalance.loc[timestamp]):
                candidates: list[tuple[float, str, str]] = []
                for asset, frame in feature_sets.items():
                    candidate_observations += 1
                    spot = f"HL:{asset}:spot"
                    perp = f"HL:{asset}:perp"
                    row = frame.loc[timestamp]
                    required = [
                        "funding_72h",
                        "positive_funding_share_72h",
                        "basis_bps",
                        "spot_depth_usd",
                        "perp_depth_usd",
                        "spot_volume_usd",
                        "perp_volume_usd",
                        "annualized_volatility",
                        "open_interest_usd",
                        "edge_net_8h_bps",
                        "edge_net_24h_bps",
                        "edge_net_72h_bps",
                    ]
                    if row[required].isna().any():
                        continue
                    complete_candidate_observations += 1
                    edges = [float(row[f"edge_net_{hours}h_bps"]) for hours in _FUNDING_WINDOWS]
                    gate_checks = {
                        "funding": (
                            float(row["funding_72h"]) / 72.0
                            >= self.min_mean_funding_hourly
                        ),
                        "positive_share": (
                            float(row["positive_funding_share_72h"])
                            >= self.min_positive_share
                        ),
                        "basis": abs(float(row["basis_bps"])) <= self.max_abs_basis_bps,
                        "depth": (
                            min(
                                float(row["spot_depth_usd"]),
                                float(row["perp_depth_usd"]),
                            )
                            >= self.min_depth_usd
                        ),
                        "volume": (
                            min(
                                float(row["spot_volume_usd"]),
                                float(row["perp_volume_usd"]),
                            )
                            >= self.min_volume_usd
                        ),
                        "open_interest": (
                            float(row["open_interest_usd"])
                            >= self.min_open_interest_usd
                        ),
                        "volatility": (
                            float(row["annualized_volatility"])
                            <= self.max_annualized_volatility
                        ),
                        "edge": min(edges) >= self.minimum_net_edge_bps,
                    }
                    alive = True
                    for gate_name in gate_names:
                        passed = gate_checks[gate_name]
                        if not passed:
                            gate_failure_counts[gate_name] += 1
                        if alive and passed:
                            gate_survivor_counts[gate_name] += 1
                        else:
                            alive = False
                    if not all(gate_checks.values()):
                        continue
                    candidates.append((min(edges), spot, perp))

                candidates.sort(reverse=True)
                selected = candidates[: self.max_positions]
                current = pd.Series(0.0, index=panel.prices.columns)
                if selected:
                    weight = pair_leg_weight / len(selected)
                    for _score, spot, perp in selected:
                        current[spot] = weight
                        current[perp] = -weight
            weights.loc[timestamp] = current

        target_gross = weights.abs().sum(axis=1)
        target_active = target_gross.gt(1e-12)
        target_entry_signals = int(
            (target_active & ~target_active.shift(1, fill_value=False)).sum()
        )
        target_exit_signals = int(
            (~target_active & target_active.shift(1, fill_value=False)).sum()
        )

        hedge_groups: dict[str, tuple[str, ...]] = {
            f"carry:{asset}": (f"HL:{asset}:spot", f"HL:{asset}:perp")
            for asset in feature_sets
        }
        return StrategyOutput(
            name=self.name,
            risk_tier=self.risk_tier,
            weights=weights,
            diagnostics={
                "logic": "long spot + short perp; maker attempt then simulated IOC hedge",
                "funding_windows_hours": list(_FUNDING_WINDOWS),
                "lookback_hours": self.lookback_hours,
                "max_positions": self.max_positions,
                "capital_fraction": self.capital_fraction,
                "perp_margin_fraction": self.perp_margin_fraction,
                "capital_basis": "spot_notional_plus_conservative_perp_margin",
                "edge_horizons_hours": list(_FUNDING_WINDOWS),
                "fees_in_signal_bps": self.round_trip_fees_bps,
                "slippage_in_signal_bps": self.estimated_round_trip_slippage_bps,
                "candidate_observations": candidate_observations,
                "complete_candidate_observations": complete_candidate_observations,
                "gate_failure_counts": gate_failure_counts,
                "gate_survivor_counts": gate_survivor_counts,
                "eligible_candidate_observations": gate_survivor_counts["edge"],
                "target_entry_signals": target_entry_signals,
                "target_exit_signals": target_exit_signals,
                "target_active_bars": int(target_active.sum()),
            },
            order_types=order_types,
            hedge_groups=hedge_groups,
        )


__all__ = ["CashAndCarryStrategy"]
