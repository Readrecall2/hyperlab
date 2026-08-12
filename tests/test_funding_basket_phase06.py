from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from hyperlab.backtest.engine import PanelBacktester
from hyperlab.backtest.execution import ExecutionConfig, MakerFillModel
from hyperlab.backtest.funding_basket import (
    audit_funding_basket_panel,
    funding_basket_stress_scenarios,
    run_funding_basket_validation,
    write_funding_basket_report,
)
from hyperlab.backtest.stress import run_stress_matrix
from hyperlab.cli import _run_panel_strategies
from hyperlab.models import CostModel, MarketPanel, RiskLimits
from hyperlab.strategies.funding_basket import FundingBasketStrategy, shrunk_covariance

ASSETS = ("BTC", "ETH", "A", "B", "C", "D", "SQUEEZE", "DELISTED")


def _panel(hours: int = 420) -> MarketPanel:
    rng = np.random.default_rng(606)
    index = pd.date_range("2025-01-01", periods=hours, freq="1h", tz="UTC")
    btc = rng.normal(0.0, 0.006, hours)
    eth_factor = 0.45 * btc + rng.normal(0.0, 0.005, hours)
    beta_pairs = (
        (1.0, 0.0),
        (0.0, 1.0),
        (0.8, 0.2),
        (0.2, 0.9),
        (-0.2, 0.7),
        (0.6, -0.1),
        (0.4, 0.5),
        (0.3, 0.3),
    )
    funding_bias = (-18, -13, -8, -3, 4, 9, 22, 15)
    prices: dict[str, np.ndarray] = {}
    funding: dict[str, np.ndarray] = {}
    volume: dict[str, np.ndarray] = {}
    depth: dict[str, np.ndarray] = {}
    for position, (asset, betas, bias) in enumerate(zip(ASSETS, beta_pairs, funding_bias, strict=True)):
        beta_btc, beta_eth = betas
        returns = beta_btc * btc + beta_eth * eth_factor + rng.normal(
            0.0, 0.0025 + position * 0.0001, hours
        )
        if asset == "SQUEEZE":
            returns[-36:] += 0.012
        column = f"HL:{asset}:perp"
        prices[column] = (100.0 + 10.0 * position) * np.exp(np.cumsum(returns))
        persistent = bias * 1e-6 + rng.normal(0.0, 0.5e-6, hours)
        funding[column] = persistent
        volume[column] = np.full(hours, 80_000_000.0 - position * 2_000_000.0)
        depth[column] = np.full(hours, 2_000_000.0 - position * 50_000.0)

    columns = list(prices)
    tradable = pd.DataFrame(True, index=index, columns=columns)
    tradable.loc[index[-24]:, "HL:DELISTED:perp"] = False
    volume["HL:DELISTED:perp"] = np.full(hours, 100_000.0)
    price_frame = pd.DataFrame(prices, index=index)
    return MarketPanel(
        prices=price_frame,
        funding=pd.DataFrame(funding, index=index),
        spreads_bps=pd.DataFrame(1.0, index=index, columns=columns),
        volume_usd=pd.DataFrame(volume, index=index),
        depth_usd=pd.DataFrame(depth, index=index),
        available_at=pd.DataFrame({column: index for column in columns}, index=index),
        finality=pd.DataFrame(True, index=index, columns=columns),
        tradable=tradable,
        metadata={
            "source": "versioned-real-fixture",
            "point_in_time": True,
            "funding_semantics": "realized_hourly",
            "calibration_status": "CALIBRATED",
            "calibration_evidence_hash": "a" * 64,
            "calibration_source": "audited-public-market-observations",
            "historical_universe_source": "point-in-time-listing-fixture",
            "lifecycle_hash": "b" * 64,
            "delisted_assets": ["DELISTED"],
        },
    )


def _strategy(**overrides: object) -> FundingBasketStrategy:
    parameters: dict[str, object] = {
        "lookback_hours": 24,
        "volatility_lookback_hours": 96,
        "beta_lookback_hours": 120,
        "liquidity_lookback_hours": 12,
        "min_market_age_hours": 96,
        "min_volume_usd": 10_000_000.0,
        "min_depth_usd": 500_000.0,
        "rebalance_hours": 6,
        "max_asset_weight": 0.30,
        "target_gross_leverage": 1.0,
        "squeeze_guard_return": 0.10,
    }
    parameters.update(overrides)
    return FundingBasketStrategy(**parameters)  # type: ignore[arg-type]


def _engine() -> PanelBacktester:
    return PanelBacktester(
        costs=CostModel(spot_fee_bps=0.0, perp_fee_bps=0.0, base_slippage_bps=0.0),
        risk_limits=RiskLimits(1.0, 0.02, 0.30),
        execution=ExecutionConfig(
            initial_capital=100_000.0,
            maker_fill=MakerFillModel(base_probability=1.0),
            require_depth=True,
            require_point_in_time=True,
        ),
    )


def test_phase06_features_are_causal_and_apply_age_and_liquidity_filters() -> None:
    panel = _panel()
    strategy = _strategy()
    features = strategy.features(panel)
    timestamp = panel.prices.index[240]

    assert features.market_age_hours.at[timestamp, "HL:A:perp"] >= 96
    assert not bool(features.eligible.at[timestamp, "HL:DELISTED:perp"])
    assert features.funding_score.at[timestamp, "HL:BTC:perp"] < features.funding_score.at[
        timestamp, "HL:D:perp"
    ]

    prefix = MarketPanel(
        prices=panel.prices.loc[:timestamp],
        funding=panel.funding.loc[:timestamp],
        spreads_bps=panel.spreads_bps.loc[:timestamp],
        volume_usd=panel.volume_usd.loc[:timestamp],
        metadata=panel.metadata,
        depth_usd=panel.depth_usd.loc[:timestamp] if panel.depth_usd is not None else None,
        available_at=panel.available_at.loc[:timestamp] if panel.available_at is not None else None,
        finality=panel.finality.loc[:timestamp] if panel.finality is not None else None,
        tradable=panel.tradable.loc[:timestamp] if panel.tradable is not None else None,
    )
    prefix_features = strategy.features(prefix)
    pd.testing.assert_series_equal(features.funding_score.loc[timestamp], prefix_features.funding_score.loc[timestamp])
    pd.testing.assert_series_equal(features.eligible.loc[timestamp], prefix_features.eligible.loc[timestamp])


def test_shrunk_covariance_is_symmetric_positive_semidefinite() -> None:
    returns = _panel().prices.pct_change(fill_method=None).dropna()
    covariance = shrunk_covariance(returns, shrinkage=0.35)

    np.testing.assert_allclose(covariance, covariance.T, atol=1e-14)
    assert float(np.linalg.eigvalsh(covariance).min()) >= -1e-12


def test_phase06_audit_does_not_mistake_a_halt_for_a_declared_delisting() -> None:
    panel = _panel()
    panel.metadata.pop("delisted_assets")

    audit = audit_funding_basket_panel(panel, minimum_history_hours=24 * 14, minimum_assets=6)

    assert not audit.passed
    assert not audit.checks["delisted_markets_included"]


def test_constrained_optimizer_is_dollar_and_btc_eth_beta_neutral_with_asset_caps() -> None:
    panel = _panel()
    strategy = _strategy(mode="optimized")
    output = strategy.generate(panel)
    features = strategy.features(panel)
    active = output.weights.abs().sum(axis=1).gt(1e-8)

    assert active.any()
    assert output.weights.loc[active].sum(axis=1).abs().max() < 1e-8
    assert output.weights.abs().max(axis=1).max() <= 0.30 + 1e-12
    decisions = output.weights.diff().abs().sum(axis=1).gt(1e-10) & active
    for timestamp in output.weights.index[decisions]:
        weights = output.weights.loc[timestamp]
        assert abs(float(weights @ features.beta_btc.loc[timestamp])) < 1e-6
        assert abs(float(weights @ features.beta_eth.loc[timestamp])) < 1e-6


def test_ranking_baseline_and_constrained_optimizer_are_both_recorded() -> None:
    panel = _panel()
    optimized = _strategy(mode="optimized").generate(panel)
    ranking = _strategy(mode="ranking").generate(panel)

    assert optimized.diagnostics["method"] == "constrained_shrunk_covariance"
    assert ranking.diagnostics["method"] == "simple_inverse_vol_ranking"
    assert not optimized.weights.equals(ranking.weights)
    assert optimized.diagnostics["turnover_penalty"] > 0.0


def test_turnover_penalty_reduces_rebalancing_and_squeeze_asset_is_never_short() -> None:
    panel = _panel()
    no_penalty = _strategy(turnover_penalty=0.0).generate(panel)
    penalized = _strategy(turnover_penalty=50.0).generate(panel)

    no_penalty_turnover = float(no_penalty.weights.diff().abs().sum(axis=1).sum())
    penalized_turnover = float(penalized.weights.diff().abs().sum(axis=1).sum())
    assert penalized_turnover <= no_penalty_turnover + 1e-12
    guarded = _strategy().features(panel).short_allowed["HL:SQUEEZE:perp"].eq(False)
    assert penalized.weights.loc[guarded, "HL:SQUEEZE:perp"].min() >= -1e-12


def test_phase06_stresses_change_market_path_and_attribute_funding_vs_relative_pnl() -> None:
    panel = _panel()
    strategy = _strategy()
    output = strategy.generate(panel)
    scenarios = funding_basket_stress_scenarios()
    results = run_stress_matrix(
        panel=panel,
        output=output,
        costs=_engine().costs,
        risk_limits=_engine().risk_limits,
        base_execution=_engine().execution,
        scenarios=scenarios,
    )

    assert {"base", "broken_correlation", "simultaneous_short_squeeze"}.issubset(results)
    base = results["base"]
    assert abs(base.metrics.funding_contribution) > 0.0
    assert abs(base.metrics.price_contribution) > 0.0
    assert not results["broken_correlation"].returns["price_return"].equals(
        base.returns["price_return"]
    )
    assert results["broken_correlation"].diagnostics["data_status"] == "SYNTHETIC"
    assert results["simultaneous_short_squeeze"].diagnostics["data_status"] == "SYNTHETIC"
    assert (
        results["simultaneous_short_squeeze"].metrics.price_contribution
        < base.metrics.price_contribution
    )


def test_leave_one_out_and_delisted_market_are_in_reproducible_report(tmp_path: Path) -> None:
    panel = _panel()
    audit = audit_funding_basket_panel(panel, minimum_history_hours=24 * 14, minimum_assets=6)
    validation = run_funding_basket_validation(panel, strategy=_strategy(), engine=_engine(), audit=audit)
    report = write_funding_basket_report(validation, output_dir=tmp_path)

    assert audit.passed
    assert audit.delisted_assets == ("DELISTED",)
    assert set(validation.leave_one_out) == set(ASSETS)
    payload = json.loads((tmp_path / "funding_basket_summary.json").read_text(encoding="utf-8"))
    assert payload["comparison"]["ranking"]["method"] == "simple_inverse_vol_ranking"
    assert payload["comparison"]["optimized"]["method"] == "constrained_shrunk_covariance"
    assert set(payload["leave_one_out"]) == set(ASSETS)
    assert payload["data_audit"]["delisted_assets"] == ["DELISTED"]
    html = report.read_text(encoding="utf-8")
    for label in (
        "Funding",
        "Performance relative",
        "Corrélation cassée",
        "Squeeze simultané",
        "Exclusion un actif à la fois",
        "Marchés délistés inclus",
    ):
        assert label in html


def test_phase06_demo_exercises_funding_basket_positions_and_costs() -> None:
    result = _run_panel_strategies(["funding_basket"], hours=1_200, seed=42)[0]

    assert result.diagnostics["method"] == "constrained_shrunk_covariance"
    assert result.diagnostics["target_entry_signals"] > 0
    assert result.diagnostics["position_entries"] > 0
    assert result.diagnostics["orders"] > 0
    assert abs(result.metrics.funding_contribution) > 0.0
    assert result.metrics.cost_contribution < 0.0
