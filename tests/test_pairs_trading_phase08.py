from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

from hyperlab.backtest.engine import PanelBacktester
from hyperlab.backtest.pairs import (
    PairSelectionConfig,
    PairsGateConfig,
    audit_pairs_panel,
    run_pairs_validation,
    select_pairs_train_only,
    write_pairs_report,
)
from hyperlab.models import CostModel, MarketPanel, RiskLimits
from hyperlab.strategies.pairs import PairModel, RobustPairsStrategy

ASSETS = ("A", "B", "C", "D", "DELISTED")


def _panel(*, hours: int = 24 * 120, calibrated: bool = True) -> MarketPanel:
    index = pd.date_range("2025-01-01", periods=hours, freq="1h", tz="UTC")
    rng = np.random.default_rng(808)
    common_ab = np.cumsum(rng.normal(0.0, 0.004, hours))
    common_cd = np.cumsum(rng.normal(0.0, 0.005, hours))
    residual_ab = np.zeros(hours)
    residual_cd = np.zeros(hours)
    for row in range(1, hours):
        residual_ab[row] = 0.78 * residual_ab[row - 1] + rng.normal(0.0, 0.006)
        residual_cd[row] = 0.72 * residual_cd[row - 1] + rng.normal(0.0, 0.007)
    logs = {
        "A": 4.6 + 1.15 * common_ab + residual_ab,
        "B": 4.2 + common_ab,
        "C": 4.4 + 0.85 * common_cd + residual_cd,
        "D": 4.0 + common_cd,
        "DELISTED": 3.8 + common_cd + 0.5 * residual_cd,
    }
    columns = [f"HL:{asset}:perp" for asset in ASSETS]
    prices = pd.DataFrame(
        {f"HL:{asset}:perp": np.exp(logs[asset]) for asset in ASSETS},
        index=index,
    )
    funding = pd.DataFrame(0.00001, index=index, columns=columns)
    spreads = pd.DataFrame(2.0, index=index, columns=columns)
    volume = pd.DataFrame(25_000_000.0, index=index, columns=columns)
    depth = pd.DataFrame(2_000_000.0, index=index, columns=columns)
    tradable = pd.DataFrame(True, index=index, columns=columns)
    tradable.loc[index[-48]:, "HL:DELISTED:perp"] = False
    available_at = pd.DataFrame(
        {column: index for column in columns}, index=index, columns=columns
    )
    finality = pd.DataFrame(True, index=index, columns=columns)
    metadata: dict[str, object] = {
        "source": "versioned-real-pairs-fixture" if calibrated else "synthetic-pairs-fixture",
        "point_in_time": True,
        "calibration_status": "CALIBRATED" if calibrated else "SYNTHETIC",
        "calibration_evidence_hash": "a" * 64,
        "calibration_source": "audited fixture",
        "historical_universe_source": "versioned lifecycle fixture",
        "lifecycle_hash": "b" * 64,
        "delisted_assets": ["DELISTED"],
        "funding_semantics": "realized_hourly",
        "parameters_frozen_before_period": True,
    }
    return MarketPanel(
        prices=prices,
        funding=funding,
        spreads_bps=spreads,
        volume_usd=volume,
        depth_usd=depth,
        available_at=available_at,
        finality=finality,
        tradable=tradable,
        metadata=metadata,
    )


def _engine() -> PanelBacktester:
    return PanelBacktester(
        costs=CostModel(0.0, 1.0, 1.0, 0.5),
        risk_limits=RiskLimits(1.0, 0.20, 0.50),
    )


def _selection_config() -> PairSelectionConfig:
    return PairSelectionConfig(
        maximum_pairs=2,
        minimum_train_bars=24 * 30,
        minimum_validation_bars=24 * 10,
        minimum_overlap_fraction=0.95,
        minimum_return_correlation=0.40,
        maximum_half_life_bars=96.0,
        maximum_beta_instability=0.80,
        lookback_bars=72,
        model_methods=("rolling", "kalman", "cointegration"),
    )


def test_phase08_audit_requires_historical_lifecycle_and_delisted_asset() -> None:
    panel = _panel(hours=24 * 30)
    passed = audit_pairs_panel(panel, minimum_history_hours=24 * 30, minimum_assets=4)
    without_delisted = replace(panel, metadata={**panel.metadata, "delisted_assets": []})
    failed = audit_pairs_panel(without_delisted, minimum_history_hours=24 * 30, minimum_assets=4)

    assert passed.passed
    assert passed.delisted_assets == ("DELISTED",)
    assert not failed.passed
    assert not failed.checks["delisted_markets_included"]


def test_pair_and_model_selection_cannot_see_final_test() -> None:
    panel = _panel()
    train_end = 24 * 72
    validation_end = 24 * 96
    train = panel.prices.index[:train_end]
    validation = panel.prices.index[train_end:validation_end]
    base = select_pairs_train_only(
        panel,
        train_index=train,
        validation_index=validation,
        config=_selection_config(),
        engine=_engine(),
    )
    changed_prices = panel.prices.copy()
    changed_prices.loc[panel.prices.index[validation_end]:, "HL:A:perp"] *= np.linspace(
        1.0, 5.0, len(panel.prices) - validation_end
    )
    changed = replace(panel, prices=changed_prices)
    counterfactual = select_pairs_train_only(
        changed,
        train_index=train,
        validation_index=validation,
        config=_selection_config(),
        engine=_engine(),
    )

    assert base == counterfactual
    assert base.selected_pairs
    assert {item.method for item in base.selected_pairs}.issubset(
        {"rolling", "kalman", "cointegration"}
    )
    assert base.selection_end == validation[-1]

    future_tradable = panel.tradable.copy()
    future_tradable.loc[train, "HL:A:perp"] = False
    future_listing = replace(panel, tradable=future_tradable)
    without_future_asset = select_pairs_train_only(
        future_listing,
        train_index=train,
        validation_index=validation,
        config=_selection_config(),
        engine=_engine(),
    )
    assert all(
        "HL:A:perp" not in (candidate.asset_a, candidate.asset_b)
        for candidate in without_future_asset.train_ranked_candidates
    )


def test_zscore_and_hedge_estimates_are_causal_under_future_rewrite() -> None:
    panel = _panel(hours=600)
    model = PairModel(
        asset_a="HL:A:perp",
        asset_b="HL:B:perp",
        method="rolling",
        hedge_ratio=1.0,
        intercept=0.0,
        lookback_bars=72,
        train_score=1.0,
        validation_score=1.0,
    )
    strategy = RobustPairsStrategy(models=(model,), volatility_lookback_bars=48)
    cutoff = panel.prices.index[430]
    changed_prices = panel.prices.copy()
    changed_prices.loc[changed_prices.index > cutoff, "HL:A:perp"] *= 3.0
    changed = replace(panel, prices=changed_prices)

    base = strategy.generate(panel)
    counterfactual = strategy.generate(changed)
    base_features = strategy.features(panel)
    counterfactual_features = strategy.features(changed)

    pd.testing.assert_frame_equal(base.weights.loc[:cutoff], counterfactual.weights.loc[:cutoff])
    pd.testing.assert_frame_equal(
        base_features[model.pair_id].loc[:cutoff],
        counterfactual_features[model.pair_id].loc[:cutoff],
    )


def test_stop_time_stop_cooldown_and_volatility_sizing_are_bounded() -> None:
    panel = _panel(hours=500)
    model = PairModel("HL:A:perp", "HL:B:perp", "cointegration", 1.15, 0.4, 72, 1.0, 1.0)
    strategy = RobustPairsStrategy(
        models=(model,),
        enter_z=1.0,
        exit_z=0.05,
        stop_z=1.6,
        max_holding_bars=8,
        cooldown_bars=6,
        volatility_lookback_bars=24,
        target_spread_volatility=0.01,
        maximum_pair_gross=0.50,
    )
    output = strategy.generate(panel)
    events = output.diagnostics["events"]

    assert output.weights.abs().sum(axis=1).max() <= 0.50 + 1e-12
    assert events["entries"] > 0
    assert events["time_stops"] + events["spread_stops"] > 0
    assert events["cooldown_blocked_entries"] > 0
    assert output.diagnostics["loss_scaling"] is False
    assert output.diagnostics["unbounded_averaging_down"] is False


def test_funding_turnover_best_pair_removal_and_correlation_break_are_reported(
    tmp_path: Path,
) -> None:
    panel = _panel(calibrated=False)
    audit = audit_pairs_panel(panel, minimum_history_hours=24 * 90, minimum_assets=4)
    validation = run_pairs_validation(
        panel,
        engine=_engine(),
        selection_config=_selection_config(),
        gate_config=PairsGateConfig(
            train_fraction=0.60,
            validation_fraction=0.20,
            minimum_stressed_return=-1.0,
            correlation_break_strength=2.0,
        ),
        audit=audit,
    )
    report = write_pairs_report(validation, output_dir=tmp_path)

    assert validation.selection.selected_pairs
    assert set(validation.scenarios) == {
        "base_final_test",
        "remove_best_pair",
        "correlation_break",
    }
    base = validation.scenarios["base_final_test"]
    assert base.metrics.turnover > 0.0
    assert abs(base.metrics.funding_contribution) > 0.0
    assert base.metrics.cost_contribution < 0.0
    assert validation.scenarios["correlation_break"].diagnostics["data_status"] == "SYNTHETIC"
    assert validation.gate_checks["remove_best_pair_survives"]
    assert validation.gate_checks["correlation_break_survives"]
    assert validation.status == "BLOCKED_UNCALIBRATED_OR_SURVIVORSHIP_BIAS"
    assert (tmp_path / "pairs_trading_summary.json").exists()
    html = report.read_text(encoding="utf-8")
    for label in (
        "Retrait de la meilleure paire",
        "Rupture de corrélation simulée",
        "Funding",
        "Turnover",
        "Sélection train uniquement",
    ):
        assert label in html
