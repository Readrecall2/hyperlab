from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from hyperlab.backtest.engine import PanelBacktester
from hyperlab.backtest.momentum import (
    MomentumGateConfig,
    MomentumSelectionConfig,
    audit_momentum_panel,
    run_momentum_validation,
    select_momentum_variant,
    write_momentum_report,
)
from hyperlab.models import CostModel, MarketPanel, RiskLimits
from hyperlab.strategies.momentum import (
    RobustMomentumStrategy,
    classify_market_regimes,
)

ASSETS = ("BTC", "ETH", "SOL", "DELISTED")


def _panel(*, hours: int = 24 * 120, calibrated: bool = True) -> MarketPanel:
    index = pd.date_range("2025-01-01", periods=hours, freq="1h", tz="UTC")
    rng = np.random.default_rng(909)
    thirds = np.array_split(np.arange(hours), 3)
    market_returns = np.zeros(hours)
    market_returns[thirds[0]] = 0.0012 + rng.normal(0.0, 0.003, len(thirds[0]))
    market_returns[thirds[1]] = -0.0010 + rng.normal(0.0, 0.004, len(thirds[1]))
    market_returns[thirds[2]] = rng.normal(0.0, 0.014, len(thirds[2]))
    prices: dict[str, np.ndarray] = {}
    for position, asset in enumerate(ASSETS):
        idiosyncratic = rng.normal(0.0, 0.0025 + position * 0.0004, hours)
        prices[f"HL:{asset}:perp"] = 100.0 * np.exp(
            np.cumsum(market_returns * (1.0 - position * 0.08) + idiosyncratic)
        )
    columns = list(prices)
    price_frame = pd.DataFrame(prices, index=index)
    volume = pd.DataFrame(
        {
            column: 20_000_000.0
            * np.exp(rng.normal(0.0, 0.15, hours))
            * (1.0 + np.abs(market_returns) * 20.0)
            for column in columns
        },
        index=index,
    )
    oi = pd.DataFrame(
        {
            column: 50_000_000.0 * np.exp(np.cumsum(rng.normal(0.0, 0.002, hours)))
            for column in columns
        },
        index=index,
    )
    liquidations = pd.DataFrame(1_000.0, index=index, columns=columns)
    liquidations.loc[index[hours // 2], "HL:BTC:perp"] = 5_000_000.0
    tradable = pd.DataFrame(True, index=index, columns=columns)
    tradable.loc[index[-24]:, "HL:DELISTED:perp"] = False
    available_at = pd.DataFrame({column: index for column in columns}, index=index)
    metadata: dict[str, object] = {
        "source": "versioned-real-momentum-fixture" if calibrated else "synthetic-momentum-fixture",
        "point_in_time": True,
        "calibration_status": "CALIBRATED" if calibrated else "SYNTHETIC",
        "calibration_evidence_hash": "a" * 64,
        "calibration_source": "audited fixture",
        "historical_universe_source": "versioned lifecycle fixture",
        "lifecycle_hash": "b" * 64,
        "delisted_assets": ["DELISTED"],
        "funding_semantics": "realized_hourly",
        "liquidation_semantics": "observed_hourly_notional",
        "parameters_frozen_before_period": True,
    }
    return MarketPanel(
        prices=price_frame,
        funding=pd.DataFrame(0.00001, index=index, columns=columns),
        spreads_bps=pd.DataFrame(2.0, index=index, columns=columns),
        volume_usd=volume,
        depth_usd=pd.DataFrame(2_000_000.0, index=index, columns=columns),
        open_interest_usd=oi,
        liquidation_usd=liquidations,
        available_at=available_at,
        finality=pd.DataFrame(True, index=index, columns=columns),
        tradable=tradable,
        metadata=metadata,
    )


def _engine() -> PanelBacktester:
    return PanelBacktester(
        costs=CostModel(0.0, 1.0, 1.0, 0.5),
        risk_limits=RiskLimits(1.0, 1.0, 0.40),
    )


def _selection() -> MomentumSelectionConfig:
    return MomentumSelectionConfig(
        signal_variants=("time_series", "breakout", "combined"),
        horizons=(12, 24, 48),
        breakout_lookback_bars=36,
        volatility_lookback_bars=24,
        regime_lookback_bars=24,
        regime_baseline_bars=72,
        correlation_lookback_bars=36,
        liquidation_lookback_bars=24,
        minimum_train_bars=24 * 20,
        minimum_validation_bars=24 * 10,
    )


def test_phase09_audit_requires_oi_liquidations_and_historical_delistings() -> None:
    panel = _panel(hours=24 * 30)
    passed = audit_momentum_panel(panel, minimum_history_hours=24 * 30, minimum_assets=4)
    without_liquidations = replace(panel, liquidation_usd=None)
    failed = audit_momentum_panel(
        without_liquidations,
        minimum_history_hours=24 * 30,
        minimum_assets=4,
    )

    assert passed.passed
    assert passed.delisted_assets == ("DELISTED",)
    assert not failed.passed
    assert not failed.checks["liquidations_observed"]


def test_features_and_regimes_are_causal_under_future_rewrite() -> None:
    panel = _panel(hours=600)
    strategy = RobustMomentumStrategy(
        horizons=(12, 24, 48),
        breakout_lookback_bars=36,
        volatility_lookback_bars=24,
        regime_lookback_bars=24,
        regime_baseline_bars=72,
        correlation_lookback_bars=36,
        liquidation_lookback_bars=24,
        rebalance_bars=1,
    )
    cutoff = panel.prices.index[430]
    changed_prices = panel.prices.copy()
    changed_prices.loc[changed_prices.index > cutoff, "HL:BTC:perp"] *= 4.0
    changed_liquidations = panel.liquidation_usd.copy()
    changed_liquidations.loc[changed_liquidations.index > cutoff] *= 100.0
    changed = replace(
        panel,
        prices=changed_prices,
        liquidation_usd=changed_liquidations,
    )

    base_features = strategy.features(panel)
    counterfactual_features = strategy.features(changed)
    pd.testing.assert_frame_equal(
        base_features["score"].loc[:cutoff],
        counterfactual_features["score"].loc[:cutoff],
    )
    pd.testing.assert_series_equal(
        classify_market_regimes(panel, strategy.regime_config).loc[:cutoff],
        classify_market_regimes(changed, strategy.regime_config).loc[:cutoff],
    )
    pd.testing.assert_frame_equal(
        strategy.generate(panel).weights.loc[:cutoff],
        strategy.generate(changed).weights.loc[:cutoff],
    )


def test_variant_selection_records_all_variants_and_cannot_see_final_test() -> None:
    panel = _panel()
    train_end = 24 * 72
    validation_end = 24 * 96
    config = _selection()
    base = select_momentum_variant(
        panel,
        train_index=panel.prices.index[:train_end],
        validation_index=panel.prices.index[train_end:validation_end],
        config=config,
        engine=_engine(),
    )
    changed_prices = panel.prices.copy()
    changed_prices.loc[panel.prices.index[validation_end]:] *= np.linspace(
        1.0,
        5.0,
        len(panel.prices) - validation_end,
    )[:, None]
    changed = replace(panel, prices=changed_prices)
    counterfactual = select_momentum_variant(
        changed,
        train_index=panel.prices.index[:train_end],
        validation_index=panel.prices.index[train_end:validation_end],
        config=config,
        engine=_engine(),
    )

    assert base == counterfactual
    assert {item.signal_variant for item in base.variant_results} == set(config.signal_variants)
    assert len(base.variant_results) == len(config.signal_variants)
    assert base.selection_end == panel.prices.index[validation_end - 1]


def test_volatility_stop_correlation_limit_and_liquidation_cooldown_are_bounded() -> None:
    panel = _panel(hours=800)
    strategy = RobustMomentumStrategy(
        signal_variant="combined",
        horizons=(12, 24, 48),
        breakout_lookback_bars=36,
        volatility_lookback_bars=24,
        regime_lookback_bars=24,
        regime_baseline_bars=72,
        correlation_lookback_bars=36,
        maximum_pairwise_correlation=0.60,
        liquidation_lookback_bars=24,
        liquidation_spike_z=4.0,
        liquidation_cooldown_bars=8,
        stop_volatility_multiple=0.75,
        stop_cooldown_bars=4,
        maximum_gross_exposure=1.0,
        maximum_asset_weight=0.40,
        rebalance_bars=1,
        minimum_signal=0.0,
    )
    output = strategy.generate(panel)
    spike = panel.liquidation_usd.sum(axis=1).idxmax()
    spike_position = panel.prices.index.get_loc(spike)

    assert output.weights.abs().sum(axis=1).max() <= 1.0 + 1e-12
    assert output.weights.abs().max(axis=1).max() <= 0.40 + 1e-12
    assert output.weights.iloc[spike_position : spike_position + 9].abs().sum(axis=1).eq(0.0).all()
    assert output.diagnostics["events"]["liquidation_spikes"] >= 1
    assert output.diagnostics["events"]["volatility_stops"] >= 1
    assert output.diagnostics["events"]["correlation_rejections"] >= 1
    assert output.diagnostics["deployable_leverage_cap"] == 1.0

    with pytest.raises(ValueError, match="1x"):
        replace(strategy, maximum_gross_exposure=1.01)


def test_report_separates_regimes_and_checks_bull_market_dependence(tmp_path: Path) -> None:
    panel = _panel(calibrated=False)
    audit = audit_momentum_panel(panel, minimum_history_hours=24 * 90, minimum_assets=4)
    validation = run_momentum_validation(
        panel,
        engine=_engine(),
        selection_config=_selection(),
        gate_config=MomentumGateConfig(
            train_fraction=0.60,
            validation_fraction=0.20,
            minimum_non_bull_pnl=-1_000_000.0,
            maximum_bull_profit_fraction=1.0,
        ),
        audit=audit,
    )
    report = write_momentum_report(validation, output_dir=tmp_path)

    assert validation.status == "BLOCKED_UNCALIBRATED_OR_SURVIVORSHIP_BIAS"
    assert set(validation.regime_performance).issuperset({"trend_up", "trend_down", "chaos"})
    assert "not_only_bull_market" in validation.gate_checks
    assert validation.result.metrics.max_gross_leverage <= 1.0 + 1e-12
    assert (tmp_path / "momentum_regime_summary.json").exists()
    html = report.read_text(encoding="utf-8")
    for label in (
        "Performance par regime",
        "Dependance au bull market",
        "time_series",
        "breakout",
        "combined",
        "Liquidation",
        "Funding",
    ):
        assert label in html
