from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from hyperlab.backtest.benchmark import PassiveBenchmarkSpec
from hyperlab.backtest.carry import (
    CarryGateSpec,
    audit_carry_panel,
    evaluate_carry_gate,
    write_carry_report,
)
from hyperlab.backtest.engine import PanelBacktester
from hyperlab.backtest.execution import ExecutionConfig, MakerFillModel
from hyperlab.backtest.stress import StressScenario, run_stress_matrix
from hyperlab.data.io import load_panel_csv, save_panel_csv
from hyperlab.models import CostModel, MarketPanel, RiskLimits, StrategyOutput
from hyperlab.strategies.carry import CashAndCarryStrategy

SPOT = "HL:BTC:spot"
PERP = "HL:BTC:perp"


def _carry_panel(hours: int = 100, *, calibrated: bool = True) -> MarketPanel:
    index = pd.date_range("2026-01-01", periods=hours, freq="1h", tz="UTC")
    spot = 100.0 * np.exp(np.linspace(0.0, 0.01, hours))
    basis = np.linspace(0.006, 0.001, hours)
    prices = pd.DataFrame({SPOT: spot, PERP: spot * (1.0 + basis)}, index=index)
    funding = pd.DataFrame({SPOT: 0.0, PERP: 0.00002}, index=index)
    spreads = pd.DataFrame({SPOT: 2.0, PERP: 1.0}, index=index)
    volume = pd.DataFrame({SPOT: 20_000_000.0, PERP: 100_000_000.0}, index=index)
    depth = pd.DataFrame({SPOT: 2_000_000.0, PERP: 5_000_000.0}, index=index)
    oi = pd.DataFrame({SPOT: np.nan, PERP: 1_000_000_000.0}, index=index)
    status = "CALIBRATED" if calibrated else "UNCALIBRATED"
    return MarketPanel(
        prices=prices,
        funding=funding,
        spreads_bps=spreads,
        volume_usd=volume,
        depth_usd=depth,
        open_interest_usd=oi,
        available_at=pd.DataFrame({column: index for column in prices}, index=index),
        finality=pd.DataFrame(True, index=index, columns=prices.columns),
        tradable=pd.DataFrame(True, index=index, columns=prices.columns),
        metadata={
            "source": "versioned-real-fixture",
            "point_in_time": True,
            "calibration_status": status,
            "calibration_evidence_hash": "a" * 64 if calibrated else None,
            "calibration_source": "audited-public-market-observations" if calibrated else "pending",
            "historical_universe_source": "fixture-lifecycle",
            "lifecycle_hash": "b" * 64,
            "funding_semantics": "realized_hourly",
        },
    )


def _output(panel: MarketPanel, *, close_last: bool = False) -> StrategyOutput:
    weights = pd.DataFrame(0.0, index=panel.prices.index, columns=panel.prices.columns)
    weights.loc[:, SPOT] = 0.25
    weights.loc[:, PERP] = -0.25
    if close_last:
        weights.iloc[-1] = 0.0
    order_types = pd.DataFrame("maker", index=weights.index, columns=weights.columns)
    return StrategyOutput(
        "cash_and_carry",
        "1 — défensif",
        weights,
        order_types=order_types,
        hedge_groups={"carry:BTC": (SPOT, PERP)},
    )


def _engine() -> PanelBacktester:
    return PanelBacktester(
        costs=CostModel(spot_fee_bps=0.0, perp_fee_bps=0.0, base_slippage_bps=0.0),
        risk_limits=RiskLimits(1.0, 0.05, 0.5),
        execution=ExecutionConfig(
            initial_capital=100_000.0,
            maker_fill=MakerFillModel(base_probability=1.0),
            require_depth=True,
            require_point_in_time=True,
        ),
        benchmark=PassiveBenchmarkSpec(annual_rate=0.045),
    )


def test_phase05_features_are_causal_and_drive_maker_hedged_targets() -> None:
    panel = _carry_panel()
    strategy = CashAndCarryStrategy(
        min_mean_funding_hourly=0.000001,
        min_positive_share=0.70,
        max_abs_basis_bps=100.0,
        min_depth_usd=1_000_000.0,
        min_open_interest_usd=100_000_000.0,
        max_annualized_volatility=1.0,
        capital_fraction=0.50,
        perp_margin_fraction=1.0,
        round_trip_fees_bps=0.0,
        estimated_round_trip_slippage_bps=0.0,
    )

    features = strategy.features(panel)["BTC"]
    assert {
        "funding_8h",
        "funding_24h",
        "funding_72h",
        "positive_funding_share_72h",
        "funding_trend_hourly",
        "basis_bps",
        "basis_convergence_bps_per_hour",
        "spot_depth_usd",
        "perp_depth_usd",
        "annualized_volatility",
        "open_interest_usd",
        "edge_net_8h_bps",
        "edge_net_24h_bps",
        "edge_net_72h_bps",
    }.issubset(features.columns)
    decision_time = panel.prices.index[80]
    prefix = MarketPanel(
        prices=panel.prices.loc[:decision_time],
        funding=panel.funding.loc[:decision_time],
        spreads_bps=panel.spreads_bps.loc[:decision_time],
        volume_usd=panel.volume_usd.loc[:decision_time],
        metadata=panel.metadata,
        depth_usd=panel.depth_usd.loc[:decision_time] if panel.depth_usd is not None else None,
        open_interest_usd=(
            panel.open_interest_usd.loc[:decision_time]
            if panel.open_interest_usd is not None
            else None
        ),
        available_at=panel.available_at.loc[:decision_time] if panel.available_at is not None else None,
        finality=panel.finality.loc[:decision_time] if panel.finality is not None else None,
        tradable=panel.tradable.loc[:decision_time] if panel.tradable is not None else None,
    )
    pd.testing.assert_series_equal(
        features.loc[decision_time],
        strategy.features(prefix)["BTC"].loc[decision_time],
    )

    output = strategy.generate(panel)
    assert output.weights.loc[decision_time, SPOT] == 0.25
    assert output.weights.loc[decision_time, PERP] == -0.25
    assert output.order_types is not None
    assert output.order_types.eq("maker").all(axis=None)
    assert output.hedge_groups == {"carry:BTC": (SPOT, PERP)}
    assert output.diagnostics["capital_basis"] == "spot_notional_plus_conservative_perp_margin"


def test_open_interest_round_trips_with_the_point_in_time_panel(tmp_path: Path) -> None:
    panel = _carry_panel()
    save_panel_csv(panel, tmp_path)

    restored = load_panel_csv(tmp_path)

    assert restored.open_interest_usd is not None
    pd.testing.assert_frame_equal(restored.open_interest_usd, panel.open_interest_usd, check_freq=False)


def test_funding_inversion_is_a_real_stress_input_not_a_label() -> None:
    panel = _carry_panel()
    output = _output(panel, close_last=True)

    results = run_stress_matrix(
        panel=panel,
        output=output,
        costs=CostModel(spot_fee_bps=0.0, perp_fee_bps=0.0, base_slippage_bps=0.0),
        risk_limits=RiskLimits(1.0, 0.05, 0.5),
        base_execution=_engine().execution,
        scenarios=(
            StressScenario("base"),
            StressScenario("funding_inversion", funding_multiplier=-1.0),
        ),
        benchmark=PassiveBenchmarkSpec(annual_rate=0.045),
    )

    assert results["base"].metrics.funding_contribution > 0.0
    assert results["funding_inversion"].metrics.funding_contribution < 0.0
    assert (
        results["funding_inversion"].metrics.total_return
        < results["base"].metrics.total_return
    )


def test_phase05_gate_fails_closed_and_report_contains_required_economics(tmp_path: Path) -> None:
    panel = _carry_panel(hours=100, calibrated=False)
    base = _engine().run(panel, _output(panel, close_last=False))
    inversion = run_stress_matrix(
        panel=panel,
        output=_output(panel, close_last=False),
        costs=_engine().costs,
        risk_limits=_engine().risk_limits,
        base_execution=_engine().execution,
        scenarios=(StressScenario("funding_inversion", funding_multiplier=-1.0),),
        benchmark=PassiveBenchmarkSpec(annual_rate=0.045),
    )["funding_inversion"]
    audit = audit_carry_panel(panel, minimum_history_hours=30 * 24)

    gate = evaluate_carry_gate(
        {"base": base, "funding_inversion": inversion},
        audit=audit,
        spec=CarryGateSpec(minimum_stressed_excess_return=0.0),
    )
    report = write_carry_report(
        {"base": base, "funding_inversion": inversion},
        gate=gate,
        audit=audit,
        output_dir=tmp_path,
        perp_margin_fraction=1.0,
    )

    assert gate.promote is False
    assert gate.status == "BLOCKED_INSUFFICIENT_REAL_DATA"
    assert any("720" in reason for reason in gate.reasons)
    payload = json.loads((tmp_path / "carry_summary.json").read_text(encoding="utf-8"))
    summary = payload["scenarios"]["base"]
    assert payload["gate"]["promote"] is False
    assert {
        "return_on_total_capital",
        "time_invested",
        "funding_received_usd",
        "basis_pnl_usd",
        "fees_usd",
        "hedge_pnl_usd",
        "max_drawdown",
        "capacity_usd",
        "max_capital_immobilized_usd",
        "opportunity_cost_usd",
        "close_complete",
    }.issubset(summary)
    html = report.read_text(encoding="utf-8")
    for label in (
        "Capital total immobilisé",
        "Temps investi",
        "Funding encaissé",
        "Basis",
        "Frais",
        "Hedge",
        "Drawdown max",
        "Capacité",
        "Coût d'opportunité",
    ):
        assert label in html


def test_phase05_gate_rejects_insufficient_stressed_outperformance() -> None:
    panel = _carry_panel(hours=30 * 24, calibrated=True)
    base = _engine().run(panel, _output(panel, close_last=True))
    inversion = run_stress_matrix(
        panel=panel,
        output=_output(panel, close_last=True),
        costs=_engine().costs,
        risk_limits=_engine().risk_limits,
        base_execution=_engine().execution,
        scenarios=(StressScenario("funding_inversion", funding_multiplier=-1.0),),
        benchmark=PassiveBenchmarkSpec(annual_rate=0.045),
    )["funding_inversion"]
    for result in (base, inversion):
        result.diagnostics["audit_status"] = "CALIBRATED"
    inversion.metrics.excess_vs_benchmark = -0.001

    gate = evaluate_carry_gate(
        {"base": base, "funding_inversion": inversion},
        audit=audit_carry_panel(panel),
        spec=CarryGateSpec(
            minimum_stressed_excess_return=0.0,
            maximum_stressed_drawdown=1.0,
        ),
    )

    assert gate.promote is False
    assert gate.status == "REJECTED_STRESSED_BENCHMARK_GATE"
    assert any("Surperformance stressée insuffisante" in reason for reason in gate.reasons)


def test_fill_ledger_exposes_size_dependent_capacity() -> None:
    panel = _carry_panel()
    result = _engine().run(panel, _output(panel, close_last=True))

    assert "capacity_usd" in result.fills
    assert result.fills.loc[result.fills["filled_weight"].ne(0.0), "capacity_usd"].gt(0.0).all()
