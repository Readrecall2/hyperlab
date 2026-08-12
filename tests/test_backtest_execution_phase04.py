from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from hyperlab.backtest.costs import CostRule, CostSchedule, SlippageModel
from hyperlab.backtest.engine import PanelBacktester
from hyperlab.backtest.execution import ExecutionConfig, MakerFillModel
from hyperlab.data.io import load_panel_csv, save_panel_csv
from hyperlab.models import MarketPanel, RiskLimits, StrategyOutput

INSTRUMENT = "HL:BTC:perp"


def _panel(
    prices: list[float],
    *,
    instruments: tuple[str, ...] = (INSTRUMENT,),
    depth_usd: float = 1_000_000.0,
    spread_bps: float = 0.0,
) -> MarketPanel:
    index = pd.date_range("2026-01-01", periods=len(prices), freq="1h", tz="UTC")
    price_frame = pd.DataFrame(
        {instrument: prices for instrument in instruments},
        index=index,
    )
    zero = pd.DataFrame(0.0, index=index, columns=price_frame.columns)
    return MarketPanel(
        prices=price_frame,
        funding=zero.copy(),
        spreads_bps=pd.DataFrame(spread_bps, index=index, columns=price_frame.columns),
        volume_usd=pd.DataFrame(10_000_000.0, index=index, columns=price_frame.columns),
        depth_usd=pd.DataFrame(depth_usd, index=index, columns=price_frame.columns),
        available_at=pd.DataFrame(
            {column: index for column in price_frame.columns},
            index=index,
        ),
        finality=pd.DataFrame(True, index=index, columns=price_frame.columns),
        tradable=pd.DataFrame(True, index=index, columns=price_frame.columns),
        metadata={
            "point_in_time": True,
            "calibration_status": "CALIBRATED",
            "calibration_evidence_hash": "b" * 64,
            "calibration_source": "versioned-fixture-observations",
            "historical_universe_source": "fixture-lifecycle",
            "lifecycle_hash": "a" * 64,
        },
    )


def _schedule(
    *instruments: str,
    maker_fee_bps: float = 0.0,
    taker_fee_bps: float = 0.0,
    base_slippage_bps: float = 0.0,
    impact_bps: float = 0.0,
    max_participation: float = 1.0,
) -> CostSchedule:
    return CostSchedule(
        rules=tuple(
            CostRule(
                instrument=instrument,
                maker_fee_bps=maker_fee_bps,
                taker_fee_bps=taker_fee_bps,
                slippage=SlippageModel(
                    base_bps=base_slippage_bps,
                    impact_coefficient_bps=impact_bps,
                    exponent=1.0,
                    max_participation=max_participation,
                ),
                source="versioned-fixture-costs",
            )
            for instrument in instruments
        ),
        calibration_status="CALIBRATED",
        calibration_evidence_hash="b" * 64,
    )


def _engine(
    costs: CostSchedule,
    execution: ExecutionConfig | None = None,
) -> PanelBacktester:
    return PanelBacktester(
        costs=costs,
        risk_limits=RiskLimits(2.0, 2.0, 2.0),
        execution=execution or ExecutionConfig(require_point_in_time=True),
    )


def test_signal_alignment_is_strict_and_next_bar_is_the_first_earned_return() -> None:
    panel = _panel([100.0, 200.0, 220.0])
    weights = pd.DataFrame({INSTRUMENT: [0.0, 1.0, 0.0]}, index=panel.prices.index)
    result = _engine(_schedule(INSTRUMENT)).run(
        panel,
        StrategyOutput("causal", "test", weights),
    )

    assert result.returns["net_return"].tolist() == pytest.approx([0.0, 0.0, 0.1])

    shifted = weights.copy()
    shifted.index = shifted.index + pd.Timedelta(minutes=1)
    with pytest.raises(ValueError, match="index must exactly match"):
        _engine(_schedule(INSTRUMENT)).run(
            panel,
            StrategyOutput("misaligned", "test", shifted),
        )


def test_half_in_btc_drifts_to_two_thirds_when_btc_doubles_without_a_fill() -> None:
    panel = _panel([100.0, 200.0])
    weights = pd.DataFrame({INSTRUMENT: [0.5, 0.5]}, index=panel.prices.index)

    result = _engine(_schedule(INSTRUMENT)).run(
        panel,
        StrategyOutput("self_financed", "test", weights),
    )

    assert result.equity.tolist() == pytest.approx([1.0, 1.5])
    assert result.weights.iloc[:, 0].tolist() == pytest.approx([0.5, 2.0 / 3.0])
    assert result.fills["filled_weight"].tolist() == pytest.approx([0.5])
    assert result.metrics.turnover == pytest.approx(0.5)


def test_zero_equity_fails_instead_of_silently_deleting_a_position() -> None:
    panel = _panel([100.0, 0.0])
    weights = pd.DataFrame({INSTRUMENT: [1.0, 1.0]}, index=panel.prices.index)

    with pytest.raises(ValueError, match="price data must be positive"):
        _engine(_schedule(INSTRUMENT)).run(
            panel,
            StrategyOutput("insolvent", "test", weights),
        )


def test_funding_at_decision_time_is_not_earned_by_a_new_position() -> None:
    panel = _panel([100.0, 100.0, 100.0])
    panel.funding.iloc[:, 0] = [0.0, 0.10, 0.10]
    weights = pd.DataFrame({INSTRUMENT: [0.0, 1.0, 0.0]}, index=panel.prices.index)

    result = _engine(_schedule(INSTRUMENT)).run(
        panel,
        StrategyOutput("funding_causal", "test", weights),
    )
    assert result.returns["funding_return"].tolist() == pytest.approx([0.0, 0.0, -0.10])


def test_point_in_time_finality_and_historical_universe_fail_closed() -> None:
    panel = _panel([100.0, 101.0, 102.0])
    weights = pd.DataFrame({INSTRUMENT: [1.0, 1.0, 0.0]}, index=panel.prices.index)
    panel.available_at.iloc[0, 0] = panel.prices.index[0] + pd.Timedelta(seconds=1)
    with pytest.raises(ValueError, match="not available"):
        _engine(_schedule(INSTRUMENT)).run(panel, StrategyOutput("late", "test", weights))

    panel.available_at.iloc[0, 0] = panel.prices.index[0]
    panel.finality.iloc[0, 0] = False
    with pytest.raises(ValueError, match="non-final"):
        _engine(_schedule(INSTRUMENT)).run(panel, StrategyOutput("draft", "test", weights))

    panel.finality.iloc[0, 0] = True
    panel.tradable.iloc[0, 0] = False
    with pytest.raises(ValueError, match="historical universe"):
        _engine(_schedule(INSTRUMENT)).run(panel, StrategyOutput("survivor", "test", weights))


def test_calibrated_claims_require_hashed_non_placeholder_evidence() -> None:
    panel = _panel([100.0, 101.0, 102.0])
    weights = pd.DataFrame(0.0, index=panel.prices.index, columns=panel.prices.columns)
    panel.metadata.pop("calibration_evidence_hash")
    with pytest.raises(ValueError, match="market data require a calibration_evidence_hash"):
        _engine(_schedule(INSTRUMENT)).run(panel, StrategyOutput("claim", "test", weights))

    rule = CostRule(
        INSTRUMENT,
        maker_fee_bps=0.0,
        taker_fee_bps=0.0,
        slippage=SlippageModel(max_participation=1.0),
        source="versioned-cost-observations",
    )
    with pytest.raises(ValueError, match="costs require a calibration_evidence_hash"):
        CostSchedule((rule,), calibration_status="CALIBRATED")
    with pytest.raises(ValueError, match="maker fills require a calibration_evidence_hash"):
        MakerFillModel(
            calibration_id="logistic-fit-v1",
            calibration_status="CALIBRATED",
        )
    with pytest.raises(ValueError, match="non-placeholder evidence sources"):
        CostSchedule(
            (
                CostRule(
                    INSTRUMENT,
                    maker_fee_bps=0.0,
                    taker_fee_bps=0.0,
                    slippage=SlippageModel(max_participation=1.0),
                    source="synthetic-placeholder",
                ),
            ),
            calibration_status="CALIBRATED",
            calibration_evidence_hash="b" * 64,
        )
    with pytest.raises(ValueError, match="non-placeholder calibration_id"):
        MakerFillModel(
            calibration_id="uncalibrated-default",
            calibration_status="CALIBRATED",
            calibration_evidence_hash="b" * 64,
        )

    calibrated_maker = MakerFillModel(
        calibration_id="logistic-fill-fit-v1",
        calibration_status="CALIBRATED",
        calibration_evidence_hash="c" * 64,
    )
    result = _engine(
        _schedule(INSTRUMENT),
        ExecutionConfig(maker_fill=calibrated_maker, require_point_in_time=True),
    ).run(panel=_panel([100.0, 101.0, 102.0]), output=StrategyOutput("proof", "test", weights))
    assert result.diagnostics["audit_status"] == "CALIBRATED"
    assert result.diagnostics["data_calibration_evidence_hash"] == "b" * 64
    assert result.diagnostics["cost_calibration_evidence_hash"] == "b" * 64
    assert result.diagnostics["maker_calibration_evidence_hash"] == "c" * 64


def test_csv_point_in_time_flags_reject_ambiguous_boolean_values(tmp_path: Path) -> None:
    panel = _panel([100.0, 101.0, 102.0])
    save_panel_csv(panel, tmp_path)
    finality_path = tmp_path / "finality.csv"
    finality_path.write_text(
        finality_path.read_text(encoding="utf-8").replace("True", "not-final", 1),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="finality CSV contains a non-boolean"):
        load_panel_csv(tmp_path)


def test_flat_observation_cannot_hide_future_or_non_final_data_used_later() -> None:
    panel = _panel([100.0, 101.0, 102.0])
    weights = pd.DataFrame({INSTRUMENT: [0.0, 1.0, 0.0]}, index=panel.prices.index)
    panel.available_at.iloc[0, 0] = panel.prices.index[2]
    with pytest.raises(ValueError, match="not available"):
        _engine(_schedule(INSTRUMENT)).run(panel, StrategyOutput("late_flat", "test", weights))

    panel.available_at.iloc[0, 0] = panel.prices.index[0]
    panel.finality.iloc[0, 0] = False
    with pytest.raises(ValueError, match="non-final"):
        _engine(_schedule(INSTRUMENT)).run(panel, StrategyOutput("draft_flat", "test", weights))


def test_non_final_auxiliary_feature_cannot_hide_behind_a_missing_price() -> None:
    panel = _panel([100.0, 101.0, 102.0])
    weights = pd.DataFrame({INSTRUMENT: [0.0, 0.0, 0.0]}, index=panel.prices.index)
    panel.prices.iloc[0, 0] = float("nan")
    panel.funding.iloc[0, 0] = 0.01
    panel.finality.iloc[0, 0] = False

    with pytest.raises(ValueError, match="non-final"):
        _engine(_schedule(INSTRUMENT)).run(
            panel,
            StrategyOutput("draft_auxiliary", "test", weights),
        )


def test_delisting_allows_only_a_flattening_trade() -> None:
    panel = _panel([100.0, 95.0, 95.0])
    panel.tradable.iloc[1:, 0] = False
    flatten = pd.DataFrame({INSTRUMENT: [1.0, 0.0, 0.0]}, index=panel.prices.index)

    result = _engine(_schedule(INSTRUMENT)).run(
        panel,
        StrategyOutput("delisting_exit", "test", flatten),
    )
    assert result.weights.iloc[:, 0].tolist() == pytest.approx([1.0, 0.0, 0.0])


def test_slippage_is_size_and_depth_dependent_and_capacity_is_explicit() -> None:
    model = SlippageModel(
        base_bps=1.0,
        impact_coefficient_bps=20.0,
        exponent=1.0,
        max_participation=0.10,
    )
    small = model.estimate(notional_usd=1_000.0, depth_usd=100_000.0)
    large = model.estimate(notional_usd=20_000.0, depth_usd=100_000.0)
    shallow = model.estimate(notional_usd=1_000.0, depth_usd=5_000.0)

    assert small.slippage_bps < large.slippage_bps
    assert small.slippage_bps < shallow.slippage_bps
    assert large.fill_fraction == pytest.approx(0.5)


def test_cost_schedule_is_point_in_time_and_unknown_instruments_fail_closed() -> None:
    schedule = CostSchedule(
        rules=(
            CostRule(
                INSTRUMENT,
                1.0,
                2.0,
                SlippageModel(max_participation=1.0),
                effective_from="2026-01-01T00:00:00Z",
                effective_to="2026-02-01T00:00:00Z",
                source="fee-v1",
            ),
            CostRule(
                INSTRUMENT,
                3.0,
                4.0,
                SlippageModel(max_participation=1.0),
                effective_from="2026-02-01T00:00:00Z",
                source="fee-v2",
            ),
        ),
        calibration_status="CALIBRATED",
        calibration_evidence_hash="b" * 64,
    )

    assert schedule.lookup(pd.Timestamp("2026-01-15T00:00:00Z"), INSTRUMENT).source == "fee-v1"
    assert schedule.lookup(pd.Timestamp("2026-02-15T00:00:00Z"), INSTRUMENT).source == "fee-v2"
    with pytest.raises(ValueError, match="no point-in-time cost rule"):
        schedule.lookup(pd.Timestamp("2026-02-15T00:00:00Z"), "HL:ETH:perp")


def test_maker_non_fill_is_first_class_and_seed_is_reproducible() -> None:
    panel = _panel([100.0, 100.0, 100.0])
    weights = pd.DataFrame({INSTRUMENT: [1.0, 1.0, 1.0]}, index=panel.prices.index)
    output = StrategyOutput(
        "maker",
        "test",
        weights,
        order_types=pd.DataFrame("maker", index=weights.index, columns=weights.columns),
    )
    never = ExecutionConfig(
        maker_fill=MakerFillModel(base_probability=0.0, calibration_id="fixture"),
        emergency_ioc=False,
        seed=7,
        require_point_in_time=True,
    )
    missed = _engine(_schedule(INSTRUMENT), never).run(panel, output)

    assert (missed.weights == 0.0).all(axis=None)
    assert missed.fills["status"].tolist() == ["NO_FILL"]
    assert missed.diagnostics["missed_orders"] == 1

    always = replace(
        never,
        maker_fill=MakerFillModel(base_probability=1.0, calibration_id="fixture"),
    )
    first = _engine(_schedule(INSTRUMENT), always).run(panel, output)
    second = _engine(_schedule(INSTRUMENT), always).run(panel, output)
    pd.testing.assert_frame_equal(first.fills, second.fills)
    assert first.fills["status"].tolist() == ["FILLED"]
    assert first.weights.iloc[0, 0] == pytest.approx(1.0)


def test_second_leg_delay_exposes_hedge_pnl() -> None:
    instruments = ("HL:BTC:spot", "HL:BTC:perp")
    panel = _panel([100.0, 110.0, 110.0, 110.0], instruments=instruments)
    weights = pd.DataFrame(
        {
            instruments[0]: [0.5, 0.5, 0.5, 0.5],
            instruments[1]: [-0.5, -0.5, -0.5, -0.5],
        },
        index=panel.prices.index,
    )
    result = _engine(
        _schedule(*instruments),
        ExecutionConfig(leg_delay_bars=1, require_point_in_time=True),
    ).run(
        panel,
        StrategyOutput(
            "two_leg",
            "test",
            weights,
            hedge_groups={"btc_basis": instruments},
        ),
    )

    assert result.weights.loc[panel.prices.index[0]].tolist() == pytest.approx([0.5, 0.0])
    assert result.weights.loc[panel.prices.index[1]].tolist() == pytest.approx([0.5 * 1.1 / 1.05, -0.5])
    assert result.returns.loc[panel.prices.index[1], "hedge_return"] == pytest.approx(0.05)


def test_imbalanced_pair_splits_matched_basis_from_residual_hedge_pnl() -> None:
    instruments = ("HL:BTC:spot", "HL:BTC:perp")
    panel = _panel([100.0, 200.0], instruments=instruments)
    weights = pd.DataFrame(
        {instruments[0]: [0.5, 0.5], instruments[1]: [-0.1, -0.1]},
        index=panel.prices.index,
    )
    result = _engine(_schedule(*instruments)).run(
        panel,
        StrategyOutput(
            "imbalanced_basis",
            "test",
            weights,
            hedge_groups={"btc_basis": instruments},
        ),
    )

    assert result.returns.loc[panel.prices.index[1], "basis_return"] == pytest.approx(0.0)
    assert result.returns.loc[panel.prices.index[1], "hedge_return"] == pytest.approx(0.4)
    assert result.returns.loc[panel.prices.index[1], "net_return"] == pytest.approx(0.4)


def test_obsolete_due_order_is_cancelled_before_it_can_execute() -> None:
    panel = _panel([100.0, 100.0, 100.0])
    weights = pd.DataFrame({INSTRUMENT: [1.0, 0.0, 0.0]}, index=panel.prices.index)
    result = _engine(
        _schedule(INSTRUMENT),
        ExecutionConfig(base_latency_bars=1, require_point_in_time=True),
    ).run(panel, StrategyOutput("obsolete", "test", weights))

    assert result.weights.iloc[:, 0].tolist() == pytest.approx([0.0, 0.0, 0.0])
    assert result.fills["status"].tolist() == ["CANCELLED"]


def test_failed_maker_entry_is_not_converted_into_an_emergency_ioc_entry() -> None:
    panel = _panel([100.0, 100.0, 100.0])
    weights = pd.DataFrame({INSTRUMENT: [1.0, 1.0, 1.0]}, index=panel.prices.index)
    maker = pd.DataFrame("maker", index=weights.index, columns=weights.columns)
    result = _engine(
        _schedule(INSTRUMENT),
        ExecutionConfig(
            maker_fill=MakerFillModel(base_probability=0.0, calibration_id="fixture"),
            maker_timeout_bars=1,
            emergency_ioc=True,
            ioc_extra_slippage_bps=0.0,
            require_point_in_time=True,
        ),
    ).run(panel, StrategyOutput("entry_not_chased", "test", weights, order_types=maker))

    assert result.fills["status"].tolist() == ["NO_FILL"]
    assert (result.weights == 0.0).all(axis=None)
    assert result.diagnostics["emergency_ioc_attempts"] == 0


def test_failed_maker_exit_uses_a_risk_reducing_emergency_ioc() -> None:
    panel = _panel([100.0, 100.0, 100.0])
    weights = pd.DataFrame({INSTRUMENT: [1.0, 0.0, 0.0]}, index=panel.prices.index)
    order_types = pd.DataFrame(
        {INSTRUMENT: ["taker", "maker", "maker"]},
        index=panel.prices.index,
    )
    result = _engine(
        _schedule(INSTRUMENT),
        ExecutionConfig(
            maker_fill=MakerFillModel(base_probability=0.0, calibration_id="fixture"),
            maker_timeout_bars=1,
            emergency_ioc=True,
            require_point_in_time=True,
        ),
    ).run(
        panel,
        StrategyOutput("emergency_exit", "test", weights, order_types=order_types),
    )

    assert result.fills["status"].tolist() == ["FILLED", "NO_FILL", "IOC_FILLED"]
    assert result.weights.iloc[:, 0].tolist() == pytest.approx([1.0, 1.0, 0.0])


def test_failed_group_leg_is_ioc_hedged_only_after_the_sibling_fills() -> None:
    instruments = ("HL:BTC:spot", "HL:BTC:perp")
    panel = _panel([100.0, 100.0, 100.0], instruments=instruments)
    weights = pd.DataFrame(
        {instruments[0]: [0.5] * 3, instruments[1]: [-0.5] * 3},
        index=panel.prices.index,
    )
    order_types = pd.DataFrame(
        {instruments[0]: ["maker"] * 3, instruments[1]: ["taker"] * 3},
        index=panel.prices.index,
    )
    result = _engine(
        _schedule(*instruments),
        ExecutionConfig(
            maker_fill=MakerFillModel(base_probability=0.0, calibration_id="fixture"),
            maker_timeout_bars=1,
            emergency_ioc=True,
            ioc_extra_slippage_bps=0.0,
            require_point_in_time=True,
        ),
    ).run(
        panel,
        StrategyOutput(
            "hedge_candidate",
            "test",
            weights,
            order_types=order_types,
            hedge_groups={"btc_basis": instruments},
        ),
    )

    assert result.fills["status"].tolist() == ["NO_FILL", "FILLED", "IOC_FILLED"]
    assert result.weights.iloc[1].tolist() == pytest.approx([0.5, -0.5])


def test_cost_components_reconcile_and_adverse_cost_stress_does_not_improve_rebate() -> None:
    panel = _panel([100.0, 100.0], spread_bps=4.0)
    weights = pd.DataFrame({INSTRUMENT: [1.0, 0.0]}, index=panel.prices.index)
    costs = _schedule(
        INSTRUMENT,
        maker_fee_bps=-1.0,
        taker_fee_bps=2.0,
        base_slippage_bps=1.0,
        impact_bps=0.0,
    )
    base = _engine(costs).run(panel, StrategyOutput("cost", "test", weights))
    stressed = _engine(
        costs,
        ExecutionConfig(cost_multiplier=2.0, require_point_in_time=True),
    ).run(panel, StrategyOutput("cost", "test", weights))

    additive = [
        "price_return",
        "funding_return",
        "basis_return",
        "spread_return",
        "fee_return",
        "slippage_return",
        "hedge_return",
    ]
    assert base.returns["net_return"].equals(base.returns[additive].sum(axis=1))
    base_cost_pnl = float(base.attribution[["spread_pnl", "fee_pnl", "slippage_pnl"]].sum().sum())
    stressed_cost_pnl = float(stressed.attribution[["spread_pnl", "fee_pnl", "slippage_pnl"]].sum().sum())
    assert stressed_cost_pnl == pytest.approx(2.0 * base_cost_pnl)

    maker = StrategyOutput(
        "rebate",
        "test",
        weights,
        order_types=pd.DataFrame("maker", index=weights.index, columns=weights.columns),
    )
    maker_base = _engine(
        costs,
        ExecutionConfig(
            maker_fill=MakerFillModel(base_probability=1.0, calibration_id="fixture"),
            require_point_in_time=True,
        ),
    ).run(panel, maker)
    maker_stress = _engine(
        costs,
        ExecutionConfig(
            maker_fill=MakerFillModel(base_probability=1.0, calibration_id="fixture"),
            cost_multiplier=2.0,
            require_point_in_time=True,
        ),
    ).run(panel, maker)
    assert maker_stress.returns["fee_return"].sum() <= maker_base.returns["fee_return"].sum()


def test_latency_stress_changes_execution_without_changing_signal() -> None:
    panel = _panel([100.0, 110.0, 121.0, 121.0])
    weights = pd.DataFrame({INSTRUMENT: [1.0, 1.0, 1.0, 0.0]}, index=panel.prices.index)
    output = StrategyOutput("latency", "test", weights)
    base = _engine(_schedule(INSTRUMENT)).run(panel, output)
    delayed = _engine(
        _schedule(INSTRUMENT),
        ExecutionConfig(base_latency_bars=1, require_point_in_time=True),
    ).run(panel, output)

    pd.testing.assert_frame_equal(base.target_weights, delayed.target_weights)
    assert base.metrics.total_return > delayed.metrics.total_return


def test_latency_does_not_starve_a_risk_reduction_after_weight_drift() -> None:
    panel = _panel([100.0, 100.0, 200.0, 200.0, 200.0])
    weights = pd.DataFrame({INSTRUMENT: [0.5] * 5}, index=panel.prices.index)
    result = PanelBacktester(
        costs=_schedule(INSTRUMENT),
        risk_limits=RiskLimits(0.5, 0.5, 0.5),
        execution=ExecutionConfig(base_latency_bars=1, require_point_in_time=True),
    ).run(panel, StrategyOutput("risk_latency", "test", weights))

    assert result.weights.iloc[:, 0].tolist() == pytest.approx([0.0, 0.5, 2.0 / 3.0, 0.5, 0.5])
    assert result.fills["status"].tolist() == ["FILLED", "FILLED"]
    assert result.fills["filled_weight"].tolist() == pytest.approx([0.5, -1.0 / 6.0])


def test_terminal_mark_expires_pending_orders_and_never_trades_the_close() -> None:
    panel = _panel([100.0, 100.0, 200.0])
    weights = pd.DataFrame({INSTRUMENT: [0.5, 0.5, 0.5]}, index=panel.prices.index)
    output = StrategyOutput(
        "terminal_mark",
        "test",
        weights,
        diagnostics={
            "terminal_mark_without_decision": panel.prices.index[-1].isoformat(),
        },
        order_types=pd.DataFrame("maker", index=weights.index, columns=weights.columns),
    )
    result = PanelBacktester(
        costs=_schedule(INSTRUMENT),
        risk_limits=RiskLimits(0.5, 0.5, 0.5),
        execution=ExecutionConfig(
            base_latency_bars=1,
            maker_fill=MakerFillModel(base_probability=1.0, calibration_id="fixture"),
            require_point_in_time=True,
        ),
    ).run(panel, output)

    assert result.fills["status"].tolist() == ["FILLED"]
    assert result.fills["timestamp"].max() < panel.prices.index[-1]
    assert result.returns.loc[
        panel.prices.index[-1], ["spread_return", "fee_return", "slippage_return"]
    ].sum() == pytest.approx(0.0)
    assert result.weights.iloc[-1, 0] == pytest.approx(2.0 / 3.0)


def test_terminal_mark_expires_an_order_due_on_the_close() -> None:
    panel = _panel([100.0, 100.0, 200.0])
    weights = pd.DataFrame({INSTRUMENT: [0.5, 0.5, 0.5]}, index=panel.prices.index)
    output = StrategyOutput(
        "terminal_due",
        "test",
        weights,
        diagnostics={"terminal_mark_without_decision": panel.prices.index[-1].isoformat()},
    )
    result = PanelBacktester(
        costs=_schedule(INSTRUMENT),
        risk_limits=RiskLimits(1.0, 1.0, 1.0),
        execution=ExecutionConfig(base_latency_bars=2, require_point_in_time=True),
    ).run(panel, output)

    assert result.fills["status"].tolist() == ["EXPIRED"]
    assert result.fills["filled_weight"].tolist() == [0.0]
    assert (result.weights == 0.0).all(axis=None)
    assert result.metrics.turnover == 0.0


def test_terminal_mark_placeholder_is_not_treated_as_a_delisted_asset_trade() -> None:
    panel = _panel([100.0, 100.0, 110.0])
    panel.tradable.iloc[-1, 0] = False
    weights = pd.DataFrame({INSTRUMENT: [0.5, 0.5, 0.5]}, index=panel.prices.index)
    result = _engine(_schedule(INSTRUMENT)).run(
        panel,
        StrategyOutput(
            "terminal_delisting",
            "test",
            weights,
            diagnostics={"terminal_mark_without_decision": panel.prices.index[-1].isoformat()},
        ),
    )

    assert result.fills["timestamp"].max() < panel.prices.index[-1]
    assert result.weights.iloc[-1, 0] > result.weights.iloc[-2, 0]
