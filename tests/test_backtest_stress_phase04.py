from __future__ import annotations

import pandas as pd
import pytest

from hyperlab.backtest.costs import CostRule, CostSchedule, SlippageModel
from hyperlab.backtest.engine import PanelBacktester
from hyperlab.backtest.execution import ExecutionConfig
from hyperlab.backtest.stress import closed_trade_ledger, remove_best_trades
from hyperlab.models import MarketPanel, RiskLimits, StrategyOutput


def test_best_trade_removal_uses_completed_trades_and_is_deterministic() -> None:
    index = pd.date_range("2026-01-01", periods=6, freq="1h", tz="UTC")
    instrument = "HL:BTC:perp"
    prices = pd.DataFrame(
        {instrument: [100.0, 150.0, 150.0, 180.0, 180.0, 198.0]},
        index=index,
    )
    zero = pd.DataFrame(0.0, index=index, columns=prices.columns)
    panel = MarketPanel(
        prices,
        zero.copy(),
        zero.copy(),
        pd.DataFrame(1_000_000.0, index=index, columns=prices.columns),
        depth_usd=pd.DataFrame(1_000_000.0, index=index, columns=prices.columns),
    )
    weights = pd.DataFrame({instrument: [1.0, 0.0, 1.0, 0.0, 1.0, 0.0]}, index=index)
    schedule = CostSchedule(
        rules=(
            CostRule(
                instrument,
                maker_fee_bps=0.0,
                taker_fee_bps=0.0,
                slippage=SlippageModel(max_participation=1.0),
            ),
        ),
        calibration_status="SYNTHETIC",
    )
    result = PanelBacktester(
        costs=schedule,
        risk_limits=RiskLimits(1.0, 1.0, 1.0),
        execution=ExecutionConfig(seed=11),
    ).run(panel, StrategyOutput("round_trips", "test", weights))

    trades = closed_trade_ledger(result)
    assert len(trades) == 3
    assert trades["net_pnl"].tolist() == pytest.approx([50_000.0, 30_000.0, 18_000.0])

    first = remove_best_trades(result, 1 / 3)
    second = remove_best_trades(result, 1 / 3)
    assert first.diagnostics["removed_trade_ids"] == ["HL:BTC:perp:000001"]
    pd.testing.assert_series_equal(first.equity, second.equity)
    assert first.metrics.total_return == pytest.approx(0.32)
    assert first.attribution["net_pnl"].sum() == pytest.approx(32_000.0)
    assert first.metrics.turnover == pytest.approx(first.fills["filled_weight"].abs().sum())
    assert first.diagnostics["best_trade_counterfactual"] == "sequential_self_financed"
    assert first.diagnostics["attribution_reconciled"] is True
    assert first.weights.loc[index[:2], instrument].eq(0.0).all()
    for breakdown in first.breakdowns.values():
        assert breakdown["total_pnl"].sum() == pytest.approx(32_000.0)
