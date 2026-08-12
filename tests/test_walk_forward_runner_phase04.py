from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from hyperlab.backtest.costs import CostRule, CostSchedule, SlippageModel
from hyperlab.backtest.engine import PanelBacktester
from hyperlab.backtest.execution import ExecutionConfig
from hyperlab.backtest.protocol import SplitPlan, TimeRange, WalkForwardSpec
from hyperlab.backtest.research import generate_causal_evaluation, run_walk_forward
from hyperlab.models import MarketPanel, RiskLimits, StrategyOutput


class _FittedStrategy:
    name = "fitted"
    risk_tier = "test"

    def __init__(self, sign: float) -> None:
        self.sign = sign

    def generate(self, panel: MarketPanel) -> StrategyOutput:
        weights = pd.DataFrame(self.sign, index=panel.prices.index, columns=panel.prices.columns)
        return StrategyOutput(self.name, self.risk_tier, weights)


class _ColumnProbeStrategy:
    name = "column_probe"
    risk_tier = "test"

    def __init__(self) -> None:
        self.seen_columns: list[tuple[str, ...]] = []

    def generate(self, panel: MarketPanel) -> StrategyOutput:
        self.seen_columns.append(tuple(str(column) for column in panel.prices.columns))
        weights = pd.DataFrame(0.0, index=panel.prices.index, columns=panel.prices.columns)
        if "HL:FUTURE:perp" in panel.prices.columns:
            weights["HL:BTC:perp"] = 1.0
        return StrategyOutput(self.name, self.risk_tier, weights)


def test_causal_feature_view_hides_future_instrument_identity_cross_sectionally() -> None:
    index = pd.date_range("2026-01-01", periods=5, freq="1h", tz="UTC")
    columns = ["HL:BTC:perp", "HL:FUTURE:perp"]
    prices = pd.DataFrame([[100.0, 10.0]] * len(index), index=index, columns=columns)
    zeros = pd.DataFrame(0.0, index=index, columns=columns)
    tradable = pd.DataFrame(
        {
            "HL:BTC:perp": [True] * len(index),
            "HL:FUTURE:perp": [False, False, False, True, True],
        },
        index=index,
    )
    panel = MarketPanel(
        prices,
        zeros.copy(),
        zeros.copy(),
        pd.DataFrame(1_000_000.0, index=index, columns=columns),
        tradable=tradable,
    )
    strategy = _ColumnProbeStrategy()

    result = generate_causal_evaluation(strategy, panel, index)

    assert all("HL:FUTURE:perp" not in seen for seen in strategy.seen_columns[:3])
    assert all("HL:FUTURE:perp" in seen for seen in strategy.seen_columns[3:])
    assert result.weights.loc[index[:3], "HL:BTC:perp"].eq(0.0).all()
    assert result.weights.loc[index[3:], "HL:BTC:perp"].eq(1.0).all()
    assert result.weights.loc[:, "HL:FUTURE:perp"].eq(0.0).all()


def test_walk_forward_fit_sees_train_only_and_oos_is_disjoint() -> None:
    index = pd.date_range("2026-01-01", periods=16, freq="1D", tz="UTC")
    instrument = "HL:BTC:perp"
    prices = pd.DataFrame({instrument: range(100, 116)}, index=index, dtype=float)
    zero = pd.DataFrame(0.0, index=index, columns=prices.columns)
    panel = MarketPanel(
        prices,
        zero.copy(),
        zero.copy(),
        pd.DataFrame(1_000_000.0, index=index, columns=prices.columns),
        depth_usd=pd.DataFrame(1_000_000.0, index=index, columns=prices.columns),
    )
    spec = WalkForwardSpec(
        bounds=TimeRange(
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 17, tzinfo=UTC),
        ),
        train_window=timedelta(days=6),
        validation_window=timedelta(days=3),
        step=timedelta(days=3),
        embargo=timedelta(days=1),
        expanding=True,
    )
    seen_train_ends: list[pd.Timestamp] = []

    def fit(train: MarketPanel) -> _FittedStrategy:
        seen_train_ends.append(train.prices.index[-1])
        return _FittedStrategy(1.0)

    schedule = CostSchedule(
        (
            CostRule(
                instrument,
                0.0,
                0.0,
                SlippageModel(max_participation=1.0),
            ),
        )
    )
    plan = SplitPlan(
        train=TimeRange(
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 7, tzinfo=UTC),
        ),
        validation=TimeRange(
            datetime(2026, 1, 7, tzinfo=UTC),
            datetime(2026, 1, 17, tzinfo=UTC),
        ),
        test=TimeRange(
            datetime(2026, 1, 17, tzinfo=UTC),
            datetime(2026, 1, 18, tzinfo=UTC),
        ),
        dataset_hash="d" * 64,
    )
    result = run_walk_forward(
        panel,
        spec.windows(),
        selection_view=plan.selection_view,
        fit_strategy=fit,
        backtester=lambda _window: PanelBacktester(
            costs=schedule,
            risk_limits=RiskLimits(1.0, 1.0, 1.0),
            execution=ExecutionConfig(),
        ),
    )

    assert len(result.folds) == 3
    assert result.out_of_sample_returns.index.is_monotonic_increasing
    assert not result.out_of_sample_returns.index.has_duplicates
    for observed, fold in zip(seen_train_ends, result.folds, strict=True):
        assert observed < fold.window.train.end
        assert observed < fold.window.validation.start
        assert fold.result.returns.index.min() >= fold.window.validation.start
        assert fold.result.returns.index.max() < fold.window.validation.end


def test_walk_forward_rejects_a_window_outside_the_locked_selection_view() -> None:
    index = pd.date_range("2026-01-01", periods=12, freq="1D", tz="UTC")
    instrument = "HL:BTC:perp"
    prices = pd.DataFrame({instrument: range(100, 112)}, index=index, dtype=float)
    zero = pd.DataFrame(0.0, index=index, columns=prices.columns)
    panel = MarketPanel(prices, zero.copy(), zero.copy(), zero.copy())
    plan = SplitPlan(
        train=TimeRange(datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 5, tzinfo=UTC)),
        validation=TimeRange(datetime(2026, 1, 5, tzinfo=UTC), datetime(2026, 1, 9, tzinfo=UTC)),
        test=TimeRange(datetime(2026, 1, 9, tzinfo=UTC), datetime(2026, 1, 13, tzinfo=UTC)),
        dataset_hash="d" * 64,
    )
    leaked = WalkForwardSpec(
        bounds=TimeRange(datetime(2026, 1, 5, tzinfo=UTC), datetime(2026, 1, 13, tzinfo=UTC)),
        train_window=timedelta(days=3),
        validation_window=timedelta(days=2),
        step=timedelta(days=2),
    ).windows()

    with pytest.raises(ValueError, match="locked selection view"):
        run_walk_forward(
            panel,
            leaked,
            selection_view=plan.selection_view,
            fit_strategy=lambda _train: _FittedStrategy(1.0),
            backtester=lambda _window: PanelBacktester(
                costs=CostSchedule((CostRule(instrument, 0.0, 0.0, SlippageModel(max_participation=1.0)),)),
                risk_limits=RiskLimits(1.0, 1.0, 1.0),
            ),
        )
