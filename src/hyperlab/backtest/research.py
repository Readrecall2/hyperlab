from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import cast

import pandas as pd

from hyperlab.backtest.engine import PanelBacktester
from hyperlab.backtest.protocol import SelectionSplitView, TimeRange, WalkForwardWindow
from hyperlab.models import BacktestResult, MarketPanel, StrategyOutput
from hyperlab.strategies.base import Strategy


def _copy_panel_index(
    panel: MarketPanel,
    index: pd.DatetimeIndex,
    *,
    slice_metadata: dict[str, str],
) -> MarketPanel:
    if index.empty:
        raise ValueError("requested panel interval contains no observations")

    def optional_frame(frame: pd.DataFrame | None) -> pd.DataFrame | None:
        return frame.loc[index].copy() if frame is not None else None

    return MarketPanel(
        prices=panel.prices.loc[index].copy(),
        funding=panel.funding.loc[index].copy(),
        spreads_bps=panel.spreads_bps.loc[index].copy(),
        volume_usd=panel.volume_usd.loc[index].copy(),
        metadata={
            **panel.metadata,
            "research_slice": slice_metadata,
        },
        depth_usd=optional_frame(panel.depth_usd),
        open_interest_usd=optional_frame(panel.open_interest_usd),
        liquidation_usd=optional_frame(panel.liquidation_usd),
        available_at=optional_frame(panel.available_at),
        finality=optional_frame(panel.finality),
        tradable=optional_frame(panel.tradable),
        regimes=panel.regimes.loc[index].copy() if panel.regimes is not None else None,
    )


def slice_panel(panel: MarketPanel, interval: TimeRange) -> MarketPanel:
    """Copy one UTC half-open interval without dropping point-in-time controls."""

    panel.validate()
    mask = (panel.prices.index >= interval.start) & (panel.prices.index < interval.end)
    index = pd.DatetimeIndex(panel.prices.index[mask])
    return _copy_panel_index(
        panel,
        index,
        slice_metadata={
            "start": interval.start.isoformat(),
            "end_exclusive": interval.end.isoformat(),
        },
    )


def _panel_prefix(panel: MarketPanel, timestamp: pd.Timestamp) -> MarketPanel:
    index = pd.DatetimeIndex(panel.prices.index[panel.prices.index <= timestamp])
    return _copy_panel_index(
        panel,
        index,
        slice_metadata={
            "start": panel.prices.index[0].isoformat(),
            "through_inclusive": timestamp.isoformat(),
        },
    )


def _strategy_feature_view(panel: MarketPanel) -> MarketPanel:
    """Hide cells outside the point-in-time tradable universe from feature code.

    The execution panel retains marks for risk-reducing exits, but a strategy may not
    use an inactive instrument as a cross-sectional feature. Historical rows from
    before a later delisting remain available, which preserves the dead asset rather
    than introducing survivorship bias.
    """

    if panel.tradable is None:
        return panel
    mask = panel.tradable.eq(True)
    # The full execution schema intentionally retains delisted instruments, but an
    # identity whose first lifecycle appearance is still in the future must not even
    # be visible to cross-sectional feature code. Otherwise counting or naming panel
    # columns is a survivorship/look-ahead side channel despite every cell being NaN.
    known_columns = mask.any(axis=0)
    visible_columns = panel.prices.columns[known_columns.to_numpy()]
    if visible_columns.empty:
        raise ValueError("no instrument is known from the historical lifecycle at this decision")
    mask = mask.loc[:, visible_columns]
    return MarketPanel(
        prices=panel.prices.loc[:, visible_columns].where(mask),
        funding=panel.funding.loc[:, visible_columns].where(mask),
        spreads_bps=panel.spreads_bps.loc[:, visible_columns].where(mask),
        volume_usd=panel.volume_usd.loc[:, visible_columns].where(mask),
        metadata={**panel.metadata, "feature_universe_masked": True},
        depth_usd=panel.depth_usd.loc[:, visible_columns].where(mask)
        if panel.depth_usd is not None
        else None,
        open_interest_usd=panel.open_interest_usd.loc[:, visible_columns].where(mask)
        if panel.open_interest_usd is not None
        else None,
        liquidation_usd=panel.liquidation_usd.loc[:, visible_columns].where(mask)
        if panel.liquidation_usd is not None
        else None,
        available_at=panel.available_at.loc[:, visible_columns].where(mask)
        if panel.available_at is not None
        else None,
        finality=panel.finality.loc[:, visible_columns].where(mask)
        if panel.finality is not None
        else None,
        tradable=panel.tradable.loc[:, visible_columns].copy(),
        regimes=panel.regimes.copy() if panel.regimes is not None else None,
    )


def _slice_output(output: StrategyOutput, index: pd.DatetimeIndex) -> StrategyOutput:
    if not index.isin(output.weights.index).all():
        raise ValueError("strategy output does not cover the complete validation interval")
    return StrategyOutput(
        name=output.name,
        risk_tier=output.risk_tier,
        weights=output.weights.loc[index].copy(),
        diagnostics=dict(output.diagnostics),
        order_types=output.order_types.loc[index].copy() if output.order_types is not None else None,
        hedge_groups=dict(output.hedge_groups),
    )


def _generate_causal_validation(
    strategy: Strategy,
    history: MarketPanel,
    validation_index: pd.DatetimeIndex,
) -> StrategyOutput:
    """Generate each OOS decision from a prefix ending exactly at that decision."""

    weight_rows: list[pd.Series] = []
    order_type_rows: list[pd.Series] = []
    latest: StrategyOutput | None = None
    accumulated_groups: dict[str, tuple[str, ...]] = {}
    uses_order_types: bool | None = None
    for timestamp in validation_index:
        prefix = _panel_prefix(history, timestamp)
        feature_view = _strategy_feature_view(prefix)
        generated = strategy.generate(feature_view)
        if not generated.weights.index.equals(feature_view.prices.index):
            raise ValueError("strategy prefix output index must exactly match its input history")
        if list(generated.weights.columns) != list(feature_view.prices.columns):
            raise ValueError("strategy prefix output columns must exactly match its input history")
        groups = dict(generated.hedge_groups)
        for group_id, instruments in groups.items():
            previous = accumulated_groups.get(group_id)
            if previous is not None and previous != instruments:
                raise ValueError("strategy changed an existing hedge group during one OOS window")
            accumulated_groups[group_id] = instruments
        has_order_types = generated.order_types is not None
        if uses_order_types is None:
            uses_order_types = has_order_types
        elif has_order_types != uses_order_types:
            raise ValueError("strategy order type policy changed shape during one OOS window")
        weight_rows.append(
            cast(pd.Series, generated.weights.loc[timestamp])
            .reindex(history.prices.columns, fill_value=0.0)
            .copy()
        )
        if generated.order_types is not None:
            if not generated.order_types.index.equals(feature_view.prices.index):
                raise ValueError("strategy order types index must exactly match its input history")
            if list(generated.order_types.columns) != list(feature_view.prices.columns):
                raise ValueError("strategy order types columns must exactly match its input history")
            order_type_rows.append(
                cast(pd.Series, generated.order_types.loc[timestamp])
                .reindex(history.prices.columns, fill_value="taker")
                .copy()
            )
        latest = generated
    if latest is None:
        raise ValueError("validation interval contains no decision")
    weights = pd.DataFrame(weight_rows, index=validation_index, columns=history.prices.columns)
    order_types = (
        pd.DataFrame(order_type_rows, index=validation_index, columns=history.prices.columns)
        if uses_order_types
        else None
    )
    return StrategyOutput(
        latest.name,
        latest.risk_tier,
        weights,
        dict(latest.diagnostics),
        order_types,
        accumulated_groups,
    )


def generate_causal_evaluation(
    strategy: Strategy,
    history: MarketPanel,
    evaluation_index: pd.DatetimeIndex,
) -> StrategyOutput:
    """Generate an evaluation slice one decision prefix at a time.

    ``history`` may contain warm-up observations before ``evaluation_index`` but may
    not extend past its last decision. This public boundary is used by the one-shot
    final-test workflow after the locked range has been revealed.
    """

    history.validate()
    if evaluation_index.empty:
        raise ValueError("evaluation index cannot be empty")
    if not evaluation_index.is_monotonic_increasing or evaluation_index.has_duplicates:
        raise ValueError("evaluation index must be ordered and unique")
    if not evaluation_index.isin(history.prices.index).all():
        raise ValueError("evaluation index must be contained in history")
    if history.prices.index[-1] > evaluation_index[-1]:
        raise ValueError("history cannot expose observations after the last evaluation decision")
    return _generate_causal_validation(strategy, history, evaluation_index)


def causal_evaluation_with_terminal_mark(
    strategy: Strategy,
    history: MarketPanel,
    evaluation_index: pd.DatetimeIndex,
    terminal_timestamp: pd.Timestamp,
) -> StrategyOutput:
    """Generate decisions without showing the terminal close used to settle PnL.

    The returned output covers the full execution panel. Its terminal target is the
    prior decision, so the unseen terminal bar can only mark an already-held position;
    it cannot create a decision from final close data.
    """

    if terminal_timestamp not in history.prices.index:
        raise ValueError("terminal timestamp must be present in history")
    if evaluation_index.empty or evaluation_index[-1] >= terminal_timestamp:
        raise ValueError("terminal mark must follow every evaluation decision")
    decision_history = _copy_panel_index(
        history,
        pd.DatetimeIndex(history.prices.index[history.prices.index < terminal_timestamp]),
        slice_metadata={
            "start": history.prices.index[0].isoformat(),
            "before_terminal": terminal_timestamp.isoformat(),
        },
    )
    decisions = generate_causal_evaluation(strategy, decision_history, evaluation_index)
    weights = decisions.weights.loc[evaluation_index].copy()
    weights.loc[terminal_timestamp] = weights.iloc[-1]
    execution_index = evaluation_index.append(pd.DatetimeIndex([terminal_timestamp]))
    weights = weights.reindex(execution_index)
    order_types = None
    if decisions.order_types is not None:
        order_types = decisions.order_types.loc[evaluation_index].copy()
        order_types.loc[terminal_timestamp] = order_types.iloc[-1]
        order_types = order_types.reindex(execution_index)
    diagnostics = {
        **decisions.diagnostics,
        "terminal_mark_without_decision": terminal_timestamp.isoformat(),
    }
    return StrategyOutput(
        decisions.name,
        decisions.risk_tier,
        weights,
        diagnostics,
        order_types,
        dict(decisions.hedge_groups),
    )


@dataclass(frozen=True, slots=True)
class WalkForwardFoldResult:
    window: WalkForwardWindow
    result: BacktestResult


@dataclass(frozen=True, slots=True)
class WalkForwardResult:
    folds: tuple[WalkForwardFoldResult, ...]

    def __post_init__(self) -> None:
        if not self.folds:
            raise ValueError("walk-forward result needs at least one fold")
        indices = [fold.result.returns.index for fold in self.folds]
        combined = indices[0]
        for index in indices[1:]:
            combined = combined.append(index)
        if combined.has_duplicates or not combined.is_monotonic_increasing:
            raise ValueError("walk-forward out-of-sample intervals must be ordered and disjoint")

    @property
    def out_of_sample_returns(self) -> pd.DataFrame:
        return pd.concat([fold.result.returns for fold in self.folds]).sort_index()

    @property
    def out_of_sample_equity(self) -> pd.Series:
        equity = (1.0 + self.out_of_sample_returns["net_return"]).cumprod()
        equity.name = "walk_forward_oos"
        return equity


def run_walk_forward(
    panel: MarketPanel,
    windows: Sequence[WalkForwardWindow],
    *,
    selection_view: SelectionSplitView,
    fit_strategy: Callable[[MarketPanel], Strategy],
    backtester: Callable[[WalkForwardWindow], PanelBacktester],
) -> WalkForwardResult:
    """Fit only on each past train range and evaluate the following OOS range.

    The fitted strategy receives a history ending at the validation boundary so its
    rolling indicators can warm up from train data. Only the validation slice is
    executed and returned. Strategy causal-prefix tests remain mandatory because a
    Python strategy implementation could otherwise inspect later rows in that history.
    """

    if not windows:
        raise ValueError("walk-forward execution needs at least one window")
    if not isinstance(selection_view, SelectionSplitView):
        raise TypeError("selection_view must hide and bind the final-test interval")
    allowed = TimeRange(selection_view.train.start, selection_view.validation.end)
    folds: list[WalkForwardFoldResult] = []
    previous_validation_end = None
    for window in windows:
        if not allowed.contains(window.train) or not allowed.contains(window.validation):
            raise ValueError("walk-forward windows must stay inside the locked selection view")
        if previous_validation_end is not None and window.validation.start < previous_validation_end:
            raise ValueError("walk-forward validation intervals overlap")
        train_panel = slice_panel(panel, window.train)
        engine = backtester(window)
        engine.validate_research_panel(train_panel)
        fitted = fit_strategy(train_panel)
        if not hasattr(fitted, "generate"):
            raise TypeError("fit_strategy must return a Strategy")
        history_range = TimeRange(window.train.start, window.validation.end)
        history = slice_panel(panel, history_range)
        engine.validate_research_panel(history)
        validation = slice_panel(panel, window.validation)
        validation_index = pd.DatetimeIndex(validation.prices.index)
        generated = _generate_causal_validation(fitted, history, validation_index)
        output = _slice_output(generated, validation_index)
        result = engine.run(validation, output)
        result.diagnostics["walk_forward_window"] = window.to_dict()
        result.diagnostics["evaluation_split"] = "walk_forward_oos"
        result.diagnostics["split_hash"] = selection_view.plan_hash
        result.diagnostics["data_hash"] = selection_view.dataset_hash
        folds.append(WalkForwardFoldResult(window=window, result=result))
        previous_validation_end = window.validation.end
    return WalkForwardResult(tuple(folds))
