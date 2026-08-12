from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Any, cast

import pandas as pd

from hyperlab.backtest.attribution import aggregate_pnl
from hyperlab.backtest.benchmark import PassiveBenchmarkSpec
from hyperlab.backtest.costs import CostSchedule
from hyperlab.backtest.engine import PNL_COMPONENT_COLUMNS, PanelBacktester
from hyperlab.backtest.execution import ExecutionConfig
from hyperlab.backtest.metrics import compute_metrics
from hyperlab.models import BacktestResult, CostModel, MarketPanel, RiskLimits, StrategyOutput


@dataclass(frozen=True, slots=True)
class StressScenario:
    name: str
    cost_multiplier: float = 1.0
    maker_fill_multiplier: float = 1.0
    added_latency_bars: int = 0
    remove_best_trade_fraction: float = 0.0
    funding_multiplier: float = 1.0

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("stress scenario name cannot be empty")
        for name, value in (
            ("cost_multiplier", self.cost_multiplier),
            ("maker_fill_multiplier", self.maker_fill_multiplier),
        ):
            if not math.isfinite(value) or value <= 0.0:
                raise ValueError(f"{name} must be finite and positive")
        if not math.isfinite(self.funding_multiplier):
            raise ValueError("funding_multiplier must be finite")
        if (
            not isinstance(self.added_latency_bars, int)
            or isinstance(self.added_latency_bars, bool)
            or self.added_latency_bars < 0
        ):
            raise ValueError("added_latency_bars must be a non-negative integer")
        if (
            not math.isfinite(self.remove_best_trade_fraction)
            or not 0.0 <= self.remove_best_trade_fraction <= 1.0
        ):
            raise ValueError("remove_best_trade_fraction must be in [0, 1]")


def execution_for_scenario(
    base: ExecutionConfig,
    scenario: StressScenario,
) -> ExecutionConfig:
    return replace(
        base,
        cost_multiplier=base.cost_multiplier * scenario.cost_multiplier,
        maker_fill_multiplier=base.maker_fill_multiplier * scenario.maker_fill_multiplier,
        base_latency_bars=base.base_latency_bars + scenario.added_latency_bars,
    )


def _trade_groups(result: BacktestResult) -> dict[str, tuple[str, ...]]:
    configured = result.diagnostics.get("hedge_groups", {})
    grouped: dict[str, tuple[str, ...]] = {}
    assigned: set[str] = set()
    if isinstance(configured, dict):
        for group_id, raw in configured.items():
            if isinstance(group_id, str) and isinstance(raw, list):
                instruments = tuple(str(value) for value in raw)
                if instruments:
                    grouped[group_id] = instruments
                    assigned.update(instruments)
    for instrument in result.weights.columns:
        if str(instrument) not in assigned:
            grouped[str(instrument)] = (str(instrument),)
    return grouped


def closed_trade_ledger(result: BacktestResult) -> pd.DataFrame:
    """Build deterministic completed position episodes from actually filled weights."""

    if result.attribution.empty:
        raise ValueError("trade removal requires the execution attribution ledger")
    required = {"timestamp", "instrument", "net_pnl"}
    if not required.issubset(result.attribution.columns):
        raise ValueError("attribution ledger is missing timestamp/instrument/net_pnl")

    rows: list[dict[str, Any]] = []
    for group_id, instruments in _trade_groups(result).items():
        unknown = set(instruments).difference(result.weights.columns)
        if unknown:
            raise ValueError(f"trade group {group_id!r} references unknown instruments")
        gross = result.weights.loc[:, list(instruments)].abs().sum(axis=1)
        active = gross.gt(1e-12)
        start: int | None = None
        sequence = 0
        for row, is_active in enumerate(active):
            if start is None and bool(is_active):
                start = row
                continue
            if start is None or bool(is_active):
                continue
            sequence += 1
            start_time = result.weights.index[start]
            end_time = result.weights.index[row]
            selection = result.attribution[
                result.attribution["instrument"].isin(instruments)
                & result.attribution["timestamp"].between(start_time, end_time, inclusive="both")
            ]
            rows.append(
                {
                    "trade_id": f"{group_id}:{sequence:06d}",
                    "group_id": group_id,
                    "start_time": start_time,
                    "end_time": end_time,
                    "instruments": instruments,
                    "net_pnl": float(selection["net_pnl"].sum()),
                    "complete": True,
                }
            )
            start = None
    return pd.DataFrame(rows)


def remove_best_trades(
    result: BacktestResult,
    fraction: float,
) -> BacktestResult:
    """Remove completed economic trades without refitting or re-ranking the strategy."""

    if not math.isfinite(fraction) or not 0.0 <= fraction <= 1.0:
        raise ValueError("fraction must be in [0, 1]")
    if fraction == 0.0:
        return result
    trades = closed_trade_ledger(result)
    if trades.empty:
        diagnostics = {
            **result.diagnostics,
            "best_trade_removal_fraction": fraction,
            "removed_trade_ids": [],
            "removed_trade_count": 0,
        }
        warnings = list(diagnostics.get("warnings", []))
        warnings.append("Best-trade removal found no completed economic trade.")
        diagnostics["warnings"] = warnings
        return replace(result, diagnostics=diagnostics)
    count = max(1, math.ceil(len(trades) * fraction))
    selected = trades.sort_values(
        ["net_pnl", "trade_id"],
        ascending=[False, True],
        kind="stable",
    ).head(count)

    attribution = result.attribution.copy(deep=True)
    attribution["timestamp"] = pd.to_datetime(attribution["timestamp"], utc=True)
    component_pnl_columns = [
        component.removesuffix("_return") + "_pnl" for component in PNL_COMPONENT_COLUMNS
    ]
    for column in component_pnl_columns:
        if column not in attribution:
            attribution[column] = 0.0
    removed_mask = pd.Series(False, index=attribution.index)
    counterfactual_weights = result.weights.copy(deep=True)
    removed_fill_mask = pd.Series(False, index=result.fills.index)
    for trade in selected.itertuples(index=False):
        instruments = cast(tuple[str, ...], trade.instruments)
        interval_mask = attribution["instrument"].isin(instruments) & attribution["timestamp"].between(
            trade.start_time,
            trade.end_time,
            inclusive="both",
        )
        removed_mask |= interval_mask
        weight_rows = counterfactual_weights.index.to_series().between(
            trade.start_time,
            trade.end_time,
            inclusive="both",
        )
        counterfactual_weights.loc[weight_rows, list(instruments)] = 0.0
        if {"timestamp", "instrument"}.issubset(result.fills.columns):
            fill_timestamps = pd.to_datetime(result.fills["timestamp"], utc=True)
            removed_fill_mask |= result.fills["instrument"].isin(instruments) & fill_timestamps.between(
                trade.start_time,
                trade.end_time,
                inclusive="both",
            )
    attribution.loc[removed_mask, component_pnl_columns] = 0.0
    if "size_usd" in attribution:
        attribution.loc[removed_mask, "size_usd"] = 0.0

    initial_capital = float(result.diagnostics.get("initial_capital", 100_000.0))
    if not math.isfinite(initial_capital) or initial_capital <= 0.0:
        raise ValueError("best-trade stress requires a finite positive initial capital")
    original_start_equity = result.equity.shift(1, fill_value=1.0)
    if bool(original_start_equity.le(0.0).any()):
        raise ValueError("best-trade stress cannot rescale a non-positive original capital path")

    # Preserve each retained component's original return rate, but apply it to the
    # counterfactual capital available at that timestamp. This makes the stress path
    # sequential and self-financed instead of subtracting fixed future dollar PnL.
    counterfactual_equity = 1.0
    equity_values: list[float] = []
    for timestamp in result.equity.index:
        original_start = float(cast(Any, original_start_equity.at[timestamp]))
        scale = counterfactual_equity / original_start
        timestamp_rows = attribution["timestamp"].eq(timestamp)
        attribution.loc[timestamp_rows, component_pnl_columns] *= scale
        attribution.loc[timestamp_rows, "size_usd"] *= scale
        pnl = float(attribution.loc[timestamp_rows, component_pnl_columns].to_numpy().sum())
        period_return = pnl / (counterfactual_equity * initial_capital)
        counterfactual_equity *= 1.0 + period_return
        if not math.isfinite(counterfactual_equity) or counterfactual_equity <= 0.0:
            raise ValueError("best-trade stress reached non-positive capital")
        equity_values.append(counterfactual_equity)

    attribution["net_pnl"] = attribution[component_pnl_columns].sum(axis=1)
    per_timestamp = attribution.groupby("timestamp", sort=False)[component_pnl_columns].sum()
    per_timestamp = per_timestamp.reindex(result.equity.index, fill_value=0.0)
    equity = pd.Series(equity_values, index=result.equity.index, name=result.equity.name)
    start_equity = equity.shift(1, fill_value=1.0)
    denominator = start_equity * initial_capital
    returns = pd.DataFrame(index=result.equity.index)
    for component in PNL_COMPONENT_COLUMNS:
        pnl_column = component.removesuffix("_return") + "_pnl"
        returns[component] = per_timestamp[pnl_column] / denominator
    returns["cost_return"] = returns[["spread_return", "fee_return", "slippage_return"]].sum(axis=1)
    returns["net_return"] = returns[list(PNL_COMPONENT_COLUMNS)].sum(axis=1)
    fills = result.fills.loc[~removed_fill_mask].copy() if not result.fills.empty else result.fills.copy()
    metrics = compute_metrics(
        returns,
        equity,
        counterfactual_weights,
        benchmark=result.benchmark,
        fills=fills,
    )
    metrics.turnover = (
        float(fills["filled_weight"].abs().sum()) if not fills.empty and "filled_weight" in fills else 0.0
    )
    long = attribution.melt(
        id_vars=["timestamp", "asset", "regime", "size_usd"],
        value_vars=component_pnl_columns,
        var_name="component",
        value_name="pnl",
    )
    long["component"] = long["component"].str.removesuffix("_pnl")
    summaries = aggregate_pnl(long)
    summaries.assert_reconciled()
    expected_pnl = (float(equity.iloc[-1]) - 1.0) * initial_capital
    if not math.isclose(summaries.total_pnl, expected_pnl, rel_tol=1e-10, abs_tol=1e-8):
        raise ValueError("best-trade stress attribution does not reconcile to counterfactual equity")
    breakdowns = {
        "asset": summaries.by_asset,
        "month": summaries.by_month,
        "regime": summaries.by_regime,
        "size": summaries.by_size_bucket,
    }
    diagnostics = {
        **result.diagnostics,
        "best_trade_removal_fraction": fraction,
        "removed_trade_ids": selected["trade_id"].tolist(),
        "removed_trade_count": count,
        "removed_fill_count": int(removed_fill_mask.sum()),
        "best_trade_counterfactual": "sequential_self_financed",
        "attribution_reconciled": True,
        "orders": len(fills),
        "missed_orders": int(fills["status"].isin(["NO_FILL", "IOC_NO_FILL", "EXPIRED"]).sum())
        if not fills.empty and "status" in fills
        else 0,
        "partial_fills": int(fills["status"].isin(["PARTIAL", "IOC_PARTIAL"]).sum())
        if not fills.empty and "status" in fills
        else 0,
        "emergency_ioc_attempts": int(fills["order_type"].eq("ioc").sum())
        if not fills.empty and "order_type" in fills
        else 0,
    }
    return replace(
        result,
        returns=returns,
        equity=equity,
        metrics=metrics,
        diagnostics=diagnostics,
        attribution=attribution,
        weights=counterfactual_weights,
        fills=fills,
        breakdowns=breakdowns,
    )


def run_stress_matrix(
    *,
    panel: MarketPanel,
    output: StrategyOutput,
    costs: CostModel | CostSchedule,
    risk_limits: RiskLimits,
    base_execution: ExecutionConfig,
    scenarios: tuple[StressScenario, ...],
    benchmark: PassiveBenchmarkSpec | None = None,
) -> dict[str, BacktestResult]:
    if not scenarios:
        raise ValueError("at least one stress scenario is required")
    if len({scenario.name for scenario in scenarios}) != len(scenarios):
        raise ValueError("stress scenario names must be unique")
    results: dict[str, BacktestResult] = {}
    for scenario in scenarios:
        stressed_panel = panel
        if scenario.funding_multiplier != 1.0:
            stressed_panel = replace(
                panel,
                funding=panel.funding * scenario.funding_multiplier,
                metadata={
                    **panel.metadata,
                    "funding_stress_multiplier": scenario.funding_multiplier,
                },
            )
        engine = PanelBacktester(
            costs=costs,
            risk_limits=risk_limits,
            execution=execution_for_scenario(base_execution, scenario),
            benchmark=benchmark,
        )
        result = engine.run(stressed_panel, output)
        if scenario.remove_best_trade_fraction > 0.0:
            result = remove_best_trades(result, scenario.remove_best_trade_fraction)
        result.diagnostics["stress_scenario"] = scenario.name
        results[scenario.name] = result
    return results


def default_stress_scenarios() -> tuple[StressScenario, ...]:
    return (
        StressScenario("base"),
        StressScenario("costs_x2", cost_multiplier=2.0),
        StressScenario("maker_fill_degraded", maker_fill_multiplier=0.5),
        StressScenario("latency_degraded", added_latency_bars=1),
        StressScenario("remove_best_5pct", remove_best_trade_fraction=0.05),
    )
