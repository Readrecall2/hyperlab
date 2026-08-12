from __future__ import annotations

import html
import json
import math
from dataclasses import asdict, dataclass, replace
from itertools import combinations
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from hyperlab.backtest.engine import PanelBacktester
from hyperlab.models import BacktestResult, MarketPanel, StrategyOutput
from hyperlab.strategies.pairs import HedgeMethod, PairModel, RobustPairsStrategy


@dataclass(frozen=True, slots=True)
class PairSelectionConfig:
    maximum_pairs: int = 3
    minimum_train_bars: int = 90 * 24
    minimum_validation_bars: int = 30 * 24
    minimum_overlap_fraction: float = 0.95
    minimum_return_correlation: float = 0.55
    maximum_half_life_bars: float = 30 * 24.0
    maximum_beta_instability: float = 0.50
    lookback_bars: int = 30 * 24
    model_methods: tuple[HedgeMethod, ...] = ("rolling", "kalman", "cointegration")

    def __post_init__(self) -> None:
        if self.maximum_pairs < 1:
            raise ValueError("maximum_pairs must be positive")
        if self.minimum_train_bars < 24 or self.minimum_validation_bars < 12:
            raise ValueError("train and validation samples are too short")
        if not 0.0 < self.minimum_overlap_fraction <= 1.0:
            raise ValueError("minimum_overlap_fraction must be in (0, 1]")
        if not -1.0 <= self.minimum_return_correlation <= 1.0:
            raise ValueError("minimum_return_correlation must be in [-1, 1]")
        if self.maximum_half_life_bars <= 0.0 or self.maximum_beta_instability < 0.0:
            raise ValueError("stability thresholds must be non-negative")
        if self.lookback_bars < 12:
            raise ValueError("lookback_bars must be at least 12")
        if not self.model_methods or len(set(self.model_methods)) != len(self.model_methods):
            raise ValueError("model_methods must be non-empty and unique")


@dataclass(frozen=True, slots=True)
class PairsGateConfig:
    train_fraction: float = 0.60
    validation_fraction: float = 0.20
    minimum_stressed_return: float = 0.0
    correlation_break_strength: float = 2.0

    def __post_init__(self) -> None:
        if not 0.0 < self.train_fraction < 1.0 or not 0.0 < self.validation_fraction < 1.0:
            raise ValueError("split fractions must be in (0, 1)")
        if self.train_fraction + self.validation_fraction >= 1.0:
            raise ValueError("train and validation must leave a final test")
        if not math.isfinite(self.minimum_stressed_return) or self.minimum_stressed_return < -1.0:
            raise ValueError("minimum_stressed_return must be finite and at least -1")
        if not math.isfinite(self.correlation_break_strength) or self.correlation_break_strength <= 0.0:
            raise ValueError("correlation_break_strength must be positive")


@dataclass(frozen=True, slots=True)
class PairCandidate:
    asset_a: str
    asset_b: str
    return_correlation: float
    hedge_ratio: float
    intercept: float
    half_life_bars: float
    beta_instability: float
    train_score: float

    @property
    def pair_id(self) -> str:
        return f"{self.asset_a}|{self.asset_b}"


@dataclass(frozen=True, slots=True)
class PairSelection:
    selected_pairs: tuple[PairModel, ...]
    train_ranked_candidates: tuple[PairCandidate, ...]
    rejected_reasons: tuple[tuple[str, str], ...]
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    validation_start: pd.Timestamp
    selection_end: pd.Timestamp


@dataclass(frozen=True, slots=True)
class PairsDataAudit:
    observed_hours: int
    minimum_history_hours: int
    assets: tuple[str, ...]
    minimum_assets: int
    delisted_assets: tuple[str, ...]
    checks: dict[str, bool]
    reasons: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return all(self.checks.values())


@dataclass(slots=True)
class PairsValidation:
    audit: PairsDataAudit
    selection: PairSelection
    scenarios: dict[str, BacktestResult]
    gate_checks: dict[str, bool]
    removed_pair: str | None
    status: str


def _perp_assets(panel: MarketPanel) -> tuple[str, ...]:
    return tuple(
        sorted(
            str(column)
            for column in panel.prices.columns
            if str(column).startswith("HL:") and str(column).endswith(":perp")
        )
    )


def _delisted_assets(panel: MarketPanel, assets: tuple[str, ...]) -> tuple[str, ...]:
    if panel.tradable is None:
        return ()
    declared = panel.metadata.get("delisted_assets")
    if not isinstance(declared, (list, tuple)):
        return ()
    names = {str(value).upper() for value in declared if isinstance(value, str)}
    result = []
    for instrument in assets:
        asset = instrument.split(":")[1]
        lifecycle = panel.tradable[instrument]
        final = lifecycle.iloc[-1]
        if asset in names and bool(lifecycle.eq(True).any()) and (pd.isna(final) or not bool(final)):
            result.append(asset)
    return tuple(result)


def audit_pairs_panel(
    panel: MarketPanel,
    *,
    minimum_history_hours: int = 180 * 24,
    minimum_assets: int = 6,
) -> PairsDataAudit:
    """Fail closed unless lifecycle, funding and liquidity history are point-in-time."""

    panel.validate()
    if minimum_history_hours < 24 or minimum_assets < 4:
        raise ValueError("pairs audit requires at least 24 hours and four assets")
    index = pd.DatetimeIndex(panel.prices.index)
    hourly = len(index) >= 2 and bool(
        index.to_series().diff().dropna().eq(pd.Timedelta(hours=1)).all()
    )
    observed_hours = len(index) if hourly else 0
    assets = _perp_assets(panel)
    delisted = _delisted_assets(panel, assets)
    source = str(panel.metadata.get("source", "")).casefold()
    evidence = panel.metadata.get("calibration_evidence_hash")
    evidence_ok = isinstance(evidence, str) and len(evidence) == 64 and all(
        character in "0123456789abcdefABCDEF" for character in evidence
    )
    lifecycle_hash = panel.metadata.get("lifecycle_hash")
    lifecycle_ok = (
        isinstance(panel.metadata.get("historical_universe_source"), str)
        and isinstance(lifecycle_hash, str)
        and len(lifecycle_hash) == 64
    )
    active = {
        instrument: (
            panel.tradable[instrument].eq(True)
            if panel.tradable is not None
            else panel.prices[instrument].notna()
        )
        for instrument in assets
    }
    funding_observed = bool(assets) and all(
        bool(mask.any()) and not bool(panel.funding.loc[mask, instrument].isna().any())
        for instrument, mask in active.items()
    )
    liquidity_observed = panel.depth_usd is not None and all(
        not bool(panel.depth_usd.loc[mask, instrument].isna().any())
        and not bool(panel.volume_usd.loc[mask, instrument].isna().any())
        for instrument, mask in active.items()
    )
    checks = {
        "hourly_regular": observed_hours > 0,
        "minimum_history": observed_hours >= minimum_history_hours,
        "minimum_cross_section": len(assets) >= minimum_assets,
        "real_not_synthetic": bool(source) and "synthetic" not in source,
        "point_in_time": panel.metadata.get("point_in_time") is True,
        "point_in_time_matrices": all(
            value is not None for value in (panel.available_at, panel.finality, panel.tradable)
        ),
        "calibrated_data": (
            str(panel.metadata.get("calibration_status", "")).upper() == "CALIBRATED"
            and evidence_ok
        ),
        "historical_lifecycle_universe": lifecycle_ok and panel.tradable is not None,
        "delisted_markets_included": bool(delisted),
        "realized_hourly_funding": panel.metadata.get("funding_semantics") == "realized_hourly",
        "funding_observed": funding_observed,
        "liquidity_observed": liquidity_observed,
    }
    messages = {
        "hourly_regular": "La grille UTC n'est pas horaire, régulière et complète.",
        "minimum_history": f"Couverture insuffisante : {observed_hours} h, {minimum_history_hours} h requises.",
        "minimum_cross_section": f"Univers insuffisant : {len(assets)} actifs, {minimum_assets} requis.",
        "real_not_synthetic": "La provenance réelle versionnée n'est pas établie.",
        "point_in_time": "Le panel n'est pas déclaré point-in-time.",
        "point_in_time_matrices": "Disponibilité, finalité ou tradabilité point-in-time manque.",
        "calibrated_data": "La calibration des données n'est pas vérifiable.",
        "historical_lifecycle_universe": "La source et le hash de lifecycle historique manquent.",
        "delisted_markets_included": "Aucun marché délisté n'est conservé dans l'univers.",
        "realized_hourly_funding": "Le funding n'est pas déclaré paiement horaire réalisé.",
        "funding_observed": "Le funding historique manque pendant une période tradable.",
        "liquidity_observed": "Volume ou profondeur manque pendant une période tradable.",
    }
    return PairsDataAudit(
        observed_hours=observed_hours,
        minimum_history_hours=minimum_history_hours,
        assets=assets,
        minimum_assets=minimum_assets,
        delisted_assets=delisted,
        checks=checks,
        reasons=tuple(messages[name] for name, passed in checks.items() if not passed),
    )


def _slice_panel(panel: MarketPanel, index: pd.DatetimeIndex) -> MarketPanel:
    return MarketPanel(
        prices=panel.prices.loc[index].copy(),
        funding=panel.funding.loc[index].copy(),
        spreads_bps=panel.spreads_bps.loc[index].copy(),
        volume_usd=panel.volume_usd.loc[index].copy(),
        metadata={**panel.metadata, "research_slice": (index[0].isoformat(), index[-1].isoformat())},
        depth_usd=panel.depth_usd.loc[index].copy() if panel.depth_usd is not None else None,
        open_interest_usd=(
            panel.open_interest_usd.loc[index].copy() if panel.open_interest_usd is not None else None
        ),
        liquidation_usd=(
            panel.liquidation_usd.loc[index].copy() if panel.liquidation_usd is not None else None
        ),
        available_at=panel.available_at.loc[index].copy() if panel.available_at is not None else None,
        finality=panel.finality.loc[index].copy() if panel.finality is not None else None,
        tradable=panel.tradable.loc[index].copy() if panel.tradable is not None else None,
        regimes=panel.regimes.loc[index].copy() if panel.regimes is not None else None,
    )


def _ols(log_a: pd.Series, log_b: pd.Series) -> tuple[float, float]:
    frame = pd.concat([log_a.rename("a"), log_b.rename("b")], axis=1).dropna()
    design = np.column_stack([np.ones(len(frame)), frame["b"].to_numpy()])
    intercept, beta = np.linalg.lstsq(design, frame["a"].to_numpy(), rcond=None)[0]
    return float(intercept), float(beta)


def _half_life(spread: pd.Series) -> float:
    values = spread.dropna()
    lagged = values.shift(1)
    delta = values.diff()
    frame = pd.concat([delta.rename("delta"), lagged.rename("lagged")], axis=1).dropna()
    if len(frame) < 12:
        return math.inf
    slope = float(np.linalg.lstsq(frame[["lagged"]].to_numpy(), frame["delta"].to_numpy(), rcond=None)[0][0])
    if slope >= 0.0:
        return math.inf
    return float(-math.log(2.0) / slope)


def _candidate(
    prices: pd.DataFrame,
    asset_a: str,
    asset_b: str,
    config: PairSelectionConfig,
) -> tuple[PairCandidate | None, str | None]:
    valid = prices[[asset_a, asset_b]].notna().all(axis=1)
    overlap = float(valid.mean())
    if overlap < config.minimum_overlap_fraction:
        return None, "insufficient_overlap"
    sample = prices.loc[valid, [asset_a, asset_b]]
    logged = sample.astype(float).map(math.log)
    returns = logged.diff().dropna()
    correlation = float(returns[asset_a].corr(returns[asset_b]))
    if not math.isfinite(correlation) or correlation < config.minimum_return_correlation:
        return None, "weak_train_correlation"
    log_a = logged[asset_a]
    log_b = logged[asset_b]
    intercept, beta = _ols(log_a, log_b)
    if not math.isfinite(beta) or beta <= 0.0:
        return None, "invalid_train_hedge_ratio"
    spread = log_a - intercept - beta * log_b
    half_life = _half_life(spread)
    if not math.isfinite(half_life) or half_life > config.maximum_half_life_bars:
        return None, "unstable_or_non_mean_reverting_train_spread"
    midpoint = len(sample) // 2
    _, beta_first = _ols(log_a.iloc[:midpoint], log_b.iloc[:midpoint])
    _, beta_second = _ols(log_a.iloc[midpoint:], log_b.iloc[midpoint:])
    instability = abs(beta_second - beta_first) / max(abs(beta), 1e-12)
    if instability > config.maximum_beta_instability:
        return None, "train_hedge_ratio_break"
    score = correlation / (1.0 + half_life) - 0.10 * instability
    return (
        PairCandidate(
            asset_a=asset_a,
            asset_b=asset_b,
            return_correlation=correlation,
            hedge_ratio=beta,
            intercept=intercept,
            half_life_bars=half_life,
            beta_instability=instability,
            train_score=score,
        ),
        None,
    )


def _validation_score(
    panel: MarketPanel,
    output: StrategyOutput,
    validation_index: pd.DatetimeIndex,
) -> float:
    """Cheap causal validation objective used only to choose the hedge estimator.

    The full execution engine remains authoritative for the frozen final variant.
    Here spread, funding, observed half-spread and turnover are all included, while
    avoiding several full order simulations for every candidate estimator.
    """

    weights = output.weights.shift(1).loc[validation_index].fillna(0.0)
    price_returns = panel.prices.pct_change(fill_method=None).loc[validation_index]
    funding = panel.funding.loc[validation_index]
    gross = (weights * price_returns).sum(axis=1) - (weights * funding).sum(axis=1)
    changes = output.weights.diff().abs().loc[validation_index].fillna(0.0)
    half_spread = panel.spreads_bps.loc[validation_index] / 20_000.0
    observed_cost = (changes * half_spread).sum(axis=1)
    net = gross - observed_cost
    equity = (1.0 + net).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    turnover = float(changes.to_numpy().sum())
    return float(equity.iloc[-1] - 1.0 + drawdown.min() - 0.0001 * turnover)


def select_pairs_train_only(
    panel: MarketPanel,
    *,
    train_index: pd.DatetimeIndex,
    validation_index: pd.DatetimeIndex,
    config: PairSelectionConfig,
    engine: PanelBacktester,
) -> PairSelection:
    """Select pair identities on train, then select their hedge method on validation."""

    # The engine argument makes the selection API explicit about the execution
    # model frozen for the research run; full execution is intentionally deferred
    # until after variant selection.
    _ = engine
    if len(train_index) < config.minimum_train_bars or len(validation_index) < config.minimum_validation_bars:
        raise ValueError("train or validation segment is shorter than the predeclared minimum")
    if train_index[-1] >= validation_index[0]:
        raise ValueError("train must end before validation starts")
    raw_end = panel.prices.index.get_loc(validation_index[-1])
    if not isinstance(raw_end, int):
        raise ValueError("selection_end must identify exactly one panel row")
    allowed = pd.DatetimeIndex(panel.prices.index[: raw_end + 1])
    if not train_index.isin(allowed).all() or not validation_index.isin(allowed).all():
        raise ValueError("selection indices must belong to the panel")
    selection_panel = _slice_panel(panel, allowed)
    instruments = _perp_assets(selection_panel)
    candidates: list[PairCandidate] = []
    rejected: list[tuple[str, str]] = []
    train_prices = selection_panel.prices.loc[train_index].copy()
    if selection_panel.tradable is not None:
        train_prices = train_prices.where(selection_panel.tradable.loc[train_index].eq(True))
    for asset_a, asset_b in combinations(instruments, 2):
        candidate, reason = _candidate(train_prices, asset_a, asset_b, config)
        pair_id = f"{asset_a}|{asset_b}"
        if candidate is None:
            rejected.append((pair_id, str(reason)))
        else:
            candidates.append(candidate)
    ranked = sorted(candidates, key=lambda item: (-item.train_score, item.pair_id))
    chosen: list[PairCandidate] = []
    used: set[str] = set()
    for candidate in ranked:
        if candidate.asset_a in used or candidate.asset_b in used:
            continue
        chosen.append(candidate)
        used.update((candidate.asset_a, candidate.asset_b))
        if len(chosen) == config.maximum_pairs:
            break
    models: list[PairModel] = []
    for candidate in chosen:
        scored: list[tuple[float, str, PairModel]] = []
        for method in config.model_methods:
            model = PairModel(
                asset_a=candidate.asset_a,
                asset_b=candidate.asset_b,
                method=method,
                hedge_ratio=candidate.hedge_ratio,
                intercept=candidate.intercept,
                lookback_bars=config.lookback_bars,
                train_score=candidate.train_score,
                validation_score=0.0,
            )
            output = RobustPairsStrategy(
                models=(model,),
                trade_start=validation_index[0],
                maximum_pair_gross=0.50,
                volatility_lookback_bars=min(72, config.lookback_bars),
            ).generate(selection_panel)
            score = _validation_score(selection_panel, output, validation_index)
            scored.append((score, method, replace(model, validation_score=score)))
        scored.sort(key=lambda item: (-item[0], item[1]))
        models.append(scored[0][2])
    return PairSelection(
        selected_pairs=tuple(models),
        train_ranked_candidates=tuple(ranked),
        rejected_reasons=tuple(sorted(rejected)),
        train_start=train_index[0],
        train_end=train_index[-1],
        validation_start=validation_index[0],
        selection_end=validation_index[-1],
    )


def _zero_output(panel: MarketPanel, *, name: str) -> StrategyOutput:
    return StrategyOutput(
        name=name,
        risk_tier="3 — offensif",
        weights=pd.DataFrame(0.0, index=panel.prices.index, columns=panel.prices.columns),
        diagnostics={"method": "no_pairs_after_preregistered_removal"},
    )


def _break_selected_pair_correlations(
    panel: MarketPanel,
    models: tuple[PairModel, ...],
    *,
    start: pd.Timestamp,
    strength: float,
) -> MarketPanel:
    prices = panel.prices.copy(deep=True)
    stressed_index = prices.index[prices.index >= start]
    for ordinal, model in enumerate(models):
        returns_a = prices[model.asset_a].pct_change(fill_method=None).loc[stressed_index].fillna(0.0)
        direction = -1.0 if ordinal % 2 == 0 else 1.0
        shocked_returns = (-strength * returns_a + direction * 0.002).clip(lower=-0.90)
        raw_start = prices.index.get_loc(start)
        if not isinstance(raw_start, int):
            raise ValueError("correlation break start must identify exactly one row")
        prior = raw_start - 1
        anchor = float(prices.iloc[max(prior, 0)][model.asset_b])
        prices.loc[stressed_index, model.asset_b] = anchor * (1.0 + shocked_returns).cumprod()
    return replace(
        panel,
        prices=prices,
        metadata={
            **panel.metadata,
            "calibration_status": "SYNTHETIC",
            "stress_parent_calibration_status": panel.metadata.get("calibration_status"),
            "stress_data_warning": "deterministic selected-pair correlation break",
            "correlation_break_strength": strength,
        },
    )


def run_pairs_validation(
    panel: MarketPanel,
    *,
    engine: PanelBacktester,
    selection_config: PairSelectionConfig | None = None,
    gate_config: PairsGateConfig | None = None,
    audit: PairsDataAudit | None = None,
) -> PairsValidation:
    """Freeze selection before final test and run the two mandatory Phase-08 gates."""

    selection_config = selection_config or PairSelectionConfig()
    gate_config = gate_config or PairsGateConfig()
    audit = audit or audit_pairs_panel(panel)
    engine.validate_research_panel(panel)
    count = len(panel.prices)
    train_end = int(count * gate_config.train_fraction)
    validation_end = int(count * (gate_config.train_fraction + gate_config.validation_fraction))
    train_index = pd.DatetimeIndex(panel.prices.index[:train_end])
    validation_index = pd.DatetimeIndex(panel.prices.index[train_end:validation_end])
    final_start = panel.prices.index[validation_end]
    selection = select_pairs_train_only(
        panel,
        train_index=train_index,
        validation_index=validation_index,
        config=selection_config,
        engine=engine,
    )
    if not selection.selected_pairs:
        raise ValueError("no pair survived the preregistered train-only stability filters")
    base_strategy = RobustPairsStrategy(models=selection.selected_pairs, trade_start=final_start)
    base = engine.run(panel, base_strategy.generate(panel))
    removed = max(
        selection.selected_pairs,
        key=lambda model: (model.validation_score, model.pair_id),
    )
    remaining = tuple(model for model in selection.selected_pairs if model != removed)
    removal_output = (
        RobustPairsStrategy(models=remaining, trade_start=final_start).generate(panel)
        if remaining
        else _zero_output(panel, name="pairs_remove_best_pair")
    )
    removed_result = engine.run(panel, removal_output)
    stressed_panel = _break_selected_pair_correlations(
        panel,
        selection.selected_pairs,
        start=final_start,
        strength=gate_config.correlation_break_strength,
    )
    stressed = engine.run(stressed_panel, base_strategy.generate(stressed_panel))
    scenarios = {
        "base_final_test": base,
        "remove_best_pair": removed_result,
        "correlation_break": stressed,
    }
    gate_checks = {
        "multiple_pairs_selected": len(selection.selected_pairs) >= 2,
        "remove_best_pair_survives": (
            len(selection.selected_pairs) >= 2
            and removed_result.metrics.total_return >= gate_config.minimum_stressed_return
        ),
        "correlation_break_survives": stressed.metrics.total_return >= gate_config.minimum_stressed_return,
        "no_liquidation_or_nonfinite_equity": all(
            bool(np.isfinite(result.equity.to_numpy()).all()) and bool(result.equity.gt(0.0).all())
            for result in scenarios.values()
        ),
    }
    if not audit.checks.get("minimum_history", False):
        status = "BLOCKED_INSUFFICIENT_REAL_DATA"
    elif not audit.passed:
        status = "BLOCKED_UNCALIBRATED_OR_SURVIVORSHIP_BIAS"
    elif any(result.diagnostics.get("audit_status") != "CALIBRATED" for result in scenarios.values()):
        status = "BLOCKED_UNCALIBRATED_EXECUTION_MODEL"
    elif not all(gate_checks.values()):
        status = "REJECTED_ROBUSTNESS_GATE"
    else:
        status = "VALIDATED_RESEARCH_ONLY"
    return PairsValidation(
        audit=audit,
        selection=selection,
        scenarios=scenarios,
        gate_checks=gate_checks,
        removed_pair=removed.pair_id,
        status=status,
    )


def _summary(result: BacktestResult) -> dict[str, object]:
    return {
        "total_return": result.metrics.total_return,
        "max_drawdown": result.metrics.max_drawdown,
        "turnover": result.metrics.turnover,
        "funding_return": result.metrics.funding_contribution,
        "cost_return": result.metrics.cost_contribution,
        "price_return": result.metrics.price_contribution,
        "fill_rate": result.metrics.fill_rate,
        "data_status": result.diagnostics.get("data_status"),
    }


def _json_selection(selection: PairSelection) -> dict[str, object]:
    return {
        "selected_pairs": [asdict(model) for model in selection.selected_pairs],
        "train_ranked_candidates": [asdict(candidate) for candidate in selection.train_ranked_candidates],
        "rejected_reasons": [list(value) for value in selection.rejected_reasons],
        "train_start": selection.train_start.isoformat(),
        "train_end": selection.train_end.isoformat(),
        "validation_start": selection.validation_start.isoformat(),
        "selection_end": selection.selection_end.isoformat(),
        "final_test_exposed_to_selection": False,
    }


def write_pairs_report(validation: PairsValidation, *, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    summaries = {name: _summary(result) for name, result in validation.scenarios.items()}
    payload = {
        "schema_version": 1,
        "status": validation.status,
        "data_audit": asdict(validation.audit),
        "selection": _json_selection(validation.selection),
        "removed_pair": validation.removed_pair,
        "gate_checks": validation.gate_checks,
        "scenarios": summaries,
        "constraints": {"martingale": False, "unbounded_averaging_down": False},
    }
    summary_path = output_dir / "pairs_trading_summary.json"
    temporary = summary_path.with_name(f".{summary_path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(summary_path)
    rows = "".join(
        "<tr>"
        f"<td>{html.escape(name)}</td>"
        f"<td>{float(cast(Any, summary['total_return'])) * 100:.2f} %</td>"
        f"<td>{float(cast(Any, summary['max_drawdown'])) * 100:.2f} %</td>"
        f"<td>{float(cast(Any, summary['funding_return'])) * 100:.3f} %</td>"
        f"<td>{float(cast(Any, summary['cost_return'])) * 100:.3f} %</td>"
        f"<td>{float(cast(Any, summary['turnover'])):.3f}</td>"
        "</tr>"
        for name, summary in summaries.items()
    )
    selected = "".join(
        f"<li>{html.escape(model.pair_id)} — {html.escape(model.method)} — score validation {model.validation_score:.6f}</li>"
        for model in validation.selection.selected_pairs
    )
    reasons = "".join(f"<li>{html.escape(reason)}</li>" for reason in validation.audit.reasons)
    document = f"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><title>HyperLab Phase 08 — pairs trading</title>
<style>body{{font-family:Segoe UI,sans-serif;max-width:1200px;margin:auto;padding:32px;background:#091321;color:#edf5ff}}table{{width:100%;border-collapse:collapse;margin:16px 0}}th,td{{padding:9px;border:1px solid #29405d;text-align:right}}th:first-child,td:first-child{{text-align:left}}section{{padding:16px;border:1px solid #496786;border-radius:10px;margin:16px 0}}</style></head>
<body><h1>Phase 08 — pairs trading robuste</h1>
<section><strong>{html.escape(validation.status)}</strong><ul>{reasons}</ul></section>
<h2>Sélection train uniquement</h2><ul>{selected}</ul>
<p>Le modèle est choisi sur validation, puis la paire et ses paramètres sont gelés avant le test final.</p>
<h2>Gate de robustesse</h2>
<p>Retrait de la meilleure paire (déterminée sur validation) : {validation.gate_checks['remove_best_pair_survives']}.</p>
<p>Rupture de corrélation simulée : {validation.gate_checks['correlation_break_survives']}.</p>
<table><thead><tr><th>Scénario</th><th>Rendement net</th><th>Drawdown</th><th>Funding</th><th>Coûts</th><th>Turnover</th></tr></thead><tbody>{rows}</tbody></table>
<p>Sizing par volatilité du spread, stop de spread, time stop et cooldown. Aucune martingale ni moyenne à la baisse non bornée. Les ruptures simulées sont explicitement SYNTHETIC.</p>
</body></html>"""
    report_path = output_dir / "pairs_trading_report.html"
    report_path.write_text(document, encoding="utf-8")
    return report_path


__all__ = [
    "PairCandidate",
    "PairSelection",
    "PairSelectionConfig",
    "PairsDataAudit",
    "PairsGateConfig",
    "PairsValidation",
    "audit_pairs_panel",
    "run_pairs_validation",
    "select_pairs_train_only",
    "write_pairs_report",
]
