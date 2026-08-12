from __future__ import annotations

import html
import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, cast

import pandas as pd

from hyperlab.backtest.engine import PanelBacktester
from hyperlab.backtest.stress import StressScenario, run_stress_matrix
from hyperlab.models import BacktestResult, MarketPanel
from hyperlab.strategies.funding_basket import FundingBasketStrategy


@dataclass(frozen=True, slots=True)
class FundingBasketDataAudit:
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
class FundingBasketValidation:
    audit: FundingBasketDataAudit
    comparison: dict[str, BacktestResult]
    stresses: dict[str, BacktestResult]
    leave_one_out: dict[str, BacktestResult]
    status: str


def funding_basket_stress_scenarios() -> tuple[StressScenario, ...]:
    """Predeclared Phase-06 stress set, including joint cross-sectional failures."""

    return (
        StressScenario("base"),
        StressScenario("costs_x2", cost_multiplier=2.0),
        StressScenario("maker_fill_degraded", maker_fill_multiplier=0.5),
        StressScenario("latency_degraded", added_latency_bars=1),
        StressScenario("broken_correlation", correlation_break_strength=1.5),
        StressScenario("simultaneous_short_squeeze", simultaneous_short_squeeze_return=0.20),
        StressScenario("remove_best_5pct", remove_best_trade_fraction=0.05),
    )


def _is_hourly(index: pd.DatetimeIndex) -> bool:
    if len(index) < 2:
        return False
    return bool(index.to_series().diff().dropna().eq(pd.Timedelta(hours=1)).all())


def _basket_assets(panel: MarketPanel) -> tuple[str, ...]:
    assets: list[str] = []
    for column in panel.prices.columns:
        parts = str(column).split(":")
        if len(parts) == 3 and parts[0] == "HL" and parts[2] == "perp":
            assets.append(parts[1])
    return tuple(sorted(set(assets)))


def _delisted_assets(panel: MarketPanel, assets: tuple[str, ...]) -> tuple[str, ...]:
    if panel.tradable is None:
        return ()
    declared = panel.metadata.get("delisted_assets")
    if not isinstance(declared, (list, tuple)) or not all(
        isinstance(value, str) and value.strip() for value in declared
    ):
        return ()
    declared_assets = {str(value).upper() for value in declared}
    result: list[str] = []
    for asset in assets:
        if asset.upper() not in declared_assets:
            continue
        column = f"HL:{asset}:perp"
        lifecycle = panel.tradable[column]
        final_value = lifecycle.iloc[-1]
        final_tradable = not pd.isna(final_value) and bool(final_value)
        if bool(lifecycle.eq(True).any()) and not final_tradable:
            result.append(asset)
    return tuple(result)


def audit_funding_basket_panel(
    panel: MarketPanel,
    *,
    minimum_history_hours: int = 90 * 24,
    minimum_assets: int = 6,
) -> FundingBasketDataAudit:
    """Fail closed when point-in-time lifecycle or delisted markets are absent."""

    panel.validate()
    if minimum_history_hours < 24:
        raise ValueError("minimum_history_hours must be at least 24")
    if minimum_assets < 4:
        raise ValueError("minimum_assets must be at least 4 for three neutralities")
    index = pd.DatetimeIndex(panel.prices.index)
    observed_hours = len(index) if _is_hourly(index) else 0
    assets = _basket_assets(panel)
    delisted = _delisted_assets(panel, assets)
    source = str(panel.metadata.get("source", "")).casefold()
    evidence = panel.metadata.get("calibration_evidence_hash")
    evidence_ok = (
        isinstance(evidence, str)
        and len(evidence) == 64
        and all(character in "0123456789abcdef" for character in evidence.casefold())
    )
    lifecycle_hash = panel.metadata.get("lifecycle_hash")
    lifecycle_ok = (
        isinstance(panel.metadata.get("historical_universe_source"), str)
        and isinstance(lifecycle_hash, str)
        and len(lifecycle_hash) == 64
    )
    perp_columns = [f"HL:{asset}:perp" for asset in assets]
    active_by_column = {
        column: (
            panel.tradable[column].eq(True)
            if panel.tradable is not None
            else panel.prices[column].notna()
        )
        for column in perp_columns
    }
    funding_observed = bool(perp_columns) and all(
        not bool(panel.funding.loc[active_by_column[column], column].isna().any())
        and bool(active_by_column[column].any())
        for column in perp_columns
    )
    liquidity_observed = panel.depth_usd is not None and all(
        not bool(panel.volume_usd.loc[active_by_column[column], column].isna().any())
        and not bool(panel.depth_usd.loc[active_by_column[column], column].isna().any())
        for column in perp_columns
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
        "realized_hourly_funding": panel.metadata.get("funding_semantics") == "realized_hourly",
        "calibrated_data": (
            str(panel.metadata.get("calibration_status", "")).upper() == "CALIBRATED"
            and evidence_ok
        ),
        "historical_lifecycle_universe": lifecycle_ok and panel.tradable is not None,
        "delisted_markets_included": bool(delisted),
        "funding_observed": funding_observed,
        "liquidity_observed": liquidity_observed,
    }
    reason_by_check = {
        "hourly_regular": "La grille UTC n'est pas horaire, régulière et complète.",
        "minimum_history": (
            f"Couverture insuffisante : {observed_hours} h observées, "
            f"{minimum_history_hours} h requises."
        ),
        "minimum_cross_section": (
            f"Cross-section insuffisante : {len(assets)} actifs, {minimum_assets} requis."
        ),
        "real_not_synthetic": "La provenance réelle versionnée n'est pas établie.",
        "point_in_time": "Les observations ne sont pas déclarées point-in-time.",
        "point_in_time_matrices": "Disponibilité, finalité ou tradabilité point-in-time manque.",
        "realized_hourly_funding": "Le funding n'est pas un paiement horaire réalisé.",
        "calibrated_data": "La calibration des données n'est pas vérifiable.",
        "historical_lifecycle_universe": "L'univers historique et son hash lifecycle manquent.",
        "delisted_markets_included": (
            "Aucun marché historiquement tradable puis délisté n'est conservé dans le panel."
        ),
        "funding_observed": "Le funding historique manque pour au moins un perp.",
        "liquidity_observed": "Volume ou profondeur manque pour au moins un perp.",
    }
    reasons = tuple(reason_by_check[name] for name, passed in checks.items() if not passed)
    return FundingBasketDataAudit(
        observed_hours=observed_hours,
        minimum_history_hours=minimum_history_hours,
        assets=assets,
        minimum_assets=minimum_assets,
        delisted_assets=delisted,
        checks=checks,
        reasons=reasons,
    )


def run_funding_basket_validation(
    panel: MarketPanel,
    *,
    strategy: FundingBasketStrategy,
    engine: PanelBacktester,
    audit: FundingBasketDataAudit | None = None,
) -> FundingBasketValidation:
    """Compare baseline/optimizer, run fixed-weight stresses and leave-one-out."""

    resolved_audit = audit or audit_funding_basket_panel(panel)
    optimized_strategy = replace(strategy, mode="optimized")
    ranking_strategy = replace(strategy, mode="ranking")
    optimized_output = optimized_strategy.generate(panel)
    comparison = {
        "ranking": engine.run(panel, ranking_strategy.generate(panel)),
        "optimized": engine.run(panel, optimized_output),
    }
    stresses = run_stress_matrix(
        panel=panel,
        output=optimized_output,
        costs=engine.costs,
        risk_limits=engine.risk_limits,
        base_execution=engine.execution,
        scenarios=funding_basket_stress_scenarios(),
        benchmark=engine.benchmark_spec,
    )
    leave_one_out: dict[str, BacktestResult] = {}
    for asset in resolved_audit.assets:
        excluded = tuple(sorted({*optimized_strategy.excluded_assets, asset}))
        candidate = replace(optimized_strategy, excluded_assets=excluded)
        leave_one_out[asset] = engine.run(panel, candidate.generate(panel))
    if not resolved_audit.checks.get("minimum_history", False):
        status = "BLOCKED_INSUFFICIENT_REAL_DATA"
    elif not resolved_audit.passed:
        status = "BLOCKED_UNCALIBRATED_OR_SURVIVORSHIP_BIAS"
    elif any(
        result.diagnostics.get("audit_status") != "CALIBRATED"
        for result in comparison.values()
    ):
        status = "BLOCKED_UNCALIBRATED_EXECUTION_MODEL"
    else:
        status = "VALIDATED_RESEARCH_ONLY"
    return FundingBasketValidation(
        audit=resolved_audit,
        comparison=comparison,
        stresses=stresses,
        leave_one_out=leave_one_out,
        status=status,
    )


def _component_pnl(result: BacktestResult, component: str) -> float:
    column = f"{component}_pnl"
    if column not in result.attribution:
        return 0.0
    return float(cast(Any, result.attribution[column].sum()))


def _summary(result: BacktestResult) -> dict[str, object]:
    return {
        "method": str(result.diagnostics.get("method", "fixed_stress_path")),
        "total_return": result.metrics.total_return,
        "funding_return": result.metrics.funding_contribution,
        "relative_performance_return": result.metrics.price_contribution,
        "funding_pnl_usd": _component_pnl(result, "funding"),
        "relative_performance_pnl_usd": _component_pnl(result, "price"),
        "cost_return": result.metrics.cost_contribution,
        "max_drawdown": result.metrics.max_drawdown,
        "turnover": result.metrics.turnover,
        "max_gross_leverage": result.metrics.max_gross_leverage,
        "max_net_exposure": result.metrics.max_net_exposure,
        "max_abs_btc_beta_exposure": result.diagnostics.get("max_abs_btc_beta_exposure"),
        "max_abs_eth_beta_exposure": result.diagnostics.get("max_abs_eth_beta_exposure"),
    }


def _percent(value: object) -> str:
    return f"{float(cast(Any, value)) * 100:.2f} %"


def _money(value: object) -> str:
    return f"{float(cast(Any, value)):,.2f} USD".replace(",", " ")


def write_funding_basket_report(
    validation: FundingBasketValidation,
    *,
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    comparison = {name: _summary(result) for name, result in validation.comparison.items()}
    stresses = {name: _summary(result) for name, result in validation.stresses.items()}
    leave_one_out = {name: _summary(result) for name, result in validation.leave_one_out.items()}
    payload = {
        "schema_version": 1,
        "status": validation.status,
        "data_audit": asdict(validation.audit),
        "comparison": comparison,
        "stresses": stresses,
        "leave_one_out": leave_one_out,
        "pnl_identity": (
            "net = relative_performance + funding + basis + spread + fees + slippage + hedge"
        ),
    }
    summary_path = output_dir / "funding_basket_summary.json"
    temporary = summary_path.with_name(f".{summary_path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(summary_path)

    comparison_rows = "".join(
        "<tr>"
        f"<td>{html.escape(name)}</td>"
        f"<td>{_percent(summary['total_return'])}</td>"
        f"<td>{_money(summary['funding_pnl_usd'])}</td>"
        f"<td>{_money(summary['relative_performance_pnl_usd'])}</td>"
        f"<td>{_percent(summary['max_drawdown'])}</td>"
        f"<td>{float(cast(Any, summary['turnover'])):.3f}</td>"
        "</tr>"
        for name, summary in comparison.items()
    )
    exclusion_rows = "".join(
        f"<tr><td>{html.escape(asset)}</td><td>{_percent(summary['total_return'])}</td>"
        f"<td>{_percent(summary['max_drawdown'])}</td></tr>"
        for asset, summary in leave_one_out.items()
    )
    reasons = "".join(f"<li>{html.escape(reason)}</li>" for reason in validation.audit.reasons)
    broken = stresses.get("broken_correlation", {})
    squeeze = stresses.get("simultaneous_short_squeeze", {})
    document = f"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><title>HyperLab Phase 06 — basket de funding</title>
<style>body{{font-family:Segoe UI,sans-serif;max-width:1200px;margin:auto;padding:32px;background:#091321;color:#edf5ff}}table{{width:100%;border-collapse:collapse;margin:16px 0}}th,td{{padding:9px;border:1px solid #29405d;text-align:right}}th:first-child,td:first-child{{text-align:left}}section{{padding:16px;border:1px solid #496786;border-radius:10px;margin:16px 0}}</style></head>
<body><h1>Phase 06 — basket de funding Hyperliquid</h1>
<section><strong>{html.escape(validation.status)}</strong><ul>{reasons}</ul></section>
<p>Marchés délistés inclus : {html.escape(', '.join(validation.audit.delisted_assets) or 'AUCUN')}</p>
<h2>Ranking simple vs optimisation contrainte</h2>
<table><thead><tr><th>Méthode</th><th>Rendement net</th><th>Funding</th><th>Performance relative</th><th>Drawdown</th><th>Turnover</th></tr></thead><tbody>{comparison_rows}</tbody></table>
<h2>Stress dédiés</h2>
<p>Corrélation cassée : rendement {_percent(broken.get('total_return', 0.0))}, drawdown {_percent(broken.get('max_drawdown', 0.0))}.</p>
<p>Squeeze simultané : rendement {_percent(squeeze.get('total_return', 0.0))}, drawdown {_percent(squeeze.get('max_drawdown', 0.0))}.</p>
<h2>Exclusion un actif à la fois</h2>
<table><thead><tr><th>Actif exclu</th><th>Rendement net</th><th>Drawdown</th></tr></thead><tbody>{exclusion_rows}</tbody></table>
<p>Données et stress synthétiques/contre-factuels restent signalés par leur provenance. Recherche uniquement — aucune route d'ordre réel.</p></body></html>"""
    report_path = output_dir / "funding_basket_report.html"
    report_path.write_text(document, encoding="utf-8")
    return report_path


__all__ = [
    "FundingBasketDataAudit",
    "FundingBasketValidation",
    "audit_funding_basket_panel",
    "funding_basket_stress_scenarios",
    "run_funding_basket_validation",
    "write_funding_basket_report",
]
