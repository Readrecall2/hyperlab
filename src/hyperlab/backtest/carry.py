from __future__ import annotations

import html
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from hyperlab.backtest.stress import StressScenario
from hyperlab.models import BacktestResult, MarketPanel


@dataclass(frozen=True, slots=True)
class CarryDataAudit:
    observed_hours: int
    minimum_history_hours: int
    assets: tuple[str, ...]
    checks: dict[str, bool]
    reasons: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return all(self.checks.values())


@dataclass(frozen=True, slots=True)
class CarryGateSpec:
    minimum_stressed_excess_return: float = 0.0
    maximum_stressed_drawdown: float = 0.10
    require_complete_close: bool = True

    def __post_init__(self) -> None:
        if not math.isfinite(self.minimum_stressed_excess_return):
            raise ValueError("minimum_stressed_excess_return must be finite")
        if (
            not math.isfinite(self.maximum_stressed_drawdown)
            or not 0.0 <= self.maximum_stressed_drawdown <= 1.0
        ):
            raise ValueError("maximum_stressed_drawdown must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class CarryGateDecision:
    promote: bool
    status: str
    reasons: tuple[str, ...]
    minimum_stressed_excess_return: float | None
    maximum_stressed_drawdown: float | None
    required_minimum_stressed_excess_return: float
    allowed_maximum_stressed_drawdown: float


def carry_stress_scenarios() -> tuple[StressScenario, ...]:
    return (
        StressScenario("base"),
        StressScenario("costs_x2", cost_multiplier=2.0),
        StressScenario("maker_fill_degraded", maker_fill_multiplier=0.5),
        StressScenario("latency_degraded", added_latency_bars=1),
        StressScenario("funding_inversion", funding_multiplier=-1.0),
        StressScenario("remove_best_5pct", remove_best_trade_fraction=0.05),
    )


def _is_hourly(index: pd.DatetimeIndex) -> bool:
    if len(index) < 2:
        return False
    deltas = index.to_series().diff().dropna()
    return bool(deltas.eq(pd.Timedelta(hours=1)).all())


def _carry_assets(panel: MarketPanel) -> tuple[str, ...]:
    instruments = {str(column) for column in panel.prices.columns}
    assets: list[str] = []
    for instrument in sorted(instruments):
        parts = instrument.split(":")
        if len(parts) != 3 or parts[0] != "HL" or parts[2] != "perp":
            continue
        asset = parts[1]
        if f"HL:{asset}:spot" in instruments:
            assets.append(asset)
    return tuple(assets)


def audit_carry_panel(panel: MarketPanel, *, minimum_history_hours: int = 30 * 24) -> CarryDataAudit:
    """Fail-closed Gate-B audit for a real point-in-time carry panel."""

    panel.validate()
    if minimum_history_hours < 72:
        raise ValueError("minimum_history_hours cannot be shorter than the 72-hour signal")
    assets = _carry_assets(panel)
    panel_index = pd.DatetimeIndex(panel.prices.index)
    observed_hours = len(panel_index) if _is_hourly(panel_index) else 0
    source = str(panel.metadata.get("source", "")).casefold()
    calibration = str(panel.metadata.get("calibration_status", "UNCALIBRATED")).upper()
    evidence = panel.metadata.get("calibration_evidence_hash")
    evidence_ok = (
        isinstance(evidence, str)
        and len(evidence) == 64
        and all(character in "0123456789abcdef" for character in evidence)
    )
    depth_complete = panel.depth_usd is not None and all(
        not panel.depth_usd[[f"HL:{asset}:spot", f"HL:{asset}:perp"]].isna().any(axis=None)
        for asset in assets
    )
    oi_complete = panel.open_interest_usd is not None and all(
        not panel.open_interest_usd[f"HL:{asset}:perp"].isna().any() for asset in assets
    )
    funding_complete = bool(assets) and all(
        not panel.funding[f"HL:{asset}:perp"].isna().any() for asset in assets
    )
    checks = {
        "hourly_regular": observed_hours > 0,
        "minimum_history": observed_hours >= minimum_history_hours,
        "real_not_synthetic": "synthetic" not in source and bool(source),
        "point_in_time": panel.metadata.get("point_in_time") is True,
        "realized_hourly_funding": panel.metadata.get("funding_semantics") == "realized_hourly",
        "calibrated_data": calibration == "CALIBRATED" and evidence_ok,
        "verified_spot_perp_pairs": bool(assets),
        "funding_complete": funding_complete,
        "depth_complete": depth_complete,
        "open_interest_complete": oi_complete,
    }
    reason_by_check = {
        "hourly_regular": "La grille de décision n'est pas horaire, UTC, régulière et complète.",
        "minimum_history": (
            f"Couverture insuffisante : {observed_hours} h observées, "
            f"{minimum_history_hours} h requises par la Gate B."
        ),
        "real_not_synthetic": "La provenance réelle versionnée n'est pas établie.",
        "point_in_time": "Les observations ne sont pas déclarées point-in-time.",
        "realized_hourly_funding": (
            "Le funding n'est pas identifié comme paiement horaire réalisé; une estimation courante "
            "ne peut pas être comptée comme encaissée."
        ),
        "calibrated_data": "Les données réelles ne portent pas une calibration vérifiable.",
        "verified_spot_perp_pairs": "Aucune identité spot/perp Hyperliquid vérifiée n'est disponible.",
        "funding_complete": "Le funding horaire perp est incomplet.",
        "depth_complete": "La profondeur exécutable spot/perp est absente ou trouée.",
        "open_interest_complete": "L'open interest perp est absent ou troué.",
    }
    reasons = tuple(reason_by_check[name] for name, passed in checks.items() if not passed)
    return CarryDataAudit(observed_hours, minimum_history_hours, assets, checks, reasons)


def _close_complete(result: BacktestResult) -> bool:
    return bool(result.weights.iloc[-1].abs().le(1e-10).all())


def evaluate_carry_gate(
    results: dict[str, BacktestResult],
    *,
    audit: CarryDataAudit,
    spec: CarryGateSpec | None = None,
) -> CarryGateDecision:
    gate = spec or CarryGateSpec()
    if "base" not in results or "funding_inversion" not in results:
        raise ValueError("carry gate requires base and funding_inversion scenarios")
    reasons = list(audit.reasons)
    stressed = [result for name, result in results.items() if name != "base"]
    minimum_excess = min(result.metrics.excess_vs_benchmark for result in stressed)
    maximum_drawdown = max(abs(result.metrics.max_drawdown) for result in stressed)
    if any(result.diagnostics.get("audit_status") != "CALIBRATED" for result in results.values()):
        reasons.append("Données, frais, profondeur ou modèle de fill restent non calibrés.")
    if gate.require_complete_close and any(not _close_complete(result) for result in results.values()):
        reasons.append("La fermeture simulée est incomplète; une exposition terminale subsiste.")
    if minimum_excess < gate.minimum_stressed_excess_return:
        reasons.append(
            "Surperformance stressée insuffisante face au benchmark passif : "
            f"{minimum_excess:.6f} < {gate.minimum_stressed_excess_return:.6f}."
        )
    if maximum_drawdown > gate.maximum_stressed_drawdown:
        reasons.append(
            f"Drawdown stressé excessif : {maximum_drawdown:.6f} > "
            f"{gate.maximum_stressed_drawdown:.6f}."
        )

    if not audit.checks.get("minimum_history", False):
        status = "BLOCKED_INSUFFICIENT_REAL_DATA"
    elif not audit.passed or any(
        result.diagnostics.get("audit_status") != "CALIBRATED" for result in results.values()
    ):
        status = "BLOCKED_UNCALIBRATED"
    elif reasons:
        status = "REJECTED_STRESSED_BENCHMARK_GATE"
    else:
        status = "PROMOTABLE_RESEARCH_ONLY"
    return CarryGateDecision(
        promote=not reasons,
        status=status,
        reasons=tuple(dict.fromkeys(reasons)),
        minimum_stressed_excess_return=minimum_excess,
        maximum_stressed_drawdown=maximum_drawdown,
        required_minimum_stressed_excess_return=gate.minimum_stressed_excess_return,
        allowed_maximum_stressed_drawdown=gate.maximum_stressed_drawdown,
    )


def _component_pnl(result: BacktestResult, name: str) -> float:
    column = f"{name}_pnl"
    return float(result.attribution[column].sum()) if column in result.attribution else 0.0


def _closing_cost(result: BacktestResult) -> float:
    if result.target_weights is None or result.attribution.empty:
        return 0.0
    gross = result.target_weights.abs().sum(axis=1)
    close_rows = gross.lt(gross.shift(1, fill_value=0.0))
    if not bool(close_rows.any()):
        return 0.0
    close_start = result.target_weights.index[close_rows][0]
    rows = pd.to_datetime(result.attribution["timestamp"], utc=True).ge(close_start)
    costs = sum(
        _as_float(result.attribution.loc[rows, column].sum())
        for column in ("spread_pnl", "fee_pnl", "slippage_pnl")
        if column in result.attribution
    )
    return -costs


def _as_float(value: object) -> float:
    return float(cast(Any, value))


def summarize_carry_result(
    result: BacktestResult,
    *,
    perp_margin_fraction: float,
) -> dict[str, object]:
    if not math.isfinite(perp_margin_fraction) or perp_margin_fraction <= 0.0:
        raise ValueError("perp_margin_fraction must be finite and positive")
    initial_capital = float(result.diagnostics.get("initial_capital", 100_000.0))
    start_equity = result.equity.shift(1, fill_value=1.0)
    spot_columns = [column for column in result.weights if str(column).endswith(":spot")]
    perp_columns = [column for column in result.weights if str(column).endswith(":perp")]
    committed_weight = result.weights[spot_columns].abs().sum(axis=1)
    committed_weight += result.weights[perp_columns].abs().sum(axis=1) * perp_margin_fraction
    committed_usd = committed_weight * start_equity.abs() * initial_capital
    max_committed_usd = float(committed_usd.max())
    total_pnl_usd = result.metrics.total_return * initial_capital
    capacities = pd.Series(dtype=float)
    if "capacity_usd" in result.fills:
        capacities = pd.to_numeric(result.fills["capacity_usd"], errors="coerce")
        capacities = capacities[capacities.map(math.isfinite) & capacities.gt(0.0)]
    benchmark_return = result.metrics.benchmark_return
    return {
        "return_on_total_capital": (
            total_pnl_usd / max_committed_usd if max_committed_usd > 0.0 else 0.0
        ),
        "portfolio_return": result.metrics.total_return,
        "time_invested": result.metrics.time_in_market,
        "funding_received_usd": _component_pnl(result, "funding"),
        "basis_pnl_usd": _component_pnl(result, "basis"),
        "fees_usd": -_component_pnl(result, "fee"),
        "spread_usd": -_component_pnl(result, "spread"),
        "slippage_usd": -_component_pnl(result, "slippage"),
        "hedge_pnl_usd": _component_pnl(result, "hedge"),
        "closing_cost_usd": _closing_cost(result),
        "max_drawdown": result.metrics.max_drawdown,
        "capacity_usd": float(capacities.min()) if not capacities.empty else None,
        "max_capital_immobilized_usd": max_committed_usd,
        "average_capital_immobilized_usd": float(committed_usd.mean()),
        "opportunity_cost_usd": benchmark_return * max_committed_usd,
        "excess_vs_passive": result.metrics.excess_vs_benchmark,
        "fill_rate": result.metrics.fill_rate,
        "emergency_ioc_attempts": int(result.diagnostics.get("emergency_ioc_attempts", 0)),
        "close_complete": _close_complete(result),
    }


def _percent(value: object) -> str:
    return f"{_as_float(value) * 100:.2f} %"


def _money(value: object) -> str:
    if value is None:
        return "INDISPONIBLE"
    return f"{_as_float(value):,.2f} USD".replace(",", " ")


def write_carry_report(
    results: dict[str, BacktestResult],
    *,
    gate: CarryGateDecision,
    audit: CarryDataAudit,
    output_dir: Path,
    perp_margin_fraction: float,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    summaries = {
        name: summarize_carry_result(result, perp_margin_fraction=perp_margin_fraction)
        for name, result in results.items()
    }
    payload = {
        "schema_version": 1,
        "gate": asdict(gate),
        "data_audit": asdict(audit),
        "perp_margin_fraction": perp_margin_fraction,
        "scenarios": summaries,
    }
    (output_dir / "carry_summary.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    rows: list[str] = []
    for name, summary in summaries.items():
        rows.append(
            "<tr>"
            f"<td>{html.escape(name)}</td>"
            f"<td>{_percent(summary['return_on_total_capital'])}</td>"
            f"<td>{_percent(summary['time_invested'])}</td>"
            f"<td>{_money(summary['funding_received_usd'])}</td>"
            f"<td>{_money(summary['basis_pnl_usd'])}</td>"
            f"<td>{_money(summary['fees_usd'])}</td>"
            f"<td>{_money(summary['hedge_pnl_usd'])}</td>"
            f"<td>{_percent(summary['max_drawdown'])}</td>"
            f"<td>{_money(summary['capacity_usd'])}</td>"
            "</tr>"
        )
    reasons = "".join(f"<li>{html.escape(reason)}</li>" for reason in gate.reasons)
    base = summaries["base"]
    document = f"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><title>HyperLab Phase 05 — cash-and-carry</title>
<style>body{{font-family:Segoe UI,sans-serif;max-width:1200px;margin:auto;padding:32px;background:#091321;color:#edf5ff}}table{{width:100%;border-collapse:collapse}}th,td{{padding:10px;border:1px solid #29405d;text-align:right}}th:first-child,td:first-child{{text-align:left}}.gate{{padding:18px;border:2px solid #f2b84b;border-radius:12px}}dt{{color:#9fb5cf}}dd{{margin:0 0 12px}}</style></head>
<body><h1>Phase 05 — cash-and-carry spot/perp</h1>
<section class="gate"><strong>{html.escape(gate.status)}</strong><ul>{reasons}</ul></section>
<p>Couverture auditée : {audit.observed_hours} h / {audit.minimum_history_hours} h requises.</p>
<dl>
<dt>Capital total immobilisé</dt><dd>{_money(base['max_capital_immobilized_usd'])}</dd>
<dt>Temps investi</dt><dd>{_percent(base['time_invested'])}</dd>
<dt>Funding encaissé</dt><dd>{_money(base['funding_received_usd'])}</dd>
<dt>Basis</dt><dd>{_money(base['basis_pnl_usd'])}</dd>
<dt>Frais</dt><dd>{_money(base['fees_usd'])}</dd>
<dt>Hedge</dt><dd>{_money(base['hedge_pnl_usd'])}</dd>
<dt>Fermeture</dt><dd>{_money(base['closing_cost_usd'])}; complète={base['close_complete']}</dd>
<dt>Drawdown max</dt><dd>{_percent(base['max_drawdown'])}</dd>
<dt>Capacité</dt><dd>{_money(base['capacity_usd'])}</dd>
<dt>Coût d'opportunité</dt><dd>{_money(base['opportunity_cost_usd'])}</dd>
</dl><table><thead><tr><th>Scénario</th><th>Rendement / capital total</th><th>Temps investi</th><th>Funding</th><th>Basis</th><th>Frais</th><th>Hedge</th><th>Drawdown</th><th>Capacité</th></tr></thead><tbody>{''.join(rows)}</tbody></table>
<p>Recherche uniquement — aucune route d'ordre ni capacité de trading réel.</p></body></html>"""
    path = output_dir / "carry_report.html"
    path.write_text(document, encoding="utf-8")
    return path


__all__ = [
    "CarryDataAudit",
    "CarryGateDecision",
    "CarryGateSpec",
    "audit_carry_panel",
    "carry_stress_scenarios",
    "evaluate_carry_gate",
    "summarize_carry_result",
    "write_carry_report",
]
