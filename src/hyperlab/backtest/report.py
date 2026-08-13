from __future__ import annotations

import html
import json
import math
from pathlib import Path

import numpy as np

from hyperlab import __version__
from hyperlab.backtest.bootstrap import block_bootstrap_ci
from hyperlab.models import BacktestResult


def _pct(value: float) -> str:
    return f"{value * 100:,.2f} %".replace(",", " ")


def _num(value: float) -> str:
    return f"{value:,.2f}".replace(",", " ")


def _is_explicitly_out_of_sample(result: BacktestResult) -> bool:
    scope = str(result.diagnostics.get("evaluation_scope", "")).strip().lower()
    split = str(result.diagnostics.get("evaluation_split", "")).strip().lower()
    return (
        result.diagnostics.get("is_out_of_sample") is True
        or scope == "out_of_sample"
        or split in {"walk_forward_oos", "validation_oos", "final_test", "stress_final_test"}
    )


def _annual_rate_over_result_period(result: BacktestResult, annual_rate: float) -> float:
    index = result.equity.index
    if len(index) < 2:
        elapsed_seconds = 86_400.0
    else:
        datetime_index = np.asarray(index.view("int64"), dtype=np.int64)
        median_nanoseconds = float(np.median(np.diff(datetime_index)))
        elapsed_seconds = float(datetime_index[-1] - datetime_index[0] + median_nanoseconds) / 1e9
    elapsed_years = elapsed_seconds / (365.25 * 24.0 * 3600.0)
    return math.expm1(math.log1p(annual_rate) * elapsed_years)


def _sparkline(result: BacktestResult, width: int = 620, height: int = 150) -> str:
    values = result.equity.to_numpy(dtype=float)
    if len(values) > 1200:
        indices = np.linspace(0, len(values) - 1, 1200).astype(int)
        values = values[indices]
    if len(values) < 2:
        values = np.array([1.0, 1.0])
    minimum = float(np.nanmin(values))
    maximum = float(np.nanmax(values))
    span = maximum - minimum if maximum > minimum else 1.0
    points: list[str] = []
    for idx, value in enumerate(values):
        x = idx / (len(values) - 1) * width
        y = height - ((float(value) - minimum) / span * (height - 12) + 6)
        points.append(f"{x:.1f},{y:.1f}")
    return (
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="Courbe de capital">'
        f'<polyline fill="none" stroke="currentColor" stroke-width="2" points="{" ".join(points)}"/>'
        "</svg>"
    )


def write_comparison_report(
    results: list[BacktestResult],
    output_dir: Path,
    *,
    title: str = "HyperLab — comparaison des stratégies",
    data_label: str = "Données synthétiques de démonstration",
    benchmark_annual_return: float = 0.045,
    bootstrap_block_size: int = 24,
    bootstrap_resamples: int = 1_000,
    bootstrap_seed: int = 42,
    bootstrap_confidence_level: float = 0.95,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows: list[str] = []
    cards: list[str] = []
    strategy_summaries: list[dict[str, object]] = []
    summary: dict[str, object] = {
        "title": title,
        "data_label": data_label,
        "benchmark_annual_return": benchmark_annual_return,
        "strategies": strategy_summaries,
    }

    for result in results:
        m = result.metrics
        audit_status = str(result.diagnostics.get("audit_status", "UNCALIBRATED"))
        result.diagnostics.setdefault("audit_status", audit_status)
        if result.benchmark is not None:
            benchmark_return = m.benchmark_return
            excess_vs_benchmark = m.excess_vs_benchmark
        else:
            benchmark_return = _annual_rate_over_result_period(result, benchmark_annual_return)
            excess_vs_benchmark = m.total_return - benchmark_return
        block_size = min(bootstrap_block_size, len(result.returns))
        if _is_explicitly_out_of_sample(result):
            uncertainty = block_bootstrap_ci(
                result.returns["net_return"],
                block_size=max(1, block_size),
                n_resamples=bootstrap_resamples,
                confidence_level=bootstrap_confidence_level,
                seed=bootstrap_seed,
            )
            result.uncertainty = {
                "available": True,
                "status": "AVAILABLE_OOS",
                "statistic": "mean_bar_return",
                "estimate": uncertainty.estimate,
                "lower": uncertainty.lower,
                "upper": uncertainty.upper,
                "confidence_level": uncertainty.confidence_level,
                "block_size": uncertainty.block_size,
                "n_resamples": uncertainty.n_resamples,
                "seed": uncertainty.seed,
                "insufficient_sample": uncertainty.insufficient_sample,
                "time_index_verified": uncertainty.time_index_verified,
                "cadence": uncertainty.cadence,
            }
            uncertainty_text = f"{_pct(uncertainty.lower)} à {_pct(uncertainty.upper)}"
        else:
            reason = "Intervalle indisponible : le résultat n'est pas explicitement étiqueté out-of-sample."
            result.uncertainty = {
                "available": False,
                "status": "UNAVAILABLE_NOT_OOS",
                "reason": reason,
                "statistic": "mean_bar_return",
                "estimate": None,
                "lower": None,
                "upper": None,
                "confidence_level": bootstrap_confidence_level,
                "block_size": max(1, block_size),
                "n_resamples": bootstrap_resamples,
                "seed": bootstrap_seed,
                "time_index_verified": False,
                "cadence": None,
            }
            warnings = list(result.diagnostics.get("warnings", []))
            if reason not in warnings:
                warnings.append(reason)
            result.diagnostics["warnings"] = warnings
            uncertainty_text = "Indisponible — résultat non étiqueté OOS"
        rows.append(
            "<tr>"
            f"<td><strong>{html.escape(result.strategy_name)}</strong><br><small>{html.escape(result.risk_tier)}</small></td>"
            f"<td>{_pct(m.total_return)}</td>"
            f"<td>{_pct(m.annualized_return)}</td>"
            f"<td>{_pct(excess_vs_benchmark)}</td>"
            f"<td>{_num(m.sharpe)}</td>"
            f"<td>{_pct(m.max_drawdown)}</td>"
            f"<td>{_pct(m.worst_day)}</td>"
            f"<td>{_num(m.turnover)}</td>"
            f"<td>{_pct(m.time_in_market)}</td>"
            "</tr>"
        )
        diagnostic_text = html.escape(json.dumps(result.diagnostics, ensure_ascii=False))
        warning_text = " ".join(str(item) for item in result.diagnostics.get("warnings", []))
        breakdown_tables = "".join(
            f"<details><summary>PnL par {html.escape(name)}</summary>{frame.to_html(border=0)}</details>"
            for name, frame in result.breakdowns.items()
        )
        cards.append(
            f"""
            <section class="strategy-card">
              <div class="notice"><strong>{html.escape(audit_status)}</strong> — {html.escape(warning_text or "Hypothèses de recherche enregistrées.")}</div>
              <div class="strategy-head">
                <div><span class="tier">{html.escape(result.risk_tier)}</span><h2>{html.escape(result.strategy_name)}</h2></div>
                <div class="return">{_pct(m.total_return)}</div>
              </div>
              <div class="chart">{_sparkline(result)}</div>
              <div class="mini-grid">
                <div><span>Annualisé</span><strong>{_pct(m.annualized_return)}</strong></div>
                <div><span>Sharpe</span><strong>{_num(m.sharpe)}</strong></div>
                <div><span>Drawdown max</span><strong>{_pct(m.max_drawdown)}</strong></div>
                <div><span>Exposition brute max</span><strong>{_num(m.max_gross_leverage)}&times;</strong></div>
                <div><span>Fill rate</span><strong>{_pct(m.fill_rate)}</strong></div>
                <div><span>Benchmark période</span><strong>{_pct(benchmark_return)}</strong></div>
                <div><span>IC bootstrap moyenne/barre</span><strong>{html.escape(uncertainty_text)}</strong></div>
              </div>
              {breakdown_tables}
              <details><summary>Diagnostics</summary><code>{diagnostic_text}</code></details>
            </section>
            """
        )
        strategy_summaries.append(
            {
                "name": result.strategy_name,
                "risk_tier": result.risk_tier,
                "metrics": m.as_dict(),
                "diagnostics": result.diagnostics,
                "uncertainty": result.uncertainty,
                "breakdowns": {
                    name: json.loads(frame.reset_index().to_json(orient="records", date_format="iso"))
                    for name, frame in result.breakdowns.items()
                },
            }
        )

    document = f"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(title)}</title>
<style>
:root {{ color-scheme: dark; --bg:#08111f; --panel:#101d31; --panel2:#14243c; --text:#eef5ff; --muted:#9db0ca; --line:#2b4262; --accent:#62d7c4; --warn:#ffcc66; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family:Inter,Segoe UI,system-ui,sans-serif; background:radial-gradient(circle at top,#173156 0,#08111f 44%); color:var(--text); line-height:1.55; }}
main {{ max-width:1180px; margin:auto; padding:48px 22px 80px; }}
.hero {{ padding:34px; border:1px solid var(--line); border-radius:24px; background:linear-gradient(135deg,rgba(20,36,60,.95),rgba(11,22,38,.95)); box-shadow:0 24px 80px rgba(0,0,0,.25); }}
h1 {{ margin:0 0 10px; font-size:clamp(2rem,5vw,4.2rem); letter-spacing:-.045em; }}
.lead {{ color:var(--muted); max-width:850px; font-size:1.08rem; }}
.notice {{ margin-top:20px; padding:15px 18px; border-left:4px solid var(--warn); background:rgba(255,204,102,.08); border-radius:10px; }}
.table-wrap {{ overflow:auto; margin:28px 0; border:1px solid var(--line); border-radius:18px; background:rgba(16,29,49,.88); }}
table {{ width:100%; border-collapse:collapse; min-width:1060px; }}
th,td {{ padding:14px 15px; border-bottom:1px solid var(--line); text-align:right; white-space:nowrap; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ color:var(--muted); font-size:.85rem; text-transform:uppercase; letter-spacing:.06em; }}
small,.tier {{ color:var(--muted); }}
.strategy-card {{ margin:24px 0; padding:25px; border:1px solid var(--line); border-radius:20px; background:linear-gradient(145deg,var(--panel),var(--panel2)); }}
.strategy-head {{ display:flex; justify-content:space-between; gap:20px; align-items:start; }}
.strategy-head h2 {{ margin:4px 0 0; }}
.return {{ font-size:1.75rem; font-weight:800; color:var(--accent); }}
.chart {{ color:var(--accent); margin:18px 0; border-radius:14px; background:rgba(4,12,23,.42); padding:8px; }}
.chart svg {{ display:block; width:100%; height:150px; }}
.mini-grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; }}
.mini-grid div {{ padding:13px; border:1px solid var(--line); border-radius:12px; background:rgba(5,14,27,.35); }}
.mini-grid span {{ display:block; color:var(--muted); font-size:.82rem; }}
.mini-grid strong {{ display:block; margin-top:3px; }}
details {{ margin-top:16px; }} code {{ display:block; white-space:pre-wrap; color:#cfe4ff; margin-top:8px; }}
details table {{ margin-top:10px; width:100%; min-width:720px; }}
footer {{ color:var(--muted); margin-top:34px; }}
@media (max-width:760px) {{ .mini-grid {{ grid-template-columns:repeat(2,1fr); }} .strategy-head {{ display:block; }} .return {{ margin-top:8px; }} }}
</style>
</head>
<body><main>
<section class="hero">
  <div class="tier">RAPPORT DE RECHERCHE</div>
  <h1>{html.escape(title)}</h1>
  <p class="lead">{html.escape(data_label)}. Les rendements ne sont ni plafonnés ni forcés. Le moteur affiche aussi bien les pertes que les résultats anormalement élevés.</p>
  <p class="lead">Benchmark économique indicatif : {_pct(benchmark_annual_return)} annualisé, avant ajustement fin du risque.</p>
  <div class="notice"><strong>Important :</strong> ce rapport de démonstration valide l'installation et la plomberie du backtester. Il ne constitue aucune preuve de rentabilité réelle.</div>
</section>
<div class="table-wrap"><table>
<thead><tr><th>Stratégie</th><th>Retour total</th><th>Annualisé</th><th>Écart vs benchmark</th><th>Sharpe</th><th>Drawdown</th><th>Pire jour</th><th>Turnover</th><th>Temps investi</th></tr></thead>
<tbody>{"".join(rows)}</tbody>
</table></div>
{"".join(cards)}
<footer>HyperLab {html.escape(__version__)} — mode recherche uniquement, aucun exécuteur d'ordres inclus.</footer>
</main></body></html>"""

    report_path = output_dir / "comparison.html"
    report_path.write_text(document, encoding="utf-8")
    (output_dir / "latest_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    for result in results:
        result.report_path = report_path
    return report_path
