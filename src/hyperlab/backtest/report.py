from __future__ import annotations

import html
import json
from pathlib import Path

import numpy as np

from hyperlab.models import BacktestResult


def _pct(value: float) -> str:
    return f"{value * 100:,.2f} %".replace(",", " ")


def _num(value: float) -> str:
    return f"{value:,.2f}".replace(",", " ")


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
        excess_vs_benchmark = m.annualized_return - benchmark_annual_return
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
        cards.append(
            f"""
            <section class="strategy-card">
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
              </div>
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
<tbody>{''.join(rows)}</tbody>
</table></div>
{''.join(cards)}
<footer>HyperLab 0.2.0 — mode recherche uniquement, aucun exécuteur d'ordres inclus.</footer>
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
