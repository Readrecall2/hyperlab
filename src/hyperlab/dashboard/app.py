from __future__ import annotations

import json
from datetime import UTC, datetime
from html import escape
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from hyperlab.storage.sqlite import database_status
from hyperlab.strategies.registry import STRATEGY_CATALOG

_RUNTIME_STATUS_MAX_AGE_SECONDS = 60.0


def _fmt_timestamp(value: int | None) -> str:
    if value is None:
        return "Jamais"
    return datetime.fromtimestamp(value / 1_000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _read_runtime_status(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {"error": "runtime_status.json illisible"}
    if not isinstance(payload, dict):
        return {"error": "runtime_status.json invalide"}
    return {str(key): value for key, value in payload.items()}


def _runtime_status_is_stale(
    runtime: dict[str, object],
    *,
    now: datetime | None = None,
) -> bool:
    updated_at = runtime.get("updated_at")
    if not isinstance(updated_at, str):
        return True
    try:
        observed = datetime.fromisoformat(updated_at)
    except ValueError:
        return True
    if observed.tzinfo is None or observed.utcoffset() is None:
        return True
    current = now or datetime.now(tz=UTC)
    age = (current - observed.astimezone(UTC)).total_seconds()
    return age < 0 or age > _RUNTIME_STATUS_MAX_AGE_SECONDS


def _runtime_summary(
    runtime: dict[str, object] | None,
    *,
    now: datetime | None = None,
) -> dict[str, str]:
    if runtime is None:
        return {
            "state": "INDISPONIBLE",
            "connection": "aucune donnée runtime",
            "gaps": "—",
            "stale_count": "—",
            "stale_detail": "statut non publié",
            "updated_at": "Jamais",
        }

    metrics_value = runtime.get("metrics")
    metrics = metrics_value if isinstance(metrics_value, dict) else {}

    def count(name: str) -> str:
        value = metrics.get(name)
        return str(value) if isinstance(value, int) and not isinstance(value, bool) else "—"

    state = metrics.get("state")
    state_label = str(state).upper() if isinstance(state, str) else "INCONNU"
    status_stale = _runtime_status_is_stale(runtime, now=now)
    alive = metrics.get("connection_alive") is True and not status_stale
    if status_stale:
        state_label = "RUNTIME STATUS STALE"
    connection = (
        f"{'active' if alive else 'inactive'} · {count('connections')} total"
        f" · {count('reconnects')} reconnexion(s)"
    )
    stale_value = metrics.get("stale_channels")
    stale_channels = (
        [value for value in stale_value if isinstance(value, str)] if isinstance(stale_value, list) else []
    )
    error = runtime.get("error")
    stale_detail = ", ".join(stale_channels) if stale_channels else "aucun flux déclaré stale"
    if isinstance(error, str) and error:
        stale_detail = f"{stale_detail} · erreur: {error}"
    if status_stale:
        stale_detail = f"runtime_status.json stale or invalid | {stale_detail}"
    updated_at = runtime.get("updated_at")
    return {
        "state": state_label,
        "connection": connection,
        "gaps": count("gaps"),
        "stale_count": str(len(stale_channels)),
        "stale_detail": stale_detail,
        "updated_at": str(updated_at) if isinstance(updated_at, str) else "Inconnue",
    }


def create_app(*, data_dir: Path = Path("data")) -> FastAPI:
    app = FastAPI(title="HyperLab Read-Only Dashboard", version="0.2.0")
    database = data_dir / "hyperlab.sqlite3"
    runtime_file = data_dir / "runtime_status.json"
    report_file = data_dir / "reports" / "latest_summary.json"

    @app.get("/health")
    def health() -> dict[str, object]:
        return {"ok": True, "mode": "readonly", "orders_enabled": False}

    @app.get("/api/status")
    def api_status() -> JSONResponse:
        status: dict[str, object] = dict(database_status(database))
        status.update({"mode": "readonly", "orders_enabled": False})
        runtime = _read_runtime_status(runtime_file)
        if runtime is not None:
            status["runtime"] = runtime
            status["runtime_status_stale"] = _runtime_status_is_stale(runtime)
        return JSONResponse(status)

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        status = database_status(database)
        runtime = _runtime_summary(_read_runtime_status(runtime_file))
        latest_report = "Aucun rapport copié sur Umbrel"
        if report_file.exists():
            try:
                payload = json.loads(report_file.read_text(encoding="utf-8"))
                latest_report = str(payload.get("title", "Rapport disponible"))
            except (OSError, json.JSONDecodeError):
                latest_report = "Rapport illisible"

        cards = "".join(
            f"""
            <article class="card">
              <div class="tier">{escape(str(entry["tier"]))}</div>
              <h3>{escape(str(entry["label"]))}</h3>
              <p>{escape(str(entry["summary"]))}</p>
              <span class="badge">{escape(str(entry["status"]))}</span>
            </article>
            """
            for entry in STRATEGY_CATALOG.values()
        )
        page_html = f"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>HyperLab</title>
<style>
:root{{--bg:#07111f;--panel:#101e34;--panel2:#142844;--text:#f0f6ff;--muted:#9db1ca;--line:#284564;--green:#5ee0bd;--amber:#ffd276}}
*{{box-sizing:border-box}}body{{margin:0;font-family:Inter,Segoe UI,system-ui,sans-serif;background:radial-gradient(circle at top,#17365e,#07111f 48%);color:var(--text);line-height:1.55}}
main{{max-width:1180px;margin:auto;padding:42px 22px 70px}}.hero{{padding:30px;border:1px solid var(--line);border-radius:24px;background:linear-gradient(135deg,rgba(20,40,68,.96),rgba(9,20,36,.96));box-shadow:0 20px 80px #0006}}
h1{{font-size:clamp(2.2rem,6vw,4.5rem);letter-spacing:-.05em;margin:4px 0 8px}}.lead,.muted{{color:var(--muted)}}.mode{{display:inline-flex;gap:8px;align-items:center;padding:8px 12px;border:1px solid #3e7d72;border-radius:99px;color:var(--green);font-weight:800;background:#5ee0bd12}}
.stats{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:24px 0}}.stat,.card{{border:1px solid var(--line);background:linear-gradient(145deg,var(--panel),var(--panel2));border-radius:17px;padding:18px}}.stat span{{display:block;color:var(--muted);font-size:.82rem}}.stat strong{{font-size:1.18rem}}.grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}}.card h3{{margin:5px 0}}.tier{{color:var(--amber);font-size:.8rem;font-weight:800;text-transform:uppercase;letter-spacing:.08em}}.badge{{display:inline-block;padding:5px 9px;border-radius:8px;background:#ffffff0b;color:var(--muted);font-size:.78rem;border:1px solid var(--line)}}.warning{{margin:24px 0;padding:16px 18px;border-left:4px solid var(--amber);border-radius:10px;background:#ffd27612}}.runtime-detail{{padding:12px 16px;border:1px solid var(--line);border-radius:12px;background:#07111f99}}code{{color:#d7e8ff}}@media(max-width:800px){{.stats,.grid{{grid-template-columns:1fr}}}}
</style></head><body><main>
<section class="hero"><div class="mode">● READ-ONLY — ORDRES IMPOSSIBLES</div><h1>HyperLab</h1><p class="lead">Collecte 24/24, laboratoire multi-stratégies et paper research pour Hyperliquid. Cette application Umbrel ne contient ni clé privée ni module d'exécution.</p></section>
<h2>Santé du collecteur public</h2>
<section class="stats">
<div class="stat"><span>État runtime</span><strong>{escape(runtime["state"])}</strong></div>
<div class="stat"><span>Connexions</span><strong>{escape(runtime["connection"])}</strong></div>
<div class="stat"><span>Trous visibles</span><strong>{escape(runtime["gaps"])}</strong></div>
<div class="stat"><span>Flux stale</span><strong>{escape(runtime["stale_count"])}</strong></div>
</section>
<p class="runtime-detail"><strong>Détail fraîcheur :</strong> {escape(runtime["stale_detail"])}<br><span class="muted">Statut runtime publié : {escape(runtime["updated_at"])}</span></p>
<p class="muted">Compatibilité legacy SQLite : {status["snapshot_count"]} snapshot(s), dernière observation {_fmt_timestamp(status["last_observed_at_ms"])}. Ce compteur ne mesure pas les lignes Parquet du collecteur Phase 02.</p>
<p class="muted">Dernier rapport : {escape(latest_report)}.</p>
<div class="warning"><strong>Règle de sécurité :</strong> le dashboard et le collecteur sont publics/read-only. Le futur exécuteur testnet/mainnet devra être un composant distinct, ajouté après les portes de validation.</div>
<h2>Catalogue de recherche</h2><section class="grid">{cards}</section>
<p class="muted">Santé API locale : <code>/health</code> — état JSON : <code>/api/status</code></p>
</main></body></html>"""
        return HTMLResponse(page_html)

    return app
