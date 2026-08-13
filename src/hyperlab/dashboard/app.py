from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from html import escape
from pathlib import Path, PurePosixPath
from urllib.parse import quote

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from hyperlab import __version__
from hyperlab.storage.sqlite import database_status
from hyperlab.strategies.registry import STRATEGY_CATALOG

_RUNTIME_STATUS_MAX_AGE_SECONDS = 60.0
_MAX_RUNTIME_STATUS_BYTES = 1024 * 1024
_MAX_LATEST_REPORT_BYTES = 1024 * 1024
_SAFE_REPORT_SUFFIXES = frozenset(
    {".csv", ".html", ".json", ".md", ".parquet", ".pdf", ".txt", ".zip"}
)


def _fmt_timestamp(value: int | None) -> str:
    if value is None:
        return "Jamais"
    return datetime.fromtimestamp(value / 1_000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _read_runtime_status(path: Path) -> dict[str, object] | None:
    try:
        if path.is_symlink():
            return {"error": "runtime_status.json invalide"}
        if not path.exists():
            return None
        if not path.is_file() or path.stat().st_size > _MAX_RUNTIME_STATUS_BYTES:
            return {"error": "runtime_status.json invalide"}
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {"error": "runtime_status.json illisible"}
    if not isinstance(payload, dict):
        return {"error": "runtime_status.json invalide"}
    return {str(key): value for key, value in payload.items()}


def _runtime_readiness(runtime: dict[str, object] | None) -> str:
    """Return ``ready`` or a stable, non-sensitive failure code."""

    if runtime is None:
        return "missing"
    if runtime.get("error") is not None:
        return "unreadable"
    schema_version = runtime.get("schema_version")
    pending_rows = runtime.get("pending_rows")
    metrics = runtime.get("metrics")
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version != 1
        or not isinstance(pending_rows, int)
        or isinstance(pending_rows, bool)
        or pending_rows < 0
        or not isinstance(metrics, dict)
    ):
        return "invalid"
    if runtime.get("mode") != "readonly" or runtime.get("orders_enabled") is not False:
        return "unsafe_mode"
    if _runtime_status_is_stale(runtime):
        return "stale"
    if runtime.get("ok") is not True or metrics.get("state") != "live":
        return "collector_unhealthy"
    if metrics.get("connection_alive") is not True:
        return "collector_disconnected"
    stale_channels = metrics.get("stale_channels")
    if not isinstance(stale_channels, list) or stale_channels:
        return "stale_data"
    return "ready"


def _paper_readiness(path: Path) -> str:
    """Verify an optional paper store without creating, migrating, or mutating it."""

    if not path.exists():
        return "not_configured"
    if path.is_symlink() or not path.is_file():
        return "unreadable"
    try:
        from hyperlab.paper.store import PaperStore

        store = PaperStore(path, initialize=False)
        runs = store.list_runs()
        if any(not store.inspect_integrity_readonly(run.run_id).ok for run in runs):
            return "corrupt"
    except (OSError, RuntimeError, ValueError, sqlite3.Error):
        return "unreadable"
    return "ready"


def _resolve_report_download(reports_dir: Path, report_path: str) -> Path | None:
    """Resolve one regular report below a non-symlinked, explicit report root."""

    if not report_path or "\\" in report_path or "\x00" in report_path or ":" in report_path:
        return None
    segments = report_path.split("/")
    if any(
        not segment or segment in {".", ".."} or segment.startswith(".")
        for segment in segments
    ):
        return None
    requested = PurePosixPath(report_path)
    if requested.is_absolute() or requested.suffix.lower() not in _SAFE_REPORT_SUFFIXES:
        return None
    try:
        if reports_dir.is_symlink():
            return None
        root = reports_dir.resolve(strict=True)
        if not root.is_dir():
            return None
        candidate = reports_dir.joinpath(*segments)
        current = reports_dir
        for segment in segments:
            current /= segment
            if current.is_symlink():
                return None
        resolved = candidate.resolve(strict=True)
    except OSError:
        return None
    if not resolved.is_relative_to(root) or not resolved.is_file():
        return None
    return resolved


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


def create_app(
    *,
    data_dir: Path = Path("data"),
    runtime_dir: Path | None = None,
    reports_dir: Path | None = None,
    paper_dir: Path | None = None,
) -> FastAPI:
    app = FastAPI(title="HyperLab Read-Only Dashboard", version=__version__)
    database = data_dir / "hyperlab.sqlite3"
    resolved_runtime_dir = data_dir if runtime_dir is None else runtime_dir
    resolved_reports_dir = data_dir / "reports" if reports_dir is None else reports_dir
    resolved_paper_dir = data_dir / "paper" if paper_dir is None else paper_dir
    runtime_file = resolved_runtime_dir / "runtime_status.json"
    report_file = resolved_reports_dir / "latest_summary.json"
    paper_database = resolved_paper_dir / "paper.sqlite3"

    @app.get("/health")
    def health() -> dict[str, object]:
        """Backward-compatible process liveness endpoint."""

        return {"ok": True, "mode": "readonly", "orders_enabled": False}

    @app.get("/health/live")
    @app.get("/live")
    def liveness() -> dict[str, object]:
        return {"ok": True, "mode": "readonly", "orders_enabled": False}

    def readiness_response() -> JSONResponse:
        runtime_check = _runtime_readiness(_read_runtime_status(runtime_file))
        paper_check = _paper_readiness(paper_database)
        legacy_check = "ready"
        try:
            database_status(database)
        except (OSError, sqlite3.Error, ValueError):
            legacy_check = "unreadable"
        ready = (
            runtime_check == "ready"
            and legacy_check == "ready"
            and paper_check in {"not_configured", "ready"}
        )
        return JSONResponse(
            {
                "ok": ready,
                "ready": ready,
                "mode": "readonly",
                "orders_enabled": False,
                "checks": {
                    "runtime_status": runtime_check,
                    "legacy_database": legacy_check,
                    "paper_database": paper_check,
                },
            },
            status_code=200 if ready else 503,
        )

    @app.get("/health/ready")
    @app.get("/ready")
    def readiness() -> JSONResponse:
        return readiness_response()

    @app.get("/api/status")
    def api_status() -> JSONResponse:
        try:
            status: dict[str, object] = dict(database_status(database))
        except (OSError, sqlite3.Error, ValueError):
            return JSONResponse(
                {
                    "ok": False,
                    "mode": "readonly",
                    "orders_enabled": False,
                    "status": "UNREADABLE_FAIL_CLOSED",
                },
                status_code=503,
            )
        status.update({"mode": "readonly", "orders_enabled": False})
        runtime = _read_runtime_status(runtime_file)
        if runtime is not None:
            status["runtime"] = runtime
            status["runtime_status_stale"] = _runtime_status_is_stale(runtime)
        return JSONResponse(status)

    @app.get("/api/paper")
    def paper_status() -> JSONResponse:
        """Expose paper projections through read-only SQLite connections only."""

        if not paper_database.exists():
            return JSONResponse(
                {
                    "mode": "paper-simulation-only",
                    "orders_enabled": False,
                    "runs": [],
                    "status": "NOT_STARTED",
                }
            )
        if paper_database.is_symlink() or not paper_database.is_file():
            return JSONResponse(
                {
                    "mode": "paper-simulation-only",
                    "orders_enabled": False,
                    "runs": [],
                    "status": "UNREADABLE_FAIL_CLOSED",
                },
                status_code=503,
            )
        try:
            from hyperlab.paper.store import PaperStore

            store = PaperStore(paper_database, initialize=False)
            runs = []
            for run in store.list_runs():
                integrity = store.inspect_integrity_readonly(run.run_id)
                if not integrity.ok:
                    runs.append(
                        {
                            "alerts": [],
                            "commit_sequence": run.commit_sequence,
                            "config_hash": run.config_hash,
                            "event_sequence": run.event_sequence,
                            "integrity": "FAILED_READONLY",
                            "orders_enabled": False,
                            "projection": None,
                            "run_id": run.run_id,
                            "status": "MANUAL_REVIEW",
                        }
                    )
                    continue
                runs.append(
                    {
                    "alerts": [alert.alert for alert in store.get_alerts(run.run_id)],
                    "commit_sequence": run.commit_sequence,
                    "config_hash": run.config_hash,
                    "event_sequence": run.event_sequence,
                    "integrity": "VERIFIED_READONLY",
                    "orders_enabled": False,
                    "projection": store.get_projection_payload(run.run_id),
                    "run_id": run.run_id,
                    "status": run.status,
                    }
                )
        except (OSError, RuntimeError, ValueError, sqlite3.Error):
            return JSONResponse(
                {
                    "mode": "paper-simulation-only",
                    "orders_enabled": False,
                    "runs": [],
                    "status": "UNREADABLE_FAIL_CLOSED",
                },
                status_code=503,
            )
        return JSONResponse(
            {
                "mode": "paper-simulation-only",
                "orders_enabled": False,
                "runs": runs,
                "status": "AVAILABLE",
            }
        )

    @app.get("/api/reports/{report_path:path}")
    def download_report(report_path: str) -> FileResponse:
        resolved = _resolve_report_download(resolved_reports_dir, report_path)
        if resolved is None:
            raise HTTPException(status_code=404, detail="report not found")
        return FileResponse(
            resolved,
            filename=resolved.name,
            media_type="application/octet-stream",
            headers={
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
            },
        )

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        try:
            status = database_status(database)
        except (OSError, sqlite3.Error, ValueError):
            status = {"snapshot_count": 0, "last_observed_at_ms": None}
        runtime = _runtime_summary(_read_runtime_status(runtime_file))
        latest_report = "Aucun rapport copié sur Umbrel"
        latest_report_link = ""
        resolved_report = _resolve_report_download(
            resolved_reports_dir,
            report_file.name,
        )
        if resolved_report is not None:
            try:
                if resolved_report.stat().st_size > _MAX_LATEST_REPORT_BYTES:
                    raise ValueError("latest report summary is too large")
                payload = json.loads(resolved_report.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("latest report summary must be an object")
                latest_report = str(payload.get("title", "Rapport disponible"))
                download_value = payload.get("download_path", report_file.name)
                download_path = (
                    download_value if isinstance(download_value, str) else report_file.name
                )
                if _resolve_report_download(resolved_reports_dir, download_path) is None:
                    raise ValueError("latest report download target is invalid")
                download_url = quote(download_path, safe="/")
                latest_report_link = (
                    f' <a class="download" href="/api/reports/{download_url}">'
                    "Télécharger</a>"
                )
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
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
.stats{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:24px 0}}.stat,.card{{border:1px solid var(--line);background:linear-gradient(145deg,var(--panel),var(--panel2));border-radius:17px;padding:18px}}.stat span{{display:block;color:var(--muted);font-size:.82rem}}.stat strong{{font-size:1.18rem}}.grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}}.card h3{{margin:5px 0}}.tier{{color:var(--amber);font-size:.8rem;font-weight:800;text-transform:uppercase;letter-spacing:.08em}}.badge{{display:inline-block;padding:5px 9px;border-radius:8px;background:#ffffff0b;color:var(--muted);font-size:.78rem;border:1px solid var(--line)}}.download{{display:inline-block;margin-left:10px;padding:5px 9px;border:1px solid var(--green);border-radius:8px;color:var(--green);text-decoration:none;font-weight:700}}.warning{{margin:24px 0;padding:16px 18px;border-left:4px solid var(--amber);border-radius:10px;background:#ffd27612}}.runtime-detail{{padding:12px 16px;border:1px solid var(--line);border-radius:12px;background:#07111f99}}code{{color:#d7e8ff}}@media(max-width:800px){{.stats,.grid{{grid-template-columns:1fr}}}}
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
<p class="muted">Dernier rapport : {escape(latest_report)}.{latest_report_link}</p>
<div class="warning"><strong>Règle de sécurité :</strong> le dashboard et le collecteur sont publics/read-only. Le futur exécuteur testnet/mainnet devra être un composant distinct, ajouté après les portes de validation.</div>
<h2>Catalogue de recherche</h2><section class="grid">{cards}</section>
<p class="muted">Santé API locale : <code>/health</code> — état JSON : <code>/api/status</code></p>
</main></body></html>"""
        return HTMLResponse(page_html)

    return app
