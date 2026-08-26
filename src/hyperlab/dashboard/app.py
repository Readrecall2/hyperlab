from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, Response

from hyperlab import __version__
from hyperlab.dashboard.h1_dashboard import (
    h1_fixture_names,
    h1_fixture_snapshot,
    h1_report_download,
    h1_snapshot,
)
from hyperlab.dashboard.h1_page import H1_CSS, H1_JS, H1_PAGE
from hyperlab.storage.sqlite import database_status

if TYPE_CHECKING:
    from hyperlab.paper.store import PaperStore


_PAPER_READINESS_RUN_LIMIT = 100
_PAPER_STATUS_ALERT_LIMIT = 50
_PAPER_STATUS_RUN_LIMIT = 50

_RUNTIME_STATUS_MAX_AGE_SECONDS = 60.0
_MAX_RUNTIME_STATUS_BYTES = 1024 * 1024
_MAX_LATEST_REPORT_BYTES = 1024 * 1024
_MAX_REPORT_DOWNLOAD_BYTES = 32 * 1024 * 1024
_SAFE_REPORT_SUFFIXES = frozenset({".csv", ".html", ".json", ".md", ".parquet", ".pdf", ".txt", ".zip"})


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


class _PaperDashboardHeadChangedError(RuntimeError):
    """One bounded dashboard read raced a durable Paper commit."""


def _paper_readiness_once(store: PaperStore) -> str:
    runs = store.list_runs(limit=_PAPER_READINESS_RUN_LIMIT + 1)
    if len(runs) > _PAPER_READINESS_RUN_LIMIT:
        return "too_many_runs"
    corrupt = any(not store.inspect_head_integrity_readonly(run.run_id).ok for run in runs)
    final_runs = store.list_runs(limit=_PAPER_READINESS_RUN_LIMIT + 1)
    if tuple(run.head_identity for run in final_runs) != tuple(run.head_identity for run in runs):
        raise _PaperDashboardHeadChangedError
    return "corrupt" if corrupt else "ready"


def _paper_readiness(path: Path) -> str:
    """Verify an optional paper store without creating, migrating, or mutating it."""

    if not path.exists():
        return "not_configured"
    if path.is_symlink() or not path.is_file():
        return "unreadable"
    try:
        from hyperlab.paper.store import PaperStore

        store = PaperStore(path, initialize=False)
        for _attempt in range(2):
            try:
                return _paper_readiness_once(store)
            except _PaperDashboardHeadChangedError:
                continue
        return "head_changed_retry"
    except (OSError, RuntimeError, ValueError, sqlite3.Error):
        return "unreadable"


def _resolve_report_download(reports_dir: Path, report_path: str) -> Path | None:
    """Resolve one regular report below a non-symlinked, explicit report root."""

    if not report_path or "\\" in report_path or "\x00" in report_path or ":" in report_path:
        return None
    segments = report_path.split("/")
    if any(not segment or segment in {".", ".."} or segment.startswith(".") for segment in segments):
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


def _paper_api_payload_once(store: PaperStore) -> dict[str, object]:
    from hyperlab.paper.reporting import paper_runtime_session_health

    listed_runs = store.list_runs(limit=_PAPER_STATUS_RUN_LIMIT + 1)
    runs_truncated = len(listed_runs) > _PAPER_STATUS_RUN_LIMIT
    selected_runs = listed_runs[-_PAPER_STATUS_RUN_LIMIT:]
    runs: list[dict[str, object]] = []
    for run in selected_runs:
        integrity = store.inspect_head_integrity_readonly(run.run_id)
        if not integrity.ok:
            runs.append(
                {
                    "alert_limit": _PAPER_STATUS_ALERT_LIMIT,
                    "alerts": [],
                    "alerts_truncated": False,
                    "commit_sequence": run.commit_sequence,
                    "config_hash": run.config_hash,
                    "event_sequence": run.event_sequence,
                    "integrity": "HEAD_ANCHORS_FAILED_READONLY",
                    "orders_enabled": False,
                    "projection": None,
                    "run_id": run.run_id,
                    "runtime_session": None,
                    "same_head_assembly": True,
                    "status": "MANUAL_REVIEW",
                }
            )
            continue
        recent_alerts = store.get_recent_alerts(
            run.run_id,
            limit=_PAPER_STATUS_ALERT_LIMIT + 1,
        )
        alerts_truncated = len(recent_alerts) > _PAPER_STATUS_ALERT_LIMIT
        projection = store.get_projection_payload(run.run_id)
        runtime_session = paper_runtime_session_health(projection)
        runtime_session["recent_incidents"] = [
            {
                "alert": alert.alert,
                "alert_id": alert.alert_id,
                "code": alert.code,
                "created_at": alert.created_at,
                "event_sequence": alert.event_sequence,
                "severity": alert.severity,
            }
            for alert in recent_alerts[-_PAPER_STATUS_ALERT_LIMIT:]
            if alert.code == "PAPER_RUNTIME_FAILURE"
        ]
        runs.append(
            {
                "alert_limit": _PAPER_STATUS_ALERT_LIMIT,
                "alerts": [alert.alert for alert in recent_alerts[-_PAPER_STATUS_ALERT_LIMIT:]],
                "alerts_truncated": alerts_truncated,
                "commit_sequence": run.commit_sequence,
                "config_hash": run.config_hash,
                "event_sequence": run.event_sequence,
                "integrity": "HEAD_ANCHORS_VERIFIED_READONLY",
                "integrity_scope": {
                    "contract": "CURRENT_AND_APPEND_HEAD_V1",
                    "full_history_verified": False,
                    "full_replay_verified": False,
                    "same_head_assembly": True,
                },
                "orders_enabled": False,
                "projection": projection,
                "run_id": run.run_id,
                "runtime_session": runtime_session,
                "same_head_assembly": True,
                "status": run.status,
            }
        )
    final_listed_runs = store.list_runs(limit=_PAPER_STATUS_RUN_LIMIT + 1)
    if tuple(run.head_identity for run in final_listed_runs) != tuple(
        run.head_identity for run in listed_runs
    ):
        raise _PaperDashboardHeadChangedError
    return {
        "head_read_attempt_limit": 2,
        "mode": "paper-simulation-only",
        "orders_enabled": False,
        "run_limit": _PAPER_STATUS_RUN_LIMIT,
        "runs": runs,
        "runs_truncated": runs_truncated,
        "same_head_assembly": True,
        "status": "AVAILABLE",
    }


def _paper_api_payload(store: PaperStore) -> dict[str, object]:
    for _attempt in range(2):
        try:
            return _paper_api_payload_once(store)
        except _PaperDashboardHeadChangedError:
            continue
    raise _PaperDashboardHeadChangedError("HEAD_CHANGED_RETRY: durable Paper head changed during two reads")


def create_app(
    *,
    data_dir: Path = Path("data"),
    runtime_dir: Path | None = None,
    reports_dir: Path | None = None,
    paper_dir: Path | None = None,
    h1_campaign_root: Path | None = None,
    h1_policy_path: Path | None = Path("config/research/hyperliquid-h1-ghost-v1.json"),
    h1_default_fixture: str = "PREPARED_NOT_STARTED",
) -> FastAPI:
    app = FastAPI(title="HyperLab Read-Only Dashboard", version=__version__)
    database = data_dir / "hyperlab.sqlite3"
    resolved_runtime_dir = data_dir if runtime_dir is None else runtime_dir
    resolved_reports_dir = data_dir / "reports" if reports_dir is None else reports_dir
    resolved_paper_dir = data_dir / "paper" if paper_dir is None else paper_dir
    runtime_file = resolved_runtime_dir / "runtime_status.json"
    report_file = resolved_reports_dir / "latest_summary.json"
    paper_database = resolved_paper_dir / "paper.sqlite3"

    if h1_default_fixture.upper() not in h1_fixture_names():
        raise ValueError("unknown H1 dashboard fixture")

    def h1_headers() -> dict[str, str]:
        return {
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "X-HyperLab-Mode": "readonly",
            "X-HyperLab-Orders-Enabled": "false",
        }

    def h1_snapshot_response() -> JSONResponse:
        payload, status_code = h1_snapshot(
            h1_campaign_root,
            policy_path=h1_policy_path,
            default_fixture=h1_default_fixture,
        )
        return JSONResponse(payload, status_code=status_code, headers=h1_headers())

    def h1_section_response(section: str) -> JSONResponse:
        payload, status_code = h1_snapshot(
            h1_campaign_root,
            policy_path=h1_policy_path,
            default_fixture=h1_default_fixture,
        )
        body = {
            "schema_version": payload["schema_version"],
            "mode": "readonly",
            "orders_enabled": False,
            "state": payload.get("state", {}),
            section: payload.get(section, {} if section not in {"feeds", "incidents"} else []),
        }
        if section == "economics":
            body["economic_evidence_status"] = payload["economic_evidence_status"]
        return JSONResponse(body, status_code=status_code, headers=h1_headers())

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

    @app.api_route("/api/h1/snapshot", methods=["GET", "HEAD"])
    def api_h1_snapshot() -> JSONResponse:
        """Return one bounded, same-head H1 campaign snapshot."""

        return h1_snapshot_response()

    @app.api_route("/api/h1/identity", methods=["GET", "HEAD"])
    def api_h1_identity() -> JSONResponse:
        return h1_section_response("identity")

    @app.api_route("/api/h1/collection", methods=["GET", "HEAD"])
    def api_h1_collection() -> JSONResponse:
        return h1_section_response("collection")

    @app.api_route("/api/h1/markets", methods=["GET", "HEAD"])
    def api_h1_markets() -> JSONResponse:
        return h1_section_response("feeds")

    @app.api_route("/api/h1/strategy", methods=["GET", "HEAD"])
    def api_h1_strategy() -> JSONResponse:
        return h1_section_response("strategy")

    @app.api_route("/api/h1/economics", methods=["GET", "HEAD"])
    def api_h1_economics() -> JSONResponse:
        return h1_section_response("economics")

    @app.api_route("/api/h1/incidents", methods=["GET", "HEAD"])
    def api_h1_incidents() -> JSONResponse:
        return h1_section_response("incidents")

    @app.api_route("/api/h1/fixtures/{fixture_name}", methods=["GET", "HEAD"])
    def api_h1_fixture(fixture_name: str) -> JSONResponse:
        """Expose one allowlisted, visibly synthetic UI fixture."""

        try:
            payload = h1_fixture_snapshot(fixture_name)
        except KeyError:
            return JSONResponse(
                {
                    "mode": "readonly",
                    "orders_enabled": False,
                    "status": "UNKNOWN_FIXTURE",
                },
                status_code=404,
                headers=h1_headers(),
            )
        return JSONResponse(payload, headers=h1_headers())

    @app.api_route("/api/h1/reports/{report_id}", methods=["GET", "HEAD"])
    def api_h1_report_download(report_id: str) -> Response:
        """Download one allowlisted final H1 report after canonical holdout opening."""

        report = h1_report_download(
            h1_campaign_root,
            report_id,
            policy_path=h1_policy_path,
        )
        if report is None:
            return JSONResponse(
                {
                    "mode": "readonly",
                    "orders_enabled": False,
                    "status": "REPORT_NOT_AVAILABLE",
                },
                status_code=404,
                headers=h1_headers(),
            )
        return Response(
            content=report.value,
            media_type="application/json",
            headers={
                **h1_headers(),
                "Content-Disposition": f'attachment; filename="{report.filename}"',
                "X-Content-SHA256": report.sha256,
            },
        )

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
            payload = _paper_api_payload(store)
        except _PaperDashboardHeadChangedError:
            return JSONResponse(
                {
                    "mode": "paper-simulation-only",
                    "orders_enabled": False,
                    "retryable": True,
                    "runs": [],
                    "status": "HEAD_CHANGED_RETRY",
                },
                status_code=409,
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
        return JSONResponse(payload)

    @app.get("/api/paper/{run_id}/report")
    def paper_report(
        run_id: str,
        after_sequence: int = Query(default=0, ge=0),
        timeline_limit: int = Query(default=100, ge=1, le=500),
        day_limit: int = Query(default=31, ge=1, le=366),
        alert_limit: int = Query(default=50, ge=1, le=200),
    ) -> JSONResponse:
        """Expose one integrity-verified, bounded Paper report without writes."""

        if not paper_database.exists():
            return JSONResponse(
                {
                    "mode": "paper-simulation-only",
                    "orders_enabled": False,
                    "status": "NOT_STARTED",
                },
                status_code=404,
            )
        if paper_database.is_symlink() or not paper_database.is_file():
            return JSONResponse(
                {
                    "mode": "paper-simulation-only",
                    "orders_enabled": False,
                    "status": "UNREADABLE_FAIL_CLOSED",
                },
                status_code=503,
            )
        try:
            from hyperlab.paper.reporting import (
                PaperReportHeadChangedError,
                PaperReportIntegrityError,
                build_paper_report,
            )
            from hyperlab.paper.store import PaperStore, RunNotFoundError

            store = PaperStore(paper_database, initialize=False)
            report = build_paper_report(
                store,
                run_id,
                after_sequence=after_sequence,
                timeline_limit=timeline_limit,
                day_limit=day_limit,
                alert_limit=alert_limit,
            )
        except RunNotFoundError:
            return JSONResponse(
                {"orders_enabled": False, "run_id": run_id, "status": "UNKNOWN_RUN"},
                status_code=404,
            )
        except PaperReportHeadChangedError:
            return JSONResponse(
                {
                    "integrity": "HEAD_CHANGED_RETRY",
                    "orders_enabled": False,
                    "retryable": True,
                    "run_id": run_id,
                    "status": "HEAD_CHANGED_RETRY",
                },
                status_code=409,
            )
        except PaperReportIntegrityError:
            return JSONResponse(
                {
                    "integrity": "FAILED_READONLY",
                    "orders_enabled": False,
                    "run_id": run_id,
                    "status": "MANUAL_REVIEW",
                },
                status_code=503,
            )
        except (OSError, RuntimeError, ValueError, sqlite3.Error):
            return JSONResponse(
                {
                    "orders_enabled": False,
                    "run_id": run_id,
                    "status": "UNREADABLE_FAIL_CLOSED",
                },
                status_code=503,
            )
        return JSONResponse(report)

    @app.api_route("/api/reports/latest", methods=["GET", "HEAD"])
    def download_latest_report() -> Response:
        """Download only the export named by the fixed public latest-summary contract."""

        resolved_summary = _resolve_report_download(resolved_reports_dir, report_file.name)
        if resolved_summary is None:
            raise HTTPException(status_code=404, detail="report not found")
        try:
            if resolved_summary.stat().st_size > _MAX_LATEST_REPORT_BYTES:
                raise ValueError("latest report summary exceeds its bounded size")
            payload = json.loads(resolved_summary.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("latest report summary must be an object")
            download_value = payload.get("download_path", report_file.name)
            if not isinstance(download_value, str):
                raise ValueError("latest report export identifier is invalid")
            resolved = _resolve_report_download(resolved_reports_dir, download_value)
            if resolved is None or resolved.is_symlink():
                raise ValueError("latest report export is unavailable")
            before = resolved.stat()
            if before.st_size > _MAX_REPORT_DOWNLOAD_BYTES:
                raise ValueError("latest report export exceeds its bounded size")
            value = resolved.read_bytes()
            after = resolved.stat()
            if resolved.is_symlink() or (
                before.st_size,
                before.st_mtime_ns,
                before.st_ino,
            ) != (
                after.st_size,
                after.st_mtime_ns,
                after.st_ino,
            ) or len(value) != before.st_size:
                return JSONResponse(
                    {
                        "mode": "readonly",
                        "orders_enabled": False,
                        "status": "HEAD_CHANGED_RETRY",
                    },
                    status_code=409,
                    headers=h1_headers(),
                )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
            raise HTTPException(status_code=404, detail="report not found") from None
        return Response(
            value,
            media_type="application/octet-stream",
            headers={
                "Cache-Control": "no-store",
                "Content-Disposition": f'attachment; filename="{resolved.name}"',
                "X-Content-Type-Options": "nosniff",
                "X-HyperLab-Mode": "readonly",
                "X-HyperLab-Orders-Enabled": "false",
            },
        )

    @app.api_route("/assets/h1-dashboard.css", methods=["GET", "HEAD"])
    def h1_stylesheet() -> Response:
        return Response(
            H1_CSS,
            media_type="text/css",
            headers={"Cache-Control": "public, max-age=300", "X-Content-Type-Options": "nosniff"},
        )

    @app.api_route("/assets/h1-dashboard.js", methods=["GET", "HEAD"])
    def h1_script() -> Response:
        return Response(
            H1_JS,
            media_type="text/javascript",
            headers={"Cache-Control": "public, max-age=300", "X-Content-Type-Options": "nosniff"},
        )

    @app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
    def index() -> HTMLResponse:
        return HTMLResponse(
            H1_PAGE,
            headers={
                "Cache-Control": "no-store",
                "Content-Security-Policy": (
                    "default-src 'none'; style-src 'self'; script-src 'self'; connect-src 'self'; "
                    "img-src 'self' data:; base-uri 'none'; form-action 'none'; object-src 'none'"
                ),
                "Permissions-Policy": "camera=(), microphone=(), geolocation=(), payment=()",
                "Referrer-Policy": "no-referrer",
                "X-Content-Type-Options": "nosniff",
                "X-HyperLab-Fixture-Label": "SYNTHETIC_FIXTURE_NOT_EVIDENCE",
                "X-HyperLab-Mode": "readonly",
                "X-HyperLab-Orders-Enabled": "false",
            },
        )

    return app
