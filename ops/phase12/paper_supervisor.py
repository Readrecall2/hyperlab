#!/usr/bin/env python3
"""Fail-closed Linux supervision helpers for the reviewed Phase 12 Paper runtime.

This file deliberately lives outside the frozen Paper release-code manifest.  It
does not implement strategy, execution, ledger, replay, or reconciliation logic;
it calls the reviewed HyperLab Paper APIs/CLI and only applies operational
admission and host-resource policy.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.models import PaperRunConfig, PaperState, deterministic_id, parse_utc, utc_text
from hyperlab.paper.runtime import PaperAdmissionError, PaperRuntimeLease, replay_paper_run
from hyperlab.paper.store import PaperStore, RunNotFoundError

DEFAULT_MINIMUM_FREE_BYTES = 5 * 1024**3
DEFAULT_MINIMUM_FREE_PERCENT = 10.0
DEFAULT_SERVICE_NAME = "hyperlab-paper.service"
MAX_LOG_LINES = 30
PAPER_REPORT_HEALTH_MAX_ATTEMPTS = 3
PAPER_REPORT_HEALTH_RETRY_DELAY_SECONDS = 0.1
_ANSI_ESCAPE_SEQUENCE = re.compile(
    r"\x1b(?:"
    r"\[[0-?]*[ -/]*[@-~]"
    r"|\][^\x1b\x07]*(?:\x07|\x1b\\)"
    r"|[PX^_][^\x1b]*\x1b\\"
    r"|[@-Z\\-_]"
    r")"
)
_RICH_BOX_DRAWING = re.compile(r"[\u2500-\u257f]")
_HEAD_CHANGED_RETRY_LITERAL = re.compile(
    r"(?<![A-Z0-9_])HEAD_CHANGED_RETRY(?![A-Z0-9_])", re.IGNORECASE
)
_CREDENTIAL_ENV_MARKERS = (
    "PRIVATE_KEY",
    "SEED_PHRASE",
    "MNEMONIC",
    "WALLET_KEY",
    "API_KEY",
)
_SAFE_BOUNDARY = {
    "authorization_purpose": "PAPER_RUNTIME",
    "authorizes_real_money": False,
    "credential_scope": "NONE",
    "environment": "PAPER",
    "execution_network": "NONE",
    "mode": "PAPER_ONLY",
    "orders_enabled": False,
}


@dataclass(frozen=True)
class DiskStatus:
    ok: bool
    database_size_bytes: int
    filesystem_free_bytes: int
    filesystem_free_percent: float
    minimum_free_bytes: int
    minimum_free_percent: float

    def to_dict(self) -> dict[str, object]:
        return {
            "database_size_bytes": self.database_size_bytes,
            "filesystem_free_bytes": self.filesystem_free_bytes,
            "filesystem_free_percent": round(self.filesystem_free_percent, 3),
            "minimum_free_bytes": self.minimum_free_bytes,
            "minimum_free_percent": self.minimum_free_percent,
            "ok": self.ok,
        }


@dataclass(frozen=True)
class PaperReportRead:
    payload: dict[str, Any] | None
    attempts: int
    status: str

    def to_dict(self) -> dict[str, object]:
        ready = self.status == "READY"
        return {
            "attempts": self.attempts,
            "max_attempts": PAPER_REPORT_HEALTH_MAX_ATTEMPTS,
            "retry_delay_seconds": PAPER_REPORT_HEALTH_RETRY_DELAY_SECONDS,
            "retryable": not ready,
            "status": self.status,
            "transient_failures": self.attempts - 1 if ready else self.attempts,
        }


def _load_config(path: Path) -> PaperRunConfig:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Paper config must be a JSON object")
    return PaperRunConfig.from_dict(payload)


def _credential_environment_names() -> list[str]:
    return sorted(
        name
        for name in os.environ
        if any(marker in name.upper() for marker in _CREDENTIAL_ENV_MARKERS)
    )


def _subprocess_environment() -> dict[str, str]:
    return {
        name: value
        for name, value in os.environ.items()
        if not any(marker in name.upper() for marker in _CREDENTIAL_ENV_MARKERS)
    }


def _run_hyperlab_json(arguments: list[str]) -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, "-m", "hyperlab", *arguments],
        check=False,
        capture_output=True,
        env=_subprocess_environment(),
        text=True,
        timeout=900,
    )
    if completed.returncode != 0:
        raise RuntimeError("reviewed HyperLab command refused")
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("reviewed HyperLab command returned non-JSON output") from error
    if not isinstance(payload, dict):
        raise RuntimeError("reviewed HyperLab command returned a non-object payload")
    return payload


def _run_hyperlab_process(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "hyperlab", *arguments],
        check=False,
        capture_output=True,
        env=_subprocess_environment(),
        text=True,
        timeout=900,
    )


def _decode_hyperlab_json(completed: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    if completed.returncode != 0:
        raise RuntimeError("reviewed HyperLab command refused")
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("reviewed HyperLab command returned non-JSON output") from error
    if not isinstance(payload, dict):
        raise RuntimeError("reviewed HyperLab command returned a non-object payload")
    return payload


def _normalize_diagnostic_text(text: str) -> str:
    without_ansi = _ANSI_ESCAPE_SEQUENCE.sub("", text)
    without_rich_framing = _RICH_BOX_DRAWING.sub(" ", without_ansi)
    return " ".join(without_rich_framing.split())


def _is_paper_report_head_changed(
    arguments: list[str],
    completed: subprocess.CompletedProcess[str],
) -> bool:
    if arguments[:2] != ["paper", "report"] or completed.returncode != 2:
        return False
    diagnostic = _normalize_diagnostic_text(f"{completed.stdout}\n{completed.stderr}")
    if _HEAD_CHANGED_RETRY_LITERAL.search(diagnostic) is not None:
        return True
    if len(arguments) < 3:
        return False
    expected_message = (
        f"paper report retry required for {arguments[2]}: "
        "durable head changed during assembly"
    )
    return expected_message.casefold() in diagnostic.casefold()


def _run_paper_report_json(arguments: list[str]) -> PaperReportRead:
    for attempt in range(1, PAPER_REPORT_HEALTH_MAX_ATTEMPTS + 1):
        completed = _run_hyperlab_process(arguments)
        if completed.returncode == 0:
            return PaperReportRead(
                payload=_decode_hyperlab_json(completed),
                attempts=attempt,
                status="READY",
            )
        if not _is_paper_report_head_changed(arguments, completed):
            _decode_hyperlab_json(completed)
            raise AssertionError("nonzero HyperLab command unexpectedly decoded")
        if attempt < PAPER_REPORT_HEALTH_MAX_ATTEMPTS:
            time.sleep(PAPER_REPORT_HEALTH_RETRY_DELAY_SECONDS)
    return PaperReportRead(
        payload=None,
        attempts=PAPER_REPORT_HEALTH_MAX_ATTEMPTS,
        status="HEAD_CHANGED_RETRY",
    )


def _verify_preflight(config_path: Path) -> dict[str, Any]:
    payload = _run_hyperlab_json(["paper", "preflight", str(config_path)])
    for key, expected in _SAFE_BOUNDARY.items():
        if payload.get(key) != expected:
            raise RuntimeError(f"Paper preflight boundary mismatch: {key}")
    if payload.get("status") != "READY" or payload.get("wallet_or_signer_required") is not False:
        raise RuntimeError("Paper preflight is not safely ready")
    return payload


def disk_status(
    database: Path,
    *,
    minimum_free_bytes: int = DEFAULT_MINIMUM_FREE_BYTES,
    minimum_free_percent: float = DEFAULT_MINIMUM_FREE_PERCENT,
) -> DiskStatus:
    if minimum_free_bytes < 0:
        raise ValueError("minimum_free_bytes must be non-negative")
    if not 0 <= minimum_free_percent <= 100:
        raise ValueError("minimum_free_percent must be between 0 and 100")
    target = database if database.exists() else database.parent
    usage = shutil.disk_usage(target)
    free_percent = (usage.free * 100.0 / usage.total) if usage.total else 0.0
    ok = usage.free >= minimum_free_bytes and free_percent >= minimum_free_percent
    return DiskStatus(
        ok=ok,
        database_size_bytes=database.stat().st_size if database.is_file() else 0,
        filesystem_free_bytes=usage.free,
        filesystem_free_percent=free_percent,
        minimum_free_bytes=minimum_free_bytes,
        minimum_free_percent=minimum_free_percent,
    )


def _latest_review(store: PaperStore, run_id: str) -> object | None:
    latest = None
    for record in store.iter_inputs(run_id, input_type="RESUME_AFTER_REVIEW"):
        latest = record
    return latest


def _review_covers_latest_critical_incident(
    store: PaperStore,
    config: PaperRunConfig,
    projection: object,
    *,
    require_offline_unclosed: bool,
) -> bool:
    count = projection.critical_incident_count
    last_at = projection.last_critical_incident_at
    if count == 0:
        return not require_offline_unclosed
    review = _latest_review(store, config.run_id)
    latest_critical = store.get_latest_alert(config.run_id, severity="CRITICAL")
    if review is None or latest_critical is None:
        return False
    payload = review.payload
    if not isinstance(payload, dict):
        return False
    reviewed_at = payload.get("reviewed_last_critical_incident_at")
    expected_at = utc_text(last_at) if last_at is not None else None
    return bool(
        review.commit_sequence > latest_critical.commit_sequence
        and payload.get("reviewed_critical_incident_count") == count
        and reviewed_at == expected_at
        and (
            not require_offline_unclosed
            or payload.get("recovery_mode") == "OFFLINE_UNCLOSED_SESSION"
        )
    )


def _latch_unclosed_session(
    store: PaperStore,
    config: PaperRunConfig,
    *,
    now: datetime,
) -> None:
    projection = store.get_projection(config.run_id)
    session_id = projection.runtime_session_id
    if not projection.runtime_session_active or session_id is None:
        return
    artifact_hash = deterministic_id(
        "paper_runtime_failure_v1",
        config.run_id,
        config.config_hash,
        "UNCLOSED_RUNTIME_SESSION",
        "UnclosedRuntimeSessionError",
        session_id,
    )
    failure_input_id = deterministic_id(
        "paper_runtime_failure_input",
        config.run_id,
        artifact_hash,
    )
    if store.get_input(config.run_id, failure_input_id) is not None:
        return
    effective_at = now
    if projection.last_received_at is not None and effective_at < projection.last_received_at:
        effective_at = projection.last_received_at
    engine = PaperEngine(store, config)
    engine.start()
    engine.pause(
        as_of=effective_at,
        reason=(
            "terminal paper runtime failure: UNCLOSED_RUNTIME_SESSION: "
            "UnclosedRuntimeSessionError"
        ),
        operator_artifact_hash=artifact_hash,
        origin="PAPER_RUNTIME_FAILURE",
    )


def inspect_durable_start(
    database: Path,
    config: PaperRunConfig,
    *,
    now: datetime | None = None,
    latch_unclosed: bool = True,
) -> dict[str, object]:
    """Apply conservative start policy while reusing exact Paper replay and lease logic."""

    checked_at = now or datetime.now(tz=UTC)
    blockers: list[str] = []
    replay_status = "NOT_RUN"
    if not database.is_file():
        return {
            **_SAFE_BOUNDARY,
            "blockers": ["DATABASE_MISSING_NO_AUTOCREATE"],
            "checked_at": utc_text(checked_at),
            "full_replay": replay_status,
            "run_id": config.run_id,
            "status": "REFUSED",
        }

    try:
        lease = PaperRuntimeLease(database, config.run_id)
    except (OSError, PaperAdmissionError):
        return {
            **_SAFE_BOUNDARY,
            "blockers": ["RUNTIME_LEASE_ALREADY_HELD"],
            "checked_at": utc_text(checked_at),
            "full_replay": replay_status,
            "run_id": config.run_id,
            "status": "REFUSED",
        }

    with lease:
        store = PaperStore(database, initialize=False)
        try:
            try:
                run = store.get_run(config.run_id)
            except RunNotFoundError:
                return {
                    **_SAFE_BOUNDARY,
                    "blockers": ["DURABLE_RUN_IDENTITY_MISMATCH"],
                    "checked_at": utc_text(checked_at),
                    "full_replay": replay_status,
                    "run_id": config.run_id,
                    "status": "REFUSED",
                }
            durable_config = PaperRunConfig.from_dict(run.config_snapshot)
            if durable_config.to_dict() != config.to_dict():
                blockers.append("DURABLE_CONFIG_IDENTITY_MISMATCH")
            if run.config_hash != config.config_hash:
                blockers.append("DURABLE_CONFIG_HASH_MISMATCH")
            if blockers:
                projection = store.get_projection(config.run_id)
            else:
                replay_paper_run(store, config.run_id)
                replay_status = "REPLAY_EXACT"
                projection = store.get_projection(config.run_id)

            if projection.state is PaperState.MANUAL_REVIEW:
                blockers.append("MANUAL_REVIEW_TERMINAL")
            if projection.state is not PaperState.FLAT:
                blockers.append(f"UNSAFE_AUTOMATIC_START_STATE_{projection.state.value}")
            if run.status != projection.state.value:
                blockers.append("RUN_PROJECTION_STATUS_MISMATCH")
            if not projection.reconciled:
                blockers.append("UNRECONCILED_DURABLE_STATE")
            if projection.positions:
                blockers.append("ACTIVE_POSITIONS_REQUIRE_REVIEW")
            if projection.active_orders:
                blockers.append("ACTIVE_ORDERS_REQUIRE_REVIEW")
            if next(store.iter_inputs(config.run_id, input_type="PAPER_KILL"), None) is not None:
                blockers.append("KILLED_RUN_TERMINAL")

            reviewed_unclosed = False
            if projection.runtime_session_active:
                reviewed_unclosed = _review_covers_latest_critical_incident(
                    store,
                    config,
                    projection,
                    require_offline_unclosed=True,
                )
                if not reviewed_unclosed:
                    if latch_unclosed and projection.state is not PaperState.MANUAL_REVIEW:
                        _latch_unclosed_session(store, config, now=checked_at)
                        projection = store.get_projection(config.run_id)
                    blockers.append("UNCLOSED_RUNTIME_SESSION_REQUIRES_REVIEW")

            if projection.critical_incident_count > 0 and not (
                reviewed_unclosed
                or _review_covers_latest_critical_incident(
                    store,
                    config,
                    projection,
                    require_offline_unclosed=False,
                )
            ):
                blockers.append("UNRESOLVED_CRITICAL_INCIDENT")

            payload: dict[str, object] = {
                **_SAFE_BOUNDARY,
                "blockers": sorted(set(blockers)),
                "checked_at": utc_text(checked_at),
                "critical_incident_count": projection.critical_incident_count,
                "full_replay": replay_status,
                "paper_state": projection.state.value,
                "reconciled": projection.reconciled,
                "run_id": config.run_id,
                "runtime_session_active": projection.runtime_session_active,
                "runtime_session_generation": projection.runtime_session_generation,
                "status": "READY" if not blockers else "REFUSED",
            }
            return payload
        finally:
            store.close()


def _systemctl_status(service_name: str) -> dict[str, object]:
    command = [
        "systemctl",
        "show",
        service_name,
        "--no-pager",
        "--property=ActiveState,SubState,MainPID,NRestarts,ExecMainStartTimestamp",
    ]
    try:
        completed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return {"available": False}
    values: dict[str, str] = {}
    for line in completed.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key] = value
    pid = int(values.get("MainPID", "0") or "0")
    return {
        "active_state": values.get("ActiveState", "UNKNOWN"),
        "available": completed.returncode == 0,
        "main_pid": pid,
        "restart_count": int(values.get("NRestarts", "0") or "0"),
        "started_at": values.get("ExecMainStartTimestamp") or None,
        "sub_state": values.get("SubState", "UNKNOWN"),
    }


def _process_status(pid: int) -> dict[str, object]:
    if pid <= 0 or os.name == "nt":
        return {"available": False}
    try:
        stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        tail = stat_text[stat_text.rfind(")") + 2 :].split()
        uptime_seconds = float(Path("/proc/uptime").read_text(encoding="ascii").split()[0])
        ticks = os.sysconf("SC_CLK_TCK")
        page_size = os.sysconf("SC_PAGE_SIZE")
        process_age = max(0.0, uptime_seconds - int(tail[19]) / ticks)
        cpu_seconds = (int(tail[11]) + int(tail[12])) / ticks
        return {
            "available": True,
            "average_cpu_percent": round(cpu_seconds * 100.0 / process_age, 3)
            if process_age > 0
            else 0.0,
            "cpu_seconds": round(cpu_seconds, 3),
            "rss_bytes": int(tail[21]) * page_size,
            "uptime_seconds": round(process_age, 3),
        }
    except (OSError, ValueError, IndexError):
        return {"available": False}


def _journal_lines(service_name: str) -> list[str]:
    try:
        completed = subprocess.run(
            [
                "journalctl",
                "--unit",
                service_name,
                "--lines",
                str(MAX_LOG_LINES),
                "--no-pager",
                "--output=cat",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if completed.returncode != 0:
        return []
    return [line[:1000] for line in completed.stdout.splitlines()[-MAX_LOG_LINES:]]


def _wall_clock_stale_status(report: dict[str, Any], *, now: datetime) -> dict[str, object]:
    runtime = report.get("runtime", {})
    source = runtime.get("source", {}) if isinstance(runtime, dict) else {}
    config = report.get("config", {})
    risk = config.get("risk", {}) if isinstance(config, dict) else {}
    stale_after = risk.get("stale_after_seconds") if isinstance(risk, dict) else None
    required = config.get("required_instruments", []) if isinstance(config, dict) else []
    latest = source.get("latest_by_instrument", {}) if isinstance(source, dict) else {}
    instruments: dict[str, object] = {}
    any_stale = False
    for instrument in required if isinstance(required, list) else []:
        market = latest.get(instrument, {}) if isinstance(latest, dict) else {}
        timestamp = market.get("received_at") if isinstance(market, dict) else None
        age_seconds = None
        stale = True
        if isinstance(timestamp, str) and isinstance(stale_after, int):
            age_seconds = (now - parse_utc(timestamp)).total_seconds()
            stale = bool(
                age_seconds < 0
                or age_seconds > stale_after
                or market.get("stale", False)
                or market.get("gap", False)
                or not market.get("tradable", True)
            )
        any_stale = any_stale or stale
        instruments[str(instrument)] = {
            "age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
            "last_public_market_at": timestamp,
            "stale": stale,
        }
    return {
        "any_required_instrument_stale": any_stale,
        "evaluated_at": utc_text(now),
        "instruments": instruments,
        "stale_after_seconds": stale_after,
    }


def _service_main_pid(service: dict[str, object]) -> int:
    value = service.get("main_pid", 0)
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return value


def build_health(
    config_path: Path,
    database: Path,
    *,
    service_name: str = DEFAULT_SERVICE_NAME,
    minimum_free_bytes: int = DEFAULT_MINIMUM_FREE_BYTES,
    minimum_free_percent: float = DEFAULT_MINIMUM_FREE_PERCENT,
) -> dict[str, object]:
    config = _load_config(config_path)
    readiness = "READY"
    try:
        _verify_preflight(config_path)
    except (OSError, RuntimeError, ValueError):
        readiness = "BLOCKED"
    report_read = _run_paper_report_json(
        [
            "paper",
            "report",
            config.run_id,
            "--database",
            str(database),
            "--after-sequence",
            "0",
            "--timeline-limit",
            "1",
            "--day-limit",
            "1",
            "--alert-limit",
            "30",
        ]
    )
    report = report_read.payload
    if report is None:
        service = _systemctl_status(service_name)
        process = _process_status(_service_main_pid(service))
        disk = disk_status(
            database,
            minimum_free_bytes=minimum_free_bytes,
            minimum_free_percent=minimum_free_percent,
        )
        return {
            **_SAFE_BOUNDARY,
            "active_order_count": None,
            "active_position_count": None,
            "blockers": ["HEAD_CHANGED_RETRY"],
            "critical_incident_count": None,
            "disk": disk.to_dict(),
            "gap_count": None,
            "integrity": "HEAD_CHANGED_RETRY",
            "latest_logs": _journal_lines(service_name),
            "paper_state": None,
            "preflight_readiness": readiness,
            "process": process,
            "readiness": "TRANSIENT_UNAVAILABLE",
            "reconciled": None,
            "reconnect_count": None,
            "report_read": report_read.to_dict(),
            "run_id": config.run_id,
            "runtime_session": None,
            "service": service,
            "source_status_at_durable_head": None,
            "stale": None,
            "status": "TRANSIENT_UNAVAILABLE",
            "transient_unavailable": True,
        }
    runtime = report["runtime"]
    source = runtime["source"]
    account = report["account"]
    risk = report["risk"]
    service = _systemctl_status(service_name)
    process = _process_status(_service_main_pid(service))
    disk = disk_status(
        database,
        minimum_free_bytes=minimum_free_bytes,
        minimum_free_percent=minimum_free_percent,
    )
    return {
        **_SAFE_BOUNDARY,
        "active_order_count": account["active_order_count"],
        "active_position_count": len(account["positions"]),
        "critical_incident_count": risk["critical_incident_count"],
        "disk": disk.to_dict(),
        "gap_count": source["gap_count"],
        "integrity": report["integrity"],
        "latest_logs": _journal_lines(service_name),
        "paper_state": runtime["state"],
        "preflight_readiness": readiness,
        "process": process,
        "readiness": readiness,
        "reconciled": runtime["reconciled"],
        "reconnect_count": source["reconnect_count"],
        "report_read": report_read.to_dict(),
        "run_id": config.run_id,
        "runtime_session": runtime["session"],
        "service": service,
        "source_status_at_durable_head": source["status"],
        "stale": _wall_clock_stale_status(report, now=datetime.now(tz=UTC)),
        "transient_unavailable": False,
    }


def _print(payload: dict[str, object]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("guard", "health"):
        command = subparsers.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--database", type=Path, required=True)
        command.add_argument("--minimum-free-bytes", type=int, default=DEFAULT_MINIMUM_FREE_BYTES)
        command.add_argument(
            "--minimum-free-percent", type=float, default=DEFAULT_MINIMUM_FREE_PERCENT
        )
        if name == "health":
            command.add_argument("--service-name", default=DEFAULT_SERVICE_NAME)
    disk = subparsers.add_parser("disk-check")
    disk.add_argument("--database", type=Path, required=True)
    disk.add_argument("--minimum-free-bytes", type=int, default=DEFAULT_MINIMUM_FREE_BYTES)
    disk.add_argument("--minimum-free-percent", type=float, default=DEFAULT_MINIMUM_FREE_PERCENT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "disk-check":
            status = disk_status(
                args.database,
                minimum_free_bytes=args.minimum_free_bytes,
                minimum_free_percent=args.minimum_free_percent,
            )
            _print({**_SAFE_BOUNDARY, "disk": status.to_dict(), "status": "READY" if status.ok else "REFUSED"})
            return 0 if status.ok else 3
        if args.command == "health":
            payload = build_health(
                args.config,
                args.database,
                service_name=args.service_name,
                minimum_free_bytes=args.minimum_free_bytes,
                minimum_free_percent=args.minimum_free_percent,
            )
            _print(payload)
            return 2 if payload.get("status") == "TRANSIENT_UNAVAILABLE" else 0

        credential_names = _credential_environment_names()
        if credential_names:
            _print(
                {
                    **_SAFE_BOUNDARY,
                    "blockers": ["CREDENTIAL_LIKE_ENVIRONMENT_PRESENT"],
                    "credential_variable_names": credential_names,
                    "status": "REFUSED",
                }
            )
            return 3
        preflight = _verify_preflight(args.config)
        config = _load_config(args.config)
        disk = disk_status(
            args.database,
            minimum_free_bytes=args.minimum_free_bytes,
            minimum_free_percent=args.minimum_free_percent,
        )
        if not disk.ok:
            _print(
                {
                    **_SAFE_BOUNDARY,
                    "blockers": ["DISK_SPACE_BELOW_THRESHOLD"],
                    "disk": disk.to_dict(),
                    "run_id": config.run_id,
                    "status": "REFUSED",
                }
            )
            return 3
        payload = inspect_durable_start(args.database, config)
        payload["disk"] = disk.to_dict()
        payload["readiness"] = preflight["status"]
        _print(payload)
        return 0 if payload["status"] == "READY" else 3
    except (OSError, RuntimeError, ValueError, KeyError, json.JSONDecodeError) as error:
        _print(
            {
                **_SAFE_BOUNDARY,
                "blockers": [f"SUPERVISOR_ERROR_{type(error).__name__.upper()}"],
                "status": "REFUSED",
            }
        )
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
