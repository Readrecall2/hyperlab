from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path, PurePosixPath

import pytest

import ops.phase12.paper_supervisor as paper_supervisor
from hyperlab.cli import _phase12_paper_source_factory
from hyperlab.paper.collector_source import HyperliquidPaperPublicSource
from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.models import PaperExecutionConfig, PaperRiskLimits, PaperRunConfig, PaperState
from hyperlab.paper.runtime import PaperRuntimeLease, PublicSourceDescriptor
from hyperlab.paper.store import PaperStore
from ops.phase12.paper_supervisor import inspect_durable_start

_ROOT = Path(__file__).resolve().parents[1]
_START = datetime(2026, 8, 18, 12, tzinfo=UTC)


def _config(*, parameters: dict[str, object] | None = None) -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="phase08_supervision_fixture",
        strategy_hash="a" * 64,
        parameters=parameters or {"technical_only": True, "version": 1},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(
            maker_fee_bps=Decimal("1.5"),
            taker_fee_bps=Decimal("4.5"),
            calibration_status="UNCALIBRATED",
            source="public-tier0-technical-only",
        ),
        risk=PaperRiskLimits(stale_after_seconds=30),
        seed=12,
        initial_cash=Decimal("10000"),
        validation_started_at=_START,
        run_kind="TECHNICAL",
        data_calibration_status="UNCALIBRATED",
        data_source="phase12-supervision-public-fixture",
        required_instruments=(),
        minimum_validation_cycles=1,
    )


def _flat_run(root: Path, config: PaperRunConfig | None = None) -> tuple[Path, PaperRunConfig]:
    frozen = config or _config()
    database = root / "paper.sqlite3"
    store = PaperStore(database)
    engine = PaperEngine(store, frozen)
    engine.start()
    engine.reconcile(as_of=_START + timedelta(seconds=1))
    store.close()
    return database, frozen


def _paper_report_payload(config: PaperRunConfig) -> dict[str, object]:
    return {
        "account": {"active_order_count": 0, "positions": {}},
        "integrity": "HEAD_ANCHORS_VERIFIED_READONLY",
        "risk": {"critical_incident_count": 0},
        "runtime": {
            "reconciled": True,
            "session": {"active": True, "generation": 4, "unclosed": True},
            "source": {"gap_count": 0, "reconnect_count": 0, "status": "OBSERVED"},
            "state": "FLAT",
        },
    }


def _completed_report(
    *,
    returncode: int,
    stdout: str = "",
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["python", "-m", "hyperlab", "paper", "report"],
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def _head_changed_result(run_id: str) -> subprocess.CompletedProcess[str]:
    return _completed_report(
        returncode=2,
        stderr=(
            "Invalid value:\nPaper report blocked:\n"
            f"paper report retry required for {run_id}:\n"
            "durable head changed during assembly"
        ),
    )


def _install_health_facts(
    monkeypatch: pytest.MonkeyPatch,
    config: PaperRunConfig,
) -> None:
    monkeypatch.setattr(paper_supervisor, "_load_config", lambda _path: config)
    monkeypatch.setattr(
        paper_supervisor,
        "_verify_preflight",
        lambda _path: {"status": "READY"},
    )
    monkeypatch.setattr(
        paper_supervisor,
        "_systemctl_status",
        lambda _service_name: {"active_state": "active", "main_pid": 123},
    )
    monkeypatch.setattr(
        paper_supervisor,
        "_process_status",
        lambda pid: {"available": True, "pid": pid},
    )
    monkeypatch.setattr(
        paper_supervisor,
        "disk_status",
        lambda _database, *, minimum_free_bytes, minimum_free_percent: (
            paper_supervisor.DiskStatus(
                ok=True,
                database_size_bytes=4096,
                filesystem_free_bytes=10_000_000_000,
                filesystem_free_percent=50.0,
                minimum_free_bytes=minimum_free_bytes,
                minimum_free_percent=minimum_free_percent,
            )
        ),
    )
    monkeypatch.setattr(paper_supervisor, "_journal_lines", lambda _service_name: [])
    monkeypatch.setattr(
        paper_supervisor,
        "_wall_clock_stale_status",
        lambda _report, *, now: {"evaluated_at": now.isoformat()},
    )


def test_supervisor_health_report_succeeds_first_try(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    _install_health_facts(monkeypatch, config)
    calls: list[list[str]] = []
    sleeps: list[float] = []

    def run(arguments: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(arguments)
        return _completed_report(
            returncode=0,
            stdout=json.dumps(_paper_report_payload(config)),
        )

    monkeypatch.setattr(paper_supervisor, "_run_hyperlab_process", run)
    monkeypatch.setattr(paper_supervisor.time, "sleep", sleeps.append)

    health = paper_supervisor.build_health(Path("config.json"), Path("paper.sqlite3"))

    assert health["integrity"] == "HEAD_ANCHORS_VERIFIED_READONLY"
    assert health["report_read"] == {
        "attempts": 1,
        "max_attempts": 3,
        "retry_delay_seconds": 0.1,
        "retryable": False,
        "status": "READY",
        "transient_failures": 0,
    }
    assert calls == [
        [
            "paper",
            "report",
            config.run_id,
            "--database",
            "paper.sqlite3",
            "--after-sequence",
            "0",
            "--timeline-limit",
            "1",
            "--day-limit",
            "1",
            "--alert-limit",
            "30",
        ]
    ]
    assert sleeps == []
    assert health["authorizes_real_money"] is False
    assert health["orders_enabled"] is False
    assert health["credential_scope"] == "NONE"
    assert health["execution_network"] == "NONE"


def test_supervisor_health_retries_one_head_change_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    _install_health_facts(monkeypatch, config)
    results = [
        _head_changed_result(config.run_id),
        _completed_report(returncode=0, stdout=json.dumps(_paper_report_payload(config))),
    ]
    sleeps: list[float] = []
    monkeypatch.setattr(paper_supervisor, "_run_hyperlab_process", lambda _arguments: results.pop(0))
    monkeypatch.setattr(paper_supervisor.time, "sleep", sleeps.append)

    health = paper_supervisor.build_health(Path("config.json"), Path("paper.sqlite3"))

    assert health["integrity"] == "HEAD_ANCHORS_VERIFIED_READONLY"
    assert health["report_read"] == {
        "attempts": 2,
        "max_attempts": 3,
        "retry_delay_seconds": 0.1,
        "retryable": False,
        "status": "READY",
        "transient_failures": 1,
    }
    assert sleeps == [0.1]


def test_supervisor_health_exhausts_head_change_as_explicit_transient_degradation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    _install_health_facts(monkeypatch, config)
    calls = 0
    sleeps: list[float] = []

    def run(_arguments: list[str]) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        return _head_changed_result(config.run_id)

    monkeypatch.setattr(paper_supervisor, "_run_hyperlab_process", run)
    monkeypatch.setattr(paper_supervisor.time, "sleep", sleeps.append)

    health = paper_supervisor.build_health(Path("config.json"), Path("paper.sqlite3"))

    assert calls == 3
    assert sleeps == [0.1, 0.1]
    assert health["status"] == "TRANSIENT_UNAVAILABLE"
    assert health["readiness"] == "TRANSIENT_UNAVAILABLE"
    assert health["integrity"] == "HEAD_CHANGED_RETRY"
    assert health["blockers"] == ["HEAD_CHANGED_RETRY"]
    assert health["transient_unavailable"] is True
    assert health["paper_state"] is None
    assert health["report_read"] == {
        "attempts": 3,
        "max_attempts": 3,
        "retry_delay_seconds": 0.1,
        "retryable": True,
        "status": "HEAD_CHANGED_RETRY",
        "transient_failures": 3,
    }
    assert health["authorizes_real_money"] is False
    assert health["orders_enabled"] is False

    printed: list[dict[str, object]] = []
    monkeypatch.setattr(paper_supervisor, "build_health", lambda *_args, **_kwargs: health)
    monkeypatch.setattr(paper_supervisor, "_print", printed.append)
    exit_code = paper_supervisor.main(
        [
            "health",
            "--config",
            "config.json",
            "--database",
            "paper.sqlite3",
        ]
    )
    assert exit_code == 2
    assert printed == [health]


def test_supervisor_health_genuine_report_error_fails_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    _install_health_facts(monkeypatch, config)
    calls = 0
    sleeps: list[float] = []

    def run(_arguments: list[str]) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        return _completed_report(returncode=2, stderr="genuine report integrity failure")

    monkeypatch.setattr(paper_supervisor, "_run_hyperlab_process", run)
    monkeypatch.setattr(paper_supervisor.time, "sleep", sleeps.append)

    with pytest.raises(RuntimeError, match="reviewed HyperLab command refused"):
        paper_supervisor.build_health(Path("config.json"), Path("paper.sqlite3"))

    assert calls == 1
    assert sleeps == []

    def fail_health(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise RuntimeError("reviewed HyperLab command refused")

    printed: list[dict[str, object]] = []
    monkeypatch.setattr(paper_supervisor, "build_health", fail_health)
    monkeypatch.setattr(paper_supervisor, "_print", printed.append)
    exit_code = paper_supervisor.main(
        [
            "health",
            "--config",
            "config.json",
            "--database",
            "paper.sqlite3",
        ]
    )
    assert exit_code == 3
    assert printed[0]["status"] == "REFUSED"
    assert printed[0]["blockers"] == ["SUPERVISOR_ERROR_RUNTIMEERROR"]


def test_supervisor_admits_only_safe_flat_stopped_run(tmp_path: Path) -> None:
    database, config = _flat_run(tmp_path)

    result = inspect_durable_start(database, config, now=_START + timedelta(seconds=2))

    assert result["status"] == "READY"
    assert result["blockers"] == []
    assert result["paper_state"] == "FLAT"
    assert result["full_replay"] == "REPLAY_EXACT"
    assert result["reconciled"] is True
    assert result["authorizes_real_money"] is False
    assert result["orders_enabled"] is False
    assert result["credential_scope"] == "NONE"
    assert result["execution_network"] == "NONE"


def test_supervisor_rejects_an_already_active_runtime_lease(tmp_path: Path) -> None:
    database, config = _flat_run(tmp_path)

    with PaperRuntimeLease(database, config.run_id):
        result = inspect_durable_start(database, config)

    assert result["status"] == "REFUSED"
    assert result["blockers"] == ["RUNTIME_LEASE_ALREADY_HELD"]


def test_supervisor_rejects_manual_review_and_killed_run(tmp_path: Path) -> None:
    database, config = _flat_run(tmp_path)
    store = PaperStore(database, initialize=False)
    engine = PaperEngine(store, config)
    engine.start()
    engine.kill(
        as_of=_START + timedelta(seconds=2),
        reason="terminal supervision fixture",
        operator_artifact_hash="c" * 64,
    )
    store.close()

    result = inspect_durable_start(database, config)

    assert result["status"] == "REFUSED"
    assert "MANUAL_REVIEW_TERMINAL" in result["blockers"]
    assert "KILLED_RUN_TERMINAL" in result["blockers"]


def test_supervisor_latches_and_rejects_unclosed_runtime_session(tmp_path: Path) -> None:
    database, config = _flat_run(tmp_path)
    store = PaperStore(database, initialize=False)
    engine = PaperEngine(store, config)
    engine.start()
    engine.start_runtime_session(
        as_of=_START + timedelta(seconds=2),
        session_id="d" * 64,
        generation=1,
    )
    store.close()

    result = inspect_durable_start(database, config, now=_START + timedelta(seconds=3))

    assert result["status"] == "REFUSED"
    assert "UNCLOSED_RUNTIME_SESSION_REQUIRES_REVIEW" in result["blockers"]
    assert "UNRESOLVED_CRITICAL_INCIDENT" in result["blockers"]
    durable = PaperStore(database, initialize=False)
    try:
        projection = durable.get_projection(config.run_id)
        failures = tuple(
            durable.iter_inputs(config.run_id, input_type="PAPER_RUNTIME_FAILURE")
        )
        assert projection.state is PaperState.PAUSED
        assert projection.runtime_session_active is True
        assert len(failures) == 1
        assert "UNCLOSED_RUNTIME_SESSION" in str(failures[0].payload["reason"])
    finally:
        durable.close()


def test_supervisor_admits_exact_reviewed_unclosed_recovery(tmp_path: Path) -> None:
    database, config = _flat_run(tmp_path)
    store = PaperStore(database, initialize=False)
    engine = PaperEngine(store, config)
    engine.start()
    engine.start_runtime_session(
        as_of=_START + timedelta(seconds=2),
        session_id="f" * 64,
        generation=1,
    )
    store.close()

    refused = inspect_durable_start(
        database,
        config,
        now=_START + timedelta(seconds=3),
    )
    assert refused["status"] == "REFUSED"

    review_store = PaperStore(database, initialize=False)
    review_engine = PaperEngine(review_store, config)
    paused = review_store.get_projection(config.run_id)
    with PaperRuntimeLease(database, config.run_id):
        reviewed = review_engine.resume_from_pause(
            as_of=_START + timedelta(seconds=4),
            review_artifact_hash="9" * 64,
            reviewed_critical_incident_count=paused.critical_incident_count,
            reviewed_last_critical_incident_at=paused.last_critical_incident_at,
            recovery_mode="OFFLINE_UNCLOSED_SESSION",
        ).projection
    review_store.close()
    assert reviewed.state is PaperState.FLAT
    assert reviewed.runtime_session_active is True

    result = inspect_durable_start(database, config, now=_START + timedelta(seconds=5))
    assert result["status"] == "READY"
    assert result["runtime_session_active"] is True


def test_supervisor_rejects_unresolved_critical_incident(tmp_path: Path) -> None:
    database, config = _flat_run(tmp_path)
    store = PaperStore(database, initialize=False)
    engine = PaperEngine(store, config)
    engine.start()
    engine.pause(
        as_of=_START + timedelta(seconds=2),
        reason="terminal runtime fixture",
        operator_artifact_hash="e" * 64,
        origin="PAPER_RUNTIME_FAILURE",
    )
    store.close()

    result = inspect_durable_start(database, config)

    assert result["status"] == "REFUSED"
    assert "UNRESOLVED_CRITICAL_INCIDENT" in result["blockers"]
    assert "UNSAFE_AUTOMATIC_START_STATE_PAUSED" in result["blockers"]


def test_supervisor_rejects_config_or_runtime_identity_mismatch(tmp_path: Path) -> None:
    database, config = _flat_run(tmp_path)
    drifted = replace(config, parameters={"technical_only": True, "version": 2})

    result = inspect_durable_start(database, drifted)

    assert result["status"] == "REFUSED"
    assert "DURABLE_RUN_IDENTITY_MISMATCH" in result["blockers"]


def test_systemd_supervision_remains_paper_only_and_fail_closed() -> None:
    service = (_ROOT / "deploy/systemd/hyperlab-paper.service").read_text(encoding="utf-8")
    environment = (
        _ROOT / "deploy/systemd/paper-supervisor.env.example"
    ).read_text(encoding="utf-8")
    runbook = (
        _ROOT / "docs/PHASE12_LIVE_PAPER_RUNBOOK.md"
    ).read_text(encoding="utf-8")
    disk_service = (
        _ROOT / "deploy/systemd/hyperlab-paper-disk-guard.service"
    ).read_text(encoding="utf-8")
    disk_stop = (
        _ROOT / "deploy/systemd/hyperlab-paper-disk-stop.service"
    ).read_text(encoding="utf-8")
    timer = (
        _ROOT / "deploy/systemd/hyperlab-paper-disk-guard.timer"
    ).read_text(encoding="utf-8")

    assert "ExecCondition=" in service
    assert "paper_supervisor.py guard" in service
    assert "-m hyperlab paper run" in service
    assert "Restart=on-failure" in service
    assert "StartLimitBurst=3" in service
    assert "KillSignal=SIGTERM" in service
    assert "TimeoutStopSec=90" in service
    assert "HYPERLAB_MODE=readonly" in service
    assert "ProtectSystem=strict" in service
    assert "ReadOnlyPaths=/opt/hyperlab-multistrategy" in service
    assert "ReadWritePaths=/var/lib/hyperlab/phase12-live-paper" in service
    assert "ReadWritePaths=/opt/hyperlab-multistrategy" not in service
    assert "UnsetEnvironment=" in service
    assert "hyperlab-testnet" not in service.casefold()
    assert "-m hyperlab testnet" not in service.casefold()
    assert "hyperliquid.exchange" not in service
    assert "OnFailure=hyperlab-paper-disk-stop.service" in disk_service
    assert "systemctl stop hyperlab-paper.service" in disk_stop
    assert "systemctl stop hyperlab-paper-disk-guard.timer" in disk_stop
    assert "RemainAfterExit" not in disk_stop
    assert "OnUnitActiveSec=1min" in timer

    checkout = PurePosixPath("/opt/hyperlab-multistrategy")
    writable_root = PurePosixPath("/var/lib/hyperlab/phase12-live-paper")
    data_root = PurePosixPath(
        next(
            line.split("=", 2)[2]
            for line in service.splitlines()
            if line.startswith("Environment=HYPERLAB_DATA_DIR=")
        )
    )
    paper_root = PurePosixPath(
        next(
            line.split("=", 2)[2]
            for line in service.splitlines()
            if line.startswith("Environment=HYPERLAB_PAPER_DIR=")
        )
    )
    status_path = data_root / "paper/phase12-public-source-status.json"
    assert data_root == writable_root
    assert paper_root == writable_root / "paper"
    assert status_path.is_relative_to(writable_root)
    assert not status_path.is_relative_to(checkout)
    assert "Do not override them here" in environment
    assert (
        "HYPERLAB_PAPER_DB=/var/lib/hyperlab/phase12-live-paper/paper/"
        "paper-ba84444.sqlite3"
    ) in environment
    assert "--offline-unclosed-recovery" in runbook
    assert "UNCLOSED_RUNTIME_SESSION_REQUIRES_REVIEW" in runbook
    assert (
        "/var/lib/hyperlab/phase12-live-paper/paper/paper-ba84444.sqlite3"
    ) in runbook
    assert (
        "9aa7213ef08ddc07d700128cf8fdf90e75a764f0867201075074e0c5fbe64436"
    ) in runbook
    assert "generation 3 with generation 4" in runbook


def test_supervised_source_factory_uses_configured_persistent_data_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "var/lib/hyperlab/phase12-live-paper"
    checkout = tmp_path / "opt/hyperlab-multistrategy"
    monkeypatch.setenv("HYPERLAB_DATA_DIR", str(data_root))
    monkeypatch.chdir(_ROOT)
    config_payload = json.loads(
        (
            _ROOT
            / "config/paper/phase08-robust-pairs-btc-eth-paper-v1/paper-config.json"
        ).read_text(encoding="utf-8")
    )
    config = PaperRunConfig.from_dict(config_payload)
    captured: dict[str, Path] = {}

    class _CapturedSource:
        descriptor = PublicSourceDescriptor(
            source=config.data_source,
            data_hash=config.data_hash,
        )

        @staticmethod
        def close() -> None:
            return None

    def capture_create_mainnet(*, runtime_status_path: Path) -> _CapturedSource:
        captured["runtime_status_path"] = runtime_status_path
        return _CapturedSource()

    monkeypatch.setattr(
        HyperliquidPaperPublicSource,
        "create_mainnet",
        staticmethod(capture_create_mainnet),
    )

    source = _phase12_paper_source_factory(config)
    source.close()

    status_path = captured["runtime_status_path"]
    assert status_path == data_root / "paper/phase12-public-source-status.json"
    assert status_path.is_relative_to(data_root)
    assert not status_path.is_relative_to(checkout)
