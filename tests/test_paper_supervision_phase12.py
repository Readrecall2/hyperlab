from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.models import PaperExecutionConfig, PaperRiskLimits, PaperRunConfig, PaperState
from hyperlab.paper.runtime import PaperRuntimeLease
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
    assert "UnsetEnvironment=" in service
    assert "hyperlab-testnet" not in service.casefold()
    assert "-m hyperlab testnet" not in service.casefold()
    assert "hyperliquid.exchange" not in service
    assert "OnFailure=hyperlab-paper-disk-stop.service" in disk_service
    assert "systemctl stop hyperlab-paper.service" in disk_stop
    assert "systemctl stop hyperlab-paper-disk-guard.timer" in disk_stop
    assert "RemainAfterExit" not in disk_stop
    assert "OnUnitActiveSec=1min" in timer
