from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from hyperlab.cli import app
from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.models import PaperExecutionConfig, PaperRiskLimits, PaperRunConfig
from hyperlab.paper.store import PaperStore


def _demo_config() -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="phase12_gate_cli_fixture",
        strategy_hash="a" * 64,
        parameters={"version": 1},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(),
        risk=PaperRiskLimits(),
        seed=12,
        initial_cash=Decimal("100000"),
        validation_started_at=datetime.now(tz=UTC) - timedelta(days=1),
    )


def test_paper_gate_cli_is_read_only_and_exits_nonzero_when_blocked(
    tmp_path: Path,
) -> None:
    database = tmp_path / "paper.sqlite3"
    config = _demo_config()
    PaperEngine(PaperStore(database), config).start()
    before = database.read_bytes()

    result = CliRunner().invoke(
        app,
        ["paper", "gate", config.run_id, "--database", str(database)],
    )

    assert result.exit_code == 2, result.output
    payload = json.loads(result.stdout)
    assert payload["passed"] is False
    assert payload["eligible"] is False
    assert payload["status"] == "BLOCKED_PRECONDITIONS"
    assert payload["blockers"] == payload["reasons"]
    assert payload["admission_status"] == "NO_COMPILED_APPROVAL"
    assert payload["checks"]["approved_admission"] is False
    assert payload["checks"]["durable_runtime_source_attestation"] is False
    assert payload["checks"]["gate_d_artifact_bytes_verified"] is False
    assert payload["mode"] == "PAPER_ONLY"
    assert payload["orders_enabled"] is False
    assert payload["run_id"] == config.run_id
    assert payload["config_hash"] == config.config_hash
    assert payload["metrics"]["minimum_observation_days"] == 42
    assert payload["metrics"]["minimum_cycles_required"] == 30
    assert payload["metrics"]["minimum_incident_free_days"] == 14
    assert database.read_bytes() == before


def test_paper_gate_cli_exposes_no_evidence_or_threshold_override() -> None:
    result = CliRunner().invoke(
        app,
        ["paper", "gate", "--help"],
        env={"COLUMNS": "160"},
    )

    assert result.exit_code == 0
    assert "--database" in result.stdout
    for forbidden in (
        "--as-of",
        "--minimum",
        "--evidence",
        "--cycles",
        "--attestation",
        "--source",
    ):
        assert forbidden not in result.stdout
