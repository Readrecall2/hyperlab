from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

import hyperlab.cli as cli_module
from hyperlab.backtest.protocol import canonical_json
from hyperlab.cli import app
from hyperlab.paper.models import PaperExecutionConfig, PaperRiskLimits, PaperRunConfig


def _config() -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="cash_and_carry",
        strategy_hash="a" * 64,
        parameters={"version": 1},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(),
        risk=PaperRiskLimits(),
        seed=12,
        initial_cash=Decimal("100000"),
        validation_started_at=datetime(2026, 8, 16, tzinfo=UTC),
    )


def test_registered_runtime_rechecks_byte_admission_before_factories_or_store(
    tmp_path: Path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    config = _config()
    config_artifact = tmp_path / "paper-config.json"
    config_artifact.write_text(
        canonical_json(config.to_dict()),
        encoding="utf-8",
    )
    database = tmp_path / "must-not-exist.sqlite3"
    calls: list[str] = []

    def strategy_factory(_config: PaperRunConfig):  # type: ignore[no-untyped-def]
        calls.append("strategy")
        raise AssertionError("strategy factory must not run")

    def source_factory(_config: PaperRunConfig):  # type: ignore[no-untyped-def]
        calls.append("source")
        raise AssertionError("source factory must not run")

    approval = cli_module._ApprovedPaperRuntimeFactories(
        candidate_id="cash_and_carry",
        config_hash=config.config_hash,
        admission_manifest_path=tmp_path / "missing-admission.json",
        admission_manifest_sha256="c" * 64,
        admission_evidence_root=tmp_path,
        strategy_factory=strategy_factory,
        source_factory=source_factory,
    )
    monkeypatch.setattr(
        cli_module,
        "_APPROVED_PAPER_RUNTIMES",
        {config.config_hash: approval},
    )

    result = CliRunner().invoke(
        app,
        ["paper", "run", str(config_artifact), "--database", str(database)],
    )

    assert result.exit_code == 2, result.output
    assert "manifeste d'admission" in result.output
    assert calls == []
    assert not database.exists()


def test_paper_run_rejects_noncanonical_or_duplicate_config_bytes(tmp_path: Path) -> None:
    config = _config()
    canonical = canonical_json(config.to_dict())
    database = tmp_path / "must-not-exist.sqlite3"

    pretty_artifact = tmp_path / "pretty.json"
    pretty_artifact.write_text(json.dumps(config.to_dict(), indent=2), encoding="utf-8")
    pretty = CliRunner().invoke(
        app,
        ["paper", "run", str(pretty_artifact), "--database", str(database)],
    )
    assert pretty.exit_code == 2
    assert "snapshot canonique complet" in pretty.output

    duplicate_artifact = tmp_path / "duplicate.json"
    duplicate_artifact.write_text(
        canonical.replace("{", '{"seed":12,', 1),
        encoding="utf-8",
    )
    duplicate = CliRunner().invoke(
        app,
        ["paper", "run", str(duplicate_artifact), "--database", str(database)],
    )
    assert duplicate.exit_code == 2
    assert "duplicate JSON key" in duplicate.output
    assert not database.exists()


def test_store_initialization_precedes_factories(
    tmp_path: Path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    config = _config()
    config_artifact = tmp_path / "paper-config.json"
    config_artifact.write_text(canonical_json(config.to_dict()), encoding="utf-8")
    calls: list[str] = []

    def strategy_factory(_config: PaperRunConfig):  # type: ignore[no-untyped-def]
        calls.append("strategy")
        raise AssertionError("strategy factory must not run")

    def source_factory(_config: PaperRunConfig):  # type: ignore[no-untyped-def]
        calls.append("source")
        raise AssertionError("source factory must not run")

    class FailingStore:
        def __init__(self, _path: Path) -> None:
            calls.append("store")
            raise RuntimeError("store init failed")

    approval = cli_module._ApprovedPaperRuntimeFactories(
        candidate_id="cash_and_carry",
        config_hash=config.config_hash,
        admission_manifest_path=tmp_path / "unused-admission.json",
        admission_manifest_sha256="c" * 64,
        admission_evidence_root=tmp_path,
        strategy_factory=strategy_factory,
        source_factory=source_factory,
    )
    monkeypatch.setattr(
        cli_module,
        "_APPROVED_PAPER_RUNTIMES",
        {config.config_hash: approval},
    )
    monkeypatch.setattr(cli_module, "_verify_approved_paper_admission", lambda *_args: None)
    monkeypatch.setattr("hyperlab.paper.store.PaperStore", FailingStore)

    result = CliRunner().invoke(
        app,
        [
            "paper",
            "run",
            str(config_artifact),
            "--database",
            str(tmp_path / "paper.sqlite3"),
        ],
    )

    assert result.exit_code == 1
    assert calls == ["store"]


def test_production_semantic_registry_is_empty_and_non_authorizing() -> None:
    assert dict(cli_module._TRUSTED_PAPER_SEMANTIC_EVALUATORS) == {}
    assert cli_module._production_semantic_admission_blockers("cash_and_carry") == (
        "NO_TRUSTED_CANDIDATE_SEMANTIC_EVALUATOR",
    )
    assert (
        "semantic_evidence_verifier"
        not in cli_module._ApprovedPaperRuntimeFactories.__dataclass_fields__
    )
