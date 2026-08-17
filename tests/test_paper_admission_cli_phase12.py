from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType

import pytest
from typer.testing import CliRunner

import hyperlab.cli as cli_module
import hyperlab.environment_authorization as authorization_module
from hyperlab.backtest.costs import CostRule, CostSchedule, SlippageModel
from hyperlab.backtest.protocol import canonical_json, canonical_sha256
from hyperlab.cli import app
from hyperlab.environment_authorization import (
    EnvironmentClass,
    EnvironmentReadinessManifest,
    EvidenceCheck,
    EvidenceVerificationContext,
    ReadinessArtifactBinding,
    ReadinessSubject,
    profile_for,
    verify_environment_readiness,
)
from hyperlab.paper.models import PaperExecutionConfig, PaperRiskLimits, PaperRunConfig

_START = datetime(2026, 8, 16, tzinfo=UTC)
_INSTRUMENT = "HYPERLIQUID:BTC:perp"
_SOURCE_IDENTITY = "hyperliquid-public-normalized-v2"


def _semantic_payload(context: EvidenceVerificationContext) -> dict[str, object]:
    return {
        "build_hash": context.subject.build_hash,
        "candidate_id": context.subject.candidate_id,
        "check": context.check.value,
        "config_hash": context.subject.config_hash,
        "environment": context.environment.value,
        "profile_sha256": context.profile_sha256,
        "purpose": context.purpose.value,
        "risk_limits_hash": context.subject.risk_limits_hash,
        "source_identity": context.subject.source_identity,
        "status": "PASS",
        "strategy_hash": context.subject.strategy_hash,
    }


def _paper_fixture_verifier(context: EvidenceVerificationContext) -> bool:
    try:
        payload = json.loads(context.artifact_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    if context.check is EvidenceCheck.FROZEN_STRATEGY_CONFIG:
        try:
            config = PaperRunConfig.from_dict(payload)
        except (KeyError, TypeError, ValueError):
            return False
        return (
            context.artifact_bytes == canonical_json(config.to_dict()).encode()
            and config.config_hash == context.subject.config_hash
            and config.strategy_hash == context.subject.strategy_hash
            and config.environment == context.environment.value
        )
    if context.check is EvidenceCheck.PUBLIC_MARKET_SOURCE:
        return (
            context.artifact_bytes == b'{"public_only":true,"schema_version":2}'
            and payload == {"public_only": True, "schema_version": 2}
            and context.subject.source_identity == _SOURCE_IDENTITY
        )
    expected = _semantic_payload(context)
    return (
        payload == expected
        and context.artifact_bytes == canonical_json(expected).encode()
    )


def _install_paper_fixture_verifiers(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    profile = profile_for(EnvironmentClass.PAPER)
    registry = {
        (
            EnvironmentClass.PAPER,
            profile.purpose,
            check,
        ): authorization_module._CompiledEvidenceVerifier(
            verifier_id=f"fixture:paper-cli:{check.value.casefold()}",
            version=1,
            verify=_paper_fixture_verifier,
        )
        for check in profile.required_checks
    }
    monkeypatch.setattr(
        authorization_module,
        "_COMPILED_EVIDENCE_VERIFIERS",
        MappingProxyType(registry),
    )


def _config(
    root: Path,
    *,
    runtime_timer_interval_seconds: float = 1.0,
    runtime_source_poll_timeout_seconds: float = 0.25,
) -> PaperRunConfig:
    source_payload = b'{"public_only":true,"schema_version":2}'
    (root / "identity").mkdir(parents=True, exist_ok=True)
    (root / "identity/public-source.json").write_bytes(source_payload)
    return PaperRunConfig(
        strategy_name="cash_and_carry",
        strategy_hash="a" * 64,
        parameters={"version": 1},
        data_hash=hashlib.sha256(source_payload).hexdigest(),
        execution=PaperExecutionConfig(
            maker_fee_bps=Decimal("1.5"),
            taker_fee_bps=Decimal("4.5"),
            source="public-conservative-tier0",
            cost_schedule=CostSchedule(
                rules=(
                    CostRule(
                        instrument="HYPERLIQUID:*:perp",
                        maker_fee_bps=1.5,
                        taker_fee_bps=4.5,
                        slippage=SlippageModel(base_bps=1.0, max_participation=0.1),
                        effective_from=_START,
                        source="public-conservative-tier0",
                    ),
                ),
                calibration_status="UNCALIBRATED",
            ),
        ),
        risk=PaperRiskLimits(),
        seed=12,
        initial_cash=Decimal("100000"),
        validation_started_at=_START,
        run_kind="TECHNICAL",
        data_source=_SOURCE_IDENTITY,
        required_instruments=(_INSTRUMENT,),
        runtime_timer_interval_seconds=runtime_timer_interval_seconds,
        runtime_source_poll_timeout_seconds=runtime_source_poll_timeout_seconds,
        minimum_validation_cycles=1,
    )

def _readiness_fixture(
    root: Path,
    *,
    runtime_timer_interval_seconds: float = 1.0,
    runtime_source_poll_timeout_seconds: float = 0.25,
) -> tuple[PaperRunConfig, Path, EnvironmentReadinessManifest, Path]:
    config = _config(
        root,
        runtime_timer_interval_seconds=runtime_timer_interval_seconds,
        runtime_source_poll_timeout_seconds=runtime_source_poll_timeout_seconds,
    )
    config_artifact = root / "paper-config.json"
    config_artifact.write_text(canonical_json(config.to_dict()), encoding="utf-8")
    profile = profile_for(EnvironmentClass.PAPER)
    evidence: dict[EvidenceCheck, ReadinessArtifactBinding] = {}
    for ordinal, check in enumerate(sorted(profile.required_checks, key=lambda item: item.value)):
        if check is EvidenceCheck.FROZEN_STRATEGY_CONFIG:
            target = config_artifact
        elif check is EvidenceCheck.PUBLIC_MARKET_SOURCE:
            target = root / "identity/public-source.json"
        else:
            target = root / f"readiness/{ordinal:02d}-{check.value.casefold()}.json"
            target.parent.mkdir(parents=True, exist_ok=True)
            context = EvidenceVerificationContext(
                check=check,
                environment=EnvironmentClass.PAPER,
                purpose=profile.purpose,
                environment_identity="PAPER",
                execution_network=profile.execution_network,
                credential_scope=profile.credential_scope,
                order_capability=profile.order_capability,
                subject=ReadinessSubject(
                    candidate_id="cash_and_carry",
                    config_hash=config.config_hash,
                    strategy_hash=config.strategy_hash,
                    build_hash=config.engine_build_hash,
                    source_identity=config.data_source,
                    risk_limits_hash=canonical_sha256(config.risk.to_dict()),
                ),
                profile_sha256=profile.profile_sha256,
                artifact_bytes=b"",
            )
            target.write_bytes(canonical_json(_semantic_payload(context)).encode())
        evidence[check] = ReadinessArtifactBinding(
            relative_path=target.relative_to(root).as_posix(),
            sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
        )
    manifest = EnvironmentReadinessManifest(
        schema_version=1,
        environment=EnvironmentClass.PAPER,
        purpose=profile.purpose,
        environment_identity="PAPER",
        execution_network=profile.execution_network,
        credential_scope=profile.credential_scope,
        order_capability=profile.order_capability,
        subject=ReadinessSubject(
            candidate_id="cash_and_carry",
            config_hash=config.config_hash,
            strategy_hash=config.strategy_hash,
            build_hash=config.engine_build_hash,
            source_identity=config.data_source,
            risk_limits_hash=canonical_sha256(config.risk.to_dict()),
        ),
        evidence=evidence,
    )
    manifest_path = root / "paper-readiness.json"
    manifest_path.write_bytes(manifest.canonical_json_bytes())
    return config, config_artifact, manifest, manifest_path


def test_registered_runtime_rechecks_technical_readiness_before_factories_or_store(
    tmp_path: Path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    config = _config(tmp_path)
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
        config_artifact_path=config_artifact,
        readiness_manifest_path=tmp_path / "missing-readiness.json",
        readiness_manifest_sha256="c" * 64,
        readiness_profile_sha256="d" * 64,
        readiness_evidence_root=tmp_path,
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
    assert "Readiness paper illisible" in result.output
    assert calls == []
    assert not database.exists()


def test_paper_run_rejects_noncanonical_or_duplicate_config_bytes(tmp_path: Path) -> None:
    config = _config(tmp_path)
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


def test_valid_technical_readiness_reaches_store_and_factory_without_gate_b_c_d(
    tmp_path: Path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    _install_paper_fixture_verifiers(monkeypatch)
    config, config_artifact, manifest, manifest_path = _readiness_fixture(tmp_path)
    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)
    assert decision.ready
    calls: list[str] = []

    def strategy_factory(_config: PaperRunConfig):  # type: ignore[no-untyped-def]
        calls.append("strategy")
        raise RuntimeError("stop after proving the technical factory boundary")

    def source_factory(_config: PaperRunConfig):  # type: ignore[no-untyped-def]
        calls.append("source")
        raise AssertionError("source factory must not run")

    class RecordingStore:
        def __init__(self, _path: Path) -> None:
            calls.append("store")

        def close(self) -> None:
            calls.append("close")

    approval = cli_module._ApprovedPaperRuntimeFactories(
        candidate_id="cash_and_carry",
        config_hash=config.config_hash,
        config_artifact_path=config_artifact,
        readiness_manifest_path=manifest_path,
        readiness_manifest_sha256=manifest.manifest_sha256,
        readiness_profile_sha256=decision.profile_sha256,
        readiness_evidence_root=tmp_path,
        strategy_factory=strategy_factory,
        source_factory=source_factory,
    )
    monkeypatch.setattr(
        cli_module,
        "_APPROVED_PAPER_RUNTIMES",
        {config.config_hash: approval},
    )
    monkeypatch.setattr(
        cli_module,
        "_production_semantic_admission_blockers",
        lambda _candidate_id: (_ for _ in ()).throw(
            AssertionError("Gate B/C semantic admission must not be consulted")
        ),
    )
    monkeypatch.setattr("hyperlab.paper.store.PaperStore", RecordingStore)

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
    assert isinstance(result.exception, RuntimeError)
    assert config.run_kind == "TECHNICAL"
    assert config.economic_prerequisites_satisfied is False
    assert config.economic_prerequisites_evidence_hash is None
    assert calls == ["store", "strategy", "close"]




@pytest.mark.parametrize(
    ("timer_interval_seconds", "poll_timeout_seconds"),
    [(2.0, 0.25), (1.0, 0.5)],
)
def test_swapped_frozen_cadence_blocks_before_store_or_factories(
    tmp_path: Path,
    monkeypatch,
    timer_interval_seconds: float,
    poll_timeout_seconds: float,
) -> None:  # type: ignore[no-untyped-def]
    _install_paper_fixture_verifiers(monkeypatch)
    config, config_artifact, manifest, manifest_path = _readiness_fixture(
        tmp_path,
        runtime_timer_interval_seconds=timer_interval_seconds,
        runtime_source_poll_timeout_seconds=poll_timeout_seconds,
    )
    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)
    assert decision.ready
    calls: list[str] = []
    database = tmp_path / "must-not-exist.sqlite3"

    def strategy_factory(_config: PaperRunConfig):  # type: ignore[no-untyped-def]
        calls.append("strategy")
        raise AssertionError("strategy factory must not run")

    def source_factory(_config: PaperRunConfig):  # type: ignore[no-untyped-def]
        calls.append("source")
        raise AssertionError("source factory must not run")

    class ForbiddenStore:
        def __init__(self, _path: Path) -> None:
            calls.append("store")
            raise AssertionError("store must not open")

    approval = cli_module._ApprovedPaperRuntimeFactories(
        candidate_id="cash_and_carry",
        config_hash=config.config_hash,
        config_artifact_path=config_artifact,
        readiness_manifest_path=manifest_path,
        readiness_manifest_sha256=manifest.manifest_sha256,
        readiness_profile_sha256=decision.profile_sha256,
        readiness_evidence_root=tmp_path,
        strategy_factory=strategy_factory,
        source_factory=source_factory,
    )
    monkeypatch.setattr(
        cli_module,
        "_APPROVED_PAPER_RUNTIMES",
        {config.config_hash: approval},
    )
    monkeypatch.setattr("hyperlab.paper.store.PaperStore", ForbiddenStore)

    result = CliRunner().invoke(
        app,
        [
            "paper",
            "run",
            str(config_artifact),
            "--database",
            str(database),
        ],
    )

    assert result.exit_code == 2, result.output
    assert "paper runtime cadence differs" in result.output
    assert calls == []
    assert not database.exists()
def test_stale_approved_readiness_profile_blocks_before_store_or_factories(
    tmp_path: Path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    _install_paper_fixture_verifiers(monkeypatch)
    config, config_artifact, manifest, manifest_path = _readiness_fixture(tmp_path)
    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)
    assert decision.ready
    stale_profile_sha256 = "f" * 64
    assert stale_profile_sha256 != decision.profile_sha256
    calls: list[str] = []
    database = tmp_path / "must-not-exist.sqlite3"

    def strategy_factory(_config: PaperRunConfig):  # type: ignore[no-untyped-def]
        calls.append("strategy")
        raise AssertionError("strategy factory must not run")

    def source_factory(_config: PaperRunConfig):  # type: ignore[no-untyped-def]
        calls.append("source")
        raise AssertionError("source factory must not run")

    class ForbiddenStore:
        def __init__(self, _path: Path) -> None:
            calls.append("store")
            raise AssertionError("store must not open")

    approval = cli_module._ApprovedPaperRuntimeFactories(
        candidate_id="cash_and_carry",
        config_hash=config.config_hash,
        config_artifact_path=config_artifact,
        readiness_manifest_path=manifest_path,
        readiness_manifest_sha256=manifest.manifest_sha256,
        readiness_profile_sha256=stale_profile_sha256,
        readiness_evidence_root=tmp_path,
        strategy_factory=strategy_factory,
        source_factory=source_factory,
    )
    monkeypatch.setattr(
        cli_module,
        "_APPROVED_PAPER_RUNTIMES",
        {config.config_hash: approval},
    )
    monkeypatch.setattr("hyperlab.paper.store.PaperStore", ForbiddenStore)

    result = CliRunner().invoke(
        app,
        ["paper", "run", str(config_artifact), "--database", str(database)],
    )

    assert result.exit_code == 2, result.output
    assert "profil readiness paper" in result.output
    assert calls == []
    assert not database.exists()


def test_production_semantic_registry_is_empty_and_non_authorizing() -> None:
    assert dict(cli_module._TRUSTED_PAPER_SEMANTIC_EVALUATORS) == {}
    assert cli_module._production_semantic_admission_blockers("cash_and_carry") == (
        "NO_TRUSTED_CANDIDATE_SEMANTIC_EVALUATOR",
    )
    assert (
        "semantic_evidence_verifier"
        not in cli_module._ApprovedPaperRuntimeFactories.__dataclass_fields__
    )
    fields = cli_module._ApprovedPaperRuntimeFactories.__dataclass_fields__
    assert {
        "readiness_manifest_path",
        "readiness_manifest_sha256",
        "readiness_profile_sha256",
        "readiness_evidence_root",
    } <= fields.keys()
    assert not {
        "admission_manifest_path",
        "admission_manifest_sha256",
        "admission_evidence_root",
    } & fields.keys()
