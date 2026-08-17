from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType

import pytest
from typer.testing import CliRunner

import hyperlab.environment_authorization as authorization_module
from hyperlab.cli import app
from hyperlab.environment_authorization import (
    EnvironmentClass,
    EnvironmentReadinessManifest,
    EvidenceCheck,
    EvidenceVerificationContext,
    ReadinessArtifactBinding,
    ReadinessSubject,
    profile_for,
)


def _fixture_payload(
    check: EvidenceCheck,
    environment: EnvironmentClass,
    purpose: str,
    subject: ReadinessSubject,
    profile_sha256: str,
) -> dict[str, object]:
    return {
        "build_hash": subject.build_hash,
        "candidate_id": subject.candidate_id,
        "check": check.value,
        "config_hash": subject.config_hash,
        "environment": environment.value,
        "profile_sha256": profile_sha256,
        "purpose": purpose,
        "risk_limits_hash": subject.risk_limits_hash,
        "source_identity": subject.source_identity,
        "status": "PASS",
        "strategy_hash": subject.strategy_hash,
    }


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _fixture_verifier(context: EvidenceVerificationContext) -> bool:
    expected = _fixture_payload(
        context.check,
        context.environment,
        context.purpose.value,
        context.subject,
        context.profile_sha256,
    )
    return context.artifact_bytes == _canonical_bytes(expected)


def _install_fixture_verifiers(
    monkeypatch: pytest.MonkeyPatch,
    environment: EnvironmentClass,
) -> None:
    profile = profile_for(environment)
    registry = {
        (environment, profile.purpose, check): authorization_module._CompiledEvidenceVerifier(
            verifier_id=f"fixture:cli:{environment.value.casefold()}:{check.value.casefold()}",
            version=1,
            verify=_fixture_verifier,
        )
        for check in profile.required_checks
    }
    monkeypatch.setattr(
        authorization_module,
        "_COMPILED_EVIDENCE_VERIFIERS",
        MappingProxyType(registry),
    )


def _manifest(root: Path, environment: EnvironmentClass) -> EnvironmentReadinessManifest:
    profile = profile_for(environment)
    subject = ReadinessSubject(
        candidate_id="cli-readiness-fixture",
        config_hash="a" * 64,
        strategy_hash="b" * 64,
        build_hash="c" * 64,
        source_identity=f"source:{environment.value.casefold()}",
        risk_limits_hash="d" * 64,
    )
    evidence: dict[EvidenceCheck, ReadinessArtifactBinding] = {}
    for ordinal, check in enumerate(sorted(profile.required_checks, key=lambda item: item.value)):
        relative_path = f"evidence/{ordinal:02d}-{check.value.casefold()}.json"
        target = root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = _canonical_bytes(
            _fixture_payload(
                check,
                environment,
                profile.purpose.value,
                subject,
                profile.profile_sha256,
            )
        )
        target.write_bytes(payload)
        evidence[check] = ReadinessArtifactBinding(
            relative_path=relative_path,
            sha256=hashlib.sha256(payload).hexdigest(),
        )
    return EnvironmentReadinessManifest(
        schema_version=1,
        environment=environment,
        purpose=profile.purpose,
        environment_identity=environment.value,
        execution_network=profile.execution_network,
        credential_scope=profile.credential_scope,
        order_capability=profile.order_capability,
        subject=subject,
        evidence=evidence,
    )


def test_gate_model_requirements_separate_non_monetary_and_real_money_gates() -> None:
    runner = CliRunner()

    paper = runner.invoke(app, ["gate-model", "requirements", "PAPER"])
    testnet = runner.invoke(app, ["gate-model", "requirements", "TESTNET"])
    mainnet = runner.invoke(app, ["gate-model", "requirements", "MAINNET"])
    ambiguous = runner.invoke(app, ["gate-model", "requirements", "paper"])

    assert paper.exit_code == testnet.exit_code == mainnet.exit_code == 0
    paper_checks = set(json.loads(paper.stdout)["required_checks"])
    testnet_checks = set(json.loads(testnet.stdout)["required_checks"])
    mainnet_payload = json.loads(mainnet.stdout)
    mainnet_checks = set(mainnet_payload["required_checks"])
    economic = {
        "GATE_B_EVIDENCE",
        "GATE_C_EVIDENCE",
        "GATE_D_FORWARD_PAPER_EVIDENCE",
    }
    assert paper_checks.isdisjoint(economic)
    assert testnet_checks.isdisjoint(economic)
    assert {
        "GATE_B_EVIDENCE",
        "GATE_C_EVIDENCE",
        "GATE_D_FORWARD_PAPER_EVIDENCE",
        "GATE_E_TESTNET_EVIDENCE",
        "GATE_F_MICRO_MAINNET_EVIDENCE",
        "HUMAN_APPROVAL",
        "DUAL_HUMAN_APPROVAL",
    } <= mainnet_checks
    assert mainnet_payload["real_money_execution_enabled_in_build"] is False
    assert mainnet_payload["semantic_verifiers"]["complete"] is False
    assert ambiguous.exit_code == 2


def test_gate_model_check_issues_only_a_ready_paper_receipt_and_is_read_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fixture_verifiers(monkeypatch, EnvironmentClass.PAPER)
    manifest = _manifest(tmp_path, EnvironmentClass.PAPER)
    manifest_path = tmp_path / "readiness.json"
    manifest_path.write_bytes(manifest.canonical_json_bytes())
    before = {
        path.relative_to(tmp_path).as_posix(): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    result = CliRunner().invoke(
        app,
        [
            "gate-model",
            "check",
            str(manifest_path),
            "--evidence-root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "READY"
    assert payload["ready"] is True
    assert payload["environment"] == "PAPER"
    assert payload["purpose"] == "PAPER_RUNTIME"
    assert payload["authorizes_real_money"] is False
    assert payload["receipt"]["environment"] == "PAPER"
    assert payload["receipt"]["purpose"] == "PAPER_RUNTIME"
    assert payload["receipt"]["authorizes_real_money"] is False
    assert payload["receipt"]["receipt_sha256"]
    after = {
        path.relative_to(tmp_path).as_posix(): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    assert after == before


def test_gate_model_check_rejects_self_asserted_files_without_compiled_verifiers(
    tmp_path: Path,
) -> None:
    assert not any(
        environment is EnvironmentClass.PAPER
        for environment, _purpose, _check in authorization_module._COMPILED_EVIDENCE_VERIFIERS
    )
    manifest = _manifest(tmp_path, EnvironmentClass.PAPER)
    manifest_path = tmp_path / "self-asserted-readiness.json"
    manifest_path.write_bytes(manifest.canonical_json_bytes())

    result = CliRunner().invoke(
        app,
        [
            "gate-model",
            "check",
            str(manifest_path),
            "--evidence-root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 2, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "BLOCKED"
    assert payload["semantic_verifiers"]["complete"] is False
    assert "receipt" not in payload
    assert {
        blocker["code"] for blocker in payload["blockers"]
    } == {"NO_COMPILED_EVIDENCE_VERIFIER"}


def test_gate_model_check_blocks_malformed_environment_identity_without_receipt(
    tmp_path: Path,
) -> None:
    manifest = replace(
        _manifest(tmp_path, EnvironmentClass.PAPER),
        environment_identity="PAPER/MAINNET",
    )
    manifest_path = tmp_path / "ambiguous-readiness.json"
    manifest_path.write_bytes(manifest.canonical_json_bytes())
    before = manifest_path.read_bytes()

    result = CliRunner().invoke(
        app,
        [
            "gate-model",
            "check",
            str(manifest_path),
            "--evidence-root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 2, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "BLOCKED"
    assert payload["ready"] is False
    assert "receipt" not in payload
    assert "ENVIRONMENT_IDENTITY_MISMATCH" in {
        blocker["code"] for blocker in payload["blockers"]
    }
    assert manifest_path.read_bytes() == before
