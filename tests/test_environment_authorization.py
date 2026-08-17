from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType

import pytest

import hyperlab.environment_authorization as authorization_module
from hyperlab.environment_authorization import (
    AuthorizationManifestError,
    AuthorizationPurpose,
    CredentialScope,
    EnvironmentClass,
    EnvironmentReadinessManifest,
    EvidenceCheck,
    EvidenceVerificationContext,
    ExecutionNetwork,
    OrderCapability,
    ReadinessArtifactBinding,
    ReadinessSubject,
    issue_environment_receipt,
    profile_for,
    receipt_scope_blockers,
    verify_environment_readiness,
)


def _evidence_document(
    check: EvidenceCheck,
    manifest_environment: EnvironmentClass,
    purpose: AuthorizationPurpose,
    execution_network: ExecutionNetwork,
    credential_scope: CredentialScope,
    order_capability: OrderCapability,
    subject: ReadinessSubject,
    profile_sha256: str,
) -> dict[str, object]:
    return {
        "build_hash": subject.build_hash,
        "candidate_id": subject.candidate_id,
        "check": check.value,
        "config_hash": subject.config_hash,
        "credential_scope": credential_scope.value,
        "environment": manifest_environment.value,
        "environment_identity": manifest_environment.value,
        "execution_network": execution_network.value,
        "order_capability": order_capability.value,
        "profile_sha256": profile_sha256,
        "purpose": purpose.value,
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
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _fixture_semantic_verifier(context: EvidenceVerificationContext) -> bool:
    expected = _evidence_document(
        context.check,
        context.environment,
        context.purpose,
        context.execution_network,
        context.credential_scope,
        context.order_capability,
        context.subject,
        context.profile_sha256,
    )
    try:
        decoded = json.loads(context.artifact_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    return (
        decoded == expected
        and context.artifact_bytes == _canonical_bytes(expected)
    )


def _install_fixture_verifiers(
    monkeypatch: pytest.MonkeyPatch,
    environment: EnvironmentClass,
) -> None:
    profile = profile_for(environment)
    registry = {
        (environment, profile.purpose, check): authorization_module._CompiledEvidenceVerifier(
            verifier_id=f"fixture:{environment.value.casefold()}:{check.value.casefold()}",
            version=1,
            verify=_fixture_semantic_verifier,
        )
        for check in profile.required_checks
    }
    monkeypatch.setattr(
        authorization_module,
        "_COMPILED_EVIDENCE_VERIFIERS",
        MappingProxyType(registry),
    )


def _replace_fixture_verifier(
    monkeypatch: pytest.MonkeyPatch,
    environment: EnvironmentClass,
    check: EvidenceCheck,
    verify: Callable[[EvidenceVerificationContext], bool],
    *,
    version: int | None = None,
) -> None:
    profile = profile_for(environment)
    scope = (environment, profile.purpose, check)
    registry = dict(authorization_module._COMPILED_EVIDENCE_VERIFIERS)
    previous = registry[scope]
    registry[scope] = authorization_module._CompiledEvidenceVerifier(
        verifier_id=previous.verifier_id,
        version=previous.version if version is None else version,
        verify=verify,
    )
    monkeypatch.setattr(
        authorization_module,
        "_COMPILED_EVIDENCE_VERIFIERS",
        MappingProxyType(registry),
    )


def _raising_semantic_verifier(_context: EvidenceVerificationContext) -> bool:
    raise RuntimeError("fixture verifier failure")


def _manifest(
    root: Path,
    environment: EnvironmentClass,
    *,
    missing: frozenset[EvidenceCheck] = frozenset(),
) -> EnvironmentReadinessManifest:
    profile = profile_for(environment)
    subject = ReadinessSubject(
        candidate_id="cash_and_carry",
        config_hash="a" * 64,
        strategy_hash="b" * 64,
        build_hash="c" * 64,
        source_identity=f"source:{environment.value.casefold()}",
        risk_limits_hash="d" * 64,
    )
    bindings: dict[EvidenceCheck, ReadinessArtifactBinding] = {}
    for ordinal, check in enumerate(sorted(profile.required_checks, key=str)):
        if check in missing:
            continue
        relative_path = f"evidence/{ordinal:02d}-{check.value.casefold()}.json"
        payload = _canonical_bytes(
            _evidence_document(
                check,
                environment,
                profile.purpose,
                profile.execution_network,
                profile.credential_scope,
                profile.order_capability,
                subject,
                profile.profile_sha256,
            )
        )
        target = root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        bindings[check] = ReadinessArtifactBinding.from_bytes(relative_path, payload)
    return EnvironmentReadinessManifest(
        schema_version=1,
        environment=environment,
        purpose=profile.purpose,
        environment_identity=environment.value,
        execution_network=profile.execution_network,
        credential_scope=profile.credential_scope,
        order_capability=profile.order_capability,
        subject=subject,
        evidence=bindings,
    )


def test_paper_is_ready_without_economic_gates_and_receipt_cannot_escalate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fixture_verifiers(monkeypatch, EnvironmentClass.PAPER)
    manifest = _manifest(tmp_path, EnvironmentClass.PAPER)

    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)

    assert decision.ready
    assert not {
        EvidenceCheck.GATE_B_EVIDENCE,
        EvidenceCheck.GATE_C_EVIDENCE,
        EvidenceCheck.GATE_D_FORWARD_PAPER_EVIDENCE,
    } & profile_for(EnvironmentClass.PAPER).required_checks
    receipt = issue_environment_receipt(decision)
    assert receipt.order_capability is OrderCapability.SIMULATED_ONLY
    assert receipt.execution_network is ExecutionNetwork.NONE
    assert receipt.credential_scope is CredentialScope.NONE
    assert receipt.authorizes_real_money is False
    assert receipt_scope_blockers(
        receipt,
        environment=EnvironmentClass.PAPER,
        purpose=AuthorizationPurpose.PAPER_RUNTIME,
        config_hash=manifest.subject.config_hash,
    ) == ()
    assert {
        blocker.code
        for blocker in receipt_scope_blockers(
            receipt,
            environment=EnvironmentClass.MAINNET,
            purpose=AuthorizationPurpose.MAINNET_EXECUTION,
            config_hash=manifest.subject.config_hash,
        )
    } >= {"ENVIRONMENT_SCOPE_MISMATCH", "PURPOSE_SCOPE_MISMATCH"}


def test_testnet_is_ready_without_gate_d_but_mainnet_scope_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fixture_verifiers(monkeypatch, EnvironmentClass.TESTNET)
    manifest = _manifest(tmp_path, EnvironmentClass.TESTNET)

    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)

    assert decision.ready
    assert EvidenceCheck.GATE_D_FORWARD_PAPER_EVIDENCE not in profile_for(
        EnvironmentClass.TESTNET
    ).required_checks
    assert {
        EvidenceCheck.EXPLICIT_TESTNET_ENDPOINT,
        EvidenceCheck.TESTNET_CONFIG_NAMESPACE,
        EvidenceCheck.NO_MAINNET_FALLBACK,
    } <= profile_for(EnvironmentClass.TESTNET).required_checks
    wrong_network = verify_environment_readiness(
        replace(manifest, execution_network=ExecutionNetwork.MAINNET),
        evidence_root=tmp_path,
    )
    assert not wrong_network.ready
    assert "EXECUTION_NETWORK_MISMATCH" in {blocker.code for blocker in wrong_network.blockers}
    wrong_scope = verify_environment_readiness(
        replace(manifest, credential_scope=CredentialScope.MAINNET),
        evidence_root=tmp_path,
    )
    assert not wrong_scope.ready
    assert "CREDENTIAL_SCOPE_MISMATCH" in {blocker.code for blocker in wrong_scope.blockers}
    receipt = issue_environment_receipt(decision)
    escalation_codes = {
        blocker.code
        for blocker in receipt_scope_blockers(
            receipt,
            environment=EnvironmentClass.MAINNET,
            purpose=AuthorizationPurpose.MAINNET_EXECUTION,
            config_hash=manifest.subject.config_hash,
        )
    }
    assert {
        "ENVIRONMENT_SCOPE_MISMATCH",
        "PURPOSE_SCOPE_MISMATCH",
        "REAL_MONEY_EXECUTION_DISABLED_IN_BUILD",
    } <= escalation_codes


def test_malformed_or_ambiguous_environment_identity_is_rejected(tmp_path: Path) -> None:
    payload = _manifest(tmp_path, EnvironmentClass.TESTNET).to_dict()
    payload["environment"] = "TESTNET/MAINNET"

    with pytest.raises(AuthorizationManifestError, match="environment"):
        EnvironmentReadinessManifest.from_mapping(payload)

    payload = _manifest(tmp_path / "other", EnvironmentClass.TESTNET).to_dict()
    payload["environment_identity"] = "MAINNET"
    manifest = EnvironmentReadinessManifest.from_mapping(payload)
    result = verify_environment_readiness(manifest, evidence_root=tmp_path / "other")
    assert not result.ready
    assert "ENVIRONMENT_IDENTITY_MISMATCH" in {blocker.code for blocker in result.blockers}


def test_manifest_bytes_reject_duplicate_keys_and_noncanonical_encoding(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, EnvironmentClass.PAPER)
    canonical = manifest.canonical_json_bytes()

    duplicate = canonical.replace(b"{", b'{"schema_version":1,', 1)
    with pytest.raises(AuthorizationManifestError, match="duplicate JSON key"):
        EnvironmentReadinessManifest.from_json_bytes(duplicate)

    pretty = json.dumps(manifest.to_dict(), indent=2, sort_keys=True).encode()
    with pytest.raises(AuthorizationManifestError, match="canonical"):
        EnvironmentReadinessManifest.from_json_bytes(pretty)


def test_real_money_tiers_require_all_economic_operational_and_human_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fixture_verifiers(monkeypatch, EnvironmentClass.MAINNET)
    required = profile_for(EnvironmentClass.MAINNET).required_checks
    missing = frozenset(
        {
            EvidenceCheck.GATE_B_EVIDENCE,
            EvidenceCheck.GATE_C_EVIDENCE,
            EvidenceCheck.GATE_D_FORWARD_PAPER_EVIDENCE,
            EvidenceCheck.GATE_E_TESTNET_EVIDENCE,
            EvidenceCheck.GATE_F_MICRO_MAINNET_EVIDENCE,
            EvidenceCheck.HUMAN_APPROVAL,
            EvidenceCheck.DUAL_HUMAN_APPROVAL,
            EvidenceCheck.SIGNER_ISOLATION,
            EvidenceCheck.SECRET_HANDLING,
            EvidenceCheck.SIGNED_CONFIGURATION,
            EvidenceCheck.KILL_SWITCH,
            EvidenceCheck.LOSS_DRAWDOWN_LIMITS,
            EvidenceCheck.RECONCILIATION,
        }
    )
    assert missing <= required
    manifest = _manifest(tmp_path, EnvironmentClass.MAINNET, missing=missing)

    result = verify_environment_readiness(manifest, evidence_root=tmp_path)

    assert not result.ready
    missing_locations = {
        blocker.location
        for blocker in result.blockers
        if blocker.code == "MISSING_REQUIRED_EVIDENCE"
    }
    assert {f"evidence.{check.value}" for check in missing} <= missing_locations
    assert "REAL_MONEY_EXECUTION_DISABLED_IN_BUILD" in {
        blocker.code for blocker in result.blockers
    }


def test_receipt_is_bound_to_exact_config_and_current_requirement_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fixture_verifiers(monkeypatch, EnvironmentClass.PAPER)
    decision = verify_environment_readiness(
        _manifest(tmp_path, EnvironmentClass.PAPER),
        evidence_root=tmp_path,
    )
    receipt = issue_environment_receipt(decision)

    blockers = receipt_scope_blockers(
        receipt,
        environment=EnvironmentClass.PAPER,
        purpose=AuthorizationPurpose.PAPER_RUNTIME,
        config_hash="f" * 64,
    )

    assert {blocker.code for blocker in blockers} == {"CONFIG_SCOPE_MISMATCH"}


def test_byte_bound_files_are_blocked_without_compiled_semantic_verifiers(
    tmp_path: Path,
) -> None:
    assert not any(
        environment is EnvironmentClass.PAPER
        for environment, _purpose, _check in authorization_module._COMPILED_EVIDENCE_VERIFIERS
    )
    manifest = _manifest(tmp_path, EnvironmentClass.PAPER)
    arbitrary_evidence: dict[EvidenceCheck, ReadinessArtifactBinding] = {}
    for check, binding in manifest.evidence.items():
        arbitrary_payload = _canonical_bytes(
            {"check": check.value, "reviewed": True}
        )
        (tmp_path / binding.relative_path).write_bytes(arbitrary_payload)
        arbitrary_evidence[check] = ReadinessArtifactBinding.from_bytes(
            binding.relative_path,
            arbitrary_payload,
        )
    manifest = replace(manifest, evidence=arbitrary_evidence)

    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)

    assert not decision.ready
    assert {blocker.code for blocker in decision.blockers} == {
        "NO_COMPILED_EVIDENCE_VERIFIER"
    }
    no_verifier_locations = {
        blocker.location
        for blocker in decision.blockers
        if blocker.code == "NO_COMPILED_EVIDENCE_VERIFIER"
    }
    assert no_verifier_locations == {
        f"evidence.{check.value}"
        for check in profile_for(EnvironmentClass.PAPER).required_checks
    }
    with pytest.raises(ValueError, match="blocked readiness decision"):
        issue_environment_receipt(decision)


def test_verifiers_are_bound_to_exact_environment_and_purpose(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fixture_verifiers(monkeypatch, EnvironmentClass.PAPER)
    manifest = _manifest(tmp_path, EnvironmentClass.TESTNET)

    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)

    assert not decision.ready
    assert {
        blocker.location
        for blocker in decision.blockers
        if blocker.code == "NO_COMPILED_EVIDENCE_VERIFIER"
    } == {
        f"evidence.{check.value}"
        for check in profile_for(EnvironmentClass.TESTNET).required_checks
    }


def test_semantic_verifier_rejects_hash_bound_but_wrong_document(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = EnvironmentClass.PAPER
    _install_fixture_verifiers(monkeypatch, environment)
    manifest = _manifest(tmp_path, environment)
    check = min(profile_for(environment).required_checks, key=lambda item: item.value)
    binding = manifest.evidence[check]
    path = tmp_path / binding.relative_path
    wrong_document = json.loads(path.read_text(encoding="utf-8"))
    wrong_document["status"] = "FAIL"
    wrong_payload = _canonical_bytes(wrong_document)
    path.write_bytes(wrong_payload)
    evidence = dict(manifest.evidence)
    evidence[check] = ReadinessArtifactBinding.from_bytes(
        binding.relative_path,
        wrong_payload,
    )

    decision = verify_environment_readiness(
        replace(manifest, evidence=evidence),
        evidence_root=tmp_path,
    )

    assert not decision.ready
    assert "EVIDENCE_SEMANTIC_VERIFICATION_FAILED" in {
        blocker.code for blocker in decision.blockers
    }


def test_semantic_verifier_exception_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = EnvironmentClass.PAPER
    _install_fixture_verifiers(monkeypatch, environment)
    check = min(profile_for(environment).required_checks, key=lambda item: item.value)
    _replace_fixture_verifier(
        monkeypatch,
        environment,
        check,
        _raising_semantic_verifier,
    )
    manifest = _manifest(tmp_path, environment)

    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)

    assert not decision.ready
    assert "EVIDENCE_VERIFIER_EXCEPTION" in {
        blocker.code for blocker in decision.blockers
    }


def test_artifact_changed_by_semantic_verifier_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = EnvironmentClass.PAPER
    _install_fixture_verifiers(monkeypatch, environment)
    check = min(profile_for(environment).required_checks, key=lambda item: item.value)
    manifest = _manifest(tmp_path, environment)
    artifact_path = tmp_path / manifest.evidence[check].relative_path

    def mutating_semantic_verifier(context: EvidenceVerificationContext) -> bool:
        passed = _fixture_semantic_verifier(context)
        artifact_path.write_bytes(context.artifact_bytes + b"\n")
        return passed

    _replace_fixture_verifier(
        monkeypatch,
        environment,
        check,
        mutating_semantic_verifier,
    )

    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)

    assert not decision.ready
    assert "ARTIFACT_CHANGED_DURING_SEMANTIC_VERIFICATION" in {
        blocker.code for blocker in decision.blockers
    }


@pytest.mark.parametrize("non_boolean_result", [None, 1, "PASS"])
def test_non_boolean_semantic_verifier_result_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    non_boolean_result: object,
) -> None:
    environment = EnvironmentClass.PAPER
    _install_fixture_verifiers(monkeypatch, environment)
    check = min(profile_for(environment).required_checks, key=lambda item: item.value)

    def non_boolean_verifier(_context: EvidenceVerificationContext) -> bool:
        return non_boolean_result  # type: ignore[return-value]

    _replace_fixture_verifier(
        monkeypatch,
        environment,
        check,
        non_boolean_verifier,
    )
    manifest = _manifest(tmp_path, environment)

    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)

    assert not decision.ready
    assert "EVIDENCE_SEMANTIC_VERIFICATION_FAILED" in {
        blocker.code for blocker in decision.blockers
    }


@pytest.mark.parametrize(
    "environment",
    [EnvironmentClass.MICRO_MAINNET, EnvironmentClass.MAINNET],
)
def test_complete_real_money_evidence_remains_build_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    environment: EnvironmentClass,
) -> None:
    _install_fixture_verifiers(monkeypatch, environment)
    manifest = _manifest(tmp_path, environment)

    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)

    assert not decision.ready
    assert {blocker.code for blocker in decision.blockers} == {
        "REAL_MONEY_EXECUTION_DISABLED_IN_BUILD"
    }


def test_receipt_rejects_current_compiled_verifier_set_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = EnvironmentClass.PAPER
    _install_fixture_verifiers(monkeypatch, environment)
    manifest = _manifest(tmp_path, environment)
    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)
    receipt = issue_environment_receipt(decision)
    check = min(profile_for(environment).required_checks, key=lambda item: item.value)

    _replace_fixture_verifier(
        monkeypatch,
        environment,
        check,
        _fixture_semantic_verifier,
        version=2,
    )
    blockers = receipt_scope_blockers(
        receipt,
        environment=environment,
        purpose=AuthorizationPurpose.PAPER_RUNTIME,
        config_hash=manifest.subject.config_hash,
    )

    assert "VERIFIER_SET_SCOPE_MISMATCH" in {
        blocker.code for blocker in blockers
    }


def test_incomplete_current_verifier_set_cannot_issue_or_scope_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = EnvironmentClass.PAPER
    _install_fixture_verifiers(monkeypatch, environment)
    manifest = _manifest(tmp_path, environment)
    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)
    receipt = issue_environment_receipt(decision)
    profile = profile_for(environment)
    check = min(profile.required_checks, key=lambda item: item.value)
    incomplete_registry = dict(authorization_module._COMPILED_EVIDENCE_VERIFIERS)
    del incomplete_registry[(environment, profile.purpose, check)]
    monkeypatch.setattr(
        authorization_module,
        "_COMPILED_EVIDENCE_VERIFIERS",
        MappingProxyType(incomplete_registry),
    )

    with pytest.raises(ValueError, match="incomplete compiled evidence verifier set"):
        issue_environment_receipt(decision)
    blockers = receipt_scope_blockers(
        receipt,
        environment=environment,
        purpose=AuthorizationPurpose.PAPER_RUNTIME,
        config_hash=manifest.subject.config_hash,
    )

    assert "NO_COMPILED_EVIDENCE_VERIFIER" in {
        blocker.code for blocker in blockers
    }
