from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from hyperlab.environment_authorization import (
    AuthorizationManifestError,
    AuthorizationPurpose,
    CredentialScope,
    EnvironmentAuthorizationReceipt,
    EnvironmentClass,
    EnvironmentReadinessManifest,
    EvidenceCheck,
    ExecutionNetwork,
    OrderCapability,
    ReadinessArtifactBinding,
    ReadinessSubject,
    compiled_evidence_verifier_status,
    issue_environment_receipt,
    profile_for,
    receipt_scope_blockers,
    verify_environment_readiness,
)
from hyperlab.environment_authorization import (
    testnet_evidence_payload as _testnet_evidence_payload,
)

_VALIDATION_ID = 'd' * 64
_VALIDATION_REPORT_SHA256 = 'e' * 64


def build_testnet_evidence_payload(
    check: EvidenceCheck,
    subject: ReadinessSubject,
    risk_limits: dict[str, object],
) -> dict[str, object]:
    return _testnet_evidence_payload(
        check,
        subject,
        risk_limits,
        validation_id=_VALIDATION_ID,
        validation_report_sha256=_VALIDATION_REPORT_SHA256,
    )

_RISK_LIMITS: dict[str, object] = {
    'cancel_requests_per_minute': 24,
    'deadman_interval_seconds': 30,
    'market_stale_after_seconds': 5,
    'max_concurrent_orders': 4,
    'max_gross_notional': '1000',
    'max_order_notional': '100',
    'max_order_quantity': '1',
    'max_position_notional': '500',
    'max_position_quantity': '5',
    'reconciliation_stale_after_seconds': 10,
    'replace_requests_per_minute': 6,
    'submit_requests_per_minute': 12,
}


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(',', ':'),
    ).encode('utf-8')


def _sha256(value: object) -> str:
    import hashlib

    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _subject(*, risk_limits: dict[str, object] | None = None) -> ReadinessSubject:
    limits = _RISK_LIMITS if risk_limits is None else risk_limits
    return ReadinessSubject(
        candidate_id='phase13-candidate',
        config_hash='a' * 64,
        strategy_hash='b' * 64,
        build_hash='c' * 64,
        source_identity='phase13:testnet-config',
        risk_limits_hash=_sha256(limits),
    )


def _testnet_manifest(root: Path) -> EnvironmentReadinessManifest:
    profile = profile_for(EnvironmentClass.TESTNET)
    subject = _subject()
    evidence: dict[EvidenceCheck, ReadinessArtifactBinding] = {}
    for ordinal, check in enumerate(sorted(profile.required_checks, key=lambda item: item.value)):
        relative_path = f'evidence/{ordinal:02d}-{check.value.casefold()}.json'
        payload = _canonical_bytes(build_testnet_evidence_payload(check, subject, _RISK_LIMITS))
        target = root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        evidence[check] = ReadinessArtifactBinding.from_bytes(relative_path, payload)
    return EnvironmentReadinessManifest(
        schema_version=1,
        environment=EnvironmentClass.TESTNET,
        purpose=AuthorizationPurpose.TESTNET_EXECUTION,
        environment_identity='TESTNET',
        execution_network=ExecutionNetwork.TESTNET,
        credential_scope=CredentialScope.TESTNET,
        order_capability=OrderCapability.TESTNET_ONLY,
        subject=subject,
        evidence=evidence,
    )


def _replace_artifact(
    root: Path,
    manifest: EnvironmentReadinessManifest,
    check: EvidenceCheck,
    payload: bytes,
) -> EnvironmentReadinessManifest:
    binding = manifest.evidence[check]
    (root / binding.relative_path).write_bytes(payload)
    evidence = dict(manifest.evidence)
    evidence[check] = ReadinessArtifactBinding.from_bytes(binding.relative_path, payload)
    return replace(manifest, evidence=evidence)


def _mutate_check_fact(payload: dict[str, object], check: EvidenceCheck) -> None:
    facts = cast(dict[str, object], payload['facts'])
    mutations: dict[EvidenceCheck, tuple[str, object]] = {
        EvidenceCheck.BOUNDED_POSITION_NOTIONAL: ('enforcement', 'PASS'),
        EvidenceCheck.CANCEL_REPLACE_SEMANTICS: (
            'replace_semantics',
            'CANCEL_THEN_SUBMIT',
        ),
        EvidenceCheck.CREDENTIAL_SCOPE_VALIDATION: ('mainnet_scope_accepted', True),
        EvidenceCheck.DETERMINISTIC_CLIENT_ORDER_IDS: ('domain', 'HYPERLAB_TESTNET'),
        EvidenceCheck.EXPLICIT_TESTNET_ENDPOINT: (
            'http_endpoint',
            'https://api.hyperliquid.xyz',
        ),
        EvidenceCheck.FAIL_CLOSED_ENVIRONMENT_IDENTITY: (
            'mainnet_identity_accepted',
            True,
        ),
        EvidenceCheck.FULL_AUDIT_LOG: ('secret_material_persisted', True),
        EvidenceCheck.ISOLATED_TESTNET_CREDENTIALS: (
            'credential_material_present',
            True,
        ),
        EvidenceCheck.KILL_SWITCH: ('kill_persistent', False),
        EvidenceCheck.NO_MAINNET_FALLBACK: ('mainnet_route_present', True),
        EvidenceCheck.ORDER_LIFECYCLE_STATE_MACHINE: (
            'ambiguous_state',
            'AMBIGUOUS_UNKNOWN',
        ),
        EvidenceCheck.RECONCILIATION: ('idempotent', False),
        EvidenceCheck.RESTART_RECOVERY: ('duplicate_submission_allowed', True),
        EvidenceCheck.TESTNET_CONFIG_NAMESPACE: (
            'configuration_namespace',
            'MAINNET',
        ),
    }
    key, value = mutations[check]
    facts[key] = value


_TESTNET_CHECKS = tuple(
    sorted(
        profile_for(EnvironmentClass.TESTNET).required_checks,
        key=lambda item: item.value,
    )
)


def test_all_compiled_testnet_artifacts_are_ready_and_receipt_cannot_escalate(
    tmp_path: Path,
) -> None:
    manifest = _testnet_manifest(tmp_path)

    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)

    assert decision.ready
    assert decision.blockers == ()
    assert len(decision.verifier_identities) == 14
    receipt = issue_environment_receipt(decision)
    assert receipt.environment is EnvironmentClass.TESTNET
    assert receipt.purpose is AuthorizationPurpose.TESTNET_EXECUTION
    assert receipt.execution_network is ExecutionNetwork.TESTNET
    assert receipt.credential_scope is CredentialScope.TESTNET
    assert receipt.order_capability is OrderCapability.TESTNET_ONLY
    assert receipt.authorizes_real_money is False
    assert receipt_scope_blockers(
        receipt,
        environment=EnvironmentClass.TESTNET,
        purpose=AuthorizationPurpose.TESTNET_EXECUTION,
        config_hash=manifest.subject.config_hash,
    ) == ()
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
        'ENVIRONMENT_SCOPE_MISMATCH',
        'PURPOSE_SCOPE_MISMATCH',
        'REAL_MONEY_EXECUTION_DISABLED_IN_BUILD',
    } <= escalation_codes


def test_compiled_registry_is_exactly_testnet_scoped_with_stable_identities() -> None:
    status = compiled_evidence_verifier_status(EnvironmentClass.TESTNET)

    assert status['complete'] is True
    assert status['required_check_count'] == 14
    identities = cast(list[dict[str, object]], status['verifier_identities'])
    assert {identity['check'] for identity in identities} == {
        check.value for check in _TESTNET_CHECKS
    }
    assert all(identity['environment'] == 'TESTNET' for identity in identities)
    assert all(identity['purpose'] == 'TESTNET_EXECUTION' for identity in identities)
    assert {
        cast(str, identity['check']): identity['version'] for identity in identities
    } == {
        check.value: (
            3 if check is EvidenceCheck.BOUNDED_POSITION_NOTIONAL else 2
        )
        for check in _TESTNET_CHECKS
    }
    assert {
        cast(str, identity['verifier_id']) for identity in identities
    } == {
        f'hyperlab:testnet-execution:{check.value.casefold()}'
        for check in _TESTNET_CHECKS
    }
    for environment in (
        EnvironmentClass.PAPER,
        EnvironmentClass.MICRO_MAINNET,
        EnvironmentClass.MAINNET,
    ):
        other = compiled_evidence_verifier_status(environment)
        assert other['complete'] is False
        assert other['verifier_identities'] == []


@pytest.mark.parametrize('check', _TESTNET_CHECKS, ids=lambda check: check.value)
def test_each_testnet_check_rejects_semantically_wrong_hash_bound_facts(
    tmp_path: Path,
    check: EvidenceCheck,
) -> None:
    manifest = _testnet_manifest(tmp_path)
    payload = build_testnet_evidence_payload(check, manifest.subject, _RISK_LIMITS)
    _mutate_check_fact(payload, check)
    manifest = _replace_artifact(tmp_path, manifest, check, _canonical_bytes(payload))

    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)

    assert not decision.ready
    assert {
        blocker.location
        for blocker in decision.blockers
        if blocker.code == 'EVIDENCE_SEMANTIC_VERIFICATION_FAILED'
    } == {f'evidence.{check.value}'}


def test_payload_builder_has_exact_final_fsm_and_native_modify_contract() -> None:
    subject = _subject()

    fsm = build_testnet_evidence_payload(
        EvidenceCheck.ORDER_LIFECYCLE_STATE_MACHINE,
        subject,
        _RISK_LIMITS,
    )['facts']
    assert fsm == {
        'ambiguous_state': 'UNKNOWN',
        'persistent': True,
        'states': [
            'REQUESTED',
            'SUBMITTED',
            'ACKNOWLEDGED',
            'OPEN',
            'PARTIALLY_FILLED',
            'FILLED',
            'CANCEL_REQUESTED',
            'CANCELLED',
            'REJECTED',
            'EXPIRED',
            'INVALID',
            'UNKNOWN',
        ],
    }
    cancel_replace = build_testnet_evidence_payload(
        EvidenceCheck.CANCEL_REPLACE_SEMANTICS,
        subject,
        _RISK_LIMITS,
    )['facts']
    assert cancel_replace == {
        'ambiguous_cancel_action': 'RECONCILE',
        'ambiguous_modify_action': 'RECONCILE',
        'blind_resubmit_allowed': False,
        'cancel_ack_required': True,
        'replace_semantics': 'NATIVE_MODIFY',
    }
    cloid = build_testnet_evidence_payload(
        EvidenceCheck.DETERMINISTIC_CLIENT_ORDER_IDS,
        subject,
        _RISK_LIMITS,
    )['facts']
    assert cloid == {
        'domain': 'hyperliquid_testnet_cloid_v1',
        'format': '0x+32_lowercase_hex',
        'pattern': '^0x[0-9a-f]{32}$',
        'retry_reuses_identifier': True,
    }


def test_payload_builder_rejects_noncanonical_or_unbounded_risk_limits() -> None:
    noncanonical = dict(_RISK_LIMITS)
    noncanonical['max_order_quantity'] = '1.0'
    with pytest.raises(ValueError, match='risk_limits'):
        build_testnet_evidence_payload(
            EvidenceCheck.BOUNDED_POSITION_NOTIONAL,
            _subject(risk_limits=noncanonical),
            noncanonical,
        )

    exceeds_position = dict(_RISK_LIMITS)
    exceeds_position['max_order_quantity'] = '6'
    with pytest.raises(ValueError, match='risk_limits'):
        build_testnet_evidence_payload(
            EvidenceCheck.BOUNDED_POSITION_NOTIONAL,
            _subject(risk_limits=exceeds_position),
            exceeds_position,
        )

    extra = dict(_RISK_LIMITS)
    extra['max_daily_loss'] = '1'
    with pytest.raises(ValueError, match='risk_limits'):
        build_testnet_evidence_payload(
            EvidenceCheck.BOUNDED_POSITION_NOTIONAL,
            _subject(risk_limits=extra),
            extra,
        )

    ceilings: dict[str, object] = {
        'cancel_requests_per_minute': 25,
        'deadman_interval_seconds': 31,
        'market_stale_after_seconds': 6,
        'max_concurrent_orders': 5,
        'max_gross_notional': '1001',
        'max_order_notional': '101',
        'max_order_quantity': '1.000001',
        'max_position_notional': '501',
        'max_position_quantity': '5.000001',
        'reconciliation_stale_after_seconds': 11,
        'replace_requests_per_minute': 7,
        'submit_requests_per_minute': 13,
    }
    for field, value in ceilings.items():
        excessive = dict(_RISK_LIMITS)
        excessive[field] = value
        with pytest.raises(ValueError, match='risk_limits'):
            build_testnet_evidence_payload(
                EvidenceCheck.BOUNDED_POSITION_NOTIONAL,
                _subject(risk_limits=excessive),
                excessive,
            )

    exponent_bomb = dict(_RISK_LIMITS)
    exponent_bomb['max_gross_notional'] = '1e1000000000'
    with pytest.raises(ValueError, match='risk_limits'):
        build_testnet_evidence_payload(
            EvidenceCheck.BOUNDED_POSITION_NOTIONAL,
            _subject(risk_limits=exponent_bomb),
            exponent_bomb,
        )


def test_testnet_evidence_requires_exact_keys_and_canonical_json(tmp_path: Path) -> None:
    manifest = _testnet_manifest(tmp_path)
    check = EvidenceCheck.EXPLICIT_TESTNET_ENDPOINT
    payload = build_testnet_evidence_payload(check, manifest.subject, _RISK_LIMITS)
    payload['status'] = 'PASS'
    manifest = _replace_artifact(tmp_path, manifest, check, _canonical_bytes(payload))

    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)

    assert not decision.ready
    assert 'EVIDENCE_SEMANTIC_VERIFICATION_FAILED' in {
        blocker.code for blocker in decision.blockers
    }

    manifest = _testnet_manifest(tmp_path / 'pretty')
    check = EvidenceCheck.EXPLICIT_TESTNET_ENDPOINT
    pretty = json.dumps(
        build_testnet_evidence_payload(check, manifest.subject, _RISK_LIMITS),
        indent=2,
        sort_keys=True,
    ).encode('utf-8')
    manifest = _replace_artifact(tmp_path / 'pretty', manifest, check, pretty)
    decision = verify_environment_readiness(manifest, evidence_root=tmp_path / 'pretty')
    assert not decision.ready
    assert 'EVIDENCE_SEMANTIC_VERIFICATION_FAILED' in {
        blocker.code for blocker in decision.blockers
    }


@pytest.mark.parametrize('field', ['validation_id', 'report_sha256'])
def test_testnet_evidence_rejects_malformed_validation_identity(
    tmp_path: Path,
    field: str,
) -> None:
    manifest = _testnet_manifest(tmp_path)
    check = EvidenceCheck.RESTART_RECOVERY
    payload = build_testnet_evidence_payload(check, manifest.subject, _RISK_LIMITS)
    validation = cast(dict[str, object], payload['validation'])
    validation[field] = 'not-a-sha256'
    manifest = _replace_artifact(tmp_path, manifest, check, _canonical_bytes(payload))

    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)

    assert not decision.ready
    assert 'EVIDENCE_SEMANTIC_VERIFICATION_FAILED' in {
        blocker.code for blocker in decision.blockers
    }

def test_receipt_loader_is_strict_canonical_and_scope_bound(tmp_path: Path) -> None:
    decision = verify_environment_readiness(
        _testnet_manifest(tmp_path),
        evidence_root=tmp_path,
    )
    receipt = issue_environment_receipt(decision)

    canonical = receipt.canonical_json_bytes()
    assert EnvironmentAuthorizationReceipt.from_json_bytes(canonical) == receipt
    pretty = json.dumps(receipt.to_dict(), indent=2, sort_keys=True).encode('utf-8')
    with pytest.raises(AuthorizationManifestError) as noncanonical_error:
        EnvironmentAuthorizationReceipt.from_json_bytes(pretty)
    assert noncanonical_error.value.code == 'NON_CANONICAL_RECEIPT'
    assert (
        EnvironmentAuthorizationReceipt.from_json_bytes(
            pretty,
            require_canonical=False,
        )
        == receipt
    )

    for key in ('schema_version', 'verifier_set_sha256'):
        missing = receipt.to_dict()
        del missing[key]
        with pytest.raises(AuthorizationManifestError) as missing_error:
            EnvironmentAuthorizationReceipt.from_object(missing)
        assert missing_error.value.code == 'INVALID_MANIFEST_SHAPE'

    extra = receipt.to_dict()
    extra['status'] = 'READY'
    with pytest.raises(AuthorizationManifestError) as extra_error:
        EnvironmentAuthorizationReceipt.from_object(extra)
    assert extra_error.value.code == 'INVALID_MANIFEST_SHAPE'

    wrong_scope = receipt.to_dict()
    wrong_scope['execution_network'] = 'MAINNET'
    with pytest.raises(AuthorizationManifestError) as scope_error:
        EnvironmentAuthorizationReceipt.from_object(wrong_scope)
    assert scope_error.value.code == 'INVALID_RECEIPT_SCOPE'

    malformed_scope = receipt.to_dict()
    malformed_scope['credential_scope'] = 'TESTNET/MAINNET'
    with pytest.raises(AuthorizationManifestError) as malformed_error:
        EnvironmentAuthorizationReceipt.from_object(malformed_scope)
    assert malformed_error.value.code == 'INVALID_ENVIRONMENT_IDENTITY'
