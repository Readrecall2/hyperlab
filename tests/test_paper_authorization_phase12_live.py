from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import replace
from functools import cache
from pathlib import Path
from typing import cast

import pytest

import hyperlab.environment_authorization as authorization_module
from hyperlab.environment_authorization import (
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
    compiled_evidence_verifier_status,
    issue_environment_receipt,
    paper_evidence_payload,
    profile_for,
    verify_environment_readiness,
)

_PAPER_CANDIDATE_ID = "phase08-robust-pairs-btc-eth-paper-v1"
_PAPER_SOURCE_IDENTITY = "hyperliquid-mainnet-public-bbo-funding-v1"
_RISK_LIMITS: dict[str, object] = {
    "max_concurrent_orders": 2,
    "max_daily_loss": "100",
    "max_drawdown": "200",
    "max_gross_notional": "2000",
    "max_instrument_notional": "1000",
    "max_net_notional": "1000",
    "max_order_notional": "250",
    "max_order_quantity": "0.25",
    "max_position_quantity": "1",
    "stale_after_seconds": 5,
    "unhedged_timeout_seconds": 20,
}


@cache
def _release_manifest_bytes() -> bytes:
    return authorization_module._build_paper_release_code_manifest_bytes()


@cache
def _runtime_environment_bytes() -> bytes:
    return authorization_module.paper_runtime_environment_attestation_bytes()


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _subject(
    *,
    candidate_id: str = _PAPER_CANDIDATE_ID,
    source_identity: str = _PAPER_SOURCE_IDENTITY,
    risk_limits: Mapping[str, object] = _RISK_LIMITS,
) -> ReadinessSubject:
    return ReadinessSubject(
        candidate_id=candidate_id,
        config_hash="a" * 64,
        strategy_hash="b" * 64,
        build_hash="c" * 64,
        source_identity=source_identity,
        risk_limits_hash=_sha256(dict(risk_limits)),
    )


def _payload(
    check: EvidenceCheck,
    *,
    subject: ReadinessSubject | None = None,
    release_code_manifest_bytes: bytes | None = None,
    runtime_environment_attestation_bytes: bytes | None = None,
) -> dict[str, object]:
    bound_subject = _subject() if subject is None else subject
    release_bytes = (
        (
            _release_manifest_bytes()
            if release_code_manifest_bytes is None
            else release_code_manifest_bytes
        )
        if check is EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
        else None
    )
    environment_bytes = (
        (
            _runtime_environment_bytes()
            if runtime_environment_attestation_bytes is None
            else runtime_environment_attestation_bytes
        )
        if check is EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
        else None
    )
    return paper_evidence_payload(
        check,
        bound_subject,
        _RISK_LIMITS if check is EvidenceCheck.BOUNDED_POSITION_NOTIONAL else None,
        release_code_manifest_bytes=release_bytes,
        runtime_environment_attestation_bytes=environment_bytes,
    )


def _manifest(root: Path) -> EnvironmentReadinessManifest:
    profile = profile_for(EnvironmentClass.PAPER)
    subject = _subject()
    release_bytes = _release_manifest_bytes()
    environment_bytes = _runtime_environment_bytes()
    (root / "release-code-manifest.json").write_bytes(release_bytes)
    (root / "runtime-environment-attestation.json").write_bytes(environment_bytes)
    evidence: dict[EvidenceCheck, ReadinessArtifactBinding] = {}
    for check in sorted(profile.required_checks, key=lambda item: item.value):
        payload = _canonical_bytes(
            _payload(
                check,
                subject=subject,
                release_code_manifest_bytes=release_bytes,
                runtime_environment_attestation_bytes=environment_bytes,
            )
        )
        relative_path = f"paper-evidence/{check.value.casefold()}.json"
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        evidence[check] = ReadinessArtifactBinding.from_bytes(relative_path, payload)
    return EnvironmentReadinessManifest(
        schema_version=1,
        environment=EnvironmentClass.PAPER,
        purpose=AuthorizationPurpose.PAPER_RUNTIME,
        environment_identity="PAPER",
        execution_network=ExecutionNetwork.NONE,
        credential_scope=CredentialScope.NONE,
        order_capability=OrderCapability.SIMULATED_ONLY,
        subject=subject,
        evidence=evidence,
    )


def _replace_artifact(
    manifest: EnvironmentReadinessManifest,
    root: Path,
    check: EvidenceCheck,
    payload: bytes,
) -> EnvironmentReadinessManifest:
    binding = manifest.evidence[check]
    (root / binding.relative_path).write_bytes(payload)
    evidence = dict(manifest.evidence)
    evidence[check] = ReadinessArtifactBinding.from_bytes(
        binding.relative_path,
        payload,
    )
    return replace(manifest, evidence=evidence)


def _semantic_failure_codes(
    manifest: EnvironmentReadinessManifest,
    root: Path,
) -> set[str]:
    decision = verify_environment_readiness(manifest, evidence_root=root)
    assert not decision.ready
    return {blocker.code for blocker in decision.blockers}


def test_compiled_paper_evidence_issues_non_escalating_receipt(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)

    decision = verify_environment_readiness(manifest, evidence_root=tmp_path)
    assert decision.ready, decision.blockers
    receipt = issue_environment_receipt(decision)

    assert receipt.environment is EnvironmentClass.PAPER
    assert receipt.purpose is AuthorizationPurpose.PAPER_RUNTIME
    assert receipt.execution_network is ExecutionNetwork.NONE
    assert receipt.credential_scope is CredentialScope.NONE
    assert receipt.order_capability is OrderCapability.SIMULATED_ONLY
    assert receipt.authorizes_real_money is False
    assert authorization_module.REAL_MONEY_EXECUTION_ENABLED_IN_BUILD is False
    assert EvidenceCheck.RESTART_RECOVERY in receipt.required_checks
    assert EvidenceCheck.KILL_SWITCH in receipt.required_checks


def test_current_release_code_digest_matches_the_independent_manifest() -> None:
    release_manifest = json.loads(_release_manifest_bytes().decode("utf-8"))

    assert authorization_module.current_paper_release_code_sha256() == (
        release_manifest["release_code_sha256"]
    )


def _write_runtime_lock(
    root: Path,
    pins: Mapping[str, str],
) -> None:
    lines: list[str] = []
    for index, (name, version) in enumerate(sorted(pins.items()), start=1):
        lines.extend(
            (
                f"{name}=={version} " + "\\",
                f"    --hash=sha256:{index:064x}",
            )
        )
    (root / "requirements-runtime.lock").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def _decoded_runtime_environment(payload: bytes) -> dict[str, object]:
    return cast(dict[str, object], json.loads(payload.decode("utf-8")))


def _rebuild_runtime_environment_artifact(
    artifact: dict[str, object],
) -> bytes:
    core = dict(artifact)
    core.pop("runtime_environment_sha256", None)
    artifact["runtime_environment_sha256"] = _sha256(core)
    return _canonical_bytes(artifact)


def test_current_runtime_environment_attestation_is_exact_and_canonical() -> None:
    payload = _runtime_environment_bytes()
    decoded = _decoded_runtime_environment(payload)
    lock = cast(dict[str, object], decoded["lock"])
    pins = cast(dict[str, dict[str, object]], lock["pins"])
    installed = cast(dict[str, str], decoded["installed_distributions"])
    interpreter = cast(dict[str, object], decoded["interpreter"])

    assert payload == _canonical_bytes(decoded)
    assert decoded["distribution_count"] == len(pins) == len(installed) == 34
    assert decoded["extras_allowed"] is True
    assert installed == {
        name: cast(str, pin["version"])
        for name, pin in pins.items()
    }
    assert interpreter["implementation"] == "cpython"
    assert interpreter["python_version"] == "3.12.10"
    assert interpreter["cache_tag"] == "cpython-312"
    assert all(
        isinstance(interpreter[name], str) and interpreter[name]
        for name in (
            "platform_machine",
            "platform_system",
            "platform_tag",
            "python_compiler",
        )
    )
    assert authorization_module.current_paper_runtime_environment_sha256() == (
        decoded["runtime_environment_sha256"]
    )


def test_runtime_environment_builder_accepts_exact_required_pins_and_allows_extras(
    tmp_path: Path,
) -> None:
    _write_runtime_lock(
        tmp_path,
        {"alpha-runtime": "1.2.3", "beta-runtime": "4.5.6"},
    )
    available = {
        "alpha-runtime": "1.2.3",
        "beta-runtime": "4.5.6",
        "unlocked-extra": "9.9.9",
    }
    requested: list[str] = []

    def lookup(name: str) -> str:
        requested.append(name)
        return available[name]

    payload = authorization_module._build_paper_runtime_environment_attestation_bytes(
        tmp_path,
        distribution_version=lookup,
    )
    decoded = _decoded_runtime_environment(payload)

    assert set(requested) == {"alpha-runtime", "beta-runtime"}
    assert "unlocked-extra" not in cast(
        dict[str, str],
        decoded["installed_distributions"],
    )
    assert decoded["distribution_count"] == 2
    assert decoded["extras_allowed"] is True


@pytest.mark.parametrize("failure", ["missing", "version"])
def test_runtime_environment_builder_rejects_missing_or_drifted_locked_distribution(
    tmp_path: Path,
    failure: str,
) -> None:
    _write_runtime_lock(tmp_path, {"alpha-runtime": "1.2.3"})

    def lookup(name: str) -> str:
        if failure == "missing":
            raise authorization_module.importlib_metadata.PackageNotFoundError(name)
        return "1.2.4"

    expected = "not installed" if failure == "missing" else "locked distribution mismatch"
    with pytest.raises(ValueError, match=expected):
        authorization_module._build_paper_runtime_environment_attestation_bytes(
            tmp_path,
            distribution_version=lookup,
        )


@pytest.mark.parametrize(
    "lock_text",
    [
        "alpha-runtime==1.2.3\n",
        "Alpha_Runtime==1.2.3 \\\n    --hash=sha256:" + "1" * 64 + "\n",
        (
            "alpha-runtime==1.2.3 \\\n    --hash=sha256:"
            + "1" * 64
            + "\nalpha-runtime==1.2.3 \\\n    --hash=sha256:"
            + "2" * 64
            + "\n"
        ),
    ],
    ids=["unhashed", "noncanonical-name", "duplicate-pin"],
)
def test_runtime_environment_lock_parser_rejects_non_exact_inputs(
    tmp_path: Path,
    lock_text: str,
) -> None:
    (tmp_path / "requirements-runtime.lock").write_text(
        lock_text,
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        authorization_module._build_paper_runtime_environment_attestation_bytes(
            tmp_path,
            distribution_version=lambda _name: "1.2.3",
        )


def test_runtime_environment_builder_rejects_distribution_toctou(
    tmp_path: Path,
) -> None:
    _write_runtime_lock(tmp_path, {"alpha-runtime": "1.2.3"})
    calls = 0

    def lookup(_name: str) -> str:
        nonlocal calls
        calls += 1
        return "1.2.3" if calls == 1 else "1.2.4"

    with pytest.raises(ValueError, match="changed while"):
        authorization_module._build_paper_runtime_environment_attestation_bytes(
            tmp_path,
            distribution_version=lookup,
        )


def test_runtime_environment_builder_rejects_interpreter_fact_toctou(
    tmp_path: Path,
) -> None:
    _write_runtime_lock(tmp_path, {"alpha-runtime": "1.2.3"})
    calls = 0

    def facts() -> Mapping[str, object]:
        nonlocal calls
        calls += 1
        current = authorization_module._current_paper_cpython_facts()
        if calls > 1:
            current["python_compiler"] = "RACING COMPILER"
        return current

    with pytest.raises(ValueError, match="changed while"):
        authorization_module._build_paper_runtime_environment_attestation_bytes(
            tmp_path,
            distribution_version=lambda _name: "1.2.3",
            interpreter_facts=facts,
        )


@pytest.mark.parametrize(
    ("field", "mutated"),
    [
        ("platform_tag", "mutated-platform"),
        ("python_compiler", "mutated-compiler"),
    ],
)
def test_runtime_environment_artifact_cannot_self_attest_platform_or_compiler_drift(
    tmp_path: Path,
    field: str,
    mutated: str,
) -> None:
    manifest = _manifest(tmp_path)
    artifact = _decoded_runtime_environment(_runtime_environment_bytes())
    interpreter = cast(dict[str, object], artifact["interpreter"])
    interpreter[field] = mutated
    mutated_bytes = _rebuild_runtime_environment_artifact(artifact)
    (tmp_path / "runtime-environment-attestation.json").write_bytes(mutated_bytes)
    check = EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
    evidence_payload = _canonical_bytes(
        _payload(
            check,
            subject=manifest.subject,
            runtime_environment_attestation_bytes=mutated_bytes,
        )
    )
    manifest = _replace_artifact(manifest, tmp_path, check, evidence_payload)

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


def test_paper_registry_is_complete_and_real_money_scopes_remain_absent() -> None:
    status = compiled_evidence_verifier_status(EnvironmentClass.PAPER)
    checks = profile_for(EnvironmentClass.PAPER).required_checks

    assert status["complete"] is True
    assert status["required_check_count"] == len(checks) == 16
    assert {
        identity["check"]
        for identity in cast(list[dict[str, object]], status["verifier_identities"])
    } == {check.value for check in checks}
    assert {
        cast(int, identity["version"])
        for identity in cast(list[dict[str, object]], status["verifier_identities"])
    } == {5}
    assert profile_for(EnvironmentClass.PAPER).profile_sha256 not in {
        "a68e9ea0e5ac2d7622f0344920a7df8426a3d8b5313a61a126de6e053aca50f8",
        "d617eb4dea1cde6c2873a81748ed4311c347736195bb387a4e7457453793ae3f",
        "d0a720a4446f33eef1772f10d00fbd88618e8452c04a854d59837b333ff0b48a",
    }
    assert all(
        environment in {EnvironmentClass.PAPER, EnvironmentClass.TESTNET}
        for environment, _purpose, _check in authorization_module._COMPILED_EVIDENCE_VERIFIERS
    )
    assert not any(
        environment in {EnvironmentClass.MICRO_MAINNET, EnvironmentClass.MAINNET}
        for environment, _purpose, _check in authorization_module._COMPILED_EVIDENCE_VERIFIERS
    )


def test_testnet_compiled_verifier_identities_are_unchanged() -> None:
    status = compiled_evidence_verifier_status(EnvironmentClass.TESTNET)
    checks = profile_for(EnvironmentClass.TESTNET).required_checks
    identities = cast(list[dict[str, object]], status["verifier_identities"])

    assert status["complete"] is True
    assert len(identities) == len(checks)
    assert {
        cast(str, identity["check"]): (
            cast(str, identity["verifier_id"]),
            cast(int, identity["version"]),
        )
        for identity in identities
    } == {
        check.value: (
            f"hyperlab:testnet-execution:{check.value.casefold()}",
            3 if check is EvidenceCheck.BOUNDED_POSITION_NOTIONAL else 2,
        )
        for check in checks
    }


@pytest.mark.parametrize(
    ("field", "mutated"),
    [
        ("authorizes_real_money", True),
        ("orders_enabled", True),
        ("execution_network", "MAINNET"),
        ("credential_scope", "TESTNET"),
        ("order_capability", "TESTNET_ONLY"),
        ("real_money_execution_enabled_in_build", True),
    ],
)
def test_runtime_scope_mutations_are_rejected(
    tmp_path: Path,
    field: str,
    mutated: object,
) -> None:
    check = EvidenceCheck.NO_PRIVATE_EXECUTION_PATH
    manifest = _manifest(tmp_path)
    payload = _payload(check, subject=manifest.subject)
    runtime_scope = cast(dict[str, object], payload["runtime_scope"])
    runtime_scope[field] = mutated
    manifest = _replace_artifact(manifest, tmp_path, check, _canonical_bytes(payload))

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }



@pytest.mark.parametrize(
    ("check", "field", "mutated"),
    [
        (EvidenceCheck.CRASH_RECOVERY, "journal_mode", "WAL"),
        (
            EvidenceCheck.CRASH_RECOVERY,
            "atomic_commit_contents",
            [
                "INBOX_INPUT",
                "EVENTS",
                "LEDGER_TRANSACTIONS_AND_ENTRIES",
                "PROJECTION_AND_HISTORY",
                "COMMIT_RECORD",
            ],
        ),
        (EvidenceCheck.PUBLIC_MARKET_SOURCE, "bootstrap_timeout_seconds", 10.0),
        (EvidenceCheck.PUBLIC_MARKET_SOURCE, "redirects_allowed", True),
        (
            EvidenceCheck.PUBLIC_MARKET_SOURCE,
            "http_final_url_must_equal_request",
            False,
        ),
        (EvidenceCheck.PUBLIC_MARKET_SOURCE, "http_required_status", 302),
        (EvidenceCheck.PUBLIC_MARKET_SOURCE, "websocket_redirect_limit", 1),
        (EvidenceCheck.PUBLIC_MARKET_SOURCE, "websocket_required_http_status", 200),
        (
            EvidenceCheck.PUBLIC_MARKET_SOURCE,
            "rest_methods",
            ["fundingHistory", "l2Book", "meta"],
        ),
        (
            EvidenceCheck.PUBLIC_MARKET_SOURCE,
            "rest_l2_book_projection",
            "FULL_BOOK_EXECUTABLE",
        ),
        (
            EvidenceCheck.NORMALIZED_MARKET_EVENT_SCHEMA,
            "adapter_schema_version",
            6,
        ),
        (
            EvidenceCheck.NORMALIZED_MARKET_EVENT_SCHEMA,
            "bbo_tradability_policy",
            "REST_BOOTSTRAP_TRADABLE",
        ),
        (
            EvidenceCheck.NORMALIZED_MARKET_EVENT_SCHEMA,
            "malformed_bbo_policy",
            "SILENT_DROP",
        ),
        (
            EvidenceCheck.NORMALIZED_MARKET_EVENT_SCHEMA,
            "gap_or_stale_action",
            "NOT_TRADABLE",
        ),
        (
            EvidenceCheck.RUNTIME_SOURCE_ATTESTATION,
            "engine_semantic_build_hash",
            "f" * 64,
        ),
        (
            EvidenceCheck.RUNTIME_SOURCE_ATTESTATION,
            "runtime_cadence_must_equal_frozen_config_before_lease",
            False,
        ),
        (EvidenceCheck.RESTART_RECOVERY, "duplicate_inputs_idempotent", False),
        (
            EvidenceCheck.CONSERVATIVE_COST_MODEL,
            "synthetic_funding_reserve_allowed",
            True,
        ),
        (
            EvidenceCheck.RESTART_RECOVERY,
            "duplicate_economic_effects_allowed",
            True,
        ),
        (EvidenceCheck.FULL_AUDIT_LOG, "append_only", True),
    ],
    ids=[
        "false-wal",
        "incomplete-atomic-contents",
        "false-bootstrap-timeout",
        "http-redirects-enabled",
        "http-final-url-unbound",
        "http-status-not-200",
        "websocket-redirect-enabled",
        "websocket-status-not-101",
        "inexact-rest-methods",
        "inexact-l2-bootstrap-projection",
        "stale-adapter-schema",
        "tradable-rest-bootstrap",
        "silent-malformed-bbo-drop",
        "understated-gap-action",
        "wrong-engine-semantic-label",
        "cadence-not-config-bound",
        "duplicate-input-not-idempotent",
        "synthetic-funding-reserve",
        "duplicate-economic-effects",
        "blanket-append-only",
    ],
)
def test_false_implementation_facts_are_rejected(
    tmp_path: Path,
    check: EvidenceCheck,
    field: str,
    mutated: object,
) -> None:
    manifest = _manifest(tmp_path)
    payload = _payload(check, subject=manifest.subject)
    facts = cast(dict[str, object], payload["facts"])
    facts[field] = mutated
    manifest = _replace_artifact(manifest, tmp_path, check, _canonical_bytes(payload))

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


@pytest.mark.parametrize(
    ("field", "mutated"),
    [
        ("runtime_timer_interval_seconds", 2.0),
        ("runtime_source_poll_timeout_seconds", 0.5),
    ],
)
def test_false_frozen_runtime_cadence_facts_are_rejected(
    tmp_path: Path,
    field: str,
    mutated: object,
) -> None:
    check = EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
    manifest = _manifest(tmp_path)
    payload = _payload(check, subject=manifest.subject)
    facts = cast(dict[str, object], payload["facts"])
    cadence = cast(dict[str, object], facts["frozen_runtime_cadence"])
    cadence[field] = mutated
    manifest = _replace_artifact(manifest, tmp_path, check, _canonical_bytes(payload))

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


@pytest.mark.parametrize(
    "check",
    [EvidenceCheck.RUNTIME_SOURCE_ATTESTATION, EvidenceCheck.CRASH_RECOVERY],
)
@pytest.mark.parametrize(
    ("field", "mutated"),
    [
        ("active_runtime_pause_and_kill_require_lease", True),
        ("runtime_acquired_after_release_and_frozen_binding_checks", False),
        (
            "runtime_acquired_before_engine_start_startup_reconciliation_and_public_source_start",
            False,
        ),
        ("runtime_admission_failure_releases_lock", False),
        ("read_only_status_report_require_lease", True),
        ("second_runtime_for_exact_database_and_run_rejected", False),
        ("runtime_held_until_close", False),
        ("standalone_reconcile_acquired_after_release_check", False),
        ("standalone_reconcile_failure_releases_lock", False),
        ("standalone_reconcile_held_until_completion", False),
        ("standalone_reconcile_requires_stopped_runtime", False),
        ("standalone_replay_acquired_after_release_check", False),
        ("standalone_replay_failure_releases_lock", False),
        ("standalone_replay_held_until_completion", False),
        ("standalone_replay_requires_stopped_runtime", False),
        ("standalone_resume_acquired_after_release_check", False),
        ("standalone_resume_config_and_release_rechecked_under_lease", False),
        ("standalone_resume_failure_releases_lock", False),
        ("standalone_resume_held_until_completion", False),
        ("standalone_resume_requires_lease_before_mutation", False),
        ("standalone_resume_requires_stopped_runtime", False),
        (
            "contention_action",
            "BLOCK_ALL_WRITER_PROCESSES",
        ),
        ("lock_mode", "PROCESS_LOCAL_MUTEX"),
        ("scope", "DATABASE_ONLY"),
    ],
)
def test_false_runtime_lease_facts_are_rejected(
    tmp_path: Path,
    check: EvidenceCheck,
    field: str,
    mutated: object,
) -> None:
    manifest = _manifest(tmp_path)
    payload = _payload(check, subject=manifest.subject)
    facts = cast(dict[str, object], payload["facts"])
    runtime_lease = cast(dict[str, object], facts["runtime_lease"])
    runtime_lease[field] = mutated
    manifest = _replace_artifact(manifest, tmp_path, check, _canonical_bytes(payload))

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


@pytest.mark.parametrize(
    ("field", "mutated"),
    [
        ("canonical_config_field", "engine_build_hash"),
        ("current_checkout_digest_required_during_construction_and_before_start", False),
        ("durable_config_snapshot", "RUN_STARTED.payload"),
        ("run_config_hash_binds_canonical_snapshot", False),
        ("run_start_config_hash_binds_canonical_snapshot", False),
    ],
)
def test_false_durable_release_code_binding_facts_are_rejected(
    tmp_path: Path,
    field: str,
    mutated: object,
) -> None:
    check = EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
    manifest = _manifest(tmp_path)
    payload = _payload(check, subject=manifest.subject)
    facts = cast(dict[str, object], payload["facts"])
    binding = cast(dict[str, object], facts["release_code_binding"])
    binding[field] = mutated
    manifest = _replace_artifact(manifest, tmp_path, check, _canonical_bytes(payload))

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


def test_runtime_lease_builder_nested_collections_are_isolated() -> None:
    subject = _subject()
    first = paper_evidence_payload(EvidenceCheck.CRASH_RECOVERY, subject)
    first_facts = cast(dict[str, object], first["facts"])
    first_lease = cast(dict[str, object], first_facts["runtime_lease"])
    first_identity_fields = cast(list[str], first_lease["identity_fields"])
    first_identity_fields.append("MUTATED")

    second = paper_evidence_payload(EvidenceCheck.CRASH_RECOVERY, subject)
    second_facts = cast(dict[str, object], second["facts"])
    second_lease = cast(dict[str, object], second_facts["runtime_lease"])

    assert second_lease["identity_fields"] == [
        "CANONICAL_DATABASE_PATH",
        "RUN_ID",
        "LEASE_SCHEMA",
    ]


def test_obsolete_sqlite_wal_fact_is_rejected(tmp_path: Path) -> None:
    check = EvidenceCheck.CRASH_RECOVERY
    manifest = _manifest(tmp_path)
    payload = _payload(check, subject=manifest.subject)
    facts = cast(dict[str, object], payload["facts"])
    facts["sqlite_wal_enabled"] = True
    manifest = _replace_artifact(manifest, tmp_path, check, _canonical_bytes(payload))

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }

@pytest.mark.parametrize(
    ("field", "mutated"),
    [
        ("concurrent_sqlite_writer_transactions_allowed", True),
        (
            "sqlite_writer_transaction_serialization",
            "PROCESS_LOCAL_MUTEX",
        ),
    ],
)
def test_false_sqlite_writer_serialization_facts_are_rejected(
    tmp_path: Path,
    field: str,
    mutated: object,
) -> None:
    check = EvidenceCheck.CRASH_RECOVERY
    manifest = _manifest(tmp_path)
    payload = _payload(check, subject=manifest.subject)
    facts = cast(dict[str, object], payload["facts"])
    facts[field] = mutated
    manifest = _replace_artifact(manifest, tmp_path, check, _canonical_bytes(payload))

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }




def test_stale_version_1_crash_evidence_is_rejected(tmp_path: Path) -> None:
    check = EvidenceCheck.CRASH_RECOVERY
    manifest = _manifest(tmp_path)
    payload = _payload(check, subject=manifest.subject)
    payload["facts"] = {
        "append_transaction_atomic": True,
        "critical_incident_on_failure": True,
        "durable_inbox_before_processing": True,
        "hash_chain_verified_on_restore": True,
        "projection_restored_from_journal": True,
        "sqlite_wal_enabled": True,
    }
    manifest = _replace_artifact(manifest, tmp_path, check, _canonical_bytes(payload))

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }

@pytest.mark.parametrize(
    "false_fact",
    ["RUNTIME_STOP", "ENVIRONMENT_AUTHORIZATION", "paper_runs", "paper_projections"],
)
def test_non_append_only_or_nonexistent_audit_facts_are_rejected(
    tmp_path: Path,
    false_fact: str,
) -> None:
    check = EvidenceCheck.FULL_AUDIT_LOG
    manifest = _manifest(tmp_path)
    payload = _payload(check, subject=manifest.subject)
    facts = cast(dict[str, object], payload["facts"])
    tables = cast(list[str], facts["append_only_tables"])
    tables.append(false_fact)
    manifest = _replace_artifact(manifest, tmp_path, check, _canonical_bytes(payload))

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


@pytest.mark.parametrize(
    ("table", "false_field"),
    [
        ("paper_events", "previous_event_hash"),
        ("paper_runs", "seed_hash"),
    ],
)
def test_nonexistent_persisted_hash_fields_are_rejected(
    tmp_path: Path,
    table: str,
    false_field: str,
) -> None:
    check = EvidenceCheck.FULL_AUDIT_LOG
    manifest = _manifest(tmp_path)
    payload = _payload(check, subject=manifest.subject)
    facts = cast(dict[str, object], payload["facts"])
    hash_fields = cast(dict[str, list[str]], facts["persisted_hash_fields"])
    hash_fields[table].append(false_field)
    manifest = _replace_artifact(manifest, tmp_path, check, _canonical_bytes(payload))

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


def test_stale_v3_runtime_evidence_without_release_binding_is_rejected(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    check = EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
    payload = _payload(check, subject=manifest.subject)
    facts = cast(dict[str, object], payload["facts"])
    del facts["release_code_manifest"]
    manifest = _replace_artifact(
        manifest,
        tmp_path,
        check,
        _canonical_bytes(payload),
    )

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


def test_release_reader_rejects_a_file_identity_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "code.py"
    target.write_bytes(b"reviewed = True\n")
    calls = 0

    def unstable_identity(value: os.stat_result) -> tuple[int, int, int, int]:
        nonlocal calls
        calls += 1
        return (0, 0, value.st_size, calls)

    monkeypatch.setattr(
        authorization_module,
        "_paper_release_stat_identity",
        unstable_identity,
    )
    with pytest.raises(ValueError, match="changed while it was being hashed"):
        authorization_module._read_stable_paper_release_file(target)


def test_cli_registry_derived_sha_values_are_canonicalized_without_a_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root = authorization_module._paper_release_repository_root()
    relative_path = "src/hyperlab/cli.py"
    cli_path = (repository_root / "src/hyperlab/cli.py").resolve(strict=True)
    canonical_before = authorization_module._canonical_paper_release_file_bytes(
        repository_root,
        relative_path,
    )
    mutated = (
        cli_path.read_bytes()
        .decode("utf-8")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )
    for index, name in enumerate(
        authorization_module._PAPER_CLI_DERIVED_BINDING_NAMES,
        start=1,
    ):
        pattern = re.compile(
            rf'(?m)^{re.escape(name)} = "[0-9a-f]{{64}}"$'
        )
        mutated, count = pattern.subn(
            f'{name} = "{index:064x}"',
            mutated,
        )
        assert count == 1
    original_reader = authorization_module._read_stable_paper_release_file

    def replaced_reader(path: Path) -> bytes:
        if path == cli_path:
            return mutated.encode("utf-8")
        return original_reader(path)

    monkeypatch.setattr(
        authorization_module,
        "_read_stable_paper_release_file",
        replaced_reader,
    )
    canonical_after = authorization_module._canonical_paper_release_file_bytes(
        repository_root,
        relative_path,
    )

    assert canonical_after == canonical_before



def test_compiled_runtime_cadence_constants_are_not_release_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root = authorization_module._paper_release_repository_root()
    relative_path = "src/hyperlab/cli.py"
    cli_path = (repository_root / relative_path).resolve(strict=True)
    canonical_before = authorization_module._canonical_paper_release_file_bytes(
        repository_root,
        relative_path,
    )
    mutated = (
        cli_path.read_bytes()
        .decode("utf-8")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )
    mutated, timer_count = re.subn(
        r"(?m)^_PHASE12_PAPER_RUNTIME_TIMER_INTERVAL_SECONDS = 1\.0$",
        "_PHASE12_PAPER_RUNTIME_TIMER_INTERVAL_SECONDS = 2.0",
        mutated,
    )
    mutated, poll_count = re.subn(
        r"(?m)^_PHASE12_PAPER_RUNTIME_SOURCE_POLL_TIMEOUT_SECONDS = 0\.25$",
        "_PHASE12_PAPER_RUNTIME_SOURCE_POLL_TIMEOUT_SECONDS = 0.5",
        mutated,
    )
    assert (timer_count, poll_count) == (1, 1)
    original_reader = authorization_module._read_stable_paper_release_file

    def replaced_reader(path: Path) -> bytes:
        if path == cli_path:
            return mutated.encode("utf-8")
        return original_reader(path)

    monkeypatch.setattr(
        authorization_module,
        "_read_stable_paper_release_file",
        replaced_reader,
    )

    canonical_after = authorization_module._canonical_paper_release_file_bytes(
        repository_root,
        relative_path,
    )

    assert canonical_after != canonical_before
    assert b"_PHASE12_PAPER_RUNTIME_TIMER_INTERVAL_SECONDS = 2.0" in canonical_after
    assert (
        b"_PHASE12_PAPER_RUNTIME_SOURCE_POLL_TIMEOUT_SECONDS = 0.5"
        in canonical_after
    )



def test_authorization_manifest_rejects_early_file_changed_after_its_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = tmp_path / "release"
    (repository / "scripts").mkdir(parents=True)
    (repository / "src" / "hyperlab").mkdir(parents=True)
    (repository / "pyproject.toml").write_text(
        "[project]\nname = 'fixture'\n",
        encoding="utf-8",
    )
    (repository / "requirements-runtime.lock").write_text(
        "fixture==1\n",
        encoding="utf-8",
    )
    (repository / "scripts" / "generate_phase12_live_paper_artifacts.py").write_text(
        "GENERATOR = True\n",
        encoding="utf-8",
    )
    (repository / "scripts" / "certify_storage_v4_phase1b.py").write_text(
        "CERTIFIER = True\n",
        encoding="utf-8",
    )
    (repository / "scripts" / "certify_storage_v4_phase1c.py").write_text(
        "CERTIFIER = True\n",
        encoding="utf-8",
    )
    (repository / "scripts" / "certify_storage_v4_phase1d_linux.py").write_text(
        "CERTIFIER = True\n",
        encoding="utf-8",
    )
    (repository / "src" / "hyperlab" / "early.py").write_text(
        "EARLY = True\n",
        encoding="utf-8",
    )
    (repository / "src" / "hyperlab" / "cli.py").write_text(
        "\n".join(
            (
                '_PHASE12_PAPER_CONFIG_HASH = "' + "0" * 64 + '"',
                '_PHASE12_PAPER_READINESS_MANIFEST_SHA256 = "' + "1" * 64 + '"',
                '_PHASE12_PAPER_READINESS_PROFILE_SHA256 = "' + "2" * 64 + '"',
                '_PHASE12_MULTISTRATEGY_CONFIG_HASH = "' + "3" * 64 + '"',
                '_PHASE12_MULTISTRATEGY_READINESS_MANIFEST_SHA256 = "' + "4" * 64 + '"',
                '_PHASE12_MULTISTRATEGY_READINESS_PROFILE_SHA256 = "' + "5" * 64 + '"',
                "",
            )
        ),
        encoding="utf-8",
    )
    original_reader = authorization_module._canonical_paper_release_file_bytes

    def racing_reader(root: Path, relative_path: str) -> bytes:
        payload = original_reader(root, relative_path)
        if relative_path == "requirements-runtime.lock":
            (root / "pyproject.toml").write_text(
                "[project]\nname = 'mutated-after-read'\n",
                encoding="utf-8",
            )
        return payload

    monkeypatch.setattr(
        authorization_module,
        "_canonical_paper_release_file_bytes",
        racing_reader,
    )

    with pytest.raises(ValueError, match="release code changed while"):
        authorization_module._build_paper_release_code_manifest_bytes(repository)


def _rebuild_release_manifest(
    release_manifest: dict[str, object],
) -> bytes:
    core = {
        "artifact_schema_version": release_manifest["artifact_schema_version"],
        "candidate_id": release_manifest["candidate_id"],
        "canonicalization": release_manifest["canonicalization"],
        "files": release_manifest["files"],
    }
    release_manifest["release_code_sha256"] = _sha256(core)
    return _canonical_bytes(release_manifest)


def _replace_release_attestation(
    manifest: EnvironmentReadinessManifest,
    root: Path,
    release_manifest_bytes: bytes,
) -> EnvironmentReadinessManifest:
    (root / "release-code-manifest.json").write_bytes(release_manifest_bytes)
    check = EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
    evidence_payload = _canonical_bytes(
        _payload(
            check,
            subject=manifest.subject,
            release_code_manifest_bytes=release_manifest_bytes,
        )
    )
    return _replace_artifact(manifest, root, check, evidence_payload)


def test_recomputed_release_manifest_cannot_self_attest_mutated_code_digest(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    release_manifest = cast(
        dict[str, object],
        json.loads(_release_manifest_bytes().decode("utf-8")),
    )
    files = cast(dict[str, str], release_manifest["files"])
    target = "src/hyperlab/paper/engine.py"
    assert target in files
    files[target] = "0" * 64
    fake_release_bytes = _rebuild_release_manifest(release_manifest)
    manifest = _replace_release_attestation(
        manifest,
        tmp_path,
        fake_release_bytes,
    )

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_release_manifest_requires_the_exact_discovered_code_path_set(
    tmp_path: Path,
    mutation: str,
) -> None:
    manifest = _manifest(tmp_path)
    release_manifest = cast(
        dict[str, object],
        json.loads(_release_manifest_bytes().decode("utf-8")),
    )
    files = cast(dict[str, str], release_manifest["files"])
    if mutation == "missing":
        del files["requirements-runtime.lock"]
    else:
        files["src/hyperlab/not_reviewed.py"] = "0" * 64
    fake_release_bytes = _rebuild_release_manifest(release_manifest)
    manifest = _replace_release_attestation(
        manifest,
        tmp_path,
        fake_release_bytes,
    )

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


def test_noncanonical_release_manifest_is_rejected_even_when_bytes_are_bound(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    release_object = cast(
        dict[str, object],
        json.loads(_release_manifest_bytes().decode("utf-8")),
    )
    noncanonical = json.dumps(
        release_object,
        allow_nan=False,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ).encode("utf-8")
    (tmp_path / "release-code-manifest.json").write_bytes(noncanonical)
    check = EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
    payload = _payload(check, subject=manifest.subject)
    facts = cast(dict[str, object], payload["facts"])
    attestation = cast(dict[str, object], facts["release_code_manifest"])
    attestation["sha256"] = hashlib.sha256(noncanonical).hexdigest()
    manifest = _replace_artifact(
        manifest,
        tmp_path,
        check,
        _canonical_bytes(payload),
    )

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


@pytest.mark.parametrize(
    ("field", "mutated"),
    [
        ("path", "evidence/release-code-manifest.json"),
        ("canonicalization", "RAW_BYTES_V1"),
        ("file_count", 1),
        ("release_code_sha256", "0" * 64),
        ("sha256", "0" * 64),
    ],
)
def test_release_attestation_binding_mutations_are_rejected(
    tmp_path: Path,
    field: str,
    mutated: object,
) -> None:
    manifest = _manifest(tmp_path)
    check = EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
    payload = _payload(check, subject=manifest.subject)
    facts = cast(dict[str, object], payload["facts"])
    attestation = cast(dict[str, object], facts["release_code_manifest"])
    attestation[field] = mutated
    manifest = _replace_artifact(
        manifest,
        tmp_path,
        check,
        _canonical_bytes(payload),
    )

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


def _replace_runtime_environment_attestation(
    manifest: EnvironmentReadinessManifest,
    root: Path,
    runtime_environment_bytes: bytes,
) -> EnvironmentReadinessManifest:
    (root / "runtime-environment-attestation.json").write_bytes(
        runtime_environment_bytes
    )
    check = EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
    evidence_payload = _canonical_bytes(
        _payload(
            check,
            subject=manifest.subject,
            runtime_environment_attestation_bytes=runtime_environment_bytes,
        )
    )
    return _replace_artifact(manifest, root, check, evidence_payload)


def test_recomputed_runtime_environment_artifact_cannot_self_attest_pin_drift(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    artifact = _decoded_runtime_environment(_runtime_environment_bytes())
    installed = cast(dict[str, str], artifact["installed_distributions"])
    lock = cast(dict[str, object], artifact["lock"])
    pins = cast(dict[str, dict[str, object]], lock["pins"])
    name = next(iter(pins))
    pins[name]["version"] = "999.0"
    installed[name] = "999.0"
    mutated_bytes = _rebuild_runtime_environment_artifact(artifact)
    manifest = _replace_runtime_environment_attestation(
        manifest,
        tmp_path,
        mutated_bytes,
    )

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


@pytest.mark.parametrize("mutation", ["noncanonical", "extra-key"])
def test_runtime_environment_artifact_rejects_noncanonical_or_extra_content(
    tmp_path: Path,
    mutation: str,
) -> None:
    manifest = _manifest(tmp_path)
    artifact = _decoded_runtime_environment(_runtime_environment_bytes())
    if mutation == "noncanonical":
        mutated_bytes = json.dumps(
            artifact,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8")
    else:
        artifact["unreviewed"] = True
        mutated_bytes = _canonical_bytes(artifact)
    (tmp_path / "runtime-environment-attestation.json").write_bytes(
        mutated_bytes
    )
    check = EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
    payload = _payload(check, subject=manifest.subject)
    facts = cast(dict[str, object], payload["facts"])
    attestation = cast(
        dict[str, object],
        facts["runtime_environment_attestation"],
    )
    attestation["sha256"] = hashlib.sha256(mutated_bytes).hexdigest()
    manifest = _replace_artifact(
        manifest,
        tmp_path,
        check,
        _canonical_bytes(payload),
    )

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


@pytest.mark.parametrize(
    ("field", "mutated"),
    [
        ("path", "evidence/runtime-environment-attestation.json"),
        ("canonicalization", "INSTALLED_VERSIONS_ONLY"),
        ("distribution_count", 1),
        ("runtime_environment_sha256", "0" * 64),
        ("sha256", "0" * 64),
    ],
)
def test_runtime_environment_attestation_binding_mutations_are_rejected(
    tmp_path: Path,
    field: str,
    mutated: object,
) -> None:
    manifest = _manifest(tmp_path)
    check = EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
    payload = _payload(check, subject=manifest.subject)
    facts = cast(dict[str, object], payload["facts"])
    attestation = cast(
        dict[str, object],
        facts["runtime_environment_attestation"],
    )
    attestation[field] = mutated
    manifest = _replace_artifact(
        manifest,
        tmp_path,
        check,
        _canonical_bytes(payload),
    )

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


def test_runtime_environment_artifact_argument_is_runtime_attestation_only() -> None:
    with pytest.raises(ValueError, match="only valid for runtime attestation"):
        paper_evidence_payload(
            EvidenceCheck.PUBLIC_MARKET_SOURCE,
            _subject(),
            runtime_environment_attestation_bytes=_runtime_environment_bytes(),
        )


def _add_top_level_key(payload: dict[str, object]) -> None:
    payload["reviewed"] = True


def _remove_required_fact(payload: dict[str, object]) -> None:
    facts = cast(dict[str, object], payload["facts"])
    del facts["public_only"]


def _mutate_subject_hash(payload: dict[str, object]) -> None:
    subject = cast(dict[str, object], payload["subject"])
    subject["config_hash"] = "f" * 64


def _mutate_check(payload: dict[str, object]) -> None:
    payload["check"] = EvidenceCheck.NO_WALLET_OR_SIGNER.value


@pytest.mark.parametrize(
    "mutate",
    [
        _add_top_level_key,
        _remove_required_fact,
        _mutate_subject_hash,
        _mutate_check,
    ],
    ids=["extra-key", "missing-fact", "wrong-subject", "wrong-check"],
)
def test_schema_completeness_and_binding_mutations_are_rejected(
    tmp_path: Path,
    mutate: Callable[[dict[str, object]], None],
) -> None:
    check = EvidenceCheck.PUBLIC_MARKET_SOURCE
    manifest = _manifest(tmp_path)
    payload = _payload(check, subject=manifest.subject)
    mutate(payload)
    manifest = _replace_artifact(manifest, tmp_path, check, _canonical_bytes(payload))

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


def test_non_canonical_evidence_is_rejected_after_hash_binding(tmp_path: Path) -> None:
    check = EvidenceCheck.DETERMINISTIC_REPLAY
    manifest = _manifest(tmp_path)
    pretty = json.dumps(
        _payload(check, subject=manifest.subject),
        allow_nan=False,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ).encode("utf-8")
    manifest = _replace_artifact(manifest, tmp_path, check, pretty)

    assert _semantic_failure_codes(manifest, tmp_path) == {
        "EVIDENCE_SEMANTIC_VERIFICATION_FAILED"
    }


@pytest.mark.parametrize(
    ("attribute", "mutated"),
    [
        ("purpose", AuthorizationPurpose.RESEARCH_REPLAY),
        ("environment", EnvironmentClass.TESTNET),
        ("environment_identity", "TESTNET"),
        ("execution_network", ExecutionNetwork.TESTNET),
        ("credential_scope", CredentialScope.TESTNET),
        ("order_capability", OrderCapability.TESTNET_ONLY),
    ],
)
def test_compiled_callback_rejects_wrong_runtime_scope(
    attribute: str,
    mutated: object,
) -> None:
    check = EvidenceCheck.PUBLIC_MARKET_SOURCE
    subject = _subject()
    artifact = _canonical_bytes(_payload(check, subject=subject))
    context = EvidenceVerificationContext(
        check=check,
        environment=EnvironmentClass.PAPER,
        purpose=AuthorizationPurpose.PAPER_RUNTIME,
        environment_identity="PAPER",
        execution_network=ExecutionNetwork.NONE,
        credential_scope=CredentialScope.NONE,
        order_capability=OrderCapability.SIMULATED_ONLY,
        subject=subject,
        profile_sha256=profile_for(EnvironmentClass.PAPER).profile_sha256,
        artifact_bytes=artifact,
    )
    verifier = authorization_module._COMPILED_EVIDENCE_VERIFIERS[
        (EnvironmentClass.PAPER, AuthorizationPurpose.PAPER_RUNTIME, check)
    ]

    assert verifier.verify(context) is True
    assert verifier.verify(replace(context, **{attribute: mutated})) is False


@pytest.mark.parametrize(
    "missing_key",
    sorted(_RISK_LIMITS),
)
def test_paper_builder_requires_every_exact_risk_key(missing_key: str) -> None:
    risk = dict(_RISK_LIMITS)
    del risk[missing_key]
    subject = _subject(risk_limits=risk)

    with pytest.raises(ValueError, match="bounded PAPER schema"):
        paper_evidence_payload(
            EvidenceCheck.BOUNDED_POSITION_NOTIONAL,
            subject,
            risk,
        )


@pytest.mark.parametrize(
    ("key", "mutated"),
    [
        ("max_gross_notional", "10000.01"),
        ("max_net_notional", "2500.01"),
        ("max_instrument_notional", "5000.01"),
        ("max_order_notional", "1000.01"),
        ("max_position_quantity", "10.01"),
        ("max_order_quantity", "2.01"),
        ("max_daily_loss", "500.01"),
        ("max_drawdown", "1000.01"),
        ("max_concurrent_orders", 5),
        ("stale_after_seconds", 11),
        ("unhedged_timeout_seconds", 61),
    ],
)
def test_paper_builder_rejects_risk_values_above_compiled_ceilings(
    key: str,
    mutated: object,
) -> None:
    risk = dict(_RISK_LIMITS)
    risk[key] = mutated
    subject = _subject(risk_limits=risk)

    with pytest.raises(ValueError, match="bounded PAPER schema"):
        paper_evidence_payload(
            EvidenceCheck.BOUNDED_POSITION_NOTIONAL,
            subject,
            risk,
        )


def test_paper_builder_rejects_uncompiled_candidate_and_source() -> None:
    with pytest.raises(ValueError, match="compiled Phase 12"):
        paper_evidence_payload(
            EvidenceCheck.PUBLIC_MARKET_SOURCE,
            _subject(candidate_id="phase05-cash-and-carry"),
        )
    with pytest.raises(ValueError, match="compiled Phase 12"):
        paper_evidence_payload(
            EvidenceCheck.PUBLIC_MARKET_SOURCE,
            _subject(source_identity="research-placeholder"),
        )
