from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

import hyperlab.paper.collector_source as collector_source_module
import scripts.generate_phase12_live_paper_artifacts as artifact_generator
from hyperlab.backtest.protocol import canonical_json
from hyperlab.environment_authorization import (
    EnvironmentClass,
    EnvironmentReadinessManifest,
    EvidenceCheck,
    current_paper_runtime_environment_sha256,
    paper_runtime_environment_attestation_bytes,
    profile_for,
    verify_environment_readiness,
)
from hyperlab.paper.collector_source import (
    HYPERLIQUID_MAINNET_PUBLIC_HTTP_URL,
    HYPERLIQUID_MAINNET_PUBLIC_WEBSOCKET_URL,
    PHASE12_PUBLIC_SOURCE_NAME,
    HyperliquidPaperPublicSource,
)
from hyperlab.paper.models import PaperRunConfig
from hyperlab.paper.pairs_strategy import (
    FrozenRobustPairsPaperConfig,
    FrozenRobustPairsPaperStrategy,
)
from scripts.generate_phase12_live_paper_artifacts import (
    ARTIFACT_INDEX,
    CANDIDATE_ID,
    CONFIG_ARTIFACT,
    READINESS_MANIFEST_ARTIFACT,
    RELEASE_CODE_MANIFEST_ARTIFACT,
    RUNTIME_ENVIRONMENT_ARTIFACT,
    SOURCE_IDENTITY_ARTIFACT,
    ArtifactDriftError,
    build_phase12_artifacts,
    build_release_code_manifest_bytes,
    build_source_identity_artifact_bytes,
    check_artifacts,
    write_artifacts,
)

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE_ROOT = ROOT / "config/paper" / CANDIDATE_ID


def test_windows_bootstrap_uses_hash_locked_dependencies_and_no_deps_editable() -> None:
    bootstrap = (ROOT / "scripts" / "bootstrap.ps1").read_text(
        encoding="utf-8"
    )

    assert (
        "-m pip install --require-hashes --requirement requirements-ci.lock"
        in bootstrap
    )
    assert "-m pip install --no-deps --editable ." in bootstrap
    assert "pip install --upgrade pip" not in bootstrap
    assert 'pip install -e ".[dev,research]"' not in bootstrap


def _config(root: Path = CANDIDATE_ROOT) -> PaperRunConfig:
    payload = json.loads((root / CONFIG_ARTIFACT).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return PaperRunConfig.from_dict(payload)


def test_checked_in_phase12_artifacts_regenerate_byte_for_byte(tmp_path: Path) -> None:
    expected = build_phase12_artifacts(repository_root=ROOT)

    assert len(expected) == 22
    assert sum(path.startswith("evidence/") for path in expected) == 16
    assert {
        ARTIFACT_INDEX,
        CONFIG_ARTIFACT,
        READINESS_MANIFEST_ARTIFACT,
        RELEASE_CODE_MANIFEST_ARTIFACT,
        RUNTIME_ENVIRONMENT_ARTIFACT,
        SOURCE_IDENTITY_ARTIFACT,
    } < set(expected)
    for relative_path, payload in expected.items():
        assert (CANDIDATE_ROOT / relative_path).read_bytes() == payload
        decoded = json.loads(payload)
        assert canonical_json(decoded).encode("utf-8") == payload

    regenerated_root = tmp_path / "candidate"
    regenerated = write_artifacts(regenerated_root, repository_root=ROOT)
    assert regenerated == expected
    assert check_artifacts(regenerated_root, repository_root=ROOT) == expected


def test_phase12_readiness_manifest_is_exact_semantic_paper_runtime() -> None:
    manifest = EnvironmentReadinessManifest.from_json_bytes(
        (CANDIDATE_ROOT / READINESS_MANIFEST_ARTIFACT).read_bytes(),
        require_canonical=True,
    )
    decision = verify_environment_readiness(manifest, evidence_root=CANDIDATE_ROOT)
    profile = profile_for(EnvironmentClass.PAPER)

    assert decision.ready
    assert decision.blockers == ()
    assert manifest.environment is EnvironmentClass.PAPER
    assert manifest.purpose is profile.purpose
    assert manifest.subject.candidate_id == CANDIDATE_ID
    assert manifest.subject.source_identity == PHASE12_PUBLIC_SOURCE_NAME
    assert set(manifest.evidence) == set(profile.required_checks)
    assert all(binding.relative_path.startswith("evidence/") for binding in manifest.evidence.values())
    assert all(
        binding.relative_path
        not in {CONFIG_ARTIFACT, SOURCE_IDENTITY_ARTIFACT}
        for binding in manifest.evidence.values()
    )



def test_generated_evidence_matches_implemented_store_and_transport_facts() -> None:
    def facts(check: EvidenceCheck) -> dict[str, object]:
        path = CANDIDATE_ROOT / "evidence" / f"{check.value.casefold()}.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert isinstance(payload, dict)
        decoded = payload["facts"]
        assert isinstance(decoded, dict)
        return decoded

    crash = facts(EvidenceCheck.CRASH_RECOVERY)
    assert crash["journal_mode"] == "DELETE"
    assert crash["synchronous"] == "FULL"
    assert crash["concurrent_sqlite_writer_transactions_allowed"] is False
    assert crash["sqlite_writer_transaction_serialization"] == (
        "BEGIN_IMMEDIATE_WITH_EXPECTED_SEQUENCE_AND_DURABLE_HEAD_HASH_GUARDS"
    )
    assert crash["append_transaction"] == "BEGIN_IMMEDIATE_ATOMIC"
    assert crash["atomic_commit_contents"] == [
        "INBOX_INPUT",
        "EVENTS",
        "LEDGER_TRANSACTIONS_AND_ENTRIES",
        "PROJECTION_CURRENT_HEAD",
        "PROJECTION_HISTORY",
        "ALERTS",
        "COMMIT_RECORD",
        "RUN_CURRENT_HEAD",
    ]
    assert crash["durable_inbox_payload_hash_persisted"] is True
    assert crash["event_hash_chain_persisted"] is True
    assert crash["commit_hash_chain_persisted"] is True
    assert "sqlite_wal_enabled" not in crash
    assert "durable_inbox_before_processing" not in crash

    expected_runtime_lease = {
        "canonical_database_path": "RESOLVE_STRICT_THEN_OS_PATH_NORMCASE",
        "contention_action": "BLOCK_SECOND_RUNTIME_OR_STANDALONE_REPLAY_OR_RECONCILE",
        "identity_fields": [
            "CANONICAL_DATABASE_PATH",
            "RUN_ID",
            "LEASE_SCHEMA",
        ],
        "identity_hash": "CANONICAL_JSON_SHA256",
        "lock_file_pattern": ".{DATABASE_NAME}.paper-runtime-{IDENTITY_SHA256}.lock",
        "lock_file_deletion_required_on_close": False,
        "lock_file_directory": "DATABASE_PARENT",
        "lock_mode": "NONBLOCKING_EXCLUSIVE_OS_LOCK",
        "lock_payload": "ONE_NUL_BYTE_FSYNCED_WHEN_EMPTY",
        "platform_backends": [
            "WINDOWS_MSVCRT_LK_NBLCK_FIRST_BYTE",
            "POSIX_FCNTL_FLOCK_EX_NB",
        ],
        "schema": "paper-runtime-exclusive-os-lock-v1",
        "os_crash_releases_lock": True,
        "scope": "EXACT_CANONICAL_DATABASE_AND_RUN",
        "read_only_status_report_require_lease": False,
        "operator_pause_resume_kill_require_lease": False,
        "runtime_acquired_after_release_and_frozen_binding_checks": True,
        "runtime_acquired_before_engine_start_startup_reconciliation_and_public_source_start": True,
        "runtime_admission_failure_releases_lock": True,
        "runtime_held_until_close": True,
        "second_runtime_for_exact_database_and_run_rejected": True,
        "standalone_reconcile_acquired_after_release_check": True,
        "standalone_reconcile_failure_releases_lock": True,
        "standalone_reconcile_held_until_completion": True,
        "standalone_reconcile_requires_stopped_runtime": True,
        "standalone_replay_acquired_after_release_check": True,
        "standalone_replay_failure_releases_lock": True,
        "standalone_replay_held_until_completion": True,
        "standalone_replay_requires_stopped_runtime": True,
    }
    assert crash["runtime_lease"] == expected_runtime_lease

    runtime = facts(EvidenceCheck.RUNTIME_SOURCE_ATTESTATION)
    assert runtime["frozen_runtime_cadence"] == {
        "runtime_source_poll_timeout_seconds": 0.25,
        "runtime_timer_interval_seconds": 1.0,
    }
    assert runtime["runtime_cadence_must_equal_frozen_config_before_lease"] is True
    assert runtime["runtime_lease"] == expected_runtime_lease
    assert runtime["release_code_binding"] == {
        "canonical_config_field": "release_code_sha256",
        "current_checkout_digest_required_during_construction_and_before_start": True,
        "durable_config_snapshot": "paper_runs.config_json",
        "run_config_hash_binds_canonical_snapshot": True,
        "run_start_config_hash_binds_canonical_snapshot": True,
    }
    assert runtime["runtime_environment_binding"] == {
        "artifact_path": RUNTIME_ENVIRONMENT_ARTIFACT,
        "canonical_config_field": "runtime_environment_sha256",
        "cpython_runtime_facts_bound": [
            "abi_flags",
            "byteorder",
            "cache_tag",
            "hexversion",
            "implementation",
            "implementation_version",
            "platform_machine",
            "platform_system",
            "platform_tag",
            "pointer_bits",
            "python_compiler",
            "python_version",
            "version_info",
        ],
        "exact_locked_required_distributions": True,
        "extra_installed_distributions_allowed": True,
        "preflight_current_environment_required": True,
        "runtime_rechecked_before_lease_and_immediately_before_source_start": True,
    }

    audit = facts(EvidenceCheck.FULL_AUDIT_LOG)
    assert audit["append_only_tables"] == [
        "paper_alerts",
        "paper_commits",
        "paper_events",
        "paper_inbox",
        "paper_ledger_entries",
        "paper_ledger_transactions",
        "paper_projection_history",
    ]
    assert audit["mutable_current_head_tables"] == [
        "paper_projections",
        "paper_runs",
    ]
    assert audit["persisted_hash_fields"] == {
        "paper_alerts": ["payload_hash"],
        "paper_commits": [
            "alert_hashes_json",
            "commit_hash",
            "event_hashes_json",
            "ledger_hashes_json",
            "previous_commit_hash",
            "projection_hash",
        ],
        "paper_events": ["event_hash", "payload_hash", "previous_hash"],
        "paper_inbox": ["commit_hash", "payload_hash"],
        "paper_ledger_entries": ["entry_hash"],
        "paper_ledger_transactions": ["transaction_hash"],
        "paper_projection_history": ["event_head_hash", "projection_hash"],
        "paper_projections": ["event_head_hash", "projection_hash"],
        "paper_runs": [
            "commit_head_hash",
            "config_hash",
            "event_head_hash",
            "projection_hash",
        ],
    }
    assert "append_only" not in audit
    assert "categories" not in audit
    serialized_audit = canonical_json(audit)
    assert "RUNTIME_STOP" not in serialized_audit
    assert "ENVIRONMENT_AUTHORIZATION" not in serialized_audit

    source = facts(EvidenceCheck.PUBLIC_MARKET_SOURCE)
    assert source["bootstrap_timeout_seconds"] == 120.0
    assert source["descriptor_schema_version"] == 1
    assert source["network"] == "mainnet"
    assert source["rest_l2_book_projection"] == "BBO_BOOTSTRAP_AND_RESYNC_ONLY"
    assert source["source_kind"] == "PUBLIC_NORMALIZED"
    assert source["redirects_allowed"] is False
    assert source["http_final_url_must_equal_request"] is True
    assert source["http_required_status"] == 200
    assert source["websocket_redirect_limit"] == 0
    assert source["websocket_required_http_status"] == 101
    assert source["rest_methods"] == [
        "metaAndAssetCtxs",
        "spotMetaAndAssetCtxs",
        "fundingHistory",
        "l2Book",
    ]

    normalized = facts(EvidenceCheck.NORMALIZED_MARKET_EVENT_SCHEMA)
    assert normalized["adapter_schema_version"] == 9
    assert normalized["bbo_tradability_policy"] == (
        "REST_BOOTSTRAP_NONTRADABLE_POST_CONNECT_EXACT_WEBSOCKET_LINEAGE_REQUIRED_MALFORMED_TERMINAL_V2"
    )
    assert normalized["feed_contract"] == (
        "SOLE_COLLECTOR_NORMALIZED_BBO_CONNECTION_FUNDING_BOUNDED_PENDING_BBO_LATEST_VALUE_V9"
    )
    assert normalized["malformed_bbo_policy"] == (
        "TERMINAL_SOURCE_FAILURE_RESTART_AND_RESYNC_REQUIRED_NO_SILENT_DROP_V1"
    )
    assert normalized["pending_bbo_coalescing"] == (
        "LATEST_PER_INSTRUMENT_PER_UTC_MINUTE_BETWEEN_CONTROL_BARRIERS_V1"
    )
    assert normalized["gap_or_stale_action"] == "PAUSE_AND_NO_EXECUTION"
    assert normalized["global_connection_policy"] == (
        "MULTI_INSTRUMENT_GLOBAL_EVENT_SORTED_ORDINAL_INITIAL_BOOTSTRAP_CONNECT_HEALTH_ONLY_V4"
    )
    assert normalized["rest_bootstrap_execution_eligible"] is False
    assert (
        normalized["rest_bootstrap_lineage"]
        == "NON_EXECUTABLE_INITIALIZATION_ONLY"
    )
    assert normalized["websocket_lineage_required_after_connect"] is True
    assert "lineage_required" not in normalized

    restart = facts(EvidenceCheck.RESTART_RECOVERY)
    assert restart["duplicate_inputs_idempotent"] is True
    assert restart["duplicate_economic_effects_allowed"] is False
    assert "duplicate_input_reprocessing_allowed" not in restart


def test_release_code_manifest_binds_the_exact_reviewed_checkout() -> None:
    release_bytes = (CANDIDATE_ROOT / RELEASE_CODE_MANIFEST_ARTIFACT).read_bytes()
    assert release_bytes == build_release_code_manifest_bytes(repository_root=ROOT)
    release = json.loads(release_bytes)
    assert isinstance(release, dict)
    assert canonical_json(release).encode("utf-8") == release_bytes
    files = release["files"]
    assert isinstance(files, dict)
    expected_paths = {
        "pyproject.toml",
        "requirements-runtime.lock",
        "scripts/generate_phase12_live_paper_artifacts.py",
        *(
            path.relative_to(ROOT).as_posix()
            for path in (ROOT / "src/hyperlab").rglob("*.py")
            if path.is_file()
        ),
    }
    assert set(files) == expected_paths
    assert {
        "src/hyperlab/environment_authorization.py",
        "src/hyperlab/cli.py",
        "src/hyperlab/paper/collector_source.py",
        "src/hyperlab/paper/engine.py",
        "src/hyperlab/paper/models.py",
        "src/hyperlab/paper/pairs_strategy.py",
        "src/hyperlab/paper/public_source.py",
        "src/hyperlab/paper/runtime.py",
        "src/hyperlab/paper/store.py",
    } < set(files)
    core = {
        "artifact_schema_version": release["artifact_schema_version"],
        "candidate_id": release["candidate_id"],
        "canonicalization": release["canonicalization"],
        "files": files,
    }
    assert hashlib.sha256(canonical_json(core).encode("utf-8")).hexdigest() == (
        release["release_code_sha256"]
    )

    runtime_evidence = json.loads(
        (
            CANDIDATE_ROOT
            / "evidence"
            / f"{EvidenceCheck.RUNTIME_SOURCE_ATTESTATION.value.casefold()}.json"
        ).read_bytes()
    )
    attestation = runtime_evidence["facts"]["release_code_manifest"]
    assert attestation == {
        "canonicalization": release["canonicalization"],
        "file_count": len(files),
        "path": RELEASE_CODE_MANIFEST_ARTIFACT,
        "release_code_sha256": release["release_code_sha256"],
        "sha256": hashlib.sha256(release_bytes).hexdigest(),
    }
    assert _config().release_code_sha256 == release["release_code_sha256"]
    assert runtime_evidence["facts"]["engine_semantic_build_hash"] == (
        _config().engine_build_hash
    )


def test_runtime_environment_artifact_binds_exact_current_python_and_config() -> None:
    artifact_bytes = (
        CANDIDATE_ROOT / RUNTIME_ENVIRONMENT_ARTIFACT
    ).read_bytes()
    assert artifact_bytes == paper_runtime_environment_attestation_bytes(ROOT)
    artifact = json.loads(artifact_bytes)
    assert isinstance(artifact, dict)
    assert canonical_json(artifact).encode("utf-8") == artifact_bytes
    assert artifact["distribution_count"] == 34
    assert artifact["extras_allowed"] is True
    assert artifact["runtime_environment_sha256"] == (
        current_paper_runtime_environment_sha256()
    )

    runtime_evidence = json.loads(
        (
            CANDIDATE_ROOT
            / "evidence"
            / f"{EvidenceCheck.RUNTIME_SOURCE_ATTESTATION.value.casefold()}.json"
        ).read_bytes()
    )
    attestation = runtime_evidence["facts"]["runtime_environment_attestation"]
    assert attestation == {
        "canonicalization": artifact["canonicalization"],
        "distribution_count": 34,
        "path": RUNTIME_ENVIRONMENT_ARTIFACT,
        "runtime_environment_sha256": artifact[
            "runtime_environment_sha256"
        ],
        "sha256": hashlib.sha256(artifact_bytes).hexdigest(),
    }
    assert _config().runtime_environment_sha256 == (
        artifact["runtime_environment_sha256"]
    )


def test_frozen_config_binds_strategy_source_costs_and_practical_risk() -> None:
    config = _config()
    source_bytes = (CANDIDATE_ROOT / SOURCE_IDENTITY_ARTIFACT).read_bytes()
    source_identity = json.loads(source_bytes)
    frozen_strategy = FrozenRobustPairsPaperConfig()
    strategy = FrozenRobustPairsPaperStrategy(frozen_strategy)

    assert config.schema_version == 2
    assert config.environment == "PAPER"
    assert config.run_kind == "TECHNICAL"
    assert config.validation_started_at == datetime(2026, 8, 17, tzinfo=UTC)
    assert not config.economically_eligible
    assert config.economic_prerequisites_satisfied is False
    assert config.economic_prerequisites_evidence_hash is None
    assert config.data_calibration_status == "UNCALIBRATED"
    assert config.data_source == PHASE12_PUBLIC_SOURCE_NAME
    assert config.data_hash == hashlib.sha256(source_bytes).hexdigest()
    assert config.required_instruments == ("HL:BTC:perp", "HL:ETH:perp")
    assert config.runtime_timer_interval_seconds == 1.0
    assert config.runtime_source_poll_timeout_seconds == 0.25
    assert len(config.release_code_sha256) == 64
    assert len(config.runtime_environment_sha256) == 64
    assert config.strategy_name == strategy.strategy_name
    assert config.strategy_hash == strategy.strategy_hash
    assert config.parameters["strategy_configuration"] == frozen_strategy.to_dict()

    assert source_identity["public_only"] is True
    assert source_identity["adapter_schema_version"] == 9
    assert source_identity["pending_bbo_coalescing"] == (
        "LATEST_PER_INSTRUMENT_PER_UTC_MINUTE_BETWEEN_CONTROL_BARRIERS_V1"
    )
    assert source_identity["bbo_tradability_policy"] == (
        "REST_BOOTSTRAP_NONTRADABLE_POST_CONNECT_EXACT_WEBSOCKET_LINEAGE_REQUIRED_MALFORMED_TERMINAL_V2"
    )
    assert source_identity["feed_contract"] == (
        "SOLE_COLLECTOR_NORMALIZED_BBO_CONNECTION_FUNDING_BOUNDED_"
        "PENDING_BBO_LATEST_VALUE_V9"
    )
    assert source_identity["malformed_bbo_policy"] == (
        "TERMINAL_SOURCE_FAILURE_RESTART_AND_RESYNC_REQUIRED_NO_SILENT_DROP_V1"
    )
    assert source_identity["global_connection_policy"] == (
        "MULTI_INSTRUMENT_GLOBAL_EVENT_SORTED_ORDINAL_INITIAL_BOOTSTRAP_CONNECT_HEALTH_ONLY_V4"
    )
    assert source_identity["transport"]["http_info_endpoint"] == HYPERLIQUID_MAINNET_PUBLIC_HTTP_URL
    assert (
        source_identity["transport"]["websocket_endpoint"]
        == HYPERLIQUID_MAINNET_PUBLIC_WEBSOCKET_URL
    )
    assert source_identity["transport"]["bootstrap_timeout_seconds"] == 120.0
    assert source_identity["transport"]["public_rest_methods"] == [
        "metaAndAssetCtxs",
        "spotMetaAndAssetCtxs",
        "fundingHistory",
        "l2Book",
    ]
    assert source_identity["transport"]["rest_l2_book_projection"] == "BBO_BOOTSTRAP_AND_RESYNC_ONLY"
    assert source_identity["transport"]["redirects_allowed"] is False
    assert source_identity["transport"]["http_final_url_must_equal_request"] is True
    assert source_identity["transport"]["http_required_status"] == 200
    assert source_identity["transport"]["websocket_redirect_limit"] == 0
    assert source_identity["transport"]["websocket_required_http_status"] == 101
    assert (
        source_identity["transport"]["transport_schema"]
        == "hyperliquid-paper-public-transport-v2"
    )
    assert source_identity["transport"]["credential_scope"] == "NONE"
    assert source_identity["transport"]["orders_enabled"] is False
    assert source_identity["transport"]["wallet_or_signer_present"] is False

    assert config.risk.to_dict() == {
        "max_concurrent_orders": 2,
        "max_daily_loss": "100",
        "max_drawdown": "200",
        "max_gross_notional": "2000",
        "max_instrument_notional": "1000",
        "max_net_notional": "1000",
        "max_order_notional": "250",
        "max_order_quantity": "0.25",
        "max_position_quantity": "1",
        "stale_after_seconds": 10,
        "unhedged_timeout_seconds": 20,
    }
    execution = config.execution
    assert execution.calibration_status == "UNCALIBRATED"
    assert execution.maker_fill.base_probability == 0.0
    assert execution.ioc_fill_probability == Decimal("0.80")
    assert execution.ioc_extra_slippage_bps == Decimal("3")
    assert execution.cost_multiplier == Decimal("1.25")
    assert execution.ack_latency_ms == 250
    assert execution.fill_latency_ms == 500
    assert execution.leg_delay_ms == 750
    assert execution.cost_schedule is not None
    assert execution.cost_schedule.calibration_status == "UNCALIBRATED"
    for instrument in config.required_instruments:
        rule = execution.cost_schedule.lookup(config.validation_started_at, instrument)
        assert rule.maker_fee_bps == 1.5
        assert rule.taker_fee_bps == 4.5
        assert rule.slippage.base_bps == 2.0
        assert rule.slippage.impact_coefficient_bps == 25.0
        assert rule.slippage.max_participation == 0.05


def test_source_artifact_matches_lazy_factory_without_constructing_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def forbidden_transport(*_args: object, **_kwargs: object) -> object:
        calls.append("transport")
        raise AssertionError("offline identity verification must not construct a transport")

    monkeypatch.setattr(
        collector_source_module,
        "HyperliquidPublicClient",
        forbidden_transport,
    )
    monkeypatch.setattr(
        collector_source_module,
        "WebsocketClientFactory",
        forbidden_transport,
    )
    monkeypatch.setattr(collector_source_module, "PublicCollector", forbidden_transport)

    generated_identity = build_source_identity_artifact_bytes()
    assert calls == []

    source = HyperliquidPaperPublicSource.create_mainnet(
        runtime_status_path=tmp_path / "runtime-status.json"
    )
    try:
        identity_bytes = (CANDIDATE_ROOT / SOURCE_IDENTITY_ARTIFACT).read_bytes()
        config = _config()
        assert calls == []
        assert source.started is False
        assert generated_identity == identity_bytes
        assert source.identity_artifact_bytes == identity_bytes
        assert source.descriptor.source == PHASE12_PUBLIC_SOURCE_NAME
        assert source.descriptor.source_kind == "PUBLIC_NORMALIZED"
        assert source.descriptor.schema_version == 1
        assert source.descriptor.public_only is True
        assert source.descriptor.bootstrap_timeout_seconds == 120.0
        assert source.descriptor.data_hash == hashlib.sha256(identity_bytes).hexdigest()
        assert source.descriptor.data_hash == config.data_hash
    finally:
        source.close()
    assert calls == []


def test_artifact_check_is_read_only_and_tampering_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_root = tmp_path / "candidate"
    write_artifacts(candidate_root, repository_root=ROOT)

    original_write_bytes = Path.write_bytes

    def forbidden_write(_path: Path, _data: bytes) -> int:
        raise AssertionError("--check path must not write")

    monkeypatch.setattr(Path, "write_bytes", forbidden_write)
    check_artifacts(candidate_root, repository_root=ROOT)
    monkeypatch.setattr(Path, "write_bytes", original_write_bytes)

    binding = EnvironmentReadinessManifest.from_json_bytes(
        (candidate_root / READINESS_MANIFEST_ARTIFACT).read_bytes(),
        require_canonical=True,
    ).evidence[EvidenceCheck.DETERMINISTIC_ACCOUNTING]
    target = candidate_root / binding.relative_path
    target.write_bytes(target.read_bytes() + b" ")

    manifest = EnvironmentReadinessManifest.from_json_bytes(
        (candidate_root / READINESS_MANIFEST_ARTIFACT).read_bytes(),
        require_canonical=True,
    )
    decision = verify_environment_readiness(manifest, evidence_root=candidate_root)
    assert not decision.ready
    assert "ARTIFACT_HASH_MISMATCH" in {blocker.code for blocker in decision.blockers}
    with pytest.raises(
        ArtifactDriftError,
        match=r"changed:evidence/deterministic_accounting\.json",
    ):
        check_artifacts(candidate_root, repository_root=ROOT)


def test_generator_rejects_an_early_release_file_changed_after_its_read(
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
                "",
            )
        ),
        encoding="utf-8",
    )
    original_reader = artifact_generator._canonical_release_file_bytes

    def racing_reader(root: Path, relative_path: str) -> bytes:
        payload = original_reader(root, relative_path)
        if relative_path == "requirements-runtime.lock":
            (root / "pyproject.toml").write_text(
                "[project]\nname = 'mutated-after-read'\n",
                encoding="utf-8",
            )
        return payload

    monkeypatch.setattr(
        artifact_generator,
        "_canonical_release_file_bytes",
        racing_reader,
    )

    with pytest.raises(ValueError, match="release code changed while"):
        build_release_code_manifest_bytes(repository_root=repository)


def test_artifact_check_and_write_reject_unexpected_files_without_mutation(
    tmp_path: Path,
) -> None:
    candidate_root = tmp_path / "candidate"
    write_artifacts(candidate_root, repository_root=ROOT)
    stale = candidate_root / "evidence" / "stale-v3.json"
    stale.write_bytes(b"{}")
    before = {
        path.relative_to(candidate_root).as_posix(): path.read_bytes()
        for path in candidate_root.rglob("*")
        if path.is_file()
    }

    with pytest.raises(
        ArtifactDriftError,
        match=r"unexpected:evidence/stale-v3\.json",
    ):
        check_artifacts(candidate_root, repository_root=ROOT)
    with pytest.raises(
        ArtifactDriftError,
        match=r"unexpected generated artifact files: evidence/stale-v3\.json",
    ):
        write_artifacts(candidate_root, repository_root=ROOT)

    after = {
        path.relative_to(candidate_root).as_posix(): path.read_bytes()
        for path in candidate_root.rglob("*")
        if path.is_file()
    }
    assert after == before
