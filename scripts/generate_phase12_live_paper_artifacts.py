from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPOSITORY_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from hyperlab.backtest.costs import CostRule, CostSchedule, SlippageModel  # noqa: E402
from hyperlab.backtest.execution import MakerFillModel  # noqa: E402
from hyperlab.backtest.protocol import canonical_json, canonical_sha256  # noqa: E402
from hyperlab.environment_authorization import (  # noqa: E402
    AuthorizationPurpose,
    EnvironmentClass,
    EnvironmentReadinessManifest,
    EvidenceCheck,
    ReadinessArtifactBinding,
    ReadinessSubject,
    paper_evidence_payload,
    paper_runtime_environment_attestation_bytes,
    profile_for,
    verify_environment_readiness,
)
from hyperlab.paper.collector_source import (  # noqa: E402
    PHASE12_PHASE05_PUBLIC_SOURCE_NAME,
    PHASE12_PUBLIC_SOURCE_NAME,
    HyperliquidPaperPublicSource,
)
from hyperlab.paper.models import PaperExecutionConfig, PaperRiskLimits, PaperRunConfig  # noqa: E402
from hyperlab.paper.pairs_strategy import (  # noqa: E402
    FrozenRobustPairsPaperConfig,
    FrozenRobustPairsPaperStrategy,
)
from hyperlab.paper.phase05_portfolio import (  # noqa: E402
    build_phase05_phase08_paper_foundation,
)

CANDIDATE_ID = "phase08-robust-pairs-btc-eth-paper-v1"
CANDIDATE_DIRECTORY = Path("config/paper") / CANDIDATE_ID
MULTISTRATEGY_CANDIDATE_ID = "phase08-phase05-multistrategy-paper-v1"
MULTISTRATEGY_CANDIDATE_DIRECTORY = Path("config/paper") / MULTISTRATEGY_CANDIDATE_ID
DEFAULT_OUTPUT_ROOT = REPOSITORY_ROOT / MULTISTRATEGY_CANDIDATE_DIRECTORY
FEE_ARTIFACT_PATH = Path("config/paper/hyperliquid-tier0-fees-2026-08-16.json")

CONFIG_ARTIFACT = "paper-config.json"
SOURCE_IDENTITY_ARTIFACT = "source-identity.json"
READINESS_MANIFEST_ARTIFACT = "readiness-manifest.json"
RELEASE_CODE_MANIFEST_ARTIFACT = "release-code-manifest.json"
RUNTIME_ENVIRONMENT_ARTIFACT = "runtime-environment-attestation.json"
ARTIFACT_INDEX = "artifact-index.json"
DEPLOYMENT_GATE_ARTIFACT = "technical-deployment-gate.json"
TECHNICAL_EVIDENCE_ARTIFACT = "technical-evidence.json"

_RELEASE_CODE_SCHEMA_VERSION = 1
_RELEASE_CODE_CANONICALIZATION = (
    "UTF8_LF_CLI_DERIVED_BINDINGS_REDACTED_OR_BINARY_V1"
)
_RELEASE_CODE_FIXED_PATHS = (
    "pyproject.toml",
    "requirements-runtime.lock",
    "scripts/certify_storage_v4_phase1b.py",
    "scripts/certify_storage_v4_phase1c.py",
    "scripts/certify_storage_v4_phase1d_linux.py",
    "scripts/generate_phase12_live_paper_artifacts.py",
)
_CLI_DERIVED_BINDING_NAMES = (
    "_PHASE12_PAPER_CONFIG_HASH",
    "_PHASE12_PAPER_READINESS_MANIFEST_SHA256",
    "_PHASE12_PAPER_READINESS_PROFILE_SHA256",
    "_PHASE12_MULTISTRATEGY_CONFIG_HASH",
    "_PHASE12_MULTISTRATEGY_READINESS_MANIFEST_SHA256",
    "_PHASE12_MULTISTRATEGY_READINESS_PROFILE_SHA256",
)
_MAX_RELEASE_FILE_BYTES = 1024 * 1024
RUNTIME_TIMER_INTERVAL_SECONDS = 1.0
RUNTIME_SOURCE_POLL_TIMEOUT_SECONDS = 0.25

VALIDATION_STARTED_AT = datetime(2026, 8, 17, tzinfo=UTC)
MULTISTRATEGY_VALIDATION_STARTED_AT = datetime(2026, 8, 18, 12, tzinfo=UTC)
FEE_OBSERVED_AT = "2026-08-16T21:06:42Z"


class ArtifactDriftError(RuntimeError):
    """Checked-in Phase 12 artifacts differ from deterministic regeneration."""


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return canonical_json(value).encode("utf-8")


def _release_stat_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


def _release_path_is_link(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or (callable(is_junction) and bool(is_junction()))


def _resolve_release_path(root: Path, relative_path: str) -> Path:
    pure = PurePosixPath(relative_path)
    if (
        not relative_path
        or "\\" in relative_path
        or pure.is_absolute()
        or pure.as_posix() != relative_path
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        raise ValueError("release path is not an exact safe POSIX-relative path")
    resolved_root = root.resolve(strict=True)
    if not resolved_root.is_dir():
        raise ValueError("release root is not a directory")
    cursor = resolved_root
    for part in pure.parts:
        cursor = cursor / part
        if _release_path_is_link(cursor):
            raise ValueError("release paths may not traverse links or junctions")
    resolved = cursor.resolve(strict=True)
    try:
        resolved.relative_to(resolved_root)
    except ValueError as error:
        raise ValueError("release path escapes its repository root") from error
    if not resolved.is_file():
        raise ValueError("release path must identify a regular file")
    return resolved


def _read_stable_release_file(path: Path) -> bytes:
    before = path.stat()
    if before.st_size > _MAX_RELEASE_FILE_BYTES:
        raise ValueError("release file exceeds the bounded size")
    with path.open("rb") as stream:
        handle_before = os.fstat(stream.fileno())
        payload = stream.read(_MAX_RELEASE_FILE_BYTES + 1)
        handle_after = os.fstat(stream.fileno())
    after = path.stat()
    identities = (
        _release_stat_identity(before),
        _release_stat_identity(handle_before),
        _release_stat_identity(handle_after),
        _release_stat_identity(after),
    )
    if len(payload) > _MAX_RELEASE_FILE_BYTES or len(set(identities)) != 1:
        raise ValueError("release file changed while it was being hashed")
    return payload


def _canonical_release_file_bytes(root: Path, relative_path: str) -> bytes:
    payload = _read_stable_release_file(
        _resolve_release_path(root, relative_path)
    )
    if b"\0" in payload:
        return payload
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        return payload
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if relative_path == "src/hyperlab/cli.py":
        for name in _CLI_DERIVED_BINDING_NAMES:
            pattern = re.compile(
                rf'(?m)^{re.escape(name)} = "[0-9a-f]{{64}}"$'
            )
            text, count = pattern.subn(
                f'{name} = "<PHASE12_DERIVED_BINDING_SHA256>"',
                text,
            )
            if count != 1:
                raise ValueError(
                    f"release CLI binding {name!r} is missing or ambiguous"
                )
    return text.encode("utf-8")


def _release_code_paths(repository_root: Path) -> tuple[str, ...]:
    resolved_root = repository_root.resolve(strict=True)
    source_root = (resolved_root / "src" / "hyperlab").resolve(strict=True)
    try:
        source_root.relative_to(resolved_root)
    except ValueError as error:
        raise ValueError("release source root escapes the repository") from error
    selected = set(_RELEASE_CODE_FIXED_PATHS)
    for path in source_root.rglob("*.py"):
        if path.is_file():
            selected.add(path.relative_to(resolved_root).as_posix())
    return tuple(sorted(selected))


def _release_path_identities(
    repository_root: Path,
    relative_paths: tuple[str, ...],
) -> dict[str, tuple[int, int, int, int]]:
    return {
        relative_path: _release_stat_identity(
            _resolve_release_path(repository_root, relative_path).stat()
        )
        for relative_path in relative_paths
    }


def _release_code_core(
    files: Mapping[str, str],
    *,
    candidate_id: str = CANDIDATE_ID,
) -> dict[str, object]:
    if candidate_id not in {CANDIDATE_ID, MULTISTRATEGY_CANDIDATE_ID}:
        raise ValueError("unsupported Phase 12 Paper candidate")
    return {
        "artifact_schema_version": _RELEASE_CODE_SCHEMA_VERSION,
        "candidate_id": candidate_id,
        "canonicalization": _RELEASE_CODE_CANONICALIZATION,
        "files": dict(files),
    }


def _release_code_sha256(
    files: Mapping[str, str],
    *,
    candidate_id: str = CANDIDATE_ID,
) -> str:
    return _sha256(_canonical_bytes(_release_code_core(files, candidate_id=candidate_id)))


def build_release_code_manifest_bytes(
    *,
    repository_root: Path = REPOSITORY_ROOT,
    candidate_id: str = CANDIDATE_ID,
) -> bytes:
    """Build the release digest independently from the compiled verifier."""

    root = repository_root.resolve(strict=True)
    selected_paths = _release_code_paths(root)
    identities_before = _release_path_identities(root, selected_paths)
    files = {
        relative_path: _sha256(
            _canonical_release_file_bytes(root, relative_path)
        )
        for relative_path in selected_paths
    }
    paths_after = _release_code_paths(root)
    if (
        paths_after != selected_paths
        or _release_path_identities(root, paths_after) != identities_before
    ):
        raise ValueError(
            "release code changed while the manifest was built"
        )
    return _canonical_bytes(
        {
            **_release_code_core(files, candidate_id=candidate_id),
            "release_code_sha256": _release_code_sha256(
                files,
                candidate_id=candidate_id,
            ),
        }
    )


def _release_code_manifest_metadata(payload: bytes) -> tuple[int, str]:
    decoded = json.loads(payload.decode("utf-8"))
    if not isinstance(decoded, Mapping) or payload != _canonical_bytes(dict(decoded)):
        raise ValueError("generated release-code manifest is not canonical")
    files = decoded.get("files")
    release_code_sha256 = decoded.get("release_code_sha256")
    if (
        not isinstance(files, Mapping)
        or not files
        or not isinstance(release_code_sha256, str)
        or len(release_code_sha256) != 64
    ):
        raise ValueError("generated release-code manifest metadata is invalid")
    return len(files), release_code_sha256


def _runtime_environment_metadata(
    payload: bytes,
    *,
    candidate_id: str = CANDIDATE_ID,
) -> tuple[int, str]:
    decoded = json.loads(payload.decode("utf-8"))
    expected_keys = {
        "artifact_schema_version",
        "candidate_id",
        "canonicalization",
        "distribution_count",
        "extras_allowed",
        "installed_distributions",
        "interpreter",
        "lock",
        "runtime_environment_sha256",
    }
    if (
        not isinstance(decoded, Mapping)
        or payload != _canonical_bytes(dict(decoded))
        or set(decoded) != expected_keys
        or decoded.get("artifact_schema_version") != 1
        or decoded.get("candidate_id") != candidate_id
        or decoded.get("extras_allowed") is not True
        or type(decoded.get("distribution_count")) is not int
        or int(decoded["distribution_count"]) <= 0
        or not isinstance(decoded.get("runtime_environment_sha256"), str)
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(decoded["runtime_environment_sha256"]),
        )
        is None
    ):
        raise ValueError("generated runtime-environment attestation is invalid")
    return (
        int(decoded["distribution_count"]),
        str(decoded["runtime_environment_sha256"]),
    )


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate fee artifact key: {key}")
        result[key] = value
    return result


def _validate_fee_artifact(payload: bytes) -> None:
    try:
        decoded = json.loads(payload.decode("utf-8"), object_pairs_hook=_strict_json_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"fee artifact is not strict UTF-8 JSON: {error}") from None
    if not isinstance(decoded, Mapping):
        raise ValueError("fee artifact must contain a JSON object")
    if (
        decoded.get("artifact_version") != 2
        or decoded.get("scope") != "OFFICIAL_PUBLIC_FEES_ONLY"
        or decoded.get("economic_eligibility") is not False
        or decoded.get("status") != "BLOCKED_INCOMPLETE_EXECUTION_CALIBRATION"
    ):
        raise ValueError("fee artifact lost its reviewed public-only non-economic scope")
    provenance = decoded.get("provenance")
    if not isinstance(provenance, Mapping) or provenance.get("retrieved_at_utc") != FEE_OBSERVED_AT:
        raise ValueError("fee artifact observation time differs from the frozen cost schedule")
    rules = decoded.get("official_fee_rules")
    if not isinstance(rules, list):
        raise ValueError("fee artifact official_fee_rules must be an array")
    perp_rules = [
        rule
        for rule in rules
        if isinstance(rule, Mapping) and rule.get("instrument_pattern") == "HL:*:perp"
    ]
    if len(perp_rules) != 1 or dict(perp_rules[0]) != {
        "instrument_pattern": "HL:*:perp",
        "maker_fee_bps": "1.5",
        "product_scope": "standard perpetuals",
        "taker_fee_bps": "4.5",
    }:
        raise ValueError("fee artifact no longer proves the frozen Tier-0 perpetual fee row")
    policy = decoded.get("policy")
    if not isinstance(policy, Mapping) or policy.get("tier") != "public tier 0":
        raise ValueError("fee artifact no longer identifies public Tier 0")
    discount_keys = (
        "aligned_quote_discount_assumed",
        "maker_rebate_assumed",
        "referral_discount_assumed",
        "staking_discount_assumed",
        "volume_discount_assumed",
    )
    if policy.get("account_or_private_data_used") is not False or any(
        policy.get(key) is not False for key in discount_keys
    ):
        raise ValueError("fee artifact may not use private account data or fee discounts")


def build_source_identity_artifact_bytes(*, multistrategy: bool = False) -> bytes:
    """Read canonical identity from the transport-lazy production factory."""

    source = HyperliquidPaperPublicSource.create_mainnet(
        runtime_status_path=(
            REPOSITORY_ROOT / ".tmp" / "phase12-artifact-generator-status.json"
        ),
        include_phase05_cash_and_carry=multistrategy,
    )
    try:
        identity_bytes = source.identity_artifact_bytes
        descriptor = source.descriptor
        expected_source = (
            PHASE12_PHASE05_PUBLIC_SOURCE_NAME
            if multistrategy
            else PHASE12_PUBLIC_SOURCE_NAME
        )
        if descriptor.source != expected_source:
            raise RuntimeError("lazy production source returned an unexpected identity")
        if descriptor.data_hash != _sha256(identity_bytes):
            raise RuntimeError("lazy production source descriptor hash is inconsistent")
        return identity_bytes
    finally:
        source.close()


def _risk_limits() -> PaperRiskLimits:
    return PaperRiskLimits(
        max_gross_notional=Decimal("2000"),
        max_net_notional=Decimal("1000"),
        max_instrument_notional=Decimal("1000"),
        max_order_notional=Decimal("250"),
        max_position_quantity=Decimal("1"),
        max_order_quantity=Decimal("0.25"),
        max_concurrent_orders=2,
        max_daily_loss=Decimal("100"),
        max_drawdown=Decimal("200"),
        stale_after_seconds=10,
        unhedged_timeout_seconds=20,
    )


def _execution_config() -> PaperExecutionConfig:
    # Current BBO spread and side-specific public depth are applied by the Paper
    # engine before these extra adverse assumptions. The schedule remains
    # deliberately UNCALIBRATED and cannot qualify an economic validation run.
    conservative_slippage = SlippageModel(
        base_bps=2.0,
        impact_coefficient_bps=25.0,
        exponent=0.5,
        max_participation=0.05,
    )
    fee_source = (
        "hyperliquid-public-tier0-observed-2026-08-16-"
        "plus-uncalibrated-conservative-ioc-v1"
    )
    cost_schedule = CostSchedule(
        rules=tuple(
            CostRule(
                instrument=instrument,
                maker_fee_bps=1.5,
                taker_fee_bps=4.5,
                slippage=conservative_slippage,
                effective_from=FEE_OBSERVED_AT,
                source=fee_source,
            )
            for instrument in ("HL:BTC:perp", "HL:ETH:perp")
        ),
        calibration_status="UNCALIBRATED",
    )
    return PaperExecutionConfig(
        maker_fill=MakerFillModel(
            base_probability=0.0,
            participation_decay=0.0,
            calibration_id="no-maker-orders-uncalibrated-phase12-v1",
            calibration_status="UNCALIBRATED",
        ),
        slippage=conservative_slippage,
        maker_fee_bps=Decimal("1.5"),
        taker_fee_bps=Decimal("4.5"),
        cost_multiplier=Decimal("1.25"),
        ioc_fill_probability=Decimal("0.80"),
        ioc_extra_slippage_bps=Decimal("3"),
        ack_latency_ms=250,
        fill_latency_ms=500,
        leg_delay_ms=750,
        cancel_latency_ms=250,
        maker_timeout_ms=1_000,
        calibration_status="UNCALIBRATED",
        source=fee_source,
        cost_schedule=cost_schedule,
    )


def build_paper_config(
    *,
    source_identity_bytes: bytes | None = None,
    fee_artifact_bytes: bytes | None = None,
    release_code_sha256: str | None = None,
    runtime_environment_sha256: str | None = None,
) -> PaperRunConfig:
    source_bytes = (
        build_source_identity_artifact_bytes()
        if source_identity_bytes is None
        else source_identity_bytes
    )
    if not isinstance(source_bytes, bytes) or not source_bytes:
        raise ValueError("source_identity_bytes must be non-empty canonical bytes")
    fee_bytes = (
        (REPOSITORY_ROOT / FEE_ARTIFACT_PATH).read_bytes()
        if fee_artifact_bytes is None
        else fee_artifact_bytes
    )
    if not isinstance(fee_bytes, bytes) or not fee_bytes:
        raise ValueError("fee_artifact_bytes must be non-empty bytes")
    _validate_fee_artifact(fee_bytes)
    frozen_release_code_sha256 = release_code_sha256
    if frozen_release_code_sha256 is None:
        _, frozen_release_code_sha256 = _release_code_manifest_metadata(
            build_release_code_manifest_bytes()
        )
    frozen_runtime_environment_sha256 = runtime_environment_sha256
    if frozen_runtime_environment_sha256 is None:
        _, frozen_runtime_environment_sha256 = _runtime_environment_metadata(
            paper_runtime_environment_attestation_bytes()
        )

    strategy_configuration = FrozenRobustPairsPaperConfig()
    strategy = FrozenRobustPairsPaperStrategy(strategy_configuration)
    source_hash = _sha256(source_bytes)
    return PaperRunConfig(
        strategy_name=strategy.strategy_name,
        strategy_hash=strategy.strategy_hash,
        parameters={
            "artifact_schema_version": 1,
            "candidate_id": CANDIDATE_ID,
            "cost_assumptions": {
                "account_fee_discount_assumed": False,
                "actual_bbo_depth_capacity_applied": True,
                "actual_bbo_spread_crossed": True,
                "adverse_fee_multiplier": "1.25",
                "economic_eligibility": False,
                "fee_artifact": {
                    "path": FEE_ARTIFACT_PATH.as_posix(),
                    "sha256": _sha256(fee_bytes),
                },
                "ioc_extra_slippage_bps": "3",
                "ioc_fill_probability": "0.80",
                "maker_fill_assumed": False,
                "status": "UNCALIBRATED",
                "taker_fee_bps_public_tier_0": "4.5",
            },
            "scope": {
                "authorizes_real_money": False,
                "gate_d_satisfied": False,
                "purpose": "PAPER_RUNTIME",
                "run_kind": "TECHNICAL",
                "validation_paper": False,
            },
            "source_identity_artifact": {
                "path": SOURCE_IDENTITY_ARTIFACT,
                "sha256": source_hash,
                "source": PHASE12_PUBLIC_SOURCE_NAME,
            },
            "strategy_configuration": strategy_configuration.to_dict(),
        },
        data_hash=source_hash,
        execution=_execution_config(),
        risk=_risk_limits(),
        seed=12_008,
        initial_cash=Decimal("10000"),
        validation_started_at=VALIDATION_STARTED_AT,
        run_kind="TECHNICAL",
        data_calibration_status="UNCALIBRATED",
        data_source=PHASE12_PUBLIC_SOURCE_NAME,
        economic_prerequisites_satisfied=False,
        required_instruments=("HL:BTC:perp", "HL:ETH:perp"),
        minimum_validation_cycles=1,
        schema_version=2,
        environment="PAPER",
        release_code_sha256=frozen_release_code_sha256,
        runtime_environment_sha256=frozen_runtime_environment_sha256,
        runtime_timer_interval_seconds=RUNTIME_TIMER_INTERVAL_SECONDS,
        runtime_source_poll_timeout_seconds=RUNTIME_SOURCE_POLL_TIMEOUT_SECONDS,
    )


def build_multistrategy_paper_config(
    *,
    release_code_sha256: str,
    runtime_environment_sha256: str,
) -> tuple[PaperRunConfig, bytes]:
    foundation = build_phase05_phase08_paper_foundation(
        runtime_status_path=(
            REPOSITORY_ROOT / ".tmp" / "phase12-multistrategy-artifact-generator-status.json"
        ),
        validation_started_at=MULTISTRATEGY_VALIDATION_STARTED_AT,
        release_code_sha256=release_code_sha256,
        runtime_environment_sha256=runtime_environment_sha256,
    )
    try:
        source_bytes = foundation.source.identity_artifact_bytes
        config = replace(
            foundation.config,
            release_code_sha256=release_code_sha256,
            runtime_environment_sha256=runtime_environment_sha256,
            runtime_timer_interval_seconds=RUNTIME_TIMER_INTERVAL_SECONDS,
            runtime_source_poll_timeout_seconds=RUNTIME_SOURCE_POLL_TIMEOUT_SECONDS,
        )
        if config.data_source != PHASE12_PHASE05_PUBLIC_SOURCE_NAME:
            raise RuntimeError("multi-strategy source identity is not the reviewed V10 successor")
        if config.data_hash != _sha256(source_bytes):
            raise RuntimeError("multi-strategy source bytes differ from PaperRunConfig")
        return config, source_bytes
    finally:
        foundation.source.close()


def _readiness_artifacts(
    config: PaperRunConfig,
    *,
    candidate_id: str = CANDIDATE_ID,
    release_code_manifest_bytes: bytes,
    runtime_environment_attestation_bytes: bytes,
) -> tuple[dict[str, bytes], EnvironmentReadinessManifest]:
    profile = profile_for(EnvironmentClass.PAPER)
    if profile.purpose is not AuthorizationPurpose.PAPER_RUNTIME:
        raise RuntimeError("compiled PAPER profile is not PAPER_RUNTIME")
    subject = ReadinessSubject(
        candidate_id=candidate_id,
        config_hash=config.config_hash,
        strategy_hash=config.strategy_hash,
        build_hash=config.engine_build_hash,
        source_identity=config.data_source,
        risk_limits_hash=canonical_sha256(config.risk.to_dict()),
    )
    evidence_files: dict[str, bytes] = {}
    bindings: dict[EvidenceCheck, ReadinessArtifactBinding] = {}
    for check in sorted(profile.required_checks, key=lambda item: item.value):
        relative_path = f"evidence/{check.value.casefold()}.json"
        payload = paper_evidence_payload(
            check,
            subject,
            risk_limits=(
                config.risk.to_dict()
                if check is EvidenceCheck.BOUNDED_POSITION_NOTIONAL
                else None
            ),
            release_code_manifest_bytes=(
                release_code_manifest_bytes
                if check is EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
                else None
            ),
            runtime_environment_attestation_bytes=(
                runtime_environment_attestation_bytes
                if check is EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
                else None
            ),
        )
        artifact_bytes = _canonical_bytes(payload)
        evidence_files[relative_path] = artifact_bytes
        bindings[check] = ReadinessArtifactBinding.from_bytes(
            relative_path,
            artifact_bytes,
        )
    manifest = EnvironmentReadinessManifest(
        schema_version=1,
        environment=EnvironmentClass.PAPER,
        purpose=profile.purpose,
        environment_identity="PAPER",
        execution_network=profile.execution_network,
        credential_scope=profile.credential_scope,
        order_capability=profile.order_capability,
        subject=subject,
        evidence=bindings,
    )
    return evidence_files, manifest


def _technical_deployment_gate_bytes(config: PaperRunConfig) -> bytes:
    return _canonical_bytes(
        {
            "artifact_schema_version": 1,
            "candidate_id": MULTISTRATEGY_CANDIDATE_ID,
            "economic_profitability_requirement": None,
            "environment": "PAPER",
            "future_smoke_requirements": {
                "bbo_freshness": {
                    "maximum_seconds": config.risk.stale_after_seconds,
                    "requirement": "ALL_REQUIRED_INSTRUMENTS_BELOW_STALE_THRESHOLD",
                },
                "coalescing": "EFFECTIVE_WITH_NONZERO_BURST_REPLACEMENTS_AND_BOUNDED_HIGH_WATER",
                "cpu_ram": {
                    "operator_limits_required_before_smoke": True,
                    "pass_condition": (
                        "OBSERVED_PEAK_RSS_AND_CPU_REMAIN_WITHIN_PREDECLARED_OPERATOR_LIMITS"
                    ),
                    "requirement": "MEASURE_AND_RECORD_PEAK_RSS_AND_CPU",
                },
                "reconnect_resync": "NO_REPEATED_PATHOLOGY_OR_UNEXPLAINED_GAP_LOOP",
                "source_queue": "NO_PERSISTENT_ACCUMULATION_AND_DRAINS_TO_ZERO",
                "sqlite_growth": "MEASURE_BYTES_AND_COMMITS_OVER_SMOKE_WINDOW",
                "unhedged_incidents": "NO_REPEATED_UNHEDGED_INCIDENT_OR_PROTECTIVE_LOOP",
            },
            "mode": "PAPER_ONLY",
            "orders_enabled": False,
            "status": "REQUIRES_FUTURE_LOCAL_OR_LINUX_SMOKE",
        }
    )


def _technical_evidence_bytes(
    config: PaperRunConfig,
    *,
    deployment_gate_bytes: bytes,
    manifest: EnvironmentReadinessManifest,
    profile_sha256: str,
    release_code_sha256: str,
    runtime_environment_sha256: str,
) -> bytes:
    strategies = [
        {
            **strategy.to_dict(),
            "strategy_config_hash": strategy.strategy_config_hash,
        }
        for strategy in config.strategy_configs
    ]
    return _canonical_bytes(
        {
            "artifact_schema_version": 1,
            "authorization": {
                "authorizes_real_money": False,
                "credential_scope": "NONE",
                "environment": "PAPER",
                "execution_network": "NONE",
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
            },
            "benchmark_reference": {
                "phase08_commits": 400,
                "phase08_frames_per_second": 34.83,
                "phase08_phase05_commits": 800,
                "phase08_phase05_frames_per_second": 14.96,
                "queue_bbo_admitted": 800,
                "queue_bbo_coalesced": 796,
                "queue_high_water": 4,
                "queue_pending_after_drain": 0,
                "status": "SYNTHETIC_TECHNICAL_ONLY",
            },
            "candidate_foundation_commit": "ca3c3bb3804b002aabd8132d002c55a4447fb582",
            "candidate_id": MULTISTRATEGY_CANDIDATE_ID,
            "deployment_gate": {
                "path": DEPLOYMENT_GATE_ARTIFACT,
                "sha256": _sha256(deployment_gate_bytes),
            },
            "economic_status": "TECHNICAL_ONLY_UNCALIBRATED",
            "identities": {
                "config_hash": config.config_hash,
                "engine_build_hash": config.engine_build_hash,
                "portfolio_id": config.portfolio_id,
                "readiness_manifest_sha256": manifest.manifest_sha256,
                "readiness_profile_sha256": profile_sha256,
                "release_code_sha256": release_code_sha256,
                "run_id": config.run_id,
                "runtime_environment_sha256": runtime_environment_sha256,
                "source_data_hash": config.data_hash,
                "source_identity": config.data_source,
            },
            "linux_bundle_pattern": (
                "authorization-<candidate-commit>-linux-cpython-<major.minor.micro>/"
            ),
            "reporting_contract": {
                "aggregate_views": ["account", "portfolio"],
                "strategy_views": [item.strategy_id for item in config.strategy_configs],
            },
            "source_contract": {
                "adapter_schema_version": 10,
                "feed_contract": (
                    "SOLE_COLLECTOR_NORMALIZED_BBO_CONNECTION_FUNDING_MARKET_CONTEXT_"
                    "BOUNDED_PENDING_BBO_LATEST_VALUE_V10"
                ),
            },
            "strategies": strategies,
        }
    )


def build_phase12_artifacts(
    *,
    repository_root: Path = REPOSITORY_ROOT,
    operator_runtime_environment_attestation_bytes: bytes | None = None,
    candidate_id: str = CANDIDATE_ID,
) -> dict[str, bytes]:
    fee_bytes = (repository_root / FEE_ARTIFACT_PATH).read_bytes()
    release_code_manifest_bytes = build_release_code_manifest_bytes(
        repository_root=repository_root,
        candidate_id=candidate_id,
    )
    runtime_environment_bytes = (
        paper_runtime_environment_attestation_bytes(
            repository_root,
            candidate_id=candidate_id,
        )
        if operator_runtime_environment_attestation_bytes is None
        else operator_runtime_environment_attestation_bytes
    )
    distribution_count, runtime_environment_sha256 = _runtime_environment_metadata(
        runtime_environment_bytes,
        candidate_id=candidate_id,
    )
    release_file_count, release_code_sha256 = _release_code_manifest_metadata(
        release_code_manifest_bytes
    )
    if candidate_id == MULTISTRATEGY_CANDIDATE_ID:
        config, source_bytes = build_multistrategy_paper_config(
            release_code_sha256=release_code_sha256,
            runtime_environment_sha256=runtime_environment_sha256,
        )
    elif candidate_id == CANDIDATE_ID:
        source_bytes = build_source_identity_artifact_bytes()
        config = build_paper_config(
            source_identity_bytes=source_bytes,
            fee_artifact_bytes=fee_bytes,
            release_code_sha256=release_code_sha256,
            runtime_environment_sha256=runtime_environment_sha256,
        )
    else:
        raise ValueError("unsupported Phase 12 Paper candidate")
    config_bytes = _canonical_bytes(config.to_dict())
    evidence_files, manifest = _readiness_artifacts(
        config,
        candidate_id=candidate_id,
        release_code_manifest_bytes=release_code_manifest_bytes,
        runtime_environment_attestation_bytes=runtime_environment_bytes,
    )
    manifest_bytes = manifest.canonical_json_bytes()
    profile = profile_for(EnvironmentClass.PAPER)
    extra_artifacts: dict[str, bytes] = {}
    if candidate_id == MULTISTRATEGY_CANDIDATE_ID:
        deployment_gate_bytes = _technical_deployment_gate_bytes(config)
        technical_evidence_bytes = _technical_evidence_bytes(
            config,
            deployment_gate_bytes=deployment_gate_bytes,
            manifest=manifest,
            profile_sha256=profile.profile_sha256,
            release_code_sha256=release_code_sha256,
            runtime_environment_sha256=runtime_environment_sha256,
        )
        extra_artifacts = {
            DEPLOYMENT_GATE_ARTIFACT: deployment_gate_bytes,
            TECHNICAL_EVIDENCE_ARTIFACT: technical_evidence_bytes,
        }
    index_bytes = _canonical_bytes(
        {
            "artifact_schema_version": 1,
            "artifacts": {
                "paper_config": {
                    "config_hash": config.config_hash,
                    "path": CONFIG_ARTIFACT,
                    "sha256": _sha256(config_bytes),
                },
                "readiness_manifest": {
                    "evidence_count": len(evidence_files),
                    "manifest_sha256": manifest.manifest_sha256,
                    "path": READINESS_MANIFEST_ARTIFACT,
                    "profile_sha256": profile.profile_sha256,
                    "sha256": _sha256(manifest_bytes),
                },
                "release_code_manifest": {
                    "canonicalization": _RELEASE_CODE_CANONICALIZATION,
                    "file_count": release_file_count,
                    "path": RELEASE_CODE_MANIFEST_ARTIFACT,
                    "release_code_sha256": release_code_sha256,
                    "sha256": _sha256(release_code_manifest_bytes),
                },
                "runtime_environment": {
                    "distribution_count": distribution_count,
                    "path": RUNTIME_ENVIRONMENT_ARTIFACT,
                    "runtime_environment_sha256": runtime_environment_sha256,
                    "sha256": _sha256(runtime_environment_bytes),
                },
                "source_identity": {
                    "path": SOURCE_IDENTITY_ARTIFACT,
                    "sha256": _sha256(source_bytes),
                    "source": config.data_source,
                },
                **(
                    {
                        "technical_deployment_gate": {
                            "path": DEPLOYMENT_GATE_ARTIFACT,
                            "sha256": _sha256(extra_artifacts[DEPLOYMENT_GATE_ARTIFACT]),
                        },
                        "technical_evidence": {
                            "path": TECHNICAL_EVIDENCE_ARTIFACT,
                            "sha256": _sha256(extra_artifacts[TECHNICAL_EVIDENCE_ARTIFACT]),
                        },
                    }
                    if extra_artifacts
                    else {}
                ),
            },
            "authorizes_real_money": False,
            "candidate_id": candidate_id,
            "environment": "PAPER",
            "purpose": "PAPER_RUNTIME",
            "run_id": config.run_id,
            "run_kind": "TECHNICAL",
            "status": "TECHNICAL_ONLY_UNCALIBRATED",
        }
    )
    return {
        ARTIFACT_INDEX: index_bytes,
        CONFIG_ARTIFACT: config_bytes,
        RELEASE_CODE_MANIFEST_ARTIFACT: release_code_manifest_bytes,
        RUNTIME_ENVIRONMENT_ARTIFACT: runtime_environment_bytes,
        SOURCE_IDENTITY_ARTIFACT: source_bytes,
        **extra_artifacts,
        **evidence_files,
        READINESS_MANIFEST_ARTIFACT: manifest_bytes,
    }


def build_phase12_multistrategy_artifacts(
    *,
    repository_root: Path = REPOSITORY_ROOT,
    operator_runtime_environment_attestation_bytes: bytes | None = None,
) -> dict[str, bytes]:
    return build_phase12_artifacts(
        repository_root=repository_root,
        operator_runtime_environment_attestation_bytes=(
            operator_runtime_environment_attestation_bytes
        ),
        candidate_id=MULTISTRATEGY_CANDIDATE_ID,
    )


def _verify_written_readiness(output_root: Path) -> None:
    manifest = EnvironmentReadinessManifest.from_json_bytes(
        (output_root / READINESS_MANIFEST_ARTIFACT).read_bytes(),
        require_canonical=True,
    )
    decision = verify_environment_readiness(manifest, evidence_root=output_root)
    if decision.blockers:
        details = "; ".join(
            f"{blocker.code}@{blocker.location}" for blocker in decision.blockers
        )
        raise ArtifactDriftError(f"PAPER/PAPER_RUNTIME readiness is blocked: {details}")



def _artifact_file_paths(output_root: Path) -> frozenset[str]:
    if not output_root.exists():
        return frozenset()
    if not output_root.is_dir():
        raise ArtifactDriftError(
            f"artifact output root is not a directory: {output_root}"
        )
    return frozenset(
        path.relative_to(output_root).as_posix()
        for path in output_root.rglob("*")
        if path.is_file() or path.is_symlink()
    )


def _unexpected_artifact_paths(
    output_root: Path, expected: Mapping[str, bytes]
) -> tuple[str, ...]:
    return tuple(sorted(_artifact_file_paths(output_root) - set(expected)))


def check_artifacts(
    output_root: Path,
    *,
    repository_root: Path = REPOSITORY_ROOT,
    candidate_id: str = CANDIDATE_ID,
) -> dict[str, bytes]:
    expected = build_phase12_artifacts(
        repository_root=repository_root,
        candidate_id=candidate_id,
    )
    drift: list[str] = []
    for relative_path, payload in sorted(expected.items()):
        target = output_root / relative_path
        if not target.is_file():
            drift.append(f"missing:{relative_path}")
        elif target.read_bytes() != payload:
            drift.append(f"changed:{relative_path}")
    drift.extend(
        f"unexpected:{path}" for path in _unexpected_artifact_paths(output_root, expected)
    )
    if drift:
        raise ArtifactDriftError("deterministic artifact drift: " + ", ".join(drift))
    _verify_written_readiness(output_root)
    return expected


def write_artifacts(
    output_root: Path,
    *,
    repository_root: Path = REPOSITORY_ROOT,
    candidate_id: str = CANDIDATE_ID,
) -> dict[str, bytes]:
    artifacts = build_phase12_artifacts(
        repository_root=repository_root,
        candidate_id=candidate_id,
    )
    unexpected = _unexpected_artifact_paths(output_root, artifacts)
    if unexpected:
        raise ArtifactDriftError(
            "unexpected generated artifact files: " + ", ".join(unexpected)
        )
    for relative_path, payload in sorted(artifacts.items()):
        target = output_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
    _verify_written_readiness(output_root)
    return artifacts


def check_multistrategy_artifacts(
    output_root: Path,
    *,
    repository_root: Path = REPOSITORY_ROOT,
) -> dict[str, bytes]:
    return check_artifacts(
        output_root,
        repository_root=repository_root,
        candidate_id=MULTISTRATEGY_CANDIDATE_ID,
    )


def write_multistrategy_artifacts(
    output_root: Path,
    *,
    repository_root: Path = REPOSITORY_ROOT,
) -> dict[str, bytes]:
    return write_artifacts(
        output_root,
        repository_root=repository_root,
        candidate_id=MULTISTRATEGY_CANDIDATE_ID,
    )


def _summary(
    artifacts: Mapping[str, bytes],
    *,
    action: str,
    candidate_id: str = MULTISTRATEGY_CANDIDATE_ID,
) -> dict[str, object]:
    config = PaperRunConfig.from_dict(
        json.loads(artifacts[CONFIG_ARTIFACT].decode("utf-8"))
    )
    manifest = EnvironmentReadinessManifest.from_json_bytes(
        artifacts[READINESS_MANIFEST_ARTIFACT],
        require_canonical=True,
    )
    _, release_code_sha256 = _release_code_manifest_metadata(
        artifacts[RELEASE_CODE_MANIFEST_ARTIFACT]
    )
    return {
        "action": action,
        "authorizes_real_money": False,
        "candidate_id": candidate_id,
        "config_hash": config.config_hash,
        "file_count": len(artifacts),
        "manifest_sha256": manifest.manifest_sha256,
        "readiness": "READY",
        "release_code_sha256": release_code_sha256,
        "runtime_environment_sha256": config.runtime_environment_sha256,
        "run_id": config.run_id,
        "run_kind": config.run_kind,
        "source_data_hash": config.data_hash,
        "status": "TECHNICAL_ONLY_UNCALIBRATED",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Regenerate the offline deterministic Phase 08 + Phase 05 TECHNICAL Paper bundle."
        )
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help=f"Candidate artifact directory (default: {DEFAULT_OUTPUT_ROOT})",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify exact regeneration without writing any artifact.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.check:
            artifacts = check_multistrategy_artifacts(args.output_root)
            action = "CHECKED_NO_WRITE"
        else:
            artifacts = write_multistrategy_artifacts(args.output_root)
            action = "REGENERATED"
    except (ArtifactDriftError, OSError, TypeError, ValueError) as error:
        print(json.dumps({"error": str(error), "status": "BLOCKED"}, sort_keys=True))
        return 1
    print(json.dumps(_summary(artifacts, action=action), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
