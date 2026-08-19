from __future__ import annotations

import hashlib
import importlib.metadata as importlib_metadata
import json
import os
import platform
import re
import struct
import sys
import sysconfig
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import TypeVar, cast

AUTHORIZATION_MANIFEST_SCHEMA_VERSION = 1
AUTHORIZATION_RECEIPT_SCHEMA_VERSION = 1
REAL_MONEY_EXECUTION_ENABLED_IN_BUILD = False
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_IDENTITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@+\-]{0,255}\Z")
_READ_CHUNK_BYTES = 1024 * 1024
_MAX_SEMANTIC_ARTIFACT_BYTES = 1024 * 1024


class EnvironmentClass(StrEnum):
    RESEARCH_REPLAY = "RESEARCH_REPLAY"
    PAPER = "PAPER"
    TESTNET = "TESTNET"
    MICRO_MAINNET = "MICRO_MAINNET"
    MAINNET = "MAINNET"


class AuthorizationPurpose(StrEnum):
    RESEARCH_REPLAY = "RESEARCH_REPLAY"
    PAPER_RUNTIME = "PAPER_RUNTIME"
    TESTNET_EXECUTION = "TESTNET_EXECUTION"
    MICRO_MAINNET_EXECUTION = "MICRO_MAINNET_EXECUTION"
    MAINNET_EXECUTION = "MAINNET_EXECUTION"


class ExecutionNetwork(StrEnum):
    NONE = "NONE"
    TESTNET = "TESTNET"
    MAINNET = "MAINNET"


class CredentialScope(StrEnum):
    NONE = "NONE"
    TESTNET = "TESTNET"
    MICRO_MAINNET = "MICRO_MAINNET"
    MAINNET = "MAINNET"


class OrderCapability(StrEnum):
    NONE = "NONE"
    SIMULATED_ONLY = "SIMULATED_ONLY"
    TESTNET_ONLY = "TESTNET_ONLY"
    REAL_MONEY = "REAL_MONEY"


class EvidenceCheck(StrEnum):
    FROZEN_INPUTS = "FROZEN_INPUTS"
    FROZEN_STRATEGY_CONFIG = "FROZEN_STRATEGY_CONFIG"
    DETERMINISTIC_REPLAY = "DETERMINISTIC_REPLAY"
    NON_AUTHORIZING_RUNTIME = "NON_AUTHORIZING_RUNTIME"
    PUBLIC_MARKET_SOURCE = "PUBLIC_MARKET_SOURCE"
    NORMALIZED_MARKET_EVENT_SCHEMA = "NORMALIZED_MARKET_EVENT_SCHEMA"
    DETERMINISTIC_ACCOUNTING = "DETERMINISTIC_ACCOUNTING"
    CONSERVATIVE_COST_MODEL = "CONSERVATIVE_COST_MODEL"
    RUNTIME_SOURCE_ATTESTATION = "RUNTIME_SOURCE_ATTESTATION"
    CRASH_RECOVERY = "CRASH_RECOVERY"
    RESTART_RECOVERY = "RESTART_RECOVERY"
    RECONCILIATION = "RECONCILIATION"
    NO_PRIVATE_EXECUTION_PATH = "NO_PRIVATE_EXECUTION_PATH"
    NO_WALLET_OR_SIGNER = "NO_WALLET_OR_SIGNER"
    EXPLICIT_TESTNET_ENDPOINT = "EXPLICIT_TESTNET_ENDPOINT"
    TESTNET_CONFIG_NAMESPACE = "TESTNET_CONFIG_NAMESPACE"
    NO_MAINNET_FALLBACK = "NO_MAINNET_FALLBACK"
    ISOLATED_TESTNET_CREDENTIALS = "ISOLATED_TESTNET_CREDENTIALS"
    CREDENTIAL_SCOPE_VALIDATION = "CREDENTIAL_SCOPE_VALIDATION"
    DETERMINISTIC_CLIENT_ORDER_IDS = "DETERMINISTIC_CLIENT_ORDER_IDS"
    ORDER_LIFECYCLE_STATE_MACHINE = "ORDER_LIFECYCLE_STATE_MACHINE"
    CANCEL_REPLACE_SEMANTICS = "CANCEL_REPLACE_SEMANTICS"
    BOUNDED_POSITION_NOTIONAL = "BOUNDED_POSITION_NOTIONAL"
    KILL_SWITCH = "KILL_SWITCH"
    FULL_AUDIT_LOG = "FULL_AUDIT_LOG"
    FAIL_CLOSED_ENVIRONMENT_IDENTITY = "FAIL_CLOSED_ENVIRONMENT_IDENTITY"
    GATE_B_EVIDENCE = "GATE_B_EVIDENCE"
    GATE_C_EVIDENCE = "GATE_C_EVIDENCE"
    GATE_D_FORWARD_PAPER_EVIDENCE = "GATE_D_FORWARD_PAPER_EVIDENCE"
    GATE_E_TESTNET_EVIDENCE = "GATE_E_TESTNET_EVIDENCE"
    GATE_F_MICRO_MAINNET_EVIDENCE = "GATE_F_MICRO_MAINNET_EVIDENCE"
    HUMAN_APPROVAL = "HUMAN_APPROVAL"
    DUAL_HUMAN_APPROVAL = "DUAL_HUMAN_APPROVAL"
    SIGNER_ISOLATION = "SIGNER_ISOLATION"
    SECRET_HANDLING = "SECRET_HANDLING"
    SIGNED_CONFIGURATION = "SIGNED_CONFIGURATION"
    LOSS_DRAWDOWN_LIMITS = "LOSS_DRAWDOWN_LIMITS"
    MICRO_MAINNET_CAPS = "MICRO_MAINNET_CAPS"


class AuthorizationManifestError(ValueError):
    def __init__(self, code: str, location: str, message: str) -> None:
        super().__init__(f"{location}: {message}")
        self.code = code
        self.location = location
        self.message = message


@dataclass(frozen=True, slots=True)
class AuthorizationBlocker:
    code: str
    location: str
    message: str


def _error(code: str, location: str, message: str) -> AuthorizationManifestError:
    return AuthorizationManifestError(code, location, message)


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _error("DUPLICATE_JSON_KEY", "$", f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    raise _error("NON_FINITE_JSON_NUMBER", "$", f"non-finite JSON number {value!r}")


def _require_mapping(value: object, *, location: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _error("INVALID_MANIFEST_SHAPE", location, "must be a JSON object")
    if any(not isinstance(key, str) for key in value):
        raise _error("INVALID_MANIFEST_SHAPE", location, "object keys must be strings")
    return cast(Mapping[str, object], value)


def _require_exact_keys(
    value: Mapping[str, object],
    expected: frozenset[str],
    *,
    location: str,
) -> None:
    actual = frozenset(value)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        details: list[str] = []
        if missing:
            details.append(f"missing keys {missing}")
        if extra:
            details.append(f"unexpected keys {extra}")
        raise _error("INVALID_MANIFEST_SHAPE", location, "; ".join(details))


def _require_string(value: object, *, location: str) -> str:
    if not isinstance(value, str):
        raise _error("INVALID_MANIFEST_SHAPE", location, "must be a string")
    return value


_EnumT = TypeVar("_EnumT", bound=StrEnum)


def _parse_enum(enum_type: type[_EnumT], value: object, *, location: str) -> _EnumT:
    raw = _require_string(value, location=location)
    try:
        return enum_type(raw)
    except ValueError as error:
        raise _error(
            "INVALID_ENVIRONMENT_IDENTITY",
            location,
            f"unknown or ambiguous {location.rsplit('.', 1)[-1]} {raw!r}",
        ) from error


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _require_identity(value: object, *, location: str) -> str:
    raw = _require_string(value, location=location)
    if _IDENTITY_RE.fullmatch(raw) is None:
        raise _error(
            "INVALID_IDENTITY",
            location,
            "must be an exact stable identifier without whitespace",
        )
    return raw


def _require_sha256(value: object, *, location: str) -> str:
    raw = _require_string(value, location=location)
    if _SHA256_RE.fullmatch(raw) is None:
        raise _error("INVALID_SHA256", location, "must be a lowercase SHA-256")
    return raw


@dataclass(frozen=True, slots=True)
class ReadinessArtifactBinding:
    relative_path: str
    sha256: str

    @classmethod
    def from_bytes(cls, relative_path: str, payload: bytes) -> ReadinessArtifactBinding:
        if not isinstance(payload, bytes):
            raise TypeError("readiness artifact payload must be bytes")
        return cls(relative_path=relative_path, sha256=hashlib.sha256(payload).hexdigest())

    @classmethod
    def from_object(
        cls,
        value: object,
        *,
        location: str,
    ) -> ReadinessArtifactBinding:
        raw = _require_mapping(value, location=location)
        _require_exact_keys(raw, frozenset({"path", "sha256"}), location=location)
        return cls(
            relative_path=_require_string(raw["path"], location=f"{location}.path"),
            sha256=_require_string(raw["sha256"], location=f"{location}.sha256"),
        )

    def to_dict(self) -> dict[str, str]:
        return {"path": self.relative_path, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class ReadinessSubject:
    candidate_id: str
    config_hash: str
    strategy_hash: str
    build_hash: str
    source_identity: str
    risk_limits_hash: str

    def __post_init__(self) -> None:
        for name in ("candidate_id", "source_identity"):
            value = getattr(self, name)
            if not isinstance(value, str) or _IDENTITY_RE.fullmatch(value) is None:
                raise ValueError(f"readiness subject {name} must be a stable identifier")
        for name in ("config_hash", "strategy_hash", "build_hash", "risk_limits_hash"):
            value = getattr(self, name)
            if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                raise ValueError(f"readiness subject {name} must be a lowercase SHA-256")

    @classmethod
    def from_object(cls, value: object, *, location: str = "$.subject") -> ReadinessSubject:
        raw = _require_mapping(value, location=location)
        expected = frozenset(
            {
                "candidate_id",
                "config_hash",
                "strategy_hash",
                "build_hash",
                "source_identity",
                "risk_limits_hash",
            }
        )
        _require_exact_keys(raw, expected, location=location)
        try:
            return cls(
                candidate_id=_require_identity(
                    raw["candidate_id"], location=f"{location}.candidate_id"
                ),
                config_hash=_require_sha256(
                    raw["config_hash"], location=f"{location}.config_hash"
                ),
                strategy_hash=_require_sha256(
                    raw["strategy_hash"], location=f"{location}.strategy_hash"
                ),
                build_hash=_require_sha256(
                    raw["build_hash"], location=f"{location}.build_hash"
                ),
                source_identity=_require_identity(
                    raw["source_identity"], location=f"{location}.source_identity"
                ),
                risk_limits_hash=_require_sha256(
                    raw["risk_limits_hash"], location=f"{location}.risk_limits_hash"
                ),
            )
        except ValueError as error:
            raise _error("INVALID_SUBJECT", location, str(error)) from error

    def to_dict(self) -> dict[str, str]:
        return {
            "build_hash": self.build_hash,
            "candidate_id": self.candidate_id,
            "config_hash": self.config_hash,
            "risk_limits_hash": self.risk_limits_hash,
            "source_identity": self.source_identity,
            "strategy_hash": self.strategy_hash,
        }


@dataclass(frozen=True, slots=True)
class EvidenceVerifierIdentity:
    environment: EnvironmentClass
    purpose: AuthorizationPurpose
    check: EvidenceCheck
    verifier_id: str
    version: int

    def __post_init__(self) -> None:
        if not isinstance(self.environment, EnvironmentClass):
            raise TypeError("evidence verifier environment must be an EnvironmentClass")
        if not isinstance(self.purpose, AuthorizationPurpose):
            raise TypeError("evidence verifier purpose must be an AuthorizationPurpose")
        if not isinstance(self.check, EvidenceCheck):
            raise TypeError("evidence verifier check must be an EvidenceCheck")
        if not isinstance(self.verifier_id, str) or _IDENTITY_RE.fullmatch(self.verifier_id) is None:
            raise ValueError("evidence verifier_id must be a stable identifier")
        if isinstance(self.version, bool) or not isinstance(self.version, int) or self.version <= 0:
            raise ValueError("evidence verifier version must be a positive integer")

    @classmethod
    def from_object(
        cls,
        value: object,
        *,
        location: str,
    ) -> EvidenceVerifierIdentity:
        raw = _require_mapping(value, location=location)
        _require_exact_keys(
            raw,
            frozenset({'check', 'environment', 'purpose', 'verifier_id', 'version'}),
            location=location,
        )
        version = raw['version']
        if isinstance(version, bool) or not isinstance(version, int):
            raise _error(
                'INVALID_RECEIPT',
                f'{location}.version',
                'must be an integer',
            )
        try:
            return cls(
                environment=_parse_enum(
                    EnvironmentClass,
                    raw['environment'],
                    location=f'{location}.environment',
                ),
                purpose=_parse_enum(
                    AuthorizationPurpose,
                    raw['purpose'],
                    location=f'{location}.purpose',
                ),
                check=_parse_enum(
                    EvidenceCheck,
                    raw['check'],
                    location=f'{location}.check',
                ),
                verifier_id=_require_identity(
                    raw['verifier_id'],
                    location=f'{location}.verifier_id',
                ),
                version=version,
            )
        except AuthorizationManifestError:
            raise
        except (TypeError, ValueError) as error:
            raise _error('INVALID_RECEIPT', location, str(error)) from error

    def to_dict(self) -> dict[str, object]:
        return {
            "check": self.check.value,
            "environment": self.environment.value,
            "purpose": self.purpose.value,
            "verifier_id": self.verifier_id,
            "version": self.version,
        }


@dataclass(frozen=True, slots=True)
class EvidenceVerificationContext:
    check: EvidenceCheck
    environment: EnvironmentClass
    purpose: AuthorizationPurpose
    environment_identity: str
    execution_network: ExecutionNetwork
    credential_scope: CredentialScope
    order_capability: OrderCapability
    subject: ReadinessSubject
    profile_sha256: str
    artifact_bytes: bytes
    evidence_root: Path | None = None


EvidenceVerifier = Callable[[EvidenceVerificationContext], bool]
_VerifierScope = tuple[EnvironmentClass, AuthorizationPurpose, EvidenceCheck]

_TESTNET_HTTP_ENDPOINT = 'https://api.hyperliquid-testnet.xyz'
_TESTNET_WEBSOCKET_ENDPOINT = 'wss://api.hyperliquid-testnet.xyz/ws'
_TESTNET_IDENTITY = 'TESTNET'
_TESTNET_CREDENTIAL_NAMESPACE = 'HYPERLAB_TESTNET'
_TESTNET_EVIDENCE_KEYS = frozenset(
    {
        'check',
        'environment',
        'facts',
        'purpose',
        'schema_version',
        'subject',
        'validation',
    }
)
_TESTNET_VALIDATION_KEYS = frozenset({'report_sha256', 'validation_id'})
_TESTNET_RISK_FACT_KEYS = frozenset(
    {'enforcement', 'limits', 'limits_sha256', 'namespace'}
)
_TESTNET_RISK_LIMIT_KEYS = frozenset(
    {
        'cancel_requests_per_minute',
        'deadman_interval_seconds',
        'market_stale_after_seconds',
        'max_concurrent_orders',
        'max_gross_notional',
        'max_order_notional',
        'max_order_quantity',
        'max_position_notional',
        'max_position_quantity',
        'reconciliation_stale_after_seconds',
        'replace_requests_per_minute',
        'submit_requests_per_minute',
    }
)
_CANONICAL_POSITIVE_DECIMAL_RE = re.compile(r'(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z')
_TESTNET_RISK_DECIMAL_CEILINGS = {
    'max_gross_notional': Decimal('1000'),
    'max_order_notional': Decimal('100'),
    'max_order_quantity': Decimal('1'),
    'max_position_notional': Decimal('500'),
    'max_position_quantity': Decimal('5'),
}
_TESTNET_RISK_INTEGER_CEILINGS = {
    'cancel_requests_per_minute': 24,
    'deadman_interval_seconds': 30,
    'market_stale_after_seconds': 5,
    'max_concurrent_orders': 4,
    'reconciliation_stale_after_seconds': 10,
    'replace_requests_per_minute': 6,
    'submit_requests_per_minute': 12,
}
_PAPER_CANDIDATE_ID = 'phase08-robust-pairs-btc-eth-paper-v1'
_PAPER_MULTISTRATEGY_CANDIDATE_ID = 'phase08-phase05-multistrategy-paper-v1'
_PAPER_STRATEGY_NAME = 'pairs_mean_reversion_phase08'
_PAPER_MULTISTRATEGY_PRIMARY_STRATEGY_NAME = 'cash_and_carry'
_PAPER_SOURCE_IDENTITY = 'hyperliquid-mainnet-public-bbo-funding-v1'
_PAPER_MULTISTRATEGY_SOURCE_IDENTITY = (
    'hyperliquid-mainnet-public-bbo-funding-context-phase05-v1'
)
_PAPER_SUPPORTED_CANDIDATE_SOURCES: Mapping[str, str] = MappingProxyType(
    {
        _PAPER_CANDIDATE_ID: _PAPER_SOURCE_IDENTITY,
        _PAPER_MULTISTRATEGY_CANDIDATE_ID: _PAPER_MULTISTRATEGY_SOURCE_IDENTITY,
    }
)


def paper_release_identity_candidate(
    candidate_id: str | None = None,
    *,
    config_schema_version: int | None = None,
) -> str:
    """Resolve one exact compiled Paper candidate for release identity checks."""

    if (candidate_id is None) == (config_schema_version is None):
        raise ValueError(
            "PAPER release identity requires exactly one candidate or config schema"
        )
    if candidate_id is not None:
        if candidate_id not in _PAPER_SUPPORTED_CANDIDATE_SOURCES:
            raise ValueError("unsupported PAPER candidate identity")
        return candidate_id
    if (
        isinstance(config_schema_version, bool)
        or not isinstance(config_schema_version, int)
        or config_schema_version not in {1, 2, 3}
    ):
        raise ValueError("unsupported PAPER config schema identity")
    resolved = (
        _PAPER_MULTISTRATEGY_CANDIDATE_ID
        if config_schema_version == 3
        else _PAPER_CANDIDATE_ID
    )
    if resolved not in _PAPER_SUPPORTED_CANDIDATE_SOURCES:
        raise ValueError("unsupported PAPER candidate identity")
    return resolved


_PAPER_HTTP_ENDPOINT = 'https://api.hyperliquid.xyz'
_PAPER_WEBSOCKET_ENDPOINT = 'wss://api.hyperliquid.xyz/ws'
_PAPER_RELEASE_CODE_MANIFEST_ARTIFACT = 'release-code-manifest.json'
_PAPER_RELEASE_CODE_MANIFEST_SCHEMA_VERSION = 1
_PAPER_RELEASE_CODE_CANONICALIZATION = (
    'UTF8_LF_CLI_DERIVED_BINDINGS_REDACTED_OR_BINARY_V1'
)
_PAPER_RELEASE_CODE_FIXED_PATHS = (
    'pyproject.toml',
    'requirements-runtime.lock',
    'scripts/generate_phase12_live_paper_artifacts.py',
)
_PAPER_RELEASE_CODE_MANIFEST_KEYS = frozenset(
    {
        'artifact_schema_version',
        'candidate_id',
        'canonicalization',
        'files',
        'release_code_sha256',
    }
)
_PAPER_RELEASE_CODE_ATTESTATION_KEYS = frozenset(
    {'canonicalization', 'file_count', 'path', 'release_code_sha256', 'sha256'}
)
_PAPER_RUNTIME_ENVIRONMENT_ARTIFACT = 'runtime-environment-attestation.json'
_PAPER_RUNTIME_ENVIRONMENT_SCHEMA_VERSION = 1
_PAPER_RUNTIME_ENVIRONMENT_CANONICALIZATION = (
    'CANONICAL_JSON_HASHED_LOCK_PINS_EXACT_DISTRIBUTIONS_CPYTHON_PLATFORM_V1'
)
_PAPER_RUNTIME_ENVIRONMENT_KEYS = frozenset(
    {
        'artifact_schema_version',
        'candidate_id',
        'canonicalization',
        'distribution_count',
        'extras_allowed',
        'installed_distributions',
        'interpreter',
        'lock',
        'runtime_environment_sha256',
    }
)
_PAPER_RUNTIME_ENVIRONMENT_LOCK_KEYS = frozenset(
    {'path', 'pins', 'sha256'}
)
_PAPER_RUNTIME_ENVIRONMENT_ATTESTATION_KEYS = frozenset(
    {
        'canonicalization',
        'distribution_count',
        'path',
        'runtime_environment_sha256',
        'sha256',
    }
)
_PAPER_RUNTIME_INTERPRETER_KEYS = frozenset(
    {
        'abi_flags',
        'byteorder',
        'cache_tag',
        'hexversion',
        'implementation',
        'implementation_version',
        'platform_machine',
        'platform_system',
        'platform_tag',
        'pointer_bits',
        'python_compiler',
        'python_version',
        'version_info',
    }
)
_PAPER_RUNTIME_VERSION_KEYS = frozenset(
    {'major', 'micro', 'minor', 'releaselevel', 'serial'}
)
_PAPER_RUNTIME_LOCK_PIN_RE = re.compile(
    r'(?P<name>[A-Za-z0-9][A-Za-z0-9._-]{0,127})=='
    r'(?P<version>[A-Za-z0-9][A-Za-z0-9.!+_-]{0,127})'
    r'(?P<continuation>\s+\\)?\Z'
)
_PAPER_RUNTIME_LOCK_HASH_RE = re.compile(
    r'--hash=sha256:(?P<digest>[0-9a-f]{64})'
    r'(?P<continuation>\s+\\)?\Z'
)
_PAPER_DISTRIBUTION_NORMALIZE_RE = re.compile(r'[-_.]+')
_PAPER_CLI_DERIVED_BINDING_NAMES = (
    '_PHASE12_PAPER_CONFIG_HASH',
    '_PHASE12_PAPER_READINESS_MANIFEST_SHA256',
    '_PHASE12_PAPER_READINESS_PROFILE_SHA256',
    '_PHASE12_MULTISTRATEGY_CONFIG_HASH',
    '_PHASE12_MULTISTRATEGY_READINESS_MANIFEST_SHA256',
    '_PHASE12_MULTISTRATEGY_READINESS_PROFILE_SHA256',
)
_PAPER_EVIDENCE_KEYS = frozenset(
    {
        'check',
        'environment',
        'facts',
        'purpose',
        'runtime_scope',
        'schema_version',
        'subject',
    }
)
_PAPER_RUNTIME_SCOPE_FACTS: Mapping[str, object] = MappingProxyType(
    {
        'authorizes_real_money': False,
        'credential_scope': CredentialScope.NONE.value,
        'execution_network': ExecutionNetwork.NONE.value,
        'order_capability': OrderCapability.SIMULATED_ONLY.value,
        'orders_enabled': False,
        'real_money_execution_enabled_in_build': False,
    }
)
_PAPER_RUNTIME_LEASE_FACTS: Mapping[str, object] = MappingProxyType(
    {
        'canonical_database_path': 'RESOLVE_STRICT_THEN_OS_PATH_NORMCASE',
        'contention_action': 'BLOCK_SECOND_RUNTIME_OR_STANDALONE_REPLAY_OR_RECONCILE',
        'identity_fields': [
            'CANONICAL_DATABASE_PATH',
            'RUN_ID',
            'LEASE_SCHEMA',
        ],
        'identity_hash': 'CANONICAL_JSON_SHA256',
        'lock_file_pattern': '.{DATABASE_NAME}.paper-runtime-{IDENTITY_SHA256}.lock',
        'lock_file_deletion_required_on_close': False,
        'lock_file_directory': 'DATABASE_PARENT',
        'lock_mode': 'NONBLOCKING_EXCLUSIVE_OS_LOCK',
        'lock_payload': 'ONE_NUL_BYTE_FSYNCED_WHEN_EMPTY',
        'platform_backends': [
            'WINDOWS_MSVCRT_LK_NBLCK_FIRST_BYTE',
            'POSIX_FCNTL_FLOCK_EX_NB',
        ],
        'schema': 'paper-runtime-exclusive-os-lock-v1',
        'os_crash_releases_lock': True,
        'read_only_status_report_require_lease': False,
        'operator_pause_resume_kill_require_lease': False,
        'runtime_acquired_after_release_and_frozen_binding_checks': True,
        'runtime_acquired_before_engine_start_startup_reconciliation_and_public_source_start': True,
        'runtime_admission_failure_releases_lock': True,
        'runtime_held_until_close': True,
        'scope': 'EXACT_CANONICAL_DATABASE_AND_RUN',
        'second_runtime_for_exact_database_and_run_rejected': True,
        'standalone_reconcile_acquired_after_release_check': True,
        'standalone_reconcile_failure_releases_lock': True,
        'standalone_reconcile_held_until_completion': True,
        'standalone_reconcile_requires_stopped_runtime': True,
        'standalone_replay_acquired_after_release_check': True,
        'standalone_replay_failure_releases_lock': True,
        'standalone_replay_held_until_completion': True,
        'standalone_replay_requires_stopped_runtime': True,
    }
)


def _paper_runtime_lease_facts() -> dict[str, object]:
    return {
        **_PAPER_RUNTIME_LEASE_FACTS,
        'identity_fields': [
            'CANONICAL_DATABASE_PATH',
            'RUN_ID',
            'LEASE_SCHEMA',
        ],
        'platform_backends': [
            'WINDOWS_MSVCRT_LK_NBLCK_FIRST_BYTE',
            'POSIX_FCNTL_FLOCK_EX_NB',
        ],
    }


_PAPER_RISK_FACT_KEYS = frozenset(
    {'enforcement', 'limits', 'limits_sha256', 'order_capability', 'protective_risk'}
)


def _paper_protective_risk_facts() -> dict[str, object]:
    return {
        'breach_transition': 'REDUCE_ONLY_BEFORE_ENTRY_ECONOMICS',
        'entry_orders_protected': True,
        'flatten_execution': (
            'STRATEGY_INDEPENDENT_IOC_REDUCE_ONLY_WITH_FRESH_BILATERAL_DURABLE_MARKETS'
        ),
        'manual_review_policy': 'NO_APPEND',
        'marked_limits': [
            'GROSS_NOTIONAL',
            'NET_NOTIONAL',
            'INSTRUMENT_NOTIONAL',
            'DAILY_LOSS',
            'DRAWDOWN',
        ],
        'missing_mark_action': 'CRITICAL_DEDUPED_FAIL_CLOSED',
        'partial_or_no_fill_action': 'RETRY_WHILE_BREACH_PERSISTS',
        'paused_policy': 'MONITOR_ALERT_PROTECT_NO_EXECUTION',
        'valuation_source': 'DURABLE_PUBLIC_BBO',
    }
_PAPER_RISK_LIMIT_KEYS = frozenset(
    {
        'max_concurrent_orders',
        'max_daily_loss',
        'max_drawdown',
        'max_gross_notional',
        'max_instrument_notional',
        'max_net_notional',
        'max_order_notional',
        'max_order_quantity',
        'max_position_quantity',
        'stale_after_seconds',
        'unhedged_timeout_seconds',
    }
)
_PAPER_RISK_DECIMAL_CEILINGS: Mapping[str, Decimal] = MappingProxyType(
    {
        'max_daily_loss': Decimal('500'),
        'max_drawdown': Decimal('1000'),
        'max_gross_notional': Decimal('10000'),
        'max_instrument_notional': Decimal('5000'),
        'max_net_notional': Decimal('2500'),
        'max_order_notional': Decimal('1000'),
        'max_order_quantity': Decimal('2'),
        'max_position_quantity': Decimal('10'),
    }
)
_PAPER_RISK_INTEGER_CEILINGS: Mapping[str, int] = MappingProxyType(
    {
        'max_concurrent_orders': 4,
        'stale_after_seconds': 10,
        'unhedged_timeout_seconds': 60,
    }
)
_PAPER_MULTISTRATEGY_RISK_DECIMAL_CEILINGS: Mapping[str, Decimal] = MappingProxyType(
    {
        **_PAPER_RISK_DECIMAL_CEILINGS,
        'max_order_quantity': Decimal('10'),
        'max_position_quantity': Decimal('11'),
    }
)
_PAPER_MULTISTRATEGY_RISK_INTEGER_CEILINGS: Mapping[str, int] = MappingProxyType(
    {
        **_PAPER_RISK_INTEGER_CEILINGS,
        'stale_after_seconds': 15,
    }
)


def _fixed_testnet_facts(check: EvidenceCheck) -> dict[str, object] | None:
    if check is EvidenceCheck.EXPLICIT_TESTNET_ENDPOINT:
        return {
            'chain_identity': _TESTNET_IDENTITY,
            'http_endpoint': _TESTNET_HTTP_ENDPOINT,
            'redirects_allowed': False,
            'websocket_endpoint': _TESTNET_WEBSOCKET_ENDPOINT,
        }
    if check is EvidenceCheck.TESTNET_CONFIG_NAMESPACE:
        return {
            'chain_identity': _TESTNET_IDENTITY,
            'configuration_namespace': _TESTNET_CREDENTIAL_NAMESPACE,
            'environment_identity': _TESTNET_IDENTITY,
        }
    if check is EvidenceCheck.NO_MAINNET_FALLBACK:
        return {
            'default_endpoint_present': False,
            'fallback_present': False,
            'mainnet_route_present': False,
        }
    if check is EvidenceCheck.ISOLATED_TESTNET_CREDENTIALS:
        return {
            'credential_material_present': False,
            'credential_namespace': _TESTNET_CREDENTIAL_NAMESPACE,
            'mainnet_namespace_reused': False,
            'paper_identity_reused': False,
        }
    if check is EvidenceCheck.CREDENTIAL_SCOPE_VALIDATION:
        return {
            'credential_namespace': _TESTNET_CREDENTIAL_NAMESPACE,
            'expected_scope': _TESTNET_IDENTITY,
            'mainnet_scope_accepted': False,
            'scope_validation_required': True,
        }
    if check is EvidenceCheck.DETERMINISTIC_CLIENT_ORDER_IDS:
        return {
            'domain': 'hyperliquid_testnet_cloid_v1',
            'format': '0x+32_lowercase_hex',
            'pattern': '^0x[0-9a-f]{32}$',
            'retry_reuses_identifier': True,
        }
    if check is EvidenceCheck.ORDER_LIFECYCLE_STATE_MACHINE:
        return {
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
    if check is EvidenceCheck.CANCEL_REPLACE_SEMANTICS:
        return {
            'ambiguous_cancel_action': 'RECONCILE',
            'ambiguous_modify_action': 'RECONCILE',
            'blind_resubmit_allowed': False,
            'cancel_ack_required': True,
            'replace_semantics': 'NATIVE_MODIFY',
        }
    if check is EvidenceCheck.RECONCILIATION:
        return {
            'authority': 'EXCHANGE_FIRST',
            'covers': [
                'OPEN_ORDERS',
                'POSITIONS',
                'FILLS',
                'BALANCES_EQUITY',
                'UNKNOWN_CLOIDS',
                'MISSING_ACKNOWLEDGEMENTS',
            ],
            'idempotent': True,
            'unresolved_action': 'PAUSED_MANUAL_REVIEW',
        }
    if check is EvidenceCheck.RESTART_RECOVERY:
        return {
            'before_new_orders': 'RECONCILE',
            'cancellation_recovery': True,
            'duplicate_submission_allowed': False,
            'fill_deduplication': True,
            'open_order_recovery': True,
            'position_invention_allowed': False,
        }
    if check is EvidenceCheck.KILL_SWITCH:
        return {
            'kill_persistent': True,
            'kill_state': 'KILLED',
            'manual_reset_required': True,
            'new_orders_blocked_when_killed': True,
            'new_orders_blocked_when_paused': True,
            'pause_persistent': True,
            'pause_state': 'PAUSED',
        }
    if check is EvidenceCheck.FAIL_CLOSED_ENVIRONMENT_IDENTITY:
        return {
            'accepted_chain_identity': _TESTNET_IDENTITY,
            'accepted_environment': _TESTNET_IDENTITY,
            'accepted_purpose': AuthorizationPurpose.TESTNET_EXECUTION.value,
            'ambiguous_identity_action': 'BLOCK',
            'mainnet_identity_accepted': False,
        }
    if check is EvidenceCheck.FULL_AUDIT_LOG:
        return {
            'append_only': True,
            'categories': [
                'RUNTIME_START',
                'RUNTIME_STOP',
                'ENVIRONMENT_AUTHORIZATION',
                'ORDER_INTENT',
                'SUBMISSION',
                'ACKNOWLEDGEMENT',
                'FILLS',
                'CANCEL_REPLACE',
                'RECONCILIATION',
                'RISK_REJECTION',
                'PAUSE',
                'KILL',
                'RECOVERY',
            ],
            'persistent': True,
            'redaction_required': True,
            'secret_categories_excluded': [
                'API_KEY',
                'MNEMONIC',
                'PRIVATE_KEY',
                'SEED_PHRASE',
                'WALLET_KEY',
            ],
            'secret_material_persisted': False,
        }
    return None


def _positive_decimal(value: object) -> Decimal | None:
    if not isinstance(value, str) or _CANONICAL_POSITIVE_DECIMAL_RE.fullmatch(value) is None:
        return None
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        return None
    if not parsed.is_finite() or parsed <= 0:
        return None
    decimal_tuple = parsed.as_tuple()
    exponent = decimal_tuple.exponent
    if (
        not isinstance(exponent, int)
        or len(decimal_tuple.digits) > 64
        or abs(exponent) > 64
        or abs(parsed.adjusted()) > 64
    ):
        return None
    canonical = format(parsed, 'f')
    if '.' in canonical:
        canonical = canonical.rstrip('0').rstrip('.')
    return parsed if value == canonical else None


def _validate_testnet_risk_limits(
    limits: Mapping[str, object],
    *,
    expected_sha256: str,
) -> bool:
    if frozenset(limits) != _TESTNET_RISK_LIMIT_KEYS:
        return False
    max_gross_notional = _positive_decimal(limits.get('max_gross_notional'))
    max_order_notional = _positive_decimal(limits.get('max_order_notional'))
    max_order_quantity = _positive_decimal(limits.get('max_order_quantity'))
    max_position_notional = _positive_decimal(limits.get('max_position_notional'))
    max_position_quantity = _positive_decimal(limits.get('max_position_quantity'))
    if (
        max_gross_notional is None
        or max_order_notional is None
        or max_order_quantity is None
        or max_position_notional is None
        or max_position_quantity is None
        or max_order_notional > max_position_notional
        or max_position_notional > max_gross_notional
        or max_order_quantity > max_position_quantity
        or max_gross_notional > _TESTNET_RISK_DECIMAL_CEILINGS['max_gross_notional']
        or max_order_notional > _TESTNET_RISK_DECIMAL_CEILINGS['max_order_notional']
        or max_order_quantity > _TESTNET_RISK_DECIMAL_CEILINGS['max_order_quantity']
        or max_position_notional
        > _TESTNET_RISK_DECIMAL_CEILINGS['max_position_notional']
        or max_position_quantity
        > _TESTNET_RISK_DECIMAL_CEILINGS['max_position_quantity']
    ):
        return False
    for name in (
        'cancel_requests_per_minute',
        'deadman_interval_seconds',
        'market_stale_after_seconds',
        'max_concurrent_orders',
        'reconciliation_stale_after_seconds',
        'replace_requests_per_minute',
        'submit_requests_per_minute',
    ):
        value = limits.get(name)
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value <= 0
            or value > _TESTNET_RISK_INTEGER_CEILINGS[name]
        ):
            return False
    limits_sha256 = hashlib.sha256(_canonical_json_bytes(dict(limits))).hexdigest()
    return limits_sha256 == expected_sha256


def _validate_testnet_risk_facts(
    facts: Mapping[str, object],
    context: EvidenceVerificationContext,
) -> bool:
    if frozenset(facts) != _TESTNET_RISK_FACT_KEYS:
        return False
    if (
        facts.get('enforcement') != 'FAIL_CLOSED'
        or facts.get('namespace') != _TESTNET_CREDENTIAL_NAMESPACE
        or facts.get('limits_sha256') != context.subject.risk_limits_hash
    ):
        return False
    limits = facts.get('limits')
    return isinstance(limits, Mapping) and _validate_testnet_risk_limits(
        limits,
        expected_sha256=context.subject.risk_limits_hash,
    )


def _verify_testnet_evidence(
    context: EvidenceVerificationContext,
    *,
    expected_check: EvidenceCheck,
) -> bool:
    if (
        context.check is not expected_check
        or context.environment is not EnvironmentClass.TESTNET
        or context.purpose is not AuthorizationPurpose.TESTNET_EXECUTION
        or context.environment_identity != _TESTNET_IDENTITY
        or context.execution_network is not ExecutionNetwork.TESTNET
        or context.credential_scope is not CredentialScope.TESTNET
        or context.order_capability is not OrderCapability.TESTNET_ONLY
    ):
        return False
    try:
        decoded = json.loads(
            context.artifact_bytes.decode('utf-8'),
            object_pairs_hook=_strict_object,
            parse_constant=_reject_constant,
        )
    except (AuthorizationManifestError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(decoded, Mapping) or frozenset(decoded) != _TESTNET_EVIDENCE_KEYS:
        return False
    if context.artifact_bytes != _canonical_json_bytes(dict(decoded)):
        return False
    subject = decoded.get('subject')
    facts = decoded.get('facts')
    validation = decoded.get('validation')
    if (
        type(decoded.get('schema_version')) is not int
        or decoded.get('schema_version') != 1
        or decoded.get('environment') != EnvironmentClass.TESTNET.value
        or decoded.get('purpose') != AuthorizationPurpose.TESTNET_EXECUTION.value
        or decoded.get('check') != expected_check.value
        or not isinstance(subject, Mapping)
        or dict(subject) != context.subject.to_dict()
        or not isinstance(facts, Mapping)
        or not isinstance(validation, Mapping)
        or frozenset(validation) != _TESTNET_VALIDATION_KEYS
        or not all(
            isinstance(validation.get(name), str)
            and _SHA256_RE.fullmatch(cast(str, validation.get(name))) is not None
            for name in _TESTNET_VALIDATION_KEYS
        )
    ):
        return False
    if expected_check is EvidenceCheck.BOUNDED_POSITION_NOTIONAL:
        return _validate_testnet_risk_facts(facts, context)
    expected_facts = _fixed_testnet_facts(expected_check)
    return expected_facts is not None and dict(facts) == expected_facts


def _make_testnet_evidence_verifier(check: EvidenceCheck) -> EvidenceVerifier:
    def verify(context: EvidenceVerificationContext) -> bool:
        return _verify_testnet_evidence(context, expected_check=check)

    return verify


def _paper_subject_is_exact(subject: ReadinessSubject) -> bool:
    return _PAPER_SUPPORTED_CANDIDATE_SOURCES.get(subject.candidate_id) == subject.source_identity


def _paper_subject_is_multistrategy(subject: ReadinessSubject) -> bool:
    return subject.candidate_id == _PAPER_MULTISTRATEGY_CANDIDATE_ID


def _paper_release_repository_root() -> Path:
    module_path = Path(__file__).resolve(strict=True)
    repository_root = module_path.parents[2]
    expected_module = (
        repository_root / 'src' / 'hyperlab' / 'environment_authorization.py'
    ).resolve(strict=True)
    if expected_module != module_path:
        raise ValueError('PAPER release verification requires the reviewed source checkout')
    return repository_root


def _paper_release_stat_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


def _paper_release_path_is_link(path: Path) -> bool:
    is_junction = getattr(path, 'is_junction', None)
    return path.is_symlink() or (
        callable(is_junction) and cast(Callable[[], bool], is_junction)()
    )


def _resolve_paper_release_path(root: Path, relative_path: str) -> Path:
    pure = PurePosixPath(relative_path)
    if (
        not relative_path
        or '\\' in relative_path
        or pure.is_absolute()
        or pure.as_posix() != relative_path
        or any(part in {'', '.', '..'} for part in pure.parts)
    ):
        raise ValueError('PAPER release path is not an exact safe POSIX-relative path')
    resolved_root = root.resolve(strict=True)
    if not resolved_root.is_dir():
        raise ValueError('PAPER release root is not a directory')
    cursor = resolved_root
    for part in pure.parts:
        cursor = cursor / part
        if _paper_release_path_is_link(cursor):
            raise ValueError('PAPER release paths may not traverse links or junctions')
    resolved = cursor.resolve(strict=True)
    try:
        resolved.relative_to(resolved_root)
    except ValueError as error:
        raise ValueError('PAPER release path escapes its root') from error
    if not resolved.is_file():
        raise ValueError('PAPER release path must identify a regular file')
    return resolved


def _read_stable_paper_release_file(path: Path) -> bytes:
    before = path.stat()
    if before.st_size > _MAX_SEMANTIC_ARTIFACT_BYTES:
        raise ValueError('PAPER release file exceeds the compiled bounded size')
    with path.open('rb') as stream:
        handle_before = os.fstat(stream.fileno())
        payload = stream.read(_MAX_SEMANTIC_ARTIFACT_BYTES + 1)
        handle_after = os.fstat(stream.fileno())
    after = path.stat()
    identities = (
        _paper_release_stat_identity(before),
        _paper_release_stat_identity(handle_before),
        _paper_release_stat_identity(handle_after),
        _paper_release_stat_identity(after),
    )
    if len(payload) > _MAX_SEMANTIC_ARTIFACT_BYTES or len(set(identities)) != 1:
        raise ValueError('PAPER release file changed while it was being hashed')
    return payload


def _canonical_paper_release_file_bytes(root: Path, relative_path: str) -> bytes:
    payload = _read_stable_paper_release_file(
        _resolve_paper_release_path(root, relative_path)
    )
    if b'\0' in payload:
        return payload
    try:
        text = payload.decode('utf-8')
    except UnicodeDecodeError:
        return payload
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    if relative_path == 'src/hyperlab/cli.py':
        for name in _PAPER_CLI_DERIVED_BINDING_NAMES:
            pattern = re.compile(
                rf'(?m)^{re.escape(name)} = "[0-9a-f]{{64}}"$'
            )
            text, count = pattern.subn(
                f'{name} = "<PHASE12_DERIVED_BINDING_SHA256>"',
                text,
            )
            if count != 1:
                raise ValueError(
                    f'PAPER release CLI binding {name!r} is missing or ambiguous'
                )
    return text.encode('utf-8')


def _paper_release_code_paths(repository_root: Path) -> tuple[str, ...]:
    resolved_root = repository_root.resolve(strict=True)
    source_root = (resolved_root / 'src' / 'hyperlab').resolve(strict=True)
    try:
        source_root.relative_to(resolved_root)
    except ValueError as error:
        raise ValueError('PAPER release source root escapes the repository') from error
    selected = set(_PAPER_RELEASE_CODE_FIXED_PATHS)
    for path in source_root.rglob('*.py'):
        if path.is_file():
            selected.add(path.relative_to(resolved_root).as_posix())
    return tuple(sorted(selected))


def _paper_release_path_identities(
    repository_root: Path,
    relative_paths: tuple[str, ...],
) -> dict[str, tuple[int, int, int, int]]:
    return {
        relative_path: _paper_release_stat_identity(
            _resolve_paper_release_path(repository_root, relative_path).stat()
        )
        for relative_path in relative_paths
    }


def _paper_release_code_core(
    files: Mapping[str, str],
    *,
    candidate_id: str = _PAPER_CANDIDATE_ID,
) -> dict[str, object]:
    if candidate_id not in _PAPER_SUPPORTED_CANDIDATE_SOURCES:
        raise ValueError('unsupported PAPER candidate identity')
    return {
        'artifact_schema_version': _PAPER_RELEASE_CODE_MANIFEST_SCHEMA_VERSION,
        'candidate_id': candidate_id,
        'canonicalization': _PAPER_RELEASE_CODE_CANONICALIZATION,
        'files': dict(files),
    }


def _paper_release_code_sha256(
    files: Mapping[str, str],
    *,
    candidate_id: str = _PAPER_CANDIDATE_ID,
) -> str:
    return hashlib.sha256(
        _canonical_json_bytes(_paper_release_code_core(files, candidate_id=candidate_id))
    ).hexdigest()


def _build_paper_release_code_manifest_bytes(
    repository_root: Path | None = None,
    *,
    candidate_id: str = _PAPER_CANDIDATE_ID,
) -> bytes:
    root = (
        _paper_release_repository_root()
        if repository_root is None
        else repository_root.resolve(strict=True)
    )
    selected_paths = _paper_release_code_paths(root)
    identities_before = _paper_release_path_identities(root, selected_paths)
    files = {
        relative_path: hashlib.sha256(
            _canonical_paper_release_file_bytes(root, relative_path)
        ).hexdigest()
        for relative_path in selected_paths
    }
    paths_after = _paper_release_code_paths(root)
    if (
        paths_after != selected_paths
        or _paper_release_path_identities(root, paths_after) != identities_before
    ):
        raise ValueError(
            'PAPER release code changed while the manifest was built'
        )
    return _canonical_json_bytes(
        {
            **_paper_release_code_core(files, candidate_id=candidate_id),
            'release_code_sha256': _paper_release_code_sha256(
                files,
                candidate_id=candidate_id,
            ),
        }
    )


def _decode_paper_release_code_manifest(
    manifest_bytes: bytes,
    *,
    expected_candidate_id: str = _PAPER_CANDIDATE_ID,
) -> tuple[dict[str, str], str]:
    if (
        not isinstance(manifest_bytes, bytes)
        or not manifest_bytes
        or len(manifest_bytes) > _MAX_SEMANTIC_ARTIFACT_BYTES
    ):
        raise ValueError('PAPER release-code manifest bytes are missing or too large')
    try:
        decoded = json.loads(
            manifest_bytes.decode('utf-8'),
            object_pairs_hook=_strict_object,
            parse_constant=_reject_constant,
        )
    except (
        AuthorizationManifestError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        raise ValueError('PAPER release-code manifest is not strict UTF-8 JSON') from error
    if (
        not isinstance(decoded, Mapping)
        or frozenset(decoded) != _PAPER_RELEASE_CODE_MANIFEST_KEYS
        or manifest_bytes != _canonical_json_bytes(dict(decoded))
        or type(decoded.get('artifact_schema_version')) is not int
        or decoded.get('artifact_schema_version')
        != _PAPER_RELEASE_CODE_MANIFEST_SCHEMA_VERSION
        or decoded.get('candidate_id') != expected_candidate_id
        or decoded.get('canonicalization') != _PAPER_RELEASE_CODE_CANONICALIZATION
    ):
        raise ValueError('PAPER release-code manifest lost its exact compiled schema')
    raw_files = decoded.get('files')
    if not isinstance(raw_files, Mapping) or not raw_files:
        raise ValueError('PAPER release-code manifest files must be a non-empty object')
    files: dict[str, str] = {}
    for raw_path, raw_digest in raw_files.items():
        if not isinstance(raw_path, str) or not isinstance(raw_digest, str):
            raise ValueError('PAPER release-code manifest file bindings must be strings')
        pure = PurePosixPath(raw_path)
        if (
            not raw_path
            or '\\' in raw_path
            or pure.is_absolute()
            or pure.as_posix() != raw_path
            or any(part in {'', '.', '..'} for part in pure.parts)
            or _SHA256_RE.fullmatch(raw_digest) is None
        ):
            raise ValueError('PAPER release-code manifest contains an invalid file binding')
        files[raw_path] = raw_digest
    release_code_sha256 = decoded.get('release_code_sha256')
    if (
        not isinstance(release_code_sha256, str)
        or _SHA256_RE.fullmatch(release_code_sha256) is None
        or release_code_sha256
        != _paper_release_code_sha256(files, candidate_id=expected_candidate_id)
    ):
        raise ValueError('PAPER release-code aggregate digest is inconsistent')
    return files, release_code_sha256


def current_paper_release_code_sha256(
    *,
    candidate_id: str = _PAPER_CANDIDATE_ID,
) -> str:
    """Hash the exact current reviewed Paper release-code path set."""

    _, release_code_sha256 = _decode_paper_release_code_manifest(
        _build_paper_release_code_manifest_bytes(candidate_id=candidate_id),
        expected_candidate_id=candidate_id,
    )
    return release_code_sha256


def _normalize_paper_distribution_name(value: str) -> str:
    return _PAPER_DISTRIBUTION_NORMALIZE_RE.sub('-', value).lower()


def _parse_paper_runtime_lock(
    lock_bytes: bytes,
) -> dict[str, dict[str, object]]:
    if (
        not isinstance(lock_bytes, bytes)
        or not lock_bytes
        or len(lock_bytes) > _MAX_SEMANTIC_ARTIFACT_BYTES
        or b'\0' in lock_bytes
    ):
        raise ValueError('PAPER runtime lock bytes are missing, binary, or too large')
    try:
        text = lock_bytes.decode('utf-8')
    except UnicodeDecodeError as error:
        raise ValueError('PAPER runtime lock must be strict UTF-8') from error

    pins: dict[str, dict[str, object]] = {}
    current_name: str | None = None
    current_closed = True
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        hash_match = _PAPER_RUNTIME_LOCK_HASH_RE.fullmatch(line)
        if hash_match is not None:
            if current_name is None or current_closed:
                raise ValueError(
                    f'PAPER runtime lock has an orphan hash at line {line_number}'
                )
            hashes = cast(list[str], pins[current_name]['hashes'])
            digest = hash_match.group('digest')
            if digest in hashes:
                raise ValueError(
                    f'PAPER runtime lock repeats a hash at line {line_number}'
                )
            hashes.append(digest)
            current_closed = hash_match.group('continuation') is None
            continue

        pin_match = _PAPER_RUNTIME_LOCK_PIN_RE.fullmatch(line)
        if pin_match is None:
            raise ValueError(
                f'PAPER runtime lock has unsupported syntax at line {line_number}'
            )
        if current_name is not None:
            previous_hashes = cast(list[str], pins[current_name]['hashes'])
            if not previous_hashes or not current_closed:
                raise ValueError(
                    f'PAPER runtime lock has an incomplete pin before line {line_number}'
                )
        if pin_match.group('continuation') is None:
            raise ValueError(
                f'PAPER runtime lock pin lacks required hashes at line {line_number}'
            )
        raw_name = pin_match.group('name')
        normalized_name = _normalize_paper_distribution_name(raw_name)
        if raw_name != normalized_name:
            raise ValueError(
                f'PAPER runtime lock distribution name is not canonical at line {line_number}'
            )
        if normalized_name in pins:
            raise ValueError(
                f'PAPER runtime lock repeats distribution {normalized_name!r}'
            )
        pins[normalized_name] = {
            'hashes': [],
            'version': pin_match.group('version'),
        }
        current_name = normalized_name
        current_closed = False
        if len(pins) > 512:
            raise ValueError('PAPER runtime lock exceeds 512 distributions')

    if current_name is None:
        raise ValueError('PAPER runtime lock contains no exact pins')
    if not cast(list[str], pins[current_name]['hashes']) or not current_closed:
        raise ValueError('PAPER runtime lock ends with an incomplete pin')
    return {
        name: {
            'hashes': sorted(cast(list[str], pin['hashes'])),
            'version': pin['version'],
        }
        for name, pin in sorted(pins.items())
    }


def _paper_runtime_version_facts(value: Sequence[object]) -> dict[str, object]:
    if len(value) != 5:
        raise ValueError('PAPER runtime version tuple is malformed')
    major, minor, micro, releaselevel, serial = value
    return {
        'major': int(cast(int, major)),
        'micro': int(cast(int, micro)),
        'minor': int(cast(int, minor)),
        'releaselevel': str(cast(str, releaselevel)),
        'serial': int(cast(int, serial)),
    }


def _current_paper_cpython_facts() -> dict[str, object]:
    implementation = sys.implementation
    if implementation.name != 'cpython':
        raise ValueError('PAPER runtime requires CPython exactly')
    version_info = _paper_runtime_version_facts(sys.version_info)
    implementation_version = _paper_runtime_version_facts(
        implementation.version
    )
    cache_tag = implementation.cache_tag
    if not isinstance(cache_tag, str) or not cache_tag:
        raise ValueError('PAPER runtime CPython cache tag is unavailable')
    return {
        'abi_flags': str(getattr(sys, 'abiflags', '')),
        'byteorder': sys.byteorder,
        'cache_tag': cache_tag,
        'hexversion': sys.hexversion,
        'implementation': implementation.name,
        'implementation_version': implementation_version,
        'platform_machine': platform.machine(),
        'platform_system': platform.system(),
        'platform_tag': sysconfig.get_platform(),
        'pointer_bits': struct.calcsize('P') * 8,
        'python_compiler': platform.python_compiler(),
        'python_version': (
            f"{version_info['major']}.{version_info['minor']}."
            f"{version_info['micro']}"
        ),
        'version_info': version_info,
    }


def _paper_runtime_hexversion(version: Mapping[str, object]) -> int:
    release_nibble = {
        'alpha': 0xA,
        'beta': 0xB,
        'candidate': 0xC,
        'final': 0xF,
    }.get(cast(str, version.get('releaselevel')))
    if release_nibble is None:
        return -1
    return (
        (cast(int, version['major']) << 24)
        | (cast(int, version['minor']) << 16)
        | (cast(int, version['micro']) << 8)
        | (release_nibble << 4)
        | cast(int, version['serial'])
    )


def _validate_paper_runtime_version_facts(value: object) -> bool:
    if not isinstance(value, Mapping) or frozenset(value) != _PAPER_RUNTIME_VERSION_KEYS:
        return False
    for name in ('major', 'minor', 'micro', 'serial'):
        item = value.get(name)
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            return False
    return (
        cast(int, value.get('major')) > 0
        and cast(int, value.get('minor')) < 256
        and cast(int, value.get('micro')) < 256
        and cast(int, value.get('serial')) < 16
        and value.get('releaselevel')
        in {'alpha', 'beta', 'candidate', 'final'}
    )


def _validate_paper_runtime_interpreter_facts(value: object) -> bool:
    if (
        not isinstance(value, Mapping)
        or frozenset(value) != _PAPER_RUNTIME_INTERPRETER_KEYS
        or value.get('implementation') != 'cpython'
        or not _validate_paper_runtime_version_facts(value.get('version_info'))
        or not _validate_paper_runtime_version_facts(
            value.get('implementation_version')
        )
        or value.get('implementation_version') != value.get('version_info')
        or not isinstance(value.get('python_version'), str)
        or not isinstance(value.get('cache_tag'), str)
        or not isinstance(value.get('abi_flags'), str)
        or any(
            not isinstance(value.get(name), str)
            or not cast(str, value.get(name))
            or len(cast(str, value.get(name))) > 256
            or any(
                character in {'\0', '\r', '\n'}
                for character in cast(str, value.get(name))
            )
            for name in (
                'platform_machine',
                'platform_system',
                'platform_tag',
                'python_compiler',
            )
        )
        or value.get('byteorder') not in {'big', 'little'}
        or isinstance(value.get('pointer_bits'), bool)
        or not isinstance(value.get('pointer_bits'), int)
        or cast(int, value.get('pointer_bits')) not in {32, 64, 128}
        or isinstance(value.get('hexversion'), bool)
        or not isinstance(value.get('hexversion'), int)
    ):
        return False
    version = cast(Mapping[str, object], value['version_info'])
    expected_version = (
        f"{version['major']}.{version['minor']}.{version['micro']}"
    )
    expected_cache_tag = f"cpython-{version['major']}{version['minor']}"
    return (
        value.get('python_version') == expected_version
        and value.get('cache_tag') == expected_cache_tag
        and cast(str, value.get('abi_flags')).isascii()
        and len(cast(str, value.get('abi_flags'))) <= 32
        and value.get('hexversion') == _paper_runtime_hexversion(version)
    )


def _paper_installed_distribution_versions(
    pins: Mapping[str, Mapping[str, object]],
    distribution_version: Callable[[str], str],
) -> dict[str, str]:
    installed: dict[str, str] = {}
    for name in pins:
        try:
            version = distribution_version(name)
        except importlib_metadata.PackageNotFoundError as error:
            raise ValueError(
                f'PAPER runtime required distribution {name!r} is not installed'
            ) from error
        if (
            not isinstance(version, str)
            or not version
            or version != version.strip()
            or any(character.isspace() for character in version)
        ):
            raise ValueError(
                f'PAPER runtime distribution {name!r} has an invalid version'
            )
        installed[name] = version
    return installed


def _paper_runtime_environment_core(
    *,
    candidate_id: str,
    installed: Mapping[str, str],
    interpreter: Mapping[str, object],
    lock_bytes: bytes,
    pins: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    return {
        'artifact_schema_version': _PAPER_RUNTIME_ENVIRONMENT_SCHEMA_VERSION,
        'candidate_id': candidate_id,
        'canonicalization': _PAPER_RUNTIME_ENVIRONMENT_CANONICALIZATION,
        'distribution_count': len(pins),
        'extras_allowed': True,
        'installed_distributions': dict(installed),
        'interpreter': dict(interpreter),
        'lock': {
            'path': 'requirements-runtime.lock',
            'pins': {name: dict(pin) for name, pin in pins.items()},
            'sha256': hashlib.sha256(lock_bytes).hexdigest(),
        },
    }


def _build_paper_runtime_environment_attestation_bytes(
    repository_root: Path | None = None,
    *,
    candidate_id: str = _PAPER_CANDIDATE_ID,
    distribution_version: Callable[[str], str] | None = None,
    interpreter_facts: Callable[[], Mapping[str, object]] | None = None,
) -> bytes:
    root = (
        _paper_release_repository_root()
        if repository_root is None
        else repository_root.resolve(strict=True)
    )
    lock_path = _resolve_paper_release_path(root, 'requirements-runtime.lock')
    lock_identity = _paper_release_stat_identity(lock_path.stat())
    lock_bytes = _canonical_paper_release_file_bytes(
        root,
        'requirements-runtime.lock',
    )
    pins = _parse_paper_runtime_lock(lock_bytes)
    lookup = distribution_version or importlib_metadata.version
    facts_reader = interpreter_facts or _current_paper_cpython_facts
    installed_before = _paper_installed_distribution_versions(pins, lookup)
    interpreter_before = dict(facts_reader())
    installed_after = _paper_installed_distribution_versions(pins, lookup)
    interpreter_after = dict(facts_reader())
    if (
        _paper_release_stat_identity(lock_path.stat()) != lock_identity
        or installed_after != installed_before
        or interpreter_after != interpreter_before
    ):
        raise ValueError(
            'PAPER runtime environment changed while the attestation was built'
        )
    if not _validate_paper_runtime_interpreter_facts(interpreter_before):
        raise ValueError('PAPER runtime interpreter facts are invalid or not CPython')
    mismatches = [
        f"{name}:locked={pin['version']},installed={installed_before[name]}"
        for name, pin in pins.items()
        if installed_before[name] != pin['version']
    ]
    if mismatches:
        raise ValueError(
            'PAPER runtime locked distribution mismatch: ' + ', '.join(mismatches)
        )
    core = _paper_runtime_environment_core(
        candidate_id=candidate_id,
        installed=installed_before,
        interpreter=interpreter_before,
        lock_bytes=lock_bytes,
        pins=pins,
    )
    return _canonical_json_bytes(
        {
            **core,
            'runtime_environment_sha256': hashlib.sha256(
                _canonical_json_bytes(core)
            ).hexdigest(),
        }
    )


def _decode_paper_runtime_environment_attestation(
    payload: bytes,
    *,
    expected_candidate_id: str = _PAPER_CANDIDATE_ID,
) -> tuple[dict[str, object], str]:
    if (
        not isinstance(payload, bytes)
        or not payload
        or len(payload) > _MAX_SEMANTIC_ARTIFACT_BYTES
    ):
        raise ValueError('PAPER runtime-environment attestation is missing or too large')
    try:
        decoded = json.loads(
            payload.decode('utf-8'),
            object_pairs_hook=_strict_object,
            parse_constant=_reject_constant,
        )
    except (
        AuthorizationManifestError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        raise ValueError(
            'PAPER runtime-environment attestation is not strict UTF-8 JSON'
        ) from error
    if (
        not isinstance(decoded, Mapping)
        or frozenset(decoded) != _PAPER_RUNTIME_ENVIRONMENT_KEYS
        or payload != _canonical_json_bytes(dict(decoded))
        or decoded.get('artifact_schema_version')
        != _PAPER_RUNTIME_ENVIRONMENT_SCHEMA_VERSION
        or decoded.get('candidate_id') != expected_candidate_id
        or decoded.get('canonicalization')
        != _PAPER_RUNTIME_ENVIRONMENT_CANONICALIZATION
        or decoded.get('extras_allowed') is not True
        or not _validate_paper_runtime_interpreter_facts(
            decoded.get('interpreter')
        )
    ):
        raise ValueError(
            'PAPER runtime-environment attestation lost its exact schema'
        )
    lock = decoded.get('lock')
    installed = decoded.get('installed_distributions')
    if (
        not isinstance(lock, Mapping)
        or frozenset(lock) != _PAPER_RUNTIME_ENVIRONMENT_LOCK_KEYS
        or lock.get('path') != 'requirements-runtime.lock'
        or not isinstance(lock.get('sha256'), str)
        or _SHA256_RE.fullmatch(cast(str, lock.get('sha256'))) is None
        or not isinstance(lock.get('pins'), Mapping)
        or not isinstance(installed, Mapping)
    ):
        raise ValueError('PAPER runtime-environment lock binding is invalid')
    pins = cast(Mapping[object, object], lock['pins'])
    if any(not isinstance(name, str) for name in pins):
        raise ValueError('PAPER runtime-environment distribution set is invalid')
    pin_names = tuple(cast(str, name) for name in pins)
    if (
        not pins
        or pin_names != tuple(sorted(pin_names))
        or frozenset(installed) != frozenset(pins)
        or type(decoded.get('distribution_count')) is not int
        or decoded.get('distribution_count') != len(pins)
    ):
        raise ValueError('PAPER runtime-environment distribution set is invalid')
    for name, raw_pin in pins.items():
        if (
            not isinstance(name, str)
            or name != _normalize_paper_distribution_name(name)
            or not isinstance(raw_pin, Mapping)
            or frozenset(raw_pin) != {'hashes', 'version'}
            or not isinstance(raw_pin.get('version'), str)
            or installed.get(name) != raw_pin.get('version')
            or not isinstance(raw_pin.get('hashes'), list)
            or not raw_pin.get('hashes')
            or any(
                not isinstance(item, str) or _SHA256_RE.fullmatch(item) is None
                for item in cast(list[object], raw_pin.get('hashes'))
            )
            or raw_pin.get('hashes')
            != sorted(set(cast(list[str], raw_pin.get('hashes'))))
        ):
            raise ValueError(
                'PAPER runtime-environment distribution binding is invalid'
            )
    core = dict(decoded)
    runtime_environment_sha256 = core.pop('runtime_environment_sha256', None)
    if (
        not isinstance(runtime_environment_sha256, str)
        or _SHA256_RE.fullmatch(runtime_environment_sha256) is None
        or runtime_environment_sha256
        != hashlib.sha256(_canonical_json_bytes(core)).hexdigest()
    ):
        raise ValueError('PAPER runtime-environment digest is inconsistent')
    return dict(decoded), runtime_environment_sha256


def paper_runtime_environment_attestation_bytes(
    repository_root: Path | None = None,
    *,
    candidate_id: str = _PAPER_CANDIDATE_ID,
) -> bytes:
    """Return canonical proof of exact locked distributions and CPython facts."""

    return _build_paper_runtime_environment_attestation_bytes(
        repository_root,
        candidate_id=candidate_id,
    )


def current_paper_runtime_environment_sha256(
    *,
    candidate_id: str = _PAPER_CANDIDATE_ID,
) -> str:
    """Hash the exact current locked Paper Python runtime environment."""

    _, runtime_environment_sha256 = (
        _decode_paper_runtime_environment_attestation(
            paper_runtime_environment_attestation_bytes(candidate_id=candidate_id),
            expected_candidate_id=candidate_id,
        )
    )
    return runtime_environment_sha256


def _paper_runtime_environment_attestation(
    attestation_bytes: bytes,
    *,
    candidate_id: str = _PAPER_CANDIDATE_ID,
) -> dict[str, object]:
    decoded, runtime_environment_sha256 = (
        _decode_paper_runtime_environment_attestation(
            attestation_bytes,
            expected_candidate_id=candidate_id,
        )
    )
    return {
        'canonicalization': _PAPER_RUNTIME_ENVIRONMENT_CANONICALIZATION,
        'distribution_count': decoded['distribution_count'],
        'path': _PAPER_RUNTIME_ENVIRONMENT_ARTIFACT,
        'runtime_environment_sha256': runtime_environment_sha256,
        'sha256': hashlib.sha256(attestation_bytes).hexdigest(),
    }


def _verify_paper_runtime_environment_attestation(
    attestation: Mapping[str, object],
    context: EvidenceVerificationContext,
) -> bool:
    if (
        frozenset(attestation) != _PAPER_RUNTIME_ENVIRONMENT_ATTESTATION_KEYS
        or attestation.get('canonicalization')
        != _PAPER_RUNTIME_ENVIRONMENT_CANONICALIZATION
        or type(attestation.get('distribution_count')) is not int
        or cast(int, attestation.get('distribution_count')) <= 0
        or attestation.get('path') != _PAPER_RUNTIME_ENVIRONMENT_ARTIFACT
        or not isinstance(attestation.get('runtime_environment_sha256'), str)
        or _SHA256_RE.fullmatch(
            cast(str, attestation.get('runtime_environment_sha256'))
        )
        is None
        or not isinstance(attestation.get('sha256'), str)
        or _SHA256_RE.fullmatch(cast(str, attestation.get('sha256'))) is None
        or context.evidence_root is None
    ):
        return False
    try:
        evidence_root = context.evidence_root.resolve(strict=True)
        artifact_path = _resolve_paper_release_path(
            evidence_root,
            _PAPER_RUNTIME_ENVIRONMENT_ARTIFACT,
        )
        artifact_identity = _paper_release_stat_identity(artifact_path.stat())
        artifact_bytes = _read_stable_paper_release_file(artifact_path)
        if (
            hashlib.sha256(artifact_bytes).hexdigest()
            != attestation.get('sha256')
        ):
            return False
        decoded, runtime_environment_sha256 = (
            _decode_paper_runtime_environment_attestation(
                artifact_bytes,
                expected_candidate_id=context.subject.candidate_id,
            )
        )
        current_bytes = paper_runtime_environment_attestation_bytes(
            candidate_id=context.subject.candidate_id,
        )
        current_decoded, current_runtime_environment_sha256 = (
            _decode_paper_runtime_environment_attestation(
                current_bytes,
                expected_candidate_id=context.subject.candidate_id,
            )
        )
        if (
            artifact_bytes != current_bytes
            or decoded != current_decoded
            or runtime_environment_sha256
            != current_runtime_environment_sha256
            or _paper_release_stat_identity(artifact_path.stat())
            != artifact_identity
        ):
            return False
    except (OSError, TypeError, ValueError):
        return False
    return (
        attestation.get('distribution_count')
        == decoded['distribution_count']
        and attestation.get('runtime_environment_sha256')
        == runtime_environment_sha256
    )


def _paper_release_code_attestation(
    manifest_bytes: bytes,
    *,
    candidate_id: str = _PAPER_CANDIDATE_ID,
) -> dict[str, object]:
    files, release_code_sha256 = _decode_paper_release_code_manifest(
        manifest_bytes,
        expected_candidate_id=candidate_id,
    )
    return {
        'canonicalization': _PAPER_RELEASE_CODE_CANONICALIZATION,
        'file_count': len(files),
        'path': _PAPER_RELEASE_CODE_MANIFEST_ARTIFACT,
        'release_code_sha256': release_code_sha256,
        'sha256': hashlib.sha256(manifest_bytes).hexdigest(),
    }


def _verify_paper_release_code_attestation(
    attestation: Mapping[str, object],
    context: EvidenceVerificationContext,
) -> bool:
    if (
        frozenset(attestation) != _PAPER_RELEASE_CODE_ATTESTATION_KEYS
        or attestation.get('canonicalization')
        != _PAPER_RELEASE_CODE_CANONICALIZATION
        or type(attestation.get('file_count')) is not int
        or cast(int, attestation.get('file_count')) <= 0
        or attestation.get('path') != _PAPER_RELEASE_CODE_MANIFEST_ARTIFACT
        or not isinstance(attestation.get('release_code_sha256'), str)
        or _SHA256_RE.fullmatch(
            cast(str, attestation.get('release_code_sha256'))
        )
        is None
        or not isinstance(attestation.get('sha256'), str)
        or _SHA256_RE.fullmatch(cast(str, attestation.get('sha256'))) is None
        or context.evidence_root is None
    ):
        return False
    try:
        evidence_root = context.evidence_root.resolve(strict=True)
        manifest_path = _resolve_paper_release_path(
            evidence_root,
            _PAPER_RELEASE_CODE_MANIFEST_ARTIFACT,
        )
        manifest_identity = _paper_release_stat_identity(manifest_path.stat())
        manifest_bytes = _read_stable_paper_release_file(manifest_path)
        if hashlib.sha256(manifest_bytes).hexdigest() != attestation.get('sha256'):
            return False
        files, release_code_sha256 = _decode_paper_release_code_manifest(
            manifest_bytes,
            expected_candidate_id=context.subject.candidate_id,
        )
        repository_root = _paper_release_repository_root()
        expected_paths = _paper_release_code_paths(repository_root)
        identities_before = _paper_release_path_identities(repository_root, expected_paths)
        if tuple(files) != expected_paths:
            return False
        for relative_path, expected_sha256 in files.items():
            actual_sha256 = hashlib.sha256(
                _canonical_paper_release_file_bytes(
                    repository_root,
                    relative_path,
                )
            ).hexdigest()
            if actual_sha256 != expected_sha256:
                return False
        paths_after = _paper_release_code_paths(repository_root)
        if (
            paths_after != expected_paths
            or _paper_release_path_identities(repository_root, paths_after)
            != identities_before
            or _paper_release_stat_identity(manifest_path.stat()) != manifest_identity
        ):
            return False
    except (OSError, TypeError, ValueError):
        return False
    return (
        attestation.get('file_count') == len(files)
        and attestation.get('release_code_sha256') == release_code_sha256
    )


def _validate_paper_risk_limits(
    limits: Mapping[str, object],
    *,
    expected_sha256: str,
    candidate_id: str = _PAPER_CANDIDATE_ID,
) -> bool:
    if frozenset(limits) != _PAPER_RISK_LIMIT_KEYS:
        return False
    decimals: dict[str, Decimal] = {}
    decimal_ceilings = (
        _PAPER_MULTISTRATEGY_RISK_DECIMAL_CEILINGS
        if candidate_id == _PAPER_MULTISTRATEGY_CANDIDATE_ID
        else _PAPER_RISK_DECIMAL_CEILINGS
    )
    integer_ceilings = (
        _PAPER_MULTISTRATEGY_RISK_INTEGER_CEILINGS
        if candidate_id == _PAPER_MULTISTRATEGY_CANDIDATE_ID
        else _PAPER_RISK_INTEGER_CEILINGS
    )
    if candidate_id not in _PAPER_SUPPORTED_CANDIDATE_SOURCES:
        return False
    for name, ceiling in decimal_ceilings.items():
        parsed = _positive_decimal(limits.get(name))
        if parsed is None or parsed > ceiling:
            return False
        decimals[name] = parsed
    if (
        decimals['max_order_notional'] > decimals['max_instrument_notional']
        or decimals['max_instrument_notional'] > decimals['max_gross_notional']
        or decimals['max_net_notional'] > decimals['max_gross_notional']
        or decimals['max_order_quantity'] > decimals['max_position_quantity']
        or decimals['max_daily_loss'] > decimals['max_drawdown']
    ):
        return False
    for name, integer_ceiling in integer_ceilings.items():
        value = limits.get(name)
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value <= 0
            or value > integer_ceiling
        ):
            return False
    limits_sha256 = hashlib.sha256(_canonical_json_bytes(dict(limits))).hexdigest()
    return limits_sha256 == expected_sha256


def _validate_paper_risk_facts(
    facts: Mapping[str, object],
    context: EvidenceVerificationContext,
) -> bool:
    if frozenset(facts) != _PAPER_RISK_FACT_KEYS:
        return False
    if (
        facts.get('enforcement') != 'FAIL_CLOSED'
        or facts.get('limits_sha256') != context.subject.risk_limits_hash
        or facts.get('order_capability') != OrderCapability.SIMULATED_ONLY.value
        or facts.get('protective_risk') != _paper_protective_risk_facts()
    ):
        return False
    limits = facts.get('limits')
    return isinstance(limits, Mapping) and _validate_paper_risk_limits(
        limits,
        expected_sha256=context.subject.risk_limits_hash,
        candidate_id=context.subject.candidate_id,
    )


def _fixed_paper_facts(
    check: EvidenceCheck,
    subject: ReadinessSubject,
) -> dict[str, object] | None:
    multistrategy = _paper_subject_is_multistrategy(subject)
    if check is EvidenceCheck.PUBLIC_MARKET_SOURCE:
        facts: dict[str, object] = {
            'admitted_record_types': ['bbo', 'connection_event', 'funding'],
            'authentication_required': False,
            'bootstrap_timeout_seconds': 120.0,
            'critical_funding_history': True,
            'descriptor_schema_version': 1,
            'http_final_url_must_equal_request': True,
            'http_endpoint': _PAPER_HTTP_ENDPOINT,
            'http_required_status': 200,
            'instruments': ['HL:BTC:perp', 'HL:ETH:perp'],
            'network': 'mainnet',
            'private_api_used': False,
            'public_only': True,
            'redirects_allowed': False,
            'reconnect_on_rest_refresh_failure': True,
            'rest_methods': ['metaAndAssetCtxs', 'spotMetaAndAssetCtxs', 'fundingHistory', 'l2Book'],
            'rest_l2_book_projection': 'BBO_BOOTSTRAP_AND_RESYNC_ONLY',
            'source_identity': subject.source_identity,
            'source_kind': 'PUBLIC_NORMALIZED',
            'transport_schema': 'hyperliquid-paper-public-transport-v2',
            'websocket_channels': ['bbo'],
            'websocket_endpoint': _PAPER_WEBSOCKET_ENDPOINT,
            'websocket_redirect_limit': 0,
            'websocket_required_http_status': 101,
        }
        if multistrategy:
            facts.update(
                {
                    'admitted_record_types': [
                        'bbo',
                        'connection_event',
                        'funding',
                        'market_context',
                    ],
                    'instruments': [
                        'HL:BTC:perp',
                        'HL:ETH:perp',
                        'HL:HYPE:perp',
                        'HL:HYPE:spot',
                    ],
                    'websocket_channels': ['activeAssetCtx', 'bbo'],
                }
            )
        return facts
    if check is EvidenceCheck.NORMALIZED_MARKET_EVENT_SCHEMA:
        facts = {
            'adapter_schema_version': 9,
            'bbo_tradability_policy': (
                'REST_BOOTSTRAP_NONTRADABLE_POST_CONNECT_EXACT_WEBSOCKET_LINEAGE_REQUIRED_MALFORMED_TERMINAL_V2'
            ),
            'causal_clock': 'RECEIVED_AT_UTC',
            'feed_contract': (
                'SOLE_COLLECTOR_NORMALIZED_BBO_CONNECTION_FUNDING_BOUNDED_PENDING_BBO_LATEST_VALUE_V9'
            ),
            'gap_or_stale_action': 'PAUSE_AND_NO_EXECUTION',
            'malformed_bbo_policy': (
                'TERMINAL_SOURCE_FAILURE_RESTART_AND_RESYNC_REQUIRED_NO_SILENT_DROP_V1'
            ),
            'pending_bbo_coalescing': (
                'LATEST_PER_INSTRUMENT_PER_UTC_MINUTE_BETWEEN_CONTROL_BARRIERS_V1'
            ),
            'global_connection_policy': (
                'MULTI_INSTRUMENT_GLOBAL_EVENT_SORTED_ORDINAL_INITIAL_BOOTSTRAP_CONNECT_HEALTH_ONLY_V4'
            ),
            'normalized_record_schema_versions': {
                'bbo': 2,
                'connection_event': 2,
                'funding': 2,
            },
            'paper_market_event_schema_version': 1,
            'rest_bootstrap_execution_eligible': False,
            'rest_bootstrap_lineage': 'NON_EXECUTABLE_INITIALIZATION_ONLY',
            'synthetic_data_allowed': False,
            'websocket_lineage_required_after_connect': True,
        }
        if multistrategy:
            facts.update(
                {
                    'adapter_schema_version': 10,
                    'feed_contract': (
                        'SOLE_COLLECTOR_NORMALIZED_BBO_CONNECTION_FUNDING_MARKET_CONTEXT_'
                        'BOUNDED_PENDING_BBO_LATEST_VALUE_V10'
                    ),
                    'funding_time_policy': 'HYPERLIQUID_FUNDING_HISTORY_FIRST_60_SECONDS_CANONICAL_EXACT_UTC_HOUR_V1',
                    'instrument_route_policy': 'EXPLICIT_MAPPING_PRODUCT_IDENTITY_BOUND_V2',
                    'market_context_policy': 'LATEST_CAUSAL_CONTEXT_ATTACHED_TO_BBO_V1',
                    'normalized_record_schema_versions': {
                        'bbo': 2,
                        'connection_event': 2,
                        'funding': 2,
                        'market_context': 1,
                    },
                    'product_identity_hash_binding': 'SOURCE_IDENTITY_EXACT_INSTRUMENT_MAP',
                }
            )
        return facts
    if check is EvidenceCheck.FROZEN_STRATEGY_CONFIG:
        facts = {
            'build_hash': subject.build_hash,
            'candidate_id': subject.candidate_id,
            'config_hash': subject.config_hash,
            'config_immutable': True,
            'gate_d_satisfied': False,
            'run_kind': 'TECHNICAL',
            'source_artifact_hash_binding': 'PAPER_CONFIG_DATA_HASH',
            'source_identity': subject.source_identity,
            'strategy_hash': subject.strategy_hash,
            'strategy_name': (
                _PAPER_MULTISTRATEGY_PRIMARY_STRATEGY_NAME
                if multistrategy
                else _PAPER_STRATEGY_NAME
            ),
            'validation_paper': False,
        }
        if multistrategy:
            facts.update(
                {
                    'ordered_strategy_identities': [
                        'phase05_cash_and_carry',
                        'phase08_robust_pairs',
                    ],
                    'portfolio_identity_binding': (
                        'PAPER_CONFIG_PORTFOLIO_ID_BINDS_ORDERED_STRATEGY_CONFIG_HASHES'
                    ),
                    'paper_config_schema_version': 3,
                }
            )
        return facts
    if check is EvidenceCheck.DETERMINISTIC_ACCOUNTING:
        facts = {
            'accounting_numeric_type': 'DECIMAL',
            'balanced_ledger_required': True,
            'fees_recorded_separately': True,
            'floating_point_accounting_allowed': False,
            'funding_recorded_separately': True,
            'partial_fills_recorded': True,
            'projection_derived_from_journal': True,
        }
        if multistrategy:
            facts.update(
                {
                    'account_and_strategy_ledgers_reconciled': True,
                    'same_instrument_offsetting_strategy_positions_preserved': True,
                    'strategy_attribution_required': True,
                }
            )
        return facts
    if check is EvidenceCheck.DETERMINISTIC_REPLAY:
        facts = {
            'event_hash_chain_verified': True,
            'event_order': 'COMMIT_SEQUENCE',
            'exact_projection_equivalence_required': True,
            'input_deduplication': True,
            'same_inputs_same_events': True,
            'strategy_hash_bound': subject.strategy_hash,
        }
        if multistrategy:
            facts.update(
                {
                    'ordered_strategy_evaluation': 'LEXICAL_STRATEGY_ID',
                    'strategy_membership_bound_by_config_hash': True,
                }
            )
        return facts
    if check is EvidenceCheck.CONSERVATIVE_COST_MODEL:
        facts = {
            'account_fee_discount_assumed': False,
            'adverse_exit_modelled': True,
            'calibration_status': 'UNCALIBRATED',
            'economic_eligibility': False,
            'execution_policy': 'IOC_TAKER_ONLY',
            'fee_policy': 'PUBLIC_TIER_0_OR_GREATER',
            'maker_fill_assumed': False,
            'missed_and_partial_fills_modelled': True,
            'nonflat_funding_without_causal_fresh_bbo_action': 'PAUSE_FAIL_CLOSED',
            'pre_activation_funding_history_ignored': True,
            'flat_without_mark_funding_effect': 'ZERO',
            'slippage_policy': 'BBO_DEPTH_PLUS_FIXED_ADVERSE',
            'synthetic_funding_reserve_allowed': False,
            'validation_scope': 'FIRST_SUPERVISED_10_TO_15_MINUTE_SMOKE',
        }
        if multistrategy:
            facts.update(
                {
                    'execution_policy': 'STRATEGY_MAKER_GTC_WITH_ENGINE_PROTECTIVE_IOC',
                    'fee_policy': 'TECHNICAL_PLACEHOLDER_ZERO_UNCALIBRATED',
                    'slippage_policy': 'TECHNICAL_PLACEHOLDER_UNCALIBRATED',
                    'validation_scope': 'TECHNICAL_SMOKE_ONLY_NOT_ECONOMIC_EVIDENCE',
                }
            )
        return facts
    if check is EvidenceCheck.RUNTIME_SOURCE_ATTESTATION:
        return {
            'config_hash': subject.config_hash,
            'descriptor_bound_before_start': True,
            'engine_semantic_build_hash': subject.build_hash,
            'frozen_runtime_cadence': {
                'runtime_source_poll_timeout_seconds': 0.25,
                'runtime_timer_interval_seconds': 1.0,
            },
            'release_code_binding': {
                'canonical_config_field': 'release_code_sha256',
                'current_checkout_digest_required_during_construction_and_before_start': True,
                'durable_config_snapshot': 'paper_runs.config_json',
                'run_config_hash_binds_canonical_snapshot': True,
                'run_start_config_hash_binds_canonical_snapshot': True,
            },
            'runtime_environment_binding': {
                'artifact_path': _PAPER_RUNTIME_ENVIRONMENT_ARTIFACT,
                'canonical_config_field': 'runtime_environment_sha256',
                'cpython_runtime_facts_bound': [
                    'abi_flags',
                    'byteorder',
                    'cache_tag',
                    'hexversion',
                    'implementation',
                    'implementation_version',
                    'platform_machine',
                    'platform_system',
                    'platform_tag',
                    'pointer_bits',
                    'python_compiler',
                    'python_version',
                    'version_info',
                ],
                'exact_locked_required_distributions': True,
                'extra_installed_distributions_allowed': True,
                'preflight_current_environment_required': True,
                'runtime_rechecked_before_lease_and_immediately_before_source_start': True,
            },
            'runtime_session_contract': {
                'active_definition': 'GENERATION_POSITIVE_ID_PRESENT_STOPPED_AT_NONE',
                'failure_leaves_active': [
                    'FAULT',
                    'IN_FLIGHT_EXCEPTION',
                    'BASE_EXCEPTION',
                    'SOURCE_OR_CLEANUP_FAILURE',
                ],
                'generation': 'STARTS_ZERO_EXACT_PLUS_ONE',
                'offline_recovery_required_before_replacement_start': True,
                'projection_fields': [
                    'runtime_session_generation',
                    'runtime_session_id',
                    'runtime_session_started_at',
                    'runtime_session_stopped_at',
                ],
                'projection_schema_version': 4 if multistrategy else 3,
                'session_id': 'LOWERCASE_SHA256',
                'start_before_public_source': True,
                'start_input_and_event': 'RUNTIME_SESSION_STARTED',
                'stop_after_successful_source_shutdown_while_lease_held': True,
                'stop_input_and_event': 'RUNTIME_SESSION_STOPPED',
                'stop_reasons': [
                    'COOPERATIVE_STOP',
                    'NORMAL_COMPLETION',
                ],
                'unclosed_failure_input': 'PAPER_RUNTIME_FAILURE',
                'unclosed_failure_origin': 'PAPER_RUNTIME_FAILURE',
                'unclosed_failure_phase': 'UNCLOSED_RUNTIME_SESSION',
                'unclosed_failure_type': 'UnclosedRuntimeSessionError',
            },
            'runtime_cadence_must_equal_frozen_config_before_lease': True,
            'runtime_lease': _paper_runtime_lease_facts(),
            'source_identity': subject.source_identity,
            'source_started_after_reconciliation': True,
            'startup_toctou_recheck': True,
            'strategy_hash': subject.strategy_hash,
        }
    if check is EvidenceCheck.CRASH_RECOVERY:
        return {
            'append_transaction': 'BEGIN_IMMEDIATE_ATOMIC',
            'atomic_commit_contents': [
                'INBOX_INPUT',
                'EVENTS',
                'LEDGER_TRANSACTIONS_AND_ENTRIES',
                'PROJECTION_CURRENT_HEAD',
                'PROJECTION_HISTORY',
                'ALERTS',
                'COMMIT_RECORD',
                'RUN_CURRENT_HEAD',
            ],
            'runtime_lease': _paper_runtime_lease_facts(),
            'commit_hash_chain_persisted': True,
            'critical_incident_on_failure': True,
            'durable_inbox_payload_hash_persisted': True,
            'event_hash_chain_persisted': True,
            'hash_chain_verified_on_restore': True,
            'journal_mode': 'DELETE',
            'projection_restored_from_journal': True,
            'concurrent_sqlite_writer_transactions_allowed': False,
            'sqlite_writer_transaction_serialization': (
                'BEGIN_IMMEDIATE_WITH_EXPECTED_SEQUENCE_AND_DURABLE_HEAD_HASH_GUARDS'
            ),
            'synchronous': 'FULL',
        }
    if check is EvidenceCheck.RESTART_RECOVERY:
        return {
            'before_public_source_start': 'RECONCILE',
            'duplicate_economic_effects_allowed': False,
            'duplicate_inputs_idempotent': True,
            'funding_settlement_deduplicated': True,
            'new_simulated_orders_before_recovery_allowed': False,
            'projection_rebuilt_from_durable_events': True,
            'strategy_state_rebuilt_from_durable_inputs': True,
            'unclosed_runtime_session_recovery': {
                'incident_summary_reverified_atomically': True,
                'offline_runtime_lease_required': True,
                'replacement_start_before_recovery_allowed': False,
                'required_failure_phase': 'UNCLOSED_RUNTIME_SESSION',
                'resume_targets': ['FLAT', 'REDUCE_ONLY'],
                'stale_market_bypass_scope': 'OFFLINE_UNCLOSED_SESSION_ONLY',
            },
        }
    if check is EvidenceCheck.RECONCILIATION:
        return {
            'authority': 'APPEND_ONLY_PAPER_JOURNAL',
            'exact_replay_required': True,
            'idempotent': True,
            'mismatch_action': 'MANUAL_REVIEW',
            'performed_before_source_start': True,
            'position_invention_allowed': False,
        }
    if check is EvidenceCheck.NO_PRIVATE_EXECUTION_PATH:
        return {
            'exchange_order_routes_present': False,
            'forbidden_client': 'hyperliquid.exchange.Exchange',
            'private_api_present': False,
            'public_market_client_only': True,
            'simulated_orders_only': True,
        }
    if check is EvidenceCheck.NO_WALLET_OR_SIGNER:
        return {
            'api_wallet_present': False,
            'credential_scope': CredentialScope.NONE.value,
            'private_signing_material_present': False,
            'seed_or_mnemonic_present': False,
            'signer_present': False,
        }
    if check is EvidenceCheck.FAIL_CLOSED_ENVIRONMENT_IDENTITY:
        return {
            'accepted_environment': EnvironmentClass.PAPER.value,
            'accepted_purpose': AuthorizationPurpose.PAPER_RUNTIME.value,
            'accepted_source_identity': subject.source_identity,
            'ambiguous_identity_action': 'BLOCK',
            'mainnet_execution_identity_accepted': False,
            'testnet_execution_identity_accepted': False,
        }
    if check is EvidenceCheck.FULL_AUDIT_LOG:
        return {
            'append_only_tables': [
                'paper_alerts',
                'paper_commits',
                'paper_events',
                'paper_inbox',
                'paper_ledger_entries',
                'paper_ledger_transactions',
                'paper_projection_history',
            ],
            'mutable_current_head_tables': [
                'paper_projections',
                'paper_runs',
            ],
            'persisted_hash_fields': {
                'paper_alerts': ['payload_hash'],
                'paper_commits': [
                    'alert_hashes_json',
                    'commit_hash',
                    'event_hashes_json',
                    'ledger_hashes_json',
                    'previous_commit_hash',
                    'projection_hash',
                ],
                'paper_events': ['event_hash', 'payload_hash', 'previous_hash'],
                'paper_inbox': ['commit_hash', 'payload_hash'],
                'paper_ledger_entries': ['entry_hash'],
                'paper_ledger_transactions': ['transaction_hash'],
                'paper_projection_history': ['event_head_hash', 'projection_hash'],
                'paper_projections': ['event_head_hash', 'projection_hash'],
                'paper_runs': [
                    'commit_head_hash',
                    'config_hash',
                    'event_head_hash',
                    'projection_hash',
                ],
            },
            'persistent': True,
            'runtime_session_events': [
                'RUNTIME_SESSION_STARTED',
                'RUNTIME_SESSION_STOPPED',
            ],
            'runtime_session_failure_input': 'PAPER_RUNTIME_FAILURE',
            'secret_material_persisted': False,
        }
    if check is EvidenceCheck.KILL_SWITCH:
        return {
            'kill_latch_state': 'MANUAL_REVIEW',
            'kill_persistent': True,
            'manual_reset_allowed': False,
            'new_simulated_orders_blocked_after_kill': True,
            'new_simulated_orders_blocked_when_paused': True,
            'pause_persistent': True,
            'paused_public_marks_continue_risk_monitoring': True,
            'paused_risk_breach_action': 'CRITICAL_ALERT_PROTECT_NO_EXECUTION',
            'reviewed_resume_fresh_frame_action': 'REDUCE_ONLY_PROTECTIVE_FLATTEN',
            'pause_state': 'PAUSED',
            'simulated_open_orders_cancelled_locally': True,
        }
    return None


def _validate_paper_runtime_source_attestation(
    facts: Mapping[str, object],
    context: EvidenceVerificationContext,
) -> bool:
    expected_facts = _fixed_paper_facts(
        EvidenceCheck.RUNTIME_SOURCE_ATTESTATION,
        context.subject,
    )
    release_code_manifest = facts.get('release_code_manifest')
    runtime_environment_attestation = facts.get(
        'runtime_environment_attestation'
    )
    semantic_facts = dict(facts)
    semantic_facts.pop('release_code_manifest', None)
    semantic_facts.pop('runtime_environment_attestation', None)
    return (
        expected_facts is not None
        and semantic_facts == expected_facts
        and isinstance(release_code_manifest, Mapping)
        and isinstance(runtime_environment_attestation, Mapping)
        and _verify_paper_release_code_attestation(
            release_code_manifest,
            context,
        )
        and _verify_paper_runtime_environment_attestation(
            runtime_environment_attestation,
            context,
        )
    )


def _verify_paper_evidence(
    context: EvidenceVerificationContext,
    *,
    expected_check: EvidenceCheck,
) -> bool:
    if (
        context.check is not expected_check
        or context.environment is not EnvironmentClass.PAPER
        or context.purpose is not AuthorizationPurpose.PAPER_RUNTIME
        or context.environment_identity != EnvironmentClass.PAPER.value
        or context.execution_network is not ExecutionNetwork.NONE
        or context.credential_scope is not CredentialScope.NONE
        or context.order_capability is not OrderCapability.SIMULATED_ONLY
        or not _paper_subject_is_exact(context.subject)
    ):
        return False
    try:
        decoded = json.loads(
            context.artifact_bytes.decode('utf-8'),
            object_pairs_hook=_strict_object,
            parse_constant=_reject_constant,
        )
    except (AuthorizationManifestError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(decoded, Mapping) or frozenset(decoded) != _PAPER_EVIDENCE_KEYS:
        return False
    if context.artifact_bytes != _canonical_json_bytes(dict(decoded)):
        return False
    subject = decoded.get('subject')
    facts = decoded.get('facts')
    runtime_scope = decoded.get('runtime_scope')
    if (
        type(decoded.get('schema_version')) is not int
        or decoded.get('schema_version') != 1
        or decoded.get('environment') != EnvironmentClass.PAPER.value
        or decoded.get('purpose') != AuthorizationPurpose.PAPER_RUNTIME.value
        or decoded.get('check') != expected_check.value
        or not isinstance(subject, Mapping)
        or dict(subject) != context.subject.to_dict()
        or not isinstance(facts, Mapping)
        or not isinstance(runtime_scope, Mapping)
        or dict(runtime_scope) != _PAPER_RUNTIME_SCOPE_FACTS
    ):
        return False
    if expected_check is EvidenceCheck.BOUNDED_POSITION_NOTIONAL:
        return _validate_paper_risk_facts(facts, context)
    if expected_check is EvidenceCheck.RUNTIME_SOURCE_ATTESTATION:
        return _validate_paper_runtime_source_attestation(facts, context)
    expected_facts = _fixed_paper_facts(expected_check, context.subject)
    return expected_facts is not None and dict(facts) == expected_facts


def _make_paper_evidence_verifier(check: EvidenceCheck) -> EvidenceVerifier:
    def verify(context: EvidenceVerificationContext) -> bool:
        return _verify_paper_evidence(context, expected_check=check)

    return verify



@dataclass(frozen=True, slots=True)
class _CompiledEvidenceVerifier:
    verifier_id: str
    version: int
    verify: EvidenceVerifier

    def __post_init__(self) -> None:
        if not isinstance(self.verifier_id, str) or _IDENTITY_RE.fullmatch(self.verifier_id) is None:
            raise ValueError("compiled evidence verifier_id must be a stable identifier")
        if isinstance(self.version, bool) or not isinstance(self.version, int) or self.version <= 0:
            raise ValueError("compiled evidence verifier version must be a positive integer")
        if not callable(self.verify):
            raise TypeError("compiled evidence verifier callback must be callable")


# Deliberately private and immutable. Production callers cannot inject callbacks
# through the readiness API. Exact PAPER and TESTNET registries are compiled below
# after their required-check sets are declared; real-money scopes remain absent.
_COMPILED_EVIDENCE_VERIFIERS: Mapping[
    _VerifierScope, _CompiledEvidenceVerifier
]


def _verifier_scope(
    environment: EnvironmentClass,
    purpose: AuthorizationPurpose,
    check: EvidenceCheck,
) -> _VerifierScope:
    return (environment, purpose, check)


def _verifier_identities_for(
    environment: EnvironmentClass,
    purpose: AuthorizationPurpose,
    required_checks: frozenset[EvidenceCheck],
) -> tuple[EvidenceVerifierIdentity, ...]:
    identities: list[EvidenceVerifierIdentity] = []
    for check in required_checks:
        verifier = _COMPILED_EVIDENCE_VERIFIERS.get(
            _verifier_scope(environment, purpose, check)
        )
        if verifier is not None:
            identities.append(
                EvidenceVerifierIdentity(
                    environment=environment,
                    purpose=purpose,
                    check=check,
                    verifier_id=verifier.verifier_id,
                    version=verifier.version,
                )
            )
    return tuple(sorted(identities, key=lambda item: item.check.value))


def _verifier_set_payload(
    environment: EnvironmentClass,
    purpose: AuthorizationPurpose,
    required_checks: frozenset[EvidenceCheck],
) -> dict[str, object]:
    required_verifiers: list[dict[str, object]] = []
    for check in sorted(required_checks, key=lambda item: item.value):
        verifier = _COMPILED_EVIDENCE_VERIFIERS.get(
            _verifier_scope(environment, purpose, check)
        )
        required_verifiers.append(
            {
                "check": check.value,
                "environment": environment.value,
                "purpose": purpose.value,
                "verifier": (
                    {
                        "verifier_id": verifier.verifier_id,
                        "version": verifier.version,
                    }
                    if verifier is not None
                    else None
                ),
            }
        )
    return {"required_verifiers": required_verifiers, "schema_version": 1}


def _verifier_set_sha256(
    environment: EnvironmentClass,
    purpose: AuthorizationPurpose,
    required_checks: frozenset[EvidenceCheck],
) -> str:
    payload = _verifier_set_payload(environment, purpose, required_checks)
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


@dataclass(frozen=True, slots=True)
class EnvironmentReadinessProfile:
    environment: EnvironmentClass
    purpose: AuthorizationPurpose
    execution_network: ExecutionNetwork
    credential_scope: CredentialScope
    order_capability: OrderCapability
    required_checks: frozenset[EvidenceCheck]

    @property
    def profile_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self.to_dict())).hexdigest()

    @property
    def profile_hash(self) -> str:
        return self.profile_sha256

    def to_dict(self) -> dict[str, object]:
        return {
            "credential_scope": self.credential_scope.value,
            "environment": self.environment.value,
            "execution_network": self.execution_network.value,
            "order_capability": self.order_capability.value,
            "purpose": self.purpose.value,
            "required_checks": sorted(check.value for check in self.required_checks),
            "verifier_set": _verifier_set_payload(
                self.environment,
                self.purpose,
                self.required_checks,
            ),
        }


_RESEARCH_CHECKS = frozenset(
    {
        EvidenceCheck.FROZEN_INPUTS,
        EvidenceCheck.FROZEN_STRATEGY_CONFIG,
        EvidenceCheck.DETERMINISTIC_REPLAY,
        EvidenceCheck.NON_AUTHORIZING_RUNTIME,
    }
)
_PAPER_CHECKS = frozenset(
    {
        EvidenceCheck.PUBLIC_MARKET_SOURCE,
        EvidenceCheck.NORMALIZED_MARKET_EVENT_SCHEMA,
        EvidenceCheck.FROZEN_STRATEGY_CONFIG,
        EvidenceCheck.DETERMINISTIC_ACCOUNTING,
        EvidenceCheck.DETERMINISTIC_REPLAY,
        EvidenceCheck.CONSERVATIVE_COST_MODEL,
        EvidenceCheck.RUNTIME_SOURCE_ATTESTATION,
        EvidenceCheck.CRASH_RECOVERY,
        EvidenceCheck.RESTART_RECOVERY,
        EvidenceCheck.KILL_SWITCH,
        EvidenceCheck.RECONCILIATION,
        EvidenceCheck.NO_PRIVATE_EXECUTION_PATH,
        EvidenceCheck.NO_WALLET_OR_SIGNER,
        EvidenceCheck.BOUNDED_POSITION_NOTIONAL,
        EvidenceCheck.FAIL_CLOSED_ENVIRONMENT_IDENTITY,
        EvidenceCheck.FULL_AUDIT_LOG,
    }
)
_TESTNET_CHECKS = frozenset(
    {
        EvidenceCheck.EXPLICIT_TESTNET_ENDPOINT,
        EvidenceCheck.TESTNET_CONFIG_NAMESPACE,
        EvidenceCheck.NO_MAINNET_FALLBACK,
        EvidenceCheck.ISOLATED_TESTNET_CREDENTIALS,
        EvidenceCheck.CREDENTIAL_SCOPE_VALIDATION,
        EvidenceCheck.DETERMINISTIC_CLIENT_ORDER_IDS,
        EvidenceCheck.ORDER_LIFECYCLE_STATE_MACHINE,
        EvidenceCheck.CANCEL_REPLACE_SEMANTICS,
        EvidenceCheck.RECONCILIATION,
        EvidenceCheck.RESTART_RECOVERY,
        EvidenceCheck.BOUNDED_POSITION_NOTIONAL,
        EvidenceCheck.KILL_SWITCH,
        EvidenceCheck.FAIL_CLOSED_ENVIRONMENT_IDENTITY,
        EvidenceCheck.FULL_AUDIT_LOG,
    }
)


def testnet_evidence_payload(
    check: EvidenceCheck,
    subject: ReadinessSubject,
    risk_limits: Mapping[str, object] | None = None,
    *,
    validation_id: str,
    validation_report_sha256: str,
) -> dict[str, object]:
    '''Build the exact semantic evidence object accepted for TESTNET execution.'''

    if not isinstance(check, EvidenceCheck) or check not in _TESTNET_CHECKS:
        raise ValueError('check is not a TESTNET execution evidence check')
    if not isinstance(subject, ReadinessSubject):
        raise TypeError('subject must be an exact ReadinessSubject')
    if (
        not isinstance(validation_id, str)
        or _SHA256_RE.fullmatch(validation_id) is None
        or not isinstance(validation_report_sha256, str)
        or _SHA256_RE.fullmatch(validation_report_sha256) is None
    ):
        raise ValueError('validation identity must contain exact lowercase SHA-256 values')
    if risk_limits is not None and (
        not isinstance(risk_limits, Mapping)
        or not _validate_testnet_risk_limits(
            risk_limits,
            expected_sha256=subject.risk_limits_hash,
        )
    ):
        raise ValueError('risk_limits do not match the exact bounded TESTNET schema')
    if check is EvidenceCheck.BOUNDED_POSITION_NOTIONAL:
        if risk_limits is None:
            raise ValueError('risk_limits are required for bounded TESTNET evidence')
        facts: dict[str, object] = {
            'enforcement': 'FAIL_CLOSED',
            'limits': dict(risk_limits),
            'limits_sha256': subject.risk_limits_hash,
            'namespace': _TESTNET_CREDENTIAL_NAMESPACE,
        }
    else:
        fixed_facts = _fixed_testnet_facts(check)
        if fixed_facts is None:
            raise ValueError('check has no compiled TESTNET evidence facts')
        facts = fixed_facts
    return {
        'check': check.value,
        'environment': EnvironmentClass.TESTNET.value,
        'facts': facts,
        'purpose': AuthorizationPurpose.TESTNET_EXECUTION.value,
        'schema_version': 1,
        'subject': subject.to_dict(),
        'validation': {
            'report_sha256': validation_report_sha256,
            'validation_id': validation_id,
        },
    }


def paper_evidence_payload(
    check: EvidenceCheck,
    subject: ReadinessSubject,
    risk_limits: Mapping[str, object] | None = None,
    *,
    release_code_manifest_bytes: bytes | None = None,
    runtime_environment_attestation_bytes: bytes | None = None,
) -> dict[str, object]:
    """Build exact, technical-only PAPER/PAPER_RUNTIME semantic evidence."""

    if not isinstance(check, EvidenceCheck) or check not in _PAPER_CHECKS:
        raise ValueError('check is not a PAPER runtime evidence check')
    if not isinstance(subject, ReadinessSubject):
        raise TypeError('subject must be an exact ReadinessSubject')
    if not _paper_subject_is_exact(subject):
        raise ValueError(
            'subject must identify a compiled Phase 12 PAPER/PAPER_RUNTIME candidate'
        )
    if (
        release_code_manifest_bytes is not None
        and check is not EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
    ):
        raise ValueError('release_code_manifest_bytes are only valid for runtime attestation')
    if (
        runtime_environment_attestation_bytes is not None
        and check is not EvidenceCheck.RUNTIME_SOURCE_ATTESTATION
    ):
        raise ValueError(
            'runtime_environment_attestation_bytes are only valid for runtime attestation'
        )
    if risk_limits is not None and (
        not isinstance(risk_limits, Mapping)
        or not _validate_paper_risk_limits(
            risk_limits,
            expected_sha256=subject.risk_limits_hash,
            candidate_id=subject.candidate_id,
        )
    ):
        raise ValueError('risk_limits do not match the bounded PAPER schema and ceilings')
    if check is EvidenceCheck.BOUNDED_POSITION_NOTIONAL:
        if risk_limits is None:
            raise ValueError('risk_limits are required for bounded PAPER evidence')
        facts: dict[str, object] = {
            'enforcement': 'FAIL_CLOSED',
            'limits': dict(risk_limits),
            'limits_sha256': subject.risk_limits_hash,
            'order_capability': OrderCapability.SIMULATED_ONLY.value,
            'protective_risk': _paper_protective_risk_facts(),
        }
    else:
        fixed_facts = _fixed_paper_facts(check, subject)
        if fixed_facts is None:
            raise ValueError('check has no compiled PAPER evidence facts')
        if check is EvidenceCheck.RUNTIME_SOURCE_ATTESTATION:
            manifest_bytes = (
                _build_paper_release_code_manifest_bytes(
                    candidate_id=subject.candidate_id,
                )
                if release_code_manifest_bytes is None
                else release_code_manifest_bytes
            )
            environment_bytes = (
                paper_runtime_environment_attestation_bytes(
                    candidate_id=subject.candidate_id,
                )
                if runtime_environment_attestation_bytes is None
                else runtime_environment_attestation_bytes
            )
            facts = {
                **fixed_facts,
                'release_code_manifest': _paper_release_code_attestation(
                    manifest_bytes,
                    candidate_id=subject.candidate_id,
                ),
                'runtime_environment_attestation': (
                    _paper_runtime_environment_attestation(
                        environment_bytes,
                        candidate_id=subject.candidate_id,
                    )
                ),
            }
        else:
            facts = fixed_facts
    return {
        'check': check.value,
        'environment': EnvironmentClass.PAPER.value,
        'facts': facts,
        'purpose': AuthorizationPurpose.PAPER_RUNTIME.value,
        'runtime_scope': dict(_PAPER_RUNTIME_SCOPE_FACTS),
        'schema_version': 1,
        'subject': subject.to_dict(),
    }



_COMPILED_EVIDENCE_VERIFIERS = MappingProxyType(
    {
        _verifier_scope(
            EnvironmentClass.TESTNET,
            AuthorizationPurpose.TESTNET_EXECUTION,
            check,
        ): _CompiledEvidenceVerifier(
            verifier_id=f'hyperlab:testnet-execution:{check.value.casefold()}',
            version=3 if check is EvidenceCheck.BOUNDED_POSITION_NOTIONAL else 2,
            verify=_make_testnet_evidence_verifier(check),
        )
        for check in _TESTNET_CHECKS
    }
    | {
        _verifier_scope(
            EnvironmentClass.PAPER,
            AuthorizationPurpose.PAPER_RUNTIME,
            check,
        ): _CompiledEvidenceVerifier(
            verifier_id=f'hyperlab:paper-runtime:{check.value.casefold()}',
            version=5,
            verify=_make_paper_evidence_verifier(check),
        )
        for check in _PAPER_CHECKS
    }
)
_REAL_MONEY_COMMON_CHECKS = frozenset(
    {
        EvidenceCheck.GATE_B_EVIDENCE,
        EvidenceCheck.GATE_C_EVIDENCE,
        EvidenceCheck.GATE_D_FORWARD_PAPER_EVIDENCE,
        EvidenceCheck.GATE_E_TESTNET_EVIDENCE,
        EvidenceCheck.HUMAN_APPROVAL,
        EvidenceCheck.SIGNER_ISOLATION,
        EvidenceCheck.SECRET_HANDLING,
        EvidenceCheck.SIGNED_CONFIGURATION,
        EvidenceCheck.DETERMINISTIC_CLIENT_ORDER_IDS,
        EvidenceCheck.ORDER_LIFECYCLE_STATE_MACHINE,
        EvidenceCheck.CANCEL_REPLACE_SEMANTICS,
        EvidenceCheck.RECONCILIATION,
        EvidenceCheck.RESTART_RECOVERY,
        EvidenceCheck.BOUNDED_POSITION_NOTIONAL,
        EvidenceCheck.KILL_SWITCH,
        EvidenceCheck.LOSS_DRAWDOWN_LIMITS,
        EvidenceCheck.FAIL_CLOSED_ENVIRONMENT_IDENTITY,
        EvidenceCheck.FULL_AUDIT_LOG,
    }
)
_MICRO_MAINNET_CHECKS = _REAL_MONEY_COMMON_CHECKS | {EvidenceCheck.MICRO_MAINNET_CAPS}
_MAINNET_CHECKS = _MICRO_MAINNET_CHECKS | {
    EvidenceCheck.GATE_F_MICRO_MAINNET_EVIDENCE,
    EvidenceCheck.DUAL_HUMAN_APPROVAL,
}

ENVIRONMENT_READINESS_PROFILES: Mapping[
    EnvironmentClass, EnvironmentReadinessProfile
] = MappingProxyType(
    {
        EnvironmentClass.RESEARCH_REPLAY: EnvironmentReadinessProfile(
            EnvironmentClass.RESEARCH_REPLAY,
            AuthorizationPurpose.RESEARCH_REPLAY,
            ExecutionNetwork.NONE,
            CredentialScope.NONE,
            OrderCapability.NONE,
            _RESEARCH_CHECKS,
        ),
        EnvironmentClass.PAPER: EnvironmentReadinessProfile(
            EnvironmentClass.PAPER,
            AuthorizationPurpose.PAPER_RUNTIME,
            ExecutionNetwork.NONE,
            CredentialScope.NONE,
            OrderCapability.SIMULATED_ONLY,
            _PAPER_CHECKS,
        ),
        EnvironmentClass.TESTNET: EnvironmentReadinessProfile(
            EnvironmentClass.TESTNET,
            AuthorizationPurpose.TESTNET_EXECUTION,
            ExecutionNetwork.TESTNET,
            CredentialScope.TESTNET,
            OrderCapability.TESTNET_ONLY,
            _TESTNET_CHECKS,
        ),
        EnvironmentClass.MICRO_MAINNET: EnvironmentReadinessProfile(
            EnvironmentClass.MICRO_MAINNET,
            AuthorizationPurpose.MICRO_MAINNET_EXECUTION,
            ExecutionNetwork.MAINNET,
            CredentialScope.MICRO_MAINNET,
            OrderCapability.REAL_MONEY,
            _MICRO_MAINNET_CHECKS,
        ),
        EnvironmentClass.MAINNET: EnvironmentReadinessProfile(
            EnvironmentClass.MAINNET,
            AuthorizationPurpose.MAINNET_EXECUTION,
            ExecutionNetwork.MAINNET,
            CredentialScope.MAINNET,
            OrderCapability.REAL_MONEY,
            _MAINNET_CHECKS,
        ),
    }
)


def profile_for(environment: EnvironmentClass) -> EnvironmentReadinessProfile:
    if not isinstance(environment, EnvironmentClass):
        raise TypeError("environment must be an exact EnvironmentClass")
    return ENVIRONMENT_READINESS_PROFILES[environment]


@dataclass(frozen=True, slots=True)
class EnvironmentReadinessManifest:
    schema_version: int
    environment: EnvironmentClass
    purpose: AuthorizationPurpose
    environment_identity: str
    execution_network: ExecutionNetwork
    credential_scope: CredentialScope
    order_capability: OrderCapability
    subject: ReadinessSubject
    evidence: Mapping[EvidenceCheck, ReadinessArtifactBinding]

    def __post_init__(self) -> None:
        if isinstance(self.schema_version, bool) or not isinstance(self.schema_version, int):
            raise TypeError("authorization manifest schema_version must be an integer")
        for name, expected_type in (
            ("environment", EnvironmentClass),
            ("purpose", AuthorizationPurpose),
            ("execution_network", ExecutionNetwork),
            ("credential_scope", CredentialScope),
            ("order_capability", OrderCapability),
        ):
            if not isinstance(getattr(self, name), expected_type):
                raise TypeError(f"authorization manifest {name} must use its exact enum")
        if not isinstance(self.environment_identity, str):
            raise TypeError("authorization manifest environment_identity must be a string")
        if not isinstance(self.subject, ReadinessSubject):
            raise TypeError("authorization manifest subject must be a ReadinessSubject")
        if not isinstance(self.evidence, Mapping):
            raise TypeError("authorization manifest evidence must be a mapping")
        frozen: dict[EvidenceCheck, ReadinessArtifactBinding] = {}
        for check, binding in self.evidence.items():
            if not isinstance(check, EvidenceCheck):
                raise TypeError("authorization manifest evidence keys must be EvidenceCheck")
            if not isinstance(binding, ReadinessArtifactBinding):
                raise TypeError("authorization manifest evidence values must be artifact bindings")
            frozen[check] = binding
        object.__setattr__(self, "evidence", MappingProxyType(frozen))

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> EnvironmentReadinessManifest:
        raw = _require_mapping(value, location="$")
        expected = frozenset(
            {
                "schema_version",
                "environment",
                "purpose",
                "environment_identity",
                "execution_network",
                "credential_scope",
                "order_capability",
                "subject",
                "evidence",
            }
        )
        _require_exact_keys(raw, expected, location="$")
        schema_version = raw["schema_version"]
        if isinstance(schema_version, bool) or not isinstance(schema_version, int):
            raise _error("INVALID_MANIFEST_SHAPE", "$.schema_version", "must be an integer")
        evidence_raw = _require_mapping(raw["evidence"], location="$.evidence")
        evidence: dict[EvidenceCheck, ReadinessArtifactBinding] = {}
        for key, binding in evidence_raw.items():
            try:
                check = EvidenceCheck(key)
            except ValueError as error:
                raise _error(
                    "UNKNOWN_EVIDENCE_CHECK",
                    f"$.evidence.{key}",
                    "evidence check is not part of the compiled policy",
                ) from error
            evidence[check] = ReadinessArtifactBinding.from_object(
                binding,
                location=f"$.evidence.{key}",
            )
        try:
            return cls(
                schema_version=schema_version,
                environment=_parse_enum(
                    EnvironmentClass, raw["environment"], location="$.environment"
                ),
                purpose=_parse_enum(
                    AuthorizationPurpose, raw["purpose"], location="$.purpose"
                ),
                environment_identity=_require_string(
                    raw["environment_identity"], location="$.environment_identity"
                ),
                execution_network=_parse_enum(
                    ExecutionNetwork,
                    raw["execution_network"],
                    location="$.execution_network",
                ),
                credential_scope=_parse_enum(
                    CredentialScope,
                    raw["credential_scope"],
                    location="$.credential_scope",
                ),
                order_capability=_parse_enum(
                    OrderCapability,
                    raw["order_capability"],
                    location="$.order_capability",
                ),
                subject=ReadinessSubject.from_object(raw["subject"]),
                evidence=evidence,
            )
        except AuthorizationManifestError:
            raise
        except (TypeError, ValueError) as error:
            raise _error("INVALID_MANIFEST", "$", str(error)) from error

    @classmethod
    def from_json_bytes(
        cls,
        payload: bytes,
        *,
        require_canonical: bool = True,
    ) -> EnvironmentReadinessManifest:
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise _error("INVALID_UTF8", "$", "manifest must be UTF-8") from error
        try:
            value = json.loads(
                text,
                object_pairs_hook=_strict_object,
                parse_constant=_reject_constant,
            )
        except AuthorizationManifestError:
            raise
        except json.JSONDecodeError as error:
            raise _error("INVALID_JSON", "$", str(error)) from error
        manifest = cls.from_mapping(_require_mapping(value, location="$"))
        if require_canonical and payload != manifest.canonical_json_bytes():
            raise _error(
                "NON_CANONICAL_MANIFEST",
                "$",
                "bytes differ from canonical sorted compact UTF-8 JSON",
            )
        return manifest

    def to_dict(self) -> dict[str, object]:
        return {
            "credential_scope": self.credential_scope.value,
            "environment": self.environment.value,
            "environment_identity": self.environment_identity,
            "evidence": {
                check.value: binding.to_dict()
                for check, binding in sorted(self.evidence.items(), key=lambda item: item[0].value)
            },
            "execution_network": self.execution_network.value,
            "order_capability": self.order_capability.value,
            "purpose": self.purpose.value,
            "schema_version": self.schema_version,
            "subject": self.subject.to_dict(),
        }

    def canonical_json_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    @property
    def manifest_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json_bytes()).hexdigest()


def _block(
    blockers: list[AuthorizationBlocker],
    code: str,
    location: str,
    message: str,
) -> None:
    blockers.append(AuthorizationBlocker(code, location, message))


def _portable_relative_path(value: str) -> tuple[str, ...] | None:
    if (
        not value
        or value != value.strip()
        or "\\" in value
        or ":" in value
        or "\x00" in value
        or "//" in value
        or value.startswith("/")
        or value.endswith("/")
    ):
        return None
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        return None
    return path.parts


def _has_link_component(root: Path, parts: Sequence[str]) -> bool:
    current = root
    for part in parts:
        current = current / part
        try:
            if current.is_symlink():
                return True
            is_junction = getattr(current, "is_junction", None)
            if callable(is_junction) and bool(is_junction()):
                return True
        except OSError:
            return True
    return False


def _hash_regular_file(path: Path) -> tuple[str | None, str | None]:
    try:
        before = path.stat()
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            while chunk := stream.read(_READ_CHUNK_BYTES):
                digest.update(chunk)
        after = path.stat()
    except OSError as error:
        return None, str(error)
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if before_identity != after_identity:
        return None, "artifact changed while being hashed"
    return digest.hexdigest(), None


def _verify_binding(
    check: EvidenceCheck,
    binding: ReadinessArtifactBinding,
    *,
    root: Path | None,
    blockers: list[AuthorizationBlocker],
    resolved_seen: dict[str, EvidenceCheck],
) -> Path | None:
    location = f"evidence.{check.value}"
    parts = _portable_relative_path(binding.relative_path)
    if parts is None:
        _block(
            blockers,
            "INVALID_ARTIFACT_PATH",
            f"{location}.path",
            "artifact path must be a normalized portable relative path",
        )
    if _SHA256_RE.fullmatch(binding.sha256) is None:
        _block(
            blockers,
            "INVALID_SHA256",
            f"{location}.sha256",
            "artifact digest must be a lowercase SHA-256",
        )
    if root is None or parts is None:
        return None
    if _has_link_component(root, parts):
        _block(
            blockers,
            "SYMLINK_ARTIFACT_REFUSED",
            f"{location}.path",
            "artifact path may not traverse a symlink or junction",
        )
        return None
    lexical = root.joinpath(*parts)
    try:
        resolved = lexical.resolve(strict=True)
        resolved.relative_to(root)
    except FileNotFoundError:
        _block(blockers, "ARTIFACT_MISSING", f"{location}.path", binding.relative_path)
        return None
    except ValueError:
        _block(
            blockers,
            "ARTIFACT_PATH_ESCAPE",
            f"{location}.path",
            "resolved artifact escapes evidence_root",
        )
        return None
    except OSError as error:
        _block(blockers, "ARTIFACT_UNREADABLE", f"{location}.path", str(error))
        return None
    normalized = os.path.normcase(str(resolved))
    previous = resolved_seen.get(normalized)
    if previous is not None:
        _block(
            blockers,
            "DUPLICATE_ARTIFACT_PATH",
            f"{location}.path",
            f"artifact is already bound to {previous.value}",
        )
        return None
    resolved_seen[normalized] = check
    if not resolved.is_file():
        _block(blockers, "ARTIFACT_NOT_REGULAR_FILE", f"{location}.path", str(resolved))
        return None
    actual, hash_error = _hash_regular_file(resolved)
    if hash_error is not None:
        code = "ARTIFACT_CHANGED_DURING_VERIFICATION" if "changed" in hash_error else "ARTIFACT_UNREADABLE"
        _block(blockers, code, f"{location}.path", hash_error)
        return None
    elif actual != binding.sha256:
        _block(
            blockers,
            "ARTIFACT_HASH_MISMATCH",
            f"{location}.sha256",
            f"expected {binding.sha256}, recomputed {actual}",
        )
        return None
    return resolved


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


def _read_semantic_artifact(
    path: Path,
    binding: ReadinessArtifactBinding,
    *,
    location: str,
    blockers: list[AuthorizationBlocker],
) -> tuple[bytes, tuple[int, int, int, int]] | None:
    try:
        path_before = path.stat()
        if path_before.st_size > _MAX_SEMANTIC_ARTIFACT_BYTES:
            _block(
                blockers,
                "SEMANTIC_ARTIFACT_TOO_LARGE",
                f"{location}.path",
                f"semantic evidence exceeds {_MAX_SEMANTIC_ARTIFACT_BYTES} bytes",
            )
            return None
        with path.open("rb") as stream:
            handle_before = os.fstat(stream.fileno())
            payload = stream.read(_MAX_SEMANTIC_ARTIFACT_BYTES + 1)
            handle_after = os.fstat(stream.fileno())
        path_after = path.stat()
    except OSError as error:
        _block(
            blockers,
            "ARTIFACT_CHANGED_DURING_SEMANTIC_VERIFICATION",
            f"{location}.path",
            str(error),
        )
        return None
    identities = (
        _stat_identity(path_before),
        _stat_identity(handle_before),
        _stat_identity(handle_after),
        _stat_identity(path_after),
    )
    if (
        len(payload) > _MAX_SEMANTIC_ARTIFACT_BYTES
        or len(set(identities)) != 1
        or hashlib.sha256(payload).hexdigest() != binding.sha256
    ):
        _block(
            blockers,
            "ARTIFACT_CHANGED_DURING_SEMANTIC_VERIFICATION",
            f"{location}.path",
            "artifact identity or bytes changed before semantic verification",
        )
        return None
    return payload, identities[0]


def _run_semantic_verifier(
    manifest: EnvironmentReadinessManifest,
    check: EvidenceCheck,
    binding: ReadinessArtifactBinding,
    resolved_path: Path | None,
    *,
    evidence_root: Path | None,
    profile_sha256: str,
    blockers: list[AuthorizationBlocker],
) -> None:
    location = f"evidence.{check.value}"
    verifier = _COMPILED_EVIDENCE_VERIFIERS.get(
        _verifier_scope(manifest.environment, manifest.purpose, check)
    )
    if verifier is None:
        _block(
            blockers,
            "NO_COMPILED_EVIDENCE_VERIFIER",
            location,
            "no compiled semantic verifier exists for this exact environment and purpose",
        )
        return
    if resolved_path is None:
        return
    verified_artifact = _read_semantic_artifact(
        resolved_path,
        binding,
        location=location,
        blockers=blockers,
    )
    if verified_artifact is None:
        return
    payload, artifact_identity = verified_artifact
    context = EvidenceVerificationContext(
        check=check,
        environment=manifest.environment,
        purpose=manifest.purpose,
        environment_identity=manifest.environment_identity,
        execution_network=manifest.execution_network,
        credential_scope=manifest.credential_scope,
        order_capability=manifest.order_capability,
        subject=manifest.subject,
        profile_sha256=profile_sha256,
        artifact_bytes=payload,
        evidence_root=evidence_root,
    )
    try:
        passed = verifier.verify(context)
    except Exception as error:
        _block(
            blockers,
            "EVIDENCE_VERIFIER_EXCEPTION",
            location,
            f"{verifier.verifier_id}@{verifier.version} raised {type(error).__name__}",
        )
        passed = False
    try:
        is_junction = getattr(resolved_path, "is_junction", None)
        is_link = resolved_path.is_symlink() or (
            callable(is_junction) and bool(is_junction())
        )
        post_callback_path = resolved_path.resolve(strict=True)
        post_callback_identity = _stat_identity(post_callback_path.stat())
    except OSError as error:
        is_link = True
        post_callback_path = resolved_path
        post_callback_identity = (-1, -1, -1, -1)
        path_error: str | None = str(error)
    else:
        path_error = None
    actual, hash_error = _hash_regular_file(resolved_path)
    if (
        path_error is not None
        or is_link
        or post_callback_path != resolved_path
        or post_callback_identity != artifact_identity
        or hash_error is not None
        or actual != binding.sha256
    ):
        _block(
            blockers,
            "ARTIFACT_CHANGED_DURING_SEMANTIC_VERIFICATION",
            f"{location}.path",
            path_error
            or hash_error
            or "artifact identity, target, or bytes changed while semantic verifier ran",
        )
        return
    if passed is not True:
        _block(
            blockers,
            "EVIDENCE_SEMANTIC_VERIFICATION_FAILED",
            location,
            f"{verifier.verifier_id}@{verifier.version} did not validate the artifact",
        )


@dataclass(frozen=True, slots=True)
class EnvironmentReadinessDecision:
    manifest: EnvironmentReadinessManifest
    profile: EnvironmentReadinessProfile
    manifest_sha256: str
    profile_sha256: str
    verifier_identities: tuple[EvidenceVerifierIdentity, ...]
    verifier_set_sha256: str
    blockers: tuple[AuthorizationBlocker, ...]

    @property
    def ready(self) -> bool:
        return not self.blockers


def verify_environment_readiness(
    manifest: EnvironmentReadinessManifest,
    *,
    evidence_root: Path,
) -> EnvironmentReadinessDecision:
    if not isinstance(manifest, EnvironmentReadinessManifest):
        raise TypeError("readiness verification requires EnvironmentReadinessManifest")
    if not isinstance(evidence_root, Path):
        raise TypeError("evidence_root must be a Path")
    profile = profile_for(manifest.environment)
    profile_sha256 = profile.profile_sha256
    verifier_identities = _verifier_identities_for(
        profile.environment,
        profile.purpose,
        profile.required_checks,
    )
    verifier_set_sha256 = _verifier_set_sha256(
        profile.environment,
        profile.purpose,
        profile.required_checks,
    )
    blockers: list[AuthorizationBlocker] = []
    if manifest.schema_version != AUTHORIZATION_MANIFEST_SCHEMA_VERSION:
        _block(
            blockers,
            "UNSUPPORTED_SCHEMA_VERSION",
            "schema_version",
            f"expected {AUTHORIZATION_MANIFEST_SCHEMA_VERSION}",
        )
    expected_fields = (
        (
            manifest.purpose,
            profile.purpose,
            "PURPOSE_MISMATCH",
            "purpose",
        ),
        (
            manifest.execution_network,
            profile.execution_network,
            "EXECUTION_NETWORK_MISMATCH",
            "execution_network",
        ),
        (
            manifest.credential_scope,
            profile.credential_scope,
            "CREDENTIAL_SCOPE_MISMATCH",
            "credential_scope",
        ),
        (
            manifest.order_capability,
            profile.order_capability,
            "ORDER_CAPABILITY_MISMATCH",
            "order_capability",
        ),
    )
    for actual, expected, code, location in expected_fields:
        if actual is not expected:
            _block(blockers, code, location, f"expected {expected.value}, got {actual.value}")
    if manifest.environment_identity != manifest.environment.value:
        _block(
            blockers,
            "ENVIRONMENT_IDENTITY_MISMATCH",
            "environment_identity",
            f"expected exact identity {manifest.environment.value!r}",
        )
    for check in sorted(profile.required_checks - manifest.evidence.keys(), key=lambda item: item.value):
        _block(
            blockers,
            "MISSING_REQUIRED_EVIDENCE",
            f"evidence.{check.value}",
            "compiled readiness profile requires a byte-bound artifact",
        )
    root: Path | None
    try:
        resolved_root = evidence_root.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        _block(blockers, "EVIDENCE_ROOT_UNAVAILABLE", "evidence_root", str(error))
        root = None
    else:
        if not resolved_root.is_dir():
            _block(
                blockers,
                "EVIDENCE_ROOT_NOT_DIRECTORY",
                "evidence_root",
                str(resolved_root),
            )
            root = None
        else:
            root = resolved_root
    resolved_seen: dict[str, EvidenceCheck] = {}
    verified_paths: dict[EvidenceCheck, Path] = {}
    for check, binding in sorted(manifest.evidence.items(), key=lambda item: item[0].value):
        resolved_path = _verify_binding(
            check,
            binding,
            root=root,
            blockers=blockers,
            resolved_seen=resolved_seen,
        )
        if resolved_path is not None:
            verified_paths[check] = resolved_path
    for check in sorted(profile.required_checks, key=lambda item: item.value):
        verifier = _COMPILED_EVIDENCE_VERIFIERS.get(
            _verifier_scope(manifest.environment, manifest.purpose, check)
        )
        if verifier is None:
            _block(
                blockers,
                "NO_COMPILED_EVIDENCE_VERIFIER",
                f"evidence.{check.value}",
                "no compiled semantic verifier exists for this exact environment and purpose",
            )
            continue
        required_binding = manifest.evidence.get(check)
        if required_binding is None:
            continue
        _run_semantic_verifier(
            manifest,
            check,
            required_binding,
            verified_paths.get(check),
            evidence_root=root,
            profile_sha256=profile_sha256,
            blockers=blockers,
        )
    if (
        verifier_set_sha256
        != _verifier_set_sha256(
            profile.environment,
            profile.purpose,
            profile.required_checks,
        )
        or profile_sha256 != profile.profile_sha256
    ):
        _block(
            blockers,
            "COMPILED_VERIFIER_SET_CHANGED_DURING_VERIFICATION",
            "verifier_set",
            "compiled semantic verifier identities changed during readiness verification",
        )
    if (
        manifest.environment in {EnvironmentClass.MICRO_MAINNET, EnvironmentClass.MAINNET}
        and not REAL_MONEY_EXECUTION_ENABLED_IN_BUILD
    ):
        _block(
            blockers,
            "REAL_MONEY_EXECUTION_DISABLED_IN_BUILD",
            "environment",
            "this build cannot authorize real-money execution",
        )
    return EnvironmentReadinessDecision(
        manifest=manifest,
        profile=profile,
        manifest_sha256=manifest.manifest_sha256,
        profile_sha256=profile_sha256,
        verifier_identities=verifier_identities,
        verifier_set_sha256=verifier_set_sha256,
        blockers=tuple(blockers),
    )


@dataclass(frozen=True, slots=True)
class EnvironmentAuthorizationReceipt:
    schema_version: int
    environment: EnvironmentClass
    purpose: AuthorizationPurpose
    environment_identity: str
    execution_network: ExecutionNetwork
    credential_scope: CredentialScope
    order_capability: OrderCapability
    subject: ReadinessSubject
    manifest_sha256: str
    profile_sha256: str
    verifier_identities: tuple[EvidenceVerifierIdentity, ...]
    verifier_set_sha256: str
    required_checks: tuple[EvidenceCheck, ...]

    def __post_init__(self) -> None:
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version != AUTHORIZATION_RECEIPT_SCHEMA_VERSION
        ):
            raise ValueError("authorization receipt schema_version is not current")
        for name, expected_type in (
            ("environment", EnvironmentClass),
            ("purpose", AuthorizationPurpose),
            ("execution_network", ExecutionNetwork),
            ("credential_scope", CredentialScope),
            ("order_capability", OrderCapability),
        ):
            if not isinstance(getattr(self, name), expected_type):
                raise TypeError(f"authorization receipt {name} must use its exact enum")
        if self.environment_identity != self.environment.value:
            raise ValueError("authorization receipt environment identity is inconsistent")
        if not isinstance(self.subject, ReadinessSubject):
            raise TypeError("authorization receipt subject must be a ReadinessSubject")
        for name in ("manifest_sha256", "profile_sha256", "verifier_set_sha256"):
            if _SHA256_RE.fullmatch(getattr(self, name)) is None:
                raise ValueError(f"authorization receipt {name} must be a lowercase SHA-256")
        if (
            not isinstance(self.verifier_identities, tuple)
            or any(
                not isinstance(identity, EvidenceVerifierIdentity)
                for identity in self.verifier_identities
            )
            or tuple(
                sorted(
                    set(self.verifier_identities),
                    key=lambda identity: identity.check.value,
                )
            )
            != self.verifier_identities
            or any(
                identity.environment is not self.environment
                or identity.purpose is not self.purpose
                for identity in self.verifier_identities
            )
        ):
            raise ValueError(
                "authorization receipt verifier identities must be exact-scope and canonically sorted"
            )
        if (
            not isinstance(self.required_checks, tuple)
            or any(not isinstance(check, EvidenceCheck) for check in self.required_checks)
            or tuple(sorted(set(self.required_checks), key=lambda check: check.value))
            != self.required_checks
        ):
            raise ValueError(
                "authorization receipt required_checks must be unique and canonically sorted"
            )

    @classmethod
    def from_object(
        cls,
        value: object,
        *,
        location: str = '$',
    ) -> EnvironmentAuthorizationReceipt:
        raw = _require_mapping(value, location=location)
        _require_exact_keys(
            raw,
            frozenset(
                {
                    'credential_scope',
                    'environment',
                    'environment_identity',
                    'execution_network',
                    'manifest_sha256',
                    'order_capability',
                    'profile_sha256',
                    'purpose',
                    'required_checks',
                    'schema_version',
                    'subject',
                    'verifier_identities',
                    'verifier_set_sha256',
                }
            ),
            location=location,
        )
        schema_version = raw['schema_version']
        if isinstance(schema_version, bool) or not isinstance(schema_version, int):
            raise _error(
                'INVALID_RECEIPT',
                f'{location}.schema_version',
                'must be an integer',
            )
        raw_checks = raw['required_checks']
        if not isinstance(raw_checks, list):
            raise _error(
                'INVALID_RECEIPT',
                f'{location}.required_checks',
                'must be a JSON array',
            )
        raw_identities = raw['verifier_identities']
        if not isinstance(raw_identities, list):
            raise _error(
                'INVALID_RECEIPT',
                f'{location}.verifier_identities',
                'must be a JSON array',
            )
        try:
            receipt = cls(
                schema_version=schema_version,
                environment=_parse_enum(
                    EnvironmentClass,
                    raw['environment'],
                    location=f'{location}.environment',
                ),
                purpose=_parse_enum(
                    AuthorizationPurpose,
                    raw['purpose'],
                    location=f'{location}.purpose',
                ),
                environment_identity=_require_string(
                    raw['environment_identity'],
                    location=f'{location}.environment_identity',
                ),
                execution_network=_parse_enum(
                    ExecutionNetwork,
                    raw['execution_network'],
                    location=f'{location}.execution_network',
                ),
                credential_scope=_parse_enum(
                    CredentialScope,
                    raw['credential_scope'],
                    location=f'{location}.credential_scope',
                ),
                order_capability=_parse_enum(
                    OrderCapability,
                    raw['order_capability'],
                    location=f'{location}.order_capability',
                ),
                subject=ReadinessSubject.from_object(
                    raw['subject'],
                    location=f'{location}.subject',
                ),
                manifest_sha256=_require_sha256(
                    raw['manifest_sha256'],
                    location=f'{location}.manifest_sha256',
                ),
                profile_sha256=_require_sha256(
                    raw['profile_sha256'],
                    location=f'{location}.profile_sha256',
                ),
                verifier_identities=tuple(
                    EvidenceVerifierIdentity.from_object(
                        item,
                        location=f'{location}.verifier_identities[{index}]',
                    )
                    for index, item in enumerate(raw_identities)
                ),
                verifier_set_sha256=_require_sha256(
                    raw['verifier_set_sha256'],
                    location=f'{location}.verifier_set_sha256',
                ),
                required_checks=tuple(
                    _parse_enum(
                        EvidenceCheck,
                        item,
                        location=f'{location}.required_checks[{index}]',
                    )
                    for index, item in enumerate(raw_checks)
                ),
            )
        except AuthorizationManifestError:
            raise
        except (TypeError, ValueError) as error:
            raise _error('INVALID_RECEIPT', location, str(error)) from error

        profile = profile_for(receipt.environment)
        expected_checks = tuple(
            sorted(profile.required_checks, key=lambda check: check.value)
        )
        if (
            receipt.purpose is not profile.purpose
            or receipt.execution_network is not profile.execution_network
            or receipt.credential_scope is not profile.credential_scope
            or receipt.order_capability is not profile.order_capability
            or receipt.required_checks != expected_checks
            or tuple(identity.check for identity in receipt.verifier_identities)
            != expected_checks
        ):
            raise _error(
                'INVALID_RECEIPT_SCOPE',
                location,
                'receipt fields do not match one exact compiled environment profile',
            )
        return receipt

    @classmethod
    def from_json_bytes(
        cls,
        payload: bytes,
        *,
        require_canonical: bool = True,
    ) -> EnvironmentAuthorizationReceipt:
        if not isinstance(payload, bytes):
            raise TypeError('authorization receipt payload must be bytes')
        try:
            text = payload.decode('utf-8')
        except UnicodeDecodeError as error:
            raise _error('INVALID_UTF8', '$', 'receipt must be UTF-8') from error
        try:
            value = json.loads(
                text,
                object_pairs_hook=_strict_object,
                parse_constant=_reject_constant,
            )
        except AuthorizationManifestError:
            raise
        except json.JSONDecodeError as error:
            raise _error('INVALID_JSON', '$', str(error)) from error
        receipt = cls.from_object(value)
        if require_canonical and payload != receipt.canonical_json_bytes():
            raise _error(
                'NON_CANONICAL_RECEIPT',
                '$',
                'bytes differ from canonical sorted compact UTF-8 JSON',
            )
        return receipt

    @property
    def authorizes_real_money(self) -> bool:
        return (
            REAL_MONEY_EXECUTION_ENABLED_IN_BUILD
            and self.environment
            in {EnvironmentClass.MICRO_MAINNET, EnvironmentClass.MAINNET}
            and self.order_capability is OrderCapability.REAL_MONEY
        )

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json_bytes()).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "credential_scope": self.credential_scope.value,
            "environment": self.environment.value,
            "environment_identity": self.environment_identity,
            "execution_network": self.execution_network.value,
            "manifest_sha256": self.manifest_sha256,
            "order_capability": self.order_capability.value,
            "profile_sha256": self.profile_sha256,
            "purpose": self.purpose.value,
            "required_checks": [check.value for check in self.required_checks],
            "schema_version": self.schema_version,
            "subject": self.subject.to_dict(),
            "verifier_identities": [
                identity.to_dict() for identity in self.verifier_identities
            ],
            "verifier_set_sha256": self.verifier_set_sha256,
        }
    def canonical_json_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


EnvironmentReadinessReceipt = EnvironmentAuthorizationReceipt


def issue_environment_receipt(
    decision: EnvironmentReadinessDecision,
) -> EnvironmentAuthorizationReceipt:
    if not isinstance(decision, EnvironmentReadinessDecision):
        raise TypeError("receipt issuance requires EnvironmentReadinessDecision")
    if not decision.ready:
        raise ValueError("cannot issue an environment receipt for a blocked readiness decision")
    manifest = decision.manifest
    profile = decision.profile
    current_verifier_identities = _verifier_identities_for(
        profile.environment,
        profile.purpose,
        profile.required_checks,
    )
    current_verifier_set_sha256 = _verifier_set_sha256(
        profile.environment,
        profile.purpose,
        profile.required_checks,
    )
    if len(current_verifier_identities) != len(profile.required_checks):
        raise ValueError(
            "cannot issue an environment receipt with an incomplete compiled "
            "evidence verifier set"
        )
    if (
        decision.profile_sha256 != profile.profile_sha256
        or decision.verifier_identities != current_verifier_identities
        or decision.verifier_set_sha256 != current_verifier_set_sha256
    ):
        raise ValueError("compiled evidence verifier set changed after readiness verification")
    return EnvironmentAuthorizationReceipt(
        schema_version=AUTHORIZATION_RECEIPT_SCHEMA_VERSION,
        environment=manifest.environment,
        purpose=manifest.purpose,
        environment_identity=manifest.environment_identity,
        execution_network=manifest.execution_network,
        credential_scope=manifest.credential_scope,
        order_capability=manifest.order_capability,
        subject=manifest.subject,
        manifest_sha256=decision.manifest_sha256,
        profile_sha256=decision.profile_sha256,
        verifier_identities=decision.verifier_identities,
        verifier_set_sha256=decision.verifier_set_sha256,
        required_checks=tuple(sorted(profile.required_checks, key=lambda check: check.value)),
    )


def receipt_scope_blockers(
    receipt: EnvironmentAuthorizationReceipt,
    *,
    environment: EnvironmentClass,
    purpose: AuthorizationPurpose,
    config_hash: str,
) -> tuple[AuthorizationBlocker, ...]:
    blockers: list[AuthorizationBlocker] = []
    if not isinstance(receipt, EnvironmentAuthorizationReceipt):
        return (
            AuthorizationBlocker(
                "INVALID_RECEIPT",
                "receipt",
                "authorization requires an EnvironmentAuthorizationReceipt",
            ),
        )
    if not isinstance(environment, EnvironmentClass):
        _block(blockers, "INVALID_REQUEST_ENVIRONMENT", "environment", "exact enum required")
        return tuple(blockers)
    if not isinstance(purpose, AuthorizationPurpose):
        _block(blockers, "INVALID_REQUEST_PURPOSE", "purpose", "exact enum required")
        return tuple(blockers)
    profile = profile_for(environment)
    if receipt.schema_version != AUTHORIZATION_RECEIPT_SCHEMA_VERSION:
        _block(
            blockers,
            "RECEIPT_SCHEMA_MISMATCH",
            "schema_version",
            "receipt schema is not current",
        )
    if receipt.environment is not environment:
        _block(
            blockers,
            "ENVIRONMENT_SCOPE_MISMATCH",
            "environment",
            f"receipt is bound to {receipt.environment.value}",
        )
    if receipt.purpose is not purpose:
        _block(
            blockers,
            "PURPOSE_SCOPE_MISMATCH",
            "purpose",
            f"receipt is bound to {receipt.purpose.value}",
        )
    if receipt.subject.config_hash != config_hash:
        _block(
            blockers,
            "CONFIG_SCOPE_MISMATCH",
            "subject.config_hash",
            "receipt is bound to a different frozen configuration",
        )
    if receipt.environment_identity != receipt.environment.value:
        _block(
            blockers,
            "RECEIPT_ENVIRONMENT_IDENTITY_MISMATCH",
            "environment_identity",
            "receipt environment identity is internally inconsistent",
        )
    compiled_fields = (
        (receipt.purpose, profile.purpose),
        (receipt.execution_network, profile.execution_network),
        (receipt.credential_scope, profile.credential_scope),
        (receipt.order_capability, profile.order_capability),
    )
    if receipt.environment is environment and any(actual is not expected for actual, expected in compiled_fields):
        _block(
            blockers,
            "PROFILE_SCOPE_MISMATCH",
            "profile",
            "receipt capabilities differ from the current compiled environment profile",
        )
    expected_checks = tuple(sorted(profile.required_checks, key=lambda check: check.value))
    expected_verifier_identities = _verifier_identities_for(
        profile.environment,
        profile.purpose,
        profile.required_checks,
    )
    expected_verifier_set_sha256 = _verifier_set_sha256(
        profile.environment,
        profile.purpose,
        profile.required_checks,
    )
    if len(expected_verifier_identities) != len(expected_checks):
        _block(
            blockers,
            "NO_COMPILED_EVIDENCE_VERIFIER",
            "verifier_set_sha256",
            "the current exact-scope compiled semantic verifier set is incomplete",
        )
    if receipt.environment is environment and (
        receipt.profile_sha256 != profile.profile_sha256
        or receipt.required_checks != expected_checks
    ):
        _block(
            blockers,
            "PROFILE_SCOPE_MISMATCH",
            "profile_sha256",
            "receipt is not bound to the current compiled requirement profile",
        )
    if receipt.environment is environment and (
        receipt.verifier_identities != expected_verifier_identities
        or receipt.verifier_set_sha256 != expected_verifier_set_sha256
    ):
        _block(
            blockers,
            "VERIFIER_SET_SCOPE_MISMATCH",
            "verifier_set_sha256",
            "receipt is not bound to the current compiled semantic verifier set",
        )
    if (
        environment in {EnvironmentClass.MICRO_MAINNET, EnvironmentClass.MAINNET}
        and not REAL_MONEY_EXECUTION_ENABLED_IN_BUILD
    ):
        _block(
            blockers,
            "REAL_MONEY_EXECUTION_DISABLED_IN_BUILD",
            "environment",
            "this build cannot consume real-money authorization receipts",
        )
    return tuple(blockers)


def compiled_evidence_verifier_status(
    environment: EnvironmentClass,
) -> dict[str, object]:
    """Expose compiled verifier identities without permitting runtime injection."""

    profile = profile_for(environment)
    identities = _verifier_identities_for(
        profile.environment,
        profile.purpose,
        profile.required_checks,
    )
    return {
        "complete": len(identities) == len(profile.required_checks),
        "environment": profile.environment.value,
        "profile_sha256": profile.profile_sha256,
        "purpose": profile.purpose.value,
        "required_check_count": len(profile.required_checks),
        "verifier_identities": [identity.to_dict() for identity in identities],
        "verifier_set_sha256": _verifier_set_sha256(
            profile.environment,
            profile.purpose,
            profile.required_checks,
        ),
    }


__all__ = [
    "AUTHORIZATION_MANIFEST_SCHEMA_VERSION",
    "AUTHORIZATION_RECEIPT_SCHEMA_VERSION",
    "ENVIRONMENT_READINESS_PROFILES",
    "REAL_MONEY_EXECUTION_ENABLED_IN_BUILD",
    "AuthorizationBlocker",
    "AuthorizationManifestError",
    "AuthorizationPurpose",
    "CredentialScope",
    "EnvironmentAuthorizationReceipt",
    "EnvironmentClass",
    "EnvironmentReadinessDecision",
    "EnvironmentReadinessManifest",
    "EnvironmentReadinessProfile",
    "EnvironmentReadinessReceipt",
    "EvidenceCheck",
    "EvidenceVerificationContext",
    "EvidenceVerifierIdentity",
    "ExecutionNetwork",
    "OrderCapability",
    "ReadinessArtifactBinding",
    "ReadinessSubject",
    "compiled_evidence_verifier_status",
    "current_paper_release_code_sha256",
    "current_paper_runtime_environment_sha256",
    "issue_environment_receipt",
    "paper_evidence_payload",
    "paper_release_identity_candidate",
    "paper_runtime_environment_attestation_bytes",
    "profile_for",
    "receipt_scope_blockers",
    "testnet_evidence_payload",
    "verify_environment_readiness",
]
