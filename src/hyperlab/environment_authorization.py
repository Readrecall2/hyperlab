from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
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


EvidenceVerifier = Callable[[EvidenceVerificationContext], bool]
_VerifierScope = tuple[EnvironmentClass, AuthorizationPurpose, EvidenceCheck]


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


# Deliberately private, immutable, and empty in this build. Production callers
# cannot inject callbacks through the readiness API. A reviewed build must compile
# concrete scope-specific semantic verifiers here before a receipt can be issued.
_COMPILED_EVIDENCE_VERIFIERS: Mapping[
    _VerifierScope, _CompiledEvidenceVerifier
] = MappingProxyType({})


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
        return hashlib.sha256(_canonical_json_bytes(self.to_dict())).hexdigest()

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
    "issue_environment_receipt",
    "profile_for",
    "receipt_scope_blockers",
    "verify_environment_readiness",
]
