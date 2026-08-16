from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import cast

ADMISSION_MANIFEST_SCHEMA_VERSION = 1
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_IDENTITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@+\-]{0,255}\Z")
_READ_CHUNK_BYTES = 1024 * 1024


class AdmissionManifestError(ValueError):
    """A manifest cannot be represented without weakening its strict schema."""

    def __init__(self, code: str, location: str, message: str) -> None:
        super().__init__(f"{location}: {message}")
        self.code = code
        self.location = location
        self.message = message


def _format_error(code: str, location: str, message: str) -> AdmissionManifestError:
    return AdmissionManifestError(code, location, message)


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _format_error("DUPLICATE_JSON_KEY", "$", f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    raise _format_error("NON_FINITE_JSON_NUMBER", "$", f"non-finite JSON number {value!r}")


def _require_object(value: object, *, location: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise _format_error("INVALID_MANIFEST_SHAPE", location, "must be a JSON object")
    return value


def _require_exact_keys(
    value: Mapping[str, object],
    expected: frozenset[str],
    *,
    location: str,
) -> None:
    actual = frozenset(value)
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    if missing or unexpected:
        details: list[str] = []
        if missing:
            details.append(f"missing keys {missing}")
        if unexpected:
            details.append(f"unexpected keys {unexpected}")
        raise _format_error("INVALID_MANIFEST_SHAPE", location, "; ".join(details))


def _require_string(value: object, *, location: str) -> str:
    if not isinstance(value, str):
        raise _format_error("INVALID_MANIFEST_SHAPE", location, "must be a string")
    return value


def _freeze_json(value: object, *, location: str) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _format_error("NON_FINITE_JSON_NUMBER", location, "must be finite")
        return value
    if isinstance(value, list):
        return tuple(
            _freeze_json(item, location=f"{location}[{index}]")
            for index, item in enumerate(value)
        )
    if isinstance(value, dict):
        frozen: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise _format_error("INVALID_MANIFEST_SHAPE", location, "object keys must be strings")
            frozen[key] = _freeze_json(item, location=f"{location}.{key}")
        return MappingProxyType(frozen)
    raise _format_error(
        "INVALID_MANIFEST_SHAPE",
        location,
        f"unsupported JSON value type {type(value).__name__}",
    )


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        _thaw_json(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class ArtifactBinding:
    relative_path: str
    sha256: str

    @classmethod
    def from_object(cls, value: object, *, location: str) -> ArtifactBinding:
        raw = _require_object(value, location=location)
        _require_exact_keys(raw, frozenset({"path", "sha256"}), location=location)
        return cls(
            relative_path=_require_string(raw["path"], location=f"{location}.path"),
            sha256=_require_string(raw["sha256"], location=f"{location}.sha256"),
        )

    def to_dict(self) -> dict[str, str]:
        return {"path": self.relative_path, "sha256": self.sha256}


def _artifact_sequence(value: object, *, location: str) -> tuple[ArtifactBinding, ...]:
    if not isinstance(value, list):
        raise _format_error("INVALID_MANIFEST_SHAPE", location, "must be an array")
    return tuple(
        ArtifactBinding.from_object(item, location=f"{location}[{index}]")
        for index, item in enumerate(value)
    )


@dataclass(frozen=True, slots=True)
class EvidenceArtifacts:
    gate_b_report: ArtifactBinding
    gate_c_report: ArtifactBinding
    data: tuple[ArtifactBinding, ...]
    calibration: tuple[ArtifactBinding, ...]
    strategy: tuple[ArtifactBinding, ...]

    @classmethod
    def from_object(cls, value: object, *, location: str = "$.evidence") -> EvidenceArtifacts:
        raw = _require_object(value, location=location)
        _require_exact_keys(
            raw,
            frozenset({"gate_b_report", "gate_c_report", "data", "calibration", "strategy"}),
            location=location,
        )
        return cls(
            gate_b_report=ArtifactBinding.from_object(
                raw["gate_b_report"], location=f"{location}.gate_b_report"
            ),
            gate_c_report=ArtifactBinding.from_object(
                raw["gate_c_report"], location=f"{location}.gate_c_report"
            ),
            data=_artifact_sequence(raw["data"], location=f"{location}.data"),
            calibration=_artifact_sequence(
                raw["calibration"], location=f"{location}.calibration"
            ),
            strategy=_artifact_sequence(raw["strategy"], location=f"{location}.strategy"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "calibration": [item.to_dict() for item in self.calibration],
            "data": [item.to_dict() for item in self.data],
            "gate_b_report": self.gate_b_report.to_dict(),
            "gate_c_report": self.gate_c_report.to_dict(),
            "strategy": [item.to_dict() for item in self.strategy],
        }


@dataclass(frozen=True, slots=True)
class FrozenResearchArtifacts:
    split_plan: ArtifactBinding
    variant_registry: ArtifactBinding
    final_reveal: ArtifactBinding

    @classmethod
    def from_object(
        cls,
        value: object,
        *,
        location: str = "$.research",
    ) -> FrozenResearchArtifacts:
        raw = _require_object(value, location=location)
        _require_exact_keys(
            raw,
            frozenset({"split_plan", "variant_registry", "final_reveal"}),
            location=location,
        )
        return cls(
            split_plan=ArtifactBinding.from_object(
                raw["split_plan"], location=f"{location}.split_plan"
            ),
            variant_registry=ArtifactBinding.from_object(
                raw["variant_registry"], location=f"{location}.variant_registry"
            ),
            final_reveal=ArtifactBinding.from_object(
                raw["final_reveal"], location=f"{location}.final_reveal"
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "final_reveal": self.final_reveal.to_dict(),
            "split_plan": self.split_plan.to_dict(),
            "variant_registry": self.variant_registry.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class ArtifactIdentity:
    identity: str
    artifact: ArtifactBinding

    @classmethod
    def from_object(cls, value: object, *, location: str) -> ArtifactIdentity:
        raw = _require_object(value, location=location)
        _require_exact_keys(raw, frozenset({"identity", "artifact"}), location=location)
        return cls(
            identity=_require_string(raw["identity"], location=f"{location}.identity"),
            artifact=ArtifactBinding.from_object(raw["artifact"], location=f"{location}.artifact"),
        )

    def to_dict(self) -> dict[str, object]:
        return {"artifact": self.artifact.to_dict(), "identity": self.identity}


@dataclass(frozen=True, slots=True)
class AdmissionIdentities:
    market_source: ArtifactIdentity
    cost_schedule: ArtifactIdentity
    frozen_config: ArtifactIdentity

    @classmethod
    def from_object(
        cls,
        value: object,
        *,
        location: str = "$.identities",
    ) -> AdmissionIdentities:
        raw = _require_object(value, location=location)
        _require_exact_keys(
            raw,
            frozenset({"market_source", "cost_schedule", "frozen_config"}),
            location=location,
        )
        return cls(
            market_source=ArtifactIdentity.from_object(
                raw["market_source"], location=f"{location}.market_source"
            ),
            cost_schedule=ArtifactIdentity.from_object(
                raw["cost_schedule"], location=f"{location}.cost_schedule"
            ),
            frozen_config=ArtifactIdentity.from_object(
                raw["frozen_config"], location=f"{location}.frozen_config"
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "cost_schedule": self.cost_schedule.to_dict(),
            "frozen_config": self.frozen_config.to_dict(),
            "market_source": self.market_source.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class AdmissionManifest:
    schema_version: int
    candidate_id: str
    gate_b_thresholds: Mapping[str, object]
    gate_c_thresholds: Mapping[str, object]
    evidence: EvidenceArtifacts
    research: FrozenResearchArtifacts
    identities: AdmissionIdentities

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> AdmissionManifest:
        raw = dict(value)
        _require_exact_keys(
            raw,
            frozenset(
                {
                    "schema_version",
                    "candidate_id",
                    "gate_thresholds",
                    "evidence",
                    "research",
                    "identities",
                }
            ),
            location="$",
        )
        schema_version = raw["schema_version"]
        if isinstance(schema_version, bool) or not isinstance(schema_version, int):
            raise _format_error("INVALID_MANIFEST_SHAPE", "$.schema_version", "must be an integer")
        thresholds = _require_object(raw["gate_thresholds"], location="$.gate_thresholds")
        _require_exact_keys(
            thresholds, frozenset({"gate_b", "gate_c"}), location="$.gate_thresholds"
        )
        gate_b = _freeze_json(
            _require_object(thresholds["gate_b"], location="$.gate_thresholds.gate_b"),
            location="$.gate_thresholds.gate_b",
        )
        gate_c = _freeze_json(
            _require_object(thresholds["gate_c"], location="$.gate_thresholds.gate_c"),
            location="$.gate_thresholds.gate_c",
        )
        return cls(
            schema_version=schema_version,
            candidate_id=_require_string(raw["candidate_id"], location="$.candidate_id"),
            gate_b_thresholds=cast(Mapping[str, object], gate_b),
            gate_c_thresholds=cast(Mapping[str, object], gate_c),
            evidence=EvidenceArtifacts.from_object(raw["evidence"]),
            research=FrozenResearchArtifacts.from_object(raw["research"]),
            identities=AdmissionIdentities.from_object(raw["identities"]),
        )

    @classmethod
    def from_json_bytes(
        cls,
        payload: bytes,
        *,
        require_canonical: bool = True,
    ) -> AdmissionManifest:
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise _format_error("INVALID_UTF8", "$", "manifest must be UTF-8") from error
        try:
            raw = json.loads(
                text,
                object_pairs_hook=_strict_object,
                parse_constant=_reject_constant,
            )
        except AdmissionManifestError:
            raise
        except json.JSONDecodeError as error:
            raise _format_error("INVALID_JSON", "$", str(error)) from error
        manifest = cls.from_mapping(_require_object(raw, location="$"))
        if require_canonical and payload != manifest.canonical_json_bytes():
            raise _format_error(
                "NON_CANONICAL_MANIFEST",
                "$",
                "bytes differ from sorted, compact UTF-8 canonical JSON",
            )
        return manifest

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "evidence": self.evidence.to_dict(),
            "gate_thresholds": {
                "gate_b": _thaw_json(self.gate_b_thresholds),
                "gate_c": _thaw_json(self.gate_c_thresholds),
            },
            "identities": self.identities.to_dict(),
            "research": self.research.to_dict(),
            "schema_version": self.schema_version,
        }

    def canonical_json_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    @property
    def manifest_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json_bytes()).hexdigest()

    def iter_artifacts(self) -> Iterable[tuple[str, ArtifactBinding]]:
        yield "evidence.gate_b_report", self.evidence.gate_b_report
        yield "evidence.gate_c_report", self.evidence.gate_c_report
        for name in ("data", "calibration", "strategy"):
            values = cast(tuple[ArtifactBinding, ...], getattr(self.evidence, name))
            for index, artifact in enumerate(values):
                yield f"evidence.{name}[{index}]", artifact
        yield "research.split_plan", self.research.split_plan
        yield "research.variant_registry", self.research.variant_registry
        yield "research.final_reveal", self.research.final_reveal
        yield "identities.market_source.artifact", self.identities.market_source.artifact
        yield "identities.cost_schedule.artifact", self.identities.cost_schedule.artifact
        yield "identities.frozen_config.artifact", self.identities.frozen_config.artifact


def load_admission_manifest(
    path: Path,
    *,
    require_canonical: bool = True,
) -> AdmissionManifest:
    """Read a strict manifest without modifying it or any evidence artifact."""

    try:
        payload = path.read_bytes()
    except OSError as error:
        raise _format_error("MANIFEST_UNREADABLE", "$", str(error)) from error
    return AdmissionManifest.from_json_bytes(payload, require_canonical=require_canonical)


@dataclass(frozen=True, slots=True)
class CandidateAdmissionPolicy:
    candidate_id: str
    gate_b_thresholds: Mapping[str, object]
    gate_c_thresholds: Mapping[str, object]

    def thresholds_dict(self) -> dict[str, object]:
        return {
            "gate_b": _thaw_json(self.gate_b_thresholds),
            "gate_c": _thaw_json(self.gate_c_thresholds),
        }


def _policy(
    candidate_id: str,
    *,
    gate_b: dict[str, object],
    gate_c: dict[str, object],
) -> CandidateAdmissionPolicy:
    return CandidateAdmissionPolicy(
        candidate_id=candidate_id,
        gate_b_thresholds=cast(
            Mapping[str, object], _freeze_json(gate_b, location=f"policy.{candidate_id}.gate_b")
        ),
        gate_c_thresholds=cast(
            Mapping[str, object], _freeze_json(gate_c, location=f"policy.{candidate_id}.gate_c")
        ),
    )


CANONICAL_CANDIDATE_POLICIES: Mapping[str, CandidateAdmissionPolicy] = MappingProxyType(
    {
        "cash_and_carry": _policy(
            "cash_and_carry",
            gate_b={"minimum_history_hours": 720},
            gate_c={
                "maximum_stressed_drawdown": 0.1,
                "minimum_stressed_excess_return": 0.0,
                "require_complete_close": True,
                "required_stress_scenarios": [
                    "funding_inversion",
                    "costs_x2",
                    "maker_fill_degraded",
                    "latency_degraded",
                    "remove_best_5pct",
                ],
            },
        ),
        "funding_basket": _policy(
            "funding_basket",
            gate_b={
                "minimum_assets": 6,
                "minimum_history_hours": 2160,
                "require_delisted_market": True,
            },
            gate_c={
                "require_leave_one_out": True,
                "required_stress_scenarios": [
                    "costs_x2",
                    "maker_fill_degraded",
                    "latency_degraded",
                    "broken_correlation",
                    "simultaneous_short_squeeze",
                    "remove_best_5pct",
                ],
            },
        ),
        "cross_exchange_funding": _policy(
            "cross_exchange_funding",
            gate_b={"minimum_history_hours": 720, "required_venue_count": 2},
            gate_c={
                "maximum_liquidation_count": 0,
                "maximum_uncovered_hours": 0.0,
                "required_outage_hours": [1, 6, 24],
            },
        ),
        "pairs_mean_reversion_phase08": _policy(
            "pairs_mean_reversion_phase08",
            gate_b={
                "minimum_assets": 6,
                "minimum_history_hours": 4320,
                "require_delisted_market": True,
            },
            gate_c={
                "correlation_break_strength": 2.0,
                "minimum_selected_pairs": 2,
                "minimum_stressed_return": 0.0,
                "train_fraction": 0.6,
                "validation_fraction": 0.2,
                "required_stress_scenarios": ["remove_best_pair", "correlation_break"],
            },
        ),
        "momentum_regime": _policy(
            "momentum_regime",
            gate_b={
                "minimum_assets": 6,
                "minimum_history_hours": 8760,
                "require_delisted_market": True,
            },
            gate_c={
                "maximum_bull_profit_fraction": 0.8,
                "maximum_gross_leverage": 1.0,
                "minimum_non_bull_pnl": 0.0,
                "required_regimes": ["trend_up", "trend_down", "chaos"],
                "require_liquidation_cooldown_exercised": True,
                "require_volatility_stop_exercised": True,
                "train_fraction": 0.6,
                "validation_fraction": 0.2,
            },
        ),
        "market_making_l2": _policy(
            "market_making_l2",
            gate_b={
                "minimum_event_count": 10000,
                "minimum_venue_count": 2,
                "require_no_declared_gaps": True,
                "require_resynchronization_observable": True,
                "require_target_sequences_observable": True,
            },
            gate_c={
                "markout_horizons_ms": [100, 1000, 5000],
                "require_chronological_out_of_sample_calibration": True,
                "require_reconciled_quotes": True,
            },
        ),
    }
)


# Candidate policy identifiers are deliberately distinct from executable strategy
# names. Keep the relationship explicit so a strategy cannot borrow another
# candidate's shorter Gate B/C policy. Phase 08 is the current non-identity case.
CANONICAL_CANDIDATE_STRATEGY_NAMES: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "cash_and_carry": frozenset({"cash_and_carry"}),
        "funding_basket": frozenset({"funding_basket"}),
        "cross_exchange_funding": frozenset({"cross_exchange_funding"}),
        "pairs_mean_reversion_phase08": frozenset({"pairs_mean_reversion"}),
        "momentum_regime": frozenset({"momentum_regime"}),
        "market_making_l2": frozenset({"market_making_l2"}),
    }
)


def canonical_thresholds_for(candidate_id: str) -> dict[str, object]:
    """Return a mutable JSON-ready copy of the immutable admission policy."""

    try:
        return CANONICAL_CANDIDATE_POLICIES[candidate_id].thresholds_dict()
    except KeyError as error:
        raise ValueError(f"unknown paper candidate {candidate_id!r}") from error


@dataclass(frozen=True, slots=True)
class AdmissionBlocker:
    code: str
    location: str
    message: str


@dataclass(frozen=True, slots=True)
class AdmissionVerification:
    candidate_id: str | None
    manifest_sha256: str | None
    blockers: tuple[AdmissionBlocker, ...]

    @property
    def evidence_bound(self) -> bool:
        """True only for byte integrity; it is deliberately not an admission decision."""

        return not self.blockers


@dataclass(frozen=True, slots=True)
class CandidateSemanticVerification:
    """Diagnostic output shape for a future candidate-specific evaluator.

    The booleans describe freshly recomputed gate decisions; they are not fields
    copied from the bound reports. Byte verification remains a separate required
    step, and the core validator binds this receipt back to those verified bytes
    and the frozen runtime identities. This receipt is deliberately non-authorizing:
    core does not yet derive Gate B/C from measured candidate-specific metrics.
    """

    candidate_id: str
    strategy_name: str
    gate_b_recomputed_pass: bool
    gate_c_recomputed_pass: bool
    gate_b_report_sha256: str
    gate_c_report_sha256: str
    frozen_config_hash: str
    data_hash: str
    blockers: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for label, value in (
            ("candidate_id", self.candidate_id),
            ("strategy_name", self.strategy_name),
        ):
            if not isinstance(value, str) or _IDENTITY_RE.fullmatch(value) is None:
                raise ValueError(f"semantic receipt {label} must be a stable identifier")
        for label, value in (
            ("gate_b_report_sha256", self.gate_b_report_sha256),
            ("gate_c_report_sha256", self.gate_c_report_sha256),
            ("frozen_config_hash", self.frozen_config_hash),
            ("data_hash", self.data_hash),
        ):
            if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                raise ValueError(f"semantic receipt {label} must be a lowercase SHA-256")
        for label, boolean_value in (
            ("gate_b_recomputed_pass", self.gate_b_recomputed_pass),
            ("gate_c_recomputed_pass", self.gate_c_recomputed_pass),
        ):
            if type(boolean_value) is not bool:
                raise TypeError(f"semantic receipt {label} must be boolean")
        if not isinstance(self.blockers, tuple) or any(
            not isinstance(item, str) or not item.strip() for item in self.blockers
        ):
            raise TypeError("semantic receipt blockers must be a tuple of non-empty strings")


def _block(
    blockers: list[AdmissionBlocker],
    code: str,
    location: str,
    message: str,
) -> None:
    blockers.append(AdmissionBlocker(code=code, location=location, message=message))


def validate_candidate_semantic_verification(
    receipt: object,
    *,
    manifest: AdmissionManifest,
    strategy_name: str,
    frozen_config_hash: str,
    data_hash: str,
) -> tuple[AdmissionBlocker, ...]:
    """Validate a diagnostic receipt without authorizing paper admission."""

    blockers: list[AdmissionBlocker] = []
    if not isinstance(receipt, CandidateSemanticVerification):
        _block(
            blockers,
            "INVALID_SEMANTIC_RECEIPT",
            "semantic_receipt",
            "candidate evaluator must return CandidateSemanticVerification",
        )
        return tuple(blockers)

    allowed_strategies = CANONICAL_CANDIDATE_STRATEGY_NAMES.get(manifest.candidate_id)
    if allowed_strategies is None:
        _block(
            blockers,
            "UNKNOWN_CANDIDATE_STRATEGY_POLICY",
            "candidate_id",
            f"no executable strategy policy for {manifest.candidate_id!r}",
        )
    elif strategy_name not in allowed_strategies:
        _block(
            blockers,
            "CANDIDATE_STRATEGY_MISMATCH",
            "strategy_name",
            f"{strategy_name!r} is not allowed for candidate {manifest.candidate_id!r}",
        )

    if receipt.candidate_id != manifest.candidate_id:
        _block(
            blockers,
            "SEMANTIC_CANDIDATE_MISMATCH",
            "semantic_receipt.candidate_id",
            f"expected {manifest.candidate_id!r}, got {receipt.candidate_id!r}",
        )
    if receipt.strategy_name != strategy_name:
        _block(
            blockers,
            "SEMANTIC_STRATEGY_MISMATCH",
            "semantic_receipt.strategy_name",
            f"expected {strategy_name!r}, got {receipt.strategy_name!r}",
        )
    if not receipt.gate_b_recomputed_pass:
        _block(
            blockers,
            "GATE_B_SEMANTIC_BLOCKED",
            "semantic_receipt.gate_b_recomputed_pass",
            "trusted evaluator did not recompute Gate B as PASS",
        )
    if not receipt.gate_c_recomputed_pass:
        _block(
            blockers,
            "GATE_C_SEMANTIC_BLOCKED",
            "semantic_receipt.gate_c_recomputed_pass",
            "trusted evaluator did not recompute Gate C as PASS",
        )

    expected_bindings = (
        (
            "GATE_B_REPORT_HASH_MISMATCH",
            "semantic_receipt.gate_b_report_sha256",
            receipt.gate_b_report_sha256,
            manifest.evidence.gate_b_report.sha256,
        ),
        (
            "GATE_C_REPORT_HASH_MISMATCH",
            "semantic_receipt.gate_c_report_sha256",
            receipt.gate_c_report_sha256,
            manifest.evidence.gate_c_report.sha256,
        ),
        (
            "FROZEN_CONFIG_HASH_MISMATCH",
            "semantic_receipt.frozen_config_hash",
            receipt.frozen_config_hash,
            frozen_config_hash,
        ),
        (
            "DATA_HASH_MISMATCH",
            "semantic_receipt.data_hash",
            receipt.data_hash,
            data_hash,
        ),
    )
    for code, location, actual, expected in expected_bindings:
        if actual != expected:
            _block(blockers, code, location, f"expected {expected}, got {actual}")

    if frozen_config_hash != manifest.identities.frozen_config.identity:
        _block(
            blockers,
            "FROZEN_CONFIG_IDENTITY_MISMATCH",
            "identities.frozen_config.identity",
            "manifest frozen-config identity differs from the runtime config hash",
        )
    if data_hash != manifest.identities.market_source.artifact.sha256:
        _block(
            blockers,
            "DATA_IDENTITY_MISMATCH",
            "identities.market_source.artifact.sha256",
            "manifest market-source artifact differs from the runtime data hash",
        )
    for index, message in enumerate(receipt.blockers):
        _block(
            blockers,
            "SEMANTIC_VERIFIER_BLOCKER",
            f"semantic_receipt.blockers[{index}]",
            message,
        )
    _block(
        blockers,
        "SEMANTIC_RECEIPT_NON_AUTHORIZING",
        "semantic_receipt",
        (
            "typed receipt fields are diagnostic only; production admission requires "
            "a concrete evaluator whose measured canonical metrics are checked by core"
        ),
    )
    return tuple(blockers)


def _same_json(left: object, right: object) -> bool:
    return _canonical_json_bytes(left) == _canonical_json_bytes(right)


def _compare_thresholds(
    expected: object,
    actual: object,
    *,
    location: str,
    blockers: list[AdmissionBlocker],
) -> None:
    if isinstance(expected, Mapping) and isinstance(actual, Mapping):
        expected_keys = set(expected)
        actual_keys = set(actual)
        for key in sorted(expected_keys - actual_keys):
            _block(
                blockers,
                "CANONICAL_THRESHOLD_MISSING",
                f"{location}.{key}",
                "required canonical threshold is missing",
            )
        for key in sorted(actual_keys - expected_keys):
            _block(
                blockers,
                "UNEXPECTED_THRESHOLD",
                f"{location}.{key}",
                "threshold is not part of the canonical candidate policy",
            )
        for key in sorted(expected_keys & actual_keys):
            _compare_thresholds(
                expected[key],
                actual[key],
                location=f"{location}.{key}",
                blockers=blockers,
            )
        return
    if isinstance(expected, (tuple, list)) and isinstance(actual, (tuple, list)):
        if not _same_json(expected, actual):
            _block(
                blockers,
                "CANONICAL_THRESHOLD_MISMATCH",
                location,
                f"expected {_thaw_json(expected)!r}, got {_thaw_json(actual)!r}",
            )
        return
    if type(expected) is not type(actual) or expected != actual:
        _block(
            blockers,
            "CANONICAL_THRESHOLD_MISMATCH",
            location,
            f"expected {expected!r}, got {actual!r}",
        )


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


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


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
    if _stat_identity(before) != _stat_identity(after):
        return None, "artifact changed while its bytes were being hashed"
    return digest.hexdigest(), None


def _verify_artifact(
    binding: ArtifactBinding,
    *,
    location: str,
    root: Path | None,
    blockers: list[AdmissionBlocker],
    resolved_seen: dict[str, str],
) -> None:
    parts = _portable_relative_path(binding.relative_path)
    if parts is None:
        _block(
            blockers,
            "INVALID_ARTIFACT_PATH",
            f"{location}.path",
            "path must be a normalized portable relative path without '.', '..', drive or backslash",
        )
    if _SHA256_RE.fullmatch(binding.sha256) is None:
        _block(
            blockers,
            "INVALID_SHA256",
            f"{location}.sha256",
            "SHA-256 must be exactly 64 lowercase hexadecimal characters",
        )
    if parts is None or root is None:
        return
    lexical = root.joinpath(*parts)
    if _has_link_component(root, parts):
        _block(
            blockers,
            "SYMLINK_ARTIFACT_REFUSED",
            f"{location}.path",
            "artifact paths may not traverse symlinks or junctions",
        )
        return
    try:
        resolved = lexical.resolve(strict=True)
    except FileNotFoundError:
        _block(blockers, "ARTIFACT_MISSING", f"{location}.path", binding.relative_path)
        return
    except OSError as error:
        _block(blockers, "ARTIFACT_UNREADABLE", f"{location}.path", str(error))
        return
    try:
        resolved.relative_to(root)
    except ValueError:
        _block(
            blockers,
            "ARTIFACT_PATH_ESCAPE",
            f"{location}.path",
            f"resolved path escapes evidence root: {resolved}",
        )
        return
    normalized = os.path.normcase(str(resolved))
    previous = resolved_seen.get(normalized)
    if previous is not None:
        _block(
            blockers,
            "DUPLICATE_ARTIFACT_PATH",
            f"{location}.path",
            f"same artifact is already bound as {previous}",
        )
        return
    resolved_seen[normalized] = location
    if not resolved.is_file():
        _block(
            blockers,
            "ARTIFACT_NOT_REGULAR_FILE",
            f"{location}.path",
            binding.relative_path,
        )
        return
    actual, hash_error = _hash_regular_file(resolved)
    if hash_error is not None:
        code = (
            "ARTIFACT_CHANGED_DURING_VERIFICATION"
            if actual is None and "changed" in hash_error
            else "ARTIFACT_UNREADABLE"
        )
        _block(blockers, code, f"{location}.path", hash_error)
        return
    if actual != binding.sha256:
        _block(
            blockers,
            "ARTIFACT_HASH_MISMATCH",
            f"{location}.sha256",
            f"expected {binding.sha256}, recomputed {actual}",
        )


def verify_admission_manifest(
    manifest: AdmissionManifest,
    *,
    evidence_root: Path,
) -> AdmissionVerification:
    """Recompute every byte binding; never infer that Gate B or C passed."""

    blockers: list[AdmissionBlocker] = []
    if manifest.schema_version != ADMISSION_MANIFEST_SCHEMA_VERSION:
        _block(
            blockers,
            "UNSUPPORTED_SCHEMA_VERSION",
            "schema_version",
            f"expected {ADMISSION_MANIFEST_SCHEMA_VERSION}, got {manifest.schema_version}",
        )
    policy = CANONICAL_CANDIDATE_POLICIES.get(manifest.candidate_id)
    if policy is None:
        _block(
            blockers,
            "UNKNOWN_CANDIDATE",
            "candidate_id",
            f"no canonical admission policy for {manifest.candidate_id!r}",
        )
    else:
        _compare_thresholds(
            policy.gate_b_thresholds,
            manifest.gate_b_thresholds,
            location="gate_thresholds.gate_b",
            blockers=blockers,
        )
        _compare_thresholds(
            policy.gate_c_thresholds,
            manifest.gate_c_thresholds,
            location="gate_thresholds.gate_c",
            blockers=blockers,
        )
    for name in ("data", "calibration", "strategy"):
        if not cast(tuple[ArtifactBinding, ...], getattr(manifest.evidence, name)):
            _block(
                blockers,
                "MISSING_REQUIRED_ARTIFACT_SET",
                f"evidence.{name}",
                "at least one explicitly bound artifact is required",
            )
    for name in ("market_source", "cost_schedule", "frozen_config"):
        identity = cast(ArtifactIdentity, getattr(manifest.identities, name)).identity
        if _IDENTITY_RE.fullmatch(identity) is None:
            _block(
                blockers,
                "INVALID_IDENTITY",
                f"identities.{name}.identity",
                "identity must be a stable 1-256 character identifier without whitespace",
            )
    root: Path | None
    try:
        resolved_root = evidence_root.resolve(strict=True)
    except (FileNotFoundError, OSError) as root_error:
        _block(blockers, "EVIDENCE_ROOT_UNAVAILABLE", "evidence_root", str(root_error))
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
    resolved_seen: dict[str, str] = {}
    for location, binding in manifest.iter_artifacts():
        _verify_artifact(
            binding,
            location=location,
            root=root,
            blockers=blockers,
            resolved_seen=resolved_seen,
        )
    try:
        manifest_sha256 = manifest.manifest_sha256
    except (AdmissionManifestError, TypeError, ValueError):
        manifest_sha256 = None
        _block(
            blockers,
            "MANIFEST_NOT_CANONICALIZABLE",
            "$",
            "manifest model contains a value outside canonical JSON",
        )
    return AdmissionVerification(
        candidate_id=manifest.candidate_id,
        manifest_sha256=manifest_sha256,
        blockers=tuple(blockers),
    )


def verify_admission_manifest_file(
    manifest_path: Path,
    *,
    evidence_root: Path,
) -> AdmissionVerification:
    """Load and verify a canonical manifest while reporting parse failures as blockers."""

    try:
        payload = manifest_path.read_bytes()
    except OSError as error:
        return AdmissionVerification(
            candidate_id=None,
            manifest_sha256=None,
            blockers=(AdmissionBlocker("MANIFEST_UNREADABLE", "$", str(error)),),
        )
    raw_sha256 = hashlib.sha256(payload).hexdigest()
    try:
        manifest = AdmissionManifest.from_json_bytes(payload, require_canonical=True)
    except AdmissionManifestError as error:
        return AdmissionVerification(
            candidate_id=None,
            manifest_sha256=raw_sha256,
            blockers=(AdmissionBlocker(error.code, error.location, error.message),),
        )
    return verify_admission_manifest(manifest, evidence_root=evidence_root)


__all__ = [
    "ADMISSION_MANIFEST_SCHEMA_VERSION",
    "CANONICAL_CANDIDATE_POLICIES",
    "CANONICAL_CANDIDATE_STRATEGY_NAMES",
    "AdmissionBlocker",
    "AdmissionIdentities",
    "AdmissionManifest",
    "AdmissionManifestError",
    "AdmissionVerification",
    "ArtifactBinding",
    "ArtifactIdentity",
    "CandidateAdmissionPolicy",
    "CandidateSemanticVerification",
    "EvidenceArtifacts",
    "FrozenResearchArtifacts",
    "canonical_thresholds_for",
    "load_admission_manifest",
    "validate_candidate_semantic_verification",
    "verify_admission_manifest",
    "verify_admission_manifest_file",
]
