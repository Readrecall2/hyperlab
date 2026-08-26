"""Immutable, fail-closed publication for Storage V4 Phase 1C evidence.

The publisher owns one fresh candidate directory.  Every report is canonical
JSON (or canonical JSONL for measurements), embeds SHA-256 links to its exact
predecessors, and is written once.  Completion markers are ordinary immutable
artifacts: a level marker is impossible before its level prerequisites verify,
and the terminal marker is impossible before the complete evidence tree does.

This module deliberately does not manufacture measurements or conclusions.
Callers supply report payloads and statuses; only completion statuses and the
three authorized terminal verdicts are fixed by this publication contract.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import stat
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from .canonical import CanonicalizationError, canonical_json_bytes
from .durability import fsync_directory

NOT_ECONOMIC_EVIDENCE = "NOT_ECONOMIC_EVIDENCE"
NOT_ALPHA_EVIDENCE = "NOT_ALPHA_EVIDENCE"
PAPER_ONLY = "PAPER_ONLY"
SYNTHETIC_CAPACITY_WORKLOAD = "SYNTHETIC_CAPACITY_WORKLOAD"

PHASE1C_EVIDENCE_MARKERS = (
    NOT_ECONOMIC_EVIDENCE,
    NOT_ALPHA_EVIDENCE,
    PAPER_ONLY,
)
PHASE1C_SYNTHETIC_MARKERS = (
    SYNTHETIC_CAPACITY_WORKLOAD,
    NOT_ECONOMIC_EVIDENCE,
    NOT_ALPHA_EVIDENCE,
    PAPER_ONLY,
)

LEVEL_COMPLETE_STATUS = "STORAGE_V4_PHASE_1C_CAPACITY_LEVEL_COMPLETE"
TERMINAL_VERDICTS = frozenset(
    {
        "STORAGE_V4_PHASE_1C_NATIVE_CAPACITY_PROVEN",
        "STORAGE_V4_PHASE_1C_NATIVE_CAPACITY_CHARACTERIZED_TARGET_NOT_MET",
        "STORAGE_V4_PHASE_1C_NATIVE_CAPACITY_CHARACTERIZED_NO_CANONICAL_TARGET",
    }
)

PROVENANCE_CONTRACT = "STORAGE_V4_PHASE_1C_PROVENANCE_V1"
_RESERVED_IDENTITY_FIELDS = frozenset(
    {
        "candidate_id",
        "candidate_root",
        "gates",
        "links",
        "provenance",
        "provenance_sha256",
        "run_id",
        "status",
    }
)


class EvidenceReportStatus(StrEnum):
    """Only statuses with defined completion semantics may enter the DAG."""

    WORKLOAD_MANIFEST_FROZEN = "STORAGE_V4_PHASE_1C_WORKLOAD_MANIFEST_FROZEN"
    NATIVE_LAYOUT_VERIFIED = "STORAGE_V4_PHASE_1C_NATIVE_LAYOUT_VERIFIED"
    GOLDEN_NATIVE_EXACT = "STORAGE_V4_PHASE_1C_GOLDEN_NATIVE_EXACT"
    CAPACITY_LEVEL_VERIFIED = "STORAGE_V4_PHASE_1C_CAPACITY_LEVEL_VERIFIED"
    SCALING_CHARACTERIZED = "STORAGE_V4_PHASE_1C_SCALING_CHARACTERIZED"
    INTEGRITY_AND_RECOVERY_VERIFIED = (
        "STORAGE_V4_PHASE_1C_INTEGRITY_AND_RECOVERY_VERIFIED"
    )
    LIMITATIONS_RECORDED = "STORAGE_V4_PHASE_1C_LIMITATIONS_RECORDED"
    MEASUREMENTS_VERIFIED = "STORAGE_V4_PHASE_1C_MEASUREMENTS_VERIFIED"
    VERIFICATION_FAILED = "STORAGE_V4_PHASE_1C_VERIFICATION_FAILED"


class EvidenceSemanticGate(StrEnum):
    """Semantic facts that must be certified before completion publication."""

    CONFIG_BOUND = "config_bound"
    INTEGRITY_VERIFIED = "integrity_verified"
    EXACT_LOGICAL_MATCH = "exact_logical_match"
    STARTUP_BOUNDED = "startup_bounded"
    TAIL_VERIFIED = "tail_verified"
    MEASUREMENTS_COMPLETE = "measurements_complete"
    SCALING_CHARACTERIZED = "scaling_characterized"
    FAULT_RECOVERY_VERIFIED = "fault_recovery_verified"
    LIMITATIONS_RECORDED = "limitations_recorded"
    GOLDEN_SOURCE_UNCHANGED = "golden_source_unchanged"


def _require_sha256(value: str, *, label: str) -> str:
    _require_text(value, label=label)
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return value


@dataclass(frozen=True, slots=True)
class Phase1CEvidenceProvenance:
    """Common immutable identity of every report in one candidate run."""

    candidate_id: str
    candidate_root: str
    run_id: str
    raw_store_id: str
    raw_lake_id: str
    paper_store_id: str
    config_identity: str
    code_identity: str
    runtime_identity: str
    golden_source_root_sha256: str | None = None
    golden_pin_sha256: str | None = None
    golden_certification_root_sha256: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "candidate_id",
            "candidate_root",
            "run_id",
            "raw_store_id",
            "raw_lake_id",
            "paper_store_id",
            "config_identity",
            "code_identity",
            "runtime_identity",
        ):
            _require_text(getattr(self, field_name), label=field_name)
        candidate_root = Path(self.candidate_root)
        if not candidate_root.is_absolute():
            raise ValueError("candidate_root must be absolute")
        golden_values = (
            self.golden_source_root_sha256,
            self.golden_pin_sha256,
            self.golden_certification_root_sha256,
        )
        if any(value is not None for value in golden_values) and not all(
            value is not None for value in golden_values
        ):
            raise ValueError("Golden provenance fields must be all present or all absent")
        for label, value in zip(
            (
                "golden_source_root_sha256",
                "golden_pin_sha256",
                "golden_certification_root_sha256",
            ),
            golden_values,
            strict=True,
        ):
            if value is not None:
                _require_sha256(value, label=label)

    @property
    def has_golden_binding(self) -> bool:
        return self.golden_source_root_sha256 is not None

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_root": self.candidate_root,
            "code_identity": self.code_identity,
            "config_identity": self.config_identity,
            "contract": PROVENANCE_CONTRACT,
            "golden_certification_root_sha256": self.golden_certification_root_sha256,
            "golden_pin_sha256": self.golden_pin_sha256,
            "golden_source_root_sha256": self.golden_source_root_sha256,
            "paper_store_id": self.paper_store_id,
            "raw_lake_id": self.raw_lake_id,
            "raw_store_id": self.raw_store_id,
            "run_id": self.run_id,
            "runtime_identity": self.runtime_identity,
        }

    @property
    def sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.as_dict())).hexdigest()


@dataclass(frozen=True, slots=True)
class Phase1CGateEvidence:
    """Typed result and digest of one independent semantic certifier."""

    gate: EvidenceSemanticGate
    passed: bool
    certifier_contract: str
    certifier_result_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.gate, EvidenceSemanticGate):
            raise TypeError("gate must be EvidenceSemanticGate")
        if type(self.passed) is not bool:
            raise TypeError("passed must be an exact bool")
        _require_text(self.certifier_contract, label="certifier_contract")
        _require_sha256(
            self.certifier_result_sha256,
            label="certifier_result_sha256",
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "certifier_contract": self.certifier_contract,
            "certifier_result_sha256": self.certifier_result_sha256,
            "passed": self.passed,
        }


@dataclass(frozen=True, slots=True)
class Phase1CEvidenceReport:
    """A report request bound to one provenance and explicit certifier results."""

    provenance_sha256: str
    status: EvidenceReportStatus
    gates: tuple[Phase1CGateEvidence, ...]
    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        _require_sha256(self.provenance_sha256, label="provenance_sha256")
        if not isinstance(self.status, EvidenceReportStatus):
            raise TypeError("status must be EvidenceReportStatus")
        if type(self.gates) is not tuple or not all(
            isinstance(gate, Phase1CGateEvidence) for gate in self.gates
        ):
            raise TypeError("gates must be a tuple of Phase1CGateEvidence")
        gate_names = tuple(gate.gate for gate in self.gates)
        if len(set(gate_names)) != len(gate_names):
            raise ValueError("semantic gates must be unique")
        if gate_names != tuple(sorted(gate_names, key=lambda gate: gate.value)):
            raise ValueError("semantic gates must be sorted canonically")
        if not isinstance(self.payload, Mapping):
            raise TypeError("payload must be a mapping")
        collision = _RESERVED_IDENTITY_FIELDS.intersection(self.payload)
        if collision:
            raise ValueError(
                "payload shadows evidence envelope identity: "
                + ", ".join(sorted(collision))
            )
        try:
            canonical_json_bytes(dict(self.payload))
        except CanonicalizationError as error:
            raise ValueError("payload must be canonicalizable JSON") from error


class EvidencePublicationError(RuntimeError):
    """Base class for a rejected or failed evidence publication."""


class UnsafeEvidencePath(EvidencePublicationError):
    """A candidate path could traverse a link, reparse point, or non-directory."""


class EvidenceAlreadyPublished(EvidencePublicationError):
    """A write-once artifact or candidate root already exists."""


class EvidenceIntegrityError(EvidencePublicationError):
    """Published evidence no longer matches its canonical bytes or link graph."""


class EvidenceIncomplete(EvidencePublicationError):
    """A completion marker was requested before all prerequisites existed."""


class EvidenceFaultPoint(StrEnum):
    """Stable boundaries used to test evidence-publication ordering."""

    BEFORE_TEMP_WRITE = "before_temp_write"
    AFTER_TEMP_WRITE = "after_temp_write"
    BEFORE_FILE_FSYNC = "before_file_fsync"
    AFTER_FILE_FSYNC = "after_file_fsync"
    BEFORE_REPLACE = "before_replace"
    AFTER_REPLACE = "after_replace"
    BEFORE_DIRECTORY_FSYNC = "before_directory_fsync"
    AFTER_DIRECTORY_FSYNC = "after_directory_fsync"
    BEFORE_REQUIRED_VERIFICATION = "before_required_verification"
    AFTER_REQUIRED_VERIFICATION = "after_required_verification"
    BEFORE_LEVEL_COMPLETE = "before_level_complete"
    AFTER_LEVEL_COMPLETE = "after_level_complete"
    BEFORE_TERMINAL_COMPLETE = "before_terminal_complete"
    AFTER_TERMINAL_COMPLETE = "after_terminal_complete"


class EvidenceFaultHook(Protocol):
    def __call__(self, point: EvidenceFaultPoint, /) -> None: ...


class EvidenceArtifactName(StrEnum):
    WORKLOAD_MANIFEST = "workload-manifest.json"
    NATIVE_LAYOUT_REPORT = "native-layout-report.json"
    GOLDEN_NATIVE_REPORT = "golden-native-report.json"
    CAPACITY_100K = "capacity-100k.json"
    CAPACITY_500K = "capacity-500k.json"
    CAPACITY_1M = "capacity-1m.json"
    SCALING_REPORT = "scaling-report.json"
    INTEGRITY_REPORT = "integrity-report.json"
    LIMITATIONS = "limitations.json"
    MEASUREMENTS = "measurements.jsonl"


class CapacityEvidenceLevel(StrEnum):
    CAPACITY_100K = "100k"
    CAPACITY_500K = "500k"
    CAPACITY_1M = "1m"


@dataclass(frozen=True, slots=True)
class _ArtifactSpec:
    contract: str
    dependencies: tuple[EvidenceArtifactName, ...]
    markers: tuple[str, ...]
    success_status: EvidenceReportStatus
    required_gates: tuple[EvidenceSemanticGate, ...]


_REPORT_SPECS: dict[EvidenceArtifactName, _ArtifactSpec] = {
    EvidenceArtifactName.WORKLOAD_MANIFEST: _ArtifactSpec(
        "STORAGE_V4_PHASE_1C_WORKLOAD_MANIFEST_V1",
        (),
        PHASE1C_SYNTHETIC_MARKERS,
        EvidenceReportStatus.WORKLOAD_MANIFEST_FROZEN,
        (EvidenceSemanticGate.CONFIG_BOUND,),
    ),
    EvidenceArtifactName.NATIVE_LAYOUT_REPORT: _ArtifactSpec(
        "STORAGE_V4_PHASE_1C_NATIVE_LAYOUT_REPORT_V1",
        (EvidenceArtifactName.WORKLOAD_MANIFEST,),
        PHASE1C_EVIDENCE_MARKERS,
        EvidenceReportStatus.NATIVE_LAYOUT_VERIFIED,
        (EvidenceSemanticGate.INTEGRITY_VERIFIED,),
    ),
    EvidenceArtifactName.GOLDEN_NATIVE_REPORT: _ArtifactSpec(
        "STORAGE_V4_PHASE_1C_GOLDEN_NATIVE_REPORT_V1",
        (EvidenceArtifactName.NATIVE_LAYOUT_REPORT,),
        PHASE1C_EVIDENCE_MARKERS,
        EvidenceReportStatus.GOLDEN_NATIVE_EXACT,
        (
            EvidenceSemanticGate.EXACT_LOGICAL_MATCH,
            EvidenceSemanticGate.GOLDEN_SOURCE_UNCHANGED,
            EvidenceSemanticGate.INTEGRITY_VERIFIED,
            EvidenceSemanticGate.STARTUP_BOUNDED,
            EvidenceSemanticGate.TAIL_VERIFIED,
        ),
    ),
    EvidenceArtifactName.CAPACITY_100K: _ArtifactSpec(
        "STORAGE_V4_PHASE_1C_CAPACITY_100K_V1",
        (
            EvidenceArtifactName.WORKLOAD_MANIFEST,
            EvidenceArtifactName.NATIVE_LAYOUT_REPORT,
        ),
        PHASE1C_SYNTHETIC_MARKERS,
        EvidenceReportStatus.CAPACITY_LEVEL_VERIFIED,
        (
            EvidenceSemanticGate.EXACT_LOGICAL_MATCH,
            EvidenceSemanticGate.INTEGRITY_VERIFIED,
            EvidenceSemanticGate.MEASUREMENTS_COMPLETE,
            EvidenceSemanticGate.STARTUP_BOUNDED,
            EvidenceSemanticGate.TAIL_VERIFIED,
        ),
    ),
    EvidenceArtifactName.CAPACITY_500K: _ArtifactSpec(
        "STORAGE_V4_PHASE_1C_CAPACITY_500K_V1",
        (
            EvidenceArtifactName.WORKLOAD_MANIFEST,
            EvidenceArtifactName.NATIVE_LAYOUT_REPORT,
            EvidenceArtifactName.CAPACITY_100K,
        ),
        PHASE1C_SYNTHETIC_MARKERS,
        EvidenceReportStatus.CAPACITY_LEVEL_VERIFIED,
        (
            EvidenceSemanticGate.EXACT_LOGICAL_MATCH,
            EvidenceSemanticGate.INTEGRITY_VERIFIED,
            EvidenceSemanticGate.MEASUREMENTS_COMPLETE,
            EvidenceSemanticGate.STARTUP_BOUNDED,
            EvidenceSemanticGate.TAIL_VERIFIED,
        ),
    ),
    EvidenceArtifactName.CAPACITY_1M: _ArtifactSpec(
        "STORAGE_V4_PHASE_1C_CAPACITY_1M_V1",
        (
            EvidenceArtifactName.WORKLOAD_MANIFEST,
            EvidenceArtifactName.NATIVE_LAYOUT_REPORT,
            EvidenceArtifactName.CAPACITY_500K,
        ),
        PHASE1C_SYNTHETIC_MARKERS,
        EvidenceReportStatus.CAPACITY_LEVEL_VERIFIED,
        (
            EvidenceSemanticGate.EXACT_LOGICAL_MATCH,
            EvidenceSemanticGate.INTEGRITY_VERIFIED,
            EvidenceSemanticGate.MEASUREMENTS_COMPLETE,
            EvidenceSemanticGate.STARTUP_BOUNDED,
            EvidenceSemanticGate.TAIL_VERIFIED,
        ),
    ),
    EvidenceArtifactName.SCALING_REPORT: _ArtifactSpec(
        "STORAGE_V4_PHASE_1C_SCALING_REPORT_V1",
        (
            EvidenceArtifactName.CAPACITY_100K,
            EvidenceArtifactName.CAPACITY_500K,
            EvidenceArtifactName.CAPACITY_1M,
        ),
        PHASE1C_SYNTHETIC_MARKERS,
        EvidenceReportStatus.SCALING_CHARACTERIZED,
        (
            EvidenceSemanticGate.MEASUREMENTS_COMPLETE,
            EvidenceSemanticGate.SCALING_CHARACTERIZED,
        ),
    ),
    EvidenceArtifactName.INTEGRITY_REPORT: _ArtifactSpec(
        "STORAGE_V4_PHASE_1C_INTEGRITY_REPORT_V1",
        (
            EvidenceArtifactName.GOLDEN_NATIVE_REPORT,
            EvidenceArtifactName.CAPACITY_100K,
            EvidenceArtifactName.CAPACITY_500K,
            EvidenceArtifactName.CAPACITY_1M,
        ),
        PHASE1C_EVIDENCE_MARKERS,
        EvidenceReportStatus.INTEGRITY_AND_RECOVERY_VERIFIED,
        (
            EvidenceSemanticGate.FAULT_RECOVERY_VERIFIED,
            EvidenceSemanticGate.INTEGRITY_VERIFIED,
        ),
    ),
    EvidenceArtifactName.LIMITATIONS: _ArtifactSpec(
        "STORAGE_V4_PHASE_1C_LIMITATIONS_V1",
        (
            EvidenceArtifactName.GOLDEN_NATIVE_REPORT,
            EvidenceArtifactName.SCALING_REPORT,
            EvidenceArtifactName.INTEGRITY_REPORT,
        ),
        PHASE1C_EVIDENCE_MARKERS,
        EvidenceReportStatus.LIMITATIONS_RECORDED,
        (EvidenceSemanticGate.LIMITATIONS_RECORDED,),
    ),
    EvidenceArtifactName.MEASUREMENTS: _ArtifactSpec(
        "STORAGE_V4_PHASE_1C_MEASUREMENT_V1",
        (
            EvidenceArtifactName.GOLDEN_NATIVE_REPORT,
            EvidenceArtifactName.CAPACITY_100K,
            EvidenceArtifactName.CAPACITY_500K,
            EvidenceArtifactName.CAPACITY_1M,
            EvidenceArtifactName.SCALING_REPORT,
            EvidenceArtifactName.INTEGRITY_REPORT,
            EvidenceArtifactName.LIMITATIONS,
        ),
        PHASE1C_SYNTHETIC_MARKERS,
        EvidenceReportStatus.MEASUREMENTS_VERIFIED,
        (EvidenceSemanticGate.MEASUREMENTS_COMPLETE,),
    ),
}

_LEVEL_REPORTS = {
    CapacityEvidenceLevel.CAPACITY_100K: EvidenceArtifactName.CAPACITY_100K,
    CapacityEvidenceLevel.CAPACITY_500K: EvidenceArtifactName.CAPACITY_500K,
    CapacityEvidenceLevel.CAPACITY_1M: EvidenceArtifactName.CAPACITY_1M,
}
_LEVEL_DIRECTORY_NAMES = {
    CapacityEvidenceLevel.CAPACITY_100K: "capacity-100k",
    CapacityEvidenceLevel.CAPACITY_500K: "capacity-500k",
    CapacityEvidenceLevel.CAPACITY_1M: "capacity-1m",
}


@dataclass(frozen=True, slots=True)
class PublishedEvidenceArtifact:
    """Exact identity and dependency edges for one immutable artifact."""

    name: str
    path: Path
    sha256: str
    size_bytes: int
    artifact_contract: str
    status: str
    markers: tuple[str, ...]
    links: tuple[tuple[str, str], ...]
    provenance_sha256: str
    gates: tuple[Phase1CGateEvidence, ...]
    jsonl: bool
    directory_fsync_supported: bool


def _require_text(value: str, *, label: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{label} must be non-empty text")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise ValueError(f"{label} must be strict UTF-8 text") from error
    return value


def _path_stat(path: Path) -> os.stat_result | None:
    try:
        return path.lstat()
    except FileNotFoundError:
        return None


def _is_link_or_reparse(path_stat: os.stat_result) -> bool:
    if stat.S_ISLNK(path_stat.st_mode):
        return True
    attributes = int(getattr(path_stat, "st_file_attributes", 0))
    reparse_mask = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & reparse_mask)


def _require_safe_directory(path: Path) -> os.stat_result:
    path_stat = _path_stat(path)
    if path_stat is None:
        raise UnsafeEvidencePath(f"evidence directory does not exist: {path}")
    if _is_link_or_reparse(path_stat):
        raise UnsafeEvidencePath(
            f"evidence path is a symbolic link or reparse point: {path}"
        )
    if not stat.S_ISDIR(path_stat.st_mode):
        raise UnsafeEvidencePath(f"evidence path is not a directory: {path}")
    return path_stat


def _require_safe_directory_chain(path: Path) -> None:
    candidates = (path, *path.parents)
    for candidate in reversed(candidates):
        _require_safe_directory(candidate)


def _require_absent(path: Path) -> None:
    path_stat = _path_stat(path)
    if path_stat is None:
        return
    if _is_link_or_reparse(path_stat):
        raise UnsafeEvidencePath(
            f"evidence target is a symbolic link or reparse point: {path}"
        )
    if not stat.S_ISREG(path_stat.st_mode):
        raise UnsafeEvidencePath(f"evidence target is not a regular file: {path}")
    raise EvidenceAlreadyPublished(f"evidence path already exists: {path}")


def _read_regular_file(path: Path) -> bytes:
    path_stat = _path_stat(path)
    if path_stat is None:
        raise EvidenceIntegrityError(f"published evidence is absent: {path}")
    if _is_link_or_reparse(path_stat) or not stat.S_ISREG(path_stat.st_mode):
        raise EvidenceIntegrityError(
            f"published evidence is not a regular non-reparse file: {path}"
        )
    try:
        return path.read_bytes()
    except OSError as error:
        raise EvidenceIntegrityError(f"could not read published evidence: {path}") from error


def _parse_canonical_json(data: bytes, *, label: str) -> dict[str, object]:
    try:
        value = json.loads(data.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EvidenceIntegrityError(f"{label} is not strict JSON") from error
    if type(value) is not dict:
        raise EvidenceIntegrityError(f"{label} must be a JSON object")
    try:
        canonical = canonical_json_bytes(value)
    except CanonicalizationError as error:
        raise EvidenceIntegrityError(f"{label} is not canonical JSON") from error
    if canonical != data:
        raise EvidenceIntegrityError(f"{label} bytes are not canonical JSON")
    return value


class Phase1CEvidencePublisher:
    """Write-once publisher for one complete Phase 1C evidence candidate."""

    def __init__(
        self,
        output_root: Path,
        *,
        provenance: Phase1CEvidenceProvenance,
        fault_hook: EvidenceFaultHook | None = None,
    ) -> None:
        if not isinstance(output_root, Path):
            raise TypeError("output_root must be pathlib.Path")
        if not isinstance(provenance, Phase1CEvidenceProvenance):
            raise TypeError("provenance must be Phase1CEvidenceProvenance")
        absolute_root = Path(os.path.abspath(os.fspath(output_root)))
        if provenance.candidate_root != os.fspath(absolute_root):
            raise ValueError("provenance candidate_root differs from output_root")
        _require_safe_directory_chain(absolute_root.parent)
        _require_absent(absolute_root)
        try:
            absolute_root.mkdir()
        except FileExistsError as error:
            raise EvidenceAlreadyPublished(
                f"evidence candidate root already exists: {absolute_root}"
            ) from error
        root_stat = _require_safe_directory(absolute_root)

        self.output_root = absolute_root
        self.provenance = provenance
        self._provenance_sha256 = provenance.sha256
        self._root_identity = (root_stat.st_dev, root_stat.st_ino)
        self._fault_hook = fault_hook
        self._records: dict[str, PublishedEvidenceArtifact] = {}
        self._level_identities: dict[CapacityEvidenceLevel, tuple[int, int]] = {}
        self._directory_fsync_supported = True
        self._sync_directory(absolute_root.parent)

    @property
    def directory_fsync_supported(self) -> bool:
        return self._directory_fsync_supported

    @property
    def records(self) -> tuple[PublishedEvidenceArtifact, ...]:
        return tuple(self._records.values())

    def make_report(
        self,
        *,
        status: EvidenceReportStatus,
        gates: Iterable[Phase1CGateEvidence],
        payload: Mapping[str, object],
    ) -> Phase1CEvidenceReport:
        """Create a typed report request cryptographically bound to this run."""

        gate_tuple = tuple(gates)
        if not all(isinstance(gate, Phase1CGateEvidence) for gate in gate_tuple):
            raise TypeError("gates must contain Phase1CGateEvidence")
        ordered_gates = tuple(sorted(gate_tuple, key=lambda gate: gate.gate.value))
        return Phase1CEvidenceReport(
            provenance_sha256=self._provenance_sha256,
            status=status,
            gates=ordered_gates,
            payload=payload,
        )

    def _trigger(self, point: EvidenceFaultPoint) -> None:
        if self._fault_hook is not None:
            self._fault_hook(point)

    def _sync_directory(self, path: Path) -> None:
        self._trigger(EvidenceFaultPoint.BEFORE_DIRECTORY_FSYNC)
        try:
            fsync_directory(path)
        except OSError as error:
            unsupported_errno = {
                errno.EBADF,
                errno.EINVAL,
                getattr(errno, "ENOTSUP", errno.EINVAL),
            }
            unsupported_winerror = {1, 50}
            if error.errno not in unsupported_errno and getattr(
                error, "winerror", None
            ) not in unsupported_winerror:
                raise
            self._directory_fsync_supported = False
        self._trigger(EvidenceFaultPoint.AFTER_DIRECTORY_FSYNC)

    def _require_live_root(self) -> None:
        root_stat = _require_safe_directory(self.output_root)
        if (root_stat.st_dev, root_stat.st_ino) != self._root_identity:
            raise UnsafeEvidencePath("evidence candidate root identity changed")

    def _require_level_directory(self, level: CapacityEvidenceLevel) -> Path:
        directory = self.output_root / _LEVEL_DIRECTORY_NAMES[level]
        identity = self._level_identities.get(level)
        if identity is None:
            _require_absent(directory)
            directory.mkdir()
            directory_stat = _require_safe_directory(directory)
            identity = (directory_stat.st_dev, directory_stat.st_ino)
            self._level_identities[level] = identity
            self._sync_directory(self.output_root)
        directory_stat = _require_safe_directory(directory)
        if (directory_stat.st_dev, directory_stat.st_ino) != identity:
            raise UnsafeEvidencePath(
                f"capacity evidence directory identity changed: {directory}"
            )
        return directory

    def _publish_bytes(self, target: Path, data: bytes) -> bool:
        self._require_live_root()
        _require_safe_directory(target.parent)
        _require_absent(target)
        temporary = target.parent / f".{target.name}.{uuid4().hex}.tmp"
        _require_absent(temporary)
        try:
            with temporary.open("xb") as stream:
                self._trigger(EvidenceFaultPoint.BEFORE_TEMP_WRITE)
                written = stream.write(data)
                if written != len(data):
                    raise EvidencePublicationError(
                        f"temporary evidence write was incomplete: {target.name}"
                    )
                self._trigger(EvidenceFaultPoint.AFTER_TEMP_WRITE)
                stream.flush()
                self._trigger(EvidenceFaultPoint.BEFORE_FILE_FSYNC)
                os.fsync(stream.fileno())
                self._trigger(EvidenceFaultPoint.AFTER_FILE_FSYNC)
            if _read_regular_file(temporary) != data:
                raise EvidenceIntegrityError(
                    f"temporary evidence read-back differs: {target.name}"
                )
            self._trigger(EvidenceFaultPoint.BEFORE_REPLACE)
            try:
                # Match durable_publish_immutable's atomic no-replace primitive
                # while retaining Evidence's stricter collision and fault contract.
                os.link(temporary, target)
            except FileExistsError as error:
                target_stat = _path_stat(target)
                if target_stat is None:
                    raise EvidenceIntegrityError(
                        "exclusive evidence publication collided but the target "
                        f"disappeared: {target.name}"
                    ) from error
                if _is_link_or_reparse(target_stat) or not stat.S_ISREG(
                    target_stat.st_mode
                ):
                    raise UnsafeEvidencePath(
                        "exclusive evidence publication collided with a non-regular "
                        f"or reparse target: {target}"
                    ) from error
                if _read_regular_file(target) != data:
                    raise EvidenceIntegrityError(
                        "exclusive evidence publication collision bytes differ: "
                        f"{target.name}"
                    ) from error
                raise EvidenceAlreadyPublished(
                    f"evidence path already exists: {target}"
                ) from error
            self._trigger(EvidenceFaultPoint.AFTER_REPLACE)
            temporary.unlink()
            self._sync_directory(target.parent)
            if _read_regular_file(target) != data:
                raise EvidenceIntegrityError(
                    f"published evidence read-back differs: {target.name}"
                )
            return self._directory_fsync_supported
        finally:
            temporary_stat = _path_stat(temporary)
            if temporary_stat is not None:
                if _is_link_or_reparse(temporary_stat) or not stat.S_ISREG(
                    temporary_stat.st_mode
                ):
                    raise UnsafeEvidencePath(
                        f"temporary evidence path was substituted: {temporary}"
                    )
                temporary.unlink()

    def _dependency_links(
        self,
        dependencies: tuple[EvidenceArtifactName, ...],
    ) -> tuple[tuple[str, str], ...]:
        missing = [item.value for item in dependencies if item.value not in self._records]
        if missing:
            raise EvidenceIncomplete(
                "required evidence has not been published: " + ", ".join(missing)
            )
        links: list[tuple[str, str]] = []
        for dependency in dependencies:
            record = self._records[dependency.value]
            self.verify_artifact(record.name)
            links.append((record.name, record.sha256))
        return tuple(links)

    def _record_publication(
        self,
        *,
        name: str,
        target: Path,
        data: bytes,
        contract: str,
        status: str,
        markers: tuple[str, ...],
        links: tuple[tuple[str, str], ...],
        provenance_sha256: str,
        gates: tuple[Phase1CGateEvidence, ...],
        jsonl: bool,
    ) -> PublishedEvidenceArtifact:
        if name in self._records:
            raise EvidenceAlreadyPublished(f"evidence artifact already published: {name}")
        directory_fsync_supported = self._publish_bytes(target, data)
        record = PublishedEvidenceArtifact(
            name=name,
            path=target,
            sha256=hashlib.sha256(data).hexdigest(),
            size_bytes=len(data),
            artifact_contract=contract,
            status=status,
            markers=markers,
            links=links,
            provenance_sha256=provenance_sha256,
            gates=gates,
            jsonl=jsonl,
            directory_fsync_supported=directory_fsync_supported,
        )
        self._records[name] = record
        self.verify_artifact(name)
        return record

    def _validate_report(
        self,
        name: EvidenceArtifactName,
        report: Phase1CEvidenceReport,
    ) -> _ArtifactSpec:
        if not isinstance(report, Phase1CEvidenceReport):
            raise TypeError("report must be Phase1CEvidenceReport")
        if report.provenance_sha256 != self._provenance_sha256:
            raise EvidenceIntegrityError("report provenance differs from candidate run")
        spec = _REPORT_SPECS[name]
        if report.status not in {
            spec.success_status,
            EvidenceReportStatus.VERIFICATION_FAILED,
        }:
            raise ValueError(f"report status is invalid for {name.value}")
        observed_gates = tuple(gate.gate for gate in report.gates)
        if observed_gates != spec.required_gates:
            raise EvidenceIncomplete(
                f"semantic gate set differs for {name.value}"
            )
        all_passed = all(gate.passed for gate in report.gates)
        if report.status is spec.success_status and not all_passed:
            raise EvidenceIncomplete(
                f"successful report contains a failed semantic gate: {name.value}"
            )
        if report.status is EvidenceReportStatus.VERIFICATION_FAILED and all_passed:
            raise EvidenceIncomplete(
                f"failed report contains no failed semantic gate: {name.value}"
            )
        if name is EvidenceArtifactName.GOLDEN_NATIVE_REPORT and not (
            self.provenance.has_golden_binding
        ):
            raise EvidenceIncomplete("Golden report requires complete Golden provenance")
        return spec

    def _require_semantic_success(self, name: EvidenceArtifactName) -> None:
        record = self.verify_artifact(name.value)
        spec = _REPORT_SPECS[name]
        if record.status != spec.success_status.value:
            raise EvidenceIncomplete(f"semantic status failed for {name.value}")
        if tuple(gate.gate for gate in record.gates) != spec.required_gates or not all(
            gate.passed for gate in record.gates
        ):
            raise EvidenceIncomplete(f"semantic gates failed for {name.value}")
        if record.provenance_sha256 != self._provenance_sha256:
            raise EvidenceIntegrityError(f"provenance differs for {name.value}")

    def publish_json(
        self,
        name: EvidenceArtifactName,
        *,
        report: Phase1CEvidenceReport,
    ) -> PublishedEvidenceArtifact:
        """Publish one prescribed canonical report after verifying dependencies."""

        if not isinstance(name, EvidenceArtifactName):
            raise TypeError("name must be EvidenceArtifactName")
        if name is EvidenceArtifactName.MEASUREMENTS:
            raise ValueError("use publish_measurements for measurements.jsonl")
        spec = self._validate_report(name, report)
        links = self._dependency_links(spec.dependencies)
        if report.status is spec.success_status:
            for dependency in spec.dependencies:
                self._require_semantic_success(dependency)
        envelope = {
            "artifact": spec.contract,
            "gates": {
                gate.gate.value: gate.as_dict() for gate in report.gates
            },
            "links": dict(links),
            "markers": list(spec.markers),
            "payload": dict(report.payload),
            "provenance": self.provenance.as_dict(),
            "provenance_sha256": self._provenance_sha256,
            "status": report.status.value,
        }
        data = canonical_json_bytes(envelope)
        return self._record_publication(
            name=name.value,
            target=self.output_root / name.value,
            data=data,
            contract=spec.contract,
            status=report.status.value,
            markers=spec.markers,
            links=links,
            provenance_sha256=self._provenance_sha256,
            gates=report.gates,
            jsonl=False,
        )

    def publish_measurements(
        self,
        measurements: Iterable[Mapping[str, object]],
        *,
        report: Phase1CEvidenceReport,
        synthetic: bool,
    ) -> PublishedEvidenceArtifact:
        """Publish a complete immutable canonical JSONL measurement set once."""

        if type(synthetic) is not bool:
            raise TypeError("synthetic must be an exact bool")
        name = EvidenceArtifactName.MEASUREMENTS
        spec = self._validate_report(name, report)
        links = self._dependency_links(spec.dependencies)
        if report.status is spec.success_status:
            for dependency in spec.dependencies:
                self._require_semantic_success(dependency)
        markers = PHASE1C_SYNTHETIC_MARKERS if synthetic else PHASE1C_EVIDENCE_MARKERS
        lines: list[bytes] = []
        for ordinal, measurement in enumerate(measurements, start=1):
            if not isinstance(measurement, Mapping):
                raise TypeError("measurements must contain mappings")
            collision = _RESERVED_IDENTITY_FIELDS.intersection(measurement)
            if collision:
                raise ValueError(
                    "measurement shadows evidence envelope identity: "
                    + ", ".join(sorted(collision))
                )
            lines.append(
                canonical_json_bytes(
                    {
                        "artifact": spec.contract,
                        "gates": {
                            gate.gate.value: gate.as_dict() for gate in report.gates
                        },
                        "links": dict(links),
                        "markers": list(markers),
                        "ordinal": ordinal,
                        "payload": dict(measurement),
                        "provenance": self.provenance.as_dict(),
                        "provenance_sha256": self._provenance_sha256,
                        "status": report.status.value,
                    }
                )
                + b"\n"
            )
        if not lines:
            raise ValueError("measurements must not be empty")
        return self._record_publication(
            name=EvidenceArtifactName.MEASUREMENTS.value,
            target=self.output_root / EvidenceArtifactName.MEASUREMENTS.value,
            data=b"".join(lines),
            contract=spec.contract,
            status=report.status.value,
            markers=markers,
            links=links,
            provenance_sha256=self._provenance_sha256,
            gates=report.gates,
            jsonl=True,
        )

    def _verify_record_bytes(
        self,
        record: PublishedEvidenceArtifact,
        data: bytes,
    ) -> None:
        if hashlib.sha256(data).hexdigest() != record.sha256:
            raise EvidenceIntegrityError(
                f"published evidence SHA-256 differs: {record.name}"
            )
        if len(data) != record.size_bytes:
            raise EvidenceIntegrityError(
                f"published evidence size differs: {record.name}"
            )
        if record.jsonl:
            if not data.endswith(b"\n") or not data:
                raise EvidenceIntegrityError(
                    f"measurement evidence is not newline-terminated: {record.name}"
                )
            lines = data.splitlines(keepends=True)
            for index, line in enumerate(lines, start=1):
                if not line.endswith(b"\n") or line == b"\n":
                    raise EvidenceIntegrityError(
                        f"measurement line {index} is empty or unterminated"
                    )
                value = _parse_canonical_json(
                    line[:-1], label=f"{record.name} line {index}"
                )
                self._verify_envelope(record, value, allow_ordinal=True)
        else:
            value = _parse_canonical_json(data, label=record.name)
            self._verify_envelope(record, value, allow_ordinal=False)

    def _verify_envelope(
        self,
        record: PublishedEvidenceArtifact,
        value: dict[str, object],
        *,
        allow_ordinal: bool,
    ) -> None:
        required = {
            "artifact",
            "gates",
            "links",
            "markers",
            "payload",
            "provenance",
            "provenance_sha256",
            "status",
        }
        allowed = required | ({"ordinal"} if allow_ordinal else set())
        if set(value) != allowed:
            raise EvidenceIntegrityError(f"evidence envelope fields differ: {record.name}")
        if value["artifact"] != record.artifact_contract:
            raise EvidenceIntegrityError(f"artifact contract differs: {record.name}")
        if value["status"] != record.status:
            raise EvidenceIntegrityError(f"artifact status differs: {record.name}")
        if value["markers"] != list(record.markers):
            raise EvidenceIntegrityError(f"artifact markers differ: {record.name}")
        if value["links"] != dict(record.links):
            raise EvidenceIntegrityError(f"artifact links differ: {record.name}")
        expected_gates = {
            gate.gate.value: gate.as_dict() for gate in record.gates
        }
        if value["gates"] != expected_gates:
            raise EvidenceIntegrityError(f"semantic gates differ: {record.name}")
        if value["provenance"] != self.provenance.as_dict():
            raise EvidenceIntegrityError(f"provenance differs: {record.name}")
        if (
            value["provenance_sha256"] != self._provenance_sha256
            or record.provenance_sha256 != self._provenance_sha256
        ):
            raise EvidenceIntegrityError(f"provenance SHA-256 differs: {record.name}")

    def verify_artifact(self, name: str) -> PublishedEvidenceArtifact:
        """Re-read and authenticate one artifact and all of its direct links."""

        _require_text(name, label="artifact name")
        self._require_live_root()
        try:
            record = self._records[name]
        except KeyError as error:
            raise EvidenceIncomplete(f"evidence artifact is not registered: {name}") from error
        data = _read_regular_file(record.path)
        self._verify_record_bytes(record, data)
        for linked_name, linked_sha256 in record.links:
            linked = self._records.get(linked_name)
            if linked is None or linked.sha256 != linked_sha256:
                raise EvidenceIntegrityError(
                    f"evidence link graph differs: {record.name} -> {linked_name}"
                )
            linked_data = _read_regular_file(linked.path)
            if hashlib.sha256(linked_data).hexdigest() != linked_sha256:
                raise EvidenceIntegrityError(
                    f"linked evidence SHA-256 differs: {linked_name}"
                )
        return record

    def publish_level_complete(
        self,
        level: CapacityEvidenceLevel,
    ) -> PublishedEvidenceArtifact:
        """Publish one level marker only after all required inputs re-verify."""

        if not isinstance(level, CapacityEvidenceLevel):
            raise TypeError("level must be CapacityEvidenceLevel")
        self._trigger(EvidenceFaultPoint.BEFORE_REQUIRED_VERIFICATION)
        dependencies = (
            EvidenceArtifactName.WORKLOAD_MANIFEST,
            EvidenceArtifactName.NATIVE_LAYOUT_REPORT,
            _LEVEL_REPORTS[level],
        )
        links = self._dependency_links(dependencies)
        for dependency in dependencies:
            self._require_semantic_success(dependency)
        self._trigger(EvidenceFaultPoint.AFTER_REQUIRED_VERIFICATION)
        directory = self._require_level_directory(level)
        if any(directory.iterdir()):
            raise EvidenceIntegrityError(
                f"capacity level directory contains an unexpected entry: {directory}"
            )
        name = f"{directory.name}/COMPLETE"
        contract = "STORAGE_V4_PHASE_1C_CAPACITY_LEVEL_COMPLETE_V1"
        level_record = self._records[_LEVEL_REPORTS[level].value]
        data = canonical_json_bytes(
            {
                "artifact": contract,
                "gates": {
                    gate.gate.value: gate.as_dict() for gate in level_record.gates
                },
                "links": dict(links),
                "markers": list(PHASE1C_SYNTHETIC_MARKERS),
                "payload": {
                    "level": level.value,
                    "verified_report_status": level_record.status,
                },
                "provenance": self.provenance.as_dict(),
                "provenance_sha256": self._provenance_sha256,
                "status": LEVEL_COMPLETE_STATUS,
            }
        )
        self._trigger(EvidenceFaultPoint.BEFORE_LEVEL_COMPLETE)
        record = self._record_publication(
            name=name,
            target=directory / "COMPLETE",
            data=data,
            contract=contract,
            status=LEVEL_COMPLETE_STATUS,
            markers=PHASE1C_SYNTHETIC_MARKERS,
            links=links,
            provenance_sha256=self._provenance_sha256,
            gates=level_record.gates,
            jsonl=False,
        )
        self._trigger(EvidenceFaultPoint.AFTER_LEVEL_COMPLETE)
        return record

    def _terminal_required_names(self) -> tuple[str, ...]:
        reports = tuple(name.value for name in EvidenceArtifactName)
        levels = tuple(
            f"{_LEVEL_DIRECTORY_NAMES[level]}/COMPLETE"
            for level in CapacityEvidenceLevel
        )
        return (*reports, *levels)

    def _verify_exact_tree(self, *, terminal_exists: bool) -> None:
        expected_root_files = {name.value for name in EvidenceArtifactName}
        if terminal_exists:
            expected_root_files.add("COMPLETE")
        expected_directories = set(_LEVEL_DIRECTORY_NAMES.values())
        observed_files: set[str] = set()
        observed_directories: set[str] = set()
        for entry in self.output_root.iterdir():
            entry_stat = _path_stat(entry)
            if entry_stat is None or _is_link_or_reparse(entry_stat):
                raise EvidenceIntegrityError(
                    f"evidence tree contains an absent or reparse entry: {entry}"
                )
            if stat.S_ISREG(entry_stat.st_mode):
                observed_files.add(entry.name)
            elif stat.S_ISDIR(entry_stat.st_mode):
                observed_directories.add(entry.name)
            else:
                raise EvidenceIntegrityError(
                    f"evidence tree contains a non-regular entry: {entry}"
                )
        if observed_files != expected_root_files:
            raise EvidenceIncomplete(
                "evidence root file set differs from the terminal contract"
            )
        if observed_directories != expected_directories:
            raise EvidenceIncomplete(
                "capacity level directory set differs from the terminal contract"
            )
        for level, directory_name in _LEVEL_DIRECTORY_NAMES.items():
            directory = self._require_level_directory(level)
            entries = tuple(directory.iterdir())
            if len(entries) != 1 or entries[0].name != "COMPLETE":
                raise EvidenceIncomplete(
                    f"capacity level is not exactly complete: {directory_name}"
                )
            _read_regular_file(entries[0])

    def publish_terminal_complete(self, verdict: str) -> PublishedEvidenceArtifact:
        """Publish root COMPLETE only for a fully verified, exact evidence tree."""

        _require_text(verdict, label="verdict")
        if verdict not in TERMINAL_VERDICTS:
            raise ValueError("verdict is not an authorized complete Phase 1C verdict")
        if not self.provenance.has_golden_binding:
            raise EvidenceIncomplete("terminal COMPLETE requires complete Golden provenance")
        self._trigger(EvidenceFaultPoint.BEFORE_REQUIRED_VERIFICATION)
        required_names = self._terminal_required_names()
        missing = [name for name in required_names if name not in self._records]
        if missing:
            raise EvidenceIncomplete(
                "terminal evidence is incomplete: " + ", ".join(missing)
            )
        for name in required_names:
            record = self.verify_artifact(name)
            if record.provenance_sha256 != self._provenance_sha256:
                raise EvidenceIntegrityError(f"terminal provenance differs: {name}")
        for report_name in EvidenceArtifactName:
            self._require_semantic_success(report_name)
        for level in CapacityEvidenceLevel:
            marker_name = f"{_LEVEL_DIRECTORY_NAMES[level]}/COMPLETE"
            marker = self._records[marker_name]
            if marker.status != LEVEL_COMPLETE_STATUS:
                raise EvidenceIncomplete(f"capacity level status failed: {level.value}")
            level_report = self._records[_LEVEL_REPORTS[level].value]
            if marker.gates != level_report.gates:
                raise EvidenceIntegrityError(
                    f"capacity level gate binding differs: {level.value}"
                )
        self._verify_exact_tree(terminal_exists=False)
        self._trigger(EvidenceFaultPoint.AFTER_REQUIRED_VERIFICATION)
        links = tuple((name, self._records[name].sha256) for name in required_names)
        contract = "STORAGE_V4_PHASE_1C_COMPLETE_V1"
        semantic_reports = {
            name.value: {
                "gates": {
                    gate.gate.value: gate.as_dict()
                    for gate in self._records[name.value].gates
                },
                "status": self._records[name.value].status,
            }
            for name in EvidenceArtifactName
        }
        data = canonical_json_bytes(
            {
                "artifact": contract,
                "gates": {},
                "links": dict(links),
                "markers": list(PHASE1C_EVIDENCE_MARKERS),
                "payload": {
                    "capacity_levels": [level.value for level in CapacityEvidenceLevel],
                    "verified_semantic_reports": semantic_reports,
                },
                "provenance": self.provenance.as_dict(),
                "provenance_sha256": self._provenance_sha256,
                "status": verdict,
            }
        )
        self._trigger(EvidenceFaultPoint.BEFORE_TERMINAL_COMPLETE)
        record = self._record_publication(
            name="COMPLETE",
            target=self.output_root / "COMPLETE",
            data=data,
            contract=contract,
            status=verdict,
            markers=PHASE1C_EVIDENCE_MARKERS,
            links=links,
            provenance_sha256=self._provenance_sha256,
            gates=(),
            jsonl=False,
        )
        self._verify_exact_tree(terminal_exists=True)
        self._trigger(EvidenceFaultPoint.AFTER_TERMINAL_COMPLETE)
        return record

    def verify_all(self) -> tuple[PublishedEvidenceArtifact, ...]:
        """Re-read every registered artifact without accepting extra sidecars."""

        for record in self.records:
            self.verify_artifact(record.name)
        terminal_exists = "COMPLETE" in self._records
        if terminal_exists:
            self._verify_exact_tree(terminal_exists=True)
        return self.records


__all__ = [
    "LEVEL_COMPLETE_STATUS",
    "NOT_ALPHA_EVIDENCE",
    "NOT_ECONOMIC_EVIDENCE",
    "PAPER_ONLY",
    "PHASE1C_EVIDENCE_MARKERS",
    "PHASE1C_SYNTHETIC_MARKERS",
    "PROVENANCE_CONTRACT",
    "SYNTHETIC_CAPACITY_WORKLOAD",
    "TERMINAL_VERDICTS",
    "CapacityEvidenceLevel",
    "EvidenceAlreadyPublished",
    "EvidenceArtifactName",
    "EvidenceFaultHook",
    "EvidenceFaultPoint",
    "EvidenceIncomplete",
    "EvidenceIntegrityError",
    "EvidencePublicationError",
    "EvidenceReportStatus",
    "EvidenceSemanticGate",
    "Phase1CEvidenceProvenance",
    "Phase1CEvidencePublisher",
    "Phase1CEvidenceReport",
    "Phase1CGateEvidence",
    "PublishedEvidenceArtifact",
    "UnsafeEvidencePath",
]
