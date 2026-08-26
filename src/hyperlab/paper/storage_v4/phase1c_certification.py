"""End-to-end offline Storage V4 Phase 1C capacity certification.

The certifier composes independently verified Golden V3 input, native raw
references, bounded-tail restart cases, isolated capacity workers, and a
write-once evidence DAG.  It is deliberately PAPER_ONLY and has no network,
venue, credential, wallet, signer, order, or deployment surface.
"""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, localcontext
from pathlib import Path, PurePosixPath

from .anchor import LocalAnchor
from .candidate_tree import (
    CandidateFileWitness,
    CandidateTreeWitness,
    CandidateTreeWitnessError,
    witness_candidate_tree,
)
from .canonical import canonical_json_bytes
from .capacity import (
    ByteCategoryCensus,
    CapacityBytePaths,
    CapacityMeasurement,
    CapacityWorkloadManifest,
    StorageGrowthAssessment,
    assess_storage_growth,
    census_byte_categories,
    compute_capacity_scaling,
    iter_capacity_commits,
)
from .capacity_runner import (
    CumulativeCapacityBoundaryResult,
    CumulativeCapacityRunResult,
    OfflineCapacityRunEvidence,
    OfflinePhase1CCapacityRunner,
)
from .capacity_shape import GoldenCapacityShape, derive_golden_capacity_shape
from .golden_reattestation import (
    GOLDEN_NATIVE_IMPORTED_REATTESTATION_V1,
    GoldenNativeReattestationConfig,
    GoldenNativeReattestationResult,
    reattest_golden_native_candidate,
)
from .golden_runner import current_runtime_identity
from .phase1c_evidence import (
    CapacityEvidenceLevel,
    EvidenceArtifactName,
    EvidenceReportStatus,
    EvidenceSemanticGate,
    Phase1CEvidenceProvenance,
    Phase1CEvidencePublisher,
    Phase1CEvidenceReport,
    Phase1CGateEvidence,
)
from .phase1c_pipeline import Phase1CAuthorityStatus
from .phase1c_preflight import (
    Phase1CPostflightVerification,
    Phase1CPreflightConfig,
    Phase1CPreflightResult,
    run_phase1c_preflight,
    verify_phase1c_postflight,
)
from .phase1c_workers import (
    Phase1CCumulativeCapacityWorkerRequest,
    run_phase1c_cumulative_capacity_worker,
)
from .phase1c_workloads import (
    CANONICAL_TARGET_GIB_PER_HOUR,
    PHASE1C_ADVERSARIAL_COMMIT_COUNT,
    PHASE1C_PRODUCTION_BATCH_COMMITS,
    PHASE1C_PRODUCTION_CHECKPOINT_EVERY_BATCHES,
    PHASE1C_TAIL_COMMIT_COUNT,
    PHASE1C_TAIL_RESTART_SIZES,
    Phase1CBaselineRatioReport,
    Phase1CWorkloadProgress,
    Phase1CWorkloadSuite,
    build_phase1c_baseline_ratio_report,
    build_phase1c_workload_suite,
    decide_phase1c_target_verdict,
)
from .raw_reference import (
    RAW_REFERENCE_CONTRACT_MARKER_V2,
    RAW_REFERENCE_FORMAT_VERSION_V2,
)
from .raw_segment import raw_footer_index_physical_bytes
from .raw_store import RawStorePaths
from .repository import RepositoryPaths
from .startup_trace import StartupFileAccessTrace, StartupFileCategory
from .tail_runner import BoundedTailRestartMatrixReport, BoundedTailRestartMatrixRunner
from .types import Hash32

PHASE1C_PROVEN = "STORAGE_V4_PHASE_1C_NATIVE_CAPACITY_PROVEN"
PHASE1C_TARGET_NOT_MET = (
    "STORAGE_V4_PHASE_1C_NATIVE_CAPACITY_CHARACTERIZED_TARGET_NOT_MET"
)
PHASE1C_NO_CANONICAL_TARGET = (
    "STORAGE_V4_PHASE_1C_NATIVE_CAPACITY_CHARACTERIZED_NO_CANONICAL_TARGET"
)

PHASE1C_CODE_IDENTITY_FORMAT = "hyperlab-storage-v4-phase1c-code-identity-v1"
PHASE1C_TEST_WITNESS_FORMAT = "hyperlab-storage-v4-phase1c-targeted-tests-v1"
PHASE1C_CERTIFICATION_FORMAT = "hyperlab-storage-v4-phase1c-certification-v1"
PHASE1C_CAPACITY_LEVELS = (100_000, 500_000, 1_000_000)
PHASE1C_EXPECTED_NEW_INGESTED_COMMITS = 1_120_005
PHASE1C_HISTORICAL_ATTEMPT_INGESTION = (
    ("native-golden-03", 204_000, 216_000),
    ("native-golden-04", 352_267, 352_267),
    ("native-capacity-05", 1_120_005, 1_120_005),
)
PHASE1C_HEARTBEAT_MIN_SECONDS = 30.0
PHASE1C_HEARTBEAT_MAX_SECONDS = 60.0

_MAX_CODE_FILE_BYTES = 4 * 1024 * 1024
_CODE_FIXED_PATHS = (
    "pyproject.toml",
    "requirements-runtime.lock",
    "scripts/certify_storage_v4_phase1c.py",
    "scripts/generate_phase12_live_paper_artifacts.py",
)

ProgressCallback = Callable[[Mapping[str, object]], None]

_LEVEL_TO_ARTIFACT = {
    100_000: EvidenceArtifactName.CAPACITY_100K,
    500_000: EvidenceArtifactName.CAPACITY_500K,
    1_000_000: EvidenceArtifactName.CAPACITY_1M,
}
_LEVEL_TO_COMPLETE = {
    100_000: CapacityEvidenceLevel.CAPACITY_100K,
    500_000: CapacityEvidenceLevel.CAPACITY_500K,
    1_000_000: CapacityEvidenceLevel.CAPACITY_1M,
}

_LIMITATIONS = (
    "WINDOWS_NTFS_MEASUREMENT_NOT_LINUX_EXT4_CERTIFICATION",
    "NO_ROOT_OWNED_EXTERNAL_ANCHOR_PROOF",
    "NO_PRODUCTION_RAW_LAKE_BACKEND",
    "SYNTHETIC_WORKLOADS_ARE_NOT_MARKET_DATA_OR_ECONOMIC_EVIDENCE",
    "NO_VENUE_COVERAGE_OR_SHADOW_RUN",
    "GOLDEN_RAW_SOURCE_IS_CERTIFIED_CANONICAL_INBOX_JSONL_NOT_ORIGINAL_WIRE",
    "PEAK_RSS_IS_PROCESS_LIFETIME_HIGH_WATER_MARK",
    "WINDOWS_CUMULATIVE_WRITE_BYTES_IS_PROCESS_SCOPED_WHEN_AVAILABLE",
    "SCRATCH_PEAK_IS_EXACT_ONLY_AT_INSTRUMENTED_DURABILITY_BOUNDARIES",
    "RAW_CUMULATIVE_MANIFEST_CHAIN_AUTHENTICATION_IS_O_SEGMENTS_SQUARED",
    "FULL_HISTORY_AUDIT_IS_OFFLINE_O_N",
    "STARTUP_FILE_TRACE_IS_PYTHON_LEVEL_NOT_OS_KERNEL_ETW_OR_SQLITE_VFS",
    "GOLDEN_NATIVE_IS_IMPORTED_READ_ONLY_REATTESTATION_NOT_FRESH_INGESTION",
    "GOLDEN_IMPORTED_TIMINGS_ARE_REATTESTATION_TIMINGS_NOT_RECOVERED_PRODUCER_TIMINGS",
    "CAPACITY_100K_500K_1M_ARE_AUTHENTICATED_PREFIXES_OF_ONE_SHARED_CUMULATIVE_RUN",
    "INTERPROCESS_RESUME_IS_BOUND_TO_AUTHENTICATED_BOUNDARY_CERTIFICATES_AND_TARGETED_PROCESS_CUT_EVIDENCE",
)


class Phase1CCertificationError(RuntimeError):
    """A blocking integrity or required-measurement failure."""


def _require_text(value: str, *, label: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{label} must be non-empty text")
    value.encode("utf-8", errors="strict")
    return value


def _require_sha256(value: str, *, label: str) -> str:
    _require_text(value, label=label)
    if len(value) != 64 or value != value.lower() or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return value


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_sha256(value: Mapping[str, object]) -> str:
    return _sha256(canonical_json_bytes(value))


def _is_link_or_reparse(path: Path) -> bool:
    if path.is_symlink():
        return True
    try:
        value = os.lstat(path)
    except OSError:
        return True
    attributes = int(getattr(value, "st_file_attributes", 0))
    mask = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & mask)


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
    )


def _safe_code_path(repository_root: Path, relative_path: str) -> Path:
    pure = PurePosixPath(relative_path)
    if (
        not relative_path
        or "\\" in relative_path
        or pure.is_absolute()
        or pure.as_posix() != relative_path
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        raise Phase1CCertificationError("code identity contains an unsafe path")
    cursor = repository_root
    for part in pure.parts:
        cursor = cursor / part
        if _is_link_or_reparse(cursor):
            raise Phase1CCertificationError(
                f"code identity path traverses a link/reparse point: {relative_path}"
            )
    resolved = cursor.resolve(strict=True)
    try:
        resolved.relative_to(repository_root)
    except ValueError as error:
        raise Phase1CCertificationError("code identity path escapes repository") from error
    if not resolved.is_file():
        raise Phase1CCertificationError(
            f"code identity path is not a regular file: {relative_path}"
        )
    return resolved


def _read_stable_code_file(path: Path) -> bytes:
    before = path.stat(follow_symlinks=False)
    if before.st_size > _MAX_CODE_FILE_BYTES:
        raise Phase1CCertificationError(f"code file exceeds size bound: {path}")
    with path.open("rb") as stream:
        opened_before = os.fstat(stream.fileno())
        payload = stream.read(_MAX_CODE_FILE_BYTES + 1)
        opened_after = os.fstat(stream.fileno())
    after = path.stat(follow_symlinks=False)
    identities = {
        _stat_identity(before),
        _stat_identity(opened_before),
        _stat_identity(opened_after),
        _stat_identity(after),
    }
    if len(payload) > _MAX_CODE_FILE_BYTES or len(identities) != 1:
        raise Phase1CCertificationError(f"code file changed while hashed: {path}")
    return payload


@dataclass(frozen=True, slots=True)
class Phase1CCodeIdentity:
    repository_root: Path
    files: tuple[tuple[str, int, str], ...]
    sha256: str

    def __post_init__(self) -> None:
        if not self.repository_root.is_absolute():
            raise ValueError("repository_root must be absolute")
        if not self.files:
            raise ValueError("code identity requires files")
        if tuple(sorted(self.files)) != self.files:
            raise ValueError("code identity files must be sorted")
        if len({item[0] for item in self.files}) != len(self.files):
            raise ValueError("code identity paths must be unique")
        for path, size, digest in self.files:
            _require_text(path, label="code path")
            if type(size) is not int or size < 0:
                raise ValueError("code file size must be non-negative")
            _require_sha256(digest, label="code file SHA-256")
        _require_sha256(self.sha256, label="code identity SHA-256")
        if self.sha256 != _canonical_sha256(self.payload_without_sha256()):
            raise ValueError("code identity SHA-256 differs from its manifest")

    def payload_without_sha256(self) -> dict[str, object]:
        return {
            "files": {
                path: {"bytes": size, "sha256": digest}
                for path, size, digest in self.files
            },
            "format": PHASE1C_CODE_IDENTITY_FORMAT,
            "repository_root": str(self.repository_root),
        }

    def payload(self) -> dict[str, object]:
        return {**self.payload_without_sha256(), "sha256": self.sha256}


def compute_phase1c_code_identity(repository_root: Path) -> Phase1CCodeIdentity:
    """Hash every HyperLab Python source plus fixed certifier/runtime inputs."""

    if not isinstance(repository_root, Path) or not repository_root.is_absolute():
        raise ValueError("repository_root must be an absolute pathlib.Path")
    root = repository_root.resolve(strict=True)
    if _is_link_or_reparse(root) or not root.is_dir():
        raise Phase1CCertificationError("repository root is not a regular directory")
    source_root = _safe_code_path(root, "src/hyperlab/__init__.py").parent
    selected = set(_CODE_FIXED_PATHS)
    for path in source_root.rglob("*.py"):
        if path.is_file():
            selected.add(path.relative_to(root).as_posix())
    relative_paths = tuple(sorted(selected))
    files: list[tuple[str, int, str]] = []
    for relative_path in relative_paths:
        payload = _read_stable_code_file(_safe_code_path(root, relative_path))
        files.append((relative_path, len(payload), _sha256(payload)))
    material = {
        "files": {
            path: {"bytes": size, "sha256": digest}
            for path, size, digest in files
        },
        "format": PHASE1C_CODE_IDENTITY_FORMAT,
        "repository_root": str(root),
    }
    return Phase1CCodeIdentity(
        repository_root=root,
        files=tuple(files),
        sha256=_canonical_sha256(material),
    )


@dataclass(frozen=True, slots=True)
class Phase1CTestWitness:
    """Canonical proof that the prescribed targeted suite exited successfully."""

    command: tuple[str, ...]
    exit_code: int
    output_sha256: str
    source_files: tuple[tuple[str, str], ...]
    summary: str
    output_log_path: str | None = None
    output_log_size_bytes: int | None = None

    def __post_init__(self) -> None:
        if not self.command or any(type(item) is not str or not item for item in self.command):
            raise ValueError("targeted test command must be a non-empty string tuple")
        if type(self.exit_code) is not int or self.exit_code != 0:
            raise Phase1CCertificationError("targeted Phase 1C tests did not pass")
        _require_sha256(self.output_sha256, label="targeted test output SHA-256")
        if not self.source_files or tuple(sorted(self.source_files)) != self.source_files:
            raise ValueError("targeted test source witnesses must be non-empty and sorted")
        if len({path for path, _ in self.source_files}) != len(self.source_files):
            raise ValueError("targeted test source paths must be unique")
        for path, digest in self.source_files:
            _require_text(path, label="targeted test source path")
            _require_sha256(digest, label="targeted test source SHA-256")
        _require_text(self.summary, label="targeted test summary")
        if (self.output_log_path is None) != (self.output_log_size_bytes is None):
            raise ValueError("targeted test log path and size must be both present or absent")
        if self.output_log_path is not None:
            if not Path(self.output_log_path).is_absolute():
                raise ValueError("targeted test output log path must be absolute")
            if type(self.output_log_size_bytes) is not int or self.output_log_size_bytes < 0:
                raise ValueError("targeted test output log size must be non-negative")

    def payload(self) -> dict[str, object]:
        return {
            "command": list(self.command),
            "exit_code": self.exit_code,
            "format": PHASE1C_TEST_WITNESS_FORMAT,
            "output_sha256": self.output_sha256,
            "output_log_path": self.output_log_path,
            "output_log_size_bytes": self.output_log_size_bytes,
            "source_files": dict(self.source_files),
            "summary": self.summary,
        }

    @property
    def sha256(self) -> str:
        return _canonical_sha256(self.payload())


def make_semantic_gate(
    gate: EvidenceSemanticGate,
    *,
    certifier_contract: str,
    result: Mapping[str, object],
    passed: bool,
) -> Phase1CGateEvidence:
    """Bind one explicit independent result to the evidence gate."""

    if not isinstance(gate, EvidenceSemanticGate):
        raise TypeError("gate must be EvidenceSemanticGate")
    _require_text(certifier_contract, label="certifier_contract")
    if type(passed) is not bool:
        raise TypeError("passed must be an exact bool")
    result_payload = dict(result)
    result_payload["certifier_contract"] = certifier_contract
    result_payload["passed"] = passed
    return Phase1CGateEvidence(
        gate=gate,
        passed=passed,
        certifier_contract=certifier_contract,
        certifier_result_sha256=_canonical_sha256(result_payload),
    )


def phase1c_test_source_witnesses(
    repository_root: Path,
    relative_paths: Iterable[str],
) -> tuple[tuple[str, str], ...]:
    """Hash the exact targeted tests bound into a runtime witness."""

    root = repository_root.resolve(strict=True)
    selected = tuple(sorted(set(relative_paths)))
    if not selected:
        raise ValueError("targeted test source list must not be empty")
    return tuple(
        (
            relative_path,
            _sha256(_read_stable_code_file(_safe_code_path(root, relative_path))),
        )
        for relative_path in selected
    )


def _decimal_ratio(numerator: int, denominator: int) -> str:
    if type(numerator) is not int or numerator < 0:
        raise ValueError("ratio numerator must be non-negative")
    if type(denominator) is not int or denominator <= 0:
        raise ValueError("ratio denominator must be positive")
    with localcontext() as context:
        context.prec = 50
        rendered = format(Decimal(numerator) / Decimal(denominator), ".12f")
    return rendered.rstrip("0").rstrip(".") or "0"


def require_exact_capacity_levels(values: Sequence[int]) -> tuple[int, int, int]:
    """Forbid silently shrinking or substituting the production staircase."""

    observed = tuple(values)
    if observed != PHASE1C_CAPACITY_LEVELS:
        raise ValueError("capacity staircase must be exactly 100k, 500k, and 1m")
    return PHASE1C_CAPACITY_LEVELS


def _validate_historical_attempt_ingestion(
    value: tuple[tuple[str, int, int], ...],
) -> None:
    if type(value) is not tuple:
        raise TypeError("historical attempt ingestion must be an exact tuple")
    labels: set[str] = set()
    for entry in value:
        if type(entry) is not tuple or len(entry) != 3:
            raise TypeError(
                "historical attempt ingestion entries must be "
                "(label, paper_commits_sealed, raw_records_published) tuples"
            )
        label, paper_commits_sealed, raw_records_published = entry
        _require_text(label, label="historical attempt label")
        if label in labels:
            raise ValueError("historical attempt labels must be unique")
        for count_label, count in (
            ("paper_commits_sealed", paper_commits_sealed),
            ("raw_records_published", raw_records_published),
        ):
            if type(count) is not int or count < 0:
                raise ValueError(
                    f"historical attempt {count_label} must be non-negative"
                )
        if raw_records_published < paper_commits_sealed:
            raise Phase1CCertificationError(
                "historical raw publication count cannot trail sealed Paper commits"
            )
        labels.add(label)


def _require_canonical_historical_attempt_ingestion(
    value: tuple[tuple[str, int, int], ...],
) -> None:
    _validate_historical_attempt_ingestion(value)
    if value != PHASE1C_HISTORICAL_ATTEMPT_INGESTION:
        raise Phase1CCertificationError(
            "historical attempt ingestion differs from authenticated prior-attempt counts"
        )


@dataclass(frozen=True, slots=True)
class Phase1CCommandWitness:
    """Canonical evidence for one required local closure command."""

    purpose: str
    command: tuple[str, ...]
    exit_code: int
    output_sha256: str
    summary: str
    output_log_path: str | None = None
    output_log_size_bytes: int | None = None

    def __post_init__(self) -> None:
        _require_text(self.purpose, label="command purpose")
        if not self.command or any(type(item) is not str or not item for item in self.command):
            raise ValueError("closure command must be a non-empty string tuple")
        if type(self.exit_code) is not int or self.exit_code != 0:
            raise Phase1CCertificationError(
                f"required closure command failed: {self.purpose}"
            )
        _require_sha256(self.output_sha256, label="closure command output SHA-256")
        _require_text(self.summary, label="closure command summary")
        if (self.output_log_path is None) != (self.output_log_size_bytes is None):
            raise ValueError("closure log path and size must be both present or absent")
        if self.output_log_path is not None:
            if not Path(self.output_log_path).is_absolute():
                raise ValueError("closure output log path must be absolute")
            if type(self.output_log_size_bytes) is not int or self.output_log_size_bytes < 0:
                raise ValueError("closure output log size must be non-negative")

    def payload(self) -> dict[str, object]:
        return {
            "command": list(self.command),
            "exit_code": self.exit_code,
            "output_sha256": self.output_sha256,
            "output_log_path": self.output_log_path,
            "output_log_size_bytes": self.output_log_size_bytes,
            "purpose": self.purpose,
            "summary": self.summary,
        }


_CLOSURE_PURPOSES = (
    "V10_GENERATE_FIRST",
    "V10_CHECK_FIRST",
    "V10_GENERATE_SECOND",
    "V10_CHECK_SECOND",
    "PHASE05_GENERATE",
    "PHASE05_CHECK",
    "PYTEST_GLOBAL_FINAL_SINGLE_RUN",
    "RUFF_GLOBAL_FINAL",
    "MYPY_HYPERLAB_FINAL",
    "GIT_DIFF_CHECK_FINAL",
)


@dataclass(frozen=True, slots=True)
class Phase1CV9ByteWitness:
    path: str
    size_bytes: int
    before_sha256: str
    after_sha256: str

    def __post_init__(self) -> None:
        _require_text(self.path, label="V9 attestation path")
        if type(self.size_bytes) is not int or self.size_bytes < 1:
            raise ValueError("V9 attestation size must be positive")
        _require_sha256(self.before_sha256, label="V9 before SHA-256")
        _require_sha256(self.after_sha256, label="V9 after SHA-256")
        if self.before_sha256 != self.after_sha256:
            raise Phase1CCertificationError("V9 attestation changed during Phase 1C closure")

    def payload(self) -> dict[str, object]:
        return {
            "after_sha256": self.after_sha256,
            "before_sha256": self.before_sha256,
            "byte_identical": True,
            "path": self.path,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class Phase1CClosureWitness:
    """All repository closure gates required before terminal COMPLETE."""

    commands: tuple[Phase1CCommandWitness, ...]
    v9: Phase1CV9ByteWitness

    def __post_init__(self) -> None:
        if type(self.commands) is not tuple or not all(
            isinstance(item, Phase1CCommandWitness) for item in self.commands
        ):
            raise TypeError("closure commands must be Phase1CCommandWitness values")
        if tuple(item.purpose for item in self.commands) != _CLOSURE_PURPOSES:
            raise Phase1CCertificationError(
                "closure commands are missing, duplicated, or out of canonical order"
            )
        if not isinstance(self.v9, Phase1CV9ByteWitness):
            raise TypeError("closure V9 witness must be Phase1CV9ByteWitness")

    def payload(self) -> dict[str, object]:
        return {
            "commands": [item.payload() for item in self.commands],
            "format": "hyperlab-storage-v4-phase1c-closure-v1",
            "global_pytest_runs": 1,
            "status": "STORAGE_V4_PHASE_1C_REPOSITORY_CLOSURE_VERIFIED",
            "v9": self.v9.payload(),
        }

    @property
    def sha256(self) -> str:
        return _canonical_sha256(self.payload())


@dataclass(frozen=True, slots=True)
class Phase1CCertificationConfig:
    repository_root: Path
    preflight: Phase1CPreflightConfig
    targeted_tests: Phase1CTestWitness
    golden_producer_candidate_root: Path
    golden_producer_stdout_log: Path
    golden_producer_stderr_log: Path
    golden_producer_stdout_sha256: str
    golden_producer_stderr_sha256: str
    historical_attempt_ingestion: tuple[tuple[str, int, int], ...]
    cumulative_resume_candidate_root: Path | None = None
    heartbeat_interval_seconds: float = PHASE1C_HEARTBEAT_MIN_SECONDS

    def __post_init__(self) -> None:
        if not isinstance(self.repository_root, Path) or not self.repository_root.is_absolute():
            raise ValueError("repository_root must be an absolute pathlib.Path")
        if not isinstance(self.preflight, Phase1CPreflightConfig):
            raise TypeError("preflight must be Phase1CPreflightConfig")
        if not isinstance(self.targeted_tests, Phase1CTestWitness):
            raise TypeError("targeted_tests must be Phase1CTestWitness")
        for label, path in (
            ("golden_producer_candidate_root", self.golden_producer_candidate_root),
            ("golden_producer_stdout_log", self.golden_producer_stdout_log),
            ("golden_producer_stderr_log", self.golden_producer_stderr_log),
        ):
            if not isinstance(path, Path) or not path.is_absolute():
                raise ValueError(f"{label} must be an absolute pathlib.Path")
        _require_sha256(
            self.golden_producer_stdout_sha256,
            label="Golden producer stdout SHA-256",
        )
        _require_sha256(
            self.golden_producer_stderr_sha256,
            label="Golden producer stderr SHA-256",
        )
        if self.cumulative_resume_candidate_root is not None and (
            not isinstance(self.cumulative_resume_candidate_root, Path)
            or not self.cumulative_resume_candidate_root.is_absolute()
        ):
            raise ValueError(
                "cumulative_resume_candidate_root must be an absolute pathlib.Path or None"
            )
        _require_canonical_historical_attempt_ingestion(
            self.historical_attempt_ingestion
        )
        interval = self.heartbeat_interval_seconds
        if (
            type(interval) not in (int, float)
            or not PHASE1C_HEARTBEAT_MIN_SECONDS
            <= float(interval)
            <= PHASE1C_HEARTBEAT_MAX_SECONDS
        ):
            raise ValueError("heartbeat interval must be between 30 and 60 seconds")


Phase1CCandidateFileWitness = CandidateFileWitness
Phase1CCandidateTreeWitness = CandidateTreeWitness


@dataclass(frozen=True, slots=True)
class Phase1CCommitAccounting:
    """Exact Paper/raw accounting, separate from read-only Golden auditing."""

    golden_commits_audited: int
    golden_commits_ingested: int
    golden_prefix_commits_reingested: int
    tail_commits_ingested: int
    adversarial_commits_ingested: int
    cumulative_commits_ingested: int
    cumulative_prefix_commits_reingested: int
    historical_attempt_ingestion: tuple[tuple[str, int, int], ...] = ()

    def __post_init__(self) -> None:
        for label, value in (
            ("golden_commits_audited", self.golden_commits_audited),
            ("golden_commits_ingested", self.golden_commits_ingested),
            (
                "golden_prefix_commits_reingested",
                self.golden_prefix_commits_reingested,
            ),
            ("tail_commits_ingested", self.tail_commits_ingested),
            ("adversarial_commits_ingested", self.adversarial_commits_ingested),
            ("cumulative_commits_ingested", self.cumulative_commits_ingested),
            (
                "cumulative_prefix_commits_reingested",
                self.cumulative_prefix_commits_reingested,
            ),
        ):
            if type(value) is not int or value < 0:
                raise TypeError(f"{label} must be a non-negative exact integer")
        if self.golden_commits_audited != 252_262:
            raise Phase1CCertificationError("Golden audited commit count must be 252262")
        if self.golden_commits_ingested != 0:
            raise Phase1CCertificationError("imported Golden must ingest zero commits")
        if self.golden_prefix_commits_reingested != 0:
            raise Phase1CCertificationError("imported Golden prefix must not be reingested")
        if self.tail_commits_ingested != (
            PHASE1C_TAIL_COMMIT_COUNT * len(PHASE1C_TAIL_RESTART_SIZES)
        ):
            raise Phase1CCertificationError("tail matrix ingestion count differs")
        if self.adversarial_commits_ingested != PHASE1C_ADVERSARIAL_COMMIT_COUNT:
            raise Phase1CCertificationError("adversarial ingestion count differs")
        if self.cumulative_commits_ingested != PHASE1C_CAPACITY_LEVELS[-1]:
            raise Phase1CCertificationError("cumulative ingestion count differs")
        if self.cumulative_prefix_commits_reingested != 0:
            raise Phase1CCertificationError("cumulative prefixes must not be reingested")
        _validate_historical_attempt_ingestion(self.historical_attempt_ingestion)
        if (
            self.current_mission_paper_commits_sealed
            != PHASE1C_EXPECTED_NEW_INGESTED_COMMITS
            or self.current_mission_raw_records_published
            != PHASE1C_EXPECTED_NEW_INGESTED_COMMITS
        ):
            raise Phase1CCertificationError(
                "current mission Paper/raw accounting must both equal 1120005"
            )

    @property
    def current_mission_paper_commits_sealed(self) -> int:
        return (
            self.golden_commits_ingested
            + self.tail_commits_ingested
            + self.adversarial_commits_ingested
            + self.cumulative_commits_ingested
        )

    @property
    def current_mission_raw_records_published(self) -> int:
        # Every current synthetic candidate passed exact raw/Paper/native audits.
        return self.current_mission_paper_commits_sealed

    @property
    def historical_paper_commits_sealed(self) -> int:
        return sum(
            paper_commits_sealed
            for _label, paper_commits_sealed, _raw_records_published in (
                self.historical_attempt_ingestion
            )
        )

    @property
    def historical_raw_records_published(self) -> int:
        return sum(
            raw_records_published
            for _label, _paper_commits_sealed, raw_records_published in (
                self.historical_attempt_ingestion
            )
        )

    @property
    def historical_raw_only_unsealed_records(self) -> int:
        return self.historical_raw_records_published - self.historical_paper_commits_sealed

    @property
    def all_attempts_paper_commits_sealed(self) -> int:
        return (
            self.historical_paper_commits_sealed
            + self.current_mission_paper_commits_sealed
        )

    @property
    def all_attempts_raw_records_published(self) -> int:
        return (
            self.historical_raw_records_published
            + self.current_mission_raw_records_published
        )

    @property
    def all_attempts_raw_only_unsealed_records(self) -> int:
        return (
            self.all_attempts_raw_records_published
            - self.all_attempts_paper_commits_sealed
        )

    def payload(self) -> dict[str, object]:
        return {
            "all_attempts_totals": {
                "paper_commits_sealed": self.all_attempts_paper_commits_sealed,
                "raw_only_unsealed_records": (
                    self.all_attempts_raw_only_unsealed_records
                ),
                "raw_records_published": self.all_attempts_raw_records_published,
            },
            "current_mission": {
                "cumulative_prefix_commits_reingested": (
                    self.cumulative_prefix_commits_reingested
                ),
                "golden_commits_audited": self.golden_commits_audited,
                "golden_reattestation_paper_commits_sealed": (
                    self.golden_commits_ingested
                ),
                "golden_prefix_commits_reingested": (
                    self.golden_prefix_commits_reingested
                ),
                "golden_reattestation_raw_records_published": 0,
                "paper_commits_sealed": self.current_mission_paper_commits_sealed,
                "raw_only_unsealed_records": 0,
                "raw_records_published": (
                    self.current_mission_raw_records_published
                ),
                "workloads": {
                    "adversarial_commits_ingested": (
                        self.adversarial_commits_ingested
                    ),
                    "cumulative_commits_ingested": self.cumulative_commits_ingested,
                    "tail_commits_ingested": self.tail_commits_ingested,
                },
            },
            "historical_attempts": [
                {
                    "label": label,
                    "paper_commits_sealed": paper_commits_sealed,
                    "raw_only_unsealed_records": (
                        raw_records_published - paper_commits_sealed
                    ),
                    "raw_records_published": raw_records_published,
                }
                for label, paper_commits_sealed, raw_records_published in (
                    self.historical_attempt_ingestion
                )
            ],
            "historical_totals": {
                "paper_commits_sealed": self.historical_paper_commits_sealed,
                "raw_only_unsealed_records": (
                    self.historical_raw_only_unsealed_records
                ),
                "raw_records_published": self.historical_raw_records_published,
            },
            "scope": (
                "EXACT_DURABLE_PAPER_AND_RAW_COUNTS; GOLDEN_READ_ONLY_AUDIT_IS_NOT_INGESTION; "
                "AUTHENTICATED_CUMULATIVE_PREFIX_IS_COUNTED_ONCE_NOT_AS_A_SEPARATE_ATTEMPT"
            ),
        }


@dataclass(frozen=True, slots=True)
class Phase1CMeasurementBundle:
    preflight: Phase1CPreflightResult
    code_identity: Phase1CCodeIdentity
    runtime_identity: Hash32
    golden_shape: GoldenCapacityShape
    workloads: Phase1CWorkloadSuite
    golden: GoldenNativeReattestationResult
    golden_byte_census: ByteCategoryCensus
    tail: BoundedTailRestartMatrixReport
    adversarial_measurement: CapacityMeasurement
    adversarial_evidence: OfflineCapacityRunEvidence
    cumulative_capacity: CumulativeCapacityRunResult
    accounting: Phase1CCommitAccounting

    @property
    def capacity_boundaries(self) -> dict[int, CumulativeCapacityBoundaryResult]:
        return {
            boundary.manifest.commit_count: boundary
            for boundary in self.cumulative_capacity.typed_boundaries
        }

    @property
    def level_measurements(self) -> tuple[tuple[int, CapacityMeasurement], ...]:
        return tuple(
            (level, self.capacity_boundaries[level].measurement)
            for level in PHASE1C_CAPACITY_LEVELS
        )

    @property
    def level_evidence(self) -> tuple[tuple[int, OfflineCapacityRunEvidence], ...]:
        return tuple(
            (level, self.capacity_boundaries[level].evidence)
            for level in PHASE1C_CAPACITY_LEVELS
        )


@dataclass(frozen=True, slots=True)
class Phase1CCertificationResult:
    verdict: str
    mission_root: Path
    evidence_root: Path
    code_identity: Phase1CCodeIdentity
    runtime_identity: Hash32
    closure: Phase1CClosureWitness
    postflight: Phase1CPostflightVerification
    artifacts: tuple[tuple[str, str, int], ...]

    def payload(self) -> dict[str, object]:
        return {
            "artifacts": [
                {"name": name, "sha256": digest, "size_bytes": size}
                for name, digest, size in self.artifacts
            ],
            "closure_sha256": self.closure.sha256,
            "code_identity": self.code_identity.payload(),
            "evidence_root": str(self.evidence_root),
            "format": PHASE1C_CERTIFICATION_FORMAT,
            "mission_root": str(self.mission_root),
            "postflight": self.postflight.to_dict(),
            "runtime_identity": self.runtime_identity.hex(),
            "verdict": self.verdict,
        }


ClosureRunner = Callable[[Path], Phase1CClosureWitness]


def _emit(progress: ProgressCallback | None, **payload: object) -> None:
    if progress is not None:
        progress(payload)


def _workload_manifest_progress_payload(
    event: Phase1CWorkloadProgress,
) -> dict[str, object]:
    if not isinstance(event, Phase1CWorkloadProgress):
        raise TypeError("event must be Phase1CWorkloadProgress")
    return {
        "checkpoint_count": 0,
        "commits_completed": event.processed_commits,
        "commits_total": event.total_commits,
        "logical_rows_completed": event.processed_logical_rows,
        "logical_rows_total": event.total_logical_rows,
        "manifest_label": event.manifest_label,
        "paper_segment_count": 0,
        "phase": "phase1c_workload_manifest",
        "processed_commits": event.processed_commits,
        "profile": event.profile.value,
        "progress_metrics_scope": (
            "CURRENT_CERTIFIER_PROCESS_MANIFEST_BUILD_AT_COMPLETED_PROGRESS_BOUNDARY"
        ),
        "raw_segment_count": 0,
        "segment_checkpoint_status": (
            "EXACT_NOT_APPLICABLE_NO_STORAGE_PUBLICATION"
        ),
        "segment_count": 0,
        "status": event.status.value,
        "total_commits": event.total_commits,
        "workload": "SYNTHETIC_CAPACITY_MANIFEST_BUILD",
        "workload_elapsed_ns": event.workload_elapsed_ns,
        "workload_id": f"phase1c-manifest:{event.manifest_label}",
        "workload_profile": event.profile.value,
    }


def _hash_candidate_file(path: Path) -> tuple[int, str]:
    before = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode) or _is_link_or_reparse(path):
        raise Phase1CCertificationError(f"candidate contains an unsafe file: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        opened_before = os.fstat(stream.fileno())
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
        opened_after = os.fstat(stream.fileno())
    after = path.stat(follow_symlinks=False)
    if len(
        {
            _stat_identity(before),
            _stat_identity(opened_before),
            _stat_identity(opened_after),
            _stat_identity(after),
        }
    ) != 1:
        raise Phase1CCertificationError(f"candidate file changed while hashed: {path}")
    return before.st_size, digest.hexdigest()


def _verify_persisted_output_log(
    *,
    path_text: str | None,
    expected_size: int | None,
    expected_sha256: str,
    label: str,
) -> dict[str, object]:
    if path_text is None or expected_size is None:
        raise Phase1CCertificationError(f"{label} has no durable output log")
    path = Path(path_text)
    if not path.is_absolute():
        raise Phase1CCertificationError(f"{label} output log path is not absolute")
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise Phase1CCertificationError(f"{label} output log is missing") from error
    if resolved != path or _is_link_or_reparse(path):
        raise Phase1CCertificationError(f"{label} output log path is unsafe")
    size, digest = _hash_candidate_file(path)
    if size != expected_size or digest != expected_sha256:
        raise Phase1CCertificationError(f"{label} output log bytes changed")
    return {"path": path_text, "sha256": digest, "size_bytes": size}


def _verify_all_output_logs(
    targeted_tests: Phase1CTestWitness,
    closure: Phase1CClosureWitness,
) -> dict[str, object]:
    return {
        "closure": [
            _verify_persisted_output_log(
                path_text=command.output_log_path,
                expected_size=command.output_log_size_bytes,
                expected_sha256=command.output_sha256,
                label=command.purpose,
            )
            for command in closure.commands
        ],
        "targeted_tests": _verify_persisted_output_log(
            path_text=targeted_tests.output_log_path,
            expected_size=targeted_tests.output_log_size_bytes,
            expected_sha256=targeted_tests.output_sha256,
            label="targeted Phase 1C tests",
        ),
    }


def _verify_golden_producer_logs(
    config: Phase1CCertificationConfig,
    result: GoldenNativeReattestationResult,
) -> dict[str, object]:
    producer = result.producer
    expected = (
        (
            "stdout",
            config.golden_producer_stdout_log,
            config.golden_producer_stdout_sha256,
            producer.stdout,
        ),
        (
            "stderr",
            config.golden_producer_stderr_log,
            config.golden_producer_stderr_sha256,
            producer.stderr,
        ),
    )
    verified: dict[str, object] = {}
    for label, path, digest, witness in expected:
        if witness.path != path or witness.sha256 != digest:
            raise Phase1CCertificationError(
                f"Golden producer {label} provenance differs from configuration"
            )
        verified[label] = _verify_persisted_output_log(
            path_text=str(path),
            expected_size=witness.size_bytes,
            expected_sha256=digest,
            label=f"Golden producer {label}",
        )
    return verified


def witness_phase1c_candidate_tree(
    root: Path,
    *,
    progress: ProgressCallback | None = None,
    heartbeat_interval_seconds: float = PHASE1C_HEARTBEAT_MIN_SECONDS,
) -> Phase1CCandidateTreeWitness:
    """Hash one complete immutable candidate without following links."""

    try:
        return witness_candidate_tree(
            root,
            progress=progress,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )
    except CandidateTreeWitnessError as error:
        raise Phase1CCertificationError(str(error)) from error


_REQUIRED_STARTUP_TRACE_CATEGORIES = frozenset(
    {
        StartupFileCategory.RAW_MANIFEST,
        StartupFileCategory.PAPER_MANIFEST,
        StartupFileCategory.PAPER_CHECKPOINT,
        StartupFileCategory.PAPER_OVERLAY,
        StartupFileCategory.RAW_ANCHOR,
        StartupFileCategory.PAPER_ANCHOR,
    }
)


def _validate_startup_file_trace(
    trace: StartupFileAccessTrace,
    *,
    expected_candidate_root: Path,
    label: str,
) -> dict[str, object]:
    if not isinstance(trace, StartupFileAccessTrace):
        raise TypeError(f"{label} startup trace must be StartupFileAccessTrace")
    if trace.candidate_root != expected_candidate_root:
        raise Phase1CCertificationError(
            f"{label} startup trace candidate root differs from its run"
        )
    observed_categories = {item.category for item in trace.opens}
    segment_categories = {
        StartupFileCategory.RAW_HISTORICAL_SEGMENT,
        StartupFileCategory.PAPER_HISTORICAL_SEGMENT,
    }
    if (
        not trace.opens
        or trace.historical_segment_open_count != 0
        or observed_categories & segment_categories
        or not _REQUIRED_STARTUP_TRACE_CATEGORIES.issubset(observed_categories)
        or any("/segments/" in item.relative_path for item in trace.opens)
    ):
        raise Phase1CCertificationError(
            f"{label} startup file trace is incomplete or opened historical segments"
        )
    payload = trace.payload()
    if (
        payload.get("historical_segment_open_count") != 0
        or payload.get("historical_segment_paths_opened") != []
    ):
        raise Phase1CCertificationError(
            f"{label} startup trace payload contradicts its typed evidence"
        )
    return {
        "label": label,
        "trace": payload,
        "verified": True,
    }


def _validate_effective_identities(
    *,
    observed_code_identity: str,
    observed_runtime_identity: str,
    expected_code_identity: Hash32,
    expected_runtime_identity: Hash32,
    label: str,
) -> dict[str, object]:
    if type(expected_code_identity) is not Hash32:
        raise TypeError("expected_code_identity must be Hash32")
    if type(expected_runtime_identity) is not Hash32:
        raise TypeError("expected_runtime_identity must be Hash32")
    if observed_code_identity != expected_code_identity.hex():
        raise Phase1CCertificationError(
            f"{label} code identity differs from the frozen certifier identity"
        )
    if observed_runtime_identity != expected_runtime_identity.hex():
        raise Phase1CCertificationError(
            f"{label} runtime identity differs from the frozen certifier identity"
        )
    return {
        "code_identity": observed_code_identity,
        "runtime_identity": observed_runtime_identity,
        "verified": True,
    }


def _validate_capacity_result(
    manifest: CapacityWorkloadManifest,
    measurement: CapacityMeasurement,
    evidence: OfflineCapacityRunEvidence,
    *,
    expected_code_identity: Hash32,
    expected_runtime_identity: Hash32,
) -> dict[str, object]:
    if not isinstance(manifest, CapacityWorkloadManifest):
        raise TypeError("capacity manifest must be CapacityWorkloadManifest")
    if not isinstance(measurement, CapacityMeasurement):
        raise TypeError("capacity measurement must be CapacityMeasurement")
    if not isinstance(evidence, OfflineCapacityRunEvidence):
        raise TypeError("capacity evidence must be OfflineCapacityRunEvidence")
    certification = evidence.certification
    startup_file_trace = _validate_startup_file_trace(
        evidence.startup_file_trace,
        expected_candidate_root=evidence.candidate_root,
        label=f"capacity-{manifest.commit_count}",
    )
    effective_identities = _validate_effective_identities(
        observed_code_identity=evidence.code_identity,
        observed_runtime_identity=evidence.runtime_identity,
        expected_code_identity=expected_code_identity,
        expected_runtime_identity=expected_runtime_identity,
        label=f"capacity-{manifest.commit_count}",
    )
    native = certification.native_audit
    oracle = evidence.oracle
    failures: list[str] = []
    if measurement.commit_count != manifest.commit_count:
        failures.append("measurement commit count")
    if measurement.logical_row_count != manifest.logical_row_count:
        failures.append("measurement logical row count")
    if measurement.workload_manifest_sha256 != manifest.sha256:
        failures.append("measurement manifest SHA-256")
    if measurement.observed_workload_sha256 != manifest.workload_sha256:
        failures.append("measurement workload SHA-256")
    if (
        oracle.commit_count != manifest.commit_count
        or oracle.logical_row_count != manifest.logical_row_count
        or oracle.workload_sha256 != manifest.workload_sha256
    ):
        failures.append("independent capacity oracle")
    if (
        native.commit_count != manifest.commit_count
        or native.final_prefix_root.hex() != oracle.final_prefix_root
        or native.market_gap_count != oracle.market_gap_count
        or native.raw_reference_count != manifest.commit_count
    ):
        failures.append("native exhaustive audit")
    if certification.alignment.status is not Phase1CAuthorityStatus.ALIGNED:
        failures.append("raw/Paper authority alignment")
    if (
        certification.raw_startup.historical_segments_read != 0
        or certification.raw_startup.manifests_opened != 1
        or certification.raw_startup.manifest_namespace_entries_scanned != 0
        or certification.paper_startup.segments_read != 0
        or not certification.paper_startup.checkpoint_used
        or certification.paper_startup.tail_entries_replayed != 0
        or measurement.startup_historical_segments_read != 0
        or measurement.startup_historical_commits_replayed != 0
        or measurement.startup_tail_entries_replayed != 0
    ):
        failures.append("bounded checkpoint plus empty-tail startup")
    if measurement.byte_census.total_bytes <= 0:
        failures.append("physical byte census")
    if evidence.audited_candidate_tree.root != evidence.candidate_root:
        failures.append("audit-bound candidate tree")
    if measurement.segment_count < 1 or measurement.checkpoint_count < 1:
        failures.append("real segment/checkpoint cycles")
    if measurement.manifest_count < 2:
        failures.append("raw and Paper manifest cycles")
    if failures:
        raise Phase1CCertificationError(
            "capacity result failed exact certification: " + ", ".join(failures)
        )
    return {
        "evidence": evidence.payload(),
        "effective_identities": effective_identities,
        "manifest": manifest.payload(),
        "measurement": measurement.payload(),
        "startup_file_access_trace": startup_file_trace,
        "verified": True,
    }


def _golden_reattestation_census(
    result: GoldenNativeReattestationResult,
) -> ByteCategoryCensus:
    """Reobserve exact current bytes; never present them as producer-time metrics."""

    if not isinstance(result, GoldenNativeReattestationResult):
        raise TypeError("Golden result must be GoldenNativeReattestationResult")
    root = result.candidate_root
    raw_paths = RawStorePaths.from_root(root / "raw")
    paper_paths = RepositoryPaths.from_root(root / "paper")
    raw_anchor = LocalAnchor(
        root / "anchors/raw.sqlite3",
        store_id=result.raw_config.store_id,
        read_only=True,
    )
    paper_anchor = LocalAnchor(
        root / "anchors/paper.sqlite3",
        store_id=result.paper_config.store_id,
        read_only=True,
    )
    segment_paths = tuple(
        sorted(
            (path for path in raw_paths.segments.iterdir() if path.is_file()),
            key=lambda path: (path.stat().st_size, path.name),
        )
    )
    batch_size = result.producer.inferred_batch_size
    complete, remainder = divmod(result.native_audit.commit_count, batch_size)
    record_counts = [batch_size] * complete
    if remainder:
        record_counts.append(remainder)
    record_counts.sort()
    if len(segment_paths) != len(record_counts):
        raise Phase1CCertificationError(
            "Golden raw segment count differs from authenticated producer batches"
        )
    census = census_byte_categories(
        CapacityBytePaths(
            raw_segments=(raw_paths.segments,),
            raw_manifests=(raw_paths.manifests,),
            raw_index=(),
            raw_embedded_index_bytes=tuple(
                (path, raw_footer_index_physical_bytes(record_count))
                for path, record_count in zip(segment_paths, record_counts, strict=True)
            ),
            paper_segments=(paper_paths.segments,),
            paper_overlay=(paper_paths.overlay,),
            paper_checkpoints=(paper_paths.checkpoints,),
            paper_manifests=(paper_paths.manifests,),
            raw_anchors_witnesses=(raw_anchor.path, raw_anchor.writer_lease_path),
            paper_anchors_witnesses=(
                paper_anchor.path,
                paper_anchor.writer_lease_path,
                paper_paths.writer_lease,
            ),
            raw_current_cache=(raw_paths.current,),
            paper_current_cache=(paper_paths.current,),
            scratch=(root / "staging",),
        ),
        scratch_peak_bytes=0,
        candidate_root=root,
    )
    if census.total_bytes != result.candidate_tree_after.total_bytes:
        raise Phase1CCertificationError(
            "Golden reattested byte census differs from its immutable candidate tree"
        )
    return census


def _validate_golden_result(
    preflight: Phase1CPreflightResult,
    result: GoldenNativeReattestationResult,
    *,
    expected_code_identity: Hash32,
    expected_runtime_identity: Hash32,
) -> dict[str, object]:
    if not isinstance(result, GoldenNativeReattestationResult):
        raise TypeError("Golden result must be GoldenNativeReattestationResult")
    expected = preflight.witness.external.golden
    startup_file_trace = _validate_startup_file_trace(
        result.startup_file_trace,
        expected_candidate_root=result.candidate_root,
        label="golden-native",
    )
    effective_identities = _validate_effective_identities(
        observed_code_identity=result.reattestor_code_identity.hex(),
        observed_runtime_identity=result.reattestor_runtime_identity.hex(),
        expected_code_identity=expected_code_identity,
        expected_runtime_identity=expected_runtime_identity,
        label="golden-native",
    )
    native = result.native_audit
    producer = result.producer
    producer_identities_bound = (
        producer.paper_run_identity == result.paper_config.run_identity.digest
        and producer.paper_config_identity == result.paper_config.config_identity.digest
        and producer.producer_code_identity == result.paper_config.code_identity.digest
        and producer.producer_runtime_identity == result.paper_config.runtime_identity.digest
        and result.raw_config.config_identity == result.paper_config.config_identity.digest
    )
    if (
        native.commit_count != expected.commit_count
        or sum(item.row_count for item in native.streams) != expected.row_count
        or len(native.streams) != expected.stream_count
        or native.market_gap_count != expected.market_gap_count
        or native.raw_reference_count != expected.commit_count
        or result.raw_audit.records_read != expected.commit_count
        or result.paper_audit.commits_read != expected.commit_count
        or result.paper_audit.rows_read != expected.row_count
        or result.raw_startup.historical_segments_read != 0
        or result.raw_startup.manifests_opened != 1
        or result.raw_startup.manifest_namespace_entries_scanned != 0
        or result.paper_startup.segments_read != 0
        or not result.paper_startup.checkpoint_used
        or result.paper_startup.tail_entries_replayed != 0
        or result.startup_file_trace.historical_segment_open_count != 0
        or result.candidate_tree_before != result.candidate_tree_after
        or result.candidate_tree_before.root != result.candidate_root
        or result.differential.report.get("commits") != expected.commit_count
        or result.differential.report.get("rows") != expected.row_count
        or not producer_identities_bound
    ):
        raise Phase1CCertificationError(
            "Golden imported reattestation failed exact differential, provenance, integrity, or startup validation"
        )
    return {
        "golden_reattestation": result.payload(),
        "effective_identities": effective_identities,
        "producer_identities_bound": producer_identities_bound,
        "startup_file_access_trace": startup_file_trace,
        "verified": True,
    }


def _validate_cumulative_capacity_result(
    workloads: Phase1CWorkloadSuite,
    result: CumulativeCapacityRunResult,
    *,
    expected_code_identity: Hash32,
    expected_runtime_identity: Hash32,
) -> dict[str, object]:
    if not isinstance(result, CumulativeCapacityRunResult):
        raise TypeError("cumulative result must be CumulativeCapacityRunResult")
    expected_manifests = workloads.golden_shaped_manifests
    boundaries = result.typed_boundaries
    if (
        result.boundary_manifests != expected_manifests
        or result.terminal_manifest != expected_manifests[-1]
        or tuple(boundary.manifest for boundary in boundaries) != expected_manifests
        or tuple(boundary.manifest.commit_count for boundary in boundaries)
        != PHASE1C_CAPACITY_LEVELS
        or result.accounting.commits_generated != PHASE1C_CAPACITY_LEVELS[-1]
        or result.accounting.commits_ingested != PHASE1C_CAPACITY_LEVELS[-1]
        or result.accounting.prefix_commits_reingested != 0
        or result.accounting.worker_count != 1
        or result.accounting.store_count != 1
        or result.accounting.stream_count != 1
        or result.terminal_shared_candidate_tree.root != result.candidate_root
    ):
        raise Phase1CCertificationError(
            "cumulative capacity run is not one exact authenticated 100k/500k/1m stream"
        )
    if result.accounting.resume_count and result.accounting.prefix_commits_audited < 1:
        raise Phase1CCertificationError(
            "resumed cumulative run lacks authenticated prefix audit accounting"
        )
    identities = {
        (
            boundary.evidence.raw_store_id,
            boundary.evidence.raw_lake_id,
            boundary.evidence.paper_store_id,
            boundary.evidence.run_id,
            boundary.evidence.config_identity,
            boundary.evidence.code_identity,
            boundary.evidence.runtime_identity,
        )
        for boundary in boundaries
    }
    if len(identities) != 1:
        raise Phase1CCertificationError(
            "cumulative capacity boundaries do not share one store/stream/config identity"
        )
    boundary_results: list[dict[str, object]] = []
    for boundary in boundaries:
        exact = _validate_capacity_result(
            boundary.manifest,
            boundary.measurement,
            boundary.evidence,
            expected_code_identity=expected_code_identity,
            expected_runtime_identity=expected_runtime_identity,
        )
        certificate = boundary.certificate
        if (
            certificate.typed_measurement is not boundary.measurement
            or certificate.typed_evidence is not boundary.evidence
            or certificate.manifest_sha256 != boundary.manifest.sha256
            or certificate.raw_manifest_root != boundary.raw_manifest_root
            or certificate.paper_manifest_root != boundary.paper_manifest_root
            or certificate.checkpoint_root != boundary.checkpoint_root
        ):
            raise Phase1CCertificationError(
                f"capacity boundary certificate diverged at {boundary.manifest.commit_count}"
            )
        boundary_results.append(
            {
                "certificate": dict(certificate.payload_mapping),
                "certificate_sha256": certificate.sha256,
                "exact": exact,
                "manifest_sha256": boundary.manifest.sha256,
                "prefix_workload_sha256": boundary.workload_prefix.sha256,
            }
        )
    return {
        "accounting": result.accounting.payload(),
        "boundaries": boundary_results,
        "candidate_root": str(result.candidate_root),
        "shared_terminal_candidate_tree": (
            result.terminal_shared_candidate_tree.payload()
        ),
        "verified": True,
    }


def _validate_tail_result(
    manifest: CapacityWorkloadManifest,
    report: BoundedTailRestartMatrixReport,
    *,
    expected_code_identity: Hash32,
    expected_runtime_identity: Hash32,
) -> dict[str, object]:
    if not isinstance(report, BoundedTailRestartMatrixReport):
        raise TypeError("tail report must be BoundedTailRestartMatrixReport")
    if report.audited_candidate_tree.root != report.candidate_root:
        raise Phase1CCertificationError(
            "tail matrix audit-bound tree differs from its candidate root"
        )
    if report.manifest.sha256 != manifest.sha256 or report.test_override:
        raise Phase1CCertificationError("tail matrix is not the production manifest")
    expected_sizes = (0, 1, 100, 10_000, 20_000)
    if report.requested_tail_sizes != expected_sizes:
        raise Phase1CCertificationError("tail matrix sizes differ from the production contract")
    startup_file_traces: list[dict[str, object]] = []
    effective_identities: list[dict[str, object]] = []
    for case in report.cases:
        certification = case.certification
        startup_file_traces.append(
            _validate_startup_file_trace(
                case.startup_file_trace,
                expected_candidate_root=case.candidate_root,
                label=f"tail-{case.tail_entries}",
            )
        )
        effective_identities.append(
            _validate_effective_identities(
                observed_code_identity=case.code_identity,
                observed_runtime_identity=case.runtime_identity,
                expected_code_identity=expected_code_identity,
                expected_runtime_identity=expected_runtime_identity,
                label=f"tail-{case.tail_entries}",
            )
        )
        if (
            case.oracle.commit_count != manifest.commit_count
            or case.oracle.logical_row_count != manifest.logical_row_count
            or case.oracle.workload_sha256 != manifest.workload_sha256
            or certification.raw_startup.historical_segments_read != 0
            or certification.raw_startup.manifests_opened != 1
            or certification.raw_startup.manifest_namespace_entries_scanned != 0
            or certification.paper_startup.segments_read != 0
            or not certification.paper_startup.checkpoint_used
            or certification.paper_startup.tail_entries_replayed != case.tail_entries
            or certification.paper_startup.historical_commits_not_read
            != case.checkpoint_commit_count
        ):
            raise Phase1CCertificationError(
                f"tail restart case is not exact or bounded: {case.tail_entries}"
            )
    return {
        "effective_identities": effective_identities,
        "startup_file_access_traces": startup_file_traces,
        "tail_matrix": report.payload(),
        "verified": True,
    }


def _gate(
    gate: EvidenceSemanticGate,
    *,
    contract: str,
    result: Mapping[str, object],
) -> Phase1CGateEvidence:
    return make_semantic_gate(
        gate,
        certifier_contract=contract,
        result=result,
        passed=True,
    )


def _tree_roots(bundle: Phase1CMeasurementBundle) -> tuple[tuple[str, Path], ...]:
    return (
        ("GOLDEN_NATIVE", bundle.golden.candidate_root),
        ("BOUNDED_TAIL_RESTART", bundle.tail.candidate_root),
        ("ADVERSARIAL_STORAGE", bundle.adversarial_evidence.candidate_root),
        ("CAPACITY_CUMULATIVE", bundle.cumulative_capacity.candidate_root),
    )


def _witness_all_candidate_trees(
    bundle: Phase1CMeasurementBundle,
    *,
    progress: ProgressCallback | None,
    heartbeat_interval_seconds: float,
) -> tuple[tuple[str, Phase1CCandidateTreeWitness], ...]:
    return tuple(
        (
            label,
            witness_phase1c_candidate_tree(
                root,
                progress=progress,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
            ),
        )
        for label, root in _tree_roots(bundle)
    )


def _require_audit_bound_candidate_trees(
    bundle: Phase1CMeasurementBundle,
    trees: tuple[tuple[str, Phase1CCandidateTreeWitness], ...],
) -> None:
    tree_by_label = dict(trees)
    bound: list[tuple[str, CandidateTreeWitness]] = [
        ("GOLDEN_NATIVE", bundle.golden.candidate_tree_before),
        ("BOUNDED_TAIL_RESTART", bundle.tail.audited_candidate_tree),
        ("ADVERSARIAL_STORAGE", bundle.adversarial_evidence.audited_candidate_tree),
        (
            "CAPACITY_CUMULATIVE",
            bundle.cumulative_capacity.terminal_shared_candidate_tree,
        ),
    ]
    for label, audited_tree in bound:
        if tree_by_label.get(label) != audited_tree:
            raise Phase1CCertificationError(
                f"{label} candidate differs from its audit-bound tree witness"
            )


def _witness_bound_candidate_trees(
    bundle: Phase1CMeasurementBundle,
    *,
    progress: ProgressCallback | None,
    heartbeat_interval_seconds: float,
) -> tuple[tuple[str, Phase1CCandidateTreeWitness], ...]:
    trees = _witness_all_candidate_trees(
        bundle,
        progress=progress,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
    )
    _require_audit_bound_candidate_trees(bundle, trees)
    return trees


def _authority_table(
    bundle: Phase1CMeasurementBundle,
    trees: tuple[tuple[str, Phase1CCandidateTreeWitness], ...],
) -> dict[str, object]:
    tree_by_label = dict(trees)
    boundaries = bundle.capacity_boundaries
    capacities = {
        str(level): {
            "authority": boundaries[level].evidence.payload()["authority"],
            "boundary_certificate_sha256": boundaries[level].certificate.sha256,
            "checkpoint_root": boundaries[level].checkpoint_root.hex(),
            "manifest_sha256": boundaries[level].manifest.sha256,
            "paper_manifest_root": boundaries[level].paper_manifest_root.hex(),
            "raw_manifest_root": boundaries[level].raw_manifest_root.hex(),
        }
        for level in PHASE1C_CAPACITY_LEVELS
    }
    tail_cases = [
        {
            "authority": case.payload()["authority"],
            "candidate_id": case.candidate_id,
            "tail_entries": case.tail_entries,
        }
        for case in bundle.tail.cases
    ]
    return {
        "adversarial": {
            "authority": bundle.adversarial_evidence.payload()["authority"],
            "tree": tree_by_label["ADVERSARIAL_STORAGE"].payload(),
        },
        "capacity_levels": capacities,
        "capacity_shared_terminal_tree": tree_by_label[
            "CAPACITY_CUMULATIVE"
        ].payload(),
        "capacity_stream_accounting": bundle.cumulative_capacity.accounting.payload(),
        "golden": {
            "authority": {
                "config_identity": bundle.golden.raw_config.config_identity.hex(),
                "paper_store_id": bundle.golden.paper_config.store_id.value,
                "raw_lake_id": bundle.golden.raw_config.lake_id.value,
                "raw_store_id": bundle.golden.raw_config.store_id.value,
                "run_id": bundle.golden.paper_config.run_id.value,
            },
            "producer": bundle.golden.producer.payload(),
            "reattestor": {
                "code_identity": bundle.golden.reattestor_code_identity.hex(),
                "runtime_identity": bundle.golden.reattestor_runtime_identity.hex(),
            },
            "tree": tree_by_label["GOLDEN_NATIVE"].payload(),
        },
        "scope": (
            "AGGREGATE_TABLE_EACH_STORE_AND_RUN_REMAINS_EXPLICIT; "
            "100K_500K_1M_SHARE_ONE_TERMINAL_CANDIDATE_TREE"
        ),
        "tail": {
            "cases": tail_cases,
            "tree": tree_by_label["BOUNDED_TAIL_RESTART"].payload(),
        },
    }


def _validate_ratio_authorities(
    bundle: Phase1CMeasurementBundle,
    report: Phase1CBaselineRatioReport,
) -> None:
    golden = bundle.preflight.witness.external.golden
    phase1b = bundle.preflight.witness.external.phase1b
    expected = (
        golden.source_size_bytes,
        golden.export_physical_bytes,
        phase1b.storage_v4_store_bytes + phase1b.anchor_bytes,
        phase1b.compatibility_segment_bytes,
    )
    observed = tuple(item.byte_count for item in report.baselines)
    if observed != expected:
        raise Phase1CCertificationError(
            "baseline ratios are not bound to preflight-authenticated byte counts"
        )


def run_phase1c_measurements(
    config: Phase1CCertificationConfig,
    *,
    progress: ProgressCallback | None = None,
) -> Phase1CMeasurementBundle:
    """Reattest Golden and ingest each new synthetic commit exactly once."""

    if not isinstance(config, Phase1CCertificationConfig):
        raise TypeError("config must be Phase1CCertificationConfig")
    _emit(progress, phase="phase1c_preflight", status="RUNNING")
    preflight = run_phase1c_preflight(config.preflight)
    _emit(progress, phase="phase1c_preflight", status="COMPLETE")
    code_identity = compute_phase1c_code_identity(config.repository_root)
    code_digest = Hash32.from_hex(code_identity.sha256)
    runtime_identity = current_runtime_identity()
    mission_root = preflight.witness.mission_root
    mission_root.mkdir()
    _emit(progress, phase="phase1c_golden_shape", status="RUNNING")
    shape = derive_golden_capacity_shape(
        preflight.golden_verification,
        scratch_parent=mission_root,
        progress=progress,
    )
    _emit(progress, phase="phase1c_workload_manifests", status="RUNNING")
    workloads = build_phase1c_workload_suite(
        shape,
        progress_callback=lambda event: _emit(
            progress, **_workload_manifest_progress_payload(event)
        ),
    )
    require_exact_capacity_levels(
        tuple(manifest.commit_count for manifest in workloads.golden_shaped_manifests)
    )

    try:
        config.golden_producer_candidate_root.relative_to(mission_root)
    except ValueError:
        pass
    else:
        raise Phase1CCertificationError(
            "imported Golden producer candidate must remain outside the fresh mission root"
        )
    _emit(progress, phase="phase1c_golden_imported_reattestation", status="RUNNING")
    golden = reattest_golden_native_candidate(
        GoldenNativeReattestationConfig(
            candidate_root=config.golden_producer_candidate_root,
            producer_stdout_log=config.golden_producer_stdout_log,
            producer_stderr_log=config.golden_producer_stderr_log,
            producer_stdout_sha256=config.golden_producer_stdout_sha256,
            producer_stderr_sha256=config.golden_producer_stderr_sha256,
            reattestor_code_identity=code_digest,
            reattestor_runtime_identity=runtime_identity,
            expected_commits=preflight.witness.external.golden.commit_count,
            expected_rows=preflight.witness.external.golden.row_count,
            expected_streams=preflight.witness.external.golden.stream_count,
            expected_market_gaps=preflight.witness.external.golden.market_gap_count,
        ),
        preflight.golden_verification,
    )
    _validate_golden_result(
        preflight,
        golden,
        expected_code_identity=code_digest,
        expected_runtime_identity=runtime_identity,
    )
    golden_byte_census = _golden_reattestation_census(golden)
    _emit(
        progress,
        audited_commits=golden.native_audit.commit_count,
        ingested_commits=0,
        phase="phase1c_golden_imported_reattestation",
        prefix_commits_reingested=0,
        status="COMPLETE",
    )

    tail = BoundedTailRestartMatrixRunner(
        candidate_root=mission_root / "tail-matrix",
        code_identity=code_digest,
        runtime_identity=runtime_identity,
        progress=progress,
    ).run(workloads.bounded_tail_restart_manifest)
    _validate_tail_result(
        workloads.bounded_tail_restart_manifest,
        tail,
        expected_code_identity=code_digest,
        expected_runtime_identity=runtime_identity,
    )

    adversarial_runner = OfflinePhase1CCapacityRunner(
        candidate_root=mission_root / "adversarial-storage",
        code_identity=code_digest,
        runtime_identity=runtime_identity,
        batch_size=PHASE1C_PRODUCTION_BATCH_COMMITS,
        checkpoint_every_batches=PHASE1C_PRODUCTION_CHECKPOINT_EVERY_BATCHES,
        progress=progress,
    )
    adversarial_measurement = adversarial_runner.run_capacity_workload(
        manifest=workloads.adversarial_storage_manifest,
        commits=iter_capacity_commits(workloads.adversarial_storage_manifest.config),
    )
    adversarial_evidence = adversarial_runner.last_evidence
    if adversarial_evidence is None:
        raise Phase1CCertificationError("adversarial runner returned no durable evidence")
    _validate_capacity_result(
        workloads.adversarial_storage_manifest,
        adversarial_measurement,
        adversarial_evidence,
        expected_code_identity=code_digest,
        expected_runtime_identity=runtime_identity,
    )
    if adversarial_measurement.commit_count != PHASE1C_ADVERSARIAL_COMMIT_COUNT:
        raise Phase1CCertificationError("adversarial workload count differs")

    capacity_candidate_root = (
        mission_root / "capacity-cumulative"
        if config.cumulative_resume_candidate_root is None
        else config.cumulative_resume_candidate_root
    )
    cumulative_capacity = run_phase1c_cumulative_capacity_worker(
        Phase1CCumulativeCapacityWorkerRequest(
            manifests=workloads.golden_shaped_manifests,
            candidate_root=capacity_candidate_root,
            code_identity=code_digest,
            runtime_identity=runtime_identity,
            batch_size=PHASE1C_PRODUCTION_BATCH_COMMITS,
            resume_existing=config.cumulative_resume_candidate_root is not None,
        ),
        progress=progress,
        heartbeat_interval_seconds=config.heartbeat_interval_seconds,
    )
    _validate_cumulative_capacity_result(
        workloads,
        cumulative_capacity,
        expected_code_identity=code_digest,
        expected_runtime_identity=runtime_identity,
    )
    accounting = Phase1CCommitAccounting(
        golden_commits_audited=golden.native_audit.commit_count,
        golden_commits_ingested=0,
        golden_prefix_commits_reingested=0,
        tail_commits_ingested=(
            workloads.bounded_tail_restart_manifest.commit_count * len(tail.cases)
        ),
        adversarial_commits_ingested=adversarial_measurement.commit_count,
        cumulative_commits_ingested=cumulative_capacity.accounting.commits_ingested,
        cumulative_prefix_commits_reingested=(
            cumulative_capacity.accounting.prefix_commits_reingested
        ),
        historical_attempt_ingestion=config.historical_attempt_ingestion,
    )
    return Phase1CMeasurementBundle(
        preflight=preflight,
        code_identity=code_identity,
        runtime_identity=runtime_identity,
        golden_shape=shape,
        workloads=workloads,
        golden=golden,
        golden_byte_census=golden_byte_census,
        tail=tail,
        adversarial_measurement=adversarial_measurement,
        adversarial_evidence=adversarial_evidence,
        cumulative_capacity=cumulative_capacity,
        accounting=accounting,
    )


def _make_report_with_results(
    publisher: Phase1CEvidencePublisher,
    *,
    status: EvidenceReportStatus,
    semantic_results: Mapping[
        EvidenceSemanticGate,
        tuple[str, Mapping[str, object]],
    ],
    payload: Mapping[str, object],
) -> Phase1CEvidenceReport:
    ordered = tuple(sorted(semantic_results, key=lambda item: item.value))
    result_payload = {
        gate.value: {
            "certifier_contract": semantic_results[gate][0],
            "passed": True,
            "result": dict(semantic_results[gate][1]),
        }
        for gate in ordered
    }
    return publisher.make_report(
        status=status,
        gates=tuple(
            _gate(
                gate,
                contract=semantic_results[gate][0],
                result=semantic_results[gate][1],
            )
            for gate in ordered
        ),
        payload={**dict(payload), "semantic_certifier_results": result_payload},
    )


def _aggregate_provenance(
    *,
    evidence_root: Path,
    bundle: Phase1CMeasurementBundle,
    code_identity: Phase1CCodeIdentity,
    authority_table: Mapping[str, object],
    heartbeat_interval_seconds: float,
    targeted_tests_sha256: str,
) -> tuple[Phase1CEvidenceProvenance, str, str]:
    authority_sha256 = _canonical_sha256(authority_table)
    configuration = {
        "commit_accounting": bundle.accounting.payload(),
        "capacity_levels": list(PHASE1C_CAPACITY_LEVELS),
        "golden_shape_sha256": bundle.golden_shape.sha256,
        "golden_imported_reattestation": bundle.golden.payload(),
        "heartbeat_interval_seconds": (
            format(Decimal(str(heartbeat_interval_seconds)), "f")
        ),
        "preflight": bundle.preflight.witness.to_dict(),
        "raw_reference_contract": RAW_REFERENCE_CONTRACT_MARKER_V2,
        "raw_reference_format_version": RAW_REFERENCE_FORMAT_VERSION_V2,
        "runtime_identity": bundle.runtime_identity.hex(),
        "targeted_tests_sha256": targeted_tests_sha256,
        "workloads": bundle.workloads.payload(),
    }
    config_identity = _canonical_sha256(configuration)
    aggregate_prefix = f"PHASE1C_EVIDENCE_AGGREGATE/{authority_sha256}"
    golden = bundle.preflight.witness.external.golden
    provenance = Phase1CEvidenceProvenance(
        candidate_id=bundle.preflight.witness.mission_root.name,
        candidate_root=str(evidence_root),
        run_id=f"{aggregate_prefix}/run",
        raw_store_id=f"{aggregate_prefix}/raw-stores",
        raw_lake_id=f"{aggregate_prefix}/raw-lakes",
        paper_store_id=f"{aggregate_prefix}/paper-stores",
        config_identity=config_identity,
        code_identity=code_identity.sha256,
        runtime_identity=bundle.runtime_identity.hex(),
        golden_source_root_sha256=golden.golden_root_hash,
        golden_pin_sha256=golden.pin.sha256,
        golden_certification_root_sha256=golden.certification_root_hash,
    )
    return provenance, authority_sha256, config_identity


def _target_assessments(
    bundle: Phase1CMeasurementBundle,
) -> tuple[StorageGrowthAssessment, dict[int, StorageGrowthAssessment]]:
    span_ns = bundle.golden_shape.end_time_ns - bundle.golden_shape.start_time_ns
    golden = assess_storage_growth(
        total_bytes=bundle.golden_byte_census.total_bytes,
        commit_count=bundle.preflight.witness.external.golden.commit_count,
        logical_span_ns=span_ns,
    )
    levels = {
        level: measurement.storage_growth
        for level, measurement in bundle.level_measurements
    }
    return golden, levels


def _publish_phase1c_evidence(
    config: Phase1CCertificationConfig,
    bundle: Phase1CMeasurementBundle,
    closure: Phase1CClosureWitness,
    *,
    progress: ProgressCallback | None,
) -> Phase1CCertificationResult:
    persisted_logs = _verify_all_output_logs(config.targeted_tests, closure)
    persisted_logs["golden_producer"] = _verify_golden_producer_logs(
        config,
        bundle.golden,
    )
    final_code = compute_phase1c_code_identity(config.repository_root)
    if final_code != bundle.code_identity:
        raise Phase1CCertificationError(
            "Phase 1C code identity changed between workers and repository closure"
        )
    final_runtime = current_runtime_identity()
    if final_runtime != bundle.runtime_identity:
        raise Phase1CCertificationError(
            "Phase 1C runtime identity changed between workers and repository closure"
        )
    current_test_sources = phase1c_test_source_witnesses(
        config.repository_root,
        (path for path, _digest in config.targeted_tests.source_files),
    )
    if current_test_sources != config.targeted_tests.source_files:
        raise Phase1CCertificationError(
            "targeted Phase 1C test sources changed after their successful run"
        )
    _emit(progress, phase="phase1c_postflight", status="RUNNING")
    postflight = verify_phase1c_postflight(
        config.preflight,
        bundle.preflight.witness,
    )
    _emit(progress, phase="phase1c_postflight", status="COMPLETE")
    trees = _witness_bound_candidate_trees(
        bundle,
        progress=progress,
        heartbeat_interval_seconds=config.heartbeat_interval_seconds,
    )
    authority_table = _authority_table(bundle, trees)
    evidence_root = bundle.preflight.witness.mission_root / "evidence"
    provenance, authority_sha256, config_identity = _aggregate_provenance(
        evidence_root=evidence_root,
        bundle=bundle,
        code_identity=final_code,
        authority_table=authority_table,
        heartbeat_interval_seconds=float(config.heartbeat_interval_seconds),
        targeted_tests_sha256=config.targeted_tests.sha256,
    )
    publisher = Phase1CEvidencePublisher(evidence_root, provenance=provenance)

    workload_result = {
        "commit_accounting": bundle.accounting.payload(),
        "config_identity": config_identity,
        "golden_shape": bundle.golden_shape.payload(),
        "runtime_identity": final_runtime.hex(),
        "target": bundle.preflight.witness.external.capacity_target.to_dict(),
        "workloads": bundle.workloads.payload(),
    }
    publisher.publish_json(
        EvidenceArtifactName.WORKLOAD_MANIFEST,
        report=_make_report_with_results(
            publisher,
            status=EvidenceReportStatus.WORKLOAD_MANIFEST_FROZEN,
            semantic_results={
                EvidenceSemanticGate.CONFIG_BOUND: (
                    "STORAGE_V4_PHASE1C_WORKLOAD_CONFIGURATION_BINDING_V1",
                    workload_result,
                )
            },
            payload={
                "commit_accounting": bundle.accounting.payload(),
                "golden_shape": bundle.golden_shape.payload(),
                "suite": bundle.workloads.payload(),
                "target_frozen_before_measurement": (
                    bundle.preflight.witness.external.capacity_target.to_dict()
                ),
            },
        ),
    )

    native_layout_result = {
        "authority_set_sha256": authority_sha256,
        "code_identity": final_code.payload(),
        "commit_accounting": bundle.accounting.payload(),
        "golden_imported_reattestation_status": (
            GOLDEN_NATIVE_IMPORTED_REATTESTATION_V1
        ),
        "raw_reference_contract": RAW_REFERENCE_CONTRACT_MARKER_V2,
        "raw_reference_format_version": RAW_REFERENCE_FORMAT_VERSION_V2,
        "runtime_identity": final_runtime.hex(),
        "targeted_tests": config.targeted_tests.payload(),
    }
    publisher.publish_json(
        EvidenceArtifactName.NATIVE_LAYOUT_REPORT,
        report=_make_report_with_results(
            publisher,
            status=EvidenceReportStatus.NATIVE_LAYOUT_VERIFIED,
            semantic_results={
                EvidenceSemanticGate.INTEGRITY_VERIFIED: (
                    "STORAGE_V4_PHASE1C_NATIVE_LAYOUT_INTEGRITY_V1",
                    native_layout_result,
                )
            },
            payload={
                "authority_matrix": authority_table,
                "authority_set_sha256": authority_sha256,
                "physical_model": {
                    "journal_public_inputs": "AUTHENTICATED_RAW_SEGMENT_REFERENCE_V2",
                    "paper_owned_data": "DIRECT_IN_PAPER_JOURNAL",
                    "raw_payload_copies": "ONE_CONTENT_ADDRESSED_RAW_COPY",
                    "rematerialization": "STREAMING_RAW_PLUS_REFERENCE_PLUS_PAPER",
                    "startup_scope": "AUTHENTICATED_CURRENT_MANIFEST_CHECKPOINT_BOUNDED_TAIL",
                },
                "raw_reference_contract": {
                    "contract": RAW_REFERENCE_CONTRACT_MARKER_V2,
                    "format_version": RAW_REFERENCE_FORMAT_VERSION_V2,
                },
            },
        ),
    )

    tail_result = _validate_tail_result(
        bundle.workloads.bounded_tail_restart_manifest,
        bundle.tail,
        expected_code_identity=Hash32.from_hex(final_code.sha256),
        expected_runtime_identity=final_runtime,
    )
    golden_result = _validate_golden_result(
        bundle.preflight,
        bundle.golden,
        expected_code_identity=Hash32.from_hex(final_code.sha256),
        expected_runtime_identity=final_runtime,
    )
    golden_tree = dict(trees)["GOLDEN_NATIVE"].payload()
    golden_semantics: dict[
        EvidenceSemanticGate,
        tuple[str, Mapping[str, object]],
    ] = {
        EvidenceSemanticGate.EXACT_LOGICAL_MATCH: (
            "STORAGE_V4_PHASE1C_IMPORTED_GOLDEN_13_STREAM_DIFFERENTIAL_V2",
            golden_result,
        ),
        EvidenceSemanticGate.GOLDEN_SOURCE_UNCHANGED: (
            "STORAGE_V4_PHASE1C_EXTERNAL_POSTFLIGHT_V1",
            postflight.to_dict(),
        ),
        EvidenceSemanticGate.INTEGRITY_VERIFIED: (
            "STORAGE_V4_PHASE1C_IMPORTED_GOLDEN_CHAIN_TREE_AND_PROVENANCE_V2",
            {"golden": golden_result, "tree": golden_tree},
        ),
        EvidenceSemanticGate.STARTUP_BOUNDED: (
            "STORAGE_V4_PHASE1C_GOLDEN_STARTUP_FILE_TRACE_V2",
            {
                "startup": bundle.golden.payload()["startup"],
                "startup_file_access_trace": (
                    bundle.golden.startup_file_trace.payload()
                ),
                "startup_file_evidence_scope": (
                    "BOUNDED_PYTHON_LEVEL_OS_OPEN_PATH_OPEN_SQLITE_CONNECT; "
                    "NOT_OS_KERNEL_ETW_OR_SQLITE_VFS"
                ),
                "timing_scope": (
                    "READ_ONLY_REATTESTATION_TIMING_NOT_RECOVERED_PRODUCER_TIMING"
                ),
            },
        ),
        EvidenceSemanticGate.TAIL_VERIFIED: (
            "STORAGE_V4_PHASE1C_BOUNDED_TAIL_MATRIX_WITH_STARTUP_TRACES_V2",
            tail_result,
        ),
    }
    publisher.publish_json(
        EvidenceArtifactName.GOLDEN_NATIVE_REPORT,
        report=_make_report_with_results(
            publisher,
            status=EvidenceReportStatus.GOLDEN_NATIVE_EXACT,
            semantic_results=golden_semantics,
            payload={
                "golden": bundle.golden.payload(),
                "golden_byte_census": {
                    **bundle.golden_byte_census.payload(),
                    "status": "REATTESTED_CURRENT_BYTES_NOT_PRODUCER_TIME_PEAKS",
                },
                "source_authority": bundle.preflight.witness.external.golden.to_dict(),
                "tree": golden_tree,
            },
        ),
    )

    level_measurements = dict(bundle.level_measurements)
    level_evidence = dict(bundle.level_evidence)
    capacity_boundaries = bundle.capacity_boundaries
    level_manifests = bundle.workloads.capacity_level_manifests
    cumulative_result = _validate_cumulative_capacity_result(
        bundle.workloads,
        bundle.cumulative_capacity,
        expected_code_identity=Hash32.from_hex(final_code.sha256),
        expected_runtime_identity=final_runtime,
    )
    shared_capacity_tree = dict(trees)["CAPACITY_CUMULATIVE"].payload()
    golden_assessment, level_assessments = _target_assessments(bundle)
    target_verdict = decide_phase1c_target_verdict(
        golden_assessment=golden_assessment,
        level_assessments=level_assessments,
        canonical_target_gib_per_hour=CANONICAL_TARGET_GIB_PER_HOUR,
    )
    diagnostic_by_label = {
        item.label: item.payload() for item in target_verdict.diagnostics
    }
    for level in PHASE1C_CAPACITY_LEVELS:
        manifest = level_manifests[level]
        measurement = level_measurements[level]
        evidence = level_evidence[level]
        boundary = capacity_boundaries[level]
        exact_result = _validate_capacity_result(
            manifest,
            measurement,
            evidence,
            expected_code_identity=Hash32.from_hex(final_code.sha256),
            expected_runtime_identity=final_runtime,
        )
        startup_result = {
            "startup": measurement.payload()["startup"],
            "worker_startup": evidence.payload()["startup"],
            "startup_file_access_trace": evidence.startup_file_trace.payload(),
            "startup_file_evidence_scope": (
                "BOUNDED_PYTHON_LEVEL_OS_OPEN_PATH_OPEN_SQLITE_CONNECT; "
                "NOT_OS_KERNEL_ETW_OR_SQLITE_VFS"
            ),
        }
        semantic_results: dict[
            EvidenceSemanticGate,
            tuple[str, Mapping[str, object]],
        ] = {
            EvidenceSemanticGate.EXACT_LOGICAL_MATCH: (
                "STORAGE_V4_PHASE1C_SYNTHETIC_FULL_WORKLOAD_ORACLE_V1",
                exact_result,
            ),
            EvidenceSemanticGate.INTEGRITY_VERIFIED: (
                "STORAGE_V4_PHASE1C_CUMULATIVE_PREFIX_CERTIFICATE_AND_SHARED_TREE_V2",
                {
                    "boundary_certificate": dict(boundary.certificate.payload_mapping),
                    "boundary_certificate_sha256": boundary.certificate.sha256,
                    "evidence": evidence.payload(),
                    "shared_terminal_tree": shared_capacity_tree,
                },
            ),
            EvidenceSemanticGate.MEASUREMENTS_COMPLETE: (
                "STORAGE_V4_PHASE1C_CAPACITY_MEASUREMENT_SCHEMA_V1",
                measurement.payload(),
            ),
            EvidenceSemanticGate.STARTUP_BOUNDED: (
                "STORAGE_V4_PHASE1C_CHECKPOINT_EMPTY_TAIL_STARTUP_FILE_TRACE_V2",
                startup_result,
            ),
            EvidenceSemanticGate.TAIL_VERIFIED: (
                "STORAGE_V4_PHASE1C_BOUNDED_TAIL_MATRIX_WITH_STARTUP_TRACES_V2",
                tail_result,
            ),
        }
        publisher.publish_json(
            _LEVEL_TO_ARTIFACT[level],
            report=_make_report_with_results(
                publisher,
                status=EvidenceReportStatus.CAPACITY_LEVEL_VERIFIED,
                semantic_results=semantic_results,
                payload={
                    "cumulative_accounting": (
                        bundle.cumulative_capacity.accounting.payload()
                    ),
                    "durable_boundary": {
                        "certificate": dict(boundary.certificate.payload_mapping),
                        "certificate_sha256": boundary.certificate.sha256,
                        "checkpoint_root": boundary.checkpoint_root.hex(),
                        "paper_manifest_root": boundary.paper_manifest_root.hex(),
                        "raw_manifest_root": boundary.raw_manifest_root.hex(),
                        "workload_prefix": {
                            "commit_count": boundary.workload_prefix.commit_count,
                            "logical_row_count": (
                                boundary.workload_prefix.logical_row_count
                            ),
                            "sha256": boundary.workload_prefix.sha256,
                        },
                    },
                    "evidence": evidence.payload(),
                    "manifest": manifest.payload(),
                    "measurement": measurement.payload(),
                    "target_diagnostic": diagnostic_by_label[f"GOLDEN_SHAPED_{level}"],
                    "shared_terminal_tree": shared_capacity_tree,
                    "tree_scope": (
                        "ONE_TERMINAL_SHARED_CANDIDATE_TREE; INTERMEDIATE_BOUNDARY_TREE "
                        "IS_NOT_COMPARED_TO_TERMINAL_TREE"
                    ),
                },
            ),
        )
        publisher.publish_level_complete(_LEVEL_TO_COMPLETE[level])

    ordered_measurements = tuple(
        level_measurements[level] for level in PHASE1C_CAPACITY_LEVELS
    )
    scaling = compute_capacity_scaling(ordered_measurements)
    ratios = build_phase1c_baseline_ratio_report(
        golden_census=bundle.golden_byte_census,
        level_censuses={
            level: level_measurements[level].byte_census
            for level in PHASE1C_CAPACITY_LEVELS
        },
    )
    _validate_ratio_authorities(bundle, ratios)
    scaling_semantics: dict[
        EvidenceSemanticGate,
        tuple[str, Mapping[str, object]],
    ] = {
        EvidenceSemanticGate.MEASUREMENTS_COMPLETE: (
            "STORAGE_V4_PHASE1C_SCALING_INPUT_SET_V1",
            {
                "cumulative_accounting": (
                    bundle.cumulative_capacity.accounting.payload()
                ),
                "levels": [item.payload() for item in ordered_measurements],
                "ratios": ratios.payload(),
            },
        ),
        EvidenceSemanticGate.SCALING_CHARACTERIZED: (
            "STORAGE_V4_PHASE1C_SCALING_AND_STARTUP_COMPARISON_V1",
            {
                "cumulative": cumulative_result,
                "scaling": scaling,
                "tail": tail_result,
            },
        ),
    }
    publisher.publish_json(
        EvidenceArtifactName.SCALING_REPORT,
        report=_make_report_with_results(
            publisher,
            status=EvidenceReportStatus.SCALING_CHARACTERIZED,
            semantic_results=scaling_semantics,
            payload={
                "baseline_ratios": ratios.payload(),
                "cumulative": cumulative_result,
                "scaling": scaling,
                "target_verdict": target_verdict.payload(),
                "tail_matrix": bundle.tail.payload(),
            },
        ),
    )

    adversarial_result = _validate_capacity_result(
        bundle.workloads.adversarial_storage_manifest,
        bundle.adversarial_measurement,
        bundle.adversarial_evidence,
        expected_code_identity=Hash32.from_hex(final_code.sha256),
        expected_runtime_identity=final_runtime,
    )
    integrity_semantics: dict[
        EvidenceSemanticGate,
        tuple[str, Mapping[str, object]],
    ] = {
        EvidenceSemanticGate.FAULT_RECOVERY_VERIFIED: (
            "STORAGE_V4_PHASE1C_FAULT_RECOVERY_TARGETED_AND_GLOBAL_TESTS_V1",
            {
                "closure": closure.payload(),
                "targeted_tests": config.targeted_tests.payload(),
            },
        ),
        EvidenceSemanticGate.INTEGRITY_VERIFIED: (
            "STORAGE_V4_PHASE1C_AGGREGATE_INTEGRITY_V1",
            {
                "adversarial": adversarial_result,
                "authority_set_sha256": authority_sha256,
                "closure_sha256": closure.sha256,
                "code_identity": final_code.payload(),
                "commit_accounting": bundle.accounting.payload(),
                "cumulative": cumulative_result,
                "persisted_logs": persisted_logs,
                "postflight": postflight.to_dict(),
                "tail": tail_result,
                "trees": {label: tree.payload() for label, tree in trees},
            },
        ),
    }
    publisher.publish_json(
        EvidenceArtifactName.INTEGRITY_REPORT,
        report=_make_report_with_results(
            publisher,
            status=EvidenceReportStatus.INTEGRITY_AND_RECOVERY_VERIFIED,
            semantic_results=integrity_semantics,
            payload={
                "adversarial": {
                    "evidence": bundle.adversarial_evidence.payload(),
                    "manifest": bundle.workloads.adversarial_storage_manifest.payload(),
                    "measurement": bundle.adversarial_measurement.payload(),
                    "tree": dict(trees)["ADVERSARIAL_STORAGE"].payload(),
                },
                "closure": closure.payload(),
                "commit_accounting": bundle.accounting.payload(),
                "cumulative": cumulative_result,
                "external_postflight": postflight.to_dict(),
                "targeted_tests": config.targeted_tests.payload(),
                "tail_matrix": bundle.tail.payload(),
            },
        ),
    )

    limitations_result = {
        "execution_model": {
            "capacity_boundaries": list(PHASE1C_CAPACITY_LEVELS),
            "capacity_candidate_root": str(
                bundle.cumulative_capacity.candidate_root
            ),
            "capacity_store_count": 1,
            "capacity_stream_count": 1,
            "capacity_worker_count": 1,
            "golden_ingested_commits": 0,
            "golden_status": GOLDEN_NATIVE_IMPORTED_REATTESTATION_V1,
            "resume_count": bundle.cumulative_capacity.accounting.resume_count,
        },
        "limitations": list(_LIMITATIONS),
        "margin_interpretation": (
            "ROADMAP_REQUIRES_MARGIN_BUT_DEFINES_NO_SECOND_NUMERIC_THRESHOLD; "
            "STRICT_LT_0.20_IS_REPORTED_WITHOUT_INVENTING_MARGIN"
        ),
        "performance_target_is_not_an_integrity_gate": True,
    }
    publisher.publish_json(
        EvidenceArtifactName.LIMITATIONS,
        report=_make_report_with_results(
            publisher,
            status=EvidenceReportStatus.LIMITATIONS_RECORDED,
            semantic_results={
                EvidenceSemanticGate.LIMITATIONS_RECORDED: (
                    "STORAGE_V4_PHASE1C_LIMITATIONS_DISCLOSURE_V1",
                    limitations_result,
                )
            },
            payload=limitations_result,
        ),
    )

    measurement_rows: list[dict[str, object]] = [
        {
            "kind": "ADVERSARIAL_STORAGE",
            "measurement": bundle.adversarial_measurement.payload(),
        }
    ]
    measurement_rows.extend(
        {
            "boundary_certificate_sha256": (
                capacity_boundaries[level].certificate.sha256
            ),
            "kind": f"GOLDEN_SHAPED_{level}",
            "measurement": level_measurements[level].payload(),
            "shared_candidate_root": str(
                bundle.cumulative_capacity.candidate_root
            ),
        }
        for level in PHASE1C_CAPACITY_LEVELS
    )
    measurement_rows.extend(
        {
            "kind": "BOUNDED_TAIL_RESTART",
            "measurement": case.payload(),
        }
        for case in bundle.tail.cases
    )
    measurement_result = {
        "commit_accounting": bundle.accounting.payload(),
        "row_count": len(measurement_rows),
        "rows_sha256": _canonical_sha256(
            {"rows": measurement_rows, "synthetic_only": True}
        ),
        "synthetic_only": True,
    }
    publisher.publish_measurements(
        measurement_rows,
        report=_make_report_with_results(
            publisher,
            status=EvidenceReportStatus.MEASUREMENTS_VERIFIED,
            semantic_results={
                EvidenceSemanticGate.MEASUREMENTS_COMPLETE: (
                    "STORAGE_V4_PHASE1C_SYNTHETIC_MEASUREMENT_SET_V1",
                    measurement_result,
                )
            },
            payload=measurement_result,
        ),
        synthetic=True,
    )

    _emit(progress, phase="phase1c_final_postflight", status="RUNNING")
    final_postflight = verify_phase1c_postflight(
        config.preflight,
        bundle.preflight.witness,
    )
    _emit(progress, phase="phase1c_final_postflight", status="COMPLETE")
    if final_postflight != postflight:
        raise Phase1CCertificationError("external authority changed before terminal publication")
    rehashed_trees = _witness_bound_candidate_trees(
        bundle,
        progress=progress,
        heartbeat_interval_seconds=config.heartbeat_interval_seconds,
    )
    if rehashed_trees != trees:
        raise Phase1CCertificationError(
            "a measured candidate tree changed before terminal publication"
        )
    if compute_phase1c_code_identity(config.repository_root) != final_code:
        raise Phase1CCertificationError("code identity changed before terminal publication")
    if current_runtime_identity() != final_runtime:
        raise Phase1CCertificationError("runtime identity changed before terminal publication")
    final_persisted_logs = _verify_all_output_logs(config.targeted_tests, closure)
    final_persisted_logs["golden_producer"] = _verify_golden_producer_logs(
        config,
        bundle.golden,
    )
    if final_persisted_logs != persisted_logs:
        raise Phase1CCertificationError("persisted closure logs changed before terminal")
    publisher.publish_terminal_complete(target_verdict.terminal_verdict)
    records = publisher.verify_all()
    return Phase1CCertificationResult(
        verdict=target_verdict.terminal_verdict,
        mission_root=bundle.preflight.witness.mission_root,
        evidence_root=evidence_root,
        code_identity=final_code,
        runtime_identity=final_runtime,
        closure=closure,
        postflight=final_postflight,
        artifacts=tuple(
            (record.name, record.sha256, record.size_bytes) for record in records
        ),
    )


def run_phase1c_certification(
    config: Phase1CCertificationConfig,
    *,
    closure_runner: ClosureRunner,
    progress: ProgressCallback | None = None,
) -> Phase1CCertificationResult:
    """Run Phase 1C end to end; terminal evidence follows full repo closure."""

    if not isinstance(config, Phase1CCertificationConfig):
        raise TypeError("config must be Phase1CCertificationConfig")
    if not callable(closure_runner):
        raise TypeError("closure_runner must be callable")
    bundle = run_phase1c_measurements(config, progress=progress)
    _emit(progress, phase="phase1c_repository_closure", status="RUNNING")
    closure = closure_runner(bundle.preflight.witness.mission_root)
    if not isinstance(closure, Phase1CClosureWitness):
        raise Phase1CCertificationError(
            "closure runner did not return Phase1CClosureWitness"
        )
    _emit(progress, phase="phase1c_repository_closure", status="COMPLETE")
    return _publish_phase1c_evidence(
        config,
        bundle,
        closure,
        progress=progress,
    )


__all__ = [
    "PHASE1C_CAPACITY_LEVELS",
    "PHASE1C_CERTIFICATION_FORMAT",
    "PHASE1C_CODE_IDENTITY_FORMAT",
    "PHASE1C_EXPECTED_NEW_INGESTED_COMMITS",
    "PHASE1C_HEARTBEAT_MAX_SECONDS",
    "PHASE1C_HEARTBEAT_MIN_SECONDS",
    "PHASE1C_HISTORICAL_ATTEMPT_INGESTION",
    "PHASE1C_NO_CANONICAL_TARGET",
    "PHASE1C_PROVEN",
    "PHASE1C_TARGET_NOT_MET",
    "PHASE1C_TEST_WITNESS_FORMAT",
    "ClosureRunner",
    "Phase1CCandidateFileWitness",
    "Phase1CCandidateTreeWitness",
    "Phase1CCertificationConfig",
    "Phase1CCertificationError",
    "Phase1CCertificationResult",
    "Phase1CClosureWitness",
    "Phase1CCodeIdentity",
    "Phase1CCommandWitness",
    "Phase1CCommitAccounting",
    "Phase1CMeasurementBundle",
    "Phase1CTestWitness",
    "Phase1CV9ByteWitness",
    "compute_phase1c_code_identity",
    "make_semantic_gate",
    "phase1c_test_source_witnesses",
    "require_exact_capacity_levels",
    "run_phase1c_certification",
    "run_phase1c_measurements",
    "witness_phase1c_candidate_tree",
]
