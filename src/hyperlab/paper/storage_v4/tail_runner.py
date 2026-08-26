"""On-disk bounded-tail restart matrix for Storage V4 Phase 1C.

Each case consumes the same frozen workload in a fresh candidate, publishes a
checkpoint at ``commit_count - tail``, and deliberately leaves the requested
suffix in the authenticated Paper overlay. This offline PAPER_ONLY runner has
no network, credential, wallet, order, or live-trading surface and never
publishes a ``COMPLETE`` marker.
"""

from __future__ import annotations

import hashlib
import os
import stat
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from .anchor import LocalAnchor
from .candidate_tree import (
    CandidateTreeWitness,
    CandidateTreeWitnessError,
    compose_candidate_tree_witness,
    witness_candidate_tree,
)
from .canonical import canonical_json_bytes
from .capacity import (
    CAPACITY_MARKERS,
    CapacityProfile,
    CapacityWorkloadHasher,
    CapacityWorkloadManifest,
    SyntheticCapacityCommit,
    iter_capacity_commits,
)
from .capacity_adapter import SyntheticCapacityPhase1CAdapter
from .capacity_oracle import CapacityOracleReport, compare_capacity_native_exact
from .contracts import RawLakeId, StorageMode
from .manifest import OpaqueIdentity
from .native_journal import NativeAuditExpectations
from .phase1c_pipeline import (
    Phase1CAuthorityReport,
    Phase1CAuthorityStatus,
    Phase1CCertificationReport,
    Phase1CSealResult,
    Phase1CWriter,
    certify_phase1c_reopen,
    inspect_phase1c_alignment,
)
from .raw_segment import RawSegmentThresholds
from .raw_store import DiskRawResolver, RawStartupReport, RawStore, RawStoreConfig
from .repository import (
    RepositoryConfig,
    RepositoryPaths,
    StartupReport,
    StorageRepository,
)
from .startup_trace import (
    StartupFileAccessTrace,
    StartupTracePaths,
    trace_startup_file_access,
)
from .types import Hash32, RunId, StoreId

_ARTIFACT = "STORAGE_V4_PHASE1C_BOUNDED_TAIL_RESTART_MATRIX_V1"
_STATUS = "STORAGE_V4_PHASE1C_BOUNDED_TAIL_RESTART_EXACT"
_GENESIS_PREFIX_ROOT = Hash32(b"\x00" * 32)
_IDENTITY_DOMAIN = b"HL4-PHASE1C-BOUNDED-TAIL-RUNNER-V1\x00"
_PRODUCTION_FIXED_TAILS = (0, 1, 100, 10_000)

ProgressCallback = Callable[[Mapping[str, object]], None]


class TailMatrixRunnerErrorCode(StrEnum):
    TYPE_INVALID = "TAIL_MATRIX_TYPE_INVALID"
    PATH_INVALID = "TAIL_MATRIX_PATH_INVALID"
    CANDIDATE_EXISTS = "TAIL_MATRIX_CANDIDATE_EXISTS"
    CONTRACT_INVALID = "TAIL_MATRIX_CONTRACT_INVALID"
    WORKLOAD_DIVERGENCE = "TAIL_MATRIX_WORKLOAD_DIVERGENCE"
    INTEGRITY_DIVERGENCE = "TAIL_MATRIX_INTEGRITY_DIVERGENCE"


class TailMatrixRunnerError(RuntimeError):
    """Fail-closed matrix rejection with a stable machine-readable code."""

    def __init__(self, code: TailMatrixRunnerErrorCode, message: str) -> None:
        if type(code) is not TailMatrixRunnerErrorCode:
            raise TypeError("tail matrix error code is invalid")
        self.code = code
        super().__init__(f"{code.value}: {message}")


def _error(code: TailMatrixRunnerErrorCode, message: str) -> TailMatrixRunnerError:
    return TailMatrixRunnerError(code, message)


class TailMatrixExactness(StrEnum):
    EXACT = "FULL_WORKLOAD_EXACT_INCLUDING_AUTHENTICATED_TAIL"


@dataclass(frozen=True, slots=True)
class TailRestartCaseReport:
    """Typed counters and truthfully scoped timings for one tail size."""

    candidate_id: str
    candidate_root: Path
    audited_candidate_tree: CandidateTreeWitness
    raw_store_id: str
    raw_lake_id: str
    paper_store_id: str
    run_id: str
    config_identity: str
    code_identity: str
    runtime_identity: str
    tail_entries: int
    checkpoint_commit_count: int
    batch_count: int
    max_batch_commits_observed: int
    startup_ns: int
    certification_with_reopen_ns: int
    independent_oracle_ns: int
    certification: Phase1CCertificationReport
    startup_file_trace: StartupFileAccessTrace
    oracle: CapacityOracleReport
    exactness: TailMatrixExactness = TailMatrixExactness.EXACT

    def __post_init__(self) -> None:
        if type(self.candidate_id) is not str or not self.candidate_id:
            raise ValueError("candidate_id must be non-empty text")
        if not isinstance(self.candidate_root, Path) or not self.candidate_root.is_absolute():
            raise ValueError("candidate_root must be an absolute pathlib.Path")
        if (
            not isinstance(self.audited_candidate_tree, CandidateTreeWitness)
            or self.audited_candidate_tree.root != self.candidate_root
        ):
            raise ValueError("audited candidate tree must bind the exact tail case root")
        for text_label, text_value in (
            ("raw_store_id", self.raw_store_id),
            ("raw_lake_id", self.raw_lake_id),
            ("paper_store_id", self.paper_store_id),
            ("run_id", self.run_id),
        ):
            if type(text_value) is not str or not text_value:
                raise ValueError(f"{text_label} must be non-empty text")
        for digest_label, digest_value in (
            ("config_identity", self.config_identity),
            ("code_identity", self.code_identity),
            ("runtime_identity", self.runtime_identity),
        ):
            if (
                type(digest_value) is not str
                or len(digest_value) != 64
                or digest_value != digest_value.lower()
                or any(character not in "0123456789abcdef" for character in digest_value)
            ):
                raise ValueError(f"{digest_label} must be a lowercase SHA-256")
        for label, value in (
            ("tail_entries", self.tail_entries),
            ("checkpoint_commit_count", self.checkpoint_commit_count),
            ("batch_count", self.batch_count),
            ("max_batch_commits_observed", self.max_batch_commits_observed),
            ("startup_ns", self.startup_ns),
            ("certification_with_reopen_ns", self.certification_with_reopen_ns),
            ("independent_oracle_ns", self.independent_oracle_ns),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{label} must be a non-negative exact integer")
        if not isinstance(self.certification, Phase1CCertificationReport):
            raise TypeError("certification must be Phase1CCertificationReport")
        if not isinstance(self.startup_file_trace, StartupFileAccessTrace):
            raise TypeError("startup_file_trace must be StartupFileAccessTrace")
        if not isinstance(self.oracle, CapacityOracleReport):
            raise TypeError("oracle must be CapacityOracleReport")
        if not isinstance(self.exactness, TailMatrixExactness):
            raise TypeError("exactness must be TailMatrixExactness")

    def payload(self) -> dict[str, object]:
        certification = self.certification
        binding = certification.alignment.binding
        native_rows = sum(stream.row_count for stream in certification.native_audit.streams)
        return {
            "audit_counters": {
                "native_commits_read": certification.native_audit.commit_count,
                "native_rows_read": native_rows,
                "paper_commits_read": certification.paper_audit.commits_read,
                "paper_manifests_read": certification.paper_audit.manifests_read,
                "paper_rows_read": certification.paper_audit.rows_read,
                "paper_segments_read": certification.paper_audit.segments_read,
                "raw_manifests_read": certification.raw_audit.manifests_read,
                "raw_records_read": certification.raw_audit.records_read,
                "raw_resolver_physical_hash_passes": (certification.raw_resolver_physical_hash_passes),
                "raw_segments_read": certification.raw_audit.segments_read,
            },
            "audited_candidate_tree": self.audited_candidate_tree.payload(),
            "authority": {
                "candidate_root": str(self.candidate_root),
                "checkpoint_raw_generation": None if binding is None else binding.raw_generation,
                "checkpoint_raw_manifest_root": (
                    None if binding is None else binding.raw_manifest_root.hex()
                ),
                "checkpoint_raw_record_count": (None if binding is None else binding.raw_record_count),
                "paper_tail_commit_count": certification.alignment.paper_tail_commit_count,
                "paper_store_id": self.paper_store_id,
                "raw_lake_id": self.raw_lake_id,
                "raw_store_id": self.raw_store_id,
                "run_id": self.run_id,
                "status": certification.alignment.status.value,
                "terminal_raw_generation": certification.alignment.raw_generation,
                "terminal_raw_manifest_root": (
                    None
                    if certification.alignment.raw_manifest_root is None
                    else certification.alignment.raw_manifest_root.hex()
                ),
                "terminal_raw_record_count": certification.alignment.raw_record_count,
                "code_identity": self.code_identity,
                "config_identity": self.config_identity,
                "runtime_identity": self.runtime_identity,
            },
            "batching": {
                "batch_count": self.batch_count,
                "max_batch_commits_observed": self.max_batch_commits_observed,
            },
            "candidate_id": self.candidate_id,
            "checkpoint_commit_count": self.checkpoint_commit_count,
            "exactness": {
                "final_prefix_root": self.oracle.final_prefix_root,
                "oracle_commits_compared": self.oracle.commit_count,
                "oracle_rows_compared": self.oracle.logical_row_count,
                "status": self.exactness.value,
                "workload_sha256": self.oracle.workload_sha256,
            },
            "startup_counters": {
                "paper_historical_commits_not_read": (
                    certification.paper_startup.historical_commits_not_read
                ),
                "paper_historical_commits_read": 0,
                "paper_segments_read": certification.paper_startup.segments_read,
                "paper_tail_entries_replayed": (certification.paper_startup.tail_entries_replayed),
                "paper_tail_rows_replayed": certification.paper_startup.tail_rows_replayed,
                "raw_historical_segments_read": (certification.raw_startup.historical_segments_read),
                "raw_manifests_opened": certification.raw_startup.manifests_opened,
                "raw_namespace_entries_scanned": (
                    certification.raw_startup.manifest_namespace_entries_scanned
                ),
            },
            "startup_file_access_trace": self.startup_file_trace.payload(),
            "tail_entries": self.tail_entries,
            "terminal_commit_count": self.oracle.commit_count,
            "timings_ns": {
                "certification_with_reopen": self.certification_with_reopen_ns,
                "independent_oracle": self.independent_oracle_ns,
                "startup_authentication": self.startup_ns,
            },
        }


@dataclass(frozen=True, slots=True)
class BoundedTailRestartMatrixReport:
    """Canonical, non-terminal report for one bounded-tail restart matrix."""

    candidate_root: Path
    audited_candidate_tree: CandidateTreeWitness
    manifest: CapacityWorkloadManifest
    batch_size: int
    requested_tail_sizes: tuple[int, ...]
    cases: tuple[TailRestartCaseReport, ...]
    test_override: bool

    def __post_init__(self) -> None:
        if not isinstance(self.candidate_root, Path) or not self.candidate_root.is_absolute():
            raise ValueError("candidate_root must be an absolute pathlib.Path")
        if (
            not isinstance(self.audited_candidate_tree, CandidateTreeWitness)
            or self.audited_candidate_tree.root != self.candidate_root
        ):
            raise ValueError("audited candidate tree must bind the exact tail matrix root")
        if not isinstance(self.manifest, CapacityWorkloadManifest):
            raise TypeError("manifest must be CapacityWorkloadManifest")
        if type(self.batch_size) is not int or self.batch_size < 1:
            raise ValueError("batch_size must be a positive exact integer")
        if tuple(case.tail_entries for case in self.cases) != self.requested_tail_sizes:
            raise ValueError("case tail sizes differ from the requested matrix")
        if not self.cases or any(
            case.candidate_root.parent != self.candidate_root for case in self.cases
        ):
            raise ValueError("tail cases must be non-empty direct children of the matrix")
        expected_tree = compose_candidate_tree_witness(
            self.candidate_root,
            tuple(case.audited_candidate_tree for case in self.cases),
        )
        if self.audited_candidate_tree != expected_tree:
            raise ValueError("tail matrix tree differs from its exact case composition")
        if type(self.test_override) is not bool:
            raise TypeError("test_override must be an exact bool")

    def payload(self) -> dict[str, object]:
        return {
            "artifact": _ARTIFACT,
            "audited_candidate_tree": self.audited_candidate_tree.payload(),
            "batch_size": self.batch_size,
            "candidate_root": str(self.candidate_root),
            "cases": [case.payload() for case in self.cases],
            "contract_scope": "TEST_OVERRIDE" if self.test_override else "PRODUCTION",
            "markers": list(CAPACITY_MARKERS),
            "profile": self.manifest.config.profile.value,
            "requested_tail_sizes": list(self.requested_tail_sizes),
            "status": _STATUS,
            "timing_scopes": {
                "certification_with_reopen": (
                    "canonical certification including its authenticated reopen and audits"
                ),
                "independent_oracle": (
                    "full-workload comparison including authenticated tail; reopen excluded"
                ),
                "startup_authentication": ("raw and Paper reopen plus alignment; exhaustive audit excluded"),
            },
            "workload_manifest_sha256": self.manifest.sha256,
            "workload": {
                "commit_count": self.manifest.commit_count,
                "logical_row_count": self.manifest.logical_row_count,
                "sha256": self.manifest.workload_sha256,
            },
            "workload_sha256": self.manifest.workload_sha256,
        }

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.payload())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


def _is_link_or_reparse_point(path: Path) -> bool:
    if path.is_symlink():
        return True
    if os.name != "nt":
        return False
    try:
        path_stat = os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    attributes = int(getattr(path_stat, "st_file_attributes", 0))
    reparse_mask = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & reparse_mask)


def _require_fresh_root(root: Path) -> None:
    if not isinstance(root, Path) or not root.is_absolute():
        raise _error(
            TailMatrixRunnerErrorCode.PATH_INVALID,
            "candidate_root must be an absolute pathlib.Path",
        )
    if root.exists() or _is_link_or_reparse_point(root):
        raise _error(
            TailMatrixRunnerErrorCode.CANDIDATE_EXISTS,
            "candidate_root already exists or is a link/reparse point",
        )
    parent = root.parent
    if not parent.is_dir() or _is_link_or_reparse_point(parent):
        raise _error(
            TailMatrixRunnerErrorCode.PATH_INVALID,
            "candidate_root parent must be an existing regular directory",
        )
    try:
        resolved_parent = parent.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise _error(
            TailMatrixRunnerErrorCode.PATH_INVALID,
            "candidate_root parent cannot be resolved",
        ) from exc
    if os.path.normcase(os.fspath(resolved_parent)) != os.path.normcase(os.fspath(parent.absolute())):
        raise _error(
            TailMatrixRunnerErrorCode.PATH_INVALID,
            "candidate_root parent must not traverse a link or reparse point",
        )


def _derived_hash(
    manifest: CapacityWorkloadManifest,
    *,
    tail_entries: int,
    label: bytes,
) -> Hash32:
    digest = hashlib.sha256(_IDENTITY_DOMAIN)
    digest.update(len(label).to_bytes(4, "big"))
    digest.update(label)
    digest.update(bytes.fromhex(manifest.sha256))
    digest.update(tail_entries.to_bytes(8, "big"))
    return Hash32(digest.digest())


def _configs(
    manifest: CapacityWorkloadManifest,
    *,
    tail_entries: int,
    code_identity: Hash32,
    runtime_identity: Hash32,
) -> tuple[RawStoreConfig, RepositoryConfig]:
    suffix = manifest.sha256[:16]
    namespace = f"SYNTHETIC_STORAGE_V4_PHASE1C_TAIL/{suffix}/{tail_entries}"
    config_identity = _derived_hash(
        manifest,
        tail_entries=tail_entries,
        label=b"config",
    )
    return (
        RawStoreConfig(
            store_id=StoreId(f"{namespace}/raw"),
            lake_id=RawLakeId(f"{namespace}/lake"),
            config_identity=config_identity,
        ),
        RepositoryConfig(
            store_id=StoreId(f"{namespace}/paper"),
            run_id=RunId(f"SYNTHETIC_STORAGE_V4_PHASE1C_TAIL/{suffix}/run"),
            mode=StorageMode.V4_NATIVE,
            run_identity=OpaqueIdentity(_derived_hash(manifest, tail_entries=0, label=b"run")),
            config_identity=OpaqueIdentity(config_identity),
            code_identity=OpaqueIdentity(code_identity),
            runtime_identity=OpaqueIdentity(runtime_identity),
            start_prefix_root=_GENESIS_PREFIX_ROOT,
        ),
    )


def _validate_startup(
    *,
    raw_startup: RawStartupReport,
    paper_startup: StartupReport,
    alignment: Phase1CAuthorityReport,
    expected_alignment: Phase1CAuthorityStatus,
    checkpoint_commit_count: int,
    tail_entries: int,
    checkpoint: Phase1CSealResult,
) -> None:
    if (
        raw_startup.historical_segments_read != 0
        or raw_startup.manifests_opened != 1
        or raw_startup.manifest_namespace_entries_scanned != 0
        or paper_startup.segments_read != 0
        or not paper_startup.checkpoint_used
        or int(paper_startup.base_commit_sequence) != checkpoint_commit_count
        or paper_startup.historical_commits_not_read != checkpoint_commit_count
        or paper_startup.tail_entries_replayed != tail_entries
        or len(paper_startup.tail_frames) != tail_entries
        or alignment.status is not expected_alignment
        or alignment.paper_tail_commit_count != tail_entries
        or alignment.binding != checkpoint.binding
    ):
        raise _error(
            TailMatrixRunnerErrorCode.INTEGRITY_DIVERGENCE,
            "startup exceeded authenticated checkpoint plus exact bounded tail",
        )


def _validate_exactness(
    *,
    manifest: CapacityWorkloadManifest,
    checkpoint_commit_count: int,
    checkpoint: Phase1CSealResult,
    terminal_expectations: NativeAuditExpectations,
    certification: Phase1CCertificationReport,
    oracle: CapacityOracleReport,
) -> None:
    native_rows = sum(stream.row_count for stream in certification.native_audit.streams)
    if (
        checkpoint.expectations.commit_count != checkpoint_commit_count
        or checkpoint.binding.raw_record_count != checkpoint_commit_count
        or terminal_expectations.commit_count != manifest.commit_count
        or terminal_expectations.raw_reference_count != manifest.commit_count
        or certification.raw_audit.records_read != manifest.commit_count
        or certification.paper_audit.commits_read != checkpoint_commit_count
        or certification.paper_audit.rows_read + certification.paper_startup.tail_rows_replayed
        != manifest.logical_row_count
        or certification.native_audit.commit_count != manifest.commit_count
        or native_rows != manifest.logical_row_count
        or certification.native_audit.final_prefix_root != terminal_expectations.final_prefix_root
        or oracle.commit_count != manifest.commit_count
        or oracle.logical_row_count != manifest.logical_row_count
        or oracle.workload_sha256 != manifest.workload_sha256
        or oracle.final_prefix_root != terminal_expectations.final_prefix_root.hex()
        or oracle.market_gap_count != manifest.config.market_gap_count
    ):
        raise _error(
            TailMatrixRunnerErrorCode.INTEGRITY_DIVERGENCE,
            "raw, Paper, native, or independent full-workload oracle diverged",
        )


class BoundedTailRestartMatrixRunner:
    """Create and certify one fresh on-disk candidate per requested tail."""

    def __init__(
        self,
        *,
        candidate_root: Path,
        code_identity: Hash32,
        runtime_identity: Hash32,
        batch_size: int = 4_096,
        raw_thresholds: RawSegmentThresholds | None = None,
        progress: ProgressCallback | None = None,
        _test_tail_sizes: tuple[int, ...] | None = None,
    ) -> None:
        if not isinstance(candidate_root, Path) or not candidate_root.is_absolute():
            raise _error(
                TailMatrixRunnerErrorCode.PATH_INVALID,
                "candidate_root must be an absolute pathlib.Path",
            )
        if type(code_identity) is not Hash32:
            raise _error(
                TailMatrixRunnerErrorCode.TYPE_INVALID,
                "code_identity must be Hash32",
            )
        if type(runtime_identity) is not Hash32:
            raise _error(
                TailMatrixRunnerErrorCode.TYPE_INVALID,
                "runtime_identity must be Hash32",
            )
        if type(batch_size) is not int or not 1 <= batch_size <= 10_000:
            raise _error(
                TailMatrixRunnerErrorCode.TYPE_INVALID,
                "batch_size must be an exact integer in [1, 10000]",
            )
        if raw_thresholds is not None and type(raw_thresholds) is not RawSegmentThresholds:
            raise _error(
                TailMatrixRunnerErrorCode.TYPE_INVALID,
                "raw_thresholds must be RawSegmentThresholds or None",
            )
        if progress is not None and not callable(progress):
            raise _error(
                TailMatrixRunnerErrorCode.TYPE_INVALID,
                "progress must be callable or None",
            )
        if _test_tail_sizes is not None and (
            type(_test_tail_sizes) is not tuple
            or not _test_tail_sizes
            or any(type(value) is not int or value < 0 for value in _test_tail_sizes)
            or tuple(sorted(set(_test_tail_sizes))) != _test_tail_sizes
        ):
            raise _error(
                TailMatrixRunnerErrorCode.TYPE_INVALID,
                "_test_tail_sizes must be a non-empty sorted unique tuple",
            )
        self._candidate_root = candidate_root
        self._code_identity = code_identity
        self._runtime_identity = runtime_identity
        self._batch_size = batch_size
        self._raw_thresholds = raw_thresholds or RawSegmentThresholds()
        self._progress = progress
        self._test_tail_sizes = _test_tail_sizes

    def _tail_sizes(self, manifest: CapacityWorkloadManifest) -> tuple[int, ...]:
        if self._test_tail_sizes is not None:
            sizes = self._test_tail_sizes
        else:
            config = manifest.config
            if config.profile is not CapacityProfile.BOUNDED_TAIL_RESTART:
                raise _error(
                    TailMatrixRunnerErrorCode.CONTRACT_INVALID,
                    "production tail matrix requires BOUNDED_TAIL_RESTART",
                )
            bounded_max = config.bounded_tail_max
            if bounded_max is None:
                raise _error(
                    TailMatrixRunnerErrorCode.CONTRACT_INVALID,
                    "production tail matrix has no bounded_tail_max",
                )
            sizes = config.tail_restart_sizes
            if sizes != (*_PRODUCTION_FIXED_TAILS, bounded_max):
                raise _error(
                    TailMatrixRunnerErrorCode.CONTRACT_INVALID,
                    "production matrix requires five distinct canonical tail sizes",
                )
        if sizes[-1] >= manifest.commit_count:
            raise _error(
                TailMatrixRunnerErrorCode.CONTRACT_INVALID,
                "largest tail must be smaller than the workload commit count",
            )
        return sizes

    def run(
        self,
        manifest: CapacityWorkloadManifest,
    ) -> BoundedTailRestartMatrixReport:
        if not isinstance(manifest, CapacityWorkloadManifest):
            raise _error(
                TailMatrixRunnerErrorCode.TYPE_INVALID,
                "manifest must be CapacityWorkloadManifest",
            )
        sizes = self._tail_sizes(manifest)
        _require_fresh_root(self._candidate_root)
        self._candidate_root.mkdir()
        cases: list[TailRestartCaseReport] = []
        for case_number, tail_entries in enumerate(sizes, start=1):
            case = self._run_case(
                manifest,
                tail_entries,
                cases_completed=case_number,
                cases_total=len(sizes),
            )
            cases.append(case)
        expected_tree = compose_candidate_tree_witness(
            self._candidate_root,
            tuple(case.audited_candidate_tree for case in cases),
        )
        try:
            audited_candidate_tree = witness_candidate_tree(
                self._candidate_root,
                progress=self._progress,
            )
        except CandidateTreeWitnessError as error:
            raise _error(
                TailMatrixRunnerErrorCode.INTEGRITY_DIVERGENCE,
                f"tail matrix could not be bound after all case audits: {error}",
            ) from error
        if audited_candidate_tree != expected_tree:
            raise _error(
                TailMatrixRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "tail matrix tree differs from the exact audited case composition",
            )
        return BoundedTailRestartMatrixReport(
            candidate_root=self._candidate_root,
            audited_candidate_tree=audited_candidate_tree,
            manifest=manifest,
            batch_size=self._batch_size,
            requested_tail_sizes=sizes,
            cases=tuple(cases),
            test_override=self._test_tail_sizes is not None,
        )

    def _run_case(
        self,
        manifest: CapacityWorkloadManifest,
        tail_entries: int,
        *,
        cases_completed: int,
        cases_total: int,
    ) -> TailRestartCaseReport:
        case_started_ns = time.perf_counter_ns()
        checkpoint_commit_count = manifest.commit_count - tail_entries
        workload_id = f"{manifest.sha256}:tail:{tail_entries}"
        candidate_id = f"tail-{tail_entries:020d}"
        candidate = self._candidate_root / candidate_id
        candidate.mkdir()
        anchors = candidate / "anchors"
        staging = candidate / "staging"
        anchors.mkdir()
        staging.mkdir()
        raw_root = candidate / "raw"
        paper_root = candidate / "paper"
        paper_paths = RepositoryPaths.from_root(paper_root)
        raw_config, paper_config = _configs(
            manifest,
            tail_entries=tail_entries,
            code_identity=self._code_identity,
            runtime_identity=self._runtime_identity,
        )
        raw_anchor = LocalAnchor.create(
            anchors / "raw.sqlite3",
            store_id=raw_config.store_id,
        )
        paper_anchor = LocalAnchor.create(
            anchors / "paper.sqlite3",
            store_id=paper_config.store_id,
        )

        hasher = CapacityWorkloadHasher()
        adapter = SyntheticCapacityPhase1CAdapter(
            run_id=paper_config.run_id,
            start_prefix_root=paper_config.start_prefix_root,
            max_batch_commits=self._batch_size,
        )
        pending: list[SyntheticCapacityCommit] = []
        batch_count = 0
        commits_completed = 0
        logical_rows_completed = 0
        max_batch_commits_observed = 0
        raw_segment_count = 0
        paper_segment_count = 0
        checkpoint_count = 0
        checkpoint: Phase1CSealResult | None = None
        terminal_expectations: NativeAuditExpectations | None = None

        def emit_snapshot(payload: Mapping[str, object]) -> None:
            if self._progress is None:
                return
            elapsed_ns = time.perf_counter_ns() - case_started_ns
            snapshot: dict[str, object] = {
                "workload": "SYNTHETIC_BOUNDED_TAIL_RESTART_V1",
                "workload_profile": manifest.config.profile.value,
                "workload_id": workload_id,
                "workload_manifest_sha256": manifest.sha256,
                "workload_sha256": manifest.workload_sha256,
                "tail_entries": tail_entries,
                "commits_completed": commits_completed,
                "commits_total": manifest.commit_count,
                "logical_rows_completed": logical_rows_completed,
                "logical_rows_total": manifest.logical_row_count,
                "elapsed_ns": elapsed_ns,
                "workload_elapsed_ns": elapsed_ns,
                "cpu_ns": None,
                "cpu_status": "UNAVAILABLE_NOT_MEASURED_FOR_BOUNDED_TAIL_CASE",
                "peak_rss_bytes": None,
                "peak_rss_status": "UNAVAILABLE_NOT_MEASURED_FOR_BOUNDED_TAIL_CASE",
                "bytes_written": None,
                "bytes_written_status": (
                    "UNAVAILABLE_NOT_MEASURED_FOR_BOUNDED_TAIL_CASE"
                ),
                "raw_segment_count": raw_segment_count,
                "paper_segment_count": paper_segment_count,
                "segment_count": raw_segment_count + paper_segment_count,
                "checkpoint_count": checkpoint_count,
                "segment_checkpoint_status": "EXACT_DURABLE_PUBLICATION_COUNTS",
                "progress_metrics_scope": (
                    "CURRENT_BOUNDED_TAIL_CASE_AT_COMPLETED_PROGRESS_BOUNDARY; "
                    "CPU_RSS_AND_WRITE_BYTES_NOT_MEASURED"
                ),
            }
            snapshot.update(payload)
            self._progress(snapshot)

        emit_snapshot(
            {
                "batch_count": 0,
                "checkpoint_boundary_commit_count": checkpoint_commit_count,
                "phase": "bounded_tail_ingest",
                "status": "STARTED",
            }
        )

        raw = RawStore.create(raw_root, anchor=raw_anchor, config=raw_config)
        try:
            paper = StorageRepository.create(
                paper_root,
                anchor=paper_anchor,
                config=paper_config,
            )
            try:
                writer = Phase1CWriter(
                    raw_store=raw,
                    paper_repository=paper,
                    staging_directory=staging,
                    raw_thresholds=self._raw_thresholds,
                )

                def flush_batch(*, publish_checkpoint: bool = False) -> None:
                    nonlocal batch_count
                    nonlocal checkpoint
                    nonlocal checkpoint_count
                    nonlocal commits_completed
                    nonlocal logical_rows_completed
                    nonlocal max_batch_commits_observed
                    nonlocal paper_segment_count
                    nonlocal raw_segment_count
                    if not pending:
                        return
                    batch = tuple(pending)
                    pending.clear()
                    batch_result = writer.append_batch(adapter.build_phase1c_batch(batch))
                    raw_segment_count += len(batch_result.raw_seals)
                    commits_completed += len(batch)
                    logical_rows_completed += sum(len(commit.rows) for commit in batch)
                    batch_count += 1
                    max_batch_commits_observed = max(
                        max_batch_commits_observed,
                        len(batch),
                    )
                    if publish_checkpoint:
                        checkpoint = writer.seal(adapter.checkpoint_state())
                        paper_segment_count += 1
                        checkpoint_count += 1
                    emit_snapshot(
                        {
                            "batch_count": batch_count,
                            "checkpoint_boundary_commit_count": (
                                checkpoint_commit_count if publish_checkpoint else None
                            ),
                            "phase": (
                                "bounded_tail_checkpoint"
                                if publish_checkpoint
                                else "bounded_tail_ingest"
                            ),
                            "status": (
                                "CHECKPOINT_PUBLISHED" if publish_checkpoint else "RUNNING"
                            ),
                        }
                    )

                for commit in iter_capacity_commits(manifest.config):
                    hasher.update(commit)
                    pending.append(commit)
                    if commit.sequence == checkpoint_commit_count:
                        flush_batch(publish_checkpoint=True)
                    elif len(pending) == self._batch_size:
                        flush_batch()
                flush_batch()
                terminal_expectations = writer.audit_expectations()
                if paper.overlay_state.tail_commit_count != tail_entries:
                    raise _error(
                        TailMatrixRunnerErrorCode.INTEGRITY_DIVERGENCE,
                        "writer tail count differs before close",
                    )
            finally:
                paper.close()
        finally:
            raw.close()

        observed = hasher.finalize()
        if (
            observed.commit_count != manifest.commit_count
            or observed.logical_row_count != manifest.logical_row_count
            or observed.sha256 != manifest.workload_sha256
        ):
            raise _error(
                TailMatrixRunnerErrorCode.WORKLOAD_DIVERGENCE,
                "streamed workload differs from its frozen manifest",
            )
        if checkpoint is None or terminal_expectations is None or batch_count < 1:
            raise _error(
                TailMatrixRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "tail candidate did not produce its exact checkpoint boundary",
            )

        try:
            audited_candidate_tree = witness_candidate_tree(
                candidate,
                progress=emit_snapshot,
            )
        except CandidateTreeWitnessError as error:
            raise _error(
                TailMatrixRunnerErrorCode.INTEGRITY_DIVERGENCE,
                f"tail case could not be bound before read-only audits: {error}",
            ) from error

        startup_trace_paths = StartupTracePaths(
            candidate_root=candidate,
            raw_root=raw_root,
            paper_root=paper_root,
            raw_anchor=raw_anchor.path,
            paper_anchor=paper_anchor.path,
            raw_anchor_writer_lease=raw_anchor.writer_lease_path,
            paper_anchor_writer_lease=paper_anchor.writer_lease_path,
            paper_writer_lease=paper_paths.writer_lease,
        )
        with trace_startup_file_access(startup_trace_paths) as startup_trace_recorder:
            startup_started = time.perf_counter_ns()
            startup_raw = RawStore.open_existing(
                raw_root,
                anchor=raw_anchor,
                config=raw_config,
            )
            try:
                startup_paper = StorageRepository.open_existing(
                    paper_root,
                    anchor=paper_anchor,
                    config=paper_config,
                )
                try:
                    startup_alignment = inspect_phase1c_alignment(
                        startup_raw,
                        startup_paper,
                    )
                    startup_ns = time.perf_counter_ns() - startup_started
                    startup_raw_report = startup_raw.startup_report
                    startup_paper_report = startup_paper.startup_report
                finally:
                    startup_paper.close()
            finally:
                startup_raw.close()
        startup_file_trace = startup_trace_recorder.result

        expected_alignment = (
            Phase1CAuthorityStatus.ALIGNED if tail_entries == 0 else Phase1CAuthorityStatus.RAW_AHEAD_OF_PAPER
        )
        _validate_startup(
            raw_startup=startup_raw_report,
            paper_startup=startup_paper_report,
            alignment=startup_alignment,
            expected_alignment=expected_alignment,
            checkpoint_commit_count=checkpoint_commit_count,
            tail_entries=tail_entries,
            checkpoint=checkpoint,
        )

        certification_started = time.perf_counter_ns()
        certification = certify_phase1c_reopen(
            raw_root=raw_root,
            raw_anchor=raw_anchor,
            raw_config=raw_config,
            paper_root=paper_root,
            paper_anchor=paper_anchor,
            paper_config=paper_config,
            binding=checkpoint.binding,
            expectations=terminal_expectations,
            expected_tail_entries=tail_entries,
            checkpoint_expectations=checkpoint.expectations,
        )
        certification_with_reopen_ns = time.perf_counter_ns() - certification_started
        _validate_startup(
            raw_startup=certification.raw_startup,
            paper_startup=certification.paper_startup,
            alignment=certification.alignment,
            expected_alignment=expected_alignment,
            checkpoint_commit_count=checkpoint_commit_count,
            tail_entries=tail_entries,
            checkpoint=checkpoint,
        )
        if (
            startup_raw_report != certification.raw_startup
            or startup_paper_report != certification.paper_startup
            or startup_alignment != certification.alignment
        ):
            raise _error(
                TailMatrixRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "independent startup report differs from certification reopen",
            )

        oracle_raw = RawStore.open_existing(
            raw_root,
            anchor=raw_anchor,
            config=raw_config,
        )
        try:
            oracle_paper = StorageRepository.open_existing(
                paper_root,
                anchor=paper_anchor,
                config=paper_config,
            )
            try:
                oracle_started = time.perf_counter_ns()
                oracle = compare_capacity_native_exact(
                    oracle_paper,
                    DiskRawResolver(oracle_raw),
                    manifest,
                    run_id=paper_config.run_id,
                    include_tail=True,
                )
                independent_oracle_ns = time.perf_counter_ns() - oracle_started
            finally:
                oracle_paper.close()
        finally:
            oracle_raw.close()

        _validate_exactness(
            manifest=manifest,
            checkpoint_commit_count=checkpoint_commit_count,
            checkpoint=checkpoint,
            terminal_expectations=terminal_expectations,
            certification=certification,
            oracle=oracle,
        )
        try:
            post_audit_candidate_tree = witness_candidate_tree(
                candidate,
                progress=emit_snapshot,
            )
        except CandidateTreeWitnessError as error:
            raise _error(
                TailMatrixRunnerErrorCode.INTEGRITY_DIVERGENCE,
                f"tail case could not be rebound after read-only audits: {error}",
            ) from error
        if post_audit_candidate_tree != audited_candidate_tree:
            raise _error(
                TailMatrixRunnerErrorCode.INTEGRITY_DIVERGENCE,
                "tail case tree changed across read-only audits",
            )
        emit_snapshot(
            {
                "batch_count": batch_count,
                "cases_completed": cases_completed,
                "cases_total": cases_total,
                "phase": "bounded_tail_case_complete",
                "status": TailMatrixExactness.EXACT.value,
            }
        )
        return TailRestartCaseReport(
            candidate_id=candidate_id,
            candidate_root=candidate,
            audited_candidate_tree=audited_candidate_tree,
            raw_store_id=raw_config.store_id.value,
            raw_lake_id=raw_config.lake_id.value,
            paper_store_id=paper_config.store_id.value,
            run_id=paper_config.run_id.value,
            config_identity=raw_config.config_identity.hex(),
            code_identity=paper_config.code_identity.digest.hex(),
            runtime_identity=paper_config.runtime_identity.digest.hex(),
            tail_entries=tail_entries,
            checkpoint_commit_count=checkpoint_commit_count,
            batch_count=batch_count,
            max_batch_commits_observed=max_batch_commits_observed,
            startup_ns=startup_ns,
            certification_with_reopen_ns=certification_with_reopen_ns,
            independent_oracle_ns=independent_oracle_ns,
            certification=certification,
            startup_file_trace=startup_file_trace,
            oracle=oracle,
        )


__all__ = [
    "BoundedTailRestartMatrixReport",
    "BoundedTailRestartMatrixRunner",
    "TailMatrixExactness",
    "TailMatrixRunnerError",
    "TailMatrixRunnerErrorCode",
    "TailRestartCaseReport",
]
