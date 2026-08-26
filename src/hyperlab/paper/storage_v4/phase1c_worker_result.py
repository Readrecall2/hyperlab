"""Safe durable hand-off for a completed cumulative Phase 1C worker.

The multiprocessing queue remains an in-process transport. The durable
authority is canonical JSON published beside the candidate and independently
pinned by a SHA-256 emitted to the driver log. No persistent object is ever
deserialized with pickle.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol, cast

from .candidate_tree import (
    CandidateFileWitness,
    CandidateTreeWitness,
    CandidateTreeWitnessError,
    witness_candidate_tree,
)
from .canonical import canonical_json_bytes
from .capacity import (
    CapacityProfile,
    CapacityTypeSpec,
    CapacityWorkloadConfig,
    CapacityWorkloadDigest,
    CapacityWorkloadManifest,
)
from .capacity_runner import (
    CumulativeCapacityAccounting,
    CumulativeCapacityRunResult,
    DurableCapacityBoundaryCertificate,
)
from .durability import DurabilityError, durable_publish_immutable
from .raw_segment import RawSegmentThresholds
from .types import Hash32

_RECEIPT_ARTIFACT = "STORAGE_V4_PHASE_1C_CUMULATIVE_WORKER_RECEIPT_V1"
_RECEIPT_AUTHORITY_ARTIFACT = (
    "STORAGE_V4_PHASE_1C_CUMULATIVE_WORKER_RECEIPT_AUTHORITY_V1"
)
_PROMOTION_ARTIFACT = "STORAGE_V4_PHASE_1C_CUMULATIVE_WORKER_PROMOTION_V1"
_BOUNDARY_ARTIFACT = "STORAGE_V4_PHASE_1C_CUMULATIVE_BOUNDARY_CERTIFICATE_V1"
_PROMOTION_STATUS = "DURABLE_CUMULATIVE_WORKER_RESULT_PROMOTED"
_MAX_RECEIPT_BYTES = 8 * 1024 * 1024
_MAX_RECEIPT_AUTHORITY_BYTES = 64 * 1024
_MAX_PROMOTION_BYTES = 64 * 1024
_MAX_BOUNDARY_CERTIFICATE_BYTES = 4 * 1024 * 1024

_RECEIPT_KEYS = frozenset(
    {"artifact", "request", "request_sha256", "result", "result_sha256"}
)
_REQUEST_KEYS = frozenset(
    {
        "batch_size",
        "candidate_root",
        "code_identity",
        "manifests",
        "raw_thresholds",
        "resume_existing",
        "runtime_identity",
    }
)
_RESULT_KEYS = frozenset(
    {
        "accounting",
        "boundaries",
        "candidate_root",
        "identities",
        "terminal_manifest_sha256",
        "terminal_shared_candidate_tree",
    }
)
_IDENTITY_KEYS = frozenset(
    {
        "candidate_root",
        "code_identity",
        "config_identity",
        "paper_store_id",
        "raw_lake_id",
        "raw_store_id",
        "run_id",
        "runtime_identity",
    }
)
_BOUNDARY_SUMMARY_KEYS = frozenset(
    {
        "checkpoint_root",
        "commit_count",
        "manifest_sha256",
        "paper_manifest_root",
        "previous_sha256",
        "raw_manifest_root",
        "sha256",
    }
)
_ACCOUNTING_KEYS = frozenset(
    {
        "commits_generated",
        "commits_ingested",
        "generator_emissions",
        "prefix_commits_audited",
        "prefix_commits_reingested",
        "raw_commits_reused",
        "raw_seal_count",
        "resume_count",
        "store_count",
        "stream_count",
        "suffix_commits_reconstructed",
        "worker_count",
    }
)
_TREE_KEYS = frozenset(
    {
        "directories",
        "directory_count",
        "file_count",
        "files",
        "root",
        "total_bytes",
        "tree_sha256",
    }
)
_TREE_FILE_KEYS = frozenset({"relative_path", "sha256", "size_bytes"})
_CERTIFICATE_KEYS = frozenset(
    {
        "artifact",
        "authority",
        "boundary_commit_count",
        "boundary_manifest",
        "boundary_manifest_sha256",
        "evidence",
        "measurement",
        "previous_certificate_sha256",
        "terminal_manifest_sha256",
        "workload_prefix",
    }
)
_CERTIFICATE_AUTHORITY_KEYS = frozenset(
    {"checkpoint_root", "paper_manifest_root", "raw_manifest_root"}
)
_WORKLOAD_PREFIX_KEYS = frozenset(
    {"commit_count", "logical_row_count", "sha256"}
)
_MEASUREMENT_KEYS = frozenset(
    {
        "byte_census",
        "counts",
        "cpu_ns",
        "durations",
        "full_history_audit_ns",
        "markers",
        "metadata_authentication_ns",
        "observed_workload_sha256",
        "retained_bytes_per_raw_input_byte",
        "rss",
        "startup",
        "storage_growth_target",
        "throughput",
        "wall_ns",
        "workload_manifest_sha256",
        "write_amplification",
    }
)
_MEASUREMENT_COUNT_KEYS = frozenset(
    {"checkpoints", "commits", "logical_rows", "manifests", "segments"}
)
_EVIDENCE_KEYS = frozenset(
    {"authority", "batching", "integrity", "scopes", "startup"}
)
_EVIDENCE_BATCHING_KEYS = frozenset(
    {"batch_count", "max_batch_commits_observed", "seal_count"}
)
_EVIDENCE_INTEGRITY_KEYS = frozenset(
    {
        "alignment_status",
        "audited_candidate_tree",
        "commit_count",
        "final_prefix_root",
        "market_gap_count",
        "oracle_commit_count",
        "oracle_final_prefix_root",
        "oracle_logical_row_count",
        "oracle_workload_sha256",
        "raw_reference_count",
        "raw_reference_prefix_root",
    }
)
_EVIDENCE_SCOPE_KEYS = frozenset(
    {
        "checkpoint",
        "manifest",
        "metadata",
        "rss",
        "scratch",
        "scratch_status",
        "storage_rate",
        "wall",
    }
)
_EVIDENCE_STARTUP_KEYS = frozenset(
    {
        "file_access_trace",
        "paper_checkpoint_used",
        "paper_historical_commits_not_read",
        "paper_segments_read",
        "paper_tail_entries_replayed",
        "raw_historical_segments_read",
        "raw_manifest_namespace_entries_scanned",
        "raw_manifests_opened",
    }
)
_PROMOTION_KEYS = frozenset(
    {
        "artifact",
        "candidate_root",
        "code_identity",
        "config_identity",
        "receipt_path",
        "receipt_sha256",
        "receipt_size_bytes",
        "result_sha256",
        "runtime_identity",
        "status",
        "terminal_certificate_sha256",
        "terminal_tree_sha256",
    }
)
_RECEIPT_AUTHORITY_KEYS = frozenset(
    {
        "artifact",
        "candidate_root",
        "code_identity",
        "receipt_path",
        "receipt_sha256",
        "receipt_size_bytes",
        "request_sha256",
        "result_sha256",
        "runtime_identity",
    }
)
_RECEIPT_EVENT_KEYS = frozenset(
    {
        *_ACCOUNTING_KEYS,
        "candidate_root",
        "phase",
        "receipt_path",
        "receipt_sha256",
        "receipt_size_bytes",
        "result_sha256",
        "status",
        "worker_result_event",
    }
)
_MANIFEST_KEYS = frozenset(
    {
        "activity_rates",
        "artifact",
        "configuration",
        "expected",
        "generator_version",
        "golden_census_sha256",
        "markers",
        "payload_sizes",
        "profile",
        "seed",
        "strategies",
        "tail_restart_sizes",
        "temporal_cadence",
        "type_distribution",
    }
)
_MANIFEST_ACTIVITY_KEYS = frozenset(
    {
        "alert_every_commits",
        "incident_every_commits",
        "ledger_every_commits",
        "market_gap_count",
        "projection_every_commits",
    }
)
_MANIFEST_CONFIGURATION_KEYS = frozenset(
    {
        "activity_payload_bytes",
        "adversarial_schedule",
        "bounded_tail_max",
        "commit_count",
        "start_time_ns",
    }
)
_MANIFEST_ACTIVITY_PAYLOAD_KEYS = frozenset(
    {"alert", "incident", "ledger", "market_gap", "projection"}
)
_MANIFEST_ADVERSARIAL_KEYS = frozenset(
    {"boundary_intervals", "funding_burst_period", "funding_burst_width"}
)
_MANIFEST_EXPECTED_KEYS = frozenset(
    {"commit_count", "logical_row_count", "workload_sha256"}
)
_MANIFEST_TEMPORAL_KEYS = frozenset({"cadence_ns", "start_time_ns", "status"})
_MANIFEST_TYPE_KEYS = frozenset(
    {
        "payload_cardinality",
        "payload_max_bytes",
        "payload_min_bytes",
        "record_type",
        "stream",
        "weight",
    }
)
_RAW_THRESHOLD_KEYS = frozenset(
    {
        "max_logical_payload_bytes",
        "max_physical_bytes",
        "max_records",
        "max_single_payload_bytes",
    }
)


class Phase1CWorkerResultError(RuntimeError):
    """A durable worker receipt, pin, or certificate failed closed."""


def _fail(message: str) -> Phase1CWorkerResultError:
    return Phase1CWorkerResultError(
        f"PHASE1C_DURABLE_WORKER_RESULT_INVALID: {message}"
    )


def _require_sha256(value: object, *, label: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise _fail(f"{label} must be a lowercase SHA-256")
    return value


def _require_text(value: object, *, label: str) -> str:
    if type(value) is not str or not value:
        raise _fail(f"{label} must be non-empty exact text")
    return value


def _require_int(value: object, *, label: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise _fail(f"{label} must be an exact integer >= {minimum}")
    return value


def _exact_mapping(
    value: object,
    keys: frozenset[str],
    *,
    label: str,
) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise _fail(f"{label} has an unexpected key set")
    return cast(dict[str, object], value)


def _is_reparse(value: os.stat_result) -> bool:
    attributes = int(getattr(value, "st_file_attributes", 0))
    mask = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & mask)


def _lstat(path: Path, *, label: str) -> os.stat_result:
    try:
        value = path.lstat()
    except OSError as exc:
        raise _fail(f"{label} cannot be lstat-ed") from exc
    if stat.S_ISLNK(value.st_mode) or _is_reparse(value):
        raise _fail(f"{label} is a link or reparse point")
    return value


def _require_direct_directory(path: Path, *, label: str) -> None:
    if not isinstance(path, Path) or not path.is_absolute():
        raise _fail(f"{label} must be an absolute pathlib.Path")
    cursor = path
    first = True
    while True:
        observed = _lstat(
            cursor,
            label=label if first else f"{label} ancestor",
        )
        if not stat.S_ISDIR(observed.st_mode):
            raise _fail(f"{label} ancestry contains a non-directory")
        parent = cursor.parent
        if parent == cursor:
            break
        cursor = parent
        first = False
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise _fail(f"{label} cannot be resolved") from exc
    if os.path.normcase(os.fspath(resolved)) != os.path.normcase(os.fspath(path)):
        raise _fail(f"{label} is not a direct path")


def _stat_signature(value: os.stat_result) -> tuple[int, int, int, int]:
    return (
        int(value.st_dev),
        int(value.st_ino),
        int(value.st_size),
        int(value.st_mtime_ns),
    )


def _stable_read(path: Path, *, maximum_bytes: int, label: str) -> bytes:
    if not isinstance(path, Path) or not path.is_absolute():
        raise _fail(f"{label} path must be absolute")
    _require_direct_directory(path.parent, label=f"{label} parent")
    before_path = _lstat(path, label=label)
    if not stat.S_ISREG(before_path.st_mode):
        raise _fail(f"{label} is not a regular file")
    if before_path.st_size < 1 or before_path.st_size > maximum_bytes:
        raise _fail(f"{label} exceeds its exact size bound")
    flags = os.O_RDONLY
    flags |= int(getattr(os, "O_BINARY", 0))
    flags |= int(getattr(os, "O_NOINHERIT", 0))
    flags |= int(getattr(os, "O_NOFOLLOW", 0))
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise _fail(f"{label} cannot be opened directly") from exc
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or not os.path.samestat(
            before_path, opened
        ):
            raise _fail(f"{label} changed between lstat and open")
        if opened.st_size < 1 or opened.st_size > maximum_bytes:
            raise _fail(f"{label} exceeds its exact size bound")
        remaining = int(opened.st_size)
        chunks: list[bytes] = []
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                raise _fail(f"{label} ended before its advertised size")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise _fail(f"{label} grew while being read")
        after_descriptor = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    after_path = _lstat(path, label=label)
    if (
        not os.path.samestat(opened, after_descriptor)
        or not os.path.samestat(after_descriptor, after_path)
        or _stat_signature(before_path) != _stat_signature(opened)
        or _stat_signature(opened) != _stat_signature(after_descriptor)
        or _stat_signature(after_descriptor) != _stat_signature(after_path)
    ):
        raise _fail(f"{label} changed during its stable read")
    return b"".join(chunks)


def _load_canonical_mapping(
    path: Path,
    *,
    maximum_bytes: int,
    label: str,
) -> tuple[bytes, dict[str, object]]:
    encoded = _stable_read(path, maximum_bytes=maximum_bytes, label=label)
    try:
        decoded = json.loads(encoded.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise _fail(f"{label} is not strict JSON") from exc
    if type(decoded) is not dict or canonical_json_bytes(decoded) != encoded:
        raise _fail(f"{label} is not exact canonical JSON")
    return encoded, cast(dict[str, object], decoded)


def _publish_canonical(
    path: Path,
    payload: dict[str, object],
    *,
    maximum_bytes: int,
    label: str,
) -> bytes:
    encoded = canonical_json_bytes(payload)
    if not 1 <= len(encoded) <= maximum_bytes:
        raise _fail(f"{label} exceeds its exact size bound")
    _require_direct_directory(path.parent, label=f"{label} parent")
    try:
        durable_publish_immutable(path, encoded)
    except (DurabilityError, OSError, TypeError, ValueError) as exc:
        raise _fail(f"{label} could not be published immutably") from exc
    observed = _stable_read(path, maximum_bytes=maximum_bytes, label=label)
    if observed != encoded:
        raise _fail(f"{label} differs after immutable publication")
    return encoded


class CumulativeWorkerRequestLike(Protocol):
    @property
    def manifests(self) -> tuple[CapacityWorkloadManifest, ...]: ...

    @property
    def candidate_root(self) -> Path: ...

    @property
    def code_identity(self) -> Hash32: ...

    @property
    def runtime_identity(self) -> Hash32: ...

    @property
    def batch_size(self) -> int: ...

    @property
    def raw_thresholds(self) -> RawSegmentThresholds | None: ...

    @property
    def resume_existing(self) -> bool: ...


def _validate_manifest_prefixes(
    manifests: tuple[CapacityWorkloadManifest, ...],
) -> None:
    if (
        type(manifests) is not tuple
        or len(manifests) < 2
        or any(
            not isinstance(item, CapacityWorkloadManifest) for item in manifests
        )
    ):
        raise _fail(
            "manifests must be an exact tuple with at least two boundaries"
        )
    counts = tuple(item.commit_count for item in manifests)
    if counts != tuple(sorted(set(counts))):
        raise _fail("manifest commit counts must be strictly increasing")
    terminal = manifests[-1]
    if any(
        replace(
            item.config,
            commit_count=terminal.config.commit_count,
            market_gap_count=terminal.config.market_gap_count,
        )
        != terminal.config
        for item in manifests[:-1]
    ):
        raise _fail("manifests differ outside exact prefix counts")


def _receipt_path(candidate_root: Path) -> Path:
    return candidate_root.parent / (
        f".{candidate_root.name}.phase1c-worker-result.json"
    )


def phase1c_cumulative_worker_receipt_authority_path(
    candidate_root: Path,
) -> Path:
    if not isinstance(candidate_root, Path) or not candidate_root.is_absolute():
        raise _fail("candidate_root must be absolute")
    return candidate_root.parent / (
        f".{candidate_root.name}.phase1c-worker-result.receipt-authority.json"
    )


def _promotion_path(candidate_root: Path) -> Path:
    return candidate_root.parent / (
        f".{candidate_root.name}.phase1c-worker-result.promoted.json"
    )


def phase1c_cumulative_worker_result_paths(
    candidate_root: Path,
) -> tuple[Path, Path]:
    if not isinstance(candidate_root, Path) or not candidate_root.is_absolute():
        raise _fail("candidate_root must be absolute")
    return _receipt_path(candidate_root), _promotion_path(candidate_root)


def _sidecar_present(path: Path, *, label: str) -> bool:
    try:
        observed = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise _fail(f"{label} cannot be inspected") from exc
    if stat.S_ISLNK(observed.st_mode) or _is_reparse(observed):
        raise _fail(f"{label} is a link or reparse point")
    return True


def validate_phase1c_cumulative_worker_sidecars(
    *,
    candidate_root: Path,
    resume_existing: bool,
) -> None:
    if type(resume_existing) is not bool:
        raise _fail("resume_existing must be an exact bool")
    receipt_path, promotion_path = phase1c_cumulative_worker_result_paths(
        candidate_root
    )
    authority_path = phase1c_cumulative_worker_receipt_authority_path(candidate_root)
    _require_direct_directory(candidate_root.parent, label="candidate parent")
    receipt_present = _sidecar_present(receipt_path, label="worker receipt")
    authority_present = _sidecar_present(
        authority_path,
        label="worker receipt authority",
    )
    promotion_present = _sidecar_present(
        promotion_path,
        label="worker promotion",
    )
    if not resume_existing and (
        receipt_present or authority_present or promotion_present
    ):
        raise _fail("fresh cumulative request has stale durable sidecars")
    if resume_existing and (authority_present or promotion_present) and not receipt_present:
        raise _fail("resume namespace contains terminal authority without its receipt")
    if resume_existing and (receipt_present or authority_present or promotion_present):
        raise _fail(
            "terminal worker receipt exists; use externally pinned closure-only "
            "promotion instead of ingestion resume"
        )


@dataclass(frozen=True, slots=True)
class Phase1CCumulativeWorkerResultQuery:
    manifests: tuple[CapacityWorkloadManifest, ...]
    candidate_root: Path
    code_identity: Hash32
    runtime_identity: Hash32
    batch_size: int
    raw_thresholds: RawSegmentThresholds | None = None
    resume_existing: bool = False

    def __post_init__(self) -> None:
        _validate_manifest_prefixes(self.manifests)
        _require_direct_directory(self.candidate_root, label="candidate_root")
        if (
            type(self.code_identity) is not Hash32
            or type(self.runtime_identity) is not Hash32
        ):
            raise _fail("code_identity and runtime_identity must be Hash32")
        if (
            type(self.batch_size) is not int
            or not 1 <= self.batch_size <= 10_000
        ):
            raise _fail(
                "batch_size must be an exact integer between 1 and 10000"
            )
        if (
            self.raw_thresholds is not None
            and type(self.raw_thresholds) is not RawSegmentThresholds
        ):
            raise _fail("raw_thresholds must be RawSegmentThresholds or None")
        if type(self.resume_existing) is not bool:
            raise _fail("resume_existing must be an exact bool")

    @classmethod
    def from_request(
        cls,
        request: CumulativeWorkerRequestLike,
    ) -> Phase1CCumulativeWorkerResultQuery:
        return cls(
            manifests=request.manifests,
            candidate_root=request.candidate_root,
            code_identity=request.code_identity,
            runtime_identity=request.runtime_identity,
            batch_size=request.batch_size,
            raw_thresholds=request.raw_thresholds,
            resume_existing=request.resume_existing,
        )


@dataclass(frozen=True, slots=True)
class Phase1CWorkerResultReceipt:
    path: Path
    sha256: str
    size_bytes: int
    result_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.path, Path) or not self.path.is_absolute():
            raise _fail("receipt path must be absolute")
        _require_sha256(self.sha256, label="receipt SHA-256")
        _require_sha256(
            self.result_sha256,
            label="receipt result SHA-256",
        )
        if (
            type(self.size_bytes) is not int
            or not 1 <= self.size_bytes <= _MAX_RECEIPT_BYTES
        ):
            raise _fail("receipt size is outside its bound")


@dataclass(frozen=True, slots=True)
class Phase1CWorkerReceiptAuthority:
    path: Path
    sha256: str
    size_bytes: int
    candidate_root: Path
    receipt: Phase1CWorkerResultReceipt
    request_sha256: str
    code_identity: str
    runtime_identity: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.candidate_root, Path)
            or not self.candidate_root.is_absolute()
        ):
            raise _fail("receipt authority candidate root must be absolute")
        if self.path != phase1c_cumulative_worker_receipt_authority_path(
            self.candidate_root
        ):
            raise _fail("receipt authority path is not the deterministic sibling")
        if not isinstance(self.receipt, Phase1CWorkerResultReceipt):
            raise _fail("receipt authority receipt has the wrong type")
        if self.receipt.path != _receipt_path(self.candidate_root):
            raise _fail("receipt authority binds a non-deterministic receipt")
        for label, value in (
            ("receipt authority SHA-256", self.sha256),
            ("receipt authority request SHA-256", self.request_sha256),
            ("receipt authority code identity", self.code_identity),
            ("receipt authority runtime identity", self.runtime_identity),
        ):
            _require_sha256(value, label=label)
        if (
            type(self.size_bytes) is not int
            or not 1 <= self.size_bytes <= _MAX_RECEIPT_AUTHORITY_BYTES
        ):
            raise _fail("receipt authority size is outside its bound")


@dataclass(frozen=True, slots=True)
class Phase1CWorkerResultPromotion:
    path: Path
    sha256: str
    receipt_sha256: str
    result_sha256: str
    terminal_certificate_sha256: str
    terminal_tree_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.path, Path) or not self.path.is_absolute():
            raise _fail("promotion path must be absolute")
        for label, value in (
            ("promotion SHA-256", self.sha256),
            ("promotion receipt SHA-256", self.receipt_sha256),
            ("promotion result SHA-256", self.result_sha256),
            (
                "promotion terminal certificate SHA-256",
                self.terminal_certificate_sha256,
            ),
            (
                "promotion terminal tree SHA-256",
                self.terminal_tree_sha256,
            ),
        ):
            _require_sha256(value, label=label)


@dataclass(frozen=True, slots=True)
class DurableCumulativeWorkerResult:
    query: Phase1CCumulativeWorkerResultQuery
    boundaries: tuple[DurableCapacityBoundaryCertificate, ...]
    terminal_shared_candidate_tree: CandidateTreeWitness
    accounting: CumulativeCapacityAccounting
    config_identity: str
    receipt: Phase1CWorkerResultReceipt
    promotion: Phase1CWorkerResultPromotion | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.query, Phase1CCumulativeWorkerResultQuery):
            raise _fail("durable query has the wrong type")
        if (
            type(self.boundaries) is not tuple
            or len(self.boundaries) != len(self.query.manifests)
            or any(
                not isinstance(item, DurableCapacityBoundaryCertificate)
                for item in self.boundaries
            )
            or tuple(item.commit_count for item in self.boundaries)
            != tuple(item.commit_count for item in self.query.manifests)
            or tuple(item.manifest_sha256 for item in self.boundaries)
            != tuple(item.sha256 for item in self.query.manifests)
        ):
            raise _fail("durable boundaries differ from the query manifests")
        if (
            not isinstance(
                self.terminal_shared_candidate_tree,
                CandidateTreeWitness,
            )
            or self.terminal_shared_candidate_tree.root
            != self.query.candidate_root
        ):
            raise _fail("terminal tree does not bind the queried candidate")
        if not isinstance(self.accounting, CumulativeCapacityAccounting):
            raise _fail("accounting has the wrong type")
        _require_sha256(self.config_identity, label="config identity")
        if not isinstance(self.receipt, Phase1CWorkerResultReceipt):
            raise _fail("receipt has the wrong type")
        if self.receipt.path != _receipt_path(self.query.candidate_root):
            raise _fail("receipt path is not the deterministic sibling")
        if self.promotion is not None:
            if not isinstance(
                self.promotion,
                Phase1CWorkerResultPromotion,
            ):
                raise _fail("promotion has the wrong type")
            if (
                self.promotion.path
                != _promotion_path(self.query.candidate_root)
                or self.promotion.receipt_sha256 != self.receipt.sha256
                or self.promotion.result_sha256
                != self.receipt.result_sha256
                or self.promotion.terminal_certificate_sha256
                != self.boundaries[-1].sha256
                or self.promotion.terminal_tree_sha256
                != self.terminal_shared_candidate_tree.tree_sha256
            ):
                raise _fail("promotion does not bind the durable result")

    def authority_payload(self) -> dict[str, object]:
        return {
            "artifact": (
                "STORAGE_V4_PHASE_1C_DURABLE_CUMULATIVE_WORKER_RESULT_V1"
            ),
            "candidate_root": str(self.query.candidate_root),
            "code_identity": self.query.code_identity.hex(),
            "config_identity": self.config_identity,
            "promotion": (
                None
                if self.promotion is None
                else {
                    "path": str(self.promotion.path),
                    "sha256": self.promotion.sha256,
                }
            ),
            "receipt": {
                "path": str(self.receipt.path),
                "result_sha256": self.receipt.result_sha256,
                "sha256": self.receipt.sha256,
                "size_bytes": self.receipt.size_bytes,
            },
            "runtime_identity": self.query.runtime_identity.hex(),
            "terminal_certificate_sha256": self.boundaries[-1].sha256,
            "terminal_tree_sha256": (
                self.terminal_shared_candidate_tree.tree_sha256
            ),
        }


@dataclass(frozen=True, slots=True)
class Phase1CCumulativeWorkerClosureResult:
    """Typed closure-only reconstruction of one durable cumulative result."""

    authority: Phase1CWorkerReceiptAuthority
    durable: DurableCumulativeWorkerResult

    def __post_init__(self) -> None:
        if not isinstance(self.authority, Phase1CWorkerReceiptAuthority):
            raise _fail("closure receipt authority has the wrong type")
        if not isinstance(self.durable, DurableCumulativeWorkerResult):
            raise _fail("closure durable result has the wrong type")
        if self.durable.promotion is None:
            raise _fail("closure durable result has not been promoted")
        if (
            self.authority.candidate_root != self.durable.query.candidate_root
            or self.authority.receipt != self.durable.receipt
            or self.authority.code_identity
            != self.durable.query.code_identity.hex()
            or self.authority.runtime_identity
            != self.durable.query.runtime_identity.hex()
            or self.authority.request_sha256
            != hashlib.sha256(
                canonical_json_bytes(_request_payload(self.durable.query))
            ).hexdigest()
        ):
            raise _fail("closure authority differs from its durable result")
        if (
            hashlib.sha256(canonical_json_bytes(self.payload())).hexdigest()
            != self.durable.receipt.result_sha256
        ):
            raise _fail("closure payload differs from the worker result digest")

    @property
    def candidate_root(self) -> Path:
        return self.durable.query.candidate_root

    @property
    def boundary_manifests(self) -> tuple[CapacityWorkloadManifest, ...]:
        return self.durable.query.manifests

    @property
    def boundaries(self) -> tuple[DurableCapacityBoundaryCertificate, ...]:
        return self.durable.boundaries

    @property
    def accounting(self) -> CumulativeCapacityAccounting:
        return self.durable.accounting

    def payload(self) -> dict[str, object]:
        return _result_payload(
            query=self.durable.query,
            boundaries=self.durable.boundaries,
            terminal_tree=self.durable.terminal_shared_candidate_tree,
            accounting=self.durable.accounting,
            identity=_identity_from_boundaries(self.durable),
        )


@dataclass(frozen=True, slots=True)
class Phase1CCumulativeWorkerQueueResult:
    result: CumulativeCapacityRunResult
    receipt: Phase1CWorkerResultReceipt

    def __post_init__(self) -> None:
        if not isinstance(self.result, CumulativeCapacityRunResult):
            raise _fail("queue result has the wrong live result type")
        if not isinstance(self.receipt, Phase1CWorkerResultReceipt):
            raise _fail("queue result has the wrong receipt type")


def _raw_thresholds_payload(
    thresholds: RawSegmentThresholds | None,
) -> dict[str, int]:
    effective = thresholds or RawSegmentThresholds()
    return {
        "max_logical_payload_bytes": effective.max_logical_payload_bytes,
        "max_physical_bytes": effective.max_physical_bytes,
        "max_records": effective.max_records,
        "max_single_payload_bytes": effective.max_single_payload_bytes,
    }


def _request_payload(
    query: Phase1CCumulativeWorkerResultQuery,
) -> dict[str, object]:
    return {
        "batch_size": query.batch_size,
        "candidate_root": str(query.candidate_root),
        "code_identity": query.code_identity.hex(),
        "manifests": [item.payload() for item in query.manifests],
        "raw_thresholds": _raw_thresholds_payload(query.raw_thresholds),
        "resume_existing": query.resume_existing,
        "runtime_identity": query.runtime_identity.hex(),
    }


def _tree_from_payload(
    value: object,
    *,
    candidate_root: Path,
    label: str,
) -> CandidateTreeWitness:
    payload = _exact_mapping(value, _TREE_KEYS, label=label)
    if payload["root"] != str(candidate_root):
        raise _fail(f"{label} binds a different candidate root")
    files_value = payload["files"]
    directories_value = payload["directories"]
    if type(files_value) is not list or type(directories_value) is not list:
        raise _fail(f"{label} files and directories must be exact lists")
    files: list[CandidateFileWitness] = []
    for index, item in enumerate(files_value):
        file_payload = _exact_mapping(
            item,
            _TREE_FILE_KEYS,
            label=f"{label} file {index}",
        )
        try:
            files.append(
                CandidateFileWitness(
                    relative_path=_require_text(
                        file_payload["relative_path"],
                        label=f"{label} file path",
                    ),
                    size_bytes=_require_int(
                        file_payload["size_bytes"],
                        label=f"{label} file size",
                    ),
                    sha256=_require_sha256(
                        file_payload["sha256"],
                        label=f"{label} file SHA-256",
                    ),
                )
            )
        except (TypeError, ValueError) as exc:
            raise _fail(f"{label} contains an invalid file witness") from exc
    if any(type(item) is not str for item in directories_value):
        raise _fail(f"{label} directories must contain exact text")
    file_count = _require_int(
        payload["file_count"],
        label=f"{label} file_count",
        minimum=1,
    )
    if file_count != len(files):
        raise _fail(f"{label} file_count differs from files")
    try:
        return CandidateTreeWitness(
            root=candidate_root,
            files=tuple(files),
            directories=tuple(cast(list[str], directories_value)),
            directory_count=_require_int(
                payload["directory_count"],
                label=f"{label} directory_count",
                minimum=1,
            ),
            total_bytes=_require_int(
                payload["total_bytes"],
                label=f"{label} total_bytes",
            ),
            tree_sha256=_require_sha256(
                payload["tree_sha256"],
                label=f"{label} tree SHA-256",
            ),
        )
    except (TypeError, ValueError) as exc:
        raise _fail(f"{label} is internally inconsistent") from exc


def _accounting_from_payload(value: object) -> CumulativeCapacityAccounting:
    payload = _exact_mapping(value, _ACCOUNTING_KEYS, label="worker accounting")
    values = {
        key: _require_int(payload[key], label=f"worker accounting {key}")
        for key in _ACCOUNTING_KEYS
    }
    try:
        accounting = CumulativeCapacityAccounting(
            commits_generated=values["commits_generated"],
            commits_ingested=values["commits_ingested"],
            prefix_commits_reingested=values["prefix_commits_reingested"],
            prefix_commits_audited=values["prefix_commits_audited"],
            suffix_commits_reconstructed=values[
                "suffix_commits_reconstructed"
            ],
            raw_commits_reused=values["raw_commits_reused"],
            raw_seal_count=values["raw_seal_count"],
            worker_count=values["worker_count"],
            store_count=values["store_count"],
            stream_count=values["stream_count"],
            resume_count=values["resume_count"],
        )
    except (TypeError, ValueError) as exc:
        raise _fail("worker accounting is internally inconsistent") from exc
    if accounting.payload() != payload:
        raise _fail("worker accounting derived fields diverged")
    return accounting


def _optional_int(
    value: object,
    *,
    label: str,
    minimum: int = 1,
) -> int | None:
    if value is None:
        return None
    return _require_int(value, label=label, minimum=minimum)


def _manifest_from_payload(value: object, *, index: int) -> CapacityWorkloadManifest:
    label = f"worker request manifest {index}"
    payload = _exact_mapping(value, _MANIFEST_KEYS, label=label)
    activity = _exact_mapping(
        payload["activity_rates"],
        _MANIFEST_ACTIVITY_KEYS,
        label=f"{label} activity rates",
    )
    configuration = _exact_mapping(
        payload["configuration"],
        _MANIFEST_CONFIGURATION_KEYS,
        label=f"{label} configuration",
    )
    activity_payload = _exact_mapping(
        configuration["activity_payload_bytes"],
        _MANIFEST_ACTIVITY_PAYLOAD_KEYS,
        label=f"{label} activity payload bytes",
    )
    adversarial = _exact_mapping(
        configuration["adversarial_schedule"],
        _MANIFEST_ADVERSARIAL_KEYS,
        label=f"{label} adversarial schedule",
    )
    expected = _exact_mapping(
        payload["expected"],
        _MANIFEST_EXPECTED_KEYS,
        label=f"{label} expected",
    )
    temporal = _exact_mapping(
        payload["temporal_cadence"],
        _MANIFEST_TEMPORAL_KEYS,
        label=f"{label} temporal cadence",
    )
    type_values = payload["type_distribution"]
    if type(type_values) is not list or not type_values:
        raise _fail(f"{label} type distribution must be a non-empty exact list")
    type_distribution: list[CapacityTypeSpec] = []
    for type_index, item in enumerate(type_values):
        type_payload = _exact_mapping(
            item,
            _MANIFEST_TYPE_KEYS,
            label=f"{label} type {type_index}",
        )
        try:
            type_distribution.append(
                CapacityTypeSpec(
                    record_type=_require_text(
                        type_payload["record_type"],
                        label=f"{label} record type",
                    ),
                    stream=_require_text(
                        type_payload["stream"],
                        label=f"{label} stream",
                    ),
                    weight=_require_int(
                        type_payload["weight"],
                        label=f"{label} weight",
                        minimum=1,
                    ),
                    payload_min_bytes=_require_int(
                        type_payload["payload_min_bytes"],
                        label=f"{label} payload minimum",
                    ),
                    payload_max_bytes=_require_int(
                        type_payload["payload_max_bytes"],
                        label=f"{label} payload maximum",
                    ),
                    payload_cardinality=_require_int(
                        type_payload["payload_cardinality"],
                        label=f"{label} payload cardinality",
                        minimum=1,
                    ),
                )
            )
        except (TypeError, ValueError) as exc:
            raise _fail(f"{label} contains an invalid type specification") from exc
    strategies_value = payload["strategies"]
    if (
        type(strategies_value) is not list
        or not strategies_value
        or any(type(item) is not str or not item for item in strategies_value)
    ):
        raise _fail(f"{label} strategies must be a non-empty exact text list")
    intervals_value = adversarial["boundary_intervals"]
    if type(intervals_value) is not list:
        raise _fail(f"{label} adversarial intervals must be an exact list")
    intervals = tuple(
        _require_int(
            item,
            label=f"{label} adversarial interval",
            minimum=2,
        )
        for item in intervals_value
    )
    golden_census_value = payload["golden_census_sha256"]
    golden_census_sha256 = (
        None
        if golden_census_value is None
        else _require_sha256(
            golden_census_value,
            label=f"{label} Golden census SHA-256",
        )
    )
    try:
        profile = CapacityProfile(
            _require_text(payload["profile"], label=f"{label} profile")
        )
        config = CapacityWorkloadConfig(
            profile=profile,
            seed=_require_int(payload["seed"], label=f"{label} seed"),
            commit_count=_require_int(
                configuration["commit_count"],
                label=f"{label} commit count",
                minimum=1,
            ),
            start_time_ns=_require_int(
                configuration["start_time_ns"],
                label=f"{label} start time",
            ),
            cadence_ns=_optional_int(
                temporal["cadence_ns"],
                label=f"{label} cadence",
            ),
            type_distribution=tuple(type_distribution),
            strategies=tuple(cast(list[str], strategies_value)),
            alert_every_commits=_optional_int(
                activity["alert_every_commits"],
                label=f"{label} alert interval",
            ),
            incident_every_commits=_optional_int(
                activity["incident_every_commits"],
                label=f"{label} incident interval",
            ),
            ledger_every_commits=_optional_int(
                activity["ledger_every_commits"],
                label=f"{label} ledger interval",
            ),
            market_gap_count=_require_int(
                activity["market_gap_count"],
                label=f"{label} market gap count",
            ),
            alert_payload_bytes=_require_int(
                activity_payload["alert"],
                label=f"{label} alert payload bytes",
            ),
            incident_payload_bytes=_require_int(
                activity_payload["incident"],
                label=f"{label} incident payload bytes",
            ),
            ledger_payload_bytes=_require_int(
                activity_payload["ledger"],
                label=f"{label} ledger payload bytes",
            ),
            market_gap_payload_bytes=_require_int(
                activity_payload["market_gap"],
                label=f"{label} market gap payload bytes",
            ),
            golden_census_sha256=golden_census_sha256,
            bounded_tail_max=_optional_int(
                configuration["bounded_tail_max"],
                label=f"{label} bounded tail max",
            ),
            projection_every_commits=_optional_int(
                activity["projection_every_commits"],
                label=f"{label} projection interval",
            ),
            projection_payload_bytes=_require_int(
                activity_payload["projection"],
                label=f"{label} projection payload bytes",
            ),
            adversarial_boundary_intervals=intervals,
        )
        manifest = CapacityWorkloadManifest(
            config=config,
            digest=CapacityWorkloadDigest(
                commit_count=_require_int(
                    expected["commit_count"],
                    label=f"{label} expected commit count",
                    minimum=1,
                ),
                logical_row_count=_require_int(
                    expected["logical_row_count"],
                    label=f"{label} expected logical rows",
                ),
                sha256=_require_sha256(
                    expected["workload_sha256"],
                    label=f"{label} workload SHA-256",
                ),
            ),
        )
    except (TypeError, ValueError) as exc:
        raise _fail(f"{label} is internally inconsistent") from exc
    if manifest.payload() != payload:
        raise _fail(f"{label} does not round-trip canonically")
    return manifest


def _raw_thresholds_from_payload(value: object) -> RawSegmentThresholds:
    payload = _exact_mapping(
        value,
        _RAW_THRESHOLD_KEYS,
        label="worker request raw thresholds",
    )
    try:
        thresholds = RawSegmentThresholds(
            max_records=_require_int(
                payload["max_records"],
                label="worker request max raw records",
                minimum=1,
            ),
            max_logical_payload_bytes=_require_int(
                payload["max_logical_payload_bytes"],
                label="worker request max logical raw bytes",
                minimum=1,
            ),
            max_physical_bytes=_require_int(
                payload["max_physical_bytes"],
                label="worker request max physical raw bytes",
                minimum=1,
            ),
            max_single_payload_bytes=_require_int(
                payload["max_single_payload_bytes"],
                label="worker request max single raw payload",
                minimum=1,
            ),
        )
    except (TypeError, ValueError) as exc:
        raise _fail("worker request raw thresholds are inconsistent") from exc
    if _raw_thresholds_payload(thresholds) != payload:
        raise _fail("worker request raw thresholds do not round-trip")
    return thresholds


def _certificate_directory(candidate_root: Path) -> Path:
    return candidate_root.parent / f".{candidate_root.name}.phase1c-boundaries"


def _certificate_path(
    directory: Path,
    manifest: CapacityWorkloadManifest,
) -> Path:
    return directory / f"{manifest.commit_count:016d}-{manifest.sha256}.json"


def _boundary_namespace(
    directory: Path,
    *,
    expected_names: frozenset[str],
) -> frozenset[str]:
    _require_direct_directory(directory, label="boundary certificate directory")
    published: set[str] = set()
    try:
        entries = tuple(os.scandir(directory))
    except OSError as exc:
        raise _fail("boundary certificate namespace cannot be enumerated") from exc
    for entry in entries:
        path = Path(entry.path)
        observed = _lstat(path, label="boundary namespace entry")
        if entry.name.startswith(".") and entry.name.endswith(".tmp"):
            if not stat.S_ISREG(observed.st_mode):
                raise _fail("boundary temporary entry is not a regular file")
            continue
        if (
            entry.name not in expected_names
            or not stat.S_ISREG(observed.st_mode)
            or entry.name in published
        ):
            raise _fail("boundary certificate namespace is forked or ambiguous")
        published.add(entry.name)
    if published != expected_names:
        raise _fail("boundary certificate namespace is incomplete")
    return frozenset(published)


def _identity_from_evidence(
    value: object,
    *,
    query: Phase1CCumulativeWorkerResultQuery,
) -> dict[str, object]:
    identity = _exact_mapping(
        value,
        _IDENTITY_KEYS,
        label="boundary evidence authority",
    )
    if (
        identity["candidate_root"] != str(query.candidate_root)
        or identity["code_identity"] != query.code_identity.hex()
        or identity["runtime_identity"] != query.runtime_identity.hex()
    ):
        raise _fail("boundary evidence authority differs from the query")
    for key in ("config_identity", "code_identity", "runtime_identity"):
        _require_sha256(identity[key], label=f"boundary authority {key}")
    for key in (
        "candidate_root",
        "paper_store_id",
        "raw_lake_id",
        "raw_store_id",
        "run_id",
    ):
        _require_text(identity[key], label=f"boundary authority {key}")
    return dict(identity)


def _validate_measurement(
    value: object,
    *,
    manifest: CapacityWorkloadManifest,
) -> dict[str, object]:
    measurement = _exact_mapping(
        value,
        _MEASUREMENT_KEYS,
        label="boundary measurement",
    )
    counts = _exact_mapping(
        measurement["counts"],
        _MEASUREMENT_COUNT_KEYS,
        label="boundary measurement counts",
    )
    if (
        counts["commits"] != manifest.commit_count
        or counts["logical_rows"] != manifest.logical_row_count
        or measurement["workload_manifest_sha256"] != manifest.sha256
        or measurement["observed_workload_sha256"]
        != manifest.workload_sha256
    ):
        raise _fail("boundary measurement differs from its manifest")
    for key in _MEASUREMENT_COUNT_KEYS:
        _require_int(
            counts[key],
            label=f"boundary measurement count {key}",
        )
    return measurement


def _validate_evidence(
    value: object,
    *,
    query: Phase1CCumulativeWorkerResultQuery,
    manifest: CapacityWorkloadManifest,
) -> tuple[dict[str, object], dict[str, object], CandidateTreeWitness]:
    evidence = _exact_mapping(
        value,
        _EVIDENCE_KEYS,
        label="boundary evidence",
    )
    identity = _identity_from_evidence(evidence["authority"], query=query)
    batching = _exact_mapping(
        evidence["batching"],
        _EVIDENCE_BATCHING_KEYS,
        label="boundary evidence batching",
    )
    for key in _EVIDENCE_BATCHING_KEYS:
        _require_int(
            batching[key],
            label=f"boundary evidence batching {key}",
            minimum=1,
        )
    integrity = _exact_mapping(
        evidence["integrity"],
        _EVIDENCE_INTEGRITY_KEYS,
        label="boundary evidence integrity",
    )
    _exact_mapping(
        evidence["scopes"],
        _EVIDENCE_SCOPE_KEYS,
        label="boundary evidence scopes",
    )
    _exact_mapping(
        evidence["startup"],
        _EVIDENCE_STARTUP_KEYS,
        label="boundary evidence startup",
    )
    tree = _tree_from_payload(
        integrity["audited_candidate_tree"],
        candidate_root=query.candidate_root,
        label="boundary audited candidate tree",
    )
    if (
        integrity["commit_count"] != manifest.commit_count
        or integrity["oracle_commit_count"] != manifest.commit_count
        or integrity["oracle_logical_row_count"]
        != manifest.logical_row_count
        or integrity["oracle_workload_sha256"]
        != manifest.workload_sha256
    ):
        raise _fail("boundary integrity evidence differs from its manifest")
    _require_text(
        integrity["alignment_status"],
        label="boundary alignment status",
    )
    for key in ("final_prefix_root", "oracle_final_prefix_root"):
        _require_sha256(
            integrity[key],
            label=f"boundary integrity {key}",
        )
    _require_sha256(
        integrity["raw_reference_prefix_root"],
        label="boundary raw reference prefix root",
    )
    for key in ("market_gap_count", "raw_reference_count"):
        _require_int(
            integrity[key],
            label=f"boundary integrity {key}",
        )
    return evidence, identity, tree


def _load_boundary_certificates(
    query: Phase1CCumulativeWorkerResultQuery,
) -> tuple[
    tuple[DurableCapacityBoundaryCertificate, ...],
    dict[str, object],
    CandidateTreeWitness,
]:
    directory = _certificate_directory(query.candidate_root)
    expected_by_name = {
        _certificate_path(directory, manifest).name: manifest
        for manifest in query.manifests
    }
    expected_names = frozenset(expected_by_name)
    _boundary_namespace(directory, expected_names=expected_names)
    certificates: list[DurableCapacityBoundaryCertificate] = []
    shared_identity: dict[str, object] | None = None
    terminal_tree: CandidateTreeWitness | None = None
    previous_sha256: str | None = None
    terminal_manifest = query.manifests[-1]
    for manifest in query.manifests:
        path = _certificate_path(directory, manifest)
        encoded, payload = _load_canonical_mapping(
            path,
            maximum_bytes=_MAX_BOUNDARY_CERTIFICATE_BYTES,
            label="boundary certificate",
        )
        certificate = _exact_mapping(
            payload,
            _CERTIFICATE_KEYS,
            label="boundary certificate",
        )
        authority = _exact_mapping(
            certificate["authority"],
            _CERTIFICATE_AUTHORITY_KEYS,
            label="boundary certificate authority",
        )
        workload_prefix = _exact_mapping(
            certificate["workload_prefix"],
            _WORKLOAD_PREFIX_KEYS,
            label="boundary workload prefix",
        )
        measurement = _validate_measurement(
            certificate["measurement"],
            manifest=manifest,
        )
        evidence, identity, audited_tree = _validate_evidence(
            certificate["evidence"],
            query=query,
            manifest=manifest,
        )
        if shared_identity is None:
            shared_identity = identity
        elif shared_identity != identity:
            raise _fail("boundary evidence authorities do not share one identity")
        if (
            certificate["artifact"] != _BOUNDARY_ARTIFACT
            or certificate["boundary_commit_count"] != manifest.commit_count
            or certificate["boundary_manifest"] != manifest.payload()
            or certificate["boundary_manifest_sha256"] != manifest.sha256
            or certificate["terminal_manifest_sha256"]
            != terminal_manifest.sha256
            or workload_prefix
            != {
                "commit_count": manifest.commit_count,
                "logical_row_count": manifest.logical_row_count,
                "sha256": manifest.workload_sha256,
            }
            or certificate["previous_certificate_sha256"]
            != previous_sha256
        ):
            raise _fail("boundary certificate does not bind the requested chain")
        roots: list[Hash32] = []
        for key in (
            "raw_manifest_root",
            "paper_manifest_root",
            "checkpoint_root",
        ):
            try:
                roots.append(
                    Hash32.from_hex(
                        _require_sha256(
                            authority[key],
                            label=f"boundary authority {key}",
                        )
                    )
                )
            except (TypeError, ValueError) as exc:
                raise _fail("boundary certificate contains an invalid root") from exc
        sha256 = hashlib.sha256(encoded).hexdigest()
        try:
            durable = DurableCapacityBoundaryCertificate(
                commit_count=manifest.commit_count,
                manifest_sha256=manifest.sha256,
                path=path,
                sha256=sha256,
                previous_sha256=previous_sha256,
                raw_manifest_root=roots[0],
                paper_manifest_root=roots[1],
                checkpoint_root=roots[2],
                canonical_payload=encoded,
                payload_mapping=certificate,
                measurement_mapping=measurement,
                evidence_mapping=evidence,
                typed_measurement=None,
                typed_evidence=None,
            )
        except (TypeError, ValueError) as exc:
            raise _fail("boundary certificate is internally inconsistent") from exc
        certificates.append(durable)
        previous_sha256 = sha256
        terminal_tree = audited_tree
    _boundary_namespace(directory, expected_names=expected_names)
    if shared_identity is None or terminal_tree is None:
        raise _fail("terminal boundary authority is missing")
    return tuple(certificates), shared_identity, terminal_tree


def _boundary_summary(
    certificate: DurableCapacityBoundaryCertificate,
) -> dict[str, object]:
    return {
        "checkpoint_root": certificate.checkpoint_root.hex(),
        "commit_count": certificate.commit_count,
        "manifest_sha256": certificate.manifest_sha256,
        "paper_manifest_root": certificate.paper_manifest_root.hex(),
        "previous_sha256": certificate.previous_sha256,
        "raw_manifest_root": certificate.raw_manifest_root.hex(),
        "sha256": certificate.sha256,
    }


def _result_payload(
    *,
    query: Phase1CCumulativeWorkerResultQuery,
    boundaries: tuple[DurableCapacityBoundaryCertificate, ...],
    terminal_tree: CandidateTreeWitness,
    accounting: CumulativeCapacityAccounting,
    identity: dict[str, object],
) -> dict[str, object]:
    return {
        "accounting": accounting.payload(),
        "boundaries": [_boundary_summary(item) for item in boundaries],
        "candidate_root": str(query.candidate_root),
        "identities": identity,
        "terminal_manifest_sha256": query.manifests[-1].sha256,
        "terminal_shared_candidate_tree": terminal_tree.payload(),
    }


def _identity_from_boundaries(
    durable: DurableCumulativeWorkerResult,
) -> dict[str, object]:
    evidence = _exact_mapping(
        durable.boundaries[-1].evidence_mapping,
        _EVIDENCE_KEYS,
        label="terminal boundary evidence",
    )
    return _identity_from_evidence(evidence["authority"], query=durable.query)


def _validate_live_result(
    query: Phase1CCumulativeWorkerResultQuery,
    result: CumulativeCapacityRunResult,
) -> tuple[
    dict[str, object],
    tuple[DurableCapacityBoundaryCertificate, ...],
    dict[str, object],
    CandidateTreeWitness,
]:
    if not isinstance(result, CumulativeCapacityRunResult):
        raise _fail("live cumulative result has the wrong type")
    if (
        result.candidate_root != query.candidate_root
        or result.boundary_manifests != query.manifests
        or result.terminal_manifest != query.manifests[-1]
        or result.terminal_shared_candidate_tree.root
        != query.candidate_root
    ):
        raise _fail("live cumulative result differs from the frozen query")
    boundaries, identity, terminal_tree = _load_boundary_certificates(query)
    if len(result.boundaries) != len(boundaries):
        raise _fail("live and durable boundary counts differ")
    for live, durable in zip(result.boundaries, boundaries, strict=True):
        if (
            live.commit_count != durable.commit_count
            or live.manifest_sha256 != durable.manifest_sha256
            or live.path != durable.path
            or live.sha256 != durable.sha256
            or live.previous_sha256 != durable.previous_sha256
            or live.raw_manifest_root != durable.raw_manifest_root
            or live.paper_manifest_root != durable.paper_manifest_root
            or live.checkpoint_root != durable.checkpoint_root
            or live.canonical_payload != durable.canonical_payload
            or dict(live.payload_mapping) != dict(durable.payload_mapping)
            or dict(live.measurement_mapping)
            != dict(durable.measurement_mapping)
            or dict(live.evidence_mapping) != dict(durable.evidence_mapping)
        ):
            raise _fail("live boundary differs from its durable certificate")
    if result.terminal_shared_candidate_tree != terminal_tree:
        raise _fail("live terminal tree differs from the terminal certificate")
    if result.accounting.commits_ingested != query.manifests[-1].commit_count:
        raise _fail("live accounting differs from the terminal manifest")
    payload = _result_payload(
        query=query,
        boundaries=boundaries,
        terminal_tree=terminal_tree,
        accounting=result.accounting,
        identity=identity,
    )
    return payload, boundaries, identity, terminal_tree


def persist_phase1c_cumulative_worker_result(
    request: CumulativeWorkerRequestLike,
    result: CumulativeCapacityRunResult,
) -> Phase1CCumulativeWorkerQueueResult:
    query = Phase1CCumulativeWorkerResultQuery.from_request(request)
    result_payload, _boundaries, _identity, _tree = _validate_live_result(
        query,
        result,
    )
    request_payload = _request_payload(query)
    envelope: dict[str, object] = {
        "artifact": _RECEIPT_ARTIFACT,
        "request": request_payload,
        "request_sha256": hashlib.sha256(
            canonical_json_bytes(request_payload)
        ).hexdigest(),
        "result": result_payload,
        "result_sha256": hashlib.sha256(
            canonical_json_bytes(result_payload)
        ).hexdigest(),
    }
    path = _receipt_path(query.candidate_root)
    encoded = _publish_canonical(
        path,
        envelope,
        maximum_bytes=_MAX_RECEIPT_BYTES,
        label="worker receipt",
    )
    receipt = Phase1CWorkerResultReceipt(
        path=path,
        sha256=hashlib.sha256(encoded).hexdigest(),
        size_bytes=len(encoded),
        result_sha256=cast(str, envelope["result_sha256"]),
    )
    return Phase1CCumulativeWorkerQueueResult(result=result, receipt=receipt)


def _load_pinned_receipt_envelope(
    candidate_root: Path,
    *,
    expected_receipt_sha256: str,
) -> tuple[
    bytes,
    dict[str, object],
    dict[str, object],
    dict[str, object],
    str,
    str,
]:
    expected_sha256 = _require_sha256(
        expected_receipt_sha256,
        label="externally pinned receipt SHA-256",
    )
    path = _receipt_path(candidate_root)
    encoded, decoded = _load_canonical_mapping(
        path,
        maximum_bytes=_MAX_RECEIPT_BYTES,
        label="worker receipt",
    )
    observed_sha256 = hashlib.sha256(encoded).hexdigest()
    if observed_sha256 != expected_sha256:
        raise _fail("worker receipt differs from its external pin")
    receipt = _exact_mapping(decoded, _RECEIPT_KEYS, label="worker receipt")
    if receipt["artifact"] != _RECEIPT_ARTIFACT:
        raise _fail("worker receipt artifact marker is invalid")
    request_payload = _exact_mapping(
        receipt["request"],
        _REQUEST_KEYS,
        label="worker receipt request",
    )
    request_sha256 = hashlib.sha256(
        canonical_json_bytes(request_payload)
    ).hexdigest()
    if receipt["request_sha256"] != request_sha256:
        raise _fail("worker receipt request digest is invalid")
    result_payload = _exact_mapping(
        receipt["result"],
        _RESULT_KEYS,
        label="worker receipt result",
    )
    result_sha256 = hashlib.sha256(
        canonical_json_bytes(result_payload)
    ).hexdigest()
    if receipt["result_sha256"] != result_sha256:
        raise _fail("worker receipt result digest is invalid")
    return (
        encoded,
        receipt,
        request_payload,
        result_payload,
        request_sha256,
        result_sha256,
    )


def load_phase1c_cumulative_worker_result_query(
    candidate_root: Path,
    *,
    expected_receipt_sha256: str,
) -> Phase1CCumulativeWorkerResultQuery:
    if not isinstance(candidate_root, Path) or not candidate_root.is_absolute():
        raise _fail("candidate_root must be absolute")
    _require_direct_directory(candidate_root, label="candidate_root")
    (
        _encoded,
        _receipt,
        request_payload,
        _result_payload_value,
        _request_sha256,
        _result_sha256,
    ) = _load_pinned_receipt_envelope(
        candidate_root,
        expected_receipt_sha256=expected_receipt_sha256,
    )
    if request_payload["candidate_root"] != str(candidate_root):
        raise _fail("worker receipt request binds a different candidate root")
    manifests_value = request_payload["manifests"]
    if type(manifests_value) is not list:
        raise _fail("worker receipt manifests must be an exact list")
    manifests = tuple(
        _manifest_from_payload(item, index=index)
        for index, item in enumerate(manifests_value)
    )
    resume_existing = request_payload["resume_existing"]
    if type(resume_existing) is not bool:
        raise _fail("worker receipt resume_existing must be an exact bool")
    try:
        query = Phase1CCumulativeWorkerResultQuery(
            manifests=manifests,
            candidate_root=candidate_root,
            code_identity=Hash32.from_hex(
                _require_sha256(
                    request_payload["code_identity"],
                    label="worker receipt code identity",
                )
            ),
            runtime_identity=Hash32.from_hex(
                _require_sha256(
                    request_payload["runtime_identity"],
                    label="worker receipt runtime identity",
                )
            ),
            batch_size=_require_int(
                request_payload["batch_size"],
                label="worker receipt batch size",
                minimum=1,
            ),
            raw_thresholds=_raw_thresholds_from_payload(
                request_payload["raw_thresholds"]
            ),
            resume_existing=resume_existing,
        )
    except (TypeError, ValueError) as exc:
        raise _fail("worker receipt query is internally inconsistent") from exc
    if _request_payload(query) != request_payload:
        raise _fail("worker receipt query does not round-trip canonically")
    return query


def load_phase1c_cumulative_worker_receipt(
    query: Phase1CCumulativeWorkerResultQuery,
    *,
    expected_receipt_sha256: str,
) -> DurableCumulativeWorkerResult:
    if not isinstance(query, Phase1CCumulativeWorkerResultQuery):
        raise _fail("query has the wrong type")
    path = _receipt_path(query.candidate_root)
    (
        encoded,
        _receipt,
        request_payload,
        result_payload,
        _request_sha256,
        result_sha256,
    ) = _load_pinned_receipt_envelope(
        query.candidate_root,
        expected_receipt_sha256=expected_receipt_sha256,
    )
    observed_sha256 = hashlib.sha256(encoded).hexdigest()
    expected_request_payload = _request_payload(query)
    if request_payload != expected_request_payload:
        raise _fail("worker receipt request differs from the frozen query")
    accounting = _accounting_from_payload(result_payload["accounting"])
    if accounting.commits_ingested != query.manifests[-1].commit_count:
        raise _fail("worker receipt accounting differs from terminal manifest")
    terminal_tree = _tree_from_payload(
        result_payload["terminal_shared_candidate_tree"],
        candidate_root=query.candidate_root,
        label="worker receipt terminal tree",
    )
    boundaries, identity, certificate_tree = _load_boundary_certificates(query)
    if terminal_tree != certificate_tree:
        raise _fail("worker receipt tree differs from terminal certificate")
    identities = _exact_mapping(
        result_payload["identities"],
        _IDENTITY_KEYS,
        label="worker receipt identities",
    )
    if identities != identity:
        raise _fail("worker receipt identities differ from boundary authority")
    boundary_summaries = result_payload["boundaries"]
    if type(boundary_summaries) is not list:
        raise _fail("worker receipt boundaries must be an exact list")
    for index, item in enumerate(boundary_summaries):
        _exact_mapping(
            item,
            _BOUNDARY_SUMMARY_KEYS,
            label=f"worker receipt boundary {index}",
        )
    expected_result_payload = _result_payload(
        query=query,
        boundaries=boundaries,
        terminal_tree=terminal_tree,
        accounting=accounting,
        identity=identity,
    )
    if result_payload != expected_result_payload:
        raise _fail("worker receipt result differs from durable authorities")
    return DurableCumulativeWorkerResult(
        query=query,
        boundaries=boundaries,
        terminal_shared_candidate_tree=terminal_tree,
        accounting=accounting,
        config_identity=_require_sha256(
            identity["config_identity"],
            label="worker receipt config identity",
        ),
        receipt=Phase1CWorkerResultReceipt(
            path=path,
            sha256=observed_sha256,
            size_bytes=len(encoded),
            result_sha256=result_sha256,
        ),
    )


def _receipt_authority_payload(
    query: Phase1CCumulativeWorkerResultQuery,
    receipt: Phase1CWorkerResultReceipt,
) -> dict[str, object]:
    request_sha256 = hashlib.sha256(
        canonical_json_bytes(_request_payload(query))
    ).hexdigest()
    return {
        "artifact": _RECEIPT_AUTHORITY_ARTIFACT,
        "candidate_root": str(query.candidate_root),
        "code_identity": query.code_identity.hex(),
        "receipt_path": str(receipt.path),
        "receipt_sha256": receipt.sha256,
        "receipt_size_bytes": receipt.size_bytes,
        "request_sha256": request_sha256,
        "result_sha256": receipt.result_sha256,
        "runtime_identity": query.runtime_identity.hex(),
    }


def persist_phase1c_cumulative_worker_receipt_authority(
    request: CumulativeWorkerRequestLike,
    event: Mapping[str, object],
) -> Phase1CWorkerReceiptAuthority:
    query = Phase1CCumulativeWorkerResultQuery.from_request(request)
    if not isinstance(event, Mapping):
        raise _fail("receipt event must be a mapping")
    event_payload = _exact_mapping(
        dict(event),
        _RECEIPT_EVENT_KEYS,
        label="durable receipt event",
    )
    if (
        event_payload["worker_result_event"] != "RECEIPT_DURABLE"
        or event_payload["status"] != "DURABLE_RECEIPT_UNPROMOTED"
        or event_payload["phase"] != "capacity_complete"
        or event_payload["candidate_root"] != str(query.candidate_root)
        or event_payload["receipt_path"] != str(_receipt_path(query.candidate_root))
    ):
        raise _fail("durable receipt event does not bind the frozen query")
    receipt_sha256 = _require_sha256(
        event_payload["receipt_sha256"],
        label="durable receipt event SHA-256",
    )
    durable = load_phase1c_cumulative_worker_receipt(
        query,
        expected_receipt_sha256=receipt_sha256,
    )
    if (
        event_payload["receipt_size_bytes"] != durable.receipt.size_bytes
        or event_payload["result_sha256"] != durable.receipt.result_sha256
        or {
            key: event_payload[key]
            for key in _ACCOUNTING_KEYS
        }
        != durable.accounting.payload()
    ):
        raise _fail("durable receipt event differs from the receipt authority")
    payload = _receipt_authority_payload(query, durable.receipt)
    path = phase1c_cumulative_worker_receipt_authority_path(query.candidate_root)
    encoded = _publish_canonical(
        path,
        payload,
        maximum_bytes=_MAX_RECEIPT_AUTHORITY_BYTES,
        label="worker receipt authority",
    )
    return Phase1CWorkerReceiptAuthority(
        path=path,
        sha256=hashlib.sha256(encoded).hexdigest(),
        size_bytes=len(encoded),
        candidate_root=query.candidate_root,
        receipt=durable.receipt,
        request_sha256=cast(str, payload["request_sha256"]),
        code_identity=query.code_identity.hex(),
        runtime_identity=query.runtime_identity.hex(),
    )


def load_phase1c_cumulative_worker_receipt_authority(
    candidate_root: Path,
    *,
    expected_receipt_sha256: str,
) -> Phase1CWorkerReceiptAuthority:
    expected_receipt = _require_sha256(
        expected_receipt_sha256,
        label="externally pinned receipt SHA-256",
    )
    query = load_phase1c_cumulative_worker_result_query(
        candidate_root,
        expected_receipt_sha256=expected_receipt,
    )
    durable = load_phase1c_cumulative_worker_receipt(
        query,
        expected_receipt_sha256=expected_receipt,
    )
    path = phase1c_cumulative_worker_receipt_authority_path(candidate_root)
    encoded, decoded = _load_canonical_mapping(
        path,
        maximum_bytes=_MAX_RECEIPT_AUTHORITY_BYTES,
        label="worker receipt authority",
    )
    payload = _exact_mapping(
        decoded,
        _RECEIPT_AUTHORITY_KEYS,
        label="worker receipt authority",
    )
    expected_payload = _receipt_authority_payload(query, durable.receipt)
    if payload != expected_payload:
        raise _fail("worker receipt authority differs from its pinned receipt")
    return Phase1CWorkerReceiptAuthority(
        path=path,
        sha256=hashlib.sha256(encoded).hexdigest(),
        size_bytes=len(encoded),
        candidate_root=candidate_root,
        receipt=durable.receipt,
        request_sha256=cast(str, payload["request_sha256"]),
        code_identity=query.code_identity.hex(),
        runtime_identity=query.runtime_identity.hex(),
    )


def require_phase1c_cumulative_worker_receipt_authority(
    authority: Phase1CWorkerReceiptAuthority,
    durable: DurableCumulativeWorkerResult,
) -> None:
    if (
        not isinstance(authority, Phase1CWorkerReceiptAuthority)
        or not isinstance(durable, DurableCumulativeWorkerResult)
    ):
        raise _fail("receipt authority attestation received the wrong type")
    observed = load_phase1c_cumulative_worker_receipt_authority(
        durable.query.candidate_root,
        expected_receipt_sha256=durable.receipt.sha256,
    )
    if observed != authority:
        raise _fail("receipt authority changed before promotion")


def attest_phase1c_cumulative_worker_queue_result(
    request: CumulativeWorkerRequestLike,
    value: object,
) -> tuple[CumulativeCapacityRunResult, DurableCumulativeWorkerResult]:
    if not isinstance(value, Phase1CCumulativeWorkerQueueResult):
        raise _fail("cumulative child returned a malformed queue result")
    query = Phase1CCumulativeWorkerResultQuery.from_request(request)
    if value.receipt.path != _receipt_path(query.candidate_root):
        raise _fail("queue receipt path differs from the deterministic sibling")
    durable = load_phase1c_cumulative_worker_receipt(
        query,
        expected_receipt_sha256=value.receipt.sha256,
    )
    if (
        durable.receipt.size_bytes != value.receipt.size_bytes
        or durable.receipt.result_sha256 != value.receipt.result_sha256
    ):
        raise _fail("queue receipt reference differs from durable bytes")
    live_payload, _boundaries, _identity, _tree = _validate_live_result(
        query,
        value.result,
    )
    if (
        hashlib.sha256(canonical_json_bytes(live_payload)).hexdigest()
        != durable.receipt.result_sha256
    ):
        raise _fail("live queue result differs from durable receipt summary")
    return value.result, durable


def _promotion_payload(
    durable: DurableCumulativeWorkerResult,
) -> dict[str, object]:
    return {
        "artifact": _PROMOTION_ARTIFACT,
        "candidate_root": str(durable.query.candidate_root),
        "code_identity": durable.query.code_identity.hex(),
        "config_identity": durable.config_identity,
        "receipt_path": str(durable.receipt.path),
        "receipt_sha256": durable.receipt.sha256,
        "receipt_size_bytes": durable.receipt.size_bytes,
        "result_sha256": durable.receipt.result_sha256,
        "runtime_identity": durable.query.runtime_identity.hex(),
        "status": _PROMOTION_STATUS,
        "terminal_certificate_sha256": durable.boundaries[-1].sha256,
        "terminal_tree_sha256": (
            durable.terminal_shared_candidate_tree.tree_sha256
        ),
    }


def promote_phase1c_cumulative_worker_result(
    durable: DurableCumulativeWorkerResult,
) -> DurableCumulativeWorkerResult:
    if not isinstance(durable, DurableCumulativeWorkerResult):
        raise _fail("durable result has the wrong type")
    refreshed = load_phase1c_cumulative_worker_receipt(
        durable.query,
        expected_receipt_sha256=durable.receipt.sha256,
    )
    if refreshed.authority_payload() != durable.authority_payload():
        raise _fail("receipt or certificate authority changed before promotion")
    path = _promotion_path(refreshed.query.candidate_root)
    payload = _promotion_payload(refreshed)
    encoded = _publish_canonical(
        path,
        payload,
        maximum_bytes=_MAX_PROMOTION_BYTES,
        label="worker promotion",
    )
    promotion = Phase1CWorkerResultPromotion(
        path=path,
        sha256=hashlib.sha256(encoded).hexdigest(),
        receipt_sha256=refreshed.receipt.sha256,
        result_sha256=refreshed.receipt.result_sha256,
        terminal_certificate_sha256=refreshed.boundaries[-1].sha256,
        terminal_tree_sha256=(
            refreshed.terminal_shared_candidate_tree.tree_sha256
        ),
    )
    return replace(refreshed, promotion=promotion)


def promote_orphaned_worker_result(
    query: Phase1CCumulativeWorkerResultQuery,
    *,
    expected_receipt_sha256: str,
) -> DurableCumulativeWorkerResult:
    durable = load_phase1c_cumulative_worker_receipt(
        query,
        expected_receipt_sha256=expected_receipt_sha256,
    )
    try:
        observed_tree = witness_candidate_tree(query.candidate_root)
    except (CandidateTreeWitnessError, OSError, TypeError, ValueError) as exc:
        raise _fail(
            "orphaned candidate tree could not be reattested"
        ) from exc
    if observed_tree != durable.terminal_shared_candidate_tree:
        raise _fail(
            "orphaned candidate tree changed after receipt publication"
        )
    return promote_phase1c_cumulative_worker_result(durable)


def close_phase1c_cumulative_worker_result_from_authority(
    candidate_root: Path,
    *,
    expected_receipt_sha256: str,
) -> Phase1CCumulativeWorkerClosureResult:
    authority = load_phase1c_cumulative_worker_receipt_authority(
        candidate_root,
        expected_receipt_sha256=expected_receipt_sha256,
    )
    query = load_phase1c_cumulative_worker_result_query(
        candidate_root,
        expected_receipt_sha256=expected_receipt_sha256,
    )
    durable = load_phase1c_cumulative_worker_receipt(
        query,
        expected_receipt_sha256=expected_receipt_sha256,
    )
    require_phase1c_cumulative_worker_receipt_authority(authority, durable)
    try:
        observed_tree = witness_candidate_tree(query.candidate_root)
    except (CandidateTreeWitnessError, OSError, TypeError, ValueError) as exc:
        raise _fail("orphaned candidate tree could not be reattested") from exc
    if observed_tree != durable.terminal_shared_candidate_tree:
        raise _fail("orphaned candidate tree changed after receipt publication")
    promotion_path = _promotion_path(candidate_root)
    if _sidecar_present(promotion_path, label="worker promotion"):
        expected_promotion_sha256 = hashlib.sha256(
            canonical_json_bytes(_promotion_payload(durable))
        ).hexdigest()
        promoted = load_phase1c_promoted_cumulative_worker_result(
            query,
            expected_promotion_sha256=expected_promotion_sha256,
        )
    else:
        promoted = promote_phase1c_cumulative_worker_result(durable)
    return Phase1CCumulativeWorkerClosureResult(
        authority=authority,
        durable=promoted,
    )


def load_phase1c_promoted_cumulative_worker_result(
    query: Phase1CCumulativeWorkerResultQuery,
    *,
    expected_promotion_sha256: str,
) -> DurableCumulativeWorkerResult:
    if not isinstance(query, Phase1CCumulativeWorkerResultQuery):
        raise _fail("query has the wrong type")
    expected_sha256 = _require_sha256(
        expected_promotion_sha256,
        label="externally pinned promotion SHA-256",
    )
    path = _promotion_path(query.candidate_root)
    encoded, decoded = _load_canonical_mapping(
        path,
        maximum_bytes=_MAX_PROMOTION_BYTES,
        label="worker promotion",
    )
    observed_sha256 = hashlib.sha256(encoded).hexdigest()
    if observed_sha256 != expected_sha256:
        raise _fail("worker promotion differs from its external pin")
    promotion_payload = _exact_mapping(
        decoded,
        _PROMOTION_KEYS,
        label="worker promotion",
    )
    if (
        promotion_payload["artifact"] != _PROMOTION_ARTIFACT
        or promotion_payload["status"] != _PROMOTION_STATUS
        or promotion_payload["candidate_root"] != str(query.candidate_root)
        or promotion_payload["code_identity"] != query.code_identity.hex()
        or promotion_payload["runtime_identity"]
        != query.runtime_identity.hex()
        or promotion_payload["receipt_path"]
        != str(_receipt_path(query.candidate_root))
    ):
        raise _fail("worker promotion does not bind the frozen query")
    receipt_sha256 = _require_sha256(
        promotion_payload["receipt_sha256"],
        label="promoted receipt SHA-256",
    )
    durable = load_phase1c_cumulative_worker_receipt(
        query,
        expected_receipt_sha256=receipt_sha256,
    )
    if (
        promotion_payload["config_identity"] != durable.config_identity
        or promotion_payload["receipt_size_bytes"]
        != durable.receipt.size_bytes
        or promotion_payload["result_sha256"]
        != durable.receipt.result_sha256
        or promotion_payload["terminal_certificate_sha256"]
        != durable.boundaries[-1].sha256
        or promotion_payload["terminal_tree_sha256"]
        != durable.terminal_shared_candidate_tree.tree_sha256
    ):
        raise _fail("worker promotion differs from the durable result")
    promotion = Phase1CWorkerResultPromotion(
        path=path,
        sha256=observed_sha256,
        receipt_sha256=receipt_sha256,
        result_sha256=durable.receipt.result_sha256,
        terminal_certificate_sha256=durable.boundaries[-1].sha256,
        terminal_tree_sha256=(
            durable.terminal_shared_candidate_tree.tree_sha256
        ),
    )
    return replace(durable, promotion=promotion)


__all__ = [
    "DurableCumulativeWorkerResult",
    "Phase1CCumulativeWorkerClosureResult",
    "Phase1CCumulativeWorkerQueueResult",
    "Phase1CCumulativeWorkerResultQuery",
    "Phase1CWorkerReceiptAuthority",
    "Phase1CWorkerResultError",
    "Phase1CWorkerResultPromotion",
    "Phase1CWorkerResultReceipt",
    "attest_phase1c_cumulative_worker_queue_result",
    "close_phase1c_cumulative_worker_result_from_authority",
    "load_phase1c_cumulative_worker_receipt",
    "load_phase1c_cumulative_worker_receipt_authority",
    "load_phase1c_cumulative_worker_result_query",
    "load_phase1c_promoted_cumulative_worker_result",
    "persist_phase1c_cumulative_worker_receipt_authority",
    "persist_phase1c_cumulative_worker_result",
    "phase1c_cumulative_worker_receipt_authority_path",
    "phase1c_cumulative_worker_result_paths",
    "promote_orphaned_worker_result",
    "promote_phase1c_cumulative_worker_result",
    "require_phase1c_cumulative_worker_receipt_authority",
    "validate_phase1c_cumulative_worker_sidecars",
]
