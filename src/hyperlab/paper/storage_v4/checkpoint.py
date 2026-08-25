"""Authenticated complete checkpoints for bounded Storage v4 recovery.

The checkpoint root is content-addressed under ``HL4-CHECKPOINT`` and binds the
manifest transition material, all four runtime identities, cumulative stream
counts, and every state section required for deterministic restart.  State is
strict canonical JSON and therefore admits no Python floats, NaN, Infinity, or
implicit ``Decimal`` values.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TypeAlias, cast

from .canonical import (
    PROTOCOL_VERSION,
    CanonicalizationError,
    canonical_json_bytes,
    frame_bytes,
    frame_hash32,
    frame_optional_hash32,
    frame_text,
    frame_u32,
    frame_u64,
    framed_hash,
)
from .manifest import OpaqueIdentity
from .types import (
    UINT32_MAX,
    UINT64_MAX,
    CanonicalObject,
    CommitSequence,
    Hash32,
    RunId,
    SegmentIdentity,
    StoreId,
    StreamId,
)

CHECKPOINT_MAGIC = b"HL4CHK\x00\x01"
CHECKPOINT_FORMAT_VERSION = 1
DOMAIN_CHECKPOINT = b"HL4-CHECKPOINT"
CHECKPOINT_STATE_SECTIONS = (
    "adapter",
    "ledger",
    "projection",
    "sessions",
    "incidents",
    "cursors",
    "stream_heads",
)
CHECKPOINT_STATE_WITNESS_DOMAIN = b"HL4-PHASE1B-CHECKPOINT-STATE\x00\x01"

CumulativeStreamCount: TypeAlias = tuple[StreamId, int]


class CheckpointFormatError(ValueError):
    """Checkpoint bytes or expected recovery bindings are invalid."""


@dataclass(frozen=True, slots=True)
class CheckpointReadLimits:
    """Fail-closed allocation limits applied before variable-size decoding."""

    max_physical_size: int = 64 * 1024 * 1024
    max_body_size: int = 64 * 1024 * 1024
    max_text_size: int = 1024 * 1024
    max_state_section_size: int = 32 * 1024 * 1024
    max_streams: int = 65_536

    def __post_init__(self) -> None:
        for label, value, maximum in (
            ("max_physical_size", self.max_physical_size, UINT64_MAX),
            ("max_body_size", self.max_body_size, UINT64_MAX),
            ("max_text_size", self.max_text_size, UINT32_MAX),
            ("max_state_section_size", self.max_state_section_size, UINT32_MAX),
            ("max_streams", self.max_streams, UINT32_MAX),
        ):
            if type(value) is not int:
                raise TypeError(f"{label} must be an exact integer")
            if value < 1 or value > maximum:
                raise ValueError(f"{label} must be positive and within its framing width")


def _decode_canonical_object(data: bytes, *, label: str) -> CanonicalObject:
    try:
        decoded = json.loads(data.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
        raise CheckpointFormatError(f"checkpoint {label} is not strict canonical JSON") from error
    if type(decoded) is not dict:
        raise CheckpointFormatError(f"checkpoint {label} must be a canonical object")
    try:
        reencoded = canonical_json_bytes(decoded)
    except CanonicalizationError as error:
        raise CheckpointFormatError(
            f"checkpoint {label} contains a non-canonical value"
        ) from error
    if reencoded != data:
        raise CheckpointFormatError(f"checkpoint {label} JSON encoding is not canonical")
    return cast(CanonicalObject, decoded)


@dataclass(frozen=True, slots=True, init=False)
class CheckpointState:
    """Immutable snapshots of every state section required for restart."""

    _canonical_sections: tuple[bytes, ...]

    def __init__(
        self,
        *,
        adapter: CanonicalObject,
        ledger: CanonicalObject,
        projection: CanonicalObject,
        sessions: CanonicalObject,
        incidents: CanonicalObject,
        cursors: CanonicalObject,
        stream_heads: CanonicalObject,
    ) -> None:
        values = (
            adapter,
            ledger,
            projection,
            sessions,
            incidents,
            cursors,
            stream_heads,
        )
        encoded: list[bytes] = []
        for label, value in zip(CHECKPOINT_STATE_SECTIONS, values, strict=True):
            if type(value) is not dict:
                raise TypeError(f"checkpoint {label} state must be an exact dict")
            try:
                encoded.append(canonical_json_bytes(value))
            except CanonicalizationError as error:
                raise CheckpointFormatError(
                    f"checkpoint {label} state is not canonical"
                ) from error
        object.__setattr__(self, "_canonical_sections", tuple(encoded))

    @classmethod
    def _from_canonical_sections(
        cls,
        sections: tuple[bytes, ...],
    ) -> CheckpointState:
        if len(sections) != len(CHECKPOINT_STATE_SECTIONS):
            raise CheckpointFormatError("checkpoint state section set is incomplete")
        decoded = tuple(
            _decode_canonical_object(value, label=label)
            for label, value in zip(CHECKPOINT_STATE_SECTIONS, sections, strict=True)
        )
        return cls(
            adapter=decoded[0],
            ledger=decoded[1],
            projection=decoded[2],
            sessions=decoded[3],
            incidents=decoded[4],
            cursors=decoded[5],
            stream_heads=decoded[6],
        )

    def _section(self, index: int) -> CanonicalObject:
        # Construction/authenticated decoding already established canonical JSON.
        return cast(CanonicalObject, json.loads(self._canonical_sections[index]))

    @property
    def adapter(self) -> CanonicalObject:
        return self._section(0)

    @property
    def ledger(self) -> CanonicalObject:
        return self._section(1)

    @property
    def projection(self) -> CanonicalObject:
        return self._section(2)

    @property
    def sessions(self) -> CanonicalObject:
        return self._section(3)

    @property
    def incidents(self) -> CanonicalObject:
        return self._section(4)

    @property
    def cursors(self) -> CanonicalObject:
        return self._section(5)

    @property
    def stream_heads(self) -> CanonicalObject:
        return self._section(6)

    @property
    def canonical_sections(self) -> tuple[bytes, ...]:
        return self._canonical_sections


def checkpoint_state_sha256(state: CheckpointState) -> Hash32:
    """Hash one complete checkpoint state for an independent persisted witness."""

    if type(state) is not CheckpointState:
        raise TypeError("checkpoint state witness requires CheckpointState")
    digest = hashlib.sha256(CHECKPOINT_STATE_WITNESS_DOMAIN)
    for section in state.canonical_sections:
        digest.update(len(section).to_bytes(8, byteorder="big", signed=False))
        digest.update(section)
    return Hash32(digest.digest())


@dataclass(frozen=True, slots=True)
class CheckpointStateWitness:
    covered_commit_sequence: CommitSequence
    state_sha256: Hash32

    def __post_init__(self) -> None:
        if type(self.covered_commit_sequence) is not CommitSequence:
            raise TypeError("checkpoint witness sequence must be CommitSequence")
        if type(self.state_sha256) is not Hash32:
            raise TypeError("checkpoint witness state digest must be Hash32")


@dataclass(frozen=True, slots=True)
class Checkpoint:
    store_id: StoreId
    run_id: RunId
    mode: str
    target_manifest_generation: int
    parent_manifest_root: Hash32 | None
    start_prefix_root: Hash32
    covered_commit_sequence: CommitSequence
    covered_prefix_root: Hash32
    covered_segment_identity: SegmentIdentity
    candidate_segment_descriptors_digest: Hash32
    run_identity: OpaqueIdentity
    config_identity: OpaqueIdentity
    code_identity: OpaqueIdentity
    runtime_identity: OpaqueIdentity
    historical_commit_count: int
    cumulative_stream_counts: tuple[CumulativeStreamCount, ...]
    state: CheckpointState

    def __post_init__(self) -> None:
        if type(self.store_id) is not StoreId or type(self.run_id) is not RunId:
            raise TypeError("checkpoint store_id and run_id must be explicit identifiers")
        if type(self.mode) is not str:
            raise TypeError("checkpoint mode must be exact text")
        if type(self.target_manifest_generation) is not int:
            raise TypeError("checkpoint target manifest generation must be an exact integer")
        if self.parent_manifest_root is not None and type(self.parent_manifest_root) is not Hash32:
            raise TypeError("checkpoint parent manifest root must be Hash32 or None")
        if type(self.start_prefix_root) is not Hash32:
            raise TypeError("checkpoint start prefix root must be Hash32")
        if type(self.covered_commit_sequence) is not CommitSequence:
            raise TypeError("checkpoint covered sequence must be CommitSequence")
        if type(self.covered_prefix_root) is not Hash32:
            raise TypeError("checkpoint covered prefix root must be Hash32")
        if type(self.covered_segment_identity) is not SegmentIdentity:
            raise TypeError("checkpoint covered segment identity must be SegmentIdentity")
        if type(self.candidate_segment_descriptors_digest) is not Hash32:
            raise TypeError("checkpoint candidate descriptor digest must be Hash32")
        for identity in (
            self.run_identity,
            self.config_identity,
            self.code_identity,
            self.runtime_identity,
        ):
            if type(identity) is not OpaqueIdentity:
                raise TypeError("checkpoint material identities must be OpaqueIdentity")
        if type(self.historical_commit_count) is not int:
            raise TypeError("checkpoint historical commit count must be an exact integer")
        if type(self.cumulative_stream_counts) is not tuple:
            raise TypeError("checkpoint cumulative stream counts must be a tuple")
        if type(self.state) is not CheckpointState:
            raise TypeError("checkpoint state must be CheckpointState")
        _verify_checkpoint_structure(self)

    @property
    def root(self) -> Hash32:
        return _checkpoint_root(self)

    @property
    def identity(self) -> Hash32:
        return self.root

    @property
    def candidate_segments_digest(self) -> Hash32:
        """Concise compatibility view of the bound descriptor-set digest."""

        return self.candidate_segment_descriptors_digest


@dataclass(slots=True)
class _Cursor:
    data: bytes
    limits: CheckpointReadLimits
    offset: int = 0

    def take(self, size: int, *, label: str) -> bytes:
        if type(size) is not int or size < 0:
            raise CheckpointFormatError(f"invalid {label} size")
        end = self.offset + size
        if end < self.offset or end > len(self.data):
            raise CheckpointFormatError(f"truncated {label}")
        value = self.data[self.offset:end]
        self.offset = end
        return value

    def u16(self, *, label: str) -> int:
        return int.from_bytes(self.take(2, label=label), "big", signed=False)

    def u32(self, *, label: str) -> int:
        return int.from_bytes(self.take(4, label=label), "big", signed=False)

    def u64(self, *, label: str) -> int:
        return int.from_bytes(self.take(8, label=label), "big", signed=False)

    def text(self, *, label: str) -> str:
        size = self.u32(label=f"{label} length")
        if size > self.limits.max_text_size:
            raise CheckpointFormatError(f"{label} exceeds text decode limit")
        try:
            return self.take(size, label=label).decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise CheckpointFormatError(f"{label} is not strict UTF-8") from error

    def hash32(self, *, label: str) -> Hash32:
        return Hash32(self.take(32, label=label))

    def optional_hash32(self, *, label: str) -> Hash32 | None:
        tag = self.take(1, label=f"{label} tag")
        if tag == b"\x00":
            return None
        if tag == b"\x01":
            return self.hash32(label=label)
        raise CheckpointFormatError(f"invalid {label} optional tag")

    def state_section(self, *, label: str) -> bytes:
        size = self.u32(label=f"{label} length")
        if size > self.limits.max_state_section_size:
            raise CheckpointFormatError(f"{label} exceeds state decode limit")
        return self.take(size, label=label)

    def require_end(self, *, label: str) -> None:
        if self.offset != len(self.data):
            raise CheckpointFormatError(f"unexpected trailing bytes in {label}")


def _frame_u16(value: int) -> bytes:
    if type(value) is not int or value < 0 or value > 0xFFFF:
        raise ValueError("uint16 framing value is invalid")
    return value.to_bytes(2, "big", signed=False)


def _validate_text(value: str, *, label: str) -> None:
    if type(value) is not str or not value:
        raise CheckpointFormatError(f"checkpoint {label} must be nonempty text")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise CheckpointFormatError(f"checkpoint {label} must be strict UTF-8") from error
    if len(encoded) > UINT32_MAX:
        raise CheckpointFormatError(f"checkpoint {label} exceeds uint32 framing")


def _validate_u64(value: int, *, label: str, minimum: int = 0) -> None:
    if type(value) is not int:
        raise CheckpointFormatError(f"checkpoint {label} must be an exact integer")
    if value < minimum or value > UINT64_MAX:
        raise CheckpointFormatError(f"checkpoint {label} is outside uint64 bounds")


def _verify_checkpoint_structure(checkpoint: Checkpoint) -> None:
    _validate_text(checkpoint.mode, label="mode")
    _validate_u64(
        checkpoint.target_manifest_generation,
        label="target manifest generation",
        minimum=1,
    )
    if checkpoint.target_manifest_generation == 1:
        if checkpoint.parent_manifest_root is not None:
            raise CheckpointFormatError(
                "checkpoint genesis target must not bind a parent manifest"
            )
    elif checkpoint.parent_manifest_root is None:
        raise CheckpointFormatError(
            "checkpoint non-genesis target requires a parent manifest root"
        )
    _validate_u64(
        checkpoint.historical_commit_count,
        label="historical commit count",
        minimum=1,
    )

    prior: bytes | None = None
    for item in checkpoint.cumulative_stream_counts:
        if type(item) is not tuple or len(item) != 2:
            raise TypeError("checkpoint cumulative stream counts must be exact pairs")
        stream_id, count = item
        if type(stream_id) is not StreamId:
            raise TypeError("checkpoint cumulative stream count key must be StreamId")
        _validate_u64(count, label="cumulative stream count", minimum=1)
        key = stream_id.value.encode("utf-8")
        if prior is not None and key <= prior:
            raise CheckpointFormatError(
                "checkpoint cumulative stream counts must be unique and UTF-8 sorted"
            )
        prior = key


def _checkpoint_body(checkpoint: Checkpoint) -> bytes:
    _verify_checkpoint_structure(checkpoint)
    values = [
        _frame_u16(PROTOCOL_VERSION),
        frame_text(checkpoint.store_id.value),
        frame_text(checkpoint.run_id.value),
        frame_text(checkpoint.mode),
        frame_u64(checkpoint.target_manifest_generation),
        frame_optional_hash32(checkpoint.parent_manifest_root),
        frame_hash32(checkpoint.start_prefix_root),
        frame_u64(int(checkpoint.covered_commit_sequence)),
        frame_hash32(checkpoint.covered_prefix_root),
        frame_hash32(checkpoint.covered_segment_identity.digest),
        frame_hash32(checkpoint.candidate_segment_descriptors_digest),
        frame_hash32(checkpoint.run_identity.digest),
        frame_hash32(checkpoint.config_identity.digest),
        frame_hash32(checkpoint.code_identity.digest),
        frame_hash32(checkpoint.runtime_identity.digest),
        frame_u64(checkpoint.historical_commit_count),
        frame_u32(len(checkpoint.cumulative_stream_counts)),
    ]
    for stream_id, count in checkpoint.cumulative_stream_counts:
        values.extend((frame_text(stream_id.value), frame_u64(count)))
    values.extend(frame_bytes(section) for section in checkpoint.state.canonical_sections)
    return b"".join(values)


def _checkpoint_root(checkpoint: Checkpoint) -> Hash32:
    return framed_hash(DOMAIN_CHECKPOINT, frame_bytes(_checkpoint_body(checkpoint)))


def build_checkpoint(
    *,
    store_id: StoreId,
    run_id: RunId,
    mode: str,
    target_manifest_generation: int,
    parent_manifest_root: Hash32 | None,
    start_prefix_root: Hash32,
    covered_commit_sequence: CommitSequence,
    covered_prefix_root: Hash32,
    covered_segment_identity: SegmentIdentity,
    candidate_segment_descriptors_digest: Hash32,
    run_identity: OpaqueIdentity,
    config_identity: OpaqueIdentity,
    code_identity: OpaqueIdentity,
    runtime_identity: OpaqueIdentity,
    historical_commit_count: int,
    cumulative_stream_counts: tuple[CumulativeStreamCount, ...],
    state: CheckpointState,
) -> Checkpoint:
    """Construct a complete immutable checkpoint and validate all local invariants."""

    return Checkpoint(
        store_id=store_id,
        run_id=run_id,
        mode=mode,
        target_manifest_generation=target_manifest_generation,
        parent_manifest_root=parent_manifest_root,
        start_prefix_root=start_prefix_root,
        covered_commit_sequence=covered_commit_sequence,
        covered_prefix_root=covered_prefix_root,
        covered_segment_identity=covered_segment_identity,
        candidate_segment_descriptors_digest=candidate_segment_descriptors_digest,
        run_identity=run_identity,
        config_identity=config_identity,
        code_identity=code_identity,
        runtime_identity=runtime_identity,
        historical_commit_count=historical_commit_count,
        cumulative_stream_counts=cumulative_stream_counts,
        state=state,
    )


def checkpoint_to_bytes(checkpoint: Checkpoint) -> bytes:
    if type(checkpoint) is not Checkpoint:
        raise TypeError("checkpoint serializer requires Checkpoint")
    body = _checkpoint_body(checkpoint)
    return b"".join(
        (
            CHECKPOINT_MAGIC,
            _frame_u16(CHECKPOINT_FORMAT_VERSION),
            frame_u64(len(body)),
            body,
            frame_hash32(_checkpoint_root(checkpoint)),
        )
    )


def _checkpoint_from_body(
    body: bytes,
    *,
    limits: CheckpointReadLimits,
) -> Checkpoint:
    cursor = _Cursor(body, limits)
    if cursor.u16(label="checkpoint protocol version") != PROTOCOL_VERSION:
        raise CheckpointFormatError("unsupported checkpoint logical protocol version")
    try:
        store_id = StoreId(cursor.text(label="checkpoint store ID"))
        run_id = RunId(cursor.text(label="checkpoint run ID"))
        mode = cursor.text(label="checkpoint mode")
        target_generation = cursor.u64(label="checkpoint target manifest generation")
        parent_root = cursor.optional_hash32(label="checkpoint parent manifest root")
        start_prefix_root = cursor.hash32(label="checkpoint start prefix root")
        covered_sequence = CommitSequence(cursor.u64(label="checkpoint covered sequence"))
        covered_prefix_root = cursor.hash32(label="checkpoint covered prefix root")
        covered_segment = SegmentIdentity(
            cursor.hash32(label="checkpoint covered segment identity")
        )
        descriptor_digest = cursor.hash32(
            label="checkpoint candidate descriptor digest"
        )
        run_identity = OpaqueIdentity(cursor.hash32(label="checkpoint run identity"))
        config_identity = OpaqueIdentity(cursor.hash32(label="checkpoint config identity"))
        code_identity = OpaqueIdentity(cursor.hash32(label="checkpoint code identity"))
        runtime_identity = OpaqueIdentity(cursor.hash32(label="checkpoint runtime identity"))
        historical_count = cursor.u64(label="checkpoint historical commit count")
        stream_count = cursor.u32(label="checkpoint cumulative stream entry count")
        if stream_count > limits.max_streams:
            raise CheckpointFormatError(
                "checkpoint cumulative stream count exceeds decode limit"
            )
        cumulative: list[CumulativeStreamCount] = []
        for index in range(stream_count):
            cumulative.append(
                (
                    StreamId(cursor.text(label=f"checkpoint stream {index} ID")),
                    cursor.u64(label=f"checkpoint stream {index} count"),
                )
            )
        sections = tuple(
            cursor.state_section(label=f"checkpoint {label} state")
            for label in CHECKPOINT_STATE_SECTIONS
        )
        cursor.require_end(label="checkpoint logical body")
        state = CheckpointState._from_canonical_sections(sections)
        return Checkpoint(
            store_id=store_id,
            run_id=run_id,
            mode=mode,
            target_manifest_generation=target_generation,
            parent_manifest_root=parent_root,
            start_prefix_root=start_prefix_root,
            covered_commit_sequence=covered_sequence,
            covered_prefix_root=covered_prefix_root,
            covered_segment_identity=covered_segment,
            candidate_segment_descriptors_digest=descriptor_digest,
            run_identity=run_identity,
            config_identity=config_identity,
            code_identity=code_identity,
            runtime_identity=runtime_identity,
            historical_commit_count=historical_count,
            cumulative_stream_counts=tuple(cumulative),
            state=state,
        )
    except CheckpointFormatError:
        raise
    except (TypeError, ValueError) as error:
        raise CheckpointFormatError("invalid checkpoint logical body") from error


def checkpoint_from_bytes(
    data: bytes,
    *,
    limits: CheckpointReadLimits | None = None,
) -> Checkpoint:
    """Parse, authenticate, and canonically reserialize one complete checkpoint."""

    if type(data) is not bytes:
        raise TypeError("checkpoint reader requires exact bytes")
    selected_limits = CheckpointReadLimits() if limits is None else limits
    if type(selected_limits) is not CheckpointReadLimits:
        raise TypeError("checkpoint read limits must be CheckpointReadLimits")
    if len(data) > selected_limits.max_physical_size:
        raise CheckpointFormatError("checkpoint physical size exceeds decode limit")
    minimum_size = len(CHECKPOINT_MAGIC) + 2 + 8 + 32
    if len(data) < minimum_size:
        raise CheckpointFormatError("checkpoint is shorter than minimum framing")
    cursor = _Cursor(data, selected_limits)
    if cursor.take(len(CHECKPOINT_MAGIC), label="checkpoint magic") != CHECKPOINT_MAGIC:
        raise CheckpointFormatError("invalid checkpoint magic")
    if cursor.u16(label="checkpoint format version") != CHECKPOINT_FORMAT_VERSION:
        raise CheckpointFormatError("unsupported checkpoint format version")
    body_size = cursor.u64(label="checkpoint body size")
    if body_size > selected_limits.max_body_size:
        raise CheckpointFormatError("checkpoint body size exceeds decode limit")
    body = cursor.take(body_size, label="checkpoint body")
    stored_root = cursor.hash32(label="checkpoint root")
    cursor.require_end(label="checkpoint file")
    checkpoint = _checkpoint_from_body(body, limits=selected_limits)
    if checkpoint.root != stored_root:
        raise CheckpointFormatError("checkpoint root mismatch")
    if checkpoint_to_bytes(checkpoint) != data:
        raise CheckpointFormatError("checkpoint serialization is not canonical")
    return checkpoint


def verify_checkpoint(
    checkpoint: Checkpoint,
    *,
    expected_store_id: StoreId,
    expected_run_id: RunId,
    expected_mode: str,
    expected_target_manifest_generation: int,
    expected_parent_manifest_root: Hash32 | None,
    expected_start_prefix_root: Hash32,
    expected_covered_commit_sequence: CommitSequence,
    expected_covered_prefix_root: Hash32,
    expected_covered_segment_identity: SegmentIdentity,
    expected_candidate_segment_descriptors_digest: Hash32,
    expected_run_identity: OpaqueIdentity,
    expected_config_identity: OpaqueIdentity,
    expected_code_identity: OpaqueIdentity,
    expected_runtime_identity: OpaqueIdentity,
) -> None:
    """Verify every recovery binding against explicit caller-owned expectations."""

    if type(checkpoint) is not Checkpoint:
        raise TypeError("checkpoint verifier requires Checkpoint")
    _verify_checkpoint_structure(checkpoint)
    if type(expected_store_id) is not StoreId or type(expected_run_id) is not RunId:
        raise TypeError("expected checkpoint store and run must be explicit identifiers")
    _validate_text(expected_mode, label="expected mode")
    _validate_u64(
        expected_target_manifest_generation,
        label="expected target manifest generation",
        minimum=1,
    )
    if expected_parent_manifest_root is not None and type(expected_parent_manifest_root) is not Hash32:
        raise TypeError("expected checkpoint parent root must be Hash32 or None")
    for label, value, expected_type in (
        ("start prefix root", expected_start_prefix_root, Hash32),
        ("covered sequence", expected_covered_commit_sequence, CommitSequence),
        ("covered prefix root", expected_covered_prefix_root, Hash32),
        ("covered segment identity", expected_covered_segment_identity, SegmentIdentity),
        (
            "candidate descriptor digest",
            expected_candidate_segment_descriptors_digest,
            Hash32,
        ),
        ("run identity", expected_run_identity, OpaqueIdentity),
        ("config identity", expected_config_identity, OpaqueIdentity),
        ("code identity", expected_code_identity, OpaqueIdentity),
        ("runtime identity", expected_runtime_identity, OpaqueIdentity),
    ):
        if type(value) is not expected_type:
            raise TypeError(f"expected checkpoint {label} has the wrong type")

    if checkpoint.store_id != expected_store_id:
        raise CheckpointFormatError("checkpoint belongs to the wrong store")
    if checkpoint.run_id != expected_run_id:
        raise CheckpointFormatError("checkpoint belongs to the wrong run")
    if checkpoint.mode != expected_mode:
        raise CheckpointFormatError("checkpoint belongs to the wrong storage mode")
    if checkpoint.target_manifest_generation > expected_target_manifest_generation:
        raise CheckpointFormatError("checkpoint is future relative to the target manifest")
    if checkpoint.target_manifest_generation < expected_target_manifest_generation:
        raise CheckpointFormatError("checkpoint is stale relative to the target manifest")
    if checkpoint.parent_manifest_root != expected_parent_manifest_root:
        raise CheckpointFormatError("checkpoint parent manifest binding differs")
    if checkpoint.start_prefix_root != expected_start_prefix_root:
        raise CheckpointFormatError("checkpoint starting prefix material differs")
    if checkpoint.covered_commit_sequence != expected_covered_commit_sequence:
        raise CheckpointFormatError("checkpoint covered sequence material differs")
    if checkpoint.covered_prefix_root != expected_covered_prefix_root:
        raise CheckpointFormatError("checkpoint covered prefix material differs")
    if checkpoint.covered_segment_identity != expected_covered_segment_identity:
        raise CheckpointFormatError("checkpoint covered segment material differs")
    if (
        checkpoint.candidate_segment_descriptors_digest
        != expected_candidate_segment_descriptors_digest
    ):
        raise CheckpointFormatError("checkpoint candidate segment material differs")
    for label, actual, expected in (
        ("run", checkpoint.run_identity, expected_run_identity),
        ("config", checkpoint.config_identity, expected_config_identity),
        ("code", checkpoint.code_identity, expected_code_identity),
        ("runtime", checkpoint.runtime_identity, expected_runtime_identity),
    ):
        if actual != expected:
            raise CheckpointFormatError(f"checkpoint {label} identity material differs")


__all__ = [
    "CHECKPOINT_FORMAT_VERSION",
    "CHECKPOINT_MAGIC",
    "CHECKPOINT_STATE_SECTIONS",
    "DOMAIN_CHECKPOINT",
    "Checkpoint",
    "CheckpointFormatError",
    "CheckpointReadLimits",
    "CheckpointState",
    "CheckpointStateWitness",
    "CumulativeStreamCount",
    "build_checkpoint",
    "checkpoint_from_bytes",
    "checkpoint_state_sha256",
    "checkpoint_to_bytes",
    "verify_checkpoint",
]
