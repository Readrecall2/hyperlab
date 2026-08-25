"""Deterministic chained manifests for immutable Storage v4 segments.

Phase 1A verifies internal structure and a caller-supplied parent root. It does
not provide a root-owned anchor and therefore does not claim anti-rollback.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .canonical import (
    DOMAIN_MANIFEST,
    PROTOCOL_VERSION,
    frame_bytes,
    frame_hash32,
    frame_optional_hash32,
    frame_text,
    frame_u32,
    frame_u64,
    framed_hash,
)
from .segment import (
    SegmentArtifact,
    SegmentFormatError,
    SegmentReadLimits,
    read_segment,
)
from .types import (
    UINT32_MAX,
    UINT64_MAX,
    CommitSequence,
    Hash32,
    LocalCount,
    ManifestIdentity,
    RunId,
    SegmentIdentity,
    StoreId,
    StreamCount,
    StreamId,
)

MANIFEST_MAGIC = b"HL4MAN\x00\x01"
MANIFEST_FORMAT_VERSION = 1


class ManifestFormatError(ValueError):
    """A manifest is malformed, non-canonical, or violates chain invariants."""


@dataclass(frozen=True, slots=True)
class ManifestReadLimits:
    """Fail-closed allocation limits for untrusted Phase 1A manifest bytes."""

    max_physical_size: int = 64 * 1024 * 1024
    max_body_size: int = 64 * 1024 * 1024
    max_descriptor_size: int = 1024 * 1024
    max_segments: int = 65_536

    def __post_init__(self) -> None:
        for label, value, maximum in (
            ("max_physical_size", self.max_physical_size, UINT64_MAX),
            ("max_body_size", self.max_body_size, UINT64_MAX),
            ("max_descriptor_size", self.max_descriptor_size, UINT32_MAX),
            ("max_segments", self.max_segments, UINT32_MAX),
        ):
            if type(value) is not int:
                raise TypeError(f"{label} must be an exact integer")
            if value < 1 or value > maximum:
                raise ValueError(f"{label} must be positive and within its framing width")


@dataclass(frozen=True, slots=True)
class OpaqueIdentity:
    """An explicitly typed but semantically opaque 32-byte attested identity."""

    digest: Hash32

    def __post_init__(self) -> None:
        if type(self.digest) is not Hash32:
            raise TypeError("opaque identity must contain Hash32")


@dataclass(frozen=True, slots=True)
class ManifestHead:
    commit_sequence: CommitSequence
    prefix_root: Hash32
    segment_identity: SegmentIdentity

    def __post_init__(self) -> None:
        if type(self.commit_sequence) is not CommitSequence:
            raise TypeError("manifest head commit_sequence must be CommitSequence")
        if type(self.prefix_root) is not Hash32:
            raise TypeError("manifest head prefix_root must be Hash32")
        if type(self.segment_identity) is not SegmentIdentity:
            raise TypeError("manifest head segment_identity must be SegmentIdentity")


@dataclass(frozen=True, slots=True)
class SegmentDescriptor:
    identity: SegmentIdentity
    run_id: RunId
    first_commit_sequence: CommitSequence
    last_commit_sequence: CommitSequence
    previous_prefix_root: Hash32
    end_prefix_root: Hash32
    merkle_root: Hash32
    physical_sha256: Hash32
    physical_size: int
    logical_size: int
    commit_count: LocalCount
    counts_by_stream: tuple[StreamCount, ...]
    codec_profile: str
    checkpoint_root: Hash32 | None = None

    def __post_init__(self) -> None:
        if type(self.identity) is not SegmentIdentity:
            raise TypeError("segment descriptor identity must be SegmentIdentity")
        if type(self.run_id) is not RunId:
            raise TypeError("segment descriptor run_id must be RunId")
        if (
            type(self.first_commit_sequence) is not CommitSequence
            or type(self.last_commit_sequence) is not CommitSequence
        ):
            raise TypeError("segment descriptor range must use CommitSequence")
        for label, value in (
            ("previous prefix root", self.previous_prefix_root),
            ("end prefix root", self.end_prefix_root),
            ("Merkle root", self.merkle_root),
            ("physical SHA-256", self.physical_sha256),
        ):
            if type(value) is not Hash32:
                raise TypeError(f"segment descriptor {label} must be Hash32")
        if type(self.physical_size) is not int or type(self.logical_size) is not int:
            raise TypeError("segment descriptor sizes must be exact integers")
        if type(self.commit_count) is not LocalCount:
            raise TypeError("segment descriptor commit_count must be LocalCount")
        if type(self.counts_by_stream) is not tuple:
            raise TypeError("segment descriptor counts must be a tuple")
        prior: bytes | None = None
        for item in self.counts_by_stream:
            if type(item) is not tuple or len(item) != 2:
                raise TypeError("segment descriptor counts must be exact pairs")
            stream_id, count = item
            if type(stream_id) is not StreamId or type(count) is not LocalCount:
                raise TypeError("segment descriptor counts require StreamId and LocalCount")
            if int(count) == 0:
                raise ValueError("segment descriptor counts cannot contain zero")
            key = stream_id.value.encode("utf-8")
            if prior is not None and key <= prior:
                raise ValueError("segment descriptor counts must be unique and UTF-8 sorted")
            prior = key
        if type(self.codec_profile) is not str or not self.codec_profile:
            raise TypeError("segment descriptor codec profile must be nonempty text")
        try:
            self.codec_profile.encode("utf-8", errors="strict")
        except UnicodeEncodeError as error:
            raise ValueError("segment descriptor codec profile must be strict UTF-8") from error
        if self.checkpoint_root is not None and type(self.checkpoint_root) is not Hash32:
            raise TypeError("segment descriptor checkpoint_root must be Hash32 or None")

    @classmethod
    def from_segment(
        cls,
        segment: SegmentArtifact,
        *,
        checkpoint_root: Hash32 | None = None,
    ) -> SegmentDescriptor:
        if type(segment) is not SegmentArtifact:
            raise TypeError("from_segment requires SegmentArtifact")
        return cls(
            identity=segment.identity,
            run_id=segment.run_id,
            first_commit_sequence=segment.first_commit_sequence,
            last_commit_sequence=segment.last_commit_sequence,
            previous_prefix_root=segment.previous_prefix_root,
            end_prefix_root=segment.end_prefix_root,
            merkle_root=segment.merkle_root,
            physical_sha256=segment.physical_sha256,
            physical_size=segment.physical_size,
            logical_size=segment.logical_size,
            commit_count=LocalCount(segment.commit_count),
            counts_by_stream=segment.counts_by_stream,
            codec_profile=segment.codec_profile.profile_id,
            checkpoint_root=checkpoint_root,
        )


@dataclass(frozen=True, slots=True)
class Manifest:
    store_id: StoreId
    run_id: RunId
    generation: int
    parent_manifest_root: Hash32 | None
    run_identity: OpaqueIdentity
    config_identity: OpaqueIdentity
    code_identity: OpaqueIdentity
    runtime_identity: OpaqueIdentity
    start_prefix_root: Hash32
    segments: tuple[SegmentDescriptor, ...]
    head: ManifestHead

    def __post_init__(self) -> None:
        if type(self.store_id) is not StoreId or type(self.run_id) is not RunId:
            raise TypeError("manifest store_id and run_id must be explicit identifiers")
        if type(self.generation) is not int:
            raise TypeError("manifest generation must be an exact integer")
        if self.parent_manifest_root is not None and type(self.parent_manifest_root) is not Hash32:
            raise TypeError("manifest parent root must be Hash32 or None")
        for value in (
            self.run_identity,
            self.config_identity,
            self.code_identity,
            self.runtime_identity,
        ):
            if type(value) is not OpaqueIdentity:
                raise TypeError("manifest attested identities must be OpaqueIdentity")
        if type(self.start_prefix_root) is not Hash32:
            raise TypeError("manifest start prefix root must be Hash32")
        if type(self.segments) is not tuple or any(
            type(segment) is not SegmentDescriptor for segment in self.segments
        ):
            raise TypeError("manifest segments must be a tuple of SegmentDescriptor")
        if type(self.head) is not ManifestHead:
            raise TypeError("manifest head must be ManifestHead")

    @property
    def identity(self) -> ManifestIdentity:
        return ManifestIdentity(_manifest_root(self))


@dataclass(slots=True)
class _Cursor:
    data: bytes
    offset: int = 0

    @property
    def remaining(self) -> int:
        return len(self.data) - self.offset

    def take(self, size: int, *, label: str) -> bytes:
        if type(size) is not int or size < 0:
            raise ManifestFormatError(f"invalid {label} size")
        end = self.offset + size
        if end > len(self.data):
            raise ManifestFormatError(f"truncated {label}")
        value = self.data[self.offset : end]
        self.offset = end
        return value

    def u8(self, *, label: str) -> int:
        return self.take(1, label=label)[0]

    def u16(self, *, label: str) -> int:
        return int.from_bytes(self.take(2, label=label), "big", signed=False)

    def u32(self, *, label: str) -> int:
        return int.from_bytes(self.take(4, label=label), "big", signed=False)

    def u64(self, *, label: str) -> int:
        return int.from_bytes(self.take(8, label=label), "big", signed=False)

    def hash32(self, *, label: str) -> Hash32:
        return Hash32(self.take(32, label=label))

    def optional_hash32(self, *, label: str) -> Hash32 | None:
        tag = self.u8(label=f"{label} tag")
        if tag == 0:
            return None
        if tag == 1:
            return self.hash32(label=label)
        raise ManifestFormatError(f"{label} has invalid optional tag {tag}")

    def bytes_u32(self, *, label: str) -> bytes:
        return self.take(self.u32(label=f"{label} length"), label=label)

    def text(self, *, label: str) -> str:
        encoded = self.bytes_u32(label=label)
        try:
            value = encoded.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise ManifestFormatError(f"{label} is not strict UTF-8") from error
        if not value:
            raise ManifestFormatError(f"{label} cannot be empty")
        return value

    def require_end(self, *, label: str) -> None:
        if self.remaining:
            raise ManifestFormatError(f"{label} has {self.remaining} trailing bytes")


def _frame_u16(value: int) -> bytes:
    if type(value) is not int or value < 0 or value > 0xFFFF:
        raise ValueError("uint16 framing value is invalid")
    return value.to_bytes(2, "big", signed=False)


def _encode_counts(counts: tuple[StreamCount, ...]) -> bytes:
    values = [frame_u32(len(counts))]
    for stream_id, count in counts:
        values.extend((frame_text(stream_id.value), frame_u32(int(count))))
    return b"".join(values)


def _decode_counts(cursor: _Cursor, *, label: str) -> tuple[StreamCount, ...]:
    values: list[StreamCount] = []
    prior: bytes | None = None
    for _ in range(cursor.u32(label=f"{label} entry count")):
        stream_id = StreamId(cursor.text(label=f"{label} stream ID"))
        key = stream_id.value.encode("utf-8")
        if prior is not None and key <= prior:
            raise ManifestFormatError(f"{label} must be unique and UTF-8 sorted")
        count = LocalCount(cursor.u32(label=f"{label} count"))
        if int(count) == 0:
            raise ManifestFormatError(f"{label} cannot contain zero")
        prior = key
        values.append((stream_id, count))
    return tuple(values)


def _encode_descriptor(descriptor: SegmentDescriptor) -> bytes:
    return b"".join(
        (
            frame_hash32(descriptor.identity.digest),
            frame_text(descriptor.run_id.value),
            frame_u64(int(descriptor.first_commit_sequence)),
            frame_u64(int(descriptor.last_commit_sequence)),
            frame_hash32(descriptor.previous_prefix_root),
            frame_hash32(descriptor.end_prefix_root),
            frame_hash32(descriptor.merkle_root),
            frame_hash32(descriptor.physical_sha256),
            frame_u64(descriptor.physical_size),
            frame_u64(descriptor.logical_size),
            frame_u32(int(descriptor.commit_count)),
            _encode_counts(descriptor.counts_by_stream),
            frame_text(descriptor.codec_profile),
            frame_optional_hash32(descriptor.checkpoint_root),
        )
    )


def _decode_descriptor(value: bytes) -> SegmentDescriptor:
    cursor = _Cursor(value)
    try:
        descriptor = SegmentDescriptor(
            identity=SegmentIdentity(cursor.hash32(label="descriptor segment identity")),
            run_id=RunId(cursor.text(label="descriptor run ID")),
            first_commit_sequence=CommitSequence(
                cursor.u64(label="descriptor first commit sequence")
            ),
            last_commit_sequence=CommitSequence(
                cursor.u64(label="descriptor last commit sequence")
            ),
            previous_prefix_root=cursor.hash32(label="descriptor previous prefix root"),
            end_prefix_root=cursor.hash32(label="descriptor end prefix root"),
            merkle_root=cursor.hash32(label="descriptor Merkle root"),
            physical_sha256=cursor.hash32(label="descriptor physical SHA-256"),
            physical_size=cursor.u64(label="descriptor physical size"),
            logical_size=cursor.u64(label="descriptor logical size"),
            commit_count=LocalCount(cursor.u32(label="descriptor commit count")),
            counts_by_stream=_decode_counts(cursor, label="descriptor stream counts"),
            codec_profile=cursor.text(label="descriptor codec profile"),
            checkpoint_root=cursor.optional_hash32(label="descriptor checkpoint root"),
        )
    except (TypeError, ValueError) as error:
        raise ManifestFormatError("invalid segment descriptor") from error
    cursor.require_end(label="segment descriptor")
    return descriptor


def _manifest_body(manifest: Manifest) -> bytes:
    fields = [
        _frame_u16(PROTOCOL_VERSION),
        frame_text(manifest.store_id.value),
        frame_text(manifest.run_id.value),
        frame_u64(manifest.generation),
        frame_optional_hash32(manifest.parent_manifest_root),
        frame_hash32(manifest.run_identity.digest),
        frame_hash32(manifest.config_identity.digest),
        frame_hash32(manifest.code_identity.digest),
        frame_hash32(manifest.runtime_identity.digest),
        frame_hash32(manifest.start_prefix_root),
        frame_u32(len(manifest.segments)),
    ]
    fields.extend(frame_bytes(_encode_descriptor(segment)) for segment in manifest.segments)
    fields.extend(
        (
            frame_u64(int(manifest.head.commit_sequence)),
            frame_hash32(manifest.head.prefix_root),
            frame_hash32(manifest.head.segment_identity.digest),
        )
    )
    return b"".join(fields)


def _manifest_root(manifest: Manifest) -> Hash32:
    return framed_hash(DOMAIN_MANIFEST, frame_bytes(_manifest_body(manifest)))


def _validate_u64(value: int, *, label: str, minimum: int = 0) -> None:
    if type(value) is not int:
        raise ManifestFormatError(f"{label} must be an exact integer")
    if value < minimum or value > UINT64_MAX:
        raise ManifestFormatError(f"{label} is outside uint64 bounds")


def _validate_descriptor(descriptor: SegmentDescriptor) -> None:
    _validate_u64(descriptor.physical_size, label="segment physical size", minimum=1)
    _validate_u64(descriptor.logical_size, label="segment logical size", minimum=1)
    first = int(descriptor.first_commit_sequence)
    last = int(descriptor.last_commit_sequence)
    if last < first:
        raise ManifestFormatError("segment range is reversed")
    expected_count = last - first + 1
    if expected_count > UINT32_MAX or int(descriptor.commit_count) != expected_count:
        raise ManifestFormatError("segment range and commit count are incompatible")


def verify_manifest(
    manifest: Manifest,
    *,
    expected_parent_root: Hash32 | None = None,
    expected_generation: int | None = None,
) -> None:
    """Fail closed on every local range, prefix, parent, and final-head invariant.

    A caller can pin the expected parent for one chain step. No external anchor
    or rollback witness exists in Phase 1A.
    """

    if type(manifest) is not Manifest:
        raise TypeError("verify_manifest requires Manifest")
    _validate_u64(manifest.generation, label="manifest generation", minimum=1)
    if manifest.generation == 1:
        if manifest.parent_manifest_root is not None:
            raise ManifestFormatError("manifest generation 1 must not have a parent")
    elif manifest.parent_manifest_root is None:
        raise ManifestFormatError("manifest generation after 1 requires a parent")

    if expected_generation is not None:
        _validate_u64(expected_generation, label="expected manifest generation", minimum=1)
        if manifest.generation != expected_generation:
            raise ManifestFormatError("manifest generation differs from expected generation")
    if expected_parent_root is not None:
        if type(expected_parent_root) is not Hash32:
            raise TypeError("expected parent root must be Hash32")
        if manifest.parent_manifest_root != expected_parent_root:
            raise ManifestFormatError("manifest parent root differs from expected parent")

    if not manifest.segments:
        raise ManifestFormatError("manifest must describe at least one segment")
    seen_identities: set[SegmentIdentity] = set()
    seen_physical_hashes: set[Hash32] = set()
    prior: SegmentDescriptor | None = None
    for descriptor in manifest.segments:
        _validate_descriptor(descriptor)
        if descriptor.run_id != manifest.run_id:
            raise ManifestFormatError("manifest segment run ID differs from manifest run")
        if descriptor.identity in seen_identities:
            raise ManifestFormatError("manifest contains a duplicated segment identity")
        if descriptor.physical_sha256 in seen_physical_hashes:
            raise ManifestFormatError("manifest contains a duplicated physical segment")
        seen_identities.add(descriptor.identity)
        seen_physical_hashes.add(descriptor.physical_sha256)

        if prior is None:
            if descriptor.previous_prefix_root != manifest.start_prefix_root:
                raise ManifestFormatError("manifest first segment prefix is incompatible")
        else:
            if int(prior.last_commit_sequence) == UINT64_MAX:
                raise ManifestFormatError("manifest cannot append after uint64 maximum")
            expected_first = int(prior.last_commit_sequence) + 1
            actual_first = int(descriptor.first_commit_sequence)
            if actual_first != expected_first:
                relation = "overlap or reorder" if actual_first < expected_first else "gap"
                raise ManifestFormatError(f"manifest segment range has {relation}")
            if descriptor.previous_prefix_root != prior.end_prefix_root:
                raise ManifestFormatError("manifest segment prefix chain is incompatible")
        prior = descriptor

    last = manifest.segments[-1]
    expected_head = ManifestHead(
        commit_sequence=last.last_commit_sequence,
        prefix_root=last.end_prefix_root,
        segment_identity=last.identity,
    )
    if manifest.head != expected_head:
        raise ManifestFormatError("manifest final head is incompatible with last segment")


def verify_manifest_transition(parent: Manifest, child: Manifest) -> None:
    """Verify one append-only authenticated manifest-chain transition.

    verify_manifest closes the invariants inside one snapshot. A parent root
    alone does not prove that the child retained the parent's descriptors or
    identities, so repository publication must also call this transition
    verifier before advancing an external anchor.
    """

    if type(parent) is not Manifest or type(child) is not Manifest:
        raise TypeError("manifest transition requires exact Manifest values")
    verify_manifest(parent)
    verify_manifest(child)

    if parent.generation == UINT64_MAX:
        raise ManifestFormatError("manifest generation cannot advance past uint64 maximum")
    if child.generation != parent.generation + 1:
        raise ManifestFormatError("child manifest generation is not the next generation")
    if child.parent_manifest_root != parent.identity.root:
        raise ManifestFormatError("child manifest parent root differs from parent identity")

    if child.store_id != parent.store_id or child.run_id != parent.run_id:
        raise ManifestFormatError("child manifest store or run identity drifted")
    if (
        child.run_identity != parent.run_identity
        or child.config_identity != parent.config_identity
        or child.code_identity != parent.code_identity
        or child.runtime_identity != parent.runtime_identity
    ):
        raise ManifestFormatError("child manifest attested identities drifted")
    if child.start_prefix_root != parent.start_prefix_root:
        raise ManifestFormatError("child manifest start prefix root drifted")

    parent_count = len(parent.segments)
    if len(child.segments) <= parent_count:
        raise ManifestFormatError("child manifest must append at least one segment")
    if child.segments[:parent_count] != parent.segments:
        raise ManifestFormatError("child manifest replaced or mutated an existing descriptor")


def verify_manifest_segments(
    manifest: Manifest,
    segments: Sequence[SegmentArtifact],
    *,
    limits: SegmentReadLimits | None = None,
) -> None:
    """Cross-check every declaration against independently re-read segment bytes."""

    verify_manifest(manifest)
    supplied = tuple(segments)
    if len(supplied) != len(manifest.segments):
        raise ManifestFormatError("manifest segment artifact count is incomplete")
    for index, (descriptor, artifact) in enumerate(
        zip(manifest.segments, supplied, strict=True)
    ):
        if type(artifact) is not SegmentArtifact:
            raise TypeError("manifest segment artifacts must be SegmentArtifact")
        try:
            verified = read_segment(artifact.data, limits=limits)
        except SegmentFormatError as error:
            raise ManifestFormatError(
                f"manifest segment artifact {index} failed verification"
            ) from error
        expected = SegmentDescriptor.from_segment(
            verified,
            checkpoint_root=descriptor.checkpoint_root,
        )
        if descriptor != expected:
            raise ManifestFormatError(
                f"manifest descriptor {index} differs from verified segment bytes"
            )


def build_manifest(
    *,
    store_id: StoreId,
    run_id: RunId,
    generation: int,
    parent_manifest_root: Hash32 | None,
    run_identity: OpaqueIdentity,
    config_identity: OpaqueIdentity,
    code_identity: OpaqueIdentity,
    runtime_identity: OpaqueIdentity,
    start_prefix_root: Hash32,
    segments: Sequence[SegmentDescriptor],
) -> Manifest:
    descriptors = tuple(segments)
    if not descriptors:
        raise ManifestFormatError("manifest must describe at least one segment")
    last = descriptors[-1]
    manifest = Manifest(
        store_id=store_id,
        run_id=run_id,
        generation=generation,
        parent_manifest_root=parent_manifest_root,
        run_identity=run_identity,
        config_identity=config_identity,
        code_identity=code_identity,
        runtime_identity=runtime_identity,
        start_prefix_root=start_prefix_root,
        segments=descriptors,
        head=ManifestHead(
            commit_sequence=last.last_commit_sequence,
            prefix_root=last.end_prefix_root,
            segment_identity=last.identity,
        ),
    )
    verify_manifest(manifest)
    return manifest


def manifest_to_bytes(manifest: Manifest) -> bytes:
    """Serialize a verified manifest deterministically with its raw 32-byte root."""

    verify_manifest(manifest)
    body = _manifest_body(manifest)
    return b"".join(
        (
            MANIFEST_MAGIC,
            _frame_u16(MANIFEST_FORMAT_VERSION),
            frame_u64(len(body)),
            body,
            frame_hash32(_manifest_root(manifest)),
        )
    )


def _manifest_from_body(body: bytes, *, limits: ManifestReadLimits) -> Manifest:
    cursor = _Cursor(body)
    if cursor.u16(label="manifest protocol version") != PROTOCOL_VERSION:
        raise ManifestFormatError("unsupported manifest logical protocol version")
    try:
        store_id = StoreId(cursor.text(label="manifest store ID"))
        run_id = RunId(cursor.text(label="manifest run ID"))
        generation = cursor.u64(label="manifest generation")
        parent = cursor.optional_hash32(label="manifest parent root")
        run_identity = OpaqueIdentity(cursor.hash32(label="manifest run identity"))
        config_identity = OpaqueIdentity(cursor.hash32(label="manifest config identity"))
        code_identity = OpaqueIdentity(cursor.hash32(label="manifest code identity"))
        runtime_identity = OpaqueIdentity(cursor.hash32(label="manifest runtime identity"))
        start_prefix = cursor.hash32(label="manifest start prefix root")
        segment_count = cursor.u32(label="manifest segment count")
        if segment_count > limits.max_segments:
            raise ManifestFormatError("manifest segment count exceeds decode limit")
        descriptor_values: list[SegmentDescriptor] = []
        for index in range(segment_count):
            descriptor_size = cursor.u32(
                label=f"manifest segment {index} length"
            )
            if descriptor_size > limits.max_descriptor_size:
                raise ManifestFormatError("manifest descriptor size exceeds decode limit")
            descriptor_values.append(
                _decode_descriptor(
                    cursor.take(descriptor_size, label=f"manifest segment {index}")
                )
            )
        descriptors = tuple(descriptor_values)
        head = ManifestHead(
            commit_sequence=CommitSequence(cursor.u64(label="manifest head sequence")),
            prefix_root=cursor.hash32(label="manifest head prefix root"),
            segment_identity=SegmentIdentity(
                cursor.hash32(label="manifest head segment identity")
            ),
        )
        manifest = Manifest(
            store_id=store_id,
            run_id=run_id,
            generation=generation,
            parent_manifest_root=parent,
            run_identity=run_identity,
            config_identity=config_identity,
            code_identity=code_identity,
            runtime_identity=runtime_identity,
            start_prefix_root=start_prefix,
            segments=descriptors,
            head=head,
        )
    except ManifestFormatError:
        raise
    except (TypeError, ValueError) as error:
        raise ManifestFormatError("invalid manifest logical body") from error
    cursor.require_end(label="manifest logical body")
    return manifest


def manifest_from_bytes(
    data: bytes,
    *,
    limits: ManifestReadLimits | None = None,
) -> Manifest:
    """Parse, authenticate, and structurally verify one complete manifest."""

    if type(data) is not bytes:
        raise TypeError("manifest reader requires exact bytes")
    selected_limits = ManifestReadLimits() if limits is None else limits
    if type(selected_limits) is not ManifestReadLimits:
        raise TypeError("manifest read limits must be ManifestReadLimits")
    if len(data) > selected_limits.max_physical_size:
        raise ManifestFormatError("manifest physical size exceeds decode limit")
    minimum_size = len(MANIFEST_MAGIC) + 2 + 8 + 32
    if len(data) < minimum_size:
        raise ManifestFormatError("manifest is shorter than minimum framing")
    cursor = _Cursor(data)
    if cursor.take(len(MANIFEST_MAGIC), label="manifest magic") != MANIFEST_MAGIC:
        raise ManifestFormatError("invalid manifest magic")
    if cursor.u16(label="manifest format version") != MANIFEST_FORMAT_VERSION:
        raise ManifestFormatError("unsupported manifest format version")
    body_size = cursor.u64(label="manifest body size")
    if body_size > selected_limits.max_body_size:
        raise ManifestFormatError("manifest body size exceeds decode limit")
    body = cursor.take(body_size, label="manifest body")
    stored_root = cursor.hash32(label="manifest root")
    cursor.require_end(label="manifest file")
    manifest = _manifest_from_body(body, limits=selected_limits)
    verify_manifest(manifest)
    if _manifest_root(manifest) != stored_root:
        raise ManifestFormatError("manifest root mismatch")
    if manifest_to_bytes(manifest) != data:
        raise ManifestFormatError("manifest serialization is not canonical")
    return manifest


__all__ = [
    "MANIFEST_FORMAT_VERSION",
    "MANIFEST_MAGIC",
    "Manifest",
    "ManifestFormatError",
    "ManifestHead",
    "ManifestReadLimits",
    "OpaqueIdentity",
    "SegmentDescriptor",
    "build_manifest",
    "manifest_from_bytes",
    "manifest_to_bytes",
    "verify_manifest",
    "verify_manifest_segments",
    "verify_manifest_transition",
]
