"""Canonical chained manifests for immutable Storage v4 raw segments."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from .canonical import canonical_json_bytes, frame_bytes, framed_hash
from .contracts import RawLakeId
from .raw_segment import RawSegmentArtifact
from .segment import CodecProfile
from .types import UINT32_MAX, UINT64_MAX, Hash32, SegmentIdentity, StoreId

RAW_MANIFEST_FORMAT_VERSION = 1
RAW_MANIFEST_CONTRACT = "hyperlab.storage_v4.raw_manifest.v1"
RAW_MANIFEST_DOMAIN = b"HL4-RAW-MANIFEST"
RAW_MANIFEST_SUFFIX = ".hl4rm"


class RawManifestErrorCode(StrEnum):
    TYPE = "RAW_MANIFEST_TYPE_INVALID"
    FORMAT = "RAW_MANIFEST_FORMAT_INVALID"
    LIMIT = "RAW_MANIFEST_LIMIT_EXCEEDED"
    HASH = "RAW_MANIFEST_HASH_MISMATCH"
    TRANSITION = "RAW_MANIFEST_TRANSITION_INVALID"


class RawManifestError(ValueError):
    def __init__(self, code: RawManifestErrorCode, message: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {message}")


def _error(code: RawManifestErrorCode, message: str) -> RawManifestError:
    return RawManifestError(code, message)


@dataclass(frozen=True, slots=True)
class RawManifestReadLimits:
    max_physical_size: int = 64 * 1024 * 1024
    max_segments: int = 65_536

    def __post_init__(self) -> None:
        if type(self.max_physical_size) is not int or self.max_physical_size < 1:
            raise ValueError("raw manifest physical limit must be positive")
        if (
            type(self.max_segments) is not int
            or self.max_segments < 1
            or self.max_segments > UINT32_MAX
        ):
            raise ValueError("raw manifest segment limit must be positive and uint32")


@dataclass(frozen=True, slots=True)
class RawSegmentDescriptor:
    segment_identity: SegmentIdentity
    segment_root: Hash32
    physical_sha256: Hash32
    physical_size: int
    record_count: int
    logical_payload_bytes: int
    stored_payload_bytes: int
    first_arrival_sequence: int
    last_arrival_sequence: int
    first_record_id: str
    last_record_id: str
    codec_profile: CodecProfile

    def __post_init__(self) -> None:
        if type(self.segment_identity) is not SegmentIdentity:
            raise TypeError("raw descriptor segment identity must be SegmentIdentity")
        for root in (self.segment_root, self.physical_sha256):
            if type(root) is not Hash32:
                raise TypeError("raw descriptor roots must be Hash32")
        for label, number, maximum in (
            ("physical_size", self.physical_size, UINT64_MAX),
            ("record_count", self.record_count, UINT32_MAX),
            ("logical_payload_bytes", self.logical_payload_bytes, UINT64_MAX),
            ("stored_payload_bytes", self.stored_payload_bytes, UINT64_MAX),
            ("first_arrival_sequence", self.first_arrival_sequence, UINT64_MAX),
            ("last_arrival_sequence", self.last_arrival_sequence, UINT64_MAX),
        ):
            if type(number) is not int or number < 0 or number > maximum:
                raise ValueError(f"raw descriptor {label} is outside its framing width")
        if self.record_count < 1 or self.physical_size < 1:
            raise ValueError("raw descriptor cannot describe an empty segment")
        if self.first_arrival_sequence > self.last_arrival_sequence:
            raise ValueError("raw descriptor arrival range is reversed")
        for label, record_id in (
            ("first_record_id", self.first_record_id),
            ("last_record_id", self.last_record_id),
        ):
            if type(record_id) is not str or not record_id:
                raise ValueError(f"raw descriptor {label} must be nonempty text")
        if type(self.codec_profile) is not CodecProfile:
            raise TypeError("raw descriptor codec must be CodecProfile")

    @classmethod
    def from_artifact(cls, artifact: RawSegmentArtifact) -> RawSegmentDescriptor:
        if type(artifact) is not RawSegmentArtifact:
            raise TypeError("raw descriptor requires RawSegmentArtifact")
        first = artifact.records[0]
        last = artifact.records[-1]
        return cls(
            segment_identity=artifact.segment_identity,
            segment_root=artifact.segment_root,
            physical_sha256=artifact.physical_sha256,
            physical_size=artifact.physical_size,
            record_count=artifact.record_count,
            logical_payload_bytes=artifact.logical_payload_bytes,
            stored_payload_bytes=artifact.stored_payload_bytes,
            first_arrival_sequence=int(first.metadata.arrival_sequence),
            last_arrival_sequence=int(last.metadata.arrival_sequence),
            first_record_id=first.metadata.record_id,
            last_record_id=last.metadata.record_id,
            codec_profile=artifact.codec_profile,
        )

    def canonical_value(self) -> dict[str, object]:
        return {
            "codec_id": self.codec_profile.codec_id,
            "codec_level": self.codec_profile.level,
            "codec_profile_id": self.codec_profile.profile_id,
            "first_arrival_sequence": self.first_arrival_sequence,
            "first_record_id": self.first_record_id,
            "last_arrival_sequence": self.last_arrival_sequence,
            "last_record_id": self.last_record_id,
            "logical_payload_bytes": self.logical_payload_bytes,
            "physical_sha256": self.physical_sha256.hex(),
            "physical_size": self.physical_size,
            "record_count": self.record_count,
            "segment_identity": self.segment_identity.digest.hex(),
            "segment_root": self.segment_root.hex(),
            "stored_payload_bytes": self.stored_payload_bytes,
        }


@dataclass(frozen=True, slots=True)
class RawManifest:
    store_id: StoreId
    lake_id: RawLakeId
    config_identity: Hash32
    generation: int
    parent_manifest_root: Hash32 | None
    segments: tuple[RawSegmentDescriptor, ...]
    total_record_count: int
    total_logical_payload_bytes: int
    total_stored_payload_bytes: int
    total_physical_segment_bytes: int
    root: Hash32

    def __post_init__(self) -> None:
        if type(self.store_id) is not StoreId or type(self.lake_id) is not RawLakeId:
            raise TypeError("raw manifest store and lake identities are required")
        if type(self.config_identity) is not Hash32 or type(self.root) is not Hash32:
            raise TypeError("raw manifest identities must be Hash32")
        if self.parent_manifest_root is not None and type(self.parent_manifest_root) is not Hash32:
            raise TypeError("raw manifest parent must be Hash32 or None")
        if type(self.generation) is not int or self.generation < 1 or self.generation > UINT64_MAX:
            raise ValueError("raw manifest generation is outside uint64")
        if type(self.segments) is not tuple or any(
            type(segment) is not RawSegmentDescriptor for segment in self.segments
        ):
            raise TypeError("raw manifest segments must be a tuple of descriptors")
        if not self.segments:
            raise ValueError("raw manifest cannot be empty")
        if self.generation == 1 and self.parent_manifest_root is not None:
            raise ValueError("raw genesis manifest cannot have a parent")
        if self.generation > 1 and self.parent_manifest_root is None:
            raise ValueError("non-genesis raw manifest requires a parent")

    def body_value(self) -> dict[str, object]:
        return {
            "config_identity": self.config_identity.hex(),
            "contract": RAW_MANIFEST_CONTRACT,
            "format_version": RAW_MANIFEST_FORMAT_VERSION,
            "generation": self.generation,
            "lake_id": self.lake_id.value,
            "parent_manifest_root": (
                self.parent_manifest_root.hex() if self.parent_manifest_root is not None else None
            ),
            "segments": [segment.canonical_value() for segment in self.segments],
            "store_id": self.store_id.value,
            "total_logical_payload_bytes": self.total_logical_payload_bytes,
            "total_physical_segment_bytes": self.total_physical_segment_bytes,
            "total_record_count": self.total_record_count,
            "total_stored_payload_bytes": self.total_stored_payload_bytes,
        }

    def canonical_value(self) -> dict[str, object]:
        return {**self.body_value(), "root": self.root.hex()}


def _root(body: dict[str, object]) -> Hash32:
    body_bytes = canonical_json_bytes(body)
    return framed_hash(RAW_MANIFEST_DOMAIN, frame_bytes(body_bytes))


def build_raw_manifest(
    *,
    store_id: StoreId,
    lake_id: RawLakeId,
    config_identity: Hash32,
    generation: int,
    parent_manifest_root: Hash32 | None,
    segments: tuple[RawSegmentDescriptor, ...],
) -> RawManifest:
    if type(segments) is not tuple or not segments:
        raise _error(RawManifestErrorCode.TYPE, "raw manifest requires segments")
    total_record_count = sum(segment.record_count for segment in segments)
    total_logical_payload_bytes = sum(segment.logical_payload_bytes for segment in segments)
    total_stored_payload_bytes = sum(segment.stored_payload_bytes for segment in segments)
    total_physical_segment_bytes = sum(segment.physical_size for segment in segments)
    provisional = RawManifest(
        store_id=store_id,
        lake_id=lake_id,
        config_identity=config_identity,
        generation=generation,
        parent_manifest_root=parent_manifest_root,
        segments=segments,
        total_record_count=total_record_count,
        total_logical_payload_bytes=total_logical_payload_bytes,
        total_stored_payload_bytes=total_stored_payload_bytes,
        total_physical_segment_bytes=total_physical_segment_bytes,
        root=Hash32(b"\x00" * 32),
    )
    manifest = RawManifest(
        store_id=store_id,
        lake_id=lake_id,
        config_identity=config_identity,
        generation=generation,
        parent_manifest_root=parent_manifest_root,
        segments=segments,
        total_record_count=total_record_count,
        total_logical_payload_bytes=total_logical_payload_bytes,
        total_stored_payload_bytes=total_stored_payload_bytes,
        total_physical_segment_bytes=total_physical_segment_bytes,
        root=_root(provisional.body_value()),
    )
    verify_raw_manifest(manifest, expected_generation=generation)
    return manifest


def verify_raw_manifest(manifest: RawManifest, *, expected_generation: int | None = None) -> None:
    if type(manifest) is not RawManifest:
        raise _error(RawManifestErrorCode.TYPE, "raw manifest type is invalid")
    try:
        if expected_generation is not None and manifest.generation != expected_generation:
            raise _error(RawManifestErrorCode.FORMAT, "raw manifest generation differs")
        if manifest.generation != len(manifest.segments):
            # Phase 1C deliberately publishes exactly one segment per generation.
            raise _error(RawManifestErrorCode.FORMAT, "raw generation differs from segment count")
        prior_last = -1
        physical_names: set[Hash32] = set()
        for descriptor in manifest.segments:
            if descriptor.first_arrival_sequence <= prior_last:
                raise _error(RawManifestErrorCode.FORMAT, "raw descriptor arrival ranges overlap")
            prior_last = descriptor.last_arrival_sequence
            if descriptor.physical_sha256 in physical_names:
                raise _error(RawManifestErrorCode.FORMAT, "raw physical segment is duplicated")
            physical_names.add(descriptor.physical_sha256)
        if manifest.total_record_count != sum(item.record_count for item in manifest.segments):
            raise _error(RawManifestErrorCode.FORMAT, "raw record total differs")
        if manifest.total_logical_payload_bytes != sum(
            item.logical_payload_bytes for item in manifest.segments
        ):
            raise _error(RawManifestErrorCode.FORMAT, "raw logical byte total differs")
        if manifest.total_stored_payload_bytes != sum(
            item.stored_payload_bytes for item in manifest.segments
        ):
            raise _error(RawManifestErrorCode.FORMAT, "raw stored byte total differs")
        if manifest.total_physical_segment_bytes != sum(
            item.physical_size for item in manifest.segments
        ):
            raise _error(RawManifestErrorCode.FORMAT, "raw physical byte total differs")
        if _root(manifest.body_value()) != manifest.root:
            raise _error(RawManifestErrorCode.HASH, "raw manifest root differs")
    except RawManifestError:
        raise
    except (TypeError, ValueError) as error:
        raise _error(RawManifestErrorCode.FORMAT, "raw manifest is malformed") from error


def verify_raw_manifest_transition(parent: RawManifest, child: RawManifest) -> None:
    try:
        verify_raw_manifest(parent)
        verify_raw_manifest(child)
        if (
            child.store_id != parent.store_id
            or child.lake_id != parent.lake_id
            or child.config_identity != parent.config_identity
            or child.generation != parent.generation + 1
            or child.parent_manifest_root != parent.root
            or len(child.segments) != len(parent.segments) + 1
            or child.segments[: len(parent.segments)] != parent.segments
        ):
            raise _error(RawManifestErrorCode.TRANSITION, "raw manifest is not one direct append")
    except RawManifestError as error:
        if error.code is RawManifestErrorCode.TRANSITION:
            raise
        raise _error(RawManifestErrorCode.TRANSITION, "raw manifest transition is invalid") from error


def raw_manifest_to_bytes(manifest: RawManifest) -> bytes:
    verify_raw_manifest(manifest)
    return canonical_json_bytes(manifest.canonical_value()) + b"\n"


def _exact_keys(value: object, keys: set[str], *, label: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise _error(RawManifestErrorCode.FORMAT, f"{label} field set differs")
    return cast(dict[str, object], value)


def _hash(value: object, *, label: str) -> Hash32:
    if type(value) is not str:
        raise _error(RawManifestErrorCode.FORMAT, f"{label} is not text")
    try:
        return Hash32.from_hex(value)
    except ValueError as error:
        raise _error(RawManifestErrorCode.FORMAT, f"{label} is not lowercase SHA-256") from error


def _integer(value: object, *, label: str) -> int:
    if type(value) is not int:
        raise _error(RawManifestErrorCode.FORMAT, f"{label} is not an exact integer")
    return value


def _descriptor_from_value(value: object) -> RawSegmentDescriptor:
    keys = {
        "codec_id",
        "codec_level",
        "codec_profile_id",
        "first_arrival_sequence",
        "first_record_id",
        "last_arrival_sequence",
        "last_record_id",
        "logical_payload_bytes",
        "physical_sha256",
        "physical_size",
        "record_count",
        "segment_identity",
        "segment_root",
        "stored_payload_bytes",
    }
    item = _exact_keys(value, keys, label="raw segment descriptor")
    try:
        codec = CodecProfile(
            codec_id=_integer(item["codec_id"], label="codec_id"),
            level=_integer(item["codec_level"], label="codec_level"),
        )
        if item["codec_profile_id"] != codec.profile_id:
            raise _error(RawManifestErrorCode.FORMAT, "raw codec profile identity differs")
        return RawSegmentDescriptor(
            segment_identity=SegmentIdentity(_hash(item["segment_identity"], label="segment_identity")),
            segment_root=_hash(item["segment_root"], label="segment_root"),
            physical_sha256=_hash(item["physical_sha256"], label="physical_sha256"),
            physical_size=_integer(item["physical_size"], label="physical_size"),
            record_count=_integer(item["record_count"], label="record_count"),
            logical_payload_bytes=_integer(
                item["logical_payload_bytes"], label="logical_payload_bytes"
            ),
            stored_payload_bytes=_integer(
                item["stored_payload_bytes"], label="stored_payload_bytes"
            ),
            first_arrival_sequence=_integer(
                item["first_arrival_sequence"], label="first_arrival_sequence"
            ),
            last_arrival_sequence=_integer(
                item["last_arrival_sequence"], label="last_arrival_sequence"
            ),
            first_record_id=cast(str, item["first_record_id"]),
            last_record_id=cast(str, item["last_record_id"]),
            codec_profile=codec,
        )
    except RawManifestError:
        raise
    except (TypeError, ValueError) as error:
        raise _error(RawManifestErrorCode.FORMAT, "raw descriptor is malformed") from error


def raw_manifest_from_bytes(
    data: bytes,
    *,
    limits: RawManifestReadLimits | None = None,
) -> RawManifest:
    selected = RawManifestReadLimits() if limits is None else limits
    if type(data) is not bytes:
        raise TypeError("raw manifest reader requires exact bytes")
    if len(data) < 2 or len(data) > selected.max_physical_size or not data.endswith(b"\n"):
        raise _error(RawManifestErrorCode.LIMIT, "raw manifest size or LF framing is invalid")
    body = data[:-1]
    try:
        decoded = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise _error(RawManifestErrorCode.FORMAT, "raw manifest is not strict JSON") from error
    if canonical_json_bytes(decoded) != body:
        raise _error(RawManifestErrorCode.FORMAT, "raw manifest JSON is not canonical")
    keys = {
        "config_identity",
        "contract",
        "format_version",
        "generation",
        "lake_id",
        "parent_manifest_root",
        "root",
        "segments",
        "store_id",
        "total_logical_payload_bytes",
        "total_physical_segment_bytes",
        "total_record_count",
        "total_stored_payload_bytes",
    }
    value = _exact_keys(decoded, keys, label="raw manifest")
    if value["contract"] != RAW_MANIFEST_CONTRACT or value["format_version"] != 1:
        raise _error(RawManifestErrorCode.FORMAT, "raw manifest contract/version differs")
    raw_segments = value["segments"]
    if type(raw_segments) is not list or not raw_segments or len(raw_segments) > selected.max_segments:
        raise _error(RawManifestErrorCode.LIMIT, "raw manifest segment count is invalid")
    parent_value = value["parent_manifest_root"]
    manifest = RawManifest(
        store_id=StoreId(cast(str, value["store_id"])),
        lake_id=RawLakeId(cast(str, value["lake_id"])),
        config_identity=_hash(value["config_identity"], label="config_identity"),
        generation=_integer(value["generation"], label="generation"),
        parent_manifest_root=(
            None if parent_value is None else _hash(parent_value, label="parent_manifest_root")
        ),
        segments=tuple(_descriptor_from_value(item) for item in raw_segments),
        total_record_count=_integer(value["total_record_count"], label="total_record_count"),
        total_logical_payload_bytes=_integer(
            value["total_logical_payload_bytes"], label="total_logical_payload_bytes"
        ),
        total_stored_payload_bytes=_integer(
            value["total_stored_payload_bytes"], label="total_stored_payload_bytes"
        ),
        total_physical_segment_bytes=_integer(
            value["total_physical_segment_bytes"], label="total_physical_segment_bytes"
        ),
        root=_hash(value["root"], label="root"),
    )
    verify_raw_manifest(manifest)
    return manifest


__all__ = [
    "RAW_MANIFEST_CONTRACT",
    "RAW_MANIFEST_FORMAT_VERSION",
    "RAW_MANIFEST_SUFFIX",
    "RawManifest",
    "RawManifestError",
    "RawManifestErrorCode",
    "RawManifestReadLimits",
    "RawSegmentDescriptor",
    "build_raw_manifest",
    "raw_manifest_from_bytes",
    "raw_manifest_to_bytes",
    "verify_raw_manifest",
    "verify_raw_manifest_transition",
]
