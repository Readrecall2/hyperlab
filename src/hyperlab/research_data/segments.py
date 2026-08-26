from __future__ import annotations

import hashlib
import os
import re
import struct
import zlib
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import Any, BinaryIO, Final, cast
from uuid import uuid4

from .canonical import CanonicalValue, canonical_json_bytes, decode_canonical_json
from .envelope import PublicDataEnvelope

SEGMENT_FORMAT_VERSION: Final = 1
MANIFEST_SCHEMA_VERSION: Final = 1
SEGMENT_CODEC: Final = "zlib-fixed-raw-v1"
SEGMENT_SUFFIX: Final = ".rdpseg"
MANIFEST_SUFFIX: Final = ".manifest.json"
_SEGMENT_MAGIC = b"RDPSEG01"
_SEGMENT_END = b"RDPSEGE1"
_BODY_MAGIC = b"RDPBODY1"
_BODY_END = b"RDPBODYE"
_CODEC_ID = 1
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_TMP_NAME = re.compile(r"^[0-9a-f]{32}\.tmp$")
_REPARSE_ATTRIBUTE = 0x400
_ZERO_HASH = b"\x00" * 32


class ResearchDataIntegrityError(ValueError):
    """Authoritative raw data is missing, corrupt, ambiguous, or unsafe."""


class UnsafeAuthorityPathError(ResearchDataIntegrityError):
    """An authoritative path contains a symlink or Windows reparse point."""


class WriterAlreadyActiveError(RuntimeError):
    """The output root already has a live writer."""


class ResearchDataCapacityError(BufferError):
    """The configured physical-byte bound cannot admit another frame."""


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _required_hash(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _HEX64.fullmatch(value) is None:
        raise ResearchDataIntegrityError(f"{label} must be a lowercase SHA-256")
    return value


def _is_reparse(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    attributes = int(getattr(metadata, "st_file_attributes", 0))
    return path.is_symlink() or bool(attributes & _REPARSE_ATTRIBUTE)


def _assert_no_reparse_components(path: Path) -> None:
    absolute = path.absolute()
    components = list(reversed((absolute, *absolute.parents)))
    for component in components:
        if (component.exists() or os.path.lexists(component)) and _is_reparse(component):
            raise UnsafeAuthorityPathError(
                f"authoritative path contains a symlink or reparse point: {component}"
            )


def _safe_directory(path: Path, *, create: bool) -> Path:
    absolute = path.absolute()
    _assert_no_reparse_components(absolute)
    if create:
        absolute.mkdir(parents=True, exist_ok=True)
    if not absolute.is_dir() or _is_reparse(absolute):
        raise UnsafeAuthorityPathError(f"authoritative directory is unsafe: {absolute}")
    _assert_no_reparse_components(absolute)
    return absolute


def _safe_regular_file(path: Path) -> None:
    _assert_no_reparse_components(path)
    if not path.is_file() or _is_reparse(path):
        raise UnsafeAuthorityPathError(f"authoritative file is unsafe: {path}")


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _compress_deterministically(value: bytes) -> bytes:
    compressor = zlib.compressobj(
        level=9,
        method=zlib.DEFLATED,
        wbits=-zlib.MAX_WBITS,
        memLevel=9,
        strategy=zlib.Z_FIXED,
    )
    return compressor.compress(value) + compressor.flush(zlib.Z_FINISH)


def _decompress_strict(value: bytes, *, expected_size: int) -> bytes:
    decoder = zlib.decompressobj(wbits=-zlib.MAX_WBITS)
    try:
        decoded = decoder.decompress(value, expected_size + 1)
        decoded += decoder.flush()
    except zlib.error as error:
        raise ResearchDataIntegrityError("segment compression stream is corrupt") from error
    if decoder.unused_data or decoder.unconsumed_tail or not decoder.eof:
        raise ResearchDataIntegrityError("segment compression stream is truncated or has trailing data")
    if len(decoded) != expected_size:
        raise ResearchDataIntegrityError("segment logical length does not match its header")
    return decoded


@dataclass(frozen=True, slots=True)
class SegmentDescriptor:
    segment_index: int
    physical_sha256: str
    previous_segment_sha256: str | None
    frame_count: int
    logical_bytes: int
    stored_bytes: int
    first_arrival_sequence: int
    last_arrival_sequence: int
    first_receive_timestamp_utc_ns: int
    last_receive_timestamp_utc_ns: int
    collection_id: str
    collector_identities: tuple[str, ...]
    source_metadata_versions: tuple[str, ...]
    codec: str = SEGMENT_CODEC

    def __post_init__(self) -> None:
        integer_fields = (
            self.segment_index,
            self.frame_count,
            self.logical_bytes,
            self.stored_bytes,
            self.first_arrival_sequence,
            self.last_arrival_sequence,
            self.first_receive_timestamp_utc_ns,
            self.last_receive_timestamp_utc_ns,
        )
        if any(type(value) is not int for value in integer_fields):
            raise ResearchDataIntegrityError("segment descriptor counters must be integers")
        if self.segment_index < 0:
            raise ResearchDataIntegrityError("segment index cannot be negative")
        _required_hash(self.physical_sha256, label="segment physical hash")
        if self.previous_segment_sha256 is not None:
            _required_hash(self.previous_segment_sha256, label="previous segment hash")
        if self.frame_count <= 0 or self.logical_bytes <= 0 or self.stored_bytes <= 0:
            raise ResearchDataIntegrityError("segment sizes and count must be positive")
        if self.first_arrival_sequence <= 0 or self.last_arrival_sequence < self.first_arrival_sequence:
            raise ResearchDataIntegrityError("segment arrival sequence range is invalid")
        if self.last_receive_timestamp_utc_ns < self.first_receive_timestamp_utc_ns:
            raise ResearchDataIntegrityError("segment UTC receive range is invalid")
        if not self.collection_id or not self.collector_identities or not self.source_metadata_versions:
            raise ResearchDataIntegrityError("segment provenance is incomplete")
        if tuple(sorted(set(self.collector_identities))) != self.collector_identities:
            raise ResearchDataIntegrityError("collector identities must be sorted and unique")
        if tuple(sorted(set(self.source_metadata_versions))) != self.source_metadata_versions:
            raise ResearchDataIntegrityError("metadata versions must be sorted and unique")
        if self.codec != SEGMENT_CODEC:
            raise ResearchDataIntegrityError("unsupported segment codec")

    def to_dict(self) -> dict[str, CanonicalValue]:
        return {
            "codec": self.codec,
            "collection_id": self.collection_id,
            "collector_identities": list(self.collector_identities),
            "first_arrival_sequence": self.first_arrival_sequence,
            "first_receive_timestamp_utc_ns": self.first_receive_timestamp_utc_ns,
            "frame_count": self.frame_count,
            "last_arrival_sequence": self.last_arrival_sequence,
            "last_receive_timestamp_utc_ns": self.last_receive_timestamp_utc_ns,
            "logical_bytes": self.logical_bytes,
            "physical_sha256": self.physical_sha256,
            "previous_segment_sha256": self.previous_segment_sha256,
            "segment_index": self.segment_index,
            "source_metadata_versions": list(self.source_metadata_versions),
            "stored_bytes": self.stored_bytes,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> SegmentDescriptor:
        expected = {
            "codec",
            "collection_id",
            "collector_identities",
            "first_arrival_sequence",
            "first_receive_timestamp_utc_ns",
            "frame_count",
            "last_arrival_sequence",
            "last_receive_timestamp_utc_ns",
            "logical_bytes",
            "physical_sha256",
            "previous_segment_sha256",
            "segment_index",
            "source_metadata_versions",
            "stored_bytes",
        }
        if set(value) != expected:
            raise ResearchDataIntegrityError("segment descriptor fields differ from schema v1")
        collectors = value["collector_identities"]
        metadata_versions = value["source_metadata_versions"]
        if not isinstance(collectors, list) or not isinstance(metadata_versions, list):
            raise ResearchDataIntegrityError("segment provenance lists are invalid")
        if any(type(item) is not str for item in (*collectors, *metadata_versions)):
            raise ResearchDataIntegrityError("segment provenance entries must be text")
        integer_fields = (
            "segment_index",
            "frame_count",
            "logical_bytes",
            "stored_bytes",
            "first_arrival_sequence",
            "last_arrival_sequence",
            "first_receive_timestamp_utc_ns",
            "last_receive_timestamp_utc_ns",
        )
        if any(type(value[field]) is not int for field in integer_fields):
            raise ResearchDataIntegrityError("segment descriptor counters must be integers")
        string_fields = ("physical_sha256", "collection_id", "codec")
        if any(type(value[field]) is not str for field in string_fields):
            raise ResearchDataIntegrityError("segment descriptor identities must be text")
        previous = value["previous_segment_sha256"]
        if previous is not None and type(previous) is not str:
            raise ResearchDataIntegrityError("previous segment hash must be absent or text")
        return cls(
            segment_index=cast(int, value["segment_index"]),
            physical_sha256=cast(str, value["physical_sha256"]),
            previous_segment_sha256=previous,
            frame_count=cast(int, value["frame_count"]),
            logical_bytes=cast(int, value["logical_bytes"]),
            stored_bytes=cast(int, value["stored_bytes"]),
            first_arrival_sequence=cast(int, value["first_arrival_sequence"]),
            last_arrival_sequence=cast(int, value["last_arrival_sequence"]),
            first_receive_timestamp_utc_ns=cast(
                int, value["first_receive_timestamp_utc_ns"]
            ),
            last_receive_timestamp_utc_ns=cast(
                int, value["last_receive_timestamp_utc_ns"]
            ),
            collection_id=cast(str, value["collection_id"]),
            collector_identities=tuple(cast(list[str], collectors)),
            source_metadata_versions=tuple(cast(list[str], metadata_versions)),
            codec=cast(str, value["codec"]),
        )


@dataclass(frozen=True, slots=True)
class SegmentArtifact:
    descriptor: SegmentDescriptor
    envelopes: tuple[PublicDataEnvelope, ...]
    physical_bytes: bytes


def _body_bytes(
    envelopes: Sequence[PublicDataEnvelope],
    *,
    segment_index: int,
    previous_segment_sha256: str | None,
) -> bytes:
    previous = _ZERO_HASH if previous_segment_sha256 is None else bytes.fromhex(previous_segment_sha256)
    parts = [_BODY_MAGIC, struct.pack(">Q", segment_index), previous, struct.pack(">I", len(envelopes))]
    for envelope in envelopes:
        frame = envelope.canonical_bytes()
        parts.extend((struct.pack(">Q", len(frame)), hashlib.sha256(frame).digest(), frame))
    core = b"".join(parts)
    return core + hashlib.sha256(core).digest() + _BODY_END


def build_segment(
    envelopes: Sequence[PublicDataEnvelope],
    *,
    segment_index: int,
    previous_segment_sha256: str | None,
    collection_id: str,
) -> SegmentArtifact:
    if not envelopes:
        raise ValueError("a segment must contain at least one envelope")
    if any(envelope.provenance.collection_id != collection_id for envelope in envelopes):
        raise ValueError("all segment frames must share the writer collection id")
    for previous_envelope, envelope in pairwise(envelopes):
        if envelope.arrival_sequence != previous_envelope.arrival_sequence + 1:
            raise ValueError("segment arrival sequences must be contiguous")
        if (
            envelope.session_identity == previous_envelope.session_identity
            and envelope.receive_monotonic_ns < previous_envelope.receive_monotonic_ns
        ):
            raise ValueError("segment monotonic receive time regressed within a session")
    body = _body_bytes(
        envelopes,
        segment_index=segment_index,
        previous_segment_sha256=previous_segment_sha256,
    )
    compressed = _compress_deterministically(body)
    prefix = b"".join(
        (
            _SEGMENT_MAGIC,
            struct.pack(">HHQQ", SEGMENT_FORMAT_VERSION, _CODEC_ID, len(body), len(compressed)),
            hashlib.sha256(body).digest(),
            compressed,
        )
    )
    physical = prefix + hashlib.sha256(prefix).digest() + _SEGMENT_END
    physical_sha256 = _sha256(physical)
    descriptor = SegmentDescriptor(
        segment_index=segment_index,
        physical_sha256=physical_sha256,
        previous_segment_sha256=previous_segment_sha256,
        frame_count=len(envelopes),
        logical_bytes=len(body),
        stored_bytes=len(physical),
        first_arrival_sequence=envelopes[0].arrival_sequence,
        last_arrival_sequence=envelopes[-1].arrival_sequence,
        first_receive_timestamp_utc_ns=min(
            item.receive_timestamp_utc_ns for item in envelopes
        ),
        last_receive_timestamp_utc_ns=max(
            item.receive_timestamp_utc_ns for item in envelopes
        ),
        collection_id=collection_id,
        collector_identities=tuple(sorted({item.collector_identity for item in envelopes})),
        source_metadata_versions=tuple(
            sorted({item.source_metadata_version for item in envelopes})
        ),
    )
    return SegmentArtifact(descriptor, tuple(envelopes), physical)


def decode_segment(value: bytes, *, expected_physical_sha256: str | None = None) -> SegmentArtifact:
    minimum = len(_SEGMENT_MAGIC) + 20 + 32 + 32 + len(_SEGMENT_END)
    if len(value) < minimum:
        raise ResearchDataIntegrityError("segment is truncated before its minimum framing")
    if expected_physical_sha256 is not None and _sha256(value) != expected_physical_sha256:
        raise ResearchDataIntegrityError("segment physical SHA-256 mismatch")
    if value[:8] != _SEGMENT_MAGIC or value[-8:] != _SEGMENT_END:
        raise ResearchDataIntegrityError("segment magic is missing")
    version, codec, logical_size, compressed_size = struct.unpack(">HHQQ", value[8:28])
    if version != SEGMENT_FORMAT_VERSION or codec != _CODEC_ID:
        raise ResearchDataIntegrityError("segment format or codec is unsupported")
    logical_sha256 = value[28:60]
    compressed_start = 60
    compressed_end = compressed_start + compressed_size
    if compressed_end + 40 != len(value):
        raise ResearchDataIntegrityError("segment physical length disagrees with its header")
    prefix = value[:compressed_end]
    if hashlib.sha256(prefix).digest() != value[compressed_end : compressed_end + 32]:
        raise ResearchDataIntegrityError("segment checksum mismatch")
    body = _decompress_strict(value[compressed_start:compressed_end], expected_size=logical_size)
    if hashlib.sha256(body).digest() != logical_sha256:
        raise ResearchDataIntegrityError("segment logical SHA-256 mismatch")
    if len(body) < 84 or body[:8] != _BODY_MAGIC or body[-8:] != _BODY_END:
        raise ResearchDataIntegrityError("segment body framing is truncated")
    if hashlib.sha256(body[:-40]).digest() != body[-40:-8]:
        raise ResearchDataIntegrityError("segment body checksum mismatch")
    segment_index = struct.unpack(">Q", body[8:16])[0]
    previous_raw = body[16:48]
    previous = None if previous_raw == _ZERO_HASH else previous_raw.hex()
    frame_count = struct.unpack(">I", body[48:52])[0]
    if frame_count <= 0:
        raise ResearchDataIntegrityError("segment body has no frames")
    cursor = 52
    envelopes: list[PublicDataEnvelope] = []
    for _ in range(frame_count):
        if cursor + 40 > len(body) - 40:
            raise ResearchDataIntegrityError("segment frame header is truncated")
        frame_size = struct.unpack(">Q", body[cursor : cursor + 8])[0]
        frame_sha256 = body[cursor + 8 : cursor + 40]
        cursor += 40
        frame_end = cursor + frame_size
        if frame_end > len(body) - 40:
            raise ResearchDataIntegrityError("segment frame payload is truncated")
        frame = body[cursor:frame_end]
        cursor = frame_end
        if hashlib.sha256(frame).digest() != frame_sha256:
            raise ResearchDataIntegrityError("segment frame SHA-256 mismatch")
        envelopes.append(PublicDataEnvelope.from_canonical_bytes(frame))
    if cursor != len(body) - 40:
        raise ResearchDataIntegrityError("segment body has trailing bytes")
    collections = {item.provenance.collection_id for item in envelopes}
    if len(collections) != 1:
        raise ResearchDataIntegrityError("segment frames mix collection identities")
    collection_id = next(iter(collections))
    physical_sha256 = _sha256(value)
    descriptor = SegmentDescriptor(
        segment_index=segment_index,
        physical_sha256=physical_sha256,
        previous_segment_sha256=previous,
        frame_count=len(envelopes),
        logical_bytes=len(body),
        stored_bytes=len(value),
        first_arrival_sequence=envelopes[0].arrival_sequence,
        last_arrival_sequence=envelopes[-1].arrival_sequence,
        first_receive_timestamp_utc_ns=min(
            item.receive_timestamp_utc_ns for item in envelopes
        ),
        last_receive_timestamp_utc_ns=max(
            item.receive_timestamp_utc_ns for item in envelopes
        ),
        collection_id=collection_id,
        collector_identities=tuple(sorted({item.collector_identity for item in envelopes})),
        source_metadata_versions=tuple(
            sorted({item.source_metadata_version for item in envelopes})
        ),
    )
    return SegmentArtifact(descriptor, tuple(envelopes), value)


def _manifest_root(segments: Sequence[SegmentDescriptor]) -> str:
    digest = hashlib.sha256(b"HYPERLAB_RESEARCH_DATA_ROOT_V1")
    for segment in segments:
        digest.update(bytes.fromhex(segment.physical_sha256))
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class ManifestRecord:
    generation: int
    collection_id: str
    previous_manifest_sha256: str | None
    segments: tuple[SegmentDescriptor, ...]
    root_sha256: str
    manifest_sha256: str

    @property
    def frame_count(self) -> int:
        return sum(item.frame_count for item in self.segments)

    @property
    def stored_segment_bytes(self) -> int:
        return sum(item.stored_bytes for item in self.segments)

    def body(self) -> dict[str, CanonicalValue]:
        return {
            "collection_id": self.collection_id,
            "frame_count": self.frame_count,
            "generation": self.generation,
            "manifest_type": "HYPERLAB_PUBLIC_RESEARCH_RAW_MANIFEST",
            "previous_manifest_sha256": self.previous_manifest_sha256,
            "root_sha256": self.root_sha256,
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "segment_count": len(self.segments),
            "segments": [item.to_dict() for item in self.segments],
            "stored_segment_bytes": self.stored_segment_bytes,
        }

    def canonical_bytes(self) -> bytes:
        body = canonical_json_bytes(self.body())
        if _sha256(body) != self.manifest_sha256:
            raise ResearchDataIntegrityError("manifest identity disagrees with its body")
        return body


def build_manifest(
    *,
    generation: int,
    collection_id: str,
    previous_manifest_sha256: str | None,
    segments: Sequence[SegmentDescriptor],
) -> ManifestRecord:
    if generation <= 0 or not collection_id or not segments:
        raise ValueError("a published manifest needs a generation, collection, and segments")
    root = _manifest_root(segments)
    provisional = ManifestRecord(
        generation=generation,
        collection_id=collection_id,
        previous_manifest_sha256=previous_manifest_sha256,
        segments=tuple(segments),
        root_sha256=root,
        manifest_sha256="0" * 64,
    )
    identity = _sha256(canonical_json_bytes(provisional.body()))
    return ManifestRecord(
        generation=generation,
        collection_id=collection_id,
        previous_manifest_sha256=previous_manifest_sha256,
        segments=tuple(segments),
        root_sha256=root,
        manifest_sha256=identity,
    )


def decode_manifest(value: bytes, *, expected_manifest_sha256: str) -> ManifestRecord:
    _required_hash(expected_manifest_sha256, label="expected manifest hash")
    if _sha256(value) != expected_manifest_sha256:
        raise ResearchDataIntegrityError("manifest content SHA-256 mismatch")
    decoded = decode_canonical_json(value, require_canonical=True)
    if not isinstance(decoded, dict):
        raise ResearchDataIntegrityError("manifest must be a canonical JSON object")
    expected = {
        "collection_id",
        "frame_count",
        "generation",
        "manifest_type",
        "previous_manifest_sha256",
        "root_sha256",
        "schema_version",
        "segment_count",
        "segments",
        "stored_segment_bytes",
    }
    if set(decoded) != expected:
        raise ResearchDataIntegrityError("manifest fields differ from schema v1")
    if decoded["schema_version"] != MANIFEST_SCHEMA_VERSION:
        raise ResearchDataIntegrityError("manifest schema version is unsupported")
    if decoded["manifest_type"] != "HYPERLAB_PUBLIC_RESEARCH_RAW_MANIFEST":
        raise ResearchDataIntegrityError("manifest type is invalid")
    raw_segments = decoded["segments"]
    if not isinstance(raw_segments, list):
        raise ResearchDataIntegrityError("manifest segments must be an array")
    segments = tuple(
        SegmentDescriptor.from_dict(cast(Mapping[str, object], item))
        for item in raw_segments
        if isinstance(item, dict)
    )
    if len(segments) != len(raw_segments):
        raise ResearchDataIntegrityError("manifest contains a non-object segment descriptor")
    previous_raw = decoded["previous_manifest_sha256"]
    for field in ("generation", "segment_count", "frame_count", "stored_segment_bytes"):
        if type(decoded[field]) is not int:
            raise ResearchDataIntegrityError(f"manifest {field} must be an integer")
    for field in ("collection_id", "root_sha256"):
        if type(decoded[field]) is not str:
            raise ResearchDataIntegrityError(f"manifest {field} must be text")
    if previous_raw is not None and type(previous_raw) is not str:
        raise ResearchDataIntegrityError("previous manifest hash must be absent or text")
    record = ManifestRecord(
        generation=cast(int, decoded["generation"]),
        collection_id=cast(str, decoded["collection_id"]),
        previous_manifest_sha256=previous_raw,
        segments=segments,
        root_sha256=cast(str, decoded["root_sha256"]),
        manifest_sha256=expected_manifest_sha256,
    )
    if record.generation <= 0 or not record.segments:
        raise ResearchDataIntegrityError("manifest generation or segment set is invalid")
    if record.previous_manifest_sha256 is not None:
        _required_hash(record.previous_manifest_sha256, label="previous manifest hash")
    if _manifest_root(record.segments) != record.root_sha256:
        raise ResearchDataIntegrityError("manifest root SHA-256 mismatch")
    if decoded["segment_count"] != len(record.segments):
        raise ResearchDataIntegrityError("manifest segment count mismatch")
    if decoded["frame_count"] != record.frame_count:
        raise ResearchDataIntegrityError("manifest frame count mismatch")
    if decoded["stored_segment_bytes"] != record.stored_segment_bytes:
        raise ResearchDataIntegrityError("manifest stored-byte count mismatch")
    _validate_segment_chain(record.segments, collection_id=record.collection_id)
    return record


def _validate_segment_chain(
    segments: Sequence[SegmentDescriptor], *, collection_id: str
) -> None:
    previous: str | None = None
    previous_arrival_sequence = 0
    for expected_index, segment in enumerate(segments):
        if segment.segment_index != expected_index:
            raise ResearchDataIntegrityError("segment indexes are not contiguous from zero")
        if segment.previous_segment_sha256 != previous:
            raise ResearchDataIntegrityError("segment physical chain is broken")
        if segment.collection_id != collection_id:
            raise ResearchDataIntegrityError("segment collection differs from manifest")
        if segment.first_arrival_sequence != previous_arrival_sequence + 1:
            raise ResearchDataIntegrityError("segment arrival sequence chain is not contiguous")
        previous = segment.physical_sha256
        previous_arrival_sequence = segment.last_arrival_sequence


class _RootWriterLock:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._file: BinaryIO | None = None

    def acquire(self) -> None:
        if _is_reparse(self._path):
            raise UnsafeAuthorityPathError("writer lock path is a symlink or reparse point")
        handle = self._path.open("a+b")
        try:
            if handle.seek(0, os.SEEK_END) == 0:
                handle.write(b"\0")
                handle.flush()
                os.fsync(handle.fileno())
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl: Any = __import__("fcntl")
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            handle.close()
            raise WriterAlreadyActiveError(
                f"another Research Data Plane writer owns {self._path.parent}"
            ) from error
        self._file = handle

    def release(self) -> None:
        handle = self._file
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl: Any = __import__("fcntl")
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
            self._file = None


class ResearchSegmentWriter:
    """Mono-writer, append-only publication of immutable public-data segments."""

    def __init__(
        self,
        root: Path,
        *,
        collection_id: str,
        max_segment_bytes: int,
        rotation_seconds: float,
        max_total_bytes: int,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        if not collection_id:
            raise ValueError("collection id is required")
        if max_segment_bytes <= 0 or rotation_seconds <= 0 or max_total_bytes <= 0:
            raise ValueError("segment and total bounds must be positive")
        self.root = _safe_directory(root, create=True)
        self._segments_dir = _safe_directory(self.root / "segments", create=True)
        self._manifests_dir = _safe_directory(self.root / "manifests", create=True)
        self._staging_dir = _safe_directory(self.root / "staging", create=True)
        self.collection_id = collection_id
        self.max_segment_bytes = max_segment_bytes
        self.rotation_ns = int(rotation_seconds * 1_000_000_000)
        self.max_total_bytes = max_total_bytes
        self._fault_injector = fault_injector
        self._lock = _RootWriterLock(self.root / ".writer.lock")
        self._lock.acquire()
        self._closed = False
        self._frames: list[PublicDataEnvelope] = []
        self._frame_bytes = 0
        self._segment_started_monotonic_ns: int | None = None
        self._segments: list[SegmentDescriptor] = []
        self._manifest: ManifestRecord | None = None
        self._last_admitted_monotonic_ns: int | None = None
        self._last_admitted_session_identity: str | None = None
        try:
            self._recover()
        except BaseException:
            self._lock.release()
            self._closed = True
            raise

    @property
    def manifest_sha256(self) -> str | None:
        return None if self._manifest is None else self._manifest.manifest_sha256

    @property
    def frame_count(self) -> int:
        return sum(item.frame_count for item in self._segments) + len(self._frames)

    @property
    def segment_count(self) -> int:
        return len(self._segments)

    @property
    def stored_segment_bytes(self) -> int:
        return sum(item.stored_bytes for item in self._segments)

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _cleanup_staging(self) -> None:
        for path in self._staging_dir.iterdir():
            if _is_reparse(path) or not path.is_file() or _TMP_NAME.fullmatch(path.name) is None:
                raise ResearchDataIntegrityError(f"unexpected staging artifact: {path.name}")
            path.unlink()
        _fsync_directory(self._staging_dir)

    def _read_manifests(self) -> dict[str, ManifestRecord]:
        records: dict[str, ManifestRecord] = {}
        for path in self._manifests_dir.iterdir():
            if not path.name.endswith(MANIFEST_SUFFIX):
                raise ResearchDataIntegrityError(f"unexpected manifest artifact: {path.name}")
            identity = path.name.removesuffix(MANIFEST_SUFFIX)
            _required_hash(identity, label="manifest filename")
            _safe_regular_file(path)
            records[identity] = decode_manifest(path.read_bytes(), expected_manifest_sha256=identity)
        return records

    def _manifest_head(self, records: Mapping[str, ManifestRecord]) -> ManifestRecord | None:
        if not records:
            return None
        referenced: set[str] = set()
        child_count: dict[str, int] = {}
        for record in records.values():
            previous = record.previous_manifest_sha256
            if previous is None:
                continue
            if previous not in records:
                raise ResearchDataIntegrityError("manifest chain references a missing predecessor")
            referenced.add(previous)
            child_count[previous] = child_count.get(previous, 0) + 1
        if any(count != 1 for count in child_count.values()):
            raise ResearchDataIntegrityError("manifest chain has a fork")
        heads = set(records) - referenced
        if len(heads) != 1:
            raise ResearchDataIntegrityError("manifest chain does not have exactly one head")
        head = records[next(iter(heads))]
        visited: list[ManifestRecord] = []
        current: ManifestRecord | None = head
        while current is not None:
            visited.append(current)
            previous = current.previous_manifest_sha256
            current = None if previous is None else records[previous]
        if len(visited) != len(records):
            raise ResearchDataIntegrityError("manifest directory contains a disconnected chain")
        ordered = list(reversed(visited))
        for generation, record in enumerate(ordered, start=1):
            if record.generation != generation:
                raise ResearchDataIntegrityError("manifest generations are not contiguous")
            if record.collection_id != self.collection_id:
                raise ResearchDataIntegrityError("manifest collection differs from requested writer")
            if generation > 1 and record.segments[: -1] != ordered[generation - 2].segments:
                raise ResearchDataIntegrityError("manifest successor is not an append-only extension")
            if generation > 1 and len(record.segments) != len(ordered[generation - 2].segments) + 1:
                raise ResearchDataIntegrityError("each manifest must publish exactly one segment")
        return head

    def _segment_path(self, identity: str) -> Path:
        _required_hash(identity, label="segment hash")
        return self._segments_dir / f"{identity}{SEGMENT_SUFFIX}"

    def _read_segment_path(self, path: Path) -> SegmentArtifact:
        if not path.name.endswith(SEGMENT_SUFFIX):
            raise ResearchDataIntegrityError(f"unexpected segment artifact: {path.name}")
        identity = path.name.removesuffix(SEGMENT_SUFFIX)
        _required_hash(identity, label="segment filename")
        _safe_regular_file(path)
        return decode_segment(path.read_bytes(), expected_physical_sha256=identity)

    def _recover(self) -> None:
        self._cleanup_staging()
        records = self._read_manifests()
        head = self._manifest_head(records)
        referenced = () if head is None else head.segments
        for descriptor in referenced:
            path = self._segment_path(descriptor.physical_sha256)
            if not path.exists():
                raise ResearchDataIntegrityError("manifest references a missing segment")
            artifact = self._read_segment_path(path)
            if artifact.descriptor != descriptor:
                raise ResearchDataIntegrityError("published segment disagrees with its manifest descriptor")
        known = {item.physical_sha256 for item in referenced}
        orphans: list[SegmentDescriptor] = []
        for path in self._segments_dir.iterdir():
            artifact = self._read_segment_path(path)
            if artifact.descriptor.physical_sha256 not in known:
                orphans.append(artifact.descriptor)
        self._segments = list(referenced)
        self._manifest = head
        for descriptor in sorted(orphans, key=lambda item: item.segment_index):
            expected_index = len(self._segments)
            previous = None if not self._segments else self._segments[-1].physical_sha256
            if descriptor.segment_index != expected_index or descriptor.previous_segment_sha256 != previous:
                raise ResearchDataIntegrityError("orphan segment cannot extend the authenticated chain")
            if descriptor.collection_id != self.collection_id:
                raise ResearchDataIntegrityError("orphan segment belongs to another collection")
            self._segments.append(descriptor)
            self._publish_manifest()
        if self._segments:
            tail = self._read_segment_path(
                self._segment_path(self._segments[-1].physical_sha256)
            ).envelopes[-1]
            self._last_admitted_monotonic_ns = tail.receive_monotonic_ns
            self._last_admitted_session_identity = tail.session_identity

    def can_accept(self, envelope: PublicDataEnvelope) -> bool:
        frame_size = len(envelope.canonical_bytes()) + 40
        logical_bound = self._frame_bytes + frame_size + 92
        compressed_bound = (
            logical_bound
            + (logical_bound >> 12)
            + (logical_bound >> 14)
            + (logical_bound >> 25)
            + 13
        )
        conservative_segment_bytes = compressed_bound + 100
        return self.stored_segment_bytes + conservative_segment_bytes <= self.max_total_bytes

    def append(self, envelope: PublicDataEnvelope) -> None:
        if self._closed:
            raise RuntimeError("cannot append to a closed Research Data Plane writer")
        if envelope.provenance.collection_id != self.collection_id:
            raise ValueError("envelope collection differs from writer collection")
        last_arrival_sequence = (
            self._frames[-1].arrival_sequence
            if self._frames
            else (0 if not self._segments else self._segments[-1].last_arrival_sequence)
        )
        if envelope.arrival_sequence != last_arrival_sequence + 1:
            raise ValueError("writer arrival sequences must be contiguous")
        if (
            self._last_admitted_monotonic_ns is not None
            and envelope.session_identity == self._last_admitted_session_identity
            and envelope.receive_monotonic_ns < self._last_admitted_monotonic_ns
        ):
            raise ValueError("writer monotonic receive time regressed within a session")
        frame_size = len(envelope.canonical_bytes()) + 40
        if frame_size + 92 > self.max_segment_bytes:
            raise ResearchDataCapacityError("one frame exceeds max_segment_bytes")
        should_rotate = bool(self._frames) and (
            self._frame_bytes + frame_size + 92 > self.max_segment_bytes
            or (
                self._segment_started_monotonic_ns is not None
                and envelope.receive_monotonic_ns - self._segment_started_monotonic_ns >= self.rotation_ns
            )
        )
        if should_rotate:
            self.flush()
        if not self.can_accept(envelope):
            raise ResearchDataCapacityError("max_total_bytes would be exceeded")
        if not self._frames:
            self._segment_started_monotonic_ns = envelope.receive_monotonic_ns
        self._frames.append(envelope)
        self._frame_bytes += frame_size
        self._last_admitted_monotonic_ns = envelope.receive_monotonic_ns
        self._last_admitted_session_identity = envelope.session_identity

    def _atomic_publish(self, target: Path, value: bytes, *, before: str, after: str) -> None:
        temporary = self._staging_dir / f"{uuid4().hex}.tmp"
        with temporary.open("xb") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        self._fault(before)
        try:
            os.link(temporary, target)
        except FileExistsError as error:
            _safe_regular_file(target)
            if target.read_bytes() != value:
                raise ResearchDataIntegrityError(
                    "content-addressed target already exists with other bytes"
                ) from error
        temporary.unlink()
        _fsync_directory(target.parent)
        self._fault(after)

    def flush(self) -> SegmentDescriptor | None:
        if self._closed:
            raise RuntimeError("cannot flush a closed Research Data Plane writer")
        if not self._frames:
            return None
        previous = None if not self._segments else self._segments[-1].physical_sha256
        artifact = build_segment(
            self._frames,
            segment_index=len(self._segments),
            previous_segment_sha256=previous,
            collection_id=self.collection_id,
        )
        if self.stored_segment_bytes + artifact.descriptor.stored_bytes > self.max_total_bytes:
            raise ResearchDataCapacityError("max_total_bytes would be exceeded by segment publication")
        target = self._segment_path(artifact.descriptor.physical_sha256)
        self._atomic_publish(
            target,
            artifact.physical_bytes,
            before="before_segment_publish",
            after="after_segment_publish",
        )
        self._segments.append(artifact.descriptor)
        self._frames = []
        self._frame_bytes = 0
        self._segment_started_monotonic_ns = None
        self._publish_manifest()
        return artifact.descriptor

    def _publish_manifest(self) -> None:
        previous = None if self._manifest is None else self._manifest.manifest_sha256
        record = build_manifest(
            generation=1 if self._manifest is None else self._manifest.generation + 1,
            collection_id=self.collection_id,
            previous_manifest_sha256=previous,
            segments=self._segments,
        )
        target = self._manifests_dir / f"{record.manifest_sha256}{MANIFEST_SUFFIX}"
        self._atomic_publish(
            target,
            record.canonical_bytes(),
            before="before_manifest_publish",
            after="after_manifest_publish",
        )
        self._manifest = record

    def close(self) -> ManifestRecord | None:
        if self._closed:
            return self._manifest
        try:
            self.flush()
            return self._manifest
        finally:
            self._closed = True
            self._lock.release()

    def abort(self) -> None:
        """Release only the OS writer lease, simulating abrupt process termination."""

        if not self._closed:
            self._closed = True
            self._lock.release()

    def __enter__(self) -> ResearchSegmentWriter:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if exc is None:
            self.close()
        else:
            self.abort()


class ResearchSegmentReader:
    """Explicit-manifest, read-only replay of immutable raw segments."""

    def __init__(self, root: Path, *, manifest_sha256: str) -> None:
        _required_hash(manifest_sha256, label="manifest hash")
        self.root = _safe_directory(root, create=False)
        self._segments_dir = _safe_directory(self.root / "segments", create=False)
        self._manifests_dir = _safe_directory(self.root / "manifests", create=False)
        self.manifest = self._load_chain(manifest_sha256)
        self._validate_segments()

    def _load_chain(self, manifest_sha256: str) -> ManifestRecord:
        records: list[ManifestRecord] = []
        current: str | None = manifest_sha256
        while current is not None:
            path = self._manifests_dir / f"{current}{MANIFEST_SUFFIX}"
            if not path.exists():
                raise ResearchDataIntegrityError("required manifest is missing")
            _safe_regular_file(path)
            record = decode_manifest(path.read_bytes(), expected_manifest_sha256=current)
            records.append(record)
            current = record.previous_manifest_sha256
        ordered = list(reversed(records))
        for generation, record in enumerate(ordered, start=1):
            if record.generation != generation:
                raise ResearchDataIntegrityError("manifest chain generations are not contiguous")
            if generation > 1:
                predecessor = ordered[generation - 2]
                if record.segments[:-1] != predecessor.segments:
                    raise ResearchDataIntegrityError("manifest chain is not append-only")
        return records[0]

    def _validate_segments(self) -> None:
        last_arrival_sequence = 0
        last_monotonic_ns: int | None = None
        last_session_identity: str | None = None
        for descriptor in self.manifest.segments:
            path = self._segments_dir / f"{descriptor.physical_sha256}{SEGMENT_SUFFIX}"
            if not path.exists():
                raise ResearchDataIntegrityError("required segment is missing")
            _safe_regular_file(path)
            artifact = decode_segment(
                path.read_bytes(), expected_physical_sha256=descriptor.physical_sha256
            )
            if artifact.descriptor != descriptor:
                raise ResearchDataIntegrityError("segment descriptor differs from manifest")
            for envelope in artifact.envelopes:
                if envelope.arrival_sequence != last_arrival_sequence + 1:
                    raise ResearchDataIntegrityError(
                        "replayed arrival sequences are not contiguous"
                    )
                if (
                    last_monotonic_ns is not None
                    and envelope.session_identity == last_session_identity
                    and envelope.receive_monotonic_ns < last_monotonic_ns
                ):
                    raise ResearchDataIntegrityError(
                        "replayed monotonic receive time regressed within a session"
                    )
                last_arrival_sequence = envelope.arrival_sequence
                last_monotonic_ns = envelope.receive_monotonic_ns
                last_session_identity = envelope.session_identity

    def iter_envelopes(self) -> Iterator[PublicDataEnvelope]:
        for descriptor in self.manifest.segments:
            path = self._segments_dir / f"{descriptor.physical_sha256}{SEGMENT_SUFFIX}"
            _safe_regular_file(path)
            artifact = decode_segment(
                path.read_bytes(), expected_physical_sha256=descriptor.physical_sha256
            )
            yield from artifact.envelopes

    def replay(self) -> tuple[PublicDataEnvelope, ...]:
        return tuple(self.iter_envelopes())


__all__ = [
    "MANIFEST_SUFFIX",
    "SEGMENT_CODEC",
    "SEGMENT_SUFFIX",
    "ManifestRecord",
    "ResearchDataCapacityError",
    "ResearchDataIntegrityError",
    "ResearchSegmentReader",
    "ResearchSegmentWriter",
    "SegmentArtifact",
    "SegmentDescriptor",
    "UnsafeAuthorityPathError",
    "WriterAlreadyActiveError",
    "build_manifest",
    "build_segment",
    "decode_manifest",
    "decode_segment",
]
