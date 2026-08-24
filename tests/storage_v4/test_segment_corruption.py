from __future__ import annotations

import hashlib
import zlib
from dataclasses import replace

import pytest

from hyperlab.paper.storage_v4.canonical import build_commit_logical
from hyperlab.paper.storage_v4.segment import (
    SEGMENT_MAGIC,
    CodecProfile,
    SegmentFormatError,
    SegmentReadLimits,
    build_segment,
    read_segment,
)
from hyperlab.paper.storage_v4.types import (
    CommitFrame,
    CommitOrdinal,
    CommitSequence,
    Hash32,
    LogicalRow,
    RunId,
    StreamId,
)

SYNTHETIC_STORAGE_V4_WORKLOAD = True
_RUN_ID = RunId("SYNTHETIC_STORAGE_V4_WORKLOAD/run-corruption")


def _frame(sequence: int, previous: Hash32, *, run_id: RunId = _RUN_ID) -> CommitFrame:
    return CommitFrame(
        run_id=run_id,
        commit_sequence=CommitSequence(sequence),
        previous_prefix_root=previous,
        rows=(
            LogicalRow(
                stream_id=StreamId("events"),
                ordinal=CommitOrdinal(0),
                value={"sequence": sequence, "payload": "x" * 80},
            ),
        ),
    )


def _frames() -> tuple[CommitFrame, ...]:
    first = _frame(100, Hash32(b"\x00" * 32))
    second = _frame(101, build_commit_logical(first).prefix_root)
    third = _frame(102, build_commit_logical(second).prefix_root)
    return first, second, third


def _artifact():
    return build_segment(
        _frames(),
        codec=CodecProfile.zlib(level=6),
        max_commits_per_block=2,
        max_logical_bytes_per_block=10_000,
    )


def _flipped(data: bytes, offset: int) -> bytes:
    changed = bytearray(data)
    changed[offset] ^= 0x01
    return bytes(changed)


def _refresh_physical_envelope(data: bytearray, artifact, *, header_changed: bool = False) -> bytes:
    footer_offset = artifact.layout.footer_offset
    footer_checksum_offset = footer_offset + 100 + 8 * artifact.block_count
    if header_changed:
        header = data[
            artifact.layout.header_offset : artifact.layout.header_offset
            + artifact.layout.header_size
        ]
        data[footer_offset + 32 : footer_offset + 64] = hashlib.sha256(header).digest()
    data[footer_offset + 64 : footer_offset + 96] = hashlib.sha256(
        data[:footer_offset]
    ).digest()
    footer_checksum_input = (
        data[footer_offset:footer_checksum_offset] + data[-16:]
    )
    data[footer_checksum_offset : footer_checksum_offset + 32] = hashlib.sha256(
        footer_checksum_input
    ).digest()
    return bytes(data)


@pytest.mark.parametrize(
    "mutator",
    (
        lambda artifact: _flipped(artifact.data, 0),
        lambda artifact: _flipped(artifact.data, len(SEGMENT_MAGIC)),
        lambda artifact: _flipped(artifact.data, artifact.layout.header_offset),
        lambda artifact: _flipped(artifact.data, artifact.layout.blocks[0].payload_offset),
        lambda artifact: _flipped(
            artifact.data, artifact.layout.blocks[0].payload_sha256_offset
        ),
        lambda artifact: _flipped(artifact.data, artifact.layout.footer_offset + 12),
    ),
    ids=(
        "bad-magic",
        "bad-version",
        "altered-header",
        "altered-payload",
        "altered-block-checksum",
        "altered-footer",
    ),
)
def test_segment_reader_rejects_physical_corruption(mutator) -> None:
    artifact = _artifact()

    with pytest.raises(SegmentFormatError):
        read_segment(mutator(artifact))


def test_segment_reader_rejects_truncation_at_multiple_positions() -> None:
    artifact = _artifact()
    positions = (
        1,
        artifact.layout.header_offset + artifact.layout.header_size // 2,
        artifact.layout.blocks[0].payload_offset + 1,
        artifact.layout.footer_offset + 1,
        len(artifact.data) - 1,
    )

    for position in positions:
        with pytest.raises(SegmentFormatError):
            read_segment(artifact.data[:position])


def test_writer_rejects_missing_duplicate_or_noncontiguous_commit() -> None:
    first, second, third = _frames()

    with pytest.raises(ValueError, match="contiguous"):
        build_segment((first, third), codec=CodecProfile.raw())
    with pytest.raises(ValueError, match="contiguous"):
        build_segment((first, second, second), codec=CodecProfile.raw())
    with pytest.raises(ValueError, match="contiguous"):
        build_segment(
            (first, replace(second, commit_sequence=CommitSequence(103))),
            codec=CodecProfile.raw(),
        )


def test_writer_rejects_wrong_previous_prefix_root() -> None:
    first, second, _ = _frames()
    bad_second = replace(second, previous_prefix_root=Hash32(b"\x99" * 32))

    with pytest.raises(ValueError, match="previous prefix root"):
        build_segment((first, bad_second), codec=CodecProfile.raw())


def test_writer_rejects_mixed_run_ids() -> None:
    first, second, _ = _frames()
    bad_second = replace(
        second,
        run_id=RunId("SYNTHETIC_STORAGE_V4_WORKLOAD/another-run"),
    )

    with pytest.raises(ValueError, match="single run"):
        build_segment((first, bad_second), codec=CodecProfile.raw())


def test_reader_applies_explicit_decode_limits_before_large_work() -> None:
    artifact = _artifact()

    with pytest.raises(SegmentFormatError, match="physical size exceeds"):
        read_segment(
            artifact.data,
            limits=SegmentReadLimits(max_physical_size=artifact.physical_size - 1),
        )
    with pytest.raises(SegmentFormatError, match="logical size exceeds"):
        read_segment(
            artifact.data,
            limits=SegmentReadLimits(max_logical_size=artifact.logical_size - 1),
        )
    with pytest.raises(SegmentFormatError, match="block count exceeds"):
        read_segment(artifact.data, limits=SegmentReadLimits(max_blocks=1))
    with pytest.raises(SegmentFormatError, match="commit count exceeds"):
        read_segment(artifact.data, limits=SegmentReadLimits(max_commits=2))


def test_reader_reaches_zlib_validation_after_reauthenticated_envelope() -> None:
    artifact = _artifact()
    block = artifact.layout.blocks[0]
    changed = bytearray(artifact.data)
    replacement = zlib.compress(b"", level=9)
    assert len(replacement) < block.payload_size
    replacement += b"\x00" * (block.payload_size - len(replacement))
    changed[block.payload_offset : block.payload_offset + block.payload_size] = replacement
    changed[
        block.payload_sha256_offset : block.payload_sha256_offset + 32
    ] = hashlib.sha256(replacement).digest()
    reauthenticated = _refresh_physical_envelope(changed, artifact)

    with pytest.raises(SegmentFormatError, match=r"zlib|decoded block"):
        read_segment(reauthenticated)


def test_reader_rejects_reauthenticated_footer_index_count_mismatch() -> None:
    artifact = _artifact()
    changed = bytearray(artifact.data)
    index_count_offset = artifact.layout.footer_offset + 96
    changed[index_count_offset : index_count_offset + 4] = (
        artifact.block_count + 1
    ).to_bytes(4, "big")
    reauthenticated = _refresh_physical_envelope(changed, artifact)

    with pytest.raises(SegmentFormatError, match=r"footer|index"):
        read_segment(reauthenticated)


def test_reader_rejects_block_commit_count_above_segment_total_before_decode() -> None:
    artifact = _artifact()
    changed = bytearray(artifact.data)
    block_commit_count_offset = artifact.layout.blocks[0].offset + 24
    block_last_sequence_offset = artifact.layout.blocks[0].offset + 16
    forged_commit_count = artifact.commit_count + 1
    forged_last_sequence = (
        int(artifact.blocks[0].first_commit_sequence) + forged_commit_count - 1
    )
    changed[block_last_sequence_offset : block_last_sequence_offset + 8] = (
        forged_last_sequence.to_bytes(8, "big")
    )
    changed[block_commit_count_offset : block_commit_count_offset + 4] = (
        forged_commit_count
    ).to_bytes(4, "big")
    reauthenticated = _refresh_physical_envelope(changed, artifact)

    with pytest.raises(SegmentFormatError, match="block commit count exceeds segment header"):
        read_segment(reauthenticated)


def test_reader_rejects_cumulative_block_logical_size_before_decode() -> None:
    artifact = _artifact()
    changed = bytearray(artifact.data)
    block_logical_size_offset = artifact.layout.blocks[0].offset + 28
    changed[block_logical_size_offset : block_logical_size_offset + 8] = (
        artifact.logical_size + 1
    ).to_bytes(8, "big")
    reauthenticated = _refresh_physical_envelope(changed, artifact)

    with pytest.raises(SegmentFormatError, match="block logical sizes exceed segment total"):
        read_segment(reauthenticated)


def test_reader_rejects_reauthenticated_logical_identity_mismatch() -> None:
    artifact = _artifact()
    changed = bytearray(artifact.data)
    run_size = len(artifact.run_id.value.encode())
    identity_offset = (
        artifact.layout.header_offset
        + 2
        + 4
        + run_size
        + 8
        + 8
        + 32
        + 32
        + 32
    )
    changed[identity_offset] ^= 0x01
    reauthenticated = _refresh_physical_envelope(
        changed,
        artifact,
        header_changed=True,
    )

    with pytest.raises(SegmentFormatError, match="logical identity"):
        read_segment(reauthenticated)
