from __future__ import annotations

from hyperlab.paper.storage_v4.canonical import build_commit_logical
from hyperlab.paper.storage_v4.segment import (
    CodecProfile,
    build_segment,
    logical_frame_size,
    read_segment,
)
from hyperlab.paper.storage_v4.types import (
    CommitFrame,
    CommitOrdinal,
    CommitSequence,
    Hash32,
    LocalCount,
    LogicalRow,
    RunId,
    StreamId,
)

SYNTHETIC_STORAGE_V4_WORKLOAD = True
_RUN_ID = RunId("SYNTHETIC_STORAGE_V4_WORKLOAD/run-roundtrip")
_RAW_SINGLE_COMMIT_GOLDEN_HEX = """
484c3453454700010001000100000000000000b20000000100000000000000d20000000000000294000100000006676f
6c64656e0000000000000001000000000000000100000000000000000000000000000000000000000000000000000000
000000005aa2cd21fe541a69f6004cab73bcdf537a2b7fef8feedf859a47579adb4d3471114ff27d28007310454d0e8f
ecc070e602062266e678f511868b8e7689453569d28fd87e8cc1f063661c012f271a83bc92f1b6386cf17250df5227d5
2155d33a0000000100000001000000066576656e747300000001484c3442000000000000000000000001000000000000
00010000000100000000000000d200000000000000d297f1dfdcaeeda0a4e351a9aed72627905651c485b064d3d019ec
1b1c5b2b5e9f484c3443000100000000000000c400000000000000010000000000000000000000000000000000000000
0000000000000000000000000000000001000000066576656e74730000000100000001000000066576656e7473000000
001273c28c1d5231ec37c005ee5947deb91b442ea21c96a0ded34fb8ab4e758bc6000000137b2261223a312c22746578
74223a22c3a9227d4046937073a78abdbf67aab1e74dc259de8d4d5a33233dc08cb0be342d83ed9b5aa2cd21fe541a69
f6004cab73bcdf537a2b7fef8feedf859a47579adb4d3471484c344654520001000100000000000100000000000000d2
0000000000000294a110c75705819841c6fda2a4a94936ecc3ed4c7f9923b61b789e37dd62e849b22927030f21797a14
27be39ad8fefee3224839e18b72fe1fb94d9f248f3bdc8e60000000100000000000000daa69341c03a968fd8d2e88675
bc52b38d5bb4a5b3659405458ca5ff50517f4f45000000000000009c484c34454e440001
"""


def _row(stream: str, ordinal: int, marker: str, *, padding: str = "") -> LogicalRow:
    return LogicalRow(
        stream_id=StreamId(stream),
        ordinal=CommitOrdinal(ordinal),
        value={"marker": marker, "padding": padding},
    )


def _frames() -> tuple[CommitFrame, ...]:
    previous = Hash32(b"\x00" * 32)
    frames: list[CommitFrame] = []
    rows_by_commit = (
        (
            _row("inputs", 0, "input-1"),
            _row("events", 0, "event-1"),
        ),
        (_row("ledger_transactions", 0, "transaction-2"),),
        (
            _row("ledger_entries", 0, "entry-3"),
            _row("alerts", 0, "alert-3"),
            _row("projection_deltas", 0, "projection-3"),
        ),
        (_row("events", 0, "event-4", padding="x" * 320),),
    )
    for sequence, rows in enumerate(rows_by_commit, start=41):
        frame = CommitFrame(
            run_id=_RUN_ID,
            commit_sequence=CommitSequence(sequence),
            previous_prefix_root=previous,
            rows=rows,
        )
        frames.append(frame)
        previous = build_commit_logical(frame).prefix_root
    return tuple(frames)


def test_raw_single_commit_matches_frozen_protocol_v1_file_vector() -> None:
    row = LogicalRow(
        StreamId("events"),
        CommitOrdinal(0),
        {"a": 1, "text": "é"},
    )
    frame = CommitFrame(
        RunId("golden"),
        CommitSequence(1),
        Hash32(bytes(32)),
        (row,),
    )
    artifact = build_segment(
        (frame,),
        codec=CodecProfile.raw(),
        max_commits_per_block=1,
        max_logical_bytes_per_block=4096,
    )
    expected = bytes.fromhex(_RAW_SINGLE_COMMIT_GOLDEN_HEX)

    assert artifact.data == expected
    assert artifact.identity.digest == Hash32.from_hex(
        "d28fd87e8cc1f063661c012f271a83bc92f1b6386cf17250df5227d52155d33a"
    )
    assert artifact.physical_sha256 == Hash32.from_hex(
        "fc4c2a8d1964ac1d6001582337a66fc381f9fcd5cb004cb503c2534c3d7cb671"
    )
    assert len(expected) == 660
    assert artifact.layout.header_offset == 40
    assert artifact.layout.header_size == 178
    assert artifact.layout.blocks[0].offset == 218
    assert artifact.layout.blocks[0].payload_offset == 294
    assert artifact.layout.blocks[0].payload_size == 210
    assert artifact.layout.footer_offset == 504
    assert artifact.layout.footer_size == 156
    assert read_segment(expected).commits == (frame,)


def test_segment_roundtrip_preserves_exact_logical_commits_and_counts() -> None:
    frames = _frames()

    artifact = build_segment(
        frames,
        codec=CodecProfile.raw(),
        max_commits_per_block=2,
        max_logical_bytes_per_block=10_000,
    )
    verified = read_segment(artifact.data)

    assert verified.commits == frames
    assert verified.identity == artifact.identity
    assert verified.physical_sha256 == artifact.physical_sha256
    assert verified.first_commit_sequence == CommitSequence(41)
    assert verified.last_commit_sequence == CommitSequence(44)
    assert verified.block_count == 2
    assert tuple(int(block.commit_count) for block in verified.blocks) == (2, 2)
    assert verified.counts_by_stream == (
        (StreamId("alerts"), LocalCount(1)),
        (StreamId("events"), LocalCount(2)),
        (StreamId("inputs"), LocalCount(1)),
        (StreamId("ledger_entries"), LocalCount(1)),
        (StreamId("ledger_transactions"), LocalCount(1)),
        (StreamId("projection_deltas"), LocalCount(1)),
    )


def test_commit_exactly_at_byte_boundary_is_not_split() -> None:
    frames = _frames()[:2]
    first_size = logical_frame_size(frames[0])

    artifact = build_segment(
        frames,
        codec=CodecProfile.raw(),
        max_commits_per_block=10,
        max_logical_bytes_per_block=first_size,
    )
    verified = read_segment(artifact.data)

    assert verified.blocks[0].logical_size == first_size
    assert int(verified.blocks[0].commit_count) == 1
    assert int(verified.blocks[1].commit_count) == 1


def test_oversized_commit_occupies_one_whole_block_without_splitting() -> None:
    frame = _frames()[-1]
    frame_size = logical_frame_size(frame)

    artifact = build_segment(
        (frame,),
        codec=CodecProfile.raw(),
        max_commits_per_block=10,
        max_logical_bytes_per_block=frame_size - 1,
    )
    verified = read_segment(artifact.data)

    assert verified.block_count == 1
    assert int(verified.blocks[0].commit_count) == 1
    assert verified.blocks[0].logical_size == frame_size
    assert verified.blocks[0].logical_size > frame_size - 1


def test_logical_identity_is_invariant_across_codec_level_and_blocking() -> None:
    frames = _frames()
    raw_wide = build_segment(
        frames,
        codec=CodecProfile.raw(),
        max_commits_per_block=10,
        max_logical_bytes_per_block=100_000,
    )
    raw_narrow = build_segment(
        frames,
        codec=CodecProfile.raw(),
        max_commits_per_block=1,
        max_logical_bytes_per_block=100,
    )
    zlib_low = build_segment(
        frames,
        codec=CodecProfile.zlib(level=1),
        max_commits_per_block=2,
        max_logical_bytes_per_block=1_000,
    )
    zlib_high = build_segment(
        frames,
        codec=CodecProfile.zlib(level=9),
        max_commits_per_block=3,
        max_logical_bytes_per_block=2_000,
    )

    artifacts = (raw_wide, raw_narrow, zlib_low, zlib_high)
    assert len({artifact.identity for artifact in artifacts}) == 1
    assert len({artifact.merkle_root for artifact in artifacts}) == 1
    assert len({artifact.end_prefix_root for artifact in artifacts}) == 1
    assert len({artifact.commit_digests for artifact in artifacts}) == 1
    assert len({artifact.physical_sha256 for artifact in artifacts}) == 4
    for artifact in artifacts:
        assert read_segment(artifact.data).commits == frames
