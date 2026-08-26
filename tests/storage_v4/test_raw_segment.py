from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from hyperlab.paper.storage_v4 import raw_segment as raw_segment_module
from hyperlab.paper.storage_v4.contracts import RawLakeId
from hyperlab.paper.storage_v4.raw_segment import (
    RawRecordMetadata,
    RawSegmentError,
    RawSegmentErrorCode,
    RawSegmentThresholds,
    RawSegmentWriter,
    raw_footer_index_physical_bytes,
    read_raw_payload,
    verify_raw_segment,
)
from hyperlab.paper.storage_v4.segment import CodecProfile
from hyperlab.paper.storage_v4.types import UINT32_MAX, EventSequence, StreamId


def _metadata(sequence: int) -> RawRecordMetadata:
    return RawRecordMetadata(
        record_id=f"input-{sequence}",
        source_id="synthetic-public-source",
        venue_id="SYNTHETIC",
        input_type="PUBLIC_MARKET_EVENT",
        source_stream_id=StreamId("public-market-wire"),
        source_first_sequence=EventSequence(sequence * 10),
        source_last_sequence=EventSequence(sequence * 10),
        arrival_sequence=EventSequence(sequence),
        source_timestamp=f"2026-01-01T00:00:{sequence:02d}Z",
        received_timestamp=f"2026-01-01T00:00:{sequence:02d}Z",
    )


@pytest.mark.parametrize("codec", [CodecProfile.raw(), CodecProfile.zlib(level=6)])
def test_streaming_segment_roundtrip_is_deterministic_and_range_resolvable(
    tmp_path: Path,
    codec: CodecProfile,
) -> None:
    payloads = (b'{"input_type":"PUBLIC_MARKET_EVENT","n":1}', b'{"n":2}')

    artifacts = []
    for directory_name in ("one", "two"):
        directory = tmp_path / directory_name
        directory.mkdir()
        writer = RawSegmentWriter(
            directory,
            lake_id=RawLakeId("synthetic-lake"),
            codec_profile=codec,
        )
        for sequence, payload in enumerate(payloads, start=1):
            writer.append(payload, _metadata(sequence))
        artifacts.append(writer.seal())

    first, second = artifacts
    assert first.path.read_bytes() == second.path.read_bytes()
    assert first.physical_sha256 == second.physical_sha256
    assert first.segment_identity == second.segment_identity
    assert first.segment_root == second.segment_root
    assert first.record_count == 2
    assert first.logical_payload_bytes == sum(map(len, payloads))
    footer_bytes = raw_footer_index_physical_bytes(first.record_count)
    assert footer_bytes == 146 + 152 * first.record_count
    assert footer_bytes == int.from_bytes(first.path.read_bytes()[-16:-8], "big")
    assert footer_bytes < first.physical_size

    verified = verify_raw_segment(first.path)
    assert verified == first.summary
    assert tuple(
        read_raw_payload(first.path, locator, expected_lake_id=RawLakeId("synthetic-lake"))
        for locator in first.records
    ) == payloads


def test_footer_index_physical_bytes_are_strictly_bounded() -> None:
    assert raw_footer_index_physical_bytes(1) == 298

    for invalid in (None, True, 1.0):
        with pytest.raises(RawSegmentError) as caught:
            raw_footer_index_physical_bytes(invalid)  # type: ignore[arg-type]
        assert caught.value.code is RawSegmentErrorCode.TYPE
    for invalid in (0, -1, UINT32_MAX + 1):
        with pytest.raises(RawSegmentError) as caught:
            raw_footer_index_physical_bytes(invalid)
        assert caught.value.code is RawSegmentErrorCode.LIMIT


def test_segment_thresholds_are_bounded_before_a_second_record_is_written(
    tmp_path: Path,
) -> None:
    thresholds = RawSegmentThresholds(
        max_records=1,
        max_logical_payload_bytes=1024,
        max_physical_bytes=4096,
        max_single_payload_bytes=1024,
    )
    writer = RawSegmentWriter(
        tmp_path,
        lake_id=RawLakeId("synthetic-lake"),
        codec_profile=CodecProfile.raw(),
        thresholds=thresholds,
    )
    writer.append(b"first", _metadata(1))

    with pytest.raises(RawSegmentError) as caught:
        writer.append(b"second", _metadata(2))

    assert caught.value.code is RawSegmentErrorCode.THRESHOLD_REACHED
    artifact = writer.seal()
    assert artifact.record_count == 1


def test_writer_accepts_and_resolves_the_exact_single_payload_boundary(
    tmp_path: Path,
) -> None:
    thresholds = RawSegmentThresholds()
    payload = bytes(thresholds.max_single_payload_bytes)
    writer = RawSegmentWriter(
        tmp_path,
        lake_id=RawLakeId("synthetic-lake"),
        codec_profile=CodecProfile.zlib(level=6),
        thresholds=thresholds,
    )

    locator = writer.append(payload, _metadata(1))
    artifact = writer.seal()

    assert locator.logical_payload_length == 32 * 1024 * 1024
    assert artifact.logical_payload_bytes == len(payload)
    assert read_raw_payload(
        artifact.path,
        locator,
        expected_lake_id=RawLakeId("synthetic-lake"),
    ) == payload


def test_duplicate_or_reordered_record_identity_is_refused(tmp_path: Path) -> None:
    writer = RawSegmentWriter(tmp_path, lake_id=RawLakeId("synthetic-lake"))
    writer.append(b"one", _metadata(1))

    with pytest.raises(RawSegmentError) as duplicate:
        writer.append(b"changed", _metadata(1))
    assert duplicate.value.code is RawSegmentErrorCode.DUPLICATE_RECORD

    with pytest.raises(RawSegmentError) as reordered:
        writer.append(b"zero", replace(_metadata(2), arrival_sequence=EventSequence(0)))
    assert reordered.value.code is RawSegmentErrorCode.RECORD_ORDER


@pytest.mark.parametrize("mutation", ["truncate", "stored", "footer"])
def test_segment_verification_fails_closed_on_truncation_or_corruption(
    tmp_path: Path,
    mutation: str,
) -> None:
    writer = RawSegmentWriter(
        tmp_path,
        lake_id=RawLakeId("synthetic-lake"),
        codec_profile=CodecProfile.zlib(level=6),
    )
    writer.append(b'{"payload":"authenticated"}', _metadata(1))
    artifact = writer.seal()
    damaged = bytearray(artifact.path.read_bytes())
    if mutation == "truncate":
        damaged.pop()
    elif mutation == "stored":
        damaged[artifact.records[0].byte_offset] ^= 0x01
    else:
        damaged[-49] ^= 0x01
    artifact.path.write_bytes(damaged)

    with pytest.raises(RawSegmentError) as caught:
        verify_raw_segment(artifact.path)
    assert caught.value.code in {
        RawSegmentErrorCode.CORRUPT,
        RawSegmentErrorCode.TRUNCATED,
        RawSegmentErrorCode.HASH_MISMATCH,
    }


def test_range_reader_checks_stored_and_logical_hashes_independently(
    tmp_path: Path,
) -> None:
    writer = RawSegmentWriter(
        tmp_path,
        lake_id=RawLakeId("synthetic-lake"),
        codec_profile=CodecProfile.zlib(level=6),
    )
    writer.append(b"payload", _metadata(1))
    artifact = writer.seal()
    locator = artifact.records[0]

    with pytest.raises(RawSegmentError) as stored:
        read_raw_payload(
            artifact.path,
            replace(locator, stored_sha256=type(locator.stored_sha256)(b"\x01" * 32)),
            expected_lake_id=RawLakeId("synthetic-lake"),
        )
    assert stored.value.code is RawSegmentErrorCode.HASH_MISMATCH

    with pytest.raises(RawSegmentError) as logical:
        read_raw_payload(
            artifact.path,
            replace(
                locator,
                logical_payload_sha256=type(locator.logical_payload_sha256)(
                    hashlib.sha256(b"other").digest()
                ),
            ),
            expected_lake_id=RawLakeId("synthetic-lake"),
        )
    assert logical.value.code is RawSegmentErrorCode.PAYLOAD_MISMATCH


def test_range_reader_refuses_a_link_swapped_between_check_and_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = RawSegmentWriter(
        tmp_path,
        lake_id=RawLakeId("synthetic-lake"),
        codec_profile=CodecProfile.raw(),
    )
    writer.append(b"payload", _metadata(1))
    artifact = writer.seal()
    original = tmp_path / "original.hl4r"
    outside = tmp_path / "outside.hl4r"
    outside.write_bytes(artifact.path.read_bytes())
    original_open = raw_segment_module.os.open
    swapped = False

    def swap_then_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        nonlocal swapped
        if Path(path) == artifact.path and not swapped:
            swapped = True
            artifact.path.replace(original)
            try:
                artifact.path.symlink_to(outside)
            except (NotImplementedError, OSError) as error:
                pytest.skip(f"file symlinks are unavailable in this environment: {error}")
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(raw_segment_module.os, "open", swap_then_open)

    with pytest.raises(RawSegmentError) as caught:
        read_raw_payload(
            artifact.path,
            artifact.records[0],
            expected_lake_id=RawLakeId("synthetic-lake"),
        )
    assert swapped is True
    assert caught.value.code is RawSegmentErrorCode.CORRUPT
