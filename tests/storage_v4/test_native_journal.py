from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from hyperlab.paper.storage_v4.canonical import (
    build_commit_logical,
    canonical_json_bytes,
    frame_bytes,
    frame_hash32,
    frame_text,
    frame_u32,
    frame_u64,
    framed_hash,
)
from hyperlab.paper.storage_v4.checkpoint import CheckpointState
from hyperlab.paper.storage_v4.contracts import CompatibilityRecord, RawLakeId
from hyperlab.paper.storage_v4.native_journal import (
    NATIVE_RAW_REFERENCE_PREFIX_DOMAIN,
    NativeAuditExpectations,
    NativeCheckpointBinding,
    NativeJournalError,
    NativeJournalErrorCode,
    NativeStreamExpectation,
    advance_native_raw_reference_prefix,
    audit_native_frames,
    bind_native_checkpoint_state,
    native_raw_reference_prefix_seed,
    rechain_native_frames,
    rematerialize_native_row,
    unbind_native_checkpoint_state,
)
from hyperlab.paper.storage_v4.raw_reference import (
    DeterministicRawLakeV2Emulator,
    RawSegmentRef,
)
from hyperlab.paper.storage_v4.raw_store import RawStoreError, RawStoreErrorCode
from hyperlab.paper.storage_v4.types import (
    CommitFrame,
    CommitOrdinal,
    CommitSequence,
    EventSequence,
    Hash32,
    LogicalRow,
    RunId,
    SegmentIdentity,
    StoreId,
    StreamId,
)

_RUN = RunId("synthetic-native-journal-run")
_LAKE = RawLakeId("synthetic-native-lake")
_STORE = StoreId("synthetic-native-store")


def _sha(value: bytes) -> Hash32:
    return Hash32(hashlib.sha256(value).digest())


def _jsonl(value: dict[str, object]) -> bytes:
    return canonical_json_bytes(value) + b"\n"


def _compat_row(stream: str, ordinal: int, value: dict[str, object]) -> LogicalRow:
    return CompatibilityRecord.from_jsonl_bytes(_jsonl(value)).to_logical_row(
        StreamId(stream),
        CommitOrdinal(ordinal),
    )


def _source_frames() -> tuple[CommitFrame, ...]:
    previous = _sha(b"golden-export-root")
    frames = []
    for sequence in range(1, 4):
        rows = [
            _compat_row(
                "inbox",
                0,
                {
                    "arrival_sequence": sequence,
                    "commit_sequence": sequence,
                    "input_id": f"input-{sequence}",
                    "payload": {"type": "PUBLIC" if sequence != 2 else "TIMER"},
                    "run_id": _RUN.value,
                },
            ),
            LogicalRow(
                StreamId("projection_history"),
                CommitOrdinal(0),
                {"revision": sequence, "run_id": _RUN.value},
            ),
        ]
        if sequence == 1:
            rows.append(
                _compat_row(
                    "events",
                    0,
                    {"event": "OPEN", "run_id": _RUN.value, "sequence": 1},
                )
            )
        if sequence == 2:
            rows.append(
                LogicalRow(
                    StreamId("alerts"),
                    CommitOrdinal(0),
                    {"code": "MARKET_GAP", "run_id": _RUN.value},
                )
            )
        frame = CommitFrame(
            run_id=_RUN,
            commit_sequence=CommitSequence(sequence),
            previous_prefix_root=previous,
            rows=tuple(rows),
            legacy_v3_identity=_sha(f"legacy-{sequence}".encode()),
        )
        previous = build_commit_logical(frame).prefix_root
        frames.append(frame)
    return tuple(frames)


def _raw_reference(
    line: bytes,
    *,
    sequence: int,
    manifest_root: Hash32,
) -> tuple[RawSegmentRef, bytes]:
    segment = b"prefix|" + line + b"|suffix"
    offset = len(b"prefix|")
    return (
        RawSegmentRef(
            raw_store_id=_STORE,
            lake_id=_LAKE,
            source_id="synthetic-public-source",
            venue_id="SYNTHETIC",
            segment_identity=SegmentIdentity(_sha(f"segment-{sequence}".encode())),
            segment_root=_sha(f"segment-root-{sequence}".encode()),
            raw_manifest_root=manifest_root,
            physical_sha256=_sha(segment),
            record_id=f"input-{sequence}",
            byte_offset=offset,
            stored_length=len(line),
            stored_sha256=_sha(line),
            logical_payload_length=len(line),
            logical_payload_sha256=_sha(line),
            input_type="PUBLIC_MARKET_DATA",
            source_stream_id=StreamId("public-wire"),
            source_first_sequence=EventSequence(sequence),
            source_last_sequence=EventSequence(sequence),
            arrival_sequence=EventSequence(sequence),
            source_timestamp=f"2026-08-25T12:00:0{sequence}Z",
            received_timestamp=f"2026-08-25T12:00:0{sequence}Z",
            codec_id="raw",
            codec_version="1",
        ),
        segment,
    )


def _native_fixture() -> tuple[
    tuple[CommitFrame, ...],
    DeterministicRawLakeV2Emulator,
    tuple[RawSegmentRef, ...],
    Hash32,
]:
    source = _source_frames()
    manifest_root = _sha(b"raw-manifest-root")
    emulator = DeterministicRawLakeV2Emulator()
    references = []
    for sequence in (1, 3):
        inbox = next(row for row in source[sequence - 1].rows if row.stream_id.value == "inbox")
        line = rematerialize_native_row(inbox, emulator)
        reference, segment = _raw_reference(
            line,
            sequence=sequence,
            manifest_root=manifest_root,
        )
        emulator.register_v2(reference, segment)
        references.append(reference)
    native = tuple(
        rechain_native_frames(
            iter(source),
            {1: references[0], 3: references[1]},
            emulator,
            start_prefix_root=source[0].previous_prefix_root,
        )
    )
    return native, emulator, tuple(references), manifest_root


def _checkpoint_state() -> CheckpointState:
    return CheckpointState(
        adapter={"paper_cursor": {"input_id": "input-3"}},
        ledger={"cash": "100.00"},
        projection={"revision": 3},
        sessions={"closed": []},
        incidents={"resolved": ["MARKET_GAP"]},
        cursors={"public": "cursor-3"},
        stream_heads={"events": 1},
    )


def _reference_prefix(references: tuple[RawSegmentRef, ...]) -> Hash32:
    prefix = framed_hash(NATIVE_RAW_REFERENCE_PREFIX_DOMAIN, b"")
    for reference in references:
        prefix = framed_hash(
            NATIVE_RAW_REFERENCE_PREFIX_DOMAIN,
            frame_hash32(prefix),
            frame_u64(int(reference.arrival_sequence)),
            frame_text("inbox"),
            frame_u32(0),
            frame_bytes(canonical_json_bytes(reference.canonical_value())),
        )
    return prefix


def test_checkpoint_binding_roundtrips_and_preserves_paper_adapter() -> None:
    state = _checkpoint_state()
    binding = NativeCheckpointBinding(
        raw_store_id=_STORE,
        raw_lake_id=_LAKE,
        raw_config_identity=_sha(b"raw-config"),
        raw_generation=4,
        raw_manifest_root=_sha(b"manifest"),
        raw_record_count=3,
        raw_last_record_id="input-3",
        raw_reference_prefix_root=_sha(b"reference-prefix"),
    )

    bound = bind_native_checkpoint_state(state, binding)
    restored, decoded = unbind_native_checkpoint_state(bound, expected_binding=binding)

    assert decoded == binding
    assert restored == state
    assert bound.adapter["paper_adapter"] == state.adapter
    assert bind_native_checkpoint_state(state, binding) == bound

    with pytest.raises(NativeJournalError) as caught:
        unbind_native_checkpoint_state(
            bound,
            expected_binding=replace(binding, raw_record_count=4),
        )
    assert caught.value.code is NativeJournalErrorCode.CHECKPOINT_BINDING_MISMATCH


def test_rematerializes_raw_compatibility_and_direct_rows_as_exact_jsonl() -> None:
    native, emulator, references, _ = _native_fixture()
    raw_row = next(row for row in native[0].rows if row.stream_id.value == "inbox")
    compatibility_row = next(row for row in native[0].rows if row.stream_id.value == "events")
    direct_row = next(
        row for row in native[0].rows if row.stream_id.value == "projection_history"
    )

    assert rematerialize_native_row(raw_row, emulator) == _jsonl(
        {
            "arrival_sequence": 1,
            "commit_sequence": 1,
            "input_id": references[0].record_id,
            "payload": {"type": "PUBLIC"},
            "run_id": _RUN.value,
        }
    )
    assert rematerialize_native_row(compatibility_row, emulator) == _jsonl(
        {"event": "OPEN", "run_id": _RUN.value, "sequence": 1}
    )
    assert rematerialize_native_row(direct_row, emulator) == (
        direct_row.canonical_bytes + b"\n"
    )


def test_durable_resolver_failure_is_normalized_to_native_error() -> None:
    native, _, _, _ = _native_fixture()
    raw_row = next(row for row in native[0].rows if row.stream_id.value == "inbox")

    class MissingResolver:
        def resolve(self, reference: RawSegmentRef) -> bytes:
            raise RawStoreError(RawStoreErrorCode.SEGMENT_MISSING, "synthetic missing raw")

    with pytest.raises(NativeJournalError) as caught:
        rematerialize_native_row(raw_row, MissingResolver())
    assert caught.value.code is NativeJournalErrorCode.RAW_REFERENCE_UNRESOLVED


def test_rechains_across_streamed_batches_and_rejects_duplicate_or_orphan_refs() -> None:
    source = _source_frames()
    manifest_root = _sha(b"raw-manifest-root")
    emulator = DeterministicRawLakeV2Emulator()
    references = []
    for sequence in (1, 3):
        inbox = next(row for row in source[sequence - 1].rows if row.stream_id.value == "inbox")
        reference, segment = _raw_reference(
            rematerialize_native_row(inbox, emulator),
            sequence=sequence,
            manifest_root=manifest_root,
        )
        emulator.register_v2(reference, segment)
        references.append(reference)

    def batches():
        yield from source[:1]
        yield from source[1:]

    native = tuple(
        rechain_native_frames(
            batches(),
            {1: references[0], 3: references[1]},
            emulator,
            start_prefix_root=source[0].previous_prefix_root,
        )
    )
    assert [int(frame.commit_sequence) for frame in native] == [1, 2, 3]
    assert native[1].previous_prefix_root == build_commit_logical(native[0]).prefix_root
    assert native[2].previous_prefix_root == build_commit_logical(native[1]).prefix_root

    duplicate = replace(references[0], arrival_sequence=EventSequence(3))
    with pytest.raises(NativeJournalError) as duplicate_error:
        tuple(
            rechain_native_frames(
                source,
                {1: references[0], 3: duplicate},
                emulator,
                start_prefix_root=source[0].previous_prefix_root,
            )
        )
    assert duplicate_error.value.code is NativeJournalErrorCode.DUPLICATE_RECORD_REFERENCE

    with pytest.raises(NativeJournalError) as orphan_error:
        tuple(
            rechain_native_frames(
                source,
                {99: references[0]},
                emulator,
                start_prefix_root=source[0].previous_prefix_root,
            )
        )
    assert orphan_error.value.code is NativeJournalErrorCode.ORPHAN_REFERENCE

    with pytest.raises(NativeJournalError) as arrival_error:
        tuple(
            rechain_native_frames(
                source,
                {1: replace(references[0], arrival_sequence=EventSequence(2))},
                emulator,
                start_prefix_root=source[0].previous_prefix_root,
            )
        )
    assert arrival_error.value.code is NativeJournalErrorCode.ARRIVAL_MISMATCH


def test_streaming_audit_matches_exact_counts_hashes_market_gap_and_raw_bindings() -> None:
    native, emulator, references, manifest_root = _native_fixture()
    by_stream: dict[str, list[bytes]] = {}
    for frame in native:
        for row in frame.rows:
            by_stream.setdefault(row.stream_id.value, []).append(
                rematerialize_native_row(row, emulator)
            )
    streams = tuple(
        NativeStreamExpectation(
            stream_id=StreamId(name),
            row_count=len(lines),
            logical_sha256=_sha(b"".join(lines)),
        )
        for name, lines in sorted(by_stream.items())
    )
    expected_reference_prefix = _reference_prefix(references)
    assert expected_reference_prefix == advance_native_raw_reference_prefix(
        advance_native_raw_reference_prefix(
            native_raw_reference_prefix_seed(),
            references[0],
            StreamId("inbox"),
            CommitOrdinal(0),
        ),
        references[1],
        StreamId("inbox"),
        CommitOrdinal(0),
    )
    expectations = NativeAuditExpectations(
        run_id=_RUN,
        start_prefix_root=native[0].previous_prefix_root,
        commit_count=3,
        final_prefix_root=build_commit_logical(native[-1]).prefix_root,
        streams=streams,
        market_gap_count=1,
        raw_reference_count=2,
        raw_manifest_roots=(manifest_root,),
        raw_last_record_id="input-3",
        raw_reference_prefix_root=expected_reference_prefix,
    )

    baseline = audit_native_frames(iter(native), emulator, expectations)
    progress: list[dict[str, object]] = []
    report = audit_native_frames(
        iter(native),
        emulator,
        expectations,
        progress=lambda payload: progress.append(dict(payload)),
    )

    assert report == baseline
    assert report.commit_count == 3
    assert report.raw_reference_count == 2
    assert report.market_gap_count == 1
    assert report.streams == streams
    assert report.raw_manifest_roots == (manifest_root,)
    assert report.final_prefix_root == expectations.final_prefix_root
    assert [item["audit_event"] for item in progress] == ["STARTED", "COMPLETE"]
    assert [item["audited_commits"] for item in progress] == [0, 3]
    assert [item["audited_rows"] for item in progress] == [
        0,
        sum(item.row_count for item in streams),
    ]
    assert all(
        item["audit_progress_authority"] == "NON_AUTHORITATIVE_OBSERVABILITY_ONLY"
        for item in progress
    )


def test_streaming_audit_catches_prefix_and_manifest_divergence() -> None:
    native, emulator, references, manifest_root = _native_fixture()
    streams: dict[str, list[bytes]] = {}
    for frame in native:
        for row in frame.rows:
            streams.setdefault(row.stream_id.value, []).append(
                rematerialize_native_row(row, emulator)
            )
    expectations = NativeAuditExpectations(
        run_id=_RUN,
        start_prefix_root=native[0].previous_prefix_root,
        commit_count=3,
        final_prefix_root=build_commit_logical(native[-1]).prefix_root,
        streams=tuple(
            NativeStreamExpectation(StreamId(name), len(lines), _sha(b"".join(lines)))
            for name, lines in sorted(streams.items())
        ),
        market_gap_count=1,
        raw_reference_count=2,
        raw_manifest_roots=(manifest_root,),
        raw_last_record_id="input-3",
        raw_reference_prefix_root=_reference_prefix(references),
    )
    broken = list(native)
    broken[1] = replace(broken[1], previous_prefix_root=_sha(b"wrong-prefix"))

    with pytest.raises(NativeJournalError) as prefix_error:
        audit_native_frames(iter(broken), emulator, expectations)
    assert prefix_error.value.code is NativeJournalErrorCode.PREFIX_DIVERGENCE

    with pytest.raises(NativeJournalError) as manifest_error:
        audit_native_frames(
            iter(native),
            emulator,
            replace(expectations, raw_manifest_roots=(_sha(b"wrong-manifest"),)),
        )
    assert manifest_error.value.code is NativeJournalErrorCode.ORPHAN_REFERENCE
