from __future__ import annotations

import hashlib
from decimal import Decimal

import pytest

import hyperlab.paper.storage_v4.canonical as canonical_module
from hyperlab.paper.storage_v4.canonical import (
    DOMAIN_COMMIT,
    DOMAIN_PREFIX,
    DOMAIN_ROW,
    PROTOCOL_VERSION,
    CanonicalizationError,
    build_commit_logical,
    canonical_json_bytes,
    commit_digest,
    commit_preimage,
    frame_bytes,
    frame_domain,
    frame_optional_bytes,
    frame_optional_hash32,
    frame_text,
    frame_u32,
    frame_u64,
    framed_hash,
    framed_preimage,
    prefix_preimage,
    row_hash,
    row_preimage,
    verify_commit_logical,
)
from hyperlab.paper.storage_v4.types import (
    AlertLogical,
    CommitFrame,
    CommitLogical,
    CommitOrdinal,
    CommitSequence,
    EventLogical,
    EventSequence,
    Hash32,
    InputLogical,
    LedgerEntryLogical,
    LedgerTransactionLogical,
    LocalCount,
    LogicalRow,
    ManifestIdentity,
    ProjectionDeltaLogical,
    RunId,
    SegmentIdentity,
    StoreId,
    StreamId,
)

SYNTHETIC_STORAGE_V4_WORKLOAD = "SYNTHETIC_STORAGE_V4_WORKLOAD"


def _hash(byte: int) -> Hash32:
    return Hash32(bytes([byte]) * 32)


def _frame(*rows: LogicalRow, legacy: Hash32 | None = None) -> CommitFrame:
    return CommitFrame(
        run_id=RunId("synthetic-run"),
        commit_sequence=CommitSequence(7),
        previous_prefix_root=_hash(1),
        rows=tuple(rows),
        legacy_v3_identity=legacy,
    )


def test_hash32_is_exact_immutable_bytes_and_hex_is_only_a_view() -> None:
    raw = bytes(range(32))
    digest = Hash32(raw)

    assert digest.value is raw
    assert bytes(digest) == raw
    assert digest.hex() == raw.hex()
    assert Hash32.from_hex(raw.hex()) == digest

    for invalid in (b"", b"x" * 31, b"x" * 33, bytearray(32), memoryview(b"x" * 32)):
        with pytest.raises((TypeError, ValueError)):
            Hash32(invalid)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Hash32.from_hex("AA" * 32)


def test_identifier_types_are_explicit_nonempty_utf8_and_not_interchangeable() -> None:
    store_id = StoreId("store-\u03b1")
    run_id = RunId("store-\u03b1")
    stream_id = StreamId("store-\u03b1")

    assert store_id.value.encode("utf-8") == b"store-\xce\xb1"
    assert store_id != run_id
    assert run_id != stream_id
    assert SegmentIdentity(_hash(2)).digest == _hash(2)
    assert ManifestIdentity(_hash(3)).digest == _hash(3)

    for constructor in (StoreId, RunId, StreamId):
        with pytest.raises(ValueError):
            constructor("")
        with pytest.raises(ValueError):
            constructor("\ud800")
        with pytest.raises(TypeError):
            constructor(b"identifier")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("constructor", "maximum"),
    [
        (CommitSequence, (1 << 64) - 1),
        (EventSequence, (1 << 64) - 1),
        (CommitOrdinal, (1 << 32) - 1),
        (LocalCount, (1 << 32) - 1),
    ],
)
def test_bounded_integer_types_accept_edges_and_reject_bool_negative_and_overflow(
    constructor: type[CommitSequence]
    | type[EventSequence]
    | type[CommitOrdinal]
    | type[LocalCount],
    maximum: int,
) -> None:
    assert int(constructor(0)) == 0
    assert int(constructor(maximum)) == maximum

    for invalid in (True, False, -1, maximum + 1, 1.0):
        with pytest.raises((TypeError, ValueError)):
            constructor(invalid)  # type: ignore[arg-type]


def test_canonical_json_is_stable_sorted_compact_utf8_and_preserves_scalar_types() -> None:
    left = {
        "z": [True, None, 0, "0"],
        "a": "café 漢字",
    }
    right = {
        "a": "café 漢字",
        "z": [True, None, 0, "0"],
    }
    expected = '{"a":"café 漢字","z":[true,null,0,"0"]}'.encode()

    assert canonical_json_bytes(left) == expected
    assert canonical_json_bytes(right) == expected
    assert b", " not in expected
    assert b": " not in expected
    assert b"\\u" not in expected


def test_canonical_json_preserves_absent_null_and_empty_values() -> None:
    absent = canonical_json_bytes({"present": 1})
    null = canonical_json_bytes({"missing": None, "present": 1})
    empties = canonical_json_bytes(
        {"array": [], "object": {}, "present": 1, "text": ""}
    )

    assert absent == b'{"present":1}'
    assert null == b'{"missing":null,"present":1}'
    assert empties == b'{"array":[],"object":{},"present":1,"text":""}'
    assert len({absent, null, empties}) == 3


def test_canonical_json_serializes_its_validated_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source: dict[str, object] = {"value": 1}
    real_dumps = canonical_module.json.dumps

    def mutate_source_before_dump(value: object, **_kwargs: object) -> str:
        source["value"] = 1.25
        return real_dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    monkeypatch.setattr(canonical_module.json, "dumps", mutate_source_before_dump)

    assert canonical_json_bytes(source) == b'{"value":1}'
    assert source == {"value": 1.25}


def test_decimal_requires_an_explicit_canonical_string() -> None:
    assert canonical_json_bytes({"amount": "1.2300"}) == b'{"amount":"1.2300"}'

    with pytest.raises(CanonicalizationError, match="Decimal"):
        canonical_json_bytes({"amount": Decimal("1.2300")})


@pytest.mark.parametrize("value", [0.0, 1.25, float("nan"), float("inf"), float("-inf")])
def test_canonical_json_rejects_all_floats(value: float) -> None:
    with pytest.raises(CanonicalizationError, match="float"):
        canonical_json_bytes({"value": value})


@pytest.mark.parametrize(
    "value",
    [
        {1: "non-text-key"},
        {"payload": b"untyped-bytes"},
        {"payload": bytearray(b"untyped-bytes")},
        {"payload": ("implicit", "tuple")},
        {"payload": object()},
    ],
)
def test_canonical_json_rejects_implicit_or_untyped_python_values(value: object) -> None:
    with pytest.raises(CanonicalizationError):
        canonical_json_bytes(value)


def test_canonical_json_rejects_non_utf8_surrogates() -> None:
    with pytest.raises(CanonicalizationError, match="UTF-8"):
        canonical_json_bytes({"value": "\ud800"})


def test_fixed_width_integer_framing_is_big_endian() -> None:
    assert frame_u32(0x01020304) == b"\x01\x02\x03\x04"
    assert frame_u64(0x0102030405060708) == b"\x01\x02\x03\x04\x05\x06\x07\x08"

    for invalid in (True, -1, 1 << 32):
        with pytest.raises((TypeError, ValueError)):
            frame_u32(invalid)  # type: ignore[arg-type]
    for invalid in (True, -1, 1 << 64):
        with pytest.raises((TypeError, ValueError)):
            frame_u64(invalid)  # type: ignore[arg-type]


def test_variable_length_framing_eliminates_concatenation_ambiguity() -> None:
    first = frame_text("ab") + frame_text("c")
    second = frame_text("a") + frame_text("bc")

    assert first == b"\x00\x00\x00\x02ab\x00\x00\x00\x01c"
    assert second == b"\x00\x00\x00\x01a\x00\x00\x00\x02bc"
    assert first != second
    assert framed_hash(DOMAIN_ROW, b"ab", b"c") != framed_hash(
        DOMAIN_ROW,
        b"a",
        b"bc",
    )
    assert framed_hash(DOMAIN_ROW) != framed_hash(DOMAIN_ROW, b"")


def test_one_domain_never_has_two_cross_schema_preimage_grammars() -> None:
    pathological = LogicalRow(
        StreamId("\x00"),
        CommitOrdinal(2304),
        None,
    )

    assert row_preimage(pathological) != framed_preimage(
        DOMAIN_ROW,
        frame_bytes(b"null"),
    )


def test_optional_framing_distinguishes_absent_present_empty_and_present_value() -> None:
    absent = frame_optional_bytes(None)
    empty = frame_optional_bytes(b"")
    value = frame_optional_bytes(b"x")

    assert absent == b"\x00"
    assert empty == b"\x01\x00\x00\x00\x00"
    assert value == b"\x01\x00\x00\x00\x01x"
    assert len({absent, empty, value}) == 3
    assert frame_optional_hash32(None) == b"\x00"
    assert frame_optional_hash32(_hash(9)) == b"\x01" + bytes(_hash(9))


def test_domain_frame_is_versioned_and_domain_hashes_are_separated() -> None:
    expected_row_prefix = frame_u32(len(DOMAIN_ROW)) + DOMAIN_ROW + b"\x00\x01"

    assert PROTOCOL_VERSION == 1
    assert frame_domain(DOMAIN_ROW) == expected_row_prefix
    assert framed_hash(DOMAIN_ROW, frame_text("same")) != framed_hash(
        DOMAIN_COMMIT,
        frame_text("same"),
    )
    assert framed_hash(DOMAIN_COMMIT, frame_text("same")) != framed_hash(
        DOMAIN_PREFIX,
        frame_text("same"),
    )


def test_row_preimage_and_hash_are_byte_exact_and_logically_invariant() -> None:
    row = LogicalRow(StreamId("events"), CommitOrdinal(2), {"b": 2, "a": "é"})
    same = LogicalRow(StreamId("events"), CommitOrdinal(2), {"a": "é", "b": 2})
    fields = (
        frame_text("events"),
        frame_u32(2),
        frame_bytes('{"a":"é","b":2}'.encode()),
    )
    expected = (
        frame_domain(DOMAIN_ROW)
        + frame_u32(len(fields))
        + b"".join(frame_bytes(field) for field in fields)
    )

    assert row_preimage(row) == expected
    assert row_hash(row) == Hash32(hashlib.sha256(expected).digest())
    assert row_hash(same) == row_hash(row)
    assert row_hash(LogicalRow(StreamId("alerts"), CommitOrdinal(2), row.value)) != row_hash(row)
    assert row_hash(LogicalRow(StreamId("events"), CommitOrdinal(1), row.value)) != row_hash(row)


def test_protocol_v1_commit_and_prefix_match_frozen_byte_vectors() -> None:
    row = LogicalRow(
        StreamId("events"),
        CommitOrdinal(0),
        {"a": 1, "text": "é"},
    )
    frame = CommitFrame(
        run_id=RunId("golden"),
        commit_sequence=CommitSequence(1),
        previous_prefix_root=Hash32(bytes(32)),
        rows=(row,),
    )
    logical = build_commit_logical(frame)

    expected_row_preimage = bytes.fromhex(
        "00000007484c342d524f570001000000030000000a000000066576656e7473"
        "000000040000000000000017000000137b2261223a312c2274657874223a22"
        "c3a9227d"
    )
    expected_commit_preimage = bytes.fromhex(
        "0000000a484c342d434f4d4d49540001000000090000000a00000006676f6c"
        "64656e00000008000000000000000100000004000000010000000a00000006"
        "6576656e747300000004000000010000000400000001000000201273c28c1d"
        "5231ec37c005ee5947deb91b442ea21c96a0ded34fb8ab4e758bc600000020"
        "00000000000000000000000000000000000000000000000000000000000000"
        "000000000100"
    )
    expected_prefix_preimage = bytes.fromhex(
        "0000000a484c342d5052454649580001000000050000000a00000006676f6c"
        "64656e00000008000000000000000100000020000000000000000000000000"
        "0000000000000000000000000000000000000000000000204046937073a78a"
        "bdbf67aab1e74dc259de8d4d5a33233dc08cb0be342d83ed9b0000000100"
    )

    assert row_preimage(row) == expected_row_preimage
    assert row_hash(row) == Hash32.from_hex(
        "1273c28c1d5231ec37c005ee5947deb91b442ea21c96a0ded34fb8ab4e758bc6"
    )
    assert commit_preimage(logical) == expected_commit_preimage
    assert logical.digest == Hash32.from_hex(
        "4046937073a78abdbf67aab1e74dc259de8d4d5a33233dc08cb0be342d83ed9b"
    )
    assert prefix_preimage(logical) == expected_prefix_preimage
    assert logical.prefix_root == Hash32.from_hex(
        "5aa2cd21fe541a69f6004cab73bcdf537a2b7fef8feedf859a47579adb4d3471"
    )


def test_commit_identity_detects_run_sequence_rows_and_declared_count_changes() -> None:
    row = LogicalRow(StreamId("events"), CommitOrdinal(0), {"value": 1})
    frame = CommitFrame(
        run_id=RunId("run-a"),
        commit_sequence=CommitSequence(7),
        previous_prefix_root=_hash(1),
        rows=(row,),
    )
    logical = build_commit_logical(frame)
    variants = (
        CommitFrame(RunId("run-b"), frame.commit_sequence, frame.previous_prefix_root, frame.rows),
        CommitFrame(frame.run_id, CommitSequence(8), frame.previous_prefix_root, frame.rows),
        CommitFrame(frame.run_id, frame.commit_sequence, frame.previous_prefix_root, ()),
        CommitFrame(
            frame.run_id,
            frame.commit_sequence,
            frame.previous_prefix_root,
            (row, LogicalRow(StreamId("events"), CommitOrdinal(1), row.value)),
        ),
        CommitFrame(
            frame.run_id,
            frame.commit_sequence,
            frame.previous_prefix_root,
            (LogicalRow(StreamId("events"), CommitOrdinal(0), {"value": 2}),),
        ),
    )

    assert all(build_commit_logical(variant).digest != logical.digest for variant in variants)

    false_counts = CommitLogical(
        frame=logical.frame,
        row_hashes=logical.row_hashes,
        counts_by_stream=((StreamId("alerts"), LocalCount(1)),),
        digest=logical.digest,
        prefix_root=logical.prefix_root,
    )
    assert commit_preimage(false_counts) != commit_preimage(logical)
    assert not verify_commit_logical(false_counts)


def test_public_commit_digest_rejects_untyped_or_inconsistent_components() -> None:
    row = LogicalRow(StreamId("events"), CommitOrdinal(0), {"value": 1})
    frame = CommitFrame(
        RunId("run"),
        CommitSequence(1),
        Hash32(bytes(32)),
        (row,),
    )
    logical = build_commit_logical(frame)

    with pytest.raises(TypeError, match="row_hashes"):
        commit_digest(
            frame,
            list(logical.row_hashes),  # type: ignore[arg-type]
            logical.counts_by_stream,
        )
    with pytest.raises(ValueError, match="row hashes"):
        commit_digest(frame, (_hash(9),), logical.counts_by_stream)
    with pytest.raises(TypeError, match="StreamId and LocalCount"):
        commit_digest(
            frame,
            logical.row_hashes,
            ((StreamId("events"), True),),  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="stream counts"):
        commit_digest(
            frame,
            logical.row_hashes,
            ((StreamId("alerts"), LocalCount(1)),),
        )


def test_commit_logical_binds_counts_order_previous_root_and_legacy_presence() -> None:
    rows = (
        LogicalRow(StreamId("events"), CommitOrdinal(0), {"event": 1}),
        LogicalRow(StreamId("inputs"), CommitOrdinal(0), {"input": 1}),
        LogicalRow(StreamId("events"), CommitOrdinal(1), {"event": 2}),
    )
    frame = _frame(*rows)
    logical = build_commit_logical(frame)

    assert logical.frame is frame
    assert logical.row_hashes == tuple(row_hash(row) for row in rows)
    assert logical.counts_by_stream == (
        (StreamId("events"), LocalCount(2)),
        (StreamId("inputs"), LocalCount(1)),
    )
    assert logical.digest == Hash32(hashlib.sha256(commit_preimage(logical)).digest())
    assert logical.prefix_root == Hash32(hashlib.sha256(prefix_preimage(logical)).digest())

    with_legacy = build_commit_logical(_frame(*rows, legacy=_hash(7)))
    with_other_previous = build_commit_logical(
        CommitFrame(
            run_id=frame.run_id,
            commit_sequence=frame.commit_sequence,
            previous_prefix_root=_hash(2),
            rows=frame.rows,
        )
    )
    reordered = build_commit_logical(_frame(rows[1], rows[0], rows[2]))

    assert with_legacy.digest != logical.digest
    assert with_legacy.prefix_root != logical.prefix_root
    assert with_other_previous.digest != logical.digest
    assert with_other_previous.prefix_root != logical.prefix_root
    assert reordered.digest != logical.digest
    assert reordered.prefix_root != logical.prefix_root


def test_commit_frame_rejects_duplicate_or_noncontiguous_stream_ordinals() -> None:
    duplicate = (
        LogicalRow(StreamId("events"), CommitOrdinal(0), {"event": 1}),
        LogicalRow(StreamId("events"), CommitOrdinal(0), {"event": 2}),
    )
    gap = (
        LogicalRow(StreamId("events"), CommitOrdinal(0), {"event": 1}),
        LogicalRow(StreamId("events"), CommitOrdinal(2), {"event": 2}),
    )

    with pytest.raises(ValueError, match="duplicate"):
        _frame(*duplicate)
    with pytest.raises(ValueError, match="contiguous"):
        _frame(*gap)


@pytest.mark.parametrize(
    "record",
    [
        InputLogical({"kind": "input"}),
        EventLogical({"kind": "event"}),
        LedgerTransactionLogical({"kind": "ledger_transaction"}),
        LedgerEntryLogical({"kind": "ledger_entry"}),
        AlertLogical({"kind": "alert"}),
        ProjectionDeltaLogical({"kind": "projection_delta"}),
    ],
)
def test_named_logical_records_produce_strict_rows(record: object) -> None:
    row = record.as_row(StreamId("named"), CommitOrdinal(0))  # type: ignore[attr-defined]

    assert row == LogicalRow(StreamId("named"), CommitOrdinal(0), record.value)  # type: ignore[attr-defined]
    assert SYNTHETIC_STORAGE_V4_WORKLOAD.startswith("SYNTHETIC_")

    with pytest.raises(CanonicalizationError):
        type(record)({"invalid": 1.5})


def test_logical_rows_snapshot_mutable_sources_and_do_not_expose_mutable_state() -> None:
    source = {"nested": [1]}
    row = LogicalRow(StreamId("events"), CommitOrdinal(0), source)
    frame = _frame(row)
    logical = build_commit_logical(frame)

    source["nested"].append(2)
    exposed = row.value
    assert isinstance(exposed, dict)
    nested = exposed["nested"]
    assert isinstance(nested, list)
    nested.append(3)

    assert row.value == {"nested": [1]}
    assert build_commit_logical(frame) == logical
