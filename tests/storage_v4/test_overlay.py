from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from hyperlab.paper.storage_v4.canonical import build_commit_logical
from hyperlab.paper.storage_v4.contracts import StorageMode
from hyperlab.paper.storage_v4.manifest import OpaqueIdentity
from hyperlab.paper.storage_v4.overlay import (
    FAULT_AFTER_COMMIT,
    FAULT_BEFORE_COMMIT,
    GENESIS_MANIFEST_GENERATION,
    GENESIS_MANIFEST_ROOT,
    OVERLAY_SCHEMA_VERSION,
    OverlayError,
    OverlayErrorCode,
    OverlayIdentity,
    OverlayThresholds,
    SQLiteOverlay,
)
from hyperlab.paper.storage_v4.records import (
    RecordFormatError,
    commit_frame_from_bytes,
    commit_frame_to_bytes,
    logical_row_from_bytes,
    logical_row_to_bytes,
)
from hyperlab.paper.storage_v4.segment import CodecProfile
from hyperlab.paper.storage_v4.types import (
    UINT64_MAX,
    CommitFrame,
    CommitOrdinal,
    CommitSequence,
    Hash32,
    LogicalRow,
    RunId,
    StoreId,
    StreamId,
)

SYNTHETIC_STORAGE_V4_WORKLOAD = True
_RUN_ID = RunId("SYNTHETIC_STORAGE_V4_WORKLOAD/overlay")
_OTHER_RUN_ID = RunId("SYNTHETIC_STORAGE_V4_WORKLOAD/other-overlay")
_STORE_ID = StoreId("SYNTHETIC_STORAGE_V4_WORKLOAD/store")
_OTHER_STORE_ID = StoreId("SYNTHETIC_STORAGE_V4_WORKLOAD/other-store")
_ZERO = Hash32(b"\x00" * 32)
_MANIFEST_ROOT = Hash32(b"\x10" * 32)
_BASE_SEQUENCE = CommitSequence(10)


def _opaque(marker: int) -> OpaqueIdentity:
    return OpaqueIdentity(Hash32(bytes([marker]) * 32))


def _identity(
    *,
    store_id: StoreId = _STORE_ID,
    run_id: RunId = _RUN_ID,
    mode: StorageMode = StorageMode.V3_COMPATIBILITY_IMPORT,
    run_identity: OpaqueIdentity | None = None,
    config_identity: OpaqueIdentity | None = None,
    code_identity: OpaqueIdentity | None = None,
    runtime_identity: OpaqueIdentity | None = None,
    codec_profile: CodecProfile | None = None,
    base_manifest_generation: int = 1,
    base_manifest_root: Hash32 = _MANIFEST_ROOT,
    base_commit_sequence: CommitSequence = _BASE_SEQUENCE,
    base_prefix_root: Hash32 = _ZERO,
    thresholds: OverlayThresholds | None = None,
) -> OverlayIdentity:
    return OverlayIdentity(
        store_id=store_id,
        run_id=run_id,
        mode=mode,
        run_identity=run_identity if run_identity is not None else _opaque(1),
        config_identity=config_identity if config_identity is not None else _opaque(2),
        code_identity=code_identity if code_identity is not None else _opaque(3),
        runtime_identity=runtime_identity if runtime_identity is not None else _opaque(4),
        codec_profile=codec_profile if codec_profile is not None else CodecProfile.zlib(),
        base_manifest_generation=base_manifest_generation,
        base_manifest_root=base_manifest_root,
        base_commit_sequence=base_commit_sequence,
        base_prefix_root=base_prefix_root,
        thresholds=thresholds if thresholds is not None else OverlayThresholds(),
    )


def _row(marker: str, *, stream: str = "events", ordinal: int = 0) -> LogicalRow:
    return LogicalRow(
        stream_id=StreamId(stream),
        ordinal=CommitOrdinal(ordinal),
        value={"marker": marker, "synthetic": True},
    )


def _frame(
    sequence: int,
    previous: Hash32,
    *,
    marker: str | None = None,
    run_id: RunId = _RUN_ID,
    rows: tuple[LogicalRow, ...] | None = None,
) -> CommitFrame:
    return CommitFrame(
        run_id=run_id,
        commit_sequence=CommitSequence(sequence),
        previous_prefix_root=previous,
        rows=rows if rows is not None else (_row(marker or f"commit-{sequence}"),),
    )


def _create(
    path: Path,
    *,
    base_sequence: int = 10,
    base_prefix: Hash32 = _ZERO,
    thresholds: OverlayThresholds | None = None,
    fault_injector: object = None,
) -> SQLiteOverlay:
    callback = fault_injector if callable(fault_injector) else None
    identity = _identity(
        base_commit_sequence=CommitSequence(base_sequence),
        base_prefix_root=base_prefix,
        thresholds=thresholds,
    )
    return SQLiteOverlay.create(
        path,
        identity=identity,
        fault_injector=callback,
    )


def _append_chain(overlay: SQLiteOverlay, first: int, last: int, previous: Hash32) -> list[CommitFrame]:
    frames: list[CommitFrame] = []
    for sequence in range(first, last + 1):
        frame = _frame(sequence, previous)
        assert overlay.append(frame)
        frames.append(frame)
        previous = build_commit_logical(frame).prefix_root
    return frames


def test_row_and_commit_records_roundtrip_deterministically_and_strictly() -> None:
    first = LogicalRow(StreamId("events"), CommitOrdinal(0), {"b": 2, "a": "é"})
    same = LogicalRow(StreamId("events"), CommitOrdinal(0), {"a": "é", "b": 2})
    row_bytes = logical_row_to_bytes(first)

    assert row_bytes == logical_row_to_bytes(same)
    assert logical_row_from_bytes(row_bytes) == first

    frame = CommitFrame(
        run_id=_RUN_ID,
        commit_sequence=CommitSequence(UINT64_MAX),
        previous_prefix_root=Hash32(b"\x55" * 32),
        rows=(first, _row("alert", stream="alerts")),
        legacy_v3_identity=Hash32(b"\x77" * 32),
    )
    encoded = commit_frame_to_bytes(frame)
    assert commit_frame_from_bytes(encoded) == frame
    assert commit_frame_to_bytes(commit_frame_from_bytes(encoded)) == encoded

    damaged = bytearray(encoded)
    damaged[-1] ^= 1
    with pytest.raises(RecordFormatError, match="SHA-256"):
        commit_frame_from_bytes(bytes(damaged))
    with pytest.raises(RecordFormatError, match="trailing"):
        commit_frame_from_bytes(encoded + b"extra")


def test_create_and_open_existing_are_distinct_and_pragmas_are_durable(tmp_path: Path) -> None:
    path = tmp_path / "overlay.sqlite3"
    with pytest.raises(OverlayError) as missing:
        SQLiteOverlay.open_existing(path, expected_identity=_identity())
    assert missing.value.code == OverlayErrorCode.MISSING
    assert not path.exists()

    overlay = _create(path)
    settings = overlay.durability_settings()
    assert settings.journal_mode == "delete"
    assert settings.synchronous == 2
    assert overlay.state.base_manifest_generation == 1
    assert overlay.state.base_manifest_root == _MANIFEST_ROOT
    overlay.close()

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (OVERLAY_SCHEMA_VERSION,)
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("delete",)

    reopened = SQLiteOverlay.open_existing(
        path,
        expected_identity=_identity(),
    )
    assert reopened.frames() == ()
    reopened.close()

    with pytest.raises(OverlayError) as exists:
        _create(path)
    assert exists.value.code == OverlayErrorCode.ALREADY_EXISTS
    with pytest.raises(OverlayError) as mismatch:
        SQLiteOverlay.open_existing(
            path,
            expected_identity=replace(_identity(), base_manifest_generation=2),
        )
    assert mismatch.value.code == OverlayErrorCode.EXPECTED_STATE_MISMATCH


@pytest.mark.parametrize(
    ("expected_field", "changes"),
    (
        ("store_id", {"store_id": _OTHER_STORE_ID}),
        ("run_id", {"run_id": _OTHER_RUN_ID}),
        ("mode", {"mode": StorageMode.V4_NATIVE}),
        ("run_identity", {"run_identity": _opaque(11)}),
        ("config_identity", {"config_identity": _opaque(12)}),
        ("code_identity", {"code_identity": _opaque(13)}),
        ("runtime_identity", {"runtime_identity": _opaque(14)}),
        ("codec_profile", {"codec_profile": CodecProfile.raw()}),
        ("base_manifest_generation", {"base_manifest_generation": 2}),
        ("base_manifest_root", {"base_manifest_root": Hash32(b"\x22" * 32)}),
        ("base_commit_sequence", {"base_commit_sequence": CommitSequence(9)}),
        ("base_prefix_root", {"base_prefix_root": Hash32(b"\x23" * 32)}),
        (
            "seal_rows",
            {"thresholds": OverlayThresholds(seal_rows=4_097, seal_bytes=16 * 1024 * 1024)},
        ),
        (
            "seal_bytes",
            {"thresholds": OverlayThresholds(seal_rows=4_096, seal_bytes=16 * 1024 * 1024 + 1)},
        ),
    ),
)
def test_open_existing_rejects_every_genesis_identity_mismatch_before_nonempty_tail_read(
    tmp_path: Path,
    expected_field: str,
    changes: dict[str, object],
) -> None:
    path = tmp_path / f"identity-{expected_field}.sqlite3"
    identity = _identity()
    overlay = SQLiteOverlay.create(path, identity=identity)
    first = _frame(11, _ZERO)
    assert overlay.append(first)
    overlay.close()

    with pytest.raises(OverlayError) as mismatch:
        SQLiteOverlay.open_existing(
            path,
            expected_identity=replace(identity, **changes),
        )
    expected_code = (
        OverlayErrorCode.WRONG_RUN
        if expected_field == "run_id"
        else OverlayErrorCode.EXPECTED_STATE_MISMATCH
    )
    assert mismatch.value.code == expected_code
    assert mismatch.value.details["field"] == expected_field

    reopened = SQLiteOverlay.open_existing(path, expected_identity=identity)
    assert reopened.frames() == (first,)
    reopened.close()


def test_schema_v2_refuses_ambiguous_v1_even_with_a_nonempty_tail(tmp_path: Path) -> None:
    assert OVERLAY_SCHEMA_VERSION == 2
    path = tmp_path / "ambiguous-v1.sqlite3"
    identity = _identity()
    overlay = SQLiteOverlay.create(path, identity=identity)
    assert overlay.append(_frame(11, _ZERO))
    overlay.close()
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version=1")

    with pytest.raises(OverlayError, match="cannot be adopted") as rejected:
        SQLiteOverlay.open_existing(path, expected_identity=identity)
    assert rejected.value.code == OverlayErrorCode.SCHEMA_MISMATCH
    assert rejected.value.details == {
        "actual": 1,
        "expected": 2,
        "migration_supported": False,
    }


def test_codec_profile_id_must_match_exact_persisted_codec_fields(tmp_path: Path) -> None:
    path = tmp_path / "codec-profile-identity.sqlite3"
    identity = _identity()
    overlay = SQLiteOverlay.create(path, identity=identity)
    assert overlay.append(_frame(11, _ZERO))
    overlay.close()
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE overlay_meta SET codec_profile_id = ? WHERE singleton = 1",
            (CodecProfile.raw().profile_id,),
        )

    with pytest.raises(OverlayError, match="codec profile identity") as corrupt:
        SQLiteOverlay.open_existing(path, expected_identity=identity)
    assert corrupt.value.code == OverlayErrorCode.CORRUPT


def _symlink_or_skip(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"symbolic links are unavailable: {error}")


def test_overlay_rejects_symlink_open_create_and_valid_alternate_substitution(
    tmp_path: Path,
) -> None:
    alternate_path = tmp_path / "alternate.sqlite3"
    alternate = _create(alternate_path)
    alternate.close()
    link = tmp_path / "overlay-link.sqlite3"
    _symlink_or_skip(link, alternate_path)

    with pytest.raises(OverlayError) as created:
        _create(link)
    assert created.value.code is OverlayErrorCode.ALREADY_EXISTS
    with pytest.raises(OverlayError) as opened:
        SQLiteOverlay.open_existing(link, expected_identity=_identity())
    assert opened.value.code is OverlayErrorCode.CORRUPT

    primary_path = tmp_path / "primary.sqlite3"
    primary = _create(primary_path)
    primary.close()
    primary_path.unlink()
    _symlink_or_skip(primary_path, alternate_path)
    with pytest.raises(OverlayError) as substituted:
        SQLiteOverlay.open_existing(primary_path, expected_identity=_identity())
    assert substituted.value.code is OverlayErrorCode.CORRUPT


def test_genesis_manifest_sentinel_is_strict_and_advances_without_a_false_commit(
    tmp_path: Path,
) -> None:
    path = tmp_path / "genesis.sqlite3"
    genesis_identity = _identity(
        base_manifest_generation=GENESIS_MANIFEST_GENERATION,
        base_manifest_root=GENESIS_MANIFEST_ROOT,
        base_commit_sequence=CommitSequence(0),
    )
    overlay = SQLiteOverlay.create(
        path,
        identity=genesis_identity,
    )
    assert overlay.state.base_manifest_generation == 0
    assert overlay.state.base_manifest_root == GENESIS_MANIFEST_ROOT
    assert overlay.frames() == ()
    assert not overlay.advance_base(
        manifest_generation=GENESIS_MANIFEST_GENERATION,
        manifest_root=GENESIS_MANIFEST_ROOT,
        base_commit_sequence=CommitSequence(0),
        base_prefix_root=_ZERO,
    )

    first = _frame(1, _ZERO)
    assert overlay.append(first)
    first_root = build_commit_logical(first).prefix_root
    assert overlay.advance_base(
        manifest_generation=1,
        manifest_root=_MANIFEST_ROOT,
        base_commit_sequence=CommitSequence(1),
        base_prefix_root=first_root,
    )
    assert overlay.frames() == ()
    assert overlay.state.base_commit_sequence == CommitSequence(1)
    overlay.close()

    reopened = SQLiteOverlay.open_existing(
        path,
        expected_identity=genesis_identity,
    )
    assert reopened.identity == genesis_identity
    assert reopened.frames() == ()
    reopened.close()

    zero_generation_path = tmp_path / "invalid-zero-generation.sqlite3"
    with pytest.raises(ValueError, match="genesis sentinel together"):
        SQLiteOverlay.create(
            zero_generation_path,
            identity=_identity(
                base_manifest_generation=0,
                base_manifest_root=_MANIFEST_ROOT,
                base_commit_sequence=CommitSequence(0),
            ),
        )
    assert not zero_generation_path.exists()

    zero_root_path = tmp_path / "invalid-zero-root.sqlite3"
    with pytest.raises(ValueError, match="genesis sentinel together"):
        SQLiteOverlay.create(
            zero_root_path,
            identity=_identity(
                base_manifest_generation=1,
                base_manifest_root=GENESIS_MANIFEST_ROOT,
                base_commit_sequence=CommitSequence(0),
            ),
        )
    assert not zero_root_path.exists()


def test_open_existing_rejects_a_corrupted_genesis_manifest_tuple(tmp_path: Path) -> None:
    path = tmp_path / "corrupt-genesis.sqlite3"
    genesis_identity = _identity(
        base_manifest_generation=GENESIS_MANIFEST_GENERATION,
        base_manifest_root=GENESIS_MANIFEST_ROOT,
        base_commit_sequence=CommitSequence(0),
    )
    overlay = SQLiteOverlay.create(
        path,
        identity=genesis_identity,
    )
    overlay.close()
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE overlay_meta SET base_manifest_root = ? WHERE singleton = 1",
            (bytes(_MANIFEST_ROOT),),
        )

    with pytest.raises(OverlayError) as corrupt:
        SQLiteOverlay.open_existing(
            path,
            expected_identity=genesis_identity,
        )
    assert corrupt.value.code == OverlayErrorCode.CORRUPT


def test_append_is_exactly_idempotent_and_rejects_conflicts_gaps_and_wrong_identity(
    tmp_path: Path,
) -> None:
    overlay = _create(tmp_path / "overlay.sqlite3")
    first = _frame(11, _ZERO)
    assert overlay.append(first)
    assert not overlay.append(first)

    with pytest.raises(OverlayError) as duplicate:
        overlay.append(_frame(11, _ZERO, marker="different"))
    assert duplicate.value.code == OverlayErrorCode.DUPLICATE_CONFLICT

    first_root = build_commit_logical(first).prefix_root
    with pytest.raises(OverlayError) as gap:
        overlay.append(_frame(13, first_root))
    assert gap.value.code == OverlayErrorCode.GAP
    with pytest.raises(OverlayError) as wrong_root:
        overlay.append(_frame(12, Hash32(b"\x99" * 32)))
    assert wrong_root.value.code == OverlayErrorCode.PREVIOUS_ROOT_MISMATCH
    with pytest.raises(OverlayError) as wrong_run:
        overlay.append(_frame(12, first_root, run_id=_OTHER_RUN_ID))
    assert wrong_run.value.code == OverlayErrorCode.WRONG_RUN
    with pytest.raises(OverlayError) as overlap:
        overlay.append(_frame(10, _ZERO))
    assert overlap.value.code == OverlayErrorCode.OVERLAP

    second = _frame(12, first_root)
    assert overlay.append(second)
    assert overlay.frames() == (first, second)
    overlay.close()


def test_uint64_sequence_text_preserves_numeric_order_beyond_sqlite_signed_range(
    tmp_path: Path,
) -> None:
    overlay = _create(tmp_path / "uint64.sqlite3", base_sequence=UINT64_MAX - 2)
    first = _frame(UINT64_MAX - 1, _ZERO)
    assert overlay.append(first)
    second = _frame(UINT64_MAX, build_commit_logical(first).prefix_root)
    assert overlay.append(second)
    assert overlay.frames() == (first, second)
    assert overlay.state.head_commit_sequence == CommitSequence(UINT64_MAX)
    overlay.close()

    reopened = SQLiteOverlay.open_existing(
        tmp_path / "uint64.sqlite3",
        expected_identity=_identity(
            base_commit_sequence=CommitSequence(UINT64_MAX - 2),
        ),
    )
    assert reopened.frames() == (first, second)
    reopened.close()


def test_row_and_byte_thresholds_use_authenticated_record_counters(tmp_path: Path) -> None:
    row_path = tmp_path / "rows.sqlite3"
    row_overlay = _create(
        row_path,
        thresholds=OverlayThresholds(seal_rows=2, seal_bytes=UINT64_MAX),
    )
    rows = (_row("event"), _row("alert", stream="alerts"))
    frame = _frame(11, _ZERO, rows=rows)
    assert row_overlay.append(frame)
    assert row_overlay.state.tail_commit_count == 1
    assert row_overlay.state.tail_row_count == 2
    assert row_overlay.state.tail_bytes == len(commit_frame_to_bytes(frame))
    assert row_overlay.seal_required
    row_overlay.close()

    byte_path = tmp_path / "bytes.sqlite3"
    encoded_size = len(commit_frame_to_bytes(_frame(11, _ZERO)))
    byte_overlay = _create(
        byte_path,
        thresholds=OverlayThresholds(seal_rows=UINT64_MAX, seal_bytes=encoded_size),
    )
    assert byte_overlay.append(_frame(11, _ZERO))
    assert byte_overlay.seal_required
    byte_overlay.close()


class _Fault:
    point: str | None = None

    def __call__(self, point: str) -> None:
        if point == self.point:
            raise RuntimeError(f"synthetic fault at {point}")


def test_fault_before_commit_rolls_back_and_fault_after_commit_is_durable(tmp_path: Path) -> None:
    path = tmp_path / "fault.sqlite3"
    fault = _Fault()
    overlay = _create(path, fault_injector=fault)
    first = _frame(11, _ZERO)

    fault.point = FAULT_BEFORE_COMMIT
    with pytest.raises(RuntimeError, match="before_commit"):
        overlay.append(first)
    fault.point = None
    assert overlay.frames() == ()
    assert overlay.state.tail_commit_count == 0
    overlay.close()

    reopened = SQLiteOverlay.open_existing(
        path,
        expected_identity=_identity(),
        fault_injector=fault,
    )
    fault.point = FAULT_AFTER_COMMIT
    with pytest.raises(RuntimeError, match="after_commit"):
        reopened.append(first)
    fault.point = None
    assert reopened.frames() == (first,)
    reopened.close()

    final = SQLiteOverlay.open_existing(path, expected_identity=_identity())
    assert final.frames() == (first,)
    assert not final.append(first)
    final.close()


def test_uncommitted_external_sqlite_transaction_is_not_adopted(tmp_path: Path) -> None:
    path = tmp_path / "external-incomplete.sqlite3"
    overlay = _create(path)
    overlay.close()
    first = _frame(11, _ZERO)
    logical = build_commit_logical(first)
    record = commit_frame_to_bytes(first)

    connection = sqlite3.connect(path, isolation_level=None)
    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        """
        INSERT INTO overlay_commits (
            commit_sequence, record_bytes, commit_digest, prefix_root,
            previous_prefix_root, row_count, byte_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"{11:020d}",
            record,
            bytes(logical.digest),
            bytes(logical.prefix_root),
            bytes(first.previous_prefix_root),
            len(first.rows),
            len(record),
        ),
    )
    connection.close()

    reopened = SQLiteOverlay.open_existing(path, expected_identity=_identity())
    assert reopened.frames() == ()
    reopened.close()


def test_advance_base_deletes_only_covered_rows_preserves_tail_and_is_idempotent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "advance.sqlite3"
    overlay = _create(path)
    frames = _append_chain(overlay, 11, 13, _ZERO)
    second_root = build_commit_logical(frames[1]).prefix_root
    new_manifest_root = Hash32(b"\x20" * 32)

    assert overlay.advance_base(
        manifest_generation=2,
        manifest_root=new_manifest_root,
        base_commit_sequence=CommitSequence(12),
        base_prefix_root=second_root,
    )
    assert overlay.frames() == (frames[2],)
    state = overlay.state
    assert state.base_manifest_generation == 2
    assert state.base_manifest_root == new_manifest_root
    assert state.base_commit_sequence == CommitSequence(12)
    assert state.base_prefix_root == second_root
    assert state.tail_commit_count == 1
    assert state.tail_row_count == len(frames[2].rows)
    assert state.tail_bytes == len(commit_frame_to_bytes(frames[2]))
    assert not overlay.advance_base(
        manifest_generation=2,
        manifest_root=new_manifest_root,
        base_commit_sequence=CommitSequence(12),
        base_prefix_root=second_root,
    )

    with pytest.raises(OverlayError) as conflict:
        overlay.advance_base(
            manifest_generation=2,
            manifest_root=Hash32(b"\x21" * 32),
            base_commit_sequence=CommitSequence(12),
            base_prefix_root=second_root,
        )
    assert conflict.value.code == OverlayErrorCode.MANIFEST_CONFLICT
    with pytest.raises(OverlayError) as prefix:
        overlay.advance_base(
            manifest_generation=3,
            manifest_root=Hash32(b"\x30" * 32),
            base_commit_sequence=CommitSequence(13),
            base_prefix_root=Hash32(b"\xff" * 32),
        )
    assert prefix.value.code == OverlayErrorCode.BASE_PREFIX_MISMATCH
    assert overlay.frames() == (frames[2],)
    overlay.close()

    reopened = SQLiteOverlay.open_existing(
        path,
        expected_identity=_identity(),
    )
    assert reopened.frames() == (frames[2],)
    third_root = build_commit_logical(frames[2]).prefix_root
    fourth = _frame(14, third_root)
    assert reopened.append(fourth)
    assert reopened.frames() == (frames[2], fourth)
    reopened.close()


def test_discard_unsealed_tail_is_exact_whole_tail_cas_and_idempotent(
    tmp_path: Path,
) -> None:
    overlay = _create(tmp_path / "discard.sqlite3")
    frames = _append_chain(overlay, 11, 13, _ZERO)
    before = overlay.verify_integrity()
    expected_base = replace(
        before,
        tail_commit_count=0,
        tail_row_count=0,
        tail_bytes=0,
        head_commit_sequence=before.base_commit_sequence,
        head_prefix_root=before.base_prefix_root,
    )

    result = overlay.discard_unsealed_tail(expected_base=expected_base)

    assert result.changed is True
    assert result.before == before
    assert result.after == expected_base
    assert result.discarded_commit_count == len(frames)
    assert result.discarded_row_count == sum(len(frame.rows) for frame in frames)
    assert result.discarded_bytes == sum(
        len(commit_frame_to_bytes(frame)) for frame in frames
    )
    assert overlay.frames() == ()
    assert overlay.verify_integrity() == expected_base

    repeated = overlay.discard_unsealed_tail(expected_base=expected_base)
    assert repeated.changed is False
    assert repeated.before == expected_base
    assert repeated.after == expected_base
    overlay.close()


def test_discard_unsealed_tail_refuses_an_arbitrary_cutoff_without_mutation(
    tmp_path: Path,
) -> None:
    overlay = _create(tmp_path / "discard-mismatch.sqlite3")
    frames = _append_chain(overlay, 11, 13, _ZERO)
    before = overlay.verify_integrity()
    invented_cutoff = replace(
        before,
        base_manifest_generation=2,
        base_manifest_root=Hash32(b"\x22" * 32),
        base_commit_sequence=frames[0].commit_sequence,
        base_prefix_root=build_commit_logical(frames[0]).prefix_root,
        tail_commit_count=0,
        tail_row_count=0,
        tail_bytes=0,
        head_commit_sequence=frames[0].commit_sequence,
        head_prefix_root=build_commit_logical(frames[0]).prefix_root,
    )

    with pytest.raises(OverlayError) as failure:
        overlay.discard_unsealed_tail(expected_base=invented_cutoff)

    assert failure.value.code is OverlayErrorCode.EXPECTED_STATE_MISMATCH
    assert overlay.verify_integrity() == before
    assert overlay.frames() == tuple(frames)
    overlay.close()


def test_discard_unsealed_tail_rolls_back_before_commit(tmp_path: Path) -> None:
    fault = _Fault()
    overlay = _create(
        tmp_path / "discard-fault.sqlite3",
        fault_injector=fault,
    )
    frames = _append_chain(overlay, 11, 12, _ZERO)
    before = overlay.verify_integrity()
    expected_base = replace(
        before,
        tail_commit_count=0,
        tail_row_count=0,
        tail_bytes=0,
        head_commit_sequence=before.base_commit_sequence,
        head_prefix_root=before.base_prefix_root,
    )
    fault.point = FAULT_BEFORE_COMMIT

    with pytest.raises(RuntimeError, match="before_commit"):
        overlay.discard_unsealed_tail(expected_base=expected_base)

    fault.point = None
    assert overlay.verify_integrity() == before
    assert overlay.frames() == tuple(frames)
    overlay.close()
