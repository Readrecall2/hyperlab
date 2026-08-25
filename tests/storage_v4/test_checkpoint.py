from __future__ import annotations

from dataclasses import replace

import pytest

from hyperlab.paper.storage_v4.checkpoint import (
    CHECKPOINT_MAGIC,
    Checkpoint,
    CheckpointFormatError,
    CheckpointReadLimits,
    CheckpointState,
    build_checkpoint,
    checkpoint_from_bytes,
    checkpoint_to_bytes,
    verify_checkpoint,
)
from hyperlab.paper.storage_v4.manifest import OpaqueIdentity
from hyperlab.paper.storage_v4.types import (
    CommitSequence,
    Hash32,
    RunId,
    SegmentIdentity,
    StoreId,
    StreamId,
)

SYNTHETIC_STORAGE_V4_WORKLOAD = True
_STORE = StoreId("SYNTHETIC_STORAGE_V4_WORKLOAD/checkpoint-store")
_RUN = RunId("SYNTHETIC_STORAGE_V4_WORKLOAD/checkpoint-run")


def _hash(marker: int) -> Hash32:
    return Hash32(bytes([marker]) * 32)


def _identity(marker: int) -> OpaqueIdentity:
    return OpaqueIdentity(_hash(marker))


def _state() -> CheckpointState:
    return CheckpointState(
        adapter={"book": {"sequence": 12}},
        ledger={"cash": "100.0000", "positions": []},
        projection={"revision": 12, "state": "HEDGED"},
        sessions={"active": None, "closed": ["session-1"]},
        incidents={"open": [], "resolved": ["MARKET_GAP"]},
        cursors={"public": "cursor-12"},
        stream_heads={"alerts": 3, "events": 12},
    )


def _checkpoint(**changes: object) -> Checkpoint:
    values: dict[str, object] = {
        "store_id": _STORE,
        "run_id": _RUN,
        "mode": "V4_NATIVE",
        "target_manifest_generation": 3,
        "parent_manifest_root": _hash(1),
        "start_prefix_root": _hash(2),
        "covered_commit_sequence": CommitSequence(12),
        "covered_prefix_root": _hash(3),
        "covered_segment_identity": SegmentIdentity(_hash(4)),
        "candidate_segment_descriptors_digest": _hash(5),
        "run_identity": _identity(6),
        "config_identity": _identity(7),
        "code_identity": _identity(8),
        "runtime_identity": _identity(9),
        "historical_commit_count": 12,
        "cumulative_stream_counts": (
            (StreamId("alerts"), 3),
            (StreamId("events"), 12),
        ),
        "state": _state(),
    }
    values.update(changes)
    return build_checkpoint(**values)  # type: ignore[arg-type]


def _expectations(checkpoint: Checkpoint) -> dict[str, object]:
    return {
        "expected_store_id": checkpoint.store_id,
        "expected_run_id": checkpoint.run_id,
        "expected_mode": checkpoint.mode,
        "expected_target_manifest_generation": checkpoint.target_manifest_generation,
        "expected_parent_manifest_root": checkpoint.parent_manifest_root,
        "expected_start_prefix_root": checkpoint.start_prefix_root,
        "expected_covered_commit_sequence": checkpoint.covered_commit_sequence,
        "expected_covered_prefix_root": checkpoint.covered_prefix_root,
        "expected_covered_segment_identity": checkpoint.covered_segment_identity,
        "expected_candidate_segment_descriptors_digest": (
            checkpoint.candidate_segment_descriptors_digest
        ),
        "expected_run_identity": checkpoint.run_identity,
        "expected_config_identity": checkpoint.config_identity,
        "expected_code_identity": checkpoint.code_identity,
        "expected_runtime_identity": checkpoint.runtime_identity,
    }


def test_checkpoint_roundtrip_binds_complete_state_and_content_address() -> None:
    checkpoint = _checkpoint()
    encoded = checkpoint_to_bytes(checkpoint)
    decoded = checkpoint_from_bytes(encoded)

    assert encoded.startswith(CHECKPOINT_MAGIC)
    assert decoded == checkpoint
    assert decoded.root == checkpoint.root
    assert decoded.identity == checkpoint.root
    assert checkpoint_to_bytes(decoded) == encoded
    assert decoded.state.adapter == {"book": {"sequence": 12}}
    assert decoded.state.ledger["cash"] == "100.0000"
    assert decoded.state.projection["state"] == "HEDGED"
    assert decoded.state.sessions["closed"] == ["session-1"]
    assert decoded.state.incidents["resolved"] == ["MARKET_GAP"]
    assert decoded.state.cursors == {"public": "cursor-12"}
    assert decoded.state.stream_heads == {"alerts": 3, "events": 12}
    verify_checkpoint(decoded, **_expectations(checkpoint))  # type: ignore[arg-type]


def test_checkpoint_state_snapshots_sources_and_never_exposes_mutable_state() -> None:
    adapter = {"nested": [1]}
    state = CheckpointState(
        adapter=adapter,
        ledger={},
        projection={},
        sessions={},
        incidents={},
        cursors={},
        stream_heads={},
    )
    checkpoint = _checkpoint(state=state)
    root = checkpoint.root

    adapter["nested"].append(2)
    exposed = state.adapter
    exposed["nested"].append(3)  # type: ignore[union-attr]

    assert state.adapter == {"nested": [1]}
    assert checkpoint.root == root


@pytest.mark.parametrize("invalid", [0.0, 1.25, float("nan"), float("inf")])
def test_checkpoint_native_state_rejects_every_float(invalid: float) -> None:
    with pytest.raises(CheckpointFormatError, match="canonical"):
        CheckpointState(
            adapter={"value": invalid},
            ledger={},
            projection={},
            sessions={},
            incidents={},
            cursors={},
            stream_heads={},
        )


def test_checkpoint_requires_parent_exactly_outside_genesis() -> None:
    genesis = _checkpoint(target_manifest_generation=1, parent_manifest_root=None)
    assert checkpoint_from_bytes(checkpoint_to_bytes(genesis)) == genesis

    with pytest.raises(CheckpointFormatError, match="genesis"):
        _checkpoint(target_manifest_generation=1, parent_manifest_root=_hash(1))
    with pytest.raises(CheckpointFormatError, match="requires a parent"):
        _checkpoint(target_manifest_generation=2, parent_manifest_root=None)


def test_checkpoint_rejects_unsorted_duplicate_zero_or_untyped_cumulative_counts() -> None:
    with pytest.raises(CheckpointFormatError, match="sorted"):
        _checkpoint(
            cumulative_stream_counts=(
                (StreamId("events"), 1),
                (StreamId("alerts"), 1),
            )
        )
    with pytest.raises(CheckpointFormatError, match="uint64"):
        _checkpoint(cumulative_stream_counts=((StreamId("events"), 0),))
    with pytest.raises(TypeError, match="exact pairs"):
        _checkpoint(cumulative_stream_counts=([StreamId("events"), 1],))


def test_checkpoint_reader_rejects_corruption_truncation_versions_and_trailing_bytes() -> None:
    encoded = checkpoint_to_bytes(_checkpoint())
    corrupt_root = bytearray(encoded)
    corrupt_root[-1] ^= 0x01
    with pytest.raises(CheckpointFormatError, match="root mismatch"):
        checkpoint_from_bytes(bytes(corrupt_root))

    for truncated in (encoded[:1], encoded[:-1], encoded[:-32]):
        with pytest.raises(CheckpointFormatError):
            checkpoint_from_bytes(truncated)
    with pytest.raises(CheckpointFormatError, match="trailing"):
        checkpoint_from_bytes(encoded + b"x")

    future_format = bytearray(encoded)
    future_format[len(CHECKPOINT_MAGIC) : len(CHECKPOINT_MAGIC) + 2] = (2).to_bytes(2, "big")
    with pytest.raises(CheckpointFormatError, match="format version"):
        checkpoint_from_bytes(bytes(future_format))

    future_protocol = bytearray(encoded)
    body_offset = len(CHECKPOINT_MAGIC) + 2 + 8
    future_protocol[body_offset : body_offset + 2] = (2).to_bytes(2, "big")
    with pytest.raises(CheckpointFormatError, match="logical protocol"):
        checkpoint_from_bytes(bytes(future_protocol))


def test_checkpoint_reader_applies_all_decode_limits_before_materialization() -> None:
    encoded = checkpoint_to_bytes(_checkpoint())
    body_size = int.from_bytes(
        encoded[len(CHECKPOINT_MAGIC) + 2 : len(CHECKPOINT_MAGIC) + 10],
        "big",
    )
    cases = (
        CheckpointReadLimits(max_physical_size=len(encoded) - 1),
        CheckpointReadLimits(max_body_size=body_size - 1),
        CheckpointReadLimits(max_text_size=1),
        CheckpointReadLimits(max_state_section_size=1),
        CheckpointReadLimits(max_streams=1),
    )

    for limits in cases:
        with pytest.raises(CheckpointFormatError, match="limit"):
            checkpoint_from_bytes(encoded, limits=limits)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("expected_store_id", StoreId("wrong-store"), "wrong store"),
        ("expected_run_id", RunId("wrong-run"), "wrong run"),
        ("expected_mode", "V3_COMPATIBILITY_IMPORT", "wrong storage mode"),
        ("expected_parent_manifest_root", _hash(99), "parent manifest"),
        ("expected_start_prefix_root", _hash(99), "starting prefix"),
        ("expected_covered_commit_sequence", CommitSequence(11), "covered sequence"),
        ("expected_covered_prefix_root", _hash(99), "covered prefix"),
        (
            "expected_covered_segment_identity",
            SegmentIdentity(_hash(99)),
            "covered segment",
        ),
        (
            "expected_candidate_segment_descriptors_digest",
            _hash(99),
            "candidate segment",
        ),
        ("expected_run_identity", _identity(99), "run identity"),
        ("expected_config_identity", _identity(99), "config identity"),
        ("expected_code_identity", _identity(99), "code identity"),
        ("expected_runtime_identity", _identity(99), "runtime identity"),
    ],
)
def test_checkpoint_verifier_rejects_wrong_identity_or_material(
    field: str,
    value: object,
    message: str,
) -> None:
    checkpoint = _checkpoint()
    expected = _expectations(checkpoint)
    expected[field] = value

    with pytest.raises(CheckpointFormatError, match=message):
        verify_checkpoint(checkpoint, **expected)  # type: ignore[arg-type]


def test_checkpoint_verifier_distinguishes_future_and_stale_generations() -> None:
    checkpoint = _checkpoint()
    expected = _expectations(checkpoint)
    expected["expected_target_manifest_generation"] = 2
    with pytest.raises(CheckpointFormatError, match="future"):
        verify_checkpoint(checkpoint, **expected)  # type: ignore[arg-type]

    expected["expected_target_manifest_generation"] = 4
    with pytest.raises(CheckpointFormatError, match="stale"):
        verify_checkpoint(checkpoint, **expected)  # type: ignore[arg-type]


def test_checkpoint_root_changes_for_state_count_and_transition_material() -> None:
    checkpoint = _checkpoint()
    variants = (
        replace(checkpoint, historical_commit_count=13),
        replace(checkpoint, covered_prefix_root=_hash(44)),
        replace(checkpoint, candidate_segment_descriptors_digest=_hash(55)),
        replace(checkpoint, state=CheckpointState(
            adapter={"changed": True},
            ledger={},
            projection={},
            sessions={},
            incidents={},
            cursors={},
            stream_heads={},
        )),
    )

    assert all(candidate.root != checkpoint.root for candidate in variants)
    assert all(checkpoint_to_bytes(candidate) != checkpoint_to_bytes(checkpoint) for candidate in variants)
