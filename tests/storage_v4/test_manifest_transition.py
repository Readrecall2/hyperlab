from __future__ import annotations

from dataclasses import replace

import pytest

from hyperlab.paper.storage_v4.canonical import build_commit_logical
from hyperlab.paper.storage_v4.manifest import (
    ManifestFormatError,
    OpaqueIdentity,
    SegmentDescriptor,
    build_manifest,
    verify_manifest_transition,
)
from hyperlab.paper.storage_v4.segment import CodecProfile, build_segment
from hyperlab.paper.storage_v4.types import (
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
_ZERO = Hash32(bytes(32))
_STORE = StoreId("SYNTHETIC_STORAGE_V4_WORKLOAD/transition-store")
_RUN = RunId("SYNTHETIC_STORAGE_V4_WORKLOAD/transition-run")


def _ident(value: int) -> OpaqueIdentity:
    return OpaqueIdentity(Hash32(bytes([value]) * 32))


def _descriptors() -> tuple[SegmentDescriptor, SegmentDescriptor]:
    first = CommitFrame(
        run_id=_RUN,
        commit_sequence=CommitSequence(1),
        previous_prefix_root=_ZERO,
        rows=(LogicalRow(StreamId("events"), CommitOrdinal(0), {"sequence": 1}),),
    )
    first_root = build_commit_logical(first).prefix_root
    second = CommitFrame(
        run_id=_RUN,
        commit_sequence=CommitSequence(2),
        previous_prefix_root=first_root,
        rows=(LogicalRow(StreamId("events"), CommitOrdinal(0), {"sequence": 2}),),
    )
    return (
        SegmentDescriptor.from_segment(build_segment((first,), codec=CodecProfile.raw())),
        SegmentDescriptor.from_segment(build_segment((second,), codec=CodecProfile.raw())),
    )


def _manifest(
    descriptors: tuple[SegmentDescriptor, ...],
    *,
    generation: int,
    parent: Hash32 | None,
):
    return build_manifest(
        store_id=_STORE,
        run_id=_RUN,
        generation=generation,
        parent_manifest_root=parent,
        run_identity=_ident(1),
        config_identity=_ident(2),
        code_identity=_ident(3),
        runtime_identity=_ident(4),
        start_prefix_root=_ZERO,
        segments=descriptors,
    )


def test_transition_accepts_exact_append_only_child() -> None:
    first, second = _descriptors()
    parent = _manifest((first,), generation=1, parent=None)
    child = _manifest((first, second), generation=2, parent=parent.identity.root)

    verify_manifest_transition(parent, child)


@pytest.mark.parametrize("case", ("skip", "wrong-parent", "replace", "identity", "no-append"))
def test_transition_rejects_chain_rewrite_or_identity_drift(case: str) -> None:
    first, second = _descriptors()
    parent = _manifest((first,), generation=1, parent=None)
    child = _manifest((first, second), generation=2, parent=parent.identity.root)

    if case == "skip":
        candidate = replace(child, generation=3)
    elif case == "wrong-parent":
        candidate = replace(child, parent_manifest_root=Hash32(b"\xff" * 32))
    elif case == "replace":
        replacement = replace(first, checkpoint_root=Hash32(b"\xaa" * 32))
        candidate = _manifest((replacement, second), generation=2, parent=parent.identity.root)
    elif case == "identity":
        candidate = replace(child, code_identity=_ident(9))
    else:
        candidate = _manifest((first,), generation=2, parent=parent.identity.root)

    with pytest.raises(ManifestFormatError):
        verify_manifest_transition(parent, candidate)
