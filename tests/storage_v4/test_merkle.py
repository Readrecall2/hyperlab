from __future__ import annotations

from collections.abc import Iterator, Sequence

import pytest

from hyperlab.paper.storage_v4.canonical import frame_bytes, frame_u32, framed_hash
from hyperlab.paper.storage_v4.merkle import (
    MerkleProof,
    build_inclusion_proof,
    merkle_root,
    verify_inclusion_proof,
)
from hyperlab.paper.storage_v4.types import Hash32

SYNTHETIC_STORAGE_V4_WORKLOAD = True

_LEAF_DOMAIN = b"HL4-MERKLE-LEAF"
_NODE_DOMAIN = b"HL4-MERKLE-NODE"
_ROOT_DOMAIN = b"HL4-MERKLE-ROOT"


def _commit_digest(value: bytes) -> Hash32:
    return framed_hash(b"HL4-TEST-COMMIT", frame_bytes(value))


def _leaf_node(commit_digest: Hash32) -> Hash32:
    return framed_hash(_LEAF_DOMAIN, bytes(commit_digest))


def _node(left: Hash32, right: Hash32) -> Hash32:
    return framed_hash(_NODE_DOMAIN, bytes(left), bytes(right))


def _root(leaf_count: int, inner_root: Hash32) -> Hash32:
    return framed_hash(_ROOT_DOMAIN, frame_u32(leaf_count), bytes(inner_root))


class _DivergentSequence(Sequence[Hash32]):
    def __init__(self, values: tuple[Hash32, ...]) -> None:
        self._values = values

    def __len__(self) -> int:
        return 1

    def __getitem__(self, index: int) -> Hash32:
        return self._values[index]

    def __iter__(self) -> Iterator[Hash32]:
        return iter(self._values)

    def __bool__(self) -> bool:
        return False


class _SiblingTuple(tuple[Hash32, ...]):
    pass


class _ProofSubclass(MerkleProof):
    pass


def test_empty_tree_has_domain_separated_root() -> None:
    root = merkle_root([])
    empty_leaf = framed_hash(_LEAF_DOMAIN, frame_bytes(b""))

    assert root == _root(0, empty_leaf)
    assert empty_leaf != framed_hash(_LEAF_DOMAIN)


def test_single_leaf_tree_domain_separates_the_commit_digest() -> None:
    commit_digest = _commit_digest(b"only")

    assert merkle_root([commit_digest]) == _root(1, _leaf_node(commit_digest))


def test_root_and_proof_snapshot_a_divergent_sequence_once() -> None:
    leaves = tuple(_commit_digest(value) for value in (b"a", b"b", b"c"))
    divergent = _DivergentSequence(leaves)

    assert merkle_root(divergent) == merkle_root(leaves)
    proof = build_inclusion_proof(divergent, 2)
    assert proof.leaf_count == len(leaves)
    assert verify_inclusion_proof(leaves[2], proof, merkle_root(leaves))


def test_even_tree_hashes_pairs_left_to_right() -> None:
    leaves = [_commit_digest(value) for value in (b"a", b"b", b"c", b"d")]
    nodes = [_leaf_node(leaf) for leaf in leaves]
    expected = _node(_node(nodes[0], nodes[1]), _node(nodes[2], nodes[3]))

    assert merkle_root(leaves) == _root(4, expected)


def test_odd_node_is_duplicated_at_every_level() -> None:
    leaves = [
        _commit_digest(value) for value in (b"a", b"b", b"c", b"d", b"e")
    ]
    nodes = [_leaf_node(leaf) for leaf in leaves]
    level_one = [
        _node(nodes[0], nodes[1]),
        _node(nodes[2], nodes[3]),
        _node(nodes[4], nodes[4]),
    ]
    level_two = [
        _node(level_one[0], level_one[1]),
        _node(level_one[2], level_one[2]),
    ]

    assert merkle_root(leaves) == _root(5, _node(level_two[0], level_two[1]))


@pytest.mark.parametrize("leaf_count", [1, 2, 3, 4, 5, 8, 9])
def test_inclusion_proof_verifies_every_leaf(leaf_count: int) -> None:
    leaves = [
        _commit_digest(index.to_bytes(2, "big")) for index in range(leaf_count)
    ]
    root = merkle_root(leaves)

    for leaf_index, leaf in enumerate(leaves):
        proof = build_inclusion_proof(leaves, leaf_index)

        assert proof.leaf_index == leaf_index
        assert proof.leaf_count == leaf_count
        assert verify_inclusion_proof(leaf, proof, root)


def test_proof_rejects_wrong_leaf_index_root_and_hash() -> None:
    leaves = [
        _commit_digest(value) for value in (b"a", b"b", b"c", b"d", b"e")
    ]
    proof = build_inclusion_proof(leaves, 4)

    wrong_index = MerkleProof(
        leaf_index=3,
        leaf_count=proof.leaf_count,
        siblings=proof.siblings,
    )
    wrong_hash = MerkleProof(
        leaf_index=proof.leaf_index,
        leaf_count=proof.leaf_count,
        siblings=(_commit_digest(b"wrong"), *proof.siblings[1:]),
    )

    assert not verify_inclusion_proof(leaves[4], wrong_index, merkle_root(leaves))
    assert not verify_inclusion_proof(
        leaves[4], proof, _commit_digest(b"wrong-root")
    )
    assert not verify_inclusion_proof(leaves[4], wrong_hash, merkle_root(leaves))


def test_proof_rejects_truncated_and_extra_sibling_paths() -> None:
    leaves = [
        _commit_digest(value) for value in (b"a", b"b", b"c", b"d", b"e")
    ]
    proof = build_inclusion_proof(leaves, 4)
    truncated = MerkleProof(
        leaf_index=proof.leaf_index,
        leaf_count=proof.leaf_count,
        siblings=proof.siblings[:-1],
    )
    extra = MerkleProof(
        leaf_index=proof.leaf_index,
        leaf_count=proof.leaf_count,
        siblings=(*proof.siblings, _commit_digest(b"extra")),
    )

    with pytest.raises(ValueError, match="proof length"):
        verify_inclusion_proof(leaves[4], truncated, merkle_root(leaves))
    with pytest.raises(ValueError, match="proof length"):
        verify_inclusion_proof(leaves[4], extra, merkle_root(leaves))


@pytest.mark.parametrize("leaf_index", [-1, 3])
def test_build_proof_rejects_out_of_range_index(leaf_index: int) -> None:
    leaves = [_commit_digest(b"a"), _commit_digest(b"b"), _commit_digest(b"c")]

    with pytest.raises(ValueError, match="leaf_index"):
        build_inclusion_proof(leaves, leaf_index)


def test_verify_rejects_structurally_invalid_proof() -> None:
    leaf = _commit_digest(b"a")

    with pytest.raises(ValueError, match="leaf_count"):
        verify_inclusion_proof(
            leaf,
            MerkleProof(leaf_index=0, leaf_count=0, siblings=()),
            merkle_root([leaf]),
        )
    with pytest.raises(ValueError, match="leaf_index"):
        verify_inclusion_proof(
            leaf,
            MerkleProof(leaf_index=1, leaf_count=1, siblings=()),
            merkle_root([leaf]),
        )


def test_verify_rejects_proof_and_sibling_tuple_subclasses() -> None:
    leaves = (_commit_digest(b"a"), _commit_digest(b"b"))
    root = merkle_root(leaves)
    proof = build_inclusion_proof(leaves, 0)
    proof_subclass = _ProofSubclass(
        proof.leaf_index,
        proof.leaf_count,
        proof.siblings,
    )
    sibling_subclass = MerkleProof(
        proof.leaf_index,
        proof.leaf_count,
        _SiblingTuple(proof.siblings),
    )

    with pytest.raises(ValueError, match="MerkleProof"):
        verify_inclusion_proof(leaves[0], proof_subclass, root)
    with pytest.raises(ValueError, match="siblings"):
        verify_inclusion_proof(leaves[0], sibling_subclass, root)


def test_duplicate_last_root_and_proof_bind_the_exact_leaf_count() -> None:
    first, second, third = (
        _commit_digest(b"a"),
        _commit_digest(b"b"),
        _commit_digest(b"c"),
    )
    three = (first, second, third)
    four = (first, second, third, third)
    proof = build_inclusion_proof(three, 2)
    forged_count = MerkleProof(
        leaf_index=proof.leaf_index,
        leaf_count=4,
        siblings=proof.siblings,
    )

    assert merkle_root(three) != merkle_root(four)
    assert not verify_inclusion_proof(third, forged_count, merkle_root(three))


def test_proof_rejects_altered_leaf_and_leaf_order() -> None:
    leaves = [_commit_digest(value) for value in (b"a", b"b", b"c")]
    proof = build_inclusion_proof(leaves, 1)

    assert not verify_inclusion_proof(
        _commit_digest(b"altered"),
        proof,
        merkle_root(leaves),
    )
    assert merkle_root(leaves) != merkle_root(tuple(reversed(leaves)))


def test_proof_rejects_non_hash_leaf_root_and_sibling() -> None:
    leaves = [_commit_digest(b"a"), _commit_digest(b"b")]
    proof = build_inclusion_proof(leaves, 0)

    with pytest.raises(ValueError, match="commit digest"):
        verify_inclusion_proof(b"short", proof, merkle_root(leaves))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="expected_root"):
        verify_inclusion_proof(leaves[0], proof, b"short")  # type: ignore[arg-type]
    malformed = MerkleProof(
        leaf_index=proof.leaf_index,
        leaf_count=proof.leaf_count,
        siblings=(b"short",),  # type: ignore[arg-type]
    )
    with pytest.raises(ValueError, match="proof sibling"):
        verify_inclusion_proof(leaves[0], malformed, merkle_root(leaves))
