"""Domain-separated Merkle trees for Storage V4 commit digests.

``merkle_root`` receives raw :class:`Hash32` commit digests.  Each digest is
first framed under ``HL4-MERKLE-LEAF``; internal pairs are framed under
``HL4-MERKLE-NODE``.  If a level has an odd width, its final node is duplicated
at that level.  The rule is applied again at every higher odd-width level.  The
inner root and exact leaf count are finally bound under ``HL4-MERKLE-ROOT``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .canonical import (
    DOMAIN_MERKLE_LEAF,
    DOMAIN_MERKLE_NODE,
    DOMAIN_MERKLE_ROOT,
    frame_bytes,
    frame_hash32,
    frame_u32,
    framed_hash,
)
from .types import UINT32_MAX, Hash32

MERKLE_LEAF_DOMAIN = DOMAIN_MERKLE_LEAF
MERKLE_NODE_DOMAIN = DOMAIN_MERKLE_NODE
MERKLE_ROOT_DOMAIN = DOMAIN_MERKLE_ROOT
_HASH_SIZE = 32


@dataclass(frozen=True, slots=True)
class MerkleProof:
    """A strict sibling path tied to one leaf index and total leaf count."""

    leaf_index: int
    leaf_count: int
    siblings: tuple[Hash32, ...]


def _require_hash(value: Hash32, *, name: str) -> None:
    if type(value) is not Hash32 or len(bytes(value)) != _HASH_SIZE:
        raise ValueError(f"{name} must be a Hash32 containing {_HASH_SIZE} bytes")


def _leaf_hash(commit_digest: Hash32) -> Hash32:
    _require_hash(commit_digest, name="commit digest")
    return framed_hash(MERKLE_LEAF_DOMAIN, bytes(commit_digest))


def _node_hash(left: Hash32, right: Hash32) -> Hash32:
    _require_hash(left, name="left node")
    _require_hash(right, name="right node")
    return framed_hash(MERKLE_NODE_DOMAIN, bytes(left), bytes(right))


def _root_hash(inner_root: Hash32, leaf_count: int) -> Hash32:
    _require_hash(inner_root, name="inner root")
    if type(leaf_count) is not int or leaf_count < 0 or leaf_count > UINT32_MAX:
        raise ValueError("leaf_count is outside uint32")
    return framed_hash(
        MERKLE_ROOT_DOMAIN,
        frame_u32(leaf_count),
        frame_hash32(inner_root),
    )


def _require_leaf_index(leaf_index: int, leaf_count: int) -> None:
    if type(leaf_index) is not int:
        raise ValueError("leaf_index must be an integer")
    if leaf_index > UINT32_MAX:
        raise ValueError("leaf_index is outside uint32")
    if leaf_index < 0 or leaf_index >= leaf_count:
        raise ValueError("leaf_index is outside the tree")


def _proof_length(leaf_count: int) -> int:
    if type(leaf_count) is not int:
        raise ValueError("leaf_count must be an integer")
    if leaf_count < 1 or leaf_count > UINT32_MAX:
        raise ValueError("leaf_count must be between 1 and uint32 maximum")

    length = 0
    width = leaf_count
    while width > 1:
        length += 1
        width = (width + 1) // 2
    return length


def _parent_level(level: Sequence[Hash32]) -> list[Hash32]:
    parents: list[Hash32] = []
    for offset in range(0, len(level), 2):
        left = level[offset]
        right = level[offset + 1] if offset + 1 < len(level) else left
        parents.append(_node_hash(left, right))
    return parents


def merkle_root(leaves: Sequence[Hash32]) -> Hash32:
    """Return the Merkle root for raw commit digests.

    The empty root hashes one explicitly framed present-empty byte string.
    A single commit digest is still leaf-domain-separated before becoming
    the root.
    """

    snapshot = tuple(leaves)
    leaf_count = len(snapshot)
    if leaf_count > UINT32_MAX:
        raise ValueError("Merkle leaf count exceeds uint32")
    if leaf_count == 0:
        return _root_hash(
            framed_hash(MERKLE_LEAF_DOMAIN, frame_bytes(b"")),
            0,
        )

    level = [_leaf_hash(leaf) for leaf in snapshot]
    while len(level) > 1:
        level = _parent_level(level)
    return _root_hash(level[0], leaf_count)


def build_inclusion_proof(
    leaves: Sequence[Hash32], leaf_index: int
) -> MerkleProof:
    """Build the canonical sibling path for ``leaves[leaf_index]``."""

    snapshot = tuple(leaves)
    leaf_count = len(snapshot)
    if leaf_count > UINT32_MAX:
        raise ValueError("Merkle leaf count exceeds uint32")
    _require_leaf_index(leaf_index, leaf_count)

    level = [_leaf_hash(leaf) for leaf in snapshot]
    index = leaf_index
    siblings: list[Hash32] = []
    while len(level) > 1:
        sibling_index = (
            index - 1
            if index % 2
            else index + 1
            if index + 1 < len(level)
            else index
        )
        siblings.append(level[sibling_index])

        level = _parent_level(level)
        index //= 2

    return MerkleProof(
        leaf_index=leaf_index,
        leaf_count=leaf_count,
        siblings=tuple(siblings),
    )


def verify_inclusion_proof(
    leaf: Hash32, proof: MerkleProof, expected_root: Hash32
) -> bool:
    """Verify an inclusion proof while rejecting non-canonical structures.

    Invalid metadata or hash sizes and truncated or overlong paths raise
    ``ValueError``.  A structurally valid proof containing the wrong index,
    sibling hash, leaf or expected root returns ``False``.
    """

    if type(proof) is not MerkleProof:
        raise ValueError("proof must be a MerkleProof")
    _require_hash(leaf, name="commit digest")
    _require_hash(expected_root, name="expected_root")

    required_length = _proof_length(proof.leaf_count)
    _require_leaf_index(proof.leaf_index, proof.leaf_count)
    if type(proof.siblings) is not tuple:
        raise ValueError("proof siblings must be a tuple")
    if len(proof.siblings) != required_length:
        raise ValueError(
            f"proof length must be {required_length}, got {len(proof.siblings)}"
        )
    for sibling in proof.siblings:
        _require_hash(sibling, name="proof sibling")

    node = _leaf_hash(leaf)
    index = proof.leaf_index
    width = proof.leaf_count
    for sibling in proof.siblings:
        if index % 2:
            node = _node_hash(sibling, node)
        elif index + 1 < width:
            node = _node_hash(node, sibling)
        else:
            # Duplicate-last is part of the canonical proof, not an arbitrary
            # sibling supplied by the prover.
            if sibling != node:
                return False
            node = _node_hash(node, node)
        index //= 2
        width = (width + 1) // 2

    return _root_hash(node, proof.leaf_count) == expected_root


__all__ = [
    "MERKLE_LEAF_DOMAIN",
    "MERKLE_NODE_DOMAIN",
    "MERKLE_ROOT_DOMAIN",
    "MerkleProof",
    "build_inclusion_proof",
    "merkle_root",
    "verify_inclusion_proof",
]
