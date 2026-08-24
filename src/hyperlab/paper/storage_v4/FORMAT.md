# HyperLab Storage v4 immutable prototype format (protocol v1)

This document specifies the Phase 1A binary contract. All integers are unsigned,
big-endian, and fixed-width (`u16`, `u32`, or `u64`). Text is strict UTF-8 and is
prefixed by a `u32` byte length. Optional values use a one-byte tag: `0x00` means
absent and `0x01` means present. A present variable-length value still carries its
length, so absent and present-empty are distinct. Hash values stored in the binary
formats are exactly 32 raw bytes; lowercase hexadecimal is only an external view.

## Logical hashes

The protocol domains are `HL4-ROW`, `HL4-COMMIT`, `HL4-PREFIX`,
`HL4-MERKLE-LEAF`, `HL4-MERKLE-NODE`, `HL4-MERKLE-ROOT`, `HL4-SEGMENT`,
and `HL4-MANIFEST`.
Every logical preimage starts with the length-prefixed domain followed by protocol
version `u16(1)`. Variable data is length-prefixed before it is appended. SHA-256
is then applied to the complete preimage. The shared `framed_hash(D, F...)`
primitive is exactly:

`SHA256(frame_domain(D) || u32(field_count) || each(u32(len(F)) || F))`.

This outer field framing is always present, including for zero fields and one
present-empty field. Schema-specific fields may themselves already be fixed-width
or length-prefixed.

A row commits to its stream ID, per-stream commit ordinal, and strict canonical
JSON value. A commit commits to its run ID, sequence, sorted per-stream counts,
ordered row hashes, previous prefix root, and explicitly tagged optional V3 legacy
identity. The prefix root commits to the previous prefix root, sequence, and commit
digest. Consequently the prefix chain detects omissions, insertions, duplicates,
substitutions, and reordering.

The exact protocol-v1 field lists passed to the common primitive are:

- row: `text(stream_id)`, `u32(ordinal)`, `bytes(canonical_json)`;
- commit: `text(run_id)`, `u64(sequence)`, `u32(stream_entry_count)`, then each
  sorted `text(stream_id)`, `u32(count)`, then `u32(row_hash_count)`, each ordered
  raw row hash, the raw previous prefix root, and `optional_hash32(legacy)`;
- prefix: `text(run_id)`, `u64(sequence)`, raw previous prefix root, raw commit
  digest, and `optional_hash32(legacy)`;
- segment identity: `text(run_id)`, `u64(first)`, `u64(last)`, raw start/end
  prefix roots, raw Merkle root, `u32(commit_count)`, one encoded aggregate-count
  table, `u32(digest_count)`, then every ordered raw commit digest;
- manifest root: one field equal to `bytes(manifest_body)`, under `HL4-MANIFEST`.

Here `text(x) = u32(UTF8_size) || UTF8(x)`, `bytes(x) = u32(size) || x`, and an
encoded count table is `u32(entry_count)` followed by sorted `text(stream_id) ||
u32(count)` entries. All hashes named raw above are exactly 32 bytes. The common
outer field count/length framing still surrounds every listed field.

Merkle inputs are commit digests. A digest is first hashed under
`HL4-MERKLE-LEAF`; internal pairs are hashed left-to-right under
`HL4-MERKLE-NODE`. The empty inner root hashes one explicitly framed empty byte
string under the leaf domain. At every level an unpaired final node is duplicated.
The externally compared root is then finalized as
`framed_hash(HL4-MERKLE-ROOT, u32(leaf_count), inner_root32)`. This binds the exact
leaf count, including the otherwise ambiguous `[A,B,C]` versus `[A,B,C,C]` case.
Inclusion proofs carry the original uint32 leaf count and index, so wrong counts,
truncated, extra, or structurally impossible paths are rejected.

## Commit frames inside blocks

Each complete commit is encoded as one indivisible frame:

| Field | Encoding |
| --- | --- |
| magic | four bytes `HL4C` |
| frame version | `u16(1)` |
| body size | `u64` |
| commit sequence | `u64` |
| previous prefix root | 32 bytes |
| optional legacy identity | tag, then 32 bytes when present |
| stream-count table | `u32` entries, each UTF-8 stream ID + `u32` count, sorted by UTF-8 bytes |
| rows | `u32` rows in commit order |
| each row | UTF-8 stream ID, `u32` ordinal, 32-byte row hash, `u32` canonical-value size, value bytes |
| commit digest | 32 bytes |
| resulting prefix root | 32 bytes |

The logical frame size includes magic, version, body-size field, and body. Blocks
contain a concatenation of whole frames only. If one frame exceeds the configured
normal block limit, that frame occupies a block by itself.

## Segment file

The fixed 40-byte segment prefix is:

| Field | Size |
| --- | ---: |
| magic `HL4SEG\x00\x01` | 8 |
| format version | 2 |
| header version | 2 |
| codec ID (`0` raw, `1` zlib) | 1 |
| codec parameter (`0` raw, `1..9` zlib level) | 1 |
| flags, must be zero | 2 |
| header size | 4 |
| block count | 4 |
| total logical frame bytes | 8 |
| complete physical file size | 8 |

The variable binary header stores protocol version, run ID, first/last commit
sequences, starting/ending prefix roots, Merkle root, logical segment identity,
commit count, and the sorted aggregate stream-count table.
Its exact order is:

| Variable header field | Encoding |
| --- | --- |
| logical protocol version | `u16(1)` |
| run ID | text |
| first / last commit sequence | `u64` + `u64` |
| start / end prefix root | 32 + 32 bytes |
| Merkle root | 32 bytes |
| logical segment identity | 32 bytes |
| commit count | `u32` |
| aggregate stream counts | count table |

Each block begins with a fixed 76-byte header:

| Field | Size |
| --- | ---: |
| magic `HL4B` | 4 |
| block index | 4 |
| first/last commit sequence | 8 + 8 |
| commit count | 4 |
| uncompressed logical size | 8 |
| encoded payload size | 8 |
| SHA-256 of encoded payload | 32 |

The footer begins with magic `HL4FTR\x00\x01` and contains footer version, flags,
block count, logical size, complete physical size, SHA-256 of the variable header,
SHA-256 of every preceding file byte, block-offset count, and ordered `u64` block
offsets. It ends with a footer SHA-256, `u64` footer size, and magic
`HL4END\x00\x01`. The footer digest covers the footer fields, its declared size, and
end magic. The trailing size and magic permit end-first discovery and detect
truncation. The independently calculated physical identity is SHA-256 of the
complete file and is deliberately not self-embedded.

In exact order the footer is: 8-byte footer magic, `u16(1)`, zero `u16` flags,
`u32(block_count)`, `u64(logical_size)`, `u64(physical_size)`, 32-byte variable-
header SHA-256, 32-byte SHA-256 of all bytes preceding the footer,
`u32(offset_count)`, each ordered `u64` block offset, 32-byte footer checksum,
`u64(footer_size)`, and 8-byte end magic. Its size is `148 + 8 * block_count`.
The footer checksum input is every footer byte through the final offset followed
by the trailing `u64(footer_size)` and end magic; it excludes the checksum field.

The logical segment identity covers run/range, starting and ending prefix roots,
Merkle root, commit count, aggregate stream counts, and all ordered commit digests.
It excludes logical/physical frame sizes, frame version, codec, compression level,
blocks, offsets, encoded sizes, filenames, and physical SHA-256.

The Phase 1A reader also applies configurable fail-closed allocation limits before
large work. Its conservative defaults are 256 MiB file/logical, 16 MiB header,
128 MiB per encoded/decoded block, 65,536 blocks, and 1,000,000 commits. These are
parser safety limits, not the future 64 MiB segment admission gate or a measured
production sizing claim.

## Manifest file

A manifest is `HL4MAN\x00\x01`, format version `u16(1)`, body size `u64`, the binary
body, then its 32-byte `HL4-MANIFEST` root. The body stores store/run IDs,
generation, explicitly tagged parent root, four opaque 32-byte run/config/code/
runtime identities, starting prefix root, ordered segment descriptors, and final
head. Each descriptor stores logical segment identity, run/range, prefix and
Merkle roots, physical SHA-256, logical/physical sizes, commit and stream counts,
codec profile, and an explicitly tagged optional checkpoint root.

The exact manifest body order is:

| Body field | Encoding |
| --- | --- |
| logical protocol version | `u16(1)` |
| store ID / run ID | text + text |
| generation | `u64` |
| parent manifest root | optional raw hash (`0x00`, or `0x01` + 32 bytes) |
| run / config / code / runtime identities | four raw 32-byte hashes |
| starting prefix root | 32 bytes |
| segment count | `u32` |
| each ordered descriptor | `u32(descriptor_size)` + descriptor bytes |
| final head | `u64(sequence)` + 32-byte prefix root + 32-byte segment identity |

Each descriptor byte string is, in order: 32-byte logical segment identity,
text run ID, `u64(first)`, `u64(last)`, 32-byte start prefix, 32-byte end prefix,
32-byte Merkle root, 32-byte physical SHA-256, `u64(physical_size)`,
`u64(logical_size)`, `u32(commit_count)`, an aggregate count table, text codec
profile, and an optional raw checkpoint hash. The stored root is exactly
`framed_hash(HL4-MANIFEST, bytes(manifest_body))` using the common grammar above.

Generation 1 is the only parentless generation. Later generations require a
parent manifest root. Segment ranges must be nonempty, ordered, contiguous, from
one run, and prefix-linked. The final head must equal the last descriptor.

The structural manifest verifier authenticates and validates those declarations.
When segment bytes are supplied, `verify_manifest_segments` independently reads
each segment and requires an exact descriptor match, including its logical
identity, prefix and Merkle roots, physical hash, sizes, counts, and codec
profile. Without the corresponding segment bytes, a standalone manifest can
authenticate only its declared descriptors, not prove that they describe any
particular external file.

The manifest reader applies configurable pre-allocation limits before copying a
body or materializing descriptors. Phase 1A defaults are 64 MiB for the complete
file and body, 1 MiB per descriptor, and 65,536 descriptors. These are parser
safety limits rather than a production manifest sizing claim.

Phase 1A does **not** publish or verify an external/root-owned anchor. Therefore a
valid standalone manifest detects internal corruption and a supplied wrong parent,
but does not prevent an attacker from rolling the whole self-consistent manifest
chain back. Anti-rollback publication is explicitly outside this prototype.
