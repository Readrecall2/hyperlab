# HyperLab Storage v4 immutable prototype format (protocol v1)

This document specifies the Phase 1A immutable binary core and the Phase 1B
checkpointed engine and recovery contract. Unless a section explicitly describes
SQLite or canonical JSON, all integers are unsigned, big-endian, and fixed-width
(`u16`, `u32`, or `u64`). Text is strict UTF-8 and is prefixed by a `u32` byte
length. Optional values use a one-byte tag: `0x00` means absent and `0x01` means
present. A present variable-length value still carries its length, so absent and
present-empty are distinct. Hash values stored in the binary formats are exactly
32 raw bytes; lowercase hexadecimal is only an external view.

## Logical hashes

The Phase 1A protocol domains are `HL4-ROW`, `HL4-COMMIT`, `HL4-PREFIX`,
`HL4-MERKLE-LEAF`, `HL4-MERKLE-NODE`, `HL4-MERKLE-ROOT`, `HL4-SEGMENT`,
and `HL4-MANIFEST`. Phase 1B adds `HL4-CHECKPOINT` and
`HL4-CANDIDATE-SEGMENT-DESCRIPTORS`.
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

An authenticated manifest file alone still does not prevent rollback of an entire
self-consistent chain. Phase 1B combines the manifest chain with an independently
opened anchor and refuses a generation/root that does not match that authority.
The included local anchor is a functional witness of this protocol, not proof of
an externally protected or root-owned production authority.

## Phase 1B logical storage modes

The storage mode is part of the logical contract and the two modes are not
interchangeable:

- `V3_COMPATIBILITY_IMPORT` preserves a certified V3 canonical JSONL record so
  it can be rematerialized byte for byte.
- `V4_NATIVE` stores a typed reference to bytes in a raw immutable segment. It
  does not wrap or duplicate a V3 canonical record.

A compatibility import requires exactly one strict canonical JSON object followed
by exactly one LF. Missing LF, CRLF, extra lines, non-canonical JSON, non-object
values, and non-finite floats are rejected. The LF is not stored. The exact object
text is stored as a UTF-8 string and rematerialization appends exactly one LF. A
finite V3 float is thus preserved as its exact canonical JSON text inside that
string, not introduced as a float into the float-free V4 canonical logical value.

The exact compatibility envelope has keys `canonical_json`,
`canonical_sha256`, `contract`, and `mode`. The hash is lowercase SHA-256 of
the exact UTF-8 object bytes; `contract` is
`hyperlab.storage_v4.v3_compatibility_record.v1`; and `mode` is
`V3_COMPATIBILITY_IMPORT`.

The certified importer owns one cursor per ordered stream and drains only rows
for the current commit. It never groups a complete component stream in memory.
Checkpoint materialization is lazy and occurs only at an actual seal boundary.
Ledger balances use exact integer-coefficient/decimal-exponent accumulation;
there is no Decimal context precision or silent rounding ceiling.

A native reference has exactly the keys `byte_length`, `byte_offset`,
`contract`, `lake_id`, `mode`, `payload_sha256`, `physical_sha256`,
`segment_identity`, `source_first_sequence`, `source_last_sequence`, and
`stream_id`. Its contract is
`hyperlab.storage_v4.raw_segment_reference.v1` and its mode is `V4_NATIVE`.
Resolution authenticates the segment key, complete physical hash, nonempty
in-bounds interval, payload hash, stream, and nonreversed source range before
returning bytes.

`DeterministicRawLakeEmulator` is an in-memory one-shot resolver for local tests.
It rejects duplicate registration, physical aliases, bad bounds, and hash
mismatches. It is not a durable raw lake, capacity implementation, or ownership
witness.

## Bounded SQLite overlay

The overlay stores only the complete mutable tail after an authenticated base.
`create` refuses an existing path and `open_existing` refuses a missing path;
expected state is never silently created. Schema identity and `user_version` are
checked on open. Every durable connection requires SQLite
`journal_mode=DELETE` and `synchronous=FULL`.

Schema v2 metadata binds store ID, run ID, storage mode, the four
run/config/code/runtime identities, codec profile, immutable genesis
generation/root/commit/prefix, seal thresholds, current base manifest
generation/root/commit/prefix, tail commit/row/stored-byte counters, and head.
Open requires the complete expected `OverlayIdentity` before any retained tail
is read. A schema-v1 overlay is deliberately ambiguous and rejected rather than
silently upgraded.
Protocol `u64` values are stored as zero-padded 20-character decimal text, which
avoids SQLite's signed-integer limit while preserving lexical order.

Before the first manifest, the only valid base-manifest sentinel is generation
zero with the all-zero 32-byte root. Generation zero with a nonzero root and a
positive generation with the zero root are corrupt. The sentinel is overlay
metadata only, not a fake manifest, checkpoint, commit, or anchor record;
published manifests and anchors start at generation 1.

Append and `advance_base` use `BEGIN IMMEDIATE`. Append reads the transactional
metadata written with the prior commit, then requires the same run, exact next
sequence, exact prior prefix, and a complete authenticated commit frame. An exact
duplicate returns false.
Conflicting duplicate/overlap, gap, wrong run/root, malformed record, overflow,
or counter mismatch rolls back with structured `OverlayError`.

`StorageRepository.overlay_state` is an O(1) seal-polling view of that same
transactional metadata; it does not rescan the retained tail after every append.
Complete tail decoding and reconciliation remain mandatory at seal, startup/
recovery, and full audit.

The tail is seal-ready when either `seal_rows` logical rows or `seal_bytes`
stored encoded bytes is reached. These are admission thresholds, not capacity
measurements. `advance_base` validates the new base, deletes only its covered
prefix, proves and preserves any contiguous tail, recomputes counters, and updates
metadata atomically. Exact repetition is idempotent; rollback, fork, gap, or
incompatible tail fails closed.

## Checkpoint and descriptor-set digest

A checkpoint is magic `HL4CHK\x00\x01`, format `u16(1)`, body size
`u64`, body, then raw checkpoint root. The root is exactly
`framed_hash(HL4-CHECKPOINT, frame_bytes(body))`.

The body contains, in order: logical protocol version; store ID, run ID, and mode;
target manifest generation and optional parent; starting prefix; covered commit
sequence, prefix, and segment identity; candidate descriptor digest; run/config/
code/runtime identities; historical commit count; sorted cumulative stream counts
using `u64`; then framed strict canonical JSON objects for adapter, ledger,
projection, sessions, incidents, cursors, and stream heads, in that exact order.
Generation 1 has no parent; later generations require one. All bindings, counts,
identities, covered head, generation, parent, and mode are checked, so stale,
future, cross-run, or partial matches are rejected.

The candidate descriptor digest is
`framed_hash(HL4-CANDIDATE-SEGMENT-DESCRIPTORS,`
`frame_bytes(u32(count) || each(frame_bytes(descriptor_material))))`. Prior
descriptors retain their checkpoint-root option; the new final descriptor is
digested with that option absent. The resulting checkpoint root is then inserted
into the final descriptor. This two-stage rule breaks the circular dependency
between a checkpoint root and the descriptor that names it.

## Manifest authority and publication

A valid manifest transition is exactly parent generation plus one, cites the
exact parent root, retains store/run and run/config/code/runtime identities and
starting prefix, retains every parent descriptor byte for byte, and appends at
least one contiguous prefix-linked descriptor. Rewriting history, skipping a
generation, or presenting a same-generation fork is rejected.

Segments are content-addressed by physical SHA-256 with suffix `.hl4s`,
checkpoints by checkpoint root with `.hl4c`, and manifests by manifest root with
`.hl4m`. Immutable publication uses a fresh exclusive temporary file, flush,
file fsync, exact readback, non-overwriting publication, and directory fsync.
An existing byte-identical object is idempotent; divergent bytes at that address
are corruption.

Seal publication has this exact causal order:

1. immutable segment;
2. complete checkpoint after computing the candidate descriptor digest;
3. append-only manifest after inserting the checkpoint root;
4. authoritative external anchor compare-and-swap;
5. mutable `CURRENT` cache;
6. `overlay.advance_base`.

The anchor, not `CURRENT`, decides the committed head. Anchor updates are
monotone compare-and-swap: exact repetition is idempotent, but stale expectation,
rollback, or fork is rejected. Any generation jump is accepted only after the
repository verifies the intervening manifest chain.

One repository instance first acquires the anchor/store-scoped OS advisory
writer lease and then the repository-root `WRITER.LEASE`; both remain held from
create/open until `close()`, and are released in reverse order. Two different
repository roots attached to the same anchor/store authority therefore cannot
write concurrently. A second writer fails immediately; process exit releases
the OS locks and harmless lease files may remain. The local implementation binds
its sidecar to the canonical anchor path and `StoreId`, and refuses symlinks,
Windows reparse points, replacement, or divergent lease identity. These leases
are concurrency admission, not root-owned external authority, and do not replace
anchor compare-and-swap.

`CURRENT` is strict canonical JSON plus LF with `format_version`,
`generation`, `manifest_root`, and `store_id`. It is only a replaceable
cache. Missing, stale, or corrupt `CURRENT` is repaired from the anchor after
the exact manifest and checkpoint authenticate; a newer or conflicting cache
never overrides the anchor. Genesis has no `CURRENT`, and symlinks are not
followed.

## Startup, recovery, and full audit

Normal anchored startup follows this order: exact anchor, exact anchored manifest,
checkpoint bound by its final descriptor, verified/reconciled overlay tail, then
`CURRENT` repair. It reads no historical segment payload. The report exposes
`segments_read = 0`, `checkpoint_used`,
`historical_segments_not_read`, `historical_commits_not_read`,
`historical_rows_not_read`, `tail_entries_replayed`,
`tail_rows_replayed`, and the final integrity result.

Reopen also inspects only the bounded identity/generation/parent prefix of each
manifest namespace file to discover a possible direct interrupted-seal
successor; it does not parse every cumulative historical manifest. That O(N)
metadata prefix work is in the same asymptotic bound as the N descriptors in
the current cumulative manifest. A clean reopen still reads zero segment
payloads. If one exact orphan successor is adopted, the report truthfully counts
its one segment and covered commits/rows as read and subtracts them from the
`historical_*_not_read` counters.

The phrase "startup O(tail)" is scoped to historical commit/row payload I/O and
replay. Exact parser work is
`O(size(anchored manifest) + size(checkpoint) + size(tail))`. Protocol-v1
manifests repeat the descriptor prefix, so Phase 1B does not claim constant
startup work in the number of historical segment descriptors.

If the overlay trails the anchor, recovery advances it only after proving that its
base is the strict genesis sentinel or an ancestor in the authenticated chain. An
overlay ahead of the anchor, on a fork, with a bad attachment, or with mismatched
counters is rejected. A crash after anchor publication but before `CURRENT` or
overlay advancement is recoverable without granting authority to either later
write.

`full_audit` is the separate O(N) path. It authenticates the entire manifest
chain, detects same-generation forks and invalid manifest namespace
entries, verifies every generation checkpoint, reads every segment, and reconciles
physical/logical identities, prefix chains, commits, rows, bytes, stream totals,
and every persisted checkpoint state. Its ordered
`CheckpointStateWitness(covered_commit_sequence, state_sha256)` values are
derived from checkpoint bytes actually reread during that audit, not from
in-memory states retained by the sealer. Normal startup does not imply this
full-history audit.

## Fault injection and fail-closed behavior

Deterministic fault points surround temporary writes, flushes, file and directory
fsyncs, exclusive publication/rename, segment/checkpoint/manifest publication,
anchor and `CURRENT` publication, and overlay transactions before and after
commit. An injected failure is raised at its selected occurrence. Reopen must
resolve to the last authenticated anchored generation plus a valid tail or fail
closed. Unanchored immutable objects and orphan temporary files have no authority;
exact immutable publication and overlay operations remain safely retryable. One
special interrupted-seal case is recoverable: if the namespace contains exactly
one fully authenticated direct manifest successor whose sole appended segment
matches an exact prefix of the durable overlay tail, reopen may compare-and-swap
that successor into the anchor and advance the overlay while preserving any
contiguous suffix. Invalid namespace entries, zero/multiple candidates, a fork,
wrong generation/parent, extra descriptors, bad segment/checkpoint bytes, or a
tail mismatch fail closed.

## Golden Phase 1B certification provenance

The compatibility certifier keeps the authenticated Golden source identities
separate from the identities of the code that creates the Storage v4 store.
Before creating a result directory and again immediately before report
publication, it recomputes the canonical multistrategy Paper release-code and
runtime-environment digests and requires exact equality with explicit CLI
inputs. The repository manifest/checkpoint/overlay `code_identity` and
`runtime_identity` are those certifier identities, while `config_identity` is
SHA-256 of the canonical configuration payload. That payload binds the Golden
root/source/run and source config/code/runtime identities, mode, store ID, codec,
seal thresholds, heartbeat/safety limits, and certifier code/runtime.

The immutable report publishes the Golden and certifier namespaces separately,
including the complete canonical configuration payload and digest. `COMPLETE`
repeats certifier code/configuration/runtime digests plus the report digest.
The progress log remains `RUNNING` through its final durable
`certification_gates_passed` record and is closed first; `COMPLETE` v2 is the
last publication and the only persisted success state.
The external Golden pin is hashed through a stable regular-file handle and
stable device/inode/mode/size/mtime witness around both Golden verifications.
Peak RSS is either measured by the platform process API or explicitly reported
`UNAVAILABLE` with a reason; a missing value is never treated as capacity proof.

## Evidence and platform limits

Phase 1B is Paper-only. It introduces no signer, wallet, private API, live-order
path, deployment, economic result, or real-money certification.

SQLite `LocalAnchor` is a local functional witness of schema, `DELETE/FULL`,
compare-and-swap, and recovery. It does not prove Linux root ownership, resistance
to a compromised administrator, remote attestation, or an operational external
authority. Local Windows evidence exercises functional logic and the Windows
directory-flush path; it does not certify Linux/ext4 durability. POSIX fsync code
likewise does not prove that a production Linux host was configured or tested.

`V3_COMPATIBILITY_IMPORT` carries exact V3 canonical text, so its measured size
is not V4-native capacity evidence. Durable raw-lake ingestion, representative
`V4_NATIVE` capacity, long-run growth, compaction, an external/root-owned
anchor, and Linux crash/power-loss evidence remain future work. This format makes
no `0.20 GiB/h` claim, no throughput or storage-capacity certification, and no
economic claim.
