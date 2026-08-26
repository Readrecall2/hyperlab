# HyperLab Storage v4 immutable and native format (protocol v1)

This document specifies the Phase 1A immutable binary core and the Phase 1B
checkpointed engine and recovery contract, plus the Phase 1C native raw-reference
path and its offline certification tools. Unless a section explicitly describes
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

The Phase 1B prototype native-reference V1 envelope has exactly the keys
`byte_length`, `byte_offset`,
`contract`, `lake_id`, `mode`, `payload_sha256`, `physical_sha256`,
`segment_identity`, `source_first_sequence`, `source_last_sequence`, and
`stream_id`. Its contract is
`hyperlab.storage_v4.raw_segment_reference.v1` and its mode is `V4_NATIVE`.
Resolution authenticates the segment key, complete physical hash, nonempty
in-bounds interval, payload hash, stream, and nonreversed source range before
returning bytes. Phase 1C does not weaken or reinterpret this decoder; it uses the
separate V2 envelope specified below.

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
is not V4-native capacity evidence. For the Phase 1B certification described
above, durable raw-lake ingestion, representative `V4_NATIVE` capacity, long-run
growth, compaction, an external/root-owned anchor, and Linux crash/power-loss
evidence were outside scope. The Phase 1C contracts below are separate native
implementation and offline-measurement surfaces; they do not retroactively turn
Phase 1B compatibility size into capacity evidence or make an unconditional
`0.20 GiB/h`, throughput, storage-capacity, or economic claim.

## Phase 1C raw-reference V2

The native Phase 1C reference contract is
`hyperlab.storage_v4.raw_segment_reference.v2` with `format_version = 2` and
`mode = V4_NATIVE`. It is a locator and authentication envelope, never a copy of
the raw payload. Its exact canonical-object keys are:

- authority and origin: `raw_store_id`, `lake_id`, `source_id`, `venue_id`;
- segment authority: `segment_identity`, `segment_root`, `raw_manifest_root`,
  `physical_sha256`;
- record location: `record_id`, `byte_offset`, `stored_length`, `stored_sha256`;
- replay payload: `logical_payload_length`, `logical_payload_sha256`;
- provenance: `input_type`, `source_stream_id`, `source_first_sequence`,
  `source_last_sequence`, `arrival_sequence`, `source_timestamp`, and
  `received_timestamp`;
- decoding: `codec_id`, `codec_version`;
- discriminator fields: `contract`, `format_version`, and `mode`.

All keys are mandatory; optional text values are represented by JSON null rather
than an omitted key. Stored and logical lengths are positive `u64`-bounded exact
integers, hashes are lowercase SHA-256, the stored interval cannot overflow, and
the source range cannot be reversed. `RawSegmentRef` is an alias of the strict
`RawSegmentReferenceV2` type. Parsing requires the exact key set and does not
fall back to V1.

Resolution authenticates the store/lake and the manifest generation that first
published the segment descriptor, the logical segment identity and segment root,
the whole-file physical SHA-256, the record locator and stored interval, both
stored and logical payload hashes and lengths, metadata/provenance fields, and
the declared codec before returning logical bytes. The in-memory
`DeterministicRawLakeV2Emulator` is only a contract test double for raw codec V1;
it proves neither filesystem durability nor external authority.

## Native raw segments and chained manifests

`RawSegmentWriter` streams strictly increasing arrival sequences into one fresh,
bounded staging file. Record IDs are unique within a segment. Each record binds
strict canonical metadata, stored bytes, logical bytes, their independent hashes
and lengths, and the selected raw/zlib codec. Segment identity and the ordered
segment root cover the complete record sequence. The footer and index permit a
bounded reader to locate a record, while full-file SHA-256 supplies the physical
content address. Admission is bounded by explicit record, logical-payload,
physical-file, and single-payload limits; these are safety bounds, not throughput
or production-capacity claims.

Capacity accounting classifies the complete authenticated raw footer as
metadata/index, not as immutable raw record bytes. Its exact physical size is
`146 + 152 * record_count`: the fixed 146-byte footer envelope plus one
152-byte locator/index entry per record. The `raw_segments` category subtracts
that exact per-segment amount and `raw_index` adds it, so total physical bytes
remain invariant.

Published raw segments are named `<physical_sha256>.hl4r`. Raw manifests are
strict canonical JSON plus one LF, use contract
`hyperlab.storage_v4.raw_manifest.v1`, and are named `<manifest_root>.hl4rm`.
Each manifest binds the raw store, lake and configuration identities, a monotone
generation, its parent root, the complete cumulative tuple of segment
descriptors, and cumulative record/logical/stored/physical byte counts. A valid
transition is generation plus one, cites the exact parent, retains every prior
descriptor byte for byte, and appends exactly one contiguous raw segment.

The raw authority publication order is staged/verified segment, immutable
content-addressed segment, authenticated `PENDING`, immutable manifest, monotone
anchor compare-and-swap, removal of `PENDING`, then mutable `CURRENT` repair. On
reopen, an exact direct successor can be adopted and a committed pending marker
can be cleared; forks, rollback, replacement, aliases, unsafe links/reparse
points, missing objects, and divergent bytes fail closed. `CURRENT` is a
repairable cache only; it never grants authority.

Normal raw-store startup opens the anchored current manifest and recovery
metadata but reads zero historical segment payloads. Its parser work is
`O(size(latest cumulative manifest))`, therefore `O(S)` descriptors for `S`
segments, plus bounded recovery metadata. It is not constant in historical
segment count. Full raw audit authenticates the chain and every segment/record
and is `O(N)` in raw records and payload bytes. Because generation `g` repeats
all `g` cumulative descriptors, parsing every generation performs
`1 + ... + S = O(S^2)` aggregate descriptor work. The current
`DiskRawResolver` chain authentication has the same cumulative-manifest
`O(S^2)` aggregate bound when it must rebuild its authority map; this limitation
is explicit rather than hidden by an `O(tail)` label.

After authenticating the manifest chain, the disk resolver maps one descriptor
per generation, locates records by binary search over the selected segment's
ordered arrival index, and retains at most one verified segment summary. That
cache bound is one segment summary, not one record: its locator tuple is bounded
by the configured maximum records per segment. A changed previously verified
file, an orphan reference, or a reference to a descriptor not newly published by
the cited manifest is rejected.

## Native journal, checkpoint binding, and raw-first pipeline

The native checkpoint binding V2 wraps, but preserves exactly, the Paper adapter
snapshot. It binds `raw_store_id`, `raw_lake_id`, `raw_config_identity`,
`raw_generation`, `raw_manifest_root`, `raw_record_count`,
`raw_last_record_id`, and the ordered `raw_reference_prefix_root`. Unbinding
requires the exact adapter contract and optional expected binding; there is no
silent downgrade or repair.

Native inbox rows own ordinal zero and rematerialize through their V2 reference
to exactly one strict canonical JSON object plus LF. The resolved record must
belong to the outer run/commit/input identity and arrival sequence. Other rows
remain direct Paper logical rows. Rechaining verifies source and native prefix
continuity, forbids a raw record from being referenced by more than one commit,
and requires the replacement payload to equal the certified compatibility
payload byte for byte. Streaming audit checks commit/row counts, stream digests,
final prefix, raw-reference count and ordered prefix, last record, and the allowed
manifest roots against explicit `NativeAuditExpectations`.

`Phase1CWriter` processes a caller-bounded batch in this causal order:

1. validate source ownership, sequence and cursor continuity;
2. stream and seal all required raw segment artifacts;
3. durably publish raw segments/manifests and advance raw authority;
4. inject the authenticated V2 references and rechain Paper commits;
5. append those commits to the Paper overlay;
6. at a requested seal, bind the raw authority into the Paper checkpoint and
   publish the Paper checkpoint/manifest/anchor.

Thus Paper never gains a reference before the corresponding raw authority is
durable. A failure after raw publication but before Paper append yields the
explicit `RAW_VALID_PAPER_ABSENT` or `RAW_VALID_PAPER_TAIL` state, not fabricated
alignment. Once an append attempt crosses that boundary and fails, the writer is
poisoned and refuses reuse; the incomplete candidate must be quarantined rather
than relabelled complete.

`inspect_phase1c_alignment` distinguishes empty, aligned, raw-ahead, raw-only,
and invalid Paper-without-raw states. `certify_phase1c_reopen` first proves normal
startup used the authenticated checkpoint plus exactly the expected bounded
tail, with zero historical raw segments and zero historical Paper segments read.
It then runs the separate exhaustive raw, Paper and native audits. Consequently
the startup claim is limited to
`O(latest manifest metadata + checkpoint + bounded tail)`; the certification
audit remains offline `O(N)` and inherits the cumulative raw-manifest `O(S^2)`
descriptor limitation described above.

## Phase 1C crash and quarantine contract

Deterministic fault points cover raw staging/copy, raw segment publication, raw
manifest publication, raw anchor publication, the raw-before-Paper boundary,
Paper segment/checkpoint/manifest/anchor publication, and `CURRENT`. Immutable
files use flush, file fsync, verified non-overwriting publication, and directory
fsync; mutable caches use atomic replacement and remain non-authoritative.

Recovery accepts only a byte-identical existing object or the single exact
authenticated successor allowed by its anchor/pending state. It rejects missing
or truncated referenced raw segments, malformed or truncated checkpoint
references, forks, duplicate record ownership, mismatched counters and prefix
roots. A crash test can establish a recoverable or fail-closed state at the
instrumented boundary; it is not a platform-independent proof for filesystem or
hardware behavior that was not exercised.

## Golden, capacity, bounded-tail, and evidence semantics

`iter_golden_native_batches` and `OfflineGoldenNativeRunner` consume an already
verified Golden V3 export. The raw record is the certified canonical inbox JSONL,
explicitly labelled `CERTIFIED_CANONICAL_JSONL_NOT_ORIGINAL_WIRE`; Phase 1C does
not claim to recover the exchange's original wire bytes. Exact differential
comparison rematerializes the native store and compares every Golden logical
stream, counts, digests, final prefix, checkpoint witness and required
`MARKET_GAP` coverage. The runner uses fresh offline raw/Paper authorities and
does not discover, replay, or mutate a live database.

The canonical Phase 1C closure does not rerun that ingestion after a producer has
reached its native Golden terminal result. It uses
`reattest_golden_native_candidate` to open the producer's raw and Paper
authorities strictly read-only, authenticate the raw segments, Paper segments,
checkpoint, manifests, witness, candidate tree, producer logs and both producer
and reattestor provenance, then repeat the exhaustive 13-stream differential.
The resulting status is `GOLDEN_NATIVE_IMPORTED_REATTESTATION_V1`; it records
zero ingested and zero prefix-reingested commits. Reobserved timings and resource
fields carry `REATTESTED_NOT_RECOVERED_ORIGINAL`: they are not relabelled as the
producer's original measurements, and the imported success does not relabel a
previous interrupted mission root as complete.

The capacity generator is deterministic and streaming. Its profiles are
`GOLDEN_SHAPED`, `ADVERSARIAL_STORAGE`, and `BOUNDED_TAIL_RESTART`. Golden-shaped
configuration records fact-derived counts and payload-size/cardinality bounds but
remains synthetic; adversarial configuration makes declared size, cardinality,
funding-burst and boundary cases observable. Every workload and report carries
`SYNTHETIC_CAPACITY_WORKLOAD`, `NOT_ECONOMIC_EVIDENCE`, `NOT_ALPHA_EVIDENCE`,
and `PAPER_ONLY` markers. The independent capacity oracle regenerates the frozen
workload and compares reopened native commits, all logical streams, workload
digest, prefix root and `MARKET_GAP` count exactly.

`OfflinePhase1CCapacityRunner` owns one fresh candidate, streams bounded batches,
performs repeated seals, reopens authorities, separates startup timing from
offline exhaustive audit, performs independent raw-resolution passes, and
reports raw authoritative bytes separately from incremental Paper bytes,
anchors, replaceable `CURRENT` caches, and scratch. RSS and process write-byte
fields are availability-qualified observations; an unavailable probe is not
reported as zero. Scratch peak is an observation at instrumented transient
growth boundaries, not a universal operating-system peak guarantee. Scaling and
GiB/hour assessment are meaningful only with a frozen cadence/source span and
the exact target provenance recorded before measurement.

The canonical Golden-shaped capacity staircase is one deterministic streaming
workload of 1,000,000 commits, not three independent ingestions. The 100,000,
500,000 and 1,000,000 manifests have the same seed, profile and configuration and
are exact prefix snapshots of the same terminal hash chain. At each frontier the
runner durably publishes a chained canonical certificate binding the workload
prefix, raw manifest, Paper manifest, checkpoint, measurement, exhaustive audit,
startup evidence and byte census. All three certificates bind one candidate,
store, stream and worker. Boundary reopen and audit time is excluded from the
active ingestion wall/CPU interval so the scaling measurements remain comparable.
No earlier prefix is regenerated, recopied or reingested; accounting must report
exactly 1,000,000 generated and ingested capacity commits and zero prefix commits
reingested.

Interprocess resume starts only from the latest complete authenticated boundary.
It reattests the raw segment chain, Paper segment chain, checkpoint, manifest,
witness and provenance before admitting the next commit. A contiguous raw-only
suffix may be reused while its unpublished Paper overlay/journal is reconstructed;
the already certified prefix is neither replayed nor reingested. Ambiguous, forked,
gapped or inconsistent published authority fails closed. The process-cut
regression requires the resumed final chain to equal a continuous execution and
records the exact audited prefix, reconstructed suffix and zero prefix
reingestion.

Every Phase 1C Golden, capacity, and bounded-tail normal reopen also emits an
ordered `StartupFileAccessTrace`. During only the synchronous reopen/alignment
scope, the tracer observes candidate-local requests through Python `os.open`,
`Path.open`, and `sqlite3.connect`, then restores the exact original functions
before any exhaustive audit or differential. Each event records its relative
path, API, category, post-scope regular-file size, and post-scope SHA-256. The
trace fails closed on a link/reparse traversal, an unclassified candidate path,
or any raw/Paper historical segment path. Its successful contract therefore
includes an explicit zero historical-segment-open count.

This is bounded Python-level evidence, not a kernel, ETW, filesystem-filter, or
SQLite VFS trace. A `sqlite3.connect` event proves the requested main database
path, but does not enumerate SQLite's internal reads or `-journal`/`-wal`/`-shm`
sidecars. Paths outside the fresh candidate are outside the trace. Hashes are
taken after the bounded scope and after runner-owned handles close; they bind
the persistent requested files, not the bytes observed at every internal read.

`BoundedTailRestartMatrixRunner` creates independent on-disk cases for the
configured tail sizes, independently reopens them, requires exactly the declared
tail replay and no historical-segment replay, then subjects the complete
checkpoint-plus-tail history to the native oracle and exhaustive audits. A tail
matrix certifies only the tested bounds and configuration.

`Phase1CEvidencePublisher` creates a fresh write-once evidence root. Canonical
reports bind code/runtime/candidate and Golden source/pin/certification
provenance, explicit semantic-gate result hashes, artifact dependencies and
technical/synthetic markers. Capacity-level `COMPLETE` markers require their
level's semantic prerequisites; the root `COMPLETE` additionally requires the
entire dependency graph, all required levels, exact Golden provenance and one of
the three authorized terminal verdicts. Failed gates may be recorded, but cannot
be promoted to a successful completion marker. Publication durability and
link/reparse defenses do not make a user-writable local evidence root an
externally protected authority.

### Phase 1C certification contract

The canonical deterministic workload seed is `20260825`. One Golden-shaped
capacity stream exposes authenticated frontiers at exactly 100,000, 500,000, and
1,000,000 commits. Only the 1,000,000-commit frontier decides the terminal
capacity verdict; the smaller frontiers and the Golden import are diagnostics.
The adversarial workload remains a separate bounded 20,000-commit test and
includes sparse 16 MiB maximum-payload probes at commits 1,866,
4,592, 10,000, and 20,000. The bounded-tail workload contains 20,001 commits and
tests independent restarts at tail sizes 0, 1, 100, 10,000, and 20,000. Production
batches contain 10,000 commits, checkpoint after every batch, and publish
manifest progress every 10,000 commits.

The roadmap target is the strict relation `<0.20 GiB/h`. Missing that storage
target is not an integrity failure: it produces the explicit characterized
target-not-met terminal verdict when all integrity gates still pass. Cross-layout
ratios remain non-like-for-like diagnostics and use four frozen physical
denominators: original V3 source 2,014,072,832 bytes; Golden V3 export
payload shards 2,456,283,751 bytes (`manifest.json` and `COMPLETE` excluded);
Phase 1B compatibility store plus anchor 528,262,318 bytes; and Phase 1B
compatibility segments 317,492,777 bytes.

Canonical preflight requires an absent fresh mission root that is a direct child
of the authorized existing parent, rejects link/reparse and input/output overlap,
and requires at least 20 binary GiB free. Before creating the candidate it binds
the complete Golden certification/export/pin authorities, the Phase 1B proof,
and the roadmap target using their expected hashes, sizes, counts, and typed
identities. Postflight re-verifies every external authority byte-for-byte. The
candidate trees are witnessed around their read-only audits, compared with the
publication witnesses, and rehashed again immediately before terminal
publication; any drift fails closed.

The imported Golden is reattested in place without mutation. The complete
Golden-shaped staircase runs in exactly one spawned worker and one candidate;
bounded tail and adversarial cases remain distinct targeted workloads. Persistent
heartbeats are constrained to 30--60 seconds and the canonical CLI uses no total
wall-clock timeout. Remote exceptions and abnormal child exits are structured
failures. CPU time, peak RSS, and write-byte observations cover only the process
scope explicitly named by each field; direct-child counters do not prove
activity for an unobserved descendant tree, so missing descendant visibility
must not be converted into a stagnation claim.

Every active-workload heartbeat names the phase, workload/profile/identity,
completed and expected commits and logical rows, elapsed and CPU time, peak RSS,
process-scoped bytes written, and exact durable raw/Paper segment and checkpoint
counts at the last completed boundary. Recent throughput is derived only from
two monotonic observations of the same immutable workload identity. When it is
calculable, the conservative ETA is the maximum of the recent and overall bounds
for every incomplete commit or row dimension; missing windows or non-positive
rates are reported as explicit unavailable statuses rather than estimates.

The write-once root inventory is exactly `workload-manifest.json`,
`native-layout-report.json`, `golden-native-report.json`, `capacity-100k.json`,
`capacity-500k.json`, `capacity-1m.json`, `scaling-report.json`,
`integrity-report.json`, `limitations.json`, and `measurements.jsonl`, plus the
capacity-level and root `COMPLETE` hierarchy. Each report is canonical and binds
its dependency hashes and semantic gates; `measurements.jsonl` is canonical
JSONL. A terminal marker cannot precede the complete authenticated inventory.

The canonical repository closure first binds the targeted Phase 1C test sources.
It then runs V10 generate/check twice, Phase05 generate/check, verifies the pinned
V9 bytes immediately before exactly one global pytest run with a fresh basetemp
and cache provider disabled, then runs global Ruff, `mypy src/hyperlab`, and
`git diff --check`, and verifies V9 again. Commands have no certifier wall timeout,
run under a sanitized environment whose projection SHA-256 is recorded, and
write persistent logs bound by path, size, and SHA-256. External authorities,
candidate trees, code identity, runtime identity, command witnesses, logs, and
V9 must all revalidate before the root `COMPLETE` file is exclusively published.

## Phase 1C scope and remaining limitations

Phase 1C is offline, public-data, `PAPER_ONLY` storage engineering. It introduces
no private key, wallet, signer, credential, private API, venue order, cancel,
mainnet, deployment, restart, or real-money path. Golden differential evidence is
technical replay/integrity evidence. Synthetic capacity and tail workloads are
mechanism/scale evidence. None of them is alpha evidence, economic validation,
profitability evidence, or authorization to trade.

The current native implementation also does not prove root-owned rollback
authority, behavior on untested filesystems/hardware, production latency under
concurrent runtime load, or unbounded scale. Normal startup still parses the
latest cumulative raw manifest; exhaustive chain/resolver work has the explicit
aggregate `O(S^2)` descriptor cost; the one-summary resolver cache is bounded by
segment limits rather than constant bytes; and a completed capacity verdict is
valid only for its frozen code/runtime/configuration, candidate hashes, workloads,
platform observations and canonical target provenance.
