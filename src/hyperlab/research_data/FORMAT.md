# Research Data Plane V1 raw format

Status: `PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY`.

This format is independent from the certified Storage V4 Phase 1C candidate.
It reuses the same integrity principles but does not mutate, import, or extend a
historical certificate.

## Envelope

Each frame is canonical UTF-8 JSON (`schema_version=1`). Binary floats are
forbidden. The exact public wire/HTTP bytes are stored as strict base64 and
bound by `content_sha256`. A missing venue sequence remains JSON `null`; local
`arrival_sequence`, `session_identity`, and gap/reconnect state do not pretend
to be an exchange sequence.

Receive time has two independent fields:

- `receive_timestamp_utc_ns`: UTC Unix epoch nanoseconds;
- `receive_monotonic_ns`: process monotonic nanoseconds.

The frame also binds venue/feed, canonical instrument or market, source time,
source sequence/cursor/event ID when actually provided, collector/session,
source-metadata version, and capture provenance.

For Lighter order-book frames, `source_sequence` is the exact documented
matching-engine `nonce`; `source_cursor` carries exact `begin_nonce` and
API-server `offset`. Generic `+1` sequence inference is disabled because the
documented rule is `current.begin_nonce == previous.nonce`. No nonce or offset
is invented. A continuity break is preserved as a final gap frame and freezes
the captured prefix.

## Segment

Suffix: `.rdpseg`. Codec profile: `zlib-fixed-raw-v1` (level 9, raw DEFLATE,
fixed Huffman strategy).

Physical layout:

1. `RDPSEG01` magic;
2. format/codec identifiers and declared logical/stored sizes;
3. SHA-256 of the uncompressed logical body;
4. deterministic compressed body;
5. SHA-256 of the physical prefix;
6. `RDPSEGE1` end magic.

The body binds the zero-based segment index, previous physical segment SHA-256,
frame count, and for every frame its length, SHA-256, and canonical envelope
bytes. A body checksum and end magic detect truncation and trailing bytes.
Published filenames are their exact physical SHA-256. Publication uses a
same-filesystem temporary file, file fsync, exclusive hard-link publication,
and directory fsync where the platform exposes it. Existing published content
is never overwritten.

## Manifest

Suffix: `.manifest.json`. A manifest is canonical JSON and its filename is the
SHA-256 of its exact bytes. It contains the cumulative ordered segment set,
counts/sizes, per-segment provenance/ranges, a raw root hash, and the previous
manifest hash. Each successor adds exactly one segment. There is no `CURRENT`
authority, symlink, or reparse-point resolution. Readers require an explicit
manifest SHA-256 and authenticate its complete predecessor chain.

## Crash boundary

- pre-publication staging files are non-authoritative and removed on writer
  recovery;
- a fully published orphan segment is validated and appended to a recovered
  manifest;
- an already published manifest is rediscovered from the content-addressed
  chain;
- missing/corrupt manifests or segments fail closed;
- one OS file lease admits exactly one writer per raw root.

The Paper boundary is one compact segment reference/summary per segment, never
one Paper commit per market-data tick. Derived datasets bind raw manifest/root,
model version, and parameter hash and remain reproducible.
