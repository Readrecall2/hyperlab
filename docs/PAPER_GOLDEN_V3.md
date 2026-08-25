# Paper Golden V3 logical replay oracle

Status: **implementation contract only**. A corpus is `GOLDEN_V3_CERTIFIED` only after every gate in
this document succeeds on one complete offline PaperStore v3 run. Code, fixtures, benchmarks, or a
partially exported real corpus are not certification and are not economic evidence.

## Scope and safety boundary

Golden V3 preserves the complete logical Paper v3 history from `RUN_START` through the terminal
durable heads. It is an offline, PAPER-only oracle for import and exhaustive differential replay.
It adds no wallet, signer, credential, private API, exchange client, order route, runtime hook,
systemd unit, or Storage v4 implementation.

The source must be a separately named local offline copy. The exporter opens it through SQLite URI
`mode=ro`, enables `query_only=ON`, and holds one coherent read transaction. It never uses
`immutable=1`, a write-capable `PaperStore(path)` constructor, migration, WAL checkpoint, `VACUUM`,
or journal-mode changes. Before and after extraction it attests the resolved path, file identity,
stat, size, SHA-256, read-only attribute, and SQLite sidecars. Any source change or new/changed
sidecar is fatal.

Source, output, scratch, and forbidden-original sentinel must be distinct physical objects. The
tool refuses the same path, hard links, symlinks, junctions, reparse traversal, or collision with
the sentinel. Every corpus, result, scratch directory, progress log, and external pin is a new
path. Nothing is overwritten or deleted. The only in-place continuation is the explicit,
fail-closed export A resume described below; it never silently selects or regenerates evidence.

## Census and admissibility

The pre-export census verifies at least:

- the first canonical input is `RUN_START`, input/commit sequences are continuous, and the exact
  run/config/release identities agree;
- input-type distribution, strategy presence and decision counts, fills, funding settlements,
  timers, source failures/gaps, and other behavioral diversity limits are recorded without
  fabricating missing observations;
- ledger transactions/entries, alerts, commits, projection revision 0 and later revisions,
  current projection, terminal state, runtime sessions, incidents, and terminal heads are
  structurally coherent and completely covered;
- ordinary store integrity succeeds and no known non-replayable guard failure is present.

The certification result and final manifest keep three categories separate:

- `BLOCKING_INTEGRITY_GATES` contains structural failures: SQLite integrity or foreign-key
  failures, source/sidecar or identity drift, forbidden orphan/uncommitted rows, invalid event,
  commit or hash chains, incomplete or invalid exports, A/B differences, replay or
  ledger/alert/projection/head differences, and invalid final fingerprints. Unknown census gap
  codes fail closed in this category.
- `COVERAGE_METADATA_NON_BLOCKING` reports strategy IDs and decision counts, observed input types,
  input/event/alert counts, valid source gaps, and explicit behavioral coverage limits. An absent
  `phase05_cash_and_carry` or `phase08_robust_pairs` strategy, zero decisions for either strategy,
  or an unobserved optional input type limits the corpus but does not corrupt a technical
  storage/replay oracle.
- `ECONOMIC_EVIDENCE` is always `NOT_ECONOMIC_PROOF`. Golden V3 never proves alpha, profitability,
  OOS validity, or Phase05/Phase08 economic fitness, even when behavioral coverage is complete.

A durable `MARKET_GAP` that is commit-framed, chain-consistent, and reproduced exactly by replay is
useful non-blocking coverage metadata. An orphan, structurally incoherent, or non-reproducible
`MARKET_GAP` remains blocking. No alert or replay row is ignored to obtain this classification.

Every successful result and final manifest therefore declares
`golden_scope=TECHNICAL_STORAGE_AND_REPLAY_ORACLE`, reports
`phase05_decision_coverage`, `phase08_decision_coverage`, `market_gap_coverage`, and
`strategy_behavior_complete`, and fixes `economic_evidence=false` and
`authorizes_real_money=false`. Missing coverage is preserved explicitly; it is never promoted into
economic evidence or real-money authorization.

## Logical streams and canonical order

Streams are emitted in this fixed order, with the following logical ordering keys:

| Stream | Canonical order |
|---|---|
| `schema` | `kind`, `name` |
| `run` | `run_id` |
| `inbox` | `commit_sequence`, `input_id` |
| `events` | `sequence` |
| `ledger_transactions` | `commit_sequence`, `component_ordinal`, `transaction_id` |
| `ledger_entries` | `commit_sequence`, `transaction_ordinal`, `entry_index`, `entry_id` |
| `alerts` | committed by `commit_sequence`, `component_ordinal`; then uncommitted by `event_sequence`, `alert_id` |
| `commits` | `commit_sequence` |
| `projection_history` | `revision` |
| `projection_current` | `run_id` |
| `runtime_sessions` | `commit_sequence`, `input_id` |
| `incidents` | the same committed-then-uncommitted alert order |
| `heads` | `run_id` |

Each record is canonical JSON encoded as UTF-8 with sorted keys, compact separators, and LF. The
manifest fixes numeric, timestamp, null, binary, and schema representations; non-finite or
ambiguous values fail closed. Shards obey both configured row and logical-byte bounds without
splitting a record. Every stream records row count, first/last logical identity, logical SHA-256,
physical SHA-256, and logical/physical sizes.

`projection_history` identity is computed from the decoded logical projection records and their
commitments, never from incidental zlib bytes. Physical hashes protect transport; logical hashes
and the global logical root define Golden identity.

## Manifest, completion marker, and external pin

The root manifest binds source provenance and pre/post fingerprint, run/config/release identity,
schema, canonicalization and shard parameters, tool/runtime versions, per-stream metadata and
roots, global logical root, terminal heads, and census/replay/differential status. Publication is
staged and fail-closed: shards first, manifest only after verification, and `COMPLETE` last. A
partial directory without the authenticated terminal marker is never consumable as complete.

The external pin is written only after a complete verified export, at a path outside the corpus.
It binds the root identity and is made locally read-only. This is useful accidental-mutation
protection, but it is not a production root-owned anchor, remote transparency log, WORM medium, or
proof against an administrator rewriting both corpus and pin.

The end-to-end certifier writes two candidate-export manifests below `corpus/`, then writes
`manifests/certification-manifest.json`. That final manifest explicitly binds the authenticated
census, canonical run/config record, both export roots and pins, exact dual-extraction result,
exhaustive replay result, preserved replay target, source stat/SHA evidence, and every small result
artifact. It also binds the three gate categories, technical-only scope, non-economic status, and
`authorizes_real_money=false`. `pin/certification.pin.json` is made locally read-only, and the
candidate-root `COMPLETE` file is created last. Without all three final files, the candidate is not
certified.

## Two independent extractions

Certification requires two complete extractions from the same unchanged source into two absent
directories with two distinct external pins. Keep both. Verify each independently, then compare
their logical manifest, stream roots, global root, counts, terminal heads, and final identity.
The complete relative file inventories and every file byte are also compared exactly. Certification
requires both logical equality and `BYTE_IDENTICAL`; neither comparison substitutes for the other.

Example PowerShell shape from the repository root:

For the standalone wrappers, each progress file must be a new `.jsonl` below an already existing
results directory. It must remain outside the source and SQLite sidecars, extraction corpus,
external pin, replay scratch root, and replay target. The wrappers never create or follow a progress
parent implicitly.

```powershell
$Python = ".\.venv\Scripts\python.exe"
$Common = @(
  "--run-id", "<RUN_ID>",
  "--sentinel", "<FORBIDDEN_ORIGINAL>",
  "--expected-size", "<SOURCE_SIZE_BYTES>",
  "--expected-sha256", "<SOURCE_SHA256>"
)

& $Python scripts\export_paper_golden_v3.py <SOURCE_COPY> <EXTRACTION_A> @Common `
  --external-pin <PIN_A> --progress-jsonl <EXPORT_A_JSONL>
& $Python scripts\export_paper_golden_v3.py <SOURCE_COPY> <EXTRACTION_B> @Common `
  --external-pin <PIN_B> --progress-jsonl <EXPORT_B_JSONL>

& $Python scripts\verify_paper_golden_v3.py verify <EXTRACTION_A> --pin <PIN_A>
& $Python scripts\verify_paper_golden_v3.py verify <EXTRACTION_B> --pin <PIN_B>
& $Python scripts\verify_paper_golden_v3.py compare <EXTRACTION_A> <EXTRACTION_B>
```

The preferred full workflow is the single fail-closed certifier. The forbidden-original sentinel
may exist; it must be a distinct physical object and must never alias the source copy.

```powershell
& $Python scripts\certify_paper_golden_v3.py <SOURCE_COPY> <NEW_CANDIDATE_ROOT> `
  --run-id <RUN_ID> --sentinel <FORBIDDEN_ORIGINAL> `
  --expected-size <SOURCE_SIZE_BYTES> --expected-sha256 <SOURCE_SHA256> `
  --shard-rows 100000 --shard-bytes 67108864 `
  --progress-jsonl <NEW_CANDIDATE_ROOT>\results\progress.jsonl
```

### Explicit export A resume

Resume is allowed only when the supplied Golden root contains exactly one unambiguous partial
candidate with one complete export A and no export B, replay scratch, final manifest, final pin,
candidate-root `COMPLETE`, or other terminal artifact. Before doing new work, the certifier
exhaustively verifies export A and its `COMPLETE`, manifest, root, exact file count and byte count,
read-only single-link pin, recorded extraction result, run identity, and source fingerprint binding.
Any ambiguity, unexpected file, mutable or aliased pin, mismatched source, or mismatched expected A
identity blocks resume. A valid resume reuses A without rewriting it, creates B only at its absent
path, and then runs the same byte comparison, exhaustive replay, source re-attestation, and final
publication gates as a fresh certification.

The positional output argument is the parent Golden root in resume mode. The progress path remains
a new `.jsonl` directly below the uniquely discovered candidate's existing `results` directory:

```powershell
& $Python scripts\certify_paper_golden_v3.py <SOURCE_COPY> <GOLDEN_ROOT> `
  --run-id <RUN_ID> --sentinel <FORBIDDEN_ORIGINAL> `
  --expected-size <SOURCE_SIZE_BYTES> --expected-sha256 <SOURCE_SHA256> `
  --shard-rows 100000 --shard-bytes 67108864 `
  --resume-existing-a `
  --expected-export-a-root-hash <EXPORT_A_ROOT_HASH> `
  --expected-export-a-file-count <EXPORT_A_FILE_COUNT> `
  --expected-export-a-bytes <EXPORT_A_BYTES> `
  --progress-jsonl <UNIQUE_PARTIAL_CANDIDATE>\results\resume-progress.jsonl
```

From a second PowerShell, monitor the durable log without touching the worker:

```powershell
Get-Content -LiteralPath <NEW_CANDIDATE_ROOT>\results\progress.jsonl -Wait
```

The CLI emits a durable heartbeat every 25 seconds. It carries elapsed and CPU time plus the latest
known phase and, when available, stream, completed rows/commits/bytes, total expected work, file
progress, record/input identity, replay target-store size, and a conservative ETA when it can be
calculated.

## Exhaustive replay and differential

Replay creates a fresh disposable target under a new scratch directory and replays every canonical
input from `RUN_START`. It then compares every logical row of inbox, events, ledger transactions,
ledger entries, alerts including uncommitted alerts, commits and their chain, projection revision
0, every intermediate projection revision, current projection, runtime sessions/incidents, and
terminal heads. There is no sampling, tolerance relaxation, or final-projection-only shortcut.
Committed `MARKET_GAP` inputs, alerts, state transitions, projections, and heads participate in the
same exhaustive comparison; replay must reproduce them exactly.

Only the fresh disposable reconstruction may use the already reviewed historical-replay storage
optimizations. The source and every durable Paper store retain their normal strict durability and
integrity rules.

```powershell
& $Python scripts\verify_paper_golden_v3.py replay <EXTRACTION_A> <NEW_SCRATCH_A> `
  --progress-jsonl <REPLAY_A_JSONL>
```

The certification CLI contains none of the old 840/900-second incident timeouts. It enforces one sole
in-process safety ceiling of **7200 seconds** by interrupting the main thread, allowing normal fail-closed
unwinding to preserve the partial candidate. This ceiling is supervisor policy only, never an
architectural pass gate and never permission to mark partial work complete.

## Interruptions, failures, and reruns

Progress JSONL is append-only and flushed as phases advance. Ctrl+C stops the current wrapper and
leaves partial corpus, log, and scratch evidence in place. An interrupted extraction/replay cannot
publish candidate-root `COMPLETE`, update the final pin, or become certified. Corruption, logical
mismatch, source change, or pin mismatch is a terminal honest failure. Missing behavioral coverage
is reported as non-blocking metadata and is not a reason to fabricate or rerun observations.

The explicit unique-A resume above is the only permitted reuse of an existing candidate. A second
real continuation is allowed only after a demonstrated defect in that resume tool is fixed and
covered by a focused regression test. Any separate complete attempt uses entirely new output,
result, scratch, and pin paths; failed evidence remains preserved.

## V3 import versus V4-native storage

Golden V3 preserves the complete logical V3 history and is authoritative for future
`V3_COMPATIBILITY_IMPORT` and differential validation. Such an importer may retain a one-to-one
mapping for every V3 logical record.

Storage v4 Phase 1B has now certified that compatibility path against the complete Golden:
252,262 commits, 1,011,362 logical rows, and all 13 streams were preserved exactly through 21
overlay/segment/manifest/checkpoint cycles, with one persisted checkpoint-state witness per
checkpoint. The certified terminal store has final prefix root
`f32965fa0b24cc189e271d682136680c2867c76074724e552a43e248897665ba` and manifest root
`a85846c7899ddf8693e4882716e80274fec18663c66958445c788822bbb41398`. Authenticated startup
used the terminal checkpoint with zero historical segments and zero tail entries replayed. This
proves historical payload replay `O(tail)` for the compatibility engine; metadata authentication
remains `O(current_manifest + checkpoint + tail)` because the current manifest is cumulative.

The compatibility segments occupy 317,492,777 bytes and the complete local Storage v4 store
528,250,030 bytes. These are Windows compatibility-import observations, not a `V4_NATIVE`
capacity result, Linux/ext4 durability certification, or economic evidence. The Golden source,
manifest, and external pin remain byte-identical.

This does not decide the physical shape of `V4_NATIVE`. A future native writer may reference
immutable raw segments and retain only the decision/audit/replay records its reviewed contract
requires; it need not copy every market update into the same V3 tables. That separation requires a
later design and certification phase.

## Verdicts

- `GOLDEN_V3_CERTIFIED`: source unchanged; both extractions are complete, independently verified,
  root-identical and byte-identical; exhaustive replay/differential is exact; final artifacts and
  every blocking integrity gate verify.
- `GOLDEN_V3_CERTIFICATION_GENUINE_INTEGRITY_BLOCKED`: a genuine structural, provenance, source,
  export, chain, ledger, alert, projection, head, or final-fingerprint inconsistency is proven.
- `GOLDEN_V3_CERTIFICATION_REPLAY_DIVERGED`: exhaustive replay has an unresolved logical
  divergence.

Absence of Phase05/Phase08 decisions, an absent strategy, an unobserved optional input type, or a
valid exactly reproduced `MARKET_GAP` does not by itself select either blocked verdict. None of
these verdicts is profitability, alpha, economic certification, deployment approval, or real-money
authorization.
