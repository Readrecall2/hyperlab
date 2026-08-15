# Phase 10 large-dataset readiness

## Status

The bounded implementation is complete. Its two-million-source-row stress,
focused suite, full repository suite, and independent final diff review are all
green. The exact operational status is:

    PHASE10_2_AUTHORIZED_AFTER_SAVED_TECHNICAL_GATE_PASS

The implementation does not raise or consult the legacy 5,000,000-row or
8,000,000,000-byte pandas materialization limits in production. It authorizes
only an offline study after an independently saved technical-gate PASS is
revalidated. It does not authorize access to a still-running capture, collector
changes, network activity, trading, order submission, signing, secrets, a
profitability claim, or a weaker Phase 10 technical gate.

## Repository and commit anchors

The design audit began from the following local, read-only anchors:

- bounded-memory worktree:
  `C:\Dev\hyperlab-phase10-bounded`;
- bounded-memory branch: `phase-10-bounded-audit`;
- bounded-memory checkpoint before the continuity refactor:
  `4cdf7f01518989623ac03ad5953daf548b26ec0b`;
- bounded branch collector baseline:
  `cb7ce3c6270d289b6ecb93042100ae71d0c61866`;
- analysis worktree:
  `C:\Dev\hyperlab-phase10-analysis`;
- analysis branch: `phase-10-lead-lag-analysis`;
- original analysis implementation:
  `470042c4d0cffd615c6803fe068ebe0f6c8fa9e0`;
- analysis checkpoint:
  `cb1c18e0bc9bfd452671d7fd9d4c70af3499bf24`;
- merge base of the two worktrees:
  `6ed00ff22c818f7b2d38b31444cc2477d5376e9a`.

The bounded continuity commit `1982efc` was merged into this worktree by
`e146cb6`. The required pre-refactor checkpoint is `e871d55`. No continuity,
collector, writer, runtime, network, DNS, or clock behavior was modified by the
Phase 10-2 implementation.

The original unbounded source references below describe commit `470042c`:

- `config/lead_lag_phase10.toml`;
- `src/hyperlab/analysis/lake.py`;
- `src/hyperlab/analysis/lead_lag.py`;
- `src/hyperlab/analysis/reporting.py`;
- `tests/test_lead_lag_analysis.py`;
- `tests/test_lead_lag_lake.py`;
- `tests/test_lead_lag_cli.py`.

## Current resource failure

The existing implementation is fail-closed, but it is not bounded-memory:

1. `analysis/lake.py:_read_selected_rows` appends every selected row to
   Python lists and converts every complete Parquet table with
   `to_pylist()`.
2. `analysis/lake.py:_reconstruct_l2` groups every L2 header and level for the
   complete requested window before producing compact snapshots.
3. `analysis/lake.py:_dataframe` creates complete BBO, trade, L2, and clock
   DataFrames while the preceding row populations are still live.
4. `analysis/lead_lag.py:_prepare_l2` explodes compact L2 snapshots back into
   a complete level-row DataFrame.
5. `analysis/lead_lag.py:_build_signals` retains complete primary and reverse
   signal tables.
6. The capacity check is called only after all source frames and both signal
   tables have been materialized.
7. Response, reverse-control, maker, and taker rows are each materialized in
   full and then copied into one long-form event DataFrame.
8. `analysis/reporting.py` adds binding columns to another event frame and
   calls one-shot `DataFrame.to_parquet()`.

The current projection for seven horizons and two execution scenarios is:

    projected_rows = 35 * primary_signal_count + 7 * reverse_signal_count

For equal primary and reverse counts, the current base constants charge at
least 573,440 estimated peak bytes per signal pair before variable strings.
Consequently the 8,000,000,000-byte bound is reached at approximately 13,950
signal pairs, well before the 5,000,000-row bound. Neither limit makes the
upstream loader or signal construction safe on a 4 GiB host.

The current `event_table_estimated_memory_bytes` field is measured only after
the final table exists. It is not a measurement of process peak RSS and must not
be represented as one.

## Implemented bounded architecture

The production CLI now dispatches exclusively to
`hyperlab.analysis.streaming.run_bounded_lead_lag_study`. The original
`LeadLagDataset` and pandas implementation remain available as the
small-synthetic-fixture semantic oracle, but are not imported or called by the
production runner.

The bounded runner is composed as follows:

1. `gate_binding.py` reads and hashes the exact saved gate-report bytes,
   validates the full version-1 semantic and observability contracts, reruns the
   independent continuity audit, and compares canonical semantic payloads after
   excluding exactly top-level `/observability`. Unknown semantic fields and
   nested fields named `observability` remain compared. The exact saved bytes
   are checked again immediately before publication.
2. `streaming_lake.py` catalogs manifests in SQLite, incrementally writes and
   hashes `selected_manifests.jsonl`, reads only record-type projections in
   bounded Arrow batches, verifies content hashes, reconstructs complete atomic
   L2 frames on disk, and exposes complete received-time batches in
   deterministic asset/strict-interval order. It never returns a complete
   source population.
3. `streaming_kernel.py` is a pandas-free causal watermark state machine. It
   applies complete equal-time batches atomically, uses `received_time` only,
   resets at every strict-interval boundary, emits all eight signal families at
   all seven horizons for BTC and ETH, and retains bounded BBO, public-trade,
   trade-window, L2, pending-response, pending-execution, and output-buffer
   state. Every configured cap is fail-closed.
4. `streaming_execution.py` evaluates Hyperliquid-only maker and taker attempts
   from scalar causal timelines. It preserves latency, fees, spread,
   depth/slippage, public queue evidence, missed and partial fills, unresolved
   exposure, adverse exit, and break-even move without constructing a complete
   execution table.
5. `streaming_store.py` writes event mappings into a disk-backed canonical
   spool with a preregistered fixed Arrow schema and nanosecond timestamps.
   Fixed-size buffers and row groups feed streaming Parquet publication; no
   complete event population or data-derived schema is held in memory.
6. `streaming_aggregates.py` computes every configured metric, bucket, decay,
   false-positive count, negative-lag control, reverse control, block-sign
   randomization, max-T value, and Benjamini-Hochberg value from projected spool
   scans. Quantiles use exact disk-backed order statistics with pandas linear
   interpolation; no approximate sketch is used. Randomization operates on
   deterministic per-hypothesis/per-block sums rather than an event-by-resample
   matrix.
7. `streaming_reporting.py` publishes schema-v2 JSON, Markdown, CSV, and Parquet
   artifacts from a hidden sibling staging directory. It binds raw and semantic
   gate hashes, canonicalizer version and excluded pointer, config hash,
   manifest-set fingerprint, and selected-manifest JSONL hash/count. A final
   input recheck, file and directory flush, and atomic rename are required;
   failure or interruption removes staging and leaves no completed report.

Production performs an exact count pass before its event pass so disk needs are
known before full output generation. Runtime-dependent timings and free-space
measurements are written to `observability.json` and excluded from the semantic
result hash; deterministic counters and bounded-state high-water marks remain in
the analysis result.

## Semantics that must not change

The streaming implementation is a resource refactor, not a research redesign.
It must preserve all of the following.

### Gate and immutable input

- `technical_capture_gate` must equal `PASS`.
- `failure_reasons` must remain empty.
- The gate must cover exactly BTC and ETH.
- The requested window and strict intervals remain half-open and based on
  `received_time`.
- The 50 ms clock-uncertainty ceiling and all 10 s and 15 s policies remain
  unchanged.
- Rejected probes, causal clock outages, cadence, generations, connection
  events, gaps, strict overlap, and raw/normalized lineage remain owned by the
  independent continuity gate.
- A fresh continuity re-audit must reproduce the saved semantic gate before any
  economic event output is staged.
- Selected content-addressed Parquet files and canonical manifests must remain
  bound to the output and must be checked for drift before publication.
- No row may be repaired, interpolated, forward-filled across a gap, or promoted
  from rejected clock evidence.

### Causal event study

- Local causal ordering uses `received_time`, never exchange time.
- All rows sharing one receive timestamp form a simultaneous venue batch.
- The complete response-venue BBO batch at signal time `t` is included in the
  baseline, so a move received at exactly `t` cannot be credited to the
  signal.
- Signals in one strict interval may not use feature, baseline, response,
  execution, or negative-lag state from another interval, even if both
  intervals share a generation tag.
- A signal at `t` may earn only from observations received after `t`.
- The horizons remain exactly 50, 100, 250, 500, 1,000, 2,000, and 5,000 ms.
- Both BTC and ETH remain mandatory.
- The signal families remain:

  - `agg_trade`;
  - `trade_imbalance`;
  - `bbo_change`;
  - `l2_imbalance`;
  - `mid_price_change`;
  - `microprice_change`;
  - `short_term_momentum`;
  - `signed_flow`.

- Response baselines, endpoint states, freshness exclusions, first-move
  direction and delay, negative-lag response, classification, interval identity,
  and exclusion reasons must retain their current meaning.
- Empty and ineligible hypothesis cells must still be reported.

### Controls and execution

- Block-sign randomization retains the same seed, sorted set of actually used
  interval blocks, resample count, hypothesis order, max-T family-wise
  correction, and Benjamini-Hochberg correction.
- Negative-lag and reverse Hyperliquid-to-Binance controls remain present.
- Every configured primary signal/horizon produces all configured maker and
  taker scenario attempts when its lifecycle fits the strict interval, even
  when its information response is not evaluable.
- Taker entry uses the first BBO at or after declared entry latency, must occur
  before the response horizon, and uses observed executable depth.
- Maker entry requires public Hyperliquid trades strictly after the observed
  entry book and at or before the deadline. Queue-ahead depletion, side, and
  price eligibility remain explicit.
- Partial fills, missed entries, unresolved exits, residual exposure, spread,
  fees, slippage, adverse exit, and capacity retain separate fields.
- Economic output remains `BEFORE_FUNDING`,
  `EVENT_REPLAY_RESEARCH_ONLY`, and not admissible as a profitability claim
  while scenarios or funding are uncalibrated.

## Semantic gate binding and nondeterministic telemetry

The bounded continuity report adds a top-level `observability` object. That
whole object is explicitly nonsemantic: it contains deterministic scan counters
alongside runtime-dependent values such as elapsed durations and scratch
high-water marks. The analysis loader at `470042c` compares the complete saved
report with a complete fresh report, so it cannot reproduce the new report
without a versioned semantic contract.

The implementation uses the versioned
`phase10_semantic_gate_payload_v1(report)` canonicalizer with these rules:

1. Validate the complete saved JSON first, including duplicate-key rejection,
   finite numbers, gate schema, thresholds, and gate status. Strictly validate
   the complete `observability` schema too: it must be a top-level object with
   `semantic == false`, known sections, nonnegative integer counters and
   high-water marks, and finite nonnegative elapsed values.
2. Compute and retain a SHA-256 hash of the exact saved report bytes.
3. Build the semantic payload by excluding exactly this top-level JSON pointer:

       /observability

   Do not remove a nested field with the same name, and do not apply recursive
   name-based filtering.
4. Files scanned, rows scanned, manifest counts, bounded-state peaks, and
   elapsed values remain present in the raw saved report and are therefore
   bound by its exact-byte SHA-256. They are observability, not technical-gate
   evidence and not inputs to saved-versus-fresh semantic equality.
5. Failure reasons, policy values, intervals, lineage, gaps, and every unknown
   field outside that exact top-level pointer remain part of the semantic
   comparison and hash.
6. Compare the saved and fresh semantic payloads canonically.
7. Persist both the raw report hash and semantic canonical hash in every
   analysis artifact binding.

The canonicalizer version and the single allowed pointer must be explicit in
code and artifacts. Broad recursive removal of fields named `elapsed`,
`duration`, or `observability` is forbidden because it could hide semantic
evidence. A malformed or unknown `observability` schema fails validation before
canonicalization; excluding it from semantic equality never means accepting it
without validation.

## Bounded architecture contract

### 1. Immutable window descriptor

The production loader replaces the full `LeadLagDataset` return value with a
small immutable descriptor containing:

- root and requested window;
- strict intervals and assets;
- raw and semantic gate hashes;
- a canonical manifest fingerprint and count;
- a lazy projected batch source;
- deterministic scan counters.

Keep `LeadLagDataset` and the pandas implementation as a reference oracle for
small synthetic fixtures only. The production CLI must use the bounded runner.

Canonical selected manifest entries must not remain a 60,000-element Python
object population. Stream them in sorted order to
`selected_manifests.jsonl`, calculate its hash incrementally, and bind its
hash, row count, and canonical manifest-set fingerprint in `result.json`.
This requires artifact schema version 2.

### 2. Projected, hash-checked input

The shared bounded lake iterator:

- prunes manifests by partition key and manifest `received_time` bounds;
- reads only columns required for the selected record type;
- hashes files incrementally;
- decodes bounded Arrow record batches;
- validates projected row count and partition identity;
- verifies file size/hash again before successful publication;
- releases Arrow and Python objects after each batch;
- exposes files scanned, rows scanned, bytes read, and largest decoded batch.

The iterator may hold only a configured number of file/run readers. Overlapping
immutable segments must use a bounded-fan-in external merge with the same
deterministic row key as the reference implementation. Holding one live reader
or heap row for every one of 60,000 files is not acceptable.

L2 book-state headers and level rows may live in different partitions. Rebuild
one atomic frame inside the complete simultaneous receive-time batch, checking
snapshot identity, lineage, side counts, contiguous levels, and numeric wire
arrival order before emitting it.

### 3. Deterministic asset/interval chunks

Process each asset and strict interval independently. A chunk owns a half-open
core of signal timestamps and clipped causal halos.

For a chunk beginning at `core_start`, the required left halo is:

    max(
        momentum_window_ms + max_book_age_ms,
        trade_window_ms,
        max(horizons_ms) + max_book_age_ms,
    )

For a chunk ending at `core_end`, the required right halo is:

    max(horizons_ms) + max(exit_latency_ms) + max_book_age_ms

Halos are clipped to the current strict interval and never cross an interval
gap. Only signals whose timestamps are in the chunk core are emitted, so halo
rows cannot duplicate output.

Chunk boundaries must be deterministic and may occur only between complete
`received_time` batches. The planner is limited by configured source rows and
expanded event rows, not only by wall-clock duration. If a single simultaneous
batch or atomic L2 frame exceeds its preregistered safety bound, abort
fail-closed; never split, drop, or sample it.

### 4. Bounded causal state

Within a chunk or streaming state machine, retain only:

- the previous terminal BBO state per venue/asset;
- the BBO history required by momentum, freshness, and negative lags;
- equal-time aggregated trades and the configured trade-window deque;
- the terminal complete L2 snapshot per receive-time batch;
- pending response horizons ordered by target time;
- pending maker/taker lifecycle states ordered by their next causal deadline;
- completed signal bundles waiting for the deterministic output watermark.

Apply all market updates in a receive-time batch before creating baselines or
resolving deadlines at that timestamp. For a response target `T`, apply the
complete batch at `T` when one exists. When no batch exists exactly at `T`,
finalize only after the next unread batch establishes a watermark strictly
greater than `T`, using the latest state at or before `T` and never a row after
`T`. End-of-interval and end-of-input act as explicit terminal watermarks.

Maker deadlines include public trades received exactly at the deadline. A
maker deadline is therefore resolved only after its complete equal-time batch,
or after the watermark has advanced strictly beyond it. Taker and exit searches
retain the current first-BBO-at-or-after rule and their configured freshness
limit. Baselines, entries, responses, maker fills, and exits retain all current
left/right inclusion boundaries.

Record deterministic high-water marks for every retained structure. A breached
state bound aborts and removes the staging directory.

### 5. Streaming output and exact aggregates

Write events through a fixed-schema `pyarrow.parquet.ParquetWriter` using a
fixed compression configuration and fixed row-group size. Preserve the existing
logical event ordering:

1. signal time;
2. asset;
3. signal family;
4. horizon;
5. signal ID;
6. row kind;
7. execution scenario;
8. execution model.

Use completed signal bundles plus a causal watermark, or bounded sorted runs
followed by an external merge. A final full-table sort is forbidden.

Accumulate counts, sums, classifications, exclusions, fill statuses, residual
exposure, and block sums incrementally. Exact q10, median, and q90 values must
not be replaced by approximate sketches. Spill narrow float64 samples by fixed
metric key, externally sort with bounded runs, and retrieve the exact
`q * (n - 1)` order statistics with the same linear interpolation as pandas.

Randomization needs only per-hypothesis/per-block response sums, counts, and
sum-of-squares. After the scan:

1. sort the exact set of blocks that had evaluable events;
2. generate the same seeded sign matrix;
3. reconstruct each randomized mean from block sums;
4. apply max-T and BH in the same configured hypothesis order.

Report every configured aggregate and bucket cell, including zeros and
non-admissible cells.

### 6. Atomic publication

The production command must retain the current destination protections:

- output outside the immutable lake;
- output must not already exist;
- no final artifact before gate validation;
- a hidden sibling staging directory after gate validation;
- cleanup on every exception;
- gate-file byte recheck and manifest-set drift recheck after all reads;
- flush/fsync of files and directory entries;
- one final atomic rename.

Evidence-binding columns are known before event writing and are added to every
event, metric, and control row as it is written. The final report binds:

- config hash;
- exact gate-report hash;
- semantic gate hash and canonicalizer version;
- selected-manifest fingerprint;
- `selected_manifests.jsonl` hash and count;
- artifact schema and streaming resource-model versions.

## Resource model and preflight

The existing `max_event_rows` and `max_estimated_event_bytes` describe a
legacy in-memory materialization. They may remain only for the reference engine.
The production config must be versioned and preregister explicit bounded
controls such as:

- maximum projected source rows per chunk;
- maximum rows in one simultaneous batch;
- maximum levels in one atomic L2 frame;
- maximum aggregate L2 levels per chunk before pandas reconstruction;
- maximum pending response states;
- maximum pending execution states;
- external-merge fan-in;
- quantile-sort run rows;
- Parquet row-group rows;
- writer-buffer rows;
- scratch-space low-watermark and reserve.

The frozen `BOUNDED_STREAMING_V1` preregistration uses these limits:

| Bound | Value |
| --- | ---: |
| retained rolling source rows | 100,000 |
| one complete simultaneous batch | 25,000 rows |
| one atomic L2 frame | 10,000 levels |
| retained L2 state | 100,000 levels |
| pending response states | 500,000 |
| pending execution states | 1,000,000 |
| external ordering fan-in | 32 |
| exact-quantile sort run | 250,000 rows |
| Parquet row group | 65,536 rows |
| writer buffer | 16,384 rows |
| scratch low-watermark | 4,000,000,000 bytes |
| scratch reserve | 2,000,000,000 bytes |

They are independent of the legacy pandas oracle limits and must not be raised
after inspecting economic results.

A preliminary streaming count pass should compute exact primary and reverse
signal counts without retaining signals. It then reports:

- source rows by venue, asset, and type;
- projected information, reverse, maker, taker, and total event rows;
- conservative output and scratch-disk requirements;
- available output-filesystem space.

Total projected rows are observability, not a RAM allocation cap. Insufficient
disk or any internal-state bound remains a fatal pre-publication error.

Production observability must include:

- manifests selected and files scanned;
- rows scanned by venue/asset/type;
- output rows and bytes by row kind;
- largest Arrow batch and simultaneous receive-time batch;
- peak BBO/trade history;
- peak pending response and execution states;
- peak completed-bundle and writer buffers;
- external merge runs/fan-in;
- quantile spill/run bytes;
- scratch peak bytes;
- elapsed time by phase.

Deterministic Phase 10-2 counters and high-water marks belong in canonical
analysis-result metadata. Separately, when a continuity report is used as the
technical gate, its complete top-level `observability` object is validated but
excluded from semantic gate equality as specified above. The exact raw
continuity-report hash still binds every counter, high-water mark, and elapsed
value in the saved evidence file.

No specific peak RSS may be claimed unless it is measured in a separate process
with a stated platform, Python/Arrow versions, fixture shape, and sampler.

## Measured bounded stress evidence

The exact production-component stress command was:

```powershell
$env:PYTHONPATH = (Resolve-Path 'src').Path
& 'C:\Dev\hyperlab-phase12\.venv\Scripts\python.exe' `
  'scripts\phase10_streaming_stress.py' `
  --manifests 60001 `
  --source-rows 2000000 `
  --minimum-output-events 2000000 `
  --writer-buffer-rows 16384 `
  --quantile-run-rows 250000 `
  --scratch-parent '.tmp'
```

It ran in a separate process on Windows 11 with Python 3.12.13, PyArrow 21.0.0,
and `GetProcessMemoryInfo.PeakWorkingSetSize` as the RSS sampler. It used a lazy
complete-received-time source for Binance USD-M and Hyperliquid BBO, L2, and
trade observations for BTC and ETH. The real kernel processed 2,000,000 source
rows in 31,250 complete batches and emitted 2,624,748 rows: 437,458 information,
437,458 control, and 1,749,832 execution. Every emitted row passed through the
disk EventSpool and fixed-schema Parquet writer. A separate exact configured
grid exercised all assets, eight families, seven horizons, scenarios, maker and
taker models, exact quantiles, and controls.

Measured results:

- peak RSS: 315,895,808 bytes (301.3 MiB);
- scratch high-water: 11,127,138,981 bytes (10.36 GiB);
- causal kernel plus event spool: 893.5733869 s;
- full fixed-schema Parquet publication: 710.2055910 s;
- exact configured-grid aggregate and Parquet pass: 1.6162266 s;
- total: 1,605.4293692 s (26.76 min);
- maximum complete source batch: 64 rows;
- maximum retained source state: 336 rows;
- maximum BBO history: 124 rows;
- maximum public-trade history: 22 rows;
- maximum trade-window state: 2 batches;
- maximum L2 history: 124 frames / 496 levels;
- maximum pending response / execution state: 332 / 832;
- maximum sink/output buffer: 16,384 rows.

A 200,064-source-row comparison run retained the same causal high-water values;
therefore 10x duration increased disk volume and elapsed time without increasing
retained causal state. Its measured peak RSS was 297,865,216 bytes. The lazy
catalog test separately traverses 60,001 manifests without constructing a file
list. Neither stress path accessed the Singapore capture.

## Required regression and stress tests

### Reference equivalence

Run the existing pandas engine and the bounded engine on the same small fixtures
and compare:

- canonical event rows and ordering;
- information, bucket, and execution metrics;
- exclusions and classification counts;
- q10/median/q90 and first-move median;
- negative-lag and reverse controls;
- randomization samples, p-values, max-T, and BH values;
- summary counts and warnings, excluding only resource-model-specific metadata.

Comparison cases must include:

- injected lag and deterministic null;
- simultaneous Binance/Hyperliquid timestamps;
- duplicated physical segmentation with identical logical rows;
- disjoint strict intervals sharing one generation tag;
- horizon and entry/exit deadlines exactly on boundaries;
- stale and missing BBO/L2;
- incomplete and multiple same-time L2 frames;
- maker with no public trade, queue-only depletion, partial fill, and complete
  fill;
- taker partial entry/exit and unresolved residual exposure;
- zero-signal and below-minimum-event cells.

Every floating comparison needs an explicit tolerance. Quantile interpolation
and seeded control arrays should be exact where the algorithms are unchanged.

### Chunk and ordering invariance

For one logical fixture, vary:

- Parquet file segmentation and manifest order;
- Arrow batch size;
- chunk row limit;
- core time boundary;
- external-merge fan-in;
- Parquet writer input batch size.

Decoded logical artifacts and canonical result hashes must remain identical.
Fixed output row-group boundaries must be independent of input batches.

### Large shape without large fixtures

Add lazy tests representative of:

- more than 60,000 virtual manifests;
- millions of source rows emitted from a deterministic batch generator;
- both venues, BTC/ETH, BBO/trade/L2/clock types;
- all seven horizons and all configured scenarios.

The tests must not construct a 60,000-entry list or a million-row DataFrame.
Use lazy manifest generators, repeated small immutable tables, counting/hash
sinks, and internal high-water assertions. Assert that peak retained state is
stable when dataset duration is multiplied while event density is held fixed.

Include one bounded temporary-disk integration test that writes real Parquet row
groups, plus a subprocess memory benchmark that is reported but not made flaky
by a platform-specific hard RSS assertion.

### Fail-closed tests

Verify that no final output survives:

- failing or non-reproducible gate;
- changed gate bytes;
- changed manifest set or data hash;
- insufficient scratch/output space;
- oversized simultaneous batch or L2 frame;
- pending-state cap breach;
- external-sort failure;
- Parquet writer failure;
- process interruption before final rename.

## Merge and implementation order

Do not implement Phase 10-2 streaming in
`C:\Dev\hyperlab-phase10-bounded`.

After Goal 1 is fully validated and committed:

1. Confirm both worktrees are clean.
2. In `C:\Dev\hyperlab-phase10-analysis`, merge the completed
   `phase-10-bounded-audit` branch into
   `phase-10-lead-lag-analysis`.
3. Preserve `cb7ce3c` and all bounded continuity changes.
4. Resolve only the shared lake, continuity canonicalization, and CLI seams.
5. Create a Git checkpoint before the risky streaming refactor.
6. Add equivalence and resource-bound tests before replacing the production
   path.
7. Implement in this order:

   - semantic gate canonicalization;
   - lazy immutable window and manifest binding;
   - bounded projected/external-merge input;
   - chunk planner and causal kernels;
   - event writer and exact aggregate spools;
   - production CLI orchestration and atomic publication;
   - observability, documentation, and stress tests.

8. Keep the reference engine callable only from tests or an explicitly
   small-fixture API. The real-lake CLI must not fall back to it.
9. Review the complete diff for collector, storage, network, DNS, clock-policy,
   technical-gate, or trading changes. Any such unintended change blocks the
   merge.

The completed bounded-memory commits must be merged into the analysis branch;
the analysis feature must not be copied into this bounded worktree. A merge
records the shared history and makes conflicts auditable.

## Validation commands

The final validation was run from `C:\Dev\hyperlab-phase10-analysis` with
`C:\Dev\hyperlab-phase12\.venv\Scripts\python.exe` because the analysis
worktree's local environment was unavailable. `PYTHONPATH` was set to the
resolved repository `src` directory and every pytest invocation used a
repository-local temporary directory with the cache provider disabled.

- `python -m ruff check .`: passed;
- `python -m mypy src/hyperlab --no-incremental`: passed, 86 source files;
- focused Phase 10-2 suite, covering gate, lake, store, kernel, execution,
  aggregates, analysis, reporting, invariance, stress, pandas oracle, and CLI:
  161 passed in 707.25 s;
- full `python -m pytest`: 966 passed in 1,267.40 s;
- five warnings came only from a pandas-oracle conversion that discards
  nonzero nanoseconds; the production kernel-to-spool-to-Parquet seam has a
  dedicated exact `+123 ns` regression;
- final `git diff --check`: passed;
- independent final read-only review: no blockers. It verified fail-closed gate
  binding, bounded projected input and state, received-time causality, exact
  controls and quantiles, fixed-schema atomic publication, unchanged legacy
  limits, and diff confinement. Its residual notes are that the synthetic
  stress separates the full 2,624,748-row kernel/spool/Parquet path from the
  1,344-row exact configured aggregate grid, the 60,001-file test exercises a
  lazy mocked catalog rather than 60,001 physical Parquet files, and measured
  synthetic RSS is evidence rather than a guarantee for a real capture. Runtime
  disk preflights and state caps remain fail-closed.

The dedicated 60,001-manifest and two-million-source-row stress command,
platform, elapsed time, internal high-water marks, scratch peak, and measured
process peak RSS are preserved in the measured evidence section above.

Do not use the Singapore six-hour capture as a development or stress fixture.
It may be analyzed only after the implementation, equivalence suite, full
regressions, and resource validation have passed.

## Readiness decision

Phase 10-2 becomes ready for a greater-than-five-million-row study only when all
of these are true:

- no complete source or event population is retained;
- production uses the bounded runner exclusively;
- all reference-equivalence tests pass;
- all 60,000-file and million-row stress tests pass;
- deterministic output is invariant to segmentation and chunking;
- semantic gate reproduction and raw report binding both pass;
- resource and disk preflights are fail-closed;
- Ruff, mypy, focused tests, full pytest, and `git diff --check` pass;
- assumptions, measured limits, and remaining economic limitations are
  documented;
- no collector/runtime/storage/network/DNS/clock-policy or trading path was
  introduced.

Every item above is now green. The six-hour Phase 10-2 study is authorized only
after the capture is complete and immutable and an independently saved
technical-gate PASS exists. The command itself revalidates that report, reruns
the technical audit, compares the semantic gate, and rechecks immutable inputs
before publication. Authorization is not an economic or profitability claim.
