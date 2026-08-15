# Phase 10 sub-second lead-lag study

## Status and scope

This command is an offline, read-only event replay over an immutable Phase 10
lake. Its outputs are always labelled `EVENT_REPLAY_RESEARCH_ONLY`. It cannot
collect data, place orders, modify the lake, relax the technical capture gate,
or make an economic claim from synthetic or uncalibrated assumptions.

This command neither launches nor monitors a real capture. The external gate
report alone determines whether a captured window is technically admissible.
Six hours of data is not proof of long-run writer stability or profitability.

## Preregistered study

[`config/lead_lag_phase10.toml`](../config/lead_lag_phase10.toml) freezes the
study before reading results:

- assets and venue pair: BTC/ETH on Binance USD-M and Hyperliquid, constrained
  by the validated gate report and immutable lake manifest;
- horizons: exactly 50, 100, 250, 500, 1,000, 2,000, and 5,000 ms;
- executable BBO prices rather than future mid-prices;
- preregistered trade-flow and momentum windows, L2 depth, time buckets,
  minimum-move band, maximum book age, sample floor, block-randomization controls,
  and versioned bounded-memory/disk controls;
- explicit baseline and stress execution scenarios.

Every scenario is `UNCALIBRATED`. Hyperliquid maker/taker fees, entry and exit
latency, per-executed-side slippage, adverse-exit slippage, order notional,
queue-ahead multiplier, maker timeout, and maximum observed-depth participation
are sensitivity inputs. Binance is the
signal/reference venue, not a simulated execution leg. These values are not
measurements of current venue conditions and cannot support a profitability
claim. Replacing a value requires a new preregistered config and independently
hashed calibration evidence; results from the final test period must never be
used to tune it.

The scenarios contain no random rejection, missed-order, or partial-fill
probabilities. Maker fills require observed Hyperliquid trades and queue
depletion. Taker partial fills and capacity are derived from observed executable
depth. Missing entry evidence produces no fill; missing exit evidence leaves an
explicit unresolved exposure and suppresses attempt-weighted economics.

Funding is not causally calibrated or evaluated by this Phase 10 capture. Every
economic field is therefore labelled `BEFORE_FUNDING`, with
`economic_admissibility=NOT_ADMISSIBLE_FUNDING_NOT_EVALUATED`. Spread/depth,
slippage, adverse-exit, and fee components remain separate and visible, but none
is a profitability claim.

## Fail-closed inputs

Run the study only with a technical gate report produced for the same immutable
lake snapshot. The loader must validate that `technical_capture_gate` is `PASS`
before analysis or output creation. A valid report must retain the Phase 10
requirements, including real BTC/ETH data, positive matched raw/normalized
Binance trades, positive strict cross-venue overlap, continuous valid causal
clock coverage with the unchanged 50 ms uncertainty ceiling, and zero lineage
and gap errors.

The TOML file has no gate thresholds or bypasses: gate truth is supplied by and
cryptographically bound to the external report. Missing, malformed, stale,
mismatched, or failing gate evidence is fatal. The analysis must not infer
approval from files present in the lake, repair lineage, interpolate rejected
clock probes, or substitute an older manifest. Gate failure must occur before
the output directory or any artifact is created.

The 5,000,000-row and 8,000,000,000-byte fields now apply only to the retained
pandas reference oracle. The production command never raises or consults those
limits. It uses `BOUNDED_STREAMING_V1`: projected Arrow reads, disk-backed
manifest and source catalogs, one received-time state machine per asset and
strict interval, bounded rolling and pending state, an exact two-pass
count/preflight, an exact disk-backed quantile/event spool, and fixed-row-group
Parquet publication.

The preregistration freezes independent limits for rolling source state,
complete simultaneous batches, levels in one atomic L2 frame, total retained L2
levels, pending response and execution states, external ordering fan-in,
exact-quantile runs, Parquet row groups, writer buffers, and scratch
free-space/reserve. Breaching any one is fatal. The total projected event
population is a disk-sizing counter, not a RAM allocation cap; the command can
therefore admit more than five million rows only when its bounded state and disk
preflights pass.

The command also refuses an output path inside `ROOT`, an existing output path,
or a non-empty output directory. This prevents publication into the immutable
lake and prevents a rerun from mixing artifacts. Choose a new sibling directory
for every run.

## Command

From the repository environment, after the six-hour capture is complete and its
independently saved technical gate has passed, substitute the two literal input
paths below. The generated output is a new sibling of the immutable lake:

```powershell
$LakeRoot = (Resolve-Path -LiteralPath 'D:\path\to\completed-singapore-lake').Path
$GateReport = (Resolve-Path -LiteralPath 'D:\path\to\saved-passing-technical-gate.json').Path
$Config = (Resolve-Path -LiteralPath '.\config\lead_lag_phase10.toml').Path
$Output = Join-Path -Path (Split-Path -Parent $LakeRoot) -ChildPath (
  'singapore-6h-phase10-2-' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
)

& '.\.venv\Scripts\python.exe' -m hyperlab lead-lag-study `
  $LakeRoot `
  --gate-report $GateReport `
  --config $Config `
  --output $Output
```

The production pipeline is intentionally ordered:

1. `hyperlab.analysis.streaming_lake.validate_bounded_lead_lag_gate` validates
   the exact saved report bytes, re-runs continuity, validates both complete v1
   schemas, and compares their versioned semantic payloads. Only the top-level
   `/observability` object is excluded from semantic equality; it is still
   schema-validated and remains bound by the raw report hash.
2. `hyperlab.analysis.streaming_lake.load_bounded_lead_lag_window` catalogs
   manifests on disk, streams `selected_manifests.jsonl`, reads required columns
   in bounded Arrow batches, reconstructs complete L2 frames, and spools rows in
   received-time order. It never returns a complete `LeadLagDataset`.
3. `hyperlab.analysis.streaming.run_bounded_lead_lag_study` performs two passes
   through the scalar received-time watermark kernel: an exact count and disk
   preflight, then direct bounded event spooling. The kernel consumes complete
   equal-time batches, resets at every asset/strict-interval boundary, and has no
   pandas/DataFrame fallback. The pandas `analyze_lead_lag` path remains only a
   small synthetic-fixture semantic oracle.
4. The v2 publisher writes bounded CSV/JSON/Markdown and fixed-row-group Parquet
   in a hidden sibling staging directory, rechecks gate bytes plus every selected
   manifest/data hash, removes scratch, and performs one atomic write-through
   rename.

Do not point `REPORT_DIR` at the lake or create it in advance. This command is
not a collector and does not launch, restart, or monitor a Singapore smoke.

## Artifacts

A successful run writes exactly one self-contained report bundle outside the
lake:

- `result.json`: canonical machine-readable result, including the research
  status, admissibility decisions, warnings, summary metrics, and SHA-256 hashes
  of the config, gate report, and source manifest;
- `report.md`: concise human-readable verdict and limitations;
- `metrics.csv`: every preregistered asset, horizon, variant, scenario, and
  UTC-time bucket, including empty and non-admissible cells;
- `controls.csv`: block-randomization, negative-lag, reverse-direction, and
  multiple-testing controls, including empty cells;
- `events.parquet`: event-level replay evidence used to derive the aggregates,
  with primary, reverse-control, execution, interval, and causal timestamps.
- `selected_manifests.jsonl`: the canonical streamed selected-manifest evidence;
  its SHA-256, line count, and canonical manifest-set fingerprint are bound in
  every output format;
- `observability.json`: explicitly non-semantic runtime telemetry, including
  scan counts, causal-state high-water marks, scratch high-water, output rows,
  and phase timings. Deterministic counters also appear in `result.json`, while
  runtime timing and available-disk values do not affect its semantic hash.

The report must make the following warnings prominent:

- `EVENT_REPLAY_RESEARCH_ONLY`;
- source-time lead is `NOT_ADMISSIBLE` because this capture has no symmetric
  Hyperliquid clock calibration for the relevant event and venue generation;
- all bundled execution scenarios are `UNCALIBRATED`;
- six hours is not proof of stability or profitability.

The hashes bind a result to the exact preregistration, gate evidence, and
manifest. They do not upgrade an uncalibrated scenario or a short capture into
economic evidence.

## Causality and interpretation

A signal at time `t` cannot use a response observed before `t`. A simultaneous
Hyperliquid batch is absorbed into the pre-signal baseline. At each horizon, the
study compares that baseline with the latest causally observed Hyperliquid BBO at
or before the target, subject to the maximum-book-age bound. If no later update
arrives while the baseline remains fresh, the unchanged as-of state produces a
zero response. Arrival-time ordering and source-time ordering are reported
separately. Without symmetric Hyperliquid clock calibration, source-time lead
remains
`NOT_ADMISSIBLE`; local arrival order must not be relabelled as source-time
causality. Invalid clock intervals, generation changes, gaps, and lineage
failures are enforced by the external gate and loader; a failure aborts before
artifact creation. Within an admitted window, unavailable executable prices and
interval exclusions remain visible in `events.parquet` and summary exclusion
counts. `controls.csv` contains the statistical controls rather than gate
failures. No missing state is imputed.

Cross-correlation or a positive post-event response is descriptive evidence,
not a fill and not a trade. Economic interpretation additionally requires
calibrated executable prices, delays, fees, slippage, queue competition,
realized funding, observed rejections, missed and partial fills, adverse exits, capacity, and
stability outside the tuning sample. The uncalibrated scenarios only show
sensitivity to declared assumptions.

Event output size scales with the number of causal signals, horizons, and
execution scenarios. The preregistered count pass sizes disk before the event
spool is created; it does not allocate a long-form event table. The report
records files/rows scanned, every output-row class, asset/interval kernels and
receive batches processed, each rolling/pending/writer high-water mark, scratch
high-water, and phase timings. Operators must not redirect temporary or final
output into the immutable lake.

## Validation boundary

Automated tests may use synthetic fixtures to verify causality, deterministic
ordering, hash stability, refusal semantics, and artifact completeness. Such
fixtures must remain visibly synthetic and cannot pass the real technical gate
or unblock Phase 10. Validation of this implementation consists of lint, static
typing, the full unit-test suite, and diff checks; it explicitly excludes any
real Singapore smoke.
