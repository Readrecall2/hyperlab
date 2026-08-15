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
  and fail-closed event-row and estimated-memory bounds;
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

The preregistration also caps projected long-form output at 5,000,000 event rows
and 8,000,000,000 estimated bytes. Before allocating the horizon/scenario event
expansion or creating output, the analysis computes its projection and fails
closed if either bound would be exceeded. Raising a cap merely to force a run is
not evidence that the workload is safe; an oversized study requires a reviewed
bounded or streaming design.

The deterministic `CONSERVATIVE_LONG_FORM_V1` projection charges at least
4,096 bytes per information/control row and 8,192 bytes per execution row,
adds variable asset, interval-tag, and scenario text, then applies a peak
materialization multiplier of 2. These constants and both projected totals are
recorded in the result summary so an accepted run remains auditable.

The command also refuses an output path inside `ROOT`, an existing output path,
or a non-empty output directory. This prevents publication into the immutable
lake and prevents a rerun from mixing artifacts. Choose a new sibling directory
for every run.

## Command

From the repository environment:

```powershell
hyperlab lead-lag-study ROOT `
  --gate-report PATH `
  --config config/lead_lag_phase10.toml `
  --output REPORT_DIR
```

The pipeline is intentionally ordered:

1. `hyperlab.analysis.lake.load_validated_lead_lag_window` validates the lake,
   manifest, gate, lineage, clock evidence, and replay window without mutation.
2. `hyperlab.analysis.lead_lag.analyze_lead_lag` performs the preregistered causal
   event replay and retains every variant and bucket, including failures and
   empty/ineligible cells. The same module owns `load_lead_lag_config`.
3. `hyperlab.analysis.reporting.write_lead_lag_artifacts` atomically publishes to
   the new external report directory only after validation and analysis succeed.

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
execution scenarios. The preregistered preflight bounds that expansion before
the long-form event table is allocated. The report records projected and actual
row counts and memory estimates. Operators must not redirect temporary or final
output into the immutable lake.

## Validation boundary

Automated tests may use synthetic fixtures to verify causality, deterministic
ordering, hash stability, refusal semantics, and artifact completeness. Such
fixtures must remain visibly synthetic and cannot pass the real technical gate
or unblock Phase 10. Validation of this implementation consists of lint, static
typing, the full unit-test suite, and diff checks; it explicitly excludes any
real Singapore smoke.
