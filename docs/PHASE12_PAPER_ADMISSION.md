# Phase 12 first-campaign admission audit

Audit date: 2026-08-16. Baseline: `da6354ee5372e8f32fc190a5f18bc2641d605f1d`.

Status: **`BLOCKED_PRECONDITION_NOT_MET`**. This is a software and evidence audit,
not Gate B, Gate C, Gate D, or economic evidence. It does not authorize a paper
campaign or Phase 13.

## Persisted evidence inventory

`reports/` and `data/reports/` contain only placeholder `.gitkeep` files and the
tracked `data/reports/.hyperlab-volume` marker. No Gate B or Gate C report for
Phases 05, 06, 07, 08, 09, or 11 is present anywhere in tracked Git history.
Implementation, unit tests, `VALIDATION.md`, a `passed` field, a `CALIBRATED`
label, or a syntactically valid digest is not saved economic evidence.

Future admission must recompute every referenced artifact's SHA-256 and enforce the
canonical strategy thresholds. In particular, it must not trust the research CLI
options that permit smaller history/event minimums than the phase contracts.

| Rank | Candidate | Gate B | Gate C | Cost/fill/latency evidence | Phase 12 admission blocker |
|---:|---|---|---|---|---|
| 1 | Phase 05 cash-and-carry | **BLOCKED**. No saved audit of the canonical 720 hourly point-in-time observations, verified spot/perp identity, realized funding, depth, volume, OI, availability/finality, or calibrations. | **BLOCKED**. No frozen split/variant registry, single final reveal, stress report, calibrated execution report, or complete-close benchmark evidence. | `config/research.toml` is explicitly `UNCALIBRATED`; its Hyperliquid taker fees are too low. | Missing real Gate B/C artifacts, a frozen incremental strategy, funding/context runtime input, and calibrated execution. |
| 2 | Phase 06 funding basket | **BLOCKED**. No saved 90-day, at-least-six-perpetual (including BTC/ETH) point-in-time/lifecycle audit. | **BLOCKED**. No saved preregistered final evaluation and stress/execution evidence. | No calibrated multi-asset rebalancing depth, latency, or fill evidence. | Real data/lifecycle/calibration evidence and a frozen paper strategy are absent. |
| 3 | Phase 07 cross-exchange funding | **BLOCKED**. No saved 30 aligned days across Hyperliquid and Binance with identities, marks/oracles/settlements, funding calendars, venue risk, and transfer policy. | **BLOCKED**. No saved frozen final evaluation. | No Binance fee/slippage, margin/liquidation, transfer delay/cost, or outage calibration. | Requires a second public source and a richer cross-venue execution/risk model. |
| 4 | Phase 08 pairs | **BLOCKED**. No saved 180-day, at-least-six-asset point-in-time/lifecycle/depth/funding audit. | **BLOCKED**. No saved frozen final evaluation. | No calibrated multi-leg spread, slippage, latency, partial/no-fill, or funding evidence. | Real history, lifecycle, calibration, and a frozen strategy are absent. |
| 5 | Phase 09 momentum/regime | **BLOCKED**. No saved 365-day, at-least-six-asset point-in-time audit covering lifecycle, funding, OI, volume, observed liquidations, depth, and regimes. | **BLOCKED**. No saved frozen final evaluation. | No calibrated directional-perp spread, slippage, latency, fill, or funding evidence. | Real history/calibration and a frozen strategy are absent. |
| 6 | Phase 11 L2 market making | **BLOCKED**. No saved real replay/calibration audit; Hyperliquid public L2 snapshots do not expose the server sequence required by the current contract. | **BLOCKED**. No saved final replay/stress evidence; current status remains `EVENT_REPLAY_RESEARCH_ONLY`. | No queue-ahead, quote/cancel latency, post-only reject, partial-fill, adverse-markout, or hedge calibration. | `BLOCKED_SEQUENCE_UNOBSERVABLE`, missing reference-venue data, and a BBO-only paper event is insufficient for atomic L2 replay. |

The shortest legitimate path is therefore Phase 05 cash-and-carry, based on
readiness and evidence burden only. This is not a profitability ranking and does
not select it for admission. `SELECTED PAPER CANDIDATE` remains `NONE`.

## Public market source

The existing Hyperliquid collector is public and read-only. A generic Phase 12
adapter may consume the collector's already-normalized records; it must not create
a second lake writer or a private venue client. The implemented seam admits only
exact latest-schema BBO and connection records. Its descriptor hash binds the
`hyperliquid` source venue to the repository's canonical `HL` paper namespace, the
explicit instrument mapping, accepted schema versions, feed/global-event policy and
bounded FIFO capacity. Schema/type mismatch, queue saturation, or a global
connection event spanning several mapped instruments is terminal. Unilateral,
non-positive or crossed BBOs are withheld, and the following bilateral BBO is
required to recover.

The exact spot/perpetual kind remains an explicit route assertion: a normalized
venue/asset BBO alone does not prove that economic identity. The source identity
records this mapping but does not turn it into evidence. Admission must separately
byte-bind and review point-in-time instrument metadata for every route before any
factory can be registered.

Trade projection is deliberately terminal: the current `MarketEvent.event_id`
includes local `received_at`, while the adapter had only process-local trade-ID
memory. A redelivery after restart could therefore trigger the strategy twice. A
future source must persist and restore the venue/asset/trade-ID economic identity,
or fan out only after durable sole-writer deduplication. Candidate use also needs a
reviewed sole-collector fan-out/atomicity contract. This BBO/connection seam is not
a complete Phase 05 source: cash-and-carry additionally needs point-in-time
spot/perp identity, funding settlement, market context, OI, volume,
lifecycle/finality, and deterministic routing into a frozen strategy. Phase 11
needs its full L2 event schema and cannot be reduced to `MarketEvent`. Consequently
the candidate-specific public source remains **BLOCKED**, without any credential,
wallet, signer, permission, or exchange-order requirement.

## Phase 05 cost contract

The checked-in public-fee evidence artifact is
`config/paper/hyperliquid-tier0-fees-2026-08-16.json`; it byte-binds
`config/paper/hyperliquid-fees-source-receipt-2026-08-16.json`. The receipt records
the UTC retrieval instant, HTTP ETag, decoded UTF-8 length/SHA-256, unknown
publisher-effective interval, and extracted Tier-0 facts. It also byte-binds the
immutable structured table transcription
`config/paper/hyperliquid-tier0-fee-table-capture-2026-08-16.json`, whose percent
rows deterministically reproduce the schedule's basis points. The schedule records
standard perpetual maker/taker `1.5/4.5` bps and standard spot maker/taker
`4.0/7.0` bps. It assumes no account, staking, referral, volume, aligned-quote, or
maker-rebate benefit and excludes special/unreviewed products.

This makes the observed table rows reproducible offline and change-detectable, but
does not reconstruct the unstored full response, provide a publisher signature, or
establish a publisher effective-from date. It deliberately has
`economic_eligibility=false` and does not call itself `CALIBRATED`. Before Phase 05
can run as `VALIDATION`, separate versioned evidence must cover:

1. actual bilateral BBO spread and executable L2 depth per frozen spot/perp leg;
2. size/horizon-dependent slippage, capacity, and adverse emergency exit;
3. ack, fill, cancel, maker timeout, and inter-leg latency;
4. maker queue/partial/no-fill/timeout and IOC partial/no-fill behavior;
5. funding settlement and all fee-rule effective intervals; and
6. a refresh/review of the public fee page immediately before freezing the run.

Any unsupported instrument or fee-page change fails closed. A fee snapshot alone
does not make `PaperExecutionConfig` or `CostSchedule` calibrated, so the overall
`COST SCHEDULE` status remains **BLOCKED**.

## Campaign freeze and deterministic identity

A canonical campaign configuration must not be created yet: its Gate B/C evidence,
data/calibration hashes, frozen strategy hash, required instruments, parameters,
source descriptor, complete execution model, risk limits, seed, cycle threshold,
and true start timestamp do not exist. Freezing placeholders would manufacture an
identity that is not eligible to start.

Once every prerequisite has independently passed, create exactly
`config/paper/phase05-cash-and-carry-validation.json` as the complete canonical
`PaperRunConfig` snapshot. Set `run_kind=VALIDATION`, at least 30 preregistered
cycles, the actual UTC `validation_started_at`, and byte-bound evidence hashes.
The engine then derives:

```text
config_hash = SHA256(canonical PaperRunConfig JSON)
run_id = SHA256(canonical {schema_version: 1, kind: "paper_run", components: [config_hash]})
```

There is therefore no legitimate campaign ID today. It will be deterministic only
after the final eligible configuration is frozen. Compute and review it without
starting the runtime:

```powershell
.\.venv\Scripts\python.exe -c "import json; from pathlib import Path; from hyperlab.paper.models import PaperRunConfig; p=Path(r'config/paper/phase05-cash-and-carry-validation.json'); print(PaperRunConfig.from_dict(json.loads(p.read_text(encoding='utf-8'))).run_id)"
```

The operational authority is the single multi-run SQLite store
`data/paper/paper.sqlite3`. The exact eventual start command is:

```powershell
.\.venv\Scripts\python.exe -m hyperlab paper run .\config\paper\phase05-cash-and-carry-validation.json --database .\data\paper\paper.sqlite3 --timer-interval-seconds 1 --source-poll-timeout-seconds 0.25
```

Today it must fail before creating state because the approved runtime registry is
empty and no trusted candidate-specific measured semantic protocol exists. A typed
receipt containing echoed hashes and PASS booleans is explicitly non-authorizing.
Do not create the named configuration or register factories until core derives both
Gate decisions from canonical measured results, binds the reviewed implementation
behavior to the strategy artifact, and a human reviews the complete frozen bind.

## Operation, recovery, and Gate D

Use these commands only after a legitimate run exists:

```powershell
.\.venv\Scripts\python.exe -m hyperlab paper status --database .\data\paper\paper.sqlite3 --run-id <RUN_ID>
.\.venv\Scripts\python.exe -m hyperlab paper replay <RUN_ID> --database .\data\paper\paper.sqlite3
.\.venv\Scripts\python.exe -m hyperlab paper reconcile <RUN_ID> --database .\data\paper\paper.sqlite3
.\.venv\Scripts\python.exe -m hyperlab paper gate <RUN_ID> --database .\data\paper\paper.sqlite3
```

On a planned or unplanned restart, preserve the database and immutable config,
run read-only status/replay inspection, reconcile, then rerun the exact same start
command. Startup restores, verifies, replays, and reconciles before polling. Never
delete, rewrite, or replace a losing, interrupted, or `MANUAL_REVIEW` run.

The current Gate D command is a read-only diagnostic bound to one stable durable
journal head. It intentionally cannot report `PASS`: the checks
`approved_admission`, `durable_runtime_source_attestation`, and
`gate_d_artifact_bytes_verified` are false facts derived inside the evaluator, not
caller options. This prevents a directly constructed journal, arbitrary
`MarketEvent`s, or digest-shaped stress/coverage/resilience assertions from being
mistaken for a promotable campaign.

A future authorizing Gate D implementation must persist and reverify the approved
runtime/source/admission identity, derive continuous coverage from durable source
lineage, and load/hash/validate the actual Gate D artifact bytes. It must then still
require all canonical operational checks:

- approved `VALIDATION` config with freshly recomputed Gate B/C semantics and
  calibrated data/execution;
- continuous public coverage and fresh required channels;
- at least 42 real forward days and the frozen cycle threshold (never below 30);
- at least 14 consecutive incident-free days;
- exact replay/reconciliation;
- persisted restart, disconnect, partial-fill, and crash-recovery exercises;
- a positive stressed net result bound after the final economic event.

The Gate command has no threshold/evidence/attestation override and returns nonzero
in this checkout. Even after a future byte-verified automated PASS, explicit human
Gate D review remains required. Phase 10 lead-lag is not a dependency of Phase 12
unless the selected strategy itself consumes Phase 10 output; Phase 05 does not.
Gate E and Phase 13 must wait for a completed, human-reviewed Gate D.
