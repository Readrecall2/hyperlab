# Phase 12 technical readiness and promotion-evidence audit

Audit date: 2026-08-16. Baseline: `da6354ee5372e8f32fc190a5f18bc2641d605f1d`.

Policy status: `PAPER` and `TESTNET` technical readiness are independent of Gates
B/C/D. At this audit's Phase 12 baseline, the Paper runtime registry had no
complete strategy + public-source candidate or compiled scope-bound semantic
verifier set, and no Phase 13 Testnet adapter existed. The later Phase 13 branch
adds a separate Testnet-only service; it does not change the Paper finding or
constitute a live Testnet/Gate E result. Gate D promotion evidence also remains
**blocked**. These
are documentation findings, not new runtime status tokens. This audit is not itself
an authorization receipt.

## Persisted evidence inventory

`reports/` and `data/reports/` contain only placeholder `.gitkeep` files and the
tracked `data/reports/.hyperlab-volume` marker. No Gate B or Gate C report for
Phases 05, 06, 07, 08, 09, or 11 is present anywhere in tracked Git history.
Implementation, unit tests, `VALIDATION.md`, a `passed` field, a `CALIBRATED`
label, or a syntactically valid digest is not saved economic evidence. This blocks
real-money promotion and a Gate-D-qualifying campaign; it does not by itself block
a non-authorizing `PAPER_RUNTIME` or `TESTNET_EXECUTION` technical readiness check.

Future admission must recompute every referenced artifact's SHA-256 and enforce the
canonical strategy thresholds. In particular, it must not trust the research CLI
options that permit smaller history/event minimums than the phase contracts.

| Rank | Candidate | Gate B | Gate C | Cost/fill/latency evidence | Current technical/promotion gaps |
|---:|---|---|---|---|---|
| 1 | Phase 05 cash-and-carry | **BLOCKED**. No saved audit of the canonical 720 hourly point-in-time observations, verified spot/perp identity, realized funding, depth, volume, OI, availability/finality, or calibrations. | **BLOCKED**. No frozen split/variant registry, single final reveal, stress report, calibrated execution report, or complete-close benchmark evidence. | `config/research.toml` is explicitly `UNCALIBRATED`; its Hyperliquid taker fees are too low. | Missing real Gate B/C artifacts, a frozen incremental strategy, funding/context runtime input, and calibrated execution. |
| 2 | Phase 06 funding basket | **BLOCKED**. No saved 90-day, at-least-six-perpetual (including BTC/ETH) point-in-time/lifecycle audit. | **BLOCKED**. No saved preregistered final evaluation and stress/execution evidence. | No calibrated multi-asset rebalancing depth, latency, or fill evidence. | Real data/lifecycle/calibration evidence and a frozen paper strategy are absent. |
| 3 | Phase 07 cross-exchange funding | **BLOCKED**. No saved 30 aligned days across Hyperliquid and Binance with identities, marks/oracles/settlements, funding calendars, venue risk, and transfer policy. | **BLOCKED**. No saved frozen final evaluation. | No Binance fee/slippage, margin/liquidation, transfer delay/cost, or outage calibration. | Requires a second public source and a richer cross-venue execution/risk model. |
| 4 | Phase 08 pairs | **BLOCKED**. No saved 180-day, at-least-six-asset point-in-time/lifecycle/depth/funding audit. | **BLOCKED**. No saved frozen final evaluation. | No calibrated multi-leg spread, slippage, latency, partial/no-fill, or funding evidence. | Real history, lifecycle, calibration, and a frozen strategy are absent. |
| 5 | Phase 09 momentum/regime | **BLOCKED**. No saved 365-day, at-least-six-asset point-in-time audit covering lifecycle, funding, OI, volume, observed liquidations, depth, and regimes. | **BLOCKED**. No saved frozen final evaluation. | No calibrated directional-perp spread, slippage, latency, fill, or funding evidence. | Real history/calibration and a frozen strategy are absent. |
| 6 | Phase 11 L2 market making | **BLOCKED**. No saved real replay/calibration audit; Hyperliquid public L2 snapshots do not expose the server sequence required by the current contract. | **BLOCKED**. No saved final replay/stress evidence; current status remains `EVENT_REPLAY_RESEARCH_ONLY`. | No queue-ahead, quote/cancel latency, post-only reject, partial-fill, adverse-markout, or hedge calibration. | `BLOCKED_SEQUENCE_UNOBSERVABLE`, missing reference-venue data, and a BBO-only paper event is insufficient for atomic L2 replay. |

The shortest legitimate path to a future Gate-D-qualifying economic campaign is
therefore Phase 05 cash-and-carry, based on evidence burden only. This is not a
profitability ranking. Technical Paper readiness is evaluated separately, and
`SELECTED PAPER CANDIDATE` remains `NONE` because no complete runtime/source bind
exists.

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
`economic_eligibility=false` and does not call itself `CALIBRATED`. It may support
an explicitly conservative, non-promotable Paper technical run once the remaining
runtime/source requirements pass. Before Phase 05 can start a Gate-D-qualifying
`VALIDATION` campaign, separate versioned evidence must cover:

1. actual bilateral BBO spread and executable L2 depth per frozen spot/perp leg;
2. size/horizon-dependent slippage, capacity, and adverse emergency exit;
3. ack, fill, cancel, maker timeout, and inter-leg latency;
4. maker queue/partial/no-fill/timeout and IOC partial/no-fill behavior;
5. funding settlement and all fee-rule effective intervals; and
6. a refresh/review of the public fee page immediately before freezing the run.

Any unsupported instrument or fee-page change fails closed. A fee snapshot alone
does not make `PaperExecutionConfig` or `CostSchedule` calibrated, so the status
remains **blocked for Gate D**, not globally blocked for Paper software exercise.

## Technical Paper bind and deterministic identity

A technical Paper configuration does not require Gate B/C evidence. It must bind
the exact environment `PAPER`, purpose `PAPER_RUNTIME`,
`authorizes_real_money=false`, frozen strategy/build/source/risk identities,
required instruments, parameters, conservative cost labels, seed and start time.
The receipt and configuration must match exactly; no Paper receipt can be consumed
for Testnet or Mainnet.

No legitimate production Paper bind exists today because the frozen candidate
strategy, complete public source descriptor/routing and approved runtime factory do
not exist, and no exact semantic verifier set is compiled. Hash-matching files or
self-declared `PASS` fields cannot fill that gap. Freezing placeholders would
manufacture technical readiness. The absent
Gate B/C reports are a separate promotion gap, not the reason this runtime is
currently blocked.

Once every technical prerequisite has independently passed, create and review one
canonical `PaperRunConfig` snapshot for the selected candidate. The engine derives:

```text
config_hash = SHA256(canonical PaperRunConfig JSON)
run_id = SHA256(canonical {schema_version: 1, kind: "paper_run", components: [config_hash]})
```

There is therefore no legitimate run ID today. It will be deterministic only after
the technically admissible configuration is frozen. Compute and review it without
starting the runtime:

```powershell
.\.venv\Scripts\python.exe -c "import json; from pathlib import Path; from hyperlab.paper.models import PaperRunConfig; p=Path(r'config/paper/SELECTED-PAPER-CONFIG.json'); print(PaperRunConfig.from_dict(json.loads(p.read_text(encoding='utf-8'))).run_id)"
```

The operational authority is the single multi-run SQLite store
`data/paper/paper.sqlite3`. The exact eventual start command is:

```powershell
.\.venv\Scripts\python.exe -m hyperlab paper run .\config\paper\SELECTED-PAPER-CONFIG.json --database .\data\paper\paper.sqlite3 --timer-interval-seconds 1 --source-poll-timeout-seconds 0.25
```

Today it must fail before creating state because the approved runtime registry is
empty and no complete candidate-specific source/strategy protocol or semantic
verifier set exists. Do not register factories until core verifies the technical
evidence and binds the reviewed implementation to the exact `PAPER` /
`PAPER_RUNTIME` receipt.

A later Gate-D-qualifying campaign is a separate qualifying evidence campaign. It starts only
after Gates B/C and calibrated execution evidence pass, with a newly frozen
`VALIDATION` configuration, a preregistered threshold of at least 30 completed
cycles and the true UTC start.
Earlier technical Paper time may inform development but cannot be backfilled into
the 42-day Gate D window.

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

The current Gate D command is a read-only real-money-promotion diagnostic bound to
one stable durable
journal head. It intentionally cannot report `PASS`: the checks
`paper_readiness_receipt_bound` (exact `PAPER` / `PAPER_RUNTIME` receipt),
`durable_runtime_source_attestation`, and
`gate_d_artifact_bytes_verified` are false facts derived inside the evaluator, not
caller options. This prevents a directly constructed journal, arbitrary
`MarketEvent`s, or digest-shaped stress/coverage/resilience assertions from being
mistaken for a promotable campaign.

A future Gate D evidence implementation must persist and reverify the approved
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
Gate D review remains required before real money. Phase 10 lead-lag is not a
dependency of Phase 12 unless the selected strategy consumes Phase 10 output.

Phase 13 Testnet may be developed and become technically ready without Gate D.
Its exact `TESTNET_EXECUTION` receipt requires isolated endpoint/credentials, order
FSM, reconciliation, recovery, limits, kill behavior and audit. Gate E is the
completed Testnet evidence later required for real-money promotion; it is not
permission to begin Testnet. The later `services/testnet-executor` implementation
is documented in [`TESTNET_EXECUTOR_PHASE13.md`](TESTNET_EXECUTOR_PHASE13.md); its
local tests do not themselves establish a completed Testnet exercise or Gate E.
