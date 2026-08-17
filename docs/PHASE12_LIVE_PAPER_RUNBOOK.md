# Phase 12 live-public-data Paper runbook

> **UNEXECUTED MANUAL PROCEDURE.** The commands in this document connect to live public
> Hyperliquid data only when a human operator runs them. They were not executed while this
> change was prepared. The frozen run is `TECHNICAL`, `UNCALIBRATED`, Paper-only, and never
> Gate D or profitability evidence.

## Frozen first runtime

The first candidate is `phase08-robust-pairs-btc-eth-paper-v1`, implemented by the actual
`pairs_mean_reversion_phase08` strategy through its frozen Paper adapter. It consumes public
BTC/ETH BBO frames, forms completed 30-second UTC mid-price bars, and emits deterministic
simulated IOC/taker intents only. It has no wallet, signer, private API client, exchange-order
route, Testnet executor, or real-money authority.

The source identity is `hyperliquid-mainnet-public-bbo-funding-v1`:

- HTTP info endpoint: `https://api.hyperliquid.xyz`; exact status `200`, redirects forbidden,
  and the final URL must equal the requested URL
- websocket endpoint: `wss://api.hyperliquid.xyz/ws`; exact upgrade status `101` and redirect
  limit `0`
- public bootstrap timeout: `120.0` seconds
- exact instruments: `HL:BTC:perp`, `HL:ETH:perp`
- exact public REST methods: `metaAndAssetCtxs`, `spotMetaAndAssetCtxs`, `fundingHistory`, and
  `l2Book` for BBO bootstrap/resynchronization only
- websocket subscriptions: BBO only; normalized output also records connection lifecycle and
  funding settlements
- REST bootstrap BBO is non-tradable; post-connect BBO requires exact websocket lineage, while
  any gap or staleness pauses the Paper engine and permits no execution
- source identity SHA-256 / `PaperRunConfig.data_hash`:
  `f819fbd0a88841cfda22fbbe6a5966a86df0f4b1b453ff261e8095d59c2ddd7c`
- frozen strategy adapter: `phase08-robust-pairs-paper-adapter-v2`,
  `completed_bar_pending_until_complete_post_bar_pair_frame_ioc_v2`; strategy SHA-256:
  `239ca1f27b9563a8fcacb5faa756364b6fc70240246dc086ac0be1633d8abb0d`
- Paper engine semantic build SHA-256 (v9):
  `3ed8b60caf4961a023ffbf588727bcbd1be95d3199c67acb65147163722836d2`

The canonical artifact directory is
`config/paper/phase08-robust-pairs-btc-eth-paper-v1`. It contains the frozen config, versioned
source identity, independently generated release-code manifest, runtime-environment attestation,
16 compiled PAPER/PAPER_RUNTIME semantic evidence files, readiness manifest, and index: 22
canonical files in total. Deterministic regeneration currently yields:

- config hash: `0733456db3979fbe483ddc2f259269a32763fb333f3acf94dd374992ca194c06`
- run ID: `9a6e48600569b329e4a6246369e9571537ad520aef5419be9e7dde489dbf76db`
- readiness manifest SHA-256:
  `d39ccc9b98d4147fbb758fdd95d8d48f77de757d18c22227f43ab91c9d9f158f`
- readiness profile SHA-256:
  `e727a03939928ea6de0201a7c58c542519669a6ec4f1575be89f3eaf10f0136a`
- release-code SHA-256:
  `3e3d7a6b3329ecfe14537d6a3e6c60c279b9e3d00b371283d65d00246a7b6afa`
- runtime-environment SHA-256:
  `f51df86b54d5505159841d6c8320b1b06a3d6c902f0460334d42953bb1884183`

If regeneration produces different identities after an intentional code change, stop. Review
and update the compiled registry and this runbook together; never hand-edit a generated JSON
artifact.

## Paper-specific technical ranking

This ranking considers integration safety and public input availability only. It makes no claim
about expected return.

| Rank | Research phase | Paper-specific technical assessment |
| ---: | --- | --- |
| 1 | Phase 08 robust pairs | Selected. The actual reviewed strategy now has a bounded deterministic adapter, and BTC/ETH BBO is the smallest adequate live-public input set. IOC-only simulation avoids an unsupported maker-queue claim. |
| 2 | Phase 05 cash-and-carry | Closest remaining fit to the existing two-leg engine, but it still needs reviewed spot/perp product identity, spot and perp liquidity/volume/OI, funding settlement, and a frozen public-source adapter. |
| 3 | Phase 09 momentum/regime | Publicly observable, but its volume, open-interest, funding, liquidation/regime history, longer rolling state, and repeated rebalance semantics need a larger normalized source and a new frozen adapter. |
| 4 | Phase 06 funding basket | Same public venue, but it needs a larger frozen perp universe, volume/funding history, basket state restoration, repeated rebalancing, and multi-leg admission/recovery rules. |
| 5 | Phase 07 cross-exchange funding | Needs a second public venue, deterministic cross-source clock/gap rules, and cross-venue leg/recovery semantics. No private execution dependency may be introduced. |
| 6 | Phase 11 market making | Needs restart-durable L2/trade identity and a measured queue/fill model. The first source intentionally does not project trades, and no maker fill may be assumed. |

After a stable Phase 08 technical campaign, add Phase 05 first. Phase 09 is the next sensible
source-expansion target; Phase 06 follows after durable multi-leg support. Each addition needs its own config hash, strategy hash, source identity,
readiness evidence, and restart tests.

## Frozen execution and risk assumptions

All execution quantities below are conspicuously `UNCALIBRATED`. They are conservative software
observation parameters, not measured execution quality:

| Component | Frozen behavior |
| --- | --- |
| Spread | Buy crosses the actual public ask; sell crosses the actual public bid. |
| Depth / partial fill | Actual side-specific public BBO depth is used, with a 5% maximum participation capacity. Any remainder is visible as an IOC expiry/partial fill. |
| Slippage | 2 bps base + `25 * sqrt(participation)` bps impact + 3 bps fixed adverse IOC slippage. |
| Fees | Public Tier-0 perpetual taker fee is 4.5 bps, multiplied adversely by 1.25 (5.625 bps charged). No account, volume, referral, staking, or maker discount is assumed. |
| No fill | Deterministic IOC fill eligibility is 80%, so a 20% no-fill outcome remains possible before depth capacity. |
| Latency | 250 ms ack, 500 ms fill, 750 ms inter-leg delay, 250 ms cancel. These are fixed assumptions, not measurements. |
| Maker | Strategy intents are IOC/taker. Maker probability is frozen to zero; no maker fill or rebate is assumed. |
| Funding | Only normalized public funding settlements are posted; no missing rate is invented. |

The fee table was observed on 2026-08-16 and is effective for this config only from its recorded
observation time. The validation start is fixed at `2026-08-17T00:00:00Z`, so the schedule is
not applied retroactively.

Pre-activation funding history is ignored. A flat run without a mark has zero funding effect;
a non-flat funding observation without a causal fresh BBO pauses fail closed. No synthetic
funding reserve is invented. Missing-middle or prolonged-outage funding recovery remains outside
this first supervised smoke and is Phase 12.5 backlog.

Paper risk limits are gross notional 2,000 USDC, net notional 1,000, per-instrument notional
1,000, per-order notional 250, position quantity 1, order quantity 0.25, two concurrent simulated
orders, daily loss 100, drawdown 200, market age 10 seconds, and unhedged timeout 20 seconds.
The strategy adds tighter asset quantities (ETH 0.25 and BTC 0.01) and 250 USDC pair gross.
Stale, gapped, malformed, ambiguous, or cost-schedule-missing inputs fail closed. `PAUSED` blocks
all execution, but fresh durable public marks continue risk monitoring. A marked cap, daily-loss,
drawdown, or missing-mark breach while `PAUSED` persists one deduplicated critical incident and
protects entry orders without transitioning or flattening. Only a reviewed resume followed by a
fresh bilateral frame can enter `REDUCE_ONLY` and perform the protective IOC flatten.
`MANUAL_REVIEW` is terminal and appends nothing; `kill` is irreversible for the run.

## One-time PowerShell setup

Run from an reviewed checkout on the `phase-12-live-paper` branch. The SQLite directory must be
local persistent storage, not a network share, temporary directory, or Singapore VPS path.

If the repository-local environment must be rebuilt, `scripts/bootstrap.ps1` installs the exact
hashed `requirements-ci.lock` set and then installs this checkout editable with `--no-deps`.
Do not replace that with an unpinned extras install or an in-place pip upgrade. Paper admission
independently requires all 34 pins in `requirements-runtime.lock` at their exact versions; extra
installed distributions are allowed but cannot substitute for a required pin. The frozen runtime
artifact also binds CPython version/cache tag/ABI/pointer width/byte order and the stable platform,
machine, and compiler facts, without persisting executable paths, hostnames, credentials, or
wallet material.

```powershell
Set-Location C:\Dev\hyperlab-multistrategy
$Repo = (Get-Location).Path
$Python = Join-Path $Repo ".venv\Scripts\python.exe"
$CandidateRoot = Join-Path $Repo "config\paper\phase08-robust-pairs-btc-eth-paper-v1"
$Config = Join-Path $CandidateRoot "paper-config.json"
$Manifest = Join-Path $CandidateRoot "readiness-manifest.json"
$DataRoot = Join-Path $Repo ".runtime\phase12-live-paper"
$PaperDir = Join-Path $DataRoot "paper"
$Db = Join-Path $PaperDir "paper.sqlite3"

if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) { throw "Reviewed .venv is missing" }
if ((git branch --show-current).Trim() -ne "phase-12-live-paper") { throw "Wrong branch" }
git status --short

New-Item -ItemType Directory -Force -Path $PaperDir | Out-Null
$env:HYPERLAB_MODE = "readonly"
$env:HYPERLAB_DATA_DIR = $DataRoot
$env:HYPERLAB_PAPER_DIR = $PaperDir

$RunId = (& $Python -c "import json,sys; print(json.load(open(sys.argv[1], encoding='utf-8'))['run_id'])" (Join-Path $CandidateRoot "artifact-index.json")).Trim()
if ($RunId -ne "9a6e48600569b329e4a6246369e9571537ad520aef5419be9e7dde489dbf76db") { throw "Unexpected run identity" }

Get-ChildItem Env: | Where-Object Name -Match 'PRIVATE_KEY|SEED_PHRASE|MNEMONIC|WALLET_KEY|API_KEY' | Select-Object Name
```

The final command lists names only, never values. Paper needs none of them. If any credential
variable is present in the operator shell, open a clean shell without it before continuing.

## A–K manual smoke and restart workflow

### A. Paper preflight (offline, no store and no transport)

```powershell
& $Python scripts\generate_phase12_live_paper_artifacts.py --check
if ($LASTEXITCODE -ne 0) { throw "Artifact regeneration check failed" }

& $Python -m hyperlab gate-model requirements PAPER
if ($LASTEXITCODE -ne 0) { throw "Compiled PAPER profile check failed" }

& $Python -m hyperlab gate-model check $Manifest --evidence-root $CandidateRoot
if ($LASTEXITCODE -ne 0) { throw "PAPER/PAPER_RUNTIME readiness is blocked" }

& $Python -m hyperlab paper preflight $Config
if ($LASTEXITCODE -ne 0) { throw "Paper preflight is blocked" }
```

Together, the readiness check and preflight must establish `PAPER`, `PAPER_RUNTIME`, `NONE`
credentials/network execution, `SIMULATED_ONLY`, `orders_enabled=false`,
`authorizes_real_money=false`, exact current release-code and runtime-environment digests,
`public_transport_started=false`, and `database_created=false`. Any locked distribution,
CPython, platform, machine, compiler, or attestation drift blocks before store mutation.

### B. Connect to the public source

Start the runtime in terminal 1. This is the first command in the procedure that may contact the
two exact public endpoints. It still cannot submit an exchange order.

```powershell
& $Python -m hyperlab paper run $Config --database $Db
```

The canonical config freezes `runtime_timer_interval_seconds=1.0` and
`runtime_source_poll_timeout_seconds=0.25`; the CLI derives both values from that config.
Optional cadence overrides are accepted only when they equal those frozen values exactly.

The runtime acquires a nonblocking OS-held exclusive lease for the exact canonical database and
run before engine start, reconciliation, and public-source start, and holds it through bounded
close. Standalone `paper replay` and `paper reconcile` acquire that same lease and therefore
require the runtime to be stopped. A second runtime, replay, or reconcile for that exact
database/run is rejected. The adjacent dotfile can persist after close; OS lock ownership, not
file existence, is the authority.

`pause`, `resume`, and `kill` intentionally do not take the runtime lease. Their mutations are
serialized by SQLite `BEGIN IMMEDIATE` and expected-head/hash checks; a race fails closed. Status,
report, and dashboard reads never take the runtime lease.

Never run two Paper runtime processes against the same database. If startup blocks on source
identity, readiness, stale data, a gap, or malformed data, preserve the database and logs; do not
weaken the check.

### C. Observe source health

In terminal 2, repeat the setup variables (but not `New-Item`) and run bounded read-only queries:

```powershell
& $Python -m hyperlab paper status --database $Db --run-id $RunId
& $Python -m hyperlab paper report $RunId --database $Db --after-sequence 0 --timeline-limit 200 --day-limit 31 --alert-limit 100
```

`status` verifies only bounded current head anchors and labels them
`HEAD_ANCHORS_VERIFIED_READONLY` or `HEAD_ANCHORS_FAILED_READONLY`; it never performs a full
history scan and returns at most 50 recent alerts. List status and `/api/paper` return the newest
50 runs, dropping only the oldest sentinel when truncated; readiness refuses more than 100 runs.
Inspect `integrity`, `runtime.state`,
`runtime.reconciled`, `runtime.source.status`, latest event timestamps/freshness flags by
instrument, reconnect/gap/connection counts, recent alerts, and risk state. A gap or
disconnect must make the source non-tradable until a fresh BBO resynchronization; continuity is
never interpolated.

SQLite remains in rollback-journal `DELETE` mode. To avoid holding a long read transaction that
could block the writer, status, report, `/api/paper`, and Paper readiness capture the exact durable
run-head identity, recheck it after assembly, and retry once. A second race returns explicit
`HEAD_CHANGED_RETRY` (CLI exit 2, HTTP 409, or non-ready readiness); it is never a verified or ready
result. Retry short status reads. For a guaranteed long report, replay, or reconciliation, stop
the runtime cleanly first; replay and reconciliation additionally acquire the exclusive lease.

The same report is available from the read-only dashboard API if a separate terminal starts:

```powershell
& $Python -m hyperlab serve --host 127.0.0.1 --port 8000
```

Then query only localhost:

```powershell
$Report = Invoke-RestMethod "http://127.0.0.1:8000/api/paper/$RunId/report?after_sequence=0&timeline_limit=200&day_limit=31&alert_limit=100"
$Report.runtime | ConvertTo-Json -Depth 20
```

### D. Run the actual strategy for 10–15 minutes

Leave terminal 1 running for 10–15 wall-clock minutes while sampling status/report from terminal
2. The 12 completed-bar warm-up alone requires about six minutes after synchronized BTC and ETH
BBO starts. Do not change the frozen thresholds because the window is quiet.

A correct market window may produce only `HOLD` decisions and no order/fill. That is not a
software failure and must never be replaced with a fabricated signal. If an entry threshold is
not naturally reached, record that outcome and extend technical observation later under the same
config; controlled local tests cover the order/fill path.

### E. Inspect intents and simulated fills

```powershell
$Report = (& $Python -m hyperlab paper report $RunId --database $Db --after-sequence 0 --timeline-limit 500 --day-limit 31 --alert-limit 100 | ConvertFrom-Json)
$Report.timeline.items | Select-Object sequence,occurred_at,event_type,strategy_name,action,signal,intent,fill_id,fill_price,fill_quantity,slippage_bps,fee
```

For any fill, verify that the market context exposes bid, ask, spread, BBO depth, source lineage,
and a tradable/non-stale state. Verify IOC partial/no-fill outcomes remain visible and that fill
price, slippage, and fee use the frozen adverse assumptions.

### F. Inspect ledger, positions, PnL, risk, and daily series

```powershell
$Report.account | ConvertTo-Json -Depth 20
$Report.daily.series | Format-Table
$Report.risk | ConvertTo-Json -Depth 20
$Report.classification | ConvertTo-Json -Depth 10
```

Account output includes positions, realized/unrealized/cumulative/daily PnL, equity/NAV,
drawdown, fees, funding, and notional exposure. Classification must remain
`PAPER_TECHNICAL`, `technical_only=true`, `gate_d_status=NOT_EVALUATED`, and
`profitability_evidence=false`.

### G. Stop cleanly

Return to terminal 1 and press **Ctrl+C once**. Wait for the final Paper JSON and the shell prompt.
Do not terminate the process, machine, or SQLite file during its bounded close. Then capture:

```powershell
& $Python -m hyperlab paper status --database $Db --run-id $RunId
$Before = (& $Python -m hyperlab paper report $RunId --database $Db --after-sequence 0 --timeline-limit 500 --day-limit 31 --alert-limit 100 | ConvertFrom-Json)
$BeforeAccount = $Before.account | ConvertTo-Json -Compress -Depth 20
$Before.runtime.session | ConvertTo-Json -Depth 10
$Before.timeline.items | Where-Object { $_.event_type -in @('RUNTIME_SESSION_STARTED','RUNTIME_SESSION_STOPPED') -or $_.input_type -eq 'PAPER_RUNTIME_FAILURE' }
```

The bounded session summary exposes only generation, hashed session ID, start/stop timestamps,
active/unclosed state, and recent sanitized runtime incidents. It never exposes a PID, hostname,
database path, executable path, or OS-lock path. A clean stop has `active=false` and a durable
`RUNTIME_SESSION_STOPPED`; a crash remains active/unclosed until explicit reviewed recovery.

### H. Restart from the same database

First verify the stopped state; operator mutations must not race a running runtime process.

```powershell
& $Python -m hyperlab paper replay $RunId --database $Db
if ($LASTEXITCODE -ne 0) { throw "Exact replay failed" }

& $Python -m hyperlab paper reconcile $RunId --database $Db
if ($LASTEXITCODE -ne 0) { throw "Reconciliation failed" }

& $Python -m hyperlab paper run $Config --database $Db
```

Allow fresh source synchronization, observe for several minutes, then stop again with Ctrl+C.
Machine restart uses this same sequence and the same persistent database/config; never copy the
database or rollback-journal files while a writer is active, or start a new run to hide a recovery
problem.

### I. Reconcile and replay after restart

```powershell
& $Python -m hyperlab paper replay $RunId --database $Db
if ($LASTEXITCODE -ne 0) { throw "Post-restart exact replay failed" }

& $Python -m hyperlab paper reconcile $RunId --database $Db
if ($LASTEXITCODE -ne 0) { throw "Post-restart reconciliation failed" }

& $Python -m hyperlab paper status --database $Db --run-id $RunId
```

An integrity, commit-chain, projection, config, strategy, source, or reconciliation mismatch is a
stop condition. Do not delete the database, edit the journal, or invent a position.

### J. Verify no duplicated economic effects

```powershell
$After = (& $Python -m hyperlab paper report $RunId --database $Db --after-sequence 0 --timeline-limit 500 --day-limit 31 --alert-limit 100 | ConvertFrom-Json)
$AfterAccount = $After.account | ConvertTo-Json -Compress -Depth 20

$DuplicateFills = $After.timeline.items | Where-Object { $_.fill_id } | Group-Object fill_id | Where-Object Count -GT 1
if ($DuplicateFills) { throw "Duplicate simulated fill identity detected" }

& $Python -m hyperlab paper replay $RunId --database $Db
if ($LASTEXITCODE -ne 0) { throw "Replay equivalence failed" }
```

Reconciliation may append an audit event, so event sequence can increase. It must not duplicate a
fill, fee, funding settlement, realized PnL, or position effect. For a restart with no new market
effects, compare `$BeforeAccount` and `$AfterAccount`; they must match. If the timeline exceeds
500 items, follow `timeline.next_after_sequence` with bounded pages before checking all fill IDs.

### K. Run the first supervised 10-15 minute technical Paper smoke

Only after A–J pass, run the same command under a reviewed local process supervisor that launches
exactly one instance and reuses the same config/database:

```powershell
& $Python -m hyperlab paper run $Config --database $Db
```

Launch exactly one instance and reuse the same config/database. Monitor bounded `status` and
`report` output, disk capacity,
reconnects, gaps, staleness, incidents, risk state, drawdown, positions, and daily UTC series.
Live `status` is head-only and bounded. Stop after 10-15 minutes even if it is quiet; do not infer
24/7 readiness. A report can return `HEAD_CHANGED_RETRY` under sustained
commits; stop cleanly before a guaranteed long report. Periodically run `replay` only while the
runtime is stopped. Preserve all losing/no-fill variants and incidents.

## Safe pause, reviewed resume, and irreversible kill

These commands journal an exact reason and artifact hash; they are not HTTP control endpoints.
`pause` and irreversible `kill` are emergency controls that may be issued while the runtime is
active. SQLite transaction and expected-head guards serialize them or fail closed; never launch
multiple operator controls concurrently. For reviewed `resume`, use the stopped/reviewed sequence
below. Resume also requires both durable release-code and runtime-environment digests to equal the
current reviewed checkout and Python environment.

Pause new simulated entries:

```powershell
& $Python -m hyperlab paper pause $RunId --database $Db --reason "Operator pause for source-health review"
```

After reviewing alerts, replay, and reconciliation, refresh the public source while the durable
state remains `PAUSED`: run the normal `paper run` command, wait for synchronized fresh BTC/ETH
BBO in the read-only report, then stop it cleanly with Ctrl+C. No new entry is admitted while the
state is `PAUSED`. Immediately resume only that durable `PAUSED` run, before its 10-second market
freshness limit expires:

```powershell
& $Python -m hyperlab paper resume $RunId --database $Db --review-reason "Replay and source-health review completed; no unresolved incident"
```

After a successful reviewed resume, relaunch the single runtime writer:

```powershell
& $Python -m hyperlab paper run $Config --database $Db
```

If a hard process/OS crash leaves an exact durable unclosed runtime session, ordinary fresh-BBO
resume cannot resolve that incident. Stop every writer, review the latest critical
`UNCLOSED_RUNTIME_SESSION` `PAPER_RUNTIME_FAILURE`, and use the explicit offline mode:

```powershell
& $Python -m hyperlab paper resume $RunId --database $Db --review-reason "Unclosed runtime session and durable incident reviewed" --offline-unclosed-recovery
```

This mode must acquire the same exact runtime lease, recheck release/config/environment under the
lease, and atomically match the reviewed critical incident count and timestamp. It fails unless
the latest critical incident is the deterministic unclosed-session failure. It can move only to
`FLAT` without positions or `REDUCE_ONLY` with positions; it never admits a fresh entry. Release
the lease on command completion, then start the normal runtime and require a new bilateral
public-source bootstrap before any strategy execution.

`MANUAL_REVIEW` can never resume. To irreversibly kill this exact run and cancel remaining
simulated orders locally:

```powershell
& $Python -m hyperlab paper kill $RunId --database $Db --reason "Unresolved integrity or risk incident" --confirm-run-id $RunId
```

Never automate `resume` or `kill`, and never reuse a killed run ID.

## TECHNICAL versus future VALIDATION Paper

This config has `run_kind=TECHNICAL`, uncalibrated data/execution/cost status, no economic
prerequisite evidence, and `economic_prerequisites_satisfied=false`. Its observations can validate
software behavior and expose operational/cost-model limitations. They cannot establish strategy
profitability and do not count toward Gate D.

Running the read-only diagnostic below is permitted, but a technical run is expected to remain
ineligible; exit code 2 is not a Paper runtime failure:

```powershell
& $Python -m hyperlab paper gate $RunId --database $Db
```

A future validation campaign must begin prospectively with independently satisfied Gate B/C and
calibration prerequisites, a separately frozen `VALIDATION` config/hash/run ID, and its own start
time. No technical event, fill, day, or PnL may be relabelled or retroactively converted into
validation/Gate-D evidence.

## Known limitations to retain visibly

- Fee evidence is public Tier 0, but spread, impact, capacity, fill probability, and all latency
  values remain uncalibrated assumptions.
- BBO provides top-of-book depth, not a complete executable L2 curve. The 5% capacity and adverse
  slippage are safeguards, not calibration evidence.
- Hyperliquid offers no public replay cursor that proves websocket continuity. Reconnects create
  explicit gaps/resynchronization; the runtime never interpolates missing market state.
- A quiet 10–15 minute smoke may produce no entry or fill. Thresholds must remain frozen.
- The bounded report paginates the timeline (maximum 500 events), recent alerts (200), and daily
  UTC series (366). Output and client memory are bounded, but daily projection, source, and ledger
  SQL work still scales with retained history. Consumers must follow cursors/pages rather than
  load unbounded history. The first manual smoke must record report latency and database growth.
- Phase 12.5 must add indexed or incremental daily summaries and evaluate source-rate profiling,
  capacity alerts, retention/archive policy, and safe event-coalescing boundaries. Those are known
  debts, not evidence for this Phase 12 launch.
- A local synthetic 1,000-BBO fixture measured about 82.8 commits/second and about 6.18 KB of
  SQLite growth per BBO. The illustrative extrapolation at exactly 1 BBO/second is about
  0.53 GB/day; it is not a measured public-source rate and does not prove 24/7 capacity.
- Strategy restoration streams all lifetime `PUBLIC_MARKET_EVENT` inputs from genesis. RAM use is
  bounded, but restart SQL and CPU are O(N) in retained inputs; restart time is not independent of
  journal length.
- Phase 12.5 must add integrity-bound incremental strategy checkpoints or summaries plus explicit
  restart and long-soak benchmarks before making a sustained restart-latency claim.
- The sustained smoke must monitor actual source rate, disk growth/free space, queue occupancy,
  commit latency, report latency, reconnects, and failure alerts. Stop fail closed before resource
  exhaustion; do not infer capacity from the synthetic fixture.
- Sustained 24/7 readiness still requires the human smoke, restart/reconcile exercise, operational
  supervision, disk monitoring, and continuing review of observed source/cost behavior.
- This authorization targets the first supervised 10-15 minute public Paper smoke only. It does
  not certify indefinite 24/7 operation, missing-middle funding recovery, prolonged source
  outages, or production capacity.
