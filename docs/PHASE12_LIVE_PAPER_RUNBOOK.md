# Phase 12 live-public-data Paper runbook

> **VALIDATION BOUNDARY.** The Paper runtime/recovery sequence was validated on the Linux VPS
> at commit `ba84444240af6cdbfc3df4f0170866a2a0c15c1f`: clean stop, exact replay,
> reconciliation, same-database generation-2 restart, post-restart `REPLAY_EXACT`, final
> `FLAT`, `active=false`, and `unclosed=false`. The systemd supervision added below is a new
> deployment procedure and is not represented as VPS-validated until the operator executes it
> from its final reviewed commit. All commands use live **public** Hyperliquid data only. The
> frozen run remains `TECHNICAL`, `UNCALIBRATED`, Paper-only, and never Gate D or profitability
> evidence.

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
  `da9784ec2c794340c482c389dda6d278373a24429baca48d4e363adf2a872525`
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

- config hash: `62ebaaf09977f88d4a75e7dc056ba23300c48453370fc9c196bd8193bec6aa3f`
- run ID: `e0a68aa1ec8b746bc877c537e35a0d5f97deba7e4a97832a75ff04002da4fcba`
- readiness manifest SHA-256:
  `5a3742e3c0ae101bcaa5bcfc540fa88be522f6c20d05b6b7400d15b0a6a9846f`
- readiness profile SHA-256:
  `e727a03939928ea6de0201a7c58c542519669a6ec4f1575be89f3eaf10f0136a`
- release-code SHA-256:
  `bb44d725ebb152c845111868fb2bfb9526105cadc0f9487e3505178aba87e0db`
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
if ($RunId -ne "e0a68aa1ec8b746bc877c537e35a0d5f97deba7e4a97832a75ff04002da4fcba") { throw "Unexpected run identity" }

Get-ChildItem Env: | Where-Object Name -Match 'PRIVATE_KEY|SEED_PHRASE|MNEMONIC|WALLET_KEY|API_KEY' | Select-Object Name
```

The final command lists names only, never values. Paper needs none of them. If any credential
variable is present in the operator shell, open a clean shell without it before continuing.

## Linux/VPS operator-specific runtime attestation

The checked-in artifact set is the reviewed Windows realization. Never copy, rename, or relabel
its `runtime-environment-attestation.json` for Linux. On the reviewed Linux VPS, generate a
separate complete 22-file operator bundle with that machine's Python. The generator rebinds the
runtime attestation, Paper config hash/run ID, readiness subject, semantic evidence, and artifact
index together. Admission accepts the operator bundle only when every Paper config field except
`runtime_environment_sha256` is byte-semantically identical to the compiled candidate. Release
code, lock file, all 34 exact distributions, strategy, risk, source identity, costs, cadence, and
Paper-only scope therefore remain unchanged.

Use a new local persistent directory outside the Git checkout. Replace
`<FINAL_REVIEWED_COMMIT>` with the single final commit from the reviewed change report:

```bash
cd /opt/hyperlab-multistrategy
PYTHON=.venv/bin/python
EXPECTED_COMMIT="<FINAL_REVIEWED_COMMIT>"
OPERATOR_ROOT="/var/lib/hyperlab/phase12-live-paper/authorization-$EXPECTED_COMMIT-linux-cpython-3.12.13"

test "$(git branch --show-current)" = "phase-12-live-paper"
test "$(git rev-parse HEAD)" = "$EXPECTED_COMMIT"
test -z "$(git status --porcelain)"
test ! -e "$OPERATOR_ROOT"

"$PYTHON" scripts/generate_phase12_live_paper_artifacts.py \
  --output-root "$OPERATOR_ROOT"
"$PYTHON" scripts/generate_phase12_live_paper_artifacts.py \
  --output-root "$OPERATOR_ROOT" --check
"$PYTHON" -m hyperlab gate-model check \
  "$OPERATOR_ROOT/readiness-manifest.json" \
  --evidence-root "$OPERATOR_ROOT"
"$PYTHON" -m hyperlab paper preflight "$OPERATOR_ROOT/paper-config.json"
```

Generation fails if the lock is not exact, any required distribution is missing or drifted, or
CPython/platform facts change during capture. Readiness and preflight independently regenerate
the current runtime identity and fail before factories, public transport, or SQLite creation when
the operator bundle belongs to another OS/interpreter. Keep using the same operator config for
the runtime:

```bash
"$PYTHON" -m hyperlab paper run \
  "$OPERATOR_ROOT/paper-config.json" \
  --database /var/lib/hyperlab/phase12-live-paper/paper/paper.sqlite3
```

The Windows checked-in config remains valid only for its exact Windows attestation; the Linux
operator config remains valid only for its exact Linux attestation. Neither is convertible into
the other, and both retain `authorizes_real_money=false` and `orders_enabled=false`.


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
close. Standalone `paper replay`, `paper reconcile`, and reviewed `paper resume` (standard or
explicit offline unclosed-session recovery) acquire that same lease and therefore require the
runtime to be stopped. A second runtime or any of those stopped-runtime operations for that exact
database/run is rejected. The adjacent dotfile can persist after close; OS lock ownership, not
file existence, is the authority.

`pause` and `kill` intentionally remain active-runtime emergency controls. Their mutations are
serialized by SQLite `BEGIN IMMEDIATE` and expected-head/hash checks; a race fails closed.
`resume` rechecks the durable config and release under the lease before constructing a mutable
store or engine. Status, report, and dashboard reads never take the runtime lease.

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

The collector status also exposes `observability.source_queue`. Monitor `pending_frames`,
`high_water_frames`, `oldest_pending_age_seconds`, `latest_adapted_age_seconds`, and
`coalesced_bbo_frames`; wire freshness alone does not prove that Paper is draining current BBOs.
Pending BBOs use latest-value replacement only for the same instrument and UTC minute. Funding,
connection/gap events, and minute boundaries remain FIFO causal barriers.

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

## Linux systemd supervision for the 48-72 hour technical soak

The supervisor is deliberately outside the frozen Paper release-code manifest. It starts the
already reviewed `paper run` command with the already reviewed Linux operator bundle and the
same persistent SQLite database/run ID. It does not regenerate artifacts, create a run, copy or
move the database, or implement accounting/reconciliation in shell. The exact Paper replay,
runtime lease, engine pause, readiness, and runtime startup paths remain authoritative.

### Restart policy and exact unit behavior

`hyperlab-paper.service` has these semantics:

- one systemd service and the existing nonblocking `PaperRuntimeLease` enforce one writer for the
  exact database/run;
- `ExecCondition` runs offline Paper preflight, release/runtime identity checks, disk thresholds,
  the OS lease check, durable config/run matching, and full exact replay before every start;
- automatic admission is intentionally narrower than normal interactive recovery: only a
  reconciled `FLAT` run with no active position/order and no unresolved critical incident is
  admitted;
- a reviewed offline unclosed-session recovery may retain the old durable session as active until
  the runtime creates its replacement generation; the exact reviewed recovery input is required;
- an unreviewed unclosed session is atomically latched through the existing Paper engine as a
  sanitized `UNCLOSED_RUNTIME_SESSION` critical failure, then refused;
- `MANUAL_REVIEW`, `PAUSED`, non-`FLAT`, killed, unreconciled, identity-drifted, replay-failed,
  active-writer, active-position/order, low-disk, and ambiguous runs are refused;
- an `ExecCondition` refusal exits nonzero without running `ExecStart`; systemd records a skipped
  condition and does not loop the runtime;
- `Restart=on-failure` is bounded by three starts per 15 minutes. A cleanly closed transient exit
  can restart after 30 seconds; a durable unsafe state makes the next condition refuse;
- reboot startup is enabled through `multi-user.target`. A planned reboot sends `SIGTERM`, waits up
  to 90 seconds for cooperative close, and can start automatically after boot. A hard power loss
  leaves an unclosed session and therefore requires the reviewed manual path below;
- systemd runs the process as `hyperlab-paper`, with no capabilities, no-new-privileges,
  `ProtectSystem=strict`, an explicit read-only `/opt/hyperlab-multistrategy`, the single writable
  `/var/lib/hyperlab/phase12-live-paper` root, and only `AF_UNIX/AF_INET/AF_INET6`;
- the unit pins `HYPERLAB_DATA_DIR=/var/lib/hyperlab/phase12-live-paper` and
  `HYPERLAB_PAPER_DIR=/var/lib/hyperlab/phase12-live-paper/paper`. The public collector therefore
  writes `paper/phase12-public-source-status.json` under that persistent root. Its atomic
  `phase12-public-source-status.tmp` write, rename, and cleanup remain in the same writable
  directory and filesystem;
- the system service environment does not inherit an SSH shell. Testnet/wallet/key variables are
  explicitly unset, and the environment file is restricted to paths and disk thresholds.

The service still exposes only `PAPER`, `PAPER_RUNTIME`, `credential_scope=NONE`,
`execution_network=NONE`, `authorizes_real_money=false`, and `orders_enabled=false`.

The narrowly scoped mutable-path inventory for the supervised runtime is the explicitly selected
SQLite database and its rollback journal in the persistent `paper` directory, the existing
runtime lease file beside that database, and the collector status JSON/temp pair above. No other
repo-relative mutable runtime path was found in the Paper startup/steady-state call graph;
`PYTHONDONTWRITEBYTECODE=1` also prevents bytecode writes in the checkout. Journald and systemd's
private temporary directory are managed outside the checkout.

### One-time install on the reviewed VPS

Keep the existing validated operator bundle and database. Do not generate a different run ID.
Set `FINAL_SUPERVISOR_COMMIT` to the final commit delivered by this change; the operator bundle
path below intentionally remains the already validated `ba844442...` bundle because none of the
files in its frozen `release-code-manifest.json` changed.

```bash
cd /opt/hyperlab-multistrategy
FINAL_SUPERVISOR_COMMIT="<FINAL_COMMIT_FROM_CHANGE_REPORT>"
CONFIG="/var/lib/hyperlab/phase12-live-paper/authorization-ba84444240af6cdbfc3df4f0170866a2a0c15c1f-linux-cpython-3.12.13/paper-config.json"
DB="/var/lib/hyperlab/phase12-live-paper/paper/paper-ba84444.sqlite3"

test "$(git branch --show-current)" = "phase-12-live-paper"
test "$(git rev-parse HEAD)" = "$FINAL_SUPERVISOR_COMMIT"
test -z "$(git status --porcelain)"
test -f "$CONFIG"
test -f "$DB"
RUN_ID="$(.venv/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["run_id"])' "$CONFIG")"
test "${#RUN_ID}" -eq 64
.venv/bin/python -m hyperlab paper status --database "$DB" --run-id "$RUN_ID"

id -u hyperlab-paper >/dev/null 2>&1 || \
  sudo useradd --system --home-dir /var/lib/hyperlab/phase12-live-paper \
    --shell /usr/sbin/nologin hyperlab-paper
sudo chown -R hyperlab-paper:hyperlab-paper /var/lib/hyperlab/phase12-live-paper
sudo -u hyperlab-paper test -x /opt/hyperlab-multistrategy/.venv/bin/python
sudo -u hyperlab-paper test -r "$CONFIG"
sudo -u hyperlab-paper test -r "$DB"

sudo install -d -m 0755 /etc/hyperlab /etc/systemd/system
sudo install -m 0600 deploy/systemd/paper-supervisor.env.example \
  /etc/hyperlab/paper-supervisor.env
sudo install -m 0644 deploy/systemd/hyperlab-paper.service \
  /etc/systemd/system/hyperlab-paper.service
sudo install -m 0644 deploy/systemd/hyperlab-paper-disk-guard.service \
  /etc/systemd/system/hyperlab-paper-disk-guard.service
sudo install -m 0644 deploy/systemd/hyperlab-paper-disk-guard.timer \
  /etc/systemd/system/hyperlab-paper-disk-guard.timer
sudo install -m 0644 deploy/systemd/hyperlab-paper-disk-stop.service \
  /etc/systemd/system/hyperlab-paper-disk-stop.service

sudo systemd-analyze verify \
  /etc/systemd/system/hyperlab-paper.service \
  /etc/systemd/system/hyperlab-paper-disk-guard.service \
  /etc/systemd/system/hyperlab-paper-disk-guard.timer \
  /etc/systemd/system/hyperlab-paper-disk-stop.service
sudo systemctl daemon-reload
```

Review `/etc/hyperlab/paper-supervisor.env` before enabling. It must contain only the exact
`CONFIG`, exact `DB`, `HYPERLAB_PAPER_MIN_FREE_BYTES=5368709120` (5 GiB), and
`HYPERLAB_PAPER_MIN_FREE_PERCENT=10`. It must contain no key, wallet, credential, hostname, or
new run ID, and must not override the unit's exact `HYPERLAB_DATA_DIR` or
`HYPERLAB_PAPER_DIR`.

Run the guard once explicitly, as the service user, before systemd start:

```bash
sudo -u hyperlab-paper env -i \
  PATH=/usr/bin:/bin HYPERLAB_MODE=readonly \
  HYPERLAB_DATA_DIR=/var/lib/hyperlab/phase12-live-paper \
  HYPERLAB_PAPER_DIR=/var/lib/hyperlab/phase12-live-paper/paper \
  /opt/hyperlab-multistrategy/.venv/bin/python \
  /opt/hyperlab-multistrategy/ops/phase12/paper_supervisor.py guard \
  --config "$CONFIG" --database "$DB" \
  --minimum-free-bytes 5368709120 --minimum-free-percent 10
```

Expected: JSON `status=READY`, `paper_state=FLAT`, `reconciled=true`,
`full_replay=REPLAY_EXACT`, and every paper-only boundary false/none as appropriate. Any refusal
is a stop condition.

### Enable, start, stop, and restart

```bash
sudo systemctl enable --now hyperlab-paper-disk-guard.timer
sudo systemctl enable --now hyperlab-paper.service
```

Stop cleanly and wait for the bounded cooperative shutdown:

```bash
sudo systemctl stop hyperlab-paper.service
sudo systemctl show hyperlab-paper.service \
  -p ActiveState -p SubState -p Result -p ExecMainStatus
```

A reviewed restart always re-runs `ExecCondition` against the same database/run:

```bash
sudo systemctl restart hyperlab-paper.service
```

Never use `systemctl kill -s SIGKILL`, delete a lock file, copy the live SQLite database, change
the config, or point the service at a fresh database to make a refusal disappear.

### Status, health, and logs

Service and timer status:

```bash
sudo systemctl status hyperlab-paper.service --no-pager
sudo systemctl status hyperlab-paper-disk-guard.timer --no-pager
sudo systemctl list-timers hyperlab-paper-disk-guard.timer --no-pager
```

The read-only health command combines the bounded same-head Paper report with systemd, `/proc`,
filesystem, and recent journald facts. It reports run ID, Paper state, active/generation/unclosed
session, integrity/readiness, BTC/ETH public timestamps and wall-clock staleness, reconnects,
gaps, critical incidents, active positions/orders, database size, free bytes/percent, process
uptime/average CPU/RSS when available, service status, and the last 30 message-only journal lines:

```bash
sudo env -i \
  PATH=/usr/bin:/bin HYPERLAB_MODE=readonly \
  HYPERLAB_DATA_DIR=/var/lib/hyperlab/phase12-live-paper \
  HYPERLAB_PAPER_DIR=/var/lib/hyperlab/phase12-live-paper/paper \
  /opt/hyperlab-multistrategy/.venv/bin/python \
  /opt/hyperlab-multistrategy/ops/phase12/paper_supervisor.py health \
  --config "$CONFIG" --database "$DB" \
  --service-name hyperlab-paper.service \
  --minimum-free-bytes 5368709120 --minimum-free-percent 10
```

Health invokes the bounded report at most three times: the initial call plus at most two supervisor
retries, with a fixed 100 ms delay between matching transient failures (200 ms maximum added delay).
A retry is allowed only for `paper report` exit 2 carrying either the explicit
`HEAD_CHANGED_RETRY` marker or the established `paper report retry required ... durable head
changed during assembly` diagnostic. Any other exit, malformed success payload, or integrity error
is not retried and remains a fail-closed supervisor error.

Every successful health payload includes `report_read` with `status=READY`, `attempts` (1-3),
`max_attempts=3`, `retry_delay_seconds=0.1`, `transient_failures` (0-2), and
`retryable=false`. If all three calls encounter the classified race, health exits 2 and returns an
explicit degraded payload rather than `SUPERVISOR_ERROR_RUNTIMEERROR`:

```json
{
  "status": "TRANSIENT_UNAVAILABLE",
  "readiness": "TRANSIENT_UNAVAILABLE",
  "preflight_readiness": "READY",
  "integrity": "HEAD_CHANGED_RETRY",
  "blockers": ["HEAD_CHANGED_RETRY"],
  "transient_unavailable": true,
  "report_read": {
    "status": "HEAD_CHANGED_RETRY",
    "attempts": 3,
    "max_attempts": 3,
    "retry_delay_seconds": 0.1,
    "transient_failures": 3,
    "retryable": true
  }
}
```

The degraded payload retains the paper-only boundary plus systemd, process, disk, and recent-log
facts. Report-derived state/count/freshness fields are `null` because no same-head report was
verified; operators must not infer `FLAT`, freshness, reconciliation, or integrity from them. Retry
the health command later without stopping or mutating the runtime. A genuine report failure still
exits 3 with `status=REFUSED` and a `SUPERVISOR_ERROR_*` blocker and requires investigation.

Recent and follow-mode logs use journald; no separate mutable log file or credential is needed:

```bash
sudo journalctl -u hyperlab-paper.service -n 200 --no-pager -o short-iso
sudo journalctl -u hyperlab-paper.service -f -o short-iso
sudo journalctl -u hyperlab-paper-disk-guard.service -n 100 --no-pager -o short-iso
```

Do not paste full host logs into reports. Preserve only the relevant sanitized Paper messages,
timestamps, exit/result codes, and hashes needed for review.

### Disk guard and recovery

The one-minute timer checks the filesystem containing the exact DB. Both 5 GiB free and 10% free
must remain available. These are initial stop thresholds for a 48-72 hour technical soak, not a
capacity claim. The monitor does not delete, rotate, vacuum, copy, archive, or truncate SQLite.

If either threshold fails, `hyperlab-paper-disk-guard.service` fails and triggers
`hyperlab-paper-disk-stop.service`. The latter stops the runtime with its normal SIGTERM/90-second
path and stops the disk timer to avoid an alert loop. Free space only by reviewing unrelated
files outside the Paper journal; never mutate the DB or its rollback journal. Then:

```bash
sudo systemctl reset-failed hyperlab-paper-disk-guard.service \
  hyperlab-paper-disk-stop.service
sudo systemctl start hyperlab-paper-disk-guard.timer
sudo systemctl start hyperlab-paper-disk-guard.service
sudo systemctl start hyperlab-paper.service
```

If the DB grows faster than available space can safely support for the remaining soak, stop and
report the measured growth. Do not invent retention or lower the thresholds for a pass.

### Reboot and manual recovery when automatic start is refused

After a planned reboot, confirm the same run and generation increment:

```bash
sudo reboot
# reconnect after boot
sudo systemctl status hyperlab-paper.service --no-pager
sudo journalctl -b -u hyperlab-paper.service --no-pager -o short-iso
```

If `ExecCondition` refuses, keep the service stopped and inspect health/status. For an unclosed
hard-crash session, the guard has already latched the exact deterministic critical incident
through `PaperEngine.pause`; use the existing explicit offline recovery only after review:

```bash
sudo systemctl stop hyperlab-paper.service
sudo -u hyperlab-paper env -i PATH=/usr/bin:/bin HYPERLAB_MODE=readonly \
  /opt/hyperlab-multistrategy/.venv/bin/python -m hyperlab paper status \
  --database "$DB" --run-id "$RUN_ID"
sudo -u hyperlab-paper env -i PATH=/usr/bin:/bin HYPERLAB_MODE=readonly \
  /opt/hyperlab-multistrategy/.venv/bin/python -m hyperlab paper replay \
  "$RUN_ID" --database "$DB"
sudo -u hyperlab-paper env -i PATH=/usr/bin:/bin HYPERLAB_MODE=readonly \
  /opt/hyperlab-multistrategy/.venv/bin/python -m hyperlab paper resume \
  "$RUN_ID" --database "$DB" \
  --review-reason "Unclosed systemd runtime session and durable incident reviewed" \
  --offline-unclosed-recovery
sudo systemctl start hyperlab-paper.service
```

#### Recovery of the reproduced generation-3 status-path failure

The reproduced failure is an unclosed-session recovery, not a standard fresh-BBO resume. After
installing the corrected unit/environment from the final reviewed commit and running
`systemctl daemon-reload`, keep the service stopped and bind the exact existing identities:

```bash
cd /opt/hyperlab-multistrategy
CONFIG="/var/lib/hyperlab/phase12-live-paper/authorization-ba84444240af6cdbfc3df4f0170866a2a0c15c1f-linux-cpython-3.12.13/paper-config.json"
DB="/var/lib/hyperlab/phase12-live-paper/paper/paper-ba84444.sqlite3"
RUN_ID="9aa7213ef08ddc07d700128cf8fdf90e75a764f0867201075074e0c5fbe64436"

test -f "$CONFIG"
test -f "$DB"
test "$(.venv/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["run_id"])' "$CONFIG")" = "$RUN_ID"
sudo systemctl stop hyperlab-paper.service
sudo systemctl show hyperlab-paper.service -p ActiveState -p SubState -p Result

paper_python() {
  sudo -u hyperlab-paper env -i \
    PATH=/usr/bin:/bin HYPERLAB_MODE=readonly \
    HYPERLAB_DATA_DIR=/var/lib/hyperlab/phase12-live-paper \
    HYPERLAB_PAPER_DIR=/var/lib/hyperlab/phase12-live-paper/paper \
    /opt/hyperlab-multistrategy/.venv/bin/python "$@"
}
```

The initial `PUBLIC_SOURCE_FAILURE` may still be the only/latest critical incident. Existing
offline recovery accepts only the deterministic `UNCLOSED_RUNTIME_SESSION` incident. Run the
existing guard once while stopped so it atomically latches that incident through
`PaperEngine.pause`:

```bash
paper_python /opt/hyperlab-multistrategy/ops/phase12/paper_supervisor.py guard \
  --config "$CONFIG" --database "$DB" \
  --minimum-free-bytes 5368709120 --minimum-free-percent 10
```

Expected: nonzero exit with `status=REFUSED` and
`UNCLOSED_RUNTIME_SESSION_REQUIRES_REVIEW`. This latch is idempotent. Review the status/latest
critical incident, then use the existing explicit offline recovery on the same DB/run:

```bash
paper_python -m hyperlab paper status --database "$DB" --run-id "$RUN_ID"
paper_python -m hyperlab paper replay "$RUN_ID" --database "$DB"
paper_python -m hyperlab paper resume "$RUN_ID" --database "$DB" \
  --review-reason "Generation 3 status-path failure and unclosed runtime incident reviewed" \
  --offline-unclosed-recovery
```

The resume command reconciles internally under the same runtime lease. Require `state=FLAT`, no
position/order, the same DB/run ID, and the old durable session still active pending replacement.
If it returns `REDUCE_ONLY`, or any identity/replay/reconciliation check fails, do not start the
unit. Otherwise rerun the guard and start only after `status=READY`, `paper_state=FLAT`, and
`full_replay=REPLAY_EXACT`:

```bash
paper_python /opt/hyperlab-multistrategy/ops/phase12/paper_supervisor.py guard \
  --config "$CONFIG" --database "$DB" \
  --minimum-free-bytes 5368709120 --minimum-free-percent 10
sudo systemctl start hyperlab-paper.service
```

The next runtime replaces durable generation 3 with generation 4 and requires a new bilateral
public-source bootstrap before strategy execution. Nothing here auto-resumes, recreates the DB,
or changes the run ID.

For a cleanly stopped `PAUSED` run, follow the existing reviewed standard resume procedure: exact
replay/reconciliation, one manual Paper run while still paused to obtain fresh bilateral BBO,
clean stop, then `paper resume` with a specific review reason. Do not automate resume.

`MANUAL_REVIEW`, a killed run, config/release/runtime mismatch, replay failure, reconciliation
failure, unexplained position/order, or unresolved critical incident has no automatic recovery.
Preserve the DB and journal, leave the unit stopped, and perform a human code/data review. Never
create another run ID or database to conceal the refusal.

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
- Phase 12.5 must add indexed or incremental daily summaries and evaluate capacity alerts plus a
  replay-preserving retention/archive policy. Those remain known debts, not evidence for launch.
- Pending BBO coalescing does not delete or rewrite durable history. It only prevents an obsolete
  not-yet-consumed BBO from entering the canonical inbox when a newer BBO for the same instrument
  and UTC minute is already pending. The frozen pairs strategy uses minute closes; a skipped minute,
  funding event, connection transition, gap, or resync is never coalesced.
- A local synthetic 1,000-BBO fixture on 18 August 2026 measured about 94.6 commits/second and
  about 6.03 KB of SQLite growth per persisted BBO. Full append-only projection history was the
  largest logical table, followed by events and inbox payloads. The illustrative extrapolation at
  exactly 1 persisted BBO/second is about
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
