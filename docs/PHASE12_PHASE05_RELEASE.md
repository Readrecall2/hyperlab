# Phase 12 Phase08+Phase05 technical Paper release

Status: **TECHNICAL_ONLY_UNCALIBRATED**. This document prepares a future local or Linux Paper smoke. It does not authorize deployment, profitability, Testnet/Mainnet execution, or real money.

## Historical and successor namespaces

- Historical Phase08 V9 evidence remains byte-for-byte under `config/paper/phase08-robust-pairs-btc-eth-paper-v1/`. `config/paper/phase08-v9-historical-attestation.json` records its frozen file hashes and historical semantic identity. It is not a current-release authorization.
- The current successor is `phase08-phase05-multistrategy-paper-v1` under `config/paper/phase08-phase05-multistrategy-paper-v1/`.
- The compiled operator registry admits only the successor config hash. Legacy schema-v2 runtime records retain their historical candidate interpretation for replay/report compatibility.

Never regenerate V9 in place or copy a runtime attestation between operating systems.

## Source-contract decision

The successor formally uses adapter schema V10 and source identity `hyperliquid-mainnet-public-bbo-funding-context-phase05-v1`. Its feed contract is `SOLE_COLLECTOR_NORMALIZED_BBO_CONNECTION_FUNDING_MARKET_CONTEXT_BOUNDED_PENDING_BBO_LATEST_VALUE_V10`.

V10 is required because the portfolio consumes BTC/ETH perpetuals, HYPE perpetual, HYPE spot `@107`, public funding, and public market context with immutable product identities. The Phase08-only constructor stays on V9. Both versions preserve latest-per-instrument-per-UTC-minute pending-BBO coalescing between causal control barriers; the successor does not relabel incompatible semantics as V9.

The source remains one lazy public/read-only collector with no credentials, wallet, signer, private API, or exchange order route. Hyperliquid `fundingHistory.time` is admitted as a finalized hourly settlement only within the first 60 seconds of its UTC hour and is then canonicalized to that exact hour; later timestamps fail closed, while `received_time` remains the independent causal observation time. V10 ordering follows the collector FIFO, gives synchronous REST bootstrap/resync a producer-scoped connection identity distinct from the succeeding WebSocket, requires non-regressing `arrival_sequence` within each producer-scoped `(connection_id, connection_epoch)`, and requires non-regressing `connection_epoch` per route; observation timestamps and independently synthesized REST/WebSocket sequence counters are not treated as one global sequence domain.

## Canonical successor bundle

The checked-in Windows realization contains 24 canonical files:

- `paper-config.json`
- `source-identity.json`
- `release-code-manifest.json`
- `runtime-environment-attestation.json`
- `readiness-manifest.json`
- `artifact-index.json`
- `technical-evidence.json`
- `technical-deployment-gate.json`
- 16 files under `evidence/`, one for each compiled PAPER/PAPER_RUNTIME requirement

`technical-evidence.json` binds the portfolio ID, ordered Phase05/Phase08 strategy and config identities, portfolio risk, V10 source/data hash, config/run/release/runtime/readiness identities, reporting structure, benchmark reference, and Paper-only scope. `artifact-index.json` is the concise bundle index. Read current hashes from those generated files; do not copy them by hand into another bundle.

The invariant scope is:

```text
environment=PAPER
mode=PAPER_ONLY
orders_enabled=false
authorizes_real_money=false
credential_scope=NONE
execution_network=NONE
```

## Deterministic checked-in verification

From the reviewed checkout and exact locked environment:

```powershell
$Repo = (Get-Location).Path
$Python = Join-Path $Repo ".venv\Scripts\python.exe"
$CandidateRoot = Join-Path $Repo "config\paper\phase08-phase05-multistrategy-paper-v1"
$Config = Join-Path $CandidateRoot "paper-config.json"
$Manifest = Join-Path $CandidateRoot "readiness-manifest.json"
$env:PYTHONPATH = "$Repo;$Repo\src"

& $Python scripts\generate_phase12_live_paper_artifacts.py --check
& $Python -m hyperlab gate-model requirements PAPER
& $Python -m hyperlab gate-model check $Manifest --evidence-root $CandidateRoot
& $Python -m hyperlab paper preflight $Config
```

Every command must exit zero. Preflight must remain offline: no public transport start and no SQLite creation.

## Future Linux operator bundle

Generate the complete Linux realization on the reviewed Linux host, outside the Git checkout and before creating the new database. Replace the placeholders only after the final commit is reviewed:

```bash
cd /opt/hyperlab-multistrategy
PYTHON=.venv/bin/python
EXPECTED_COMMIT="<FINAL_REVIEWED_COMMIT>"
OPERATOR_ROOT="/var/lib/hyperlab/phase12-phase05/authorization-$EXPECTED_COMMIT-linux-cpython-3.12.13"
CONFIG="$OPERATOR_ROOT/paper-config.json"
MANIFEST="$OPERATOR_ROOT/readiness-manifest.json"
DB="/var/lib/hyperlab/phase12-phase05/paper/paper-$EXPECTED_COMMIT.sqlite3"

test "$(git rev-parse HEAD)" = "$EXPECTED_COMMIT"
test -z "$(git status --porcelain)"
test ! -e "$OPERATOR_ROOT"
test ! -e "$DB"

"$PYTHON" scripts/generate_phase12_live_paper_artifacts.py --output-root "$OPERATOR_ROOT"
"$PYTHON" scripts/generate_phase12_live_paper_artifacts.py --output-root "$OPERATOR_ROOT" --check
"$PYTHON" -m hyperlab gate-model requirements PAPER
"$PYTHON" -m hyperlab gate-model check "$MANIFEST" --evidence-root "$OPERATOR_ROOT"
"$PYTHON" -m hyperlab paper preflight "$CONFIG"
```

The Linux runtime attestation intentionally changes the config hash and run ID while all other compiled candidate semantics remain equal. Admission fails closed on lock, distribution, interpreter, platform, release, source, strategy, risk, cadence, or readiness drift.

Only after review of those outputs may a human operator perform a new-database technical smoke using the existing commands:

```bash
"$PYTHON" -m hyperlab paper run "$CONFIG" --database "$DB"
RUN_ID="$("$PYTHON" -c 'import json,sys; print(json.load(open(sys.argv[1]))["run_id"])' "$CONFIG")"
"$PYTHON" -m hyperlab paper replay "$RUN_ID" --database "$DB"
"$PYTHON" -m hyperlab paper reconcile "$RUN_ID" --database "$DB"
"$PYTHON" -m hyperlab paper report "$RUN_ID" --database "$DB"
```

Use the existing reviewed guard and systemd machinery only after replacing its config/database paths with this new namespace and independently reviewing the service diff. Do not point it at the existing Phase08 V9 database, reuse its run ID, copy its bundle, or touch the running V9 service.

## Technical deployment gate

`technical-deployment-gate.json` requires the future smoke to record and pass all of the following:

- no persistent source-queue accumulation and a drain to zero;
- every required instrument's BBO age below the 15-second stale threshold;
- effective bounded latest-value coalescing during an observed burst;
- measured peak CPU and RSS within operator limits declared before the smoke;
- observed SQLite bytes and durable commits over the smoke window;
- no repeated reconnect/resync pathology or unexplained gap loop;
- no repeated unhedged incident or protective loop.

Profitability is explicitly absent from this gate. Any missing measurement is a blocked result, not a pass.

## Required review before the first smoke

1. Review the final diff, release-code manifest, source identity, config, readiness evidence, technical evidence, and artifact index.
2. Verify the historical V9 attestation and the successor regeneration independently.
3. Build the exact locked Linux CPython environment and generate a fresh Linux bundle there.
4. Declare CPU/RAM limits and the smoke duration before starting.
5. Allocate a new persistent local database path and confirm the V9 service/database are untouched.
6. Run generation, `--check`, gate-model requirements/check, and offline preflight.
7. Perform a supervised new-run smoke, clean stop, exact replay, reconciliation, report, guard, and only then a reviewed systemd deployment exercise.
8. Preserve `TECHNICAL_ONLY_UNCALIBRATED`; do not infer economic eligibility from technical success.
