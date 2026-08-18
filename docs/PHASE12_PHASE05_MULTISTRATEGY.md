# Phase 12: Phase 05 + Phase 08 deterministic Paper portfolio

Status: **TECHNICAL_ONLY_UNCALIBRATED**.

This work onboards the reviewed Phase 05 cash-and-carry strategy into the schema-v3 deterministic Paper core beside the real Phase 08 pairs adapter. It is not a deployment, economic gate, or real-money authorization. The source remains public/read-only and lazy; no network source is started by the factory.

## Audit mapping

The unchanged Phase 05 research contract comes from:

- `src/hyperlab/strategies/carry.py`: causal feature construction and signal gates.
- `src/hyperlab/backtest/carry.py`: long-spot/short-perpetual accounting and costs.
- `docs/CASH_AND_CARRY_PHASE05.md`: phase assumptions and limitations.
- `tests/test_cash_and_carry_phase05.py`: existing feature, gate, cost, and no-look-ahead evidence.

The Paper adapter maps that contract as follows:

| Reviewed Phase 05 item | Deterministic Paper mapping |
|---|---|
| Decision frequency | Completed UTC-hour bars only; an hour is finalized only after a later observation |
| Execution causality | A completed signal remains pending until both leg quotes are at or after the bar boundary, then is consumed once |
| Position | Long HYPE spot and short HYPE perpetual, equal target notional |
| Funding | Durable public hourly settlements, bounded to the causal completed-bar horizon |
| Windows | 8 h, 24 h, and 72 h |
| Funding gate | 72 h mean hourly funding at least 0.000005 |
| Positive funding gate | 72 h positive share at least 0.70 |
| Basis gate | Absolute basis at most 150 bps |
| Liquidity gates | Each leg depth at least USD 100,000 and volume at least USD 1,000,000 |
| Open interest | Perpetual open interest at least USD 5,000,000 |
| Volatility | Annualized volatility at most 1.50 |
| Net edge | Minimum net edge across 8/24/72 h is non-negative |
| Costs embedded in signal | 11 bps round-trip fees, 4 bps slippage, observed spreads, 4.5% annual opportunity rate |
| Entry | Two strategy-owned maker/GTC orders in one hedge group: spot BUY then perpetual SELL |
| Exit | When eligibility ceases, two strategy-owned reduce-only maker/GTC orders |
| Failure path | Existing unhedged timeout; strategy-attributed alert; portfolio protection; reduce-only IOC emergency flatten |
| Capital | 50% research allocation represented by a USD 500 Phase 05 gross cap in the technical fixture |
| Restart state | Completed bars, funding observations, and signal lineage rebuilt only from durable public inputs |

No economic parameter was tuned or inferred during onboarding.

## Stable identities

The stable strategy ID is `phase05_cash_and_carry`. Its strategy hash binds the reviewed economics; its strategy-config hash additionally binds instruments, product identities, quantity steps, retained history, execution skew, and strategy-local risk.

The production technical mapping is:

- spot: `HL:HYPE:spot`, public source asset `@107`;
- perpetual: `HL:HYPE:perp`, public source asset `HYPE`;
- Phase 08: `HL:ETH:perp` and `HL:BTC:perp`.

The generated technical evidence records:

- portfolio ID `964323215b055b977faf1ef713f4642226cedcdec2a779ecf0ae5a27f68f41bb`;
- config hash `79049391ac63ca089900787b239b26ef80826877a924f23b3ccaf01789d3c75c`;
- run ID `213e9597702c8331892788c7b2841e283c14868e88987ef5d61765200b6b7601`;
- Phase 05 strategy hash `76e4b4ab6c1af42bb408a2f22163affbb88b0e717d49f2e88696c5abd0063f0f`;
- Phase 05 strategy-config hash `d5d0c18e77a3e1a5ba1a11f9fda646ce9bd8d4a68c476d4d620a62886bc4af24`;
- Phase 08 strategy hash `239ca1f27b9563a8fcacb5faa756364b6fc70240246dc086ac0be1633d8abb0d`;
- Phase 08 strategy-config hash `3c41aff21544b83e03bf53a991b268a3e3c9e97c448ec353e47bfd470eadd75d`.

Runtime-environment and release-code hashes are intentionally recorded in `technical-evidence.json` rather than copied here; they must fail closed when the environment or source changes.

## Shared public source

One collector profile covers BTC, ETH, HYPE perpetual, and HYPE spot. It subscribes only to public `bbo` and `activeAssetCtx` channels and uses public funding history. There are no credentials, signer, wallet, private endpoints, or order routes.

The Phase 05 source mode is opt-in and uses source schema V10. Phase 08-only construction remains on the exact legacy V9 source identity and coalescing behavior. V10 adds immutable product-identity hashes and attaches the latest causal normalized market context to each BBO. Missing, malformed, mismatched, or future context fails the affected strategy locally; a global source fault still pauses/protects the portfolio.

The pending queue retains the reviewed latest-per-instrument-per-UTC-minute coalescing between control barriers. Funding and connection records remain causal FIFO barriers.

## Deterministic scheduling and admission

Every fresh source event is durably admitted once. Strategies evaluate the same decision frame sequentially in lexical `strategy_id` order:

1. `phase05_cash_and_carry`
2. `phase08_robust_pairs`

A strategy receives the shared frame for causal context but only its declared primary/order instruments are bound into decision admission. Unrelated instruments cannot alter either adapter's signal hash. Decisions, hedge groups, orders, positions, fees, funding, realized PnL, alerts, and failures retain strategy ownership.

## Risk hierarchy

Strategy-local limits are evaluated before portfolio limits:

| Scope | Gross | Net | Instrument | Order | Active orders | Daily loss | Drawdown |
|---|---:|---:|---:|---:|---:|---:|---:|
| Phase 05 | 500 | 300 | 250 | 250 | 2 | 100 | 200 |
| Phase 08 | 250 | 250 | 250 | 250 | 2 | 75 | 150 |
| Portfolio | 750 | 500 | 500 | 250 | 4 | 175 | 350 |

All notionals are USD in this technical fixture. A local failure without owned exposure is isolated. A local failure with exposure, a global stale/source failure, or an unhedged timeout escalates to portfolio protection. Portfolio `REDUCE_ONLY`/`EMERGENCY_FLATTEN` dominates every strategy.

The timeout remains 20 seconds. Emergency exits are strategy-attributed, reduce-only IOC orders. The historical 11-state transition map is unchanged: an incomplete local hedge moves through the existing global protective state.

## Accounting, restart, and reporting

The existing schema-v3 ledger performs exact account/strategy reconciliation:

- account net position is the sum of attributed strategy positions;
- opposing same-instrument exposures remain separate in gross attribution;
- fees, funding, and realized PnL are booked to the owning strategy;
- portfolio totals must equal the sum of strategy totals;
- reports retain the legacy schema while adding bounded strategy sections.

On restart, the runtime first replays durable engine events, then restores each adapter from only its required durable public inputs. Funding observations are restored before subsequent decisions. No recovery shortcut or synthetic position reconstruction was added.

## Benchmark

`scripts/benchmark_paper_phase05_portfolio.py` runs the real adapters against deterministic synthetic canonical BBOs and validates every durable commit, store integrity, and exact replay.

Measured on Windows 11, 200 logical frames, three repetitions:

| Case | Median frames/s | Median evaluations/s | Durable commits/repetition |
|---|---:|---:|---:|
| Phase 08 only | 34.83 | 34.83 | 400 |
| Phase 08 + Phase 05 | 14.96 | 29.92 | 800 |

A combined logical frame contains four durable market commits instead of two and evaluates two real strategies. The source burst probe admitted 800 BBO records, coalesced 796 still-pending replacements, reached a high-water mark of four frames, and drained to zero. No persistent backlog was observed.

**SYNTHETIC TECHNICAL THROUGHPUT ONLY; NOT ECONOMIC OR DEPLOYMENT EVIDENCE.**

## Evidence and reproducibility

Generated artifacts:

- `reports/phase12-phase05/benchmark.json`;
- `reports/phase12-phase05/technical-evidence.json`.

Reproduce them with:

```powershell
$env:PYTHONPATH = "src"
python scripts/benchmark_paper_phase05_portfolio.py --frames 200 --repetitions 3 --output reports/phase12-phase05/benchmark.json
python scripts/generate_phase05_paper_evidence.py
python scripts/generate_phase05_paper_evidence.py --check
```

The evidence explicitly fixes:

- `environment=PAPER`;
- `mode=PAPER_ONLY`;
- `orders_enabled=false`;
- `authorizes_real_money=false`;
- `credential_scope=NONE`;
- `execution_network=NONE`;
- `economic_prerequisites_satisfied=false`;
- `economically_eligible=false`.

## Validation interpretation

Focused Phase 05, real Phase 05+08, generic multi-strategy, reporting, store, replay, reconciliation, runtime, source, and Phase 08 compatibility tests are the acceptance surface.

The checked-in deployed Phase 08 V9 candidate is deliberately not regenerated in this task. Because this branch changes Paper source code, its frozen release-code attestation must fail closed. A failing V9 artifact/preflight verification is therefore a deployment blocker and positive integrity evidence, not permission to update or bypass the deployed artifact. Promotion requires a separate reviewed deployment phase with newly generated candidate artifacts.

## Remaining deployment blockers

Before any real two-strategy VPS Paper deployment:

1. independently review this diff and its technical evidence;
2. calibrate execution, costs, and data or retain non-economic technical status;
3. perform a dedicated candidate-artifact generation and authorization review;
4. re-run the complete release, manifest, runtime-environment, and artifact gates;
5. explicitly authorize deployment without adding private or real-money execution capability.

This task does not access the VPS, a live Paper database, or `phase-12-live-paper`, and it does not modify real-money capability.
