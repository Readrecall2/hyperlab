# Phase 12 multi-strategy Paper foundation

This foundation remains `environment=PAPER`, `mode=PAPER_ONLY`, with simulated orders only. It adds no wallet, signer, private exchange client, order transport, Testnet/Mainnet authorization, or real-money capability.

## Architecture

The legacy path remains available unchanged for schema-v1/v2 run configurations:

`public source -> PaperRuntime -> PaperRunner -> PaperEngine -> inbox/events/ledger/projection -> replay/reconciliation/report`

Schema-v3 runs use the additive portfolio path:

`one public source -> PaperRuntime -> PortfolioRunner -> strategies in strategy_id order -> PaperEngine -> shared account plus strategy attribution`

One canonical market frame is admitted once. Every eligible strategy receives that same immutable frame sequentially in sorted `strategy_id` order. Each decision is risk-checked first against its immutable strategy budget and then against authoritative portfolio limits. No evaluation concurrency is used.

## Identity and compatibility

`PaperStrategyConfig` freezes `strategy_id`, `strategy_name`, `strategy_hash`, parameters, required instruments, and strategy risk. Its canonical hash is `strategy_config_hash`. `portfolio_id` binds the sorted pairs of strategy ID and config hash. Schema-v3 `config_hash` and `run_id` include the complete strategy membership and every strategy budget, so adding, removing, or changing a strategy creates a new run identity.

Legacy event, decision, order, config, projection, and engine-build serialization is conditional: schema-v1/v2 payloads do not gain strategy fields and retain their historical IDs/hashes. New multi-strategy runs use config schema 3 and projection schema 4. The SQLite store stays at schema 2 because its append-only JSON event/projection columns and ledger account strings already support additive attribution; no historical row is rewritten or migrated.

## Attribution and reconciliation

Every new multi-strategy decision and order carries explicit strategy identity. Fills update both:

- strategy-local position, inventory value, cost basis, cash flow, realized PnL, fees, state, and incidents;
- aggregate account position, cash, PnL, fees, and protective state.

Strategy ledger accounts use `strategy:<strategy_id>:` prefixes. Their transactions are derived from the strategy's own pre-fill position, independently of the account transaction. This is required when an account-level close is a strategy-level open. Exact reconciliation verifies cash, inventory, fees, realized PnL, and non-zero inventory account sets at both scopes. Account realized PnL and summed strategy realized PnL can legitimately differ while internally offsetting strategy positions remain open; account equity/cash and the attributed strategy views remain the authoritative reconcilable truths for their respective scopes.

The account position is the algebraic net while portfolio gross risk uses attributed gross exposure. Instrument ownership is never inferred from the instrument name. Net-flat offsetting positions therefore remain visible, funded, risk-controlled, replayable, and flattenable.

## Failure semantics

Strategy evaluation/decision-admission failures produce a durable `STRATEGY_LOCAL_FAILURE`, a strategy-attributed critical alert, and a local `PAUSED` transition. Other strategies continue in stable order when the failed strategy is flat. If it owns exposure, isolation is not safe: the portfolio enters `REDUCE_ONLY` and the existing automatic protective path submits attributed exits in stable strategy order. Strategy-budget rejection is durable and local.

Store integrity, event/input replay, reconciliation, public-source identity/health, runtime identity/lease, and portfolio protective failures retain global fail-closed semantics. Aggregate risk always dominates strategy admission. A global pause does not fabricate local strategy pauses.

## Read models and limits

Multi-strategy reports use schema 2 and expose the existing account-net `account` view, an attributed-gross `portfolio` view, and a sorted `strategies` mapping containing identity, state, decisions, accounting, risk, and incidents. Legacy reports remain schema 1.

This phase does not onboard Phase 05, allocate capital dynamically, add cross-strategy optimization, add a dashboard, or change production deployment. Phase 05 integration still requires a reviewed adapter/config, explicit risk allocation, synthetic same-instrument and restart fixtures, and fresh offline Paper validation under a new immutable run identity.

Synthetic throughput can be measured with:

`python scripts/benchmark_paper_multistrategy.py --frames 200 --repetitions 3`
