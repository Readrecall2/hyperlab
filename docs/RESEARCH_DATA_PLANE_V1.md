# HyperLab Research Data Plane V1

## Boundary and verdict scope

This component is strictly `PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY`. It has no
wallet, signer, credential, private API, account endpoint, order route, cancel
route, RFQ route, or real-execution dependency. It produces raw evidence,
derived views, offline logical-relative-value candidates, and compact Paper
references. It makes no alpha, capacity, profitability, or economic-readiness
claim.

Synthetic tests are visibly labeled `SYNTHETIC/FIXTURE` and are not probe
evidence.

## Architecture

| Layer | Authority and responsibility |
|---|---|
| Raw public data | Immutable deterministic compressed segments and chained content-addressed manifests |
| Derived research | Regenerated from explicit raw manifest/root plus model/parameter version |
| Paper journal | Segment references, decisions, hypothetical actions, risk, and compact results only |
| Reports | Probe health/result JSON, gaps, duplicates, reconnects, counts, coverage, hashes, and limitations |

The new raw store is autonomous from the certified Storage V4 Phase 1C
candidate. See `src/hyperlab/research_data/FORMAT.md` for exact framing,
publication, recovery, and read-only rules.

## Official public surfaces frozen on 2026-08-26

### Hyperliquid

The adapter permits public `/info` requests and public WebSocket subscriptions
for `bbo`, `l2Book`, `trades`, `allMids`, and `activeAssetCtx`. The L2 message is
treated as an aggregated snapshot, never a delta. No exchange sequence is
invented. Reconnect generations are explicit.

Official references:

- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits

There is no verified credential-free global active-TWAP or liquidation stream
in the selected API contract. H3/H4 labels therefore remain
`*_GLOBAL_PUBLIC_SOURCE_UNVERIFIED`; a price crash is never relabeled as a
liquidation. OI is available through active asset context.

### Polymarket

The adapter permits Gamma market/event metadata, public CLOB books, last-price,
tick-size and fee parameters, market-filtered public Data API trades, and the
public market WebSocket (`book`, `price_change`, `last_trade_price`, tick-size
and lifecycle messages). Economic market identity, condition/token/outcome
identity, point-in-time rule text/source/date/state, and raw payload stay
separate. A token-scoped probe records an explicit limitation when it cannot
resolve a Gamma rule/event identifier; it never substitutes an unrelated
event census.

Official references:

- https://docs.polymarket.com/api-reference/markets/get-market-by-id
- https://docs.polymarket.com/api-reference/events/get-event-by-id
- https://docs.polymarket.com/api-reference/markets/get-market-by-token
- https://docs.polymarket.com/api-reference/market-data/get-order-book
- https://docs.polymarket.com/api-reference/market-data/get-last-trade-price
- https://docs.polymarket.com/api-reference/market-data/get-fee-rate
- https://docs.polymarket.com/api-reference/market-data/get-tick-size
- https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets
- https://docs.polymarket.com/api-reference/wss/market
- https://docs.polymarket.com/concepts/resolution
- https://docs.polymarket.com/trading/fees

Authenticated user trade history is not used. Public last-trade price evidence
comes from the public market channel.

### Kalshi

The adapter permits unauthenticated Predictions REST for series, events,
markets, books, public trades, incentives, and scheduled fee changes. Fixed
point dollar/quantity strings, price grids, status, rules, settlement/result,
and parameter changes remain raw and versioned.

Official references:

- https://docs.kalshi.com/getting_started/quick_start_market_data
- https://docs.kalshi.com/getting_started/orderbook_responses
- https://docs.kalshi.com/api-reference/market/get-markets
- https://docs.kalshi.com/api-reference/market/get-trades
- https://docs.kalshi.com/api-reference/incentive-programs/get-incentives
- https://docs.kalshi.com/api-reference/exchange/get-series-fee-changes

Kalshi currently requires authentication during the WebSocket handshake even
for public market-data channels. V1 therefore refuses Kalshi WebSocket rather
than introducing a key or signature. The public REST adapter and its fixtures
remain usable.

Incentives use a separate ledger. `hypothetical_reward` is research-only,
`realizable_reward` stays unpresumed, and primary economics always uses
`reward=0`.

## Semantic catalogue and K4 scanner

The catalogue represents equivalent markets, mutual exclusion, exhaustive
sets, parity, nested thresholds, ranges, differing expiries, conditional
implications, and wording duplicates. It never infers an economic relation from
text similarity. A relation binds deterministic ID/type/members/formal rule,
provenance, version, confidence, `VERIFIED|UNVERIFIED`, and human/machine
justification.

`UNVERIFIED` is refused by the K4 scanner. The minimal scanner supports a
formally specified buy-complete-set contract and:

- consumes observed executable asks, never midpoints;
- bounds quantity by the worst leg;
- requires one point-in-time ID and exact rule versions;
- prices fees and conservative slippage;
- sequences the scarcest leg first and exposes non-fill risk;
- reports immobilized capital, payout, gross/net edge, time remaining and all
  no-op reasons;
- assumes neither simultaneous legs nor rewards;
- returns `NO_OPPORTUNITY` cleanly.

It emits candidates only. It cannot send an order.

## H1/H3/H4 dataset contracts

V1 freezes `BID_ONLY|ASK_ONLY|NO_QUOTE`, explicit no-trade state, action-delay
bands, markouts at 100 ms, 500 ms, 1 s, 5 s and 30 s plus fill-to-close, causal
event windows, and matched-control keys. TWAP/liquidation/forced-flow labels are
admitted only when a verified official public source event was observed no later
than the dataset observation.

No policy optimizer, tuner, or economic claim is included.

## Operator CLI

Example (PowerShell local, one bounded Hyperliquid probe):

```powershell
hyperlab research-data probe `
  --output-root .\probe-output\hyperliquid-001 `
  --venue hyperliquid `
  --feeds metadata,bbo,l2_book,trades,all_mids,active_asset_context `
  --instruments BTC `
  --duration-seconds 120 `
  --max-bytes 67108864 `
  --segment-bytes 4194304 `
  --rotation-seconds 30 `
  --progress-interval 10
```

The CLI refuses an existing output root, hidden feed defaults, missing venue,
missing duration, and missing explicit target/bounded census. Before network
work it prints local execution location, expected/maximum duration, no-prompt
behavior, safe monitoring path, Ctrl+C behavior, and terminal completion signal.

Terminal files:

- `<output-root>/reports/health.json`;
- `<output-root>/reports/result.json`;
- `<output-root>/raw/{segments,manifests,staging}`.

Exit codes:

- `0`: `COMPLETE` or bounded `MAX_BYTES_REACHED`;
- `3`: `PUBLIC_SOURCE_UNAVAILABLE`;
- `4`: `PUBLIC_SOURCE_INVALID` (payload public incompatible avec le contrat fail-closed);
- `5`: `BACKPRESSURE_LIMIT_REACHED` (gap visible, jamais masqué);
- `130`: `INTERRUPTED_RECOVERABLE`;
- Typer validation errors use the standard configuration-error exit.

The result reports frames, segments, stored bytes, gaps, duplicates, reconnects,
queue high-water, source-timestamp coverage, terminal health, manifest SHA-256,
raw root SHA-256, and exact limitations.
