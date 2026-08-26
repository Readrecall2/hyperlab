# Lighter Public Probe V1

Terminal status: `LIGHTER_PUBLIC_SOURCE_UNAVAILABLE_BOUNDED`.

## Boundary

This component is strictly `PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY`. It contains
no strategy, alpha test, API key, account, wallet, signer, authentication token,
private channel, transaction route, proxy, regional bypass, or Lighter SDK
dependency. It never sends, creates, cancels, or modifies an order.
Both HTTP and WebSocket transports disable environment-derived proxies and
connect directly to the exact allowlisted Lighter hosts with redirects disabled.

The probe asks one technical question only: can the documented public Lighter
surface provide a bounded, reproducible microstructure prefix suitable for a
future Ghost study? A green result is not economic, latency-tier, capacity,
execution, or real-money evidence.

## Official contract frozen on 2026-08-26

| Surface | Frozen official URL | Documentary capability |
|---|---|---|
| WebSocket | https://apidocs.lighter.xyz/docs/websocket-reference | Plain mainnet WSS connection; public `order_book`, `ticker`, `market_stats`, and `trade`; client keepalive; `order_book` initially sends a full snapshot and then state changes |
| Market metadata | https://apidocs.lighter.xyz/reference/orderbooks | `GET /api/v1/orderBooks`; market type/status, minimums, size/price/quote precision and maker/taker fee strings |
| Market details | https://apidocs.lighter.xyz/reference/orderbookdetails | `GET /api/v1/orderBookDetails`; documented metadata endpoint, not needed by the bounded V1 run when `orderBooks` is complete |
| Recent trades | https://apidocs.lighter.xyz/reference/recenttrades | Public recent-trade REST surface; V1 uses the public live `trade` channel instead |
| Account types | https://apidocs.lighter.xyz/docs/account-types | Standard, Plus and Premium fee/delay tables; documentary only |
| Trading fees | https://docs.lighter.xyz/trading/trading-fees | Public account-type fee narrative; documentary only |
| Rate limits | https://apidocs.lighter.xyz/docs/rate-limits | Public REST/WSS limits and reconnect warning; documentary capacity, not an entitlement |

The exact machine-readable capture is `config/lighter-public-contract-v1.json`.
Its page-update labels are recorded as observed on the capture date; relative
labels are not converted into invented publication dates.

Documented values are deliberately separated from measurements:

- the 50 ms order-book batching statement is a documented server behavior, not
  a measured end-to-end latency;
- Standard/Plus/Premium order delays and fees are documented account scenarios,
  not an observed tier or accessible account;
- 100/250/500/1000 ms are versioned comparable Ghost freshness scenarios, not
  Lighter account observations;
- source-to-local-receive deltas include source clock offset and are never named
  network-only latency.

## Public schema and continuity

The adapter preserves exact HTTP/WSS bytes in Research Data Plane envelopes.
Every frame binds the actual server timestamp when present, local UTC receive
time, local monotonic receive time, local arrival sequence, connection epoch,
content hash and public provenance.

The frozen public field shapes used by V1 are deliberately narrow:

| Feed | Required documentary shape preserved by the adapter |
|---|---|
| `GET orderBooks` | top-level `code`, `order_books[]`; each selected row binds `market_id`, `symbol`, `market_type`, `status`, `supported_price_decimals`, `supported_size_decimals`, `supported_quote_decimals`, `min_base_amount`, `min_quote_amount`, `maker_fee`, `taker_fee` |
| `order_book/{market}` | `channel`; nested `order_book.code`, `asks`, `bids`, `nonce`, `begin_nonce`, `offset`, `last_updated_at`; matching outer `offset` when supplied |
| `ticker/{market}` | `channel`, `nonce`, `last_updated_at`; nested public ticker/BBO object retained byte-for-byte |
| `market_stats/{market}` | `channel`, `timestamp`; nested `market_stats.market_id` plus the public price, open-interest and funding fields retained byte-for-byte |
| `trade/{market}` | `channel`, `nonce`, `trades[]`, optional `liquidation_trades[]`; per-trade server `timestamp` retained byte-for-byte |

Fields not required for identity, timing or continuity are not normalized away:
they remain available in the authenticated raw payload. Any missing required
field or type mismatch is `PUBLIC_SOURCE_INVALID`, never inferred.

For `order_book` only, the documented continuity rule is authoritative:
`current.begin_nonce == previous.nonce`. `offset` must increase inside a
connection epoch but is never treated as contiguous and may reset after a
reconnect. The adapter records exact `nonce`, `begin_nonce` and `offset`; it
does not synthesize a venue sequence. A mismatch or offset regression publishes
the offending raw frame, marks a gap, freezes that market and terminates the
prefix fail-closed. Reconnect starts a new explicit epoch and resubscribes for a
new snapshot before continuity resumes.

## One bounded operator run

The authorized run uses exactly one command and a new output root:

```powershell
& 'C:\Dev\hyperlab-multistrategy\.venv\Scripts\python.exe' -m hyperlab research-data probe `
  --output-root .\docs\evidence\lighter-public-probe-v1 `
  --venue lighter `
  --feeds metadata,order_book,ticker,market_stats,trades `
  --census-limit 2 `
  --duration-seconds 600 `
  --max-frames 5000 `
  --max-bytes 67108864 `
  --segment-bytes 16777216 `
  --max-segments 4 `
  --rotation-seconds 150 `
  --progress-interval 10
```

Collection stops at the first of 600 seconds, 5,000 admitted frames, 64 MiB of
published raw segments, or four published segments. There is no automatic probe
rerun. The normal in-probe reconnect logic remains bounded and visible because
reconnect behavior is itself part of the capability audit.

After the network command, the strictly offline command
`research-data lighter-report --output-root ...` authenticates the explicit
manifest chain, replays every admitted envelope and writes
`reports/lighter-public-probe-v1.json`.

Terminal verdicts are exactly:

- `LIGHTER_PUBLIC_PROBE_V1_GREEN`;
- `LIGHTER_PUBLIC_SOURCE_UNAVAILABLE_BOUNDED`.

The latter keeps missing frames, manifests and hashes as JSON `null`; it never
manufactures evidence and never blocks Hyperliquid work.

## Bounded public result — 2026-08-26

Exactly one public run was attempted from the local Windows worktree. The
direct, unauthenticated `GET
https://mainnet.zklighter.elliot.ai/api/v1/orderBooks?filter=all` request
returned HTTP 403 after 625 ms. The fail-closed census dependency stopped the
probe before any WebSocket connection or subscription was attempted. There was
no proxy, regional bypass, credential, account or automatic rerun.

Observed terminal facts:

- accessible endpoint/channel in this probe: none;
- markets, frames, segments and published raw bytes: 0;
- gaps, duplicates and reconnects: 0;
- temporal coverage and freshness distributions: empty;
- manifest SHA-256 and raw root SHA-256: `null`;
- offline recovery: `NOT_AVAILABLE_NO_AUTHENTICATED_MANIFEST`;
- report: `docs/evidence/lighter-public-probe-v1/reports/lighter-public-probe-v1.json`.

This result establishes only bounded source unavailability from this direct
probe location. It does not contradict the frozen documentary capabilities,
does not establish whether an account tier is accessible, and does not block
the Hyperliquid path.
