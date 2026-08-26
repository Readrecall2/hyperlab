# Lighter Official Public Access Completion V1

Boundary: `PAPER_ONLY / GHOST_ONLY / PUBLIC_DATA_ONLY`.

This complement tests official public WebSocket access only. It implements no
strategy, alpha search, account access, order route, signer, wallet, credential,
proxy, VPN, IP rotation, or undocumented endpoint.

## Frozen official documentation

The only page refreshed for this completion was the official
[Lighter WebSocket reference](https://apidocs.lighter.xyz/docs/websocket-reference),
retrieved on 2026-08-26 and labelled `Updated 15 days ago` at capture time. The
machine-readable documented-facts contract is
`config/lighter-public-contract-v1.json`; its canonical SHA-256 is
`462c54d1f35cd9e56457e287515f55714b5d761d53647308f5ebdd51daee75d8`.

The documentary facts used here are:

- normal public URL: `wss://mainnet.zklighter.elliot.ai/stream`;
- official restricted-region read-only URL:
  `wss://mainnet.zklighter.elliot.ai/stream?readonly=true`;
- documented public subscription formats `order_book/{MARKET_INDEX}`,
  `ticker/{MARKET_INDEX}`, `market_stats/{MARKET_INDEX}`, and
  `trade/{MARKET_INDEX}`;
- documented examples use `market_index=0`;
- order-book continuity is `current.begin_nonce == previous.nonce`; `offset`
  increases but is not an exchange-global contiguous sequence and may change
  across connections.

These are documentary capabilities, not measured access, capacity, account
tier, network latency, or freshness observations.

## Causal correction and security contract

The previous probe requested `metadata`; its mandatory REST census therefore
received the acquired HTTP 403 and returned before attempting WebSocket. The
completion accepts an explicit documented `market_index=0` without a REST
census. Consequently symbol metadata, market type, status, precision, minimums,
and public fees remain `UNKNOWN_NOT_OBSERVED_NO_REST_CENSUS` unless present in a
raw public WebSocket payload; they are never invented.

The allowlist accepts only the exact official host, `/stream`, and either no
query or exactly `readonly=true`. It accepts only the four public subscriptions
above. Every other scheme, host, path, query, fragment, private/account channel,
authentication field, transaction message, or explicit proxy path fails
closed.

The one-shot connection sequence is fixed:

1. one normal handshake;
2. only if it fails before collection, one `readonly=true` handshake;
3. after the first HTTP 101, one bounded collection with no retry or reconnect.

The first threshold wins: 600 seconds, 5,000 frames, 64 MiB, or four segments.
The CLI is `research-data lighter-access-completion --output-root <new-path>`.
It announces local execution, duration budget, no-prompt behavior, health-file
monitoring, Ctrl+C semantics, and its terminal report before opening a socket.

## Evidence and verdicts

The acquired REST-403 evidence remains byte-identical under
`docs/evidence/lighter-public-probe-v1/` and is not rerun. The new complement is
published independently under
`docs/evidence/lighter-public-probe-v1-access-completion/`. Its authenticated
offline report is
`reports/lighter-official-public-access-completion-v1.json`.

Only these access verdicts are valid:

- `LIGHTER_OFFICIAL_WS_PUBLIC_ACCESS_GREEN`;
- `LIGHTER_OFFICIAL_READONLY_WS_ACCESS_GREEN`;
- `LIGHTER_PUBLIC_ACCESS_EXHAUSTED_OFFICIAL_PATHS`;
- `LIGHTER_OFFICIAL_WS_ACCESS_BLOCKED_INTEGRITY`.

A green verdict means only that the bounded public raw capture and offline
recovery are suitable inputs for a future Ghost data-quality study. It is not
alpha, profitability, account-tier, latency, capacity, or trading evidence.

## Acquired completion result

The single authorized sequence ran from Windows local on 2026-08-26. The normal
handshake returned HTTP 403 in 187 ms before collection. The sole conditional
`readonly=true` handshake then returned HTTP 403 in 78 ms before collection.
No further connection was attempted.

Observed totals are zero frames, zero segments, zero stored bytes, zero gaps,
zero duplicates, and zero reconnects. Therefore manifest and root hashes are
null and offline recovery is explicitly
`NOT_AVAILABLE_NO_AUTHENTICATED_MANIFEST`; no hash was invented. The immutable
completion report SHA-256 is
`5a1805ba48b59e37997c1f59777b7b312407a2df7f41529113da03c31bd9e906`.

Terminal access verdict:
`LIGHTER_PUBLIC_ACCESS_EXHAUSTED_OFFICIAL_PATHS`. Lighter is not eligible from
this Windows-path evidence for a future Ghost data-quality study. This does not
generalize the historical REST 403, and it does not block Hyperliquid work.
