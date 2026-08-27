# Prediction Markets Candidate V1 — bounded access evidence

Capture date: 2026-08-27. Boundary:
`PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY`.

Exactly one effective direct probe was run per venue. Both stopped after their
first official DNS lookup failed. No retry, proxy, credential, raw frame,
segment, manifest or root hash was introduced.

| Venue | Official first target | Calls | Elapsed ms | Terminal health | Result SHA-256 |
|---|---|---:|---:|---|---|
| Polymarket | `https://gamma-api.polymarket.com/markets/keyset?closed=false&limit=5` | 1 | 11 235 | `PUBLIC_SOURCE_UNAVAILABLE` | `4d9b15d44fd99ae1cf13fd032350e220b5dc477cfc42acdad83550a299751c5a` |
| Kalshi | `https://external-api.kalshi.com/trade-api/v2/exchange/status` | 1 | 11 139 | `PUBLIC_SOURCE_UNAVAILABLE` | `ad4ede07a268ee372127a52ba0ad406aeb8b552d5cac0a4a732a4f296736bcf7` |

Each venue directory contains the exact `probe-config.json`, terminal
`health.json`, terminal `result.json` and the writer lock from the attempted
empty raw store. The null raw identities are intentional evidence that zero
frames were admitted.

These results qualify only this Windows network path at this instant. They are
not global endpoint availability claims and not economic evidence.

`access-bundle-v1/` is the offline-rebuilt access inventory pinned by
`bundle_sha256=965a42f2169c16201323477c0eb1ba7a8b540b24109c1d9252d5d9fcce55bbe5`.
It contains no authenticated raw frame, depth/trade dataset, replay, campaign
receipt or economic claim.
