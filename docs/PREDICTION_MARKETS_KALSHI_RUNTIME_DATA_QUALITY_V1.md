# Prediction Markets — Kalshi runtime data quality V1

## Boundary and verdict

This change is strictly `PAPER_ONLY / GHOST_ONLY / PUBLIC_DATA_ONLY`. It adds no
wallet, signer, credential, private API, order route, live command, economic
claim, or retrospective backfill.

The rejected Kalshi ordinal 0 of campaign
`pm-20260828t024827z-bcb5280f` is immutable terminal evidence. It remains
`PUBLIC_SOURCE_INVALID`, `source_usable=false`, and
`economic_eligible=false`. The runner treats every authenticated terminal
ordinal as accounted, so neither restart nor recovery can schedule it again.

## Proven root cause and evidence limit

The previous Kalshi adapter selected the first truthy field among
`updated_time`, `last_updated_ts`, `created_time`, and `settlement_ts` for
several unrelated response shapes. That generic fallback conflated observation
freshness with creation, settlement, effective, schedule, and future-resume
times. The common envelope correctly rejected a normalized negative epoch with:

`ValueError:source timestamp must be absent or a non-negative UTC epoch value`

The immutable raw segments from the real rejected slot are not present in this
local worktree. Therefore, the generic selection defect and the negative-epoch
failure path are proven, but the exact wire field selected for that particular
slot is not. `settlement_ts` is a plausible candidate, not an asserted fact.

## Explicit response-time contract

Kalshi's activated REST response schemas document date-time values as RFC3339
strings. Unix-second integers are documented for request filters, not for an
activated response timestamp. Consequently the numeric response-epoch allowlist
is empty. A future numeric field requires a new field-specific contract and an
explicit unit before it can be admitted.

| Feed | Raw field mapped to `source_timestamp` | Rule |
|---|---|---|
| `series` | singular `series.last_updated_ts` | strict RFC3339; list/empty page is absent |
| `events` | singular `event.last_updated_ts` | strict RFC3339; list/empty page is absent |
| `markets` | singular `market.updated_time` | strict RFC3339; never `settlement_ts` |
| `order_book` | none | explicitly absent |
| `trades` | `trades[0].created_time` only for a one-record page | strict RFC3339; multi/empty page is absent |
| `block_trades` | `trades[0].created_time` only for a one-record page | strict RFC3339 plus exact `is_block_trade=true` |
| `incentives` | none | start/end are program windows, not observation freshness |
| `fee_changes` | none | `scheduled_ts` is an effective schedule |
| `event_fee_changes` | none | `scheduled_ts` is an effective schedule |
| `event_metadata` | none | no reliable source timestamp |
| `exchange_status` | none | estimated resume is nullable/future, not freshness |
| `exchange_schedule` | none | schedule windows are not freshness |
| `historical_cutoff` | none | the three fields are different archive boundaries |
| `historical_markets` | singular `market.updated_time` | strict RFC3339; list/empty page is absent |
| `historical_trades` | `trades[0].created_time` only for a one-record page | strict RFC3339; multi/empty page is absent |

Accepted RFC3339 values require an explicit `Z` or numeric offset and allow one
to nine fractional digits. They normalize deterministically to UTC nanoseconds.
Boolean, numeric, negative/pre-epoch, non-finite, empty, malformed, offset-free,
and ambiguous values fail closed. An absent documented field remains absent; it
is never replaced with `received_at`.

Primary contracts consulted:

- <https://docs.kalshi.com/api-reference/market/get-series-list>
- <https://docs.kalshi.com/api-reference/events/get-events>
- <https://docs.kalshi.com/api-reference/market/get-markets>
- <https://docs.kalshi.com/api-reference/market/get-market-orderbook>
- <https://docs.kalshi.com/api-reference/market/get-trades>
- <https://docs.kalshi.com/api-reference/incentive-programs/get-incentives>
- <https://docs.kalshi.com/api-reference/exchange/get-series-fee-changes>
- <https://docs.kalshi.com/api-reference/events/get-event-fee-changes>
- <https://docs.kalshi.com/api-reference/events/get-event-metadata>
- <https://docs.kalshi.com/api-reference/exchange/get-exchange-status>
- <https://docs.kalshi.com/api-reference/exchange/get-exchange-schedule>
- <https://docs.kalshi.com/api-reference/historical/get-historical-cutoff-timestamps>
- <https://docs.kalshi.com/api-reference/historical/get-historical-markets>
- <https://docs.kalshi.com/api-reference/historical/get-historical-trades>
- <https://docs.kalshi.com/getting_started/pagination>
- <https://docs.kalshi.com/getting_started/fixed_point_migration>

## Adjacent runtime invariants closed

- Cursor values are opaque exact strings. Absent, null, and empty mean terminal
  pagination; booleans, numbers, arrays, objects, control characters, and
  oversized strings are invalid.
- IDs/tickers are exact non-empty strings; no `str()` coercion remains in the
  activated Kalshi scope and trade path.
- Empty documented pages are successful raw observations, not network
  unavailability. Pagination preserves first-record ordering and admits a
  duplicate identity only once.
- Prices use bounded fixed-point dollar strings; quantities use fixed-point
  quantity strings; floats and boolean coercions are rejected. YES and NO
  trade prices are independently bounded: the response contract does not state
  that both fields must sum to one, and the documented example does not satisfy
  such a constraint, so no undocumented complement rule is imposed.
- Trades require exact boolean `is_block_trade`; current block/non-block feeds
  reject contradictory records.
- Series fee changes use `series_fee_change_arr`; event fee changes use
  `event_fee_changes`; nullable event overrides must be cleared or supplied as
  a pair.
- Authenticated excluded/recovered terminals remain terminal and economically
  excluded. `PUBLIC_SOURCE_UNAVAILABLE_RECOVERED` no longer becomes source
  usable because its network/queue/tail counters cannot be reconstructed.
- A zero-frame `PUBLIC_SOURCE_INVALID` terminal receipt remains authenticated
  terminal evidence. It requires an explicit error, carries no fabricated raw
  artifact, and is neither retried nor converted into a source-unavailable slot.
- A broken ledger symlink is corruption, not an absent ledger. Appends use a
  no-follow regular-file descriptor where the platform provides it.
- The monitor requires exact `NRestarts=0`. A data-quality alert with zero
  restarts remains an alert but is not misreported as a systemd operational
  failure. Missing collection metrics remain absent, never zero.

## P3 limitations

- Kalshi public REST does not prove a cross-endpoint total order or order-book
  sequence. `gaps=0` is not continuity certification.
- A multi-record page has no single justified source timestamp, even if every
  record contains a time.
- Kalshi's order-book guide says the endpoint needs no authentication while the
  generated endpoint reference currently displays authentication headers. The
  frozen public-only pack does not add credentials to resolve that documentation
  contradiction; any resulting HTTP rejection remains honestly classified as
  `PUBLIC_SOURCE_UNAVAILABLE`.
- The one allowed direct local diagnostic terminated on DNS unavailability and
  yielded no raw frame. It cannot refine the real rejected slot's wire field and
  must not be rerun as if its result were non-terminal.
