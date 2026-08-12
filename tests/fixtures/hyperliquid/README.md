# Hyperliquid public-message fixtures

## Provenance

The WebSocket values in this directory were captured from Hyperliquid's public
mainnet endpoints on 2026-08-12. The capture did not use a wallet address,
credential, signer, or private endpoint. `ws_open.txt` is the exact SDK-visible
opening text. Decimal values remain JSON strings, as sent by the venue.

The following captured messages retain their exchange values:

- `ws_subscription_response.json`
- `ws_pong.json`
- `ws_l2_book.json`
- `ws_bbo.json`
- `ws_trades.json`
- `ws_candle.json`
- `ws_active_asset_ctx.json`

The L2 capture was reduced to the first two levels on each side. The trades
capture was reduced to one trade. Those are the only array reductions in the
WebSocket capture.

The original transaction hash and the two public user addresses in
`ws_trades.json` were discarded and replaced deterministically:

- transaction hash -> `0x1111...1111` (64 hexadecimal digits)
- first user -> `0xaaaa...aaaa` (40 hexadecimal digits)
- second user -> `0xbbbb...bbbb` (40 hexadecimal digits)

No lookup table containing the originals is retained.

## Documentary and derived fixtures

`ws_bbo_one_sided.json` is a deliberate robustness variant of the captured BBO:
'rest_spot_context_prev_day_zero.json' is a reduced exact public
'spotMetaAndAssetCtxs' response captured on mainnet on 2026-08-12. It keeps
the '@189' pair, its two public token metadata objects, and its context.
Hyperliquid returned 'prevDayPx="0.0"', 'dayNtlVlm="0.0"', a positive
'markPx', and no 'midPx'; no field was fabricated.

the ask was replaced by `null`. `ws_active_spot_asset_ctx.json` is a documentary
shape fixture because no active-spot message was captured during this session;
it must not be cited as an observed market value.

The `rest_*.json` files are reduced public REST contract fixtures derived from
the same captured BTC values, rather than independent archival REST responses.
Bootstrap universes contain one instrument/context, funding history contains one
row, candles contain one candle, and L2 contains two levels per side. The spot
bootstrap is explicitly documentary for the same reason as the active-spot
fixture. These reductions keep replay small and visible; they are not synthetic
market results and must not be used for research conclusions.

`replay/` contains byte-for-byte copies of all nine supported WebSocket shapes
under numeric names, including the documentary active-spot shape described
above. The sequence covers opening text, subscription acknowledgement, pong,
perp and spot contexts, BBO, L2, trades, and candles so replay can prove stable
path ordering and deterministic parsing without accessing a network.
