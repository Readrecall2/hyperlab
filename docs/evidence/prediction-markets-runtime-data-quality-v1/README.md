# Local bounded Kalshi diagnostic

This directory records the only direct public diagnostic allowed for the
runtime data-quality correction. It was run locally, never on the VPS, with the
`PAPER_ONLY / GHOST_ONLY / PUBLIC_DATA_ONLY` boundary and no secret, account,
private endpoint, WebSocket authentication, or order capability.

The run
`kalshi-local-diagnostic-20260828t105443z-bcb5280f` made one bounded public REST
call. DNS resolution for `external-api.kalshi.com` failed. Its terminal result
is `PUBLIC_SOURCE_UNAVAILABLE`, with `network_calls=1`, `frames=0`,
`segments=0`, and no raw manifest/root. This terminal result was not rerun and
is not evidence about the wire timestamp that caused the real VPS ordinal 0 to
be rejected.

The `raw` directory contains only the writer's one-byte lock sentinel; it
contains no segment, manifest, response payload, or substitute raw evidence.
