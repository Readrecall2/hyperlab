# Research Data Plane fixtures

Every JSON payload in this directory is synthetic and carries the visible
`SYNTHETIC/FIXTURE` label. These payloads test documented public schemas; they
are not probe evidence, venue attestations, economic evidence, or real trades.

The `lighter_*` fixtures exercise public market metadata, order-book
`nonce/begin_nonce/offset` continuity, ticker/BBO, market stats and trades. They
are deterministic schema fixtures, never observations from the bounded probe.
