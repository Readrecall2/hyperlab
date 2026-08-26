# Base Realism / Ghost V1

Boundary: `PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY`.

This package is a venue-neutral hypothetical execution mechanism. It never
selects a strategy, claims an edge, opens a network connection, or exposes a
wallet, signer, private endpoint, or order route.

The canonical fixture records exact decimal strings, explicit model/version
identities, source/receive/decision/transit/admission/ack/cancel time, clock
uncertainty, point-in-time grids and fee schedules, finite executable depth,
queue scenarios, health transitions, hypothetical orders, and multi-leg
dependencies. Binary floats, implicit midpoint execution, infinite depth,
negative primary fees/rebates, non-pessimistic primary queues, and unlabelled
synthetic data are refused.

A decision may consume only a book whose receive-time uncertainty interval is
strictly known before the decision interval. Gap, reconnect, outage, and stale
states block every new hypothetical admission. POST_ONLY/ALO orders reject when
marketable and require observed aggressor flow to deplete the configured queue;
touch alone never fills. IOC and forced close consume finite levels in order and
preserve unfilled residual inventory.

Research manifests are opened by explicit SHA-256 through
`ResearchSegmentReader`. The V1 manifest adapter intentionally accepts exactly
one authenticated `ghost_fixture` envelope with visible
`SYNTHETIC/FIXTURE` provenance. Future venue adapters may map immutable public
envelopes to the same primitives without changing execution semantics.

PnL attribution always exposes spread, signal, fees, adverse selection,
inventory, hedge, funding, opportunity cost, forced close, reward, and rebate.
V1 does not infer spread, signal, or adverse-selection alpha, so those fields
remain exactly zero. Primary reward and rebate are exactly zero. This is an
explicit limitation, not favorable economic evidence. Exposure, capital
immobilization, partial fills, residual closeout, every NO-TRADE reason, and
reconciliation differences are reported.
