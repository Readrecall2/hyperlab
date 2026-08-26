from __future__ import annotations

import hashlib
import json
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import ROUND_DOWN, Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, cast

from hyperlab.research_data.adapters import (
    HYPERLIQUID_METADATA_VERSION,
    HYPERLIQUID_PUBLIC_HTTP_URL,
    HYPERLIQUID_PUBLIC_WEBSOCKET_URL,
)
from hyperlab.research_data.canonical import canonical_json_bytes
from hyperlab.research_data.datasets import MARKOUT_HORIZONS_MS, H1Action
from hyperlab.research_data.envelope import SYNTHETIC_FIXTURE_LABEL, PublicDataEnvelope, Venue
from hyperlab.research_data.segments import ResearchSegmentReader

from .models import (
    AUTHENTICATED_PUBLIC_RESEARCH_LABEL,
    BOUNDARY,
    GhostReport,
    Side,
)
from .replay import GhostFixture, GhostReplay

H1_POLICY_VERSION = "HYPERLIQUID_H1_GHOST_V1"
H1_READY = "HYPERLIQUID_H1_GHOST_V1_READY_FOR_PROSPECTIVE_EVIDENCE"
ECONOMIC_NOT_AVAILABLE = "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE"
_ZERO = Decimal("0")
_BPS = Decimal("10000")
_NS_PER_MS = 1_000_000
_NS_PER_DAY = 86_400_000_000_000
_NS_PER_HOUR = 3_600_000_000_000


def _mapping(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _sequence(value: object, *, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be an array")
    return value


def _decimal(value: object, *, label: str, positive: bool = False) -> Decimal:
    if not isinstance(value, (str, int, Decimal)) or isinstance(value, bool):
        raise TypeError(f"{label} must be an exact decimal string or integer")
    result = Decimal(value)
    if not result.is_finite() or (positive and result <= 0):
        raise ValueError(f"{label} must be finite" + (" and positive" if positive else ""))
    return result


def _integer(value: object, *, label: str, positive: bool = False) -> int:
    if type(value) is not int or value < (1 if positive else 0):
        raise ValueError(f"{label} must be a valid integer")
    return value


def _text(value: object, *, label: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{label} must be non-empty text")
    return value


def _canonical_hash(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class H1Variant:
    variant_id: str
    threshold: Decimal
    requires_trade_flow_confirmation: bool
    status: str
    holdout_access: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "depth_imbalance_threshold": self.threshold,
            "holdout_access": self.holdout_access,
            "requires_trade_flow_confirmation": self.requires_trade_flow_confirmation,
            "status": self.status,
            "variant_id": self.variant_id,
        }


@dataclass(frozen=True, slots=True)
class H1PolicyConfig:
    body: dict[str, Any]
    config_sha256: str
    policy_id: str
    instruments: tuple[str, ...]
    variants: tuple[H1Variant, ...]
    primary_variant_id: str
    latency_scenarios_ms: tuple[int, ...]
    primary_hurdle_latency_ms: int
    queue_cancellation_ahead_credit: tuple[str, str, str]
    depth_levels: int
    stale_after_ns: int
    context_stale_after_ns: int
    trade_flow_lookback_ns: int
    funding_boundary_guard_ns: int
    markout_max_lateness_ns: int
    minimum_recent_trades: int
    imbalance_threshold: Decimal
    minimum_depth_notional: Decimal
    spread_bps_min: Decimal
    spread_bps_max: Decimal
    quote_notional: Decimal
    max_displayed_fraction: Decimal
    ttl_ns: int
    maker_fee_bps: Decimal
    taker_fee_bps: Decimal
    inventory_limit_notional: Decimal
    minimum_fills: int
    minimum_markets: int
    minimum_regimes: int
    top_one_percent_max: Decimal
    closeout_slippage_p99_max: Decimal
    inventory_notional_p99_max: Decimal

    @classmethod
    def from_path(cls, path: Path) -> H1PolicyConfig:
        return cls.from_bytes(path.read_bytes())

    @classmethod
    def from_bytes(cls, value: bytes) -> H1PolicyConfig:
        try:
            decoded = json.loads(value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("H1 policy config is not UTF-8 JSON") from error
        body = _mapping(decoded, label="H1 policy config")
        expected = {
            "action_policy",
            "boundary",
            "costs",
            "economic_claim",
            "economic_gates",
            "execution",
            "features",
            "policy_id",
            "risk",
            "runner",
            "schema_version",
            "splits",
            "status",
            "universe",
            "variants",
        }
        if set(body) != expected or body.get("schema_version") != 1:
            raise ValueError("H1 policy config fields or schema version are invalid")
        if body.get("boundary") != BOUNDARY:
            raise ValueError("H1 policy config crossed the research-only boundary")
        if body.get("economic_claim") != ECONOMIC_NOT_AVAILABLE:
            raise ValueError("H1 policy config cannot claim economic evidence")
        action = _mapping(body["action_policy"], label="H1 action policy")
        if action.get("quote_sides_at_once") != 1 or action.get("no_quote_is_explicit") is not True:
            raise ValueError("H1 policy must be one-sided with explicit NO_QUOTE")
        universe = _mapping(body["universe"], label="H1 universe")
        instruments = tuple(
            _text(item, label="H1 instrument")
            for item in _sequence(universe.get("instruments"), label="H1 instruments")
        )
        if (
            not instruments
            or len(instruments) != len(set(instruments))
            or any(not item.startswith("HL:") or not item.endswith(":perp") for item in instruments)
            or universe.get("selection_uses_future_pnl") is not False
        ):
            raise ValueError("H1 universe is not uniquely preselected without future PnL")
        raw_variants = _sequence(body["variants"], label="H1 variants")
        variants = tuple(
            H1Variant(
                variant_id=_text(item.get("variant_id"), label="H1 variant id"),
                threshold=_decimal(
                    item.get("depth_imbalance_threshold"), label="H1 variant threshold", positive=True
                ),
                requires_trade_flow_confirmation=item.get("requires_trade_flow_confirmation") is True,
                status=_text(item.get("status"), label="H1 variant status"),
                holdout_access=_text(item.get("holdout_access"), label="H1 holdout access"),
            )
            for item in (_mapping(raw, label="H1 variant") for raw in raw_variants)
        )
        primary = tuple(item for item in variants if item.status == "PRIMARY_FROZEN_UNOBSERVED")
        if len(primary) != 1 or any(item.holdout_access != "SEALED" for item in variants):
            raise ValueError("H1 variants require one frozen primary and sealed holdout")
        features = _mapping(body["features"], label="H1 features")
        execution = _mapping(body["execution"], label="H1 execution")
        risk = _mapping(body["risk"], label="H1 risk")
        costs = _mapping(body["costs"], label="H1 costs")
        economic_gates = _mapping(body["economic_gates"], label="H1 economic gates")
        latencies = tuple(
            _integer(item, label="H1 latency", positive=True)
            for item in _sequence(execution.get("latency_scenarios_ms"), label="H1 latencies")
        )
        if latencies != (100, 250, 500, 1_000) or execution.get(
            "primary_hurdle_latency_ms"
        ) != 500:
            raise ValueError("H1 latency scenarios differ from the frozen hurdle contract")
        queue = _mapping(
            execution.get("cancellation_ahead_credit"), label="H1 queue credit"
        )
        credits = (
            str(queue.get("pessimistic_primary")),
            str(queue.get("conservative")),
            str(queue.get("sensitivity_non_promotable")),
        )
        if credits != ("0", "0.25", "0.50"):
            raise ValueError("H1 queue cancellation-ahead credits exceed the frozen bounds")
        if execution.get("time_in_force") != "ALO":
            raise ValueError("H1 primary execution must be ALO")
        if costs.get("privileged_tier_assumed") is not False or str(
            costs.get("rebate_bps_primary")
        ) != "0":
            raise ValueError("H1 costs cannot assume privileges or primary rebates")
        splits = _mapping(body["splits"], label="H1 splits")
        if splits.get("policy") != "FIXED_UTC_OFFSETS_FROM_CAMPAIGN_START_NO_PNL_DEPENDENCE":
            raise ValueError("H1 chronological splits are not frozen")
        minimum_fills = _integer(
            economic_gates.get("minimum_fills"), label="H1 minimum fills", positive=True
        )
        minimum_markets = _integer(
            economic_gates.get("minimum_markets"), label="H1 minimum markets", positive=True
        )
        minimum_regimes = _integer(
            economic_gates.get("minimum_regimes"), label="H1 minimum regimes", positive=True
        )
        if (
            minimum_fills != 5_000
            or minimum_markets != 3
            or minimum_regimes != 3
            or economic_gates.get("lcb95_net_per_fill_positive_at_latency_ms") != 500
        ):
            raise ValueError("H1 prospective economic hurdles differ from the frozen contract")
        imbalance_threshold = _decimal(
            features.get("depth_imbalance_threshold"),
            label="H1 imbalance threshold",
            positive=True,
        )
        if primary[0].threshold != imbalance_threshold:
            raise ValueError("H1 primary variant differs from the executable threshold")
        if risk.get("one_active_quote_per_instrument") is not True:
            raise ValueError("H1 risk contract requires one active quote per instrument")
        return cls(
            body=body,
            config_sha256=_canonical_hash(body),
            policy_id=_text(body["policy_id"], label="H1 policy id"),
            instruments=instruments,
            variants=variants,
            primary_variant_id=primary[0].variant_id,
            latency_scenarios_ms=latencies,
            primary_hurdle_latency_ms=500,
            queue_cancellation_ahead_credit=credits,
            depth_levels=_integer(features.get("book_depth_levels"), label="H1 depth levels", positive=True),
            stale_after_ns=_integer(features.get("stale_after_ms"), label="H1 stale age", positive=True)
            * _NS_PER_MS,
            context_stale_after_ns=_integer(
                features.get("context_stale_after_ms"), label="H1 context age", positive=True
            )
            * _NS_PER_MS,
            trade_flow_lookback_ns=_integer(
                features.get("trade_flow_lookback_ms"), label="H1 flow lookback", positive=True
            )
            * _NS_PER_MS,
            funding_boundary_guard_ns=_integer(
                features.get("funding_boundary_guard_ms"), label="H1 funding guard", positive=True
            )
            * _NS_PER_MS,
            markout_max_lateness_ns=_integer(
                features.get("markout_max_lateness_ms"),
                label="H1 markout maximum lateness",
                positive=True,
            )
            * _NS_PER_MS,
            minimum_recent_trades=_integer(
                features.get("minimum_recent_trades"), label="H1 minimum trades", positive=True
            ),
            imbalance_threshold=imbalance_threshold,
            minimum_depth_notional=_decimal(
                features.get("minimum_depth_notional_each_side_usdc"),
                label="H1 minimum depth",
                positive=True,
            ),
            spread_bps_min=_decimal(
                features.get("spread_bps_min"), label="H1 minimum spread", positive=True
            ),
            spread_bps_max=_decimal(
                features.get("spread_bps_max"), label="H1 maximum spread", positive=True
            ),
            quote_notional=_decimal(
                execution.get("quote_notional_usdc"), label="H1 quote notional", positive=True
            ),
            max_displayed_fraction=_decimal(
                execution.get("quote_size_max_displayed_fraction"),
                label="H1 displayed fraction",
                positive=True,
            ),
            ttl_ns=_integer(execution.get("ttl_ms"), label="H1 TTL", positive=True)
            * _NS_PER_MS,
            maker_fee_bps=_decimal(costs.get("maker_fee_bps"), label="H1 maker fee"),
            taker_fee_bps=_decimal(costs.get("taker_fee_bps"), label="H1 taker fee"),
            inventory_limit_notional=_decimal(
                risk.get("inventory_limit_notional_usdc"),
                label="H1 inventory limit",
                positive=True,
            ),
            minimum_fills=minimum_fills,
            minimum_markets=minimum_markets,
            minimum_regimes=minimum_regimes,
            top_one_percent_max=_decimal(
                economic_gates.get("top_one_percent_net_contribution_max"),
                label="H1 top-one-percent gate",
                positive=True,
            ),
            closeout_slippage_p99_max=_decimal(
                economic_gates.get("closeout_slippage_p99_bps_max"),
                label="H1 closeout p99 gate",
                positive=True,
            ),
            inventory_notional_p99_max=_decimal(
                economic_gates.get("inventory_notional_p99_usdc_max"),
                label="H1 inventory p99 gate",
                positive=True,
            ),
        )


class H1Split(StrEnum):
    TRAIN = "TRAIN"
    VALIDATION = "VALIDATION"
    HOLDOUT = "HOLDOUT"


@dataclass(frozen=True, slots=True)
class H1Markout:
    horizon_ms: int
    observed_at_ns: int
    markout_bps: Decimal | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "horizon_ms": self.horizon_ms,
            "markout_bps": self.markout_bps,
            "observed_at_ns": self.observed_at_ns,
        }


@dataclass(frozen=True, slots=True)
class H1Decision:
    observation_id: str
    order_id: str | None
    instrument_id: str
    decision_time_ns: int
    source_arrival_sequence: int
    action: H1Action
    reason: str
    split: H1Split
    regime: str
    quote_price: Decimal | None
    quote_quantity: Decimal | None
    decision_mid: Decimal | None
    features: dict[str, Decimal | int | str | None]
    markouts: tuple[H1Markout, ...]
    fill_to_close_markout_bps: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action.value,
            "decision_mid": self.decision_mid,
            "decision_time_ns": self.decision_time_ns,
            "features": self.features,
            "fill_to_close_markout_bps": self.fill_to_close_markout_bps,
            "instrument_id": self.instrument_id,
            "markouts": [item.to_dict() for item in self.markouts],
            "observation_id": self.observation_id,
            "order_id": self.order_id,
            "quote_price": self.quote_price,
            "quote_quantity": self.quote_quantity,
            "reason": self.reason,
            "regime": self.regime,
            "source_arrival_sequence": self.source_arrival_sequence,
            "split": self.split.value,
        }


@dataclass(frozen=True, slots=True)
class H1Attribution:
    spread: Decimal
    signal: Decimal
    fees: Decimal
    adverse_selection: Decimal
    inventory: Decimal
    funding: Decimal
    forced_close: Decimal
    opportunity_cost: Decimal
    rebate: Decimal
    realized_pnl: Decimal | None
    unrealized_pnl: Decimal | None
    net: Decimal
    reconciliation_difference: Decimal

    def to_dict(self) -> dict[str, Any]:
        return {
            "adverse_selection": self.adverse_selection,
            "fees": self.fees,
            "forced_close": self.forced_close,
            "funding": self.funding,
            "inventory": self.inventory,
            "net": self.net,
            "opportunity_cost": self.opportunity_cost,
            "realized_pnl": self.realized_pnl,
            "rebate": self.rebate,
            "reconciliation_difference": self.reconciliation_difference,
            "signal": self.signal,
            "spread": self.spread,
            "unrealized_pnl": self.unrealized_pnl,
        }


@dataclass(frozen=True, slots=True)
class H1Concentration:
    by_instrument: dict[str, Decimal]
    by_utc_day: dict[str, Decimal]
    by_event: dict[str, Decimal]
    top_one_percent_share: Decimal | None
    inventory_notional_p99: Decimal | None
    closeout_slippage_p99_bps: Decimal | None
    lcb95_net_per_fill: Decimal | None
    conservative_fills: int
    completed_inventory_matches: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "by_event": self.by_event,
            "by_instrument": self.by_instrument,
            "by_utc_day": self.by_utc_day,
            "closeout_slippage_p99_bps": self.closeout_slippage_p99_bps,
            "completed_inventory_matches": self.completed_inventory_matches,
            "conservative_fills": self.conservative_fills,
            "inventory_notional_p99": self.inventory_notional_p99,
            "lcb95_net_per_fill": self.lcb95_net_per_fill,
            "top_one_percent_share": self.top_one_percent_share,
        }


@dataclass(frozen=True, slots=True)
class H1LatencyReport:
    latency_ms: int
    role: str
    promotable_alone: bool
    decisions: tuple[H1Decision, ...]
    ghost: GhostReport
    attribution: H1Attribution
    concentration: H1Concentration
    economic_gates: dict[str, bool]
    limitations: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "attribution": self.attribution.to_dict(),
            "concentration": self.concentration.to_dict(),
            "decisions": [item.to_dict() for item in self.decisions],
            "economic_gates": self.economic_gates,
            "ghost": self.ghost.to_dict(),
            "latency_ms": self.latency_ms,
            "limitations": list(self.limitations),
            "promotable_alone": self.promotable_alone,
            "role": self.role,
        }


@dataclass(frozen=True, slots=True)
class H1StudyReport:
    policy_config_sha256: str
    raw_manifest_sha256: str
    raw_root_sha256: str
    segment_sha256s: tuple[str, ...]
    synthetic: bool
    latency_reports: tuple[H1LatencyReport, ...]
    variants: tuple[H1Variant, ...]
    technical_verdict: str = H1_READY
    economic_status: str = ECONOMIC_NOT_AVAILABLE
    boundary: str = BOUNDARY
    schema_version: int = 1

    def _body(self) -> dict[str, Any]:
        return {
            "boundary": self.boundary,
            "economic_status": self.economic_status,
            "latency_reports": [item.to_dict() for item in self.latency_reports],
            "policy_config_sha256": self.policy_config_sha256,
            "raw_manifest_sha256": self.raw_manifest_sha256,
            "raw_root_sha256": self.raw_root_sha256,
            "schema_version": self.schema_version,
            "segment_sha256s": list(self.segment_sha256s),
            "synthetic": self.synthetic,
            "technical_verdict": self.technical_verdict,
            "variants": [item.to_dict() for item in self.variants],
        }

    @property
    def report_sha256(self) -> str:
        return _canonical_hash(self._body())

    def to_dict(self) -> dict[str, Any]:
        return {**self._body(), "report_sha256": self.report_sha256}

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())


@dataclass(frozen=True, slots=True)
class _Bbo:
    receive_ns: int
    arrival: int
    bid: Decimal
    bid_quantity: Decimal
    ask: Decimal
    ask_quantity: Decimal

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")


@dataclass(frozen=True, slots=True)
class _Context:
    receive_ns: int
    arrival: int
    funding: Decimal | None


@dataclass(frozen=True, slots=True)
class _Trade:
    receive_ns: int
    arrival: int
    event_id: str
    instrument: str
    price: Decimal
    quantity: Decimal
    side: Side


@dataclass(frozen=True, slots=True)
class _Book:
    receive_ns: int
    source_ns: int | None
    arrival: int
    instrument: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    healthy: bool

    @property
    def mid(self) -> Decimal:
        return (self.bids[0][0] + self.asks[0][0]) / Decimal("2")


def _raw_json(envelope: PublicDataEnvelope) -> object:
    try:
        return json.loads(envelope.raw_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Hyperliquid Research payload is not UTF-8 JSON") from error


def _data(envelope: PublicDataEnvelope) -> dict[str, Any]:
    decoded = _mapping(_raw_json(envelope), label="Hyperliquid frame")
    candidate = decoded.get("data", decoded)
    return _mapping(candidate, label="Hyperliquid frame data")


def _parse_bbo(envelope: PublicDataEnvelope) -> _Bbo:
    data = _data(envelope)
    sides = _sequence(data.get("bbo"), label="Hyperliquid BBO")
    if len(sides) != 2:
        raise ValueError("Hyperliquid BBO must contain exactly two sides")
    bid = _mapping(sides[0], label="Hyperliquid bid")
    ask = _mapping(sides[1], label="Hyperliquid ask")
    result = _Bbo(
        receive_ns=envelope.receive_timestamp_utc_ns,
        arrival=envelope.arrival_sequence,
        bid=_decimal(bid.get("px"), label="BBO bid", positive=True),
        bid_quantity=_decimal(bid.get("sz"), label="BBO bid size", positive=True),
        ask=_decimal(ask.get("px"), label="BBO ask", positive=True),
        ask_quantity=_decimal(ask.get("sz"), label="BBO ask size", positive=True),
    )
    if result.bid >= result.ask:
        raise ValueError("Hyperliquid BBO is crossed")
    return result


def _parse_book(envelope: PublicDataEnvelope) -> _Book:
    data = _data(envelope)
    sides = _sequence(data.get("levels"), label="Hyperliquid L2 sides")
    if len(sides) != 2:
        raise ValueError("Hyperliquid L2 must contain exactly two sides")

    def levels(raw: object, *, reverse: bool) -> tuple[tuple[Decimal, Decimal], ...]:
        result = tuple(
            (
                _decimal(item.get("px"), label="L2 price", positive=True),
                _decimal(item.get("sz"), label="L2 quantity", positive=True),
            )
            for item in (
                _mapping(entry, label="L2 level")
                for entry in _sequence(raw, label="L2 side")
            )
        )
        if not result:
            raise ValueError("Hyperliquid L2 side is empty")
        prices = tuple(item[0] for item in result)
        if prices != tuple(sorted(prices, reverse=reverse)) or len(prices) != len(set(prices)):
            raise ValueError("Hyperliquid L2 levels are not strictly price ordered")
        return result

    bids = levels(sides[0], reverse=True)
    asks = levels(sides[1], reverse=False)
    if bids[0][0] >= asks[0][0]:
        raise ValueError("Hyperliquid L2 is crossed")
    assert envelope.instrument_id is not None
    return _Book(
        receive_ns=envelope.receive_timestamp_utc_ns,
        source_ns=envelope.source_timestamp_ns,
        arrival=envelope.arrival_sequence,
        instrument=envelope.instrument_id,
        bids=bids,
        asks=asks,
        healthy=not (envelope.state.gap_detected or envelope.state.reconnect),
    )


def _parse_context(envelope: PublicDataEnvelope) -> _Context:
    data = _data(envelope)
    context = _mapping(data.get("ctx"), label="Hyperliquid active context")
    funding = None if context.get("funding") is None else _decimal(
        context.get("funding"), label="Hyperliquid funding"
    )
    return _Context(
        receive_ns=envelope.receive_timestamp_utc_ns,
        arrival=envelope.arrival_sequence,
        funding=funding,
    )


def _parse_trades(envelope: PublicDataEnvelope) -> tuple[_Trade, ...]:
    data = _mapping(_raw_json(envelope), label="Hyperliquid trade frame").get("data")
    rows = _sequence(data, label="Hyperliquid trades")
    assert envelope.instrument_id is not None
    parsed: list[_Trade] = []
    for index, raw in enumerate(rows):
        item = _mapping(raw, label="Hyperliquid trade")
        side = {"B": Side.BUY, "A": Side.SELL}.get(str(item.get("side")))
        if side is None:
            raise ValueError("Hyperliquid aggressor side is unresolved")
        source_id = f"{item.get('time')}:{item.get('coin')}:{item.get('tid')}"
        parsed.append(
            _Trade(
                receive_ns=envelope.receive_timestamp_utc_ns,
                arrival=envelope.arrival_sequence,
                event_id=f"trade:{envelope.arrival_sequence}:{index}:{source_id}",
                instrument=envelope.instrument_id,
                price=_decimal(item.get("px"), label="trade price", positive=True),
                quantity=_decimal(item.get("sz"), label="trade quantity", positive=True),
                side=side,
            )
        )
    return tuple(parsed)


def _metadata(envelopes: Sequence[PublicDataEnvelope]) -> dict[str, list[tuple[int, int]]]:
    versions: dict[str, list[tuple[int, int]]] = {}
    for envelope in envelopes:
        if envelope.feed_type != "metadata":
            continue
        decoded = _raw_json(envelope)
        if not isinstance(decoded, list) or not decoded or not isinstance(decoded[0], dict):
            continue
        universe = decoded[0].get("universe")
        if not isinstance(universe, list):
            continue
        for raw in universe:
            if not isinstance(raw, dict) or raw.get("name") is None:
                continue
            decimals = raw.get("szDecimals")
            if type(decimals) is not int or decimals < 0 or decimals > 18:
                raise ValueError("Hyperliquid metadata size decimals are invalid")
            instrument = f"HL:{raw['name']}:perp"
            versions.setdefault(instrument, []).append(
                (envelope.receive_timestamp_utc_ns, decimals)
            )
    return versions


def _metadata_at(
    versions: Mapping[str, list[tuple[int, int]]], instrument: str, decision_ns: int
) -> int | None:
    eligible = [item for item in versions.get(instrument, ()) if item[0] <= decision_ns]
    return None if not eligible else sorted(eligible)[-1][1]


def _split(decision_ns: int, campaign_start_ns: int) -> H1Split:
    day = max(0, (decision_ns - campaign_start_ns) // _NS_PER_DAY)
    if day < 7:
        return H1Split.TRAIN
    if day < 10:
        return H1Split.VALIDATION
    return H1Split.HOLDOUT


def _regime(mid: Decimal, history: Sequence[tuple[int, Decimal]], at_ns: int) -> str:
    eligible = [item for item in history if at_ns - 30_000_000_000 <= item[0] < at_ns]
    if not eligible:
        return "REGIME_UNRESOLVED"
    prior = eligible[0][1]
    move = abs(mid - prior) / prior * _BPS
    if move <= Decimal("5"):
        return "LOW"
    if move <= Decimal("20"):
        return "MID"
    return "HIGH"


def _markouts(
    *,
    action: H1Action,
    decision_ns: int,
    decision_mid: Decimal | None,
    mids: Sequence[tuple[int, Decimal]],
    max_lateness_ns: int,
) -> tuple[H1Markout, ...]:
    result: list[H1Markout] = []
    sign = Decimal("1") if action is H1Action.BID_ONLY else Decimal("-1")
    for horizon in MARKOUT_HORIZONS_MS:
        target = decision_ns + horizon * _NS_PER_MS
        future = next((item for item in mids if item[0] >= target), None)
        markout = None
        observed_at = target if future is None else future[0]
        if (
            action is not H1Action.NO_QUOTE
            and decision_mid is not None
            and future is not None
            and future[0] - target <= max_lateness_ns
        ):
            markout = sign * (future[1] - decision_mid) / decision_mid * _BPS
        result.append(H1Markout(horizon, observed_at, markout))
    return tuple(result)


def _price_quantum(book: _Book) -> Decimal:
    exponents = [
        cast(int, price.as_tuple().exponent)
        for price, _ in book.bids + book.asks
    ]
    if not exponents:
        raise ValueError("H1 grid requires observed authenticated L2 prices")
    return Decimal("1").scaleb(min(exponents))


def _floor_lot(value: Decimal, lot: Decimal) -> Decimal:
    return (value / lot).to_integral_value(rounding=ROUND_DOWN) * lot


def _role(latency_ms: int) -> str:
    return {
        100: "BOUNDARY_NEVER_SUFFICIENT",
        250: "DIAGNOSTIC",
        500: "PRIMARY_HURDLE",
        1_000: "STRESS",
    }[latency_ms]


@dataclass(slots=True)
class _Prepared:
    events: list[dict[str, Any]]
    decisions: list[H1Decision]
    books: list[_Book]
    mids: dict[str, list[tuple[int, Decimal]]]
    grid_versions: dict[str, list[tuple[int, Decimal, int]]]


def _prepare(
    envelopes: Sequence[PublicDataEnvelope], config: H1PolicyConfig, *, latency_ms: int
) -> _Prepared:
    metadata = _metadata(envelopes)
    latest_bbo: dict[str, _Bbo] = {}
    latest_context: dict[str, _Context] = {}
    trades: dict[str, deque[_Trade]] = {instrument: deque() for instrument in config.instruments}
    books: list[_Book] = []
    mids: dict[str, list[tuple[int, Decimal]]] = {instrument: [] for instrument in config.instruments}
    decisions: list[H1Decision] = []
    events: list[dict[str, Any]] = []
    grid_versions: dict[str, list[tuple[int, Decimal, int]]] = {
        instrument: [] for instrument in config.instruments
    }
    generation_floor = 0
    campaign_start_ns = envelopes[0].receive_timestamp_utc_ns

    for envelope in envelopes:
        if envelope.venue is not Venue.HYPERLIQUID:
            raise ValueError("H1 replay accepts only Hyperliquid Research segments")
        if envelope.state.duplicate:
            continue
        if envelope.state.gap_detected or envelope.state.reconnect:
            generation_floor = envelope.arrival_sequence
            for configured_instrument in config.instruments:
                events.append(
                    {
                        "event_id": f"health:{envelope.arrival_sequence}:{configured_instrument}",
                        "instrument_id": configured_instrument,
                        "kind": "GAP" if envelope.state.gap_detected else "RECONNECT",
                        "reason": envelope.state.reason or "SOURCE_GAP_OR_RECONNECT",
                        "receive_ns": envelope.receive_timestamp_utc_ns,
                        "venue": "hyperliquid",
                    }
                )
        envelope_instrument = envelope.instrument_id
        if envelope.feed_type == "bbo" and envelope_instrument in config.instruments:
            parsed_bbo = _parse_bbo(envelope)
            latest_bbo[envelope_instrument] = parsed_bbo
            mids[envelope_instrument].append((parsed_bbo.receive_ns, parsed_bbo.mid))
        elif (
            envelope.feed_type == "active_asset_context"
            and envelope_instrument in config.instruments
        ):
            latest_context[envelope_instrument] = _parse_context(envelope)
        elif envelope.feed_type == "trades" and envelope_instrument in config.instruments:
            for trade in _parse_trades(envelope):
                trades[trade.instrument].append(trade)
                events.append(
                    {
                        "aggressor_side": trade.side.value,
                        "event_id": trade.event_id,
                        "instrument_id": trade.instrument,
                        "kind": "TRADE",
                        "price": format(trade.price, "f"),
                        "quantity": format(trade.quantity, "f"),
                        "receive_ns": trade.receive_ns,
                        "source_ns": None,
                        "venue": "hyperliquid",
                    }
                )
        elif envelope.feed_type == "l2_book" and envelope_instrument in config.instruments:
            book = _parse_book(envelope)
            books.append(book)
            mids[book.instrument].append((book.receive_ns, book.mid))
            bbo = latest_bbo.get(book.instrument)
            context = latest_context.get(book.instrument)
            metadata_decimals = _metadata_at(metadata, book.instrument, book.receive_ns)
            generation_fresh = (
                book.arrival > generation_floor
                and bbo is not None
                and bbo.arrival > generation_floor
                and context is not None
                and context.arrival > generation_floor
            )
            if metadata_decimals is not None:
                observed_tick = _price_quantum(book)
                versions = grid_versions[book.instrument]
                tick = observed_tick if not versions else min(versions[-1][1], observed_tick)
                version = (book.receive_ns, tick, metadata_decimals)
                if versions and versions[-1][0] == book.receive_ns and versions[-1][1:] != version[1:]:
                    raise ValueError("H1 point-in-time grid changes are ambiguous at one timestamp")
                if not versions or versions[-1][1:] != version[1:]:
                    versions.append(version)
                events.append(
                    {
                        "asks": [
                            [format(price, "f"), format(quantity, "f")]
                            for price, quantity in book.asks
                        ],
                        "bids": [
                            [format(price, "f"), format(quantity, "f")]
                            for price, quantity in book.bids
                        ],
                        "clock_uncertainty_ns": 0,
                        "event_id": f"book:{book.arrival}:{book.instrument}",
                        "instrument_id": book.instrument,
                        "kind": "BOOK",
                        "receive_ns": book.receive_ns,
                        "resync_complete": generation_fresh,
                        "source_ns": book.source_ns,
                        "venue": "hyperliquid",
                    }
                )
            else:
                events.append(
                    {
                        "event_id": f"grid-unresolved:{book.arrival}:{book.instrument}",
                        "instrument_id": book.instrument,
                        "kind": "GAP",
                        "reason": "POINT_IN_TIME_GRID_UNRESOLVED",
                        "receive_ns": book.receive_ns,
                        "venue": "hyperliquid",
                    }
                )
            decision_ns = book.receive_ns + 1
            observation_id = f"h1:{book.arrival}:{book.instrument}"
            action = H1Action.NO_QUOTE
            reason = "SIGNAL_NOT_SELECTIVE"
            quote_price: Decimal | None = None
            quote_quantity: Decimal | None = None
            features: dict[str, Decimal | int | str | None] = {}
            if not generation_fresh:
                reason = "SOURCE_GAP_OR_RECONNECT"
            elif metadata_decimals is None:
                reason = "POINT_IN_TIME_GRID_UNRESOLVED"
            elif bbo is None or decision_ns - bbo.receive_ns > config.stale_after_ns:
                reason = "BBO_STALE_OR_MISSING"
            elif context is None or decision_ns - context.receive_ns > config.context_stale_after_ns:
                reason = "CONTEXT_STALE_OR_MISSING"
            elif context.funding is None:
                reason = "POINT_IN_TIME_COST_UNRESOLVED"
            elif bbo.bid != book.bids[0][0] or bbo.ask != book.asks[0][0]:
                reason = "BBO_L2_DIVERGENCE"
            else:
                mid = book.mid
                spread_bps = (book.asks[0][0] - book.bids[0][0]) / mid * _BPS
                bid_depth = sum(
                    (price * quantity for price, quantity in book.bids[: config.depth_levels]), _ZERO
                )
                ask_depth = sum(
                    (price * quantity for price, quantity in book.asks[: config.depth_levels]), _ZERO
                )
                total_quantity = sum(
                    (quantity for _, quantity in book.bids[: config.depth_levels]), _ZERO
                ) + sum((quantity for _, quantity in book.asks[: config.depth_levels]), _ZERO)
                bid_quantity = sum(
                    (quantity for _, quantity in book.bids[: config.depth_levels]), _ZERO
                )
                ask_quantity = sum(
                    (quantity for _, quantity in book.asks[: config.depth_levels]), _ZERO
                )
                imbalance = (
                    _ZERO if total_quantity == 0 else (bid_quantity - ask_quantity) / total_quantity
                )
                microprice = (
                    book.asks[0][0] * book.bids[0][1]
                    + book.bids[0][0] * book.asks[0][1]
                ) / (book.bids[0][1] + book.asks[0][1])
                microprice_tilt = (microprice - mid) / mid * _BPS
                cutoff = decision_ns - config.trade_flow_lookback_ns
                while trades[book.instrument] and trades[book.instrument][0].receive_ns < cutoff:
                    trades[book.instrument].popleft()
                recent = tuple(
                    item
                    for item in trades[book.instrument]
                    if item.receive_ns <= decision_ns and item.arrival > generation_floor
                )
                buy_flow = sum(
                    (item.quantity for item in recent if item.side is Side.BUY), _ZERO
                )
                sell_flow = sum(
                    (item.quantity for item in recent if item.side is Side.SELL), _ZERO
                )
                flow_total = buy_flow + sell_flow
                flow_imbalance = _ZERO if flow_total == 0 else (buy_flow - sell_flow) / flow_total
                regime = _regime(mid, mids[book.instrument], decision_ns)
                features = {
                    "ask_depth_notional": ask_depth,
                    "bid_depth_notional": bid_depth,
                    "depth_imbalance": imbalance,
                    "microprice_tilt_bps": microprice_tilt,
                    "recent_trade_count": len(recent),
                    "regime": regime,
                    "spread_bps": spread_bps,
                    "trade_flow_imbalance": flow_imbalance,
                }
                boundary_offset = decision_ns % _NS_PER_HOUR
                funding_guard = config.funding_boundary_guard_ns
                if min(boundary_offset, _NS_PER_HOUR - boundary_offset) <= funding_guard:
                    reason = "FUNDING_BOUNDARY_UNRESOLVED"
                elif bid_depth < config.minimum_depth_notional or ask_depth < config.minimum_depth_notional:
                    reason = "INSUFFICIENT_FINITE_DEPTH"
                elif not (config.spread_bps_min <= spread_bps <= config.spread_bps_max):
                    reason = "SPREAD_OUTSIDE_FROZEN_BOUNDS"
                elif len(recent) < config.minimum_recent_trades:
                    reason = "TRADE_FLOW_INSUFFICIENT"
                elif imbalance >= config.imbalance_threshold and flow_imbalance >= 0 and microprice_tilt >= 0:
                    action = H1Action.BID_ONLY
                    reason = "BID_SIGNAL_CONFIRMED"
                elif imbalance <= -config.imbalance_threshold and flow_imbalance <= 0 and microprice_tilt <= 0:
                    action = H1Action.ASK_ONLY
                    reason = "ASK_SIGNAL_CONFIRMED"
                if action is not H1Action.NO_QUOTE:
                    quote_price = book.bids[0][0] if action is H1Action.BID_ONLY else book.asks[0][0]
                    displayed = book.bids[0][1] if action is H1Action.BID_ONLY else book.asks[0][1]
                    lot = Decimal("1").scaleb(-metadata_decimals)
                    quote_quantity = _floor_lot(
                        min(config.quote_notional / quote_price, displayed * config.max_displayed_fraction),
                        lot,
                    )
                    if quote_quantity <= 0:
                        action = H1Action.NO_QUOTE
                        reason = "QUOTE_SIZE_BELOW_POINT_IN_TIME_LOT"
                        quote_price = None
                        quote_quantity = None
            regime_value = str(features.get("regime", "REGIME_UNRESOLVED"))
            order_id = None
            if action is not H1Action.NO_QUOTE:
                assert quote_price is not None and quote_quantity is not None
                order_id = f"order:{latency_ms}:{book.arrival}:{book.instrument}"
                admission_ns = decision_ns + latency_ms * _NS_PER_MS
                events.append(
                    {
                        "cancel_request_ns": admission_ns + config.ttl_ns,
                        "decision_ns": decision_ns,
                        "depends_on_order_id": None,
                        "group_id": None,
                        "instrument_id": book.instrument,
                        "kind": "ORDER",
                        "leg_index": 0,
                        "limit_price": format(quote_price, "f"),
                        "order_id": order_id,
                        "quantity": format(quote_quantity, "f"),
                        "role": "PRIMARY",
                        "side": "BUY" if action is H1Action.BID_ONLY else "SELL",
                        "time_in_force": "ALO",
                        "venue": "hyperliquid",
                    }
                )
            decisions.append(
                H1Decision(
                    observation_id=observation_id,
                    order_id=order_id,
                    instrument_id=book.instrument,
                    decision_time_ns=decision_ns,
                    source_arrival_sequence=book.arrival,
                    action=action,
                    reason=reason,
                    split=_split(decision_ns, campaign_start_ns),
                    regime=regime_value,
                    quote_price=quote_price,
                    quote_quantity=quote_quantity,
                    decision_mid=book.mid,
                    features=features,
                    markouts=(),
                )
            )
    for index, decision in enumerate(decisions):
        decisions[index] = replace(
            decision,
            markouts=_markouts(
                action=decision.action,
                decision_ns=decision.decision_time_ns,
                decision_mid=decision.decision_mid,
                mids=tuple(sorted(mids[decision.instrument_id])),
                max_lateness_ns=config.markout_max_lateness_ns,
            ),
        )
    return _Prepared(events, decisions, books, mids, grid_versions)


def _model(prepared: _Prepared, config: H1PolicyConfig, *, latency_ms: int) -> dict[str, Any]:
    instruments = tuple(
        sorted(instrument for instrument, versions in prepared.grid_versions.items() if versions)
    )
    if not instruments:
        raise ValueError("H1 replay requires at least one authenticated L2 snapshot")
    grids = []
    costs = []
    mechanisms = []
    for instrument in instruments:
        versions = prepared.grid_versions[instrument]
        for index, (effective_from, tick, size_decimals) in enumerate(versions):
            effective_to = None if index + 1 == len(versions) else versions[index + 1][0]
            grids.append(
                {
                    "effective_from_ns": effective_from,
                    "effective_to_ns": effective_to,
                    "grid_id": f"hl-observed-grid-v1:{instrument}:{index}",
                    "instrument_id": instrument,
                    "lot_size": format(Decimal("1").scaleb(-size_decimals), "f"),
                    "tick_size": format(tick, "f"),
                    "venue": "hyperliquid",
                }
            )
        costs.append(
            {
                "effective_from_ns": 0,
                "effective_to_ns": None,
                "funding_bps": "0",
                "hedge_fee_bps": "0",
                "instrument_id": instrument,
                "maker_fee_bps": format(config.maker_fee_bps, "f"),
                "opportunity_cost_bps_per_second": "0",
                "schedule_id": f"hl-public-tier0-zero-rebate-v1:{instrument}",
                "taker_fee_bps": format(config.taker_fee_bps, "f"),
                "venue": "hyperliquid",
            }
        )
        mechanisms.append(
            {
                "cancel_replace_loses_priority": True,
                "effective_from_ns": 0,
                "effective_to_ns": None,
                "instrument_id": instrument,
                "maker_fill_requires_aggressor_flow": True,
                "mechanism_id": f"hl-public-alo-ghost-v1:{instrument}",
                "supported_time_in_force": ["POST_ONLY", "ALO", "IOC"],
                "supports_partial_fills": True,
                "venue": "hyperliquid",
            }
        )
    return {
        "closeout": {"model_id": "last-fresh-finite-depth-closeout-v1", "required": True},
        "cost_schedules": costs,
        "grids": grids,
        "latency": {
            "ack_ns": 0,
            "admission_ns": 0,
            "cancel_ns": latency_ms * _NS_PER_MS,
            "clock_uncertainty_ns": 0,
            "decision_ns": 0,
            "model_id": f"hl-h1-latency-{latency_ms}ms-v1",
            "transit_ns": latency_ms * _NS_PER_MS,
        },
        "mechanisms": mechanisms,
        "model_version": "BASE_REALISM_GHOST_V1",
        "multi_leg_timeout_ns": config.ttl_ns,
        "queue": {
            "conservative_ahead_multiplier": "0.75",
            "model_id": "hl-h1-displayed-ahead-queue-v1",
            "pessimistic_ahead_multiplier": "1",
            "primary": "PESSIMISTIC",
            "sensitivity_ahead_multiplier": "0.50",
        },
        "risk": {
            "max_abs_inventory_notional": format(config.inventory_limit_notional, "f"),
            "one_active_maker_per_instrument": True,
        },
        "stale_after_ns": config.stale_after_ns,
    }


def _fixture(prepared: _Prepared, config: H1PolicyConfig, *, latency_ms: int, synthetic: bool) -> GhostFixture:
    priority = {"GAP": 0, "RECONNECT": 0, "OUTAGE": 0, "BOOK": 1, "ORDER": 2, "TRADE": 3}
    events = sorted(
        prepared.events,
        key=lambda event: (
            cast(int, event["decision_ns"] if event["kind"] == "ORDER" else event["receive_ns"]),
            priority[cast(str, event["kind"])],
            str(event.get("event_id") or event.get("order_id")),
        ),
    )
    body = {
        "boundary": BOUNDARY,
        "events": events,
        "fixture_label": (
            SYNTHETIC_FIXTURE_LABEL if synthetic else AUTHENTICATED_PUBLIC_RESEARCH_LABEL
        ),
        "model": _model(prepared, config, latency_ms=latency_ms),
        "scenario_id": f"hyperliquid-h1-ghost-v1:{latency_ms}ms",
        "schema_version": 1,
    }
    encoded = canonical_json_bytes(body)
    return (
        GhostFixture.from_bytes(encoded)
        if synthetic
        else GhostFixture.from_authenticated_public_bytes(encoded)
    )


def _updated_decisions(
    decisions: Sequence[H1Decision], ghost: GhostReport
) -> tuple[H1Decision, ...]:
    by_order = {item.order_id: item for item in ghost.orders}
    result = []
    for decision in decisions:
        order = None if decision.order_id is None else by_order[decision.order_id]
        if order is not None and order.reason in {"ACTIVE_QUOTE_EXISTS", "INVENTORY_LIMIT"}:
            result.append(
                replace(
                    decision,
                    action=H1Action.NO_QUOTE,
                    reason=order.reason,
                    quote_price=None,
                    quote_quantity=None,
                )
            )
        else:
            result.append(decision)
    return tuple(result)


def _book_mid_at(books: Sequence[_Book], instrument: str, at_ns: int) -> Decimal | None:
    eligible = [book.mid for book in books if book.instrument == instrument and book.receive_ns <= at_ns]
    return None if not eligible else eligible[-1]


def _attribution(
    ghost: GhostReport, decisions: Sequence[H1Decision], books: Sequence[_Book]
) -> H1Attribution:
    decision_by_order = {item.order_id: item for item in decisions if item.order_id is not None}
    spread = _ZERO
    signal = _ZERO
    for fill in ghost.fills:
        decision = decision_by_order.get(fill.order_id)
        if decision is None or fill.forced or decision.decision_mid is None:
            continue
        sign = Decimal("1") if fill.side is Side.BUY else Decimal("-1")
        spread += sign * (decision.decision_mid - fill.price) * fill.quantity
        markout = next((item for item in decision.markouts if item.horizon_ms == 100), None)
        if markout is not None and markout.markout_bps is not None:
            signal += fill.notional * markout.markout_bps / _BPS
    forced_close = _ZERO
    for fill in ghost.fills:
        if not fill.forced:
            continue
        mid = _book_mid_at(books, fill.instrument_id, fill.timestamp_ns)
        if mid is not None:
            forced_close += fill.side.sign * (mid - fill.price) * fill.quantity
    gross_cashflow = ghost.pnl.inventory + ghost.pnl.hedge + ghost.pnl.forced_close
    inventory = _ZERO
    adverse = gross_cashflow - spread - signal - forced_close
    net = (
        spread
        + signal
        + ghost.pnl.fees
        + adverse
        + inventory
        + ghost.pnl.funding
        + forced_close
        + ghost.pnl.opportunity_cost
        + ghost.pnl.rebate
    )
    components = (
        spread
        + signal
        + ghost.pnl.fees
        + adverse
        + inventory
        + ghost.pnl.funding
        + forced_close
        + ghost.pnl.opportunity_cost
        + ghost.pnl.rebate
    )
    resolved = not ghost.exposure.unresolved_closeout
    return H1Attribution(
        spread=spread,
        signal=signal,
        fees=ghost.pnl.fees,
        adverse_selection=adverse,
        inventory=inventory,
        funding=ghost.pnl.funding,
        forced_close=forced_close,
        opportunity_cost=ghost.pnl.opportunity_cost,
        rebate=ghost.pnl.rebate,
        realized_pnl=net if resolved else None,
        unrealized_pnl=_ZERO if resolved else None,
        net=net,
        reconciliation_difference=net - components,
    )


@dataclass(slots=True)
class _OpenLot:
    order_id: str
    side: Side
    quantity: Decimal
    price: Decimal
    fee_per_quantity: Decimal
    timestamp_ns: int


@dataclass(frozen=True, slots=True)
class _RoundTripStats:
    concentration: H1Concentration
    fill_to_close_bps: dict[str, Decimal]


def _percentile_99(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, (99 * len(ordered) + 99) // 100)
    return ordered[min(rank - 1, len(ordered) - 1)]


def _round_trip_stats(
    ghost: GhostReport,
    decisions: Sequence[H1Decision],
    books: Sequence[_Book],
) -> _RoundTripStats:
    decision_by_order = {item.order_id: item for item in decisions if item.order_id is not None}
    lots: dict[str, deque[_OpenLot]] = {}
    positions: dict[str, Decimal] = {}
    samples: list[Decimal] = []
    fill_net: dict[str, Decimal] = {}
    by_instrument: dict[str, Decimal] = {}
    by_day: dict[str, Decimal] = {}
    by_event = {"PRIMARY_OFFSET": _ZERO, "FORCED_CLOSE": _ZERO}
    inventory_path: list[Decimal] = []
    closeout_slippage: list[Decimal] = []
    close_net: dict[str, Decimal] = {}
    close_notional: dict[str, Decimal] = {}

    for fill in ghost.fills:
        if not fill.forced:
            fill_net.setdefault(fill.order_id, _ZERO)
        instrument_lots = lots.setdefault(fill.instrument_id, deque())
        remaining = fill.quantity
        fee_per_quantity = fill.fee / fill.quantity
        while remaining > 0 and instrument_lots and instrument_lots[0].side is not fill.side:
            opened = instrument_lots[0]
            matched = min(remaining, opened.quantity)
            gross = opened.side.sign * (fill.price - opened.price) * matched
            net = gross - (opened.fee_per_quantity + fee_per_quantity) * matched
            samples.append(net)
            if fill.forced:
                fill_net[opened.order_id] = fill_net.get(opened.order_id, _ZERO) + net
            else:
                half = net / Decimal("2")
                fill_net[opened.order_id] = fill_net.get(opened.order_id, _ZERO) + half
                fill_net[fill.order_id] = fill_net.get(fill.order_id, _ZERO) + half
            by_instrument[fill.instrument_id] = by_instrument.get(fill.instrument_id, _ZERO) + net
            day = str(fill.timestamp_ns // _NS_PER_DAY)
            by_day[day] = by_day.get(day, _ZERO) + net
            event = "FORCED_CLOSE" if fill.forced else "PRIMARY_OFFSET"
            by_event[event] += net
            close_net[opened.order_id] = close_net.get(opened.order_id, _ZERO) + net
            close_notional[opened.order_id] = close_notional.get(
                opened.order_id, _ZERO
            ) + opened.price * matched
            if fill.forced:
                mid = _book_mid_at(books, fill.instrument_id, fill.timestamp_ns)
                if mid is not None:
                    closeout_slippage.append(abs(fill.price - mid) / mid * _BPS)
            opened.quantity -= matched
            remaining -= matched
            if opened.quantity == 0:
                instrument_lots.popleft()
        if remaining > 0:
            instrument_lots.append(
                _OpenLot(
                    order_id=fill.order_id,
                    side=fill.side,
                    quantity=remaining,
                    price=fill.price,
                    fee_per_quantity=fee_per_quantity,
                    timestamp_ns=fill.timestamp_ns,
                )
            )
        positions[fill.instrument_id] = positions.get(fill.instrument_id, _ZERO) + (
            fill.side.sign * fill.quantity
        )
        inventory_path.append(abs(positions[fill.instrument_id]) * fill.price)

    per_fill_samples = list(fill_net.values())
    top_share = None
    positive_total = sum((max(item, _ZERO) for item in per_fill_samples), _ZERO)
    if len(per_fill_samples) >= 100 and positive_total > 0:
        count = max(1, (len(per_fill_samples) + 99) // 100)
        top = sum(
            sorted((max(item, _ZERO) for item in per_fill_samples), reverse=True)[:count],
            _ZERO,
        )
        top_share = top / positive_total
    lcb = None
    if len(per_fill_samples) >= 2:
        count_decimal = Decimal(len(per_fill_samples))
        mean = sum(per_fill_samples, _ZERO) / count_decimal
        variance = sum(((item - mean) ** 2 for item in per_fill_samples), _ZERO) / Decimal(
            len(per_fill_samples) - 1
        )
        lcb = mean - Decimal("1.96") * (variance / count_decimal).sqrt()
    fill_to_close = {
        order_id: close_net[order_id] / notional * _BPS
        for order_id, notional in close_notional.items()
        if notional > 0 and order_id in decision_by_order
    }
    return _RoundTripStats(
        concentration=H1Concentration(
            by_instrument=dict(sorted(by_instrument.items())),
            by_utc_day=dict(sorted(by_day.items())),
            by_event=by_event,
            top_one_percent_share=top_share,
            inventory_notional_p99=_percentile_99(inventory_path),
            closeout_slippage_p99_bps=(
                _percentile_99(closeout_slippage)
                if closeout_slippage
                else (_ZERO if not ghost.exposure.unresolved_closeout else None)
            ),
            lcb95_net_per_fill=lcb,
            conservative_fills=sum(not item.forced for item in ghost.fills),
            completed_inventory_matches=len(samples),
        ),
        fill_to_close_bps=fill_to_close,
    )


def _gates(
    ghost: GhostReport,
    decisions: Sequence[H1Decision],
    concentration: H1Concentration,
    config: H1PolicyConfig,
    *,
    latency_ms: int,
    synthetic: bool,
) -> dict[str, bool]:
    fills = tuple(item for item in ghost.fills if not item.forced)
    markets = {item.instrument_id for item in fills}
    decision_by_order = {item.order_id: item for item in decisions if item.order_id is not None}
    regimes = {
        decision_by_order[item.order_id].regime
        for item in fills
        if item.order_id in decision_by_order
        and decision_by_order[item.order_id].regime in {"LOW", "MID", "HIGH"}
    }
    return {
        "closeout_resolved": not ghost.exposure.unresolved_closeout,
        "latency_is_primary_500ms": latency_ms == 500,
        "closeout_slippage_p99_acceptable": (
            concentration.closeout_slippage_p99_bps is not None
            and concentration.closeout_slippage_p99_bps
            <= config.closeout_slippage_p99_max
        ),
        "inventory_notional_p99_acceptable": (
            concentration.inventory_notional_p99 is not None
            and concentration.inventory_notional_p99
            <= config.inventory_notional_p99_max
        ),
        "lcb95_net_positive_500ms_zero_rebate": (
            latency_ms == 500
            and concentration.lcb95_net_per_fill is not None
            and concentration.lcb95_net_per_fill > 0
        ),
        "minimum_fills_5000": concentration.conservative_fills >= config.minimum_fills,
        "minimum_markets_3": len(markets) >= config.minimum_markets,
        "minimum_regimes_3": len(regimes) >= config.minimum_regimes,
        "not_synthetic": not synthetic,
        "reconciliation_exact": ghost.pnl.reconciliation_difference == 0,
        "top_one_percent_not_dominant": (
            concentration.top_one_percent_share is not None
            and concentration.top_one_percent_share <= config.top_one_percent_max
        ),
    }


def replay_h1_research_manifest(
    root: Path,
    manifest_sha256: str,
    *,
    config: H1PolicyConfig,
) -> H1StudyReport:
    reader = ResearchSegmentReader(root, manifest_sha256=manifest_sha256)
    envelopes = reader.replay()
    if not envelopes:
        raise ValueError("H1 replay requires a non-empty authenticated manifest")
    fixture_flags = {item.provenance.fixture_label is not None for item in envelopes}
    if len(fixture_flags) != 1:
        raise ValueError("H1 manifest cannot mix synthetic and public provenance")
    synthetic = next(iter(fixture_flags))
    if synthetic and any(
        item.provenance.fixture_label != SYNTHETIC_FIXTURE_LABEL for item in envelopes
    ):
        raise ValueError("H1 synthetic provenance is not visibly labelled")
    if not synthetic:
        allowed_public_provenance = {
            (HYPERLIQUID_PUBLIC_HTTP_URL, "PUBLIC_HTTP"),
            (HYPERLIQUID_PUBLIC_WEBSOCKET_URL, "PUBLIC_WEBSOCKET"),
        }
        if any(
            (item.provenance.source_url, item.provenance.transport)
            not in allowed_public_provenance
            or item.source_metadata_version != HYPERLIQUID_METADATA_VERSION
            or (
                item.feed_type == "metadata"
                and item.provenance.transport != "PUBLIC_HTTP"
            )
            for item in envelopes
        ):
            raise ValueError("H1 public provenance is not an official pinned Hyperliquid source")
    reports: list[H1LatencyReport] = []
    for latency_ms in config.latency_scenarios_ms:
        prepared = _prepare(envelopes, config, latency_ms=latency_ms)
        fixture = _fixture(prepared, config, latency_ms=latency_ms, synthetic=synthetic)
        ghost = GhostReplay(
            fixture,
            input_adapter_id="hyperliquid-h1-authenticated-research-v1",
            raw_manifest_sha256=reader.manifest.manifest_sha256,
            raw_root_sha256=reader.manifest.root_sha256,
            segment_sha256s=tuple(
                item.physical_sha256 for item in reader.manifest.segments
            ),
        ).run()
        decisions = _updated_decisions(prepared.decisions, ghost)
        round_trips = _round_trip_stats(ghost, decisions, prepared.books)
        decisions = tuple(
            replace(
                decision,
                fill_to_close_markout_bps=round_trips.fill_to_close_bps.get(
                    decision.order_id
                ),
            )
            if decision.order_id is not None
            else decision
            for decision in decisions
        )
        attribution = _attribution(ghost, decisions, prepared.books)
        concentration = round_trips.concentration
        gates = _gates(
            ghost,
            decisions,
            concentration,
            config,
            latency_ms=latency_ms,
            synthetic=synthetic,
        )
        reports.append(
            H1LatencyReport(
                latency_ms=latency_ms,
                role=_role(latency_ms),
                promotable_alone=False,
                decisions=decisions,
                ghost=ghost,
                attribution=attribution,
                concentration=concentration,
                economic_gates=gates,
                limitations=(
                    "PRIMARY_REBATE_ZERO",
                    "NO_FINALIZED_FUNDING_SETTLEMENT_IN_H1_FEEDS",
                    "100MS_BOUNDARY_NEVER_SUFFICIENT",
                    "VARIANTS_REGISTERED_BEFORE_HOLDOUT_AND_NOT_RETROACTIVELY_SELECTED",
                    "SMALL_OR_SYNTHETIC_PREFIX_IS_TECHNICAL_ONLY",
                ),
            )
        )
    return H1StudyReport(
        policy_config_sha256=config.config_sha256,
        raw_manifest_sha256=reader.manifest.manifest_sha256,
        raw_root_sha256=reader.manifest.root_sha256,
        segment_sha256s=tuple(item.physical_sha256 for item in reader.manifest.segments),
        synthetic=synthetic,
        latency_reports=tuple(reports),
        variants=config.variants,
    )


__all__ = [
    "ECONOMIC_NOT_AVAILABLE",
    "H1_POLICY_VERSION",
    "H1_READY",
    "H1Attribution",
    "H1Concentration",
    "H1Decision",
    "H1LatencyReport",
    "H1Markout",
    "H1PolicyConfig",
    "H1StudyReport",
    "H1Variant",
    "replay_h1_research_manifest",
]
