from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum

from .canonical import CanonicalValue, canonical_json_bytes, canonical_value
from .envelope import Venue

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class RelationType(StrEnum):
    EQUIVALENT = "EQUIVALENT"
    MUTUALLY_EXCLUSIVE = "MUTUALLY_EXCLUSIVE"
    EXHAUSTIVE = "EXHAUSTIVE"
    PARITY = "PARITY"
    NESTED_THRESHOLD = "NESTED_THRESHOLD"
    RANGE = "RANGE"
    DIFFERENT_EXPIRY = "DIFFERENT_EXPIRY"
    CONDITIONAL_IMPLICATION = "CONDITIONAL_IMPLICATION"
    WORDING_DUPLICATE = "WORDING_DUPLICATE"


class RelationStatus(StrEnum):
    VERIFIED = "VERIFIED"
    UNVERIFIED = "UNVERIFIED"


class K4Status(StrEnum):
    CANDIDATE = "CANDIDATE"
    NO_OPPORTUNITY = "NO_OPPORTUNITY"
    REFUSED_UNVERIFIED = "REFUSED_UNVERIFIED"


@dataclass(frozen=True, slots=True)
class OutcomeIdentity:
    venue: Venue
    economic_market_id: str
    outcome_id: str
    outcome_label: str

    def __post_init__(self) -> None:
        if not self.economic_market_id or not self.outcome_id or not self.outcome_label:
            raise ValueError("outcome economic identity is incomplete")

    def to_dict(self) -> dict[str, CanonicalValue]:
        return {
            "economic_market_id": self.economic_market_id,
            "outcome_id": self.outcome_id,
            "outcome_label": self.outcome_label,
            "venue": self.venue.value,
        }


@dataclass(frozen=True, slots=True)
class MarketRuleVersion:
    venue: Venue
    economic_market_id: str
    version_id: str
    rule_text: str
    resolution_source: str
    opens_at: str | None
    closes_at: str | None
    resolves_at: str | None
    market_status: str
    outcomes: tuple[OutcomeIdentity, ...]
    source_metadata_version: str
    raw_content_sha256: str

    def __post_init__(self) -> None:
        if _SHA256.fullmatch(self.version_id) is None:
            raise ValueError("market rule version id must be a SHA-256")
        if _SHA256.fullmatch(self.raw_content_sha256) is None:
            raise ValueError("market rule raw identity must be a SHA-256")
        if not self.source_metadata_version or not self.market_status:
            raise ValueError("market rule source version and status are required")
        if any(
            outcome.venue is not self.venue
            or outcome.economic_market_id != self.economic_market_id
            for outcome in self.outcomes
        ):
            raise ValueError("market rule outcomes must belong to the versioned market")
        core = {
            "closes_at": self.closes_at,
            "economic_market_id": self.economic_market_id,
            "market_status": self.market_status,
            "opens_at": self.opens_at,
            "outcomes": [item.to_dict() for item in self.outcomes],
            "raw_content_sha256": self.raw_content_sha256,
            "resolution_source": self.resolution_source,
            "resolves_at": self.resolves_at,
            "rule_text": self.rule_text,
            "source_metadata_version": self.source_metadata_version,
            "venue": self.venue.value,
        }
        if hashlib.sha256(canonical_json_bytes(core)).hexdigest() != self.version_id:
            raise ValueError("market rule version id does not bind its content")

    @classmethod
    def create(
        cls,
        *,
        venue: Venue,
        economic_market_id: str,
        rule_text: str,
        resolution_source: str,
        opens_at: str | None,
        closes_at: str | None,
        resolves_at: str | None,
        market_status: str,
        outcomes: Sequence[OutcomeIdentity],
        source_metadata_version: str,
        raw_content_sha256: str,
    ) -> MarketRuleVersion:
        if not economic_market_id or not rule_text or not resolution_source:
            raise ValueError("market rule identity, text, and resolution source are required")
        if not outcomes:
            raise ValueError("market rule version requires outcomes")
        outcome_tuple = tuple(
            sorted(outcomes, key=lambda item: (item.outcome_id, item.outcome_label))
        )
        if len({item.outcome_id for item in outcome_tuple}) != len(outcome_tuple):
            raise ValueError("market rule outcome identities must be unique")
        core = {
            "closes_at": closes_at,
            "economic_market_id": economic_market_id,
            "market_status": market_status,
            "opens_at": opens_at,
            "outcomes": [item.to_dict() for item in outcome_tuple],
            "raw_content_sha256": raw_content_sha256,
            "resolution_source": resolution_source,
            "resolves_at": resolves_at,
            "rule_text": rule_text,
            "source_metadata_version": source_metadata_version,
            "venue": venue.value,
        }
        version_id = hashlib.sha256(canonical_json_bytes(core)).hexdigest()
        return cls(
            venue=venue,
            economic_market_id=economic_market_id,
            version_id=version_id,
            rule_text=rule_text,
            resolution_source=resolution_source,
            opens_at=opens_at,
            closes_at=closes_at,
            resolves_at=resolves_at,
            market_status=market_status,
            outcomes=outcome_tuple,
            source_metadata_version=source_metadata_version,
            raw_content_sha256=raw_content_sha256,
        )


@dataclass(frozen=True, slots=True)
class SemanticRelation:
    relation_id: str
    relation_type: RelationType
    members: tuple[OutcomeIdentity, ...]
    formal_rule: Mapping[str, CanonicalValue]
    provenance: tuple[str, ...]
    version: int
    confidence: Decimal
    status: RelationStatus
    human_justification: str
    machine_justification: Mapping[str, CanonicalValue]

    def __post_init__(self) -> None:
        if not self.relation_id.startswith("REL:") or len(self.relation_id) != 68:
            raise ValueError("relation id must be a deterministic REL: SHA-256")
        if len(self.members) < 2 or self.version <= 0:
            raise ValueError("semantic relation needs at least two members and a version")
        member_keys = {(item.venue, item.economic_market_id, item.outcome_id) for item in self.members}
        if len(member_keys) != len(self.members):
            raise ValueError("semantic relation members must be unique")
        if not self.provenance or not self.human_justification:
            raise ValueError("semantic relation provenance and justification are required")
        if not self.confidence.is_finite() or self.confidence < 0 or self.confidence > 1:
            raise ValueError("semantic relation confidence must be within [0, 1]")
        canonical_json_bytes(self.formal_rule)
        canonical_json_bytes(self.machine_justification)
        expected = self.deterministic_id(
            relation_type=self.relation_type,
            members=self.members,
            formal_rule=self.formal_rule,
            provenance=self.provenance,
            version=self.version,
            status=self.status,
            confidence=self.confidence,
            human_justification=self.human_justification,
            machine_justification=self.machine_justification,
        )
        if expected != self.relation_id:
            raise ValueError("relation id does not bind the semantic relation content")

    @staticmethod
    def deterministic_id(
        *,
        relation_type: RelationType,
        members: Sequence[OutcomeIdentity],
        formal_rule: Mapping[str, CanonicalValue],
        provenance: Sequence[str],
        version: int,
        status: RelationStatus,
        confidence: Decimal,
        human_justification: str,
        machine_justification: Mapping[str, CanonicalValue],
    ) -> str:
        body = {
            "confidence": format(confidence, "f"),
            "formal_rule": formal_rule,
            "human_justification": human_justification,
            "machine_justification": machine_justification,
            "members": [item.to_dict() for item in members],
            "provenance": list(provenance),
            "relation_type": relation_type.value,
            "status": status.value,
            "version": version,
        }
        return f"REL:{hashlib.sha256(canonical_json_bytes(body)).hexdigest()}"

    @classmethod
    def create(
        cls,
        *,
        relation_type: RelationType,
        members: Sequence[OutcomeIdentity],
        formal_rule: Mapping[str, object],
        provenance: Sequence[str],
        version: int,
        confidence: Decimal,
        status: RelationStatus,
        human_justification: str,
        machine_justification: Mapping[str, object],
    ) -> SemanticRelation:
        canonical_rule = canonical_value(formal_rule)
        canonical_machine = canonical_value(machine_justification)
        if not isinstance(canonical_rule, dict) or not isinstance(canonical_machine, dict):
            raise ValueError("relation rules and machine justification must be objects")
        member_tuple = tuple(
            sorted(
                members,
                key=lambda item: (
                    item.venue.value,
                    item.economic_market_id,
                    item.outcome_id,
                ),
            )
        )
        provenance_tuple = tuple(sorted(set(provenance)))
        relation_id = cls.deterministic_id(
            relation_type=relation_type,
            members=member_tuple,
            formal_rule=canonical_rule,
            provenance=provenance_tuple,
            version=version,
            status=status,
            confidence=confidence,
            human_justification=human_justification,
            machine_justification=canonical_machine,
        )
        return cls(
            relation_id=relation_id,
            relation_type=relation_type,
            members=member_tuple,
            formal_rule=canonical_rule,
            provenance=provenance_tuple,
            version=version,
            confidence=confidence,
            status=status,
            human_justification=human_justification,
            machine_justification=canonical_machine,
        )


@dataclass(frozen=True, slots=True)
class SemanticCatalog:
    relations: tuple[SemanticRelation, ...]

    @classmethod
    def build(cls, relations: Sequence[SemanticRelation]) -> SemanticCatalog:
        ordered = tuple(sorted(relations, key=lambda item: item.relation_id))
        if len({item.relation_id for item in ordered}) != len(ordered):
            raise ValueError("semantic catalog relation ids must be unique")
        return cls(ordered)

    @property
    def catalog_sha256(self) -> str:
        payload = [
            {
                "confidence": format(item.confidence, "f"),
                "formal_rule": item.formal_rule,
                "human_justification": item.human_justification,
                "machine_justification": item.machine_justification,
                "members": [member.to_dict() for member in item.members],
                "provenance": list(item.provenance),
                "relation_id": item.relation_id,
                "relation_type": item.relation_type.value,
                "status": item.status.value,
                "version": item.version,
            }
            for item in self.relations
        ]
        return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


@dataclass(frozen=True, slots=True)
class PredictionBookSnapshot:
    venue: Venue
    economic_market_id: str
    outcome_id: str
    point_in_time_id: str
    observed_at_ns: int
    yes_bid: Decimal | None
    yes_bid_size: Decimal | None
    yes_ask: Decimal | None
    yes_ask_size: Decimal | None
    no_bid: Decimal | None
    no_bid_size: Decimal | None
    no_ask: Decimal | None
    no_ask_size: Decimal | None
    conservative_fee_per_contract: Decimal
    rule_version_id: str
    closes_at_ns: int
    raw_segment_sha256: str

    def __post_init__(self) -> None:
        if not all(
            (
                self.economic_market_id,
                self.outcome_id,
                self.point_in_time_id,
                self.rule_version_id,
                self.raw_segment_sha256,
            )
        ):
            raise ValueError("prediction book identity/provenance is incomplete")
        if _SHA256.fullmatch(self.rule_version_id) is None:
            raise ValueError("prediction book rule version must be a SHA-256")
        if (
            type(self.observed_at_ns) is not int
            or type(self.closes_at_ns) is not int
            or self.observed_at_ns < 0
            or self.closes_at_ns <= self.observed_at_ns
        ):
            raise ValueError("prediction book observation/close times are invalid")
        decimal_values = (
            self.yes_bid,
            self.yes_bid_size,
            self.yes_ask,
            self.yes_ask_size,
            self.no_bid,
            self.no_bid_size,
            self.no_ask,
            self.no_ask_size,
            self.conservative_fee_per_contract,
        )
        if any(value is not None and not value.is_finite() for value in decimal_values):
            raise ValueError("prediction book decimals must be finite")
        if self.conservative_fee_per_contract < 0:
            raise ValueError("conservative fee cannot be negative")
        if _SHA256.fullmatch(self.raw_segment_sha256) is None:
            raise ValueError("prediction book raw segment identity must be a SHA-256")
        pairs = (
            (self.yes_bid, self.yes_bid_size),
            (self.yes_ask, self.yes_ask_size),
            (self.no_bid, self.no_bid_size),
            (self.no_ask, self.no_ask_size),
        )
        for price, quantity in pairs:
            if (price is None) != (quantity is None):
                raise ValueError("book price and quantity must be present together")
            if price is not None and (price < 0 or price > 1 or quantity is None or quantity < 0):
                raise ValueError("prediction book price or quantity is invalid")
        if self.yes_bid is not None and self.yes_ask is not None and self.yes_bid > self.yes_ask:
            raise ValueError("prediction YES book is crossed")
        if self.no_bid is not None and self.no_ask is not None and self.no_bid > self.no_ask:
            raise ValueError("prediction NO book is crossed")


@dataclass(frozen=True, slots=True)
class IncentiveLedgerEntry:
    venue: Venue
    program_id: str
    market_id: str
    period_start: str
    period_end: str
    target_size: Decimal
    discount_factor: Decimal
    program_version: str
    hypothetical_reward: Decimal
    realizable_reward: Decimal | None = None

    def __post_init__(self) -> None:
        if self.venue is not Venue.KALSHI:
            raise ValueError("the initial incentive ledger is Kalshi-specific")
        if not all(
            (
                self.program_id,
                self.market_id,
                self.period_start,
                self.period_end,
                self.program_version,
            )
        ):
            raise ValueError("incentive program provenance is incomplete")
        values = (self.target_size, self.discount_factor, self.hypothetical_reward)
        if any(not value.is_finite() for value in values) or min(values) < 0:
            raise ValueError("incentive ledger values cannot be negative")
        if self.realizable_reward is not None:
            raise ValueError("realizable reward must remain unpresumed in Research Data Plane V1")

    @property
    def primary_economics_reward(self) -> Decimal:
        return Decimal("0")


@dataclass(frozen=True, slots=True)
class K4Leg:
    venue: Venue
    economic_market_id: str
    outcome_id: str
    action: str
    observed_bid: Decimal | None
    observed_ask: Decimal
    executable_quantity: Decimal
    fee_per_contract: Decimal
    rule_version_id: str
    raw_segment_sha256: str


@dataclass(frozen=True, slots=True)
class K4ScanResult:
    status: K4Status
    relation_id: str
    legs: tuple[K4Leg, ...]
    executable_quantity: Decimal
    observed_point_in_time_id: str | None
    leg_sequencing: tuple[str, ...]
    non_fill_risk: str
    observed_cost: Decimal
    observed_spread_cost: Decimal
    fees: Decimal
    conservative_slippage: Decimal
    capital_immobilized: Decimal
    guaranteed_payout: Decimal
    gross_edge: Decimal
    conservative_net_edge: Decimal
    gross_edge_rate: Decimal
    conservative_net_edge_rate: Decimal
    resolution_rule: Mapping[str, CanonicalValue]
    time_remaining_ns: int
    rewards_in_primary_economics: Decimal
    reasons: tuple[str, ...]


class K4Scanner:
    """Offline logical-relative-value scanner; it has no order or transport surface."""

    def __init__(
        self,
        *,
        conservative_slippage_bps: Decimal,
        minimum_net_edge: Decimal = Decimal("0"),
    ) -> None:
        if (
            not conservative_slippage_bps.is_finite()
            or not minimum_net_edge.is_finite()
            or conservative_slippage_bps < 0
            or minimum_net_edge < 0
        ):
            raise ValueError("K4 slippage and minimum edge cannot be negative")
        self.conservative_slippage_bps = conservative_slippage_bps
        self.minimum_net_edge = minimum_net_edge

    def scan(
        self,
        relation: SemanticRelation,
        snapshots: Sequence[PredictionBookSnapshot],
        *,
        observed_at_ns: int,
    ) -> K4ScanResult:
        if observed_at_ns < 0:
            raise ValueError("K4 observation time cannot be negative")
        base = self._empty_result(relation, observed_at_ns=observed_at_ns)
        if relation.status is RelationStatus.UNVERIFIED:
            return self._replace_reasons(
                base,
                status=K4Status.REFUSED_UNVERIFIED,
                reasons=("RELATION_UNVERIFIED_NOT_PROMOTABLE",),
            )
        contract = relation.formal_rule.get("scanner_contract")
        if contract != "BUY_COMPLETE_SET_V1":
            return self._replace_reasons(
                base,
                status=K4Status.NO_OPPORTUNITY,
                reasons=("UNSUPPORTED_FORMAL_RULE",),
            )
        raw_legs = relation.formal_rule.get("legs")
        payout_raw = relation.formal_rule.get("guaranteed_payout")
        rule_versions = relation.formal_rule.get("resolution_rule_versions")
        if (
            not isinstance(raw_legs, list)
            or not raw_legs
            or not isinstance(rule_versions, dict)
            or relation.formal_rule.get("resolution_unambiguous") is not True
        ):
            return self._replace_reasons(
                base,
                status=K4Status.NO_OPPORTUNITY,
                reasons=("FORMAL_RULE_INCOMPLETE_OR_AMBIGUOUS",),
            )
        try:
            payout = Decimal(str(payout_raw))
        except (InvalidOperation, ValueError):
            return self._replace_reasons(
                base,
                status=K4Status.NO_OPPORTUNITY,
                reasons=("GUARANTEED_PAYOUT_INVALID",),
            )
        if not payout.is_finite() or payout <= 0:
            return self._replace_reasons(
                base,
                status=K4Status.NO_OPPORTUNITY,
                reasons=("GUARANTEED_PAYOUT_INVALID",),
            )
        by_key = {(item.economic_market_id, item.outcome_id): item for item in snapshots}
        if len(by_key) != len(snapshots):
            return self._replace_reasons(
                base,
                status=K4Status.NO_OPPORTUNITY,
                reasons=("DUPLICATE_OR_AMBIGUOUS_BOOK_SNAPSHOT",),
            )
        relation_members = {
            (item.economic_market_id, item.outcome_id): item.venue
            for item in relation.members
        }
        if len(relation_members) != len(relation.members):
            return self._replace_reasons(
                base,
                status=K4Status.NO_OPPORTUNITY,
                reasons=("AMBIGUOUS_RELATION_MEMBERS",),
            )
        legs: list[K4Leg] = []
        selected_snapshots: list[PredictionBookSnapshot] = []
        close_times: list[int] = []
        missing: list[str] = []
        for raw_leg in raw_legs:
            if not isinstance(raw_leg, dict):
                missing.append("FORMAL_LEG_INVALID")
                continue
            market_id = str(raw_leg.get("market_id") or "")
            outcome_id = str(raw_leg.get("outcome_id") or "")
            side = str(raw_leg.get("side") or "")
            snapshot = by_key.get((market_id, outcome_id))
            if snapshot is None:
                missing.append(f"MISSING_BOOK:{market_id}:{outcome_id}")
                continue
            expected_venue = relation_members.get((market_id, outcome_id))
            if expected_venue is None or expected_venue is not snapshot.venue:
                missing.append(f"FORMAL_LEG_NOT_RELATION_MEMBER:{market_id}:{outcome_id}")
                continue
            if snapshot.observed_at_ns != observed_at_ns:
                missing.append(f"OBSERVATION_TIME_MISMATCH:{market_id}:{outcome_id}")
                continue
            expected_rule = rule_versions.get(f"{market_id}:{outcome_id}")
            if expected_rule != snapshot.rule_version_id:
                missing.append(f"RULE_VERSION_MISMATCH:{market_id}:{outcome_id}")
                continue
            if side == "YES":
                bid, ask, size = snapshot.yes_bid, snapshot.yes_ask, snapshot.yes_ask_size
                action = "BUY_YES"
            elif side == "NO":
                bid, ask, size = snapshot.no_bid, snapshot.no_ask, snapshot.no_ask_size
                action = "BUY_NO"
            else:
                missing.append(f"UNSUPPORTED_LEG_SIDE:{market_id}:{outcome_id}")
                continue
            if bid is None:
                missing.append(f"NO_OBSERVED_BID:{market_id}:{outcome_id}")
                continue
            if ask is None or size is None or size <= 0:
                missing.append(f"NO_EXECUTABLE_ASK:{market_id}:{outcome_id}")
                continue
            legs.append(
                K4Leg(
                    venue=snapshot.venue,
                    economic_market_id=market_id,
                    outcome_id=outcome_id,
                    action=action,
                    observed_bid=bid,
                    observed_ask=ask,
                    executable_quantity=size,
                    fee_per_contract=snapshot.conservative_fee_per_contract,
                    rule_version_id=snapshot.rule_version_id,
                    raw_segment_sha256=snapshot.raw_segment_sha256,
                )
            )
            selected_snapshots.append(snapshot)
            close_times.append(snapshot.closes_at_ns)
        if missing or len(legs) != len(raw_legs):
            return self._replace_reasons(
                base,
                status=K4Status.NO_OPPORTUNITY,
                reasons=tuple(missing or ("NO_EXECUTABLE_LEGS",)),
            )
        point_ids = {item.point_in_time_id for item in selected_snapshots}
        if len(point_ids) != 1:
            return self._replace_reasons(
                base,
                status=K4Status.NO_OPPORTUNITY,
                reasons=("SNAPSHOTS_NOT_POINT_IN_TIME",),
            )
        if min(close_times) <= observed_at_ns:
            return self._replace_reasons(
                base,
                status=K4Status.NO_OPPORTUNITY,
                reasons=("MARKET_CLOSED_OR_EXPIRED",),
            )
        quantity = min(item.executable_quantity for item in legs)
        observed_cost = sum((item.observed_ask * quantity for item in legs), Decimal("0"))
        spread_cost = sum(
            (
                (item.observed_ask - item.observed_bid) * quantity
                for item in legs
                if item.observed_bid is not None
            ),
            Decimal("0"),
        )
        fees = sum((item.fee_per_contract * quantity for item in legs), Decimal("0"))
        slippage = observed_cost * self.conservative_slippage_bps / Decimal("10000")
        payout_amount = payout * quantity
        gross_edge = payout_amount - observed_cost
        net_edge = gross_edge - fees - slippage
        capital = observed_cost + fees + slippage
        gross_rate = Decimal("0") if capital == 0 else gross_edge / capital
        net_rate = Decimal("0") if capital == 0 else net_edge / capital
        ordered = tuple(
            f"{item.economic_market_id}:{item.outcome_id}:{item.action}"
            for item in sorted(
                legs,
                key=lambda item: (
                    item.executable_quantity,
                    item.economic_market_id,
                    item.outcome_id,
                ),
            )
        )
        reasons: tuple[str, ...]
        status: K4Status
        if quantity <= 0:
            status = K4Status.NO_OPPORTUNITY
            reasons = ("WORST_LEG_HAS_ZERO_QUANTITY",)
        elif net_edge <= self.minimum_net_edge:
            status = K4Status.NO_OPPORTUNITY
            reasons = ("CONSERVATIVE_NET_EDGE_NOT_POSITIVE",)
        else:
            status = K4Status.CANDIDATE
            reasons = ()
        return K4ScanResult(
            status=status,
            relation_id=relation.relation_id,
            legs=tuple(legs),
            executable_quantity=quantity,
            observed_point_in_time_id=next(iter(point_ids)),
            leg_sequencing=ordered,
            non_fill_risk="SEQUENTIAL_LEG_RISK_NOT_ASSUMED_SIMULTANEOUS",
            observed_cost=observed_cost,
            observed_spread_cost=spread_cost,
            fees=fees,
            conservative_slippage=slippage,
            capital_immobilized=capital,
            guaranteed_payout=payout_amount,
            gross_edge=gross_edge,
            conservative_net_edge=net_edge,
            gross_edge_rate=gross_rate,
            conservative_net_edge_rate=net_rate,
            resolution_rule=relation.formal_rule,
            time_remaining_ns=max(min(close_times) - observed_at_ns, 0),
            rewards_in_primary_economics=Decimal("0"),
            reasons=reasons,
        )

    @staticmethod
    def _empty_result(
        relation: SemanticRelation, *, observed_at_ns: int
    ) -> K4ScanResult:
        return K4ScanResult(
            status=K4Status.NO_OPPORTUNITY,
            relation_id=relation.relation_id,
            legs=(),
            executable_quantity=Decimal("0"),
            observed_point_in_time_id=None,
            leg_sequencing=(),
            non_fill_risk="SEQUENTIAL_LEG_RISK_NOT_ASSUMED_SIMULTANEOUS",
            observed_cost=Decimal("0"),
            observed_spread_cost=Decimal("0"),
            fees=Decimal("0"),
            conservative_slippage=Decimal("0"),
            capital_immobilized=Decimal("0"),
            guaranteed_payout=Decimal("0"),
            gross_edge=Decimal("0"),
            conservative_net_edge=Decimal("0"),
            gross_edge_rate=Decimal("0"),
            conservative_net_edge_rate=Decimal("0"),
            resolution_rule=relation.formal_rule,
            time_remaining_ns=max(0, -observed_at_ns),
            rewards_in_primary_economics=Decimal("0"),
            reasons=(),
        )

    @staticmethod
    def _replace_reasons(
        result: K4ScanResult,
        *,
        status: K4Status,
        reasons: tuple[str, ...],
    ) -> K4ScanResult:
        return K4ScanResult(
            status=status,
            relation_id=result.relation_id,
            legs=result.legs,
            executable_quantity=result.executable_quantity,
            observed_point_in_time_id=result.observed_point_in_time_id,
            leg_sequencing=result.leg_sequencing,
            non_fill_risk=result.non_fill_risk,
            observed_cost=result.observed_cost,
            observed_spread_cost=result.observed_spread_cost,
            fees=result.fees,
            conservative_slippage=result.conservative_slippage,
            capital_immobilized=result.capital_immobilized,
            guaranteed_payout=result.guaranteed_payout,
            gross_edge=result.gross_edge,
            conservative_net_edge=result.conservative_net_edge,
            gross_edge_rate=result.gross_edge_rate,
            conservative_net_edge_rate=result.conservative_net_edge_rate,
            resolution_rule=result.resolution_rule,
            time_remaining_ns=result.time_remaining_ns,
            rewards_in_primary_economics=result.rewards_in_primary_economics,
            reasons=reasons,
        )


__all__ = [
    "IncentiveLedgerEntry",
    "K4Leg",
    "K4ScanResult",
    "K4Scanner",
    "K4Status",
    "MarketRuleVersion",
    "OutcomeIdentity",
    "PredictionBookSnapshot",
    "RelationStatus",
    "RelationType",
    "SemanticCatalog",
    "SemanticRelation",
]
