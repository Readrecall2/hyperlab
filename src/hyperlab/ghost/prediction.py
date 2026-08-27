from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from hyperlab.research_data.adapters import KALSHI_METADATA_VERSION, POLYMARKET_METADATA_VERSION
from hyperlab.research_data.canonical import (
    canonical_json_bytes,
    decode_canonical_json,
)
from hyperlab.research_data.derived import DerivedDatasetIdentity
from hyperlab.research_data.envelope import (
    SYNTHETIC_FIXTURE_LABEL,
    CaptureProvenance,
    SessionEnvelopeFactory,
    Venue,
)
from hyperlab.research_data.prediction import (
    K4Scanner,
    K4Status,
    PredictionBookSnapshot,
    RelationStatus,
    RelationType,
    SemanticCatalog,
    SemanticRelation,
)
from hyperlab.research_data.prediction_candidate import (
    BOUNDARY,
    GHOST_MODEL_VERSION,
    CandidatePreregistration,
    CandidateVariant,
    FeeModel,
    PredictionCollectionBinding,
    PredictionDepthLevel,
    PredictionDepthSnapshot,
    PredictionFeeSchedule,
    PredictionPointInTimeDataset,
    PredictionReplaySeal,
    PredictionTickBand,
    PredictionTickGrid,
    revalidate_prediction_dataset,
    revalidate_prediction_fee_schedule,
)
from hyperlab.research_data.prediction_contracts import (
    EvidenceClassification,
    OfficialPublicContract,
    PredictionIdentityGraph,
    revalidate_prediction_graph,
)
from hyperlab.research_data.prediction_evidence import (
    PredictionRawEvidenceIndex,
    PredictionRawRecordRef,
    prediction_raw_record_ref,
    prediction_raw_records,
)
from hyperlab.research_data.prediction_time import prediction_rfc3339_to_ns
from hyperlab.research_data.segments import ResearchSegmentReader, ResearchSegmentWriter

_ZERO = Decimal("0")
_ONE = Decimal("1")
_REPLAY_VERIFICATION_TOKEN = object()
_GROUP_EXECUTION_TOKEN = object()
_OPPORTUNITY_EXECUTION_TOKEN = object()
_OPPORTUNITY_VERIFICATION_TOKEN = object()
_GROUP_REPORT_VERIFICATION_TOKEN = object()
_CAMPAIGN_RUNNER_TOKEN = object()
_CAMPAIGN_REPORT_VERIFICATION_TOKEN = object()


def _prediction_record_source_time_ns(
    envelope: Any,
    record: Mapping[str, Any],
) -> int | None:
    if (
        envelope.venue is Venue.POLYMARKET
        and envelope.feed_type == "metadata"
        and record.get("updatedAt") is not None
    ):
        return prediction_rfc3339_to_ns(
            record.get("updatedAt"),
            label="Polymarket Gamma updatedAt",
        )
    if (
        envelope.venue is Venue.POLYMARKET
        and record.get("timestamp") is not None
        and (
            envelope.feed_type
            in {"last_trade_price", "order_book", "price_change"}
            or record.get("event_type")
            in {"book", "last_trade_price", "price_change"}
        )
    ):
        try:
            milliseconds = int(str(record.get("timestamp")))
        except ValueError as error:
            raise ValueError("Polymarket record timestamp is not an integer") from error
        if milliseconds < 0:
            raise ValueError("Polymarket record timestamp cannot be negative")
        return milliseconds * 1_000_000
    if envelope.venue is Venue.KALSHI and record.get("settlement_ts") is not None:
        try:
            return prediction_rfc3339_to_ns(
                record.get("settlement_ts"),
                label="Kalshi settlement_ts",
            )
        except ValueError as error:
            raise ValueError("Kalshi settlement_ts is not RFC3339") from error
    return envelope.source_timestamp_ns


def _expected_settlement_source_time_ns(
    envelope: Any,
    record: Mapping[str, Any],
    state: PredictionSettlementState,
) -> int | None:
    if envelope.venue is Venue.KALSHI and state not in _TERMINAL_STATES:
        try:
            return _prediction_record_source_time_ns(envelope, record)
        except ValueError:
            return None
    return _prediction_record_source_time_ns(envelope, record)


class PredictionExecutionRole(StrEnum):
    MAKER = "MAKER"
    TAKER = "TAKER"


@dataclass(frozen=True, slots=True)
class PredictionVariantExecutionPolicy:
    variant_id: str
    family_id: str
    parameters_sha256: str
    latency_ns: int
    minimum_net_edge: Decimal
    slippage_bps: Decimal
    pessimistic_queue: bool

    @classmethod
    def from_variant(cls, variant: CandidateVariant) -> PredictionVariantExecutionPolicy:
        parameters = variant.parameters
        latency_ms = parameters.get("latency_ms")
        if type(latency_ms) is not int or latency_ms <= 0:
            raise ValueError("prediction variant latency_ms must be a positive integer")
        minimum_raw = parameters.get("minimum_net_edge", "0")
        slippage_raw = parameters.get("slippage_bps", "0")
        minimum = Decimal(str(minimum_raw))
        slippage = Decimal(str(slippage_raw))
        if not minimum.is_finite() or not slippage.is_finite() or min(minimum, slippage) < 0:
            raise ValueError("prediction variant edge/slippage parameters are invalid")
        queue = str(parameters.get("queue", "PESSIMISTIC"))
        if queue != "PESSIMISTIC":
            raise ValueError("prediction candidate v1 requires pessimistic queue")
        return cls(
            variant_id=variant.variant_id,
            family_id=variant.family_id,
            parameters_sha256=variant.parameters_sha256,
            latency_ns=latency_ms * 1_000_000,
            minimum_net_edge=minimum,
            slippage_bps=slippage,
            pessimistic_queue=True,
        )


class PredictionSettlementState(StrEnum):
    TRADING = "TRADING"
    CLOSED_UNRESOLVED = "CLOSED_UNRESOLVED"
    DETERMINED = "DETERMINED"
    DISPUTED = "DISPUTED"
    AMENDED = "AMENDED"
    RESOLVED_BINARY = "RESOLVED_BINARY"
    RESOLVED_50_50 = "RESOLVED_50_50"
    VOID = "VOID"
    CANCELLED = "CANCELLED"
    FINALIZED = "FINALIZED"
    SETTLED = "SETTLED"


_TERMINAL_STATES = {
    PredictionSettlementState.RESOLVED_BINARY,
    PredictionSettlementState.RESOLVED_50_50,
    PredictionSettlementState.VOID,
    PredictionSettlementState.CANCELLED,
    PredictionSettlementState.FINALIZED,
    PredictionSettlementState.SETTLED,
}


@dataclass(frozen=True, slots=True)
class PredictionSettlementEvidence:
    venue: Venue
    market_id: str
    outcome_id: str
    state: PredictionSettlementState
    source_event_time_ns: int | None
    received_time_utc_ns: int
    received_monotonic_ns: int
    payout_per_contract: Decimal | None
    rule_version_id: str
    resolution_rule_version_id: str
    source_event_id: str
    raw_manifest_sha256: str
    raw_root_sha256: str
    raw_ref: PredictionRawRecordRef
    collector_identity: str
    session_identity: str
    source_url: str
    classification: EvidenceClassification
    synthetic_fixture: bool = False

    def __post_init__(self) -> None:
        if (
            not self.market_id
            or not self.outcome_id
            or not self.rule_version_id
            or not self.resolution_rule_version_id
            or not self.source_event_id
            or not self.collector_identity
            or not self.session_identity
            or not self.source_url
            or len(self.raw_manifest_sha256) != 64
            or len(self.raw_root_sha256) != 64
            or min(self.received_time_utc_ns, self.received_monotonic_ns) < 0
        ):
            raise ValueError("prediction settlement evidence identity is incomplete")
        if self.source_event_time_ns is not None and self.source_event_time_ns < 0:
            raise ValueError("prediction settlement source time is invalid")
        terminal = self.state in _TERMINAL_STATES
        if terminal != (self.payout_per_contract is not None):
            raise ValueError("only terminal prediction settlement can carry a payout")
        if self.payout_per_contract is not None and (
            not self.payout_per_contract.is_finite()
            or self.payout_per_contract < 0
            or self.payout_per_contract > 1
        ):
            raise ValueError("prediction payout must be within [0,1]")
        if (
            terminal
            and self.classification is not EvidenceClassification.OBSERVED_PUBLICLY
            and not self.synthetic_fixture
        ):
            raise ValueError("terminal prediction payout requires observed public evidence")
        if self.state is PredictionSettlementState.RESOLVED_50_50:
            if self.venue is not Venue.POLYMARKET or self.payout_per_contract != Decimal("0.5"):
                raise ValueError("50/50 settlement is Polymarket-only with payout 0.5")
        elif self.state in {
            PredictionSettlementState.RESOLVED_BINARY,
            PredictionSettlementState.FINALIZED,
            PredictionSettlementState.SETTLED,
        } and self.payout_per_contract not in {_ZERO, _ONE}:
            raise ValueError("binary prediction settlement payout must be zero or one")
        if (
            self.state
            in {
                PredictionSettlementState.VOID,
                PredictionSettlementState.CANCELLED,
            }
            and not self.synthetic_fixture
        ):
            raise ValueError("public void/cancelled payout is unsupported fail-closed")


@dataclass(frozen=True, slots=True)
class PredictionOrderIntent:
    order_id: str
    opportunity_id: str
    variant_id: str
    candidate_config_sha256: str
    campaign_manifest_sha256: str | None
    collection_probe_binding_sha256: str | None
    collection_terminal_result_sha256: str | None
    variant_parameters_sha256: str
    signal_dataset_sha256: str
    venue: Venue
    market_id: str
    outcome_id: str
    signal_point_in_time_id: str
    signal_arrival_sequence: int
    signal_time_utc_ns: int
    signal_received_monotonic_ns: int
    decision_time_utc_ns: int
    decision_monotonic_ns: int
    quantity: Decimal
    limit_price: Decimal
    role: PredictionExecutionRole

    def __post_init__(self) -> None:
        identities = (
            self.order_id,
            self.opportunity_id,
            self.variant_id,
            self.market_id,
            self.outcome_id,
            self.signal_point_in_time_id,
        )
        hashes = (
            self.candidate_config_sha256,
            self.variant_parameters_sha256,
            self.signal_dataset_sha256,
        )
        if any(not item for item in identities) or any(len(item) != 64 for item in hashes):
            raise ValueError("prediction order intent identity is incomplete")
        if not self.opportunity_id.startswith("OPP:") or len(self.opportunity_id) != 68:
            raise ValueError("prediction opportunity id must be OPP: plus SHA-256")
        campaign_bindings = (
            self.campaign_manifest_sha256,
            self.collection_probe_binding_sha256,
            self.collection_terminal_result_sha256,
        )
        if any(item is not None for item in campaign_bindings) != all(
            item is not None for item in campaign_bindings
        ) or any(item is not None and len(item) != 64 for item in campaign_bindings):
            raise ValueError("prediction order intent campaign bindings are incomplete")
        if self.decision_time_utc_ns <= self.signal_time_utc_ns:
            raise ValueError("SAME_TIMESTAMP_PREDICTION_ACTION_FORBIDDEN")
        if self.decision_monotonic_ns <= self.signal_received_monotonic_ns:
            raise ValueError("SAME_MONOTONIC_PREDICTION_ACTION_FORBIDDEN")
        if self.signal_arrival_sequence <= 0:
            raise ValueError("prediction signal arrival watermark is invalid")
        if (
            not self.quantity.is_finite()
            or not self.limit_price.is_finite()
            or self.quantity <= 0
            or self.limit_price <= 0
            or self.limit_price >= 1
        ):
            raise ValueError("prediction order quantity, price, or queue is invalid")


@dataclass(frozen=True, slots=True)
class MakerAggressorEvidence:
    venue: Venue
    market_id: str
    outcome_id: str
    source_event_time_ns: int | None
    received_time_utc_ns: int
    received_monotonic_ns: int
    price: Decimal
    quantity: Decimal
    aggressor_side: str
    source_trade_id: str
    block_trade: bool
    source_event_id: str
    raw_manifest_sha256: str
    raw_root_sha256: str
    raw_ref: PredictionRawRecordRef
    collector_identity: str
    session_identity: str
    source_url: str

    def __post_init__(self) -> None:
        if (
            not self.market_id
            or not self.outcome_id
            or not self.source_event_id
            or not self.source_trade_id
            or not self.collector_identity
            or not self.session_identity
            or not self.source_url
            or len(self.raw_manifest_sha256) != 64
            or len(self.raw_root_sha256) != 64
            or min(self.received_time_utc_ns, self.received_monotonic_ns) < 0
            or self.price <= 0
            or self.price >= 1
            or self.quantity <= 0
        ):
            raise ValueError("maker aggressor evidence is invalid")
        if self.aggressor_side not in {"BUY", "SELL"} or self.block_trade:
            raise ValueError("maker aggressor side or block classification is invalid")
        if self.source_event_time_ns is not None and self.source_event_time_ns < 0:
            raise ValueError("maker aggressor source time is invalid")


@dataclass(frozen=True, slots=True)
class PredictionGhostFill:
    price: Decimal
    quantity: Decimal
    fee: Decimal

    def to_dict(self) -> dict[str, str]:
        return {
            "fee": format(self.fee, "f"),
            "price": format(self.price, "f"),
            "quantity": format(self.quantity, "f"),
        }


@dataclass(frozen=True, slots=True)
class PredictionGhostReport:
    order_id: str
    opportunity_id: str
    variant_id: str
    candidate_config_sha256: str
    campaign_manifest_sha256: str | None
    collection_probe_binding_sha256: str | None
    collection_terminal_result_sha256: str | None
    variant_parameters_sha256: str
    dataset_sha256: str
    dataset_synthetic: bool
    venue: Venue
    event_id: str
    market_id: str
    outcome_id: str
    status: str
    reason: str
    role: PredictionExecutionRole
    requested_quantity: Decimal
    filled_quantity: Decimal
    missed_quantity: Decimal
    fills: tuple[PredictionGhostFill, ...]
    average_price: Decimal | None
    entry_notional: Decimal
    payout: Decimal | None
    gross_pnl: Decimal | None
    fees: Decimal
    net_pnl: Decimal | None
    spread_cost: Decimal
    finite_depth_slippage: Decimal
    turnover: Decimal
    drawdown: Decimal | None
    capital_immobilized_notional_ns: Decimal
    unresolved_exposure: Decimal
    settlement_state: PredictionSettlementState
    signal_time_utc_ns: int
    signal_received_monotonic_ns: int
    decision_time_utc_ns: int
    decision_monotonic_ns: int
    admission_time_utc_ns: int
    admission_monotonic_ns: int
    report_time_utc_ns: int
    report_time_monotonic_ns: int
    max_evidence_received_utc_ns: int
    limit_price: Decimal
    fee_schedule_evidence_sha256: str
    tick_grid_evidence_sha256: str
    reconciliation_difference: Decimal | None
    raw_manifest_sha256: str
    raw_root_sha256: str
    raw_content_sha256s: tuple[str, ...]
    limitations: tuple[str, ...]
    _verification_token: object | None = field(default=None, compare=False, repr=False)
    _verified_report_sha256: str | None = field(default=None, compare=False, repr=False)

    def _body(self) -> dict[str, Any]:
        return {
            "attribution": {
                "capital_immobilized_notional_ns": format(self.capital_immobilized_notional_ns, "f"),
                "drawdown": None if self.drawdown is None else format(self.drawdown, "f"),
                "fees": format(self.fees, "f"),
                "finite_depth_slippage": format(self.finite_depth_slippage, "f"),
                "gross_pnl": None if self.gross_pnl is None else format(self.gross_pnl, "f"),
                "net_pnl": None if self.net_pnl is None else format(self.net_pnl, "f"),
                "payout": None if self.payout is None else format(self.payout, "f"),
                "spread_cost": format(self.spread_cost, "f"),
                "turnover": format(self.turnover, "f"),
                "unresolved_exposure": format(self.unresolved_exposure, "f"),
            },
            "average_price": (None if self.average_price is None else format(self.average_price, "f")),
            "boundary": BOUNDARY,
            "candidate_config_sha256": self.candidate_config_sha256,
            "campaign_manifest_sha256": self.campaign_manifest_sha256,
            "collection_probe_binding_sha256": self.collection_probe_binding_sha256,
            "collection_terminal_result_sha256": self.collection_terminal_result_sha256,
            "dataset_sha256": self.dataset_sha256,
            "dataset_synthetic": self.dataset_synthetic,
            "economic_claim": "NONE_RESEARCH_MECHANISM_ONLY",
            "event_id": self.event_id,
            "filled_quantity": format(self.filled_quantity, "f"),
            "fills": [item.to_dict() for item in self.fills],
            "limitations": list(self.limitations),
            "limit_price": format(self.limit_price, "f"),
            "market_id": self.market_id,
            "missed_quantity": format(self.missed_quantity, "f"),
            "model_version": GHOST_MODEL_VERSION,
            "model_bindings": {
                "fee_schedule_evidence_sha256": self.fee_schedule_evidence_sha256,
                "tick_grid_evidence_sha256": self.tick_grid_evidence_sha256,
                "variant_parameters_sha256": self.variant_parameters_sha256,
            },
            "order_id": self.order_id,
            "opportunity_id": self.opportunity_id,
            "outcome_id": self.outcome_id,
            "provenance": {
                "raw_content_sha256s": list(self.raw_content_sha256s),
                "raw_manifest_sha256": self.raw_manifest_sha256,
                "raw_root_sha256": self.raw_root_sha256,
            },
            "reason": self.reason,
            "reconciliation_difference": (
                None
                if self.reconciliation_difference is None
                else format(self.reconciliation_difference, "f")
            ),
            "requested_quantity": format(self.requested_quantity, "f"),
            "role": self.role.value,
            "schema_version": 1,
            "settlement_state": self.settlement_state.value,
            "status": self.status,
            "times": {
                "admission_monotonic_ns": self.admission_monotonic_ns,
                "admission_time_utc_ns": self.admission_time_utc_ns,
                "decision_monotonic_ns": self.decision_monotonic_ns,
                "decision_time_utc_ns": self.decision_time_utc_ns,
                "report_time_monotonic_ns": self.report_time_monotonic_ns,
                "report_time_utc_ns": self.report_time_utc_ns,
                "max_evidence_received_utc_ns": self.max_evidence_received_utc_ns,
                "signal_received_monotonic_ns": self.signal_received_monotonic_ns,
                "signal_time_utc_ns": self.signal_time_utc_ns,
            },
            "variant_id": self.variant_id,
            "venue": self.venue.value,
        }

    @property
    def report_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self._body())).hexdigest()

    @property
    def replay_verified(self) -> bool:
        return (
            self._verification_token is _REPLAY_VERIFICATION_TOKEN
            and self._verified_report_sha256 == self.report_sha256
        )

    def to_dict(self) -> dict[str, Any]:
        return {**self._body(), "report_sha256": self.report_sha256}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> PredictionGhostReport:
        def mapping(item: object, label: str) -> Mapping[str, object]:
            if not isinstance(item, Mapping):
                raise ValueError(f"{label} must be an object")
            return item

        def text(item: object, label: str) -> str:
            if type(item) is not str or not item:
                raise ValueError(f"{label} must be non-empty text")
            return item

        def decimal(item: object, label: str) -> Decimal:
            try:
                result = Decimal(str(item))
            except Exception as error:
                raise ValueError(f"{label} must be an exact decimal") from error
            if not result.is_finite():
                raise ValueError(f"{label} must be finite")
            return result

        def optional_decimal(item: object, label: str) -> Decimal | None:
            return None if item is None else decimal(item, label)

        def integer(item: object, label: str) -> int:
            if type(item) is not int:
                raise ValueError(f"{label} must be an integer")
            return item

        if (
            value.get("boundary") != BOUNDARY
            or value.get("economic_claim") != "NONE_RESEARCH_MECHANISM_ONLY"
            or value.get("model_version") != GHOST_MODEL_VERSION
            or value.get("schema_version") != 1
        ):
            raise ValueError("prediction Ghost report contract is invalid")
        attribution = mapping(value.get("attribution"), "prediction attribution")
        provenance = mapping(value.get("provenance"), "prediction provenance")
        bindings = mapping(value.get("model_bindings"), "prediction model bindings")
        times = mapping(value.get("times"), "prediction report times")
        raw_fills = value.get("fills")
        raw_hashes = provenance.get("raw_content_sha256s")
        limitations = value.get("limitations")
        if (
            not isinstance(raw_fills, list)
            or not isinstance(raw_hashes, list)
            or not isinstance(limitations, list)
        ):
            raise ValueError("prediction Ghost report arrays are invalid")
        fills = tuple(
            PredictionGhostFill(
                price=decimal(mapping(item, "prediction fill").get("price"), "fill price"),
                quantity=decimal(mapping(item, "prediction fill").get("quantity"), "fill quantity"),
                fee=decimal(mapping(item, "prediction fill").get("fee"), "fill fee"),
            )
            for item in raw_fills
        )
        report = cls(
            order_id=text(value.get("order_id"), "order id"),
            opportunity_id=text(value.get("opportunity_id"), "opportunity id"),
            variant_id=text(value.get("variant_id"), "variant id"),
            candidate_config_sha256=text(value.get("candidate_config_sha256"), "candidate config hash"),
            campaign_manifest_sha256=(
                None
                if value.get("campaign_manifest_sha256") is None
                else text(value.get("campaign_manifest_sha256"), "campaign manifest hash")
            ),
            collection_probe_binding_sha256=(
                None
                if value.get("collection_probe_binding_sha256") is None
                else text(value.get("collection_probe_binding_sha256"), "collection binding hash")
            ),
            collection_terminal_result_sha256=(
                None
                if value.get("collection_terminal_result_sha256") is None
                else text(
                    value.get("collection_terminal_result_sha256"),
                    "collection terminal result hash",
                )
            ),
            variant_parameters_sha256=text(
                bindings.get("variant_parameters_sha256"), "variant parameters hash"
            ),
            dataset_sha256=text(value.get("dataset_sha256"), "dataset hash"),
            dataset_synthetic=value.get("dataset_synthetic") is True,
            venue=Venue(text(value.get("venue"), "venue")),
            event_id=text(value.get("event_id"), "event id"),
            market_id=text(value.get("market_id"), "market id"),
            outcome_id=text(value.get("outcome_id"), "outcome id"),
            status=text(value.get("status"), "status"),
            reason=text(value.get("reason"), "reason"),
            role=PredictionExecutionRole(text(value.get("role"), "execution role")),
            requested_quantity=decimal(value.get("requested_quantity"), "requested quantity"),
            filled_quantity=decimal(value.get("filled_quantity"), "filled quantity"),
            missed_quantity=decimal(value.get("missed_quantity"), "missed quantity"),
            fills=fills,
            average_price=optional_decimal(value.get("average_price"), "average price"),
            entry_notional=sum((item.price * item.quantity for item in fills), _ZERO),
            payout=optional_decimal(attribution.get("payout"), "payout"),
            gross_pnl=optional_decimal(attribution.get("gross_pnl"), "gross pnl"),
            fees=decimal(attribution.get("fees"), "fees"),
            net_pnl=optional_decimal(attribution.get("net_pnl"), "net pnl"),
            spread_cost=decimal(attribution.get("spread_cost"), "spread cost"),
            finite_depth_slippage=decimal(attribution.get("finite_depth_slippage"), "finite depth slippage"),
            turnover=decimal(attribution.get("turnover"), "turnover"),
            drawdown=optional_decimal(attribution.get("drawdown"), "drawdown"),
            capital_immobilized_notional_ns=decimal(
                attribution.get("capital_immobilized_notional_ns"), "capital lock"
            ),
            unresolved_exposure=decimal(attribution.get("unresolved_exposure"), "unresolved exposure"),
            settlement_state=PredictionSettlementState(
                text(value.get("settlement_state"), "settlement state")
            ),
            signal_time_utc_ns=integer(times.get("signal_time_utc_ns"), "signal UTC time"),
            signal_received_monotonic_ns=integer(
                times.get("signal_received_monotonic_ns"), "signal monotonic time"
            ),
            decision_time_utc_ns=integer(times.get("decision_time_utc_ns"), "decision UTC time"),
            decision_monotonic_ns=integer(times.get("decision_monotonic_ns"), "decision monotonic time"),
            admission_time_utc_ns=integer(times.get("admission_time_utc_ns"), "admission UTC time"),
            admission_monotonic_ns=integer(times.get("admission_monotonic_ns"), "admission monotonic time"),
            report_time_utc_ns=integer(times.get("report_time_utc_ns"), "report UTC time"),
            report_time_monotonic_ns=integer(times.get("report_time_monotonic_ns"), "report monotonic time"),
            max_evidence_received_utc_ns=integer(
                times.get("max_evidence_received_utc_ns"),
                "maximum evidence receive time",
            ),
            limit_price=decimal(value.get("limit_price"), "limit price"),
            fee_schedule_evidence_sha256=text(
                bindings.get("fee_schedule_evidence_sha256"), "fee evidence hash"
            ),
            tick_grid_evidence_sha256=text(bindings.get("tick_grid_evidence_sha256"), "tick evidence hash"),
            reconciliation_difference=optional_decimal(
                value.get("reconciliation_difference"), "reconciliation difference"
            ),
            raw_manifest_sha256=text(provenance.get("raw_manifest_sha256"), "raw manifest hash"),
            raw_root_sha256=text(provenance.get("raw_root_sha256"), "raw root hash"),
            raw_content_sha256s=tuple(text(item, "raw content hash") for item in raw_hashes),
            limitations=tuple(text(item, "limitation") for item in limitations),
        )
        if value.get("report_sha256") != report.report_sha256:
            raise ValueError("prediction Ghost report SHA-256 diverged")
        return report


class PredictionOpportunityStatus(StrEnum):
    CANDIDATE = "CANDIDATE"
    NO_ACTION = "NO_ACTION"
    REFUSED = "REFUSED"


@dataclass(frozen=True, slots=True)
class PredictionOpportunitySignal:
    venue: Venue
    dataset_sha256: str
    event_id: str
    market_id: str
    outcome_id: str
    point_in_time_id: str
    arrival_sequence: int
    received_time_utc_ns: int
    received_monotonic_ns: int
    raw_content_sha256: str
    raw_record_index: int
    collector_identity: str
    session_identity: str

    @classmethod
    def from_row(
        cls,
        row: PredictionDepthSnapshot,
        *,
        dataset_sha256: str,
    ) -> PredictionOpportunitySignal:
        return cls(
            venue=row.venue,
            dataset_sha256=dataset_sha256,
            event_id=row.event_id,
            market_id=row.market_id,
            outcome_id=row.outcome_id,
            point_in_time_id=row.point_in_time_id,
            arrival_sequence=row.arrival_sequence,
            received_time_utc_ns=row.received_time_utc_ns,
            received_monotonic_ns=row.received_monotonic_ns,
            raw_content_sha256=row.raw_content_sha256,
            raw_record_index=row.raw_record_index,
            collector_identity=row.collector_identity,
            session_identity=row.session_identity,
        )

    def __post_init__(self) -> None:
        hashes = (self.dataset_sha256, self.point_in_time_id, self.raw_content_sha256)
        if (
            any(len(item) != 64 for item in hashes)
            or not self.event_id
            or not self.market_id
            or not self.outcome_id
            or not self.collector_identity
            or not self.session_identity
            or self.arrival_sequence <= 0
            or min(self.received_time_utc_ns, self.received_monotonic_ns) < 0
            or self.raw_record_index < 0
        ):
            raise ValueError("prediction opportunity signal identity is invalid")

    @property
    def member_key(self) -> tuple[Venue, str, str]:
        return (self.venue, self.market_id, self.outcome_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "arrival_sequence": self.arrival_sequence,
            "collector_identity": self.collector_identity,
            "dataset_sha256": self.dataset_sha256,
            "event_id": self.event_id,
            "market_id": self.market_id,
            "outcome_id": self.outcome_id,
            "point_in_time_id": self.point_in_time_id,
            "raw_content_sha256": self.raw_content_sha256,
            "raw_record_index": self.raw_record_index,
            "received_monotonic_ns": self.received_monotonic_ns,
            "received_time_utc_ns": self.received_time_utc_ns,
            "session_identity": self.session_identity,
            "venue": self.venue.value,
        }


@dataclass(frozen=True, slots=True)
class PredictionLegEvidenceKey:
    opportunity_id: str
    signal_dataset_sha256: str
    venue: Venue
    market_id: str
    outcome_id: str

    def __post_init__(self) -> None:
        if (
            not self.opportunity_id.startswith("OPP:")
            or len(self.opportunity_id) != 68
            or len(self.signal_dataset_sha256) != 64
            or not self.market_id
            or not self.outcome_id
        ):
            raise ValueError("prediction leg evidence key is incomplete")

    @classmethod
    def from_signal(
        cls,
        opportunity_id: str,
        signal: PredictionOpportunitySignal,
    ) -> PredictionLegEvidenceKey:
        return cls(
            opportunity_id=opportunity_id,
            signal_dataset_sha256=signal.dataset_sha256,
            venue=signal.venue,
            market_id=signal.market_id,
            outcome_id=signal.outcome_id,
        )


@dataclass(frozen=True, slots=True)
class PredictionOpportunity:
    opportunity_id: str
    family_id: str
    variant_id: str
    variant_parameters_sha256: str
    candidate_config_sha256: str
    campaign_manifest_sha256: str | None
    runner_policy_sha256: str
    dataset_bundle_sha256: str
    semantic_catalog_sha256: str
    relation_ids: tuple[str, ...]
    signals: tuple[PredictionOpportunitySignal, ...]
    trigger_point_in_time_id: str
    execution_sequence: tuple[str, ...]
    quantity: Decimal
    status: PredictionOpportunityStatus
    reasons: tuple[str, ...]
    _verification_token: object | None = field(default=None, compare=False, repr=False)
    _verified_sha256: str | None = field(default=None, compare=False, repr=False)

    @classmethod
    def _create_for_runner(
        cls,
        *,
        family_id: str,
        variant_id: str,
        variant_parameters_sha256: str,
        candidate_config_sha256: str,
        campaign_manifest_sha256: str | None,
        runner_policy_sha256: str,
        dataset_bundle_sha256: str,
        semantic_catalog_sha256: str,
        relation_ids: Sequence[str],
        signals: Sequence[PredictionOpportunitySignal],
        trigger_point_in_time_id: str,
        execution_sequence: Sequence[str],
        quantity: Decimal,
        status: PredictionOpportunityStatus,
        reasons: Sequence[str],
        _runner_token: object,
    ) -> PredictionOpportunity:
        if _runner_token is not _CAMPAIGN_RUNNER_TOKEN:
            raise ValueError("prediction opportunities are created only by the campaign runner")
        relation_tuple = tuple(sorted(set(relation_ids)))
        signal_tuple = tuple(
            sorted(
                signals,
                key=lambda item: (
                    item.received_time_utc_ns,
                    item.venue.value,
                    item.arrival_sequence,
                    item.market_id,
                    item.outcome_id,
                ),
            )
        )
        identity = cls._identity_payload(
            family_id=family_id,
            variant_id=variant_id,
            variant_parameters_sha256=variant_parameters_sha256,
            candidate_config_sha256=candidate_config_sha256,
            campaign_manifest_sha256=campaign_manifest_sha256,
            runner_policy_sha256=runner_policy_sha256,
            dataset_bundle_sha256=dataset_bundle_sha256,
            semantic_catalog_sha256=semantic_catalog_sha256,
            relation_ids=relation_tuple,
            signals=signal_tuple,
            trigger_point_in_time_id=trigger_point_in_time_id,
            execution_sequence=tuple(execution_sequence),
        )
        opportunity = cls(
            opportunity_id=f"OPP:{hashlib.sha256(canonical_json_bytes(identity)).hexdigest()}",
            family_id=family_id,
            variant_id=variant_id,
            variant_parameters_sha256=variant_parameters_sha256,
            candidate_config_sha256=candidate_config_sha256,
            campaign_manifest_sha256=campaign_manifest_sha256,
            runner_policy_sha256=runner_policy_sha256,
            dataset_bundle_sha256=dataset_bundle_sha256,
            semantic_catalog_sha256=semantic_catalog_sha256,
            relation_ids=relation_tuple,
            signals=signal_tuple,
            trigger_point_in_time_id=trigger_point_in_time_id,
            execution_sequence=tuple(execution_sequence),
            quantity=quantity,
            status=status,
            reasons=tuple(reasons),
        )
        return replace(
            opportunity,
            _verification_token=_OPPORTUNITY_VERIFICATION_TOKEN,
            _verified_sha256=opportunity.opportunity_sha256,
        )

    @staticmethod
    def _identity_payload(
        *,
        family_id: str,
        variant_id: str,
        variant_parameters_sha256: str,
        candidate_config_sha256: str,
        campaign_manifest_sha256: str | None,
        runner_policy_sha256: str,
        dataset_bundle_sha256: str,
        semantic_catalog_sha256: str,
        relation_ids: Sequence[str],
        signals: Sequence[PredictionOpportunitySignal],
        trigger_point_in_time_id: str,
        execution_sequence: Sequence[str],
    ) -> dict[str, Any]:
        return {
            "campaign_manifest_sha256": campaign_manifest_sha256,
            "candidate_config_sha256": candidate_config_sha256,
            "dataset_bundle_sha256": dataset_bundle_sha256,
            "execution_sequence": list(execution_sequence),
            "family_id": family_id,
            "model_version": "PREDICTION_OPPORTUNITY_RUNNER_V1",
            "relation_ids": list(relation_ids),
            "runner_policy_sha256": runner_policy_sha256,
            "semantic_catalog_sha256": semantic_catalog_sha256,
            "signals": [item.to_dict() for item in signals],
            "trigger_point_in_time_id": trigger_point_in_time_id,
            "variant_id": variant_id,
            "variant_parameters_sha256": variant_parameters_sha256,
        }

    def __post_init__(self) -> None:
        identity = self._identity_payload(
            family_id=self.family_id,
            variant_id=self.variant_id,
            variant_parameters_sha256=self.variant_parameters_sha256,
            candidate_config_sha256=self.candidate_config_sha256,
            campaign_manifest_sha256=self.campaign_manifest_sha256,
            runner_policy_sha256=self.runner_policy_sha256,
            dataset_bundle_sha256=self.dataset_bundle_sha256,
            semantic_catalog_sha256=self.semantic_catalog_sha256,
            relation_ids=self.relation_ids,
            signals=self.signals,
            trigger_point_in_time_id=self.trigger_point_in_time_id,
            execution_sequence=self.execution_sequence,
        )
        expected = f"OPP:{hashlib.sha256(canonical_json_bytes(identity)).hexdigest()}"
        hashes = (
            self.variant_parameters_sha256,
            self.candidate_config_sha256,
            self.runner_policy_sha256,
            self.dataset_bundle_sha256,
            self.semantic_catalog_sha256,
        )
        if (
            self.opportunity_id != expected
            or any(len(item) != 64 for item in hashes)
            or not self.family_id
            or not self.variant_id
            or not self.signals
            or len(
                [
                    item
                    for item in self.signals
                    if item.point_in_time_id == self.trigger_point_in_time_id
                ]
            )
            != 1
            or len(set(self.execution_sequence)) != len(self.execution_sequence)
            or (
                self.status is PredictionOpportunityStatus.CANDIDATE
                and not self.execution_sequence
            )
            or (
                self.campaign_manifest_sha256 is not None
                and len(self.campaign_manifest_sha256) != 64
            )
            or any(item.dataset_sha256 == "" for item in self.signals)
            or not self.quantity.is_finite()
            or self.quantity <= 0
            or (self.status is PredictionOpportunityStatus.CANDIDATE and self.reasons)
            or (self.status is not PredictionOpportunityStatus.CANDIDATE and not self.reasons)
        ):
            raise ValueError("prediction opportunity is not a canonical preregistered decision")

    def _body(self) -> dict[str, Any]:
        return {
            **self._identity_payload(
                family_id=self.family_id,
                variant_id=self.variant_id,
                variant_parameters_sha256=self.variant_parameters_sha256,
                candidate_config_sha256=self.candidate_config_sha256,
                campaign_manifest_sha256=self.campaign_manifest_sha256,
                runner_policy_sha256=self.runner_policy_sha256,
                dataset_bundle_sha256=self.dataset_bundle_sha256,
                semantic_catalog_sha256=self.semantic_catalog_sha256,
                relation_ids=self.relation_ids,
                signals=self.signals,
                trigger_point_in_time_id=self.trigger_point_in_time_id,
                execution_sequence=self.execution_sequence,
            ),
            "opportunity_id": self.opportunity_id,
            "quantity": format(self.quantity, "f"),
            "reasons": list(self.reasons),
            "status": self.status.value,
        }

    @property
    def opportunity_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self._body())).hexdigest()

    @property
    def replay_verified(self) -> bool:
        return (
            self._verification_token is _OPPORTUNITY_VERIFICATION_TOKEN
            and self._verified_sha256 == self.opportunity_sha256
        )

    def to_dict(self) -> dict[str, Any]:
        return {**self._body(), "opportunity_sha256": self.opportunity_sha256}


def _consume_taker(
    levels: Sequence[PredictionDepthLevel],
    *,
    quantity: Decimal,
    limit_price: Decimal,
) -> tuple[PredictionGhostFill, ...]:
    remaining = quantity
    fills: list[PredictionGhostFill] = []
    for level in levels:
        if remaining == 0 or level.price > limit_price:
            break
        filled = min(remaining, level.quantity)
        fills.append(
            PredictionGhostFill(
                price=level.price,
                quantity=filled,
                fee=_ZERO,
            )
        )
        remaining -= filled
    return tuple(fills)


def _consume_maker(
    snapshot: PredictionDepthSnapshot,
    intent: PredictionOrderIntent,
    evidence: MakerAggressorEvidence | None,
    *,
    admission_time_utc_ns: int,
    admission_monotonic_ns: int,
) -> tuple[PredictionGhostFill, ...]:
    if evidence is None:
        return ()
    if (
        evidence.venue is not snapshot.venue
        or evidence.market_id != snapshot.market_id
        or evidence.outcome_id != snapshot.outcome_id
        or evidence.received_time_utc_ns < admission_time_utc_ns
        or evidence.received_monotonic_ns < admission_monotonic_ns
        or evidence.aggressor_side != "SELL"
        or evidence.price > intent.limit_price
        or intent.limit_price >= snapshot.asks[0].price
    ):
        return ()
    pessimistic_queue = sum(
        (item.quantity for item in snapshot.bids if item.price >= intent.limit_price),
        _ZERO,
    )
    after_queue = max(evidence.quantity - pessimistic_queue, _ZERO)
    filled = min(intent.quantity, after_queue)
    if filled == 0:
        return ()
    return (
        PredictionGhostFill(
            price=intent.limit_price,
            quantity=filled,
            fee=_ZERO,
        ),
    )


def _apply_order_fee(
    fills: Sequence[PredictionGhostFill],
    *,
    fee_schedule: PredictionFeeSchedule,
    maker: bool,
) -> tuple[PredictionGhostFill, ...]:
    if not fills:
        return ()
    order_fee = fee_schedule.order_fee(
        levels=tuple((item.price, item.quantity) for item in fills),
        maker=maker,
    )
    return tuple(
        PredictionGhostFill(
            price=item.price,
            quantity=item.quantity,
            fee=order_fee if index == len(fills) - 1 else _ZERO,
        )
        for index, item in enumerate(fills)
    )


@dataclass(frozen=True, slots=True)
class PredictionGhostGroupReport:
    order_id: str
    opportunity_id: str
    family_id: str
    variant_id: str
    candidate_config_sha256: str
    campaign_manifest_sha256: str | None
    collection_probe_binding_sha256: str | None
    collection_terminal_result_sha256: str | None
    variant_parameters_sha256: str
    dataset_sha256: str
    child_dataset_sha256s: tuple[str, ...]
    dataset_synthetic: bool
    semantic_catalog_sha256: str
    relation_ids: tuple[str, ...]
    leg_reports: tuple[PredictionGhostReport, ...]
    venue: Venue
    market_id: str
    outcome_id: str
    signal_time_utc_ns: int
    max_evidence_received_utc_ns: int
    gross_pnl: Decimal | None
    fees: Decimal
    net_pnl: Decimal | None
    spread_cost: Decimal
    finite_depth_slippage: Decimal
    turnover: Decimal
    drawdown: Decimal | None
    capital_immobilized_notional_ns: Decimal
    unresolved_exposure: Decimal
    reconciliation_difference: Decimal | None
    worst_leg_fill_ratio: Decimal
    status: str
    reason: str
    _verification_token: object | None = field(default=None, compare=False, repr=False)
    _verified_report_sha256: str | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        if (
            not self.order_id
            or not self.opportunity_id.startswith("OPP:")
            or len(self.opportunity_id) != 68
            or self.family_id not in {"K4_COMPLETE_SET_LOGICAL_RV", "K6_CROSS_VENUE_EQUIVALENCE"}
            or len(self.leg_reports) < 2
            or len({item.order_id for item in self.leg_reports}) != len(self.leg_reports)
            or any(item.opportunity_id != self.opportunity_id for item in self.leg_reports)
            or not self.worst_leg_fill_ratio.is_finite()
            or self.worst_leg_fill_ratio < 0
            or self.worst_leg_fill_ratio > 1
        ):
            raise ValueError("prediction Ghost group report identity is invalid")
        if self.reconciliation_difference not in {None, _ZERO}:
            raise ValueError("prediction Ghost group report does not reconcile")
        bindings = {
            (
                item.candidate_config_sha256,
                item.campaign_manifest_sha256,
                item.collection_probe_binding_sha256,
                item.collection_terminal_result_sha256,
                item.variant_id,
                item.variant_parameters_sha256,
            )
            for item in self.leg_reports
        }
        expected_binding = (
            self.candidate_config_sha256,
            self.campaign_manifest_sha256,
            self.collection_probe_binding_sha256,
            self.collection_terminal_result_sha256,
            self.variant_id,
            self.variant_parameters_sha256,
        )
        child_hashes = tuple(sorted({item.dataset_sha256 for item in self.leg_reports}))
        all_settled = all(item.net_pnl is not None for item in self.leg_reports)
        expected_net = (
            None
            if not all_settled
            else sum(
                (item.net_pnl for item in self.leg_reports if item.net_pnl is not None),
                _ZERO,
            )
        )
        expected_gross = (
            None
            if not all_settled
            else sum(
                (item.gross_pnl for item in self.leg_reports if item.gross_pnl is not None),
                _ZERO,
            )
        )
        expected_worst = min(
            item.filled_quantity / item.requested_quantity for item in self.leg_reports
        )
        expected_status = (
            "COMPLETE"
            if expected_worst == _ONE
            else ("MISSED" if expected_worst == _ZERO else "PARTIAL")
        )
        if (
            bindings != {expected_binding}
            or self.child_dataset_sha256s != child_hashes
            or self.dataset_synthetic != all(item.dataset_synthetic for item in self.leg_reports)
            or self.signal_time_utc_ns
            != max(item.signal_time_utc_ns for item in self.leg_reports)
            or self.max_evidence_received_utc_ns
            != max(item.max_evidence_received_utc_ns for item in self.leg_reports)
            or self.gross_pnl != expected_gross
            or self.net_pnl != expected_net
            or self.fees != sum((item.fees for item in self.leg_reports), _ZERO)
            or self.spread_cost != sum((item.spread_cost for item in self.leg_reports), _ZERO)
            or self.finite_depth_slippage
            != sum((item.finite_depth_slippage for item in self.leg_reports), _ZERO)
            or self.turnover != sum((item.turnover for item in self.leg_reports), _ZERO)
            or self.capital_immobilized_notional_ns
            != sum(
                (item.capital_immobilized_notional_ns for item in self.leg_reports),
                _ZERO,
            )
            or self.unresolved_exposure
            != sum((item.unresolved_exposure for item in self.leg_reports), _ZERO)
            or self.worst_leg_fill_ratio != expected_worst
            or self.status != expected_status
        ):
            raise ValueError("prediction Ghost group report aggregate or binding diverged")

    def _body(self) -> dict[str, Any]:
        return {
            "attribution": {
                "capital_immobilized_notional_ns": format(
                    self.capital_immobilized_notional_ns, "f"
                ),
                "drawdown": None if self.drawdown is None else format(self.drawdown, "f"),
                "fees": format(self.fees, "f"),
                "finite_depth_slippage": format(self.finite_depth_slippage, "f"),
                "gross_pnl": None if self.gross_pnl is None else format(self.gross_pnl, "f"),
                "net_pnl": None if self.net_pnl is None else format(self.net_pnl, "f"),
                "spread_cost": format(self.spread_cost, "f"),
                "turnover": format(self.turnover, "f"),
                "unresolved_exposure": format(self.unresolved_exposure, "f"),
            },
            "boundary": BOUNDARY,
            "campaign_manifest_sha256": self.campaign_manifest_sha256,
            "candidate_config_sha256": self.candidate_config_sha256,
            "child_dataset_sha256s": list(self.child_dataset_sha256s),
            "collection_probe_binding_sha256": self.collection_probe_binding_sha256,
            "collection_terminal_result_sha256": self.collection_terminal_result_sha256,
            "dataset_sha256": self.dataset_sha256,
            "dataset_synthetic": self.dataset_synthetic,
            "economic_claim": "NONE_RESEARCH_MECHANISM_ONLY",
            "family_id": self.family_id,
            "leg_reports": [item.to_dict() for item in self.leg_reports],
            "market_id": self.market_id,
            "model_version": "PREDICTION_MARKETS_GHOST_GROUP_V1",
            "opportunity_id": self.opportunity_id,
            "order_id": self.order_id,
            "outcome_id": self.outcome_id,
            "reason": self.reason,
            "reconciliation_difference": (
                None
                if self.reconciliation_difference is None
                else format(self.reconciliation_difference, "f")
            ),
            "relation_ids": list(self.relation_ids),
            "semantic_catalog_sha256": self.semantic_catalog_sha256,
            "signal_time_utc_ns": self.signal_time_utc_ns,
            "max_evidence_received_utc_ns": self.max_evidence_received_utc_ns,
            "status": self.status,
            "variant_id": self.variant_id,
            "variant_parameters_sha256": self.variant_parameters_sha256,
            "venue": self.venue.value,
            "worst_leg_fill_ratio": format(self.worst_leg_fill_ratio, "f"),
        }

    @property
    def report_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self._body())).hexdigest()

    @property
    def replay_verified(self) -> bool:
        return (
            self._verification_token is _GROUP_REPORT_VERIFICATION_TOKEN
            and self._verified_report_sha256 == self.report_sha256
            and all(item.replay_verified for item in self.leg_reports)
        )

    def to_dict(self) -> dict[str, Any]:
        return {**self._body(), "report_sha256": self.report_sha256}


@dataclass(frozen=True, slots=True)
class PredictionCampaignCollectionReceipt:
    venue: Venue
    child_dataset_sha256: str
    collection_id: str
    prospective_shard_ordinal: int
    probe_binding_sha256: str
    terminal_result_sha256: str
    raw_manifest_sha256: str
    raw_root_sha256: str

    def __post_init__(self) -> None:
        if (
            not self.collection_id
            or type(self.prospective_shard_ordinal) is not int
            or self.prospective_shard_ordinal < 0
            or any(
                len(item) != 64
                for item in (
                    self.child_dataset_sha256,
                    self.probe_binding_sha256,
                    self.terminal_result_sha256,
                    self.raw_manifest_sha256,
                    self.raw_root_sha256,
                )
            )
        ):
            raise ValueError("prediction campaign collection receipt is incomplete")

    def to_dict(self) -> dict[str, Any]:
        return {
            "child_dataset_sha256": self.child_dataset_sha256,
            "collection_id": self.collection_id,
            "prospective_shard_ordinal": self.prospective_shard_ordinal,
            "probe_binding_sha256": self.probe_binding_sha256,
            "raw_manifest_sha256": self.raw_manifest_sha256,
            "raw_root_sha256": self.raw_root_sha256,
            "terminal_result_sha256": self.terminal_result_sha256,
            "venue": self.venue.value,
        }


PredictionTopLevelReport = PredictionGhostReport | PredictionGhostGroupReport


@dataclass(frozen=True, slots=True)
class PredictionCampaignReplayReport:
    candidate_config_sha256: str
    campaign_manifest_sha256: str | None
    runner_policy_sha256: str
    dataset_sha256: str
    child_dataset_sha256s: tuple[str, ...]
    dataset_synthetic: bool
    semantic_catalog_sha256: str
    collection_receipts: tuple[PredictionCampaignCollectionReceipt, ...]
    replay_seal_sha256: str
    opportunities: tuple[PredictionOpportunity, ...]
    reports: tuple[PredictionTopLevelReport, ...]
    evidence_cutoff_utc_ns_exclusive: int
    selection_split_view: Mapping[str, Any]
    prospective_slot_coverage: Mapping[str, Any] | None
    _verification_token: object | None = field(default=None, compare=False, repr=False)
    _verified_report_sha256: str | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        hashes = (
            self.candidate_config_sha256,
            self.runner_policy_sha256,
            self.dataset_sha256,
            self.semantic_catalog_sha256,
            self.replay_seal_sha256,
            *self.child_dataset_sha256s,
        )
        if (
            any(len(item) != 64 for item in hashes)
            or not self.child_dataset_sha256s
            or tuple(sorted(set(self.child_dataset_sha256s)))
            != self.child_dataset_sha256s
            or self.evidence_cutoff_utc_ns_exclusive <= 0
            or (
                self.campaign_manifest_sha256 is not None
                and len(self.campaign_manifest_sha256) != 64
            )
        ):
            raise ValueError("prediction campaign replay identity is invalid")
        expected_selection_fields = {"dataset_hash", "plan_hash", "train", "validation"}
        if (
            set(self.selection_split_view) != expected_selection_fields
            or self.selection_split_view.get("dataset_hash") != self.dataset_sha256
            or "test" in self.selection_split_view
            or "final_test" in self.selection_split_view
        ):
            raise ValueError("prediction campaign selection view is invalid or exposes holdout")
        if self.dataset_synthetic:
            if (
                self.campaign_manifest_sha256 is not None
                or self.collection_receipts
                or self.prospective_slot_coverage is not None
            ):
                raise ValueError("synthetic prediction campaign cannot claim public receipts")
        else:
            receipt_datasets = tuple(
                item.child_dataset_sha256 for item in self.collection_receipts
            )
            if (
                not receipt_datasets
                or len(set(receipt_datasets)) != len(receipt_datasets)
                or set(receipt_datasets) != set(self.child_dataset_sha256s)
            ):
                raise ValueError("public prediction campaign receipts are missing or ambiguous")
            coverage = self.prospective_slot_coverage
            if not isinstance(coverage, Mapping) or set(coverage) != {
                "economic_corpus_complete",
                "evidence_cutoff_utc_ns_exclusive",
                "excluded_receipts",
                "expected_ordinal_exclusive",
                "nonreplayable_raw_receipts",
                "schedule_accounted",
                "venues",
            }:
                raise ValueError("public prediction campaign lacks slot coverage")
            expected = coverage.get("expected_ordinal_exclusive")
            raw_coverage: set[tuple[Venue, int]] = set()
            excluded_coverage: set[tuple[Venue, int]] = set()
            nonreplayable_coverage: set[tuple[Venue, int]] = set()
            missing_any = False
            raw_venues = coverage.get("venues")
            if (
                type(expected) is not int
                or expected <= 0
                or coverage.get("evidence_cutoff_utc_ns_exclusive")
                != self.evidence_cutoff_utc_ns_exclusive
                or not isinstance(raw_venues, Mapping)
                or set(raw_venues) != {Venue.POLYMARKET.value, Venue.KALSHI.value}
            ):
                raise ValueError("public prediction slot coverage identity diverged")
            expected_ordinals = set(range(expected))
            for venue in (Venue.POLYMARKET, Venue.KALSHI):
                raw_entry = raw_venues.get(venue.value)
                if not isinstance(raw_entry, Mapping) or set(raw_entry) != {
                    "excluded_ordinals",
                    "missing_ordinals",
                    "nonreplayable_raw_ordinals",
                    "raw_ordinals",
                }:
                    raise ValueError("public prediction venue slot coverage schema diverged")
                classified: list[set[int]] = []
                for field_name in (
                    "raw_ordinals",
                    "excluded_ordinals",
                    "nonreplayable_raw_ordinals",
                    "missing_ordinals",
                ):
                    raw_values = raw_entry.get(field_name)
                    if (
                        not isinstance(raw_values, list)
                        or any(type(item) is not int for item in raw_values)
                        or raw_values != sorted(set(raw_values))
                    ):
                        raise ValueError("public prediction slot coverage is not canonical")
                    classified.append(set(raw_values))
                (
                    raw_ordinals,
                    excluded_ordinals,
                    nonreplayable_raw_ordinals,
                    missing_ordinals,
                ) = classified
                if (
                    raw_ordinals & excluded_ordinals
                    or raw_ordinals & nonreplayable_raw_ordinals
                    or raw_ordinals & missing_ordinals
                    or excluded_ordinals & nonreplayable_raw_ordinals
                    or excluded_ordinals & missing_ordinals
                    or nonreplayable_raw_ordinals & missing_ordinals
                    or raw_ordinals
                    | excluded_ordinals
                    | nonreplayable_raw_ordinals
                    | missing_ordinals
                    != expected_ordinals
                ):
                    raise ValueError("public prediction slot coverage is not exhaustive")
                raw_coverage.update((venue, ordinal) for ordinal in raw_ordinals)
                excluded_coverage.update((venue, ordinal) for ordinal in excluded_ordinals)
                nonreplayable_coverage.update(
                    (venue, ordinal) for ordinal in nonreplayable_raw_ordinals
                )
                missing_any |= bool(missing_ordinals)
            receipt_slots = {
                (item.venue, item.prospective_shard_ordinal)
                for item in self.collection_receipts
            }
            raw_excluded_receipts = coverage.get("excluded_receipts")
            if not isinstance(raw_excluded_receipts, list):
                raise ValueError("public prediction excluded receipt ledger is invalid")
            excluded_receipt_slots: set[tuple[Venue, int]] = set()
            receipt_order: list[tuple[str, int, str]] = []
            expected_excluded_fields = {
                "classification",
                "frames",
                "ordinal",
                "probe_config_sha256",
                "raw_manifest_sha256",
                "raw_root_sha256",
                "terminal_health",
                "terminal_result_sha256",
                "venue",
            }
            for raw_receipt in raw_excluded_receipts:
                if not isinstance(raw_receipt, Mapping) or set(raw_receipt) != expected_excluded_fields:
                    raise ValueError("public prediction excluded receipt schema diverged")
                venue = Venue(str(raw_receipt.get("venue") or ""))
                ordinal = raw_receipt.get("ordinal")
                frames = raw_receipt.get("frames")
                probe_hash = str(raw_receipt.get("probe_config_sha256") or "")
                terminal_hash = str(raw_receipt.get("terminal_result_sha256") or "")
                raw_manifest = raw_receipt.get("raw_manifest_sha256")
                raw_root = raw_receipt.get("raw_root_sha256")
                hashes = (probe_hash, terminal_hash)
                optional_hashes = (raw_manifest, raw_root)
                if (
                    type(ordinal) is not int
                    or ordinal < 0
                    or type(frames) is not int
                    or frames < 0
                    or any(
                        len(item) != 64
                        or any(character not in "0123456789abcdef" for character in item)
                        for item in hashes
                    )
                    or (
                        frames == 0
                        and any(item is not None for item in optional_hashes)
                    )
                    or (
                        frames > 0
                        and any(
                            type(item) is not str
                            or len(item) != 64
                            or any(
                                character not in "0123456789abcdef"
                                for character in item
                            )
                            for item in optional_hashes
                        )
                    )
                    or not str(raw_receipt.get("classification") or "")
                    or not str(raw_receipt.get("terminal_health") or "")
                ):
                    raise ValueError("public prediction excluded receipt identity diverged")
                slot = (venue, ordinal)
                if slot in excluded_receipt_slots:
                    raise ValueError("public prediction excluded receipt slot is duplicated")
                excluded_receipt_slots.add(slot)
                receipt_order.append((venue.value, ordinal, probe_hash))
            if receipt_order != sorted(receipt_order):
                raise ValueError("public prediction excluded receipts are not canonical")
            raw_nonreplayable_receipts = coverage.get("nonreplayable_raw_receipts")
            if not isinstance(raw_nonreplayable_receipts, list):
                raise ValueError("public prediction nonreplayable raw ledger is invalid")
            nonreplayable_receipt_slots: set[tuple[Venue, int]] = set()
            nonreplayable_order: list[tuple[str, int, str]] = []
            expected_nonreplayable_fields = {
                "frames",
                "ordinal",
                "probe_binding_sha256",
                "raw_manifest_sha256",
                "raw_root_sha256",
                "terminal_result_sha256",
                "venue",
            }
            for raw_receipt in raw_nonreplayable_receipts:
                if (
                    not isinstance(raw_receipt, Mapping)
                    or set(raw_receipt) != expected_nonreplayable_fields
                ):
                    raise ValueError("public prediction nonreplayable raw receipt schema diverged")
                venue = Venue(str(raw_receipt.get("venue") or ""))
                ordinal = raw_receipt.get("ordinal")
                frames = raw_receipt.get("frames")
                hashes = tuple(
                    str(raw_receipt.get(key) or "")
                    for key in (
                        "probe_binding_sha256",
                        "raw_manifest_sha256",
                        "raw_root_sha256",
                        "terminal_result_sha256",
                    )
                )
                if (
                    type(ordinal) is not int
                    or ordinal < 0
                    or type(frames) is not int
                    or frames <= 0
                    or any(
                        len(item) != 64
                        or any(character not in "0123456789abcdef" for character in item)
                        for item in hashes
                    )
                ):
                    raise ValueError("public prediction nonreplayable raw identity diverged")
                slot = (venue, ordinal)
                if slot in nonreplayable_receipt_slots:
                    raise ValueError("public prediction nonreplayable raw slot is duplicated")
                nonreplayable_receipt_slots.add(slot)
                nonreplayable_order.append((venue.value, ordinal, hashes[0]))
            if nonreplayable_order != sorted(nonreplayable_order):
                raise ValueError("public prediction nonreplayable raw receipts are not canonical")
            economic_complete = (
                not missing_any
                and not excluded_coverage
                and not nonreplayable_coverage
            )
            if (
                raw_coverage != receipt_slots
                or excluded_coverage != excluded_receipt_slots
                or nonreplayable_coverage != nonreplayable_receipt_slots
                or coverage.get("schedule_accounted") is not (not missing_any)
                or coverage.get("economic_corpus_complete") is not economic_complete
            ):
                raise ValueError("public prediction slot coverage diverges from raw receipts")
        opportunity_ids = tuple(item.opportunity_id for item in self.opportunities)
        if len(set(opportunity_ids)) != len(opportunity_ids):
            raise ValueError("prediction campaign contains duplicate opportunities")
        if any(
            item.candidate_config_sha256 != self.candidate_config_sha256
            or item.campaign_manifest_sha256 != self.campaign_manifest_sha256
            or item.runner_policy_sha256 != self.runner_policy_sha256
            or item.dataset_bundle_sha256 != self.dataset_sha256
            or item.semantic_catalog_sha256 != self.semantic_catalog_sha256
            or any(
                signal.dataset_sha256 not in self.child_dataset_sha256s
                for signal in item.signals
            )
            for item in self.opportunities
        ):
            raise ValueError("prediction campaign opportunity binding diverged")
        if any(
            signal.received_time_utc_ns >= self.evidence_cutoff_utc_ns_exclusive
            for opportunity in self.opportunities
            for signal in opportunity.signals
        ):
            raise ValueError("prediction campaign materialized holdout signal evidence")
        candidate_ids = tuple(
            item.opportunity_id
            for item in self.opportunities
            if item.status is PredictionOpportunityStatus.CANDIDATE
        )
        report_ids = tuple(item.opportunity_id for item in self.reports)
        if report_ids != candidate_ids or len(set(report_ids)) != len(report_ids):
            raise ValueError("prediction campaign report set is not exhaustive")
        if any(
            item.candidate_config_sha256 != self.candidate_config_sha256
            or item.campaign_manifest_sha256 != self.campaign_manifest_sha256
            or item.dataset_synthetic != self.dataset_synthetic
            or item.dataset_sha256 not in {*self.child_dataset_sha256s, self.dataset_sha256}
            for item in self.reports
        ):
            raise ValueError("prediction campaign top-level report binding diverged")
        if any(
            item.signal_time_utc_ns >= self.evidence_cutoff_utc_ns_exclusive
            or item.max_evidence_received_utc_ns
            >= self.evidence_cutoff_utc_ns_exclusive
            for item in self.reports
        ):
            raise ValueError("prediction campaign report crosses the sealed holdout cutoff")
        if any(
            isinstance(item, PredictionGhostReport)
            and self._opportunity_by_id(item.opportunity_id).family_id
            in {"K4_COMPLETE_SET_LOGICAL_RV", "K6_CROSS_VENUE_EQUIVALENCE"}
            for item in self.reports
        ):
            raise ValueError("prediction campaign cannot evaluate an isolated multi-leg report")

    def _opportunity_by_id(self, opportunity_id: str) -> PredictionOpportunity:
        matches = [item for item in self.opportunities if item.opportunity_id == opportunity_id]
        if len(matches) != 1:
            raise ValueError("prediction campaign opportunity lookup is ambiguous")
        return matches[0]

    @property
    def opportunity_ids(self) -> tuple[str, ...]:
        return tuple(item.opportunity_id for item in self.opportunities)

    @property
    def candidate_opportunity_ids(self) -> tuple[str, ...]:
        return tuple(
            item.opportunity_id
            for item in self.opportunities
            if item.status is PredictionOpportunityStatus.CANDIDATE
        )

    @property
    def opportunity_set_sha256(self) -> str:
        return hashlib.sha256(
            canonical_json_bytes([item.to_dict() for item in self.opportunities])
        ).hexdigest()

    def _body(self) -> dict[str, Any]:
        return {
            "boundary": BOUNDARY,
            "campaign_manifest_sha256": self.campaign_manifest_sha256,
            "candidate_config_sha256": self.candidate_config_sha256,
            "child_dataset_sha256s": list(self.child_dataset_sha256s),
            "collection_receipts": [item.to_dict() for item in self.collection_receipts],
            "dataset_sha256": self.dataset_sha256,
            "dataset_synthetic": self.dataset_synthetic,
            "economic_claim": "NONE_RESEARCH_MECHANISM_ONLY",
            "evidence_cutoff_utc_ns_exclusive": self.evidence_cutoff_utc_ns_exclusive,
            "model_version": "PREDICTION_CAMPAIGN_GHOST_REPLAY_V1",
            "opportunities": [item.to_dict() for item in self.opportunities],
            "opportunity_set_sha256": self.opportunity_set_sha256,
            "prospective_slot_coverage": self.prospective_slot_coverage,
            "reports": [item.to_dict() for item in self.reports],
            "replay_seal_sha256": self.replay_seal_sha256,
            "runner_policy_sha256": self.runner_policy_sha256,
            "selection_split_view": dict(self.selection_split_view),
            "selection_view_sha256": hashlib.sha256(
                canonical_json_bytes(self.selection_split_view)
            ).hexdigest(),
            "schema_version": 1,
            "semantic_catalog_sha256": self.semantic_catalog_sha256,
            "status": "COMPLETE_EXHAUSTIVE_REPLAY",
        }

    @property
    def report_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self._body())).hexdigest()

    @property
    def replay_verified(self) -> bool:
        return (
            self._verification_token is _CAMPAIGN_REPORT_VERIFICATION_TOKEN
            and self._verified_report_sha256 == self.report_sha256
            and all(item.replay_verified for item in self.opportunities)
            and all(item.replay_verified for item in self.reports)
        )

    def to_dict(self) -> dict[str, Any]:
        return {**self._body(), "report_sha256": self.report_sha256}


class PredictionGhostReplay:
    """Deterministic public-data replay with no transport or order surface."""

    def __init__(
        self,
        *,
        raw_root: Path,
        manifest_sha256: str,
        dataset: PredictionPointInTimeDataset,
        preregistration: CandidatePreregistration,
        contracts: Mapping[Venue, OfficialPublicContract],
        collection_binding: PredictionCollectionBinding | None,
        semantic_catalog: SemanticCatalog,
        identity_graphs: Sequence[PredictionIdentityGraph],
        fee_schedules: Mapping[str, PredictionFeeSchedule],
        tick_grids: Mapping[str, PredictionTickGrid],
        maximum_book_age_ns: int,
        graph_observations: Sequence[PredictionIdentityGraph] | None = None,
        prospective_shard_ordinal: int | None = None,
        _direct_synthetic_fixture: bool = False,
    ) -> None:
        if maximum_book_age_ns <= 0:
            raise ValueError("prediction Ghost book age must be positive")
        if maximum_book_age_ns != preregistration.runner_policy.maximum_book_age_ns:
            raise ValueError("prediction Ghost book age diverges from preregistration")
        self._contracts = dict(contracts)
        self._collection_binding = collection_binding
        reader = ResearchSegmentReader(raw_root, manifest_sha256=manifest_sha256)
        self._raw_index = PredictionRawEvidenceIndex(reader, contracts=self._contracts)
        self._manifest_sha256 = self._raw_index.manifest_sha256
        self._root_sha256 = self._raw_index.root_sha256
        if (
            dataset.identity.raw_manifest_sha256 != self._manifest_sha256
            or dataset.identity.raw_root_sha256 != self._root_sha256
            or not dataset.rows
        ):
            raise ValueError("prediction Ghost dataset diverged from authenticated manifest")
        self._dataset = dataset
        self._dataset_sha256 = dataset.dataset_sha256
        if dataset.synthetic:
            if prospective_shard_ordinal is not None:
                raise ValueError("synthetic prediction Ghost cannot claim a campaign shard ordinal")
        elif type(prospective_shard_ordinal) is not int or prospective_shard_ordinal < 0:
            raise ValueError("public prediction Ghost requires an authenticated shard ordinal")
        self._prospective_shard_ordinal = prospective_shard_ordinal
        self._preregistration = preregistration
        if (
            dataset.candidate_config_sha256 is not None
            and dataset.candidate_config_sha256 != preregistration.config_sha256
        ):
            raise ValueError("prediction dataset candidate binding diverged from preregistration")
        self._semantic_catalog = semantic_catalog
        if dataset.semantic_catalog_sha256 != semantic_catalog.catalog_sha256:
            raise ValueError("prediction Ghost semantic catalog binding diverged")
        self._identity_graphs = tuple(identity_graphs)
        graphs_by_rule = {
            (item.venue, item.market_id, item.rule_version.version_id): item
            for item in self._identity_graphs
        }
        if len(graphs_by_rule) != len(self._identity_graphs):
            raise ValueError("prediction Ghost identity graph rule versions are ambiguous")
        self._graphs_by_rule = graphs_by_rule
        self._graph_observations = tuple(
            self._identity_graphs if graph_observations is None else graph_observations
        )
        graphs_by_observation: dict[
            tuple[Venue, str, str, str], PredictionIdentityGraph
        ] = {}
        for graph in self._graph_observations:
            observation_key = (
                graph.venue,
                graph.market_id,
                graph.rule_version.version_id,
                graph.raw_graph_sha256,
            )
            if observation_key in graphs_by_observation:
                raise ValueError("prediction Ghost graph observation is duplicated")
            representative = graphs_by_rule.get(observation_key[:3])
            if representative is None:
                raise ValueError(
                    "prediction Ghost graph observation lacks a semantic representative"
                )
            representative.assert_compatible_successor(
                graph,
                explicit_rule_version_transition=False,
            )
            graphs_by_observation[observation_key] = graph
        if any(
            representative not in self._graph_observations
            for representative in self._identity_graphs
        ):
            raise ValueError("prediction Ghost semantic representative is not a raw observation")
        self._graphs_by_observation = graphs_by_observation
        graphs_by_market: dict[tuple[Venue, str], tuple[PredictionIdentityGraph, ...]] = {}
        for graph in self._identity_graphs:
            market_key = (graph.venue, graph.market_id)
            graphs_by_market[market_key] = (*graphs_by_market.get(market_key, ()), graph)
        self._graphs_by_market = graphs_by_market
        for graph in self._graph_observations:
            revalidate_prediction_graph(self._raw_index, graph)
        self._graphs_by_point_in_time: dict[str, PredictionIdentityGraph] = {}
        for row in dataset.rows:
            if row.graph_observation_sha256 is None:
                if not _direct_synthetic_fixture:
                    raise ValueError("prediction Ghost row lacks a graph observation identity")
                continue
            selected_graph = graphs_by_observation.get(
                (
                    row.venue,
                    row.market_id,
                    row.rule_version_id,
                    row.graph_observation_sha256,
                )
            )
            if selected_graph is None:
                raise ValueError("prediction Ghost row graph observation is absent")
            self._graphs_by_point_in_time[row.point_in_time_id] = selected_graph
        self._fee_schedules = dict(fee_schedules)
        for schedule in self._fee_schedules.values():
            schedule_graphs = graphs_by_market.get((schedule.venue, schedule.market_id), ())
            schedule_graph = next(
                (
                    graph
                    for graph in reversed(schedule_graphs)
                    if tuple(item.outcome_id for item in graph.outcomes)
                    == schedule.outcome_ids
                ),
                None,
            )
            if schedule_graph is None and not _direct_synthetic_fixture:
                raise ValueError("prediction Ghost fee schedule lacks an identity graph")
            if schedule_graph is None:
                continue
            revalidate_prediction_fee_schedule(
                self._raw_index,
                schedule,
                contract=self._contracts[schedule.venue],
                graph=schedule_graph,
            )
        self._tick_grids = dict(tick_grids)
        if not _direct_synthetic_fixture:
            fee_by_market: dict[str, list[PredictionFeeSchedule]] = {}
            for schedule in self._fee_schedules.values():
                fee_by_market.setdefault(schedule.market_id, []).append(schedule)
            tick_by_market: dict[str, list[PredictionTickGrid]] = {}
            for grid in self._tick_grids.values():
                tick_by_market.setdefault(grid.market_id, []).append(grid)
            revalidate_prediction_dataset(
                dataset,
                raw_root=raw_root,
                manifest_sha256=manifest_sha256,
                contracts=self._contracts,
                semantic_catalog=semantic_catalog,
                graphs=self._graph_observations,
                fee_schedules={key: tuple(value) for key, value in fee_by_market.items()},
                tick_grids={key: tuple(value) for key, value in tick_by_market.items()},
                collection_binding=collection_binding,
            )
        elif not dataset.synthetic or any(
            item.provenance.fixture_label != SYNTHETIC_FIXTURE_LABEL
            or item.feed_type != "ghost_fixture"
            for item in self._raw_index.envelopes
        ):
            raise ValueError("direct prediction fixture bypass is synthetic ghost_fixture only")
        self._dataset_rows = frozenset(dataset.rows)
        self._maximum_book_age_ns = maximum_book_age_ns

    def _graph_for_snapshot(
        self,
        snapshot: PredictionDepthSnapshot,
    ) -> PredictionIdentityGraph | None:
        graph = self._graphs_by_point_in_time.get(snapshot.point_in_time_id)
        if graph is not None:
            return graph
        return self._graphs_by_rule.get(
            (snapshot.venue, snapshot.market_id, snapshot.rule_version_id)
        )

    def _validate_settlement_raw(
        self,
        settlement: PredictionSettlementEvidence,
        graph: PredictionIdentityGraph,
    ) -> None:
        envelope, record = self._raw_index.require_record(
            settlement.raw_ref,
            venue=settlement.venue,
            allowed_feeds=(
                "events",
                "ghost_fixture",
                "historical_markets",
                "market_batch",
                "market_lifecycle",
                "markets",
                "metadata",
            ),
        )
        expected_source_time = _expected_settlement_source_time_ns(
            envelope,
            record,
            settlement.state,
        )
        if (
            settlement.raw_manifest_sha256 != self._manifest_sha256
            or settlement.raw_root_sha256 != self._root_sha256
            or settlement.received_time_utc_ns != envelope.receive_timestamp_utc_ns
            or settlement.received_monotonic_ns != envelope.receive_monotonic_ns
            or settlement.source_event_time_ns != expected_source_time
            or settlement.collector_identity != envelope.collector_identity
            or settlement.session_identity != envelope.session_identity
            or settlement.source_url != envelope.provenance.source_url
            or settlement.source_event_id
            not in {
                settlement.raw_ref.raw_record_sha256,
                str(envelope.source_event_id or settlement.raw_ref.raw_record_sha256),
            }
        ):
            raise ValueError("prediction settlement clocks or provenance diverged from raw")
        if (
            graph.venue is not settlement.venue
            or graph.market_id != settlement.market_id
            or settlement.outcome_id not in {item.outcome_id for item in graph.outcomes}
            or settlement.resolution_rule_version_id != graph.rule_version.version_id
        ):
            raise ValueError("prediction settlement graph identity diverged")
        if settlement.synthetic_fixture:
            if envelope.provenance.fixture_label != SYNTHETIC_FIXTURE_LABEL:
                raise ValueError("synthetic settlement lacks fixture provenance")
            raw_payout = (
                None if record.get("payout") is None else Decimal(str(record.get("payout")))
            )
            if (
                str(record.get("outcome_id") or "") != settlement.outcome_id
                or str(record.get("state") or "") != settlement.state.value
                or raw_payout != settlement.payout_per_contract
            ):
                raise ValueError("synthetic settlement diverged from explicit fixture raw")
            return
        if (
            settlement.classification is not EvidenceClassification.OBSERVED_PUBLICLY
            or envelope.provenance.transport == "FIXTURE"
        ):
            raise ValueError("public settlement is not observed public evidence")
        if settlement.venue is Venue.POLYMARKET:
            if settlement.state in _TERMINAL_STATES:
                raw_market = str(
                    record.get("market")
                    or record.get("conditionId")
                    or record.get("condition_id")
                    or ""
                )
                if raw_market != settlement.market_id:
                    raise ValueError("Polymarket settlement market identity diverged")
                graph_outcomes = {item.outcome_id for item in graph.outcomes}
                if settlement.state is PredictionSettlementState.RESOLVED_50_50:
                    payouts = record.get("payouts")
                    if not isinstance(payouts, Mapping) or {
                        str(key): Decimal(str(value)) for key, value in payouts.items()
                    } != {item: Decimal("0.5") for item in graph_outcomes}:
                        raise ValueError("Polymarket 50/50 settlement lacks exact public payouts")
                    return
                winner = str(record.get("winning_asset_id") or record.get("winningAssetId") or "")
                if (
                    record.get("event_type") != "market_resolved"
                    or winner not in graph_outcomes
                ):
                    raise ValueError("Polymarket terminal settlement lacks resolution event")
                expected = _ONE if winner == settlement.outcome_id else _ZERO
                if settlement.payout_per_contract != expected:
                    raise ValueError("Polymarket terminal payout diverged from resolution event")
            elif settlement.payout_per_contract is not None:
                raise ValueError("Polymarket non-terminal settlement cannot carry payout")
            else:
                if record.get("event_type") == "market_resolved":
                    raise ValueError("Polymarket resolution event cannot be non-terminal")
                expected_state = (
                    PredictionSettlementState.CLOSED_UNRESOLVED
                    if record.get("closed") is True
                    else PredictionSettlementState.TRADING
                )
                if settlement.state is not expected_state:
                    raise ValueError("Polymarket lifecycle state diverged from raw")
            return
        status = str(record.get("status") or "").lower()
        result = str(record.get("result") or "").lower()
        if str(record.get("ticker") or "") != settlement.market_id:
            raise ValueError("Kalshi settlement market ticker diverged")
        if settlement.state in _TERMINAL_STATES:
            if status != "finalized" or result not in {"yes", "no"}:
                raise ValueError("Kalshi terminal settlement lacks finalized binary result")
            try:
                raw_settlement = Decimal(str(record.get("settlement_value_dollars")))
            except Exception as error:
                raise ValueError("Kalshi terminal settlement value is invalid") from error
            expected_raw_settlement = _ONE if result == "yes" else _ZERO
            if (
                not raw_settlement.is_finite()
                or raw_settlement != expected_raw_settlement
                or record.get("settlement_ts") is None
                or _prediction_record_source_time_ns(envelope, record) is None
            ):
                raise ValueError("Kalshi terminal result, payout, or timestamp diverged")
            yes_wins = result == "yes"
            outcome_yes = settlement.outcome_id.endswith(":YES")
            expected = _ONE if yes_wins == outcome_yes else _ZERO
            if settlement.payout_per_contract != expected:
                raise ValueError("Kalshi payout diverged from finalized result")
        else:
            expected_state = {
                "active": PredictionSettlementState.TRADING,
                "amended": PredictionSettlementState.AMENDED,
                "closed": PredictionSettlementState.CLOSED_UNRESOLVED,
                "determined": PredictionSettlementState.DETERMINED,
                "disputed": PredictionSettlementState.DISPUTED,
                "finalized": PredictionSettlementState.CLOSED_UNRESOLVED,
                "inactive": PredictionSettlementState.CLOSED_UNRESOLVED,
                "initialized": PredictionSettlementState.TRADING,
            }.get(status, PredictionSettlementState.CLOSED_UNRESOLVED)
            if settlement.state is not expected_state or settlement.payout_per_contract is not None:
                raise ValueError("Kalshi non-terminal lifecycle state diverged from raw")

    def _validate_aggressor_raw(
        self,
        evidence: MakerAggressorEvidence,
        graph: PredictionIdentityGraph,
    ) -> None:
        envelope, record = self._raw_index.require_record(
            evidence.raw_ref,
            venue=evidence.venue,
            allowed_feeds=("ghost_fixture", "last_trade_price", "trades"),
        )
        if (
            evidence.raw_manifest_sha256 != self._manifest_sha256
            or evidence.raw_root_sha256 != self._root_sha256
            or evidence.received_time_utc_ns != envelope.receive_timestamp_utc_ns
            or evidence.received_monotonic_ns != envelope.receive_monotonic_ns
            or evidence.source_event_time_ns
            != _prediction_record_source_time_ns(envelope, record)
            or evidence.collector_identity != envelope.collector_identity
            or evidence.session_identity != envelope.session_identity
            or evidence.source_url != envelope.provenance.source_url
        ):
            raise ValueError("maker aggressor clocks or provenance diverged from raw")
        if (
            graph.venue is not evidence.venue
            or graph.market_id != evidence.market_id
            or evidence.outcome_id not in {item.outcome_id for item in graph.outcomes}
        ):
            raise ValueError("maker aggressor graph identity diverged")
        if envelope.provenance.fixture_label == SYNTHETIC_FIXTURE_LABEL:
            return
        if evidence.venue is Venue.POLYMARKET:
            if envelope.feed_type != "last_trade_price" or envelope.source_timestamp_ns is None:
                raise ValueError("Polymarket REST trade time cannot prove a maker fill")
            side = str(record.get("side") or "").upper()
            size = Decimal(str(record.get("size")))
            price = Decimal(str(record.get("price")))
            market = str(record.get("conditionId") or record.get("market") or "")
            asset = str(record.get("asset") or record.get("asset_id") or "")
            trade_id = str(
                record.get("transactionHash")
                or record.get("trade_id")
                or envelope.source_event_id
                or ""
            )
            if (side, size, price, market, asset, trade_id) != (
                evidence.aggressor_side,
                evidence.quantity,
                evidence.price,
                evidence.market_id,
                evidence.outcome_id,
                evidence.source_trade_id,
            ):
                raise ValueError("Polymarket maker evidence diverged from trade record")
            return
        if bool(record.get("is_block_trade")):
            raise ValueError("Kalshi block trade cannot prove LOB maker fill")
        outcome_side = str(record.get("taker_outcome_side") or "").upper()
        book_side = str(record.get("taker_book_side") or "").upper()
        side_map = {"ASK": "BUY", "BUY": "BUY", "BID": "SELL", "SELL": "SELL"}
        expected_side = side_map.get(book_side)
        expected_outcome_side = "YES" if evidence.outcome_id.endswith(":YES") else "NO"
        raw_price = (
            record.get("yes_price_dollars")
            if expected_outcome_side == "YES"
            else record.get("no_price_dollars")
        )
        if raw_price is None and record.get("yes_price_dollars") is not None:
            raw_price = _ONE - Decimal(str(record.get("yes_price_dollars")))
        if (
            outcome_side != expected_outcome_side
            or expected_side is None
            or evidence.aggressor_side != expected_side
            or evidence.source_trade_id != str(record.get("trade_id") or "")
            or evidence.market_id != str(record.get("ticker") or "")
            or evidence.price != Decimal(str(raw_price))
            or evidence.quantity != Decimal(str(record.get("count_fp")))
        ):
            raise ValueError("Kalshi maker evidence side or trade identity diverged")

    def run(
        self,
        *,
        intent: PredictionOrderIntent,
        settlement: PredictionSettlementEvidence,
        report_time_utc_ns: int,
        report_time_monotonic_ns: int,
        maker_aggressor: MakerAggressorEvidence | None = None,
        _group_execution_token: object | None = None,
        _opportunity_execution_token: object | None = None,
        _opportunity: PredictionOpportunity | None = None,
        _settlement_validation_engine: PredictionGhostReplay | None = None,
    ) -> PredictionGhostReport:
        snapshots = tuple(
            item
            for item in self._dataset.rows
            if item.venue is intent.venue
            and item.market_id == intent.market_id
            and item.outcome_id == intent.outcome_id
        )
        if not snapshots:
            raise ValueError("prediction Ghost requires an authenticated canonical book stream")
        if not self._dataset.synthetic and (
            _opportunity_execution_token is not _OPPORTUNITY_EXECUTION_TOKEN
            or _opportunity is None
            or not _opportunity.replay_verified
            or _opportunity.opportunity_id != intent.opportunity_id
            or not intent.order_id.startswith(f"{intent.opportunity_id}:LEG:")
        ):
            raise ValueError("public prediction execution requires a canonical opportunity")
        if (
            intent.candidate_config_sha256 != self._preregistration.config_sha256
            or intent.signal_dataset_sha256 != self._dataset_sha256
            or intent.campaign_manifest_sha256 != self._dataset.campaign_manifest_sha256
            or intent.collection_probe_binding_sha256
            != self._dataset.collection_probe_binding_sha256
            or intent.collection_terminal_result_sha256
            != self._dataset.collection_terminal_result_sha256
        ):
            raise ValueError("prediction intent preregistration or dataset binding diverged")
        variant = self._preregistration.require_variant(
            intent.variant_id,
            parameters_sha256=intent.variant_parameters_sha256,
        )
        policy = PredictionVariantExecutionPolicy.from_variant(variant)
        if not self._dataset.synthetic:
            if (
                policy.family_id
                in {"K4_COMPLETE_SET_LOGICAL_RV", "K6_CROSS_VENUE_EQUIVALENCE"}
                and _group_execution_token is not _GROUP_EXECUTION_TOKEN
            ):
                raise ValueError("prediction multi-leg variant requires the canonical group runner")
            if (
                policy.family_id == "K5_INCENTIVE_QUEUE"
                and intent.role is not PredictionExecutionRole.MAKER
            ):
                raise ValueError("prediction single-leg public execution role is not preregistered")
        admission_time_utc_ns = intent.decision_time_utc_ns + policy.latency_ns
        admission_monotonic_ns = intent.decision_monotonic_ns + policy.latency_ns
        ordered = tuple(
            sorted(
                snapshots,
                key=lambda item: (item.received_monotonic_ns, item.arrival_sequence),
            )
        )
        if tuple(snapshots) != ordered:
            raise ValueError("prediction Ghost book stream must be in causal arrival order")
        stream_identity = {
            (
                item.venue,
                item.event_id,
                item.market_id,
                item.outcome_id,
                item.raw_manifest_sha256,
                item.raw_root_sha256,
            )
            for item in ordered
        }
        if len(stream_identity) != 1:
            raise ValueError("prediction Ghost book stream mixed identity or provenance")
        signal_candidates = [
            item
            for item in ordered
            if item.point_in_time_id == intent.signal_point_in_time_id
            and item.arrival_sequence == intent.signal_arrival_sequence
            and item.received_monotonic_ns == intent.signal_received_monotonic_ns
        ]
        if len(signal_candidates) != 1:
            raise ValueError("PREDICTION_SIGNAL_BOOK_BINDING_DIVERGED")
        signal_snapshot = signal_candidates[0]
        signal_clock = (
            signal_snapshot.collector_identity,
            signal_snapshot.session_identity,
        )
        if any(
            envelope.venue is signal_snapshot.venue
            and envelope.collector_identity == signal_snapshot.collector_identity
            and envelope.arrival_sequence > signal_snapshot.arrival_sequence
            and envelope.receive_monotonic_ns <= admission_monotonic_ns
            and envelope.receive_timestamp_utc_ns <= admission_time_utc_ns
            and (
                envelope.session_identity != signal_snapshot.session_identity
                or envelope.state.gap_detected
                or envelope.state.reconnect
            )
            for envelope in self._raw_index.envelopes
        ):
            raise ValueError("PREDICTION_CONTINUITY_CHANGED_BETWEEN_SIGNAL_AND_ADMISSION")
        clock_ordered = tuple(
            item
            for item in ordered
            if (item.collector_identity, item.session_identity) == signal_clock
        )
        runner_policy = self._preregistration.runner_policy
        trigger_signal = (
            None
            if _opportunity is None
            else next(
                (
                    item
                    for item in _opportunity.signals
                    if item.point_in_time_id == _opportunity.trigger_point_in_time_id
                ),
                None,
            )
        )
        try:
            leg_index = int(intent.order_id.rsplit(":LEG:", 1)[1])
        except (IndexError, ValueError) as error:
            if not self._dataset.synthetic:
                raise ValueError("public prediction order id lacks its runner leg index") from error
            leg_index = 0
        if leg_index < 0:
            raise ValueError("prediction runner leg index cannot be negative")
        scheduled_delay_ns = runner_policy.decision_delay_ns + leg_index * (
            policy.latency_ns + 1
        )
        expected_limit = (
            signal_snapshot.bids[0].price
            if intent.role is PredictionExecutionRole.MAKER
            else signal_snapshot.asks[0].price
        )
        if not self._dataset.synthetic and (
            trigger_signal is None
            or intent.signal_time_utc_ns != trigger_signal.received_time_utc_ns
            or intent.signal_received_monotonic_ns != trigger_signal.received_monotonic_ns
            or signal_snapshot.received_time_utc_ns > intent.signal_time_utc_ns
            or signal_snapshot.received_monotonic_ns > intent.signal_received_monotonic_ns
            or intent.decision_time_utc_ns
            != intent.signal_time_utc_ns + scheduled_delay_ns
            or intent.decision_monotonic_ns
            != intent.signal_received_monotonic_ns + scheduled_delay_ns
            or intent.quantity != runner_policy.quantity
            or intent.limit_price != expected_limit
        ):
            raise ValueError("public prediction intent was not derived by the preregistered runner")
        admission_candidates = [
            item
            for item in clock_ordered
            if item.received_monotonic_ns <= admission_monotonic_ns
            and item.received_time_utc_ns <= admission_time_utc_ns
            and item.arrival_sequence >= signal_snapshot.arrival_sequence
        ]
        if not admission_candidates:
            raise ValueError("PREDICTION_ADMISSION_HAS_NO_CAUSAL_BOOK")
        snapshot = admission_candidates[-1]
        if (
            intent.venue is not snapshot.venue
            or intent.market_id != snapshot.market_id
            or intent.outcome_id != snapshot.outcome_id
        ):
            raise ValueError("prediction Ghost intent/book identity mismatch")
        if signal_snapshot.rule_version_id != snapshot.rule_version_id:
            raise ValueError("PREDICTION_RULE_CHANGED_BETWEEN_SIGNAL_AND_ADMISSION")
        signal_snapshot.tick_grid.assert_price(intent.limit_price)
        snapshot.tick_grid.assert_price(intent.limit_price)
        if intent.role is PredictionExecutionRole.TAKER:
            maximum_limit = signal_snapshot.asks[0].price * (
                _ONE + policy.slippage_bps / Decimal("10000")
            )
            if intent.limit_price > maximum_limit:
                raise ValueError("PREDICTION_LIMIT_EXCEEDS_PREREGISTERED_SLIPPAGE")
        signal_snapshot.assert_causal_decision(
            signal_time_utc_ns=intent.signal_time_utc_ns,
            signal_monotonic_ns=intent.signal_received_monotonic_ns,
            decision_time_utc_ns=intent.decision_time_utc_ns,
            decision_monotonic_ns=intent.decision_monotonic_ns,
            maximum_age_ns=self._maximum_book_age_ns,
        )
        if not snapshot.execution_eligible:
            raise ValueError("PREDICTION_ADMISSION_BOOK_NOT_EXECUTION_ELIGIBLE")
        utc_age = admission_time_utc_ns - snapshot.received_time_utc_ns
        monotonic_age = admission_monotonic_ns - snapshot.received_monotonic_ns
        if (
            utc_age < 0
            or monotonic_age < 0
            or utc_age > self._maximum_book_age_ns
            or monotonic_age > self._maximum_book_age_ns
        ):
            raise ValueError("PREDICTION_BOOK_STALE_AT_ADMISSION")
        semantic_feeds = {
            "event_fee_changes",
            "event_metadata",
            "events",
            "fee_changes",
            "fees",
            "historical_markets",
            "markets",
            "metadata",
            "market_lifecycle",
            "series",
            "tick_size",
            "tick_size_change",
        }
        snapshot_position = (snapshot.arrival_sequence, snapshot.raw_record_index)
        semantic_batch_events = {"market_resolved", "new_market", "tick_size_change"}
        semantic_update_seen = False
        for envelope in self._raw_index.envelopes:
            if (
                (envelope.collector_identity, envelope.session_identity) != signal_clock
                or envelope.receive_monotonic_ns > admission_monotonic_ns
                or envelope.receive_timestamp_utc_ns > admission_time_utc_ns
            ):
                continue
            for raw_record_index, raw_record in enumerate(prediction_raw_records(envelope)):
                position = (envelope.arrival_sequence, raw_record_index)
                if position <= snapshot_position:
                    continue
                if envelope.feed_type in semantic_feeds or (
                    envelope.feed_type == "market_batch"
                    and raw_record.get("event_type") in semantic_batch_events
                ):
                    semantic_update_seen = True
                    break
            if semantic_update_seen:
                break
        if semantic_update_seen:
            raise ValueError("PREDICTION_SEMANTIC_UPDATE_WITHOUT_FRESH_BOOK")
        fee_schedule = self._fee_schedules.get(snapshot.fee_schedule_id)
        if fee_schedule is None:
            raise ValueError("PREDICTION_FEE_SCHEDULE_MISSING_FAIL_CLOSED")
        if fee_schedule.venue is not snapshot.venue:
            raise ValueError("prediction fee schedule venue mismatch")
        fee_schedule.assert_effective_at(admission_time_utc_ns)
        if not fee_schedule.exact_usable:
            raise ValueError("PREDICTION_FEE_UNKNOWN_FAIL_CLOSED")
        if (
            settlement.venue is not snapshot.venue
            or settlement.market_id != snapshot.market_id
            or settlement.outcome_id != snapshot.outcome_id
            or settlement.rule_version_id != snapshot.rule_version_id
        ):
            raise ValueError("prediction settlement identity or rule version mismatch")
        entry_graph = self._graph_for_snapshot(snapshot)
        resolution_graph: PredictionIdentityGraph | None = None
        if entry_graph is None and not self._dataset.synthetic:
            raise ValueError("prediction Ghost settlement lacks an authenticated graph")
        if entry_graph is not None:
            close_ns = _prediction_close_time_ns(entry_graph.rule_version.closes_at)
            if close_ns is None or admission_time_utc_ns >= close_ns:
                raise ValueError("PREDICTION_ADMISSION_AT_OR_AFTER_MARKET_CLOSE")
            validation_engine = _settlement_validation_engine or self
            resolution_graph = validation_engine._graphs_by_rule.get(
                (
                    snapshot.venue,
                    snapshot.market_id,
                    settlement.resolution_rule_version_id,
                )
            )
            if resolution_graph is None:
                raise ValueError("prediction resolution rule graph is absent")
            entry_graph.assert_compatible_successor(
                resolution_graph,
                explicit_rule_version_transition=(
                    entry_graph.rule_version.version_id
                    != resolution_graph.rule_version.version_id
                ),
            )
            if (
                entry_graph.rule_version.rule_text
                != resolution_graph.rule_version.rule_text
                or entry_graph.rule_version.resolution_source
                != resolution_graph.rule_version.resolution_source
                or entry_graph.rule_version.opens_at
                != resolution_graph.rule_version.opens_at
                or entry_graph.rule_version.closes_at
                != resolution_graph.rule_version.closes_at
                or entry_graph.rule_version.source_metadata_version
                != resolution_graph.rule_version.source_metadata_version
            ):
                raise ValueError("PREDICTION_RESOLUTION_RULE_LINEAGE_CHANGED_FAIL_CLOSED")
            if validation_engine is self and max(
                item.arrival_sequence for item in resolution_graph.source_refs
            ) < max(item.arrival_sequence for item in entry_graph.source_refs):
                raise ValueError("prediction resolution rule predates entry rule")
            validation_engine._validate_settlement_raw(settlement, resolution_graph)
        elif not settlement.synthetic_fixture:
            raise ValueError("prediction Ghost direct fixture settlement is not synthetic")
        if maker_aggressor is not None:
            if (
                maker_aggressor.collector_identity,
                maker_aggressor.session_identity,
            ) != signal_clock:
                raise ValueError("prediction maker evidence crosses monotonic clock domains")
            if entry_graph is None:
                if not self._dataset.synthetic:
                    raise ValueError("prediction Ghost maker evidence lacks an authenticated graph")
            else:
                self._validate_aggressor_raw(maker_aggressor, entry_graph)
        if report_time_utc_ns < admission_time_utc_ns or report_time_monotonic_ns < admission_monotonic_ns:
            raise ValueError("prediction report time precedes admission")
        settlement_same_clock = (
            settlement.collector_identity,
            settlement.session_identity,
        ) == signal_clock
        if settlement.received_time_utc_ns > report_time_utc_ns or (
            settlement_same_clock
            and settlement.received_monotonic_ns > report_time_monotonic_ns
        ):
            raise ValueError("FUTURE_PREDICTION_SETTLEMENT_EVIDENCE_FORBIDDEN")
        if maker_aggressor is not None and (
            maker_aggressor.received_time_utc_ns > report_time_utc_ns
            or maker_aggressor.received_monotonic_ns > report_time_monotonic_ns
        ):
            raise ValueError("FUTURE_PREDICTION_MAKER_EVIDENCE_FORBIDDEN")
        uncosted_fills = (
            _consume_taker(
                snapshot.asks,
                quantity=intent.quantity,
                limit_price=intent.limit_price,
            )
            if intent.role is PredictionExecutionRole.TAKER
            else _consume_maker(
                snapshot,
                intent,
                maker_aggressor,
                admission_time_utc_ns=admission_time_utc_ns,
                admission_monotonic_ns=admission_monotonic_ns,
            )
        )
        fills = _apply_order_fee(
            uncosted_fills,
            fee_schedule=fee_schedule,
            maker=intent.role is PredictionExecutionRole.MAKER,
        )
        filled = sum((item.quantity for item in fills), _ZERO)
        missed = intent.quantity - filled
        entry_notional = sum((item.price * item.quantity for item in fills), _ZERO)
        fees = sum((item.fee for item in fills), _ZERO)
        average = None if filled == 0 else entry_notional / filled
        best_ask_reference = snapshot.asks[0].price * filled
        finite_depth_slippage = max(entry_notional - best_ask_reference, _ZERO)
        midpoint = (snapshot.bids[0].price + snapshot.asks[0].price) / Decimal("2")
        spread_cost = _ZERO if average is None else max(average - midpoint, _ZERO) * filled
        terminal = settlement.state in _TERMINAL_STATES
        if terminal and (
            settlement.received_time_utc_ns < admission_time_utc_ns
            or (
                settlement_same_clock
                and settlement.received_monotonic_ns < admission_monotonic_ns
            )
        ):
            raise ValueError("prediction settlement predates admission")
        if terminal and settlement_same_clock:
            capital_lock_duration_ns = max(
                settlement.received_monotonic_ns - admission_monotonic_ns,
                0,
            )
        else:
            release_utc_ns = (
                settlement.received_time_utc_ns if terminal else report_time_utc_ns
            )
            capital_lock_duration_ns = max(release_utc_ns - admission_time_utc_ns, 0)
        capital_lock = entry_notional * Decimal(capital_lock_duration_ns)
        payout = (
            None
            if not terminal or settlement.payout_per_contract is None
            else settlement.payout_per_contract * filled
        )
        gross = None if payout is None else payout - entry_notional
        net = None if gross is None else gross - fees
        reconciliation = None if net is None or payout is None else net - (payout - entry_notional - fees)
        unresolved = filled if payout is None else _ZERO
        if filled == 0:
            status = "MISSED"
            reason = (
                "MAKER_FILL_REQUIRES_EXPLICIT_AGGRESSOR_FLOW"
                if intent.role is PredictionExecutionRole.MAKER and maker_aggressor is None
                else "NO_EXECUTABLE_FINITE_DEPTH"
            )
        elif missed > 0:
            status = "PARTIAL"
            reason = "FINITE_DEPTH_OR_PESSIMISTIC_QUEUE_PARTIAL_FILL"
        else:
            status = "FILLED"
            reason = "EXPLICIT_FINITE_DEPTH_OR_AGGRESSOR_EVIDENCE"
        limitations = (
            "NO_AUTOMATIC_MAKER_FILL",
            "QUEUE_PRIMARY_PESSIMISTIC",
            "CAPITAL_LOCKED_UNTIL_OBSERVED_TERMINAL_SETTLEMENT",
            "NO_ACCOUNT_REWARD_IN_PRIMARY_ECONOMICS",
        )
        evidence_content_sha256s = tuple(
            dict.fromkeys(
                (
                    snapshot.raw_content_sha256,
                    signal_snapshot.raw_content_sha256,
                    settlement.raw_ref.content_sha256,
                    *(
                        ()
                        if entry_graph is None
                        else tuple(item.content_sha256 for item in entry_graph.source_refs)
                    ),
                    *(
                        ()
                        if resolution_graph is None
                        else tuple(item.content_sha256 for item in resolution_graph.source_refs)
                    ),
                    *(item.content_sha256 for item in fee_schedule.source_refs),
                    *(item.content_sha256 for item in snapshot.tick_grid.source_refs),
                    *(
                        ()
                        if maker_aggressor is None
                        else (maker_aggressor.raw_ref.content_sha256,)
                    ),
                )
            )
        )
        evidence_receive_times = [
            envelope.receive_timestamp_utc_ns
            for envelope in self._raw_index.envelopes
            if envelope.content_sha256 in set(evidence_content_sha256s)
        ]
        evidence_receive_times.append(settlement.received_time_utc_ns)
        if maker_aggressor is not None:
            evidence_receive_times.append(maker_aggressor.received_time_utc_ns)
        report = PredictionGhostReport(
            order_id=intent.order_id,
            opportunity_id=intent.opportunity_id,
            variant_id=intent.variant_id,
            candidate_config_sha256=self._preregistration.config_sha256,
            campaign_manifest_sha256=self._dataset.campaign_manifest_sha256,
            collection_probe_binding_sha256=self._dataset.collection_probe_binding_sha256,
            collection_terminal_result_sha256=self._dataset.collection_terminal_result_sha256,
            variant_parameters_sha256=policy.parameters_sha256,
            dataset_sha256=self._dataset_sha256,
            dataset_synthetic=self._dataset.synthetic,
            venue=intent.venue,
            event_id=snapshot.event_id,
            market_id=intent.market_id,
            outcome_id=intent.outcome_id,
            status=status,
            reason=reason,
            role=intent.role,
            requested_quantity=intent.quantity,
            filled_quantity=filled,
            missed_quantity=missed,
            fills=fills,
            average_price=average,
            entry_notional=entry_notional,
            payout=payout,
            gross_pnl=gross,
            fees=fees,
            net_pnl=net,
            spread_cost=spread_cost,
            finite_depth_slippage=finite_depth_slippage,
            turnover=entry_notional,
            drawdown=None if net is None else max(-net, _ZERO),
            capital_immobilized_notional_ns=capital_lock,
            unresolved_exposure=unresolved,
            settlement_state=settlement.state,
            signal_time_utc_ns=intent.signal_time_utc_ns,
            signal_received_monotonic_ns=intent.signal_received_monotonic_ns,
            decision_time_utc_ns=intent.decision_time_utc_ns,
            decision_monotonic_ns=intent.decision_monotonic_ns,
            admission_time_utc_ns=admission_time_utc_ns,
            admission_monotonic_ns=admission_monotonic_ns,
            report_time_utc_ns=report_time_utc_ns,
            report_time_monotonic_ns=report_time_monotonic_ns,
            max_evidence_received_utc_ns=max(evidence_receive_times),
            limit_price=intent.limit_price,
            fee_schedule_evidence_sha256=fee_schedule.evidence_sha256,
            tick_grid_evidence_sha256=snapshot.tick_grid.evidence_sha256,
            reconciliation_difference=reconciliation,
            raw_manifest_sha256=snapshot.raw_manifest_sha256,
            raw_root_sha256=snapshot.raw_root_sha256,
            raw_content_sha256s=evidence_content_sha256s,
            limitations=limitations,
        )
        return replace(
            report,
            _verification_token=_REPLAY_VERIFICATION_TOKEN,
            _verified_report_sha256=report.report_sha256,
        )

    def run_group(
        self,
        *,
        opportunity: PredictionOpportunity,
        relations: Sequence[SemanticRelation],
        legs: Sequence[
            tuple[
                PredictionOrderIntent,
                PredictionSettlementEvidence,
                MakerAggressorEvidence | None,
            ]
        ],
        report_time_utc_ns: int,
        report_time_monotonic_ns: int,
        settlement_validation_engines: Mapping[str, PredictionGhostReplay] | None = None,
    ) -> PredictionGhostGroupReport:
        if (
            not opportunity.replay_verified
            or opportunity.status is not PredictionOpportunityStatus.CANDIDATE
            or opportunity.family_id != "K4_COMPLETE_SET_LOGICAL_RV"
            or opportunity.candidate_config_sha256 != self._preregistration.config_sha256
            or opportunity.campaign_manifest_sha256
            != self._dataset.campaign_manifest_sha256
            or opportunity.runner_policy_sha256
            != self._preregistration.runner_policy.policy_sha256
            or opportunity.semantic_catalog_sha256 != self._semantic_catalog.catalog_sha256
            or tuple(sorted(item.relation_id for item in relations))
            != opportunity.relation_ids
        ):
            raise ValueError("prediction Ghost group requires a verified canonical opportunity")
        if len(legs) < 2:
            raise ValueError("prediction Ghost group requires at least two legs")
        leg_members = {(intent.venue, intent.market_id, intent.outcome_id) for intent, _, _ in legs}
        if len(leg_members) != len(legs):
            raise ValueError("prediction Ghost group contains duplicate legs")
        if len({intent.quantity for intent, _, _ in legs}) != 1:
            raise ValueError("prediction Ghost complete-set quantities must match")
        if len({intent.variant_id for intent, _, _ in legs}) != 1:
            raise ValueError("prediction Ghost complete set must use one preregistered variant")
        if any(intent.opportunity_id != opportunity.opportunity_id for intent, _, _ in legs):
            raise ValueError("prediction Ghost group leg opportunity binding diverged")
        leg_sequence = tuple(
            f"{intent.venue.value}:{intent.market_id}:{intent.outcome_id}"
            for intent, _settlement, _aggressor in legs
        )
        if (
            opportunity.variant_id != legs[0][0].variant_id
            or opportunity.variant_parameters_sha256
            != legs[0][0].variant_parameters_sha256
            or opportunity.quantity != legs[0][0].quantity
            or any(intent.quantity != opportunity.quantity for intent, _, _ in legs)
            or any(intent.signal_dataset_sha256 != self._dataset_sha256 for intent, _, _ in legs)
            or any(signal.dataset_sha256 != self._dataset_sha256 for signal in opportunity.signals)
            or {signal.member_key for signal in opportunity.signals} != leg_members
            or opportunity.execution_sequence != leg_sequence
            or tuple(
                intent.order_id for intent, _settlement, _aggressor in legs
            )
            != tuple(
                f"{opportunity.opportunity_id}:LEG:{index}"
                for index in range(len(legs))
            )
        ):
            raise ValueError("prediction Ghost group opportunity/leg bindings diverged")
        signals_by_member = {signal.member_key: signal for signal in opportunity.signals}
        if len(signals_by_member) != len(opportunity.signals):
            raise ValueError("prediction Ghost group opportunity signals are ambiguous")
        trigger_signal = next(
            item
            for item in opportunity.signals
            if item.point_in_time_id == opportunity.trigger_point_in_time_id
        )
        for intent, _settlement, _aggressor in legs:
            signal = signals_by_member[(intent.venue, intent.market_id, intent.outcome_id)]
            if (
                intent.signal_point_in_time_id != signal.point_in_time_id
                or intent.signal_arrival_sequence != signal.arrival_sequence
                or intent.signal_time_utc_ns != trigger_signal.received_time_utc_ns
                or intent.signal_received_monotonic_ns
                != trigger_signal.received_monotonic_ns
                or intent.campaign_manifest_sha256
                != opportunity.campaign_manifest_sha256
            ):
                raise ValueError("prediction Ghost group signal schedule diverged")
        group_variant = self._preregistration.require_variant(
            legs[0][0].variant_id,
            parameters_sha256=legs[0][0].variant_parameters_sha256,
        )
        if group_variant.family_id != "K4_COMPLETE_SET_LOGICAL_RV":
            raise ValueError("prediction complete-set runner requires the K4 family")
        signal_graphs: list[PredictionIdentityGraph] = []
        for intent, _settlement, _aggressor in legs:
            signal_rows = [
                row
                for row in self._dataset.rows
                if row.point_in_time_id == intent.signal_point_in_time_id
                and row.market_id == intent.market_id
                and row.outcome_id == intent.outcome_id
            ]
            if len(signal_rows) != 1:
                raise ValueError("prediction Ghost complete-set signal row is ambiguous")
            row = signal_rows[0]
            graph = self._graph_for_snapshot(row)
            if graph is None:
                raise ValueError("prediction Ghost complete set lacks an authenticated graph")
            signal_graphs.append(graph)
        event_ids = {graph.event_id for graph in signal_graphs}
        if len(event_ids) != 1:
            raise ValueError("prediction Ghost complete set must share one event")
        required_relation_types = {
            RelationType.EXHAUSTIVE,
            RelationType.MUTUALLY_EXCLUSIVE,
        }
        admitted_relation_types: set[RelationType] = set()
        for relation in relations:
            if relation not in self._semantic_catalog.relations:
                raise ValueError("prediction Ghost relation is absent from the authenticated catalog")
            relation_members = {
                (item.venue, item.economic_market_id, item.outcome_id) for item in relation.members
            }
            if relation_members != leg_members:
                continue
            if relation.status is not RelationStatus.VERIFIED:
                raise ValueError("prediction Ghost relation is not verified")
            graph_matches = [
                graph
                for graph in set(signal_graphs)
                if {(item.venue, item.economic_market_id, item.outcome_id) for item in graph.outcomes}
                == leg_members
                and tuple(relation.provenance) == graph.source_content_sha256s
            ]
            if len(graph_matches) != 1:
                raise ValueError("prediction Ghost relation is not a single authenticated graph")
            graph = graph_matches[0]
            if (
                relation.relation_type is RelationType.EXHAUSTIVE
                and relation.formal_rule.get("guaranteed_payout_per_unit") != "1"
            ):
                raise ValueError("prediction Ghost exhaustive payout is not explicit")
            if (
                relation.relation_type is RelationType.MUTUALLY_EXCLUSIVE
                and relation.formal_rule.get("simultaneous_winners_max") != 1
            ):
                raise ValueError("prediction Ghost mutual exclusion rule is not explicit")
            admitted_relation_types.add(relation.relation_type)
        if not required_relation_types.issubset(admitted_relation_types):
            raise ValueError("prediction Ghost complete set lacks exhaustive/mutually-exclusive proof")
        payout_per_set = sum(
            (
                settlement.payout_per_contract
                for _intent, settlement, _aggressor in legs
                if settlement.payout_per_contract is not None
            ),
            _ZERO,
        )
        if (
            all(settlement.state in _TERMINAL_STATES for _, settlement, _ in legs)
            and payout_per_set != _ONE
        ):
            raise ValueError("prediction complete-set terminal payouts do not reconcile to one")
        reports: list[PredictionGhostReport] = []
        previous_admission: int | None = None
        for intent, settlement, aggressor in legs:
            if previous_admission is not None and intent.decision_monotonic_ns <= previous_admission:
                raise ValueError("prediction group legs must be causally sequential")
            report = self.run(
                intent=intent,
                settlement=settlement,
                report_time_utc_ns=report_time_utc_ns,
                report_time_monotonic_ns=report_time_monotonic_ns,
                maker_aggressor=aggressor,
                _group_execution_token=_GROUP_EXECUTION_TOKEN,
                _opportunity_execution_token=_OPPORTUNITY_EXECUTION_TOKEN,
                _opportunity=opportunity,
                _settlement_validation_engine=(
                    None
                    if settlement_validation_engines is None
                    else settlement_validation_engines.get(
                        settlement.raw_manifest_sha256
                    )
                ),
            )
            reports.append(report)
            previous_admission = report.admission_monotonic_ns
        worst_fill = min(report.filled_quantity / report.requested_quantity for report in reports)
        all_settled = all(report.net_pnl is not None for report in reports)
        net = (
            None
            if not all_settled
            else sum(
                (report.net_pnl for report in reports if report.net_pnl is not None),
                _ZERO,
            )
        )
        gross = (
            None
            if not all_settled
            else sum(
                (report.gross_pnl for report in reports if report.gross_pnl is not None),
                _ZERO,
            )
        )
        status = "COMPLETE" if worst_fill == _ONE else ("MISSED" if worst_fill == _ZERO else "PARTIAL")
        group_report = PredictionGhostGroupReport(
            order_id=f"{opportunity.opportunity_id}:GROUP",
            opportunity_id=opportunity.opportunity_id,
            family_id=opportunity.family_id,
            variant_id=opportunity.variant_id,
            candidate_config_sha256=self._preregistration.config_sha256,
            campaign_manifest_sha256=self._dataset.campaign_manifest_sha256,
            collection_probe_binding_sha256=self._dataset.collection_probe_binding_sha256,
            collection_terminal_result_sha256=self._dataset.collection_terminal_result_sha256,
            variant_parameters_sha256=opportunity.variant_parameters_sha256,
            dataset_sha256=opportunity.dataset_bundle_sha256,
            child_dataset_sha256s=(self._dataset_sha256,),
            dataset_synthetic=self._dataset.synthetic,
            semantic_catalog_sha256=self._semantic_catalog.catalog_sha256,
            relation_ids=opportunity.relation_ids,
            leg_reports=tuple(reports),
            venue=reports[0].venue,
            market_id=(
                "GROUP:"
                + hashlib.sha256(
                    canonical_json_bytes(
                        {
                            "economic_markets": sorted(
                                {
                                    f"{venue.value}:{market_id}"
                                    for venue, market_id, _outcome_id in leg_members
                                }
                            )
                        }
                    )
                ).hexdigest()
            ),
            outcome_id="COMPLETE_SET",
            signal_time_utc_ns=max(item.signal_time_utc_ns for item in reports),
            max_evidence_received_utc_ns=max(
                item.max_evidence_received_utc_ns for item in reports
            ),
            gross_pnl=gross,
            fees=sum((item.fees for item in reports), _ZERO),
            net_pnl=net,
            spread_cost=sum((item.spread_cost for item in reports), _ZERO),
            finite_depth_slippage=sum(
                (item.finite_depth_slippage for item in reports), _ZERO
            ),
            turnover=sum((item.turnover for item in reports), _ZERO),
            drawdown=None if net is None else max(-net, _ZERO),
            capital_immobilized_notional_ns=sum(
                (item.capital_immobilized_notional_ns for item in reports), _ZERO
            ),
            unresolved_exposure=sum((item.unresolved_exposure for item in reports), _ZERO),
            reconciliation_difference=None if net is None else _ZERO,
            worst_leg_fill_ratio=worst_fill,
            status=status,
            reason="SEQUENTIAL_LEG_RISK_NOT_ASSUMED_SIMULTANEOUS",
        )
        return replace(
            group_report,
            _verification_token=_GROUP_REPORT_VERIFICATION_TOKEN,
            _verified_report_sha256=group_report.report_sha256,
        )


def _prediction_close_time_ns(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return prediction_rfc3339_to_ns(value, label="prediction close time")
    except ValueError:
        return None


def _opportunity_member_text(member: tuple[Venue, str, str]) -> str:
    return f"{member[0].value}:{member[1]}:{member[2]}"


class PredictionCampaignGhostReplay:
    """Exhaustive, deterministic opportunity runner over authenticated datasets."""

    def __init__(
        self,
        engines: Sequence[PredictionGhostReplay],
        *,
        prospective_slot_coverage: Mapping[str, Any] | None = None,
    ) -> None:
        if not engines:
            raise ValueError("prediction campaign replay requires at least one engine")
        self._engines = tuple(
            sorted(
                engines,
                key=lambda item: (
                    item._prospective_shard_ordinal is None,
                    -1
                    if item._prospective_shard_ordinal is None
                    else item._prospective_shard_ordinal,
                    item._manifest_sha256,
                ),
            )
        )
        self._preregistration = self._engines[0]._preregistration
        self._semantic_catalog = self._engines[0]._semantic_catalog
        config_hashes = {item._preregistration.config_sha256 for item in self._engines}
        catalog_hashes = {item._semantic_catalog.catalog_sha256 for item in self._engines}
        campaign_hashes = {item._dataset.campaign_manifest_sha256 for item in self._engines}
        synthetic_values = {item._dataset.synthetic for item in self._engines}
        if (
            config_hashes != {self._preregistration.config_sha256}
            or catalog_hashes != {self._semantic_catalog.catalog_sha256}
            or len(campaign_hashes) != 1
            or len(synthetic_values) != 1
        ):
            raise ValueError("prediction campaign engines mix incompatible bindings")
        self._campaign_manifest_sha256 = next(iter(campaign_hashes))
        self._dataset_synthetic = next(iter(synthetic_values))
        self._prospective_slot_coverage = prospective_slot_coverage
        self._child_dataset_sha256s = tuple(
            sorted({item._dataset_sha256 for item in self._engines})
        )
        if len(self._child_dataset_sha256s) != len(self._engines):
            raise ValueError("prediction campaign engines duplicate a dataset")
        if len(self._child_dataset_sha256s) == 1:
            self._dataset_sha256 = self._child_dataset_sha256s[0]
        else:
            bundle = {
                "campaign_manifest_sha256": self._campaign_manifest_sha256,
                "candidate_config_sha256": self._preregistration.config_sha256,
                "child_dataset_sha256s": list(self._child_dataset_sha256s),
                "model_version": "PREDICTION_DATASET_BUNDLE_V1",
                "runner_policy_sha256": self._preregistration.runner_policy.policy_sha256,
                "semantic_catalog_sha256": self._semantic_catalog.catalog_sha256,
            }
            self._dataset_sha256 = hashlib.sha256(
                canonical_json_bytes(bundle)
            ).hexdigest()
        by_venue: dict[Venue, list[PredictionGhostReplay]] = {}
        by_dataset: dict[str, PredictionGhostReplay] = {}
        entries: list[tuple[PredictionGhostReplay, PredictionDepthSnapshot]] = []
        receipts: list[PredictionCampaignCollectionReceipt] = []
        public_slots: set[tuple[Venue, int]] = set()
        for engine in self._engines:
            venues = {row.venue for row in engine._dataset.rows}
            if len(venues) != 1:
                raise ValueError("prediction campaign child dataset must contain one venue")
            venue = next(iter(venues))
            if engine._dataset_sha256 in by_dataset:
                raise ValueError("prediction campaign child dataset identity is duplicated")
            by_dataset[engine._dataset_sha256] = engine
            by_venue.setdefault(venue, []).append(engine)
            entries.extend((engine, row) for row in engine._dataset.rows)
            binding = engine._collection_binding
            if self._dataset_synthetic:
                if binding is not None or engine._prospective_shard_ordinal is not None:
                    raise ValueError("synthetic campaign engine cannot claim a collection binding")
                continue
            shard_ordinal = engine._prospective_shard_ordinal
            if (
                binding is None
                or type(shard_ordinal) is not int
                or binding.terminal_result_sha256 is None
                or binding.raw_manifest_sha256 is None
                or binding.raw_root_sha256 is None
                or binding.probe_binding_sha256
                != engine._dataset.collection_probe_binding_sha256
                or binding.terminal_result_sha256
                != engine._dataset.collection_terminal_result_sha256
            ):
                raise ValueError("public campaign engine lacks an authenticated terminal receipt")
            slot = (venue, shard_ordinal)
            if slot in public_slots:
                raise ValueError("prediction campaign engines duplicate a prospective shard slot")
            public_slots.add(slot)
            receipts.append(
                PredictionCampaignCollectionReceipt(
                    venue=venue,
                    child_dataset_sha256=engine._dataset_sha256,
                    collection_id=binding.collection_id,
                    prospective_shard_ordinal=shard_ordinal,
                    probe_binding_sha256=binding.probe_binding_sha256,
                    terminal_result_sha256=binding.terminal_result_sha256,
                    raw_manifest_sha256=binding.raw_manifest_sha256,
                    raw_root_sha256=binding.raw_root_sha256,
                )
            )
        self._engines_by_venue = {
            venue: tuple(
                sorted(
                    engines_for_venue,
                    key=lambda item: (
                        -1
                        if item._prospective_shard_ordinal is None
                        else item._prospective_shard_ordinal,
                        item._manifest_sha256,
                    ),
                )
            )
            for venue, engines_for_venue in by_venue.items()
        }
        self._engines_by_dataset = by_dataset
        self._engines_by_manifest: dict[str, PredictionGhostReplay] = {}
        for engine in self._engines:
            previous = self._engines_by_manifest.get(engine._manifest_sha256)
            if previous is not None and previous is not engine:
                raise ValueError("prediction campaign raw manifest is bound to multiple shards")
            self._engines_by_manifest[engine._manifest_sha256] = engine
        self._entries = tuple(
            sorted(
                entries,
                key=lambda item: (
                    -1
                    if item[0]._prospective_shard_ordinal is None
                    else item[0]._prospective_shard_ordinal,
                    item[0]._manifest_sha256,
                    item[1].arrival_sequence,
                    item[1].raw_record_index,
                    item[1].received_monotonic_ns,
                    item[1].venue.value,
                    item[1].market_id,
                    item[1].outcome_id,
                ),
            )
        )
        capture_keys = [
            (
                row.venue,
                row.collector_identity,
                row.session_identity,
                row.arrival_sequence,
                row.raw_content_sha256,
                row.raw_record_index,
                row.outcome_id,
            )
            for _engine, row in self._entries
        ]
        if len(capture_keys) != len(set(capture_keys)):
            raise ValueError("prediction campaign shards overlap authenticated capture rows")
        self._receipts = tuple(
            sorted(
                receipts,
                key=lambda item: (
                    item.prospective_shard_ordinal,
                    item.raw_manifest_sha256,
                    item.venue.value,
                ),
            )
        )
        graphs: list[PredictionIdentityGraph] = []
        for engine in self._engines:
            graphs.extend(engine._graph_observations)
        graphs_by_observation: dict[
            tuple[Venue, str, str, str], PredictionIdentityGraph
        ] = {}
        for graph in graphs:
            graph_observation_key = (
                graph.venue,
                graph.market_id,
                graph.rule_version.version_id,
                graph.raw_graph_sha256,
            )
            previous_graph = graphs_by_observation.get(graph_observation_key)
            if previous_graph is not None and previous_graph != graph:
                raise ValueError("prediction campaign graph observation diverged across shards")
            graphs_by_observation[graph_observation_key] = graph
        self._graphs = tuple(
            graphs_by_observation[key]
            for key in sorted(
                graphs_by_observation,
                key=lambda item: (item[0].value, item[1], item[2], item[3]),
            )
        )

    @property
    def dataset_sha256(self) -> str:
        return self._dataset_sha256

    def _opportunity(
        self,
        *,
        variant: CandidateVariant,
        trigger: PredictionDepthSnapshot,
        signals: Sequence[PredictionOpportunitySignal],
        relations: Sequence[SemanticRelation],
        status: PredictionOpportunityStatus,
        reasons: Sequence[str],
        execution_sequence: Sequence[str],
    ) -> PredictionOpportunity:
        return PredictionOpportunity._create_for_runner(
            family_id=variant.family_id,
            variant_id=variant.variant_id,
            variant_parameters_sha256=variant.parameters_sha256,
            candidate_config_sha256=self._preregistration.config_sha256,
            campaign_manifest_sha256=self._campaign_manifest_sha256,
            runner_policy_sha256=self._preregistration.runner_policy.policy_sha256,
            dataset_bundle_sha256=self._dataset_sha256,
            semantic_catalog_sha256=self._semantic_catalog.catalog_sha256,
            relation_ids=tuple(item.relation_id for item in relations),
            signals=signals,
            trigger_point_in_time_id=trigger.point_in_time_id,
            execution_sequence=execution_sequence,
            quantity=self._preregistration.runner_policy.quantity,
            status=status,
            reasons=reasons,
            _runner_token=_CAMPAIGN_RUNNER_TOKEN,
        )

    @staticmethod
    def _row_member(row: PredictionDepthSnapshot) -> tuple[Venue, str, str]:
        return (row.venue, row.market_id, row.outcome_id)

    @staticmethod
    def _relation_members(
        relation: SemanticRelation,
    ) -> tuple[tuple[Venue, str, str], ...]:
        return tuple(
            sorted(
                (
                    (item.venue, item.economic_market_id, item.outcome_id)
                    for item in relation.members
                ),
                key=lambda item: (item[0].value, item[1], item[2]),
            )
        )

    @staticmethod
    def _signal(
        engine: PredictionGhostReplay,
        row: PredictionDepthSnapshot,
    ) -> PredictionOpportunitySignal:
        return PredictionOpportunitySignal.from_row(
            row,
            dataset_sha256=engine._dataset_sha256,
        )

    def _latest_rows_same_engine(
        self,
        *,
        engine: PredictionGhostReplay,
        members: Sequence[tuple[Venue, str, str]],
        trigger: PredictionDepthSnapshot,
        allowed_point_in_time_ids: frozenset[str],
    ) -> dict[tuple[Venue, str, str], PredictionDepthSnapshot]:
        latest: dict[tuple[Venue, str, str], PredictionDepthSnapshot] = {}
        member_set = set(members)
        for row in engine._dataset.rows:
            key = self._row_member(row)
            if (
                key in member_set
                and row.point_in_time_id in allowed_point_in_time_ids
                and row.collector_identity == trigger.collector_identity
                and row.session_identity == trigger.session_identity
                and row.received_time_utc_ns <= trigger.received_time_utc_ns
                and row.received_monotonic_ns <= trigger.received_monotonic_ns
                and (row.arrival_sequence, row.raw_record_index)
                <= (trigger.arrival_sequence, trigger.raw_record_index)
            ):
                previous = latest.get(key)
                if previous is None or (
                    row.received_monotonic_ns,
                    row.arrival_sequence,
                    row.raw_record_index,
                ) > (
                    previous.received_monotonic_ns,
                    previous.arrival_sequence,
                    previous.raw_record_index,
                ):
                    latest[key] = row
        return latest

    def _enumerate_k5(
        self,
        *,
        engine: PredictionGhostReplay,
        trigger: PredictionDepthSnapshot,
        variant: CandidateVariant,
    ) -> PredictionOpportunity | None:
        if trigger.venue is not Venue.KALSHI:
            return None
        signal = self._signal(engine, trigger)
        reasons: list[str] = []
        graph = engine._graph_for_snapshot(trigger)
        if graph is None or not graph.execution_admissible:
            reasons.append("AUTHENTICATED_ACTIVE_GRAPH_REQUIRED")
        elif graph.rule_version.market_status.upper() != "ACTIVE":
            reasons.append("MARKET_NOT_IN_FROZEN_ACTIVE_UNIVERSE")
        if not trigger.execution_eligible:
            reasons.extend(trigger.ineligibility_reasons or ("BOOK_NOT_EXECUTION_ELIGIBLE",))
        schedule = engine._fee_schedules.get(trigger.fee_schedule_id)
        if schedule is None or not schedule.exact_usable:
            reasons.append("FEE_UNKNOWN_FAIL_CLOSED")
        reasons.append("CUMULATIVE_AGGRESSOR_FLOW_NOT_IMPLEMENTED_FAIL_CLOSED")
        status = (
            PredictionOpportunityStatus.CANDIDATE
            if not reasons
            else PredictionOpportunityStatus.REFUSED
        )
        return self._opportunity(
            variant=variant,
            trigger=trigger,
            signals=(signal,),
            relations=(),
            status=status,
            reasons=tuple(dict.fromkeys(reasons)),
            execution_sequence=(
                _opportunity_member_text(signal.member_key),
            ),
        )

    def _enumerate_k6(
        self,
        *,
        trigger_engine: PredictionGhostReplay,
        trigger: PredictionDepthSnapshot,
        variant: CandidateVariant,
        allowed_point_in_time_ids: frozenset[str],
    ) -> tuple[PredictionOpportunity, ...]:
        trigger_member = self._row_member(trigger)
        opportunities: list[PredictionOpportunity] = []
        relations = sorted(
            (
                item
                for item in self._semantic_catalog.relations
                if item.relation_type is RelationType.EQUIVALENT
                and trigger_member in self._relation_members(item)
            ),
            key=lambda item: item.relation_id,
        )
        for relation in relations:
            signals: list[PredictionOpportunitySignal] = [
                self._signal(trigger_engine, trigger)
            ]
            for member in self._relation_members(relation):
                if member == trigger_member:
                    continue
                venue_engines = self._engines_by_venue.get(member[0], ())
                if not venue_engines:
                    continue
                candidates = [
                    (engine, row)
                    for engine in venue_engines
                    for row in engine._dataset.rows
                    if self._row_member(row) == member
                    and row.point_in_time_id in allowed_point_in_time_ids
                    and row.received_time_utc_ns <= trigger.received_time_utc_ns
                ]
                if candidates:
                    engine, row = max(
                        candidates,
                        key=lambda item: (
                            item[1].received_time_utc_ns,
                            item[0]._dataset_sha256,
                            item[1].arrival_sequence,
                        ),
                    )
                    signals.append(self._signal(engine, row))
            reasons = ["REFUSED_CROSS_VENUE_CLOCK_DOMAIN_UNVERIFIED"]
            if relation.status is not RelationStatus.VERIFIED:
                reasons.append("RELATION_UNVERIFIED_NOT_PROMOTABLE")
            if len(signals) != len(relation.members):
                reasons.append("MISSING_CAUSAL_MEMBER_BOOK")
            opportunities.append(
                self._opportunity(
                    variant=variant,
                    trigger=trigger,
                    signals=signals,
                    relations=(relation,),
                    status=PredictionOpportunityStatus.REFUSED,
                    reasons=reasons,
                    execution_sequence=(),
                )
            )
        return tuple(opportunities)

    def _enumerate_k4(
        self,
        *,
        engine: PredictionGhostReplay,
        trigger: PredictionDepthSnapshot,
        variant: CandidateVariant,
        allowed_point_in_time_ids: frozenset[str],
    ) -> tuple[PredictionOpportunity, ...]:
        trigger_member = self._row_member(trigger)
        trigger_graph = engine._graph_for_snapshot(trigger)
        grouped: dict[
            tuple[tuple[Venue, str, str], ...],
            list[SemanticRelation],
        ] = {}
        for relation in self._semantic_catalog.relations:
            if relation.relation_type not in {
                RelationType.EXHAUSTIVE,
                RelationType.MUTUALLY_EXCLUSIVE,
            }:
                continue
            if (
                trigger_graph is None
                or relation.machine_justification.get("rule_version_id")
                != trigger.rule_version_id
                or relation.machine_justification.get("raw_graph_sha256")
                != trigger_graph.raw_graph_sha256
                or tuple(relation.provenance)
                != trigger_graph.source_content_sha256s
            ):
                continue
            members = self._relation_members(relation)
            if trigger_member in members:
                grouped.setdefault(members, []).append(relation)
        opportunities: list[PredictionOpportunity] = []
        for members, raw_relations in sorted(
            grouped.items(),
            key=lambda item: tuple(
                (member[0].value, member[1], member[2]) for member in item[0]
            ),
        ):
            relations = tuple(sorted(raw_relations, key=lambda item: item.relation_id))
            exhaustive = [
                item for item in relations if item.relation_type is RelationType.EXHAUSTIVE
            ]
            mutually_exclusive = [
                item
                for item in relations
                if item.relation_type is RelationType.MUTUALLY_EXCLUSIVE
            ]
            latest = self._latest_rows_same_engine(
                engine=engine,
                members=members,
                trigger=trigger,
                allowed_point_in_time_ids=allowed_point_in_time_ids,
            )
            signal_rows = tuple(latest[key] for key in members if key in latest)
            signals = tuple(self._signal(engine, row) for row in signal_rows)
            reasons: list[str] = []
            if len(exhaustive) != 1 or len(mutually_exclusive) != 1:
                reasons.append("EXACT_EXHAUSTIVE_AND_MUTEX_RELATIONS_REQUIRED")
            if len(latest) != len(members):
                reasons.append("MISSING_CAUSAL_MEMBER_BOOK")
            elif any(
                not row.execution_eligible
                or row.collector_identity != trigger.collector_identity
                or row.session_identity != trigger.session_identity
                or trigger.received_time_utc_ns - row.received_time_utc_ns
                > self._preregistration.runner_policy.maximum_book_age_ns
                or trigger.received_monotonic_ns - row.received_monotonic_ns
                > self._preregistration.runner_policy.maximum_book_age_ns
                for row in signal_rows
            ):
                reasons.append("STALE_OR_INELIGIBLE_CAUSAL_MEMBER_BOOK")
            if any(member[0] is not trigger.venue for member in members):
                reasons.append("K4_CROSS_COLLECTOR_CLOCK_DOMAIN_FORBIDDEN")
            graph = (
                trigger_graph
                if trigger_graph is not None
                and {
                    (item.venue, item.economic_market_id, item.outcome_id)
                    for item in trigger_graph.outcomes
                }
                == set(members)
                and {row.rule_version_id for row in signal_rows}
                == {trigger_graph.rule_version.version_id}
                else None
            )
            if graph is None or not graph.execution_admissible:
                reasons.append("SINGLE_AUTHENTICATED_COMPLETE_GRAPH_REQUIRED")
            elif graph.rule_version.market_status.upper() != "ACTIVE":
                reasons.append("MARKET_NOT_IN_FROZEN_ACTIVE_UNIVERSE")
            elif any(
                relation.status is not RelationStatus.VERIFIED
                or tuple(relation.provenance) != graph.source_content_sha256s
                for relation in relations
            ):
                reasons.append("RELATION_PROVENANCE_DIVERGED")
            close_ns = (
                None
                if graph is None
                else _prediction_close_time_ns(graph.rule_version.closes_at)
            )
            execution_policy = PredictionVariantExecutionPolicy.from_variant(variant)
            latest_admission_ns = trigger.received_time_utc_ns + (
                self._preregistration.runner_policy.decision_delay_ns
                + max(len(members) - 1, 0) * (execution_policy.latency_ns + 1)
                + execution_policy.latency_ns
            )
            if close_ns is None or latest_admission_ns >= close_ns:
                reasons.append("MARKET_CLOSE_TIME_UNKNOWN_OR_PASSED")
            scan_result = None
            if not reasons and graph is not None and exhaustive:
                assert close_ns is not None
                pit_id = hashlib.sha256(
                    canonical_json_bytes(
                        {
                            "rows": [row.point_in_time_id for row in signal_rows],
                            "trigger": trigger.point_in_time_id,
                        }
                    )
                ).hexdigest()
                formal_legs = exhaustive[0].formal_rule.get("legs")
                side_by_member: dict[tuple[str, str], str] = {}
                if isinstance(formal_legs, list):
                    for item in formal_legs:
                        if isinstance(item, Mapping):
                            side_by_member[(str(item.get("market_id")), str(item.get("outcome_id")))] = str(
                                item.get("side")
                            )
                adapted: list[PredictionBookSnapshot] = []
                for row in signal_rows:
                    schedule = engine._fee_schedules.get(row.fee_schedule_id)
                    if schedule is None or not schedule.exact_usable:
                        reasons.append("FEE_UNKNOWN_FAIL_CLOSED")
                        break
                    quantity = self._preregistration.runner_policy.quantity
                    try:
                        fee_per_contract = schedule.order_fee(
                            levels=((row.asks[0].price, quantity),),
                            maker=False,
                        ) / quantity
                    except ValueError:
                        reasons.append("FEE_UNKNOWN_FAIL_CLOSED")
                        break
                    side = side_by_member.get((row.market_id, row.outcome_id), "YES")
                    yes_side = side == "YES"
                    adapted.append(
                        PredictionBookSnapshot(
                            venue=row.venue,
                            economic_market_id=row.market_id,
                            outcome_id=row.outcome_id,
                            point_in_time_id=pit_id,
                            observed_at_ns=trigger.received_time_utc_ns,
                            yes_bid=row.bids[0].price if yes_side else None,
                            yes_bid_size=(
                                min(row.bids[0].quantity, quantity)
                                if yes_side
                                else None
                            ),
                            yes_ask=row.asks[0].price if yes_side else None,
                            yes_ask_size=(
                                min(row.asks[0].quantity, quantity)
                                if yes_side
                                else None
                            ),
                            no_bid=row.bids[0].price if not yes_side else None,
                            no_bid_size=(
                                min(row.bids[0].quantity, quantity)
                                if not yes_side
                                else None
                            ),
                            no_ask=row.asks[0].price if not yes_side else None,
                            no_ask_size=(
                                min(row.asks[0].quantity, quantity)
                                if not yes_side
                                else None
                            ),
                            conservative_fee_per_contract=fee_per_contract,
                            rule_version_id=row.rule_version_id,
                            closes_at_ns=close_ns,
                            raw_segment_sha256=row.raw_content_sha256,
                        )
                    )
                if not reasons:
                    scan_result = K4Scanner(
                        conservative_slippage_bps=execution_policy.slippage_bps,
                        minimum_net_edge=execution_policy.minimum_net_edge,
                    ).scan(
                        exhaustive[0],
                        adapted,
                        observed_at_ns=trigger.received_time_utc_ns,
                    )
            if reasons:
                status = PredictionOpportunityStatus.REFUSED
                execution_sequence: tuple[str, ...] = ()
            elif scan_result is None:
                status = PredictionOpportunityStatus.REFUSED
                reasons.append("K4_SCANNER_NOT_RUN")
                execution_sequence = ()
            elif (
                scan_result.status is K4Status.CANDIDATE
                and scan_result.executable_quantity
                >= self._preregistration.runner_policy.quantity
            ):
                status = PredictionOpportunityStatus.CANDIDATE
                by_scan_text = {
                    f"{item.economic_market_id}:{item.outcome_id}:{item.action}": item
                    for item in scan_result.legs
                }
                ordered_legs = [by_scan_text[item] for item in scan_result.leg_sequencing]
                execution_sequence = tuple(
                    _opportunity_member_text(
                        (item.venue, item.economic_market_id, item.outcome_id)
                    )
                    for item in ordered_legs
                )
            elif scan_result.status is K4Status.CANDIDATE:
                status = PredictionOpportunityStatus.NO_ACTION
                reasons.append("WORST_LEG_DEPTH_BELOW_PREREGISTERED_QUANTITY")
                execution_sequence = ()
            elif scan_result.status is K4Status.REFUSED_UNVERIFIED:
                status = PredictionOpportunityStatus.REFUSED
                reasons.extend(scan_result.reasons or ("RELATION_UNVERIFIED_NOT_PROMOTABLE",))
                execution_sequence = ()
            else:
                status = PredictionOpportunityStatus.NO_ACTION
                reasons.extend(scan_result.reasons or ("K4_NO_EXECUTABLE_EDGE",))
                execution_sequence = ()
            opportunities.append(
                self._opportunity(
                    variant=variant,
                    trigger=trigger,
                    signals=signals or (self._signal(engine, trigger),),
                    relations=relations,
                    status=status,
                    reasons=tuple(dict.fromkeys(reasons)),
                    execution_sequence=execution_sequence,
                )
            )
        return tuple(opportunities)

    def _require_replay_seal(self, seal: PredictionReplaySeal) -> None:
        if (
            seal.candidate_config_sha256 != self._preregistration.config_sha256
            or seal.runner_policy_sha256
            != self._preregistration.runner_policy.policy_sha256
            or seal.dataset_sha256 != self._dataset_sha256
            or (
                self._campaign_manifest_sha256 is not None
                and seal.campaign_manifest_sha256
                != self._campaign_manifest_sha256
            )
        ):
            raise ValueError("prediction replay seal diverged from campaign bindings")

    def enumerate_opportunities(
        self,
        *,
        seal: PredictionReplaySeal,
    ) -> tuple[PredictionOpportunity, ...]:
        self._require_replay_seal(seal)
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        train_delta = seal.selection_view.train.start.astimezone(UTC) - epoch
        train_start_ns = (
            train_delta.days * 86_400_000_000_000
            + train_delta.seconds * 1_000_000_000
            + train_delta.microseconds * 1_000
        )
        active_entries = tuple(
            (engine, row)
            for engine, row in self._entries
            if train_start_ns
            <= row.received_time_utc_ns
            < seal.evidence_cutoff_utc_ns_exclusive - seal.embargo_ns
        )
        allowed_point_in_time_ids = frozenset(
            row.point_in_time_id for _engine, row in active_entries
        )
        entry_order = {
            row.point_in_time_id: index
            for index, (_engine, row) in enumerate(active_entries)
        }
        if len(entry_order) != len(active_entries):
            raise ValueError("prediction campaign active point-in-time identity is duplicated")
        variants = tuple(sorted(self._preregistration.variants, key=lambda item: item.variant_id))
        opportunities: list[PredictionOpportunity] = []
        for engine, trigger in active_entries:
            for variant in variants:
                if variant.family_id == "K4_COMPLETE_SET_LOGICAL_RV":
                    opportunities.extend(
                        self._enumerate_k4(
                            engine=engine,
                            trigger=trigger,
                            variant=variant,
                            allowed_point_in_time_ids=allowed_point_in_time_ids,
                        )
                    )
                elif variant.family_id == "K5_INCENTIVE_QUEUE":
                    opportunity = self._enumerate_k5(
                        engine=engine,
                        trigger=trigger,
                        variant=variant,
                    )
                    if opportunity is not None:
                        opportunities.append(opportunity)
                elif variant.family_id == "K6_CROSS_VENUE_EQUIVALENCE":
                    opportunities.extend(
                        self._enumerate_k6(
                            trigger_engine=engine,
                            trigger=trigger,
                            variant=variant,
                            allowed_point_in_time_ids=allowed_point_in_time_ids,
                        )
                    )
                else:
                    raise ValueError("prediction campaign contains an unsupported family")
        opportunities.sort(
            key=lambda item: (
                entry_order[item.trigger_point_in_time_id],
                item.variant_id,
                next(
                    signal.market_id
                    for signal in item.signals
                    if signal.point_in_time_id == item.trigger_point_in_time_id
                ),
                next(
                    signal.outcome_id
                    for signal in item.signals
                    if signal.point_in_time_id == item.trigger_point_in_time_id
                ),
                item.opportunity_id,
            )
        )
        deduplicated: list[PredictionOpportunity] = []
        seen_k4_economic_states: dict[
            tuple[object, ...],
            tuple[tuple[str, ...], PredictionOpportunityStatus, tuple[str, ...]],
        ] = {}
        for item in opportunities:
            if item.family_id != "K4_COMPLETE_SET_LOGICAL_RV":
                deduplicated.append(item)
                continue
            causal_state = (
                item.variant_id,
                item.relation_ids,
                tuple(
                    sorted(
                        (
                            signal.dataset_sha256,
                            signal.venue.value,
                            signal.market_id,
                            signal.outcome_id,
                            signal.point_in_time_id,
                        )
                        for signal in item.signals
                    )
                ),
            )
            decision = (item.execution_sequence, item.status, item.reasons)
            previous_decision = seen_k4_economic_states.get(causal_state)
            if previous_decision is not None:
                if previous_decision != decision:
                    raise ValueError("K4_CAUSAL_STATE_DECISION_DIVERGED")
                continue
            seen_k4_economic_states[causal_state] = decision
            deduplicated.append(item)
        opportunities = deduplicated
        identifiers = [item.opportunity_id for item in opportunities]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("prediction campaign opportunity enumeration is not unique")
        return tuple(opportunities)

    def _intent(
        self,
        *,
        opportunity: PredictionOpportunity,
        signal: PredictionOpportunitySignal,
        leg_index: int,
        role: PredictionExecutionRole,
    ) -> PredictionOrderIntent:
        engine = next(
            (
                item
                for item in self._engines
                if item._dataset_sha256 == signal.dataset_sha256
            ),
            None,
        )
        if engine is None:
            raise ValueError("prediction opportunity signal dataset is absent")
        rows = [
            row
            for row in engine._dataset.rows
            if row.point_in_time_id == signal.point_in_time_id
            and row.arrival_sequence == signal.arrival_sequence
            and self._row_member(row) == signal.member_key
        ]
        if len(rows) != 1:
            raise ValueError("prediction opportunity signal row is ambiguous")
        row = rows[0]
        trigger = next(
            item
            for item in opportunity.signals
            if item.point_in_time_id == opportunity.trigger_point_in_time_id
        )
        policy = PredictionVariantExecutionPolicy.from_variant(
            self._preregistration.require_variant(
                opportunity.variant_id,
                parameters_sha256=opportunity.variant_parameters_sha256,
            )
        )
        delay = self._preregistration.runner_policy.decision_delay_ns + leg_index * (
            policy.latency_ns + 1
        )
        return PredictionOrderIntent(
            order_id=f"{opportunity.opportunity_id}:LEG:{leg_index}",
            opportunity_id=opportunity.opportunity_id,
            variant_id=opportunity.variant_id,
            candidate_config_sha256=self._preregistration.config_sha256,
            campaign_manifest_sha256=engine._dataset.campaign_manifest_sha256,
            collection_probe_binding_sha256=engine._dataset.collection_probe_binding_sha256,
            collection_terminal_result_sha256=(
                engine._dataset.collection_terminal_result_sha256
            ),
            variant_parameters_sha256=opportunity.variant_parameters_sha256,
            signal_dataset_sha256=engine._dataset_sha256,
            venue=signal.venue,
            market_id=signal.market_id,
            outcome_id=signal.outcome_id,
            signal_point_in_time_id=signal.point_in_time_id,
            signal_arrival_sequence=signal.arrival_sequence,
            signal_time_utc_ns=trigger.received_time_utc_ns,
            signal_received_monotonic_ns=trigger.received_monotonic_ns,
            decision_time_utc_ns=trigger.received_time_utc_ns + delay,
            decision_monotonic_ns=trigger.received_monotonic_ns + delay,
            quantity=opportunity.quantity,
            limit_price=(
                row.bids[0].price
                if role is PredictionExecutionRole.MAKER
                else row.asks[0].price
            ),
            role=role,
        )

    def run_campaign(
        self,
        *,
        seal: PredictionReplaySeal,
        settlements: Mapping[PredictionLegEvidenceKey, PredictionSettlementEvidence],
        maker_aggressors: Mapping[PredictionLegEvidenceKey, MakerAggressorEvidence]
        | None = None,
    ) -> PredictionCampaignReplayReport:
        self._require_replay_seal(seal)
        opportunities = self.enumerate_opportunities(seal=seal)
        reports: list[PredictionTopLevelReport] = []
        for opportunity in opportunities:
            if opportunity.status is not PredictionOpportunityStatus.CANDIDATE:
                continue
            signals_by_text = {
                _opportunity_member_text(item.member_key): item
                for item in opportunity.signals
            }
            if set(opportunity.execution_sequence) - set(signals_by_text):
                raise ValueError("prediction campaign execution sequence is not signal-bound")
            if opportunity.family_id == "K6_CROSS_VENUE_EQUIVALENCE":
                raise ValueError("K6 must remain refused without an authenticated clock contract")
            role = (
                PredictionExecutionRole.MAKER
                if opportunity.family_id == "K5_INCENTIVE_QUEUE"
                else PredictionExecutionRole.TAKER
            )
            legs: list[
                tuple[
                    PredictionOrderIntent,
                    PredictionSettlementEvidence,
                    MakerAggressorEvidence | None,
                ]
            ] = []
            leg_signals: list[PredictionOpportunitySignal] = []
            for index, member_text in enumerate(opportunity.execution_sequence):
                signal = signals_by_text[member_text]
                key = PredictionLegEvidenceKey.from_signal(
                    opportunity.opportunity_id,
                    signal,
                )
                settlement = settlements.get(key)
                if settlement is None:
                    raise ValueError(
                        f"prediction campaign candidate lacks settlement evidence:{member_text}"
                    )
                if (
                    settlement.received_time_utc_ns
                    >= seal.evidence_cutoff_utc_ns_exclusive
                ):
                    raise ValueError("prediction settlement exceeds the authenticated cutoff")
                aggressor = None if maker_aggressors is None else maker_aggressors.get(key)
                if (
                    aggressor is not None
                    and aggressor.received_time_utc_ns
                    >= seal.evidence_cutoff_utc_ns_exclusive
                ):
                    raise ValueError("prediction maker evidence exceeds the sealed cutoff")
                legs.append(
                    (
                        self._intent(
                            opportunity=opportunity,
                            signal=signal,
                            leg_index=index,
                            role=role,
                        ),
                        settlement,
                        aggressor,
                    )
                )
                leg_signals.append(signal)
            dataset_ids = {item.dataset_sha256 for item in leg_signals}
            clock_domains = {
                (item.collector_identity, item.session_identity)
                for item in leg_signals
            }
            if len(dataset_ids) != 1 or len(clock_domains) != 1:
                raise ValueError(
                    "prediction campaign leg group crosses shard or monotonic clock domain"
                )
            child_dataset_sha256 = next(iter(dataset_ids))
            engine = self._engines_by_dataset.get(child_dataset_sha256)
            if engine is None:
                raise ValueError("prediction campaign signal shard is absent")
            report_time_utc_ns = seal.evidence_cutoff_utc_ns_exclusive - 1
            report_time_monotonic_ns = max(
                (
                    intent.decision_monotonic_ns
                    + PredictionVariantExecutionPolicy.from_variant(
                        self._preregistration.require_variant(
                            intent.variant_id,
                            parameters_sha256=intent.variant_parameters_sha256,
                        )
                    ).latency_ns
                    for intent, _settlement, _aggressor in legs
                ),
                default=0,
            ) + 1
            for _intent, settlement, aggressor in legs:
                signal_clock = next(iter(clock_domains))
                if (
                    settlement.collector_identity,
                    settlement.session_identity,
                ) == signal_clock:
                    report_time_monotonic_ns = max(
                        report_time_monotonic_ns,
                        settlement.received_monotonic_ns,
                    )
                if aggressor is not None:
                    report_time_monotonic_ns = max(
                        report_time_monotonic_ns,
                        aggressor.received_monotonic_ns,
                    )
            if opportunity.family_id == "K4_COMPLETE_SET_LOGICAL_RV":
                relation_map = {
                    item.relation_id: item for item in self._semantic_catalog.relations
                }
                reports.append(
                    engine.run_group(
                        opportunity=opportunity,
                        relations=tuple(
                            relation_map[item] for item in opportunity.relation_ids
                        ),
                        legs=legs,
                        report_time_utc_ns=report_time_utc_ns,
                        report_time_monotonic_ns=report_time_monotonic_ns,
                        settlement_validation_engines=self._engines_by_manifest,
                    )
                )
            else:
                if len(legs) != 1:
                    raise ValueError("prediction single-leg opportunity is ambiguous")
                intent, settlement, aggressor = legs[0]
                reports.append(
                    engine.run(
                        intent=intent,
                        settlement=settlement,
                        maker_aggressor=aggressor,
                        report_time_utc_ns=report_time_utc_ns,
                        report_time_monotonic_ns=report_time_monotonic_ns,
                        _opportunity_execution_token=_OPPORTUNITY_EXECUTION_TOKEN,
                        _opportunity=opportunity,
                        _settlement_validation_engine=self._engines_by_manifest.get(
                            settlement.raw_manifest_sha256
                        ),
                    )
                )
        report = PredictionCampaignReplayReport(
            candidate_config_sha256=self._preregistration.config_sha256,
            campaign_manifest_sha256=self._campaign_manifest_sha256,
            runner_policy_sha256=self._preregistration.runner_policy.policy_sha256,
            dataset_sha256=self._dataset_sha256,
            child_dataset_sha256s=self._child_dataset_sha256s,
            dataset_synthetic=self._dataset_synthetic,
            semantic_catalog_sha256=self._semantic_catalog.catalog_sha256,
            collection_receipts=self._receipts,
            replay_seal_sha256=seal.seal_sha256,
            opportunities=opportunities,
            reports=tuple(reports),
            evidence_cutoff_utc_ns_exclusive=(
                seal.evidence_cutoff_utc_ns_exclusive
            ),
            selection_split_view=seal.selection_view.to_dict(),
            prospective_slot_coverage=self._prospective_slot_coverage,
        )
        return replace(
            report,
            _verification_token=_CAMPAIGN_REPORT_VERIFICATION_TOKEN,
            _verified_report_sha256=report.report_sha256,
        )


def build_synthetic_prediction_campaign_replay(
    reports: Sequence[PredictionGhostReport],
    *,
    preregistration: CandidatePreregistration,
    semantic_catalog_sha256: str,
    seal: PredictionReplaySeal,
) -> PredictionCampaignReplayReport:
    """Wrap verified K5 fixtures for mechanism tests; never public/economic evidence."""

    if not reports or len(semantic_catalog_sha256) != 64:
        raise ValueError("synthetic prediction campaign fixture is incomplete")
    dataset_hashes = {item.dataset_sha256 for item in reports}
    if len(dataset_hashes) != 1:
        raise ValueError("synthetic prediction campaign fixture mixes datasets")
    dataset_sha256 = next(iter(dataset_hashes))
    if (
        seal.dataset_sha256 != dataset_sha256
        or seal.candidate_config_sha256 != preregistration.config_sha256
        or seal.runner_policy_sha256
        != preregistration.runner_policy.policy_sha256
    ):
        raise ValueError("synthetic campaign replay seal diverged")
    opportunities: list[PredictionOpportunity] = []
    rebound_reports: list[PredictionGhostReport] = []
    ordered = tuple(
        sorted(
            reports,
            key=lambda item: (
                item.signal_time_utc_ns,
                item.variant_id,
                item.market_id,
                item.outcome_id,
                item.report_sha256,
            ),
        )
    )
    for index, report in enumerate(ordered, start=1):
        variant = preregistration.require_variant(
            report.variant_id,
            parameters_sha256=report.variant_parameters_sha256,
        )
        if (
            not report.replay_verified
            or not report.dataset_synthetic
            or variant.family_id != "K5_INCENTIVE_QUEUE"
            or report.requested_quantity != preregistration.runner_policy.quantity
            or report.campaign_manifest_sha256 is not None
            or report.collection_probe_binding_sha256 is not None
            or report.collection_terminal_result_sha256 is not None
        ):
            raise ValueError("synthetic campaign fixtures must be verified single-leg K5 reports")
        point_in_time_id = hashlib.sha256(
            canonical_json_bytes(
                {
                    "fixture_label": "SYNTHETIC/FIXTURE",
                    "report_sha256": report.report_sha256,
                    "signal_index": index,
                }
            )
        ).hexdigest()
        signal = PredictionOpportunitySignal(
            venue=report.venue,
            dataset_sha256=report.dataset_sha256,
            event_id=report.event_id,
            market_id=report.market_id,
            outcome_id=report.outcome_id,
            point_in_time_id=point_in_time_id,
            arrival_sequence=index,
            received_time_utc_ns=report.signal_time_utc_ns,
            received_monotonic_ns=report.signal_received_monotonic_ns,
            raw_content_sha256=report.raw_content_sha256s[0],
            raw_record_index=0,
            collector_identity="prediction-synthetic-campaign-v1",
            session_identity="prediction-synthetic-campaign-v1",
        )
        opportunity = PredictionOpportunity._create_for_runner(
            family_id=variant.family_id,
            variant_id=variant.variant_id,
            variant_parameters_sha256=variant.parameters_sha256,
            candidate_config_sha256=preregistration.config_sha256,
            campaign_manifest_sha256=None,
            runner_policy_sha256=preregistration.runner_policy.policy_sha256,
            dataset_bundle_sha256=dataset_sha256,
            semantic_catalog_sha256=semantic_catalog_sha256,
            relation_ids=(),
            signals=(signal,),
            trigger_point_in_time_id=point_in_time_id,
            execution_sequence=(_opportunity_member_text(signal.member_key),),
            quantity=preregistration.runner_policy.quantity,
            status=PredictionOpportunityStatus.CANDIDATE,
            reasons=(),
            _runner_token=_CAMPAIGN_RUNNER_TOKEN,
        )
        rebound = replace(
            report,
            order_id=f"{opportunity.opportunity_id}:LEG:0",
            opportunity_id=opportunity.opportunity_id,
            _verification_token=None,
            _verified_report_sha256=None,
        )
        rebound = replace(
            rebound,
            _verification_token=_REPLAY_VERIFICATION_TOKEN,
            _verified_report_sha256=rebound.report_sha256,
        )
        opportunities.append(opportunity)
        rebound_reports.append(rebound)
    campaign = PredictionCampaignReplayReport(
        candidate_config_sha256=preregistration.config_sha256,
        campaign_manifest_sha256=None,
        runner_policy_sha256=preregistration.runner_policy.policy_sha256,
        dataset_sha256=dataset_sha256,
        child_dataset_sha256s=(dataset_sha256,),
        dataset_synthetic=True,
        semantic_catalog_sha256=semantic_catalog_sha256,
        collection_receipts=(),
        replay_seal_sha256=seal.seal_sha256,
        opportunities=tuple(opportunities),
        reports=tuple(rebound_reports),
        evidence_cutoff_utc_ns_exclusive=seal.evidence_cutoff_utc_ns_exclusive,
        selection_split_view=seal.selection_view.to_dict(),
        prospective_slot_coverage=None,
    )
    return replace(
        campaign,
        _verification_token=_CAMPAIGN_REPORT_VERIFICATION_TOKEN,
        _verified_report_sha256=campaign.report_sha256,
    )


def replay_prediction_fixture(raw: bytes) -> PredictionGhostReport:
    decoded = decode_canonical_json(raw, require_canonical=True)
    if not isinstance(decoded, dict):
        raise ValueError("prediction Ghost fixture must be a canonical object")
    if decoded.get("fixture_label") != "SYNTHETIC/FIXTURE":
        raise ValueError("prediction direct fixture must be visibly SYNTHETIC/FIXTURE")

    def mapping(value: object, label: str) -> Mapping[str, object]:
        if not isinstance(value, Mapping):
            raise ValueError(f"{label} must be an object")
        return value

    def decimal(value: object) -> Decimal:
        return Decimal(str(value))

    def integer(value: object, label: str) -> int:
        if type(value) is not int:
            raise ValueError(f"{label} must be an integer")
        return value

    snapshot_raw = mapping(decoded.get("snapshot"), "prediction snapshot")

    def levels(key: str) -> tuple[PredictionDepthLevel, ...]:
        raw_levels = snapshot_raw.get(key)
        if not isinstance(raw_levels, list):
            raise ValueError(f"snapshot {key} must be an array")
        return tuple(
            PredictionDepthLevel(
                decimal(mapping(item, f"{key} level")["price"]),
                decimal(mapping(item, f"{key} level")["quantity"]),
            )
            for item in raw_levels
        )

    reasons_raw = snapshot_raw.get("ineligibility_reasons")
    if not isinstance(reasons_raw, list):
        reasons_raw = []
    intent_raw = mapping(decoded.get("intent"), "prediction intent")
    settlement_raw = mapping(decoded.get("settlement"), "prediction settlement")
    with TemporaryDirectory(prefix="hyperlab-prediction-fixture-") as temporary:
        raw_root = Path(temporary) / "raw"
        provenance = CaptureProvenance(
            collection_id="prediction-ghost-synthetic-fixture-v1",
            source_url="fixture://prediction-ghost-v1",
            transport="FIXTURE",
            fixture_label=SYNTHETIC_FIXTURE_LABEL,
        )
        factory = SessionEnvelopeFactory(
            venue=Venue(str(snapshot_raw["venue"])),
            collector_identity="prediction-ghost-synthetic-fixture-v1",
            session_identity="prediction-ghost-synthetic-fixture-v1",
            source_metadata_version=(
                POLYMARKET_METADATA_VERSION
                if Venue(str(snapshot_raw["venue"])) is Venue.POLYMARKET
                else KALSHI_METADATA_VERSION
            ),
            provenance=provenance,
        )
        envelope = factory.make(
            feed_type="ghost_fixture",
            instrument_id=str(snapshot_raw["outcome_id"]),
            market_id=str(snapshot_raw["market_id"]),
            source_timestamp_ns=None,
            receive_timestamp_utc_ns=integer(snapshot_raw["received_time_utc_ns"], "received UTC time"),
            receive_monotonic_ns=integer(snapshot_raw["received_monotonic_ns"], "received monotonic time"),
            raw_payload=raw,
            source_event_id="prediction-ghost-synthetic-fixture-v1",
            infer_source_sequence_continuity=False,
        )
        writer = ResearchSegmentWriter(
            raw_root,
            collection_id=provenance.collection_id,
            max_segment_bytes=max(len(envelope.canonical_bytes()) * 2, 65_536),
            rotation_seconds=30,
            max_total_bytes=max(len(envelope.canonical_bytes()) * 4, 262_144),
        )
        writer.append(envelope)
        manifest = writer.close()
        if manifest is None:
            raise AssertionError("synthetic prediction fixture manifest was not published")
        raw_reference = prediction_raw_record_ref(envelope, 0)

        fee_raw = mapping(decoded.get("fee_schedule"), "fee schedule")
        fixture_outcomes = (
            str(snapshot_raw["outcome_id"]),
            f"{snapshot_raw['market_id']}:SYNTHETIC_COMPLEMENT",
        )
        fee = PredictionFeeSchedule(
            schedule_id=str(fee_raw["schedule_id"]),
            venue=Venue(str(fee_raw["venue"])),
            market_id=str(snapshot_raw["market_id"]),
            outcome_ids=fixture_outcomes,
            classification=EvidenceClassification(str(fee_raw["classification"])),
            model=FeeModel(str(fee_raw["model"])),
            effective_from_ns=integer(fee_raw["effective_from_ns"], "fee effective_from_ns"),
            effective_to_ns=(
                None
                if fee_raw.get("effective_to_ns") is None
                else integer(fee_raw["effective_to_ns"], "fee effective_to_ns")
            ),
            taker_rate=decimal(fee_raw["taker_rate"]),
            maker_rate=decimal(fee_raw["maker_rate"]),
            multiplier=decimal(fee_raw["multiplier"]),
            exponent=decimal(fee_raw["exponent"]),
            rounding_quantum=decimal(fee_raw["rounding_quantum"]),
            rounding_complete=fee_raw.get("rounding_complete") is True,
            rounding_scope=str(fee_raw.get("rounding_scope", "PER_FILL")),
            account_precision_quantum=(
                None
                if fee_raw.get("account_precision_quantum") is None
                else decimal(fee_raw["account_precision_quantum"])
            ),
            source_refs=(raw_reference,),
            synthetic_fixture=True,
        )
        tick_size = decimal(snapshot_raw["tick_size"])
        tick_grid = PredictionTickGrid(
            grid_id="prediction-ghost-synthetic-fixture-grid-v1",
            venue=Venue(str(snapshot_raw["venue"])),
            market_id=str(snapshot_raw["market_id"]),
            outcome_ids=fixture_outcomes,
            classification=EvidenceClassification.UNKNOWN_NOT_OBSERVED,
            bands=(PredictionTickBand(Decimal("0"), Decimal("1"), tick_size),),
            source_refs=(raw_reference,),
            synthetic_fixture=True,
        )
        snapshot = PredictionDepthSnapshot(
            venue=Venue(str(snapshot_raw["venue"])),
            event_id=str(snapshot_raw["event_id"]),
            market_id=str(snapshot_raw["market_id"]),
            outcome_id=str(snapshot_raw["outcome_id"]),
            point_in_time_id=str(snapshot_raw["point_in_time_id"]),
            rule_version_id=str(snapshot_raw["rule_version_id"]),
            graph_observation_sha256=None,
            fee_schedule_id=str(snapshot_raw["fee_schedule_id"]),
            tick_grid=tick_grid,
            bids=levels("bids"),
            asks=levels("asks"),
            source_event_time_ns=(
                None
                if snapshot_raw.get("source_event_time_ns") is None
                else integer(snapshot_raw["source_event_time_ns"], "source event time")
            ),
            source_time_ns=(
                None
                if snapshot_raw.get("source_time_ns") is None
                else integer(snapshot_raw["source_time_ns"], "source time")
            ),
            received_time_utc_ns=envelope.receive_timestamp_utc_ns,
            received_monotonic_ns=envelope.receive_monotonic_ns,
            arrival_sequence=envelope.arrival_sequence,
            raw_manifest_sha256=manifest.manifest_sha256,
            raw_root_sha256=manifest.root_sha256,
            raw_content_sha256=envelope.content_sha256,
            raw_record_index=0,
            source_metadata_version=envelope.source_metadata_version,
            collector_identity=envelope.collector_identity,
            session_identity=envelope.session_identity,
            source_url=envelope.provenance.source_url,
            source_transport=envelope.provenance.transport,
            gap_detected=False,
            duplicate=False,
            reconnect=False,
            ask_derivation=str(snapshot_raw["ask_derivation"]),
            execution_eligible=snapshot_raw.get("execution_eligible") is True,
            ineligibility_reasons=tuple(str(item) for item in reasons_raw),
        )
        settlement_received = integer(
            settlement_raw.get("received_time_utc_ns", settlement_raw.get("observed_at_ns")),
            "settlement received time",
        )
        settlement = PredictionSettlementEvidence(
            venue=Venue(str(settlement_raw["venue"])),
            market_id=str(settlement_raw["market_id"]),
            outcome_id=str(settlement_raw["outcome_id"]),
            state=PredictionSettlementState(str(settlement_raw["state"])),
            source_event_time_ns=(
                None
                if settlement_raw.get("source_event_time_ns") is None
                else integer(settlement_raw["source_event_time_ns"], "settlement source time")
            ),
            received_time_utc_ns=settlement_received,
            received_monotonic_ns=integer(
                settlement_raw.get("received_monotonic_ns", settlement_received),
                "settlement monotonic time",
            ),
            payout_per_contract=(
                None
                if settlement_raw.get("payout_per_contract") is None
                else decimal(settlement_raw["payout_per_contract"])
            ),
            rule_version_id=str(settlement_raw["rule_version_id"]),
            resolution_rule_version_id=str(
                settlement_raw.get(
                    "resolution_rule_version_id",
                    settlement_raw["rule_version_id"],
                )
            ),
            source_event_id=str(settlement_raw["source_event_id"]),
            raw_manifest_sha256=manifest.manifest_sha256,
            raw_root_sha256=manifest.root_sha256,
            raw_ref=raw_reference,
            collector_identity=envelope.collector_identity,
            session_identity=envelope.session_identity,
            source_url=envelope.provenance.source_url,
            classification=EvidenceClassification(str(settlement_raw["classification"])),
            synthetic_fixture=True,
        )
        semantic_catalog = SemanticCatalog.build(())
        dataset = PredictionPointInTimeDataset(
            identity=DerivedDatasetIdentity.build(
                manifest=manifest,
                model_version="PREDICTION_GHOST_SYNTHETIC_FIXTURE_DATASET_V1",
                parameters={"fixture_sha256": hashlib.sha256(raw).hexdigest()},
            ),
            semantic_catalog_sha256=semantic_catalog.catalog_sha256,
            rows=(snapshot,),
            synthetic=True,
        )
        project_root = Path(__file__).resolve().parents[3]
        preregistration = CandidatePreregistration.from_path(
            project_root / "config/research/prediction-markets-candidate-v1.json"
        )
        variant_id = str(intent_raw["variant_id"])
        variant_matches = [item for item in preregistration.variants if item.variant_id == variant_id]
        if len(variant_matches) != 1:
            raise ValueError("synthetic prediction fixture variant is not preregistered")
        variant = variant_matches[0]
        intent = PredictionOrderIntent(
            order_id=str(intent_raw["order_id"]),
            opportunity_id=f"OPP:{hashlib.sha256(canonical_json_bytes({'dataset_sha256': dataset.dataset_sha256, 'point_in_time_id': snapshot.point_in_time_id, 'variant_id': variant_id})).hexdigest()}",
            variant_id=variant_id,
            candidate_config_sha256=preregistration.config_sha256,
            campaign_manifest_sha256=None,
            collection_probe_binding_sha256=None,
            collection_terminal_result_sha256=None,
            variant_parameters_sha256=variant.parameters_sha256,
            signal_dataset_sha256=dataset.dataset_sha256,
            venue=Venue(str(intent_raw["venue"])),
            market_id=str(intent_raw["market_id"]),
            outcome_id=str(intent_raw["outcome_id"]),
            signal_point_in_time_id=snapshot.point_in_time_id,
            signal_arrival_sequence=snapshot.arrival_sequence,
            signal_time_utc_ns=integer(intent_raw["signal_time_utc_ns"], "signal time"),
            signal_received_monotonic_ns=snapshot.received_monotonic_ns,
            decision_time_utc_ns=integer(intent_raw["decision_time_utc_ns"], "decision time"),
            decision_monotonic_ns=integer(intent_raw["decision_monotonic_ns"], "decision monotonic time"),
            quantity=decimal(intent_raw["quantity"]),
            limit_price=decimal(intent_raw["limit_price"]),
            role=PredictionExecutionRole(str(intent_raw["role"])),
        )
        contracts = {
            Venue.POLYMARKET: OfficialPublicContract.from_path(
                project_root / "config/research/polymarket-public-contract-v1.json"
            ),
            Venue.KALSHI: OfficialPublicContract.from_path(
                project_root / "config/research/kalshi-public-contract-v1.json"
            ),
        }
        return PredictionGhostReplay(
            raw_root=raw_root,
            manifest_sha256=manifest.manifest_sha256,
            dataset=dataset,
            preregistration=preregistration,
            contracts=contracts,
            collection_binding=None,
            semantic_catalog=semantic_catalog,
            identity_graphs=(),
            fee_schedules={fee.schedule_id: fee},
            tick_grids={tick_grid.grid_id: tick_grid},
            maximum_book_age_ns=integer(decoded["maximum_book_age_ns"], "maximum book age"),
            _direct_synthetic_fixture=True,
        ).run(
            intent=intent,
            settlement=settlement,
            report_time_utc_ns=integer(decoded["report_time_utc_ns"], "report time"),
            report_time_monotonic_ns=integer(decoded["report_time_monotonic_ns"], "report monotonic time"),
        )


__all__ = [
    "MakerAggressorEvidence",
    "PredictionExecutionRole",
    "PredictionGhostFill",
    "PredictionGhostReplay",
    "PredictionGhostReport",
    "PredictionOrderIntent",
    "PredictionSettlementEvidence",
    "PredictionSettlementState",
    "replay_prediction_fixture",
]
