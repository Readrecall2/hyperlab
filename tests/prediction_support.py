from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

from hyperlab.ghost.prediction import (
    MakerAggressorEvidence,
    PredictionExecutionRole,
    PredictionGhostReplay,
    PredictionOrderIntent,
    PredictionSettlementEvidence,
    PredictionSettlementState,
)
from hyperlab.research_data.adapters import POLYMARKET_METADATA_VERSION
from hyperlab.research_data.canonical import canonical_json_bytes
from hyperlab.research_data.envelope import (
    SYNTHETIC_FIXTURE_LABEL,
    CaptureProvenance,
    PublicDataEnvelope,
    SessionEnvelopeFactory,
    Venue,
)
from hyperlab.research_data.prediction import SemanticCatalog
from hyperlab.research_data.prediction_candidate import (
    CandidatePreregistration,
    FeeModel,
    PredictionCollectionBinding,
    PredictionFeeSchedule,
    PredictionPointInTimeDataset,
    PredictionTickBand,
    PredictionTickGrid,
    build_prediction_dataset,
)
from hyperlab.research_data.prediction_contracts import (
    EvidenceClassification,
    OfficialPublicContract,
    PredictionIdentityGraph,
    build_prediction_graph_from_raw,
)
from hyperlab.research_data.prediction_evidence import (
    PredictionRawEvidenceIndex,
    PredictionRawRecordRef,
    prediction_raw_record_ref,
)
from hyperlab.research_data.segments import ResearchSegmentReader, ResearchSegmentWriter

ROOT = Path(__file__).resolve().parents[1]
BASE_UTC_NS = 1_800_000_000_000_000_000


@dataclass(frozen=True, slots=True)
class BookSpec:
    received_monotonic_ns: int
    received_time_utc_ns: int
    bids: tuple[tuple[str, str], ...] = (("0.40", "10"),)
    asks: tuple[tuple[str, str], ...] = (("0.42", "2"), ("0.43", "1"))


@dataclass(frozen=True, slots=True)
class PolymarketFixtureBundle:
    raw_root: Path
    manifest_sha256: str
    root_sha256: str
    envelopes: tuple[PublicDataEnvelope, ...]
    graph: PredictionIdentityGraph
    fee: PredictionFeeSchedule
    tick_grid: PredictionTickGrid
    dataset: PredictionPointInTimeDataset
    preregistration: CandidatePreregistration
    contracts: dict[Venue, OfficialPublicContract]
    collection_binding: PredictionCollectionBinding | None
    settlement: PredictionSettlementEvidence
    aggressor: MakerAggressorEvidence | None

    def replay(self) -> PredictionGhostReplay:
        return PredictionGhostReplay(
            raw_root=self.raw_root,
            manifest_sha256=self.manifest_sha256,
            dataset=self.dataset,
            preregistration=self.preregistration,
            contracts=self.contracts,
            collection_binding=self.collection_binding,
            semantic_catalog=SemanticCatalog.build(()),
            identity_graphs=(self.graph,),
            fee_schedules={self.fee.schedule_id: self.fee},
            tick_grids={self.tick_grid.grid_id: self.tick_grid},
            maximum_book_age_ns=2_000_000_000,
        )

    def intent(
        self,
        *,
        role: PredictionExecutionRole = PredictionExecutionRole.TAKER,
        quantity: str = "4",
        limit_price: str | None = None,
        variant_id: str | None = None,
        order_id: str = "fixture-order",
    ) -> PredictionOrderIntent:
        signal = self.dataset.rows[0]
        selected_variant = variant_id or (
            "K5_V1_ZERO_REWARD_CONTROL"
            if role is PredictionExecutionRole.MAKER
            else "K4_V1_PRIMARY_500MS"
        )
        variant = next(
            item for item in self.preregistration.variants if item.variant_id == selected_variant
        )
        return PredictionOrderIntent(
            order_id=order_id,
            opportunity_id=f"OPP:{hashlib.sha256(canonical_json_bytes({'dataset_sha256': self.dataset.dataset_sha256, 'point_in_time_id': signal.point_in_time_id, 'variant_id': selected_variant})).hexdigest()}",
            variant_id=selected_variant,
            candidate_config_sha256=self.preregistration.config_sha256,
            campaign_manifest_sha256=self.dataset.campaign_manifest_sha256,
            collection_probe_binding_sha256=self.dataset.collection_probe_binding_sha256,
            collection_terminal_result_sha256=self.dataset.collection_terminal_result_sha256,
            variant_parameters_sha256=variant.parameters_sha256,
            signal_dataset_sha256=self.dataset.dataset_sha256,
            venue=signal.venue,
            market_id=signal.market_id,
            outcome_id=signal.outcome_id,
            signal_point_in_time_id=signal.point_in_time_id,
            signal_arrival_sequence=signal.arrival_sequence,
            signal_time_utc_ns=signal.received_time_utc_ns + 1,
            signal_received_monotonic_ns=signal.received_monotonic_ns,
            decision_time_utc_ns=signal.received_time_utc_ns + 2,
            decision_monotonic_ns=signal.received_monotonic_ns + 2,
            quantity=Decimal(quantity),
            limit_price=Decimal(
                limit_price
                if limit_price is not None
                else ("0.40" if role is PredictionExecutionRole.MAKER else "0.42")
            ),
            role=role,
        )


def _raw(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def _contracts() -> dict[Venue, OfficialPublicContract]:
    return {
        Venue.POLYMARKET: OfficialPublicContract.from_path(
            ROOT / "config/research/polymarket-public-contract-v1.json"
        ),
        Venue.KALSHI: OfficialPublicContract.from_path(
            ROOT / "config/research/kalshi-public-contract-v1.json"
        ),
    }


def build_polymarket_fixture(
    tmp_path: Path,
    *,
    books: tuple[BookSpec, ...] = (
        BookSpec(received_monotonic_ns=1_000, received_time_utc_ns=BASE_UTC_NS),
    ),
    fee_after_books: bool = False,
    tick_after_books: bool = False,
    include_aggressor: bool = False,
    settlement_state: PredictionSettlementState = PredictionSettlementState.FINALIZED,
    settlement_payout: str | None = "1",
) -> PolymarketFixtureBundle:
    if not books:
        raise ValueError("fixture requires at least one book")
    ordered_books = tuple(sorted(books, key=lambda item: item.received_monotonic_ns))
    if ordered_books != books or books[0].received_monotonic_ns <= 500:
        raise ValueError("fixture books must be monotone and follow metadata")
    raw_root = tmp_path / "raw"
    provenance = CaptureProvenance(
        collection_id=f"prediction-fixture-{tmp_path.name}",
        source_url="fixture://prediction-markets-v1",
        transport="FIXTURE",
        fixture_label=SYNTHETIC_FIXTURE_LABEL,
    )
    factory = SessionEnvelopeFactory(
        venue=Venue.POLYMARKET,
        collector_identity="prediction-fixture-v1",
        session_identity="prediction-fixture-v1",
        source_metadata_version=POLYMARKET_METADATA_VERSION,
        provenance=provenance,
    )
    envelopes: list[PublicDataEnvelope] = []

    def add(
        feed_type: str,
        payload: Any,
        *,
        monotonic_ns: int,
        utc_ns: int,
        instrument_id: str | None = None,
        market_id: str | None = "fixture-condition",
        source_url: str | None = None,
    ) -> PublicDataEnvelope:
        envelope = factory.make(
            feed_type=feed_type,
            instrument_id=instrument_id,
            market_id=market_id,
            source_timestamp_ns=None,
            receive_timestamp_utc_ns=utc_ns,
            receive_monotonic_ns=monotonic_ns,
            raw_payload=_raw(payload),
            source_event_id=f"fixture-{feed_type}-{len(envelopes) + 1}",
            infer_source_sequence_continuity=False,
            provenance=(
                None
                if source_url is None
                else CaptureProvenance(
                    provenance.collection_id,
                    source_url,
                    "FIXTURE",
                    SYNTHETIC_FIXTURE_LABEL,
                )
            ),
        )
        envelopes.append(envelope)
        return envelope

    market = {
        "acceptingOrders": True,
        "archived": False,
        "clobTokenIds": '["fixture-token-yes","fixture-token-no"]',
        "closed": False,
        "conditionId": "fixture-condition",
        "enableNegRisk": False,
        "enableOrderBook": True,
        "endDate": "2030-12-31T00:00:00Z",
        "events": [{"id": "fixture-event"}],
        "feeSchedule": {"exponent": "1", "rate": "0", "takerOnly": True},
        "id": "fixture-gamma-market",
        "minimum_tick_size": "0.01",
        "negRisk": False,
        "outcomes": '["YES","NO"]',
        "questionID": "fixture-question",
        "resolutionSource": "SYNTHETIC/FIXTURE deterministic resolver",
        "restricted": False,
        "rules": "SYNTHETIC/FIXTURE deterministic rule.",
        "startDate": "2026-01-01T00:00:00Z",
    }
    event = {
        "id": "fixture-event",
        "markets": [{"id": "fixture-gamma-market"}],
    }
    market_envelope = add("metadata", market, monotonic_ns=100, utc_ns=BASE_UTC_NS - 900)
    event_envelope = add("events", event, monotonic_ns=200, utc_ns=BASE_UTC_NS - 800)
    clob_envelope = add(
        "metadata",
        {
            "fd": {"e": "1", "r": "0", "to": True},
            "t": [
                {"o": "YES", "t": "fixture-token-yes"},
                {"o": "NO", "t": "fixture-token-no"},
            ],
        },
        monotonic_ns=300,
        utc_ns=BASE_UTC_NS - 700,
        source_url=(
            "fixture://prediction-markets-v1/polymarket/clob-markets/fixture-condition"
        ),
    )

    tick_envelope: PublicDataEnvelope | None = None
    fee_envelope: PublicDataEnvelope | None = None
    if not tick_after_books:
        tick_envelope = market_envelope
    if not fee_after_books:
        fee_envelope = add(
            "fees",
            {"base_fee": "0"},
            monotonic_ns=400,
            utc_ns=BASE_UTC_NS - 600,
        )

    for spec in books:
        add(
            "order_book",
            {
                "asks": [{"price": price, "size": size} for price, size in spec.asks],
                "asset_id": "fixture-token-yes",
                "bids": [{"price": price, "size": size} for price, size in spec.bids],
                "market": "fixture-condition",
            },
            monotonic_ns=spec.received_monotonic_ns,
            utc_ns=spec.received_time_utc_ns,
            instrument_id="fixture-token-yes",
        )

    tail_monotonic = books[-1].received_monotonic_ns
    if tick_after_books:
        tick_envelope = add(
            "tick_size",
            {"minimum_tick_size": "0.01"},
            monotonic_ns=tail_monotonic + 100,
            utc_ns=BASE_UTC_NS + tail_monotonic + 100,
        )
        tail_monotonic += 100
    if fee_after_books:
        fee_envelope = add(
            "fees",
            {"base_fee": "0"},
            monotonic_ns=tail_monotonic + 100,
            utc_ns=BASE_UTC_NS + tail_monotonic + 100,
        )
        tail_monotonic += 100
    assert tick_envelope is not None and fee_envelope is not None

    settlement_monotonic = max(tail_monotonic + 100, 800_000_000)
    settlement_envelope = add(
        "ghost_fixture",
        {
            "outcome_id": "fixture-token-yes",
            "payout": settlement_payout,
            "state": settlement_state.value,
        },
        monotonic_ns=settlement_monotonic,
        utc_ns=BASE_UTC_NS + settlement_monotonic,
    )
    aggressor_envelope: PublicDataEnvelope | None = None
    if include_aggressor:
        aggressor_envelope = add(
            "ghost_fixture",
            {
                "aggressor_side": "SELL",
                "price": "0.40",
                "quantity": "11",
                "trade_id": "fixture-aggressor",
            },
            monotonic_ns=settlement_monotonic + 100,
            utc_ns=BASE_UTC_NS + settlement_monotonic + 100,
        )

    writer = ResearchSegmentWriter(
        raw_root,
        collection_id=provenance.collection_id,
        max_segment_bytes=2_000_000,
        rotation_seconds=30,
        max_total_bytes=4_000_000,
    )
    for envelope in envelopes:
        writer.append(envelope)
    manifest = writer.close()
    assert manifest is not None
    index = PredictionRawEvidenceIndex(
        ResearchSegmentReader(raw_root, manifest_sha256=manifest.manifest_sha256)
    )
    market_ref = prediction_raw_record_ref(market_envelope, 0)
    event_ref = prediction_raw_record_ref(event_envelope, 0)
    clob_ref = prediction_raw_record_ref(clob_envelope, 0)
    fee_ref = prediction_raw_record_ref(fee_envelope, 0)
    tick_ref = prediction_raw_record_ref(tick_envelope, 0)
    graph = build_prediction_graph_from_raw(
        index,
        venue=Venue.POLYMARKET,
        market_ref=market_ref,
        event_ref=event_ref,
        clob_market_refs=(clob_ref,),
    )
    fee = PredictionFeeSchedule(
        schedule_id="fixture-fee-v1",
        venue=Venue.POLYMARKET,
        market_id=graph.market_id,
        outcome_ids=tuple(item.outcome_id for item in graph.outcomes),
        classification=EvidenceClassification.UNKNOWN_NOT_OBSERVED,
        model=FeeModel.ZERO,
        effective_from_ns=0,
        effective_to_ns=None,
        taker_rate=Decimal("0"),
        maker_rate=Decimal("0"),
        multiplier=Decimal("1"),
        exponent=Decimal("1"),
        rounding_quantum=Decimal("0.00001"),
        rounding_complete=True,
        rounding_scope="PER_FILL",
        account_precision_quantum=None,
        source_refs=tuple(
            sorted(
                {market_ref, clob_ref, fee_ref},
                key=lambda item: (item.arrival_sequence, item.raw_record_index),
            )
        ),
        synthetic_fixture=True,
    )
    tick_grid = PredictionTickGrid(
        grid_id="fixture-tick-v1",
        venue=Venue.POLYMARKET,
        market_id=graph.market_id,
        outcome_ids=tuple(item.outcome_id for item in graph.outcomes),
        classification=EvidenceClassification.UNKNOWN_NOT_OBSERVED,
        bands=(PredictionTickBand(Decimal("0"), Decimal("1"), Decimal("0.01")),),
        source_refs=(tick_ref,),
        synthetic_fixture=True,
    )
    preregistration = CandidatePreregistration.from_path(
        ROOT / "config/research/prediction-markets-candidate-v1.json"
    )
    contracts = _contracts()
    dataset = build_prediction_dataset(
        raw_root=raw_root,
        manifest_sha256=manifest.manifest_sha256,
        contracts=contracts,
        semantic_catalog=SemanticCatalog.build(()),
        graphs=(graph,),
        fee_schedules={graph.market_id: (fee,)},
        tick_grids={graph.market_id: (tick_grid,)},
    )
    settlement_ref = prediction_raw_record_ref(settlement_envelope, 0)
    settlement = PredictionSettlementEvidence(
        venue=Venue.POLYMARKET,
        market_id=graph.market_id,
        outcome_id="fixture-token-yes",
        state=settlement_state,
        source_event_time_ns=None,
        received_time_utc_ns=settlement_envelope.receive_timestamp_utc_ns,
        received_monotonic_ns=settlement_envelope.receive_monotonic_ns,
        payout_per_contract=(
            None if settlement_payout is None else Decimal(settlement_payout)
        ),
        rule_version_id=graph.rule_version.version_id,
        resolution_rule_version_id=graph.rule_version.version_id,
        source_event_id=str(settlement_envelope.source_event_id),
        raw_manifest_sha256=manifest.manifest_sha256,
        raw_root_sha256=manifest.root_sha256,
        raw_ref=settlement_ref,
        collector_identity=settlement_envelope.collector_identity,
        session_identity=settlement_envelope.session_identity,
        source_url=settlement_envelope.provenance.source_url,
        classification=EvidenceClassification.UNKNOWN_NOT_OBSERVED,
        synthetic_fixture=True,
    )
    aggressor = None
    if aggressor_envelope is not None:
        aggressor = MakerAggressorEvidence(
            venue=Venue.POLYMARKET,
            market_id=graph.market_id,
            outcome_id="fixture-token-yes",
            source_event_time_ns=None,
            received_time_utc_ns=aggressor_envelope.receive_timestamp_utc_ns,
            received_monotonic_ns=aggressor_envelope.receive_monotonic_ns,
            price=Decimal("0.40"),
            quantity=Decimal("11"),
            aggressor_side="SELL",
            source_trade_id="fixture-aggressor",
            block_trade=False,
            source_event_id="fixture-aggressor",
            raw_manifest_sha256=manifest.manifest_sha256,
            raw_root_sha256=manifest.root_sha256,
            raw_ref=prediction_raw_record_ref(aggressor_envelope, 0),
            collector_identity=aggressor_envelope.collector_identity,
            session_identity=aggressor_envelope.session_identity,
            source_url=aggressor_envelope.provenance.source_url,
        )
    return PolymarketFixtureBundle(
        raw_root=raw_root,
        manifest_sha256=manifest.manifest_sha256,
        root_sha256=manifest.root_sha256,
        envelopes=tuple(envelopes),
        graph=graph,
        fee=fee,
        tick_grid=tick_grid,
        dataset=dataset,
        preregistration=preregistration,
        contracts=contracts,
        collection_binding=None,
        settlement=settlement,
        aggressor=aggressor,
    )


def corrupt_reference(reference: PredictionRawRecordRef) -> PredictionRawRecordRef:
    return replace(reference, raw_record_sha256="0" * 64)
