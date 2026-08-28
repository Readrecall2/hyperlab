from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from typer.testing import CliRunner

import hyperlab.research_data.cli as research_cli
from hyperlab.cli import app
from hyperlab.ghost.prediction import (
    PredictionGhostReplay,
    PredictionSettlementState,
    _expected_settlement_source_time_ns,
)
from hyperlab.research_data.canonical import canonical_json_bytes, decode_canonical_json
from hyperlab.research_data.envelope import (
    CaptureProvenance,
    GapDuplicateReconnectState,
    PublicDataEnvelope,
    Venue,
)
from hyperlab.research_data.prediction import MarketRuleVersion, SemanticCatalog
from hyperlab.research_data.prediction_bundle import (
    CAMPAIGN_BOUND_EXCLUDED_SLOT_RECEIPT,
    SYNTHETIC_SOURCE_STATUS,
    UNBOUND_AVAILABILITY_OBSERVATION,
    PredictionBundleSource,
    PredictionUnavailableSource,
    VerifiedPredictionResearchBundle,
    _canonical_graph_observations,
    _Coverage,
    _discover_graphs,
    _index_aggressor_evidence,
    _is_lifecycle_candidate,
    _prospective_slot_coverage,
    _record_source_time_ns,
    _select_latest_atomic_lifecycle_event,
    _settlement_state,
    _validate_lifecycle_evidence,
    _validate_unavailable_campaign_bindings,
    build_prediction_research_bundle,
    evaluate_verified_prediction_bundle,
    replay_verified_prediction_bundle,
    verify_prediction_campaign_replay_artifact,
    verify_prediction_research_bundle,
)
from hyperlab.research_data.prediction_candidate import (
    INSUFFICIENT_PUBLIC_CORPUS,
    PredictionCollectionBinding,
    PublicSourceStatus,
    _book_projections,
    build_prediction_dataset,
    prepare_prediction_campaign,
)
from hyperlab.research_data.prediction_contracts import (
    build_prediction_semantic_catalog_from_graphs,
)
from hyperlab.research_data.prediction_evidence import (
    PredictionRawEvidenceIndex,
    prediction_raw_record_ref,
)
from hyperlab.research_data.prediction_time import prediction_rfc3339_to_ns
from hyperlab.research_data.probe import (
    ProbeConfig,
    ProbeReport,
    _kalshi_trade_ids,
    _probe_binding_payload,
    _probe_binding_sha256,
)
from hyperlab.research_data.segments import ResearchSegmentReader, ResearchSegmentWriter
from tests.prediction_support import (
    BASE_UTC_NS,
    PolymarketFixtureBundle,
    build_polymarket_fixture,
)

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "value",
    (
        "2026-08-27 01:02:03Z",
        "2026-08-27T01:02Z",
        "2026-08-27T01:02:03",
        "2026-08-27T01:02:03.1234567890Z",
        "2026-08-27T01:02:03z",
    ),
)
def test_prediction_rfc3339_parser_is_strict(value: str) -> None:
    with pytest.raises(ValueError, match="RFC3339"):
        prediction_rfc3339_to_ns(value, label="fixture timestamp")


def test_prediction_rfc3339_parser_preserves_nine_fractional_digits() -> None:
    assert prediction_rfc3339_to_ns(
        "1970-01-01T00:00:00.123456789Z",
        label="fixture timestamp",
    ) == 123_456_789
    assert prediction_rfc3339_to_ns(
        "1970-01-01T01:00:00.000000001+01:00",
        label="fixture timestamp",
    ) == 1


def test_kalshi_finality_requires_raw_settlement_timestamp() -> None:
    envelope = SimpleNamespace(
        venue=Venue.KALSHI,
        provenance=SimpleNamespace(transport="PUBLIC_HTTP"),
        source_timestamp_ns=123,
    )
    base = {
        "result": "yes",
        "settlement_value_dollars": "1",
        "status": "finalized",
    }
    state, payout, limitation, source_time = _settlement_state(
        SimpleNamespace(envelope=envelope, record=base),
        outcome_id="KXTEST:YES",
    )
    assert state is PredictionSettlementState.CLOSED_UNRESOLVED
    assert payout is None
    assert limitation == "INVALID_FINALIZED_SETTLEMENT"
    assert source_time is None

    finalized = {**base, "settlement_ts": "1970-01-01T00:00:00.123456789Z"}
    state, payout, limitation, source_time = _settlement_state(
        SimpleNamespace(envelope=envelope, record=finalized),
        outcome_id="KXTEST:YES",
    )
    assert state is PredictionSettlementState.FINALIZED
    assert payout == 1
    assert limitation is None
    assert source_time == 123_456_789

    state, payout, limitation, source_time = _settlement_state(
        SimpleNamespace(
            envelope=envelope,
            record={"settlement_ts": "1970-01-01T00:00:00Z", "status": "finalized"},
        ),
        outcome_id="KXTEST:YES",
    )
    assert state is PredictionSettlementState.CLOSED_UNRESOLVED
    assert payout is None
    assert limitation == "INVALID_FINALIZED_SETTLEMENT"
    assert source_time is None


def test_kalshi_invalid_nonterminal_timestamp_remains_a_limitation() -> None:
    envelope = SimpleNamespace(
        venue=Venue.KALSHI,
        provenance=SimpleNamespace(transport="PUBLIC_HTTP"),
        source_timestamp_ns=123,
    )
    state, payout, limitation, source_time = _settlement_state(
        SimpleNamespace(
            envelope=envelope,
            record={
                "settlement_ts": "2026-08-27 01:02:03Z",
                "status": "disputed",
            },
        ),
        outcome_id="KXTEST:YES",
    )
    assert state is PredictionSettlementState.DISPUTED
    assert payout is None
    assert limitation == "INVALID_LIFECYCLE_TIMESTAMP"
    assert source_time is None


def test_kalshi_ghost_nonterminal_timestamp_cannot_omit_valid_raw_time() -> None:
    envelope = SimpleNamespace(
        venue=Venue.KALSHI,
        feed_type="markets",
        source_timestamp_ns=456,
    )
    assert _expected_settlement_source_time_ns(
        envelope,
        {"settlement_ts": "1970-01-01T00:00:00.123456789Z"},
        PredictionSettlementState.DISPUTED,
    ) == 123_456_789
    assert (
        _expected_settlement_source_time_ns(
            envelope,
            {"settlement_ts": "not-rfc3339"},
            PredictionSettlementState.DISPUTED,
        )
        is None
    )
    assert _expected_settlement_source_time_ns(
        envelope,
        {},
        PredictionSettlementState.DISPUTED,
    ) == 456


def test_kalshi_trade_ids_require_nonempty_strings() -> None:
    assert _kalshi_trade_ids(
        [{"is_block_trade": False, "ticker": "KXTEST", "trade_id": "trade-1"}],
        expected_ticker="KXTEST",
        expected_block_trade=False,
    ) == ("trade-1",)
    for invalid in (None, 7, ""):
        with pytest.raises(ValueError, match="identity or ticker"):
            _kalshi_trade_ids(
                [{"is_block_trade": False, "ticker": "KXTEST", "trade_id": invalid}],
                expected_ticker="KXTEST",
                expected_block_trade=False,
            )


def test_aggressor_trade_identity_cannot_change_outcome(tmp_path: Path) -> None:
    fixture = build_polymarket_fixture(tmp_path / "lake", include_aggressor=True)
    assert fixture.aggressor is not None
    indexed = {}
    _index_aggressor_evidence(indexed, fixture.aggressor)
    changed = replace(
        fixture.aggressor,
        outcome_id="fixture-token-no",
        raw_ref=replace(fixture.aggressor.raw_ref, raw_record_sha256="a" * 64),
    )
    with pytest.raises(ValueError, match="source trade changed silently"):
        _index_aggressor_evidence(indexed, changed)

    duplicate = replace(
        fixture.aggressor,
        received_time_utc_ns=fixture.aggressor.received_time_utc_ns + 1,
        received_monotonic_ns=fixture.aggressor.received_monotonic_ns + 1,
        raw_ref=replace(
            fixture.aggressor.raw_ref,
            arrival_sequence=fixture.aggressor.raw_ref.arrival_sequence + 1,
        ),
    )
    _index_aggressor_evidence(indexed, duplicate)
    assert tuple(indexed.values()) == (fixture.aggressor,)


def test_polymarket_lifecycle_excludes_books_and_auxiliary_records(tmp_path: Path) -> None:
    fixture = build_polymarket_fixture(tmp_path / "lake")
    graph = fixture.graph
    resolved = SimpleNamespace(
        envelope=SimpleNamespace(
            feed_type="market_lifecycle",
            provenance=SimpleNamespace(transport="PUBLIC_WEBSOCKET"),
            source_timestamp_ns=None,
            venue=Venue.POLYMARKET,
        ),
        record={"event_type": "market_resolved", "market": graph.market_id},
    )
    book = SimpleNamespace(
        envelope=SimpleNamespace(
            feed_type="order_book",
            provenance=SimpleNamespace(transport="PUBLIC_WEBSOCKET"),
            source_timestamp_ns=None,
            venue=Venue.POLYMARKET,
        ),
        record={"event_type": "book", "market": graph.market_id},
    )
    assert _is_lifecycle_candidate(resolved, graph, synthetic=False)
    assert not _is_lifecycle_candidate(book, graph, synthetic=False)


def test_prediction_coverage_rejects_unknown_direct_and_batch_schemas(tmp_path: Path) -> None:
    fixture = build_polymarket_fixture(tmp_path / "lake")
    base = fixture.envelopes[0]
    unknown_direct = replace(base, feed_type="unknown_future_event")
    with pytest.raises(ValueError, match="UNSUPPORTED_PUBLIC_SCHEMA_FAIL_CLOSED"):
        _Coverage(SimpleNamespace(envelopes=(unknown_direct,)))

    payload = b'[{"event_type":"future_event"}]'
    unknown_batch = replace(
        base,
        content_sha256=hashlib.sha256(payload).hexdigest(),
        feed_type="market_batch",
        raw_payload=payload,
    )
    with pytest.raises(ValueError, match="UNSUPPORTED_PUBLIC_SCHEMA_FAIL_CLOSED"):
        _Coverage(SimpleNamespace(envelopes=(unknown_batch,)))


def test_polymarket_undocumented_ws_timestamps_are_not_promoted() -> None:
    entry = SimpleNamespace(
        envelope=SimpleNamespace(
            feed_type="best_bid_ask",
            source_timestamp_ns=None,
            venue=Venue.POLYMARKET,
        ),
        record={"event_type": "best_bid_ask", "timestamp": "123"},
    )
    assert _record_source_time_ns(entry) is None


def _polymarket_update_envelope(
    base: object,
    payload: object,
    *,
    arrival_delta: int,
    feed_type: str,
    state: GapDuplicateReconnectState | None = None,
) -> object:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return replace(
        base,
        arrival_sequence=base.arrival_sequence + arrival_delta,
        content_sha256=hashlib.sha256(raw).hexdigest(),
        feed_type=feed_type,
        raw_payload=raw,
        receive_monotonic_ns=base.receive_monotonic_ns + arrival_delta,
        receive_timestamp_utc_ns=base.receive_timestamp_utc_ns + arrival_delta,
        source_timestamp_ns=1_787_688_000_000_000_000,
        state=state or GapDuplicateReconnectState(),
    )


def test_polymarket_depth_state_applies_delta_and_zero_removal(tmp_path: Path) -> None:
    fixture = build_polymarket_fixture(tmp_path / "lake")
    base = next(item for item in fixture.envelopes if item.feed_type == "order_book")
    bootstrap = _polymarket_update_envelope(
        base,
        {
            "asks": [{"price": "0.44", "size": "18"}],
            "asset_id": "fixture-token-yes",
            "bids": [
                {"price": "0.41", "size": "12"},
                {"price": "0.40", "size": "9"},
            ],
            "market": fixture.graph.market_id,
        },
        arrival_delta=1,
        feed_type="order_book",
    )
    by_outcome = {item.outcome_id: fixture.graph for item in fixture.graph.outcomes}
    state = {}
    _book_projections(
        bootstrap,
        by_market={fixture.graph.market_id: fixture.graph},
        by_outcome=by_outcome,
        polymarket_state=state,
    )
    delta = _polymarket_update_envelope(
        base,
        {
            "event_type": "price_change",
            "market": fixture.graph.market_id,
            "price_changes": [
                {
                    "asset_id": "fixture-token-yes",
                    "price": "0.41",
                    "side": "BUY",
                    "size": "0",
                },
                {
                    "asset_id": "fixture-token-yes",
                    "price": "0.42",
                    "side": "BUY",
                    "size": "7",
                },
            ],
            "timestamp": "1787688000000",
        },
        arrival_delta=2,
        feed_type="price_change",
    )
    projected = _book_projections(
        delta,
        by_market={fixture.graph.market_id: fixture.graph},
        by_outcome=by_outcome,
        polymarket_state=state,
    )
    bids = projected[0][3][0][1]
    assert tuple((item.price, item.quantity) for item in bids) == (
        (Decimal("0.42"), Decimal("7")),
        (Decimal("0.40"), Decimal("9")),
    )


def test_polymarket_depth_delta_requires_bootstrap_and_resets_on_reconnect(
    tmp_path: Path,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "lake")
    base = next(item for item in fixture.envelopes if item.feed_type == "order_book")
    by_outcome = {item.outcome_id: fixture.graph for item in fixture.graph.outcomes}
    delta_payload = {
        "event_type": "price_change",
        "market": fixture.graph.market_id,
        "price_changes": [
            {
                "asset_id": "fixture-token-yes",
                "price": "0.40",
                "side": "BUY",
                "size": "5",
            }
        ],
        "timestamp": "1787688000000",
    }
    delta = _polymarket_update_envelope(
        base,
        delta_payload,
        arrival_delta=1,
        feed_type="price_change",
    )
    with pytest.raises(ValueError, match="DELTA_BEFORE_BOOK_BOOTSTRAP"):
        _book_projections(
            delta,
            by_market={fixture.graph.market_id: fixture.graph},
            by_outcome=by_outcome,
            polymarket_state={},
        )

    state = {}
    _book_projections(
        base,
        by_market={fixture.graph.market_id: fixture.graph},
        by_outcome=by_outcome,
        polymarket_state=state,
    )
    reconnect = _polymarket_update_envelope(
        base,
        delta_payload,
        arrival_delta=2,
        feed_type="price_change",
        state=GapDuplicateReconnectState(
            reconnect=True,
            reason="SYNTHETIC_RECONNECT",
        ),
    )
    with pytest.raises(ValueError, match="DELTA_BEFORE_BOOK_BOOTSTRAP"):
        _book_projections(
            reconnect,
            by_market={fixture.graph.market_id: fixture.graph},
            by_outcome=by_outcome,
            polymarket_state=state,
        )


def test_polymarket_raw_reauthentication_resumes_with_exact_graph_observation(
    tmp_path: Path,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "seed")
    initial = tuple(
        envelope
        for envelope in fixture.envelopes
        if envelope.feed_type != "ghost_fixture"
    )
    market, event, clob, fee, book = initial

    def reobserve(
        envelope: PublicDataEnvelope,
        *,
        arrival_sequence: int,
        received_monotonic_ns: int,
        state: GapDuplicateReconnectState | None = None,
    ) -> PublicDataEnvelope:
        return replace(
            envelope,
            arrival_sequence=arrival_sequence,
            receive_monotonic_ns=received_monotonic_ns,
            receive_timestamp_utc_ns=BASE_UTC_NS + received_monotonic_ns,
            source_event_id=f"fixture-reobservation-{arrival_sequence}",
            state=state or GapDuplicateReconnectState(),
        )

    reconnect = replace(
        reobserve(
            book,
            arrival_sequence=6,
            received_monotonic_ns=1_100,
            state=GapDuplicateReconnectState(
                reconnect=True,
                reason="RECONNECT_BOUNDARY",
            ),
        ),
        feed_type="heartbeat",
        instrument_id="PM:GLOBAL",
        market_id="PM:GLOBAL",
    )
    post_market = reobserve(
        market,
        arrival_sequence=7,
        received_monotonic_ns=1_200,
    )
    post_event = reobserve(
        event,
        arrival_sequence=8,
        received_monotonic_ns=1_300,
    )
    post_clob = reobserve(
        clob,
        arrival_sequence=9,
        received_monotonic_ns=1_400,
    )
    post_fee = reobserve(
        fee,
        arrival_sequence=10,
        received_monotonic_ns=1_500,
    )
    post_book = reobserve(
        book,
        arrival_sequence=11,
        received_monotonic_ns=1_600,
    )
    envelopes = (*initial, reconnect, post_market, post_event, post_clob, post_fee, post_book)
    raw_root = tmp_path / "reauthenticated" / "raw"
    writer = ResearchSegmentWriter(
        raw_root,
        collection_id=fixture.envelopes[0].provenance.collection_id,
        max_segment_bytes=2_000_000,
        rotation_seconds=30,
        max_total_bytes=4_000_000,
    )
    for envelope in envelopes:
        writer.append(envelope)
    manifest = writer.close()
    assert manifest is not None
    index = PredictionRawEvidenceIndex(
        ResearchSegmentReader(raw_root, manifest_sha256=manifest.manifest_sha256),
        contracts=fixture.contracts,
    )
    coverage = _Coverage(index)
    discovery = _discover_graphs(index, coverage, Venue.POLYMARKET)
    assert len(discovery.observations) == 2
    assert len(discovery.representatives) == 1
    assert {
        graph.rule_version.version_id for graph in discovery.observations
    } == {fixture.graph.rule_version.version_id}
    assert len({graph.raw_graph_sha256 for graph in discovery.observations}) == 2

    post_market_ref = prediction_raw_record_ref(post_market, 0)
    post_clob_ref = prediction_raw_record_ref(post_clob, 0)
    post_fee_ref = prediction_raw_record_ref(post_fee, 0)
    reauthenticated_fee = replace(
        fixture.fee,
        schedule_id="fixture-fee-reobserved-v1",
        source_refs=(post_market_ref, post_clob_ref, post_fee_ref),
    )
    reauthenticated_tick = replace(
        fixture.tick_grid,
        grid_id="fixture-tick-reobserved-v1",
        source_refs=(post_market_ref,),
    )
    fee_schedules = {
        fixture.graph.market_id: (fixture.fee, reauthenticated_fee),
    }
    tick_grids = {
        fixture.graph.market_id: (fixture.tick_grid, reauthenticated_tick),
    }
    dataset = build_prediction_dataset(
        raw_root=raw_root,
        manifest_sha256=manifest.manifest_sha256,
        contracts=fixture.contracts,
        semantic_catalog=SemanticCatalog.build(()),
        graphs=discovery.observations,
        fee_schedules=fee_schedules,
        tick_grids=tick_grids,
    )
    rows_by_arrival = {
        row.arrival_sequence: row
        for row in dataset.rows
        if row.outcome_id == "fixture-token-yes"
    }
    assert set(rows_by_arrival) == {5, 11}
    assert rows_by_arrival[5].graph_observation_sha256 == discovery.observations[0].raw_graph_sha256
    assert rows_by_arrival[11].graph_observation_sha256 == discovery.observations[1].raw_graph_sha256

    engine = PredictionGhostReplay(
        raw_root=raw_root,
        manifest_sha256=manifest.manifest_sha256,
        dataset=dataset,
        preregistration=fixture.preregistration,
        contracts=fixture.contracts,
        collection_binding=None,
        semantic_catalog=SemanticCatalog.build(()),
        identity_graphs=discovery.representatives,
        graph_observations=discovery.observations,
        fee_schedules={
            item.schedule_id: item for item in (fixture.fee, reauthenticated_fee)
        },
        tick_grids={
            item.grid_id: item for item in (fixture.tick_grid, reauthenticated_tick)
        },
        maximum_book_age_ns=fixture.preregistration.runner_policy.maximum_book_age_ns,
    )
    assert (
        engine._graph_for_snapshot(rows_by_arrival[11]).raw_graph_sha256
        == discovery.observations[1].raw_graph_sha256
    )


def test_polymarket_batch_applies_book_before_later_delta(tmp_path: Path) -> None:
    fixture = build_polymarket_fixture(tmp_path / "lake")
    base = next(item for item in fixture.envelopes if item.feed_type == "order_book")
    batch = _polymarket_update_envelope(
        base,
        [
            {
                "asks": [{"price": "0.44", "size": "18"}],
                "asset_id": "fixture-token-yes",
                "bids": [{"price": "0.41", "size": "12"}],
                "event_type": "book",
                "market": fixture.graph.market_id,
                "timestamp": "1787688000000",
            },
            {
                "event_type": "price_change",
                "market": fixture.graph.market_id,
                "price_changes": [
                    {
                        "asset_id": "fixture-token-yes",
                        "price": "0.42",
                        "side": "BUY",
                        "size": "7",
                    }
                ],
                "timestamp": "1787688000001",
            },
        ],
        arrival_delta=1,
        feed_type="market_batch",
    )
    by_outcome = {item.outcome_id: fixture.graph for item in fixture.graph.outcomes}
    projected = _book_projections(
        batch,
        by_market={fixture.graph.market_id: fixture.graph},
        by_outcome=by_outcome,
        polymarket_state={},
    )
    assert len(projected) == 2
    assert projected[-1][3][0][1][0].price == Decimal("0.42")


def test_polymarket_mixed_batch_book_cannot_see_later_graph_record(
    tmp_path: Path,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "lake")
    base = next(item for item in fixture.envelopes if item.feed_type == "order_book")
    book = {
        "asks": [{"price": "0.44", "size": "18"}],
        "asset_id": "fixture-token-yes",
        "bids": [{"price": "0.41", "size": "12"}],
        "event_type": "book",
        "market": fixture.graph.market_id,
        "timestamp": "1787688000000",
    }
    later_graph_record = {
        "asset_id": "fixture-token-yes",
        "event_type": "tick_size_change",
        "market": fixture.graph.market_id,
        "new_tick_size": "0.001",
        "old_tick_size": "0.01",
    }
    graph_maps = (
        {fixture.graph.market_id: fixture.graph},
        {outcome.outcome_id: fixture.graph for outcome in fixture.graph.outcomes},
    )
    batch = _polymarket_update_envelope(
        base,
        [book, later_graph_record],
        arrival_delta=1,
        feed_type="market_batch",
    )
    selected_indices: list[int] = []

    def select_graphs(raw_record_index: int):
        selected_indices.append(raw_record_index)
        return ({}, {}) if raw_record_index == 0 else graph_maps

    with pytest.raises(ValueError, match="absent from the identity graph"):
        _book_projections(
            batch,
            by_market=graph_maps[0],
            by_outcome=graph_maps[1],
            polymarket_state={},
            graph_selector=select_graphs,
        )
    assert selected_indices == [0]

    graph_first = _polymarket_update_envelope(
        base,
        [later_graph_record, book],
        arrival_delta=2,
        feed_type="market_batch",
    )
    projected = _book_projections(
        graph_first,
        by_market=graph_maps[0],
        by_outcome=graph_maps[1],
        polymarket_state={},
        graph_selector=lambda raw_record_index: graph_maps
        if raw_record_index == 1
        else ({}, {}),
    )
    assert projected[0][0] == 1


def _unavailable(venue: str) -> PredictionUnavailableSource:
    return PredictionUnavailableSource.from_probe_output(
        ROOT
        / "docs/evidence/prediction-markets-candidate-v1"
        / f"{venue}-public-001"
    )


def _campaign_bound_terminal_receipt(
    root: Path,
    *,
    fixture: PolymarketFixtureBundle,
    campaign: Mapping[str, object],
    venue: Venue,
    ordinal: int,
    cutoff_delta_ns: int = 0,
    network_calls: int = 1,
    positive_raw: bool = False,
) -> PredictionUnavailableSource:
    plan = fixture.preregistration.collection_plans[venue]
    starts = datetime.fromisoformat(
        str(campaign["starts_at_utc"]).replace("Z", "+00:00")
    )
    scheduled = fixture.preregistration.prospective_shard_policy.scheduled_start(
        starts,
        ordinal,
    )
    scheduled_text = scheduled.isoformat(timespec="microseconds").replace(
        "+00:00",
        "Z",
    )
    slot_start_ns = prediction_rfc3339_to_ns(
        scheduled_text,
        label="synthetic receipt slot start",
    )
    slot_end_ns = slot_start_ns + (
        fixture.preregistration.prospective_shard_policy.cadence_seconds
        * 1_000_000_000
    )
    collection_id = fixture.preregistration.prospective_shard_policy.collection_id(
        base_collection_id=plan.collection_id(str(campaign["campaign_id"])),
        campaign_manifest_sha256=str(campaign["manifest_sha256"]),
        venue=venue,
        ordinal=ordinal,
        scheduled_start=scheduled,
    )
    config = ProbeConfig(
        output_root=root,
        venue=venue,
        feeds=plan.feeds,
        instruments=(),
        census_limit=plan.census_limit,
        duration_seconds=plan.duration_seconds,
        max_bytes=plan.max_bytes,
        max_segment_bytes=plan.max_segment_bytes,
        rotation_seconds=float(plan.rotation_seconds),
        progress_interval_seconds=float(plan.progress_interval_seconds),
        collection_id=collection_id,
        max_frames=plan.max_frames,
        max_segments=plan.max_segments,
        max_network_calls=plan.max_network_calls,
        campaign_manifest_sha256=str(campaign["manifest_sha256"]),
        official_contract_sha256=fixture.contracts[venue].contract_sha256,
        candidate_config_sha256=fixture.preregistration.config_sha256,
        collection_cutoff_utc_ns_exclusive=slot_end_ns + cutoff_delta_ns,
    )
    binding_payload = _probe_binding_payload(config, collection_id=collection_id)
    binding_sha256 = _probe_binding_sha256(binding_payload)
    reports = root / "reports"
    reports.mkdir(parents=True)
    (reports / "probe-config.json").write_bytes(
        canonical_json_bytes(
            {**binding_payload, "probe_binding_sha256": binding_sha256}
        )
    )

    raw_manifest_sha256 = None
    raw_root_sha256 = None
    frame_count = 0
    segment_count = 0
    byte_count = 0
    terminal_health = PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE.value
    error = "TimeoutError: SYNTHETIC/FIXTURE terminal receipt"
    limitations = ("NO_AUTHENTICATED_RAW_FRAME", "SYNTHETIC/FIXTURE")
    report_bindings: dict[str, str | None] = {
        "campaign_manifest_sha256": None,
        "candidate_config_sha256": None,
        "official_contract_sha256": None,
        "probe_binding_sha256": None,
    }
    if positive_raw:
        if venue is not Venue.POLYMARKET:
            raise ValueError("positive raw regression fixture is Polymarket-only")
        source = fixture.envelopes[0]
        public_like = replace(
            source,
            collector_identity="prediction-terminal-receipt-fixture-v1",
            session_identity=f"probe-binding-{binding_sha256}:fixture-session",
            receive_timestamp_utc_ns=slot_start_ns + 1_000_000_000,
            receive_monotonic_ns=1,
            provenance=CaptureProvenance(
                collection_id=collection_id,
                source_url=(
                    "https://gamma-api.polymarket.com/markets/"
                    "synthetic-fixture-market"
                ),
                transport="PUBLIC_HTTP",
            ),
        )
        writer = ResearchSegmentWriter(
            root / "raw",
            collection_id=collection_id,
            max_segment_bytes=plan.max_segment_bytes,
            rotation_seconds=float(plan.rotation_seconds),
            max_total_bytes=plan.max_bytes,
        )
        writer.append(public_like)
        manifest = writer.close()
        assert manifest is not None
        raw_manifest_sha256 = manifest.manifest_sha256
        raw_root_sha256 = manifest.root_sha256
        frame_count = manifest.frame_count
        segment_count = len(manifest.segments)
        byte_count = manifest.stored_segment_bytes
        terminal_health = "PUBLIC_SOURCE_INVALID"
        error = "SYNTHETIC/FIXTURE forced fail-closed terminal"
        limitations = (
            "SYNTHETIC/FIXTURE PUBLIC-LIKE RAW FOR REGRESSION ONLY",
        )
        report_bindings = {
            "campaign_manifest_sha256": str(campaign["manifest_sha256"]),
            "candidate_config_sha256": fixture.preregistration.config_sha256,
            "official_contract_sha256": fixture.contracts[venue].contract_sha256,
            "probe_binding_sha256": binding_sha256,
        }

    result = ProbeReport(
        schema_version=1,
        boundary="PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        venue=venue.value,
        terminal_health=terminal_health,
        collection_id=collection_id,
        requested_duration_seconds=plan.duration_seconds,
        elapsed_ms=1,
        frames=frame_count,
        segments=segment_count,
        bytes=byte_count,
        gaps=0,
        duplicates=0,
        reconnects=0,
        queue_high_water=0,
        source_timestamp_min_ns=None,
        source_timestamp_max_ns=None,
        manifest_sha256=raw_manifest_sha256,
        root_sha256=raw_root_sha256,
        network_calls=network_calls,
        limitations=limitations,
        error=error,
        **report_bindings,
    )
    (reports / "result.json").write_bytes(canonical_json_bytes(result.to_dict()))
    return PredictionUnavailableSource.from_probe_output(root)


def _campaign_bound_public_source(
    root: Path,
    *,
    fixture: PolymarketFixtureBundle,
    campaign: Mapping[str, object],
    ordinal: int,
    cutoff_delta_ns: int = 0,
) -> PredictionBundleSource:
    venue = Venue.POLYMARKET
    plan = fixture.preregistration.collection_plans[venue]
    starts = datetime.fromisoformat(
        str(campaign["starts_at_utc"]).replace("Z", "+00:00")
    )
    scheduled = fixture.preregistration.prospective_shard_policy.scheduled_start(
        starts,
        ordinal,
    )
    slot_start_ns = prediction_rfc3339_to_ns(
        scheduled.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        label="synthetic public shard slot start",
    )
    slot_end_ns = slot_start_ns + (
        fixture.preregistration.prospective_shard_policy.cadence_seconds
        * 1_000_000_000
    )
    collection_id = fixture.preregistration.prospective_shard_policy.collection_id(
        base_collection_id=plan.collection_id(str(campaign["campaign_id"])),
        campaign_manifest_sha256=str(campaign["manifest_sha256"]),
        venue=venue,
        ordinal=ordinal,
        scheduled_start=scheduled,
    )
    config = ProbeConfig(
        output_root=root,
        venue=venue,
        feeds=plan.feeds,
        instruments=(),
        census_limit=plan.census_limit,
        duration_seconds=plan.duration_seconds,
        max_bytes=plan.max_bytes,
        max_segment_bytes=plan.max_segment_bytes,
        rotation_seconds=float(plan.rotation_seconds),
        progress_interval_seconds=float(plan.progress_interval_seconds),
        collection_id=collection_id,
        max_frames=plan.max_frames,
        max_segments=plan.max_segments,
        max_network_calls=plan.max_network_calls,
        campaign_manifest_sha256=str(campaign["manifest_sha256"]),
        official_contract_sha256=fixture.contracts[venue].contract_sha256,
        candidate_config_sha256=fixture.preregistration.config_sha256,
        collection_cutoff_utc_ns_exclusive=slot_end_ns + cutoff_delta_ns,
    )
    binding_payload = _probe_binding_payload(config, collection_id=collection_id)
    binding_sha256 = _probe_binding_sha256(binding_payload)
    reports = root / "reports"
    reports.mkdir(parents=True)
    (reports / "probe-config.json").write_bytes(
        canonical_json_bytes(
            {**binding_payload, "probe_binding_sha256": binding_sha256}
        )
    )
    endpoints = {
        "events": "https://gamma-api.polymarket.com/events/synthetic-fixture-event",
        "fees": "https://clob.polymarket.com/fee-rate?token_id=synthetic-fixture",
        "last_trade_price": (
            "https://clob.polymarket.com/last-trade-price?token_id=synthetic-fixture"
        ),
        "metadata": (
            "https://gamma-api.polymarket.com/markets/synthetic-fixture-market"
        ),
        "order_book": "https://clob.polymarket.com/book?token_id=synthetic-fixture",
        "public_trades": (
            "https://data-api.polymarket.com/trades?market=synthetic-fixture-market"
        ),
        "tick_size": (
            "https://clob.polymarket.com/tick-size?token_id=synthetic-fixture"
        ),
    }
    required_feeds = tuple(feed for feed in plan.feeds if feed in endpoints)
    writer = ResearchSegmentWriter(
        root / "raw",
        collection_id=collection_id,
        max_segment_bytes=plan.max_segment_bytes,
        rotation_seconds=float(plan.rotation_seconds),
        max_total_bytes=plan.max_bytes,
    )
    raw_payload = canonical_json_bytes({"fixture_label": "SYNTHETIC/FIXTURE"})
    for arrival_sequence, feed in enumerate(required_feeds, start=1):
        writer.append(
            PublicDataEnvelope.from_raw(
                venue=venue,
                feed_type=feed,
                instrument_id="synthetic-fixture-token",
                market_id="synthetic-fixture-market",
                source_timestamp_ns=None,
                receive_timestamp_utc_ns=slot_start_ns + arrival_sequence,
                receive_monotonic_ns=arrival_sequence,
                source_sequence=None,
                source_cursor=None,
                arrival_sequence=arrival_sequence,
                source_event_id=f"synthetic-fixture-{feed}",
                raw_payload=raw_payload,
                collector_identity="prediction-bound-shard-fixture-v1",
                session_identity=f"probe-binding-{binding_sha256}:fixture-session",
                state=GapDuplicateReconnectState(),
                source_metadata_version=fixture.envelopes[0].source_metadata_version,
                provenance=CaptureProvenance(
                    collection_id=collection_id,
                    source_url=endpoints[feed],
                    transport="PUBLIC_HTTP",
                ),
            )
        )
    manifest = writer.close()
    assert manifest is not None
    result = ProbeReport(
        schema_version=1,
        boundary="PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        venue=venue.value,
        terminal_health="COMPLETE",
        collection_id=collection_id,
        requested_duration_seconds=plan.duration_seconds,
        elapsed_ms=1,
        frames=manifest.frame_count,
        segments=len(manifest.segments),
        bytes=manifest.stored_segment_bytes,
        gaps=0,
        duplicates=0,
        reconnects=0,
        queue_high_water=0,
        source_timestamp_min_ns=None,
        source_timestamp_max_ns=None,
        manifest_sha256=manifest.manifest_sha256,
        root_sha256=manifest.root_sha256,
        network_calls=len(required_feeds),
        limitations=("SYNTHETIC/FIXTURE cutoff regression only",),
        error=None,
        probe_binding_sha256=binding_sha256,
        campaign_manifest_sha256=str(campaign["manifest_sha256"]),
        official_contract_sha256=fixture.contracts[venue].contract_sha256,
        candidate_config_sha256=fixture.preregistration.config_sha256,
    )
    (reports / "result.json").write_bytes(canonical_json_bytes(result.to_dict()))
    return PredictionBundleSource.from_probe_output(root)


def _campaign(tmp_path: Path) -> VerifiedPredictionResearchBundle:
    fixture = build_polymarket_fixture(tmp_path / "lake")
    signal = datetime.fromtimestamp(BASE_UTC_NS / 1_000_000_000, tz=UTC)
    manifest = prepare_prediction_campaign(
        output_root=tmp_path / "campaign",
        campaign_id="synthetic-prediction-bundle-v1",
        starts_at_utc=(signal - timedelta(days=1)).isoformat().replace("+00:00", "Z"),
        preregistration=fixture.preregistration,
        contracts=tuple(fixture.contracts.values()),
    )
    verified = build_prediction_research_bundle(
        output_root=tmp_path / "bundle",
        sources=(
            PredictionBundleSource(
                raw_root=fixture.raw_root,
                manifest_sha256=fixture.manifest_sha256,
            ),
        ),
        preregistration=fixture.preregistration,
        campaign_manifest=manifest,
        contracts=fixture.contracts,
        unavailable_sources=(_unavailable("kalshi"),),
        allow_synthetic_fixtures=True,
    )
    return verified


def test_bundle_rebuild_replay_and_evaluation_are_raw_bound_and_deterministic(
    tmp_path: Path,
) -> None:
    verified = _campaign(tmp_path)
    assert verified.source_status_by_venue == {
        Venue.POLYMARKET: SYNTHETIC_SOURCE_STATUS,
        Venue.KALSHI: PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE.value,
    }
    first_verify = verify_prediction_research_bundle(
        verified.root,
        expected_bundle_sha256=verified.bundle_sha256,
    )
    second_verify = verify_prediction_research_bundle(
        verified.root,
        expected_bundle_sha256=verified.bundle_sha256,
    )
    assert first_verify.bundle_sha256 == second_verify.bundle_sha256
    assert first_verify.manifest == second_verify.manifest

    first_replay = replay_verified_prediction_bundle(first_verify)
    second_replay = replay_verified_prediction_bundle(second_verify)
    first_bytes = canonical_json_bytes(first_replay.to_dict()) + b"\n"
    second_bytes = canonical_json_bytes(second_replay.to_dict()) + b"\n"
    assert first_bytes == second_bytes
    assert verify_prediction_campaign_replay_artifact(first_verify, first_bytes) == first_replay

    evaluation = evaluate_verified_prediction_bundle(first_verify, first_replay)
    assert evaluation["economic_evidence_status"] == PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE.value
    assert evaluation["holdout"] == {"access": "SEALED", "metrics_exposed": False}
    assert evaluation["go_no_go"] == "NO_GO_GHOST_ONLY_ECONOMIC_EVIDENCE_NOT_AVAILABLE"
    assert evaluation["source_status_by_venue"] == {
        "kalshi": PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE.value,
        "polymarket": SYNTHETIC_SOURCE_STATUS,
    }


def test_bundle_rejects_derived_artifact_and_replay_substitution(tmp_path: Path) -> None:
    verified = _campaign(tmp_path)
    replay = replay_verified_prediction_bundle(verified)
    replay_bytes = canonical_json_bytes(replay.to_dict()) + b"\n"
    with pytest.raises(ValueError, match="differs from in-process raw rebuild"):
        verify_prediction_campaign_replay_artifact(verified, replay_bytes + b" ")

    artifact = verified.root / "artifacts" / "semantic-catalog.json"
    artifact.write_bytes(artifact.read_bytes() + b" ")
    with pytest.raises(ValueError, match="artifact differs from raw rebuild"):
        verify_prediction_research_bundle(
            verified.root,
            expected_bundle_sha256=verified.bundle_sha256,
        )


def test_bundle_rejects_rehashed_raw_descriptor_substitution(tmp_path: Path) -> None:
    verified = _campaign(tmp_path)
    manifest = json.loads((verified.root / "bundle-manifest.json").read_text(encoding="utf-8"))
    manifest["shards"][0]["raw_root_sha256"] = "0" * 64
    body = {key: value for key, value in manifest.items() if key != "bundle_sha256"}
    manifest["bundle_sha256"] = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    (verified.root / "bundle-manifest.json").write_bytes(
        canonical_json_bytes(manifest) + b"\n"
    )
    with pytest.raises(ValueError, match="raw identity diverged"):
        verify_prediction_research_bundle(
            verified.root,
            expected_bundle_sha256=manifest["bundle_sha256"],
        )


def test_bundle_requires_explicit_synthetic_and_unavailable_boundaries(tmp_path: Path) -> None:
    fixture = build_polymarket_fixture(tmp_path / "lake")
    signal = datetime.fromtimestamp(BASE_UTC_NS / 1_000_000_000, tz=UTC)
    campaign = prepare_prediction_campaign(
        output_root=tmp_path / "campaign",
        campaign_id="synthetic-prediction-boundaries-v1",
        starts_at_utc=(signal - timedelta(days=1)).isoformat().replace("+00:00", "Z"),
        preregistration=fixture.preregistration,
        contracts=tuple(fixture.contracts.values()),
    )
    source = PredictionBundleSource(
        raw_root=fixture.raw_root,
        manifest_sha256=fixture.manifest_sha256,
    )
    with pytest.raises(ValueError, match="explicit fixture permission"):
        build_prediction_research_bundle(
            output_root=tmp_path / "no-synthetic-permission",
            sources=(source,),
            preregistration=fixture.preregistration,
            campaign_manifest=campaign,
            contracts=fixture.contracts,
            unavailable_sources=(_unavailable("kalshi"),),
        )
    with pytest.raises(ValueError, match="omitted a venue"):
        build_prediction_research_bundle(
            output_root=tmp_path / "no-unavailability",
            sources=(source,),
            preregistration=fixture.preregistration,
            campaign_manifest=campaign,
            contracts=fixture.contracts,
            allow_synthetic_fixtures=True,
        )
    with pytest.raises(ValueError, match="marks an included venue unavailable"):
        build_prediction_research_bundle(
            output_root=tmp_path / "included-and-unavailable",
            sources=(source,),
            preregistration=fixture.preregistration,
            campaign_manifest=campaign,
            contracts=fixture.contracts,
            unavailable_sources=(
                _unavailable("polymarket"),
                _unavailable("kalshi"),
            ),
            allow_synthetic_fixtures=True,
        )


def test_bundle_cli_rebuilds_replay_before_evaluation(tmp_path: Path) -> None:
    verified = _campaign(tmp_path)
    replay_path = tmp_path / "campaign-replay.json"
    evaluation_path = tmp_path / "evaluation.json"
    runner = CliRunner()

    replay = runner.invoke(
        app,
        [
            "ghost",
            "prediction-replay",
            "--bundle-root",
            str(verified.root),
            "--expected-bundle-sha256",
            verified.bundle_sha256,
            "--output",
            str(replay_path),
        ],
    )
    assert replay.exit_code == 0, replay.output
    assert replay_path.read_bytes() == (
        canonical_json_bytes(replay_verified_prediction_bundle(verified).to_dict()) + b"\n"
    )

    evaluation = runner.invoke(
        app,
        [
            "ghost",
            "prediction-evaluate",
            "--bundle-root",
            str(verified.root),
            "--expected-bundle-sha256",
            verified.bundle_sha256,
            "--campaign-replay",
            str(replay_path),
            "--output",
            str(evaluation_path),
        ],
    )
    assert evaluation.exit_code == 0, evaluation.output
    assert b'"holdout":{"access":"SEALED","metrics_exposed":false}' in (
        evaluation_path.read_bytes()
    )


@pytest.mark.parametrize(
    ("terminal_health", "expected_exit_code"),
    (
        ("BACKPRESSURE_LIMIT_REACHED", 5),
        ("CONTINUITY_BROKEN_FROZEN", 5),
        ("CONTINUITY_UNKNOWN_AFTER_RECONNECT_FROZEN", 5),
        ("INTERRUPTED_RECOVERABLE", 130),
    ),
)
def test_prediction_collect_returns_nonzero_for_nonadmissible_terminal_health(
    tmp_path: Path,
    monkeypatch,
    terminal_health: str,
    expected_exit_code: int,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "lake")
    plan = fixture.preregistration.collection_plans[Venue.POLYMARKET]
    campaign_root = tmp_path / "campaign"
    prepare_prediction_campaign(
        output_root=campaign_root,
        campaign_id=f"synthetic-cli-terminal-{expected_exit_code}",
        starts_at_utc=(datetime.now(UTC) - timedelta(seconds=1))
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
        preregistration=fixture.preregistration,
        contracts=tuple(fixture.contracts.values()),
    )

    def fake_probe(config, **_kwargs):
        return ProbeReport(
            schema_version=1,
            boundary="PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
            venue=config.venue.value,
            terminal_health=terminal_health,
            collection_id=config.collection_id or "fixture-collection",
            requested_duration_seconds=config.duration_seconds,
            elapsed_ms=1,
            frames=1,
            segments=1,
            bytes=100,
            gaps=0,
            duplicates=0,
            reconnects=0,
            queue_high_water=1,
            source_timestamp_min_ns=None,
            source_timestamp_max_ns=None,
            manifest_sha256="a" * 64,
            root_sha256="b" * 64,
            network_calls=1,
            limitations=("SYNTHETIC/FIXTURE terminal mapping",),
            error=None,
        )

    monkeypatch.setattr(research_cli, "run_public_probe", fake_probe)
    result = CliRunner().invoke(
        app,
        [
            "research-data",
            "prediction-collect",
            "--output-root",
            str(tmp_path / f"output-{terminal_health}"),
            "--venue",
            "polymarket",
            "--campaign-manifest",
            str(campaign_root / "campaign-manifest.json"),
            "--feeds",
            ",".join(plan.feeds),
            "--shard-ordinal",
            "0",
            "--census-limit",
            str(plan.census_limit),
            "--duration-seconds",
            str(plan.duration_seconds),
            "--max-network-calls",
            str(plan.max_network_calls),
            "--max-frames",
            str(plan.max_frames),
            "--max-bytes",
            str(plan.max_bytes),
        ],
    )
    assert result.exit_code == expected_exit_code, result.output
    assert f'"terminal_health": "{terminal_health}"' in result.output


@pytest.mark.parametrize(
    ("venue", "expected_sha256"),
    (
        (
            "polymarket-public-001",
            "4d9b15d44fd99ae1cf13fd032350e220b5dc477cfc42acdad83550a299751c5a",
        ),
        (
            "kalshi-public-001",
            "ad4ede07a268ee372127a52ba0ad406aeb8b552d5cac0a4a732a4f296736bcf7",
        ),
    ),
)
def test_bounded_public_probe_unavailability_never_invents_raw_identity(
    venue: str,
    expected_sha256: str,
) -> None:
    result_path = (
        Path(__file__).resolve().parents[2]
        / "docs/evidence/prediction-markets-candidate-v1"
        / venue
        / "reports/result.json"
    )
    raw = result_path.read_bytes()
    decoded = decode_canonical_json(raw, require_canonical=True)
    assert isinstance(decoded, dict)
    assert hashlib.sha256(raw).hexdigest() == expected_sha256
    assert decoded["terminal_health"] == PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE.value
    assert decoded["network_calls"] == 1
    assert decoded["frames"] == 0
    assert decoded["segments"] == 0
    assert decoded["bytes"] == 0
    assert decoded["manifest_sha256"] is None
    assert decoded["root_sha256"] is None


def test_unavailable_receipt_is_exact_zero_frame_and_unbound() -> None:
    for venue in ("polymarket", "kalshi"):
        source = _unavailable(venue)
        assert source.classification == UNBOUND_AVAILABILITY_OBSERVATION
        assert source.venue is Venue(venue)
        assert len(source.probe_config_sha256) == 64
        assert len(source.terminal_result_sha256) == 64


def test_committed_access_bundle_remains_legacy_compatible_and_byte_pinned() -> None:
    verified = verify_prediction_research_bundle(
        ROOT / "docs/evidence/prediction-markets-candidate-v1/access-bundle-v1",
        expected_bundle_sha256=(
            "965a42f2169c16201323477c0eb1ba7a8b540b24109c1d9252d5d9fcce55bbe5"
        ),
    )
    assert verified.prospective_slot_coverage is None
    assert len(verified.unavailable_sources) == 2
    assert all(
        item.classification == UNBOUND_AVAILABILITY_OBSERVATION
        for item in verified.unavailable_sources
    )


def test_campaign_bound_receipts_roundtrip_same_venue_and_expose_cli_ledger(
    tmp_path: Path,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "fixture")
    campaign = prepare_prediction_campaign(
        output_root=tmp_path / "campaign",
        campaign_id="fixture-bound-receipts-roundtrip",
        starts_at_utc="2026-09-01T00:00:00Z",
        preregistration=fixture.preregistration,
        contracts=tuple(fixture.contracts.values()),
    )
    receipts = tuple(
        _campaign_bound_terminal_receipt(
            tmp_path / f"receipt-{ordinal}",
            fixture=fixture,
            campaign=campaign,
            venue=Venue.POLYMARKET,
            ordinal=ordinal,
        )
        for ordinal in (0, 1)
    )
    built = build_prediction_research_bundle(
        output_root=tmp_path / "bundle",
        sources=(),
        preregistration=fixture.preregistration,
        campaign_manifest=campaign,
        contracts=fixture.contracts,
        unavailable_sources=receipts,
    )
    verified = verify_prediction_research_bundle(
        built.root,
        expected_bundle_sha256=built.bundle_sha256,
    )
    assert verified.manifest == built.manifest
    assert len(verified.unavailable_sources) == 2
    assert {item.venue for item in verified.unavailable_sources} == {
        Venue.POLYMARKET
    }
    assert {item.collection_id for item in verified.unavailable_sources} == {
        item.collection_id for item in receipts
    }
    coverage = cast(dict[str, object], verified.prospective_slot_coverage)
    venues = cast(dict[str, dict[str, object]], coverage["venues"])
    assert venues["polymarket"]["excluded_ordinals"] == [0, 1]
    assert coverage["schedule_accounted"] is False
    assert coverage["economic_corpus_complete"] is False
    descriptors = cast(
        list[dict[str, object]],
        verified.manifest["unavailable_receipts"],
    )
    assert len({item["relative_root"] for item in descriptors}) == 2

    cli_result = CliRunner().invoke(
        app,
        [
            "research-data",
            "prediction-bundle-verify",
            "--bundle-root",
            str(verified.root),
            "--expected-bundle-sha256",
            verified.bundle_sha256,
        ],
    )
    assert cli_result.exit_code == 0, cli_result.output
    cli_payload = json.loads(cli_result.output)
    assert cli_payload["prospective_slot_coverage"] == coverage

    duplicate = replace(
        receipts[0],
        probe_config_sha256="f" * 64,
        terminal_result_sha256="e" * 64,
    )
    with pytest.raises(
        ValueError,
        match="prediction unavailable campaign slot is duplicated",
    ):
        _validate_unavailable_campaign_bindings(
            (receipts[0], duplicate),
            preregistration=fixture.preregistration,
            campaign_manifest=campaign,
            contracts=fixture.contracts,
        )


def test_campaign_bound_receipt_rejects_self_hashed_wrong_slot_cutoff(
    tmp_path: Path,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "fixture")
    campaign = prepare_prediction_campaign(
        output_root=tmp_path / "campaign",
        campaign_id="fixture-wrong-receipt-cutoff",
        starts_at_utc="2026-09-01T00:00:00Z",
        preregistration=fixture.preregistration,
        contracts=tuple(fixture.contracts.values()),
    )
    receipt = _campaign_bound_terminal_receipt(
        tmp_path / "receipt",
        fixture=fixture,
        campaign=campaign,
        venue=Venue.KALSHI,
        ordinal=0,
        cutoff_delta_ns=1,
    )
    with pytest.raises(
        ValueError,
        match="cutoff diverges from its authenticated shard slot",
    ):
        build_prediction_research_bundle(
            output_root=tmp_path / "bundle",
            sources=(),
            preregistration=fixture.preregistration,
            campaign_manifest=campaign,
            contracts=fixture.contracts,
            unavailable_sources=(receipt,),
        )


def test_campaign_bound_raw_shard_rejects_self_hashed_wrong_slot_cutoff(
    tmp_path: Path,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "fixture")
    campaign = prepare_prediction_campaign(
        output_root=tmp_path / "campaign",
        campaign_id="fixture-wrong-raw-shard-cutoff",
        starts_at_utc="2026-09-01T00:00:00Z",
        preregistration=fixture.preregistration,
        contracts=tuple(fixture.contracts.values()),
    )
    source = _campaign_bound_public_source(
        tmp_path / "collection",
        fixture=fixture,
        campaign=campaign,
        ordinal=0,
        cutoff_delta_ns=1,
    )
    with pytest.raises(
        ValueError,
        match="collection cutoff diverges from its authenticated shard slot",
    ):
        build_prediction_research_bundle(
            output_root=tmp_path / "bundle",
            sources=(source,),
            preregistration=fixture.preregistration,
            campaign_manifest=campaign,
            contracts=fixture.contracts,
        )


def test_zero_call_bound_receipt_is_excluded_not_source_unavailability(
    tmp_path: Path,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "fixture")
    campaign = prepare_prediction_campaign(
        output_root=tmp_path / "campaign",
        campaign_id="fixture-zero-call-receipt",
        starts_at_utc="2026-09-01T00:00:00Z",
        preregistration=fixture.preregistration,
        contracts=tuple(fixture.contracts.values()),
    )
    receipt = _campaign_bound_terminal_receipt(
        tmp_path / "receipt",
        fixture=fixture,
        campaign=campaign,
        venue=Venue.KALSHI,
        ordinal=0,
        network_calls=0,
    )
    assert receipt.classification == CAMPAIGN_BOUND_EXCLUDED_SLOT_RECEIPT


def test_positive_raw_fail_closed_receipt_is_copied_but_never_replayed(
    tmp_path: Path,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "fixture")
    campaign = prepare_prediction_campaign(
        output_root=tmp_path / "campaign",
        campaign_id="fixture-positive-raw-terminal-receipt",
        starts_at_utc="2026-09-01T00:00:00Z",
        preregistration=fixture.preregistration,
        contracts=tuple(fixture.contracts.values()),
    )
    receipt = _campaign_bound_terminal_receipt(
        tmp_path / "receipt",
        fixture=fixture,
        campaign=campaign,
        venue=Venue.POLYMARKET,
        ordinal=2,
        positive_raw=True,
    )
    assert receipt.classification == CAMPAIGN_BOUND_EXCLUDED_SLOT_RECEIPT
    assert receipt.frame_count == 1
    built = build_prediction_research_bundle(
        output_root=tmp_path / "bundle",
        sources=(),
        preregistration=fixture.preregistration,
        campaign_manifest=campaign,
        contracts=fixture.contracts,
        unavailable_sources=(receipt,),
    )
    verified = verify_prediction_research_bundle(
        built.root,
        expected_bundle_sha256=built.bundle_sha256,
    )
    coverage = cast(dict[str, object], verified.prospective_slot_coverage)
    venues = cast(dict[str, dict[str, object]], coverage["venues"])
    assert venues["polymarket"]["excluded_ordinals"] == [2]
    assert coverage["economic_corpus_complete"] is False
    assert verified.campaign_runner is None
    assert verified.source_status_by_venue[Venue.POLYMARKET] == (
        INSUFFICIENT_PUBLIC_CORPUS
    )


@pytest.mark.parametrize(
    ("field", "value"),
    (("duplicates", 1), ("gaps", 1), ("queue_high_water", 1), ("reconnects", 1)),
)
def test_positive_raw_excluded_receipt_rejects_counter_substitution(
    tmp_path: Path,
    field: str,
    value: int,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "fixture")
    campaign = prepare_prediction_campaign(
        output_root=tmp_path / "campaign",
        campaign_id="prediction-counter-substitution-fixture",
        starts_at_utc="2026-09-01T00:00:00Z",
        preregistration=fixture.preregistration,
        contracts=tuple(fixture.contracts.values()),
    )
    receipt = _campaign_bound_terminal_receipt(
        tmp_path / "receipt",
        fixture=fixture,
        campaign=campaign,
        venue=Venue.POLYMARKET,
        ordinal=0,
        positive_raw=True,
    )
    result_path = receipt.probe_root / "reports" / "result.json"
    result = json.loads(result_path.read_bytes())
    result[field] = value
    result_path.write_bytes(canonical_json_bytes(result))
    with pytest.raises(
        ValueError,
        match=r"counters diverge|queue or reconnect counters diverge",
    ):
        PredictionUnavailableSource.from_probe_output(receipt.probe_root)


def test_bundle_verification_requires_exact_external_pin(tmp_path: Path) -> None:
    verified = _campaign(tmp_path)
    with pytest.raises(ValueError, match="manifest identity diverged"):
        verify_prediction_research_bundle(
            verified.root,
            expected_bundle_sha256="0" * 64,
        )


def test_prospective_slot_coverage_exposes_missing_excluded_and_nonreplayable(
    tmp_path: Path,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "fixture")
    campaign = prepare_prediction_campaign(
        output_root=tmp_path / "campaign",
        campaign_id="fixture-slot-ledger-v1",
        starts_at_utc="2026-09-01T00:00:00Z",
        preregistration=fixture.preregistration,
        contracts=tuple(fixture.contracts.values()),
    )
    replayable = SimpleNamespace(
        synthetic=False,
        venue=Venue.POLYMARKET,
        prospective_ordinal=0,
        engine=object(),
    )
    partial = _prospective_slot_coverage(
        shards=(replayable,),
        unavailable_slots={},
        preregistration=fixture.preregistration,
        campaign_manifest=campaign,
    )
    assert partial is not None
    assert partial["schedule_accounted"] is False
    assert partial["economic_corpus_complete"] is False
    assert cast(dict[str, object], partial["venues"])["polymarket"]

    expected = cast(int, partial["expected_ordinal_exclusive"])

    def excluded(venue: Venue, ordinal: int) -> PredictionUnavailableSource:
        return PredictionUnavailableSource(
            probe_root=tmp_path / f"excluded-{venue.value}-{ordinal}",
            venue=venue,
            collection_id=f"excluded-{venue.value}-{ordinal}",
            probe_config_sha256=hashlib.sha256(
                f"config:{venue.value}:{ordinal}".encode()
            ).hexdigest(),
            terminal_result_sha256=hashlib.sha256(
                f"result:{venue.value}:{ordinal}".encode()
            ).hexdigest(),
            probe_payload={},
            classification=CAMPAIGN_BOUND_EXCLUDED_SLOT_RECEIPT,
            campaign_manifest_sha256=cast(str, campaign["manifest_sha256"]),
            candidate_config_sha256=fixture.preregistration.config_sha256,
            official_contract_sha256=fixture.contracts[venue].contract_sha256,
            terminal_health="PUBLIC_SOURCE_UNAVAILABLE",
            frame_count=0,
            raw_manifest_sha256=None,
            raw_root_sha256=None,
        )

    excluded_slots = {
        (venue, ordinal): excluded(venue, ordinal)
        for venue in (Venue.POLYMARKET, Venue.KALSHI)
        for ordinal in range(expected)
        if (venue, ordinal) != (Venue.POLYMARKET, 0)
    }
    accounted = _prospective_slot_coverage(
        shards=(replayable,),
        unavailable_slots=excluded_slots,
        preregistration=fixture.preregistration,
        campaign_manifest=campaign,
    )
    assert accounted is not None
    assert accounted["schedule_accounted"] is True
    assert accounted["economic_corpus_complete"] is False
    assert len(cast(list[object], accounted["excluded_receipts"])) == 2 * expected - 1

    binding = SimpleNamespace(
        frame_count=1,
        probe_binding_sha256="a" * 64,
        terminal_result_sha256="b" * 64,
    )
    nonreplayable = SimpleNamespace(
        synthetic=False,
        venue=Venue.POLYMARKET,
        prospective_ordinal=1,
        engine=None,
        binding=binding,
        manifest_sha256="c" * 64,
        index=SimpleNamespace(root_sha256="d" * 64),
    )
    nonreplayable_coverage = _prospective_slot_coverage(
        shards=(replayable, nonreplayable),
        unavailable_slots={},
        preregistration=fixture.preregistration,
        campaign_manifest=campaign,
    )
    assert nonreplayable_coverage is not None
    assert nonreplayable_coverage["economic_corpus_complete"] is False
    receipts = cast(
        list[dict[str, object]],
        nonreplayable_coverage["nonreplayable_raw_receipts"],
    )
    assert [(item["venue"], item["ordinal"]) for item in receipts] == [("polymarket", 1)]


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("frames", 1),
        ("manifest_sha256", "0" * 64),
        ("network_calls", 121),
        ("error", ""),
        ("terminal_health", "COMPLETE"),
    ),
)
def test_unavailable_receipt_rejects_zero_frame_or_budget_tamper(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    root = tmp_path / "tampered-probe"
    shutil.copytree(_unavailable("kalshi").probe_root, root)
    result_path = root / "reports" / "result.json"
    result = json.loads(result_path.read_text(encoding="utf-8"))
    result[field] = value
    result_path.write_bytes(canonical_json_bytes(result))
    with pytest.raises(
        ValueError,
        match=(
            r"terminal invariants diverged|zero-frame prediction slot receipt|"
            r"requires campaign binding|lacks direct evidence|"
            r"terminal unexpectedly carries an error"
        ),
    ):
        PredictionUnavailableSource.from_probe_output(root)


def test_bound_collection_rejects_self_consistent_raw_over_frozen_frame_cap(
    tmp_path: Path,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "fixture")
    collection_root = tmp_path / "forged-bound-collection"
    shutil.copytree(fixture.raw_root, collection_root / "raw")
    reader = ResearchSegmentReader(
        collection_root / "raw",
        manifest_sha256=fixture.manifest_sha256,
    )
    manifest = reader.manifest
    reports = collection_root / "reports"
    reports.mkdir(parents=True)
    plan = fixture.preregistration.collection_plans[Venue.POLYMARKET]
    probe_config = ProbeConfig(
        output_root=collection_root,
        venue=Venue.POLYMARKET,
        feeds=plan.feeds,
        instruments=(),
        census_limit=plan.census_limit,
        duration_seconds=plan.duration_seconds,
        max_bytes=plan.max_bytes,
        max_segment_bytes=plan.max_segment_bytes,
        rotation_seconds=float(plan.rotation_seconds),
        progress_interval_seconds=float(plan.progress_interval_seconds),
        collection_id=manifest.collection_id,
        max_frames=manifest.frame_count - 1,
        max_segments=plan.max_segments,
        max_network_calls=plan.max_network_calls,
        campaign_manifest_sha256="a" * 64,
        official_contract_sha256=fixture.contracts[
            Venue.POLYMARKET
        ].contract_sha256,
        candidate_config_sha256=fixture.preregistration.config_sha256,
        collection_cutoff_utc_ns_exclusive=2_000_000_000_000_000_000,
    )
    binding_payload = _probe_binding_payload(
        probe_config,
        collection_id=manifest.collection_id,
    )
    binding_sha256 = _probe_binding_sha256(binding_payload)
    (reports / "probe-config.json").write_bytes(
        canonical_json_bytes(
            {**binding_payload, "probe_binding_sha256": binding_sha256}
        )
    )
    result = ProbeReport(
        schema_version=1,
        boundary="PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        venue=Venue.POLYMARKET.value,
        terminal_health="COMPLETE",
        collection_id=manifest.collection_id,
        requested_duration_seconds=plan.duration_seconds,
        elapsed_ms=1,
        frames=manifest.frame_count,
        segments=len(manifest.segments),
        bytes=manifest.stored_segment_bytes,
        gaps=0,
        duplicates=0,
        reconnects=0,
        queue_high_water=0,
        source_timestamp_min_ns=None,
        source_timestamp_max_ns=None,
        manifest_sha256=manifest.manifest_sha256,
        root_sha256=manifest.root_sha256,
        network_calls=1,
        limitations=(),
        error=None,
        probe_binding_sha256=binding_sha256,
        campaign_manifest_sha256="a" * 64,
        official_contract_sha256=fixture.contracts[
            Venue.POLYMARKET
        ].contract_sha256,
        candidate_config_sha256=fixture.preregistration.config_sha256,
    )
    (reports / "result.json").write_bytes(canonical_json_bytes(result.to_dict()))
    with pytest.raises(ValueError, match="terminal collection result is not admissible"):
        PredictionCollectionBinding.from_probe_output(collection_root)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("economic_claim", "ALPHA_CERTIFIED"),
        ("status", "ECONOMICALLY_READY"),
        ("unexpected", True),
    ),
)
def test_bundle_rehashed_claim_or_schema_tamper_still_fails(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    verified = _campaign(tmp_path)
    manifest_path = verified.root / "bundle-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest[field] = value
    body = {key: item for key, item in manifest.items() if key != "bundle_sha256"}
    manifest["bundle_sha256"] = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    manifest_path.write_bytes(canonical_json_bytes(manifest) + b"\n")
    with pytest.raises(ValueError, match="manifest identity diverged"):
        verify_prediction_research_bundle(
            verified.root,
            expected_bundle_sha256=manifest["bundle_sha256"],
        )


def test_multishard_graph_reobservations_keep_semantic_versions_causal(
    tmp_path: Path,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "lake")
    graph = fixture.graph
    exact_graphs, exact_versions = _canonical_graph_observations(
        (
            SimpleNamespace(graph_observations=(graph,)),
            SimpleNamespace(graph_observations=(graph,)),
        )
    )
    assert exact_graphs == (graph,)
    assert set(exact_versions.values()) == {1}
    exact_catalog = build_prediction_semantic_catalog_from_graphs(
        exact_graphs,
        semantic_versions=exact_versions,
    )
    assert len(exact_catalog.relations) == 2

    distinct_refs = tuple(
        replace(
            reference,
            arrival_sequence=reference.arrival_sequence + 100,
            content_sha256=hashlib.sha256(
                f"reobservation:{index}".encode()
            ).hexdigest(),
        )
        for index, reference in enumerate(graph.source_refs)
    )
    reobserved = replace(
        graph,
        raw_graph_sha256="a" * 64,
        source_refs=distinct_refs,
    )
    graphs, versions = _canonical_graph_observations(
        (
            SimpleNamespace(graph_observations=(graph,)),
            SimpleNamespace(graph_observations=(reobserved,)),
        )
    )
    assert len(graphs) == 2
    assert set(versions.values()) == {1}
    catalog = build_prediction_semantic_catalog_from_graphs(
        graphs,
        semantic_versions=versions,
    )
    assert len(catalog.relations) == 4
    assert {relation.version for relation in catalog.relations} == {1}


def test_multishard_unordered_rule_versions_fail_closed(tmp_path: Path) -> None:
    fixture = build_polymarket_fixture(tmp_path / "lake")
    graph = fixture.graph
    rule = graph.rule_version
    closed_rule = MarketRuleVersion.create(
        venue=rule.venue,
        economic_market_id=rule.economic_market_id,
        rule_text=rule.rule_text,
        resolution_source=rule.resolution_source,
        opens_at=rule.opens_at,
        closes_at=rule.closes_at,
        resolves_at=rule.resolves_at,
        market_status="CLOSED",
        outcomes=rule.outcomes,
        source_metadata_version=rule.source_metadata_version,
        raw_content_sha256="b" * 64,
    )
    closed = replace(
        graph,
        rule_version=closed_rule,
        raw_graph_sha256="c" * 64,
        execution_admissible=False,
        ineligibility_reasons=("MARKET_CLOSED",),
    )
    with pytest.raises(ValueError, match="CROSS_SHARD_RULE_TIMELINE_UNAUTHENTICATED"):
        _canonical_graph_observations(
            (
                SimpleNamespace(graph_observations=(graph,)),
                SimpleNamespace(graph_observations=(closed,)),
            )
        )


def test_lifecycle_latest_state_is_atomic_and_conflicts_fail_closed(
    tmp_path: Path,
) -> None:
    fixture = build_polymarket_fixture(tmp_path / "lake")
    terminal = fixture.settlement
    disputed = replace(
        terminal,
        state=PredictionSettlementState.DISPUTED,
        payout_per_contract=None,
        received_time_utc_ns=terminal.received_time_utc_ns + 10,
        received_monotonic_ns=terminal.received_monotonic_ns + 10,
        source_event_id="fixture-disputed-later",
        raw_ref=replace(terminal.raw_ref, raw_record_sha256="d" * 64),
    )
    selected = _select_latest_atomic_lifecycle_event(
        (terminal, disputed),
        required_outcomes={terminal.outcome_id},
    )
    assert selected[terminal.outcome_id].state is PredictionSettlementState.DISPUTED
    assert selected[terminal.outcome_id].payout_per_contract is None

    no_outcome = next(
        item.outcome_id
        for item in fixture.graph.outcomes
        if item.outcome_id != terminal.outcome_id
    )
    different_event_no = replace(
        terminal,
        outcome_id=no_outcome,
        payout_per_contract=terminal.payout_per_contract - 1,
        source_event_id="fixture-other-atomic-event",
        raw_ref=replace(terminal.raw_ref, raw_record_sha256="e" * 64),
    )
    with pytest.raises(ValueError, match="no atomic raw-bound lifecycle evidence"):
        _select_latest_atomic_lifecycle_event(
            (terminal, different_event_no),
            required_outcomes={terminal.outcome_id, no_outcome},
        )

    conflicting = replace(
        terminal,
        payout_per_contract=terminal.payout_per_contract - 1,
        received_time_utc_ns=terminal.received_time_utc_ns + 20,
        received_monotonic_ns=terminal.received_monotonic_ns + 20,
        source_event_id="fixture-conflicting-terminal",
        raw_ref=replace(terminal.raw_ref, raw_record_sha256="f" * 64),
    )
    with pytest.raises(ValueError, match="CONFLICTING_TERMINAL_SETTLEMENT"):
        _validate_lifecycle_evidence(
            (
                SimpleNamespace(
                    semantic_graphs=(fixture.graph,),
                    settlements=(terminal, conflicting),
                ),
            )
        )

    closed_after_terminal = replace(
        terminal,
        state=PredictionSettlementState.CLOSED_UNRESOLVED,
        payout_per_contract=None,
        received_time_utc_ns=terminal.received_time_utc_ns + 30,
        received_monotonic_ns=terminal.received_monotonic_ns + 30,
        source_event_id="fixture-closed-after-terminal",
        raw_ref=replace(terminal.raw_ref, raw_record_sha256="1" * 64),
    )
    with pytest.raises(ValueError, match="TERMINAL_LIFECYCLE_REGRESSION_INVALID"):
        _validate_lifecycle_evidence(
            (
                SimpleNamespace(
                    semantic_graphs=(fixture.graph,),
                    settlements=(terminal, closed_after_terminal),
                ),
            )
        )

    cross_domain_disputed = replace(
        disputed,
        session_identity="fixture-other-clock-domain:0",
    )
    with pytest.raises(ValueError, match="CROSS_SHARD_LIFECYCLE_ORDER_UNAUTHENTICATED"):
        _validate_lifecycle_evidence(
            (
                SimpleNamespace(
                    semantic_graphs=(fixture.graph,),
                    settlements=(terminal, cross_domain_disputed),
                ),
            )
        )


def test_evaluation_rejects_verified_but_noncanonical_replay(tmp_path: Path) -> None:
    verified = _campaign(tmp_path)
    replay = replay_verified_prediction_bundle(verified)
    changed = replace(
        replay,
        evidence_cutoff_utc_ns_exclusive=replay.evidence_cutoff_utc_ns_exclusive + 1,
    )
    changed = replace(changed, _verified_report_sha256=changed.report_sha256)
    assert changed.replay_verified is True
    with pytest.raises(ValueError, match="differs from canonical raw resolver"):
        evaluate_verified_prediction_bundle(verified, changed)
