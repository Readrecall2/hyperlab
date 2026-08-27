from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest
import typer

from hyperlab.ghost.prediction import (
    PredictionExecutionRole,
    build_synthetic_prediction_campaign_replay,
)
from hyperlab.research_data.canonical import decode_canonical_json
from hyperlab.research_data.cli import _validate_prediction_shard_window
from hyperlab.research_data.envelope import Venue
from hyperlab.research_data.prediction import SemanticCatalog
from hyperlab.research_data.prediction_candidate import (
    AWAITING_HUMAN_EXECUTION,
    INSUFFICIENT_PUBLIC_CORPUS,
    CandidatePreregistration,
    PublicSourceStatus,
    build_prediction_dataset,
    build_prediction_replay_seal,
    build_prediction_split_plan,
    evaluate_preregistered,
    prediction_prospective_shard_ordinal,
    prepare_prediction_campaign,
    verify_prediction_collection_plan_payload,
)
from hyperlab.research_data.prediction_contracts import (
    OfficialPublicContract,
    revalidate_prediction_graph,
)
from hyperlab.research_data.prediction_evidence import (
    PredictionRawEvidenceIndex,
    prediction_raw_record_ref,
)
from hyperlab.research_data.segments import ResearchSegmentReader
from tests.prediction_support import (
    BASE_UTC_NS,
    BookSpec,
    build_polymarket_fixture,
    corrupt_reference,
)

ROOT = Path(__file__).resolve().parents[2]


def _candidate() -> CandidatePreregistration:
    return CandidatePreregistration.from_path(
        ROOT / "config/research/prediction-markets-candidate-v1.json"
    )


def _contracts() -> tuple[OfficialPublicContract, OfficialPublicContract]:
    return (
        OfficialPublicContract.from_path(
            ROOT / "config/research/polymarket-public-contract-v1.json"
        ),
        OfficialPublicContract.from_path(
            ROOT / "config/research/kalshi-public-contract-v1.json"
        ),
    )


def test_candidate_preregisters_all_variants_and_seals_holdout() -> None:
    candidate = _candidate()
    assert candidate.holdout_access == "SEALED"
    assert candidate.primary_technical_family == "K4_COMPLETE_SET_LOGICAL_RV"
    assert {item.family_id for item in candidate.variants} == {
        "K4_COMPLETE_SET_LOGICAL_RV",
        "K5_INCENTIVE_QUEUE",
        "K6_CROSS_VENUE_EQUIVALENCE",
    }
    assert len({item.parameters_sha256 for item in candidate.variants}) == len(candidate.variants)
    assert any("NEGATIVE_CONTROL" in item.role for item in candidate.variants)
    policy = candidate.prospective_shard_policy
    assert policy.campaign_days == 28
    assert policy.expected_shards_per_venue == 672
    assert policy.collection_duration_seconds == 120
    assert policy.cadence_seconds == 3600
    assert policy.missed_slot_policy == "RECORD_GAP_NO_BACKFILL"


def test_prediction_shard_start_reserves_the_full_frozen_duration() -> None:
    scheduled = datetime(2026, 9, 1, tzinfo=UTC)
    last_admissible_start = scheduled + timedelta(seconds=3_600 - 120)
    _validate_prediction_shard_window(
        now=last_admissible_start,
        scheduled_start=scheduled,
        collection_duration_seconds=120,
        cadence_seconds=3_600,
    )
    with pytest.raises(typer.BadParameter, match="full frozen collection duration"):
        _validate_prediction_shard_window(
            now=last_admissible_start + timedelta(microseconds=1),
            scheduled_start=scheduled,
            collection_duration_seconds=120,
            cadence_seconds=3_600,
        )


def test_point_in_time_dataset_reconstructs_authenticated_objects_and_is_deterministic(
    tmp_path: Path,
) -> None:
    bundle = build_polymarket_fixture(tmp_path)
    row = bundle.dataset.rows[0]
    assert row.received_time_utc_ns == BASE_UTC_NS
    assert row.received_monotonic_ns == 1_000
    assert row.execution_eligible is True
    assert row.raw_manifest_sha256 == bundle.manifest_sha256
    rebuilt = build_prediction_dataset(
        raw_root=bundle.raw_root,
        manifest_sha256=bundle.manifest_sha256,
        contracts=bundle.contracts,
        semantic_catalog=SemanticCatalog.build(()),
        graphs=(bundle.graph,),
        fee_schedules={bundle.graph.market_id: (bundle.fee,)},
        tick_grids={bundle.graph.market_id: (bundle.tick_grid,)},
    )
    assert rebuilt.dataset_sha256 == bundle.dataset.dataset_sha256
    assert rebuilt.rows == bundle.dataset.rows
    with pytest.raises(ValueError, match="SAME_TIMESTAMP"):
        row.assert_causal_decision(
            signal_time_utc_ns=row.received_time_utc_ns,
            signal_monotonic_ns=row.received_monotonic_ns,
            decision_time_utc_ns=row.received_time_utc_ns,
            decision_monotonic_ns=row.received_monotonic_ns,
            maximum_age_ns=1_000_000,
        )


def test_graph_and_raw_record_revalidation_reject_content_substitution(tmp_path: Path) -> None:
    bundle = build_polymarket_fixture(tmp_path)
    index = PredictionRawEvidenceIndex(
        ResearchSegmentReader(bundle.raw_root, manifest_sha256=bundle.manifest_sha256)
    )
    with pytest.raises(ValueError, match="diverged"):
        revalidate_prediction_graph(
            index,
            replace(bundle.graph, raw_graph_sha256="f" * 64),
        )
    market_ref = prediction_raw_record_ref(bundle.envelopes[0], 0)
    with pytest.raises(ValueError, match="diverged"):
        index.require_record(
            corrupt_reference(market_ref),
            venue=Venue.POLYMARKET,
            allowed_feeds=("metadata",),
        )


def test_future_fee_is_not_applied_retroactively(tmp_path: Path) -> None:
    bundle = build_polymarket_fixture(tmp_path, fee_after_books=True)
    row = bundle.dataset.rows[0]
    assert row.execution_eligible is False
    assert row.ineligibility_reasons == ("FEE_SCHEDULE_UNKNOWN_FAIL_CLOSED",)
    assert row.fee_schedule_id.startswith("UNKNOWN:")


def test_future_tick_is_not_applied_retroactively(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no causal authenticated tick grid"):
        build_polymarket_fixture(tmp_path, tick_after_books=True)


def test_dataset_orders_by_monotone_arrival_even_when_utc_regresses(tmp_path: Path) -> None:
    bundle = build_polymarket_fixture(
        tmp_path,
        books=(
            BookSpec(1_000, BASE_UTC_NS),
            BookSpec(
                400_000_000,
                BASE_UTC_NS - 10,
                asks=(("0.43", "5"),),
            ),
        ),
    )
    assert len(bundle.dataset.rows) == 2
    assert [item.received_monotonic_ns for item in bundle.dataset.rows] == [1_000, 400_000_000]
    assert bundle.dataset.rows[1].received_time_utc_ns < bundle.dataset.rows[0].received_time_utc_ns


def test_evaluation_keeps_losers_and_seals_holdout(tmp_path: Path) -> None:
    bundle = build_polymarket_fixture(tmp_path / "lake")
    intent = bundle.intent(role=PredictionExecutionRole.MAKER, quantity="1")
    report = bundle.replay().run(
        intent=intent,
        settlement=bundle.settlement,
        report_time_utc_ns=bundle.settlement.received_time_utc_ns + 100,
        report_time_monotonic_ns=bundle.settlement.received_monotonic_ns + 100,
    )
    signal = datetime.fromtimestamp(report.signal_time_utc_ns / 1_000_000_000, tz=UTC)
    campaign_start = signal - timedelta(days=15)
    campaign = prepare_prediction_campaign(
        output_root=tmp_path / "campaign",
        campaign_id="fixture-prediction-campaign",
        starts_at_utc=campaign_start.isoformat().replace("+00:00", "Z"),
        preregistration=bundle.preregistration,
        contracts=_contracts(),
    )
    selection = build_prediction_split_plan(
        preregistration=bundle.preregistration,
        dataset_sha256=bundle.dataset.dataset_sha256,
        prospective_start=campaign_start,
    ).selection_view
    campaign_replay = build_synthetic_prediction_campaign_replay(
        (report,),
        preregistration=bundle.preregistration,
        semantic_catalog_sha256=SemanticCatalog.build(()).catalog_sha256,
        seal=build_prediction_replay_seal(
            campaign_manifest=campaign,
            preregistration=bundle.preregistration,
            selection_view=selection,
        ),
    )
    result = evaluate_preregistered(
        preregistration=bundle.preregistration,
        selection_view=selection,
        campaign_manifest=campaign,
        campaign_replay=campaign_replay,
        source_status=PublicSourceStatus.OBSERVED_PUBLICLY,
    )
    assert result["economic_evidence_status"] == INSUFFICIENT_PUBLIC_CORPUS
    assert len(cast(list[object], result["variants"])) == len(bundle.preregistration.variants)
    assert result["holdout"] == {"access": "SEALED", "metrics_exposed": False}
    holdout_start = signal - timedelta(
        days=bundle.preregistration.train_days + bundle.preregistration.validation_days
    )
    holdout_campaign = prepare_prediction_campaign(
        output_root=tmp_path / "holdout-campaign",
        campaign_id="fixture-sealed-holdout",
        starts_at_utc=holdout_start.isoformat().replace("+00:00", "Z"),
        preregistration=bundle.preregistration,
        contracts=_contracts(),
    )
    holdout_selection = build_prediction_split_plan(
        preregistration=bundle.preregistration,
        dataset_sha256=bundle.dataset.dataset_sha256,
        prospective_start=holdout_start,
    ).selection_view
    with pytest.raises(ValueError, match="campaign replay binding"):
        evaluate_preregistered(
            preregistration=bundle.preregistration,
            selection_view=holdout_selection,
            campaign_manifest=holdout_campaign,
            campaign_replay=campaign_replay,
            source_status=PublicSourceStatus.OBSERVED_PUBLICLY,
        )


def test_evaluation_rejects_verified_validation_to_train_rollback(tmp_path: Path) -> None:
    bundle = build_polymarket_fixture(tmp_path / "lake")
    source = bundle.replay().run(
        intent=bundle.intent(role=PredictionExecutionRole.MAKER, quantity="1"),
        settlement=bundle.settlement,
        report_time_utc_ns=bundle.settlement.received_time_utc_ns + 100,
        report_time_monotonic_ns=bundle.settlement.received_monotonic_ns + 100,
    )
    day_ns = 86_400 * 1_000_000_000

    def shifted(*, day_offset: int, suffix: str):
        delta = day_offset * day_ns
        changed = replace(
            source,
            order_id=f"synthetic-fixture-{suffix}",
            signal_time_utc_ns=source.signal_time_utc_ns + delta,
            signal_received_monotonic_ns=(
                source.signal_received_monotonic_ns + delta
            ),
            decision_time_utc_ns=source.decision_time_utc_ns + delta,
            decision_monotonic_ns=source.decision_monotonic_ns + delta,
            admission_time_utc_ns=source.admission_time_utc_ns + delta,
            admission_monotonic_ns=source.admission_monotonic_ns + delta,
            max_evidence_received_utc_ns=(
                source.max_evidence_received_utc_ns + delta
            ),
            report_time_utc_ns=source.report_time_utc_ns + delta,
            report_time_monotonic_ns=source.report_time_monotonic_ns + delta,
        )
        return replace(changed, _verified_report_sha256=changed.report_sha256)

    train_report = shifted(day_offset=0, suffix="train")
    validation_report = shifted(
        day_offset=bundle.preregistration.train_days,
        suffix="validation",
    )
    campaign_start = datetime.fromtimestamp(
        train_report.signal_time_utc_ns / 1_000_000_000,
        tz=UTC,
    ) - timedelta(days=1)
    campaign = prepare_prediction_campaign(
        output_root=tmp_path / "campaign",
        campaign_id="fixture-causal-split-rollback",
        starts_at_utc=campaign_start.isoformat().replace("+00:00", "Z"),
        preregistration=bundle.preregistration,
        contracts=_contracts(),
    )
    selection = build_prediction_split_plan(
        preregistration=bundle.preregistration,
        dataset_sha256=bundle.dataset.dataset_sha256,
        prospective_start=campaign_start,
    ).selection_view
    replay = build_synthetic_prediction_campaign_replay(
        (train_report, validation_report),
        preregistration=bundle.preregistration,
        semantic_catalog_sha256=SemanticCatalog.build(()).catalog_sha256,
        seal=build_prediction_replay_seal(
            campaign_manifest=campaign,
            preregistration=bundle.preregistration,
            selection_view=selection,
        ),
    )
    rollback = replace(
        replay,
        opportunities=tuple(reversed(replay.opportunities)),
        reports=tuple(reversed(replay.reports)),
    )
    rollback = replace(
        rollback,
        _verified_report_sha256=rollback.report_sha256,
    )
    assert rollback.replay_verified is True

    with pytest.raises(ValueError, match="PREDICTION_CAUSAL_SPLIT_ROLLBACK"):
        evaluate_preregistered(
            preregistration=bundle.preregistration,
            selection_view=selection,
            campaign_manifest=campaign,
            campaign_replay=rollback,
            source_status=PublicSourceStatus.OBSERVED_PUBLICLY,
        )


def test_campaign_pack_is_local_unique_and_awaits_human_execution(tmp_path: Path) -> None:
    output = tmp_path / "campaign"
    manifest = prepare_prediction_campaign(
        output_root=output,
        campaign_id="fixture-prediction-campaign",
        starts_at_utc="2026-09-01T00:00:00Z",
        preregistration=_candidate(),
        contracts=_contracts(),
    )
    assert manifest["status"] == AWAITING_HUMAN_EXECUTION
    assert manifest["vps_or_h1_path"] == "NONE"
    assert manifest["prospective_shard_policy"] == _candidate().prospective_shard_policy.to_dict()
    assert (output / "operator-commands.json").read_text().count("never_execute") == 1
    assert "--shard-ordinal <SCHEDULED_SHARD_ORDINAL>" in (
        output / "operator-commands.json"
    ).read_text(encoding="utf-8")
    with pytest.raises(FileExistsError):
        prepare_prediction_campaign(
            output_root=output,
            campaign_id="fixture-prediction-campaign",
            starts_at_utc="2026-09-01T00:00:00Z",
            preregistration=_candidate(),
            contracts=_contracts(),
        )


def test_prospective_shard_identity_is_fully_recomputed(tmp_path: Path) -> None:
    candidate = _candidate()
    manifest = prepare_prediction_campaign(
        output_root=tmp_path / "campaign-shards",
        campaign_id="fixture-shard-authentication",
        starts_at_utc="2026-09-01T00:00:00Z",
        preregistration=candidate,
        contracts=_contracts(),
    )
    venue = Venue.POLYMARKET
    ordinal = 17
    starts = datetime.fromisoformat(str(manifest["starts_at_utc"]).replace("Z", "+00:00"))
    scheduled = candidate.prospective_shard_policy.scheduled_start(starts, ordinal)
    collection_id = candidate.prospective_shard_policy.collection_id(
        base_collection_id=candidate.collection_plans[venue].collection_id(
            str(manifest["campaign_id"])
        ),
        campaign_manifest_sha256=str(manifest["manifest_sha256"]),
        venue=venue,
        ordinal=ordinal,
        scheduled_start=scheduled,
    )
    assert prediction_prospective_shard_ordinal(
        preregistration=candidate,
        campaign_manifest=manifest,
        venue=venue,
        collection_id=collection_id,
    ) == ordinal

    changed_suffix = f"{collection_id[:-1]}{'0' if collection_id[-1] != '0' else '1'}"
    with pytest.raises(ValueError, match="digest diverged"):
        prediction_prospective_shard_ordinal(
            preregistration=candidate,
            campaign_manifest=manifest,
            venue=venue,
            collection_id=changed_suffix,
        )
    with pytest.raises(ValueError, match="not a prospective campaign shard"):
        prediction_prospective_shard_ordinal(
            preregistration=candidate,
            campaign_manifest=manifest,
            venue=venue,
            collection_id=candidate.collection_plans[venue].collection_id(
                str(manifest["campaign_id"])
            ),
        )


def test_campaign_collection_binding_must_match_every_frozen_plan_field() -> None:
    plan = _candidate().collection_plans[Venue.KALSHI]
    payload = {
        "census_limit": plan.census_limit,
        "duration_seconds": plan.duration_seconds,
        "feeds": list(plan.feeds),
        "instruments": [],
        "max_bytes": plan.max_bytes,
        "max_frames": plan.max_frames,
        "max_network_calls": plan.max_network_calls,
        "max_segment_bytes": plan.max_segment_bytes,
        "max_segments": plan.max_segments,
        "progress_interval_seconds": format(plan.progress_interval_seconds, "g"),
        "rotation_seconds": format(plan.rotation_seconds, "g"),
        "venue": plan.venue.value,
    }
    verify_prediction_collection_plan_payload(payload, plan)
    with pytest.raises(ValueError, match="diverged from the frozen plan"):
        verify_prediction_collection_plan_payload(
            {**payload, "max_network_calls": plan.max_network_calls - 1},
            plan,
        )


def test_committed_future_campaign_pack_remains_bound_and_unlaunched() -> None:
    root = (
        ROOT
        / "ops/prediction_markets_candidate_v1"
        / "prediction-markets-v1-20260901t000000z-aa60c0ff"
    )
    raw = (root / "campaign-manifest.json").read_bytes()
    decoded = decode_canonical_json(raw[:-1], require_canonical=True)
    assert isinstance(decoded, dict)
    candidate = _candidate()
    starts = datetime.fromisoformat(str(decoded["starts_at_utc"]).replace("Z", "+00:00"))
    selection = build_prediction_split_plan(
        preregistration=candidate,
        dataset_sha256="0" * 64,
        prospective_start=starts,
    ).selection_view
    seal = build_prediction_replay_seal(
        campaign_manifest=decoded,
        preregistration=candidate,
        selection_view=selection,
    )
    assert seal.campaign_manifest_sha256 == decoded["manifest_sha256"]
    assert decoded["status"] == AWAITING_HUMAN_EXECUTION
    assert decoded["vps_or_h1_path"] == "NONE"
    assert (root / "campaign-manifest.sha256").read_text(encoding="utf-8").startswith(
        hashlib.sha256(raw).hexdigest()
    )


def test_public_source_unavailable_never_becomes_economic_evidence(tmp_path: Path) -> None:
    bundle = build_polymarket_fixture(tmp_path / "lake")
    report = bundle.replay().run(
        intent=bundle.intent(role=PredictionExecutionRole.MAKER, quantity="1"),
        settlement=bundle.settlement,
        report_time_utc_ns=bundle.settlement.received_time_utc_ns + 100,
        report_time_monotonic_ns=bundle.settlement.received_monotonic_ns + 100,
    )
    signal = datetime.fromtimestamp(report.signal_time_utc_ns / 1_000_000_000, tz=UTC)
    start = signal - timedelta(days=15)
    campaign = prepare_prediction_campaign(
        output_root=tmp_path / "campaign",
        campaign_id="fixture-public-source-unavailable",
        starts_at_utc=start.isoformat().replace("+00:00", "Z"),
        preregistration=bundle.preregistration,
        contracts=_contracts(),
    )
    result = evaluate_preregistered(
        preregistration=bundle.preregistration,
        selection_view=build_prediction_split_plan(
            preregistration=bundle.preregistration,
            dataset_sha256=bundle.dataset.dataset_sha256,
            prospective_start=start,
        ).selection_view,
        campaign_manifest=campaign,
        campaign_replay=build_synthetic_prediction_campaign_replay(
            (report,),
            preregistration=bundle.preregistration,
            semantic_catalog_sha256=SemanticCatalog.build(()).catalog_sha256,
            seal=build_prediction_replay_seal(
                campaign_manifest=campaign,
                preregistration=bundle.preregistration,
                selection_view=build_prediction_split_plan(
                    preregistration=bundle.preregistration,
                    dataset_sha256=bundle.dataset.dataset_sha256,
                    prospective_start=start,
                ).selection_view,
            ),
        ),
        source_status=PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE,
    )
    assert result["economic_evidence_status"] == "PUBLIC_SOURCE_UNAVAILABLE"
    assert result["go_no_go"] == "NO_GO_GHOST_ONLY_ECONOMIC_EVIDENCE_NOT_AVAILABLE"
    variants = cast(list[dict[str, object]], result["variants"])
    assert Decimal(cast(str, variants[0]["net_pnl"])) >= 0
