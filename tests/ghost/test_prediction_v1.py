from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from hyperlab.ghost.prediction import (
    _CAMPAIGN_RUNNER_TOKEN,
    PredictionCampaignGhostReplay,
    PredictionExecutionRole,
    PredictionGhostReport,
    PredictionOpportunity,
    PredictionOpportunitySignal,
    PredictionOpportunityStatus,
    PredictionSettlementState,
)
from hyperlab.research_data.envelope import Venue
from hyperlab.research_data.prediction_evidence import prediction_raw_record_ref
from tests.prediction_support import BASE_UTC_NS, BookSpec, build_polymarket_fixture


def _report_times(bundle: object) -> tuple[int, int]:
    settlement = bundle.settlement  # type: ignore[attr-defined]
    aggressor = bundle.aggressor  # type: ignore[attr-defined]
    utc = settlement.received_time_utc_ns
    monotonic = settlement.received_monotonic_ns
    if aggressor is not None:
        utc = max(utc, aggressor.received_time_utc_ns)
        monotonic = max(monotonic, aggressor.received_monotonic_ns)
    return utc + 100, monotonic + 100


def test_k4_multi_outcome_state_is_enumerated_once_per_variant() -> None:
    dataset_sha256 = "a" * 64
    point_ids = {"YES": "1" * 64, "NO": "2" * 64}
    signals = tuple(
        PredictionOpportunitySignal(
            venue=Venue.KALSHI,
            dataset_sha256=dataset_sha256,
            event_id="KXEVENT",
            market_id="KXMARKET",
            outcome_id=f"KXMARKET:{outcome}",
            point_in_time_id=point_ids[outcome],
            arrival_sequence=1,
            received_time_utc_ns=1_800_000_000_000_000_000,
            received_monotonic_ns=1_000,
            raw_content_sha256="b" * 64,
            raw_record_index=0,
            collector_identity="fixture-kalshi-collector",
            session_identity="fixture-kalshi-session",
        )
        for outcome in ("YES", "NO")
    )
    opportunities = {
        signal.point_in_time_id: PredictionOpportunity._create_for_runner(
            family_id="K4_COMPLETE_SET_LOGICAL_RV",
            variant_id="K4_V1_PRIMARY_500MS",
            variant_parameters_sha256="c" * 64,
            candidate_config_sha256="d" * 64,
            campaign_manifest_sha256=None,
            runner_policy_sha256="e" * 64,
            dataset_bundle_sha256=dataset_sha256,
            semantic_catalog_sha256="f" * 64,
            relation_ids=("fixture-complete-set",),
            signals=signals,
            trigger_point_in_time_id=signal.point_in_time_id,
            execution_sequence=(),
            quantity=Decimal("1"),
            status=PredictionOpportunityStatus.REFUSED,
            reasons=("SYNTHETIC_FIXTURE_K4_STATE",),
            _runner_token=_CAMPAIGN_RUNNER_TOKEN,
        )
        for signal in signals
    }
    rows = tuple(
        SimpleNamespace(
            point_in_time_id=signal.point_in_time_id,
            received_time_utc_ns=signal.received_time_utc_ns,
        )
        for signal in sorted(signals, key=lambda item: item.outcome_id)
    )
    variant = SimpleNamespace(
        family_id="K4_COMPLETE_SET_LOGICAL_RV",
        variant_id="K4_V1_PRIMARY_500MS",
    )
    runner = object.__new__(PredictionCampaignGhostReplay)
    runner._preregistration = SimpleNamespace(variants=(variant,))
    runner._require_replay_seal = lambda _seal: None
    runner._enumerate_k4 = lambda **kwargs: (
        opportunities[kwargs["trigger"].point_in_time_id],
    )
    seal = SimpleNamespace(
        selection_view=SimpleNamespace(
            train=SimpleNamespace(start=datetime(1970, 1, 1, tzinfo=UTC))
        ),
        evidence_cutoff_utc_ns_exclusive=1_800_000_000_000_001_000,
        embargo_ns=0,
    )

    runner._entries = tuple((object(), row) for row in rows)
    first = runner.enumerate_opportunities(seal=seal)
    repeated = runner.enumerate_opportunities(seal=seal)

    assert len(first) == 1
    assert tuple(item.to_dict() for item in repeated) == tuple(
        item.to_dict() for item in first
    )
    assert first[0].trigger_point_in_time_id == point_ids["NO"]

    divergent = dict(opportunities)
    divergent[point_ids["YES"]] = replace(
        divergent[point_ids["YES"]],
        reasons=("SYNTHETIC_FIXTURE_DIVERGED_DECISION",),
    )
    runner._enumerate_k4 = lambda **kwargs: (
        divergent[kwargs["trigger"].point_in_time_id],
    )
    with pytest.raises(ValueError, match="K4_CAUSAL_STATE_DECISION_DIVERGED"):
        runner.enumerate_opportunities(seal=seal)


def test_campaign_opportunities_follow_causal_entry_order_when_utc_regresses() -> None:
    dataset_sha256 = "a" * 64
    received_times = (1_000, 900)
    signals = tuple(
        PredictionOpportunitySignal(
            venue=Venue.KALSHI,
            dataset_sha256=dataset_sha256,
            event_id="KXEVENT",
            market_id=f"KXMARKET-{index}",
            outcome_id=f"KXMARKET-{index}:YES",
            point_in_time_id=str(index + 1) * 64,
            arrival_sequence=10 + index,
            received_time_utc_ns=received_time,
            received_monotonic_ns=10 + index,
            raw_content_sha256=chr(ord("b") + index) * 64,
            raw_record_index=0,
            collector_identity="fixture-kalshi-collector",
            session_identity="fixture-kalshi-session",
        )
        for index, received_time in enumerate(received_times)
    )
    opportunities = {
        signal.point_in_time_id: PredictionOpportunity._create_for_runner(
            family_id="K5_INCENTIVE_QUEUE",
            variant_id="K5_V1_ZERO_REWARD_CONTROL",
            variant_parameters_sha256="d" * 64,
            candidate_config_sha256="e" * 64,
            campaign_manifest_sha256=None,
            runner_policy_sha256="f" * 64,
            dataset_bundle_sha256=dataset_sha256,
            semantic_catalog_sha256="0" * 64,
            relation_ids=(),
            signals=(signal,),
            trigger_point_in_time_id=signal.point_in_time_id,
            execution_sequence=(),
            quantity=Decimal("1"),
            status=PredictionOpportunityStatus.REFUSED,
            reasons=("SYNTHETIC_FIXTURE_K5_REFUSAL",),
            _runner_token=_CAMPAIGN_RUNNER_TOKEN,
        )
        for signal in signals
    }
    rows = tuple(
        SimpleNamespace(
            point_in_time_id=signal.point_in_time_id,
            received_time_utc_ns=signal.received_time_utc_ns,
        )
        for signal in signals
    )
    runner = object.__new__(PredictionCampaignGhostReplay)
    runner._entries = tuple((object(), row) for row in rows)
    runner._preregistration = SimpleNamespace(
        variants=(
            SimpleNamespace(
                family_id="K5_INCENTIVE_QUEUE",
                variant_id="K5_V1_ZERO_REWARD_CONTROL",
            ),
        )
    )
    runner._require_replay_seal = lambda _seal: None
    runner._enumerate_k5 = lambda **kwargs: opportunities[
        kwargs["trigger"].point_in_time_id
    ]
    seal = SimpleNamespace(
        selection_view=SimpleNamespace(
            train=SimpleNamespace(start=datetime(1970, 1, 1, tzinfo=UTC))
        ),
        evidence_cutoff_utc_ns_exclusive=2_000,
        embargo_ns=0,
    )

    ordered = runner.enumerate_opportunities(seal=seal)

    assert tuple(item.trigger_point_in_time_id for item in ordered) == tuple(
        signal.point_in_time_id for signal in signals
    )


def test_k4_latest_rows_cannot_see_a_later_record_in_the_same_envelope() -> None:
    member_a = (Venue.POLYMARKET, "fixture-market", "fixture-a")
    member_b = (Venue.POLYMARKET, "fixture-market", "fixture-b")

    def row(
        member: tuple[Venue, str, str],
        *,
        arrival_sequence: int,
        raw_record_index: int,
        point_in_time_id: str,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            venue=member[0],
            market_id=member[1],
            outcome_id=member[2],
            point_in_time_id=point_in_time_id,
            collector_identity="fixture-collector",
            session_identity="fixture-session",
            received_time_utc_ns=1_000,
            received_monotonic_ns=1_000,
            arrival_sequence=arrival_sequence,
            raw_record_index=raw_record_index,
        )

    old_b = row(member_b, arrival_sequence=9, raw_record_index=0, point_in_time_id="1" * 64)
    trigger_a = row(
        member_a,
        arrival_sequence=10,
        raw_record_index=0,
        point_in_time_id="2" * 64,
    )
    future_b = row(
        member_b,
        arrival_sequence=10,
        raw_record_index=1,
        point_in_time_id="3" * 64,
    )
    runner = object.__new__(PredictionCampaignGhostReplay)
    engine = SimpleNamespace(_dataset=SimpleNamespace(rows=(old_b, trigger_a, future_b)))
    allowed = frozenset((old_b.point_in_time_id, trigger_a.point_in_time_id, future_b.point_in_time_id))

    at_first_record = runner._latest_rows_same_engine(
        engine=engine,
        members=(member_a, member_b),
        trigger=trigger_a,
        allowed_point_in_time_ids=allowed,
    )
    at_second_record = runner._latest_rows_same_engine(
        engine=engine,
        members=(member_a, member_b),
        trigger=future_b,
        allowed_point_in_time_ids=allowed,
    )

    assert at_first_record[member_b] is old_b
    assert at_second_record[member_b] is future_b


def test_taker_consumes_finite_depth_partially_and_reconciles_settlement(
    tmp_path: Path,
) -> None:
    bundle = build_polymarket_fixture(tmp_path)
    intent = bundle.intent(role=PredictionExecutionRole.TAKER, quantity="4")
    report_time_utc_ns, report_time_monotonic_ns = _report_times(bundle)
    report = bundle.replay().run(
        intent=intent,
        settlement=bundle.settlement,
        report_time_utc_ns=report_time_utc_ns,
        report_time_monotonic_ns=report_time_monotonic_ns,
    )
    assert report.status == "PARTIAL"
    assert report.filled_quantity == Decimal("2")
    assert report.missed_quantity == Decimal("2")
    assert report.entry_notional == Decimal("0.84")
    assert report.payout == Decimal("2")
    assert report.net_pnl == Decimal("1.16")
    assert report.reconciliation_difference == 0
    repeated = bundle.replay().run(
        intent=intent,
        settlement=bundle.settlement,
        report_time_utc_ns=report_time_utc_ns,
        report_time_monotonic_ns=report_time_monotonic_ns,
    )
    assert repeated.to_dict() == report.to_dict()


def test_maker_never_fills_without_explicit_side_aware_aggressor_flow(
    tmp_path: Path,
) -> None:
    bundle = build_polymarket_fixture(tmp_path, include_aggressor=True)
    intent = bundle.intent(role=PredictionExecutionRole.MAKER, quantity="2")
    report_time_utc_ns, report_time_monotonic_ns = _report_times(bundle)
    missed = bundle.replay().run(
        intent=intent,
        settlement=bundle.settlement,
        report_time_utc_ns=report_time_utc_ns,
        report_time_monotonic_ns=report_time_monotonic_ns,
    )
    assert missed.status == "MISSED"
    assert missed.reason == "MAKER_FILL_REQUIRES_EXPLICIT_AGGRESSOR_FLOW"
    assert bundle.aggressor is not None
    filled = bundle.replay().run(
        intent=intent,
        settlement=bundle.settlement,
        maker_aggressor=bundle.aggressor,
        report_time_utc_ns=report_time_utc_ns,
        report_time_monotonic_ns=report_time_monotonic_ns,
    )
    assert filled.status == "PARTIAL"
    assert filled.filled_quantity == Decimal("1")
    with pytest.raises(ValueError, match="side or block"):
        replace(bundle.aggressor, aggressor_side="BUY", block_trade=True)


def test_preregistration_and_dataset_bindings_are_mandatory(tmp_path: Path) -> None:
    bundle = build_polymarket_fixture(tmp_path)
    intent = bundle.intent()
    report_time_utc_ns, report_time_monotonic_ns = _report_times(bundle)
    with pytest.raises(ValueError, match="preregistration or dataset binding"):
        bundle.replay().run(
            intent=replace(intent, candidate_config_sha256="f" * 64),
            settlement=bundle.settlement,
            report_time_utc_ns=report_time_utc_ns,
            report_time_monotonic_ns=report_time_monotonic_ns,
        )
    with pytest.raises(ValueError, match="parameters diverged"):
        bundle.replay().run(
            intent=replace(intent, variant_parameters_sha256="f" * 64),
            settlement=bundle.settlement,
            report_time_utc_ns=report_time_utc_ns,
            report_time_monotonic_ns=report_time_monotonic_ns,
        )


def test_monotonic_admission_ignores_later_utc_regressed_book(tmp_path: Path) -> None:
    bundle = build_polymarket_fixture(
        tmp_path,
        books=(
            BookSpec(1_000, BASE_UTC_NS),
            BookSpec(
                600_000_000,
                BASE_UTC_NS - 100,
                asks=(("0.43", "10"),),
            ),
        ),
    )
    report_time_utc_ns, report_time_monotonic_ns = _report_times(bundle)
    report = bundle.replay().run(
        intent=bundle.intent(quantity="2"),
        settlement=bundle.settlement,
        report_time_utc_ns=report_time_utc_ns,
        report_time_monotonic_ns=report_time_monotonic_ns,
    )
    assert report.status == "FILLED"
    assert report.average_price == Decimal("0.42")


def test_latest_pre_admission_book_controls_fill_even_when_utc_regresses(
    tmp_path: Path,
) -> None:
    bundle = build_polymarket_fixture(
        tmp_path,
        books=(
            BookSpec(1_000, BASE_UTC_NS),
            BookSpec(
                400_000_000,
                BASE_UTC_NS - 100,
                asks=(("0.43", "10"),),
            ),
        ),
    )
    report_time_utc_ns, report_time_monotonic_ns = _report_times(bundle)
    report = bundle.replay().run(
        intent=bundle.intent(quantity="2"),
        settlement=bundle.settlement,
        report_time_utc_ns=report_time_utc_ns,
        report_time_monotonic_ns=report_time_monotonic_ns,
    )
    assert report.status == "MISSED"
    assert report.filled_quantity == 0


def test_settlement_must_resolve_to_the_authenticated_raw_record(tmp_path: Path) -> None:
    bundle = build_polymarket_fixture(tmp_path)
    forged = replace(
        bundle.settlement,
        raw_ref=prediction_raw_record_ref(bundle.envelopes[0], 0),
    )
    report_time_utc_ns, report_time_monotonic_ns = _report_times(bundle)
    with pytest.raises(ValueError, match="clocks or provenance diverged"):
        bundle.replay().run(
            intent=bundle.intent(quantity="1"),
            settlement=forged,
            report_time_utc_ns=report_time_utc_ns,
            report_time_monotonic_ns=report_time_monotonic_ns,
        )


def test_report_parser_recomputes_hash_and_rejects_tampering(tmp_path: Path) -> None:
    bundle = build_polymarket_fixture(tmp_path)
    report_time_utc_ns, report_time_monotonic_ns = _report_times(bundle)
    report = bundle.replay().run(
        intent=bundle.intent(quantity="1"),
        settlement=bundle.settlement,
        report_time_utc_ns=report_time_utc_ns,
        report_time_monotonic_ns=report_time_monotonic_ns,
    )
    assert PredictionGhostReport.from_dict(report.to_dict()) == report
    tampered = report.to_dict()
    attribution = tampered["attribution"]
    assert isinstance(attribution, dict)
    attribution["net_pnl"] = "999"
    with pytest.raises(ValueError, match="SHA-256 diverged"):
        PredictionGhostReport.from_dict(tampered)


def test_unresolved_resolution_locks_capital_and_never_invents_pnl(tmp_path: Path) -> None:
    bundle = build_polymarket_fixture(
        tmp_path,
        settlement_state=PredictionSettlementState.DISPUTED,
        settlement_payout=None,
    )
    unresolved = bundle.settlement
    report_time_utc_ns, report_time_monotonic_ns = _report_times(bundle)
    report = bundle.replay().run(
        intent=bundle.intent(quantity="1"),
        settlement=unresolved,
        report_time_utc_ns=report_time_utc_ns,
        report_time_monotonic_ns=report_time_monotonic_ns,
    )
    assert report.net_pnl is None
    assert report.payout is None
    assert report.unresolved_exposure == Decimal("1")
    assert report.capital_immobilized_notional_ns > 0


def test_unknown_causal_fee_fails_closed_at_admission(tmp_path: Path) -> None:
    bundle = build_polymarket_fixture(tmp_path, fee_after_books=True)
    report_time_utc_ns, report_time_monotonic_ns = _report_times(bundle)
    with pytest.raises(ValueError, match="BOOK_NOT_EXECUTION_ELIGIBLE"):
        bundle.replay().run(
            intent=bundle.intent(quantity="1"),
            settlement=bundle.settlement,
            report_time_utc_ns=report_time_utc_ns,
            report_time_monotonic_ns=report_time_monotonic_ns,
        )
