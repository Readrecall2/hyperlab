from __future__ import annotations

from decimal import Decimal

import pytest

from hyperlab.research_data.datasets import (
    MARKOUT_HORIZONS_MS,
    ActionDelayBand,
    CausalEventLabel,
    EventLabelType,
    H1Action,
    H1DatasetRow,
    MarkoutObservation,
    MatchedControlKey,
    ResearchState,
    build_event_window,
    build_h1_row,
)
from hyperlab.research_data.envelope import Venue
from hyperlab.research_data.prediction import (
    IncentiveLedgerEntry,
    K4Scanner,
    K4Status,
    MarketRuleVersion,
    OutcomeIdentity,
    PredictionBookSnapshot,
    RelationStatus,
    RelationType,
    SemanticCatalog,
    SemanticRelation,
)


def _members() -> tuple[OutcomeIdentity, OutcomeIdentity]:
    return (
        OutcomeIdentity(Venue.POLYMARKET, "pm-market", "pm-yes", "Yes"),
        OutcomeIdentity(Venue.KALSHI, "kalshi-market", "kalshi-no", "No"),
    )


def _relation(status: RelationStatus = RelationStatus.VERIFIED) -> SemanticRelation:
    members = _members()
    return SemanticRelation.create(
        relation_type=RelationType.PARITY,
        members=members,
        formal_rule={
            "guaranteed_payout": "1",
            "legs": [
                {"market_id": "pm-market", "outcome_id": "pm-yes", "side": "YES"},
                {
                    "market_id": "kalshi-market",
                    "outcome_id": "kalshi-no",
                    "side": "NO",
                },
            ],
            "resolution_rule_versions": {
                "kalshi-market:kalshi-no": "d" * 64,
                "pm-market:pm-yes": "c" * 64,
            },
            "resolution_unambiguous": True,
            "scanner_contract": "BUY_COMPLETE_SET_V1",
        },
        provenance=("human-reviewed-fixture",),
        version=1,
        confidence=Decimal("0.95"),
        status=status,
        human_justification="Synthetic fixture relation reviewed for deterministic tests.",
        machine_justification={"fixture_label": "SYNTHETIC/FIXTURE", "reviewed": True},
    )


def _snapshots(*, point_ids: tuple[str, str] = ("pit-1", "pit-1")) -> tuple[PredictionBookSnapshot, ...]:
    return (
        PredictionBookSnapshot(
            venue=Venue.POLYMARKET,
            economic_market_id="pm-market",
            outcome_id="pm-yes",
            point_in_time_id=point_ids[0],
            observed_at_ns=100,
            yes_bid=Decimal("0.39"),
            yes_bid_size=Decimal("8"),
            yes_ask=Decimal("0.40"),
            yes_ask_size=Decimal("5"),
            no_bid=Decimal("0.59"),
            no_bid_size=Decimal("4"),
            no_ask=Decimal("0.61"),
            no_ask_size=Decimal("4"),
            conservative_fee_per_contract=Decimal("0.01"),
            rule_version_id="c" * 64,
            closes_at_ns=10_000,
            raw_segment_sha256="a" * 64,
        ),
        PredictionBookSnapshot(
            venue=Venue.KALSHI,
            economic_market_id="kalshi-market",
            outcome_id="kalshi-no",
            point_in_time_id=point_ids[1],
            observed_at_ns=100,
            yes_bid=Decimal("0.49"),
            yes_bid_size=Decimal("4"),
            yes_ask=Decimal("0.51"),
            yes_ask_size=Decimal("4"),
            no_bid=Decimal("0.49"),
            no_bid_size=Decimal("6"),
            no_ask=Decimal("0.50"),
            no_ask_size=Decimal("3"),
            conservative_fee_per_contract=Decimal("0.01"),
            rule_version_id="d" * 64,
            closes_at_ns=9_000,
            raw_segment_sha256="b" * 64,
        ),
    )


def test_market_rule_version_and_semantic_catalog_are_deterministic() -> None:
    members = _members()
    first = MarketRuleVersion.create(
        venue=Venue.POLYMARKET,
        economic_market_id="pm-market",
        rule_text="Rule version one",
        resolution_source="https://official.example/fixture",
        opens_at="2026-08-26T00:00:00Z",
        closes_at="2026-08-27T00:00:00Z",
        resolves_at=None,
        market_status="OPEN",
        outcomes=(members[0],),
        source_metadata_version="fixture-v1",
        raw_content_sha256="c" * 64,
    )
    again = MarketRuleVersion.create(
        venue=Venue.POLYMARKET,
        economic_market_id="pm-market",
        rule_text="Rule version one",
        resolution_source="https://official.example/fixture",
        opens_at="2026-08-26T00:00:00Z",
        closes_at="2026-08-27T00:00:00Z",
        resolves_at=None,
        market_status="OPEN",
        outcomes=(members[0],),
        source_metadata_version="fixture-v1",
        raw_content_sha256="c" * 64,
    )
    changed = MarketRuleVersion.create(
        venue=Venue.POLYMARKET,
        economic_market_id="pm-market",
        rule_text="Rule version two",
        resolution_source="https://official.example/fixture",
        opens_at="2026-08-26T00:00:00Z",
        closes_at="2026-08-27T00:00:00Z",
        resolves_at=None,
        market_status="OPEN",
        outcomes=(members[0],),
        source_metadata_version="fixture-v1",
        raw_content_sha256="d" * 64,
    )
    assert first.version_id == again.version_id
    assert changed.version_id != first.version_id
    assert SemanticCatalog.build((_relation(),)).catalog_sha256 == SemanticCatalog.build(
        (_relation(),)
    ).catalog_sha256


def test_unverified_relation_is_refused_even_when_text_looks_similar() -> None:
    result = K4Scanner(conservative_slippage_bps=Decimal("0")).scan(
        _relation(RelationStatus.UNVERIFIED), _snapshots(), observed_at_ns=100
    )
    assert result.status is K4Status.REFUSED_UNVERIFIED
    assert result.reasons == ("RELATION_UNVERIFIED_NOT_PROMOTABLE",)
    assert result.legs == ()


def test_k4_uses_real_asks_worst_leg_sequencing_fees_and_zero_reward() -> None:
    result = K4Scanner(conservative_slippage_bps=Decimal("10")).scan(
        _relation(), _snapshots(), observed_at_ns=100
    )

    assert result.status is K4Status.CANDIDATE
    assert result.executable_quantity == Decimal("3")
    assert result.observed_cost == Decimal("2.70")
    assert result.fees == Decimal("0.06")
    assert result.conservative_slippage == Decimal("0.0027")
    assert result.guaranteed_payout == Decimal("3")
    assert result.conservative_net_edge == Decimal("0.2373")
    assert result.leg_sequencing[0] == "kalshi-market:kalshi-no:BUY_NO"
    assert result.non_fill_risk == "SEQUENTIAL_LEG_RISK_NOT_ASSUMED_SIMULTANEOUS"
    assert result.rewards_in_primary_economics == 0
    assert [leg.observed_ask for leg in result.legs] == [Decimal("0.40"), Decimal("0.50")]


def test_k4_returns_no_opportunity_for_non_point_in_time_books() -> None:
    result = K4Scanner(conservative_slippage_bps=Decimal("0")).scan(
        _relation(), _snapshots(point_ids=("pit-1", "pit-2")), observed_at_ns=100
    )
    assert result.status is K4Status.NO_OPPORTUNITY
    assert result.reasons == ("SNAPSHOTS_NOT_POINT_IN_TIME",)


def test_kalshi_incentive_ledger_never_makes_primary_edge_positive() -> None:
    ledger = IncentiveLedgerEntry(
        venue=Venue.KALSHI,
        program_id="fixture-program",
        market_id="kalshi-market",
        period_start="2026-08-26T00:00:00Z",
        period_end="2026-08-27T00:00:00Z",
        target_size=Decimal("100"),
        discount_factor=Decimal("0.5"),
        program_version="fixture-v1",
        hypothetical_reward=Decimal("25"),
    )
    assert ledger.realizable_reward is None
    assert ledger.primary_economics_reward == 0


def test_h1_markouts_and_h3_h4_event_windows_are_causal() -> None:
    assert MARKOUT_HORIZONS_MS == (100, 500, 1_000, 5_000, 30_000, 120_000)
    decision = 1_000_000_000
    markouts = tuple(
        MarkoutObservation(
            horizon_ms=horizon,
            observed_at_ns=decision + horizon * 1_000_000,
            markout=Decimal("0"),
        )
        for horizon in MARKOUT_HORIZONS_MS
    )
    row = H1DatasetRow(
        observation_id="fixture-observation",
        instrument_id="HL:BTC:perp",
        decision_time_ns=decision,
        action=H1Action.NO_QUOTE,
        state=ResearchState.NO_TRADE,
        action_delay_band=ActionDelayBand(100, 500),
        markouts=markouts,
        fill_to_close_markout=None,
        no_trade_reason="SYNTHETIC_FIXTURE_NO_SIGNAL",
    )
    assert row.action is H1Action.NO_QUOTE

    event = CausalEventLabel(
        label_type=EventLabelType.FORCED_FLOW,
        source_event_id="fixture-event",
        source_event_time_ns=2_000_000_000,
        observed_at_ns=2_000_000_001,
        source_metadata_version="fixture-v1",
        official_public_source=True,
        verified_causal=True,
    )
    key = MatchedControlKey("HL:BTC:perp", "12:00", "v1", "s1", "l1")
    window = build_event_window(
        event=event,
        instrument_id="HL:BTC:perp",
        pre_event_ms=500,
        post_event_ms=5_000,
        matched_control_key=key,
        action_delay_band=ActionDelayBand(100, 500),
    )
    assert window.window_start_ns < event.source_event_time_ns < window.window_end_ns

    built = build_h1_row(
        observation_id="fixture-built-row",
        instrument_id="HL:BTC:perp",
        decision_time_ns=decision,
        action=H1Action.NO_QUOTE,
        state=ResearchState.NO_TRADE,
        action_delay_band=ActionDelayBand(5, 25),
        markout_observations={
            item.horizon_ms: (item.observed_at_ns, item.markout) for item in markouts
        },
        fill_to_close_markout=None,
        fill_to_close_observed_at_ns=None,
        no_trade_reason="SYNTHETIC/FIXTURE no-trade state",
    )
    assert built.action is H1Action.NO_QUOTE
    with pytest.raises(ValueError, match="verified official public"):
        CausalEventLabel(
            label_type=EventLabelType.LIQUIDATION,
            source_event_id="price-crash-is-not-a-label",
            source_event_time_ns=1,
            observed_at_ns=2,
            source_metadata_version="fixture-v1",
            official_public_source=False,
            verified_causal=False,
        )
