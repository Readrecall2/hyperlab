from __future__ import annotations

import json
from pathlib import Path

import pytest

from hyperlab.research_data.adapters import (
    KALSHI_METADATA_VERSION,
    POLYMARKET_METADATA_VERSION,
    KalshiPublicAdapter,
    PolymarketPublicAdapter,
    _iso_to_ns,
)
from hyperlab.research_data.envelope import Venue
from hyperlab.research_data.prediction_contracts import (
    BoundedCursorPager,
    EvidenceClassification,
    OfficialPublicContract,
    PredictionIdentityGraph,
)
from hyperlab.research_data.probe import _polymarket_token_parameter_plan

ROOT = Path(__file__).resolve().parents[2]


def test_official_prediction_contracts_are_versioned_documentary_and_unobserved() -> None:
    polymarket = OfficialPublicContract.from_path(ROOT / "config/research/polymarket-public-contract-v1.json")
    kalshi = OfficialPublicContract.from_path(ROOT / "config/research/kalshi-public-contract-v1.json")

    assert polymarket.venue is Venue.POLYMARKET
    assert kalshi.venue is Venue.KALSHI
    assert polymarket.accessibility is EvidenceClassification.UNKNOWN_NOT_OBSERVED
    assert kalshi.accessibility is EvidenceClassification.UNKNOWN_NOT_OBSERVED
    assert len(polymarket.contract_sha256) == len(kalshi.contract_sha256) == 64
    assert polymarket.endpoint("gamma-markets-keyset").url.endswith("/markets/keyset")
    assert kalshi.endpoint("historical-cutoff").url.endswith("/historical/cutoff")


def test_prediction_adapter_pagination_is_explicit_and_bounded() -> None:
    polymarket = PolymarketPublicAdapter()
    market_request = polymarket.market_census_request(limit=100, after_cursor="opaque==")
    event_request = polymarket.event_census_request(limit=500, after_cursor="event==")
    assert market_request.url.endswith("/markets/keyset")
    assert dict(market_request.query)["after_cursor"] == "opaque=="
    assert event_request.url.endswith("/events/keyset")

    kalshi = KalshiPublicAdapter()
    assert dict(kalshi.market_census_request(limit=1000, cursor="next==").query)["cursor"] == "next=="
    incentives = kalshi.requests_for_market(
        ticker="KXTEST",
        event_ticker="KXEVENT",
        series_ticker="KXSERIES",
        feeds=("incentives", "event_fee_changes", "trades"),
    )
    incentive = next(item for item in incentives if item.url.endswith("/incentive_programs"))
    trade = next(item for item in incentives if item.url.endswith("/markets/trades"))
    assert "market_ticker" not in dict(incentive.query)
    assert dict(trade.query)["is_block_trade"] == "false"
    assert any(item.url.endswith("/events/fee_changes") for item in incentives)
    historical = kalshi.historical_trade_request(
        "KXTEST",
        limit=1000,
        cursor="historical==",
    )
    assert historical.url.endswith("/historical/trades")
    assert dict(historical.query)["cursor"] == "historical=="


def test_polymarket_token_parameters_are_planned_feed_major() -> None:
    adapter = PolymarketPublicAdapter()
    plan = _polymarket_token_parameter_plan(
        adapter,
        (("token-yes", "condition"), ("token-no", "condition")),
        {"fees", "last_trade_price", "order_book", "tick_size"},
    )
    assert [item[2] for item in plan] == [
        "fees",
        "fees",
        "tick_size",
        "tick_size",
        "order_book",
        "order_book",
        "last_trade_price",
        "last_trade_price",
    ]
    assert [dict(item[3].query)["token_id"] for item in plan] == [
        "token-yes",
        "token-no",
    ] * 4


def test_adapter_rfc3339_timestamp_preserves_nanoseconds() -> None:
    assert _iso_to_ns("1970-01-01T00:00:00.123456789Z") == 123_456_789


def test_cursor_pager_rejects_cycles_and_preserves_losing_duplicates() -> None:
    pager = BoundedCursorPager(max_pages=2, max_items=3)
    assert pager.admit(
        requested_cursor=None,
        next_cursor="cursor-2",
        item_ids=("a", "b"),
    ) == ("a", "b")
    assert pager.admit(
        requested_cursor="cursor-2",
        next_cursor=None,
        item_ids=("b", "c"),
    ) == ("c",)
    with pytest.raises(BufferError, match="MAX_PAGES"):
        pager.admit(requested_cursor=None, next_cursor=None, item_ids=("d",))

    cyclic = BoundedCursorPager(max_pages=2, max_items=10)
    cyclic.admit(requested_cursor=None, next_cursor="same", item_ids=("a",))
    with pytest.raises(ValueError, match="CURSOR_CYCLE"):
        cyclic.admit(requested_cursor="same", next_cursor="same", item_ids=("b",))


def test_polymarket_identity_graph_authenticates_outcome_token_indices() -> None:
    market = {
        "acceptingOrders": True,
        "archived": False,
        "clobTokenIds": json.dumps(["token-yes", "token-no"]),
        "closed": False,
        "conditionId": "condition-1",
        "enableNegRisk": False,
        "enableOrderBook": True,
        "endDate": "2026-12-31T00:00:00Z",
        "events": [{"id": "event-1"}],
        "id": "gamma-market-1",
        "negRisk": False,
        "outcomes": json.dumps(["YES", "NO"]),
        "questionID": "question-1",
        "resolutionSource": "Official fixture resolver.",
        "restricted": False,
        "rules": "The official fixture rule.",
        "startDate": "2026-01-01T00:00:00Z",
    }
    event = {"id": "event-1", "markets": [{"id": "gamma-market-1"}]}
    graph = PredictionIdentityGraph.from_polymarket(
        market=market,
        event=event,
        clob_markets=(
            {
                "condition_id": "condition-1",
                "tokens": [
                    {"token_id": "token-yes"},
                    {"token_id": "token-no"},
                ],
            },
        ),
        source_metadata_version=POLYMARKET_METADATA_VERSION,
    )
    assert [item.outcome_id for item in graph.outcomes] == ["token-yes", "token-no"]

    broken = {**market, "clobTokenIds": json.dumps(["token-yes"])}
    with pytest.raises(ValueError, match="incomplete"):
        PredictionIdentityGraph.from_polymarket(
            market=broken,
            event=event,
            clob_markets=(
                {
                    "condition_id": "condition-1",
                    "tokens": [
                        {"token_id": "token-yes"},
                        {"token_id": "token-no"},
                    ],
                },
            ),
            source_metadata_version=POLYMARKET_METADATA_VERSION,
        )


def test_kalshi_identity_graph_refuses_mve_and_silent_rule_change() -> None:
    market = {
        "close_time": "2026-12-31T00:00:00Z",
        "event_ticker": "KXEVENT",
        "open_time": "2026-01-01T00:00:00Z",
        "rules_primary": "Official primary rule.",
        "status": "active",
        "ticker": "KXEVENT-YES",
    }
    event = {
        "event_ticker": "KXEVENT",
        "rules_primary": "Official primary rule.",
        "series_ticker": "KXSERIES",
    }
    metadata = {
        "event_ticker": "KXEVENT",
        "settlement_sources": [{"name": "Official source"}],
    }
    series = {"ticker": "KXSERIES"}
    first = PredictionIdentityGraph.from_kalshi(
        market=market,
        event=event,
        event_metadata=metadata,
        series=series,
        source_metadata_version=KALSHI_METADATA_VERSION,
    )
    changed = PredictionIdentityGraph.from_kalshi(
        market={**market, "rules_primary": "Changed official rule."},
        event=event,
        event_metadata=metadata,
        series=series,
        source_metadata_version=KALSHI_METADATA_VERSION,
    )
    with pytest.raises(ValueError, match="WITHOUT_TRANSITION"):
        first.assert_compatible_successor(changed, explicit_rule_version_transition=False)
    first.assert_compatible_successor(changed, explicit_rule_version_transition=True)

    with pytest.raises(ValueError, match="multivariate"):
        PredictionIdentityGraph.from_kalshi(
            market=market,
            event={**event, "is_multivariate": True},
            event_metadata=metadata,
            series=series,
            source_metadata_version=KALSHI_METADATA_VERSION,
        )
