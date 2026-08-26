from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from hyperlab.ghost.replay import GhostFixture, GhostReplay
from hyperlab.research_data.canonical import canonical_json_bytes, decode_canonical_json

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "tests" / "fixtures" / "ghost" / "base_realism_v1.json"


def _body() -> dict[str, object]:
    value = decode_canonical_json(BASE.read_bytes().rstrip(b"\r\n"))
    assert isinstance(value, dict)
    return value


def _scenario(events: list[dict[str, object]]) -> GhostFixture:
    value = _body()
    value["scenario_id"] = "risk-and-cost-scenario"
    value["events"] = events
    return GhostFixture.from_bytes(canonical_json_bytes(value))


def _book(
    at: int,
    *,
    bid: str = "99",
    ask: str = "101",
    quantity: str = "5",
) -> dict[str, object]:
    return {
        "asks": [[ask, quantity]],
        "bids": [[bid, quantity]],
        "clock_uncertainty_ns": 1,
        "event_id": f"book-{at}",
        "instrument_id": "HL:BTC:perp",
        "kind": "BOOK",
        "receive_ns": at,
        "resync_complete": True,
        "source_ns": at - 10,
        "venue": "hyperliquid",
    }


def _order(
    order_id: str,
    decision_ns: int,
    side: str,
    quantity: str,
    limit_price: str,
    *,
    group_id: str | None = None,
    leg_index: int = 0,
    role: str = "PRIMARY",
    dependency: str | None = None,
) -> dict[str, object]:
    return {
        "cancel_request_ns": None,
        "decision_ns": decision_ns,
        "depends_on_order_id": dependency,
        "group_id": group_id,
        "instrument_id": "HL:BTC:perp",
        "kind": "ORDER",
        "leg_index": leg_index,
        "limit_price": limit_price,
        "order_id": order_id,
        "quantity": quantity,
        "role": role,
        "side": side,
        "time_in_force": "IOC",
        "venue": "hyperliquid",
    }


def test_repeated_ioc_never_reuses_depth_and_closeout_occurs_after_last_ack() -> None:
    report = GhostReplay(
        _scenario(
            [
                _book(100),
                _order("buy-1", 110, "BUY", "4", "101"),
                _order("buy-2", 120, "BUY", "4", "101"),
            ]
        )
    ).run()

    assert report.orders[0].filled_quantity == Decimal("4")
    assert report.orders[1].filled_quantity == Decimal("1")
    assert report.orders[1].unfilled_quantity == Decimal("3")
    assert report.orders[1].reason == "FINITE_DEPTH_EXHAUSTED"
    assert report.pnl.forced_close == Decimal("495")
    assert report.exposure.positions == {}
    assert report.exposure.unresolved_closeout == {}
    assert report.exposure.reconciliation_difference == 0
    assert report.pnl.reconciliation_difference == 0


def test_funding_uses_position_open_at_the_point_in_time_not_final_position() -> None:
    funding = {
        "event_id": "funding-1",
        "instrument_id": "HL:BTC:perp",
        "kind": "FUNDING",
        "rate_bps": "10",
        "receive_ns": 120,
        "reference_price": "100",
        "venue": "hyperliquid",
    }
    report = GhostReplay(
        _scenario(
            [
                _book(100),
                _order("buy", 110, "BUY", "1", "101"),
                funding,
                _book(200, bid="103", ask="104"),
                _order("hedge", 210, "SELL", "1", "103", role="HEDGE"),
            ]
        )
    ).run()

    assert report.exposure.positions == {}
    assert report.pnl.funding == Decimal("-0.1")
    assert report.pnl.inventory == Decimal("-101")
    assert report.pnl.hedge == Decimal("103")
    assert report.pnl.opportunity_cost < 0
    assert report.pnl.reconciliation_difference == 0


def test_outage_blocks_closeout_and_reports_exact_residual_inventory() -> None:
    outage = {
        "event_id": "outage-1",
        "instrument_id": "HL:BTC:perp",
        "kind": "OUTAGE",
        "reason": "PUBLIC_TRANSPORT_UNAVAILABLE",
        "receive_ns": 120,
        "venue": "hyperliquid",
    }
    report = GhostReplay(
        _scenario([_book(100), _order("buy", 110, "BUY", "2", "101"), outage])
    ).run()

    assert report.orders[0].status == "FILLED"
    assert report.pnl.forced_close == 0
    assert report.exposure.positions == {"hyperliquid|HL:BTC:perp": Decimal("2")}
    assert report.exposure.unresolved_closeout == report.exposure.positions
    assert report.exposure.reconciliation_difference == 0


def test_multi_leg_timeout_is_fill_aware_and_remains_hedge_pending_until_closeout() -> None:
    gap = {
        "event_id": "gap-1",
        "instrument_id": "HL:BTC:perp",
        "kind": "GAP",
        "reason": "SOURCE_SEQUENCE_GAP",
        "receive_ns": 130,
        "venue": "hyperliquid",
    }
    report = GhostReplay(
        _scenario(
            [
                _book(100),
                _order("leg-1", 110, "BUY", "2", "101", group_id="g", leg_index=0),
                gap,
                _order(
                    "leg-2",
                    250,
                    "SELL",
                    "8",
                    "99",
                    group_id="g",
                    leg_index=1,
                    role="HEDGE",
                    dependency="leg-1",
                ),
            ]
        )
    ).run()

    assert report.orders[1].depends_on_filled_quantity == Decimal("2")
    assert report.orders[1].status == "NO_TRADE"
    assert report.orders[1].reason == "VENUE_HEALTH_GAP"
    assert report.groups[0].status == "TIMED_OUT"
    assert report.groups[0].worst_leg_fill_ratio == 0
    assert report.exposure.unresolved_closeout == {
        "hyperliquid|HL:BTC:perp": Decimal("2")
    }


def test_discontinuous_book_move_is_not_smoothed_or_midpoint_filled() -> None:
    report = GhostReplay(
        _scenario(
            [
                _book(100),
                _order("buy", 110, "BUY", "2", "101"),
                _book(200, bid="80", ask="81"),
                _order("sell", 210, "SELL", "2", "80", role="HEDGE"),
            ]
        )
    ).run()

    assert [item.average_price for item in report.orders] == [
        Decimal("101"),
        Decimal("80"),
    ]
    assert report.pnl.inventory == Decimal("-202")
    assert report.pnl.hedge == Decimal("160")
    assert report.pnl.net < Decimal("-42")
    assert report.pnl.reconciliation_difference == 0


def test_primary_models_refuse_unmarked_synthetic_non_pessimistic_queue_or_optional_closeout() -> None:
    unmarked = _body()
    unmarked["fixture_label"] = "fixture"
    with pytest.raises(ValueError, match="SYNTHETIC/FIXTURE"):
        GhostFixture.from_bytes(canonical_json_bytes(unmarked))

    queue_body = _body()
    model = queue_body["model"]
    assert isinstance(model, dict)
    queue = model["queue"]
    assert isinstance(queue, dict)
    queue["primary"] = "CONSERVATIVE"
    with pytest.raises(ValueError, match="PRIMARY_QUEUE_SCENARIO"):
        GhostReplay(GhostFixture.from_bytes(canonical_json_bytes(queue_body)))

    closeout_body = _body()
    closeout_model = closeout_body["model"]
    assert isinstance(closeout_model, dict)
    closeout = closeout_model["closeout"]
    assert isinstance(closeout, dict)
    closeout["required"] = False
    with pytest.raises(ValueError, match="PESSIMISTIC_CLOSEOUT"):
        GhostReplay(GhostFixture.from_bytes(canonical_json_bytes(closeout_body)))


def test_report_exposes_every_latency_stage_and_model_identity() -> None:
    report = GhostReplay(GhostFixture.from_bytes(BASE.read_bytes())).run()
    timeline = report.orders[0].timeline
    assert timeline == {
        "ack_ns": 117,
        "admission_ns": 115,
        "cancel_ack_ns": None,
        "cancel_request_ns": None,
        "decision_complete_ns": 111,
        "decision_ns": 110,
        "transit_ns": 113,
    }
    assert report.latency_model_id == "fixture-latency-v1"
    assert report.queue_model_id == "fixture-queue-v1"
    assert report.grid_version_ids == ("hl-grid-v1",)
    assert report.cost_schedule_ids == ("fixture-fees-v1",)
    assert report.mechanism_version_ids == ("fixture-mechanism-v1",)
    assert report.closeout_model_id == "finite-depth-closeout-v1"


def test_dependent_leg_cannot_look_ahead_to_a_future_fill() -> None:
    report = GhostReplay(
        _scenario(
            [
                _book(100),
                _order(
                    "leg-1",
                    110,
                    "BUY",
                    "2",
                    "101",
                    group_id="future-fill",
                    leg_index=0,
                ),
                _order(
                    "leg-2",
                    112,
                    "SELL",
                    "2",
                    "99",
                    group_id="future-fill",
                    leg_index=1,
                    role="HEDGE",
                    dependency="leg-1",
                ),
            ]
        )
    ).run()

    assert report.orders[0].fill_timestamp_ns == 115
    assert report.orders[1].status == "NO_TRADE"
    assert report.orders[1].reason == "DEPENDENCY_FILL_NOT_KNOWN_AT_DECISION"
    assert report.orders[1].fill_timestamp_ns is None


def test_maker_flow_at_gap_is_never_counted_as_a_fill() -> None:
    maker = _order("maker", 110, "BUY", "2", "99")
    maker["time_in_force"] = "POST_ONLY"
    gap = {
        "event_id": "gap-maker",
        "instrument_id": "HL:BTC:perp",
        "kind": "GAP",
        "reason": "SOURCE_SEQUENCE_GAP",
        "receive_ns": 120,
        "venue": "hyperliquid",
    }
    trade = {
        "aggressor_side": "SELL",
        "event_id": "post-gap-trade",
        "instrument_id": "HL:BTC:perp",
        "kind": "TRADE",
        "price": "99",
        "quantity": "20",
        "receive_ns": 120,
        "source_ns": 115,
        "venue": "hyperliquid",
    }
    report = GhostReplay(_scenario([_book(100), maker, gap, trade])).run()

    assert report.orders[0].status == "MISSED"
    assert report.orders[0].filled_quantity == 0
    assert report.orders[0].fill_timestamp_ns is None
    assert report.orders[0].queue_sensitivity == {
        "CONSERVATIVE": "0",
        "PESSIMISTIC": "0",
        "SENSITIVITY": "0",
    }
