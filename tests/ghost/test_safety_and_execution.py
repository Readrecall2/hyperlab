from __future__ import annotations

from pathlib import Path

from hyperlab.ghost.replay import GhostFixture, GhostReplay
from hyperlab.research_data.canonical import canonical_json_bytes, decode_canonical_json

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "tests" / "fixtures" / "ghost" / "base_realism_v1.json"


def _scenario(events: list[dict[str, object]], *, stale_after_ns: int = 1_000) -> GhostFixture:
    value = decode_canonical_json(BASE.read_bytes().rstrip(b"\r\n"))
    assert isinstance(value, dict)
    value["scenario_id"] = "safety-scenario"
    value["events"] = events
    model = value["model"]
    assert isinstance(model, dict)
    model["stale_after_ns"] = stale_after_ns
    return GhostFixture.from_bytes(canonical_json_bytes(value))


def _book(at: int = 100) -> dict[str, object]:
    return {
        "asks": [["101", "5"]],
        "bids": [["99", "5"]],
        "clock_uncertainty_ns": 1,
        "event_id": f"book-{at}",
        "instrument_id": "HL:BTC:perp",
        "kind": "BOOK",
        "receive_ns": at,
        "resync_complete": True,
        "source_ns": at - 10,
        "venue": "hyperliquid",
    }


def _order(order_id: str, decision: int, tif: str, side: str, price: str) -> dict[str, object]:
    return {
        "cancel_request_ns": None,
        "decision_ns": decision,
        "depends_on_order_id": None,
        "group_id": None,
        "instrument_id": "HL:BTC:perp",
        "kind": "ORDER",
        "leg_index": 0,
        "limit_price": price,
        "order_id": order_id,
        "quantity": "2",
        "role": "PRIMARY",
        "side": side,
        "time_in_force": tif,
        "venue": "hyperliquid",
    }


def test_gap_stale_and_reconnect_block_every_new_hypothetical_action() -> None:
    gap = {
        "event_id": "gap-1",
        "instrument_id": "HL:BTC:perp",
        "kind": "GAP",
        "receive_ns": 105,
        "reason": "SOURCE_SEQUENCE_GAP",
        "venue": "hyperliquid",
    }
    gap_report = GhostReplay(
        _scenario([_book(), gap, _order("gap-order", 110, "IOC", "BUY", "101")])
    ).run()
    assert gap_report.orders[0].status == "NO_TRADE"
    assert gap_report.orders[0].reason == "VENUE_HEALTH_GAP"

    stale_report = GhostReplay(
        _scenario([_book(), _order("stale-order", 200, "IOC", "BUY", "101")], stale_after_ns=50)
    ).run()
    assert stale_report.orders[0].reason == "VENUE_HEALTH_STALE"

    reconnect = {
        "event_id": "reconnect-1",
        "instrument_id": "HL:BTC:perp",
        "kind": "RECONNECT",
        "receive_ns": 105,
        "reason": "TRANSPORT_RECONNECTED_AWAITING_RESYNC",
        "venue": "hyperliquid",
    }
    reconnect_report = GhostReplay(
        _scenario([_book(), reconnect, _order("reconnect-order", 110, "IOC", "BUY", "101")])
    ).run()
    assert reconnect_report.orders[0].reason == "VENUE_HEALTH_RECONNECT"
    assert reconnect_report.no_trade_reasons == ("VENUE_HEALTH_RECONNECT",)


def test_post_only_never_fills_on_contact_and_queue_sensitivity_is_explicit() -> None:
    maker = _order("maker", 110, "POST_ONLY", "BUY", "99")
    maker["cancel_request_ns"] = 128
    contact_trade = {
        "aggressor_side": "SELL",
        "event_id": "trade-contact",
        "instrument_id": "HL:BTC:perp",
        "kind": "TRADE",
        "price": "99",
        "quantity": "4",
        "receive_ns": 130,
        "source_ns": 125,
        "venue": "hyperliquid",
    }
    report = GhostReplay(_scenario([_book(), maker, contact_trade])).run()
    order = report.orders[0]
    assert order.status == "MISSED"
    assert order.filled_quantity == 0
    assert order.queue_sensitivity == {
        "CONSERVATIVE": "0",
        "PESSIMISTIC": "0",
        "SENSITIVITY": "1.5",
    }
    assert order.cancel_fill_race_observed is True


def test_post_only_cross_is_rejected_and_ioc_has_no_infinite_depth() -> None:
    rejected = GhostReplay(
        _scenario([_book(), _order("cross", 110, "ALO", "BUY", "101")])
    ).run()
    assert rejected.orders[0].status == "REJECTED"
    assert rejected.orders[0].reason == "POST_ONLY_WOULD_TAKE"

    partial_order = _order("partial", 110, "IOC", "BUY", "101")
    partial_order["quantity"] = "8"
    partial = GhostReplay(_scenario([_book(), partial_order])).run().orders[0]
    assert partial.status == "PARTIAL"
    assert partial.filled_quantity == 5
    assert partial.unfilled_quantity == 3
    assert partial.reason == "FINITE_DEPTH_EXHAUSTED"
