from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from hyperlab.api.public import PublicBootstrap
from hyperlab.collector.bootstrap import parse_bootstrap
from hyperlab.collector.models import WireEnvelope
from hyperlab.collector.parser import parse_websocket_message
from hyperlab.data.schema import RecordType

FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"
RECEIVED_TIME = datetime.fromtimestamp(1_786_490_460_000 / 1_000, tz=UTC)


def _raw(name: str) -> str:
    # A fixture file's final line ending is repository formatting, not WebSocket payload data.
    return (FIXTURES / name).read_text(encoding="utf-8").rstrip("\r\n")


def _envelope(
    name: str,
    *,
    arrival_sequence: int = 7,
    raw_message: str | None = None,
) -> WireEnvelope:
    return WireEnvelope(
        raw_message=_raw(name) if raw_message is None else raw_message,
        received_time=RECEIVED_TIME,
        connection_id="fixture-connection",
        connection_epoch=3,
        arrival_sequence=arrival_sequence,
    )


def _record_type(record: Any) -> str:
    value = record.record_type
    return value.value if hasattr(value, "value") else value


def _records(parsed: Any, record_type: str) -> list[Any]:
    return [record for record in parsed.records if _record_type(record) == record_type]


def _single(parsed: Any, record_type: str) -> Any:
    records = _records(parsed, record_type)
    assert len(records) == 1
    return records[0]


def _assert_wire_record(name: str, parsed: Any, expected_channel: str | None) -> None:
    raw_message = _raw(name)
    wire = _single(parsed, "wire_message")

    assert parsed.channel == expected_channel
    assert wire.asset is not None
    assert wire.row["source_sequence"] is None
    assert wire.row["connection_id"] == "fixture-connection"
    assert wire.row["connection_epoch"] == 3
    assert wire.row["arrival_sequence"] == 7
    assert wire.row["channel"] == expected_channel
    assert wire.row["raw_message"] == raw_message
    assert wire.row["is_json"] is (expected_channel is not None)
    assert wire.row["payload_sha256"] == hashlib.sha256(raw_message.encode()).hexdigest()


@pytest.mark.parametrize(
    ("name", "channel"),
    [
        ("ws_open.txt", None),
        ("ws_subscription_response.json", "subscriptionResponse"),
        ("ws_pong.json", "pong"),
    ],
)
def test_connection_and_control_messages_emit_only_the_wire_record(
    name: str,
    channel: str | None,
) -> None:
    parsed = parse_websocket_message(_envelope(name))

    _assert_wire_record(name, parsed, channel)
    assert [_record_type(record) for record in parsed.records] == ["wire_message"]
    assert not parsed.issues


def test_l2_book_is_an_explicit_snapshot_without_invented_sequence() -> None:
    parsed = parse_websocket_message(_envelope("ws_l2_book.json"))

    _assert_wire_record("ws_l2_book.json", parsed, "l2Book")
    snapshots = _records(parsed, "l2_snapshot")
    assert len(snapshots) == 4
    assert not _records(parsed, "l2_delta")
    assert not parsed.issues
    assert {record.asset for record in snapshots} == {"BTC"}
    assert {(record.row["side"], record.row["level"]) for record in snapshots} == {
        ("bid", 0),
        ("bid", 1),
        ("ask", 0),
        ("ask", 1),
    }
    assert all(record.row["source_sequence"] is None for record in snapshots)
    assert all(record.row["last_sequence"] is None for record in snapshots)

    best_bid = next(
        record for record in snapshots if record.row["side"] == "bid" and record.row["level"] == 0
    )
    best_ask = next(
        record for record in snapshots if record.row["side"] == "ask" and record.row["level"] == 0
    )
    assert best_bid.row["price"] == Decimal("63529.0")
    assert best_bid.row["quantity"] == Decimal("11.00191")
    assert best_bid.row["order_count"] == 36
    assert best_ask.row["price"] == Decimal("63530.0")
    assert best_ask.row["quantity"] == Decimal("6.12632")
    assert best_ask.row["order_count"] == 15
    assert {record.row["snapshot_id"] for record in snapshots} == {snapshots[0].row["snapshot_id"]}


def test_bbo_requires_both_sides_and_preserves_the_captured_values() -> None:
    parsed = parse_websocket_message(_envelope("ws_bbo.json"))

    _assert_wire_record("ws_bbo.json", parsed, "bbo")
    bbo = _single(parsed, "bbo")
    assert bbo.asset == "BTC"
    assert bbo.row["source_sequence"] is None
    assert bbo.row["bid_price"] == Decimal("63529.0")
    assert bbo.row["bid_quantity"] == Decimal("11.31732")
    assert bbo.row["ask_price"] == Decimal("63530.0")
    assert bbo.row["ask_quantity"] == Decimal("6.06021")
    assert not parsed.issues


def test_one_sided_bbo_is_normalized_without_inventing_the_missing_side() -> None:
    parsed = parse_websocket_message(_envelope("ws_bbo_one_sided.json"))

    _assert_wire_record("ws_bbo_one_sided.json", parsed, "bbo")
    bbo = _single(parsed, "bbo")
    assert bbo.row["bid_price"] == Decimal("63529.0")
    assert bbo.row["bid_quantity"] == Decimal("11.31732")
    assert bbo.row["ask_price"] is None
    assert bbo.row["ask_quantity"] is None
    assert bbo.row["source_sequence"] is None
    assert not parsed.issues


def test_trade_id_is_stable_and_hyperliquid_sides_map_to_aggressor_sides() -> None:
    raw_message = _raw("ws_trades.json")
    first = parse_websocket_message(_envelope("ws_trades.json", arrival_sequence=7))
    second = parse_websocket_message(_envelope("ws_trades.json", arrival_sequence=99))
    buy = _single(first, "trade")
    repeated = _single(second, "trade")

    _assert_wire_record("ws_trades.json", first, "trades")
    assert buy.asset == "BTC"
    assert buy.row["trade_id"] == "1786490408993:BTC:729145529777842"
    assert repeated.row["trade_id"] == buy.row["trade_id"]
    assert buy.row["aggressor_side"] == "buy"
    assert buy.row["price"] == Decimal("63525.0")
    assert buy.row["quantity"] == Decimal("0.00118")
    assert buy.row["quote_quantity"] == Decimal("74.9595")

    ask_payload = json.loads(raw_message)
    ask_payload["data"][0]["side"] = "A"
    ask_raw = json.dumps(ask_payload, separators=(",", ":"))
    sell = _single(
        parse_websocket_message(_envelope("ws_trades.json", raw_message=ask_raw)),
        "trade",
    )
    assert sell.row["aggressor_side"] == "sell"
    assert sell.row["trade_id"] == buy.row["trade_id"]


def test_trade_fixture_uses_only_documented_deterministic_pseudonyms() -> None:
    trade = json.loads(_raw("ws_trades.json"))["data"][0]

    assert trade["hash"] == "0x" + "1" * 64
    assert trade["users"] == ["0x" + "a" * 40, "0x" + "b" * 40]


def test_candle_maps_exchange_times_and_ohlcv_without_lookahead() -> None:
    parsed = parse_websocket_message(_envelope("ws_candle.json"))

    _assert_wire_record("ws_candle.json", parsed, "candle")
    candle = _single(parsed, "candle")
    assert candle.asset == "BTC"
    assert candle.row["source_sequence"] is None
    assert candle.row["interval"] == "1m"
    assert candle.row["open_time"] == datetime.fromtimestamp(1_786_490_400_000 / 1_000, tz=UTC)
    assert candle.row["close_time"] == datetime.fromtimestamp(1_786_490_459_999 / 1_000, tz=UTC)
    assert candle.row["open"] == Decimal("63522.0")
    assert candle.row["high"] == Decimal("63530.0")
    assert candle.row["low"] == Decimal("63521.0")
    assert candle.row["close"] == Decimal("63530.0")
    assert candle.row["base_volume"] == Decimal("1.96917")
    assert candle.row["quote_volume"] is None
    assert candle.row["trade_count"] == 47


def test_perp_active_context_maps_mark_oracle_mid_funding_oi_and_volume() -> None:
    parsed = parse_websocket_message(_envelope("ws_active_asset_ctx.json"))

    _assert_wire_record("ws_active_asset_ctx.json", parsed, "activeAssetCtx")
    context = _single(parsed, "market_context")
    assert context.asset == "BTC"
    assert context.row["instrument_kind"] == "perp"
    assert context.row["source_sequence"] is None
    assert context.row["mark_price"] == Decimal("63527.0")
    assert context.row["oracle_price"] == Decimal("63560.0")
    assert context.row["mid_price"] == Decimal("63529.5")
    assert context.row["current_funding_rate"] == Decimal("-0.000001763")
    assert context.row["open_interest_quantity"] == Decimal("39345.79612")
    assert context.row["open_interest_notional"] == Decimal("2499520390.115240")
    assert context.row["base_volume_24h"] == Decimal("19325.30151")
    assert context.row["notional_volume_24h"] == Decimal("1234095189.848790884")
    assert context.row["previous_day_price"] == Decimal("63970.0")
    assert context.row["circulating_supply"] is None


def test_documentary_spot_context_maps_only_fields_present_on_spot() -> None:
    parsed = parse_websocket_message(_envelope("ws_active_spot_asset_ctx.json"))

    _assert_wire_record("ws_active_spot_asset_ctx.json", parsed, "activeSpotAssetCtx")
    context = _single(parsed, "market_context")
    assert context.asset == "@107"
    assert context.row["instrument_kind"] == "spot"
    assert context.row["mark_price"] == Decimal("63527.0")
    assert context.row["mid_price"] == Decimal("63529.5")
    assert context.row["notional_volume_24h"] == Decimal("1234095189.848790884")
    assert context.row["circulating_supply"] == Decimal("19880000.0")
    assert context.row["oracle_price"] is None
    assert context.row["current_funding_rate"] is None
    assert context.row["open_interest_quantity"] is None


def test_rest_spot_context_preserves_real_zero_previous_day_price() -> None:
    spot_payload = json.loads((FIXTURES / "rest_spot_context_prev_day_zero.json").read_text("utf-8"))
    records = parse_bootstrap(
        PublicBootstrap(
            observed_at_ms=1_786_490_460_000,
            perp_payload=[{"universe": []}, []],
            spot_payload=spot_payload,
        )
    )

    context = next(record for record in records if record.record_type == RecordType.MARKET_CONTEXT)

    assert context.asset == "@189"
    assert context.row["instrument_kind"] == "spot"
    assert context.row["mark_price"] == Decimal("620.0")
    assert context.row["mid_price"] is None
    assert context.row["notional_volume_24h"] == Decimal("0.0")
    assert context.row["previous_day_price"] == Decimal("0.0")


@pytest.mark.parametrize("include_null_field", [True, False])
def test_spot_context_tolerates_null_or_absent_previous_day_price(
    include_null_field: bool,
) -> None:
    context: dict[str, object] = {
        "dayNtlVlm": "0.0",
        "markPx": "1.0",
        "midPx": None,
        "circulatingSupply": "0.0",
    }
    if include_null_field:
        context["prevDayPx"] = None
    raw_message = json.dumps(
        {"channel": "activeSpotAssetCtx", "data": {"coin": "@71", "ctx": context}},
        separators=(",", ":"),
    )

    parsed = parse_websocket_message(_envelope("ws_active_spot_asset_ctx.json", raw_message=raw_message))
    normalized = _single(parsed, "market_context")

    assert normalized.row["mark_price"] == Decimal("1.0")
    assert normalized.row["previous_day_price"] is None


def test_empty_l2_book_preserves_header_without_fabricated_levels() -> None:
    envelope = WireEnvelope(
        raw_message=json.dumps(
            {
                "channel": "l2Book",
                "data": {"coin": "EMPTY", "time": 1786490423370, "levels": [[], []]},
            },
            separators=(",", ":"),
        ),
        received_time=datetime(2026, 8, 12, tzinfo=UTC),
        connection_id="empty-book",
        connection_epoch=2,
        arrival_sequence=7,
    )

    parsed = parse_websocket_message(envelope)

    states = [record for record in parsed.records if record.record_type == RecordType.L2_BOOK_STATE]
    levels = [record for record in parsed.records if record.record_type == RecordType.L2_SNAPSHOT]
    assert len(states) == 1
    assert (states[0].row["bid_level_count"], states[0].row["ask_level_count"]) == (0, 0)
    assert levels == []


def test_all_json_fixtures_are_valid_and_opening_text_is_exact() -> None:
    assert _raw("ws_open.txt") == "Websocket connection established."
    for path in sorted(FIXTURES.glob("*.json")):
        json.loads(path.read_text(encoding="utf-8"))
