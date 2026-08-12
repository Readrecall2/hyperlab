from __future__ import annotations

import json
import socket
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

import pytest

from hyperlab.collector.replay import replay_fixture

FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"
REPLAY = FIXTURES / "replay"
FROZEN_TIME = datetime.fromtimestamp(1_786_490_460_000 / 1_000, tz=UTC)
EXPECTED_REPLAY_SOURCES = {
    "001_open.txt": "ws_open.txt",
    "002_subscription_response.json": "ws_subscription_response.json",
    "003_pong.json": "ws_pong.json",
    "004_active_asset_ctx.json": "ws_active_asset_ctx.json",
    "005_active_spot_asset_ctx.json": "ws_active_spot_asset_ctx.json",
    "006_bbo.json": "ws_bbo.json",
    "007_l2_book.json": "ws_l2_book.json",
    "008_trades.json": "ws_trades.json",
    "009_candle.json": "ws_candle.json",
}


@dataclass
class FrozenClock:
    calls: int = 0

    def __call__(self) -> datetime:
        self.calls += 1
        return FROZEN_TIME


def _record_type(record: Any) -> str:
    value = record.record_type
    return value.value if hasattr(value, "value") else value


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return str(value.value)
    raise TypeError(f"unsupported replay value: {type(value).__name__}")


def _canonical_bytes(records: list[Any]) -> bytes:
    payload = [
        {
            "record_type": _record_type(record),
            "asset": record.asset,
            "row": record.row,
        }
        for record in records
    ]
    return json.dumps(
        payload,
        default=_json_default,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def test_replay_is_offline_stably_ordered_and_byte_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def network_forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("replay attempted to create a network socket")

    monkeypatch.setattr(socket, "socket", network_forbidden)
    assert [path.name for path in sorted(REPLAY.iterdir())] == list(EXPECTED_REPLAY_SOURCES)
    for replay_name, source_name in EXPECTED_REPLAY_SOURCES.items():
        assert (REPLAY / replay_name).read_bytes() == (FIXTURES / source_name).read_bytes()

    first_records: list[Any] = []
    second_records: list[Any] = []
    first_clock = FrozenClock()
    second_clock = FrozenClock()

    first_summary = replay_fixture(REPLAY, first_records.append, first_clock)
    second_summary = replay_fixture(REPLAY, second_records.append, second_clock)

    expected_summary = {
        "fixture_count": 9,
        "record_count": 19,
        "issue_count": 0,
        "channels": [
            None,
            "subscriptionResponse",
            "pong",
            "activeAssetCtx",
            "activeSpotAssetCtx",
            "bbo",
            "l2Book",
            "trades",
            "candle",
        ],
    }
    assert first_summary == second_summary == expected_summary
    assert first_clock.calls == second_clock.calls == 9
    assert _canonical_bytes(first_records) == _canonical_bytes(second_records)

    record_types = [_record_type(record) for record in first_records]
    assert Counter(record_types) == Counter(
        {
            "wire_message": 9,
            "market_context": 2,
            "bbo": 1,
            "l2_book_state": 1,
            "l2_snapshot": 4,
            "trade": 1,
            "candle": 1,
        }
    )
    assert "l2_delta" not in record_types
    assert all(record.row["source_sequence"] is None for record in first_records)

    wire_records = [record for record in first_records if _record_type(record) == "wire_message"]
    assert wire_records[0].row["raw_message"] == "Websocket connection established."
    assert wire_records[0].row["is_json"] is False
    assert [record.row["arrival_sequence"] for record in wire_records] == list(range(1, 10))

    contexts = [record for record in first_records if _record_type(record) == "market_context"]
    assert [(record.asset, record.row["instrument_kind"]) for record in contexts] == [
        ("BTC", "perp"),
        ("@107", "spot"),
    ]

    bbo = next(record for record in first_records if _record_type(record) == "bbo")
    assert (bbo.row["bid_price"], bbo.row["ask_price"]) == (
        Decimal("63529.0"),
        Decimal("63530.0"),
    )

    book_state = next(record for record in first_records if _record_type(record) == "l2_book_state")
    assert (book_state.row["bid_level_count"], book_state.row["ask_level_count"]) == (2, 2)
    levels = [record for record in first_records if _record_type(record) == "l2_snapshot"]
    assert [(record.row["side"], record.row["level"]) for record in levels] == [
        ("bid", 0),
        ("bid", 1),
        ("ask", 0),
        ("ask", 1),
    ]

    trade = next(record for record in first_records if _record_type(record) == "trade")
    assert trade.row["trade_id"] == "1786490408993:BTC:729145529777842"
    candle = next(record for record in first_records if _record_type(record) == "candle")
    assert (candle.asset, candle.row["interval"], candle.row["is_final"]) == (
        "BTC",
        "1m",
        None,
    )
