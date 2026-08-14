from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from hyperlab.collector.models import ParsedMessage, ParsedRecord, WireEnvelope
from hyperlab.data.schema import RecordType, instrument, latest_schema_for

_OPEN_MESSAGE = "Websocket connection established."
_VENUE = "hyperliquid"


def _datetime_ms(value: object) -> datetime:
    try:
        milliseconds = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid millisecond timestamp: {value!r}") from exc
    if milliseconds < 0:
        raise ValueError("millisecond timestamp cannot be negative")
    return datetime.fromtimestamp(milliseconds / 1_000, tz=UTC)


def _decimal(value: object, *, required: bool = False) -> Decimal | None:
    if value is None or value == "":
        if required:
            raise ValueError("required numeric value is missing")
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid numeric value: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"non-finite numeric value: {value!r}")
    return result


def _required_decimal(value: object) -> Decimal:
    result = _decimal(value, required=True)
    assert result is not None
    return result


def _mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _sequence(value: object, *, label: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{label} must be an array")
    return value


def _common(
    record_type: RecordType,
    asset: str,
    envelope: WireEnvelope,
    *,
    event_time: datetime,
    exchange_time: datetime | None,
) -> dict[str, object]:
    return {
        "schema_version": latest_schema_for(record_type).version,
        "record_type": record_type.value,
        "venue": _VENUE,
        "asset": asset,
        "event_time": event_time,
        "exchange_time": exchange_time,
        "received_time": envelope.received_time,
        "source_sequence": None,
        "connection_id": envelope.connection_id,
    }


def _observation_id(envelope: WireEnvelope) -> str:
    return f"{envelope.connection_id}:{envelope.connection_epoch}:{envelope.arrival_sequence}"


def _message_asset(channel: str | None, data: object) -> str | None:
    if channel == "candle" and isinstance(data, Mapping):
        return None if not data.get("s") else str(data["s"])
    if channel == "trades" and isinstance(data, Sequence) and data:
        first = data[0]
        if isinstance(first, Mapping):
            return None if not first.get("coin") else str(first["coin"])
    if isinstance(data, Mapping):
        return None if not data.get("coin") else str(data["coin"])
    return None


def _wire_record(
    envelope: WireEnvelope,
    *,
    channel: str | None,
    message_asset: str | None,
    is_json: bool,
) -> ParsedRecord:
    row = _common(
        RecordType.WIRE_MESSAGE,
        "GLOBAL",
        envelope,
        event_time=envelope.received_time,
        exchange_time=None,
    )
    row.update(
        {
            "connection_epoch": envelope.connection_epoch,
            "arrival_sequence": envelope.arrival_sequence,
            "capture_epoch_id": envelope.capture_epoch_id,
            "channel": channel,
            "message_asset": message_asset,
            "raw_message": envelope.raw_message,
            "is_json": is_json,
            "payload_sha256": hashlib.sha256(envelope.raw_message.encode()).hexdigest(),
        }
    )
    return ParsedRecord(RecordType.WIRE_MESSAGE, "GLOBAL", row)


def parse_websocket_message(envelope: WireEnvelope) -> ParsedMessage:
    """Parse one public wire message without network access or hidden state."""

    try:
        decoded = json.loads(envelope.raw_message)
    except json.JSONDecodeError:
        wire = _wire_record(
            envelope,
            channel=None,
            message_asset=None,
            is_json=False,
        )
        parse_issues = () if envelope.raw_message == _OPEN_MESSAGE else ("invalid_json",)
        return ParsedMessage(channel=None, records=(wire,), issues=parse_issues)

    if not isinstance(decoded, Mapping):
        wire = _wire_record(envelope, channel=None, message_asset=None, is_json=True)
        return ParsedMessage(channel=None, records=(wire,), issues=("json_root_not_object",))

    raw_channel = decoded.get("channel")
    channel = raw_channel if isinstance(raw_channel, str) and raw_channel else None
    data = decoded.get("data")
    asset = _message_asset(channel, data)
    records: list[ParsedRecord] = [_wire_record(envelope, channel=channel, message_asset=asset, is_json=True)]
    issues: list[str] = []
    acknowledged: Mapping[str, Any] | None = None
    is_pong = channel == "pong"

    try:
        if channel == "subscriptionResponse":
            ack = _mapping(data, label="subscription response")
            subscription = ack.get("subscription")
            if isinstance(subscription, Mapping):
                acknowledged = subscription
        elif channel == "l2Book":
            records.extend(_parse_l2(data, envelope))
        elif channel == "bbo":
            records.extend(_parse_bbo(data, envelope))
        elif channel == "trades":
            records.extend(_parse_trades(data, envelope))
        elif channel == "candle":
            records.append(_parse_candle(data, envelope))
        elif channel in {"activeAssetCtx", "activeSpotAssetCtx"}:
            records.append(_parse_context(channel, data, envelope))
        elif channel != "pong":
            issues.append("unknown_channel")
    except (KeyError, TypeError, ValueError) as exc:
        issues.append(f"invalid_{channel or 'message'}:{type(exc).__name__}:{exc}")

    return ParsedMessage(
        channel=channel,
        records=tuple(records),
        issues=tuple(issues),
        acknowledged_subscription=acknowledged,
        is_pong=is_pong,
    )


def _parse_l2(data: object, envelope: WireEnvelope) -> list[ParsedRecord]:
    book = _mapping(data, label="l2Book data")
    coin = str(book["coin"])
    exchange_time = _datetime_ms(book["time"])
    levels = _sequence(book["levels"], label="l2Book levels")
    if len(levels) != 2:
        raise ValueError("l2Book levels must contain bids and asks")
    snapshot_id = (
        f"ws:{envelope.connection_id}:{envelope.connection_epoch}:"
        f"{envelope.arrival_sequence}:{int(str(book['time']))}"
    )
    book_epoch_id = f"{envelope.connection_id}:{envelope.connection_epoch}"
    bid_levels = _sequence(levels[0], label="l2Book bid levels")
    ask_levels = _sequence(levels[1], label="l2Book ask levels")
    header = _common(
        RecordType.L2_BOOK_STATE,
        coin,
        envelope,
        event_time=exchange_time,
        exchange_time=exchange_time,
    )
    header.update(
        {
            "snapshot_id": snapshot_id,
            "book_epoch_id": book_epoch_id,
            "bid_level_count": len(bid_levels),
            "ask_level_count": len(ask_levels),
        }
    )
    records: list[ParsedRecord] = [ParsedRecord(RecordType.L2_BOOK_STATE, coin, header)]
    for side, raw_levels in zip(("bid", "ask"), levels, strict=True):
        side_levels = _sequence(raw_levels, label=f"l2Book {side} levels")
        for level_number, raw_level in enumerate(side_levels):
            level = _mapping(raw_level, label=f"l2Book {side} level")
            row = _common(
                RecordType.L2_SNAPSHOT,
                coin,
                envelope,
                event_time=exchange_time,
                exchange_time=exchange_time,
            )
            row.update(
                {
                    "snapshot_id": snapshot_id,
                    "book_epoch_id": book_epoch_id,
                    "last_sequence": None,
                    "side": side,
                    "level": level_number,
                    "price": _required_decimal(level["px"]),
                    "quantity": _required_decimal(level["sz"]),
                    "order_count": int(str(level["n"])),
                }
            )
            records.append(ParsedRecord(RecordType.L2_SNAPSHOT, coin, row))
    return records


def _parse_bbo(data: object, envelope: WireEnvelope) -> list[ParsedRecord]:
    payload = _mapping(data, label="bbo data")
    sides = _sequence(payload["bbo"], label="bbo sides")
    if len(sides) != 2:
        raise ValueError("bbo must contain bid and ask")
    bid = None if sides[0] is None else _mapping(sides[0], label="bbo bid")
    ask = None if sides[1] is None else _mapping(sides[1], label="bbo ask")
    coin = str(payload["coin"])
    exchange_time = _datetime_ms(payload["time"])
    row = _common(
        RecordType.BBO,
        coin,
        envelope,
        event_time=exchange_time,
        exchange_time=exchange_time,
    )
    row.update(
        {
            "update_id": (
                f"{payload['time']}:{coin}:{envelope.connection_id}:"
                f"{envelope.connection_epoch}:{envelope.arrival_sequence}"
            ),
            "bid_price": None if bid is None else _required_decimal(bid["px"]),
            "bid_quantity": None if bid is None else _required_decimal(bid["sz"]),
            "ask_price": None if ask is None else _required_decimal(ask["px"]),
            "ask_quantity": None if ask is None else _required_decimal(ask["sz"]),
        }
    )
    return [ParsedRecord(RecordType.BBO, coin, row)]


def _parse_trades(data: object, envelope: WireEnvelope) -> list[ParsedRecord]:
    trades = _sequence(data, label="trades data")
    records: list[ParsedRecord] = []
    for raw_trade in trades:
        trade = _mapping(raw_trade, label="trade")
        coin = str(trade["coin"])
        trade_time = int(str(trade["time"]))
        trade_id = int(str(trade["tid"]))
        exchange_time = _datetime_ms(trade_time)
        price = _required_decimal(trade["px"])
        quantity = _required_decimal(trade["sz"])
        side = {"B": "buy", "A": "sell"}.get(str(trade.get("side")), "unknown")
        row = _common(
            RecordType.TRADE,
            coin,
            envelope,
            event_time=exchange_time,
            exchange_time=exchange_time,
        )
        row.update(
            {
                "trade_id": f"{trade_time}:{coin}:{trade_id}",
                "aggressor_side": side,
                "price": price,
                "quantity": quantity,
                "quote_quantity": price * quantity,
                "is_liquidation": None,
                "connection_epoch": envelope.connection_epoch,
                "arrival_sequence": envelope.arrival_sequence,
            }
        )
        records.append(ParsedRecord(RecordType.TRADE, coin, row))
    return records


def _parse_candle(data: object, envelope: WireEnvelope) -> ParsedRecord:
    candle = _mapping(data, label="candle data")
    coin = str(candle["s"])
    open_time = _datetime_ms(candle["t"])
    close_time = _datetime_ms(candle["T"])
    row = _common(
        RecordType.CANDLE,
        coin,
        envelope,
        event_time=open_time,
        exchange_time=None,
    )
    row.update(
        {
            "interval": str(candle["i"]),
            "open_time": open_time,
            "close_time": close_time,
            "open": _required_decimal(candle["o"]),
            "high": _required_decimal(candle["h"]),
            "low": _required_decimal(candle["l"]),
            "close": _required_decimal(candle["c"]),
            "base_volume": _required_decimal(candle["v"]),
            "quote_volume": None,
            "trade_count": int(str(candle["n"])),
            "is_final": None,
            "observation_id": _observation_id(envelope),
        }
    )
    return ParsedRecord(RecordType.CANDLE, coin, row)


def _parse_context(
    channel: str,
    data: object,
    envelope: WireEnvelope,
) -> ParsedRecord:
    payload = _mapping(data, label="active asset context")
    coin = str(payload["coin"])
    context = _mapping(payload["ctx"], label="asset context")
    instrument_kind = "spot" if channel == "activeSpotAssetCtx" else "perp"
    mark_price = _decimal(context.get("markPx"))
    open_interest = _decimal(context.get("openInterest"))
    open_interest_notional = (
        mark_price * open_interest if mark_price is not None and open_interest is not None else None
    )
    row = _common(
        RecordType.MARKET_CONTEXT,
        coin,
        envelope,
        event_time=envelope.received_time,
        exchange_time=None,
    )
    row.update(
        {
            "instrument_kind": instrument_kind,
            "instrument_id": instrument(_VENUE, coin, instrument_kind),
            "mark_price": mark_price,
            "oracle_price": _decimal(context.get("oraclePx")),
            "mid_price": _decimal(context.get("midPx")),
            "current_funding_rate": _decimal(context.get("funding")),
            "open_interest_quantity": open_interest,
            "open_interest_notional": open_interest_notional,
            "base_volume_24h": _decimal(context.get("dayBaseVlm")),
            "notional_volume_24h": _decimal(context.get("dayNtlVlm")),
            "previous_day_price": _decimal(context.get("prevDayPx")),
            "circulating_supply": _decimal(context.get("circulatingSupply")),
            "observation_id": _observation_id(envelope),
        }
    )
    return ParsedRecord(RecordType.MARKET_CONTEXT, coin, row)
