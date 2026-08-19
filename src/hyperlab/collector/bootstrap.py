from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from hyperlab.api.public import PublicBootstrap, hyperliquid_spot_coin
from hyperlab.collector.models import ParsedRecord, WireEnvelope
from hyperlab.collector.parser import (
    _common,
    _datetime_ms,
    _decimal,
    _mapping,
    _observation_id,
    _parse_bbo,
    _parse_candle,
    _parse_l2,
    _required_decimal,
    _sequence,
)
from hyperlab.data.schema import RecordType, instrument


def _canonical_metadata(value: object) -> tuple[str, str]:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return payload, hashlib.sha256(payload.encode()).hexdigest()


def _metadata_record(
    envelope: WireEnvelope,
    *,
    asset: str,
    kind: str,
    source_symbol: str,
    source_index: int | None,
    base_token: str | None,
    quote_token: str | None,
    sz_decimals: int | None,
    wei_decimals: int | None,
    max_leverage: int | None,
    margin_table_id: int | None,
    is_canonical: bool | None,
    full_name: str | None,
    raw_metadata: object,
) -> ParsedRecord:
    metadata_json, metadata_sha256 = _canonical_metadata(raw_metadata)
    row = _common(
        RecordType.INSTRUMENT_METADATA,
        asset,
        envelope,
        event_time=envelope.received_time,
        exchange_time=None,
    )
    row.update(
        {
            "instrument_kind": kind,
            "instrument_id": instrument("hyperliquid", source_symbol, kind),
            "source_symbol": source_symbol,
            "source_index": source_index,
            "base_token": base_token,
            "quote_token": quote_token,
            "sz_decimals": sz_decimals,
            "wei_decimals": wei_decimals,
            "max_leverage": max_leverage,
            "margin_table_id": margin_table_id,
            "is_canonical": is_canonical,
            "full_name": full_name,
            "metadata_sha256": metadata_sha256,
            "metadata_json": metadata_json,
        }
    )
    return ParsedRecord(RecordType.INSTRUMENT_METADATA, asset, row)


def _context_record(
    envelope: WireEnvelope,
    *,
    asset: str,
    source_symbol: str,
    kind: str,
    context: Mapping[str, Any],
) -> ParsedRecord:
    mark = _decimal(context.get("markPx"))
    open_interest = _decimal(context.get("openInterest"))
    row = _common(
        RecordType.MARKET_CONTEXT,
        asset,
        envelope,
        event_time=envelope.received_time,
        exchange_time=None,
    )
    row.update(
        {
            "instrument_kind": kind,
            "instrument_id": instrument("hyperliquid", source_symbol, kind),
            "mark_price": mark,
            "oracle_price": _decimal(context.get("oraclePx")),
            "mid_price": _decimal(context.get("midPx")),
            "current_funding_rate": _decimal(context.get("funding")),
            "open_interest_quantity": open_interest,
            "open_interest_notional": (
                mark * open_interest if mark is not None and open_interest is not None else None
            ),
            "base_volume_24h": _decimal(context.get("dayBaseVlm")),
            "notional_volume_24h": _decimal(context.get("dayNtlVlm")),
            "previous_day_price": _decimal(context.get("prevDayPx")),
            "circulating_supply": _decimal(context.get("circulatingSupply")),
            "observation_id": _observation_id(envelope),
        }
    )
    return ParsedRecord(RecordType.MARKET_CONTEXT, asset, row)


def parse_bootstrap(
    bootstrap: PublicBootstrap,
    *,
    connection_id: str = "rest-bootstrap",
    connection_epoch: int = 1,
) -> tuple[ParsedRecord, ...]:
    envelope = WireEnvelope(
        raw_message="{}",
        received_time=datetime.fromtimestamp(bootstrap.observed_at_ms / 1_000, tz=UTC),
        connection_id=connection_id,
        connection_epoch=connection_epoch,
        arrival_sequence=1,
    )
    perp_meta, perp_contexts = bootstrap.perp_payload
    spot_meta, spot_contexts = bootstrap.spot_payload
    perp = _mapping(perp_meta, label="perp metadata")
    spot = _mapping(spot_meta, label="spot metadata")
    perp_universe = _sequence(perp.get("universe"), label="perp universe")
    perp_ctxs = _sequence(perp_contexts, label="perp contexts")
    if len(perp_universe) != len(perp_ctxs):
        raise ValueError("perp metadata and contexts are not aligned")

    records: list[ParsedRecord] = []
    for index, (raw_meta, raw_context) in enumerate(zip(perp_universe, perp_ctxs, strict=True)):
        metadata = _mapping(raw_meta, label="perp instrument")
        context = _mapping(raw_context, label="perp context")
        source_symbol = str(metadata["name"])
        records.append(
            _metadata_record(
                envelope,
                asset=source_symbol,
                kind="perp",
                source_symbol=source_symbol,
                source_index=index,
                base_token=None,
                quote_token=None,
                sz_decimals=int(metadata["szDecimals"]),
                wei_decimals=None,
                max_leverage=(None if metadata.get("maxLeverage") is None else int(metadata["maxLeverage"])),
                margin_table_id=(
                    None if metadata.get("marginTableId") is None else int(metadata["marginTableId"])
                ),
                is_canonical=None,
                full_name=None,
                raw_metadata=metadata,
            )
        )
        records.append(
            _context_record(
                envelope,
                asset=source_symbol,
                source_symbol=source_symbol,
                kind="perp",
                context=context,
            )
        )

    tokens = {
        int(token["index"]): token
        for raw_token in _sequence(spot.get("tokens"), label="spot tokens")
        if (token := _mapping(raw_token, label="spot token"))
    }
    contexts_by_coin = {
        str(context["coin"]): context
        for raw_context in _sequence(spot_contexts, label="spot contexts")
        if (context := _mapping(raw_context, label="spot context")) and context.get("coin")
    }
    for raw_pair in _sequence(spot.get("universe"), label="spot universe"):
        pair = _mapping(raw_pair, label="spot pair")
        token_indexes = _sequence(pair.get("tokens"), label="spot pair tokens")
        if len(token_indexes) != 2:
            raise ValueError("spot pair must contain base and quote token indices")
        base = _mapping(tokens[int(token_indexes[0])], label="spot base token")
        quote = _mapping(tokens[int(token_indexes[1])], label="spot quote token")
        source_symbol = hyperliquid_spot_coin(pair)
        asset = source_symbol
        records.append(
            _metadata_record(
                envelope,
                asset=asset,
                kind="spot",
                source_symbol=source_symbol,
                source_index=int(pair["index"]),
                base_token=str(base["name"]),
                quote_token=str(quote["name"]),
                sz_decimals=int(base["szDecimals"]),
                wei_decimals=int(base["weiDecimals"]),
                max_leverage=None,
                margin_table_id=None,
                is_canonical=(None if pair.get("isCanonical") is None else bool(pair["isCanonical"])),
                full_name=None if base.get("fullName") is None else str(base["fullName"]),
                raw_metadata={"pair": pair, "base": base, "quote": quote},
            )
        )
        spot_context = contexts_by_coin.get(source_symbol)
        if spot_context is not None:
            records.append(
                _context_record(
                    envelope,
                    asset=asset,
                    source_symbol=source_symbol,
                    kind="spot",
                    context=spot_context,
                )
            )
    return tuple(records)


def historical_envelope(
    received_time: datetime,
    *,
    connection_id: str,
    connection_epoch: int,
    arrival_sequence: int = 1,
) -> WireEnvelope:
    return WireEnvelope(
        raw_message="{}",
        received_time=received_time,
        connection_id=connection_id,
        connection_epoch=connection_epoch,
        arrival_sequence=arrival_sequence,
    )


def _finalized_hourly_funding_time(value: object) -> datetime:
    observed_time = _datetime_ms(value)
    funding_hour = observed_time.replace(minute=0, second=0, microsecond=0)
    if observed_time - funding_hour >= timedelta(seconds=60):
        raise ValueError(
            "finalized Hyperliquid funding time must lie within the first 60 seconds "
            "of its UTC hour"
        )
    return funding_hour


def parse_funding_history(
    payload: object,
    envelope: WireEnvelope,
) -> tuple[ParsedRecord, ...]:
    records: list[ParsedRecord] = []
    for raw_item in _sequence(payload, label="funding history"):
        item = _mapping(raw_item, label="funding record")
        coin = str(item["coin"])
        funding_time = _finalized_hourly_funding_time(item["time"])
        row = _common(
            RecordType.FUNDING,
            coin,
            envelope,
            event_time=funding_time,
            exchange_time=funding_time,
        )
        row.update(
            {
                "funding_time": funding_time,
                "funding_rate": _required_decimal(item["fundingRate"]),
                "funding_interval_seconds": 3_600,
                "rate_kind": "hyperliquid-hourly-settlement",
                "mark_price": None,
                "oracle_price": None,
                "observation_id": _observation_id(envelope),
            }
        )
        records.append(ParsedRecord(RecordType.FUNDING, coin, row))
    return tuple(records)


def parse_candles(
    payload: object,
    envelope: WireEnvelope,
) -> tuple[ParsedRecord, ...]:
    return tuple(_parse_candle(item, envelope) for item in _sequence(payload, label="candle history"))


def parse_l2_snapshot(payload: object, envelope: WireEnvelope) -> tuple[ParsedRecord, ...]:
    records = _parse_l2(payload, envelope)
    result: list[ParsedRecord] = []
    for record in records:
        row = dict(record.row)
        row["snapshot_id"] = str(row["snapshot_id"]).replace("ws:", "rest:", 1)
        row["event_time"] = envelope.received_time
        row["exchange_time"] = row.get("exchange_time")
        result.append(ParsedRecord(record.record_type, record.asset, row))
    return tuple(result)


def parse_bbo_from_l2(payload: object, envelope: WireEnvelope) -> tuple[ParsedRecord, ...]:
    """Derive the documented BBO state from a full REST L2 snapshot."""

    book = _mapping(payload, label="REST L2 book")
    levels = _sequence(book["levels"], label="REST L2 sides")
    if len(levels) != 2:
        raise ValueError("REST L2 book must contain bid and ask sides")
    bid_levels = _sequence(levels[0], label="REST L2 bids")
    ask_levels = _sequence(levels[1], label="REST L2 asks")
    bbo_payload = {
        "coin": book["coin"],
        "time": book["time"],
        "bbo": [
            None if not bid_levels else bid_levels[0],
            None if not ask_levels else ask_levels[0],
        ],
    }
    records = _parse_bbo(bbo_payload, envelope)
    result: list[ParsedRecord] = []
    for record in records:
        row = dict(record.row)
        row["update_id"] = f"rest:{row['update_id']}"
        result.append(
            ParsedRecord(record.record_type, record.asset, row)
        )
    return tuple(result)
