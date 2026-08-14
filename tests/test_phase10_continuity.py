from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from hyperlab.cli import app
from hyperlab.collector.bootstrap import (
    historical_envelope,
    parse_bbo_from_l2,
    parse_l2_snapshot,
)
from hyperlab.collector.models import ParsedRecord, WireEnvelope
from hyperlab.collector.parser import parse_websocket_message
from hyperlab.collector.storage import BatchingLakeSink
from hyperlab.data import cli as data_cli
from hyperlab.data.continuity import (
    PHASE_10_STATUS,
    Interval,
    _at_or_after_resync_arm,
    _duration,
    _merge,
    _required_market_intervals,
    _wire_kind,
    audit_phase10_continuity,
)
from hyperlab.data.schema import RecordType
from hyperlab.venues.base import measure_clock
from hyperlab.venues.binance import BinancePublicConnector, clock_record

BASE = datetime(2026, 8, 13, 12, tzinfo=UTC)
runner = CliRunner()


def _binance_connector() -> BinancePublicConnector:
    template = {
        "pair": "",
        "contractType": "PERPETUAL",
        "status": "TRADING",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
        ],
    }
    symbols = [
        {
            **template,
            "symbol": f"{asset}USDT",
            "pair": f"{asset}USDT",
            "baseAsset": asset,
        }
        for asset in ("BTC", "ETH")
    ]
    return BinancePublicConnector.from_exchange_info(
        {"symbols": symbols},
        ("BTC", "ETH"),
    )


def _binance_frame(
    connector: BinancePublicConnector,
    *,
    asset: str,
    kind: str,
    received: datetime,
    connection_id: str,
    arrival: int,
    source_sequence: int,
    capture: str,
    connection_epoch: int = 1,
    empty_l2: bool = False,
    zero_bbo_quantity: bool = False,
) -> tuple[ParsedRecord, ...]:
    symbol = f"{asset}USDT"
    milliseconds = int(received.timestamp() * 1_000)
    if kind == "bbo":
        stream = f"{symbol.lower()}@bookTicker"
        data: dict[str, object] = {
            "e": "bookTicker",
            "E": milliseconds,
            "T": milliseconds,
            "s": symbol,
            "u": source_sequence,
            "b": "60000",
            "B": "0" if zero_bbo_quantity else "1",
            "a": "60001",
            "A": "1",
        }
    elif kind == "l2":
        stream = f"{symbol.lower()}@depth20@100ms"
        data = {
            "e": "depthUpdate",
            "E": milliseconds,
            "T": milliseconds,
            "s": symbol,
            "U": source_sequence,
            "u": source_sequence,
            "pu": source_sequence - 1,
            "b": [] if empty_l2 else [[
                "60000",
                "0" if zero_bbo_quantity else "1",
            ]],
            "a": [] if empty_l2 else [["60001", "1"]],
        }
    else:
        stream = f"{symbol.lower()}@aggTrade"
        data = {
            "e": "aggTrade",
            "E": milliseconds,
            "T": milliseconds,
            "s": symbol,
            "a": source_sequence,
            "p": "60000",
            "q": "0.01",
            "m": False,
        }
    envelope = WireEnvelope(
        json.dumps({"stream": stream, "data": data}, separators=(",", ":")),
        received,
        connection_id,
        connection_epoch,
        arrival,
        capture,
    )
    parsed = connector.parse_message(envelope)
    if empty_l2:
        assert len(parsed.issues) == 1
        assert "partial depth BBO requires non-empty bid and ask sides" in (
            parsed.issues[0]
        )
    else:
        assert parsed.issues == ()
    return parsed.records


def _hyperliquid_frame(
    *,
    asset: str,
    kind: str,
    received: datetime,
    arrival: int,
    capture: str,
    connection_id: str = "hyperliquid-public-1",
) -> tuple[ParsedRecord, ...]:
    milliseconds = int(received.timestamp() * 1_000)
    if kind == "bbo":
        payload: object = {
            "channel": "bbo",
            "data": {
                "coin": asset,
                "time": milliseconds,
                "bbo": [
                    {"px": "60000", "sz": "1", "n": 1},
                    {"px": "60001", "sz": "1", "n": 1},
                ],
            },
        }
    elif kind == "l2":
        payload = {
            "channel": "l2Book",
            "data": {
                "coin": asset,
                "time": milliseconds,
                "levels": [
                    [{"px": "60000", "sz": "1", "n": 1}],
                    [{"px": "60001", "sz": "1", "n": 1}],
                ],
            },
        }
    else:
        payload = {
            "channel": "trades",
            "data": [
                {
                    "coin": asset,
                    "side": "B",
                    "px": "60000",
                    "sz": "0.01",
                    "time": milliseconds,
                    "tid": milliseconds,
                }
            ],
        }
    return parse_websocket_message(
        WireEnvelope(
            json.dumps(payload, separators=(",", ":")),
            received,
            connection_id,
            1,
            arrival,
            capture,
        )
    ).records


def _binance_connection_event(
    *,
    event_kind: str,
    asset: str,
    received: datetime,
    capture: str,
    channel: str,
    connection_id: str = "binance-public-1",
    socket_role: str = "public",
    book_epoch_id: str | None = None,
    resync_snapshot_id: str | None = None,
    reason: str | None = None,
    connection_epoch: int = 1,
) -> ParsedRecord:
    return ParsedRecord(
        record_type=RecordType.CONNECTION_EVENT,
        asset=asset,
        row={
            "schema_version": 2,
            "record_type": "connection_event",
            "venue": "binance_usdm",
            "asset": asset,
            "event_time": received,
            "exchange_time": None,
            "received_time": received,
            "source_sequence": None,
            "connection_id": connection_id,
            "event_kind": event_kind,
            "channel": channel,
            "book_epoch_id": book_epoch_id,
            "reason": reason
            or (
                "complete Binance top-20 snapshot received"
                if event_kind.startswith("resync_")
                else "coverage_unknown:clock_sync request failed"
            ),
            "expected_sequence": None,
            "observed_sequence": None,
            "resync_snapshot_id": resync_snapshot_id,
            "connection_epoch": connection_epoch,
            "capture_epoch_id": capture,
            "socket_role": socket_role,
        },
    )


def _hyperliquid_connection_event(
    *,
    received: datetime,
    capture: str,
    event_kind: str = "connect",
    socket_role: str = "public",
    connection_id: str = "hyperliquid-public-1",
) -> ParsedRecord:
    return ParsedRecord(
        record_type=RecordType.CONNECTION_EVENT,
        asset="GLOBAL",
        row={
            "schema_version": 2,
            "record_type": "connection_event",
            "venue": "hyperliquid",
            "asset": "GLOBAL",
            "event_time": received,
            "exchange_time": None,
            "received_time": received,
            "source_sequence": None,
            "connection_id": connection_id,
            "event_kind": event_kind,
            "channel": "public",
            "book_epoch_id": None,
            "reason": None,
            "expected_sequence": None,
            "observed_sequence": None,
            "resync_snapshot_id": None,
            "connection_epoch": 1,
            "capture_epoch_id": capture,
            "socket_role": socket_role,
        },
    )


def _write_continuity_lake(
    root: Path,
    *,
    omit_binance_raw_trade: bool = False,
    omit_binance_resync: bool = False,
    orphan_binance_resync_complete: bool = False,
    binance_resync_time_shift_seconds: float = 0.0,
    mutate_binance_trade_received: bool = False,
    mutate_binance_trade_quote: bool = False,
    invalid_clock_at: datetime | None = None,
    invalid_clock_times: tuple[datetime, ...] = (),
    clock_failure_at: datetime | None = None,
    loose_clock_at: datetime | None = None,
    wrong_clock_identity_at: datetime | None = None,
    clock_sample_seconds: tuple[float, ...] = (0.5, 10.5, 20.5),
    prewindow_clock_invalid: bool = False,
    prewindow_clock_rejection_count: int = 0,
    prewindow_clock_failure: bool = False,
    prewindow_disconnect: bool = False,
    prewindow_conflicting_capture_event: bool = False,
    prewindow_unbound_clock_invalid: bool = False,
    add_invalid_only_capture: bool = False,
    straddle_trade_gap: bool = False,
    omit_binance_connect: bool = False,
    binance_market_connect_role: str = "market",
    binance_market_connection_epoch: int = 1,
    omit_hyperliquid_connect: bool = False,
    hyperliquid_connect_role: str = "public",
    hyperliquid_message_asset_override: object = Ellipsis,
    hyperliquid_message_asset_kind: str = "l2",
    hyperliquid_channel_override: object = Ellipsis,
    hyperliquid_channel_kind: str = "bbo",
    mixed_hyperliquid_trade_batch: bool = False,
    add_hyperliquid_rest_bootstrap: bool = False,
    corrupt_hyperliquid_rest_bbo: bool = False,
    forge_hyperliquid_rest_bbo_identity: bool = False,
    forge_hyperliquid_rest_l2_identity: bool = False,
    mutate_hyperliquid_rest_bbo_value: bool = False,
    offset_jump_at: datetime | None = None,
    mutate_binance_bbo: bool = False,
    mutate_binance_depth_bbo: bool = False,
    omit_binance_depth_bbo: bool = False,
    delayed_hyperliquid_trade: bool = False,
    extra_trade_at: datetime | None = None,
    extra_market_refresh_at: datetime | None = None,
    public_arrival_offset: int = 0,
    omit_binance_l2_levels: bool = False,
    empty_binance_l2: bool = False,
    zero_binance_bbo_quantity: bool = False,
    add_orphan_binance_l2_level: bool = False,
    add_orphan_binance_bbo: bool = False,
    forge_binance_maker_type: bool = False,
    forge_binance_stream_type: object | None = None,
    binance_message_asset_override: object = Ellipsis,
    binance_message_asset_kind: str = "l2",
    binance_channel_override: object = Ellipsis,
    binance_channel_kind: str = "l2",
    add_orphan_required_wire: bool = False,
    add_connect_only_capture: bool = False,
    add_half_paired_capture: bool = False,
    add_historical_connect_capture: bool = False,
    add_unbound_connect: bool = False,
    bound_gap_at: datetime | None = None,
    disconnect_at: datetime | None = None,
    clean_disconnect_at: datetime | None = None,
    clean_disconnect_roles: tuple[str, ...] = (
        "binance_public",
        "binance_market",
        "hyperliquid_public",
    ),
    clean_disconnect_unknown_identity: bool = False,
    add_unbound_gap: bool = False,
    orphan_resync_event_kind: str | None = None,
    temporal_resync_snapshot: str | None = None,
    add_second_complete_hyperliquid_capture: bool = False,
    periodic_refresh_seconds: tuple[int, ...] = (),
) -> None:
    connector = _binance_connector()
    binance_capture = "binance-capture-1"
    hyperliquid_capture = "hyperliquid-capture-1"
    records: list[ParsedRecord] = []
    if add_historical_connect_capture:
        for role in ("public", "market"):
            records.append(
                _binance_connection_event(
                    event_kind="connect",
                    asset="GLOBAL",
                    received=BASE - timedelta(minutes=1),
                    capture="binance-historical-capture",
                    channel=role,
                    connection_id=f"binance-historical-{role}",
                    socket_role=role,
                )
            )
    if not omit_binance_connect:
        records.extend(
            (
                _binance_connection_event(
                    event_kind="connect",
                    asset="GLOBAL",
                    received=BASE,
                    capture=binance_capture,
                    channel="public",
                    connection_id="binance-public-1",
                    socket_role="public",
                ),
                _binance_connection_event(
                    event_kind="connect",
                    asset="GLOBAL",
                    received=BASE,
                    capture=binance_capture,
                    channel=binance_market_connect_role,
                    connection_id="binance-market-1",
                    socket_role=binance_market_connect_role,
                    connection_epoch=binance_market_connection_epoch,
                ),
            )
        )
    if not omit_hyperliquid_connect:
        records.append(
            _hyperliquid_connection_event(
                received=BASE,
                capture=hyperliquid_capture,
                socket_role=hyperliquid_connect_role,
            )
        )
    public_arrival = public_arrival_offset
    market_arrival_offset = 0
    if straddle_trade_gap:
        records.extend(
            _binance_frame(
                connector,
                asset="BTC",
                kind="trade",
                received=BASE - timedelta(seconds=1),
                connection_id="binance-market-1",
                arrival=1,
                source_sequence=998,
                capture=binance_capture,
            )
        )
        market_arrival_offset = 1
    for asset_number, asset in enumerate(("BTC", "ETH"), start=1):
        asset_index = asset_number - 1
        market_arrival = asset_number + market_arrival_offset
        for kind, offset in (("l2", 2.0),):
            public_arrival += 1
            received = BASE + timedelta(seconds=offset + asset_index * 3)
            frame_records = _binance_frame(
                connector,
                asset=asset,
                kind=kind,
                received=received,
                connection_id="binance-public-1",
                arrival=public_arrival,
                source_sequence=100 + asset_index * 100 + public_arrival,
                capture=binance_capture,
                empty_l2=(empty_binance_l2 and asset == "BTC" and kind == "l2"),
                zero_bbo_quantity=(
                    zero_binance_bbo_quantity and asset == "BTC" and kind == "l2"
                ),
            )
            if mutate_binance_bbo and asset == "BTC" and kind == "l2":
                frame_records = tuple(
                    (
                        ParsedRecord(
                            record.record_type,
                            record.asset,
                            {**record.row, "update_id": "BTCUSDT:mutated"},
                        )
                        if record.record_type == RecordType.BBO
                        else record
                    )
                    for record in frame_records
                )
            if (
                mutate_binance_depth_bbo
                and asset == "BTC"
                and kind == "l2"
            ):
                frame_records = tuple(
                    (
                        ParsedRecord(
                            record.record_type,
                            record.asset,
                            {**record.row, "update_id": "BTCUSDT:mutated-depth"},
                        )
                        if record.record_type == RecordType.BBO
                        else record
                    )
                    for record in frame_records
                )
            if add_orphan_binance_bbo and asset == "BTC" and kind == "l2":
                source = next(
                    record
                    for record in frame_records
                    if record.record_type == RecordType.BBO
                )
                records.append(
                    ParsedRecord(
                        source.record_type,
                        source.asset,
                        {
                            **source.row,
                            "received_time": received + timedelta(milliseconds=500),
                            "update_id": "BTCUSDT:orphan-normalized",
                        },
                    )
                )
            if omit_binance_depth_bbo and asset == "BTC" and kind == "l2":
                frame_records = tuple(
                    record
                    for record in frame_records
                    if record.record_type != RecordType.BBO
                )
            if omit_binance_l2_levels and asset == "BTC" and kind == "l2":
                frame_records = tuple(
                    record
                    for record in frame_records
                    if record.record_type != RecordType.L2_SNAPSHOT
                )
            if add_orphan_binance_l2_level and asset == "BTC" and kind == "l2":
                source_level = next(
                    record
                    for record in frame_records
                    if record.record_type == RecordType.L2_SNAPSHOT
                )
                records.append(
                    ParsedRecord(
                        source_level.record_type,
                        source_level.asset,
                        {
                            **source_level.row,
                            "snapshot_id": "orphan-normalized-snapshot",
                        },
                    )
                )
            if (
                kind == "l2"
                and not omit_binance_resync
                and not (empty_binance_l2 and asset == "BTC")
            ):
                state = next(
                    record
                    for record in frame_records
                    if record.record_type.value == "l2_book_state"
                )
                event_kinds = (
                    ("resync_complete",)
                    if orphan_binance_resync_complete
                    else ("resync_start", "resync_complete")
                )
                for event_kind in event_kinds:
                    records.append(
                        _binance_connection_event(
                            event_kind=event_kind,
                            asset=asset,
                            received=received
                            + timedelta(seconds=binance_resync_time_shift_seconds),
                            capture=binance_capture,
                            channel=f"{asset.lower()}usdt@depth20@100ms",
                            book_epoch_id=str(state.row["book_epoch_id"]),
                            resync_snapshot_id=(
                                str(state.row["snapshot_id"])
                                if event_kind == "resync_complete"
                                else None
                            ),
                        )
                    )
            records.extend(frame_records)
        trade_records = _binance_frame(
            connector,
            asset=asset,
            kind="trade",
            received=BASE + timedelta(seconds=7),
            connection_id="binance-market-1",
            connection_epoch=binance_market_connection_epoch,
            arrival=market_arrival,
            source_sequence=1_000 + asset_index,
            capture=binance_capture,
        )
        if mutate_binance_trade_received and asset == "BTC":
            trade_records = tuple(
                (
                    ParsedRecord(
                        record.record_type,
                        record.asset,
                        {
                            **record.row,
                            "received_time": (
                                record.row["received_time"]
                                + timedelta(milliseconds=1)
                            ),
                        },
                    )
                    if record.record_type == RecordType.TRADE
                    else record
                )
                for record in trade_records
            )
        if mutate_binance_trade_quote and asset == "BTC":
            trade_records = tuple(
                (
                    ParsedRecord(
                        record.record_type,
                        record.asset,
                        {**record.row, "quote_quantity": Decimal("999")},
                    )
                    if record.record_type == RecordType.TRADE
                    else record
                )
                for record in trade_records
            )
        if asset == "BTC" and (
            forge_binance_maker_type or forge_binance_stream_type is not None
        ):
            forged: list[ParsedRecord] = []
            for record in trade_records:
                if record.record_type != RecordType.WIRE_MESSAGE:
                    forged.append(record)
                    continue
                payload = json.loads(str(record.row["raw_message"]))
                data = payload["data"]
                assert isinstance(data, dict)
                if forge_binance_maker_type:
                    data["m"] = "false"
                if forge_binance_stream_type is not None:
                    data["st"] = forge_binance_stream_type
                raw_message = json.dumps(payload, separators=(",", ":"))
                forged.append(
                    ParsedRecord(
                        record.record_type,
                        record.asset,
                        {
                            **record.row,
                            "raw_message": raw_message,
                            "payload_sha256": hashlib.sha256(
                                raw_message.encode()
                            ).hexdigest(),
                        },
                    )
                )
            trade_records = tuple(forged)
        records.extend(
            record
            for record in trade_records
            if not (
                omit_binance_raw_trade
                and record.record_type.value == "wire_message"
            )
        )
        for kind, offset in (("bbo", 1.2), ("l2", 2.2), ("trade", 7.0)):
            if mixed_hyperliquid_trade_batch and kind == "trade":
                continue
            hyperliquid_arrival = {
                ("BTC", "bbo"): 1,
                ("BTC", "l2"): 2,
                ("ETH", "bbo"): 3,
                ("ETH", "l2"): 4,
                ("BTC", "trade"): 5,
                ("ETH", "trade"): 6,
            }[(asset, kind)]
            records.extend(
                _hyperliquid_frame(
                    asset=asset,
                    kind=kind,
                    received=BASE
                    + timedelta(
                        seconds=(
                            (28.0 + asset_index)
                            if delayed_hyperliquid_trade and kind == "trade"
                            else (
                                offset
                                if kind == "trade"
                                else offset + asset_index * 3
                            )
                        )
                    ),
                    arrival=hyperliquid_arrival,
                    capture=hyperliquid_capture,
                )
            )

    if add_hyperliquid_rest_bootstrap:
        for asset_index, asset in enumerate(("BTC", "ETH"), start=1):
            received = BASE + timedelta(seconds=8, milliseconds=asset_index)
            source_time = received - timedelta(milliseconds=100)
            payload = {
                "coin": asset,
                "time": int(source_time.timestamp() * 1_000),
                "levels": [
                    [{"px": "60000", "sz": "1", "n": 1}],
                    [{"px": "60001", "sz": "1", "n": 1}],
                ],
            }
            envelope = historical_envelope(
                received,
                connection_id="hyperliquid-public-1",
                connection_epoch=1,
                arrival_sequence=100 + asset_index,
            )
            rest_l2 = parse_l2_snapshot(payload, envelope)
            if forge_hyperliquid_rest_l2_identity and asset == "BTC":
                rest_l2 = tuple(
                    ParsedRecord(
                        record.record_type,
                        record.asset,
                        {
                            **record.row,
                            "snapshot_id": (
                                "rest:hyperliquid-public-1:999:999:"
                                f"{int(source_time.timestamp() * 1_000)}"
                            ),
                            "book_epoch_id": "hyperliquid-public-1:999",
                        },
                    )
                    for record in rest_l2
                )
            records.extend(rest_l2)
            rest_bbo = parse_bbo_from_l2(payload, envelope)
            if corrupt_hyperliquid_rest_bbo and asset == "BTC":
                rest_bbo = tuple(
                    ParsedRecord(
                        record.record_type,
                        record.asset,
                        {**record.row, "update_id": "rest:malformed"},
                    )
                    for record in rest_bbo
                )
            if forge_hyperliquid_rest_bbo_identity and asset == "BTC":
                rest_bbo = tuple(
                    ParsedRecord(
                        record.record_type,
                        record.asset,
                        {
                            **record.row,
                            "update_id": (
                                str(record.row["update_id"]).rsplit(":", 2)[0]
                                + ":999:999"
                            ),
                        },
                    )
                    for record in rest_bbo
                )
            if mutate_hyperliquid_rest_bbo_value and asset == "BTC":
                rest_bbo = tuple(
                    ParsedRecord(
                        record.record_type,
                        record.asset,
                        {**record.row, "bid_price": Decimal("1")},
                    )
                    for record in rest_bbo
                )
            records.extend(rest_bbo)

    if mixed_hyperliquid_trade_batch:
        received = BASE + timedelta(seconds=7)
        milliseconds = int(received.timestamp() * 1_000)
        payload = {
            "channel": "trades",
            "data": [
                {
                    "coin": asset,
                    "side": "B",
                    "px": "60000",
                    "sz": "0.01",
                    "time": milliseconds,
                    "tid": milliseconds + asset_index,
                }
                for asset_index, asset in enumerate(("BTC", "ETH"))
            ],
        }
        records.extend(
            parse_websocket_message(
                WireEnvelope(
                    json.dumps(payload, separators=(",", ":")),
                    received,
                    "hyperliquid-public-1",
                    1,
                    5,
                    hyperliquid_capture,
                )
            ).records
        )

    if extra_trade_at is not None:
        for asset_number, asset in enumerate(("BTC", "ETH"), start=1):
            records.extend(
                _binance_frame(
                    connector,
                    asset=asset,
                    kind="trade",
                    received=extra_trade_at,
                    connection_id="binance-market-1",
                    connection_epoch=binance_market_connection_epoch,
                    arrival=2 + asset_number,
                    source_sequence=1_000 + asset_number,
                    capture=binance_capture,
                )
            )
            records.extend(
                _hyperliquid_frame(
                    asset=asset,
                    kind="trade",
                    received=extra_trade_at,
                    arrival=6 + asset_number,
                    capture=hyperliquid_capture,
                )
            )

    if extra_market_refresh_at is not None:
        for asset_index, asset in enumerate(("BTC", "ETH")):
            book_at = extra_market_refresh_at + timedelta(seconds=asset_index * 2)
            records.extend(
                _binance_frame(
                    connector,
                    asset=asset,
                    kind="l2",
                    received=book_at,
                    connection_id="binance-public-1",
                    arrival=3 + asset_index,
                    source_sequence=20_003 + asset_index,
                    capture=binance_capture,
                )
            )
            for kind, received, arrival in (
                ("bbo", book_at + timedelta(milliseconds=200), 7 + asset_index * 2),
                (
                    "l2",
                    book_at + timedelta(seconds=1, milliseconds=200),
                    8 + asset_index * 2,
                ),
            ):
                records.extend(
                    _hyperliquid_frame(
                        asset=asset,
                        kind=kind,
                        received=received,
                        arrival=arrival,
                        capture=hyperliquid_capture,
                    )
                )
            trade_at = extra_market_refresh_at + timedelta(seconds=5)
            records.extend(
                _binance_frame(
                    connector,
                    asset=asset,
                    kind="trade",
                    received=trade_at,
                    connection_id="binance-market-1",
                    connection_epoch=binance_market_connection_epoch,
                    arrival=3 + asset_index,
                    source_sequence=1_001 + asset_index,
                    capture=binance_capture,
                )
            )
            records.extend(
                _hyperliquid_frame(
                    asset=asset,
                    kind="trade",
                    received=trade_at + timedelta(milliseconds=200),
                    arrival=11 + asset_index,
                    capture=hyperliquid_capture,
                )
            )

    if periodic_refresh_seconds:
        public_next = 3
        market_next = 3
        hyperliquid_next = 7
        for refresh_number, seconds in enumerate(periodic_refresh_seconds, start=1):
            for asset_index, asset in enumerate(("BTC", "ETH")):
                book_at = BASE + timedelta(
                    seconds=seconds + asset_index * 0.1
                )
                trade_at = book_at + timedelta(milliseconds=40)
                records.extend(
                    _binance_frame(
                        connector,
                        asset=asset,
                        kind="l2",
                        received=book_at + timedelta(milliseconds=20),
                        connection_id="binance-public-1",
                        arrival=public_next,
                        source_sequence=30_000 + public_next,
                        capture=binance_capture,
                    )
                )
                public_next += 1
                for kind, received in (
                    ("bbo", book_at),
                    ("l2", book_at + timedelta(milliseconds=20)),
                ):
                    records.extend(
                        _hyperliquid_frame(
                            asset=asset,
                            kind=kind,
                            received=received + timedelta(milliseconds=5),
                            arrival=hyperliquid_next,
                            capture=hyperliquid_capture,
                        )
                    )
                    hyperliquid_next += 1
                records.extend(
                    _binance_frame(
                        connector,
                        asset=asset,
                        kind="trade",
                        received=trade_at,
                        connection_id="binance-market-1",
                        connection_epoch=binance_market_connection_epoch,
                        arrival=market_next,
                        source_sequence=1_000 + asset_index + refresh_number,
                        capture=binance_capture,
                    )
                )
                market_next += 1
                records.extend(
                    _hyperliquid_frame(
                        asset=asset,
                        kind="trade",
                        received=trade_at + timedelta(milliseconds=5),
                        arrival=hyperliquid_next,
                        capture=hyperliquid_capture,
                    )
                )
                hyperliquid_next += 1
    if add_second_complete_hyperliquid_capture:
        second_hl_capture = "hyperliquid-capture-2"
        second_hl_connection = "hyperliquid-public-2"
        records.append(
            _hyperliquid_connection_event(
                received=BASE + timedelta(seconds=8),
                capture=second_hl_capture,
                connection_id=second_hl_connection,
            )
        )
        for asset_index, asset in enumerate(("BTC", "ETH")):
            for kind, offset in (("bbo", 9.0), ("l2", 10.0), ("trade", 13.0)):
                arrival = {
                    ("BTC", "bbo"): 1,
                    ("BTC", "l2"): 2,
                    ("ETH", "bbo"): 3,
                    ("ETH", "l2"): 4,
                    ("BTC", "trade"): 5,
                    ("ETH", "trade"): 6,
                }[(asset, kind)]
                records.extend(
                    _hyperliquid_frame(
                        asset=asset,
                        kind=kind,
                        received=BASE + timedelta(
                            seconds=offset + asset_index * 2
                        ),
                        arrival=arrival,
                        capture=second_hl_capture,
                        connection_id=second_hl_connection,
                    )
                )

    if add_orphan_required_wire:
        orphan_at = BASE + timedelta(seconds=8)
        orphan = _binance_frame(
            connector,
            asset="BTC",
            kind="trade",
            received=orphan_at,
            connection_id="binance-market-1",
            arrival=3,
            source_sequence=1_002,
            capture=binance_capture,
        )[0]
        payload = json.loads(str(orphan.row["raw_message"]))
        data = payload["data"]
        assert isinstance(data, dict)
        data["st"] = None
        raw_message = json.dumps(payload, separators=(",", ":"))
        records.append(
            ParsedRecord(
                RecordType.WIRE_MESSAGE,
                "GLOBAL",
                {
                    **orphan.row,
                    "raw_message": raw_message,
                    "payload_sha256": hashlib.sha256(raw_message.encode()).hexdigest(),
                },
            )
        )

    if add_connect_only_capture or add_half_paired_capture:
        second_capture = "binance-connect-only-capture"
        roles = ("public",) if add_half_paired_capture else ("public", "market")
        for role in roles:
            records.append(
                _binance_connection_event(
                    event_kind="connect",
                    asset="GLOBAL",
                    received=BASE + timedelta(seconds=25),
                    capture=second_capture,
                    channel=role,
                    connection_id=f"binance-connect-only-{role}",
                    socket_role=role,
                )
            )

    if add_unbound_connect:
        record = _binance_connection_event(
            event_kind="connect",
            asset="GLOBAL",
            received=BASE + timedelta(seconds=25),
            capture="binance-unbound-capture",
            channel="public",
            connection_id="binance-unbound-public",
            socket_role="public",
        )
        records.append(
            ParsedRecord(
                record.record_type,
                record.asset,
                {**record.row, "connection_epoch": None},
            )
        )

    if any(value is not None for value in (bound_gap_at, disconnect_at)):
        for event_kind, at, reason in (
            ("gap", bound_gap_at, None),
            ("disconnect", disconnect_at, None),
        ):
            if at is not None:
                records.append(
                    _binance_connection_event(
                        event_kind=event_kind,
                        asset="GLOBAL",
                        received=at,
                        capture=binance_capture,
                        channel="public",
                        reason=reason,
                    )
                )
    if clean_disconnect_at is not None:
        for connection_id, socket_role in (
            ("binance-public-1", "public"),
            ("binance-market-1", "market"),
        ):
            if f"binance_{socket_role}" not in clean_disconnect_roles:
                continue
            records.append(
                _binance_connection_event(
                    event_kind="disconnect",
                    asset="GLOBAL",
                    received=clean_disconnect_at,
                    capture=binance_capture,
                    channel=socket_role,
                    connection_id=(
                        "unknown-clean-connection"
                        if clean_disconnect_unknown_identity
                        and socket_role == "public"
                        else connection_id
                    ),
                    socket_role=socket_role,
                    connection_epoch=(
                        1
                        if socket_role == "public"
                        else binance_market_connection_epoch
                    ),
                    reason="collector stop requested or bounded run completed",
                )
            )
        if "hyperliquid_public" in clean_disconnect_roles:
            records.append(
                ParsedRecord(
                    RecordType.CONNECTION_EVENT,
                    "GLOBAL",
                    {
                        **_hyperliquid_connection_event(
                            received=clean_disconnect_at,
                            capture=hyperliquid_capture,
                            event_kind="disconnect",
                        ).row,
                        "reason": "collector stop requested or bounded run completed",
                    },
                )
            )
    if add_unbound_gap:
        event = _binance_connection_event(
            event_kind="gap",
            asset="GLOBAL",
            received=BASE + timedelta(seconds=25),
            capture=binance_capture,
            channel="public",
            connection_id="unknown-gap-connection",
        )
        records.append(
            ParsedRecord(
                event.record_type,
                event.asset,
                {
                    **event.row,
                    "connection_epoch": None,
                    "capture_epoch_id": None,
                },
            )
        )
    if orphan_resync_event_kind is not None:
        records.append(
            _binance_connection_event(
                event_kind=orphan_resync_event_kind,
                asset="BTC",
                received=BASE + timedelta(seconds=20),
                capture="orphan-resync-capture",
                channel="btcusdt@depth20@100ms",
                connection_id="orphan-resync-public",
                socket_role="public",
                book_epoch_id="orphan-resync-public:1",
                resync_snapshot_id=(
                    "orphan-snapshot"
                    if orphan_resync_event_kind == "resync_complete"
                    else None
                ),
            )
        )
    if temporal_resync_snapshot is not None:
        l2_states = sorted(
            (
                record
                for record in records
                if record.record_type == RecordType.L2_BOOK_STATE
                and record.row.get("venue") == "binance_usdm"
                and record.asset == "BTC"
            ),
            key=lambda record: record.row["received_time"],
        )
        target = (
            l2_states[0]
            if temporal_resync_snapshot == "stale"
            else l2_states[-1]
        )
        completion_at = BASE + timedelta(seconds=12)
        for event_kind in ("resync_start", "resync_complete"):
            records.append(
                _binance_connection_event(
                    event_kind=event_kind,
                    asset="BTC",
                    received=completion_at,
                    capture=binance_capture,
                    channel="btcusdt@depth20@100ms",
                    book_epoch_id=str(target.row["book_epoch_id"]),
                    resync_snapshot_id=(
                        str(target.row["snapshot_id"])
                        if event_kind == "resync_complete"
                        else None
                    ),
                )
            )

    prewindow_rejections = max(
        prewindow_clock_rejection_count,
        int(prewindow_clock_invalid),
    )
    if prewindow_rejections:
        samples = [("clock-pre-valid", BASE + timedelta(milliseconds=50), 20)]
        samples.extend(
            (
                f"clock-pre-invalid-{index}",
                BASE + timedelta(milliseconds=100 * index),
                102,
            )
            for index in range(1, prewindow_rejections + 1)
        )
        for observation_id, response, delay_ms in samples:
            sent = response - timedelta(milliseconds=delay_ms)
            records.append(
                clock_record(
                    measure_clock(
                        "binance_usdm",
                        request_sent_time=sent,
                        response_received_time=response,
                        server_time=sent + timedelta(milliseconds=delay_ms / 2),
                    ),
                    observation_id,
                    connection_id="binance-public-1",
                    connection_epoch=1,
                    capture_epoch_id=binance_capture,
                )
            )
    if prewindow_unbound_clock_invalid:
        response = BASE - timedelta(seconds=1)
        records.append(
            clock_record(
                measure_clock(
                    "binance_usdm",
                    request_sent_time=response - timedelta(milliseconds=20),
                    response_received_time=response,
                    server_time=response - timedelta(milliseconds=10),
                ),
                "clock-pre-unbound-invalid",
            )
        )
    for index, seconds in enumerate(clock_sample_seconds, start=1):
        response = BASE + timedelta(seconds=seconds)
        sent = response - timedelta(milliseconds=20)
        measurement = measure_clock(
            "binance_usdm",
            request_sent_time=sent,
            response_received_time=response,
            server_time=sent + timedelta(milliseconds=10),
        )
        records.append(
            clock_record(
                measurement,
                f"clock-{index}",
                connection_id="binance-public-1",
                connection_epoch=1,
                capture_epoch_id=binance_capture,
            )
        )
    rejected_clock_times = (
        (() if invalid_clock_at is None else (invalid_clock_at,))
        + invalid_clock_times
    )
    for rejected_index, rejected_at in enumerate(rejected_clock_times, start=1):
        records.append(
            clock_record(
                measure_clock(
                    "binance_usdm",
                    request_sent_time=rejected_at - timedelta(milliseconds=102),
                    response_received_time=rejected_at,
                    server_time=rejected_at - timedelta(milliseconds=51),
                ),
                f"clock-invalid-{rejected_index}",
                connection_id="binance-public-1",
                connection_epoch=1,
                capture_epoch_id=binance_capture,
                max_uncertainty_ms=Decimal("50"),
            )
        )
    if clock_failure_at is not None:
        records.append(
            _binance_connection_event(
                event_kind="gap",
                asset="GLOBAL",
                received=clock_failure_at,
                capture=binance_capture,
                channel="clock_sync",
                connection_id="binance-public-1:clock",
                socket_role="clock",
            )
        )
    if prewindow_clock_failure:
        records.extend(
            (
                clock_record(
                    measure_clock(
                        "binance_usdm",
                        request_sent_time=BASE + timedelta(milliseconds=30),
                        response_received_time=BASE + timedelta(milliseconds=50),
                        server_time=BASE + timedelta(milliseconds=40),
                    ),
                    "clock-pre-gap-valid",
                    connection_id="binance-public-1",
                    connection_epoch=1,
                    capture_epoch_id=binance_capture,
                ),
                _binance_connection_event(
                    event_kind="gap",
                    asset="GLOBAL",
                    received=BASE + timedelta(milliseconds=100),
                    capture=binance_capture,
                    channel="clock_sync",
                    connection_id="binance-public-1:clock",
                    socket_role="clock",
                ),
            )
        )
    if prewindow_disconnect:
        records.append(
            _binance_connection_event(
                event_kind="disconnect",
                asset="GLOBAL",
                received=BASE - timedelta(seconds=1),
                capture=binance_capture,
                channel="public",
            )
        )
    if prewindow_conflicting_capture_event:
        records.append(
            _binance_connection_event(
                event_kind="gap",
                asset="GLOBAL",
                received=BASE - timedelta(minutes=1),
                capture="wrong-capture-for-active-identity",
                channel="public",
                connection_id="binance-public-1",
                socket_role="public",
            )
        )
    if loose_clock_at is not None:
        records.append(
            clock_record(
                measure_clock(
                    "binance_usdm",
                    request_sent_time=loose_clock_at - timedelta(milliseconds=102),
                    response_received_time=loose_clock_at,
                    server_time=loose_clock_at - timedelta(milliseconds=51),
                ),
                "clock-loose-policy",
                connection_id="binance-public-1",
                connection_epoch=1,
                capture_epoch_id=binance_capture,
                max_uncertainty_ms=Decimal("100"),
            )
        )
    if wrong_clock_identity_at is not None:
        records.append(
            clock_record(
                measure_clock(
                    "binance_usdm",
                    request_sent_time=wrong_clock_identity_at
                    - timedelta(milliseconds=20),
                    response_received_time=wrong_clock_identity_at,
                    server_time=wrong_clock_identity_at
                    - timedelta(milliseconds=10),
                ),
                "clock-wrong-wire-identity",
                connection_id="binance-public-1",
                connection_epoch=2,
                capture_epoch_id=binance_capture,
            )
        )
    if offset_jump_at is not None:
        records.append(
            clock_record(
                measure_clock(
                    "binance_usdm",
                    request_sent_time=offset_jump_at - timedelta(milliseconds=20),
                    response_received_time=offset_jump_at,
                    server_time=offset_jump_at - timedelta(milliseconds=110),
                ),
                "clock-offset-jump",
                connection_id="binance-public-1",
                connection_epoch=1,
                capture_epoch_id=binance_capture,
            )
        )
    if add_invalid_only_capture:
        second_capture = "binance-capture-invalid-only"
        records.extend(
            (
                _binance_connection_event(
                    event_kind="connect",
                    asset="GLOBAL",
                    received=BASE + timedelta(seconds=7.5),
                    capture=second_capture,
                    channel="public",
                    connection_id="binance-public-invalid-only",
                    socket_role="public",
                ),
                _binance_connection_event(
                    event_kind="connect",
                    asset="GLOBAL",
                    received=BASE + timedelta(seconds=7.5),
                    capture=second_capture,
                    channel="market",
                    connection_id="binance-market-invalid-only",
                    socket_role="market",
                ),
            )
        )
        second_public_arrival = 0
        for asset_index, asset in enumerate(("BTC", "ETH")):
            for kind, offset in (("bbo", 8.0), ("l2", 9.0)):
                second_public_arrival += 1
                received = BASE + timedelta(
                    seconds=offset + asset_index * 2
                )
                frame_records = _binance_frame(
                    connector,
                    asset=asset,
                    kind=kind,
                    received=received,
                    connection_id="binance-public-invalid-only",
                    arrival=second_public_arrival,
                    source_sequence=8_000 + second_public_arrival,
                    capture=second_capture,
                )
                if kind == "l2":
                    state = next(
                        record
                        for record in frame_records
                        if record.record_type == RecordType.L2_BOOK_STATE
                    )
                    for event_kind in ("resync_start", "resync_complete"):
                        records.append(
                            _binance_connection_event(
                                event_kind=event_kind,
                                asset=asset,
                                received=received,
                                capture=second_capture,
                                channel=f"{asset.lower()}usdt@depth20@100ms",
                                connection_id="binance-public-invalid-only",
                                book_epoch_id=str(state.row["book_epoch_id"]),
                                resync_snapshot_id=(
                                    str(state.row["snapshot_id"])
                                    if event_kind == "resync_complete"
                                    else None
                                ),
                            )
                        )
                records.extend(frame_records)
            records.extend(
                _binance_frame(
                    connector,
                    asset=asset,
                    kind="trade",
                    received=BASE + timedelta(seconds=12),
                    connection_id="binance-market-invalid-only",
                    arrival=asset_index + 1,
                    source_sequence=9_000 + asset_index,
                    capture=second_capture,
                )
            )
        response = BASE + timedelta(seconds=12, milliseconds=500)
        records.append(
            clock_record(
                measure_clock(
                    "binance_usdm",
                    request_sent_time=response - timedelta(milliseconds=102),
                    response_received_time=response,
                    server_time=response - timedelta(milliseconds=51),
                ),
                "clock-invalid-only-capture",
                connection_id="binance-public-invalid-only",
                connection_epoch=1,
                capture_epoch_id=second_capture,
            )
        )

    for venue, override, expected_kind in (
        (
            "binance_usdm",
            binance_message_asset_override,
            binance_message_asset_kind,
        ),
        (
            "hyperliquid",
            hyperliquid_message_asset_override,
            hyperliquid_message_asset_kind,
        ),
    ):
        if override is Ellipsis:
            continue
        for index, record in enumerate(records):
            if (
                record.record_type != RecordType.WIRE_MESSAGE
                or record.row.get("venue") != venue
                or record.row.get("message_asset") != "BTC"
                or _wire_kind(
                    record.row.get("channel"),
                    record.row.get("raw_message"),
                )
                != expected_kind
            ):
                continue
            records[index] = ParsedRecord(
                record.record_type,
                record.asset,
                {**record.row, "message_asset": override},
            )
            break
    for venue, override, expected_kind in (
        ("binance_usdm", binance_channel_override, binance_channel_kind),
        (
            "hyperliquid",
            hyperliquid_channel_override,
            hyperliquid_channel_kind,
        ),
    ):
        if override is Ellipsis:
            continue
        for index, record in enumerate(records):
            if (
                record.record_type != RecordType.WIRE_MESSAGE
                or record.row.get("venue") != venue
                or record.row.get("message_asset") != "BTC"
                or _wire_kind(
                    record.row.get("channel"),
                    record.row.get("raw_message"),
                )
                != expected_kind
            ):
                continue
            records[index] = ParsedRecord(
                record.record_type,
                record.asset,
                {**record.row, "channel": override},
            )
            break

    sink = BatchingLakeSink(root, batch_size=10_000, persistent_dedup=False)
    try:
        assert sink.add_many(records) == len(records)
        sink.flush()
    finally:
        sink.close()


def test_real_lake_audit_passes_technical_gate_but_keeps_phase_blocked(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(lake)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "PASS", (
        payload["failure_reasons"],
        payload["validation"],
    )
    assert payload["phase_10_status"] == PHASE_10_STATUS
    trades = payload["binance_trades"]
    assert isinstance(trades, dict)
    assert trades["normalized_total"] == 2
    assert trades["raw_agg_trade_total"] == 2
    assert trades["normalized_with_raw_lineage_total"] == 2
    clock = payload["clock_sync"]
    assert isinstance(clock, dict)
    assert clock["coverage_continuous"] is True
    overlap = payload["strict_phase_10_overlap"]
    assert isinstance(overlap, dict)
    assert float(str(overlap["duration_seconds"])) > 0
    assert payload["failure_reasons"] == []


def test_real_lake_audit_accepts_explicit_hyperliquid_rest_bootstrap_provenance(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(
        lake,
        add_hyperliquid_rest_bootstrap=True,
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "PASS", payload["failure_reasons"]
    lineage = payload["connection_lineage"]
    assert isinstance(lineage, dict)
    assert lineage["hyperliquid"]["normalized_market_lineage_rejections"] == 0
    level_lineage = payload["normalized_l2_level_lineage"]
    assert isinstance(level_lineage, dict)
    assert level_lineage["orphan_level_total"] == 0
    raw_lineage = payload["required_wire_lineage"]
    assert isinstance(raw_lineage, dict)
    assert raw_lineage["orphan_required_wire_total"] == 0


def test_real_lake_audit_fails_without_raw_binance_agg_trade(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(lake, omit_binance_raw_trade=True)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "FAIL"
    assert payload["phase_10_status"] == PHASE_10_STATUS
    trades = payload["binance_trades"]
    assert isinstance(trades, dict)
    assert trades["normalized_total"] == 2
    assert trades["raw_agg_trade_total"] == 0
    assert trades["normalized_with_raw_lineage_total"] == 0
    reasons = payload["failure_reasons"]
    assert isinstance(reasons, list)
    assert any(
        str(reason).startswith("binance_raw_agg_trade_missing:")
        for reason in reasons
    )


def test_real_lake_audit_requires_binance_v2_resync_complete(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(lake, omit_binance_resync=True)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "FAIL"
    resync = payload["binance_l2_resync"]
    assert isinstance(resync, dict)
    assert resync["missing_count"] == 2
    assert payload["strict_phase_10_overlap"] == {
        "duration_seconds": 0.0,
        "interval_count": 0,
        "by_asset": {
            "BTC": {"interval_count": 0, "duration_seconds": 0.0},
            "ETH": {"interval_count": 0, "duration_seconds": 0.0},
        },
        "intervals": [],
    }
    reasons = payload["failure_reasons"]
    assert isinstance(reasons, list)
    assert {
        "binance_l2_resync_missing:BTC:binance-capture-1",
        "binance_l2_resync_missing:ETH:binance-capture-1",
    }.issubset(reasons)


def test_real_lake_audit_keeps_isolated_high_rtt_probe_non_covering_without_revocation(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    rejected_at = BASE + timedelta(seconds=5.5)
    _write_continuity_lake(lake, invalid_clock_at=rejected_at)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "PASS", payload["failure_reasons"]
    clock = payload["clock_sync"]
    assert isinstance(clock, dict)
    assert clock["invalid_v2_samples"] == 1
    assert clock["rejected_probe_samples"] == 1
    assert clock["hard_invalid_v2_samples"] == 0
    assert clock["in_window_invalid_events"] == 1
    assert clock["in_window_rejected_probe_events"] == 1
    assert clock["in_window_hard_invalid_events"] == 0
    assert clock["coverage_continuous"] is True
    assert "clock_sync_in_window_invalid_sample" not in payload["failure_reasons"]
    assert any(
        datetime.fromisoformat(str(item["start"]).replace("Z", "+00:00"))
        < rejected_at
        < datetime.fromisoformat(str(item["end"]).replace("Z", "+00:00"))
        for item in clock["intervals"]
        if isinstance(item, dict)
    )


def test_real_lake_audit_rejects_consecutive_high_rtt_probes_before_coverage_expires(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(
        lake,
        clock_sample_seconds=(0.5,),
        invalid_clock_times=(
            BASE + timedelta(seconds=5.5),
            BASE + timedelta(seconds=10.5),
        ),
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=12),
    )

    clock = payload["clock_sync"]
    assert isinstance(clock, dict)
    assert payload["technical_capture_gate"] == "FAIL"
    assert clock["rejected_probe_samples"] == 2
    assert clock["hard_invalid_v2_samples"] == 0
    assert clock["sample_spacing_violations"] == 0
    assert clock["consecutive_rejection_violations"] == 1
    assert clock["max_consecutive_rejected_probes"] == 2
    assert clock["strict_max_consecutive_rejected_probes"] == 1
    assert clock["coverage_continuous"] is False
    assert "clock_sync_consecutive_rejected_probes" in payload["failure_reasons"]


def test_real_lake_audit_cuts_coverage_until_valid_recovery_after_second_rejection(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    second_rejection = BASE + timedelta(seconds=7.5)
    recovery = BASE + timedelta(seconds=9.5)
    _write_continuity_lake(
        lake,
        clock_sample_seconds=(0.5, 9.5, 19.5, 29.5),
        invalid_clock_times=(
            BASE + timedelta(seconds=5.5),
            second_rejection,
        ),
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    clock = payload["clock_sync"]
    assert isinstance(clock, dict)
    assert payload["technical_capture_gate"] == "FAIL"
    assert clock["sample_spacing_violations"] == 0
    assert clock[
        "consecutive_rejection_violation_capture_generations"
    ] == ["binance-capture-1"]
    assert clock["consecutive_rejection_outages"] == [
        {
            "capture_epoch_id": "binance-capture-1",
            "start": second_rejection.isoformat().replace("+00:00", "Z"),
            "end": recovery.isoformat().replace("+00:00", "Z"),
            "duration_seconds": 2.0,
        }
    ]
    assert clock["coverage_continuous"] is False
    assert "clock_sync_consecutive_rejected_probes" in payload["failure_reasons"]


def test_real_lake_audit_rejects_high_rtt_run_after_last_valid_interval_expires(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(
        lake,
        clock_sample_seconds=(0.5,),
        invalid_clock_times=tuple(
            BASE + timedelta(seconds=seconds)
            for seconds in (5.5, 10.5, 15.5, 20.5)
        ),
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    clock = payload["clock_sync"]
    assert isinstance(clock, dict)
    assert payload["technical_capture_gate"] == "FAIL"
    assert clock["rejected_probe_samples"] == 4
    assert clock["hard_invalid_v2_samples"] == 0
    assert clock["coverage_continuous"] is False
    assert "clock_sync_not_continuous" in payload["failure_reasons"]


def test_real_lake_audit_clock_failure_event_cuts_only_clock_coverage(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    failure_at = BASE + timedelta(seconds=12.6)
    _write_continuity_lake(
        lake,
        clock_failure_at=failure_at,
        extra_trade_at=BASE + timedelta(seconds=21),
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "FAIL"
    clock = payload["clock_sync"]
    assert isinstance(clock, dict)
    assert clock["failure_events"] == 1
    assert clock["invalid_v2_samples"] == 0
    assert clock["coverage_continuous"] is False
    intervals = clock["intervals"]
    assert isinstance(intervals, list)
    assert all(
        not (
            datetime.fromisoformat(str(item["start"]).replace("Z", "+00:00"))
            < failure_at
            < datetime.fromisoformat(str(item["end"]).replace("Z", "+00:00"))
        )
        for item in intervals
        if isinstance(item, dict)
    )
    overlap = payload["strict_phase_10_overlap"]
    assert isinstance(overlap, dict)
    assert overlap["duration_seconds"] == 0.0
    assert "clock_sync_in_window_failure_event" in payload["failure_reasons"]


def test_real_lake_audit_rejects_looser_self_declared_clock_policy(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    loose_at = BASE + timedelta(seconds=12.6)
    _write_continuity_lake(lake, loose_clock_at=loose_at)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "FAIL"
    clock = payload["clock_sync"]
    assert isinstance(clock, dict)
    assert clock["strict_policy_rejections"] == 1
    assert clock["strict_max_sampling_interval_ms"] == 10_000
    assert clock["strict_max_age_ms"] == 15_000
    assert clock["strict_max_uncertainty_ms"] == 50.0
    assert clock["coverage_continuous"] is False
    intervals = clock["intervals"]
    assert isinstance(intervals, list)
    assert all(
        not (
            datetime.fromisoformat(str(item["start"]).replace("Z", "+00:00"))
            < loose_at
            < datetime.fromisoformat(str(item["end"]).replace("Z", "+00:00"))
        )
        for item in intervals
        if isinstance(item, dict)
    )


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    (
        (
            {"orphan_binance_resync_complete": True},
            "binance_l2_resync_missing:BTC:binance-capture-1",
        ),
        (
            {"binance_resync_time_shift_seconds": -1.0},
            "binance_l2_resync_missing:BTC:binance-capture-1",
        ),
        (
            {"wrong_clock_identity_at": BASE + timedelta(seconds=12.6)},
            "clock_sync_invalid_event_unbound",
        ),
        (
            {"add_invalid_only_capture": True},
            "clock_sync_missing_valid_for_market_capture:binance-capture-invalid-only",
        ),
        (
            {"mutate_binance_trade_received": True},
            "binance_market_raw_lineage_rejected",
        ),
        (
            {"mutate_binance_trade_quote": True},
            "binance_market_raw_lineage_rejected",
        ),
        (
            {"mutate_binance_bbo": True},
            "binance_market_raw_lineage_rejected",
        ),
        (
            {"mutate_binance_depth_bbo": True},
            "binance_market_raw_lineage_rejected",
        ),
        (
            {"omit_binance_depth_bbo": True},
            "required_raw_wire_without_exact_normalization",
        ),
        (
            {
                "add_hyperliquid_rest_bootstrap": True,
                "corrupt_hyperliquid_rest_bbo": True,
            },
            "hyperliquid_market_raw_lineage_rejected",
        ),
        (
            {
                "add_hyperliquid_rest_bootstrap": True,
                "forge_hyperliquid_rest_bbo_identity": True,
            },
            "hyperliquid_market_raw_lineage_rejected",
        ),
        (
            {
                "add_hyperliquid_rest_bootstrap": True,
                "forge_hyperliquid_rest_l2_identity": True,
            },
            "hyperliquid_market_raw_lineage_rejected",
        ),
        (
            {
                "add_hyperliquid_rest_bootstrap": True,
                "mutate_hyperliquid_rest_bbo_value": True,
            },
            "hyperliquid_market_raw_lineage_rejected",
        ),
        (
            {"omit_binance_l2_levels": True},
            "binance_market_raw_lineage_rejected",
        ),
        (
            {"empty_binance_l2": True},
            "required_raw_wire_without_exact_normalization",
        ),
        (
            {"zero_binance_bbo_quantity": True},
            "binance_market_raw_lineage_rejected",
        ),
        (
            {"forge_binance_maker_type": True},
            "binance_market_raw_lineage_rejected",
        ),
        (
            {"forge_binance_stream_type": "01"},
            "binance_market_raw_lineage_rejected",
        ),
        (
            {"forge_binance_stream_type": True},
            "binance_market_raw_lineage_rejected",
        ),
        (
            {"add_orphan_required_wire": True},
            "required_raw_wire_without_exact_normalization",
        ),
        (
            {"binance_message_asset_override": "ETH"},
            "required_raw_wire_without_exact_normalization",
        ),
        (
            {"binance_message_asset_override": None},
            "required_raw_wire_without_exact_normalization",
        ),
        (
            {"hyperliquid_message_asset_override": "ETH"},
            "required_raw_wire_without_exact_normalization",
        ),
        (
            {"hyperliquid_message_asset_override": None},
            "required_raw_wire_without_exact_normalization",
        ),
        (
            {"binance_channel_override": None},
            "required_raw_wire_without_exact_normalization",
        ),
        (
            {"binance_channel_override": "corrupt"},
            "required_raw_wire_without_exact_normalization",
        ),
        (
            {"hyperliquid_channel_override": None},
            "required_raw_wire_without_exact_normalization",
        ),
        (
            {"hyperliquid_channel_override": "corrupt"},
            "required_raw_wire_without_exact_normalization",
        ),
        (
            {"add_orphan_binance_bbo": True},
            "binance_market_raw_lineage_rejected",
        ),
        (
            {"add_orphan_binance_l2_level": True},
            "normalized_l2_level_without_exact_raw_header_lineage",
        ),
        (
            {"add_connect_only_capture": True},
            "binance_market_capture_incomplete:binance-connect-only-capture",
        ),
        (
            {"add_half_paired_capture": True},
            "binance_connection_role_lineage_invalid:binance-connect-only-capture",
        ),
        (
            {"add_unbound_connect": True},
            "binance_connection_event_unbound",
        ),
        (
            {"binance_market_connect_role": "public"},
            "binance_connection_role_lineage_invalid:binance-capture-1",
        ),
        (
            {"binance_market_connection_epoch": 2},
            "binance_connection_role_lineage_invalid:binance-capture-1",
        ),
        (
            {"omit_hyperliquid_connect": True},
            "hyperliquid_connection_role_lineage_invalid:hyperliquid-capture-1",
        ),
        (
            {"hyperliquid_connect_role": "market"},
            "hyperliquid_connection_role_lineage_invalid:hyperliquid-capture-1",
        ),
        (
            {"straddle_trade_gap": True},
            "bounded_lake_gaps_present",
        ),
        (
            {"public_arrival_offset": 1},
            "bounded_lake_gaps_present",
        ),
    ),
    ids=(
        "orphan-resync-complete",
        "noncausal-resync-time",
        "wrong-clock-identity",
        "invalid-only-generation",
        "trade-received-time",
        "trade-quote-quantity",
        "mutated-bbo",
        "mutated-depth-derived-bbo",
        "missing-depth-derived-bbo",
        "malformed-hyperliquid-rest-bbo",
        "forged-hyperliquid-rest-bbo-identity",
        "forged-hyperliquid-rest-l2-identity",
        "mismatched-hyperliquid-rest-bbo-value",
        "missing-l2-levels",
        "empty-sided-l2",
        "zero-bbo-quantity",
        "nonbool-maker",
        "malformed-string-stream-type",
        "bool-stream-type",
        "orphan-required-wire",
        "binance-cross-asset-message-metadata",
        "binance-null-message-metadata",
        "hyperliquid-cross-asset-message-metadata",
        "hyperliquid-null-message-metadata",
        "binance-null-required-channel",
        "binance-corrupt-required-channel",
        "hyperliquid-null-required-channel",
        "hyperliquid-corrupt-required-channel",
        "orphan-normalized-bbo",
        "orphan-normalized-l2-level",
        "connect-only-generation",
        "half-paired-generation",
        "unbound-connect",
        "wrong-binance-role",
        "binance-cross-role-epoch-splice",
        "missing-hyperliquid-connect",
        "wrong-hyperliquid-role",
        "trade-gap-straddles-start",
        "new-epoch-arrival-does-not-start-at-one",
    ),
)
def test_real_lake_audit_rejects_adversarial_lineage_mutations(
    tmp_path: Path,
    mutation: dict[str, object],
    expected_reason: str,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(lake, **mutation)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "FAIL"
    assert payload["phase_10_status"] == PHASE_10_STATUS
    assert expected_reason in payload["failure_reasons"]
    overlap = payload["strict_phase_10_overlap"]
    assert isinstance(overlap, dict)
    assert overlap["duration_seconds"] == 0.0


def test_real_lake_audit_accepts_mixed_asset_hyperliquid_trade_frame(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(lake, mixed_hyperliquid_trade_batch=True)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "PASS", payload["failure_reasons"]
    assert payload["phase_10_status"] == PHASE_10_STATUS


def test_real_lake_audit_enforces_actual_clock_sample_spacing(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(
        lake,
        clock_sample_seconds=(0.5, 12.5, 24.5),
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    clock = payload["clock_sync"]
    assert isinstance(clock, dict)
    assert payload["technical_capture_gate"] == "FAIL"
    assert clock["sample_spacing_violations"] == 2
    assert clock["actual_max_sample_gap_ms"] == 12_000.0
    assert "clock_sync_sample_spacing_exceeded" in payload["failure_reasons"]


def test_real_lake_audit_rejects_clock_offset_discontinuity(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(
        lake,
        offset_jump_at=BASE + timedelta(seconds=12.6),
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    clock = payload["clock_sync"]
    assert isinstance(clock, dict)
    assert payload["technical_capture_gate"] == "FAIL"
    assert int(str(clock["offset_discontinuities"])) > 0
    assert clock["coverage_continuous"] is False
    assert "clock_sync_offset_discontinuity" in payload["failure_reasons"]


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    (
        (
            {"bound_gap_at": BASE + timedelta(seconds=25)},
            "in_window_capture_gap_event",
        ),
        (
            {"clock_failure_at": BASE + timedelta(seconds=25.6)},
            "clock_sync_in_window_failure_event",
        ),
        (
            {"disconnect_at": BASE + timedelta(seconds=25)},
            "in_window_disconnect_without_clean_stop_reason",
        ),
        (
            {"add_unbound_gap": True},
            "binance_gap_or_disconnect_unbound",
        ),
    ),
    ids=(
        "bound-gap-near-tail",
        "clock-failure-near-tail",
        "disconnect-without-clean-stop",
        "unbound-gap",
    ),
)
def test_real_lake_audit_never_hides_explicit_failures_in_tail_margin(
    tmp_path: Path,
    mutation: dict[str, object],
    expected_reason: str,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(lake, **mutation)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "FAIL"
    assert expected_reason in payload["failure_reasons"]
    overlap = payload["strict_phase_10_overlap"]
    assert isinstance(overlap, dict)
    assert overlap["duration_seconds"] == 0.0


def test_real_lake_audit_reports_failure_reason_by_capture_generation(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    failure_at = BASE + timedelta(seconds=25)
    _write_continuity_lake(
        lake,
        bound_gap_at=failure_at,
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    events = payload["connection_events"]
    assert isinstance(events, dict)
    binance_events = events["binance_usdm"]
    assert isinstance(binance_events, dict)
    failures = binance_events["failure_events_by_capture_generation"]
    assert isinstance(failures, dict)
    [event] = failures["binance-capture-1"]
    assert event["event_kind"] == "gap"
    assert event["socket_role"] == "public"
    assert event["reason"] == "coverage_unknown:clock_sync request failed"




@pytest.mark.parametrize(
    "event_kind",
    ("resync_start", "resync_complete"),
)
def test_real_lake_audit_rejects_orphan_resync_events(
    tmp_path: Path,
    event_kind: str,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(
        lake,
        orphan_resync_event_kind=event_kind,
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "FAIL"
    assert "resync_event_unbound" in payload["failure_reasons"]
    events = payload["connection_events"]
    assert isinstance(events, dict)
    binance_events = events["binance_usdm"]
    assert isinstance(binance_events, dict)
    assert binance_events["unbound_resync_events"] == 1
    overlap = payload["strict_phase_10_overlap"]
    assert isinstance(overlap, dict)
    assert overlap["duration_seconds"] == 0.0


@pytest.mark.parametrize("snapshot_timing", ("stale", "future"))
def test_real_lake_audit_rejects_noncausal_resync_snapshot_completion(
    tmp_path: Path,
    snapshot_timing: str,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(
        lake,
        temporal_resync_snapshot=snapshot_timing,
        extra_market_refresh_at=(
            BASE + timedelta(seconds=15)
            if snapshot_timing == "future"
            else None
        ),
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "FAIL"
    assert "requested_window_trailing_margin_exceeded" in payload["failure_reasons"]
    overlap = payload["strict_phase_10_overlap"]
    assert isinstance(overlap, dict)
    assert overlap["duration_seconds"] == 0.0


def test_real_lake_audit_allows_only_explicit_clean_shutdown_margin(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(
        lake,
        clean_disconnect_at=BASE + timedelta(seconds=25),
        add_historical_connect_capture=True,
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "PASS", payload
    assert payload["failure_reasons"] == []
    window = payload["requested_window"]
    assert isinstance(window, dict)
    assert window["trailing_margin_within_limit"] is True
    assert window["trailing_terminal_roles_complete"] is True


@pytest.mark.parametrize(
    "mutation",
    (
        {
            "clean_disconnect_roles": (
                "binance_public",
                "hyperliquid_public",
            )
        },
        {"clean_disconnect_unknown_identity": True},
    ),
    ids=("partial-terminal-role-set", "unknown-terminal-identity"),
)
def test_real_lake_audit_rejects_incomplete_or_unbound_clean_shutdown(
    tmp_path: Path,
    mutation: dict[str, object],
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(
        lake,
        clean_disconnect_at=BASE + timedelta(seconds=25),
        **mutation,
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "FAIL"
    assert "requested_window_trailing_clean_stop_incomplete" in payload[
        "failure_reasons"
    ]


@pytest.mark.parametrize(
    ("mutation", "expect_covered_before_recovery"),
    (
        ({"prewindow_clock_invalid": True}, True),
        ({"prewindow_clock_failure": True}, False),
    ),
    ids=("invalid-sample", "clock-gap"),
)
def test_real_lake_audit_distinguishes_pre_window_rejection_from_failure(
    tmp_path: Path,
    mutation: dict[str, object],
    expect_covered_before_recovery: bool,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(
        lake,
        clock_sample_seconds=(4.5, 14.5, 24.5),
        **mutation,
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE + timedelta(milliseconds=250),
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "PASS", payload["failure_reasons"]
    clock = payload["clock_sync"]
    assert isinstance(clock, dict)
    assert clock["coverage_continuous"] is True
    intervals = clock["intervals"]
    assert isinstance(intervals, list)
    covered_before_recovery = any(
        (
            datetime.fromisoformat(str(item["start"]).replace("Z", "+00:00"))
            <= BASE + timedelta(seconds=1)
            < datetime.fromisoformat(str(item["end"]).replace("Z", "+00:00"))
        )
        for item in intervals
        if isinstance(item, dict)
    )
    assert covered_before_recovery is expect_covered_before_recovery


def test_real_lake_audit_retains_consecutive_pre_window_rejection_state(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(
        lake,
        clock_sample_seconds=(4.5, 14.5, 24.5),
        prewindow_clock_rejection_count=2,
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE + timedelta(milliseconds=250),
        end=BASE + timedelta(seconds=30),
    )

    clock = payload["clock_sync"]
    assert isinstance(clock, dict)
    assert payload["technical_capture_gate"] == "PASS", payload["failure_reasons"]
    assert clock["rejected_probe_samples"] == 2
    assert clock["max_consecutive_rejected_probes"] == 2
    assert clock["consecutive_rejection_violations"] == 1
    assert clock[
        "consecutive_rejection_violation_capture_generations"
    ] == []
    outages = clock["consecutive_rejection_outages"]
    assert isinstance(outages, list)
    assert len(outages) == 1
    assert outages[0]["capture_epoch_id"] == "binance-capture-1"
    assert "clock_sync_consecutive_rejected_probes" not in payload["failure_reasons"]


def test_real_lake_audit_rejects_retained_pre_window_unbound_clock(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(lake, prewindow_unbound_clock_invalid=True)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "FAIL"
    assert "clock_sync_invalid_event_unbound" in payload["failure_reasons"]


def test_real_lake_audit_rejects_pre_window_conflicting_capture_event(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(lake, prewindow_conflicting_capture_event=True)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "FAIL"
    assert "binance_gap_or_disconnect_unbound" in payload["failure_reasons"]


@pytest.mark.parametrize(
    ("start", "end", "expected_reason"),
    (
        (
            BASE - timedelta(minutes=2),
            BASE + timedelta(seconds=30),
            "requested_window_leading_margin_exceeded",
        ),
        (
            BASE,
            BASE + timedelta(minutes=15),
            "requested_window_trailing_margin_exceeded",
        ),
    ),
    ids=("leading-silence", "trailing-silence"),
)
def test_real_lake_audit_rejects_large_unassessed_window_margins(
    tmp_path: Path,
    start: datetime,
    end: datetime,
    expected_reason: str,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(lake)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=start,
        end=end,
    )

    assert payload["technical_capture_gate"] == "FAIL"
    assert expected_reason in payload["failure_reasons"]


def test_real_lake_audit_requires_each_final_overlap_to_contain_all_trades(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(lake, delayed_hyperliquid_trade=True)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "FAIL"
    overlap = payload["strict_phase_10_overlap"]
    assert isinstance(overlap, dict)
    assert overlap["duration_seconds"] == 0.0
    assert "strict_phase10_overlap_zero" in payload["failure_reasons"]


def test_real_lake_audit_reuses_exact_binance_resync_for_later_l2_frames(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(
        lake,
        extra_market_refresh_at=BASE + timedelta(seconds=25),
        clock_sample_seconds=(0.5, 10.5, 20.5, 30.5, 40.5),
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=45),
    )

    assert payload["technical_capture_gate"] == "PASS", payload["failure_reasons"]
    assert payload["phase_10_status"] == PHASE_10_STATUS
    overlap = payload["strict_phase_10_overlap"]
    assert isinstance(overlap, dict)
    assert float(str(overlap["duration_seconds"])) > 30


def test_real_lake_audit_rejects_multiple_complete_hyperliquid_generations(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(lake, add_second_complete_hyperliquid_capture=True)

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(seconds=30),
    )

    assert payload["technical_capture_gate"] == "FAIL"
    assert (
        "hyperliquid_multiple_active_capture_generations"
        in payload["failure_reasons"]
    )


def test_audit_rejects_looser_direct_market_freshness_policy(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    _write_continuity_lake(lake)

    with pytest.raises(ValueError, match="strict 30-second bound"):
        audit_phase10_continuity(
            lake,
            assets=("BTC", "ETH"),
            start=BASE,
            end=BASE + timedelta(seconds=30),
            state_ttl=timedelta(seconds=31),
        )


def test_real_lake_audit_passes_sustained_fifteen_minute_capture(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    refreshes = tuple(range(25, 876, 25))
    clocks = tuple(float(value) + 0.5 for value in range(0, 891, 10))
    _write_continuity_lake(
        lake,
        periodic_refresh_seconds=refreshes,
        clock_sample_seconds=clocks,
        clean_disconnect_at=BASE + timedelta(seconds=895),
    )

    payload = data_cli.phase10_continuity_report(
        lake,
        assets=("BTC", "ETH"),
        start=BASE,
        end=BASE + timedelta(minutes=15),
    )

    assert payload["technical_capture_gate"] == "PASS", (
        payload["failure_reasons"],
        payload["validation"],
    )
    assert payload["phase_10_status"] == PHASE_10_STATUS
    clock = payload["clock_sync"]
    assert isinstance(clock, dict)
    assert clock["coverage_continuous"] is True
    overlap = payload["strict_phase_10_overlap"]
    assert isinstance(overlap, dict)
    assert float(str(overlap["duration_seconds"])) > 850


def test_global_interval_union_is_chronological_across_capture_tags() -> None:
    intervals = (
        Interval(BASE, BASE + timedelta(seconds=4), "capture-b"),
        Interval(BASE + timedelta(seconds=2), BASE + timedelta(seconds=6), "capture-a"),
        Interval(BASE + timedelta(seconds=8), BASE + timedelta(seconds=9), "capture-b"),
    )

    merged = _merge(intervals, preserve_tags=False)

    assert [(item.start, item.end) for item in merged] == [
        (BASE, BASE + timedelta(seconds=6)),
        (BASE + timedelta(seconds=8), BASE + timedelta(seconds=9)),
    ]
    assert _duration(intervals) == timedelta(seconds=7)


def test_trade_is_a_point_event_that_arms_bbo_l2_state_without_forward_fill() -> None:
    observations = {
        "capture-1": {
            "BTC": {
                "bbo": [BASE],
                "l2": [BASE + timedelta(seconds=1)],
                "trade": [BASE + timedelta(seconds=5)],
            },
            "ETH": {
                "bbo": [BASE],
                "l2": [BASE + timedelta(seconds=1)],
                "trade": [],
            },
        }
    }

    result = _required_market_intervals(
        observations,
        ("BTC", "ETH"),
        timedelta(seconds=30),
        BASE,
        BASE + timedelta(minutes=1),
        (),
    )

    assert result["capture-1"]["BTC"] == (
        Interval(
            BASE + timedelta(seconds=5),
            BASE + timedelta(seconds=30),
            "capture-1",
        ),
    )
    assert result["capture-1"]["ETH"] == ()


def test_trade_freshness_expires_even_while_bbo_and_l2_keep_refreshing() -> None:
    observations = {
        "capture-1": {
            "BTC": {
                "bbo": [
                    BASE,
                    BASE + timedelta(seconds=20),
                    BASE + timedelta(seconds=40),
                ],
                "l2": [
                    BASE,
                    BASE + timedelta(seconds=20),
                    BASE + timedelta(seconds=40),
                ],
                "trade": [BASE + timedelta(seconds=5)],
            }
        }
    }

    result = _required_market_intervals(
        observations,
        ("BTC",),
        timedelta(seconds=30),
        BASE,
        BASE + timedelta(minutes=1),
        (),
    )

    assert result["capture-1"]["BTC"] == (
        Interval(
            BASE + timedelta(seconds=5),
            BASE + timedelta(seconds=35),
            "capture-1",
        ),
    )


def test_equal_time_l2_cannot_be_armed_by_a_later_physical_arrival() -> None:
    arm = (BASE + timedelta(seconds=2), 3)

    assert not _at_or_after_resync_arm(BASE + timedelta(seconds=2), 2, arm)
    assert _at_or_after_resync_arm(BASE + timedelta(seconds=2), 3, arm)
    assert _at_or_after_resync_arm(
        BASE + timedelta(seconds=2, microseconds=1),
        1,
        arm,
    )


def _payload(gate: str) -> dict[str, object]:
    return {
        "audit_version": 1,
        "phase_10_status": PHASE_10_STATUS,
        "technical_capture_gate": gate,
        "binance_trades": {"normalized_total": 2, "raw_agg_trade_total": 2},
        "clock_sync": {"coverage_continuous": gate == "PASS"},
        "strict_phase_10_overlap": {
            "duration_seconds": 1.0 if gate == "PASS" else 0.0
        },
        "failure_reasons": [] if gate == "PASS" else ["strict_phase10_overlap_zero"],
    }


def test_cli_passes_exact_utc_bounds_and_never_unblocks_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def report(root: Path, **kwargs: object) -> dict[str, object]:
        calls.append({"root": root, **kwargs})
        return _payload("PASS")

    monkeypatch.setattr(data_cli, "phase10_continuity_report", report)
    result = runner.invoke(
        app,
        [
            "data",
            "continuity",
            str(tmp_path),
            "--assets",
            "BTC,ETH",
            "--start",
            "2026-08-13T12:00:00Z",
            "--end",
            "2026-08-13T12:15:00+00:00",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["phase_10_status"] == PHASE_10_STATUS
    assert payload["technical_capture_gate"] == "PASS"
    assert calls == [
        {
            "root": tmp_path,
            "assets": ("BTC", "ETH"),
            "start": BASE,
            "end": BASE + timedelta(minutes=15),
        }
    ]


def test_cli_emits_fail_report_and_exits_two(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        data_cli,
        "phase10_continuity_report",
        lambda *args, **kwargs: _payload("FAIL"),
    )

    result = runner.invoke(
        app,
        [
            "data",
            "continuity",
            str(tmp_path),
            "--assets",
            "BTC,ETH",
            "--start",
            "2026-08-13T12:00:00Z",
            "--end",
            "2026-08-13T12:15:00Z",
            "--json",
        ],
    )

    assert result.exit_code == 2
    assert json.loads(result.output)["technical_capture_gate"] == "FAIL"
    assert "Traceback" not in result.output


def test_cli_rejects_non_utc_bounds_before_reading_lake(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        data_cli,
        "phase10_continuity_report",
        lambda *args, **kwargs: pytest.fail("invalid bounds must fail before lake access"),
    )

    result = runner.invoke(
        app,
        [
            "data",
            "continuity",
            str(tmp_path),
            "--assets",
            "BTC,ETH",
            "--start",
            "2026-08-13T14:00:00+02:00",
            "--end",
            "2026-08-13T14:15:00+02:00",
            "--json",
        ],
    )

    assert result.exit_code == 2
    assert "INVALID_TIMESTAMP [start]" in result.output
    assert "Traceback" not in result.output
