from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

import hyperlab.data.lake as lake_module
from hyperlab.data.lake import (
    PartitionKey,
    PartitionValidationError,
    inventory_partitions,
    read_hashed_table,
    validate_partition,
    write_partition,
)
from hyperlab.data.schema import (
    SCHEMA_VERSION_METADATA,
    RecordType,
    check_schema_evolution,
    schema_for,
)

DAY = date(2026, 8, 11)
NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)
METADATA_JSON = '{"name":"BTC","szDecimals":5}'
METADATA_SHA256 = hashlib.sha256(METADATA_JSON.encode()).hexdigest()
WIRE_RAW = '{"channel":"pong"}'
WIRE_SHA256 = hashlib.sha256(WIRE_RAW.encode()).hexdigest()


def _common(record_type: RecordType) -> dict[str, object]:
    return {
        "schema_version": 1,
        "record_type": record_type.value,
        "venue": "hyperliquid",
        "asset": "BTC",
        "event_time": NOW,
        "exchange_time": NOW,
        "received_time": NOW + timedelta(milliseconds=1),
        "source_sequence": 100,
        "connection_id": "connection-1",
    }


def _valid_row(record_type: RecordType) -> dict[str, object]:
    row = _common(record_type)
    values: dict[RecordType, dict[str, object]] = {
        RecordType.INSTRUMENT_METADATA: {
            "instrument_kind": "perp",
            "instrument_id": "hyperliquid:BTC:perp",
            "source_symbol": "BTC",
            "source_index": 0,
            "base_token": "BTC",
            "quote_token": "USDC",
            "sz_decimals": 5,
            "wei_decimals": None,
            "max_leverage": 50,
            "margin_table_id": 20,
            "is_canonical": True,
            "full_name": "Bitcoin",
            "metadata_sha256": METADATA_SHA256,
            "metadata_json": METADATA_JSON,
        },
        RecordType.MARKET_CONTEXT: {
            "instrument_kind": "perp",
            "instrument_id": "hyperliquid:BTC:perp",
            "mark_price": Decimal("100"),
            "oracle_price": Decimal("99"),
            "mid_price": Decimal("99.5"),
            "current_funding_rate": Decimal("-0.0001"),
            "open_interest_quantity": Decimal("10"),
            "open_interest_notional": Decimal("1000"),
            "base_volume_24h": Decimal("20"),
            "notional_volume_24h": Decimal("2000"),
            "previous_day_price": Decimal("98"),
            "circulating_supply": None,
            "observation_id": "connection-1:1:100",
        },
        RecordType.WIRE_MESSAGE: {
            "source_sequence": None,
            "connection_epoch": 1,
            "arrival_sequence": 1,
            "channel": "pong",
            "message_asset": None,
            "raw_message": WIRE_RAW,
            "is_json": True,
            "payload_sha256": WIRE_SHA256,
        },
        RecordType.CANDLE: {
            "interval": "1h",
            "open_time": NOW - timedelta(hours=1),
            "close_time": NOW,
            "open": Decimal("100"),
            "high": Decimal("102"),
            "low": Decimal("99"),
            "close": Decimal("101"),
            "base_volume": Decimal("10"),
            "quote_volume": Decimal("1005"),
            "trade_count": 2,
            "is_final": True,
        },
        RecordType.BBO: {
            "update_id": "bbo-1",
            "bid_price": Decimal("100"),
            "bid_quantity": Decimal("1"),
            "ask_price": Decimal("101"),
            "ask_quantity": Decimal("2"),
        },
        RecordType.L2_SNAPSHOT: {
            "snapshot_id": "snapshot-1",
            "book_epoch_id": "epoch-1",
            "last_sequence": 100,
            "side": "bid",
            "level": 0,
            "price": Decimal("100"),
            "quantity": Decimal("1"),
            "order_count": 1,
        },
        RecordType.L2_DELTA: {
            "update_id": "update-1",
            "book_epoch_id": "epoch-1",
            "first_sequence": 100,
            "last_sequence": 101,
            "side": "bid",
            "price": Decimal("100"),
            "quantity": Decimal("1"),
            "action": "set",
        },
        RecordType.TRADE: {
            "trade_id": "trade-1",
            "aggressor_side": "unknown",
            "price": Decimal("100"),
            "quantity": Decimal("1"),
            "quote_quantity": Decimal("100"),
            "is_liquidation": False,
        },
        RecordType.FUNDING: {
            "funding_time": NOW,
            "funding_rate": Decimal("-0.0001"),
            "funding_interval_seconds": 3600,
            "rate_kind": "venue-estimate",
            "mark_price": Decimal("100"),
            "oracle_price": Decimal("99"),
        },
        RecordType.OPEN_INTEREST: {
            "open_interest_quantity": Decimal("10"),
            "open_interest_notional": Decimal("1000"),
            "mark_price": Decimal("100"),
        },
        RecordType.FEE: {
            "fee_schedule_id": "fees-1",
            "scope": "venue-default",
            "instrument_kind": "perp",
            "maker_fee_bps": Decimal("-0.5"),
            "taker_fee_bps": Decimal("2"),
            "effective_from": NOW,
            "effective_to": NOW + timedelta(days=1),
            "fee_currency": "USDC",
        },
        RecordType.CONNECTION_EVENT: {
            "event_kind": "connect",
            "channel": "l2Book",
            "book_epoch_id": "epoch-1",
            "reason": None,
            "expected_sequence": None,
            "observed_sequence": None,
            "resync_snapshot_id": None,
        },
        RecordType.INSTRUMENT_LIFECYCLE: {
            "source_symbol": "BTC",
            "instrument_id": "hyperliquid:BTC:perp",
            "instrument_kind": "perp",
            "status": "listed",
            "valid_from": NOW,
            "valid_to": NOW + timedelta(days=1),
        },
    }
    row.update(values[record_type])
    return row


def _table(record_type: RecordType, **changes: object) -> pa.Table:
    row = _valid_row(record_type)
    row.update(changes)
    return pa.Table.from_pylist([row], schema=schema_for(record_type).schema)


@pytest.mark.parametrize(
    "record_type",
    [
        RecordType.INSTRUMENT_METADATA,
        RecordType.MARKET_CONTEXT,
        RecordType.WIRE_MESSAGE,
    ],
)
def test_write_accepts_valid_phase_02_records(
    tmp_path: Path,
    record_type: RecordType,
) -> None:
    manifest = write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", record_type),
        _table(record_type),
    )

    assert manifest.row_count == 1
    assert manifest.quality == "ok"


def test_wire_message_preserves_exact_non_json_text(tmp_path: Path) -> None:
    raw_message = "Websocket connection established."
    payload_sha256 = hashlib.sha256(raw_message.encode()).hexdigest()
    table = _table(
        RecordType.WIRE_MESSAGE,
        asset="GLOBAL",
        channel=None,
        raw_message=raw_message,
        is_json=False,
        payload_sha256=payload_sha256,
    )
    manifest = write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "GLOBAL", RecordType.WIRE_MESSAGE),
        table,
    )

    decoded = read_hashed_table(tmp_path, manifest)
    assert decoded.column("raw_message")[0].as_py() == raw_message
    assert decoded.column("source_sequence")[0].as_py() is None


@pytest.mark.parametrize("version", [1, 2])
@pytest.mark.parametrize("field", ["connection_epoch", "arrival_sequence"])
def test_wire_lineage_is_strictly_positive_in_every_schema_version(
    tmp_path: Path,
    version: int,
    field: str,
) -> None:
    row = _valid_row(RecordType.WIRE_MESSAGE)
    row.update(schema_version=version, **{field: 0})
    if version == 2:
        row["capture_epoch_id"] = "capture-1"
    table = pa.Table.from_pylist(
        [row],
        schema=schema_for(RecordType.WIRE_MESSAGE, version=version).schema,
    )

    with pytest.raises(PartitionValidationError, match=field):
        write_partition(
            tmp_path,
            PartitionKey("hyperliquid", DAY, "BTC", RecordType.WIRE_MESSAGE),
            table,
        )


@pytest.mark.parametrize(
    ("record_type", "field"),
    [
        (RecordType.TRADE, "connection_epoch"),
        (RecordType.TRADE, "arrival_sequence"),
        (RecordType.CONNECTION_EVENT, "connection_epoch"),
    ],
)
def test_optional_v2_lineage_is_positive_when_present(
    tmp_path: Path,
    record_type: RecordType,
    field: str,
) -> None:
    row = _valid_row(record_type)
    row["schema_version"] = 2
    if record_type == RecordType.TRADE:
        row.update(connection_epoch=1, arrival_sequence=1)
    else:
        row.update(
            connection_epoch=1,
            capture_epoch_id="capture-1",
            socket_role="public",
        )
    row[field] = 0
    table = pa.Table.from_pylist(
        [row],
        schema=schema_for(record_type, version=2).schema,
    )

    with pytest.raises(PartitionValidationError, match=field):
        write_partition(
            tmp_path,
            PartitionKey("hyperliquid", DAY, "BTC", record_type),
            table,
        )


@pytest.mark.parametrize(
    ("record_type", "changes", "message"),
    [
        (
            RecordType.INSTRUMENT_METADATA,
            {"max_leverage": 0},
            "max_leverage.*positive",
        ),
        (RecordType.MARKET_CONTEXT, {"mark_price": Decimal("0")}, "positive price"),
        (RecordType.MARKET_CONTEXT, {"oracle_price": Decimal("0")}, "positive price"),
        (RecordType.MARKET_CONTEXT, {"mid_price": Decimal("0")}, "positive price"),
        (RecordType.MARKET_CONTEXT, {"previous_day_price": Decimal("-1")}, "non-negative"),
        (
            RecordType.MARKET_CONTEXT,
            {"base_volume_24h": Decimal("-1")},
            "non-negative",
        ),
        (RecordType.WIRE_MESSAGE, {"source_sequence": 1}, "must remain null"),
        (RecordType.WIRE_MESSAGE, {"is_json": False}, "is_json"),
        (
            RecordType.WIRE_MESSAGE,
            {"payload_sha256": "0" * 64},
            "does not match",
        ),
        (RecordType.CANDLE, {"open": Decimal("0")}, "positive price"),
        (RecordType.CANDLE, {"base_volume": Decimal("-1")}, "non-negative"),
        (RecordType.CANDLE, {"high": Decimal("100")}, "OHLC"),
        (RecordType.CANDLE, {"open_time": NOW}, "open_time.*close_time"),
        (RecordType.BBO, {"bid_price": Decimal("102")}, "bid_price.*ask_price"),
        (RecordType.BBO, {"ask_quantity": Decimal("-1")}, "non-negative"),
        (RecordType.L2_SNAPSHOT, {"price": Decimal("0")}, "positive price"),
        (RecordType.L2_SNAPSHOT, {"quantity": Decimal("-1")}, "non-negative"),
        (RecordType.L2_DELTA, {"first_sequence": 102}, "first_sequence.*last_sequence"),
        (RecordType.TRADE, {"quantity": Decimal("0")}, "trade quantity.*positive"),
        (RecordType.FUNDING, {"mark_price": Decimal("0")}, "positive price"),
        (
            RecordType.OPEN_INTEREST,
            {"open_interest_notional": Decimal("-1")},
            "non-negative",
        ),
        (RecordType.OPEN_INTEREST, {"mark_price": Decimal("0")}, "positive price"),
        (
            RecordType.FEE,
            {"effective_to": NOW - timedelta(seconds=1)},
            "effective_from.*effective_to",
        ),
        (
            RecordType.INSTRUMENT_LIFECYCLE,
            {"valid_to": NOW - timedelta(seconds=1)},
            "valid_from.*valid_to",
        ),
    ],
)
def test_write_rejects_invalid_market_semantics(
    tmp_path: Path,
    record_type: RecordType,
    changes: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(PartitionValidationError, match=message):
        write_partition(
            tmp_path,
            PartitionKey("hyperliquid", DAY, "BTC", record_type),
            _table(record_type, **changes),
        )


@pytest.mark.parametrize("previous_day_price", [Decimal("0"), None])
def test_market_context_accepts_unavailable_previous_day_reference(
    tmp_path: Path,
    previous_day_price: Decimal | None,
) -> None:
    manifest = write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "@189", RecordType.MARKET_CONTEXT),
        _table(
            RecordType.MARKET_CONTEXT,
            asset="@189",
            instrument_kind="spot",
            instrument_id="hyperliquid:@189:spot",
            mark_price=Decimal("620"),
            oracle_price=None,
            mid_price=None,
            previous_day_price=previous_day_price,
        ),
    )
    assert read_hashed_table(tmp_path, manifest).column("previous_day_price")[0].as_py() == previous_day_price


def test_instrument_metadata_requires_valid_json_even_when_hash_matches(
    tmp_path: Path,
) -> None:
    invalid_json = "{"
    matching_hash = hashlib.sha256(invalid_json.encode()).hexdigest()

    with pytest.raises(PartitionValidationError, match=r"metadata_json.*valid JSON"):
        write_partition(
            tmp_path,
            PartitionKey("hyperliquid", DAY, "BTC", RecordType.INSTRUMENT_METADATA),
            _table(
                RecordType.INSTRUMENT_METADATA,
                metadata_json=invalid_json,
                metadata_sha256=matching_hash,
            ),
        )


@pytest.mark.parametrize(
    ("record_type", "field", "value"),
    [
        (RecordType.INSTRUMENT_METADATA, "instrument_kind", "future"),
        (RecordType.MARKET_CONTEXT, "instrument_kind", "future"),
        (RecordType.L2_SNAPSHOT, "side", "middle"),
        (RecordType.L2_DELTA, "side", "middle"),
        (RecordType.L2_DELTA, "action", "append"),
        (RecordType.TRADE, "aggressor_side", "middle"),
        (RecordType.CONNECTION_EVENT, "event_kind", "maybe"),
        (RecordType.FEE, "instrument_kind", "future"),
        (RecordType.INSTRUMENT_LIFECYCLE, "instrument_kind", "future"),
        (RecordType.INSTRUMENT_LIFECYCLE, "status", "erased"),
    ],
)
def test_write_rejects_unknown_closed_vocabulary(
    tmp_path: Path,
    record_type: RecordType,
    field: str,
    value: str,
) -> None:
    with pytest.raises(PartitionValidationError, match=field):
        write_partition(
            tmp_path,
            PartitionKey("hyperliquid", DAY, "BTC", record_type),
            _table(record_type, **{field: value}),
        )


@pytest.mark.parametrize(
    ("record_type", "field"),
    [
        (RecordType.FUNDING, "rate_kind"),
        (RecordType.INSTRUMENT_METADATA, "source_symbol"),
        (RecordType.MARKET_CONTEXT, "instrument_id"),
        (RecordType.WIRE_MESSAGE, "channel"),
        (RecordType.FEE, "scope"),
    ],
)
def test_write_rejects_empty_extensible_vocabulary(
    tmp_path: Path,
    record_type: RecordType,
    field: str,
) -> None:
    with pytest.raises(PartitionValidationError, match=field):
        write_partition(
            tmp_path,
            PartitionKey("hyperliquid", DAY, "BTC", record_type),
            _table(record_type, **{field: "  "}),
        )


def test_validate_rechecks_semantics_from_the_decoded_parquet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", RecordType.TRADE),
        _table(RecordType.TRADE),
    )
    invalid = _table(RecordType.TRADE, quantity=Decimal("0"))
    monkeypatch.setattr(lake_module, "_read_hashed_path", lambda *_args, **_kwargs: invalid)

    with pytest.raises(PartitionValidationError, match="trade quantity must be positive"):
        validate_partition(tmp_path / manifest.relative_manifest_path)


@pytest.mark.parametrize("record_type", [RecordType.L2_SNAPSHOT, RecordType.L2_DELTA])
def test_inventory_rejects_inconsistent_l2_metadata_split_across_files(
    tmp_path: Path,
    record_type: RecordType,
) -> None:
    identifier = "snapshot_id" if record_type == RecordType.L2_SNAPSHOT else "update_id"
    first = _valid_row(record_type)
    second = _valid_row(record_type)
    second.update(
        side="ask",
        price=Decimal("101"),
        event_time=NOW + timedelta(seconds=1),
        exchange_time=NOW + timedelta(seconds=1),
        received_time=NOW + timedelta(seconds=1, milliseconds=1),
        source_sequence=101,
    )
    second["book_epoch_id"] = "epoch-2"
    spec = schema_for(record_type)

    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", record_type),
        pa.Table.from_pylist([first], schema=spec.schema),
    )
    write_partition(
        tmp_path,
        PartitionKey("hyperliquid", DAY, "BTC", record_type),
        pa.Table.from_pylist([second], schema=spec.schema),
    )

    with pytest.raises(
        PartitionValidationError,
        match=rf"inconsistent L2 .* metadata.*{identifier}",
    ):
        inventory_partitions(tmp_path)


def test_l2_metadata_must_be_consistent_inside_one_snapshot(tmp_path: Path) -> None:
    spec = schema_for(RecordType.L2_SNAPSHOT)
    first = {**_valid_row(RecordType.L2_SNAPSHOT), "side": "ask", "price": Decimal("101")}
    second = {**first, "side": "bid", "price": Decimal("100"), "connection_id": "other"}

    with pytest.raises(PartitionValidationError, match="inconsistent L2 snapshot metadata"):
        write_partition(
            tmp_path,
            PartitionKey("hyperliquid", DAY, "BTC", RecordType.L2_SNAPSHOT),
            pa.Table.from_pylist([first, second], schema=spec.schema),
        )


def test_schema_evolution_rejects_out_of_range_version() -> None:
    previous = schema_for(RecordType.TRADE)
    candidate_schema = previous.schema.append(pa.field("note", pa.string(), nullable=True))
    candidate_schema = candidate_schema.with_metadata(
        {
            **(candidate_schema.metadata or {}),
            SCHEMA_VERSION_METADATA: b"65536",
        }
    )
    candidate = replace(previous, version=65_536, schema=candidate_schema)

    with pytest.raises(ValueError, match="schema version must be between 1 and 65535"):
        check_schema_evolution(previous, candidate)


def test_schema_evolution_rejects_schema_metadata_changes_other_than_version() -> None:
    previous = schema_for(RecordType.TRADE)
    candidate_schema = previous.schema.append(pa.field("note", pa.string(), nullable=True))
    candidate_schema = candidate_schema.with_metadata(
        {
            **(candidate_schema.metadata or {}),
            SCHEMA_VERSION_METADATA: b"2",
            b"hyperlab.semantic_contract": b"changed",
        }
    )
    candidate = replace(previous, version=2, schema=candidate_schema)

    with pytest.raises(ValueError, match="schema metadata changed"):
        check_schema_evolution(previous, candidate)
