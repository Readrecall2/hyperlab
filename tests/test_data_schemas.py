from __future__ import annotations

from dataclasses import replace

import pyarrow as pa
import pytest

from hyperlab.data.schema import (
    RecordType,
    SchemaSpec,
    check_schema_evolution,
    registered_schemas,
    schema_fingerprint,
    schema_for,
)

UTC_NS = pa.timestamp("ns", tz="UTC")
COMMON_FIELDS = {
    "schema_version": (pa.uint16(), False),
    "record_type": (pa.string(), False),
    "venue": (pa.string(), False),
    "asset": (pa.string(), False),
    "event_time": (UTC_NS, False),
    "exchange_time": (UTC_NS, True),
    "received_time": (UTC_NS, False),
    "source_sequence": (pa.uint64(), True),
    "connection_id": (pa.string(), True),
}


def test_registry_contains_every_phase_01_record_type() -> None:
    assert {spec.record_type for spec in registered_schemas()} == set(RecordType)
    assert {item.value for item in RecordType} == {
        "candle",
        "bbo",
        "l2_snapshot",
        "l2_delta",
        "trade",
        "funding",
        "open_interest",
        "fee",
        "connection_event",
        "instrument_lifecycle",
    }


@pytest.mark.parametrize("record_type", list(RecordType))
def test_v1_schemas_share_the_auditable_utc_envelope(record_type: RecordType) -> None:
    spec = schema_for(record_type)

    assert isinstance(spec, SchemaSpec)
    assert spec.version == 1
    assert spec.schema.metadata == {
        b"hyperlab.schema_name": record_type.value.encode(),
        b"hyperlab.schema_version": b"1",
    }
    for name, (expected_type, expected_nullable) in COMMON_FIELDS.items():
        field = spec.schema.field(name)
        assert field.type == expected_type
        assert field.nullable is expected_nullable


def test_price_quantity_and_rate_fields_use_decimal128() -> None:
    decimal_type = pa.decimal128(38, 18)
    expectations = {
        RecordType.CANDLE: {"open", "high", "low", "close", "base_volume", "quote_volume"},
        RecordType.BBO: {"bid_price", "bid_quantity", "ask_price", "ask_quantity"},
        RecordType.L2_SNAPSHOT: {"price", "quantity"},
        RecordType.L2_DELTA: {"price", "quantity"},
        RecordType.TRADE: {"price", "quantity", "quote_quantity"},
        RecordType.FUNDING: {"funding_rate", "mark_price", "oracle_price"},
        RecordType.OPEN_INTEREST: {
            "open_interest_quantity",
            "open_interest_notional",
            "mark_price",
        },
        RecordType.FEE: {"maker_fee_bps", "taker_fee_bps"},
    }

    for record_type, names in expectations.items():
        schema = schema_for(record_type).schema
        assert {schema.field(name).type for name in names} == {decimal_type}


def test_l2_snapshots_and_deltas_are_distinct_schemas() -> None:
    snapshot = schema_for(RecordType.L2_SNAPSHOT)
    delta = schema_for("l2_delta")

    assert "snapshot_id" in snapshot.schema.names
    assert "level" in snapshot.schema.names
    assert "action" not in snapshot.schema.names
    assert "update_id" in delta.schema.names
    assert "action" in delta.schema.names
    assert "level" not in delta.schema.names
    assert snapshot.primary_key != delta.primary_key
    assert schema_fingerprint(snapshot) != schema_fingerprint(delta.schema)


def test_unknown_record_type_or_version_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown record type"):
        schema_for("orders")
    with pytest.raises(ValueError, match="unknown schema version"):
        schema_for(RecordType.CANDLE, version=99)


def test_nullable_addition_is_a_compatible_schema_evolution() -> None:
    previous = schema_for(RecordType.TRADE)
    candidate_schema = previous.schema.append(pa.field("venue_flags", pa.string(), nullable=True))
    candidate_schema = candidate_schema.with_metadata(
        {
            b"hyperlab.schema_name": b"trade",
            b"hyperlab.schema_version": b"2",
        }
    )
    candidate = replace(previous, version=2, schema=candidate_schema)

    check_schema_evolution(previous, candidate)


@pytest.mark.parametrize("key_name", ["primary_key", "order_key"])
def test_schema_evolution_cannot_silently_change_a_key(key_name: str) -> None:
    previous = schema_for(RecordType.TRADE)
    candidate_schema = previous.schema.append(pa.field("venue_flags", pa.string(), nullable=True))
    candidate_schema = candidate_schema.with_metadata(
        {
            b"hyperlab.schema_name": b"trade",
            b"hyperlab.schema_version": b"2",
        }
    )
    candidate = replace(
        previous,
        version=2,
        schema=candidate_schema,
        **{key_name: ("event_time",)},
    )

    with pytest.raises(ValueError, match=f"{key_name.replace('_', ' ')} changed"):
        check_schema_evolution(previous, candidate)


def test_schema_evolution_rejects_a_duplicate_field_name() -> None:
    previous = schema_for(RecordType.TRADE)
    candidate_schema = previous.schema.append(pa.field("trade_id", pa.string(), nullable=True))
    candidate_schema = candidate_schema.with_metadata(
        {
            b"hyperlab.schema_name": b"trade",
            b"hyperlab.schema_version": b"2",
        }
    )
    candidate = replace(previous, version=2, schema=candidate_schema)

    with pytest.raises(ValueError, match="field names must remain unique"):
        check_schema_evolution(previous, candidate)


@pytest.mark.parametrize("mutation", ["remove", "change_type", "make_required", "required_addition"])
def test_destructive_or_required_schema_evolution_is_rejected(mutation: str) -> None:
    previous = schema_for(RecordType.TRADE)
    fields = list(previous.schema)
    if mutation == "remove":
        fields = [field for field in fields if field.name != "trade_id"]
    elif mutation == "change_type":
        index = previous.schema.get_field_index("trade_id")
        fields[index] = pa.field("trade_id", pa.uint64(), nullable=False)
    elif mutation == "make_required":
        index = previous.schema.get_field_index("exchange_time")
        fields[index] = pa.field("exchange_time", UTC_NS, nullable=False)
    else:
        fields.append(pa.field("required_new_field", pa.string(), nullable=False))

    candidate_schema = pa.schema(
        fields,
        metadata={
            b"hyperlab.schema_name": b"trade",
            b"hyperlab.schema_version": b"2",
        },
    )
    candidate = replace(previous, version=2, schema=candidate_schema)

    with pytest.raises(ValueError, match="incompatible schema evolution"):
        check_schema_evolution(previous, candidate)


def test_fingerprint_is_stable_and_sensitive_to_schema_changes() -> None:
    candle = schema_for(RecordType.CANDLE)
    assert schema_fingerprint(candle) == schema_fingerprint(candle.schema)
    changed = candle.schema.append(pa.field("optional", pa.string()))
    assert schema_fingerprint(changed) != schema_fingerprint(candle)
