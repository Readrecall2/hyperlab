from __future__ import annotations

from dataclasses import replace

import pyarrow as pa
import pytest

from hyperlab.data.schema import (
    BREAKING_SCHEMA_TRANSITIONS,
    RecordType,
    SchemaSpec,
    check_schema_evolution,
    latest_schema_for,
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


def test_registry_contains_every_phase_02_record_type() -> None:
    assert {spec.record_type for spec in registered_schemas()} == set(RecordType)
    assert {item.value for item in RecordType} == {
        "instrument_metadata",
        "market_context",
        "wire_message",
        "candle",
        "bbo",
        "l2_book_state",
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
        RecordType.MARKET_CONTEXT: {
            "mark_price",
            "oracle_price",
            "mid_price",
            "current_funding_rate",
            "open_interest_quantity",
            "open_interest_notional",
            "base_volume_24h",
            "notional_volume_24h",
            "previous_day_price",
            "circulating_supply",
        },
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
        schema = latest_schema_for(record_type).schema
        assert {schema.field(name).type for name in names} == {decimal_type}


def test_phase_02_public_collector_schemas_are_strict() -> None:
    expected = {
        RecordType.INSTRUMENT_METADATA: [
            ("instrument_kind", pa.string(), False),
            ("instrument_id", pa.string(), False),
            ("source_symbol", pa.string(), False),
            ("source_index", pa.uint32(), True),
            ("base_token", pa.string(), True),
            ("quote_token", pa.string(), True),
            ("sz_decimals", pa.uint8(), True),
            ("wei_decimals", pa.uint8(), True),
            ("max_leverage", pa.uint32(), True),
            ("margin_table_id", pa.uint32(), True),
            ("is_canonical", pa.bool_(), True),
            ("full_name", pa.string(), True),
            ("metadata_sha256", pa.string(), False),
            ("metadata_json", pa.string(), False),
        ],
        RecordType.MARKET_CONTEXT: [
            ("instrument_kind", pa.string(), False),
            ("instrument_id", pa.string(), False),
            ("mark_price", pa.decimal128(38, 18), True),
            ("oracle_price", pa.decimal128(38, 18), True),
            ("mid_price", pa.decimal128(38, 18), True),
            ("current_funding_rate", pa.decimal128(38, 18), True),
            ("open_interest_quantity", pa.decimal128(38, 18), True),
            ("open_interest_notional", pa.decimal128(38, 18), True),
            ("base_volume_24h", pa.decimal128(38, 18), True),
            ("notional_volume_24h", pa.decimal128(38, 18), True),
            ("previous_day_price", pa.decimal128(38, 18), True),
            ("circulating_supply", pa.decimal128(38, 18), True),
            ("observation_id", pa.string(), False),
        ],
        RecordType.WIRE_MESSAGE: [
            ("connection_epoch", pa.uint64(), False),
            ("arrival_sequence", pa.uint64(), False),
            ("channel", pa.string(), True),
            ("message_asset", pa.string(), True),
            ("raw_message", pa.string(), False),
            ("is_json", pa.bool_(), False),
            ("payload_sha256", pa.string(), False),
        ],
    }

    for record_type, expected_fields in expected.items():
        schema = latest_schema_for(record_type).schema
        phase_fields = list(schema)[len(COMMON_FIELDS) :]
        assert [(field.name, field.type, field.nullable) for field in phase_fields] == expected_fields


def test_latest_bbo_represents_documented_nullable_sides_and_keeps_raw_wire() -> None:
    bbo = latest_schema_for(RecordType.BBO).schema
    wire = latest_schema_for(RecordType.WIRE_MESSAGE).schema

    assert all(
        bbo.field(name).nullable for name in ("bid_price", "bid_quantity", "ask_price", "ask_quantity")
    )
    assert not wire.field("raw_message").nullable
    assert wire.field("channel").nullable


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


def test_schema_lookup_defaults_to_v1_while_latest_is_explicit() -> None:
    by_record_type: dict[RecordType, list[SchemaSpec]] = {}
    for spec in registered_schemas():
        by_record_type.setdefault(spec.record_type, []).append(spec)

    assert set(by_record_type) == set(RecordType)
    for record_type, specs in by_record_type.items():
        versions = sorted(spec.version for spec in specs)
        implicit = schema_for(record_type)
        explicit_v1 = schema_for(record_type, version=1)
        latest = latest_schema_for(record_type)

        assert implicit is explicit_v1
        assert implicit.version == 1
        assert latest.version == versions[-1]
        assert schema_for(record_type, version=latest.version) is latest


def test_every_registered_multiversion_transition_is_declared_breaking() -> None:
    versions_by_type: dict[RecordType, list[int]] = {}
    for spec in registered_schemas():
        versions_by_type.setdefault(spec.record_type, []).append(spec.version)
    expected_transitions = {
        (record_type, previous, candidate)
        for record_type, versions in versions_by_type.items()
        for previous, candidate in zip(sorted(versions), sorted(versions)[1:], strict=False)
    }

    assert set(BREAKING_SCHEMA_TRANSITIONS) == expected_transitions
    for transition, reason in BREAKING_SCHEMA_TRANSITIONS.items():
        record_type, previous_version, candidate_version = transition
        assert reason.strip()
        previous = schema_for(record_type, version=previous_version)
        candidate = schema_for(record_type, version=candidate_version)
        with pytest.raises(ValueError, match="incompatible schema evolution"):
            check_schema_evolution(previous, candidate)
