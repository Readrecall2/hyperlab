from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum

import pyarrow as pa

UTC_TIMESTAMP = pa.timestamp("ns", tz="UTC")
MARKET_DECIMAL = pa.decimal128(38, 18)
SCHEMA_NAME_METADATA = b"hyperlab.schema_name"
SCHEMA_VERSION_METADATA = b"hyperlab.schema_version"


class RecordType(StrEnum):
    INSTRUMENT_METADATA = "instrument_metadata"
    MARKET_CONTEXT = "market_context"
    WIRE_MESSAGE = "wire_message"
    CANDLE = "candle"
    BBO = "bbo"
    L2_BOOK_STATE = "l2_book_state"
    L2_SNAPSHOT = "l2_snapshot"
    L2_DELTA = "l2_delta"
    TRADE = "trade"
    FUNDING = "funding"
    OPEN_INTEREST = "open_interest"
    FEE = "fee"
    CONNECTION_EVENT = "connection_event"
    INSTRUMENT_LIFECYCLE = "instrument_lifecycle"
    CLOCK_SYNC = "clock_sync"


@dataclass(frozen=True, slots=True)
class SchemaSpec:
    record_type: RecordType
    version: int
    schema: pa.Schema
    primary_key: tuple[str, ...]
    order_key: tuple[str, ...]


def instrument(exchange: str, asset: str, kind: str) -> str:
    if kind not in {"spot", "perp"}:
        raise ValueError(f"unsupported instrument kind: {kind}")
    return f"{exchange.upper()}:{asset.upper()}:{kind}"


def parse_instrument(value: str) -> tuple[str, str, str]:
    parts = value.split(":")
    if len(parts) != 3:
        raise ValueError(f"invalid instrument id: {value}")
    exchange, asset, kind = parts
    if kind not in {"spot", "perp"}:
        raise ValueError(f"invalid instrument kind: {kind}")
    return exchange, asset, kind


def _field(name: str, data_type: pa.DataType, *, nullable: bool = False) -> pa.Field:
    return pa.field(name, data_type, nullable=nullable)


def _common_fields() -> list[pa.Field]:
    return [
        _field("schema_version", pa.uint16()),
        _field("record_type", pa.string()),
        _field("venue", pa.string()),
        _field("asset", pa.string()),
        _field("event_time", UTC_TIMESTAMP),
        _field("exchange_time", UTC_TIMESTAMP, nullable=True),
        _field("received_time", UTC_TIMESTAMP),
        _field("source_sequence", pa.uint64(), nullable=True),
        _field("connection_id", pa.string(), nullable=True),
    ]


def _make_spec(
    record_type: RecordType,
    fields: list[pa.Field],
    *,
    primary_key: tuple[str, ...],
    order_key: tuple[str, ...],
    version: int = 1,
) -> SchemaSpec:
    schema = pa.schema(
        [*_common_fields(), *fields],
        metadata={
            SCHEMA_NAME_METADATA: record_type.value.encode(),
            SCHEMA_VERSION_METADATA: str(version).encode(),
        },
    )
    return SchemaSpec(
        record_type=record_type,
        version=version,
        schema=schema,
        primary_key=primary_key,
        order_key=order_key,
    )


_V1_SPECS = (
    _make_spec(
        RecordType.INSTRUMENT_METADATA,
        [
            _field("instrument_kind", pa.string()),
            _field("instrument_id", pa.string()),
            _field("source_symbol", pa.string()),
            _field("source_index", pa.uint32(), nullable=True),
            _field("base_token", pa.string(), nullable=True),
            _field("quote_token", pa.string(), nullable=True),
            _field("sz_decimals", pa.uint8(), nullable=True),
            _field("wei_decimals", pa.uint8(), nullable=True),
            _field("max_leverage", pa.uint32(), nullable=True),
            _field("margin_table_id", pa.uint32(), nullable=True),
            _field("is_canonical", pa.bool_(), nullable=True),
            _field("full_name", pa.string(), nullable=True),
            _field("metadata_sha256", pa.string()),
            _field("metadata_json", pa.string()),
        ],
        primary_key=("venue", "instrument_id", "event_time", "metadata_sha256"),
        order_key=("event_time", "received_time", "instrument_id", "metadata_sha256"),
    ),
    _make_spec(
        RecordType.MARKET_CONTEXT,
        [
            _field("instrument_kind", pa.string()),
            _field("instrument_id", pa.string()),
            _field("mark_price", MARKET_DECIMAL, nullable=True),
            _field("oracle_price", MARKET_DECIMAL, nullable=True),
            _field("mid_price", MARKET_DECIMAL, nullable=True),
            _field("current_funding_rate", MARKET_DECIMAL, nullable=True),
            _field("open_interest_quantity", MARKET_DECIMAL, nullable=True),
            _field("open_interest_notional", MARKET_DECIMAL, nullable=True),
            _field("base_volume_24h", MARKET_DECIMAL, nullable=True),
            _field("notional_volume_24h", MARKET_DECIMAL, nullable=True),
            _field("previous_day_price", MARKET_DECIMAL, nullable=True),
            _field("circulating_supply", MARKET_DECIMAL, nullable=True),
            _field("observation_id", pa.string()),
        ],
        primary_key=("venue", "instrument_id", "observation_id"),
        order_key=("event_time", "received_time", "source_sequence", "instrument_id"),
    ),
    _make_spec(
        RecordType.WIRE_MESSAGE,
        [
            _field("connection_epoch", pa.uint64()),
            _field("arrival_sequence", pa.uint64()),
            _field("channel", pa.string(), nullable=True),
            _field("message_asset", pa.string(), nullable=True),
            _field("raw_message", pa.string()),
            _field("is_json", pa.bool_()),
            _field("payload_sha256", pa.string()),
            _field("capture_epoch_id", pa.string(), nullable=True),
        ],
        primary_key=(
            "venue",
            "connection_id",
            "connection_epoch",
            "arrival_sequence",
        ),
        order_key=(
            "event_time",
            "received_time",
            "connection_epoch",
            "arrival_sequence",
        ),
        version=2,
    ),
    _make_spec(
        RecordType.CANDLE,
        [
            _field("interval", pa.string()),
            _field("open_time", UTC_TIMESTAMP),
            _field("close_time", UTC_TIMESTAMP),
            _field("open", MARKET_DECIMAL),
            _field("high", MARKET_DECIMAL),
            _field("low", MARKET_DECIMAL),
            _field("close", MARKET_DECIMAL),
            _field("base_volume", MARKET_DECIMAL),
            _field("quote_volume", MARKET_DECIMAL, nullable=True),
            _field("trade_count", pa.uint64(), nullable=True),
            _field("is_final", pa.bool_(), nullable=True),
            _field("observation_id", pa.string()),
        ],
        primary_key=("venue", "asset", "interval", "open_time", "observation_id"),
        order_key=("event_time", "received_time", "source_sequence", "open_time"),
        version=2,
    ),
    _make_spec(
        RecordType.BBO,
        [
            _field("update_id", pa.string()),
            _field("bid_price", MARKET_DECIMAL, nullable=True),
            _field("bid_quantity", MARKET_DECIMAL, nullable=True),
            _field("ask_price", MARKET_DECIMAL, nullable=True),
            _field("ask_quantity", MARKET_DECIMAL, nullable=True),
        ],
        primary_key=("venue", "asset", "update_id"),
        order_key=("event_time", "received_time", "source_sequence"),
        version=2,
    ),
    _make_spec(
        RecordType.L2_BOOK_STATE,
        [
            _field("snapshot_id", pa.string()),
            _field("book_epoch_id", pa.string()),
            _field("bid_level_count", pa.uint32()),
            _field("ask_level_count", pa.uint32()),
        ],
        primary_key=("venue", "asset", "snapshot_id"),
        order_key=("event_time", "received_time", "snapshot_id"),
    ),
    _make_spec(
        RecordType.L2_SNAPSHOT,
        [
            _field("snapshot_id", pa.string()),
            _field("book_epoch_id", pa.string()),
            _field("last_sequence", pa.uint64(), nullable=True),
            _field("side", pa.string()),
            _field("level", pa.uint32()),
            _field("price", MARKET_DECIMAL),
            _field("quantity", MARKET_DECIMAL),
            _field("order_count", pa.uint64(), nullable=True),
        ],
        primary_key=("venue", "asset", "snapshot_id", "side", "level"),
        order_key=(
            "event_time",
            "received_time",
            "source_sequence",
            "snapshot_id",
            "side",
            "level",
        ),
    ),
    _make_spec(
        RecordType.L2_DELTA,
        [
            _field("update_id", pa.string()),
            _field("book_epoch_id", pa.string()),
            _field("first_sequence", pa.uint64(), nullable=True),
            _field("last_sequence", pa.uint64(), nullable=True),
            _field("side", pa.string()),
            _field("price", MARKET_DECIMAL),
            _field("quantity", MARKET_DECIMAL),
            _field("action", pa.string()),
        ],
        primary_key=("venue", "asset", "update_id", "side", "price"),
        order_key=(
            "event_time",
            "received_time",
            "source_sequence",
            "update_id",
            "side",
            "price",
        ),
    ),
    _make_spec(
        RecordType.TRADE,
        [
            _field("trade_id", pa.string()),
            _field("aggressor_side", pa.string()),
            _field("price", MARKET_DECIMAL),
            _field("quantity", MARKET_DECIMAL),
            _field("quote_quantity", MARKET_DECIMAL, nullable=True),
            _field("is_liquidation", pa.bool_(), nullable=True),
            _field("connection_epoch", pa.uint64(), nullable=True),
            _field("arrival_sequence", pa.uint64(), nullable=True),
        ],
        primary_key=("venue", "asset", "trade_id"),
        order_key=("event_time", "received_time", "source_sequence", "trade_id"),
        version=2,
    ),
    _make_spec(
        RecordType.FUNDING,
        [
            _field("funding_time", UTC_TIMESTAMP),
            _field("funding_rate", MARKET_DECIMAL),
            _field("funding_interval_seconds", pa.uint32()),
            _field("rate_kind", pa.string()),
            _field("mark_price", MARKET_DECIMAL, nullable=True),
            _field("oracle_price", MARKET_DECIMAL, nullable=True),
            _field("observation_id", pa.string()),
        ],
        primary_key=("venue", "asset", "funding_time", "rate_kind", "observation_id"),
        order_key=("event_time", "received_time", "source_sequence", "funding_time"),
        version=2,
    ),
    _make_spec(
        RecordType.OPEN_INTEREST,
        [
            _field("open_interest_quantity", MARKET_DECIMAL),
            _field("open_interest_notional", MARKET_DECIMAL, nullable=True),
            _field("mark_price", MARKET_DECIMAL, nullable=True),
        ],
        primary_key=("venue", "asset", "event_time", "received_time"),
        order_key=("event_time", "received_time", "source_sequence"),
    ),
    _make_spec(
        RecordType.FEE,
        [
            _field("fee_schedule_id", pa.string()),
            _field("scope", pa.string()),
            _field("instrument_kind", pa.string()),
            _field("maker_fee_bps", MARKET_DECIMAL),
            _field("taker_fee_bps", MARKET_DECIMAL),
            _field("effective_from", UTC_TIMESTAMP),
            _field("effective_to", UTC_TIMESTAMP, nullable=True),
            _field("fee_currency", pa.string(), nullable=True),
        ],
        primary_key=("venue", "asset", "fee_schedule_id", "effective_from"),
        order_key=("event_time", "received_time", "source_sequence", "effective_from"),
    ),
    _make_spec(
        RecordType.CONNECTION_EVENT,
        [
            _field("event_kind", pa.string()),
            _field("channel", pa.string(), nullable=True),
            _field("book_epoch_id", pa.string(), nullable=True),
            _field("reason", pa.string(), nullable=True),
            _field("expected_sequence", pa.uint64(), nullable=True),
            _field("observed_sequence", pa.uint64(), nullable=True),
            _field("resync_snapshot_id", pa.string(), nullable=True),
            _field("connection_epoch", pa.uint64(), nullable=True),
            _field("capture_epoch_id", pa.string(), nullable=True),
            _field("socket_role", pa.string(), nullable=True),
        ],
        primary_key=("venue", "asset", "connection_id", "event_time", "event_kind"),
        order_key=("event_time", "received_time", "source_sequence", "event_kind"),
        version=2,
    ),
    _make_spec(
        RecordType.INSTRUMENT_LIFECYCLE,
        [
            _field("source_symbol", pa.string()),
            _field("instrument_id", pa.string()),
            _field("instrument_kind", pa.string()),
            _field("status", pa.string()),
            _field("valid_from", UTC_TIMESTAMP),
            _field("valid_to", UTC_TIMESTAMP, nullable=True),
        ],
        primary_key=("venue", "asset", "source_symbol", "valid_from"),
        order_key=("event_time", "received_time", "source_sequence", "valid_from"),
    ),
    _make_spec(
        RecordType.CLOCK_SYNC,
        [
            _field("request_sent_time", UTC_TIMESTAMP),
            _field("response_received_time", UTC_TIMESTAMP),
            _field("server_time", UTC_TIMESTAMP),
            _field("round_trip_latency_ms", MARKET_DECIMAL),
            _field("estimated_clock_drift_ms", MARKET_DECIMAL),
            _field("drift_uncertainty_ms", MARKET_DECIMAL),
            _field("observation_id", pa.string()),
            _field("connection_epoch", pa.uint64(), nullable=True),
            _field("capture_epoch_id", pa.string(), nullable=True),
            _field("causal_valid_from", UTC_TIMESTAMP, nullable=True),
            _field("causal_valid_until", UTC_TIMESTAMP, nullable=True),
            _field("sample_status", pa.string(), nullable=True),
            _field("invalid_reason", pa.string(), nullable=True),
            _field("sampling_interval_ms", pa.uint64(), nullable=True),
            _field("max_age_ms", pa.uint64(), nullable=True),
            _field("max_uncertainty_ms", MARKET_DECIMAL, nullable=True),
        ],
        primary_key=("venue", "observation_id"),
        order_key=("event_time", "received_time", "observation_id"),
        version=2,
    ),
    _make_spec(
        RecordType.CLOCK_SYNC,
        [
            _field("request_sent_time", UTC_TIMESTAMP),
            _field("response_received_time", UTC_TIMESTAMP),
            _field("server_time", UTC_TIMESTAMP),
            _field("round_trip_latency_ms", MARKET_DECIMAL),
            _field("estimated_clock_drift_ms", MARKET_DECIMAL),
            _field("drift_uncertainty_ms", MARKET_DECIMAL),
            _field("observation_id", pa.string()),
            _field("connection_epoch", pa.uint64(), nullable=True),
            _field("capture_epoch_id", pa.string(), nullable=True),
            _field("causal_valid_from", UTC_TIMESTAMP, nullable=True),
            _field("causal_valid_until", UTC_TIMESTAMP, nullable=True),
            _field("sample_status", pa.string(), nullable=True),
            _field("invalid_reason", pa.string(), nullable=True),
            _field("sampling_interval_ms", pa.uint64(), nullable=True),
            _field("max_age_ms", pa.uint64(), nullable=True),
            _field("max_uncertainty_ms", MARKET_DECIMAL, nullable=True),
            _field("clock_schedule_overdue_ms", MARKET_DECIMAL, nullable=True),
            _field("single_flight_blocked_ms", MARKET_DECIMAL, nullable=True),
            _field("executor_submit_to_worker_start_ms", MARKET_DECIMAL, nullable=True),
            _field("worker_completion_to_supervisor_drain_ms", MARKET_DECIMAL, nullable=True),
            _field("transport_lock_wait_ms", MARKET_DECIMAL, nullable=True),
            _field("requests_adapter_header_elapsed_ms", MARKET_DECIMAL, nullable=True),
            _field("session_get_total_ms", MARKET_DECIMAL, nullable=True),
            _field("json_decode_ms", MARKET_DECIMAL, nullable=True),
            _field("diagnostic_prepare_ms", MARKET_DECIMAL, nullable=True),
            _field("diagnostic_finalize_ms", MARKET_DECIMAL, nullable=True),
            _field("new_urllib3_connection_object_created", pa.bool_(), nullable=True),
            _field("requests_session_reused", pa.bool_(), nullable=True),
            _field("urllib3_connection_identity", pa.string(), nullable=True),
            _field("urllib3_connection_reused", pa.bool_(), nullable=True),
            _field("tls_socket_identity", pa.string(), nullable=True),
            _field("tls_socket_reused", pa.bool_(), nullable=True),
            _field("tls_session_reused", pa.bool_(), nullable=True),
            _field("post_request_observation_current", pa.bool_(), nullable=True),
            _field("peer_ip", pa.string(), nullable=True),
            _field("peer_port", pa.uint16(), nullable=True),
            _field("socket_family", pa.string(), nullable=True),
            _field("response_cloudfront_pop", pa.string(), nullable=True),
            _field("response_cache", pa.string(), nullable=True),
        ],
        primary_key=("venue", "observation_id"),
        order_key=("event_time", "received_time", "observation_id"),
        version=3,
    ),
)

_LEGACY_V1_SPECS = (
    _make_spec(
        RecordType.WIRE_MESSAGE,
        [
            _field("connection_epoch", pa.uint64()),
            _field("arrival_sequence", pa.uint64()),
            _field("channel", pa.string(), nullable=True),
            _field("message_asset", pa.string(), nullable=True),
            _field("raw_message", pa.string()),
            _field("is_json", pa.bool_()),
            _field("payload_sha256", pa.string()),
        ],
        primary_key=(
            "venue",
            "connection_id",
            "connection_epoch",
            "arrival_sequence",
        ),
        order_key=(
            "event_time",
            "received_time",
            "connection_epoch",
            "arrival_sequence",
        ),
    ),
    _make_spec(
        RecordType.CANDLE,
        [
            _field("interval", pa.string()),
            _field("open_time", UTC_TIMESTAMP),
            _field("close_time", UTC_TIMESTAMP),
            _field("open", MARKET_DECIMAL),
            _field("high", MARKET_DECIMAL),
            _field("low", MARKET_DECIMAL),
            _field("close", MARKET_DECIMAL),
            _field("base_volume", MARKET_DECIMAL),
            _field("quote_volume", MARKET_DECIMAL, nullable=True),
            _field("trade_count", pa.uint64(), nullable=True),
            _field("is_final", pa.bool_()),
        ],
        primary_key=("venue", "asset", "interval", "open_time"),
        order_key=("event_time", "received_time", "source_sequence", "open_time"),
    ),
    _make_spec(
        RecordType.BBO,
        [
            _field("update_id", pa.string(), nullable=True),
            _field("bid_price", MARKET_DECIMAL),
            _field("bid_quantity", MARKET_DECIMAL),
            _field("ask_price", MARKET_DECIMAL),
            _field("ask_quantity", MARKET_DECIMAL),
        ],
        primary_key=("venue", "asset", "event_time", "received_time", "source_sequence"),
        order_key=("event_time", "received_time", "source_sequence"),
    ),
    _make_spec(
        RecordType.FUNDING,
        [
            _field("funding_time", UTC_TIMESTAMP),
            _field("funding_rate", MARKET_DECIMAL),
            _field("funding_interval_seconds", pa.uint32()),
            _field("rate_kind", pa.string()),
            _field("mark_price", MARKET_DECIMAL, nullable=True),
            _field("oracle_price", MARKET_DECIMAL, nullable=True),
        ],
        primary_key=("venue", "asset", "funding_time", "rate_kind"),
        order_key=("event_time", "received_time", "source_sequence", "funding_time"),
    ),
    _make_spec(
        RecordType.TRADE,
        [
            _field("trade_id", pa.string()),
            _field("aggressor_side", pa.string()),
            _field("price", MARKET_DECIMAL),
            _field("quantity", MARKET_DECIMAL),
            _field("quote_quantity", MARKET_DECIMAL, nullable=True),
            _field("is_liquidation", pa.bool_(), nullable=True),
        ],
        primary_key=("venue", "asset", "trade_id"),
        order_key=("event_time", "received_time", "source_sequence", "trade_id"),
    ),
    _make_spec(
        RecordType.CONNECTION_EVENT,
        [
            _field("event_kind", pa.string()),
            _field("channel", pa.string(), nullable=True),
            _field("book_epoch_id", pa.string(), nullable=True),
            _field("reason", pa.string(), nullable=True),
            _field("expected_sequence", pa.uint64(), nullable=True),
            _field("observed_sequence", pa.uint64(), nullable=True),
            _field("resync_snapshot_id", pa.string(), nullable=True),
        ],
        primary_key=("venue", "asset", "connection_id", "event_time", "event_kind"),
        order_key=("event_time", "received_time", "source_sequence", "event_kind"),
    ),
    _make_spec(
        RecordType.CLOCK_SYNC,
        [
            _field("request_sent_time", UTC_TIMESTAMP),
            _field("response_received_time", UTC_TIMESTAMP),
            _field("server_time", UTC_TIMESTAMP),
            _field("round_trip_latency_ms", MARKET_DECIMAL),
            _field("estimated_clock_drift_ms", MARKET_DECIMAL),
            _field("drift_uncertainty_ms", MARKET_DECIMAL),
            _field("observation_id", pa.string()),
        ],
        primary_key=("venue", "observation_id"),
        order_key=("event_time", "received_time", "observation_id"),
    ),
)


BREAKING_SCHEMA_TRANSITIONS: dict[tuple[RecordType, int, int], str] = {
    (RecordType.BBO, 1, 2): "nullable one-sided public book and collector-frame identity",
    (RecordType.CANDLE, 1, 2): "public observations retain revisions without fabricated finality",
    (RecordType.FUNDING, 1, 2): "received-time observations permit explicit source corrections",
}


_SCHEMA_REGISTRY = {(spec.record_type, spec.version): spec for spec in (*_V1_SPECS, *_LEGACY_V1_SPECS)}


def _normalize_record_type(record_type: RecordType | str) -> RecordType:
    try:
        return record_type if isinstance(record_type, RecordType) else RecordType(record_type)
    except ValueError:
        raise ValueError(f"unknown record type: {record_type}") from None


def schema_for(record_type: RecordType | str, version: int = 1) -> SchemaSpec:
    normalized = _normalize_record_type(record_type)
    try:
        return _SCHEMA_REGISTRY[(normalized, version)]
    except KeyError:
        raise ValueError(f"unknown schema version {version} for record type {normalized.value}") from None


def latest_schema_for(record_type: RecordType | str) -> SchemaSpec:
    normalized = _normalize_record_type(record_type)
    versions = [
        registered_version
        for registered_type, registered_version in _SCHEMA_REGISTRY
        if registered_type == normalized
    ]
    return _SCHEMA_REGISTRY[(normalized, max(versions))]


def registered_schemas() -> tuple[SchemaSpec, ...]:
    return tuple(
        sorted(
            _SCHEMA_REGISTRY.values(),
            key=lambda spec: (spec.record_type.value, spec.version),
        )
    )


def schema_fingerprint(schema_or_spec: pa.Schema | SchemaSpec) -> str:
    schema = schema_or_spec.schema if isinstance(schema_or_spec, SchemaSpec) else schema_or_spec
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _metadata_value(schema: pa.Schema, key: bytes) -> bytes | None:
    return (schema.metadata or {}).get(key)


def check_schema_evolution(previous: SchemaSpec, candidate: SchemaSpec) -> None:
    """Reject every change except appending nullable fields in a newer version."""

    def incompatible(reason: str) -> None:
        raise ValueError(f"incompatible schema evolution: {reason}")

    previous_name = _metadata_value(previous.schema, SCHEMA_NAME_METADATA)
    candidate_name = _metadata_value(candidate.schema, SCHEMA_NAME_METADATA)
    if previous.record_type != candidate.record_type or previous_name != candidate_name:
        incompatible("schema name changed")
    if not 1 <= candidate.version <= 65_535:
        incompatible("schema version must be between 1 and 65535")
    if candidate.version <= previous.version:
        incompatible("schema version must increase")
    if candidate.primary_key != previous.primary_key:
        incompatible("primary key changed")
    if candidate.order_key != previous.order_key:
        incompatible("order key changed")
    if len(set(candidate.schema.names)) != len(candidate.schema.names):
        incompatible("field names must remain unique")
    expected_candidate_version = str(candidate.version).encode()
    if _metadata_value(candidate.schema, SCHEMA_VERSION_METADATA) != expected_candidate_version:
        incompatible("schema metadata version does not match the candidate version")
    previous_metadata = dict(previous.schema.metadata or {})
    candidate_metadata = dict(candidate.schema.metadata or {})
    previous_metadata.pop(SCHEMA_VERSION_METADATA, None)
    candidate_metadata.pop(SCHEMA_VERSION_METADATA, None)
    if candidate_metadata != previous_metadata:
        incompatible("schema metadata changed")
    if len(candidate.schema) < len(previous.schema):
        incompatible("fields were removed")

    for index, old_field in enumerate(previous.schema):
        new_field = candidate.schema[index]
        if old_field.name != new_field.name:
            incompatible(f"field {old_field.name!r} was removed or reordered")
        if old_field.type != new_field.type:
            incompatible(f"field {old_field.name!r} changed type")
        if old_field.nullable != new_field.nullable:
            incompatible(f"field {old_field.name!r} changed nullability")
        if old_field.metadata != new_field.metadata:
            incompatible(f"field {old_field.name!r} changed metadata")

    for added_field in list(candidate.schema)[len(previous.schema) :]:
        if not added_field.nullable:
            incompatible(f"new field {added_field.name!r} must be nullable")
