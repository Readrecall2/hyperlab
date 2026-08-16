from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

from hyperlab.collector.models import ParsedMessage, WireEnvelope


def _utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{label} must be an explicit UTC timestamp")
    return value


@dataclass(frozen=True, slots=True)
class NormalizedInstrument:
    """Explicit mapping from one venue contract to HyperLab base-asset units."""

    venue: str
    source_symbol: str
    asset: str
    base_asset: str
    quote_asset: str
    instrument_kind: str
    contract_kind: str
    quantity_multiplier: Decimal
    price_tick: Decimal
    quantity_step: Decimal
    status: str

    def __post_init__(self) -> None:
        if self.instrument_kind != "perp":
            raise ValueError("reference venue instruments must be perpetuals")
        if self.contract_kind != "linear":
            raise ValueError("only explicitly linear contracts are supported")
        if self.quantity_multiplier <= 0 or self.price_tick <= 0 or self.quantity_step <= 0:
            raise ValueError("instrument multipliers and increments must be positive")

    def normalize_quantity(self, source_quantity: object) -> Decimal:
        quantity = Decimal(str(source_quantity)) * self.quantity_multiplier
        if not quantity.is_finite() or quantity < 0:
            raise ValueError(f"invalid source quantity: {source_quantity!r}")
        return quantity


@dataclass(frozen=True, slots=True)
class HttpRequestDiagnostics:
    """Optional userspace HTTP timings; never an input to clock validity."""

    transport_lock_wait_ms: float | None
    requests_adapter_header_elapsed_ms: float | None
    session_get_total_ms: float | None
    json_decode_ms: float | None
    # Constructor names are retained for compatibility. The precise urllib3
    # counter aliases below are the public observability labels.
    pool_connections_before: int | None
    pool_connections_after: int | None
    pool_connection_delta: int | None
    pool_requests_before: int | None
    pool_requests_after: int | None
    pool_request_delta: int | None
    new_pool_connection_created: bool | None
    outcome: str = "success"
    failure_stage: str | None = None
    exception_type: str | None = None
    requests_session_reused: bool = False
    urllib3_connection_identity: str | None = None
    urllib3_connection_reused: bool | None = None
    tls_socket_identity: str | None = None
    tls_socket_reused: bool | None = None
    tls_session_reused: bool | None = None
    diagnostic_prepare_ms: float | None = None
    diagnostic_finalize_ms: float | None = None
    request_completion_sequence: int | None = None
    finalization_completion_sequence: int | None = None
    post_request_observation_current: bool | None = None
    peer_ip: str | None = None
    peer_port: int | None = None
    socket_family: str | None = None
    response_cloudfront_pop: str | None = None
    response_cache: str | None = None
    request_boundary_monotonic_elapsed_ms: float | None = None
    request_boundary_thread_cpu_ms: float | None = None
    request_boundary_thread_runqueue_wait_ms: float | None = None
    request_boundary_thread_timeslice_delta: int | None = None
    requests_session_request_ordinal: int | None = None
    urllib3_connection_observed_age_ms: float | None = None
    tls_socket_observed_age_ms: float | None = None

    def __post_init__(self) -> None:
        timings = (
            self.transport_lock_wait_ms,
            self.session_get_total_ms,
            self.json_decode_ms,
            self.diagnostic_prepare_ms,
            self.diagnostic_finalize_ms,
            self.request_boundary_monotonic_elapsed_ms,
            self.request_boundary_thread_cpu_ms,
            self.request_boundary_thread_runqueue_wait_ms,
            self.urllib3_connection_observed_age_ms,
            self.tls_socket_observed_age_ms,
        )
        if any(value is not None and value < 0 for value in timings):
            raise ValueError("HTTP diagnostic timings must be non-negative")
        if (
            self.requests_adapter_header_elapsed_ms is not None
            and self.requests_adapter_header_elapsed_ms < 0
        ):
            raise ValueError("Requests adapter/header elapsed time must be non-negative")
        counters = (
            self.pool_connections_before,
            self.pool_connections_after,
            self.pool_requests_before,
            self.pool_requests_after,
        )
        if any(value is not None and value < 0 for value in counters):
            raise ValueError("HTTP pool diagnostic counters must be non-negative")
        sequences = (
            self.request_completion_sequence,
            self.finalization_completion_sequence,
        )
        if any(value is not None and value < 0 for value in sequences):
            raise ValueError("HTTP diagnostic completion sequences must be non-negative")
        if self.request_boundary_thread_timeslice_delta is not None and (
            isinstance(self.request_boundary_thread_timeslice_delta, bool)
            or not isinstance(self.request_boundary_thread_timeslice_delta, int)
            or self.request_boundary_thread_timeslice_delta < 0
        ):
            raise ValueError("request boundary timeslice delta must be a non-negative integer")
        if self.requests_session_request_ordinal is not None and (
            isinstance(self.requests_session_request_ordinal, bool)
            or not isinstance(self.requests_session_request_ordinal, int)
            or self.requests_session_request_ordinal <= 0
        ):
            raise ValueError("Requests session request ordinal must be a positive integer")
        if self.outcome not in {"success", "failure"}:
            raise ValueError("HTTP diagnostic outcome must be success or failure")
        if self.outcome == "success" and (self.failure_stage is not None or self.exception_type is not None):
            raise ValueError("successful HTTP diagnostics cannot carry failure details")
        if self.outcome == "failure" and (self.failure_stage is None or self.exception_type is None):
            raise ValueError("failed HTTP diagnostics require a stage and exception type")
        identities = (
            self.urllib3_connection_identity,
            self.tls_socket_identity,
        )
        ports = (self.peer_port,)
        if any(value is not None and not 0 <= value <= 65_535 for value in ports):
            raise ValueError("HTTP transport ports must be between 0 and 65535")
        labels = (
            self.peer_ip,
            self.socket_family,
            self.response_cloudfront_pop,
            self.response_cache,
        )
        if any(value is not None and not value for value in labels):
            raise ValueError("HTTP transport labels must not be empty")
        if any(value is not None and not value for value in identities):
            raise ValueError("HTTP transport identities must not be empty")

    @property
    def urllib3_connection_objects_created_total_before(self) -> int | None:
        return self.pool_connections_before

    @property
    def urllib3_connection_objects_created_total_after(self) -> int | None:
        return self.pool_connections_after

    @property
    def urllib3_connection_objects_created_delta(self) -> int | None:
        return self.pool_connection_delta

    @property
    def urllib3_requests_started_total_before(self) -> int | None:
        return self.pool_requests_before

    @property
    def urllib3_requests_started_total_after(self) -> int | None:
        return self.pool_requests_after

    @property
    def urllib3_requests_started_delta(self) -> int | None:
        return self.pool_request_delta

    @property
    def new_urllib3_connection_object_created(self) -> bool | None:
        return self.new_pool_connection_created

    # Deprecated ambiguous aliases retained only for compatibility.
    @property
    def urllib3_pool_objects_before(self) -> int | None:
        return self.pool_connections_before

    @property
    def urllib3_pool_objects_after(self) -> int | None:
        return self.pool_connections_after

    @property
    def urllib3_pool_object_delta(self) -> int | None:
        return self.pool_connection_delta

    @property
    def urllib3_pool_requests_before(self) -> int | None:
        return self.pool_requests_before

    @property
    def urllib3_pool_requests_after(self) -> int | None:
        return self.pool_requests_after

    @property
    def urllib3_pool_request_delta(self) -> int | None:
        return self.pool_request_delta

    @property
    def new_urllib3_pool_object_created(self) -> bool | None:
        return self.new_pool_connection_created


@dataclass(frozen=True, slots=True)
class ClockMeasurement:
    venue: str
    request_sent_time: datetime
    response_received_time: datetime
    server_time: datetime
    round_trip_latency_ms: Decimal
    estimated_clock_drift_ms: Decimal
    drift_uncertainty_ms: Decimal
    http_diagnostics: HttpRequestDiagnostics | None = None

    def adjusted_one_way_latency_ms(self, source_time: datetime, received_time: datetime) -> Decimal:
        """Return signed latency; negative values expose bad clocks instead of being clipped."""

        _utc(source_time, label="source_time")
        _utc(received_time, label="received_time")
        apparent = Decimal(str((received_time - source_time).total_seconds() * 1_000))
        return apparent - self.estimated_clock_drift_ms


def measure_clock(
    venue: str,
    *,
    request_sent_time: datetime,
    response_received_time: datetime,
    server_time: datetime,
    http_diagnostics: HttpRequestDiagnostics | None = None,
) -> ClockMeasurement:
    sent = _utc(request_sent_time, label="request_sent_time")
    received = _utc(response_received_time, label="response_received_time")
    server = _utc(server_time, label="server_time")
    if received < sent:
        raise ValueError("clock response cannot precede the request")
    round_trip_ms = Decimal(str((received - sent).total_seconds() * 1_000))
    midpoint = sent + (received - sent) / 2
    # local - server: add this value to apparent receive-minus-source latency.
    drift_ms = Decimal(str((midpoint - server).total_seconds() * 1_000))
    return ClockMeasurement(
        venue=venue,
        request_sent_time=sent,
        response_received_time=received,
        server_time=server,
        round_trip_latency_ms=round_trip_ms,
        estimated_clock_drift_ms=drift_ms,
        drift_uncertainty_ms=round_trip_ms / 2,
        http_diagnostics=http_diagnostics,
    )


class PublicVenueConnector(Protocol):
    """Replay-safe parsing boundary implemented by every public venue adapter."""

    venue: str

    def parse_message(self, envelope: WireEnvelope) -> ParsedMessage: ...

    def instrument_for_asset(self, asset: str) -> NormalizedInstrument: ...
