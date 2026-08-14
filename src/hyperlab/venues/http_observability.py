from __future__ import annotations

import socket
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

_MAX_TEXT_LENGTH = 512


@dataclass(frozen=True, slots=True)
class HttpPathObservation:
    urllib3_connection_identity: str | None
    tls_socket_identity: str | None
    tls_session_reused: bool | None
    peer_ip: str | None
    peer_port: int | None
    socket_family: str | None
    response_cloudfront_pop: str | None
    response_cache: str | None


def _opaque_identity(value: object | None) -> str | None:
    if value is None:
        return None
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}:{id(value):x}"


def _safe_getattr(target: object | None, name: str) -> object | None:
    try:
        return getattr(target, name, None)
    except Exception:
        return None


def _address(value: object) -> tuple[str | None, int | None]:
    if not isinstance(value, tuple) or len(value) < 2:
        return None, None
    host, port = value[0], value[1]
    if not isinstance(host, str) or not isinstance(port, int):
        return None, None
    return host, port


def _peer_address(target: object | None) -> tuple[str | None, int | None]:
    getpeername = _safe_getattr(target, "getpeername")
    if not callable(getpeername):
        return None, None
    try:
        return _address(getpeername())
    except Exception:
        return None, None


def _family_name(value: object) -> str | None:
    if not isinstance(value, int):
        return None
    try:
        return socket.AddressFamily(value).name
    except ValueError:
        return f"AF_UNKNOWN_{value}"


def _header(response: object, name: str) -> str | None:
    headers = _safe_getattr(response, "headers")
    getter = _safe_getattr(headers, "get")
    if not callable(getter):
        return None
    try:
        value = getter(name)
    except Exception:
        return None
    if not isinstance(value, str) or not value:
        return None
    return value[:_MAX_TEXT_LENGTH]


class HttpPeerPathObserver:
    """Inspect only the peer and allowlisted CloudFront routing headers."""

    @staticmethod
    def observe_response(response: Any) -> HttpPathObservation:
        raw = _safe_getattr(response, "raw")
        connection = _safe_getattr(raw, "connection")
        if connection is None:
            connection = _safe_getattr(raw, "_connection")
        tls_socket = _safe_getattr(connection, "sock")
        peer_ip, peer_port = _peer_address(tls_socket)
        session_reused = _safe_getattr(tls_socket, "session_reused")
        if not isinstance(session_reused, bool):
            session_reused = None
        return HttpPathObservation(
            urllib3_connection_identity=_opaque_identity(connection),
            tls_socket_identity=_opaque_identity(tls_socket),
            tls_session_reused=session_reused,
            peer_ip=peer_ip,
            peer_port=peer_port,
            socket_family=_family_name(_safe_getattr(tls_socket, "family")),
            response_cloudfront_pop=_header(response, "X-Amz-Cf-Pop"),
            response_cache=_header(response, "X-Cache"),
        )


@dataclass(frozen=True, slots=True)
class ResolvedAddress:
    family: str
    ip: str
    port: int

    def as_dict(self) -> dict[str, object]:
        return {"family": self.family, "ip": self.ip, "port": self.port}


@dataclass(frozen=True, slots=True)
class DnsResolutionSnapshot:
    host: str
    port: int
    duration_ms: float
    addresses: tuple[ResolvedAddress, ...]
    error_type: str | None = None
    error_message: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "host": self.host,
            "port": self.port,
            "duration_ms": self.duration_ms,
            "addresses": [address.as_dict() for address in self.addresses],
            "error_type": self.error_type,
            "error_message": self.error_message,
        }


Resolver = Callable[..., Sequence[tuple[Any, ...]]]


def resolve_dns_snapshot(
    host: str,
    port: int,
    *,
    resolver: Resolver = socket.getaddrinfo,
    monotonic: Callable[[], float] = time.perf_counter,
) -> DnsResolutionSnapshot:
    """Resolve only for the standalone probe, never the collector request path."""

    started_at = monotonic()
    try:
        answers = resolver(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        completed_at = monotonic()
        return DnsResolutionSnapshot(
            host=host,
            port=port,
            duration_ms=max(completed_at - started_at, 0.0) * 1_000,
            addresses=(),
            error_type=type(exc).__name__,
            error_message=str(exc)[:_MAX_TEXT_LENGTH],
        )
    completed_at = monotonic()
    normalized: set[tuple[str, str, int]] = set()
    for answer in answers:
        if len(answer) < 5:
            continue
        family = _family_name(answer[0])
        ip, answer_port = _address(answer[4])
        if family is not None and ip is not None and answer_port is not None:
            normalized.add((family, ip, answer_port))
    addresses = tuple(
        ResolvedAddress(family=family, ip=ip, port=answer_port)
        for family, ip, answer_port in sorted(normalized)
    )
    return DnsResolutionSnapshot(
        host=host,
        port=port,
        duration_ms=max(completed_at - started_at, 0.0) * 1_000,
        addresses=addresses,
    )


class ClockProbeClient(Protocol):
    def clock_measurement(self) -> Any: ...

    def close(self) -> None: ...


_COMPARABLE_FIELDS = (
    "peer_ip",
    "peer_port",
    "socket_family",
    "response_cloudfront_pop",
    "response_cache",
    "requests_adapter_header_elapsed_ms",
    "session_get_total_ms",
    "requests_session_reused",
    "urllib3_connection_identity",
    "urllib3_connection_reused",
    "tls_socket_identity",
    "tls_socket_reused",
    "tls_session_reused",
)


def _diagnostic_value(diagnostics: object | None, field: str) -> object | None:
    return None if diagnostics is None else getattr(diagnostics, field, None)


def _probe(client: ClockProbeClient, *, mode: str, sample_index: int) -> dict[str, object]:
    try:
        measurement = client.clock_measurement()
    except Exception as exc:
        diagnostics = getattr(exc, "http_diagnostics", None)
        payload: dict[str, object] = {
            "mode": mode,
            "sample_index": sample_index,
            "outcome": "error",
            "exception_type": type(exc).__name__,
            "exception_message": str(exc)[:_MAX_TEXT_LENGTH],
        }
    else:
        diagnostics = getattr(measurement, "http_diagnostics", None)
        payload = {
            "mode": mode,
            "sample_index": sample_index,
            "outcome": "success",
            "request_sent_time": measurement.request_sent_time.isoformat(),
            "response_received_time": measurement.response_received_time.isoformat(),
            "round_trip_latency_ms": float(measurement.round_trip_latency_ms),
            "drift_uncertainty_ms": float(measurement.drift_uncertainty_ms),
            "estimated_clock_drift_ms": float(measurement.estimated_clock_drift_ms),
        }
    payload.update(
        {field: _diagnostic_value(diagnostics, field) for field in _COMPARABLE_FIELDS}
    )
    return payload


def _unique_values(samples: Sequence[Mapping[str, object]], field: str) -> list[str]:
    return sorted(
        {
            value
            for sample in samples
            if isinstance((value := sample.get(field)), str) and value
        }
    )


def diagnose_http_paths(
    *,
    samples: int,
    fresh_sample_count: int = 1,
    interval_seconds: float,
    client_factory: Callable[[], ClockProbeClient],
    resolver: Resolver = socket.getaddrinfo,
    sleeper: Callable[[float], None] = time.sleep,
    runtime_status: Mapping[str, object] | None = None,
    host: str = "fapi.binance.com",
    port: int = 443,
) -> dict[str, object]:
    if not 1 <= samples <= 240:
        raise ValueError("persistent diagnostic sample count must be between 1 and 240")
    if not 1 <= fresh_sample_count <= 3:
        raise ValueError("fresh diagnostic sample count must be between 1 and 3")
    if interval_seconds < 0:
        raise ValueError("diagnostic interval must be non-negative")
    dns = resolve_dns_snapshot(host, port, resolver=resolver)
    fresh_samples: list[dict[str, object]] = []
    persistent_samples: list[dict[str, object]] = []
    for sample_index in range(1, fresh_sample_count + 1):
        fresh_client = client_factory()
        try:
            fresh_samples.append(
                _probe(fresh_client, mode="fresh_connection", sample_index=sample_index)
            )
        finally:
            fresh_client.close()
    persistent_client = client_factory()
    try:
        for sample_index in range(1, samples + 1):
            persistent_samples.append(
                _probe(
                    persistent_client,
                    mode="persistent_session",
                    sample_index=sample_index,
                )
            )
            if sample_index < samples and interval_seconds:
                sleeper(interval_seconds)
    finally:
        persistent_client.close()

    runtime_latest: Mapping[str, object] | None = None
    if runtime_status is not None:
        observability = runtime_status.get("clock_observability")
        if isinstance(observability, Mapping):
            candidate = observability.get("latest")
            if isinstance(candidate, Mapping):
                runtime_latest = candidate
    runtime_peer = None if runtime_latest is None else runtime_latest.get("peer_ip")
    runtime_pop = (
        None if runtime_latest is None else runtime_latest.get("response_cloudfront_pop")
    )
    runtime_peer_ip = runtime_peer if isinstance(runtime_peer, str) and runtime_peer else None
    runtime_cloudfront_pop = runtime_pop if isinstance(runtime_pop, str) and runtime_pop else None
    fresh_ips = _unique_values(fresh_samples, "peer_ip")
    persistent_ips = _unique_values(persistent_samples, "peer_ip")
    fresh_pops = _unique_values(fresh_samples, "response_cloudfront_pop")
    persistent_pops = _unique_values(persistent_samples, "response_cloudfront_pop")
    return {
        "dns": dns.as_dict(),
        "fresh_samples": fresh_samples,
        "persistent_samples": persistent_samples,
        "runtime_latest": (
            None
            if runtime_latest is None
            else {field: runtime_latest.get(field) for field in _COMPARABLE_FIELDS}
        ),
        "comparison": {
            "runtime_selected_peer_ip": runtime_peer_ip,
            "runtime_cloudfront_pop": runtime_cloudfront_pop,
            "fresh_selected_peer_ips": fresh_ips,
            "persistent_selected_peer_ips": persistent_ips,
            "fresh_cloudfront_pops": fresh_pops,
            "persistent_cloudfront_pops": persistent_pops,
            "runtime_peer_matches_any_fresh": (
                None if runtime_peer_ip is None else runtime_peer_ip in fresh_ips
            ),
            "runtime_peer_matches_persistent": (
                None if runtime_peer_ip is None else runtime_peer_ip in persistent_ips
            ),
            "runtime_pop_matches_any_fresh": (
                None if runtime_cloudfront_pop is None else runtime_cloudfront_pop in fresh_pops
            ),
            "runtime_pop_matches_persistent": (
                None
                if runtime_cloudfront_pop is None
                else runtime_cloudfront_pop in persistent_pops
            ),
            "same_peer_or_pop_is_not_proof_of_same_edge": True,
        },
    }
