from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import LifoQueue
from threading import Event, Thread
from types import SimpleNamespace
from typing import Any

import pytest
import requests

from hyperlab.venues.binance import (
    BinancePublicHttpRequestError,
    BinancePublicRestClient,
    RequestsJsonTransport,
    clock_record,
)

BASE = datetime(2026, 8, 12, 12, tzinfo=UTC)


class ControlledClock:
    def __init__(self) -> None:
        self.current = BASE

    def now(self) -> datetime:
        return self.current

    def monotonic(self) -> float:
        return (self.current - BASE).total_seconds()

    def advance(self, milliseconds: int) -> None:
        self.current += timedelta(milliseconds=milliseconds)


class AdvancingLock:
    def __init__(self, clock: ControlledClock, wait_ms: int) -> None:
        self.clock = clock
        self.wait_ms = wait_ms

    def __enter__(self) -> AdvancingLock:
        self.clock.advance(self.wait_ms)
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback


class FakeTlsSocket:
    def __init__(self, *, session_reused: bool) -> None:
        self.session_reused = session_reused


class FakeConnection:
    def __init__(self, socket: FakeTlsSocket) -> None:
        self.sock = socket


class ObservedResponse:
    def __init__(
        self,
        payload: object,
        *,
        clock: ControlledClock,
        connection: FakeConnection,
        status_code: int = 200,
        decode_delay_ms: int = 0,
        json_error: Exception | None = None,
    ) -> None:
        self.payload = payload
        self.clock = clock
        self.status_code = status_code
        self.decode_delay_ms = decode_delay_ms
        self.json_error = json_error
        self.elapsed = timedelta(milliseconds=21)
        self.raw = SimpleNamespace(connection=connection)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self) -> object:
        self.clock.advance(self.decode_delay_ms)
        if self.json_error is not None:
            raise self.json_error
        return self.payload


class ObservedSession:
    def __init__(
        self,
        *,
        clock: ControlledClock,
        connection: FakeConnection,
        get_delay_ms: int,
        decode_delay_ms: int,
        status_code: int = 200,
        get_error: Exception | None = None,
        json_error: Exception | None = None,
        server_time: datetime = BASE,
    ) -> None:
        self.clock = clock
        self.connection = connection
        self.get_delay_ms = get_delay_ms
        self.decode_delay_ms = decode_delay_ms
        self.status_code = status_code
        self.get_error = get_error
        self.json_error = json_error
        self.server_time = server_time
        self.calls = 0
        self.close_calls = 0
        self.trust_env = True
        self.auth: object = ("ambient", "credential")
        self.headers = {"Authorization": "must-not-leak"}
        self.cookies = {"session": "must-not-leak"}
        self.params = {"apiKey": "must-not-leak"}
        self.proxies = {"https": "https://ambient.invalid"}
        self.cert: object = "ambient-client-cert"
        self.verify = False

    def get(self, url: str, **kwargs: object) -> ObservedResponse:
        del url, kwargs
        self.calls += 1
        self.clock.advance(self.get_delay_ms)
        if self.get_error is not None:
            raise self.get_error
        return ObservedResponse(
            {"serverTime": int(self.server_time.timestamp() * 1_000)},
            clock=self.clock,
            connection=self.connection,
            status_code=self.status_code,
            decode_delay_ms=self.decode_delay_ms,
            json_error=self.json_error,
        )

    def close(self) -> None:
        self.close_calls += 1


def _delayed_pool_snapshots(
    clock: ControlledClock,
    snapshots: tuple[tuple[int, int], ...],
    *,
    delay_ms: int,
) -> Any:
    remaining = iter(snapshots)

    def snapshot(_url: str) -> tuple[int, int]:
        clock.advance(delay_ms)
        return next(remaining)

    return snapshot


def test_diagnostic_introspection_is_outside_authoritative_clock_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = ControlledClock()
    socket = FakeTlsSocket(session_reused=False)
    connection = FakeConnection(socket)
    session = ObservedSession(
        clock=clock,
        connection=connection,
        get_delay_ms=67,
        decode_delay_ms=7,
        server_time=BASE + timedelta(milliseconds=67),
    )
    transport = RequestsJsonTransport(
        session=session,  # type: ignore[arg-type]
        monotonic=clock.monotonic,
    )
    transport._lock = AdvancingLock(clock, 10)  # type: ignore[assignment]
    monkeypatch.setattr(
        transport,
        "_pool_snapshot",
        _delayed_pool_snapshots(
            clock,
            ((3, 10), (4, 11), (4, 11), (4, 12)),
            delay_ms=25,
        ),
    )
    client = BinancePublicRestClient(transport=transport, clock=clock.now)

    try:
        measurement = client.clock_measurement()
        second = client._get("/fapi/v1/time")
    finally:
        client.close()

    assert measurement.round_trip_latency_ms == Decimal("84")
    assert measurement.drift_uncertainty_ms == Decimal("42")
    assert measurement.response_received_time - measurement.request_sent_time == timedelta(milliseconds=84)
    record = clock_record(
        measurement,
        "boundary-safe-clock",
        connection_id="binance-public-1",
        connection_epoch=1,
        capture_epoch_id="binance-capture-1",
    )
    assert record.row["sample_status"] == "valid"
    assert "http_diagnostics" not in record.row

    first_diagnostics = measurement.http_diagnostics
    assert first_diagnostics is not None
    assert first_diagnostics.outcome == "success"
    assert first_diagnostics.diagnostic_prepare_ms == pytest.approx(35)
    assert first_diagnostics.diagnostic_finalize_ms == pytest.approx(35)
    assert first_diagnostics.transport_lock_wait_ms == pytest.approx(10)
    assert first_diagnostics.session_get_total_ms == pytest.approx(67)
    assert first_diagnostics.json_decode_ms == pytest.approx(7)
    assert first_diagnostics.urllib3_connection_objects_created_total_before == 3
    assert first_diagnostics.urllib3_connection_objects_created_total_after == 4
    assert first_diagnostics.urllib3_connection_objects_created_delta == 1
    assert first_diagnostics.urllib3_requests_started_total_before == 10
    assert first_diagnostics.urllib3_requests_started_total_after == 11
    assert first_diagnostics.urllib3_requests_started_delta == 1
    assert first_diagnostics.new_urllib3_connection_object_created is True
    assert first_diagnostics.pool_connection_delta == 1
    assert first_diagnostics.urllib3_pool_object_delta == 1
    assert first_diagnostics.requests_session_reused is False
    assert first_diagnostics.urllib3_connection_identity is not None
    assert first_diagnostics.urllib3_connection_reused is None
    assert first_diagnostics.tls_socket_identity is not None
    assert first_diagnostics.tls_socket_reused is None
    assert first_diagnostics.tls_session_reused is False
    assert first_diagnostics.request_completion_sequence == 1
    assert first_diagnostics.finalization_completion_sequence == 1
    assert first_diagnostics.post_request_observation_current is True

    second_diagnostics = second.http_diagnostics
    assert second_diagnostics is not None
    assert second.response_received_time - second.request_sent_time == timedelta(milliseconds=84)
    assert second_diagnostics.requests_session_reused is True
    assert second_diagnostics.urllib3_connection_objects_created_delta == 0
    assert second_diagnostics.urllib3_requests_started_delta == 1
    assert second_diagnostics.new_urllib3_connection_object_created is False
    assert second_diagnostics.urllib3_connection_reused is True
    assert second_diagnostics.tls_socket_reused is True
    assert second_diagnostics.tls_session_reused is None
    assert second_diagnostics.request_completion_sequence == 2
    assert second_diagnostics.finalization_completion_sequence == 2
    assert second_diagnostics.post_request_observation_current is True


@pytest.mark.parametrize(
    (
        "failure_kind",
        "expected_stage",
        "expected_exception_type",
        "expected_raised_type",
        "expected_boundary_ms",
        "expected_json_ms",
        "expected_adapter_ms",
    ),
    (
        ("session_get", "session_get", "Timeout", requests.Timeout, 47, None, None),
        ("http_status", "http_status", "RuntimeError", RuntimeError, 47, None, 21),
        ("json_decode", "json_decode", "ValueError", ValueError, 58, 11, 21),
    ),
)
def test_http_failures_retain_partial_diagnostics_outside_clock_boundary(
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
    expected_stage: str,
    expected_exception_type: str,
    expected_raised_type: type[Exception],
    expected_boundary_ms: int,
    expected_json_ms: int | None,
    expected_adapter_ms: int | None,
) -> None:
    clock = ControlledClock()
    connection = FakeConnection(FakeTlsSocket(session_reused=False))
    session = ObservedSession(
        clock=clock,
        connection=connection,
        get_delay_ms=40,
        decode_delay_ms=11 if failure_kind == "json_decode" else 0,
        status_code=429 if failure_kind == "http_status" else 200,
        get_error=requests.Timeout("simulated timeout") if failure_kind == "session_get" else None,
        json_error=ValueError("simulated JSON failure") if failure_kind == "json_decode" else None,
    )
    transport = RequestsJsonTransport(
        session=session,  # type: ignore[arg-type]
        monotonic=clock.monotonic,
    )
    transport._lock = AdvancingLock(clock, 7)  # type: ignore[assignment]
    monkeypatch.setattr(
        transport,
        "_pool_snapshot",
        _delayed_pool_snapshots(clock, ((1, 4), (1, 5)), delay_ms=9),
    )
    client = BinancePublicRestClient(transport=transport, clock=clock.now)

    try:
        with pytest.raises(expected_raised_type) as raised:
            client.clock_measurement()
    finally:
        client.close()

    error = raised.value
    request_sent_time = vars(error)["request_sent_time"]
    response_received_time = vars(error)["response_received_time"]
    assert response_received_time - request_sent_time == timedelta(
        milliseconds=expected_boundary_ms
    )
    diagnostics = vars(error)["http_diagnostics"]
    assert diagnostics.outcome == "failure"
    assert diagnostics.diagnostic_prepare_ms == pytest.approx(16)
    assert diagnostics.diagnostic_finalize_ms == pytest.approx(16)
    assert diagnostics.failure_stage == expected_stage
    assert diagnostics.exception_type == expected_exception_type
    assert diagnostics.transport_lock_wait_ms == pytest.approx(7)
    assert diagnostics.session_get_total_ms == pytest.approx(40)
    assert diagnostics.json_decode_ms == (
        None if expected_json_ms is None else pytest.approx(expected_json_ms)
    )
    assert diagnostics.requests_adapter_header_elapsed_ms == (
        None if expected_adapter_ms is None else pytest.approx(expected_adapter_ms)
    )
    assert diagnostics.urllib3_connection_objects_created_delta == 0
    assert diagnostics.urllib3_requests_started_delta == 1
    assert diagnostics.new_urllib3_connection_object_created is False
    assert diagnostics.requests_session_reused is False
    assert diagnostics.request_completion_sequence == 1
    assert diagnostics.finalization_completion_sequence == 1
    assert diagnostics.post_request_observation_current is True
    if failure_kind == "session_get":
        assert diagnostics.urllib3_connection_identity is None
        assert isinstance(error, requests.Timeout)
    else:
        assert diagnostics.urllib3_connection_identity is not None

class NonAnnotatableError(RuntimeError):
    def __setattr__(self, name: str, value: object) -> None:
        if name in {
            "request_sent_time",
            "response_received_time",
            "http_diagnostics",
        }:
            raise AttributeError("diagnostic annotations refused")
        super().__setattr__(name, value)


class LocalKeepAliveHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        body = b'{"serverTime":1786536000000}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def log_message(self, format: str, *args: object) -> None:
        del format, args


def test_unannotatable_transport_error_uses_stable_wrapper_with_original_cause() -> None:
    clock = ControlledClock()
    original = NonAnnotatableError("immutable diagnostic surface")
    session = ObservedSession(
        clock=clock,
        connection=FakeConnection(FakeTlsSocket(session_reused=False)),
        get_delay_ms=5,
        decode_delay_ms=0,
        get_error=original,
    )
    transport = RequestsJsonTransport(
        session=session,  # type: ignore[arg-type]
        monotonic=clock.monotonic,
    )
    client = BinancePublicRestClient(transport=transport, clock=clock.now)

    try:
        with pytest.raises(BinancePublicHttpRequestError) as raised:
            client.clock_measurement()
    finally:
        client.close()

    error = raised.value
    assert error.original_exception is original
    assert error.__cause__ is original
    assert error.http_diagnostics.outcome == "failure"
    assert error.http_diagnostics.failure_stage == "session_get"
    assert error.http_diagnostics.post_request_observation_current is True


def test_pool_queue_identity_lookup_does_not_remove_idle_connection() -> None:
    pooled_connection = FakeConnection(FakeTlsSocket(session_reused=False))
    direct_connection = FakeConnection(FakeTlsSocket(session_reused=True))
    connection_queue: LifoQueue[object | None] = LifoQueue(maxsize=3)
    connection_queue.put(None)
    connection_queue.put(pooled_connection)
    response = SimpleNamespace(
        raw=SimpleNamespace(
            _pool=SimpleNamespace(pool=connection_queue),
            connection=direct_connection,
        )
    )
    before = tuple(connection_queue.queue)

    evidence = RequestsJsonTransport._transport_identity_evidence(response)

    assert tuple(connection_queue.queue) == before
    assert connection_queue.qsize() == 2
    assert evidence.urllib3_connection_identity == RequestsJsonTransport._opaque_identity(
        pooled_connection
    )
    assert evidence.tls_socket_identity == RequestsJsonTransport._opaque_identity(
        pooled_connection.sock
    )
    assert evidence.tls_session_reused is False


def test_local_http_keepalive_exposes_real_urllib3_connection_reuse() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), LocalKeepAliveHandler)
    server.daemon_threads = True
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}/time"
    transport = RequestsJsonTransport()

    def request_diagnostics() -> Any:
        transport.prepare_diagnostics(url)
        transport.get_json(url, {}, 2.0)
        diagnostics = transport.consume_diagnostics()
        assert diagnostics is not None
        return diagnostics

    try:
        first = request_diagnostics()
        second = request_diagnostics()
    finally:
        transport.close()
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)

    assert not server_thread.is_alive()
    assert first.urllib3_connection_objects_created_delta == 1
    assert first.urllib3_requests_started_delta == 1
    assert first.urllib3_connection_identity is not None
    assert first.urllib3_connection_reused is None
    assert first.tls_session_reused is None
    assert first.peer_ip == "127.0.0.1"
    assert first.peer_port == server.server_address[1]
    assert first.socket_family == "AF_INET"
    assert second.urllib3_connection_objects_created_delta == 0
    assert second.urllib3_requests_started_delta == 1
    assert second.urllib3_connection_identity == first.urllib3_connection_identity
    assert second.urllib3_connection_reused is True
    assert second.tls_socket_identity == first.tls_socket_identity
    assert second.tls_socket_reused is True
    assert second.tls_session_reused is None
    assert second.peer_ip == first.peer_ip
    assert second.peer_port == first.peer_port
    assert second.socket_family == first.socket_family


def test_later_request_completion_makes_post_request_evidence_indeterminate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = ControlledClock()
    connection = FakeConnection(FakeTlsSocket(session_reused=False))
    session = ObservedSession(
        clock=clock,
        connection=connection,
        get_delay_ms=0,
        decode_delay_ms=0,
    )
    transport = RequestsJsonTransport(
        session=session,  # type: ignore[arg-type]
        monotonic=clock.monotonic,
    )
    monkeypatch.setattr(
        transport,
        "_pool_snapshot",
        lambda _url: (1, session.calls),
    )
    first_completed = Event()
    second_finalized = Event()
    results: dict[str, Any] = {}
    errors: list[BaseException] = []

    def first_worker() -> None:
        try:
            transport.prepare_diagnostics("https://example.test/time")
            transport.get_json("https://example.test/time", {}, 1.0)
            first_completed.set()
            if not second_finalized.wait(timeout=5):
                raise AssertionError("second request did not finalize")
            results["first"] = transport.consume_diagnostics()
        except BaseException as exc:
            errors.append(exc)
            first_completed.set()

    def second_worker() -> None:
        try:
            if not first_completed.wait(timeout=5):
                raise AssertionError("first request did not complete")
            transport.prepare_diagnostics("https://example.test/time")
            transport.get_json("https://example.test/time", {}, 1.0)
            results["second"] = transport.consume_diagnostics()
        except BaseException as exc:
            errors.append(exc)
        finally:
            second_finalized.set()

    first_thread = Thread(target=first_worker)
    second_thread = Thread(target=second_worker)
    first_thread.start()
    second_thread.start()
    first_thread.join(timeout=5)
    second_thread.join(timeout=5)
    transport.close()

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert errors == []
    first = results["first"]
    second = results["second"]
    assert first.request_completion_sequence == 1
    assert first.finalization_completion_sequence == 2
    assert first.post_request_observation_current is False
    assert first.urllib3_connection_objects_created_delta is None
    assert first.urllib3_requests_started_delta is None
    assert first.new_urllib3_connection_object_created is None
    assert first.urllib3_connection_identity is None
    assert first.urllib3_connection_reused is None
    assert first.tls_socket_identity is None
    assert first.tls_socket_reused is None
    assert first.tls_session_reused is None
    assert second.request_completion_sequence == 2
    assert second.finalization_completion_sequence == 2
    assert second.post_request_observation_current is True
    assert second.urllib3_connection_objects_created_delta == 0
    assert second.urllib3_requests_started_delta == 1


def test_interleaved_request_makes_counter_window_delta_indeterminate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = ControlledClock()
    connection = FakeConnection(FakeTlsSocket(session_reused=False))
    session = ObservedSession(
        clock=clock,
        connection=connection,
        get_delay_ms=0,
        decode_delay_ms=0,
    )
    transport = RequestsJsonTransport(
        session=session,  # type: ignore[arg-type]
        monotonic=clock.monotonic,
    )
    monkeypatch.setattr(
        transport,
        "_pool_snapshot",
        lambda _url: (1, session.calls),
    )
    first_prepared = Event()
    second_finalized = Event()
    results: dict[str, Any] = {}
    errors: list[BaseException] = []

    def delayed_worker() -> None:
        try:
            transport.prepare_diagnostics("https://example.test/time")
            first_prepared.set()
            if not second_finalized.wait(timeout=5):
                raise AssertionError("interleaved request did not finalize")
            transport.get_json("https://example.test/time", {}, 1.0)
            results["delayed"] = transport.consume_diagnostics()
        except BaseException as exc:
            errors.append(exc)
            first_prepared.set()

    def interleaved_worker() -> None:
        try:
            if not first_prepared.wait(timeout=5):
                raise AssertionError("delayed request did not prepare")
            transport.prepare_diagnostics("https://example.test/time")
            transport.get_json("https://example.test/time", {}, 1.0)
            results["interleaved"] = transport.consume_diagnostics()
        except BaseException as exc:
            errors.append(exc)
        finally:
            second_finalized.set()

    delayed_thread = Thread(target=delayed_worker)
    interleaved_thread = Thread(target=interleaved_worker)
    delayed_thread.start()
    interleaved_thread.start()
    delayed_thread.join(timeout=5)
    interleaved_thread.join(timeout=5)
    transport.close()

    assert not delayed_thread.is_alive()
    assert not interleaved_thread.is_alive()
    assert errors == []
    delayed = results["delayed"]
    assert delayed.request_completion_sequence == 2
    assert delayed.finalization_completion_sequence == 2
    assert delayed.post_request_observation_current is True
    assert delayed.urllib3_connection_identity is not None
    assert delayed.urllib3_connection_objects_created_delta is None
    assert delayed.urllib3_requests_started_delta is None
    assert delayed.new_urllib3_connection_object_created is None
