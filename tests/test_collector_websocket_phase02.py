from __future__ import annotations

import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import websocket

from hyperlab.collector.websocket import (
    ReceivedWireMessage,
    WebsocketClientFactory,
    WebsocketClientSocket,
)


class FakeConnection:
    def __init__(self, messages: Iterator[object] | None = None) -> None:
        self._messages = messages
        self._closed = threading.Event()
        self.timeout: float | None = None
        self.sent: list[str] = []
        self.close_calls = 0

    def settimeout(self, timeout: float) -> None:
        self.timeout = timeout

    def send(self, message: str) -> None:
        self.sent.append(message)

    def recv(self) -> object:
        if self._closed.wait(0.01):
            raise ConnectionError("closed")
        if self._messages is None:
            raise websocket.WebSocketTimeoutException()
        try:
            item = next(self._messages)
        except StopIteration:
            self._messages = None
            raise websocket.WebSocketTimeoutException() from None
        if isinstance(item, BaseException):
            raise item
        return item

    def close(self) -> None:
        self.close_calls += 1
        self._closed.set()


class FakeRecvDataConnection(FakeConnection):
    def recv_data(self) -> tuple[int, object]:
        result = super().recv()
        assert isinstance(result, tuple) and len(result) == 2
        opcode, payload = result
        assert isinstance(opcode, int)
        return opcode, payload


def test_background_reader_accepts_text_and_decodes_bytes() -> None:
    connected_at = datetime(2026, 8, 12, 11, 59, 59, 999000, tzinfo=UTC)
    first_received_at = datetime(2026, 8, 12, 12, 0, tzinfo=UTC)
    second_received_at = first_received_at + timedelta(milliseconds=1)
    reception_times = iter((connected_at, first_received_at, second_received_at))
    connection = FakeConnection(iter(("first", b"second")))
    socket = WebsocketClientSocket(
        connection,
        queue_capacity=2,
        clock=lambda: next(reception_times),
    )
    try:
        assert socket.receive(0.2) == ReceivedWireMessage("first", first_received_at)
        assert socket.receive(0.2) == ReceivedWireMessage("second", second_received_at)
        socket.send_json({"method": "ping", "nested": {"enabled": True}})
        assert connection.sent == ['{"method":"ping","nested":{"enabled":true}}']
        assert connection.timeout == 1.0
    finally:
        socket.close()


def test_socket_telemetry_reports_labeled_queue_timing_without_changing_wire_time() -> None:
    connected_at = datetime(2026, 8, 12, 11, 59, 59, tzinfo=UTC)
    received_at = connected_at + timedelta(seconds=1)
    wall_times = iter((connected_at, received_at))
    monotonic_times = iter((100_000_000, 102_000_000, 110_000_000, 120_000_000))
    socket = WebsocketClientSocket(
        FakeConnection(iter(("wire",))),
        queue_capacity=2,
        clock=lambda: next(wall_times),
        monotonic_ns=lambda: next(monotonic_times),
        venue="binance_usdm",
        socket_role="market",
        reader_name="test-binance-market-reader",
    )
    try:
        received = socket.receive(0.2)
        assert received == ReceivedWireMessage("wire", received_at)
        assert received is not None
        assert received.received_monotonic_ns == 100_000_000

        telemetry = socket.telemetry_snapshot()

        assert telemetry["venue"] == "binance_usdm"
        assert telemetry["socket_role"] == "market"
        assert telemetry["reader_name"] == "test-binance-market-reader"
        assert telemetry["queue_depth"] == 0
        assert telemetry["queue_high_water"] == 1
        assert telemetry["oldest_message_age_ms"] is None
        assert telemetry["latest_message_received_age_ms"] == 20.0
        assert telemetry["overflow_count"] == 0
        assert telemetry["terminal_reason"] is None
        enqueue = telemetry["enqueue_delay_ms"]
        residence = telemetry["dequeue_residence_ms"]
        assert isinstance(enqueue, dict)
        assert isinstance(residence, dict)
        assert enqueue["p99_ms"] == 2.0
        assert residence["p99_ms"] == 10.0
    finally:
        socket.close()


def test_paused_reader_captures_connection_time_before_first_wire() -> None:
    connected_at = datetime(2026, 8, 12, 11, 59, 59, 999000, tzinfo=UTC)
    first_received_at = connected_at + timedelta(milliseconds=1)
    timestamps = iter((connected_at, first_received_at))
    connection = FakeConnection(iter(("first",)))
    socket = WebsocketClientSocket(
        connection,
        queue_capacity=1,
        clock=lambda: next(timestamps),
        start_immediately=False,
    )
    try:
        assert socket.connected_at == connected_at
        assert not socket._reader.is_alive()

        socket.start_receiving()

        assert socket.receive(0.2) == ReceivedWireMessage("first", first_received_at)
        assert socket.connected_at < first_received_at
    finally:
        socket.close()


def test_paused_reader_can_close_before_start_without_joining() -> None:
    connection = FakeConnection(iter(("must-not-be-read",)))
    socket = WebsocketClientSocket(
        connection,
        queue_capacity=1,
        start_immediately=False,
    )

    socket.close()

    assert connection.close_calls == 1
    assert not socket._reader.is_alive()
    with pytest.raises(RuntimeError, match="cannot start a closed"):
        socket.start_receiving()


def test_default_reader_names_are_unique_and_include_venue_and_role() -> None:
    first = WebsocketClientSocket(
        FakeConnection(),
        queue_capacity=1,
        venue="binance_usdm",
        socket_role="market",
        start_immediately=False,
    )
    second = WebsocketClientSocket(
        FakeConnection(),
        queue_capacity=1,
        venue="binance_usdm",
        socket_role="market",
        start_immediately=False,
    )
    try:
        assert first.reader_name.startswith("hyperlab-ws-binance_usdm-market-")
        assert second.reader_name.startswith("hyperlab-ws-binance_usdm-market-")
        assert first.reader_name != second.reader_name
        assert first._reader.name == first.reader_name
        assert second._reader.name == second.reader_name
    finally:
        first.close()
        second.close()


def test_receive_timeout_and_invalid_timeout_are_explicit() -> None:
    connection = FakeConnection()
    socket = WebsocketClientSocket(connection, queue_capacity=1)
    try:
        assert socket.receive(0.02) is None
        with pytest.raises(ValueError, match="timeout_seconds must be positive"):
            socket.receive(0.0)
    finally:
        socket.close()


def test_close_stops_reader_and_closes_connection() -> None:
    connection = FakeConnection()
    socket = WebsocketClientSocket(connection, queue_capacity=1)

    socket.close()

    assert connection.close_calls == 1
    assert not socket._reader.is_alive()
    socket.close()
    assert connection.close_calls == 2


def test_terminal_reader_error_is_raised_to_consumer() -> None:
    connection = FakeConnection(iter((RuntimeError("wire failed"),)))
    socket = WebsocketClientSocket(connection, queue_capacity=1)
    try:
        with pytest.raises(RuntimeError, match="wire failed"):
            socket.receive(0.2)
    finally:
        socket.close()


def test_invalid_utf8_bytes_become_a_visible_terminal_error() -> None:
    connection = FakeConnection(iter((b"\xff",)))
    socket = WebsocketClientSocket(connection, queue_capacity=1)
    try:
        with pytest.raises(UnicodeDecodeError):
            socket.receive(0.2)
    finally:
        socket.close()


def test_non_utc_receive_timestamp_becomes_a_visible_terminal_error() -> None:
    connection = FakeConnection(iter(("message",)))
    socket = WebsocketClientSocket(
        connection,
        queue_capacity=1,
        clock=iter(
            (
                datetime(2026, 8, 12, 12, 0, tzinfo=UTC),
                datetime(2026, 8, 12, 12, 0),
            )
        ).__next__,
    )
    try:
        with pytest.raises(ValueError, match="received_time must be timezone-aware"):
            socket.receive(0.2)
    finally:
        socket.close()


def test_close_opcode_preserves_peer_details_after_draining_accepted_fifo() -> None:
    connection = FakeRecvDataConnection(
        iter(
            (
                (websocket.ABNF.OPCODE_TEXT, "accepted-before-close"),
                (websocket.ABNF.OPCODE_CLOSE, b"\x03\xe9maintenance"),
            )
        )
    )
    socket = WebsocketClientSocket(connection, queue_capacity=2)
    try:
        socket._reader.join(timeout=0.5)
        assert not socket._reader.is_alive()
        assert socket._messages.qsize() == 1
        received = socket.receive(0.2)
        assert received is not None
        assert received.raw_message == "accepted-before-close"
        with pytest.raises(ConnectionError, match=r"code=1001.*reason=maintenance"):
            socket.receive(0.2)
        telemetry = socket.telemetry_snapshot()
        assert telemetry["terminal_origin"] == "peer_close_frame"
        assert telemetry["terminal_close_code"] == 1001
        assert telemetry["terminal_close_reason"] == "maintenance"
        assert telemetry["terminal_observed_at"] is not None
        assert float(str(telemetry["terminal_observed_age_ms"])) >= 0
    finally:
        socket.close()


def test_transport_error_surfaces_only_after_accepted_fifo_is_drained() -> None:
    connection = FakeConnection(
        iter(("accepted-before-reset", ConnectionResetError("wire reset")))
    )
    socket = WebsocketClientSocket(connection, queue_capacity=2)
    try:
        socket._reader.join(timeout=0.5)
        assert not socket._reader.is_alive()
        received = socket.receive(0.2)
        assert received is not None
        assert received.raw_message == "accepted-before-reset"
        with pytest.raises(ConnectionResetError, match="wire reset"):
            socket.receive(0.2)
        assert socket.telemetry_snapshot()["terminal_origin"] == "transport_exception"
    finally:
        socket.close()


def test_legacy_empty_recv_remains_a_visible_close_after_fifo_drain() -> None:
    connection = FakeConnection(iter(("accepted-before-empty", "")))
    socket = WebsocketClientSocket(connection, queue_capacity=2)
    try:
        socket._reader.join(timeout=0.5)
        assert not socket._reader.is_alive()
        received = socket.receive(0.2)
        assert received is not None
        assert received.raw_message == "accepted-before-empty"
        with pytest.raises(ConnectionError, match="public websocket closed"):
            socket.receive(0.2)
        telemetry = socket.telemetry_snapshot()
        assert telemetry["terminal_origin"] == "legacy_recv_empty"
        assert telemetry["terminal_close_code"] is None
        assert telemetry["terminal_close_reason"] is None
    finally:
        socket.close()


def test_bounded_queue_saturation_fails_before_processing_the_backlog() -> None:
    connection = FakeConnection(iter(("first", "overflow")))
    socket = WebsocketClientSocket(connection, queue_capacity=1)
    try:
        assert connection._closed.wait(0.5), "reader did not close the saturated connection"
        assert socket._messages.qsize() == 1
        with pytest.raises(BufferError, match="bounded public websocket queue is full"):
            socket.receive(0.2)
        assert socket._messages.qsize() == 1
        assert connection.close_calls == 1
        telemetry = socket.telemetry_snapshot()
        assert telemetry["queue_depth"] == 1
        assert telemetry["queue_high_water"] == 1
        assert telemetry["overflow_count"] == 1
        assert telemetry["terminal_exception_type"] == "WebsocketQueueOverflow"
        assert telemetry["terminal_origin"] == "queue_overflow"
        assert telemetry["terminal_observed_at"] is not None
        assert float(str(telemetry["terminal_observed_age_ms"])) >= 0
        assert "local capacity exhausted" in str(telemetry["terminal_reason"])
        assert telemetry["oldest_message_age_ms"] is not None
    finally:
        socket.close()


@pytest.mark.parametrize(
    ("network", "expected_url"),
    [
        ("mainnet", "wss://api.hyperliquid.xyz/ws"),
        ("testnet", "wss://api.hyperliquid-testnet.xyz/ws"),
    ],
)
def test_factory_uses_public_network_url_and_connection_options(
    monkeypatch: pytest.MonkeyPatch,
    network: str,
    expected_url: str,
) -> None:
    received_at = datetime(2026, 8, 12, 12, 0, tzinfo=UTC)
    connection = FakeConnection(iter(("wire",)))
    calls: list[tuple[str, dict[str, Any]]] = []

    def fake_create_connection(url: str, **kwargs: Any) -> FakeConnection:
        calls.append((url, kwargs))
        return connection

    monkeypatch.setattr(websocket, "create_connection", fake_create_connection)
    factory = WebsocketClientFactory(
        queue_capacity=7,
        clock=lambda: received_at,
    )

    socket = factory.connect(network, timeout_seconds=3.5)
    try:
        assert calls == [(expected_url, {"timeout": 3.5, "enable_multithread": True})]
        assert socket._messages.maxsize == 7
        assert socket.receive(0.2) == ReceivedWireMessage("wire", received_at)
    finally:
        socket.close()


def test_factory_rejects_unknown_network_without_opening_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_if_called(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("create_connection must not be called")

    monkeypatch.setattr(websocket, "create_connection", fail_if_called)

    with pytest.raises(ValueError, match="unsupported network: devnet"):
        WebsocketClientFactory().connect("devnet", timeout_seconds=1.0)


@pytest.mark.parametrize("capacity", [0, -1])
def test_queue_capacity_must_be_positive(capacity: int) -> None:
    with pytest.raises(ValueError, match="queue_capacity must be positive"):
        WebsocketClientFactory(queue_capacity=capacity)

    with pytest.raises(ValueError, match="queue_capacity must be positive"):
        WebsocketClientSocket(FakeConnection(), queue_capacity=capacity)
