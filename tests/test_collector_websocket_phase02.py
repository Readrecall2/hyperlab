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


def test_background_reader_accepts_text_and_decodes_bytes() -> None:
    first_received_at = datetime(2026, 8, 12, 12, 0, tzinfo=UTC)
    second_received_at = first_received_at + timedelta(milliseconds=1)
    reception_times = iter((first_received_at, second_received_at))
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
        clock=lambda: datetime(2026, 8, 12, 12, 0),
    )
    try:
        with pytest.raises(ValueError, match="received_time must be timezone-aware"):
            socket.receive(0.2)
    finally:
        socket.close()


def test_bounded_queue_saturation_is_visible_after_buffered_message() -> None:
    connection = FakeConnection(iter(("first", "overflow")))
    socket = WebsocketClientSocket(connection, queue_capacity=1)
    try:
        assert connection._closed.wait(0.5), "reader did not close the saturated connection"
        received = socket.receive(0.2)
        assert received is not None
        assert received.raw_message == "first"
        with pytest.raises(BufferError, match="bounded Hyperliquid websocket queue is full"):
            socket.receive(0.02)
        assert connection.close_calls == 1
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
