from __future__ import annotations

import json
import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class ReceivedWireMessage:
    """Public wire payload timestamped immediately after ``recv`` returns."""

    raw_message: str
    received_time: datetime

    def __post_init__(self) -> None:
        if self.received_time.tzinfo is None or self.received_time.utcoffset() is None:
            raise ValueError("received_time must be timezone-aware")
        if self.received_time.utcoffset() != UTC.utcoffset(self.received_time):
            raise ValueError("received_time must use UTC")


class PublicSocket(Protocol):
    connected_at: datetime

    def start_receiving(self) -> None: ...

    def send_json(self, payload: dict[str, object]) -> None: ...

    def receive(self, timeout_seconds: float) -> ReceivedWireMessage | None: ...

    def close(self) -> None: ...


class PublicSocketFactory(Protocol):
    def connect(self, network: str, timeout_seconds: float) -> PublicSocket: ...


class WebsocketClientSocket:
    """Receive public wire messages on a bounded background queue."""

    def __init__(
        self,
        connection: Any,
        *,
        queue_capacity: int,
        clock: Callable[[], datetime] = _utc_now,
        start_immediately: bool = True,
    ) -> None:
        if queue_capacity <= 0:
            raise ValueError("queue_capacity must be positive")
        self._connection = connection
        self._messages: queue.Queue[ReceivedWireMessage] = queue.Queue(maxsize=queue_capacity)
        self._clock = clock
        self._closed = threading.Event()
        self._terminal_error: BaseException | None = None
        self.connected_at = self._clock()
        ReceivedWireMessage("", self.connected_at)
        self._connection.settimeout(1.0)
        self._reader = threading.Thread(
            target=self._read_forever,
            name="public-market-data-ws-reader",
            daemon=True,
        )
        self._reader_lock = threading.Lock()
        self._reader_started = False
        if start_immediately:
            self.start_receiving()

    def start_receiving(self) -> None:
        """Activate the reader once the supervisor has persisted connection lineage."""

        with self._reader_lock:
            if self._reader_started:
                return
            if self._closed.is_set():
                raise RuntimeError("cannot start a closed public websocket reader")
            self._reader.start()
            self._reader_started = True

    def send_json(self, payload: dict[str, object]) -> None:
        self._connection.send(json.dumps(payload, separators=(",", ":")))

    def receive(self, timeout_seconds: float) -> ReceivedWireMessage | None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not self._reader_started:
            raise RuntimeError("public websocket reader has not been started")
        self._raise_terminal_error()
        try:
            received = self._messages.get(timeout=timeout_seconds)
        except queue.Empty:
            self._raise_terminal_error()
            return None
        self._raise_terminal_error()
        return received

    def _raise_terminal_error(self) -> None:
        error = self._terminal_error
        if error is not None:
            raise error from None

    def close(self) -> None:
        self._closed.set()
        try:
            self._connection.close()
        finally:
            if self._reader_started and threading.current_thread() is not self._reader:
                self._reader.join(timeout=2.0)

    def _read_forever(self) -> None:
        import websocket

        while not self._closed.is_set():
            try:
                message = self._connection.recv()
                received_time = self._clock()
            except websocket.WebSocketTimeoutException:
                continue
            except BaseException as exc:
                if not self._closed.is_set():
                    self._terminal_error = exc
                return
            if isinstance(message, bytes):
                try:
                    message = message.decode("utf-8")
                except UnicodeDecodeError as exc:
                    self._terminal_error = exc
                    return
            if not isinstance(message, str):
                self._terminal_error = TypeError(
                    f"unexpected websocket message type: {type(message).__name__}"
                )
                return
            if not message:
                self._terminal_error = ConnectionError("Hyperliquid websocket closed")
                return
            try:
                received = ReceivedWireMessage(
                    raw_message=message,
                    received_time=received_time,
                )
            except BaseException as exc:
                self._terminal_error = exc
                return
            try:
                self._messages.put_nowait(received)
            except queue.Full:
                self._terminal_error = BufferError(
                    "bounded public websocket queue is full; reconnect required"
                )
                self._closed.set()
                self._connection.close()
                return


class WebsocketClientFactory:
    """Public transport with bounded buffering; the supervisor owns reconnection."""

    def __init__(
        self,
        *,
        queue_capacity: int = 10_000,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if queue_capacity <= 0:
            raise ValueError("queue_capacity must be positive")
        self.queue_capacity = queue_capacity
        self._clock = clock

    def connect(self, network: str, timeout_seconds: float) -> WebsocketClientSocket:
        import websocket

        if network == "mainnet":
            url = "wss://api.hyperliquid.xyz/ws"
        elif network == "testnet":
            url = "wss://api.hyperliquid-testnet.xyz/ws"
        else:
            raise ValueError(f"unsupported network: {network}")
        connection = websocket.create_connection(
            url,
            timeout=timeout_seconds,
            enable_multithread=True,
        )
        return WebsocketClientSocket(
            connection,
            queue_capacity=self.queue_capacity,
            clock=self._clock,
        )


class UrlWebsocketClientFactory:
    """Public transport for a connector-owned market-data URL."""

    def __init__(
        self,
        url: str,
        *,
        queue_capacity: int = 10_000,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if not url.startswith("wss://"):
            raise ValueError("public websocket URL must use wss")
        if queue_capacity <= 0:
            raise ValueError("queue_capacity must be positive")
        self.url = url
        self.queue_capacity = queue_capacity
        self._clock = clock

    def _connect(
        self,
        network: str,
        timeout_seconds: float,
        *,
        start_immediately: bool,
    ) -> WebsocketClientSocket:
        import websocket

        if network != "public":
            raise ValueError("URL websocket factory only supports the public network label")
        connection = websocket.create_connection(
            self.url,
            timeout=timeout_seconds,
            enable_multithread=True,
        )
        return WebsocketClientSocket(
            connection,
            queue_capacity=self.queue_capacity,
            clock=self._clock,
            start_immediately=start_immediately,
        )

    def connect(self, network: str, timeout_seconds: float) -> WebsocketClientSocket:
        return self._connect(network, timeout_seconds, start_immediately=True)

    def connect_paused(
        self,
        network: str,
        timeout_seconds: float,
    ) -> WebsocketClientSocket:
        return self._connect(network, timeout_seconds, start_immediately=False)
