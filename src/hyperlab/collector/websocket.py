from __future__ import annotations

import itertools
import json
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from hyperlab.collector.telemetry import MonotonicTimingSummary


def _utc_now() -> datetime:
    return datetime.now(UTC)


_READER_SEQUENCE = itertools.count(1)


def _reader_name(
    venue: str | None,
    socket_role: str | None,
    requested_name: str | None,
) -> str:
    if requested_name is not None:
        if not requested_name.strip():
            raise ValueError("reader name must not be empty")
        return requested_name
    identity = "-".join(value for value in (venue, socket_role) if value) or "public"
    safe_identity = "".join(
        character if character.isalnum() or character in {"-", "_"} else "-" for character in identity
    )
    return f"hyperlab-ws-{safe_identity}-{next(_READER_SEQUENCE)}"


def _optional_label(value: str | None, *, label: str) -> str | None:
    if value is not None and not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value


class WebsocketQueueOverflow(BufferError):
    """The bounded network-reader queue rejected a wire message."""


class WebsocketConsumerBackpressure(BufferError):
    """A supervisor left a queued wire message beyond its existing deadline."""


@dataclass(frozen=True, slots=True)
class ReceivedWireMessage:
    """Public wire payload timestamped immediately after ``recv`` returns."""

    raw_message: str
    received_time: datetime
    received_monotonic_ns: int | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        if self.received_time.tzinfo is None or self.received_time.utcoffset() is None:
            raise ValueError("received_time must be timezone-aware")
        if self.received_time.utcoffset() != UTC.utcoffset(self.received_time):
            raise ValueError("received_time must use UTC")
        if self.received_monotonic_ns is not None and self.received_monotonic_ns < 0:
            raise ValueError("received_monotonic_ns must be non-negative")


class _ObservedMessageQueue(queue.Queue[ReceivedWireMessage]):
    """Queue with an exact high-water mark updated under the queue mutex."""

    def __init__(self, *, capacity: int) -> None:
        super().__init__(maxsize=capacity)
        self.high_water = 0

    def _put(self, item: ReceivedWireMessage) -> None:
        super()._put(item)
        self.high_water = max(self.high_water, self._qsize())

    def telemetry_state(
        self,
        observed_monotonic_ns: int,
    ) -> tuple[int, int, float | None]:
        with self.mutex:
            depth = self._qsize()
            high_water = self.high_water
            oldest = None if not self.queue else self.queue[0]
        if oldest is None or oldest.received_monotonic_ns is None:
            oldest_age_ms = None
        else:
            oldest_age_ms = max(observed_monotonic_ns - oldest.received_monotonic_ns, 0) / 1_000_000
        return depth, high_water, oldest_age_ms


class PublicSocket(Protocol):
    connected_at: datetime

    def start_receiving(self) -> None: ...

    def send_json(self, payload: dict[str, object]) -> None: ...

    def receive(self, timeout_seconds: float) -> ReceivedWireMessage | None: ...
    def telemetry_snapshot(self) -> dict[str, object]: ...

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
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        venue: str | None = None,
        socket_role: str | None = None,
        reader_name: str | None = None,
        start_immediately: bool = True,
    ) -> None:
        if queue_capacity <= 0:
            raise ValueError("queue_capacity must be positive")
        self._connection = connection
        self._messages = _ObservedMessageQueue(capacity=queue_capacity)
        self._clock = clock
        self._monotonic_ns = monotonic_ns
        self.venue = _optional_label(venue, label="venue")
        self.socket_role = _optional_label(socket_role, label="socket role")
        self.reader_name = _reader_name(self.venue, self.socket_role, reader_name)
        self._closed = threading.Event()
        self._terminal_error: BaseException | None = None
        self._terminal_reason: str | None = None
        self._terminal_lock = threading.Lock()
        self._telemetry_lock = threading.Lock()
        self._overflow_count = 0
        self._latest_received_monotonic_ns: int | None = None
        self._enqueue_delay = MonotonicTimingSummary()
        self._dequeue_residence = MonotonicTimingSummary()
        self.connected_at = self._clock()
        ReceivedWireMessage("", self.connected_at)
        self._connection.settimeout(1.0)
        self._reader = threading.Thread(
            target=self._read_forever,
            name=self.reader_name,
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
        dequeued_monotonic_ns = self._monotonic_ns()
        if received.received_monotonic_ns is not None:
            self._dequeue_residence.observe_ns(max(dequeued_monotonic_ns - received.received_monotonic_ns, 0))
        self._raise_terminal_error()
        return received

    def _set_terminal_error(self, error: BaseException) -> None:
        with self._terminal_lock:
            if self._terminal_error is not None:
                return
            self._terminal_error = error
            detail = str(error).strip()
            self._terminal_reason = (
                type(error).__name__ if not detail else f"{type(error).__name__}: {detail}"
            )

    def _raise_terminal_error(self) -> None:
        with self._terminal_lock:
            error = self._terminal_error
        if error is not None:
            raise error from None

    def telemetry_snapshot(self) -> dict[str, object]:
        observed_monotonic_ns = self._monotonic_ns()
        depth, high_water, oldest_age_ms = self._messages.telemetry_state(observed_monotonic_ns)
        with self._telemetry_lock:
            overflow_count = self._overflow_count
            latest_received_monotonic_ns = self._latest_received_monotonic_ns
        latest_received_age_ms = (
            None
            if latest_received_monotonic_ns is None
            else max(observed_monotonic_ns - latest_received_monotonic_ns, 0) / 1_000_000
        )
        with self._terminal_lock:
            terminal_reason = self._terminal_reason
            terminal_type = None if self._terminal_error is None else type(self._terminal_error).__name__
        return {
            "venue": self.venue,
            "socket_role": self.socket_role,
            "reader_name": self.reader_name,
            "reader_started": self._reader_started,
            "reader_alive": self._reader.is_alive(),
            "closed": self._closed.is_set(),
            "queue_capacity": self._messages.maxsize,
            "queue_depth": depth,
            "queue_high_water": high_water,
            "oldest_message_age_ms": oldest_age_ms,
            "latest_message_received_age_ms": latest_received_age_ms,
            "enqueue_delay_ms": self._enqueue_delay.as_dict(),
            "dequeue_residence_ms": self._dequeue_residence.as_dict(),
            "overflow_count": overflow_count,
            "terminal_exception_type": terminal_type,
            "terminal_reason": terminal_reason,
        }

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
                received_monotonic_ns = self._monotonic_ns()
                with self._telemetry_lock:
                    self._latest_received_monotonic_ns = received_monotonic_ns
            except websocket.WebSocketTimeoutException:
                continue
            except BaseException as exc:
                if not self._closed.is_set():
                    self._set_terminal_error(exc)
                return
            if isinstance(message, bytes):
                try:
                    message = message.decode("utf-8")
                except UnicodeDecodeError as exc:
                    self._set_terminal_error(exc)
                    return
            if not isinstance(message, str):
                self._set_terminal_error(
                    TypeError(f"unexpected websocket message type: {type(message).__name__}")
                )
                return
            if not message:
                self._set_terminal_error(ConnectionError("public websocket closed"))
                return
            try:
                received = ReceivedWireMessage(
                    raw_message=message,
                    received_time=received_time,
                    received_monotonic_ns=received_monotonic_ns,
                )
            except BaseException as exc:
                self._set_terminal_error(exc)
                return
            try:
                self._messages.put_nowait(received)
            except queue.Full:
                with self._telemetry_lock:
                    self._overflow_count += 1
                self._set_terminal_error(
                    WebsocketQueueOverflow("bounded public websocket queue is full; local capacity exhausted")
                )
                self._closed.set()
                self._connection.close()
                return
            enqueued_monotonic_ns = self._monotonic_ns()
            self._enqueue_delay.observe_ns(max(enqueued_monotonic_ns - received_monotonic_ns, 0))


class WebsocketClientFactory:
    """Public transport with bounded buffering; the supervisor owns reconnection."""

    def __init__(
        self,
        *,
        queue_capacity: int = 10_000,
        clock: Callable[[], datetime] = _utc_now,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        venue: str | None = "hyperliquid",
        socket_role: str | None = "public",
        reader_name: str | None = None,
    ) -> None:
        if queue_capacity <= 0:
            raise ValueError("queue_capacity must be positive")
        self.queue_capacity = queue_capacity
        self._clock = clock
        self._monotonic_ns = monotonic_ns
        self._venue = venue
        self._socket_role = socket_role
        self._reader_name = reader_name

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
            monotonic_ns=self._monotonic_ns,
            venue=self._venue,
            socket_role=self._socket_role,
            reader_name=self._reader_name,
        )


class UrlWebsocketClientFactory:
    """Public transport for a connector-owned market-data URL."""

    def __init__(
        self,
        url: str,
        *,
        queue_capacity: int = 10_000,
        clock: Callable[[], datetime] = _utc_now,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        venue: str | None = None,
        socket_role: str | None = None,
        reader_name: str | None = None,
    ) -> None:
        if not url.startswith("wss://"):
            raise ValueError("public websocket URL must use wss")
        if queue_capacity <= 0:
            raise ValueError("queue_capacity must be positive")
        self.url = url
        self.queue_capacity = queue_capacity
        self._clock = clock
        self._monotonic_ns = monotonic_ns
        self._venue = venue
        self._socket_role = socket_role
        self._reader_name = reader_name

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
            monotonic_ns=self._monotonic_ns,
            venue=self._venue,
            socket_role=self._socket_role,
            reader_name=self._reader_name,
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
