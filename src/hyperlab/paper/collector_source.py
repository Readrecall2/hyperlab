from __future__ import annotations

import math
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict
from pathlib import Path
from types import MappingProxyType
from typing import Protocol

from hyperlab.api.public import (
    PUBLIC_INFO_REDIRECTS_ALLOWED,
    PUBLIC_INFO_REQUIRED_HTTP_STATUS,
    HyperliquidPublicClient,
)
from hyperlab.collector.models import CollectorConfig, ParsedRecord
from hyperlab.collector.runtime import PublicCollector
from hyperlab.collector.storage import CoordinatedWriterError, FlushResult
from hyperlab.collector.websocket import (
    PUBLIC_WEBSOCKET_REDIRECT_LIMIT,
    PUBLIC_WEBSOCKET_REQUIRED_HTTP_STATUS,
    WebsocketClientFactory,
)
from hyperlab.paper.public_source import (
    BoundedPublicRecordSource,
    PublicRecordAdapterError,
    PublicRecordMarketEventAdapter,
    PublicRecordQueueFull,
    PublicRecordSourceClosed,
    PublicSourceItem,
)
from hyperlab.paper.runtime import PublicSourceDescriptor

HYPERLIQUID_MAINNET_PUBLIC_HTTP_URL = "https://api.hyperliquid.xyz"
HYPERLIQUID_MAINNET_PUBLIC_WEBSOCKET_URL = "wss://api.hyperliquid.xyz/ws"
PHASE12_PUBLIC_SOURCE_NAME = "hyperliquid-mainnet-public-bbo-funding-v1"
PHASE12_PUBLIC_ASSETS = ("BTC", "ETH")
PHASE12_PUBLIC_BOOTSTRAP_TIMEOUT_SECONDS = 120.0
PHASE12_PUBLIC_INSTRUMENTS: Mapping[tuple[str, str], str] = MappingProxyType(
    {
        ("hyperliquid", "BTC"): "HL:BTC:perp",
        ("hyperliquid", "ETH"): "HL:ETH:perp",
    }
)


class PublicCollectorLifecycleError(RuntimeError):
    """The owned public collector could not provide a complete bounded lifecycle."""


class _CollectorLifecycle(Protocol):
    def run(self, *, max_messages: int = 0, duration_seconds: float | None = None) -> object: ...

    def stop(self) -> None: ...

    def close(self) -> None: ...


_CollectorFactory = Callable[[], _CollectorLifecycle]


def phase12_public_collector_config() -> CollectorConfig:
    """Frozen public-only collector profile for the first Phase 12 Paper runtime."""

    return CollectorConfig(
        network="mainnet",
        assets=PHASE12_PUBLIC_ASSETS,
        candle_intervals=(),
        subscription_channels=("bbo",),
        collect_funding_history=True,
        reconnect_on_rest_refresh_failure=True,
        critical_funding_history=True,
        batch_size=500,
        flush_interval_seconds=5.0,
        heartbeat_interval_seconds=20.0,
        pong_timeout_seconds=45.0,
        ws_connect_timeout_seconds=15.0,
        stale_after_seconds=15.0,
        backoff_initial_seconds=1.0,
        backoff_max_seconds=30.0,
        backoff_jitter_ratio=0.2,
        backoff_reset_after_seconds=60.0,
        history_lookback_hours=24,
        rest_refresh_interval_seconds=300.0,
        funding_grace_seconds=600.0,
        queue_capacity=10_000,
    )


def _validate_phase12_public_config(config: CollectorConfig) -> None:
    if config.network != "mainnet":
        raise ValueError("Phase 12 Paper public source is frozen to mainnet public data")
    if config.assets != PHASE12_PUBLIC_ASSETS:
        raise ValueError("Phase 12 Paper public source requires exact BTC/ETH asset order")
    if config.subscription_channels != ("bbo",):
        raise ValueError("Phase 12 Paper public source subscribes only to public BBO")
    if config.candle_intervals:
        raise ValueError("Phase 12 Paper public source does not subscribe to candles")
    if config.collect_funding_history is not True:
        raise ValueError("Phase 12 Paper public source requires public funding history")
    if config.reconnect_on_rest_refresh_failure is not True:
        raise ValueError("Phase 12 Paper source must reconnect after a REST refresh failure")
    if config.critical_funding_history is not True:
        raise ValueError("Phase 12 Paper source requires critical public funding health")


class _PaperCollectorSink:
    """Collector sink that performs no lake writes and fails closed on fan-out loss."""

    def __init__(self, source: BoundedPublicRecordSource) -> None:
        self._source = source

    @property
    def high_water(self) -> int:
        return self._source.high_water

    @property
    def pending_count(self) -> int:
        # Items are admitted directly to the bounded consumer FIFO; there is no
        # separate undurable writer buffer for the collector to flush.
        return 0

    @property
    def should_flush(self) -> bool:
        return False

    def add(self, record: ParsedRecord) -> bool:
        try:
            return self._source.feed(record)
        except (
            PublicRecordAdapterError,
            PublicRecordQueueFull,
            PublicRecordSourceClosed,
        ) as exc:
            raise CoordinatedWriterError(
                "Paper public-source fan-out became incomplete and is terminal"
            ) from exc

    def add_many(self, records: Iterable[ParsedRecord]) -> int:
        admitted = 0
        for record in records:
            admitted += int(self.add(record))
        return admitted

    def flush(self) -> FlushResult:
        return FlushResult((), 0, 0)

    def close(self) -> None:
        # The lifecycle wrapper closes the consumer only after the collector
        # thread has terminated.
        return None


class HyperliquidPaperPublicSource:
    """Own one lazy public collector and expose its bounded normalized Paper FIFO.

    Construction and descriptor inspection allocate no HTTP or websocket
    transport. Start creates the public clients immediately before launching
    the collector thread; no credential, wallet, signer, or order route exists.
    """

    def __init__(
        self,
        *,
        source: BoundedPublicRecordSource,
        collector_factory: _CollectorFactory,
        collector_config: CollectorConfig,
        join_timeout_seconds: float,
    ) -> None:
        if not callable(collector_factory):
            raise TypeError("collector_factory must be callable")
        if (
            isinstance(join_timeout_seconds, bool)
            or not isinstance(join_timeout_seconds, (int, float))
            or not math.isfinite(float(join_timeout_seconds))
            or join_timeout_seconds <= 0
        ):
            raise ValueError("join_timeout_seconds must be finite and positive")
        self._source = source
        self._collector_factory = collector_factory
        self._collector_config = collector_config
        self._identity_artifact_bytes = source.identity_artifact_bytes
        self._join_timeout_seconds = join_timeout_seconds
        self._state_lock = threading.Lock()
        self._stop_requested = threading.Event()
        self._collector: _CollectorLifecycle | None = None
        self._thread: threading.Thread | None = None
        self._started = False
        self._closed = False

    @classmethod
    def create_mainnet(
        cls,
        *,
        runtime_status_path: Path,
        collector_config: CollectorConfig | None = None,
        paper_queue_capacity: int = 4_096,
        wire_queue_capacity: int = 10_000,
        request_timeout_seconds: float = 10.0,
        join_timeout_seconds: float = 10.0,
    ) -> HyperliquidPaperPublicSource:
        config = phase12_public_collector_config() if collector_config is None else collector_config
        _validate_phase12_public_config(config)
        if (
            isinstance(wire_queue_capacity, bool)
            or not isinstance(wire_queue_capacity, int)
            or wire_queue_capacity <= 0
        ):
            raise ValueError("wire_queue_capacity must be a positive integer")
        if (
            isinstance(request_timeout_seconds, bool)
            or not isinstance(request_timeout_seconds, (int, float))
            or not math.isfinite(float(request_timeout_seconds))
            or request_timeout_seconds <= 0
        ):
            raise ValueError("request_timeout_seconds must be finite and positive")
        if (
            isinstance(join_timeout_seconds, bool)
            or not isinstance(join_timeout_seconds, (int, float))
            or not math.isfinite(float(join_timeout_seconds))
            or join_timeout_seconds <= 0
        ):
            raise ValueError("join_timeout_seconds must be finite and positive")

        transport_identity = {
            "assets": list(PHASE12_PUBLIC_ASSETS),
            "bootstrap_timeout_seconds": PHASE12_PUBLIC_BOOTSTRAP_TIMEOUT_SECONDS,
            "collector_config": asdict(config),
            "credential_scope": "NONE",
            "execution_routes_present": False,
            "http_info_endpoint": HYPERLIQUID_MAINNET_PUBLIC_HTTP_URL,
            "http_final_url_must_equal_request": True,
            "http_required_status": PUBLIC_INFO_REQUIRED_HTTP_STATUS,
            "join_timeout_seconds": join_timeout_seconds,
            "network": "mainnet",
            "orders_enabled": False,
            "paper_queue_capacity_frames": paper_queue_capacity,
            "public_rest_methods": [
                "metaAndAssetCtxs",
                "spotMetaAndAssetCtxs",
                "fundingHistory",
                "l2Book",
            ],
            "rest_l2_book_projection": "BBO_BOOTSTRAP_AND_RESYNC_ONLY",
            "redirects_allowed": PUBLIC_INFO_REDIRECTS_ALLOWED,
            "request_timeout_seconds": request_timeout_seconds,
            "subscription_payloads": [subscription.payload() for subscription in config.subscriptions()],
            "transport_schema": "hyperliquid-paper-public-transport-v2",
            "wallet_or_signer_present": False,
            "websocket_endpoint": HYPERLIQUID_MAINNET_PUBLIC_WEBSOCKET_URL,
            "websocket_redirect_limit": PUBLIC_WEBSOCKET_REDIRECT_LIMIT,
            "websocket_required_http_status": PUBLIC_WEBSOCKET_REQUIRED_HTTP_STATUS,
            "wire_queue_capacity_messages": wire_queue_capacity,
        }
        adapter = PublicRecordMarketEventAdapter(
            instruments=PHASE12_PUBLIC_INSTRUMENTS,
            queue_capacity=paper_queue_capacity,
            identity_context=transport_identity,
        )
        descriptor = PublicSourceDescriptor(
            source=PHASE12_PUBLIC_SOURCE_NAME,
            data_hash=adapter.identity_hash,
            bootstrap_timeout_seconds=PHASE12_PUBLIC_BOOTSTRAP_TIMEOUT_SECONDS,
        )
        source = BoundedPublicRecordSource(
            descriptor=descriptor,
            adapter=adapter,
            capacity=paper_queue_capacity,
        )

        def build_collector() -> _CollectorLifecycle:
            resolved_rest = HyperliquidPublicClient(
                network="mainnet",
                timeout_seconds=request_timeout_seconds,
            )
            resolved_socket_factory = WebsocketClientFactory(
                queue_capacity=wire_queue_capacity,
                venue="hyperliquid",
                socket_role="paper-public",
                reader_name="hyperlab-paper-public-wire",
            )
            return PublicCollector(
                config,
                rest=resolved_rest,
                socket_factory=resolved_socket_factory,
                sink=_PaperCollectorSink(source),
                runtime_status_path=runtime_status_path,
            )

        return cls(
            source=source,
            collector_factory=build_collector,
            collector_config=config,
            join_timeout_seconds=join_timeout_seconds,
        )

    @property
    def descriptor(self) -> PublicSourceDescriptor:
        return self._source.descriptor

    @property
    def collector_config(self) -> CollectorConfig:
        return self._collector_config

    @property
    def identity_artifact_bytes(self) -> bytes:
        return self._identity_artifact_bytes

    @property
    def started(self) -> bool:
        with self._state_lock:
            return self._started

    def start(self) -> None:
        with self._state_lock:
            if self._closed:
                raise PublicRecordSourceClosed("public Paper source is closed")
            if self._started:
                raise PublicCollectorLifecycleError(
                    "public Paper source is single-start; construct a new source to restart"
                )
            if self._stop_requested.is_set():
                raise PublicCollectorLifecycleError(
                    "public Paper source was stopped before start and cannot be restarted"
                )
            self._started = True
            try:
                collector = self._collector_factory()
            except Exception as exc:
                error = PublicCollectorLifecycleError(
                    f"public collector construction failed: {type(exc).__name__}: {exc}"
                )
                self._source.fail(error)
                raise error from exc
            self._collector = collector
            thread = threading.Thread(
                target=self._run_collector,
                name="hyperlab-paper-public-collector",
                daemon=False,
            )
            self._thread = thread
            try:
                thread.start()
            except Exception as exc:
                cleanup_error: Exception | None = None
                try:
                    collector.close()
                except Exception as close_exc:
                    cleanup_error = close_exc
                detail = f"{type(exc).__name__}: {exc}"
                if cleanup_error is not None:
                    detail += f"; cleanup {type(cleanup_error).__name__}: {cleanup_error}"
                error = PublicCollectorLifecycleError(f"public collector thread start failed: {detail}")
                self._source.fail(error)
                raise error from exc

    def _run_collector(self) -> None:
        collector = self._collector
        if collector is None:
            self._source.fail(PublicCollectorLifecycleError("public collector thread has no collector"))
            return
        try:
            collector.run()
        except Exception as exc:
            if not self._stop_requested.is_set():
                self._source.fail(exc)
        else:
            if not self._stop_requested.is_set():
                self._source.fail(
                    PublicCollectorLifecycleError("public collector terminated without a stop request")
                )

    def poll(self, *, timeout_seconds: float) -> PublicSourceItem | None:
        with self._state_lock:
            started = self._started
        if not started:
            raise PublicCollectorLifecycleError(
                "public Paper source must be started after runtime reconciliation"
            )
        return self._source.poll(timeout_seconds=timeout_seconds)

    def stop(self) -> None:
        self._stop_requested.set()
        with self._state_lock:
            collector = self._collector
        if collector is not None:
            collector.stop()

    def close(self) -> None:
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            collector = self._collector
            thread = self._thread
        shutdown_error: Exception | None = None
        try:
            self.stop()
        except Exception as exc:
            shutdown_error = exc
        if thread is not None:
            thread.join(timeout=self._join_timeout_seconds)
        if collector is not None:
            try:
                collector.close()
            except Exception as exc:
                if shutdown_error is None:
                    shutdown_error = exc
        if thread is not None and thread.is_alive():
            thread.join(timeout=self._join_timeout_seconds)
        self._source.close()
        if thread is not None and thread.is_alive():
            error = PublicCollectorLifecycleError(
                "public collector did not terminate inside the bounded shutdown deadline"
            )
            if shutdown_error is not None:
                raise error from shutdown_error
            raise error
        if shutdown_error is not None:
            raise PublicCollectorLifecycleError(
                "public collector shutdown raised an exception"
            ) from shutdown_error


__all__ = [
    "HYPERLIQUID_MAINNET_PUBLIC_HTTP_URL",
    "HYPERLIQUID_MAINNET_PUBLIC_WEBSOCKET_URL",
    "PHASE12_PUBLIC_ASSETS",
    "PHASE12_PUBLIC_BOOTSTRAP_TIMEOUT_SECONDS",
    "PHASE12_PUBLIC_INSTRUMENTS",
    "PHASE12_PUBLIC_SOURCE_NAME",
    "HyperliquidPaperPublicSource",
    "PublicCollectorLifecycleError",
    "phase12_public_collector_config",
]
