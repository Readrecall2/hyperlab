from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from hyperlab.api.public import PublicBootstrap
from hyperlab.collector.models import CollectorConfig, ParsedRecord
from hyperlab.collector.runtime import PublicCollector
from hyperlab.collector.storage import FlushResult
from hyperlab.data.schema import RecordType
from hyperlab.paper.collector_source import (
    HYPERLIQUID_MAINNET_PUBLIC_HTTP_URL,
    HYPERLIQUID_MAINNET_PUBLIC_WEBSOCKET_URL,
    PHASE12_PUBLIC_BOOTSTRAP_TIMEOUT_SECONDS,
    PHASE12_PUBLIC_SOURCE_NAME,
    HyperliquidPaperPublicSource,
    PublicCollectorLifecycleError,
    phase12_public_collector_config,
)
from hyperlab.paper.public_source import (
    BoundedPublicRecordSource,
    PublicRecordMarketEventAdapter,
)
from hyperlab.paper.runtime import PublicSourceDescriptor

NOW = datetime(2026, 8, 17, 12, tzinfo=UTC)


class _FixtureRest:
    def __init__(self) -> None:
        self.bootstrap_calls = 0
        self.funding_calls: list[str] = []
        self.candle_calls = 0
        self.l2_calls: list[str] = []
        self.close_calls = 0

    def bootstrap(self, *, observed_at_ms: int | None = None) -> PublicBootstrap:
        del observed_at_ms
        self.bootstrap_calls += 1
        universe = [
            {
                "name": asset,
                "szDecimals": 5,
                "maxLeverage": 50,
                "marginTableId": 20,
            }
            for asset in ("BTC", "ETH")
        ]
        contexts = [
            {
                "dayBaseVlm": "10",
                "dayNtlVlm": "500000",
                "funding": "0.00001",
                "markPx": str(mark),
                "midPx": str(mark),
                "openInterest": "2",
                "oraclePx": str(mark),
                "prevDayPx": str(mark - 1),
            }
            for mark in (50_000, 3_000)
        ]
        return PublicBootstrap(
            observed_at_ms=int(NOW.timestamp() * 1_000),
            perp_payload=[{"universe": universe}, contexts],
            spot_payload=[{"tokens": [], "universe": []}, []],
        )

    def funding_history(
        self,
        asset: str,
        start_ms: int,
        end_ms: int | None = None,
    ) -> object:
        del start_ms
        assert end_ms is not None
        self.funding_calls.append(asset)
        return [
            {
                "coin": asset,
                "fundingRate": "0.00001",
                "premium": "0",
                "time": end_ms - 3_600_000,
            }
        ]

    def candles(self, asset: str, interval: str, start_ms: int, end_ms: int) -> object:
        del asset, interval, start_ms, end_ms
        self.candle_calls += 1
        raise AssertionError("restricted Paper collector must not request candles")

    def l2_snapshot(
        self,
        asset: str,
        *,
        n_sig_figs: int | None = None,
        mantissa: int | None = None,
    ) -> object:
        assert n_sig_figs is None and mantissa is None
        self.l2_calls.append(asset)
        mid = Decimal("50000") if asset == "BTC" else Decimal("3000")
        return {
            "coin": asset,
            "levels": [
                [{"n": 1, "px": str(mid - 1), "sz": "2"}],
                [{"n": 1, "px": str(mid + 1), "sz": "3"}],
            ],
            "time": int(NOW.timestamp() * 1_000),
        }

    def close(self) -> None:
        self.close_calls += 1


class _NoSocketFactory:
    def connect(self, network: str, timeout_seconds: float) -> Any:
        del network, timeout_seconds
        raise AssertionError("fixture test must not connect a websocket")


class _NullSink:
    high_water = 0
    pending_count = 0
    should_flush = False

    def add(self, record: ParsedRecord) -> bool:
        del record
        return True

    def add_many(self, records: Any) -> int:
        return len(tuple(records))

    def flush(self) -> FlushResult:
        return FlushResult((), 0, 0)

    def close(self) -> None:
        return None


class _BlockingCollector:
    def __init__(self) -> None:
        self.run_started = threading.Event()
        self.stop_requested = threading.Event()
        self.stop_calls = 0
        self.close_calls = 0

    def run(
        self,
        *,
        max_messages: int = 0,
        duration_seconds: float | None = None,
    ) -> object:
        del max_messages, duration_seconds
        self.run_started.set()
        assert self.stop_requested.wait(timeout=2)
        return object()

    def stop(self) -> None:
        self.stop_calls += 1
        self.stop_requested.set()

    def close(self) -> None:
        self.close_calls += 1
        self.stop_requested.set()


class _ReturningCollector(_BlockingCollector):
    def run(
        self,
        *,
        max_messages: int = 0,
        duration_seconds: float | None = None,
    ) -> object:
        del max_messages, duration_seconds
        self.run_started.set()
        return object()


def _bounded_source() -> BoundedPublicRecordSource:
    adapter = PublicRecordMarketEventAdapter(
        instruments={("hyperliquid", "BTC"): "HL:BTC:perp"},
        queue_capacity=2,
    )
    return BoundedPublicRecordSource(
        descriptor=PublicSourceDescriptor(
            source="fixture-public",
            data_hash=adapter.identity_hash,
        ),
        adapter=adapter,
        capacity=2,
    )


def test_default_collector_subscriptions_are_unchanged() -> None:
    config = CollectorConfig()

    assert config.subscription_count == 5
    assert [subscription.key for subscription in config.subscriptions()] == [
        "activeAssetCtx:BTC",
        "bbo:BTC",
        "l2Book:BTC",
        "trades:BTC",
        "candle:BTC:1m",
    ]
    assert config.collect_funding_history is True
    assert config.reconnect_on_rest_refresh_failure is False
    assert config.critical_funding_history is False
    assert config.rest_refresh_enabled is True


def test_phase12_profile_is_exact_public_bbo_plus_funding() -> None:
    config = phase12_public_collector_config()

    assert config.network == "mainnet"
    assert config.assets == ("BTC", "ETH")
    assert config.candle_intervals == ()
    assert config.subscription_channels == ("bbo",)
    assert config.subscription_count == 2
    assert [subscription.payload() for subscription in config.subscriptions()] == [
        {"type": "bbo", "coin": "BTC"},
        {"type": "bbo", "coin": "ETH"},
    ]
    assert config.collect_funding_history is True
    assert config.reconnect_on_rest_refresh_failure is True
    assert config.critical_funding_history is True
    assert config.rest_refresh_enabled is True

    no_rest_refresh = CollectorConfig(
        assets=("BTC",),
        candle_intervals=(),
        subscription_channels=("bbo",),
        collect_funding_history=False,
    )
    assert no_rest_refresh.rest_refresh_enabled is False

    with pytest.raises(ValueError, match="candle_intervals must be empty"):
        CollectorConfig(subscription_channels=("bbo",))


def test_restricted_rest_materialization_emits_bbo_and_funding_but_not_l2_or_candles(
    tmp_path: Path,
) -> None:
    rest = _FixtureRest()
    collector = PublicCollector(
        phase12_public_collector_config(),
        rest=rest,
        socket_factory=_NoSocketFactory(),
        sink=_NullSink(),
        runtime_status_path=tmp_path / "paper-public-status.json",
        clock=lambda: NOW,
    )
    try:
        records = tuple(
            collector._iter_rest_records(
                connection_id="fixture-rest",
                connection_epoch=1,
                history_hours=24,
                include_l2=True,
                query_end=NOW,
            )
        )
    finally:
        collector.close()

    assert rest.bootstrap_calls == 1
    assert rest.funding_calls == ["BTC", "ETH"]
    assert rest.candle_calls == 0
    assert rest.l2_calls == ["BTC", "ETH"]
    record_types = {record.record_type for record in records}
    assert RecordType.BBO in record_types
    assert RecordType.FUNDING in record_types
    assert RecordType.L2_BOOK_STATE not in record_types
    assert RecordType.L2_SNAPSHOT not in record_types
    assert RecordType.L2_DELTA not in record_types
    assert RecordType.CANDLE not in record_types


def test_create_mainnet_is_transport_lazy_and_binds_exact_public_identity(
    tmp_path: Path,
) -> None:
    source = HyperliquidPaperPublicSource.create_mainnet(
        runtime_status_path=tmp_path / "paper-public-status.json",
    )
    try:
        assert source.started is False
        assert (
            source.descriptor.bootstrap_timeout_seconds
            == PHASE12_PUBLIC_BOOTSTRAP_TIMEOUT_SECONDS
        )
        assert source.descriptor.source == PHASE12_PUBLIC_SOURCE_NAME
        identity = json.loads(source.identity_artifact_bytes)
        transport = identity["transport"]
        assert identity["funding_dedupe_capacity_settlements"] == 4_096
        collector_identity = transport["collector_config"]
        assert collector_identity["reconnect_on_rest_refresh_failure"] is True
        assert collector_identity["critical_funding_history"] is True
        assert collector_identity["subscription_channels"] == ["bbo"]
        assert collector_identity["collect_funding_history"] is True
        assert (
            transport["bootstrap_timeout_seconds"]
            == PHASE12_PUBLIC_BOOTSTRAP_TIMEOUT_SECONDS
            == 120.0
        )
        assert transport["http_info_endpoint"] == HYPERLIQUID_MAINNET_PUBLIC_HTTP_URL
        assert transport["http_final_url_must_equal_request"] is True
        assert transport["http_required_status"] == 200
        assert transport["redirects_allowed"] is False
        assert transport["websocket_endpoint"] == HYPERLIQUID_MAINNET_PUBLIC_WEBSOCKET_URL
        assert transport["websocket_redirect_limit"] == 0
        assert transport["websocket_required_http_status"] == 101
        assert transport["transport_schema"] == "hyperliquid-paper-public-transport-v2"
        assert transport["public_rest_methods"] == [
            "metaAndAssetCtxs",
            "spotMetaAndAssetCtxs",
            "fundingHistory",
            "l2Book",
        ]
        assert transport["rest_l2_book_projection"] == "BBO_BOOTSTRAP_AND_RESYNC_ONLY"
        assert transport["subscription_payloads"] == [
            {"coin": "BTC", "type": "bbo"},
            {"coin": "ETH", "type": "bbo"},
        ]
        assert transport["credential_scope"] == "NONE"
        assert transport["orders_enabled"] is False
        assert transport["wallet_or_signer_present"] is False
        assert transport["join_timeout_seconds"] == 10.0
        assert transport["request_timeout_seconds"] == 10.0
        assert transport["wire_queue_capacity_messages"] == 10_000
        with pytest.raises(PublicCollectorLifecycleError, match="must be started"):
            source.poll(timeout_seconds=0)
    finally:
        source.close()

    assert not (tmp_path / "paper-public-status.json").exists()


def test_collector_status_atomic_write_stays_in_persistent_paper_directory(
    tmp_path: Path,
) -> None:
    persistent_root = tmp_path / "var/lib/hyperlab/phase12-live-paper"
    status_path = persistent_root / "paper/phase12-public-source-status.json"
    collector = PublicCollector(
        phase12_public_collector_config(),
        rest=_FixtureRest(),
        socket_factory=_NoSocketFactory(),
        sink=_NullSink(),
        runtime_status_path=status_path,
        clock=lambda: NOW,
    )
    try:
        collector._publish_status()
    finally:
        collector.close()

    payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert payload["mode"] == "readonly"
    assert payload["orders_enabled"] is False
    assert payload["network"] == "mainnet"
    assert status_path.is_relative_to(persistent_root)
    assert not status_path.with_suffix(".tmp").exists()


def test_create_mainnet_rejects_invalid_transport_bounds_without_start(
    tmp_path: Path,
) -> None:
    status_path = tmp_path / "paper-public-status.json"

    with pytest.raises(ValueError, match="wire_queue_capacity"):
        HyperliquidPaperPublicSource.create_mainnet(
            runtime_status_path=status_path,
            wire_queue_capacity=True,
        )
    with pytest.raises(ValueError, match="request_timeout_seconds"):
        HyperliquidPaperPublicSource.create_mainnet(
            runtime_status_path=status_path,
            request_timeout_seconds=float("nan"),
        )
    with pytest.raises(ValueError, match="join_timeout_seconds"):
        HyperliquidPaperPublicSource.create_mainnet(
            runtime_status_path=status_path,
            join_timeout_seconds=float("inf"),
        )

    assert not status_path.exists()


def test_stop_before_start_is_terminal_and_does_not_construct_collector() -> None:
    source = _bounded_source()
    collector = _BlockingCollector()
    wrapper = HyperliquidPaperPublicSource(
        source=source,
        collector_factory=lambda: collector,
        collector_config=phase12_public_collector_config(),
        join_timeout_seconds=2,
    )

    wrapper.stop()
    with pytest.raises(PublicCollectorLifecycleError, match="stopped before start"):
        wrapper.start()
    assert wrapper.started is False
    assert not collector.run_started.is_set()
    wrapper.close()


def test_worker_thread_start_failure_closes_collector_and_latches_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingThread:
        def __init__(self, *, target: Any, name: str, daemon: bool) -> None:
            del target, name, daemon

        def start(self) -> None:
            raise RuntimeError("fixture thread launch failed")

        def join(self, *, timeout: float) -> None:
            del timeout

        def is_alive(self) -> bool:
            return False

    monkeypatch.setattr(
        "hyperlab.paper.collector_source.threading.Thread",
        FailingThread,
    )
    source = _bounded_source()
    collector = _BlockingCollector()
    wrapper = HyperliquidPaperPublicSource(
        source=source,
        collector_factory=lambda: collector,
        collector_config=phase12_public_collector_config(),
        join_timeout_seconds=2,
    )

    with pytest.raises(PublicCollectorLifecycleError, match="thread start failed"):
        wrapper.start()
    with pytest.raises(PublicCollectorLifecycleError, match="thread start failed"):
        wrapper.poll(timeout_seconds=0)
    assert collector.close_calls == 1
    wrapper.close()


def test_lifecycle_starts_once_and_stops_collector_cooperatively() -> None:
    source = _bounded_source()
    collector = _BlockingCollector()
    factory_calls = 0

    def factory() -> _BlockingCollector:
        nonlocal factory_calls
        factory_calls += 1
        return collector

    wrapper = HyperliquidPaperPublicSource(
        source=source,
        collector_factory=factory,
        collector_config=phase12_public_collector_config(),
        join_timeout_seconds=2,
    )
    assert factory_calls == 0

    wrapper.start()
    assert collector.run_started.wait(timeout=1)
    assert factory_calls == 1
    with pytest.raises(PublicCollectorLifecycleError, match="single-start"):
        wrapper.start()

    wrapper.stop()
    wrapper.close()
    assert collector.stop_calls >= 1
    assert collector.close_calls == 1


def test_unexpected_collector_exit_latches_source_terminal() -> None:
    source = _bounded_source()
    collector = _ReturningCollector()
    wrapper = HyperliquidPaperPublicSource(
        source=source,
        collector_factory=lambda: collector,
        collector_config=phase12_public_collector_config(),
        join_timeout_seconds=2,
    )

    wrapper.start()
    assert collector.run_started.wait(timeout=1)
    with pytest.raises(
        PublicCollectorLifecycleError,
        match="terminated without a stop request",
    ):
        wrapper.poll(timeout_seconds=0.5)
    wrapper.close()
