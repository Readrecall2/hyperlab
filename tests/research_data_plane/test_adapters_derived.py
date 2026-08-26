from __future__ import annotations

import ast
from pathlib import Path

import pytest

from hyperlab.research_data import adapters as adapters_module
from hyperlab.research_data.adapters import (
    HyperliquidPublicAdapter,
    KalshiPublicAdapter,
    PolymarketPublicAdapter,
    PublicHttpRequest,
    PublicWebsocketSubscription,
    all_public_route_specs,
)
from hyperlab.research_data.derived import build_hyperliquid_views
from hyperlab.research_data.envelope import (
    SYNTHETIC_FIXTURE_LABEL,
    CaptureProvenance,
    SessionEnvelopeFactory,
    Venue,
)

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "research_data"


def _factory(venue: Venue) -> SessionEnvelopeFactory:
    return SessionEnvelopeFactory(
        venue=venue,
        collector_identity="fixture-adapter-v1",
        session_identity=f"fixture-{venue.value}",
        source_metadata_version=f"fixture-{venue.value}-metadata-v1",
        provenance=CaptureProvenance(
            collection_id=f"fixture-{venue.value}-collection",
            source_url=f"fixture://{venue.value}",
            transport="FIXTURE",
            fixture_label=SYNTHETIC_FIXTURE_LABEL,
        ),
    )


def _raw(name: str) -> bytes:
    value = (FIXTURES / name).read_bytes()
    assert SYNTHETIC_FIXTURE_LABEL.encode() in value
    return value


def test_hyperliquid_public_fixtures_build_causal_minimal_views() -> None:
    adapter = HyperliquidPublicAdapter()
    factory = _factory(Venue.HYPERLIQUID)
    envelopes = []
    for index, name in enumerate(
        ("hyperliquid_bbo.json", "hyperliquid_l2.json", "hyperliquid_trades.json")
    ):
        envelopes.append(
            adapter.envelope_from_websocket(
                _raw(name),
                factory=factory,
                receive_timestamp_utc_ns=1_800_000_000_000_000_000 + index,
                receive_monotonic_ns=10_000 + index,
            )
        )
    views = build_hyperliquid_views(envelopes)

    assert views.bbo[0].instrument_id == "HL:BTC:perp"
    assert str(views.bbo[0].spread) == "0.10"
    assert str(views.bbo[0].imbalance) == "0.1666666666666666666666666667"
    assert views.l2_snapshots[0].is_snapshot is True
    assert len(views.l2_snapshots[0].levels) == 4
    assert views.trades[0].aggressor_side == "BUY"
    assert views.trades[0].causal_liquidation_label is None
    assert all(item.source_sequence is None for item in envelopes)


def test_polymarket_and_kalshi_public_fixtures_preserve_ids_and_absent_sequence() -> None:
    polymarket = PolymarketPublicAdapter()
    pm_envelope = polymarket.envelope_from_websocket(
        _raw("polymarket_book.json"),
        factory=_factory(Venue.POLYMARKET),
        receive_timestamp_utc_ns=1_800_000_000_000_000_000,
        receive_monotonic_ns=20_000,
    )
    assert pm_envelope.feed_type == "order_book"
    assert pm_envelope.instrument_id == "PM:fixture-token-yes"
    assert pm_envelope.market_id == "PM:fixture-condition"
    assert pm_envelope.source_sequence is None

    kalshi = KalshiPublicAdapter()
    kalshi_envelope = kalshi.envelope_from_http(
        _raw("kalshi_market.json"),
        feed_type="markets",
        ticker="FIXTURE-MARKET",
        factory=_factory(Venue.KALSHI),
        receive_timestamp_utc_ns=1_800_000_000_000_000_000,
        receive_monotonic_ns=30_000,
    )
    assert kalshi_envelope.market_id == "KALSHI:FIXTURE-MARKET"
    assert kalshi_envelope.source_sequence is None
    orderbook = kalshi.envelope_from_http(
        _raw("kalshi_orderbook.json"),
        feed_type="order_book",
        ticker="FIXTURE-MARKET",
        factory=_factory(Venue.KALSHI),
        receive_timestamp_utc_ns=1_800_000_000_000_000_001,
        receive_monotonic_ns=30_001,
    )
    assert orderbook.feed_type == "order_book"
    assert orderbook.raw_payload == _raw("kalshi_orderbook.json")
    assert kalshi.websocket_subscriptions() == ()
    assert kalshi.websocket_limitation.endswith("AUTHENTICATED_WEBSOCKET_HANDSHAKE")


def test_only_allowlisted_public_routes_and_subscriptions_exist() -> None:
    specs = all_public_route_specs()
    for spec in specs:
        if isinstance(spec, PublicHttpRequest):
            assert spec.url.startswith("https://")
            assert not spec.url.startswith("https://api.hyperliquid.xyz/exchange")
            assert spec.method in {"GET", "POST"}
        else:
            assert isinstance(spec, PublicWebsocketSubscription)
            assert spec.url.startswith("wss://")
            payload = str(spec.payload).lower()
            assert "private" not in payload
            assert "api_key" not in payload
            assert "signature" not in payload
            assert "wallet" not in payload
    source_path = Path(adapters_module.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imports = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    assert "hyperliquid.exchange" not in imports
    assert HyperliquidPublicAdapter().unavailable_public_global_labels == (
        "TWAP_GLOBAL_PUBLIC_SOURCE_UNVERIFIED",
        "LIQUIDATION_GLOBAL_PUBLIC_SOURCE_UNVERIFIED",
    )
    assert any(spec.url == "https://data-api.polymarket.com/trades" for spec in specs)
    assert any("/events" in spec.url for spec in specs)


def test_private_or_non_allowlisted_transport_specs_are_refused() -> None:
    with pytest.raises(ValueError):
        PublicHttpRequest(method="GET", url="https://clob.polymarket.com/orders")
    with pytest.raises(ValueError):
        PublicHttpRequest(method="GET", url="https://not-official.example/markets")
    with pytest.raises(ValueError):
        PublicWebsocketSubscription(
            url="wss://ws-subscriptions-clob.polymarket.com/ws/user",
            payload={"type": "user"},
        )


def test_polymarket_public_metadata_prices_fees_and_trade_specs_are_explicit() -> None:
    adapter = PolymarketPublicAdapter()
    assert adapter.market_metadata_request("123").url.endswith("/markets/123")
    assert adapter.event_metadata_request("456").url.endswith("/events/456")
    parameters = adapter.token_parameter_requests(
        ("fixture-token",),
        feeds=("order_book", "last_trade_price", "tick_size", "fees"),
    )
    assert {feed for feed, _ in parameters} == {
        "fees",
        "last_trade_price",
        "order_book",
        "tick_size",
    }
    trades = adapter.public_trade_request(("fixture-condition",), limit=10)
    assert trades.url == "https://data-api.polymarket.com/trades"
    assert dict(trades.query) == {"limit": "10", "market": "fixture-condition"}
