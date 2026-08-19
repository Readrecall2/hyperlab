from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from hyperlab.collector.models import ParsedRecord
from hyperlab.data.schema import RecordType, latest_schema_for
from hyperlab.paper.models import MarketEvent
from hyperlab.paper.public_source import (
    BoundedPublicRecordSource,
    PublicFundingSettlement,
    PublicRecordAdapterError,
    PublicRecordMarketEventAdapter,
    PublicRecordQueueFull,
    PublicRecordSourceClosed,
)
from hyperlab.paper.runtime import PublicSourceDescriptor

INSTRUMENTS = {("hyperliquid", "BTC"): "HL:BTC:perp"}
V10_INSTRUMENTS = {
    ("hyperliquid", "@107"): "HL:HYPE:spot",
    ("hyperliquid", "BTC"): "HL:BTC:perp",
}
V10_PRODUCT_IDENTITIES = {
    "HL:HYPE:spot": "1" * 64,
    "HL:BTC:perp": "2" * 64,
}
START = datetime(2026, 8, 16, 12, 0, tzinfo=UTC)


def _adapter(
    *,
    queue_capacity: int = 2,
    funding_dedupe_capacity: int = 4_096,
) -> PublicRecordMarketEventAdapter:
    return PublicRecordMarketEventAdapter(
        instruments=INSTRUMENTS,
        queue_capacity=queue_capacity,
        funding_dedupe_capacity=funding_dedupe_capacity,
    )


def _v10_adapter() -> PublicRecordMarketEventAdapter:
    return PublicRecordMarketEventAdapter(
        instruments=V10_INSTRUMENTS,
        queue_capacity=4,
        include_market_context=True,
        product_identity_hashes=V10_PRODUCT_IDENTITIES,
    )


def _common_row(
    record_type: RecordType,
    received_at: datetime,
    *,
    asset: str,
) -> dict[str, object]:
    row = {name: None for name in latest_schema_for(record_type).schema.names}
    row.update(
        {
            "schema_version": latest_schema_for(record_type).version,
            "record_type": record_type.value,
            "venue": "hyperliquid",
            "asset": asset,
            "event_time": received_at,
            "exchange_time": received_at,
            "received_time": received_at,
            "source_sequence": None,
            "connection_id": "public-connection-1",
        }
    )
    return row


def _bbo(
    received_at: datetime,
    *,
    update_id: str,
    asset: str = "BTC",
    bid_price: object = Decimal("100"),
    ask_price: object = Decimal("101"),
    bid_quantity: object = Decimal("2"),
    ask_quantity: object = Decimal("3"),
) -> ParsedRecord:
    row = _common_row(RecordType.BBO, received_at, asset=asset)
    row.update(
        {
            "update_id": update_id,
            "bid_price": bid_price,
            "ask_price": ask_price,
            "bid_quantity": bid_quantity,
            "ask_quantity": ask_quantity,
        }
    )
    return ParsedRecord(record_type=RecordType.BBO, asset=asset, row=row)


def _market_context(
    received_at: datetime,
    *,
    observation_id: str,
    asset: str = "@107",
    connection_id: str = "public-connection-1",
) -> ParsedRecord:
    row = _common_row(RecordType.MARKET_CONTEXT, received_at, asset=asset)
    kind = "spot" if asset.startswith("@") else "perp"
    row.update(
        {
            "connection_id": connection_id,
            "instrument_kind": kind,
            "instrument_id": f"HYPERLIQUID:{asset}:{kind}",
            "mark_price": Decimal("100"),
            "mid_price": Decimal("100"),
            "observation_id": observation_id,
        }
    )
    return ParsedRecord(record_type=RecordType.MARKET_CONTEXT, asset=asset, row=row)


def _trade(received_at: datetime, *, trade_id: str) -> ParsedRecord:
    row = _common_row(RecordType.TRADE, received_at, asset="BTC")
    row.update(
        {
            "trade_id": trade_id,
            "aggressor_side": "buy",
            "price": Decimal("100.5"),
            "quantity": Decimal("0.25"),
            "quote_quantity": None,
            "is_liquidation": False,
            "connection_epoch": 1,
            "arrival_sequence": 1,
        }
    )
    return ParsedRecord(record_type=RecordType.TRADE, asset="BTC", row=row)


def _funding(
    received_at: datetime,
    *,
    funding_rate: object = Decimal("0.00001"),
    observation_id: str = "funding-observation-1",
    asset: str = "BTC",
    funding_time: datetime | None = None,
) -> ParsedRecord:
    settled_at = START - timedelta(hours=1) if funding_time is None else funding_time
    row = _common_row(RecordType.FUNDING, received_at, asset=asset)
    row.update(
        {
            "event_time": settled_at,
            "exchange_time": settled_at,
            "funding_time": settled_at,
            "funding_rate": funding_rate,
            "funding_interval_seconds": 3_600,
            "rate_kind": "hyperliquid-hourly-settlement",
            "mark_price": None,
            "oracle_price": None,
            "observation_id": observation_id,
        }
    )
    return ParsedRecord(record_type=RecordType.FUNDING, asset=asset, row=row)


def _connection(
    received_at: datetime,
    *,
    event_kind: str,
    asset: str = "BTC",
    connection_epoch: int = 1,
) -> ParsedRecord:
    row = _common_row(RecordType.CONNECTION_EVENT, received_at, asset=asset)
    row.update(
        {
            "exchange_time": None,
            "event_kind": event_kind,
            "channel": "bbo",
            "book_epoch_id": None,
            "reason": None,
            "expected_sequence": None,
            "observed_sequence": None,
            "resync_snapshot_id": None,
            "connection_epoch": connection_epoch,
            "capture_epoch_id": f"capture-{connection_epoch}",
            "socket_role": "market",
        }
    )
    return ParsedRecord(
        record_type=RecordType.CONNECTION_EVENT,
        asset=asset,
        row=row,
    )


def _event(frame: Mapping[str, MarketEvent] | None) -> MarketEvent:
    assert frame is not None
    assert len(frame) == 1
    return next(iter(frame.values()))


def _source(adapter: PublicRecordMarketEventAdapter) -> BoundedPublicRecordSource:
    return BoundedPublicRecordSource(
        descriptor=PublicSourceDescriptor(
            source="collector-fanout",
            data_hash=adapter.identity_hash,
        ),
        adapter=adapter,
        capacity=adapter.queue_capacity,
    )


def test_bilateral_bbo_without_admitted_ws_lineage_is_nontradable() -> None:
    record = _bbo(START, update_id="book-1")

    first = _event(_adapter().adapt(record))
    replay = _event(_adapter().adapt(record))

    assert first.received_at == START
    assert first.instrument == "HL:BTC:perp"
    assert first.bid_price == Decimal("100")
    assert first.ask_price == Decimal("101")
    assert first.bid_depth == Decimal("2")
    assert first.ask_depth == Decimal("3")
    assert first.tradable is False
    assert first.gap is False
    assert first.source_connection_id is None
    assert first.event_id == replay.event_id
    assert first.source_sequence == replay.source_sequence


def test_market_events_expose_exact_normalized_source_lineage() -> None:
    adapter = _adapter()
    bbo = _event(
        adapter.adapt(
            _bbo(
                START,
                update_id="1723810000000:BTC:public-connection-1:7:9",
            )
        )
    )

    assert bbo.source_event_kind == "bbo"
    assert bbo.source_connection_id == "public-connection-1"
    assert bbo.source_connection_epoch == 7
    assert bbo.tradable is False

    gap = _event(
        adapter.adapt(
            _connection(
                START + timedelta(seconds=1),
                event_kind="gap",
            )
        )
    )
    assert gap.source_event_kind == "gap"
    assert gap.source_connection_id == "public-connection-1"
    assert gap.source_connection_epoch == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("bid_price", None),
        ("ask_price", Decimal("0")),
        ("bid_quantity", Decimal("-1")),
        ("ask_quantity", None),
        ("bid_price", Decimal("102")),
    ],
)
def test_malformed_bootstrap_bbo_latches_source_terminal(
    field: str,
    value: object,
) -> None:
    values = {
        "bid_price": Decimal("100"),
        "ask_price": Decimal("101"),
        "bid_quantity": Decimal("2"),
        "ask_quantity": Decimal("3"),
    }
    values[field] = value
    adapter = _adapter()
    source = _source(adapter)

    with pytest.raises(PublicRecordAdapterError, match="supported BBO"):
        source.feed(_bbo(START, update_id="bootstrap-malformed", **values))
    with pytest.raises(PublicRecordAdapterError, match="supported BBO"):
        source.feed(
            _bbo(START + timedelta(milliseconds=1), update_id="later-valid")
        )
    with pytest.raises(PublicRecordAdapterError, match="supported BBO"):
        source.poll(timeout_seconds=0)


def test_malformed_bbo_after_valid_websocket_book_cannot_resume_invisibly() -> None:
    adapter = _adapter(queue_capacity=8)
    source = _source(adapter)
    rest = _bbo(
        START,
        update_id="rest:1723810000000:BTC:public-connection-1:1:1",
    )
    connect = _connection(START + timedelta(milliseconds=1), event_kind="connect")
    valid = _bbo(
        START + timedelta(milliseconds=2),
        update_id="1723810000000:BTC:public-connection-1:1:1",
    )

    assert source.feed(rest)
    assert not _event(source.poll(timeout_seconds=0)).tradable
    assert source.feed(connect)
    assert not _event(source.poll(timeout_seconds=0)).tradable
    assert source.feed(valid)
    assert _event(source.poll(timeout_seconds=0)).tradable

    with pytest.raises(PublicRecordAdapterError, match="supported BBO"):
        source.feed(
            _bbo(
                START + timedelta(milliseconds=3),
                update_id="1723810000000:BTC:public-connection-1:1:2",
                ask_quantity=None,
            )
        )
    with pytest.raises(PublicRecordAdapterError, match="supported BBO"):
        source.feed(
            _bbo(
                START + timedelta(milliseconds=4),
                update_id="1723810000000:BTC:public-connection-1:1:3",
            )
        )
    with pytest.raises(PublicRecordAdapterError, match="supported BBO"):
        source.poll(timeout_seconds=0)


def test_initial_global_connect_after_rest_bootstrap_is_health_only() -> None:
    adapter = PublicRecordMarketEventAdapter(
        instruments={
            ("hyperliquid", "BTC"): "HL:BTC:perp",
            ("hyperliquid", "ETH"): "HL:ETH:perp",
        },
        queue_capacity=2,
    )
    rest_btc = _event(
        adapter.adapt(
            _bbo(
                START,
                update_id="rest:1723810000000:BTC:public-connection-1:1:1",
            )
        )
    )
    rest_eth = _event(
        adapter.adapt(
            _bbo(
                START + timedelta(milliseconds=1),
                update_id="rest:1723810000001:ETH:public-connection-1:1:2",
                asset="ETH",
            )
        )
    )
    assert all(not event.tradable and not event.gap for event in (rest_btc, rest_eth))
    assert all(event.source_connection_id is None for event in (rest_btc, rest_eth))

    frame = adapter.adapt(
        _connection(
            START + timedelta(milliseconds=2),
            event_kind="connect",
            asset="GLOBAL",
            connection_epoch=2,
        )
    )

    assert isinstance(frame, Mapping)
    assert tuple(frame) == ("HL:BTC:perp", "HL:ETH:perp")
    assert all(not event.gap and not event.tradable for event in frame.values())
    assert all(event.source_event_kind == "connect" for event in frame.values())

    btc = _event(
        adapter.adapt(
            _bbo(
                START + timedelta(milliseconds=3),
                update_id="1723810000003:BTC:public-connection-1:2:3",
            )
        )
    )
    eth = _event(
        adapter.adapt(
            _bbo(
                START + timedelta(milliseconds=4),
                update_id="1723810000004:ETH:public-connection-1:2:4",
                asset="ETH",
            )
        )
    )
    assert btc.tradable and not btc.gap
    assert eth.tradable and not eth.gap
    assert btc.source_connection_epoch == eth.source_connection_epoch == 2


def test_gap_requires_resync_completion_and_then_a_fresh_book() -> None:
    adapter = _adapter()
    _event(adapter.adapt(_bbo(START, update_id="book-1")))

    gap = _event(
        adapter.adapt(
            _connection(START + timedelta(seconds=1), event_kind="gap")
        )
    )
    assert gap.gap and not gap.tradable

    still_blocked = _event(
        adapter.adapt(
            _bbo(START + timedelta(seconds=2), update_id="book-2")
        )
    )
    assert still_blocked.gap and not still_blocked.tradable

    completed = _event(
        adapter.adapt(
            _connection(
                START + timedelta(seconds=3),
                event_kind="resync_complete",
            )
        )
    )
    assert completed.gap and not completed.tradable

    connected = _event(
        adapter.adapt(
            _connection(
                START + timedelta(seconds=4),
                event_kind="connect",
            )
        )
    )
    assert connected.gap and not connected.tradable

    recovered = _event(
        adapter.adapt(
            _bbo(
                START + timedelta(seconds=5),
                update_id="1723810000005:BTC:public-connection-1:1:5",
            )
        )
    )
    assert not recovered.gap and recovered.tradable


def test_multi_instrument_global_gap_is_one_frame_and_blocks_every_stream() -> None:
    adapter = PublicRecordMarketEventAdapter(
        instruments={
            ("hyperliquid", "BTC"): "HL:BTC:perp",
            ("hyperliquid", "ETH"): "HL:ETH:perp",
        },
        queue_capacity=2,
    )
    _event(adapter.adapt(_bbo(START, update_id="book-1")))
    _event(
        adapter.adapt(
            _bbo(
                START + timedelta(milliseconds=1),
                update_id="book-2",
                asset="ETH",
            )
        )
    )

    global_frame = adapter.adapt(
        _connection(
            START + timedelta(milliseconds=2),
            event_kind="gap",
            asset="GLOBAL",
        )
    )
    assert isinstance(global_frame, Mapping)
    assert tuple(global_frame) == ("HL:BTC:perp", "HL:ETH:perp")
    assert all(event.gap and not event.tradable for event in global_frame.values())
    assert tuple(event.capture_ordinal for event in global_frame.values()) == (1, 2)

    blocked_btc = _event(
        adapter.adapt(
            _bbo(
                START + timedelta(milliseconds=3),
                update_id="book-3",
            )
        )
    )
    blocked_eth = _event(
        adapter.adapt(
            _bbo(
                START + timedelta(milliseconds=4),
                update_id="book-4",
                asset="ETH",
            )
        )
    )
    assert blocked_btc.gap and not blocked_btc.tradable
    assert blocked_eth.gap and not blocked_eth.tradable


def test_global_connection_fanout_ordinals_are_stable_across_mapping_order() -> None:
    mappings = (
        {
            ("hyperliquid", "BTC"): "HL:BTC:perp",
            ("hyperliquid", "ETH"): "HL:ETH:perp",
        },
        {
            ("hyperliquid", "ETH"): "HL:ETH:perp",
            ("hyperliquid", "BTC"): "HL:BTC:perp",
        },
    )
    fingerprints: list[tuple[tuple[str, int, str], ...]] = []
    global_record = _connection(
        START + timedelta(milliseconds=2),
        event_kind="disconnect",
        asset="GLOBAL",
    )

    for instruments in mappings:
        adapter = PublicRecordMarketEventAdapter(
            instruments=instruments,
            queue_capacity=2,
        )
        assert adapter.adapt(_bbo(START, update_id="book-1")) is not None
        assert (
            adapter.adapt(
                _bbo(
                    START + timedelta(milliseconds=1),
                    update_id="book-2",
                    asset="ETH",
                )
            )
            is not None
        )
        frame = adapter.adapt(global_record)
        assert isinstance(frame, Mapping)
        fingerprints.append(
            tuple(
                (instrument, event.capture_ordinal, event.event_id)
                for instrument, event in frame.items()
            )
        )

    assert fingerprints[0] == fingerprints[1]
    assert tuple((instrument, ordinal) for instrument, ordinal, _ in fingerprints[0]) == (
        ("HL:BTC:perp", 1),
        ("HL:ETH:perp", 2),
    )


@pytest.mark.parametrize(
    "case",
    ["missing_version", "legacy_version", "wrong_type", "extra_field"],
)
def test_exact_latest_normalized_schema_is_enforced_before_state_mutation(
    case: str,
) -> None:
    adapter = _adapter()
    record = _bbo(START + timedelta(seconds=1), update_id="invalid")
    row = dict(record.row)
    if case == "missing_version":
        del row["schema_version"]
    elif case == "legacy_version":
        row["schema_version"] = 1
    elif case == "wrong_type":
        row["record_type"] = RecordType.TRADE.value
    else:
        row["unexpected"] = "not in schema"

    with pytest.raises(PublicRecordAdapterError):
        adapter.adapt(ParsedRecord(RecordType.BBO, "BTC", row))

    # Validation precedes the arrival-order mutation, so a valid earlier row remains admissible.
    assert _event(adapter.adapt(_bbo(START, update_id="valid"))).tradable is False


def test_non_decimal_bbo_scalar_latches_bounded_source_terminal() -> None:
    adapter = _adapter()
    source = _source(adapter)

    with pytest.raises(PublicRecordAdapterError, match="finite Decimal"):
        source.feed(
            _bbo(
                START,
                update_id="wrong-scalar-type",
                bid_price="100",
            )
        )
    with pytest.raises(PublicRecordAdapterError, match="finite Decimal"):
        source.poll(timeout_seconds=0)


def test_trade_projection_is_disabled_and_latches_bounded_source_terminal() -> None:
    adapter = _adapter()
    source = _source(adapter)

    with pytest.raises(PublicRecordAdapterError, match="restart-durable"):
        source.feed(_trade(START, trade_id="stable-trade"))
    with pytest.raises(PublicRecordAdapterError, match="restart-durable"):
        source.poll(timeout_seconds=0)


def test_public_funding_settlement_has_stable_identity_and_rejects_corrections() -> None:
    adapter = _adapter()
    record = _funding(START)

    first = adapter.adapt(record)
    replay = _adapter().adapt(record)

    assert isinstance(first, PublicFundingSettlement)
    assert isinstance(replay, PublicFundingSettlement)
    assert first.event_id == replay.event_id
    assert first.instrument == "HL:BTC:perp"
    assert first.funding_time == START - timedelta(hours=1)
    assert first.received_at == START
    assert first.funding_rate == Decimal("0.00001")
    assert first.funding_interval_seconds == 3_600

    assert (
        adapter.adapt(
            _funding(
                START + timedelta(seconds=1),
                observation_id="funding-observation-2",
            )
        )
        is None
    )
    with pytest.raises(PublicRecordAdapterError, match="correction conflicts"):
        adapter.adapt(
            _funding(
                START + timedelta(seconds=2),
                funding_rate=Decimal("0.00002"),
                observation_id="funding-observation-correction",
            )
        )


def test_funding_dedupe_window_is_bounded_and_recently_used() -> None:
    adapter = _adapter(funding_dedupe_capacity=2)
    first_time = START - timedelta(hours=3)
    second_time = START - timedelta(hours=2)
    third_time = START - timedelta(hours=1)

    assert isinstance(
        adapter.adapt(_funding(START, funding_time=first_time)),
        PublicFundingSettlement,
    )
    assert isinstance(
        adapter.adapt(_funding(START, funding_time=second_time)),
        PublicFundingSettlement,
    )
    assert (
        adapter.adapt(
            _funding(
                START + timedelta(seconds=1),
                observation_id="first-replay",
                funding_time=first_time,
            )
        )
        is None
    )
    assert isinstance(
        adapter.adapt(_funding(START, funding_time=third_time)),
        PublicFundingSettlement,
    )
    assert isinstance(
        adapter.adapt(
            _funding(
                START + timedelta(seconds=2),
                observation_id="second-after-eviction",
                funding_time=second_time,
            )
        ),
        PublicFundingSettlement,
    )


@pytest.mark.parametrize("capacity", [0, True])
def test_funding_dedupe_capacity_must_be_a_positive_integer(capacity: int) -> None:
    with pytest.raises(ValueError, match="funding_dedupe_capacity"):
        PublicRecordMarketEventAdapter(
            instruments=INSTRUMENTS,
            queue_capacity=2,
            funding_dedupe_capacity=capacity,
        )


def test_funding_timestamp_mismatch_latches_bounded_source_terminal() -> None:
    adapter = _adapter()
    source = _source(adapter)
    record = _funding(START)
    row = dict(record.row)
    row["event_time"] = START - timedelta(hours=2)

    with pytest.raises(PublicRecordAdapterError, match="must equal funding_time"):
        source.feed(ParsedRecord(RecordType.FUNDING, "BTC", row))
    with pytest.raises(PublicRecordAdapterError, match="must equal funding_time"):
        source.poll(timeout_seconds=0)


def test_funding_correction_latches_bounded_source_terminal() -> None:
    adapter = _adapter()
    source = _source(adapter)
    source.feed(_funding(START))
    assert isinstance(source.poll(timeout_seconds=0), PublicFundingSettlement)

    with pytest.raises(PublicRecordAdapterError, match="correction conflicts"):
        source.feed(
            _funding(
                START + timedelta(seconds=1),
                funding_rate=Decimal("0.00002"),
                observation_id="funding-observation-correction",
            )
        )
    with pytest.raises(PublicRecordAdapterError, match="correction conflicts"):
        source.poll(timeout_seconds=0)


def test_bounded_source_emits_funding_and_tracks_fifo_high_water() -> None:
    adapter = _adapter()
    source = _source(adapter)

    assert source.feed(_funding(START)) is True
    assert source.pending_count == 1
    assert source.high_water == 1
    item = source.poll(timeout_seconds=0)
    assert isinstance(item, PublicFundingSettlement)
    assert source.pending_count == 0


def test_async_funding_refresh_does_not_advance_websocket_receipt_order() -> None:
    adapter = _adapter()

    assert isinstance(
        adapter.adapt(_funding(START + timedelta(seconds=10))),
        PublicFundingSettlement,
    )
    book = _event(adapter.adapt(_bbo(START, update_id="book-after-refresh")))
    assert not book.tradable and book.received_at == START


def test_adapter_identity_binds_canonical_transport_context() -> None:
    first = PublicRecordMarketEventAdapter(
        instruments=INSTRUMENTS,
        queue_capacity=2,
        identity_context={
            "channels": ["bbo"],
            "websocket_endpoint": "wss://api.hyperliquid.xyz/ws",
        },
    )
    second = PublicRecordMarketEventAdapter(
        instruments=INSTRUMENTS,
        queue_capacity=2,
        identity_context={
            "channels": ["bbo"],
            "websocket_endpoint": "wss://different.invalid/ws",
        },
    )

    assert first.identity_hash != second.identity_hash
    identity = json.loads(first.identity_artifact_bytes)
    assert identity["transport"] == {
        "channels": ["bbo"],
        "websocket_endpoint": "wss://api.hyperliquid.xyz/ws",
    }


def test_adapter_identity_binds_mapping_schema_capacity_and_coalescing() -> None:
    forward = PublicRecordMarketEventAdapter(
        instruments={
            ("hyperliquid", "BTC"): "HL:BTC:perp",
            ("hyperliquid", "ETH"): "HL:ETH:perp",
        },
        queue_capacity=128,
    )
    reversed_mapping = PublicRecordMarketEventAdapter(
        instruments={
            ("hyperliquid", "ETH"): "HL:ETH:perp",
            ("hyperliquid", "BTC"): "HL:BTC:perp",
        },
        queue_capacity=128,
    )
    different_capacity = PublicRecordMarketEventAdapter(
        instruments=forward.instruments,
        queue_capacity=129,
    )
    different_dedupe_capacity = PublicRecordMarketEventAdapter(
        instruments=forward.instruments,
        queue_capacity=128,
        funding_dedupe_capacity=4_095,
    )
    different_kind = PublicRecordMarketEventAdapter(
        instruments={("hyperliquid", "BTC"): "HL:BTC:spot"},
        queue_capacity=128,
    )

    assert forward.identity_hash == reversed_mapping.identity_hash
    assert forward.identity_artifact_bytes == reversed_mapping.identity_artifact_bytes
    assert different_capacity.identity_hash != forward.identity_hash
    assert different_kind.identity_hash != forward.identity_hash
    identity = json.loads(forward.identity_artifact_bytes)
    assert different_dedupe_capacity.identity_hash != forward.identity_hash
    assert identity["feed_contract"].startswith("SOLE_COLLECTOR_")
    assert identity["adapter_schema_version"] == 9
    assert (
        identity["pending_bbo_coalescing"]
        == "LATEST_PER_INSTRUMENT_PER_UTC_MINUTE_BETWEEN_CONTROL_BARRIERS_V1"
    )
    assert (
        identity["global_connection_policy"]
        == "MULTI_INSTRUMENT_GLOBAL_EVENT_SORTED_ORDINAL_INITIAL_BOOTSTRAP_CONNECT_HEALTH_ONLY_V4"
    )
    assert (
        identity["bbo_tradability_policy"]
        == "REST_BOOTSTRAP_NONTRADABLE_POST_CONNECT_EXACT_WEBSOCKET_LINEAGE_REQUIRED_"
        "MALFORMED_TERMINAL_V2"
    )
    assert (
        identity["malformed_bbo_policy"]
        == "TERMINAL_SOURCE_FAILURE_RESTART_AND_RESYNC_REQUIRED_NO_SILENT_DROP_V1"
    )
    assert identity["normalized_record_schema_versions"] == {
        "bbo": 2,
        "connection_event": 2,
        "funding": 2,
    }
    assert identity["paper_market_schema_version"] == 1
    assert identity["funding_dedupe_capacity_settlements"] == 4_096
    assert identity["queue_capacity_frames"] == 128
    assert identity["instrument_route_policy"].startswith("EXPLICIT_MAPPING_")
    assert identity["source_venue_aliases"] == [
        {"paper_exchange": "HL", "source_venue": "hyperliquid"}
    ]
    assert identity["trade_projection"].startswith("BLOCKED_")
    assert identity["transport"] == {}


def test_source_rejects_unbound_descriptor_or_capacity() -> None:
    adapter = _adapter(queue_capacity=2)
    with pytest.raises(ValueError, match="canonical source identity"):
        BoundedPublicRecordSource(
            descriptor=PublicSourceDescriptor(
                source="collector-fanout",
                data_hash="a" * 64,
            ),
            adapter=adapter,
            capacity=2,
        )
    with pytest.raises(ValueError, match="frozen in the source identity"):
        BoundedPublicRecordSource(
            descriptor=PublicSourceDescriptor(
                source="collector-fanout",
                data_hash=adapter.identity_hash,
            ),
            adapter=adapter,
            capacity=1,
        )


@pytest.mark.parametrize(
    "instrument",
    ["HL:ETH:perp", "HYPERLIQUID:BTC:perp"],
)
def test_mapping_must_match_canonical_paper_instrument_identity(
    instrument: str,
) -> None:
    with pytest.raises(ValueError, match="must match the canonical HL paper instrument"):
        PublicRecordMarketEventAdapter(
            instruments={
                ("hyperliquid", "BTC"): instrument,
            },
            queue_capacity=2,
        )


def test_received_time_regression_is_rejected() -> None:
    adapter = _adapter()
    _event(adapter.adapt(_bbo(START, update_id="book-1")))

    with pytest.raises(PublicRecordAdapterError, match="out of received_time order"):
        adapter.adapt(
            _bbo(
                START - timedelta(microseconds=1),
                update_id="book-2",
            )
        )


def test_v10_accepts_overlapping_received_times_in_authoritative_causal_order() -> None:
    adapter = _v10_adapter()
    source = _source(adapter)

    assert source.feed(
        _bbo(
            START,
            update_id="1723809600000:BTC:public-connection-1:1:10",
        )
    )
    assert source.feed(
        _market_context(
            START - timedelta(milliseconds=2),
            observation_id="public-connection-1:1:11",
        )
    ) is False
    assert _event(source.poll(timeout_seconds=0)).instrument == "HL:BTC:perp"


def test_v10_rejects_genuine_per_connection_causal_regression() -> None:
    adapter = _v10_adapter()
    _event(
        adapter.adapt(
            _bbo(
                START,
                update_id="1723809600000:BTC:public-connection-1:1:10",
            )
        )
    )

    with pytest.raises(PublicRecordAdapterError, match="arrival_sequence"):
        adapter.adapt(
            _market_context(
                START + timedelta(milliseconds=2),
                observation_id="public-connection-1:1:9",
            )
        )


def test_bounded_source_coalesces_pending_bbo_within_one_utc_minute() -> None:
    adapter = _adapter(queue_capacity=1)
    source = _source(adapter)
    assert source.feed(_bbo(START, update_id="book-1")) is True
    assert source.feed(
        _bbo(
            START + timedelta(seconds=10),
            update_id="book-2",
            bid_price=Decimal("102"),
            ask_price=Decimal("103"),
        )
    ) is True

    snapshot = source.queue_snapshot(as_of=START + timedelta(seconds=10))
    assert snapshot == {
        "capacity_frames": 1,
        "pending_frames": 1,
        "high_water_frames": 1,
        "adapted_items": 2,
        "enqueued_items": 2,
        "polled_items": 0,
        "coalesced_bbo_frames": 1,
        "oldest_pending_received_at": "2026-08-16T12:00:10.000000+00:00",
        "newest_pending_received_at": "2026-08-16T12:00:10.000000+00:00",
        "oldest_pending_age_seconds": 0.0,
        "latest_adapted_received_at": "2026-08-16T12:00:10.000000+00:00",
        "latest_adapted_age_seconds": 0.0,
        "pending_bbo_coalescing": (
            "LATEST_PER_INSTRUMENT_PER_UTC_MINUTE_BETWEEN_CONTROL_BARRIERS_V1"
        ),
    }
    latest = _event(source.poll(timeout_seconds=0))
    assert latest.received_at == START + timedelta(seconds=10)
    assert latest.bid_price == Decimal("102")
    assert source.pending_count == 0


def test_slower_consumer_stays_current_under_sustained_bbo_burst() -> None:
    adapter = PublicRecordMarketEventAdapter(
        instruments={
            ("hyperliquid", "BTC"): "HL:BTC:perp",
            ("hyperliquid", "ETH"): "HL:ETH:perp",
        },
        queue_capacity=4_096,
    )
    source = _source(adapter)
    last_consumed_at: datetime | None = None

    for second in range(30):
        for ordinal in range(20):
            asset = "BTC" if ordinal % 2 == 0 else "ETH"
            assert source.feed(
                _bbo(
                    START + timedelta(seconds=second, milliseconds=ordinal),
                    asset=asset,
                    update_id=f"{asset}-{second}-{ordinal}",
                )
            )
        for _ in range(10):
            item = source.poll(timeout_seconds=0)
            if item is None:
                break
            last_consumed_at = max(event.received_at for event in item.values())

    produced_at = START + timedelta(seconds=29, milliseconds=19)
    assert last_consumed_at is not None
    assert (produced_at - last_consumed_at).total_seconds() <= 0.001
    snapshot = source.queue_snapshot(as_of=produced_at)
    assert snapshot["pending_frames"] == 0
    assert snapshot["high_water_frames"] == 2
    assert snapshot["adapted_items"] == 600
    assert snapshot["enqueued_items"] == 600
    assert snapshot["polled_items"] == 60
    assert snapshot["coalesced_bbo_frames"] == 540
    assert snapshot["latest_adapted_age_seconds"] == 0.0


def test_bounded_source_preserves_minute_boundary_and_saturation_is_terminal() -> None:
    adapter = _adapter(queue_capacity=1)
    source = _source(adapter)
    assert source.feed(_bbo(START + timedelta(seconds=59), update_id="book-1")) is True

    with pytest.raises(PublicRecordQueueFull, match="coverage is incomplete"):
        source.feed(
            _bbo(
                START + timedelta(minutes=1),
                update_id="book-2",
            )
        )
    with pytest.raises(PublicRecordQueueFull, match="coverage is incomplete"):
        source.poll(timeout_seconds=0)


def test_bounded_source_never_coalesces_across_funding_barrier() -> None:
    adapter = _adapter(queue_capacity=3)
    source = _source(adapter)
    first_at = START + timedelta(seconds=1)
    funding_at = START + timedelta(seconds=2)
    latest_at = START + timedelta(seconds=3)

    assert source.feed(_bbo(first_at, update_id="book-before-funding"))
    assert source.feed(_funding(funding_at, observation_id="funding-barrier"))
    assert source.feed(_bbo(latest_at, update_id="book-after-funding"))

    assert _event(source.poll(timeout_seconds=0)).received_at == first_at
    assert isinstance(source.poll(timeout_seconds=0), PublicFundingSettlement)
    assert _event(source.poll(timeout_seconds=0)).received_at == latest_at
    assert source.queue_snapshot(as_of=latest_at)["coalesced_bbo_frames"] == 0


def test_source_stop_and_close_reject_later_feeds() -> None:
    adapter = _adapter()
    source = _source(adapter)
    source.stop()

    assert source.poll(timeout_seconds=0) is None
    with pytest.raises(PublicRecordSourceClosed):
        source.feed(_bbo(START, update_id="book-1"))
    source.close()


def test_unrelated_public_record_types_are_ignored() -> None:
    record = ParsedRecord(
        record_type=RecordType.OPEN_INTEREST,
        asset="BTC",
        row={"this": "record is outside the MarketEvent seam"},
    )

    assert _adapter().adapt(record) is None
