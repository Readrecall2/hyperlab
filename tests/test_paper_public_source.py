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
    PublicRecordAdapterError,
    PublicRecordMarketEventAdapter,
    PublicRecordQueueFull,
    PublicRecordSourceClosed,
)
from hyperlab.paper.runtime import PublicSourceDescriptor

INSTRUMENTS = {("hyperliquid", "BTC"): "HL:BTC:perp"}
START = datetime(2026, 8, 16, 12, 0, tzinfo=UTC)


def _adapter(*, queue_capacity: int = 2) -> PublicRecordMarketEventAdapter:
    return PublicRecordMarketEventAdapter(
        instruments=INSTRUMENTS,
        queue_capacity=queue_capacity,
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


def _connection(
    received_at: datetime,
    *,
    event_kind: str,
    asset: str = "BTC",
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
            "connection_epoch": 1,
            "capture_epoch_id": "capture-1",
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


def test_bilateral_bbo_preserves_received_time_and_stable_lineage_identity() -> None:
    record = _bbo(START, update_id="book-1")

    first = _event(_adapter().adapt(record))
    replay = _event(_adapter().adapt(record))

    assert first.received_at == START
    assert first.instrument == "HL:BTC:perp"
    assert first.bid_price == Decimal("100")
    assert first.ask_price == Decimal("101")
    assert first.bid_depth == Decimal("2")
    assert first.ask_depth == Decimal("3")
    assert first.tradable is True
    assert first.event_id == replay.event_id
    assert first.source_sequence == replay.source_sequence


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
def test_unilateral_nonpositive_or_crossed_bbo_is_withheld_fail_closed(
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

    assert adapter.adapt(_bbo(START, update_id="book-1", **values)) is None
    recovered = _event(
        adapter.adapt(
            _bbo(
                START + timedelta(milliseconds=1),
                update_id="book-2",
            )
        )
    )
    assert recovered.tradable is True


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

    recovered = _event(
        adapter.adapt(
            _bbo(START + timedelta(seconds=4), update_id="book-3")
        )
    )
    assert not recovered.gap and recovered.tradable


def test_multi_instrument_global_gap_is_terminal_and_blocks_every_stream() -> None:
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

    with pytest.raises(PublicRecordAdapterError, match="crash-atomically"):
        adapter.adapt(
            _connection(
                START + timedelta(milliseconds=2),
                event_kind="gap",
                asset="GLOBAL",
            )
        )

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
    assert _event(adapter.adapt(_bbo(START, update_id="valid"))).tradable is True


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


def test_adapter_identity_binds_mapping_schema_and_fifo_capacity() -> None:
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
    different_kind = PublicRecordMarketEventAdapter(
        instruments={("hyperliquid", "BTC"): "HL:BTC:spot"},
        queue_capacity=128,
    )

    assert forward.identity_hash == reversed_mapping.identity_hash
    assert forward.identity_artifact_bytes == reversed_mapping.identity_artifact_bytes
    assert different_capacity.identity_hash != forward.identity_hash
    assert different_kind.identity_hash != forward.identity_hash
    identity = json.loads(forward.identity_artifact_bytes)
    assert identity["feed_contract"].startswith("SOLE_COLLECTOR_")
    assert (
        identity["global_connection_policy"]
        == "MULTI_INSTRUMENT_GLOBAL_EVENT_TERMINAL_V1"
    )
    assert identity["normalized_record_schema_versions"] == {
        "bbo": 2,
        "connection_event": 2,
    }
    assert identity["paper_market_schema_version"] == 1
    assert identity["queue_capacity_frames"] == 128
    assert identity["instrument_route_policy"].startswith("EXPLICIT_MAPPING_")
    assert identity["source_venue_aliases"] == [
        {"paper_exchange": "HL", "source_venue": "hyperliquid"}
    ]
    assert identity["trade_projection"].startswith("BLOCKED_")


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


def test_bounded_source_is_fifo_and_saturation_is_terminal() -> None:
    adapter = _adapter(queue_capacity=1)
    source = _source(adapter)
    assert source.feed(_bbo(START, update_id="book-1")) is True

    with pytest.raises(PublicRecordQueueFull, match="coverage is incomplete"):
        source.feed(
            _bbo(
                START + timedelta(milliseconds=1),
                update_id="book-2",
            )
        )
    with pytest.raises(PublicRecordQueueFull, match="coverage is incomplete"):
        source.poll(timeout_seconds=0)


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
        record_type=RecordType.FUNDING,
        asset="BTC",
        row={"this": "record is outside the MarketEvent seam"},
    )

    assert _adapter().adapt(record) is None
