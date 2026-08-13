from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from typer.testing import CliRunner

from hyperlab.cli import app
from hyperlab.collector.models import ParsedRecord
from hyperlab.data.lake import PartitionKey, write_partition
from hyperlab.data.schema import RecordType, schema_for
from hyperlab.strategies.market_making_l2 import (
    AdaptiveMarketMakerConfig,
    L2MarketMakingReplay,
    MarketMakingDataError,
    audit_market_making_records,
    load_market_making_records,
    write_market_making_report,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def _common(
    record_type: RecordType,
    *,
    venue: str = "hyperliquid",
    at_ms: int,
    sequence: int | None,
    connection: str = "c1",
) -> dict[str, object]:
    at = BASE + timedelta(milliseconds=at_ms)
    return {
        "schema_version": 1,
        "record_type": record_type.value,
        "venue": venue,
        "asset": "BTC",
        "event_time": at,
        "exchange_time": at,
        "received_time": at,
        "source_sequence": sequence,
        "connection_id": connection,
    }


def _snapshot(
    *,
    at_ms: int,
    sequence: int,
    bid: str,
    bid_qty: str,
    ask: str,
    ask_qty: str,
    venue: str = "hyperliquid",
    epoch: str = "epoch-1",
) -> list[ParsedRecord]:
    snapshot_id = f"{venue}-{at_ms}-{sequence}"
    header = _common(RecordType.L2_BOOK_STATE, venue=venue, at_ms=at_ms, sequence=sequence)
    header.update(
        {
            "snapshot_id": snapshot_id,
            "book_epoch_id": epoch,
            "bid_level_count": 1,
            "ask_level_count": 1,
        }
    )
    records: list[ParsedRecord] = [ParsedRecord(RecordType.L2_BOOK_STATE, "BTC", header)]
    for side, price, quantity in (("bid", bid, bid_qty), ("ask", ask, ask_qty)):
        row = _common(RecordType.L2_SNAPSHOT, venue=venue, at_ms=at_ms, sequence=sequence)
        row.update(
            {
                "snapshot_id": snapshot_id,
                "book_epoch_id": epoch,
                "last_sequence": sequence,
                "side": side,
                "level": 0,
                "price": Decimal(price),
                "quantity": Decimal(quantity),
                "order_count": 1,
            }
        )
        records.append(ParsedRecord(RecordType.L2_SNAPSHOT, "BTC", row))
    return records


def _delta(
    *,
    at_ms: int,
    first_sequence: int,
    last_sequence: int,
    side: str,
    price: str,
    quantity: str,
    action: str = "set",
    epoch: str = "epoch-1",
) -> ParsedRecord:
    row = _common(RecordType.L2_DELTA, at_ms=at_ms, sequence=last_sequence)
    row.update(
        {
            "update_id": f"u-{last_sequence}",
            "book_epoch_id": epoch,
            "first_sequence": first_sequence,
            "last_sequence": last_sequence,
            "side": side,
            "price": Decimal(price),
            "quantity": Decimal(quantity),
            "action": action,
        }
    )
    return ParsedRecord(RecordType.L2_DELTA, "BTC", row)


def _trade(
    *,
    at_ms: int,
    sequence: int,
    side: str,
    price: str,
    quantity: str,
) -> ParsedRecord:
    row = _common(RecordType.TRADE, at_ms=at_ms, sequence=sequence)
    row.update(
        {
            "trade_id": f"t-{sequence}",
            "aggressor_side": side,
            "price": Decimal(price),
            "quantity": Decimal(quantity),
            "quote_quantity": Decimal(price) * Decimal(quantity),
            "is_liquidation": False,
        }
    )
    return ParsedRecord(RecordType.TRADE, "BTC", row)


def _connection(*, at_ms: int, kind: str, epoch: str = "epoch-1") -> ParsedRecord:
    row = _common(RecordType.CONNECTION_EVENT, at_ms=at_ms, sequence=None)
    row.update(
        {
            "event_kind": kind,
            "channel": "l2Book:BTC",
            "book_epoch_id": epoch,
            "reason": "fixture",
            "expected_sequence": None,
            "observed_sequence": None,
            "resync_snapshot_id": None,
        }
    )
    return ParsedRecord(RecordType.CONNECTION_EVENT, "BTC", row)


def _config(**overrides: object) -> AdaptiveMarketMakerConfig:
    values: dict[str, object] = {
        "target_venue": "hyperliquid",
        "asset": "BTC",
        "order_size": 1.0,
        "max_order_size": 2.0,
        "max_inventory": 5.0,
        "maker_fee_bps": 0.0,
        "taker_fee_bps": 0.0,
        "minimum_half_spread_bps": 1.0,
        "toxicity_spread_bps": 0.0,
        "inventory_skew_bps": 0.0,
        "quote_latency_ms": 0,
        "cancel_latency_ms": 0,
        "replace_threshold_bps": 0.0,
        "toxicity_limit": 1.0,
        "toxicity_ewma_alpha": 1.0,
        "order_flow_scale": 10.0,
        "max_book_age_ms": 10_000,
        "markout_horizons_ms": (100, 1_000, 5_000),
    }
    values.update(overrides)
    return AdaptiveMarketMakerConfig(**values)


def test_queue_is_consumed_deterministically_and_partial_fill_preserves_priority() -> None:
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="2", ask="101", ask_qty="2"),
        _trade(at_ms=10, sequence=2, side="sell", price="99", quantity="2.5"),
        _trade(at_ms=20, sequence=3, side="sell", price="99", quantity="1.0"),
        *_snapshot(at_ms=5_020, sequence=4, bid="100", bid_qty="2", ask="102", ask_qty="2"),
    ]

    result = L2MarketMakingReplay(_config(size_toxicity_sensitivity=0.0)).run(records)

    assert [fill.quantity for fill in result.fills] == pytest.approx([0.5, 0.5])
    assert result.metrics.partial_fills == 2
    assert result.metrics.filled_units == pytest.approx(1.0)
    assert result.metrics.fill_ratio == pytest.approx(
        result.metrics.filled_units / result.metrics.quoted_units
    )
    assert result.metrics.maker_rate == pytest.approx(1.0)
    assert result.metrics.cancel_to_fill == pytest.approx(
        result.metrics.cancel_count / result.metrics.maker_fills
    )
    assert result.fills[0].quote_created_at == BASE


def test_cancel_replace_loses_priority_and_same_event_flow_cannot_fill_new_quote() -> None:
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        *_snapshot(at_ms=10, sequence=2, bid="100", bid_qty="5", ask="102", ask_qty="1"),
        _trade(at_ms=10, sequence=3, side="sell", price="100", quantity="5"),
        _trade(at_ms=20, sequence=4, side="sell", price="100", quantity="5.5"),
        *_snapshot(at_ms=5_020, sequence=5, bid="101", bid_qty="1", ask="103", ask_qty="1"),
    ]

    result = L2MarketMakingReplay(_config()).run(records)

    assert result.metrics.cancel_count >= 2
    assert result.metrics.replace_count >= 2
    assert len(result.fills) == 1
    assert result.fills[0].received_time == BASE + timedelta(milliseconds=20)
    assert result.fills[0].quantity == pytest.approx(0.5)
    assert result.fills[0].queue_ahead_before == pytest.approx(5.0)


def test_cancel_latency_leaves_the_old_quote_exposed_until_acknowledgement() -> None:
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        *_snapshot(at_ms=10, sequence=2, bid="100", bid_qty="1", ask="102", ask_qty="1"),
        _trade(at_ms=20, sequence=3, side="sell", price="99", quantity="1"),
        *_snapshot(at_ms=70, sequence=4, bid="101", bid_qty="1", ask="103", ask_qty="1"),
    ]

    result = L2MarketMakingReplay(_config(cancel_latency_ms=50, queue_ahead_fraction=0.0)).run(records)

    assert len(result.fills) == 1
    assert result.fills[0].price == pytest.approx(99.0)
    assert result.fills[0].quote_created_at == BASE
    assert result.metrics.replace_count >= 2


def test_displayed_cancellations_reduce_only_the_queue_ahead() -> None:
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="5", ask="101", ask_qty="1"),
        *_snapshot(at_ms=10, sequence=2, bid="99", bid_qty="2", ask="101", ask_qty="1"),
        _trade(at_ms=20, sequence=3, side="sell", price="99", quantity="2.5"),
        *_snapshot(at_ms=5_020, sequence=4, bid="100", bid_qty="1", ask="102", ask_qty="1"),
    ]

    result = L2MarketMakingReplay(_config()).run(records)

    assert len(result.fills) == 1
    assert result.fills[0].queue_ahead_before == pytest.approx(2.0)
    assert result.fills[0].quantity == pytest.approx(0.5)


def test_book_move_through_an_active_quote_is_not_assumed_filled() -> None:
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        *_snapshot(at_ms=10, sequence=2, bid="97", bid_qty="1", ask="98", ask_qty="1"),
    ]

    result = L2MarketMakingReplay(_config()).run(records)

    assert result.fills == ()
    assert result.metrics.unresolved_trade_throughs == 1
    assert result.metrics.quote_state_known is False
    assert result.status == "BLOCKED_UNRECONCILED_QUOTES"


def test_book_cross_before_quote_latency_is_a_post_only_reject_not_a_fill() -> None:
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        *_snapshot(at_ms=10, sequence=2, bid="97", bid_qty="1", ask="98", ask_qty="1"),
    ]

    result = L2MarketMakingReplay(_config(quote_latency_ms=50)).run(records)

    assert result.fills == ()
    assert result.metrics.post_only_rejects == 1
    assert result.metrics.unresolved_trade_throughs == 0


def test_all_trades_from_one_received_frame_hit_only_preexisting_quotes() -> None:
    first_trade = _trade(at_ms=10, sequence=2, side="sell", price="99", quantity="1")
    second_trade = _trade(at_ms=10, sequence=3, side="buy", price="101", quantity="1")
    second_row = dict(second_trade.row)
    second_row["connection_id"] = first_trade.row["connection_id"]
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        first_trade,
        ParsedRecord(RecordType.TRADE, "BTC", second_row),
        *_snapshot(at_ms=5_010, sequence=4, bid="99", bid_qty="1", ask="101", ask_qty="1"),
    ]

    result = L2MarketMakingReplay(_config(queue_ahead_fraction=0.0)).run(records)

    assert [(fill.side, fill.price) for fill in result.fills] == [
        ("buy", 99.0),
        ("sell", 101.0),
    ]


def test_minimum_spread_covers_fee_and_inventory_toxicity_reduce_size() -> None:
    records = [
        *_snapshot(
            at_ms=0,
            sequence=1,
            bid="99.99",
            bid_qty="1",
            ask="100.01",
            ask_qty="1",
        ),
        _trade(at_ms=10, sequence=2, side="sell", price="99.95", quantity="1"),
        *_snapshot(
            at_ms=20,
            sequence=3,
            bid="99.99",
            bid_qty="1",
            ask="100.01",
            ask_qty="1",
        ),
    ]

    result = L2MarketMakingReplay(
        _config(
            maker_fee_bps=5.0,
            minimum_half_spread_bps=1.0,
            inventory_skew_bps=100.0,
            size_toxicity_sensitivity=1.0,
            queue_ahead_fraction=0.0,
        )
    ).run(records)

    first = result.observations[0]
    after_fill = result.observations[1]
    assert first.quoted_bid == pytest.approx(99.95)
    assert first.quoted_ask == pytest.approx(100.05)
    assert after_fill.quoted_bid is not None and after_fill.quoted_bid < first.quoted_bid
    assert after_fill.quoted_bid_size == pytest.approx(0.9)
    assert after_fill.quoted_ask_size == pytest.approx(0.9)
    assert result.metrics.minimum_quote_size < result.metrics.maximum_quote_size


def test_multivenue_microprice_flow_spread_and_toxicity_withdrawal_are_causal() -> None:
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="9", ask="101", ask_qty="1"),
        *_snapshot(
            at_ms=1,
            sequence=1,
            bid="199",
            bid_qty="1",
            ask="201",
            ask_qty="1",
            venue="binance_usdm",
            epoch="binance-1",
        ),
        _trade(at_ms=10, sequence=2, side="buy", price="101", quantity="20"),
        *_snapshot(at_ms=20, sequence=3, bid="98", bid_qty="1", ask="100", ask_qty="9"),
    ]

    result = L2MarketMakingReplay(
        _config(
            venue_weights={"hyperliquid": 0.75, "binance_usdm": 0.25},
            toxicity_limit=0.2,
            toxicity_spread_bps=10.0,
        )
    ).run(records)

    first = result.observations[0]
    assert first.microprice == pytest.approx(100.8)
    assert first.imbalance == pytest.approx(0.8)
    assert result.observations[1].fair_value == pytest.approx(125.6)
    assert result.metrics.toxic_withdrawals >= 1
    assert result.metrics.minimum_quoted_half_spread_bps >= 1.0
    assert result.final_bid is None
    assert result.final_ask is None


def test_unconfigured_reference_cannot_claim_a_multivenue_fair_value() -> None:
    records = [
        *_snapshot(at_ms=0, sequence=10, bid="99", bid_qty="5", ask="101", ask_qty="5"),
        *_snapshot(
            at_ms=1,
            sequence=20,
            bid="99",
            bid_qty="5",
            ask="101",
            ask_qty="5",
            venue="binance_usdm",
        ),
    ]
    result = L2MarketMakingReplay(
        _config(
            calibration_status="CALIBRATED",
            calibration_evidence_hash="a" * 64,
            data_label="IMMUTABLE_LAKE_REPLAY",
        )
    ).run(records)

    assert result.metrics.observed_venues == ("binance_usdm", "hyperliquid")
    assert result.status == "BLOCKED_SINGLE_VENUE"


def test_stale_configured_reference_withdraws_quotes_instead_of_falling_back() -> None:
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        *_snapshot(
            at_ms=1,
            sequence=1,
            bid="99",
            bid_qty="1",
            ask="101",
            ask_qty="1",
            venue="binance_usdm",
            epoch="binance-1",
        ),
        *_snapshot(at_ms=100, sequence=2, bid="100", bid_qty="1", ask="102", ask_qty="1"),
    ]

    result = L2MarketMakingReplay(
        _config(
            venue_weights={"hyperliquid": 0.75, "binance_usdm": 0.25},
            max_book_age_ms=50,
        )
    ).run(records)

    assert result.metrics.observed_venues == ("binance_usdm", "hyperliquid")
    assert result.final_bid is None
    assert result.final_ask is None


def test_future_book_changes_cannot_rewrite_prior_quotes_or_fills() -> None:
    prefix = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        _trade(at_ms=10, sequence=2, side="sell", price="99", quantity="1"),
        *_snapshot(at_ms=110, sequence=3, bid="98", bid_qty="1", ask="100", ask_qty="1"),
    ]
    base = [
        *prefix,
        *_snapshot(at_ms=1_010, sequence=4, bid="97", bid_qty="1", ask="99", ask_qty="1"),
    ]
    changed = [
        *prefix,
        *_snapshot(at_ms=1_010, sequence=4, bid="197", bid_qty="1", ask="199", ask_qty="1"),
    ]
    config = _config(queue_ahead_fraction=0.0)

    base_result = L2MarketMakingReplay(config).run(base)
    changed_result = L2MarketMakingReplay(config).run(changed)

    cutoff = BASE + timedelta(seconds=1)
    base_prefix = [item for item in base_result.observations if item.received_time < cutoff]
    changed_prefix = [item for item in changed_result.observations if item.received_time < cutoff]
    assert base_prefix == changed_prefix
    assert [
        (fill.received_time, fill.side, fill.price, fill.quantity)
        for fill in base_result.fills
        if fill.received_time < cutoff
    ] == [
        (fill.received_time, fill.side, fill.price, fill.quantity)
        for fill in changed_result.fills
        if fill.received_time < cutoff
    ]


def test_markouts_split_spread_capture_from_adverse_selection() -> None:
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        _trade(at_ms=10, sequence=2, side="sell", price="99", quantity="1"),
        *_snapshot(at_ms=110, sequence=3, bid="97", bid_qty="1", ask="99", ask_qty="1"),
        *_snapshot(at_ms=1_010, sequence=4, bid="95", bid_qty="1", ask="97", ask_qty="1"),
        *_snapshot(at_ms=5_010, sequence=5, bid="93", bid_qty="1", ask="95", ask_qty="1"),
    ]

    result = L2MarketMakingReplay(_config(queue_ahead_fraction=0.0)).run(records)
    fill = result.fills[0]

    assert fill.spread_capture == pytest.approx(1.0)
    assert fill.markouts[100] == pytest.approx(-1.0)
    assert fill.markouts[1_000] == pytest.approx(-3.0)
    assert fill.markouts[5_000] == pytest.approx(-5.0)
    assert result.metrics.spread_pnl == pytest.approx(1.0)
    assert result.metrics.markout_pnl[100] == pytest.approx(-1.0)
    assert result.metrics.adverse_selection_pnl[100] == pytest.approx(-2.0)


def test_sequence_gap_fails_closed_until_explicit_resynchronization() -> None:
    records = [
        *_snapshot(at_ms=0, sequence=10, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        _delta(
            at_ms=10,
            first_sequence=12,
            last_sequence=12,
            side="bid",
            price="99",
            quantity="2",
        ),
        _trade(at_ms=20, sequence=13, side="sell", price="99", quantity="5"),
        _connection(at_ms=30, kind="resync_start", epoch="epoch-2"),
        *_snapshot(
            at_ms=40,
            sequence=20,
            bid="98",
            bid_qty="1",
            ask="100",
            ask_qty="1",
            epoch="epoch-2",
        ),
        _connection(at_ms=41, kind="resync_complete", epoch="epoch-2"),
        _trade(at_ms=50, sequence=21, side="sell", price="98", quantity="1"),
        *_snapshot(
            at_ms=5_050,
            sequence=22,
            bid="99",
            bid_qty="1",
            ask="101",
            ask_qty="1",
            epoch="epoch-2",
        ),
    ]

    result = L2MarketMakingReplay(_config(queue_ahead_fraction=0.0)).run(records)

    assert result.metrics.sequence_gaps == 1
    assert result.metrics.resynchronizations == 1
    assert len(result.fills) == 1
    assert result.fills[0].price == pytest.approx(98.0)


def test_disconnect_marks_abandoned_quotes_and_forbids_phantom_fills() -> None:
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        _connection(at_ms=10, kind="disconnect"),
        _trade(at_ms=20, sequence=2, side="sell", price="99", quantity="10"),
    ]

    result = L2MarketMakingReplay(_config(queue_ahead_fraction=0.0)).run(records)

    assert result.metrics.abandoned_quotes == 2
    assert result.metrics.outage_count == 1
    assert result.metrics.quote_state_known is False
    assert result.fills == ()
    assert result.status == "BLOCKED_UNRECONCILED_QUOTES"


def test_optional_hedge_is_taker_only_and_reported_separately() -> None:
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        _trade(at_ms=10, sequence=2, side="sell", price="99", quantity="1"),
        *_snapshot(at_ms=20, sequence=3, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        *_snapshot(
            at_ms=21,
            sequence=1,
            bid="100",
            bid_qty="3",
            ask="102",
            ask_qty="3",
            venue="binance_usdm",
            epoch="binance-1",
        ),
    ]

    result = L2MarketMakingReplay(
        _config(
            hedge_venue="binance_usdm",
            hedge_trigger_inventory=0.5,
            queue_ahead_fraction=0.0,
        )
    ).run(records)

    assert result.metrics.hedge_count == 1
    assert result.metrics.taker_fills == 1
    assert result.metrics.maker_rate == pytest.approx(0.5)
    assert result.metrics.taker_rate == pytest.approx(0.5)
    assert result.ending_inventory == pytest.approx(0.0)
    assert result.metrics.hedge_pnl == pytest.approx(-1.0)


@pytest.mark.parametrize(
    "records, message",
    [
        (
            [
                *_snapshot(
                    at_ms=0,
                    sequence=1,
                    bid="101",
                    bid_qty="1",
                    ask="100",
                    ask_qty="1",
                )
            ],
            "crossed",
        ),
        (
            [
                *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="1", ask="101", ask_qty="1"),
                _delta(
                    at_ms=10,
                    first_sequence=1,
                    last_sequence=1,
                    side="bid",
                    price="99",
                    quantity="2",
                ),
            ],
            "non-increasing",
        ),
    ],
)
def test_invalid_l2_replay_is_rejected(records: list[ParsedRecord], message: str) -> None:
    with pytest.raises(MarketMakingDataError, match=message):
        L2MarketMakingReplay(_config()).run(records)


def test_snapshot_header_must_match_the_exact_level_count() -> None:
    records = _snapshot(
        at_ms=0,
        sequence=1,
        bid="99",
        bid_qty="1",
        ask="101",
        ask_qty="1",
    )
    bad_header = dict(records[0].row)
    bad_header["ask_level_count"] = 2
    records[0] = ParsedRecord(RecordType.L2_BOOK_STATE, "BTC", bad_header)

    with pytest.raises(MarketMakingDataError, match="level count"):
        L2MarketMakingReplay(_config()).run(records)


def test_toy_simulator_remains_explicitly_labelled() -> None:
    from hyperlab.strategies.market_making import InventoryAwareMarketMaker

    assert InventoryAwareMarketMaker().simulation_label == "TOY"


def test_audit_requires_multivenue_sequences_resync_and_calibration() -> None:
    evidence_hash = "a" * 64
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        *_snapshot(
            at_ms=1,
            sequence=1,
            bid="99",
            bid_qty="1",
            ask="101",
            ask_qty="1",
            venue="binance_usdm",
            epoch="binance-1",
        ),
        _trade(at_ms=2, sequence=2, side="buy", price="101", quantity="1"),
        _connection(at_ms=3, kind="resync_complete"),
    ]

    audit = audit_market_making_records(
        records,
        asset="BTC",
        target_venue="hyperliquid",
        minimum_events=1,
        calibration_evidence_hash=evidence_hash,
    )

    assert audit.passed is True
    assert audit.reasons == ()

    without_sequence = []
    for record in records:
        row = dict(record.row)
        if record.record_type == RecordType.L2_SNAPSHOT and row["venue"] == "hyperliquid":
            row["last_sequence"] = None
        without_sequence.append(ParsedRecord(record.record_type, record.asset, row))
    blocked = audit_market_making_records(
        without_sequence,
        asset="BTC",
        target_venue="hyperliquid",
        minimum_events=1,
        calibration_evidence_hash=evidence_hash,
    )
    assert blocked.checks["target_sequences_observable"] is False
    assert blocked.passed is False

    ambiguous = [
        *records[:2],
        _trade(at_ms=0, sequence=9, side="buy", price="101", quantity="1"),
        *records[2:],
    ]
    tied = audit_market_making_records(
        ambiguous,
        asset="BTC",
        target_venue="hyperliquid",
        minimum_events=1,
        calibration_evidence_hash=evidence_hash,
    )
    assert tied.checks["receive_order_unambiguous"] is False


def _write_records(root: Path, records: list[ParsedRecord]) -> None:
    grouped: dict[tuple[str, RecordType], list[dict[str, object]]] = {}
    for record in records:
        venue = str(record.row["venue"])
        grouped.setdefault((venue, record.record_type), []).append(dict(record.row))
    for (venue, record_type), rows in grouped.items():
        spec = schema_for(record_type)
        rows.sort(
            key=lambda row: (
                row["received_time"],
                str(row.get("snapshot_id", "")),
                str(row.get("side", "")),
            )
        )
        table = pa.Table.from_pylist(rows, schema=spec.schema)
        write_partition(
            root,
            PartitionKey(venue, date(2026, 1, 1), "BTC", record_type),
            table,
        )


def test_loader_validates_lake_artifacts_and_report_is_reproducible(tmp_path: Path) -> None:
    records = [
        *_snapshot(at_ms=0, sequence=1, bid="99", bid_qty="1", ask="101", ask_qty="1"),
        *_snapshot(
            at_ms=1,
            sequence=1,
            bid="99",
            bid_qty="1",
            ask="101",
            ask_qty="1",
            venue="binance_usdm",
            epoch="binance-1",
        ),
        _trade(at_ms=10, sequence=2, side="sell", price="99", quantity="2"),
        _connection(at_ms=20, kind="resync_complete"),
    ]
    lake = tmp_path / "lake"
    _write_records(lake, records)

    loaded, manifests = load_market_making_records(
        lake,
        asset="BTC",
        venues=("hyperliquid", "binance_usdm"),
    )

    assert len(loaded) == len(records)
    assert len(manifests) == 6
    result = L2MarketMakingReplay(
        _config(
            venue_weights={"hyperliquid": 0.75, "binance_usdm": 0.25},
            queue_ahead_fraction=0.0,
        )
    ).run(loaded)
    audit = audit_market_making_records(
        loaded,
        asset="BTC",
        target_venue="hyperliquid",
        minimum_events=1,
        calibration_evidence_hash="b" * 64,
        manifest_hashes=(manifest.sha256 for manifest in manifests),
    )
    first_report = write_market_making_report(result, output_dir=tmp_path / "first", audit=audit)
    second_report = write_market_making_report(result, output_dir=tmp_path / "second", audit=audit)

    first_json = tmp_path / "first" / "market_making_summary.json"
    second_json = tmp_path / "second" / "market_making_summary.json"
    assert first_json.read_bytes() == second_json.read_bytes()
    assert first_json.read_bytes().endswith(b"\n")
    assert json.loads(first_json.read_text(encoding="utf-8"))["result"]["status"]
    assert "NO ORDER ROUTE" in first_report.read_text(encoding="utf-8")
    assert second_report.is_file()


def test_calibrated_status_rejects_toy_or_synthetic_labels() -> None:
    with pytest.raises(ValueError, match="cannot be declared CALIBRATED"):
        _config(
            calibration_status="CALIBRATED",
            calibration_evidence_hash="c" * 64,
            data_label="SYNTHETIC_TOY",
        )


def test_cli_exposes_only_audit_and_offline_replay_for_phase11() -> None:
    output = CliRunner().invoke(app, ["--help"])

    assert output.exit_code == 0
    assert "market-making-audit" in output.output
    assert "market-making-replay" in output.output
    assert "market-making-live" not in output.output
    assert "market-making-trade" not in output.output


def test_cli_audit_and_replay_fail_closed_on_an_empty_lake(tmp_path: Path) -> None:
    runner = CliRunner()
    audit_path = tmp_path / "audit.json"

    audit = runner.invoke(
        app,
        [
            "market-making-audit",
            "--data",
            str(tmp_path / "empty"),
            "--minimum-events",
            "1",
            "--output",
            str(audit_path),
        ],
    )
    replay_dir = tmp_path / "replay"
    replay = runner.invoke(
        app,
        [
            "market-making-replay",
            "--data",
            str(tmp_path / "empty"),
            "--minimum-events",
            "1",
            "--output",
            str(replay_dir),
        ],
    )

    assert audit.exit_code == 2
    assert json.loads(audit_path.read_text(encoding="utf-8"))["passed"] is False
    assert replay.exit_code == 2
    assert "BLOCKED_DATA_READINESS" in replay.output
    readiness = json.loads((replay_dir / "market_making_readiness.json").read_text(encoding="utf-8"))
    assert readiness["passed"] is False
