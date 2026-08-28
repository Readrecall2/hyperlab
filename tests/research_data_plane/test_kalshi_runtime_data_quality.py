from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from hyperlab.research_data import probe as probe_module
from hyperlab.research_data.adapters import KALSHI_METADATA_VERSION, KalshiPublicAdapter
from hyperlab.research_data.canonical import canonical_json_bytes
from hyperlab.research_data.envelope import (
    SYNTHETIC_FIXTURE_LABEL,
    CaptureProvenance,
    SessionEnvelopeFactory,
    Venue,
)
from hyperlab.research_data.prediction_evidence import prediction_raw_records
from hyperlab.research_data.prediction_time import prediction_rfc3339_to_ns
from hyperlab.research_data.segments import ResearchSegmentReader, ResearchSegmentWriter

_STAMP = "2026-08-28T10:20:30.123456789Z"
_STAMP_NS = prediction_rfc3339_to_ns(_STAMP, label="fixture timestamp")


def _factory() -> SessionEnvelopeFactory:
    return SessionEnvelopeFactory(
        venue=Venue.KALSHI,
        collector_identity="fixture-kalshi-runtime-v1",
        session_identity="fixture-kalshi-runtime-session",
        source_metadata_version=KALSHI_METADATA_VERSION,
        provenance=CaptureProvenance(
            collection_id="fixture-kalshi-runtime-collection",
            source_url="fixture://kalshi/runtime-data-quality",
            transport="FIXTURE",
            fixture_label=SYNTHETIC_FIXTURE_LABEL,
        ),
    )


def _envelope(
    payload: object,
    *,
    feed_type: str,
    ticker: str = "FIXTURE-MARKET",
):
    raw = canonical_json_bytes(payload)
    envelope = KalshiPublicAdapter().envelope_from_http(
        raw,
        feed_type=feed_type,
        ticker=ticker,
        factory=_factory(),
        receive_timestamp_utc_ns=1_788_000_000_000_000_000,
        receive_monotonic_ns=10_000,
    )
    assert envelope.raw_payload == raw
    assert envelope.content_sha256 == hashlib.sha256(raw).hexdigest()
    return envelope


@pytest.mark.parametrize(
    ("feed_type", "ticker", "payload", "expected_timestamp_ns"),
    (
        (
            "series",
            "FIXTURE-SERIES",
            {"series": {"ticker": "FIXTURE-SERIES", "last_updated_ts": _STAMP}},
            _STAMP_NS,
        ),
        (
            "events",
            "FIXTURE-EVENT",
            {
                "event": {
                    "event_ticker": "FIXTURE-EVENT",
                    "last_updated_ts": _STAMP,
                    "series_ticker": "FIXTURE-SERIES",
                }
            },
            _STAMP_NS,
        ),
        (
            "markets",
            "FIXTURE-MARKET",
            {
                "market": {
                    "event_ticker": "FIXTURE-EVENT",
                    "ticker": "FIXTURE-MARKET",
                    "updated_time": _STAMP,
                }
            },
            _STAMP_NS,
        ),
        (
            "order_book",
            "FIXTURE-MARKET",
            {"orderbook_fp": {"no_dollars": [], "yes_dollars": []}},
            None,
        ),
        (
            "trades",
            "FIXTURE-MARKET",
            {
                "cursor": "",
                "trades": [
                    {
                        "count_fp": "1.00",
                        "created_time": _STAMP,
                        "is_block_trade": False,
                        "no_price_dollars": "0.6000",
                        "taker_book_side": "bid",
                        "taker_outcome_side": "yes",
                        "ticker": "FIXTURE-MARKET",
                        "trade_id": "FIXTURE-TRADE-1",
                        "yes_price_dollars": "0.4000",
                    }
                ],
            },
            _STAMP_NS,
        ),
        (
            "block_trades",
            "FIXTURE-MARKET",
            {
                "cursor": None,
                "trades": [
                    {
                        "count_fp": "2.00",
                        "created_time": _STAMP,
                        "is_block_trade": True,
                        "no_price_dollars": "0.5500",
                        "taker_book_side": "ask",
                        "taker_outcome_side": "no",
                        "ticker": "FIXTURE-MARKET",
                        "trade_id": "FIXTURE-BLOCK-1",
                        "yes_price_dollars": "0.4500",
                    }
                ],
            },
            _STAMP_NS,
        ),
        (
            "incentives",
            "GLOBAL",
            {
                "incentive_programs": [
                    {
                        "end_date": "2026-08-30T00:00:00Z",
                        "id": "FIXTURE-INCENTIVE-1",
                        "market_ticker": "FIXTURE-MARKET",
                        "start_date": "2026-08-28T00:00:00Z",
                    }
                ],
                "next_cursor": "",
            },
            None,
        ),
        (
            "fee_changes",
            "FIXTURE-SERIES",
            {
                "series_fee_change_arr": [
                    {
                        "fee_multiplier": 1,
                        "fee_type": "quadratic",
                        "id": "FIXTURE-SERIES-FEE-1",
                        "scheduled_ts": _STAMP,
                        "series_ticker": "FIXTURE-SERIES",
                    }
                ]
            },
            None,
        ),
        (
            "event_fee_changes",
            "FIXTURE-EVENT",
            {
                "cursor": "",
                "event_fee_changes": [
                    {
                        "event_ticker": "FIXTURE-EVENT",
                        "fee_multiplier_override": None,
                        "fee_type_override": None,
                        "id": "FIXTURE-EVENT-FEE-1",
                        "scheduled_ts": _STAMP,
                    }
                ],
            },
            None,
        ),
        (
            "event_metadata",
            "FIXTURE-EVENT",
            {
                "competition": None,
                "competition_scope": None,
                "market_details": [{"market_ticker": "FIXTURE-MARKET"}],
                "settlement_sources": [{"name": "Fixture", "url": "https://example.test"}],
            },
            None,
        ),
        (
            "exchange_status",
            "GLOBAL",
            {
                "exchange_active": True,
                "exchange_estimated_resume_time": None,
                "trading_active": True,
            },
            None,
        ),
        (
            "exchange_schedule",
            "GLOBAL",
            {
                "schedule": {
                    "maintenance_windows": [],
                    "standard_hours": [
                        {"end_time": "2026-12-31T23:59:59Z", "start_time": _STAMP}
                    ],
                }
            },
            None,
        ),
        (
            "historical_cutoff",
            "GLOBAL",
            {
                "market_settled_ts": _STAMP,
                "orders_updated_ts": _STAMP,
                "trades_created_ts": _STAMP,
            },
            None,
        ),
        (
            "historical_markets",
            "FIXTURE-MARKET",
            {
                "market": {
                    "event_ticker": "FIXTURE-EVENT",
                    "ticker": "FIXTURE-MARKET",
                    "updated_time": _STAMP,
                }
            },
            _STAMP_NS,
        ),
        (
            "historical_trades",
            "FIXTURE-MARKET",
            {
                "cursor": "",
                "trades": [
                    {
                        "count_fp": "1.00",
                        "created_time": _STAMP,
                        "is_block_trade": False,
                        "no_price_dollars": "0.6000",
                        "taker_book_side": "bid",
                        "taker_outcome_side": "yes",
                        "ticker": "FIXTURE-MARKET",
                        "trade_id": "FIXTURE-HISTORICAL-TRADE-1",
                        "yes_price_dollars": "0.4000",
                    }
                ],
            },
            _STAMP_NS,
        ),
    ),
)
def test_all_enabled_kalshi_feeds_build_raw_envelopes_with_explicit_time_semantics(
    feed_type: str,
    ticker: str,
    payload: object,
    expected_timestamp_ns: int | None,
) -> None:
    envelope = _envelope(payload, feed_type=feed_type, ticker=ticker)
    assert envelope.source_timestamp_ns == expected_timestamp_ns


@pytest.mark.parametrize(
    ("value", "match"),
    (
        ("not-a-time", "RFC3339"),
        ("1969-12-31T23:59:59Z", "non-negative"),
        (-1, "RFC3339 text"),
        (True, "RFC3339 text"),
        (1_788_000_000, "RFC3339 text"),
    ),
)
def test_kalshi_documented_rfc3339_fields_fail_closed_on_malformed_or_ambiguous_wire_values(
    value: object,
    match: str,
) -> None:
    payload = {
        "market": {
            "event_ticker": "FIXTURE-EVENT",
            "ticker": "FIXTURE-MARKET",
            "updated_time": value,
        }
    }
    with pytest.raises(ValueError, match=match):
        _envelope(payload, feed_type="markets")


def test_kalshi_rfc3339_offsets_and_fractions_normalize_exactly() -> None:
    utc = _envelope(
        {"market": {"ticker": "FIXTURE-MARKET", "updated_time": _STAMP}},
        feed_type="markets",
    )
    offset = _envelope(
        {
            "market": {
                "ticker": "FIXTURE-MARKET",
                "updated_time": "2026-08-28T12:20:30.123456789+02:00",
            }
        },
        feed_type="markets",
    )
    assert utc.source_timestamp_ns == offset.source_timestamp_ns == _STAMP_NS


def test_kalshi_pages_keep_cursor_and_raw_but_never_invent_a_scalar_timestamp() -> None:
    payload = {
        "cursor": "opaque/cursor==",
        "markets": [
            {"ticker": "FIXTURE-MARKET-A", "updated_time": "2026-08-28T10:00:00Z"},
            {"ticker": "FIXTURE-MARKET-B", "updated_time": "2026-08-28T10:01:00Z"},
        ],
    }
    envelope = _envelope(payload, feed_type="markets", ticker="CENSUS")
    assert envelope.source_cursor == "opaque/cursor=="
    assert envelope.source_timestamp_ns is None
    assert envelope.raw_payload == canonical_json_bytes(payload)


@pytest.mark.parametrize("cursor", (False, 0, 123, [], {}))
def test_kalshi_cursor_types_are_never_coerced(cursor: object) -> None:
    with pytest.raises(ValueError, match="cursor"):
        _envelope(
            {"cursor": cursor, "markets": []},
            feed_type="markets",
            ticker="CENSUS",
        )


def test_kalshi_empty_page_is_a_successful_raw_observation_not_unavailability() -> None:
    raw = canonical_json_bytes({"cursor": "", "markets": []})
    assert probe_module._kalshi_markets(raw, limit=5) == ()
    envelope = _envelope({"cursor": "", "markets": []}, feed_type="markets", ticker="CENSUS")
    assert envelope.source_cursor is None
    assert envelope.source_timestamp_ns is None
    assert prediction_raw_records(envelope) == ()


def test_kalshi_page_deduplication_keeps_only_the_first_scope_for_each_ticker() -> None:
    scopes = (
        ("MARKET-1", "EVENT-1", None),
        ("MARKET-1", "EVENT-DIVERGED", None),
        ("MARKET-2", "EVENT-2", None),
    )
    pager = probe_module.BoundedCursorPager(max_pages=2, max_items=5)
    admitted = pager.admit(
        requested_cursor=None,
        next_cursor="opaque-next",
        item_ids=[scope[0] for scope in scopes],
    )
    assert admitted == ("MARKET-1", "MARKET-2")
    assert probe_module._admitted_kalshi_scopes(scopes, admitted) == (
        ("MARKET-1", "EVENT-1", None),
        ("MARKET-2", "EVENT-2", None),
    )


def test_kalshi_fee_wrappers_and_cleared_override_survive_raw_record_projection() -> None:
    series = _envelope(
        {
            "series_fee_change_arr": [
                {
                    "fee_multiplier": 1,
                    "fee_type": "quadratic",
                    "id": "SERIES-FEE-1",
                    "scheduled_ts": _STAMP,
                    "series_ticker": "FIXTURE-SERIES",
                }
            ]
        },
        feed_type="fee_changes",
        ticker="FIXTURE-SERIES",
    )
    event = _envelope(
        {
            "cursor": "",
            "event_fee_changes": [
                {
                    "event_ticker": "FIXTURE-EVENT",
                    "fee_multiplier_override": None,
                    "fee_type_override": None,
                    "id": "EVENT-FEE-1",
                    "scheduled_ts": _STAMP,
                }
            ],
        },
        feed_type="event_fee_changes",
        ticker="FIXTURE-EVENT",
    )
    assert prediction_raw_records(series)[0]["id"] == "SERIES-FEE-1"
    projected_event = prediction_raw_records(event)[0]
    assert projected_event["id"] == "EVENT-FEE-1"
    assert projected_event["fee_type_override"] is None
    assert projected_event["fee_multiplier_override"] is None


def test_kalshi_trade_identity_and_block_classification_are_strict() -> None:
    valid = [
        {
            "is_block_trade": True,
            "ticker": "FIXTURE-MARKET",
            "trade_id": "BLOCK-1",
        }
    ]
    assert probe_module._kalshi_trade_ids(
        valid,
        expected_ticker="FIXTURE-MARKET",
        expected_block_trade=True,
    ) == ("BLOCK-1",)
    with pytest.raises(ValueError, match="block-trade"):
        probe_module._kalshi_trade_ids(
            valid,
            expected_ticker="FIXTURE-MARKET",
            expected_block_trade=False,
        )
    with pytest.raises(ValueError, match="identity"):
        probe_module._kalshi_trade_ids(
            [{"is_block_trade": False, "ticker": "FIXTURE-MARKET", "trade_id": 7}],
            expected_ticker="FIXTURE-MARKET",
            expected_block_trade=False,
        )


def test_kalshi_trade_prices_do_not_invent_an_undocumented_complement_rule() -> None:
    payload = {
        "cursor": "",
        "trades": [
            {
                "count_fp": "10.00",
                "created_time": _STAMP,
                "is_block_trade": False,
                "no_price_dollars": "0.5600",
                "taker_book_side": "bid",
                "taker_outcome_side": "yes",
                "ticker": "FIXTURE-MARKET",
                "trade_id": "OFFICIAL-SHAPE-1",
                "yes_price_dollars": "0.5600",
            }
        ],
    }
    envelope = _envelope(payload, feed_type="trades")
    assert envelope.source_timestamp_ns == _STAMP_NS
    assert prediction_raw_records(envelope)[0]["yes_price_dollars"] == "0.5600"
    assert prediction_raw_records(envelope)[0]["no_price_dollars"] == "0.5600"


def test_kalshi_raw_segment_manifest_and_replay_are_byte_identical(tmp_path: Path) -> None:
    raw_payload = {
        "market": {
            "event_ticker": "FIXTURE-EVENT",
            "ticker": "FIXTURE-MARKET",
            "updated_time": _STAMP,
        }
    }
    envelope = _envelope(raw_payload, feed_type="markets")
    manifests = []
    for name in ("first", "second"):
        root = tmp_path / name
        writer = ResearchSegmentWriter(
            root,
            collection_id="fixture-kalshi-runtime-collection",
            max_segment_bytes=1_000_000,
            rotation_seconds=60.0,
            max_total_bytes=10_000_000,
        )
        writer.append(envelope)
        manifest = writer.close()
        assert manifest is not None
        manifests.append(manifest)
        replay = ResearchSegmentReader(root, manifest_sha256=manifest.manifest_sha256).replay()
        assert replay == (envelope,)
        assert replay[0].canonical_bytes() == envelope.canonical_bytes()
        assert replay[0].raw_payload == canonical_json_bytes(raw_payload)
    assert manifests[0].manifest_sha256 == manifests[1].manifest_sha256
    assert manifests[0].root_sha256 == manifests[1].root_sha256
