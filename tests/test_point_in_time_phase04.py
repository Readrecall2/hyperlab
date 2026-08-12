from __future__ import annotations

import pandas as pd
import pytest

from hyperlab.backtest.point_in_time import (
    CandleFinalityPolicy,
    PointInTimeError,
    join_venues_as_of,
    select_candle_revisions_as_of,
    universe_mask_as_of,
)


def _candle_row(
    *,
    received_time: str,
    close: float,
    is_final: bool | None,
    close_time: str = "2026-01-01T01:00:00Z",
) -> dict[str, object]:
    return {
        "venue": "hyperliquid",
        "asset": "BTC",
        "interval": "1h",
        "open_time": pd.Timestamp("2026-01-01T00:00:00Z"),
        "close_time": pd.Timestamp(close_time),
        "received_time": pd.Timestamp(received_time),
        "is_final": is_final,
        "close": close,
    }


def test_future_candle_revision_cannot_change_an_earlier_as_of_view() -> None:
    policy = CandleFinalityPolicy(unknown_finality_delay=pd.Timedelta("1min"))
    first = pd.DataFrame(
        [
            _candle_row(
                received_time="2026-01-01T01:01:00Z",
                close=100.0,
                is_final=None,
            )
        ]
    )
    decision = pd.Timestamp("2026-01-01T01:05:00Z")
    baseline = select_candle_revisions_as_of(first, decision, finality_policy=policy)

    with_future_revision = pd.concat(
        [
            first,
            pd.DataFrame(
                [
                    _candle_row(
                        received_time="2026-01-01T01:06:00Z",
                        close=9_999.0,
                        is_final=None,
                    )
                ]
            ),
        ],
        ignore_index=True,
    )
    unchanged = select_candle_revisions_as_of(
        with_future_revision,
        decision,
        finality_policy=policy,
    )
    later = select_candle_revisions_as_of(
        with_future_revision,
        pd.Timestamp("2026-01-01T01:07:00Z"),
        finality_policy=policy,
    )

    assert baseline["close"].tolist() == [100.0]
    assert unchanged["close"].tolist() == baseline["close"].tolist()
    assert later["close"].tolist() == [9_999.0]


def test_candle_must_be_closed_and_received_before_the_decision() -> None:
    policy = CandleFinalityPolicy(unknown_finality_delay=pd.Timedelta(0))
    decision = pd.Timestamp("2026-01-01T01:00:30Z")
    rows = pd.DataFrame(
        [
            _candle_row(
                received_time="2026-01-01T01:00:31Z",
                close=101.0,
                is_final=True,
            )
        ]
    )
    assert select_candle_revisions_as_of(rows, decision, finality_policy=policy).empty

    received_but_not_closed = rows.copy()
    received_but_not_closed["received_time"] = pd.Timestamp("2026-01-01T01:00:20Z")
    received_but_not_closed["close_time"] = pd.Timestamp("2026-01-01T01:01:00Z")
    assert select_candle_revisions_as_of(
        received_but_not_closed,
        decision,
        finality_policy=policy,
    ).empty


def test_false_finality_is_never_selected_and_unknown_requires_explicit_delay() -> None:
    decision = pd.Timestamp("2026-01-01T01:05:00Z")
    false_observation = pd.DataFrame(
        [
            _candle_row(
                received_time="2026-01-01T01:01:00Z",
                close=100.0,
                is_final=False,
            )
        ]
    )
    permissive = CandleFinalityPolicy(unknown_finality_delay=pd.Timedelta(0))
    assert select_candle_revisions_as_of(
        false_observation,
        decision,
        finality_policy=permissive,
    ).empty

    unknown = false_observation.copy()
    unknown["is_final"] = None
    reject_unknown = CandleFinalityPolicy(unknown_finality_delay=None)
    delayed = CandleFinalityPolicy(unknown_finality_delay=pd.Timedelta("10min"))
    assert select_candle_revisions_as_of(
        unknown,
        decision,
        finality_policy=reject_unknown,
    ).empty
    assert select_candle_revisions_as_of(
        unknown,
        decision,
        finality_policy=delayed,
    ).empty
    assert select_candle_revisions_as_of(
        unknown,
        pd.Timestamp("2026-01-01T01:10:00Z"),
        finality_policy=delayed,
    )["close"].tolist() == [100.0]


def test_latest_eligible_revision_keeps_an_earlier_final_candle() -> None:
    candles = pd.DataFrame(
        [
            _candle_row(
                received_time="2026-01-01T01:01:00Z",
                close=100.0,
                is_final=True,
            ),
            _candle_row(
                received_time="2026-01-01T01:02:00Z",
                close=9_999.0,
                is_final=False,
            ),
        ]
    )

    result = select_candle_revisions_as_of(
        candles,
        pd.Timestamp("2026-01-01T01:05:00Z"),
        finality_policy=CandleFinalityPolicy(unknown_finality_delay=None),
    )

    assert result["close"].tolist() == [100.0]
    assert result["received_time"].tolist() == [pd.Timestamp("2026-01-01T01:01:00Z")]


def test_join_venues_never_uses_a_future_received_observation() -> None:
    decisions = pd.DataFrame({"decision_time": [pd.Timestamp("2026-01-01T00:00:30Z")]})
    venue_a = pd.DataFrame(
        {
            "event_time": [pd.Timestamp("2026-01-01T00:00:05Z")],
            "received_time": [pd.Timestamp("2026-01-01T00:00:10Z")],
            "price": [100.0],
        }
    )
    venue_b = pd.DataFrame(
        {
            "event_time": [pd.Timestamp("2026-01-01T00:00:35Z")],
            "received_time": [pd.Timestamp("2026-01-01T00:00:40Z")],
            "price": [200.0],
        }
    )
    baseline = join_venues_as_of(
        {"HL": venue_a, "REF": venue_b},
        decisions,
        max_staleness=pd.Timedelta("1min"),
        value_columns=("price",),
    )

    venue_a_with_future = pd.concat(
        [
            venue_a,
            pd.DataFrame(
                {
                    "event_time": [pd.Timestamp("2026-01-01T00:00:45Z")],
                    "received_time": [pd.Timestamp("2026-01-01T00:00:50Z")],
                    "price": [9_999.0],
                }
            ),
        ],
        ignore_index=True,
    )
    unchanged = join_venues_as_of(
        {"HL": venue_a_with_future, "REF": venue_b},
        decisions,
        max_staleness=pd.Timedelta("1min"),
        value_columns=("price",),
    )

    assert baseline.at[0, "HL__available"]
    assert baseline.at[0, "HL__price"] == pytest.approx(100.0)
    assert not baseline.at[0, "REF__available"]
    assert pd.isna(baseline.at[0, "REF__price"])
    assert unchanged.at[0, "HL__price"] == baseline.at[0, "HL__price"]


def test_join_marks_stale_values_unavailable_and_nulls_the_payload() -> None:
    observations = pd.DataFrame(
        {
            "event_time": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "received_time": [pd.Timestamp("2026-01-01T00:00:10Z")],
            "price": [100.0],
        }
    )
    result = join_venues_as_of(
        {"HL": observations},
        pd.DatetimeIndex(["2026-01-01T00:01:00Z"]),
        max_staleness=pd.Timedelta("30s"),
        value_columns=("price",),
    )

    assert result.at[0, "HL__stale"]
    assert not result.at[0, "HL__available"]
    assert result.at[0, "HL__age"] == pd.Timedelta("60s")
    assert result.at[0, "HL__event_age"] == pd.Timedelta("60s")
    assert result.at[0, "HL__receive_age"] == pd.Timedelta("50s")
    assert pd.isna(result.at[0, "HL__price"])
    assert result.at[0, "HL__received_time"] == pd.Timestamp("2026-01-01T00:00:10Z")


def test_join_requires_event_time_and_uses_it_for_staleness() -> None:
    decisions = pd.DatetimeIndex(["2026-01-01T00:01:00Z"])
    late_receipt = pd.DataFrame(
        {
            "event_time": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "received_time": [pd.Timestamp("2026-01-01T00:00:55Z")],
            "price": [100.0],
        }
    )
    result = join_venues_as_of(
        {"HL": late_receipt},
        decisions,
        max_staleness=pd.Timedelta("30s"),
        value_columns=("price",),
    )

    assert result.at[0, "HL__receive_age"] == pd.Timedelta("5s")
    assert result.at[0, "HL__event_age"] == pd.Timedelta("60s")
    assert result.at[0, "HL__stale"]
    assert pd.isna(result.at[0, "HL__price"])

    with pytest.raises(PointInTimeError, match=r"missing columns.*event_time"):
        join_venues_as_of(
            {"HL": late_receipt.drop(columns="event_time")},
            decisions,
            max_staleness=pd.Timedelta("30s"),
            value_columns=("price",),
        )


def test_join_rejects_a_source_timestamp_after_receipt_or_decision() -> None:
    impossible = pd.DataFrame(
        {
            "event_time": [pd.Timestamp("2026-01-01T00:00:40Z")],
            "received_time": [pd.Timestamp("2026-01-01T00:00:10Z")],
            "price": [100.0],
        }
    )

    with pytest.raises(PointInTimeError, match="event timestamp later than its receive timestamp"):
        join_venues_as_of(
            {"HL": impossible},
            pd.DatetimeIndex(["2026-01-01T00:00:30Z"]),
            max_staleness=pd.Timedelta("1min"),
            value_columns=("price",),
        )

    with pytest.raises(PointInTimeError, match="event timestamp later than its receive timestamp"):
        join_venues_as_of(
            {"HL": impossible},
            pd.DatetimeIndex(["2026-01-01T00:00:50Z"]),
            max_staleness=pd.Timedelta("1min"),
            value_columns=("price",),
        )


def test_join_aligns_each_asset_within_each_venue_stream() -> None:
    decisions = pd.DataFrame(
        {
            "decision_time": [
                pd.Timestamp("2026-01-01T00:00:30Z"),
                pd.Timestamp("2026-01-01T00:00:30Z"),
            ],
            "asset": ["BTC", "ETH"],
        }
    )
    observations = pd.DataFrame(
        {
            "asset": ["ETH", "BTC"],
            "event_time": [
                pd.Timestamp("2026-01-01T00:00:04Z"),
                pd.Timestamp("2026-01-01T00:00:05Z"),
            ],
            "received_time": [
                pd.Timestamp("2026-01-01T00:00:09Z"),
                pd.Timestamp("2026-01-01T00:00:10Z"),
            ],
            "price": [200.0, 100.0],
        }
    )

    result = join_venues_as_of(
        {"HL": observations},
        decisions,
        by=("asset",),
        max_staleness=pd.Timedelta("1min"),
        value_columns=("price",),
    )

    assert result.loc[result["asset"].eq("BTC"), "HL__price"].item() == pytest.approx(100.0)
    assert result.loc[result["asset"].eq("ETH"), "HL__price"].item() == pytest.approx(200.0)


def _lifecycle_row(
    asset: str,
    status: str,
    valid_from: str,
    received_time: str,
) -> dict[str, object]:
    return {
        "venue": "hyperliquid",
        "asset": asset,
        "status": status,
        "valid_from": pd.Timestamp(valid_from),
        "valid_to": pd.NaT,
        "received_time": pd.Timestamp(received_time),
    }


def test_universe_mask_is_as_of_and_keeps_delisted_assets_as_columns() -> None:
    lifecycle = pd.DataFrame(
        [
            _lifecycle_row(
                "OLD",
                "listed",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
            ),
            _lifecycle_row(
                "OLD",
                "renamed",
                "2026-01-02T00:00:00Z",
                "2026-01-02T00:00:00Z",
            ),
            _lifecycle_row(
                "OLD",
                "delisted",
                "2026-01-03T00:00:00Z",
                "2026-01-03T00:10:00Z",
            ),
            _lifecycle_row(
                "NEW",
                "listed",
                "2026-01-02T00:00:00Z",
                "2026-01-02T00:00:00Z",
            ),
        ]
    )
    decisions = pd.DatetimeIndex(
        [
            "2026-01-01T12:00:00Z",
            "2026-01-02T12:00:00Z",
            "2026-01-03T00:05:00Z",
            "2026-01-03T00:20:00Z",
        ]
    )

    mask = universe_mask_as_of(
        lifecycle,
        decisions,
        identity_columns=("venue", "asset"),
    )

    assert mask[("hyperliquid", "OLD")].tolist() == [True, True, True, False]
    assert mask[("hyperliquid", "NEW")].tolist() == [False, True, True, True]
    assert ("hyperliquid", "OLD") in mask.columns


def test_future_lifecycle_event_cannot_change_an_earlier_universe_mask() -> None:
    lifecycle = pd.DataFrame(
        [
            _lifecycle_row(
                "BTC",
                "listed",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
            )
        ]
    )
    decisions = pd.DatetimeIndex(["2026-01-01T12:00:00Z", "2026-01-02T12:00:00Z"])
    baseline = universe_mask_as_of(
        lifecycle,
        decisions,
        identity_columns=("venue", "asset"),
    )
    with_future = pd.concat(
        [
            lifecycle,
            pd.DataFrame(
                [
                    _lifecycle_row(
                        "BTC",
                        "delisted",
                        "2026-01-03T00:00:00Z",
                        "2026-01-03T00:00:01Z",
                    ),
                    _lifecycle_row(
                        "FUTURE",
                        "listed",
                        "2026-01-04T00:00:00Z",
                        "2026-01-04T00:00:01Z",
                    ),
                ]
            ),
        ],
        ignore_index=True,
    )
    unchanged = universe_mask_as_of(
        with_future,
        decisions,
        identity_columns=("venue", "asset"),
    )

    pd.testing.assert_frame_equal(unchanged, baseline)
