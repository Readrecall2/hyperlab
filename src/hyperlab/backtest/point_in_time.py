from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast

import numpy as np
import pandas as pd


class PointInTimeError(ValueError):
    """Raised when an as-of view cannot be built without an ambiguous chronology."""


@dataclass(frozen=True, slots=True)
class CandleFinalityPolicy:
    """Explicit policy for candle observations with unknown source finality.

    ``None`` rejects observations whose ``is_final`` value is unknown. A non-negative
    delay permits them only once ``decision_time >= close_time + delay``. Observations
    explicitly marked ``is_final=False`` are never eligible.
    """

    unknown_finality_delay: pd.Timedelta | None

    def __post_init__(self) -> None:
        delay = self.unknown_finality_delay
        if delay is None:
            return
        normalized = pd.Timedelta(delay)
        if pd.isna(normalized) or normalized < pd.Timedelta(0):
            raise ValueError("unknown_finality_delay must be finite and non-negative")
        object.__setattr__(self, "unknown_finality_delay", normalized)


def _require_columns(frame: pd.DataFrame, columns: Sequence[str], *, label: str) -> None:
    if not frame.columns.is_unique:
        raise PointInTimeError(f"{label} columns must be unique")
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise PointInTimeError(f"{label} is missing columns: {missing}")


def _utc_timestamp(value: object, *, label: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(cast(Any, value))
    except (TypeError, ValueError) as exc:
        raise PointInTimeError(f"{label} must be a valid timestamp") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise PointInTimeError(f"{label} must be timezone-aware UTC")
    if timestamp.utcoffset() != timedelta(0):
        raise PointInTimeError(f"{label} must use UTC")
    return timestamp.tz_convert("UTC")


def _utc_column(
    frame: pd.DataFrame,
    column: str,
    *,
    label: str,
    allow_missing: bool = False,
) -> pd.Series:
    source = frame[column]
    if source.isna().all() and allow_missing:
        return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]", name=column)
    try:
        converted = pd.to_datetime(source, errors="raise")
    except (TypeError, ValueError) as exc:
        raise PointInTimeError(f"{label}.{column} must contain valid timestamps") from exc
    if not isinstance(converted, pd.Series):
        raise PointInTimeError(f"{label}.{column} could not be interpreted as timestamps")
    if converted.isna().any() and not allow_missing:
        raise PointInTimeError(f"{label}.{column} cannot contain missing timestamps")
    timezone = converted.dt.tz
    if timezone is None:
        raise PointInTimeError(f"{label}.{column} must be timezone-aware UTC")
    if str(timezone).upper() not in {"UTC", "UTC+00:00"}:
        raise PointInTimeError(f"{label}.{column} must use UTC")
    return converted.dt.tz_convert("UTC")


def _finality(value: object) -> bool | None:
    if bool(pd.isna(cast(Any, value))):
        return None
    if not isinstance(value, (bool, np.bool_)):
        raise PointInTimeError("is_final values must be true, false, or null")
    return bool(value)


def _non_negative_timedelta(value: pd.Timedelta, *, label: str) -> pd.Timedelta:
    normalized = pd.Timedelta(value)
    if pd.isna(normalized) or normalized < pd.Timedelta(0):
        raise ValueError(f"{label} must be finite and non-negative")
    return normalized


def _decision_frame(
    decisions: pd.DataFrame | pd.DatetimeIndex | Sequence[object],
    *,
    decision_time_column: str,
    by: Sequence[str],
) -> pd.DataFrame:
    if isinstance(decisions, pd.DataFrame):
        result = decisions.copy()
    else:
        if by:
            raise PointInTimeError("grouped as-of joins require a decision DataFrame")
        result = pd.DataFrame({decision_time_column: list(decisions)})
    _require_columns(result, [decision_time_column, *by], label="decisions")
    result[decision_time_column] = _utc_column(
        result,
        decision_time_column,
        label="decisions",
    )
    duplicate_key = [*by, decision_time_column]
    if result.duplicated(duplicate_key).any():
        raise PointInTimeError("decision keys must be unique")
    return result


def select_candle_revisions_as_of(
    candles: pd.DataFrame,
    decision_time: object,
    *,
    finality_policy: CandleFinalityPolicy,
    candle_key: Sequence[str] = ("venue", "asset", "interval", "open_time"),
    received_time_column: str = "received_time",
    close_time_column: str = "close_time",
    finality_column: str = "is_final",
) -> pd.DataFrame:
    """Select the latest eligible revision of every logical candle.

    Filtering by availability and candle close happens *before* revision selection, so
    appending a future correction cannot alter an earlier point-in-time view. If two
    revisions of one candle share the same receive timestamp, their order is unknowable
    and the function fails instead of choosing from input row order.
    """

    key = tuple(candle_key)
    if not key:
        raise ValueError("candle_key cannot be empty")
    required = [*key, received_time_column, close_time_column, finality_column]
    _require_columns(candles, required, label="candles")
    if candles[list(key)].isna().any(axis=None):
        raise PointInTimeError("candle identity columns cannot be null")

    decision = _utc_timestamp(decision_time, label="decision_time")
    data = candles.copy()
    data[received_time_column] = _utc_column(
        data,
        received_time_column,
        label="candles",
    )
    data[close_time_column] = _utc_column(
        data,
        close_time_column,
        label="candles",
    )
    data[finality_column].map(_finality)

    availability = data[received_time_column].le(decision) & data[close_time_column].le(decision)
    delay = finality_policy.unknown_finality_delay
    finality_states = data[finality_column].map(_finality)
    eligible_finality = finality_states.eq(True)
    if delay is not None:
        eligible_finality |= finality_states.isna() & data[close_time_column].add(delay).le(decision)

    # Finality is part of eligibility, not a property checked after choosing a
    # revision. Otherwise a later provisional correction would hide an earlier
    # final revision and make the point-in-time view spuriously empty.
    eligible = data.loc[availability & eligible_finality].copy()
    if eligible.empty:
        result = eligible.reset_index(drop=True)
        result.attrs["decision_time"] = decision.isoformat()
        result.attrs["unknown_finality_delay"] = None if delay is None else str(delay)
        return result
    ambiguity_key = [*key, received_time_column]
    if eligible.duplicated(ambiguity_key, keep=False).any():
        raise PointInTimeError("candle revisions have an ambiguous receive-time tie")

    latest = (
        eligible.sort_values([*key, received_time_column], kind="stable")
        .groupby(list(key), sort=False, dropna=False)
        .tail(1)
    )
    result = latest.sort_values(list(key), kind="stable").reset_index(drop=True)
    result.attrs["decision_time"] = decision.isoformat()
    result.attrs["unknown_finality_delay"] = None if delay is None else str(delay)
    return result


def join_venues_as_of(
    observations_by_venue: Mapping[str, pd.DataFrame],
    decisions: pd.DataFrame | pd.DatetimeIndex | Sequence[object],
    *,
    max_staleness: pd.Timedelta,
    by: Sequence[str] = (),
    decision_time_column: str = "decision_time",
    received_time_column: str = "received_time",
    event_time_column: str = "event_time",
    value_columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Join venue observations to decisions using receive-time-only backward joins.

    One input frame represents a venue and may contain several streams distinguished by
    ``by`` columns. Receipt time controls causal availability; event time controls
    freshness. The result keeps both timestamps as evidence, nulls stale payload
    values, and exposes ``<venue>__available``, ``__stale``, ``__event_age`` and
    ``__receive_age`` (``__age`` remains an alias for event age). A selected source
    event timestamp later than its decision is rejected explicitly.
    """

    if not observations_by_venue:
        raise ValueError("at least one venue is required")
    staleness = _non_negative_timedelta(max_staleness, label="max_staleness")
    groups = tuple(by)
    left = _decision_frame(
        decisions,
        decision_time_column=decision_time_column,
        by=groups,
    )
    left["_pit_decision_order"] = np.arange(len(left), dtype=np.int64)
    left = left.sort_values([decision_time_column, *groups], kind="stable")

    for venue, source in observations_by_venue.items():
        if not venue or "__" in venue:
            raise ValueError("venue keys must be non-empty and cannot contain '__'")
        required = [*groups, received_time_column, event_time_column]
        _require_columns(source, required, label=f"venue {venue}")
        if groups and source[list(groups)].isna().any(axis=None):
            raise PointInTimeError(f"venue {venue} grouping columns cannot be null")

        frame = source.copy()
        frame[received_time_column] = _utc_column(
            frame,
            received_time_column,
            label=f"venue {venue}",
        )
        frame[event_time_column] = _utc_column(
            frame,
            event_time_column,
            label=f"venue {venue}",
        )
        if frame[event_time_column].gt(frame[received_time_column]).any():
            raise PointInTimeError(f"venue {venue} has an event timestamp later than its receive timestamp")
        if frame.duplicated([*groups, received_time_column], keep=False).any():
            raise PointInTimeError(f"venue {venue} has ambiguous observations at the same receive time")

        if value_columns is None:
            payload = [
                str(column)
                for column in frame.columns
                if column not in {*groups, received_time_column, event_time_column}
            ]
        else:
            payload = list(value_columns)
            _require_columns(frame, payload, label=f"venue {venue}")
            payload = [
                column
                for column in payload
                if column not in {*groups, received_time_column, event_time_column}
            ]

        selected_columns = [*groups, received_time_column, event_time_column]
        selected_columns.extend(payload)
        prefixed_received = f"{venue}__received_time"
        rename = {received_time_column: prefixed_received}
        prefixed_event = f"{venue}__event_time"
        rename[event_time_column] = prefixed_event
        prefixed_payload = [f"{venue}__{column}" for column in payload]
        rename.update(dict(zip(payload, prefixed_payload, strict=True)))
        right = frame[selected_columns].rename(columns=rename)
        right = right.sort_values([prefixed_received, *groups], kind="stable")

        try:
            left = pd.merge_asof(
                left,
                right,
                left_on=decision_time_column,
                right_on=prefixed_received,
                by=list(groups) or None,
                direction="backward",
                allow_exact_matches=True,
            )
        except (TypeError, ValueError) as exc:
            raise PointInTimeError(f"venue {venue} cannot be joined causally: {exc}") from exc

        matched = left[prefixed_received].notna()
        receive_age = left[decision_time_column] - left[prefixed_received]
        if (matched & receive_age.lt(pd.Timedelta(0))).any():
            raise PointInTimeError(f"venue {venue} selected an observation from the future")
        event_age = left[decision_time_column] - left[prefixed_event]
        future_event = matched & event_age.lt(pd.Timedelta(0))
        if future_event.any():
            raise PointInTimeError(f"venue {venue} selected an event timestamp later than the decision")
        stale = matched & event_age.gt(staleness)
        if prefixed_payload:
            left.loc[stale, prefixed_payload] = pd.NA
        left[f"{venue}__receive_age"] = receive_age
        left[f"{venue}__event_age"] = event_age
        left[f"{venue}__age"] = event_age
        left[f"{venue}__stale"] = stale.astype(bool)
        left[f"{venue}__available"] = (matched & ~stale).astype(bool)

    return (
        left.sort_values("_pit_decision_order", kind="stable")
        .drop(columns="_pit_decision_order")
        .reset_index(drop=True)
    )


def universe_mask_as_of(
    lifecycle: pd.DataFrame,
    decision_times: pd.DatetimeIndex | Sequence[object],
    *,
    identity_columns: Sequence[str] | None = None,
    status_column: str = "status",
    valid_from_column: str = "valid_from",
    valid_to_column: str = "valid_to",
    received_time_column: str = "received_time",
) -> pd.DataFrame:
    """Reconstruct a point-in-time tradable-universe mask from lifecycle records.

    Columns are the union of all historical identities, including instruments that are
    delisted by the final decision. Prefer the stable ``(venue, instrument_id)`` identity
    when available; callers may explicitly request ``(venue, asset)`` for asset masks.
    ``listed`` and ``renamed`` are active states, while ``delisted`` is inactive.
    """

    if lifecycle.empty:
        raise PointInTimeError("lifecycle data cannot be empty")
    identity: tuple[str, ...]
    if identity_columns is None:
        identity = (
            ("venue", "instrument_id")
            if {"venue", "instrument_id"}.issubset(lifecycle.columns)
            else ("venue", "asset")
        )
    else:
        identity = tuple(identity_columns)
    if not identity:
        raise ValueError("identity_columns cannot be empty")
    required = [
        *identity,
        status_column,
        valid_from_column,
        valid_to_column,
        received_time_column,
    ]
    _require_columns(lifecycle, required, label="lifecycle")
    if lifecycle[list(identity)].isna().any(axis=None):
        raise PointInTimeError("lifecycle identity columns cannot be null")

    data = lifecycle.copy()
    data[valid_from_column] = _utc_column(
        data,
        valid_from_column,
        label="lifecycle",
    )
    data[valid_to_column] = _utc_column(
        data,
        valid_to_column,
        label="lifecycle",
        allow_missing=True,
    )
    data[received_time_column] = _utc_column(
        data,
        received_time_column,
        label="lifecycle",
    )
    statuses = data[status_column].map(lambda value: str(value).lower())
    allowed = {"listed", "renamed", "delisted"}
    invalid = sorted(set(statuses).difference(allowed))
    if invalid:
        raise PointInTimeError(f"unsupported lifecycle statuses: {invalid}")
    data[status_column] = statuses
    invalid_interval = data[valid_to_column].notna() & data[valid_to_column].lt(data[valid_from_column])
    if invalid_interval.any():
        raise PointInTimeError("lifecycle valid_to cannot precede valid_from")
    ambiguity_key = [*identity, valid_from_column, received_time_column]
    if data.duplicated(ambiguity_key, keep=False).any():
        raise PointInTimeError("lifecycle contains ambiguous simultaneous state changes")

    decisions = pd.DatetimeIndex([_utc_timestamp(value, label="decision_time") for value in decision_times])
    if decisions.empty:
        raise PointInTimeError("decision_times cannot be empty")
    if not decisions.is_monotonic_increasing or decisions.has_duplicates:
        raise PointInTimeError("decision_times must be strictly increasing")

    horizon = decisions[-1]
    known_by_horizon = data.loc[data[received_time_column].le(horizon) & data[valid_from_column].le(horizon)]
    identity_values = sorted(
        {tuple(row) for row in known_by_horizon[list(identity)].itertuples(index=False, name=None)},
        key=lambda values: tuple(str(value) for value in values),
    )
    columns = pd.MultiIndex.from_tuples(identity_values, names=list(identity))
    mask = pd.DataFrame(False, index=decisions, columns=columns, dtype=bool)
    mask.index.name = "decision_time"

    for values in identity_values:
        stream = data.copy()
        for column, value in zip(identity, values, strict=True):
            stream = stream.loc[stream[column].eq(value)]
        for decision in decisions:
            known = stream.loc[
                stream[received_time_column].le(decision) & stream[valid_from_column].le(decision)
            ]
            if known.empty:
                continue
            latest = known.sort_values(
                [valid_from_column, received_time_column],
                kind="stable",
            ).iloc[-1]
            valid_to = latest[valid_to_column]
            within_interval = pd.isna(valid_to) or decision < valid_to
            mask.at[decision, values] = bool(
                within_interval and latest[status_column] in {"listed", "renamed"}
            )

    return mask


__all__ = [
    "CandleFinalityPolicy",
    "PointInTimeError",
    "join_venues_as_of",
    "select_candle_revisions_as_of",
    "universe_mask_as_of",
]
