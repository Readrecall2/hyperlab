from __future__ import annotations

import hashlib
import hmac
import json
import math
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import TypeAlias

JsonValue: TypeAlias = bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"] | None


def _normalize_json(value: object, *, path: str = "$") -> JsonValue:
    """Return a deterministic JSON-compatible copy, rejecting ambiguous values."""

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} must not contain NaN or infinity")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} contains a non-string mapping key")
            normalized[key] = _normalize_json(item, path=f"{path}.{key}")
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize_json(item, path=f"{path}[{index}]") for index, item in enumerate(value)]
    raise TypeError(f"{path} contains unsupported value type {type(value).__name__}")


def canonical_json(value: object) -> str:
    """Serialize JSON data identically across runs and mapping insertion orders."""

    return json.dumps(
        _normalize_json(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _utc_datetime(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must use UTC")
    return value.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _nonempty_identifier(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{label} cannot be empty")
    if any(character.isspace() for character in normalized):
        raise ValueError(f"{label} cannot contain whitespace")
    return normalized


def _sha256_identifier(value: str, *, label: str) -> str:
    normalized = _nonempty_identifier(value, label=label)
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return normalized


@dataclass(frozen=True, slots=True)
class TimeRange:
    """A non-empty UTC half-open interval ``[start, end)``."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        start = _utc_datetime(self.start, label="start")
        end = _utc_datetime(self.end, label="end")
        if start >= end:
            raise ValueError("a time range must be non-empty and satisfy start < end")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)

    @property
    def duration(self) -> timedelta:
        return self.end - self.start

    def contains(self, other: TimeRange) -> bool:
        return self.start <= other.start and other.end <= self.end

    def to_dict(self) -> dict[str, JsonValue]:
        return {"start": _utc_text(self.start), "end": _utc_text(self.end)}


@dataclass(frozen=True, slots=True)
class SelectionSplitView:
    """The only split description that parameter selection should receive.

    It intentionally has no final-test range. ``plan_hash`` binds the view to the
    complete immutable plan without disclosing that plan's held-out timestamps.
    """

    train: TimeRange
    validation: TimeRange
    dataset_hash: str
    plan_hash: str

    def __post_init__(self) -> None:
        if self.train.end > self.validation.start:
            raise ValueError("train and validation ranges must not overlap")
        object.__setattr__(
            self,
            "dataset_hash",
            _sha256_identifier(self.dataset_hash, label="dataset_hash"),
        )
        object.__setattr__(self, "plan_hash", _sha256_identifier(self.plan_hash, label="plan_hash"))

    @property
    def exposes_final_test(self) -> bool:
        """Machine-checkable declaration backed by the deliberately narrow schema."""

        return False

    def __call__(self) -> SelectionSplitView:
        """Allow both ``plan.selection_view`` and ``plan.selection_view()`` usage."""

        return self

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "dataset_hash": self.dataset_hash,
            "plan_hash": self.plan_hash,
            "train": self.train.to_dict(),
            "validation": self.validation.to_dict(),
        }

    def canonical_json(self) -> str:
        payload = self.to_dict()
        if "test" in payload or "final_test" in payload:
            raise AssertionError("selection view unexpectedly exposes the final test")
        return canonical_json(payload)

    def walk_forward(self, spec: WalkForwardSpec) -> tuple[WalkForwardWindow, ...]:
        available = TimeRange(self.train.start, self.validation.end)
        if not available.contains(spec.bounds):
            raise ValueError("walk-forward bounds must stay inside train and validation data")
        return spec.windows()


@dataclass(frozen=True, slots=True)
class SplitPlan:
    """Chronological train/validation/final-test ranges tied to one dataset snapshot."""

    train: TimeRange
    validation: TimeRange
    test: TimeRange
    dataset_hash: str

    def __post_init__(self) -> None:
        if self.train.end > self.validation.start:
            raise ValueError("train and validation ranges must not overlap")
        if self.validation.end > self.test.start:
            raise ValueError("validation and final-test ranges must not overlap")
        object.__setattr__(
            self,
            "dataset_hash",
            _sha256_identifier(self.dataset_hash, label="dataset_hash"),
        )

    @property
    def bounds(self) -> TimeRange:
        return TimeRange(self.train.start, self.test.end)

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "dataset_hash": self.dataset_hash,
            "schema_version": 1,
            "test": self.test.to_dict(),
            "train": self.train.to_dict(),
            "validation": self.validation.to_dict(),
        }

    def canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    def artifact(self) -> dict[str, JsonValue]:
        """Return the self-authenticating payload persisted before any trial.

        The registry stores this complete object in its first ``plan_created``
        event.  Keeping the hash next to the canonical UTC payload makes the
        pre-commitment independently reproducible without exposing the final
        interval to selection code.
        """

        return {"plan": self.to_dict(), "plan_hash": self.canonical_hash}

    @property
    def canonical_hash(self) -> str:
        return canonical_sha256(self.to_dict())

    @property
    def selection_view(self) -> SelectionSplitView:
        return SelectionSplitView(
            train=self.train,
            validation=self.validation,
            dataset_hash=self.dataset_hash,
            plan_hash=self.canonical_hash,
        )


class FinalTestState(StrEnum):
    LOCKED = "locked"
    VARIANT_FROZEN = "variant_frozen"
    REVEALED = "revealed"


class FinalTestLock:
    """One-shot state machine around a split plan's final-test range.

    This is a research-protocol guard, not a cryptographic access-control system.
    Selection code should receive only :attr:`selection_view`, never the source
    :class:`SplitPlan`.
    """

    __slots__ = (
        "_final_test",
        "_frozen_variant_hash",
        "_reveal_token",
        "_selection_view",
        "_state",
    )

    def __init__(self, plan: SplitPlan) -> None:
        self._selection_view = plan.selection_view
        self._final_test = plan.test
        self._state = FinalTestState.LOCKED
        self._frozen_variant_hash: str | None = None
        self._reveal_token: str | None = None

    @property
    def selection_view(self) -> SelectionSplitView:
        return self._selection_view

    @property
    def plan_hash(self) -> str:
        return self._selection_view.plan_hash

    @property
    def dataset_hash(self) -> str:
        return self._selection_view.dataset_hash

    @property
    def state(self) -> FinalTestState:
        return self._state

    @property
    def frozen_variant_hash(self) -> str | None:
        return self._frozen_variant_hash

    @property
    def was_revealed(self) -> bool:
        return self._state == FinalTestState.REVEALED

    def freeze_variant(self, variant_hash: str) -> str:
        if self._state != FinalTestState.LOCKED:
            raise RuntimeError("the final-test lock can freeze exactly one variant")
        normalized_hash = _nonempty_identifier(variant_hash, label="variant_hash")
        token = canonical_sha256(
            {
                "plan_hash": self._selection_view.plan_hash,
                "purpose": "one-shot-final-test-reveal",
                "variant_hash": normalized_hash,
            }
        )
        self._frozen_variant_hash = normalized_hash
        self._reveal_token = token
        self._state = FinalTestState.VARIANT_FROZEN
        return token

    def verify_reveal_token(self, token: str) -> None:
        """Validate a one-shot reveal without exposing the held-out range."""

        if self._state == FinalTestState.LOCKED:
            raise RuntimeError("freeze a selected variant before revealing the final test")
        if self._state == FinalTestState.REVEALED:
            raise RuntimeError("the final test has already been revealed")
        if self._reveal_token is None or not hmac.compare_digest(token, self._reveal_token):
            raise PermissionError("invalid final-test reveal token")

    def reveal_final_test(self, token: str) -> TimeRange:
        self.verify_reveal_token(token)
        self._state = FinalTestState.REVEALED
        return self._final_test


def _duration_microseconds(value: timedelta) -> int:
    return ((value.days * 86_400) + value.seconds) * 1_000_000 + value.microseconds


@dataclass(frozen=True, slots=True)
class WalkForwardWindow:
    ordinal: int
    train: TimeRange
    validation: TimeRange
    embargo: timedelta

    def __post_init__(self) -> None:
        if self.ordinal < 0:
            raise ValueError("walk-forward window ordinal must be non-negative")
        if self.embargo < timedelta(0):
            raise ValueError("walk-forward embargo must be non-negative")
        if self.train.end + self.embargo > self.validation.start:
            raise ValueError("walk-forward train and validation ranges violate the embargo")

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "embargo_microseconds": _duration_microseconds(self.embargo),
            "ordinal": self.ordinal,
            "train": self.train.to_dict(),
            "validation": self.validation.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class WalkForwardSpec:
    """Generate rolling or expanding chronological validation windows."""

    bounds: TimeRange
    train_window: timedelta
    validation_window: timedelta
    step: timedelta
    embargo: timedelta = timedelta(0)
    expanding: bool = False

    def __post_init__(self) -> None:
        for label, value in (
            ("train_window", self.train_window),
            ("validation_window", self.validation_window),
            ("step", self.step),
        ):
            if value <= timedelta(0):
                raise ValueError(f"{label} must be positive")
        if self.embargo < timedelta(0):
            raise ValueError("embargo must be non-negative")
        if self.step < self.validation_window:
            raise ValueError("step must not create overlapping out-of-sample windows")
        required = self.train_window + self.embargo + self.validation_window
        if required > self.bounds.duration:
            raise ValueError("walk-forward bounds do not contain one complete non-empty window")

    def iter_windows(self) -> Iterator[WalkForwardWindow]:
        first_train_end = self.bounds.start + self.train_window
        ordinal = 0
        while True:
            train_end = first_train_end + ordinal * self.step
            train_start = self.bounds.start if self.expanding else train_end - self.train_window
            validation_start = train_end + self.embargo
            validation_end = validation_start + self.validation_window
            if validation_end > self.bounds.end:
                break
            yield WalkForwardWindow(
                ordinal=ordinal,
                train=TimeRange(train_start, train_end),
                validation=TimeRange(validation_start, validation_end),
                embargo=self.embargo,
            )
            ordinal += 1

    def windows(self) -> tuple[WalkForwardWindow, ...]:
        windows = tuple(self.iter_windows())
        if not windows:
            raise ValueError("walk-forward specification produced no windows")
        return windows

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "bounds": self.bounds.to_dict(),
            "embargo_microseconds": _duration_microseconds(self.embargo),
            "expanding": self.expanding,
            "step_microseconds": _duration_microseconds(self.step),
            "train_window_microseconds": _duration_microseconds(self.train_window),
            "validation_window_microseconds": _duration_microseconds(self.validation_window),
        }

    @property
    def canonical_hash(self) -> str:
        return canonical_sha256(self.to_dict())
