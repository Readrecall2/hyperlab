from __future__ import annotations

import json
import math
import os
import re
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import cast

from hyperlab.backtest.protocol import (
    FinalTestLock,
    FinalTestState,
    JsonValue,
    SelectionSplitView,
    SplitPlan,
    TimeRange,
    canonical_json,
    canonical_sha256,
)

_PERFORMANCE_TERMS = frozenset(
    {"alpha", "cagr", "gain", "gains", "performance", "profit", "pnl", "return", "returns", "roi", "yield"}
)
_TARGET_TERMS = frozenset({"aim", "desired", "goal", "objective", "target"})
_TARGET_MARKERS = ("closest_to", "closeness_to", "distance_from", "distance_to")
_TEMPORAL_TERMS = frozenset(
    {"annual", "annualized", "daily", "month", "monthly", "week", "weekly", "year", "yearly"}
)
_ALLOWED_OBJECTIVE_METRICS = frozenset(
    {
        "annualized_return",
        "calmar",
        "downside_deviation",
        "information_ratio",
        "max_drawdown",
        "net_return",
        "profit_factor",
        "sharpe",
        "sortino",
        "total_return",
        "turnover",
        "volatility",
    }
)
_SELECTION_SPLITS = frozenset({"validation", "walk_forward"})
_RESULT_SPLITS = frozenset({"train", "validation", "walk_forward", "stress"})
_ALL_RESULT_SPLITS = _RESULT_SPLITS | {"final_test"}
_RESULT_STATUSES = frozenset({"success", "failure", "interrupted"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _normalized_key(key: str) -> str:
    camel_split = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key.strip())
    return re.sub(r"[^a-zA-Z0-9]+", "_", camel_split).strip("_").casefold()


def _looks_like_return_target(key: str) -> bool:
    normalized = _normalized_key(key)
    tokens = frozenset(part for part in normalized.split("_") if part)
    performance = bool(tokens.intersection(_PERFORMANCE_TERMS)) or bool(
        tokens.intersection(_TARGET_TERMS) and tokens.intersection(_TEMPORAL_TERMS)
    )
    targeted = bool(tokens.intersection(_TARGET_TERMS)) or any(
        marker in normalized for marker in _TARGET_MARKERS
    )
    return performance and targeted


def _reject_return_targets(value: object, *, path: str = "parameters") -> None:
    """Reject target-return aliases, including camelCase and split nested forms."""

    if isinstance(value, Mapping):
        normalized_keys = [_normalized_key(key) for key in value if isinstance(key, str)]
        normalized_string_values = [_normalized_key(item) for item in value.values() if isinstance(item, str)]
        sibling_tokens = frozenset(
            token
            for key in [*normalized_keys, *normalized_string_values]
            for token in key.split("_")
            if token
        )
        if sibling_tokens.intersection(_PERFORMANCE_TERMS) and sibling_tokens.intersection(_TARGET_TERMS):
            raise ValueError(f"forbidden return target parameter at {path}")
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} contains a non-string key")
            if _looks_like_return_target(key):
                raise ValueError(f"forbidden return target parameter at {path}.{key}")
            if isinstance(item, str) and _looks_like_return_target(item):
                raise ValueError(f"forbidden return target value at {path}.{key}")
            _reject_return_targets(item, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            _reject_return_targets(item, path=f"{path}[{index}]")


def _freeze(value: JsonValue) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _json_copy(value: object) -> JsonValue:
    decoded: object = json.loads(canonical_json(value))
    return cast(JsonValue, decoded)


def _identifier(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{label} cannot be empty")
    if any(character.isspace() for character in normalized):
        raise ValueError(f"{label} cannot contain whitespace")
    return normalized


def _sha256(value: str, *, label: str) -> str:
    normalized = _identifier(value, label=label)
    if not _SHA256_RE.fullmatch(normalized):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return normalized


class ObjectiveDirection(StrEnum):
    MAXIMIZE = "maximize"
    MINIMIZE = "minimize"


@dataclass(frozen=True, slots=True)
class SelectionObjective:
    """An allowlisted ranking metric and direction, deliberately without a target."""

    metric: str
    direction: ObjectiveDirection | str = ObjectiveDirection.MAXIMIZE

    def __post_init__(self) -> None:
        metric = _normalized_key(_identifier(self.metric, label="objective metric"))
        if _looks_like_return_target(metric):
            raise ValueError("a return target cannot be used as a selection objective")
        if metric not in _ALLOWED_OBJECTIVE_METRICS:
            allowed = ", ".join(sorted(_ALLOWED_OBJECTIVE_METRICS))
            raise ValueError(f"objective metric must be allowlisted; choose one of: {allowed}")
        try:
            direction = ObjectiveDirection(self.direction)
        except ValueError as error:
            raise ValueError("objective direction must be 'maximize' or 'minimize'") from error
        object.__setattr__(self, "metric", metric)
        object.__setattr__(self, "direction", direction)

    def to_dict(self) -> dict[str, JsonValue]:
        return {"direction": str(self.direction), "metric": self.metric}


@dataclass(frozen=True, slots=True)
class VariantSpec:
    """Complete, immutable identity of one attempted research variant."""

    strategy_name: str
    parameters: Mapping[str, object]
    split_hash: str
    data_hash: str
    code_hash: str
    objective: SelectionObjective
    seed: int = 0
    scenario: str = "realistic"

    def __post_init__(self) -> None:
        if not isinstance(self.parameters, Mapping):
            raise TypeError("parameters must be a mapping containing the complete parameter set")
        _reject_return_targets(self.parameters)
        parameter_copy = _json_copy(self.parameters)
        if not isinstance(parameter_copy, dict):
            raise TypeError("parameters must serialize to a JSON object")
        if isinstance(self.seed, bool) or not isinstance(self.seed, int):
            raise TypeError("seed must be an integer")
        if not isinstance(self.objective, SelectionObjective):
            raise TypeError("objective must be a SelectionObjective")
        object.__setattr__(
            self,
            "strategy_name",
            _identifier(self.strategy_name, label="strategy_name"),
        )
        object.__setattr__(self, "parameters", _freeze(parameter_copy))
        object.__setattr__(self, "split_hash", _sha256(self.split_hash, label="split_hash"))
        object.__setattr__(self, "data_hash", _sha256(self.data_hash, label="data_hash"))
        object.__setattr__(self, "code_hash", _sha256(self.code_hash, label="code_hash"))
        object.__setattr__(self, "scenario", _identifier(self.scenario, label="scenario"))

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "code_hash": self.code_hash,
            "data_hash": self.data_hash,
            "objective": self.objective.to_dict(),
            "parameters": _json_copy(self.parameters),
            "scenario": self.scenario,
            "schema_version": 1,
            "seed": self.seed,
            "split_hash": self.split_hash,
            "strategy_name": self.strategy_name,
        }

    def canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def canonical_hash(self) -> str:
        return canonical_sha256(self.to_dict())

    @property
    def variant_hash(self) -> str:
        return self.canonical_hash


def _validated_metrics(metrics: Mapping[str, object], *, allow_non_finite: bool) -> dict[str, JsonValue]:
    if not isinstance(metrics, Mapping):
        raise TypeError("metrics must be a mapping")
    result: dict[str, JsonValue] = {}
    for name, raw_value in metrics.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError("metric names must be non-empty strings")
        if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
            raise TypeError(f"metric {name!r} must be numeric")
        value = float(raw_value)
        if math.isfinite(value):
            result[name] = value
        elif not allow_non_finite:
            raise ValueError(f"metric {name!r} must be finite for selection")
        else:
            result[name] = {
                "non_finite": "nan" if math.isnan(value) else ("infinity" if value > 0 else "-infinity")
            }
    return result


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Typed selection input carrying plan, data, split and event provenance."""

    variant_hash: str
    metrics: Mapping[str, object]
    split_hash: str
    data_hash: str
    evaluation_split: str = "validation"
    event_hash: str | None = None
    window_hash: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "variant_hash", _identifier(self.variant_hash, label="variant_hash"))
        validated = _validated_metrics(self.metrics, allow_non_finite=False)
        object.__setattr__(self, "metrics", MappingProxyType(validated))
        object.__setattr__(self, "split_hash", _identifier(self.split_hash, label="split_hash"))
        object.__setattr__(self, "data_hash", _identifier(self.data_hash, label="data_hash"))
        split = _normalized_key(self.evaluation_split)
        if split not in _SELECTION_SPLITS:
            raise ValueError("selection results must come from validation or walk-forward OOS")
        object.__setattr__(self, "evaluation_split", split)
        if self.event_hash is not None:
            object.__setattr__(self, "event_hash", _sha256(self.event_hash, label="event_hash"))
        if self.window_hash is not None:
            object.__setattr__(self, "window_hash", _identifier(self.window_hash, label="window_hash"))

    @property
    def plan_hash(self) -> str:
        return self.split_hash

    @classmethod
    def from_event(cls, event: Mapping[str, object]) -> ValidationResult:
        if event.get("event_type") != "result_recorded" or event.get("status") != "success":
            raise ValueError("selection provenance must be a successful result event")
        raw_metrics = event.get("metrics")
        if not isinstance(raw_metrics, Mapping):
            raise ValueError("result event lacks metrics")
        metrics = {
            str(name): value
            for name, value in raw_metrics.items()
            if isinstance(name, str)
            and isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(float(value))
        }
        provenance = event.get("provenance")
        window_hash = provenance.get("window_hash") if isinstance(provenance, Mapping) else None
        return cls(
            variant_hash=cast(str, event.get("variant_hash")),
            metrics=metrics,
            split_hash=cast(str, event.get("plan_hash")),
            data_hash=cast(str, event.get("data_hash")),
            evaluation_split=cast(str, event.get("split")),
            event_hash=cast(str, event.get("event_hash")),
            window_hash=cast(str | None, window_hash),
        )


def select_best_variant(
    results: Sequence[ValidationResult],
    objective: SelectionObjective,
    *,
    selection_view: SelectionSplitView,
) -> ValidationResult:
    """Select exclusively from typed validation/WF results; test input is impossible."""

    if not results:
        raise ValueError("at least one validation result is required")
    if not isinstance(objective, SelectionObjective):
        raise TypeError("objective must be a SelectionObjective")
    if not isinstance(selection_view, SelectionSplitView):
        raise TypeError("selection_view must hide and bind the final-test interval")
    candidates: list[tuple[float, str, ValidationResult]] = []
    seen: set[tuple[str, str]] = set()
    for result in results:
        if not isinstance(result, ValidationResult):
            raise TypeError("selection accepts ValidationResult instances only")
        identity = (result.variant_hash, result.evaluation_split)
        if identity in seen:
            raise ValueError(f"duplicate selection result for {result.variant_hash}")
        seen.add(identity)
        if result.split_hash != selection_view.plan_hash:
            raise ValueError("validation result belongs to a different split plan")
        if result.data_hash != selection_view.dataset_hash:
            raise ValueError("validation result belongs to a different dataset snapshot")
        if objective.metric not in result.metrics:
            raise ValueError(
                f"validation result {result.variant_hash} lacks objective metric {objective.metric!r}"
            )
        score = cast(float, result.metrics[objective.metric])
        rank = -score if objective.direction == ObjectiveDirection.MAXIMIZE else score
        candidates.append((rank, result.variant_hash, result))
    return min(candidates, key=lambda item: (item[0], item[1]))[2]


def _utc_timestamp(value: datetime) -> str:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError("registry clock must return an aware UTC datetime")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


@contextmanager
def _exclusive_lock(path: Path, *, timeout_seconds: float = 30.0) -> Iterator[None]:
    """Take a best-effort portable interprocess lock on a dedicated one-byte file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            deadline = time.monotonic() + timeout_seconds
            while True:
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("timed out acquiring the registry lock") from None
                    time.sleep(0.01)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            lock_ex = cast(int, fcntl.LOCK_EX)  # type: ignore[attr-defined]
            lock_un = cast(int, fcntl.LOCK_UN)  # type: ignore[attr-defined]
            flock = cast(
                Callable[[int, int], object],
                fcntl.flock,  # type: ignore[attr-defined]
            )
            flock(handle.fileno(), lock_ex)
            try:
                yield
            finally:
                flock(handle.fileno(), lock_un)


class ResearchRegistry:
    """Durable append-only research protocol registry.

    Each JSONL event is SHA-256 chained. Mutations hold a separate portable
    interprocess lock and ``fsync`` the event before atomically replacing a
    ``.head.json`` sidecar containing the committed sequence, hash and byte size.
    Consequently removing even a complete terminal line is detected, which a
    hash chain alone cannot do. A crash between the two commits fails closed as
    a head mismatch and requires explicit audit/recovery; it is never guessed.
    """

    def __init__(self, path: Path, *, clock: Callable[[], datetime] | None = None) -> None:
        self.path = path
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._head_path = path.with_name(f"{path.name}.head.json")
        self._lock_path = path.with_name(f"{path.name}.lock")

    @staticmethod
    def _validate_events(events: Sequence[dict[str, JsonValue]]) -> None:
        plans: dict[str, dict[str, JsonValue]] = {}
        variants: dict[str, dict[str, JsonValue]] = {}
        results: set[tuple[str, str]] = set()
        result_events: dict[str, dict[str, JsonValue]] = {}
        selections: dict[str, str] = {}
        frozen: dict[str, str] = {}
        revealed: set[str] = set()
        final_results: set[str] = set()
        previous_hash: str | None = None

        for line_number, event in enumerate(events, start=1):
            event_hash = event.get("event_hash")
            unsigned = {key: value for key, value in event.items() if key != "event_hash"}
            if not isinstance(event_hash, str) or not _SHA256_RE.fullmatch(event_hash):
                raise ValueError(f"registry line {line_number} lacks a valid SHA-256 event hash")
            if event_hash != canonical_sha256(unsigned):
                raise ValueError(f"registry line {line_number} has an invalid event hash")
            if unsigned.get("sequence") != line_number:
                raise ValueError(f"registry line {line_number} has an invalid sequence")
            if unsigned.get("previous_event_hash") != previous_hash:
                raise ValueError(f"registry line {line_number} breaks the hash chain")
            event_type = unsigned.get("event_type")

            if event_type == "plan_created":
                plan_hash = unsigned.get("plan_hash")
                plan = unsigned.get("plan")
                data_hash = unsigned.get("data_hash")
                if not isinstance(plan_hash, str) or not isinstance(plan, dict):
                    raise ValueError(f"registry line {line_number} lacks its split plan")
                if plan_hash != canonical_sha256(plan):
                    raise ValueError(f"registry line {line_number} has an invalid split plan hash")
                if plan.get("dataset_hash") != data_hash:
                    raise ValueError(f"registry line {line_number} has a mismatched dataset hash")
                if plan_hash in plans:
                    raise ValueError(f"registry line {line_number} duplicates a split plan")
                plans[plan_hash] = plan
            elif event_type == "variant_registered":
                variant_hash = unsigned.get("variant_hash")
                variant = unsigned.get("variant")
                if not isinstance(variant_hash, str) or not isinstance(variant, dict):
                    raise ValueError(f"registry line {line_number} lacks its variant payload")
                if variant_hash != canonical_sha256(variant):
                    raise ValueError(f"registry line {line_number} has an invalid variant hash")
                if variant_hash in variants:
                    raise ValueError(
                        f"registry line {line_number}: variant is already registered (duplicate)"
                    )
                plan_hash = variant.get("split_hash")
                plan = plans.get(cast(str, plan_hash))
                if plan is None:
                    raise ValueError(f"registry line {line_number} registers a variant before its plan")
                if variant.get("data_hash") != plan.get("dataset_hash"):
                    raise ValueError(f"registry line {line_number} binds a variant to the wrong data")
                variants[variant_hash] = variant
            elif event_type == "result_recorded":
                variant_hash = unsigned.get("variant_hash")
                split = unsigned.get("split")
                status = unsigned.get("status")
                if not isinstance(variant_hash, str) or variant_hash not in variants:
                    raise ValueError(f"registry line {line_number} records a result before its variant")
                if split not in _ALL_RESULT_SPLITS or status not in _RESULT_STATUSES:
                    raise ValueError(f"registry line {line_number} has an invalid result split/status")
                variant = variants[variant_hash]
                plan_hash = variant.get("split_hash")
                if unsigned.get("plan_hash") != plan_hash or unsigned.get("data_hash") != variant.get(
                    "data_hash"
                ):
                    raise ValueError(f"registry line {line_number} has invalid result provenance")
                result_key = (variant_hash, cast(str, split))
                if result_key in results:
                    raise ValueError(f"registry line {line_number} duplicates a variant/split result")
                results.add(result_key)
                if status in {"failure", "interrupted"}:
                    error = unsigned.get("error")
                    if not isinstance(error, dict) or not error.get("message"):
                        raise ValueError(f"registry line {line_number} lacks failure provenance")
                if split == "final_test":
                    if (
                        not isinstance(plan_hash, str)
                        or plan_hash not in revealed
                        or frozen.get(plan_hash) != variant_hash
                    ):
                        raise ValueError(f"registry line {line_number} records an unapproved final test")
                    if plan_hash in final_results:
                        raise ValueError(f"registry line {line_number} duplicates a final-test result")
                    final_results.add(plan_hash)
                result_events[event_hash] = event
            elif event_type == "variant_selected":
                variant_hash = unsigned.get("variant_hash")
                plan_hash = unsigned.get("plan_hash")
                source_hash = unsigned.get("result_event_hash")
                source = result_events.get(cast(str, source_hash))
                if not isinstance(variant_hash, str) or variant_hash not in variants:
                    raise ValueError(f"registry line {line_number} selects an unknown variant")
                if not isinstance(plan_hash, str) or plan_hash in selections:
                    raise ValueError(f"registry line {line_number} duplicates a plan selection")
                if variants[variant_hash].get("split_hash") != plan_hash:
                    raise ValueError(f"registry line {line_number} selects from the wrong plan")
                if unsigned.get("objective") != variants[variant_hash].get("objective"):
                    raise ValueError(f"registry line {line_number} changes the frozen objective")
                if (
                    source is None
                    or source.get("variant_hash") != variant_hash
                    or source.get("status") != "success"
                    or source.get("split") not in _SELECTION_SPLITS
                ):
                    raise ValueError(f"registry line {line_number} selects without validation/WF evidence")
                selections[plan_hash] = variant_hash
            elif event_type == "final_test_frozen":
                variant_hash = unsigned.get("variant_hash")
                plan_hash = unsigned.get("plan_hash")
                if not isinstance(plan_hash, str) or selections.get(plan_hash) != variant_hash:
                    raise ValueError(f"registry line {line_number} freezes an unselected variant")
                if plan_hash in frozen:
                    raise ValueError(f"registry line {line_number} freezes the same final test twice")
                frozen[plan_hash] = cast(str, variant_hash)
            elif event_type == "final_test_revealed":
                plan_hash = unsigned.get("plan_hash")
                variant_hash = unsigned.get("variant_hash")
                if not isinstance(plan_hash, str) or frozen.get(plan_hash) != variant_hash:
                    raise ValueError(f"registry line {line_number} reveals without its matching freeze")
                if plan_hash in revealed:
                    raise ValueError(f"registry line {line_number} reveals the same final test twice")
                revealed.add(plan_hash)
            else:
                raise ValueError(f"registry line {line_number} has an unknown event type")
            previous_hash = event_hash

    def _read_head(self) -> dict[str, JsonValue] | None:
        if not self._head_path.exists():
            return None
        try:
            decoded = json.loads(self._head_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError("registry head sidecar is unreadable") from error
        if not isinstance(decoded, dict):
            raise ValueError("registry head sidecar must be a JSON object")
        head = _json_copy(decoded)
        if not isinstance(head, dict):
            raise AssertionError("head normalization did not produce an object")
        head_hash = head.get("head_hash")
        unsigned = {key: value for key, value in head.items() if key != "head_hash"}
        if head_hash != canonical_sha256(unsigned):
            raise ValueError("registry head sidecar has an invalid hash")
        return head

    def _load_events_unlocked(self) -> list[dict[str, JsonValue]]:
        if not self.path.exists():
            if self._head_path.exists():
                raise ValueError("registry file is missing but its committed head exists")
            return []
        raw = self.path.read_bytes()
        if not raw:
            if self._head_path.exists():
                raise ValueError("registry was truncated before its committed head")
            return []
        lines = raw.splitlines(keepends=True)
        if not lines[-1].endswith(b"\n"):
            raise ValueError(f"registry line {len(lines)} is incomplete")
        events: list[dict[str, JsonValue]] = []
        for line_number, raw_line in enumerate(lines, start=1):
            try:
                decoded = json.loads(raw_line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ValueError(f"registry line {line_number} is invalid JSON") from error
            if not isinstance(decoded, dict):
                raise ValueError(f"registry line {line_number} must be a JSON object")
            event = _json_copy(decoded)
            if not isinstance(event, dict):
                raise AssertionError("registry event normalization did not produce an object")
            events.append(event)
        self._validate_events(events)
        head = self._read_head()
        if head is None:
            raise ValueError("non-empty registry lacks its committed head sidecar")
        if (
            head.get("sequence") != len(events)
            or head.get("event_hash") != events[-1].get("event_hash")
            or head.get("file_size") != len(raw)
        ):
            raise ValueError("registry content does not match its committed head (possible truncation)")
        return events

    def _write_head(self, event: Mapping[str, object], file_size: int) -> None:
        unsigned: dict[str, object] = {
            "event_hash": event["event_hash"],
            "file_size": file_size,
            "schema_version": 1,
            "sequence": event["sequence"],
        }
        head = {**unsigned, "head_hash": canonical_sha256(unsigned)}
        payload = (canonical_json(head) + "\n").encode("utf-8")
        temporary = self._head_path.with_name(f".{self._head_path.name}.{os.getpid()}.{time.time_ns()}.tmp")
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            offset = 0
            while offset < len(payload):
                written = os.write(descriptor, payload[offset:])
                if written <= 0:
                    raise OSError("failed to write the complete registry head")
                offset += written
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, self._head_path)

    def _append(self, payload: Mapping[str, object]) -> dict[str, JsonValue]:
        with _exclusive_lock(self._lock_path):
            events = self._load_events_unlocked()
            previous_hash = str(events[-1]["event_hash"]) if events else None
            event: dict[str, object] = {
                **payload,
                "previous_event_hash": previous_hash,
                "recorded_at": _utc_timestamp(self._clock()),
                "sequence": len(events) + 1,
            }
            event["event_hash"] = canonical_sha256(event)
            normalized = _json_copy(event)
            if not isinstance(normalized, dict):
                raise AssertionError("registry event normalization did not produce an object")
            self._validate_events([*events, normalized])
            line = (canonical_json(event) + "\n").encode("utf-8")
            self.path.parent.mkdir(parents=True, exist_ok=True)
            flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY | getattr(os, "O_BINARY", 0)
            descriptor = os.open(self.path, flags, 0o600)
            try:
                offset = 0
                while offset < len(line):
                    written = os.write(descriptor, line[offset:])
                    if written <= 0:
                        raise OSError("failed to append the complete registry event")
                    offset += written
                os.fsync(descriptor)
                file_size = os.lseek(descriptor, 0, os.SEEK_END)
            finally:
                os.close(descriptor)
            self._write_head(normalized, file_size)
            return normalized

    def _load_events(self) -> list[dict[str, JsonValue]]:
        with _exclusive_lock(self._lock_path):
            return self._load_events_unlocked()

    def events(self) -> tuple[dict[str, JsonValue], ...]:
        return tuple(self._load_events())

    def register_plan(self, plan: SplitPlan) -> str:
        """Persist the complete UTC plan and data binding before any variant."""

        if not isinstance(plan, SplitPlan):
            raise TypeError("plan must be a SplitPlan")
        self._append(
            {
                "data_hash": plan.dataset_hash,
                "event_type": "plan_created",
                "plan": plan.to_dict(),
                "plan_hash": plan.canonical_hash,
            }
        )
        return plan.canonical_hash

    def register_variant(self, variant: VariantSpec) -> str:
        if not isinstance(variant, VariantSpec):
            raise TypeError("variant must be a VariantSpec")
        self._append(
            {
                "event_type": "variant_registered",
                "variant": variant.to_dict(),
                "variant_hash": variant.variant_hash,
            }
        )
        return variant.variant_hash

    def _registered_variant(self, variant_hash: str) -> dict[str, JsonValue]:
        normalized = _identifier(variant_hash, label="variant_hash")
        for event in self._load_events():
            if event.get("event_type") == "variant_registered" and event.get("variant_hash") == normalized:
                variant = event.get("variant")
                if isinstance(variant, dict):
                    return variant
        raise ValueError(f"variant {normalized} must be registered before recording results")

    def _result_payload(
        self,
        variant_hash: str,
        *,
        split: str,
        status: str,
        metrics: Mapping[str, object] | None,
        provenance: Mapping[str, object] | None,
        error: str | BaseException | None,
    ) -> dict[str, object]:
        variant = self._registered_variant(variant_hash)
        normalized_split = _normalized_key(split)
        if normalized_split == "final_test":
            raise ValueError(
                "use record_final_success, record_final_failure, or "
                "record_final_interrupted with a revealed FinalTestLock"
            )
        if normalized_split not in _RESULT_SPLITS:
            raise ValueError(f"unsupported result split: {split}")
        payload: dict[str, object] = {
            "data_hash": variant["data_hash"],
            "event_type": "result_recorded",
            "metrics": _validated_metrics(metrics or {}, allow_non_finite=True),
            "plan_hash": variant["split_hash"],
            "provenance": _json_copy(provenance or {}),
            "split": normalized_split,
            "status": status,
            "variant_hash": variant_hash,
        }
        if error is not None:
            message = str(error)
            if not message and isinstance(error, BaseException):
                message = type(error).__name__
            if not message:
                raise ValueError(f"{status} error cannot be empty")
            payload["error"] = {"message": message, "type": type(error).__name__}
        return payload

    def record_success(
        self,
        variant_hash: str,
        metrics: Mapping[str, object],
        *,
        split: str = "validation",
        provenance: Mapping[str, object] | None = None,
    ) -> dict[str, JsonValue]:
        return self._append(
            self._result_payload(
                variant_hash,
                split=split,
                status="success",
                metrics=metrics,
                provenance=provenance,
                error=None,
            )
        )

    def record_failure(
        self,
        variant_hash: str,
        error: str | BaseException,
        *,
        split: str = "validation",
        metrics: Mapping[str, object] | None = None,
        provenance: Mapping[str, object] | None = None,
    ) -> dict[str, JsonValue]:
        return self._append(
            self._result_payload(
                variant_hash,
                split=split,
                status="failure",
                metrics=metrics,
                provenance=provenance,
                error=error,
            )
        )

    def record_interrupted(
        self,
        variant_hash: str,
        reason: str | BaseException,
        *,
        split: str = "validation",
        metrics: Mapping[str, object] | None = None,
        provenance: Mapping[str, object] | None = None,
    ) -> dict[str, JsonValue]:
        return self._append(
            self._result_payload(
                variant_hash,
                split=split,
                status="interrupted",
                metrics=metrics,
                provenance=provenance,
                error=reason,
            )
        )

    def record_selection(
        self,
        result: ValidationResult,
        *,
        objective: SelectionObjective,
        selection_view: SelectionSplitView,
    ) -> dict[str, JsonValue]:
        if not isinstance(result, ValidationResult):
            raise TypeError("result must be a ValidationResult")
        select_best_variant([result], objective, selection_view=selection_view)
        if result.event_hash is None:
            raise ValueError("selected result must carry its registry event_hash provenance")
        source = next(
            (
                event
                for event in self._load_events()
                if event.get("event_hash") == result.event_hash
                and event.get("event_type") == "result_recorded"
            ),
            None,
        )
        if source is None:
            raise ValueError("selected result event_hash does not exist in this registry")
        persisted = ValidationResult.from_event(source)
        if (
            persisted.variant_hash != result.variant_hash
            or dict(persisted.metrics) != dict(result.metrics)
            or persisted.split_hash != result.split_hash
            or persisted.data_hash != result.data_hash
            or persisted.evaluation_split != result.evaluation_split
            or persisted.window_hash != result.window_hash
        ):
            raise ValueError("selected result does not exactly match its persisted event provenance")
        return self._append(
            {
                "data_hash": selection_view.dataset_hash,
                "event_type": "variant_selected",
                "objective": objective.to_dict(),
                "plan_hash": selection_view.plan_hash,
                "result_event_hash": result.event_hash,
                "source_split": result.evaluation_split,
                "variant_hash": result.variant_hash,
            }
        )

    def freeze_final_test(self, variant_hash: str, lock: FinalTestLock) -> str:
        """Persist the selected freeze before returning the deterministic token."""

        if not isinstance(lock, FinalTestLock):
            raise TypeError("lock must be a FinalTestLock")
        if lock.state != FinalTestState.LOCKED:
            raise ValueError("the supplied final-test lock is not locked")
        variant = self._registered_variant(variant_hash)
        if variant.get("split_hash") != lock.plan_hash or variant.get("data_hash") != lock.dataset_hash:
            raise ValueError("the registered variant belongs to a different split plan or dataset")
        self._append(
            {
                "data_hash": lock.dataset_hash,
                "event_type": "final_test_frozen",
                "plan_hash": lock.plan_hash,
                "variant_hash": variant_hash,
            }
        )
        return lock.freeze_variant(variant_hash)

    def reveal_final_test(self, variant_hash: str, lock: FinalTestLock, token: str) -> TimeRange:
        """Reveal once and persist auditable evidence before final evaluation."""

        if not isinstance(lock, FinalTestLock):
            raise TypeError("lock must be a FinalTestLock")
        if lock.frozen_variant_hash != variant_hash:
            raise ValueError("the final-test lock is bound to a different variant")
        lock.verify_reveal_token(token)
        self._append(
            {
                "data_hash": lock.dataset_hash,
                "event_type": "final_test_revealed",
                "plan_hash": lock.plan_hash,
                "variant_hash": variant_hash,
            }
        )
        return lock.reveal_final_test(token)

    def _final_payload(
        self,
        variant_hash: str,
        *,
        lock: FinalTestLock,
        status: str,
        metrics: Mapping[str, object] | None,
        error: str | BaseException | None,
    ) -> dict[str, object]:
        variant = self._registered_variant(variant_hash)
        if not isinstance(lock, FinalTestLock) or lock.state != FinalTestState.REVEALED:
            raise ValueError("the final test must be revealed after freezing the variant")
        if lock.frozen_variant_hash != variant_hash:
            raise ValueError("the final-test lock is bound to a different variant")
        if variant.get("split_hash") != lock.plan_hash or variant.get("data_hash") != lock.dataset_hash:
            raise ValueError("the registered variant belongs to a different split plan or dataset")
        payload: dict[str, object] = {
            "data_hash": lock.dataset_hash,
            "event_type": "result_recorded",
            "metrics": _validated_metrics(metrics or {}, allow_non_finite=True),
            "plan_hash": lock.plan_hash,
            "provenance": {"lock_state": lock.state.value},
            "split": "final_test",
            "status": status,
            "variant_hash": variant_hash,
        }
        if error is not None:
            message = str(error)
            if not message and isinstance(error, BaseException):
                message = type(error).__name__
            if not message:
                raise ValueError(f"{status} error cannot be empty")
            payload["error"] = {"message": message, "type": type(error).__name__}
        return payload

    def record_final_success(
        self,
        variant_hash: str,
        metrics: Mapping[str, object],
        *,
        lock: FinalTestLock,
    ) -> dict[str, JsonValue]:
        return self._append(
            self._final_payload(variant_hash, lock=lock, status="success", metrics=metrics, error=None)
        )

    def record_final_failure(
        self,
        variant_hash: str,
        error: str | BaseException,
        *,
        lock: FinalTestLock,
        metrics: Mapping[str, object] | None = None,
    ) -> dict[str, JsonValue]:
        return self._append(
            self._final_payload(variant_hash, lock=lock, status="failure", metrics=metrics, error=error)
        )

    def record_final_interrupted(
        self,
        variant_hash: str,
        reason: str | BaseException,
        *,
        lock: FinalTestLock,
        metrics: Mapping[str, object] | None = None,
    ) -> dict[str, JsonValue]:
        return self._append(
            self._final_payload(variant_hash, lock=lock, status="interrupted", metrics=metrics, error=reason)
        )
