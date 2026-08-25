"""Deterministic crash boundaries for the isolated Storage v4 engine.

Fault injection is deliberately callback based.  Production callers normally
pass ``None`` while tests install :class:`DeterministicFaultInjector` at one
precise boundary.  An injected crash is never translated into an ordinary I/O
error, which lets recovery tests distinguish an interrupted publication from a
rejected one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, TypeAlias


class FaultPoint(StrEnum):
    """Stable before/after boundaries used by Phase 1B recovery tests."""

    BEFORE_TEMP_WRITE = "before_temp_write"
    AFTER_TEMP_WRITE = "after_temp_write"
    BEFORE_FLUSH = "before_flush"
    AFTER_FLUSH = "after_flush"
    BEFORE_FILE_FSYNC = "before_file_fsync"
    AFTER_FILE_FSYNC = "after_file_fsync"
    BEFORE_RENAME = "before_rename"
    AFTER_RENAME = "after_rename"
    BEFORE_EXCLUSIVE_PUBLISH = "before_exclusive_publish"
    AFTER_EXCLUSIVE_PUBLISH = "after_exclusive_publish"
    BEFORE_DIRECTORY_FSYNC = "before_directory_fsync"
    AFTER_DIRECTORY_FSYNC = "after_directory_fsync"
    BEFORE_SEGMENT_PUBLICATION = "before_segment_publication"
    AFTER_SEGMENT_PUBLICATION = "after_segment_publication"
    BEFORE_CHECKPOINT_PUBLICATION = "before_checkpoint_publication"
    AFTER_CHECKPOINT_PUBLICATION = "after_checkpoint_publication"
    BEFORE_MANIFEST_PUBLICATION = "before_manifest_publication"
    AFTER_MANIFEST_PUBLICATION = "after_manifest_publication"
    BEFORE_CURRENT_PUBLICATION = "before_current_publication"
    AFTER_CURRENT_PUBLICATION = "after_current_publication"
    BEFORE_ANCHOR_PUBLICATION = "before_anchor_publication"
    AFTER_ANCHOR_PUBLICATION = "after_anchor_publication"
    BEFORE_OVERLAY_TRANSACTION = "before_overlay_transaction"
    AFTER_OVERLAY_TRANSACTION = "after_overlay_transaction"


class FaultCallback(Protocol):
    """A synchronous callback invoked immediately at one crash boundary."""

    def __call__(self, point: FaultPoint, /) -> None: ...


FaultHook: TypeAlias = FaultCallback | None


class InjectedCrash(RuntimeError):
    """A deterministic simulated process interruption."""

    def __init__(self, point: FaultPoint, occurrence: int) -> None:
        self.point = point
        self.occurrence = occurrence
        super().__init__(
            f"injected crash at {point.value} occurrence {occurrence}"
        )


@dataclass(slots=True)
class DeterministicFaultInjector:
    """Raise once at the selected occurrence of one selected fault point."""

    point: FaultPoint
    occurrence: int = 1
    _seen: int = field(init=False, default=0, repr=False)
    _triggered: bool = field(init=False, default=False, repr=False)

    def __post_init__(self) -> None:
        if type(self.point) is not FaultPoint:
            raise TypeError("fault injector point must be FaultPoint")
        if type(self.occurrence) is not int:
            raise TypeError("fault injector occurrence must be an exact integer")
        if self.occurrence < 1:
            raise ValueError("fault injector occurrence must be positive")

    @property
    def seen(self) -> int:
        return self._seen

    @property
    def triggered(self) -> bool:
        return self._triggered

    def __call__(self, point: FaultPoint, /) -> None:
        if type(point) is not FaultPoint:
            raise TypeError("fault callback point must be FaultPoint")
        if point is not self.point or self._triggered:
            return
        self._seen += 1
        if self._seen == self.occurrence:
            self._triggered = True
            raise InjectedCrash(point, self._seen)

    def reset(self) -> None:
        """Re-arm the same deterministic plan for a separate recovery run."""

        self._seen = 0
        self._triggered = False


def trigger_fault(hook: FaultHook, point: FaultPoint) -> None:
    """Invoke ``hook`` without hiding injected or caller-defined failures."""

    if type(point) is not FaultPoint:
        raise TypeError("fault point must be FaultPoint")
    if hook is not None:
        hook(point)


__all__ = [
    "DeterministicFaultInjector",
    "FaultCallback",
    "FaultHook",
    "FaultPoint",
    "InjectedCrash",
    "trigger_fault",
]
