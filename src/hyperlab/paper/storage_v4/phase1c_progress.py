"""Deterministic, fail-closed Phase 1C heartbeat progress windows.

This module is deliberately stdlib-only.  It normalizes the latest completed
workload boundary and derives recent throughput only from two observations of
the same immutable workload identity.  A heartbeat never infers descendant
process activity or a stagnation verdict from missing counters.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal, localcontext

_NANOSECONDS_PER_SECOND = 1_000_000_000
_NO_ACTIVE_WORKLOAD = "UNAVAILABLE_NO_ACTIVE_WORKLOAD_PROGRESS"
_INVALID_COUNTERS = "UNAVAILABLE_PROGRESS_COUNTERS_MISSING_OR_INVALID"
_INVALID_SEGMENT_COUNTERS = (
    "UNAVAILABLE_SEGMENT_OR_CHECKPOINT_COUNTERS_MISSING_INVALID_OR_INCOHERENT"
)
_SEGMENT_COUNTER_REGRESSION = "UNAVAILABLE_SEGMENT_OR_CHECKPOINT_COUNTER_REGRESSION"
_INSUFFICIENT_WINDOW = "UNAVAILABLE_INSUFFICIENT_HEARTBEAT_WINDOW"


def _text(value: object) -> str | None:
    return value if type(value) is str and bool(value) else None


def _counter(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _status(value: object, *, available: bool, unavailable: str) -> str:
    observed = _text(value)
    if observed is not None:
        return observed
    return "AVAILABLE" if available else unavailable


def _rate_text(completed: int, elapsed_ns: int) -> str:
    if elapsed_ns <= 0:
        raise ValueError("recent throughput requires a positive elapsed window")
    with localcontext() as context:
        context.prec = 28
        rate = (
            Decimal(completed)
            * Decimal(_NANOSECONDS_PER_SECOND)
            / Decimal(elapsed_ns)
        )
    return format(rate.normalize(), "f")


def _ceil_div(numerator: int, denominator: int) -> int:
    if numerator < 0 or denominator <= 0:
        raise ValueError("ceiling division requires non-negative/positive operands")
    return (numerator + denominator - 1) // denominator


@dataclass(frozen=True, slots=True)
class HeartbeatCounterSample:
    """One validated cumulative workload observation."""

    workload_id: str
    observed_elapsed_ns: int
    workload_elapsed_ns: int
    commits_completed: int
    commits_total: int
    logical_rows_completed: int
    logical_rows_total: int
    raw_segment_count: int
    paper_segment_count: int
    segment_count: int
    checkpoint_count: int


@dataclass(slots=True)
class Phase1CHeartbeatWindow:
    """Render normalized fields and an exact same-workload recent window."""

    _previous: HeartbeatCounterSample | None = field(default=None, init=False)

    def render(
        self,
        progress: Mapping[str, object] | None,
        *,
        observed_elapsed_ns: int,
    ) -> dict[str, object]:
        if type(observed_elapsed_ns) is not int or observed_elapsed_ns < 0:
            raise ValueError("heartbeat elapsed time must be a non-negative exact integer")

        source: Mapping[str, object] = {} if progress is None else progress
        workload = _text(source.get("workload"))
        workload_profile = _text(source.get("workload_profile"))
        workload_id = _text(source.get("workload_id"))
        commits_completed = _counter(source.get("commits_completed"))
        commits_total = _counter(source.get("commits_total"))
        logical_rows_completed = _counter(source.get("logical_rows_completed"))
        logical_rows_total = _counter(source.get("logical_rows_total"))
        workload_elapsed_ns = _counter(
            source.get("workload_elapsed_ns", source.get("elapsed_ns"))
        )
        cpu_ns = _counter(source.get("cpu_ns"))
        peak_rss_bytes = _counter(source.get("peak_rss_bytes"))
        bytes_written = _counter(source.get("bytes_written"))
        raw_segment_count = _counter(source.get("raw_segment_count"))
        paper_segment_count = _counter(source.get("paper_segment_count"))
        segment_count = _counter(source.get("segment_count"))
        checkpoint_count = _counter(source.get("checkpoint_count"))
        segment_counts_valid = (
            raw_segment_count is not None
            and paper_segment_count is not None
            and segment_count is not None
            and checkpoint_count is not None
            and segment_count == raw_segment_count + paper_segment_count
        )

        normalized: dict[str, object] = {
            "workload": workload,
            "workload_profile": workload_profile,
            "workload_id": workload_id,
            "commits_completed": commits_completed,
            "commits_total": commits_total,
            "logical_rows_completed": logical_rows_completed,
            "logical_rows_total": logical_rows_total,
            "workload_elapsed_ns": workload_elapsed_ns,
            "cpu_ns": cpu_ns,
            "cpu_status": _status(
                source.get("cpu_status"),
                available=cpu_ns is not None,
                unavailable="UNAVAILABLE_WORKLOAD_CPU_COUNTER",
            ),
            "peak_rss_bytes": peak_rss_bytes,
            "peak_rss_status": _status(
                source.get("peak_rss_status"),
                available=peak_rss_bytes is not None,
                unavailable="UNAVAILABLE_WORKLOAD_RSS_COUNTER",
            ),
            "bytes_written": bytes_written,
            "bytes_written_status": _status(
                source.get("bytes_written_status"),
                available=bytes_written is not None,
                unavailable="UNAVAILABLE_WORKLOAD_WRITE_BYTE_COUNTER",
            ),
            "raw_segment_count": raw_segment_count,
            "paper_segment_count": paper_segment_count,
            "segment_count": segment_count,
            "checkpoint_count": checkpoint_count,
            "segment_checkpoint_status": (
                _status(
                    source.get("segment_checkpoint_status"),
                    available=True,
                    unavailable=_INVALID_SEGMENT_COUNTERS,
                )
                if segment_counts_valid
                else _INVALID_SEGMENT_COUNTERS
            ),
            "progress_metrics_scope": (
                _text(source.get("progress_metrics_scope"))
                or "UNAVAILABLE_NO_COMPLETED_WORKLOAD_PROGRESS_BOUNDARY"
            ),
            "recent_window_elapsed_ns": None,
            "recent_commits_completed": None,
            "recent_logical_rows_completed": None,
            "recent_commits_per_second": None,
            "recent_logical_rows_per_second": None,
            "recent_throughput_status": _NO_ACTIVE_WORKLOAD,
            "conservative_eta_ns": None,
            "conservative_eta_status": _NO_ACTIVE_WORKLOAD,
        }

        metadata_available = all(
            value is not None for value in (workload, workload_profile, workload_id)
        )
        counters_available = all(
            value is not None
            for value in (
                commits_completed,
                commits_total,
                logical_rows_completed,
                logical_rows_total,
                workload_elapsed_ns,
            )
        )
        if not metadata_available or not counters_available:
            self._previous = None
            unavailable = _NO_ACTIVE_WORKLOAD if not metadata_available else _INVALID_COUNTERS
            normalized["recent_throughput_status"] = unavailable
            normalized["conservative_eta_status"] = unavailable
            return normalized

        assert workload_id is not None
        assert commits_completed is not None
        assert commits_total is not None
        assert logical_rows_completed is not None
        assert logical_rows_total is not None
        assert workload_elapsed_ns is not None
        if commits_completed > commits_total or logical_rows_completed > logical_rows_total:
            self._previous = None
            normalized["recent_throughput_status"] = _INVALID_COUNTERS
            normalized["conservative_eta_status"] = _INVALID_COUNTERS
            return normalized
        if not segment_counts_valid:
            self._previous = None
            normalized["recent_throughput_status"] = _INVALID_SEGMENT_COUNTERS
            normalized["conservative_eta_status"] = _INVALID_SEGMENT_COUNTERS
            return normalized

        assert raw_segment_count is not None
        assert paper_segment_count is not None
        assert segment_count is not None
        assert checkpoint_count is not None
        current = HeartbeatCounterSample(
            workload_id=workload_id,
            observed_elapsed_ns=observed_elapsed_ns,
            workload_elapsed_ns=workload_elapsed_ns,
            commits_completed=commits_completed,
            commits_total=commits_total,
            logical_rows_completed=logical_rows_completed,
            logical_rows_total=logical_rows_total,
            raw_segment_count=raw_segment_count,
            paper_segment_count=paper_segment_count,
            segment_count=segment_count,
            checkpoint_count=checkpoint_count,
        )
        complete = (
            commits_completed == commits_total
            and logical_rows_completed == logical_rows_total
        )
        previous = self._previous
        if previous is None or previous.workload_id != current.workload_id:
            self._previous = current
            normalized["recent_throughput_status"] = _INSUFFICIENT_WINDOW
            normalized["conservative_eta_status"] = _INSUFFICIENT_WINDOW
            if complete:
                normalized["conservative_eta_ns"] = 0
                normalized["conservative_eta_status"] = "COMPLETE"
            return normalized

        if current.observed_elapsed_ns <= previous.observed_elapsed_ns:
            unavailable = "UNAVAILABLE_HEARTBEAT_CLOCK_NOT_ADVANCED"
            normalized["recent_throughput_status"] = unavailable
            normalized["conservative_eta_status"] = unavailable
            return normalized
        if (
            current.commits_total != previous.commits_total
            or current.logical_rows_total != previous.logical_rows_total
        ):
            unavailable = "UNAVAILABLE_WORKLOAD_TOTAL_CHANGED"
            normalized["recent_throughput_status"] = unavailable
            normalized["conservative_eta_status"] = unavailable
            return normalized
        segment_counters_regressed = (
            current.raw_segment_count < previous.raw_segment_count
            or current.paper_segment_count < previous.paper_segment_count
            or current.segment_count < previous.segment_count
            or current.checkpoint_count < previous.checkpoint_count
        )
        if (
            current.commits_completed < previous.commits_completed
            or current.logical_rows_completed < previous.logical_rows_completed
            or current.workload_elapsed_ns < previous.workload_elapsed_ns
        ):
            unavailable = "UNAVAILABLE_COUNTER_REGRESSION"
            if segment_counters_regressed:
                normalized["segment_checkpoint_status"] = (
                    _SEGMENT_COUNTER_REGRESSION
                )
            normalized["recent_throughput_status"] = unavailable
            normalized["conservative_eta_status"] = unavailable
            return normalized
        if segment_counters_regressed:
            normalized["segment_checkpoint_status"] = _SEGMENT_COUNTER_REGRESSION
            normalized["recent_throughput_status"] = _SEGMENT_COUNTER_REGRESSION
            normalized["conservative_eta_status"] = _SEGMENT_COUNTER_REGRESSION
            return normalized

        recent_elapsed_ns = current.observed_elapsed_ns - previous.observed_elapsed_ns
        recent_commits = current.commits_completed - previous.commits_completed
        recent_rows = current.logical_rows_completed - previous.logical_rows_completed
        self._previous = current
        normalized["recent_window_elapsed_ns"] = recent_elapsed_ns
        normalized["recent_commits_completed"] = recent_commits
        normalized["recent_logical_rows_completed"] = recent_rows
        if recent_commits == 0 and recent_rows == 0:
            unavailable = "UNAVAILABLE_NO_POSITIVE_RECENT_PROGRESS"
            normalized["recent_throughput_status"] = unavailable
            normalized["conservative_eta_status"] = "COMPLETE" if complete else unavailable
            if complete:
                normalized["conservative_eta_ns"] = 0
            return normalized

        normalized["recent_commits_per_second"] = _rate_text(
            recent_commits,
            recent_elapsed_ns,
        )
        normalized["recent_logical_rows_per_second"] = _rate_text(
            recent_rows,
            recent_elapsed_ns,
        )
        normalized["recent_throughput_status"] = "AVAILABLE_SAME_WORKLOAD_WINDOW"
        if complete:
            normalized["conservative_eta_ns"] = 0
            normalized["conservative_eta_status"] = "COMPLETE"
            return normalized
        dimensions = (
            (
                current.commits_total - current.commits_completed,
                recent_commits,
                current.commits_completed,
            ),
            (
                current.logical_rows_total - current.logical_rows_completed,
                recent_rows,
                current.logical_rows_completed,
            ),
        )
        eta_candidates: list[int] = []
        for remaining, recent_completed, overall_completed in dimensions:
            if remaining == 0:
                continue
            if recent_completed == 0:
                normalized["conservative_eta_status"] = (
                    "UNAVAILABLE_INCOMPLETE_DIMENSION_HAS_NO_POSITIVE_RECENT_RATE"
                )
                return normalized
            if overall_completed == 0 or current.workload_elapsed_ns == 0:
                normalized["conservative_eta_status"] = (
                    "UNAVAILABLE_INCOMPLETE_DIMENSION_HAS_NO_POSITIVE_OVERALL_RATE"
                )
                return normalized
            eta_candidates.extend(
                (
                    _ceil_div(remaining * recent_elapsed_ns, recent_completed),
                    _ceil_div(
                        remaining * current.workload_elapsed_ns,
                        overall_completed,
                    ),
                )
            )
        normalized["conservative_eta_ns"] = max(eta_candidates)
        normalized["conservative_eta_status"] = (
            "AVAILABLE_MAX_OF_COMMIT_ROW_RECENT_AND_OVERALL_RATES"
        )
        return normalized


__all__ = ["HeartbeatCounterSample", "Phase1CHeartbeatWindow"]
