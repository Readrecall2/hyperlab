from __future__ import annotations

import gc
import importlib
import math
import os
import sys
import threading
import time
from collections import deque
from collections.abc import Callable
from pathlib import Path
from typing import Any


class MonotonicTimingSummary:
    """Thread-safe timing summary with bounded percentile storage."""

    def __init__(self, *, window_capacity: int = 2_048) -> None:
        if window_capacity <= 0:
            raise ValueError("timing window capacity must be positive")
        self.window_capacity = window_capacity
        self._samples_ns: deque[int] = deque(maxlen=window_capacity)
        self._count = 0
        self._total_ns = 0
        self._minimum_ns: int | None = None
        self._maximum_ns: int | None = None
        self._lock = threading.Lock()

    def observe_ns(self, duration_ns: int) -> None:
        if isinstance(duration_ns, bool) or duration_ns < 0:
            raise ValueError("monotonic duration must be a non-negative integer")
        with self._lock:
            self._samples_ns.append(duration_ns)
            self._count += 1
            self._total_ns += duration_ns
            self._minimum_ns = duration_ns if self._minimum_ns is None else min(self._minimum_ns, duration_ns)
            self._maximum_ns = duration_ns if self._maximum_ns is None else max(self._maximum_ns, duration_ns)

    def observe_seconds(self, duration_seconds: float) -> None:
        if not math.isfinite(duration_seconds) or duration_seconds < 0:
            raise ValueError("monotonic duration must be finite and non-negative")
        self.observe_ns(round(duration_seconds * 1_000_000_000))

    def observe_since(
        self,
        started_monotonic_ns: int,
        *,
        ended_monotonic_ns: int | None = None,
    ) -> None:
        ended = time.monotonic_ns() if ended_monotonic_ns is None else ended_monotonic_ns
        if ended < started_monotonic_ns:
            raise ValueError("monotonic end precedes start")
        self.observe_ns(ended - started_monotonic_ns)

    @staticmethod
    def _percentile(values: list[int], quantile: float) -> float:
        position = (len(values) - 1) * quantile
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            return float(values[lower])
        fraction = position - lower
        return values[lower] + (values[upper] - values[lower]) * fraction

    def as_dict(self) -> dict[str, object]:
        with self._lock:
            values = sorted(self._samples_ns)
            count = self._count
            total_ns = self._total_ns
            minimum_ns = self._minimum_ns
            maximum_ns = self._maximum_ns
        if not values:
            return {
                "count": 0,
                "window_count": 0,
                "window_capacity": self.window_capacity,
                "window_truncated": False,
                "min_ms": None,
                "mean_ms": None,
                "p50_ms": None,
                "p95_ms": None,
                "p99_ms": None,
                "max_ms": None,
            }

        def milliseconds(value_ns: float | int | None) -> float | None:
            return None if value_ns is None else value_ns / 1_000_000

        return {
            "count": count,
            "window_count": len(values),
            "window_capacity": self.window_capacity,
            "window_truncated": count > len(values),
            "min_ms": milliseconds(minimum_ns),
            "mean_ms": milliseconds(total_ns / count),
            "p50_ms": milliseconds(self._percentile(values, 0.50)),
            "p95_ms": milliseconds(self._percentile(values, 0.95)),
            "p99_ms": milliseconds(self._percentile(values, 0.99)),
            "max_ms": milliseconds(maximum_ns),
        }


class SchedulingWatchdog:
    """Measure Python watchdog wake-up lag without performing collector work."""

    def __init__(
        self,
        *,
        interval_seconds: float = 0.1,
        window_capacity: int = 2_048,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        thread_name: str = "hyperlab-scheduling-watchdog",
    ) -> None:
        if not math.isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError("watchdog interval must be finite and positive")
        if not thread_name:
            raise ValueError("watchdog thread name must not be empty")
        self.interval_seconds = interval_seconds
        self._interval_ns = round(interval_seconds * 1_000_000_000)
        self._monotonic_ns = monotonic_ns
        self._stop = threading.Event()
        self._started = False
        self._start_lock = threading.Lock()
        self.lag = MonotonicTimingSummary(window_capacity=window_capacity)
        self._thread = threading.Thread(
            target=self._run,
            name=thread_name,
            daemon=True,
        )

    def start(self) -> None:
        with self._start_lock:
            if self._started:
                return
            if self._stop.is_set():
                raise RuntimeError("cannot restart a stopped scheduling watchdog")
            self._thread.start()
            self._started = True

    def close(self) -> None:
        self._stop.set()
        if self._started and threading.current_thread() is not self._thread:
            self._thread.join(timeout=max(self.interval_seconds * 4, 1.0))

    def _run(self) -> None:
        expected = self._monotonic_ns() + self._interval_ns
        while not self._stop.wait(self.interval_seconds):
            observed = self._monotonic_ns()
            self.lag.observe_ns(max(observed - expected, 0))
            elapsed_intervals = max(((observed - expected) // self._interval_ns) + 1, 1)
            expected += elapsed_intervals * self._interval_ns

    def as_dict(self) -> dict[str, object]:
        return {
            "started": self._started,
            "thread_alive": self._thread.is_alive(),
            "interval_ms": self.interval_seconds * 1_000,
            "lag_ms": self.lag.as_dict(),
        }


def _proc_status() -> dict[str, int]:
    path = Path("/proc/self/status")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return {}
    result: dict[str, int] = {}
    for line in lines:
        label, separator, raw_value = line.partition(":")
        if not separator:
            continue
        fields = raw_value.strip().split()
        if not fields or not fields[0].isdigit():
            continue
        value = int(fields[0])
        if len(fields) > 1 and fields[1].lower() == "kb":
            value *= 1_024
        result[label] = value
    return result


def _resource_usage() -> Any | None:
    try:
        resource = importlib.import_module("resource")
        return resource.getrusage(resource.RUSAGE_SELF)
    except (AttributeError, ImportError, OSError):
        return None


def _process_memory(status: dict[str, int], usage: Any | None) -> dict[str, object]:
    current_rss = status.get("VmRSS")
    peak_rss = status.get("VmHWM")
    if peak_rss is None and usage is not None:
        raw_peak = int(usage.ru_maxrss)
        peak_rss = raw_peak if sys.platform == "darwin" else raw_peak * 1_024
    allocated_blocks = getattr(sys, "getallocatedblocks", None)
    return {
        "rss_bytes": current_rss,
        "peak_rss_bytes": peak_rss,
        "python_allocated_blocks": (int(allocated_blocks()) if callable(allocated_blocks) else None),
    }


def _context_switches(status: dict[str, int], usage: Any | None) -> dict[str, int | None]:
    voluntary = status.get("voluntary_ctxt_switches")
    involuntary = status.get("nonvoluntary_ctxt_switches")
    if voluntary is None and usage is not None:
        voluntary = int(usage.ru_nvcsw)
    if involuntary is None and usage is not None:
        involuntary = int(usage.ru_nivcsw)
    return {
        "voluntary": voluntary,
        "involuntary": involuntary,
    }


def _read_linux_text(path: str) -> str | None:
    try:
        return Path(path).read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return None


def _parse_linux_int_fields(payload: str | None) -> dict[str, int] | None:
    if payload is None:
        return None
    values: dict[str, int] = {}
    for line in payload.splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        try:
            values[fields[0]] = int(fields[1])
        except ValueError:
            continue
    return values or None


def _parse_pressure(payload: str | None) -> dict[str, object] | None:
    if payload is None:
        return None
    result: dict[str, object] = {}
    for line in payload.splitlines():
        fields = line.split()
        if not fields:
            continue
        metrics: dict[str, float | int] = {}
        for field in fields[1:]:
            label, separator, raw_value = field.partition("=")
            if not separator:
                continue
            try:
                metrics[label] = int(raw_value) if label == "total" else float(raw_value)
            except ValueError:
                continue
        if metrics:
            result[fields[0]] = metrics
    return result or None


def _parse_schedstat(payload: str | None) -> dict[str, int] | None:
    if payload is None:
        return None
    fields = payload.split()
    if len(fields) < 3:
        return None
    try:
        values = [int(field) for field in fields[:3]]
    except ValueError:
        return None
    return {
        "cpu_runtime_ns": values[0],
        "runqueue_wait_ns": values[1],
        "timeslices": values[2],
    }


def _parse_proc_stat_cpu(payload: str | None) -> dict[str, int] | None:
    if payload is None:
        return None
    first = next((line for line in payload.splitlines() if line.startswith("cpu ")), None)
    if first is None:
        return None
    labels = (
        "user_ticks",
        "nice_ticks",
        "system_ticks",
        "idle_ticks",
        "iowait_ticks",
        "irq_ticks",
        "softirq_ticks",
        "steal_ticks",
        "guest_ticks",
        "guest_nice_ticks",
    )
    try:
        values = [int(field) for field in first.split()[1:]]
    except ValueError:
        return None
    return dict(zip(labels, values, strict=False))


def _parse_cpu_max(payload: str | None) -> dict[str, object] | None:
    if payload is None:
        return None
    fields = payload.split()
    if len(fields) != 2:
        return None
    try:
        period_us = int(fields[1])
        quota_us = None if fields[0] == "max" else int(fields[0])
    except ValueError:
        return None
    return {
        "quota_us": quota_us,
        "period_us": period_us,
        "quota_cores": None if quota_us is None or period_us <= 0 else quota_us / period_us,
    }


def _linux_scheduler_snapshot() -> dict[str, object]:
    if not sys.platform.startswith("linux"):
        return {"available": False}
    return {
        "available": True,
        "proc_self_schedstat": _parse_schedstat(_read_linux_text("/proc/self/schedstat")),
        "cgroup_v2_cpu_stat": _parse_linux_int_fields(_read_linux_text("/sys/fs/cgroup/cpu.stat")),
        "cgroup_v2_cpu_max": _parse_cpu_max(_read_linux_text("/sys/fs/cgroup/cpu.max")),
        "pressure": {
            resource_name: _parse_pressure(_read_linux_text(f"/proc/pressure/{resource_name}"))
            for resource_name in ("cpu", "memory", "io")
        },
        "host_cpu_ticks": _parse_proc_stat_cpu(_read_linux_text("/proc/stat")),
    }


class ProcessRuntimeTelemetry:
    """Low-overhead stdlib process and scheduling telemetry for runtime JSON."""

    def __init__(
        self,
        *,
        window_capacity: int = 2_048,
        watchdog_interval_seconds: float = 0.1,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        process_time_ns: Callable[[], int] = time.process_time_ns,
        auto_start: bool = True,
    ) -> None:
        self._monotonic_ns = monotonic_ns
        self._process_time_ns = process_time_ns
        self._sample_lock = threading.Lock()
        self._last_wall_ns = monotonic_ns()
        self._last_cpu_ns = process_time_ns()
        self.worker_scheduling_lag = MonotonicTimingSummary(window_capacity=window_capacity)
        self.gc_pause = MonotonicTimingSummary(window_capacity=window_capacity)
        self._gc_lock = threading.Lock()
        self._gc_started_by_generation: dict[int, int] = {}
        self._gc_callback: Callable[[str, dict[str, Any]], None] = self._observe_gc
        gc.callbacks.append(self._gc_callback)
        self._closed = False
        self.watchdog = SchedulingWatchdog(
            interval_seconds=watchdog_interval_seconds,
            window_capacity=window_capacity,
            monotonic_ns=monotonic_ns,
        )
        if auto_start:
            self.watchdog.start()

    def __enter__(self) -> ProcessRuntimeTelemetry:
        self.watchdog.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._gc_callback in gc.callbacks:
            gc.callbacks.remove(self._gc_callback)
        self.watchdog.close()

    def _observe_gc(self, phase: str, info: dict[str, Any]) -> None:
        generation = info.get("generation")
        if not isinstance(generation, int):
            return
        observed = self._monotonic_ns()
        started: int | None = None
        with self._gc_lock:
            if phase == "start":
                self._gc_started_by_generation[generation] = observed
            elif phase == "stop":
                started = self._gc_started_by_generation.pop(generation, None)
        if started is not None:
            self.gc_pause.observe_ns(max(observed - started, 0))

    def record_worker_scheduling_lag(
        self,
        expected_monotonic_ns: int,
        *,
        observed_monotonic_ns: int | None = None,
    ) -> None:
        observed = self._monotonic_ns() if observed_monotonic_ns is None else observed_monotonic_ns
        self.worker_scheduling_lag.observe_ns(max(observed - expected_monotonic_ns, 0))

    def snapshot(self) -> dict[str, object]:
        wall_now = self._monotonic_ns()
        cpu_now = self._process_time_ns()
        with self._sample_lock:
            wall_delta = max(wall_now - self._last_wall_ns, 0)
            cpu_delta = max(cpu_now - self._last_cpu_ns, 0)
            self._last_wall_ns = wall_now
            self._last_cpu_ns = cpu_now
        core_ratio = None if wall_delta == 0 else cpu_delta / wall_delta
        logical_cpu_count = os.cpu_count()
        status = _proc_status()
        usage = _resource_usage()
        gc_stats = [
            {
                "generation": generation,
                "collections": int(values.get("collections", 0)),
                "collected": int(values.get("collected", 0)),
                "uncollectable": int(values.get("uncollectable", 0)),
            }
            for generation, values in enumerate(gc.get_stats())
        ]
        return {
            "process_cpu": {
                "sample_wall_ms": wall_delta / 1_000_000,
                "core_ratio": core_ratio,
                "core_percent": None if core_ratio is None else core_ratio * 100,
                "host_capacity_ratio": (
                    None if core_ratio is None or not logical_cpu_count else core_ratio / logical_cpu_count
                ),
                "logical_cpu_count": logical_cpu_count,
            },
            "memory": _process_memory(status, usage),
            "gc": {
                "counts": list(gc.get_count()),
                "thresholds": list(gc.get_threshold()),
                "generations": gc_stats,
                "pause_ms": self.gc_pause.as_dict(),
            },
            "context_switches": _context_switches(status, usage),
            "linux_scheduler": _linux_scheduler_snapshot(),
            "threads": {
                "active_count": threading.active_count(),
                "proc_status_count": status.get("Threads"),
            },
            "scheduling": {
                "worker_lag_ms": self.worker_scheduling_lag.as_dict(),
                "watchdog": self.watchdog.as_dict(),
            },
        }
