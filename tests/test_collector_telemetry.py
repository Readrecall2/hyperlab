from __future__ import annotations

import json

import pytest

import hyperlab.collector.telemetry as telemetry_module
from hyperlab.collector.telemetry import (
    MonotonicTimingSummary,
    ProcessRuntimeTelemetry,
    SchedulingWatchdog,
)


class ManualCounter:
    def __init__(self, value: int = 0) -> None:
        self.value = value

    def __call__(self) -> int:
        return self.value


def test_monotonic_timing_summary_is_bounded_but_retains_lifetime_extrema() -> None:
    summary = MonotonicTimingSummary(window_capacity=3)

    for duration_ms in (1, 2, 3, 4):
        summary.observe_ns(duration_ms * 1_000_000)

    payload = summary.as_dict()
    assert payload == {
        "count": 4,
        "window_count": 3,
        "window_capacity": 3,
        "window_truncated": True,
        "min_ms": 1.0,
        "mean_ms": 2.5,
        "p50_ms": 3.0,
        "p95_ms": pytest.approx(3.9),
        "p99_ms": pytest.approx(3.98),
        "max_ms": 4.0,
    }


def test_monotonic_timing_summary_rejects_invalid_durations() -> None:
    summary = MonotonicTimingSummary()

    with pytest.raises(ValueError, match="non-negative integer"):
        summary.observe_ns(-1)
    with pytest.raises(ValueError, match="finite and non-negative"):
        summary.observe_seconds(float("inf"))
    with pytest.raises(ValueError, match="end precedes start"):
        summary.observe_since(2, ended_monotonic_ns=1)


def test_process_runtime_snapshot_separates_cpu_and_scheduling_lag() -> None:
    monotonic = ManualCounter()
    process_cpu = ManualCounter()
    telemetry = ProcessRuntimeTelemetry(
        monotonic_ns=monotonic,
        process_time_ns=process_cpu,
        auto_start=False,
        window_capacity=4,
    )
    telemetry._observe_gc("start", {"generation": 1})
    monotonic.value = 40_000_000
    telemetry._observe_gc("stop", {"generation": 1})
    monotonic.value = 1_000_000_000
    process_cpu.value = 250_000_000
    telemetry.record_worker_scheduling_lag(
        900_000_000,
        observed_monotonic_ns=1_000_000_000,
    )
    telemetry.watchdog.lag.observe_ns(25_000_000)

    snapshot = telemetry.snapshot()
    telemetry.close()

    process = snapshot["process_cpu"]
    assert isinstance(process, dict)
    assert process["sample_wall_ms"] == 1_000.0
    assert process["core_ratio"] == 0.25
    assert process["core_percent"] == 25.0
    scheduling = snapshot["scheduling"]
    assert isinstance(scheduling, dict)
    worker = scheduling["worker_lag_ms"]
    assert isinstance(worker, dict)
    assert worker["p99_ms"] == 100.0
    watchdog = scheduling["watchdog"]
    assert isinstance(watchdog, dict)
    assert watchdog["started"] is False
    watchdog_lag = watchdog["lag_ms"]
    assert isinstance(watchdog_lag, dict)
    assert watchdog_lag["max_ms"] == 25.0
    gc_payload = snapshot["gc"]
    assert isinstance(gc_payload, dict)
    assert gc_payload["pause_ms"]["max_ms"] == 40.0
    assert isinstance(snapshot["memory"], dict)
    assert isinstance(snapshot["context_switches"], dict)
    assert isinstance(snapshot["linux_scheduler"], dict)
    json.dumps(snapshot, allow_nan=False)


@pytest.mark.parametrize("interval", [0.0, -1.0, float("inf")])
def test_watchdog_interval_must_be_finite_and_positive(interval: float) -> None:
    with pytest.raises(ValueError, match="finite and positive"):
        SchedulingWatchdog(interval_seconds=interval)


def test_linux_scheduler_snapshot_parses_cgroup_psi_and_runqueue_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = {
        "/proc/self/schedstat": "1000000 250000 7\n",
        "/sys/fs/cgroup/cpu.stat": ("usage_usec 5000\nnr_periods 20\nnr_throttled 3\nthrottled_usec 900\n"),
        "/sys/fs/cgroup/cpu.max": "200000 100000\n",
        "/proc/pressure/cpu": (
            "some avg10=1.25 avg60=0.50 avg300=0.10 total=12345\n"
            "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
        ),
        "/proc/pressure/memory": None,
        "/proc/pressure/io": None,
        "/proc/stat": "cpu  1 2 3 4 5 6 7 8 9 10\n",
    }
    monkeypatch.setattr(telemetry_module.sys, "platform", "linux")
    monkeypatch.setattr(telemetry_module, "_read_linux_text", payloads.get)

    snapshot = telemetry_module._linux_scheduler_snapshot()

    assert snapshot["available"] is True
    assert snapshot["proc_self_schedstat"]["runqueue_wait_ns"] == 250_000
    assert snapshot["cgroup_v2_cpu_stat"]["nr_throttled"] == 3
    assert snapshot["cgroup_v2_cpu_max"]["quota_cores"] == 2.0
    assert snapshot["pressure"]["cpu"]["some"]["avg10"] == 1.25
    assert snapshot["pressure"]["memory"] is None
    assert snapshot["host_cpu_ticks"]["iowait_ticks"] == 5
    assert snapshot["host_cpu_ticks"]["steal_ticks"] == 8
