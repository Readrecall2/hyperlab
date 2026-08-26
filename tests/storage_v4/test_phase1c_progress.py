from __future__ import annotations

from hyperlab.paper.storage_v4.phase1c_progress import Phase1CHeartbeatWindow


def _progress(
    *,
    workload_id: str = "workload-a",
    elapsed_ns: int,
    commits_completed: int,
    logical_rows_completed: int,
) -> dict[str, object]:
    return {
        "workload": "SYNTHETIC_CAPACITY_V1",
        "workload_profile": "GOLDEN_SHAPED",
        "workload_id": workload_id,
        "commits_completed": commits_completed,
        "commits_total": 100,
        "logical_rows_completed": logical_rows_completed,
        "logical_rows_total": 200,
        "workload_elapsed_ns": elapsed_ns,
        "cpu_ns": elapsed_ns // 2,
        "cpu_status": "CURRENT_WORKER_PROCESS_CPU_SINCE_WORKLOAD_START",
        "peak_rss_bytes": 123,
        "peak_rss_status": "PROCESS_LIFETIME_HIGH_WATER_MARK",
        "bytes_written": commits_completed * 10,
        "bytes_written_status": "PROCESS_SCOPED_WRITE_TRANSFER_DELTA",
        "raw_segment_count": commits_completed // 10,
        "paper_segment_count": commits_completed // 10,
        "segment_count": 2 * (commits_completed // 10),
        "checkpoint_count": commits_completed // 10,
        "segment_checkpoint_status": "EXACT_DURABLE_PUBLICATION_COUNTS",
        "progress_metrics_scope": "CURRENT_WORKER_PROCESS_AT_COMPLETED_BOUNDARY",
    }


def test_first_sample_has_no_recent_rate_or_eta() -> None:
    window = Phase1CHeartbeatWindow()

    rendered = window.render(
        _progress(
            elapsed_ns=10_000_000_000,
            commits_completed=20,
            logical_rows_completed=40,
        ),
        observed_elapsed_ns=10_000_000_000,
    )

    assert rendered["recent_commits_per_second"] is None
    assert rendered["recent_logical_rows_per_second"] is None
    assert rendered["recent_throughput_status"] == (
        "UNAVAILABLE_INSUFFICIENT_HEARTBEAT_WINDOW"
    )
    assert rendered["conservative_eta_ns"] is None
    assert rendered["conservative_eta_status"] == (
        "UNAVAILABLE_INSUFFICIENT_HEARTBEAT_WINDOW"
    )


def test_second_positive_sample_uses_exact_recent_rate_and_conservative_eta() -> None:
    window = Phase1CHeartbeatWindow()
    window.render(
        _progress(
            elapsed_ns=10_000_000_000,
            commits_completed=20,
            logical_rows_completed=40,
        ),
        observed_elapsed_ns=10_000_000_000,
    )

    rendered = window.render(
        _progress(
            elapsed_ns=20_000_000_000,
            commits_completed=30,
            logical_rows_completed=60,
        ),
        observed_elapsed_ns=20_000_000_000,
    )

    assert rendered["recent_window_elapsed_ns"] == 10_000_000_000
    assert rendered["recent_commits_completed"] == 10
    assert rendered["recent_logical_rows_completed"] == 20
    assert rendered["recent_commits_per_second"] == "1"
    assert rendered["recent_logical_rows_per_second"] == "2"
    assert rendered["recent_throughput_status"] == "AVAILABLE_SAME_WORKLOAD_WINDOW"
    assert rendered["conservative_eta_ns"] == 70_000_000_000
    assert rendered["conservative_eta_status"] == (
        "AVAILABLE_MAX_OF_COMMIT_ROW_RECENT_AND_OVERALL_RATES"
    )


def test_no_progress_regression_missing_and_workload_change_are_fail_closed() -> None:
    window = Phase1CHeartbeatWindow()
    first = _progress(
        elapsed_ns=10_000_000_000,
        commits_completed=20,
        logical_rows_completed=40,
    )
    window.render(first, observed_elapsed_ns=10_000_000_000)

    stalled = window.render(first, observed_elapsed_ns=20_000_000_000)
    assert stalled["recent_commits_per_second"] is None
    assert stalled["conservative_eta_ns"] is None
    assert stalled["recent_throughput_status"] == (
        "UNAVAILABLE_NO_POSITIVE_RECENT_PROGRESS"
    )

    regressed = window.render(
        _progress(
            elapsed_ns=9_000_000_000,
            commits_completed=19,
            logical_rows_completed=39,
        ),
        observed_elapsed_ns=30_000_000_000,
    )
    assert regressed["recent_throughput_status"] == "UNAVAILABLE_COUNTER_REGRESSION"
    assert regressed["conservative_eta_ns"] is None

    missing = window.render(
        {"workload": "SYNTHETIC_CAPACITY_V1"},
        observed_elapsed_ns=40_000_000_000,
    )
    assert missing["recent_throughput_status"] == (
        "UNAVAILABLE_NO_ACTIVE_WORKLOAD_PROGRESS"
    )

    changed = window.render(
        _progress(
            workload_id="workload-b",
            elapsed_ns=5_000_000_000,
            commits_completed=10,
            logical_rows_completed=20,
        ),
        observed_elapsed_ns=50_000_000_000,
    )
    assert changed["recent_throughput_status"] == (
        "UNAVAILABLE_INSUFFICIENT_HEARTBEAT_WINDOW"
    )
    assert changed["conservative_eta_ns"] is None


def test_segment_counters_missing_bool_or_sum_mismatch_are_fail_closed() -> None:
    invalid_payloads: list[dict[str, object]] = []
    missing = _progress(
        elapsed_ns=20_000_000_000,
        commits_completed=30,
        logical_rows_completed=60,
    )
    missing.pop("raw_segment_count")
    invalid_payloads.append(missing)
    boolean = _progress(
        elapsed_ns=20_000_000_000,
        commits_completed=30,
        logical_rows_completed=60,
    )
    boolean["checkpoint_count"] = False
    invalid_payloads.append(boolean)
    incoherent = _progress(
        elapsed_ns=20_000_000_000,
        commits_completed=30,
        logical_rows_completed=60,
    )
    incoherent["segment_count"] = 5
    invalid_payloads.append(incoherent)

    unavailable = (
        "UNAVAILABLE_SEGMENT_OR_CHECKPOINT_COUNTERS_MISSING_INVALID_OR_INCOHERENT"
    )
    for invalid in invalid_payloads:
        window = Phase1CHeartbeatWindow()
        window.render(
            _progress(
                elapsed_ns=10_000_000_000,
                commits_completed=20,
                logical_rows_completed=40,
            ),
            observed_elapsed_ns=10_000_000_000,
        )
        rendered = window.render(invalid, observed_elapsed_ns=20_000_000_000)

        assert rendered["segment_checkpoint_status"] == unavailable
        assert rendered["recent_throughput_status"] == unavailable
        assert rendered["conservative_eta_status"] == unavailable
        assert rendered["recent_commits_per_second"] is None
        assert rendered["conservative_eta_ns"] is None


def test_segment_or_checkpoint_counter_regression_is_fail_closed() -> None:
    regressions = (
        {
            "raw_segment_count": 1,
            "paper_segment_count": 3,
            "segment_count": 4,
        },
        {"checkpoint_count": 1},
    )
    for overrides in regressions:
        window = Phase1CHeartbeatWindow()
        window.render(
            _progress(
                elapsed_ns=10_000_000_000,
                commits_completed=20,
                logical_rows_completed=40,
            ),
            observed_elapsed_ns=10_000_000_000,
        )
        regressed = _progress(
            elapsed_ns=20_000_000_000,
            commits_completed=30,
            logical_rows_completed=60,
        )
        regressed.update(overrides)
        rendered = window.render(regressed, observed_elapsed_ns=20_000_000_000)

        unavailable = "UNAVAILABLE_SEGMENT_OR_CHECKPOINT_COUNTER_REGRESSION"
        assert rendered["segment_checkpoint_status"] == unavailable
        assert rendered["recent_throughput_status"] == unavailable
        assert rendered["conservative_eta_status"] == unavailable
        assert rendered["recent_commits_per_second"] is None
        assert rendered["conservative_eta_ns"] is None


def test_workload_identity_change_starts_a_new_recent_window() -> None:
    window = Phase1CHeartbeatWindow()
    window.render(
        _progress(
            workload_id="workload-a",
            elapsed_ns=10_000_000_000,
            commits_completed=20,
            logical_rows_completed=40,
        ),
        observed_elapsed_ns=10_000_000_000,
    )

    changed = window.render(
        _progress(
            workload_id="workload-b",
            elapsed_ns=5_000_000_000,
            commits_completed=10,
            logical_rows_completed=20,
        ),
        observed_elapsed_ns=20_000_000_000,
    )
    assert changed["recent_throughput_status"] == (
        "UNAVAILABLE_INSUFFICIENT_HEARTBEAT_WINDOW"
    )

    continued = window.render(
        _progress(
            workload_id="workload-b",
            elapsed_ns=15_000_000_000,
            commits_completed=20,
            logical_rows_completed=40,
        ),
        observed_elapsed_ns=30_000_000_000,
    )
    assert continued["recent_throughput_status"] == "AVAILABLE_SAME_WORKLOAD_WINDOW"
    assert continued["recent_commits_completed"] == 10
    assert continued["recent_logical_rows_completed"] == 20


def test_complete_workload_reports_zero_eta() -> None:
    window = Phase1CHeartbeatWindow()
    window.render(
        _progress(
            elapsed_ns=10_000_000_000,
            commits_completed=90,
            logical_rows_completed=180,
        ),
        observed_elapsed_ns=10_000_000_000,
    )

    complete = window.render(
        _progress(
            elapsed_ns=20_000_000_000,
            commits_completed=100,
            logical_rows_completed=200,
        ),
        observed_elapsed_ns=20_000_000_000,
    )

    assert complete["conservative_eta_ns"] == 0
    assert complete["conservative_eta_status"] == "COMPLETE"


def test_first_complete_sample_reports_zero_eta_without_a_recent_window() -> None:
    rendered = Phase1CHeartbeatWindow().render(
        _progress(
            elapsed_ns=1,
            commits_completed=100,
            logical_rows_completed=200,
        ),
        observed_elapsed_ns=1,
    )

    assert rendered["recent_throughput_status"] == (
        "UNAVAILABLE_INSUFFICIENT_HEARTBEAT_WINDOW"
    )
    assert rendered["conservative_eta_ns"] == 0
    assert rendered["conservative_eta_status"] == "COMPLETE"


def test_eta_uses_the_slowest_commit_or_row_recent_and_overall_rate() -> None:
    window = Phase1CHeartbeatWindow()
    window.render(
        _progress(
            elapsed_ns=10_000_000_000,
            commits_completed=20,
            logical_rows_completed=40,
        ),
        observed_elapsed_ns=10_000_000_000,
    )

    rendered = window.render(
        _progress(
            elapsed_ns=20_000_000_000,
            commits_completed=30,
            logical_rows_completed=50,
        ),
        observed_elapsed_ns=20_000_000_000,
    )

    # Commits imply 70 s recent ETA; rows imply 150 s recent ETA.
    assert rendered["recent_commits_per_second"] == "1"
    assert rendered["recent_logical_rows_per_second"] == "1"
    assert rendered["conservative_eta_ns"] == 150_000_000_000
    assert rendered["conservative_eta_status"] == (
        "AVAILABLE_MAX_OF_COMMIT_ROW_RECENT_AND_OVERALL_RATES"
    )


def test_eta_is_unavailable_when_an_incomplete_dimension_has_no_recent_rate() -> None:
    window = Phase1CHeartbeatWindow()
    window.render(
        _progress(
            elapsed_ns=10_000_000_000,
            commits_completed=90,
            logical_rows_completed=100,
        ),
        observed_elapsed_ns=10_000_000_000,
    )

    rendered = window.render(
        _progress(
            elapsed_ns=20_000_000_000,
            commits_completed=100,
            logical_rows_completed=100,
        ),
        observed_elapsed_ns=20_000_000_000,
    )

    assert rendered["recent_commits_per_second"] == "1"
    assert rendered["recent_logical_rows_per_second"] == "0"
    assert rendered["conservative_eta_ns"] is None
    assert rendered["conservative_eta_status"] == (
        "UNAVAILABLE_INCOMPLETE_DIMENSION_HAS_NO_POSITIVE_RECENT_RATE"
    )
