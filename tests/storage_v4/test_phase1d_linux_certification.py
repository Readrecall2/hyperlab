from __future__ import annotations

import pytest

from hyperlab.paper.storage_v4.faults import FaultPoint
from hyperlab.paper.storage_v4.phase1d_linux_certification import (
    PHASE1D_CERTIFIED_VERDICT,
    PHASE1D_READY_VERDICT,
    build_crash_plan,
    build_phase1d_workload_manifest,
    decide_phase1d_verdict,
)


def test_phase1d_workload_is_bounded_representative_and_reproducible() -> None:
    first = build_phase1d_workload_manifest(commit_count=129)
    second = build_phase1d_workload_manifest(commit_count=129)

    assert first == second
    assert first.commit_count == 129
    assert first.config.market_gap_count >= 1
    assert first.logical_row_count > first.commit_count
    assert first.config.strategies == (
        "phase05_cash_and_carry",
        "phase08_robust_pairs",
    )


def test_crash_plan_covers_real_sigterm_and_sigkill_at_critical_boundaries() -> None:
    plan = build_crash_plan()

    assert {case.signal_name for case in plan} == {"SIGKILL", "SIGTERM"}
    assert {case.store_kind for case in plan} == {"paper", "raw"}
    assert {
        FaultPoint.AFTER_FILE_FSYNC,
        FaultPoint.AFTER_RENAME,
        FaultPoint.AFTER_DIRECTORY_FSYNC,
        FaultPoint.AFTER_SEGMENT_PUBLICATION,
        FaultPoint.AFTER_CHECKPOINT_PUBLICATION,
        FaultPoint.AFTER_MANIFEST_PUBLICATION,
        FaultPoint.AFTER_ANCHOR_PUBLICATION,
        FaultPoint.AFTER_CURRENT_PUBLICATION,
        FaultPoint.AFTER_RAW_SEGMENT_PUBLICATION,
        FaultPoint.AFTER_RAW_MANIFEST_PUBLICATION,
        FaultPoint.AFTER_RAW_ANCHOR_PUBLICATION,
    }.issubset({case.fault_point for case in plan})
    assert len({case.case_id for case in plan}) == len(plan)


def test_ext4_verdict_is_impossible_without_observed_ext4() -> None:
    assert decide_phase1d_verdict(filesystem_type="ext4", gates_passed=True) == (PHASE1D_CERTIFIED_VERDICT)
    assert decide_phase1d_verdict(filesystem_type="xfs", gates_passed=True) == (PHASE1D_READY_VERDICT)
    with pytest.raises(ValueError, match="gates"):
        decide_phase1d_verdict(filesystem_type="ext4", gates_passed=False)
