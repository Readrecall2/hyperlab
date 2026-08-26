from __future__ import annotations

from collections.abc import Iterator

import pytest

import hyperlab.paper.storage_v4.phase1c_progress as audit_progress_module
from hyperlab.paper.storage_v4.phase1c_progress import (
    AUDIT_PROGRESS_AUTHORITY,
    AUDIT_PROGRESS_CONTRACT,
    BoundedAuditProgress,
)


def _clock(values: tuple[int, ...]) -> Iterator[int]:
    return iter(values)


def test_audit_progress_emits_started_spaced_heartbeat_and_forced_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    second = 1_000_000_000
    clock = _clock((100 * second, 110 * second, 130 * second, 159 * second, 160 * second))
    monkeypatch.setattr(audit_progress_module, "monotonic_ns", lambda: next(clock))
    monkeypatch.setattr(audit_progress_module, "time_ns", lambda: 1_800_000_000_000_000_000)
    events: list[dict[str, object]] = []

    progress = BoundedAuditProgress(
        phase="synthetic_full_audit",
        progress=lambda payload: events.append(dict(payload)),
        totals={"commits": 9, "segments": 3},
    )
    progress.advance({"commits": 3, "segments": 1})
    progress.advance({"commits": 6, "segments": 2})
    progress.advance({"commits": 9, "segments": 3})
    progress.complete({"commits": 9, "segments": 3})

    assert [event["audit_event"] for event in events] == [
        "STARTED",
        "HEARTBEAT",
        "COMPLETE",
    ]
    assert [event["heartbeat_sequence"] for event in events] == [0, 1, 2]
    assert [event["phase_elapsed_ns"] for event in events] == [
        0,
        30 * second,
        60 * second,
    ]
    assert [event["audited_segments"] for event in events] == [0, 2, 3]
    assert [event["audited_commits"] for event in events] == [0, 6, 9]
    assert all(event["segments_total"] == 3 for event in events)
    assert all(event["commits_total"] == 9 for event in events)
    assert all(
        event["phase_started_at_unix_ns"] == 1_800_000_000_000_000_000
        for event in events
    )
    assert all(event["audit_progress_contract"] == AUDIT_PROGRESS_CONTRACT for event in events)
    assert all(event["audit_progress_authority"] == AUDIT_PROGRESS_AUTHORITY for event in events)
    assert events[-1]["status"] == "COMPLETE"


def test_audit_progress_rejects_unbounded_heartbeat_intervals() -> None:
    with pytest.raises(ValueError, match="between 30 and 60 seconds"):
        BoundedAuditProgress(
            phase="synthetic_full_audit",
            progress=None,
            totals={},
            heartbeat_interval_seconds=29.0,
        )


def test_audit_progress_refuses_overshoot_and_incomplete_complete() -> None:
    overshoot = BoundedAuditProgress(
        phase="synthetic_full_audit",
        progress=lambda _payload: None,
        totals={"segments": 1},
    )
    with pytest.raises(ValueError, match="audited_segments exceeds segments_total"):
        overshoot.advance({"segments": 2})

    incomplete = BoundedAuditProgress(
        phase="synthetic_full_audit",
        progress=lambda _payload: None,
        totals={"segments": 2},
    )
    with pytest.raises(ValueError, match="COMPLETE requires exact totals: segments"):
        incomplete.complete({"segments": 1})

    disabled = BoundedAuditProgress(
        phase="synthetic_full_audit",
        progress=None,
        totals={"segments": 1},
    )
    disabled.advance({"segments": 2})
    disabled.complete({"segments": 0})


def test_audit_progress_does_not_mask_base_exception() -> None:
    class AuditProgressAbort(BaseException):
        pass

    def abort(_payload: object) -> None:
        raise AuditProgressAbort

    with pytest.raises(AuditProgressAbort):
        BoundedAuditProgress(
            phase="synthetic_full_audit",
            progress=abort,
            totals={"segments": 1},
        )
