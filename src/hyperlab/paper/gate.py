from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from hyperlab.paper.models import (
    PaperEventType,
    PaperRunConfig,
    PaperState,
    StoredPaperEvent,
    decimal_text,
    decimal_value,
    utc_text,
)
from hyperlab.paper.store import (
    ConcurrentWriteError,
    IntegrityError,
    IntegrityReport,
    PaperStore,
    RunNotFoundError,
    RunRecord,
)


class PaperGateStatus(StrEnum):
    PASS = "PASS"
    BLOCKED_OBSERVATION_WINDOW = "BLOCKED_OBSERVATION_WINDOW"
    BLOCKED_INSUFFICIENT_CYCLES = "BLOCKED_INSUFFICIENT_CYCLES"
    BLOCKED_CRITICAL_INCIDENT = "BLOCKED_CRITICAL_INCIDENT"
    BLOCKED_STRESSED_RESULT = "BLOCKED_STRESSED_RESULT"
    BLOCKED_RECONCILIATION = "BLOCKED_RECONCILIATION"
    BLOCKED_CONFIG_DRIFT = "BLOCKED_CONFIG_DRIFT"
    BLOCKED_UNCALIBRATED = "BLOCKED_UNCALIBRATED"
    BLOCKED_PRECONDITIONS = "BLOCKED_PRECONDITIONS"
    BLOCKED_RESILIENCE_EXERCISES = "BLOCKED_RESILIENCE_EXERCISES"


@dataclass(frozen=True, slots=True)
class PaperGateEvidence:
    """A deterministic evaluation instant; all substantive evidence is durable."""

    as_of: datetime

    def __post_init__(self) -> None:
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("paper gate as_of must be timezone-aware UTC")
        if self.as_of.utcoffset() != timedelta(0):
            raise ValueError("paper gate as_of must use UTC")
        object.__setattr__(self, "as_of", self.as_of.astimezone(UTC))


@dataclass(frozen=True, slots=True)
class PaperGateSnapshot:
    """The exact durable head and metrics used by one Gate D evaluation."""

    run_id: str
    config_hash: str
    event_sequence: int
    event_head_hash: str
    commit_sequence: int
    commit_head_hash: str
    projection_revision: int
    projection_hash: str
    completed_cycles: int
    critical_incident_count: int
    incident_free_days_completed: int
    minimum_cycles_required: int
    minimum_incident_free_days: int
    minimum_observation_days: int
    observation_days_completed: int
    operational_state: PaperState
    required_instruments: tuple[str, ...]
    run_kind: str
    validation_started_at: datetime

    def to_metrics_dict(self) -> dict[str, object]:
        return {
            "commit_head_hash": self.commit_head_hash,
            "commit_sequence": self.commit_sequence,
            "completed_cycles": self.completed_cycles,
            "critical_incident_count": self.critical_incident_count,
            "event_head_hash": self.event_head_hash,
            "event_sequence": self.event_sequence,
            "incident_free_days_completed": self.incident_free_days_completed,
            "minimum_cycles_required": self.minimum_cycles_required,
            "minimum_incident_free_days": self.minimum_incident_free_days,
            "minimum_observation_days": self.minimum_observation_days,
            "observation_days_completed": self.observation_days_completed,
            "operational_state": self.operational_state.value,
            "projection_hash": self.projection_hash,
            "projection_revision": self.projection_revision,
            "required_instruments": list(self.required_instruments),
            "run_kind": self.run_kind,
            "validation_started_at": utc_text(self.validation_started_at),
        }


@dataclass(frozen=True, slots=True)
class PaperGateResult:
    status: PaperGateStatus
    eligible: bool
    reasons: tuple[str, ...]
    checks: dict[str, bool]
    evaluated_at: datetime
    snapshot: PaperGateSnapshot

    def to_dict(self) -> dict[str, object]:
        return {
            "checks": dict(sorted(self.checks.items())),
            "config_hash": self.snapshot.config_hash,
            "eligible": self.eligible,
            "evaluated_at": utc_text(self.evaluated_at),
            "metrics": self.snapshot.to_metrics_dict(),
            "reasons": list(self.reasons),
            "run_id": self.snapshot.run_id,
            "status": self.status.value,
        }


_STATUS_PRIORITY = (
    PaperGateStatus.BLOCKED_PRECONDITIONS,
    PaperGateStatus.BLOCKED_UNCALIBRATED,
    PaperGateStatus.BLOCKED_CONFIG_DRIFT,
    PaperGateStatus.BLOCKED_RECONCILIATION,
    PaperGateStatus.BLOCKED_RESILIENCE_EXERCISES,
    PaperGateStatus.BLOCKED_OBSERVATION_WINDOW,
    PaperGateStatus.BLOCKED_INSUFFICIENT_CYCLES,
    PaperGateStatus.BLOCKED_CRITICAL_INCIDENT,
    PaperGateStatus.BLOCKED_STRESSED_RESULT,
)
_REQUIRED_EXERCISES = frozenset(
    {"RESTART", "DISCONNECT", "PARTIAL_FILL", "CRASH_RECOVERY"}
)


def _latest_persisted_evidence(
    events: tuple[StoredPaperEvent, ...],
    as_of: datetime,
    config: PaperRunConfig,
) -> tuple[Decimal | None, bool, set[str], bool]:
    stressed_pnl: Decimal | None = None
    stress_covers_final_economic_prefix = False
    exercises: set[str] = set()
    continuous_coverage = False
    latest_economic_sequence = 0
    latest_stress_sequence = 0
    economic_types = {
        PaperEventType.DECISION_RECORDED,
        PaperEventType.ORDER_PLANNED,
        PaperEventType.RISK_ACCEPTED,
        PaperEventType.RISK_REJECTED,
        PaperEventType.ORDER_ACKED,
        PaperEventType.ORDER_REJECTED,
        PaperEventType.CANCEL_REQUESTED,
        PaperEventType.ORDER_PARTIALLY_FILLED,
        PaperEventType.ORDER_FILLED,
        PaperEventType.ORDER_CANCELLED,
        PaperEventType.ORDER_EXPIRED,
        PaperEventType.ORDER_NO_FILL,
        PaperEventType.MARK_RECORDED,
        PaperEventType.FUNDING_POSTED,
        PaperEventType.CYCLE_COMPLETED,
    }
    for stored in events:
        event = stored.event
        if event.received_at > as_of:
            continue
        if event.event_type in economic_types:
            latest_economic_sequence = stored.sequence
        if event.event_type in {
            PaperEventType.STRESS_RESULT_RECORDED,
            PaperEventType.RESILIENCE_EXERCISE_RECORDED,
            PaperEventType.OBSERVATION_COVERAGE_RECORDED,
        } and str(event.payload.get("config_hash")) != config.config_hash:
            continue
        if event.event_type is PaperEventType.STRESS_RESULT_RECORDED:
            latest_stress_sequence = stored.sequence
            stressed_pnl = decimal_value(
                str(event.payload["stressed_net_pnl"]),
                label="persisted stressed_net_pnl",
            )
            stress_covers_final_economic_prefix = (
                int(str(event.payload.get("evaluated_event_sequence", -1)))
                == stored.sequence - 1
                and str(event.payload.get("evaluated_event_head_hash"))
                == stored.previous_event_hash
            )
        elif event.event_type is PaperEventType.RESILIENCE_EXERCISE_RECORDED:
            exercises.add(str(event.payload["exercise"]).upper())
        elif event.event_type is PaperEventType.OBSERVATION_COVERAGE_RECORDED:
            continuous_coverage = (
                bool(event.payload.get("continuous"))
                and str(event.payload.get("window_start"))
                == utc_text(config.validation_started_at)
                and datetime.fromisoformat(
                    str(event.payload["window_end"]).replace("Z", "+00:00")
                )
                >= as_of
            )
    stress_covers_final_economic_prefix = (
        stress_covers_final_economic_prefix
        and latest_stress_sequence > latest_economic_sequence
    )
    return (
        stressed_pnl,
        stress_covers_final_economic_prefix,
        exercises,
        continuous_coverage,
    )


def _require_stable_snapshot(
    *,
    before: RunRecord,
    report: IntegrityReport,
    run: RunRecord,
    after: RunRecord,
    projection_sequence: int,
    projection_event_hash: str | None,
    events: tuple[StoredPaperEvent, ...],
) -> None:
    """Reject a Gate D read if any authoritative durable anchor changed."""

    report_anchor = (
        report.event_count,
        report.event_head_hash,
        report.commit_count,
        report.commit_head_hash,
    )
    run_anchor = (
        run.event_sequence,
        run.event_head_hash,
        run.commit_sequence,
        run.commit_head_hash,
    )
    events_bound = (
        (run.event_sequence == 0 and not events)
        or (
            len(events) == run.event_sequence
            and events[-1].sequence == run.event_sequence
            and events[-1].event_hash == run.event_head_hash
        )
    )
    if (
        before != run
        or run != after
        or report_anchor != run_anchor
        or projection_sequence != run.event_sequence
        or projection_event_hash != run.event_head_hash
        or not events_bound
    ):
        raise ConcurrentWriteError(
            "paper Gate D durable head changed during read-only evaluation; "
            "evaluate only against a stable paper journal"
        )


def evaluate_paper_gate(
    store: PaperStore,
    run_id: str,
    evidence: PaperGateEvidence,
) -> PaperGateResult:
    """Evaluate Gate D from the authoritative journal; never create an executor."""

    if not isinstance(store, PaperStore):
        raise TypeError("paper gate requires the authoritative PaperStore")
    try:
        before = store.get_run(run_id)
        report = store.inspect_integrity_readonly(run_id)
        if not report.ok:
            raise IntegrityError(report)
        run = store.get_run(run_id)
        config = PaperRunConfig.from_dict(run.config_snapshot)
        projection = store.get_projection(run_id)
        events = store.get_events(run_id)
        after = store.get_run(run_id)
    except (IntegrityError, RunNotFoundError):
        raise
    _require_stable_snapshot(
        before=before,
        report=report,
        run=run,
        after=after,
        projection_sequence=projection.last_sequence,
        projection_event_hash=projection.last_event_hash,
        events=events,
    )
    if run.config_hash != config.config_hash or run.run_id != config.run_id:
        raise ValueError("paper gate durable run is not bound to its frozen configuration")
    if projection.run_id != run_id or projection.config_hash != config.config_hash:
        raise ValueError("paper gate projection is not bound to the durable run")
    if evidence.as_of < config.validation_started_at:
        raise ValueError("paper gate as_of precedes the validation window")
    if (
        projection.last_received_at is not None
        and evidence.as_of < projection.last_received_at
    ):
        raise ValueError("paper gate as_of precedes the durable paper state")

    elapsed = evidence.as_of - config.validation_started_at
    last_incident = max(projection.critical_incidents, default=None)
    incident_free_since = last_incident or config.validation_started_at
    incident_free_duration = evidence.as_of - incident_free_since
    stressed_pnl, stress_bound, exercises, continuous_coverage = _latest_persisted_evidence(
        events,
        evidence.as_of,
        config,
    )
    required_instruments = set(config.required_instruments)
    required_instruments.update(projection.positions)
    required_instruments.update(
        order.intent.instrument for order in projection.active_orders
    )
    fresh_channels = all(
        instrument in projection.last_market_received_at_by_instrument
        and timedelta(0)
        <= evidence.as_of
        - projection.last_market_received_at_by_instrument[instrument]
        <= timedelta(seconds=config.risk.stale_after_seconds)
        for instrument in required_instruments
    )
    fresh_at_gate = (
        projection.last_market_received_at is not None
        and timedelta(0)
        <= evidence.as_of - projection.last_market_received_at
        <= timedelta(seconds=config.risk.stale_after_seconds)
        and fresh_channels
    )
    checks = {
        "approved_admission": False,
        "calibrated_models": config.economically_eligible,
        "config_frozen": report.ok and run.config_hash == config.config_hash,
        "continuous_observation": continuous_coverage and fresh_at_gate,
        "durable_runtime_source_attestation": False,
        "economic_prerequisites": config.economic_prerequisites_satisfied,
        "gate_d_artifact_bytes_verified": False,
        "incident_free_14_days": incident_free_duration >= timedelta(days=14),
        "minimum_42_days": elapsed >= timedelta(days=42),
        "minimum_cycles": projection.completed_cycles >= config.minimum_validation_cycles,
        "operational_state": projection.state in {PaperState.FLAT, PaperState.HEDGED},
        "positive_stressed_net_pnl": (
            stressed_pnl is not None and stressed_pnl > 0 and stress_bound
        ),
        "reconciled": projection.reconciled and report.ok,
        "resilience_exercises": exercises >= _REQUIRED_EXERCISES,
        "validation_run": config.run_kind == "VALIDATION",
    }
    reasons: list[str] = []
    statuses: set[PaperGateStatus] = set()

    def block(check: str, status: PaperGateStatus, message: str) -> None:
        if not checks[check]:
            statuses.add(status)
            reasons.append(message)

    block(
        "operational_state",
        PaperGateStatus.BLOCKED_PRECONDITIONS,
        "paper run remains in a pending or protective operational state",
    )
    block(
        "economic_prerequisites",
        PaperGateStatus.BLOCKED_PRECONDITIONS,
        "strategy economic prerequisites are not satisfied",
    )
    block(
        "validation_run",
        PaperGateStatus.BLOCKED_PRECONDITIONS,
        "synthetic/demo runs are never economically promotable",
    )
    block(
        "continuous_observation",
        PaperGateStatus.BLOCKED_PRECONDITIONS,
        "continuous public-data coverage and a fresh terminal observation are required",
    )
    block(
        "calibrated_models",
        PaperGateStatus.BLOCKED_UNCALIBRATED,
        "data, point-in-time costs, latency, and fill models require calibration evidence",
    )
    block(
        "config_frozen",
        PaperGateStatus.BLOCKED_CONFIG_DRIFT,
        "the durable frozen strategy/configuration failed its integrity binding",
    )
    block(
        "reconciled",
        PaperGateStatus.BLOCKED_RECONCILIATION,
        "paper events, ledger, and projection do not reconcile exactly",
    )
    block(
        "resilience_exercises",
        PaperGateStatus.BLOCKED_RESILIENCE_EXERCISES,
        "persisted restart, crash recovery, disconnect, and partial-fill exercises are required",
    )
    block(
        "minimum_42_days",
        PaperGateStatus.BLOCKED_OBSERVATION_WINDOW,
        f"only {elapsed.days} of the minimum 42 observation days are complete",
    )
    block(
        "minimum_cycles",
        PaperGateStatus.BLOCKED_INSUFFICIENT_CYCLES,
        f"only {projection.completed_cycles} of {config.minimum_validation_cycles} frozen cycles are complete",
    )
    block(
        "incident_free_14_days",
        PaperGateStatus.BLOCKED_CRITICAL_INCIDENT,
        f"only {incident_free_duration.days} of 14 incident-free days are complete",
    )
    block(
        "positive_stressed_net_pnl",
        PaperGateStatus.BLOCKED_STRESSED_RESULT,
        (
            "no durable final-prefix-bound stressed result is available"
            if stressed_pnl is None
            else (
                "stressed result is not bound after the final economic event"
                if not stress_bound
                else f"stressed net PnL must be positive; observed {decimal_text(stressed_pnl)}"
            )
        ),
    )
    production_blockers = (
        (
            "approved_admission",
            "no durable production admission receipt is bound to this run",
        ),
        (
            "durable_runtime_source_attestation",
            (
                "the journal does not prove creation and input by the compiled "
                "approved runtime/source"
            ),
        ),
        (
            "gate_d_artifact_bytes_verified",
            (
                "stress, resilience, and coverage artifact bytes are not durably "
                "bound and reverified"
            ),
        ),
    )
    for check, message in production_blockers:
        if not checks[check]:
            reasons.append(message)
    status = next((candidate for candidate in _STATUS_PRIORITY if candidate in statuses), None)
    if status is None:
        status = (
            PaperGateStatus.BLOCKED_PRECONDITIONS
            if any(not checks[name] for name, _ in production_blockers)
            else PaperGateStatus.PASS
        )
    snapshot = PaperGateSnapshot(
        run_id=run.run_id,
        config_hash=config.config_hash,
        event_sequence=run.event_sequence,
        event_head_hash=run.event_head_hash,
        commit_sequence=run.commit_sequence,
        commit_head_hash=run.commit_head_hash,
        projection_revision=run.projection_revision,
        projection_hash=run.projection_hash,
        completed_cycles=projection.completed_cycles,
        critical_incident_count=len(projection.critical_incidents),
        incident_free_days_completed=max(incident_free_duration.days, 0),
        minimum_cycles_required=config.minimum_validation_cycles,
        minimum_incident_free_days=14,
        minimum_observation_days=42,
        observation_days_completed=max(elapsed.days, 0),
        operational_state=projection.state,
        required_instruments=tuple(sorted(required_instruments)),
        run_kind=config.run_kind,
        validation_started_at=config.validation_started_at,
    )
    return PaperGateResult(
        status=status,
        eligible=status is PaperGateStatus.PASS,
        reasons=tuple(reasons),
        checks=checks,
        evaluated_at=evidence.as_of,
        snapshot=snapshot,
    )


__all__ = [
    "PaperGateEvidence",
    "PaperGateResult",
    "PaperGateSnapshot",
    "PaperGateStatus",
    "evaluate_paper_gate",
]
