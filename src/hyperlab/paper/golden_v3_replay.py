from __future__ import annotations

import gc
import hashlib
import os
import sqlite3
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from itertools import zip_longest
from pathlib import Path
from tempfile import TemporaryDirectory, mkdtemp
from time import perf_counter
from typing import cast

from hyperlab.backtest.protocol import canonical_json, canonical_sha256
from hyperlab.paper.engine import PaperCommandResult, PaperEngine
from hyperlab.paper.golden_v3 import (
    GOLDEN_STREAM_NAMES,
    GoldenDifferentialError,
    GoldenVerification,
    GoldenVerificationError,
    _fsync_directory,
    _has_reparse_component,
    _mkdir_durable,
    golden_replay_semantic_row,
    iter_golden_stream,
    iter_sqlite_logical_stream,
    verify_golden_v3,
)
from hyperlab.paper.models import (
    DecisionIntent,
    MarketEvent,
    PaperEvent,
    PaperRunConfig,
    StoredPaperEvent,
    deterministic_id,
    parse_utc,
    utc_text,
)
from hyperlab.paper.store import IntegrityError, PaperStore

ProgressCallback = Callable[[Mapping[str, object]], None]
_PROGRESS_ROW_INTERVAL = 1_000
_COPY_CHUNK_BYTES = 1024 * 1024
_COPY_PROGRESS_BYTES = 256 * 1024**2


class GoldenReplayError(GoldenVerificationError):
    """Raised when a verified Golden corpus cannot be replayed exactly."""


class GoldenReplayMismatchError(GoldenReplayError):
    """Raised only when reconstructed state diverges from valid Golden inputs."""


@dataclass(frozen=True, slots=True)
class _CanonicalInput:
    run_id: str
    input_id: str
    payload: dict[str, object]
    payload_hash: str
    first_event_sequence: int | None
    last_event_sequence: int
    commit_sequence: int
    commit_hash: str


@dataclass(slots=True)
class _LegacyObservationState:
    events: Iterator[StoredPaperEvent]
    current: StoredPaperEvent | None = None


def _emit(progress: ProgressCallback | None, **payload: object) -> None:
    if progress is None:
        return
    progress(
        {
            "mode": "PAPER_ONLY",
            "orders_enabled": False,
            **payload,
        }
    )


def _mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise GoldenReplayError(f"{label} must be an object")
    if any(not isinstance(key, str) for key in value):
        raise GoldenReplayError(f"{label} contains a non-string key")
    return cast(Mapping[str, object], value)


def _text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise GoldenReplayError(f"{label} must be a non-empty string")
    return value


def _integer(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise GoldenReplayError(f"{label} must be an integer >= {minimum}")
    return value


def _optional_integer(value: object, *, label: str, minimum: int = 0) -> int | None:
    if value is None:
        return None
    return _integer(value, label=label, minimum=minimum)


def _canonical_input(row: Mapping[str, object], *, expected_run_id: str) -> _CanonicalInput:
    run_id = _text(row.get("run_id"), label="inbox.run_id")
    if run_id != expected_run_id:
        raise GoldenReplayError("Golden inbox row belongs to a different run")
    payload = dict(_mapping(row.get("payload"), label="inbox.payload"))
    payload_hash = _text(row.get("payload_hash"), label="inbox.payload_hash")
    if canonical_sha256(payload) != payload_hash:
        raise GoldenReplayError("Golden inbox payload differs from its canonical hash")
    return _CanonicalInput(
        run_id=run_id,
        input_id=_text(row.get("input_id"), label="inbox.input_id"),
        payload=payload,
        payload_hash=payload_hash,
        first_event_sequence=_optional_integer(
            row.get("first_event_sequence"),
            label="inbox.first_event_sequence",
            minimum=1,
        ),
        last_event_sequence=_integer(
            row.get("last_event_sequence"),
            label="inbox.last_event_sequence",
        ),
        commit_sequence=_integer(
            row.get("commit_sequence"),
            label="inbox.commit_sequence",
            minimum=1,
        ),
        commit_hash=_text(row.get("commit_hash"), label="inbox.commit_hash"),
    )


def _load_run_contract(
    export_root: Path,
    manifest: Mapping[str, object],
    *,
    verification: GoldenVerification,
) -> tuple[str, PaperRunConfig, _CanonicalInput, Iterator[_CanonicalInput]]:
    run_rows = iter(
        iter_golden_stream(export_root, "run", verification=verification)
    )
    try:
        run_row = _mapping(next(run_rows), label="run stream row")
    except StopIteration as error:
        raise GoldenReplayError("Golden run stream is empty") from error
    try:
        next(run_rows)
    except StopIteration:
        pass
    else:
        raise GoldenReplayError("Golden run stream must contain exactly one row")

    run_id = _text(run_row.get("run_id"), label="run.run_id")
    config_payload = _mapping(run_row.get("config"), label="run.config")
    try:
        config = PaperRunConfig.from_dict(config_payload)
    except (KeyError, TypeError, ValueError) as error:
        raise GoldenReplayError("Golden run config cannot be reconstructed") from error
    if config.run_id != run_id:
        raise GoldenReplayError("Golden run_id differs from its canonical config identity")
    config_hash = _text(run_row.get("config_hash"), label="run.config_hash")
    if config.config_hash != config_hash:
        raise GoldenReplayError("Golden config hash differs from its canonical config")
    manifest_run_id = manifest.get("run_id")
    if manifest_run_id is not None and manifest_run_id != run_id:
        raise GoldenReplayError("Golden manifest run_id differs from its run stream")
    manifest_config_hash = manifest.get("config_hash")
    if manifest_config_hash is not None and manifest_config_hash != config_hash:
        raise GoldenReplayError("Golden manifest config hash differs from its run stream")

    input_rows = iter(
        iter_golden_stream(export_root, "inbox", verification=verification)
    )
    try:
        first = _canonical_input(
            _mapping(next(input_rows), label="inbox stream row"),
            expected_run_id=run_id,
        )
    except StopIteration as error:
        raise GoldenReplayError("Golden inbox is empty") from error
    expected_payload = {
        "config_hash": config.config_hash,
        "input_type": "RUN_START",
        "run_id": run_id,
    }
    expected_input_id = deterministic_id("paper_input_run_started", run_id)
    if (
        first.commit_sequence != 1
        or first.input_id != expected_input_id
        or first.payload != expected_payload
        or first.payload_hash != canonical_sha256(expected_payload)
    ):
        raise GoldenReplayError("Golden inbox must begin with the exact canonical RUN_START")

    def remaining() -> Iterator[_CanonicalInput]:
        expected_commit_sequence = 2
        for raw_row in input_rows:
            record = _canonical_input(
                _mapping(raw_row, label="inbox stream row"),
                expected_run_id=run_id,
            )
            if record.commit_sequence != expected_commit_sequence:
                raise GoldenReplayError(
                    "Golden inbox commit sequence is not contiguous at "
                    f"{expected_commit_sequence}"
                )
            if record.payload.get("input_type") == "RUN_START":
                raise GoldenReplayError("Golden inbox contains RUN_START after commit 1")
            expected_commit_sequence += 1
            yield record

    return run_id, config, first, remaining()


def _iter_legacy_events(
    export_root: Path,
    *,
    run_id: str,
    verification: GoldenVerification,
) -> Iterator[StoredPaperEvent]:
    for raw_row in iter_golden_stream(
        export_root,
        "events",
        verification=verification,
    ):
        row = _mapping(raw_row, label="events stream row")
        if _text(row.get("run_id"), label="event.run_id") != run_id:
            raise GoldenReplayError("Golden event belongs to a different run")
        raw_payload = row.get("payload", row.get("event"))
        payload = _mapping(raw_payload, label="event.payload")
        previous_hash = row.get("previous_hash", row.get("previous_event_hash"))
        if previous_hash is not None and not isinstance(previous_hash, str):
            raise GoldenReplayError("event.previous_hash must be a string or null")
        try:
            yield StoredPaperEvent(
                event=PaperEvent.from_dict(payload),
                sequence=_integer(row.get("sequence"), label="event.sequence", minimum=1),
                previous_event_hash=previous_hash,
                event_hash=_text(row.get("event_hash"), label="event.event_hash"),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise GoldenReplayError("Golden event cannot support legacy replay") from error


def _dispatch_input(
    replay_engine: PaperEngine,
    record: _CanonicalInput,
    *,
    legacy: _LegacyObservationState,
) -> PaperCommandResult:
    payload = record.payload
    input_type = _text(payload.get("input_type"), label="inbox.payload.input_type")
    if input_type == "RUN_START":
        raise GoldenReplayError("RUN_START may appear only at commit 1")
    if input_type == "RUNTIME_SESSION_STARTED":
        raw_replacement = payload.get("replaces_unclosed_session_id")
        return replay_engine.start_runtime_session(
            as_of=parse_utc(str(payload["started_at"])),
            session_id=str(payload["session_id"]),
            generation=int(str(payload["generation"])),
            replaces_unclosed_session_id=(
                str(raw_replacement) if raw_replacement is not None else None
            ),
        )
    if input_type == "RUNTIME_SESSION_STOPPED":
        return replay_engine.stop_runtime_session(
            as_of=parse_utc(str(payload["stopped_at"])),
            session_id=str(payload["session_id"]),
            generation=int(str(payload["generation"])),
            reason=str(payload["reason"]),
        )
    if input_type == "PUBLIC_MARKET_EVENT":
        raw_market = payload.get("market")
        if not isinstance(raw_market, Mapping):
            raise GoldenReplayError("replay market input lacks market payload")
        market = MarketEvent.from_dict(raw_market)
        return replay_engine.process_market(
            market,
            processed_at=parse_utc(
                str(payload.get("processed_at", utc_text(market.received_at)))
            ),
            execution_policy=str(payload.get("execution_policy", "EXECUTE")),
            _cash_math_version=int(str(payload.get("cash_math_version", 1))),
        )
    if input_type == "STRATEGY_DECISION":
        raw_decision = payload.get("decision")
        raw_markets = payload.get("markets")
        if not isinstance(raw_decision, Mapping) or not isinstance(raw_markets, Sequence):
            raise GoldenReplayError("replay decision input is incomplete")
        decision = DecisionIntent.from_dict(raw_decision)
        markets = {
            market.instrument: market
            for item in raw_markets
            if isinstance(item, Mapping)
            for market in (MarketEvent.from_dict(item),)
        }
        return replay_engine.submit_decision(
            decision,
            markets,
            processed_at=parse_utc(
                str(payload.get("processed_at", utc_text(decision.decided_at)))
            ),
        )
    if input_type == "CANCEL_REQUEST":
        return replay_engine.request_cancel(
            str(payload["order_id"]),
            requested_at=datetime.fromisoformat(
                str(payload["requested_at"]).replace("Z", "+00:00")
            ),
            input_id=record.input_id,
        )
    if input_type == "PUBLIC_FUNDING_SETTLEMENT":
        return replay_engine.post_funding(
            instrument=str(payload["instrument"]),
            amount=Decimal(str(payload["amount"])),
            occurred_at=datetime.fromisoformat(
                str(payload["occurred_at"]).replace("Z", "+00:00")
            ),
            source_event_id=str(payload["source_event_id"]),
            funding_rate=(
                Decimal(str(payload["funding_rate"]))
                if payload.get("funding_rate") is not None
                else None
            ),
            funding_interval_seconds=(
                int(str(payload["funding_interval_seconds"]))
                if payload.get("funding_interval_seconds") is not None
                else None
            ),
            rate_kind=(
                str(payload["rate_kind"])
                if payload.get("rate_kind") is not None
                else None
            ),
            mark_price=(
                Decimal(str(payload["mark_price"]))
                if payload.get("mark_price") is not None
                else None
            ),
            source_mark_price=(
                Decimal(str(payload["source_mark_price"]))
                if payload.get("source_mark_price") is not None
                else None
            ),
            oracle_price=(
                Decimal(str(payload["oracle_price"]))
                if payload.get("oracle_price") is not None
                else None
            ),
            position_quantity=(
                Decimal(str(payload["position_quantity"]))
                if payload.get("position_quantity") is not None
                else None
            ),
            mark_source=(
                str(payload["mark_source"])
                if payload.get("mark_source") is not None
                else None
            ),
            source_observation_id=(
                str(payload["source_observation_id"])
                if payload.get("source_observation_id") is not None
                else None
            ),
            received_at=(
                datetime.fromisoformat(
                    str(payload["received_at"]).replace("Z", "+00:00")
                )
                if payload.get("received_at") is not None
                else None
            ),
            processed_at=parse_utc(
                str(
                    payload.get(
                        "processed_at",
                        payload.get("received_at", payload["occurred_at"]),
                    )
                )
            ),
            applicability=str(payload.get("applicability", "APPLIED")),
            source_activation_cutoff=(
                datetime.fromisoformat(
                    str(payload["source_activation_cutoff"]).replace("Z", "+00:00")
                )
                if payload.get("source_activation_cutoff") is not None
                else None
            ),
            _cash_math_version=int(str(payload.get("cash_math_version", 1))),
        )
    if input_type == "TIMER":
        return replay_engine.process_timer(
            as_of=datetime.fromisoformat(str(payload["as_of"]).replace("Z", "+00:00"))
        )
    if input_type == "RECONCILE":
        reconcile_at = datetime.fromisoformat(
            str(payload["as_of"]).replace("Z", "+00:00")
        )
        return replay_engine._reconcile(
            as_of=reconcile_at,
            verification=replay_engine._verified_historical_replay_prefix(),
        )
    if input_type == "STRESS_RESULT":
        return replay_engine.record_stress_result(
            artifact_hash=str(payload["artifact_hash"]),
            stressed_net_pnl=Decimal(str(payload["stressed_net_pnl"])),
            evaluated_at=datetime.fromisoformat(
                str(payload["evaluated_at"]).replace("Z", "+00:00")
            ),
        )
    if input_type == "RESILIENCE_EXERCISE":
        return replay_engine.record_resilience_exercise(
            exercise=str(payload["exercise"]),
            artifact_hash=str(payload["artifact_hash"]),
            exercised_at=datetime.fromisoformat(
                str(payload["exercised_at"]).replace("Z", "+00:00")
            ),
        )
    if input_type == "OBSERVATION_COVERAGE":
        raw_recorded_at = payload.get("recorded_at")
        if isinstance(raw_recorded_at, str):
            recorded_at = parse_utc(raw_recorded_at)
        elif replay_engine.config.schema_version == 1:
            target_sequence = record.first_event_sequence
            if target_sequence is None:
                raise GoldenReplayError(
                    "legacy observation coverage input has no durable event"
                )
            while legacy.current is None or legacy.current.sequence < target_sequence:
                try:
                    legacy.current = next(legacy.events)
                except StopIteration as error:
                    raise GoldenReplayError(
                        "legacy observation coverage event is missing"
                    ) from error
            if (
                legacy.current.sequence != target_sequence
                or legacy.current.event.correlation_id != record.input_id
            ):
                raise GoldenReplayError(
                    "legacy observation coverage event differs from its input"
                )
            recorded_at = legacy.current.event.received_at
        else:
            raise GoldenReplayError("schema-v2 observation coverage lacks recorded_at")
        return replay_engine.record_observation_coverage(
            artifact_hash=str(payload["artifact_hash"]),
            window_start=datetime.fromisoformat(
                str(payload["window_start"]).replace("Z", "+00:00")
            ),
            window_end=datetime.fromisoformat(
                str(payload["window_end"]).replace("Z", "+00:00")
            ),
            continuous=bool(payload["continuous"]),
            recorded_at=recorded_at,
        )
    if input_type in {
        "OPERATOR_PAUSE",
        "PUBLIC_SOURCE_FAILURE",
        "PAPER_RUNTIME_FAILURE",
    }:
        return replay_engine.pause(
            as_of=datetime.fromisoformat(str(payload["as_of"]).replace("Z", "+00:00")),
            reason=str(payload["reason"]),
            operator_artifact_hash=str(payload["operator_artifact_hash"]),
            origin={
                "OPERATOR_PAUSE": "OPERATOR",
                "PUBLIC_SOURCE_FAILURE": "PUBLIC_SOURCE_FAILURE",
                "PAPER_RUNTIME_FAILURE": "PAPER_RUNTIME_FAILURE",
            }[input_type],
        )
    if input_type == "STRATEGY_LOCAL_FAILURE":
        raw_market_event_ids = payload.get("market_event_ids")
        if not isinstance(raw_market_event_ids, Sequence) or isinstance(
            raw_market_event_ids,
            (str, bytes),
        ):
            raise GoldenReplayError(
                "strategy-local failure replay requires market_event_ids"
            )
        return replay_engine.record_strategy_failure(
            strategy_id=str(payload["strategy_id"]),
            as_of=parse_utc(str(payload["as_of"])),
            phase=str(payload["phase"]),
            error_type=str(payload["error_type"]),
            market_event_ids=tuple(str(item) for item in raw_market_event_ids),
        )
    if input_type == "PAPER_KILL":
        return replay_engine.kill(
            as_of=datetime.fromisoformat(str(payload["as_of"]).replace("Z", "+00:00")),
            reason=str(payload["reason"]),
            operator_artifact_hash=str(payload["operator_artifact_hash"]),
        )
    if input_type == "RESUME_AFTER_REVIEW":
        return replay_engine.resume_from_pause(
            as_of=datetime.fromisoformat(str(payload["as_of"]).replace("Z", "+00:00")),
            review_artifact_hash=str(payload["review_artifact_hash"]),
            reviewed_critical_incident_count=cast(
                int,
                payload["reviewed_critical_incident_count"],
            ),
            reviewed_last_critical_incident_at=(
                parse_utc(cast(str, payload["reviewed_last_critical_incident_at"]))
                if isinstance(payload.get("reviewed_last_critical_incident_at"), str)
                else None
            ),
            recovery_mode=str(payload["recovery_mode"]),
        )
    raise GoldenReplayError(f"unsupported durable replay input type {input_type!r}")


def _row_identity(stream_name: str, row: Mapping[str, object]) -> str:
    for key in (
        "commit_sequence",
        "revision",
        "sequence",
        "input_id",
        "transaction_id",
        "entry_id",
        "alert_id",
        "run_id",
    ):
        if key in row:
            return f"{key}={row[key]!r}"
    return f"sha256={canonical_sha256(row)}"


def _compare_streams_connection(
    export_root: Path,
    connection: sqlite3.Connection,
    run_id: str,
    *,
    verification: GoldenVerification,
    progress: ProgressCallback | None,
    target_path: Path,
    progress_phase: str = "differential",
    progress_complete_phase: str = "differential_stream_complete",
    validation_step: str | None = None,
) -> dict[str, object]:
    manifest_streams = _mapping(
        verification.manifest.get("streams"),
        label="Golden manifest.streams",
    )
    validation_fields = (
        {"validation_step": validation_step}
        if validation_step is not None
        else {}
    )

    def differential_progress_fields(
        completed: int,
        *,
        elapsed_seconds: float,
        total_expected: int,
    ) -> dict[str, object]:
        remaining = max(total_expected - completed, 0)
        eta_seconds = (
            elapsed_seconds * remaining / completed
            if completed > 0 and remaining > 0
            else 0.0
        )
        return {
            "elapsed_seconds": elapsed_seconds,
            "eta_seconds": eta_seconds,
            "rows_completed": completed,
            "target_path": str(target_path),
            "target_store_bytes": (
                target_path.stat().st_size if target_path.is_file() else 0
            ),
            "total_expected": total_expected,
        }

    stream_results: dict[str, dict[str, object]] = {}
    total_rows = 0
    sentinel = object()
    for stream_name in GOLDEN_STREAM_NAMES:
        stream_started = perf_counter()
        stream_manifest = _mapping(
            manifest_streams.get(stream_name),
            label=f"Golden manifest.streams.{stream_name}",
        )
        total_expected = _integer(
            stream_manifest.get("row_count"),
            label=f"Golden manifest.streams.{stream_name}.row_count",
        )

        expected_rows = (
            golden_replay_semantic_row(stream_name, row)
            for row in iter_golden_stream(
                export_root,
                stream_name,
                verification=verification,
            )
        )
        actual_rows = (
            golden_replay_semantic_row(stream_name, row)
            for row in iter_sqlite_logical_stream(
                connection,
                run_id,
                stream_name,
            )
        )
        rows_compared = 0
        for ordinal, (expected, actual) in enumerate(
            zip_longest(expected_rows, actual_rows, fillvalue=sentinel),
            start=1,
        ):
            if expected is sentinel:
                actual_row = cast(Mapping[str, object], actual)
                raise GoldenDifferentialError(
                    f"replay stream {stream_name!r} has unexpected target row "
                    f"{ordinal} ({_row_identity(stream_name, actual_row)})"
                )
            if actual is sentinel:
                expected_row = cast(Mapping[str, object], expected)
                raise GoldenDifferentialError(
                    f"replay stream {stream_name!r} is missing target row "
                    f"{ordinal} ({_row_identity(stream_name, expected_row)})"
                )
            expected_row = cast(Mapping[str, object], expected)
            actual_row = cast(Mapping[str, object], actual)
            if canonical_json(expected_row) != canonical_json(actual_row):
                raise GoldenDifferentialError(
                    f"replay stream {stream_name!r} differs at row {ordinal} "
                    f"({_row_identity(stream_name, expected_row)})"
                )
            rows_compared += 1
            total_rows += 1
            if rows_compared == 1 or rows_compared % _PROGRESS_ROW_INTERVAL == 0:
                _emit(
                    progress,
                    phase=progress_phase,
                    stream=stream_name,
                    total_rows_completed=total_rows,
                    **validation_fields,
                    **differential_progress_fields(
                        rows_compared,
                        elapsed_seconds=perf_counter() - stream_started,
                        total_expected=total_expected,
                    ),
                )
        elapsed = perf_counter() - stream_started
        stream_results[stream_name] = {
            "rows_compared": rows_compared,
            "seconds": elapsed,
        }
        _emit(
            progress,
            phase=progress_complete_phase,
            stream=stream_name,
            total_rows_completed=total_rows,
            **validation_fields,
            **differential_progress_fields(
                rows_compared,
                elapsed_seconds=elapsed,
                total_expected=total_expected,
            ),
        )
    return {
        "rows_compared": total_rows,
        "streams": stream_results,
    }


def _compare_streams(
    export_root: Path,
    target_store: PaperStore,
    run_id: str,
    *,
    verification: GoldenVerification,
    progress: ProgressCallback | None,
) -> dict[str, object]:
    return _compare_streams_connection(
        export_root,
        target_store._connect(),
        run_id,
        verification=verification,
        progress=progress,
        target_path=target_store.path,
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _preserve_target_copy(
    source: Path,
    scratch_root: Path,
    *,
    target_filename: str,
    progress: ProgressCallback | None = None,
) -> Path:
    preserved_root = Path(
        mkdtemp(
            prefix="golden-v3-replay-preserved-",
            dir=scratch_root,
        )
    )
    _fsync_directory(scratch_root)
    partial = preserved_root / f"{target_filename}.partial"
    target = preserved_root / target_filename
    total_expected = source.stat().st_size
    bytes_completed = 0
    next_progress = _COPY_PROGRESS_BYTES
    started = perf_counter()
    _emit(
        progress,
        phase="target_preservation",
        bytes_completed=0,
        elapsed_seconds=0.0,
        target_path=str(target),
        target_store_bytes=0,
        total_expected=total_expected,
    )
    with source.open("rb") as source_stream, partial.open("xb") as target_stream:
        while chunk := source_stream.read(_COPY_CHUNK_BYTES):
            target_stream.write(chunk)
            bytes_completed += len(chunk)
            if bytes_completed >= next_progress or bytes_completed == total_expected:
                _emit(
                    progress,
                    phase="target_preservation",
                    bytes_completed=bytes_completed,
                    elapsed_seconds=perf_counter() - started,
                    target_path=str(target),
                    target_store_bytes=bytes_completed,
                    total_expected=total_expected,
                )
                while next_progress <= bytes_completed:
                    next_progress += _COPY_PROGRESS_BYTES
        target_stream.flush()
        os.fsync(target_stream.fileno())
    os.replace(partial, target)
    _fsync_directory(preserved_root)
    preserved = target.resolve(strict=True)
    _emit(
        progress,
        phase="target_preservation_complete",
        bytes_completed=bytes_completed,
        elapsed_seconds=perf_counter() - started,
        target_path=str(preserved),
        target_store_bytes=bytes_completed,
        total_expected=total_expected,
    )
    return preserved


def _fingerprint_target(
    path: Path,
    *,
    progress: ProgressCallback | None = None,
) -> tuple[str, int]:
    target_bytes = path.stat().st_size
    started = perf_counter()
    _emit(
        progress,
        phase="target_fingerprint",
        bytes_completed=0,
        elapsed_seconds=0.0,
        target_path=str(path),
        target_store_bytes=target_bytes,
        total_expected=target_bytes,
    )
    target_sha256 = _sha256_file(path)
    _emit(
        progress,
        phase="target_fingerprint_complete",
        bytes_completed=target_bytes,
        elapsed_seconds=perf_counter() - started,
        target_path=str(path),
        target_store_bytes=target_bytes,
        total_expected=target_bytes,
    )
    return target_sha256, target_bytes


def replay_golden_v3(
    export_root: Path | str,
    scratch_root: Path | str,
    progress: ProgressCallback | None = None,
    target_filename: str = "paper-replay.sqlite3",
    verification: GoldenVerification | None = None,
) -> dict[str, object]:
    """Rebuild a disposable PaperStore from Golden inputs and compare every logical row."""

    started = perf_counter()
    filename_path = Path(target_filename)
    if (
        not target_filename
        or filename_path.is_absolute()
        or filename_path.name != target_filename
    ):
        raise GoldenReplayError("target_filename must be one non-empty path component")
    scratch_lexical = Path(
        os.path.abspath(os.fspath(Path(scratch_root).expanduser()))
    )
    if _has_reparse_component(scratch_lexical):
        raise GoldenReplayError(
            "scratch_root contains a symlink, junction, or reparse component"
        )
    scratch = scratch_lexical.resolve()
    export = Path(export_root).resolve(strict=True)
    if scratch == export or export in scratch.parents:
        raise GoldenReplayError("scratch_root must be distinct from and outside the Golden export")

    verify_started = perf_counter()
    _emit(progress, phase="verify_export", rows_completed=0)
    if verification is None:
        verification = verify_golden_v3(export)
    elif verification.export_root != export:
        raise GoldenReplayError(
            "verified Golden root differs from the requested replay export"
        )
    manifest = _mapping(verification.manifest, label="Golden manifest")
    census = _mapping(manifest.get("census"), label="Golden census")
    total_expected = _integer(
        census.get("commit_count"),
        label="Golden census.commit_count",
        minimum=1,
    )
    verify_seconds = perf_counter() - verify_started
    _emit(
        progress,
        phase="verify_export_complete",
        rows_completed=0,
        elapsed_seconds=verify_seconds,
    )

    run_id, config, first_input, remaining_inputs = _load_run_contract(
        export,
        manifest,
        verification=verification,
    )
    _mkdir_durable(scratch, exist_ok=True)
    if _has_reparse_component(scratch):
        raise GoldenReplayError(
            "scratch_root contains a symlink, junction, or reparse component"
        )
    scratch = scratch.resolve(strict=True)

    temporary_directory = TemporaryDirectory(
        prefix="golden-v3-replay-",
        dir=scratch,
    )
    temporary_target = Path(temporary_directory.name) / target_filename
    replay_store: PaperStore | None = None
    preserved_target: Path | None = None
    target_head_identity: list[object] | None = None
    projection_hash: str | None = None
    event_count: int | None = None
    commit_count: int | None = None
    differential: dict[str, object] | None = None
    replay_seconds = 0.0
    target_integrity_seconds = 0.0
    differential_seconds = 0.0
    preserve_started: float | None = None

    def replay_progress_fields(
        completed: int,
        *,
        elapsed_seconds: float,
    ) -> dict[str, object]:
        fields: dict[str, object] = {
            "commits_completed": completed,
            "elapsed_seconds": elapsed_seconds,
            "rows_completed": completed,
            "target_path": str(temporary_target),
            "target_store_bytes": (
                temporary_target.stat().st_size
                if temporary_target.is_file()
                else 0
            ),
            "total_expected": total_expected,
        }
        if 0 < completed < total_expected and elapsed_seconds > 0:
            fields["eta_seconds"] = (
                elapsed_seconds * (total_expected - completed) / completed
            )
        return fields

    try:
        replay_store = PaperStore._create_temporary_historical_replay(
            temporary_directory,
            filename=target_filename,
        )
        replay_engine = PaperEngine._for_historical_replay(replay_store, config)
        legacy = _LegacyObservationState(
            events=iter(
                _iter_legacy_events(
                    export,
                    run_id=run_id,
                    verification=verification,
                )
            )
        )
        replay_started = perf_counter()
        _emit(
            progress,
            phase="replay",
            **replay_progress_fields(0, elapsed_seconds=0.0),
        )
        started_result = replay_engine.start()
        if (
            started_result.append.idempotent
            or started_result.append.input_id != first_input.input_id
            or started_result.append.commit_sequence != first_input.commit_sequence
            or started_result.append.commit_hash != first_input.commit_hash
        ):
            raise GoldenReplayMismatchError(
                "replayed RUN_START differs from the Golden commit"
            )
        completed_inputs = 1
        _emit(
            progress,
            phase="replay",
            last_input_id=first_input.input_id,
            **replay_progress_fields(
                completed_inputs,
                elapsed_seconds=perf_counter() - replay_started,
            ),
        )
        for record in remaining_inputs:
            try:
                result = _dispatch_input(
                    replay_engine,
                    record,
                    legacy=legacy,
                )
            except GoldenReplayError:
                raise
            except (KeyError, TypeError, ValueError) as error:
                raise GoldenReplayMismatchError(
                    f"Golden input {record.input_id!r} cannot be replayed exactly"
                ) from error
            if (
                result.append.idempotent
                or result.append.input_id != record.input_id
                or result.append.commit_sequence != record.commit_sequence
                or result.append.commit_hash != record.commit_hash
            ):
                raise GoldenReplayMismatchError(
                    f"replayed input {record.input_id!r} differs from its Golden commit"
                )
            completed_inputs += 1
            if (
                completed_inputs == 1
                or completed_inputs % _PROGRESS_ROW_INTERVAL == 0
            ):
                elapsed = perf_counter() - replay_started
                _emit(
                    progress,
                    phase="replay",
                    last_input_id=record.input_id,
                    **replay_progress_fields(
                        completed_inputs,
                        elapsed_seconds=elapsed,
                    ),
                )
        replay_seconds = perf_counter() - replay_started
        _emit(
            progress,
            phase="replay_complete",
            **replay_progress_fields(
                completed_inputs,
                elapsed_seconds=replay_seconds,
            ),
        )

        integrity_started = perf_counter()
        _emit(
            progress,
            phase="target_integrity",
            rows_completed=completed_inputs,
            commits_completed=completed_inputs,
            target_path=str(temporary_target),
            target_store_bytes=temporary_target.stat().st_size,
            total_expected=total_expected,
        )
        integrity = replay_store.inspect_integrity_readonly(run_id)
        if not integrity.ok:
            raise GoldenReplayMismatchError(str(IntegrityError(integrity)))
        replayed_projection = replay_store.get_projection(run_id)
        semantic_errors = replay_engine._ledger_reconciliation_errors(
            replayed_projection
        )
        if semantic_errors:
            raise GoldenReplayMismatchError(
                "replayed target ledger does not reconcile exactly: "
                + "; ".join(semantic_errors)
            )
        target_integrity_seconds = perf_counter() - integrity_started
        _emit(
            progress,
            phase="target_integrity_complete",
            rows_completed=completed_inputs,
            commits_completed=completed_inputs,
            elapsed_seconds=target_integrity_seconds,
            target_path=str(temporary_target),
            target_store_bytes=temporary_target.stat().st_size,
            total_expected=total_expected,
        )

        head_before = replay_store.get_run(run_id)
        differential_started = perf_counter()
        differential = _compare_streams(
            export,
            replay_store,
            run_id,
            verification=verification,
            progress=progress,
        )
        differential_seconds = perf_counter() - differential_started
        head_after = replay_store.get_run(run_id)
        if head_after.head_identity != head_before.head_identity:
            raise GoldenReplayMismatchError(
                "replay target head changed during differential"
            )
        target_head_identity = list(head_after.head_identity)
        projection_hash = replayed_projection.canonical_hash
        event_count = head_after.event_sequence
        commit_count = head_after.commit_sequence
    finally:
        primary_error = sys.exception()
        cleanup_errors: list[BaseException] = []
        if replay_store is not None:
            try:
                replay_store.close()
            except BaseException as error:
                cleanup_errors.append(error)
        if temporary_target.is_file():
            try:
                preserve_started = perf_counter()
                preserved_target = _preserve_target_copy(
                    temporary_target,
                    scratch,
                    target_filename=target_filename,
                    progress=progress,
                )
                _emit(
                    progress,
                    phase=(
                        "target_preserved"
                        if primary_error is None
                        else "partial_target_preserved"
                    ),
                    target_path=str(preserved_target),
                    target_store_bytes=preserved_target.stat().st_size,
                    rows_completed=commit_count or 0,
                    commits_completed=commit_count or 0,
                    total_expected=total_expected,
                )
            except BaseException as error:
                cleanup_errors.append(error)
        del replay_store
        gc.collect()
        try:
            temporary_directory.cleanup()
        except BaseException as error:
            cleanup_errors.append(error)
        if cleanup_errors:
            if primary_error is None:
                raise cleanup_errors[0]
            for cleanup_error in cleanup_errors:
                primary_error.add_note(
                    "Golden replay cleanup/preservation also failed with "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )

    if (
        preserved_target is None
        or target_head_identity is None
        or projection_hash is None
        or event_count is None
        or commit_count is None
        or differential is None
        or preserve_started is None
    ):
        raise GoldenReplayError("Golden replay completed without a closed preserved target")
    target_sha256, target_bytes = _fingerprint_target(
        preserved_target,
        progress=progress,
    )
    preserve_seconds = perf_counter() - preserve_started
    total_seconds = perf_counter() - started
    source_root_hash = _text(
        manifest.get("root_hash"),
        label="manifest.root_hash",
    )
    timings = {
        "verify_export_seconds": verify_seconds,
        "replay_seconds": replay_seconds,
        "target_integrity_seconds": target_integrity_seconds,
        "differential_seconds": differential_seconds,
        "preserve_fingerprint_seconds": preserve_seconds,
        "total_seconds": total_seconds,
    }
    replay_result: dict[str, object] = {
        "commit_count": commit_count,
        "config_hash": config.config_hash,
        "differential": differential,
        "event_count": event_count,
        "mode": "PAPER_ONLY",
        "orders_enabled": False,
        "projection_hash": projection_hash,
        "rows_compared": differential["rows_compared"],
        "run_id": run_id,
        "source_root_hash": source_root_hash,
        "status": "REPLAY_DIFFERENTIAL_EXACT",
        "target_bytes": target_bytes,
        "target_head_identity": target_head_identity,
        "target_path": str(preserved_target),
        "target_sha256": target_sha256,
        "timings": timings,
    }
    _emit(
        progress,
        phase="complete",
        status="REPLAY_DIFFERENTIAL_EXACT",
        rows_completed=differential["rows_compared"],
        commits_completed=commit_count,
        target_path=str(preserved_target),
        target_store_bytes=target_bytes,
        total_expected=total_expected,
        elapsed_seconds=total_seconds,
    )
    return replay_result


__all__ = [
    "GoldenReplayError",
    "GoldenReplayMismatchError",
    "ProgressCallback",
    "replay_golden_v3",
]
