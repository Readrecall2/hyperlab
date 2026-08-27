from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, NoReturn, TextIO

from hyperlab.research_data.envelope import Venue
from hyperlab.research_data.prediction_candidate import (
    CandidatePreregistration,
    PredictionCollectionBinding,
    validate_prediction_campaign_manifest,
)
from hyperlab.research_data.prediction_contracts import OfficialPublicContract
from hyperlab.research_data.prediction_evidence import PredictionRawEvidenceIndex
from hyperlab.research_data.segments import ResearchSegmentReader

BOUNDARY = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
ECONOMIC_STATUS = "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE"
_MAX_JSON_BYTES = 4 * 1024 * 1024
_MAX_LEDGER_BYTES = 8 * 1024 * 1024


class RunnerError(RuntimeError):
    """Fail-closed campaign runner error."""


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: object, *, label: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise RunnerError(f"{label} is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise RunnerError(f"{label} is not RFC3339") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RunnerError(f"{label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _regular_file_bytes(path: Path, *, maximum_bytes: int) -> bytes:
    try:
        before = path.lstat()
    except OSError as error:
        raise RunnerError(f"required file is unreadable: {path}") from error
    if path.is_symlink() or not path.is_file() or before.st_size > maximum_bytes:
        raise RunnerError(f"required file is unsafe: {path}")
    raw = path.read_bytes()
    after = path.lstat()
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise RunnerError(f"file head changed during read: {path}")
    if len(raw) != before.st_size:
        raise RunnerError(f"short read: {path}")
    return raw


def _object(path: Path, *, maximum_bytes: int = _MAX_JSON_BYTES) -> dict[str, Any]:
    try:
        value = json.loads(_regular_file_bytes(path, maximum_bytes=maximum_bytes).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RunnerError(f"invalid JSON object: {path}") from error
    if not isinstance(value, dict):
        raise RunnerError(f"JSON root must be an object: {path}")
    return value


def _pinned_object(path: Path) -> dict[str, Any]:
    raw = _regular_file_bytes(path, maximum_bytes=_MAX_JSON_BYTES)
    pin = _regular_file_bytes(path.with_suffix(".sha256"), maximum_bytes=256)
    fields = pin.decode("ascii").strip().split()
    if (
        len(fields) != 2
        or fields[1] != path.name
        or sha256_bytes(raw) != fields[0]
    ):
        raise RunnerError(f"pinned object physical SHA-256 diverged: {path}")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RunnerError(f"pinned object is invalid JSON: {path}") from error
    if not isinstance(value, dict):
        raise RunnerError(f"pinned object must be an object: {path}")
    return value


@dataclass(frozen=True, slots=True)
class CampaignContext:
    campaign_root: Path
    manifest: Mapping[str, object]
    preregistration: CandidatePreregistration
    contracts: Mapping[Venue, OfficialPublicContract]
    start: datetime
    end: datetime
    cadence_seconds: int
    duration_seconds: int
    expected_slots: int


def load_campaign_context(campaign_root: Path, source_root: Path) -> CampaignContext:
    root = campaign_root.resolve(strict=True)
    if campaign_root.is_symlink() or not root.is_dir():
        raise RunnerError("campaign root must be a real directory")
    manifest_path = root / "campaign-manifest.json"
    raw_manifest = _regular_file_bytes(manifest_path, maximum_bytes=_MAX_JSON_BYTES)
    pin = _regular_file_bytes(root / "campaign-manifest.sha256", maximum_bytes=256)
    fields = pin.decode("ascii").strip().split()
    if len(fields) != 2 or fields[1] != "campaign-manifest.json":
        raise RunnerError("campaign manifest pin is malformed")
    if sha256_bytes(raw_manifest) != fields[0]:
        raise RunnerError("campaign manifest physical SHA-256 diverged")
    try:
        manifest_value = json.loads(raw_manifest.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RunnerError("campaign manifest is invalid JSON") from error
    if not isinstance(manifest_value, dict):
        raise RunnerError("campaign manifest must be an object")
    config_root = source_root / "config" / "research"
    try:
        preregistration = CandidatePreregistration.from_path(
            config_root / "prediction-markets-candidate-v1.json"
        )
        contracts = {
            Venue.POLYMARKET: OfficialPublicContract.from_path(
                config_root / "polymarket-public-contract-v1.json"
            ),
            Venue.KALSHI: OfficialPublicContract.from_path(
                config_root / "kalshi-public-contract-v1.json"
            ),
        }
        validate_prediction_campaign_manifest(
            campaign_manifest=manifest_value,
            preregistration=preregistration,
            contracts=contracts,
        )
    except (OSError, ValueError) as error:
        raise RunnerError(f"campaign semantic binding diverged: {error}") from error
    policy = preregistration.prospective_shard_policy
    start = _parse_utc(manifest_value.get("starts_at_utc"), label="campaign start")
    end = start + timedelta(days=policy.campaign_days)
    return CampaignContext(
        campaign_root=root,
        manifest=manifest_value,
        preregistration=preregistration,
        contracts=contracts,
        start=start,
        end=end,
        cadence_seconds=policy.cadence_seconds,
        duration_seconds=policy.collection_duration_seconds,
        expected_slots=policy.expected_shards_per_venue,
    )


def _ledger_body(entry: Mapping[str, object]) -> dict[str, object]:
    return {key: value for key, value in entry.items() if key != "entry_sha256"}


def read_ledger(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    raw = _regular_file_bytes(path, maximum_bytes=_MAX_LEDGER_BYTES)
    rows: list[dict[str, Any]] = []
    previous = "0" * 64
    seen: set[int] = set()
    for index, line in enumerate(raw.splitlines(), start=1):
        try:
            value = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RunnerError(f"ledger line {index} is invalid") from error
        if not isinstance(value, dict):
            raise RunnerError(f"ledger line {index} is not an object")
        ordinal = value.get("ordinal")
        claimed = value.get("entry_sha256")
        if (
            type(ordinal) is not int
            or ordinal < 0
            or ordinal in seen
            or value.get("previous_entry_sha256") != previous
            or not isinstance(claimed, str)
            or sha256_bytes(canonical_json_bytes(_ledger_body(value))) != claimed
        ):
            raise RunnerError(f"ledger chain diverged at line {index}")
        seen.add(ordinal)
        previous = claimed
        rows.append(value)
    return rows


def append_ledger(path: Path, body: Mapping[str, object]) -> dict[str, object]:
    rows = read_ledger(path)
    ordinal = body.get("ordinal")
    if type(ordinal) is not int or ordinal < 0:
        raise RunnerError("ledger ordinal is invalid")
    if any(row["ordinal"] == ordinal for row in rows):
        raise RunnerError("ledger ordinal is already terminally accounted")
    previous = "0" * 64 if not rows else str(rows[-1]["entry_sha256"])
    chained = {**body, "previous_entry_sha256": previous}
    entry = {**chained, "entry_sha256": sha256_bytes(canonical_json_bytes(chained))}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab", buffering=0) as handle:
        handle.write(canonical_json_bytes(entry) + b"\n")
        os.fsync(handle.fileno())
    _fsync_directory(path.parent)
    return entry


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_object(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes(value) + b"\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


@dataclass(frozen=True, slots=True)
class ScheduleDecision:
    action: str
    ordinal: int | None
    missing_ordinals: tuple[int, ...]
    wait_seconds: float


def schedule_decision(
    context: CampaignContext,
    ledger: Sequence[Mapping[str, object]],
    *,
    now: datetime,
) -> ScheduleDecision:
    current = now.astimezone(UTC)
    accounted: set[int] = set()
    for row in ledger:
        ordinal = row.get("ordinal")
        if type(ordinal) is not int:
            raise RunnerError("ledger schedule ordinal is invalid")
        accounted.add(ordinal)
    if current < context.start:
        return ScheduleDecision("WAIT_FOR_START", None, (), (context.start - current).total_seconds())
    elapsed_seconds = max(0.0, (current - context.start).total_seconds())
    current_ordinal = int(elapsed_seconds // context.cadence_seconds)
    if current >= context.end or current_ordinal >= context.expected_slots:
        missing = tuple(index for index in range(context.expected_slots) if index not in accounted)
        return ScheduleDecision("COMPLETE_WINDOW", None, missing, 0.0)
    missing = tuple(index for index in range(current_ordinal) if index not in accounted)
    if current_ordinal in accounted:
        next_start = context.start + timedelta(seconds=(current_ordinal + 1) * context.cadence_seconds)
        return ScheduleDecision("WAIT_NEXT_SLOT", None, missing, (next_start - current).total_seconds())
    slot_start = context.start + timedelta(seconds=current_ordinal * context.cadence_seconds)
    latest_start = slot_start + timedelta(
        seconds=context.cadence_seconds - context.duration_seconds
    )
    if current > latest_start:
        return ScheduleDecision(
            "MISSED_CURRENT_SLOT",
            None,
            (*missing, current_ordinal),
            max(0.0, (slot_start + timedelta(seconds=context.cadence_seconds) - current).total_seconds()),
        )
    return ScheduleDecision("RUN_SLOT", current_ordinal, missing, 0.0)


def _slot_start(context: CampaignContext, ordinal: int) -> datetime:
    return context.start + timedelta(seconds=ordinal * context.cadence_seconds)


def _missed_entry(context: CampaignContext, venue: Venue, ordinal: int, now: datetime) -> dict[str, object]:
    return {
        "boundary": BOUNDARY,
        "bytes": None,
        "duplicates": None,
        "error": "scheduled slot elapsed without an admitted full collection window",
        "frames": None,
        "gaps": None,
        "manifest_sha256": None,
        "ordinal": ordinal,
        "reconnects": None,
        "recorded_at_utc": _utc_text(now),
        "root_sha256": None,
        "scheduled_start_utc": _utc_text(_slot_start(context, ordinal)),
        "segments": None,
        "terminal_health": "MISSING_SLOT_NO_BACKFILL",
        "venue": venue.value,
    }


def _result_entry(
    context: CampaignContext,
    venue: Venue,
    ordinal: int,
    result: Mapping[str, object],
    *,
    now: datetime,
) -> dict[str, object]:
    return {
        "boundary": BOUNDARY,
        "bytes": result.get("bytes"),
        "duplicates": result.get("duplicates"),
        "error": result.get("error"),
        "frames": result.get("frames"),
        "gaps": result.get("gaps"),
        "manifest_sha256": result.get("manifest_sha256"),
        "ordinal": ordinal,
        "reconnects": result.get("reconnects"),
        "recorded_at_utc": _utc_text(now),
        "root_sha256": result.get("root_sha256"),
        "scheduled_start_utc": _utc_text(_slot_start(context, ordinal)),
        "segments": result.get("segments"),
        "terminal_health": result.get("terminal_health"),
        "terminal_result_sha256": sha256_bytes(canonical_json_bytes(result)),
        "venue": venue.value,
    }


def _state(
    *,
    context: CampaignContext,
    venue: Venue,
    ledger: Sequence[Mapping[str, object]],
    lifecycle: str,
    now: datetime,
    error: str | None = None,
    active_ordinal: int | None = None,
    capacity: Mapping[str, object] | None = None,
) -> dict[str, object]:
    latest = None if not ledger else ledger[-1]
    return {
        "active_ordinal": active_ordinal,
        "boundary": BOUNDARY,
        "campaign_id": context.manifest["campaign_id"],
        "capacity": capacity,
        "economic_evidence_status": ECONOMIC_STATUS,
        "error": error,
        "expected_slots": context.expected_slots,
        "holdout": {"access": "SEALED", "metrics_exposed": False},
        "last_terminal": None if latest is None else latest.get("terminal_health"),
        "lifecycle": lifecycle,
        "recorded_slots": len(ledger),
        "schema_version": 1,
        "updated_at_utc": _utc_text(now),
        "venue": venue.value,
    }


def capacity_snapshot(
    *,
    campaign_root: Path,
    h1_reserved_bytes: int,
    prediction_maximum_raw_bytes: int,
    safety_margin_bytes: int,
    accounted_bytes: int,
) -> dict[str, object]:
    if any(
        type(value) is not int or value < 0
        for value in (
            h1_reserved_bytes,
            prediction_maximum_raw_bytes,
            safety_margin_bytes,
            accounted_bytes,
        )
    ):
        raise RunnerError("capacity inputs are invalid")
    remaining_prediction = max(0, prediction_maximum_raw_bytes - accounted_bytes)
    required = h1_reserved_bytes + remaining_prediction + safety_margin_bytes
    available = shutil.disk_usage(campaign_root).free
    return {
        "admitted": available >= required,
        "available_bytes": available,
        "h1_reserved_bytes": h1_reserved_bytes,
        "prediction_remaining_bytes": remaining_prediction,
        "required_free_bytes": required,
        "safety_margin_bytes": safety_margin_bytes,
    }


def _probe_command(
    *,
    python: Path,
    source_root: Path,
    context: CampaignContext,
    venue: Venue,
    ordinal: int,
    output_root: Path,
) -> list[str]:
    plan = context.preregistration.collection_plans[venue]
    return [
        str(python),
        "-m",
        "hyperlab",
        "research-data",
        "prediction-collect",
        "--venue",
        venue.value.lower(),
        "--campaign-manifest",
        str(context.campaign_root / "campaign-manifest.json"),
        "--shard-ordinal",
        str(ordinal),
        "--feeds",
        ",".join(plan.feeds),
        "--census-limit",
        str(plan.census_limit),
        "--duration-seconds",
        str(plan.duration_seconds),
        "--max-network-calls",
        str(plan.max_network_calls),
        "--max-frames",
        str(plan.max_frames),
        "--max-bytes",
        str(plan.max_bytes),
        "--polymarket-contract",
        str(source_root / "config/research/polymarket-public-contract-v1.json"),
        "--kalshi-contract",
        str(source_root / "config/research/kalshi-public-contract-v1.json"),
        "--candidate-config",
        str(source_root / "config/research/prediction-markets-candidate-v1.json"),
        "--output-root",
        str(output_root),
    ]


def _recover_command(
    *,
    python: Path,
    output_root: Path,
    venue: Venue,
    requested_duration_seconds: int,
    error: str,
) -> list[str]:
    return [
        str(python),
        "-m",
        "hyperlab",
        "research-data",
        "prediction-recover",
        "--output-root",
        str(output_root),
        "--venue",
        venue.value.lower(),
        "--requested-duration-seconds",
        str(requested_duration_seconds),
        "--terminal-health",
        "RECOVERED_AFTER_PROCESS_ERROR",
        "--error",
        error[:1024],
    ]


def _datetime_utc_ns(value: datetime) -> int:
    normalized = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = normalized - epoch
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _validate_result(
    output_root: Path,
    context: CampaignContext,
    venue: Venue,
    *,
    ordinal: int,
) -> dict[str, Any]:
    try:
        binding = PredictionCollectionBinding.from_probe_output(output_root)
        plan = context.preregistration.collection_plans[venue]
        binding.verify_collection_plan(plan)
        campaign_sha256 = str(context.manifest["manifest_sha256"])
        contract = context.contracts[venue]
        scheduled_start = _slot_start(context, ordinal)
        expected_collection_id = context.preregistration.prospective_shard_policy.collection_id(
            base_collection_id=plan.collection_id(str(context.manifest["campaign_id"])),
            campaign_manifest_sha256=campaign_sha256,
            venue=venue,
            ordinal=ordinal,
            scheduled_start=scheduled_start,
        )
        expected_cutoff = _datetime_utc_ns(scheduled_start) + context.cadence_seconds * 1_000_000_000
        if (
            binding.venue is not venue
            or binding.campaign_manifest_sha256 != campaign_sha256
            or binding.candidate_config_sha256 != context.preregistration.config_sha256
            or binding.official_contract_sha256 != contract.contract_sha256
            or binding.collection_id != expected_collection_id
            or binding.payload.get("collection_cutoff_utc_ns_exclusive") != expected_cutoff
        ):
            raise ValueError("prediction terminal collection identity diverged from the scheduled slot")
        if binding.raw_manifest_sha256 is None:
            raise ValueError("prediction terminal collection lacks its raw manifest identity")
        reader = ResearchSegmentReader(
            output_root / "raw",
            manifest_sha256=binding.raw_manifest_sha256,
        )
        index = PredictionRawEvidenceIndex(reader, contracts=context.contracts)
        binding.verify(index, contract=contract)
        if any(
            envelope.receive_timestamp_utc_ns >= expected_cutoff
            for envelope in index.envelopes
        ):
            raise ValueError("prediction terminal collection contains post-cutoff raw evidence")
    except (OSError, ValueError) as error:
        raise RunnerError(f"terminal collection receipt failed authentication: {error}") from error
    result = _object(output_root / "reports" / "result.json")
    if result.get("venue") != venue.value or result.get("terminal_health") is None:
        raise RunnerError("terminal result venue or health diverged")
    return result


class _RunnerLease(AbstractContextManager[None]):
    def __init__(self, path: Path) -> None:
        self._path = path
        self._handle: TextIO | None = None

    def __enter__(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self._path.open("a+", encoding="utf-8")
        try:
            if os.name == "nt":
                import msvcrt

                self._handle.seek(0)
                if self._path.stat().st_size == 0:
                    self._handle.write("0")
                    self._handle.flush()
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                flock = vars(fcntl)["flock"]
                lock_ex = int(vars(fcntl)["LOCK_EX"])
                lock_nb = int(vars(fcntl)["LOCK_NB"])
                flock(self._handle.fileno(), lock_ex | lock_nb)
        except OSError as error:
            self._handle.close()
            self._handle = None
            raise RunnerError("another runner already owns the venue lease") from error
        return None

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None


class VenueRunner:
    def __init__(
        self,
        *,
        context: CampaignContext,
        source_root: Path,
        python: Path,
        venue: Venue,
        h1_reserved_bytes: int,
        prediction_maximum_raw_bytes: int,
        safety_margin_bytes: int,
    ) -> None:
        self.context = context
        self.source_root = source_root
        self.python = python
        self.venue = venue
        self.venue_root = context.campaign_root / venue.value.lower()
        self.ledger_path = self.venue_root / "ledger.jsonl"
        self.state_path = self.venue_root / "state.json"
        self.runs_root = self.venue_root / "runs"
        self.h1_reserved_bytes = h1_reserved_bytes
        self.prediction_maximum_raw_bytes = prediction_maximum_raw_bytes
        self.safety_margin_bytes = safety_margin_bytes
        self.stop_requested = False
        self.child: subprocess.Popen[bytes] | None = None

    def request_stop(self, _signum: int, _frame: object) -> None:
        self.stop_requested = True
        if self.child is not None and self.child.poll() is None:
            self.child.send_signal(signal.SIGINT)

    def _accounted_bytes(self) -> int:
        total = 0
        # Deliberately read only this venue's ledger. The global reservation is
        # therefore conservative, while corruption or a concurrent append in
        # the other venue can never terminate this independent service.
        for row in read_ledger(self.ledger_path):
            value = row.get("bytes")
            if type(value) is int and value >= 0:
                total += value
        return total

    def _publish(self, lifecycle: str, *, error: str | None = None, ordinal: int | None = None) -> None:
        rows = read_ledger(self.ledger_path)
        capacity = capacity_snapshot(
            campaign_root=self.context.campaign_root,
            h1_reserved_bytes=self.h1_reserved_bytes,
            prediction_maximum_raw_bytes=self.prediction_maximum_raw_bytes,
            safety_margin_bytes=self.safety_margin_bytes,
            accounted_bytes=self._accounted_bytes(),
        )
        _atomic_object(
            self.state_path,
            _state(
                context=self.context,
                venue=self.venue,
                ledger=rows,
                lifecycle=lifecycle,
                now=datetime.now(UTC),
                error=error,
                active_ordinal=ordinal,
                capacity=capacity,
            ),
        )

    def _record_missing(self, ordinals: Sequence[int]) -> None:
        rows = read_ledger(self.ledger_path)
        accounted = {int(row["ordinal"]) for row in rows}
        for ordinal in ordinals:
            if ordinal not in accounted:
                append_ledger(
                    self.ledger_path,
                    _missed_entry(self.context, self.venue, ordinal, datetime.now(UTC)),
                )
                accounted.add(ordinal)

    def _run_slot(self, ordinal: int) -> None:
        slot_start = _slot_start(self.context, ordinal)
        leaf = f"shard-{ordinal:04d}-{slot_start.strftime('%Y%m%dT%H%M%SZ')}"
        output_root = self.runs_root / leaf
        self._publish("COLLECTING", ordinal=ordinal)
        if output_root.exists():
            if (output_root / "reports" / "result.json").exists():
                result = _validate_result(
                    output_root,
                    self.context,
                    self.venue,
                    ordinal=ordinal,
                )
            elif any((output_root / "raw" / "segments").glob("*.rdpseg")):
                command = _recover_command(
                    python=self.python,
                    output_root=output_root,
                    venue=self.venue,
                    requested_duration_seconds=self.context.duration_seconds,
                    error="runner restarted after process exit without terminal result",
                )
                completed = subprocess.run(command, check=False, cwd=self.source_root)
                if completed.returncode != 0:
                    raise RunnerError("authenticated recovery command failed")
                result = _validate_result(
                    output_root,
                    self.context,
                    self.venue,
                    ordinal=ordinal,
                )
            else:
                append_ledger(
                    self.ledger_path,
                    {
                        **_missed_entry(
                            self.context,
                            self.venue,
                            ordinal,
                            datetime.now(UTC),
                        ),
                        "error": "existing shard root has no terminal receipt or recoverable segment",
                        "terminal_health": "PROCESS_ERROR_NO_TERMINAL_RECEIPT",
                    },
                )
                return
        else:
            command = _probe_command(
                python=self.python,
                source_root=self.source_root,
                context=self.context,
                venue=self.venue,
                ordinal=ordinal,
                output_root=output_root,
            )
            environment = {
                **os.environ,
                "HOME": "/home/hyperlab",
                "PYTHONNOUSERSITE": "1",
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONUNBUFFERED": "1",
                "TZ": "UTC",
            }
            self.child = subprocess.Popen(command, cwd=self.source_root, env=environment)
            return_code = self.child.wait()
            self.child = None
            if (output_root / "reports" / "result.json").exists():
                result = _validate_result(
                    output_root,
                    self.context,
                    self.venue,
                    ordinal=ordinal,
                )
            elif any((output_root / "raw" / "segments").glob("*.rdpseg")):
                recovery = subprocess.run(
                    _recover_command(
                        python=self.python,
                        output_root=output_root,
                        venue=self.venue,
                        requested_duration_seconds=self.context.duration_seconds,
                        error=f"collector process exit={return_code} without terminal result",
                    ),
                    check=False,
                    cwd=self.source_root,
                    env=environment,
                )
                if recovery.returncode != 0:
                    raise RunnerError("authenticated recovery command failed")
                result = _validate_result(
                    output_root,
                    self.context,
                    self.venue,
                    ordinal=ordinal,
                )
            else:
                append_ledger(
                    self.ledger_path,
                    {
                        **_missed_entry(
                            self.context,
                            self.venue,
                            ordinal,
                            datetime.now(UTC),
                        ),
                        "error": f"collector process exit={return_code} without terminal receipt",
                        "terminal_health": "PROCESS_ERROR_NO_TERMINAL_RECEIPT",
                    },
                )
                return
        append_ledger(
            self.ledger_path,
            _result_entry(
                self.context,
                self.venue,
                ordinal,
                result,
                now=datetime.now(UTC),
            ),
        )

    def _publish_integrity_failure(self, error: BaseException) -> None:
        _atomic_object(
            self.state_path,
            {
                "active_ordinal": None,
                "boundary": BOUNDARY,
                "campaign_id": self.context.manifest["campaign_id"],
                "capacity": None,
                "economic_evidence_status": ECONOMIC_STATUS,
                "error": f"{type(error).__name__}:{error}"[:2048],
                "expected_slots": self.context.expected_slots,
                "holdout": {"access": "SEALED", "metrics_exposed": False},
                "last_terminal": None,
                "lifecycle": "INTEGRITY_FAILED",
                "recorded_slots": None,
                "schema_version": 1,
                "updated_at_utc": _utc_text(datetime.now(UTC)),
                "venue": self.venue.value,
            },
        )

    def _run_owned(self) -> int:
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)
        while not self.stop_requested:
            rows = read_ledger(self.ledger_path)
            decision = schedule_decision(self.context, rows, now=datetime.now(UTC))
            self._record_missing(decision.missing_ordinals)
            capacity = capacity_snapshot(
                campaign_root=self.context.campaign_root,
                h1_reserved_bytes=self.h1_reserved_bytes,
                prediction_maximum_raw_bytes=self.prediction_maximum_raw_bytes,
                safety_margin_bytes=self.safety_margin_bytes,
                accounted_bytes=self._accounted_bytes(),
            )
            if capacity["admitted"] is not True:
                self._publish("CAPACITY_REFUSED", error="coexistence reservation is no longer proven")
                return 4
            if decision.action == "COMPLETE_WINDOW":
                self._publish("COMPLETE_WINDOW")
                return 0
            if decision.action == "RUN_SLOT" and decision.ordinal is not None:
                self._run_slot(decision.ordinal)
                continue
            lifecycle = (
                "PREPARED"
                if decision.action == "WAIT_FOR_START"
                else "WAITING_NEXT_SLOT"
            )
            self._publish(lifecycle)
            deadline = time.monotonic() + min(max(decision.wait_seconds, 0.1), 30.0)
            while not self.stop_requested and time.monotonic() < deadline:
                time.sleep(min(1.0, deadline - time.monotonic()))
        self._publish("INTERRUPTED_RECOVERABLE")
        return 130

    def run(self) -> int:
        self.venue_root.mkdir(parents=True, exist_ok=True)
        self.runs_root.mkdir(parents=True, exist_ok=True)
        with _RunnerLease(self.venue_root / ".runner.lock"):
            try:
                return self._run_owned()
            except (OSError, RunnerError, ValueError) as error:
                self._publish_integrity_failure(error)
                raise


def _positive_int(value: object, *, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise RunnerError(f"{label} is invalid")
    return value


def run_from_handoff(handoff_path: Path, venue: Venue) -> int:
    handoff = _pinned_object(handoff_path)
    if handoff.get("boundary") != BOUNDARY:
        raise RunnerError("handoff boundary diverged")
    source_root = Path(str(handoff.get("source_root"))).resolve(strict=True)
    campaign_root = Path(str(handoff.get("campaign_root"))).resolve(strict=True)
    python = source_root / ".venv" / "bin" / "python"
    if not python.is_file() or python.is_symlink():
        raise RunnerError("offline runtime Python is absent")
    context = load_campaign_context(campaign_root, source_root)
    disk = handoff.get("disk")
    if not isinstance(disk, Mapping):
        raise RunnerError("handoff disk contract is absent")
    runner = VenueRunner(
        context=context,
        source_root=source_root,
        python=python,
        venue=venue,
        h1_reserved_bytes=_positive_int(disk.get("h1_reserved_bytes"), label="H1 reservation"),
        prediction_maximum_raw_bytes=_positive_int(
            disk.get("prediction_maximum_raw_bytes"), label="prediction raw budget"
        ),
        safety_margin_bytes=_positive_int(disk.get("safety_margin_bytes"), label="safety margin"),
    )
    return runner.run()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prediction Markets persistent venue runner")
    parser.add_argument("--handoff", type=Path, required=True)
    parser.add_argument("--venue", choices=("polymarket", "kalshi"), required=True)
    return parser


def _fail(message: str) -> NoReturn:
    print(f"PREDICTION_RUNNER_REFUSED:{message}", file=sys.stderr)
    raise SystemExit(4)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    venue = Venue.POLYMARKET if arguments.venue == "polymarket" else Venue.KALSHI
    try:
        return run_from_handoff(arguments.handoff, venue)
    except (OSError, RunnerError, ValueError) as error:
        _fail(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
