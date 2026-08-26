from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from hyperlab.ghost.h1 import (
    H1_READY,
    H1PolicyConfig,
    replay_h1_research_manifest,
)

from .canonical import canonical_json_bytes
from .envelope import CaptureProvenance, SessionEnvelopeFactory, Venue
from .probe import (
    HYPERLIQUID_METADATA_VERSION,
    HYPERLIQUID_PUBLIC_HTTP_URL,
    ProbeConfig,
    _Counters,
    _default_http_session,
    _hyperliquid_probe,
)
from .segments import ResearchDataCapacityError, ResearchSegmentReader, ResearchSegmentWriter

CAMPAIGN_BOUNDARY = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("campaign timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("campaign timestamp is invalid") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("campaign timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _atomic_bytes(path: Path, value: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("xb") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _atomic_json(path: Path, body: Mapping[str, object]) -> None:
    _atomic_bytes(path, canonical_json_bytes(body))


@dataclass(frozen=True, slots=True)
class H1CampaignPreparation:
    campaign_id: str
    manifest_sha256: str
    policy_config_sha256: str
    campaign_root: Path


def _fee_contract(path: Path, config: H1PolicyConfig) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    identity = _sha256(raw)
    expected = str(config.body["costs"]["fee_artifact_sha256"])
    if identity != expected:
        raise ValueError("fee artifact differs from the pre-registered policy identity")
    decoded = json.loads(raw.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("fee artifact must be a JSON object")
    policy = decoded.get("policy")
    rules = decoded.get("official_fee_rules")
    if (
        not isinstance(policy, dict)
        or policy.get("account_or_private_data_used") is not False
        or policy.get("maker_rebate_assumed") is not False
        or policy.get("tier") != "public tier 0"
        or not isinstance(rules, list)
    ):
        raise ValueError("fee artifact is not the prudent public tier-0 contract")
    perp = [item for item in rules if isinstance(item, dict) and item.get("instrument_pattern") == "HL:*:perp"]
    if len(perp) != 1 or str(perp[0].get("maker_fee_bps")) != format(
        config.maker_fee_bps, "f"
    ) or str(perp[0].get("taker_fee_bps")) != format(config.taker_fee_bps, "f"):
        raise ValueError("fee artifact schedule differs from H1 conservative costs")
    return decoded, identity


def prepare_h1_campaign(
    campaign_root: Path,
    *,
    config_path: Path,
    fee_artifact_path: Path,
    starts_at_utc: datetime,
    fee_reviewed_at_utc: datetime,
) -> H1CampaignPreparation:
    """Freeze policy, fees, universe and chronological holdout before collection."""

    if campaign_root.exists():
        raise FileExistsError("H1 campaign root must be new")
    if starts_at_utc.tzinfo is None or starts_at_utc.utcoffset() is None:
        raise ValueError("campaign start must be timezone-aware")
    if fee_reviewed_at_utc.tzinfo is None or fee_reviewed_at_utc.utcoffset() is None:
        raise ValueError("fee review must be timezone-aware")
    starts = starts_at_utc.astimezone(UTC)
    reviewed = fee_reviewed_at_utc.astimezone(UTC)
    prepared = _utc_now()
    if starts < prepared:
        raise ValueError("campaign must be frozen before its prospective UTC start")
    if reviewed > prepared:
        raise ValueError("fee review cannot be future-dated at campaign preparation")
    if reviewed > starts:
        raise ValueError("fee review cannot occur after campaign start")
    if starts - reviewed > timedelta(hours=24):
        raise ValueError("fee review must occur within 24 hours before campaign start")
    config = H1PolicyConfig.from_path(config_path)
    fee_body, fee_sha256 = _fee_contract(fee_artifact_path, config)
    runner = config.body["runner"]
    assert isinstance(runner, dict)
    holdout_start = starts + timedelta(days=10)
    campaign_end = starts + timedelta(days=14)
    base: dict[str, object] = {
        "boundary": CAMPAIGN_BOUNDARY,
        "collection": {
            "completion_signal": "state/health.json:COMPLETE_COLLECTION_WINDOW_OR_VERIFIED_THRESHOLDS",
            "ctrl_c": "INTERRUPTED_RECOVERABLE_AFTER_AUTHENTICATED_TAIL_CLOSE",
            "feeds": runner["feeds"],
            "maximum_days": runner["maximum_days"],
            "maximum_raw_bytes": runner["maximum_raw_bytes"],
            "minimum_days": runner["minimum_days"],
            "prompt_behavior": "NO_PROMPT",
            "rotation_seconds": runner["rotation_seconds"],
            "segment_bytes": runner["segment_bytes"],
        },
        "economic_claim": "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE",
        "ends_at_utc": _utc_text(campaign_end),
        "fee_artifact_observed_on": fee_body.get("observed_on"),
        "fee_artifact_path": fee_artifact_path.as_posix(),
        "fee_artifact_sha256": fee_sha256,
        "fee_reviewed_at_utc": _utc_text(reviewed),
        "holdout": {
            "access": "SEALED_UNTIL_COLLECTION_COMPLETE",
            "ends_at_utc": _utc_text(campaign_end),
            "starts_at_utc": _utc_text(holdout_start),
        },
        "policy_config_path": config_path.as_posix(),
        "policy_config_sha256": config.config_sha256,
        "policy_id": config.policy_id,
        "prepared_at_utc": _utc_text(prepared),
        "schema_version": 1,
        "splits": config.body["splits"],
        "starts_at_utc": _utc_text(starts),
        "universe": list(config.instruments),
        "variants": [item.to_dict() for item in config.variants],
    }
    campaign_id = f"h1-{_sha256(canonical_json_bytes(base))[:24]}"
    manifest = {**base, "campaign_id": campaign_id}
    raw = canonical_json_bytes(manifest)
    manifest_sha256 = _sha256(raw)
    campaign_root.mkdir(parents=True)
    operator = campaign_root / "operator"
    state = campaign_root / "state"
    operator.mkdir()
    state.mkdir()
    _atomic_bytes(campaign_root / "campaign-manifest.json", raw)
    _atomic_bytes(
        campaign_root / "campaign-manifest.sha256",
        f"{manifest_sha256}  campaign-manifest.json\n".encode("ascii"),
    )
    windows = f"""LOCATION=Windows PowerShell local
EXPECTED_DURATION=7-14 days
MAXIMUM_DURATION=14 days plus bounded finalization
PROMPT_BEHAVIOR=NO_PROMPT
MONITOR={campaign_root / 'state' / 'health.json'}
CTRL_C=closes the admitted tail and returns INTERRUPTED_RECOVERABLE; resume with --resume
COMPLETION_SIGNAL=state/health.json terminal_health COMPLETE_COLLECTION_WINDOW_OR_VERIFIED_THRESHOLDS
COMMAND=& '.\\.venv\\Scripts\\python.exe' -m hyperlab research-data h1-collect --campaign-root '{campaign_root}' --config '{config_path}'
"""
    tabby = """NO_VPS_COMMAND_EXECUTED_BY_CODEX
LOCATION=Tabby - VPS Bash, only after operator review
EXPECTED_DURATION=7-14 days
MAXIMUM_DURATION=14 days plus bounded finalization
PROMPT_BEHAVIOR=NO_PROMPT
PATHS=replace CAMPAIGN_ROOT_ON_VPS and CONFIG_PATH_ON_VPS with reviewed VPS paths
MONITOR=CAMPAIGN_ROOT_ON_VPS/state/health.json from a second Tabby tab
CTRL_C=closes the admitted tail and returns INTERRUPTED_RECOVERABLE; resume with --resume
COMPLETION_SIGNAL=state/health.json terminal_health COMPLETE_COLLECTION_WINDOW_OR_VERIFIED_THRESHOLDS
COMMAND=.venv/bin/python -m hyperlab research-data h1-collect --campaign-root 'CAMPAIGN_ROOT_ON_VPS' --config 'CONFIG_PATH_ON_VPS'
"""
    _atomic_bytes(operator / "windows-powershell.txt", windows.encode("utf-8"))
    _atomic_bytes(operator / "tabby-vps-bash.txt", tabby.encode("utf-8"))
    _atomic_json(
        state / "health.json",
        {
            "boundary": CAMPAIGN_BOUNDARY,
            "campaign_id": campaign_id,
            "manifest_sha256": None,
            "monitoring": "state/health.json",
            "raw_root_sha256": None,
            "terminal_health": "PREPARED_NOT_STARTED",
        },
    )
    return H1CampaignPreparation(
        campaign_id=campaign_id,
        manifest_sha256=manifest_sha256,
        policy_config_sha256=config.config_sha256,
        campaign_root=campaign_root,
    )


def _load_campaign(campaign_root: Path, config: H1PolicyConfig) -> dict[str, Any]:
    manifest_path = campaign_root / "campaign-manifest.json"
    raw = manifest_path.read_bytes()
    pin = (campaign_root / "campaign-manifest.sha256").read_text(encoding="ascii").split()[0]
    if _sha256(raw) != pin:
        raise ValueError("campaign manifest differs from its pre-collection pin")
    decoded = json.loads(raw)
    if not isinstance(decoded, dict):
        raise ValueError("campaign manifest must be an object")
    if decoded.get("policy_config_sha256") != config.config_sha256:
        raise ValueError("campaign policy config identity changed after preparation")
    if decoded.get("boundary") != CAMPAIGN_BOUNDARY:
        raise ValueError("campaign boundary is invalid")
    return decoded


def _verified_threshold_stop(
    campaign_root: Path,
    *,
    config: H1PolicyConfig,
    campaign_start: datetime,
    raw_root: Path,
) -> bool:
    """Accept only a fully gated public prefix after three fixed holdout days."""

    if _utc_now() < campaign_start + timedelta(days=13):
        return False
    path = campaign_root / "state" / "verified-threshold-report.json"
    if not path.is_file():
        return False
    try:
        raw_report = path.read_bytes()
        stripped = raw_report.rstrip(b"\r\n")
        if raw_report[len(stripped) :] not in {b"", b"\n", b"\r\n"}:
            return False
        decoded = json.loads(stripped.decode("utf-8"))
        if not isinstance(decoded, dict):
            return False
        if (
            decoded.get("technical_verdict") != H1_READY
            or decoded.get("policy_config_sha256") != config.config_sha256
            or decoded.get("synthetic") is not False
        ):
            return False
        raw_manifest = decoded.get("raw_manifest_sha256")
        raw_root_sha = decoded.get("raw_root_sha256")
        if not isinstance(raw_manifest, str) or not isinstance(raw_root_sha, str):
            return False
        reader = ResearchSegmentReader(raw_root, manifest_sha256=raw_manifest)
        if reader.manifest.root_sha256 != raw_root_sha:
            return False
        recomputed = replay_h1_research_manifest(
            raw_root,
            raw_manifest,
            config=config,
        )
        if stripped != recomputed.canonical_bytes():
            return False
        latency_reports = decoded.get("latency_reports")
        if not isinstance(latency_reports, list):
            return False
        hurdle = [
            item
            for item in latency_reports
            if isinstance(item, dict) and item.get("latency_ms") == 500
        ]
        if len(hurdle) != 1 or not isinstance(hurdle[0].get("economic_gates"), dict):
            return False
        gates = hurdle[0]["economic_gates"]
        return bool(gates) and all(value is True for value in gates.values())
    except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return False


def collect_h1_campaign(
    campaign_root: Path,
    *,
    config_path: Path,
    resume: bool,
    stop_requested: Callable[[], bool] = lambda: False,
    progress: Callable[[Mapping[str, object]], None] = lambda _health: None,
) -> dict[str, object]:
    """Run or resume one public-only campaign against its immutable manifest."""

    config = H1PolicyConfig.from_path(config_path)
    campaign = _load_campaign(campaign_root, config)
    starts = _parse_utc(str(campaign["starts_at_utc"]))
    ends = _parse_utc(str(campaign["ends_at_utc"]))
    now = _utc_now()
    if now < starts:
        raise ValueError("campaign collection cannot start before its frozen UTC start")
    if now >= ends:
        raise ValueError("campaign collection window is already closed")
    raw_root = campaign_root / "raw"
    existing = raw_root.exists()
    if existing and not resume:
        raise FileExistsError("existing campaign raw root requires explicit --resume")
    if not existing and resume:
        raise FileNotFoundError("--resume requires an existing campaign raw root")
    runner = config.body["runner"]
    assert isinstance(runner, dict)
    collection_id = str(campaign["campaign_id"])
    writer = ResearchSegmentWriter(
        raw_root,
        collection_id=collection_id,
        max_segment_bytes=int(runner["segment_bytes"]),
        rotation_seconds=float(runner["rotation_seconds"]),
        max_total_bytes=int(runner["maximum_raw_bytes"]),
    )
    counters = _Counters()
    if writer.manifest_sha256 is not None:
        prior = ResearchSegmentReader(
            raw_root, manifest_sha256=writer.manifest_sha256
        ).replay()
        for envelope in prior:
            counters.observe(envelope)
        counters.reconnects = sum(int(item.state.reconnect) for item in prior)
    factory = SessionEnvelopeFactory(
        venue=Venue.HYPERLIQUID,
        collector_identity="hyperlab-h1-prospective-campaign-v1",
        session_identity=f"{collection_id}-resume-{writer.frame_count}",
        source_metadata_version=HYPERLIQUID_METADATA_VERSION,
        provenance=CaptureProvenance(
            collection_id,
            HYPERLIQUID_PUBLIC_HTTP_URL,
            "PUBLIC_HTTP",
        ),
        initial_arrival_sequence=writer.frame_count,
    )
    if writer.frame_count:
        factory.begin_reconnect()
    coins = tuple(item.split(":", 2)[1] for item in config.instruments)
    probe_config = ProbeConfig(
        output_root=campaign_root / "unused-probe-root",
        venue=Venue.HYPERLIQUID,
        feeds=tuple(str(item) for item in runner["feeds"]),
        instruments=coins,
        census_limit=0,
        duration_seconds=300,
        max_bytes=int(runner["maximum_raw_bytes"]),
        max_segment_bytes=int(runner["segment_bytes"]),
        rotation_seconds=float(runner["rotation_seconds"]),
        progress_interval_seconds=float(runner["progress_interval_seconds"]),
        collection_id=collection_id,
    )
    started_mono = time.monotonic()
    deadline = started_mono + max(0.0, (ends - now).total_seconds())
    state_path = campaign_root / "state" / "health.json"
    threshold_report_path = campaign_root / "state" / "verified-threshold-report.json"

    def publish(frame_count: int, *, terminal: str = "RUNNING", error: str | None = None) -> dict[str, object]:
        health: dict[str, object] = {
            "boundary": CAMPAIGN_BOUNDARY,
            "campaign_id": collection_id,
            "elapsed_ms_this_process": int((time.monotonic() - started_mono) * 1_000),
            "error": error,
            "frames": frame_count,
            "gaps": counters.gaps,
            "manifest_sha256": writer.manifest_sha256,
            "monitoring": str(state_path),
            "queue_high_water": counters.queue_high_water,
            "reconnects": counters.reconnects,
            "segments": writer.segment_count,
            "stored_bytes": writer.stored_segment_bytes,
            "terminal_health": terminal,
            "verified_threshold_report": str(threshold_report_path),
        }
        _atomic_json(state_path, health)
        progress(health)
        return health

    publish(writer.frame_count)
    session: Any | None = None
    terminal = "COMPLETE_COLLECTION_WINDOW"
    error: str | None = None

    def publish_progress(frame_count: int) -> None:
        publish(frame_count)

    threshold_candidate = False

    def campaign_stop_requested() -> bool:
        nonlocal threshold_candidate
        threshold_candidate = _verified_threshold_stop(
            campaign_root,
            config=config,
            campaign_start=starts,
            raw_root=raw_root,
        )
        return stop_requested() or threshold_candidate

    try:
        session = _default_http_session()
        _hyperliquid_probe(
            probe_config,
            factory=factory,
            writer=writer,
            counters=counters,
            deadline=deadline,
            stop_requested=campaign_stop_requested,
            progress=publish_progress,
            session=session,
        )
        if stop_requested():
            terminal = "INTERRUPTED_RECOVERABLE"
        elif threshold_candidate:
            terminal = "THRESHOLD_CANDIDATE_PENDING_FINAL_PREFIX_CHECK"
    except KeyboardInterrupt:
        terminal = "INTERRUPTED_RECOVERABLE"
    except ResearchDataCapacityError as caught:
        terminal = "MAX_BYTES_REACHED"
        error = str(caught)
    except (ConnectionError, OSError, TimeoutError) as caught:
        terminal = "PUBLIC_SOURCE_UNAVAILABLE_RECOVERABLE"
        error = f"{type(caught).__name__}:{caught}"
    except ValueError as caught:
        terminal = "PUBLIC_SOURCE_INVALID_FAIL_CLOSED"
        error = f"{type(caught).__name__}:{caught}"
    except BaseException:
        writer.abort()
        raise
    finally:
        if session is not None:
            session.close()
    manifest = writer.close()
    if threshold_candidate and manifest is not None and terminal != "INTERRUPTED_RECOVERABLE":
        try:
            final_report = replay_h1_research_manifest(
                raw_root,
                manifest.manifest_sha256,
                config=config,
            )
            hurdle = next(
                item
                for item in final_report.latency_reports
                if item.latency_ms == config.primary_hurdle_latency_ms
            )
            if (
                not final_report.synthetic
                and hurdle.economic_gates
                and all(hurdle.economic_gates.values())
            ):
                _atomic_bytes(threshold_report_path, final_report.canonical_bytes() + b"\n")
                terminal = "COMPLETE_VERIFIED_THRESHOLDS"
            else:
                terminal = "THRESHOLD_CANDIDATE_NOT_FINAL_RESUME_REQUIRED"
        except (OSError, ValueError) as caught:
            terminal = "FINAL_THRESHOLD_REPLAY_INVALID_FAIL_CLOSED"
            error = f"{type(caught).__name__}:{caught}"
    health = publish(writer.frame_count, terminal=terminal, error=error)
    if manifest is not None:
        health["manifest_sha256"] = manifest.manifest_sha256
        health["raw_root_sha256"] = manifest.root_sha256
        _atomic_json(state_path, health)
    return health


__all__ = [
    "CAMPAIGN_BOUNDARY",
    "H1CampaignPreparation",
    "collect_h1_campaign",
    "prepare_h1_campaign",
]
