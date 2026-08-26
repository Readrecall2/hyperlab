from __future__ import annotations

import hashlib
import json
import os
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from hyperlab.ghost.models import VenueHealth

from .canonical import CanonicalValue, canonical_json_bytes, canonical_value
from .envelope import PublicDataEnvelope
from .lighter import (
    LIGHTER_DOCUMENTARY_CONTRACT,
    LIGHTER_PUBLIC_READONLY_WEBSOCKET_URL,
    LIGHTER_PUBLIC_WEBSOCKET_URL,
    lighter_market_census,
)
from .segments import ResearchSegmentReader

LIGHTER_GREEN = "LIGHTER_PUBLIC_PROBE_V1_GREEN"
LIGHTER_UNAVAILABLE = "LIGHTER_PUBLIC_SOURCE_UNAVAILABLE_BOUNDED"
LIGHTER_REPORT_NAME = "lighter-public-probe-v1.json"
LIGHTER_ACCESS_COMPLETION_REPORT_NAME = (
    "lighter-official-public-access-completion-v1.json"
)
LIGHTER_OFFICIAL_WS_PUBLIC_ACCESS_GREEN = "LIGHTER_OFFICIAL_WS_PUBLIC_ACCESS_GREEN"
LIGHTER_OFFICIAL_READONLY_WS_ACCESS_GREEN = (
    "LIGHTER_OFFICIAL_READONLY_WS_ACCESS_GREEN"
)
LIGHTER_PUBLIC_ACCESS_EXHAUSTED_OFFICIAL_PATHS = (
    "LIGHTER_PUBLIC_ACCESS_EXHAUSTED_OFFICIAL_PATHS"
)
LIGHTER_OFFICIAL_WS_ACCESS_BLOCKED_INTEGRITY = (
    "LIGHTER_OFFICIAL_WS_ACCESS_BLOCKED_INTEGRITY"
)
_SUCCESS_TERMINALS = {
    "COMPLETE",
    "MAX_BYTES_REACHED",
    "MAX_DURATION_REACHED",
    "MAX_FRAMES_REACHED",
    "MAX_SEGMENTS_REACHED",
}
_REQUIRED_PUBLIC_FEEDS = ("metadata", "order_book", "ticker", "market_stats", "trades")


def _read_object(path: Path) -> Mapping[str, Any]:
    try:
        decoded = json.loads(
            path.read_text(encoding="utf-8"),
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"invalid Lighter probe JSON: {path}") from error
    if not isinstance(decoded, Mapping):
        raise ValueError(f"Lighter probe JSON must be an object: {path}")
    return decoded


def _required_int(value: object, *, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _required_text(value: object, *, label: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{label} must be non-empty text")
    return value


def _optional_hash(value: object, *, label: str) -> str | None:
    if value is None:
        return None
    text = _required_text(value, label=label)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise ValueError(f"{label} must be a lowercase SHA-256 or null")
    return text


def _ns_as_ms(value: int) -> str:
    return format(Decimal(value) / Decimal(1_000_000), "f")


def _nearest_rank(values: Sequence[int], numerator: int, denominator: int = 100) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, (len(ordered) * numerator + denominator - 1) // denominator)
    return ordered[rank - 1]


def _distribution(values: Sequence[int]) -> dict[str, CanonicalValue]:
    if not values:
        return {
            "count": 0,
            "max_ms": None,
            "min_ms": None,
            "p50_ms": None,
            "p95_ms": None,
            "p99_ms": None,
            "percentile_method": "NEAREST_RANK",
        }
    return {
        "count": len(values),
        "max_ms": _ns_as_ms(max(values)),
        "min_ms": _ns_as_ms(min(values)),
        "p50_ms": _ns_as_ms(_nearest_rank(values, 50) or 0),
        "p95_ms": _ns_as_ms(_nearest_rank(values, 95) or 0),
        "p99_ms": _ns_as_ms(_nearest_rank(values, 99) or 0),
        "percentile_method": "NEAREST_RANK",
    }


def _atomic_json(path: Path, body: Mapping[str, object]) -> None:
    value = canonical_json_bytes(body)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("xb") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def build_lighter_probe_report(output_root: Path) -> dict[str, CanonicalValue]:
    reports_root = output_root / "reports"
    result = _read_object(reports_root / "result.json")
    config = _read_object(reports_root / "probe-config.json")
    if result.get("venue") != "lighter" or config.get("venue") != "lighter":
        raise ValueError("Lighter report requires an exact Lighter probe output")
    terminal = _required_text(result.get("terminal_health"), label="terminal health")
    frames = _required_int(result.get("frames"), label="frame count")
    segments = _required_int(result.get("segments"), label="segment count")
    stored_bytes = _required_int(result.get("bytes"), label="stored byte count")
    gaps = _required_int(result.get("gaps"), label="gap count")
    duplicates = _required_int(result.get("duplicates"), label="duplicate count")
    reconnects = _required_int(result.get("reconnects"), label="reconnect count")
    manifest_sha256 = _optional_hash(result.get("manifest_sha256"), label="manifest hash")
    root_sha256 = _optional_hash(result.get("root_sha256"), label="raw root hash")

    envelopes: tuple[PublicDataEnvelope, ...] = ()
    recovery_status = "NOT_AVAILABLE_NO_AUTHENTICATED_MANIFEST"
    if manifest_sha256 is not None:
        reader = ResearchSegmentReader(
            output_root / "raw", manifest_sha256=manifest_sha256
        )
        envelopes = reader.replay()
        if len(envelopes) != frames:
            raise ValueError("offline recovery frame count differs from terminal report")
        if len(reader.manifest.segments) != segments:
            raise ValueError("offline recovery segment count differs from terminal report")
        if reader.manifest.stored_segment_bytes != stored_bytes:
            raise ValueError("offline recovery byte count differs from terminal report")
        if reader.manifest.root_sha256 != root_sha256:
            raise ValueError("offline recovery root differs from terminal report")
        recovery_status = "PASS_EXPLICIT_MANIFEST_FULL_REPLAY"
    elif frames or segments or stored_bytes or root_sha256 is not None:
        raise ValueError("probe report claims raw evidence without a manifest")

    feed_counts: Counter[str] = Counter(envelope.feed_type for envelope in envelopes)
    replay_gaps = sum(int(envelope.state.gap_detected) for envelope in envelopes)
    replay_duplicates = sum(int(envelope.state.duplicate) for envelope in envelopes)
    replay_reconnect_boundaries = sum(int(envelope.state.reconnect) for envelope in envelopes)
    if replay_gaps != gaps:
        raise ValueError("offline recovery gap count differs from terminal report")
    if replay_duplicates != duplicates:
        raise ValueError("offline recovery duplicate count differs from terminal report")
    if replay_reconnect_boundaries > reconnects:
        raise ValueError("offline recovery reconnect boundaries exceed terminal report")
    market_counts: Counter[str] = Counter(
        envelope.market_id
        for envelope in envelopes
        if envelope.market_id is not None and envelope.market_id.startswith("LIGHTER:MARKET:")
    )
    connection_epochs: list[str] = sorted(
        {envelope.session_identity for envelope in envelopes}
    )
    source_deltas_by_feed: dict[str, list[int]] = defaultdict(list)
    source_deltas: list[int] = []
    interarrival_by_feed: dict[str, list[int]] = defaultdict(list)
    previous_monotonic: dict[tuple[str, str, str], int] = {}
    receive_values = [envelope.receive_timestamp_utc_ns for envelope in envelopes]
    source_values = [
        envelope.source_timestamp_ns
        for envelope in envelopes
        if envelope.source_timestamp_ns is not None
    ]
    for envelope in envelopes:
        if envelope.source_timestamp_ns is not None:
            delta = envelope.receive_timestamp_utc_ns - envelope.source_timestamp_ns
            source_deltas.append(delta)
            source_deltas_by_feed[envelope.feed_type].append(delta)
        key = (
            envelope.session_identity,
            envelope.feed_type,
            envelope.market_id or envelope.instrument_id or "GLOBAL",
        )
        previous = previous_monotonic.get(key)
        if previous is not None:
            interarrival_by_feed[envelope.feed_type].append(
                envelope.receive_monotonic_ns - previous
            )
        previous_monotonic[key] = envelope.receive_monotonic_ns

    metadata_rows: dict[int, dict[str, CanonicalValue]] = {}
    for envelope in envelopes:
        if envelope.feed_type != "metadata":
            continue
        for market in lighter_market_census(envelope.raw_payload, limit=100):
            metadata_rows[market.market_id] = market.to_dict()
    observed_ids: list[int] = sorted(
        int(market_id.rsplit(":", 1)[-1]) for market_id in market_counts
    )
    observed_metadata = [
        metadata_rows[market_id] for market_id in observed_ids if market_id in metadata_rows
    ]

    scenario_health: dict[str, object] = {}
    for threshold_ms in (100, 250, 500, 1000):
        counts: Counter[str] = Counter()
        threshold_ns = threshold_ms * 1_000_000
        for envelope in envelopes:
            if envelope.state.gap_detected:
                counts[VenueHealth.GAP.value] += 1
            elif envelope.state.reconnect:
                counts[VenueHealth.RECONNECT.value] += 1
            elif envelope.source_timestamp_ns is None:
                counts["UNASSESSED_NO_SOURCE_TIMESTAMP"] += 1
            else:
                delta = envelope.receive_timestamp_utc_ns - envelope.source_timestamp_ns
                if delta < 0:
                    counts["CLOCK_UNCERTAIN_SOURCE_AHEAD"] += 1
                elif delta <= threshold_ns:
                    counts[VenueHealth.FRESH.value] += 1
                else:
                    counts[VenueHealth.STALE.value] += 1
        scenario_health[str(threshold_ms)] = dict(sorted(counts.items()))

    requested_feeds_raw = config.get("feeds")
    if not isinstance(requested_feeds_raw, list) or any(
        type(item) is not str for item in requested_feeds_raw
    ):
        raise ValueError("probe config feeds must be an array of text")
    requested_feeds = tuple(cast(list[str], requested_feeds_raw))
    channel_access: dict[str, object] = {
        feed: {
            "accessible_without_auth_in_this_probe": feed_counts[feed] > 0,
            "frames": feed_counts[feed],
            "requested": feed in requested_feeds,
        }
        for feed in _REQUIRED_PUBLIC_FEEDS
    }
    all_required_accessible = all(feed_counts[feed] > 0 for feed in _REQUIRED_PUBLIC_FEEDS)
    recovered = recovery_status == "PASS_EXPLICIT_MANIFEST_FULL_REPLAY"
    green = terminal in _SUCCESS_TERMINALS and gaps == 0 and recovered and all_required_accessible
    verdict = LIGHTER_GREEN if green else LIGHTER_UNAVAILABLE
    contract_bytes = canonical_json_bytes(LIGHTER_DOCUMENTARY_CONTRACT)
    raw_error = result.get("error")
    if raw_error is not None and type(raw_error) is not str:
        raise ValueError("probe error must be null or text")
    raw_limitations_value = result.get("limitations")
    if not isinstance(raw_limitations_value, list) or any(
        type(item) is not str for item in raw_limitations_value
    ):
        raise ValueError("probe limitations must be an array of text")
    raw_limitations = cast(list[str], raw_limitations_value)

    report_object: dict[str, object] = {
        "access": {
            "channels": channel_access,
            "http_endpoint": "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks",
            "websocket_endpoint": "wss://mainnet.zklighter.elliot.ai/stream",
        },
        "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        "capture": {
            "bytes": stored_bytes,
            "collection_id": _required_text(result.get("collection_id"), label="collection id"),
            "duplicates": duplicates,
            "elapsed_ms": _required_int(result.get("elapsed_ms"), label="elapsed time"),
            "frames": frames,
            "gaps": gaps,
            "reconnects": reconnects,
            "segments": segments,
            "terminal_health": terminal,
        },
        "connection_epochs": connection_epochs,
        "differences_from_hyperliquid": [
            "LIGHTER_ORDER_BOOK_IS_DOCUMENTED_AS_INITIAL_SNAPSHOT_THEN_STATE_CHANGES",
            "LIGHTER_EXPOSES_MATCHING_ENGINE_NONCE_AND_BEGIN_NONCE_CONTINUITY",
            "LIGHTER_OFFSET_IS_API_SERVER_LOCAL_AND_NOT_A_CONTIGUOUS_EXCHANGE_SEQUENCE",
            "HYPERLIQUID_RESEARCH_DATA_PLANE_TREATS_L2BOOK_AS_AGGREGATED_SNAPSHOTS_WITHOUT_INVENTED_SEQUENCE",
            "LIGHTER_ACCOUNT_TIER_FEES_AND_DELAYS_ARE_DOCUMENTARY_SCENARIOS_ONLY",
        ],
        "documentary_contract": {
            "account_tier_access_observed": False,
            "contract_sha256": hashlib.sha256(contract_bytes).hexdigest(),
            "frozen_contract": LIGHTER_DOCUMENTARY_CONTRACT,
            "metadata_version": LIGHTER_DOCUMENTARY_CONTRACT["metadata_version"],
            "scope": LIGHTER_DOCUMENTARY_CONTRACT["scope"],
            "versioned_comparable_scenarios_ms": LIGHTER_DOCUMENTARY_CONTRACT[
                "comparable_scenarios_ms"
            ],
        },
        "error": raw_error,
        "feed_counts": dict(sorted(feed_counts.items())),
        "freshness": {
            "classification": "SOURCE_TO_LOCAL_RECEIVE_DELTA_NOT_NETWORK_ONLY_LATENCY",
            "clock_sync": "UNCALIBRATED",
            "health_by_versioned_threshold_ms": scenario_health,
            "source_to_receive_delta_all": _distribution(source_deltas),
            "source_to_receive_delta_by_feed": {
                feed: _distribution(values)
                for feed, values in sorted(source_deltas_by_feed.items())
            },
        },
        "interarrival": {
            "basis": "LOCAL_MONOTONIC_WITHIN_CONNECTION_EPOCH_FEED_MARKET",
            "by_feed": {
                feed: _distribution(values)
                for feed, values in sorted(interarrival_by_feed.items())
            },
        },
        "limitations": [
            *raw_limitations,
            "ACCOUNT_AND_TIER_ACCESS_NOT_REQUESTED_OR_OBSERVED",
            "DOCUMENTED_FEES_AND_ORDER_DELAYS_ARE_NOT_MEASURED_ACCOUNT_VALUES",
            "LOCAL_CLOCK_OFFSET_AND_UNCERTAINTY_ARE_NOT_CALIBRATED",
            "BOUNDED_PUBLIC_PROBE_IS_NOT_ALPHA_CAPACITY_OR_PROFITABILITY_EVIDENCE",
        ],
        "markets_observed": [
            {"frame_count": market_counts[f"LIGHTER:MARKET:{market_id}"], "market_id": market_id}
            for market_id in observed_ids
        ],
        "metadata_and_public_fees_observed": observed_metadata,
        "raw_evidence": {
            "manifest_sha256": manifest_sha256,
            "offline_recovery": recovery_status,
            "replay_duplicate_count": replay_duplicates,
            "replay_gap_count": replay_gaps,
            "replay_reconnect_boundaries": replay_reconnect_boundaries,
            "root_sha256": root_sha256,
        },
        "schema_version": 1,
        "temporal_coverage": {
            "receive_timestamp_max_ns": None if not receive_values else max(receive_values),
            "receive_timestamp_min_ns": None if not receive_values else min(receive_values),
            "source_timestamp_max_ns": None if not source_values else max(source_values),
            "source_timestamp_min_ns": None if not source_values else min(source_values),
        },
        "verdict": verdict,
        "verdict_scope": "SUITABILITY_FOR_FUTURE_BOUNDED_GHOST_STUDY_ONLY",
    }
    report = canonical_value(report_object)
    if not isinstance(report, dict):
        raise AssertionError("canonical Lighter report must remain an object")
    return report


def write_lighter_probe_report(output_root: Path) -> dict[str, CanonicalValue]:
    report = build_lighter_probe_report(output_root)
    _atomic_json(output_root / "reports" / LIGHTER_REPORT_NAME, report)
    return report


def _access_attempts(result: Mapping[str, Any]) -> list[dict[str, CanonicalValue]]:
    raw_attempts = result.get("connection_attempts")
    if not isinstance(raw_attempts, list) or len(raw_attempts) > 2:
        raise ValueError("Lighter access completion requires at most two handshake attempts")
    expected = (
        ("normal", LIGHTER_PUBLIC_WEBSOCKET_URL),
        ("readonly", LIGHTER_PUBLIC_READONLY_WEBSOCKET_URL),
    )
    attempts: list[dict[str, CanonicalValue]] = []
    for index, raw in enumerate(raw_attempts):
        if not isinstance(raw, Mapping):
            raise ValueError("Lighter handshake attempt must be an object")
        mode, url = expected[index]
        if raw.get("mode") != mode or raw.get("logical_url") != url:
            raise ValueError("Lighter handshake attempts violated the official URL order")
        duration_ms = _required_int(raw.get("duration_ms"), label="handshake duration")
        handshake_result = _required_text(
            raw.get("handshake_result"), label="handshake result"
        )
        if handshake_result not in {"HTTP_101", "FAILED_BEFORE_COLLECTION"}:
            raise ValueError("Lighter handshake result is not recognized")
        status = raw.get("http_status")
        if status is not None and type(status) is not int:
            raise ValueError("Lighter handshake HTTP status must be integer or null")
        error_type = raw.get("error_type")
        error_message = raw.get("error_message")
        if any(value is not None and type(value) is not str for value in (error_type, error_message)):
            raise ValueError("Lighter handshake error fields must be text or null")
        if handshake_result == "HTTP_101":
            if status != 101 or error_type is not None or error_message is not None:
                raise ValueError("successful Lighter handshake evidence is inconsistent")
        elif not error_type or not error_message:
            raise ValueError("failed Lighter handshake must preserve its exact available error")
        attempts.append(
            {
                "duration_ms": duration_ms,
                "error_message": cast(str | None, error_message),
                "error_type": cast(str | None, error_type),
                "handshake_result": handshake_result,
                "http_status": status,
                "logical_url": url,
                "mode": mode,
            }
        )
    if len(attempts) == 2 and attempts[0]["handshake_result"] != "FAILED_BEFORE_COLLECTION":
        raise ValueError("readonly Lighter handshake is allowed only after normal handshake failure")
    if any(item["handshake_result"] == "HTTP_101" for item in attempts[:-1]):
        raise ValueError("Lighter access completion continued after a successful handshake")
    return attempts


def build_lighter_access_completion_report(
    output_root: Path,
) -> dict[str, CanonicalValue]:
    """Authenticate and classify the one-shot official WebSocket completion probe."""

    base = cast(dict[str, Any], build_lighter_probe_report(output_root))
    result = _read_object(output_root / "reports" / "result.json")
    config = _read_object(output_root / "reports" / "probe-config.json")
    attempts = _access_attempts(result)
    if config.get("instruments") != ["0"]:
        raise ValueError("Lighter access completion requires documented market_index 0")
    if config.get("feeds") != ["order_book", "ticker", "market_stats", "trades"]:
        raise ValueError("Lighter access completion requires the four documented public feeds")

    frames = _required_int(result.get("frames"), label="frame count")
    gaps = _required_int(result.get("gaps"), label="gap count")
    reconnects = _required_int(result.get("reconnects"), label="reconnect count")
    terminal = _required_text(result.get("terminal_health"), label="terminal health")
    successful = [item for item in attempts if item["handshake_result"] == "HTTP_101"]
    if frames and not successful:
        raise ValueError("Lighter raw frames require a successful recorded handshake")

    feed_counts_raw = base.get("feed_counts")
    if not isinstance(feed_counts_raw, Mapping):
        raise ValueError("Lighter recovered feed counts must be an object")
    websocket_feeds = ("order_book", "ticker", "market_stats", "trades")
    all_public_feeds_observed = all(
        type(feed_counts_raw.get(feed)) is int and cast(int, feed_counts_raw[feed]) > 0
        for feed in websocket_feeds
    )
    raw_evidence = base.get("raw_evidence")
    if not isinstance(raw_evidence, Mapping):
        raise ValueError("Lighter recovered raw evidence must be an object")
    recovered = raw_evidence.get("offline_recovery") == "PASS_EXPLICIT_MANIFEST_FULL_REPLAY"
    green = (
        len(successful) == 1
        and terminal in _SUCCESS_TERMINALS
        and frames > 0
        and gaps == 0
        and reconnects == 0
        and recovered
        and all_public_feeds_observed
    )
    exhausted = (
        len(attempts) == 2
        and not successful
        and all(item["handshake_result"] == "FAILED_BEFORE_COLLECTION" for item in attempts)
        and terminal == "PUBLIC_SOURCE_UNAVAILABLE"
        and frames == 0
        and result.get("manifest_sha256") is None
    )
    if green:
        verdict = (
            LIGHTER_OFFICIAL_WS_PUBLIC_ACCESS_GREEN
            if successful[0]["mode"] == "normal"
            else LIGHTER_OFFICIAL_READONLY_WS_ACCESS_GREEN
        )
    elif exhausted:
        verdict = LIGHTER_PUBLIC_ACCESS_EXHAUSTED_OFFICIAL_PATHS
    else:
        verdict = LIGHTER_OFFICIAL_WS_ACCESS_BLOCKED_INTEGRITY

    observed_symbols: set[str] = set()
    manifest_sha256 = result.get("manifest_sha256")
    if type(manifest_sha256) is str:
        reader = ResearchSegmentReader(
            output_root / "raw", manifest_sha256=manifest_sha256
        )
        for envelope in reader.iter_envelopes():
            try:
                payload = json.loads(envelope.raw_payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, Mapping):
                continue
            ticker = payload.get("ticker")
            stats = payload.get("market_stats")
            candidates = (
                ticker.get("s") if isinstance(ticker, Mapping) else None,
                stats.get("symbol") if isinstance(stats, Mapping) else None,
            )
            observed_symbols.update(
                value for value in candidates if type(value) is str and value
            )

    contract = base.get("documentary_contract")
    if not isinstance(contract, Mapping):
        raise ValueError("Lighter documentary contract must be an object")
    channels = {
        feed: {
            "frames": cast(int, feed_counts_raw.get(feed, 0)),
            "observed_without_auth": cast(int, feed_counts_raw.get(feed, 0)) > 0,
            "requested": True,
        }
        for feed in websocket_feeds
    }
    base["access"] = {
        "channels": channels,
        "handshake_attempts": attempts,
        "proxy_policy": "DIRECT_ONLY_ENVIRONMENT_PROXY_DISABLED",
        "rest_census": {
            "attempted_in_completion": False,
            "historical_evidence_preserved_at": "docs/evidence/lighter-public-probe-v1",
            "historical_scope": "GET_orderBooks_HTTP_403_FROM_PRIOR_WINDOWS_PATH_ONLY",
        },
    }
    base["documentation_observation_separation"] = {
        "documentation": {
            "contract_sha256": contract.get("contract_sha256"),
            "network_observation": False,
            "retrieved_on": "2026-08-26",
            "url": "https://apidocs.lighter.xyz/docs/websocket-reference",
        },
        "observations": {
            "account_or_tier_observed": False,
            "handshake_attempts": attempts,
            "metadata_rest_census_observed": False,
            "raw_frames_observed": frames,
        },
    }
    base["metadata_precision_and_fees"] = {
        "market_index": 0,
        "maker_fee": None,
        "market_type": None,
        "min_base_amount": None,
        "min_quote_amount": None,
        "price_precision": None,
        "size_precision": None,
        "status": "UNKNOWN_NOT_OBSERVED_NO_REST_CENSUS",
        "taker_fee": None,
    }
    base["metadata_and_public_fees_observed"] = []
    base["report_kind"] = "LIGHTER_OFFICIAL_PUBLIC_ACCESS_COMPLETION_V1"
    base["schema_version"] = 2
    base["symbols_observed_in_public_payloads"] = sorted(observed_symbols)
    base["future_ghost_eligibility"] = (
        "ELIGIBLE_FOR_FUTURE_BOUNDED_GHOST_DATA_QUALITY_STUDY_NOT_ALPHA"
        if green
        else "NOT_ELIGIBLE_FROM_THIS_COMPLETION_EVIDENCE"
    )
    limitations = base.get("limitations")
    if not isinstance(limitations, list):
        raise ValueError("Lighter report limitations must be an array")
    base["limitations"] = [
        *limitations,
        "PRIOR_REST_403_NOT_REEXECUTED",
        "MARKET_INDEX_0_FROM_OFFICIAL_DOCUMENTATION_EXAMPLE",
        "MARKET_METADATA_PRECISION_AND_FEES_UNKNOWN_WITHOUT_REST_CENSUS",
        "NO_AUTOMATIC_RETRY_OR_RECONNECT_IN_ACCESS_COMPLETION",
    ]
    base["verdict"] = verdict
    base["verdict_scope"] = "OFFICIAL_PUBLIC_WEBSOCKET_ACCESS_AND_BOUNDED_RECOVERY_ONLY"
    report = canonical_value(base)
    if not isinstance(report, dict):
        raise AssertionError("canonical Lighter completion report must remain an object")
    return report


def write_lighter_access_completion_report(
    output_root: Path,
) -> dict[str, CanonicalValue]:
    report = build_lighter_access_completion_report(output_root)
    _atomic_json(
        output_root / "reports" / LIGHTER_ACCESS_COMPLETION_REPORT_NAME,
        report,
    )
    return report


__all__ = [
    "LIGHTER_ACCESS_COMPLETION_REPORT_NAME",
    "LIGHTER_GREEN",
    "LIGHTER_OFFICIAL_READONLY_WS_ACCESS_GREEN",
    "LIGHTER_OFFICIAL_WS_ACCESS_BLOCKED_INTEGRITY",
    "LIGHTER_OFFICIAL_WS_PUBLIC_ACCESS_GREEN",
    "LIGHTER_PUBLIC_ACCESS_EXHAUSTED_OFFICIAL_PATHS",
    "LIGHTER_REPORT_NAME",
    "LIGHTER_UNAVAILABLE",
    "build_lighter_access_completion_report",
    "build_lighter_probe_report",
    "write_lighter_access_completion_report",
    "write_lighter_probe_report",
]
