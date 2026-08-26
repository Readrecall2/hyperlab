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
from .lighter import LIGHTER_DOCUMENTARY_CONTRACT, lighter_market_census
from .segments import ResearchSegmentReader

LIGHTER_GREEN = "LIGHTER_PUBLIC_PROBE_V1_GREEN"
LIGHTER_UNAVAILABLE = "LIGHTER_PUBLIC_SOURCE_UNAVAILABLE_BOUNDED"
LIGHTER_REPORT_NAME = "lighter-public-probe-v1.json"
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


__all__ = [
    "LIGHTER_GREEN",
    "LIGHTER_REPORT_NAME",
    "LIGHTER_UNAVAILABLE",
    "build_lighter_probe_report",
    "write_lighter_probe_report",
]
