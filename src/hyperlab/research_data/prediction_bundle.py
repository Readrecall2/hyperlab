from __future__ import annotations

import hashlib
import os
import shutil
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from itertools import pairwise
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, cast
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from hyperlab.ghost.prediction import (
    MakerAggressorEvidence,
    PredictionCampaignGhostReplay,
    PredictionCampaignReplayReport,
    PredictionGhostReplay,
    PredictionLegEvidenceKey,
    PredictionOpportunityStatus,
    PredictionSettlementEvidence,
    PredictionSettlementState,
)

from .canonical import CanonicalValue, canonical_json_bytes, canonical_value, decode_canonical_json
from .envelope import PublicDataEnvelope, Venue
from .prediction import SemanticCatalog
from .prediction_candidate import (
    BOUNDARY,
    INSUFFICIENT_PUBLIC_CORPUS,
    CandidatePreregistration,
    FeeModel,
    PredictionCampaignEvaluationReport,
    PredictionCollectionBinding,
    PredictionFeeSchedule,
    PredictionPointInTimeDataset,
    PredictionReplaySeal,
    PredictionTickBand,
    PredictionTickGrid,
    PredictionTradeDataset,
    PublicSourceStatus,
    build_kalshi_unknown_fee_schedule_from_raw,
    build_polymarket_fee_schedule_from_raw,
    build_prediction_dataset,
    build_prediction_replay_seal,
    build_prediction_split_plan,
    build_prediction_tick_grid_from_raw,
    build_prediction_trade_dataset,
    evaluate_preregistered,
    prediction_prospective_shard_ordinal,
    validate_prediction_campaign_manifest,
    verify_prediction_collection_plan_payload,
)
from .prediction_contracts import (
    EvidenceClassification,
    OfficialPublicContract,
    PredictionIdentityGraph,
    build_prediction_graph_from_raw,
    build_prediction_semantic_catalog_from_graphs,
)
from .prediction_evidence import (
    PredictionRawEvidenceIndex,
    PredictionRawRecordRef,
    prediction_raw_record_ref,
    prediction_raw_records,
)
from .prediction_time import prediction_rfc3339_to_ns
from .segments import (
    MANIFEST_SUFFIX,
    SEGMENT_SUFFIX,
    ResearchSegmentReader,
    decode_manifest,
)

MODEL_VERSION = "PREDICTION_RESEARCH_BUNDLE_V1"
SYNTHETIC_SOURCE_STATUS = "SYNTHETIC_FIXTURE_MECHANISM_ONLY"
UNBOUND_AVAILABILITY_OBSERVATION = "UNBOUND_PUBLIC_AVAILABILITY_OBSERVATION"
CAMPAIGN_BOUND_UNAVAILABILITY_RECEIPT = "CAMPAIGN_BOUND_PUBLIC_UNAVAILABILITY_RECEIPT"
CAMPAIGN_BOUND_EXCLUDED_SLOT_RECEIPT = "CAMPAIGN_BOUND_EXPLICIT_GAP_EXCLUDED_FROM_ECONOMICS"
_ADMISSIBLE_RAW_TERMINALS = {
    "COMPLETE",
    "MAX_BYTES_REACHED",
    "MAX_DURATION_REACHED",
    "MAX_FRAMES_REACHED",
    "MAX_NETWORK_CALLS_REACHED",
    "MAX_SEGMENTS_REACHED",
    "PUBLIC_SOURCE_UNAVAILABLE_RECOVERED",
}
_EXCLUDED_SLOT_TERMINALS = {
    "BACKPRESSURE_LIMIT_REACHED",
    "CONTINUITY_BROKEN_FROZEN",
    "CONTINUITY_UNKNOWN_AFTER_RECONNECT_FROZEN",
    "INTERRUPTED_RECOVERABLE",
    "INTERRUPTED_RECOVERED",
    "PUBLIC_SOURCE_INVALID",
    "PUBLIC_SOURCE_UNAVAILABLE",
    "RECOVERED_AFTER_PROCESS_ERROR",
}
_ZERO = Decimal("0")
_ONE = Decimal("1")
_TERMINAL_ERROR_MAX_UTF8_BYTES = 2_048


@dataclass(frozen=True, slots=True)
class PredictionBundleSource:
    raw_root: Path
    manifest_sha256: str
    collection_root: Path | None = None

    @classmethod
    def from_probe_output(cls, collection_root: Path) -> PredictionBundleSource:
        binding = PredictionCollectionBinding.from_probe_output(collection_root)
        assert binding.raw_manifest_sha256 is not None
        return cls(
            raw_root=collection_root / "raw",
            manifest_sha256=binding.raw_manifest_sha256,
            collection_root=collection_root,
        )


@dataclass(frozen=True, slots=True)
class PredictionPublicSourceInvalidReceipt:
    """Authenticated receipt metadata only; never sufficient for economic admission."""

    binding: PredictionCollectionBinding
    result_payload: Mapping[str, CanonicalValue]
    terminal_result_sha256: str
    terminal_error: str
    frame_count: int
    byte_count: int
    segment_count: int
    elapsed_ms: int
    network_calls: int
    raw_manifest_sha256: str
    raw_root_sha256: str
    classification: str = CAMPAIGN_BOUND_EXCLUDED_SLOT_RECEIPT
    source_usable: bool = False
    economic_eligible: bool = False

    @classmethod
    def from_report_bytes(
        cls,
        *,
        probe_config_raw: bytes,
        terminal_result_raw: bytes,
    ) -> PredictionPublicSourceInvalidReceipt:
        config_value = decode_canonical_json(probe_config_raw, require_canonical=True)
        if not isinstance(config_value, Mapping):
            raise ValueError("prediction invalid-source probe config must be an object")
        binding = PredictionCollectionBinding.from_bytes(probe_config_raw)
        result_value = decode_canonical_json(terminal_result_raw, require_canonical=True)
        if not isinstance(result_value, Mapping):
            raise ValueError("prediction invalid-source terminal result must be an object")
        result = cast(Mapping[str, Any], result_value)
        expected_result_fields = {
            "boundary",
            "bytes",
            "campaign_manifest_sha256",
            "candidate_config_sha256",
            "collection_id",
            "connection_attempts",
            "duplicates",
            "elapsed_ms",
            "error",
            "frames",
            "gaps",
            "limitations",
            "manifest_sha256",
            "network_calls",
            "official_contract_sha256",
            "probe_binding_sha256",
            "queue_high_water",
            "reconnects",
            "requested_duration_seconds",
            "root_sha256",
            "schema_version",
            "segments",
            "source_timestamp_max_ns",
            "source_timestamp_min_ns",
            "terminal_health",
            "venue",
        }
        if set(result) != expected_result_fields:
            raise ValueError("prediction invalid-source terminal fields differ from schema v1")
        duration = binding.payload.get("duration_seconds")
        max_bytes = binding.payload.get("max_bytes")
        max_frames = binding.payload.get("max_frames")
        max_network_calls = binding.payload.get("max_network_calls")
        max_segments = binding.payload.get("max_segments")
        frame_count = result.get("frames")
        byte_count = result.get("bytes")
        segment_count = result.get("segments")
        elapsed_ms = result.get("elapsed_ms")
        network_calls = result.get("network_calls")
        terminal_error = result.get("error")
        manifest_sha256 = result.get("manifest_sha256")
        root_sha256 = result.get("root_sha256")
        limitations = result.get("limitations")
        connection_attempts = result.get("connection_attempts")
        source_min = result.get("source_timestamp_min_ns")
        source_max = result.get("source_timestamp_max_ns")
        if (
            result.get("boundary") != BOUNDARY
            or result.get("schema_version") != 1
            or result.get("terminal_health") != "PUBLIC_SOURCE_INVALID"
            or result.get("venue") != binding.venue.value
            or result.get("collection_id") != binding.collection_id
            or result.get("campaign_manifest_sha256")
            != binding.campaign_manifest_sha256
            or result.get("candidate_config_sha256") != binding.candidate_config_sha256
            or result.get("official_contract_sha256")
            != binding.official_contract_sha256
            or result.get("probe_binding_sha256") != binding.probe_binding_sha256
            or type(duration) is not int
            or duration <= 0
            or result.get("requested_duration_seconds") != duration
            or type(frame_count) is not int
            or frame_count <= 0
            or type(byte_count) is not int
            or byte_count <= 0
            or type(segment_count) is not int
            or segment_count <= 0
            or type(elapsed_ms) is not int
            or elapsed_ms < 0
            or elapsed_ms > duration * 1_000
            or type(network_calls) is not int
            or network_calls < 0
            or any(
                type(limit) is not int or limit <= 0
                for limit in (max_bytes, max_frames, max_network_calls, max_segments)
            )
            or frame_count > cast(int, max_frames)
            or byte_count > cast(int, max_bytes)
            or network_calls > cast(int, max_network_calls)
            or segment_count > cast(int, max_segments)
            or type(terminal_error) is not str
            or not terminal_error.strip()
            or len(terminal_error.encode("utf-8")) > _TERMINAL_ERROR_MAX_UTF8_BYTES
            or type(manifest_sha256) is not str
            or type(root_sha256) is not str
            or any(
                len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
                for value in (manifest_sha256, root_sha256)
            )
            or not isinstance(limitations, list)
            or any(type(item) is not str or not item for item in limitations)
            or not isinstance(connection_attempts, list)
            or any(not isinstance(item, Mapping) for item in connection_attempts)
            or any(
                type(result.get(key)) is not int or cast(int, result.get(key)) < 0
                for key in ("duplicates", "gaps", "queue_high_water", "reconnects")
            )
            or any(
                value is not None and (type(value) is not int or value < 0)
                for value in (source_min, source_max)
            )
            or ((source_min is None) != (source_max is None))
            or (
                type(source_min) is int
                and type(source_max) is int
                and source_min > source_max
            )
        ):
            raise ValueError("prediction invalid-source terminal result is not admissible")
        canonical_result = cast(Mapping[str, CanonicalValue], result_value)
        return cls(
            binding=binding,
            result_payload=canonical_result,
            terminal_result_sha256=_sha256(terminal_result_raw),
            terminal_error=terminal_error,
            frame_count=frame_count,
            byte_count=byte_count,
            segment_count=segment_count,
            elapsed_ms=elapsed_ms,
            network_calls=network_calls,
            raw_manifest_sha256=manifest_sha256,
            raw_root_sha256=root_sha256,
        )

    @classmethod
    def from_probe_reports(cls, probe_root: Path) -> PredictionPublicSourceInvalidReceipt:
        reports_root = probe_root / "reports"
        return cls.from_report_bytes(
            probe_config_raw=_read_stable_regular_bytes(
                reports_root / "probe-config.json"
            ),
            terminal_result_raw=_read_stable_regular_bytes(
                reports_root / "result.json"
            ),
        )


@dataclass(frozen=True, slots=True)
class PredictionUnavailableSource:
    probe_root: Path
    venue: Venue
    collection_id: str
    probe_config_sha256: str
    terminal_result_sha256: str
    probe_payload: Mapping[str, CanonicalValue]
    classification: str
    campaign_manifest_sha256: str | None
    candidate_config_sha256: str | None
    official_contract_sha256: str | None
    terminal_health: str
    frame_count: int
    raw_manifest_sha256: str | None
    raw_root_sha256: str | None

    @classmethod
    def from_probe_output(cls, probe_root: Path) -> PredictionUnavailableSource:
        config_path = probe_root / "reports" / "probe-config.json"
        result_path = probe_root / "reports" / "result.json"
        config_raw = _read_stable_regular_bytes(config_path)
        result_raw = _read_stable_regular_bytes(result_path)
        config = _canonical_mapping_from_bytes(
            config_raw, label="unavailable probe config"
        )
        result = _canonical_mapping_from_bytes(
            result_raw, label="unavailable probe result"
        )
        invalid_receipt = (
            PredictionPublicSourceInvalidReceipt.from_report_bytes(
                probe_config_raw=config_raw,
                terminal_result_raw=result_raw,
            )
            if result.get("terminal_health") == "PUBLIC_SOURCE_INVALID"
            else None
        )
        expected_config_fields = {
            "boundary",
            "campaign_manifest_sha256",
            "candidate_config_sha256",
            "census_limit",
            "collection_id",
            "duration_seconds",
            "feeds",
            "instruments",
            "max_bytes",
            "max_frames",
            "max_network_calls",
            "max_segment_bytes",
            "max_segments",
            "official_contract_sha256",
            "probe_binding_sha256",
            "progress_interval_seconds",
            "proxy_policy",
            "rotation_seconds",
            "schema_version",
            "venue",
        }
        optional_config_fields = {"collection_cutoff_utc_ns_exclusive"}
        expected_result_fields = {
            "boundary",
            "bytes",
            "campaign_manifest_sha256",
            "candidate_config_sha256",
            "collection_id",
            "connection_attempts",
            "duplicates",
            "elapsed_ms",
            "error",
            "frames",
            "gaps",
            "limitations",
            "manifest_sha256",
            "network_calls",
            "official_contract_sha256",
            "probe_binding_sha256",
            "queue_high_water",
            "reconnects",
            "requested_duration_seconds",
            "root_sha256",
            "schema_version",
            "segments",
            "source_timestamp_max_ns",
            "source_timestamp_min_ns",
            "terminal_health",
            "venue",
        }
        terminal_error = result.get("error")
        error_required = {
            "BACKPRESSURE_LIMIT_REACHED",
            "INTERRUPTED_RECOVERED",
            "MAX_BYTES_REACHED",
            "PUBLIC_SOURCE_INVALID",
            "PUBLIC_SOURCE_UNAVAILABLE",
            "PUBLIC_SOURCE_UNAVAILABLE_RECOVERED",
            "RECOVERED_AFTER_PROCESS_ERROR",
        }
        if (
            frozenset(config)
            not in {
                frozenset(expected_config_fields),
                frozenset(expected_config_fields | optional_config_fields),
            }
            or set(result) != expected_result_fields
        ):
            raise ValueError("unavailable prediction probe schema diverged")
        claimed_binding = config.get("probe_binding_sha256")
        config_body = {
            key: value for key, value in config.items() if key != "probe_binding_sha256"
        }
        if (
            type(claimed_binding) is not str
            or len(claimed_binding) != 64
            or _sha256(canonical_json_bytes(config_body)) != claimed_binding
        ):
            raise ValueError("unavailable prediction probe config self-hash diverged")
        venue = Venue(str(config.get("venue") or ""))
        collection_id = str(config.get("collection_id") or "")
        duration = config.get("duration_seconds")
        max_calls = config.get("max_network_calls")
        network_calls = result.get("network_calls")
        elapsed_ms = result.get("elapsed_ms")
        binding_values = (
            config.get("campaign_manifest_sha256"),
            config.get("candidate_config_sha256"),
            config.get("official_contract_sha256"),
        )
        unbound = all(item is None for item in binding_values)
        campaign_bound = all(
            type(item) is str
            and len(item) == 64
            and all(character in "0123456789abcdef" for character in item)
            for item in binding_values
        )
        if not unbound and not campaign_bound:
            raise ValueError("unavailable prediction probe has partial campaign binding")
        cutoff = config.get("collection_cutoff_utc_ns_exclusive")
        if campaign_bound and (type(cutoff) is not int or cutoff <= 0):
            raise ValueError("campaign-bound unavailable probe lacks its slot cutoff")
        frame_count = result.get("frames")
        byte_count = result.get("bytes")
        segment_count = result.get("segments")
        terminal_health = str(result.get("terminal_health") or "")
        manifest_sha256 = result.get("manifest_sha256")
        root_sha256 = result.get("root_sha256")
        max_bytes = config.get("max_bytes")
        max_frames = config.get("max_frames")
        max_segment_bytes = config.get("max_segment_bytes")
        max_segments = config.get("max_segments")
        requested_feed_values = config.get("feeds")
        known_terminal = terminal_health in (
            _ADMISSIBLE_RAW_TERMINALS | _EXCLUDED_SLOT_TERMINALS
        )
        if (
            venue not in {Venue.POLYMARKET, Venue.KALSHI}
            or not collection_id
            or config.get("boundary") != BOUNDARY
            or result.get("boundary") != BOUNDARY
            or config.get("schema_version") != 1
            or result.get("schema_version") != 1
            or config.get("proxy_policy") != "DIRECT_ONLY_ENVIRONMENT_PROXY_DISABLED"
            or result.get("venue") != venue.value
            or result.get("collection_id") != collection_id
            or type(duration) is not int
            or duration <= 0
            or result.get("requested_duration_seconds") != duration
            or type(max_calls) is not int
            or max_calls <= 0
            or type(network_calls) is not int
            or network_calls < 0
            or network_calls > max_calls
            or type(elapsed_ms) is not int
            or elapsed_ms < 0
            or elapsed_ms > duration * 1000
            or not terminal_health
            or not known_terminal
            or not isinstance(requested_feed_values, list)
            or any(type(item) is not str or not item for item in requested_feed_values)
            or type(frame_count) is not int
            or frame_count < 0
            or type(byte_count) is not int
            or byte_count < 0
            or type(segment_count) is not int
            or segment_count < 0
            or any(
                type(limit) is not int or limit <= 0
                for limit in (max_bytes, max_frames, max_segment_bytes, max_segments)
            )
            or frame_count > cast(int, max_frames)
            or byte_count > cast(int, max_bytes)
            or segment_count > cast(int, max_segments)
            or not isinstance(result.get("limitations"), list)
            or not isinstance(result.get("connection_attempts"), list)
            or any(
                type(result.get(key)) is not int or cast(int, result.get(key)) < 0
                for key in ("duplicates", "gaps", "queue_high_water", "reconnects")
            )
            or any(
                result.get(key) is not None
                and (
                    type(result.get(key)) is not int
                    or cast(int, result.get(key)) < 0
                )
                for key in ("source_timestamp_max_ns", "source_timestamp_min_ns")
            )
            or (
                (result.get("source_timestamp_min_ns") is None)
                != (result.get("source_timestamp_max_ns") is None)
            )
            or (
                type(result.get("source_timestamp_min_ns")) is int
                and type(result.get("source_timestamp_max_ns")) is int
                and cast(int, result.get("source_timestamp_min_ns"))
                > cast(int, result.get("source_timestamp_max_ns"))
            )
            or (
                terminal_error is not None
                and (
                    type(terminal_error) is not str
                    or not terminal_error.strip()
                    or len(terminal_error.encode("utf-8"))
                    > _TERMINAL_ERROR_MAX_UTF8_BYTES
                )
            )
        ):
            raise ValueError("unavailable prediction probe terminal invariants diverged")
        if terminal_health in error_required and type(terminal_error) is not str:
            raise ValueError("prediction excluded slot terminal lacks its causal error")
        if terminal_health not in error_required and terminal_error is not None:
            raise ValueError("prediction terminal unexpectedly carries an error")
        if terminal_health in {
            "CONTINUITY_BROKEN_FROZEN",
            "CONTINUITY_UNKNOWN_AFTER_RECONNECT_FROZEN",
        } and not (cast(int, result.get("gaps")) or cast(int, result.get("reconnects"))):
            raise ValueError("prediction continuity failure lacks a gap or reconnect marker")
        raw_manifest: str | None = None
        raw_root: str | None = None
        if frame_count == 0:
            if (
                byte_count != 0
                or segment_count != 0
                or any(
                    result.get(key) is not None
                    for key in (
                        "campaign_manifest_sha256",
                        "candidate_config_sha256",
                        "manifest_sha256",
                        "official_contract_sha256",
                        "probe_binding_sha256",
                        "root_sha256",
                        "source_timestamp_max_ns",
                        "source_timestamp_min_ns",
                    )
                )
                or "NO_AUTHENTICATED_RAW_FRAME" not in result["limitations"]
            ):
                raise ValueError("zero-frame prediction slot receipt is inconsistent")
            if unbound:
                if (
                    terminal_health
                    != PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE.value
                    or network_calls <= 0
                    or type(result.get("error")) is not str
                ):
                    raise ValueError("unbound availability observation lacks direct evidence")
                classification = UNBOUND_AVAILABILITY_OBSERVATION
            else:
                classification = (
                    CAMPAIGN_BOUND_UNAVAILABILITY_RECEIPT
                    if terminal_health
                    == PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE.value
                    and network_calls > 0
                    else CAMPAIGN_BOUND_EXCLUDED_SLOT_RECEIPT
                )
        else:
            if unbound:
                raise ValueError("positive-frame excluded slot requires campaign binding")
            raw_manifest = str(manifest_sha256 or "")
            raw_root = str(root_sha256 or "")
            if (
                byte_count <= 0
                or segment_count <= 0
                or len(raw_manifest) != 64
                or len(raw_root) != 64
                or any(
                    character not in "0123456789abcdef"
                    for value in (raw_manifest, raw_root)
                    for character in value
                )
                or result.get("probe_binding_sha256") != claimed_binding
                or result.get("campaign_manifest_sha256") != binding_values[0]
                or result.get("candidate_config_sha256") != binding_values[1]
                or result.get("official_contract_sha256") != binding_values[2]
            ):
                raise ValueError("positive-frame excluded slot binding is inconsistent")
            reader = ResearchSegmentReader(
                probe_root / "raw",
                manifest_sha256=raw_manifest,
            )
            if (
                reader.manifest.root_sha256 != raw_root
                or reader.manifest.frame_count != frame_count
                or reader.manifest.stored_segment_bytes != byte_count
                or len(reader.manifest.segments) != segment_count
                or any(
                    descriptor.stored_bytes > cast(int, max_segment_bytes)
                    for descriptor in reader.manifest.segments
                )
            ):
                raise ValueError("excluded slot terminal result diverges from authenticated raw")
            envelopes = reader.replay()
            observed_duplicates = sum(int(envelope.state.duplicate) for envelope in envelopes)
            observed_gaps = sum(int(envelope.state.gap_detected) for envelope in envelopes)
            observed_reconnect_boundaries = sum(
                int(envelope.state.reconnect) for envelope in envelopes
            )
            observed_websocket = any(
                envelope.provenance.transport == "PUBLIC_WEBSOCKET"
                for envelope in envelopes
            )
            queue_high_water = cast(int, result.get("queue_high_water"))
            reconnects = cast(int, result.get("reconnects"))
            if (
                result.get("duplicates") != observed_duplicates
                or result.get("gaps") != observed_gaps
                or result.get("connection_attempts") != []
            ):
                raise ValueError("excluded slot counters diverge from authenticated raw")
            if venue is Venue.KALSHI and (queue_high_water != 0 or reconnects != 0):
                raise ValueError("Kalshi excluded slot carries impossible queue or reconnect counters")
            if venue is Venue.POLYMARKET and (
                queue_high_water != int(observed_websocket)
                or reconnects != observed_reconnect_boundaries
            ):
                raise ValueError("Polymarket excluded slot queue or reconnect counters diverge")
            source_timestamps = [
                envelope.source_timestamp_ns
                for envelope in envelopes
                if envelope.source_timestamp_ns is not None
            ]
            observed_source_min = min(source_timestamps) if source_timestamps else None
            observed_source_max = max(source_timestamps) if source_timestamps else None
            session_prefix = f"probe-binding-{claimed_binding}:"
            if any(
                envelope.venue is not venue
                or envelope.provenance.collection_id != collection_id
                or not envelope.session_identity.startswith(session_prefix)
                or envelope.receive_timestamp_utc_ns >= cast(int, cutoff)
                for envelope in envelopes
            ):
                raise ValueError("excluded slot raw provenance diverges from its probe binding")
            if (
                result.get("source_timestamp_min_ns") != observed_source_min
                or result.get("source_timestamp_max_ns") != observed_source_max
            ):
                raise ValueError("excluded slot source timestamp bounds diverge from raw")
            try:
                PredictionCollectionBinding.from_probe_output(probe_root)
            except ValueError as binding_error:
                if terminal_health in _ADMISSIBLE_RAW_TERMINALS:
                    observed_feeds = {item.feed_type for item in envelopes}
                    requested_feeds = set(cast(list[str], requested_feed_values))
                    dynamic_optional = {
                        Venue.POLYMARKET: {
                            "best_bid_ask",
                            "market_lifecycle",
                            "price_change",
                            "tick_size_change",
                        },
                        Venue.KALSHI: set(),
                    }[venue]
                    if not (requested_feeds - dynamic_optional - observed_feeds):
                        raise ValueError(
                            "admissible raw terminal failed for a non-coverage invariant"
                        ) from binding_error
                elif terminal_health not in _EXCLUDED_SLOT_TERMINALS:
                    raise ValueError(
                        "prediction excluded slot terminal is not allowlisted"
                    ) from binding_error
            else:
                if terminal_health != PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE.value:
                    raise ValueError(
                        "admissible raw collection cannot be relabeled as an excluded slot"
                    )
            classification = CAMPAIGN_BOUND_EXCLUDED_SLOT_RECEIPT
        if invalid_receipt is not None and (
            classification != invalid_receipt.classification
            or venue is not invalid_receipt.binding.venue
            or collection_id != invalid_receipt.binding.collection_id
            or _sha256(result_raw) != invalid_receipt.terminal_result_sha256
            or raw_manifest != invalid_receipt.raw_manifest_sha256
            or raw_root != invalid_receipt.raw_root_sha256
        ):
            raise ValueError("prediction invalid-source receipt diverged during raw authentication")
        return cls(
            probe_root=probe_root,
            venue=venue,
            collection_id=collection_id,
            probe_config_sha256=_sha256(config_raw),
            terminal_result_sha256=_sha256(result_raw),
            probe_payload=cast(Mapping[str, CanonicalValue], config),
            classification=classification,
            campaign_manifest_sha256=cast(str | None, binding_values[0]),
            candidate_config_sha256=cast(str | None, binding_values[1]),
            official_contract_sha256=cast(str | None, binding_values[2]),
            terminal_health=terminal_health,
            frame_count=frame_count,
            raw_manifest_sha256=raw_manifest,
            raw_root_sha256=raw_root,
        )

    def copy_to(self, target: Path) -> None:
        refreshed = self.from_probe_output(self.probe_root)
        if refreshed != self:
            raise ValueError("unavailable prediction probe changed before bundling")
        (target / "reports").mkdir(parents=True)
        for filename in ("probe-config.json", "result.json"):
            _write_new(
                target / "reports" / filename,
                (self.probe_root / "reports" / filename).read_bytes(),
            )
        if self.raw_manifest_sha256 is not None:
            _copy_authenticated_raw(
                self.probe_root / "raw",
                target / "raw",
                self.raw_manifest_sha256,
            )

    def descriptor(
        self,
        *,
        relative_root: str,
        include_terminal_fields: bool = True,
    ) -> dict[str, Any]:
        descriptor: dict[str, Any] = {
            "campaign_binding": (
                None
                if self.classification == UNBOUND_AVAILABILITY_OBSERVATION
                else {
                    "campaign_manifest_sha256": self.campaign_manifest_sha256,
                    "candidate_config_sha256": self.candidate_config_sha256,
                    "official_contract_sha256": self.official_contract_sha256,
                }
            ),
            "classification": self.classification,
            "collection_id": self.collection_id,
            "probe_config_sha256": self.probe_config_sha256,
            "relative_root": relative_root,
            "terminal_result_sha256": self.terminal_result_sha256,
            "venue": self.venue.value,
        }
        if include_terminal_fields:
            descriptor.update(
                {
                    "frames": self.frame_count,
                    "raw_manifest_sha256": self.raw_manifest_sha256,
                    "raw_root_sha256": self.raw_root_sha256,
                    "terminal_health": self.terminal_health,
                }
            )
        return descriptor


@dataclass(frozen=True, slots=True)
class _RawEntry:
    envelope: PublicDataEnvelope
    record: Mapping[str, Any]
    reference: PredictionRawRecordRef


class _Coverage:
    def __init__(self, index: PredictionRawEvidenceIndex) -> None:
        self._roles: dict[PredictionRawRecordRef, set[str]] = {}
        self._rejections: dict[PredictionRawRecordRef, set[str]] = {}
        self._controls: list[dict[str, Any]] = []
        self.entries: list[_RawEntry] = []
        for envelope in index.envelopes:
            if envelope.feed_type == "heartbeat":
                self._controls.append(
                    {
                        "arrival_sequence": envelope.arrival_sequence,
                        "content_sha256": envelope.content_sha256,
                        "feed_type": envelope.feed_type,
                        "role": "CONTROL_NON_ECONOMIC",
                        "venue": envelope.venue.value,
                    }
                )
                continue
            if envelope.feed_type.startswith("unknown_"):
                raise ValueError("UNSUPPORTED_PUBLIC_SCHEMA_FAIL_CLOSED")
            records = prediction_raw_records(envelope)
            for raw_record_index, record in enumerate(records):
                if envelope.feed_type == "market_batch" and record.get("event_type") not in {
                    "best_bid_ask",
                    "book",
                    "last_trade_price",
                    "market_resolved",
                    "new_market",
                    "price_change",
                    "tick_size_change",
                }:
                    raise ValueError("UNSUPPORTED_PUBLIC_SCHEMA_FAIL_CLOSED")
                reference = prediction_raw_record_ref(envelope, raw_record_index)
                self._roles[reference] = set()
                self._rejections[reference] = set()
                if envelope.feed_type in {
                    "exchange_schedule",
                    "exchange_status",
                    "historical_cutoff",
                    "incentives",
                    "last_trade_price",
                } or (
                    envelope.feed_type == "market_batch"
                    and record.get("event_type") == "last_trade_price"
                ):
                    self._roles[reference].add("OBSERVED_AUXILIARY_NON_ECONOMIC")
                self.entries.append(_RawEntry(envelope, record, reference))

    def role(self, references: Sequence[PredictionRawRecordRef], role: str) -> None:
        for reference in references:
            if reference in self._roles:
                self._roles[reference].add(role)

    def reject(self, reference: PredictionRawRecordRef, reason: str) -> None:
        if reference in self._rejections:
            self._rejections[reference].add(reason)

    def payload(self) -> dict[str, Any]:
        records: list[dict[str, Any]] = []
        for entry in self.entries:
            roles = sorted(self._roles[entry.reference])
            rejections = sorted(self._rejections[entry.reference])
            if not roles and not rejections:
                raise ValueError("UNCLASSIFIED_RAW_RECORD_FAIL_CLOSED")
            records.append(
                {
                    **entry.reference.to_dict(),
                    "feed_type": entry.envelope.feed_type,
                    "rejections": rejections,
                    "roles": roles,
                    "source_url": entry.envelope.provenance.source_url,
                    "venue": entry.envelope.venue.value,
                }
            )
        return {
            "controls": self._controls,
            "record_count": len(records),
            "records": records,
            "schema_version": 1,
        }


@dataclass(slots=True)
class _ShardRuntime:
    key: str
    relative_root: str
    venue: Venue
    raw_root: Path
    manifest_sha256: str
    index: PredictionRawEvidenceIndex
    binding: PredictionCollectionBinding | None
    prospective_ordinal: int | None
    synthetic: bool
    coverage: _Coverage
    graph_observations: tuple[PredictionIdentityGraph, ...]
    semantic_graphs: tuple[PredictionIdentityGraph, ...]
    fees: tuple[PredictionFeeSchedule, ...]
    ticks: tuple[PredictionTickGrid, ...]
    settlements: tuple[PredictionSettlementEvidence, ...]
    aggressors: tuple[MakerAggressorEvidence, ...]
    dataset: PredictionPointInTimeDataset | None = None
    trades: PredictionTradeDataset | None = None
    engine: PredictionGhostReplay | None = None
    limitation: str | None = None
    trade_limitation: str | None = None


@dataclass(frozen=True, slots=True)
class _GraphDiscovery:
    observations: tuple[PredictionIdentityGraph, ...]
    representatives: tuple[PredictionIdentityGraph, ...]


@dataclass(frozen=True, slots=True)
class VerifiedPredictionResearchBundle:
    root: Path
    manifest: Mapping[str, Any]
    preregistration: CandidatePreregistration
    campaign_manifest: Mapping[str, Any]
    contracts: Mapping[Venue, OfficialPublicContract]
    semantic_catalog: SemanticCatalog
    shards: tuple[_ShardRuntime, ...]
    campaign_runner: PredictionCampaignGhostReplay | None
    replay_seal: PredictionReplaySeal | None
    source_status_by_venue: Mapping[Venue, str]
    unavailable_sources: tuple[PredictionUnavailableSource, ...]
    prospective_slot_coverage: Mapping[str, Any] | None

    @property
    def bundle_sha256(self) -> str:
        return cast(str, self.manifest["bundle_sha256"])


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _graph_watermark(graph: PredictionIdentityGraph) -> tuple[int, int]:
    return max(
        (reference.arrival_sequence, reference.raw_record_index)
        for reference in graph.source_refs
    )


def _datetime_utc_ns(value: datetime) -> int:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("prediction campaign shard time must be timezone-aware")
    delta = value.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _canonical_mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return cast(Mapping[str, Any], value)


def _read_stable_regular_bytes(path: Path, *, maximum_bytes: int = 8 * 1024 * 1024) -> bytes:
    before = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size > maximum_bytes:
        raise ValueError(f"prediction report is unsafe or oversized: {path}")
    with path.open("rb") as handle:
        opened_before = os.fstat(handle.fileno())
        raw = handle.read(maximum_bytes + 1)
        opened_after = os.fstat(handle.fileno())
    after = path.lstat()
    identities = (
        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns),
        (
            opened_before.st_dev,
            opened_before.st_ino,
            opened_before.st_size,
            opened_before.st_mtime_ns,
        ),
        (
            opened_after.st_dev,
            opened_after.st_ino,
            opened_after.st_size,
            opened_after.st_mtime_ns,
        ),
        (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns),
    )
    if len(raw) > maximum_bytes or len(set(identities)) != 1 or len(raw) != before.st_size:
        raise ValueError(f"prediction report changed during read: {path}")
    return raw


def _canonical_mapping_from_bytes(raw: bytes, *, label: str) -> Mapping[str, Any]:
    if raw.endswith(b"\n"):
        raw = raw[:-1]
    decoded = decode_canonical_json(raw, require_canonical=True)
    return _canonical_mapping(decoded, label=label)


def _read_canonical_mapping(path: Path, *, label: str) -> Mapping[str, Any]:
    return _canonical_mapping_from_bytes(
        _read_stable_regular_bytes(path), label=label
    )


def _safe_relative(value: str) -> Path:
    windows = PureWindowsPath(value)
    pure = PurePosixPath(value)
    if (
        "\\" in value
        or ":" in value
        or windows.is_absolute()
        or windows.drive
        or windows.root
        or pure.is_absolute()
        or not pure.parts
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        raise ValueError("prediction bundle contains an unsafe relative path")
    return Path(*pure.parts)


def _resolved_child(root: Path, relative: str, *, must_exist: bool = True) -> Path:
    root_resolved = root.resolve()
    path = root / _safe_relative(relative)
    if must_exist and path.is_symlink():
        raise ValueError("prediction bundle path cannot be a symlink")
    resolved = path.resolve(strict=must_exist)
    if not resolved.is_relative_to(root_resolved):
        raise ValueError("prediction bundle path escapes its authenticated root")
    return resolved


def _write_new(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())


def _copy_authenticated_raw(source: Path, target: Path, manifest_sha256: str) -> None:
    reader = ResearchSegmentReader(source, manifest_sha256=manifest_sha256)
    (target / "segments").mkdir(parents=True)
    (target / "manifests").mkdir(parents=True)
    for descriptor in reader.manifest.segments:
        source_path = source / "segments" / f"{descriptor.physical_sha256}{SEGMENT_SUFFIX}"
        target_path = target / "segments" / source_path.name
        shutil.copyfile(source_path, target_path)
        if _sha256(target_path.read_bytes()) != descriptor.physical_sha256:
            raise ValueError("copied prediction segment hash diverged")
    current: str | None = manifest_sha256
    while current is not None:
        source_path = source / "manifests" / f"{current}{MANIFEST_SUFFIX}"
        raw = source_path.read_bytes()
        record = decode_manifest(raw, expected_manifest_sha256=current)
        _write_new(target / "manifests" / source_path.name, raw)
        current = record.previous_manifest_sha256
    copied = ResearchSegmentReader(target, manifest_sha256=manifest_sha256)
    if copied.manifest != reader.manifest or copied.replay() != reader.replay():
        raise ValueError("copied prediction raw authority diverged")


def _event_id_from_polymarket_market(record: Mapping[str, Any]) -> str | None:
    raw_events = record.get("events")
    if not isinstance(raw_events, list):
        return None
    ids = {
        str(item.get("id"))
        for item in raw_events
        if isinstance(item, Mapping) and item.get("id") is not None
    }
    return next(iter(ids)) if len(ids) == 1 else None


def _discover_graphs(
    index: PredictionRawEvidenceIndex,
    coverage: _Coverage,
    venue: Venue,
) -> _GraphDiscovery:
    candidates: list[PredictionIdentityGraph] = []
    candidate_keys: set[tuple[str, tuple[PredictionRawRecordRef, ...]]] = set()
    if venue is Venue.POLYMARKET:
        markets: dict[str, _RawEntry] = {}
        events: dict[str, _RawEntry] = {}
        clob: dict[str, dict[str, _RawEntry]] = {}

        def attempt_all() -> None:
            for market_id, market_entry in sorted(markets.items()):
                event_id = _event_id_from_polymarket_market(market_entry.record)
                event_entry = None if event_id is None else events.get(event_id)
                clob_entries = tuple(
                    sorted(
                        clob.get(market_id, {}).values(),
                        key=lambda item: (item.reference.arrival_sequence, item.reference.raw_record_index),
                    )
                )
                if event_entry is None or not clob_entries:
                    coverage.reject(market_entry.reference, "INCOMPLETE_EVENT_OR_CLOB_IDENTITY_GRAPH")
                    continue
                try:
                    graph = build_prediction_graph_from_raw(
                        index,
                        venue=venue,
                        market_ref=market_entry.reference,
                        event_ref=event_entry.reference,
                        clob_market_refs=tuple(item.reference for item in clob_entries),
                    )
                except ValueError as error:
                    coverage.reject(market_entry.reference, f"GRAPH_REJECTED:{error}")
                    continue
                key = (graph.rule_version.version_id, graph.source_refs)
                if key not in candidate_keys:
                    candidates.append(graph)
                    candidate_keys.add(key)

        for entry in coverage.entries:
            record = entry.record
            if entry.envelope.feed_type == "metadata" and record.get("clobTokenIds") is not None:
                market_id = str(record.get("conditionId") or "")
                if market_id:
                    markets[market_id] = entry
                    attempt_all()
            elif entry.envelope.feed_type == "events" and isinstance(record.get("markets"), list):
                event_id = str(record.get("id") or "")
                if event_id:
                    events[event_id] = entry
                    attempt_all()
            elif entry.envelope.feed_type == "metadata" and record.get("t") is not None:
                parsed = urlsplit(entry.envelope.provenance.source_url)
                prefixes = ("/clob-markets/", "/polymarket/clob-markets/")
                matching = [prefix for prefix in prefixes if parsed.path.startswith(prefix)]
                market_id = (
                    ""
                    if len(matching) != 1
                    else parsed.path.removeprefix(matching[0])
                )
                if market_id and "/" not in market_id:
                    clob[market_id] = {"FULL": entry}
                    attempt_all()
    else:
        kalshi_markets: dict[str, _RawEntry] = {}
        kalshi_events: dict[str, _RawEntry] = {}
        kalshi_metadata: dict[str, _RawEntry] = {}
        kalshi_series: dict[str, _RawEntry] = {}

        def attempt_all() -> None:
            for _ticker, market_entry in sorted(kalshi_markets.items()):
                event_id = str(market_entry.record.get("event_ticker") or "")
                event_entry = kalshi_events.get(event_id)
                metadata_entry = kalshi_metadata.get(event_id)
                series_id = (
                    ""
                    if event_entry is None
                    else str(event_entry.record.get("series_ticker") or "")
                )
                series_entry = kalshi_series.get(series_id)
                if event_entry is None or metadata_entry is None or series_entry is None:
                    coverage.reject(market_entry.reference, "INCOMPLETE_EVENT_METADATA_SERIES_GRAPH")
                    continue
                try:
                    graph = build_prediction_graph_from_raw(
                        index,
                        venue=venue,
                        market_ref=market_entry.reference,
                        event_ref=event_entry.reference,
                        event_metadata_ref=metadata_entry.reference,
                        series_ref=series_entry.reference,
                    )
                except ValueError as error:
                    coverage.reject(market_entry.reference, f"GRAPH_REJECTED:{error}")
                    continue
                key = (graph.rule_version.version_id, graph.source_refs)
                if key not in candidate_keys:
                    candidates.append(graph)
                    candidate_keys.add(key)

        for entry in coverage.entries:
            record = entry.record
            if entry.envelope.feed_type in {"historical_markets", "markets"} and record.get(
                "ticker"
            ):
                kalshi_markets[str(record["ticker"])] = entry
                attempt_all()
            elif entry.envelope.feed_type == "events" and record.get("event_ticker"):
                kalshi_events[str(record["event_ticker"])] = entry
                attempt_all()
            elif entry.envelope.feed_type == "event_metadata":
                parsed = urlsplit(entry.envelope.provenance.source_url)
                parts = tuple(part for part in parsed.path.split("/") if part)
                event_id = ""
                if len(parts) == 5 and parts[:3] == ("trade-api", "v2", "events") and parts[4] == "metadata":
                    event_id = parts[3]
                elif len(parts) == 4 and parts[:2] == ("kalshi", "events") and parts[3] == "metadata":
                    event_id = parts[2]
                if event_id:
                    kalshi_metadata[event_id] = entry
                    attempt_all()
            elif entry.envelope.feed_type == "series":
                series_id = str(record.get("ticker") or record.get("series_ticker") or "")
                if series_id:
                    kalshi_series[series_id] = entry
                    attempt_all()

    authenticated_candidates: list[PredictionIdentityGraph] = []
    graph_source_feeds = (
        "event_metadata",
        "events",
        "historical_markets",
        "market_batch",
        "markets",
        "metadata",
        "series",
    )
    for graph in candidates:
        graph_domains = {
            (source.collector_identity, source.session_identity)
            for reference in graph.source_refs
            for source in (
                index.require_envelope(
                    reference,
                    venue=graph.venue,
                    allowed_feeds=graph_source_feeds,
                ),
            )
        }
        graph_watermark = _graph_watermark(graph)
        anchor = max(
            graph.source_refs,
            key=lambda reference: (
                reference.arrival_sequence,
                reference.raw_record_index,
            ),
        )
        if len(graph_domains) != 1:
            coverage.reject(anchor, "IDENTITY_GRAPH_CLOCK_DOMAIN_MIXED")
            continue
        graph_domain = next(iter(graph_domains))
        reset_positions = [
            (envelope.arrival_sequence, -1)
            for envelope in index.envelopes
            if envelope.venue is graph.venue
            and (envelope.collector_identity, envelope.session_identity) == graph_domain
            and (envelope.arrival_sequence, -1) <= graph_watermark
            and (envelope.state.gap_detected or envelope.state.reconnect)
        ]
        if reset_positions and min(
            (reference.arrival_sequence, reference.raw_record_index)
            for reference in graph.source_refs
        ) <= max(reset_positions):
            coverage.reject(
                anchor,
                "IDENTITY_GRAPH_REAUTHENTICATION_INCOMPLETE_AFTER_DISCONTINUITY",
            )
            continue
        authenticated_candidates.append(graph)

    observations = tuple(
        sorted(
            authenticated_candidates,
            key=lambda item: (
                _graph_watermark(item),
                item.market_id,
                item.rule_version.version_id,
                item.raw_graph_sha256,
            ),
        )
    )
    for graph in observations:
        coverage.role(graph.source_refs, "IDENTITY_GRAPH_AUTHORITY")
    timelines: dict[tuple[Venue, str], list[PredictionIdentityGraph]] = {}
    for graph in observations:
        timelines.setdefault((graph.venue, graph.market_id), []).append(graph)
    for timeline in timelines.values():
        timeline.sort(
            key=lambda graph: (
                _graph_watermark(graph),
                graph.rule_version.version_id,
                graph.raw_graph_sha256,
            )
        )
        semantic_representatives: dict[str, PredictionIdentityGraph] = {}
        semantic_order: list[str] = []
        watermarks: dict[tuple[int, int], set[str]] = {}
        for graph in timeline:
            semantic_id = graph.rule_version.version_id
            watermarks.setdefault(_graph_watermark(graph), set()).add(
                graph.raw_graph_sha256
            )
            representative = semantic_representatives.get(semantic_id)
            if representative is None:
                semantic_representatives[semantic_id] = graph
            else:
                representative.assert_compatible_successor(
                    graph,
                    explicit_rule_version_transition=False,
                )
            if not semantic_order or semantic_order[-1] != semantic_id:
                semantic_order.append(semantic_id)
        if any(len(observation_ids) > 1 for observation_ids in watermarks.values()):
            raise ValueError("INTRA_SHARD_RULE_TIMELINE_UNAUTHENTICATED")
        if len(set(semantic_order)) != len(semantic_order):
            raise ValueError("INTRA_SHARD_RULE_TIMELINE_CYCLE")
        for previous_id, successor_id in pairwise(semantic_order):
            semantic_representatives[previous_id].assert_compatible_successor(
                semantic_representatives[successor_id],
                explicit_rule_version_transition=True,
            )
    ordered_observations = tuple(
        sorted(
            observations,
            key=lambda item: (
                item.venue.value,
                item.market_id,
                _graph_watermark(item),
                item.raw_graph_sha256,
            ),
        )
    )
    representatives: dict[tuple[Venue, str, str], PredictionIdentityGraph] = {}
    for graph in ordered_observations:
        representatives.setdefault(
            (graph.venue, graph.market_id, graph.rule_version.version_id),
            graph,
        )
    return _GraphDiscovery(
        observations=ordered_observations,
        representatives=tuple(representatives.values()),
    )


def _canonical_graph_observations(
    shards: Sequence[_ShardRuntime],
) -> tuple[
    tuple[PredictionIdentityGraph, ...],
    dict[tuple[Venue, str, str], int],
]:
    observations: dict[
        tuple[Venue, str, str, str], PredictionIdentityGraph
    ] = {}
    representatives: dict[tuple[Venue, str, str], PredictionIdentityGraph] = {}
    nodes: dict[tuple[Venue, str], set[tuple[Venue, str, str]]] = {}
    edges: dict[
        tuple[Venue, str],
        dict[tuple[Venue, str, str], set[tuple[Venue, str, str]]],
    ] = {}
    for shard in shards:
        local: dict[tuple[Venue, str], list[PredictionIdentityGraph]] = {}
        for graph in shard.graph_observations:
            semantic_key = (graph.venue, graph.market_id, graph.rule_version.version_id)
            observation_key = (*semantic_key, graph.raw_graph_sha256)
            previous_observation = observations.get(observation_key)
            if previous_observation is not None and previous_observation != graph:
                raise ValueError("prediction graph observation identity is ambiguous")
            observations[observation_key] = graph
            representative = representatives.get(semantic_key)
            if representative is None:
                representatives[semantic_key] = graph
            else:
                representative.assert_compatible_successor(
                    graph,
                    explicit_rule_version_transition=False,
                )
            market_key = semantic_key[:2]
            nodes.setdefault(market_key, set()).add(semantic_key)
            local.setdefault(market_key, []).append(graph)
        for market_key, timeline in local.items():
            timeline.sort(
                key=lambda graph: (
                    _graph_watermark(graph),
                    graph.rule_version.version_id,
                    graph.raw_graph_sha256,
                )
            )
            watermarks: dict[tuple[int, int], set[tuple[Venue, str, str]]] = {}
            for graph in timeline:
                watermarks.setdefault(
                    _graph_watermark(graph),
                    set(),
                ).add((graph.venue, graph.market_id, graph.rule_version.version_id))
            if any(len(keys) > 1 for keys in watermarks.values()):
                raise ValueError("INTRA_SHARD_RULE_TIMELINE_UNAUTHENTICATED")
            local_keys: list[tuple[Venue, str, str]] = []
            for graph in timeline:
                semantic_key = (graph.venue, graph.market_id, graph.rule_version.version_id)
                if not local_keys or local_keys[-1] != semantic_key:
                    local_keys.append(semantic_key)
            if len(set(local_keys)) != len(local_keys):
                raise ValueError("INTRA_SHARD_RULE_TIMELINE_CYCLE")
            graph_by_key = {
                (graph.venue, graph.market_id, graph.rule_version.version_id): graph
                for graph in timeline
            }
            for previous_key, successor_key in pairwise(local_keys):
                graph_by_key[previous_key].assert_compatible_successor(
                    graph_by_key[successor_key],
                    explicit_rule_version_transition=True,
                )
                edges.setdefault(market_key, {}).setdefault(previous_key, set()).add(
                    successor_key
                )

    semantic_versions: dict[tuple[Venue, str, str], int] = {}
    for market_key, market_nodes in sorted(
        nodes.items(), key=lambda item: (item[0][0].value, item[0][1])
    ):
        successors = edges.get(market_key, {})
        indegree = {node: 0 for node in market_nodes}
        for source, targets in successors.items():
            if source not in market_nodes or not targets <= market_nodes:
                raise AssertionError("prediction semantic constraint escaped its market")
            for target in targets:
                indegree[target] += 1
        remaining = set(market_nodes)
        ordered: list[tuple[Venue, str, str]] = []
        while remaining:
            ready = sorted(
                (node for node in remaining if indegree[node] == 0),
                key=lambda item: item[2],
            )
            if len(ready) != 1:
                raise ValueError("CROSS_SHARD_RULE_TIMELINE_UNAUTHENTICATED")
            selected = ready[0]
            ordered.append(selected)
            remaining.remove(selected)
            for target in successors.get(selected, set()):
                indegree[target] -= 1
        for ordinal, semantic_key in enumerate(ordered, start=1):
            semantic_versions[semantic_key] = ordinal

    ordered_observations = tuple(
        graph
        for _, graph in sorted(
            observations.items(),
            key=lambda item: (
                item[0][0].value,
                item[0][1],
                semantic_versions[item[0][:3]],
                item[0][3],
            ),
        )
    )
    return ordered_observations, semantic_versions


def _lifecycle_event_key(
    item: PredictionSettlementEvidence,
) -> tuple[Venue, str, str, str, str, str, str]:
    return (
        item.venue,
        item.market_id,
        item.rule_version_id,
        item.resolution_rule_version_id,
        item.raw_manifest_sha256,
        item.raw_ref.raw_record_sha256,
        item.source_event_id,
    )


def _lifecycle_order_key(
    items: Sequence[PredictionSettlementEvidence],
) -> int:
    source_times = {item.source_event_time_ns for item in items}
    received_times = {item.received_time_utc_ns for item in items}
    if len(source_times) != 1 or len(received_times) != 1:
        raise ValueError("PREDICTION_LIFECYCLE_EVENT_IS_NOT_ATOMIC")
    return next(iter(received_times))


def _lifecycle_monotonic_domain(
    items: Sequence[PredictionSettlementEvidence],
) -> tuple[tuple[str, str], int]:
    domains = {(item.collector_identity, item.session_identity) for item in items}
    if len(domains) != 1:
        raise ValueError("PREDICTION_LIFECYCLE_EVENT_CLOCK_DOMAIN_IS_NOT_ATOMIC")
    return next(iter(domains)), max(item.received_monotonic_ns for item in items)


def _lifecycle_source_time(
    items: Sequence[PredictionSettlementEvidence],
) -> int | None:
    source_times = {item.source_event_time_ns for item in items}
    if len(source_times) != 1:
        raise ValueError("PREDICTION_LIFECYCLE_EVENT_IS_NOT_ATOMIC")
    return next(iter(source_times))


def _validate_lifecycle_evidence(shards: Sequence[_ShardRuntime]) -> None:
    graphs = tuple(graph for shard in shards for graph in shard.semantic_graphs)
    expected_outcomes = {
        (graph.venue, graph.market_id, graph.rule_version.version_id): {
            outcome.outcome_id for outcome in graph.outcomes
        }
        for graph in graphs
    }
    events: dict[
        tuple[Venue, str, str, str, str, str, str],
        dict[str, PredictionSettlementEvidence],
    ] = {}
    for shard in shards:
        for item in shard.settlements:
            outcome_map = events.setdefault(_lifecycle_event_key(item), {})
            previous = outcome_map.get(item.outcome_id)
            if previous is not None and previous != item:
                raise ValueError("PREDICTION_LIFECYCLE_EVENT_OUTCOME_CONFLICT")
            outcome_map[item.outcome_id] = item
    by_lineage: dict[
        tuple[Venue, str, str],
        list[
            tuple[
                int,
                tuple[str, str],
                int,
                int | None,
                tuple[tuple[str, str, str | None], ...],
                tuple[Venue, str, str, str, str, str, str],
            ]
        ],
    ] = {}
    for key, outcome_map in events.items():
        items = tuple(outcome_map.values())
        expected = expected_outcomes.get((key[0], key[1], key[3]))
        if expected is None:
            raise ValueError("PREDICTION_LIFECYCLE_RESOLUTION_RULE_IS_ABSENT")
        if not all(item.synthetic_fixture for item in items) and set(outcome_map) != expected:
            raise ValueError("PREDICTION_PUBLIC_LIFECYCLE_EVENT_IS_NOT_OUTCOME_COMPLETE")
        states = tuple(
            sorted(
                (
                    outcome_id,
                    item.state.value,
                    (
                        None
                        if item.payout_per_contract is None
                        else format(item.payout_per_contract, "f")
                    ),
                )
                for outcome_id, item in outcome_map.items()
            )
        )
        domain, monotonic = _lifecycle_monotonic_domain(items)
        by_lineage.setdefault((key[0], key[1], key[2]), []).append(
            (
                _lifecycle_order_key(items),
                domain,
                monotonic,
                _lifecycle_source_time(items),
                states,
                key,
            )
        )
    for timeline in by_lineage.values():
        ordered: list[
            tuple[
                int,
                tuple[str, str],
                int,
                int | None,
                tuple[tuple[str, str, str | None], ...],
                tuple[Venue, str, str, str, str, str, str],
            ]
        ] = []
        timeline_domains = {item[1] for item in timeline}
        if len(timeline_domains) > 1:
            state_vectors = {item[4] for item in timeline}
            if any(item[3] is None for item in timeline):
                if len(state_vectors) > 1:
                    raise ValueError("CROSS_SHARD_LIFECYCLE_ORDER_UNAUTHENTICATED")
                ordered = sorted(timeline, key=lambda item: item[5])
            else:
                by_source: dict[int, list[Any]] = {}
                for timeline_item in timeline:
                    assert timeline_item[3] is not None
                    by_source.setdefault(timeline_item[3], []).append(timeline_item)
                for source_time in sorted(by_source):
                    simultaneous = by_source[source_time]
                    if len({item[4] for item in simultaneous}) > 1:
                        raise ValueError("CROSS_SHARD_LIFECYCLE_ORDER_UNAUTHENTICATED")
                    ordered.extend(sorted(simultaneous, key=lambda item: item[5]))
        else:
            by_received: dict[int, list[Any]] = {}
            for timeline_item in timeline:
                by_received.setdefault(timeline_item[0], []).append(timeline_item)
            for received_time in sorted(by_received):
                simultaneous = by_received[received_time]
                state_vectors = {item[4] for item in simultaneous}
                if len(state_vectors) > 1 and len({item[2] for item in simultaneous}) == 1:
                    raise ValueError("CROSS_SHARD_LIFECYCLE_ORDER_UNAUTHENTICATED")
                simultaneous.sort(key=lambda item: (item[2], item[5]))
                ordered.extend(simultaneous)
        for previous_event, successor_event in pairwise(ordered):
            previous_terminal = bool(previous_event[4]) and all(
                payout is not None for _outcome, _state, payout in previous_event[4]
            )
            successor_states = {state for _outcome, state, _payout in successor_event[4]}
            if previous_terminal and PredictionSettlementState.TRADING.value in successor_states:
                raise ValueError("PREDICTION_TERMINAL_LIFECYCLE_REGRESSION_TO_TRADING")
            if previous_terminal and any(
                state
                not in {
                    PredictionSettlementState.AMENDED.value,
                    PredictionSettlementState.DISPUTED.value,
                }
                for state in successor_states
            ) and any(payout is None for _outcome, _state, payout in successor_event[4]):
                raise ValueError("PREDICTION_TERMINAL_LIFECYCLE_REGRESSION_INVALID")
        terminal_vectors = {
            tuple((outcome, payout) for outcome, _state, payout in states)
            for _received, _domain, _monotonic, _source, states, _event_key in ordered
            if states and all(payout is not None for _outcome, _state, payout in states)
        }
        if len(terminal_vectors) > 1:
            raise ValueError("CONFLICTING_TERMINAL_SETTLEMENT")


def _select_latest_atomic_lifecycle_event(
    candidates: Sequence[PredictionSettlementEvidence],
    *,
    required_outcomes: set[str],
) -> dict[str, PredictionSettlementEvidence]:
    events: dict[
        tuple[Venue, str, str, str, str, str, str],
        dict[str, PredictionSettlementEvidence],
    ] = {}
    for item in candidates:
        outcome_map = events.setdefault(_lifecycle_event_key(item), {})
        previous = outcome_map.get(item.outcome_id)
        if previous is not None and previous != item:
            raise ValueError("PREDICTION_LIFECYCLE_EVENT_OUTCOME_CONFLICT")
        outcome_map[item.outcome_id] = item
    complete_events = [
        (event_key, outcome_map)
        for event_key, outcome_map in events.items()
        if required_outcomes <= set(outcome_map)
    ]
    if not complete_events:
        raise ValueError("prediction candidate has no atomic raw-bound lifecycle evidence")
    state_vectors_by_event = {
        event_key: tuple(
            sorted(
                (
                    outcome_id,
                    item.state.value,
                    None
                    if item.payout_per_contract is None
                    else format(item.payout_per_contract, "f"),
                )
                for outcome_id, item in outcome_map.items()
            )
        )
        for event_key, outcome_map in complete_events
    }
    domains = {
        _lifecycle_monotonic_domain(tuple(outcome_map.values()))[0]
        for _event_key, outcome_map in complete_events
    }
    latest = list(complete_events)
    if len(domains) > 1:
        source_times = {
            event_key: _lifecycle_source_time(tuple(outcome_map.values()))
            for event_key, outcome_map in complete_events
        }
        if any(value is None for value in source_times.values()):
            if len(set(state_vectors_by_event.values())) > 1:
                raise ValueError("CROSS_SHARD_LIFECYCLE_ORDER_UNAUTHENTICATED")
        else:
            latest_source = max(cast(int, value) for value in source_times.values())
            latest = [
                (event_key, outcome_map)
                for event_key, outcome_map in complete_events
                if source_times[event_key] == latest_source
            ]
    else:
        latest_received = max(
            _lifecycle_order_key(tuple(outcome_map.values()))
            for _event_key, outcome_map in complete_events
        )
        latest = [
            (event_key, outcome_map)
            for event_key, outcome_map in complete_events
            if _lifecycle_order_key(tuple(outcome_map.values())) == latest_received
        ]
        latest_monotonic = max(
            _lifecycle_monotonic_domain(tuple(outcome_map.values()))[1]
            for _event_key, outcome_map in latest
        )
        latest = [
            (event_key, outcome_map)
            for event_key, outcome_map in latest
            if _lifecycle_monotonic_domain(tuple(outcome_map.values()))[1]
            == latest_monotonic
        ]
    state_vectors = {
        tuple(
            sorted(
                (
                    outcome_id,
                    item.state.value,
                    None
                    if item.payout_per_contract is None
                    else format(item.payout_per_contract, "f"),
                )
                for outcome_id, item in outcome_map.items()
            )
        )
        for _event_key, outcome_map in latest
    }
    if len(state_vectors) > 1:
        raise ValueError("CROSS_SHARD_LIFECYCLE_ORDER_UNAUTHENTICATED")
    return sorted(latest, key=lambda item: item[0])[0][1]


def _source_record(
    index: PredictionRawEvidenceIndex,
    reference: PredictionRawRecordRef,
    venue: Venue,
) -> tuple[PublicDataEnvelope, Mapping[str, Any]]:
    return index.require_record(
        reference,
        venue=venue,
        allowed_feeds=(
            "event_fee_changes",
            "event_metadata",
            "events",
            "fee_changes",
            "fees",
            "ghost_fixture",
            "historical_markets",
            "market_batch",
            "markets",
            "metadata",
            "series",
            "tick_size",
            "tick_size_change",
        ),
    )


def _synthetic_polymarket_fee(
    index: PredictionRawEvidenceIndex,
    graph: PredictionIdentityGraph,
    coverage: _Coverage,
) -> PredictionFeeSchedule:
    graph_sources = [_source_record(index, reference, graph.venue) for reference in graph.source_refs]
    rate = _ZERO
    exponent = _ONE
    refs: list[PredictionRawRecordRef] = []
    for (envelope, record), reference in zip(graph_sources, graph.source_refs, strict=True):
        raw_fd = record.get("fd")
        raw_schedule = record.get("feeSchedule")
        if isinstance(raw_fd, Mapping):
            rate = Decimal(str(raw_fd.get("r")))
            exponent = Decimal(str(raw_fd.get("e")))
            refs.append(reference)
        if isinstance(raw_schedule, Mapping):
            rate = Decimal(str(raw_schedule.get("rate")))
            exponent = Decimal(str(raw_schedule.get("exponent")))
            refs.append(reference)
        if envelope.feed_type == "metadata" and (raw_fd is not None or raw_schedule is not None):
            refs.append(reference)
    for entry in coverage.entries:
        if entry.envelope.feed_type == "fees" and entry.envelope.market_id == graph.market_id:
            refs.append(entry.reference)
    ordered_refs = tuple(
        sorted(set(refs), key=lambda item: (item.arrival_sequence, item.raw_record_index))
    )
    identity = {
        "market_id": graph.market_id,
        "source_refs": [item.to_dict() for item in ordered_refs],
        "synthetic": True,
    }
    schedule = PredictionFeeSchedule(
        schedule_id=f"SYNTHETIC-PM-FEE:{_sha256(canonical_json_bytes(identity))}",
        venue=graph.venue,
        market_id=graph.market_id,
        outcome_ids=tuple(item.outcome_id for item in graph.outcomes),
        classification=EvidenceClassification.UNKNOWN_NOT_OBSERVED,
        model=FeeModel.ZERO if rate == _ZERO else FeeModel.POLYMARKET_QUADRATIC,
        effective_from_ns=0,
        effective_to_ns=None,
        taker_rate=rate,
        maker_rate=_ZERO,
        multiplier=_ONE,
        exponent=exponent,
        rounding_quantum=Decimal("0.00001"),
        rounding_complete=True,
        rounding_scope="PER_FILL",
        account_precision_quantum=None,
        source_refs=ordered_refs,
        synthetic_fixture=True,
    )
    coverage.role(ordered_refs, "SYNTHETIC_FEE_MECHANISM_ONLY")
    return schedule


def _synthetic_tick(
    index: PredictionRawEvidenceIndex,
    graph: PredictionIdentityGraph,
    coverage: _Coverage,
) -> PredictionTickGrid:
    selected: tuple[PredictionRawRecordRef, Decimal] | None = None
    for reference in graph.source_refs:
        _envelope, record = _source_record(index, reference, graph.venue)
        raw_tick = record.get("minimum_tick_size", record.get("tick_size"))
        if raw_tick is not None:
            selected = (reference, Decimal(str(raw_tick)))
            break
    if selected is None:
        raise ValueError("synthetic prediction graph lacks an explicit tick")
    reference, tick = selected
    identity = {"market_id": graph.market_id, "ref": reference.to_dict(), "tick": format(tick, "f")}
    grid = PredictionTickGrid(
        grid_id=f"SYNTHETIC-TICK:{_sha256(canonical_json_bytes(identity))}",
        venue=graph.venue,
        market_id=graph.market_id,
        outcome_ids=tuple(item.outcome_id for item in graph.outcomes),
        classification=EvidenceClassification.UNKNOWN_NOT_OBSERVED,
        bands=(PredictionTickBand(_ZERO, _ONE, tick),),
        source_refs=(reference,),
        synthetic_fixture=True,
    )
    coverage.role((reference,), "SYNTHETIC_TICK_MECHANISM_ONLY")
    return grid


def _discover_fees_and_ticks(
    index: PredictionRawEvidenceIndex,
    coverage: _Coverage,
    graphs: Sequence[PredictionIdentityGraph],
    *,
    synthetic: bool,
) -> tuple[tuple[PredictionFeeSchedule, ...], tuple[PredictionTickGrid, ...]]:
    fees: dict[str, PredictionFeeSchedule] = {}
    ticks: dict[str, PredictionTickGrid] = {}
    entries = coverage.entries
    for graph in graphs:
        if synthetic:
            if graph.venue is not Venue.POLYMARKET:
                raise ValueError("synthetic prediction bundle supports explicit Polymarket fixtures only")
            fee = _synthetic_polymarket_fee(index, graph, coverage)
            tick = _synthetic_tick(index, graph, coverage)
            fees[fee.evidence_sha256] = fee
            ticks[tick.evidence_sha256] = tick
            continue
        if graph.venue is Venue.POLYMARKET:
            source_records = [
                (reference, *_source_record(index, reference, graph.venue))
                for reference in graph.source_refs
            ]
            gamma_refs = [
                reference
                for reference, _envelope, record in source_records
                if isinstance(record.get("feeSchedule"), Mapping)
            ]
            clob_refs = [
                reference
                for reference, _envelope, record in source_records
                if isinstance(record.get("fd"), Mapping)
            ]
            endpoint_by_token: dict[str, PredictionRawRecordRef] = {}
            for entry in entries:
                if entry.envelope.feed_type != "fees":
                    continue
                tokens = parse_qs(urlsplit(entry.envelope.provenance.source_url).query).get(
                    "token_id", []
                )
                if len(tokens) != 1 or tokens[0] not in {item.outcome_id for item in graph.outcomes}:
                    continue
                endpoint_by_token[tokens[0]] = entry.reference
                if len(endpoint_by_token) != len(graph.outcomes) or not gamma_refs or not clob_refs:
                    continue
                refs = (gamma_refs[-1], clob_refs[-1], *endpoint_by_token.values())
                try:
                    fee = build_polymarket_fee_schedule_from_raw(
                        index,
                        graph=graph,
                        source_refs=refs,
                    )
                except ValueError as error:
                    coverage.reject(entry.reference, f"FEE_REJECTED:{error}")
                    continue
                fees[fee.evidence_sha256] = fee
                coverage.role(fee.source_refs, "FEE_TIMELINE_AUTHORITY")

            latest_tick: dict[str, tuple[PredictionRawRecordRef, Decimal]] = {}
            outcome_ids = {item.outcome_id for item in graph.outcomes}
            for entry in entries:
                if entry.envelope.feed_type not in {
                    "market_batch",
                    "tick_size",
                    "tick_size_change",
                }:
                    continue
                if entry.envelope.feed_type == "market_batch" and entry.record.get(
                    "event_type"
                ) != "tick_size_change":
                    continue
                query_tokens = parse_qs(
                    urlsplit(entry.envelope.provenance.source_url).query
                ).get("token_id", [])
                token = str(entry.record.get("asset_id") or "")
                if not token and len(query_tokens) == 1:
                    token = query_tokens[0]
                if token not in outcome_ids:
                    continue
                raw_market = entry.record.get("market")
                if raw_market is not None and str(raw_market) != graph.market_id:
                    coverage.reject(entry.reference, "TICK_MARKET_IDENTITY_DIVERGED")
                    continue
                raw_tick = (
                    entry.record.get("new_tick_size")
                    if entry.envelope.feed_type in {"market_batch", "tick_size_change"}
                    else entry.record.get("minimum_tick_size", entry.record.get("tick_size"))
                )
                try:
                    tick_value = Decimal(str(raw_tick))
                except Exception:
                    coverage.reject(entry.reference, "TICK_VALUE_INVALID")
                    continue
                previous = latest_tick.get(token)
                old_tick = entry.record.get("old_tick_size")
                if old_tick is not None and previous is not None and Decimal(str(old_tick)) != previous[1]:
                    coverage.reject(entry.reference, "TICK_OLD_VALUE_CHAIN_DIVERGED")
                    continue
                latest_tick[token] = (entry.reference, tick_value)
                if set(latest_tick) != outcome_ids:
                    coverage.reject(entry.reference, "TICK_TRANSITION_PENDING_OTHER_OUTCOME")
                    continue
                refs = tuple(latest_tick[item][0] for item in sorted(outcome_ids))
                try:
                    tick = build_prediction_tick_grid_from_raw(
                        index,
                        graph=graph,
                        source_refs=refs,
                    )
                except ValueError as error:
                    coverage.reject(entry.reference, f"TICK_REJECTED:{error}")
                    continue
                ticks[tick.evidence_sha256] = tick
                coverage.role(tick.source_refs, "TICK_TIMELINE_AUTHORITY")
        else:
            market_refs = []
            for reference in graph.source_refs:
                _envelope, record = _source_record(index, reference, graph.venue)
                if str(record.get("ticker") or "") == graph.market_id and isinstance(
                    record.get("price_ranges"), list
                ):
                    market_refs.append(reference)
            for reference in market_refs:
                try:
                    tick = build_prediction_tick_grid_from_raw(
                        index,
                        graph=graph,
                        source_refs=(reference,),
                    )
                except ValueError as error:
                    coverage.reject(reference, f"TICK_REJECTED:{error}")
                    continue
                ticks[tick.evidence_sha256] = tick
                coverage.role(tick.source_refs, "TICK_TIMELINE_AUTHORITY")
            series_refs = []
            event_fee_refs = []
            series_fee_refs = []
            for entry in entries:
                if entry.envelope.feed_type == "series" and str(
                    entry.record.get("ticker") or entry.record.get("series_ticker") or ""
                ) == graph.series_id:
                    series_refs.append(entry.reference)
                elif entry.envelope.feed_type == "event_fee_changes" and str(
                    entry.record.get("event_ticker") or ""
                ) == graph.event_id:
                    event_fee_refs.append(entry.reference)
                elif entry.envelope.feed_type == "fee_changes":
                    query = parse_qs(urlsplit(entry.envelope.provenance.source_url).query)
                    if query.get("series_ticker") == [graph.series_id]:
                        series_fee_refs.append(entry.reference)
            if series_refs and event_fee_refs and series_fee_refs:
                refs = (series_refs[-1], event_fee_refs[-1], series_fee_refs[-1])
                try:
                    fee = build_kalshi_unknown_fee_schedule_from_raw(
                        index,
                        graph=graph,
                        source_refs=refs,
                    )
                except ValueError as error:
                    coverage.reject(refs[-1], f"FEE_REJECTED:{error}")
                else:
                    fees[fee.evidence_sha256] = fee
                    coverage.role(fee.source_refs, "FEE_UNKNOWN_FAIL_CLOSED_AUTHORITY")
    return (
        tuple(sorted(fees.values(), key=lambda item: (item.market_id, item.effective_from_ns))),
        tuple(
            sorted(
                ticks.values(),
                key=lambda item: (
                    item.market_id,
                    max(
                        (reference.arrival_sequence, reference.raw_record_index)
                        for reference in item.source_refs
                    ),
                ),
            )
        ),
    )


def _record_source_time_ns(entry: _RawEntry) -> int | None:
    record = entry.record
    if (
        entry.envelope.venue is Venue.POLYMARKET
        and entry.envelope.feed_type == "metadata"
        and record.get("updatedAt") is not None
    ):
        return prediction_rfc3339_to_ns(
            record["updatedAt"],
            label="Polymarket Gamma updatedAt",
        )
    if (
        entry.envelope.venue is Venue.POLYMARKET
        and record.get("timestamp") is not None
        and (
            entry.envelope.feed_type in {"last_trade_price", "order_book", "price_change"}
            or record.get("event_type") in {"book", "last_trade_price", "price_change"}
        )
    ):
        value = int(str(record["timestamp"]))
        if value < 0:
            raise ValueError("Polymarket source timestamp cannot be negative")
        return value * 1_000_000
    if entry.envelope.venue is Venue.KALSHI and record.get("settlement_ts") is not None:
        return prediction_rfc3339_to_ns(
            record["settlement_ts"],
            label="Kalshi settlement timestamp",
        )
    return entry.envelope.source_timestamp_ns


def _settlement_state(
    entry: _RawEntry,
    *,
    outcome_id: str,
) -> tuple[PredictionSettlementState, Decimal | None, str | None, int | None]:
    record = entry.record
    if entry.envelope.provenance.transport == "FIXTURE":
        if record.get("state") is None or str(record.get("outcome_id") or "") != outcome_id:
            return PredictionSettlementState.TRADING, None, None, _record_source_time_ns(entry)
        return (
            PredictionSettlementState(str(record["state"])),
            None if record.get("payout") is None else Decimal(str(record["payout"])),
            None,
            _record_source_time_ns(entry),
        )
    if entry.envelope.venue is Venue.POLYMARKET:
        if record.get("event_type") == "market_resolved":
            payouts = record.get("payouts")
            if isinstance(payouts, Mapping) and outcome_id in payouts:
                payout = Decimal(str(payouts[outcome_id]))
                if payout == Decimal("0.5"):
                    return (
                        PredictionSettlementState.RESOLVED_50_50,
                        payout,
                        None,
                        _record_source_time_ns(entry),
                    )
            winner = str(record.get("winning_asset_id") or record.get("winningAssetId") or "")
            if winner:
                return (
                    PredictionSettlementState.RESOLVED_BINARY,
                    _ONE if winner == outcome_id else _ZERO,
                    None,
                    _record_source_time_ns(entry),
                )
        return (
            PredictionSettlementState.CLOSED_UNRESOLVED
            if record.get("closed") is True
            else PredictionSettlementState.TRADING,
            None,
            None,
            _record_source_time_ns(entry),
        )
    status = str(record.get("status") or "").lower()
    result = str(record.get("result") or "").lower()
    if status == "finalized" and result not in {"yes", "no"}:
        return (
            PredictionSettlementState.CLOSED_UNRESOLVED,
            None,
            "INVALID_FINALIZED_SETTLEMENT",
            None,
        )
    if status == "finalized" and result in {"yes", "no"}:
        try:
            settlement_value = Decimal(str(record.get("settlement_value_dollars")))
            source_time = prediction_rfc3339_to_ns(
                record.get("settlement_ts"),
                label="Kalshi finalized settlement_ts",
            )
        except (ValueError, ArithmeticError):
            return (
                PredictionSettlementState.CLOSED_UNRESOLVED,
                None,
                "INVALID_FINALIZED_SETTLEMENT",
                None,
            )
        expected_value = _ONE if result == "yes" else _ZERO
        if not settlement_value.is_finite() or settlement_value != expected_value or source_time is None:
            return (
                PredictionSettlementState.CLOSED_UNRESOLVED,
                None,
                "INVALID_FINALIZED_SETTLEMENT",
                None,
            )
        yes_outcome = outcome_id.endswith(":YES")
        return (
            PredictionSettlementState.FINALIZED,
            _ONE if (result == "yes") == yes_outcome else _ZERO,
            None,
            source_time,
        )
    state = {
        "active": PredictionSettlementState.TRADING,
        "amended": PredictionSettlementState.AMENDED,
        "closed": PredictionSettlementState.CLOSED_UNRESOLVED,
        "determined": PredictionSettlementState.DETERMINED,
        "disputed": PredictionSettlementState.DISPUTED,
        "finalized": PredictionSettlementState.CLOSED_UNRESOLVED,
        "inactive": PredictionSettlementState.CLOSED_UNRESOLVED,
        "initialized": PredictionSettlementState.TRADING,
    }.get(status, PredictionSettlementState.CLOSED_UNRESOLVED)
    try:
        lifecycle_source_time = _record_source_time_ns(entry)
    except ValueError:
        return state, None, "INVALID_LIFECYCLE_TIMESTAMP", None
    return state, None, None, lifecycle_source_time


def _index_aggressor_evidence(
    aggressors: dict[tuple[Venue, str], MakerAggressorEvidence],
    evidence: MakerAggressorEvidence,
) -> None:
    key = (evidence.venue, evidence.source_trade_id)
    previous = aggressors.get(key)
    if previous is not None:
        if previous.raw_ref.raw_record_sha256 == evidence.raw_ref.raw_record_sha256:
            return
        raise ValueError("prediction source trade changed silently")
    aggressors[key] = evidence


def _assert_resolution_lineage(
    entry_graph: PredictionIdentityGraph,
    resolution_graph: PredictionIdentityGraph,
) -> None:
    entry_graph.assert_compatible_successor(
        resolution_graph,
        explicit_rule_version_transition=(
            entry_graph.rule_version.version_id
            != resolution_graph.rule_version.version_id
        ),
    )
    if (
        entry_graph.rule_version.rule_text != resolution_graph.rule_version.rule_text
        or entry_graph.rule_version.resolution_source
        != resolution_graph.rule_version.resolution_source
        or entry_graph.rule_version.opens_at != resolution_graph.rule_version.opens_at
        or entry_graph.rule_version.closes_at != resolution_graph.rule_version.closes_at
        or entry_graph.rule_version.source_metadata_version
        != resolution_graph.rule_version.source_metadata_version
    ):
        raise ValueError("PREDICTION_RESOLUTION_RULE_LINEAGE_CHANGED_FAIL_CLOSED")


def _is_lifecycle_candidate(
    entry: _RawEntry,
    graph: PredictionIdentityGraph,
    *,
    synthetic: bool,
) -> bool:
    record = entry.record
    if synthetic:
        return (
            entry.envelope.feed_type == "ghost_fixture"
            and str(record.get("outcome_id") or "")
            in {item.outcome_id for item in graph.outcomes}
        )
    record_market = str(
        record.get("market")
        or record.get("conditionId")
        or record.get("condition_id")
        or record.get("ticker")
        or ""
    )
    if record_market != graph.market_id:
        return False
    if graph.venue is Venue.POLYMARKET:
        if entry.envelope.feed_type == "metadata":
            return (
                str(record.get("conditionId") or "") == graph.market_id
                and type(record.get("closed")) is bool
            )
        return (
            entry.envelope.feed_type in {"market_batch", "market_lifecycle"}
            and record.get("event_type") in {"market_resolved", "new_market"}
        )
    return entry.envelope.feed_type in {"historical_markets", "markets"}


def _discover_settlements_and_aggressors(
    index: PredictionRawEvidenceIndex,
    coverage: _Coverage,
    graphs: Sequence[PredictionIdentityGraph],
    *,
    synthetic: bool,
) -> tuple[tuple[PredictionSettlementEvidence, ...], tuple[MakerAggressorEvidence, ...]]:
    settlements: dict[tuple[str, str, str, str], PredictionSettlementEvidence] = {}
    aggressors: dict[tuple[Venue, str], MakerAggressorEvidence] = {}
    by_market: dict[str, list[PredictionIdentityGraph]] = {}
    by_outcome: dict[str, list[PredictionIdentityGraph]] = {}
    for graph in graphs:
        by_market.setdefault(graph.market_id, []).append(graph)
        for outcome in graph.outcomes:
            by_outcome.setdefault(outcome.outcome_id, []).append(graph)

    for entry in coverage.entries:
        record_market = str(
            entry.record.get("market")
            or entry.record.get("conditionId")
            or entry.record.get("condition_id")
            or entry.record.get("ticker")
            or ""
        )
        fixture_outcome = str(entry.record.get("outcome_id") or "")
        candidate_graphs = list(by_market.get(record_market, ()))
        if synthetic and entry.envelope.feed_type == "ghost_fixture" and fixture_outcome:
            candidate_graphs.extend(by_outcome.get(fixture_outcome, ()))
        if synthetic and entry.envelope.feed_type == "ghost_fixture" and not candidate_graphs:
            candidate_graphs = list(graphs)
        causal_graphs = [
            graph
            for graph in candidate_graphs
            if _graph_watermark(graph)
            <= (entry.reference.arrival_sequence, entry.reference.raw_record_index)
        ]
        if not causal_graphs:
            continue
        latest_watermark = max(
            _graph_watermark(graph) for graph in causal_graphs
        )
        latest_graphs = [
            graph
            for graph in causal_graphs
            if _graph_watermark(graph) == latest_watermark
        ]
        if len({graph.rule_version.version_id for graph in latest_graphs}) != 1:
            raise ValueError("PREDICTION_LIFECYCLE_GRAPH_SELECTION_AMBIGUOUS")
        resolution_graph = sorted(
            latest_graphs,
            key=lambda graph: graph.raw_graph_sha256,
        )[0]
        is_fixture_settlement = synthetic and _is_lifecycle_candidate(
            entry,
            resolution_graph,
            synthetic=True,
        )
        settlement_candidate = _is_lifecycle_candidate(
            entry,
            resolution_graph,
            synthetic=synthetic,
        )
        if settlement_candidate:
            entry_graphs: list[PredictionIdentityGraph] = []
            for entry_graph in by_market.get(resolution_graph.market_id, ()):
                if _graph_watermark(entry_graph) > latest_watermark:
                    continue
                try:
                    _assert_resolution_lineage(entry_graph, resolution_graph)
                except ValueError:
                    continue
                entry_graphs.append(entry_graph)
            if not entry_graphs:
                raise ValueError("PREDICTION_LIFECYCLE_HAS_NO_COMPATIBLE_ENTRY_RULE")
            for entry_graph in entry_graphs:
                for outcome in resolution_graph.outcomes:
                    if is_fixture_settlement and fixture_outcome != outcome.outcome_id:
                        continue
                    state, payout, limitation, source_time = _settlement_state(
                        entry,
                        outcome_id=outcome.outcome_id,
                    )
                    if limitation is not None:
                        coverage.reject(entry.reference, limitation)
                    evidence = PredictionSettlementEvidence(
                        venue=resolution_graph.venue,
                        market_id=resolution_graph.market_id,
                        outcome_id=outcome.outcome_id,
                        state=state,
                        source_event_time_ns=source_time,
                        received_time_utc_ns=entry.envelope.receive_timestamp_utc_ns,
                        received_monotonic_ns=entry.envelope.receive_monotonic_ns,
                        payout_per_contract=payout,
                        rule_version_id=entry_graph.rule_version.version_id,
                        resolution_rule_version_id=(
                            resolution_graph.rule_version.version_id
                        ),
                        source_event_id=str(
                            entry.envelope.source_event_id
                            or entry.reference.raw_record_sha256
                        ),
                        raw_manifest_sha256=index.manifest_sha256,
                        raw_root_sha256=index.root_sha256,
                        raw_ref=entry.reference,
                        collector_identity=entry.envelope.collector_identity,
                        session_identity=entry.envelope.session_identity,
                        source_url=entry.envelope.provenance.source_url,
                        classification=(
                            EvidenceClassification.UNKNOWN_NOT_OBSERVED
                            if synthetic
                            else EvidenceClassification.OBSERVED_PUBLICLY
                        ),
                        synthetic_fixture=synthetic,
                    )
                    key = (
                        evidence.rule_version_id,
                        evidence.resolution_rule_version_id,
                        evidence.outcome_id,
                        evidence.raw_ref.raw_record_sha256,
                    )
                    previous_settlement = settlements.get(key)
                    if previous_settlement is not None and previous_settlement != evidence:
                        raise ValueError("PREDICTION_LIFECYCLE_RECORD_CHANGED_SILENTLY")
                    settlements[key] = evidence
            coverage.role((entry.reference,), "SETTLEMENT_OR_LIFECYCLE_AUTHORITY")

        aggressor_evidence: MakerAggressorEvidence | None = None
        aggressor_role: str | None = None
        if synthetic and entry.envelope.feed_type == "ghost_fixture" and entry.record.get(
            "aggressor_side"
        ):
            outcome_id = fixture_outcome or next(iter(resolution_graph.outcomes)).outcome_id
            aggressor_evidence = MakerAggressorEvidence(
                venue=resolution_graph.venue,
                market_id=resolution_graph.market_id,
                outcome_id=outcome_id,
                source_event_time_ns=_record_source_time_ns(entry),
                received_time_utc_ns=entry.envelope.receive_timestamp_utc_ns,
                received_monotonic_ns=entry.envelope.receive_monotonic_ns,
                price=Decimal(str(entry.record["price"])),
                quantity=Decimal(str(entry.record["quantity"])),
                aggressor_side=str(entry.record["aggressor_side"]),
                source_trade_id=str(entry.record["trade_id"]),
                block_trade=False,
                source_event_id=str(entry.envelope.source_event_id or entry.record["trade_id"]),
                raw_manifest_sha256=index.manifest_sha256,
                raw_root_sha256=index.root_sha256,
                raw_ref=entry.reference,
                collector_identity=entry.envelope.collector_identity,
                session_identity=entry.envelope.session_identity,
                source_url=entry.envelope.provenance.source_url,
            )
            aggressor_role = "SYNTHETIC_AGGRESSOR_MECHANISM_ONLY"
        elif (
            resolution_graph.venue is Venue.KALSHI
            and entry.envelope.feed_type == "trades"
            and record_market == resolution_graph.market_id
            and not bool(entry.record.get("is_block_trade"))
        ):
            outcome_side = str(entry.record.get("taker_outcome_side") or "").upper()
            if outcome_side not in {"YES", "NO"}:
                coverage.reject(entry.reference, "AGGRESSOR_OUTCOME_SIDE_INVALID")
                continue
            outcome_id = f"{resolution_graph.market_id}:{outcome_side}"
            book_side = str(entry.record.get("taker_book_side") or "").upper()
            side = {"ASK": "BUY", "BUY": "BUY", "BID": "SELL", "SELL": "SELL"}.get(
                book_side
            )
            if side is None:
                coverage.reject(entry.reference, "AGGRESSOR_BOOK_SIDE_INVALID")
                continue
            raw_price = entry.record.get(
                "yes_price_dollars" if outcome_side == "YES" else "no_price_dollars"
            )
            if raw_price is None and entry.record.get("yes_price_dollars") is not None:
                raw_price = _ONE - Decimal(str(entry.record["yes_price_dollars"]))
            aggressor_evidence = MakerAggressorEvidence(
                venue=resolution_graph.venue,
                market_id=resolution_graph.market_id,
                outcome_id=outcome_id,
                source_event_time_ns=_record_source_time_ns(entry),
                received_time_utc_ns=entry.envelope.receive_timestamp_utc_ns,
                received_monotonic_ns=entry.envelope.receive_monotonic_ns,
                price=Decimal(str(raw_price)),
                quantity=Decimal(str(entry.record.get("count_fp"))),
                aggressor_side=side,
                source_trade_id=str(entry.record.get("trade_id") or ""),
                block_trade=False,
                source_event_id=str(
                    entry.envelope.source_event_id or entry.reference.raw_record_sha256
                ),
                raw_manifest_sha256=index.manifest_sha256,
                raw_root_sha256=index.root_sha256,
                raw_ref=entry.reference,
                collector_identity=entry.envelope.collector_identity,
                session_identity=entry.envelope.session_identity,
                source_url=entry.envelope.provenance.source_url,
            )
            aggressor_role = "MAKER_AGGRESSOR_AUTHORITY"
        if aggressor_evidence is not None and aggressor_role is not None:
            _index_aggressor_evidence(aggressors, aggressor_evidence)
            coverage.role((entry.reference,), aggressor_role)
    return (
        tuple(
            sorted(
                settlements.values(),
                key=lambda item: (
                    item.received_time_utc_ns,
                    item.venue.value,
                    item.market_id,
                    item.outcome_id,
                    item.raw_ref.arrival_sequence,
                ),
            )
        ),
        tuple(
            sorted(
                aggressors.values(),
                key=lambda item: (
                    item.received_time_utc_ns,
                    item.venue.value,
                    item.market_id,
                    item.outcome_id,
                    item.source_trade_id,
                ),
            )
        ),
    )


def _graph_payload(graph: PredictionIdentityGraph) -> dict[str, Any]:
    rule = graph.rule_version
    return {
        "event_id": graph.event_id,
        "execution_admissible": graph.execution_admissible,
        "ineligibility_reasons": list(graph.ineligibility_reasons),
        "market_id": graph.market_id,
        "multivariate": graph.multivariate,
        "negative_risk": graph.negative_risk,
        "outcomes": [item.to_dict() for item in graph.outcomes],
        "raw_graph_sha256": graph.raw_graph_sha256,
        "rule_version": {
            "closes_at": rule.closes_at,
            "market_status": rule.market_status,
            "opens_at": rule.opens_at,
            "raw_content_sha256": rule.raw_content_sha256,
            "resolution_source": rule.resolution_source,
            "resolves_at": rule.resolves_at,
            "rule_text": rule.rule_text,
            "source_metadata_version": rule.source_metadata_version,
            "version_id": rule.version_id,
        },
        "series_id": graph.series_id,
        "source_refs": [item.to_dict() for item in graph.source_refs],
        "venue": graph.venue.value,
    }


def _fee_payload(fee: PredictionFeeSchedule) -> dict[str, Any]:
    return {
        "account_precision_quantum": (
            None
            if fee.account_precision_quantum is None
            else format(fee.account_precision_quantum, "f")
        ),
        "classification": fee.classification.value,
        "effective_from_ns": fee.effective_from_ns,
        "effective_to_ns": fee.effective_to_ns,
        "evidence_sha256": fee.evidence_sha256,
        "exponent": None if fee.exponent is None else format(fee.exponent, "f"),
        "maker_rate": None if fee.maker_rate is None else format(fee.maker_rate, "f"),
        "market_id": fee.market_id,
        "model": fee.model.value,
        "multiplier": None if fee.multiplier is None else format(fee.multiplier, "f"),
        "outcome_ids": list(fee.outcome_ids),
        "rounding_complete": fee.rounding_complete,
        "rounding_quantum": (
            None if fee.rounding_quantum is None else format(fee.rounding_quantum, "f")
        ),
        "rounding_scope": fee.rounding_scope,
        "schedule_id": fee.schedule_id,
        "source_refs": [item.to_dict() for item in fee.source_refs],
        "synthetic_fixture": fee.synthetic_fixture,
        "taker_rate": None if fee.taker_rate is None else format(fee.taker_rate, "f"),
        "venue": fee.venue.value,
    }


def _tick_payload(tick: PredictionTickGrid) -> dict[str, Any]:
    return {
        "bands": [item.to_dict() for item in tick.bands],
        "classification": tick.classification.value,
        "evidence_sha256": tick.evidence_sha256,
        "grid_id": tick.grid_id,
        "market_id": tick.market_id,
        "outcome_ids": list(tick.outcome_ids),
        "source_refs": [item.to_dict() for item in tick.source_refs],
        "synthetic_fixture": tick.synthetic_fixture,
        "venue": tick.venue.value,
    }


def _dataset_payload(dataset: PredictionPointInTimeDataset) -> dict[str, Any]:
    return {
        "campaign_manifest_sha256": dataset.campaign_manifest_sha256,
        "candidate_config_sha256": dataset.candidate_config_sha256,
        "collection_probe_binding_sha256": dataset.collection_probe_binding_sha256,
        "collection_terminal_result_sha256": dataset.collection_terminal_result_sha256,
        "dataset_sha256": dataset.dataset_sha256,
        "identity": {
            "model_version": dataset.identity.model_version,
            "parameters_sha256": dataset.identity.parameters_sha256,
            "raw_manifest_sha256": dataset.identity.raw_manifest_sha256,
            "raw_root_sha256": dataset.identity.raw_root_sha256,
        },
        "rows": [canonical_value(item.to_dict()) for item in dataset.rows],
        "semantic_catalog_sha256": dataset.semantic_catalog_sha256,
        "synthetic": dataset.synthetic,
    }


def _trade_payload(
    dataset: PredictionTradeDataset | None,
    *,
    limitation: str | None,
) -> dict[str, Any]:
    if dataset is None:
        return {
            "rows": [],
            "status": limitation or "NO_AUTHENTICATED_TRADES_OBSERVED",
        }
    return {
        "campaign_manifest_sha256": dataset.campaign_manifest_sha256,
        "candidate_config_sha256": dataset.candidate_config_sha256,
        "collection_probe_binding_sha256": dataset.collection_probe_binding_sha256,
        "collection_terminal_result_sha256": dataset.collection_terminal_result_sha256,
        "dataset_sha256": dataset.dataset_sha256,
        "identity": {
            "model_version": dataset.identity.model_version,
            "parameters_sha256": dataset.identity.parameters_sha256,
            "raw_manifest_sha256": dataset.identity.raw_manifest_sha256,
            "raw_root_sha256": dataset.identity.raw_root_sha256,
        },
        "rows": [canonical_value(item.to_dict()) for item in dataset.rows],
        "semantic_catalog_sha256": dataset.semantic_catalog_sha256,
        "synthetic": dataset.synthetic,
        "status": "COMPLETE",
    }


def _settlement_payload(item: PredictionSettlementEvidence) -> dict[str, Any]:
    return {
        "classification": item.classification.value,
        "collector_identity": item.collector_identity,
        "market_id": item.market_id,
        "outcome_id": item.outcome_id,
        "payout_per_contract": (
            None if item.payout_per_contract is None else format(item.payout_per_contract, "f")
        ),
        "raw_manifest_sha256": item.raw_manifest_sha256,
        "raw_ref": item.raw_ref.to_dict(),
        "raw_root_sha256": item.raw_root_sha256,
        "received_monotonic_ns": item.received_monotonic_ns,
        "received_time_utc_ns": item.received_time_utc_ns,
        "resolution_rule_version_id": item.resolution_rule_version_id,
        "rule_version_id": item.rule_version_id,
        "session_identity": item.session_identity,
        "source_event_id": item.source_event_id,
        "source_event_time_ns": item.source_event_time_ns,
        "source_url": item.source_url,
        "state": item.state.value,
        "synthetic_fixture": item.synthetic_fixture,
        "venue": item.venue.value,
    }


def _aggressor_payload(item: MakerAggressorEvidence) -> dict[str, Any]:
    return {
        "aggressor_side": item.aggressor_side,
        "block_trade": item.block_trade,
        "collector_identity": item.collector_identity,
        "market_id": item.market_id,
        "outcome_id": item.outcome_id,
        "price": format(item.price, "f"),
        "quantity": format(item.quantity, "f"),
        "raw_manifest_sha256": item.raw_manifest_sha256,
        "raw_ref": item.raw_ref.to_dict(),
        "raw_root_sha256": item.raw_root_sha256,
        "received_monotonic_ns": item.received_monotonic_ns,
        "received_time_utc_ns": item.received_time_utc_ns,
        "session_identity": item.session_identity,
        "source_event_id": item.source_event_id,
        "source_event_time_ns": item.source_event_time_ns,
        "source_trade_id": item.source_trade_id,
        "source_url": item.source_url,
        "venue": item.venue.value,
    }


def _catalog_payload(catalog: SemanticCatalog) -> dict[str, Any]:
    return {
        "catalog_sha256": catalog.catalog_sha256,
        "relations": [
            {
                "confidence": format(item.confidence, "f"),
                "formal_rule": dict(item.formal_rule),
                "human_justification": item.human_justification,
                "machine_justification": dict(item.machine_justification),
                "members": [member.to_dict() for member in item.members],
                "provenance": list(item.provenance),
                "relation_id": item.relation_id,
                "relation_type": item.relation_type.value,
                "status": item.status.value,
                "version": item.version,
            }
            for item in catalog.relations
        ],
    }


def _artifact_payloads(
    shards: Sequence[_ShardRuntime],
    catalog: SemanticCatalog,
) -> tuple[dict[str, bytes], dict[str, int]]:
    payloads: dict[str, bytes] = {
        "artifacts/semantic-catalog.json": canonical_json_bytes(_catalog_payload(catalog)) + b"\n"
    }
    rows: dict[str, int] = {"artifacts/semantic-catalog.json": len(catalog.relations)}
    for shard in shards:
        prefix = f"artifacts/collections/{shard.key}"
        values: dict[str, tuple[Any, int]] = {
            f"{prefix}/coverage.json": (shard.coverage.payload(), len(shard.coverage.entries)),
            f"{prefix}/depth-dataset.json": (
                {"rows": [], "status": shard.limitation or "NO_AUTHENTICATED_BOOKS_OBSERVED"}
                if shard.dataset is None
                else _dataset_payload(shard.dataset),
                0 if shard.dataset is None else len(shard.dataset.rows),
            ),
            f"{prefix}/fee-timeline.json": (
                [_fee_payload(item) for item in shard.fees],
                len(shard.fees),
            ),
            f"{prefix}/graph-timeline.json": (
                [_graph_payload(item) for item in shard.graph_observations],
                len(shard.graph_observations),
            ),
            f"{prefix}/maker-aggressors.json": (
                [_aggressor_payload(item) for item in shard.aggressors],
                len(shard.aggressors),
            ),
            f"{prefix}/settlements.json": (
                [_settlement_payload(item) for item in shard.settlements],
                len(shard.settlements),
            ),
            f"{prefix}/tick-timeline.json": (
                [_tick_payload(item) for item in shard.ticks],
                len(shard.ticks),
            ),
            f"{prefix}/trade-dataset.json": (
                _trade_payload(
                    shard.trades,
                    limitation=shard.trade_limitation,
                ),
                0 if shard.trades is None else len(shard.trades.rows),
            ),
        }
        for path, (value, count) in values.items():
            payloads[path] = canonical_json_bytes(value) + b"\n"
            rows[path] = count
    return payloads, rows


def _load_inputs(
    root: Path,
) -> tuple[
    CandidatePreregistration,
    Mapping[str, Any],
    dict[Venue, OfficialPublicContract],
]:
    candidate = CandidatePreregistration.from_path(
        _resolved_child(root, "inputs/candidate-config.json")
    )
    campaign_raw = _read_canonical_mapping(
        _resolved_child(root, "inputs/campaign-manifest.json"),
        label="prediction bundle campaign manifest",
    )
    canonical_campaign = canonical_value(campaign_raw)
    if not isinstance(canonical_campaign, dict):
        raise AssertionError("prediction campaign must remain canonical")
    claimed = campaign_raw.get("manifest_sha256")
    body = {key: value for key, value in campaign_raw.items() if key != "manifest_sha256"}
    if not isinstance(claimed, str) or _sha256(canonical_json_bytes(body)) != claimed:
        raise ValueError("prediction bundle campaign manifest self-hash diverged")
    contracts = {
        Venue.POLYMARKET: OfficialPublicContract.from_path(
            _resolved_child(root, "inputs/polymarket-public-contract.json")
        ),
        Venue.KALSHI: OfficialPublicContract.from_path(
            _resolved_child(root, "inputs/kalshi-public-contract.json")
        ),
    }
    validate_prediction_campaign_manifest(
        campaign_manifest=canonical_campaign,
        preregistration=candidate,
        contracts=contracts,
    )
    return candidate, canonical_campaign, contracts


def _derive(
    root: Path,
    descriptors: Sequence[Mapping[str, Any]],
    preregistration: CandidatePreregistration,
    campaign_manifest: Mapping[str, Any],
    contracts: Mapping[Venue, OfficialPublicContract],
) -> tuple[tuple[_ShardRuntime, ...], SemanticCatalog, PredictionCampaignGhostReplay | None, PredictionReplaySeal | None]:
    shards: list[_ShardRuntime] = []
    seen_keys: set[str] = set()
    seen_manifests: set[str] = set()
    seen_public_slots: set[tuple[Venue, int]] = set()
    for raw_descriptor in descriptors:
        if set(raw_descriptor) != {
            "binding",
            "key",
            "raw_manifest_sha256",
            "raw_root_sha256",
            "relative_root",
            "synthetic",
            "venue",
        }:
            raise ValueError("prediction bundle shard descriptor schema diverged")
        relative_root = str(raw_descriptor.get("relative_root") or "")
        collection_root = _resolved_child(root, relative_root)
        manifest_sha256 = str(raw_descriptor.get("raw_manifest_sha256") or "")
        venue = Venue(str(raw_descriptor.get("venue") or ""))
        key = str(raw_descriptor.get("key") or "")
        if (
            len(manifest_sha256) != 64
            or any(item not in "0123456789abcdef" for item in manifest_sha256)
            or key != f"{venue.value}-{manifest_sha256[:16]}"
            or relative_root != f"collections/{key}"
            or key in seen_keys
            or manifest_sha256 in seen_manifests
        ):
            raise ValueError("prediction bundle shard descriptor identity diverged")
        raw_root = collection_root / "raw"
        reader = ResearchSegmentReader(raw_root, manifest_sha256=manifest_sha256)
        index = PredictionRawEvidenceIndex(reader, contracts=contracts)
        if not index.envelopes:
            raise ValueError("prediction bundle shard contains no authenticated frames")
        fixture_flags = {item.provenance.fixture_label is not None for item in index.envelopes}
        if len(fixture_flags) != 1:
            raise ValueError("prediction shard mixes synthetic and public provenance")
        synthetic = next(iter(fixture_flags))
        if (
            raw_descriptor.get("raw_root_sha256") != reader.manifest.root_sha256
            or raw_descriptor.get("synthetic") is not synthetic
        ):
            raise ValueError("prediction bundle shard raw identity diverged")
        binding = None
        prospective_ordinal = None
        if synthetic:
            if raw_descriptor.get("binding") is not None:
                raise ValueError("synthetic prediction shard cannot claim a public binding")
        else:
            binding = PredictionCollectionBinding.from_probe_output(collection_root)
            binding.verify(index, contract=contracts[venue])
            if (
                binding.campaign_manifest_sha256 != campaign_manifest.get("manifest_sha256")
                or binding.candidate_config_sha256 != preregistration.config_sha256
            ):
                raise ValueError("prediction shard campaign binding diverged")
            prospective_ordinal = prediction_prospective_shard_ordinal(
                preregistration=preregistration,
                campaign_manifest=campaign_manifest,
                venue=venue,
                collection_id=binding.collection_id,
            )
            binding.verify_collection_plan(preregistration.collection_plans[venue])
            campaign_start = datetime.fromisoformat(
                str(campaign_manifest.get("starts_at_utc") or "").replace(
                    "Z",
                    "+00:00",
                )
            )
            scheduled_start = preregistration.prospective_shard_policy.scheduled_start(
                campaign_start,
                prospective_ordinal,
            )
            slot_start_ns = _datetime_utc_ns(scheduled_start)
            slot_end_ns = slot_start_ns + (
                preregistration.prospective_shard_policy.cadence_seconds
                * 1_000_000_000
            )
            if (
                binding.payload.get("collection_cutoff_utc_ns_exclusive")
                != slot_end_ns
            ):
                raise ValueError(
                    "prediction collection cutoff diverges from its authenticated shard slot"
                )
            if any(
                envelope.receive_timestamp_utc_ns < slot_start_ns
                or envelope.receive_timestamp_utc_ns >= slot_end_ns
                for envelope in index.envelopes
            ):
                raise ValueError("prediction collection frames escape the authenticated shard slot")
            slot = (venue, prospective_ordinal)
            if slot in seen_public_slots:
                raise ValueError("prediction prospective shard slot is duplicated")
            seen_public_slots.add(slot)
            expected_binding = {
                "probe_binding_sha256": binding.probe_binding_sha256,
                "terminal_result_sha256": binding.terminal_result_sha256,
            }
            if raw_descriptor.get("binding") != expected_binding:
                raise ValueError("prediction shard terminal receipt descriptor diverged")
        if {item.venue for item in index.envelopes} != {venue}:
            raise ValueError("prediction shard contains another venue")
        coverage = _Coverage(index)
        graph_discovery = _discover_graphs(index, coverage, venue)
        fees, ticks = _discover_fees_and_ticks(
            index,
            coverage,
            graph_discovery.observations,
            synthetic=synthetic,
        )
        settlements, aggressors = _discover_settlements_and_aggressors(
            index,
            coverage,
            graph_discovery.observations,
            synthetic=synthetic,
        )
        shards.append(
            _ShardRuntime(
                key=key,
                relative_root=relative_root,
                venue=venue,
                raw_root=raw_root,
                manifest_sha256=manifest_sha256,
                index=index,
                binding=binding,
                prospective_ordinal=prospective_ordinal,
                synthetic=synthetic,
                coverage=coverage,
                graph_observations=graph_discovery.observations,
                semantic_graphs=graph_discovery.representatives,
                fees=fees,
                ticks=ticks,
                settlements=settlements,
                aggressors=aggressors,
            )
        )
        seen_keys.add(key)
        seen_manifests.add(manifest_sha256)
    shards.sort(
        key=lambda item: (
            item.prospective_ordinal is None,
            -1 if item.prospective_ordinal is None else item.prospective_ordinal,
            item.manifest_sha256,
            item.venue.value,
        )
    )
    all_graphs, semantic_versions = _canonical_graph_observations(shards)
    _validate_lifecycle_evidence(shards)
    catalog = build_prediction_semantic_catalog_from_graphs(
        all_graphs,
        semantic_versions=semantic_versions,
    )
    engines: list[PredictionGhostReplay] = []
    for shard in shards:
        usable_markets = {tick_item.market_id for tick_item in shard.ticks}
        dataset_graph_observations = tuple(
            graph_item
            for graph_item in shard.graph_observations
            if graph_item.market_id in usable_markets
        )
        dataset_graphs = tuple(
            graph_item
            for graph_item in shard.semantic_graphs
            if graph_item.market_id in usable_markets
        )
        fee_by_market: dict[str, list[PredictionFeeSchedule]] = {}
        for fee_item in shard.fees:
            if fee_item.market_id in usable_markets:
                fee_by_market.setdefault(fee_item.market_id, []).append(fee_item)
        tick_by_market: dict[str, list[PredictionTickGrid]] = {}
        for tick_item in shard.ticks:
            tick_by_market.setdefault(tick_item.market_id, []).append(tick_item)
        has_books = any(
            entry.envelope.feed_type in {"market_batch", "order_book"}
            and (
                entry.envelope.feed_type != "market_batch"
                or entry.record.get("event_type") == "book"
            )
            for entry in shard.coverage.entries
        )
        if dataset_graph_observations and dataset_graphs and has_books:
            try:
                shard.dataset = build_prediction_dataset(
                    raw_root=shard.raw_root,
                    manifest_sha256=shard.manifest_sha256,
                    contracts=contracts,
                    semantic_catalog=catalog,
                    graphs=dataset_graph_observations,
                    fee_schedules={key: tuple(value) for key, value in fee_by_market.items()},
                    tick_grids={key: tuple(value) for key, value in tick_by_market.items()},
                    collection_binding=shard.binding,
                )
            except ValueError as error:
                shard.limitation = f"DEPTH_DATASET_REJECTED:{error}"
                for entry in shard.coverage.entries:
                    if entry.envelope.feed_type in {
                        "best_bid_ask",
                        "order_book",
                        "price_change",
                    } or (
                        entry.envelope.feed_type == "market_batch"
                        and entry.record.get("event_type")
                        in {"best_bid_ask", "book", "price_change"}
                    ):
                        shard.coverage.reject(entry.reference, shard.limitation)
            else:
                represented_depth_refs: set[PredictionRawRecordRef] = set()
                for depth_row in shard.dataset.rows:
                    reference = prediction_raw_record_ref(
                        next(
                            envelope_item
                            for envelope_item in shard.index.envelopes
                            if envelope_item.content_sha256 == depth_row.raw_content_sha256
                            and envelope_item.arrival_sequence == depth_row.arrival_sequence
                        ),
                        depth_row.raw_record_index,
                    )
                    represented_depth_refs.add(reference)
                    shard.coverage.role((reference,), "DEPTH_DATASET_ROW")
                for entry in shard.coverage.entries:
                    if entry.envelope.feed_type == "best_bid_ask" or (
                        entry.envelope.feed_type == "market_batch"
                        and entry.record.get("event_type") == "best_bid_ask"
                    ):
                        shard.coverage.role(
                            (entry.reference,),
                            "BBO_DEPTH_CROSSCHECK_AUTHORITY",
                        )
                    elif (
                        entry.envelope.feed_type in {"order_book", "price_change"}
                        or (
                            entry.envelope.feed_type == "market_batch"
                            and entry.record.get("event_type")
                            in {"book", "price_change"}
                        )
                    ) and entry.reference not in represented_depth_refs:
                        shard.coverage.reject(
                            entry.reference,
                            "DEPTH_ROW_NOT_ADMITTED_CAUSAL_BLACKOUT",
                        )
        trade_records = [
            entry
            for entry in shard.coverage.entries
            if entry.envelope.feed_type
            in {"block_trades", "historical_trades", "public_trades", "trades"}
        ]
        if trade_records:
            if shard.graph_observations:
                try:
                    shard.trades = build_prediction_trade_dataset(
                        raw_root=shard.raw_root,
                        manifest_sha256=shard.manifest_sha256,
                        contracts=contracts,
                        semantic_catalog=catalog,
                        graphs=shard.graph_observations,
                        collection_binding=shard.binding,
                    )
                except ValueError as error:
                    shard.trade_limitation = f"TRADE_DATASET_REJECTED:{error}"
                    for entry in trade_records:
                        shard.coverage.reject(entry.reference, shard.trade_limitation)
                else:
                    represented_trade_refs: set[PredictionRawRecordRef] = set()
                    for trade_row in shard.trades.rows:
                        matching = [
                            entry.reference
                            for entry in trade_records
                            if entry.reference.arrival_sequence
                            == trade_row.arrival_sequence
                            and entry.reference.raw_record_index
                            == trade_row.raw_record_index
                            and entry.reference.raw_record_sha256
                            == trade_row.raw_record_sha256
                        ]
                        represented_trade_refs.update(matching)
                        shard.coverage.role(matching, "TRADE_DATASET_ROW")
                    for entry in trade_records:
                        if entry.reference not in represented_trade_refs:
                            shard.coverage.reject(
                                entry.reference,
                                "TRADE_RECORD_DEDUPLICATED_NOT_ADMITTED",
                            )
            else:
                shard.trade_limitation = "TRADE_DATASET_REJECTED:NO_AUTHENTICATED_GRAPH"
                for entry in trade_records:
                    shard.coverage.reject(entry.reference, shard.trade_limitation)
        if shard.dataset is not None:
            engine_market_keys = {
                (graph_item.venue, graph_item.market_id)
                for graph_item in dataset_graphs
            }
            engine_fees = {
                fee_item.schedule_id: fee_item
                for fee_item in shard.fees
                if (fee_item.venue, fee_item.market_id) in engine_market_keys
            }
            engine_ticks = {
                tick_item.grid_id: tick_item
                for tick_item in shard.ticks
                if (tick_item.venue, tick_item.market_id) in engine_market_keys
            }
            engine = PredictionGhostReplay(
                raw_root=shard.raw_root,
                manifest_sha256=shard.manifest_sha256,
                dataset=shard.dataset,
                preregistration=preregistration,
                contracts=contracts,
                collection_binding=shard.binding,
                semantic_catalog=catalog,
                identity_graphs=dataset_graphs,
                graph_observations=dataset_graph_observations,
                fee_schedules=engine_fees,
                tick_grids=engine_ticks,
                maximum_book_age_ns=preregistration.runner_policy.maximum_book_age_ns,
                prospective_shard_ordinal=shard.prospective_ordinal,
            )
            shard.engine = engine
            engines.append(engine)
    campaign_runner = None if not engines else PredictionCampaignGhostReplay(engines)
    replay_seal = None
    if campaign_runner is not None:
        starts = datetime.fromisoformat(
            str(campaign_manifest.get("starts_at_utc") or "").replace("Z", "+00:00")
        )
        selection = build_prediction_split_plan(
            preregistration=preregistration,
            dataset_sha256=campaign_runner.dataset_sha256,
            prospective_start=starts,
        ).selection_view
        replay_seal = build_prediction_replay_seal(
            campaign_manifest=campaign_manifest,
            preregistration=preregistration,
            selection_view=selection,
        )
    return tuple(shards), catalog, campaign_runner, replay_seal


def _artifact_manifest(payloads: Mapping[str, bytes], rows: Mapping[str, int]) -> dict[str, Any]:
    return {
        path: {
            "bytes": len(value),
            "rows": rows[path],
            "sha256": _sha256(value),
        }
        for path, value in sorted(payloads.items())
    }


def _verify_unavailable_descriptors(
    root: Path,
    raw_descriptors: object,
) -> tuple[PredictionUnavailableSource, ...]:
    if not isinstance(raw_descriptors, list) or any(
        not isinstance(item, Mapping) for item in raw_descriptors
    ):
        raise ValueError("prediction unavailable receipt descriptors are invalid")
    descriptors = [cast(Mapping[str, Any], item) for item in raw_descriptors]
    descriptor_order = [
        (
            str(item.get("venue") or ""),
            str(item.get("collection_id") or ""),
            str(item.get("probe_config_sha256") or ""),
        )
        for item in descriptors
    ]
    if descriptor_order != sorted(descriptor_order):
        raise ValueError("prediction unavailable receipts are not canonically ordered")
    sources: list[PredictionUnavailableSource] = []
    seen_keys: set[tuple[Venue, str]] = set()
    seen_roots: set[str] = set()
    legacy_fields = {
        "campaign_binding",
        "classification",
        "collection_id",
        "probe_config_sha256",
        "relative_root",
        "terminal_result_sha256",
        "venue",
    }
    terminal_fields = {
        "frames",
        "raw_manifest_sha256",
        "raw_root_sha256",
        "terminal_health",
    }
    for descriptor in descriptors:
        descriptor_fields = set(descriptor)
        if frozenset(descriptor_fields) not in {
            frozenset(legacy_fields),
            frozenset(legacy_fields | terminal_fields),
        }:
            raise ValueError("prediction unavailable receipt descriptor schema diverged")
        venue = Venue(str(descriptor.get("venue") or ""))
        collection_id = str(descriptor.get("collection_id") or "")
        key = (venue, collection_id)
        if (
            venue not in {Venue.POLYMARKET, Venue.KALSHI}
            or not collection_id
            or key in seen_keys
        ):
            raise ValueError("prediction unavailable receipt identity is duplicated or invalid")
        relative_root = str(descriptor.get("relative_root") or "")
        expected_new_root = (
            f"unavailable/{venue.value}-{descriptor.get('probe_config_sha256')}"
        )
        legacy_descriptor = descriptor_fields == legacy_fields
        if (
            relative_root
            not in {f"unavailable/{venue.value}", expected_new_root}
            or (legacy_descriptor and relative_root != f"unavailable/{venue.value}")
            or (not legacy_descriptor and relative_root != expected_new_root)
            or relative_root in seen_roots
        ):
            raise ValueError("prediction unavailable receipt path diverged")
        source = PredictionUnavailableSource.from_probe_output(
            _resolved_child(root, relative_root)
        )
        if descriptor != source.descriptor(
            relative_root=relative_root,
            include_terminal_fields=not legacy_descriptor,
        ):
            raise ValueError("prediction unavailable receipt identity diverged")
        seen_keys.add(key)
        seen_roots.add(relative_root)
        sources.append(source)
    return tuple(sources)


def _validate_unavailable_campaign_bindings(
    unavailable_sources: Sequence[PredictionUnavailableSource],
    *,
    preregistration: CandidatePreregistration,
    campaign_manifest: Mapping[str, object],
    contracts: Mapping[Venue, OfficialPublicContract],
) -> dict[tuple[Venue, int], PredictionUnavailableSource]:
    bound_slots: dict[tuple[Venue, int], PredictionUnavailableSource] = {}
    unbound_venues: set[Venue] = set()
    for unavailable_source in unavailable_sources:
        venue = unavailable_source.venue
        if unavailable_source.classification == UNBOUND_AVAILABILITY_OBSERVATION:
            if venue in unbound_venues:
                raise ValueError("prediction unbound availability observation is duplicated")
            unbound_venues.add(venue)
            continue
        if (
            unavailable_source.classification
            not in {
                CAMPAIGN_BOUND_UNAVAILABILITY_RECEIPT,
                CAMPAIGN_BOUND_EXCLUDED_SLOT_RECEIPT,
            }
            or unavailable_source.campaign_manifest_sha256
            != campaign_manifest.get("manifest_sha256")
            or unavailable_source.candidate_config_sha256
            != preregistration.config_sha256
            or unavailable_source.official_contract_sha256
            != contracts[venue].contract_sha256
        ):
            raise ValueError("prediction unavailable campaign receipt binding diverged")
        ordinal = prediction_prospective_shard_ordinal(
            preregistration=preregistration,
            campaign_manifest=campaign_manifest,
            venue=venue,
            collection_id=unavailable_source.collection_id,
        )
        verify_prediction_collection_plan_payload(
            unavailable_source.probe_payload,
            preregistration.collection_plans[venue],
        )
        campaign_start = datetime.fromisoformat(
            str(campaign_manifest.get("starts_at_utc") or "").replace("Z", "+00:00")
        )
        scheduled_start = preregistration.prospective_shard_policy.scheduled_start(
            campaign_start,
            ordinal,
        )
        slot_start_ns = _datetime_utc_ns(scheduled_start)
        slot_end_ns = (
            slot_start_ns
            + preregistration.prospective_shard_policy.cadence_seconds * 1_000_000_000
        )
        if (
            unavailable_source.probe_payload.get(
                "collection_cutoff_utc_ns_exclusive"
            )
            != slot_end_ns
        ):
            raise ValueError(
                "prediction unavailable receipt cutoff diverges from its authenticated shard slot"
            )
        if unavailable_source.raw_manifest_sha256 is not None:
            reader = ResearchSegmentReader(
                unavailable_source.probe_root / "raw",
                manifest_sha256=unavailable_source.raw_manifest_sha256,
            )
            index = PredictionRawEvidenceIndex(reader, contracts=contracts)
            if any(
                envelope.provenance.fixture_label is not None
                or envelope.venue is not venue
                or envelope.receive_timestamp_utc_ns < slot_start_ns
                or envelope.receive_timestamp_utc_ns >= slot_end_ns
                for envelope in index.envelopes
            ):
                raise ValueError("prediction excluded slot frames escape their scheduled window")
        slot = (venue, ordinal)
        if slot in bound_slots:
            raise ValueError("prediction unavailable campaign slot is duplicated")
        bound_slots[slot] = unavailable_source
    return bound_slots


def _prospective_slot_coverage(
    *,
    shards: Sequence[_ShardRuntime],
    unavailable_slots: Mapping[tuple[Venue, int], PredictionUnavailableSource],
    preregistration: CandidatePreregistration,
    campaign_manifest: Mapping[str, object],
) -> dict[str, Any] | None:
    public_shards = tuple(item for item in shards if not item.synthetic)
    if not public_shards and not unavailable_slots:
        return None
    if any(item.synthetic for item in shards):
        raise ValueError("synthetic prediction bundles cannot claim prospective slot coverage")
    campaign_start_ns = prediction_rfc3339_to_ns(
        str(campaign_manifest.get("starts_at_utc") or ""),
        label="prediction campaign start",
    )
    validation = _canonical_mapping(
        campaign_manifest.get("validation"),
        label="prediction campaign validation range",
    )
    evidence_cutoff_ns = prediction_rfc3339_to_ns(
        str(validation.get("end_exclusive") or ""),
        label="prediction campaign evidence cutoff",
    )
    cadence_ns = (
        preregistration.prospective_shard_policy.cadence_seconds * 1_000_000_000
    )
    span_ns = evidence_cutoff_ns - campaign_start_ns
    expected_ordinal_exclusive, remainder = divmod(span_ns, cadence_ns)
    if (
        span_ns <= 0
        or remainder != 0
        or expected_ordinal_exclusive
        > preregistration.prospective_shard_policy.expected_shards_per_venue
    ):
        raise ValueError("prediction prospective evidence cutoff is not cadence-aligned")
    expected_ordinals = set(range(expected_ordinal_exclusive))
    raw_slots: set[tuple[Venue, int]] = set()
    nonreplayable_raw_slots: set[tuple[Venue, int]] = set()
    for shard in public_shards:
        if shard.prospective_ordinal is None:
            raise ValueError("public prediction shard lacks its prospective ordinal")
        slot = (shard.venue, shard.prospective_ordinal)
        if shard.engine is None:
            nonreplayable_raw_slots.add(slot)
        else:
            raw_slots.add(slot)
    unavailable_slot_set = set(unavailable_slots)
    if (
        raw_slots & unavailable_slot_set
        or nonreplayable_raw_slots & unavailable_slot_set
        or raw_slots & nonreplayable_raw_slots
    ):
        raise ValueError("prediction prospective slot has both raw data and unavailability")
    coverage_by_venue: dict[str, Any] = {}
    for venue in (Venue.POLYMARKET, Venue.KALSHI):
        raw_ordinals = sorted(ordinal for item, ordinal in raw_slots if item is venue)
        nonreplayable_raw_ordinals = sorted(
            ordinal for item, ordinal in nonreplayable_raw_slots if item is venue
        )
        unavailable_ordinals = sorted(
            ordinal for item, ordinal in unavailable_slot_set if item is venue
        )
        observed = (
            set(raw_ordinals)
            | set(nonreplayable_raw_ordinals)
            | set(unavailable_ordinals)
        )
        if observed - expected_ordinals:
            raise ValueError("prediction bundle materializes sealed holdout slot evidence")
        missing = sorted(expected_ordinals - observed)
        coverage_by_venue[venue.value] = {
            "raw_ordinals": raw_ordinals,
            "excluded_ordinals": unavailable_ordinals,
            "missing_ordinals": missing,
            "nonreplayable_raw_ordinals": nonreplayable_raw_ordinals,
        }
    nonreplayable_raw_receipts = [
        {
            "frames": cast(PredictionCollectionBinding, shard.binding).frame_count,
            "ordinal": cast(int, shard.prospective_ordinal),
            "probe_binding_sha256": cast(
                PredictionCollectionBinding,
                shard.binding,
            ).probe_binding_sha256,
            "raw_manifest_sha256": shard.manifest_sha256,
            "raw_root_sha256": shard.index.root_sha256,
            "terminal_result_sha256": cast(
                PredictionCollectionBinding,
                shard.binding,
            ).terminal_result_sha256,
            "venue": shard.venue.value,
        }
        for shard in sorted(
            (item for item in public_shards if item.engine is None),
            key=lambda item: (
                item.venue.value,
                cast(int, item.prospective_ordinal),
                item.manifest_sha256,
            ),
        )
    ]
    excluded_receipts = [
        {
            "classification": source.classification,
            "frames": source.frame_count,
            "ordinal": ordinal,
            "probe_config_sha256": source.probe_config_sha256,
            "raw_manifest_sha256": source.raw_manifest_sha256,
            "raw_root_sha256": source.raw_root_sha256,
            "terminal_health": source.terminal_health,
            "terminal_result_sha256": source.terminal_result_sha256,
            "venue": venue.value,
        }
        for (venue, ordinal), source in sorted(
            unavailable_slots.items(),
            key=lambda item: (item[0][0].value, item[0][1]),
        )
    ]
    schedule_accounted = all(
        not coverage_by_venue[venue.value]["missing_ordinals"]
        for venue in (Venue.POLYMARKET, Venue.KALSHI)
    )
    economic_corpus_complete = schedule_accounted and all(
        not coverage_by_venue[venue.value]["excluded_ordinals"]
        and not coverage_by_venue[venue.value]["nonreplayable_raw_ordinals"]
        for venue in (Venue.POLYMARKET, Venue.KALSHI)
    )
    return {
        "economic_corpus_complete": economic_corpus_complete,
        "evidence_cutoff_utc_ns_exclusive": evidence_cutoff_ns,
        "excluded_receipts": excluded_receipts,
        "expected_ordinal_exclusive": expected_ordinal_exclusive,
        "nonreplayable_raw_receipts": nonreplayable_raw_receipts,
        "schedule_accounted": schedule_accounted,
        "venues": coverage_by_venue,
    }


def _bind_campaign_slot_coverage(
    runner: PredictionCampaignGhostReplay | None,
    shards: Sequence[_ShardRuntime],
    slot_coverage: Mapping[str, Any] | None,
) -> PredictionCampaignGhostReplay | None:
    if runner is None or slot_coverage is None:
        return runner
    engines = tuple(
        shard.engine
        for shard in shards
        if shard.engine is not None
    )
    if not engines:
        raise AssertionError("replayable prediction bundle lost its engines")
    return PredictionCampaignGhostReplay(
        engines,
        prospective_slot_coverage=slot_coverage,
    )


def build_prediction_research_bundle(
    *,
    output_root: Path,
    sources: Sequence[PredictionBundleSource],
    preregistration: CandidatePreregistration,
    campaign_manifest: Mapping[str, object],
    contracts: Mapping[Venue, OfficialPublicContract],
    unavailable_sources: Sequence[PredictionUnavailableSource] = (),
    allow_synthetic_fixtures: bool = False,
) -> VerifiedPredictionResearchBundle:
    if output_root.exists():
        raise FileExistsError("prediction research bundle output root must be new")
    unavailable_keys = {(item.venue, item.collection_id) for item in unavailable_sources}
    if len(unavailable_keys) != len(unavailable_sources):
        raise ValueError("prediction unavailable source identity is duplicated")
    if not sources and not unavailable_sources:
        raise ValueError("prediction research bundle requires raw sources or explicit unavailability")
    if set(contracts) != {Venue.POLYMARKET, Venue.KALSHI}:
        raise ValueError("prediction research bundle requires both official contracts")
    canonical_campaign = canonical_value(campaign_manifest)
    if not isinstance(canonical_campaign, dict):
        raise ValueError("prediction campaign manifest must be canonical")
    validate_prediction_campaign_manifest(
        campaign_manifest=canonical_campaign,
        preregistration=preregistration,
        contracts=contracts,
    )
    unavailable_slots = _validate_unavailable_campaign_bindings(
        unavailable_sources,
        preregistration=preregistration,
        campaign_manifest=canonical_campaign,
        contracts=contracts,
    )
    staging = output_root.parent / f".{output_root.name}.tmp-{uuid4().hex}"
    staging.mkdir(parents=True)
    try:
        _write_new(
            staging / "inputs" / "candidate-config.json",
            canonical_json_bytes(preregistration.payload) + b"\n",
        )
        _write_new(
            staging / "inputs" / "campaign-manifest.json",
            canonical_json_bytes(canonical_campaign) + b"\n",
        )
        for venue, filename in (
            (Venue.POLYMARKET, "polymarket-public-contract.json"),
            (Venue.KALSHI, "kalshi-public-contract.json"),
        ):
            _write_new(
                staging / "inputs" / filename,
                canonical_json_bytes(contracts[venue].payload) + b"\n",
            )
        descriptors: list[dict[str, Any]] = []
        unavailable_descriptors: list[dict[str, Any]] = []
        seen_manifests: set[str] = set()
        seen_venues: set[Venue] = set()
        synthetic_values: set[bool] = set()
        for source in sorted(sources, key=lambda item: item.manifest_sha256):
            if source.manifest_sha256 in seen_manifests:
                raise ValueError("prediction bundle source manifest is duplicated")
            reader = ResearchSegmentReader(source.raw_root, manifest_sha256=source.manifest_sha256)
            venues = {item.venue for item in reader.replay()}
            if len(venues) != 1 or next(iter(venues)) not in {Venue.POLYMARKET, Venue.KALSHI}:
                raise ValueError("prediction bundle source must contain one prediction venue")
            venue = next(iter(venues))
            synthetic = all(item.provenance.fixture_label is not None for item in reader.replay())
            if synthetic and not allow_synthetic_fixtures:
                raise ValueError("synthetic prediction bundle requires explicit fixture permission")
            if not synthetic and source.collection_root is None:
                raise ValueError("public prediction bundle source requires its probe output root")
            key = f"{venue.value}-{source.manifest_sha256[:16]}"
            relative_root = f"collections/{key}"
            target_root = staging / _safe_relative(relative_root)
            _copy_authenticated_raw(source.raw_root, target_root / "raw", source.manifest_sha256)
            binding_payload: Any = None
            if source.collection_root is not None:
                binding = PredictionCollectionBinding.from_probe_output(source.collection_root)
                if binding.raw_manifest_sha256 != source.manifest_sha256:
                    raise ValueError("prediction bundle source terminal manifest diverged")
                (target_root / "reports").mkdir()
                for filename in ("probe-config.json", "result.json"):
                    raw = (source.collection_root / "reports" / filename).read_bytes()
                    _write_new(target_root / "reports" / filename, raw)
                binding_payload = {
                    "probe_binding_sha256": binding.probe_binding_sha256,
                    "terminal_result_sha256": binding.terminal_result_sha256,
                }
            descriptors.append(
                {
                    "binding": binding_payload,
                    "key": key,
                    "raw_manifest_sha256": source.manifest_sha256,
                    "raw_root_sha256": reader.manifest.root_sha256,
                    "relative_root": relative_root,
                    "synthetic": synthetic,
                    "venue": venue.value,
                }
            )
            seen_manifests.add(source.manifest_sha256)
            seen_venues.add(venue)
            synthetic_values.add(synthetic)
        descriptors.sort(key=lambda item: str(item["key"]))
        for unavailable_source in sorted(
            unavailable_sources,
            key=lambda item: (
                item.venue.value,
                item.collection_id,
                item.probe_config_sha256,
            ),
        ):
            relative_root = (
                f"unavailable/{unavailable_source.venue.value}-"
                f"{unavailable_source.probe_config_sha256}"
            )
            unavailable_source.copy_to(staging / _safe_relative(relative_root))
            unavailable_descriptors.append(
                unavailable_source.descriptor(relative_root=relative_root)
            )
        unbound_venues = {
            item.venue
            for item in unavailable_sources
            if item.classification == UNBOUND_AVAILABILITY_OBSERVATION
        }
        if len(synthetic_values) > 1:
            raise ValueError("prediction bundle cannot mix public and synthetic shards")
        if synthetic_values == {True}:
            if unavailable_slots:
                raise ValueError("synthetic prediction bundle cannot claim campaign slot receipts")
            missing = {Venue.POLYMARKET, Venue.KALSHI} - seen_venues
            if seen_venues & unbound_venues:
                raise ValueError("prediction bundle marks an included venue unavailable")
            if missing != unbound_venues:
                raise ValueError("prediction bundle omitted a venue without terminal unavailability")
        elif sources:
            if unbound_venues:
                raise ValueError("public campaign bundle cannot use unbound availability observations")
        elif unbound_venues:
            if (
                unbound_venues != {Venue.POLYMARKET, Venue.KALSHI}
                or len(unavailable_sources) != 2
            ):
                raise ValueError("access-only bundle requires one unbound observation per venue")
        elif not unavailable_slots:
            raise ValueError("prediction campaign bundle lacks terminal slot receipts")
        shards, catalog, runner, seal = _derive(
            staging,
            descriptors,
            preregistration,
            canonical_campaign,
            contracts,
        )
        slot_coverage = _prospective_slot_coverage(
            shards=shards,
            unavailable_slots=unavailable_slots,
            preregistration=preregistration,
            campaign_manifest=canonical_campaign,
        )
        runner = _bind_campaign_slot_coverage(runner, shards, slot_coverage)
        payloads, row_counts = _artifact_payloads(shards, catalog)
        for path, value in payloads.items():
            _write_new(staging / _safe_relative(path), value)
        statuses: dict[str, Any] = {}
        unavailable_venues = {item.venue for item in unavailable_sources}
        for venue in (Venue.POLYMARKET, Venue.KALSHI):
            venue_shards = [item for item in shards if item.venue is venue]
            venue_receipts = [item for item in unavailable_sources if item.venue is venue]
            if venue_shards and all(item.synthetic for item in venue_shards):
                statuses[venue.value] = SYNTHETIC_SOURCE_STATUS
            elif venue_shards:
                statuses[venue.value] = PublicSourceStatus.OBSERVED_PUBLICLY.value
            elif venue_receipts and all(
                item.classification
                in {
                    UNBOUND_AVAILABILITY_OBSERVATION,
                    CAMPAIGN_BOUND_UNAVAILABILITY_RECEIPT,
                }
                for item in venue_receipts
            ):
                statuses[venue.value] = PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE.value
            elif venue in unavailable_venues or slot_coverage is not None:
                statuses[venue.value] = INSUFFICIENT_PUBLIC_CORPUS
            else:
                raise ValueError("prediction bundle omitted a venue without source status")
        body: dict[str, Any] = {
            "artifacts": _artifact_manifest(payloads, row_counts),
            "boundary": BOUNDARY,
            "campaign_manifest_sha256": cast(str, canonical_campaign["manifest_sha256"]),
            "candidate_config_sha256": preregistration.config_sha256,
            "child_dataset_sha256s": sorted(
                item.dataset.dataset_sha256 for item in shards if item.dataset is not None
            ),
            "contracts": {
                venue.value: contracts[venue].contract_sha256
                for venue in (Venue.POLYMARKET, Venue.KALSHI)
            },
            "dataset_bundle_sha256": None if runner is None else runner.dataset_sha256,
            "economic_claim": "NONE_RESEARCH_MECHANISM_ONLY",
            "model_version": MODEL_VERSION,
            "prospective_slot_coverage": slot_coverage,
            "replay_seal_sha256": None if seal is None else seal.seal_sha256,
            "schema_version": 1,
            "semantic_catalog_sha256": catalog.catalog_sha256,
            "shards": descriptors,
            "source_status_by_venue": statuses,
            "status": "READY_FOR_OFFLINE_VERIFICATION",
            "unavailable_receipts": unavailable_descriptors,
        }
        manifest = {**body, "bundle_sha256": _sha256(canonical_json_bytes(body))}
        _write_new(staging / "bundle-manifest.json", canonical_json_bytes(manifest) + b"\n")
        os.replace(staging, output_root)
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return verify_prediction_research_bundle(
        output_root,
        expected_bundle_sha256=cast(str, manifest["bundle_sha256"]),
    )


def verify_prediction_research_bundle(
    root: Path,
    *,
    expected_bundle_sha256: str,
) -> VerifiedPredictionResearchBundle:
    if (
        len(expected_bundle_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_bundle_sha256)
    ):
        raise ValueError("prediction bundle expected SHA-256 is invalid")
    manifest_raw = _read_canonical_mapping(
        root / "bundle-manifest.json",
        label="prediction bundle manifest",
    )
    manifest = canonical_value(manifest_raw)
    if not isinstance(manifest, dict):
        raise AssertionError("prediction bundle manifest must remain canonical")
    claimed = manifest.get("bundle_sha256")
    body = {key: value for key, value in manifest.items() if key != "bundle_sha256"}
    expected_fields = {
        "artifacts",
        "boundary",
        "bundle_sha256",
        "campaign_manifest_sha256",
        "candidate_config_sha256",
        "child_dataset_sha256s",
        "contracts",
        "dataset_bundle_sha256",
        "economic_claim",
        "model_version",
        "prospective_slot_coverage",
        "replay_seal_sha256",
        "schema_version",
        "semantic_catalog_sha256",
        "shards",
        "source_status_by_venue",
        "status",
        "unavailable_receipts",
    }
    legacy_expected_fields = expected_fields - {"prospective_slot_coverage"}
    if (
        set(manifest) not in (expected_fields, legacy_expected_fields)
        or manifest.get("schema_version") != 1
        or manifest.get("model_version") != MODEL_VERSION
        or manifest.get("boundary") != BOUNDARY
        or manifest.get("economic_claim") != "NONE_RESEARCH_MECHANISM_ONLY"
        or manifest.get("status") != "READY_FOR_OFFLINE_VERIFICATION"
        or not isinstance(claimed, str)
        or claimed != expected_bundle_sha256
        or _sha256(canonical_json_bytes(body)) != claimed
    ):
        raise ValueError("prediction bundle manifest identity diverged")
    preregistration, campaign, contracts = _load_inputs(root)
    unavailable = _verify_unavailable_descriptors(
        root,
        manifest.get("unavailable_receipts"),
    )
    if (
        manifest.get("candidate_config_sha256") != preregistration.config_sha256
        or manifest.get("campaign_manifest_sha256") != campaign.get("manifest_sha256")
        or manifest.get("contracts")
        != {venue.value: contracts[venue].contract_sha256 for venue in contracts}
    ):
        raise ValueError("prediction bundle input bindings diverged")
    unavailable_slots = _validate_unavailable_campaign_bindings(
        unavailable,
        preregistration=preregistration,
        campaign_manifest=campaign,
        contracts=contracts,
    )
    raw_descriptors = manifest.get("shards")
    if not isinstance(raw_descriptors, list) or any(
        not isinstance(item, Mapping) for item in raw_descriptors
    ):
        raise ValueError("prediction bundle shard descriptors are invalid")
    descriptors = [cast(Mapping[str, Any], item) for item in raw_descriptors]
    if [str(item.get("key") or "") for item in descriptors] != sorted(
        str(item.get("key") or "") for item in descriptors
    ):
        raise ValueError("prediction bundle shard descriptors are not canonically ordered")
    shards, catalog, runner, seal = _derive(
        root,
        descriptors,
        preregistration,
        campaign,
        contracts,
    )
    slot_coverage = _prospective_slot_coverage(
        shards=shards,
        unavailable_slots=unavailable_slots,
        preregistration=preregistration,
        campaign_manifest=campaign,
    )
    runner = _bind_campaign_slot_coverage(runner, shards, slot_coverage)
    if "prospective_slot_coverage" in manifest:
        if manifest.get("prospective_slot_coverage") != slot_coverage:
            raise ValueError("prediction prospective slot coverage diverged")
    elif (
        descriptors
        or runner is not None
        or any(
            item.classification != UNBOUND_AVAILABILITY_OBSERVATION
            for item in unavailable
        )
    ):
        raise ValueError("legacy prediction bundle cannot omit prospective slot coverage")
    payloads, rows = _artifact_payloads(shards, catalog)
    expected_artifacts = _artifact_manifest(payloads, rows)
    if manifest.get("artifacts") != expected_artifacts:
        raise ValueError("prediction bundle derived artifact inventory diverged")
    for path, value in payloads.items():
        if _resolved_child(root, path).read_bytes() != value:
            raise ValueError(f"prediction bundle artifact differs from raw rebuild:{path}")
    child_hashes = sorted(item.dataset.dataset_sha256 for item in shards if item.dataset is not None)
    if (
        manifest.get("semantic_catalog_sha256") != catalog.catalog_sha256
        or manifest.get("child_dataset_sha256s") != child_hashes
        or manifest.get("dataset_bundle_sha256") != (None if runner is None else runner.dataset_sha256)
        or manifest.get("replay_seal_sha256") != (None if seal is None else seal.seal_sha256)
    ):
        raise ValueError("prediction bundle reconstructed identities diverged")
    raw_statuses = _canonical_mapping(
        manifest.get("source_status_by_venue"),
        label="prediction source status map",
    )
    statuses = {
        venue: str(raw_statuses.get(venue.value) or "")
        for venue in (Venue.POLYMARKET, Venue.KALSHI)
    }
    expected_statuses: dict[Venue, str] = {}
    for venue in (Venue.POLYMARKET, Venue.KALSHI):
        venue_shards = [item for item in shards if item.venue is venue]
        venue_receipts = [item for item in unavailable if item.venue is venue]
        if venue_shards and all(item.synthetic for item in venue_shards):
            if venue_receipts and any(
                item.classification != UNBOUND_AVAILABILITY_OBSERVATION
                for item in venue_receipts
            ):
                raise ValueError("synthetic bundle claims a campaign slot receipt")
            if venue_receipts:
                raise ValueError("prediction bundle includes and marks a venue unavailable")
            expected_statuses[venue] = SYNTHETIC_SOURCE_STATUS
        elif venue_shards:
            if any(item.classification == UNBOUND_AVAILABILITY_OBSERVATION for item in venue_receipts):
                raise ValueError("public campaign bundle contains unbound availability evidence")
            expected_statuses[venue] = PublicSourceStatus.OBSERVED_PUBLICLY.value
        elif venue_receipts and all(
            item.classification
            in {
                UNBOUND_AVAILABILITY_OBSERVATION,
                CAMPAIGN_BOUND_UNAVAILABILITY_RECEIPT,
            }
            for item in venue_receipts
        ):
            expected_statuses[venue] = PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE.value
        elif venue_receipts or slot_coverage is not None:
            expected_statuses[venue] = INSUFFICIENT_PUBLIC_CORPUS
        else:
            raise ValueError("prediction bundle lacks an authenticated source status receipt")
    if statuses != expected_statuses:
        raise ValueError("prediction bundle source status diverged from its shards")
    return VerifiedPredictionResearchBundle(
        root=root,
        manifest=manifest,
        preregistration=preregistration,
        campaign_manifest=campaign,
        contracts=contracts,
        semantic_catalog=catalog,
        shards=shards,
        campaign_runner=runner,
        replay_seal=seal,
        source_status_by_venue=statuses,
        unavailable_sources=unavailable,
        prospective_slot_coverage=slot_coverage,
    )


def _evidence_for_opportunities(
    verified: VerifiedPredictionResearchBundle,
) -> tuple[
    dict[PredictionLegEvidenceKey, PredictionSettlementEvidence],
    dict[PredictionLegEvidenceKey, MakerAggressorEvidence],
]:
    runner = verified.campaign_runner
    seal = verified.replay_seal
    if runner is None or seal is None:
        raise ValueError("prediction bundle has no authenticated replayable dataset")
    opportunities = runner.enumerate_opportunities(seal=seal)
    settlements = tuple(item for shard in verified.shards for item in shard.settlements)
    aggressors = tuple(item for shard in verified.shards for item in shard.aggressors)
    by_dataset = {
        shard.dataset.dataset_sha256: shard
        for shard in verified.shards
        if shard.dataset is not None
    }
    settlement_map: dict[PredictionLegEvidenceKey, PredictionSettlementEvidence] = {}
    aggressor_map: dict[PredictionLegEvidenceKey, MakerAggressorEvidence] = {}
    for opportunity in opportunities:
        if opportunity.status is not PredictionOpportunityStatus.CANDIDATE:
            continue
        trigger = next(
            item
            for item in opportunity.signals
            if item.point_in_time_id == opportunity.trigger_point_in_time_id
        )
        execution_signals = tuple(
            signal
            for signal in opportunity.signals
            if f"{signal.venue.value}:{signal.market_id}:{signal.outcome_id}"
            in opportunity.execution_sequence
        )
        signal_rules: dict[PredictionLegEvidenceKey, str] = {}
        signals_by_lineage: dict[
            tuple[Venue, str, str],
            list[tuple[Any, PredictionLegEvidenceKey]],
        ] = {}
        for signal in execution_signals:
            shard = by_dataset.get(signal.dataset_sha256)
            if shard is None or shard.dataset is None:
                raise ValueError("prediction opportunity refers to an absent shard")
            matching_rows = [
                row
                for row in shard.dataset.rows
                if row.point_in_time_id == signal.point_in_time_id
                and row.market_id == signal.market_id
                and row.outcome_id == signal.outcome_id
            ]
            if len(matching_rows) != 1:
                raise ValueError("prediction opportunity row lookup is ambiguous")
            rule_version_id = matching_rows[0].rule_version_id
            key = PredictionLegEvidenceKey.from_signal(opportunity.opportunity_id, signal)
            signal_rules[key] = rule_version_id
            signals_by_lineage.setdefault(
                (signal.venue, signal.market_id, rule_version_id),
                [],
            ).append((signal, key))
        for (venue, market_id, rule_version_id), lineage_signals in signals_by_lineage.items():
            required_outcomes = {signal.outcome_id for signal, _key in lineage_signals}
            lifecycle_candidates = [
                item
                for item in settlements
                if item.venue is venue
                and item.market_id == market_id
                and item.rule_version_id == rule_version_id
                and item.received_time_utc_ns < seal.evidence_cutoff_utc_ns_exclusive
            ]
            selected_event = _select_latest_atomic_lifecycle_event(
                lifecycle_candidates,
                required_outcomes=required_outcomes,
            )
            for signal, key in lineage_signals:
                selected = selected_event.get(signal.outcome_id)
                if selected is None:
                    raise AssertionError("atomic lifecycle event lost a required outcome")
                settlement_map[key] = selected
        for signal in execution_signals:
            key = PredictionLegEvidenceKey.from_signal(opportunity.opportunity_id, signal)
            if key not in signal_rules:
                raise AssertionError("prediction execution signal rule was not reconstructed")
            matching_aggressors = [
                item
                for item in aggressors
                if item.venue is signal.venue
                and item.market_id == signal.market_id
                and item.outcome_id == signal.outcome_id
                and trigger.received_time_utc_ns < item.received_time_utc_ns
                < seal.evidence_cutoff_utc_ns_exclusive
                and (item.collector_identity, item.session_identity)
                == (signal.collector_identity, signal.session_identity)
            ]
            if matching_aggressors:
                aggressor_map[key] = min(
                    matching_aggressors,
                    key=lambda item: (item.received_time_utc_ns, item.raw_ref.arrival_sequence),
                )
    return settlement_map, aggressor_map


def replay_verified_prediction_bundle(
    verified: VerifiedPredictionResearchBundle,
) -> PredictionCampaignReplayReport:
    refreshed = verify_prediction_research_bundle(
        verified.root,
        expected_bundle_sha256=verified.bundle_sha256,
    )
    if refreshed.campaign_runner is None or refreshed.replay_seal is None:
        raise ValueError("prediction bundle is not replayable because no book dataset was built")
    settlements, aggressors = _evidence_for_opportunities(refreshed)
    return refreshed.campaign_runner.run_campaign(
        seal=refreshed.replay_seal,
        settlements=settlements,
        maker_aggressors=aggressors,
    )


def verify_prediction_campaign_replay_artifact(
    verified: VerifiedPredictionResearchBundle,
    artifact: bytes,
) -> PredictionCampaignReplayReport:
    rebuilt = replay_verified_prediction_bundle(verified)
    expected = canonical_json_bytes(rebuilt.to_dict()) + b"\n"
    if artifact != expected:
        raise ValueError("prediction campaign replay artifact differs from in-process raw rebuild")
    return rebuilt


def evaluate_verified_prediction_bundle(
    verified: VerifiedPredictionResearchBundle,
    campaign_replay: PredictionCampaignReplayReport,
) -> dict[str, CanonicalValue]:
    refreshed = verify_prediction_research_bundle(
        verified.root,
        expected_bundle_sha256=verified.bundle_sha256,
    )
    canonical_replay = replay_verified_prediction_bundle(refreshed)
    if (
        campaign_replay.report_sha256 != canonical_replay.report_sha256
        or campaign_replay.to_dict() != canonical_replay.to_dict()
    ):
        raise ValueError("prediction evaluation replay differs from canonical raw resolver")
    statuses = set(refreshed.source_status_by_venue.values())
    source_status = (
        PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE
        if PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE.value in statuses
        else PublicSourceStatus.OBSERVED_PUBLICLY
    )
    if refreshed.replay_seal is None:
        raise ValueError("prediction bundle has no replay seal")
    return evaluate_preregistered(
        preregistration=refreshed.preregistration,
        selection_view=refreshed.replay_seal.selection_view,
        campaign_manifest=refreshed.campaign_manifest,
        campaign_replay=cast(PredictionCampaignEvaluationReport, campaign_replay),
        source_status=source_status,
        source_status_by_venue=refreshed.source_status_by_venue,
    )


__all__ = [
    "CAMPAIGN_BOUND_EXCLUDED_SLOT_RECEIPT",
    "CAMPAIGN_BOUND_UNAVAILABILITY_RECEIPT",
    "MODEL_VERSION",
    "UNBOUND_AVAILABILITY_OBSERVATION",
    "PredictionBundleSource",
    "PredictionPublicSourceInvalidReceipt",
    "PredictionUnavailableSource",
    "VerifiedPredictionResearchBundle",
    "build_prediction_research_bundle",
    "evaluate_verified_prediction_bundle",
    "replay_verified_prediction_bundle",
    "verify_prediction_campaign_replay_artifact",
    "verify_prediction_research_bundle",
]
