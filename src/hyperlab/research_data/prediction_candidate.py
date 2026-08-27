from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_HALF_UP, Decimal, InvalidOperation
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from hyperlab.backtest.protocol import SelectionSplitView, SplitPlan, TimeRange

from .canonical import CanonicalValue, canonical_json_bytes, canonical_value
from .derived import DerivedDatasetIdentity
from .envelope import PublicDataEnvelope, Venue
from .prediction import SemanticCatalog
from .prediction_contracts import (
    BOUNDARY,
    EvidenceClassification,
    OfficialPublicContract,
    PredictionIdentityGraph,
    revalidate_prediction_graph,
)
from .prediction_evidence import (
    PredictionRawEvidenceIndex,
    PredictionRawRecordRef,
    prediction_raw_record_ref,
    prediction_raw_records,
)
from .prediction_time import prediction_rfc3339_to_ns
from .segments import ManifestRecord, ResearchSegmentReader

READY = "READY_FOR_PROSPECTIVE_EVIDENCE"
ECONOMIC_NOT_AVAILABLE = "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE"
INSUFFICIENT_PUBLIC_CORPUS = "INSUFFICIENT_PUBLIC_CORPUS"
PUBLIC_SOURCE_UNAVAILABLE = "PUBLIC_SOURCE_UNAVAILABLE"
AWAITING_HUMAN_EXECUTION = "AWAITING_HUMAN_EXECUTION"
DATASET_MODEL_VERSION = "PREDICTION_MARKETS_POINT_IN_TIME_V1"
TRADE_DATASET_MODEL_VERSION = "PREDICTION_MARKETS_TRADES_POINT_IN_TIME_V1"
GHOST_MODEL_VERSION = "PREDICTION_MARKETS_GHOST_V1"

_ZERO = Decimal("0")
_ONE = Decimal("1")
_SHA256_LENGTH = 64
RawPosition = tuple[int, int]


def _mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, *, label: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{label} must be an array")
    return value


def _text(value: object, *, label: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label} must be non-empty text")
    return value


def _decimal(value: object, *, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{label} must be an exact decimal") from error
    if not result.is_finite():
        raise ValueError(f"{label} must be finite")
    return result


def _atomic_new_bytes(path: Path, value: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("xb") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


@dataclass(frozen=True, slots=True)
class CandidateVariant:
    variant_id: str
    family_id: str
    role: str
    parameters: Mapping[str, CanonicalValue]
    falsifiers: tuple[str, ...]

    @property
    def parameters_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.parameters)).hexdigest()


@dataclass(frozen=True, slots=True)
class PredictionRunnerPolicy:
    model_version: str
    trigger_rule: str
    signal_clock: str
    decision_delay_ns: int
    maximum_book_age_ns: int
    quantity: Decimal
    maximum_quantity: Decimal
    embargo_seconds: int
    universe: Mapping[str, CanonicalValue]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> PredictionRunnerPolicy:
        expected = {
            "decision_delay_ns",
            "embargo_seconds",
            "maximum_book_age_ns",
            "maximum_quantity",
            "model_version",
            "quantity",
            "signal_clock",
            "trigger_rule",
            "universe",
        }
        if set(value) != expected:
            raise ValueError("prediction runner policy fields differ from schema v1")
        integer_values: dict[str, int] = {}
        for key in ("decision_delay_ns", "embargo_seconds", "maximum_book_age_ns"):
            item = value.get(key)
            if type(item) is not int or item <= 0:
                raise ValueError(f"prediction runner {key} must be a positive integer")
            integer_values[key] = item
        quantity = _decimal(value.get("quantity"), label="runner quantity")
        maximum_quantity = _decimal(
            value.get("maximum_quantity"), label="runner maximum quantity"
        )
        if quantity <= 0 or maximum_quantity < quantity:
            raise ValueError("prediction runner quantity or cap is invalid")
        universe = canonical_value(_mapping(value.get("universe"), label="runner universe"))
        if not isinstance(universe, dict):
            raise AssertionError("prediction runner universe must remain an object")
        if universe != {
            "market_statuses": ["ACTIVE"],
            "selection": "ALL_AUTHENTICATED_COMPLETE_RELATIONS_IN_COLLECTION",
            "venues": ["POLYMARKET", "KALSHI"],
        }:
            raise ValueError("prediction runner universe is not the frozen public-data universe")
        model_version = _text(value.get("model_version"), label="runner model version")
        trigger_rule = _text(value.get("trigger_rule"), label="runner trigger rule")
        signal_clock = _text(value.get("signal_clock"), label="runner signal clock")
        if (
            model_version != "PREDICTION_OPPORTUNITY_RUNNER_V1"
            or trigger_rule != "EVERY_AUTHENTICATED_FULL_BOOK_SNAPSHOT"
            or signal_clock != "RECEIVED_UTC_AND_LOCAL_MONOTONIC"
        ):
            raise ValueError("prediction runner causal policy is unsupported")
        return cls(
            model_version=model_version,
            trigger_rule=trigger_rule,
            signal_clock=signal_clock,
            decision_delay_ns=integer_values["decision_delay_ns"],
            maximum_book_age_ns=integer_values["maximum_book_age_ns"],
            quantity=quantity,
            maximum_quantity=maximum_quantity,
            embargo_seconds=integer_values["embargo_seconds"],
            universe=universe,
        )

    @property
    def policy_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.to_dict())).hexdigest()

    def to_dict(self) -> dict[str, CanonicalValue]:
        return {
            "decision_delay_ns": self.decision_delay_ns,
            "embargo_seconds": self.embargo_seconds,
            "maximum_book_age_ns": self.maximum_book_age_ns,
            "maximum_quantity": format(self.maximum_quantity, "f"),
            "model_version": self.model_version,
            "quantity": format(self.quantity, "f"),
            "signal_clock": self.signal_clock,
            "trigger_rule": self.trigger_rule,
            "universe": dict(self.universe),
        }


@dataclass(frozen=True, slots=True)
class PredictionCollectionPlan:
    venue: Venue
    attempt_id: str
    feeds: tuple[str, ...]
    census_limit: int
    duration_seconds: int
    max_network_calls: int
    max_frames: int
    max_bytes: int
    max_segment_bytes: int
    max_segments: int
    rotation_seconds: int
    progress_interval_seconds: int

    @classmethod
    def from_mapping(
        cls,
        venue: Venue,
        value: Mapping[str, Any],
    ) -> PredictionCollectionPlan:
        expected = {
            "attempt_id",
            "census_limit",
            "duration_seconds",
            "feeds",
            "instrument_selection",
            "max_bytes",
            "max_frames",
            "max_network_calls",
            "max_segment_bytes",
            "max_segments",
            "one_shot",
            "progress_interval_seconds",
            "retry_policy",
            "rotation_seconds",
        }
        if set(value) != expected:
            raise ValueError("prediction collection plan fields differ from schema v1")
        if (
            value.get("instrument_selection") != "CENSUS_ONLY"
            or value.get("one_shot") is not True
            or value.get("retry_policy") != "NONE_AFTER_TERMINAL_RESULT"
        ):
            raise ValueError("prediction collection plan must be one-shot census-only")
        feeds = tuple(
            _text(item, label="prediction collection feed")
            for item in _sequence(value.get("feeds"), label="prediction collection feeds")
        )
        if not feeds or len(feeds) != len(set(feeds)):
            raise ValueError("prediction collection plan feeds must be unique")
        numeric: dict[str, int] = {}
        for key in (
            "census_limit",
            "duration_seconds",
            "max_network_calls",
            "max_frames",
            "max_bytes",
            "max_segment_bytes",
            "max_segments",
            "rotation_seconds",
            "progress_interval_seconds",
        ):
            item = value.get(key)
            if type(item) is not int or item <= 0:
                raise ValueError(f"prediction collection {key} must be a positive integer")
            numeric[key] = item
        if (
            numeric["duration_seconds"] > 300
            or numeric["max_network_calls"] > 1_000
            or numeric["max_frames"] > 50_000
            or numeric["max_bytes"] > 64 * 1024 * 1024
            or numeric["max_segment_bytes"] > numeric["max_bytes"]
            or numeric["max_segments"] > 100
        ):
            raise ValueError("prediction collection plan exceeds the bounded public-probe caps")
        census = numeric["census_limit"]
        if venue is Venue.POLYMARKET:
            token_http_count = len(
                set(feeds) & {"fees", "last_trade_price", "order_book", "tick_size"}
            )
            minimum_calls = (
                1
                + census
                + (census if "events" in feeds else 0)
                + 2 * census * token_http_count
                + (census if "public_trades" in feeds else 0)
                + (1 if set(feeds) & {"best_bid_ask", "market_lifecycle", "price_change", "tick_size_change"} else 0)
            )
        else:
            global_calls = len(
                set(feeds) & {"exchange_schedule", "exchange_status", "incentives"}
            )
            current_census = 1 if set(feeds) & {"events", "markets"} else 0
            historical_census = (
                2
                if set(feeds) & {"historical_markets", "historical_trades"}
                else 0
            )
            minimum_calls = global_calls + current_census + historical_census + 14 * census
        if numeric["max_network_calls"] < minimum_calls:
            raise ValueError(
                "prediction collection max_network_calls cannot cover its frozen census"
            )
        return cls(
            venue=venue,
            attempt_id=_text(value.get("attempt_id"), label="prediction attempt id"),
            feeds=feeds,
            census_limit=numeric["census_limit"],
            duration_seconds=numeric["duration_seconds"],
            max_network_calls=numeric["max_network_calls"],
            max_frames=numeric["max_frames"],
            max_bytes=numeric["max_bytes"],
            max_segment_bytes=numeric["max_segment_bytes"],
            max_segments=numeric["max_segments"],
            rotation_seconds=numeric["rotation_seconds"],
            progress_interval_seconds=numeric["progress_interval_seconds"],
        )

    def collection_id(self, campaign_id: str) -> str:
        return f"{_text(campaign_id, label='campaign id')}-{self.attempt_id}"

    def to_dict(self, *, campaign_id: str) -> dict[str, CanonicalValue]:
        return {
            "attempt_id": self.attempt_id,
            "census_limit": self.census_limit,
            "collection_id": self.collection_id(campaign_id),
            "duration_seconds": self.duration_seconds,
            "feeds": list(self.feeds),
            "instrument_selection": "CENSUS_ONLY",
            "max_bytes": self.max_bytes,
            "max_frames": self.max_frames,
            "max_network_calls": self.max_network_calls,
            "max_segment_bytes": self.max_segment_bytes,
            "max_segments": self.max_segments,
            "one_shot": True,
            "progress_interval_seconds": self.progress_interval_seconds,
            "retry_policy": "NONE_AFTER_TERMINAL_RESULT",
            "rotation_seconds": self.rotation_seconds,
            "venue": self.venue.value,
        }


@dataclass(frozen=True, slots=True)
class PredictionProspectiveShardPolicy:
    campaign_days: int
    cadence_seconds: int
    collection_duration_seconds: int
    expected_shards_per_venue: int
    identity_scheme: str
    missed_slot_policy: str
    ordering: str
    overlap_policy: str
    retry_policy: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> PredictionProspectiveShardPolicy:
        expected = {
            "campaign_days",
            "cadence_seconds",
            "collection_duration_seconds",
            "expected_shards_per_venue",
            "identity_scheme",
            "missed_slot_policy",
            "ordering",
            "overlap_policy",
            "retry_policy",
        }
        if set(value) != expected:
            raise ValueError("prediction prospective shard policy fields differ from schema v1")
        numeric: dict[str, int] = {}
        for key in (
            "campaign_days",
            "cadence_seconds",
            "collection_duration_seconds",
            "expected_shards_per_venue",
        ):
            item = value.get(key)
            if type(item) is not int or item <= 0:
                raise ValueError(f"prediction prospective shard {key} must be positive")
            numeric[key] = item
        if (
            numeric["collection_duration_seconds"] >= numeric["cadence_seconds"]
            or numeric["campaign_days"] * 86_400 % numeric["cadence_seconds"] != 0
            or numeric["expected_shards_per_venue"]
            != numeric["campaign_days"] * 86_400 // numeric["cadence_seconds"]
        ):
            raise ValueError("prediction prospective shard cadence or count is inconsistent")
        strings = {
            key: _text(value.get(key), label=f"prediction prospective shard {key}")
            for key in (
                "identity_scheme",
                "missed_slot_policy",
                "ordering",
                "overlap_policy",
                "retry_policy",
            )
        }
        if strings != {
            "identity_scheme": "SHA256_CAMPAIGN_VENUE_ORDINAL_SCHEDULED_START_V1",
            "missed_slot_policy": "RECORD_GAP_NO_BACKFILL",
            "ordering": "SCHEDULED_START_THEN_CHILD_MANIFEST_SHA256",
            "overlap_policy": "STRICT_NON_OVERLAP",
            "retry_policy": "NO_RETRY_AFTER_TERMINAL_RESULT",
        }:
            raise ValueError("prediction prospective shard policy is not fail-closed")
        return cls(
            campaign_days=numeric["campaign_days"],
            cadence_seconds=numeric["cadence_seconds"],
            collection_duration_seconds=numeric["collection_duration_seconds"],
            expected_shards_per_venue=numeric["expected_shards_per_venue"],
            identity_scheme=strings["identity_scheme"],
            missed_slot_policy=strings["missed_slot_policy"],
            ordering=strings["ordering"],
            overlap_policy=strings["overlap_policy"],
            retry_policy=strings["retry_policy"],
        )

    def to_dict(self) -> dict[str, CanonicalValue]:
        return {
            "campaign_days": self.campaign_days,
            "cadence_seconds": self.cadence_seconds,
            "collection_duration_seconds": self.collection_duration_seconds,
            "expected_shards_per_venue": self.expected_shards_per_venue,
            "identity_scheme": self.identity_scheme,
            "missed_slot_policy": self.missed_slot_policy,
            "ordering": self.ordering,
            "overlap_policy": self.overlap_policy,
            "retry_policy": self.retry_policy,
        }

    @property
    def policy_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.to_dict())).hexdigest()

    def scheduled_start(self, campaign_start: datetime, ordinal: int) -> datetime:
        if ordinal < 0 or ordinal >= self.expected_shards_per_venue:
            raise ValueError("prediction prospective shard ordinal is outside the campaign")
        if campaign_start.tzinfo is None or campaign_start.utcoffset() is None:
            raise ValueError("prediction prospective campaign start must be timezone-aware")
        return campaign_start.astimezone(UTC) + timedelta(
            seconds=ordinal * self.cadence_seconds
        )

    def collection_id(
        self,
        *,
        base_collection_id: str,
        campaign_manifest_sha256: str,
        venue: Venue,
        ordinal: int,
        scheduled_start: datetime,
    ) -> str:
        if len(campaign_manifest_sha256) != _SHA256_LENGTH:
            raise ValueError("prediction shard campaign hash is invalid")
        identity = {
            "campaign_manifest_sha256": campaign_manifest_sha256,
            "ordinal": ordinal,
            "scheduled_start_utc": scheduled_start.astimezone(UTC)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
            "venue": venue.value,
        }
        digest = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        return f"{base_collection_id}-shard-{ordinal:04d}-{digest[:16]}"


@dataclass(frozen=True, slots=True)
class CandidatePreregistration:
    candidate_id: str
    status: str
    economic_status: str
    primary_technical_family: str
    variants: tuple[CandidateVariant, ...]
    holdout_access: str
    train_days: int
    validation_days: int
    final_test_days: int
    minimum_observations_per_variant: int
    minimum_markets: int
    familywise_alpha: Decimal
    runner_policy: PredictionRunnerPolicy
    collection_plans: Mapping[Venue, PredictionCollectionPlan]
    prospective_shard_policy: PredictionProspectiveShardPolicy
    payload: Mapping[str, CanonicalValue]
    config_sha256: str

    @classmethod
    def from_bytes(cls, raw: bytes) -> CandidatePreregistration:
        try:
            decoded = json.loads(
                raw.decode("utf-8"),
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("prediction candidate config must be strict UTF-8 JSON") from error
        root = _mapping(decoded, label="prediction candidate config")
        if root.get("schema_version") != 1 or root.get("boundary") != BOUNDARY:
            raise ValueError("prediction candidate schema or boundary is invalid")
        if root.get("status") != READY:
            raise ValueError("prediction candidate must remain prospective-evidence only")
        economic = _mapping(root.get("economic_fingerprint"), label="economic fingerprint")
        if economic.get("selection") != "NONE_BEFORE_SUFFICIENT_PUBLIC_OOS_EVIDENCE":
            raise ValueError("economic fingerprint cannot be preselected")
        if economic.get("status") != ECONOMIC_NOT_AVAILABLE:
            raise ValueError("candidate config cannot claim economic evidence")
        primary_family = _text(root.get("primary_technical_family"), label="primary technical family")
        variants: list[CandidateVariant] = []
        primary_roles = 0
        for raw_family in _sequence(root.get("families"), label="candidate families"):
            family = _mapping(raw_family, label="candidate family")
            family_id = _text(family.get("family_id"), label="family id")
            role = _text(family.get("role"), label="family role")
            if role == "PRIMARY_TECHNICAL_FAMILY_NOT_ECONOMIC_SELECTION":
                primary_roles += 1
                if family_id != primary_family:
                    raise ValueError("primary technical family identity diverged")
            falsifiers = tuple(
                _text(item, label="falsifier")
                for item in _sequence(family.get("falsifiers"), label="falsifiers")
            )
            if not falsifiers:
                raise ValueError("every candidate family requires falsifiers")
            for raw_variant in _sequence(family.get("variants"), label="family variants"):
                variant = _mapping(raw_variant, label="candidate variant")
                canonical_parameters = canonical_value(
                    _mapping(variant.get("parameters"), label="variant parameters")
                )
                if not isinstance(canonical_parameters, dict):
                    raise AssertionError("variant parameters must remain an object")
                variants.append(
                    CandidateVariant(
                        variant_id=_text(variant.get("variant_id"), label="variant id"),
                        family_id=family_id,
                        role=role,
                        parameters=canonical_parameters,
                        falsifiers=falsifiers,
                    )
                )
        if primary_roles != 1 or not variants:
            raise ValueError("candidate requires one technical primary and registered variants")
        if len({item.variant_id for item in variants}) != len(variants):
            raise ValueError("candidate variant ids must be unique")
        holdout = _mapping(root.get("holdout"), label="holdout")
        if holdout.get("access") != "SEALED" or holdout.get("reporting_while_sealed") != "NO_HOLDOUT_METRICS":
            raise ValueError("candidate final test must remain sealed without metric leakage")
        splits = _mapping(root.get("splits"), label="chronological splits")
        if splits.get("chronology") != ["TRAIN", "VALIDATION", "FINAL_TEST"]:
            raise ValueError("candidate splits must be chronological")
        evaluation = _mapping(root.get("evaluation"), label="candidate evaluation")
        if (
            evaluation.get("multiple_testing_population") != "ALL_REGISTERED_VARIANTS_INCLUDING_LOSERS"
            or evaluation.get("confidence_method") != "BONFERRONI_CANTELLI_LCB"
        ):
            raise ValueError("candidate correction must include every registered variant")
        canonical = canonical_value(root)
        if not isinstance(canonical, dict):
            raise AssertionError("candidate config root must remain an object")
        body = canonical_json_bytes(canonical)
        train_days = int(splits["train_days"])
        validation_days = int(splits["validation_days"])
        final_test_days = int(splits["final_test_days"])
        minimum_observations = int(evaluation["minimum_observations_per_variant"])
        minimum_markets = int(evaluation["minimum_markets"])
        familywise_alpha = _decimal(evaluation["familywise_alpha"], label="familywise alpha")
        if (
            min(
                train_days,
                validation_days,
                final_test_days,
                minimum_observations,
                minimum_markets,
            )
            <= 0
        ):
            raise ValueError("candidate windows and corpus minima must be positive")
        if familywise_alpha <= 0 or familywise_alpha >= 1:
            raise ValueError("candidate familywise alpha must be within (0,1)")
        runner_policy = PredictionRunnerPolicy.from_mapping(
            _mapping(root.get("runner_policy"), label="prediction runner policy")
        )
        raw_collection_plans = _mapping(
            root.get("collection_plans"), label="prediction collection plans"
        )
        if set(raw_collection_plans) != {Venue.POLYMARKET.value, Venue.KALSHI.value}:
            raise ValueError("prediction candidate requires both frozen venue collection plans")
        collection_plans = {
            venue: PredictionCollectionPlan.from_mapping(
                venue,
                _mapping(raw_collection_plans[venue.value], label=f"{venue.value} collection plan"),
            )
            for venue in (Venue.POLYMARKET, Venue.KALSHI)
        }
        prospective_shard_policy = PredictionProspectiveShardPolicy.from_mapping(
            _mapping(
                root.get("prospective_shard_policy"),
                label="prediction prospective shard policy",
            )
        )
        if (
            prospective_shard_policy.campaign_days
            != train_days + validation_days + final_test_days
            or any(
                item.duration_seconds
                != prospective_shard_policy.collection_duration_seconds
                for item in collection_plans.values()
            )
        ):
            raise ValueError("prediction prospective shards diverge from splits or collection plans")
        return cls(
            candidate_id=_text(root.get("candidate_id"), label="candidate id"),
            status=READY,
            economic_status=ECONOMIC_NOT_AVAILABLE,
            primary_technical_family=primary_family,
            variants=tuple(variants),
            holdout_access="SEALED",
            train_days=train_days,
            validation_days=validation_days,
            final_test_days=final_test_days,
            minimum_observations_per_variant=minimum_observations,
            minimum_markets=minimum_markets,
            familywise_alpha=familywise_alpha,
            runner_policy=runner_policy,
            collection_plans=collection_plans,
            prospective_shard_policy=prospective_shard_policy,
            payload=canonical,
            config_sha256=hashlib.sha256(body).hexdigest(),
        )

    @classmethod
    def from_path(cls, path: Path) -> CandidatePreregistration:
        return cls.from_bytes(path.read_bytes())

    def require_variant(
        self,
        variant_id: str,
        *,
        parameters_sha256: str,
    ) -> CandidateVariant:
        matches = [item for item in self.variants if item.variant_id == variant_id]
        if len(matches) != 1:
            raise ValueError("prediction intent variant is not preregistered")
        variant = matches[0]
        if variant.parameters_sha256 != parameters_sha256:
            raise ValueError("prediction intent variant parameters diverged from preregistration")
        return variant


def prediction_prospective_shard_ordinal(
    *,
    preregistration: CandidatePreregistration,
    campaign_manifest: Mapping[str, object],
    venue: Venue,
    collection_id: str,
) -> int:
    """Recompute and authenticate one preregistered prospective shard identity."""

    campaign_hash = _text(
        campaign_manifest.get("manifest_sha256"),
        label="prediction shard campaign hash",
    )
    if len(campaign_hash) != _SHA256_LENGTH or any(
        character not in "0123456789abcdef" for character in campaign_hash
    ):
        raise ValueError("prediction shard campaign hash is invalid")
    campaign_id = _text(campaign_manifest.get("campaign_id"), label="campaign id")
    base_collection_id = preregistration.collection_plans[venue].collection_id(campaign_id)
    prefix = f"{base_collection_id}-shard-"
    if not collection_id.startswith(prefix):
        raise ValueError("prediction collection is not a prospective campaign shard")
    suffix = collection_id.removeprefix(prefix)
    parts = suffix.split("-")
    if (
        len(parts) != 2
        or len(parts[0]) != 4
        or not parts[0].isdigit()
        or len(parts[1]) != 16
        or any(character not in "0123456789abcdef" for character in parts[1])
    ):
        raise ValueError("prediction prospective shard identity is malformed")
    ordinal = int(parts[0])
    try:
        campaign_start = datetime.fromisoformat(
            _text(
                campaign_manifest.get("starts_at_utc"),
                label="campaign start",
            ).replace("Z", "+00:00")
        )
        scheduled_start = preregistration.prospective_shard_policy.scheduled_start(
            campaign_start,
            ordinal,
        )
    except ValueError as error:
        raise ValueError("prediction prospective shard schedule is invalid") from error
    expected = preregistration.prospective_shard_policy.collection_id(
        base_collection_id=base_collection_id,
        campaign_manifest_sha256=campaign_hash,
        venue=venue,
        ordinal=ordinal,
        scheduled_start=scheduled_start,
    )
    if collection_id != expected:
        raise ValueError("prediction prospective shard digest diverged")
    return ordinal


class FeeModel(StrEnum):
    POLYMARKET_QUADRATIC = "POLYMARKET_QUADRATIC"
    KALSHI_QUADRATIC = "KALSHI_QUADRATIC"
    ZERO = "ZERO"
    UNKNOWN = "UNKNOWN"


def build_prediction_split_plan(
    *,
    preregistration: CandidatePreregistration,
    dataset_sha256: str,
    prospective_start: datetime,
) -> SplitPlan:
    if prospective_start.tzinfo is None or prospective_start.utcoffset() is None:
        raise ValueError("prediction split start must be timezone-aware")
    start = prospective_start.astimezone(UTC)
    train_end = start + timedelta(days=preregistration.train_days)
    validation_end = train_end + timedelta(days=preregistration.validation_days)
    final_end = validation_end + timedelta(days=preregistration.final_test_days)
    return SplitPlan(
        train=TimeRange(start, train_end),
        validation=TimeRange(train_end, validation_end),
        test=TimeRange(validation_end, final_end),
        dataset_hash=dataset_sha256,
    )


def verify_prediction_collection_plan_payload(
    payload: Mapping[str, object],
    plan: PredictionCollectionPlan,
) -> None:
    expected: dict[str, CanonicalValue] = {
        "census_limit": plan.census_limit,
        "duration_seconds": plan.duration_seconds,
        "feeds": list(plan.feeds),
        "instruments": [],
        "max_bytes": plan.max_bytes,
        "max_frames": plan.max_frames,
        "max_network_calls": plan.max_network_calls,
        "max_segment_bytes": plan.max_segment_bytes,
        "max_segments": plan.max_segments,
        "progress_interval_seconds": format(plan.progress_interval_seconds, "g"),
        "rotation_seconds": format(plan.rotation_seconds, "g"),
        "venue": plan.venue.value,
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        raise ValueError("prediction collection binding diverged from the frozen plan")


@dataclass(frozen=True, slots=True)
class PredictionCollectionBinding:
    venue: Venue
    collection_id: str
    campaign_manifest_sha256: str
    candidate_config_sha256: str
    official_contract_sha256: str
    probe_binding_sha256: str
    payload: Mapping[str, CanonicalValue]
    terminal_result_sha256: str | None = None
    raw_manifest_sha256: str | None = None
    raw_root_sha256: str | None = None
    frame_count: int | None = None
    terminal_health: str | None = None
    observed_feeds: tuple[str, ...] = ()

    def verify_collection_plan(self, plan: PredictionCollectionPlan) -> None:
        if plan.venue is not self.venue:
            raise ValueError("prediction collection plan venue diverged")
        verify_prediction_collection_plan_payload(self.payload, plan)

    @classmethod
    def from_bytes(cls, raw: bytes) -> PredictionCollectionBinding:
        try:
            decoded = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("prediction collection binding must be strict UTF-8 JSON") from error
        root = _mapping(decoded, label="prediction collection binding")
        expected = {
            "boundary",
            "campaign_manifest_sha256",
            "candidate_config_sha256",
            "census_limit",
            "collection_id",
            "collection_cutoff_utc_ns_exclusive",
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
        if set(root) != expected:
            raise ValueError("prediction collection binding fields differ from probe schema v1")
        if (
            root.get("boundary") != BOUNDARY
            or root.get("schema_version") != 1
            or root.get("proxy_policy") != "DIRECT_ONLY_ENVIRONMENT_PROXY_DISABLED"
        ):
            raise ValueError("prediction collection binding boundary or schema is invalid")
        cutoff = root.get("collection_cutoff_utc_ns_exclusive")
        if type(cutoff) is not int or cutoff <= 0:
            raise ValueError("prediction collection binding slot cutoff is invalid")
        claimed = _text(root.get("probe_binding_sha256"), label="probe binding hash")
        payload_raw = {key: value for key, value in root.items() if key != "probe_binding_sha256"}
        canonical_payload = canonical_value(payload_raw)
        if not isinstance(canonical_payload, dict):
            raise AssertionError("prediction collection binding payload must remain an object")
        computed = hashlib.sha256(canonical_json_bytes(canonical_payload)).hexdigest()
        if claimed != computed:
            raise ValueError("prediction collection binding self-hash diverged")
        hashes = tuple(
            _text(root.get(key), label=key)
            for key in (
                "campaign_manifest_sha256",
                "candidate_config_sha256",
                "official_contract_sha256",
            )
        )
        if any(len(item) != _SHA256_LENGTH for item in (*hashes, claimed)):
            raise ValueError("prediction collection binding requires SHA-256 identities")
        return cls(
            venue=Venue(_text(root.get("venue"), label="binding venue")),
            collection_id=_text(root.get("collection_id"), label="binding collection id"),
            campaign_manifest_sha256=hashes[0],
            candidate_config_sha256=hashes[1],
            official_contract_sha256=hashes[2],
            probe_binding_sha256=claimed,
            payload=canonical_payload,
        )

    @classmethod
    def from_path(cls, path: Path) -> PredictionCollectionBinding:
        return cls.from_bytes(path.read_bytes())

    @classmethod
    def from_probe_output(cls, output_root: Path) -> PredictionCollectionBinding:
        reports_root = output_root / "reports"
        binding = cls.from_path(reports_root / "probe-config.json")
        result_path = reports_root / "result.json"
        try:
            raw_result = result_path.read_bytes()
            decoded = json.loads(raw_result.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("prediction collection requires a terminal result.json") from error
        result = _mapping(decoded, label="prediction terminal collection result")
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
            raise ValueError("prediction terminal collection result fields differ from schema v1")
        canonical_result = canonical_json_bytes(canonical_value(result))
        if raw_result != canonical_result:
            raise ValueError("prediction terminal collection result is not canonical JSON")
        manifest_sha256 = _text(
            result.get("manifest_sha256"), label="terminal raw manifest hash"
        )
        root_sha256 = _text(result.get("root_sha256"), label="terminal raw root hash")
        frame_count = result.get("frames")
        byte_count = result.get("bytes")
        segment_count = result.get("segments")
        elapsed_ms = result.get("elapsed_ms")
        network_calls = result.get("network_calls")
        terminal_health = _text(result.get("terminal_health"), label="terminal health")
        duration_seconds = binding.payload.get("duration_seconds")
        max_bytes = binding.payload.get("max_bytes")
        max_frames = binding.payload.get("max_frames")
        max_network_calls = binding.payload.get("max_network_calls")
        max_segment_bytes = binding.payload.get("max_segment_bytes")
        max_segments = binding.payload.get("max_segments")
        limits = (
            duration_seconds,
            max_bytes,
            max_frames,
            max_network_calls,
            max_segment_bytes,
            max_segments,
        )
        error_required = terminal_health in {
            "MAX_BYTES_REACHED",
            "PUBLIC_SOURCE_UNAVAILABLE_RECOVERED",
        }
        accepted_terminal = {
            "COMPLETE",
            "MAX_BYTES_REACHED",
            "MAX_DURATION_REACHED",
            "MAX_FRAMES_REACHED",
            "MAX_NETWORK_CALLS_REACHED",
            "MAX_SEGMENTS_REACHED",
            "PUBLIC_SOURCE_UNAVAILABLE_RECOVERED",
        }
        if (
            type(frame_count) is not int
            or frame_count <= 0
            or type(byte_count) is not int
            or byte_count <= 0
            or type(segment_count) is not int
            or segment_count <= 0
            or type(elapsed_ms) is not int
            or elapsed_ms < 0
            or type(network_calls) is not int
            or network_calls < 0
            or any(type(limit) is not int or limit <= 0 for limit in limits)
            or result.get("boundary") != BOUNDARY
            or result.get("schema_version") != 1
            or result.get("requested_duration_seconds") != duration_seconds
            or elapsed_ms > cast(int, duration_seconds) * 1_000
            or frame_count > cast(int, max_frames)
            or byte_count > cast(int, max_bytes)
            or network_calls > cast(int, max_network_calls)
            or segment_count > cast(int, max_segments)
            or len(manifest_sha256) != _SHA256_LENGTH
            or len(root_sha256) != _SHA256_LENGTH
            or any(
                character not in "0123456789abcdef"
                for value in (manifest_sha256, root_sha256)
                for character in value
            )
            or terminal_health not in accepted_terminal
            or (
                error_required
                and (
                    type(result.get("error")) is not str
                    or not cast(str, result.get("error")).strip()
                )
            )
            or (not error_required and result.get("error") is not None)
            or result.get("probe_binding_sha256") != binding.probe_binding_sha256
            or result.get("campaign_manifest_sha256") != binding.campaign_manifest_sha256
            or result.get("candidate_config_sha256") != binding.candidate_config_sha256
            or result.get("official_contract_sha256") != binding.official_contract_sha256
            or result.get("collection_id") != binding.collection_id
            or result.get("venue") != binding.venue.value
            or not isinstance(result.get("connection_attempts"), list)
            or not isinstance(result.get("limitations"), list)
            or any(
                type(result.get(key)) is not int or cast(int, result.get(key)) < 0
                for key in ("duplicates", "gaps", "queue_high_water", "reconnects")
            )
            or any(
                result.get(key) is not None and type(result.get(key)) is not int
                for key in ("source_timestamp_max_ns", "source_timestamp_min_ns")
            )
        ):
            raise ValueError("prediction terminal collection result is not admissible")
        reader = ResearchSegmentReader(output_root / "raw", manifest_sha256=manifest_sha256)
        if (
            reader.manifest.root_sha256 != root_sha256
            or reader.manifest.frame_count != frame_count
            or reader.manifest.stored_segment_bytes != byte_count
            or len(reader.manifest.segments) != segment_count
            or any(
                descriptor.stored_bytes > cast(int, max_segment_bytes)
                for descriptor in reader.manifest.segments
            )
        ):
            raise ValueError("prediction terminal result diverges from authenticated raw manifest")
        observed_feeds = tuple(sorted({item.feed_type for item in reader.replay()}))
        requested_feeds = set(
            _text(item, label="prediction requested feed")
            for item in _sequence(binding.payload.get("feeds"), label="prediction requested feeds")
        )
        dynamic_optional = {
            Venue.POLYMARKET: {
                "best_bid_ask",
                "market_lifecycle",
                "price_change",
                "tick_size_change",
            },
            Venue.KALSHI: set(),
        }[binding.venue]
        required_feeds = requested_feeds - dynamic_optional
        missing_feeds = required_feeds - set(observed_feeds)
        if missing_feeds:
            raise ValueError(
                f"prediction terminal collection lacks required feed coverage:{sorted(missing_feeds)}"
            )
        return replace(
            binding,
            terminal_result_sha256=hashlib.sha256(canonical_result).hexdigest(),
            raw_manifest_sha256=manifest_sha256,
            raw_root_sha256=root_sha256,
            frame_count=frame_count,
            terminal_health=terminal_health,
            observed_feeds=observed_feeds,
        )

    def verify(
        self,
        index: PredictionRawEvidenceIndex,
        *,
        contract: OfficialPublicContract,
    ) -> None:
        if contract.venue is not self.venue or contract.contract_sha256 != self.official_contract_sha256:
            raise ValueError("prediction collection official contract binding diverged")
        if (
            self.terminal_result_sha256 is None
            or self.raw_manifest_sha256 != index.manifest_sha256
            or self.raw_root_sha256 != index.root_sha256
            or self.frame_count != len(index.envelopes)
            or self.terminal_health is None
        ):
            raise ValueError("prediction collection lacks an authenticated terminal result")
        prefix = f"probe-binding-{self.probe_binding_sha256}:"
        if any(
            envelope.venue is not self.venue
            or envelope.provenance.collection_id != self.collection_id
            or not envelope.session_identity.startswith(prefix)
            for envelope in index.envelopes
        ):
            raise ValueError("prediction collection raw session binding diverged")


@dataclass(frozen=True, slots=True)
class PredictionFeeSchedule:
    schedule_id: str
    venue: Venue
    market_id: str
    outcome_ids: tuple[str, ...]
    classification: EvidenceClassification
    model: FeeModel
    effective_from_ns: int
    effective_to_ns: int | None
    taker_rate: Decimal | None
    maker_rate: Decimal | None
    multiplier: Decimal | None
    exponent: Decimal | None
    rounding_quantum: Decimal | None
    rounding_complete: bool
    rounding_scope: str
    account_precision_quantum: Decimal | None
    source_refs: tuple[PredictionRawRecordRef, ...]
    synthetic_fixture: bool = False

    def __post_init__(self) -> None:
        if (
            not self.schedule_id
            or not self.market_id
            or len(self.outcome_ids) < 2
            or len(self.outcome_ids) != len(set(self.outcome_ids))
            or any(not item for item in self.outcome_ids)
        ):
            raise ValueError("prediction fee schedule identity is incomplete")
        if (
            tuple(
                sorted(
                    set(self.source_refs),
                    key=lambda item: (item.arrival_sequence, item.raw_record_index),
                )
            )
            != self.source_refs
        ):
            raise ValueError("prediction fee source references must be unique and causal")
        if self.venue not in {Venue.POLYMARKET, Venue.KALSHI}:
            raise ValueError("prediction fee schedule venue is invalid")
        if self.effective_from_ns < 0 or (
            self.effective_to_ns is not None and self.effective_to_ns <= self.effective_from_ns
        ):
            raise ValueError("prediction fee schedule interval is invalid")
        values = (
            self.taker_rate,
            self.maker_rate,
            self.multiplier,
            self.exponent,
            self.rounding_quantum,
            self.account_precision_quantum,
        )
        if any(value is not None and (not value.is_finite() or value < 0) for value in values):
            raise ValueError("prediction fee schedule values cannot be negative")
        if self.model is FeeModel.UNKNOWN and any(value is not None for value in values):
            raise ValueError("unknown prediction fee schedule cannot carry numeric assumptions")
        if self.rounding_scope not in {
            "UNKNOWN",
            "PER_FILL",
            "PER_ORDER_CUMULATIVE",
        }:
            raise ValueError("prediction fee rounding scope is invalid")
        if self.model is FeeModel.UNKNOWN and self.rounding_scope != "UNKNOWN":
            raise ValueError("unknown prediction fee schedule needs unknown rounding")
        if self.model is FeeModel.ZERO and any(
            value not in {None, _ZERO} for value in (self.taker_rate, self.maker_rate)
        ):
            raise ValueError("zero prediction fee model requires zero maker and taker rates")
        if self.venue is Venue.POLYMARKET and self.model not in {
            FeeModel.UNKNOWN,
            FeeModel.POLYMARKET_QUADRATIC,
            FeeModel.ZERO,
        }:
            raise ValueError("Polymarket fee schedule uses the wrong model")
        if self.venue is Venue.KALSHI and self.model not in {
            FeeModel.UNKNOWN,
            FeeModel.KALSHI_QUADRATIC,
        }:
            raise ValueError("Kalshi fee schedule uses the wrong model")

    @property
    def exact_usable(self) -> bool:
        allowed_evidence = (
            self.classification is EvidenceClassification.OBSERVED_PUBLICLY or self.synthetic_fixture
        )
        required = (
            self.taker_rate,
            self.maker_rate,
            self.multiplier,
            self.exponent,
            self.rounding_quantum,
        )
        correct_rounding_scope = (
            self.model is FeeModel.ZERO
            or (self.model is FeeModel.POLYMARKET_QUADRATIC and self.rounding_scope == "PER_FILL")
            or (
                self.model is FeeModel.KALSHI_QUADRATIC
                and self.rounding_scope == "PER_ORDER_CUMULATIVE"
                and self.account_precision_quantum is not None
            )
        )
        exact_venue_contract = (
            self.venue is Venue.POLYMARKET
            and self.multiplier == _ONE
            and self.exponent == _ONE
            and self.rounding_quantum == Decimal("0.00001")
            and self.account_precision_quantum is None
            and (
                self.model is FeeModel.POLYMARKET_QUADRATIC
                or (
                    self.model is FeeModel.ZERO
                    and self.taker_rate == _ZERO
                    and self.maker_rate == _ZERO
                )
            )
        ) or (
            self.venue is Venue.KALSHI
            and self.model is FeeModel.KALSHI_QUADRATIC
            and self.taker_rate == Decimal("0.07")
            and self.maker_rate == Decimal("0.0175")
            and self.exponent == _ONE
            and self.rounding_quantum == Decimal("0.000001")
            and self.account_precision_quantum in {Decimal("0.0001"), Decimal("0.01")}
        )
        return (
            allowed_evidence
            and self.model is not FeeModel.UNKNOWN
            and bool(self.source_refs)
            and all(value is not None for value in required)
            and self.rounding_complete
            and correct_rounding_scope
            and exact_venue_contract
        )

    @property
    def evidence_sha256(self) -> str:
        body = {
            "account_precision_quantum": (
                None
                if self.account_precision_quantum is None
                else format(self.account_precision_quantum, "f")
            ),
            "classification": self.classification.value,
            "effective_from_ns": self.effective_from_ns,
            "effective_to_ns": self.effective_to_ns,
            "exponent": None if self.exponent is None else format(self.exponent, "f"),
            "maker_rate": None if self.maker_rate is None else format(self.maker_rate, "f"),
            "market_id": self.market_id,
            "model": self.model.value,
            "multiplier": None if self.multiplier is None else format(self.multiplier, "f"),
            "rounding_complete": self.rounding_complete,
            "rounding_quantum": (
                None if self.rounding_quantum is None else format(self.rounding_quantum, "f")
            ),
            "rounding_scope": self.rounding_scope,
            "schedule_id": self.schedule_id,
            "outcome_ids": list(self.outcome_ids),
            "source_refs": [item.to_dict() for item in self.source_refs],
            "synthetic_fixture": self.synthetic_fixture,
            "taker_rate": None if self.taker_rate is None else format(self.taker_rate, "f"),
            "venue": self.venue.value,
        }
        return hashlib.sha256(canonical_json_bytes(body)).hexdigest()

    def assert_effective_at(self, timestamp_ns: int) -> None:
        if timestamp_ns < self.effective_from_ns or (
            self.effective_to_ns is not None and timestamp_ns >= self.effective_to_ns
        ):
            raise ValueError("PREDICTION_FEE_SCHEDULE_NOT_EFFECTIVE")

    def _raw_fee(self, *, price: Decimal, quantity: Decimal, maker: bool) -> Decimal:
        if not self.exact_usable:
            raise ValueError("PREDICTION_FEE_UNKNOWN_FAIL_CLOSED")
        if not (_ZERO <= price <= _ONE) or quantity <= 0:
            raise ValueError("prediction fee price or quantity is invalid")
        assert self.taker_rate is not None
        assert self.maker_rate is not None
        assert self.multiplier is not None
        assert self.exponent is not None
        rate = self.maker_rate if maker else self.taker_rate
        if self.model is FeeModel.ZERO:
            return _ZERO
        if self.exponent != _ONE:
            raise ValueError("PREDICTION_FEE_EXPONENT_UNSUPPORTED_FAIL_CLOSED")
        return self.multiplier * rate * quantity * price * (_ONE - price)

    def _round(self, raw: Decimal) -> Decimal:
        if raw == 0:
            return _ZERO
        assert self.rounding_quantum is not None
        if self.model is FeeModel.POLYMARKET_QUADRATIC:
            if raw < self.rounding_quantum:
                return _ZERO
            return raw.quantize(self.rounding_quantum, rounding=ROUND_HALF_UP)
        return raw.quantize(self.rounding_quantum, rounding=ROUND_CEILING)

    def fee(self, *, price: Decimal, quantity: Decimal, maker: bool) -> Decimal:
        return self._round(self._raw_fee(price=price, quantity=quantity, maker=maker))

    def order_fee(self, *, levels: Sequence[tuple[Decimal, Decimal]], maker: bool) -> Decimal:
        if not levels:
            return _ZERO
        if self.rounding_scope == "PER_ORDER_CUMULATIVE":
            raw = sum(
                (self._raw_fee(price=price, quantity=quantity, maker=maker) for price, quantity in levels),
                _ZERO,
            )
            return self._round(raw)
        return sum(
            (self.fee(price=price, quantity=quantity, maker=maker) for price, quantity in levels),
            _ZERO,
        )


@dataclass(frozen=True, slots=True)
class PredictionTickBand:
    lower_inclusive: Decimal
    upper_inclusive: Decimal
    tick_size: Decimal

    def __post_init__(self) -> None:
        if (
            not self.lower_inclusive.is_finite()
            or not self.upper_inclusive.is_finite()
            or not self.tick_size.is_finite()
            or self.lower_inclusive < 0
            or self.upper_inclusive > 1
            or self.lower_inclusive >= self.upper_inclusive
            or self.tick_size <= 0
            or self.tick_size > 1
        ):
            raise ValueError("prediction tick band is invalid")

    def to_dict(self) -> dict[str, str]:
        return {
            "lower_inclusive": format(self.lower_inclusive, "f"),
            "tick_size": format(self.tick_size, "f"),
            "upper_inclusive": format(self.upper_inclusive, "f"),
        }


@dataclass(frozen=True, slots=True)
class PredictionTickGrid:
    grid_id: str
    venue: Venue
    market_id: str
    outcome_ids: tuple[str, ...]
    classification: EvidenceClassification
    bands: tuple[PredictionTickBand, ...]
    source_refs: tuple[PredictionRawRecordRef, ...]
    synthetic_fixture: bool = False

    def __post_init__(self) -> None:
        if (
            not self.grid_id
            or not self.market_id
            or len(self.outcome_ids) < 2
            or len(self.outcome_ids) != len(set(self.outcome_ids))
            or any(not item for item in self.outcome_ids)
            or self.venue not in {Venue.POLYMARKET, Venue.KALSHI}
            or not self.bands
        ):
            raise ValueError("prediction tick grid identity is incomplete")
        if (
            tuple(
                sorted(
                    set(self.source_refs),
                    key=lambda item: (item.arrival_sequence, item.raw_record_index),
                )
            )
            != self.source_refs
        ):
            raise ValueError("prediction tick source references must be unique and causal")
        ordered = tuple(sorted(self.bands, key=lambda item: item.lower_inclusive))
        if ordered != self.bands or ordered[0].lower_inclusive != 0:
            raise ValueError("prediction tick bands must start at zero in order")
        if ordered[-1].upper_inclusive != 1:
            raise ValueError("prediction tick bands must cover price one")
        if any(left.upper_inclusive != right.lower_inclusive for left, right in pairwise(ordered)):
            raise ValueError("prediction tick bands must be contiguous")

    @property
    def exact_usable(self) -> bool:
        return bool(self.source_refs) and (
            self.classification is EvidenceClassification.OBSERVED_PUBLICLY or self.synthetic_fixture
        )

    @property
    def evidence_sha256(self) -> str:
        body = {
            "bands": [item.to_dict() for item in self.bands],
            "classification": self.classification.value,
            "grid_id": self.grid_id,
            "market_id": self.market_id,
            "outcome_ids": list(self.outcome_ids),
            "source_refs": [item.to_dict() for item in self.source_refs],
            "synthetic_fixture": self.synthetic_fixture,
            "venue": self.venue.value,
        }
        return hashlib.sha256(canonical_json_bytes(body)).hexdigest()

    def tick_for(self, price: Decimal) -> Decimal:
        if price < 0 or price > 1:
            raise ValueError("prediction price is outside [0,1]")
        for index, band in enumerate(self.bands):
            if band.lower_inclusive <= price and (
                price < band.upper_inclusive
                or (index == len(self.bands) - 1 and price == band.upper_inclusive)
            ):
                return band.tick_size
        raise ValueError("prediction tick grid does not cover price")

    def assert_price(self, price: Decimal) -> None:
        for index, band in enumerate(self.bands):
            if band.lower_inclusive <= price and (
                price < band.upper_inclusive
                or (index == len(self.bands) - 1 and price == band.upper_inclusive)
            ):
                if (price - band.lower_inclusive) % band.tick_size != 0:
                    raise ValueError("prediction price is off its authenticated tick band")
                return
        raise ValueError("prediction tick grid does not cover price")


@dataclass(frozen=True, slots=True)
class PredictionDepthLevel:
    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        if (
            not self.price.is_finite()
            or not self.quantity.is_finite()
            or self.price < 0
            or self.price > 1
            or self.quantity <= 0
        ):
            raise ValueError("prediction depth level is invalid")

    def to_dict(self) -> dict[str, str]:
        return {"price": format(self.price, "f"), "quantity": format(self.quantity, "f")}


@dataclass(frozen=True, slots=True)
class PredictionDepthSnapshot:
    venue: Venue
    event_id: str
    market_id: str
    outcome_id: str
    point_in_time_id: str
    rule_version_id: str
    graph_observation_sha256: str | None
    fee_schedule_id: str
    tick_grid: PredictionTickGrid
    bids: tuple[PredictionDepthLevel, ...]
    asks: tuple[PredictionDepthLevel, ...]
    source_event_time_ns: int | None
    source_time_ns: int | None
    received_time_utc_ns: int
    received_monotonic_ns: int
    arrival_sequence: int
    raw_manifest_sha256: str
    raw_root_sha256: str
    raw_content_sha256: str
    raw_record_index: int
    source_metadata_version: str
    collector_identity: str
    session_identity: str
    source_url: str
    source_transport: str
    gap_detected: bool
    duplicate: bool
    reconnect: bool
    ask_derivation: str
    execution_eligible: bool
    ineligibility_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        identities = (
            self.event_id,
            self.market_id,
            self.outcome_id,
            self.point_in_time_id,
            self.rule_version_id,
            self.fee_schedule_id,
            self.tick_grid.grid_id,
            self.raw_manifest_sha256,
            self.raw_root_sha256,
            self.raw_content_sha256,
            self.source_metadata_version,
            self.collector_identity,
            self.session_identity,
            self.source_url,
            self.source_transport,
        )
        if any(not item for item in identities):
            raise ValueError("prediction depth snapshot identity is incomplete")
        if self.graph_observation_sha256 is None:
            if self.source_transport != "FIXTURE":
                raise ValueError("public prediction depth lacks its raw graph observation")
        elif len(self.graph_observation_sha256) != _SHA256_LENGTH:
            raise ValueError("prediction depth graph observation identity is invalid")
        if self.tick_grid.venue is not self.venue or self.tick_grid.market_id != self.market_id:
            raise ValueError("prediction tick grid identity diverged")
        if not self.bids or not self.asks:
            raise ValueError("prediction book requires finite two-sided depth")
        if any(left.price <= right.price for left, right in zip(self.bids, self.bids[1:], strict=False)):
            raise ValueError("prediction bids must be strictly descending")
        if any(left.price >= right.price for left, right in zip(self.asks, self.asks[1:], strict=False)):
            raise ValueError("prediction asks must be strictly ascending")
        if self.bids[0].price >= self.asks[0].price:
            raise ValueError("prediction book is crossed")
        for level in (*self.bids, *self.asks):
            self.tick_grid.assert_price(level.price)
        if self.arrival_sequence <= 0 or min(self.received_time_utc_ns, self.received_monotonic_ns) < 0:
            raise ValueError("prediction receive clocks are invalid")
        if self.raw_record_index < 0:
            raise ValueError("prediction raw record index is invalid")
        if self.execution_eligible == bool(self.ineligibility_reasons):
            raise ValueError("prediction eligibility and reasons are inconsistent")

    def assert_causal_decision(
        self,
        *,
        signal_time_utc_ns: int,
        signal_monotonic_ns: int,
        decision_time_utc_ns: int,
        decision_monotonic_ns: int,
        maximum_age_ns: int,
    ) -> None:
        if maximum_age_ns <= 0:
            raise ValueError("prediction maximum age must be positive")
        if self.received_time_utc_ns > signal_time_utc_ns:
            raise ValueError("LOOKAHEAD_PREDICTION_BOOK_RECEIVED_AFTER_SIGNAL")
        if self.received_monotonic_ns > signal_monotonic_ns:
            raise ValueError("LOOKAHEAD_PREDICTION_BOOK_MONOTONIC_AFTER_SIGNAL")
        if (
            decision_time_utc_ns <= signal_time_utc_ns
            or decision_monotonic_ns <= signal_monotonic_ns
        ):
            raise ValueError("SAME_TIMESTAMP_PREDICTION_ACTION_FORBIDDEN")
        utc_age = signal_time_utc_ns - self.received_time_utc_ns
        monotonic_age = signal_monotonic_ns - self.received_monotonic_ns
        if (
            utc_age < 0
            or monotonic_age < 0
            or utc_age > maximum_age_ns
            or monotonic_age > maximum_age_ns
        ):
            raise ValueError("PREDICTION_BOOK_STALE")
        if not self.execution_eligible:
            raise ValueError("PREDICTION_BOOK_NOT_EXECUTION_ELIGIBLE")

    def to_dict(self) -> dict[str, Any]:
        return {
            "arrival_sequence": self.arrival_sequence,
            "ask_derivation": self.ask_derivation,
            "asks": [item.to_dict() for item in self.asks],
            "bids": [item.to_dict() for item in self.bids],
            "collector_identity": self.collector_identity,
            "duplicate": self.duplicate,
            "event_id": self.event_id,
            "execution_eligible": self.execution_eligible,
            "fee_schedule_id": self.fee_schedule_id,
            "gap_detected": self.gap_detected,
            "graph_observation_sha256": self.graph_observation_sha256,
            "ineligibility_reasons": list(self.ineligibility_reasons),
            "market_id": self.market_id,
            "outcome_id": self.outcome_id,
            "point_in_time_id": self.point_in_time_id,
            "raw_content_sha256": self.raw_content_sha256,
            "raw_record_index": self.raw_record_index,
            "raw_manifest_sha256": self.raw_manifest_sha256,
            "raw_root_sha256": self.raw_root_sha256,
            "received_monotonic_ns": self.received_monotonic_ns,
            "received_time_utc_ns": self.received_time_utc_ns,
            "reconnect": self.reconnect,
            "rule_version_id": self.rule_version_id,
            "session_identity": self.session_identity,
            "source_event_time_ns": self.source_event_time_ns,
            "source_metadata_version": self.source_metadata_version,
            "source_time_ns": self.source_time_ns,
            "source_transport": self.source_transport,
            "source_url": self.source_url,
            "tick_grid": {
                "bands": [item.to_dict() for item in self.tick_grid.bands],
                "evidence_sha256": self.tick_grid.evidence_sha256,
                "grid_id": self.tick_grid.grid_id,
                "source_refs": [item.to_dict() for item in self.tick_grid.source_refs],
            },
            "venue": self.venue.value,
        }


@dataclass(frozen=True, slots=True)
class PredictionPointInTimeDataset:
    identity: DerivedDatasetIdentity
    semantic_catalog_sha256: str
    rows: tuple[PredictionDepthSnapshot, ...]
    synthetic: bool
    campaign_manifest_sha256: str | None = None
    candidate_config_sha256: str | None = None
    collection_probe_binding_sha256: str | None = None
    collection_terminal_result_sha256: str | None = None

    def __post_init__(self) -> None:
        bindings = (
            self.campaign_manifest_sha256,
            self.candidate_config_sha256,
            self.collection_probe_binding_sha256,
            self.collection_terminal_result_sha256,
        )
        if self.synthetic and any(item is not None for item in bindings):
            raise ValueError("synthetic prediction dataset cannot claim campaign binding")
        if not self.synthetic and (
            not all(item is not None for item in bindings)
            or any(len(cast(str, item)) != _SHA256_LENGTH for item in bindings)
        ):
            raise ValueError("public prediction dataset requires authenticated campaign bindings")

    @property
    def dataset_sha256(self) -> str:
        body = {
            "campaign_manifest_sha256": self.campaign_manifest_sha256,
            "candidate_config_sha256": self.candidate_config_sha256,
            "collection_probe_binding_sha256": self.collection_probe_binding_sha256,
            "collection_terminal_result_sha256": self.collection_terminal_result_sha256,
            "identity": {
                "model_version": self.identity.model_version,
                "parameters_sha256": self.identity.parameters_sha256,
                "raw_manifest_sha256": self.identity.raw_manifest_sha256,
                "raw_root_sha256": self.identity.raw_root_sha256,
            },
            "rows": [item.to_dict() for item in self.rows],
            "semantic_catalog_sha256": self.semantic_catalog_sha256,
            "synthetic": self.synthetic,
        }
        return hashlib.sha256(canonical_json_bytes(body)).hexdigest()


@dataclass(frozen=True, slots=True)
class PredictionTradeObservation:
    venue: Venue
    event_id: str
    market_id: str
    outcome_id: str
    source_trade_id: str
    trade_observation_id: str
    rule_version_id: str
    graph_observation_sha256: str
    side: str
    price: Decimal
    quantity: Decimal
    source_event_time_ns: int | None
    source_time_raw: str
    source_time_classification: EvidenceClassification
    received_time_utc_ns: int
    received_monotonic_ns: int
    arrival_sequence: int
    block_trade: bool
    raw_manifest_sha256: str
    raw_root_sha256: str
    raw_content_sha256: str
    raw_record_sha256: str
    raw_record_index: int
    source_metadata_version: str
    source_url: str
    source_transport: str
    synthetic: bool

    def __post_init__(self) -> None:
        identities = (
            self.event_id,
            self.market_id,
            self.outcome_id,
            self.source_trade_id,
            self.trade_observation_id,
            self.rule_version_id,
            self.graph_observation_sha256,
            self.source_time_raw,
            self.raw_manifest_sha256,
            self.raw_root_sha256,
            self.raw_content_sha256,
            self.raw_record_sha256,
            self.source_metadata_version,
            self.source_url,
            self.source_transport,
        )
        if any(not item for item in identities):
            raise ValueError("prediction trade identity is incomplete")
        if len(self.trade_observation_id) != _SHA256_LENGTH or any(
            len(value) != _SHA256_LENGTH
            for value in (
                self.raw_manifest_sha256,
                self.raw_root_sha256,
                self.raw_content_sha256,
                self.raw_record_sha256,
                self.graph_observation_sha256,
            )
        ):
            raise ValueError("prediction trade hash identity is invalid")
        if (
            not self.price.is_finite()
            or not self.quantity.is_finite()
            or self.price < 0
            or self.price > 1
            or self.quantity <= 0
        ):
            raise ValueError("prediction trade price or quantity is invalid")
        if self.arrival_sequence <= 0 or min(self.received_time_utc_ns, self.received_monotonic_ns) < 0:
            raise ValueError("prediction trade receive clocks are invalid")
        if self.raw_record_index < 0:
            raise ValueError("prediction trade raw record index is invalid")
        if self.source_event_time_ns is not None and self.source_event_time_ns < 0:
            raise ValueError("prediction trade source event time is invalid")
        if (
            self.source_time_classification is EvidenceClassification.DOCUMENTED
            and self.source_event_time_ns is None
        ):
            raise ValueError("documented prediction trade time must be normalized")

    def assert_causal_decision(self, *, signal_time_utc_ns: int, decision_time_utc_ns: int) -> None:
        if self.received_time_utc_ns > signal_time_utc_ns:
            raise ValueError("LOOKAHEAD_PREDICTION_TRADE_RECEIVED_AFTER_SIGNAL")
        if decision_time_utc_ns <= signal_time_utc_ns:
            raise ValueError("SAME_TIMESTAMP_PREDICTION_ACTION_FORBIDDEN")

    def to_dict(self) -> dict[str, Any]:
        return {
            "arrival_sequence": self.arrival_sequence,
            "block_trade": self.block_trade,
            "event_id": self.event_id,
            "graph_observation_sha256": self.graph_observation_sha256,
            "market_id": self.market_id,
            "outcome_id": self.outcome_id,
            "price": format(self.price, "f"),
            "quantity": format(self.quantity, "f"),
            "raw_content_sha256": self.raw_content_sha256,
            "raw_manifest_sha256": self.raw_manifest_sha256,
            "raw_record_sha256": self.raw_record_sha256,
            "raw_record_index": self.raw_record_index,
            "raw_root_sha256": self.raw_root_sha256,
            "received_monotonic_ns": self.received_monotonic_ns,
            "received_time_utc_ns": self.received_time_utc_ns,
            "rule_version_id": self.rule_version_id,
            "side": self.side,
            "source_event_time_ns": self.source_event_time_ns,
            "source_metadata_version": self.source_metadata_version,
            "source_time_classification": self.source_time_classification.value,
            "source_time_raw": self.source_time_raw,
            "source_trade_id": self.source_trade_id,
            "source_transport": self.source_transport,
            "source_url": self.source_url,
            "synthetic": self.synthetic,
            "trade_observation_id": self.trade_observation_id,
            "venue": self.venue.value,
        }


@dataclass(frozen=True, slots=True)
class PredictionTradeDataset:
    identity: DerivedDatasetIdentity
    semantic_catalog_sha256: str
    rows: tuple[PredictionTradeObservation, ...]
    synthetic: bool
    campaign_manifest_sha256: str | None = None
    candidate_config_sha256: str | None = None
    collection_probe_binding_sha256: str | None = None
    collection_terminal_result_sha256: str | None = None

    def __post_init__(self) -> None:
        bindings = (
            self.campaign_manifest_sha256,
            self.candidate_config_sha256,
            self.collection_probe_binding_sha256,
            self.collection_terminal_result_sha256,
        )
        if self.synthetic and any(item is not None for item in bindings):
            raise ValueError("synthetic prediction trade dataset cannot claim campaign binding")
        if not self.synthetic and not all(item is not None for item in bindings):
            raise ValueError("public prediction trade dataset requires campaign binding")

    @property
    def dataset_sha256(self) -> str:
        body = {
            "campaign_manifest_sha256": self.campaign_manifest_sha256,
            "candidate_config_sha256": self.candidate_config_sha256,
            "collection_probe_binding_sha256": self.collection_probe_binding_sha256,
            "collection_terminal_result_sha256": self.collection_terminal_result_sha256,
            "identity": {
                "model_version": self.identity.model_version,
                "parameters_sha256": self.identity.parameters_sha256,
                "raw_manifest_sha256": self.identity.raw_manifest_sha256,
                "raw_root_sha256": self.identity.raw_root_sha256,
            },
            "rows": [item.to_dict() for item in self.rows],
            "semantic_catalog_sha256": self.semantic_catalog_sha256,
            "synthetic": self.synthetic,
        }
        return hashlib.sha256(canonical_json_bytes(body)).hexdigest()


def _levels(
    values: object,
    *,
    label: str,
    object_shape: bool,
    descending: bool,
) -> tuple[PredictionDepthLevel, ...]:
    result: list[PredictionDepthLevel] = []
    for raw in _sequence(values, label=label):
        if object_shape:
            item = _mapping(raw, label=f"{label} level")
            price = _decimal(item.get("price"), label=f"{label} price")
            quantity = _decimal(item.get("size"), label=f"{label} quantity")
        else:
            pair = _sequence(raw, label=f"{label} level")
            if len(pair) != 2:
                raise ValueError(f"{label} level must contain price and quantity")
            price = _decimal(pair[0], label=f"{label} price")
            quantity = _decimal(pair[1], label=f"{label} quantity")
        result.append(PredictionDepthLevel(price, quantity))
    ordered = tuple(sorted(result, key=lambda item: item.price, reverse=descending))
    if len({item.price for item in ordered}) != len(ordered):
        raise ValueError(f"{label} contains duplicate price levels")
    return ordered


def _snapshot_id(
    envelope: PublicDataEnvelope,
    manifest: ManifestRecord,
    graph: PredictionIdentityGraph,
    outcome_id: str,
    raw_record_index: int,
) -> str:
    body = {
        "arrival_sequence": envelope.arrival_sequence,
        "content_sha256": envelope.content_sha256,
        "manifest_sha256": manifest.manifest_sha256,
        "market_id": graph.market_id,
        "outcome_id": outcome_id,
        "raw_graph_sha256": graph.raw_graph_sha256,
        "raw_record_index": raw_record_index,
        "rule_version_id": graph.rule_version.version_id,
    }
    return hashlib.sha256(canonical_json_bytes(body)).hexdigest()


def _eligibility(
    envelope: PublicDataEnvelope,
    graph: PredictionIdentityGraph,
    fee: PredictionFeeSchedule,
    tick_grid: PredictionTickGrid,
) -> tuple[bool, tuple[str, ...]]:
    reasons: list[str] = []
    if envelope.state.gap_detected:
        reasons.append("RAW_GAP_DETECTED")
    if envelope.state.duplicate:
        reasons.append("RAW_DUPLICATE")
    if envelope.state.reconnect:
        reasons.append("RECONNECT_REQUIRES_NEW_BOOTSTRAP")
    reasons.extend(graph.ineligibility_reasons)
    if not fee.exact_usable:
        reasons.append("FEE_SCHEDULE_UNKNOWN_FAIL_CLOSED")
    if not tick_grid.exact_usable:
        reasons.append("TICK_GRID_UNKNOWN_FAIL_CLOSED")
    return not reasons, tuple(reasons)


def _source_envelopes(
    index: PredictionRawEvidenceIndex,
    references: Sequence[PredictionRawRecordRef],
    *,
    venue: Venue,
    allowed_feeds: Sequence[str],
) -> tuple[tuple[PublicDataEnvelope, Mapping[str, Any]], ...]:
    return tuple(
        index.require_record(reference, venue=venue, allowed_feeds=allowed_feeds) for reference in references
    )


def _causal_refs(
    references: Sequence[PredictionRawRecordRef],
) -> tuple[PredictionRawRecordRef, ...]:
    ordered = tuple(
        sorted(
            set(references),
            key=lambda item: (item.arrival_sequence, item.raw_record_index),
        )
    )
    if not ordered:
        raise ValueError("prediction evidence factory requires raw references")
    return ordered


def _require_public_factory_sources(
    sources: Sequence[tuple[PublicDataEnvelope, Mapping[str, Any]]],
) -> None:
    if any(
        envelope.provenance.transport == "FIXTURE"
        or envelope.provenance.fixture_label is not None
        for envelope, _record in sources
    ):
        raise ValueError("public prediction evidence factory refuses synthetic fixtures")


def build_polymarket_fee_schedule_from_raw(
    index: PredictionRawEvidenceIndex,
    *,
    graph: PredictionIdentityGraph,
    source_refs: Sequence[PredictionRawRecordRef],
) -> PredictionFeeSchedule:
    if graph.venue is not Venue.POLYMARKET:
        raise ValueError("Polymarket fee factory requires a Polymarket graph")
    references = _causal_refs(source_refs)
    sources = _source_envelopes(
        index,
        references,
        venue=Venue.POLYMARKET,
        allowed_feeds=("fees", "metadata"),
    )
    gamma_values: list[tuple[Decimal, Decimal, bool]] = []
    clob_values: list[tuple[Decimal, Decimal, bool]] = []
    endpoint_rates: dict[str, Decimal] = {}
    outcome_ids = tuple(item.outcome_id for item in graph.outcomes)
    _require_public_factory_sources(sources)
    for envelope, record in sources:
        gamma = record.get("feeSchedule")
        if isinstance(gamma, Mapping):
            if str(record.get("conditionId") or "") != graph.market_id:
                raise ValueError("Polymarket fee Gamma market identity diverged")
            gamma_values.append(
                (
                    _decimal(gamma.get("rate"), label="Polymarket Gamma fee rate"),
                    _decimal(gamma.get("exponent"), label="Polymarket Gamma fee exponent"),
                    gamma.get("takerOnly") is True,
                )
            )
        clob = record.get("fd")
        if isinstance(clob, Mapping):
            if str(record.get("condition_id") or record.get("conditionId") or "") != graph.market_id:
                raise ValueError("Polymarket fee CLOB market identity diverged")
            clob_values.append(
                (
                    _decimal(clob.get("r"), label="Polymarket CLOB fee rate"),
                    _decimal(clob.get("e"), label="Polymarket CLOB fee exponent"),
                    clob.get("to") is True,
                )
            )
        endpoint = record.get("base_fee", record.get("fee_rate_bps"))
        if endpoint is not None:
            endpoint_rate = _decimal(endpoint, label="Polymarket endpoint fee rate")
            token_values = parse_qs(urlsplit(envelope.provenance.source_url).query).get(
                "token_id", []
            )
            if len(token_values) != 1 or token_values[0] not in outcome_ids:
                raise ValueError("Polymarket fee endpoint token identity diverged")
            token_id = token_values[0]
            if token_id in endpoint_rates:
                raise ValueError("Polymarket fee endpoint token evidence is duplicated")
            endpoint_rates[token_id] = endpoint_rate
    if (
        len(gamma_values) != 1
        or len(clob_values) != 1
        or set(endpoint_rates) != set(outcome_ids)
        or gamma_values[0] != clob_values[0]
        or gamma_values[0][2] is not True
        or set(endpoint_rates.values()) != {gamma_values[0][0]}
    ):
        raise ValueError("Polymarket fee evidence is incomplete or contradictory")
    rate, exponent, _taker_only = gamma_values[0]
    effective_from_ns = max(item.receive_timestamp_utc_ns for item, _record in sources)
    identity = {
        "effective_from_ns": effective_from_ns,
        "market_id": graph.market_id,
        "outcome_ids": list(outcome_ids),
        "rate": format(rate, "f"),
        "exponent": format(exponent, "f"),
        "source_refs": [item.to_dict() for item in references],
    }
    return PredictionFeeSchedule(
        schedule_id=f"PM-FEE:{hashlib.sha256(canonical_json_bytes(identity)).hexdigest()}",
        venue=Venue.POLYMARKET,
        market_id=graph.market_id,
        outcome_ids=outcome_ids,
        classification=EvidenceClassification.OBSERVED_PUBLICLY,
        model=FeeModel.ZERO if rate == _ZERO else FeeModel.POLYMARKET_QUADRATIC,
        effective_from_ns=effective_from_ns,
        effective_to_ns=None,
        taker_rate=rate,
        maker_rate=_ZERO,
        multiplier=_ONE,
        exponent=exponent,
        rounding_quantum=Decimal("0.00001"),
        rounding_complete=True,
        rounding_scope="PER_FILL",
        account_precision_quantum=None,
        source_refs=references,
    )


def build_kalshi_unknown_fee_schedule_from_raw(
    index: PredictionRawEvidenceIndex,
    *,
    graph: PredictionIdentityGraph,
    source_refs: Sequence[PredictionRawRecordRef],
) -> PredictionFeeSchedule:
    if graph.venue is not Venue.KALSHI or graph.series_id is None:
        raise ValueError("Kalshi fee factory requires a complete Kalshi graph")
    references = _causal_refs(source_refs)
    sources = _source_envelopes(
        index,
        references,
        venue=Venue.KALSHI,
        allowed_feeds=("event_fee_changes", "fee_changes", "series"),
    )
    _require_public_factory_sources(sources)
    series_bound = False
    event_bound = False
    for envelope, record in sources:
        if envelope.feed_type == "series":
            if str(record.get("ticker") or record.get("series_ticker") or "") != graph.series_id:
                raise ValueError("Kalshi fee series identity diverged")
            series_bound = True
        elif envelope.feed_type == "fee_changes":
            query = parse_qs(urlsplit(envelope.provenance.source_url).query)
            record_series = str(record.get("series_ticker") or graph.series_id)
            if query.get("series_ticker") != [graph.series_id] or record_series != graph.series_id:
                raise ValueError("Kalshi series fee history identity diverged")
            series_bound = True
        elif envelope.feed_type == "event_fee_changes":
            if str(record.get("event_ticker") or "") != graph.event_id:
                raise ValueError("Kalshi event fee history identity diverged")
            event_bound = True
    if not series_bound or not event_bound:
        raise ValueError("Kalshi fee evidence lacks both series and event histories")
    identity = {
        "market_id": graph.market_id,
        "outcome_ids": [item.outcome_id for item in graph.outcomes],
        "reason": "ACCOUNT_PRECISION_AND_EFFECTIVE_OVERRIDE_NOT_FULLY_OBSERVED",
        "source_refs": [item.to_dict() for item in references],
    }
    return PredictionFeeSchedule(
        schedule_id=f"KALSHI-FEE-UNKNOWN:{hashlib.sha256(canonical_json_bytes(identity)).hexdigest()}",
        venue=Venue.KALSHI,
        market_id=graph.market_id,
        outcome_ids=tuple(item.outcome_id for item in graph.outcomes),
        classification=EvidenceClassification.UNKNOWN_NOT_OBSERVED,
        model=FeeModel.UNKNOWN,
        effective_from_ns=0,
        effective_to_ns=None,
        taker_rate=None,
        maker_rate=None,
        multiplier=None,
        exponent=None,
        rounding_quantum=None,
        rounding_complete=False,
        rounding_scope="UNKNOWN",
        account_precision_quantum=None,
        source_refs=references,
    )


def build_prediction_tick_grid_from_raw(
    index: PredictionRawEvidenceIndex,
    *,
    graph: PredictionIdentityGraph,
    source_refs: Sequence[PredictionRawRecordRef],
) -> PredictionTickGrid:
    allowed_feeds = (
        ("markets", "historical_markets")
        if graph.venue is Venue.KALSHI
        else ("market_batch", "metadata", "tick_size", "tick_size_change")
    )
    references = _causal_refs(source_refs)
    sources = _source_envelopes(
        index,
        references,
        venue=graph.venue,
        allowed_feeds=allowed_feeds,
    )
    _require_public_factory_sources(sources)
    outcome_ids = tuple(item.outcome_id for item in graph.outcomes)
    if graph.venue is Venue.KALSHI:
        if len(sources) != 1:
            raise ValueError("Kalshi tick authority requires exactly one market record")
        _envelope, record = sources[0]
        if (
            str(record.get("ticker") or "") != graph.market_id
            or str(record.get("event_ticker") or "") != graph.event_id
        ):
            raise ValueError("Kalshi tick market or event identity diverged")
        raw_ranges = record.get("price_ranges")
        if not isinstance(raw_ranges, list) or not raw_ranges:
            raise ValueError("Kalshi tick authority lacks price_ranges")
        bands = tuple(
            PredictionTickBand(
                _decimal(
                    _mapping(item, label="Kalshi price range").get("start_dollars"),
                    label="Kalshi price range start",
                ),
                _decimal(
                    _mapping(item, label="Kalshi price range").get("end_dollars"),
                    label="Kalshi price range end",
                ),
                _decimal(
                    _mapping(item, label="Kalshi price range").get("step_dollars"),
                    label="Kalshi price range step",
                ),
            )
            for item in raw_ranges
        )
    else:
        ticks_by_token: dict[str, Decimal] = {}
        for envelope, record in sources:
            if (
                record.get("conditionId") is not None
                and str(record.get("conditionId")) != graph.market_id
            ):
                raise ValueError("Polymarket tick market identity diverged")
            token_values = parse_qs(urlsplit(envelope.provenance.source_url).query).get(
                "token_id", []
            )
            raw_asset = record.get("asset_id")
            token_id = (
                str(raw_asset)
                if raw_asset is not None
                else (token_values[0] if len(token_values) == 1 else "")
            )
            if token_id not in outcome_ids or (token_values and token_values != [token_id]):
                raise ValueError("Polymarket tick endpoint token identity diverged")
            raw_market = record.get("market")
            if raw_market is not None and str(raw_market) != graph.market_id:
                raise ValueError("Polymarket tick transition market identity diverged")
            if token_id in ticks_by_token:
                raise ValueError("Polymarket tick token evidence is duplicated")
            raw_tick = (
                record.get("new_tick_size")
                if envelope.feed_type == "tick_size_change"
                or record.get("event_type") == "tick_size_change"
                else record.get("minimum_tick_size", record.get("tick_size"))
            )
            ticks_by_token[token_id] = _decimal(
                raw_tick,
                label="Polymarket tick size",
            )
        if set(ticks_by_token) != set(outcome_ids) or len(set(ticks_by_token.values())) != 1:
            raise ValueError("Polymarket tick evidence is incomplete or contradictory")
        tick = next(iter(ticks_by_token.values()))
        bands = (PredictionTickBand(_ZERO, _ONE, tick),)
    identity = {
        "bands": [item.to_dict() for item in bands],
        "market_id": graph.market_id,
        "outcome_ids": list(outcome_ids),
        "source_refs": [item.to_dict() for item in references],
        "venue": graph.venue.value,
    }
    return PredictionTickGrid(
        grid_id=f"TICK:{hashlib.sha256(canonical_json_bytes(identity)).hexdigest()}",
        venue=graph.venue,
        market_id=graph.market_id,
        outcome_ids=outcome_ids,
        classification=EvidenceClassification.OBSERVED_PUBLICLY,
        bands=bands,
        source_refs=references,
    )


def revalidate_prediction_fee_schedule(
    index: PredictionRawEvidenceIndex,
    schedule: PredictionFeeSchedule,
    *,
    contract: OfficialPublicContract,
    graph: PredictionIdentityGraph,
) -> None:
    graph_outcomes = tuple(item.outcome_id for item in graph.outcomes)
    if (
        contract.venue is not schedule.venue
        or graph.venue is not schedule.venue
        or graph.market_id != schedule.market_id
        or schedule.outcome_ids != graph_outcomes
    ):
        raise ValueError("prediction fee contract venue diverged")
    if schedule.synthetic_fixture:
        sources = _source_envelopes(
            index,
            schedule.source_refs,
            venue=schedule.venue,
            allowed_feeds=("fees", "ghost_fixture", "metadata"),
        )
        if any(envelope.provenance.fixture_label != "SYNTHETIC/FIXTURE" for envelope, _ in sources):
            raise ValueError("synthetic prediction fee lacks fixture provenance")
        return
    if schedule.venue is Venue.POLYMARKET:
        if not schedule.exact_usable:
            return
        expected = build_polymarket_fee_schedule_from_raw(
            index,
            graph=graph,
            source_refs=schedule.source_refs,
        )
        if schedule != expected:
            raise ValueError("Polymarket fee schedule was not canonically derived from raw")
        return
    if schedule.exact_usable:
        raise ValueError("KALSHI_FEE_EXACTNESS_NOT_OBSERVED_FAIL_CLOSED")
    if schedule.source_refs:
        expected_unknown = build_kalshi_unknown_fee_schedule_from_raw(
            index,
            graph=graph,
            source_refs=schedule.source_refs,
        )
        if schedule != expected_unknown:
            raise ValueError("Kalshi unknown fee schedule was not canonically derived from raw")


def revalidate_prediction_tick_grid(
    index: PredictionRawEvidenceIndex,
    grid: PredictionTickGrid,
    *,
    contract: OfficialPublicContract,
    graph: PredictionIdentityGraph,
) -> None:
    if (
        contract.venue is not grid.venue
        or graph.venue is not grid.venue
        or graph.market_id != grid.market_id
        or grid.outcome_ids != tuple(item.outcome_id for item in graph.outcomes)
        or not grid.exact_usable
    ):
        raise ValueError("prediction tick grid is not exact for its official contract")
    sources = _source_envelopes(
        index,
        grid.source_refs,
        venue=grid.venue,
        allowed_feeds=(
            "ghost_fixture",
            "historical_markets",
            "market_batch",
            "markets",
            "metadata",
            "tick_size",
            "tick_size_change",
        ),
    )
    if grid.synthetic_fixture:
        if any(envelope.provenance.fixture_label != "SYNTHETIC/FIXTURE" for envelope, _ in sources):
            raise ValueError("synthetic prediction tick lacks fixture provenance")
        return
    if grid.classification is not EvidenceClassification.OBSERVED_PUBLICLY:
        raise ValueError("public prediction tick must be observed, not merely documented")
    expected = build_prediction_tick_grid_from_raw(
        index,
        graph=graph,
        source_refs=grid.source_refs,
    )
    if grid != expected:
        raise ValueError("prediction tick grid was not canonically derived from raw")


def _book_projections(
    envelope: PublicDataEnvelope,
    *,
    by_market: Mapping[str, PredictionIdentityGraph],
    by_outcome: Mapping[str, PredictionIdentityGraph],
    polymarket_state: dict[
        tuple[str, str, str, str],
        tuple[dict[Decimal, Decimal], dict[Decimal, Decimal]],
    ]
    | None = None,
    graph_selector: Callable[
        [int],
        tuple[
            Mapping[str, PredictionIdentityGraph],
            Mapping[str, PredictionIdentityGraph],
        ],
    ]
    | None = None,
) -> tuple[
    tuple[
        int,
        PredictionIdentityGraph,
        int | None,
        tuple[
            tuple[
                str,
                tuple[PredictionDepthLevel, ...],
                tuple[PredictionDepthLevel, ...],
                str,
            ],
            ...,
        ],
    ],
    ...,
]:
    decoded = json.loads(envelope.raw_payload.decode("utf-8"))
    if envelope.venue is Venue.POLYMARKET:
        if polymarket_state is None:
            polymarket_state = {}
        clock_domain = (envelope.collector_identity, envelope.session_identity)
        if envelope.state.gap_detected or envelope.state.reconnect:
            for book_state_key in tuple(polymarket_state):
                if book_state_key[:2] == clock_domain:
                    del polymarket_state[book_state_key]
        if envelope.feed_type == "order_book":
            raw_records: tuple[tuple[int, Mapping[str, Any]], ...] = (
                (0, _mapping(decoded, label="Polymarket order book")),
            )
        elif envelope.feed_type == "market_batch":
            raw_records = tuple(
                (index, _mapping(item, label="Polymarket batch record"))
                for index, item in enumerate(_sequence(decoded, label="Polymarket WebSocket batch"))
                if isinstance(item, Mapping)
                and item.get("event_type") in {"best_bid_ask", "book", "price_change"}
            )
        elif envelope.feed_type in {"best_bid_ask", "price_change"}:
            raw_records = ((0, _mapping(decoded, label="Polymarket market update")),)
        else:
            return ()
        projections: list[
            tuple[
                int,
                PredictionIdentityGraph,
                int | None,
                tuple[
                    tuple[
                        str,
                        tuple[PredictionDepthLevel, ...],
                        tuple[PredictionDepthLevel, ...],
                        str,
                    ],
                    ...,
                ],
            ]
        ] = []
        for raw_index, record in raw_records:
            _record_by_market, record_by_outcome = (
                graph_selector(raw_index)
                if graph_selector is not None
                else (by_market, by_outcome)
            )
            event_type = str(record.get("event_type") or "")
            if envelope.feed_type == "order_book":
                event_type = "book"
            source_event_time_ns = envelope.source_timestamp_ns
            if event_type in {"book", "price_change"} and record.get("timestamp") is not None:
                try:
                    source_milliseconds = int(str(record.get("timestamp")))
                except ValueError as error:
                    raise ValueError("Polymarket market-data timestamp is not an integer") from error
                if source_milliseconds < 0:
                    raise ValueError("Polymarket market-data timestamp cannot be negative")
                source_event_time_ns = source_milliseconds * 1_000_000
            if event_type == "book":
                outcome_id = _text(
                    record.get("asset_id")
                    or (
                        envelope.instrument_id.removeprefix("PM:")
                        if envelope.instrument_id is not None
                        and envelope.feed_type == "order_book"
                        else None
                    ),
                    label="Polymarket book token",
                )
                graph = record_by_outcome.get(outcome_id)
                if graph is None:
                    raise ValueError("Polymarket book token is absent from the identity graph")
                market_id = _text(record.get("market"), label="Polymarket book market")
                if market_id != graph.market_id:
                    raise ValueError("Polymarket book market/token identity diverged")
                bids = _levels(
                    record.get("bids"),
                    label="Polymarket bids",
                    object_shape=True,
                    descending=True,
                )
                asks = _levels(
                    record.get("asks"),
                    label="Polymarket asks",
                    object_shape=True,
                    descending=False,
                )
                polymarket_state[(*clock_domain, market_id, outcome_id)] = (
                    {item.price: item.quantity for item in bids},
                    {item.price: item.quantity for item in asks},
                )
                projections.append(
                    (
                        raw_index,
                        graph,
                        source_event_time_ns,
                        ((outcome_id, bids, asks, "WIRE_ASKS"),),
                    )
                )
                continue
            market_id = _text(record.get("market"), label="Polymarket update market")
            if event_type == "best_bid_ask":
                outcome_id = _text(record.get("asset_id"), label="Polymarket BBO token")
                graph = record_by_outcome.get(outcome_id)
                state = polymarket_state.get((*clock_domain, market_id, outcome_id))
                if graph is None or graph.market_id != market_id or state is None:
                    raise ValueError("POLYMARKET_BBO_WITHOUT_BOOK_BOOTSTRAP")
                best_bid = _decimal(record.get("best_bid"), label="Polymarket best bid")
                best_ask = _decimal(record.get("best_ask"), label="Polymarket best ask")
                if best_bid != max(state[0]) or best_ask != min(state[1]):
                    raise ValueError("POLYMARKET_BBO_DEPTH_DIVERGED")
                continue
            if event_type != "price_change":
                continue
            raw_changes = _sequence(
                record.get("price_changes"),
                label="Polymarket price changes",
            )
            if not raw_changes:
                raise ValueError("Polymarket price change array cannot be empty")
            changed: dict[str, PredictionIdentityGraph] = {}
            for raw_change in raw_changes:
                change = _mapping(raw_change, label="Polymarket price change")
                outcome_id = _text(
                    change.get("asset_id"),
                    label="Polymarket price-change token",
                )
                graph = record_by_outcome.get(outcome_id)
                state_key = (*clock_domain, market_id, outcome_id)
                state = polymarket_state.get(state_key)
                if graph is None or graph.market_id != market_id:
                    raise ValueError("Polymarket price-change identity diverged")
                if state is None:
                    raise ValueError("POLYMARKET_DELTA_BEFORE_BOOK_BOOTSTRAP")
                side = _text(change.get("side"), label="Polymarket price-change side").upper()
                if side not in {"BUY", "SELL"}:
                    raise ValueError("Polymarket price-change side is invalid")
                price = _decimal(change.get("price"), label="Polymarket price-change price")
                size = _decimal(change.get("size"), label="Polymarket price-change size")
                if price < _ZERO or price > _ONE or size < _ZERO:
                    raise ValueError("Polymarket price-change level is invalid")
                levels = state[0] if side == "BUY" else state[1]
                if size == _ZERO:
                    levels.pop(price, None)
                else:
                    levels[price] = size
                changed[outcome_id] = graph
            outcome_books = []
            selected_graph: PredictionIdentityGraph | None = None
            for outcome_id in sorted(changed):
                graph = changed[outcome_id]
                if selected_graph is not None and selected_graph != graph:
                    raise ValueError("Polymarket price-change batch crossed markets")
                selected_graph = graph
                bids_state, asks_state = polymarket_state[
                    (*clock_domain, market_id, outcome_id)
                ]
                if not bids_state or not asks_state:
                    raise ValueError("POLYMARKET_DELTA_PRODUCED_ONE_SIDED_BOOK_BLACKOUT")
                bids = tuple(
                    PredictionDepthLevel(price, bids_state[price])
                    for price in sorted(bids_state, reverse=True)
                )
                asks = tuple(
                    PredictionDepthLevel(price, asks_state[price])
                    for price in sorted(asks_state)
                )
                outcome_books.append(
                    (outcome_id, bids, asks, "WIRE_ASKS_DELTA_RECONSTRUCTED")
                )
            if selected_graph is None:
                raise AssertionError("Polymarket price change must select a graph")
            projections.append(
                (
                    raw_index,
                    selected_graph,
                    source_event_time_ns,
                    tuple(outcome_books),
                )
            )
        return tuple(projections)
    if envelope.venue is not Venue.KALSHI or envelope.feed_type != "order_book":
        return ()
    record = _mapping(decoded, label="Kalshi order book")
    orderbook = _mapping(record.get("orderbook_fp"), label="Kalshi orderbook_fp")
    raw_market_id = envelope.market_id.removeprefix("KALSHI:") if envelope.market_id is not None else ""
    graph = by_market.get(raw_market_id)
    if graph is None:
        raise ValueError("Kalshi book market is absent from the identity graph")
    yes_bids = _levels(
        orderbook.get("yes_dollars"),
        label="Kalshi YES bids",
        object_shape=False,
        descending=True,
    )
    no_bids = _levels(
        orderbook.get("no_dollars"),
        label="Kalshi NO bids",
        object_shape=False,
        descending=True,
    )
    yes_asks = tuple(
        sorted(
            (PredictionDepthLevel(_ONE - item.price, item.quantity) for item in no_bids),
            key=lambda item: item.price,
        )
    )
    no_asks = tuple(
        sorted(
            (PredictionDepthLevel(_ONE - item.price, item.quantity) for item in yes_bids),
            key=lambda item: item.price,
        )
    )
    return (
        (
            0,
            graph,
            envelope.source_timestamp_ns,
            (
                (
                    f"{graph.market_id}:YES",
                    yes_bids,
                    yes_asks,
                    "DOCUMENTED_COMPLEMENT_FROM_NO_BIDS",
                ),
                (
                    f"{graph.market_id}:NO",
                    no_bids,
                    no_asks,
                    "DOCUMENTED_COMPLEMENT_FROM_YES_BIDS",
                ),
            ),
        ),
    )


def build_prediction_dataset(
    *,
    raw_root: Path,
    manifest_sha256: str,
    contracts: Mapping[Venue, OfficialPublicContract],
    semantic_catalog: SemanticCatalog,
    graphs: Sequence[PredictionIdentityGraph],
    fee_schedules: Mapping[str, Sequence[PredictionFeeSchedule]],
    tick_grids: Mapping[str, Sequence[PredictionTickGrid]],
    collection_binding: PredictionCollectionBinding | None = None,
) -> PredictionPointInTimeDataset:
    reader = ResearchSegmentReader(raw_root, manifest_sha256=manifest_sha256)
    raw_index = PredictionRawEvidenceIndex(reader, contracts=contracts)
    manifest = reader.manifest
    envelopes = raw_index.envelopes
    if not envelopes:
        raise ValueError("prediction dataset requires authenticated raw envelopes")
    fixture_flags = {item.provenance.fixture_label is not None for item in envelopes}
    if len(fixture_flags) != 1:
        raise ValueError("prediction dataset cannot mix public and synthetic provenance")
    synthetic = next(iter(fixture_flags))
    manifest_venues = {item.venue for item in envelopes}
    if any(venue not in contracts or contracts[venue].venue is not venue for venue in manifest_venues):
        raise ValueError("prediction dataset is missing an official venue contract")
    if synthetic:
        if collection_binding is not None:
            raise ValueError("synthetic prediction dataset cannot claim a public collection binding")
    else:
        if collection_binding is None or manifest_venues != {collection_binding.venue}:
            raise ValueError("public prediction dataset requires one authenticated collection binding")
        collection_binding.verify(raw_index, contract=contracts[collection_binding.venue])
    graphs_by_observation = {
        (
            item.venue,
            item.market_id,
            item.rule_version.version_id,
            item.raw_graph_sha256,
        ): item
        for item in graphs
    }
    if len(graphs_by_observation) != len(graphs):
        raise ValueError("prediction dataset graph observations are duplicated")
    graph_timelines: dict[tuple[Venue, str], list[PredictionIdentityGraph]] = {}
    for graph in graphs:
        revalidate_prediction_graph(raw_index, graph)
        graph_timelines.setdefault((graph.venue, graph.market_id), []).append(graph)
    for timeline in graph_timelines.values():
        timeline.sort(
            key=lambda item: (
                max(
                    (ref.arrival_sequence, ref.raw_record_index)
                    for ref in item.source_refs
                ),
                item.raw_graph_sha256,
            )
        )
        semantic_order: list[str] = []
        watermarks: set[RawPosition] = set()
        for previous, successor in pairwise(timeline):
            previous_watermark = max(
                (ref.arrival_sequence, ref.raw_record_index) for ref in previous.source_refs
            )
            successor_watermark = max(
                (ref.arrival_sequence, ref.raw_record_index) for ref in successor.source_refs
            )
            if previous_watermark >= successor_watermark:
                raise ValueError("prediction graph timeline watermark is not increasing")
            previous.assert_compatible_successor(
                successor,
                explicit_rule_version_transition=True,
            )
        for graph in timeline:
            watermark = max(
                (ref.arrival_sequence, ref.raw_record_index) for ref in graph.source_refs
            )
            if watermark in watermarks:
                raise ValueError("prediction graph point-in-time watermark is ambiguous")
            watermarks.add(watermark)
            semantic_id = graph.rule_version.version_id
            if not semantic_order or semantic_order[-1] != semantic_id:
                semantic_order.append(semantic_id)
        if len(set(semantic_order)) != len(semantic_order):
            raise ValueError("prediction graph rule timeline contains a cycle")

    def graph_for_evidence(
        *,
        venue: Venue,
        market_id: str,
        outcome_ids: tuple[str, ...],
        references: Sequence[PredictionRawRecordRef],
    ) -> PredictionIdentityGraph:
        evidence_watermark = max(
            (item.arrival_sequence, item.raw_record_index) for item in references
        )
        candidates = [
            item
            for item in graph_timelines.get((venue, market_id), ())
            if tuple(outcome.outcome_id for outcome in item.outcomes) == outcome_ids
        ]
        if not candidates:
            raise ValueError("prediction evidence lacks a causal compatible identity graph")
        causal_candidates = [
            item
            for item in candidates
            if max(
                (ref.arrival_sequence, ref.raw_record_index) for ref in item.source_refs
            )
            <= evidence_watermark
        ]
        if causal_candidates:
            return max(
                causal_candidates,
                key=lambda item: max(
                    (ref.arrival_sequence, ref.raw_record_index)
                    for ref in item.source_refs
                ),
            )
        # Static fee/tick evidence may be one of the inputs completed by a later
        # event/market graph.  Binding it to that first compatible graph is safe:
        # latest_graphs() below still prevents any book row from seeing the graph
        # before the graph's own arrival watermark.
        return min(
            candidates,
            key=lambda item: max(
                (ref.arrival_sequence, ref.raw_record_index) for ref in item.source_refs
            ),
        )

    for market_id, schedules in fee_schedules.items():
        if not schedules:
            raise ValueError(f"prediction fee schedule sequence is empty: {market_id}")
        for schedule in schedules:
            fee_graph = graph_for_evidence(
                venue=schedule.venue,
                market_id=market_id,
                outcome_ids=schedule.outcome_ids,
                references=schedule.source_refs,
            )
            revalidate_prediction_fee_schedule(
                raw_index,
                schedule,
                contract=contracts[schedule.venue],
                graph=fee_graph,
            )
    for market_id, grids in tick_grids.items():
        if not grids:
            raise ValueError(f"prediction tick grid sequence is empty: {market_id}")
        for grid in grids:
            tick_graph = graph_for_evidence(
                venue=grid.venue,
                market_id=market_id,
                outcome_ids=grid.outcome_ids,
                references=grid.source_refs,
            )
            revalidate_prediction_tick_grid(
                raw_index,
                grid,
                contract=contracts[grid.venue],
                graph=tick_graph,
            )

    def last_discontinuity_position(
        envelope: PublicDataEnvelope,
        raw_record_index: int,
    ) -> RawPosition | None:
        target = (envelope.arrival_sequence, raw_record_index)
        domain = (envelope.collector_identity, envelope.session_identity)
        resets = [
            (candidate.arrival_sequence, -1)
            for candidate in envelopes
            if candidate.venue is envelope.venue
            and (candidate.collector_identity, candidate.session_identity) == domain
            and (candidate.arrival_sequence, -1) <= target
            and (candidate.state.gap_detected or candidate.state.reconnect)
        ]
        return None if not resets else max(resets)

    def latest_graphs(
        envelope: PublicDataEnvelope,
        raw_record_index: int,
    ) -> tuple[dict[str, PredictionIdentityGraph], dict[str, PredictionIdentityGraph]]:
        selected: dict[str, tuple[RawPosition, PredictionIdentityGraph]] = {}
        evidence_position = (envelope.arrival_sequence, raw_record_index)
        evidence_domain = (envelope.collector_identity, envelope.session_identity)
        reset_position = last_discontinuity_position(envelope, raw_record_index)
        for graph in graphs:
            if graph.venue is not envelope.venue:
                continue
            watermark = max(
                (item.arrival_sequence, item.raw_record_index)
                for item in graph.source_refs
            )
            if watermark > evidence_position:
                continue
            if reset_position is not None and min(
                (item.arrival_sequence, item.raw_record_index)
                for item in graph.source_refs
            ) <= reset_position:
                continue
            graph_domains = {
                (
                    source.collector_identity,
                    source.session_identity,
                )
                for reference in graph.source_refs
                for source in (
                    raw_index.require_envelope(
                        reference,
                        venue=graph.venue,
                        allowed_feeds=(
                            "event_metadata",
                            "events",
                            "historical_markets",
                            "market_batch",
                            "markets",
                            "metadata",
                            "series",
                        ),
                    ),
                )
            }
            if graph_domains != {evidence_domain}:
                continue
            previous = selected.get(graph.market_id)
            if previous is not None and previous[0] == watermark:
                raise ValueError("prediction graph point-in-time selection is ambiguous")
            if previous is None or previous[0] < watermark:
                selected[graph.market_id] = (watermark, graph)
        by_market = {key: item[1] for key, item in selected.items()}
        by_outcome = {outcome.outcome_id: graph for graph in by_market.values() for outcome in graph.outcomes}
        if len(by_outcome) != sum(len(item.outcomes) for item in by_market.values()):
            raise ValueError("prediction dataset contains duplicate causal outcome identities")
        return by_market, by_outcome

    def source_watermark(
        references: Sequence[PredictionRawRecordRef],
        *,
        venue: Venue,
        allowed_feeds: Sequence[str],
    ) -> tuple[RawPosition, int]:
        envelopes_for_sources = tuple(
            raw_index.require_envelope(
                reference,
                venue=venue,
                allowed_feeds=allowed_feeds,
            )
            for reference in references
        )
        return (
            max(
                (reference.arrival_sequence, reference.raw_record_index)
                for reference in references
            ),
            max(item.receive_monotonic_ns for item in envelopes_for_sources),
        )

    def source_domains(
        references: Sequence[PredictionRawRecordRef],
        *,
        venue: Venue,
        allowed_feeds: Sequence[str],
    ) -> set[tuple[str, str]]:
        return {
            (source.collector_identity, source.session_identity)
            for reference in references
            for source in (
                raw_index.require_envelope(
                    reference,
                    venue=venue,
                    allowed_feeds=allowed_feeds,
                ),
            )
        }

    def latest_polymarket_fee_refs(
        envelope: PublicDataEnvelope,
        target_raw_record_index: int,
        graph: PredictionIdentityGraph,
    ) -> frozenset[PredictionRawRecordRef] | None:
        latest: dict[str, PredictionRawRecordRef] = {}
        outcome_ids = {item.outcome_id for item in graph.outcomes}
        reset_position = last_discontinuity_position(
            envelope,
            target_raw_record_index,
        )
        for candidate in envelopes:
            if (candidate.arrival_sequence, 0) > (
                envelope.arrival_sequence,
                target_raw_record_index,
            ):
                break
            if candidate.venue is not Venue.POLYMARKET or candidate.feed_type != "fees":
                continue
            if (
                candidate.collector_identity,
                candidate.session_identity,
            ) != (envelope.collector_identity, envelope.session_identity):
                continue
            if reset_position is not None and (
                candidate.arrival_sequence,
                0,
            ) <= reset_position:
                continue
            tokens = parse_qs(urlsplit(candidate.provenance.source_url).query).get(
                "token_id",
                [],
            )
            if len(tokens) == 1 and tokens[0] in outcome_ids:
                records = prediction_raw_records(candidate)
                if len(records) != 1:
                    raise ValueError("Polymarket fee endpoint record is ambiguous")
                latest[tokens[0]] = prediction_raw_record_ref(candidate, 0)
        if set(latest) != outcome_ids:
            return None
        current_refs = set(latest.values())
        for reference in graph.source_refs:
            try:
                _source_envelope, record = raw_index.require_record(
                    reference,
                    venue=Venue.POLYMARKET,
                    allowed_feeds=("metadata",),
                )
            except ValueError:
                continue
            if isinstance(record.get("feeSchedule"), Mapping) or isinstance(
                record.get("fd"),
                Mapping,
            ):
                current_refs.add(reference)
        return frozenset(current_refs)

    def latest_polymarket_tick_refs(
        envelope: PublicDataEnvelope,
        target_raw_record_index: int,
        graph: PredictionIdentityGraph,
    ) -> frozenset[PredictionRawRecordRef] | None:
        latest: dict[str, PredictionRawRecordRef] = {}
        outcome_ids = {item.outcome_id for item in graph.outcomes}
        reset_position = last_discontinuity_position(
            envelope,
            target_raw_record_index,
        )
        for candidate in envelopes:
            if candidate.arrival_sequence > envelope.arrival_sequence:
                break
            if candidate.venue is not Venue.POLYMARKET or candidate.feed_type not in {
                "market_batch",
                "tick_size",
                "tick_size_change",
            }:
                continue
            if (
                candidate.collector_identity,
                candidate.session_identity,
            ) != (envelope.collector_identity, envelope.session_identity):
                continue
            for candidate_raw_record_index, record in enumerate(
                prediction_raw_records(candidate)
            ):
                if (candidate.arrival_sequence, candidate_raw_record_index) > (
                    envelope.arrival_sequence,
                    target_raw_record_index,
                ):
                    continue
                if reset_position is not None and (
                    candidate.arrival_sequence,
                    candidate_raw_record_index,
                ) <= reset_position:
                    continue
                if (
                    candidate.feed_type == "market_batch"
                    and record.get("event_type") != "tick_size_change"
                ):
                    continue
                token = str(record.get("asset_id") or "")
                if not token:
                    tokens = parse_qs(
                        urlsplit(candidate.provenance.source_url).query
                    ).get("token_id", [])
                    token = tokens[0] if len(tokens) == 1 else ""
                if token in outcome_ids:
                    latest[token] = prediction_raw_record_ref(
                        candidate,
                        candidate_raw_record_index,
                    )
        if set(latest) != outcome_ids:
            return None
        return frozenset(latest.values())

    rows: list[PredictionDepthSnapshot] = []
    projected_books: list[
        tuple[PublicDataEnvelope, int, PredictionIdentityGraph, int | None, Any]
    ] = []
    polymarket_state: dict[
        tuple[str, str, str, str],
        tuple[dict[Decimal, Decimal], dict[Decimal, Decimal]],
    ] = {}
    for envelope in envelopes:
        if envelope.venue not in {Venue.POLYMARKET, Venue.KALSHI}:
            continue
        if envelope.venue is Venue.POLYMARKET and (
            envelope.state.gap_detected or envelope.state.reconnect
        ):
            clock_domain = (envelope.collector_identity, envelope.session_identity)
            for book_state_key in tuple(polymarket_state):
                if book_state_key[:2] == clock_domain:
                    del polymarket_state[book_state_key]
        by_market, by_outcome = latest_graphs(envelope, 0)
        is_polymarket_batch = (
            envelope.venue is Venue.POLYMARKET
            and envelope.feed_type == "market_batch"
        )
        if not by_market and not is_polymarket_batch:
            continue
        selector = (
            (lambda index, selected=envelope: latest_graphs(selected, index))
            if is_polymarket_batch
            else None
        )
        projected_books.extend(
            (envelope, raw_index, graph, source_event_time_ns, outcome_books)
            for raw_index, graph, source_event_time_ns, outcome_books in _book_projections(
                envelope,
                by_market=by_market,
                by_outcome=by_outcome,
                polymarket_state=polymarket_state,
                graph_selector=selector,
            )
        )

    represented_tick_refs = {
        reference
        for grids in tick_grids.values()
        for grid in grids
        for reference in grid.source_refs
    }
    maximum_book_position: dict[tuple[Venue, str], RawPosition] = {}
    for (
        envelope,
        book_raw_record_index,
        graph,
        _source_event_time_ns,
        _outcome_books,
    ) in projected_books:
        key = (graph.venue, graph.market_id)
        maximum_book_position[key] = max(
            maximum_book_position.get(key, (0, 0)),
            (envelope.arrival_sequence, book_raw_record_index),
        )
    outcome_market = {
        outcome.outcome_id: graph.market_id
        for graph in graphs
        for outcome in graph.outcomes
    }
    observed_tick_state: dict[tuple[Venue, str, str], str] = {}
    for envelope in envelopes:
        if envelope.venue not in {Venue.POLYMARKET, Venue.KALSHI}:
            continue
        if envelope.provenance.fixture_label is not None:
            continue
        for raw_record_index, raw_record in enumerate(prediction_raw_records(envelope)):
            market_id = ""
            state_key = ""
            state_value: object | None = None
            if envelope.venue is Venue.KALSHI and envelope.feed_type in {
                "historical_markets",
                "markets",
            }:
                market_id = str(raw_record.get("ticker") or "")
                state_key = market_id
                state_value = raw_record.get("price_ranges")
                if not isinstance(state_value, list) or not state_value:
                    continue
            elif envelope.venue is Venue.POLYMARKET and (
                envelope.feed_type in {"tick_size", "tick_size_change"}
                or (
                    envelope.feed_type == "market_batch"
                    and raw_record.get("event_type") == "tick_size_change"
                )
            ):
                token_id = str(raw_record.get("asset_id") or "")
                if not token_id:
                    query_tokens = parse_qs(
                        urlsplit(envelope.provenance.source_url).query
                    ).get("token_id", [])
                    token_id = query_tokens[0] if len(query_tokens) == 1 else ""
                if token_id not in outcome_market:
                    continue
                market_id = str(raw_record.get("market") or outcome_market.get(token_id) or "")
                state_key = token_id
                state_value = (
                    raw_record.get("new_tick_size")
                    if raw_record.get("event_type") == "tick_size_change"
                    or envelope.feed_type == "tick_size_change"
                    else raw_record.get(
                        "minimum_tick_size",
                        raw_record.get("tick_size"),
                    )
                )
                if not market_id or state_value is None:
                    raise ValueError("Polymarket tick transition identity is incomplete")
            else:
                continue
            book_watermark = maximum_book_position.get((envelope.venue, market_id))
            if book_watermark is None or (
                envelope.arrival_sequence,
                raw_record_index,
            ) > book_watermark:
                continue
            canonical_state = hashlib.sha256(canonical_json_bytes(state_value)).hexdigest()
            tick_state_key = (envelope.venue, market_id, state_key)
            if observed_tick_state.get(tick_state_key) == canonical_state:
                continue
            observed_tick_state[tick_state_key] = canonical_state
            raw_ref = prediction_raw_record_ref(envelope, raw_record_index)
            if raw_ref not in represented_tick_refs:
                raise ValueError(
                    "prediction raw tick transition is omitted from the supplied grid timeline"
                )
    for (
        envelope,
        raw_record_index,
        graph,
        source_event_time_ns,
        outcome_books,
    ) in projected_books:
        current_fee_refs = (
            latest_polymarket_fee_refs(envelope, raw_record_index, graph)
            if graph.venue is Venue.POLYMARKET and not synthetic
            else None
        )
        evidence_position = (envelope.arrival_sequence, raw_record_index)
        causal_fees: list[tuple[RawPosition, PredictionFeeSchedule]] = []
        for fee_candidate in fee_schedules.get(graph.market_id, ()):
            if graph.venue is Venue.POLYMARKET and not synthetic and (
                current_fee_refs is None
                or not current_fee_refs.issubset(fee_candidate.source_refs)
            ):
                continue
            fee_feeds = (
                "event_fee_changes",
                "fee_changes",
                "fees",
                "metadata",
                "series",
            )
            watermark, monotonic = source_watermark(
                fee_candidate.source_refs,
                venue=graph.venue,
                allowed_feeds=fee_feeds,
            )
            if (
                watermark <= evidence_position
                and monotonic <= envelope.receive_monotonic_ns
                and source_domains(
                    fee_candidate.source_refs,
                    venue=graph.venue,
                    allowed_feeds=fee_feeds,
                )
                == {(envelope.collector_identity, envelope.session_identity)}
                and fee_candidate.effective_from_ns <= envelope.receive_timestamp_utc_ns
                and (
                    fee_candidate.effective_to_ns is None
                    or envelope.receive_timestamp_utc_ns < fee_candidate.effective_to_ns
                )
            ):
                causal_fees.append((watermark, fee_candidate))
        causal_fees.sort(key=lambda item: item[0])
        if len(causal_fees) >= 2 and causal_fees[-1][0] == causal_fees[-2][0]:
            raise ValueError("prediction fee point-in-time selection is ambiguous")
        if not causal_fees:
            fee = PredictionFeeSchedule(
                schedule_id=f"UNKNOWN:{graph.market_id}",
                venue=graph.venue,
                market_id=graph.market_id,
                outcome_ids=tuple(item.outcome_id for item in graph.outcomes),
                classification=EvidenceClassification.UNKNOWN_NOT_OBSERVED,
                model=FeeModel.UNKNOWN,
                effective_from_ns=0,
                effective_to_ns=None,
                taker_rate=None,
                maker_rate=None,
                multiplier=None,
                exponent=None,
                rounding_quantum=None,
                rounding_complete=False,
                rounding_scope="UNKNOWN",
                account_precision_quantum=None,
                source_refs=(),
            )
        else:
            fee = causal_fees[-1][1]
        if fee.venue is not graph.venue:
            raise ValueError("prediction fee venue/graph identity diverged")
        current_tick_refs = (
            latest_polymarket_tick_refs(envelope, raw_record_index, graph)
            if graph.venue is Venue.POLYMARKET and not synthetic
            else None
        )
        causal_grids: list[tuple[RawPosition, PredictionTickGrid]] = []
        for tick_candidate in tick_grids.get(graph.market_id, ()):
            if graph.venue is Venue.POLYMARKET and not synthetic and (
                current_tick_refs is None
                or not current_tick_refs.issubset(tick_candidate.source_refs)
            ):
                continue
            tick_feeds = (
                "historical_markets",
                "market_batch",
                "markets",
                "metadata",
                "tick_size",
                "tick_size_change",
            )
            watermark, monotonic = source_watermark(
                tick_candidate.source_refs,
                venue=graph.venue,
                allowed_feeds=tick_feeds,
            )
            if (
                watermark <= evidence_position
                and monotonic <= envelope.receive_monotonic_ns
                and source_domains(
                    tick_candidate.source_refs,
                    venue=graph.venue,
                    allowed_feeds=tick_feeds,
                )
                == {(envelope.collector_identity, envelope.session_identity)}
            ):
                causal_grids.append((watermark, tick_candidate))
        causal_grids.sort(key=lambda item: item[0])
        if not causal_grids:
            raise ValueError("prediction book has no causal authenticated tick grid")
        if len(causal_grids) >= 2 and causal_grids[-1][0] == causal_grids[-2][0]:
            raise ValueError("prediction tick point-in-time selection is ambiguous")
        tick_grid = causal_grids[-1][1]
        eligible, reasons = _eligibility(envelope, graph, fee, tick_grid)
        for outcome_id, bids, asks, ask_derivation in outcome_books:
            rows.append(
                PredictionDepthSnapshot(
                    venue=envelope.venue,
                    event_id=graph.event_id,
                    market_id=graph.market_id,
                    outcome_id=outcome_id,
                    point_in_time_id=_snapshot_id(envelope, manifest, graph, outcome_id, raw_record_index),
                    rule_version_id=graph.rule_version.version_id,
                    graph_observation_sha256=graph.raw_graph_sha256,
                    fee_schedule_id=fee.schedule_id,
                    tick_grid=tick_grid,
                    bids=bids,
                    asks=asks,
                    source_event_time_ns=source_event_time_ns,
                    source_time_ns=source_event_time_ns,
                    received_time_utc_ns=envelope.receive_timestamp_utc_ns,
                    received_monotonic_ns=envelope.receive_monotonic_ns,
                    arrival_sequence=envelope.arrival_sequence,
                    raw_manifest_sha256=manifest.manifest_sha256,
                    raw_root_sha256=manifest.root_sha256,
                    raw_content_sha256=envelope.content_sha256,
                    raw_record_index=raw_record_index,
                    source_metadata_version=envelope.source_metadata_version,
                    collector_identity=envelope.collector_identity,
                    session_identity=envelope.session_identity,
                    source_url=envelope.provenance.source_url,
                    source_transport=envelope.provenance.transport,
                    gap_detected=envelope.state.gap_detected,
                    duplicate=envelope.state.duplicate,
                    reconnect=envelope.state.reconnect,
                    ask_derivation=ask_derivation,
                    execution_eligible=eligible,
                    ineligibility_reasons=reasons,
                )
            )
    ordered = tuple(
        sorted(
            rows,
            key=lambda item: (
                item.received_monotonic_ns,
                item.arrival_sequence,
                item.raw_record_index,
                item.venue.value,
                item.market_id,
                item.outcome_id,
            ),
        )
    )
    if not ordered:
        raise ValueError("prediction dataset contains no authenticated order books")
    parameters = {
        "collection_probe_binding_sha256": (
            None if collection_binding is None else collection_binding.probe_binding_sha256
        ),
        "collection_terminal_result_sha256": (
            None if collection_binding is None else collection_binding.terminal_result_sha256
        ),
        "fee_schedule_evidence_sha256s": sorted(
            item.evidence_sha256 for schedules in fee_schedules.values() for item in schedules
        ),
        "graph_observation_sha256s": sorted(item.raw_graph_sha256 for item in graphs),
        "graph_rule_versions": sorted(
            {item.rule_version.version_id for item in graphs}
        ),
        "official_contract_sha256s": {
            venue.value: contracts[venue].contract_sha256
            for venue in sorted(manifest_venues, key=lambda item: item.value)
        },
        "semantic_catalog_sha256": semantic_catalog.catalog_sha256,
        "tick_grid_evidence_sha256s": sorted(
            item.evidence_sha256 for grids in tick_grids.values() for item in grids
        ),
    }
    return PredictionPointInTimeDataset(
        identity=DerivedDatasetIdentity.build(
            manifest=manifest,
            model_version=DATASET_MODEL_VERSION,
            parameters=parameters,
        ),
        semantic_catalog_sha256=semantic_catalog.catalog_sha256,
        rows=ordered,
        synthetic=synthetic,
        campaign_manifest_sha256=(
            None if collection_binding is None else collection_binding.campaign_manifest_sha256
        ),
        candidate_config_sha256=(
            None if collection_binding is None else collection_binding.candidate_config_sha256
        ),
        collection_probe_binding_sha256=(
            None if collection_binding is None else collection_binding.probe_binding_sha256
        ),
        collection_terminal_result_sha256=(
            None if collection_binding is None else collection_binding.terminal_result_sha256
        ),
    )


def revalidate_prediction_dataset(
    dataset: PredictionPointInTimeDataset,
    *,
    raw_root: Path,
    manifest_sha256: str,
    contracts: Mapping[Venue, OfficialPublicContract],
    semantic_catalog: SemanticCatalog,
    graphs: Sequence[PredictionIdentityGraph],
    fee_schedules: Mapping[str, Sequence[PredictionFeeSchedule]],
    tick_grids: Mapping[str, Sequence[PredictionTickGrid]],
    collection_binding: PredictionCollectionBinding | None = None,
) -> PredictionPointInTimeDataset:
    rebuilt = build_prediction_dataset(
        raw_root=raw_root,
        manifest_sha256=manifest_sha256,
        contracts=contracts,
        semantic_catalog=semantic_catalog,
        graphs=graphs,
        fee_schedules=fee_schedules,
        tick_grids=tick_grids,
        collection_binding=collection_binding,
    )
    if rebuilt != dataset or rebuilt.dataset_sha256 != dataset.dataset_sha256:
        raise ValueError("prediction dataset diverged from deterministic raw reconstruction")
    return rebuilt


def _trade_observation_id(
    *,
    manifest: ManifestRecord,
    venue: Venue,
    source_trade_key: str,
    raw_record_sha256: str,
) -> str:
    body = {
        "manifest_sha256": manifest.manifest_sha256,
        "raw_record_sha256": raw_record_sha256,
        "source_trade_key": source_trade_key,
        "venue": venue.value,
    }
    return hashlib.sha256(canonical_json_bytes(body)).hexdigest()


def build_prediction_trade_dataset(
    *,
    raw_root: Path,
    manifest_sha256: str,
    contracts: Mapping[Venue, OfficialPublicContract],
    semantic_catalog: SemanticCatalog,
    graphs: Sequence[PredictionIdentityGraph],
    collection_binding: PredictionCollectionBinding | None = None,
) -> PredictionTradeDataset:
    reader = ResearchSegmentReader(raw_root, manifest_sha256=manifest_sha256)
    raw_index = PredictionRawEvidenceIndex(reader, contracts=contracts)
    manifest = reader.manifest
    envelopes = raw_index.envelopes
    if not envelopes:
        raise ValueError("prediction trade dataset requires authenticated raw envelopes")
    fixture_flags = {item.provenance.fixture_label is not None for item in envelopes}
    if len(fixture_flags) != 1:
        raise ValueError("prediction trade dataset cannot mix provenance classes")
    synthetic = next(iter(fixture_flags))
    manifest_venues = {item.venue for item in envelopes}
    if any(venue not in contracts or contracts[venue].venue is not venue for venue in manifest_venues):
        raise ValueError("prediction trade dataset is missing an official venue contract")
    if synthetic:
        if collection_binding is not None:
            raise ValueError("synthetic prediction trade dataset cannot claim public binding")
    else:
        if collection_binding is None or manifest_venues != {collection_binding.venue}:
            raise ValueError("public prediction trade dataset requires one collection binding")
        collection_binding.verify(raw_index, contract=contracts[collection_binding.venue])
    graph_observation_keys: set[tuple[Venue, str, str, str]] = set()
    for graph in graphs:
        revalidate_prediction_graph(raw_index, graph)
        observation_key = (
            graph.venue,
            graph.market_id,
            graph.rule_version.version_id,
            graph.raw_graph_sha256,
        )
        if observation_key in graph_observation_keys:
            raise ValueError("prediction trade graph observation is duplicated")
        graph_observation_keys.add(observation_key)
    admitted: dict[str, tuple[str, PredictionTradeObservation]] = {}
    for envelope in envelopes:
        if envelope.feed_type not in {
            "block_trades",
            "historical_trades",
            "public_trades",
            "trades",
        }:
            continue
        decoded = json.loads(envelope.raw_payload.decode("utf-8"))
        if envelope.venue is Venue.POLYMARKET:
            records = _sequence(decoded, label="Polymarket public trades")
        elif envelope.venue is Venue.KALSHI:
            page = _mapping(decoded, label="Kalshi public trades page")
            records = _sequence(page.get("trades"), label="Kalshi public trades")
        else:
            raise ValueError("prediction trade dataset received a non-prediction venue")
        for raw_record_index, raw_record in enumerate(records):
            evidence_position = (envelope.arrival_sequence, raw_record_index)
            evidence_domain = (envelope.collector_identity, envelope.session_identity)
            reset_positions = [
                (candidate.arrival_sequence, -1)
                for candidate in envelopes
                if candidate.venue is envelope.venue
                and (candidate.collector_identity, candidate.session_identity)
                == evidence_domain
                and (candidate.arrival_sequence, -1) <= evidence_position
                and (candidate.state.gap_detected or candidate.state.reconnect)
            ]
            reset_position = None if not reset_positions else max(reset_positions)
            causal_graphs: dict[str, tuple[RawPosition, PredictionIdentityGraph]] = {}
            for candidate in graphs:
                if candidate.venue is not envelope.venue:
                    continue
                watermark = max(
                    (item.arrival_sequence, item.raw_record_index)
                    for item in candidate.source_refs
                )
                if watermark > evidence_position:
                    continue
                if reset_position is not None and min(
                    (item.arrival_sequence, item.raw_record_index)
                    for item in candidate.source_refs
                ) <= reset_position:
                    continue
                graph_domains = {
                    (source.collector_identity, source.session_identity)
                    for reference in candidate.source_refs
                    for source in (
                        raw_index.require_envelope(
                            reference,
                            venue=candidate.venue,
                            allowed_feeds=(
                                "event_metadata",
                                "events",
                                "historical_markets",
                                "market_batch",
                                "markets",
                                "metadata",
                                "series",
                            ),
                        ),
                    )
                }
                if graph_domains != {evidence_domain}:
                    continue
                previous_graph = causal_graphs.get(candidate.market_id)
                if previous_graph is None or previous_graph[0] < watermark:
                    causal_graphs[candidate.market_id] = (watermark, candidate)
                elif previous_graph[0] == watermark:
                    raise ValueError(
                        "prediction trade graph point-in-time selection is ambiguous"
                    )
            by_market = {key: value[1] for key, value in causal_graphs.items()}
            by_outcome = {
                outcome.outcome_id: graph
                for graph in by_market.values()
                for outcome in graph.outcomes
            }
            record = _mapping(raw_record, label="prediction trade record")
            canonical_record = canonical_value(record)
            if not isinstance(canonical_record, dict):
                raise AssertionError("prediction trade record must remain an object")
            record_sha256 = hashlib.sha256(canonical_json_bytes(canonical_record)).hexdigest()
            if envelope.venue is Venue.POLYMARKET:
                market_id = _text(record.get("conditionId"), label="Polymarket trade condition id")
                outcome_id = _text(record.get("asset"), label="Polymarket trade token id")
                matched_graph = by_outcome.get(outcome_id)
                if matched_graph is None or matched_graph.market_id != market_id:
                    raise ValueError("Polymarket trade market/token identity diverged from graph")
                graph = matched_graph
                source_trade_id = _text(
                    record.get("transactionHash"),
                    label="Polymarket transaction hash",
                )
                price = _decimal(record.get("price"), label="Polymarket trade price")
                quantity = _decimal(record.get("size"), label="Polymarket trade quantity")
                side = _text(record.get("side"), label="Polymarket trade side").upper()
                source_time_raw = _text(
                    str(record.get("timestamp")) if record.get("timestamp") is not None else None,
                    label="Polymarket trade source time",
                )
                source_event_time_ns = None
                source_time_classification = EvidenceClassification.UNKNOWN_NOT_OBSERVED
                block_trade = False
                source_trade_key = hashlib.sha256(
                    canonical_json_bytes(
                        {
                            "asset": outcome_id,
                            "price": format(price, "f"),
                            "quantity": format(quantity, "f"),
                            "side": side,
                            "source_time_raw": source_time_raw,
                            "source_trade_id": source_trade_id,
                        }
                    )
                ).hexdigest()
            else:
                market_id = _text(record.get("ticker"), label="Kalshi trade ticker")
                matched_graph = by_market.get(market_id)
                if matched_graph is None:
                    raise ValueError("Kalshi trade market is absent from identity graph")
                graph = matched_graph
                source_trade_id = _text(record.get("trade_id"), label="Kalshi trade id")
                source_trade_key = source_trade_id
                quantity = _decimal(record.get("count_fp"), label="Kalshi trade quantity")
                taker_outcome_side = _text(
                    record.get("taker_outcome_side"),
                    label="Kalshi taker outcome side",
                ).upper()
                taker_book_side = _text(
                    record.get("taker_book_side"),
                    label="Kalshi taker book side",
                ).upper()
                if taker_outcome_side not in {"YES", "NO"} or taker_book_side not in {
                    "ASK",
                    "BID",
                    "BUY",
                    "SELL",
                }:
                    raise ValueError("Kalshi trade side is not documented or supported")
                outcome_id = f"{market_id}:{taker_outcome_side}"
                if outcome_id not in {item.outcome_id for item in graph.outcomes}:
                    raise ValueError("Kalshi trade outcome is absent from identity graph")
                yes_raw = record.get("yes_price_dollars")
                no_raw = record.get("no_price_dollars")
                yes_price = (
                    None
                    if yes_raw is None
                    else _decimal(yes_raw, label="Kalshi YES trade price")
                )
                no_price = (
                    None
                    if no_raw is None
                    else _decimal(no_raw, label="Kalshi NO trade price")
                )
                if yes_price is None and no_price is None:
                    raise ValueError("Kalshi trade lacks both binary outcome prices")
                if yes_price is not None and no_price is not None and yes_price + no_price != 1:
                    raise ValueError("Kalshi YES/NO trade prices are not complementary")
                if taker_outcome_side == "YES":
                    price = yes_price if yes_price is not None else _ONE - cast(Decimal, no_price)
                else:
                    price = no_price if no_price is not None else _ONE - cast(Decimal, yes_price)
                if price < _ZERO or price > _ONE:
                    raise ValueError("Kalshi trade price is outside binary bounds")
                side = f"TAKER_{taker_outcome_side}_{taker_book_side}"
                source_time_raw = _text(record.get("created_time"), label="Kalshi trade created time")
                source_event_time_ns = prediction_rfc3339_to_ns(
                    source_time_raw,
                    label="Kalshi trade created time",
                )
                source_time_classification = EvidenceClassification.DOCUMENTED
                block_trade = bool(record.get("is_block_trade"))
                if (envelope.feed_type == "block_trades" and not block_trade) or (
                    envelope.feed_type == "trades" and block_trade
                ):
                    raise ValueError("Kalshi block-trade feed classification diverged")
            trade_observation = PredictionTradeObservation(
                venue=envelope.venue,
                event_id=graph.event_id,
                market_id=graph.market_id,
                outcome_id=outcome_id,
                source_trade_id=source_trade_id,
                trade_observation_id=_trade_observation_id(
                    manifest=manifest,
                    venue=envelope.venue,
                    source_trade_key=source_trade_key,
                    raw_record_sha256=record_sha256,
                ),
                rule_version_id=graph.rule_version.version_id,
                graph_observation_sha256=graph.raw_graph_sha256,
                side=side,
                price=price,
                quantity=quantity,
                source_event_time_ns=source_event_time_ns,
                source_time_raw=source_time_raw,
                source_time_classification=source_time_classification,
                received_time_utc_ns=envelope.receive_timestamp_utc_ns,
                received_monotonic_ns=envelope.receive_monotonic_ns,
                arrival_sequence=envelope.arrival_sequence,
                block_trade=block_trade,
                raw_manifest_sha256=manifest.manifest_sha256,
                raw_root_sha256=manifest.root_sha256,
                raw_content_sha256=envelope.content_sha256,
                raw_record_sha256=record_sha256,
                raw_record_index=raw_record_index,
                source_metadata_version=envelope.source_metadata_version,
                source_url=envelope.provenance.source_url,
                source_transport=envelope.provenance.transport,
                synthetic=synthetic,
            )
            previous_trade = admitted.get(source_trade_key)
            if previous_trade is not None:
                if previous_trade[0] != record_sha256:
                    raise ValueError("prediction source trade changed silently")
                continue
            admitted[source_trade_key] = (record_sha256, trade_observation)
    ordered = tuple(
        sorted(
            (item[1] for item in admitted.values()),
            key=lambda item: (
                item.received_monotonic_ns,
                item.arrival_sequence,
                item.raw_record_index,
                item.venue.value,
                item.source_trade_id,
            ),
        )
    )
    if not ordered:
        raise ValueError("prediction trade dataset contains no authenticated trades")
    parameters = {
        "collection_probe_binding_sha256": (
            None if collection_binding is None else collection_binding.probe_binding_sha256
        ),
        "collection_terminal_result_sha256": (
            None if collection_binding is None else collection_binding.terminal_result_sha256
        ),
        "graph_observation_sha256s": sorted(item.raw_graph_sha256 for item in graphs),
        "graph_rule_versions": sorted(
            {item.rule_version.version_id for item in graphs}
        ),
        "official_contract_sha256s": {
            venue.value: contracts[venue].contract_sha256
            for venue in sorted(manifest_venues, key=lambda item: item.value)
        },
        "semantic_catalog_sha256": semantic_catalog.catalog_sha256,
        "timestamp_policy": {
            "kalshi": "DOCUMENTED_RFC3339_NORMALIZED_TO_NS",
            "polymarket": "RAW_TIMESTAMP_PRESERVED_UNIT_UNKNOWN_NOT_OBSERVED",
        },
    }
    return PredictionTradeDataset(
        identity=DerivedDatasetIdentity.build(
            manifest=manifest,
            model_version=TRADE_DATASET_MODEL_VERSION,
            parameters=parameters,
        ),
        semantic_catalog_sha256=semantic_catalog.catalog_sha256,
        rows=ordered,
        synthetic=synthetic,
        campaign_manifest_sha256=(
            None if collection_binding is None else collection_binding.campaign_manifest_sha256
        ),
        candidate_config_sha256=(
            None if collection_binding is None else collection_binding.candidate_config_sha256
        ),
        collection_probe_binding_sha256=(
            None if collection_binding is None else collection_binding.probe_binding_sha256
        ),
        collection_terminal_result_sha256=(
            None if collection_binding is None else collection_binding.terminal_result_sha256
        ),
    )


class EvaluationSplit(StrEnum):
    TRAIN = "TRAIN"
    VALIDATION = "VALIDATION"
    FINAL_TEST = "FINAL_TEST"


class PublicSourceStatus(StrEnum):
    OBSERVED_PUBLICLY = "OBSERVED_PUBLICLY"
    PUBLIC_SOURCE_UNAVAILABLE = PUBLIC_SOURCE_UNAVAILABLE


@dataclass(frozen=True, slots=True)
class EvaluationObservation:
    opportunity_id: str
    variant_id: str
    venue: Venue
    market_id: str
    outcome_id: str
    observed_at_ns: int
    max_evidence_received_utc_ns: int
    net_pnl: Decimal
    gross_pnl: Decimal
    costs: Decimal
    spread_cost: Decimal
    finite_depth_slippage: Decimal
    capital_immobilized_notional_ns: Decimal
    drawdown: Decimal
    turnover: Decimal
    exposure: Decimal
    dataset_sha256: str
    source_report_sha256: str
    synthetic: bool

    def __post_init__(self) -> None:
        if (
            not self.variant_id
            or not self.opportunity_id.startswith("OPP:")
            or len(self.opportunity_id) != 68
            or not self.market_id
            or not self.outcome_id
            or self.observed_at_ns < 0
            or self.max_evidence_received_utc_ns < self.observed_at_ns
            or len(self.dataset_sha256) != _SHA256_LENGTH
            or len(self.source_report_sha256) != _SHA256_LENGTH
        ):
            raise ValueError("evaluation observation identity is invalid")
        values = (
            self.net_pnl,
            self.gross_pnl,
            self.costs,
            self.spread_cost,
            self.finite_depth_slippage,
            self.capital_immobilized_notional_ns,
            self.drawdown,
            self.turnover,
            self.exposure,
        )
        if any(not item.is_finite() for item in values):
            raise ValueError("evaluation observations must be finite")
        if min(
            self.costs,
            self.spread_cost,
            self.finite_depth_slippage,
            self.capital_immobilized_notional_ns,
            self.drawdown,
            self.turnover,
            self.exposure,
        ) < 0:
            raise ValueError("evaluation costs and risks cannot be negative")
        if self.net_pnl != self.gross_pnl - self.costs:
            raise ValueError("evaluation net PnL does not reconcile")


class PredictionEvaluationReport(Protocol):
    order_id: str
    opportunity_id: str
    variant_id: str
    candidate_config_sha256: str
    campaign_manifest_sha256: str | None
    collection_probe_binding_sha256: str | None
    collection_terminal_result_sha256: str | None
    variant_parameters_sha256: str
    dataset_sha256: str
    dataset_synthetic: bool
    venue: Venue
    market_id: str
    outcome_id: str
    signal_time_utc_ns: int
    max_evidence_received_utc_ns: int
    net_pnl: Decimal | None
    gross_pnl: Decimal | None
    fees: Decimal
    spread_cost: Decimal
    finite_depth_slippage: Decimal
    capital_immobilized_notional_ns: Decimal
    drawdown: Decimal | None
    turnover: Decimal
    unresolved_exposure: Decimal
    reconciliation_difference: Decimal | None

    @property
    def replay_verified(self) -> bool: ...

    @property
    def report_sha256(self) -> str: ...

    def to_dict(self) -> dict[str, Any]: ...


class PredictionCampaignEvaluationReceipt(Protocol):
    venue: Venue
    child_dataset_sha256: str
    collection_id: str
    prospective_shard_ordinal: int
    probe_binding_sha256: str
    terminal_result_sha256: str
    raw_manifest_sha256: str
    raw_root_sha256: str


class PredictionCampaignEvaluationReport(Protocol):
    candidate_config_sha256: str
    campaign_manifest_sha256: str | None
    runner_policy_sha256: str
    dataset_sha256: str
    child_dataset_sha256s: tuple[str, ...]
    dataset_synthetic: bool
    semantic_catalog_sha256: str
    collection_receipts: tuple[PredictionCampaignEvaluationReceipt, ...]
    reports: tuple[PredictionEvaluationReport, ...]
    replay_seal_sha256: str
    evidence_cutoff_utc_ns_exclusive: int
    selection_split_view: Mapping[str, Any]
    prospective_slot_coverage: Mapping[str, Any] | None

    @property
    def opportunity_ids(self) -> tuple[str, ...]: ...

    @property
    def candidate_opportunity_ids(self) -> tuple[str, ...]: ...

    @property
    def opportunity_set_sha256(self) -> str: ...

    @property
    def replay_verified(self) -> bool: ...

    @property
    def report_sha256(self) -> str: ...

    def to_dict(self) -> dict[str, Any]: ...


def _assert_campaign_selection_binding(
    *,
    campaign_manifest: Mapping[str, object],
    preregistration: CandidatePreregistration,
    selection_view: SelectionSplitView,
) -> None:
    manifest_hash = campaign_manifest.get("manifest_sha256")
    if type(manifest_hash) is not str or len(manifest_hash) != _SHA256_LENGTH:
        raise ValueError("prediction campaign manifest hash is missing")
    body = {key: value for key, value in campaign_manifest.items() if key != "manifest_sha256"}
    if hashlib.sha256(canonical_json_bytes(body)).hexdigest() != manifest_hash:
        raise ValueError("prediction campaign manifest self-hash diverged")
    if (
        campaign_manifest.get("boundary") != BOUNDARY
        or campaign_manifest.get("candidate_config_sha256") != preregistration.config_sha256
        or campaign_manifest.get("status") != AWAITING_HUMAN_EXECUTION
        or campaign_manifest.get("runner_policy_sha256")
        != preregistration.runner_policy.policy_sha256
    ):
        raise ValueError("prediction campaign candidate or boundary binding diverged")
    campaign_id = _text(campaign_manifest.get("campaign_id"), label="campaign id")
    expected_collection_plans = {
        venue.value: preregistration.collection_plans[venue].to_dict(
            campaign_id=campaign_id
        )
        for venue in (Venue.POLYMARKET, Venue.KALSHI)
    }
    if campaign_manifest.get("collection_plans") != expected_collection_plans:
        raise ValueError("prediction campaign collection plan binding diverged")
    if (
        campaign_manifest.get("prospective_shard_policy")
        != preregistration.prospective_shard_policy.to_dict()
        or campaign_manifest.get("prospective_shard_policy_sha256")
        != preregistration.prospective_shard_policy.policy_sha256
    ):
        raise ValueError("prediction campaign prospective shard policy binding diverged")
    starts_at = _text(campaign_manifest.get("starts_at_utc"), label="campaign start")
    expected = build_prediction_split_plan(
        preregistration=preregistration,
        dataset_sha256=selection_view.dataset_hash,
        prospective_start=datetime.fromisoformat(starts_at.replace("Z", "+00:00")),
    )
    if expected.selection_view != selection_view:
        raise ValueError("prediction selection view diverged from authenticated campaign")
    train = _mapping(campaign_manifest.get("train"), label="campaign train range")
    validation = _mapping(campaign_manifest.get("validation"), label="campaign validation range")
    holdout = _mapping(campaign_manifest.get("holdout"), label="campaign holdout range")
    plan = expected.to_dict()
    plan_train = _mapping(plan.get("train"), label="expected train range")
    plan_validation = _mapping(plan.get("validation"), label="expected validation range")
    plan_test = _mapping(plan.get("test"), label="expected test range")
    if (
        train.get("start_inclusive") != plan_train["start"]
        or train.get("end_exclusive") != plan_train["end"]
        or validation.get("start_inclusive") != plan_validation["start"]
        or validation.get("end_exclusive") != plan_validation["end"]
        or holdout.get("start_inclusive") != plan_test["start"]
        or holdout.get("end_exclusive") != plan_test["end"]
        or holdout.get("access") != "SEALED"
    ):
        raise ValueError("prediction campaign split ranges diverged")


def validate_prediction_campaign_manifest(
    *,
    campaign_manifest: Mapping[str, object],
    preregistration: CandidatePreregistration,
    contracts: Mapping[Venue, OfficialPublicContract],
) -> None:
    expected_fields = {
        "boundary",
        "campaign_id",
        "candidate_config_sha256",
        "candidate_id",
        "collection_plans",
        "contracts",
        "economic_evidence_status",
        "holdout",
        "manifest_sha256",
        "operator_commands_are_text_only",
        "prospective_shard_policy",
        "prospective_shard_policy_sha256",
        "runner_policy_sha256",
        "schema_version",
        "starts_at_utc",
        "status",
        "train",
        "validation",
        "vps_or_h1_path",
    }
    if set(campaign_manifest) != expected_fields:
        raise ValueError("prediction campaign manifest schema diverged")
    if set(contracts) != {Venue.POLYMARKET, Venue.KALSHI}:
        raise ValueError("prediction campaign validation requires both contracts")
    if (
        campaign_manifest.get("schema_version") != 1
        or campaign_manifest.get("candidate_id") != preregistration.candidate_id
        or campaign_manifest.get("economic_evidence_status") != ECONOMIC_NOT_AVAILABLE
        or campaign_manifest.get("operator_commands_are_text_only") is not True
        or campaign_manifest.get("vps_or_h1_path") != "NONE"
        or campaign_manifest.get("contracts")
        != {
            venue.value: contracts[venue].contract_sha256
            for venue in (Venue.POLYMARKET, Venue.KALSHI)
        }
    ):
        raise ValueError("prediction campaign immutable controls diverged")
    starts_at = datetime.fromisoformat(
        _text(campaign_manifest.get("starts_at_utc"), label="campaign start").replace(
            "Z", "+00:00"
        )
    )
    selection = build_prediction_split_plan(
        preregistration=preregistration,
        dataset_sha256="0" * 64,
        prospective_start=starts_at,
    ).selection_view
    _assert_campaign_selection_binding(
        campaign_manifest=campaign_manifest,
        preregistration=preregistration,
        selection_view=selection,
    )


def _datetime_utc_ns(value: datetime) -> int:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("prediction replay seal time must be timezone-aware")
    delta = value.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


@dataclass(frozen=True, slots=True)
class PredictionReplaySeal:
    campaign_manifest_sha256: str
    candidate_config_sha256: str
    runner_policy_sha256: str
    dataset_sha256: str
    selection_view: SelectionSplitView
    evidence_cutoff_utc_ns_exclusive: int
    embargo_ns: int

    def __post_init__(self) -> None:
        hashes = (
            self.campaign_manifest_sha256,
            self.candidate_config_sha256,
            self.runner_policy_sha256,
            self.dataset_sha256,
            self.selection_view.plan_hash,
        )
        if (
            any(len(item) != _SHA256_LENGTH for item in hashes)
            or self.selection_view.dataset_hash != self.dataset_sha256
            or self.evidence_cutoff_utc_ns_exclusive
            != _datetime_utc_ns(self.selection_view.validation.end)
            or self.embargo_ns <= 0
            or self.embargo_ns
            >= _datetime_utc_ns(self.selection_view.validation.end)
            - _datetime_utc_ns(self.selection_view.validation.start)
        ):
            raise ValueError("prediction replay seal identity or cutoff is invalid")

    def to_dict(self) -> dict[str, CanonicalValue]:
        selection = canonical_value(self.selection_view.to_dict())
        if not isinstance(selection, dict):
            raise AssertionError("prediction replay selection view must remain an object")
        return {
            "campaign_manifest_sha256": self.campaign_manifest_sha256,
            "candidate_config_sha256": self.candidate_config_sha256,
            "dataset_sha256": self.dataset_sha256,
            "embargo_ns": self.embargo_ns,
            "evidence_cutoff_utc_ns_exclusive": self.evidence_cutoff_utc_ns_exclusive,
            "runner_policy_sha256": self.runner_policy_sha256,
            "selection_split_view": selection,
            "selection_view_sha256": hashlib.sha256(
                canonical_json_bytes(self.selection_view.to_dict())
            ).hexdigest(),
        }

    @property
    def seal_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.to_dict())).hexdigest()


def build_prediction_replay_seal(
    *,
    campaign_manifest: Mapping[str, object],
    preregistration: CandidatePreregistration,
    selection_view: SelectionSplitView,
) -> PredictionReplaySeal:
    _assert_campaign_selection_binding(
        campaign_manifest=campaign_manifest,
        preregistration=preregistration,
        selection_view=selection_view,
    )
    return PredictionReplaySeal(
        campaign_manifest_sha256=_text(
            campaign_manifest.get("manifest_sha256"),
            label="campaign manifest hash",
        ),
        candidate_config_sha256=preregistration.config_sha256,
        runner_policy_sha256=preregistration.runner_policy.policy_sha256,
        dataset_sha256=selection_view.dataset_hash,
        selection_view=selection_view,
        evidence_cutoff_utc_ns_exclusive=_datetime_utc_ns(selection_view.validation.end),
        embargo_ns=preregistration.runner_policy.embargo_seconds * 1_000_000_000,
    )


def _cantelli_lcb(values: Sequence[Decimal], alpha: Decimal) -> Decimal | None:
    if len(values) < 2:
        return None
    count = Decimal(len(values))
    mean = sum(values, _ZERO) / count
    variance = sum(((item - mean) ** 2 for item in values), _ZERO) / Decimal(len(values) - 1)
    penalty = (((_ONE - alpha) / alpha) * variance / count).sqrt()
    return mean - penalty


def _maximum_drawdown(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    equity = _ZERO
    peak = _ZERO
    maximum = _ZERO
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def evaluate_preregistered(
    *,
    preregistration: CandidatePreregistration,
    selection_view: SelectionSplitView,
    campaign_manifest: Mapping[str, object],
    campaign_replay: PredictionCampaignEvaluationReport,
    source_status: PublicSourceStatus,
    source_status_by_venue: Mapping[Venue, str] | None = None,
) -> dict[str, CanonicalValue]:
    from hyperlab.ghost.prediction import PredictionCampaignReplayReport

    if not isinstance(campaign_replay, PredictionCampaignReplayReport):
        raise ValueError("prediction evaluation requires the nominal campaign replay artifact")
    _assert_campaign_selection_binding(
        campaign_manifest=campaign_manifest,
        preregistration=preregistration,
        selection_view=selection_view,
    )
    campaign_hash = cast(str, campaign_manifest["manifest_sha256"])
    expected_seal = build_prediction_replay_seal(
        campaign_manifest=campaign_manifest,
        preregistration=preregistration,
        selection_view=selection_view,
    )
    if not campaign_replay.replay_verified:
        raise ValueError("prediction evaluation requires a verified exhaustive campaign replay")
    replay_payload = campaign_replay.to_dict()
    replay_claimed_hash = replay_payload.pop("report_sha256", None)
    replay_computed_hash = hashlib.sha256(canonical_json_bytes(replay_payload)).hexdigest()
    if (
        replay_claimed_hash != campaign_replay.report_sha256
        or replay_computed_hash != campaign_replay.report_sha256
        or campaign_replay.candidate_config_sha256 != preregistration.config_sha256
        or (
            campaign_replay.dataset_synthetic
            and campaign_replay.campaign_manifest_sha256 is not None
        )
        or (
            not campaign_replay.dataset_synthetic
            and campaign_replay.campaign_manifest_sha256 != campaign_hash
        )
        or campaign_replay.runner_policy_sha256
        != preregistration.runner_policy.policy_sha256
        or campaign_replay.dataset_sha256 != selection_view.dataset_hash
        or campaign_replay.replay_seal_sha256 != expected_seal.seal_sha256
        or campaign_replay.evidence_cutoff_utc_ns_exclusive
        != expected_seal.evidence_cutoff_utc_ns_exclusive
        or campaign_replay.selection_split_view != selection_view.to_dict()
        or len(set(campaign_replay.opportunity_ids))
        != len(campaign_replay.opportunity_ids)
        or tuple(item.opportunity_id for item in campaign_replay.reports)
        != campaign_replay.candidate_opportunity_ids
    ):
        raise ValueError("prediction campaign replay binding or exhaustive set diverged")
    receipt_by_dataset: dict[str, PredictionCampaignEvaluationReceipt] = {}
    receipt_slots: set[tuple[Venue, int]] = set()
    for receipt in campaign_replay.collection_receipts:
        if receipt.child_dataset_sha256 in receipt_by_dataset:
            raise ValueError("prediction campaign collection receipt is ambiguous")
        ordinal = prediction_prospective_shard_ordinal(
            preregistration=preregistration,
            campaign_manifest=campaign_manifest,
            venue=receipt.venue,
            collection_id=receipt.collection_id,
        )
        slot = (receipt.venue, ordinal)
        if receipt.prospective_shard_ordinal != ordinal or slot in receipt_slots or (
            receipt.child_dataset_sha256
            not in campaign_replay.child_dataset_sha256s
            or any(
                len(item) != _SHA256_LENGTH
                for item in (
                    receipt.probe_binding_sha256,
                    receipt.terminal_result_sha256,
                    receipt.raw_manifest_sha256,
                    receipt.raw_root_sha256,
                )
            )
        ):
            raise ValueError("prediction campaign collection receipt diverged from plan")
        receipt_slots.add(slot)
        receipt_by_dataset[receipt.child_dataset_sha256] = receipt
    if campaign_replay.dataset_synthetic:
        if receipt_by_dataset or campaign_replay.campaign_manifest_sha256 is not None:
            raise ValueError("synthetic campaign replay cannot claim public receipts")
    elif set(receipt_by_dataset) != set(campaign_replay.child_dataset_sha256s):
        raise ValueError("public campaign replay requires terminal collection receipts")
    reports = campaign_replay.reports
    seen_opportunities: set[str] = set()
    seen_reports: set[str] = set()
    observations: list[EvaluationObservation] = []
    pending_resolutions: list[dict[str, CanonicalValue]] = []
    attribution_components: list[
        tuple[str, PredictionEvaluationReport]
    ] = []
    for report in reports:
        if not report.replay_verified:
            raise ValueError("prediction evaluation requires an in-process deterministic replay")
        payload = report.to_dict()
        claimed_hash = payload.pop("report_sha256", None)
        computed_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        if claimed_hash != report.report_sha256 or computed_hash != report.report_sha256:
            raise ValueError("prediction evaluation report hash diverged")
        if report.opportunity_id in seen_opportunities or report.report_sha256 in seen_reports:
            raise ValueError("prediction evaluation contains duplicate reports")
        seen_opportunities.add(report.opportunity_id)
        seen_reports.add(report.report_sha256)
        if (
            report.candidate_config_sha256 != preregistration.config_sha256
            or report.dataset_sha256
            not in {
                *campaign_replay.child_dataset_sha256s,
                campaign_replay.dataset_sha256,
            }
        ):
            raise ValueError("prediction evaluation report binding diverged")
        if report.dataset_synthetic:
            if (
                report.campaign_manifest_sha256 is not None
                or report.collection_probe_binding_sha256 is not None
                or report.collection_terminal_result_sha256 is not None
            ):
                raise ValueError("synthetic prediction report cannot claim public campaign binding")
        else:
            report_children = tuple(
                cast(
                    Sequence[str],
                    getattr(report, "child_dataset_sha256s", (report.dataset_sha256,)),
                )
            )
            receipts = [receipt_by_dataset.get(item) for item in report_children]
            if (
                report.campaign_manifest_sha256 != campaign_hash
                or not report_children
                or any(item is None for item in receipts)
                or len(set(report_children)) != len(report_children)
                or report.collection_probe_binding_sha256
                != cast(PredictionCampaignEvaluationReceipt, receipts[0]).probe_binding_sha256
                or report.collection_terminal_result_sha256
                != cast(
                    PredictionCampaignEvaluationReceipt,
                    receipts[0],
                ).terminal_result_sha256
            ):
                raise ValueError("public prediction report campaign binding diverged")
        preregistration.require_variant(
            report.variant_id,
            parameters_sha256=report.variant_parameters_sha256,
        )
        if report.net_pnl is None or report.gross_pnl is None or report.drawdown is None:
            if report.unresolved_exposure <= _ZERO:
                raise ValueError("unresolved prediction report lacks locked exposure")
            pending_resolutions.append(
                {
                    "capital_immobilized_notional_ns": format(
                        report.capital_immobilized_notional_ns,
                        "f",
                    ),
                    "market_id": report.market_id,
                    "opportunity_id": report.opportunity_id,
                    "outcome_id": report.outcome_id,
                    "source_report_sha256": report.report_sha256,
                    "unresolved_exposure": format(report.unresolved_exposure, "f"),
                    "variant_id": report.variant_id,
                    "venue": report.venue.value,
                }
            )
            continue
        if report.reconciliation_difference != _ZERO:
            raise ValueError("prediction economic evaluation requires reconciled reports")
        observations.append(
            EvaluationObservation(
                opportunity_id=report.opportunity_id,
                variant_id=report.variant_id,
                venue=report.venue,
                market_id=report.market_id,
                outcome_id=report.outcome_id,
                observed_at_ns=report.signal_time_utc_ns,
                max_evidence_received_utc_ns=report.max_evidence_received_utc_ns,
                net_pnl=report.net_pnl,
                gross_pnl=report.gross_pnl,
                costs=report.fees,
                spread_cost=report.spread_cost,
                finite_depth_slippage=report.finite_depth_slippage,
                capital_immobilized_notional_ns=(
                    report.capital_immobilized_notional_ns
                ),
                drawdown=report.drawdown,
                turnover=report.turnover,
                exposure=report.turnover,
                dataset_sha256=campaign_replay.dataset_sha256,
                source_report_sha256=report.report_sha256,
                synthetic=report.dataset_synthetic,
            )
        )
        component_reports = cast(
            Sequence[PredictionEvaluationReport],
            getattr(report, "leg_reports", (report,)),
        )
        if any(
            item.net_pnl is None or item.gross_pnl is None
            for item in component_reports
        ):
            raise ValueError("resolved group report contains unresolved attribution legs")
        attribution_components.extend(
            (report.opportunity_id, item) for item in component_reports
        )
    registered = {item.variant_id for item in preregistration.variants}
    if any(item.variant_id not in registered for item in observations):
        raise ValueError("evaluation contains an unregistered variant")
    if selection_view.exposes_final_test:
        raise ValueError("selection view must never expose final test")
    if any(item.dataset_sha256 != selection_view.dataset_hash for item in observations):
        raise ValueError("evaluation observation dataset identity diverged")
    ordered = observations
    assigned: list[tuple[EvaluationObservation, EvaluationSplit]] = []
    purged: list[tuple[EvaluationObservation, str]] = []
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    embargo = timedelta(seconds=preregistration.runner_policy.embargo_seconds)
    last_split_rank = -1
    for item in ordered:
        observed = epoch + timedelta(microseconds=item.observed_at_ns // 1_000)
        max_evidence = epoch + timedelta(
            microseconds=item.max_evidence_received_utc_ns // 1_000
        )
        if selection_view.train.start <= observed < selection_view.train.end:
            split = EvaluationSplit.TRAIN
        elif selection_view.validation.start <= observed < selection_view.validation.end:
            split = EvaluationSplit.VALIDATION
        elif observed >= selection_view.validation.end:
            raise ValueError("SEALED_HOLDOUT_OBSERVATION_FORBIDDEN")
        else:
            raise ValueError("evaluation observation falls outside the split plan")
        split_rank = {
            EvaluationSplit.TRAIN: 0,
            EvaluationSplit.VALIDATION: 1,
        }[split]
        if split_rank < last_split_rank:
            raise ValueError("PREDICTION_CAUSAL_SPLIT_ROLLBACK")
        last_split_rank = split_rank
        if split is EvaluationSplit.TRAIN:
            if observed >= selection_view.train.end - embargo or max_evidence >= selection_view.train.end:
                purged.append((item, "PURGED_CROSSES_TRAIN_SPLIT"))
            else:
                assigned.append((item, EvaluationSplit.TRAIN))
        else:
            if (
                observed >= selection_view.validation.end - embargo
                or max_evidence >= selection_view.validation.end
            ):
                if max_evidence >= selection_view.validation.end:
                    raise ValueError("SEALED_HOLDOUT_EVIDENCE_FORBIDDEN")
                purged.append((item, "PURGED_CROSSES_HOLDOUT_EMBARGO"))
            else:
                assigned.append((item, EvaluationSplit.VALIDATION))
    adjusted_alpha = preregistration.familywise_alpha / Decimal(len(registered))
    variant_reports: list[dict[str, CanonicalValue]] = []
    coverage = campaign_replay.prospective_slot_coverage
    coverage_complete = campaign_replay.dataset_synthetic or (
        isinstance(coverage, Mapping)
        and coverage.get("economic_corpus_complete") is True
    )
    corpus_sufficient = not pending_resolutions and coverage_complete
    for variant in preregistration.variants:
        selected = [
            item
            for item, split in assigned
            if item.variant_id == variant.variant_id and split is EvaluationSplit.VALIDATION
        ]
        markets = {item.market_id for item in selected}
        lcb = _cantelli_lcb([item.net_pnl for item in selected], adjusted_alpha)
        enough = (
            len(selected) >= preregistration.minimum_observations_per_variant
            and len(markets) >= preregistration.minimum_markets
            and not any(item.synthetic for item in selected)
        )
        required_for_corpus = variant.role != "REGISTERED_EXPLORATORY"
        variant_corpus_sufficient = (
            enough and coverage_complete and not pending_resolutions
        )
        if required_for_corpus:
            corpus_sufficient &= variant_corpus_sufficient
        variant_reports.append(
            {
                "adjusted_alpha": format(adjusted_alpha, "f"),
                "capital_immobilized_notional_ns": format(
                    sum(
                        (
                            item.capital_immobilized_notional_ns
                            for item in selected
                        ),
                        _ZERO,
                    ),
                    "f",
                ),
                "costs": format(sum((item.costs for item in selected), _ZERO), "f"),
                "drawdown_max": (
                    None
                    if not selected
                    else format(
                        cast(Decimal, _maximum_drawdown([item.net_pnl for item in selected])),
                        "f",
                    )
                ),
                "exposure_max": (
                    None if not selected else format(max(item.exposure for item in selected), "f")
                ),
                "family_id": variant.family_id,
                "finite_depth_slippage": format(
                    sum((item.finite_depth_slippage for item in selected), _ZERO),
                    "f",
                ),
                "gross_pnl": format(sum((item.gross_pnl for item in selected), _ZERO), "f"),
                "lcb_net_pnl_per_observation": None if lcb is None else format(lcb, "f"),
                "markets": len(markets),
                "net_pnl": format(sum((item.net_pnl for item in selected), _ZERO), "f"),
                "observations": len(selected),
                "role": variant.role,
                "spread_cost": format(
                    sum((item.spread_cost for item in selected), _ZERO), "f"
                ),
                "status": (
                    "EXPLORATORY_NOT_ECONOMIC_GATE"
                    if not required_for_corpus
                    else (
                        "CORPUS_SUFFICIENT"
                        if variant_corpus_sufficient
                        else INSUFFICIENT_PUBLIC_CORPUS
                    )
                ),
                "turnover": format(sum((item.turnover for item in selected), _ZERO), "f"),
                "variant_id": variant.variant_id,
            }
        )
    if source_status is PublicSourceStatus.PUBLIC_SOURCE_UNAVAILABLE:
        status = PUBLIC_SOURCE_UNAVAILABLE
    elif not corpus_sufficient:
        status = INSUFFICIENT_PUBLIC_CORPUS
    else:
        status = ECONOMIC_NOT_AVAILABLE
    validation_items = [item for item, split in assigned if split is EvaluationSplit.VALIDATION]
    validation_opportunities = {item.opportunity_id for item in validation_items}
    validation_components = [
        component
        for opportunity_id, component in attribution_components
        if opportunity_id in validation_opportunities
    ]
    attribution: list[dict[str, CanonicalValue]] = []
    attribution_keys = sorted(
        {
            (item.venue.value, item.market_id, item.outcome_id)
            for item in validation_components
        }
    )
    for venue, market_id, outcome_id in attribution_keys:
        scoped = [
            item
            for item in validation_components
            if (
                item.venue.value,
                item.market_id,
                item.outcome_id,
            )
            == (venue, market_id, outcome_id)
        ]
        attribution.append(
            {
                "capital_immobilized_notional_ns": format(
                    sum(
                        (item.capital_immobilized_notional_ns for item in scoped),
                        _ZERO,
                    ),
                    "f",
                ),
                "costs": format(sum((item.fees for item in scoped), _ZERO), "f"),
                "finite_depth_slippage": format(
                    sum((item.finite_depth_slippage for item in scoped), _ZERO),
                    "f",
                ),
                "gross_pnl": format(
                    sum((cast(Decimal, item.gross_pnl) for item in scoped), _ZERO),
                    "f",
                ),
                "market_id": market_id,
                "net_pnl": format(
                    sum(
                        (
                            cast(Decimal, item.net_pnl)
                            for item in scoped
                        ),
                        _ZERO,
                    ),
                    "f",
                ),
                "outcome_id": outcome_id,
                "spread_cost": format(
                    sum((item.spread_cost for item in scoped), _ZERO), "f"
                ),
                "venue": venue,
            }
        )
    body: dict[str, Any] = {
        "attribution": attribution,
        "boundary": BOUNDARY,
        "candidate_config_sha256": preregistration.config_sha256,
        "candidate_id": preregistration.candidate_id,
        "campaign_manifest_sha256": campaign_manifest["manifest_sha256"],
        "confidence_method": "BONFERRONI_CANTELLI_LCB",
        "economic_evidence_status": status,
        "holdout": {
            "access": preregistration.holdout_access,
            "metrics_exposed": False,
        },
        "multiple_testing_population": len(registered),
        "opportunity_set_sha256": campaign_replay.opportunity_set_sha256,
        "pending_resolutions": pending_resolutions,
        "purged_observations": [
            {
                "opportunity_id": item.opportunity_id,
                "reason": reason,
                "variant_id": item.variant_id,
            }
            for item, reason in purged
        ],
        "source_report_sha256s": [item.report_sha256 for item in reports],
        "source_campaign_replay_sha256": campaign_replay.report_sha256,
        "schema_version": 1,
        "go_no_go": "NO_GO_GHOST_ONLY_ECONOMIC_EVIDENCE_NOT_AVAILABLE",
        "selection_split_view": selection_view.to_dict(),
        "source_status": source_status.value,
        "variants": variant_reports,
    }
    if source_status_by_venue is not None:
        if set(source_status_by_venue) != {Venue.POLYMARKET, Venue.KALSHI}:
            raise ValueError("prediction evaluation venue source status map is incomplete")
        body["source_status_by_venue"] = {
            venue.value: source_status_by_venue[venue]
            for venue in (Venue.POLYMARKET, Venue.KALSHI)
        }
    result = {
        **body,
        "report_sha256": hashlib.sha256(canonical_json_bytes(body)).hexdigest(),
    }
    canonical = canonical_value(result)
    if not isinstance(canonical, dict):
        raise AssertionError("evaluation report must remain an object")
    return canonical


def prepare_prediction_campaign(
    *,
    output_root: Path,
    campaign_id: str,
    starts_at_utc: str,
    preregistration: CandidatePreregistration,
    contracts: Sequence[OfficialPublicContract],
) -> Mapping[str, CanonicalValue]:
    if output_root.exists():
        raise FileExistsError("prediction campaign output root must be new")
    if not campaign_id or not starts_at_utc:
        raise ValueError("prediction campaign identity and prospective start are required")
    starts_at = datetime.fromisoformat(starts_at_utc.replace("Z", "+00:00"))
    if starts_at.tzinfo is None or starts_at.utcoffset() is None:
        raise ValueError("prediction campaign start must be timezone-aware")
    starts_at = starts_at.astimezone(UTC)
    train_end = starts_at + timedelta(days=preregistration.train_days)
    validation_end = train_end + timedelta(days=preregistration.validation_days)
    final_end = validation_end + timedelta(days=preregistration.final_test_days)
    by_venue = {item.venue: item for item in contracts}
    if set(by_venue) != {Venue.POLYMARKET, Venue.KALSHI}:
        raise ValueError("prediction campaign requires both official venue contracts")
    collection_plans = canonical_value(
        {
            venue.value: preregistration.collection_plans[venue].to_dict(
                campaign_id=campaign_id
            )
            for venue in (Venue.POLYMARKET, Venue.KALSHI)
        }
    )
    if not isinstance(collection_plans, dict):
        raise AssertionError("prediction collection plans must remain an object")
    output_root.mkdir(parents=True)
    manifest_body: dict[str, CanonicalValue] = {
        "boundary": BOUNDARY,
        "campaign_id": campaign_id,
        "candidate_config_sha256": preregistration.config_sha256,
        "candidate_id": preregistration.candidate_id,
        "contracts": {
            venue.value: by_venue[venue].contract_sha256 for venue in (Venue.POLYMARKET, Venue.KALSHI)
        },
        "collection_plans": collection_plans,
        "economic_evidence_status": ECONOMIC_NOT_AVAILABLE,
        "holdout": {
            "access": "SEALED",
            "end_exclusive": final_end.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "reporting": "NO_HOLDOUT_METRICS",
            "start_inclusive": validation_end.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
        },
        "operator_commands_are_text_only": True,
        "prospective_shard_policy": preregistration.prospective_shard_policy.to_dict(),
        "prospective_shard_policy_sha256": (
            preregistration.prospective_shard_policy.policy_sha256
        ),
        "runner_policy_sha256": preregistration.runner_policy.policy_sha256,
        "schema_version": 1,
        "starts_at_utc": starts_at_utc,
        "train": {
            "end_exclusive": train_end.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "start_inclusive": starts_at.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
        },
        "validation": {
            "end_exclusive": validation_end.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "start_inclusive": train_end.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
        },
        "status": AWAITING_HUMAN_EXECUTION,
        "vps_or_h1_path": "NONE",
    }
    manifest = {
        **manifest_body,
        "manifest_sha256": hashlib.sha256(canonical_json_bytes(manifest_body)).hexdigest(),
    }

    def operator_command(plan: PredictionCollectionPlan) -> str:
        return (
            "python -m hyperlab.cli research-data prediction-collect "
            f"--venue {plan.venue.value.lower()} "
            "--campaign-manifest <CAMPAIGN_MANIFEST_PATH> "
            "--shard-ordinal <SCHEDULED_SHARD_ORDINAL> "
            f"--feeds {','.join(plan.feeds)} --census-limit {plan.census_limit} "
            f"--duration-seconds {plan.duration_seconds} "
            f"--max-network-calls {plan.max_network_calls} "
            f"--max-frames {plan.max_frames} --max-bytes {plan.max_bytes} "
            "--output-root <NEW_PATH>"
        )

    operator_body = {
        "boundary": BOUNDARY,
        "commands": {
            "kalshi": operator_command(preregistration.collection_plans[Venue.KALSHI]),
            "polymarket": operator_command(
                preregistration.collection_plans[Venue.POLYMARKET]
            ),
        },
        "execution": AWAITING_HUMAN_EXECUTION,
        "expected_shards_per_venue": (
            preregistration.prospective_shard_policy.expected_shards_per_venue
        ),
        "location": "LOCAL_WINDOWS_POWERSHELL_ONLY",
        "never_execute_automatically": True,
        "schema_version": 1,
    }
    manifest_bytes = canonical_json_bytes(manifest) + b"\n"
    _atomic_new_bytes(output_root / "campaign-manifest.json", manifest_bytes)
    _atomic_new_bytes(
        output_root / "campaign-manifest.sha256",
        (hashlib.sha256(manifest_bytes).hexdigest() + "  campaign-manifest.json\n").encode(),
    )
    _atomic_new_bytes(
        output_root / "operator-commands.json",
        canonical_json_bytes(operator_body) + b"\n",
    )
    return manifest


__all__ = [
    "AWAITING_HUMAN_EXECUTION",
    "DATASET_MODEL_VERSION",
    "ECONOMIC_NOT_AVAILABLE",
    "GHOST_MODEL_VERSION",
    "INSUFFICIENT_PUBLIC_CORPUS",
    "PUBLIC_SOURCE_UNAVAILABLE",
    "READY",
    "TRADE_DATASET_MODEL_VERSION",
    "CandidatePreregistration",
    "CandidateVariant",
    "EvaluationSplit",
    "FeeModel",
    "PredictionDepthLevel",
    "PredictionDepthSnapshot",
    "PredictionEvaluationReport",
    "PredictionFeeSchedule",
    "PredictionPointInTimeDataset",
    "PredictionTickBand",
    "PredictionTickGrid",
    "PredictionTradeDataset",
    "PredictionTradeObservation",
    "PublicSourceStatus",
    "build_prediction_dataset",
    "build_prediction_split_plan",
    "build_prediction_trade_dataset",
    "evaluate_preregistered",
    "prediction_prospective_shard_ordinal",
    "prepare_prediction_campaign",
    "revalidate_prediction_dataset",
    "revalidate_prediction_fee_schedule",
    "revalidate_prediction_tick_grid",
    "validate_prediction_campaign_manifest",
    "verify_prediction_collection_plan_payload",
]
