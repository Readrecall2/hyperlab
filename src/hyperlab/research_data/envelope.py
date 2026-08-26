from __future__ import annotations

import base64
import binascii
import hashlib
import re
from collections import deque
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from .canonical import CanonicalValue, canonical_json_bytes, decode_canonical_json

RAW_ENVELOPE_SCHEMA_VERSION = 1
SYNTHETIC_FIXTURE_LABEL = "SYNTHETIC/FIXTURE"
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class Venue(StrEnum):
    HYPERLIQUID = "hyperliquid"
    LIGHTER = "lighter"
    POLYMARKET = "polymarket"
    KALSHI = "kalshi"


def _identifier(value: str, *, label: str) -> str:
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{label} is not a canonical identifier")
    return value


def _optional_identifier(value: str | None, *, label: str) -> str | None:
    return None if value is None else _identifier(value, label=label)


def _opaque_text(value: str, *, label: str) -> str:
    if type(value) is not str or not value or len(value) > 4096 or "\x00" in value:
        raise ValueError(f"{label} must be non-empty bounded text")
    return value


def _optional_opaque_text(value: str | None, *, label: str) -> str | None:
    return None if value is None else _opaque_text(value, label=label)


@dataclass(frozen=True, slots=True)
class GapDuplicateReconnectState:
    gap_detected: bool = False
    duplicate: bool = False
    reconnect: bool = False
    reason: str | None = None

    def __post_init__(self) -> None:
        if type(self.gap_detected) is not bool or type(self.duplicate) is not bool:
            raise TypeError("gap and duplicate states must be booleans")
        if type(self.reconnect) is not bool:
            raise TypeError("reconnect state must be a boolean")
        if self.reason is not None:
            _identifier(self.reason, label="gap state reason")
        if not (self.gap_detected or self.duplicate or self.reconnect) and self.reason is not None:
            raise ValueError("a normal frame cannot carry an exceptional state reason")

    def to_dict(self) -> dict[str, CanonicalValue]:
        return {
            "duplicate": self.duplicate,
            "gap_detected": self.gap_detected,
            "reason": self.reason,
            "reconnect": self.reconnect,
        }


@dataclass(frozen=True, slots=True)
class CaptureProvenance:
    collection_id: str
    source_url: str
    transport: str
    fixture_label: str | None = None

    def __post_init__(self) -> None:
        _identifier(self.collection_id, label="collection id")
        if type(self.source_url) is not str or not self.source_url.startswith(("https://", "wss://", "fixture://")):
            raise ValueError("source URL must use an explicitly public or fixture scheme")
        if self.transport not in {"PUBLIC_HTTP", "PUBLIC_WEBSOCKET", "FIXTURE"}:
            raise ValueError("unsupported capture transport")
        if self.transport == "FIXTURE" and self.fixture_label != SYNTHETIC_FIXTURE_LABEL:
            raise ValueError("fixtures must carry the visible SYNTHETIC/FIXTURE label")
        if self.transport != "FIXTURE" and self.fixture_label is not None:
            raise ValueError("a public capture cannot claim fixture provenance")

    def to_dict(self) -> dict[str, CanonicalValue]:
        return {
            "collection_id": self.collection_id,
            "fixture_label": self.fixture_label,
            "source_url": self.source_url,
            "transport": self.transport,
        }


@dataclass(frozen=True, slots=True)
class PublicDataEnvelope:
    schema_version: int
    venue: Venue
    feed_type: str
    instrument_id: str | None
    market_id: str | None
    source_timestamp_ns: int | None
    receive_timestamp_utc_ns: int
    receive_monotonic_ns: int
    source_sequence: int | str | None
    source_cursor: str | None
    arrival_sequence: int
    source_event_id: str | None
    raw_payload: bytes
    content_sha256: str
    collector_identity: str
    session_identity: str
    state: GapDuplicateReconnectState
    source_metadata_version: str
    provenance: CaptureProvenance

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != RAW_ENVELOPE_SCHEMA_VERSION:
            raise ValueError("unsupported raw envelope schema version")
        if type(self.venue) is not Venue:
            raise TypeError("venue must be a Venue")
        _identifier(self.feed_type, label="feed type")
        _optional_identifier(self.instrument_id, label="instrument id")
        _optional_identifier(self.market_id, label="market id")
        if self.instrument_id is None and self.market_id is None:
            raise ValueError("an instrument_id or market_id is required")
        for value, label in (
            (self.receive_timestamp_utc_ns, "UTC receive timestamp"),
            (self.receive_monotonic_ns, "monotonic receive timestamp"),
            (self.arrival_sequence, "arrival sequence"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{label} must be a non-negative integer")
        if self.arrival_sequence == 0:
            raise ValueError("arrival sequence is one-based")
        if self.source_timestamp_ns is not None and (
            type(self.source_timestamp_ns) is not int or self.source_timestamp_ns < 0
        ):
            raise ValueError("source timestamp must be absent or a non-negative UTC epoch value")
        if self.source_sequence is not None and type(self.source_sequence) not in {int, str}:
            raise TypeError("source sequence must remain absent, integer, or exact text")
        if type(self.source_sequence) is int and self.source_sequence < 0:
            raise ValueError("numeric source sequence cannot be negative")
        if type(self.source_sequence) is str:
            _opaque_text(self.source_sequence, label="source sequence")
        _optional_opaque_text(self.source_cursor, label="source cursor")
        _optional_opaque_text(self.source_event_id, label="source event id")
        if type(self.raw_payload) is not bytes:
            raise TypeError("raw payload must be exact bytes")
        if _SHA256.fullmatch(self.content_sha256) is None:
            raise ValueError("content SHA-256 must be lowercase hexadecimal")
        if hashlib.sha256(self.raw_payload).hexdigest() != self.content_sha256:
            raise ValueError("raw payload SHA-256 mismatch")
        _identifier(self.collector_identity, label="collector identity")
        _identifier(self.session_identity, label="session identity")
        _identifier(self.source_metadata_version, label="source metadata version")
        if type(self.state) is not GapDuplicateReconnectState:
            raise TypeError("state must be GapDuplicateReconnectState")
        if type(self.provenance) is not CaptureProvenance:
            raise TypeError("provenance must be CaptureProvenance")

    @classmethod
    def from_raw(
        cls,
        *,
        venue: Venue,
        feed_type: str,
        instrument_id: str | None,
        market_id: str | None,
        source_timestamp_ns: int | None,
        receive_timestamp_utc_ns: int,
        receive_monotonic_ns: int,
        source_sequence: int | str | None,
        source_cursor: str | None,
        arrival_sequence: int,
        source_event_id: str | None,
        raw_payload: bytes,
        collector_identity: str,
        session_identity: str,
        state: GapDuplicateReconnectState,
        source_metadata_version: str,
        provenance: CaptureProvenance,
    ) -> PublicDataEnvelope:
        return cls(
            schema_version=RAW_ENVELOPE_SCHEMA_VERSION,
            venue=venue,
            feed_type=feed_type,
            instrument_id=instrument_id,
            market_id=market_id,
            source_timestamp_ns=source_timestamp_ns,
            receive_timestamp_utc_ns=receive_timestamp_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            source_sequence=source_sequence,
            source_cursor=source_cursor,
            arrival_sequence=arrival_sequence,
            source_event_id=source_event_id,
            raw_payload=raw_payload,
            content_sha256=hashlib.sha256(raw_payload).hexdigest(),
            collector_identity=collector_identity,
            session_identity=session_identity,
            state=state,
            source_metadata_version=source_metadata_version,
            provenance=provenance,
        )

    def to_dict(self) -> dict[str, CanonicalValue]:
        return {
            "arrival_sequence": self.arrival_sequence,
            "collector_identity": self.collector_identity,
            "content_sha256": self.content_sha256,
            "feed_type": self.feed_type,
            "instrument_id": self.instrument_id,
            "market_id": self.market_id,
            "provenance": self.provenance.to_dict(),
            "raw_payload_base64": base64.b64encode(self.raw_payload).decode("ascii"),
            "receive_monotonic_ns": self.receive_monotonic_ns,
            "receive_timestamp_utc_ns": self.receive_timestamp_utc_ns,
            "schema_version": self.schema_version,
            "session_identity": self.session_identity,
            "source_cursor": self.source_cursor,
            "source_event_id": self.source_event_id,
            "source_metadata_version": self.source_metadata_version,
            "source_sequence": self.source_sequence,
            "source_timestamp_ns": self.source_timestamp_ns,
            "state": self.state.to_dict(),
            "venue": self.venue.value,
        }

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    @classmethod
    def from_canonical_bytes(cls, value: bytes) -> PublicDataEnvelope:
        decoded = decode_canonical_json(value, require_canonical=True)
        if not isinstance(decoded, dict):
            raise ValueError("raw envelope must be a canonical JSON object")
        expected = {
            "arrival_sequence",
            "collector_identity",
            "content_sha256",
            "feed_type",
            "instrument_id",
            "market_id",
            "provenance",
            "raw_payload_base64",
            "receive_monotonic_ns",
            "receive_timestamp_utc_ns",
            "schema_version",
            "session_identity",
            "source_cursor",
            "source_event_id",
            "source_metadata_version",
            "source_sequence",
            "source_timestamp_ns",
            "state",
            "venue",
        }
        if set(decoded) != expected:
            raise ValueError("raw envelope fields differ from schema v1")
        state = decoded["state"]
        provenance = decoded["provenance"]
        if not isinstance(state, dict) or not isinstance(provenance, dict):
            raise ValueError("raw envelope nested records must be objects")
        if set(state) != {"duplicate", "gap_detected", "reason", "reconnect"}:
            raise ValueError("raw envelope state fields differ from schema v1")
        if set(provenance) != {
            "collection_id",
            "fixture_label",
            "source_url",
            "transport",
        }:
            raise ValueError("raw envelope provenance fields differ from schema v1")
        for field in ("duplicate", "gap_detected", "reconnect"):
            if type(state[field]) is not bool:
                raise ValueError(f"raw envelope state {field} must be a boolean")
        if state["reason"] is not None and type(state["reason"]) is not str:
            raise ValueError("raw envelope state reason must be absent or text")
        for field in ("collection_id", "source_url", "transport"):
            if type(provenance[field]) is not str:
                raise ValueError(f"raw envelope provenance {field} must be text")
        if provenance["fixture_label"] is not None and type(provenance["fixture_label"]) is not str:
            raise ValueError("raw envelope provenance fixture_label must be absent or text")
        encoded_payload = decoded["raw_payload_base64"]
        if type(encoded_payload) is not str:
            raise ValueError("raw payload must be encoded as base64 text")
        try:
            raw_payload = base64.b64decode(encoded_payload, validate=True)
        except (binascii.Error, ValueError) as error:
            raise ValueError("raw payload is not strict base64") from error

        def required_int(field: str) -> int:
            candidate = decoded[field]
            if type(candidate) is not int:
                raise ValueError(f"raw envelope {field} must be an integer")
            return candidate

        def optional_int(field: str) -> int | None:
            candidate = decoded[field]
            if candidate is None:
                return None
            if type(candidate) is not int:
                raise ValueError(f"raw envelope {field} must be absent or an integer")
            return candidate

        def required_text(field: str) -> str:
            candidate = decoded[field]
            if type(candidate) is not str:
                raise ValueError(f"raw envelope {field} must be text")
            return candidate

        def optional_text(field: str) -> str | None:
            candidate = decoded[field]
            if candidate is not None and type(candidate) is not str:
                raise ValueError(f"raw envelope {field} must be absent or text")
            return candidate

        source_sequence_value = decoded["source_sequence"]
        if source_sequence_value is not None and type(source_sequence_value) not in {int, str}:
            raise ValueError("raw envelope source_sequence has an invalid type")
        return cls(
            schema_version=required_int("schema_version"),
            venue=Venue(required_text("venue")),
            feed_type=required_text("feed_type"),
            instrument_id=optional_text("instrument_id"),
            market_id=optional_text("market_id"),
            source_timestamp_ns=optional_int("source_timestamp_ns"),
            receive_timestamp_utc_ns=required_int("receive_timestamp_utc_ns"),
            receive_monotonic_ns=required_int("receive_monotonic_ns"),
            source_sequence=cast(int | str | None, source_sequence_value),
            source_cursor=optional_text("source_cursor"),
            arrival_sequence=required_int("arrival_sequence"),
            source_event_id=optional_text("source_event_id"),
            raw_payload=raw_payload,
            content_sha256=required_text("content_sha256"),
            collector_identity=required_text("collector_identity"),
            session_identity=required_text("session_identity"),
            state=GapDuplicateReconnectState(
                gap_detected=cast(bool, state["gap_detected"]),
                duplicate=cast(bool, state["duplicate"]),
                reconnect=cast(bool, state["reconnect"]),
                reason=state["reason"],
            ),
            source_metadata_version=required_text("source_metadata_version"),
            provenance=CaptureProvenance(
                collection_id=cast(str, provenance["collection_id"]),
                source_url=cast(str, provenance["source_url"]),
                transport=cast(str, provenance["transport"]),
                fixture_label=provenance["fixture_label"],
            ),
        )


class SessionEnvelopeFactory:
    """Assign local arrival order and explicit gap/duplicate/reconnect evidence."""

    def __init__(
        self,
        *,
        venue: Venue,
        collector_identity: str,
        session_identity: str,
        source_metadata_version: str,
        provenance: CaptureProvenance,
        dedup_capacity: int = 10_000,
        initial_arrival_sequence: int = 0,
    ) -> None:
        if dedup_capacity <= 0:
            raise ValueError("dedup capacity must be positive")
        if type(initial_arrival_sequence) is not int or initial_arrival_sequence < 0:
            raise ValueError("initial arrival sequence must be a non-negative integer")
        self.venue = venue
        self.collector_identity = _identifier(collector_identity, label="collector identity")
        self._session_base = _identifier(session_identity, label="session identity")
        self.source_metadata_version = _identifier(
            source_metadata_version, label="source metadata version"
        )
        self.provenance = provenance
        self._arrival_sequence = initial_arrival_sequence
        self._generation = 0
        self._last_monotonic_ns: int | None = None
        self._last_source_sequence: dict[tuple[str, str], int] = {}
        self._seen_order: deque[str] = deque()
        self._seen: set[str] = set()
        self._dedup_capacity = dedup_capacity
        self._reconnect_pending = False

    @property
    def session_identity(self) -> str:
        return f"{self._session_base}:{self._generation}"

    def begin_reconnect(self) -> None:
        self._generation += 1
        self._last_source_sequence.clear()
        self._reconnect_pending = True

    def make(
        self,
        *,
        feed_type: str,
        instrument_id: str | None,
        market_id: str | None,
        source_timestamp_ns: int | None,
        receive_timestamp_utc_ns: int,
        receive_monotonic_ns: int,
        raw_payload: bytes,
        source_sequence: int | str | None = None,
        source_cursor: str | None = None,
        source_event_id: str | None = None,
        provenance: CaptureProvenance | None = None,
        infer_source_sequence_continuity: bool = True,
        explicit_gap_detected: bool = False,
        explicit_gap_reason: str | None = None,
    ) -> PublicDataEnvelope:
        if type(infer_source_sequence_continuity) is not bool:
            raise TypeError("source sequence continuity mode must be a boolean")
        if type(explicit_gap_detected) is not bool:
            raise TypeError("explicit gap state must be a boolean")
        if explicit_gap_detected and explicit_gap_reason is None:
            raise ValueError("an explicit gap requires an explicit reason")
        if not explicit_gap_detected and explicit_gap_reason is not None:
            raise ValueError("an explicit gap reason requires an explicit gap")
        if self._last_monotonic_ns is not None and receive_monotonic_ns < self._last_monotonic_ns:
            raise ValueError("receive monotonic time regressed within the collector")
        self._last_monotonic_ns = receive_monotonic_ns
        self._arrival_sequence += 1
        identity_material = "|".join(
            (
                feed_type,
                instrument_id or "",
                market_id or "",
                source_event_id or "",
                str(source_timestamp_ns),
                hashlib.sha256(raw_payload).hexdigest(),
            )
        )
        duplicate_key = hashlib.sha256(identity_material.encode("utf-8")).hexdigest()
        duplicate = duplicate_key in self._seen
        if not duplicate:
            self._seen.add(duplicate_key)
            self._seen_order.append(duplicate_key)
            while len(self._seen_order) > self._dedup_capacity:
                self._seen.remove(self._seen_order.popleft())

        gap = explicit_gap_detected
        reason: str | None = explicit_gap_reason
        sequence_key = (feed_type, instrument_id or market_id or "")
        if infer_source_sequence_continuity and type(source_sequence) is int:
            previous = self._last_source_sequence.get(sequence_key)
            if previous is not None and source_sequence > previous + 1:
                gap = True
                reason = "SOURCE_SEQUENCE_GAP"
            elif previous is not None and source_sequence < previous:
                gap = True
                reason = "SOURCE_SEQUENCE_REGRESSION"
            self._last_source_sequence[sequence_key] = source_sequence
        if duplicate:
            reason = "DUPLICATE_SOURCE_EVENT"
        elif self._reconnect_pending and reason is None:
            reason = "RECONNECT_BOUNDARY"

        state = GapDuplicateReconnectState(
            gap_detected=gap,
            duplicate=duplicate,
            reconnect=self._reconnect_pending,
            reason=reason,
        )
        self._reconnect_pending = False
        selected_provenance = self.provenance if provenance is None else provenance
        if selected_provenance.collection_id != self.provenance.collection_id:
            raise ValueError("per-frame provenance must retain the factory collection id")
        return PublicDataEnvelope.from_raw(
            venue=self.venue,
            feed_type=feed_type,
            instrument_id=instrument_id,
            market_id=market_id,
            source_timestamp_ns=source_timestamp_ns,
            receive_timestamp_utc_ns=receive_timestamp_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            source_sequence=source_sequence,
            source_cursor=source_cursor,
            arrival_sequence=self._arrival_sequence,
            source_event_id=source_event_id,
            raw_payload=raw_payload,
            collector_identity=self.collector_identity,
            session_identity=self.session_identity,
            state=state,
            source_metadata_version=self.source_metadata_version,
            provenance=selected_provenance,
        )


__all__ = [
    "RAW_ENVELOPE_SCHEMA_VERSION",
    "SYNTHETIC_FIXTURE_LABEL",
    "CaptureProvenance",
    "GapDuplicateReconnectState",
    "PublicDataEnvelope",
    "SessionEnvelopeFactory",
    "Venue",
]
