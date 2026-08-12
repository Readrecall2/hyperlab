from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta

from hyperlab.collector.models import ParsedMessage, ParsedRecord, WireEnvelope
from hyperlab.data.schema import RecordType
from hyperlab.venues.base import PublicVenueConnector


@dataclass(frozen=True, slots=True)
class ReplayIssue:
    kind: str
    venue: str
    at: datetime
    detail: str


@dataclass(frozen=True, slots=True)
class ReplayEvent:
    venue: str
    envelope: WireEnvelope
    parsed: ParsedMessage


@dataclass(frozen=True, slots=True)
class MultiVenueReplayResult:
    events: tuple[ReplayEvent, ...]
    issues: tuple[ReplayIssue, ...]
    record_count: int

    def as_dict(self) -> dict[str, object]:
        counts: dict[str, int] = {}
        for issue in self.issues:
            counts[issue.kind] = counts.get(issue.kind, 0) + 1
        return {
            "event_count": len(self.events),
            "record_count": self.record_count,
            "issue_count": len(self.issues),
            "issues_by_kind": dict(sorted(counts.items())),
            "network_enabled": False,
        }


def _source_time(parsed: ParsedMessage) -> datetime | None:
    times: list[datetime] = []
    for record in parsed.records:
        value = record.row.get("event_time")
        if record.record_type != RecordType.WIRE_MESSAGE and isinstance(value, datetime):
            times.append(value)
    return None if not times else min(times)


def replay_synchronized(
    connectors: Mapping[str, PublicVenueConnector],
    captures: Mapping[str, Iterable[WireEnvelope]],
    *,
    sink: Callable[[ParsedRecord], object] | None = None,
    max_clock_skew: timedelta = timedelta(milliseconds=250),
    venue_absent_after: timedelta = timedelta(seconds=5),
) -> MultiVenueReplayResult:
    """Merge by receive time; never reorder frames using an exchange clock."""

    if max_clock_skew < timedelta(0) or venue_absent_after <= timedelta(0):
        raise ValueError("replay synchronization thresholds are invalid")
    if set(connectors) != set(captures) or len(connectors) < 2:
        raise ValueError("replay requires the same two or more venues in connectors and captures")

    issues: list[ReplayIssue] = []
    merged: list[tuple[datetime, str, int, int, WireEnvelope]] = []
    for venue, raw_envelopes in captures.items():
        previous_received: datetime | None = None
        for envelope in raw_envelopes:
            if previous_received is not None and envelope.received_time < previous_received:
                issues.append(
                    ReplayIssue(
                        "capture_receive_regression",
                        venue,
                        envelope.received_time,
                        f"{envelope.received_time.isoformat()} < {previous_received.isoformat()}",
                    )
                )
            previous_received = envelope.received_time
            merged.append(
                (
                    envelope.received_time,
                    venue,
                    envelope.connection_epoch,
                    envelope.arrival_sequence,
                    envelope,
                )
            )
    merged.sort(key=lambda item: item[:4])

    events: list[ReplayEvent] = []
    last_received: dict[str, datetime] = {}
    last_source: dict[tuple[str, str | None], datetime] = {}
    absent: set[str] = set()
    record_count = 0
    for received_time, venue, _epoch, _sequence, envelope in merged:
        connector = connectors[venue]
        if connector.venue != venue:
            raise ValueError(f"connector key {venue!r} does not match connector venue")
        previous = last_received.get(venue)
        if previous is not None and received_time - previous > venue_absent_after:
            issues.append(
                ReplayIssue(
                    "venue_absence_gap",
                    venue,
                    received_time,
                    f"silence={(received_time - previous).total_seconds():.6f}s",
                )
            )
        if venue in absent:
            issues.append(ReplayIssue("venue_recovered", venue, received_time, "frame received again"))
            absent.remove(venue)
        last_received[venue] = received_time

        for other_venue, other_time in sorted(last_received.items()):
            if other_venue == venue:
                continue
            silence = received_time - other_time
            if silence > venue_absent_after and other_venue not in absent:
                issues.append(
                    ReplayIssue(
                        "venue_absent",
                        other_venue,
                        received_time,
                        f"silent while {venue} advanced by {silence.total_seconds():.6f}s",
                    )
                )
                absent.add(other_venue)

        if len(last_received) == len(connectors):
            spread = max(last_received.values()) - min(last_received.values())
            if spread > max_clock_skew:
                issues.append(
                    ReplayIssue(
                        "venues_desynchronized",
                        venue,
                        received_time,
                        f"receive spread={spread.total_seconds():.6f}s",
                    )
                )

        parsed = connector.parse_message(envelope)
        source_time = _source_time(parsed)
        stream_key = (venue, parsed.channel)
        prior_source = last_source.get(stream_key)
        if source_time is not None:
            if prior_source is not None and source_time < prior_source:
                issues.append(
                    ReplayIssue(
                        "source_time_out_of_order",
                        venue,
                        received_time,
                        f"{source_time.isoformat()} < {prior_source.isoformat()}",
                    )
                )
            last_source[stream_key] = max(source_time, prior_source or source_time)
        event = ReplayEvent(venue, envelope, parsed)
        events.append(event)
        for record in parsed.records:
            if sink is not None:
                sink(record)
            record_count += 1

    return MultiVenueReplayResult(tuple(events), tuple(issues), record_count)
