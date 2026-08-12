from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from hyperlab.collector.models import ParsedRecord, WireEnvelope
from hyperlab.collector.parser import parse_websocket_message


def replay_fixture(
    path: Path,
    sink: Callable[[ParsedRecord], object],
    clock: Callable[[], datetime],
) -> dict[str, object]:
    """Replay fixture files in lexical order without constructing a network client."""

    if not path.is_dir():
        raise ValueError(f"replay fixture directory not found: {path}")
    fixture_paths = tuple(
        candidate
        for candidate in sorted(path.iterdir(), key=lambda item: item.name)
        if candidate.is_file() and candidate.suffix.lower() in {".json", ".txt"}
    )
    if not fixture_paths:
        raise ValueError(f"replay fixture directory is empty: {path}")

    channels: list[str | None] = []
    issue_count = 0
    record_count = 0
    for sequence, fixture_path in enumerate(fixture_paths, start=1):
        raw_message = fixture_path.read_text(encoding="utf-8").rstrip("\r\n")
        envelope = WireEnvelope(
            raw_message=raw_message,
            received_time=clock(),
            connection_id="replay-connection",
            connection_epoch=1,
            arrival_sequence=sequence,
        )
        parsed = parse_websocket_message(envelope)
        channels.append(parsed.channel)
        issue_count += len(parsed.issues)
        for record in parsed.records:
            sink(record)
            record_count += 1

    return {
        "fixture_count": len(fixture_paths),
        "record_count": record_count,
        "issue_count": issue_count,
        "channels": channels,
    }
