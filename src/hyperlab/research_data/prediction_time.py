from __future__ import annotations

import re
from datetime import UTC, datetime

_RFC3339_PATTERN = re.compile(
    r"(?P<base>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?"
    r"(?P<offset>Z|[+-]\d{2}:\d{2})\Z"
)


def prediction_rfc3339_to_ns(value: object, *, label: str) -> int:
    """Parse one strict RFC3339 timestamp without losing nanoseconds."""

    if type(value) is not str or not value:
        raise ValueError(f"{label} must be non-empty RFC3339 text")
    match = _RFC3339_PATTERN.fullmatch(value)
    if match is None:
        raise ValueError(f"{label} must be strict RFC3339 with an explicit offset")
    offset = "+00:00" if match.group("offset") == "Z" else match.group("offset")
    try:
        timestamp = datetime.fromisoformat(f"{match.group('base')}{offset}")
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError(f"{label} must be timezone-aware")
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        delta = timestamp.astimezone(UTC) - epoch
    except (ValueError, OverflowError) as error:
        raise ValueError(f"{label} must be a valid RFC3339 timestamp") from error
    fraction = match.group("fraction") or ""
    fraction_ns = int(fraction.ljust(9, "0")) if fraction else 0
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + fraction_ns
    )


__all__ = ["prediction_rfc3339_to_ns"]
