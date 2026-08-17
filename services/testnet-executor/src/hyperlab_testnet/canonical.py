"""Dependency-light canonical primitives for the isolated Testnet service."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import TypeAlias

JsonValue: TypeAlias = (
    bool
    | int
    | float
    | str
    | list["JsonValue"]
    | dict[str, "JsonValue"]
    | None
)

_MAX_DECIMAL_COEFFICIENT_DIGITS = 64
_MAX_DECIMAL_ABSOLUTE_EXPONENT = 64
_MAX_DECIMAL_ABSOLUTE_ADJUSTED = 64


def require_utc(value: datetime, *, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{label} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must use UTC")
    return value.astimezone(UTC)


def utc_text(value: datetime) -> str:
    return require_utc(value, label="timestamp").isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def parse_utc(value: str, *, label: str = "timestamp") -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError(f"{label} must be an ISO-8601 UTC timestamp") from error
    return require_utc(parsed, label=label)


def decimal_value(
    value: Decimal | str | int,
    *,
    label: str,
    positive: bool = False,
    non_negative: bool = False,
) -> Decimal:
    if isinstance(value, (bool, float)):
        raise TypeError(f"{label} must be an exact Decimal, integer, or decimal string")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{label} must be a valid decimal") from error
    if not result.is_finite():
        raise ValueError(f"{label} must be finite")
    decimal_tuple = result.as_tuple()
    exponent = decimal_tuple.exponent
    if (
        not isinstance(exponent, int)
        or len(decimal_tuple.digits) > _MAX_DECIMAL_COEFFICIENT_DIGITS
        or abs(exponent) > _MAX_DECIMAL_ABSOLUTE_EXPONENT
        or abs(result.adjusted()) > _MAX_DECIMAL_ABSOLUTE_ADJUSTED
    ):
        raise ValueError(f"{label} exceeds the exact decimal representation bound")
    if positive and result <= 0:
        raise ValueError(f"{label} must be positive")
    if non_negative and result < 0:
        raise ValueError(f"{label} must be non-negative")
    return Decimal(0) if result == 0 else result


def decimal_text(value: Decimal) -> str:
    exact = decimal_value(value, label="decimal")
    text = format(exact, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _identifier(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{label} cannot be empty")
    if any(character.isspace() for character in normalized):
        raise ValueError(f"{label} cannot contain whitespace")
    return normalized


def _json_value(value: object, *, path: str = "$") -> JsonValue:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} cannot contain NaN or infinity")
        return value
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, datetime):
        return utc_text(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        normalized: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} contains a non-string key")
            normalized[key] = _json_value(item, path=f"{path}.{key}")
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item, path=f"{path}[{index}]") for index, item in enumerate(value)]
    raise TypeError(f"{path} contains unsupported type {type(value).__name__}")


def canonical_json(value: object) -> str:
    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def deterministic_id(kind: str, *components: object) -> str:
    return canonical_sha256(
        {
            "components": [_json_value(component) for component in components],
            "kind": _identifier(kind, label="identifier kind"),
            "schema_version": 1,
        }
    )


def parse_instrument(value: str) -> tuple[str, str, str]:
    if not isinstance(value, str):
        raise TypeError("instrument must be a string")
    parts = value.split(":")
    if len(parts) != 3 or not all(parts):
        raise ValueError("instrument must use the canonical VENUE:ASSET:kind form")
    venue, asset, kind = parts
    if kind not in {"spot", "perp"}:
        raise ValueError(f"unsupported instrument kind {kind!r}")
    return venue, asset, kind


__all__ = [
    "JsonValue",
    "canonical_json",
    "canonical_sha256",
    "decimal_text",
    "decimal_value",
    "deterministic_id",
    "parse_instrument",
    "parse_utc",
    "require_utc",
    "utc_text",
]
