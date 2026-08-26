from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import NoReturn, TypeAlias, cast

CanonicalScalar: TypeAlias = bool | int | str | None
CanonicalValue: TypeAlias = CanonicalScalar | list["CanonicalValue"] | dict[str, "CanonicalValue"]


class CanonicalDataError(ValueError):
    """A value is not representable by the Research Data Plane JSON contract."""


def _reject_constant(value: str) -> NoReturn:
    raise CanonicalDataError(f"non-finite JSON constant {value!r} is forbidden")


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise CanonicalDataError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def canonical_value(value: object) -> CanonicalValue:
    """Return a detached JSON value while rejecting floats and ambiguous mappings."""

    if value is None or type(value) in {bool, int, str}:
        return cast(CanonicalScalar, value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise CanonicalDataError("non-finite Decimal is forbidden")
        return format(value, "f")
    if isinstance(value, Mapping):
        normalized: dict[str, CanonicalValue] = {}
        for key, item in value.items():
            if type(key) is not str or not key:
                raise CanonicalDataError("canonical object keys must be non-empty text")
            normalized[key] = canonical_value(item)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [canonical_value(item) for item in value]
    if isinstance(value, float):
        raise CanonicalDataError("binary floats are forbidden; preserve the source decimal string")
    raise CanonicalDataError(f"unsupported canonical value type {type(value).__name__}")


def canonical_json_bytes(value: object) -> bytes:
    normalized = canonical_value(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def decode_canonical_json(value: bytes, *, require_canonical: bool = True) -> CanonicalValue:
    try:
        decoded = json.loads(
            value.decode("utf-8"),
            object_pairs_hook=_strict_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CanonicalDataError("payload is not strict UTF-8 JSON") from error
    normalized = canonical_value(decoded)
    if require_canonical and canonical_json_bytes(normalized) != value:
        raise CanonicalDataError("JSON payload is not byte-canonical")
    return normalized


__all__ = [
    "CanonicalDataError",
    "CanonicalValue",
    "canonical_json_bytes",
    "canonical_value",
    "decode_canonical_json",
]
