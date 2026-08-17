from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from hyperlab.paper.models import (
    decimal_text as paper_decimal_text,
)
from hyperlab.paper.models import (
    decimal_value as paper_decimal_value,
)
from hyperlab.paper.models import (
    deterministic_id as paper_deterministic_id,
)
from hyperlab.paper.models import parse_utc as paper_parse_utc
from hyperlab.paper.models import utc_text as paper_utc_text

from hyperlab_testnet.canonical import (
    decimal_text,
    decimal_value,
    deterministic_id,
    parse_instrument,
    parse_utc,
    utc_text,
)


def test_testnet_canonical_primitives_match_phase12_pure_semantics() -> None:
    timestamp = datetime(2026, 8, 17, 5, 30, 12, 123456, tzinfo=UTC)
    value = Decimal("-0.125000")
    components = ("run", 7, value, timestamp, {"nested": [True, None]})

    assert decimal_value(value, label="value") == paper_decimal_value(value, label="value")
    assert decimal_text(value) == paper_decimal_text(value)
    assert utc_text(timestamp) == paper_utc_text(timestamp)
    assert parse_utc(utc_text(timestamp)) == paper_parse_utc(paper_utc_text(timestamp))
    assert deterministic_id("synthetic-testnet", *components) == paper_deterministic_id(
        "synthetic-testnet", *components
    )


def test_testnet_canonical_primitives_reject_ambiguous_values() -> None:
    with pytest.raises(TypeError):
        decimal_value(True, label="value")
    with pytest.raises(ValueError):
        parse_utc("not-a-timestamp")
    with pytest.raises(ValueError):
        deterministic_id("contains whitespace", "value")
    with pytest.raises(ValueError):
        parse_instrument("HL:BTC")
    for value in ("1e1000000000", "1e-1000000000", "1" * 65):
        with pytest.raises(ValueError, match="representation bound"):
            decimal_value(value, label="value")


def test_testnet_instrument_parser_is_dependency_light_and_exact() -> None:
    assert parse_instrument("HL:BTC:perp") == ("HL", "BTC", "perp")
    with pytest.raises(ValueError):
        parse_instrument("HL:BTC:future")
