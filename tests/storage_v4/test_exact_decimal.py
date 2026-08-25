from __future__ import annotations

import pytest

from hyperlab.paper.storage_v4.exact_decimal import ExactDecimalSum

SYNTHETIC_STORAGE_V4_WORKLOAD = True


def test_exact_decimal_cancellation_exceeds_normal_decimal_context_precision() -> None:
    large = "1" + ("0" * 120)
    value = ExactDecimalSum().add_text(large).add_text("1").add_text("-" + large)

    assert value.text == "1"


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        (("1.2500", "-0.25"), "1"),
        (("0.0001", "0.0002"), "0.0003"),
        (("1e3", "2e2"), "1200"),
        (("-0", "0.000"), "0"),
    ],
)
def test_exact_decimal_output_is_plain_normalized_text(
    values: tuple[str, ...],
    expected: str,
) -> None:
    total = ExactDecimalSum()
    for value in values:
        total = total.add_text(value)
    assert total.text == expected


@pytest.mark.parametrize("value", ("", "NaN", "Infinity", "-Infinity", "not-a-number"))
def test_exact_decimal_rejects_nonfinite_or_malformed_text(value: str) -> None:
    with pytest.raises(ValueError):
        ExactDecimalSum.from_text(value)
