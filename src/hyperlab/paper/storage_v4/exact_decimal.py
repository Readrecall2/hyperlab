"""Exact finite decimal accumulation without context-dependent rounding."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Self


@dataclass(frozen=True, slots=True)
class ExactDecimalSum:
    """One exact integer coefficient times ten to an integer exponent."""

    coefficient: int = 0
    exponent: int = 0

    def __post_init__(self) -> None:
        if type(self.coefficient) is not int or type(self.exponent) is not int:
            raise TypeError("exact decimal coefficient and exponent must be integers")
        if self.coefficient == 0 and self.exponent != 0:
            raise ValueError("exact decimal zero must use exponent zero")
        if self.coefficient != 0 and self.coefficient % 10 == 0:
            raise ValueError("exact decimal coefficient must be normalized")

    @classmethod
    def from_text(cls, value: str) -> Self:
        if type(value) is not str or not value:
            raise ValueError("exact decimal input must be nonempty text")
        try:
            parsed = Decimal(value)
        except InvalidOperation as error:
            raise ValueError("exact decimal input is malformed") from error
        if not parsed.is_finite():
            raise ValueError("exact decimal input must be finite")
        parts = parsed.as_tuple()
        if type(parts.exponent) is not int:
            raise ValueError("exact decimal exponent is not finite")
        coefficient = 0
        for digit in parts.digits:
            coefficient = coefficient * 10 + digit
        if parts.sign:
            coefficient = -coefficient
        return cls._normalized(coefficient, parts.exponent)

    @classmethod
    def _normalized(cls, coefficient: int, exponent: int) -> Self:
        if coefficient == 0:
            return cls()
        while coefficient % 10 == 0:
            coefficient //= 10
            exponent += 1
        return cls(coefficient=coefficient, exponent=exponent)

    def add(self, other: ExactDecimalSum) -> Self:
        if type(other) is not ExactDecimalSum:
            raise TypeError("exact decimal addition requires ExactDecimalSum")
        common_exponent = min(self.exponent, other.exponent)
        left = self.coefficient * (10 ** (self.exponent - common_exponent))
        right = other.coefficient * (10 ** (other.exponent - common_exponent))
        return type(self)._normalized(left + right, common_exponent)

    def add_text(self, value: str) -> Self:
        return self.add(type(self).from_text(value))

    @property
    def text(self) -> str:
        if self.coefficient == 0:
            return "0"
        sign = "-" if self.coefficient < 0 else ""
        digits = str(abs(self.coefficient))
        if self.exponent >= 0:
            return sign + digits + ("0" * self.exponent)
        fractional_places = -self.exponent
        if len(digits) > fractional_places:
            split = len(digits) - fractional_places
            return sign + digits[:split] + "." + digits[split:]
        return sign + "0." + ("0" * (fractional_places - len(digits))) + digits


__all__ = ["ExactDecimalSum"]
