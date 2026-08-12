from __future__ import annotations

from decimal import Decimal

from hyperlab.api.public import parse_carry_markets


def test_parse_carry_market_payload() -> None:
    perp = [
        {"universe": [{"name": "HYPE"}]},
        [
            {"markPx": "101", "funding": "0.00001", "dayNtlVlm": "1000", "openInterest": "10"},
        ],
    ]
    spot = [
        {
            "tokens": [
                {"name": "USDC", "index": 0, "tokenId": "quote"},
                {"name": "REMAPPED-HYPE", "index": 150, "tokenId": "0x0d01dc56dcaaca66ad901c959b4011ec"},
            ],
            "universe": [{"name": "@107", "index": 107, "tokens": [150, 0]}],
        },
        [{"coin": "@107", "midPx": "100", "dayNtlVlm": "500"}],
    ]
    result = parse_carry_markets(perp, spot, observed_at_ms=123)
    assert len(result) == 1
    assert result[0].asset == "HYPE"
    assert result[0].basis_bps == Decimal("100")
    assert result[0].observed_at_ms == 123
