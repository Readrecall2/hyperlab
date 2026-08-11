from __future__ import annotations

from decimal import Decimal

from hyperlab.api.public import parse_carry_markets


def test_parse_carry_market_payload() -> None:
    perp = [
        {"universe": [{"name": "BTC"}, {"name": "ETH"}]},
        [
            {"markPx": "101", "funding": "0.00001", "dayNtlVlm": "1000", "openInterest": "10"},
            {"markPx": "2000", "funding": "0", "dayNtlVlm": "900", "openInterest": "8"},
        ],
    ]
    spot = [
        {
            "tokens": [
                {"name": "BTC", "index": 0},
                {"name": "USDC", "index": 1},
            ],
            "universe": [{"name": "BTC/USDC", "tokens": [0, 1]}],
        },
        [{"midPx": "100", "dayNtlVlm": "500"}],
    ]
    result = parse_carry_markets(perp, spot, observed_at_ms=123)
    assert len(result) == 1
    assert result[0].asset == "BTC"
    assert result[0].basis_bps == Decimal("100")
    assert result[0].observed_at_ms == 123
