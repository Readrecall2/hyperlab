from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.request import Request

import pytest

from hyperlab.api.public import HyperliquidPublicClient, parse_carry_markets


class FakeInfo:
    def __init__(self, responder: Callable[[Mapping[str, Any]], object]) -> None:
        self.responder = responder
        self.posts: list[tuple[str, dict[str, Any]]] = []

    def post(self, url_path: str, payload: Any = None) -> object:
        assert isinstance(payload, Mapping)
        copied = dict(payload)
        self.posts.append((url_path, copied))
        return self.responder(copied)


class _FakeHttpResponse:
    def __init__(
        self,
        payload: bytes,
        *,
        final_url: str = "https://api.hyperliquid.xyz/info",
        status: int = 200,
    ) -> None:
        self.payload = payload
        self.final_url = final_url
        self.status = status

    def __enter__(self) -> _FakeHttpResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        return self.payload if size < 0 else self.payload[:size]

    def geturl(self) -> str:
        return self.final_url

    def getcode(self) -> int:
        return self.status


class _FakeOpener:
    def __init__(self, responses: Iterator[_FakeHttpResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[Request, float]] = []

    def open(self, request: Request, *, timeout: float) -> _FakeHttpResponse:
        self.calls.append((request, timeout))
        return next(self.responses)


def test_default_transport_is_public_info_only_and_sdk_is_not_a_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opener = _FakeOpener(
        iter(
            (
                _FakeHttpResponse(b'[{"universe":[]},[]]'),
                _FakeHttpResponse(b'[{"tokens":[],"universe":[]},[]]'),
            )
        )
    )
    redirect_handlers: list[object] = []

    def fake_build_opener(*handlers: object) -> _FakeOpener:
        redirect_handlers.extend(handlers)
        return opener

    monkeypatch.setattr("hyperlab.api.public.urllib.request.build_opener", fake_build_opener)

    result = HyperliquidPublicClient(network="mainnet", timeout_seconds=7.0).bootstrap(
        observed_at_ms=123
    )

    assert result.observed_at_ms == 123
    assert [request.full_url for request, _ in opener.calls] == [
        "https://api.hyperliquid.xyz/info",
        "https://api.hyperliquid.xyz/info",
    ]
    assert [request.method for request, _ in opener.calls] == ["POST", "POST"]
    assert [timeout for _, timeout in opener.calls] == [7.0, 7.0]
    assert b'"type":"metaAndAssetCtxs"' in opener.calls[0][0].data
    assert b'"type":"spotMetaAndAssetCtxs"' in opener.calls[1][0].data
    assert len(redirect_handlers) == 1
    redirect_handler = redirect_handlers[0]
    assert redirect_handler.__class__.__name__ == "_NoRedirectHandler"
    assert (
        redirect_handler.redirect_request(
            opener.calls[0][0],
            None,
            302,
            "Found",
            {},
            "https://redirect.invalid/info",
        )
        is None
    )

    project = Path(__file__).resolve().parents[1] / "pyproject.toml"
    assert "hyperliquid-python-sdk" not in project.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("response", "error"),
    [
        (
            _FakeHttpResponse(
                b"{}",
                final_url="https://redirect.invalid/info",
            ),
            "response URL differs",
        ),
        (_FakeHttpResponse(b"{}", status=204), "exact HTTP 200"),
    ],
)
def test_default_transport_rejects_unbound_final_url_or_status(
    monkeypatch: pytest.MonkeyPatch,
    response: _FakeHttpResponse,
    error: str,
) -> None:
    opener = _FakeOpener(iter((response,)))
    monkeypatch.setattr(
        "hyperlab.api.public.urllib.request.build_opener",
        lambda *_handlers: opener,
    )

    client = HyperliquidPublicClient(network="mainnet", timeout_seconds=7.0)
    with pytest.raises(RuntimeError, match=error):
        client.bootstrap(observed_at_ms=123)


def _perp_context(*, mid: str, volume: str) -> dict[str, str]:
    return {
        "dayNtlVlm": volume,
        "funding": "0.00001",
        "markPx": mid,
        "midPx": mid,
        "openInterest": "2",
    }


def _spot_context(*, coin: str, mid: str, volume: str) -> dict[str, str]:
    return {
        "coin": coin,
        "dayNtlVlm": volume,
        "markPx": mid,
        "midPx": mid,
    }


def test_spot_contexts_are_joined_by_coin_across_index_gaps_and_reordering() -> None:
    perp_payload = [
        {"universe": [{"name": "HYPE"}, {"name": "STABLE"}]},
        [
            _perp_context(mid="55.1", volume="1000"),
            _perp_context(mid="0.0324", volume="2000"),
        ],
    ]
    spot_payload = [
        {
            "tokens": [
                {"index": 0, "name": "USDC", "tokenId": "quote"},
                {
                    "index": 150,
                    "name": "REMAPPED-HYPE",
                    "tokenId": "0x0d01dc56dcaaca66ad901c959b4011ec",
                },
                {
                    "index": 398,
                    "name": "STABLE",
                    "tokenId": "0xec43194f64d555bdaef5afb5b6c6c686",
                },
            ],
            "universe": [
                {"index": 107, "name": "@107", "tokens": [150, 0]},
                {"index": 258, "name": "@258", "tokens": [398, 0]},
            ],
        },
        [
            _spot_context(coin="@258", mid="0.0323", volume="222"),
            _spot_context(coin="@107", mid="55.0", volume="111"),
        ],
    ]

    snapshots = parse_carry_markets(perp_payload, spot_payload, observed_at_ms=123)

    assert [(row.asset, row.spot_mid, row.spot_volume_usd) for row in snapshots] == [
        ("HYPE", Decimal("55.0"), Decimal("111")),
        ("STABLE", Decimal("0.0323"), Decimal("222")),
    ]
    assert [row.observed_at_ms for row in snapshots] == [123, 123]


def test_mainnet_carry_identity_regression_uses_token_id_not_shared_ticker() -> None:
    identities = (
        ("AZTEC", "@285", 285, 442, "0x32b15e526a6136d7215cbbbfa924afc7", None, False),
        ("BERA", "@117", 117, 80, "0x0b6ae68f39bfd088744374daa99db226", None, False),
        ("HYPE", "@107", 107, 150, "0x0d01dc56dcaaca66ad901c959b4011ec", "Hyperliquid", False),
        ("MON", "@129", 129, 164, "0x622cf551933f19f9136303dcab56488c", "MON", False),
        ("PUMP", "@20", 20, 26, "0xefa7e286b99ea49ce6a21d21bb41636f", None, False),
        ("PURR", "PURR/USDC", 0, 1, "0xc1fb593aeffbeb02f85e0308e9956a90", None, True),
        ("STABLE", "@258", 258, 398, "0xec43194f64d555bdaef5afb5b6c6c686", "Stable", False),
        ("TRUMP", "@9", 9, 10, "0x368cb581f0d51e21aa19996d38ffdf6f", None, False),
    )
    perp_payload = [
        {"universe": [{"name": symbol} for symbol, *_rest in identities]},
        [_perp_context(mid="1", volume="1000") for _identity in identities],
    ]
    spot_payload = [
        {
            "tokens": [
                {"index": 0, "name": "USDC", "tokenId": "quote", "isCanonical": True},
                *[
                    {
                        "index": token_index,
                        "name": symbol,
                        "tokenId": token_id,
                        "fullName": full_name,
                        "isCanonical": is_canonical,
                    }
                    for (
                        symbol,
                        _pair_name,
                        _pair_index,
                        token_index,
                        token_id,
                        full_name,
                        is_canonical,
                    ) in identities
                ],
            ],
            "universe": [
                {
                    "index": pair_index,
                    "name": pair_name,
                    "tokens": [token_index, 0],
                    "isCanonical": is_canonical,
                }
                for (
                    _symbol,
                    pair_name,
                    pair_index,
                    token_index,
                    _token_id,
                    _full_name,
                    is_canonical,
                ) in identities
            ],
        },
        [
            _spot_context(
                coin=pair_name if pair_name == "PURR/USDC" else f"@{pair_index}",
                mid="1",
                volume="100",
            )
            for _symbol, pair_name, pair_index, *_rest in identities
        ],
    ]

    snapshots = parse_carry_markets(perp_payload, spot_payload, observed_at_ms=123)

    assert [row.asset for row in snapshots] == ["AZTEC", "HYPE", "PURR", "STABLE"]


def test_funding_history_paginates_inclusively_without_duplicates() -> None:
    first_page = [
        {"coin": "BTC", "fundingRate": "0.00001", "premium": "0", "time": timestamp}
        for timestamp in range(1_000, 1_500)
    ]
    final_page = [{"coin": "BTC", "fundingRate": "0.00002", "premium": "0", "time": 1_500}]

    def respond(payload: Mapping[str, Any]) -> object:
        assert payload["type"] == "fundingHistory"
        return first_page if payload["startTime"] == 1_000 else final_page

    info = FakeInfo(respond)
    client = HyperliquidPublicClient(info=info)

    rows = client.funding_history("BTC", 1_000, 1_500)

    assert len(rows) == 501
    assert [rows[0]["time"], rows[-1]["time"]] == [1_000, 1_500]
    assert info.posts == [
        (
            "/info",
            {
                "type": "fundingHistory",
                "coin": "BTC",
                "startTime": 1_000,
                "endTime": 1_500,
            },
        ),
        (
            "/info",
            {
                "type": "fundingHistory",
                "coin": "BTC",
                "startTime": 1_500,
                "endTime": 1_500,
            },
        ),
    ]


def test_candles_paginate_at_the_official_five_thousand_row_limit() -> None:
    first_page = [{"s": "BTC", "i": "1m", "t": timestamp} for timestamp in range(10_001, 15_001)]
    final_page = [{"s": "BTC", "i": "1m", "t": 10_000}]

    def respond(payload: Mapping[str, Any]) -> object:
        assert payload["type"] == "candleSnapshot"
        request = payload["req"]
        assert isinstance(request, Mapping)
        return first_page if request["endTime"] == 15_000 else final_page

    info = FakeInfo(respond)
    client = HyperliquidPublicClient(info=info)

    rows = client.candles("BTC", "1m", 10_000, 15_000)

    assert len(rows) == 5_001
    assert [rows[0]["t"], rows[-1]["t"]] == [10_000, 15_000]
    requests = [post[1]["req"] for post in info.posts]
    assert requests == [
        {
            "coin": "BTC",
            "interval": "1m",
            "startTime": 10_000,
            "endTime": 15_000,
        },
        {
            "coin": "BTC",
            "interval": "1m",
            "startTime": 10_000,
            "endTime": 10_000,
        },
    ]


def test_bootstrap_uses_only_public_info_payloads_without_identity_fields() -> None:
    responses = {
        "metaAndAssetCtxs": [{"universe": []}, []],
        "spotMetaAndAssetCtxs": [{"tokens": [], "universe": []}, []],
    }
    info = FakeInfo(lambda payload: responses[str(payload["type"])])

    result = HyperliquidPublicClient(info=info).bootstrap(observed_at_ms=456)

    assert result.observed_at_ms == 456
    assert [payload for _, payload in info.posts] == [
        {"type": "metaAndAssetCtxs"},
        {"type": "spotMetaAndAssetCtxs"},
    ]
    assert all(set(payload) == {"type"} for _, payload in info.posts)


def test_cancel_between_bootstrap_requests_prevents_the_second_call() -> None:
    holder: dict[str, HyperliquidPublicClient] = {}

    def respond(payload: Mapping[str, Any]) -> object:
        assert payload["type"] == "metaAndAssetCtxs"
        holder["client"].cancel()
        return [{"universe": []}, []]

    info = FakeInfo(respond)
    client = HyperliquidPublicClient(info=info)
    holder["client"] = client

    with pytest.raises(InterruptedError, match="cancelled"):
        client.bootstrap()

    assert [payload for _, payload in info.posts] == [
        {"type": "metaAndAssetCtxs"},
    ]
