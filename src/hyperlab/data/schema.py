from __future__ import annotations


def instrument(exchange: str, asset: str, kind: str) -> str:
    if kind not in {"spot", "perp"}:
        raise ValueError(f"unsupported instrument kind: {kind}")
    return f"{exchange.upper()}:{asset.upper()}:{kind}"


def parse_instrument(value: str) -> tuple[str, str, str]:
    parts = value.split(":")
    if len(parts) != 3:
        raise ValueError(f"invalid instrument id: {value}")
    exchange, asset, kind = parts
    if kind not in {"spot", "perp"}:
        raise ValueError(f"invalid instrument kind: {kind}")
    return exchange, asset, kind
