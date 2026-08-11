from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any


@dataclass(frozen=True, slots=True)
class CarrySnapshot:
    observed_at_ms: int
    asset: str
    spot_pair: str
    spot_mid: Decimal
    perp_mid: Decimal
    funding_hourly: Decimal
    basis_bps: Decimal
    perp_volume_usd: Decimal
    spot_volume_usd: Decimal
    open_interest: Decimal

    def serializable(self) -> dict[str, str | int]:
        raw = asdict(self)
        return {key: str(value) if isinstance(value, Decimal) else value for key, value in raw.items()}


def _decimal(value: object, default: str = "0") -> Decimal:
    if value is None or value == "":
        value = default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid numeric value: {value!r}") from exc


def parse_carry_markets(
    perp_payload: Sequence[Any],
    spot_payload: Sequence[Any],
    *,
    observed_at_ms: int | None = None,
) -> list[CarrySnapshot]:
    if len(perp_payload) != 2 or len(spot_payload) != 2:
        raise ValueError("unexpected Hyperliquid metadata payload")
    perp_meta, perp_contexts = perp_payload
    spot_meta, spot_contexts = spot_payload
    if not isinstance(perp_meta, Mapping) or not isinstance(spot_meta, Mapping):
        raise ValueError("metadata payload must be an object")

    token_by_index: dict[int, Mapping[str, Any]] = {}
    for token in spot_meta.get("tokens", []):
        if isinstance(token, Mapping) and "index" in token:
            token_by_index[int(token["index"])] = token

    spot_by_asset: dict[str, tuple[str, Mapping[str, Any]]] = {}
    for pair, context in zip(spot_meta.get("universe", []), spot_contexts, strict=False):
        if not isinstance(pair, Mapping) or not isinstance(context, Mapping):
            continue
        token_indexes = pair.get("tokens")
        if not isinstance(token_indexes, Sequence) or len(token_indexes) != 2:
            continue
        base = token_by_index.get(int(token_indexes[0]))
        quote = token_by_index.get(int(token_indexes[1]))
        if not base or not quote or str(quote.get("name")) != "USDC":
            continue
        asset = str(base.get("name", ""))
        if asset:
            spot_by_asset[asset] = (f"{asset}/USDC", context)

    timestamp = observed_at_ms if observed_at_ms is not None else int(time.time() * 1000)
    snapshots: list[CarrySnapshot] = []
    for meta, context in zip(perp_meta.get("universe", []), perp_contexts, strict=False):
        if not isinstance(meta, Mapping) or not isinstance(context, Mapping):
            continue
        asset = str(meta.get("name", ""))
        spot_match = spot_by_asset.get(asset)
        if not asset or spot_match is None:
            continue
        spot_pair, spot_context = spot_match
        spot_mid = _decimal(spot_context.get("midPx") or spot_context.get("markPx"))
        perp_mid = _decimal(context.get("midPx") or context.get("markPx"))
        funding = _decimal(context.get("funding"))
        if spot_mid <= 0 or perp_mid <= 0:
            continue
        basis_bps = (perp_mid / spot_mid - Decimal("1")) * Decimal("10000")
        snapshots.append(
            CarrySnapshot(
                observed_at_ms=timestamp,
                asset=asset,
                spot_pair=spot_pair,
                spot_mid=spot_mid,
                perp_mid=perp_mid,
                funding_hourly=funding,
                basis_bps=basis_bps,
                perp_volume_usd=_decimal(context.get("dayNtlVlm")),
                spot_volume_usd=_decimal(spot_context.get("dayNtlVlm")),
                open_interest=_decimal(context.get("openInterest")),
            )
        )
    return sorted(snapshots, key=lambda item: item.asset)


class HyperliquidPublicClient:
    """Read-only wrapper around the official Hyperliquid Python SDK."""

    def __init__(self, *, network: str = "mainnet", timeout_seconds: float = 15.0) -> None:
        try:
            from hyperliquid.info import Info
            from hyperliquid.utils import constants
        except ImportError as exc:
            raise RuntimeError("Install the project dependencies before calling the API") from exc

        if network == "mainnet":
            base_url = constants.MAINNET_API_URL
        elif network == "testnet":
            base_url = constants.TESTNET_API_URL
        else:
            raise ValueError(f"unsupported network: {network}")
        self.info = Info(base_url, skip_ws=True, timeout=timeout_seconds)

    def carry_snapshot(self) -> list[CarrySnapshot]:
        return parse_carry_markets(
            self.info.meta_and_asset_ctxs(),
            self.info.spot_meta_and_asset_ctxs(),
        )

    def funding_history(self, asset: str, start_ms: int, end_ms: int | None = None) -> Any:
        return self.info.funding_history(asset, start_ms, end_ms)

    def candles(self, asset: str, interval: str, start_ms: int, end_ms: int) -> Any:
        return self.info.candles_snapshot(asset, interval, start_ms, end_ms)

    def l2_snapshot(self, asset: str) -> Any:
        return self.info.l2_snapshot(asset)
