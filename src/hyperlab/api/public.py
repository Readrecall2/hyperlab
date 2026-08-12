from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

_FUNDING_PAGE_SIZE = 500
_CANDLE_PAGE_SIZE = 5_000
_VALID_CANDLE_INTERVALS = frozenset(
    {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d", "3d", "1w", "1M"}
)


class PublicInfo(Protocol):
    """Narrow public surface used from the pinned official SDK."""

    def post(self, url_path: str, payload: Any = None) -> Any: ...


@dataclass(frozen=True, slots=True)
class PublicBootstrap:
    observed_at_ms: int
    perp_payload: Sequence[Any]
    spot_payload: Sequence[Any]


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


def _decimal(value: object) -> Decimal:
    if value is None or value == "":
        raise ValueError("missing numeric value")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid numeric value: {value!r}") from exc


def _payload_pair(payload: Sequence[Any], *, label: str) -> tuple[Mapping[str, Any], list[Any]]:
    if len(payload) != 2:
        raise ValueError(f"unexpected Hyperliquid {label} payload")
    metadata, contexts = payload
    if not isinstance(metadata, Mapping) or not isinstance(contexts, list):
        raise ValueError(f"Hyperliquid {label} payload must contain metadata and contexts")
    return metadata, contexts


def _mapping_list(value: object, *, label: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"Hyperliquid {label} must be a list of objects")
    return value


def hyperliquid_spot_coin(pair: Mapping[str, Any]) -> str:
    """Return the exact API coin for one spot-universe entry."""

    name = str(pair.get("name", ""))
    if name == "PURR/USDC":
        return name
    if "index" not in pair:
        return name
    return f"@{int(pair['index'])}"


def parse_carry_markets(
    perp_payload: Sequence[Any],
    spot_payload: Sequence[Any],
    *,
    observed_at_ms: int | None = None,
) -> list[CarrySnapshot]:
    perp_meta, raw_perp_contexts = _payload_pair(perp_payload, label="perp metadata")
    spot_meta, raw_spot_contexts = _payload_pair(spot_payload, label="spot metadata")
    perp_universe = _mapping_list(perp_meta.get("universe"), label="perp universe")
    perp_contexts = _mapping_list(raw_perp_contexts, label="perp contexts")
    if len(perp_universe) != len(perp_contexts):
        raise ValueError("perp metadata and contexts are not aligned")
    spot_universe = _mapping_list(spot_meta.get("universe"), label="spot universe")
    spot_contexts = _mapping_list(raw_spot_contexts, label="spot contexts")

    token_by_index: dict[int, Mapping[str, Any]] = {}
    for token in _mapping_list(spot_meta.get("tokens"), label="spot tokens"):
        if isinstance(token, Mapping) and "index" in token:
            token_by_index[int(token["index"])] = token

    # Spot contexts are keyed by their API coin. They are not positionally aligned
    # with spotMeta.universe: delistings leave gaps in universe indices.
    spot_context_by_coin: dict[str, Mapping[str, Any]] = {}
    for context in spot_contexts:
        coin = context.get("coin")
        if isinstance(coin, str) and coin:
            spot_context_by_coin[coin] = context

    spot_by_asset: dict[str, tuple[str, Mapping[str, Any]]] = {}
    for position, pair in enumerate(spot_universe):
        token_indexes = pair.get("tokens")
        if not isinstance(token_indexes, Sequence) or len(token_indexes) != 2:
            continue
        base = token_by_index.get(int(token_indexes[0]))
        quote = token_by_index.get(int(token_indexes[1]))
        if not base or not quote or str(quote.get("name")) != "USDC":
            continue
        asset = str(base.get("name", ""))
        source_coin = hyperliquid_spot_coin(pair)
        spot_context = spot_context_by_coin.get(source_coin)
        if spot_context is None and isinstance(pair.get("index"), int):
            pair_index = int(pair["index"])
            if pair_index < len(spot_contexts):
                candidate = spot_contexts[pair_index]
                if candidate.get("coin") == source_coin:
                    spot_context = candidate
        if spot_context is None and not spot_context_by_coin and len(spot_universe) == len(spot_contexts):
            spot_context = spot_contexts[position]
        if asset and spot_context is not None:
            spot_by_asset[asset] = (str(pair.get("name") or f"{asset}/USDC"), spot_context)

    timestamp = observed_at_ms if observed_at_ms is not None else int(time.time() * 1000)
    snapshots: list[CarrySnapshot] = []
    for meta, context in zip(perp_universe, perp_contexts, strict=True):
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


class WeightedMinuteLimiter:
    """Bound public Info weight inside Hyperliquid's rolling per-IP budget."""

    def __init__(
        self,
        *,
        capacity: int = 1_200,
        window_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if capacity <= 0 or window_seconds <= 0:
            raise ValueError("rate limit capacity and window must be positive")
        self.capacity = capacity
        self.window_seconds = window_seconds
        self.clock = clock
        self.sleeper = sleeper
        self._events: deque[tuple[float, int]] = deque()
        self._used = 0

    def acquire(
        self,
        weight: int,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> None:
        if weight <= 0 or weight > self.capacity:
            raise ValueError("request weight must be within the limiter capacity")
        while True:
            if should_stop is not None and should_stop():
                raise InterruptedError("public REST rate-limit wait cancelled")
            now = self.clock()
            while self._events and now - self._events[0][0] >= self.window_seconds:
                _, expired_weight = self._events.popleft()
                self._used -= expired_weight
            if self._used + weight <= self.capacity:
                self._events.append((now, weight))
                self._used += weight
                return
            delay = max(
                self.window_seconds - (now - self._events[0][0]),
                0.001,
            )
            self.sleeper(min(delay, 0.25))

    """Read-only wrapper around the official Hyperliquid Python SDK."""


class HyperliquidPublicClient:
    def __init__(
        self,
        *,
        network: str = "mainnet",
        timeout_seconds: float = 15.0,
        info: PublicInfo | None = None,
        rate_limiter: WeightedMinuteLimiter | None = None,
    ) -> None:
        if network not in {"mainnet", "testnet"}:
            raise ValueError(f"unsupported network: {network}")
        self.network = network
        self._rate_limiter = rate_limiter or WeightedMinuteLimiter()
        self._cancelled = threading.Event()
        if info is not None:
            self.info = info
            return
        try:
            from hyperliquid.info import Info
            from hyperliquid.utils import constants
        except ImportError as exc:
            raise RuntimeError("Install the project dependencies before calling the API") from exc

        base_url = constants.MAINNET_API_URL if network == "mainnet" else constants.TESTNET_API_URL
        # Info 0.24.0 otherwise performs implicit spotMeta and meta requests in
        # __init__. Empty metadata makes every network request explicit.
        self.info = Info(
            base_url,
            skip_ws=True,
            timeout=timeout_seconds,
            meta={"universe": []},
            spot_meta={"tokens": [], "universe": []},
        )

    def __enter__(self) -> HyperliquidPublicClient:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        session = getattr(self.info, "session", None)
        close = getattr(session, "close", None)
        if callable(close):
            close()

    def cancel(self) -> None:
        self._cancelled.set()

    def _post(self, payload: Mapping[str, object], *, weight: int = 20) -> Any:
        if self._cancelled.is_set():
            raise InterruptedError("public REST collection cancelled")
        self._rate_limiter.acquire(weight, should_stop=self._cancelled.is_set)
        if self._cancelled.is_set():
            raise InterruptedError("public REST collection cancelled")
        response = self.info.post("/info", dict(payload))
        if self._cancelled.is_set():
            raise InterruptedError("public REST collection cancelled")
        if isinstance(response, Mapping) and set(response) == {"error"}:
            raise RuntimeError(f"Hyperliquid Info API returned invalid JSON: {response['error']}")
        return response

    def bootstrap(self, *, observed_at_ms: int | None = None) -> PublicBootstrap:
        perp = self._post({"type": "metaAndAssetCtxs"})
        spot = self._post({"type": "spotMetaAndAssetCtxs"})
        if not isinstance(perp, list) or not isinstance(spot, list):
            raise ValueError("unexpected Hyperliquid bootstrap payload")
        _payload_pair(perp, label="perp metadata")
        _payload_pair(spot, label="spot metadata")
        return PublicBootstrap(
            observed_at_ms=(observed_at_ms if observed_at_ms is not None else int(time.time() * 1000)),
            perp_payload=perp,
            spot_payload=spot,
        )

    def carry_snapshot(self) -> list[CarrySnapshot]:
        bootstrap = self.bootstrap()
        return parse_carry_markets(
            bootstrap.perp_payload,
            bootstrap.spot_payload,
            observed_at_ms=bootstrap.observed_at_ms,
        )

    def funding_history(self, asset: str, start_ms: int, end_ms: int | None = None) -> Any:
        if start_ms < 0 or (end_ms is not None and end_ms < start_ms):
            raise ValueError("invalid funding history time range")
        cursor = start_ms
        unique: dict[tuple[str, int], Mapping[str, Any]] = {}
        while True:
            payload: dict[str, object] = {
                "type": "fundingHistory",
                "coin": asset,
                "startTime": cursor,
            }
            if end_ms is not None:
                payload["endTime"] = end_ms
            response = self._post(payload, weight=45)
            page = _mapping_list(response, label="funding history")
            for item in page:
                timestamp = int(item["time"])
                if timestamp >= start_ms and (end_ms is None or timestamp <= end_ms):
                    unique[(str(item.get("coin", asset)), timestamp)] = item
            if len(page) < _FUNDING_PAGE_SIZE or not page:
                break
            last_time = max(int(item["time"]) for item in page)
            if end_ms is not None and last_time >= end_ms:
                break
            next_cursor = last_time + 1
            if next_cursor <= cursor:
                raise RuntimeError("funding history pagination made no progress")
            cursor = next_cursor
        return [unique[key] for key in sorted(unique, key=lambda item: (item[1], item[0]))]

    def funding_history_pages(
        self,
        asset: str,
        start_ms: int,
        end_ms: int | None = None,
    ) -> Iterator[list[Mapping[str, Any]]]:
        if start_ms < 0 or (end_ms is not None and end_ms < start_ms):
            raise ValueError("invalid funding history time range")
        cursor = start_ms
        while True:
            payload: dict[str, object] = {"type": "fundingHistory", "coin": asset, "startTime": cursor}
            if end_ms is not None:
                payload["endTime"] = end_ms
            page = _mapping_list(self._post(payload, weight=45), label="funding history")
            selected = [
                item
                for item in page
                if int(item["time"]) >= start_ms and (end_ms is None or int(item["time"]) <= end_ms)
            ]
            yield selected
            if len(page) < _FUNDING_PAGE_SIZE or not page:
                break
            last_time = max(int(item["time"]) for item in page)
            if end_ms is not None and last_time >= end_ms:
                break
            next_cursor = last_time + 1
            if next_cursor <= cursor:
                raise RuntimeError("funding history pagination made no progress")
            cursor = next_cursor

    def candles(self, asset: str, interval: str, start_ms: int, end_ms: int) -> Any:
        if interval not in _VALID_CANDLE_INTERVALS:
            raise ValueError(f"unsupported candle interval: {interval}")
        if start_ms < 0 or end_ms < start_ms:
            raise ValueError("invalid candle time range")
        cursor_end = end_ms
        unique: dict[tuple[str, str, int], Mapping[str, Any]] = {}
        while True:
            response = self._post(
                {
                    "type": "candleSnapshot",
                    "req": {
                        "coin": asset,
                        "interval": interval,
                        "startTime": start_ms,
                        "endTime": cursor_end,
                    },
                },
                weight=104,
            )
            page = _mapping_list(response, label="candle snapshot")
            for item in page:
                open_time = int(item["t"])
                if start_ms <= open_time <= end_ms:
                    key = (str(item.get("s", asset)), str(item.get("i", interval)), open_time)
                    unique[key] = item
            if len(page) < _CANDLE_PAGE_SIZE or not page:
                break
            first_time = min(int(item["t"]) for item in page)
            if first_time <= start_ms:
                break
            next_end = first_time - 1
            if next_end >= cursor_end:
                raise RuntimeError("candle pagination made no progress")
            cursor_end = next_end
        return [unique[key] for key in sorted(unique, key=lambda item: (item[2], item[0], item[1]))]

    def candle_pages(
        self,
        asset: str,
        interval: str,
        start_ms: int,
        end_ms: int,
    ) -> Iterator[list[Mapping[str, Any]]]:
        if interval not in _VALID_CANDLE_INTERVALS:
            raise ValueError(f"unsupported candle interval: {interval}")
        if start_ms < 0 or end_ms < start_ms:
            raise ValueError("invalid candle time range")
        cursor_end = end_ms
        while True:
            page = _mapping_list(
                self._post(
                    {
                        "type": "candleSnapshot",
                        "req": {
                            "coin": asset,
                            "interval": interval,
                            "startTime": start_ms,
                            "endTime": cursor_end,
                        },
                    },
                    weight=104,
                ),
                label="candle snapshot",
            )
            selected = [item for item in page if start_ms <= int(item["t"]) <= end_ms]
            yield selected
            if len(page) < _CANDLE_PAGE_SIZE or not page:
                break
            first_time = min(int(item["t"]) for item in page)
            if first_time <= start_ms:
                break
            next_end = first_time - 1
            if next_end >= cursor_end:
                raise RuntimeError("candle pagination made no progress")
            cursor_end = next_end

    def l2_snapshot(
        self,
        asset: str,
        *,
        n_sig_figs: int | None = None,
        mantissa: int | None = None,
    ) -> Any:
        if n_sig_figs is not None and n_sig_figs not in {2, 3, 4, 5}:
            raise ValueError("n_sig_figs must be one of 2, 3, 4, 5, or None")
        if mantissa is not None and (n_sig_figs != 5 or mantissa not in {1, 2, 5}):
            raise ValueError("mantissa requires n_sig_figs=5 and must be one of 1, 2, 5")
        payload: dict[str, object] = {"type": "l2Book", "coin": asset}
        if n_sig_figs is not None:
            payload["nSigFigs"] = n_sig_figs
        if mantissa is not None:
            payload["mantissa"] = mantissa
        response = self._post(payload, weight=2)
        if not isinstance(response, Mapping):
            raise ValueError("unexpected Hyperliquid L2 payload")
        return response

    def all_mids(self, *, dex: str = "") -> Mapping[str, Any]:
        response = self._post({"type": "allMids", "dex": dex}, weight=2)
        if not isinstance(response, Mapping):
            raise ValueError("unexpected Hyperliquid allMids payload")
        return response
