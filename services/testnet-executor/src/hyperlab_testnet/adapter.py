"""Strict raw-HTTP Hyperliquid Testnet adapter.

Only the official signing primitives are reused.  The SDK ``Exchange`` client is
intentionally absent because its optional base URL defaults to Mainnet.  Every
request produced here is bound to the single compiled Testnet origin and to one
of the two allowlisted paths.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import Any, NoReturn, Protocol, cast

import requests
from hyperliquid.utils.signing import (
    order_request_to_order_wire,
    order_wires_to_order_action,
    sign_l1_action,
)
from hyperliquid.utils.types import Cloid

from hyperlab_testnet.canonical import decimal_value
from hyperlab_testnet.models import OrderSide, TestnetOrderIntent, TimeInForce, validate_cloid

TESTNET_API_ORIGIN = "https://api.hyperliquid-testnet.xyz"
TESTNET_WEBSOCKET_URL = "wss://api.hyperliquid-testnet.xyz/ws"
_EXCHANGE_PATH = "/exchange"
_INFO_PATH = "/info"
_ALLOWED_PATHS = frozenset({_EXCHANGE_PATH, _INFO_PATH})
_ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}\Z")
_CLOID_RE = re.compile(r"0x[0-9a-f]{32}\Z")
_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
_TIF_WIRE = MappingProxyType(
    {
        TimeInForce.ALO: "Alo",
        TimeInForce.GTC: "Gtc",
        TimeInForce.IOC: "Ioc",
    }
)
_READ_TYPES = frozenset(
    {
        "clearinghouseState",
        "extraAgents",
        "allMids",
        "meta",
        "openOrders",
        "orderStatus",
        "spotClearinghouseState",
        "userFills",
        "userFillsByTime",
        "userRole",
    }
)


class AdapterError(RuntimeError):
    """Base class whose messages never include venue payloads or signatures."""


class EndpointIsolationError(AdapterError):
    pass


class ReadTransportError(AdapterError):
    pass


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate venue JSON key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    del value
    raise ValueError("non-finite venue JSON number")


def _strict_json_loads(encoded: bytes) -> object:
    return json.loads(
        encoded,
        object_pairs_hook=_strict_json_object,
        parse_constant=_reject_json_constant,
    )


class OutcomeKind(StrEnum):
    RESTING = "RESTING"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    REPLACED = "REPLACED"
    DEADMAN_ARMED = "DEADMAN_ARMED"
    AMBIGUOUS = "AMBIGUOUS"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class ActionOutcome:
    """Redacted semantic result; raw signed material is never retained."""

    kind: OutcomeKind
    code: str
    venue_order_id: str | None = None
    filled_quantity: Decimal | None = None
    average_fill_price: Decimal | None = None

    @property
    def ambiguous(self) -> bool:
        return self.kind in {OutcomeKind.AMBIGUOUS, OutcomeKind.UNKNOWN}


@dataclass(frozen=True, slots=True)
class VerifiedExtraAgent:
    name: str
    address: str
    valid_until_ms: int


@dataclass(frozen=True, slots=True)
class PerpAssetConstraints:
    coin: str
    asset: int
    size_decimals: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.coin, str)
            or not self.coin
            or self.coin != self.coin.strip()
            or ":" in self.coin
        ):
            raise ValueError("perp constraint coin is invalid")
        if isinstance(self.asset, bool) or not isinstance(self.asset, int) or self.asset < 0:
            raise ValueError("perp constraint asset id is invalid")
        if (
            isinstance(self.size_decimals, bool)
            or not isinstance(self.size_decimals, int)
            or not 0 <= self.size_decimals <= 6
        ):
            raise ValueError("perp size_decimals must be between 0 and 6")


@dataclass(frozen=True, slots=True)
class HttpResult:
    status_code: int
    payload: object


class JsonHttpTransport(Protocol):
    def post_json(
        self,
        *,
        url: str,
        payload: Mapping[str, object],
        timeout_seconds: float,
    ) -> HttpResult: ...


class SigningAccount(Protocol):
    address: str

    def sign_message(self, signable_message: object) -> object: ...


class TestnetSigner:
    """Non-serializable wrapper around an eth-account LocalAccount."""

    __slots__ = ("_account", "address")

    def __init__(self, account: object, address: str) -> None:
        self._account = account
        self.address = _address(address, label="signer address")

    def sign_message(self, signable_message: object) -> object:
        method = getattr(self._account, "sign_message", None)
        if not callable(method):
            raise TypeError("credential did not construct a signing account")
        return method(signable_message)

    def sign_typed_data(self, *args: object, **kwargs: object) -> object:
        method = getattr(self._account, "sign_typed_data", None)
        if not callable(method):
            raise TypeError("credential did not construct a signing account")
        return method(*args, **kwargs)

    def __repr__(self) -> str:
        return "TestnetSigner(<redacted>)"

    __str__ = __repr__

    def __reduce__(self) -> NoReturn:
        raise TypeError("TestnetSigner is deliberately non-serializable")


def testnet_signer_from_secret(secret: str | bytes) -> TestnetSigner:
    """Construct the only secret-bearing object; callers must discard input promptly."""

    if not isinstance(secret, (str, bytes)) or not secret:
        raise ValueError("a non-empty dedicated Testnet credential is required")
    # Keep eth-account out of every other module and code path.
    from eth_account import Account

    try:
        account = Account.from_key(secret)
    except Exception:
        raise ValueError("invalid dedicated Testnet credential") from None
    return TestnetSigner(account, str(account.address))


class RequestsJsonTransport:
    """Small no-redirect/no-env-proxy HTTP transport for the compiled origin."""

    __slots__ = ("_session",)

    def __init__(self) -> None:
        session = requests.Session()
        session.trust_env = False
        self._session = session

    def post_json(
        self,
        *,
        url: str,
        payload: Mapping[str, object],
        timeout_seconds: float,
    ) -> HttpResult:
        _require_exact_url(url)
        response: requests.Response | None = None
        try:
            response = self._session.post(
                url,
                json=cast(Any, dict(payload)),
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": "HyperLab-Testnet-Executor/0.3.0.dev0",
                },
                timeout=timeout_seconds,
                allow_redirects=False,
                stream=True,
            )
        except requests.RequestException as error:
            raise ConnectionError(type(error).__name__) from None
        try:
            if response.is_redirect or response.url != url:
                raise EndpointIsolationError("redirected or replaced Testnet endpoint refused")
            raw_length = response.headers.get("Content-Length")
            if raw_length is not None:
                if not raw_length.isdigit():
                    raise ReadTransportError("venue Content-Length is invalid")
                if int(raw_length) > _MAX_RESPONSE_BYTES:
                    raise ReadTransportError("venue response exceeds the compiled size limit")
            buffer = bytearray()
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not isinstance(chunk, bytes):
                    raise ReadTransportError("venue response chunk is not bytes")
                buffer.extend(chunk)
                if len(buffer) > _MAX_RESPONSE_BYTES:
                    raise ReadTransportError("venue response exceeds the compiled size limit")
            encoded = bytes(buffer)
        except requests.RequestException as error:
            raise ConnectionError(type(error).__name__) from None
        finally:
            response.close()
        try:
            decoded = _strict_json_loads(encoded)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            decoded = None
        return HttpResult(status_code=int(response.status_code), payload=decoded)


def _address(value: str, *, label: str) -> str:
    if not isinstance(value, str) or _ADDRESS_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be an exact 20-byte hexadecimal address")
    return value.lower()


def _require_exact_origin(origin: str) -> None:
    if origin != TESTNET_API_ORIGIN:
        raise EndpointIsolationError("only the compiled Hyperliquid Testnet origin is allowed")


def _require_exact_url(url: str) -> None:
    if not isinstance(url, str):
        raise EndpointIsolationError("venue URL must be an exact string")
    for path in _ALLOWED_PATHS:
        if url == TESTNET_API_ORIGIN + path:
            return
    raise EndpointIsolationError("venue URL is not an allowlisted Testnet endpoint/path")


def _timeout_seconds(value: float) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TypeError("timeout_seconds must be numeric")
    normalized = float(value)
    if not 0 < normalized <= 60:
        raise ValueError("timeout_seconds must be in (0, 60]")
    return normalized


def read_testnet_meta(
    *,
    origin: str,
    http: JsonHttpTransport,
    timeout_seconds: float,
) -> object:
    """Bootstrap the Testnet perp universe before constructing a signed adapter."""

    _require_exact_origin(origin)
    timeout = _timeout_seconds(timeout_seconds)
    try:
        result = http.post_json(
            url=origin + _INFO_PATH,
            payload={"type": "meta"},
            timeout_seconds=timeout,
        )
    except Exception as error:
        raise ReadTransportError(f"Testnet meta read failed ({type(error).__name__})") from None
    if result.status_code != 200 or result.payload is None:
        raise ReadTransportError("Testnet meta read did not return valid JSON with HTTP 200")
    return result.payload


def perp_constraints_from_meta(meta: object) -> Mapping[str, PerpAssetConstraints]:
    """Strictly derive and freeze perp ids and precision from universe order."""

    if not isinstance(meta, Mapping):
        raise ValueError("Testnet meta must be a JSON object")
    universe = meta.get("universe")
    if not isinstance(universe, list) or not universe:
        raise ValueError("Testnet meta universe must be a non-empty array")
    constraints: dict[str, PerpAssetConstraints] = {}
    for asset, entry in enumerate(universe):
        if not isinstance(entry, Mapping):
            raise ValueError("Testnet meta universe entries must be JSON objects")
        name = entry.get("name")
        if (
            not isinstance(name, str)
            or not name
            or name != name.strip()
            or ":" in name
            or any(ord(character) < 33 or ord(character) > 126 for character in name)
        ):
            raise ValueError("Testnet meta contains an invalid perp name")
        size_decimals = entry.get("szDecimals")
        if name in constraints:
            raise ValueError("Testnet meta contains duplicate perp names")
        if isinstance(size_decimals, bool) or not isinstance(size_decimals, int):
            raise ValueError("Testnet meta contains an invalid szDecimals")
        constraints[name] = PerpAssetConstraints(name, asset, size_decimals)
    return MappingProxyType(constraints)


def asset_map_from_meta(meta: object) -> Mapping[str, int]:
    """Compatibility view of the strict frozen Testnet perp constraints."""

    constraints = perp_constraints_from_meta(meta)
    return MappingProxyType({coin: constraint.asset for coin, constraint in constraints.items()})


def verify_extra_agent_scope(
    payload: object,
    *,
    expected_api_wallet_address: str,
    now_ms: int,
) -> VerifiedExtraAgent:
    """Require one currently valid venue registration for the configured API wallet."""

    expected = _address(
        expected_api_wallet_address,
        label="expected API wallet address",
    )
    if isinstance(now_ms, bool) or not isinstance(now_ms, int) or now_ms <= 0:
        raise ValueError("now_ms must be a positive integer")
    if not isinstance(payload, list):
        raise ValueError("extraAgents response must be an array")
    active: list[VerifiedExtraAgent] = []
    for entry in payload:
        if not isinstance(entry, Mapping) or set(entry) != {
            "address",
            "name",
            "validUntil",
        }:
            raise ValueError("extraAgents contains a malformed entry")
        name = entry.get("name")
        valid_until = entry.get("validUntil")
        if not isinstance(name, str) or not name or name != name.strip():
            raise ValueError("extraAgents contains an invalid name")
        if isinstance(valid_until, bool) or not isinstance(valid_until, int) or valid_until <= 0:
            raise ValueError("extraAgents contains an invalid validity timestamp")
        address = _address(cast(str, entry.get("address")), label="extra agent address")
        if valid_until > now_ms:
            active.append(VerifiedExtraAgent(name, address, valid_until))
    if len(active) != 1 or active[0].address != expected:
        raise ValueError("configured Testnet API wallet must be the only active extra agent")
    match = active[0]
    return match


def parse_all_mids(payload: object) -> Mapping[str, Decimal]:
    if not isinstance(payload, Mapping) or not payload:
        raise ValueError("allMids response must be a non-empty object")
    marks: dict[str, Decimal] = {}
    for coin, raw_mark in payload.items():
        if not isinstance(coin, str) or not coin or coin != coin.strip() or ":" in coin:
            raise ValueError("allMids contains an invalid perp name")
        mark = _decimal_field(raw_mark, label=f"allMids.{coin}")
        marks[f"HL:{coin}:perp"] = mark
    return MappingProxyType(marks)


def verify_user_role(payload: object) -> None:
    if not isinstance(payload, Mapping) or set(payload) != {"role"} or payload.get("role") != "user":
        raise ValueError("configured account must have the exact Hyperliquid user role")


def _positive_decimal(value: Decimal, *, label: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
        raise ValueError(f"{label} must be a positive finite Decimal")
    return value


def _wire_float(value: Decimal, *, label: str) -> float:
    normalized = _positive_decimal(value, label=label)
    result = float(normalized)
    if not Decimal(str(result)).is_finite():
        raise ValueError(f"{label} cannot be encoded by the official signing primitive")
    return result


def _decimal_places(value: Decimal) -> int:
    exponent = value.normalize().as_tuple().exponent
    if not isinstance(exponent, int):
        raise ValueError("finite Decimal exponent is required")
    return max(0, -exponent)


def _validate_order_precision(
    constraint: PerpAssetConstraints,
    *,
    quantity: Decimal,
    limit_price: Decimal,
) -> None:
    normalized_quantity = _positive_decimal(quantity, label="quantity")
    normalized_price = _positive_decimal(limit_price, label="limit_price")
    if _decimal_places(normalized_quantity) > constraint.size_decimals:
        raise ValueError("quantity exceeds the Testnet perp szDecimals constraint")
    max_price_decimals = 6 - constraint.size_decimals
    if _decimal_places(normalized_price) > max_price_decimals:
        raise ValueError("limit price exceeds the Hyperliquid perp decimal constraint")
    if (
        normalized_price != normalized_price.to_integral_value()
        and len(normalized_price.normalize().as_tuple().digits) > 5
    ):
        raise ValueError("non-integer limit price exceeds five significant figures")


def _decimal_field(value: object, *, label: str, allow_zero: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{label} is not a decimal")
    try:
        result = decimal_value(
            str(value),
            label=label,
            positive=not allow_zero,
            non_negative=allow_zero,
        )
    except (TypeError, ValueError):
        raise ValueError(f"{label} is not a decimal") from None
    return result


def _venue_oid(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("venue order id has an invalid type")
    oid = str(value)
    if not oid or not oid.isdigit():
        raise ValueError("venue order id is not a non-negative integer")
    return oid


SignL1 = Callable[[object, object, object, int, int | None, bool], object]


class _LiveConstraintsVerification:
    """One-use proof that this adapter just matched its frozen Testnet metadata."""

    __slots__ = ("_consumed", "_owner")

    def __init__(self, owner: object) -> None:
        self._owner = owner
        self._consumed = False

    def consume_for(self, owner: object) -> None:
        if self._owner is not owner or self._consumed:
            raise AdapterError("fresh live Testnet constraint verification is required")
        self._consumed = True


class HyperliquidTestnetAdapter:
    """Explicit Testnet-only order and read adapter with no endpoint fallback."""

    __slots__ = (
        "_account_address",
        "_action_ttl_ms",
        "_api_wallet_address",
        "_assets",
        "_constraints",
        "_http",
        "_origin",
        "_sign_l1",
        "_signer",
        "_timeout_seconds",
    )

    def __init__(
        self,
        *,
        origin: str,
        account_address: str,
        api_wallet_address: str,
        signer: SigningAccount,
        asset_constraints_by_coin: Mapping[str, PerpAssetConstraints],
        http: JsonHttpTransport,
        timeout_seconds: float,
        action_ttl_ms: int,
        sign_l1: SignL1 = sign_l1_action,
    ) -> None:
        _require_exact_origin(origin)
        timeout = _timeout_seconds(timeout_seconds)
        if isinstance(action_ttl_ms, bool) or not isinstance(action_ttl_ms, int):
            raise TypeError("action_ttl_ms must be an integer")
        if not 1_000 <= action_ttl_ms <= 60_000:
            raise ValueError("action_ttl_ms must be between 1000 and 60000")
        if not callable(sign_l1):
            raise TypeError("sign_l1 must be callable")
        constraints: dict[str, PerpAssetConstraints] = {}
        seen_assets: set[int] = set()
        for coin, constraint in asset_constraints_by_coin.items():
            if not isinstance(constraint, PerpAssetConstraints) or constraint.coin != coin:
                raise ValueError("asset constraint keys must exactly match their perp names")
            if constraint.asset in seen_assets:
                raise ValueError("asset constraint ids must be unique")
            constraints[coin] = constraint
            seen_assets.add(constraint.asset)
        if not constraints:
            raise ValueError("frozen non-empty Testnet perp constraints are required")
        assets = {coin: constraint.asset for coin, constraint in constraints.items()}
        self._origin = origin
        self._account_address = _address(account_address, label="account address")
        self._api_wallet_address = _address(
            api_wallet_address,
            label="expected API wallet address",
        )
        signer_address = _address(signer.address, label="signer address")
        if signer_address == "0x" + "0" * 40:
            raise ValueError("zero signer address is refused")
        if signer_address != self._api_wallet_address:
            raise ValueError("signer address does not match the configured Testnet API wallet")
        if self._account_address == self._api_wallet_address:
            raise ValueError("a distinct dedicated Testnet API wallet is required")
        self._signer = signer
        self._assets = MappingProxyType(assets)
        self._constraints = MappingProxyType(constraints)
        self._http = http
        self._timeout_seconds = timeout
        self._action_ttl_ms = action_ttl_ms
        self._sign_l1 = sign_l1

    @property
    def origin(self) -> str:
        return self._origin

    @property
    def account_address(self) -> str:
        return self._account_address

    @property
    def api_wallet_address(self) -> str:
        return self._api_wallet_address

    @property
    def asset_by_coin(self) -> Mapping[str, int]:
        return self._assets

    @property
    def constraints_by_coin(self) -> Mapping[str, PerpAssetConstraints]:
        return self._constraints

    @property
    def action_ttl_ms(self) -> int:
        return self._action_ttl_ms

    def _asset(self, coin: str) -> int:
        try:
            return self._assets[coin]
        except KeyError:
            raise ValueError("coin is absent from the frozen Testnet asset map") from None

    def _constraint(self, coin: str) -> PerpAssetConstraints:
        try:
            return self._constraints[coin]
        except KeyError:
            raise ValueError("coin is absent from the frozen Testnet constraints") from None

    def verify_live_constraints(self) -> object:
        try:
            live = perp_constraints_from_meta(self.read_meta())
        except (ReadTransportError, TypeError, ValueError):
            raise AdapterError("live Testnet perp constraints are unavailable") from None
        if dict(live) != dict(self._constraints):
            raise AdapterError("live Testnet perp constraints changed; signed action refused")
        return _LiveConstraintsVerification(self)

    def _consume_live_constraints_verification(self, verification: object) -> None:
        if not isinstance(verification, _LiveConstraintsVerification):
            raise AdapterError("fresh live Testnet constraint verification is required")
        verification.consume_for(self)

    def _post_signed(self, action: Mapping[str, object], *, nonce: int) -> HttpResult | None:
        if isinstance(nonce, bool) or not isinstance(nonce, int) or nonce <= 0:
            raise ValueError("nonce must be a durable positive millisecond integer")
        expires_after = nonce + self._action_ttl_ms
        try:
            signature = self._sign_l1(
                self._signer,
                dict(action),
                None,
                nonce,
                expires_after,
                False,
            )
        except Exception as error:
            raise AdapterError(f"Testnet action signing failed ({type(error).__name__})") from None
        payload: dict[str, object] = {
            "action": dict(action),
            "expiresAfter": expires_after,
            "nonce": nonce,
            "signature": signature,
            "vaultAddress": None,
        }
        try:
            return self._http.post_json(
                url=self._origin + _EXCHANGE_PATH,
                payload=payload,
                timeout_seconds=self._timeout_seconds,
            )
        except Exception:
            return None
        finally:
            # Do not retain a signed payload or signature on the adapter or outcome.
            payload.clear()
            signature = None

    def submit_order(
        self,
        intent: TestnetOrderIntent,
        *,
        nonce: int,
        constraint_verification: object,
    ) -> ActionOutcome:
        self._consume_live_constraints_verification(constraint_verification)
        if not isinstance(intent, TestnetOrderIntent):
            raise TypeError("submit_order requires a TestnetOrderIntent")
        coin = _coin_from_instrument(intent.instrument)
        _validate_order_precision(
            self._constraint(coin),
            quantity=intent.quantity,
            limit_price=intent.limit_price,
        )
        request: dict[str, object] = {
            "coin": coin,
            "is_buy": intent.side is OrderSide.BUY,
            "limit_px": _wire_float(intent.limit_price, label="limit_price"),
            "order_type": {"limit": {"tif": _TIF_WIRE[cast(TimeInForce, intent.time_in_force)]}},
            "reduce_only": intent.reduce_only,
            "sz": _wire_float(intent.quantity, label="quantity"),
            "cloid": Cloid.from_str(validate_cloid(intent.cloid)),
        }
        wire = order_request_to_order_wire(cast(Any, request), self._asset(coin))
        action = cast(Mapping[str, object], order_wires_to_order_action([wire]))
        response = self._post_signed(action, nonce=nonce)
        if response is None:
            return ActionOutcome(OutcomeKind.AMBIGUOUS, "SUBMIT_TRANSPORT_AMBIGUOUS")
        return _parse_order_result(response, operation="submit")

    def cancel_by_cloid(self, *, coin: str, cloid: str, nonce: int) -> ActionOutcome:
        action = {
            "type": "cancelByCloid",
            "cancels": [{"asset": self._asset(coin), "cloid": validate_cloid(cloid)}],
        }
        response = self._post_signed(action, nonce=nonce)
        if response is None:
            return ActionOutcome(OutcomeKind.AMBIGUOUS, "CANCEL_TRANSPORT_AMBIGUOUS")
        return _parse_cancel_result(response)

    def replace_order(
        self,
        *,
        original_cloid: str,
        replacement: TestnetOrderIntent,
        nonce: int,
        constraint_verification: object,
    ) -> ActionOutcome:
        self._consume_live_constraints_verification(constraint_verification)
        if not isinstance(replacement, TestnetOrderIntent):
            raise TypeError("replacement must be a TestnetOrderIntent")
        coin = _coin_from_instrument(replacement.instrument)
        _validate_order_precision(
            self._constraint(coin),
            quantity=replacement.quantity,
            limit_price=replacement.limit_price,
        )
        request: dict[str, object] = {
            "coin": coin,
            "is_buy": replacement.side is OrderSide.BUY,
            "limit_px": _wire_float(replacement.limit_price, label="limit_price"),
            "order_type": {"limit": {"tif": _TIF_WIRE[cast(TimeInForce, replacement.time_in_force)]}},
            "reduce_only": replacement.reduce_only,
            "sz": _wire_float(replacement.quantity, label="quantity"),
            "cloid": Cloid.from_str(validate_cloid(replacement.cloid)),
        }
        wire = order_request_to_order_wire(cast(Any, request), self._asset(coin))
        action = {
            "type": "batchModify",
            "modifies": [{"oid": validate_cloid(original_cloid), "order": wire}],
        }
        response = self._post_signed(action, nonce=nonce)
        if response is None:
            return ActionOutcome(OutcomeKind.AMBIGUOUS, "REPLACE_TRANSPORT_AMBIGUOUS")
        return _parse_order_result(response, operation="replace")

    def schedule_cancel(self, *, cancel_at_ms: int, nonce: int) -> ActionOutcome:
        if isinstance(cancel_at_ms, bool) or not isinstance(cancel_at_ms, int):
            raise TypeError("cancel_at_ms must be an integer")
        if not nonce + 5_000 <= cancel_at_ms <= nonce + 120_000:
            raise ValueError("dead-man cancellation must be 5 to 120 seconds after the durable nonce")
        response = self._post_signed(
            {"type": "scheduleCancel", "time": cancel_at_ms},
            nonce=nonce,
        )
        if response is None:
            return ActionOutcome(OutcomeKind.AMBIGUOUS, "DEADMAN_TRANSPORT_AMBIGUOUS")
        return _parse_schedule_cancel_result(response)

    def _read(self, payload: Mapping[str, object]) -> object:
        request_type = payload.get("type")
        if request_type not in _READ_TYPES:
            raise ValueError("read request type is not allowlisted")
        try:
            result = self._http.post_json(
                url=self._origin + _INFO_PATH,
                payload=payload,
                timeout_seconds=self._timeout_seconds,
            )
        except Exception as error:
            raise ReadTransportError(f"Testnet read failed ({type(error).__name__})") from None
        if result.status_code != 200:
            raise ReadTransportError("Testnet read returned a non-200 status")
        if result.payload is None:
            raise ReadTransportError("Testnet read returned invalid JSON")
        return result.payload

    def read_meta(self) -> object:
        return self._read({"type": "meta"})

    def read_all_mids(self) -> object:
        return self._read({"type": "allMids"})

    def read_extra_agents(self) -> object:
        return self._read({"type": "extraAgents", "user": self._account_address})

    def read_user_role(self) -> object:
        return self._read({"type": "userRole", "user": self._account_address})

    def read_open_orders(self) -> object:
        return self._read({"type": "openOrders", "user": self._account_address})

    def read_user_fills(self) -> object:
        return self._read({"type": "userFills", "user": self._account_address})

    def read_user_fills_by_time(self, *, start_time_ms: int, end_time_ms: int) -> object:
        for label, value in {
            "start_time_ms": start_time_ms,
            "end_time_ms": end_time_ms,
        }.items():
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{label} must be a positive integer")
        if start_time_ms > end_time_ms:
            raise ValueError("fill time range is inverted")
        return self._read(
            {
                "aggregateByTime": False,
                "endTime": end_time_ms,
                "startTime": start_time_ms,
                "type": "userFillsByTime",
                "user": self._account_address,
            }
        )

    def read_clearinghouse_state(self) -> object:
        return self._read({"type": "clearinghouseState", "user": self._account_address})

    def read_spot_clearinghouse_state(self) -> object:
        return self._read({"type": "spotClearinghouseState", "user": self._account_address})

    def read_order_status(self, cloid: str) -> object:
        return self._read(
            {
                "type": "orderStatus",
                "user": self._account_address,
                "oid": validate_cloid(cloid),
            }
        )


def _coin_from_instrument(instrument: str) -> str:
    parts = instrument.split(":")
    if len(parts) != 3 or parts[0] != "HL" or parts[2] != "perp" or not parts[1]:
        raise ValueError("Testnet execution supports only canonical HL:<coin>:perp instruments")
    return parts[1]


def _response_status(result: HttpResult) -> Mapping[str, object] | None:
    if result.status_code < 200 or result.status_code >= 300:
        return None
    if not isinstance(result.payload, Mapping):
        return None
    return cast(Mapping[str, object], result.payload)


def _single_status(payload: Mapping[str, object]) -> object | None:
    if set(payload) != {"response", "status"} or payload.get("status") != "ok":
        return None
    response = payload.get("response")
    if (
        not isinstance(response, Mapping)
        or set(response) != {"data", "type"}
        or response.get("type") != "order"
    ):
        return None
    data = response.get("data")
    if not isinstance(data, Mapping) or set(data) != {"statuses"}:
        return None
    statuses = data.get("statuses")
    if not isinstance(statuses, list) or len(statuses) != 1:
        return None
    return cast(object, statuses[0])


def _is_known_venue_error(payload: Mapping[str, object]) -> bool:
    return (
        set(payload) == {"response", "status"}
        and payload.get("status") == "err"
        and isinstance(payload.get("response"), str)
        and bool(cast(str, payload.get("response")))
    )


def _parse_order_result(result: HttpResult, *, operation: str) -> ActionOutcome:
    payload = _response_status(result)
    if payload is None:
        return ActionOutcome(OutcomeKind.AMBIGUOUS, f"{operation.upper()}_HTTP_AMBIGUOUS")
    if _is_known_venue_error(payload):
        return ActionOutcome(OutcomeKind.REJECTED, "VENUE_REJECTED")
    status = _single_status(payload)
    if isinstance(status, Mapping) and set(status) == {"resting"}:
        resting = status["resting"]
        if isinstance(resting, Mapping) and set(resting) == {"oid"}:
            try:
                oid = _venue_oid(resting.get("oid"))
            except ValueError:
                pass
            else:
                kind = OutcomeKind.REPLACED if operation == "replace" else OutcomeKind.RESTING
                return ActionOutcome(kind, "VENUE_RESTING", venue_order_id=oid)
    if isinstance(status, Mapping) and set(status) == {"filled"}:
        filled = status["filled"]
        if isinstance(filled, Mapping) and set(filled) == {"avgPx", "oid", "totalSz"}:
            try:
                return ActionOutcome(
                    OutcomeKind.FILLED,
                    "VENUE_FILLED",
                    venue_order_id=_venue_oid(filled.get("oid")),
                    filled_quantity=_decimal_field(filled.get("totalSz"), label="filled size"),
                    average_fill_price=_decimal_field(filled.get("avgPx"), label="average fill price"),
                )
            except ValueError:
                pass
    if (
        isinstance(status, Mapping)
        and set(status) == {"error"}
        and isinstance(status.get("error"), str)
        and bool(status.get("error"))
    ):
        return ActionOutcome(OutcomeKind.REJECTED, "VENUE_REJECTED")
    return ActionOutcome(OutcomeKind.UNKNOWN, f"{operation.upper()}_RESPONSE_UNKNOWN")


def _parse_cancel_result(result: HttpResult) -> ActionOutcome:
    payload = _response_status(result)
    if payload is None:
        return ActionOutcome(OutcomeKind.AMBIGUOUS, "CANCEL_HTTP_AMBIGUOUS")
    if _is_known_venue_error(payload):
        return ActionOutcome(OutcomeKind.REJECTED, "CANCEL_REJECTED")
    status = _single_status(payload)
    if status == "success":
        return ActionOutcome(OutcomeKind.CANCELLED, "VENUE_CANCELLED")
    if (
        isinstance(status, Mapping)
        and set(status) == {"error"}
        and isinstance(status.get("error"), str)
        and bool(status.get("error"))
    ):
        return ActionOutcome(OutcomeKind.REJECTED, "CANCEL_REJECTED")
    return ActionOutcome(OutcomeKind.UNKNOWN, "CANCEL_RESPONSE_UNKNOWN")


def _parse_schedule_cancel_result(result: HttpResult) -> ActionOutcome:
    payload = _response_status(result)
    if payload is None:
        return ActionOutcome(OutcomeKind.AMBIGUOUS, "DEADMAN_HTTP_AMBIGUOUS")
    if _is_known_venue_error(payload):
        return ActionOutcome(OutcomeKind.REJECTED, "DEADMAN_REJECTED")
    response = payload.get("response")
    if (
        set(payload) == {"response", "status"}
        and payload.get("status") == "ok"
        and isinstance(response, Mapping)
        and set(response) == {"type"}
        and response.get("type") == "default"
    ):
        return ActionOutcome(OutcomeKind.DEADMAN_ARMED, "DEADMAN_ARMED")
    return ActionOutcome(OutcomeKind.UNKNOWN, "DEADMAN_RESPONSE_UNKNOWN")


__all__ = [
    "TESTNET_API_ORIGIN",
    "TESTNET_WEBSOCKET_URL",
    "ActionOutcome",
    "AdapterError",
    "EndpointIsolationError",
    "HttpResult",
    "HyperliquidTestnetAdapter",
    "JsonHttpTransport",
    "OutcomeKind",
    "PerpAssetConstraints",
    "ReadTransportError",
    "RequestsJsonTransport",
    "TestnetSigner",
    "VerifiedExtraAgent",
    "asset_map_from_meta",
    "parse_all_mids",
    "perp_constraints_from_meta",
    "read_testnet_meta",
    "testnet_signer_from_secret",
    "verify_extra_agent_scope",
    "verify_user_role",
]
