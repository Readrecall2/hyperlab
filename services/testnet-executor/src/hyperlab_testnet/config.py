"""Strict immutable configuration for the isolated Hyperliquid Testnet service."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import cast

from .canonical import (
    JsonValue,
    canonical_json,
    canonical_sha256,
    decimal_text,
    decimal_value,
    deterministic_id,
)

TESTNET_HTTP_ENDPOINT = "https://api.hyperliquid-testnet.xyz"
TESTNET_WS_ENDPOINT = "wss://api.hyperliquid-testnet.xyz/ws"
TESTNET_ENVIRONMENT = "TESTNET"
TESTNET_PURPOSE = "TESTNET_EXECUTION"
TESTNET_CHAIN_IDENTITY = "TESTNET"
TESTNET_CREDENTIAL_NAMESPACE = "HYPERLAB_TESTNET"
TESTNET_EXECUTOR_VERSION = "0.3.0.dev0"
TESTNET_CONFIG_SCHEMA_VERSION = 1

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_IDENTITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@+\-]{0,255}\Z")
_ADDRESS_RE = re.compile(r"0x[0-9a-f]{40}\Z")
_RISK_DECIMAL_CEILINGS = {
    "max_gross_notional": Decimal("1000"),
    "max_position_notional": Decimal("500"),
    "max_order_notional": Decimal("100"),
    "max_position_quantity": Decimal("5"),
    "max_order_quantity": Decimal("1"),
}
_RISK_INTEGER_CEILINGS = {
    "max_concurrent_orders": 4,
    "submit_requests_per_minute": 12,
    "cancel_requests_per_minute": 24,
    "replace_requests_per_minute": 6,
    "market_stale_after_seconds": 5,
    "reconciliation_stale_after_seconds": 10,
    "deadman_interval_seconds": 30,
}


class TestnetConfigError(ValueError):
    """A Testnet configuration is missing, ambiguous, or non-canonical."""


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise TestnetConfigError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    raise TestnetConfigError(f"non-finite JSON number {value!r}")


def _require_exact_keys(
    value: Mapping[str, object],
    expected: frozenset[str],
    *,
    location: str,
) -> None:
    actual = frozenset(value)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        details: list[str] = []
        if missing:
            details.append(f"missing keys {missing}")
        if extra:
            details.append(f"unexpected keys {extra}")
        raise TestnetConfigError(f"{location}: {'; '.join(details)}")


def _identity(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _IDENTITY_RE.fullmatch(value) is None:
        raise TestnetConfigError(f"{label} must be an exact stable identifier")
    return value


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise TestnetConfigError(f"{label} must be a lowercase SHA-256")
    return value


def normalize_testnet_address(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _ADDRESS_RE.fullmatch(value) is None:
        raise TestnetConfigError(
            f"{label} must be 0x followed by exactly 20 lowercase-hex bytes"
        )
    return value


@dataclass(frozen=True, slots=True)
class TestnetRiskLimits:
    max_gross_notional: Decimal = Decimal("1000")
    max_position_notional: Decimal = Decimal("500")
    max_order_notional: Decimal = Decimal("100")
    max_position_quantity: Decimal = Decimal("5")
    max_order_quantity: Decimal = Decimal("1")
    max_concurrent_orders: int = 4
    submit_requests_per_minute: int = 12
    cancel_requests_per_minute: int = 24
    replace_requests_per_minute: int = 6
    market_stale_after_seconds: int = 5
    reconciliation_stale_after_seconds: int = 10
    deadman_interval_seconds: int = 30

    def __post_init__(self) -> None:
        for name in (
            "max_gross_notional",
            "max_position_notional",
            "max_order_notional",
            "max_position_quantity",
            "max_order_quantity",
        ):
            object.__setattr__(
                self,
                name,
                decimal_value(getattr(self, name), label=name, positive=True),
            )
        for name in (
            "max_concurrent_orders",
            "submit_requests_per_minute",
            "cancel_requests_per_minute",
            "replace_requests_per_minute",
            "market_stale_after_seconds",
            "reconciliation_stale_after_seconds",
            "deadman_interval_seconds",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise TestnetConfigError(f"{name} must be a positive integer")
        for name, decimal_ceiling in _RISK_DECIMAL_CEILINGS.items():
            if getattr(self, name) > decimal_ceiling:
                raise TestnetConfigError(
                    f"{name} exceeds the compiled conservative Testnet ceiling"
                )
        for name, integer_ceiling in _RISK_INTEGER_CEILINGS.items():
            if getattr(self, name) > integer_ceiling:
                raise TestnetConfigError(
                    f"{name} exceeds the compiled conservative Testnet ceiling"
                )
        if self.max_order_notional > self.max_position_notional:
            raise TestnetConfigError("max_order_notional cannot exceed max_position_notional")
        if self.max_order_quantity > self.max_position_quantity:
            raise TestnetConfigError("max_order_quantity cannot exceed max_position_quantity")
        if self.max_position_notional > self.max_gross_notional:
            raise TestnetConfigError("max_position_notional cannot exceed max_gross_notional")

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "cancel_requests_per_minute": self.cancel_requests_per_minute,
            "deadman_interval_seconds": self.deadman_interval_seconds,
            "market_stale_after_seconds": self.market_stale_after_seconds,
            "max_concurrent_orders": self.max_concurrent_orders,
            "max_gross_notional": decimal_text(self.max_gross_notional),
            "max_order_notional": decimal_text(self.max_order_notional),
            "max_order_quantity": decimal_text(self.max_order_quantity),
            "max_position_quantity": decimal_text(self.max_position_quantity),
            "max_position_notional": decimal_text(self.max_position_notional),
            "reconciliation_stale_after_seconds": self.reconciliation_stale_after_seconds,
            "replace_requests_per_minute": self.replace_requests_per_minute,
            "submit_requests_per_minute": self.submit_requests_per_minute,
        }

    @property
    def limits_hash(self) -> str:
        return canonical_sha256(self.to_dict())

    @classmethod
    def from_mapping(cls, value: object) -> TestnetRiskLimits:
        if not isinstance(value, Mapping):
            raise TestnetConfigError("$.risk_limits must be an object")
        raw = cast(Mapping[str, object], value)
        _require_exact_keys(
            raw,
            frozenset(
                {
                    "cancel_requests_per_minute",
                    "deadman_interval_seconds",
                    "market_stale_after_seconds",
                    "max_concurrent_orders",
                    "max_gross_notional",
                    "max_order_notional",
                    "max_order_quantity",
                    "max_position_quantity",
                    "max_position_notional",
                    "reconciliation_stale_after_seconds",
                    "replace_requests_per_minute",
                    "submit_requests_per_minute",
                }
            ),
            location="$.risk_limits",
        )
        try:
            return cls(
                max_gross_notional=decimal_value(
                    str(raw["max_gross_notional"]),
                    label="max_gross_notional",
                    positive=True,
                ),
                max_position_notional=decimal_value(
                    str(raw["max_position_notional"]),
                    label="max_position_notional",
                    positive=True,
                ),
                max_order_notional=decimal_value(
                    str(raw["max_order_notional"]),
                    label="max_order_notional",
                    positive=True,
                ),
                max_order_quantity=decimal_value(
                    str(raw["max_order_quantity"]),
                    label="max_order_quantity",
                    positive=True,
                ),
                max_position_quantity=decimal_value(
                    str(raw["max_position_quantity"]),
                    label="max_position_quantity",
                    positive=True,
                ),
                max_concurrent_orders=int(str(raw["max_concurrent_orders"])),
                submit_requests_per_minute=int(str(raw["submit_requests_per_minute"])),
                cancel_requests_per_minute=int(str(raw["cancel_requests_per_minute"])),
                replace_requests_per_minute=int(str(raw["replace_requests_per_minute"])),
                market_stale_after_seconds=int(str(raw["market_stale_after_seconds"])),
                reconciliation_stale_after_seconds=int(
                    str(raw["reconciliation_stale_after_seconds"])
                ),
                deadman_interval_seconds=int(str(raw["deadman_interval_seconds"])),
            )
        except (KeyError, TypeError, ValueError) as error:
            if isinstance(error, TestnetConfigError):
                raise
            raise TestnetConfigError(f"invalid risk_limits: {error}") from error


@dataclass(frozen=True, slots=True)
class TestnetConfig:
    candidate_id: str
    account_address: str
    api_wallet_address: str
    strategy_name: str
    strategy_hash: str
    build_hash: str
    source_identity: str
    source_hash: str
    risk_limits: TestnetRiskLimits = field(default_factory=TestnetRiskLimits)
    schema_version: int = TESTNET_CONFIG_SCHEMA_VERSION
    executor_version: str = TESTNET_EXECUTOR_VERSION
    environment: str = TESTNET_ENVIRONMENT
    purpose: str = TESTNET_PURPOSE
    chain_identity: str = TESTNET_CHAIN_IDENTITY
    credential_namespace: str = TESTNET_CREDENTIAL_NAMESPACE
    http_endpoint: str = TESTNET_HTTP_ENDPOINT
    ws_endpoint: str = TESTNET_WS_ENDPOINT

    def __post_init__(self) -> None:
        if self.schema_version != TESTNET_CONFIG_SCHEMA_VERSION:
            raise TestnetConfigError(
                f"schema_version must be exactly {TESTNET_CONFIG_SCHEMA_VERSION}"
            )
        exact_values = {
            "executor_version": (self.executor_version, TESTNET_EXECUTOR_VERSION),
            "environment": (self.environment, TESTNET_ENVIRONMENT),
            "purpose": (self.purpose, TESTNET_PURPOSE),
            "chain_identity": (self.chain_identity, TESTNET_CHAIN_IDENTITY),
            "credential_namespace": (
                self.credential_namespace,
                TESTNET_CREDENTIAL_NAMESPACE,
            ),
            "http_endpoint": (self.http_endpoint, TESTNET_HTTP_ENDPOINT),
            "ws_endpoint": (self.ws_endpoint, TESTNET_WS_ENDPOINT),
        }
        for label, (actual, expected) in exact_values.items():
            if actual != expected:
                raise TestnetConfigError(f"{label} must be exactly {expected!r}")
        object.__setattr__(self, "candidate_id", _identity(self.candidate_id, label="candidate_id"))
        object.__setattr__(
            self,
            "strategy_name",
            _identity(self.strategy_name, label="strategy_name"),
        )
        object.__setattr__(
            self,
            "source_identity",
            _identity(self.source_identity, label="source_identity"),
        )
        object.__setattr__(self, "strategy_hash", _sha256(self.strategy_hash, label="strategy_hash"))
        object.__setattr__(self, "build_hash", _sha256(self.build_hash, label="build_hash"))
        object.__setattr__(self, "source_hash", _sha256(self.source_hash, label="source_hash"))
        object.__setattr__(
            self,
            "account_address",
            normalize_testnet_address(self.account_address, label="account_address"),
        )
        object.__setattr__(
            self,
            "api_wallet_address",
            normalize_testnet_address(self.api_wallet_address, label="api_wallet_address"),
        )
        if self.account_address == self.api_wallet_address:
            raise TestnetConfigError("api_wallet_address must be distinct from account_address")
        if not isinstance(self.risk_limits, TestnetRiskLimits):
            raise TypeError("risk_limits must be a TestnetRiskLimits")

    @property
    def config_hash(self) -> str:
        return canonical_sha256(self.to_dict())

    @property
    def run_id(self) -> str:
        return deterministic_id("hyperliquid_testnet_run_v1", self.config_hash)

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "account_address": self.account_address,
            "api_wallet_address": self.api_wallet_address,
            "build_hash": self.build_hash,
            "candidate_id": self.candidate_id,
            "chain_identity": self.chain_identity,
            "credential_namespace": self.credential_namespace,
            "environment": self.environment,
            "executor_version": self.executor_version,
            "http_endpoint": self.http_endpoint,
            "purpose": self.purpose,
            "risk_limits": self.risk_limits.to_dict(),
            "schema_version": self.schema_version,
            "source_hash": self.source_hash,
            "source_identity": self.source_identity,
            "strategy_hash": self.strategy_hash,
            "strategy_name": self.strategy_name,
            "ws_endpoint": self.ws_endpoint,
        }

    def canonical_json_bytes(self) -> bytes:
        return canonical_json(self.to_dict()).encode("utf-8")

    def to_readiness_subject(self) -> dict[str, str]:
        return {
            "build_hash": self.build_hash,
            "candidate_id": self.candidate_id,
            "config_hash": self.config_hash,
            "risk_limits_hash": self.risk_limits.limits_hash,
            "source_identity": self.source_identity,
            "strategy_hash": self.strategy_hash,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> TestnetConfig:
        _require_exact_keys(
            value,
            frozenset(
                {
                    "account_address",
                    "api_wallet_address",
                    "build_hash",
                    "candidate_id",
                    "chain_identity",
                    "credential_namespace",
                    "environment",
                    "executor_version",
                    "http_endpoint",
                    "purpose",
                    "risk_limits",
                    "schema_version",
                    "source_hash",
                    "source_identity",
                    "strategy_hash",
                    "strategy_name",
                    "ws_endpoint",
                }
            ),
            location="$",
        )
        try:
            return cls(
                candidate_id=str(value["candidate_id"]),
                account_address=str(value["account_address"]),
                api_wallet_address=str(value["api_wallet_address"]),
                strategy_name=str(value["strategy_name"]),
                strategy_hash=str(value["strategy_hash"]),
                build_hash=str(value["build_hash"]),
                source_identity=str(value["source_identity"]),
                source_hash=str(value["source_hash"]),
                risk_limits=TestnetRiskLimits.from_mapping(value["risk_limits"]),
                schema_version=int(str(value["schema_version"])),
                executor_version=str(value["executor_version"]),
                environment=str(value["environment"]),
                purpose=str(value["purpose"]),
                chain_identity=str(value["chain_identity"]),
                credential_namespace=str(value["credential_namespace"]),
                http_endpoint=str(value["http_endpoint"]),
                ws_endpoint=str(value["ws_endpoint"]),
            )
        except (KeyError, TypeError, ValueError) as error:
            if isinstance(error, TestnetConfigError):
                raise
            raise TestnetConfigError(f"invalid Testnet configuration: {error}") from error

    @classmethod
    def from_json_bytes(cls, payload: bytes) -> TestnetConfig:
        if not isinstance(payload, bytes):
            raise TypeError("Testnet configuration payload must be bytes")
        json_payload = payload
        if json_payload.endswith(b"\r\n"):
            json_payload = json_payload[:-2]
        elif json_payload.endswith(b"\n"):
            json_payload = json_payload[:-1]
        try:
            decoded = json.loads(
                json_payload.decode("utf-8"),
                object_pairs_hook=_strict_object,
                parse_constant=_reject_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise TestnetConfigError(f"invalid Testnet configuration JSON: {error}") from error
        if not isinstance(decoded, Mapping):
            raise TestnetConfigError("Testnet configuration root must be an object")
        config = cls.from_mapping(cast(Mapping[str, object], decoded))
        if json_payload != config.canonical_json_bytes():
            raise TestnetConfigError(
                "Testnet configuration bytes must be canonical JSON with at most one terminal EOL"
            )
        return config


__all__ = [
    "TESTNET_CHAIN_IDENTITY",
    "TESTNET_CONFIG_SCHEMA_VERSION",
    "TESTNET_CREDENTIAL_NAMESPACE",
    "TESTNET_ENVIRONMENT",
    "TESTNET_EXECUTOR_VERSION",
    "TESTNET_HTTP_ENDPOINT",
    "TESTNET_PURPOSE",
    "TESTNET_WS_ENDPOINT",
    "TestnetConfig",
    "TestnetConfigError",
    "TestnetRiskLimits",
    "normalize_testnet_address",
]
