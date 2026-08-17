"""Dedicated, non-serializable Hyperliquid Testnet credential boundary."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import NoReturn, SupportsIndex

from .canonical import canonical_sha256
from .config import (
    TESTNET_CREDENTIAL_NAMESPACE,
    TestnetConfig,
    TestnetConfigError,
    normalize_testnet_address,
)

TESTNET_PRIVATE_KEY_ENV = "HYPERLAB_TESTNET_PRIVATE_KEY"
TESTNET_ACCOUNT_ADDRESS_ENV = "HYPERLAB_TESTNET_ACCOUNT_ADDRESS"
TESTNET_API_WALLET_ADDRESS_ENV = "HYPERLAB_TESTNET_API_WALLET_ADDRESS"

_PRIVATE_KEY_RE = re.compile(r"(?:0x)?[0-9a-fA-F]{64}\Z")
_FORBIDDEN_EXACT_NAMES = frozenset(
    {
        "HYPERLAB_PRIVATE_KEY",
        "HYPERLAB_API_KEY",
        "HYPERLAB_WALLET_PRIVATE_KEY",
        "PRIVATE_KEY",
    }
)
_FORBIDDEN_PREFIXES = (
    "HYPERLAB_MAINNET_",
    "HYPERLAB_MICRO_MAINNET_",
    "HYPERLAB_PAPER_",
)
_SECRET_NAME_MARKERS = ("KEY", "SECRET", "SEED", "MNEMONIC", "TOKEN", "SIGNATURE")
_ALLOWED_CREDENTIAL_NAMES = frozenset(
    {
        TESTNET_PRIVATE_KEY_ENV,
        TESTNET_ACCOUNT_ADDRESS_ENV,
        TESTNET_API_WALLET_ADDRESS_ENV,
    }
)


class TestnetCredentialError(ValueError):
    """Credentials are missing, incorrectly scoped, or inconsistent with config."""


class SecretSerializationError(TypeError):
    """A caller attempted to serialize or pickle a Testnet secret."""


class TestnetSecret:
    """An opaque signer input with deliberately redacted representations.

    Only the signing adapter should call :meth:`reveal_for_signer`.  The value is
    never part of a config, store record, log payload, or exception message.
    """

    __slots__ = ("__value",)

    def __init__(self, value: str) -> None:
        if not isinstance(value, str) or _PRIVATE_KEY_RE.fullmatch(value) is None:
            raise TestnetCredentialError("Testnet private key has an invalid shape")
        normalized = value[2:] if value.startswith("0x") else value
        self.__value = f"0x{normalized.lower()}"

    def __repr__(self) -> str:
        return "TestnetSecret([REDACTED])"

    def __str__(self) -> str:
        return "[REDACTED]"

    def __bytes__(self) -> bytes:
        raise SecretSerializationError("Testnet secrets cannot be converted to bytes")

    def __reduce__(self) -> NoReturn:
        raise SecretSerializationError("Testnet secrets cannot be pickled")

    def __reduce_ex__(self, protocol: SupportsIndex) -> NoReturn:
        del protocol
        raise SecretSerializationError("Testnet secrets cannot be pickled")

    def __getstate__(self) -> object:
        raise SecretSerializationError("Testnet secrets cannot be serialized")

    def reveal_for_signer(self) -> str:
        """Return the key only at the audited signer-construction boundary."""

        return self.__value


@dataclass(frozen=True, slots=True)
class TestnetCredentials:
    private_key: TestnetSecret
    account_address: str
    api_wallet_address: str
    namespace: str = TESTNET_CREDENTIAL_NAMESPACE

    def __post_init__(self) -> None:
        if not isinstance(self.private_key, TestnetSecret):
            raise TypeError("private_key must be a TestnetSecret")
        if self.namespace != TESTNET_CREDENTIAL_NAMESPACE:
            raise TestnetCredentialError(
                f"credential namespace must be exactly {TESTNET_CREDENTIAL_NAMESPACE!r}"
            )
        try:
            account = normalize_testnet_address(
                self.account_address,
                label="credential account_address",
            )
            api_wallet = normalize_testnet_address(
                self.api_wallet_address,
                label="credential api_wallet_address",
            )
        except TestnetConfigError as error:
            raise TestnetCredentialError(str(error)) from error
        object.__setattr__(self, "account_address", account)
        object.__setattr__(self, "api_wallet_address", api_wallet)
        if account == api_wallet:
            raise TestnetCredentialError(
                "Testnet API wallet must be distinct from the account address"
            )

    def __repr__(self) -> str:
        return (
            "TestnetCredentials(private_key=[REDACTED], "
            f"account_address={self.account_address!r}, "
            f"api_wallet_address={self.api_wallet_address!r}, "
            f"namespace={self.namespace!r})"
        )

    def __str__(self) -> str:
        return self.__repr__()

    def __getstate__(self) -> object:
        raise SecretSerializationError("Testnet credentials cannot be serialized")

    def __reduce__(self) -> NoReturn:
        raise SecretSerializationError("Testnet credentials cannot be pickled")

    def __reduce_ex__(self, protocol: SupportsIndex) -> NoReturn:
        del protocol
        raise SecretSerializationError("Testnet credentials cannot be pickled")

    def to_dict(self) -> dict[str, object]:
        raise SecretSerializationError("Testnet credentials have no serializable form")

    @property
    def public_scope_hash(self) -> str:
        return canonical_sha256(
            {
                "account_address": self.account_address,
                "api_wallet_address": self.api_wallet_address,
                "namespace": self.namespace,
            }
        )

    def validate_derived_api_wallet_address(self, derived_address: str) -> None:
        """Fail closed after the adapter constructs its signer from the secret."""

        try:
            normalized = normalize_testnet_address(
                derived_address.lower(),
                label="derived signer address",
            )
        except TestnetConfigError as error:
            raise TestnetCredentialError("derived signer address has an invalid shape") from error
        if normalized != self.api_wallet_address:
            raise TestnetCredentialError(
                "derived signer address does not match the configured Testnet API wallet"
            )


def _forbidden_namespace_names(environ: Mapping[str, str]) -> tuple[str, ...]:
    forbidden: list[str] = []
    for name, value in environ.items():
        if not value:
            continue
        upper = name.upper()
        if upper in _ALLOWED_CREDENTIAL_NAMES:
            continue
        if upper in _FORBIDDEN_EXACT_NAMES:
            forbidden.append(name)
            continue
        if upper.startswith(_FORBIDDEN_PREFIXES) and any(
            marker in upper for marker in _SECRET_NAME_MARKERS
        ):
            forbidden.append(name)
            continue
        if upper.startswith(("HYPERLAB_", "HYPERLIQUID_")) and any(
            marker in upper for marker in _SECRET_NAME_MARKERS
        ):
            forbidden.append(name)
    return tuple(sorted(forbidden))


def load_testnet_credentials(
    config: TestnetConfig,
    *,
    environ: Mapping[str, str] | None = None,
) -> TestnetCredentials:
    """Load only the exact ``HYPERLAB_TESTNET_*`` credential namespace."""

    if not isinstance(config, TestnetConfig):
        raise TypeError("config must be a TestnetConfig")
    source = os.environ if environ is None else environ
    forbidden = _forbidden_namespace_names(source)
    if forbidden:
        # Variable names are safe to report; values never are.
        raise TestnetCredentialError(
            "generic/Mainnet credential namespaces are forbidden: " + ", ".join(forbidden)
        )
    required = (
        TESTNET_PRIVATE_KEY_ENV,
        TESTNET_ACCOUNT_ADDRESS_ENV,
        TESTNET_API_WALLET_ADDRESS_ENV,
    )
    missing = [name for name in required if not source.get(name)]
    if missing:
        raise TestnetCredentialError(
            "missing dedicated Testnet credential variables: " + ", ".join(missing)
        )
    account = str(source[TESTNET_ACCOUNT_ADDRESS_ENV]).lower()
    api_wallet = str(source[TESTNET_API_WALLET_ADDRESS_ENV]).lower()
    if account != config.account_address:
        raise TestnetCredentialError(
            "Testnet credential account address differs from immutable configuration"
        )
    if api_wallet != config.api_wallet_address:
        raise TestnetCredentialError(
            "Testnet credential API wallet differs from immutable configuration"
        )
    return TestnetCredentials(
        private_key=TestnetSecret(str(source[TESTNET_PRIVATE_KEY_ENV])),
        account_address=account,
        api_wallet_address=api_wallet,
    )


__all__ = [
    "TESTNET_ACCOUNT_ADDRESS_ENV",
    "TESTNET_API_WALLET_ADDRESS_ENV",
    "TESTNET_PRIVATE_KEY_ENV",
    "SecretSerializationError",
    "TestnetCredentialError",
    "TestnetCredentials",
    "TestnetSecret",
    "load_testnet_credentials",
]
