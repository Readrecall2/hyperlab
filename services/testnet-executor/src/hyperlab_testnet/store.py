"""Crash-safe SQLite authority for Hyperliquid Testnet execution.

Every signed action is represented by an AMBIGUOUS durable outbox row and a
wallet-global, crash-durable nonce before network I/O.  The store never accepts
signatures, signed payloads, private keys, or other secret-like material.
"""

from __future__ import annotations

import importlib
import json
import os
import re
import sqlite3
import stat
import threading
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Self, cast

from .canonical import (
    canonical_json,
    canonical_sha256,
    decimal_text,
    decimal_value,
    deterministic_id,
    parse_utc,
    utc_text,
)
from .config import TestnetConfig
from .models import (
    ActionAttemptStatus,
    ActionKind,
    OrderStatus,
    RuntimeState,
    TestnetOrder,
    TestnetOrderIntent,
    require_order_transition,
)

SCHEMA_VERSION = 1
ZERO_HASH = "0" * 64
ACCOUNT_ACTION_IDENTITY_CAPACITY = 100_000

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_IDENTITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@+\-]{0,255}\Z")
_RAW_PRIVATE_KEY_RE = re.compile(r"0x[0-9a-fA-F]{64}\Z")
_RAW_UNPREFIXED_SECRET_RE = re.compile(r"[0-9a-fA-F]{64}\Z")
_RAW_SIGNATURE_RE = re.compile(r"0x[0-9a-fA-F]{128,132}\Z")
_REASON_CODE_RE = re.compile(r"[A-Z][A-Z0-9_:\-]{0,127}\Z")
_FORBIDDEN_KEY_MARKERS = (
    "private_key",
    "secret",
    "seed",
    "mnemonic",
    "signature",
    "signed_payload",
    "api_key",
    "password",
    "token",
)
_PUBLIC_HEX_IDENTITY_FIELDS = frozenset(
    {
        "action_id",
        "decision_id",
        "event_id",
        "fill_id",
        "identity",
        "order_id",
        "run_id",
        "snapshot_id",
    }
)
_PUBLIC_HEX_HASH_FIELDS = frozenset(
    {
        "account_scope_hash",
        "audit_head_hash",
        "build_hash",
        "config_hash",
        "event_hash",
        "expected_source_hash",
        "expected_strategy_hash",
        "intent_hash",
        "payload_hash",
        "position_hash",
        "previous_hash",
        "proof_hash",
        "reconciliation_failure_input_hash",
        "reconciliation_input_hash",
        "reconciliation_issue_details_hash",
        "reconciliation_snapshot_hash",
        "record_hash",
        "response_hash",
        "risk_limits_hash",
        "snapshot_hash",
        "source_hash",
        "spot_balance_hash",
        "strategy_hash",
        "venue_hash_digest",
        "wallet_scope_hash",
    }
)


class TestnetStoreError(RuntimeError):
    pass


class SchemaVersionError(TestnetStoreError):
    pass


class RunNotFoundError(TestnetStoreError):
    pass


class RunConflictError(TestnetStoreError):
    pass


class IdempotencyConflictError(TestnetStoreError):
    pass


class AmbiguousActionReplayError(IdempotencyConflictError):
    pass


class OrderConflictError(TestnetStoreError):
    pass


class SecretPersistenceError(TestnetStoreError):
    pass


class WalletLeaseError(TestnetStoreError):
    pass


@dataclass(frozen=True, slots=True)
class _AccountRateEvent:
    action_id: str
    kind: ActionKind
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class _WindowsControlSecurity:
    owner_sid: str
    current_sid: str
    dacl_present: bool
    allowed_aces: tuple[tuple[str, int], ...]
    unsupported_ace_types: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class IntegrityIssue:
    code: str
    detail: str


@dataclass(frozen=True, slots=True)
class IntegrityReport:
    run_id: str
    ok: bool
    audit_count: int
    audit_head_hash: str
    issues: tuple[IntegrityIssue, ...]

    def __bool__(self) -> bool:
        return self.ok


class IntegrityError(TestnetStoreError):
    def __init__(self, report: IntegrityReport) -> None:
        super().__init__(
            f"Testnet store integrity failed for {report.run_id}: "
            + "; ".join(f"{issue.code}: {issue.detail}" for issue in report.issues)
        )
        self.report = report


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: str
    config_hash: str
    config: Mapping[str, object]
    runtime_state: RuntimeState
    state_reason: str | None
    last_nonce: int
    last_reconciled_at: datetime | None
    reconciliation_snapshot_hash: str | None
    reconciliation_snapshot_id: str | None
    audit_count: int
    audit_head_hash: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ActionAttemptRecord:
    run_id: str
    action_id: str
    order_id: str | None
    kind: ActionKind
    nonce: int
    payload: Mapping[str, object]
    payload_hash: str
    status: ActionAttemptStatus
    response: Mapping[str, object] | None
    created_at: datetime
    resolved_at: datetime | None


@dataclass(frozen=True, slots=True)
class FillRecord:
    run_id: str
    fill_id: str
    order_id: str
    venue_order_id: str
    quantity: Decimal
    price: Decimal
    fee: Decimal
    payload: Mapping[str, object]
    payload_hash: str
    received_at: datetime


@dataclass(frozen=True, slots=True)
class RemoteSnapshotRecord:
    run_id: str
    snapshot_id: str
    payload: Mapping[str, object]
    payload_hash: str
    received_at: datetime
    reconciled: bool


@dataclass(frozen=True, slots=True)
class AuditEventRecord:
    run_id: str
    sequence: int
    event_id: str
    event_type: str
    payload: Mapping[str, object]
    payload_hash: str
    previous_hash: str
    event_hash: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class WalletLeaseRecord:
    run_id: str
    wallet_scope_hash: str
    owner_id: str
    acquired_at: datetime
    renewed_at: datetime
    lock_path: Path


@dataclass(slots=True)
class _WalletLeaseHandle:
    stream: BinaryIO
    record: WalletLeaseRecord


@dataclass(frozen=True, slots=True)
class OrderProjectionUpdate:
    order_id: str
    status: OrderStatus
    venue_order_id: str | None = None
    filled_quantity: Decimal | None = None
    average_fill_price: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "order_id", _sha256(self.order_id, label="order_id"))
        object.__setattr__(self, "status", OrderStatus(self.status))
        if self.venue_order_id is not None:
            object.__setattr__(
                self,
                "venue_order_id",
                _identity(self.venue_order_id, label="venue_order_id"),
            )
        if self.filled_quantity is not None:
            object.__setattr__(
                self,
                "filled_quantity",
                decimal_value(
                    self.filled_quantity,
                    label="filled_quantity",
                    non_negative=True,
                ),
            )
        if self.average_fill_price is not None:
            object.__setattr__(
                self,
                "average_fill_price",
                decimal_value(
                    self.average_fill_price,
                    label="average_fill_price",
                    positive=True,
                ),
            )


@dataclass(frozen=True, slots=True)
class ReconciliationFill:
    fill_id: str
    order_id: str
    venue_order_id: str
    quantity: Decimal
    price: Decimal
    fee: Decimal
    payload: Mapping[str, object]
    received_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "fill_id", _identity(self.fill_id, label="fill_id"))
        object.__setattr__(self, "order_id", _sha256(self.order_id, label="order_id"))
        object.__setattr__(
            self,
            "venue_order_id",
            _identity(self.venue_order_id, label="venue_order_id"),
        )
        object.__setattr__(
            self,
            "quantity",
            decimal_value(self.quantity, label="fill quantity", positive=True),
        )
        object.__setattr__(
            self,
            "price",
            decimal_value(self.price, label="fill price", positive=True),
        )
        object.__setattr__(self, "fee", decimal_value(self.fee, label="fill fee"))
        _payload(self.payload, label="reconciliation fill payload")
        object.__setattr__(
            self,
            "received_at",
            _utc(self.received_at, label="fill received_at"),
        )


@dataclass(frozen=True, slots=True)
class ReconciliationActionResolution:
    action_id: str
    status: ActionAttemptStatus
    proof: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "action_id",
            _sha256(self.action_id, label="action_id"),
        )
        object.__setattr__(self, "status", ActionAttemptStatus(self.status))
        if self.status is ActionAttemptStatus.AMBIGUOUS:
            raise ValueError("reconciliation action resolution cannot remain AMBIGUOUS")
        normalized, _, _ = _payload(
            self.proof,
            label="authoritative action resolution proof",
        )
        if not normalized:
            raise ValueError("authoritative action resolution proof cannot be empty")


@dataclass(frozen=True, slots=True)
class ReconciliationIssue:
    issue_code: str
    details: Mapping[str, object]

    def __post_init__(self) -> None:
        code = _identity(self.issue_code, label="reconciliation issue_code")
        if _REASON_CODE_RE.fullmatch(code) is None:
            raise ValueError(
                "reconciliation issue_code must be a bounded uppercase stable code"
            )
        object.__setattr__(self, "issue_code", code)
        _payload(self.details, label="reconciliation issue details")


_SCHEMA_SQL = """
CREATE TABLE testnet_schema (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL CHECK (version > 0),
    created_at TEXT NOT NULL
);

CREATE TABLE testnet_runs (
    run_id TEXT NOT NULL PRIMARY KEY,
    config_json TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    runtime_state TEXT NOT NULL,
    state_reason TEXT,
    last_nonce INTEGER NOT NULL CHECK (last_nonce >= 0),
    last_reconciled_at TEXT,
    reconciliation_snapshot_hash TEXT,
    reconciliation_snapshot_id TEXT,
    audit_count INTEGER NOT NULL CHECK (audit_count >= 0),
    audit_head_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE testnet_orders (
    run_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    cloid TEXT NOT NULL,
    intent_json TEXT NOT NULL,
    intent_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    filled_quantity_text TEXT NOT NULL,
    average_fill_price_text TEXT,
    venue_order_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, order_id),
    UNIQUE (run_id, cloid),
    FOREIGN KEY (run_id) REFERENCES testnet_runs(run_id)
);

CREATE TABLE testnet_actions (
    run_id TEXT NOT NULL,
    action_id TEXT NOT NULL,
    order_id TEXT,
    action_kind TEXT NOT NULL,
    nonce INTEGER NOT NULL CHECK (nonce > 0),
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    response_json TEXT,
    response_hash TEXT,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    PRIMARY KEY (run_id, action_id),
    UNIQUE (run_id, nonce),
    FOREIGN KEY (run_id) REFERENCES testnet_runs(run_id),
    FOREIGN KEY (run_id, order_id) REFERENCES testnet_orders(run_id, order_id)
);

CREATE TABLE testnet_action_rate_events (
    run_id TEXT NOT NULL,
    action_id TEXT NOT NULL,
    action_kind TEXT NOT NULL,
    nonce INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    PRIMARY KEY (run_id, action_id),
    FOREIGN KEY (run_id, action_id) REFERENCES testnet_actions(run_id, action_id)
);

CREATE TABLE testnet_fills (
    run_id TEXT NOT NULL,
    fill_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    venue_order_id TEXT NOT NULL,
    quantity_text TEXT NOT NULL,
    price_text TEXT NOT NULL,
    fee_text TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (run_id, fill_id),
    FOREIGN KEY (run_id, order_id) REFERENCES testnet_orders(run_id, order_id)
);

CREATE TABLE testnet_fill_aggregates (
    run_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    stable_quantity_text TEXT NOT NULL,
    stable_notional_text TEXT NOT NULL,
    stable_fee_text TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, order_id),
    FOREIGN KEY (run_id, order_id) REFERENCES testnet_orders(run_id, order_id)
);

CREATE TABLE testnet_remote_snapshots (
    run_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    received_at TEXT NOT NULL,
    reconciled INTEGER NOT NULL CHECK (reconciled IN (0, 1)),
    PRIMARY KEY (run_id, snapshot_id),
    FOREIGN KEY (run_id) REFERENCES testnet_runs(run_id)
);

CREATE TABLE testnet_audit_events (
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, sequence),
    UNIQUE (run_id, event_id),
    UNIQUE (run_id, event_hash),
    FOREIGN KEY (run_id) REFERENCES testnet_runs(run_id)
);

CREATE INDEX testnet_orders_status_idx ON testnet_orders(run_id, status);
CREATE INDEX testnet_actions_status_idx ON testnet_actions(run_id, status);
CREATE INDEX testnet_rate_window_idx
    ON testnet_action_rate_events(run_id, action_kind, occurred_at);
CREATE INDEX testnet_fills_order_idx ON testnet_fills(run_id, order_id);

CREATE TRIGGER testnet_runs_config_immutable
BEFORE UPDATE ON testnet_runs
WHEN OLD.run_id != NEW.run_id
  OR OLD.config_json != NEW.config_json
  OR OLD.config_hash != NEW.config_hash
  OR OLD.created_at != NEW.created_at
BEGIN SELECT RAISE(ABORT, 'Testnet run configuration is immutable'); END;

CREATE TRIGGER testnet_orders_intent_immutable
BEFORE UPDATE ON testnet_orders
WHEN OLD.run_id != NEW.run_id
  OR OLD.order_id != NEW.order_id
  OR OLD.cloid != NEW.cloid
  OR OLD.intent_json != NEW.intent_json
  OR OLD.intent_hash != NEW.intent_hash
  OR OLD.created_at != NEW.created_at
BEGIN SELECT RAISE(ABORT, 'Testnet order intent is immutable'); END;

CREATE TRIGGER testnet_actions_identity_immutable
BEFORE UPDATE ON testnet_actions
WHEN OLD.run_id != NEW.run_id
  OR OLD.action_id != NEW.action_id
  OR COALESCE(OLD.order_id, '') != COALESCE(NEW.order_id, '')
  OR OLD.action_kind != NEW.action_kind
  OR OLD.nonce != NEW.nonce
  OR OLD.payload_json != NEW.payload_json
  OR OLD.payload_hash != NEW.payload_hash
  OR OLD.created_at != NEW.created_at
BEGIN SELECT RAISE(ABORT, 'Testnet action identity is immutable'); END;

CREATE TRIGGER testnet_audit_no_update BEFORE UPDATE ON testnet_audit_events
BEGIN SELECT RAISE(ABORT, 'Testnet audit is append-only'); END;
CREATE TRIGGER testnet_audit_no_delete BEFORE DELETE ON testnet_audit_events
BEGIN SELECT RAISE(ABORT, 'Testnet audit is append-only'); END;
CREATE TRIGGER testnet_fills_no_update BEFORE UPDATE ON testnet_fills
BEGIN SELECT RAISE(ABORT, 'Testnet fills are append-only'); END;
CREATE TRIGGER testnet_fills_no_delete BEFORE DELETE ON testnet_fills
BEGIN SELECT RAISE(ABORT, 'Testnet fills are append-only'); END;
CREATE TRIGGER testnet_snapshots_no_update BEFORE UPDATE ON testnet_remote_snapshots
BEGIN SELECT RAISE(ABORT, 'Testnet snapshots are append-only'); END;
CREATE TRIGGER testnet_snapshots_no_delete BEFORE DELETE ON testnet_remote_snapshots
BEGIN SELECT RAISE(ABORT, 'Testnet snapshots are append-only'); END;
CREATE TRIGGER testnet_actions_no_delete BEFORE DELETE ON testnet_actions
BEGIN SELECT RAISE(ABORT, 'Testnet actions are append-only'); END;
CREATE TRIGGER testnet_rate_no_update BEFORE UPDATE ON testnet_action_rate_events
BEGIN SELECT RAISE(ABORT, 'Testnet rate events are append-only'); END;
CREATE TRIGGER testnet_rate_no_delete BEFORE DELETE ON testnet_action_rate_events
BEGIN SELECT RAISE(ABORT, 'Testnet rate events are append-only'); END;
"""

_REQUIRED_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "testnet_schema": ("singleton", "version", "created_at"),
    "testnet_runs": (
        "run_id",
        "config_json",
        "config_hash",
        "runtime_state",
        "state_reason",
        "last_nonce",
        "last_reconciled_at",
        "reconciliation_snapshot_hash",
        "reconciliation_snapshot_id",
        "audit_count",
        "audit_head_hash",
        "created_at",
        "updated_at",
    ),
    "testnet_orders": (
        "run_id",
        "order_id",
        "cloid",
        "intent_json",
        "intent_hash",
        "status",
        "filled_quantity_text",
        "average_fill_price_text",
        "venue_order_id",
        "created_at",
        "updated_at",
    ),
    "testnet_actions": (
        "run_id",
        "action_id",
        "order_id",
        "action_kind",
        "nonce",
        "payload_json",
        "payload_hash",
        "status",
        "response_json",
        "response_hash",
        "created_at",
        "resolved_at",
    ),
    "testnet_action_rate_events": (
        "run_id",
        "action_id",
        "action_kind",
        "nonce",
        "occurred_at",
    ),
    "testnet_fills": (
        "run_id",
        "fill_id",
        "order_id",
        "venue_order_id",
        "quantity_text",
        "price_text",
        "fee_text",
        "payload_json",
        "payload_hash",
        "received_at",
    ),
    "testnet_fill_aggregates": (
        "run_id",
        "order_id",
        "stable_quantity_text",
        "stable_notional_text",
        "stable_fee_text",
        "updated_at",
    ),
    "testnet_remote_snapshots": (
        "run_id",
        "snapshot_id",
        "payload_json",
        "payload_hash",
        "received_at",
        "reconciled",
    ),
    "testnet_audit_events": (
        "run_id",
        "sequence",
        "event_id",
        "event_type",
        "payload_json",
        "payload_hash",
        "previous_hash",
        "event_hash",
        "created_at",
    ),
}
_REQUIRED_TRIGGERS = frozenset(
    {
        "testnet_actions_identity_immutable",
        "testnet_actions_no_delete",
        "testnet_audit_no_delete",
        "testnet_audit_no_update",
        "testnet_fills_no_delete",
        "testnet_fills_no_update",
        "testnet_orders_intent_immutable",
        "testnet_rate_no_delete",
        "testnet_rate_no_update",
        "testnet_runs_config_immutable",
        "testnet_snapshots_no_delete",
        "testnet_snapshots_no_update",
    }
)
_REQUIRED_INDEXES = frozenset(
    {
        "testnet_actions_status_idx",
        "testnet_fills_order_idx",
        "testnet_orders_status_idx",
        "testnet_rate_window_idx",
    }
)
_REQUIRED_FOREIGN_KEYS: Mapping[str, frozenset[tuple[str, str, str]]] = {
    "testnet_orders": frozenset({("run_id", "testnet_runs", "run_id")}),
    "testnet_actions": frozenset(
        {
            ("run_id", "testnet_runs", "run_id"),
            ("run_id", "testnet_orders", "run_id"),
            ("order_id", "testnet_orders", "order_id"),
        }
    ),
    "testnet_action_rate_events": frozenset(
        {
            ("run_id", "testnet_actions", "run_id"),
            ("action_id", "testnet_actions", "action_id"),
        }
    ),
    "testnet_fills": frozenset(
        {
            ("run_id", "testnet_orders", "run_id"),
            ("order_id", "testnet_orders", "order_id"),
        }
    ),
    "testnet_fill_aggregates": frozenset(
        {
            ("run_id", "testnet_orders", "run_id"),
            ("order_id", "testnet_orders", "order_id"),
        }
    ),
    "testnet_remote_snapshots": frozenset(
        {("run_id", "testnet_runs", "run_id")}
    ),
    "testnet_audit_events": frozenset(
        {("run_id", "testnet_runs", "run_id")}
    ),
}


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _utc(value: datetime | None, *, label: str) -> datetime:
    candidate = _now() if value is None else value
    return parse_utc(utc_text(candidate), label=label)


def _sha256(value: str, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return value


def _identity(value: str, *, label: str) -> str:
    if not isinstance(value, str) or _IDENTITY_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a stable identifier")
    return value


def _secret_free(
    value: object,
    *,
    path: str = "$",
    field_name: str | None = None,
) -> None:
    if isinstance(value, Mapping):
        for raw_key, item in value.items():
            if not isinstance(raw_key, str):
                raise SecretPersistenceError(f"{path}: payload keys must be strings")
            normalized = raw_key.casefold().replace("-", "_")
            if any(marker in normalized for marker in _FORBIDDEN_KEY_MARKERS):
                raise SecretPersistenceError(
                    f"{path}.{raw_key}: secret-like fields are forbidden in persistence"
                )
            _secret_free(
                item,
                path=f"{path}.{raw_key}",
                field_name=normalized,
            )
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            _secret_free(item, path=f"{path}[{index}]", field_name=field_name)
        return
    if isinstance(value, (bytes, bytearray)):
        raise SecretPersistenceError(f"{path}: binary payloads are forbidden")
    if isinstance(value, str) and (
        _RAW_PRIVATE_KEY_RE.fullmatch(value) is not None
        or _RAW_SIGNATURE_RE.fullmatch(value) is not None
        or "BEGIN PRIVATE KEY" in value.upper()
    ):
        raise SecretPersistenceError(f"{path}: secret-like values are forbidden")
    if (
        isinstance(value, str)
        and _RAW_UNPREFIXED_SECRET_RE.fullmatch(value) is not None
        and not (
            field_name is not None
            and _SHA256_RE.fullmatch(value) is not None
            and field_name
            in (_PUBLIC_HEX_HASH_FIELDS | _PUBLIC_HEX_IDENTITY_FIELDS)
        )
    ):
        raise SecretPersistenceError(
            f"{path}: unprefixed secret-like 32-byte hex is forbidden"
        )


def _payload(value: Mapping[str, object], *, label: str) -> tuple[dict[str, object], str, str]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a mapping")
    _secret_free(value, path=label)
    try:
        text = canonical_json(value)
        decoded = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} must be canonical JSON data: {error}") from error
    if not isinstance(decoded, dict):
        raise ValueError(f"{label} must be a JSON object")
    return cast(dict[str, object], decoded), text, canonical_sha256(decoded)


def _decode(value: str, *, label: str) -> dict[str, object]:
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise IntegrityError(
            IntegrityReport("unknown", False, 0, ZERO_HASH, (IntegrityIssue("JSON", label),))
        )
    return cast(dict[str, object], decoded)


def _audit_genesis(run_id: str, config_hash: str) -> str:
    return canonical_sha256(
        {
            "config_hash": config_hash,
            "domain": "hyperliquid_testnet_audit_genesis_v1",
            "run_id": run_id,
        }
    )


def deterministic_action_id(
    run_id: str,
    kind: ActionKind | str,
    order_id: str | None,
    ordinal: int = 0,
) -> str:
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 0:
        raise ValueError("action ordinal must be a non-negative integer")
    return deterministic_id(
        "hyperliquid_testnet_action_v1",
        run_id,
        str(kind),
        order_id,
        ordinal,
    )


def _normalized_reconciliation_input(
    order_updates: Sequence[OrderProjectionUpdate],
    fills: Sequence[ReconciliationFill],
    action_resolutions: Sequence[ReconciliationActionResolution],
) -> dict[str, object]:
    normalized_updates: list[dict[str, object]] = []
    for update in order_updates:
        if not isinstance(update, OrderProjectionUpdate):
            raise TypeError("order_updates must contain OrderProjectionUpdate values")
        normalized_updates.append(
            {
                "average_fill_price": (
                    decimal_text(update.average_fill_price)
                    if update.average_fill_price is not None
                    else None
                ),
                "filled_quantity": (
                    decimal_text(update.filled_quantity)
                    if update.filled_quantity is not None
                    else None
                ),
                "order_id": update.order_id,
                "status": update.status.value,
                "venue_order_id": update.venue_order_id,
            }
        )
    normalized_fills: list[dict[str, object]] = []
    for fill in fills:
        if not isinstance(fill, ReconciliationFill):
            raise TypeError("fills must contain ReconciliationFill values")
        normalized_payload, _, payload_hash = _payload(
            fill.payload,
            label="reconciliation fill payload",
        )
        normalized_fills.append(
            {
                "fee": decimal_text(fill.fee),
                "fill_id": fill.fill_id,
                "order_id": fill.order_id,
                "payload": normalized_payload,
                "payload_hash": payload_hash,
                "price": decimal_text(fill.price),
                "quantity": decimal_text(fill.quantity),
                "received_at": utc_text(fill.received_at),
                "venue_order_id": fill.venue_order_id,
            }
        )
    normalized_resolutions: list[dict[str, object]] = []
    for resolution in action_resolutions:
        if not isinstance(resolution, ReconciliationActionResolution):
            raise TypeError(
                "action_resolutions must contain ReconciliationActionResolution values"
            )
        normalized_proof, _, proof_hash = _payload(
            resolution.proof,
            label="authoritative action resolution proof",
        )
        normalized_resolutions.append(
            {
                "action_id": resolution.action_id,
                "proof": normalized_proof,
                "proof_hash": proof_hash,
                "status": resolution.status.value,
            }
        )
    return {
        "action_resolutions": normalized_resolutions,
        "fills": normalized_fills,
        "order_updates": normalized_updates,
    }


def _reconciliation_input_hash(
    order_updates: Sequence[OrderProjectionUpdate],
    fills: Sequence[ReconciliationFill],
    action_resolutions: Sequence[ReconciliationActionResolution],
) -> str:
    return canonical_sha256(
        _normalized_reconciliation_input(order_updates, fills, action_resolutions)
    )


def _normalized_reconciliation_issues(
    issues: Sequence[ReconciliationIssue],
) -> tuple[dict[str, object], ...]:
    normalized: list[dict[str, object]] = []
    for issue in issues:
        if not isinstance(issue, ReconciliationIssue):
            raise TypeError("issues must contain ReconciliationIssue values")
        details, _, details_hash = _payload(
            issue.details,
            label="reconciliation issue details",
        )
        normalized.append(
            {
                "details": details,
                "reconciliation_issue_details_hash": details_hash,
                "issue_code": issue.issue_code,
            }
        )
    if not normalized:
        raise ValueError("a reconciliation failure requires at least one issue")
    return tuple(normalized)


def _acquire_os_lock(stream: BinaryIO, *, blocking: bool = False) -> None:
    stream.seek(0, os.SEEK_END)
    if stream.tell() == 0:
        stream.write(b"\0")
        stream.flush()
        os.fsync(stream.fileno())
    stream.seek(0)
    try:
        if os.name == "nt":
            import msvcrt

            mode = msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK
            msvcrt.locking(stream.fileno(), mode, 1)
        else:
            fcntl = importlib.import_module("fcntl")
            mode = fcntl.LOCK_EX
            if not blocking:
                mode |= fcntl.LOCK_NB
            fcntl.flock(
                stream.fileno(),
                mode,
            )
    except OSError as error:
        raise WalletLeaseError(
            "Testnet account/API-wallet scope is already held by another owner"
        ) from error


def _release_os_lock(stream: BinaryIO) -> None:
    if stream.closed:
        return
    stream.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        fcntl = importlib.import_module("fcntl")
        fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _fsync_directory(path: Path) -> None:
    """Persist directory entries where the host exposes a directory fsync."""

    if os.name == "nt":
        # CPython cannot portably open Windows directories for os.fsync().
        # Regular control files are still individually fsynced before this
        # explicit durability barrier call.
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


_WINDOWS_REPARSE_ATTRIBUTE = 0x00000400
_WINDOWS_FOREIGN_WRITE_MASK = (
    0x00000002
    | 0x00000004
    | 0x00000010
    | 0x00000040
    | 0x00000100
    | 0x00010000
    | 0x00040000
    | 0x00080000
    | 0x10000000
    | 0x40000000
)
_WINDOWS_SYSTEM_SID = "S-1-5-18"
_WINDOWS_ADMINISTRATORS_SID = "S-1-5-32-544"


def _windows_reparse_point(path: Path) -> bool:
    metadata = path.lstat()
    attributes = getattr(metadata, "st_file_attributes", 0)
    return path.is_symlink() or bool(attributes & _WINDOWS_REPARSE_ATTRIBUTE)


def _validate_windows_control_components(path: Path) -> None:
    anchor = Path(path.anchor)
    current = anchor
    try:
        relative = path.relative_to(anchor)
    except ValueError as error:
        raise WalletLeaseError("Windows Testnet control path has no stable anchor") from error
    for part in relative.parts:
        current /= part
        if not os.path.lexists(current):
            raise WalletLeaseError(
                "Windows Testnet control registry must be provisioned before use"
            )
        metadata = current.lstat()
        if _windows_reparse_point(current) or not stat.S_ISDIR(metadata.st_mode):
            raise WalletLeaseError(
                "Windows Testnet control path contains a reparse or non-directory component"
            )


def _windows_sid_text(advapi32: object, kernel32: object, sid: int) -> str:
    import ctypes

    converted = ctypes.c_void_p()
    if not advapi32.ConvertSidToStringSidW(  # type: ignore[attr-defined]
        ctypes.c_void_p(sid),
        ctypes.byref(converted),
    ):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        return ctypes.wstring_at(converted)
    finally:
        kernel32.LocalFree(converted)  # type: ignore[attr-defined]


def _inspect_windows_control_security(path: Path) -> _WindowsControlSecurity:
    import ctypes
    from ctypes import wintypes

    class _SidAndAttributes(ctypes.Structure):
        _fields_ = [("sid", ctypes.c_void_p), ("attributes", wintypes.DWORD)]

    class _TokenUser(ctypes.Structure):
        _fields_ = [("user", _SidAndAttributes)]

    class _AclSizeInformation(ctypes.Structure):
        _fields_ = [
            ("ace_count", wintypes.DWORD),
            ("acl_bytes_in_use", wintypes.DWORD),
            ("acl_bytes_free", wintypes.DWORD),
        ]

    class _AceHeader(ctypes.Structure):
        _fields_ = [
            ("ace_type", wintypes.BYTE),
            ("ace_flags", wintypes.BYTE),
            ("ace_size", wintypes.WORD),
        ]

    class _AccessAllowedAce(ctypes.Structure):
        _fields_ = [
            ("header", _AceHeader),
            ("mask", wintypes.DWORD),
            ("sid_start", wintypes.DWORD),
        ]

    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    advapi32.OpenProcessToken.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
    ]
    advapi32.OpenProcessToken.restype = wintypes.BOOL
    advapi32.GetTokenInformation.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
    ]
    advapi32.GetTokenInformation.restype = wintypes.BOOL
    advapi32.ConvertSidToStringSidW.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    advapi32.ConvertSidToStringSidW.restype = wintypes.BOOL
    advapi32.GetNamedSecurityInfoW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_void_p),
    ]
    advapi32.GetNamedSecurityInfoW.restype = wintypes.DWORD
    advapi32.GetAclInformation.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.c_int,
    ]
    advapi32.GetAclInformation.restype = wintypes.BOOL
    advapi32.GetAce.argtypes = [
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    advapi32.GetAce.restype = wintypes.BOOL
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    token = wintypes.HANDLE()
    if not advapi32.OpenProcessToken(
        kernel32.GetCurrentProcess(),
        0x0008,
        ctypes.byref(token),
    ):
        raise WalletLeaseError("cannot inspect the Windows Testnet execution SID")
    try:
        required = wintypes.DWORD()
        advapi32.GetTokenInformation(token, 1, None, 0, ctypes.byref(required))
        if required.value == 0:
            raise WalletLeaseError("Windows token user information is unavailable")
        token_buffer = ctypes.create_string_buffer(required.value)
        if not advapi32.GetTokenInformation(
            token,
            1,
            token_buffer,
            required,
            ctypes.byref(required),
        ):
            raise WalletLeaseError("Windows token user information cannot be read")
        token_user = ctypes.cast(
            token_buffer,
            ctypes.POINTER(_TokenUser),
        ).contents
        current_sid = _windows_sid_text(
            advapi32,
            kernel32,
            cast(int, token_user.user.sid),
        )
    finally:
        kernel32.CloseHandle(token)

    owner = ctypes.c_void_p()
    dacl = ctypes.c_void_p()
    descriptor = ctypes.c_void_p()
    result = advapi32.GetNamedSecurityInfoW(
        str(path),
        1,
        0x00000001 | 0x00000004,
        ctypes.byref(owner),
        None,
        ctypes.byref(dacl),
        None,
        ctypes.byref(descriptor),
    )
    if result != 0 or not owner.value or not descriptor.value:
        raise WalletLeaseError("Windows Testnet control security descriptor is unavailable")
    try:
        owner_sid = _windows_sid_text(
            advapi32,
            kernel32,
            owner.value,
        )
        if not dacl.value:
            return _WindowsControlSecurity(owner_sid, current_sid, False, ())
        acl_info = _AclSizeInformation()
        if not advapi32.GetAclInformation(
            dacl,
            ctypes.byref(acl_info),
            ctypes.sizeof(acl_info),
            2,
        ):
            raise WalletLeaseError("Windows Testnet control DACL cannot be inspected")
        allowed: list[tuple[str, int]] = []
        unsupported: list[int] = []
        for index in range(acl_info.ace_count):
            ace_pointer = ctypes.c_void_p()
            if not advapi32.GetAce(dacl, index, ctypes.byref(ace_pointer)):
                raise WalletLeaseError("Windows Testnet control ACE cannot be inspected")
            header = ctypes.cast(
                ace_pointer,
                ctypes.POINTER(_AceHeader),
            ).contents
            if header.ace_type == 1:
                continue
            if header.ace_type != 0:
                unsupported.append(int(header.ace_type))
                continue
            ace = ctypes.cast(
                ace_pointer,
                ctypes.POINTER(_AccessAllowedAce),
            ).contents
            if ace_pointer.value is None:
                raise WalletLeaseError("Windows Testnet control ACE pointer is invalid")
            sid_address = ace_pointer.value + _AccessAllowedAce.sid_start.offset
            allowed.append(
                (
                    _windows_sid_text(advapi32, kernel32, sid_address),
                    int(ace.mask),
                )
            )
        return _WindowsControlSecurity(
            owner_sid,
            current_sid,
            True,
            tuple(allowed),
            tuple(unsupported),
        )
    finally:
        kernel32.LocalFree(descriptor)


def _validate_windows_control_security(facts: _WindowsControlSecurity) -> None:
    trusted = {
        facts.current_sid,
        _WINDOWS_SYSTEM_SID,
        _WINDOWS_ADMINISTRATORS_SID,
    }
    if facts.owner_sid not in trusted:
        raise WalletLeaseError("Windows Testnet control owner SID is not trusted")
    if not facts.dacl_present:
        raise WalletLeaseError("Windows Testnet control registry has a null DACL")
    if facts.unsupported_ace_types:
        raise WalletLeaseError("Windows Testnet control DACL has unsupported ACE types")
    for sid, mask in facts.allowed_aces:
        if sid not in trusted and mask & _WINDOWS_FOREIGN_WRITE_MASK:
            raise WalletLeaseError(
                "Windows Testnet control DACL grants write/control rights to another SID"
            )


def _validate_windows_project_security(path: Path) -> None:
    if len(path.parents) < 2:
        raise WalletLeaseError("Windows Testnet control project path is incomplete")
    project_components = (path.parents[1], path.parent, path)
    for component in project_components:
        _validate_windows_control_security(
            _inspect_windows_control_security(component)
        )


def _replace_control_file(source: Path, target: Path) -> None:
    if os.name != "nt":
        os.replace(source, target)
        _fsync_directory(target.parent)
        return
    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    from ctypes import wintypes

    kernel32.MoveFileExW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.LPCWSTR,
        wintypes.DWORD,
    ]
    kernel32.MoveFileExW.restype = wintypes.BOOL
    if not kernel32.MoveFileExW(str(source), str(target), 0x00000001 | 0x00000008):
        raise ctypes.WinError(ctypes.get_last_error())


def _publish_control_file_exclusive(source: Path, target: Path) -> None:
    if os.name != "nt":
        try:
            os.link(source, target)
        except FileExistsError:
            raise
        source.unlink()
        _fsync_directory(target.parent)
        return
    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    from ctypes import wintypes

    kernel32.MoveFileExW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.LPCWSTR,
        wintypes.DWORD,
    ]
    kernel32.MoveFileExW.restype = wintypes.BOOL
    if not kernel32.MoveFileExW(str(source), str(target), 0x00000008):
        error = ctypes.get_last_error()
        if error in {80, 183}:
            raise FileExistsError(str(target))
        raise ctypes.WinError(error)


def _default_control_root() -> Path:
    if os.name == "nt":
        import ctypes

        buffer = ctypes.create_unicode_buffer(32768)
        loader = getattr(ctypes, "windll", None)
        shell32 = getattr(loader, "shell32", None)
        resolver = getattr(shell32, "SHGetFolderPathW", None)
        if resolver is None or resolver(None, 0x0023, None, 0, buffer) != 0:
            raise WalletLeaseError(
                "Windows Common Application Data is unavailable for Testnet control"
            )
        common_data = Path(buffer.value)
        root = common_data / "HyperLab" / "TestnetExecutor" / "control-v1"
    else:
        root = Path("/var/lib/hyperlab/testnet-executor/control-v1")
    if not root.is_absolute():
        raise WalletLeaseError("durable Testnet control registry path must be absolute")
    return root


class TestnetStore:
    def __init__(
        self,
        path: Path | str,
        *,
        timeout_seconds: float = 30.0,
        lease_root: Path | str | None = None,
        owner_id: str | None = None,
    ) -> None:
        self.path = Path(path)
        self._timeout_seconds = timeout_seconds
        self._read_only = False
        self._production_control_root = lease_root is None
        # lease_root is an internal synthetic-test/deployment-provisioning seam.
        # Normal commands never expose it and always use the OS-pinned registry.
        self._lease_root = (
            Path(lease_root)
            if lease_root is not None
            else _default_control_root()
        )
        if not self._lease_root.is_absolute():
            raise WalletLeaseError("Testnet control registry path must be absolute")
        self._owner_id = _identity(
            owner_id or f"owner-{os.getpid()}-{uuid.uuid4().hex}",
            label="wallet lease owner_id",
        )
        self._lease_guard = threading.RLock()
        self._wallet_leases: dict[str, _WalletLeaseHandle] = {}
        self.initialize()

    @classmethod
    def open_existing_readonly(
        cls,
        path: Path | str,
        *,
        timeout_seconds: float = 30.0,
    ) -> Self:
        """Open an existing store without creating directories, files, or journals."""

        target = Path(path)
        if not target.is_file():
            raise FileNotFoundError(f"Testnet store does not exist: {target}")
        instance = cls.__new__(cls)
        instance.path = target
        instance._timeout_seconds = timeout_seconds
        instance._read_only = True
        instance._production_control_root = False
        instance._lease_root = target.parent
        instance._owner_id = "readonly"
        instance._lease_guard = threading.RLock()
        instance._wallet_leases = {}
        with instance._read_connection() as connection:
            instance._verify_schema(connection)
        return instance

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.close()

    def close(self) -> None:
        """Release every OS-held wallet lease; SQLite connections are short-lived."""

        with self._lease_guard:
            for run_id in tuple(self._wallet_leases):
                self.release_wallet_lease(run_id)

    @staticmethod
    def _wallet_scope_from_run(run: RunRecord) -> str:
        account = run.config.get("account_address")
        environment = run.config.get("environment")
        if (
            not isinstance(account, str)
            or environment != "TESTNET"
        ):
            raise RunConflictError("durable run lacks an exact Testnet account scope")
        return canonical_sha256(
            {
                "account_address": account,
                "environment": environment,
            }
        )

    @staticmethod
    def _api_wallet_scope_from_run(run: RunRecord) -> str:
        api_wallet = run.config.get("api_wallet_address")
        environment = run.config.get("environment")
        if not isinstance(api_wallet, str) or environment != "TESTNET":
            raise RunConflictError(
                "durable run lacks an exact Testnet API-wallet nonce scope"
            )
        return canonical_sha256(
            {
                "api_wallet_address": api_wallet,
                "environment": environment,
            }
        )

    def _wallet_nonce_lock_path(self, run: RunRecord) -> Path:
        scope_hash = self._api_wallet_scope_from_run(run)
        return self._lease_root / f"{scope_hash}.nonce.lock"

    def _wallet_nonce_path(self, run: RunRecord) -> Path:
        scope_hash = self._api_wallet_scope_from_run(run)
        return self._lease_root / f"{scope_hash}.nonce.json"

    def _account_rate_lock_path(self, run: RunRecord) -> Path:
        return self._lease_root / f"{self._wallet_scope_from_run(run)}.rate.lock"

    def _account_rate_path(self, run: RunRecord) -> Path:
        return self._lease_root / f"{self._wallet_scope_from_run(run)}.rate.json"

    @staticmethod
    def _rate_limit_for_run(run: RunRecord, kind: ActionKind) -> int:
        key_by_kind = {
            ActionKind.SUBMIT: "submit_requests_per_minute",
            ActionKind.CANCEL: "cancel_requests_per_minute",
            ActionKind.REPLACE: "replace_requests_per_minute",
        }
        key = key_by_kind.get(kind)
        if key is None:
            raise ValueError(f"{kind.value} does not use the ordinary account rate lane")
        risk_limits = run.config.get("risk_limits")
        if not isinstance(risk_limits, Mapping):
            raise RunConflictError("durable run lacks exact Testnet risk limits")
        value = risk_limits.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise RunConflictError(f"durable run has invalid {key}")
        return value

    @staticmethod
    def _decode_account_rate_entries(
        raw_entries: object,
        *,
        label: str,
        maximum: int,
    ) -> tuple[_AccountRateEvent, ...]:
        if not isinstance(raw_entries, list) or len(raw_entries) > maximum:
            raise WalletLeaseError(f"account rate {label} count is invalid")
        events: list[_AccountRateEvent] = []
        identities: set[str] = set()
        for raw_event in raw_entries:
            if not isinstance(raw_event, dict) or set(raw_event) != {
                "action_id",
                "action_kind",
                "occurred_at",
            }:
                raise WalletLeaseError(f"account rate {label} fields are invalid")
            event = cast(dict[str, object], raw_event)
            action_id = _sha256(str(event["action_id"]), label="rate action_id")
            kind = ActionKind(str(event["action_kind"]))
            if kind not in {ActionKind.SUBMIT, ActionKind.CANCEL, ActionKind.REPLACE}:
                raise WalletLeaseError(f"account rate {label} has an invalid action kind")
            occurred_at = parse_utc(
                str(event["occurred_at"]),
                label=f"account rate {label} occurred_at",
            )
            if action_id in identities:
                raise WalletLeaseError(f"account rate {label} contains duplicate action ids")
            identities.add(action_id)
            events.append(_AccountRateEvent(action_id, kind, occurred_at))
        expected = sorted(
            events,
            key=lambda item: (item.occurred_at, item.action_id),
        )
        if events != expected:
            raise WalletLeaseError(f"account rate {label} is not canonically ordered")
        return tuple(events)

    def _read_account_rate_ledger(
        self,
        run: RunRecord,
    ) -> tuple[tuple[_AccountRateEvent, ...], tuple[_AccountRateEvent, ...]]:
        path = self._account_rate_path(run)
        if not os.path.lexists(path):
            return (), ()
        try:
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise WalletLeaseError("account rate ledger is not a regular file")
            raw = path.read_bytes()
            decoded = json.loads(raw.decode("utf-8"))
            if not isinstance(decoded, dict):
                raise WalletLeaseError("account rate ledger is not an object")
            record = cast(dict[str, object], decoded)
            if set(record) != {
                "account_scope_hash",
                "action_identities",
                "events",
                "record_hash",
                "schema_version",
                "updated_at",
            }:
                raise WalletLeaseError("account rate ledger fields are invalid")
            raw_identities = record["action_identities"]
            raw_events = record["events"]
            core = {
                "account_scope_hash": record["account_scope_hash"],
                "action_identities": raw_identities,
                "events": raw_events,
                "schema_version": record["schema_version"],
                "updated_at": record["updated_at"],
            }
            if (
                record["schema_version"] != 1
                or record["account_scope_hash"] != self._wallet_scope_from_run(run)
                or not isinstance(record["record_hash"], str)
                or record["record_hash"] != canonical_sha256(core)
                or canonical_json(record).encode("utf-8") != raw
            ):
                raise WalletLeaseError("account rate ledger integrity is invalid")
            parse_utc(str(record["updated_at"]), label="account rate updated_at")
            events = self._decode_account_rate_entries(
                raw_events,
                label="event",
                maximum=1_024,
            )
            identities = self._decode_account_rate_entries(
                raw_identities,
                label="action identity",
                maximum=ACCOUNT_ACTION_IDENTITY_CAPACITY,
            )
            identity_by_id = {item.action_id: item for item in identities}
            if any(identity_by_id.get(event.action_id) != event for event in events):
                raise WalletLeaseError(
                    "account rate event is not bound to its durable action identity"
                )
            return events, identities
        except WalletLeaseError:
            raise
        except (OSError, UnicodeDecodeError, ValueError, TypeError) as error:
            raise WalletLeaseError("account rate ledger cannot be verified") from error

    def _write_account_rate_ledger(
        self,
        run: RunRecord,
        events: Sequence[_AccountRateEvent],
        identities: Sequence[_AccountRateEvent],
        *,
        updated_at: datetime,
    ) -> None:
        serialized_events = [
            {
                "action_id": event.action_id,
                "action_kind": event.kind.value,
                "occurred_at": utc_text(event.occurred_at),
            }
            for event in events
        ]
        serialized_identities = [
            {
                "action_id": event.action_id,
                "action_kind": event.kind.value,
                "occurred_at": utc_text(event.occurred_at),
            }
            for event in identities
        ]
        core: dict[str, object] = {
            "account_scope_hash": self._wallet_scope_from_run(run),
            "action_identities": serialized_identities,
            "events": serialized_events,
            "schema_version": 1,
            "updated_at": utc_text(updated_at),
        }
        record = {**core, "record_hash": canonical_sha256(core)}
        _secret_free(record, path="account rate ledger")
        payload = canonical_json(record).encode("utf-8")
        target = self._account_rate_path(run)
        if os.path.lexists(target):
            metadata = target.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise WalletLeaseError("account rate ledger target is unsafe")
        temporary = self._lease_root / (
            f".{self._wallet_scope_from_run(run)}."
            f"{canonical_sha256({'owner_id': self._owner_id})[:16]}."
            f"{uuid.uuid4().hex}.rate.tmp"
        )
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(temporary, flags, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            _replace_control_file(temporary, target)
        finally:
            if os.path.lexists(temporary):
                metadata = temporary.lstat()
                if stat.S_ISREG(metadata.st_mode) and not stat.S_ISLNK(
                    metadata.st_mode
                ):
                    temporary.unlink()

    @staticmethod
    def _rate_burn_fault_point(
        point: str,
        kind: ActionKind,
        action_id: str,
    ) -> None:
        del point, kind, action_id

    def _burn_account_rate_event(
        self,
        run: RunRecord,
        *,
        action_id: str,
        kind: ActionKind,
        occurred_at: datetime,
    ) -> tuple[int, int, bool, int]:
        limit = self._rate_limit_for_run(run, kind)
        lock = self._open_control_lock(self._account_rate_lock_path(run))
        locked = False
        try:
            _acquire_os_lock(lock, blocking=True)
            locked = True
            events, identities = self._read_account_rate_ledger(run)
            threshold = occurred_at - timedelta(minutes=1)
            recent = tuple(event for event in events if event.occurred_at >= threshold)
            existing = next(
                (event for event in identities if event.action_id == action_id),
                None,
            )
            if existing is not None:
                if existing.kind is not kind:
                    raise RunConflictError(
                        "account rate action identity has a divergent action kind"
                    )
                raise AmbiguousActionReplayError(
                    "account action identity was already burned; never reserve or resend"
                )
            observed = sum(event.kind is kind for event in recent)
            if observed >= limit:
                raise RunConflictError(
                    f"account-scoped {kind.value.lower()} request rate limit reached"
                )
            if len(identities) >= ACCOUNT_ACTION_IDENTITY_CAPACITY:
                raise RunConflictError(
                    "account action identity ledger reached its compiled capacity"
                )
            new_identity = _AccountRateEvent(action_id, kind, occurred_at)
            appended = tuple(
                sorted(
                    (*recent, new_identity),
                    key=lambda item: (item.occurred_at, item.action_id),
                )
            )
            appended_identities = tuple(
                sorted(
                    (*identities, new_identity),
                    key=lambda item: (item.occurred_at, item.action_id),
                )
            )
            prior_updated = max(
                (event.occurred_at for event in events),
                default=occurred_at,
            )
            self._write_account_rate_ledger(
                run,
                appended,
                appended_identities,
                updated_at=max(prior_updated, occurred_at),
            )
            self._rate_burn_fault_point(
                "AFTER_GLOBAL_RATE_BURN_BEFORE_SQLITE",
                kind,
                action_id,
            )
            return observed + 1, limit, True, len(appended_identities)
        finally:
            try:
                if locked:
                    _release_os_lock(lock)
            finally:
                lock.close()

    def _read_wallet_nonce_watermark(self, run: RunRecord) -> int:
        path = self._wallet_nonce_path(run)
        if not os.path.lexists(path):
            return 0
        try:
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise WalletLeaseError(
                    "API-wallet nonce watermark is not a regular file"
                )
            raw = path.read_bytes()
            decoded = json.loads(raw.decode("utf-8"))
            if not isinstance(decoded, dict):
                raise WalletLeaseError("API-wallet nonce watermark is malformed")
            expected_keys = {
                "burned_at",
                "last_nonce",
                "record_hash",
                "wallet_scope_hash",
            }
            if set(decoded) != expected_keys:
                raise WalletLeaseError("API-wallet nonce watermark fields differ")
            core = {
                "burned_at": decoded["burned_at"],
                "last_nonce": decoded["last_nonce"],
                "wallet_scope_hash": decoded["wallet_scope_hash"],
            }
            last_nonce = decoded["last_nonce"]
            valid = (
                isinstance(last_nonce, int)
                and not isinstance(last_nonce, bool)
                and last_nonce > 0
                and isinstance(decoded["record_hash"], str)
                and decoded["record_hash"] == canonical_sha256(core)
                and decoded["wallet_scope_hash"]
                == self._api_wallet_scope_from_run(run)
                and canonical_json(decoded).encode("utf-8") == raw
            )
            parse_utc(str(decoded["burned_at"]), label="wallet nonce burned_at")
            if not valid:
                raise WalletLeaseError("API-wallet nonce watermark is invalid")
            return cast(int, last_nonce)
        except WalletLeaseError:
            raise
        except (OSError, UnicodeDecodeError, ValueError, TypeError) as error:
            raise WalletLeaseError(
                "API-wallet nonce watermark cannot be verified"
            ) from error

    def _burn_wallet_nonce(
        self,
        run: RunRecord,
        *,
        at: datetime,
        minimum_nonce: int | None,
    ) -> tuple[int, int]:
        if minimum_nonce is not None and (
            isinstance(minimum_nonce, bool)
            or not isinstance(minimum_nonce, int)
            or minimum_nonce <= 0
        ):
            raise ValueError("minimum_nonce must be a positive integer")
        self._ensure_control_root()
        lock = self._open_control_lock(self._wallet_nonce_lock_path(run))
        locked = False
        temporary: Path | None = None
        try:
            _acquire_os_lock(lock, blocking=True)
            locked = True
            previous = self._read_wallet_nonce_watermark(run)
            clock_floor = int(at.timestamp() * 1000)
            nonce = max(previous + 1, clock_floor, minimum_nonce or 1)
            core: dict[str, object] = {
                "burned_at": utc_text(at),
                "last_nonce": nonce,
                "wallet_scope_hash": self._api_wallet_scope_from_run(run),
            }
            record = {**core, "record_hash": canonical_sha256(core)}
            _secret_free(record, path="wallet nonce watermark")
            payload = canonical_json(record).encode("utf-8")
            target = self._wallet_nonce_path(run)
            if os.path.lexists(target):
                target_metadata = target.lstat()
                if stat.S_ISLNK(target_metadata.st_mode) or not stat.S_ISREG(
                    target_metadata.st_mode
                ):
                    raise WalletLeaseError(
                        "API-wallet nonce watermark target is unsafe"
                    )
            temporary = self._lease_root / (
                f".{self._api_wallet_scope_from_run(run)}."
                f"{canonical_sha256({'owner_id': self._owner_id})[:16]}."
                f"{uuid.uuid4().hex}.nonce.tmp"
            )
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(temporary, flags, 0o600)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
            except BaseException:
                raise
            _replace_control_file(temporary, target)
            temporary = None
            return nonce, previous
        finally:
            if temporary is not None and os.path.lexists(temporary):
                metadata = temporary.lstat()
                if stat.S_ISREG(metadata.st_mode) and not stat.S_ISLNK(
                    metadata.st_mode
                ):
                    temporary.unlink()
            try:
                if locked:
                    _release_os_lock(lock)
            finally:
                lock.close()

    @staticmethod
    def _write_lease_metadata(
        stream: BinaryIO,
        record: WalletLeaseRecord,
    ) -> None:
        metadata: dict[str, object] = {
            "acquired_at": utc_text(record.acquired_at),
            "owner_id": record.owner_id,
            "pid": os.getpid(),
            "renewed_at": utc_text(record.renewed_at),
            "run_id": record.run_id,
            "wallet_scope_hash": record.wallet_scope_hash,
        }
        _secret_free(metadata, path="wallet lease metadata")
        payload = canonical_json(metadata).encode("utf-8")
        stream.seek(0)
        stream.write(payload)
        stream.truncate()
        stream.flush()
        os.fsync(stream.fileno())
        stream.seek(0)

    def acquire_wallet_lease(
        self,
        run_id: str,
        *,
        acquired_at: datetime | None = None,
    ) -> WalletLeaseRecord:
        """Acquire the process-held single writer lease for an account/API wallet."""

        if self._read_only:
            raise TestnetStoreError("read-only TestnetStore cannot acquire a writer lease")
        normalized = _sha256(run_id, label="run_id")
        at = _utc(acquired_at, label="wallet lease acquired_at")
        run = self.get_run(normalized)
        scope_hash = self._wallet_scope_from_run(run)
        with self._lease_guard:
            existing = self._wallet_leases.get(normalized)
            if existing is not None:
                return existing.record
            for handle in self._wallet_leases.values():
                if handle.record.wallet_scope_hash == scope_hash:
                    raise WalletLeaseError(
                        "this store owner already holds the wallet scope for another run"
                    )
            lock_path = self._lease_root / f"{scope_hash}.lock"
            stream = self._open_control_lock(lock_path)
            try:
                _acquire_os_lock(stream)
                record = WalletLeaseRecord(
                    run_id=normalized,
                    wallet_scope_hash=scope_hash,
                    owner_id=self._owner_id,
                    acquired_at=at,
                    renewed_at=at,
                    lock_path=lock_path,
                )
                self._write_lease_metadata(stream, record)
            except BaseException:
                stream.close()
                raise
            self._wallet_leases[normalized] = _WalletLeaseHandle(stream, record)
            return record

    def wallet_lease(self, run_id: str) -> WalletLeaseRecord | None:
        normalized = _sha256(run_id, label="run_id")
        with self._lease_guard:
            handle = self._wallet_leases.get(normalized)
            return handle.record if handle is not None else None

    def renew_wallet_lease(
        self,
        run_id: str,
        *,
        renewed_at: datetime | None = None,
    ) -> WalletLeaseRecord:
        normalized = _sha256(run_id, label="run_id")
        at = _utc(renewed_at, label="wallet lease renewed_at")
        with self._lease_guard:
            handle = self._wallet_leases.get(normalized)
            if handle is None or handle.stream.closed:
                raise WalletLeaseError("Testnet wallet lease is not held by this owner")
            if at < handle.record.renewed_at:
                raise WalletLeaseError("wallet lease renewal timestamp cannot move backwards")
            record = WalletLeaseRecord(
                run_id=handle.record.run_id,
                wallet_scope_hash=handle.record.wallet_scope_hash,
                owner_id=handle.record.owner_id,
                acquired_at=handle.record.acquired_at,
                renewed_at=at,
                lock_path=handle.record.lock_path,
            )
            self._write_lease_metadata(handle.stream, record)
            handle.record = record
            return record

    def release_wallet_lease(self, run_id: str) -> None:
        normalized = _sha256(run_id, label="run_id")
        with self._lease_guard:
            handle = self._wallet_leases.pop(normalized, None)
            if handle is None:
                return
            try:
                _release_os_lock(handle.stream)
            finally:
                handle.stream.close()

    def _require_wallet_lease(self, run_id: str) -> WalletLeaseRecord:
        with self._lease_guard:
            handle = self._wallet_leases.get(run_id)
            if handle is None or handle.stream.closed:
                raise WalletLeaseError(
                    "active Testnet operations require the account/API-wallet writer lease"
                )
            return handle.record

    def _account_kill_path(self, run: RunRecord) -> Path:
        scope_hash = self._wallet_scope_from_run(run)
        return self._lease_root / f"{scope_hash}.killed.json"

    def _ensure_control_root(self) -> None:
        if self._production_control_root:
            if not os.path.lexists(self._lease_root):
                raise WalletLeaseError(
                    "production Testnet control registry must be provisioned before use"
                )
        else:
            self._lease_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        metadata = self._lease_root.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise WalletLeaseError(
                "Testnet control registry must be a real protected directory"
            )
        if os.name == "nt":
            _validate_windows_control_components(self._lease_root)
            if self._production_control_root:
                _validate_windows_project_security(self._lease_root)
        elif metadata.st_mode & 0o077:
            raise WalletLeaseError(
                "Testnet control registry permissions must exclude group/other access"
            )

    def _open_control_lock(self, path: Path) -> BinaryIO:
        self._ensure_control_root()
        if os.path.lexists(path):
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(
                metadata.st_mode
            ):
                raise WalletLeaseError("Testnet control lock path is not a regular file")
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags, 0o600)
        return os.fdopen(descriptor, "r+b")

    def _account_kill_latched_run(self, run: RunRecord) -> bool:
        kill_path = self._account_kill_path(run)
        if not os.path.lexists(kill_path):
            return False
        try:
            metadata = kill_path.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                return True
            raw = kill_path.read_bytes()
            decoded = json.loads(raw.decode("utf-8"))
            if not isinstance(decoded, dict):
                return True
            expected_keys = {
                "killed_at",
                "reason",
                "record_hash",
                "run_id",
                "wallet_scope_hash",
            }
            if set(decoded) != expected_keys:
                return True
            core = {
                "killed_at": decoded["killed_at"],
                "reason": decoded["reason"],
                "run_id": decoded["run_id"],
                "wallet_scope_hash": decoded["wallet_scope_hash"],
            }
            valid = (
                isinstance(decoded["record_hash"], str)
                and decoded["record_hash"] == canonical_sha256(core)
                and decoded["wallet_scope_hash"] == self._wallet_scope_from_run(run)
                and canonical_json(decoded).encode("utf-8") == raw
                and isinstance(decoded["reason"], str)
                and _REASON_CODE_RE.fullmatch(decoded["reason"]) is not None
            )
            if not valid:
                return True
            return bool(decoded)
        except (OSError, UnicodeDecodeError, ValueError, TypeError):
            return True

    def account_kill_latched(self, run_id: str) -> bool:
        normalized = _sha256(run_id, label="run_id")
        return self._account_kill_latched_run(self.get_run(normalized))

    def _account_send_gate_path(self, run: RunRecord) -> Path:
        scope_hash = self._wallet_scope_from_run(run)
        return self._lease_root / f"{scope_hash}.send-gate.lock"

    @contextmanager
    def final_send_permit(
        self,
        run_id: str,
        action_id: str,
    ) -> Iterator[ActionAttemptRecord]:
        """Serialize the final network-send decision against account kill."""

        normalized_run = _sha256(run_id, label="run_id")
        normalized_action = _sha256(action_id, label="action_id")
        lease = self._require_wallet_lease(normalized_run)
        run = self.get_run(normalized_run)
        gate = self._open_control_lock(self._account_send_gate_path(run))
        locked = False
        try:
            _acquire_os_lock(gate, blocking=True)
            locked = True
            # Re-read every authority only after the kill/send serialization point.
            run = self.get_run(normalized_run)
            action = self.get_action(normalized_run, normalized_action)
            if action.status is not ActionAttemptStatus.AMBIGUOUS:
                raise RunConflictError("final send requires an AMBIGUOUS durable action")
            if action.response is not None:
                raise RunConflictError(
                    "an observed ambiguous response must be reconciled, never resent"
                )
            if action.kind in {ActionKind.SUBMIT, ActionKind.REPLACE}:
                if self._account_kill_latched_run(run):
                    raise RunConflictError(
                        "durable account-scoped Testnet kill latch blocks final send"
                    )
                if run.runtime_state is not RuntimeState.RUNNING:
                    raise RunConflictError(
                        f"runtime state {run.runtime_state.value} blocks final send"
                    )
                if (
                    run.last_reconciled_at is None
                    or run.last_reconciled_at < lease.acquired_at
                ):
                    raise RunConflictError(
                        "final send requires reconciliation after wallet lease acquisition"
                    )
            yield action
        finally:
            try:
                if locked:
                    _release_os_lock(gate)
            finally:
                gate.close()

    def _latch_account_kill(
        self,
        run: RunRecord,
        *,
        reason: str,
        killed_at: datetime,
    ) -> None:
        kill_path = self._account_kill_path(run)
        gate = self._open_control_lock(self._account_send_gate_path(run))
        locked = False
        temporary: Path | None = None
        try:
            _acquire_os_lock(gate, blocking=True)
            locked = True
            core: dict[str, object] = {
                "killed_at": utc_text(killed_at),
                "reason": reason,
                "run_id": run.run_id,
                "wallet_scope_hash": self._wallet_scope_from_run(run),
            }
            metadata = {**core, "record_hash": canonical_sha256(core)}
            _secret_free(metadata, path="account kill metadata")
            payload = canonical_json(metadata).encode("utf-8")
            if os.path.lexists(kill_path):
                return
            temporary = self._lease_root / (
                f".{self._wallet_scope_from_run(run)}."
                f"{uuid.uuid4().hex}.kill.tmp"
            )
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            try:
                descriptor = os.open(
                    temporary,
                    flags,
                    0o600,
                )
            except FileExistsError:
                return
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                _publish_control_file_exclusive(temporary, kill_path)
                temporary = None
                _fsync_directory(kill_path.parent)
            except FileExistsError:
                return
            except BaseException:
                # Partial latch presence remains authoritative and fails closed.
                raise
        finally:
            if temporary is not None and os.path.lexists(temporary):
                temporary_metadata = temporary.lstat()
                if stat.S_ISREG(temporary_metadata.st_mode) and not stat.S_ISLNK(
                    temporary_metadata.st_mode
                ):
                    temporary.unlink()
            try:
                if locked:
                    _release_os_lock(gate)
            finally:
                gate.close()


    def _connect(self) -> sqlite3.Connection:
        if self._read_only:
            raise TestnetStoreError("read-only TestnetStore cannot mutate durable state")
        connection = sqlite3.connect(
            self.path,
            timeout=self._timeout_seconds,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute(f"PRAGMA busy_timeout={int(self._timeout_seconds * 1000)}")
        return connection

    def _read_connection(self) -> sqlite3.Connection:
        uri = f"{self.path.resolve().as_uri()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA query_only=ON")
        return connection

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {0, SCHEMA_VERSION}:
                raise SchemaVersionError(
                    f"Testnet store schema {version} is not supported by {SCHEMA_VERSION}"
                )
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            if version == 0:
                if tables:
                    raise SchemaVersionError(
                        "Testnet store has tables but no recognized schema version"
                    )
                created = utc_text(_now()).replace("'", "''")
                connection.executescript(
                    "BEGIN IMMEDIATE;\n"
                    + _SCHEMA_SQL
                    + f"\nINSERT INTO testnet_schema VALUES (1, {SCHEMA_VERSION}, '{created}');\n"
                    + f"PRAGMA user_version={SCHEMA_VERSION};\nCOMMIT;"
                )
            self._verify_schema(connection)

    @staticmethod
    def _verify_schema(connection: sqlite3.Connection) -> None:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        row = connection.execute(
            "SELECT version FROM testnet_schema WHERE singleton=1"
        ).fetchone()
        if version != SCHEMA_VERSION or row is None or int(row[0]) != SCHEMA_VERSION:
            raise SchemaVersionError("Testnet schema metadata is inconsistent")
        if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
            raise SchemaVersionError("Testnet store requires foreign_keys=ON")
        if int(connection.execute("PRAGMA synchronous").fetchone()[0]) < 2:
            raise SchemaVersionError("Testnet store requires synchronous=FULL")

    @staticmethod
    def _require_run(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM testnet_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise RunNotFoundError(f"unknown Testnet run {run_id!r}")
        return cast(sqlite3.Row, row)

    def _append_audit(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        event_type: str,
        payload: Mapping[str, object],
        *,
        created_at: datetime,
    ) -> AuditEventRecord:
        event_name = _identity(event_type, label="audit event_type")
        normalized, payload_json, payload_hash = _payload(payload, label="audit payload")
        run = self._require_run(connection, run_id)
        sequence = int(run["audit_count"]) + 1
        previous_hash = str(run["audit_head_hash"])
        created_text = utc_text(created_at)
        event_id = deterministic_id(
            "hyperliquid_testnet_audit_event_v1",
            run_id,
            sequence,
            event_name,
            payload_hash,
        )
        event_hash = canonical_sha256(
            {
                "created_at": created_text,
                "event_id": event_id,
                "event_type": event_name,
                "payload_hash": payload_hash,
                "previous_hash": previous_hash,
                "run_id": run_id,
                "sequence": sequence,
            }
        )
        connection.execute(
            """
            INSERT INTO testnet_audit_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                sequence,
                event_id,
                event_name,
                payload_json,
                payload_hash,
                previous_hash,
                event_hash,
                created_text,
            ),
        )
        connection.execute(
            """
            UPDATE testnet_runs
            SET audit_count=?, audit_head_hash=?, updated_at=? WHERE run_id=?
            """,
            (sequence, event_hash, created_text, run_id),
        )
        return AuditEventRecord(
            run_id,
            sequence,
            event_id,
            event_name,
            normalized,
            payload_hash,
            previous_hash,
            event_hash,
            created_at,
        )

    def append_audit(
        self,
        run_id: str,
        event_type: str,
        payload: Mapping[str, object],
        *,
        created_at: datetime | None = None,
    ) -> AuditEventRecord:
        """Append a public, secret-screened event to the durable hash chain."""

        normalized = _sha256(run_id, label="run_id")
        at = _utc(created_at, label="audit created_at")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                event = self._append_audit(
                    connection,
                    normalized,
                    event_type,
                    payload,
                    created_at=at,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return event

    def record_reconciliation_issue(
        self,
        run_id: str,
        issue_code: str,
        *,
        details: Mapping[str, object],
        detected_at: datetime | None = None,
        latch_manual_review: bool = True,
    ) -> AuditEventRecord:
        """Persist a reconciliation discrepancy and optionally latch execution."""

        normalized = _sha256(run_id, label="run_id")
        code = _identity(issue_code, label="reconciliation issue_code")
        if not isinstance(latch_manual_review, bool):
            raise TypeError("latch_manual_review must be a boolean")
        normalized_details, _, _ = _payload(
            details,
            label="reconciliation issue details",
        )
        at = _utc(detected_at, label="reconciliation issue detected_at")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                run = self._require_run(connection, normalized)
                current = RuntimeState(str(run["runtime_state"]))
                latched = latch_manual_review and current not in {
                    RuntimeState.KILLED,
                    RuntimeState.MANUAL_REVIEW,
                }
                if latched:
                    connection.execute(
                        """
                        UPDATE testnet_runs
                        SET runtime_state=?, state_reason=?, updated_at=?
                        WHERE run_id=?
                        """,
                        (
                            RuntimeState.MANUAL_REVIEW.value,
                            "RECONCILIATION_ISSUE",
                            utc_text(at),
                            normalized,
                        ),
                    )
                event = self._append_audit(
                    connection,
                    normalized,
                    "RECONCILIATION_ISSUE",
                    {
                        "details": normalized_details,
                        "issue_code": code,
                        "manual_review_latched": latched,
                    },
                    created_at=at,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return event

    def create_run(
        self,
        config: TestnetConfig,
        *,
        created_at: datetime | None = None,
    ) -> RunRecord:
        if not isinstance(config, TestnetConfig):
            raise TypeError("config must be a TestnetConfig")
        at = _utc(created_at, label="created_at")
        config_payload, config_json, config_hash = _payload(
            config.to_dict(),
            label="Testnet config",
        )
        if config_hash != config.config_hash:
            raise RunConflictError("Testnet config hash differs from canonical snapshot")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT * FROM testnet_runs WHERE run_id=?",
                    (config.run_id,),
                ).fetchone()
                if existing is not None:
                    if (
                        str(existing["config_json"]) != config_json
                        or str(existing["config_hash"]) != config_hash
                    ):
                        raise RunConflictError(
                            "Testnet run identity has a different immutable configuration"
                        )
                    connection.rollback()
                    return self.get_run(config.run_id)
                genesis = _audit_genesis(config.run_id, config_hash)
                timestamp = utc_text(at)
                connection.execute(
                    """
                    INSERT INTO testnet_runs
                    VALUES (?, ?, ?, ?, NULL, 0, NULL, NULL, NULL, 0, ?, ?, ?)
                    """,
                    (
                        config.run_id,
                        config_json,
                        config_hash,
                        RuntimeState.STOPPED.value,
                        genesis,
                        timestamp,
                        timestamp,
                    ),
                )
                self._append_audit(
                    connection,
                    config.run_id,
                    "RUN_CREATED",
                    {
                        "config_hash": config_hash,
                        "environment": config.environment,
                        "purpose": config.purpose,
                    },
                    created_at=at,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        del config_payload
        return self.get_run(config.run_id)

    @staticmethod
    def _run_record(row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            run_id=str(row["run_id"]),
            config_hash=str(row["config_hash"]),
            config=_decode(str(row["config_json"]), label="run config"),
            runtime_state=RuntimeState(str(row["runtime_state"])),
            state_reason=cast(str | None, row["state_reason"]),
            last_nonce=int(row["last_nonce"]),
            last_reconciled_at=(
                parse_utc(str(row["last_reconciled_at"]))
                if row["last_reconciled_at"] is not None
                else None
            ),
            reconciliation_snapshot_hash=cast(
                str | None,
                row["reconciliation_snapshot_hash"],
            ),
            reconciliation_snapshot_id=cast(
                str | None,
                row["reconciliation_snapshot_id"],
            ),
            audit_count=int(row["audit_count"]),
            audit_head_hash=str(row["audit_head_hash"]),
            created_at=parse_utc(str(row["created_at"])),
            updated_at=parse_utc(str(row["updated_at"])),
        )

    def get_run(self, run_id: str) -> RunRecord:
        normalized = _sha256(run_id, label="run_id")
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM testnet_runs WHERE run_id=?",
                (normalized,),
            ).fetchone()
        if row is None:
            raise RunNotFoundError(f"unknown Testnet run {normalized!r}")
        return self._run_record(row)

    def list_runs(self) -> tuple[str, ...]:
        with self._read_connection() as connection:
            rows = connection.execute(
                "SELECT run_id FROM testnet_runs ORDER BY created_at, run_id"
            ).fetchall()
        return tuple(str(row["run_id"]) for row in rows)

    def set_runtime_state(
        self,
        run_id: str,
        state: RuntimeState | str,
        *,
        reason: str | None = None,
        at: datetime | None = None,
    ) -> RunRecord:
        """Persist runtime admission, serializing protective states against sends."""

        if self._read_only:
            raise TestnetStoreError("read-only TestnetStore cannot mutate durable state")
        normalized = _sha256(run_id, label="run_id")
        target = RuntimeState(state)
        if target not in {
            RuntimeState.PAUSED,
            RuntimeState.MANUAL_REVIEW,
            RuntimeState.STOPPED,
        }:
            return self._set_runtime_state_unserialized(
                normalized,
                target,
                reason=reason,
                at=at,
            )
        run = self.get_run(normalized)
        gate = self._open_control_lock(self._account_send_gate_path(run))
        locked = False
        try:
            _acquire_os_lock(gate, blocking=True)
            locked = True
            return self._set_runtime_state_unserialized(
                normalized,
                target,
                reason=reason,
                at=at,
            )
        finally:
            try:
                if locked:
                    _release_os_lock(gate)
            finally:
                gate.close()

    def _set_runtime_state_unserialized(
        self,
        run_id: str,
        state: RuntimeState | str,
        *,
        reason: str | None = None,
        at: datetime | None = None,
    ) -> RunRecord:
        normalized = _sha256(run_id, label="run_id")
        target = RuntimeState(state)
        timestamp = _utc(at, label="runtime state timestamp")
        lease: WalletLeaseRecord | None = None
        if target is RuntimeState.KILLED:
            before = self.get_run(normalized)
            if before.runtime_state is RuntimeState.KILLED:
                if not self._account_kill_latched_run(before):
                    stored_reason = before.state_reason
                    safe_reason = (
                        stored_reason
                        if stored_reason is not None
                        and _REASON_CODE_RE.fullmatch(stored_reason) is not None
                        else "KILLED"
                    )
                    self._latch_account_kill(
                        before,
                        reason=safe_reason,
                        killed_at=timestamp,
                    )
                return self.get_run(normalized)
            if (
                reason is None
                or not isinstance(reason, str)
                or _REASON_CODE_RE.fullmatch(reason) is None
            ):
                raise ValueError(
                    "KILLED requires a bounded uppercase stable reason code"
                )
            # Create the account-wide latch before opening the run transaction.
            # This serializes against a final network send without a DB/lock cycle.
            self._latch_account_kill(before, reason=reason, killed_at=timestamp)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                run = self._require_run(connection, normalized)
                run_record = self._run_record(run)
                current = RuntimeState(str(run["runtime_state"]))
                if target in {RuntimeState.STARTING, RuntimeState.RUNNING} and (
                    self._account_kill_latched_run(run_record)
                ):
                    raise RunConflictError(
                        "durable account-scoped Testnet kill latch blocks activation"
                    )
                if current is target:
                    connection.rollback()
                    return self.get_run(normalized)
                if reason is not None and (
                    not isinstance(reason, str)
                    or _REASON_CODE_RE.fullmatch(reason) is None
                ):
                    raise ValueError(
                        "runtime state reason must be a bounded uppercase stable reason code"
                    )
                if target in {
                    RuntimeState.PAUSED,
                    RuntimeState.MANUAL_REVIEW,
                    RuntimeState.KILLED,
                } and reason is None:
                    raise ValueError(f"{target.value} requires a non-empty reason")
                if target in {RuntimeState.STARTING, RuntimeState.RUNNING}:
                    lease = self._require_wallet_lease(normalized)
                if current is RuntimeState.KILLED and target is not RuntimeState.KILLED:
                    raise RunConflictError("KILLED Testnet runtime cannot be re-enabled")
                if current is RuntimeState.MANUAL_REVIEW and target not in {
                    RuntimeState.MANUAL_REVIEW,
                    RuntimeState.KILLED,
                }:
                    raise RunConflictError("MANUAL_REVIEW requires explicit offline repair")
                legal_runtime_targets: Mapping[RuntimeState, frozenset[RuntimeState]] = {
                    RuntimeState.STOPPED: frozenset(
                        {
                            RuntimeState.STARTING,
                            RuntimeState.PAUSED,
                            RuntimeState.MANUAL_REVIEW,
                            RuntimeState.KILLED,
                        }
                    ),
                    RuntimeState.STARTING: frozenset(
                        {
                            RuntimeState.RUNNING,
                            RuntimeState.PAUSED,
                            RuntimeState.MANUAL_REVIEW,
                            RuntimeState.KILLED,
                            RuntimeState.STOPPED,
                        }
                    ),
                    RuntimeState.RUNNING: frozenset(
                        {
                            RuntimeState.STARTING,
                            RuntimeState.PAUSED,
                            RuntimeState.MANUAL_REVIEW,
                            RuntimeState.KILLED,
                            RuntimeState.STOPPED,
                        }
                    ),
                    RuntimeState.PAUSED: frozenset(
                        {
                            RuntimeState.STARTING,
                            RuntimeState.MANUAL_REVIEW,
                            RuntimeState.KILLED,
                            RuntimeState.STOPPED,
                        }
                    ),
                    RuntimeState.MANUAL_REVIEW: frozenset({RuntimeState.KILLED}),
                    RuntimeState.KILLED: frozenset(),
                }
                if target not in legal_runtime_targets[current]:
                    raise RunConflictError(
                        f"illegal Testnet runtime transition: {current.value} -> {target.value}"
                    )
                if target is RuntimeState.RUNNING:
                    if current is not RuntimeState.STARTING:
                        raise RunConflictError("RUNNING admission requires STARTING")
                    if run["last_reconciled_at"] is None:
                        raise RunConflictError(
                            "RUNNING admission requires authoritative reconciliation"
                        )
                    assert lease is not None
                    reconciled_at = parse_utc(str(run["last_reconciled_at"]))
                    if reconciled_at < lease.acquired_at:
                        raise RunConflictError(
                            "RUNNING admission requires reconciliation after wallet lease acquisition"
                        )
                    ambiguous = int(
                        connection.execute(
                            """
                            SELECT COUNT(*) FROM testnet_actions
                            WHERE run_id=? AND status=?
                            """,
                            (normalized, ActionAttemptStatus.AMBIGUOUS.value),
                        ).fetchone()[0]
                    )
                    if ambiguous:
                        raise RunConflictError(
                            "RUNNING admission requires every ambiguous action to be reconciled"
                        )
                connection.execute(
                    """
                    UPDATE testnet_runs SET runtime_state=?, state_reason=?, updated_at=?
                    WHERE run_id=?
                    """,
                    (target.value, reason, utc_text(timestamp), normalized),
                )
                self._append_audit(
                    connection,
                    normalized,
                    "RUNTIME_STATE_CHANGED",
                    {
                        "reason": reason,
                        "source": current.value,
                        "target": target.value,
                    },
                    created_at=timestamp,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return self.get_run(normalized)

    def pause(self, run_id: str, *, reason: str, at: datetime | None = None) -> RunRecord:
        return self.set_runtime_state(run_id, RuntimeState.PAUSED, reason=reason, at=at)

    def kill(self, run_id: str, *, reason: str, at: datetime | None = None) -> RunRecord:
        return self.set_runtime_state(run_id, RuntimeState.KILLED, reason=reason, at=at)

    @staticmethod
    def _order_record(row: sqlite3.Row) -> TestnetOrder:
        intent = TestnetOrderIntent.from_dict(
            _decode(str(row["intent_json"]), label="order intent")
        )
        return TestnetOrder(
            intent=intent,
            status=OrderStatus(str(row["status"])),
            filled_quantity=decimal_value(
                str(row["filled_quantity_text"]),
                label="filled_quantity",
                non_negative=True,
            ),
            average_fill_price=(
                decimal_value(
                    str(row["average_fill_price_text"]),
                    label="average_fill_price",
                    positive=True,
                )
                if row["average_fill_price_text"] is not None
                else None
            ),
            venue_order_id=cast(str | None, row["venue_order_id"]),
            updated_at=parse_utc(str(row["updated_at"])),
        )

    def create_order(self, intent: TestnetOrderIntent) -> TestnetOrder:
        if not isinstance(intent, TestnetOrderIntent):
            raise TypeError("intent must be a TestnetOrderIntent")
        normalized, intent_json, intent_hash = _payload(intent.to_dict(), label="order intent")
        del normalized
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                run = self._require_run(connection, intent.run_id)
                if self._account_kill_latched_run(self._run_record(run)):
                    raise RunConflictError(
                        "durable account-scoped Testnet kill latch blocks new intents"
                    )
                state = RuntimeState(str(run["runtime_state"]))
                if state is not RuntimeState.RUNNING:
                    raise RunConflictError(
                        f"runtime state {state.value} blocks new Testnet order intents"
                    )
                existing = connection.execute(
                    "SELECT * FROM testnet_orders WHERE run_id=? AND order_id=?",
                    (intent.run_id, intent.order_id),
                ).fetchone()
                if existing is not None:
                    if (
                        str(existing["intent_json"]) == intent_json
                        and str(existing["intent_hash"]) == intent_hash
                    ):
                        connection.rollback()
                        return self._order_record(existing)
                    connection.execute(
                        """
                        UPDATE testnet_runs SET runtime_state=?, state_reason=?
                        WHERE run_id=? AND runtime_state NOT IN (?, ?)
                        """,
                        (
                            RuntimeState.MANUAL_REVIEW.value,
                            "divergent order intent identity",
                            intent.run_id,
                            RuntimeState.KILLED.value,
                            RuntimeState.MANUAL_REVIEW.value,
                        ),
                    )
                    self._append_audit(
                        connection,
                        intent.run_id,
                        "IDEMPOTENCY_CONFLICT",
                        {"identity": intent.order_id, "kind": "ORDER_INTENT"},
                        created_at=intent.created_at,
                    )
                    connection.commit()
                    raise IdempotencyConflictError(
                        "order_id was reused with a divergent Testnet intent"
                    )
                timestamp = utc_text(intent.created_at)
                connection.execute(
                    """
                    INSERT INTO testnet_orders VALUES (?, ?, ?, ?, ?, ?, '0', NULL, NULL, ?, ?)
                    """,
                    (
                        intent.run_id,
                        intent.order_id,
                        intent.cloid,
                        intent_json,
                        intent_hash,
                        OrderStatus.REQUESTED.value,
                        timestamp,
                        timestamp,
                    ),
                )
                self._append_audit(
                    connection,
                    intent.run_id,
                    "ORDER_INTENT_PERSISTED",
                    {
                        "cloid": intent.cloid,
                        "intent_hash": intent_hash,
                        "order_id": intent.order_id,
                    },
                    created_at=intent.created_at,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return self.get_order(intent.run_id, intent.order_id)

    def get_order(self, run_id: str, order_id: str) -> TestnetOrder:
        normalized_run = _sha256(run_id, label="run_id")
        normalized_order = _sha256(order_id, label="order_id")
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM testnet_orders WHERE run_id=? AND order_id=?",
                (normalized_run, normalized_order),
            ).fetchone()
        if row is None:
            raise OrderConflictError(f"unknown Testnet order {normalized_order!r}")
        return self._order_record(row)

    def list_orders(self, run_id: str, *, active_only: bool = False) -> tuple[TestnetOrder, ...]:
        normalized = _sha256(run_id, label="run_id")
        with self._read_connection() as connection:
            rows = connection.execute(
                "SELECT * FROM testnet_orders WHERE run_id=? ORDER BY created_at, order_id",
                (normalized,),
            ).fetchall()
        orders = tuple(self._order_record(row) for row in rows)
        if active_only:
            return tuple(order for order in orders if order.status.reserves_exposure)
        return orders

    def transition_order(
        self,
        run_id: str,
        order_id: str,
        target: OrderStatus | str,
        *,
        at: datetime,
        venue_order_id: str | None = None,
        reason: str | None = None,
    ) -> TestnetOrder:
        normalized_run = _sha256(run_id, label="run_id")
        normalized_order = _sha256(order_id, label="order_id")
        target_status = OrderStatus(target)
        timestamp = _utc(at, label="order transition timestamp")
        if target_status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}:
            raise OrderConflictError(
                "fill-bearing projections require complete_action or apply_reconciliation"
            )
        if venue_order_id is not None:
            venue_order_id = _identity(venue_order_id, label="venue_order_id")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._require_run(connection, normalized_run)
                row = connection.execute(
                    "SELECT * FROM testnet_orders WHERE run_id=? AND order_id=?",
                    (normalized_run, normalized_order),
                ).fetchone()
                if row is None:
                    raise OrderConflictError(f"unknown Testnet order {normalized_order!r}")
                current = OrderStatus(str(row["status"]))
                if timestamp < parse_utc(str(row["updated_at"])):
                    raise OrderConflictError("order transition timestamp cannot move backwards")
                existing_venue_id = cast(str | None, row["venue_order_id"])
                if (
                    existing_venue_id is not None
                    and venue_order_id is not None
                    and existing_venue_id != venue_order_id
                ):
                    raise OrderConflictError("venue_order_id cannot be rebound")
                if current is target_status:
                    connection.rollback()
                    return self._order_record(row)
                require_order_transition(current, target_status)
                connection.execute(
                    """
                    UPDATE testnet_orders
                    SET status=?, venue_order_id=COALESCE(venue_order_id, ?), updated_at=?
                    WHERE run_id=? AND order_id=?
                    """,
                    (
                        target_status.value,
                        venue_order_id,
                        utc_text(timestamp),
                        normalized_run,
                        normalized_order,
                    ),
                )
                self._append_audit(
                    connection,
                    normalized_run,
                    "ORDER_STATUS_CHANGED",
                    {
                        "order_id": normalized_order,
                        "reason": reason,
                        "source": current.value,
                        "target": target_status.value,
                        "venue_order_id": venue_order_id or existing_venue_id,
                    },
                    created_at=timestamp,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return self.get_order(normalized_run, normalized_order)

    @staticmethod
    def _nonce_burn_fault_point(point: str, nonce: int) -> None:
        del point, nonce

    def _next_nonce(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        at: datetime,
        minimum_nonce: int | None,
    ) -> tuple[int, int]:
        run = TestnetStore._require_run(connection, run_id)
        if minimum_nonce is not None and (
            isinstance(minimum_nonce, bool)
            or not isinstance(minimum_nonce, int)
            or minimum_nonce <= 0
        ):
            raise ValueError("minimum_nonce must be a positive integer")
        durable_minimum = max(int(run["last_nonce"]) + 1, minimum_nonce or 1)
        nonce, previous_wallet_nonce = self._burn_wallet_nonce(
            self._run_record(run),
            at=at,
            minimum_nonce=durable_minimum,
        )
        self._nonce_burn_fault_point("AFTER_GLOBAL_BURN_BEFORE_SQLITE", nonce)
        connection.execute(
            "UPDATE testnet_runs SET last_nonce=?, updated_at=? WHERE run_id=?",
            (nonce, utc_text(at), run_id),
        )
        return nonce, previous_wallet_nonce

    def allocate_nonce(
        self,
        run_id: str,
        *,
        minimum_nonce: int | None = None,
        allocated_at: datetime | None = None,
    ) -> int:
        normalized = _sha256(run_id, label="run_id")
        at = _utc(allocated_at, label="nonce allocated_at")
        self._require_wallet_lease(normalized)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                nonce, previous_wallet_nonce = self._next_nonce(
                    connection,
                    normalized,
                    at,
                    minimum_nonce,
                )
                self._append_audit(
                    connection,
                    normalized,
                    "NONCE_ALLOCATED",
                    {
                        "global_wallet_watermark_burned_before_sqlite": True,
                        "nonce": nonce,
                        "previous_wallet_nonce": previous_wallet_nonce,
                        "reserved_without_action": True,
                    },
                    created_at=at,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return nonce

    @staticmethod
    def _action_record(row: sqlite3.Row) -> ActionAttemptRecord:
        response = (
            _decode(str(row["response_json"]), label="action response")
            if row["response_json"] is not None
            else None
        )
        return ActionAttemptRecord(
            run_id=str(row["run_id"]),
            action_id=str(row["action_id"]),
            order_id=cast(str | None, row["order_id"]),
            kind=ActionKind(str(row["action_kind"])),
            nonce=int(row["nonce"]),
            payload=_decode(str(row["payload_json"]), label="action payload"),
            payload_hash=str(row["payload_hash"]),
            status=ActionAttemptStatus(str(row["status"])),
            response=response,
            created_at=parse_utc(str(row["created_at"])),
            resolved_at=(
                parse_utc(str(row["resolved_at"]))
                if row["resolved_at"] is not None
                else None
            ),
        )

    def reserve_action(
        self,
        run_id: str,
        *,
        action_id: str,
        kind: ActionKind | str,
        order_id: str | None,
        payload: Mapping[str, object],
        created_at: datetime,
        minimum_nonce: int | None = None,
        expires_after_delta_ms: int | None = None,
    ) -> ActionAttemptRecord:
        normalized_run = _sha256(run_id, label="run_id")
        normalized_action = _sha256(action_id, label="action_id")
        action_kind = ActionKind(kind)
        normalized_order = _sha256(order_id, label="order_id") if order_id else None
        if action_kind is not ActionKind.SCHEDULE_CANCEL and normalized_order is None:
            raise ValueError(f"{action_kind.value} requires order_id")
        at = _utc(created_at, label="action created_at")
        lease = self._require_wallet_lease(normalized_run)
        normalized_payload, _, _ = _payload(payload, label="action payload")
        if "expires_after_ms" in normalized_payload:
            raise ValueError(
                "expires_after_ms is store-owned and must never be caller-precomputed"
            )
        if expires_after_delta_ms is not None and (
            isinstance(expires_after_delta_ms, bool)
            or not isinstance(expires_after_delta_ms, int)
            or not 1_000 <= expires_after_delta_ms <= 60_000
        ):
            raise ValueError("expires_after_delta_ms must be an integer from 1000 to 60000")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                run = self._require_run(connection, normalized_run)
                if action_kind in {ActionKind.SUBMIT, ActionKind.REPLACE} and (
                    self._account_kill_latched_run(self._run_record(run))
                ):
                    raise RunConflictError(
                        "durable account-scoped Testnet kill latch blocks risk-increasing actions"
                    )
                existing = connection.execute(
                    "SELECT * FROM testnet_actions WHERE run_id=? AND action_id=?",
                    (normalized_run, normalized_action),
                ).fetchone()
                if existing is not None:
                    expected_payload = dict(normalized_payload)
                    if expires_after_delta_ms is not None:
                        expected_payload["expires_after_ms"] = (
                            int(existing["nonce"]) + expires_after_delta_ms
                        )
                    _, expected_payload_json, expected_payload_hash = _payload(
                        expected_payload,
                        label="action payload",
                    )
                    if (
                        str(existing["action_kind"]) == action_kind.value
                        and cast(str | None, existing["order_id"]) == normalized_order
                        and str(existing["payload_json"]) == expected_payload_json
                        and str(existing["payload_hash"]) == expected_payload_hash
                    ):
                        connection.rollback()
                        if (
                            ActionAttemptStatus(str(existing["status"]))
                            is ActionAttemptStatus.AMBIGUOUS
                        ):
                            raise AmbiguousActionReplayError(
                                "ambiguous signed action is already durable; reconcile and never resend"
                            )
                        return self._action_record(existing)
                    connection.execute(
                        """
                        UPDATE testnet_runs SET runtime_state=?, state_reason=?
                        WHERE run_id=? AND runtime_state NOT IN (?, ?)
                        """,
                        (
                            RuntimeState.MANUAL_REVIEW.value,
                            "divergent action idempotency key",
                            normalized_run,
                            RuntimeState.KILLED.value,
                            RuntimeState.MANUAL_REVIEW.value,
                        ),
                    )
                    self._append_audit(
                        connection,
                        normalized_run,
                        "IDEMPOTENCY_CONFLICT",
                        {"identity": normalized_action, "kind": "ACTION"},
                        created_at=at,
                    )
                    connection.commit()
                    raise IdempotencyConflictError(
                        "action_id was reused with a divergent Testnet action"
                    )
                runtime_state = RuntimeState(str(run["runtime_state"]))
                if action_kind in {ActionKind.SUBMIT, ActionKind.REPLACE} and (
                    run["last_reconciled_at"] is None
                    or parse_utc(str(run["last_reconciled_at"])) < lease.acquired_at
                ):
                    raise RunConflictError(
                        "risk-increasing signed actions require reconciliation after "
                        "wallet lease acquisition"
                    )
                if action_kind in {ActionKind.SUBMIT, ActionKind.REPLACE} and runtime_state is not RuntimeState.RUNNING:
                    raise RunConflictError(
                        f"runtime state {runtime_state.value} blocks {action_kind.value}"
                    )
                order_row: sqlite3.Row | None = None
                if normalized_order is not None:
                    order_row = connection.execute(
                        "SELECT * FROM testnet_orders WHERE run_id=? AND order_id=?",
                        (normalized_run, normalized_order),
                    ).fetchone()
                    if order_row is None:
                        raise OrderConflictError(f"unknown Testnet order {normalized_order!r}")
                account_rate_count: int | None = None
                account_rate_limit: int | None = None
                account_rate_burned = False
                account_action_identity_count: int | None = None
                account_rate_lane = "PROTECTIVE_EXEMPT"
                if action_kind in {
                    ActionKind.SUBMIT,
                    ActionKind.CANCEL,
                    ActionKind.REPLACE,
                }:
                    (
                        account_rate_count,
                        account_rate_limit,
                        account_rate_burned,
                        account_action_identity_count,
                    ) = self._burn_account_rate_event(
                        self._run_record(run),
                        action_id=normalized_action,
                        kind=action_kind,
                        occurred_at=at,
                    )
                    account_rate_lane = "ORDINARY_ACCOUNT_LIMIT"
                nonce, previous_wallet_nonce = self._next_nonce(
                    connection,
                    normalized_run,
                    at,
                    minimum_nonce,
                )
                durable_payload = dict(normalized_payload)
                if expires_after_delta_ms is not None:
                    durable_payload["expires_after_ms"] = (
                        nonce + expires_after_delta_ms
                    )
                _, payload_json, payload_hash = _payload(
                    durable_payload,
                    label="action payload",
                )
                connection.execute(
                    """
                    INSERT INTO testnet_actions
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, NULL)
                    """,
                    (
                        normalized_run,
                        normalized_action,
                        normalized_order,
                        action_kind.value,
                        nonce,
                        payload_json,
                        payload_hash,
                        ActionAttemptStatus.AMBIGUOUS.value,
                        utc_text(at),
                    ),
                )
                connection.execute(
                    "INSERT INTO testnet_action_rate_events VALUES (?, ?, ?, ?, ?)",
                    (
                        normalized_run,
                        normalized_action,
                        action_kind.value,
                        nonce,
                        utc_text(at),
                    ),
                )
                if order_row is not None and action_kind is not ActionKind.SCHEDULE_CANCEL:
                    current = OrderStatus(str(order_row["status"]))
                    target = (
                        OrderStatus.CANCEL_REQUESTED
                        if action_kind is ActionKind.CANCEL
                        else OrderStatus.SUBMITTED
                    )
                    if action_kind in {ActionKind.SUBMIT, ActionKind.REPLACE} and (
                        current is not OrderStatus.REQUESTED
                    ):
                        raise OrderConflictError(
                            f"{action_kind.value} order_id must identify the REQUESTED "
                            "new/replacement order"
                        )
                    if current is not target:
                        require_order_transition(current, target)
                        connection.execute(
                            """
                            UPDATE testnet_orders SET status=?, updated_at=?
                            WHERE run_id=? AND order_id=?
                            """,
                            (target.value, utc_text(at), normalized_run, normalized_order),
                        )
                        self._append_audit(
                            connection,
                            normalized_run,
                            "ORDER_STATUS_CHANGED",
                            {
                                "order_id": normalized_order,
                                "reason": "signed action reserved before network I/O",
                                "source": current.value,
                                "target": target.value,
                            },
                            created_at=at,
                        )
                self._append_audit(
                    connection,
                    normalized_run,
                    "ACTION_RESERVED_AMBIGUOUS",
                    {
                        "action_id": normalized_action,
                        "action_kind": action_kind.value,
                        "account_rate_burned_before_sqlite": account_rate_burned,
                        "account_action_identity_capacity": ACCOUNT_ACTION_IDENTITY_CAPACITY,
                        "account_action_identity_count": account_action_identity_count,
                        "account_rate_count": account_rate_count,
                        "account_rate_lane": account_rate_lane,
                        "account_rate_limit": account_rate_limit,
                        "global_wallet_watermark_burned_before_sqlite": True,
                        "nonce": nonce,
                        "order_id": normalized_order,
                        "payload_hash": payload_hash,
                        "previous_wallet_nonce": previous_wallet_nonce,
                    },
                    created_at=at,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return self.get_action(normalized_run, normalized_action)

    def get_action(self, run_id: str, action_id: str) -> ActionAttemptRecord:
        normalized_run = _sha256(run_id, label="run_id")
        normalized_action = _sha256(action_id, label="action_id")
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM testnet_actions WHERE run_id=? AND action_id=?",
                (normalized_run, normalized_action),
            ).fetchone()
        if row is None:
            raise TestnetStoreError(f"unknown Testnet action {normalized_action!r}")
        return self._action_record(row)

    def list_ambiguous_actions(self, run_id: str) -> tuple[ActionAttemptRecord, ...]:
        normalized = _sha256(run_id, label="run_id")
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM testnet_actions
                WHERE run_id=? AND status=? ORDER BY nonce
                """,
                (normalized, ActionAttemptStatus.AMBIGUOUS.value),
            ).fetchall()
        return tuple(self._action_record(row) for row in rows)

    def observe_ambiguous_action(
        self,
        run_id: str,
        action_id: str,
        *,
        response: Mapping[str, object],
        order_updates: Sequence[OrderProjectionUpdate],
        observed_at: datetime,
    ) -> ActionAttemptRecord:
        """Atomically retain an uncertain response without resolving or resending."""

        normalized_run = _sha256(run_id, label="run_id")
        normalized_action = _sha256(action_id, label="action_id")
        at = _utc(observed_at, label="ambiguous action observed_at")
        for update in order_updates:
            if not isinstance(update, OrderProjectionUpdate):
                raise TypeError("order_updates must contain OrderProjectionUpdate values")
            if update.status is not OrderStatus.UNKNOWN:
                raise ValueError("ambiguous action projections must remain UNKNOWN")
            if update.filled_quantity is None:
                raise ValueError(
                    "ambiguous action projections require exact filled_quantity"
                )
        normalized_response, response_json, response_hash = _payload(
            response,
            label="ambiguous action response",
        )
        if not normalized_response:
            raise ValueError("ambiguous action response proof cannot be empty")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._require_run(connection, normalized_run)
                row = connection.execute(
                    "SELECT * FROM testnet_actions WHERE run_id=? AND action_id=?",
                    (normalized_run, normalized_action),
                ).fetchone()
                if row is None:
                    raise TestnetStoreError(
                        f"unknown Testnet action {normalized_action!r}"
                    )
                if (
                    ActionAttemptStatus(str(row["status"]))
                    is not ActionAttemptStatus.AMBIGUOUS
                ):
                    raise IdempotencyConflictError(
                        "only an AMBIGUOUS action can receive uncertain observations"
                    )
                if at < parse_utc(str(row["created_at"])):
                    raise IdempotencyConflictError(
                        "ambiguous response timestamp predates the durable action"
                    )
                if row["response_json"] is not None:
                    projections_match = True
                    for update in order_updates:
                        durable = connection.execute(
                            """
                            SELECT status, venue_order_id, filled_quantity_text,
                                   average_fill_price_text
                            FROM testnet_orders WHERE run_id=? AND order_id=?
                            """,
                            (normalized_run, update.order_id),
                        ).fetchone()
                        projections_match = projections_match and (
                            durable is not None
                            and str(durable["status"]) == update.status.value
                            and (
                                update.venue_order_id is None
                                or str(durable["venue_order_id"])
                                == update.venue_order_id
                            )
                            and (
                                update.filled_quantity is None
                                or str(durable["filled_quantity_text"])
                                == decimal_text(update.filled_quantity)
                            )
                            and (
                                (
                                    durable["average_fill_price_text"] is None
                                    and update.average_fill_price is None
                                )
                                or (
                                    durable["average_fill_price_text"] is not None
                                    and update.average_fill_price is not None
                                    and str(durable["average_fill_price_text"])
                                    == decimal_text(update.average_fill_price)
                                )
                            )
                        )
                    if (
                        str(row["response_json"]) == response_json
                        and str(row["response_hash"]) == response_hash
                        and projections_match
                    ):
                        connection.rollback()
                        return self._action_record(row)
                    connection.execute(
                        """
                        UPDATE testnet_runs
                        SET runtime_state=?, state_reason=?, updated_at=?
                        WHERE run_id=? AND runtime_state NOT IN (?, ?)
                        """,
                        (
                            RuntimeState.MANUAL_REVIEW.value,
                            "DIVERGENT_AMBIGUOUS_OBSERVATION",
                            utc_text(at),
                            normalized_run,
                            RuntimeState.KILLED.value,
                            RuntimeState.MANUAL_REVIEW.value,
                        ),
                    )
                    self._append_audit(
                        connection,
                        normalized_run,
                        "IDEMPOTENCY_CONFLICT",
                        {
                            "identity": normalized_action,
                            "kind": "AMBIGUOUS_ACTION_OBSERVATION",
                        },
                        created_at=at,
                    )
                    connection.commit()
                    raise IdempotencyConflictError(
                        "ambiguous action observation was redelivered divergently"
                    )
                connection.execute(
                    """
                    UPDATE testnet_actions SET response_json=?, response_hash=?
                    WHERE run_id=? AND action_id=?
                    """,
                    (
                        response_json,
                        response_hash,
                        normalized_run,
                        normalized_action,
                    ),
                )
                applied: list[dict[str, object]] = []
                for update in order_updates:
                    order = self._apply_order_update(
                        connection,
                        normalized_run,
                        update,
                        at=at,
                    )
                    applied.append(
                        {
                            "average_fill_price": (
                                decimal_text(order.average_fill_price)
                                if order.average_fill_price is not None
                                else None
                            ),
                            "filled_quantity": decimal_text(order.filled_quantity),
                            "order_id": order.intent.order_id,
                            "status": order.status.value,
                            "venue_order_id": order.venue_order_id,
                        }
                    )
                self._append_audit(
                    connection,
                    normalized_run,
                    "AMBIGUOUS_ACTION_OBSERVED",
                    {
                        "action_id": normalized_action,
                        "order_updates": applied,
                        "response_hash": response_hash,
                    },
                    created_at=at,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return self.get_action(normalized_run, normalized_action)

    def resolve_action(
        self,
        run_id: str,
        action_id: str,
        status: ActionAttemptStatus | str,
        *,
        response: Mapping[str, object],
        resolved_at: datetime,
    ) -> ActionAttemptRecord:
        return self.complete_action(
            run_id,
            action_id,
            status,
            response=response,
            order_updates=(),
            resolved_at=resolved_at,
        )

    def _resolve_reconciliation_action_tx(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        resolution: ReconciliationActionResolution,
        *,
        at: datetime,
    ) -> ActionAttemptRecord:
        normalized_proof, proof_json, proof_hash = _payload(
            resolution.proof,
            label="authoritative action resolution proof",
        )
        del normalized_proof
        row = connection.execute(
            "SELECT * FROM testnet_actions WHERE run_id=? AND action_id=?",
            (run_id, resolution.action_id),
        ).fetchone()
        if row is None:
            raise TestnetStoreError(
                f"unknown Testnet action {resolution.action_id!r}"
            )
        current = ActionAttemptStatus(str(row["status"]))
        if current is not ActionAttemptStatus.AMBIGUOUS:
            if (
                current is resolution.status
                and str(row["response_json"]) == proof_json
                and str(row["response_hash"]) == proof_hash
                and str(row["resolved_at"]) == utc_text(at)
            ):
                return self._action_record(row)
            raise IdempotencyConflictError(
                "authoritative action resolution diverges from durable outcome"
            )
        if at < parse_utc(str(row["created_at"])):
            raise IdempotencyConflictError(
                "authoritative action resolution predates the durable attempt"
            )
        connection.execute(
            """
            UPDATE testnet_actions
            SET status=?, response_json=?, response_hash=?, resolved_at=?
            WHERE run_id=? AND action_id=?
            """,
            (
                resolution.status.value,
                proof_json,
                proof_hash,
                utc_text(at),
                run_id,
                resolution.action_id,
            ),
        )
        self._append_audit(
            connection,
            run_id,
            "ACTION_RESOLVED",
            {
                "action_id": resolution.action_id,
                "authoritative_reconciliation": True,
                "order_updates": [],
                "response_hash": proof_hash,
                "status": resolution.status.value,
            },
            created_at=at,
        )
        updated = connection.execute(
            "SELECT * FROM testnet_actions WHERE run_id=? AND action_id=?",
            (run_id, resolution.action_id),
        ).fetchone()
        assert updated is not None
        return self._action_record(updated)

    def _apply_order_update(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        update: OrderProjectionUpdate,
        *,
        at: datetime,
    ) -> TestnetOrder:
        row = connection.execute(
            "SELECT * FROM testnet_orders WHERE run_id=? AND order_id=?",
            (run_id, update.order_id),
        ).fetchone()
        if row is None:
            raise OrderConflictError(f"unknown Testnet order {update.order_id!r}")
        current = OrderStatus(str(row["status"]))
        if at < parse_utc(str(row["updated_at"])):
            raise OrderConflictError("order projection timestamp cannot move backwards")
        if current is not update.status:
            require_order_transition(current, update.status)
        existing_venue = cast(str | None, row["venue_order_id"])
        if (
            existing_venue is not None
            and update.venue_order_id is not None
            and existing_venue != update.venue_order_id
        ):
            raise OrderConflictError("venue_order_id cannot be rebound")
        venue_order_id = update.venue_order_id or existing_venue
        if venue_order_id is not None and update.status.reserves_exposure:
            duplicate = connection.execute(
                """
                SELECT order_id FROM testnet_orders
                WHERE run_id=? AND venue_order_id=? AND order_id!=?
                  AND status IN (?, ?, ?, ?, ?, ?, ?)
                LIMIT 1
                """,
                (
                    run_id,
                    venue_order_id,
                    update.order_id,
                    OrderStatus.REQUESTED.value,
                    OrderStatus.SUBMITTED.value,
                    OrderStatus.ACKNOWLEDGED.value,
                    OrderStatus.OPEN.value,
                    OrderStatus.PARTIALLY_FILLED.value,
                    OrderStatus.CANCEL_REQUESTED.value,
                    OrderStatus.UNKNOWN.value,
                ),
            ).fetchone()
            if duplicate is not None:
                raise OrderConflictError(
                    "one venue_order_id maps to multiple exposure-reserving local orders"
                )
        intent = TestnetOrderIntent.from_dict(
            _decode(str(row["intent_json"]), label="order intent")
        )
        filled = (
            decimal_value(
                update.filled_quantity,
                label="filled_quantity",
                non_negative=True,
            )
            if update.filled_quantity is not None
            else decimal_value(
                str(row["filled_quantity_text"]),
                label="filled_quantity",
                non_negative=True,
            )
        )
        durable_filled = decimal_value(
            str(row["filled_quantity_text"]),
            label="durable filled_quantity",
            non_negative=True,
        )
        if filled < durable_filled:
            raise OrderConflictError("filled quantity projection cannot move backwards")
        average = (
            update.average_fill_price
            if update.average_fill_price is not None
            else (
                decimal_value(
                    str(row["average_fill_price_text"]),
                    label="average_fill_price",
                    positive=True,
                )
                if row["average_fill_price_text"] is not None
                else None
            )
        )
        if filled > intent.quantity:
            raise OrderConflictError("filled quantity exceeds requested quantity")
        if filled > 0 and average is None:
            raise OrderConflictError("a positive filled quantity requires average_fill_price")
        if filled == 0 and average is not None:
            raise OrderConflictError("zero filled quantity cannot have average_fill_price")
        if update.status is OrderStatus.FILLED and filled != intent.quantity:
            raise OrderConflictError("FILLED requires exact requested quantity")
        if update.status is OrderStatus.PARTIALLY_FILLED and not (
            Decimal(0) < filled < intent.quantity
        ):
            raise OrderConflictError(
                "PARTIALLY_FILLED requires a positive non-complete quantity"
            )
        if update.status in {
            OrderStatus.REQUESTED,
            OrderStatus.SUBMITTED,
            OrderStatus.ACKNOWLEDGED,
            OrderStatus.OPEN,
            OrderStatus.REJECTED,
            OrderStatus.INVALID,
        } and filled != 0:
            raise OrderConflictError(f"{update.status.value} cannot carry filled quantity")
        connection.execute(
            """
            UPDATE testnet_orders
            SET status=?, venue_order_id=?, filled_quantity_text=?,
                average_fill_price_text=?, updated_at=?
            WHERE run_id=? AND order_id=?
            """,
            (
                update.status.value,
                venue_order_id,
                decimal_text(filled),
                decimal_text(average) if average is not None else None,
                utc_text(at),
                run_id,
                update.order_id,
            ),
        )
        self._append_audit(
            connection,
            run_id,
            "ORDER_PROJECTION_UPDATED",
            {
                "average_fill_price": (
                    decimal_text(average) if average is not None else None
                ),
                "filled_quantity": decimal_text(filled),
                "order_id": update.order_id,
                "source": current.value,
                "status": update.status.value,
                "venue_order_id": venue_order_id,
            },
            created_at=at,
        )
        updated_row = connection.execute(
            "SELECT * FROM testnet_orders WHERE run_id=? AND order_id=?",
            (run_id, update.order_id),
        ).fetchone()
        assert updated_row is not None
        return self._order_record(updated_row)

    def complete_action(
        self,
        run_id: str,
        action_id: str,
        status: ActionAttemptStatus | str,
        *,
        response: Mapping[str, object],
        order_updates: Sequence[OrderProjectionUpdate],
        resolved_at: datetime,
    ) -> ActionAttemptRecord:
        normalized_run = _sha256(run_id, label="run_id")
        normalized_action = _sha256(action_id, label="action_id")
        target = ActionAttemptStatus(status)
        if target is ActionAttemptStatus.AMBIGUOUS:
            raise ValueError("resolved action status cannot remain AMBIGUOUS")
        at = _utc(resolved_at, label="action resolved_at")
        for update in order_updates:
            if not isinstance(update, OrderProjectionUpdate):
                raise TypeError("order_updates must contain OrderProjectionUpdate values")
        _, response_json, response_hash = _payload(response, label="action response")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._require_run(connection, normalized_run)
                row = connection.execute(
                    "SELECT * FROM testnet_actions WHERE run_id=? AND action_id=?",
                    (normalized_run, normalized_action),
                ).fetchone()
                if row is None:
                    raise TestnetStoreError(f"unknown Testnet action {normalized_action!r}")
                current = ActionAttemptStatus(str(row["status"]))
                if current is not ActionAttemptStatus.AMBIGUOUS:
                    if (
                        current is target
                        and str(row["response_json"]) == response_json
                        and str(row["response_hash"]) == response_hash
                    ):
                        for update in order_updates:
                            durable = connection.execute(
                                """
                                SELECT status, venue_order_id, filled_quantity_text,
                                       average_fill_price_text
                                FROM testnet_orders WHERE run_id=? AND order_id=?
                                """,
                                (normalized_run, update.order_id),
                            ).fetchone()
                            projection_matches = (
                                durable is not None
                                and str(durable["status"]) == update.status.value
                                and (
                                    update.venue_order_id is None
                                    or str(durable["venue_order_id"])
                                    == update.venue_order_id
                                )
                                and (
                                    update.filled_quantity is None
                                    or str(durable["filled_quantity_text"])
                                    == decimal_text(update.filled_quantity)
                                )
                                and (
                                    update.average_fill_price is None
                                    or str(durable["average_fill_price_text"])
                                    == decimal_text(update.average_fill_price)
                                )
                            )
                            if not projection_matches:
                                connection.execute(
                                    """
                                    UPDATE testnet_runs
                                    SET runtime_state=?, state_reason=?
                                    WHERE run_id=? AND runtime_state NOT IN (?, ?)
                                    """,
                                    (
                                        RuntimeState.MANUAL_REVIEW.value,
                                        "divergent completed action projection",
                                        normalized_run,
                                        RuntimeState.KILLED.value,
                                        RuntimeState.MANUAL_REVIEW.value,
                                    ),
                                )
                                self._append_audit(
                                    connection,
                                    normalized_run,
                                    "IDEMPOTENCY_CONFLICT",
                                    {
                                        "identity": normalized_action,
                                        "kind": "ACTION_PROJECTION",
                                    },
                                    created_at=at,
                                )
                                connection.commit()
                                raise IdempotencyConflictError(
                                    "resolved action order projection is not idempotent"
                                )
                        connection.rollback()
                        return self._action_record(row)
                    connection.execute(
                        """
                        UPDATE testnet_runs
                        SET runtime_state=?, state_reason=?, updated_at=?
                        WHERE run_id=? AND runtime_state NOT IN (?, ?)
                        """,
                        (
                            RuntimeState.MANUAL_REVIEW.value,
                            "DIVERGENT_ACTION_OUTCOME",
                            utc_text(at),
                            normalized_run,
                            RuntimeState.KILLED.value,
                            RuntimeState.MANUAL_REVIEW.value,
                        ),
                    )
                    self._append_audit(
                        connection,
                        normalized_run,
                        "IDEMPOTENCY_CONFLICT",
                        {
                            "identity": normalized_action,
                            "kind": "ACTION_OUTCOME",
                        },
                        created_at=at,
                    )
                    connection.commit()
                    raise IdempotencyConflictError(
                        "resolved action was redelivered with divergent outcome"
                    )
                connection.execute(
                    """
                    UPDATE testnet_actions
                    SET status=?, response_json=?, response_hash=?, resolved_at=?
                    WHERE run_id=? AND action_id=?
                    """,
                    (
                        target.value,
                        response_json,
                        response_hash,
                        utc_text(at),
                        normalized_run,
                        normalized_action,
                    ),
                )
                applied: list[dict[str, object]] = []
                for update in order_updates:
                    order = self._apply_order_update(
                        connection,
                        normalized_run,
                        update,
                        at=at,
                    )
                    applied.append(
                        {
                            "filled_quantity": decimal_text(order.filled_quantity),
                            "order_id": order.intent.order_id,
                            "status": order.status.value,
                            "venue_order_id": order.venue_order_id,
                        }
                    )
                self._append_audit(
                    connection,
                    normalized_run,
                    "ACTION_RESOLVED",
                    {
                        "action_id": normalized_action,
                        "response_hash": response_hash,
                        "status": target.value,
                        "order_updates": applied,
                    },
                    created_at=at,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return self.get_action(normalized_run, normalized_action)

    def account_action_identity_usage(self, run_id: str) -> tuple[int, int]:
        normalized = _sha256(run_id, label="run_id")
        with self._read_connection() as connection:
            run = self._run_record(self._require_run(connection, normalized))
        lock = self._open_control_lock(self._account_rate_lock_path(run))
        locked = False
        try:
            _acquire_os_lock(lock, blocking=True)
            locked = True
            _, identities = self._read_account_rate_ledger(run)
            return len(identities), ACCOUNT_ACTION_IDENTITY_CAPACITY
        finally:
            try:
                if locked:
                    _release_os_lock(lock)
            finally:
                lock.close()

    def count_actions_since(
        self,
        run_id: str,
        kind: ActionKind | str,
        *,
        since: datetime,
    ) -> int:
        normalized = _sha256(run_id, label="run_id")
        action_kind = ActionKind(kind)
        lower = _utc(since, label="rate window since")
        if action_kind in {
            ActionKind.SUBMIT,
            ActionKind.CANCEL,
            ActionKind.REPLACE,
        }:
            with self._read_connection() as connection:
                run = self._run_record(self._require_run(connection, normalized))
            lock = self._open_control_lock(self._account_rate_lock_path(run))
            locked = False
            try:
                _acquire_os_lock(lock, blocking=True)
                locked = True
                events, _ = self._read_account_rate_ledger(run)
                return sum(
                    event.kind is action_kind and event.occurred_at >= lower
                    for event in events
                )
            finally:
                try:
                    if locked:
                        _release_os_lock(lock)
                finally:
                    lock.close()
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) FROM testnet_action_rate_events
                WHERE run_id=? AND action_kind=? AND occurred_at>=?
                """,
                (normalized, action_kind.value, utc_text(lower)),
            ).fetchone()
        return int(row[0])

    @staticmethod
    def _fill_record(row: sqlite3.Row) -> FillRecord:
        return FillRecord(
            run_id=str(row["run_id"]),
            fill_id=str(row["fill_id"]),
            order_id=str(row["order_id"]),
            venue_order_id=str(row["venue_order_id"]),
            quantity=decimal_value(
                str(row["quantity_text"]), label="fill quantity", positive=True
            ),
            price=decimal_value(str(row["price_text"]), label="fill price", positive=True),
            fee=decimal_value(str(row["fee_text"]), label="fill fee"),
            payload=_decode(str(row["payload_json"]), label="fill payload"),
            payload_hash=str(row["payload_hash"]),
            received_at=parse_utc(str(row["received_at"])),
        )

    def _record_fill_tx(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        *,
        fill_id: str,
        order_id: str,
        venue_order_id: str,
        quantity: Decimal,
        price: Decimal,
        fee: Decimal,
        payload: Mapping[str, object],
        received_at: datetime,
        projection_at: datetime | None = None,
    ) -> FillRecord:
        normalized_fill = _identity(fill_id, label="fill_id")
        normalized_order = _sha256(order_id, label="order_id")
        normalized_venue = _identity(venue_order_id, label="venue_order_id")
        fill_quantity = decimal_value(quantity, label="fill quantity", positive=True)
        fill_price = decimal_value(price, label="fill price", positive=True)
        fill_fee = decimal_value(fee, label="fill fee")
        received = _utc(received_at, label="fill received_at")
        projection_time = (
            _utc(projection_at, label="fill projection_at")
            if projection_at is not None
            else received
        )
        if projection_time < received:
            raise ValueError("fill projection_at cannot precede received_at")
        _, payload_json, payload_hash = _payload(payload, label="fill payload")
        duplicate = connection.execute(
            "SELECT * FROM testnet_fills WHERE run_id=? AND fill_id=?",
            (run_id, normalized_fill),
        ).fetchone()
        if duplicate is not None:
            same = (
                str(duplicate["order_id"]) == normalized_order
                and str(duplicate["venue_order_id"]) == normalized_venue
                and str(duplicate["quantity_text"]) == decimal_text(fill_quantity)
                and str(duplicate["price_text"]) == decimal_text(fill_price)
                and str(duplicate["fee_text"]) == decimal_text(fill_fee)
                and str(duplicate["payload_json"]) == payload_json
                and str(duplicate["payload_hash"]) == payload_hash
                and str(duplicate["received_at"]) == utc_text(received)
            )
            if same:
                return self._fill_record(duplicate)
            raise IdempotencyConflictError("fill_id was reused with divergent venue facts")
        order_row = connection.execute(
            "SELECT * FROM testnet_orders WHERE run_id=? AND order_id=?",
            (run_id, normalized_order),
        ).fetchone()
        if order_row is None:
            raise OrderConflictError(f"unknown Testnet order {normalized_order!r}")
        bound_venue = cast(str | None, order_row["venue_order_id"])
        if bound_venue is not None and bound_venue != normalized_venue:
            raise OrderConflictError("fill venue_order_id differs from bound order")
        intent = TestnetOrderIntent.from_dict(
            _decode(str(order_row["intent_json"]), label="order intent")
        )
        projected_before = decimal_value(
            str(order_row["filled_quantity_text"]),
            label="projected filled quantity",
            non_negative=True,
        )
        projected_average = (
            decimal_value(
                str(order_row["average_fill_price_text"]),
                label="projected average fill price",
                positive=True,
            )
            if order_row["average_fill_price_text"] is not None
            else None
        )
        aggregate = connection.execute(
            """
            SELECT * FROM testnet_fill_aggregates WHERE run_id=? AND order_id=?
            """,
            (run_id, normalized_order),
        ).fetchone()
        stable_before = (
            decimal_value(
                str(aggregate["stable_quantity_text"]),
                label="stable filled quantity",
                non_negative=True,
            )
            if aggregate is not None
            else Decimal(0)
        )
        stable_notional_before = (
            decimal_value(
                str(aggregate["stable_notional_text"]),
                label="stable fill notional",
                non_negative=True,
            )
            if aggregate is not None
            else Decimal(0)
        )
        stable_fee_before = (
            decimal_value(str(aggregate["stable_fee_text"]), label="stable fill fee")
            if aggregate is not None
            else Decimal(0)
        )
        stable_quantity = stable_before + fill_quantity
        stable_notional = stable_notional_before + fill_quantity * fill_price
        stable_fee = stable_fee_before + fill_fee
        if stable_quantity > intent.quantity:
            raise OrderConflictError("stable venue fills exceed requested order quantity")
        connection.execute(
            "INSERT INTO testnet_fills VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                normalized_fill,
                normalized_order,
                normalized_venue,
                decimal_text(fill_quantity),
                decimal_text(fill_price),
                decimal_text(fill_fee),
                payload_json,
                payload_hash,
                utc_text(received),
            ),
        )
        connection.execute(
            """
            INSERT INTO testnet_fill_aggregates VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, order_id) DO UPDATE SET
                stable_quantity_text=excluded.stable_quantity_text,
                stable_notional_text=excluded.stable_notional_text,
                stable_fee_text=excluded.stable_fee_text,
                updated_at=excluded.updated_at
            """,
            (
                run_id,
                normalized_order,
                decimal_text(stable_quantity),
                decimal_text(stable_notional),
                decimal_text(stable_fee),
                utc_text(projection_time),
            ),
        )
        resulting_quantity = max(projected_before, stable_quantity)
        average = (
            stable_notional / stable_quantity
            if stable_quantity >= projected_before and stable_quantity > 0
            else projected_average
        )
        current_status = OrderStatus(str(order_row["status"]))
        if current_status in {OrderStatus.REJECTED, OrderStatus.INVALID}:
            raise OrderConflictError(
                f"stable fill cannot belong to {current_status.value} order"
            )
        if current_status in {OrderStatus.CANCELLED, OrderStatus.EXPIRED}:
            target = current_status
        elif resulting_quantity == intent.quantity:
            target = OrderStatus.FILLED
        else:
            target = OrderStatus.PARTIALLY_FILLED
        self._apply_order_update(
            connection,
            run_id,
            OrderProjectionUpdate(
                normalized_order,
                target,
                venue_order_id=normalized_venue,
                filled_quantity=resulting_quantity,
                average_fill_price=average,
            ),
            at=projection_time,
        )
        self._append_audit(
            connection,
            run_id,
            "FILL_RECORDED",
            {
                "fill_id": normalized_fill,
                "order_id": normalized_order,
                "payload_hash": payload_hash,
                "price": decimal_text(fill_price),
                "quantity": decimal_text(fill_quantity),
                "resulting_status": target.value,
                "venue_order_id": normalized_venue,
            },
            created_at=projection_time,
        )
        row = connection.execute(
            "SELECT * FROM testnet_fills WHERE run_id=? AND fill_id=?",
            (run_id, normalized_fill),
        ).fetchone()
        assert row is not None
        return self._fill_record(row)

    def record_fill(
        self,
        run_id: str,
        *,
        fill_id: str,
        order_id: str,
        venue_order_id: str,
        quantity: Decimal,
        price: Decimal,
        fee: Decimal,
        payload: Mapping[str, object],
        received_at: datetime,
    ) -> FillRecord:
        normalized_run = _sha256(run_id, label="run_id")
        normalized_fill = _identity(fill_id, label="fill_id")
        conflict: IdempotencyConflictError | OrderConflictError | None = None
        result: FillRecord | None = None
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._require_run(connection, normalized_run)
                result = self._record_fill_tx(
                    connection,
                    normalized_run,
                    fill_id=normalized_fill,
                    order_id=order_id,
                    venue_order_id=venue_order_id,
                    quantity=quantity,
                    price=price,
                    fee=fee,
                    payload=payload,
                    received_at=received_at,
                )
                connection.commit()
            except (IdempotencyConflictError, OrderConflictError) as error:
                if connection.in_transaction:
                    connection.rollback()
                conflict = error
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        if conflict is not None:
            self.record_reconciliation_issue(
                normalized_run,
                "FILL_CONFLICT",
                details={
                    "error_type": type(conflict).__name__,
                    "fill_id": normalized_fill,
                },
                detected_at=received_at,
            )
            raise conflict
        assert result is not None
        return result

    def get_fill(self, run_id: str, fill_id: str) -> FillRecord:
        normalized_run = _sha256(run_id, label="run_id")
        normalized_fill = _identity(fill_id, label="fill_id")
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM testnet_fills WHERE run_id=? AND fill_id=?",
                (normalized_run, normalized_fill),
            ).fetchone()
        if row is None:
            raise TestnetStoreError(f"unknown Testnet fill {normalized_fill!r}")
        return self._fill_record(row)

    def list_fills(self, run_id: str) -> tuple[FillRecord, ...]:
        normalized = _sha256(run_id, label="run_id")
        with self._read_connection() as connection:
            rows = connection.execute(
                "SELECT * FROM testnet_fills WHERE run_id=? ORDER BY received_at, fill_id",
                (normalized,),
            ).fetchall()
        return tuple(self._fill_record(row) for row in rows)

    @staticmethod
    def _snapshot_record(row: sqlite3.Row) -> RemoteSnapshotRecord:
        return RemoteSnapshotRecord(
            run_id=str(row["run_id"]),
            snapshot_id=str(row["snapshot_id"]),
            payload=_decode(str(row["payload_json"]), label="remote snapshot"),
            payload_hash=str(row["payload_hash"]),
            received_at=parse_utc(str(row["received_at"])),
            reconciled=bool(int(row["reconciled"])),
        )

    @staticmethod
    def _authoritative_snapshot_payload(
        *,
        positions: Mapping[str, Decimal],
        spot_balances: Mapping[str, Decimal],
        equity: Decimal,
        withdrawable: Decimal,
        open_orders: Sequence[Mapping[str, object]],
        source_cursor: str | None,
    ) -> dict[str, object]:
        if not isinstance(positions, Mapping) or not isinstance(spot_balances, Mapping):
            raise TypeError("positions and spot_balances must be mappings")
        normalized_positions = {
            str(instrument): decimal_text(
                decimal_value(quantity, label=f"position {instrument}")
            )
            for instrument, quantity in positions.items()
        }
        normalized_balances = {
            str(asset): decimal_text(
                decimal_value(quantity, label=f"spot balance {asset}")
            )
            for asset, quantity in spot_balances.items()
        }
        normalized_orders: list[dict[str, object]] = []
        for index, order in enumerate(open_orders):
            normalized, _, _ = _payload(order, label=f"open_orders[{index}]")
            normalized_orders.append(normalized)
        if source_cursor is not None:
            _identity(source_cursor, label="source_cursor")
        result: dict[str, object] = {
            "equity": decimal_text(decimal_value(equity, label="equity", non_negative=True)),
            "open_orders": normalized_orders,
            "positions": normalized_positions,
            "source_cursor": source_cursor,
            "spot_balances": normalized_balances,
            "withdrawable": decimal_text(
                decimal_value(withdrawable, label="withdrawable", non_negative=True)
            ),
        }
        _payload(result, label="authoritative remote snapshot")
        return result

    def _insert_snapshot_tx(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        *,
        payload: Mapping[str, object],
        received_at: datetime,
        reconciled: bool,
        snapshot_id: str | None,
    ) -> RemoteSnapshotRecord:
        _, payload_json, payload_hash = _payload(payload, label="remote snapshot")
        normalized_snapshot = self._snapshot_identity(
            run_id,
            payload_hash=payload_hash,
            received_at=received_at,
            snapshot_id=snapshot_id,
        )
        duplicate = connection.execute(
            """
            SELECT * FROM testnet_remote_snapshots
            WHERE run_id=? AND snapshot_id=?
            """,
            (run_id, normalized_snapshot),
        ).fetchone()
        if duplicate is not None:
            if (
                str(duplicate["payload_json"]) == payload_json
                and str(duplicate["payload_hash"]) == payload_hash
                and str(duplicate["received_at"]) == utc_text(received_at)
                and bool(int(duplicate["reconciled"])) is reconciled
            ):
                return self._snapshot_record(duplicate)
            raise IdempotencyConflictError(
                "snapshot_id was reused with divergent authoritative state"
            )
        connection.execute(
            "INSERT INTO testnet_remote_snapshots VALUES (?, ?, ?, ?, ?, ?)",
            (
                run_id,
                normalized_snapshot,
                payload_json,
                payload_hash,
                utc_text(received_at),
                int(reconciled),
            ),
        )
        row = connection.execute(
            """
            SELECT * FROM testnet_remote_snapshots
            WHERE run_id=? AND snapshot_id=?
            """,
            (run_id, normalized_snapshot),
        ).fetchone()
        assert row is not None
        return self._snapshot_record(row)

    @staticmethod
    def _snapshot_identity(
        run_id: str,
        *,
        payload_hash: str,
        received_at: datetime,
        snapshot_id: str | None,
    ) -> str:
        return (
            _sha256(snapshot_id, label="snapshot_id")
            if snapshot_id is not None
            else deterministic_id(
                "hyperliquid_testnet_remote_snapshot_v1",
                run_id,
                utc_text(received_at),
                payload_hash,
            )
        )

    def record_remote_snapshot(
        self,
        run_id: str,
        *,
        positions: Mapping[str, Decimal],
        spot_balances: Mapping[str, Decimal],
        equity: Decimal,
        withdrawable: Decimal,
        open_orders: Sequence[Mapping[str, object]],
        received_at: datetime,
        source_cursor: str | None = None,
        snapshot_id: str | None = None,
    ) -> RemoteSnapshotRecord:
        """Persist a remote read without advancing reconciliation freshness."""

        normalized_run = _sha256(run_id, label="run_id")
        at = _utc(received_at, label="snapshot received_at")
        payload = self._authoritative_snapshot_payload(
            positions=positions,
            spot_balances=spot_balances,
            equity=equity,
            withdrawable=withdrawable,
            open_orders=open_orders,
            source_cursor=source_cursor,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._require_run(connection, normalized_run)
                result = self._insert_snapshot_tx(
                    connection,
                    normalized_run,
                    payload=payload,
                    received_at=at,
                    reconciled=False,
                    snapshot_id=snapshot_id,
                )
                self._append_audit(
                    connection,
                    normalized_run,
                    "REMOTE_SNAPSHOT_RECORDED",
                    {
                        "payload_hash": result.payload_hash,
                        "reconciled": False,
                        "snapshot_id": result.snapshot_id,
                    },
                    created_at=at,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return result

    def _reconciliation_failure_fault_point(self, stage: str) -> None:
        """Private deterministic transaction fault point used by synthetic tests."""

        del stage

    def apply_reconciliation_failure(
        self,
        run_id: str,
        *,
        positions: Mapping[str, Decimal],
        spot_balances: Mapping[str, Decimal],
        equity: Decimal,
        withdrawable: Decimal,
        open_orders: Sequence[Mapping[str, object]],
        order_updates: Sequence[OrderProjectionUpdate],
        fills: Sequence[ReconciliationFill],
        issues: Sequence[ReconciliationIssue],
        detected_at: datetime,
        action_resolutions: Sequence[ReconciliationActionResolution] = (),
        source_cursor: str | None = None,
        snapshot_id: str | None = None,
    ) -> RemoteSnapshotRecord:
        """Atomically persist a discrepant snapshot, every issue, and the latch."""

        normalized_run = _sha256(run_id, label="run_id")
        at = _utc(detected_at, label="reconciliation failure detected_at")
        normalized_issues = _normalized_reconciliation_issues(issues)
        remote_input = _normalized_reconciliation_input(
            order_updates,
            fills,
            action_resolutions,
        )
        failure_input_hash = canonical_sha256(
            {"issues": normalized_issues, "remote_input": remote_input}
        )
        payload = self._authoritative_snapshot_payload(
            positions=positions,
            spot_balances=spot_balances,
            equity=equity,
            withdrawable=withdrawable,
            open_orders=open_orders,
            source_cursor=source_cursor,
        )
        payload["reconciliation_failure_input_hash"] = failure_input_hash
        payload["reconciliation_observations"] = remote_input
        _, expected_payload_json, expected_payload_hash = _payload(
            payload,
            label="failed reconciliation remote snapshot",
        )
        expected_snapshot_id = self._snapshot_identity(
            normalized_run,
            payload_hash=expected_payload_hash,
            received_at=at,
            snapshot_id=snapshot_id,
        )
        result: RemoteSnapshotRecord | None = None
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                run = self._require_run(connection, normalized_run)
                duplicate = connection.execute(
                    """
                    SELECT * FROM testnet_remote_snapshots
                    WHERE run_id=? AND snapshot_id=?
                    """,
                    (normalized_run, expected_snapshot_id),
                ).fetchone()
                if duplicate is not None:
                    matching_event = False
                    for event_row in connection.execute(
                        """
                        SELECT payload_json FROM testnet_audit_events
                        WHERE run_id=? AND event_type=?
                        """,
                        (normalized_run, "RECONCILIATION_FAILURE_APPLIED"),
                    ):
                        event_payload = _decode(
                            str(event_row["payload_json"]),
                            label="reconciliation failure audit payload",
                        )
                        if (
                            event_payload.get("snapshot_id") == expected_snapshot_id
                            and event_payload.get(
                                "reconciliation_failure_input_hash"
                            )
                            == failure_input_hash
                        ):
                            matching_event = True
                            break
                    if (
                        str(duplicate["payload_json"]) == expected_payload_json
                        and str(duplicate["payload_hash"]) == expected_payload_hash
                        and str(duplicate["received_at"]) == utc_text(at)
                        and not bool(int(duplicate["reconciled"]))
                        and matching_event
                    ):
                        result = self._snapshot_record(duplicate)
                        connection.rollback()
                        return result
                    raise IdempotencyConflictError(
                        "snapshot_id was reused with divergent reconciliation failure"
                    )
                result = self._insert_snapshot_tx(
                    connection,
                    normalized_run,
                    payload=payload,
                    received_at=at,
                    reconciled=False,
                    snapshot_id=expected_snapshot_id,
                )
                self._reconciliation_failure_fault_point("after_snapshot")
                current = RuntimeState(str(run["runtime_state"]))
                latched = current not in {
                    RuntimeState.KILLED,
                    RuntimeState.MANUAL_REVIEW,
                }
                if latched:
                    connection.execute(
                        """
                        UPDATE testnet_runs
                        SET runtime_state=?, state_reason=?, updated_at=?
                        WHERE run_id=?
                        """,
                        (
                            RuntimeState.MANUAL_REVIEW.value,
                            "RECONCILIATION_FAILURE",
                            utc_text(at),
                            normalized_run,
                        ),
                    )
                self._reconciliation_failure_fault_point("after_runtime_latch")
                self._append_audit(
                    connection,
                    normalized_run,
                    "RECONCILIATION_FAILURE_APPLIED",
                    {
                        "reconciliation_failure_input_hash": failure_input_hash,
                        "issues": list(normalized_issues),
                        "manual_review_latched": latched,
                        "snapshot_hash": result.payload_hash,
                        "snapshot_id": result.snapshot_id,
                    },
                    created_at=at,
                )
                self._reconciliation_failure_fault_point("after_audit")
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        assert result is not None
        return result

    def apply_reconciliation(
        self,
        run_id: str,
        *,
        positions: Mapping[str, Decimal],
        spot_balances: Mapping[str, Decimal],
        equity: Decimal,
        withdrawable: Decimal,
        open_orders: Sequence[Mapping[str, object]],
        order_updates: Sequence[OrderProjectionUpdate],
        fills: Sequence[ReconciliationFill],
        reconciled_at: datetime,
        action_resolutions: Sequence[ReconciliationActionResolution] = (),
        source_cursor: str | None = None,
        snapshot_id: str | None = None,
    ) -> RemoteSnapshotRecord:
        """Atomically apply authoritative facts and advance reconciliation freshness."""

        normalized_run = _sha256(run_id, label="run_id")
        at = _utc(reconciled_at, label="reconciled_at")
        self._require_wallet_lease(normalized_run)
        reconciliation_input_hash = _reconciliation_input_hash(
            order_updates,
            fills,
            action_resolutions,
        )
        payload = self._authoritative_snapshot_payload(
            positions=positions,
            spot_balances=spot_balances,
            equity=equity,
            withdrawable=withdrawable,
            open_orders=open_orders,
            source_cursor=source_cursor,
        )
        payload["reconciliation_input_hash"] = reconciliation_input_hash
        _, expected_payload_json, expected_payload_hash = _payload(
            payload,
            label="reconciled remote snapshot",
        )
        expected_snapshot_id = self._snapshot_identity(
            normalized_run,
            payload_hash=expected_payload_hash,
            received_at=at,
            snapshot_id=snapshot_id,
        )
        conflict: IdempotencyConflictError | OrderConflictError | None = None
        result: RemoteSnapshotRecord | None = None
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._require_run(connection, normalized_run)
                duplicate = connection.execute(
                    """
                    SELECT * FROM testnet_remote_snapshots
                    WHERE run_id=? AND snapshot_id=?
                    """,
                    (normalized_run, expected_snapshot_id),
                ).fetchone()
                if duplicate is not None:
                    if (
                        str(duplicate["payload_json"]) == expected_payload_json
                        and str(duplicate["payload_hash"]) == expected_payload_hash
                        and str(duplicate["received_at"]) == utc_text(at)
                        and bool(int(duplicate["reconciled"]))
                    ):
                        result = self._snapshot_record(duplicate)
                        connection.rollback()
                        return result
                    raise IdempotencyConflictError(
                        "snapshot_id was reused with divergent reconciliation inputs"
                    )
                for resolution in action_resolutions:
                    if not isinstance(
                        resolution,
                        ReconciliationActionResolution,
                    ):
                        raise TypeError(
                            "action_resolutions must contain "
                            "ReconciliationActionResolution values"
                        )
                    self._resolve_reconciliation_action_tx(
                        connection,
                        normalized_run,
                        resolution,
                        at=at,
                    )
                releasing_updates: list[OrderProjectionUpdate] = []
                binding_updates: list[OrderProjectionUpdate] = []
                for update in order_updates:
                    if not isinstance(update, OrderProjectionUpdate):
                        raise TypeError(
                            "order_updates must contain OrderProjectionUpdate values"
                        )
                    target = (
                        binding_updates
                        if update.status.reserves_exposure
                        else releasing_updates
                    )
                    target.append(update)
                # Native amend may reuse one venue OID. Release terminal originals
                # first, then bind the replacement, all within this transaction.
                for update in (*releasing_updates, *binding_updates):
                    self._apply_order_update(
                        connection,
                        normalized_run,
                        update,
                        at=at,
                    )
                for fill in fills:
                    if not isinstance(fill, ReconciliationFill):
                        raise TypeError("fills must contain ReconciliationFill values")
                    self._record_fill_tx(
                        connection,
                        normalized_run,
                        fill_id=fill.fill_id,
                        order_id=fill.order_id,
                        venue_order_id=fill.venue_order_id,
                        quantity=fill.quantity,
                        price=fill.price,
                        fee=fill.fee,
                        payload=fill.payload,
                        received_at=fill.received_at,
                        projection_at=at,
                    )
                result = self._insert_snapshot_tx(
                    connection,
                    normalized_run,
                    payload=payload,
                    received_at=at,
                    reconciled=True,
                    snapshot_id=expected_snapshot_id,
                )
                connection.execute(
                    """
                    UPDATE testnet_runs
                    SET last_reconciled_at=?, reconciliation_snapshot_hash=?,
                        reconciliation_snapshot_id=?, updated_at=?
                    WHERE run_id=?
                    """,
                    (
                        utc_text(at),
                        result.payload_hash,
                        result.snapshot_id,
                        utc_text(at),
                        normalized_run,
                    ),
                )
                self._append_audit(
                    connection,
                    normalized_run,
                    "RECONCILIATION_APPLIED",
                    {
                        "action_resolution_count": len(action_resolutions),
                        "fill_count": len(fills),
                        "order_update_count": len(order_updates),
                        "snapshot_hash": result.payload_hash,
                        "snapshot_id": result.snapshot_id,
                    },
                    created_at=at,
                )
                connection.commit()
            except (IdempotencyConflictError, OrderConflictError) as error:
                if connection.in_transaction:
                    connection.rollback()
                conflict = error
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        if conflict is not None:
            self.record_reconciliation_issue(
                normalized_run,
                "RECONCILIATION_CONFLICT",
                details={
                    "error_type": type(conflict).__name__,
                    "snapshot_id": expected_snapshot_id,
                },
                detected_at=at,
            )
            raise conflict
        assert result is not None
        return result

    def get_remote_snapshot(self, run_id: str, snapshot_id: str) -> RemoteSnapshotRecord:
        normalized_run = _sha256(run_id, label="run_id")
        normalized_snapshot = _sha256(snapshot_id, label="snapshot_id")
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM testnet_remote_snapshots
                WHERE run_id=? AND snapshot_id=?
                """,
                (normalized_run, normalized_snapshot),
            ).fetchone()
        if row is None:
            raise TestnetStoreError(f"unknown remote snapshot {normalized_snapshot!r}")
        return self._snapshot_record(row)

    def latest_remote_snapshot(self, run_id: str) -> RemoteSnapshotRecord | None:
        normalized = _sha256(run_id, label="run_id")
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM testnet_remote_snapshots
                WHERE run_id=? ORDER BY received_at DESC, snapshot_id DESC LIMIT 1
                """,
                (normalized,),
            ).fetchone()
        return self._snapshot_record(row) if row is not None else None

    def latest_reconciled_snapshot(
        self,
        run_id: str,
    ) -> RemoteSnapshotRecord | None:
        normalized = _sha256(run_id, label="run_id")
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT snapshots.* FROM testnet_remote_snapshots AS snapshots
                JOIN testnet_runs AS runs
                  ON runs.run_id=snapshots.run_id
                 AND runs.reconciliation_snapshot_id=snapshots.snapshot_id
                WHERE runs.run_id=? AND snapshots.reconciled=1
                """,
                (normalized,),
            ).fetchone()
        return self._snapshot_record(row) if row is not None else None

    def last_reconciled_at(self, run_id: str) -> datetime | None:
        return self.get_run(run_id).last_reconciled_at

    @staticmethod
    def _audit_record(row: sqlite3.Row) -> AuditEventRecord:
        return AuditEventRecord(
            run_id=str(row["run_id"]),
            sequence=int(row["sequence"]),
            event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            payload=_decode(str(row["payload_json"]), label="audit payload"),
            payload_hash=str(row["payload_hash"]),
            previous_hash=str(row["previous_hash"]),
            event_hash=str(row["event_hash"]),
            created_at=parse_utc(str(row["created_at"])),
        )

    def get_audit_events(self, run_id: str) -> tuple[AuditEventRecord, ...]:
        normalized = _sha256(run_id, label="run_id")
        with self._read_connection() as connection:
            rows = connection.execute(
                "SELECT * FROM testnet_audit_events WHERE run_id=? ORDER BY sequence",
                (normalized,),
            ).fetchall()
        return tuple(self._audit_record(row) for row in rows)

    def _collect_integrity_issues(
        self,
        connection: sqlite3.Connection,
        run_id: str,
    ) -> tuple[IntegrityIssue, ...]:
        issues: list[IntegrityIssue] = []

        def issue(code: str, detail: str) -> None:
            issues.append(IntegrityIssue(code, detail))

        objects = connection.execute(
            """
            SELECT type, name FROM sqlite_master
            WHERE type IN ('table', 'trigger', 'index')
            """
        ).fetchall()
        names_by_type = {
            object_type: {
                str(row["name"])
                for row in objects
                if str(row["type"]) == object_type
            }
            for object_type in ("table", "trigger", "index")
        }
        for table, expected_columns in _REQUIRED_COLUMNS.items():
            if table not in names_by_type["table"]:
                issue("SCHEMA_TABLE_MISSING", table)
                continue
            actual_columns = tuple(
                str(row["name"])
                for row in connection.execute(
                    f"PRAGMA table_info({table})"
                )
            )
            if actual_columns != expected_columns:
                issue("SCHEMA_COLUMNS", f"{table} columns differ")
        for trigger in sorted(_REQUIRED_TRIGGERS - names_by_type["trigger"]):
            issue("SCHEMA_TRIGGER_MISSING", trigger)
        for index in sorted(_REQUIRED_INDEXES - names_by_type["index"]):
            issue("SCHEMA_INDEX_MISSING", index)
        for table, expected_keys in _REQUIRED_FOREIGN_KEYS.items():
            actual_keys = frozenset(
                (
                    str(row["from"]),
                    str(row["table"]),
                    str(row["to"]),
                )
                for row in connection.execute(
                    f"PRAGMA foreign_key_list({table})"
                )
            )
            if actual_keys != expected_keys:
                issue("SCHEMA_FOREIGN_KEYS", f"{table} foreign keys differ")
        for row in connection.execute("PRAGMA foreign_key_check"):
            issue(
                "FOREIGN_KEY_VIOLATION",
                f"{row['table']} row {row['rowid']} references {row['parent']}",
            )

        run = self._require_run(connection, run_id)
        try:
            config_json = str(run["config_json"])
            config = TestnetConfig.from_json_bytes(config_json.encode("utf-8"))
            if config.config_hash != str(run["config_hash"]) or config.run_id != run_id:
                issue("CONFIG_BINDING", "config does not bind the durable run")
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            issue("CONFIG_INVALID", str(error))

        previous = _audit_genesis(run_id, str(run["config_hash"]))
        expected_sequence = 0
        expected_runtime = RuntimeState.STOPPED
        expected_reason: str | None = None
        expected_reconciled_at: str | None = None
        expected_snapshot_hash: str | None = None
        expected_snapshot_id: str | None = None
        expected_nonce = 0
        expected_updated_at = str(run["created_at"])
        expected_actions: dict[
            str,
            tuple[str, str | None, str | None],
        ] = {}
        expected_orders: dict[
            str,
            tuple[str, str, str | None, str | None, str],
        ] = {}
        audit_rows = connection.execute(
            "SELECT * FROM testnet_audit_events WHERE run_id=? ORDER BY sequence",
            (run_id,),
        ).fetchall()
        for row in audit_rows:
            expected_sequence += 1
            sequence = int(row["sequence"])
            if sequence != expected_sequence:
                issue("AUDIT_SEQUENCE", f"expected {expected_sequence}, found {sequence}")
            payload: dict[str, object] = {}
            try:
                payload = _decode(str(row["payload_json"]), label="audit payload")
                _secret_free(payload, path="audit payload")
                payload_hash = canonical_sha256(payload)
            except (TypeError, ValueError, SecretPersistenceError, IntegrityError) as error:
                issue("AUDIT_PAYLOAD", str(error))
                payload_hash = str(row["payload_hash"])
            if payload_hash != str(row["payload_hash"]):
                issue("AUDIT_PAYLOAD_HASH", f"sequence {sequence} payload differs")
            if str(row["previous_hash"]) != previous:
                issue("AUDIT_CHAIN", f"sequence {sequence} previous hash differs")
            expected_id = deterministic_id(
                "hyperliquid_testnet_audit_event_v1",
                run_id,
                sequence,
                str(row["event_type"]),
                str(row["payload_hash"]),
            )
            if str(row["event_id"]) != expected_id:
                issue("AUDIT_EVENT_ID", f"sequence {sequence} event id differs")
            expected_hash = canonical_sha256(
                {
                    "created_at": str(row["created_at"]),
                    "event_id": str(row["event_id"]),
                    "event_type": str(row["event_type"]),
                    "payload_hash": str(row["payload_hash"]),
                    "previous_hash": str(row["previous_hash"]),
                    "run_id": run_id,
                    "sequence": sequence,
                }
            )
            if str(row["event_hash"]) != expected_hash:
                issue("AUDIT_EVENT_HASH", f"sequence {sequence} hash differs")
            event_type = str(row["event_type"])
            event_created_at = str(row["created_at"])
            expected_updated_at = event_created_at
            try:
                if event_type == "RUNTIME_STATE_CHANGED":
                    expected_runtime = RuntimeState(str(payload["target"]))
                    raw_reason = payload.get("reason")
                    if raw_reason is not None and not isinstance(raw_reason, str):
                        raise TypeError("runtime reason must be text or null")
                    expected_reason = raw_reason
                elif event_type == "IDEMPOTENCY_CONFLICT":
                    if expected_runtime not in {
                        RuntimeState.KILLED,
                        RuntimeState.MANUAL_REVIEW,
                    }:
                        expected_runtime = RuntimeState.MANUAL_REVIEW
                        conflict_kind = str(payload.get("kind"))
                        expected_reason = {
                            "ACTION": "divergent action idempotency key",
                            "ACTION_OUTCOME": "DIVERGENT_ACTION_OUTCOME",
                            "ACTION_PROJECTION": "divergent completed action projection",
                            "AMBIGUOUS_ACTION_OBSERVATION": (
                                "DIVERGENT_AMBIGUOUS_OBSERVATION"
                            ),
                            "ORDER_INTENT": "divergent order intent identity",
                        }.get(conflict_kind, "IDEMPOTENCY_CONFLICT")
                elif event_type == "RECONCILIATION_ISSUE" and bool(
                    payload.get("manual_review_latched")
                ):
                    if expected_runtime is not RuntimeState.KILLED:
                        expected_runtime = RuntimeState.MANUAL_REVIEW
                        expected_reason = "RECONCILIATION_ISSUE"
                elif event_type == "RECONCILIATION_FAILURE_APPLIED" and bool(
                    payload.get("manual_review_latched")
                ):
                    if expected_runtime is not RuntimeState.KILLED:
                        expected_runtime = RuntimeState.MANUAL_REVIEW
                        expected_reason = "RECONCILIATION_FAILURE"
                elif event_type == "INTEGRITY_FAILURE_LATCHED":
                    if expected_runtime not in {
                        RuntimeState.KILLED,
                        RuntimeState.MANUAL_REVIEW,
                    }:
                        expected_runtime = RuntimeState.MANUAL_REVIEW
                        expected_reason = "persistent Testnet store integrity failure"
                elif event_type == "ORDER_INTENT_PERSISTED":
                    order_id = str(payload["order_id"])
                    expected_orders[order_id] = (
                        OrderStatus.REQUESTED.value,
                        "0",
                        None,
                        None,
                        event_created_at,
                    )
                elif event_type == "ORDER_STATUS_CHANGED":
                    order_id = str(payload["order_id"])
                    prior = expected_orders[order_id]
                    raw_venue = payload.get("venue_order_id")
                    venue = (
                        prior[3]
                        if raw_venue is None
                        else str(raw_venue)
                    )
                    expected_orders[order_id] = (
                        str(payload["target"]),
                        prior[1],
                        prior[2],
                        venue,
                        event_created_at,
                    )
                elif event_type == "ORDER_PROJECTION_UPDATED":
                    order_id = str(payload["order_id"])
                    raw_average = payload.get("average_fill_price")
                    raw_venue = payload.get("venue_order_id")
                    expected_orders[order_id] = (
                        str(payload["status"]),
                        str(payload["filled_quantity"]),
                        str(raw_average) if raw_average is not None else None,
                        str(raw_venue) if raw_venue is not None else None,
                        event_created_at,
                    )
                elif event_type == "ACTION_RESERVED_AMBIGUOUS":
                    action_id = str(payload["action_id"])
                    expected_actions[action_id] = (
                        ActionAttemptStatus.AMBIGUOUS.value,
                        None,
                        None,
                    )
                    raw_nonce = payload["nonce"]
                    if isinstance(raw_nonce, bool) or not isinstance(raw_nonce, int):
                        raise TypeError("action nonce must be an integer")
                    expected_nonce = max(expected_nonce, raw_nonce)
                elif event_type == "AMBIGUOUS_ACTION_OBSERVED":
                    action_id = str(payload["action_id"])
                    expected_actions[action_id] = (
                        ActionAttemptStatus.AMBIGUOUS.value,
                        str(payload["response_hash"]),
                        None,
                    )
                elif event_type == "ACTION_RESOLVED":
                    action_id = str(payload["action_id"])
                    expected_actions[action_id] = (
                        str(payload["status"]),
                        str(payload["response_hash"]),
                        event_created_at,
                    )
                elif event_type == "NONCE_ALLOCATED":
                    raw_nonce = payload["nonce"]
                    if isinstance(raw_nonce, bool) or not isinstance(raw_nonce, int):
                        raise TypeError("allocated nonce must be an integer")
                    expected_nonce = max(expected_nonce, raw_nonce)
                elif event_type == "RECONCILIATION_APPLIED":
                    expected_reconciled_at = event_created_at
                    expected_snapshot_hash = str(payload["snapshot_hash"])
                    expected_snapshot_id = str(payload["snapshot_id"])
            except (KeyError, TypeError, ValueError) as error:
                issue(
                    "AUDIT_PROJECTION_EVENT_INVALID",
                    f"sequence {sequence}: {error}",
                )
            previous = str(row["event_hash"])
        if expected_sequence != int(run["audit_count"]):
            issue("AUDIT_COUNT", "audit_count differs from append-only rows")
        if previous != str(run["audit_head_hash"]):
            issue("AUDIT_HEAD", "audit_head_hash differs from computed chain")
        if str(run["runtime_state"]) != expected_runtime.value:
            issue(
                "RUNTIME_PROJECTION",
                f"expected {expected_runtime.value}, found {run['runtime_state']}",
            )
        if cast(str | None, run["state_reason"]) != expected_reason:
            issue("RUNTIME_REASON_PROJECTION", "state_reason differs from audit replay")
        if int(run["last_nonce"]) != expected_nonce:
            issue("NONCE_PROJECTION", "last_nonce differs from audit replay")
        if cast(str | None, run["last_reconciled_at"]) != expected_reconciled_at:
            issue(
                "RECONCILIATION_TIME_PROJECTION",
                "last_reconciled_at differs from audit replay",
            )
        if (
            cast(str | None, run["reconciliation_snapshot_hash"])
            != expected_snapshot_hash
        ):
            issue(
                "RECONCILIATION_HASH_PROJECTION",
                "reconciliation_snapshot_hash differs from audit replay",
            )
        if cast(str | None, run["reconciliation_snapshot_id"]) != expected_snapshot_id:
            issue(
                "RECONCILIATION_ID_PROJECTION",
                "reconciliation_snapshot_id differs from audit replay",
            )
        if expected_snapshot_id is not None:
            authoritative = connection.execute(
                """
                SELECT payload_hash, received_at, reconciled
                FROM testnet_remote_snapshots
                WHERE run_id=? AND snapshot_id=?
                """,
                (run_id, expected_snapshot_id),
            ).fetchone()
            if (
                authoritative is None
                or str(authoritative["payload_hash"]) != expected_snapshot_hash
                or str(authoritative["received_at"]) != expected_reconciled_at
                or not bool(int(authoritative["reconciled"]))
            ):
                issue(
                    "RECONCILIATION_SNAPSHOT_BINDING",
                    "run pointer does not bind an exact reconciled snapshot",
                )
        if str(run["updated_at"]) != expected_updated_at:
            issue("RUN_UPDATED_AT_PROJECTION", "updated_at differs from audit replay")

        active_venue_ids: dict[str, str] = {}
        order_rows = connection.execute(
            "SELECT * FROM testnet_orders WHERE run_id=?",
            (run_id,),
        ).fetchall()
        seen_orders: set[str] = set()
        for row in order_rows:
            try:
                intent_payload = _decode(str(row["intent_json"]), label="order intent")
                _secret_free(intent_payload, path="order intent")
                intent = TestnetOrderIntent.from_dict(intent_payload)
                if canonical_sha256(intent_payload) != str(row["intent_hash"]):
                    issue("ORDER_INTENT_HASH", f"order {row['order_id']} hash differs")
                if intent.order_id != str(row["order_id"]) or intent.cloid != str(row["cloid"]):
                    issue("ORDER_IDENTITY", f"order {row['order_id']} identity differs")
                order = self._order_record(row)
                order_id = order.intent.order_id
                seen_orders.add(order_id)
                expected_order = expected_orders.get(order_id)
                actual_projection = (
                    order.status.value,
                    str(row["filled_quantity_text"]),
                    cast(str | None, row["average_fill_price_text"]),
                    order.venue_order_id,
                    str(row["updated_at"]),
                )
                if expected_order is None:
                    issue("ORDER_AUDIT_MISSING", f"order {order_id} has no creation event")
                elif actual_projection != expected_order:
                    issue(
                        "ORDER_PROJECTION",
                        f"order {order_id} differs from audit replay",
                    )
                if order.status.reserves_exposure and order.venue_order_id is not None:
                    prior_order_id = active_venue_ids.setdefault(
                        order.venue_order_id,
                        order.intent.order_id,
                    )
                    if prior_order_id != order.intent.order_id:
                        issue(
                            "ACTIVE_VENUE_ORDER_DUPLICATE",
                            f"venue order {order.venue_order_id} maps to multiple active orders",
                        )
            except (TypeError, ValueError, IntegrityError, SecretPersistenceError) as error:
                issue("ORDER_INVALID", f"order {row['order_id']}: {error}")
        for missing_order in sorted(set(expected_orders) - seen_orders):
            issue(
                "ORDER_ROW_MISSING",
                f"audit references missing order {missing_order}",
            )

        max_nonce = 0
        seen_actions: set[str] = set()
        for row in connection.execute(
            "SELECT * FROM testnet_actions WHERE run_id=? ORDER BY nonce",
            (run_id,),
        ):
            max_nonce = max(max_nonce, int(row["nonce"]))
            action_id = str(row["action_id"])
            seen_actions.add(action_id)
            expected_action = expected_actions.get(action_id)
            actual_action = (
                str(row["status"]),
                cast(str | None, row["response_hash"]),
                cast(str | None, row["resolved_at"]),
            )
            if expected_action is None:
                issue(
                    "ACTION_AUDIT_MISSING",
                    f"action {action_id} has no reservation event",
                )
            elif actual_action != expected_action:
                issue(
                    "ACTION_PROJECTION",
                    f"action {action_id} differs from audit replay",
                )
            try:
                payload = _decode(str(row["payload_json"]), label="action payload")
                _secret_free(payload, path="action payload")
                if canonical_sha256(payload) != str(row["payload_hash"]):
                    issue("ACTION_PAYLOAD_HASH", f"action {row['action_id']} differs")
                if row["response_json"] is not None:
                    response = _decode(str(row["response_json"]), label="action response")
                    _secret_free(response, path="action response")
                    if canonical_sha256(response) != str(row["response_hash"]):
                        issue(
                            "ACTION_RESPONSE_HASH",
                            f"action {row['action_id']} response differs",
                        )
            except (TypeError, ValueError, IntegrityError, SecretPersistenceError) as error:
                issue("ACTION_INVALID", f"action {row['action_id']}: {error}")
        for missing_action in sorted(set(expected_actions) - seen_actions):
            issue(
                "ACTION_ROW_MISSING",
                f"audit references missing action {missing_action}",
            )
        if int(run["last_nonce"]) < max_nonce:
            issue("NONCE_ROLLBACK", "last_nonce is below a durable action nonce")

        for table, id_column, code in (
            ("testnet_fills", "fill_id", "FILL_PAYLOAD_HASH"),
            ("testnet_remote_snapshots", "snapshot_id", "SNAPSHOT_PAYLOAD_HASH"),
        ):
            for row in connection.execute(
                f"SELECT * FROM {table} WHERE run_id=?",
                (run_id,),
            ):
                try:
                    payload = _decode(str(row["payload_json"]), label=table)
                    _secret_free(payload, path=table)
                    if canonical_sha256(payload) != str(row["payload_hash"]):
                        issue(code, f"{row[id_column]} payload differs")
                except (TypeError, ValueError, IntegrityError, SecretPersistenceError) as error:
                    issue(code, f"{row[id_column]}: {error}")

        fill_order_ids = {
            str(row["order_id"])
            for row in connection.execute(
                "SELECT DISTINCT order_id FROM testnet_fills WHERE run_id=?",
                (run_id,),
            )
        }
        aggregate_order_ids: set[str] = set()
        for row in connection.execute(
            "SELECT * FROM testnet_fill_aggregates WHERE run_id=?",
            (run_id,),
        ):
            aggregate_order_ids.add(str(row["order_id"]))
            totals = connection.execute(
                """
                SELECT quantity_text, price_text, fee_text FROM testnet_fills
                WHERE run_id=? AND order_id=?
                """,
                (run_id, str(row["order_id"])),
            ).fetchall()
            quantity = sum(
                (Decimal(str(item["quantity_text"])) for item in totals),
                Decimal(0),
            )
            notional = sum(
                (
                    Decimal(str(item["quantity_text"]))
                    * Decimal(str(item["price_text"]))
                    for item in totals
                ),
                Decimal(0),
            )
            fee = sum(
                (Decimal(str(item["fee_text"])) for item in totals),
                Decimal(0),
            )
            if (
                decimal_text(quantity) != str(row["stable_quantity_text"])
                or decimal_text(notional) != str(row["stable_notional_text"])
                or decimal_text(fee) != str(row["stable_fee_text"])
            ):
                issue(
                    "FILL_AGGREGATE",
                    f"order {row['order_id']} stable fill aggregate differs",
                )
        for order_id in sorted(fill_order_ids - aggregate_order_ids):
            issue("FILL_AGGREGATE_MISSING", f"order {order_id} has fills but no aggregate")
        for order_id in sorted(aggregate_order_ids - fill_order_ids):
            issue("FILL_AGGREGATE_ORPHAN", f"order {order_id} aggregate has no fills")
        return tuple(issues)

    def inspect_integrity_readonly(self, run_id: str) -> IntegrityReport:
        normalized = _sha256(run_id, label="run_id")
        with self._read_connection() as connection:
            run = self._require_run(connection, normalized)
            issues = self._collect_integrity_issues(connection, normalized)
            return IntegrityReport(
                normalized,
                not issues,
                int(run["audit_count"]),
                str(run["audit_head_hash"]),
                issues,
            )

    def verify_integrity(
        self,
        run_id: str,
        *,
        raise_on_error: bool = True,
    ) -> IntegrityReport:
        normalized = _sha256(run_id, label="run_id")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                run = self._require_run(connection, normalized)
                issues = self._collect_integrity_issues(connection, normalized)
                if issues:
                    current = RuntimeState(str(run["runtime_state"]))
                    if current not in {
                        RuntimeState.KILLED,
                        RuntimeState.MANUAL_REVIEW,
                    }:
                        connection.execute(
                            """
                            UPDATE testnet_runs SET runtime_state=?, state_reason=?
                            WHERE run_id=?
                            """,
                            (
                                RuntimeState.MANUAL_REVIEW.value,
                                "persistent Testnet store integrity failure",
                                normalized,
                            ),
                        )
                    self._append_audit(
                        connection,
                        normalized,
                        "INTEGRITY_FAILURE_LATCHED",
                        {"issue_codes": [item.code for item in issues]},
                        created_at=_now(),
                    )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        durable = self.get_run(normalized)
        report = IntegrityReport(
            normalized,
            not issues,
            durable.audit_count,
            durable.audit_head_hash,
            issues,
        )
        if issues and raise_on_error:
            raise IntegrityError(report)
        return report


__all__ = [
    "ACCOUNT_ACTION_IDENTITY_CAPACITY",
    "SCHEMA_VERSION",
    "ActionAttemptRecord",
    "AmbiguousActionReplayError",
    "AuditEventRecord",
    "FillRecord",
    "IdempotencyConflictError",
    "IntegrityError",
    "IntegrityIssue",
    "IntegrityReport",
    "OrderConflictError",
    "OrderProjectionUpdate",
    "ReconciliationActionResolution",
    "ReconciliationFill",
    "ReconciliationIssue",
    "RemoteSnapshotRecord",
    "RunConflictError",
    "RunNotFoundError",
    "RunRecord",
    "SchemaVersionError",
    "SecretPersistenceError",
    "TestnetStore",
    "TestnetStoreError",
    "WalletLeaseError",
    "WalletLeaseRecord",
    "deterministic_action_id",
]
