from __future__ import annotations

import json
import sqlite3
import zlib
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal, DecimalException, InvalidOperation
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import Protocol, Self, cast

from hyperlab.backtest.protocol import JsonValue, canonical_json, canonical_sha256
from hyperlab.paper.models import (
    LedgerEntry,
    PaperEvent,
    PaperEventType,
    PaperProjection,
    PaperRunConfig,
    StoredPaperEvent,
    deterministic_id,
)
from hyperlab.paper.reducer import (
    PAPER_CASH_MATH_VERSION,
    apply_event,
    transaction_ledger_amounts,
)

SCHEMA_VERSION = 1
STORE_SCHEMA_VERSION = 3
GENESIS_SEQUENCE = 0
ZERO_HASH = "0" * 64


def _raise_if_interrupted(should_stop: Callable[[], bool] | None) -> None:
    if should_stop is not None and should_stop():
        raise InterruptedError("paper startup interrupted")

FAULT_STAGES = frozenset(
    {
        "before_begin",
        "after_input",
        "after_events",
        "after_ledger",
        "before_commit",
        "after_commit",
    }
)

PAPER_STATES = frozenset(
    {
        "FLAT",
        "ENTRY_PLANNED",
        "LEG_1_PENDING",
        "HEDGE_PENDING",
        "HEDGED",
        "EXIT_PLANNED",
        "EXIT_PENDING",
        "PAUSED",
        "REDUCE_ONLY",
        "MANUAL_REVIEW",
        "EMERGENCY_FLATTEN",
    }
)

_REQUIRED_APPEND_ONLY_TRIGGERS = frozenset(
    {
        "paper_alerts_no_delete",
        "paper_alerts_no_update",
        "paper_commits_no_delete",
        "paper_commits_no_update",
        "paper_events_no_delete",
        "paper_events_no_update",
        "paper_inbox_no_delete",
        "paper_inbox_no_update",
        "paper_ledger_entries_no_delete",
        "paper_ledger_entries_no_update",
        "paper_ledger_transactions_no_delete",
        "paper_ledger_transactions_no_update",
        "paper_projection_history_no_delete",
        "paper_projection_history_no_update",
        "paper_runs_config_immutable",
        "paper_runs_no_delete",
    }
)

_REQUIRED_SCHEMA_TABLES = frozenset(
    {
        "paper_alerts",
        "paper_commits",
        "paper_events",
        "paper_inbox",
        "paper_ledger_entries",
        "paper_ledger_transactions",
        "paper_projection_history",
        "paper_projections",
        "paper_runs",
        "paper_schema",
    }
)

_REQUIRED_HOT_PATH_INDEXES = frozenset(
    {
        "paper_alerts_commit_idx",
        "paper_alerts_run_severity_sequence_idx",
        "paper_events_input_idx",
        "paper_ledger_transactions_input_idx",
    }
)


class CanonicalRecord(Protocol):
    """Minimal model contract used by the store.

    Paper models deliberately own their domain validation.  The store only needs
    their deterministic JSON representation, which keeps this module independent
    from the execution engine and from any network-facing package.
    """

    def to_dict(self) -> Mapping[str, object]: ...


FaultInjector = Callable[[str], None]


class PaperStoreError(RuntimeError):
    """Base class for fail-closed paper-store errors."""


class SchemaVersionError(PaperStoreError):
    """The database schema is absent, inconsistent, or newer than this code."""


class RunNotFoundError(PaperStoreError):
    """The requested immutable paper run does not exist."""


class RunConflictError(PaperStoreError):
    """A deterministic run identifier was reused with different configuration."""


class IdempotencyConflictError(PaperStoreError):
    """An idempotency identifier was reused outside its one durable input."""


class AppendConflictError(PaperStoreError):
    """Supplied derived state differs from independent event replay."""


class ConcurrentWriteError(PaperStoreError):
    """The caller's expected event sequence no longer matches the durable head."""


class LedgerImbalanceError(PaperStoreError):
    """A ledger transaction is not exactly balanced in every unit."""


class IntegrityError(PaperStoreError):
    """Durable paper state failed verification and was latched for manual review."""

    def __init__(self, report: IntegrityReport) -> None:
        self.report = report
        detail = "; ".join(f"{issue.code}: {issue.detail}" for issue in report.issues)
        super().__init__(f"paper store integrity failure for {report.run_id}: {detail}")


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: str
    config_snapshot: dict[str, JsonValue]
    config_hash: str
    seed: int | None
    status: str
    created_at: str
    event_sequence: int
    event_head_hash: str
    commit_sequence: int
    commit_head_hash: str
    projection_revision: int
    projection_hash: str

    @property
    def head_identity(self) -> tuple[str, str, str, int, str, int, str, int, str]:
        """Return the exact mutable durable-head identity for coherent reads."""

        return (
            self.run_id,
            self.config_hash,
            self.status,
            self.event_sequence,
            self.event_head_hash,
            self.commit_sequence,
            self.commit_head_hash,
            self.projection_revision,
            self.projection_hash,
        )


@dataclass(frozen=True, slots=True)
class InputRecord:
    run_id: str
    input_id: str
    payload: dict[str, JsonValue]
    payload_hash: str
    first_event_sequence: int | None
    last_event_sequence: int
    commit_sequence: int
    commit_hash: str
    created_at: str


@dataclass(frozen=True, slots=True)
class StoredEventRecord:
    run_id: str
    sequence: int
    event_id: str
    event_type: str
    event: dict[str, JsonValue]
    payload_hash: str
    previous_hash: str
    event_hash: str
    input_id: str
    created_at: str

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "created_at": self.created_at,
            "event": self.event,
            "event_hash": self.event_hash,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "input_id": self.input_id,
            "payload_hash": self.payload_hash,
            "previous_hash": self.previous_hash,
            "run_id": self.run_id,
            "sequence": self.sequence,
        }


@dataclass(frozen=True, slots=True)
class ProjectionHistoryRecord:
    """One immutable projection revision returned by a bounded read query."""

    run_id: str
    revision: int
    input_id: str | None
    event_sequence: int
    event_head_hash: str
    status: str
    projection: dict[str, JsonValue]
    utc_date: str
    projection_hash: str
    created_at: str


@dataclass(frozen=True, slots=True)
class LedgerRecord:
    run_id: str
    transaction_id: str
    entry_index: int
    entry_id: str
    event_id: str | None
    account: str
    unit: str
    amount: Decimal
    entry: dict[str, JsonValue]
    entry_hash: str


@dataclass(frozen=True, slots=True)
class AlertRecord:
    run_id: str
    alert_id: str
    commit_sequence: int | None
    event_sequence: int
    severity: str
    code: str
    alert: dict[str, JsonValue]
    payload_hash: str
    created_at: str


@dataclass(frozen=True, slots=True)
class AppendResult:
    run_id: str
    input_id: str
    first_sequence: int | None
    last_sequence: int
    appended_event_count: int
    event_head_hash: str
    commit_sequence: int
    commit_hash: str
    idempotent: bool = False


@dataclass(frozen=True, slots=True)
class IntegrityIssue:
    code: str
    detail: str


@dataclass(frozen=True, slots=True)
class IntegrityReport:
    run_id: str
    ok: bool
    event_count: int
    event_head_hash: str
    commit_count: int
    commit_head_hash: str
    issues: tuple[IntegrityIssue, ...] = ()

    def __bool__(self) -> bool:
        return self.ok


class _IntegrityIssueCollector:
    """Retain the first deterministic occurrence of each finite issue code."""

    def __init__(self) -> None:
        self._by_code: dict[str, IntegrityIssue] = {}

    def add(self, code: str, detail: str) -> None:
        self._by_code.setdefault(code, IntegrityIssue(code, detail))

    def __len__(self) -> int:
        return len(self._by_code)

    def freeze(self) -> tuple[IntegrityIssue, ...]:
        return tuple(self._by_code.values())


@dataclass(frozen=True, slots=True)
class _PreparedEvent:
    sequence: int
    event_id: str
    event_type: str
    payload_json: str
    payload_hash: str
    previous_hash: str
    event_hash: str


@dataclass(frozen=True, slots=True)
class _PreparedLedgerEntry:
    transaction_id: str
    entry_index: int
    entry_id: str
    event_id: str | None
    account: str
    unit: str
    amount_text: str
    payload_json: str
    entry_hash: str


@dataclass(frozen=True, slots=True)
class _PreparedLedgerTransaction:
    transaction_id: str
    event_sequence: int
    entries: tuple[_PreparedLedgerEntry, ...]
    transaction_hash: str


@dataclass(frozen=True, slots=True)
class _PreparedAlert:
    alert_id: str
    event_sequence: int
    severity: str
    code: str
    payload_json: str
    payload_hash: str


_SCHEMA_SQL = """
CREATE TABLE paper_schema (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL CHECK (version > 0),
    created_at TEXT NOT NULL
);

CREATE TABLE paper_runs (
    run_id TEXT NOT NULL PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    config_json TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    seed_text TEXT,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    event_count INTEGER NOT NULL CHECK (event_count >= 0),
    event_head_hash TEXT NOT NULL,
    commit_count INTEGER NOT NULL CHECK (commit_count >= 0),
    commit_head_hash TEXT NOT NULL,
    projection_revision INTEGER NOT NULL CHECK (projection_revision >= 0),
    projection_hash TEXT NOT NULL
);

CREATE TABLE paper_inbox (
    run_id TEXT NOT NULL,
    input_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    first_event_sequence INTEGER,
    last_event_sequence INTEGER NOT NULL CHECK (last_event_sequence >= 0),
    commit_sequence INTEGER NOT NULL CHECK (commit_sequence > 0),
    commit_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, input_id),
    UNIQUE (run_id, commit_sequence),
    FOREIGN KEY (run_id) REFERENCES paper_runs(run_id)
);

CREATE TABLE paper_events (
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL,
    input_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, sequence),
    UNIQUE (run_id, event_id),
    UNIQUE (run_id, event_hash),
    FOREIGN KEY (run_id, input_id) REFERENCES paper_inbox(run_id, input_id)
);

CREATE TABLE paper_ledger_transactions (
    run_id TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    input_id TEXT NOT NULL,
    event_sequence INTEGER NOT NULL CHECK (event_sequence >= 0),
    entry_count INTEGER NOT NULL CHECK (entry_count >= 2),
    transaction_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, transaction_id),
    UNIQUE (run_id, transaction_hash),
    FOREIGN KEY (run_id, input_id) REFERENCES paper_inbox(run_id, input_id)
);

CREATE TABLE paper_ledger_entries (
    run_id TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    entry_index INTEGER NOT NULL CHECK (entry_index >= 0),
    entry_id TEXT NOT NULL,
    event_id TEXT,
    account TEXT NOT NULL,
    unit TEXT NOT NULL,
    amount_text TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    entry_hash TEXT NOT NULL,
    PRIMARY KEY (run_id, transaction_id, entry_index),
    UNIQUE (run_id, entry_id),
    FOREIGN KEY (run_id, transaction_id)
        REFERENCES paper_ledger_transactions(run_id, transaction_id)
);

CREATE TABLE paper_projections (
    run_id TEXT NOT NULL PRIMARY KEY,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    event_sequence INTEGER NOT NULL CHECK (event_sequence >= 0),
    event_head_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    effective_status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    projection_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES paper_runs(run_id)
);

CREATE TABLE paper_projection_history (
    run_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    input_id TEXT,
    event_sequence INTEGER NOT NULL CHECK (event_sequence >= 0),
    event_head_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_zlib BLOB,
    payload_codec TEXT NOT NULL DEFAULT 'json',
    last_received_at TEXT,
    utc_date TEXT,
    projection_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, revision),
    FOREIGN KEY (run_id) REFERENCES paper_runs(run_id)
);

CREATE TABLE paper_alerts (
    run_id TEXT NOT NULL,
    alert_id TEXT NOT NULL,
    commit_sequence INTEGER,
    event_sequence INTEGER NOT NULL CHECK (event_sequence >= 0),
    severity TEXT NOT NULL,
    code TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, alert_id),
    FOREIGN KEY (run_id) REFERENCES paper_runs(run_id)
);

CREATE TABLE paper_commits (
    run_id TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL CHECK (commit_sequence > 0),
    input_id TEXT NOT NULL,
    first_event_sequence INTEGER,
    last_event_sequence INTEGER NOT NULL CHECK (last_event_sequence >= 0),
    event_hashes_json TEXT NOT NULL,
    ledger_hashes_json TEXT NOT NULL,
    projection_revision INTEGER NOT NULL CHECK (projection_revision > 0),
    projection_hash TEXT NOT NULL,
    alert_hashes_json TEXT NOT NULL,
    previous_commit_hash TEXT NOT NULL,
    commit_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, commit_sequence),
    UNIQUE (run_id, input_id),
    UNIQUE (run_id, commit_hash),
    FOREIGN KEY (run_id, input_id) REFERENCES paper_inbox(run_id, input_id)
);

CREATE INDEX paper_events_input_idx ON paper_events(run_id, input_id, sequence);
CREATE INDEX paper_ledger_event_idx ON paper_ledger_entries(run_id, event_id);
CREATE INDEX paper_alerts_sequence_idx ON paper_alerts(run_id, event_sequence, alert_id);
CREATE INDEX paper_alerts_run_severity_sequence_idx
    ON paper_alerts(run_id, severity, event_sequence DESC);
CREATE INDEX paper_ledger_transactions_input_idx
    ON paper_ledger_transactions(run_id, input_id);
CREATE INDEX paper_alerts_commit_idx ON paper_alerts(run_id, commit_sequence);

CREATE TRIGGER paper_events_no_update
BEFORE UPDATE ON paper_events BEGIN
    SELECT RAISE(ABORT, 'paper events are append-only');
END;
CREATE TRIGGER paper_events_no_delete
BEFORE DELETE ON paper_events BEGIN
    SELECT RAISE(ABORT, 'paper events are append-only');
END;
CREATE TRIGGER paper_inbox_no_update
BEFORE UPDATE ON paper_inbox BEGIN
    SELECT RAISE(ABORT, 'paper inbox is append-only');
END;
CREATE TRIGGER paper_inbox_no_delete
BEFORE DELETE ON paper_inbox BEGIN
    SELECT RAISE(ABORT, 'paper inbox is append-only');
END;
CREATE TRIGGER paper_ledger_transactions_no_update
BEFORE UPDATE ON paper_ledger_transactions BEGIN
    SELECT RAISE(ABORT, 'paper ledger is append-only');
END;
CREATE TRIGGER paper_ledger_transactions_no_delete
BEFORE DELETE ON paper_ledger_transactions BEGIN
    SELECT RAISE(ABORT, 'paper ledger is append-only');
END;
CREATE TRIGGER paper_ledger_entries_no_update
BEFORE UPDATE ON paper_ledger_entries BEGIN
    SELECT RAISE(ABORT, 'paper ledger is append-only');
END;
CREATE TRIGGER paper_ledger_entries_no_delete
BEFORE DELETE ON paper_ledger_entries BEGIN
    SELECT RAISE(ABORT, 'paper ledger is append-only');
END;
CREATE TRIGGER paper_projection_history_no_update
BEFORE UPDATE ON paper_projection_history BEGIN
    SELECT RAISE(ABORT, 'paper projection history is append-only');
END;
CREATE TRIGGER paper_projection_history_no_delete
BEFORE DELETE ON paper_projection_history BEGIN
    SELECT RAISE(ABORT, 'paper projection history is append-only');
END;
CREATE TRIGGER paper_commits_no_update
BEFORE UPDATE ON paper_commits BEGIN
    SELECT RAISE(ABORT, 'paper commits are append-only');
END;
CREATE TRIGGER paper_commits_no_delete
BEFORE DELETE ON paper_commits BEGIN
    SELECT RAISE(ABORT, 'paper commits are append-only');
END;
CREATE TRIGGER paper_alerts_no_update
BEFORE UPDATE ON paper_alerts BEGIN
    SELECT RAISE(ABORT, 'paper alerts are append-only');
END;
CREATE TRIGGER paper_alerts_no_delete
BEFORE DELETE ON paper_alerts BEGIN
    SELECT RAISE(ABORT, 'paper alerts are append-only');
END;
CREATE TRIGGER paper_runs_config_immutable
BEFORE UPDATE ON paper_runs
WHEN OLD.run_id != NEW.run_id
  OR OLD.schema_version != NEW.schema_version
  OR OLD.config_json != NEW.config_json
  OR OLD.config_hash != NEW.config_hash
  OR COALESCE(OLD.seed_text, '') != COALESCE(NEW.seed_text, '')
  OR OLD.created_at != NEW.created_at
BEGIN
    SELECT RAISE(ABORT, 'paper run configuration is immutable');
END;
CREATE TRIGGER paper_runs_no_delete
BEFORE DELETE ON paper_runs BEGIN
    SELECT RAISE(ABORT, 'paper runs cannot be deleted');
END;
"""


def _now_text() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _identifier(value: object, *, label: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{label} cannot be empty")
    if any(character.isspace() for character in normalized):
        raise ValueError(f"{label} cannot contain whitespace")
    return normalized


def _decimal_text(value: object, *, label: str) -> str:
    if isinstance(value, (bool, float)):
        raise TypeError(f"{label} must use Decimal, int, or a decimal string; floats are forbidden")
    try:
        decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{label} is not a valid decimal") from error
    if not decimal_value.is_finite():
        raise ValueError(f"{label} must be finite")
    if decimal_value.is_zero():
        return "0"
    return format(decimal_value.normalize(), "f")


def _json_value(value: object, *, label: str) -> JsonValue:
    if isinstance(value, Enum):
        return _json_value(value.value, label=label)
    if isinstance(value, Decimal):
        return _decimal_text(value, label=label)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{label} datetime must be timezone-aware")
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    if value is None or isinstance(value, (bool, int, float, str)):
        return cast(JsonValue, value)
    if isinstance(value, Mapping):
        normalized: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{label} contains a non-string mapping key")
            normalized[key] = _json_value(item, label=f"{label}.{key}")
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item, label=f"{label}[{index}]") for index, item in enumerate(value)]
    raise TypeError(f"{label} contains unsupported value type {type(value).__name__}")


def _record_dict(value: object, *, label: str) -> dict[str, JsonValue]:
    raw: object
    to_dict = getattr(value, "to_dict", None)
    unsigned_dict = getattr(value, "unsigned_dict", None)
    if callable(to_dict):
        raw = to_dict()
    elif callable(unsigned_dict):
        raw = unsigned_dict()
    elif isinstance(value, Mapping):
        raw = value
    elif is_dataclass(value) and not isinstance(value, type):
        raw = asdict(value)
    else:
        raise TypeError(f"{label} must be a mapping, dataclass, or expose to_dict()")
    normalized = _json_value(raw, label=label)
    if not isinstance(normalized, dict):
        raise TypeError(f"{label} must serialize to a JSON object")
    return normalized


def _canonical_record(value: object, *, label: str) -> tuple[dict[str, JsonValue], str, str]:
    payload = _record_dict(value, label=label)
    payload_json = canonical_json(payload)
    return payload, payload_json, canonical_sha256(payload)


def _json_object(value: str, *, label: str) -> dict[str, JsonValue]:
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as error:
        raise ValueError(f"{label} is not valid JSON") from error
    if not isinstance(decoded, dict):
        raise ValueError(f"{label} must contain a JSON object")
    return cast(dict[str, JsonValue], decoded)



_HISTORY_CODEC_JSON = "json"
_HISTORY_CODEC_ZLIB_JSON_V1 = "zlib-json-v1"


def _projection_history_storage(
    payload: Mapping[str, JsonValue],
    payload_json: str,
) -> tuple[str, bytes, str, str | None, str | None]:
    raw_last_received_at = payload.get("last_received_at")
    last_received_at = (
        raw_last_received_at
        if isinstance(raw_last_received_at, str)
        else None
    )
    utc_date = (
        last_received_at[:10]
        if last_received_at is not None and len(last_received_at) >= 10
        else None
    )
    compressed = zlib.compress(payload_json.encode("utf-8"), level=6)
    return (
        "",
        compressed,
        _HISTORY_CODEC_ZLIB_JSON_V1,
        last_received_at,
        utc_date,
    )


def _projection_history_json(row: sqlite3.Row, *, label: str) -> str:
    keys = set(row.keys())
    codec = (
        str(row["payload_codec"])
        if "payload_codec" in keys
        else _HISTORY_CODEC_JSON
    )
    if codec == _HISTORY_CODEC_JSON:
        return str(row["payload_json"])
    if codec != _HISTORY_CODEC_ZLIB_JSON_V1:
        raise ValueError(f"{label} uses unsupported payload codec {codec!r}")
    if "payload_zlib" not in keys or row["payload_zlib"] is None:
        raise ValueError(f"{label} compressed payload is missing")
    raw = row["payload_zlib"]
    if not isinstance(raw, (bytes, bytearray, memoryview)):
        raise ValueError(f"{label} compressed payload has invalid storage type")
    try:
        return zlib.decompress(bytes(raw)).decode("utf-8")
    except (zlib.error, UnicodeDecodeError) as error:
        raise ValueError(f"{label} compressed payload is invalid") from error


def _projection_history_payload(
    row: sqlite3.Row,
    *,
    label: str,
) -> dict[str, JsonValue]:
    return _json_object(_projection_history_json(row, label=label), label=label)


def _state_from_projection(payload: Mapping[str, JsonValue], *, default: str) -> str:
    raw = payload.get("state", payload.get("status", default))
    state = raw.value if isinstance(raw, Enum) else raw
    if not isinstance(state, str) or state not in PAPER_STATES:
        raise ValueError(f"paper projection state must be one of {sorted(PAPER_STATES)}")
    return state


def _optional_int(payload: Mapping[str, JsonValue], names: Sequence[str]) -> int | None:
    for name in names:
        if name not in payload:
            continue
        value = payload[name]
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"projection {name} must be an integer")
        return value
    return None


def _optional_text(payload: Mapping[str, JsonValue], names: Sequence[str]) -> str | None:
    for name in names:
        if name not in payload:
            continue
        value = payload[name]
        if value is None:
            return None
        if not isinstance(value, str):
            raise TypeError(f"projection {name} must be a string")
        return value
    return None


def _event_genesis(run_id: str, config_hash: str) -> str:
    return canonical_sha256(
        {"config_hash": config_hash, "domain": "hyperlab-paper-event-genesis-v1", "run_id": run_id}
    )


def _commit_genesis(run_id: str, config_hash: str) -> str:
    return canonical_sha256(
        {"config_hash": config_hash, "domain": "hyperlab-paper-commit-genesis-v1", "run_id": run_id}
    )


def _event_hash(
    *,
    sequence: int,
    payload: Mapping[str, JsonValue],
    previous_hash: str,
) -> str:
    return canonical_sha256(
        {
            **payload,
            "previous_event_hash": previous_hash,
            "sequence": sequence,
        }
    )


def _ledger_transaction_hash(
    run_id: str,
    transaction_id: str,
    entries: Sequence[_PreparedLedgerEntry],
) -> str:
    return canonical_sha256(
        {
            "domain": "hyperlab-paper-ledger-transaction-v1",
            "entry_hashes": [entry.entry_hash for entry in entries],
            "run_id": run_id,
            "transaction_id": transaction_id,
        }
    )


def _commit_hash(
    *,
    run_id: str,
    commit_sequence: int,
    input_id: str,
    first_event_sequence: int | None,
    last_event_sequence: int,
    event_hashes: Sequence[str],
    ledger_hashes: Sequence[str],
    projection_revision: int,
    projection_hash: str,
    alert_hashes: Sequence[str],
    previous_commit_hash: str,
) -> str:
    return canonical_sha256(
        {
            "alert_hashes": list(alert_hashes),
            "commit_sequence": commit_sequence,
            "domain": "hyperlab-paper-commit-v1",
            "event_hashes": list(event_hashes),
            "first_event_sequence": first_event_sequence,
            "input_id": input_id,
            "last_event_sequence": last_event_sequence,
            "ledger_hashes": list(ledger_hashes),
            "previous_commit_hash": previous_commit_hash,
            "projection_hash": projection_hash,
            "projection_revision": projection_revision,
            "run_id": run_id,
        }
    )


def _row_value(row: sqlite3.Row, key: str) -> object:
    return row[key]


class PaperStore:
    """Crash-safe SQLite authority for one or more immutable paper runs."""

    def __init__(
        self,
        path: Path | str,
        *,
        fault_injector: FaultInjector | None = None,
        timeout_seconds: float = 30.0,
        initialize: bool = True,
        historical_replay_only: bool = False,
    ) -> None:
        self.path = Path(path)
        if not isinstance(historical_replay_only, bool):
            raise TypeError("historical_replay_only must be boolean")
        if historical_replay_only and (not initialize or self.path.exists()):
            raise ValueError(
                "historical_replay_only requires a fresh disposable store"
            )
        self._historical_replay_only = historical_replay_only
        self._fault_injector = fault_injector
        self._timeout_seconds = timeout_seconds
        if initialize:
            self.initialize()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback

    def close(self) -> None:
        """Compatibility no-op: operations use short-lived SQLite connections."""

    def _inject(self, stage: str) -> None:
        if stage not in FAULT_STAGES:
            raise ValueError(f"unknown paper-store fault stage {stage!r}")
        if self._fault_injector is not None:
            self._fault_injector(stage)

    @staticmethod
    def _observe_integrity_buffer(name: str, size: int) -> None:
        """Test instrumentation hook for bounded verifier working sets."""

        del name, size

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self._timeout_seconds,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute(f"PRAGMA busy_timeout={int(self._timeout_seconds * 1_000)}")
        return connection

    def _read_connection(self) -> sqlite3.Connection:
        uri = f"{self.path.resolve().as_uri()}?mode=ro"
        connection = sqlite3.connect(
            uri,
            uri=True,
            timeout=self._timeout_seconds,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA query_only=ON")
        return connection

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            if version > STORE_SCHEMA_VERSION:
                raise SchemaVersionError(
                    "paper store schema "
                    f"{version} is newer than supported schema {STORE_SCHEMA_VERSION}"
                )
            if version not in {0, 1, 2, STORE_SCHEMA_VERSION}:
                raise SchemaVersionError(
                    "paper store schema "
                    f"{version} has no forward migration to {STORE_SCHEMA_VERSION}"
                )
            if version == 0:
                if tables:
                    raise SchemaVersionError(
                        "paper store has tables but no recognized schema version; refusing to guess"
                    )
                created_at = _now_text().replace("'", "''")
                connection.executescript(
                    "BEGIN IMMEDIATE;\n"
                    + _SCHEMA_SQL
                    + f"\nINSERT INTO paper_schema VALUES (1, {STORE_SCHEMA_VERSION}, "
                    + f"'{created_at}');\n"
                    + f"PRAGMA user_version={STORE_SCHEMA_VERSION};\nCOMMIT;"
                )
            elif version == 1:
                self._migrate_v1_to_v2(connection)
                self._migrate_v2_to_v3(connection)
            elif version == 2:
                self._migrate_v2_to_v3(connection)
            self._verify_schema(connection)

    @staticmethod
    def _migrate_v1_to_v2(connection: sqlite3.Connection) -> None:
        metadata = connection.execute(
            "SELECT version FROM paper_schema WHERE singleton=1"
        ).fetchone()
        legacy_columns = tuple(
            str(row[2])
            for row in connection.execute("PRAGMA index_info('paper_events_input_idx')")
        )
        if metadata is None or int(metadata[0]) != 1:
            raise SchemaVersionError("paper store v1 metadata is inconsistent")
        if legacy_columns != ("run_id", "input_id"):
            raise SchemaVersionError(
                "paper store v1 event-input index differs from the recognized schema"
            )
        connection.executescript(
            "BEGIN IMMEDIATE;\n"
            "DROP INDEX paper_events_input_idx;\n"
            "CREATE INDEX paper_events_input_idx "
            "ON paper_events(run_id, input_id, sequence);\n"
            "UPDATE paper_schema SET version=2 WHERE singleton=1;\n"
            "PRAGMA user_version=2;\n"
            "COMMIT;"
        )

    @staticmethod
    def _migrate_v2_to_v3(connection: sqlite3.Connection) -> None:
        metadata = connection.execute(
            "SELECT version FROM paper_schema WHERE singleton=1"
        ).fetchone()
        if metadata is None or int(metadata[0]) != 2:
            raise SchemaVersionError("paper store v2 metadata is inconsistent")

        columns = {
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info('paper_projection_history')"
            )
        }
        required_legacy = {
            "run_id",
            "revision",
            "input_id",
            "event_sequence",
            "event_head_hash",
            "status",
            "payload_json",
            "projection_hash",
            "created_at",
        }
        if not required_legacy.issubset(columns):
            raise SchemaVersionError(
                "paper store v2 projection history differs from recognized schema"
            )

        connection.execute("BEGIN IMMEDIATE")
        try:
            if "payload_zlib" not in columns:
                connection.execute(
                    "ALTER TABLE paper_projection_history "
                    "ADD COLUMN payload_zlib BLOB"
                )
            if "payload_codec" not in columns:
                connection.execute(
                    "ALTER TABLE paper_projection_history "
                    "ADD COLUMN payload_codec TEXT NOT NULL DEFAULT 'json'"
                )
            if "last_received_at" not in columns:
                connection.execute(
                    "ALTER TABLE paper_projection_history "
                    "ADD COLUMN last_received_at TEXT"
                )
            if "utc_date" not in columns:
                connection.execute(
                    "ALTER TABLE paper_projection_history "
                    "ADD COLUMN utc_date TEXT"
                )

            connection.execute(
                "UPDATE paper_schema SET version=3 WHERE singleton=1"
            )
            connection.execute("PRAGMA user_version=3")
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise

    def _verify_schema(self, connection: sqlite3.Connection) -> None:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != STORE_SCHEMA_VERSION:
            raise SchemaVersionError(
                "paper store schema "
                f"{version} is not supported by schema {STORE_SCHEMA_VERSION} code"
            )
        try:
            row = connection.execute("SELECT version FROM paper_schema WHERE singleton=1").fetchone()
        except sqlite3.DatabaseError as error:
            raise SchemaVersionError("paper store schema metadata is missing") from error
        if row is None or int(row[0]) != STORE_SCHEMA_VERSION:
            raise SchemaVersionError("paper store schema metadata disagrees with PRAGMA user_version")
        event_input_columns = tuple(
            str(index_row[2])
            for index_row in connection.execute(
                "PRAGMA index_info('paper_events_input_idx')"
            )
        )
        if event_input_columns != ("run_id", "input_id", "sequence"):
            raise SchemaVersionError(
                "paper event-input index does not bind input order to event sequence"
            )
        history_columns = {
            str(column_row[1])
            for column_row in connection.execute(
                "PRAGMA table_info('paper_projection_history')"
            )
        }
        required_history_columns = {
            "payload_json",
            "payload_zlib",
            "payload_codec",
            "last_received_at",
            "utc_date",
        }
        if not required_history_columns.issubset(history_columns):
            raise SchemaVersionError(
                "paper projection history compression columns are missing"
            )

        foreign_keys = int(connection.execute("PRAGMA foreign_keys").fetchone()[0])
        synchronous = int(connection.execute("PRAGMA synchronous").fetchone()[0])
        if foreign_keys != 1 or synchronous < 2:
            raise SchemaVersionError("paper store requires foreign_keys=ON and synchronous=FULL")

    def create_run(
        self,
        run_id: str | PaperRunConfig,
        config_snapshot: object | None = None,
        *,
        config_hash: str | None = None,
        seed: int | None = None,
        initial_projection: object | None = None,
        status: str = "FLAT",
        created_at: str | None = None,
    ) -> RunRecord:
        if isinstance(run_id, PaperRunConfig):
            run_config = run_id
            if config_snapshot is not None:
                if initial_projection is not None or not isinstance(
                    config_snapshot,
                    PaperProjection,
                ):
                    raise TypeError(
                        "when create_run receives PaperRunConfig, the optional second positional "
                        "argument must be its initial PaperProjection"
                    )
                initial_projection = config_snapshot
            config_snapshot = run_config
            normalized_run_id = run_config.run_id
            if config_hash is not None and config_hash != run_config.config_hash:
                raise RunConflictError("config_hash differs from PaperRunConfig.config_hash")
            config_hash = run_config.config_hash
            if seed is not None and seed != run_config.seed:
                raise RunConflictError("seed differs from immutable PaperRunConfig.seed")
            seed = run_config.seed
            if initial_projection is None:
                initial_projection = PaperProjection(
                    run_id=run_config.run_id,
                    config_hash=run_config.config_hash,
                    initial_cash=run_config.initial_cash,
                )
        else:
            normalized_run_id = _identifier(run_id, label="run_id")
            if config_snapshot is None:
                raise TypeError("config_snapshot is required when run_id is passed as a string")
        if seed is not None and (isinstance(seed, bool) or not isinstance(seed, int)):
            raise TypeError("seed must be an integer or None")
        if status not in PAPER_STATES:
            raise ValueError(f"paper status must be one of {sorted(PAPER_STATES)}")
        _config, config_json, calculated_hash = _canonical_record(
            config_snapshot,
            label="config_snapshot",
        )
        if config_hash is not None and config_hash != calculated_hash:
            raise RunConflictError("config_hash does not match the canonical configuration snapshot")
        durable_config_hash = calculated_hash
        event_head = _event_genesis(normalized_run_id, durable_config_hash)
        commit_head = _commit_genesis(normalized_run_id, durable_config_hash)
        if initial_projection is None:
            projection: dict[str, JsonValue] = {
                "config_hash": durable_config_hash,
                "event_head_hash": event_head,
                "event_sequence": 0,
                "run_id": normalized_run_id,
                "state": status,
            }
        else:
            projection = _record_dict(initial_projection, label="initial_projection")
        projection_status = _state_from_projection(projection, default=status)
        self._validate_projection_binding(
            projection,
            run_id=normalized_run_id,
            config_hash=durable_config_hash,
            event_sequence=0,
            event_head_hash=event_head,
        )
        projection_json = canonical_json(projection)
        projection_hash = canonical_sha256(projection)
        now = created_at or _now_text()
        seed_text = str(seed) if seed is not None else None
        with self._connect() as connection:
            self._verify_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT * FROM paper_runs WHERE run_id=?",
                    (normalized_run_id,),
                ).fetchone()
                if existing is not None:
                    same = (
                        str(existing["config_json"]) == config_json
                        and str(existing["config_hash"]) == durable_config_hash
                        and cast(str | None, existing["seed_text"]) == seed_text
                    )
                    if same:
                        connection.rollback()
                        return self._run_from_row(existing)
                    self._persist_guard_alert(
                        connection,
                        normalized_run_id,
                        code="RUN_CONFIG_CONFLICT",
                        detail="run_id was reused with a divergent immutable configuration",
                        issues=(),
                    )
                    connection.commit()
                    raise RunConflictError("run_id already exists with a different immutable configuration")
                connection.execute(
                    """
                    INSERT INTO paper_runs (
                        run_id, schema_version, config_json, config_hash, seed_text, status,
                        created_at, event_count, event_head_hash, commit_count,
                        commit_head_hash, projection_revision, projection_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, 0, ?, 0, ?)
                    """,
                    (
                        normalized_run_id,
                        SCHEMA_VERSION,
                        config_json,
                        durable_config_hash,
                        seed_text,
                        projection_status,
                        now,
                        event_head,
                        commit_head,
                        projection_hash,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO paper_projections (
                        run_id, revision, event_sequence, event_head_hash, status,
                        effective_status, payload_json, projection_hash, updated_at
                    ) VALUES (?, 0, 0, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_run_id,
                        event_head,
                        projection_status,
                        projection_status,
                        projection_json,
                        projection_hash,
                        now,
                    ),
                )
                (
                    history_json,
                    history_zlib,
                    history_codec,
                    history_last_received_at,
                    history_utc_date,
                ) = _projection_history_storage(projection, projection_json)
                connection.execute(
                    """
                    INSERT INTO paper_projection_history (
                        run_id, revision, input_id, event_sequence, event_head_hash,
                        status, payload_json, payload_zlib, payload_codec,
                        last_received_at, utc_date, projection_hash, created_at
                    ) VALUES (?, 0, NULL, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_run_id,
                        event_head,
                        projection_status,
                        history_json,
                        history_zlib,
                        history_codec,
                        history_last_received_at,
                        history_utc_date,
                        projection_hash,
                        now,
                    ),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        return self.get_run(normalized_run_id)

    def append_atomic(
        self,
        run_id: str,
        input_id: str,
        input_payload: object,
        events: Sequence[object],
        ledger_entries: Sequence[object],
        projection: object,
        *,
        alerts: Sequence[object] = (),
        expected_sequence: int | None = None,
    ) -> AppendResult:
        normalized_run_id = _identifier(run_id, label="run_id")
        normalized_input_id = _identifier(input_id, label="input_id")
        _input, input_json, input_hash = _canonical_record(input_payload, label="input_payload")
        if expected_sequence is not None and (isinstance(expected_sequence, bool) or expected_sequence < 0):
            raise ValueError("expected_sequence must be a non-negative integer or None")
        self._inject("before_begin")
        with self._connect() as connection:
            self._verify_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            committed = False
            try:
                run = connection.execute(
                    "SELECT * FROM paper_runs WHERE run_id=?",
                    (normalized_run_id,),
                ).fetchone()
                if run is None:
                    raise RunNotFoundError(f"unknown paper run {normalized_run_id!r}")
                duplicate = connection.execute(
                    "SELECT * FROM paper_inbox WHERE run_id=? AND input_id=?",
                    (normalized_run_id, normalized_input_id),
                ).fetchone()
                if duplicate is not None:
                    if (
                        str(duplicate["payload_hash"]) == input_hash
                        and str(duplicate["payload_json"]) == input_json
                    ):
                        connection.rollback()
                        first_raw = duplicate["first_event_sequence"]
                        return AppendResult(
                            run_id=normalized_run_id,
                            input_id=normalized_input_id,
                            first_sequence=int(first_raw) if first_raw is not None else None,
                            last_sequence=int(duplicate["last_event_sequence"]),
                            appended_event_count=0,
                            event_head_hash=str(run["event_head_hash"]),
                            commit_sequence=int(duplicate["commit_sequence"]),
                            commit_hash=str(duplicate["commit_hash"]),
                            idempotent=True,
                        )
                    self._persist_guard_alert(
                        connection,
                        normalized_run_id,
                        code="INBOX_IDEMPOTENCY_CONFLICT",
                        detail=f"input_id {normalized_input_id!r} was reused with divergent payload",
                        issues=(),
                    )
                    connection.commit()
                    committed = True
                    raise IdempotencyConflictError(
                        f"input_id {normalized_input_id!r} already has a different payload"
                    )
                if str(run["status"]) == "MANUAL_REVIEW":
                    raise IntegrityError(
                        IntegrityReport(
                            run_id=normalized_run_id,
                            ok=False,
                            event_count=int(run["event_count"]),
                            event_head_hash=str(run["event_head_hash"]),
                            commit_count=int(run["commit_count"]),
                            commit_head_hash=str(run["commit_head_hash"]),
                            issues=(
                                IntegrityIssue(
                                    "MANUAL_REVIEW_LATCHED",
                                    "run is fail-closed pending manual review",
                                ),
                            ),
                        )
                    )
                current_sequence = int(run["event_count"])
                if expected_sequence is not None and expected_sequence != current_sequence:
                    raise ConcurrentWriteError(
                        f"expected event sequence {expected_sequence}, durable sequence is {current_sequence}"
                    )
                guard_issues = self._collect_append_head_issues(
                    connection,
                    normalized_run_id,
                    run,
                )
                if guard_issues:
                    self._persist_guard_alert(
                        connection,
                        normalized_run_id,
                        code="PAPER_STORE_INTEGRITY_FAILURE",
                        detail="pre-append integrity verification failed",
                        issues=guard_issues,
                    )
                    connection.commit()
                    committed = True
                    report = self._report(connection, normalized_run_id, guard_issues)
                    raise IntegrityError(report)
                paper_events = self._paper_events(events)
                if not paper_events:
                    raise AppendConflictError(
                        "a new input must append at least one event; only a duplicate input may no-op"
                    )
                prepared_events = self._prepare_events(
                    connection,
                    normalized_run_id,
                    paper_events,
                    start_sequence=current_sequence,
                    previous_hash=str(run["event_head_hash"]),
                )
                resulting_sequence = current_sequence + len(prepared_events)
                resulting_event_head = (
                    prepared_events[-1].event_hash if prepared_events else str(run["event_head_hash"])
                )
                prepared_ledger = self._prepare_ledger(
                    connection,
                    normalized_run_id,
                    ledger_entries,
                    known_event_sequences={event.event_id: event.sequence for event in prepared_events},
                )
                if prepared_ledger and not prepared_events:
                    raise LedgerImbalanceError(
                        "ledger mutations require at least one newly appended lifecycle event"
                    )
                alert_event_sequences: dict[str, int] = {}
                for event, prepared in zip(
                    paper_events,
                    prepared_events,
                    strict=True,
                ):
                    if event.event_type is not PaperEventType.ALERT_RAISED:
                        continue
                    alert_payload = _record_dict(event.payload, label="alert event payload")
                    alert_id = self._alert_id(normalized_run_id, alert_payload)
                    if alert_id in alert_event_sequences:
                        raise IdempotencyConflictError(
                            f"alert_id {alert_id!r} is duplicated within a new input"
                        )
                    alert_event_sequences[alert_id] = prepared.sequence
                prepared_alerts = self._prepare_alerts(
                    connection,
                    normalized_run_id,
                    alerts,
                    known_alert_sequences=alert_event_sequences,
                )
                projection_payload, projection_json, projection_hash = _canonical_record(
                    projection,
                    label="projection",
                )
                self._validate_projection_binding(
                    projection_payload,
                    run_id=normalized_run_id,
                    config_hash=str(run["config_hash"]),
                    event_sequence=resulting_sequence,
                    event_head_hash=resulting_event_head,
                )
                self._validate_replayed_append(
                    connection,
                    normalized_run_id,
                    _input,
                    paper_events,
                    prepared_events,
                    ledger_entries,
                    alerts,
                    projection_json,
                )
                projection_status = _state_from_projection(
                    projection_payload,
                    default=str(run["status"]),
                )
                commit_sequence = int(run["commit_count"]) + 1
                projection_revision = int(run["projection_revision"]) + 1
                first_event_sequence = prepared_events[0].sequence if prepared_events else None
                event_hashes = [event.event_hash for event in prepared_events]
                ledger_hashes = [item.transaction_hash for item in prepared_ledger]
                alert_hashes = [item.payload_hash for item in prepared_alerts]
                commit_hash = _commit_hash(
                    run_id=normalized_run_id,
                    commit_sequence=commit_sequence,
                    input_id=normalized_input_id,
                    first_event_sequence=first_event_sequence,
                    last_event_sequence=resulting_sequence,
                    event_hashes=event_hashes,
                    ledger_hashes=ledger_hashes,
                    projection_revision=projection_revision,
                    projection_hash=projection_hash,
                    alert_hashes=alert_hashes,
                    previous_commit_hash=str(run["commit_head_hash"]),
                )
                now = _now_text()
                connection.execute(
                    """
                    INSERT INTO paper_inbox (
                        run_id, input_id, payload_json, payload_hash, first_event_sequence,
                        last_event_sequence, commit_sequence, commit_hash, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_run_id,
                        normalized_input_id,
                        input_json,
                        input_hash,
                        first_event_sequence,
                        resulting_sequence,
                        commit_sequence,
                        commit_hash,
                        now,
                    ),
                )
                self._inject("after_input")
                connection.executemany(
                    """
                    INSERT INTO paper_events (
                        run_id, sequence, event_id, event_type, payload_json, payload_hash,
                        previous_hash, event_hash, input_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            normalized_run_id,
                            item.sequence,
                            item.event_id,
                            item.event_type,
                            item.payload_json,
                            item.payload_hash,
                            item.previous_hash,
                            item.event_hash,
                            normalized_input_id,
                            now,
                        )
                        for item in prepared_events
                    ],
                )
                self._inject("after_events")
                for transaction in prepared_ledger:
                    connection.execute(
                        """
                        INSERT INTO paper_ledger_transactions (
                            run_id, transaction_id, input_id, event_sequence,
                            entry_count, transaction_hash, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            normalized_run_id,
                            transaction.transaction_id,
                            normalized_input_id,
                            transaction.event_sequence,
                            len(transaction.entries),
                            transaction.transaction_hash,
                            now,
                        ),
                    )
                    connection.executemany(
                        """
                        INSERT INTO paper_ledger_entries (
                            run_id, transaction_id, entry_index, entry_id, event_id,
                            account, unit, amount_text, payload_json, entry_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                normalized_run_id,
                                entry.transaction_id,
                                entry.entry_index,
                                entry.entry_id,
                                entry.event_id,
                                entry.account,
                                entry.unit,
                                entry.amount_text,
                                entry.payload_json,
                                entry.entry_hash,
                            )
                            for entry in transaction.entries
                        ],
                    )
                self._inject("after_ledger")
                connection.execute(
                    """
                    UPDATE paper_projections
                    SET revision=?, event_sequence=?, event_head_hash=?, status=?, effective_status=?,
                        payload_json=?, projection_hash=?, updated_at=?
                    WHERE run_id=?
                    """,
                    (
                        projection_revision,
                        resulting_sequence,
                        resulting_event_head,
                        projection_status,
                        projection_status,
                        projection_json,
                        projection_hash,
                        now,
                        normalized_run_id,
                    ),
                )
                (
                    history_json,
                    history_zlib,
                    history_codec,
                    history_last_received_at,
                    history_utc_date,
                ) = _projection_history_storage(projection_payload, projection_json)
                connection.execute(
                    """
                    INSERT INTO paper_projection_history (
                        run_id, revision, input_id, event_sequence, event_head_hash,
                        status, payload_json, payload_zlib, payload_codec,
                        last_received_at, utc_date, projection_hash, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_run_id,
                        projection_revision,
                        normalized_input_id,
                        resulting_sequence,
                        resulting_event_head,
                        projection_status,
                        history_json,
                        history_zlib,
                        history_codec,
                        history_last_received_at,
                        history_utc_date,
                        projection_hash,
                        now,
                    ),
                )
                connection.executemany(
                    """
                    INSERT INTO paper_alerts (
                        run_id, alert_id, commit_sequence, event_sequence, severity,
                        code, payload_json, payload_hash, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            normalized_run_id,
                            item.alert_id,
                            commit_sequence,
                            item.event_sequence,
                            item.severity,
                            item.code,
                            item.payload_json,
                            item.payload_hash,
                            now,
                        )
                        for item in prepared_alerts
                    ],
                )
                connection.execute(
                    """
                    INSERT INTO paper_commits (
                        run_id, commit_sequence, input_id, first_event_sequence,
                        last_event_sequence, event_hashes_json, ledger_hashes_json,
                        projection_revision, projection_hash, alert_hashes_json,
                        previous_commit_hash, commit_hash, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_run_id,
                        commit_sequence,
                        normalized_input_id,
                        first_event_sequence,
                        resulting_sequence,
                        canonical_json(event_hashes),
                        canonical_json(ledger_hashes),
                        projection_revision,
                        projection_hash,
                        canonical_json(alert_hashes),
                        str(run["commit_head_hash"]),
                        commit_hash,
                        now,
                    ),
                )
                connection.execute(
                    """
                    UPDATE paper_runs
                    SET status=?, event_count=?, event_head_hash=?, commit_count=?,
                        commit_head_hash=?, projection_revision=?, projection_hash=?
                    WHERE run_id=?
                    """,
                    (
                        projection_status,
                        resulting_sequence,
                        resulting_event_head,
                        commit_sequence,
                        commit_hash,
                        projection_revision,
                        projection_hash,
                        normalized_run_id,
                    ),
                )
                self._inject("before_commit")
                connection.commit()
                committed = True
            except IdempotencyConflictError as error:
                if connection.in_transaction and not committed:
                    self._persist_guard_alert(
                        connection,
                        normalized_run_id,
                        code="DURABLE_ID_REUSE_CONFLICT",
                        detail=str(error),
                        issues=(),
                    )
                    connection.commit()
                    committed = True
                raise
            except BaseException:
                if connection.in_transaction and not committed:
                    connection.rollback()
                raise
        self._inject("after_commit")
        return AppendResult(
            run_id=normalized_run_id,
            input_id=normalized_input_id,
            first_sequence=first_event_sequence,
            last_sequence=resulting_sequence,
            appended_event_count=len(prepared_events),
            event_head_hash=resulting_event_head,
            commit_sequence=commit_sequence,
            commit_hash=commit_hash,
        )

    def append_transaction(
        self,
        run_id: str,
        input_id: str,
        input_payload: object,
        events: Sequence[object],
        ledger_entries: Sequence[object],
        projection: object,
        *,
        alerts: Sequence[object] = (),
        expected_sequence: int | None = None,
    ) -> AppendResult:
        """Named alias for callers that describe the unit as a store transaction."""

        return self.append_atomic(
            run_id,
            input_id,
            input_payload,
            events,
            ledger_entries,
            projection,
            alerts=alerts,
            expected_sequence=expected_sequence,
        )

    @staticmethod
    def _paper_events(events: Sequence[object]) -> tuple[PaperEvent, ...]:
        result: list[PaperEvent] = []
        for raw_event in events:
            if isinstance(raw_event, PaperEvent):
                result.append(raw_event)
                continue
            payload = _record_dict(raw_event, label="paper_event")
            try:
                result.append(PaperEvent.from_dict(cast(Mapping[str, object], payload)))
            except (KeyError, TypeError, ValueError) as error:
                raise AppendConflictError(
                    f"paper event cannot be reconstructed for independent replay: {error}"
                ) from error
        return tuple(result)

    @staticmethod
    def _alert_id(run_id: str, payload: Mapping[str, JsonValue]) -> str:
        raw_alert_id = payload.get("alert_id", payload.get("id"))
        if raw_alert_id is not None:
            return _identifier(raw_alert_id, label="alert_id")
        return canonical_sha256(
            {
                "alert": dict(payload),
                "domain": "hyperlab-paper-alert-id-v1",
                "run_id": run_id,
            }
        )

    @staticmethod
    def _expected_ledger_entries(
        run_id: str,
        projection: PaperProjection,
        event: PaperEvent,
    ) -> tuple[LedgerEntry, ...]:
        amounts: tuple[tuple[str, Decimal], ...]
        if event.event_type is PaperEventType.RUN_STARTED:
            amounts = (
                ("asset:cash", projection.initial_cash),
                ("equity:initial_capital", -projection.initial_cash),
            )
            transaction_id = deterministic_id("paper_initial_capital", run_id)
        else:
            amounts = transaction_ledger_amounts(projection, event)
            transaction_id = deterministic_id("paper_fill_transaction", event.event_id)
        return tuple(
            LedgerEntry.create(
                run_id=run_id,
                event_id=event.event_id,
                transaction_id=transaction_id,
                account=account,
                amount=amount,
                ordinal=index,
            )
            for index, (account, amount) in enumerate(amounts)
        )

    def _validate_replayed_append(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        input_payload: Mapping[str, JsonValue],
        events: Sequence[PaperEvent],
        prepared_events: Sequence[_PreparedEvent],
        ledger_entries: Sequence[object],
        alerts: Sequence[object],
        supplied_projection_json: str,
    ) -> None:
        durable = connection.execute(
            "SELECT payload_json FROM paper_projections WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if durable is None:
            raise AppendConflictError("durable projection is missing")
        try:
            durable_payload = _json_object(
                str(durable["payload_json"]),
                label="durable projection",
            )
            working = PaperProjection.from_dict(
                cast(Mapping[str, object], durable_payload),
            )
        except (DecimalException, KeyError, TypeError, ValueError) as error:
            raise AppendConflictError(
                f"durable projection cannot be reconstructed for replay: {error}"
            ) from error
        if len(events) != len(prepared_events):
            raise AssertionError("prepared paper event count differs from replay input")

        cash_event_types = {
            PaperEventType.ORDER_PARTIALLY_FILLED,
            PaperEventType.ORDER_FILLED,
            PaperEventType.FUNDING_POSTED,
        }
        cash_events = tuple(
            event for event in events if event.event_type in cash_event_types
        )
        if cash_events:
            raw_input_version = input_payload.get("cash_math_version", 1)
            if (
                isinstance(raw_input_version, bool)
                or not isinstance(raw_input_version, int)
                or raw_input_version not in {1, PAPER_CASH_MATH_VERSION}
            ):
                raise AppendConflictError("input cash_math_version is not supported")
            for event in cash_events:
                raw_event_version = event.payload.get("cash_math_version", 1)
                if (
                    isinstance(raw_event_version, bool)
                    or not isinstance(raw_event_version, int)
                    or raw_event_version not in {1, PAPER_CASH_MATH_VERSION}
                ):
                    raise AppendConflictError("event cash_math_version is not supported")
                if raw_event_version != raw_input_version:
                    raise AppendConflictError(
                        "cash event version differs from its durable input version"
                    )
            if (
                raw_input_version != PAPER_CASH_MATH_VERSION
                and not self._historical_replay_only
            ):
                raise AppendConflictError("new cash inputs and events must use cash_math_version 2")
        expected_ledger: list[LedgerEntry] = []
        for event, prepared in zip(events, prepared_events, strict=True):
            if working.last_sequence + 1 != prepared.sequence:
                raise AppendConflictError(
                    "durable projection sequence does not continue into the supplied events"
                )
            if working.last_event_hash != prepared.previous_hash:
                raise AppendConflictError(
                    "durable projection event head does not continue into the supplied events"
                )
            try:
                expected_ledger.extend(self._expected_ledger_entries(run_id, working, event))
                apply_event(working, event)
            except (DecimalException, KeyError, TypeError, ValueError) as error:
                raise AppendConflictError(
                    f"paper event replay failed for {event.event_id}: {error}"
                ) from error
            working.last_sequence = prepared.sequence
            working.last_event_hash = prepared.event_hash

        expected_ledger_json = canonical_json([entry.to_dict() for entry in expected_ledger])
        supplied_ledger_json = canonical_json(
            [
                _record_dict(entry, label=f"ledger_entries[{index}]")
                for index, entry in enumerate(ledger_entries)
            ]
        )
        if supplied_ledger_json != expected_ledger_json:
            raise AppendConflictError("supplied ledger entries differ from exact reducer-derived entries")
        expected_alerts_json = canonical_json(
            [
                _record_dict(event.payload, label="ALERT_RAISED payload")
                for event in events
                if event.event_type is PaperEventType.ALERT_RAISED
            ]
        )
        supplied_alerts_json = canonical_json(
            [_record_dict(alert, label=f"alerts[{index}]") for index, alert in enumerate(alerts)]
        )
        if supplied_alerts_json != expected_alerts_json:
            raise AppendConflictError("supplied alerts differ from exact ALERT_RAISED event payloads")
        try:
            validated = PaperProjection.from_dict(working.to_dict())
        except (DecimalException, KeyError, TypeError, ValueError) as error:
            raise AppendConflictError(
                f"replayed post-event projection violates PaperProjection invariants: {error}"
            ) from error
        replayed_projection_json = canonical_json(validated.to_dict())
        if supplied_projection_json != replayed_projection_json:
            raise AppendConflictError(
                "supplied projection differs from durable projection plus supplied events"
            )

    def _prepare_events(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        events: Sequence[PaperEvent],
        *,
        start_sequence: int,
        previous_hash: str,
    ) -> tuple[_PreparedEvent, ...]:
        result: list[_PreparedEvent] = []
        seen: dict[str, str] = {}
        durable_previous = previous_hash
        for event in events:
            payload, payload_json, payload_hash = _canonical_record(event, label="paper_event")
            raw_event_id = payload.get("event_id", payload.get("id"))
            if raw_event_id is None:
                event_id = canonical_sha256(
                    {"domain": "hyperlab-paper-event-id-v1", "event": payload, "run_id": run_id}
                )
            else:
                event_id = _identifier(raw_event_id, label="event_id")
            raw_type = payload.get("event_type", payload.get("type", payload.get("kind")))
            event_type = _identifier(raw_type, label="event_type")
            payload_run_id = payload.get("run_id")
            if payload_run_id is not None and payload_run_id != run_id:
                raise ValueError(f"event {event_id!r} belongs to a different run")
            if event_id in seen:
                raise IdempotencyConflictError(f"event_id {event_id!r} is duplicated within a new input")
            seen[event_id] = payload_hash
            existing = connection.execute(
                "SELECT payload_hash, payload_json FROM paper_events WHERE run_id=? AND event_id=?",
                (run_id, event_id),
            ).fetchone()
            if existing is not None:
                raise IdempotencyConflictError(f"event_id {event_id!r} already belongs to a durable input")
            sequence = start_sequence + len(result) + 1
            chained_hash = _event_hash(
                sequence=sequence,
                payload=payload,
                previous_hash=durable_previous,
            )
            result.append(
                _PreparedEvent(
                    sequence=sequence,
                    event_id=event_id,
                    event_type=event_type,
                    payload_json=payload_json,
                    payload_hash=payload_hash,
                    previous_hash=durable_previous,
                    event_hash=chained_hash,
                )
            )
            durable_previous = chained_hash
        return tuple(result)

    def _prepare_ledger(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        ledger_entries: Sequence[object],
        *,
        known_event_sequences: Mapping[str, int],
    ) -> tuple[_PreparedLedgerTransaction, ...]:
        grouped: dict[str, list[_PreparedLedgerEntry]] = defaultdict(list)
        balances: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
        entry_ids: set[str] = set()
        for raw_entry in ledger_entries:
            payload, payload_json, entry_hash = _canonical_record(raw_entry, label="ledger_entry")
            transaction_id = _identifier(
                payload.get("transaction_id", payload.get("ledger_transaction_id")),
                label="ledger transaction_id",
            )
            account = _identifier(
                payload.get("account", payload.get("account_code")),
                label="ledger account",
            )
            raw_unit = payload.get(
                "unit",
                payload.get("currency", payload.get("asset", payload.get("instrument_id", "USD"))),
            )
            unit = _identifier(raw_unit, label="ledger unit")
            if "amount" not in payload:
                raise ValueError("ledger entry must contain an exact amount")
            amount_text = _decimal_text(payload["amount"], label="ledger amount")
            amount = Decimal(amount_text)
            raw_entry_id = payload.get("entry_id", payload.get("id"))
            entry_id = (
                _identifier(raw_entry_id, label="ledger entry_id")
                if raw_entry_id is not None
                else canonical_sha256(
                    {
                        "domain": "hyperlab-paper-ledger-entry-id-v1",
                        "entry": payload,
                        "run_id": run_id,
                    }
                )
            )
            if entry_id in entry_ids:
                raise IdempotencyConflictError(f"duplicate ledger entry_id {entry_id!r}")
            entry_ids.add(entry_id)
            existing_entry = connection.execute(
                """
                SELECT 1 FROM paper_ledger_entries
                WHERE run_id=? AND entry_id=?
                """,
                (run_id, entry_id),
            ).fetchone()
            if existing_entry is not None:
                raise IdempotencyConflictError(
                    f"ledger entry_id {entry_id!r} already belongs to a durable input"
                )
            raw_event_id = payload.get("event_id")
            event_id = (
                _identifier(raw_event_id, label="ledger event_id") if raw_event_id is not None else None
            )
            if event_id is None or event_id not in known_event_sequences:
                raise AppendConflictError("ledger entries must reference an event appended by the same input")
            entry = _PreparedLedgerEntry(
                transaction_id=transaction_id,
                entry_index=len(grouped[transaction_id]),
                entry_id=entry_id,
                event_id=event_id,
                account=account,
                unit=unit,
                amount_text=amount_text,
                payload_json=payload_json,
                entry_hash=entry_hash,
            )
            grouped[transaction_id].append(entry)
            balances[(transaction_id, unit)] += amount
        result: list[_PreparedLedgerTransaction] = []
        for transaction_id, entries in grouped.items():
            if len(entries) < 2:
                raise LedgerImbalanceError(
                    f"ledger transaction {transaction_id!r} must contain at least two entries"
                )
            bad_units = [
                (unit, balance)
                for (candidate, unit), balance in balances.items()
                if candidate == transaction_id and balance != Decimal(0)
            ]
            if bad_units:
                detail = ", ".join(f"{unit}={balance}" for unit, balance in bad_units)
                raise LedgerImbalanceError(
                    f"ledger transaction {transaction_id!r} is not exactly balanced: {detail}"
                )
            transaction_event_ids = {entry.event_id for entry in entries}
            if len(transaction_event_ids) != 1 or None in transaction_event_ids:
                raise AppendConflictError(
                    f"ledger transaction {transaction_id!r} must belong to exactly one event"
                )
            transaction_event_id = cast(str, next(iter(transaction_event_ids)))
            transaction_hash = _ledger_transaction_hash(run_id, transaction_id, entries)
            existing = connection.execute(
                """
                SELECT transaction_hash FROM paper_ledger_transactions
                WHERE run_id=? AND transaction_id=?
                """,
                (run_id, transaction_id),
            ).fetchone()
            if existing is not None:
                raise IdempotencyConflictError(
                    f"ledger transaction_id {transaction_id!r} already belongs to a durable input"
                )
            result.append(
                _PreparedLedgerTransaction(
                    transaction_id=transaction_id,
                    event_sequence=known_event_sequences[transaction_event_id],
                    entries=tuple(entries),
                    transaction_hash=transaction_hash,
                )
            )
        return tuple(result)

    def _prepare_alerts(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        alerts: Sequence[object],
        *,
        known_alert_sequences: Mapping[str, int],
    ) -> tuple[_PreparedAlert, ...]:
        result: list[_PreparedAlert] = []
        seen: dict[str, str] = {}
        for raw_alert in alerts:
            payload, payload_json, payload_hash = _canonical_record(raw_alert, label="paper_alert")
            alert_id = self._alert_id(run_id, payload)
            severity = _identifier(payload.get("severity", "WARNING"), label="alert severity")
            code = _identifier(
                payload.get("code", payload.get("alert_type", "PAPER_ALERT")),
                label="alert code",
            )
            if alert_id in seen:
                raise IdempotencyConflictError(f"alert_id {alert_id!r} is duplicated within a new input")
            seen[alert_id] = payload_hash
            existing = connection.execute(
                "SELECT payload_hash, payload_json FROM paper_alerts WHERE run_id=? AND alert_id=?",
                (run_id, alert_id),
            ).fetchone()
            if existing is not None:
                raise IdempotencyConflictError(f"alert_id {alert_id!r} already belongs to a durable input")
            if alert_id not in known_alert_sequences:
                raise AppendConflictError("alerts must be backed by an ALERT_RAISED event in the same input")
            result.append(
                _PreparedAlert(
                    alert_id=alert_id,
                    event_sequence=known_alert_sequences[alert_id],
                    severity=severity,
                    code=code,
                    payload_json=payload_json,
                    payload_hash=payload_hash,
                )
            )
        return tuple(result)

    @staticmethod
    def _validate_projection_binding(
        projection: Mapping[str, JsonValue],
        *,
        run_id: str,
        config_hash: str,
        event_sequence: int,
        event_head_hash: str,
    ) -> None:
        projection_run_id = _optional_text(projection, ("run_id",))
        if projection_run_id is not None and projection_run_id != run_id:
            raise ValueError("projection belongs to a different run_id")
        projection_config_hash = _optional_text(projection, ("config_hash",))
        if projection_config_hash is not None and projection_config_hash != config_hash:
            raise ValueError("projection config_hash differs from the immutable run configuration")
        projection_sequence = _optional_int(
            projection,
            ("event_sequence", "last_sequence", "sequence"),
        )
        if projection_sequence is not None and projection_sequence != event_sequence:
            raise ValueError(
                f"projection event sequence {projection_sequence} != durable sequence {event_sequence}"
            )
        projection_head = _optional_text(
            projection,
            ("event_head_hash", "head_hash", "last_event_hash"),
        )
        if projection_head is not None and projection_head != event_head_hash:
            raise ValueError("projection event head differs from the durable event head")
        if "last_event_hash" in projection and projection["last_event_hash"] is None and event_sequence != 0:
            raise ValueError("a non-empty projection must bind its last_event_hash")

    def verify_integrity(
        self,
        run_id: str,
        *,
        raise_on_error: bool = True,
        should_stop: Callable[[], bool] | None = None,
    ) -> IntegrityReport:
        normalized_run_id = _identifier(run_id, label="run_id")
        with self._connect() as connection:
            self._verify_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            try:
                if (
                    connection.execute(
                        "SELECT 1 FROM paper_runs WHERE run_id=?",
                        (normalized_run_id,),
                    ).fetchone()
                    is None
                ):
                    raise RunNotFoundError(f"unknown paper run {normalized_run_id!r}")
                issues = self._collect_integrity_issues(
                    connection,
                    normalized_run_id,
                    should_stop=should_stop,
                )
                if issues:
                    self._persist_guard_alert(
                        connection,
                        normalized_run_id,
                        code="PAPER_STORE_INTEGRITY_FAILURE",
                        detail="explicit paper-store integrity verification failed",
                        issues=issues,
                    )
                connection.commit()
                report = self._report(connection, normalized_run_id, issues)
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        if not report.ok and raise_on_error:
            raise IntegrityError(report)
        return report

    def latch_unreadable_projection(self, run_id: str) -> bool:
        """Latch a model-invalid current projection without appending to its journal.

        Runtime failure persistence normally pauses through an ordinary canonical
        input. That path is unavailable when the durable projection itself cannot
        be reconstructed. Revalidate under the write lock, then leave append-only
        integrity evidence and MANUAL_REVIEW state without inventing an event or
        commit. Return whether the latch was required.
        """

        normalized_run_id = _identifier(run_id, label="run_id")
        with self._connect() as connection:
            self._verify_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            try:
                if (
                    connection.execute(
                        "SELECT 1 FROM paper_runs WHERE run_id=?",
                        (normalized_run_id,),
                    ).fetchone()
                    is None
                ):
                    raise RunNotFoundError(f"unknown paper run {normalized_run_id!r}")
                projection = connection.execute(
                    "SELECT payload_json FROM paper_projections WHERE run_id=?",
                    (normalized_run_id,),
                ).fetchone()
                if projection is None:
                    raise AppendConflictError("durable projection is missing")
                try:
                    payload = _json_object(
                        str(projection["payload_json"]),
                        label="current projection",
                    )
                    PaperProjection.from_dict(
                        cast(Mapping[str, object], payload),
                    )
                except (DecimalException, KeyError, TypeError, ValueError) as error:
                    issue = IntegrityIssue(
                        "CURRENT_PROJECTION_MODEL_INVALID",
                        f"{type(error).__name__}: {error}",
                    )
                    self._persist_guard_alert(
                        connection,
                        normalized_run_id,
                        code="PAPER_STORE_INTEGRITY_FAILURE",
                        detail="durable projection cannot be reconstructed by PaperProjection",
                        issues=(issue,),
                    )
                    connection.commit()
                    return True
                connection.rollback()
                return False
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

    def inspect_integrity_readonly(self, run_id: str) -> IntegrityReport:
        """Verify a run through query-only SQLite without latching or writing."""

        normalized_run_id = _identifier(run_id, label="run_id")
        with self._read_connection() as connection:
            self._verify_schema(connection)
            run = connection.execute(
                "SELECT 1 FROM paper_runs WHERE run_id=?",
                (normalized_run_id,),
            ).fetchone()
            if run is None:
                raise RunNotFoundError(f"unknown paper run {normalized_run_id!r}")
            issues = self._collect_integrity_issues(connection, normalized_run_id)
            return self._report(connection, normalized_run_id, issues)

    def inspect_head_integrity_readonly(self, run_id: str) -> IntegrityReport:
        """Validate current and append-head anchors through query-only SQLite.

        This interactive contract is independent of retained journal length. It
        validates the current run/config, event and projection heads, immutable
        projection-history head, and either genesis or the latest inbox/commit
        with its directly bound events, ledger rows, alerts, and predecessor
        anchors. Work is bounded by one immutable commit rather than full run
        history.

        It does not authenticate earlier journal rows, verify the complete hash
        chains, or perform deterministic replay. Stop the runtime writer before
        using full replay/reconciliation and inspect_integrity_readonly for
        those stopped-runtime operations.
        """

        normalized_run_id = _identifier(run_id, label="run_id")
        with self._read_connection() as connection:
            self._verify_schema(connection)
            run = connection.execute(
                "SELECT * FROM paper_runs WHERE run_id=?",
                (normalized_run_id,),
            ).fetchone()
            if run is None:
                raise RunNotFoundError(f"unknown paper run {normalized_run_id!r}")
            issues = self._collect_append_head_issues(
                connection,
                normalized_run_id,
                run,
            )
            return self._report(connection, normalized_run_id, issues)

    def check_integrity(self, run_id: str) -> IntegrityReport:
        """Return a report without raising, while retaining the fail-closed latch."""

        return self.verify_integrity(run_id, raise_on_error=False)

    def _collect_append_head_issues(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        run: sqlite3.Row,
    ) -> tuple[IntegrityIssue, ...]:
        """Validate the append boundary without scanning historical rows.

        This guard is intentionally bounded by the latest commit.  Full history
        verification remains the responsibility of ``verify_integrity`` and the
        read-only audit helper, which are run at startup/reconcile boundaries.
        """

        issues: list[IntegrityIssue] = []

        def issue(code: str, detail: str) -> None:
            issues.append(IntegrityIssue(code, detail))

        table_names = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name LIKE 'paper_%'
                """
            )
        }
        missing_tables = _REQUIRED_SCHEMA_TABLES - table_names
        if missing_tables:
            issue(
                "PAPER_SCHEMA_TABLE_MISSING",
                f"required paper tables are missing: {sorted(missing_tables)}",
            )
            # Do not continue into table-specific probes: a missing authoritative
            # table is already a fail-closed result and later SELECTs would leak a
            # raw sqlite3.OperationalError instead of the integrity issue.
            return tuple(issues)
        index_names = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='index' AND name IN (
                    'paper_alerts_commit_idx', 'paper_alerts_run_severity_sequence_idx',
                    'paper_events_input_idx',
                    'paper_ledger_transactions_input_idx'
                )
                """
            )
        }
        missing_indexes = _REQUIRED_HOT_PATH_INDEXES - index_names
        if missing_indexes:
            issue(
                "PAPER_HOT_PATH_INDEX_MISSING",
                f"required paper indexes are missing: {sorted(missing_indexes)}",
            )
        trigger_names = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='trigger' AND name IN (
                    'paper_alerts_no_delete', 'paper_alerts_no_update',
                    'paper_commits_no_delete', 'paper_commits_no_update',
                    'paper_events_no_delete', 'paper_events_no_update',
                    'paper_inbox_no_delete', 'paper_inbox_no_update',
                    'paper_ledger_entries_no_delete', 'paper_ledger_entries_no_update',
                    'paper_ledger_transactions_no_delete',
                    'paper_ledger_transactions_no_update',
                    'paper_projection_history_no_delete',
                    'paper_projection_history_no_update',
                    'paper_runs_config_immutable', 'paper_runs_no_delete'
                )
                """
            )
        }
        missing_triggers = _REQUIRED_APPEND_ONLY_TRIGGERS - trigger_names
        if missing_triggers:
            issue(
                "APPEND_ONLY_TRIGGER_MISSING",
                f"required append-only triggers are missing: {sorted(missing_triggers)}",
            )

        try:
            config = _json_object(str(run["config_json"]), label="run config_json")
            if canonical_json(config) != str(run["config_json"]):
                issue("CONFIG_NOT_CANONICAL", "stored config JSON is not canonical")
            calculated_config_hash = canonical_sha256(config)
            if calculated_config_hash != str(run["config_hash"]):
                issue("CONFIG_HASH_MISMATCH", "stored config hash does not match config JSON")
        except (TypeError, ValueError) as error:
            calculated_config_hash = str(run["config_hash"])
            issue("CONFIG_INVALID", str(error))
        if int(run["schema_version"]) != SCHEMA_VERSION:
            issue("RUN_SCHEMA_VERSION", "run schema version is unknown")
        if str(run["status"]) not in PAPER_STATES:
            issue("RUN_STATE_UNKNOWN", f"unknown run state {run['status']!r}")

        event_count = int(run["event_count"])
        event_head = str(run["event_head_hash"])
        if event_count == 0:
            if event_head != _event_genesis(run_id, calculated_config_hash):
                issue("EVENT_HEAD_MISMATCH", "empty run event head differs from genesis")
        else:
            event = connection.execute(
                "SELECT * FROM paper_events WHERE run_id=? AND sequence=?",
                (run_id, event_count),
            ).fetchone()
            if event is None:
                issue("EVENT_HEAD_MISSING", f"event head sequence {event_count} is missing")
            else:
                try:
                    event_payload = _json_object(
                        str(event["payload_json"]),
                        label="head event",
                    )
                    event_payload_hash = canonical_sha256(event_payload)
                    if canonical_json(event_payload) != str(event["payload_json"]):
                        issue("EVENT_HEAD_NOT_CANONICAL", "head event JSON is not canonical")
                    if event_payload_hash != str(event["payload_hash"]):
                        issue("EVENT_HEAD_PAYLOAD_HASH", "head event payload hash differs")
                    calculated_event_hash = _event_hash(
                        sequence=event_count,
                        payload=event_payload,
                        previous_hash=str(event["previous_hash"]),
                    )
                    if calculated_event_hash != str(event["event_hash"]):
                        issue("EVENT_HEAD_HASH_INVALID", "head event hash is invalid")
                except (TypeError, ValueError) as error:
                    issue("EVENT_HEAD_INVALID", str(error))
                if str(event["event_hash"]) != event_head:
                    issue("EVENT_HEAD_MISMATCH", "run event head differs from the head row")

        projection = connection.execute(
            "SELECT * FROM paper_projections WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if projection is None:
            issue("PROJECTION_MISSING", "current projection is missing")
        else:
            try:
                projection_payload = _json_object(
                    str(projection["payload_json"]),
                    label="current projection",
                )
                projection_hash = canonical_sha256(projection_payload)
                if canonical_json(projection_payload) != str(projection["payload_json"]):
                    issue("PROJECTION_NOT_CANONICAL", "current projection is not canonical")
                if projection_hash != str(projection["projection_hash"]):
                    issue("PROJECTION_HASH", "current projection hash differs")
                self._validate_projection_binding(
                    projection_payload,
                    run_id=run_id,
                    config_hash=calculated_config_hash,
                    event_sequence=event_count,
                    event_head_hash=event_head,
                )
                projected_state = _state_from_projection(
                    projection_payload,
                    default=str(projection["status"]),
                )
                if projected_state != str(projection["status"]):
                    issue("PROJECTION_STATE", "projection state differs from its payload")
            except (TypeError, ValueError) as error:
                issue("PROJECTION_INVALID", str(error))
            if int(projection["revision"]) != int(run["projection_revision"]):
                issue("PROJECTION_REVISION", "run and current projection revisions differ")
            if int(projection["event_sequence"]) != event_count:
                issue("PROJECTION_EVENT_SEQUENCE", "projection and run event sequences differ")
            if str(projection["event_head_hash"]) != event_head:
                issue("PROJECTION_EVENT_HEAD", "projection and run event heads differ")
            if str(projection["projection_hash"]) != str(run["projection_hash"]):
                issue("PROJECTION_RUN_HASH", "projection and run hashes differ")
            if str(projection["status"]) != str(run["status"]) or str(projection["effective_status"]) != str(
                run["status"]
            ):
                issue("RUN_PROJECTION_STATE", "run and projection states differ")

        revision = int(run["projection_revision"])
        history = connection.execute(
            """
            SELECT * FROM paper_projection_history
            WHERE run_id=? AND revision=?
            """,
            (run_id, revision),
        ).fetchone()
        if history is None:
            issue("PROJECTION_HISTORY_HEAD_MISSING", f"projection revision {revision} is missing")
        elif projection is not None:
            try:
                history_payload_json = _projection_history_json(
                    history,
                    label=f"projection revision {revision}",
                )
            except ValueError as error:
                issue("PROJECTION_HISTORY_HEAD", str(error))
                history_payload_json = ""
            if (
                str(history["projection_hash"]) != str(projection["projection_hash"])
                or history_payload_json != str(projection["payload_json"])
                or int(history["event_sequence"]) != event_count
                or str(history["event_head_hash"]) != event_head
                or str(history["status"]) != str(projection["status"])
            ):
                issue(
                    "PROJECTION_HISTORY_HEAD",
                    "projection head differs from immutable history",
                )

        commit_count = int(run["commit_count"])
        commit_head = str(run["commit_head_hash"])
        if commit_count == 0:
            if commit_head != _commit_genesis(run_id, calculated_config_hash):
                issue("COMMIT_HEAD_MISMATCH", "empty run commit head differs from genesis")
            if revision != 0:
                issue("COMMIT_PROJECTION_REVISION", "empty run projection revision is not zero")
            if history is not None and history["input_id"] is not None:
                issue("PROJECTION_HISTORY_INPUT", "initial projection unexpectedly has an input")
            return tuple(issues)

        commit = connection.execute(
            "SELECT * FROM paper_commits WHERE run_id=? AND commit_sequence=?",
            (run_id, commit_count),
        ).fetchone()
        inbox = connection.execute(
            "SELECT * FROM paper_inbox WHERE run_id=? AND commit_sequence=?",
            (run_id, commit_count),
        ).fetchone()
        if commit is None:
            issue("COMMIT_HEAD_MISSING", f"commit head sequence {commit_count} is missing")
            return tuple(issues)
        if inbox is None:
            issue("COMMIT_INPUT_MISSING", f"commit {commit_count} has no inbox row")
            return tuple(issues)

        input_id = str(commit["input_id"])
        if history is not None and str(history["input_id"]) != input_id:
            issue("PROJECTION_HISTORY_INPUT", "projection head input differs from commit")
        try:
            input_payload = _json_object(str(inbox["payload_json"]), label="head input")
            if canonical_json(input_payload) != str(inbox["payload_json"]):
                issue("INPUT_NOT_CANONICAL", "head input JSON is not canonical")
            if canonical_sha256(input_payload) != str(inbox["payload_hash"]):
                issue("INPUT_HASH", "head input hash differs")
        except (TypeError, ValueError) as error:
            issue("INPUT_INVALID", str(error))
        if (
            str(inbox["input_id"]) != input_id
            or int(inbox["commit_sequence"]) != commit_count
            or str(inbox["commit_hash"]) != str(commit["commit_hash"])
            or int(inbox["last_event_sequence"]) != int(commit["last_event_sequence"])
        ):
            issue("COMMIT_INPUT_MISMATCH", f"commit {commit_count} differs from inbox")

        first_raw = commit["first_event_sequence"]
        first_sequence = int(first_raw) if first_raw is not None else None
        last_sequence = int(commit["last_event_sequence"])
        event_rows = connection.execute(
            """
            SELECT * FROM paper_events
            WHERE run_id=? AND input_id=? ORDER BY sequence
            """,
            (run_id, input_id),
        ).fetchall()
        expected_event_previous = _event_genesis(run_id, calculated_config_hash)
        if first_sequence is not None and first_sequence > 1:
            prior_event = connection.execute(
                """
                SELECT event_hash FROM paper_events
                WHERE run_id=? AND sequence=?
                """,
                (run_id, first_sequence - 1),
            ).fetchone()
            if prior_event is None:
                issue("COMMIT_PREVIOUS_EVENT_MISSING", f"event {first_sequence - 1} is missing")
            else:
                expected_event_previous = str(prior_event[0])
        event_hashes: list[str] = []
        expected_alert_rows: dict[str, tuple[int, str, str]] = {}
        event_sequences: dict[str, int] = {}
        for offset, event in enumerate(event_rows):
            sequence = int(event["sequence"])
            if first_sequence is None or sequence != first_sequence + offset:
                issue("COMMIT_EVENT_SEQUENCE", f"commit {commit_count} events are not contiguous")
            try:
                event_payload = _json_object(
                    str(event["payload_json"]),
                    label=f"commit {commit_count} event {sequence}",
                )
                canonical_event = canonical_json(event_payload)
                event_payload_hash = canonical_sha256(event_payload)
                if canonical_event != str(event["payload_json"]):
                    issue("COMMIT_EVENT_NOT_CANONICAL", f"event {sequence} is not canonical")
                if event_payload_hash != str(event["payload_hash"]):
                    issue("COMMIT_EVENT_PAYLOAD_HASH", f"event {sequence} payload hash differs")
                if event_payload.get("event_id") != str(event["event_id"]):
                    issue("COMMIT_EVENT_ID", f"event {sequence} ID differs from payload")
                event_type = event_payload.get("event_type")
                if event_type != str(event["event_type"]):
                    issue("COMMIT_EVENT_TYPE", f"event {sequence} type differs from payload")
                if event_payload.get("run_id") != run_id:
                    issue("COMMIT_EVENT_RUN", f"event {sequence} belongs to another run")
                if event_type == PaperEventType.ALERT_RAISED.value:
                    raw_alert = event_payload.get("payload")
                    if not isinstance(raw_alert, Mapping):
                        issue("COMMIT_ALERT_EVENT_PAYLOAD", f"event {sequence} lacks an alert")
                    else:
                        alert_payload = cast(dict[str, JsonValue], dict(raw_alert))
                        alert_id = self._alert_id(run_id, alert_payload)
                        expected_alert_rows[alert_id] = (
                            sequence,
                            canonical_json(alert_payload),
                            canonical_sha256(alert_payload),
                        )
                calculated_event_hash = _event_hash(
                    sequence=sequence,
                    payload=event_payload,
                    previous_hash=str(event["previous_hash"]),
                )
                if calculated_event_hash != str(event["event_hash"]):
                    issue("COMMIT_EVENT_HASH", f"event {sequence} hash differs")
            except (TypeError, ValueError) as error:
                issue("COMMIT_EVENT_INVALID", f"event {sequence}: {error}")
            if str(event["previous_hash"]) != expected_event_previous:
                issue("COMMIT_EVENT_PREVIOUS", f"event {sequence} previous hash differs")
            expected_event_previous = str(event["event_hash"])
            event_hashes.append(str(event["event_hash"]))
            event_sequences[str(event["event_id"])] = sequence

        transaction_rows = connection.execute(
            """
            SELECT * FROM paper_ledger_transactions
            WHERE run_id=? AND input_id=? ORDER BY rowid
            """,
            (run_id, input_id),
        ).fetchall()
        ledger_hashes: list[str] = []
        for transaction in transaction_rows:
            transaction_id = str(transaction["transaction_id"])
            entry_rows = connection.execute(
                """
                SELECT * FROM paper_ledger_entries
                WHERE run_id=? AND transaction_id=? ORDER BY entry_index
                """,
                (run_id, transaction_id),
            ).fetchall()
            entries: list[_PreparedLedgerEntry] = []
            balances: dict[str, Decimal] = defaultdict(Decimal)
            for entry_index, entry in enumerate(entry_rows):
                if int(entry["entry_index"]) != entry_index:
                    issue(
                        "COMMIT_LEDGER_ENTRY_SEQUENCE",
                        f"transaction {transaction_id!r} entry indexes differ",
                    )
                try:
                    entry_payload = _json_object(
                        str(entry["payload_json"]),
                        label=f"ledger entry {entry['entry_id']}",
                    )
                    entry_hash = canonical_sha256(entry_payload)
                    amount_text = _decimal_text(entry_payload.get("amount"), label="ledger amount")
                    if canonical_json(entry_payload) != str(entry["payload_json"]):
                        issue(
                            "COMMIT_LEDGER_NOT_CANONICAL",
                            f"ledger entry {entry['entry_id']!r} is not canonical",
                        )
                    if entry_hash != str(entry["entry_hash"]):
                        issue(
                            "COMMIT_LEDGER_ENTRY_HASH",
                            f"ledger entry {entry['entry_id']!r} hash differs",
                        )
                    if amount_text != str(entry["amount_text"]):
                        issue(
                            "COMMIT_LEDGER_AMOUNT",
                            f"ledger entry {entry['entry_id']!r} amount differs",
                        )
                    expected_entry_columns = {
                        "account": str(entry["account"]),
                        "currency": str(entry["unit"]),
                        "entry_id": str(entry["entry_id"]),
                        "event_id": cast(str | None, entry["event_id"]),
                        "run_id": run_id,
                        "transaction_id": transaction_id,
                    }
                    if any(
                        entry_payload.get(name) != expected
                        for name, expected in expected_entry_columns.items()
                    ):
                        issue(
                            "COMMIT_LEDGER_COLUMNS",
                            f"ledger entry {entry['entry_id']!r} columns differ from payload",
                        )
                except (TypeError, ValueError) as error:
                    entry_hash = str(entry["entry_hash"])
                    amount_text = str(entry["amount_text"])
                    issue("COMMIT_LEDGER_ENTRY_INVALID", f"entry {entry['entry_id']!r}: {error}")
                try:
                    amount = Decimal(str(entry["amount_text"]))
                    if not amount.is_finite():
                        raise InvalidOperation
                    balances[str(entry["unit"])] += amount
                except InvalidOperation:
                    issue(
                        "COMMIT_LEDGER_AMOUNT_INVALID",
                        f"ledger entry {entry['entry_id']!r} has invalid decimal text",
                    )
                entries.append(
                    _PreparedLedgerEntry(
                        transaction_id=transaction_id,
                        entry_index=int(entry["entry_index"]),
                        entry_id=str(entry["entry_id"]),
                        event_id=cast(str | None, entry["event_id"]),
                        account=str(entry["account"]),
                        unit=str(entry["unit"]),
                        amount_text=amount_text,
                        payload_json=str(entry["payload_json"]),
                        entry_hash=entry_hash,
                    )
                )
            if len(entry_rows) != int(transaction["entry_count"]):
                issue("COMMIT_LEDGER_ENTRY_COUNT", f"transaction {transaction_id!r} count differs")
            if any(balance != Decimal(0) for balance in balances.values()):
                issue("COMMIT_LEDGER_IMBALANCE", f"transaction {transaction_id!r} is imbalanced")
            transaction_event_ids = {entry.event_id for entry in entries}
            if len(transaction_event_ids) != 1 or None in transaction_event_ids:
                issue(
                    "COMMIT_LEDGER_EVENT",
                    f"transaction {transaction_id!r} does not bind exactly one event",
                )
            else:
                transaction_event_id = cast(str, next(iter(transaction_event_ids)))
                if event_sequences.get(transaction_event_id) != int(transaction["event_sequence"]):
                    issue(
                        "COMMIT_LEDGER_EVENT_SEQUENCE",
                        f"transaction {transaction_id!r} event sequence differs",
                    )
            calculated_transaction_hash = _ledger_transaction_hash(
                run_id,
                transaction_id,
                entries,
            )
            if calculated_transaction_hash != str(transaction["transaction_hash"]):
                issue("COMMIT_LEDGER_HASH", f"transaction {transaction_id!r} hash differs")
            ledger_hashes.append(str(transaction["transaction_hash"]))

        alert_rows = connection.execute(
            """
            SELECT * FROM paper_alerts
            WHERE run_id=? AND commit_sequence=? ORDER BY rowid
            """,
            (run_id, commit_count),
        ).fetchall()
        alert_hashes: list[str] = []
        durable_alert_ids: set[str] = set()
        for alert in alert_rows:
            alert_id = str(alert["alert_id"])
            durable_alert_ids.add(alert_id)
            try:
                alert_payload = _json_object(
                    str(alert["payload_json"]),
                    label=f"alert {alert_id}",
                )
                alert_hash = canonical_sha256(alert_payload)
                if canonical_json(alert_payload) != str(alert["payload_json"]):
                    issue("COMMIT_ALERT_NOT_CANONICAL", f"alert {alert_id!r} is not canonical")
                if alert_hash != str(alert["payload_hash"]):
                    issue("COMMIT_ALERT_HASH", f"alert {alert_id!r} hash differs")
                if (
                    self._alert_id(run_id, alert_payload) != alert_id
                    or alert_payload.get("severity") != str(alert["severity"])
                    or alert_payload.get("code") != str(alert["code"])
                ):
                    issue("COMMIT_ALERT_COLUMNS", f"alert {alert_id!r} columns differ")
            except (TypeError, ValueError) as error:
                issue("COMMIT_ALERT_INVALID", f"alert {alert_id!r}: {error}")
            expected_alert = expected_alert_rows.get(alert_id)
            if expected_alert is None:
                issue("COMMIT_ALERT_EVENT_MISSING", f"alert {alert_id!r} lacks an event")
            elif (
                int(alert["event_sequence"]) != expected_alert[0]
                or str(alert["payload_json"]) != expected_alert[1]
                or str(alert["payload_hash"]) != expected_alert[2]
            ):
                issue("COMMIT_ALERT_EVENT_MISMATCH", f"alert {alert_id!r} differs from event")
            alert_hashes.append(str(alert["payload_hash"]))
        if set(expected_alert_rows) != durable_alert_ids:
            issue("COMMIT_ALERT_SET", f"commit {commit_count} alert rows differ from events")
        try:
            stored_event_hashes = cast(
                list[str],
                json.loads(str(commit["event_hashes_json"])),
            )
            stored_ledger_hashes = cast(
                list[str],
                json.loads(str(commit["ledger_hashes_json"])),
            )
            stored_alert_hashes = cast(
                list[str],
                json.loads(str(commit["alert_hashes_json"])),
            )
        except (json.JSONDecodeError, TypeError) as error:
            issue("COMMIT_COMPONENT_JSON", f"commit {commit_count}: {error}")
            stored_event_hashes, stored_ledger_hashes, stored_alert_hashes = [], [], []
        if stored_event_hashes != event_hashes:
            issue("COMMIT_EVENT_HASHES", f"commit {commit_count} event hashes differ")
        if stored_ledger_hashes != ledger_hashes:
            issue("COMMIT_LEDGER_HASHES", f"commit {commit_count} ledger hashes differ")
        if stored_alert_hashes != alert_hashes:
            issue("COMMIT_ALERT_HASHES", f"commit {commit_count} alert hashes differ")
        if first_sequence is None or first_sequence + len(event_hashes) - 1 != last_sequence:
            issue("COMMIT_EVENT_RANGE", f"commit {commit_count} event range differs")
        if last_sequence != event_count:
            issue("COMMIT_EVENT_COVERAGE", "head commit does not end at the run event head")
        if int(commit["projection_revision"]) != revision:
            issue("COMMIT_PROJECTION_REVISION", "head commit projection revision differs")
        if str(commit["projection_hash"]) != str(run["projection_hash"]):
            issue("COMMIT_PROJECTION_HASH", "head commit projection hash differs")

        prior_commit = (
            None
            if commit_count == 1
            else connection.execute(
                """
                SELECT commit_hash FROM paper_commits
                WHERE run_id=? AND commit_sequence=?
                """,
                (run_id, commit_count - 1),
            ).fetchone()
        )
        expected_previous_commit_hash = _commit_genesis(run_id, calculated_config_hash)
        if commit_count > 1:
            if prior_commit is None:
                issue("COMMIT_PREVIOUS_MISSING", f"commit {commit_count - 1} is missing")
            else:
                expected_previous_commit_hash = str(prior_commit[0])
        if str(commit["previous_commit_hash"]) != expected_previous_commit_hash:
            issue("COMMIT_CHAIN_PREVIOUS", f"commit {commit_count} previous hash differs")
        calculated_commit_hash = _commit_hash(
            run_id=run_id,
            commit_sequence=commit_count,
            input_id=input_id,
            first_event_sequence=first_sequence,
            last_event_sequence=last_sequence,
            event_hashes=stored_event_hashes,
            ledger_hashes=stored_ledger_hashes,
            projection_revision=int(commit["projection_revision"]),
            projection_hash=str(commit["projection_hash"]),
            alert_hashes=stored_alert_hashes,
            previous_commit_hash=str(commit["previous_commit_hash"]),
        )
        if calculated_commit_hash != str(commit["commit_hash"]):
            issue("COMMIT_HASH", f"commit {commit_count} hash differs")
        if str(commit["commit_hash"]) != commit_head:
            issue("COMMIT_HEAD_MISMATCH", "run commit head differs from the head row")
        return tuple(issues)

    def _collect_integrity_issues(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> tuple[IntegrityIssue, ...]:
        _raise_if_interrupted(should_stop)
        issues = _IntegrityIssueCollector()

        def issue(code: str, detail: str) -> None:
            issues.add(code, detail)
            self._observe_integrity_buffer("issue_codes", len(issues))

        run = connection.execute(
            "SELECT * FROM paper_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if run is None:
            return (IntegrityIssue("RUN_MISSING", f"run {run_id!r} is missing"),)
        try:
            config = _json_object(str(run["config_json"]), label="run config_json")
            canonical_config = canonical_json(config)
            config_hash = canonical_sha256(config)
            if canonical_config != str(run["config_json"]):
                issue("CONFIG_NOT_CANONICAL", "stored config JSON is not canonical")
            if config_hash != str(run["config_hash"]):
                issue("CONFIG_HASH_MISMATCH", "stored config hash does not match config JSON")
        except (TypeError, ValueError) as error:
            config_hash = str(run["config_hash"])
            issue("CONFIG_INVALID", str(error))
        if int(run["schema_version"]) != SCHEMA_VERSION:
            issue("RUN_SCHEMA_VERSION", "run schema version is unknown")
        if str(run["status"]) not in PAPER_STATES:
            issue("RUN_STATE_UNKNOWN", f"unknown run state {run['status']!r}")
        expected_event_previous = _event_genesis(run_id, config_hash)
        event_rows = connection.execute(
            "SELECT * FROM paper_events WHERE run_id=? ORDER BY sequence",
            (run_id,),
        )
        expected_sequence = 0
        event_count = 0
        for row in event_rows:
            event_count += 1
            self._observe_integrity_buffer("event_row", 1)
            _raise_if_interrupted(should_stop)
            sequence = int(row["sequence"])
            expected_sequence += 1
            if sequence != expected_sequence:
                issue(
                    "EVENT_SEQUENCE_GAP",
                    f"expected event sequence {expected_sequence}, found {sequence}",
                )
                expected_sequence = sequence
            try:
                payload = _json_object(str(row["payload_json"]), label=f"event {sequence}")
                canonical_payload = canonical_json(payload)
                payload_hash = canonical_sha256(payload)
                if canonical_payload != str(row["payload_json"]):
                    issue("EVENT_NOT_CANONICAL", f"event {sequence} JSON is not canonical")
                if payload_hash != str(row["payload_hash"]):
                    issue("EVENT_PAYLOAD_HASH", f"event {sequence} payload hash differs")
                payload_event_id = payload.get("event_id", payload.get("id"))
                if payload_event_id is not None and payload_event_id != str(row["event_id"]):
                    issue("EVENT_ID_MISMATCH", f"event {sequence} ID differs from payload")
                payload_type = payload.get("event_type", payload.get("type", payload.get("kind")))
                if payload_type != str(row["event_type"]):
                    issue("EVENT_TYPE_MISMATCH", f"event {sequence} type differs from payload")
                if payload.get("run_id") != run_id:
                    issue("EVENT_RUN_MISMATCH", f"event {sequence} belongs to another run")
                if payload_type == PaperEventType.ALERT_RAISED.value:
                    raw_alert = payload.get("payload")
                    if not isinstance(raw_alert, Mapping):
                        issue("ALERT_EVENT_PAYLOAD", f"event {sequence} lacks an alert object")
                    else:
                        alert_payload = cast(dict[str, JsonValue], dict(raw_alert))
                        alert_id = self._alert_id(run_id, alert_payload)
                        expected_alert_json = canonical_json(alert_payload)
                        expected_alert_hash = canonical_sha256(alert_payload)
                        alert_row = connection.execute(
                            """
                            SELECT commit_sequence, event_sequence, payload_json, payload_hash
                            FROM paper_alerts WHERE run_id=? AND alert_id=?
                            """,
                            (run_id, alert_id),
                        ).fetchone()
                        if alert_row is None or alert_row["commit_sequence"] is None:
                            issue(
                                "ALERT_ROWS_MISSING",
                                f"ALERT_RAISED event {sequence} lacks a committed alert row",
                            )
                        elif (
                            int(alert_row["event_sequence"]) != sequence
                            or str(alert_row["payload_json"]) != expected_alert_json
                            or str(alert_row["payload_hash"]) != expected_alert_hash
                        ):
                            issue(
                                "ALERT_EVENT_MISMATCH",
                                f"alert {alert_id!r} differs from ALERT_RAISED event {sequence}",
                            )
            except (TypeError, ValueError) as error:
                payload = {}
                issue("EVENT_INVALID", f"event {sequence}: {error}")
            stored_previous = str(row["previous_hash"])
            if stored_previous != expected_event_previous:
                issue("EVENT_CHAIN_PREVIOUS", f"event {sequence} previous hash differs")
            calculated_event_hash = _event_hash(
                sequence=sequence,
                payload=payload,
                previous_hash=stored_previous,
            )
            if calculated_event_hash != str(row["event_hash"]):
                issue("EVENT_CHAIN_HASH", f"event {sequence} chain hash differs")
            expected_event_previous = str(row["event_hash"])
        if event_count != int(run["event_count"]):
            issue(
                "EVENT_COUNT_MISMATCH",
                f"run anchors {run['event_count']} events but table contains {event_count}",
            )
        if expected_event_previous != str(run["event_head_hash"]):
            issue("EVENT_HEAD_MISMATCH", "run event head differs from verified chain head")

        transaction_rows = connection.execute(
            """
            SELECT * FROM paper_ledger_transactions
            WHERE run_id=? ORDER BY rowid
            """,
            (run_id,),
        )
        for transaction in transaction_rows:
            self._observe_integrity_buffer("ledger_transaction_row", 1)
            _raise_if_interrupted(should_stop)
            transaction_id = str(transaction["transaction_id"])
            rows = connection.execute(
                """
                SELECT * FROM paper_ledger_entries
                WHERE run_id=? AND transaction_id=? ORDER BY entry_index
                """,
                (run_id, transaction_id),
            )
            prepared: list[_PreparedLedgerEntry] = []
            balances: dict[str, Decimal] = defaultdict(Decimal)
            entry_count = 0
            transaction_event_id: str | None = None
            transaction_event_mismatch = False
            for index, row in enumerate(rows):
                entry_count += 1
                self._observe_integrity_buffer("ledger_entry_row", 1)
                _raise_if_interrupted(should_stop)
                if int(row["entry_index"]) != index:
                    issue(
                        "LEDGER_ENTRY_GAP",
                        f"transaction {transaction_id!r} has a non-contiguous entry index",
                    )
                try:
                    payload = _json_object(
                        str(row["payload_json"]),
                        label=f"ledger entry {row['entry_id']}",
                    )
                    payload_json = canonical_json(payload)
                    entry_hash = canonical_sha256(payload)
                    if payload_json != str(row["payload_json"]):
                        issue(
                            "LEDGER_NOT_CANONICAL",
                            f"ledger entry {row['entry_id']!r} JSON is not canonical",
                        )
                    if entry_hash != str(row["entry_hash"]):
                        issue(
                            "LEDGER_ENTRY_HASH",
                            f"ledger entry {row['entry_id']!r} hash differs",
                        )
                    if (
                        payload.get("entry_id") != str(row["entry_id"])
                        or payload.get("run_id") != run_id
                        or payload.get("event_id") != cast(str | None, row["event_id"])
                        or payload.get("transaction_id") != transaction_id
                        or payload.get("account") != str(row["account"])
                        or payload.get("currency") != str(row["unit"])
                    ):
                        issue(
                            "LEDGER_ENTRY_METADATA",
                            f"ledger entry {row['entry_id']!r} columns differ from payload",
                        )
                    amount_text = _decimal_text(payload.get("amount"), label="ledger amount")
                    if amount_text != str(row["amount_text"]):
                        issue(
                            "LEDGER_AMOUNT_MISMATCH",
                            f"ledger entry {row['entry_id']!r} amount differs from payload",
                        )
                except (TypeError, ValueError) as error:
                    entry_hash = str(row["entry_hash"])
                    amount_text = str(row["amount_text"])
                    issue("LEDGER_ENTRY_INVALID", f"entry {row['entry_id']!r}: {error}")
                try:
                    amount = Decimal(str(row["amount_text"]))
                    if not amount.is_finite():
                        raise InvalidOperation
                    balances[str(row["unit"])] += amount
                except InvalidOperation:
                    issue(
                        "LEDGER_AMOUNT_INVALID",
                        f"ledger entry {row['entry_id']!r} has invalid decimal text",
                    )
                event_id = cast(str | None, row["event_id"])
                if event_id is None:
                    transaction_event_mismatch = True
                elif transaction_event_id is None:
                    transaction_event_id = event_id
                elif event_id != transaction_event_id:
                    transaction_event_mismatch = True
                if (
                    event_id is not None
                    and connection.execute(
                        "SELECT 1 FROM paper_events WHERE run_id=? AND event_id=?",
                        (run_id, event_id),
                    ).fetchone()
                    is None
                ):
                    issue(
                        "LEDGER_EVENT_MISSING",
                        f"ledger entry {row['entry_id']!r} references an unknown event",
                    )
                prepared.append(
                    _PreparedLedgerEntry(
                        transaction_id=transaction_id,
                        entry_index=int(row["entry_index"]),
                        entry_id=str(row["entry_id"]),
                        event_id=event_id,
                        account=str(row["account"]),
                        unit=str(row["unit"]),
                        amount_text=amount_text,
                        payload_json=str(row["payload_json"]),
                        entry_hash=entry_hash,
                    )
                )
                self._observe_integrity_buffer("transaction_entries", len(prepared))
                self._observe_integrity_buffer("transaction_units", len(balances))
            if entry_count != int(transaction["entry_count"]):
                issue(
                    "LEDGER_ENTRY_COUNT",
                    f"transaction {transaction_id!r} entry count differs",
                )
            for unit, balance in balances.items():
                if balance != Decimal(0):
                    issue(
                        "LEDGER_IMBALANCE",
                        f"transaction {transaction_id!r} unit {unit!r} balances to {balance}",
                    )
            if transaction_event_mismatch or transaction_event_id is None:
                issue(
                    "LEDGER_TRANSACTION_EVENT",
                    f"transaction {transaction_id!r} does not bind exactly one event",
                )
            else:
                event_row = connection.execute(
                    "SELECT sequence FROM paper_events WHERE run_id=? AND event_id=?",
                    (run_id, transaction_event_id),
                ).fetchone()
                if event_row is None:
                    issue(
                        "LEDGER_EVENT_MISSING",
                        f"transaction {transaction_id!r} references an unknown event",
                    )
                elif int(event_row["sequence"]) != int(transaction["event_sequence"]):
                    issue(
                        "LEDGER_EVENT_SEQUENCE",
                        f"transaction {transaction_id!r} event sequence differs",
                    )
            calculated_transaction_hash = _ledger_transaction_hash(
                run_id,
                transaction_id,
                prepared,
            )
            if calculated_transaction_hash != str(transaction["transaction_hash"]):
                issue(
                    "LEDGER_TRANSACTION_HASH",
                    f"transaction {transaction_id!r} hash differs",
                )

        alert_rows = connection.execute(
            "SELECT * FROM paper_alerts WHERE run_id=? ORDER BY rowid",
            (run_id,),
        )
        has_safety_alert = False
        for row in alert_rows:
            self._observe_integrity_buffer("alert_row", 1)
            _raise_if_interrupted(should_stop)
            alert_id = str(row["alert_id"])
            alert_code = str(row["code"])
            if alert_code.endswith("INTEGRITY_FAILURE") or alert_code.endswith("CONFLICT"):
                has_safety_alert = True
            try:
                payload = _json_object(str(row["payload_json"]), label=f"alert {alert_id}")
                payload_json = canonical_json(payload)
                payload_hash = canonical_sha256(payload)
                if payload_json != str(row["payload_json"]):
                    issue("ALERT_NOT_CANONICAL", f"alert {alert_id!r} is not canonical")
                if payload_hash != str(row["payload_hash"]):
                    issue("ALERT_HASH", f"alert {alert_id!r} hash differs")
                if payload.get("code") != alert_code or payload.get("severity") != str(row["severity"]):
                    issue(
                        "ALERT_METADATA_MISMATCH",
                        f"alert {alert_id!r} columns differ from payload",
                    )
            except (TypeError, ValueError) as error:
                issue("ALERT_INVALID", f"alert {alert_id!r}: {error}")
            commit_raw = row["commit_sequence"]
            if commit_raw is not None:
                event_row = connection.execute(
                    """
                    SELECT event_type, payload_json FROM paper_events
                    WHERE run_id=? AND sequence=?
                    """,
                    (run_id, int(row["event_sequence"])),
                ).fetchone()
                if event_row is None or str(event_row["event_type"]) != (PaperEventType.ALERT_RAISED.value):
                    issue(
                        "ALERT_EVENT_MISSING",
                        f"alert {alert_id!r} has no ALERT_RAISED event",
                    )
                else:
                    try:
                        event_payload = _json_object(
                            str(event_row["payload_json"]),
                            label=f"alert event {row['event_sequence']}",
                        )
                        raw_alert = event_payload.get("payload")
                        if not isinstance(raw_alert, Mapping):
                            raise TypeError("ALERT_RAISED payload is not an object")
                        expected_alert = cast(dict[str, JsonValue], dict(raw_alert))
                        if (
                            self._alert_id(run_id, expected_alert) != alert_id
                            or canonical_json(expected_alert) != str(row["payload_json"])
                            or canonical_sha256(expected_alert) != str(row["payload_hash"])
                        ):
                            issue(
                                "ALERT_EVENT_MISMATCH",
                                f"alert {alert_id!r} differs from its ALERT_RAISED event",
                            )
                    except (TypeError, ValueError) as error:
                        issue(
                            "ALERT_EVENT_MISMATCH",
                            f"alert {alert_id!r} event is invalid: {error}",
                        )

        inbox_rows = connection.execute(
            "SELECT * FROM paper_inbox WHERE run_id=? ORDER BY commit_sequence",
            (run_id,),
        )
        inbox_count = 0
        for row in inbox_rows:
            inbox_count += 1
            self._observe_integrity_buffer("inbox_row", 1)
            _raise_if_interrupted(should_stop)
            try:
                payload = _json_object(str(row["payload_json"]), label=f"input {row['input_id']}")
                if canonical_json(payload) != str(row["payload_json"]):
                    issue("INPUT_NOT_CANONICAL", f"input {row['input_id']!r} is not canonical")
                if canonical_sha256(payload) != str(row["payload_hash"]):
                    issue("INPUT_HASH", f"input {row['input_id']!r} hash differs")
            except (TypeError, ValueError) as error:
                issue("INPUT_INVALID", f"input {row['input_id']!r}: {error}")

        def component_hashes(raw: object, *, commit_sequence: int, label: str) -> list[str]:
            try:
                parsed = json.loads(str(raw))
            except json.JSONDecodeError as error:
                issue("COMMIT_COMPONENT_JSON", f"commit {commit_sequence}: {error}")
                return []
            if not isinstance(parsed, list) or any(not isinstance(value, str) for value in parsed):
                issue(
                    "COMMIT_COMPONENT_JSON",
                    f"commit {commit_sequence} {label} must be a JSON string list",
                )
                return []
            return cast(list[str], parsed)

        expected_commit_previous = _commit_genesis(run_id, config_hash)
        commit_rows = connection.execute(
            "SELECT * FROM paper_commits WHERE run_id=? ORDER BY commit_sequence",
            (run_id,),
        )
        commit_count = 0
        prior_last_sequence = 0
        for expected_commit_sequence, row in enumerate(commit_rows, start=1):
            commit_count += 1
            self._observe_integrity_buffer("commit_row", 1)
            _raise_if_interrupted(should_stop)
            commit_sequence = int(row["commit_sequence"])
            if commit_sequence != expected_commit_sequence:
                issue(
                    "COMMIT_SEQUENCE_GAP",
                    f"expected commit {expected_commit_sequence}, found {commit_sequence}",
                )
            input_id = str(row["input_id"])
            first_raw = row["first_event_sequence"]
            first_sequence = int(first_raw) if first_raw is not None else None
            last_sequence = int(row["last_event_sequence"])
            expected_hashes = [
                str(component["event_hash"])
                for component in connection.execute(
                    """
                    SELECT event_hash FROM paper_events
                    WHERE run_id=? AND input_id=? ORDER BY sequence
                    """,
                    (run_id, input_id),
                )
            ]
            expected_ledger_hashes = [
                str(component["transaction_hash"])
                for component in connection.execute(
                    """
                    SELECT transaction_hash FROM paper_ledger_transactions
                    WHERE run_id=? AND input_id=? ORDER BY rowid
                    """,
                    (run_id, input_id),
                )
            ]
            expected_alert_hashes = [
                str(component["payload_hash"])
                for component in connection.execute(
                    """
                    SELECT payload_hash FROM paper_alerts
                    WHERE run_id=? AND commit_sequence=? ORDER BY rowid
                    """,
                    (run_id, commit_sequence),
                )
            ]
            self._observe_integrity_buffer("commit_event_hashes", len(expected_hashes))
            self._observe_integrity_buffer("commit_ledger_hashes", len(expected_ledger_hashes))
            self._observe_integrity_buffer("commit_alert_hashes", len(expected_alert_hashes))
            if first_sequence is None:
                if expected_hashes or last_sequence != prior_last_sequence:
                    issue("COMMIT_EVENT_RANGE", f"commit {commit_sequence} has an invalid empty range")
            else:
                if first_sequence != prior_last_sequence + 1:
                    issue("COMMIT_EVENT_RANGE", f"commit {commit_sequence} does not continue the chain")
                if last_sequence != first_sequence + len(expected_hashes) - 1:
                    issue("COMMIT_EVENT_RANGE", f"commit {commit_sequence} event range differs")
            prior_last_sequence = last_sequence
            event_hashes = component_hashes(
                row["event_hashes_json"],
                commit_sequence=commit_sequence,
                label="event hashes",
            )
            ledger_hashes = component_hashes(
                row["ledger_hashes_json"],
                commit_sequence=commit_sequence,
                label="ledger hashes",
            )
            alert_hashes = component_hashes(
                row["alert_hashes_json"],
                commit_sequence=commit_sequence,
                label="alert hashes",
            )
            self._observe_integrity_buffer("stored_commit_event_hashes", len(event_hashes))
            self._observe_integrity_buffer("stored_commit_ledger_hashes", len(ledger_hashes))
            self._observe_integrity_buffer("stored_commit_alert_hashes", len(alert_hashes))
            if event_hashes != expected_hashes:
                issue("COMMIT_EVENT_HASHES", f"commit {commit_sequence} event hashes differ")
            if ledger_hashes != expected_ledger_hashes:
                issue("COMMIT_LEDGER_HASHES", f"commit {commit_sequence} ledger hashes differ")
            if alert_hashes != expected_alert_hashes:
                issue("COMMIT_ALERT_HASHES", f"commit {commit_sequence} alert hashes differ")
            if str(row["previous_commit_hash"]) != expected_commit_previous:
                issue("COMMIT_CHAIN_PREVIOUS", f"commit {commit_sequence} previous hash differs")
            calculated_commit_hash = _commit_hash(
                run_id=run_id,
                commit_sequence=commit_sequence,
                input_id=input_id,
                first_event_sequence=first_sequence,
                last_event_sequence=last_sequence,
                event_hashes=event_hashes,
                ledger_hashes=ledger_hashes,
                projection_revision=int(row["projection_revision"]),
                projection_hash=str(row["projection_hash"]),
                alert_hashes=alert_hashes,
                previous_commit_hash=str(row["previous_commit_hash"]),
            )
            if calculated_commit_hash != str(row["commit_hash"]):
                issue("COMMIT_HASH", f"commit {commit_sequence} hash differs")
            expected_commit_previous = str(row["commit_hash"])
            history_anchor = connection.execute(
                """
                SELECT input_id, revision, projection_hash
                FROM paper_projection_history WHERE run_id=? AND revision=?
                """,
                (run_id, commit_sequence),
            ).fetchone()
            if (
                history_anchor is None
                or cast(str | None, history_anchor["input_id"]) != input_id
                or int(row["projection_revision"]) != commit_sequence
                or int(history_anchor["revision"]) != commit_sequence
                or str(history_anchor["projection_hash"]) != str(row["projection_hash"])
            ):
                issue(
                    "PROJECTION_COMMIT_ANCHOR",
                    f"projection revision {commit_sequence} differs",
                )
            inbox = connection.execute(
                "SELECT * FROM paper_inbox WHERE run_id=? AND commit_sequence=?",
                (run_id, commit_sequence),
            ).fetchone()
            if inbox is None:
                issue("COMMIT_INPUT_MISSING", f"commit {commit_sequence} has no inbox record")
            elif (
                str(inbox["input_id"]) != input_id
                or str(inbox["commit_hash"]) != str(row["commit_hash"])
                or (int(inbox["first_event_sequence"]) if inbox["first_event_sequence"] is not None else None)
                != first_sequence
                or int(inbox["last_event_sequence"]) != last_sequence
            ):
                issue("COMMIT_INPUT_MISMATCH", f"commit {commit_sequence} differs from inbox")
        if inbox_count != commit_count:
            issue("INBOX_COMMIT_COUNT", "inbox and commit counts differ")
        if commit_count != int(run["commit_count"]):
            issue(
                "COMMIT_COUNT_MISMATCH",
                f"run anchors {run['commit_count']} commits but table contains {commit_count}",
            )
        if expected_commit_previous != str(run["commit_head_hash"]):
            issue("COMMIT_HEAD_MISMATCH", "run commit head differs from verified commit chain")
        if prior_last_sequence != event_count:
            issue("COMMIT_EVENT_COVERAGE", "commit ranges do not cover the complete event journal")
        if (
            connection.execute(
                """
            SELECT 1 FROM paper_alerts AS alert
            WHERE alert.run_id=? AND alert.commit_sequence IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM paper_commits AS commit_row
                  WHERE commit_row.run_id=alert.run_id
                    AND commit_row.commit_sequence=alert.commit_sequence
              )
            LIMIT 1
            """,
                (run_id,),
            ).fetchone()
            is not None
        ):
            issue("COMMIT_ALERT_HASHES", "a committed alert has no owning commit")

        history_rows = connection.execute(
            "SELECT * FROM paper_projection_history WHERE run_id=? ORDER BY revision",
            (run_id,),
        )
        history_count = 0
        latest_history_hash: str | None = None
        latest_history_json: str | None = None
        for expected_revision, row in enumerate(history_rows):
            history_count += 1
            self._observe_integrity_buffer("projection_history_row", 1)
            _raise_if_interrupted(should_stop)
            revision = int(row["revision"])
            if revision != expected_revision:
                issue("PROJECTION_HISTORY_GAP", f"expected projection revision {expected_revision}")
            try:
                history_payload_json = _projection_history_json(
                    row,
                    label=f"projection revision {revision}",
                )
                payload = _json_object(
                    history_payload_json,
                    label=f"projection revision {revision}",
                )
                if canonical_json(payload) != history_payload_json:
                    issue("PROJECTION_NOT_CANONICAL", f"projection revision {revision}")
                if canonical_sha256(payload) != str(row["projection_hash"]):
                    issue("PROJECTION_HASH", f"projection revision {revision} hash differs")
                self._validate_projection_binding(
                    payload,
                    run_id=run_id,
                    config_hash=config_hash,
                    event_sequence=int(row["event_sequence"]),
                    event_head_hash=str(row["event_head_hash"]),
                )
                if _state_from_projection(payload, default=str(row["status"])) != str(row["status"]):
                    issue("PROJECTION_STATE", f"projection revision {revision} state differs")
            except (TypeError, ValueError) as error:
                issue("PROJECTION_INVALID", f"projection revision {revision}: {error}")
            if revision > 0:
                anchor = connection.execute(
                    """
                    SELECT input_id, projection_revision, projection_hash
                    FROM paper_commits WHERE run_id=? AND commit_sequence=?
                    """,
                    (run_id, revision),
                ).fetchone()
                if (
                    anchor is None
                    or str(anchor["input_id"]) != cast(str | None, row["input_id"])
                    or int(anchor["projection_revision"]) != revision
                    or str(anchor["projection_hash"]) != str(row["projection_hash"])
                ):
                    issue("PROJECTION_COMMIT_ANCHOR", f"projection revision {revision} differs")
            latest_history_hash = str(row["projection_hash"])
            try:
                latest_history_json = _projection_history_json(
                    row,
                    label=f"projection revision {revision}",
                )
            except ValueError:
                latest_history_json = None
        if history_count != commit_count + 1:
            issue("PROJECTION_HISTORY_COUNT", "projection history and commit counts differ")

        projection = connection.execute(
            "SELECT * FROM paper_projections WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if projection is None:
            issue("PROJECTION_MISSING", "current projection is missing")
        else:
            revision = int(projection["revision"])
            if revision != int(run["projection_revision"]):
                issue("PROJECTION_REVISION", "run and current projection revisions differ")
            if int(projection["event_sequence"]) != int(run["event_count"]):
                issue("PROJECTION_EVENT_SEQUENCE", "projection and run event sequences differ")
            if str(projection["event_head_hash"]) != str(run["event_head_hash"]):
                issue("PROJECTION_EVENT_HEAD", "projection and run event heads differ")
            if str(projection["projection_hash"]) != str(run["projection_hash"]):
                issue("PROJECTION_RUN_HASH", "projection and run hashes differ")
            try:
                payload = _json_object(str(projection["payload_json"]), label="current projection")
                calculated = canonical_sha256(payload)
                if canonical_json(payload) != str(projection["payload_json"]):
                    issue("CURRENT_PROJECTION_NOT_CANONICAL", "current projection is not canonical")
                if calculated != str(projection["projection_hash"]):
                    issue("CURRENT_PROJECTION_HASH", "current projection hash differs")
                self._validate_projection_binding(
                    payload,
                    run_id=run_id,
                    config_hash=config_hash,
                    event_sequence=int(projection["event_sequence"]),
                    event_head_hash=str(projection["event_head_hash"]),
                )
                projected_state = _state_from_projection(payload, default=str(projection["status"]))
                if projected_state != str(projection["status"]):
                    issue("CURRENT_PROJECTION_STATE", "current projection state differs")
                try:
                    PaperProjection.from_dict(
                        cast(Mapping[str, object], payload),
                    )
                except (DecimalException, KeyError, TypeError, ValueError) as error:
                    issue(
                        "CURRENT_PROJECTION_MODEL_INVALID",
                        f"{type(error).__name__}: {error}",
                    )
            except (TypeError, ValueError) as error:
                issue("CURRENT_PROJECTION_INVALID", str(error))
            if latest_history_hash is not None and (
                latest_history_hash != str(projection["projection_hash"])
                or latest_history_json != str(projection["payload_json"])
            ):
                issue("CURRENT_PROJECTION_HISTORY", "current projection differs from history")
            safety_latched = str(run["status"]) == "MANUAL_REVIEW" and has_safety_alert
            if not safety_latched and str(projection["status"]) != str(run["status"]):
                issue("RUN_PROJECTION_STATE", "run and projection states differ")
            if safety_latched and str(projection["effective_status"]) != "MANUAL_REVIEW":
                issue("SAFETY_LATCH_MISSING", "integrity alert did not latch effective projection state")
        return issues.freeze()

    def _report(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        issues: tuple[IntegrityIssue, ...],
    ) -> IntegrityReport:
        row = connection.execute(
            """
            SELECT event_count, event_head_hash, commit_count, commit_head_hash
            FROM paper_runs WHERE run_id=?
            """,
            (run_id,),
        ).fetchone()
        if row is None:
            return IntegrityReport(run_id, False, 0, ZERO_HASH, 0, ZERO_HASH, issues)
        return IntegrityReport(
            run_id=run_id,
            ok=not issues,
            event_count=int(row["event_count"]),
            event_head_hash=str(row["event_head_hash"]),
            commit_count=int(row["commit_count"]),
            commit_head_hash=str(row["commit_head_hash"]),
            issues=issues,
        )

    def _persist_guard_alert(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        *,
        code: str,
        detail: str,
        issues: Sequence[IntegrityIssue],
    ) -> None:
        run = connection.execute(
            "SELECT event_count FROM paper_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if run is None:
            return
        payload: dict[str, JsonValue] = {
            "code": code,
            "detail": detail,
            "issues": [{"code": item.code, "detail": item.detail} for item in issues],
            "run_id": run_id,
            "severity": "CRITICAL",
        }
        payload_hash = canonical_sha256(payload)
        alert_id = canonical_sha256(
            {
                "domain": "hyperlab-paper-integrity-alert-v1",
                "payload_hash": payload_hash,
                "run_id": run_id,
            }
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO paper_alerts (
                run_id, alert_id, commit_sequence, event_sequence, severity,
                code, payload_json, payload_hash, created_at
            ) VALUES (?, ?, NULL, ?, 'CRITICAL', ?, ?, ?, ?)
            """,
            (
                run_id,
                alert_id,
                int(run["event_count"]),
                code,
                canonical_json(payload),
                payload_hash,
                _now_text(),
            ),
        )
        connection.execute(
            "UPDATE paper_runs SET status='MANUAL_REVIEW' WHERE run_id=?",
            (run_id,),
        )
        connection.execute(
            "UPDATE paper_projections SET effective_status='MANUAL_REVIEW' WHERE run_id=?",
            (run_id,),
        )

    @staticmethod
    def _run_from_row(row: sqlite3.Row) -> RunRecord:
        seed_raw = cast(str | None, row["seed_text"])
        return RunRecord(
            run_id=str(row["run_id"]),
            config_snapshot=_json_object(str(row["config_json"]), label="config_json"),
            config_hash=str(row["config_hash"]),
            seed=int(seed_raw) if seed_raw is not None else None,
            status=str(row["status"]),
            created_at=str(row["created_at"]),
            event_sequence=int(row["event_count"]),
            event_head_hash=str(row["event_head_hash"]),
            commit_sequence=int(row["commit_count"]),
            commit_head_hash=str(row["commit_head_hash"]),
            projection_revision=int(row["projection_revision"]),
            projection_hash=str(row["projection_hash"]),
        )

    def get_run(self, run_id: str) -> RunRecord:
        normalized_run_id = _identifier(run_id, label="run_id")
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM paper_runs WHERE run_id=?",
                (normalized_run_id,),
            ).fetchone()
        if row is None:
            raise RunNotFoundError(f"unknown paper run {normalized_run_id!r}")
        return self._run_from_row(row)

    def list_runs(self, *, limit: int | None = None) -> tuple[RunRecord, ...]:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0):
            raise ValueError("limit must be a positive integer or None")
        sql = "SELECT * FROM paper_runs ORDER BY created_at, run_id"
        parameters: tuple[object, ...] = ()
        if limit is not None:
            sql = (
                "SELECT * FROM ("
                "SELECT * FROM paper_runs "
                "ORDER BY created_at DESC, run_id DESC LIMIT ?"
                ") ORDER BY created_at, run_id"
            )
            parameters = (limit,)
        with self._read_connection() as connection:
            rows = connection.execute(sql, parameters).fetchall()
        return tuple(self._run_from_row(row) for row in rows)

    def get_event_records(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
        limit: int | None = None,
    ) -> tuple[StoredEventRecord, ...]:
        normalized_run_id = _identifier(run_id, label="run_id")
        if after_sequence < 0:
            raise ValueError("after_sequence must be non-negative")
        if limit is not None and limit <= 0:
            raise ValueError("limit must be positive or None")
        sql = "SELECT * FROM paper_events WHERE run_id=? AND sequence>? ORDER BY sequence"
        parameters: list[object] = [normalized_run_id, after_sequence]
        if limit is not None:
            sql += " LIMIT ?"
            parameters.append(limit)
        with self._read_connection() as connection:
            rows = connection.execute(sql, parameters).fetchall()
        return tuple(
            StoredEventRecord(
                run_id=str(row["run_id"]),
                sequence=int(row["sequence"]),
                event_id=str(row["event_id"]),
                event_type=str(row["event_type"]),
                event=_json_object(str(row["payload_json"]), label="event payload"),
                payload_hash=str(row["payload_hash"]),
                previous_hash=str(row["previous_hash"]),
                event_hash=str(row["event_hash"]),
                input_id=str(row["input_id"]),
                created_at=str(row["created_at"]),
            )
            for row in rows
        )

    def _iter_events_stream(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
    ) -> Iterable[StoredPaperEvent]:
        """Stream the immutable event journal in sequence order with bounded memory."""

        normalized_run_id = _identifier(run_id, label="run_id")
        if after_sequence < 0:
            raise ValueError("after_sequence must be non-negative")
        with self._read_connection() as connection:
            cursor = connection.execute(
                """
                SELECT payload_json, sequence, previous_hash, event_hash
                FROM paper_events
                WHERE run_id=? AND sequence>? ORDER BY sequence
                """,
                (normalized_run_id, after_sequence),
            )
            for row in cursor:
                event_payload = _json_object(str(row["payload_json"]), label="event payload")
                yield StoredPaperEvent(
                    event=PaperEvent.from_dict(event_payload),
                    sequence=int(row["sequence"]),
                    previous_event_hash=str(row["previous_hash"]),
                    event_hash=str(row["event_hash"]),
                )

    def get_event_records_by_type(
        self,
        run_id: str,
        *,
        event_types: Sequence[str],
        after_sequence: int = 0,
        limit: int,
    ) -> tuple[StoredEventRecord, ...]:
        """Return a bounded event page filtered in SQLite, never in memory."""

        normalized_run_id = _identifier(run_id, label="run_id")
        if after_sequence < 0:
            raise ValueError("after_sequence must be non-negative")
        if limit <= 0:
            raise ValueError("limit must be positive")
        normalized_types = tuple(
            dict.fromkeys(_identifier(event_type, label="event_type") for event_type in event_types)
        )
        if not normalized_types:
            return ()
        placeholders = ",".join("?" for _ in normalized_types)
        sql = (
            "SELECT * FROM paper_events "
            f"WHERE run_id=? AND sequence>? AND event_type IN ({placeholders}) "
            "ORDER BY sequence LIMIT ?"
        )
        parameters: list[object] = [
            normalized_run_id,
            after_sequence,
            *normalized_types,
            limit,
        ]
        with self._read_connection() as connection:
            rows = connection.execute(sql, parameters).fetchall()
        return tuple(
            StoredEventRecord(
                run_id=str(row["run_id"]),
                sequence=int(row["sequence"]),
                event_id=str(row["event_id"]),
                event_type=str(row["event_type"]),
                event=_json_object(str(row["payload_json"]), label="event payload"),
                payload_hash=str(row["payload_hash"]),
                previous_hash=str(row["previous_hash"]),
                event_hash=str(row["event_hash"]),
                input_id=str(row["input_id"]),
                created_at=str(row["created_at"]),
            )
            for row in rows
        )

    def get_events(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
        limit: int | None = None,
    ) -> tuple[StoredPaperEvent, ...]:
        records = self.get_event_records(
            run_id,
            after_sequence=after_sequence,
            limit=limit,
        )
        return tuple(
            StoredPaperEvent(
                event=PaperEvent.from_dict(cast(Mapping[str, object], record.event)),
                sequence=record.sequence,
                previous_event_hash=record.previous_hash,
                event_hash=record.event_hash,
            )
            for record in records
        )

    def list_events(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
        limit: int | None = None,
    ) -> tuple[StoredPaperEvent, ...]:
        return self.get_events(run_id, after_sequence=after_sequence, limit=limit)

    def iter_events(
        self,
        run_id: str,
        *,
        after_sequence: int = 0,
    ) -> Iterable[StoredPaperEvent]:
        return self._iter_events_stream(run_id, after_sequence=after_sequence)

    def get_projection_payload(self, run_id: str) -> dict[str, JsonValue]:
        normalized_run_id = _identifier(run_id, label="run_id")
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT payload_json, effective_status FROM paper_projections WHERE run_id=?",
                (normalized_run_id,),
            ).fetchone()
        if row is None:
            raise RunNotFoundError(f"unknown paper run {normalized_run_id!r}")
        payload = _json_object(str(row["payload_json"]), label="projection")
        if str(row["effective_status"]) == "MANUAL_REVIEW":
            payload["state"] = "MANUAL_REVIEW"
            payload["reconciled"] = False
        return payload

    def get_projection(self, run_id: str) -> PaperProjection:
        return PaperProjection.from_dict(self.get_projection_payload(run_id))

    def get_projection_before_received_at(
        self,
        run_id: str,
        *,
        before: datetime,
    ) -> PaperProjection | None:
        """Return the last durable projection strictly before one UTC instant."""

        normalized_run_id = _identifier(run_id, label="run_id")
        if not isinstance(before, datetime):
            raise TypeError("before must be a datetime")
        if before.tzinfo is None or before.utcoffset() is None:
            raise ValueError("before must be timezone-aware")
        cutoff = before.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
        with self._read_connection() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM paper_projection_history
                WHERE run_id=?
                  AND COALESCE(
                        last_received_at,
                        CASE
                            WHEN payload_json != ''
                             AND json_type(payload_json, '$.last_received_at')='text'
                            THEN json_extract(payload_json, '$.last_received_at')
                            ELSE NULL
                        END
                      ) < ?
                ORDER BY revision DESC
                LIMIT 1
                """,
                (normalized_run_id, cutoff),
            ).fetchone()
        if row is None:
            return None
        return PaperProjection.from_dict(
            _projection_history_payload(row, label="historical projection"),
        )

    def load_projection(self, run_id: str) -> PaperProjection:
        return self.get_projection(run_id)

    def get_daily_projection_records(
        self,
        run_id: str,
        *,
        limit: int,
    ) -> tuple[ProjectionHistoryRecord, ...]:
        """Return the last projection of each recent UTC day, chronologically."""

        normalized_run_id = _identifier(run_id, label="run_id")
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                WITH dated AS (
                    SELECT
                        h.*,
                        COALESCE(
                            h.utc_date,
                            CASE
                                WHEN h.payload_json != ''
                                 AND json_type(h.payload_json, '$.last_received_at')='text'
                                THEN substr(
                                    json_extract(h.payload_json, '$.last_received_at'),
                                    1,
                                    10
                                )
                                ELSE substr(
                                    json_extract(r.config_json, '$.validation_started_at'),
                                    1,
                                    10
                                )
                            END
                        ) AS report_utc_date
                    FROM paper_projection_history AS h
                    JOIN paper_runs AS r ON r.run_id=h.run_id
                    WHERE h.run_id=?
                ), ranked AS (
                    SELECT
                        *,
                        row_number() OVER (
                            PARTITION BY report_utc_date
                            ORDER BY revision DESC
                        ) AS day_rank
                    FROM dated
                )
                SELECT *
                FROM ranked
                WHERE day_rank=1
                ORDER BY report_utc_date DESC
                LIMIT ?
                """,
                (normalized_run_id, limit),
            ).fetchall()

        records: list[ProjectionHistoryRecord] = []
        for row in rows:
            payload = _projection_history_payload(
                row,
                label="projection history payload",
            )
            report_projection: dict[str, JsonValue] = {
                "cash": payload.get("cash"),
                "cost_basis": payload.get("cost_basis", {}),
                "fees": payload.get("fees"),
                "initial_cash": payload.get("initial_cash"),
                "inventory_value": payload.get("inventory_value", {}),
                "marks": payload.get("marks", {}),
                "peak_equity": payload.get("peak_equity"),
                "positions": payload.get("positions", {}),
                "realized_pnl": payload.get("realized_pnl"),
                "session_date": payload.get("session_date"),
                "session_start_equity": payload.get("session_start_equity"),
            }
            records.append(
                ProjectionHistoryRecord(
                    run_id=str(row["run_id"]),
                    revision=int(row["revision"]),
                    input_id=(
                        str(row["input_id"])
                        if row["input_id"] is not None
                        else None
                    ),
                    event_sequence=int(row["event_sequence"]),
                    event_head_hash=str(row["event_head_hash"]),
                    status=str(row["status"]),
                    projection=report_projection,
                    projection_hash=str(row["projection_hash"]),
                    created_at=str(row["created_at"]),
                    utc_date=str(row["report_utc_date"]),
                )
            )

        return tuple(reversed(records))

    def get_ledger_account_total(
        self,
        run_id: str,
        *,
        account: str,
        unit: str = "USD",
    ) -> Decimal:
        """Sum one ledger account exactly with bounded memory."""

        normalized_run_id = _identifier(run_id, label="run_id")
        normalized_account = _identifier(account, label="ledger account")
        normalized_unit = _identifier(unit, label="ledger unit")
        total = Decimal(0)
        with self._read_connection() as connection:
            cursor = connection.execute(
                """
                SELECT amount_text FROM paper_ledger_entries
                WHERE run_id=? AND account=? AND unit=?
                ORDER BY transaction_id, entry_index
                """,
                (normalized_run_id, normalized_account, normalized_unit),
            )
            for row in cursor:
                total += Decimal(str(row["amount_text"]))
        return total

    def get_funding_by_utc_date(
        self,
        run_id: str,
        *,
        utc_dates: Sequence[str],
    ) -> dict[str, Decimal]:
        """Sum durable funding events for a bounded set of UTC dates."""

        normalized_run_id = _identifier(run_id, label="run_id")
        normalized_dates = tuple(dict.fromkeys(str(value) for value in utc_dates))
        if len(normalized_dates) > 367:
            raise ValueError("at most 367 UTC dates may be queried at once")
        for value in normalized_dates:
            try:
                parsed = datetime.fromisoformat(f"{value}T00:00:00+00:00")
            except ValueError as error:
                raise ValueError("UTC dates must use YYYY-MM-DD") from error
            if parsed.date().isoformat() != value:
                raise ValueError("UTC dates must use YYYY-MM-DD")
        totals = {value: Decimal(0) for value in normalized_dates}
        if not normalized_dates:
            return totals
        placeholders = ",".join("?" for _ in normalized_dates)
        sql = (
            "SELECT "
            "substr(json_extract(payload_json, '$.received_at'), 1, 10) AS utc_date, "
            "json_extract(payload_json, '$.payload.amount') AS amount_text "
            "FROM paper_events WHERE run_id=? AND event_type=? AND "
            f"substr(json_extract(payload_json, '$.received_at'), 1, 10) IN ({placeholders}) "
            "ORDER BY sequence"
        )
        with self._read_connection() as connection:
            cursor = connection.execute(
                sql,
                [
                    normalized_run_id,
                    PaperEventType.FUNDING_POSTED.value,
                    *normalized_dates,
                ],
            )
            for row in cursor:
                date = str(row["utc_date"])
                totals[date] += Decimal(str(row["amount_text"]))
        return totals

    def iter_ledger_entries(self, run_id: str) -> Iterable[LedgerRecord]:
        """Stream ledger entries in reducer event and transaction order."""

        normalized_run_id = _identifier(run_id, label="run_id")
        with self._read_connection() as connection:
            cursor = connection.execute(
                """
                SELECT ledger.*
                FROM paper_ledger_entries AS ledger
                JOIN paper_events AS event
                  ON event.run_id = ledger.run_id
                 AND event.event_id = ledger.event_id
                WHERE ledger.run_id=?
                ORDER BY event.sequence, ledger.transaction_id, ledger.entry_index
                """,
                (normalized_run_id,),
            )
            for row in cursor:
                yield LedgerRecord(
                    run_id=str(row["run_id"]),
                    transaction_id=str(row["transaction_id"]),
                    entry_index=int(row["entry_index"]),
                    entry_id=str(row["entry_id"]),
                    event_id=cast(str | None, row["event_id"]),
                    account=str(row["account"]),
                    unit=str(row["unit"]),
                    amount=Decimal(str(row["amount_text"])),
                    entry=_json_object(str(row["payload_json"]), label="ledger entry"),
                    entry_hash=str(row["entry_hash"]),
                )

    def get_ledger_entries(self, run_id: str) -> tuple[LedgerRecord, ...]:
        normalized_run_id = _identifier(run_id, label="run_id")
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM paper_ledger_entries
                WHERE run_id=? ORDER BY transaction_id, entry_index
                """,
                (normalized_run_id,),
            ).fetchall()
        return tuple(
            LedgerRecord(
                run_id=str(row["run_id"]),
                transaction_id=str(row["transaction_id"]),
                entry_index=int(row["entry_index"]),
                entry_id=str(row["entry_id"]),
                event_id=cast(str | None, row["event_id"]),
                account=str(row["account"]),
                unit=str(row["unit"]),
                amount=Decimal(str(row["amount_text"])),
                entry=_json_object(str(row["payload_json"]), label="ledger entry"),
                entry_hash=str(row["entry_hash"]),
            )
            for row in rows
        )

    def contains_alert(self, run_id: str, alert_id: str) -> bool:
        """Return whether one exact durable alert exists via the primary key."""

        normalized_run_id = _identifier(run_id, label="run_id")
        normalized_alert_id = _identifier(alert_id, label="alert_id")
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT 1 FROM paper_alerts WHERE run_id=? AND alert_id=? LIMIT 1",
                (normalized_run_id, normalized_alert_id),
            ).fetchone()
        return row is not None
    def get_latest_alert(
        self,
        run_id: str,
        *,
        severity: str | None = None,
    ) -> AlertRecord | None:
        """Return the newest exact alert through a SQL LIMIT 1 query."""

        normalized_run_id = _identifier(run_id, label="run_id")
        if severity is None:
            sql = """
                SELECT * FROM paper_alerts
                WHERE run_id=?
                ORDER BY event_sequence DESC, created_at DESC, alert_id DESC
                LIMIT 1
            """
            parameters: tuple[object, ...] = (normalized_run_id,)
        else:
            normalized_severity = _identifier(severity, label="severity")
            sql = """
                SELECT * FROM paper_alerts
                WHERE run_id=? AND severity=?
                ORDER BY event_sequence DESC, created_at DESC, alert_id DESC
                LIMIT 1
            """
            parameters = (normalized_run_id, normalized_severity)
        with self._read_connection() as connection:
            row = connection.execute(sql, parameters).fetchone()
        if row is None:
            return None
        return AlertRecord(
            run_id=str(row["run_id"]),
            alert_id=str(row["alert_id"]),
            commit_sequence=(
                int(row["commit_sequence"])
                if row["commit_sequence"] is not None
                else None
            ),
            event_sequence=int(row["event_sequence"]),
            severity=str(row["severity"]),
            code=str(row["code"]),
            alert=_json_object(str(row["payload_json"]), label="alert"),
            payload_hash=str(row["payload_hash"]),
            created_at=str(row["created_at"]),
        )


    def get_alerts(self, run_id: str) -> tuple[AlertRecord, ...]:
        normalized_run_id = _identifier(run_id, label="run_id")
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM paper_alerts
                WHERE run_id=? ORDER BY event_sequence, created_at, alert_id
                """,
                (normalized_run_id,),
            ).fetchall()
        return tuple(
            AlertRecord(
                run_id=str(row["run_id"]),
                alert_id=str(row["alert_id"]),
                commit_sequence=(int(row["commit_sequence"]) if row["commit_sequence"] is not None else None),
                event_sequence=int(row["event_sequence"]),
                severity=str(row["severity"]),
                code=str(row["code"]),
                alert=_json_object(str(row["payload_json"]), label="alert"),
                payload_hash=str(row["payload_hash"]),
                created_at=str(row["created_at"]),
            )
            for row in rows
        )

    def get_recent_alerts(
        self,
        run_id: str,
        *,
        limit: int,
    ) -> tuple[AlertRecord, ...]:
        """Return a bounded recent alert page in chronological order."""

        normalized_run_id = _identifier(run_id, label="run_id")
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM paper_alerts
                WHERE run_id=?
                ORDER BY event_sequence DESC, created_at DESC, alert_id DESC
                LIMIT ?
                """,
                (normalized_run_id, limit),
            ).fetchall()
        alerts = tuple(
            AlertRecord(
                run_id=str(row["run_id"]),
                alert_id=str(row["alert_id"]),
                commit_sequence=(int(row["commit_sequence"]) if row["commit_sequence"] is not None else None),
                event_sequence=int(row["event_sequence"]),
                severity=str(row["severity"]),
                code=str(row["code"]),
                alert=_json_object(str(row["payload_json"]), label="alert"),
                payload_hash=str(row["payload_hash"]),
                created_at=str(row["created_at"]),
            )
            for row in rows
        )
        return tuple(reversed(alerts))

    def contains_input(self, run_id: str, input_id: str) -> bool:
        return self.get_input(run_id, input_id) is not None

    @staticmethod
    def _input_from_row(row: sqlite3.Row) -> InputRecord:
        return InputRecord(
            run_id=str(row["run_id"]),
            input_id=str(row["input_id"]),
            payload=_json_object(str(row["payload_json"]), label="input payload"),
            payload_hash=str(row["payload_hash"]),
            first_event_sequence=(
                int(row["first_event_sequence"]) if row["first_event_sequence"] is not None else None
            ),
            last_event_sequence=int(row["last_event_sequence"]),
            commit_sequence=int(row["commit_sequence"]),
            commit_hash=str(row["commit_hash"]),
            created_at=str(row["created_at"]),
        )

    def get_input(self, run_id: str, input_id: str) -> InputRecord | None:
        """Return one inbox row through its ``(run_id, input_id)`` primary key."""

        normalized_run_id = _identifier(run_id, label="run_id")
        normalized_input_id = _identifier(input_id, label="input_id")
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM paper_inbox WHERE run_id=? AND input_id=?",
                (normalized_run_id, normalized_input_id),
            ).fetchone()
        return self._input_from_row(row) if row is not None else None

    def get_input_payloads(
        self,
        run_id: str,
        *,
        input_ids: Sequence[str],
    ) -> dict[str, dict[str, JsonValue]]:
        """Fetch a bounded set of canonical inbox payloads in one read query."""

        normalized_run_id = _identifier(run_id, label="run_id")
        normalized_ids = tuple(
            dict.fromkeys(_identifier(input_id, label="input_id") for input_id in input_ids)
        )
        if not normalized_ids:
            return {}
        if len(normalized_ids) > 1_000:
            raise ValueError("at most 1000 input_ids may be queried at once")
        placeholders = ",".join("?" for _ in normalized_ids)
        sql = (
            f"SELECT input_id, payload_json FROM paper_inbox WHERE run_id=? AND input_id IN ({placeholders})"
        )
        with self._read_connection() as connection:
            rows = connection.execute(sql, [normalized_run_id, *normalized_ids]).fetchall()
        return {
            str(row["input_id"]): _json_object(
                str(row["payload_json"]),
                label="input payload",
            )
            for row in rows
        }

    def get_public_market_source_summary(
        self,
        run_id: str,
        *,
        latest_instrument_limit: int,
    ) -> dict[str, JsonValue]:
        """Aggregate public-market health in SQLite with bounded output."""

        normalized_run_id = _identifier(run_id, label="run_id")
        if latest_instrument_limit <= 0:
            raise ValueError("latest_instrument_limit must be positive")
        with self._read_connection() as connection:
            aggregate = connection.execute(
                """
                SELECT
                    count(*) AS event_count,
                    count(DISTINCT json_extract(payload_json, '$.market.instrument'))
                        AS instrument_count,
                    sum(CASE WHEN json_extract(payload_json, '$.market.gap') = 1
                        THEN 1 ELSE 0 END) AS gap_count,
                    sum(CASE WHEN json_extract(payload_json, '$.market.stale') = 1
                        THEN 1 ELSE 0 END) AS stale_count,
                    sum(CASE WHEN json_extract(payload_json, '$.market.tradable') = 0
                        THEN 1 ELSE 0 END) AS nontradable_count,
                    count(DISTINCT json_extract(
                        payload_json, '$.market.source_connection_epoch'
                    )) AS connection_epoch_count,
                    count(DISTINCT json_extract(
                        payload_json, '$.market.source_connection_id'
                    )) AS connection_id_count,
                    max(json_extract(payload_json, '$.market.received_at'))
                        AS last_received_at
                FROM paper_inbox
                WHERE run_id=?
                  AND json_extract(payload_json, '$.input_type')='PUBLIC_MARKET_EVENT'
                """,
                (normalized_run_id,),
            ).fetchone()
            latest_rows = connection.execute(
                """
                WITH markets AS (
                    SELECT
                        json_extract(payload_json, '$.market.instrument') AS instrument,
                        payload_json,
                        commit_sequence,
                        row_number() OVER (
                            PARTITION BY json_extract(
                                payload_json, '$.market.instrument'
                            )
                            ORDER BY commit_sequence DESC, input_id DESC
                        ) AS instrument_rank
                    FROM paper_inbox
                    WHERE run_id=?
                      AND json_extract(payload_json, '$.input_type')='PUBLIC_MARKET_EVENT'
                )
                SELECT instrument, payload_json FROM markets
                WHERE instrument_rank=1 AND instrument IS NOT NULL
                ORDER BY instrument
                LIMIT ?
                """,
                (normalized_run_id, latest_instrument_limit),
            ).fetchall()
            kind_rows = connection.execute(
                """
                SELECT
                    json_extract(payload_json, '$.market.source_event_kind') AS event_kind,
                    count(*) AS event_count
                FROM paper_inbox
                WHERE run_id=?
                  AND json_extract(payload_json, '$.input_type')='PUBLIC_MARKET_EVENT'
                  AND json_type(
                      payload_json, '$.market.source_event_kind'
                  )='text'
                GROUP BY event_kind
                ORDER BY event_kind
                LIMIT 32
                """,
                (normalized_run_id,),
            ).fetchall()

        event_count = int(aggregate["event_count"] or 0)
        instrument_count = int(aggregate["instrument_count"] or 0)
        epoch_count = int(aggregate["connection_epoch_count"] or 0)
        connection_id_count = int(aggregate["connection_id_count"] or 0)
        latest_by_instrument: dict[str, JsonValue] = {}
        for row in latest_rows:
            payload = _json_object(str(row["payload_json"]), label="input payload")
            market = payload.get("market")
            if isinstance(market, dict):
                latest_by_instrument[str(row["instrument"])] = market
        kind_counts: dict[str, JsonValue] = {
            str(row["event_kind"]): int(row["event_count"]) for row in kind_rows
        }
        return {
            "connection_epoch_count": epoch_count,
            "connection_id_count": connection_id_count,
            "event_count": event_count,
            "gap_count": int(aggregate["gap_count"] or 0),
            "instrument_count": instrument_count,
            "last_received_at": (
                str(aggregate["last_received_at"]) if aggregate["last_received_at"] is not None else None
            ),
            "latest_by_instrument": latest_by_instrument,
            "latest_instruments_truncated": instrument_count > len(latest_by_instrument),
            "nontradable_count": int(aggregate["nontradable_count"] or 0),
            "reconnect_count": max(0, epoch_count - 1, connection_id_count - 1),
            "source_event_kind_counts": kind_counts,
            "stale_count": int(aggregate["stale_count"] or 0),
        }

    def iter_inputs(
        self,
        run_id: str,
        *,
        input_type: str | None = None,
        after_commit_sequence: int = 0,
    ) -> Iterable[InputRecord]:
        """Stream the canonical inbox in commit order with bounded memory."""

        normalized_run_id = _identifier(run_id, label="run_id")
        normalized_type = _identifier(input_type, label="input_type") if input_type is not None else None
        if (
            isinstance(after_commit_sequence, bool)
            or not isinstance(after_commit_sequence, int)
            or after_commit_sequence < 0
        ):
            raise ValueError("after_commit_sequence must be a non-negative integer")
        sql = "SELECT * FROM paper_inbox WHERE run_id=? AND commit_sequence>?"
        parameters: list[object] = [normalized_run_id, after_commit_sequence]
        if normalized_type is not None:
            sql += " AND json_extract(payload_json, '$.input_type')=?"
            parameters.append(normalized_type)
        sql += " ORDER BY commit_sequence, input_id"
        with self._read_connection() as connection:
            cursor = connection.execute(sql, parameters)
            for row in cursor:
                yield self._input_from_row(row)

    def get_inputs(self, run_id: str) -> tuple[InputRecord, ...]:
        """Return the canonical inbox in durable commit order for exact replay."""

        normalized_run_id = _identifier(run_id, label="run_id")
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM paper_inbox
                WHERE run_id=? ORDER BY commit_sequence, input_id
                """,
                (normalized_run_id,),
            ).fetchall()
        return tuple(self._input_from_row(row) for row in rows)

    def read_snapshot(self, run_id: str) -> dict[str, JsonValue]:
        """Return a self-contained read-only dashboard view."""

        run = self.get_run(run_id)
        projection = self.get_projection(run_id)
        alerts = self.get_alerts(run_id)
        return {
            "alerts": [alert.alert for alert in alerts],
            "commit_head_hash": run.commit_head_hash,
            "commit_sequence": run.commit_sequence,
            "config_hash": run.config_hash,
            "event_head_hash": run.event_head_hash,
            "event_sequence": run.event_sequence,
            "mode": "PAPER_ONLY",
            "projection": projection.to_dict(),
            "run_id": run.run_id,
            "status": run.status,
        }


__all__ = [
    "FAULT_STAGES",
    "PAPER_STATES",
    "SCHEMA_VERSION",
    "STORE_SCHEMA_VERSION",
    "AlertRecord",
    "AppendConflictError",
    "AppendResult",
    "ConcurrentWriteError",
    "FaultInjector",
    "IdempotencyConflictError",
    "InputRecord",
    "IntegrityError",
    "IntegrityIssue",
    "IntegrityReport",
    "LedgerImbalanceError",
    "LedgerRecord",
    "PaperStore",
    "PaperStoreError",
    "ProjectionHistoryRecord",
    "RunConflictError",
    "RunNotFoundError",
    "RunRecord",
    "SchemaVersionError",
    "StoredEventRecord",
]
