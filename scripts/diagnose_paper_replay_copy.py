from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import math
import os
import queue
import re
import secrets
import shutil
import signal
import sqlite3
import statistics
import subprocess
import sys
import threading
from collections import Counter, defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, nullcontext, suppress
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter, perf_counter_ns, process_time, thread_time, thread_time_ns
from types import FrameType
from typing import Any, TypedDict, TypeGuard, cast

import hyperlab.paper.engine as engine_module
import hyperlab.paper.runtime as runtime_module
import hyperlab.paper.store as store_module
from hyperlab.paper import AppendResult, PaperEngine, PaperProjection, PaperStore
from hyperlab.paper.runtime import replay_paper_run

_MAX_WALL_SECONDS = 840.0
_SOURCE_OPEN_MODE = "sqlite-mode=ro;immutable=1;query_only=ON"
_WORKER_TOKEN_ENV = "HYPERLAB_REPLAY_DIAGNOSTIC_WORKER_TOKEN"
_WORKER_PROTOCOL_TOKEN: str | None = None
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_EXPECTED_PHASES = (
    "source_integrity",
    "event_replay",
    "canonical_input_replay_total",
    "replay_store_generation",
    "target_integrity",
    "target_ledger_reconciliation",
    "final_exact_comparisons",
)
_WORKER_RESULT_FIELDS = frozenset(
    {
        "authorizes_real_money",
        "bounded_historical_prefix_certification_count",
        "config_hash",
        "event",
        "event_count",
        "event_head_hash",
        "historical_ledger_reconciliation_count",
        "mode",
        "orders_enabled",
        "peak_rss_bytes",
        "peak_rss_source",
        "profile",
        "projection_hash",
        "projection_history_decode_count",
        "replay_cpu_seconds",
        "replay_wall_seconds",
        "run_id",
        "source_head_identity",
        "source_open_mode",
        "source_projection_hash",
        "source_query_only_verified",
        "source_sqlite_connection_count",
        "source_write_connection_attempts",
        "status",
        "target_database_bytes",
        "target_initial_identity",
        "target_head_identity",
        "target_logical_transaction_counts",
        "target_projection_hash",
        "target_paper_store_sqlite_connection_count",
        "target_sqlite_connection_count",
    }
)
_PROFILE_FIELDS = frozenset(
    {
        "counters",
        "instrumentation_mode",
        "logical_row_counts_note",
        "phase_counters",
        "phase_timings",
        "replay_progress",
        "sqlite_sql_text_tracing",
    }
)
_PHASE_TIMING_FIELDS = frozenset(
    {
        "counters",
        "cpu_seconds",
        "peak_rss_bytes",
        "peak_rss_source",
        "status",
        "wall_seconds",
    }
)
_PEAK_RSS_SOURCES = frozenset(
    {
        "peak-rss-unavailable",
        "resource-getrusage",
        "windows-GetProcessMemoryInfo",
        "windows-GetProcessMemoryInfo-unavailable",
    }
)
_WORKER_FAILURE_STATUSES = frozenset(
    {
        "DIAGNOSTIC_WORKER_FAILED",
        "REFUSED_COPY_HAS_SQLITE_SIDECAR",
        "REFUSED_COPY_MATCHES_FORBIDDEN_ORIGINAL",
        "REFUSED_COPY_NOT_FILE",
        "REFUSED_COPY_SYMLINK",
        "REFUSED_EXPECTED_SHA256",
        "REFUSED_FINGERPRINT_DEADLINE",
        "REFUSED_ORIGINAL_IDENTITY_CHECK",
        "REFUSED_ORIGINAL_NOT_FILE",
        "REFUSED_PATH_RESOLUTION",
        "REFUSED_RUN_ID",
        "REFUSED_SCRATCH_HEADROOM",
        "REFUSED_SCRATCH_NOT_DIRECTORY",
        "REFUSED_SCRATCH_SYMLINK",
        "REFUSED_SOURCE_CHANGED_DURING_FINGERPRINT",
        "REFUSED_SOURCE_JOURNAL_MODE",
        "REFUSED_SOURCE_READ_GUARD",
        "REFUSED_SOURCE_SQLITE_HEADER",
        "REFUSED_WORKER_SOURCE_HASH",
        "SOURCE_COPY_CHANGED",
        "SOURCE_COPY_SQLITE_SIDECAR_APPEARED",
    }
)
_MAX_WORKER_LINE_CHARACTERS = 1_000_000
_MAX_QUEUED_WORKER_LINES = 64
_MAX_OVERHEAD_CHILD_LINES = 4_096
_MAX_OVERHEAD_CHILD_CHARACTERS = 8_000_000
_WORKER_LINE_TOO_LONG = "\0worker-line-too-long"
_WORKER_OUTPUT_QUEUE_FULL = "\0worker-output-queue-full"
_SAFE_COUNTER_NAME = re.compile(r"^[a-z0-9_.-]+$")
_SAFE_SQLITE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_REPLAY_INPUT_TYPES = frozenset(
    {
        "CANCEL_REQUEST",
        "OBSERVATION_COVERAGE",
        "OPERATOR_PAUSE",
        "OTHER",
        "PAPER_KILL",
        "PAPER_RUNTIME_FAILURE",
        "PUBLIC_FUNDING_SETTLEMENT",
        "PUBLIC_MARKET_EVENT",
        "PUBLIC_SOURCE_FAILURE",
        "RECONCILE",
        "RESILIENCE_EXERCISE",
        "RESUME_AFTER_REVIEW",
        "RUN_START",
        "RUNTIME_SESSION_STARTED",
        "RUNTIME_SESSION_STOPPED",
        "STRATEGY_DECISION",
        "STRATEGY_LOCAL_FAILURE",
        "STRESS_RESULT",
        "TIMER",
    }
)
_REPLAY_TIMING_VERSION = 2
_INSTRUMENTATION_MODES = ("OFF", "V2", "V3")
_REPLAY_INSTRUMENTATION_MODES = frozenset(_INSTRUMENTATION_MODES)
_REPLAY_TIMING_V3_VERSION = 3
_REPLAY_V3_EQP_LIMIT = 8
_REPLAY_V3_HISTOGRAM_BUCKETS = 64
_REPLAY_V3_INDEX_LIMIT = 16
_REPLAY_V3_QUERY_FINGERPRINT_LIMIT = 8
_REPLAY_V3_SCOPE_ACCOUNTING_TOLERANCE_NS = 0
# Fail closed unless at least 99% of wall time is attributed. This is a
# diagnostic completeness policy, not a production performance threshold.
_REPLAY_V3_UNATTRIBUTED_WALL_FRACTION_TOLERANCE = 0.01
_REPLAY_V3_EXCLUSIVE_SEMANTICS = "operation spans exclude nested profiled operations; span_count counts exclusive fragments"
_REPLAY_TAIL_LIMIT = 16
_REPLAY_SLOWEST_LIMIT = 8
_REPLAY_OPERATIONS = frozenset(
    {
        "append_events_insert",
        "append_input_canonicalization",
        "append_ledger_insert",
        "append_prepare_alerts",
        "append_prepare_events",
        "append_prepare_ledger",
        "append_prepare_through_inbox",
        "append_projection_and_commit_rows",
        "append_projection_canonicalization",
        "append_projection_history_storage",
        "append_record_canonicalization",
        "append_replay_validation",
        "append_result_build",
        "append_sqlite_commit",
        "create_run",
        "engine_commit_prepare",
        "historical_filtered_input_lookup",
        "historical_full_integrity_verification",
        "historical_head_integrity",
        "historical_prefix_certification",
        "historical_projection_before_lookup",
        "historical_reconcile",
        "input_dispatch",
        "replay_store_post_inputs",
        "replay_store_setup",
        "source_input_fetch",
    }
)
_REPLAY_V3_INPUT_TYPES = _REPLAY_INPUT_TYPES | {"UNKNOWN"}
_REPLAY_V3_OPERATIONS = _REPLAY_OPERATIONS | {
    "UNKNOWN",
    "funding_json_decode",
    "funding_lookup_residual",
    "funding_query_prepare",
    "funding_reconstruct_canonicalize",
    "funding_sqlite_execute",
    "funding_sqlite_fetch",
    "input_business_logic",
    "input_commit_prepare",
    "input_commit_return",
    "input_dispatch_prepare",
    "diagnostic_post_commit_accounting",
    "input_result_return",
    "validation_alert_comparison",
    "validation_alert_expected_canonicalization",
    "validation_alert_supplied_canonicalization",
    "validation_apply_events",
    "validation_expected_ledger",
    "validation_failure_diagnostics",
    "validation_ledger_comparison",
    "validation_ledger_expected_canonicalization",
    "validation_ledger_supplied_canonicalization",
    "validation_projection_canonicalization",
    "validation_projection_comparison",
    "validation_projection_decode",
    "validation_projection_query",
    "validation_projection_reconstruction",
    "validation_residual",
}
_REPLAY_V3_PHASE_OPERATIONS = frozenset(
    {
        "UNKNOWN",
        "replay_store_post_inputs",
        "replay_store_setup",
        "source_input_fetch",
    }
)
_REPLAY_V3_REQUIRED_PHASE_OPERATIONS = (
    _REPLAY_V3_PHASE_OPERATIONS - {"UNKNOWN"}
)
_REPLAY_V3_REQUIRED_ALL_INPUT_OPERATIONS = frozenset(
    {
        "input_business_logic",
        "input_dispatch_prepare",
    }
)
_REPLAY_V3_REQUIRED_COMMITTED_INPUT_OPERATIONS = frozenset(
    {
        "append_input_canonicalization",
        "append_prepare_alerts",
        "append_prepare_events",
        "append_prepare_ledger",
        "append_projection_canonicalization",
        "append_projection_history_storage",
        "diagnostic_post_commit_accounting",
        "input_commit_prepare",
        "input_commit_return",
        "input_result_return",
        "validation_alert_comparison",
        "validation_alert_expected_canonicalization",
        "validation_alert_supplied_canonicalization",
        "validation_apply_events",
        "validation_expected_ledger",
        "validation_ledger_comparison",
        "validation_ledger_expected_canonicalization",
        "validation_ledger_supplied_canonicalization",
        "validation_projection_canonicalization",
        "validation_projection_comparison",
        "validation_projection_decode",
        "validation_projection_query",
        "validation_projection_reconstruction",
        "validation_residual",
    }
)
_REPLAY_V3_REQUIRED_ALL_INPUT_SCOPES = frozenset(
    {"business_reducer", "input_dispatch", "replay_input"}
)
_REPLAY_V3_REQUIRED_COMMITTED_INPUT_SCOPES = frozenset(
    {"engine_commit", "replay_validation", "store_append"}
)
_REPLAY_V3_MATRIX_LIMIT = len(_REPLAY_V3_INPUT_TYPES) * len(_REPLAY_V3_OPERATIONS)
_REPLAY_V3_SCOPE_PARENTS: dict[str, str | None] = {
    "UNKNOWN": None,
    "business_reducer": "input_dispatch",
    "engine_commit": "business_reducer",
    "funding_lookup": "business_reducer",
    "input_dispatch": "replay_input",
    "replay_input": None,
    "replay_validation": "store_append",
    "store_append": "engine_commit",
}
_APPEND_STAGE_OPERATIONS = {
    "before_begin": "append_prepare_through_inbox",
    "after_input": "append_events_insert",
    "after_events": "append_ledger_insert",
    "after_ledger": "append_projection_and_commit_rows",
    "before_commit": "append_sqlite_commit",
    "after_commit": "append_result_build",
}
_ROW_HOOKS = frozenset(
    {
        "alert_row",
        "commit_row",
        "event_row",
        "inbox_row",
        "ledger_entry_row",
        "ledger_transaction_row",
        "projection_history_row",
    }
)


class DiagnosticRefusal(ValueError):
    def __init__(self, status: str, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


@dataclass(frozen=True, slots=True)
class SourceStat:
    size: int
    mtime_ns: int
    mode: int
    device: int
    inode: int

    @classmethod
    def read(cls, path: Path) -> SourceStat:
        value = path.stat()
        return cls(
            size=int(value.st_size),
            mtime_ns=int(value.st_mtime_ns),
            mode=int(value.st_mode),
            device=int(value.st_dev),
            inode=int(value.st_ino),
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "device": self.device,
            "inode": self.inode,
            "mode": self.mode,
            "mtime_ns": self.mtime_ns,
            "size": self.size,
        }


@dataclass(frozen=True, slots=True)
class Fingerprint:
    sha256: str
    stat: SourceStat
    elapsed_seconds: float


def _emit(record: Mapping[str, object]) -> None:
    emitted = dict(record)
    if _WORKER_PROTOCOL_TOKEN is not None:
        emitted["_worker_protocol_token"] = _WORKER_PROTOCOL_TOKEN
    print(json.dumps(emitted, ensure_ascii=False, sort_keys=True), flush=True)


def _sha256(path: Path, *, deadline: float | None = None) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            if deadline is not None and perf_counter() >= deadline:
                raise DiagnosticRefusal(
                    "REFUSED_FINGERPRINT_DEADLINE",
                    "source fingerprint exceeded the diagnostic wall budget",
                )
            digest.update(chunk)
    return digest.hexdigest()


def _fingerprint(path: Path, *, deadline: float | None = None) -> Fingerprint:
    before = SourceStat.read(path)
    started = perf_counter()
    digest = _sha256(path, deadline=deadline)
    elapsed = perf_counter() - started
    after = SourceStat.read(path)
    if after != before:
        raise DiagnosticRefusal(
            "REFUSED_SOURCE_CHANGED_DURING_FINGERPRINT",
            "the explicit database copy changed while it was fingerprinted",
        )
    return Fingerprint(digest, after, elapsed)


def _sqlite_sidecars(path: Path) -> tuple[Path, ...]:
    return tuple(
        candidate
        for suffix in ("-journal", "-shm", "-wal")
        if (candidate := Path(f"{path}{suffix}")).exists()
    )


def _sqlite_header_modes(path: Path) -> tuple[int, int]:
    with path.open("rb") as handle:
        header = handle.read(20)
    if len(header) != 20 or header[:16] != b"SQLite format 3\x00":
        raise DiagnosticRefusal(
            "REFUSED_SOURCE_SQLITE_HEADER",
            "the explicit source copy has no complete SQLite format-3 header",
        )
    return header[18], header[19]


@contextmanager
def _hold_source_snapshot(path: Path) -> Iterator[str]:
    connection: sqlite3.Connection | None = None
    try:
        if _sqlite_header_modes(path) != (1, 1):
            raise DiagnosticRefusal(
                "REFUSED_SOURCE_JOURNAL_MODE",
                "the explicit copy must use canonical SQLite DELETE journal mode",
            )
        if _sqlite_sidecars(path):
            raise DiagnosticRefusal(
                "REFUSED_COPY_HAS_SQLITE_SIDECAR",
                "the immutable SQLite copy must have no journal, shm, or wal sidecar",
            )
        connection = sqlite3.connect(
            f"{path.resolve().as_uri()}?mode=ro",
            uri=True,
            timeout=1.0,
            isolation_level=None,
        )
        connection.execute("PRAGMA query_only=ON")
        journal_mode = "delete"
        connection.execute("BEGIN")
        connection.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
        if _sqlite_sidecars(path):
            raise DiagnosticRefusal(
                "SOURCE_COPY_SQLITE_SIDECAR_APPEARED",
                "a SQLite sidecar appeared while acquiring the source read guard",
            )
        yield journal_mode
    except DiagnosticRefusal:
        raise
    except sqlite3.Error as error:
        raise DiagnosticRefusal(
            "REFUSED_SOURCE_READ_GUARD",
            f"SQLite {type(error).__name__} while acquiring the read guard",
        ) from error
    finally:
        if connection is not None:
            if connection.in_transaction:
                connection.rollback()
            connection.close()


def _peak_rss_bytes() -> tuple[int | None, str]:
    if os.name == "nt":

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", ctypes.c_ulong),
                ("PageFaultCount", ctypes.c_ulong),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        current_process = ctypes.windll.kernel32.GetCurrentProcess
        current_process.restype = ctypes.c_void_p
        memory_info = ctypes.windll.psapi.GetProcessMemoryInfo
        memory_info.argtypes = (
            ctypes.c_void_p,
            ctypes.POINTER(ProcessMemoryCounters),
            ctypes.c_ulong,
        )
        memory_info.restype = ctypes.c_int
        if memory_info(current_process(), ctypes.byref(counters), counters.cb):
            return int(counters.PeakWorkingSetSize), "windows-GetProcessMemoryInfo"
        return None, "windows-GetProcessMemoryInfo-unavailable"
    try:
        import resource
    except ImportError:
        return None, "peak-rss-unavailable"
    resource_api = cast(Any, resource)
    raw = int(resource_api.getrusage(resource_api.RUSAGE_SELF).ru_maxrss)
    return raw * (1 if sys.platform == "darwin" else 1024), "resource-getrusage"


def _resolve_inputs(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    if not _HEX_64.fullmatch(args.run_id):
        raise DiagnosticRefusal("REFUSED_RUN_ID", "run_id must be 64 lowercase hexadecimal characters")
    if not _HEX_64.fullmatch(args.expected_sha256):
        raise DiagnosticRefusal(
            "REFUSED_EXPECTED_SHA256",
            "expected SHA-256 must be 64 lowercase hexadecimal characters",
        )
    if args.database_copy.is_symlink():
        raise DiagnosticRefusal("REFUSED_COPY_SYMLINK", "the explicit database copy must not be a symlink")
    if args.scratch_root.is_symlink():
        raise DiagnosticRefusal("REFUSED_SCRATCH_SYMLINK", "the scratch root must not be a symlink")
    try:
        database_copy = args.database_copy.resolve(strict=True)
        forbidden_original = args.forbid_original.resolve(strict=True)
        scratch_root = args.scratch_root.resolve(strict=True)
    except OSError as error:
        raise DiagnosticRefusal(
            "REFUSED_PATH_RESOLUTION",
            f"path resolution failed with {type(error).__name__}",
        ) from error
    if not database_copy.is_file():
        raise DiagnosticRefusal("REFUSED_COPY_NOT_FILE", "the explicit database copy is not a file")
    if not forbidden_original.is_file():
        raise DiagnosticRefusal("REFUSED_ORIGINAL_NOT_FILE", "the forbidden original path is not a file")
    if not scratch_root.is_dir():
        raise DiagnosticRefusal("REFUSED_SCRATCH_NOT_DIRECTORY", "the scratch root is not a directory")
    same_file = os.path.normcase(str(database_copy)) == os.path.normcase(str(forbidden_original))
    if not same_file:
        try:
            same_file = os.path.samefile(database_copy, forbidden_original)
        except OSError as error:
            raise DiagnosticRefusal(
                "REFUSED_ORIGINAL_IDENTITY_CHECK",
                f"original identity check failed with {type(error).__name__}",
            ) from error
    copy_stat = SourceStat.read(database_copy)
    original_stat = SourceStat.read(forbidden_original)
    same_inode = copy_stat.inode != 0 and (
        copy_stat.device,
        copy_stat.inode,
    ) == (original_stat.device, original_stat.inode)
    if same_file or same_inode:
        raise DiagnosticRefusal(
            "REFUSED_COPY_MATCHES_FORBIDDEN_ORIGINAL",
            "the diagnostic requires a distinct explicit SQLite copy",
        )
    if _sqlite_header_modes(database_copy) != (1, 1):
        raise DiagnosticRefusal(
            "REFUSED_SOURCE_JOURNAL_MODE",
            "the explicit copy must use canonical SQLite DELETE journal mode",
        )
    if _sqlite_sidecars(database_copy):
        raise DiagnosticRefusal(
            "REFUSED_COPY_HAS_SQLITE_SIDECAR",
            "the immutable SQLite copy must have no journal, shm, or wal sidecar",
        )
    if shutil.disk_usage(scratch_root).free <= copy_stat.size:
        raise DiagnosticRefusal(
            "REFUSED_SCRATCH_HEADROOM",
            "scratch free space must exceed the source-copy size",
        )
    return database_copy, forbidden_original, scratch_root


def _sanitized_query_plan_detail(detail: str) -> dict[str, object]:
    normalized = detail.upper()
    if normalized.startswith("SEARCH "):
        access = "SEARCH"
    elif normalized.startswith("SCAN "):
        access = "SCAN"
    else:
        access = "OTHER"
    index_match = re.search(
        r"\bUSING (?:COVERING )?INDEX ([A-Za-z_][A-Za-z0-9_]*)",
        detail,
        flags=re.IGNORECASE,
    )
    index_name = index_match.group(1) if index_match is not None else None
    if (
        index_name is not None
        and _SAFE_SQLITE_IDENTIFIER.fullmatch(index_name) is None
    ):
        raise RuntimeError("SQLite query plan exposed an unsafe index identifier")
    return {
        "access": access,
        "covering_index": "USING COVERING INDEX" in normalized,
        "index_name": index_name,
        "uses_temp_btree": "USE TEMP B-TREE" in normalized,
    }


def _capture_sanitized_funding_eqp(
    path: Path,
    *,
    run_id: str,
) -> dict[str, object]:
    connection = sqlite3.connect(
        f"{path.resolve(strict=True).as_uri()}?mode=ro",
        uri=True,
        timeout=1.0,
        isolation_level=None,
    )
    try:
        connection.execute("PRAGMA query_only=ON")
        query_only_row = connection.execute("PRAGMA query_only").fetchone()
        query_only_verified = (
            query_only_row is not None and int(query_only_row[0]) == 1
        )
        if not query_only_verified:
            raise RuntimeError("funding EQP connection is not query_only")
        changes_before = connection.total_changes
        transaction_before = connection.in_transaction
        column_rows = connection.execute(
            "PRAGMA table_info('paper_inbox')"
        ).fetchall()
        if not 1 <= len(column_rows) <= 32:
            raise RuntimeError("paper_inbox column inventory is outside the V3 bound")
        columns = [str(row[1]) for row in column_rows]
        if (
            len(set(columns)) != len(columns)
            or any(
                _SAFE_SQLITE_IDENTIFIER.fullmatch(column) is None
                for column in columns
            )
            or not {
                "commit_sequence",
                "input_id",
                "payload_json",
                "run_id",
            }.issubset(columns)
        ):
            raise RuntimeError("paper_inbox column inventory is invalid")

        raw_indexes = connection.execute(
            "PRAGMA index_list('paper_inbox')"
        ).fetchall()
        if len(raw_indexes) > _REPLAY_V3_INDEX_LIMIT:
            raise RuntimeError("paper_inbox index inventory exceeds the V3 bound")
        existing_indexes: list[dict[str, object]] = []
        for row in raw_indexes:
            name = str(row[1])
            origin = str(row[3])
            if (
                _SAFE_SQLITE_IDENTIFIER.fullmatch(name) is None
                or origin not in {"c", "pk", "u"}
            ):
                raise RuntimeError("paper_inbox index inventory is invalid")
            existing_indexes.append(
                {
                    "name": name,
                    "origin": origin,
                    "partial": bool(row[4]),
                    "unique": bool(row[2]),
                }
            )
        existing_indexes.sort(key=lambda item: cast(str, item["name"]))

        plan_rows = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT * FROM paper_inbox
            WHERE run_id=?
              AND commit_sequence>?
              AND json_extract(payload_json, '$.input_type')=?
            ORDER BY commit_sequence, input_id
            """,
            (run_id, 0, "RECONCILE"),
        ).fetchall()
        if not 1 <= len(plan_rows) <= _REPLAY_V3_EQP_LIMIT:
            raise RuntimeError("funding EQP row count is outside the V3 bound")
        eqp: list[dict[str, object]] = []
        for row in plan_rows:
            sanitized = _sanitized_query_plan_detail(str(row[3]))
            eqp.append(
                {
                    **sanitized,
                    "parent_id": int(row[1]),
                    "select_id": int(row[0]),
                }
            )
        changes_after = connection.total_changes
        if (
            changes_after != changes_before
            or connection.in_transaction != transaction_before
        ):
            raise RuntimeError("funding EQP read-only invariants changed")
        selected_indexes = [
            cast(str, item["index_name"])
            for item in eqp
            if isinstance(item.get("index_name"), str)
        ]
        return {
            "eqp": eqp,
            "eqp_capture_query_only_verified": query_only_verified,
            "eqp_capture_total_changes": changes_after - changes_before,
            "existing_indexes": existing_indexes,
            "fallback_scan_detected": any(
                item["access"] == "SCAN" for item in eqp
            ),
            "query_shape": {
                "columns": sorted(columns),
                "order_by": ["commit_sequence", "input_id"],
                "predicates": [
                    "run_id_equal",
                    "commit_sequence_greater_than",
                    "payload_input_type_equal",
                ],
                "table": "paper_inbox",
            },
            "selected_index_name": (
                selected_indexes[0] if selected_indexes else None
            ),
        }
    finally:
        if connection.in_transaction:
            connection.rollback()
        connection.close()


_MAX_FRESH_STORE_SCHEMA_OBJECTS = 128
_MAX_FRESH_STORE_SCHEMA_SQL_CHARACTERS = 1_000_000


def _canonical_identity_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _fresh_historical_store_identity(
    store: PaperStore,
    connection: sqlite3.Connection,
) -> str:
    if not store.historical_replay_only:
        raise RuntimeError("initial identity requires a historical replay store")
    changes_before = connection.total_changes
    raw_schema = list(
        connection.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE type IN ('table', 'index', 'trigger') "
            "AND name NOT LIKE 'sqlite_%' ORDER BY type, name"
        )
    )
    if not 1 <= len(raw_schema) <= _MAX_FRESH_STORE_SCHEMA_OBJECTS:
        raise RuntimeError("fresh replay store schema exceeds its identity bound")
    schema: list[dict[str, object]] = []
    table_names: list[str] = []
    schema_sql_characters = 0
    for raw_type, raw_name, raw_table, raw_sql in raw_schema:
        object_type = str(raw_type)
        name = str(raw_name)
        table = str(raw_table)
        sql = None if raw_sql is None else str(raw_sql)
        if (
            object_type not in {"index", "table", "trigger"}
            or _SAFE_SQLITE_IDENTIFIER.fullmatch(name) is None
            or _SAFE_SQLITE_IDENTIFIER.fullmatch(table) is None
        ):
            raise RuntimeError("fresh replay store schema identity is invalid")
        if sql is not None:
            schema_sql_characters += len(sql)
        schema.append(
            {
                "name": name,
                "sql": sql,
                "table": table,
                "type": object_type,
            }
        )
        if object_type == "table":
            table_names.append(name)
    if schema_sql_characters > _MAX_FRESH_STORE_SCHEMA_SQL_CHARACTERS:
        raise RuntimeError("fresh replay store schema SQL exceeds its identity bound")
    if "paper_schema" not in table_names:
        raise RuntimeError("fresh replay store schema metadata is missing")

    row_counts: dict[str, int] = {}
    for name in sorted(table_names):
        count_row = connection.execute(
            f'SELECT COUNT(*) FROM "{name}"'
        ).fetchone()
        if count_row is None:
            raise RuntimeError("fresh replay store row count is unavailable")
        row_counts[name] = int(count_row[0])
    if row_counts.get("paper_schema") != 1 or any(
        count != 0
        for name, count in row_counts.items()
        if name != "paper_schema"
    ):
        raise RuntimeError("historical replay store is not logically fresh")
    metadata = list(
        connection.execute(
            "SELECT singleton, version FROM paper_schema ORDER BY singleton"
        )
    )
    if len(metadata) != 1:
        raise RuntimeError("fresh replay store metadata is invalid")
    pragmas = {
        "foreign_keys": int(connection.execute("PRAGMA foreign_keys").fetchone()[0]),
        "journal_mode": str(connection.execute("PRAGMA journal_mode").fetchone()[0]).upper(),
        "synchronous": int(connection.execute("PRAGMA synchronous").fetchone()[0]),
        "user_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
    }
    if connection.total_changes != changes_before:
        raise RuntimeError("fresh replay store identity capture changed the store")
    return _canonical_identity_sha256(
        {
            "paper_schema": {
                "singleton": int(metadata[0][0]),
                "version": int(metadata[0][1]),
            },
            "pragmas": pragmas,
            "row_counts": row_counts,
            "schema": schema,
        }
    )

_OVERHEAD_REPORT_VERSION = 1
_OVERHEAD_MIN_REPETITIONS = 3
_OVERHEAD_MAX_REPETITIONS = 9
_OVERHEAD_PROJECTION_COMMITS = 252_262


def _replay_overhead_schedule(repetitions: int) -> tuple[str, ...]:
    if (
        isinstance(repetitions, bool)
        or not isinstance(repetitions, int)
        or not _OVERHEAD_MIN_REPETITIONS
        <= repetitions
        <= _OVERHEAD_MAX_REPETITIONS
    ):
        raise ValueError(
            "overhead repetitions must be an integer between 3 and 9"
        )
    schedule: list[str] = []
    for repetition in range(repetitions):
        schedule.extend(
            ("OFF", "V2", "V3")
            if repetition % 2 == 0
            else ("V3", "V2", "OFF")
        )
    return tuple(schedule)


def _median_and_mad(values: Sequence[float]) -> dict[str, object]:
    if not values:
        raise ValueError("overhead statistic requires observations")
    median = float(statistics.median(values))
    return {
        "mad": float(
            statistics.median(abs(value - median) for value in values)
        ),
        "median": median,
        "samples": [float(value) for value in values],
    }


def _build_replay_overhead_report(
    observations: Sequence[Mapping[str, object]],
    *,
    repetitions: int,
    projection_commits: int = _OVERHEAD_PROJECTION_COMMITS,
) -> dict[str, object]:
    schedule = _replay_overhead_schedule(repetitions)
    if (
        isinstance(projection_commits, bool)
        or not isinstance(projection_commits, int)
        or projection_commits <= 0
    ):
        raise ValueError("overhead projection commits must be positive")
    if len(observations) != len(schedule):
        raise ValueError("overhead observations do not match the schedule")
    required_fields = {
        "cpu_seconds",
        "input_count",
        "input_type_counts",
        "input_type_wall_seconds",
        "logical_result_identity",
        "mode",
        "peak_rss_bytes",
        "run_ordinal",
        "store_bytes",
        "store_initial_identity",
        "wall_seconds",
        "workload_identity",
    }
    normalized: list[dict[str, object]] = []
    identity_fields = (
        "logical_result_identity",
        "store_initial_identity",
        "workload_identity",
    )
    reference_identities: tuple[object, ...] | None = None
    reference_counts: dict[str, int] | None = None
    reference_input_count: int | None = None
    reference_store_bytes: int | None = None
    for ordinal, (expected_mode, observation) in enumerate(
        zip(schedule, observations, strict=True)
    ):
        if set(observation) != required_fields:
            raise ValueError("overhead observation schema is invalid")
        if (
            observation.get("mode") != expected_mode
            or observation.get("run_ordinal") != ordinal
            or any(
                not isinstance(observation.get(name), str)
                or _HEX_64.fullmatch(cast(str, observation[name])) is None
                for name in identity_fields
            )
            or not _is_nonnegative_number(observation.get("wall_seconds"))
            or float(cast(float, observation["wall_seconds"])) <= 0.0
            or not _is_nonnegative_number(observation.get("cpu_seconds"))
            or not _is_nonnegative_integer(
                observation.get("peak_rss_bytes")
            )
            or not _is_nonnegative_integer(observation.get("store_bytes"))
            or observation.get("store_bytes") == 0
            or not _is_nonnegative_integer(observation.get("input_count"))
            or observation.get("input_count") == 0
        ):
            raise ValueError("overhead observation values are invalid")
        raw_counts = observation.get("input_type_counts")
        if (
            not isinstance(raw_counts, Mapping)
            or not raw_counts
            or len(raw_counts) > len(_REPLAY_V3_INPUT_TYPES)
            or any(
                input_type not in _REPLAY_V3_INPUT_TYPES
                or not _is_nonnegative_integer(count)
                or count == 0
                for input_type, count in raw_counts.items()
            )
        ):
            raise ValueError("overhead input type counts are invalid")
        counts = {
            cast(str, input_type): cast(int, count)
            for input_type, count in raw_counts.items()
        }
        input_count = cast(int, observation["input_count"])
        if sum(counts.values()) != input_count:
            raise ValueError("overhead input type counts do not conserve")
        raw_type_wall = observation.get("input_type_wall_seconds")
        if not isinstance(raw_type_wall, Mapping):
            raise ValueError("overhead input type timing is invalid")
        if expected_mode == "OFF":
            if raw_type_wall:
                raise ValueError("OFF overhead observation must not time types")
        elif (
            set(raw_type_wall) != set(counts)
            or any(
                not _is_nonnegative_number(seconds)
                for seconds in raw_type_wall.values()
            )
        ):
            raise ValueError("instrumented type timing is incomplete")
        identities = tuple(observation[name] for name in identity_fields)
        store_bytes = cast(int, observation["store_bytes"])
        if reference_identities is None:
            reference_identities = identities
            reference_counts = counts
            reference_input_count = input_count
            reference_store_bytes = store_bytes
        elif (
            identities != reference_identities
            or counts != reference_counts
            or input_count != reference_input_count
            or store_bytes != reference_store_bytes
        ):
            raise ValueError(
                "overhead runs do not share an exact logical workload/result"
            )
        normalized.append(dict(observation))

    assert reference_identities is not None
    assert reference_counts is not None
    assert reference_input_count is not None
    assert reference_store_bytes is not None
    per_mode: dict[str, dict[str, object]] = {}
    type_medians: dict[str, dict[str, float]] = {}
    for mode in _INSTRUMENTATION_MODES:
        mode_rows = [row for row in normalized if row["mode"] == mode]
        wall_values = [
            float(cast(float, row["wall_seconds"])) for row in mode_rows
        ]
        cpu_values = [
            float(cast(float, row["cpu_seconds"])) for row in mode_rows
        ]
        rss_values = [
            float(cast(int, row["peak_rss_bytes"])) for row in mode_rows
        ]
        throughput_values = [
            reference_input_count / wall for wall in wall_values
        ]
        per_mode[mode] = {
            "commits_per_second": _median_and_mad(throughput_values),
            "cpu_seconds": _median_and_mad(cpu_values),
            "peak_rss_bytes": _median_and_mad(rss_values),
            "repetitions": repetitions,
            "store_bytes": reference_store_bytes,
            "wall_seconds": _median_and_mad(wall_values),
        }
        if mode != "OFF":
            type_medians[mode] = {
                input_type: float(
                    statistics.median(
                        cast(
                            Mapping[str, float],
                            row["input_type_wall_seconds"],
                        )[input_type]
                        for row in mode_rows
                    )
                )
                for input_type in sorted(reference_counts)
            }

    off_wall = cast(
        Mapping[str, object], per_mode["OFF"]["wall_seconds"]
    )
    off_cpu = cast(
        Mapping[str, object], per_mode["OFF"]["cpu_seconds"]
    )
    baseline_wall = cast(float, off_wall["median"])
    baseline_cpu = cast(float, off_cpu["median"])
    overhead_vs_off: dict[str, dict[str, object]] = {}
    for mode in ("V2", "V3"):
        mode_wall = cast(
            Mapping[str, object], per_mode[mode]["wall_seconds"]
        )
        mode_cpu = cast(
            Mapping[str, object], per_mode[mode]["cpu_seconds"]
        )
        wall_delta = cast(float, mode_wall["median"]) - baseline_wall
        cpu_delta = cast(float, mode_cpu["median"]) - baseline_cpu
        wall_per_commit = wall_delta / reference_input_count
        cpu_per_commit = cpu_delta / reference_input_count
        overhead_vs_off[mode] = {
            "cpu_seconds_absolute": cpu_delta,
            "cpu_seconds_per_commit": cpu_per_commit,
            "cpu_seconds_relative": (
                cpu_delta / baseline_cpu if baseline_cpu else None
            ),
            "projection_commits": projection_commits,
            "projected_cpu_seconds": cpu_per_commit * projection_commits,
            "projected_wall_seconds": wall_per_commit * projection_commits,
            "wall_seconds_absolute": wall_delta,
            "wall_seconds_per_commit": wall_per_commit,
            "wall_seconds_relative": wall_delta / baseline_wall,
        }

    incremental_by_type: dict[str, dict[str, object]] = {}
    for input_type, count in sorted(reference_counts.items()):
        delta = (
            type_medians["V3"][input_type]
            - type_medians["V2"][input_type]
        )
        incremental_by_type[input_type] = {
            "input_count": count,
            "measured_v3_minus_v2_wall_seconds": delta,
            "measured_v3_minus_v2_wall_seconds_per_input": delta / count,
        }
    v2_wall_delta = cast(
        float, overhead_vs_off["V2"]["wall_seconds_absolute"]
    )
    allocated_v2_by_type = {
        input_type: {
            "allocation_method": "count_weighted_not_causal",
            "allocated_v2_over_off_wall_seconds": (
                v2_wall_delta * count / reference_input_count
            ),
            "input_count": count,
        }
        for input_type, count in sorted(reference_counts.items())
    }
    return {
        "identities": {
            name: value
            for name, value in zip(
                identity_fields,
                reference_identities,
                strict=True,
            )
        },
        "input_count": reference_input_count,
        "input_type_counts": dict(sorted(reference_counts.items())),
        "mode_order": list(schedule),
        "overhead_by_input_type": {
            "v2_over_off_allocation": allocated_v2_by_type,
            "v3_minus_v2_measured": incremental_by_type,
        },
        "off_baseline_semantics": (
            "diagnostic common wrappers/counters retained; "
            "V2/V3 replay timing and replay-store observer disabled"
        ),
        "overhead_vs_off": overhead_vs_off,
        "per_mode": per_mode,
        "production_noninstrumented_estimate": {
            "qualification": (
                "conservative diagnostic baseline, not a direct production "
                "measurement because common diagnostic wrappers remain"
            ),
            "replay_wall_seconds_median": baseline_wall,
            "replay_wall_seconds_per_commit": (
                baseline_wall / reference_input_count
            ),
            "replay_wall_seconds_projected": (
                baseline_wall
                / reference_input_count
                * projection_commits
            ),
        },
        "projection_commits": projection_commits,
        "repetitions": repetitions,
        "store_bytes": reference_store_bytes,
        "version": _OVERHEAD_REPORT_VERSION,
    }

class ReplayProfiler:
    def __init__(
        self,
        progress_every_rows: int,
        instrumentation_mode: str = "V2",
    ) -> None:
        if instrumentation_mode not in _REPLAY_INSTRUMENTATION_MODES:
            raise ValueError(f"unsupported replay instrumentation mode {instrumentation_mode!r}")
        self.instrumentation_mode = instrumentation_mode
        self.progress_every_rows = progress_every_rows
        self.started_wall = perf_counter()
        self.sequence = 0
        self.active_phase = "worker_setup"
        self.counters: Counter[str] = Counter()
        self.phase_counters: defaultdict[str, Counter[str]] = defaultdict(Counter)
        self.phase_starts: dict[str, tuple[float, float, Counter[str]]] = {}
        self.phase_timings: dict[str, dict[str, object]] = {}
        self._emit_lock = threading.Lock()
        self.target_connections: list[sqlite3.Connection] = []
        self.source_connection_count = 0
        self.source_query_only_verified = False
        self.source_write_connection_attempts = 0
        self.target_integrity_complete = False
        self.final_comparison_started = False
        self.target_identity: dict[str, object] = {}
        self.target_initial_identity: str | None = None
        self.target_database_bytes: int | None = None
        self.target_database_path: Path | None = None
        self.progress_units = 0
        self.next_progress = progress_every_rows
        self.replay_expected_target_commits = 0
        self.source_replay_first: dict[str, object] | None = None
        self.source_replay_tail: list[dict[str, object]] = []
        self.replay_operation_timings: dict[str, dict[str, float | int]] = {}
        self.replay_input_type_timings: dict[str, dict[str, float | int]] = {}
        self.replay_input_type_counts: Counter[str] = Counter()
        self.completed_replay_input_tail: list[dict[str, object]] = []
        self.projection_history_sizes = {
            "latest_commit_sequence": 0,
            "latest_projection_characters": 0,
            "latest_zlib_bytes": 0,
            "max_projection_characters": 0,
            "max_zlib_bytes": 0,
        }
        self.slowest_completed_replay_inputs: list[dict[str, object]] = []
        self.last_completed_replay_input: dict[str, object] | None = None
        self._current_replay_input: dict[str, Any] | None = None
        self._active_replay_operation: str | None = None
        self._active_replay_operation_started_wall: float | None = None
        self._active_replay_operation_started_cpu: float | None = None
        self._historical_append_depth = 0
        self._replay_timing_lock = threading.RLock()
        self._v3_active_replay_operation: str | None = None
        self._v3_active_replay_operation_started_wall_ns: int | None = None
        self._v3_active_replay_operation_started_cpu_ns: int | None = None
        self._v3_operation_matrix: dict[tuple[str, str], dict[str, Any]] = {}
        self._v3_phase_operation_timings: dict[str, dict[str, int]] = {}
        self._v3_input_type_timings: dict[str, dict[str, int]] = {}
        self._v3_parent_scope_timings: dict[str, dict[str, int]] = {}
        self._v3_scope_stack: list[dict[str, object]] = []
        self._v3_completed_input_count = 0
        self._v3_completed_input_wall_ns = 0
        self._v3_completed_input_cpu_ns = 0
        self._v3_replay_phase_started_wall_ns: int | None = None
        self._v3_replay_phase_wall_ns: int | None = None
        self._v3_validation_depth = 0
        self._v3_funding_lookup_depth = 0
        self._v3_funding_lookup: dict[str, Any] = {
            "lookup_count": 0,
            "rows_returned": 0,
            "payload_characters": 0,
            "max_historical_distance": 0,
            "query_fingerprints": [],
            "eqp": [],
            "eqp_capture_connection_count": 0,
            "eqp_capture_query_only_verified": False,
            "eqp_capture_thread_cpu_nanoseconds": 0,
            "eqp_capture_total_changes": 0,
            "eqp_capture_wall_nanoseconds": 0,
            "existing_indexes": [],
            "fallback_scan_detected": False,
            "query_shape": {
                "columns": [],
                "order_by": ["commit_sequence", "input_id"],
                "predicates": [
                    "run_id_equal",
                    "commit_sequence_greater_than",
                    "payload_input_type_equal",
                ],
                "table": "paper_inbox",
            },
            "requested_after_commit_sequence_max": 0,
            "requested_after_commit_sequence_min": None,
            "selected_index_name": None,
        }
        self._v3_funding_plan_captured = False
        self._v3_observer_stack: list[tuple[str, str | None]] = []

    def replay_input_type(self, payload: Mapping[str, object]) -> str:
        raw = payload.get("input_type")
        if isinstance(raw, str) and raw in _REPLAY_INPUT_TYPES:
            return raw
        return "UNKNOWN" if self.instrumentation_mode == "V3" else "OTHER"

    @staticmethod
    def _add_timing(
        timings: dict[str, dict[str, float | int]],
        name: str,
        *,
        wall_seconds: float,
        thread_cpu_seconds: float,
    ) -> None:
        entry = timings.setdefault(
            name,
            {
                "span_count": 0,
                "thread_cpu_seconds": 0.0,
                "max_wall_seconds": 0.0,
                "wall_seconds": 0.0,
            },
        )
        entry["span_count"] = int(entry["span_count"]) + 1
        entry["thread_cpu_seconds"] = float(entry["thread_cpu_seconds"]) + max(
            0.0, thread_cpu_seconds
        )
        entry["wall_seconds"] = float(entry["wall_seconds"]) + max(0.0, wall_seconds)
        entry["max_wall_seconds"] = max(
            float(entry["max_wall_seconds"]),
            max(0.0, wall_seconds),
        )

    def _finish_replay_operation_locked(self, now_wall: float, now_cpu: float) -> None:
        # Nested operations close and later resume their parent, so these are
        # exclusive timing spans rather than logical call counts.
        name = self._active_replay_operation
        started_wall = self._active_replay_operation_started_wall
        started_cpu = self._active_replay_operation_started_cpu
        if name is not None and started_wall is not None and started_cpu is not None:
            self._add_timing(
                self.replay_operation_timings,
                name,
                wall_seconds=now_wall - started_wall,
                thread_cpu_seconds=now_cpu - started_cpu,
            )
        self._active_replay_operation = None
        self._active_replay_operation_started_wall = None
        self._active_replay_operation_started_cpu = None

    def _finish_v3_replay_operation_locked(self, now_wall_ns: int, now_cpu_ns: int) -> None:
        name = self._v3_active_replay_operation
        started_wall = self._v3_active_replay_operation_started_wall_ns
        started_cpu = self._v3_active_replay_operation_started_cpu_ns
        current = self._current_replay_input
        if name is not None and started_wall is not None and started_cpu is not None and current is not None:
            wall_ns = max(0, now_wall_ns - started_wall)
            cpu_ns = max(0, now_cpu_ns - started_cpu)
            operations = cast(dict[str, dict[str, Any]], current["v3_operations"])
            entry = operations.setdefault(
                name,
                {
                    "histogram": [0] * _REPLAY_V3_HISTOGRAM_BUCKETS,
                    "max_wall_nanoseconds": 0,
                    "span_count": 0,
                    "thread_cpu_nanoseconds": 0,
                    "wall_nanoseconds": 0,
                },
            )
            entry["span_count"] = int(entry["span_count"]) + 1
            entry["thread_cpu_nanoseconds"] = int(entry["thread_cpu_nanoseconds"]) + cpu_ns
            entry["wall_nanoseconds"] = int(entry["wall_nanoseconds"]) + wall_ns
            entry["max_wall_nanoseconds"] = max(int(entry["max_wall_nanoseconds"]), wall_ns)
            histogram = cast(list[int], entry["histogram"])
            bucket = min(len(histogram) - 1, wall_ns.bit_length())
            histogram[bucket] += 1
            if self._v3_scope_stack:
                active_scope = self._v3_scope_stack[-1]
                active_scope["direct_operation_wall_nanoseconds"] = (
                    cast(int, active_scope["direct_operation_wall_nanoseconds"])
                    + wall_ns
                )
                active_scope["direct_operation_cpu_nanoseconds"] = (
                    cast(int, active_scope["direct_operation_cpu_nanoseconds"])
                    + cpu_ns
                )
        elif (
            name is not None
            and started_wall is not None
            and started_cpu is not None
            and current is None
        ):
            wall_ns = max(0, now_wall_ns - started_wall)
            cpu_ns = max(0, now_cpu_ns - started_cpu)
            phase_name = (
                name if name in _REPLAY_V3_PHASE_OPERATIONS else "UNKNOWN"
            )
            phase_entry = self._v3_phase_operation_timings.setdefault(
                phase_name,
                {
                    "max_wall_nanoseconds": 0,
                    "span_count": 0,
                    "thread_cpu_nanoseconds": 0,
                    "wall_nanoseconds": 0,
                },
            )
            phase_entry["span_count"] += 1
            phase_entry["thread_cpu_nanoseconds"] += cpu_ns
            phase_entry["wall_nanoseconds"] += wall_ns
            phase_entry["max_wall_nanoseconds"] = max(
                phase_entry["max_wall_nanoseconds"],
                wall_ns,
            )
        self._v3_active_replay_operation = None
        self._v3_active_replay_operation_started_wall_ns = None
        self._v3_active_replay_operation_started_cpu_ns = None

    def transition_replay_operation(self, name: str | None) -> None:
        if self.instrumentation_mode == "OFF":
            return
        if self.instrumentation_mode == "V2":
            if name is not None and name not in _REPLAY_OPERATIONS:
                raise ValueError(f"unknown replay timing operation {name!r}")
            now_wall = perf_counter()
            now_cpu = thread_time()
            with self._replay_timing_lock:
                if name == self._active_replay_operation:
                    return
                self._finish_replay_operation_locked(now_wall, now_cpu)
                if name is not None:
                    self._active_replay_operation = name
                    self._active_replay_operation_started_wall = now_wall
                    self._active_replay_operation_started_cpu = now_cpu
            return
        normalized = name if name is None or name in _REPLAY_V3_OPERATIONS else "UNKNOWN"
        now_wall_ns = perf_counter_ns()
        now_cpu_ns = thread_time_ns()
        with self._replay_timing_lock:
            if normalized == self._v3_active_replay_operation:
                return
            self._finish_v3_replay_operation_locked(now_wall_ns, now_cpu_ns)
            if normalized is not None:
                self._v3_active_replay_operation = normalized
                self._v3_active_replay_operation_started_wall_ns = now_wall_ns
                self._v3_active_replay_operation_started_cpu_ns = now_cpu_ns

    @contextmanager
    def replay_operation(self, name: str) -> Iterator[None]:
        if self.instrumentation_mode == "OFF":
            yield
            return
        with self._replay_timing_lock:
            previous = (
                self._active_replay_operation
                if self.instrumentation_mode == "V2"
                else self._v3_active_replay_operation
            )
        self.transition_replay_operation(name)
        try:
            yield
        finally:
            self.transition_replay_operation(previous)

    def begin_historical_append(self) -> None:
        self._historical_append_depth += 1

    def finish_historical_append(self) -> None:
        self._historical_append_depth -= 1
        if self._historical_append_depth < 0:
            raise RuntimeError("historical replay append timing depth underflow")

    @property
    def historical_append_active(self) -> bool:
        return self._historical_append_depth > 0

    def set_source_replay_tail(
        self,
        *,
        expected_target_commits: int,
        first_record: tuple[int, str] | None,
        records: Sequence[tuple[int, str]],
    ) -> None:
        first = (
            {
                "commit_sequence": first_record[0],
                "input_type": first_record[1],
            }
            if first_record is not None
            else None
        )
        tail = [
            {
                "commit_sequence": commit_sequence,
                "input_type": input_type,
            }
            for commit_sequence, input_type in records[-_REPLAY_TAIL_LIMIT:]
        ]
        with self._replay_timing_lock:
            self.replay_expected_target_commits = expected_target_commits
            self.source_replay_first = first
            self.source_replay_tail = tail

    def observe_projection_history_storage(
        self,
        *,
        projection_characters: int,
        zlib_bytes: int,
    ) -> None:
        commit_sequence = int(self.counters["target_append_transaction_count"])
        if self.historical_append_active:
            commit_sequence += 1
        with self._replay_timing_lock:
            sizes = self.projection_history_sizes
            sizes["latest_commit_sequence"] = commit_sequence
            sizes["latest_projection_characters"] = projection_characters
            sizes["latest_zlib_bytes"] = zlib_bytes
            sizes["max_projection_characters"] = max(
                sizes["max_projection_characters"], projection_characters
            )
            sizes["max_zlib_bytes"] = max(sizes["max_zlib_bytes"], zlib_bytes)
            if self.instrumentation_mode == "V3" and self._current_replay_input is not None:
                self._current_replay_input["projection_after_characters"] = projection_characters

    def begin_parent_scope(self, name: str) -> None:
        if self.instrumentation_mode != "V3":
            return
        normalized = (
            name if name in _REPLAY_V3_SCOPE_PARENTS else "UNKNOWN"
        )
        with self._replay_timing_lock:
            previous_operation = self._v3_active_replay_operation
        self.transition_replay_operation(None)
        started_cpu_ns = thread_time_ns()
        started_wall_ns = perf_counter_ns()
        with self._replay_timing_lock:
            self._v3_scope_stack.append(
                {
                    "child_cpu_nanoseconds": 0,
                    "child_wall_nanoseconds": 0,
                    "direct_operation_cpu_nanoseconds": 0,
                    "direct_operation_wall_nanoseconds": 0,
                    "name": normalized,
                    "started_cpu_ns": started_cpu_ns,
                    "started_wall_ns": started_wall_ns,
                }
            )
        self.transition_replay_operation(previous_operation)

    def finish_parent_scope(self, name: str) -> None:
        if self.instrumentation_mode != "V3":
            return
        normalized = (
            name if name in _REPLAY_V3_SCOPE_PARENTS else "UNKNOWN"
        )
        with self._replay_timing_lock:
            previous_operation = self._v3_active_replay_operation
        self.transition_replay_operation(None)
        now_wall_ns = perf_counter_ns()
        now_cpu_ns = thread_time_ns()
        with self._replay_timing_lock:
            if not self._v3_scope_stack:
                raise RuntimeError("replay timing scope stack underflow")
            active = self._v3_scope_stack[-1]
            if active["name"] != normalized:
                raise RuntimeError(
                    "replay timing scope mismatch: "
                    f"expected {active['name']!r}, got {normalized!r}"
                )
            self._v3_scope_stack.pop()
            wall_ns = max(
                0,
                now_wall_ns - cast(int, active["started_wall_ns"]),
            )
            cpu_ns = max(
                0,
                now_cpu_ns - cast(int, active["started_cpu_ns"]),
            )
            child_wall_ns = cast(
                int, active["child_wall_nanoseconds"]
            )
            child_cpu_ns = cast(
                int, active["child_cpu_nanoseconds"]
            )
            direct_wall_ns = cast(
                int, active["direct_operation_wall_nanoseconds"]
            )
            direct_cpu_ns = cast(
                int, active["direct_operation_cpu_nanoseconds"]
            )
            self_wall_ns = max(0, wall_ns - child_wall_ns)
            self_cpu_ns = max(0, cpu_ns - child_cpu_ns)
            unattributed_wall_ns = max(
                0, self_wall_ns - direct_wall_ns
            )
            unattributed_cpu_ns = max(
                0, self_cpu_ns - direct_cpu_ns
            )
            accounting_delta_ns = abs(
                wall_ns
                - child_wall_ns
                - direct_wall_ns
                - unattributed_wall_ns
            )
            current = self._current_replay_input
            if current is not None:
                scopes = cast(
                    dict[str, dict[str, int]],
                    current["v3_scopes"],
                )
                entry = scopes.setdefault(
                    normalized,
                    {
                        "accounting_delta_wall_nanoseconds": 0,
                        "children_inclusive_wall_nanoseconds": 0,
                        "direct_operation_thread_cpu_nanoseconds": 0,
                        "direct_operation_wall_nanoseconds": 0,
                        "inclusive_thread_cpu_nanoseconds": 0,
                        "inclusive_wall_nanoseconds": 0,
                        "self_thread_cpu_nanoseconds": 0,
                        "self_wall_nanoseconds": 0,
                        "span_count": 0,
                        "unattributed_thread_cpu_nanoseconds": 0,
                        "unattributed_wall_nanoseconds": 0,
                    },
                )
                entry["span_count"] += 1
                entry[
                    "accounting_delta_wall_nanoseconds"
                ] += accounting_delta_ns
                entry["inclusive_wall_nanoseconds"] += wall_ns
                entry["inclusive_thread_cpu_nanoseconds"] += cpu_ns
                entry[
                    "children_inclusive_wall_nanoseconds"
                ] += child_wall_ns
                entry[
                    "direct_operation_wall_nanoseconds"
                ] += direct_wall_ns
                entry[
                    "direct_operation_thread_cpu_nanoseconds"
                ] += direct_cpu_ns
                entry["self_wall_nanoseconds"] += self_wall_ns
                entry["self_thread_cpu_nanoseconds"] += self_cpu_ns
                entry[
                    "unattributed_wall_nanoseconds"
                ] += unattributed_wall_ns
                entry[
                    "unattributed_thread_cpu_nanoseconds"
                ] += unattributed_cpu_ns
            if self._v3_scope_stack:
                parent = self._v3_scope_stack[-1]
                parent["child_wall_nanoseconds"] = (
                    cast(int, parent["child_wall_nanoseconds"])
                    + wall_ns
                )
                parent["child_cpu_nanoseconds"] = (
                    cast(int, parent["child_cpu_nanoseconds"])
                    + cpu_ns
                )
        self.transition_replay_operation(previous_operation)
    @contextmanager
    def replay_parent_scope(self, name: str) -> Iterator[None]:
        self.begin_parent_scope(name)
        try:
            yield
        finally:
            self.finish_parent_scope(name)

    def begin_replay_input(self, *, commit_sequence: int, input_type: str) -> None:
        if self.instrumentation_mode == "OFF":
            with self._replay_timing_lock:
                if self._current_replay_input is not None:
                    raise RuntimeError("replay input progress already active")
                self._current_replay_input = {
                    "commit_sequence": commit_sequence,
                    "input_type": input_type,
                }
            return
        if self.instrumentation_mode == "V3":
            self.transition_replay_operation(None)
            now_wall_ns = perf_counter_ns()
            now_cpu_ns = thread_time_ns()
            normalized_type = input_type if input_type in _REPLAY_V3_INPUT_TYPES else "UNKNOWN"
            with self._replay_timing_lock:
                if self._current_replay_input is not None:
                    raise RuntimeError("replay input timing already active")
                self._current_replay_input = {
                    "alert_count": 0,
                    "commit_sequence": commit_sequence,
                    "event_count": 0,
                    "funding_lookup": None,
                    "input_type": normalized_type,
                    "ledger_entry_count": 0,
                    "projection_after_characters": self.projection_history_sizes[
                        "latest_projection_characters"
                    ],
                    "projection_before_characters": self.projection_history_sizes[
                        "latest_projection_characters"
                    ],
                    "query_fingerprints": [],
                    "started_cpu_ns": now_cpu_ns,
                    "started_wall_ns": now_wall_ns,
                    "v3_operations": {},
                    "v3_scopes": {},
                }
            self.begin_parent_scope("replay_input")
            self.begin_parent_scope("input_dispatch")
            self.transition_replay_operation("input_dispatch_prepare")
            return
        self.transition_replay_operation(None)
        now_wall = perf_counter()
        now_cpu = thread_time()
        with self._replay_timing_lock:
            if self._current_replay_input is not None:
                raise RuntimeError("replay input timing already active")
            self._current_replay_input = {
                "commit_sequence": commit_sequence,
                "input_type": input_type,
                "operation_before": {
                    name: (
                        int(values["span_count"]),
                        float(values["thread_cpu_seconds"]),
                        float(values["wall_seconds"]),
                    )
                    for name, values in self.replay_operation_timings.items()
                },
                "started_cpu": now_cpu,
                "started_wall": now_wall,
            }
        self.transition_replay_operation("input_dispatch")

    @staticmethod
    def _merge_v3_histogram(target: list[int], source: Sequence[int]) -> None:
        for index, count in enumerate(source[: len(target)]):
            target[index] += int(count)

    def _finish_replay_input_v3(self, *, completed: bool) -> None:
        self.transition_replay_operation(None)
        while self._v3_scope_stack:
            self.finish_parent_scope(cast(str, self._v3_scope_stack[-1]["name"]))
        now_wall_ns = perf_counter_ns()
        now_cpu_ns = thread_time_ns()
        with self._replay_timing_lock:
            current = self._current_replay_input
            if current is None:
                return
            self._current_replay_input = None
            if not completed:
                return
            commit_sequence = cast(int, current["commit_sequence"])
            input_type = cast(str, current["input_type"])
            operations = cast(dict[str, dict[str, Any]], current["v3_operations"])
            wall_ns = max(
                0,
                now_wall_ns - cast(int, current["started_wall_ns"]),
            )
            cpu_ns = max(0, now_cpu_ns - cast(int, current["started_cpu_ns"]))
            type_entry = self._v3_input_type_timings.setdefault(
                input_type,
                {
                    "max_wall_nanoseconds": 0,
                    "span_count": 0,
                    "thread_cpu_nanoseconds": 0,
                    "wall_nanoseconds": 0,
                },
            )
            type_entry["span_count"] += 1
            type_entry["thread_cpu_nanoseconds"] += cpu_ns
            type_entry["wall_nanoseconds"] += wall_ns
            type_entry["max_wall_nanoseconds"] = max(
                type_entry["max_wall_nanoseconds"], wall_ns
            )
            for operation, values in operations.items():
                matrix_entry = self._v3_operation_matrix.setdefault(
                    (input_type, operation),
                    {
                        "affected_input_count": 0,
                        "histogram": [0] * _REPLAY_V3_HISTOGRAM_BUCKETS,
                        "max_wall_nanoseconds": 0,
                        "span_count": 0,
                        "thread_cpu_nanoseconds": 0,
                        "wall_nanoseconds": 0,
                    },
                )
                matrix_entry["affected_input_count"] = (
                    int(matrix_entry["affected_input_count"]) + 1
                )
                for name in (
                    "span_count",
                    "thread_cpu_nanoseconds",
                    "wall_nanoseconds",
                ):
                    matrix_entry[name] = int(matrix_entry[name]) + int(values[name])
                matrix_entry["max_wall_nanoseconds"] = max(
                    int(matrix_entry["max_wall_nanoseconds"]),
                    int(values["max_wall_nanoseconds"]),
                )
                self._merge_v3_histogram(
                    cast(list[int], matrix_entry["histogram"]),
                    cast(Sequence[int], values["histogram"]),
                )
            scopes = cast(dict[str, dict[str, int]], current["v3_scopes"])
            for scope, scope_values in scopes.items():
                aggregate = self._v3_parent_scope_timings.setdefault(
                    scope,
                    {
                        "accounting_delta_wall_nanoseconds": 0,
                        "affected_input_count": 0,
                        "children_inclusive_wall_nanoseconds": 0,
                        "direct_operation_thread_cpu_nanoseconds": 0,
                        "direct_operation_wall_nanoseconds": 0,
                        "inclusive_thread_cpu_nanoseconds": 0,
                        "inclusive_wall_nanoseconds": 0,
                        "run_start_affected_input_count": 0,
                        "self_thread_cpu_nanoseconds": 0,
                        "self_wall_nanoseconds": 0,
                        "span_count": 0,
                        "unattributed_thread_cpu_nanoseconds": 0,
                        "unattributed_wall_nanoseconds": 0,
                    },
                )
                aggregate["affected_input_count"] += 1
                if input_type == "RUN_START":
                    aggregate["run_start_affected_input_count"] += 1
                for name, value in scope_values.items():
                    aggregate[name] += value
            operation_items = [
                {
                    "operation": name,
                    "span_count": int(values["span_count"]),
                    "thread_cpu_nanoseconds": int(values["thread_cpu_nanoseconds"]),
                    "wall_nanoseconds": int(values["wall_nanoseconds"]),
                }
                for name, values in sorted(operations.items())
            ]
            scope_items = [
                {
                    **values,
                    "parent": _REPLAY_V3_SCOPE_PARENTS[name],
                    "scope": name,
                    "timing_character": "inclusive_and_self",
                }
                for name, values in sorted(scopes.items())
            ]
            detail: dict[str, object] = {
                "alert_count": int(current["alert_count"]),
                "commit_sequence": commit_sequence,
                "event_count": int(current["event_count"]),
                "funding_lookup": current["funding_lookup"],
                "input_type": input_type,
                "ledger_entry_count": int(current["ledger_entry_count"]),
                "operations": operation_items,
                "projection_after_characters": int(
                    current["projection_after_characters"]
                ),
                "projection_before_characters": int(
                    current["projection_before_characters"]
                ),
                "projection_characters": self.projection_history_sizes[
                    "latest_projection_characters"
                ],
                "query_fingerprints": sorted(
                    cast(Sequence[str], current["query_fingerprints"])
                ),
                "scopes": scope_items,
                "thread_cpu_nanoseconds": cpu_ns,
                "wall_nanoseconds": wall_ns,
                "zlib_bytes": self.projection_history_sizes["latest_zlib_bytes"],
            }
            self._v3_completed_input_count += 1
            self.replay_input_type_counts[input_type] += 1
            self._v3_completed_input_wall_ns += wall_ns
            self._v3_completed_input_cpu_ns += cpu_ns
            self.completed_replay_input_tail.append(detail)
            del self.completed_replay_input_tail[:-_REPLAY_TAIL_LIMIT]
            self.last_completed_replay_input = {
                "commit_sequence": commit_sequence,
                "input_type": input_type,
            }
            self.slowest_completed_replay_inputs.append(detail)
            self.slowest_completed_replay_inputs.sort(
                key=lambda item: (
                    -cast(int, item["wall_nanoseconds"]),
                    cast(int, item["commit_sequence"]),
                )
            )
            del self.slowest_completed_replay_inputs[_REPLAY_SLOWEST_LIMIT:]
    def finish_replay_input(self, *, completed: bool) -> None:
        if self.instrumentation_mode == "OFF":
            with self._replay_timing_lock:
                current = self._current_replay_input
                self._current_replay_input = None
                if completed and current is not None:
                    input_type = cast(str, current["input_type"])
                    normalized_type = (
                        input_type if input_type in _REPLAY_INPUT_TYPES else "OTHER"
                    )
                    self.replay_input_type_counts[normalized_type] += 1
                    self.last_completed_replay_input = {
                        "commit_sequence": cast(int, current["commit_sequence"]),
                        "input_type": normalized_type,
                    }
            return
        if self.instrumentation_mode == "V3":
            self._finish_replay_input_v3(completed=completed)
            return
        self.transition_replay_operation(None)
        now_wall = perf_counter()
        now_cpu = thread_time()
        with self._replay_timing_lock:
            current = self._current_replay_input
            if current is None:
                return
            self._current_replay_input = None
            if not completed:
                return
            commit_sequence = cast(int, current["commit_sequence"])
            input_type = cast(str, current["input_type"])
            self.replay_input_type_counts[
                input_type if input_type in _REPLAY_INPUT_TYPES else "OTHER"
            ] += 1
            started_wall = cast(float, current["started_wall"])
            started_cpu = cast(float, current["started_cpu"])
            wall_seconds = max(0.0, now_wall - started_wall)
            cpu_seconds = max(0.0, now_cpu - started_cpu)
            self._add_timing(
                self.replay_input_type_timings,
                input_type,
                wall_seconds=wall_seconds,
                thread_cpu_seconds=cpu_seconds,
            )
            before = cast(
                dict[str, tuple[int, float, float]],
                current["operation_before"],
            )
            input_operations: dict[str, dict[str, float | int]] = {}
            for name, values in self.replay_operation_timings.items():
                previous_span_count, previous_cpu, previous_wall = before.get(
                    name,
                    (0, 0.0, 0.0),
                )
                span_count = int(values["span_count"]) - previous_span_count
                if span_count <= 0:
                    continue
                input_operations[name] = {
                    "span_count": span_count,
                    "thread_cpu_seconds": max(
                        0.0,
                        float(values["thread_cpu_seconds"]) - previous_cpu,
                    ),
                    "wall_seconds": max(0.0, float(values["wall_seconds"]) - previous_wall),
                }
            completed_input: dict[str, object] = {
                "commit_sequence": commit_sequence,
                "input_type": input_type,
                "thread_cpu_seconds": cpu_seconds,
                "wall_seconds": wall_seconds,
            }
            tail_input = {
                **completed_input,
                "operations": input_operations,
                "projection_characters": self.projection_history_sizes["latest_projection_characters"],
                "zlib_bytes": self.projection_history_sizes["latest_zlib_bytes"],
            }
            self.completed_replay_input_tail.append(tail_input)
            del self.completed_replay_input_tail[:-_REPLAY_TAIL_LIMIT]
            self.last_completed_replay_input = {
                "commit_sequence": commit_sequence,
                "input_type": input_type,
            }
            self.slowest_completed_replay_inputs.append(completed_input)
            self.slowest_completed_replay_inputs.sort(
                key=lambda item: (
                    -cast(float, item["wall_seconds"]),
                    cast(int, item["commit_sequence"]),
                )
            )
            del self.slowest_completed_replay_inputs[_REPLAY_SLOWEST_LIMIT:]

    def replay_timing_snapshot(self) -> dict[str, object]:
        now_wall = perf_counter()
        target_database_bytes = self.target_database_bytes
        target_path = self.target_database_path
        if target_path is not None:
            with suppress(OSError):
                target_database_bytes = target_path.stat().st_size
        with self._replay_timing_lock:
            active_input: dict[str, object] | None = None
            if self._current_replay_input is not None:
                current = self._current_replay_input
                commit_sequence = cast(int, current["commit_sequence"])
                input_type = cast(str, current["input_type"])
                started_wall = cast(float, current["started_wall"])
                active_input = {
                    "commit_sequence": commit_sequence,
                    "input_type": input_type,
                    "wall_seconds": max(0.0, now_wall - started_wall),
                }
            active_operation: dict[str, object] | None = None
            if (
                self._active_replay_operation is not None
                and self._active_replay_operation_started_wall is not None
            ):
                active_operation = {
                    "name": self._active_replay_operation,
                    "wall_seconds": max(
                        0.0,
                        now_wall - self._active_replay_operation_started_wall,
                    ),
                }
            expected = self.replay_expected_target_commits
            completed = int(self.counters["target_append_transaction_count"])
            return {
                "active_input": active_input,
                "active_operation": active_operation,
                "completed_input_tail": [
                    dict(item) for item in self.completed_replay_input_tail
                ],
                "completed_target_commits": completed,
                "expected_target_commits": expected,
                "input_type_timings": {
                    name: dict(values)
                    for name, values in sorted(self.replay_input_type_timings.items())
                },
                "last_completed_input": (
                    dict(self.last_completed_replay_input)
                    if self.last_completed_replay_input is not None
                    else None
                ),
                "operation_timings": {
                    name: dict(values)
                    for name, values in sorted(self.replay_operation_timings.items())
                },
                "projection_history_sizes": dict(self.projection_history_sizes),
                "remaining_target_commits": max(0, expected - completed),
                "slowest_completed_inputs": [
                    dict(item) for item in self.slowest_completed_replay_inputs
                ],
                "source_first_input": (
                    dict(self.source_replay_first)
                    if self.source_replay_first is not None
                    else None
                ),
                "source_tail": [dict(item) for item in self.source_replay_tail],
                "target_database_bytes": target_database_bytes,
                "version": _REPLAY_TIMING_VERSION,
            }

    def replay_progress_snapshot(self) -> dict[str, object]:
        target_bytes = self.target_database_bytes
        if self.target_database_path is not None:
            with suppress(OSError):
                target_bytes = self.target_database_path.stat().st_size
        with self._replay_timing_lock:
            active: dict[str, object] | None = None
            if self._current_replay_input is not None:
                current = self._current_replay_input
                if self.instrumentation_mode == "V3":
                    elapsed = max(0, perf_counter_ns() - cast(int, current["started_wall_ns"]))
                elif self.instrumentation_mode == "V2":
                    elapsed = max(
                        0,
                        int(
                            (perf_counter() - cast(float, current["started_wall"]))
                            * 1_000_000_000
                        ),
                    )
                else:
                    elapsed = 0
                raw_type = cast(str, current["input_type"])
                active = {
                    "commit_sequence": cast(int, current["commit_sequence"]),
                    "input_type": raw_type if raw_type in _REPLAY_INPUT_TYPES else "OTHER",
                    "wall_nanoseconds": elapsed,
                }
            expected = self.replay_expected_target_commits
            completed = int(self.counters["target_append_transaction_count"])
            return {
                "active_input": active,
                "completed_target_commits": completed,
                "expected_target_commits": expected,
                "input_type_counts": dict(sorted(self.replay_input_type_counts.items())),
                "last_completed_input": (
                    dict(self.last_completed_replay_input)
                    if self.last_completed_replay_input is not None
                    else None
                ),
                "projection_history_sizes": dict(self.projection_history_sizes),
                "remaining_target_commits": max(0, expected - completed),
                "source_first_input": (
                    dict(self.source_replay_first)
                    if self.source_replay_first is not None
                    else None
                ),
                "source_tail": [dict(item) for item in self.source_replay_tail],
                "target_database_bytes": target_bytes,
            }

    @staticmethod
    def _v3_histogram_quantile(histogram: Sequence[int], percentile: int) -> int:
        total = sum(histogram)
        if total <= 0:
            return 0
        target = max(1, math.ceil(total * percentile / 100))
        observed = 0
        for bucket, count in enumerate(histogram):
            observed += int(count)
            if observed >= target:
                return 0 if bucket == 0 else 1 << (bucket - 1)
        return 1 << (len(histogram) - 2)

    def replay_timing_v3_snapshot(self) -> dict[str, object]:
        with self._replay_timing_lock:
            total_wall = self._v3_completed_input_wall_ns
            phase_wall = self._v3_replay_phase_wall_ns
            if phase_wall is None:
                phase_started = self._v3_replay_phase_started_wall_ns
                phase_wall = (
                    max(0, perf_counter_ns() - phase_started)
                    if phase_started is not None
                    else 0
                )
            matrix: list[dict[str, object]] = []
            exclusive_wall = 0
            for (input_type, operation), values in sorted(
                self._v3_operation_matrix.items()
            ):
                span_count = int(values["span_count"])
                wall = int(values["wall_nanoseconds"])
                histogram = cast(Sequence[int], values["histogram"])
                exclusive_wall += wall
                matrix.append(
                    {
                        "affected_input_count": int(values["affected_input_count"]),
                        "input_type": input_type,
                        "max_wall_nanoseconds": int(values["max_wall_nanoseconds"]),
                        "mean_thread_cpu_nanoseconds": (
                            int(values["thread_cpu_nanoseconds"]) / span_count
                        ),
                        "mean_wall_nanoseconds": wall / span_count,
                        "operation": operation,
                        "p50_wall_nanoseconds": self._v3_histogram_quantile(histogram, 50),
                        "p95_wall_nanoseconds": self._v3_histogram_quantile(histogram, 95),
                        "phase_wall_share": wall / phase_wall if phase_wall else 0.0,
                        "span_count": span_count,
                        "thread_cpu_nanoseconds": int(values["thread_cpu_nanoseconds"]),
                        "timing_character": "exclusive",
                        "wall_nanoseconds": wall,
                    }
                )
            phase_operations: list[dict[str, object]] = []
            phase_operation_wall = 0
            for operation, values in sorted(
                self._v3_phase_operation_timings.items()
            ):
                wall = int(values["wall_nanoseconds"])
                phase_operation_wall += wall
                phase_operations.append(
                    {
                        "max_wall_nanoseconds": int(
                            values["max_wall_nanoseconds"]
                        ),
                        "operation": operation,
                        "phase_wall_share": (
                            wall / phase_wall if phase_wall else 0.0
                        ),
                        "span_count": int(values["span_count"]),
                        "thread_cpu_nanoseconds": int(
                            values["thread_cpu_nanoseconds"]
                        ),
                        "timing_character": "exclusive",
                        "wall_nanoseconds": wall,
                    }
                )
            known_diagnostic_wall = int(
                self._v3_funding_lookup["eqp_capture_wall_nanoseconds"]
            )
            residual = max(0, total_wall - exclusive_wall)
            over_attributed = max(0, exclusive_wall - total_wall)
            conservation_delta = abs(total_wall - exclusive_wall - residual)
            outside_completed_inputs = max(0, phase_wall - total_wall)
            phase_attributed_wall = (
                exclusive_wall + phase_operation_wall + known_diagnostic_wall
            )
            phase_residual = max(0, phase_wall - phase_attributed_wall)
            phase_over_attributed = max(0, phase_attributed_wall - phase_wall)
            phase_conservation_delta = abs(
                phase_wall - phase_attributed_wall - phase_residual
            )
            totals = [
                {
                    "input_type": input_type,
                    "max_wall_nanoseconds": values["max_wall_nanoseconds"],
                    "mean_wall_nanoseconds": (
                        values["wall_nanoseconds"] / values["span_count"]
                    ),
                    "span_count": values["span_count"],
                    "thread_cpu_nanoseconds": values["thread_cpu_nanoseconds"],
                    "wall_nanoseconds": values["wall_nanoseconds"],
                }
                for input_type, values in sorted(self._v3_input_type_timings.items())
            ]
            parents = []
            scope_accounting_delta = 0
            for name, scope_values in sorted(self._v3_parent_scope_timings.items()):
                delta = max(
                    0,
                    scope_values["inclusive_wall_nanoseconds"]
                    - scope_values["children_inclusive_wall_nanoseconds"],
                )
                scope_accounting_delta += scope_values[
                    "accounting_delta_wall_nanoseconds"
                ]
                parents.append(
                    {
                        **scope_values,
                        "parent": _REPLAY_V3_SCOPE_PARENTS[name],
                        "parent_child_delta_wall_nanoseconds": delta,
                        "scope": name,
                        "timing_character": "inclusive_and_self",
                    }
                )
            unknown_input_count = self.replay_input_type_counts["UNKNOWN"]
            unknown_operation_span_count = sum(
                int(values["span_count"])
                for (input_type, operation), values in self._v3_operation_matrix.items()
                if input_type == "UNKNOWN" or operation == "UNKNOWN"
            ) + int(
                self._v3_phase_operation_timings.get(
                    "UNKNOWN",
                    {"span_count": 0},
                )["span_count"]
            )
            completed_input_count = self._v3_completed_input_count
            run_start_input_count = int(
                self.replay_input_type_counts["RUN_START"]
            )
            committed_input_count = max(
                0,
                completed_input_count - run_start_input_count,
            )
            required_phase_operation_coverage = [
                {
                    "missing_span_count": int(
                        operation not in self._v3_phase_operation_timings
                    ),
                    "operation": operation,
                    "span_count": int(
                        self._v3_phase_operation_timings.get(
                            operation,
                            {"span_count": 0},
                        )["span_count"]
                    ),
                }
                for operation in sorted(
                    _REPLAY_V3_REQUIRED_PHASE_OPERATIONS
                )
            ]
            required_operation_coverage = []
            for population, required_operations, required_input_count in (
                (
                    "all_inputs",
                    _REPLAY_V3_REQUIRED_ALL_INPUT_OPERATIONS,
                    completed_input_count,
                ),
                (
                    "committed_inputs",
                    _REPLAY_V3_REQUIRED_COMMITTED_INPUT_OPERATIONS,
                    committed_input_count,
                ),
            ):
                for operation in sorted(required_operations):
                    if population == "all_inputs":
                        affected = sum(
                            int(values["affected_input_count"])
                            for (input_type, candidate), values in self._v3_operation_matrix.items()
                            if candidate == operation
                        )
                        unexpected = 0
                    else:
                        affected = sum(
                            int(values["affected_input_count"])
                            for (input_type, candidate), values in self._v3_operation_matrix.items()
                            if candidate == operation and input_type != "RUN_START"
                        )
                        unexpected = sum(
                            int(values["affected_input_count"])
                            for (input_type, candidate), values in self._v3_operation_matrix.items()
                            if candidate == operation and input_type == "RUN_START"
                        )
                    required_operation_coverage.append(
                        {
                            "affected_input_count": affected,
                            "missing_input_count": max(
                                0, required_input_count - affected
                            ),
                            "operation": operation,
                            "required_input_count": required_input_count,
                            "required_population": population,
                            "unexpected_population_input_count": unexpected,
                        }
                    )
            scope_affected_counts = {
                name: int(values["affected_input_count"])
                for name, values in self._v3_parent_scope_timings.items()
            }
            scope_run_start_affected_counts = {
                name: int(values["run_start_affected_input_count"])
                for name, values in self._v3_parent_scope_timings.items()
            }
            required_scope_coverage = []
            for population, required_scopes, required_input_count in (
                (
                    "all_inputs",
                    _REPLAY_V3_REQUIRED_ALL_INPUT_SCOPES,
                    completed_input_count,
                ),
                (
                    "committed_inputs",
                    _REPLAY_V3_REQUIRED_COMMITTED_INPUT_SCOPES,
                    committed_input_count,
                ),
            ):
                for scope in sorted(required_scopes):
                    total_affected = scope_affected_counts.get(scope, 0)
                    run_start_affected = (
                        scope_run_start_affected_counts.get(scope, 0)
                    )
                    if population == "all_inputs":
                        affected = total_affected
                        unexpected = 0
                    else:
                        affected = total_affected - run_start_affected
                        unexpected = run_start_affected
                    required_scope_coverage.append(
                        {
                            "affected_input_count": affected,
                            "missing_input_count": max(
                                0, required_input_count - affected
                            ),
                            "required_input_count": required_input_count,
                            "required_population": population,
                            "scope": scope,
                            "unexpected_population_input_count": unexpected,
                        }
                    )
            required_coverage_complete = (
                completed_input_count > 0
                and run_start_input_count == 1
                and all(
                    item["missing_input_count"] == 0
                    and item["unexpected_population_input_count"] == 0
                    for item in (
                        *required_operation_coverage,
                        *required_scope_coverage,
                    )
                )
                and all(
                    item["missing_span_count"] == 0
                    for item in required_phase_operation_coverage
                )
            )
            input_residual_wall_fraction = (
                residual / total_wall if total_wall else 0.0
            )
            required_scope_names = (
                _REPLAY_V3_REQUIRED_ALL_INPUT_SCOPES
                | _REPLAY_V3_REQUIRED_COMMITTED_INPUT_SCOPES
            )
            max_scope_unattributed_wall_fraction = max(
                (
                    (
                        int(values["unattributed_wall_nanoseconds"])
                        / int(values["inclusive_wall_nanoseconds"])
                        if int(values["inclusive_wall_nanoseconds"])
                        else 0.0
                    )
                    for name, values in self._v3_parent_scope_timings.items()
                    if name in required_scope_names
                ),
                default=0.0,
            )
            phase_unattributed_wall_fraction = (
                phase_residual / phase_wall if phase_wall else 0.0
            )
            unattributed_wall_within_tolerance = (
                input_residual_wall_fraction
                <= _REPLAY_V3_UNATTRIBUTED_WALL_FRACTION_TOLERANCE
                and max_scope_unattributed_wall_fraction
                <= _REPLAY_V3_UNATTRIBUTED_WALL_FRACTION_TOLERANCE
                and phase_unattributed_wall_fraction
                <= _REPLAY_V3_UNATTRIBUTED_WALL_FRACTION_TOLERANCE
            )
            funding = {
                **self._v3_funding_lookup,
                "eqp": list(cast(Sequence[object], self._v3_funding_lookup["eqp"])),
                "eqp_capture_phase_wall_share": (
                    int(self._v3_funding_lookup["eqp_capture_wall_nanoseconds"])
                    / phase_wall
                    if phase_wall
                    else 0.0
                ),
                "query_fingerprints": sorted(
                    cast(Sequence[str], self._v3_funding_lookup["query_fingerprints"])
                ),
            }
            phase_unattributed_excluding_known_diagnostics = phase_residual
            phase_capacity_for_known_diagnostics = max(
                0,
                phase_wall - exclusive_wall - phase_operation_wall,
            )
            phase_over_attributed_known_diagnostics = max(
                0,
                known_diagnostic_wall
                - phase_capacity_for_known_diagnostics,
            )
            instrumentation_complete = (
                conservation_delta <= _REPLAY_V3_SCOPE_ACCOUNTING_TOLERANCE_NS
                and phase_conservation_delta
                <= _REPLAY_V3_SCOPE_ACCOUNTING_TOLERANCE_NS
                and phase_wall > 0
                and phase_wall >= total_wall
                and scope_accounting_delta
                <= _REPLAY_V3_SCOPE_ACCOUNTING_TOLERANCE_NS
                and required_coverage_complete
                and unattributed_wall_within_tolerance
                and phase_over_attributed_known_diagnostics == 0
                and unknown_input_count == 0
                and unknown_operation_span_count == 0
                and not self._v3_scope_stack
                and not self._v3_observer_stack
            )
            return {
                "accounting_tolerance_nanoseconds": (
                    _REPLAY_V3_SCOPE_ACCOUNTING_TOLERANCE_NS
                ),
                "clock_unit": "nanoseconds",
                "completed_input_count": self._v3_completed_input_count,
                "completed_input_tail": [
                    dict(item) for item in self.completed_replay_input_tail
                ],
                "completed_input_thread_cpu_nanoseconds": self._v3_completed_input_cpu_ns,
                "completed_input_wall_nanoseconds": total_wall,
                "conservation_delta_wall_nanoseconds": conservation_delta,
                "exclusive_operation_wall_nanoseconds": exclusive_wall,
                "exclusive_semantics": _REPLAY_V3_EXCLUSIVE_SEMANTICS,
                "funding_lookup": funding,
                "input_residual_wall_fraction": input_residual_wall_fraction,
                "input_residual_wall_nanoseconds": residual,
                "input_type_operation_matrix": matrix,
                "input_type_totals": totals,
                "instrumentation_complete": instrumentation_complete,
                "instrumentation_mode": "V3",
                "open_observer_span_count": len(self._v3_observer_stack),
                "open_parent_scope_count": len(self._v3_scope_stack),
                "operation_taxonomy": sorted(_REPLAY_V3_OPERATIONS),
                "over_attributed_operation_wall_nanoseconds": over_attributed,
                "outside_completed_inputs_wall_nanoseconds": (
                    outside_completed_inputs
                ),
                "parent_scopes": parents,
                "phase_known_diagnostic_wall_nanoseconds": known_diagnostic_wall,
                "phase_conservation_delta_wall_nanoseconds": (
                    phase_conservation_delta
                ),
                "phase_operation_taxonomy": sorted(
                    _REPLAY_V3_PHASE_OPERATIONS
                ),
                "phase_operation_timings": phase_operations,
                "phase_operation_wall_nanoseconds": phase_operation_wall,
                "phase_over_attributed_operation_wall_nanoseconds": (
                    phase_over_attributed
                ),
                "phase_unattributed_excluding_known_diagnostics_wall_nanoseconds": (
                    phase_unattributed_excluding_known_diagnostics
                ),
                "phase_unattributed_wall_fraction": (
                    phase_unattributed_wall_fraction
                ),
                "phase_unattributed_wall_nanoseconds": phase_residual,
                "phase_over_attributed_known_diagnostic_wall_nanoseconds": (
                    phase_over_attributed_known_diagnostics
                ),
                "projection_history_sizes": dict(self.projection_history_sizes),
                "replay_store_phase_wall_nanoseconds": phase_wall,
                "required_coverage_complete": required_coverage_complete,
                "required_operation_coverage": required_operation_coverage,
                "required_phase_operation_coverage": (
                    required_phase_operation_coverage
                ),
                "required_scope_coverage": required_scope_coverage,
                "scope_taxonomy": [
                    {"name": name, "parent": parent}
                    for name, parent in sorted(_REPLAY_V3_SCOPE_PARENTS.items())
                ],
                "slowest_completed_inputs": [
                    dict(item) for item in self.slowest_completed_replay_inputs
                ],
                "unattributed_wall_fraction_tolerance": (
                    _REPLAY_V3_UNATTRIBUTED_WALL_FRACTION_TOLERANCE
                ),
                "unattributed_wall_within_tolerance": (
                    unattributed_wall_within_tolerance
                ),
                "max_scope_unattributed_wall_fraction": (
                    max_scope_unattributed_wall_fraction
                ),
                "unknown_input_count": unknown_input_count,
                "unknown_operation_span_count": unknown_operation_span_count,
                "version": _REPLAY_TIMING_V3_VERSION,
            }

    def _append_current_query_fingerprint_locked(self, query_shape: str) -> str:
        fingerprint = hashlib.sha256(
            f"hyperlab-replay-query-shape:{query_shape}:v1".encode("ascii")
        ).hexdigest()
        current = self._current_replay_input
        if current is not None:
            fingerprints = cast(list[str], current["query_fingerprints"])
            if (
                fingerprint not in fingerprints
                and len(fingerprints) < _REPLAY_V3_QUERY_FINGERPRINT_LIMIT
            ):
                fingerprints.append(fingerprint)
        return fingerprint

    def begin_funding_lookup(self) -> None:
        if self.instrumentation_mode == "V3":
            self._v3_funding_lookup_depth += 1

    def finish_funding_lookup(self) -> None:
        if self.instrumentation_mode != "V3":
            return
        self._v3_funding_lookup_depth -= 1
        if self._v3_funding_lookup_depth < 0:
            raise RuntimeError("funding lookup observation depth underflow")

    @property
    def funding_lookup_active(self) -> bool:
        return self.instrumentation_mode == "V3" and self._v3_funding_lookup_depth > 0

    def observe_historical_replay(
        self,
        operation: str,
        state: str,
        metadata: Mapping[str, object],
    ) -> None:
        if self.instrumentation_mode != "V3":
            return
        mapped = {
            "filtered_input_fetch": "funding_sqlite_fetch",
            "filtered_input_query_prepare": "funding_query_prepare",
            "filtered_input_row_reconstruct": "funding_reconstruct_canonicalize",
            "filtered_input_sqlite_execute": "funding_sqlite_execute",
            "validation_alert_comparison": "validation_alert_comparison",
            "validation_alert_expected_canonicalization": (
                "validation_alert_expected_canonicalization"
            ),
            "validation_alert_supplied_canonicalization": (
                "validation_alert_supplied_canonicalization"
            ),
            "validation_event_apply": "validation_apply_events",
            "validation_failure_diagnostic": "validation_failure_diagnostics",
            "validation_ledger_comparison": "validation_ledger_comparison",
            "validation_ledger_expected_canonicalization": (
                "validation_ledger_expected_canonicalization"
            ),
            "validation_ledger_reconstruction": "validation_expected_ledger",
            "validation_ledger_supplied_canonicalization": (
                "validation_ledger_supplied_canonicalization"
            ),
            "validation_projection_canonicalization": (
                "validation_projection_canonicalization"
            ),
            "validation_projection_comparison": "validation_projection_comparison",
            "validation_projection_record_decode": "validation_projection_decode",
            "validation_projection_reconstruction": (
                "validation_projection_reconstruction"
            ),
            "validation_projection_sqlite_load": "validation_projection_query",
            "validation_state_reconstruction": (
                "validation_projection_reconstruction"
            ),
        }.get(operation, "UNKNOWN")
        if state == "begin":
            with self._replay_timing_lock:
                if operation == "filtered_input_query_prepare":
                    after = metadata.get("after_commit_sequence")
                    parameter_count = metadata.get("parameter_count")
                    query_shape = metadata.get("query_shape")
                    if (
                        not _is_nonnegative_integer(after)
                        or parameter_count != 3
                        or query_shape
                        != "paper_inbox_run_after_commit_input_type_ordered"
                    ):
                        raise RuntimeError(
                            "funding lookup observer metadata is invalid"
                        )
                    after_sequence = after
                    funding = self._v3_funding_lookup
                    funding["lookup_count"] = int(funding["lookup_count"]) + 1
                    funding["requested_after_commit_sequence_max"] = max(
                        int(funding["requested_after_commit_sequence_max"]),
                        after_sequence,
                    )
                    current_min = funding["requested_after_commit_sequence_min"]
                    funding["requested_after_commit_sequence_min"] = (
                        after_sequence
                        if current_min is None
                        else min(cast(int, current_min), after_sequence)
                    )
                    fingerprint = self._append_current_query_fingerprint_locked(
                        query_shape
                    )
                    fingerprints = cast(
                        list[str], funding["query_fingerprints"]
                    )
                    if (
                        fingerprint not in fingerprints
                        and len(fingerprints)
                        < _REPLAY_V3_QUERY_FINGERPRINT_LIMIT
                    ):
                        fingerprints.append(fingerprint)
                    current = self._current_replay_input
                    if current is not None:
                        detail = current["funding_lookup"]
                        if not isinstance(detail, dict):
                            detail = {
                                "after_commit_sequence_max": after_sequence,
                                "after_commit_sequence_min": after_sequence,
                                "lookup_count": 0,
                                "max_historical_distance": 0,
                                "parameter_count": parameter_count,
                                "payload_characters": 0,
                                "query_fingerprints": [],
                                "returned_commit_sequence_max": 0,
                                "rows_returned": 0,
                                "selected_index_name": funding[
                                    "selected_index_name"
                                ],
                            }
                            current["funding_lookup"] = detail
                        detail["lookup_count"] = cast(
                            int, detail["lookup_count"]
                        ) + 1
                        detail["after_commit_sequence_max"] = max(
                            cast(int, detail["after_commit_sequence_max"]),
                            after_sequence,
                        )
                        detail["after_commit_sequence_min"] = min(
                            cast(int, detail["after_commit_sequence_min"]),
                            after_sequence,
                        )
                        detail_fingerprints = cast(
                            list[str], detail["query_fingerprints"]
                        )
                        if (
                            fingerprint not in detail_fingerprints
                            and len(detail_fingerprints)
                            < _REPLAY_V3_QUERY_FINGERPRINT_LIMIT
                        ):
                            detail_fingerprints.append(fingerprint)
                elif operation == "validation_projection_sqlite_load":
                    self._append_current_query_fingerprint_locked(
                        "paper_projections_payload_by_run_id"
                    )
                self._v3_observer_stack.append(
                    (operation, self._v3_active_replay_operation)
                )
            self.transition_replay_operation(mapped)
            return
        if state != "end":
            raise RuntimeError("invalid historical replay observer state")
        with self._replay_timing_lock:
            if not self._v3_observer_stack:
                raise RuntimeError("historical replay observer stack underflow")
            active_operation, previous = self._v3_observer_stack.pop()
            if active_operation != operation:
                raise RuntimeError("historical replay observer nesting mismatch")
            if operation == "filtered_input_row_reconstruct":
                sequence = metadata.get("commit_sequence")
                payload_characters = metadata.get("payload_json_characters")
                if (
                    not _is_nonnegative_integer(sequence)
                    or sequence == 0
                    or not _is_nonnegative_integer(payload_characters)
                ):
                    raise RuntimeError(
                        "funding lookup row metadata is invalid"
                    )
                funding = self._v3_funding_lookup
                after_min = funding["requested_after_commit_sequence_min"]
                after_sequence = cast(int, after_min) if after_min is not None else 0
                distance = max(0, sequence - after_sequence)
                funding["rows_returned"] = int(funding["rows_returned"]) + 1
                funding["payload_characters"] = (
                    int(funding["payload_characters"])
                    + payload_characters
                )
                funding["max_historical_distance"] = max(
                    int(funding["max_historical_distance"]),
                    distance,
                )
                current = self._current_replay_input
                if current is not None and isinstance(
                    current["funding_lookup"], dict
                ):
                    detail = cast(
                        dict[str, object], current["funding_lookup"]
                    )
                    detail["rows_returned"] = cast(
                        int, detail["rows_returned"]
                    ) + 1
                    detail["payload_characters"] = cast(
                        int, detail["payload_characters"]
                    ) + payload_characters
                    detail["returned_commit_sequence_max"] = max(
                        cast(int, detail["returned_commit_sequence_max"]),
                        sequence,
                    )
                    detail["max_historical_distance"] = max(
                        cast(int, detail["max_historical_distance"]),
                        distance,
                    )
        self.transition_replay_operation(previous)

    def observe_funding_record(
        self,
        record: object,
        *,
        after_commit_sequence: int,
    ) -> None:
        if self.instrumentation_mode != "V3":
            return
        sequence = int(cast(Any, record).commit_sequence)
        if sequence <= after_commit_sequence:
            raise RuntimeError("funding lookup returned an invalid sequence")

    def capture_funding_eqp(self, path: Path, *, run_id: str) -> None:
        if self.instrumentation_mode != "V3":
            return
        if self._v3_funding_plan_captured:
            raise RuntimeError("funding EQP was already captured")
        started_cpu_ns = thread_time_ns()
        started_wall_ns = perf_counter_ns()
        captured = _capture_sanitized_funding_eqp(path, run_id=run_id)
        captured["eqp_capture_connection_count"] = 1
        captured["eqp_capture_thread_cpu_nanoseconds"] = max(
            0, thread_time_ns() - started_cpu_ns
        )
        captured["eqp_capture_wall_nanoseconds"] = max(
            0, perf_counter_ns() - started_wall_ns
        )
        with self._replay_timing_lock:
            self._v3_funding_lookup.update(captured)
            selected_index = captured["selected_index_name"]
            for collection in (
                self.completed_replay_input_tail,
                self.slowest_completed_replay_inputs,
            ):
                for item in collection:
                    detail = item.get("funding_lookup")
                    if isinstance(detail, dict):
                        detail["selected_index_name"] = selected_index
            self._v3_funding_plan_captured = True
    def emit(self, event: str, **payload: object) -> None:
        with self._emit_lock:
            self.sequence += 1
            _emit(
                {
                    "elapsed_seconds": perf_counter() - self.started_wall,
                    "event": event,
                    "sequence": self.sequence,
                    **payload,
                }
            )

    def _replay_diagnostic_snapshots(
        self,
    ) -> tuple[dict[str, object], dict[str, object] | None]:
        with self._replay_timing_lock:
            progress = self.replay_progress_snapshot()
            timing: dict[str, object] | None = None
            if self.instrumentation_mode == "V2":
                timing = self.replay_timing_snapshot()
            elif self.instrumentation_mode == "V3":
                timing = self.replay_timing_v3_snapshot()
            return progress, timing

    def heartbeat(self) -> None:
        phase = self.active_phase
        started = self.phase_starts.get(phase)
        peak_rss, peak_source = _peak_rss_bytes()
        progress, timing = self._replay_diagnostic_snapshots()
        payload: dict[str, object] = {
            "phase": phase,
            "phase_cpu_seconds": (process_time() - started[1]) if started is not None else None,
            "phase_wall_seconds": (perf_counter() - started[0]) if started is not None else None,
            "peak_rss_bytes": peak_rss,
            "peak_rss_source": peak_source,
            "replay_progress": progress,
            "rows_observed": sum(
                self.counters[f"integrity_rows.{name}"] for name in _ROW_HOOKS
            ),
            "target_commits": progress["completed_target_commits"],
        }
        if self.instrumentation_mode == "V2":
            payload["replay_timing_v2"] = timing
        elif self.instrumentation_mode == "V3":
            payload["replay_timing_v3"] = timing
        self.emit("phase_heartbeat", **payload)

    def start_phase(self, name: str) -> None:
        if name not in self.phase_starts:
            if self.instrumentation_mode == "V3" and name == "replay_store_generation":
                with self._replay_timing_lock:
                    self._v3_replay_phase_started_wall_ns = perf_counter_ns()
                    self._v3_replay_phase_wall_ns = None
            self.phase_starts[name] = (perf_counter(), process_time(), self.counters.copy())
            self.emit("phase_started", phase=name)

    def finish_phase(self, name: str, status: str = "completed") -> None:
        started = self.phase_starts.pop(name, None)
        if started is None:
            return
        started_wall, started_cpu, before = started
        delta = self.counters.copy()
        delta.subtract(before)
        peak_rss, peak_source = _peak_rss_bytes()
        timing: dict[str, object] = {
            "cpu_seconds": process_time() - started_cpu,
            "counters": {key: value for key, value in sorted(delta.items()) if value},
            "peak_rss_bytes": peak_rss,
            "peak_rss_source": peak_source,
            "status": status,
            "wall_seconds": perf_counter() - started_wall,
        }
        self.phase_timings[name] = timing
        if self.instrumentation_mode == "V3" and name == "replay_store_generation":
            with self._replay_timing_lock:
                started_wall_ns = self._v3_replay_phase_started_wall_ns
                if started_wall_ns is not None:
                    self._v3_replay_phase_wall_ns = max(
                        0,
                        perf_counter_ns() - started_wall_ns,
                    )
        self.emit("phase_finished", phase=name, **timing)

    def add(self, key: str, value: int = 1, *, progress: bool = False) -> None:
        self.counters[key] += value
        self.phase_counters[self.active_phase][key] += value
        if not progress:
            return
        self.progress_units += value
        if self.progress_units < self.next_progress:
            return
        phase_started = self.phase_starts.get(self.active_phase)
        self.emit(
            "phase_progress",
            phase=self.active_phase,
            phase_cpu_seconds=(process_time() - phase_started[1]) if phase_started is not None else None,
            phase_wall_seconds=(perf_counter() - phase_started[0]) if phase_started is not None else None,
            progress_units=self.progress_units,
            rows_observed=sum(
                count for name, count in self.counters.items() if name.startswith("integrity_rows.")
            ),
            target_commits=self.counters["target_append_transaction_count"],
        )
        self.next_progress += self.progress_every_rows

    def observe_integrity(self, name: str, size: int) -> None:
        if name in _ROW_HOOKS:
            self.add(f"integrity_rows.{name}", size, progress=True)
            return
        key = f"integrity_buffer_peak.{name}"
        self.counters[key] = max(self.counters[key], size)
        phase = self.phase_counters[self.active_phase]
        phase[key] = max(phase[key], size)

    def observe_json(self, value: str, label: str) -> None:
        label_family = label.partition(" ")[0].lower().replace("-", "_")
        self.add("json_decode_count", progress=True)
        self.add("json_input_characters", len(value))
        self.add(f"json_decode_family.{label_family}")

    def observe_history(self, row: sqlite3.Row, decoded: str) -> None:
        keys = set(row.keys())
        codec = str(row["payload_codec"]) if "payload_codec" in keys else "json"
        raw = row["payload_zlib"] if "payload_zlib" in keys else None
        stored = (
            len(bytes(raw))
            if isinstance(raw, (bytes, bytearray, memoryview))
            else len(str(row["payload_json"]))
        )
        self.add("projection_history_decode_count")
        self.add(f"projection_history_decode_codec.{codec}")
        self.add("projection_history_stored_bytes_read", stored)
        self.add("projection_history_decoded_characters", len(decoded))

    def attach_source(self, _connection: sqlite3.Connection) -> None:
        self.source_connection_count += 1

    def attach_target(self, connection: sqlite3.Connection) -> None:
        if all(connection is not existing for existing in self.target_connections):
            self.target_connections.append(connection)

    def summary(self) -> dict[str, object]:
        progress, timing = self._replay_diagnostic_snapshots()
        result: dict[str, object] = {
            "counters": dict(sorted(self.counters.items())),
            "instrumentation_mode": self.instrumentation_mode,
            "logical_row_counts_note": (
                "integrity row hooks and append arguments; not physical SQLite scans"
            ),
            "phase_counters": {
                phase: dict(sorted(values.items()))
                for phase, values in sorted(self.phase_counters.items())
            },
            "phase_timings": self.phase_timings,
            "replay_progress": progress,
            "sqlite_sql_text_tracing": "disabled_to_avoid_expanded_payload_materialization",
        }
        if self.instrumentation_mode == "V2":
            result["replay_timing_v2"] = timing
        elif self.instrumentation_mode == "V3":
            result["replay_timing_v3"] = timing
        return result


def _heartbeat_loop(profiler: ReplayProfiler, stopped: threading.Event) -> None:
    while not stopped.wait(5.0):
        profiler.heartbeat()


class ImmutableCopyPaperStore(PaperStore):
    def __init__(self, path: Path, profiler: ReplayProfiler) -> None:
        self.profiler = profiler
        super().__init__(path, initialize=False)

    def initialize(self) -> None:
        self.profiler.add("source_initialize_noop_count")

    def _connect(self) -> sqlite3.Connection:
        self.profiler.source_write_connection_attempts += 1
        raise RuntimeError("diagnostic source write connection is forbidden")

    def _read_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            f"{self.path.resolve().as_uri()}?mode=ro&immutable=1",
            uri=True,
            timeout=self._timeout_seconds,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA query_only=ON")
        query_only_row = connection.execute("PRAGMA query_only").fetchone()
        if query_only_row is None or int(query_only_row[0]) != 1:
            connection.close()
            raise RuntimeError("diagnostic source query_only verification failed")
        self.profiler.source_query_only_verified = True
        self.profiler.attach_source(connection)
        return connection


def _profile_replay(
    database_copy: Path,
    scratch_root: Path,
    run_id: str,
    progress_every_rows: int,
    instrumentation_mode: str = "V2",
) -> dict[str, object]:
    profiler = ReplayProfiler(
        progress_every_rows, instrumentation_mode=instrumentation_mode
    )
    source_store = ImmutableCopyPaperStore(database_copy, profiler)
    original_connect = PaperStore._connect
    original_create_run = PaperStore.create_run
    original_append_atomic = PaperStore.append_atomic
    original_inject = PaperStore._inject
    original_iter_inputs = PaperStore.iter_inputs
    original_observe = PaperStore._observe_integrity_buffer
    original_observe_descriptor = vars(PaperStore)["_observe_integrity_buffer"]
    original_inspect = PaperStore.inspect_integrity_readonly
    original_inspect_head = PaperStore.inspect_head_integrity_readonly
    original_verify_integrity = PaperStore.verify_integrity
    original_projection_before = PaperStore.get_projection_before_received_at
    original_prepare_events = PaperStore._prepare_events
    original_prepare_ledger = PaperStore._prepare_ledger
    original_prepare_alerts = PaperStore._prepare_alerts
    original_validate_replayed_append = PaperStore._validate_replayed_append
    original_verify_input = PaperEngine.verify_input_replay
    original_ledger = PaperEngine._ledger_reconciliation_errors
    original_prefix = PaperEngine._verified_historical_replay_prefix
    original_commit = PaperEngine._commit
    original_reconcile = PaperEngine._reconcile
    original_canonical_record = store_module._canonical_record
    original_json = store_module._json_object
    original_history_json = store_module._projection_history_json
    original_history_storage = store_module._projection_history_storage
    original_runtime_replay = runtime_module.replay_projection
    original_temporary_store = engine_module._temporary_historical_replay_store
    original_temporary_directory = engine_module.TemporaryDirectory

    def instrumented_connect(store: PaperStore) -> sqlite3.Connection:
        connection = original_connect(store)
        if store.historical_replay_only:
            profiler.attach_target(connection)
        return connection

    def instrumented_inject(store: PaperStore, stage: str) -> None:
        original_inject(store, stage)
        if store.historical_replay_only and profiler.historical_append_active:
            profiler.transition_replay_operation(_APPEND_STAGE_OPERATIONS[stage])

    def instrumented_iter_inputs(
        store: PaperStore,
        target_run_id: str,
        *,
        input_type: str | None = None,
        after_commit_sequence: int = 0,
    ) -> Iterator[object]:
        records = iter(
            cast(Any, original_iter_inputs)(
                store,
                target_run_id,
                input_type=input_type,
                after_commit_sequence=after_commit_sequence,
            )
        )
        profile_source_replay = (
            store is source_store
            and input_type is None
            and after_commit_sequence == 0
            and profiler.active_phase == "replay_store_generation"
        )
        if profile_source_replay:
            while True:
                try:
                    with profiler.replay_operation("source_input_fetch"):
                        record = next(records)
                except StopIteration:
                    profiler.transition_replay_operation("replay_store_post_inputs")
                    return
                raw_payload = getattr(record, "payload", None)
                payload = raw_payload if isinstance(raw_payload, Mapping) else {}
                profiler.begin_replay_input(
                    commit_sequence=int(record.commit_sequence),
                    input_type=profiler.replay_input_type(payload),
                )
                if profiler.instrumentation_mode == "V3":
                    profiler.begin_parent_scope("business_reducer")
                    profiler.transition_replay_operation(
                        "input_business_logic"
                    )
                try:
                    yield record
                except BaseException:
                    profiler.finish_replay_input(completed=False)
                    raise
                else:
                    profiler.finish_replay_input(completed=True)
        elif (
            store.historical_replay_only
            and input_type is not None
            and profiler.active_phase == "replay_store_generation"
        ):
            lookup_operation = (
                "funding_lookup_residual"
                if profiler.instrumentation_mode == "V3"
                else "historical_filtered_input_lookup"
            )
            while True:
                profiler.begin_funding_lookup()
                try:
                    with (
                        profiler.replay_parent_scope("funding_lookup"),
                        profiler.replay_operation(lookup_operation),
                    ):
                        record = next(records)
                except StopIteration:
                    return
                finally:
                    profiler.finish_funding_lookup()
                profiler.observe_funding_record(
                    record,
                    after_commit_sequence=after_commit_sequence,
                )
                yield record
        else:
            yield from records

    def instrumented_create_run(
        store: PaperStore,
        *args: object,
        **kwargs: object,
    ) -> object:
        if store.historical_replay_only:
            with profiler.replay_operation("create_run"):
                result = cast(Any, original_create_run)(store, *args, **kwargs)
        else:
            result = cast(Any, original_create_run)(store, *args, **kwargs)
        if store.historical_replay_only:
            profiler.add("target_create_run_transaction_count")
            profiler.add("target_rows_written.paper_runs")
            profiler.add("target_rows_written.paper_projections")
            profiler.add("target_rows_written.paper_projection_history")
        return result

    def instrumented_append_atomic(
        store: PaperStore,
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
        profile_append = store.historical_replay_only
        operation = (
            profiler.replay_operation("append_input_canonicalization")
            if profile_append
            else nullcontext()
        )
        scope = (
            profiler.replay_parent_scope("store_append")
            if profile_append
            else nullcontext()
        )
        if profile_append:
            profiler.begin_historical_append()
            current = profiler._current_replay_input
            if profiler.instrumentation_mode == "V3" and current is not None:
                current["event_count"] = len(events)
                current["ledger_entry_count"] = len(ledger_entries)
                current["alert_count"] = len(alerts)
        try:
            with scope, operation:
                result = original_append_atomic(
                    store,
                    run_id,
                    input_id,
                    input_payload,
                    events,
                    ledger_entries,
                    projection,
                    alerts=alerts,
                    expected_sequence=expected_sequence,
                )
                if (
                    profile_append
                    and profiler.instrumentation_mode == "V3"
                ):
                    profiler.transition_replay_operation(
                        "diagnostic_post_commit_accounting"
                    )
                if profile_append and not result.idempotent:
                    transaction_ids = {
                        str(
                            entry.get("transaction_id")
                            if isinstance(entry, Mapping)
                            else getattr(entry, "transaction_id", None)
                        )
                        for entry in ledger_entries
                    }
                    transaction_ids.discard("None")
                    profiler.add("target_append_transaction_count", progress=True)
                    profiler.add("target_rows_written.paper_inbox")
                    profiler.add("target_rows_written.paper_events", result.appended_event_count)
                    profiler.add("target_rows_written.paper_ledger_transactions", len(transaction_ids))
                    profiler.add("target_rows_written.paper_ledger_entries", len(ledger_entries))
                    profiler.add("target_rows_written.paper_projection_history")
                    profiler.add("target_rows_written.paper_alerts", len(alerts))
                    profiler.add("target_rows_written.paper_commits")
                    profiler.add("target_rows_updated.paper_runs")
                    profiler.add("target_rows_updated.paper_projections")
            if profile_append and profiler.instrumentation_mode == "V3":
                profiler.transition_replay_operation("input_commit_return")
            return result
        finally:
            if profile_append:
                profiler.finish_historical_append()

    def timed_prepare_events(
        store: PaperStore,
        *args: object,
        **kwargs: object,
    ) -> object:
        if store.historical_replay_only and profiler.historical_append_active:
            with profiler.replay_operation("append_prepare_events"):
                return cast(Any, original_prepare_events)(store, *args, **kwargs)
        return cast(Any, original_prepare_events)(store, *args, **kwargs)

    def timed_prepare_ledger(
        store: PaperStore,
        *args: object,
        **kwargs: object,
    ) -> object:
        if store.historical_replay_only and profiler.historical_append_active:
            with profiler.replay_operation("append_prepare_ledger"):
                return cast(Any, original_prepare_ledger)(store, *args, **kwargs)
        return cast(Any, original_prepare_ledger)(store, *args, **kwargs)

    def timed_prepare_alerts(
        store: PaperStore,
        *args: object,
        **kwargs: object,
    ) -> object:
        if store.historical_replay_only and profiler.historical_append_active:
            with profiler.replay_operation("append_prepare_alerts"):
                return cast(Any, original_prepare_alerts)(store, *args, **kwargs)
        return cast(Any, original_prepare_alerts)(store, *args, **kwargs)

    def timed_validate_replayed_append(
        store: PaperStore,
        *args: object,
        **kwargs: object,
    ) -> object:
        if store.historical_replay_only and profiler.historical_append_active:
            operation_name = (
                "validation_residual"
                if profiler.instrumentation_mode == "V3"
                else "append_replay_validation"
            )
            with (
                profiler.replay_parent_scope("replay_validation"),
                profiler.replay_operation(operation_name),
            ):
                return cast(Any, original_validate_replayed_append)(
                    store, *args, **kwargs
                )
        return cast(Any, original_validate_replayed_append)(store, *args, **kwargs)

    def timed_verify_integrity(
        store: PaperStore,
        *args: object,
        **kwargs: object,
    ) -> object:
        if (
            store.historical_replay_only
            and profiler.active_phase == "replay_store_generation"
        ):
            with profiler.replay_operation("historical_full_integrity_verification"):
                return cast(Any, original_verify_integrity)(store, *args, **kwargs)
        return cast(Any, original_verify_integrity)(store, *args, **kwargs)

    def timed_inspect_head(
        store: PaperStore,
        *args: object,
        **kwargs: object,
    ) -> object:
        if (
            store.historical_replay_only
            and profiler.active_phase == "replay_store_generation"
        ):
            with profiler.replay_operation("historical_head_integrity"):
                return cast(Any, original_inspect_head)(store, *args, **kwargs)
        return cast(Any, original_inspect_head)(store, *args, **kwargs)

    def timed_projection_before(
        store: PaperStore,
        *args: object,
        **kwargs: object,
    ) -> object:
        if (
            store.historical_replay_only
            and profiler.active_phase == "replay_store_generation"
        ):
            with profiler.replay_operation("historical_projection_before_lookup"):
                return cast(Any, original_projection_before)(store, *args, **kwargs)
        return cast(Any, original_projection_before)(store, *args, **kwargs)

    def instrumented_canonical_record(
        value: object,
        *,
        label: str,
    ) -> tuple[dict[str, object], str, str]:
        if not profiler.historical_append_active:
            return cast(
                tuple[dict[str, object], str, str],
                original_canonical_record(value, label=label),
            )
        if label == "input_payload":
            operation = "append_input_canonicalization"
        elif label == "projection":
            operation = "append_projection_canonicalization"
        else:
            operation = "append_record_canonicalization"
        with profiler.replay_operation(operation):
            return cast(
                tuple[dict[str, object], str, str],
                original_canonical_record(value, label=label),
            )

    def instrumented_observe(_store: PaperStore, name: str, size: int) -> None:
        original_observe(name, size)
        profiler.observe_integrity(name, size)

    def instrumented_json(value: str, *, label: str) -> dict[str, object]:
        profiler.observe_json(value, label)
        if profiler.funding_lookup_active:
            with profiler.replay_operation("funding_json_decode"):
                return cast(
                    dict[str, object],
                    original_json(value, label=label),
                )
        return cast(dict[str, object], original_json(value, label=label))

    def instrumented_history_json(row: sqlite3.Row, *, label: str) -> str:
        try:
            decoded = original_history_json(row, label=label)
        except BaseException:
            profiler.add("projection_history_decode_failure_count")
            raise
        profiler.observe_history(row, decoded)
        return decoded

    def instrumented_history_storage(
        payload: Mapping[str, object],
        payload_json: str,
    ) -> tuple[str, bytes, str, str | None, str | None]:
        operation = (
            profiler.replay_operation("append_projection_history_storage")
            if profiler.historical_append_active
            else nullcontext()
        )
        with operation:
            result = original_history_storage(cast(Any, payload), payload_json)
        profiler.observe_projection_history_storage(
            projection_characters=len(payload_json),
            zlib_bytes=len(result[1]),
        )
        profiler.add("projection_history_encode_count")
        profiler.add("projection_history_encode_input_characters", len(payload_json))
        profiler.add("projection_history_encoded_zlib_bytes", len(result[1]))
        return result

    def timed_inspect(store: PaperStore, target_run_id: str) -> object:
        phase = "target_integrity" if store.historical_replay_only else "source_integrity"
        if store.historical_replay_only:
            profiler.transition_replay_operation(None)
            profiler.finish_phase("replay_store_generation")
        profiler.active_phase = phase
        profiler.start_phase(phase)
        try:
            return original_inspect(store, target_run_id)
        finally:
            status = "failed" if sys.exc_info()[0] is not None else "completed"
            profiler.finish_phase(phase, status)
            if store.historical_replay_only:
                profiler.target_integrity_complete = status == "completed"
                profiler.active_phase = "canonical_input_replay_post_integrity"
            else:
                profiler.active_phase = "top_level"

    def timed_runtime_replay(*args: object, **kwargs: object) -> PaperProjection:
        profiler.active_phase = "event_replay"
        profiler.start_phase("event_replay")
        try:
            return original_runtime_replay(*args, **kwargs)
        finally:
            status = "failed" if sys.exc_info()[0] is not None else "completed"
            profiler.finish_phase("event_replay", status)
            profiler.active_phase = "top_level"

    def timed_ledger(
        engine: PaperEngine,
        projection: PaperProjection,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> tuple[str, ...]:
        if engine.store.historical_replay_only:
            profiler.add("historical_ledger_reconciliation_count")
        if engine.store.historical_replay_only and profiler.target_integrity_complete:
            profiler.active_phase = "target_ledger_reconciliation"
            profiler.start_phase("target_ledger_reconciliation")
            try:
                return original_ledger(engine, projection, should_stop=should_stop)
            finally:
                status = "failed" if sys.exc_info()[0] is not None else "completed"
                profiler.finish_phase("target_ledger_reconciliation", status)
                if status == "completed":
                    profiler.active_phase = "final_exact_comparisons"
                    profiler.start_phase("final_exact_comparisons")
                    profiler.final_comparison_started = True
        return original_ledger(engine, projection, should_stop=should_stop)

    def counted_prefix(engine: PaperEngine) -> object:
        profiler.add("bounded_historical_prefix_certification_count")
        if (
            engine.store.historical_replay_only
            and profiler.active_phase == "replay_store_generation"
        ):
            with profiler.replay_operation("historical_prefix_certification"):
                return original_prefix(engine)
        return original_prefix(engine)

    def timed_commit(
        engine: PaperEngine,
        *args: object,
        **kwargs: object,
    ) -> object:
        if (
            engine.store.historical_replay_only
            and profiler.active_phase == "replay_store_generation"
        ):
            if profiler.instrumentation_mode == "V3":
                with profiler.replay_parent_scope("engine_commit"):
                    profiler.transition_replay_operation(
                        "input_commit_prepare"
                    )
                    result = cast(Any, original_commit)(
                        engine, *args, **kwargs
                    )
                    profiler.transition_replay_operation(
                        "input_commit_return"
                    )
                profiler.transition_replay_operation(
                    "input_result_return"
                )
                return result
            with profiler.replay_operation("engine_commit_prepare"):
                return cast(Any, original_commit)(
                    engine, *args, **kwargs
                )
        return cast(Any, original_commit)(engine, *args, **kwargs)

    def timed_reconcile(
        engine: PaperEngine,
        *args: object,
        **kwargs: object,
    ) -> object:
        if (
            engine.store.historical_replay_only
            and profiler.active_phase == "replay_store_generation"
        ):
            with profiler.replay_operation("historical_reconcile"):
                return cast(Any, original_reconcile)(engine, *args, **kwargs)
        return cast(Any, original_reconcile)(engine, *args, **kwargs)

    def timed_verify_input(
        engine: PaperEngine,
        *,
        _source_integrity: object | None = None,
    ) -> PaperProjection:
        profiler.active_phase = "canonical_input_replay_setup"
        profiler.start_phase("canonical_input_replay_total")
        source_run = engine.store.get_run(engine.run_id)
        tail_records: list[tuple[int, str]] = []
        tail_after = max(0, source_run.commit_sequence - _REPLAY_TAIL_LIMIT)
        for record in cast(Any, original_iter_inputs)(
            engine.store,
            engine.run_id,
            after_commit_sequence=tail_after,
        ):
            raw_payload = getattr(record, "payload", None)
            payload = raw_payload if isinstance(raw_payload, Mapping) else {}
            tail_records.append(
                (
                    int(record.commit_sequence),
                    profiler.replay_input_type(payload),
                )
            )
        first_record = tail_records[0] if tail_after == 0 and tail_records else None
        if tail_after > 0:
            first_records = iter(
                cast(Any, original_iter_inputs)(
                    engine.store,
                    engine.run_id,
                    after_commit_sequence=0,
                )
            )
            try:
                first_source_record = next(first_records, None)
            finally:
                close_first_records = getattr(first_records, "close", None)
                if callable(close_first_records):
                    close_first_records()
            if first_source_record is not None:
                raw_first_payload = getattr(first_source_record, "payload", None)
                first_payload = (
                    raw_first_payload
                    if isinstance(raw_first_payload, Mapping)
                    else {}
                )
                first_record = (
                    int(first_source_record.commit_sequence),
                    profiler.replay_input_type(first_payload),
                )
        if source_run.commit_sequence > 0 and first_record is None:
            raise RuntimeError("source replay prefix has no first input")
        profiler.set_source_replay_tail(
            expected_target_commits=source_run.commit_sequence,
            first_record=first_record,
            records=tail_records,
        )
        try:
            return original_verify_input(engine, _source_integrity=cast(Any, _source_integrity))
        finally:
            failed = sys.exc_info()[0] is not None
            profiler.finish_replay_input(completed=False)
            profiler.transition_replay_operation(None)
            if profiler.final_comparison_started:
                profiler.finish_phase("final_exact_comparisons", "failed" if failed else "completed")
                profiler.final_comparison_started = False
            profiler.finish_phase("replay_store_generation", "failed" if failed else "completed")
            profiler.finish_phase("canonical_input_replay_total", "failed" if failed else "completed")
            profiler.active_phase = "top_level"

    def owned_temporary_directory(*args: object, **kwargs: object) -> TemporaryDirectory[str]:
        kwargs["dir"] = str(scratch_root)
        return original_temporary_directory(*args, **kwargs)

    @contextmanager
    def captured_temporary_store() -> Iterator[PaperStore]:
        profiler.active_phase = "replay_store_generation"
        profiler.start_phase("replay_store_generation")
        profiler.transition_replay_operation("replay_store_setup")
        with original_temporary_store() as replay_store:
            profiler.target_database_path = replay_store.path
            profiler.target_initial_identity = _fresh_historical_store_identity(
                replay_store,
                original_connect(replay_store),
            )
            previous_observer: object | None = None
            if profiler.instrumentation_mode == "V3":
                previous_observer = cast(Any, replay_store)._set_historical_replay_observer(
                    profiler.observe_historical_replay
                )
                profiler.transition_replay_operation(None)
                profiler.capture_funding_eqp(
                    replay_store.path,
                    run_id=run_id,
                )
                profiler.transition_replay_operation("replay_store_setup")
            primary_error: BaseException | None = None
            try:
                yield replay_store
            except BaseException as error:
                primary_error = error
                raise
            finally:
                if profiler.instrumentation_mode == "V3":
                    cast(Any, replay_store)._set_historical_replay_observer(
                        cast(Any, previous_observer)
                    )
                if profiler.final_comparison_started:
                    profiler.finish_phase(
                        "final_exact_comparisons",
                        "failed" if primary_error is not None else "completed",
                    )
                    profiler.final_comparison_started = False
                try:
                    target_run = replay_store.get_run(run_id)
                    target_projection = replay_store.get_projection(run_id)
                    profiler.target_identity = {
                        "head_identity": list(target_run.head_identity),
                        "projection_hash": target_projection.canonical_hash,
                    }
                    if replay_store.path.exists():
                        profiler.target_database_bytes = replay_store.path.stat().st_size
                except BaseException as capture_error:
                    if primary_error is None:
                        raise
                    primary_error.add_note(
                        f"target diagnostic capture failed: {type(capture_error).__name__}: {capture_error}"
                    )

    PaperStore._connect = instrumented_connect
    PaperStore.create_run = instrumented_create_run
    PaperStore.append_atomic = instrumented_append_atomic
    PaperStore._inject = instrumented_inject
    PaperStore.iter_inputs = instrumented_iter_inputs
    PaperStore._observe_integrity_buffer = instrumented_observe
    PaperStore.inspect_integrity_readonly = timed_inspect
    PaperStore.inspect_head_integrity_readonly = timed_inspect_head
    PaperStore.verify_integrity = timed_verify_integrity
    PaperStore.get_projection_before_received_at = timed_projection_before
    PaperStore._prepare_events = timed_prepare_events
    PaperStore._prepare_ledger = timed_prepare_ledger
    PaperStore._prepare_alerts = timed_prepare_alerts
    PaperStore._validate_replayed_append = timed_validate_replayed_append
    PaperEngine.verify_input_replay = timed_verify_input
    PaperEngine._ledger_reconciliation_errors = timed_ledger
    PaperEngine._verified_historical_replay_prefix = counted_prefix
    PaperEngine._commit = timed_commit
    PaperEngine._reconcile = timed_reconcile
    store_module._canonical_record = instrumented_canonical_record
    store_module._json_object = instrumented_json
    store_module._projection_history_json = instrumented_history_json
    store_module._projection_history_storage = instrumented_history_storage
    runtime_module.replay_projection = timed_runtime_replay
    engine_module.TemporaryDirectory = owned_temporary_directory
    engine_module._temporary_historical_replay_store = captured_temporary_store
    heartbeat_stopped = threading.Event()
    heartbeat_thread: threading.Thread | None = None
    heartbeat_started = False
    try:
        heartbeat_thread = threading.Thread(
            target=_heartbeat_loop,
            args=(profiler, heartbeat_stopped),
            daemon=True,
        )
        heartbeat_thread.start()
        heartbeat_started = True
        started_wall = perf_counter()
        started_cpu = process_time()
        verification = replay_paper_run(source_store, run_id)
        replay_wall = perf_counter() - started_wall
        replay_cpu = process_time() - started_cpu
        source_run = source_store.get_run(run_id)
        source_projection = source_store.get_projection(run_id)
    finally:
        heartbeat_stopped.set()
        if heartbeat_started and heartbeat_thread is not None:
            heartbeat_thread.join(timeout=1)
        engine_module._temporary_historical_replay_store = original_temporary_store
        engine_module.TemporaryDirectory = original_temporary_directory
        runtime_module.replay_projection = original_runtime_replay
        store_module._projection_history_storage = original_history_storage
        store_module._projection_history_json = original_history_json
        store_module._json_object = original_json
        store_module._canonical_record = original_canonical_record
        PaperEngine._reconcile = original_reconcile
        PaperEngine._commit = original_commit
        PaperEngine._verified_historical_replay_prefix = original_prefix
        PaperEngine._ledger_reconciliation_errors = original_ledger
        PaperEngine.verify_input_replay = original_verify_input
        PaperStore._validate_replayed_append = original_validate_replayed_append
        PaperStore._prepare_alerts = original_prepare_alerts
        PaperStore._prepare_ledger = original_prepare_ledger
        PaperStore._prepare_events = original_prepare_events
        PaperStore.get_projection_before_received_at = original_projection_before
        PaperStore.verify_integrity = original_verify_integrity
        PaperStore.inspect_head_integrity_readonly = original_inspect_head
        PaperStore.inspect_integrity_readonly = original_inspect
        PaperStore._observe_integrity_buffer = cast(Any, original_observe_descriptor)
        PaperStore.iter_inputs = original_iter_inputs
        PaperStore._inject = original_inject
        PaperStore.append_atomic = original_append_atomic
        PaperStore.create_run = original_create_run
        PaperStore._connect = original_connect
        source_store.close()
    peak_rss, peak_source = _peak_rss_bytes()
    return {
        **verification.to_dict(),
        "authorizes_real_money": False,
        "bounded_historical_prefix_certification_count": profiler.counters[
            "bounded_historical_prefix_certification_count"
        ],
        "historical_ledger_reconciliation_count": profiler.counters["historical_ledger_reconciliation_count"],
        "peak_rss_bytes": peak_rss,
        "peak_rss_source": peak_source,
        "profile": profiler.summary(),
        "projection_history_decode_count": profiler.counters["projection_history_decode_count"],
        "replay_cpu_seconds": replay_cpu,
        "replay_wall_seconds": replay_wall,
        "source_head_identity": list(source_run.head_identity),
        "source_open_mode": _SOURCE_OPEN_MODE,
        "source_projection_hash": source_projection.canonical_hash,
        "source_query_only_verified": profiler.source_query_only_verified,
        "source_sqlite_connection_count": profiler.source_connection_count,
        "source_write_connection_attempts": profiler.source_write_connection_attempts,
        "target_database_bytes": profiler.target_database_bytes,
        "target_initial_identity": profiler.target_initial_identity,
        "target_head_identity": profiler.target_identity.get("head_identity"),
        "target_projection_hash": profiler.target_identity.get("projection_hash"),
        "target_paper_store_sqlite_connection_count": len(
            profiler.target_connections
        ),
        "target_sqlite_connection_count": (
            len(profiler.target_connections)
            + int(
                profiler._v3_funding_lookup[
                    "eqp_capture_connection_count"
                ]
            )
        ),
        "target_logical_transaction_counts": {
            "append_atomic": profiler.counters["target_append_transaction_count"],
            "create_run": profiler.counters["target_create_run_transaction_count"],
        },
    }


def _force_worker_timeout() -> None:
    os._exit(124)


def _worker_main(args: argparse.Namespace) -> int:
    expected_token = os.environ.pop(_WORKER_TOKEN_ENV, "")
    supplied_token = args._worker_token or ""
    if not expected_token or not supplied_token or not secrets.compare_digest(expected_token, supplied_token):
        _emit({"event": "worker_failed", "status": "REFUSED_UNSUPERVISED_WORKER"})
        return 2

    global _WORKER_PROTOCOL_TOKEN
    _WORKER_PROTOCOL_TOKEN = supplied_token
    worker_deadline = perf_counter() + args.wall_limit_seconds
    watchdog = threading.Timer(args.wall_limit_seconds, _force_worker_timeout)
    watchdog.daemon = True
    watchdog.start()
    try:
        try:
            database_copy, _forbidden_original, scratch_root = _resolve_inputs(args)
            with _hold_source_snapshot(database_copy):
                before = _fingerprint(database_copy, deadline=worker_deadline)
                if before.sha256 != args.expected_sha256:
                    raise DiagnosticRefusal(
                        "REFUSED_WORKER_SOURCE_HASH",
                        "worker source copy no longer matches the expected SHA-256",
                    )
                result = _profile_replay(
                    database_copy,
                    scratch_root,
                    args.run_id,
                    args.progress_every_rows,
                    instrumentation_mode=args.instrumentation_mode,
                )
                after = _fingerprint(database_copy, deadline=worker_deadline)
                if _sqlite_sidecars(database_copy):
                    raise DiagnosticRefusal(
                        "SOURCE_COPY_SQLITE_SIDECAR_APPEARED",
                        "a SQLite sidecar appeared beside the explicit source copy",
                    )
                if after.sha256 != before.sha256 or after.stat != before.stat:
                    raise DiagnosticRefusal(
                        "SOURCE_COPY_CHANGED",
                        "the explicit source copy changed inside the worker",
                    )
        except DiagnosticRefusal as error:
            _emit({"detail": error.detail, "event": "worker_failed", "status": error.status})
            return 2
        except BaseException as error:
            _emit(
                {
                    "detail": "canonical replay raised an exception",
                    "event": "worker_failed",
                    "exception_type": type(error).__name__,
                    "status": "DIAGNOSTIC_WORKER_FAILED",
                }
            )
            return 1
        _emit({**result, "event": "worker_result", "status": "WORKER_COMPLETE"})
        return 0
    finally:
        watchdog.cancel()


def _offer_worker_message(
    messages: queue.Queue[str | None],
    message: str,
) -> bool:
    try:
        messages.put_nowait(message)
        return True
    except queue.Full:
        while True:
            try:
                messages.get_nowait()
            except queue.Empty:
                break
        messages.put_nowait(_WORKER_OUTPUT_QUEUE_FULL)
        return False


def _forward_stdout(stream: Any, messages: queue.Queue[str | None]) -> None:
    try:
        while True:
            line = stream.readline(_MAX_WORKER_LINE_CHARACTERS + 2)
            if not line:
                break
            if len(line) > _MAX_WORKER_LINE_CHARACTERS:
                if not _offer_worker_message(messages, _WORKER_LINE_TOO_LONG):
                    return
                while line and not line.endswith(("\r", "\n")):
                    line = stream.readline(_MAX_WORKER_LINE_CHARACTERS + 2)
                continue
            if not _offer_worker_message(messages, line.rstrip("\r\n")):
                return
    finally:
        try:
            messages.put_nowait(None)
        except queue.Full:
            while True:
                try:
                    messages.get_nowait()
                except queue.Empty:
                    break
            messages.put_nowait(_WORKER_OUTPUT_QUEUE_FULL)
            messages.put_nowait(None)


class _ReplayTimingEntry(TypedDict):
    max_wall_seconds: int | float
    span_count: int
    thread_cpu_seconds: int | float
    wall_seconds: int | float


class _ReplayOperationSpan(TypedDict):
    span_count: int
    thread_cpu_seconds: int | float
    wall_seconds: int | float


class _ReplayInputReference(TypedDict):
    commit_sequence: int
    input_type: str


def _protocol_failure(detail: str) -> dict[str, object]:
    return {
        "detail": detail,
        "status": "DIAGNOSTIC_WORKER_PROTOCOL_FAILURE",
    }


def _is_nonnegative_integer(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_nonnegative_number(value: object) -> TypeGuard[int | float]:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and value >= 0
    )


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and _HEX_64.fullmatch(value) is not None


def _is_counter_mapping(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and len(value) <= 200
        and all(
            isinstance(name, str)
            and len(name) <= 128
            and _SAFE_COUNTER_NAME.fullmatch(name) is not None
            and _is_nonnegative_integer(count)
            for name, count in value.items()
        )
    )


def _is_peak_rss(peak_bytes: object, source: object) -> bool:
    return (peak_bytes is None or _is_nonnegative_integer(peak_bytes)) and source in _PEAK_RSS_SOURCES


def _is_replay_timing_entry(value: object) -> TypeGuard[_ReplayTimingEntry]:
    return (
        isinstance(value, Mapping)
        and set(value)
        == {
            "max_wall_seconds",
            "span_count",
            "thread_cpu_seconds",
            "wall_seconds",
        }
        and _is_nonnegative_integer(value.get("span_count"))
        and value.get("span_count") != 0
        and _is_nonnegative_number(value.get("max_wall_seconds"))
        and _is_nonnegative_number(value.get("thread_cpu_seconds"))
        and _is_nonnegative_number(value.get("wall_seconds"))
        and float(value["max_wall_seconds"]) <= float(value["wall_seconds"])
    )


def _is_replay_input_reference(value: object) -> TypeGuard[_ReplayInputReference]:
    return (
        isinstance(value, Mapping)
        and set(value) == {"commit_sequence", "input_type"}
        and _is_nonnegative_integer(value.get("commit_sequence"))
        and value.get("commit_sequence") != 0
        and value.get("input_type") in _REPLAY_V3_INPUT_TYPES
    )


def _is_replay_timed_input(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value)
        == {
            "commit_sequence",
            "input_type",
            "thread_cpu_seconds",
            "wall_seconds",
        }
        and _is_nonnegative_integer(value.get("commit_sequence"))
        and value.get("commit_sequence") != 0
        and value.get("input_type") in _REPLAY_INPUT_TYPES
        and _is_nonnegative_number(value.get("thread_cpu_seconds"))
        and _is_nonnegative_number(value.get("wall_seconds"))
    )


def _is_replay_completed_input(value: object) -> bool:
    if (
        not isinstance(value, Mapping)
        or set(value)
        != {
            "commit_sequence",
            "input_type",
            "operations",
            "projection_characters",
            "thread_cpu_seconds",
            "wall_seconds",
            "zlib_bytes",
        }
        or not _is_nonnegative_integer(value.get("commit_sequence"))
        or value.get("commit_sequence") == 0
        or value.get("input_type") not in _REPLAY_INPUT_TYPES
        or not _is_nonnegative_integer(value.get("projection_characters"))
        or value.get("projection_characters") == 0
        or not _is_nonnegative_number(value.get("thread_cpu_seconds"))
        or not _is_nonnegative_number(value.get("wall_seconds"))
        or not _is_nonnegative_integer(value.get("zlib_bytes"))
        or value.get("zlib_bytes") == 0
    ):
        return False
    operations = value.get("operations")
    if not (
        isinstance(operations, Mapping)
        and len(operations) <= len(_REPLAY_OPERATIONS)
        and all(
            operation in _REPLAY_OPERATIONS
            and isinstance(timing, Mapping)
            and set(timing) == {"span_count", "thread_cpu_seconds", "wall_seconds"}
            and _is_nonnegative_integer(timing.get("span_count"))
            and timing.get("span_count") != 0
            and _is_nonnegative_number(timing.get("thread_cpu_seconds"))
            and _is_nonnegative_number(timing.get("wall_seconds"))
            for operation, timing in operations.items()
        )
    ):
        return False
    required_operations = {"input_dispatch"}
    if value.get("input_type") == "RECONCILE":
        required_operations.update(
            {
                "historical_head_integrity",
                "historical_prefix_certification",
                "historical_reconcile",
            }
        )
    return required_operations.issubset(operations)

def _is_replay_progress(
    value: object,
    *,
    terminal: bool,
    target_commits: object,
    expected_commits: object | None = None,
) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "active_input",
        "completed_target_commits",
        "expected_target_commits",
        "input_type_counts",
        "last_completed_input",
        "projection_history_sizes",
        "remaining_target_commits",
        "source_first_input",
        "source_tail",
        "target_database_bytes",
    }:
        return False
    expected = value.get("expected_target_commits")
    completed = value.get("completed_target_commits")
    remaining = value.get("remaining_target_commits")
    if not all(
        _is_nonnegative_integer(item)
        for item in (expected, completed, remaining)
    ):
        return False
    expected_count = cast(int, expected)
    completed_count = cast(int, completed)
    if (
        completed_count > expected_count
        or remaining != expected_count - completed_count
        or completed != target_commits
        or (expected_commits is not None and expected != expected_commits)
    ):
        return False
    target_bytes = value.get("target_database_bytes")
    if target_bytes is not None and not _is_nonnegative_integer(target_bytes):
        return False
    source_first = value.get("source_first_input")
    if expected_count == 0:
        if source_first is not None:
            return False
    elif (
        not _is_replay_input_reference(source_first)
        or source_first.get("commit_sequence") != 1
        or source_first.get("input_type") != "RUN_START"
    ):
        return False
    tail = value.get("source_tail")
    if (
        not isinstance(tail, list)
        or len(tail) != min(expected_count, _REPLAY_TAIL_LIMIT)
        or any(not _is_replay_input_reference(item) for item in tail)
    ):
        return False
    tail_sequences = [int(item["commit_sequence"]) for item in tail]
    if tail_sequences != list(
        range(max(1, expected_count - len(tail) + 1), expected_count + 1)
    ):
        return False
    last_completed = value.get("last_completed_input")
    if (
        last_completed is not None
        and not _is_replay_input_reference(last_completed)
    ):
        return False
    last_sequence = (
        int(last_completed["commit_sequence"])
        if isinstance(last_completed, Mapping)
        else 0
    )
    if last_sequence > completed_count:
        return False
    type_counts = value.get("input_type_counts")
    if (
        not isinstance(type_counts, Mapping)
        or len(type_counts) > len(_REPLAY_V3_INPUT_TYPES)
        or any(
            input_type not in _REPLAY_V3_INPUT_TYPES
            or not _is_nonnegative_integer(count)
            or count == 0
            for input_type, count in type_counts.items()
        )
        or sum(cast(int, count) for count in type_counts.values())
        != last_sequence
    ):
        return False
    active = value.get("active_input")
    if active is not None and (
        not isinstance(active, Mapping)
        or set(active)
        != {"commit_sequence", "input_type", "wall_nanoseconds"}
        or not _is_nonnegative_integer(active.get("commit_sequence"))
        or active.get("commit_sequence") == 0
        or active.get("input_type") not in _REPLAY_V3_INPUT_TYPES
        or not _is_nonnegative_integer(active.get("wall_nanoseconds"))
        or active.get("commit_sequence") != last_sequence + 1
        or completed_count not in {last_sequence, active.get("commit_sequence")}
    ):
        return False
    bootstrap_pending = (
        active is None and completed_count == 1 and last_sequence == 0
    )
    if (
        active is None
        and completed_count != last_sequence
        and not bootstrap_pending
    ):
        return False
    sizes = value.get("projection_history_sizes")
    if (
        not isinstance(sizes, Mapping)
        or set(sizes)
        != {
            "latest_commit_sequence",
            "latest_projection_characters",
            "latest_zlib_bytes",
            "max_projection_characters",
            "max_zlib_bytes",
        }
        or any(not _is_nonnegative_integer(item) for item in sizes.values())
        or sizes["latest_commit_sequence"] > expected_count
        or sizes["latest_projection_characters"]
        > sizes["max_projection_characters"]
        or sizes["latest_zlib_bytes"] > sizes["max_zlib_bytes"]
    ):
        return False
    if not terminal:
        return True
    return (
        active is None
        and completed_count == expected_count
        and remaining == 0
        and expected_count > 0
        and isinstance(last_completed, Mapping)
        and last_sequence == expected_count
        and bool(tail)
        and dict(last_completed) == dict(tail[-1])
        and _is_nonnegative_integer(target_bytes)
        and target_bytes != 0
        and sizes["latest_commit_sequence"] == expected_count
        and sum(cast(int, count) for count in type_counts.values())
        == expected_count
    )

def _is_v3_funding_detail(value: object) -> bool:
    if value is None:
        return True
    if not isinstance(value, Mapping) or set(value) != {
        "after_commit_sequence_max",
        "after_commit_sequence_min",
        "lookup_count",
        "max_historical_distance",
        "parameter_count",
        "payload_characters",
        "query_fingerprints",
        "returned_commit_sequence_max",
        "rows_returned",
        "selected_index_name",
    }:
        return False
    integer_names = (
        "after_commit_sequence_max",
        "after_commit_sequence_min",
        "lookup_count",
        "max_historical_distance",
        "parameter_count",
        "payload_characters",
        "returned_commit_sequence_max",
        "rows_returned",
    )
    if (
        any(
            not _is_nonnegative_integer(value.get(name))
            for name in integer_names
        )
        or value.get("lookup_count") == 0
        or value.get("parameter_count") != 3
        or cast(int, value["after_commit_sequence_min"])
        > cast(int, value["after_commit_sequence_max"])
        or cast(int, value["rows_returned"])
        > cast(int, value["lookup_count"])
    ):
        return False
    selected_index = value.get("selected_index_name")
    if (
        selected_index is not None
        and (
            not isinstance(selected_index, str)
            or _SAFE_SQLITE_IDENTIFIER.fullmatch(selected_index) is None
        )
    ):
        return False
    fingerprints = value.get("query_fingerprints")
    return (
        isinstance(fingerprints, list)
        and len(fingerprints) <= _REPLAY_V3_QUERY_FINGERPRINT_LIMIT
        and len(set(fingerprints)) == len(fingerprints)
        and fingerprints == sorted(fingerprints)
        and all(_is_sha256(item) for item in fingerprints)
    )


def _is_v3_detailed_input(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "alert_count",
        "commit_sequence",
        "event_count",
        "funding_lookup",
        "input_type",
        "ledger_entry_count",
        "operations",
        "projection_after_characters",
        "projection_before_characters",
        "projection_characters",
        "query_fingerprints",
        "scopes",
        "thread_cpu_nanoseconds",
        "wall_nanoseconds",
        "zlib_bytes",
    }:
        return False
    if (
        not _is_nonnegative_integer(value.get("commit_sequence"))
        or value.get("commit_sequence") == 0
        or value.get("input_type") not in _REPLAY_V3_INPUT_TYPES
        or any(
            not _is_nonnegative_integer(value.get(name))
            for name in (
                "alert_count",
                "event_count",
                "ledger_entry_count",
                "projection_after_characters",
                "projection_before_characters",
                "projection_characters",
                "thread_cpu_nanoseconds",
                "wall_nanoseconds",
                "zlib_bytes",
            )
        )
        or not _is_v3_funding_detail(value.get("funding_lookup"))
    ):
        return False
    fingerprints = value.get("query_fingerprints")
    if (
        not isinstance(fingerprints, list)
        or len(fingerprints) > _REPLAY_V3_QUERY_FINGERPRINT_LIMIT
        or len(set(fingerprints)) != len(fingerprints)
        or fingerprints != sorted(fingerprints)
        or any(not _is_sha256(item) for item in fingerprints)
    ):
        return False
    operations = value.get("operations")
    if (
        not isinstance(operations, list)
        or len(operations) > len(_REPLAY_V3_OPERATIONS)
    ):
        return False
    names: set[str] = set()
    for item in operations:
        if (
            not isinstance(item, Mapping)
            or set(item)
            != {
                "operation",
                "span_count",
                "thread_cpu_nanoseconds",
                "wall_nanoseconds",
            }
            or item.get("operation") not in _REPLAY_V3_OPERATIONS
            or not _is_nonnegative_integer(item.get("span_count"))
            or item.get("span_count") == 0
            or not _is_nonnegative_integer(
                item.get("thread_cpu_nanoseconds")
            )
            or not _is_nonnegative_integer(item.get("wall_nanoseconds"))
            or item.get("operation") in names
        ):
            return False
        names.add(cast(str, item["operation"]))
    scopes = value.get("scopes")
    if (
        not isinstance(scopes, list)
        or len(scopes) > len(_REPLAY_V3_SCOPE_PARENTS)
    ):
        return False
    scope_names: set[str] = set()
    for item in scopes:
        if not isinstance(item, Mapping) or set(item) != {
            "accounting_delta_wall_nanoseconds",
            "children_inclusive_wall_nanoseconds",
            "direct_operation_thread_cpu_nanoseconds",
            "direct_operation_wall_nanoseconds",
            "inclusive_thread_cpu_nanoseconds",
            "inclusive_wall_nanoseconds",
            "parent",
            "scope",
            "self_thread_cpu_nanoseconds",
            "self_wall_nanoseconds",
            "span_count",
            "timing_character",
            "unattributed_thread_cpu_nanoseconds",
            "unattributed_wall_nanoseconds",
        }:
            return False
        scope = item.get("scope")
        integer_names = (
            "accounting_delta_wall_nanoseconds",
            "children_inclusive_wall_nanoseconds",
            "direct_operation_thread_cpu_nanoseconds",
            "direct_operation_wall_nanoseconds",
            "inclusive_thread_cpu_nanoseconds",
            "inclusive_wall_nanoseconds",
            "self_thread_cpu_nanoseconds",
            "self_wall_nanoseconds",
            "span_count",
            "unattributed_thread_cpu_nanoseconds",
            "unattributed_wall_nanoseconds",
        )
        if (
            scope not in _REPLAY_V3_SCOPE_PARENTS
            or scope in scope_names
            or item.get("parent")
            != _REPLAY_V3_SCOPE_PARENTS.get(cast(str, scope))
            or item.get("timing_character") != "inclusive_and_self"
            or any(
                not _is_nonnegative_integer(item.get(name))
                for name in integer_names
            )
            or item.get("span_count") == 0
        ):
            return False
        inclusive = cast(int, item["inclusive_wall_nanoseconds"])
        children = cast(
            int, item["children_inclusive_wall_nanoseconds"]
        )
        direct = cast(int, item["direct_operation_wall_nanoseconds"])
        self_wall = cast(int, item["self_wall_nanoseconds"])
        unattributed = cast(
            int, item["unattributed_wall_nanoseconds"]
        )
        accounting_delta = cast(
            int, item["accounting_delta_wall_nanoseconds"]
        )
        if (
            children > inclusive
            or self_wall != inclusive - children
            or unattributed != max(0, self_wall - direct)
            or accounting_delta
            != abs(inclusive - children - direct - unattributed)
        ):
            return False
        scope_names.add(cast(str, scope))
    return True


def _is_v3_funding_summary(value: object, *, terminal: bool) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "eqp",
        "eqp_capture_connection_count",
        "eqp_capture_phase_wall_share",
        "eqp_capture_query_only_verified",
        "eqp_capture_thread_cpu_nanoseconds",
        "eqp_capture_total_changes",
        "eqp_capture_wall_nanoseconds",
        "existing_indexes",
        "fallback_scan_detected",
        "lookup_count",
        "max_historical_distance",
        "payload_characters",
        "query_fingerprints",
        "query_shape",
        "requested_after_commit_sequence_max",
        "requested_after_commit_sequence_min",
        "rows_returned",
        "selected_index_name",
    }:
        return False
    if (
        not isinstance(value.get("eqp_capture_query_only_verified"), bool)
        or not isinstance(value.get("fallback_scan_detected"), bool)
        or not _is_nonnegative_number(
            value.get("eqp_capture_phase_wall_share")
        )
        or float(value["eqp_capture_phase_wall_share"]) > 1.0
        or any(
            not _is_nonnegative_integer(value.get(name))
            for name in (
                "eqp_capture_connection_count",
                "eqp_capture_thread_cpu_nanoseconds",
                "eqp_capture_total_changes",
                "eqp_capture_wall_nanoseconds",
                "lookup_count",
                "max_historical_distance",
                "payload_characters",
                "requested_after_commit_sequence_max",
                "rows_returned",
            )
        )
    ):
        return False
    lookup_count = cast(int, value["lookup_count"])
    requested_min = value.get("requested_after_commit_sequence_min")
    if (
        (lookup_count == 0 and requested_min is not None)
        or (
            lookup_count > 0
            and (
                not _is_nonnegative_integer(requested_min)
                or requested_min
                > cast(int, value["requested_after_commit_sequence_max"])
            )
        )
        or cast(int, value["rows_returned"]) > lookup_count
    ):
        return False
    fingerprints = value.get("query_fingerprints")
    if (
        not isinstance(fingerprints, list)
        or len(fingerprints) > _REPLAY_V3_QUERY_FINGERPRINT_LIMIT
        or len(set(fingerprints)) != len(fingerprints)
        or fingerprints != sorted(fingerprints)
        or any(not _is_sha256(item) for item in fingerprints)
    ):
        return False
    indexes = value.get("existing_indexes")
    if (
        not isinstance(indexes, list)
        or len(indexes) > _REPLAY_V3_INDEX_LIMIT
    ):
        return False
    index_names: list[str] = []
    for index in indexes:
        if (
            not isinstance(index, Mapping)
            or set(index) != {"name", "origin", "partial", "unique"}
            or not isinstance(index.get("name"), str)
            or _SAFE_SQLITE_IDENTIFIER.fullmatch(
                cast(str, index["name"])
            )
            is None
            or index.get("origin") not in {"c", "pk", "u"}
            or not isinstance(index.get("partial"), bool)
            or not isinstance(index.get("unique"), bool)
        ):
            return False
        index_names.append(cast(str, index["name"]))
    if len(set(index_names)) != len(index_names) or index_names != sorted(
        index_names
    ):
        return False
    plans = value.get("eqp")
    if not isinstance(plans, list) or len(plans) > _REPLAY_V3_EQP_LIMIT:
        return False
    for plan in plans:
        if (
            not isinstance(plan, Mapping)
            or set(plan)
            != {
                "access",
                "covering_index",
                "index_name",
                "parent_id",
                "select_id",
                "uses_temp_btree",
            }
            or plan.get("access") not in {"OTHER", "SCAN", "SEARCH"}
            or not isinstance(plan.get("covering_index"), bool)
            or not isinstance(plan.get("uses_temp_btree"), bool)
            or not _is_nonnegative_integer(plan.get("parent_id"))
            or not _is_nonnegative_integer(plan.get("select_id"))
            or (
                plan.get("index_name") is not None
                and (
                    not isinstance(plan.get("index_name"), str)
                    or _SAFE_SQLITE_IDENTIFIER.fullmatch(
                        cast(str, plan["index_name"])
                    )
                    is None
                )
            )
        ):
            return False
    selected_indexes = [
        cast(str, plan["index_name"])
        for plan in plans
        if isinstance(plan.get("index_name"), str)
    ]
    selected_index = value.get("selected_index_name")
    if (
        selected_index
        != (selected_indexes[0] if selected_indexes else None)
        or value.get("fallback_scan_detected")
        != any(plan["access"] == "SCAN" for plan in plans)
    ):
        return False
    shape = value.get("query_shape")
    if (
        not isinstance(shape, Mapping)
        or set(shape) != {"columns", "order_by", "predicates", "table"}
        or shape.get("table") != "paper_inbox"
        or shape.get("order_by") != ["commit_sequence", "input_id"]
        or shape.get("predicates")
        != [
            "run_id_equal",
            "commit_sequence_greater_than",
            "payload_input_type_equal",
        ]
    ):
        return False
    columns = shape.get("columns")
    if (
        not isinstance(columns, list)
        or len(columns) > 32
        or len(set(columns)) != len(columns)
        or columns != sorted(columns)
        or any(
            not isinstance(column, str)
            or _SAFE_SQLITE_IDENTIFIER.fullmatch(column) is None
            for column in columns
        )
    ):
        return False
    if not terminal:
        return True
    return (
        bool(plans)
        and bool(columns)
        and value.get("eqp_capture_connection_count") == 1
        and value.get("eqp_capture_query_only_verified") is True
        and value.get("eqp_capture_total_changes") == 0
        and {
            "commit_sequence",
            "input_id",
            "payload_json",
            "run_id",
        }.issubset(columns)
    )

def _is_replay_timing_v3(
    value: object,
    *,
    terminal: bool,
    progress: Mapping[str, object],
) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "accounting_tolerance_nanoseconds",
        "clock_unit",
        "completed_input_count",
        "completed_input_tail",
        "completed_input_thread_cpu_nanoseconds",
        "completed_input_wall_nanoseconds",
        "conservation_delta_wall_nanoseconds",
        "exclusive_operation_wall_nanoseconds",
        "exclusive_semantics",
        "funding_lookup",
        "input_residual_wall_fraction",
        "input_residual_wall_nanoseconds",
        "input_type_operation_matrix",
        "input_type_totals",
        "instrumentation_complete",
        "instrumentation_mode",
        "open_observer_span_count",
        "open_parent_scope_count",
        "operation_taxonomy",
        "over_attributed_operation_wall_nanoseconds",
        "outside_completed_inputs_wall_nanoseconds",
        "parent_scopes",
        "phase_known_diagnostic_wall_nanoseconds",
        "phase_conservation_delta_wall_nanoseconds",
        "phase_operation_taxonomy",
        "phase_operation_timings",
        "phase_operation_wall_nanoseconds",
        "phase_over_attributed_known_diagnostic_wall_nanoseconds",
        "phase_over_attributed_operation_wall_nanoseconds",
        "phase_unattributed_wall_fraction",
        "phase_unattributed_wall_nanoseconds",
        "phase_unattributed_excluding_known_diagnostics_wall_nanoseconds",
        "projection_history_sizes",
        "replay_store_phase_wall_nanoseconds",
        "required_coverage_complete",
        "required_operation_coverage",
        "required_phase_operation_coverage",
        "required_scope_coverage",
        "scope_taxonomy",
        "slowest_completed_inputs",
        "max_scope_unattributed_wall_fraction",
        "unattributed_wall_fraction_tolerance",
        "unattributed_wall_within_tolerance",
        "unknown_input_count",
        "unknown_operation_span_count",
        "version",
    }:
        return False
    integer_names = (
        "accounting_tolerance_nanoseconds",
        "completed_input_count",
        "completed_input_thread_cpu_nanoseconds",
        "completed_input_wall_nanoseconds",
        "conservation_delta_wall_nanoseconds",
        "exclusive_operation_wall_nanoseconds",
        "input_residual_wall_nanoseconds",
        "open_observer_span_count",
        "open_parent_scope_count",
        "over_attributed_operation_wall_nanoseconds",
        "outside_completed_inputs_wall_nanoseconds",
        "phase_conservation_delta_wall_nanoseconds",
        "phase_known_diagnostic_wall_nanoseconds",
        "phase_operation_wall_nanoseconds",
        "phase_over_attributed_known_diagnostic_wall_nanoseconds",
        "phase_over_attributed_operation_wall_nanoseconds",
        "phase_unattributed_excluding_known_diagnostics_wall_nanoseconds",
        "phase_unattributed_wall_nanoseconds",
        "replay_store_phase_wall_nanoseconds",
        "unknown_input_count",
        "unknown_operation_span_count",
    )
    if (
        value.get("version") != _REPLAY_TIMING_V3_VERSION
        or value.get("instrumentation_mode") != "V3"
        or value.get("clock_unit") != "nanoseconds"
        or value.get("exclusive_semantics")
        != _REPLAY_V3_EXCLUSIVE_SEMANTICS
        or value.get("accounting_tolerance_nanoseconds")
        != _REPLAY_V3_SCOPE_ACCOUNTING_TOLERANCE_NS
        or value.get("operation_taxonomy")
        != sorted(_REPLAY_V3_OPERATIONS)
        or value.get("phase_operation_taxonomy")
        != sorted(_REPLAY_V3_PHASE_OPERATIONS)
        or value.get("scope_taxonomy")
        != [
            {"name": name, "parent": parent}
            for name, parent in sorted(_REPLAY_V3_SCOPE_PARENTS.items())
        ]
        or not isinstance(value.get("instrumentation_complete"), bool)
        or not isinstance(value.get("required_coverage_complete"), bool)
        or not isinstance(
            value.get("unattributed_wall_within_tolerance"), bool
        )
        or value.get("unattributed_wall_fraction_tolerance")
        != _REPLAY_V3_UNATTRIBUTED_WALL_FRACTION_TOLERANCE
        or any(
            not _is_nonnegative_number(value.get(name))
            or float(value[name]) > 1.0
            for name in (
                "input_residual_wall_fraction",
                "max_scope_unattributed_wall_fraction",
                "phase_unattributed_wall_fraction",
            )
        )
        or any(
            not _is_nonnegative_integer(value.get(name))
            for name in integer_names
        )
        or not _is_v3_funding_summary(
            value.get("funding_lookup"),
            terminal=terminal,
        )
        or value.get("projection_history_sizes")
        != progress.get("projection_history_sizes")
    ):
        return False
    completed = cast(int, value["completed_input_count"])
    last_completed = progress.get("last_completed_input")
    expected_completed = (
        int(last_completed["commit_sequence"])
        if isinstance(last_completed, Mapping)
        else 0
    )
    if completed != expected_completed:
        return False

    totals = value.get("input_type_totals")
    if (
        not isinstance(totals, list)
        or len(totals) > len(_REPLAY_V3_INPUT_TYPES)
    ):
        return False
    type_counts: dict[str, int] = {}
    total_wall = 0
    total_cpu = 0
    for item in totals:
        if (
            not isinstance(item, Mapping)
            or set(item)
            != {
                "input_type",
                "max_wall_nanoseconds",
                "mean_wall_nanoseconds",
                "span_count",
                "thread_cpu_nanoseconds",
                "wall_nanoseconds",
            }
            or item.get("input_type") not in _REPLAY_V3_INPUT_TYPES
            or item.get("input_type") in type_counts
            or not _is_nonnegative_integer(item.get("span_count"))
            or item.get("span_count") == 0
            or not _is_nonnegative_integer(item.get("wall_nanoseconds"))
            or not _is_nonnegative_integer(
                item.get("thread_cpu_nanoseconds")
            )
            or not _is_nonnegative_integer(
                item.get("max_wall_nanoseconds")
            )
            or not _is_nonnegative_number(
                item.get("mean_wall_nanoseconds")
            )
            or cast(int, item["max_wall_nanoseconds"])
            > cast(int, item["wall_nanoseconds"])
        ):
            return False
        input_type = cast(str, item["input_type"])
        span_count = cast(int, item["span_count"])
        wall = cast(int, item["wall_nanoseconds"])
        if (
            abs(
                float(item["mean_wall_nanoseconds"])
                - wall / span_count
            )
            > 1.0
        ):
            return False
        type_counts[input_type] = span_count
        total_wall += wall
        total_cpu += cast(int, item["thread_cpu_nanoseconds"])
    progress_type_counts = progress.get("input_type_counts")
    if (
        sum(type_counts.values()) != completed
        or type_counts != progress_type_counts
        or total_wall != value.get("completed_input_wall_nanoseconds")
        or total_cpu
        != value.get("completed_input_thread_cpu_nanoseconds")
    ):
        return False

    phase_operations = value.get("phase_operation_timings")
    if (
        not isinstance(phase_operations, list)
        or len(phase_operations) > len(_REPLAY_V3_PHASE_OPERATIONS)
    ):
        return False
    phase_operation_names: set[str] = set()
    phase_operation_wall = 0
    unknown_phase_operation_spans = 0
    phase_wall = cast(int, value["replay_store_phase_wall_nanoseconds"])
    for item in phase_operations:
        if (
            not isinstance(item, Mapping)
            or set(item)
            != {
                "max_wall_nanoseconds",
                "operation",
                "phase_wall_share",
                "span_count",
                "thread_cpu_nanoseconds",
                "timing_character",
                "wall_nanoseconds",
            }
            or item.get("operation") not in _REPLAY_V3_PHASE_OPERATIONS
            or item.get("operation") in phase_operation_names
            or item.get("timing_character") != "exclusive"
            or any(
                not _is_nonnegative_integer(item.get(name))
                for name in (
                    "max_wall_nanoseconds",
                    "span_count",
                    "thread_cpu_nanoseconds",
                    "wall_nanoseconds",
                )
            )
            or item.get("span_count") == 0
            or cast(int, item["max_wall_nanoseconds"])
            > cast(int, item["wall_nanoseconds"])
            or not _is_nonnegative_number(item.get("phase_wall_share"))
            or float(item["phase_wall_share"]) > 1.0
        ):
            return False
        operation = cast(str, item["operation"])
        wall = cast(int, item["wall_nanoseconds"])
        expected_share = wall / phase_wall if phase_wall else 0.0
        if (
            abs(float(item["phase_wall_share"]) - expected_share)
            > 1e-12
        ):
            return False
        phase_operation_names.add(operation)
        phase_operation_wall += wall
        if operation == "UNKNOWN":
            unknown_phase_operation_spans += cast(int, item["span_count"])
    if value.get("phase_operation_wall_nanoseconds") != phase_operation_wall:
        return False

    matrix = value.get("input_type_operation_matrix")
    if not isinstance(matrix, list) or len(matrix) > _REPLAY_V3_MATRIX_LIMIT:
        return False
    matrix_pairs: set[tuple[str, str]] = set()
    exclusive_wall = 0
    unknown_operation_spans = unknown_phase_operation_spans
    for item in matrix:
        if (
            not isinstance(item, Mapping)
            or set(item)
            != {
                "affected_input_count",
                "input_type",
                "max_wall_nanoseconds",
                "mean_thread_cpu_nanoseconds",
                "mean_wall_nanoseconds",
                "operation",
                "p50_wall_nanoseconds",
                "p95_wall_nanoseconds",
                "phase_wall_share",
                "span_count",
                "thread_cpu_nanoseconds",
                "timing_character",
                "wall_nanoseconds",
            }
            or item.get("timing_character") != "exclusive"
            or item.get("input_type") not in type_counts
            or item.get("operation") not in _REPLAY_V3_OPERATIONS
            or (
                item.get("operation") in _REPLAY_V3_PHASE_OPERATIONS
                and item.get("operation") != "UNKNOWN"
            )
        ):
            return False
        pair = (
            cast(str, item["input_type"]),
            cast(str, item["operation"]),
        )
        numeric_names = (
            "affected_input_count",
            "max_wall_nanoseconds",
            "p50_wall_nanoseconds",
            "p95_wall_nanoseconds",
            "span_count",
            "thread_cpu_nanoseconds",
            "wall_nanoseconds",
        )
        if (
            pair in matrix_pairs
            or any(
                not _is_nonnegative_integer(item.get(name))
                for name in numeric_names
            )
            or item.get("span_count") == 0
            or item.get("affected_input_count") == 0
            or cast(int, item["affected_input_count"])
            > type_counts[pair[0]]
            or cast(int, item["affected_input_count"])
            > cast(int, item["span_count"])
            or not _is_nonnegative_number(
                item.get("mean_thread_cpu_nanoseconds")
            )
            or not _is_nonnegative_number(
                item.get("mean_wall_nanoseconds")
            )
            or not _is_nonnegative_number(item.get("phase_wall_share"))
            or float(item["phase_wall_share"]) > 1.0
            or cast(int, item["p50_wall_nanoseconds"])
            > cast(int, item["p95_wall_nanoseconds"])
            or cast(int, item["p95_wall_nanoseconds"])
            > cast(int, item["max_wall_nanoseconds"])
            or cast(int, item["max_wall_nanoseconds"])
            > cast(int, item["wall_nanoseconds"])
        ):
            return False
        spans = cast(int, item["span_count"])
        wall = cast(int, item["wall_nanoseconds"])
        cpu = cast(int, item["thread_cpu_nanoseconds"])
        phase_wall = cast(int, value["replay_store_phase_wall_nanoseconds"])
        expected_share = wall / phase_wall if phase_wall else 0.0
        if (
            abs(float(item["mean_wall_nanoseconds"]) - wall / spans)
            > 1.0
            or abs(
                float(item["mean_thread_cpu_nanoseconds"])
                - cpu / spans
            )
            > 1.0
            or abs(float(item["phase_wall_share"]) - expected_share)
            > 1e-12
        ):
            return False
        matrix_pairs.add(pair)
        exclusive_wall += wall
        if pair[0] == "UNKNOWN" or pair[1] == "UNKNOWN":
            unknown_operation_spans += spans

    residual = max(0, total_wall - exclusive_wall)
    over_attributed = max(0, exclusive_wall - total_wall)
    conservation_delta = abs(total_wall - exclusive_wall - residual)
    funding_summary = cast(Mapping[str, object], value["funding_lookup"])
    known_diagnostic_wall = cast(
        int, funding_summary["eqp_capture_wall_nanoseconds"]
    )
    outside_completed_inputs = max(0, phase_wall - total_wall)
    phase_attributed_wall = (
        exclusive_wall + phase_operation_wall + known_diagnostic_wall
    )
    phase_residual = max(0, phase_wall - phase_attributed_wall)
    phase_over_attributed = max(0, phase_attributed_wall - phase_wall)
    phase_conservation_delta = abs(
        phase_wall - phase_attributed_wall - phase_residual
    )
    if (
        exclusive_wall
        != value.get("exclusive_operation_wall_nanoseconds")
        or residual != value.get("input_residual_wall_nanoseconds")
        or over_attributed
        != value.get("over_attributed_operation_wall_nanoseconds")
        or conservation_delta
        != value.get("conservation_delta_wall_nanoseconds")
        or outside_completed_inputs
        != value.get("outside_completed_inputs_wall_nanoseconds")
        or phase_residual != value.get("phase_unattributed_wall_nanoseconds")
        or phase_over_attributed
        != value.get("phase_over_attributed_operation_wall_nanoseconds")
        or phase_conservation_delta
        != value.get("phase_conservation_delta_wall_nanoseconds")
    ):
        return False

    input_residual_wall_fraction = residual / total_wall if total_wall else 0.0
    phase_unattributed_wall_fraction = (
        phase_residual / phase_wall if phase_wall else 0.0
    )
    phase_unattributed_excluding_known_diagnostics = phase_residual
    phase_capacity_for_known_diagnostics = max(
        0,
        phase_wall - exclusive_wall - phase_operation_wall,
    )
    phase_over_attributed_known_diagnostics = max(
        0,
        known_diagnostic_wall - phase_capacity_for_known_diagnostics,
    )
    expected_eqp_phase_share = (
        known_diagnostic_wall / phase_wall if phase_wall else 0.0
    )
    if (
        abs(
            float(value["input_residual_wall_fraction"])
            - input_residual_wall_fraction
        )
        > 1e-12
        or abs(
            float(value["phase_unattributed_wall_fraction"])
            - phase_unattributed_wall_fraction
        )
        > 1e-12
        or value.get("phase_known_diagnostic_wall_nanoseconds")
        != known_diagnostic_wall
        or value.get(
            "phase_unattributed_excluding_known_diagnostics_wall_nanoseconds"
        )
        != phase_unattributed_excluding_known_diagnostics
        or value.get(
            "phase_over_attributed_known_diagnostic_wall_nanoseconds"
        )
        != phase_over_attributed_known_diagnostics
        or abs(
            float(
                cast(
                    float | int,
                    funding_summary["eqp_capture_phase_wall_share"],
                )
            )
            - expected_eqp_phase_share
        )
        > 1e-12
    ):
        return False

    run_start_input_count = type_counts.get("RUN_START", 0)
    committed_input_count = max(0, completed - run_start_input_count)
    phase_operation_by_name = {
        cast(str, item["operation"]): item
        for item in phase_operations
        if isinstance(item, Mapping)
    }
    required_phase_operation_coverage = [
        {
            "missing_span_count": int(
                operation not in phase_operation_by_name
            ),
            "operation": operation,
            "span_count": int(
                phase_operation_by_name.get(
                    operation,
                    {"span_count": 0},
                )["span_count"]
            ),
        }
        for operation in sorted(_REPLAY_V3_REQUIRED_PHASE_OPERATIONS)
    ]
    if (
        value.get("required_phase_operation_coverage")
        != required_phase_operation_coverage
    ):
        return False
    required_operation_coverage = []
    for population, required_operations, required_input_count in (
        ("all_inputs", _REPLAY_V3_REQUIRED_ALL_INPUT_OPERATIONS, completed),
        (
            "committed_inputs",
            _REPLAY_V3_REQUIRED_COMMITTED_INPUT_OPERATIONS,
            committed_input_count,
        ),
    ):
        for operation in sorted(required_operations):
            if population == "all_inputs":
                affected = sum(
                    int(item["affected_input_count"])
                    for item in matrix
                    if item["operation"] == operation
                )
                unexpected = 0
            else:
                affected = sum(
                    int(item["affected_input_count"])
                    for item in matrix
                    if item["operation"] == operation
                    and item["input_type"] != "RUN_START"
                )
                unexpected = sum(
                    int(item["affected_input_count"])
                    for item in matrix
                    if item["operation"] == operation
                    and item["input_type"] == "RUN_START"
                )
            required_operation_coverage.append(
                {
                    "affected_input_count": affected,
                    "missing_input_count": max(0, required_input_count - affected),
                    "operation": operation,
                    "required_input_count": required_input_count,
                    "required_population": population,
                    "unexpected_population_input_count": unexpected,
                }
            )
    if value.get("required_operation_coverage") != required_operation_coverage:
        return False
    parents = value.get("parent_scopes")
    if (
        not isinstance(parents, list)
        or len(parents) > len(_REPLAY_V3_SCOPE_PARENTS)
    ):
        return False
    seen_scopes: set[str] = set()
    scope_accounting_delta = 0
    for item in parents:
        if not isinstance(item, Mapping) or set(item) != {
            "accounting_delta_wall_nanoseconds",
            "affected_input_count",
            "children_inclusive_wall_nanoseconds",
            "direct_operation_thread_cpu_nanoseconds",
            "direct_operation_wall_nanoseconds",
            "inclusive_thread_cpu_nanoseconds",
            "inclusive_wall_nanoseconds",
            "parent",
            "parent_child_delta_wall_nanoseconds",
            "run_start_affected_input_count",
            "scope",
            "self_thread_cpu_nanoseconds",
            "self_wall_nanoseconds",
            "span_count",
            "timing_character",
            "unattributed_thread_cpu_nanoseconds",
            "unattributed_wall_nanoseconds",
        }:
            return False
        scope = item.get("scope")
        scope_numeric_names = (
            "accounting_delta_wall_nanoseconds",
            "affected_input_count",
            "children_inclusive_wall_nanoseconds",
            "direct_operation_thread_cpu_nanoseconds",
            "direct_operation_wall_nanoseconds",
            "inclusive_thread_cpu_nanoseconds",
            "inclusive_wall_nanoseconds",
            "parent_child_delta_wall_nanoseconds",
            "run_start_affected_input_count",
            "self_thread_cpu_nanoseconds",
            "self_wall_nanoseconds",
            "span_count",
            "unattributed_thread_cpu_nanoseconds",
            "unattributed_wall_nanoseconds",
        )
        if (
            scope not in _REPLAY_V3_SCOPE_PARENTS
            or scope in seen_scopes
            or item.get("parent")
            != _REPLAY_V3_SCOPE_PARENTS.get(cast(str, scope))
            or item.get("timing_character") != "inclusive_and_self"
            or any(
                not _is_nonnegative_integer(item.get(name))
                for name in scope_numeric_names
            )
            or item.get("span_count") == 0
            or item.get("affected_input_count") == 0
            or cast(int, item["affected_input_count"]) > completed
            or cast(int, item["run_start_affected_input_count"])
            > cast(int, item["affected_input_count"])
            or cast(int, item["run_start_affected_input_count"])
            > run_start_input_count
            or (
                cast(int, item["affected_input_count"])
                - cast(int, item["run_start_affected_input_count"])
                > committed_input_count
            )
            or cast(int, item["affected_input_count"])
            > cast(int, item["span_count"])
        ):
            return False
        inclusive = cast(int, item["inclusive_wall_nanoseconds"])
        children = cast(
            int, item["children_inclusive_wall_nanoseconds"]
        )
        direct = cast(int, item["direct_operation_wall_nanoseconds"])
        self_wall = cast(int, item["self_wall_nanoseconds"])
        unattributed = cast(
            int, item["unattributed_wall_nanoseconds"]
        )
        delta = cast(
            int, item["accounting_delta_wall_nanoseconds"]
        )
        if (
            children > inclusive
            or self_wall != inclusive - children
            or item["parent_child_delta_wall_nanoseconds"] != self_wall
            or unattributed != max(0, self_wall - direct)
            or delta != abs(inclusive - children - direct - unattributed)
        ):
            return False
        scope_accounting_delta += delta
        seen_scopes.add(cast(str, scope))

    scope_by_name = {
        cast(str, item["scope"]): item
        for item in parents
        if isinstance(item, Mapping)
    }
    required_scope_coverage = []
    for population, required_scopes, required_input_count in (
        ("all_inputs", _REPLAY_V3_REQUIRED_ALL_INPUT_SCOPES, completed),
        (
            "committed_inputs",
            _REPLAY_V3_REQUIRED_COMMITTED_INPUT_SCOPES,
            committed_input_count,
        ),
    ):
        for scope in sorted(required_scopes):
            scope_item = scope_by_name.get(scope)
            total_affected = (
                int(scope_item["affected_input_count"])
                if scope_item is not None
                else 0
            )
            run_start_affected = (
                int(scope_item["run_start_affected_input_count"])
                if scope_item is not None
                else 0
            )
            if population == "all_inputs":
                affected = total_affected
                unexpected = 0
            else:
                affected = total_affected - run_start_affected
                unexpected = run_start_affected
            required_scope_coverage.append(
                {
                    "affected_input_count": affected,
                    "missing_input_count": max(0, required_input_count - affected),
                    "required_input_count": required_input_count,
                    "required_population": population,
                    "scope": scope,
                    "unexpected_population_input_count": unexpected,
                }
            )
    if value.get("required_scope_coverage") != required_scope_coverage:
        return False
    required_coverage_complete = (
        completed > 0
        and run_start_input_count == 1
        and all(
            item["missing_input_count"] == 0
            and item["unexpected_population_input_count"] == 0
            for item in (
                *required_operation_coverage,
                *required_scope_coverage,
            )
        )
        and all(
            item["missing_span_count"] == 0
            for item in required_phase_operation_coverage
        )
    )
    required_scope_names = (
        _REPLAY_V3_REQUIRED_ALL_INPUT_SCOPES
        | _REPLAY_V3_REQUIRED_COMMITTED_INPUT_SCOPES
    )
    max_scope_unattributed_wall_fraction = max(
        (
            (
                int(item["unattributed_wall_nanoseconds"])
                / int(item["inclusive_wall_nanoseconds"])
                if int(item["inclusive_wall_nanoseconds"])
                else 0.0
            )
            for scope, item in scope_by_name.items()
            if scope in required_scope_names
        ),
        default=0.0,
    )
    unattributed_wall_within_tolerance = (
        input_residual_wall_fraction
        <= _REPLAY_V3_UNATTRIBUTED_WALL_FRACTION_TOLERANCE
        and max_scope_unattributed_wall_fraction
        <= _REPLAY_V3_UNATTRIBUTED_WALL_FRACTION_TOLERANCE
        and phase_unattributed_wall_fraction
        <= _REPLAY_V3_UNATTRIBUTED_WALL_FRACTION_TOLERANCE
    )
    unknown_input_count = type_counts.get("UNKNOWN", 0)
    instrumentation_complete = (
        conservation_delta <= _REPLAY_V3_SCOPE_ACCOUNTING_TOLERANCE_NS
        and phase_conservation_delta
        <= _REPLAY_V3_SCOPE_ACCOUNTING_TOLERANCE_NS
        and phase_wall > 0
        and phase_wall >= total_wall
        and scope_accounting_delta
        <= _REPLAY_V3_SCOPE_ACCOUNTING_TOLERANCE_NS
        and required_coverage_complete
        and unattributed_wall_within_tolerance
        and phase_over_attributed_known_diagnostics == 0
        and unknown_input_count == 0
        and unknown_operation_spans == 0
        and value.get("open_parent_scope_count") == 0
        and value.get("open_observer_span_count") == 0
    )
    if (
        value.get("unknown_input_count") != unknown_input_count
        or value.get("unknown_operation_span_count")
        != unknown_operation_spans
        or value.get("required_coverage_complete")
        is not required_coverage_complete
        or abs(
            float(value["max_scope_unattributed_wall_fraction"])
            - max_scope_unattributed_wall_fraction
        )
        > 1e-12
        or value.get("unattributed_wall_within_tolerance")
        is not unattributed_wall_within_tolerance
        or value.get("instrumentation_complete")
        is not instrumentation_complete
    ):
        return False
    tail = value.get("completed_input_tail")
    slowest = value.get("slowest_completed_inputs")
    if (
        not isinstance(tail, list)
        or len(tail) != min(completed, _REPLAY_TAIL_LIMIT)
        or any(not _is_v3_detailed_input(item) for item in tail)
        or [item["commit_sequence"] for item in tail]
        != list(
            range(max(1, completed - len(tail) + 1), completed + 1)
        )
        or not isinstance(slowest, list)
        or len(slowest) != min(completed, _REPLAY_SLOWEST_LIMIT)
        or any(not _is_v3_detailed_input(item) for item in slowest)
        or slowest
        != sorted(
            slowest,
            key=lambda item: (
                -int(item["wall_nanoseconds"]),
                int(item["commit_sequence"]),
            ),
        )
        or len({item["commit_sequence"] for item in slowest})
        != len(slowest)
    ):
        return False
    if not terminal:
        return True
    return completed > 0 and bool(instrumentation_complete)

def _is_replay_timing_v2(
    value: object,
    *,
    terminal: bool,
    target_commits: object,
    expected_commits: object | None = None,
) -> bool:
    if (
        not isinstance(value, Mapping)
        or set(value)
        != {
            "active_input",
            "active_operation",
            "completed_input_tail",
            "completed_target_commits",
            "expected_target_commits",
            "input_type_timings",
            "last_completed_input",
            "operation_timings",
            "projection_history_sizes",
            "remaining_target_commits",
            "slowest_completed_inputs",
            "source_first_input",
            "source_tail",
            "target_database_bytes",
            "version",
        }
        or value.get("version") != _REPLAY_TIMING_VERSION
    ):
        return False
    expected = value.get("expected_target_commits")
    completed = value.get("completed_target_commits")
    remaining = value.get("remaining_target_commits")
    if (
        not _is_nonnegative_integer(expected)
        or not _is_nonnegative_integer(completed)
        or not _is_nonnegative_integer(remaining)
    ):
        return False
    expected_count = expected
    completed_count = completed
    remaining_count = remaining
    if (
        completed_count > expected_count
        or remaining_count != expected_count - completed_count
        or completed_count != target_commits
        or (expected_commits is not None and expected != expected_commits)
    ):
        return False
    target_bytes = value.get("target_database_bytes")
    if target_bytes is not None and not _is_nonnegative_integer(target_bytes):
        return False
    source_first = value.get("source_first_input")
    if expected_count == 0:
        if source_first is not None:
            return False
    elif (
        not _is_replay_input_reference(source_first)
        or not isinstance(source_first, Mapping)
        or source_first["commit_sequence"] != 1
        or source_first["input_type"] != "RUN_START"
    ):
        return False

    type_timings = value.get("input_type_timings")
    if (
        not isinstance(type_timings, Mapping)
        or len(type_timings) > len(_REPLAY_INPUT_TYPES)
        or any(
            input_type not in _REPLAY_INPUT_TYPES or not _is_replay_timing_entry(timing)
            for input_type, timing in type_timings.items()
        )
    ):
        return False
    operation_timings = value.get("operation_timings")
    if (
        not isinstance(operation_timings, Mapping)
        or len(operation_timings) > len(_REPLAY_OPERATIONS)
        or any(
            operation not in _REPLAY_OPERATIONS or not _is_replay_timing_entry(timing)
            for operation, timing in operation_timings.items()
        )
    ):
        return False
    validated_type_timings = cast(Mapping[str, _ReplayTimingEntry], type_timings)
    validated_operation_timings = cast(
        Mapping[str, _ReplayTimingEntry], operation_timings
    )
    sizes = value.get("projection_history_sizes")
    if (
        not isinstance(sizes, Mapping)
        or set(sizes)
        != {
            "latest_commit_sequence",
            "latest_projection_characters",
            "latest_zlib_bytes",
            "max_projection_characters",
            "max_zlib_bytes",
        }
        or any(not _is_nonnegative_integer(item) for item in sizes.values())
    ):
        return False
    validated_sizes = cast(Mapping[str, int], sizes)
    if (
        validated_sizes["latest_commit_sequence"] > expected_count
        or validated_sizes["latest_projection_characters"]
        > validated_sizes["max_projection_characters"]
        or validated_sizes["latest_zlib_bytes"] > validated_sizes["max_zlib_bytes"]
    ):
        return False

    tail = value.get("source_tail")
    if (
        not isinstance(tail, list)
        or len(tail) != min(expected_count, _REPLAY_TAIL_LIMIT)
        or any(not _is_replay_input_reference(item) for item in tail)
    ):
        return False
    tail_sequences = [int(item["commit_sequence"]) for item in tail]
    expected_tail = list(
        range(max(1, expected_count - len(tail) + 1), expected_count + 1)
    )
    if tail_sequences != expected_tail:
        return False
    if (
        tail_sequences
        and tail_sequences[0] == 1
        and isinstance(source_first, Mapping)
        and dict(source_first) != dict(tail[0])
    ):
        return False
    tail_by_sequence = {
        int(item["commit_sequence"]): str(item["input_type"])
        for item in tail
    }
    if isinstance(source_first, Mapping):
        tail_by_sequence[1] = str(source_first["input_type"])

    last_completed = value.get("last_completed_input")
    if last_completed is not None and not _is_replay_input_reference(last_completed):
        return False
    if isinstance(last_completed, Mapping) and int(last_completed["commit_sequence"]) > expected_count:
        return False
    if isinstance(last_completed, Mapping) and int(last_completed["commit_sequence"]) > completed_count:
        return False
    last_completed_sequence = (
        int(last_completed["commit_sequence"])
        if isinstance(last_completed, Mapping)
        else 0
    )
    if (
        isinstance(last_completed, Mapping)
        and last_completed_sequence in tail_by_sequence
        and last_completed["input_type"] != tail_by_sequence[last_completed_sequence]
    ):
        return False
    completed_tail = value.get("completed_input_tail")
    if (
        not isinstance(completed_tail, list)
        or len(completed_tail) != min(last_completed_sequence, _REPLAY_TAIL_LIMIT)
        or any(not _is_replay_completed_input(item) for item in completed_tail)
    ):
        return False
    completed_tail_sequences = [int(item["commit_sequence"]) for item in completed_tail]
    completed_tail_end = (
        int(last_completed["commit_sequence"])
        if isinstance(last_completed, Mapping)
        else 0
    )
    expected_completed_tail = list(
        range(max(1, completed_tail_end - len(completed_tail) + 1), completed_tail_end + 1)
    )
    if completed_tail_sequences != expected_completed_tail:
        return False
    if any(
        int(item["projection_characters"]) > validated_sizes["max_projection_characters"]
        or int(item["zlib_bytes"]) > validated_sizes["max_zlib_bytes"]
        for item in completed_tail
    ):
        return False
    if any(
        int(item["commit_sequence"]) in tail_by_sequence
        and item["input_type"] != tail_by_sequence[int(item["commit_sequence"])]
        for item in completed_tail
    ):
        return False
    if isinstance(last_completed, Mapping) and (
        not completed_tail
        or {
            "commit_sequence": completed_tail[-1]["commit_sequence"],
            "input_type": completed_tail[-1]["input_type"],
        }
        != dict(last_completed)
    ):
        return False
    completed_type_counts = Counter(str(item["input_type"]) for item in completed_tail)
    observed_type_counts = {
        str(input_type): int(timing["span_count"])
        for input_type, timing in validated_type_timings.items()
    }
    if last_completed_sequence <= _REPLAY_TAIL_LIMIT:
        if observed_type_counts != dict(completed_type_counts):
            return False
    elif any(
        observed_type_counts.get(input_type, 0) < count
        for input_type, count in completed_type_counts.items()
    ):
        return False
    completed_operation_spans: Counter[str] = Counter()
    for item in completed_tail:
        completed_operation_spans.update(
            {
                str(operation): int(timing["span_count"])
                for operation, timing in cast(
                    Mapping[str, _ReplayOperationSpan], item["operations"]
                ).items()
            }
        )
    if any(
        operation not in validated_operation_timings
        or validated_operation_timings[operation]["span_count"] < span_count
        for operation, span_count in completed_operation_spans.items()
    ):
        return False
    active_input = value.get("active_input")
    if (
        active_input is not None
        and (
            not isinstance(active_input, Mapping)
            or set(active_input) != {"commit_sequence", "input_type", "wall_seconds"}
            or not _is_nonnegative_integer(active_input.get("commit_sequence"))
            or active_input.get("commit_sequence") == 0
            or active_input.get("input_type") not in _REPLAY_INPUT_TYPES
            or not _is_nonnegative_number(active_input.get("wall_seconds"))
            or int(active_input["commit_sequence"]) > expected_count
        )
    ):
        return False
    if isinstance(active_input, Mapping):
        active_sequence = int(active_input["commit_sequence"])
        if (
            active_sequence != last_completed_sequence + 1
            or completed_count not in {last_completed_sequence, active_sequence}
            or (
                active_sequence in tail_by_sequence
                and active_input["input_type"] != tail_by_sequence[active_sequence]
            )
        ):
            return False
    bootstrap_run_start_pending = (
        active_input is None
        and completed_count == 1
        and last_completed_sequence == 0
        and isinstance(source_first, Mapping)
    )
    if (
        active_input is None
        and completed_count != last_completed_sequence
        and not bootstrap_run_start_pending
    ):
        return False
    active_operation = value.get("active_operation")
    if (
        active_operation is not None
        and (
            not isinstance(active_operation, Mapping)
            or set(active_operation) != {"name", "wall_seconds"}
            or active_operation.get("name") not in _REPLAY_OPERATIONS
            or not _is_nonnegative_number(active_operation.get("wall_seconds"))
        )
    ):
        return False
    slowest = value.get("slowest_completed_inputs")
    if (
        not isinstance(slowest, list)
        or len(slowest) != min(last_completed_sequence, _REPLAY_SLOWEST_LIMIT)
        or any(not _is_replay_timed_input(item) for item in slowest)
    ):
        return False
    slowest_sequences = [int(item["commit_sequence"]) for item in slowest]
    if (
        len(set(slowest_sequences)) != len(slowest_sequences)
        or any(sequence > last_completed_sequence for sequence in slowest_sequences)
        or slowest != sorted(
            slowest,
            key=lambda item: (
                -float(item["wall_seconds"]),
                int(item["commit_sequence"]),
            ),
        )
        or any(
            int(item["commit_sequence"]) in tail_by_sequence
            and item["input_type"] != tail_by_sequence[int(item["commit_sequence"])]
            for item in slowest
        )
    ):
        return False
    input_span_count = sum(
        int(timing["span_count"])
        for timing in validated_type_timings.values()
    )
    if input_span_count != last_completed_sequence:
        return False
    run_start_timing = validated_type_timings.get("RUN_START")
    if last_completed_sequence >= 1 and (
        not isinstance(run_start_timing, Mapping)
        or run_start_timing.get("span_count") != 1
    ):
        return False
    if not terminal:
        return True
    return not (
        active_input is not None
        or active_operation is not None
        or completed_count != expected_count
        or remaining_count != 0
        or any(
            completed_item["commit_sequence"] != source_item["commit_sequence"]
            or completed_item["input_type"] != source_item["input_type"]
            for completed_item, source_item in zip(completed_tail, tail, strict=True)
        )
        or (expected_count > 0 and not validated_operation_timings)
        or (
            bool(completed_tail)
            and (
                completed_tail[-1]["projection_characters"]
                != validated_sizes["latest_projection_characters"]
                or completed_tail[-1]["zlib_bytes"]
                != validated_sizes["latest_zlib_bytes"]
            )
        )
        or not _is_nonnegative_integer(target_bytes)
        or target_bytes == 0
        or validated_sizes["latest_commit_sequence"] != expected_count
        or input_span_count != expected_count
        or expected_count == 0
        or "OTHER" in validated_type_timings
        or any(item["input_type"] == "OTHER" for item in slowest)
        or (expected_count > 0 and not isinstance(last_completed, Mapping))
        or (
            isinstance(last_completed, Mapping)
            and int(last_completed["commit_sequence"]) != expected_count
        )
        or (
            isinstance(last_completed, Mapping)
            and tail
            and dict(last_completed) != dict(tail[-1])
        )
    )


def _worker_result_protocol_failure(
    record: Mapping[str, object],
    *,
    expected_run_id: str,
    expected_instrumentation_mode: str = "V2",
) -> dict[str, object] | None:
    failure_detail = "worker terminal result failed fail-closed schema validation"
    if (
        expected_instrumentation_mode not in _REPLAY_INSTRUMENTATION_MODES
        or set(record) != _WORKER_RESULT_FIELDS
        or record.get("status") != "WORKER_COMPLETE"
        or record.get("authorizes_real_money") is not False
        or record.get("mode") != "PAPER_ONLY"
        or record.get("orders_enabled") is not False
        or record.get("run_id") != expected_run_id
        or record.get("source_open_mode") != _SOURCE_OPEN_MODE
        or record.get("source_query_only_verified") is not True
        or record.get("source_write_connection_attempts") != 0
        or record.get("historical_ledger_reconciliation_count") != 2
    ):
        return _protocol_failure(failure_detail)

    hashes = (
        record.get("config_hash"),
        record.get("event_head_hash"),
        record.get("projection_hash"),
        record.get("source_projection_hash"),
        record.get("target_projection_hash"),
        record.get("target_initial_identity"),
    )
    if not all(_is_sha256(value) for value in hashes):
        return _protocol_failure(failure_detail)
    if not (
        record.get("projection_hash")
        == record.get("source_projection_hash")
        == record.get("target_projection_hash")
    ):
        return _protocol_failure(failure_detail)

    source_head = record.get("source_head_identity")
    target_head = record.get("target_head_identity")
    if (
        not isinstance(source_head, list)
        or len(source_head) != 9
        or target_head != source_head
        or source_head[0] != expected_run_id
        or source_head[1] != record.get("config_hash")
        or source_head[2] not in store_module.PAPER_STATES
        or source_head[3] != record.get("event_count")
        or source_head[4] != record.get("event_head_hash")
        or source_head[8] != record.get("projection_hash")
        or not all(_is_nonnegative_integer(source_head[index]) for index in (3, 5, 7))
        or not all(_is_sha256(source_head[index]) for index in (1, 4, 6, 8))
    ):
        return _protocol_failure(failure_detail)

    if (
        not _is_nonnegative_integer(record.get("event_count"))
        or not _is_nonnegative_integer(record.get("bounded_historical_prefix_certification_count"))
        or not _is_nonnegative_integer(record.get("projection_history_decode_count"))
        or not _is_nonnegative_integer(record.get("source_sqlite_connection_count"))
        or record.get("source_sqlite_connection_count") == 0
        or record.get("target_paper_store_sqlite_connection_count") != 1
        or record.get("target_sqlite_connection_count")
        != 1 + int(expected_instrumentation_mode == "V3")
        or not _is_nonnegative_integer(record.get("target_database_bytes"))
        or record.get("target_database_bytes") == 0
        or not _is_nonnegative_number(record.get("replay_cpu_seconds"))
        or not _is_nonnegative_number(record.get("replay_wall_seconds"))
        or not _is_peak_rss(record.get("peak_rss_bytes"), record.get("peak_rss_source"))
    ):
        return _protocol_failure(failure_detail)

    transactions = record.get("target_logical_transaction_counts")
    if (
        not isinstance(transactions, Mapping)
        or set(transactions) != {"append_atomic", "create_run"}
        or not _is_nonnegative_integer(transactions.get("append_atomic"))
        or transactions.get("create_run") != 1
    ):
        return _protocol_failure(failure_detail)

    profile = record.get("profile")
    expected_profile_fields = set(_PROFILE_FIELDS)
    if expected_instrumentation_mode == "V2":
        expected_profile_fields.add("replay_timing_v2")
    elif expected_instrumentation_mode == "V3":
        expected_profile_fields.add("replay_timing_v3")
    if (
        not isinstance(profile, Mapping)
        or set(profile) != expected_profile_fields
        or profile.get("instrumentation_mode")
        != expected_instrumentation_mode
        or profile.get("logical_row_counts_note")
        != "integrity row hooks and append arguments; not physical SQLite scans"
        or profile.get("sqlite_sql_text_tracing")
        != "disabled_to_avoid_expanded_payload_materialization"
        or not _is_counter_mapping(profile.get("counters"))
    ):
        return _protocol_failure(failure_detail)
    progress = profile.get("replay_progress")
    if not _is_replay_progress(
        progress,
        terminal=True,
        target_commits=transactions.get("append_atomic"),
        expected_commits=source_head[5],
    ):
        return _protocol_failure(failure_detail)
    validated_progress = cast(Mapping[str, object], progress)
    input_type_counts = cast(
        Mapping[str, int], validated_progress["input_type_counts"]
    )
    reconcile_input_count = int(input_type_counts.get("RECONCILE", 0))
    if (
        record.get("bounded_historical_prefix_certification_count")
        != reconcile_input_count
        or validated_progress["target_database_bytes"]
        != record.get("target_database_bytes")
    ):
        return _protocol_failure(failure_detail)
    if expected_instrumentation_mode == "V2":
        if not _is_replay_timing_v2(
            profile.get("replay_timing_v2"),
            terminal=True,
            target_commits=transactions.get("append_atomic"),
            expected_commits=source_head[5],
        ):
            return _protocol_failure(failure_detail)
        replay_timing = cast(
            Mapping[str, object], profile["replay_timing_v2"]
        )
        if (
            replay_timing["target_database_bytes"]
            != record.get("target_database_bytes")
        ):
            return _protocol_failure(failure_detail)
    elif expected_instrumentation_mode == "V3" and not _is_replay_timing_v3(
        profile.get("replay_timing_v3"),
        terminal=True,
        progress=validated_progress,
    ):
        return _protocol_failure(failure_detail)
    phase_counters = profile.get("phase_counters")
    if (
        not isinstance(phase_counters, Mapping)
        or len(phase_counters) > 20
        or any(
            not isinstance(phase, str)
            or len(phase) > 64
            or _SAFE_COUNTER_NAME.fullmatch(phase) is None
            or not _is_counter_mapping(counters)
            for phase, counters in phase_counters.items()
        )
    ):
        return _protocol_failure(failure_detail)

    timings = profile.get("phase_timings")
    if not isinstance(timings, Mapping) or set(timings) != set(_EXPECTED_PHASES):
        return _protocol_failure(failure_detail)
    for phase in _EXPECTED_PHASES:
        timing = timings[phase]
        if (
            not isinstance(timing, Mapping)
            or set(timing) != _PHASE_TIMING_FIELDS
            or timing.get("status") != "completed"
            or not _is_nonnegative_number(timing.get("cpu_seconds"))
            or not _is_nonnegative_number(timing.get("wall_seconds"))
            or not _is_counter_mapping(timing.get("counters"))
            or not _is_peak_rss(timing.get("peak_rss_bytes"), timing.get("peak_rss_source"))
        ):
            return _protocol_failure(failure_detail)
    return None


def _phase_record_protocol_failure(
    record: Mapping[str, object],
    *,
    expected_instrumentation_mode: str = "V2",
) -> dict[str, object] | None:
    event = record.get("event")
    heartbeat_fields = {
        "elapsed_seconds",
        "event",
        "peak_rss_bytes",
        "peak_rss_source",
        "phase",
        "phase_cpu_seconds",
        "phase_wall_seconds",
        "replay_progress",
        "rows_observed",
        "sequence",
        "target_commits",
    }
    if expected_instrumentation_mode == "V2":
        heartbeat_fields.add("replay_timing_v2")
    if expected_instrumentation_mode == "V3":
        heartbeat_fields.add("replay_timing_v3")
    expected_fields = {
        "phase_started": frozenset({"elapsed_seconds", "event", "phase", "sequence"}),
        "phase_finished": frozenset(
            {
                "counters",
                "cpu_seconds",
                "elapsed_seconds",
                "event",
                "peak_rss_bytes",
                "peak_rss_source",
                "phase",
                "sequence",
                "status",
                "wall_seconds",
            }
        ),
        "phase_heartbeat": frozenset(heartbeat_fields),
        "phase_progress": frozenset(
            {
                "elapsed_seconds",
                "event",
                "phase",
                "phase_cpu_seconds",
                "phase_wall_seconds",
                "progress_units",
                "rows_observed",
                "sequence",
                "target_commits",
            }
        ),
    }
    fields = expected_fields.get(str(event))
    phase = record.get("phase")
    if (
        expected_instrumentation_mode not in _REPLAY_INSTRUMENTATION_MODES
        or fields is None
        or set(record) != fields
        or not isinstance(phase, str)
        or len(phase) > 64
        or _SAFE_COUNTER_NAME.fullmatch(phase) is None
        or not _is_nonnegative_number(record.get("elapsed_seconds"))
        or not _is_nonnegative_integer(record.get("sequence"))
        or record.get("sequence") == 0
    ):
        return _protocol_failure("worker emitted an invalid phase event")
    if event == "phase_started":
        return None
    if event == "phase_finished":
        if (
            record.get("status") not in {"completed", "failed"}
            or not _is_nonnegative_number(record.get("cpu_seconds"))
            or not _is_nonnegative_number(record.get("wall_seconds"))
            or not _is_counter_mapping(record.get("counters"))
            or not _is_peak_rss(record.get("peak_rss_bytes"), record.get("peak_rss_source"))
        ):
            return _protocol_failure("worker emitted an invalid phase event")
        return None
    for name in ("phase_cpu_seconds", "phase_wall_seconds"):
        field_value = record.get(name)
        if field_value is not None and not _is_nonnegative_number(field_value):
            return _protocol_failure("worker emitted an invalid phase event")
    if (
        not _is_nonnegative_integer(record.get("rows_observed"))
        or not _is_nonnegative_integer(record.get("target_commits"))
        or (
            event == "phase_progress"
            and not _is_nonnegative_integer(record.get("progress_units"))
        )
    ):
        return _protocol_failure("worker emitted an invalid phase event")
    if event != "phase_heartbeat":
        return None
    progress = record.get("replay_progress")
    if (
        not _is_peak_rss(record.get("peak_rss_bytes"), record.get("peak_rss_source"))
        or not _is_replay_progress(
            progress,
            terminal=False,
            target_commits=record.get("target_commits"),
        )
    ):
        return _protocol_failure("worker emitted an invalid phase event")
    validated_progress = cast(Mapping[str, object], progress)
    if expected_instrumentation_mode == "V2" and not _is_replay_timing_v2(
        record.get("replay_timing_v2"),
        terminal=False,
        target_commits=record.get("target_commits"),
    ):
        return _protocol_failure("worker emitted an invalid phase event")
    if expected_instrumentation_mode == "V3" and not _is_replay_timing_v3(
        record.get("replay_timing_v3"),
        terminal=False,
        progress=validated_progress,
    ):
        return _protocol_failure("worker emitted an invalid phase event")
    return None
def _validated_worker_failure(
    record: Mapping[str, object],
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    status = record.get("status")
    expected_fields = {"detail", "event", "status"}
    if status == "DIAGNOSTIC_WORKER_FAILED":
        expected_fields.add("exception_type")
    detail = record.get("detail")
    exception_type = record.get("exception_type")
    if (
        status not in _WORKER_FAILURE_STATUSES
        or set(record) != expected_fields
        or not isinstance(detail, str)
        or len(detail) > 1_024
        or (
            status == "DIAGNOSTIC_WORKER_FAILED"
            and (
                not isinstance(exception_type, str)
                or len(exception_type) > 128
                or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", exception_type) is None
            )
        )
    ):
        return None, _protocol_failure("worker emitted an invalid failure terminal")
    sanitized: dict[str, object] = {
        "detail": "worker reported a fail-closed diagnostic error",
        "event": "worker_failed",
        "status": status,
    }
    if status == "DIAGNOSTIC_WORKER_FAILED":
        sanitized["exception_type"] = exception_type
    return sanitized, None


def _decode_worker_line(
    line: str,
    *,
    expected_run_id: str,
    expected_token: str,
    expected_instrumentation_mode: str = "V2",
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    if line == _WORKER_LINE_TOO_LONG or len(line) > _MAX_WORKER_LINE_CHARACTERS:
        return None, _protocol_failure("worker output exceeded the bounded line size")
    if line == _WORKER_OUTPUT_QUEUE_FULL:
        return None, _protocol_failure(
            "worker output exceeded the bounded supervisor queue capacity"
        )
    try:
        decoded = json.loads(line)
    except json.JSONDecodeError:
        return None, _protocol_failure("worker emitted non-JSON output")
    if not isinstance(decoded, dict):
        return None, _protocol_failure("worker emitted a non-object JSON value")
    record = cast(dict[str, object], decoded)
    protocol_token = record.pop("_worker_protocol_token", None)
    if not isinstance(protocol_token, str) or not secrets.compare_digest(
        protocol_token,
        expected_token,
    ):
        return None, _protocol_failure("worker emitted an unauthenticated event")
    event = record.get("event")
    if event in {"phase_finished", "phase_heartbeat", "phase_progress", "phase_started"}:
        failure = _phase_record_protocol_failure(
            record,
            expected_instrumentation_mode=expected_instrumentation_mode,
        )
        if failure is not None:
            return None, failure
    elif event == "worker_failed":
        validated_record, failure = _validated_worker_failure(record)
        if failure is not None:
            return None, failure
        if validated_record is None:
            return None, _protocol_failure(
                "worker emitted an invalid failure terminal"
            )
        record = validated_record
    elif event == "worker_result":
        failure = _worker_result_protocol_failure(
            record,
            expected_run_id=expected_run_id,
            expected_instrumentation_mode=expected_instrumentation_mode,
        )
        if failure is not None:
            return None, failure
    else:
        return None, _protocol_failure("worker emitted an unknown event")
    return record, None


_OVERHEAD_CHILD_EVENTS = frozenset(
    {
        "diagnostic_result",
        "phase_finished",
        "phase_heartbeat",
        "phase_progress",
        "phase_started",
        "source_fingerprint_before",
        "source_sidecar_detected",
        "worker_failed",
        "worker_result",
        "worker_timeout",
    }
)


def _overhead_child_command(args: argparse.Namespace, mode: str) -> list[str]:
    if mode not in _REPLAY_INSTRUMENTATION_MODES:
        raise ValueError("invalid overhead instrumentation mode")
    return [
        sys.executable,
        "-u",
        str(Path(__file__).resolve()),
        "--database-copy",
        str(args.database_copy),
        "--forbid-original",
        str(args.forbid_original),
        "--scratch-root",
        str(args.scratch_root),
        "--run-id",
        str(args.run_id),
        "--expected-sha256",
        str(args.expected_sha256),
        "--wall-limit-seconds",
        str(args.wall_limit_seconds),
        "--progress-every-rows",
        str(args.progress_every_rows),
        "--instrumentation-mode",
        mode,
    ]


def _run_overhead_child(
    command: Sequence[str],
    *,
    environment: Mapping[str, str],
) -> dict[str, object]:
    process = subprocess.Popen(
        list(command),
        cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=dict(environment),
        text=True,
        encoding="utf-8",
        errors="strict",
        bufsize=1,
    )
    try:
        if process.stdout is None:
            raise DiagnosticRefusal(
                "OVERHEAD_CHILD_PROTOCOL_FAILURE",
                "overhead child stdout is unavailable",
            )
        line_count = 0
        character_count = 0
        terminal: dict[str, object] | None = None
        while True:
            line = process.stdout.readline(_MAX_WORKER_LINE_CHARACTERS + 2)
            if not line:
                break
            line_count += 1
            character_count += len(line)
            if (
                len(line) > _MAX_WORKER_LINE_CHARACTERS
                or line_count > _MAX_OVERHEAD_CHILD_LINES
                or character_count > _MAX_OVERHEAD_CHILD_CHARACTERS
            ):
                raise DiagnosticRefusal(
                    "OVERHEAD_CHILD_OUTPUT_LIMIT",
                    "overhead child output exceeded its bounded capture",
                )
            stripped = line.rstrip("\r\n")
            if not stripped:
                continue
            try:
                decoded = json.loads(stripped)
            except json.JSONDecodeError as error:
                raise DiagnosticRefusal(
                    "OVERHEAD_CHILD_PROTOCOL_FAILURE",
                    "overhead child emitted non-JSON output",
                ) from error
            if not isinstance(decoded, dict):
                raise DiagnosticRefusal(
                    "OVERHEAD_CHILD_PROTOCOL_FAILURE",
                    "overhead child emitted a non-object JSON value",
                )
            record = cast(dict[str, object], decoded)
            event = record.get("event")
            if event not in _OVERHEAD_CHILD_EVENTS:
                raise DiagnosticRefusal(
                    "OVERHEAD_CHILD_PROTOCOL_FAILURE",
                    "overhead child emitted an unknown event",
                )
            if terminal is not None:
                raise DiagnosticRefusal(
                    "OVERHEAD_CHILD_PROTOCOL_FAILURE",
                    "overhead child emitted output after its terminal event",
                )
            if event == "diagnostic_result":
                terminal = record
        return_code = process.wait()
        if return_code != 0 or terminal is None:
            raise DiagnosticRefusal(
                "OVERHEAD_CHILD_FAILED",
                "overhead child did not complete an exact replay",
            )
        return terminal
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def _source_stat_identity(value: object) -> dict[str, int] | None:
    if not isinstance(value, Mapping) or set(value) != {
        "device",
        "inode",
        "mode",
        "mtime_ns",
        "size",
    }:
        return None
    if any(not _is_nonnegative_integer(item) for item in value.values()):
        return None
    return {str(name): cast(int, item) for name, item in value.items()}


def _overhead_input_type_wall_seconds(
    profile: Mapping[str, object],
    *,
    mode: str,
) -> dict[str, float]:
    if mode == "OFF":
        return {}
    if mode == "V2":
        timing = cast(Mapping[str, object], profile["replay_timing_v2"])
        type_timings = cast(
            Mapping[str, Mapping[str, object]],
            timing["input_type_timings"],
        )
        return {
            input_type: float(cast(float | int, values["wall_seconds"]))
            for input_type, values in sorted(type_timings.items())
        }
    if mode == "V3":
        timing = cast(Mapping[str, object], profile["replay_timing_v3"])
        totals = cast(Sequence[Mapping[str, object]], timing["input_type_totals"])
        return {
            cast(str, item["input_type"]): cast(int, item["wall_nanoseconds"])
            / 1_000_000_000.0
            for item in totals
        }
    raise ValueError("invalid overhead instrumentation mode")


def _overhead_observation_from_terminal(
    record: Mapping[str, object],
    *,
    expected_run_id: str,
    expected_mode: str,
    expected_source_sha256: str,
    script_sha256: str,
    run_ordinal: int,
) -> dict[str, object]:
    failure_status = "OVERHEAD_CHILD_PROTOCOL_FAILURE"
    required_worker_fields = _WORKER_RESULT_FIELDS - {"event", "status"}
    if (
        record.get("event") != "diagnostic_result"
        or record.get("status") != "REPLAY_EXACT"
        or record.get("mode") != "PAPER_ONLY"
        or record.get("orders_enabled") is not False
        or record.get("authorizes_real_money") is not False
        or record.get("instrumentation_mode") != expected_mode
        or not required_worker_fields.issubset(record)
        or not _is_sha256(script_sha256)
    ):
        raise DiagnosticRefusal(
            failure_status,
            "overhead child terminal schema is invalid",
        )
    worker_record = {
        name: record[name]
        for name in required_worker_fields
    }
    worker_record["event"] = "worker_result"
    worker_record["status"] = "WORKER_COMPLETE"
    if _worker_result_protocol_failure(
        worker_record,
        expected_run_id=expected_run_id,
        expected_instrumentation_mode=expected_mode,
    ) is not None:
        raise DiagnosticRefusal(
            failure_status,
            "overhead child worker result is invalid",
        )

    source_stat_before = _source_stat_identity(record.get("source_stat_before"))
    source_stat_after = _source_stat_identity(record.get("source_stat_after"))
    if (
        record.get("source_sha256_before") != expected_source_sha256
        or record.get("source_sha256_after") != expected_source_sha256
        or record.get("source_sha256_unchanged") is not True
        or source_stat_before is None
        or source_stat_after != source_stat_before
        or record.get("source_stat_unchanged") is not True
        or record.get("source_sidecars_observed") != []
    ):
        raise DiagnosticRefusal(
            failure_status,
            "overhead child source identity is invalid",
        )

    profile = cast(Mapping[str, object], record["profile"])
    progress = cast(Mapping[str, object], profile["replay_progress"])
    input_type_counts = {
        str(input_type): int(count)
        for input_type, count in cast(
            Mapping[str, int],
            progress["input_type_counts"],
        ).items()
    }
    input_count = cast(int, progress["completed_target_commits"])
    peak_rss_bytes = record.get("peak_rss_bytes")
    if not _is_nonnegative_integer(peak_rss_bytes):
        raise DiagnosticRefusal(
            "OVERHEAD_METRIC_UNAVAILABLE",
            "overhead child peak RSS is unavailable",
        )
    target_initial_identity = record.get("target_initial_identity")
    if not _is_sha256(target_initial_identity):
        raise DiagnosticRefusal(
            failure_status,
            "overhead child initial store identity is invalid",
        )
    workload_identity = _canonical_identity_sha256(
        {
            "config_hash": record["config_hash"],
            "input_type_counts": input_type_counts,
            "run_id": expected_run_id,
            "script_sha256": script_sha256,
            "source_head_identity": record["source_head_identity"],
            "source_sha256": expected_source_sha256,
            "source_stat": source_stat_before,
        }
    )
    logical_result_identity = _canonical_identity_sha256(
        {
            "event_count": record["event_count"],
            "event_head_hash": record["event_head_hash"],
            "projection_hash": record["projection_hash"],
            "target_head_identity": record["target_head_identity"],
            "target_logical_transaction_counts": record[
                "target_logical_transaction_counts"
            ],
            "target_projection_hash": record["target_projection_hash"],
        }
    )
    return {
        "cpu_seconds": record["replay_cpu_seconds"],
        "input_count": input_count,
        "input_type_counts": input_type_counts,
        "input_type_wall_seconds": _overhead_input_type_wall_seconds(
            profile,
            mode=expected_mode,
        ),
        "logical_result_identity": logical_result_identity,
        "mode": expected_mode,
        "peak_rss_bytes": peak_rss_bytes,
        "run_ordinal": run_ordinal,
        "store_bytes": record["target_database_bytes"],
        "store_initial_identity": target_initial_identity,
        "wall_seconds": record["replay_wall_seconds"],
        "workload_identity": workload_identity,
    }


def _run_local_replay_overhead(args: argparse.Namespace) -> int:
    repetitions = args.overhead_repetitions
    schedule = _replay_overhead_schedule(repetitions)
    script_sha256 = _sha256(Path(__file__).resolve())
    environment = os.environ.copy()
    environment.pop(_WORKER_TOKEN_ENV, None)
    observations: list[dict[str, object]] = []
    common = {
        "authorizes_real_money": False,
        "mode": "PAPER_ONLY",
        "orders_enabled": False,
    }
    try:
        for run_ordinal, instrumentation_mode in enumerate(schedule):
            _emit(
                {
                    **common,
                    "event": "overhead_run_started",
                    "instrumentation_mode": instrumentation_mode,
                    "run_ordinal": run_ordinal,
                    "total_runs": len(schedule),
                }
            )
            terminal = _run_overhead_child(
                _overhead_child_command(args, instrumentation_mode),
                environment=environment,
            )
            if _sha256(Path(__file__).resolve()) != script_sha256:
                raise DiagnosticRefusal(
                    "OVERHEAD_CODE_CHANGED",
                    "diagnostic code changed during the overhead protocol",
                )
            observation = _overhead_observation_from_terminal(
                terminal,
                expected_run_id=args.run_id,
                expected_mode=instrumentation_mode,
                expected_source_sha256=args.expected_sha256,
                script_sha256=script_sha256,
                run_ordinal=run_ordinal,
            )
            observations.append(observation)
            _emit(
                {
                    **common,
                    "cpu_seconds": observation["cpu_seconds"],
                    "event": "overhead_run_finished",
                    "input_count": observation["input_count"],
                    "instrumentation_mode": instrumentation_mode,
                    "peak_rss_bytes": observation["peak_rss_bytes"],
                    "run_ordinal": run_ordinal,
                    "store_bytes": observation["store_bytes"],
                    "total_runs": len(schedule),
                    "wall_seconds": observation["wall_seconds"],
                }
            )
        report = _build_replay_overhead_report(
            observations,
            repetitions=repetitions,
        )
    except KeyboardInterrupt:
        _emit(
            {
                **common,
                "event": "overhead_result",
                "status": "OVERHEAD_INTERRUPTED",
            }
        )
        return 130
    except DiagnosticRefusal as error:
        _emit(
            {
                **common,
                "detail": "local overhead protocol refused a child measurement",
                "event": "overhead_result",
                "status": error.status,
            }
        )
        return 1
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        _emit(
            {
                **common,
                "detail": "local overhead protocol failed closed",
                "event": "overhead_result",
                "exception_type": type(error).__name__,
                "status": "OVERHEAD_PROTOCOL_FAILURE",
            }
        )
        return 1
    _emit(
        {
            **report,
            **common,
            "event": "overhead_result",
            "status": "OVERHEAD_COMPLETE",
        }
    )
    return 0

def _supervise_locked(
    args: argparse.Namespace,
    source_journal_mode: str,
    *,
    termination_requested: threading.Event | None = None,
) -> int:
    started = perf_counter()
    deadline = started + args.wall_limit_seconds
    try:
        database_copy, forbidden_original, scratch_root = _resolve_inputs(args)
        before = _fingerprint(database_copy, deadline=deadline)
        if before.sha256 != args.expected_sha256:
            raise DiagnosticRefusal(
                "REFUSED_SOURCE_HASH_MISMATCH",
                "the explicit database copy does not match --expected-sha256",
            )
    except DiagnosticRefusal as error:
        _emit({"detail": error.detail, "event": "diagnostic_result", "status": error.status})
        return 2
    _emit(
        {
            "database_copy_bytes": before.stat.size,
            "event": "source_fingerprint_before",
            "forbidden_original_identity_checked": True,
            "scratch_free_bytes": int(shutil.disk_usage(scratch_root).free),
            "source_journal_mode": source_journal_mode,
            "source_lock_mode": "sqlite-shared-read-transaction",
            "source_sha256": before.sha256,
            "source_stat": before.stat.to_dict(),
        }
    )
    reserve = max(15.0, before.elapsed_seconds * 2.0 + 5.0)
    worker_deadline = deadline - reserve
    worker_budget_seconds = worker_deadline - perf_counter() - 5.0
    if worker_budget_seconds < 30.0:
        _emit(
            {
                "event": "diagnostic_result",
                "status": "REFUSED_INSUFFICIENT_WALL_BUDGET",
            }
        )
        return 2

    script = Path(__file__).resolve()
    worker_result: dict[str, object] | None = None
    worker_failure: dict[str, object] | None = None
    worker_protocol_failure: dict[str, object] | None = None
    worker_terminal_seen = False
    timed_out = False
    interrupted = False
    sidecars_observed: set[str] = set()
    last_worker_phase: str | None = None
    last_worker_sequence: int | None = None
    last_replay_progress: dict[str, object] | None = None
    last_replay_timing_v2: dict[str, object] | None = None
    last_replay_timing_v3: dict[str, object] | None = None
    return_code: int | None = None

    def observe_worker_record(record: dict[str, object]) -> None:
        nonlocal last_worker_phase, last_worker_sequence
        nonlocal last_replay_progress
        nonlocal last_replay_timing_v2, last_replay_timing_v3
        nonlocal worker_failure, worker_protocol_failure, worker_result, worker_terminal_seen
        event = record.get("event")
        if worker_terminal_seen:
            if worker_protocol_failure is None:
                worker_protocol_failure = _protocol_failure(
                    "worker emitted an event after its terminal event"
                )
            return
        phase = record.get("phase")
        if isinstance(phase, str):
            last_worker_phase = phase
        sequence = record.get("sequence")
        if isinstance(sequence, int) and not isinstance(sequence, bool):
            if last_worker_sequence is not None and sequence <= last_worker_sequence:
                if worker_protocol_failure is None:
                    worker_protocol_failure = _protocol_failure(
                        "worker emitted a non-increasing event sequence"
                    )
                return
            last_worker_sequence = sequence
        replay_progress = record.get("replay_progress")
        if isinstance(replay_progress, dict):
            last_replay_progress = replay_progress
        replay_timing = record.get("replay_timing_v2")
        if isinstance(replay_timing, dict):
            last_replay_timing_v2 = replay_timing
        replay_timing_v3 = record.get("replay_timing_v3")
        if isinstance(replay_timing_v3, dict):
            last_replay_timing_v3 = replay_timing_v3
        if event in {"worker_failed", "worker_result"}:
            worker_terminal_seen = True
        if event == "worker_result":
            worker_result = record
        elif event == "worker_failed" and worker_failure is None:
            worker_failure = record
        _emit(record)

    def observe_sidecars() -> bool:
        sidecars = _sqlite_sidecars(database_copy)
        sidecars_observed.update(str(path)[len(str(database_copy)) :] for path in sidecars)
        return bool(sidecars)

    operational_failure: dict[str, object] | None = None
    if termination_requested is not None and termination_requested.is_set():
        interrupted = True
    try:
        if interrupted:
            raise KeyboardInterrupt
        with TemporaryDirectory(prefix="hyperlab-paper-replay-diagnostic-", dir=scratch_root) as owned:
            worker_token = secrets.token_hex(32)
            worker_environment = os.environ.copy()
            worker_environment[_WORKER_TOKEN_ENV] = worker_token
            command = [
                sys.executable,
                "-u",
                str(script),
                "--_worker",
                "--_worker-token",
                worker_token,
                "--database-copy",
                str(database_copy),
                "--forbid-original",
                str(forbidden_original),
                "--scratch-root",
                owned,
                "--run-id",
                args.run_id,
                "--expected-sha256",
                args.expected_sha256,
                "--wall-limit-seconds",
                str(worker_budget_seconds),
                "--progress-every-rows",
                str(args.progress_every_rows),
                "--instrumentation-mode",
                args.instrumentation_mode,
            ]
            process: subprocess.Popen[str] | None = None
            reader: threading.Thread | None = None
            try:
                process = subprocess.Popen(
                    command,
                    cwd=script.parents[1],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    env=worker_environment,
                    text=True,
                    bufsize=1,
                )
                if process.stdout is None:
                    process.kill()
                    raise RuntimeError("diagnostic worker stdout is unavailable")
                messages: queue.Queue[str | None] = queue.Queue(
                    maxsize=_MAX_QUEUED_WORKER_LINES
                )
                reader = threading.Thread(
                    target=_forward_stdout,
                    args=(process.stdout, messages),
                    daemon=True,
                )
                reader.start()
                stream_done = False
                try:
                    while True:
                        try:
                            line = messages.get(timeout=0.2)
                        except queue.Empty:
                            line = ""
                        if line is None:
                            stream_done = True
                        elif line:
                            record, protocol_failure = _decode_worker_line(
                                line,
                                expected_run_id=args.run_id,
                                expected_token=worker_token,
                                expected_instrumentation_mode=(
                                    args.instrumentation_mode
                                ),
                            )
                            if protocol_failure is not None:
                                if worker_protocol_failure is None:
                                    worker_protocol_failure = protocol_failure
                                break
                            elif record is not None:
                                observe_worker_record(record)
                                if worker_protocol_failure is not None:
                                    break
                        if observe_sidecars():
                            worker_failure = {
                                "detail": "a SQLite sidecar appeared beside the explicit source copy",
                                "status": "SOURCE_COPY_SQLITE_SIDECAR_APPEARED",
                            }
                            _emit(
                                {
                                    "event": "source_sidecar_detected",
                                    "status": "SOURCE_COPY_SQLITE_SIDECAR_APPEARED",
                                }
                            )
                            break
                        process_finished = process.poll() is not None
                        if termination_requested is not None and termination_requested.is_set():
                            interrupted = True
                            break
                        if process_finished and stream_done:
                            break
                        if perf_counter() >= worker_deadline:
                            timed_out = True
                            _emit(
                                {
                                    "event": "worker_timeout",
                                    "instrumentation_mode": args.instrumentation_mode,
                                    "last_replay_progress": last_replay_progress,
                                    "last_replay_timing_v2": last_replay_timing_v2,
                                    "last_replay_timing_v3": last_replay_timing_v3,
                                    "last_worker_phase": last_worker_phase,
                                    "last_worker_sequence": last_worker_sequence,
                                    "status": "DIAGNOSTIC_TIMEOUT",
                                }
                            )
                            break
                except KeyboardInterrupt:
                    interrupted = True
                finally:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
                    reader.join(timeout=2)
                while True:
                    try:
                        remaining = messages.get_nowait()
                    except queue.Empty:
                        break
                    if not remaining:
                        continue
                    if worker_protocol_failure is not None:
                        continue
                    record, protocol_failure = _decode_worker_line(
                        remaining,
                        expected_run_id=args.run_id,
                        expected_token=worker_token,
                        expected_instrumentation_mode=args.instrumentation_mode,
                    )
                    if protocol_failure is not None:
                        if worker_protocol_failure is None:
                            worker_protocol_failure = protocol_failure
                    elif record is not None:
                        observe_worker_record(record)
                observe_sidecars()
                return_code = process.poll()
                if return_code is None:
                    process.kill()
                    return_code = process.wait(timeout=5)
                if return_code == 124:
                    timed_out = True
            finally:
                if process is not None and process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                if reader is not None:
                    reader.join(timeout=2)
    except KeyboardInterrupt:
        interrupted = True
    except BaseException as error:
        operational_failure = {
            "detail": "diagnostic supervisor raised an exception",
            "exception_type": type(error).__name__,
            "status": "DIAGNOSTIC_SUPERVISOR_FAILED",
        }

    try:
        after = _fingerprint(database_copy, deadline=deadline)
    except DiagnosticRefusal as error:
        _emit(
            {
                "authorizes_real_money": False,
                "detail": error.detail,
                "elapsed_seconds": perf_counter() - started,
                "event": "diagnostic_result",
                "instrumentation_mode": args.instrumentation_mode,
                "last_replay_progress": last_replay_progress,
                "last_replay_timing_v2": last_replay_timing_v2,
                "last_replay_timing_v3": last_replay_timing_v3,
                "last_worker_phase": last_worker_phase,
                "last_worker_sequence": last_worker_sequence,
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
                "source_journal_mode": source_journal_mode,
                "source_lock_mode": "sqlite-shared-read-transaction",
                "source_sidecars_observed": sorted(sidecars_observed),
                "source_sha256_before": before.sha256,
                "status": error.status,
            }
        )
        return 1
    except KeyboardInterrupt:
        _emit(
            {
                "authorizes_real_money": False,
                "event": "diagnostic_result",
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
                "source_sha256_before": before.sha256,
                "status": "DIAGNOSTIC_INTERRUPTED",
            }
        )
        return 130
    except BaseException as error:
        _emit(
            {
                "authorizes_real_money": False,
                "detail": "final source fingerprint raised an exception",
                "event": "diagnostic_result",
                "exception_type": type(error).__name__,
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
                "source_sha256_before": before.sha256,
                "status": "DIAGNOSTIC_SUPERVISOR_FAILED",
            }
        )
        return 1
    if worker_result is not None:
        profile = worker_result.get("profile")
        if isinstance(profile, dict):
            terminal_progress = profile.get("replay_progress")
            if isinstance(terminal_progress, dict):
                last_replay_progress = terminal_progress
            terminal_v2 = profile.get("replay_timing_v2")
            if isinstance(terminal_v2, dict):
                last_replay_timing_v2 = terminal_v2
            terminal_v3 = profile.get("replay_timing_v3")
            if isinstance(terminal_v3, dict):
                last_replay_timing_v3 = terminal_v3
    observe_sidecars()
    unchanged_hash = after.sha256 == before.sha256
    unchanged_stat = after.stat == before.stat
    common: dict[str, object] = {
        "authorizes_real_money": False,
        "elapsed_seconds": perf_counter() - started,
        "event": "diagnostic_result",
        "instrumentation_mode": args.instrumentation_mode,
        "last_replay_progress": last_replay_progress,
        "last_replay_timing_v2": last_replay_timing_v2,
        "last_replay_timing_v3": last_replay_timing_v3,
        "last_worker_phase": last_worker_phase,
        "last_worker_sequence": last_worker_sequence,
        "mode": "PAPER_ONLY",
        "orders_enabled": False,
        "source_journal_mode": source_journal_mode,
        "source_lock_mode": "sqlite-shared-read-transaction",
        "source_sidecars_observed": sorted(sidecars_observed),
        "source_sha256_after": after.sha256,
        "source_sha256_before": before.sha256,
        "source_sha256_unchanged": unchanged_hash,
        "source_stat_after": after.stat.to_dict(),
        "source_stat_before": before.stat.to_dict(),
        "source_stat_unchanged": unchanged_stat,
    }
    if sidecars_observed:
        _emit({**common, "status": "SOURCE_COPY_SQLITE_SIDECAR_APPEARED"})
        return 1
    if not unchanged_hash or not unchanged_stat:
        _emit({**common, "status": "SOURCE_COPY_CHANGED"})
        return 1
    if termination_requested is not None and termination_requested.is_set():
        interrupted = True
    if operational_failure is not None:
        _emit({**common, **operational_failure})
        return 1
    if interrupted:
        _emit({**common, "status": "DIAGNOSTIC_INTERRUPTED"})
        return 130
    if timed_out:
        _emit({**common, "status": "DIAGNOSTIC_TIMEOUT"})
        return 124
    effective_failure = worker_protocol_failure or worker_failure
    if effective_failure is not None or worker_result is None or return_code != 0:
        detail = (
            str(effective_failure.get("detail"))
            if effective_failure is not None
            else f"worker exited with status {return_code}"
        )
        status = (
            str(effective_failure.get("status"))
            if effective_failure is not None
            else "DIAGNOSTIC_WORKER_FAILED"
        )
        _emit({**common, "detail": detail, "status": status})
        return 1
    result = {key: value for key, value in worker_result.items() if key not in {"event", "status"}}
    _emit({**result, **common, "status": "REPLAY_EXACT"})
    return 0


@contextmanager
def _sigterm_interrupt_request() -> Iterator[threading.Event]:
    requested = threading.Event()
    if threading.current_thread() is not threading.main_thread() or not hasattr(signal, "SIGTERM"):
        yield requested
        return
    sigterm = signal.SIGTERM
    previous_handler = signal.getsignal(sigterm)

    def request_interrupt(_signum: int, _frame: FrameType | None) -> None:
        requested.set()

    signal.signal(sigterm, request_interrupt)
    try:
        yield requested
    finally:
        signal.signal(sigterm, previous_handler)


def _supervise(args: argparse.Namespace) -> int:
    try:
        with _sigterm_interrupt_request() as termination_requested:
            database_copy, _forbidden_original, _scratch_root = _resolve_inputs(args)
            with _hold_source_snapshot(database_copy) as source_journal_mode:
                return _supervise_locked(
                    args,
                    source_journal_mode,
                    termination_requested=termination_requested,
                )
    except DiagnosticRefusal as error:
        _emit({"detail": error.detail, "event": "diagnostic_result", "status": error.status})
        return 2
    except KeyboardInterrupt:
        _emit(
            {
                "authorizes_real_money": False,
                "event": "diagnostic_result",
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
                "status": "DIAGNOSTIC_INTERRUPTED",
            }
        )
        return 130
    except BaseException as error:
        _emit(
            {
                "authorizes_real_money": False,
                "detail": "diagnostic supervisor raised an exception before source attestation",
                "event": "diagnostic_result",
                "exception_type": type(error).__name__,
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
                "status": "DIAGNOSTIC_SUPERVISOR_FAILED",
            }
        )
        return 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Profile canonical Paper replay on one explicit immutable SQLite copy."
    )
    parser.add_argument("--database-copy", type=Path, required=True)
    parser.add_argument("--forbid-original", type=Path, required=True)
    parser.add_argument("--scratch-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--wall-limit-seconds", type=float, default=_MAX_WALL_SECONDS)
    parser.add_argument("--progress-every-rows", type=int, default=10_000)
    parser.add_argument(
        "--instrumentation-mode",
        choices=_INSTRUMENTATION_MODES,
        default="V2",
        help="Diagnostic instrumentation detail (default: V2).",
    )
    parser.add_argument(
        "--overhead-repetitions",
        type=int,
        default=None,
        metavar="3..9",
        help=(
            "Run the local OFF/V2/V3 overhead protocol with 3 to 9 "
            "alternating repetitions (disabled by default)."
        ),
    )
    parser.add_argument("--_worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--_worker-token", help=argparse.SUPPRESS)
    return parser


def main() -> int:
    parser = _parser()
    args = parser.parse_args()
    if not 30.0 <= args.wall_limit_seconds <= _MAX_WALL_SECONDS:
        parser.error("--wall-limit-seconds must be between 30 and 840")
    if args.progress_every_rows <= 0:
        parser.error("--progress-every-rows must be positive")
    if args.overhead_repetitions is not None and not (
        _OVERHEAD_MIN_REPETITIONS
        <= args.overhead_repetitions
        <= _OVERHEAD_MAX_REPETITIONS
    ):
        parser.error("--overhead-repetitions must be between 3 and 9")
    if args._worker and args.overhead_repetitions is not None:
        parser.error("--overhead-repetitions is forbidden in worker mode")
    if args.overhead_repetitions is not None:
        return _run_local_replay_overhead(args)
    return _worker_main(args) if args._worker else _supervise(args)


if __name__ == "__main__":
    raise SystemExit(main())
