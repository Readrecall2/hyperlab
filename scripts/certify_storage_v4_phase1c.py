"""Run the pinned offline Storage v4 Phase 1C capacity certification.

This entry point is intentionally narrow.  Every external authority is pinned,
all subprocesses are local and timeout-free, and the terminal evidence can only
be published after the repository closure callback returns its complete
witness.  It has no network, venue, credential, wallet, signer, order, or
deployment surface.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import importlib
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from hyperlab.paper.storage_v4.capacity_runner import (
    current_process_cumulative_write_bytes,
)
from hyperlab.paper.storage_v4.phase1c_certification import (
    PHASE1C_HEARTBEAT_MAX_SECONDS,
    PHASE1C_HEARTBEAT_MIN_SECONDS,
    Phase1CCertificationConfig,
    Phase1CCertificationError,
    Phase1CClosureWitness,
    Phase1CCommandWitness,
    Phase1CTestWitness,
    Phase1CV9ByteWitness,
    phase1c_test_source_witnesses,
    run_phase1c_certification,
)
from hyperlab.paper.storage_v4.phase1c_preflight import (
    DEFAULT_MINIMUM_FREE_BYTES,
    Phase1BProofExpectations,
    Phase1CGoldenExpectations,
    Phase1CPreflightConfig,
)
from hyperlab.paper.storage_v4.phase1c_progress import Phase1CHeartbeatWindow

GOLDEN_CERTIFICATION_ROOT = Path(
    r"C:\Dev\hyperlab-offline-validation\e45f5569\golden-v3"
    r"\candidate-e45f5569-20260824-01"
)
GOLDEN_EXPORT_ROOT = GOLDEN_CERTIFICATION_ROOT / "corpus" / "extract-a"
GOLDEN_PIN_PATH = GOLDEN_CERTIFICATION_ROOT / "pin" / "extract-a.pin.json"
PHASE1B_ROOT = Path(
    r"C:\Dev\hyperlab-offline-validation\e45f5569"
    r"\storage-v4-phase-1b\retry-02"
)
PHASE1C_ALLOWED_PARENT = Path(
    r"C:\Dev\hyperlab-offline-validation\e45f5569\storage-v4-phase-1c"
)
GOLDEN_PRODUCER_MISSION_ROOT = PHASE1C_ALLOWED_PARENT / "native-golden-04"
GOLDEN_IMPORTED_CANDIDATE_ROOT = (
    GOLDEN_PRODUCER_MISSION_ROOT / "golden-native"
)
GOLDEN_PRODUCER_LOG_ROOT = (
    PHASE1C_ALLOWED_PARENT / "native-golden-04-driver-logs"
)
GOLDEN_PRODUCER_STDOUT_LOG = (
    GOLDEN_PRODUCER_LOG_ROOT / "stdout.jsonl"
)
GOLDEN_PRODUCER_STDERR_LOG = (
    GOLDEN_PRODUCER_LOG_ROOT / "stderr.log"
)

PINNED_CERTIFICATION_ROOT_HASH = (
    "4797d81cc089e8f57a2a8c7cf0762c4463a7e85a56d8426210b160a4338d6ad0"
)
PINNED_GOLDEN_ROOT_HASH = (
    "4ebc64a7974be92da8d5dde926ddb16f58d9e8c14287bd8da5f7c92156912376"
)
PINNED_GOLDEN_SOURCE_SHA256 = (
    "dbe96c1ef65aee4a591406b086912339317926b173dbb1c957e099fcd12e39a2"
)
PINNED_GOLDEN_RUN_ID = (
    "7e6c1c014ef851fabce026e963cbdfb44a17725e1913cf095cc8ae4c3d419e8d"
)
PINNED_GOLDEN_PIN_SHA256 = (
    "bb445a75e683e83adb1928c515502ecd92f42f9de1219339305f906bbd4bbb5c"
)
PINNED_GOLDEN_PRODUCER_STDOUT_SHA256 = (
    "1d607865260ebdaa962bbdd3a26dcf593133670a831e68698429a322dc3c015c"
)
PINNED_GOLDEN_PRODUCER_STDERR_SHA256 = (
    "c845708e70c72a7d9aab0dfa8f27cb84f7e5d55cdd20cf218203fe52b6b6f970"
)
PINNED_PHASE1B_REPORT_SHA256 = (
    "165eac6bd45ae6093a96dbd35b88c3a6301858adca0e7aa6396a665f82c400ca"
)
PINNED_PHASE1B_MANIFEST_ROOT = (
    "a85846c7899ddf8693e4882716e80274fec18663c66958445c788822bbb41398"
)
PINNED_PHASE1B_FINAL_PREFIX_ROOT = (
    "f32965fa0b24cc189e271d682136680c2867c76074724e552a43e248897665ba"
)
PINNED_ROADMAP_SHA256 = (
    "341885995fd8cf38c6c007770817d30cb67c5e30650b8f6a4a9fa6140f3abb72"
)
PINNED_ROADMAP_TARGET_LINE = 339
PINNED_ROADMAP_TARGET_OCCURRENCES = 3
PINNED_V9_SIZE_BYTES = 2_833
PINNED_V9_SHA256 = (
    "7f3216b97ffeb60d18c05572e5642f08dbb589caebcbc746fa5829b6fa565d33"
)

DEFAULT_HEARTBEAT_SECONDS = 45.0
DEFAULT_CANDIDATE_NAME = "native-capacity-05"
HISTORICAL_ATTEMPT_INGESTION = (
    ("native-golden-03", 204_000, 216_000),
    ("native-golden-04", 352_267, 352_267),
    ("native-capacity-05", 1_120_005, 1_120_005),
)
_TEST_BASETEMP_PREFIX = ".tmp-pytest-phase1c-certify-"
_OUTPUT_TAIL_BYTES = 16 * 1024
_READ_CHUNK_BYTES = 64 * 1024
_CANDIDATE_NAME_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,62}\Z")
_WINDOWS_RESERVED_LEAF_NAMES = frozenset(
    {
        "AUX",
        "CON",
        "NUL",
        "PRN",
        *(f"COM{index}" for index in range(1, 10)),
        *(f"LPT{index}" for index in range(1, 10)),
    }
)
_SUBPROCESS_ENVIRONMENT_CONTRACT = (
    "hyperlab-phase1c-sanitized-subprocess-environment-v1"
)
_SUBPROCESS_ENVIRONMENT_UNSET_EXACT = frozenset(
    {
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_CEILING_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_NOSYSTEM",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_SYSTEM",
        "GIT_DIFF_OPTS",
        "GIT_DIR",
        "GIT_DISCOVERY_ACROSS_FILESYSTEM",
        "GIT_EXEC_PATH",
        "GIT_EXTERNAL_DIFF",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_WORK_TREE",
        "MYPY_CONFIG_FILE",
        "MYPYPATH",
        "PYTHONBREAKPOINT",
        "PYTHONHOME",
        "PYTHONINSPECT",
        "PYTHONOPTIMIZE",
        "PYTHONPATH",
        "PYTHONSTARTUP",
        "PYTHONUSERBASE",
        "PYTHONWARNINGS",
    }
)
_SUBPROCESS_ENVIRONMENT_UNSET_PREFIXES = (
    "COVERAGE",
    "COV_CORE_",
    "GIT_CONFIG_",
    "PYTEST_",
)
_SUBPROCESS_ENVIRONMENT_FIXED = {
    "GIT_ATTR_NOSYSTEM": "1",
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_OPTIONAL_LOCKS": "0",
    "GIT_PAGER": "",
    "GIT_TERMINAL_PROMPT": "0",
    "PYTHONDONTWRITEBYTECODE": "1",
    "PYTHONHASHSEED": "0",
    "PYTHONNOUSERSITE": "1",
}
_DIRECT_SUBPROCESS_METRICS_SCOPE = (
    "DIRECT_SUBPROCESS_ONLY_EXCLUDES_DESCENDANTS_AND_PROCESS_TREE"
)
_PROCESS_TREE_METRICS_LIMITATION = (
    "PROCESS_TREE_AGGREGATION_UNAVAILABLE_WITH_STDLIB_WITHOUT_A_JOB_OBJECT; "
    "DIRECT_SUBPROCESS_COUNTERS_ONLY; NO_STAGNATION_INFERRED_FROM_MISSING_TREE_COUNTERS"
)

TARGETED_TEST_PATHS = (
    "tests/storage_v4/test_capacity.py",
    "tests/storage_v4/test_capacity_adapter.py",
    "tests/storage_v4/test_capacity_runner.py",
    "tests/storage_v4/test_capacity_shape.py",
    "tests/storage_v4/test_faults_durability.py",
    "tests/storage_v4/test_golden_import.py",
    "tests/storage_v4/test_golden_native.py",
    "tests/storage_v4/test_golden_reattestation.py",
    "tests/storage_v4/test_native_journal.py",
    "tests/storage_v4/test_phase1c_certification.py",
    "tests/storage_v4/test_phase1c_certify_cli.py",
    "tests/storage_v4/test_phase1c_evidence.py",
    "tests/storage_v4/test_phase1c_pipeline.py",
    "tests/storage_v4/test_phase1c_preflight.py",
    "tests/storage_v4/test_phase1c_progress.py",
    "tests/storage_v4/test_phase1c_workers.py",
    "tests/storage_v4/test_phase1c_workloads.py",
    "tests/storage_v4/test_raw_manifest.py",
    "tests/storage_v4/test_raw_reference.py",
    "tests/storage_v4/test_raw_segment.py",
    "tests/storage_v4/test_raw_store.py",
    "tests/storage_v4/test_startup_trace.py",
    "tests/storage_v4/test_tail_runner.py",
    "tests/test_paper_operator_cli_phase12_live.py",
    "tests/test_paper_runtime_candidate_identity.py",
    "tests/test_paper_runtime_phase12.py",
    "tests/test_readonly_boundary.py",
)

ProgressCallback = Callable[[Mapping[str, object]], None]
_EMIT_LOCK = threading.Lock()


@dataclass(frozen=True, slots=True)
class _CommandExecution:
    command: tuple[str, ...]
    exit_code: int
    output_sha256: str
    output_bytes: int
    summary: str
    environment_projection_sha256: str
    output_log_path: Path | None = None
    output_log_size_bytes: int | None = None

    def __post_init__(self) -> None:
        if re.fullmatch(r"[0-9a-f]{64}", self.environment_projection_sha256) is None:
            raise ValueError("command environment projection SHA-256 is invalid")
        marker = (
            "environment_projection_sha256="
            f"{self.environment_projection_sha256}"
        )
        if marker not in self.summary:
            raise ValueError("command summary does not bind its environment projection")


@dataclass(frozen=True, slots=True)
class _SubprocessEnvironment:
    values: dict[str, str]
    projection: dict[str, object]
    sha256: str


@dataclass(frozen=True, slots=True)
class _DirectSubprocessMetrics:
    cpu_ns: int | None
    peak_rss_bytes: int | None
    cumulative_write_bytes: int | None
    scope: str
    limitation: str

    def payload(self) -> dict[str, object]:
        return {
            "subprocess_cpu_ns": self.cpu_ns,
            "subprocess_cumulative_write_bytes": self.cumulative_write_bytes,
            "subprocess_metrics_scope": self.scope,
            "subprocess_peak_rss_bytes": self.peak_rss_bytes,
            "subprocess_process_tree_metrics_limitation": self.limitation,
        }


def _emit(payload: Mapping[str, object], *, error: bool = False) -> None:
    with _EMIT_LOCK:
        print(
            json.dumps(dict(payload), sort_keys=True, separators=(",", ":")),
            file=sys.stderr if error else sys.stdout,
            flush=True,
        )


def _repository_root() -> Path:
    return Path(__file__).resolve(strict=True).parents[1]


def _canonical_sha256(payload: Mapping[str, object]) -> str:
    data = json.dumps(
        dict(payload),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _validate_minimum_free_bytes(value: int) -> int:
    if type(value) is not int or value < DEFAULT_MINIMUM_FREE_BYTES:
        raise ValueError(
            "minimum_free_bytes must be at least the canonical 20 GiB floor"
        )
    return value


def _mission_root_for_candidate(candidate_name: str) -> Path:
    if type(candidate_name) is not str or _CANDIDATE_NAME_PATTERN.fullmatch(
        candidate_name
    ) is None:
        raise ValueError(
            "candidate name must be a 1-63 character direct ASCII leaf using "
            "letters, digits, underscores, or hyphens"
        )
    if candidate_name.upper() in _WINDOWS_RESERVED_LEAF_NAMES:
        raise ValueError("candidate name is reserved on Windows")
    mission_root = PHASE1C_ALLOWED_PARENT / candidate_name
    if mission_root.parent != PHASE1C_ALLOWED_PARENT or mission_root.name != candidate_name:
        raise ValueError("candidate name must remain a direct leaf of the allowed parent")
    return mission_root


def _validate_cumulative_resume_candidate_root(value: Path | None) -> Path | None:
    if value is None:
        return None
    if not isinstance(value, Path) or not value.is_absolute():
        raise ValueError("cumulative resume candidate root must be an absolute path")
    resolved = value.resolve(strict=True)
    if resolved.name != "capacity-cumulative":
        raise ValueError(
            "cumulative resume candidate root must name capacity-cumulative"
        )
    mission_root = resolved.parent
    if mission_root.parent != PHASE1C_ALLOWED_PARENT:
        raise ValueError(
            "cumulative resume candidate root must remain under the Phase 1C parent"
        )
    if _mission_root_for_candidate(mission_root.name) != mission_root:
        raise ValueError("cumulative resume mission root is not canonical")
    return resolved


def _is_controlled_subprocess_environment_key(name: str) -> bool:
    normalized = name.upper()
    return (
        normalized in _SUBPROCESS_ENVIRONMENT_UNSET_EXACT
        or normalized in _SUBPROCESS_ENVIRONMENT_FIXED
        or any(
            normalized.startswith(prefix)
            for prefix in _SUBPROCESS_ENVIRONMENT_UNSET_PREFIXES
        )
    )


def _subprocess_environment() -> _SubprocessEnvironment:
    values = {
        name: value
        for name, value in os.environ.items()
        if not _is_controlled_subprocess_environment_key(name)
    }
    values.update(_SUBPROCESS_ENVIRONMENT_FIXED)
    projection: dict[str, object] = {
        "contract": _SUBPROCESS_ENVIRONMENT_CONTRACT,
        "fixed": dict(sorted(_SUBPROCESS_ENVIRONMENT_FIXED.items())),
        "unset_exact": sorted(
            _SUBPROCESS_ENVIRONMENT_UNSET_EXACT
            - _SUBPROCESS_ENVIRONMENT_FIXED.keys()
        ),
        "unset_prefixes": list(_SUBPROCESS_ENVIRONMENT_UNSET_PREFIXES),
    }
    for name, value in values.items():
        normalized = name.upper()
        if not _is_controlled_subprocess_environment_key(name):
            continue
        if normalized not in _SUBPROCESS_ENVIRONMENT_FIXED:
            raise RuntimeError("sanitized subprocess environment retained a denied control")
        if value != _SUBPROCESS_ENVIRONMENT_FIXED[normalized]:
            raise RuntimeError("sanitized subprocess environment fixed value differs")
    return _SubprocessEnvironment(
        values=values,
        projection=projection,
        sha256=_canonical_sha256(projection),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Reattest the imported read-only native Golden result and certify "
            "one cumulative 1m offline capacity workload with authenticated "
            "100k, 500k, and 1m boundaries."
        )
    )
    parser.add_argument(
        "--candidate-name",
        default=DEFAULT_CANDIDATE_NAME,
        help=(
            "fresh direct child name under the fixed Phase 1C parent; use a new "
            "leaf such as native-capacity-06 after preserving a failed candidate"
        ),
    )
    parser.add_argument(
        "--heartbeat-seconds",
        type=float,
        default=DEFAULT_HEARTBEAT_SECONDS,
        help="subprocess and worker heartbeat interval, constrained to 30-60 seconds",
    )
    parser.add_argument(
        "--minimum-free-bytes",
        type=int,
        default=DEFAULT_MINIMUM_FREE_BYTES,
        help=(
            "fail-closed free-space floor for the pinned Phase 1C parent; values "
            "below the canonical 20 GiB floor are refused"
        ),
    )
    parser.add_argument(
        "--resume-cumulative-candidate-root",
        type=Path,
        default=None,
        help=(
            "absolute existing .../<mission>/capacity-cumulative root under the "
            "fixed Phase 1C parent; reattest its sealed boundary and ingest only "
            "the remaining suffix"
        ),
    )
    return parser


def _build_preflight_config(
    repository_root: Path,
    *,
    candidate_name: str = DEFAULT_CANDIDATE_NAME,
    minimum_free_bytes: int,
) -> Phase1CPreflightConfig:
    root = repository_root.resolve(strict=True)
    return Phase1CPreflightConfig(
        mission_root=_mission_root_for_candidate(candidate_name),
        allowed_parent=PHASE1C_ALLOWED_PARENT,
        golden_certification_root=GOLDEN_CERTIFICATION_ROOT,
        golden_export_root=GOLDEN_EXPORT_ROOT,
        golden_pin_path=GOLDEN_PIN_PATH,
        phase1b_root=PHASE1B_ROOT,
        roadmap_path=root / "HyperLab_Master_Roadmap_V4_2026-08-22.html",
        golden=Phase1CGoldenExpectations(
            certification_root_hash=PINNED_CERTIFICATION_ROOT_HASH,
            golden_root_hash=PINNED_GOLDEN_ROOT_HASH,
            source_sha256=PINNED_GOLDEN_SOURCE_SHA256,
            run_id=PINNED_GOLDEN_RUN_ID,
            pin_sha256=PINNED_GOLDEN_PIN_SHA256,
            source_size_bytes=2_014_072_832,
            export_physical_bytes=2_456_283_751,
            commit_count=252_262,
            row_count=1_011_362,
            stream_count=13,
            market_gap_count=1,
        ),
        phase1b=Phase1BProofExpectations(
            report_sha256=PINNED_PHASE1B_REPORT_SHA256,
            manifest_root=PINNED_PHASE1B_MANIFEST_ROOT,
            final_prefix_root=PINNED_PHASE1B_FINAL_PREFIX_ROOT,
            storage_v4_store_bytes=528_250_030,
            anchor_bytes=12_288,
            compatibility_segment_bytes=317_492_777,
        ),
        minimum_free_bytes=_validate_minimum_free_bytes(minimum_free_bytes),
        expected_roadmap_sha256=PINNED_ROADMAP_SHA256,
        expected_target_line_number=PINNED_ROADMAP_TARGET_LINE,
        expected_canonical_target_occurrences=PINNED_ROADMAP_TARGET_OCCURRENCES,
    )


def _validate_heartbeat(value: float) -> float:
    if type(value) not in (int, float) or not (
        PHASE1C_HEARTBEAT_MIN_SECONDS
        <= float(value)
        <= PHASE1C_HEARTBEAT_MAX_SECONDS
    ):
        raise ValueError("heartbeat interval must be between 30 and 60 seconds")
    return float(value)


def _process_peak_rss_bytes() -> int | None:
    """Best-effort process-lifetime peak RSS with an explicit unavailable state."""

    if os.name == "nt":
        try:
            from ctypes import wintypes

            class _ProcessMemoryCounters(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            psapi = ctypes.WinDLL("psapi", use_last_error=True)
            get_current_process = kernel32.GetCurrentProcess
            get_current_process.argtypes = []
            get_current_process.restype = wintypes.HANDLE
            get_process_memory_info = psapi.GetProcessMemoryInfo
            get_process_memory_info.argtypes = [
                wintypes.HANDLE,
                ctypes.POINTER(_ProcessMemoryCounters),
                wintypes.DWORD,
            ]
            get_process_memory_info.restype = wintypes.BOOL
            counters = _ProcessMemoryCounters()
            counters.cb = ctypes.sizeof(counters)
            if not get_process_memory_info(
                get_current_process(),
                ctypes.byref(counters),
                counters.cb,
            ):
                return None
            return int(counters.PeakWorkingSetSize)
        except (AttributeError, OSError, ValueError):
            return None
    try:
        resource = importlib.import_module("resource")
        usage = resource.getrusage(resource.RUSAGE_SELF)
        peak = usage.ru_maxrss
        if type(peak) not in (int, float) or peak < 0:
            return None
    except (AttributeError, ImportError, OSError, ValueError):
        return None
    return int(peak) if sys.platform == "darwin" else int(peak * 1024)


def _counter_or_none(probe: Callable[[], int | None]) -> int | None:
    try:
        value = probe()
    except (OSError, RuntimeError, ValueError):
        return None
    if type(value) is not int or value < 0:
        return None
    return value


def _unavailable_direct_subprocess_metrics(reason: str) -> _DirectSubprocessMetrics:
    return _DirectSubprocessMetrics(
        cpu_ns=None,
        peak_rss_bytes=None,
        cumulative_write_bytes=None,
        scope=_DIRECT_SUBPROCESS_METRICS_SCOPE,
        limitation=f"{_PROCESS_TREE_METRICS_LIMITATION}; {reason}",
    )


def _direct_subprocess_metrics(pid: int) -> _DirectSubprocessMetrics:
    """Best-effort Windows counters for one direct subprocess, never its tree."""

    if type(pid) is not int or pid <= 0:
        return _unavailable_direct_subprocess_metrics("DIRECT_SUBPROCESS_PID_INVALID")
    if os.name != "nt":
        return _unavailable_direct_subprocess_metrics(
            "DIRECT_SUBPROCESS_COUNTERS_UNAVAILABLE_NON_WINDOWS"
        )
    try:
        from ctypes import wintypes

        class _ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        class _IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        open_process = kernel32.OpenProcess
        open_process.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        open_process.restype = wintypes.HANDLE
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = [wintypes.HANDLE]
        close_handle.restype = wintypes.BOOL
        get_process_times = kernel32.GetProcessTimes
        get_process_times.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME),
        ]
        get_process_times.restype = wintypes.BOOL
        get_process_io_counters = kernel32.GetProcessIoCounters
        get_process_io_counters.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(_IoCounters),
        ]
        get_process_io_counters.restype = wintypes.BOOL
        get_process_memory_info = psapi.GetProcessMemoryInfo
        get_process_memory_info.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(_ProcessMemoryCounters),
            wintypes.DWORD,
        ]
        get_process_memory_info.restype = wintypes.BOOL
        process_query_information = 0x0400
        process_query_limited_information = 0x1000
        process_vm_read = 0x0010
        handle = open_process(
            process_query_information
            | process_query_limited_information
            | process_vm_read,
            False,
            pid,
        )
        if not handle:
            return _unavailable_direct_subprocess_metrics(
                "DIRECT_SUBPROCESS_OPEN_PROCESS_FAILED_OR_PROCESS_EXITED"
            )
        try:
            creation = wintypes.FILETIME()
            exit_time = wintypes.FILETIME()
            kernel = wintypes.FILETIME()
            user = wintypes.FILETIME()
            cpu_ns: int | None = None
            if get_process_times(
                handle,
                ctypes.byref(creation),
                ctypes.byref(exit_time),
                ctypes.byref(kernel),
                ctypes.byref(user),
            ):
                kernel_100ns = (kernel.dwHighDateTime << 32) | kernel.dwLowDateTime
                user_100ns = (user.dwHighDateTime << 32) | user.dwLowDateTime
                cpu_ns = int((kernel_100ns + user_100ns) * 100)

            memory = _ProcessMemoryCounters()
            memory.cb = ctypes.sizeof(memory)
            peak_rss_bytes = (
                int(memory.PeakWorkingSetSize)
                if get_process_memory_info(
                    handle,
                    ctypes.byref(memory),
                    memory.cb,
                )
                else None
            )

            io = _IoCounters()
            cumulative_write_bytes = (
                int(io.WriteTransferCount)
                if get_process_io_counters(handle, ctypes.byref(io))
                else None
            )
        finally:
            close_handle(handle)
    except (AttributeError, OSError, TypeError, ValueError):
        return _unavailable_direct_subprocess_metrics(
            "DIRECT_SUBPROCESS_COUNTER_QUERY_FAILED"
        )
    limitation = _PROCESS_TREE_METRICS_LIMITATION
    if cpu_ns is None or peak_rss_bytes is None or cumulative_write_bytes is None:
        limitation = f"{limitation}; ONE_OR_MORE_DIRECT_SUBPROCESS_COUNTERS_UNAVAILABLE"
    return _DirectSubprocessMetrics(
        cpu_ns=cpu_ns,
        peak_rss_bytes=peak_rss_bytes,
        cumulative_write_bytes=cumulative_write_bytes,
        scope=_DIRECT_SUBPROCESS_METRICS_SCOPE,
        limitation=limitation,
    )


def _subprocess_progress_assessment(
    *,
    previous_metrics: _DirectSubprocessMetrics,
    current_metrics: _DirectSubprocessMetrics,
    previous_output_bytes: int,
    current_output_bytes: int,
) -> str:
    if current_output_bytes > previous_output_bytes:
        return "PROGRESS_OBSERVED_OUTPUT_BYTES_INCREASED"
    for previous, current in (
        (previous_metrics.cpu_ns, current_metrics.cpu_ns),
        (
            previous_metrics.cumulative_write_bytes,
            current_metrics.cumulative_write_bytes,
        ),
    ):
        if previous is not None and current is not None and current > previous:
            return "PROGRESS_OBSERVED_DIRECT_SUBPROCESS_COUNTER_INCREASED"
    return (
        "INDETERMINATE_NO_DIRECT_SUBPROCESS_PROGRESS_OBSERVED; "
        "PROCESS_TREE_NOT_OBSERVED; NOT_DECLARED_STAGNANT"
    )


class _WholeCertificationHeartbeat:
    """Emit a parent-process snapshot every 45 seconds with no wall timeout."""

    def __init__(
        self,
        *,
        emit: ProgressCallback,
        interval_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
    ) -> None:
        if float(interval_seconds) != DEFAULT_HEARTBEAT_SECONDS:
            raise ValueError("whole-certification heartbeat must recur every 45 seconds")
        self._emit = emit
        self._interval_seconds = float(interval_seconds)
        self._latest_progress: dict[str, object] | None = None
        self._heartbeat_window = Phase1CHeartbeatWindow()
        self._latest_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started_ns = 0
        self._cpu_started_ns = 0

    def progress(self, payload: Mapping[str, object]) -> None:
        snapshot = dict(payload)
        with self._latest_lock:
            self._latest_progress = snapshot
        self._emit(snapshot)

    def _payload(self) -> dict[str, object]:
        elapsed_ns = time.monotonic_ns() - self._started_ns
        with self._latest_lock:
            latest = (
                None if self._latest_progress is None else dict(self._latest_progress)
            )
            normalized = self._heartbeat_window.render(
                latest,
                observed_elapsed_ns=elapsed_ns,
            )
        candidate_phase = None if latest is None else latest.get("phase")
        active_phase = (
            candidate_phase
            if type(candidate_phase) is str and bool(candidate_phase)
            else "phase1c_whole_certification"
        )
        peak_rss = _counter_or_none(_process_peak_rss_bytes)
        write_bytes = _counter_or_none(current_process_cumulative_write_bytes)
        return {
            **normalized,
            "descendant_process_visibility_scope": (
                "LATEST_PROGRESS_SNAPSHOT_ONLY; "
                "DESCENDANT_PROCESS_TREE_NOT_DIRECTLY_OBSERVED"
            ),
            "elapsed_ns": elapsed_ns,
            "event": "heartbeat",
            "heartbeat_scope": "PHASE1C_WHOLE_CERTIFICATION",
            "last_progress": latest,
            "phase": active_phase,
            "process_cpu_ns": time.process_time_ns() - self._cpu_started_ns,
            "process_cpu_scope": "CERTIFIER_PARENT_PROCESS_ONLY_SINCE_RUN_START",
            "process_cumulative_write_bytes": write_bytes,
            "process_cumulative_write_bytes_scope": (
                "WINDOWS_PARENT_PROCESS_CUMULATIVE_WRITE_TRANSFER_BYTES"
                if write_bytes is not None
                else "UNAVAILABLE_NON_WINDOWS_OR_OS_QUERY_FAILED"
            ),
            "process_peak_rss_bytes": peak_rss,
            "process_peak_rss_scope": (
                "PARENT_PROCESS_LIFETIME_HIGH_WATER_MARK"
                if peak_rss is not None
                else "UNAVAILABLE_OS_QUERY_FAILED"
            ),
            "status": "RUNNING",
        }

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            self._emit(self._payload())

    def __enter__(self) -> _WholeCertificationHeartbeat:
        if self._thread is not None:
            raise RuntimeError("whole-certification heartbeat cannot be reused")
        self._started_ns = time.monotonic_ns()
        self._cpu_started_ns = time.process_time_ns()
        self._thread = threading.Thread(
            target=self._run,
            name="phase1c-whole-certification-heartbeat",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(
        self,
        _exception_type: object,
        _exception: object,
        _traceback: object,
    ) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()


def _summary_from_tail(
    tail: bytes,
    *,
    environment_projection_sha256: str,
    purpose: str,
    output_bytes: int,
    output_log_path: Path | None,
) -> str:
    lines = tail.decode("utf-8", errors="replace").splitlines()
    last_line = next((line.strip() for line in reversed(lines) if line.strip()), "")
    if len(last_line) > 500:
        last_line = last_line[-500:]
    suffix = f"; last_line={last_line}" if last_line else ""
    log_suffix = (
        ""
        if output_log_path is None
        else f"; output_log_path={output_log_path}; output_log_size_bytes={output_bytes}"
    )
    return (
        f"{purpose}: output_bytes={output_bytes}; "
        f"environment_contract={_SUBPROCESS_ENVIRONMENT_CONTRACT}; "
        f"environment_projection_sha256={environment_projection_sha256}"
        f"{log_suffix}{suffix}"
    )


def _write_stream(stream: BinaryIO, payload: bytes) -> None:
    stream.write(payload)
    stream.flush()


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _run_streamed_command(
    command: Sequence[str],
    *,
    cwd: Path,
    purpose: str,
    heartbeat_seconds: float,
    progress: ProgressCallback | None = None,
    output_stream: BinaryIO | None = None,
    output_log_path: Path | None = None,
) -> _CommandExecution:
    """Run one local command without a timeout and hash its combined output."""

    normalized = tuple(os.fspath(item) for item in command)
    if not normalized or any(not item for item in normalized):
        raise ValueError("command must contain non-empty arguments")
    interval = _validate_heartbeat(heartbeat_seconds)
    environment = _subprocess_environment()
    resolved_cwd = cwd.resolve(strict=True)
    resolved_log_path: Path | None = None
    if output_log_path is not None:
        if not output_log_path.is_absolute():
            raise ValueError("command output log path must be absolute")
        resolved_log_path = output_log_path
        if resolved_log_path.parent.resolve(strict=True) != resolved_log_path.parent:
            raise Phase1CCertificationError("command output log parent path is unsafe")
        if os.path.lexists(resolved_log_path):
            raise Phase1CCertificationError("command output log already exists")
    sink = output_stream if output_stream is not None else sys.stderr.buffer
    started = time.monotonic()
    if progress is not None:
        progress(
            {
                "command": list(normalized),
                "environment_projection": environment.projection,
                "environment_projection_sha256": environment.sha256,
                "output_log_path": (
                    None if resolved_log_path is None else os.fspath(resolved_log_path)
                ),
                "phase": "phase1c_local_command",
                "purpose": purpose,
                "status": "RUNNING",
            }
        )
    process = subprocess.Popen(
        normalized,
        cwd=resolved_cwd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
        env=environment.values,
    )
    subprocess_metrics = _direct_subprocess_metrics(process.pid)
    previous_subprocess_metrics = subprocess_metrics
    previous_heartbeat_output_bytes = 0
    process_stdout = process.stdout
    if process_stdout is None:
        _stop_process(process)
        raise RuntimeError("subprocess combined output pipe is unavailable")
    log_stream: BinaryIO | None = None
    if resolved_log_path is not None:
        try:
            log_stream = resolved_log_path.open("xb")
        except BaseException:
            _stop_process(process)
            raise

    chunks: queue.Queue[bytes | None] = queue.Queue()

    def read_output() -> None:
        try:
            while payload := process_stdout.read(_READ_CHUNK_BYTES):
                chunks.put(payload)
        finally:
            chunks.put(None)

    reader = threading.Thread(target=read_output, name="phase1c-command-output", daemon=True)
    reader.start()
    digest = hashlib.sha256()
    output_bytes = 0
    tail = b""
    next_heartbeat = started + interval
    reached_eof = False
    try:
        while not reached_eof:
            now = time.monotonic()
            wait_seconds = max(0.05, min(0.5, next_heartbeat - now))
            try:
                payload = chunks.get(timeout=wait_seconds)
            except queue.Empty:
                payload = b""
            if payload is None:
                reached_eof = True
            elif payload:
                digest.update(payload)
                output_bytes += len(payload)
                tail = (tail + payload)[-_OUTPUT_TAIL_BYTES:]
                if log_stream is not None:
                    log_stream.write(payload)
                _write_stream(sink, payload)
            now = time.monotonic()
            if now >= next_heartbeat:
                if log_stream is not None:
                    log_stream.flush()
                    os.fsync(log_stream.fileno())
                if progress is not None:
                    current_subprocess_metrics = _direct_subprocess_metrics(process.pid)
                    progress_assessment = _subprocess_progress_assessment(
                        previous_metrics=previous_subprocess_metrics,
                        current_metrics=current_subprocess_metrics,
                        previous_output_bytes=previous_heartbeat_output_bytes,
                        current_output_bytes=output_bytes,
                    )
                    progress(
                        {
                            "elapsed_seconds": int(now - started),
                            "environment_projection_sha256": environment.sha256,
                            "output_bytes": output_bytes,
                            "output_log_path": (
                                None
                                if resolved_log_path is None
                                else os.fspath(resolved_log_path)
                            ),
                            "phase": "phase1c_local_command",
                            "purpose": purpose,
                            "status": "HEARTBEAT",
                            "subprocess_pid": process.pid,
                            "subprocess_progress_assessment": progress_assessment,
                            **current_subprocess_metrics.payload(),
                        }
                    )
                    subprocess_metrics = current_subprocess_metrics
                    previous_subprocess_metrics = current_subprocess_metrics
                    previous_heartbeat_output_bytes = output_bytes
                next_heartbeat = now + interval
        exit_code = process.wait()
    except BaseException:
        _stop_process(process)
        raise
    finally:
        process_stdout.close()
        reader.join(timeout=5)
        if log_stream is not None:
            log_stream.flush()
            os.fsync(log_stream.fileno())
            log_stream.close()

    output_sha256 = digest.hexdigest()
    summary = _summary_from_tail(
        tail,
        environment_projection_sha256=environment.sha256,
        purpose=purpose,
        output_bytes=output_bytes,
        output_log_path=resolved_log_path,
    )
    if progress is not None:
        progress(
            {
                "elapsed_seconds": int(time.monotonic() - started),
                "exit_code": exit_code,
                "environment_projection_sha256": environment.sha256,
                "output_bytes": output_bytes,
                "output_log_path": (
                    None if resolved_log_path is None else os.fspath(resolved_log_path)
                ),
                "output_sha256": output_sha256,
                "phase": "phase1c_local_command",
                "purpose": purpose,
                "status": "COMPLETE" if exit_code == 0 else "FAILED",
                "subprocess_pid": process.pid,
                **subprocess_metrics.payload(),
            }
        )
    return _CommandExecution(
        command=normalized,
        exit_code=exit_code,
        output_sha256=output_sha256,
        output_bytes=output_bytes,
        summary=summary,
        environment_projection_sha256=environment.sha256,
        output_log_path=resolved_log_path,
        output_log_size_bytes=(None if resolved_log_path is None else output_bytes),
    )


def _new_pytest_basetemp(repository_root: Path, *, label: str) -> Path:
    safe_label = "".join(character for character in label if character.isalnum() or character == "-")
    if not safe_label:
        raise ValueError("pytest basetemp label must contain safe characters")
    return Path(
        tempfile.mkdtemp(
            prefix=f"{_TEST_BASETEMP_PREFIX}{safe_label}-",
            dir=repository_root,
        )
    ).resolve(strict=True)


def _cleanup_pytest_basetemp(repository_root: Path, path: Path) -> None:
    root = repository_root.resolve(strict=True)
    target = path.resolve(strict=True)
    if target.parent != root or not target.name.startswith(_TEST_BASETEMP_PREFIX):
        raise Phase1CCertificationError("refusing to remove an unowned pytest basetemp")
    shutil.rmtree(target)


def _command_witness(execution: _CommandExecution, *, purpose: str) -> Phase1CCommandWitness:
    return Phase1CCommandWitness(
        purpose=purpose,
        command=execution.command,
        exit_code=execution.exit_code,
        output_sha256=execution.output_sha256,
        summary=execution.summary,
        output_log_path=(
            None
            if execution.output_log_path is None
            else os.fspath(execution.output_log_path)
        ),
        output_log_size_bytes=execution.output_log_size_bytes,
    )


def _run_targeted_tests(
    repository_root: Path,
    *,
    heartbeat_seconds: float,
    output_log_path: Path | None = None,
    progress: ProgressCallback | None = None,
) -> Phase1CTestWitness:
    before = phase1c_test_source_witnesses(repository_root, TARGETED_TEST_PATHS)
    basetemp = _new_pytest_basetemp(repository_root, label="targeted")
    command = (
        sys.executable,
        "-m",
        "pytest",
        *TARGETED_TEST_PATHS,
        "-p",
        "no:cacheprovider",
        "--basetemp",
        str(basetemp),
    )
    try:
        execution = _run_streamed_command(
            command,
            cwd=repository_root,
            purpose="PHASE1C_TARGETED_TESTS",
            heartbeat_seconds=heartbeat_seconds,
            progress=progress,
            output_log_path=output_log_path,
        )
    finally:
        _cleanup_pytest_basetemp(repository_root, basetemp)
    after = phase1c_test_source_witnesses(repository_root, TARGETED_TEST_PATHS)
    if after != before:
        raise Phase1CCertificationError(
            "targeted Phase 1C test sources changed while pytest was running"
        )
    return Phase1CTestWitness(
        command=execution.command,
        exit_code=execution.exit_code,
        output_sha256=execution.output_sha256,
        source_files=after,
        summary=execution.summary,
        output_log_path=(
            None
            if execution.output_log_path is None
            else os.fspath(execution.output_log_path)
        ),
        output_log_size_bytes=execution.output_log_size_bytes,
    )


def _new_targeted_log_path(candidate_name: str) -> Path:
    _mission_root_for_candidate(candidate_name)
    allowed_parent = PHASE1C_ALLOWED_PARENT.resolve(strict=True)
    directory = Path(
        tempfile.mkdtemp(
            prefix=f"{candidate_name}-targeted-logs-",
            dir=allowed_parent,
        )
    ).resolve(strict=True)
    if directory.parent != allowed_parent:
        raise Phase1CCertificationError("targeted log directory escaped allowed parent")
    return directory / "phase1c-targeted-tests.log"


def _stable_file_witness(path: Path) -> tuple[int, str]:
    before = path.stat(follow_symlinks=False)
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        opened_before = os.fstat(stream.fileno())
        while payload := stream.read(1024 * 1024):
            digest.update(payload)
        opened_after = os.fstat(stream.fileno())
    after = path.stat(follow_symlinks=False)
    identities = {
        (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns)
        for item in (before, opened_before, opened_after, after)
    }
    if len(identities) != 1 or not path.is_file() or path.is_symlink():
        raise Phase1CCertificationError(f"file changed or is unsafe while hashed: {path}")
    return before.st_size, digest.hexdigest()


def _require_pinned_v9(path: Path) -> tuple[int, str]:
    size_bytes, sha256 = _stable_file_witness(path)
    if size_bytes != PINNED_V9_SIZE_BYTES or sha256 != PINNED_V9_SHA256:
        raise Phase1CCertificationError(
            "Phase 08 V9 historical attestation differs from the pinned bytes"
        )
    return size_bytes, sha256


def _closure_commands(repository_root: Path, global_basetemp: Path) -> tuple[tuple[str, tuple[str, ...]], ...]:
    python = sys.executable
    v10 = "scripts/generate_phase12_live_paper_artifacts.py"
    phase05 = "scripts/generate_phase05_paper_evidence.py"
    return (
        ("V10_GENERATE_FIRST", (python, v10)),
        ("V10_CHECK_FIRST", (python, v10, "--check")),
        ("V10_GENERATE_SECOND", (python, v10)),
        ("V10_CHECK_SECOND", (python, v10, "--check")),
        ("PHASE05_GENERATE", (python, phase05)),
        ("PHASE05_CHECK", (python, phase05, "--check")),
        (
            "PYTEST_GLOBAL_FINAL_SINGLE_RUN",
            (
                python,
                "-m",
                "pytest",
                "-p",
                "no:cacheprovider",
                "--basetemp",
                str(global_basetemp),
            ),
        ),
        ("RUFF_GLOBAL_FINAL", (python, "-m", "ruff", "check", ".")),
        ("MYPY_HYPERLAB_FINAL", (python, "-m", "mypy", "src/hyperlab")),
        ("GIT_DIFF_CHECK_FINAL", ("git", "diff", "--check")),
    )


def _make_closure_runner(
    repository_root: Path,
    *,
    heartbeat_seconds: float,
    progress: ProgressCallback | None = None,
) -> Callable[[Path], Phase1CClosureWitness]:
    root = repository_root.resolve(strict=True)
    v9_path = root / "config" / "paper" / "phase08-v9-historical-attestation.json"

    def run_closure(mission_root: Path) -> Phase1CClosureWitness:
        resolved_mission_root = mission_root.resolve(strict=True)
        closure_logs = resolved_mission_root / "closure-logs"
        closure_logs.mkdir(exist_ok=False)
        if closure_logs.resolve(strict=True).parent != resolved_mission_root:
            raise Phase1CCertificationError("closure log directory escaped mission root")
        size_before, sha_before = _require_pinned_v9(v9_path)
        basetemp = _new_pytest_basetemp(root, label="global-final")
        witnesses: list[Phase1CCommandWitness] = []
        try:
            for index, (purpose, command) in enumerate(
                _closure_commands(root, basetemp),
                start=1,
            ):
                output_log_path = closure_logs / (
                    f"{index:02d}-{purpose.lower().replace('_', '-')}.log"
                )
                execution = _run_streamed_command(
                    command,
                    cwd=root,
                    purpose=purpose,
                    heartbeat_seconds=heartbeat_seconds,
                    progress=progress,
                    output_log_path=output_log_path,
                )
                if purpose == "PHASE05_CHECK":
                    pre_global_size, pre_global_sha256 = _require_pinned_v9(v9_path)
                    execution = _CommandExecution(
                        command=execution.command,
                        exit_code=execution.exit_code,
                        output_sha256=execution.output_sha256,
                        output_bytes=execution.output_bytes,
                        summary=(
                            f"{execution.summary}; "
                            f"v9_pre_global_size_bytes={pre_global_size}; "
                            f"v9_pre_global_sha256={pre_global_sha256}"
                        ),
                        environment_projection_sha256=(
                            execution.environment_projection_sha256
                        ),
                        output_log_path=execution.output_log_path,
                        output_log_size_bytes=execution.output_log_size_bytes,
                    )
                witnesses.append(_command_witness(execution, purpose=purpose))
        finally:
            _cleanup_pytest_basetemp(root, basetemp)
        size_after, sha_after = _require_pinned_v9(v9_path)
        if size_before != size_after:
            raise Phase1CCertificationError("V9 attestation byte length changed during closure")
        return Phase1CClosureWitness(
            commands=tuple(witnesses),
            v9=Phase1CV9ByteWitness(
                path=v9_path.relative_to(root).as_posix(),
                size_bytes=size_after,
                before_sha256=sha_before,
                after_sha256=sha_after,
            ),
        )

    return run_closure


def _progress(payload: Mapping[str, object]) -> None:
    _emit(payload)


def main(argv: Sequence[str] | None = None) -> int:
    namespace = _parser().parse_args(argv)
    try:
        heartbeat_seconds = _validate_heartbeat(namespace.heartbeat_seconds)
        minimum_free_bytes = _validate_minimum_free_bytes(namespace.minimum_free_bytes)
        repository_root = _repository_root()
        candidate_name = namespace.candidate_name
        _mission_root_for_candidate(candidate_name)
        cumulative_resume_candidate_root = (
            _validate_cumulative_resume_candidate_root(
                namespace.resume_cumulative_candidate_root
            )
        )
        targeted_log_path = _new_targeted_log_path(candidate_name)
        targeted_tests = _run_targeted_tests(
            repository_root,
            heartbeat_seconds=heartbeat_seconds,
            output_log_path=targeted_log_path,
            progress=_progress,
        )
        config = Phase1CCertificationConfig(
            repository_root=repository_root,
            preflight=_build_preflight_config(
                repository_root,
                candidate_name=candidate_name,
                minimum_free_bytes=minimum_free_bytes,
            ),
            targeted_tests=targeted_tests,
            golden_producer_candidate_root=GOLDEN_IMPORTED_CANDIDATE_ROOT,
            golden_producer_stdout_log=GOLDEN_PRODUCER_STDOUT_LOG,
            golden_producer_stderr_log=GOLDEN_PRODUCER_STDERR_LOG,
            golden_producer_stdout_sha256=(
                PINNED_GOLDEN_PRODUCER_STDOUT_SHA256
            ),
            golden_producer_stderr_sha256=(
                PINNED_GOLDEN_PRODUCER_STDERR_SHA256
            ),
            cumulative_resume_candidate_root=cumulative_resume_candidate_root,
            historical_attempt_ingestion=HISTORICAL_ATTEMPT_INGESTION,
            heartbeat_interval_seconds=heartbeat_seconds,
        )
        with _WholeCertificationHeartbeat(emit=_progress) as heartbeat:
            result = run_phase1c_certification(
                config,
                closure_runner=_make_closure_runner(
                    repository_root,
                    heartbeat_seconds=heartbeat_seconds,
                    progress=heartbeat.progress,
                ),
                progress=heartbeat.progress,
            )
    except KeyboardInterrupt:
        _emit({"status": "INTERRUPTED"}, error=True)
        return 130
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        _emit(
            {
                "error": str(error),
                "error_type": type(error).__name__,
                "status": "STORAGE_V4_PHASE_1C_CERTIFICATION_FAILED",
            },
            error=True,
        )
        return 2
    _emit(result.payload())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
