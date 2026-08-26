"""Read-only two-step successor certifier for Phase 1C candidate-05.

``reattest`` authenticates the immutable old producer candidate and publishes a
receipt only.  ``finalize`` consumes that receipt plus pre-existing gate
witnesses and publishes report/manifest/pin/COMPLETE without reopening the old
candidate.  No option can create a workload or candidate-06.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, BinaryIO, TextIO, cast

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_SUCCESSOR_MODULE_NAME = "_hyperlab_phase1c_successor_standalone_v1"
_SUCCESSOR_MODULE_PATH = (
    REPOSITORY_ROOT
    / "src"
    / "hyperlab"
    / "paper"
    / "storage_v4"
    / "phase1c_successor.py"
)


def _load_successor_module() -> ModuleType:
    existing = sys.modules.get(_SUCCESSOR_MODULE_NAME)
    if existing is not None:
        if Path(str(existing.__file__)).resolve() != _SUCCESSOR_MODULE_PATH:
            raise RuntimeError("standalone successor module name is already bound")
        return existing
    specification = importlib.util.spec_from_file_location(
        _SUCCESSOR_MODULE_NAME,
        _SUCCESSOR_MODULE_PATH,
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("standalone successor module could not be located")
    module = importlib.util.module_from_spec(specification)
    sys.modules[_SUCCESSOR_MODULE_NAME] = module
    previous_dont_write_bytecode = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        specification.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(_SUCCESSOR_MODULE_NAME, None)
        raise
    finally:
        sys.dont_write_bytecode = previous_dont_write_bytecode
    if module.__package__ not in {"", None}:
        raise RuntimeError("standalone successor unexpectedly acquired a package")
    return module


_SUCCESSOR_MODULE = _load_successor_module()
canonical_json_bytes = _SUCCESSOR_MODULE.canonical_json_bytes
PHASE1C_TARGET_NOT_MET_VERDICT = cast(
    str,
    _SUCCESSOR_MODULE.PHASE1C_TARGET_NOT_MET_VERDICT,
)
SUCCESSOR_REATTESTED_STATUS = cast(
    str,
    _SUCCESSOR_MODULE.SUCCESSOR_REATTESTED_STATUS,
)
Phase1CSuccessorConfig = _SUCCESSOR_MODULE.Phase1CSuccessorConfig
Phase1CSuccessorError = _SUCCESSOR_MODULE.Phase1CSuccessorError
Phase1CSuccessorExpectations = _SUCCESSOR_MODULE.Phase1CSuccessorExpectations
finalize_phase1c_successor_closure = (
    _SUCCESSOR_MODULE.finalize_phase1c_successor_closure
)
load_phase1c_successor_closure_witness = (
    _SUCCESSOR_MODULE.load_phase1c_successor_closure_witness
)
load_phase1c_successor_test_witness = (
    _SUCCESSOR_MODULE.load_phase1c_successor_test_witness
)
reattest_phase1c_successor = _SUCCESSOR_MODULE.reattest_phase1c_successor
durable_publish_immutable = _SUCCESSOR_MODULE.durable_publish_immutable
fsync_directory = _SUCCESSOR_MODULE.fsync_directory
Phase1CSuccessorClosureWitness = (
    _SUCCESSOR_MODULE.Phase1CSuccessorClosureWitness
)
Phase1CSuccessorCommandWitness = (
    _SUCCESSOR_MODULE.Phase1CSuccessorCommandWitness
)
Phase1CSuccessorTestWitness = _SUCCESSOR_MODULE.Phase1CSuccessorTestWitness
Phase1CSuccessorV9ByteWitness = (
    _SUCCESSOR_MODULE.Phase1CSuccessorV9ByteWitness
)
SUCCESSOR_CLOSURE_WITNESS_NAME = cast(
    str, _SUCCESSOR_MODULE.SUCCESSOR_CLOSURE_WITNESS_NAME
)
SUCCESSOR_TARGETED_LOG_NAME = cast(
    str, _SUCCESSOR_MODULE.SUCCESSOR_TARGETED_LOG_NAME
)
SUCCESSOR_TARGETED_TEST_PATHS = cast(
    tuple[str, ...], _SUCCESSOR_MODULE.SUCCESSOR_TARGETED_TEST_PATHS
)
SUCCESSOR_TARGETED_WITNESS_NAME = cast(
    str, _SUCCESSOR_MODULE.SUCCESSOR_TARGETED_WITNESS_NAME
)
SUCCESSOR_V9_RELATIVE_PATH = cast(
    str, _SUCCESSOR_MODULE.SUCCESSOR_V9_RELATIVE_PATH
)
SUCCESSOR_V9_SHA256 = cast(str, _SUCCESSOR_MODULE.SUCCESSOR_V9_SHA256)
SUCCESSOR_V9_SIZE_BYTES = cast(int, _SUCCESSOR_MODULE.SUCCESSOR_V9_SIZE_BYTES)
_read_stable_regular_file = _SUCCESSOR_MODULE._read_stable_regular_file
_MAX_GATE_LOG_BYTES = 128 * 1024 * 1024
_MAX_GATE_SOURCE_BYTES = 4 * 1024 * 1024
_TARGETED_TIMEOUT_SECONDS = 1_800
_GATE_TIMEOUT_SECONDS = {
    "V10_CHECK": 900,
    "PHASE05_CHECK": 900,
    "RUFF_GLOBAL_FINAL": 900,
    "MYPY_HYPERLAB_FINAL": 1_800,
    "PYTEST_GLOBAL_FINAL_SINGLE_RUN": 7_200,
    "GIT_DIFF_CHECK_FINAL": 300,
}

PHASE1C_PARENT = Path(
    r"C:\Dev\hyperlab-offline-validation\e45f5569\storage-v4-phase-1c"
)
CANDIDATE05_MISSION_ROOT = PHASE1C_PARENT / "native-capacity-05"
CANDIDATE05_CAPACITY_ROOT = CANDIDATE05_MISSION_ROOT / "capacity-cumulative"
CANDIDATE05_BOUNDARY_ROOT = (
    CANDIDATE05_MISSION_ROOT / ".capacity-cumulative.phase1c-boundaries"
)
CANDIDATE05_STDOUT_LOG = (
    PHASE1C_PARENT / "native-capacity-05-driver-logs" / "stdout.jsonl"
)
RUN06_CANDIDATE_ROOT = PHASE1C_PARENT / "native-capacity-06"
BASELINE_BYTE_WITNESS_PATH = (
    REPOSITORY_ROOT
    / "config"
    / "paper"
    / "storage-v4-phase1c-successor-baseline-byte-witness.json"
)

CANDIDATE05_BOUNDARY_COUNTS = (100_000, 500_000, 1_000_000)
CANDIDATE05_TERMINAL_CERTIFICATE_SHA256 = (
    "f2a472511062a96e4b99fbdd911d1e4cf301ffc57ee0d005ff79f3292b5deba2"
)
CANDIDATE05_TERMINAL_MANIFEST_SHA256 = (
    "e3659295ac8dba4de20006ef28261cb426724c57b0caa14f7745595fb07902ba"
)
CANDIDATE05_TERMINAL_TREE_SHA256 = (
    "91bf5ddb17932402729e92874b00fc8b3e84276814659c7cf6e38998c36a0d2c"
)
CANDIDATE05_PRODUCER_CODE_IDENTITY = (
    "eb648dca8d36fe1deb81b6c517611ad50228b20e6a06f8cf9bb3489faf26b3f1"
)
CANDIDATE05_PRODUCER_RUNTIME_IDENTITY = (
    "eddf683b6a5bd017bcb5bc20230ba16bd7fad46131907e69339f7c9a3aef89e0"
)
CANDIDATE05_CONFIG_IDENTITY = (
    "51c86ce99f18d6abefc4c90a0330392b63a344b0b9167af813eba1e7de5bdd6d"
)
CANDIDATE05_STDOUT_SHA256 = (
    "5e6407914e908cf22c08f210ea76d3ff89264d98488a6a3701dfa959b0c15ff8"
)
CANDIDATE05_STDOUT_SIZE_BYTES = 5_209_155
BASELINE_BYTE_WITNESS_SHA256 = (
    "32c30490eb3a9934165a67fd76b0127fb698316710fdadd67b08be081335c740"
)
BASELINE_BYTE_WITNESS_SIZE_BYTES = 32_222
ACQUIRED_VERIFIER_BASELINE_IDENTITY = (
    "fa0e55fb4a42488eaa52a69355909c578f45994c64e2849df3e859a0089c5936"
)
ACQUIRED_VERIFIER_FILE_COUNT = 138
BASELINE_COMMIT = "f6c34d3c1e37bccf7ae72ef26cb8d8797dda8ed5"
PRODUCER_DEPENDENCY_CLOSURE_SHA256 = (
    "e8bc7f8f4e3fce05bbb5681b95963414cea6d26a0de813d3f22a39a30a0c9bb7"
)
PRODUCER_DEPENDENCY_CLOSURE_FILE_COUNT = 104
PRODUCER_DEPENDENCY_ENTRYPOINTS = (
    "hyperlab.paper.storage_v4.capacity_runner",
    "hyperlab.paper.storage_v4.phase1c_workers",
    "hyperlab.paper.storage_v4.phase1c_workloads",
)


def _emit(
    *,
    phase: str,
    status: str,
    stream: TextIO | None = None,
    **payload: object,
) -> None:
    event = {"phase": phase, "status": status, **payload}
    destination = sys.stdout if stream is None else stream
    print(
        canonical_json_bytes(event).decode("utf-8"),
        file=destination,
        flush=True,
    )


def _absolute_path(value: str, *, label: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise Phase1CSuccessorError(f"{label} must be an absolute path")
    return path


def _expectations() -> Any:
    return Phase1CSuccessorExpectations(
        boundary_commit_counts=CANDIDATE05_BOUNDARY_COUNTS,
        terminal_certificate_sha256=CANDIDATE05_TERMINAL_CERTIFICATE_SHA256,
        terminal_manifest_sha256=CANDIDATE05_TERMINAL_MANIFEST_SHA256,
        terminal_tree_sha256=CANDIDATE05_TERMINAL_TREE_SHA256,
        producer_code_identity=CANDIDATE05_PRODUCER_CODE_IDENTITY,
        producer_runtime_identity=CANDIDATE05_PRODUCER_RUNTIME_IDENTITY,
        config_identity=CANDIDATE05_CONFIG_IDENTITY,
        producer_stdout_size_bytes=CANDIDATE05_STDOUT_SIZE_BYTES,
        workload_profile="GOLDEN_SHAPED",
        workload_seed=20_260_825,
        generator_version="storage-v4-synthetic-capacity-v3",
        baseline_byte_witness_sha256=BASELINE_BYTE_WITNESS_SHA256,
        baseline_byte_witness_size_bytes=BASELINE_BYTE_WITNESS_SIZE_BYTES,
        acquired_verifier_baseline_identity=ACQUIRED_VERIFIER_BASELINE_IDENTITY,
        acquired_verifier_file_count=ACQUIRED_VERIFIER_FILE_COUNT,
        baseline_commit=BASELINE_COMMIT,
        producer_dependency_closure_sha256=(
            PRODUCER_DEPENDENCY_CLOSURE_SHA256
        ),
        producer_dependency_closure_file_count=(
            PRODUCER_DEPENDENCY_CLOSURE_FILE_COUNT
        ),
        producer_dependency_entrypoints=PRODUCER_DEPENDENCY_ENTRYPOINTS,
    )


def _build_config(repository_root: Path, output_root: Path) -> Any:
    if repository_root != REPOSITORY_ROOT:
        raise Phase1CSuccessorError(
            "repository root differs from the path-bound acquired verifier witness"
        )
    return Phase1CSuccessorConfig(
        repository_root=repository_root,
        baseline_byte_witness_path=BASELINE_BYTE_WITNESS_PATH,
        source_mission_root=CANDIDATE05_MISSION_ROOT,
        capacity_candidate_root=CANDIDATE05_CAPACITY_ROOT,
        boundary_certificate_root=CANDIDATE05_BOUNDARY_ROOT,
        producer_stdout_log=CANDIDATE05_STDOUT_LOG,
        producer_stdout_sha256=CANDIDATE05_STDOUT_SHA256,
        run06_candidate_root=RUN06_CANDIDATE_ROOT,
        receipt_root=output_root,
        expectations=_expectations(),
    )


def _reattest_progress(payload: object) -> None:
    if type(payload) is not dict:
        raise Phase1CSuccessorError("successor progress event must be an exact mapping")
    event = cast(dict[str, object], payload)
    expected = {
        "bytes_hashed",
        "candidate_root",
        "files_completed",
        "files_total",
        "phase",
        "status",
    }
    if set(event) != expected:
        raise Phase1CSuccessorError("successor progress event fields differ")
    if event["phase"] != "phase1c_candidate_tree_hash" or event["status"] != "RUNNING":
        raise Phase1CSuccessorError("successor progress event phase/status differs")
    _emit(
        phase=event["phase"],
        status=event["status"],
        bytes_hashed=event["bytes_hashed"],
        candidate_root=event["candidate_root"],
        files_completed=event["files_completed"],
        files_total=event["files_total"],
    )


def _is_reparse(observed: os.stat_result) -> bool:
    attributes = int(getattr(observed, "st_file_attributes", 0))
    mask = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & mask)


def _require_direct_directory(path: Path, *, label: str) -> Path:
    if not path.is_absolute():
        raise Phase1CSuccessorError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
        observed = os.lstat(path)
    except OSError as error:
        raise Phase1CSuccessorError(f"{label} is missing") from error
    if (
        resolved != path
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or _is_reparse(observed)
    ):
        raise Phase1CSuccessorError(f"{label} is indirect or unsafe")
    return resolved


def _paths_overlap(first: Path, second: Path) -> bool:
    left = os.path.normcase(os.path.abspath(os.fspath(first)))
    right = os.path.normcase(os.path.abspath(os.fspath(second)))
    try:
        common = os.path.commonpath((left, right))
    except ValueError:
        return False
    return common in (left, right)


def _create_gate_root(
    gate_root: Path,
    *,
    repository_root: Path,
    receipt_root: Path,
) -> Path:
    repository = _require_direct_directory(repository_root, label="repository root")
    receipt = _require_direct_directory(receipt_root, label="successor receipt root")
    if repository != REPOSITORY_ROOT:
        raise Phase1CSuccessorError("gate repository differs from path-bound checkout")
    if not gate_root.is_absolute():
        raise Phase1CSuccessorError("successor gate root must be absolute")
    parent = _require_direct_directory(gate_root.parent, label="gate root parent")
    if gate_root.exists() or gate_root.is_symlink():
        raise Phase1CSuccessorError("successor gate root must be absent")
    for forbidden in (
        repository,
        CANDIDATE05_MISSION_ROOT,
        CANDIDATE05_CAPACITY_ROOT,
        CANDIDATE05_BOUNDARY_ROOT,
        RUN06_CANDIDATE_ROOT,
        receipt,
    ):
        if _paths_overlap(gate_root, forbidden):
            raise Phase1CSuccessorError("successor gate root overlaps a forbidden root")
    try:
        gate_root.mkdir()
        fsync_directory(parent)
        (gate_root / "logs").mkdir()
        (gate_root / "pytest").mkdir()
        (gate_root / "pytest" / "targeted").mkdir()
        (gate_root / "pytest" / "global").mkdir()
        fsync_directory(gate_root / "pytest")
        fsync_directory(gate_root)
    except OSError as error:
        raise Phase1CSuccessorError("successor gate root creation failed") from error
    return _require_direct_directory(gate_root, label="successor gate root")


def _gate_environment() -> dict[str, str]:
    environment = dict(os.environ)
    denied_exact = {
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_NOSYSTEM",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_SYSTEM",
        "GIT_DIR",
        "GIT_EXTERNAL_DIFF",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_WORK_TREE",
        "MYPY_CONFIG_FILE",
        "MYPYPATH",
        "PYTHONHOME",
        "PYTHONINSPECT",
        "PYTHONOPTIMIZE",
        "PYTHONPATH",
        "PYTHONSTARTUP",
        "PYTHONUSERBASE",
        "PYTHONWARNINGS",
    }
    for name in tuple(environment):
        if name in denied_exact or name.startswith(("COVERAGE", "COV_CORE_", "PYTEST_")):
            environment.pop(name, None)
    environment.update(
        {
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_PAGER": "",
            "GIT_TERMINAL_PROMPT": "0",
            "NO_COLOR": "1",
            "PIP_NO_INDEX": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONHASHSEED": "0",
            "PYTHONNOUSERSITE": "1",
            "UV_OFFLINE": "1",
        }
    )
    return environment


def _read_temporary_output(stream: BinaryIO, *, label: str) -> bytes:
    stream.flush()
    size = os.fstat(stream.fileno()).st_size
    if size > _MAX_GATE_LOG_BYTES:
        raise Phase1CSuccessorError(f"{label} exceeds the gate output bound")
    stream.seek(0)
    payload = stream.read()
    if type(payload) is not bytes or len(payload) != size:
        raise Phase1CSuccessorError(f"{label} could not be read exactly")
    return payload


def _gate_log_bytes(
    *, purpose: str, command: tuple[str, ...], stdout: bytes, stderr: bytes
) -> bytes:
    if len(stdout) + len(stderr) > _MAX_GATE_LOG_BYTES:
        raise Phase1CSuccessorError("combined gate output exceeds its bound")
    header = canonical_json_bytes(
        {
            "artifact": "HYPERLAB_PHASE1C_SUCCESSOR_GATE_LOG_V1",
            "command": list(command),
            "purpose": purpose,
            "stderr_bytes": len(stderr),
            "stdout_bytes": len(stdout),
        }
    )
    return b"".join(
        (
            header,
            b"\nSTDOUT\n",
            stdout,
            b"\nSTDERR\n",
            stderr,
        )
    )


@dataclass(frozen=True, slots=True)
class _GateExecution:
    purpose: str
    command: tuple[str, ...]
    exit_code: int
    output_sha256: str
    output_log_path: Path
    output_log_size_bytes: int
    stdout_bytes: int
    stderr_bytes: int
    timeout_seconds: int

    @property
    def summary(self) -> str:
        return (
            f"exit_code={self.exit_code}; timeout_seconds={self.timeout_seconds}; "
            f"stdout_bytes={self.stdout_bytes}; stderr_bytes={self.stderr_bytes}"
        )


def _run_gate_command(
    command: tuple[str, ...],
    *,
    cwd: Path,
    purpose: str,
    timeout_seconds: int,
    output_log_path: Path,
) -> _GateExecution:
    if type(timeout_seconds) is not int or timeout_seconds < 1:
        raise Phase1CSuccessorError("gate timeout must be a positive exact integer")
    _emit(
        phase="phase1c_successor_gates",
        status="GATE_RUNNING",
        purpose=purpose,
        timeout_seconds=timeout_seconds,
    )
    timed_out = False
    with tempfile.TemporaryFile(mode="w+b") as stdout_stream, tempfile.TemporaryFile(
        mode="w+b"
    ) as stderr_stream:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=_gate_environment(),
            stdin=subprocess.DEVNULL,
            stdout=stdout_stream,
            stderr=stderr_stream,
            shell=False,
        )
        try:
            exit_code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            process.kill()
            try:
                exit_code = process.wait(timeout=30)
            except subprocess.TimeoutExpired as error:
                raise Phase1CSuccessorError(
                    f"gate process did not terminate after timeout: {purpose}"
                ) from error
        stdout = _read_temporary_output(cast(BinaryIO, stdout_stream), label="gate stdout")
        stderr = _read_temporary_output(cast(BinaryIO, stderr_stream), label="gate stderr")
    log_bytes = _gate_log_bytes(
        purpose=purpose, command=command, stdout=stdout, stderr=stderr
    )
    durable_publish_immutable(output_log_path, log_bytes)
    digest = hashlib.sha256(log_bytes).hexdigest()
    if timed_out:
        raise Phase1CSuccessorError(
            f"gate timed out after {timeout_seconds} seconds: {purpose}"
        )
    if exit_code != 0:
        raise Phase1CSuccessorError(
            f"gate exited nonzero ({exit_code}): {purpose}"
        )
    execution = _GateExecution(
        purpose=purpose,
        command=command,
        exit_code=exit_code,
        output_sha256=digest,
        output_log_path=output_log_path,
        output_log_size_bytes=len(log_bytes),
        stdout_bytes=len(stdout),
        stderr_bytes=len(stderr),
        timeout_seconds=timeout_seconds,
    )
    _emit(
        phase="phase1c_successor_gates",
        status="GATE_COMPLETE",
        purpose=purpose,
        output_sha256=digest,
        output_size_bytes=len(log_bytes),
    )
    return execution


def _stable_file_witness(path: Path, *, label: str) -> tuple[int, str]:
    payload = _read_stable_regular_file(
        path, label=label, maximum_bytes=_MAX_GATE_SOURCE_BYTES
    )
    return len(payload), hashlib.sha256(payload).hexdigest()


def _targeted_source_witnesses(repository_root: Path) -> tuple[tuple[str, str], ...]:
    return tuple(
        (
            relative,
            _stable_file_witness(
                repository_root / Path(relative), label=f"targeted source {relative}"
            )[1],
        )
        for relative in SUCCESSOR_TARGETED_TEST_PATHS
    )


def _targeted_command(gate_root: Path) -> tuple[str, ...]:
    return (
        sys.executable,
        "-m",
        "pytest",
        *SUCCESSOR_TARGETED_TEST_PATHS,
        "-p",
        "no:cacheprovider",
        "--basetemp",
        str(gate_root / "pytest" / "targeted"),
    )


def _closure_commands(gate_root: Path) -> tuple[tuple[str, tuple[str, ...]], ...]:
    python = sys.executable
    return (
        (
            "V10_CHECK",
            (python, "scripts/generate_phase12_live_paper_artifacts.py", "--check"),
        ),
        (
            "PHASE05_CHECK",
            (python, "scripts/generate_phase05_paper_evidence.py", "--check"),
        ),
        ("RUFF_GLOBAL_FINAL", (python, "-m", "ruff", "check", ".")),
        ("MYPY_HYPERLAB_FINAL", (python, "-m", "mypy", "src/hyperlab")),
        (
            "PYTEST_GLOBAL_FINAL_SINGLE_RUN",
            (
                python,
                "-m",
                "pytest",
                "-p",
                "no:cacheprovider",
                "--basetemp",
                str(gate_root / "pytest" / "global"),
            ),
        ),
        (
            "GIT_DIFF_CHECK_FINAL",
            ("git", "-c", "core.whitespace=cr-at-eol", "diff", "--check"),
        ),
    )


def _run_gates(arguments: argparse.Namespace) -> int:
    phase = "phase1c_successor_gates"
    repository_root = _absolute_path(
        arguments.repository_root, label="repository root"
    )
    receipt_root = _absolute_path(arguments.receipt_root, label="receipt root")
    requested_gate_root = _absolute_path(arguments.gate_root, label="gate root")
    gate_root = _create_gate_root(
        requested_gate_root,
        repository_root=repository_root,
        receipt_root=receipt_root,
    )
    _emit(
        phase=phase,
        status="RUNNING",
        candidate_opened=False,
        global_pytest_expected_runs=1,
        gate_root=str(gate_root),
    )
    v9_path = repository_root / Path(SUCCESSOR_V9_RELATIVE_PATH)
    v9_before = _stable_file_witness(v9_path, label="pinned V9 before gates")
    if v9_before != (SUCCESSOR_V9_SIZE_BYTES, SUCCESSOR_V9_SHA256):
        raise Phase1CSuccessorError("pinned V9 differs before successor gates")
    sources_before = _targeted_source_witnesses(repository_root)
    targeted_execution = _run_gate_command(
        _targeted_command(gate_root),
        cwd=repository_root,
        purpose="SUCCESSOR_TARGETED_TESTS",
        timeout_seconds=_TARGETED_TIMEOUT_SECONDS,
        output_log_path=gate_root / "logs" / SUCCESSOR_TARGETED_LOG_NAME,
    )
    sources_after = _targeted_source_witnesses(repository_root)
    if sources_after != sources_before:
        raise Phase1CSuccessorError("targeted sources changed during successor tests")
    targeted = Phase1CSuccessorTestWitness(
        command=targeted_execution.command,
        exit_code=targeted_execution.exit_code,
        output_sha256=targeted_execution.output_sha256,
        source_files=sources_after,
        summary=targeted_execution.summary,
        output_log_path=str(targeted_execution.output_log_path),
        output_log_size_bytes=targeted_execution.output_log_size_bytes,
    )
    targeted_bytes = canonical_json_bytes(targeted.payload())
    targeted_path = gate_root / SUCCESSOR_TARGETED_WITNESS_NAME
    durable_publish_immutable(targeted_path, targeted_bytes)
    closure_witnesses: list[Any] = []
    for index, (purpose, command) in enumerate(
        _closure_commands(gate_root), start=1
    ):
        execution = _run_gate_command(
            command,
            cwd=repository_root,
            purpose=purpose,
            timeout_seconds=_GATE_TIMEOUT_SECONDS[purpose],
            output_log_path=(
                gate_root
                / "logs"
                / f"{index:02d}-{purpose.lower().replace('_', '-')}.log"
            ),
        )
        closure_witnesses.append(
            Phase1CSuccessorCommandWitness(
                purpose=purpose,
                command=execution.command,
                exit_code=execution.exit_code,
                output_sha256=execution.output_sha256,
                summary=execution.summary,
                output_log_path=str(execution.output_log_path),
                output_log_size_bytes=execution.output_log_size_bytes,
            )
        )
    v9_after = _stable_file_witness(v9_path, label="pinned V9 after gates")
    if v9_after != v9_before:
        raise Phase1CSuccessorError("pinned V9 changed during successor gates")
    closure = Phase1CSuccessorClosureWitness(
        commands=tuple(closure_witnesses),
        v9=Phase1CSuccessorV9ByteWitness(
            path=SUCCESSOR_V9_RELATIVE_PATH,
            size_bytes=v9_after[0],
            before_sha256=v9_before[1],
            after_sha256=v9_after[1],
        ),
    )
    closure_bytes = canonical_json_bytes(closure.payload())
    closure_path = gate_root / SUCCESSOR_CLOSURE_WITNESS_NAME
    durable_publish_immutable(closure_path, closure_bytes)
    _emit(
        phase=phase,
        status="COMPLETE",
        candidate_opened=False,
        closure_witness_path=str(closure_path),
        closure_witness_sha256=hashlib.sha256(closure_bytes).hexdigest(),
        global_pytest_runs=1,
        targeted_witness_path=str(targeted_path),
        targeted_witness_sha256=hashlib.sha256(targeted_bytes).hexdigest(),
        terminal_signal="SUCCESSOR_GATE_WITNESSES_PUBLISHED",
        v9_unchanged=True,
    )
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only Phase 1C candidate-05 successor closure"
    )
    subparsers = parser.add_subparsers(dest="mode", required=True)
    reattest = subparsers.add_parser("reattest")
    reattest.add_argument("--output-root", required=True)
    reattest.add_argument("--repository-root", default=str(REPOSITORY_ROOT))
    finalize = subparsers.add_parser("finalize")
    finalize.add_argument("--output-root", required=True)
    finalize.add_argument("--repository-root", default=str(REPOSITORY_ROOT))
    finalize.add_argument("--receipt-sha256", required=True)
    finalize.add_argument("--targeted-tests-witness", required=True)
    finalize.add_argument("--closure-witness", required=True)
    gates = subparsers.add_parser("gates")
    gates.add_argument("--gate-root", required=True)
    gates.add_argument("--receipt-root", required=True)
    gates.add_argument("--repository-root", default=str(REPOSITORY_ROOT))
    return parser


def _run_reattest(arguments: argparse.Namespace) -> int:
    phase = "phase1c_successor_reattest"
    repository_root = _absolute_path(
        arguments.repository_root, label="repository root"
    )
    output_root = _absolute_path(arguments.output_root, label="output root")
    _emit(
        phase=phase,
        status="RUNNING",
        candidate="native-capacity-05",
        commits_ingested_during_succession=0,
        run06_commits=0,
    )
    receipt = reattest_phase1c_successor(
        _build_config(repository_root, output_root),
        progress=_reattest_progress,
    )
    _emit(
        phase=phase,
        status="COMPLETE",
        complete_published=False,
        receipt_path=str(receipt.path),
        receipt_sha256=receipt.sha256,
        successor_status=SUCCESSOR_REATTESTED_STATUS,
        terminal_signal="SUCCESSOR_RECEIPT_PUBLISHED_NO_COMPLETE",
    )
    return 0


def _run_finalize(arguments: argparse.Namespace) -> int:
    phase = "phase1c_successor_finalize"
    repository_root = _absolute_path(
        arguments.repository_root, label="repository root"
    )
    output_root = _absolute_path(arguments.output_root, label="output root")
    targeted_path = _absolute_path(
        arguments.targeted_tests_witness, label="targeted test witness"
    )
    closure_path = _absolute_path(
        arguments.closure_witness, label="closure witness"
    )
    _emit(
        phase=phase,
        status="RUNNING",
        candidate_reopened=False,
        commits_ingested_during_succession=0,
    )
    targeted = load_phase1c_successor_test_witness(targeted_path)
    closure = load_phase1c_successor_closure_witness(closure_path)
    publication = finalize_phase1c_successor_closure(
        repository_root=repository_root,
        receipt_root=output_root,
        expected_receipt_sha256=arguments.receipt_sha256,
        targeted_tests=targeted,
        closure=closure,
    )
    _emit(
        phase=phase,
        status="COMPLETE",
        complete_path=str(publication.root / "COMPLETE"),
        complete_sha256=publication.complete_sha256,
        terminal_signal="COMPLETE",
        verdict=PHASE1C_TARGET_NOT_MET_VERDICT,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.mode == "reattest":
            return _run_reattest(arguments)
        if arguments.mode == "finalize":
            return _run_finalize(arguments)
        if arguments.mode == "gates":
            return _run_gates(arguments)
        raise Phase1CSuccessorError("unknown successor mode")
    except (OSError, ValueError, TypeError, Phase1CSuccessorError) as error:
        _emit(
            phase=f"phase1c_successor_{arguments.mode}",
            status="FAILED",
            stream=sys.stderr,
            error=str(error),
            error_type=type(error).__name__,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
