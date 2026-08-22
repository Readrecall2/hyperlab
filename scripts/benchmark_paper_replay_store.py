from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

import hyperlab.paper.engine as paper_engine_module
from hyperlab.paper import (
    MarketEvent,
    PaperEngine,
    PaperExecutionConfig,
    PaperProjection,
    PaperRiskLimits,
    PaperRunConfig,
    PaperStore,
)
from hyperlab.paper.runtime import replay_paper_run

_START = datetime(2026, 8, 21, 12, tzinfo=UTC)
_INSTRUMENT = "HYPERLIQUID:BTC:perp"
_WARNING = (
    "SYNTHETIC PERFORMANCE FIXTURE ONLY: this benchmark is not market evidence, "
    "Paper validation, or profitability evidence."
)


def _config() -> PaperRunConfig:
    return PaperRunConfig(
        strategy_name="replay_store_benchmark",
        strategy_hash="a" * 64,
        parameters={"fixture": "SYNTHETIC_REPLAY_STORE_BENCHMARK"},
        data_hash="b" * 64,
        execution=PaperExecutionConfig(
            calibration_status="SYNTHETIC",
            source="deterministic-benchmark-fixture",
        ),
        risk=PaperRiskLimits(),
        seed=23,
        initial_cash=Decimal("100000"),
        validation_started_at=_START,
        data_calibration_status="SYNTHETIC",
        data_source="deterministic-benchmark-fixture",
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
        get_current_process = ctypes.windll.kernel32.GetCurrentProcess
        get_current_process.restype = ctypes.c_void_p
        get_process_memory_info = ctypes.windll.psapi.GetProcessMemoryInfo
        get_process_memory_info.argtypes = (
            ctypes.c_void_p,
            ctypes.POINTER(ProcessMemoryCounters),
            ctypes.c_ulong,
        )
        get_process_memory_info.restype = ctypes.c_int
        if get_process_memory_info(
            get_current_process(),
            ctypes.byref(counters),
            counters.cb,
        ):
            return int(counters.PeakWorkingSetSize), "windows-GetProcessMemoryInfo"
        return None, "windows-GetProcessMemoryInfo-unavailable"
    try:
        import resource
    except ImportError:
        return None, "peak-rss-unavailable"
    raw_peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    multiplier = 1 if sys.platform == "darwin" else 1024
    return raw_peak * multiplier, "resource-getrusage"


def _create_source(path: Path, *, commits: int) -> dict[str, object]:
    config = _config()
    store = PaperStore(path, historical_replay_only=True)
    engine = PaperEngine(store, config)
    engine.start()
    for sequence in range(1, commits):
        engine.process_market(
            MarketEvent.create(
                received_at=_START + timedelta(milliseconds=sequence),
                instrument=_INSTRUMENT,
                bid_price=Decimal("100"),
                ask_price=Decimal("101"),
                bid_depth=Decimal("100"),
                ask_depth=Decimal("100"),
                source_sequence=sequence,
            )
        )
        if sequence % 10_000 == 0:
            print(f"generated {sequence + 1}/{commits} synthetic commits", file=sys.stderr)
    integrity = store.inspect_integrity_readonly(config.run_id)
    run = store.get_run(config.run_id)
    if not integrity.ok or run.commit_sequence != commits:
        raise RuntimeError("synthetic source database failed integrity or commit-count validation")
    head_identity = list(run.head_identity)
    store.close()
    return {
        "commit_sequence": run.commit_sequence,
        "head_identity": head_identity,
        "run_id": config.run_id,
        "sha256": _sha256(path),
    }


class _LegacyReplayStore(PaperStore):
    """Benchmark-only model of the pre-fix short-lived DELETE/FULL replay target."""

    def __init__(self, path: Path | str, *args: object, **kwargs: object) -> None:
        kwargs["historical_replay_only"] = False
        super().__init__(path, *args, **kwargs)


def _run_worker(source_path: Path, *, mode: str) -> dict[str, object]:
    source_sha256 = _sha256(source_path)
    source_store = PaperStore(source_path, initialize=False)
    runs = source_store.list_runs()
    if len(runs) != 1:
        raise RuntimeError("benchmark source must contain exactly one Paper run")
    run = runs[0]
    if (
        run.config_snapshot.get("data_calibration_status") != "SYNTHETIC"
        or run.config_snapshot.get("parameters")
        != {"fixture": "SYNTHETIC_REPLAY_STORE_BENCHMARK"}
    ):
        raise RuntimeError("benchmark worker refuses a non-synthetic or unknown source database")
    config = PaperRunConfig.from_dict(run.config_snapshot)
    original_verify_input_replay = paper_engine_module.PaperEngine.verify_input_replay

    def legacy_verify_input_replay(engine: PaperEngine) -> PaperProjection:
        original_store_class = paper_engine_module.PaperStore
        paper_engine_module.PaperStore = _LegacyReplayStore
        try:
            return original_verify_input_replay(engine)
        finally:
            paper_engine_module.PaperStore = original_store_class

    if mode == "legacy":
        paper_engine_module.PaperEngine.verify_input_replay = (  # type: ignore[method-assign]
            legacy_verify_input_replay
        )
    started = perf_counter()
    try:
        replayed = replay_paper_run(source_store, config.run_id)
    finally:
        paper_engine_module.PaperEngine.verify_input_replay = (  # type: ignore[method-assign]
            original_verify_input_replay
        )
    elapsed = perf_counter() - started
    source_after = source_store.get_run(config.run_id)
    durable = source_store.get_projection(config.run_id)
    source_store.close()
    if _sha256(source_path) != source_sha256:
        raise RuntimeError("canonical replay modified the shared source database")
    if replayed.projection_hash != durable.canonical_hash:
        raise RuntimeError("benchmark replay projection differs from its source")
    peak_rss, peak_rss_source = _peak_rss_bytes()
    return {
        "commit_sequence": source_after.commit_sequence,
        "commits_per_second": source_after.commit_sequence / elapsed,
        "elapsed_seconds": elapsed,
        "head_identity": list(source_after.head_identity),
        "mode": mode,
        "measurement_path": (
            "replay_paper_run: integrity + event replay + canonical input replay"
        ),
        "peak_rss_bytes": peak_rss,
        "peak_rss_source": peak_rss_source,
        "source_sha256": source_sha256,
        "target_sqlite_mode": (
            "short-lived DELETE/FULL" if mode == "legacy" else "persistent MEMORY/OFF"
        ),
    }


def _subprocess_worker(script: Path, source: Path, *, mode: str) -> dict[str, object]:
    try:
        completed = subprocess.run(
            [
                sys.executable,
                str(script),
                "--worker",
                mode,
                "--source-database",
                str(source),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        raise RuntimeError(
            f"{mode} benchmark worker failed: {error.stderr.strip()}"
        ) from error
    return json.loads(completed.stdout)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark disposable canonical Paper replay storage"
    )
    parser.add_argument(
        "--commits",
        type=int,
        choices=(1_000, 100_000, 150_000),
        default=1_000,
        help="synthetic source size; 100000 and 150000 model supervisor-scale history",
    )
    parser.add_argument("--mode", choices=("legacy", "optimized", "both"), default="both")
    parser.add_argument("--worker", choices=("legacy", "optimized"), help=argparse.SUPPRESS)
    parser.add_argument("--source-database", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    print(_WARNING, file=sys.stderr)
    if args.worker is not None:
        if args.source_database is None:
            parser.error("--worker requires --source-database")
        print(json.dumps(_run_worker(args.source_database, mode=args.worker), sort_keys=True))
        return 0

    modes = ("legacy", "optimized") if args.mode == "both" else (args.mode,)
    script = Path(__file__).resolve()
    with TemporaryDirectory(prefix="hyperlab-paper-replay-benchmark-") as directory:
        source = Path(directory) / "synthetic-source.sqlite3"
        source_identity = _create_source(source, commits=args.commits)
        results = [_subprocess_worker(script, source, mode=mode) for mode in modes]
        for result in results:
            if (
                result["source_sha256"] != source_identity["sha256"]
                or result["head_identity"] != source_identity["head_identity"]
            ):
                raise RuntimeError("benchmark modes did not consume the same exact source database")
    report: dict[str, object] = {
        "artifact_status": "NON_ARTIFACT_SYNTHETIC_BENCHMARK",
        "source": source_identity,
        "warning": _WARNING,
        "workers": results,
    }
    if len(results) == 2:
        legacy = float(results[0]["elapsed_seconds"])
        optimized = float(results[1]["elapsed_seconds"])
        report["speedup_legacy_over_optimized"] = legacy / optimized
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
