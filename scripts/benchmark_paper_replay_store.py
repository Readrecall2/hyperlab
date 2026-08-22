from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter, process_time

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


def _create_source(
    temporary_directory: TemporaryDirectory[str],
    *,
    commits: int,
    reconcile_every: int = 0,
    ledger_every: int = 0,
) -> tuple[Path, dict[str, object]]:
    config = _config()
    if reconcile_every < 0 or reconcile_every == 1:
        raise ValueError("reconcile_every must be zero or at least two")
    if ledger_every < 0 or ledger_every == 1:
        raise ValueError("ledger_every must be zero or at least two")
    path = Path(temporary_directory.name) / "synthetic-source.sqlite3"
    store = PaperStore._create_temporary_historical_replay(
        temporary_directory,
        filename=path.name,
    )
    engine = PaperEngine._for_historical_replay(store, config)
    market_sequence = 0
    try:
        engine.start()
        for commit_sequence in range(2, commits + 1):
            observed_at = _START + timedelta(milliseconds=commit_sequence)
            if reconcile_every and commit_sequence % reconcile_every == 0:
                engine.reconcile(as_of=observed_at)
            elif ledger_every and commit_sequence % ledger_every == 0:
                funding_source_id = hashlib.sha256(
                    f"synthetic-ledger:{commit_sequence}".encode("ascii")
                ).hexdigest()
                engine.post_funding(
                    instrument=_INSTRUMENT,
                    amount=Decimal("0"),
                    occurred_at=observed_at,
                    source_event_id=funding_source_id,
                )
            else:
                market_sequence += 1
                engine.process_market(
                    MarketEvent.create(
                        received_at=observed_at,
                        instrument=_INSTRUMENT,
                        bid_price=Decimal("100"),
                        ask_price=Decimal("101"),
                        bid_depth=Decimal("100"),
                        ask_depth=Decimal("100"),
                        source_sequence=market_sequence,
                    )
                )
            if commit_sequence % 10_000 == 1:
                print(
                    f"generated {commit_sequence}/{commits} synthetic commits",
                    file=sys.stderr,
                )
        integrity = store.inspect_integrity_readonly(config.run_id)
        run = store.get_run(config.run_id)
        projection = store.get_projection(config.run_id)
        semantic_errors = engine._ledger_reconciliation_errors(projection)
        if not integrity.ok or run.commit_sequence != commits or semantic_errors:
            raise RuntimeError("synthetic source failed integrity, commit-count, or semantic validation")
        head_identity = list(run.head_identity)
        reconcile_input_count = sum(
            1
            for _record in store.iter_inputs(
                config.run_id,
                input_type="RECONCILE",
            )
        )
        ledger_entry_count = sum(1 for _entry in store.iter_ledger_entries(config.run_id))
        alert_count = len(store.get_alerts(config.run_id))
        projection_hash = projection.canonical_hash
    finally:
        store.close()
    source_identity: dict[str, object] = {
        "alert_count": alert_count,
        "commit_sequence": run.commit_sequence,
        "head_identity": head_identity,
        "ledger_entry_count": ledger_entry_count,
        "ledger_every": ledger_every,
        "projection_hash": projection_hash,
        "reconcile_every": reconcile_every,
        "reconcile_input_count": reconcile_input_count,
        "run_id": config.run_id,
        "sha256": _sha256(path),
    }
    return path, source_identity


def _run_worker(source_path: Path, *, mode: str) -> dict[str, object]:
    if mode not in {"reference", "optimized"}:
        raise ValueError("worker mode must be reference or optimized")
    source_sha256 = _sha256(source_path)
    source_store = PaperStore(source_path, initialize=False)
    runs = source_store.list_runs()
    if len(runs) != 1:
        source_store.close()
        raise RuntimeError("benchmark source must contain exactly one Paper run")
    run = runs[0]
    if run.config_snapshot.get("data_calibration_status") != "SYNTHETIC" or run.config_snapshot.get(
        "parameters"
    ) != {"fixture": "SYNTHETIC_REPLAY_STORE_BENCHMARK"}:
        source_store.close()
        raise RuntimeError("benchmark worker refuses a non-synthetic or unknown source database")
    config = PaperRunConfig.from_dict(run.config_snapshot)
    instrumentation = {
        "bounded_prefix_certification_count": 0,
        "final_full_integrity_count": 0,
        "historical_ledger_reconciliation_count": 0,
        "reference_full_prefix_rescan_count": 0,
        "reference_prefix_commit_work": 0,
    }
    sql_counts: dict[str, int] = {
        "begin": 0,
        "delete": 0,
        "insert": 0,
        "other": 0,
        "select": 0,
        "update": 0,
    }
    target_identity: dict[str, object] = {}
    traced_connections: set[int] = set()
    original_connect = PaperStore._connect
    original_inspect = PaperStore.inspect_integrity_readonly
    original_ledger_reconciliation = PaperEngine._ledger_reconciliation_errors
    original_prefix_certification = PaperEngine._verified_historical_replay_prefix
    original_temporary_store = paper_engine_module._temporary_historical_replay_store

    def trace_sql(statement: str) -> None:
        verb = statement.lstrip().partition(" ")[0].lower()
        key = verb if verb in sql_counts and verb != "other" else "other"
        sql_counts[key] += 1

    def instrumented_connect(store: PaperStore) -> sqlite3.Connection:
        connection = original_connect(store)
        if store.historical_replay_only and id(connection) not in traced_connections:
            traced_connections.add(id(connection))
            connection.set_trace_callback(trace_sql)
        return connection

    def counted_inspect(store: PaperStore, run_id: str) -> object:
        if store.historical_replay_only:
            instrumentation["final_full_integrity_count"] += 1
        return original_inspect(store, run_id)

    def counted_ledger_reconciliation(
        engine: PaperEngine,
        projection: PaperProjection,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> tuple[str, ...]:
        if engine.store.historical_replay_only:
            instrumentation["historical_ledger_reconciliation_count"] += 1
        return original_ledger_reconciliation(
            engine,
            projection,
            should_stop=should_stop,
        )

    def reference_prefix(engine: PaperEngine) -> object:
        prefix = engine.store.get_run(engine.run_id).commit_sequence
        instrumentation["reference_full_prefix_rescan_count"] += 1
        instrumentation["reference_prefix_commit_work"] += prefix
        return engine._verify_durable_state()

    def optimized_prefix(engine: PaperEngine) -> object:
        instrumentation["bounded_prefix_certification_count"] += 1
        return original_prefix_certification(engine)

    @contextmanager
    def captured_temporary_store() -> Iterator[PaperStore]:
        with original_temporary_store() as replay_store:
            yield replay_store
            target_run = replay_store.get_run(config.run_id)
            target_identity.update(
                {
                    "head_identity": list(target_run.head_identity),
                    "projection_hash": replay_store.get_projection(config.run_id).canonical_hash,
                }
            )

    PaperStore._connect = instrumented_connect  # type: ignore[method-assign]
    PaperStore.inspect_integrity_readonly = counted_inspect  # type: ignore[method-assign]
    PaperEngine._ledger_reconciliation_errors = (  # type: ignore[method-assign]
        counted_ledger_reconciliation
    )
    PaperEngine._verified_historical_replay_prefix = (  # type: ignore[method-assign]
        reference_prefix if mode == "reference" else optimized_prefix
    )
    paper_engine_module._temporary_historical_replay_store = captured_temporary_store
    started_wall = perf_counter()
    started_cpu = process_time()
    try:
        replayed = replay_paper_run(source_store, config.run_id)
        cpu_seconds = process_time() - started_cpu
        elapsed_seconds = perf_counter() - started_wall
        source_after = source_store.get_run(config.run_id)
        durable = source_store.get_projection(config.run_id)
    finally:
        paper_engine_module._temporary_historical_replay_store = original_temporary_store
        PaperEngine._verified_historical_replay_prefix = (  # type: ignore[method-assign]
            original_prefix_certification
        )
        PaperEngine._ledger_reconciliation_errors = (  # type: ignore[method-assign]
            original_ledger_reconciliation
        )
        PaperStore.inspect_integrity_readonly = original_inspect  # type: ignore[method-assign]
        PaperStore._connect = original_connect  # type: ignore[method-assign]
        source_store.close()
    if _sha256(source_path) != source_sha256:
        raise RuntimeError("canonical replay modified the shared source database")
    if replayed.projection_hash != durable.canonical_hash:
        raise RuntimeError("benchmark replay projection differs from its source")
    if (
        target_identity.get("head_identity") != list(source_after.head_identity)
        or target_identity.get("projection_hash") != durable.canonical_hash
    ):
        raise RuntimeError("benchmark replay target differs from the canonical source")
    peak_rss, peak_rss_source = _peak_rss_bytes()
    return {
        "commit_sequence": source_after.commit_sequence,
        "commits_per_second": source_after.commit_sequence / elapsed_seconds,
        "cpu_seconds": cpu_seconds,
        "elapsed_seconds": elapsed_seconds,
        "exact_target_equality": {
            "alerts": True,
            "events": True,
            "head": True,
            "ledger": True,
            "projection": True,
        },
        "head_identity": list(source_after.head_identity),
        "instrumentation": instrumentation,
        "mode": mode,
        "measurement_path": (
            "replay_paper_run: source integrity + event replay + canonical input "
            "replay + final target integrity + final ledger reconciliation"
        ),
        "peak_rss_bytes": peak_rss,
        "peak_rss_source": peak_rss_source,
        "source_projection_hash": durable.canonical_hash,
        "source_sha256": source_sha256,
        "sqlite_statement_counts": sql_counts,
        "target_head_identity": target_identity["head_identity"],
        "target_projection_hash": target_identity["projection_hash"],
        "target_sqlite_mode": "disposable MEMORY/OFF in both modes",
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
        raise RuntimeError(f"{mode} benchmark worker failed: {error.stderr.strip()}") from error
    return json.loads(completed.stdout)


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark disposable canonical Paper replay storage")
    parser.add_argument(
        "--commits",
        type=int,
        choices=(1_000, 100_000, 150_000, 250_000, 252_500),
        default=1_000,
        help="synthetic source size; 100000 through 252500 model supervisor-scale history",
    )
    parser.add_argument(
        "--mode",
        choices=("reference", "optimized", "both"),
        default="both",
    )
    parser.add_argument(
        "--reconcile-every",
        type=int,
        default=0,
        help="insert one exact RECONCILE input at each Nth synthetic commit",
    )
    parser.add_argument(
        "--ledger-every",
        type=int,
        default=0,
        help="replace each Nth non-RECONCILE commit with a balanced funding ledger event",
    )
    parser.add_argument(
        "--worker-order",
        choices=("reference-first", "optimized-first"),
        default="optimized-first",
        help="make cold/warm worker order explicit and reproducible",
    )
    parser.add_argument(
        "--worker",
        choices=("reference", "optimized"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--source-database", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    print(_WARNING, file=sys.stderr)
    if args.worker is not None:
        if args.source_database is None:
            parser.error("--worker requires --source-database")
        print(
            json.dumps(
                _run_worker(args.source_database, mode=args.worker),
                sort_keys=True,
            )
        )
        return 0

    if args.mode == "both":
        modes = (
            ("reference", "optimized")
            if args.worker_order == "reference-first"
            else ("optimized", "reference")
        )
    else:
        modes = (args.mode,)
    script = Path(__file__).resolve()
    temporary_directory = TemporaryDirectory(prefix="hyperlab-paper-replay-benchmark-")
    try:
        source, source_identity = _create_source(
            temporary_directory,
            commits=args.commits,
            reconcile_every=args.reconcile_every,
            ledger_every=args.ledger_every,
        )
        results: list[dict[str, object]] = []
        expected_reconciles = int(source_identity["reconcile_input_count"])
        for order_position, mode in enumerate(modes, start=1):
            result = _subprocess_worker(script, source, mode=mode)
            result["cache_condition"] = (
                "first worker; coldest available OS-cache position"
                if order_position == 1
                else "later worker; source pages may be OS-cache warm"
            )
            result["order_position"] = order_position
            if (
                result["source_sha256"] != source_identity["sha256"]
                or result["head_identity"] != source_identity["head_identity"]
                or result["source_projection_hash"] != source_identity["projection_hash"]
                or result["target_head_identity"] != source_identity["head_identity"]
                or result["target_projection_hash"] != source_identity["projection_hash"]
                or not all(bool(value) for value in dict(result["exact_target_equality"]).values())
            ):
                raise RuntimeError("benchmark modes did not consume and reproduce the same exact source")
            instrumentation = dict(result["instrumentation"])
            expected_ledger_checks = expected_reconciles + 2 if mode == "reference" else 2
            if (
                instrumentation["final_full_integrity_count"] != 1
                or instrumentation["historical_ledger_reconciliation_count"] != expected_ledger_checks
            ):
                raise RuntimeError("benchmark worker did not include the intended final verification")
            if mode == "reference":
                if (
                    instrumentation["reference_full_prefix_rescan_count"] != expected_reconciles
                    or instrumentation["bounded_prefix_certification_count"] != 0
                ):
                    raise RuntimeError("reference worker did not exercise full prefix rescans")
            elif (
                instrumentation["bounded_prefix_certification_count"] != expected_reconciles
                or instrumentation["reference_full_prefix_rescan_count"] != 0
            ):
                raise RuntimeError("optimized worker did not exercise bounded prefix certification")
            results.append(result)
    finally:
        temporary_directory.cleanup()
    report: dict[str, object] = {
        "artifact_status": "NON_ARTIFACT_SYNTHETIC_BENCHMARK",
        "source": source_identity,
        "warning": _WARNING,
        "worker_order": list(modes),
        "workers": results,
    }
    if len(results) == 2:
        by_mode = {str(result["mode"]): result for result in results}
        reference_seconds = float(by_mode["reference"]["elapsed_seconds"])
        optimized_seconds = float(by_mode["optimized"]["elapsed_seconds"])
        report["speedup_reference_over_optimized"] = reference_seconds / optimized_seconds
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
