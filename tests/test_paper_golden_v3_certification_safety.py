from __future__ import annotations

import hashlib
import json
import os
import stat
import threading
import time
from argparse import Namespace
from pathlib import Path

import pytest
from pytest import MonkeyPatch

import hyperlab.paper.golden_v3_certification as certification
import scripts.certify_paper_golden_v3 as cli
from hyperlab.paper.golden_v3_certification import (
    GoldenCertificationError,
    GoldenReplayDivergenceError,
    certify_golden_v3,
)


def _readonly_non_database(tmp_path: Path) -> tuple[Path, int, str]:
    source = tmp_path / "offline-copy.sqlite3"
    source.write_bytes(b"not-a-database")
    size = source.stat().st_size
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    source.chmod(stat.S_IREAD)
    return source, size, digest


def _certify_paths(
    source: Path,
    candidate: Path,
    sentinel: Path,
    *,
    size: int,
    digest: str,
) -> None:
    certify_golden_v3(
        source,
        candidate,
        "0" * 64,
        sentinel_path=sentinel,
        expected_source_size=size,
        expected_source_sha256=digest,
    )


def test_candidate_may_not_equal_or_contain_forbidden_sentinel(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source, size, digest = _readonly_non_database(tmp_path)
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)
    sentinel = tmp_path / "forbidden-original.sqlite3"
    try:
        with pytest.raises(GoldenCertificationError, match=r"sentinel|collision|contain"):
            _certify_paths(
                source,
                sentinel,
                sentinel,
                size=size,
                digest=digest,
            )
        assert not sentinel.exists()

        nested_candidate = sentinel / "candidate"
        with pytest.raises(GoldenCertificationError, match=r"sentinel|collision|contain"):
            _certify_paths(
                source,
                nested_candidate,
                sentinel,
                size=size,
                digest=digest,
            )
        assert not sentinel.exists()
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)


def test_source_and_candidate_reparse_paths_are_refused_before_candidate_creation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source, size, digest = _readonly_non_database(tmp_path)
    source_alias = tmp_path / "source-alias.sqlite3"
    real_output_parent = tmp_path / "real-output-parent"
    output_alias = tmp_path / "output-alias"
    real_output_parent.mkdir()
    try:
        try:
            source_alias.symlink_to(source)
            output_alias.symlink_to(real_output_parent, target_is_directory=True)
        except OSError as exc:
            pytest.skip(f"filesystem symlinks unavailable: {exc}")
        monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)

        source_candidate = tmp_path / "source-alias-candidate"
        with pytest.raises(GoldenCertificationError, match=r"symlink|junction|reparse"):
            _certify_paths(
                source_alias,
                source_candidate,
                tmp_path / "forbidden-original.sqlite3",
                size=size,
                digest=digest,
            )
        assert not source_candidate.exists()

        candidate = output_alias / "candidate"
        with pytest.raises(GoldenCertificationError, match=r"symlink|junction|reparse"):
            _certify_paths(
                source,
                candidate,
                tmp_path / "forbidden-original.sqlite3",
                size=size,
                digest=digest,
            )
        assert not (real_output_parent / "candidate").exists()
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)


@pytest.mark.parametrize("flagged_path", ["source", "candidate"])
def test_reparse_detection_is_fail_closed_without_symlink_privileges(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    flagged_path: str,
) -> None:
    source, size, digest = _readonly_non_database(tmp_path)
    candidate = tmp_path / "candidate"
    original = certification._has_reparse_component

    def synthetic_reparse(path: Path) -> bool:
        lexical = certification._lexical_absolute(path)
        expected = source if flagged_path == "source" else candidate
        if lexical == certification._lexical_absolute(expected):
            return True
        return original(path)

    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)
    monkeypatch.setattr(
        certification,
        "_has_reparse_component",
        synthetic_reparse,
    )
    try:
        with pytest.raises(GoldenCertificationError, match=r"symlink|junction|reparse"):
            _certify_paths(
                source,
                candidate,
                tmp_path / "forbidden-original.sqlite3",
                size=size,
                digest=digest,
            )
        assert not candidate.exists()
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)


def test_source_sentinel_hardlink_alias_is_refused_before_candidate_creation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source, size, digest = _readonly_non_database(tmp_path)
    sentinel = tmp_path / "forbidden-original.sqlite3"
    candidate = tmp_path / "candidate"
    try:
        try:
            os.link(source, sentinel)
        except OSError as exc:
            pytest.skip(f"filesystem hardlinks unavailable: {exc}")
        monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)
        with pytest.raises(GoldenCertificationError, match=r"hard.?link|same file|alias"):
            _certify_paths(
                source,
                candidate,
                sentinel,
                size=size,
                digest=digest,
            )
        assert not candidate.exists()
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)


def test_candidate_and_progress_sink_exist_before_source_prehash(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source, size, digest = _readonly_non_database(tmp_path)
    candidate = tmp_path / "candidate"
    observed: list[bool] = []

    def stop_at_prehash(_source: Path) -> dict[str, object]:
        observed.append(
            candidate.is_dir()
            and (candidate / "results").is_dir()
            and (candidate / "results" / "progress.jsonl").is_file()
        )
        raise GoldenCertificationError("synthetic prehash stop")

    def progress(record: object) -> None:
        del record
        path = candidate / "results" / "progress.jsonl"
        path.write_text("{}\n", encoding="utf-8", newline="\n")

    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)
    monkeypatch.setattr(certification, "_source_fingerprint", stop_at_prehash)
    try:
        with pytest.raises(GoldenCertificationError, match="synthetic prehash stop"):
            certify_golden_v3(
                source,
                candidate,
                "0" * 64,
                sentinel_path=tmp_path / "forbidden-original.sqlite3",
                expected_source_size=size,
                expected_source_sha256=digest,
                progress=progress,
            )
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)
    assert observed == [True]


def test_heartbeat_is_durable_and_async_failure_is_surfaced_before_complete(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    candidate = tmp_path / "candidate"
    results = candidate / "results"
    results.mkdir(parents=True)
    path = results / "progress.jsonl"
    progress = cli._ProgressJsonl(path, cli._JsonConsole())
    heartbeat = cli._Heartbeat(progress, interval_seconds=0.01)
    progress.bind_heartbeat(heartbeat)
    progress({"phase": "certification_start"})
    heartbeat.start()
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        if any(row.get("event") == "heartbeat" for row in rows):
            break
        time.sleep(0.005)
    else:
        pytest.fail("no durable heartbeat was written")
    heartbeat.stop()
    progress.close()

    failure_path = results / "failure-progress.jsonl"
    failing_progress = cli._ProgressJsonl(failure_path, cli._JsonConsole())
    failing_heartbeat = cli._Heartbeat(failing_progress, interval_seconds=0.01)
    failing_progress.bind_heartbeat(failing_heartbeat)
    failing_progress({"phase": "certification_start"})

    def fail_heartbeat(*, elapsed_seconds: float, cpu_seconds: float) -> None:
        del elapsed_seconds, cpu_seconds
        raise OSError("synthetic heartbeat sink failure")

    monkeypatch.setattr(failing_progress, "heartbeat", fail_heartbeat)
    failing_heartbeat.start()
    deadline = time.monotonic() + 1.0
    while not failing_heartbeat.failed and time.monotonic() < deadline:
        time.sleep(0.005)
    with pytest.raises(OSError, match="heartbeat"):
        failing_progress({"phase": "certification_ready_to_publish"})
    failing_heartbeat.stop(raise_on_error=False)
    failing_progress.close_quietly()


def test_progress_creation_fsyncs_directory_once_and_fails_closed(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    results = tmp_path / "candidate" / "results"
    results.mkdir(parents=True)
    calls: list[Path] = []

    def record_directory_fsync(path: Path) -> None:
        calls.append(path)

    monkeypatch.setattr(cli, "_fsync_directory", record_directory_fsync)
    progress = cli._ProgressJsonl(results / "progress.jsonl", cli._JsonConsole())
    progress({"phase": "certification_start"})
    progress({"phase": "source_fingerprint"})
    progress.close()
    assert calls == [results]

    def fail_directory_fsync(_path: Path) -> None:
        raise OSError("synthetic directory fsync failure")

    monkeypatch.setattr(cli, "_fsync_directory", fail_directory_fsync)
    failing_progress = cli._ProgressJsonl(
        results / "failure-progress.jsonl",
        cli._JsonConsole(),
    )
    with pytest.raises(OSError, match="directory fsync"):
        failing_progress({"phase": "certification_start"})
    failing_progress.close_quietly()


def test_heartbeat_preserves_target_and_enriches_stream_total_and_eta(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    clock = {"now": 0.0}
    monkeypatch.setattr(cli.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(cli, "_fsync_directory", lambda _path: None)
    results = tmp_path / "candidate" / "results"
    results.mkdir(parents=True)
    path = results / "progress.jsonl"
    progress = cli._ProgressJsonl(path, cli._JsonConsole())

    progress(
        {
            "phase": "stream",
            "rows_completed": 10,
            "stream": "inbox",
            "total_expected": 80,
        }
    )
    clock["now"] = 2.0
    progress(
        {
            "commits_completed": 50,
            "phase": "replay",
            "target_path": "scratch/replay.sqlite3",
            "target_store_bytes": 4096,
            "total_expected": 100,
        }
    )
    clock["now"] = 5.0
    progress(
        {
            "phase": "differential",
            "rows_completed": 20,
            "stream": "inbox",
        }
    )
    clock["now"] = 15.0
    progress.heartbeat(elapsed_seconds=15.0, cpu_seconds=3.0)
    progress.close()

    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    differential = rows[-2]
    heartbeat = rows[-1]
    assert differential["total_expected"] == 80
    assert heartbeat["phase"] == "differential"
    assert heartbeat["stream"] == "inbox"
    assert heartbeat["rows_completed"] == 20
    assert heartbeat["total_expected"] == 80
    assert heartbeat["target_path"] == "scratch/replay.sqlite3"
    assert heartbeat["target_store_bytes"] == 4096
    assert heartbeat["eta_seconds"] == 30.0
    assert heartbeat["elapsed_seconds"] == 15.0
    assert heartbeat["cpu_seconds"] == 3.0


def test_preserved_validation_fingerprint_resets_stale_differential_eta(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "_fsync_directory", lambda _path: None)
    results = tmp_path / "candidate" / "results"
    results.mkdir(parents=True)
    path = results / "progress.jsonl"
    progress = cli._ProgressJsonl(path, cli._JsonConsole())
    progress(
        {
            "phase": "differential",
            "rows_completed": 20,
            "stream": "inbox",
            "total_expected": 80,
            "eta_seconds": 30.0,
        }
    )
    progress(
        {
            "bytes_completed": 0,
            "phase": "preserved_target_validation",
            "validation_step": "fingerprint",
            "target_path": "scratch/preserved.sqlite3",
            "target_store_bytes": 4_096,
            "total_expected": 4_096,
        }
    )
    progress.heartbeat(elapsed_seconds=4.0, cpu_seconds=1.0)
    progress.close()

    heartbeat = json.loads(path.read_text(encoding="utf-8").splitlines()[-1])
    assert heartbeat["phase"] == "preserved_target_validation"
    assert heartbeat["validation_step"] == "fingerprint"
    assert heartbeat["bytes_completed"] == 0
    assert heartbeat["total_expected"] == 4_096
    assert "eta_seconds" not in heartbeat
    assert "stream" not in heartbeat
    assert "rows_completed" not in heartbeat


def test_final_preserved_validation_same_phase_resets_differential_eta(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "_fsync_directory", lambda _path: None)
    results = tmp_path / "candidate" / "results"
    results.mkdir(parents=True)
    path = results / "progress.jsonl"
    progress = cli._ProgressJsonl(path, cli._JsonConsole())
    progress(
        {
            "eta_seconds": 0.0,
            "phase": "final_preserved_target_validation",
            "rows_completed": 1_000,
            "stream": "inbox",
            "total_expected": 1_000,
            "validation_step": "differential",
        }
    )
    progress(
        {
            "bytes_completed": 0,
            "phase": "final_preserved_target_validation",
            "validation_step": "final_fingerprint",
            "target_path": "scratch/preserved.sqlite3",
            "target_store_bytes": 4_096,
            "total_expected": 4_096,
        }
    )
    progress.heartbeat(elapsed_seconds=8.0, cpu_seconds=2.0)
    progress.close()

    heartbeat = json.loads(path.read_text(encoding="utf-8").splitlines()[-1])
    assert heartbeat["phase"] == "final_preserved_target_validation"
    assert heartbeat["validation_step"] == "final_fingerprint"
    assert heartbeat["bytes_completed"] == 0
    assert heartbeat["total_expected"] == 4_096
    assert "eta_seconds" not in heartbeat
    assert "rows_completed" not in heartbeat
    assert "stream" not in heartbeat


def test_deadline_interrupts_main_thread_and_cli_reports_timeout(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fired = threading.Event()
    monkeypatch.setattr(cli._thread, "interrupt_main", fired.set)
    deadline = cli._SafetyDeadline(seconds=0.01)
    deadline.start()
    assert fired.wait(1.0)
    assert deadline.expired
    deadline.stop()
    assert cli._MAX_SAFETY_SECONDS == 7_200.0

    source = tmp_path / "source.sqlite3"
    source.write_bytes(b"offline")
    candidate = tmp_path / "candidate"
    progress = candidate / "results" / "progress.jsonl"

    class ExpiredDeadline:
        expired = True

        def start(self) -> None:
            return None

        def stop(self) -> None:
            return None

    def interrupted_certification(*args: object, **kwargs: object) -> object:
        del args, kwargs
        candidate.mkdir()
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "_SafetyDeadline", ExpiredDeadline)
    monkeypatch.setattr(cli, "certify_golden_v3", interrupted_certification)
    exit_code = cli.main(
        [
            str(source),
            str(candidate),
            "--run-id",
            "0" * 64,
            "--sentinel",
            str(tmp_path / "forbidden-original.sqlite3"),
            "--expected-size",
            str(source.stat().st_size),
            "--expected-sha256",
            hashlib.sha256(source.read_bytes()).hexdigest(),
            "--shard-rows",
            "1000",
            "--shard-bytes",
            "1000000",
            "--progress-jsonl",
            str(progress),
        ]
    )
    assert exit_code == 124
    assert candidate.is_dir()
    assert not (candidate / "COMPLETE").exists()
    payloads = [
        json.loads(line)
        for line in capsys.readouterr().err.splitlines()
        if line.strip()
    ]
    assert payloads[-1]["status"] == "TIMEOUT"


@pytest.mark.parametrize(
    ("error", "expected_status"),
    [
        (
            GoldenCertificationError("synthetic malformed exact replay evidence"),
            "GOLDEN_V3_CERTIFICATION_GENUINE_INTEGRITY_BLOCKED",
        ),
        (
            GoldenReplayDivergenceError("synthetic replay divergence"),
            "GOLDEN_V3_CERTIFICATION_REPLAY_DIVERGED",
        ),
    ],
)
def test_cli_reports_exact_failure_verdicts(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    error: GoldenCertificationError,
    expected_status: str,
) -> None:
    source = tmp_path / "source.sqlite3"
    source.write_bytes(b"offline")
    candidate = tmp_path / expected_status.lower()

    def failed_certification(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise error

    monkeypatch.setattr(cli, "certify_golden_v3", failed_certification)
    exit_code = cli.main(
        [
            str(source),
            str(candidate),
            "--run-id",
            "0" * 64,
            "--sentinel",
            str(tmp_path / "forbidden-original.sqlite3"),
            "--expected-size",
            str(source.stat().st_size),
            "--expected-sha256",
            hashlib.sha256(source.read_bytes()).hexdigest(),
            "--shard-rows",
            "1000",
            "--shard-bytes",
            "1000000",
            "--progress-jsonl",
            str(candidate / "results" / "progress.jsonl"),
        ]
    )
    assert exit_code == 2
    payloads = [
        json.loads(line)
        for line in capsys.readouterr().err.splitlines()
        if line.strip()
    ]
    assert payloads[-1]["status"] == expected_status


def test_failure_verdict_inspects_cause_and_context_chains() -> None:
    cause = GoldenCertificationError("cause wrapper")
    cause.__cause__ = GoldenReplayDivergenceError("nested replay cause")
    context = GoldenCertificationError("context wrapper")
    context.__context__ = GoldenReplayDivergenceError("nested replay context")

    assert cli._failure_verdict(cause) == "GOLDEN_V3_CERTIFICATION_REPLAY_DIVERGED"
    assert cli._failure_verdict(context) == "GOLDEN_V3_CERTIFICATION_REPLAY_DIVERGED"


def test_cli_preserves_lexical_paths_and_requires_progress_below_candidate(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.sqlite3"
    source.write_bytes(b"offline")
    candidate = tmp_path / "candidate"
    sentinel = tmp_path / "forbidden-original.sqlite3"
    namespace = Namespace(
        source=source,
        candidate_root=candidate,
        run_id="0" * 64,
        sentinel=sentinel,
        expected_size=source.stat().st_size,
        expected_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        shard_rows=1_000,
        shard_bytes=1_000_000,
        progress_jsonl=tmp_path / "outside.jsonl",
    )
    with pytest.raises(ValueError, match=r"progress.*candidate"):
        cli._validated_arguments(namespace)


@pytest.mark.parametrize(
    "gate",
    [
        "source_expected_identity",
        "source_immutability",
        "dual_extraction",
        "dual_extraction_bytes",
        "replay_differential",
    ],
)
def test_final_verifier_requires_every_exact_gate(gate: str) -> None:
    gates = {
        "source_expected_identity": "EXACT",
        "source_immutability": "EXACT",
        "dual_extraction": "EXACT",
        "dual_extraction_bytes": "EXACT",
        "replay_differential": "EXACT",
    }
    gates[gate] = "SKIPPED"
    with pytest.raises(GoldenCertificationError, match="exactness gates"):
        certification._require_exact_certification_gates(gates)


def test_final_verifier_accepts_every_exact_gate_and_continues() -> None:
    gates = {
        "source_expected_identity": "EXACT",
        "source_immutability": "EXACT",
        "dual_extraction": "EXACT",
        "dual_extraction_bytes": "EXACT",
        "replay_differential": "EXACT",
    }

    certification._require_exact_certification_gates(gates)

    assert set(gates) == {
        "source_expected_identity",
        "source_immutability",
        "dual_extraction",
        "dual_extraction_bytes",
        "replay_differential",
    }
