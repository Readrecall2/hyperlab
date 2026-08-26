from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.certify_storage_v4_phase1c_successor as cli

PHASE1C_TARGET_NOT_MET_VERDICT = cli.PHASE1C_TARGET_NOT_MET_VERDICT
SUCCESSOR_REATTESTED_STATUS = cli.SUCCESSOR_REATTESTED_STATUS


def _events(output: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in output.splitlines() if line]


def test_cli_and_core_load_without_storage_package_or_ingestion_imports() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    program = textwrap.dedent(
        """
        import importlib.abc
        import sys

        class RejectStoragePackage(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.startswith("hyperlab.paper.storage_v4"):
                    raise AssertionError(f"forbidden package import: {fullname}")
                return None

        sys.meta_path.insert(0, RejectStoragePackage())
        import scripts.certify_storage_v4_phase1c_successor as cli

        forbidden = (
            "capacity_runner",
            "ingestion",
            "phase1c_workers",
            "phase1c_workloads",
            "phase1c_pipeline",
            "golden_runner",
            "writer",
        )
        loaded = sorted(
            name for name in sys.modules
            if any(token in name for token in forbidden)
        )
        if loaded:
            raise AssertionError(f"forbidden modules loaded: {loaded}")
        if cli._SUCCESSOR_MODULE.__package__ not in {"", None}:
            raise AssertionError("successor core is not standalone")
        print("STANDALONE_SUCCESSOR_OK")
        """
    )
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    completed = subprocess.run(
        [sys.executable, "-c", program],
        cwd=repository_root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "STANDALONE_SUCCESSOR_OK"


def test_cli_pins_exact_candidate05_and_acquired_baseline_contract() -> None:
    assert Path(
        r"C:\Dev\hyperlab-offline-validation\e45f5569"
        r"\storage-v4-phase-1c\native-capacity-05"
    ) == cli.CANDIDATE05_MISSION_ROOT
    assert cli.RUN06_CANDIDATE_ROOT.name == "native-capacity-06"
    assert cli.CANDIDATE05_BOUNDARY_COUNTS == (100_000, 500_000, 1_000_000)
    assert cli.CANDIDATE05_TERMINAL_CERTIFICATE_SHA256 == (
        "f2a472511062a96e4b99fbdd911d1e4cf301ffc57ee0d005ff79f3292b5deba2"
    )
    assert cli.CANDIDATE05_TERMINAL_MANIFEST_SHA256 == (
        "e3659295ac8dba4de20006ef28261cb426724c57b0caa14f7745595fb07902ba"
    )
    assert cli.CANDIDATE05_TERMINAL_TREE_SHA256 == (
        "91bf5ddb17932402729e92874b00fc8b3e84276814659c7cf6e38998c36a0d2c"
    )
    assert cli.CANDIDATE05_PRODUCER_CODE_IDENTITY == (
        "eb648dca8d36fe1deb81b6c517611ad50228b20e6a06f8cf9bb3489faf26b3f1"
    )
    assert cli.ACQUIRED_VERIFIER_BASELINE_IDENTITY == (
        "fa0e55fb4a42488eaa52a69355909c578f45994c64e2849df3e859a0089c5936"
    )
    assert cli.BASELINE_BYTE_WITNESS_SHA256 == (
        "32c30490eb3a9934165a67fd76b0127fb698316710fdadd67b08be081335c740"
    )
    assert cli.BASELINE_BYTE_WITNESS_SIZE_BYTES == 32_222
    assert cli.PRODUCER_DEPENDENCY_CLOSURE_SHA256 == (
        "e8bc7f8f4e3fce05bbb5681b95963414cea6d26a0de813d3f22a39a30a0c9bb7"
    )
    assert cli.PRODUCER_DEPENDENCY_CLOSURE_FILE_COUNT == 104
    expectations = cli._expectations()
    assert expectations.workload_profile == "GOLDEN_SHAPED"
    assert expectations.workload_seed == 20_260_825
    assert expectations.generator_version == "storage-v4-synthetic-capacity-v3"


def test_cli_reattest_emits_compact_receipt_terminal_without_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_root = (tmp_path / "successor-output").resolve()
    receipt_path = output_root / "successor-receipt.json"
    sentinel_config = object()
    monkeypatch.setattr(cli, "_build_config", lambda _repo, _output: sentinel_config)
    calls: list[object] = []

    def _reattest(config: object, *, progress: object) -> SimpleNamespace:
        calls.append((config, progress))
        assert callable(progress)
        progress(
            {
                "bytes_hashed": 123,
                "candidate_root": "candidate-05",
                "files_completed": 2,
                "files_total": 5,
                "phase": "phase1c_candidate_tree_hash",
                "status": "RUNNING",
            }
        )
        return SimpleNamespace(
            path=receipt_path,
            sha256="a" * 64,
        )

    monkeypatch.setattr(cli, "reattest_phase1c_successor", _reattest)

    assert (
        cli.main(
            [
                "reattest",
                "--output-root",
                str(output_root),
                "--repository-root",
                str(cli.REPOSITORY_ROOT),
            ]
        )
        == 0
    )

    assert len(calls) == 1
    assert calls[0][0] is sentinel_config
    events = _events(capsys.readouterr().out)
    assert [event["status"] for event in events] == [
        "RUNNING",
        "RUNNING",
        "COMPLETE",
    ]
    assert events[1]["bytes_hashed"] == 123
    assert events[1]["files_completed"] == 2
    assert events[1]["files_total"] == 5
    assert events[-1]["successor_status"] == SUCCESSOR_REATTESTED_STATUS
    assert events[-1]["complete_published"] is False
    assert events[-1]["terminal_signal"] == (
        "SUCCESSOR_RECEIPT_PUBLISHED_NO_COMPLETE"
    )


def test_cli_finalize_never_builds_or_opens_candidate_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_root = (tmp_path / "successor-output").resolve()
    targeted_path = (tmp_path / "targeted.json").resolve()
    closure_path = (tmp_path / "closure.json").resolve()
    targeted_path.write_bytes(b"{}")
    closure_path.write_bytes(b"{}")
    targeted = object()
    closure = object()
    monkeypatch.setattr(
        cli,
        "_build_config",
        lambda *_args: pytest.fail("finalize must not build candidate config"),
    )
    monkeypatch.setattr(
        cli, "load_phase1c_successor_test_witness", lambda path: targeted
    )
    monkeypatch.setattr(
        cli, "load_phase1c_successor_closure_witness", lambda path: closure
    )
    calls: list[dict[str, object]] = []

    def _finalize(**kwargs: object) -> SimpleNamespace:
        calls.append(kwargs)
        return SimpleNamespace(root=output_root, complete_sha256="b" * 64)

    monkeypatch.setattr(cli, "finalize_phase1c_successor_closure", _finalize)

    assert (
        cli.main(
            [
                "finalize",
                "--output-root",
                str(output_root),
                "--repository-root",
                str(cli.REPOSITORY_ROOT),
                "--receipt-sha256",
                "c" * 64,
                "--targeted-tests-witness",
                str(targeted_path),
                "--closure-witness",
                str(closure_path),
            ]
        )
        == 0
    )

    assert len(calls) == 1
    assert calls[0]["targeted_tests"] is targeted
    assert calls[0]["closure"] is closure
    events = _events(capsys.readouterr().out)
    assert events[0]["candidate_reopened"] is False
    assert events[-1]["terminal_signal"] == "COMPLETE"
    assert events[-1]["verdict"] == PHASE1C_TARGET_NOT_MET_VERDICT


def test_cli_gates_runs_only_canonical_bounded_commands_and_publishes_witnesses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    receipt_root = (tmp_path / "receipt").resolve()
    receipt_root.mkdir()
    gate_root = (tmp_path / "gate-root").resolve()
    calls: list[tuple[str, tuple[str, ...], int]] = []

    def _create_gate_root(
        root: Path, *, repository_root: Path, receipt_root: Path
    ) -> Path:
        del repository_root, receipt_root
        (root / "logs").mkdir(parents=True)
        (root / "pytest" / "targeted").mkdir(parents=True)
        (root / "pytest" / "global").mkdir()
        return root

    def _run_gate_command(
        command: tuple[str, ...],
        *,
        cwd: Path,
        purpose: str,
        timeout_seconds: int,
        output_log_path: Path,
    ) -> object:
        assert cwd == cli.REPOSITORY_ROOT
        assert timeout_seconds > 0
        calls.append((purpose, command, timeout_seconds))
        payload = f"{purpose} passed\n".encode()
        cli.durable_publish_immutable(output_log_path, payload)
        return cli._GateExecution(
            purpose=purpose,
            command=command,
            exit_code=0,
            output_sha256=hashlib.sha256(payload).hexdigest(),
            output_log_path=output_log_path,
            output_log_size_bytes=len(payload),
            stdout_bytes=len(payload),
            stderr_bytes=0,
            timeout_seconds=timeout_seconds,
        )

    monkeypatch.setattr(cli, "_create_gate_root", _create_gate_root)
    monkeypatch.setattr(cli, "_run_gate_command", _run_gate_command)
    monkeypatch.setattr(
        cli,
        "_build_config",
        lambda *_args: pytest.fail("gates must not build or open candidate config"),
    )

    assert (
        cli.main(
            [
                "gates",
                "--gate-root",
                str(gate_root),
                "--receipt-root",
                str(receipt_root),
                "--repository-root",
                str(cli.REPOSITORY_ROOT),
            ]
        )
        == 0
    )

    assert [purpose for purpose, _command, _timeout in calls] == [
        "SUCCESSOR_TARGETED_TESTS",
        "V10_CHECK",
        "PHASE05_CHECK",
        "RUFF_GLOBAL_FINAL",
        "MYPY_HYPERLAB_FINAL",
        "PYTEST_GLOBAL_FINAL_SINGLE_RUN",
        "GIT_DIFF_CHECK_FINAL",
    ]
    global_pytest = [
        command
        for purpose, command, _timeout in calls
        if purpose == "PYTEST_GLOBAL_FINAL_SINGLE_RUN"
    ]
    assert len(global_pytest) == 1
    assert Path(global_pytest[0][-1]).is_absolute()
    assert global_pytest[0][-1] == str(gate_root / "pytest" / "global")
    assert calls[-1][1] == (
        "git",
        "-c",
        "core.whitespace=cr-at-eol",
        "diff",
        "--check",
    )
    targeted = json.loads(
        (gate_root / cli.SUCCESSOR_TARGETED_WITNESS_NAME).read_text("utf-8")
    )
    closure = json.loads(
        (gate_root / cli.SUCCESSOR_CLOSURE_WITNESS_NAME).read_text("utf-8")
    )
    assert tuple(targeted["source_files"]) == cli.SUCCESSOR_TARGETED_TEST_PATHS
    assert closure["global_pytest_runs"] == 1
    events = _events(capsys.readouterr().out)
    assert events[-1]["terminal_signal"] == "SUCCESSOR_GATE_WITNESSES_PUBLISHED"
    assert events[-1]["candidate_opened"] is False


def test_git_diff_gate_accepts_crlf_and_rejects_true_trailing_space(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    environment = cli._gate_environment()
    subprocess.run(
        ("git", "init", "--quiet"),
        cwd=repository,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    tracked = repository / "tracked.txt"
    tracked.write_bytes(b"baseline\n")
    subprocess.run(
        ("git", "add", "--", tracked.name),
        cwd=repository,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    command = cli._closure_commands(tmp_path / "gate")[-1][1]

    tracked.write_bytes(b"baseline\r\nclean\r\n")
    clean = subprocess.run(
        command,
        cwd=repository,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert clean.returncode == 0, clean.stdout + clean.stderr

    tracked.write_bytes(b"baseline\r\ntrailing \r\n")
    trailing = subprocess.run(
        command,
        cwd=repository,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert trailing.returncode != 0
    assert "trailing whitespace" in trailing.stdout + trailing.stderr


def test_cli_exposes_no_candidate_or_workload_override() -> None:
    with pytest.raises(SystemExit):
        cli.main(
            [
                "reattest",
                "--output-root",
                str((Path.cwd() / "fresh").resolve()),
                "--candidate-name",
                "native-capacity-06",
            ]
        )


def test_cli_failure_is_terminal_and_does_not_claim_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "_build_config",
        lambda *_args: (_ for _ in ()).throw(ValueError("synthetic refusal")),
    )

    assert (
        cli.main(
            [
                "reattest",
                "--output-root",
                str((tmp_path / "output").resolve()),
                "--repository-root",
                str(cli.REPOSITORY_ROOT),
            ]
        )
        == 1
    )

    captured = capsys.readouterr()
    assert "COMPLETE" not in captured.out
    failure = _events(captured.err)
    assert failure == [
        {
            "error": "synthetic refusal",
            "error_type": "ValueError",
            "phase": "phase1c_successor_reattest",
            "status": "FAILED",
        }
    ]
