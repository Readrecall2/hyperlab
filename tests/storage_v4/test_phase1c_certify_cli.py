from __future__ import annotations

import ast
import hashlib
import io
import json
import os
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.certify_storage_v4_phase1c as cli
from hyperlab.paper.storage_v4.phase1c_certification import (
    Phase1CCertificationError,
)


def _execution(
    command: tuple[str, ...],
    *,
    exit_code: int = 0,
    output_log_path: Path | None = None,
) -> cli._CommandExecution:
    payload = b"synthetic output\n"
    environment = cli._subprocess_environment()
    if output_log_path is not None:
        output_log_path.write_bytes(payload)
    return cli._CommandExecution(
        command=command,
        exit_code=exit_code,
        output_sha256=hashlib.sha256(payload).hexdigest(),
        output_bytes=len(payload),
        summary=(
            "synthetic command summary; "
            f"environment_projection_sha256={environment.sha256}"
        ),
        environment_projection_sha256=environment.sha256,
        output_log_path=output_log_path,
        output_log_size_bytes=(None if output_log_path is None else len(payload)),
    )


def _materialize_targeted_sources(root: Path) -> None:
    for relative_path in cli.TARGETED_TEST_PATHS:
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"# {relative_path}\n".encode())


def test_command_execution_requires_bound_environment_projection() -> None:
    with pytest.raises(ValueError, match="does not bind"):
        cli._CommandExecution(
            command=(sys.executable, "-c", "pass"),
            exit_code=0,
            output_sha256=hashlib.sha256(b"").hexdigest(),
            output_bytes=0,
            summary="missing projection binding",
            environment_projection_sha256="0" * 64,
        )


def test_pinned_preflight_config_has_exact_authorities_and_target(tmp_path: Path) -> None:
    minimum_free_bytes = cli.DEFAULT_MINIMUM_FREE_BYTES + 123
    config = cli._build_preflight_config(
        tmp_path,
        candidate_name="native-golden-02",
        minimum_free_bytes=minimum_free_bytes,
    )

    assert config.mission_root == Path(
        r"C:\Dev\hyperlab-offline-validation\e45f5569"
        r"\storage-v4-phase-1c\native-golden-02"
    )
    assert config.allowed_parent == Path(
        r"C:\Dev\hyperlab-offline-validation\e45f5569\storage-v4-phase-1c"
    )
    assert config.golden_certification_root == Path(
        r"C:\Dev\hyperlab-offline-validation\e45f5569\golden-v3"
        r"\candidate-e45f5569-20260824-01"
    )
    assert config.golden_export_root == (
        config.golden_certification_root / "corpus" / "extract-a"
    )
    assert config.golden_pin_path == (
        config.golden_certification_root / "pin" / "extract-a.pin.json"
    )
    assert config.phase1b_root == Path(
        r"C:\Dev\hyperlab-offline-validation\e45f5569"
        r"\storage-v4-phase-1b\retry-02"
    )
    assert config.roadmap_path == (
        tmp_path / "HyperLab_Master_Roadmap_V4_2026-08-22.html"
    )
    assert config.expected_roadmap_sha256 == (
        "341885995fd8cf38c6c007770817d30cb67c5e30650b8f6a4a9fa6140f3abb72"
    )
    assert config.expected_target_line_number == 339
    assert config.expected_canonical_target_occurrences == 3
    assert config.minimum_free_bytes == minimum_free_bytes
    assert config.golden.certification_root_hash == (
        "4797d81cc089e8f57a2a8c7cf0762c4463a7e85a56d8426210b160a4338d6ad0"
    )
    assert config.golden.golden_root_hash == (
        "4ebc64a7974be92da8d5dde926ddb16f58d9e8c14287bd8da5f7c92156912376"
    )
    assert config.golden.source_sha256 == (
        "dbe96c1ef65aee4a591406b086912339317926b173dbb1c957e099fcd12e39a2"
    )
    assert config.golden.run_id == (
        "7e6c1c014ef851fabce026e963cbdfb44a17725e1913cf095cc8ae4c3d419e8d"
    )
    assert config.golden.pin_sha256 == (
        "bb445a75e683e83adb1928c515502ecd92f42f9de1219339305f906bbd4bbb5c"
    )
    assert (
        config.golden.commit_count,
        config.golden.row_count,
        config.golden.stream_count,
        config.golden.market_gap_count,
    ) == (252_262, 1_011_362, 13, 1)
    assert config.golden.source_size_bytes == 2_014_072_832
    assert config.golden.export_physical_bytes == 2_456_283_751
    assert config.phase1b.report_sha256 == (
        "165eac6bd45ae6093a96dbd35b88c3a6301858adca0e7aa6396a665f82c400ca"
    )
    assert config.phase1b.manifest_root == (
        "a85846c7899ddf8693e4882716e80274fec18663c66958445c788822bbb41398"
    )
    assert config.phase1b.final_prefix_root == (
        "f32965fa0b24cc189e271d682136680c2867c76074724e552a43e248897665ba"
    )
    assert config.phase1b.storage_v4_store_bytes == 528_250_030
    assert config.phase1b.anchor_bytes == 12_288
    assert config.phase1b.compatibility_segment_bytes == 317_492_777
    assert cli.PINNED_V9_SIZE_BYTES == 2_833
    assert cli.PINNED_V9_SHA256 == (
        "7f3216b97ffeb60d18c05572e5642f08dbb589caebcbc746fa5829b6fa565d33"
    )
    assert cli.DEFAULT_CANDIDATE_NAME == "native-capacity-05"
    assert Path(
        r"C:\Dev\hyperlab-offline-validation\e45f5569"
        r"\storage-v4-phase-1c\native-golden-04\golden-native"
    ) == cli.GOLDEN_IMPORTED_CANDIDATE_ROOT
    assert Path(
        r"C:\Dev\hyperlab-offline-validation\e45f5569"
        r"\storage-v4-phase-1c\native-golden-04-driver-logs\stdout.jsonl"
    ) == cli.GOLDEN_PRODUCER_STDOUT_LOG
    assert Path(
        r"C:\Dev\hyperlab-offline-validation\e45f5569"
        r"\storage-v4-phase-1c\native-golden-04-driver-logs\stderr.log"
    ) == cli.GOLDEN_PRODUCER_STDERR_LOG
    assert cli.GOLDEN_PRODUCER_LOG_ROOT.parent == cli.PHASE1C_ALLOWED_PARENT
    assert cli.GOLDEN_PRODUCER_LOG_ROOT.parent != cli.GOLDEN_PRODUCER_MISSION_ROOT
    assert cli.PINNED_GOLDEN_PRODUCER_STDOUT_SHA256 == (
        "1d607865260ebdaa962bbdd3a26dcf593133670a831e68698429a322dc3c015c"
    )
    assert cli.PINNED_GOLDEN_PRODUCER_STDERR_SHA256 == (
        "c845708e70c72a7d9aab0dfa8f27cb84f7e5d55cdd20cf218203fe52b6b6f970"
    )
    assert cli.HISTORICAL_ATTEMPT_INGESTION == (
        ("native-golden-03", 204_000, 216_000),
        ("native-golden-04", 352_267, 352_267),
        ("native-capacity-05", 1_120_005, 1_120_005),
    )
    assert "tests/storage_v4/test_golden_reattestation.py" in (
        cli.TARGETED_TEST_PATHS
    )


def test_preflight_refuses_to_lower_canonical_twenty_gib_floor(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least the canonical 20 GiB floor"):
        cli._build_preflight_config(
            tmp_path,
            minimum_free_bytes=cli.DEFAULT_MINIMUM_FREE_BYTES - 1,
        )


@pytest.mark.parametrize(
    "candidate_name",
    (
        "",
        ".",
        "..",
        "native/golden-02",
        r"native\golden-02",
        "C:native-golden-02",
        "native-golden-02.",
        "native golden 02",
        "CON",
        "com1",
        "x" * 64,
    ),
)
def test_candidate_name_must_be_a_safe_direct_windows_leaf(
    candidate_name: str,
) -> None:
    with pytest.raises(ValueError, match="candidate name"):
        cli._mission_root_for_candidate(candidate_name)


def test_candidate_name_allows_fresh_append_only_successor() -> None:
    assert cli._mission_root_for_candidate("native-golden-02") == (
        cli.PHASE1C_ALLOWED_PARENT / "native-golden-02"
    )


def test_cumulative_resume_root_is_existing_canonical_phase1c_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed_parent = tmp_path / "storage-v4-phase-1c"
    resume_root = allowed_parent / "native-capacity-05" / "capacity-cumulative"
    resume_root.mkdir(parents=True)
    monkeypatch.setattr(cli, "PHASE1C_ALLOWED_PARENT", allowed_parent)

    assert cli._validate_cumulative_resume_candidate_root(resume_root) == (
        resume_root.resolve(strict=True)
    )

    wrong_leaf = resume_root.parent / "golden-native"
    wrong_leaf.mkdir()
    with pytest.raises(ValueError, match="must name capacity-cumulative"):
        cli._validate_cumulative_resume_candidate_root(wrong_leaf)

    outside = tmp_path / "outside" / "capacity-cumulative"
    outside.mkdir(parents=True)
    with pytest.raises(ValueError, match="under the Phase 1C parent"):
        cli._validate_cumulative_resume_candidate_root(outside)


def test_closure_only_request_requires_external_pin_and_excludes_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed_parent = tmp_path / "storage-v4-phase1c"
    candidate_root = allowed_parent / "native-capacity-test" / "capacity-cumulative"
    candidate_root.mkdir(parents=True)
    monkeypatch.setattr(cli, "PHASE1C_ALLOWED_PARENT", allowed_parent)

    assert cli._validate_closure_only_request(
        candidate_root=candidate_root,
        receipt_sha256="a" * 64,
        resume_candidate_root=None,
    ) == (candidate_root.resolve(strict=True), "a" * 64)
    with pytest.raises(ValueError, match="required together"):
        cli._validate_closure_only_request(
            candidate_root=candidate_root,
            receipt_sha256=None,
            resume_candidate_root=None,
        )
    with pytest.raises(ValueError, match="lowercase hexadecimal"):
        cli._validate_closure_only_request(
            candidate_root=candidate_root,
            receipt_sha256="A" * 64,
            resume_candidate_root=None,
        )
    with pytest.raises(ValueError, match="mutually exclusive"):
        cli._validate_closure_only_request(
            candidate_root=candidate_root,
            receipt_sha256="a" * 64,
            resume_candidate_root=candidate_root,
        )


def test_streamed_command_hashes_combined_output_without_shell_or_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = io.BytesIO()
    output_log = tmp_path / "combined.log"
    progress: list[dict[str, object]] = []
    payload = b"stdout-first\nstderr-second\n"
    command = (
        sys.executable,
        "-c",
        (
            "import sys;"
            "sys.stdout.buffer.write(b'stdout-first\\n');"
            "sys.stdout.buffer.flush();"
            "sys.stderr.buffer.write(b'stderr-second\\n');"
            "sys.stderr.buffer.flush()"
        ),
    )
    metrics = cli._DirectSubprocessMetrics(
        cpu_ns=123,
        peak_rss_bytes=456,
        cumulative_write_bytes=789,
        scope=cli._DIRECT_SUBPROCESS_METRICS_SCOPE,
        limitation=cli._PROCESS_TREE_METRICS_LIMITATION,
    )
    monkeypatch.setattr(cli, "_direct_subprocess_metrics", lambda _pid: metrics)
    environment = cli._subprocess_environment()

    result = cli._run_streamed_command(
        command,
        cwd=tmp_path,
        purpose="SYNTHETIC",
        heartbeat_seconds=30.0,
        progress=lambda value: progress.append(dict(value)),
        output_stream=output,
        output_log_path=output_log,
    )

    assert result.command == command
    assert result.exit_code == 0
    assert result.output_bytes == len(payload)
    assert result.output_sha256 == hashlib.sha256(payload).hexdigest()
    assert output.getvalue() == payload
    assert output_log.read_bytes() == payload
    assert result.output_log_path == output_log
    assert result.output_log_size_bytes == len(payload)
    assert result.environment_projection_sha256 == environment.sha256
    assert f"environment_projection_sha256={environment.sha256}" in result.summary
    assert progress[0]["status"] == "RUNNING"
    assert progress[0]["environment_projection"] == environment.projection
    assert progress[-1]["status"] == "COMPLETE"
    assert progress[-1]["subprocess_cpu_ns"] == 123
    assert progress[-1]["subprocess_peak_rss_bytes"] == 456
    assert progress[-1]["subprocess_cumulative_write_bytes"] == 789
    assert progress[-1]["subprocess_metrics_scope"] == (
        "DIRECT_SUBPROCESS_ONLY_EXCLUDES_DESCENDANTS_AND_PROCESS_TREE"
    )
    assert progress[-1]["subprocess_process_tree_metrics_limitation"] == (
        cli._PROCESS_TREE_METRICS_LIMITATION
    )


def test_streamed_command_sanitizes_and_binds_control_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    removed = {
        "COVERAGE_PROCESS_START": "poison-coverage",
        "COV_CORE_SOURCE": "poison-cov-core",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "core.pager",
        "GIT_CONFIG_VALUE_0": "poison-pager",
        "GIT_DIR": "poison-git-dir",
        "GIT_INDEX_FILE": "poison-git-index",
        "GIT_WORK_TREE": "poison-git-work-tree",
        "MYPYPATH": "poison-mypy",
        "PYTEST_ADDOPTS": "--collect-only -k poison",
        "PYTEST_PLUGINS": "poison_plugin",
        "PYTHONHOME": "poison-python-home",
        "PYTHONPATH": "poison-python-path",
    }
    for name, value in removed.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "poison-global-config")
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "0")
    monkeypatch.setenv("PYTHONHASHSEED", "999")
    selected = (
        *removed,
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_NOSYSTEM",
        "PYTHONHASHSEED",
    )
    command = (
        sys.executable,
        "-c",
        (
            "import json,os,time;"
            f"keys={selected!r};"
            "print(json.dumps({key:os.environ.get(key) for key in keys},sort_keys=True));"
            "time.sleep(0.2)"
        ),
    )
    output = io.BytesIO()
    progress: list[dict[str, object]] = []
    environment = cli._subprocess_environment()

    result = cli._run_streamed_command(
        command,
        cwd=tmp_path,
        purpose="SANITIZED_ENVIRONMENT",
        heartbeat_seconds=30.0,
        progress=lambda value: progress.append(dict(value)),
        output_stream=output,
    )

    observed = json.loads(output.getvalue().decode("utf-8"))
    assert all(observed[name] is None for name in removed)
    assert observed["GIT_CONFIG_GLOBAL"] == cli._SUBPROCESS_ENVIRONMENT_FIXED[
        "GIT_CONFIG_GLOBAL"
    ]
    assert observed["GIT_CONFIG_NOSYSTEM"] == "1"
    assert observed["PYTHONHASHSEED"] == "0"
    assert result.exit_code == 0
    assert result.environment_projection_sha256 == environment.sha256
    assert progress[0]["environment_projection"] == environment.projection
    assert progress[0]["environment_projection_sha256"] == environment.sha256
    assert f"environment_projection_sha256={environment.sha256}" in result.summary
    if os.name == "nt":
        assert isinstance(progress[-1]["subprocess_cpu_ns"], int)
        assert progress[-1]["subprocess_cpu_ns"] >= 0
        assert isinstance(progress[-1]["subprocess_peak_rss_bytes"], int)
        assert progress[-1]["subprocess_peak_rss_bytes"] > 0
        assert isinstance(progress[-1]["subprocess_cumulative_write_bytes"], int)
        assert progress[-1]["subprocess_cumulative_write_bytes"] >= 0


def test_missing_process_tree_counters_never_create_a_false_stagnation_verdict() -> None:
    metrics = cli._DirectSubprocessMetrics(
        cpu_ns=100,
        peak_rss_bytes=200,
        cumulative_write_bytes=300,
        scope=cli._DIRECT_SUBPROCESS_METRICS_SCOPE,
        limitation=cli._PROCESS_TREE_METRICS_LIMITATION,
    )

    assessment = cli._subprocess_progress_assessment(
        previous_metrics=metrics,
        current_metrics=metrics,
        previous_output_bytes=400,
        current_output_bytes=400,
    )

    assert assessment == (
        "INDETERMINATE_NO_DIRECT_SUBPROCESS_PROGRESS_OBSERVED; "
        "PROCESS_TREE_NOT_OBSERVED; NOT_DECLARED_STAGNANT"
    )


def test_targeted_tests_hash_sources_before_and_after_and_clean_basetemp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _materialize_targeted_sources(tmp_path)
    observed_basetemps: list[Path] = []

    def run(
        command: tuple[str, ...],
        **_kwargs: object,
    ) -> cli._CommandExecution:
        basetemp = Path(command[command.index("--basetemp") + 1])
        assert basetemp.is_dir()
        assert basetemp.parent == tmp_path
        observed_basetemps.append(basetemp)
        output_log_path = _kwargs.get("output_log_path")
        assert isinstance(output_log_path, Path)
        return _execution(command, output_log_path=output_log_path)

    monkeypatch.setattr(cli, "_run_streamed_command", run)

    output_log_path = tmp_path / "targeted.log"
    witness = cli._run_targeted_tests(
        tmp_path,
        heartbeat_seconds=30.0,
        output_log_path=output_log_path,
    )

    assert witness.exit_code == 0
    assert witness.output_log_path == str(output_log_path)
    assert witness.output_log_size_bytes == len(b"synthetic output\n")
    assert output_log_path.read_bytes() == b"synthetic output\n"
    assert tuple(path for path, _digest in witness.source_files) == cli.TARGETED_TEST_PATHS
    assert "tests/storage_v4/test_phase1c_progress.py" in cli.TARGETED_TEST_PATHS
    assert (
        "tests/storage_v4/test_phase1c_worker_result_resume.py"
        in cli.TARGETED_TEST_PATHS
    )
    required_global_paths = (
        "tests/test_paper_operator_cli_phase12_live.py",
        "tests/test_paper_runtime_candidate_identity.py",
        "tests/test_paper_runtime_phase12.py",
        "tests/test_readonly_boundary.py",
    )
    assert cli.TARGETED_TEST_PATHS[-4:] == required_global_paths
    assert all(path in witness.command for path in required_global_paths)
    witness_hashes = dict(witness.source_files)
    assert all(path in witness_hashes for path in required_global_paths)
    assert len(observed_basetemps) == 1
    assert not observed_basetemps[0].exists()


def test_targeted_tests_fail_if_a_bound_source_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _materialize_targeted_sources(tmp_path)

    def run(
        command: tuple[str, ...],
        **_kwargs: object,
    ) -> cli._CommandExecution:
        (tmp_path / cli.TARGETED_TEST_PATHS[0]).write_bytes(b"# changed\n")
        output_log_path = _kwargs.get("output_log_path")
        assert isinstance(output_log_path, Path)
        return _execution(command, output_log_path=output_log_path)

    monkeypatch.setattr(cli, "_run_streamed_command", run)

    with pytest.raises(Phase1CCertificationError, match="sources changed"):
        cli._run_targeted_tests(
            tmp_path,
            heartbeat_seconds=30.0,
            output_log_path=tmp_path / "targeted.log",
        )
    assert not tuple(tmp_path.glob(f"{cli._TEST_BASETEMP_PREFIX}*"))


def test_closure_runs_exact_order_once_and_proves_pinned_v9(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    v9 = tmp_path / "config" / "paper" / "phase08-v9-historical-attestation.json"
    v9.parent.mkdir(parents=True)
    v9.write_bytes(b"pinned synthetic v9\n")
    monkeypatch.setattr(cli, "PINNED_V9_SIZE_BYTES", v9.stat().st_size)
    monkeypatch.setattr(cli, "PINNED_V9_SHA256", hashlib.sha256(v9.read_bytes()).hexdigest())
    purposes: list[str] = []
    commands: list[tuple[str, ...]] = []
    basetemps: list[Path] = []
    events: list[str] = []
    real_require_pinned_v9 = cli._require_pinned_v9

    def require_pinned_v9(path: Path) -> tuple[int, str]:
        events.append("V9_PINNED")
        return real_require_pinned_v9(path)

    def run(
        command: tuple[str, ...],
        *,
        purpose: str,
        **_kwargs: object,
    ) -> cli._CommandExecution:
        output_log_path = _kwargs.get("output_log_path")
        assert isinstance(output_log_path, Path)
        purposes.append(purpose)
        events.append(purpose)
        commands.append(command)
        if purpose == "PYTEST_GLOBAL_FINAL_SINGLE_RUN":
            basetemp = Path(command[command.index("--basetemp") + 1])
            assert basetemp.is_dir()
            basetemps.append(basetemp)
        return _execution(command, output_log_path=output_log_path)

    monkeypatch.setattr(cli, "_run_streamed_command", run)
    monkeypatch.setattr(cli, "_require_pinned_v9", require_pinned_v9)
    mission_root = tmp_path / "mission"
    mission_root.mkdir()
    closure = cli._make_closure_runner(
        tmp_path,
        heartbeat_seconds=30.0,
    )(mission_root)

    assert purposes == [
        "V10_GENERATE_FIRST",
        "V10_CHECK_FIRST",
        "V10_GENERATE_SECOND",
        "V10_CHECK_SECOND",
        "PHASE05_GENERATE",
        "PHASE05_CHECK",
        "PYTEST_GLOBAL_FINAL_SINGLE_RUN",
        "RUFF_GLOBAL_FINAL",
        "MYPY_HYPERLAB_FINAL",
        "GIT_DIFF_CHECK_FINAL",
    ]
    assert events == [
        "V9_PINNED",
        "V10_GENERATE_FIRST",
        "V10_CHECK_FIRST",
        "V10_GENERATE_SECOND",
        "V10_CHECK_SECOND",
        "PHASE05_GENERATE",
        "PHASE05_CHECK",
        "V9_PINNED",
        "PYTEST_GLOBAL_FINAL_SINGLE_RUN",
        "RUFF_GLOBAL_FINAL",
        "MYPY_HYPERLAB_FINAL",
        "GIT_DIFF_CHECK_FINAL",
        "V9_PINNED",
    ]
    assert sum("pytest" in command for command in commands) == 1
    assert commands[0][1:] == ("scripts/generate_phase12_live_paper_artifacts.py",)
    assert commands[1][1:] == (
        "scripts/generate_phase12_live_paper_artifacts.py",
        "--check",
    )
    assert commands[4][1:] == ("scripts/generate_phase05_paper_evidence.py",)
    assert commands[5][1:] == ("scripts/generate_phase05_paper_evidence.py", "--check")
    assert commands[-1] == (
        "git",
        "-c",
        "core.whitespace=cr-at-eol",
        "diff",
        "--check",
    )
    assert closure.payload()["global_pytest_runs"] == 1
    assert closure.v9.size_bytes == v9.stat().st_size
    assert closure.v9.before_sha256 == closure.v9.after_sha256
    assert "v9_pre_global_size_bytes=" in closure.commands[5].summary
    assert "v9_pre_global_sha256=" in closure.commands[5].summary
    assert len(tuple((mission_root / "closure-logs").glob("*.log"))) == 10
    assert all(command.output_log_path is not None for command in closure.commands)
    assert all(command.output_log_size_bytes == 17 for command in closure.commands)
    assert len(basetemps) == 1
    assert not basetemps[0].exists()


def test_failed_closure_command_returns_no_witness_and_cleans_basetemp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    v9 = tmp_path / "config" / "paper" / "phase08-v9-historical-attestation.json"
    v9.parent.mkdir(parents=True)
    v9.write_bytes(b"pinned synthetic v9\n")
    monkeypatch.setattr(cli, "PINNED_V9_SIZE_BYTES", v9.stat().st_size)
    monkeypatch.setattr(cli, "PINNED_V9_SHA256", hashlib.sha256(v9.read_bytes()).hexdigest())
    call_count = 0

    def run(command: tuple[str, ...], **_kwargs: object) -> cli._CommandExecution:
        nonlocal call_count
        call_count += 1
        output_log_path = _kwargs.get("output_log_path")
        assert isinstance(output_log_path, Path)
        return _execution(
            command,
            exit_code=1 if call_count == 3 else 0,
            output_log_path=output_log_path,
        )

    monkeypatch.setattr(cli, "_run_streamed_command", run)
    runner = cli._make_closure_runner(tmp_path, heartbeat_seconds=30.0)
    mission_root = tmp_path / "mission"
    mission_root.mkdir()

    with pytest.raises(Phase1CCertificationError, match="V10_GENERATE_SECOND"):
        runner(mission_root)
    assert call_count == 3
    assert not tuple(tmp_path.glob(f"{cli._TEST_BASETEMP_PREFIX}*"))


def test_cleanup_refuses_non_owned_directory(tmp_path: Path) -> None:
    unowned = tmp_path / "not-owned"
    unowned.mkdir()

    with pytest.raises(Phase1CCertificationError, match="unowned"):
        cli._cleanup_pytest_basetemp(tmp_path, unowned)
    assert unowned.is_dir()


def test_module_has_import_safe_main_guard() -> None:
    path = Path(cli.__file__)
    module = ast.parse(path.read_text(encoding="utf-8"))
    guarded = [
        node
        for node in module.body
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "__name__"
    ]

    assert guarded
    top_level_call_names: set[str] = set()
    for statement in module.body:
        if not isinstance(statement, ast.Expr) or not isinstance(
            statement.value,
            ast.Call,
        ):
            continue
        if isinstance(statement.value.func, ast.Name):
            top_level_call_names.add(statement.value.func.id)
    assert "run_phase1c_certification" not in top_level_call_names


def test_main_binds_imported_golden_and_cumulative_accounting_without_workload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targeted_tests = object.__new__(cli.Phase1CTestWitness)
    captured: dict[str, object] = {}
    allowed_parent = tmp_path / "storage-v4-phase-1c"
    resume_root = allowed_parent / "native-capacity-05" / "capacity-cumulative"
    resume_root.mkdir(parents=True)

    monkeypatch.setattr(cli, "_repository_root", lambda: tmp_path)
    monkeypatch.setattr(cli, "PHASE1C_ALLOWED_PARENT", allowed_parent)
    monkeypatch.setattr(
        cli,
        "_new_targeted_log_path",
        lambda _candidate_name: tmp_path / "targeted.log",
    )
    monkeypatch.setattr(
        cli,
        "_run_targeted_tests",
        lambda *_args, **_kwargs: targeted_tests,
    )
    monkeypatch.setattr(
        cli,
        "_build_preflight_config",
        lambda *_args, **_kwargs: object.__new__(cli.Phase1CPreflightConfig),
    )
    monkeypatch.setattr(
        cli,
        "_WholeCertificationHeartbeat",
        lambda **_kwargs: nullcontext(SimpleNamespace(progress=lambda _payload: None)),
    )

    def certify(config: object, **_kwargs: object) -> SimpleNamespace:
        captured["config"] = config
        return SimpleNamespace(payload=lambda: {"status": "COMPLETE"})

    monkeypatch.setattr(cli, "run_phase1c_certification", certify)

    assert cli.main(
        ["--resume-cumulative-candidate-root", str(resume_root)]
    ) == 0
    config = captured["config"]
    assert isinstance(config, cli.Phase1CCertificationConfig)
    assert config.golden_producer_candidate_root == (
        cli.GOLDEN_IMPORTED_CANDIDATE_ROOT
    )
    assert config.golden_producer_stdout_log == cli.GOLDEN_PRODUCER_STDOUT_LOG
    assert config.golden_producer_stderr_log == cli.GOLDEN_PRODUCER_STDERR_LOG
    assert config.golden_producer_stdout_sha256 == (
        cli.PINNED_GOLDEN_PRODUCER_STDOUT_SHA256
    )
    assert config.golden_producer_stderr_sha256 == (
        cli.PINNED_GOLDEN_PRODUCER_STDERR_SHA256
    )
    assert config.cumulative_resume_candidate_root == resume_root.resolve(strict=True)
    assert config.historical_attempt_ingestion == cli.HISTORICAL_ATTEMPT_INGESTION


def test_main_closure_only_skips_tests_preflight_workers_and_ingestion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    allowed_parent = tmp_path / "storage-v4-phase1c"
    candidate_root = allowed_parent / "native-capacity-test" / "capacity-cumulative"
    candidate_root.mkdir(parents=True)
    expected_receipt_sha256 = "a" * 64
    fake_result = object.__new__(cli.Phase1CCumulativeWorkerClosureResult)
    captured: dict[str, object] = {}

    def bomb(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("closure-only must bypass normal certification work")

    def close(root: Path, *, expected_receipt_sha256: str) -> object:
        captured["candidate_root"] = root
        captured["receipt_sha256"] = expected_receipt_sha256
        return fake_result

    monkeypatch.setattr(cli, "PHASE1C_ALLOWED_PARENT", allowed_parent)
    monkeypatch.setattr(cli, "_validate_heartbeat", bomb)
    monkeypatch.setattr(cli, "_run_targeted_tests", bomb)
    monkeypatch.setattr(cli, "_build_preflight_config", bomb)
    monkeypatch.setattr(cli, "run_phase1c_certification", bomb)
    monkeypatch.setattr(
        cli,
        "close_phase1c_cumulative_worker_result_from_authority",
        close,
    )
    monkeypatch.setattr(
        cli,
        "_cumulative_orphan_closure_payload",
        lambda result: {
            "closure_scope": "CUMULATIVE_WORKER_RESULT_ONLY",
            "result_is_typed": isinstance(
                result,
                cli.Phase1CCumulativeWorkerClosureResult,
            ),
            "status": "STORAGE_V4_PHASE_1C_CUMULATIVE_ORPHAN_CLOSURE_READY",
        },
    )

    assert cli.main(
        [
            "--closure-only-cumulative-candidate-root",
            str(candidate_root),
            "--closure-only-receipt-sha256",
            expected_receipt_sha256,
        ]
    ) == 0
    assert captured == {
        "candidate_root": candidate_root.resolve(strict=True),
        "receipt_sha256": expected_receipt_sha256,
    }
    emitted = json.loads(capsys.readouterr().out)
    assert emitted["status"] == (
        "STORAGE_V4_PHASE_1C_CUMULATIVE_ORPHAN_CLOSURE_READY"
    )
    assert emitted["result_is_typed"] is True


def test_progress_emits_canonical_json(capsys: pytest.CaptureFixture[str]) -> None:
    cli._progress({"status": "RUNNING", "phase": "synthetic"})

    line = capsys.readouterr().out
    assert line == '{"phase":"synthetic","status":"RUNNING"}\n'
    assert json.loads(line) == {"phase": "synthetic", "status": "RUNNING"}


def test_whole_run_heartbeat_carries_latest_progress_and_honest_scopes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    emitted: list[dict[str, object]] = []
    monkeypatch.setattr(cli, "_process_peak_rss_bytes", lambda: 123_456)
    monkeypatch.setattr(cli, "current_process_cumulative_write_bytes", lambda: 789_012)

    with cli._WholeCertificationHeartbeat(
        emit=lambda payload: emitted.append(dict(payload)),
    ) as heartbeat:
        heartbeat.progress({"phase": "preflight_scan", "status": "RUNNING"})
        payload = heartbeat._payload()

    assert emitted == [{"phase": "preflight_scan", "status": "RUNNING"}]
    assert payload["event"] == "heartbeat"
    assert payload["heartbeat_scope"] == "PHASE1C_WHOLE_CERTIFICATION"
    assert payload["phase"] == "preflight_scan"
    assert payload["last_progress"] == {
        "phase": "preflight_scan",
        "status": "RUNNING",
    }
    assert payload["workload"] is None
    assert payload["commits_completed"] is None
    assert payload["logical_rows_completed"] is None
    assert payload["recent_throughput_status"] == (
        "UNAVAILABLE_NO_ACTIVE_WORKLOAD_PROGRESS"
    )
    assert payload["conservative_eta_status"] == (
        "UNAVAILABLE_NO_ACTIVE_WORKLOAD_PROGRESS"
    )
    assert payload["descendant_process_visibility_scope"] == (
        "LATEST_PROGRESS_SNAPSHOT_ONLY; "
        "DESCENDANT_PROCESS_TREE_NOT_DIRECTLY_OBSERVED"
    )
    assert "stagnation_assessment" not in payload
    assert payload["process_peak_rss_bytes"] == 123_456
    assert payload["process_peak_rss_scope"] == (
        "PARENT_PROCESS_LIFETIME_HIGH_WATER_MARK"
    )
    assert payload["process_cumulative_write_bytes"] == 789_012
    assert payload["process_cumulative_write_bytes_scope"] == (
        "WINDOWS_PARENT_PROCESS_CUMULATIVE_WRITE_TRANSFER_BYTES"
    )
    assert payload["process_cpu_scope"] == (
        "CERTIFIER_PARENT_PROCESS_ONLY_SINCE_RUN_START"
    )


def test_whole_run_heartbeat_derives_same_workload_rate_and_eta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = iter((10_000_000_000, 20_000_000_000))
    monkeypatch.setattr(cli.time, "monotonic_ns", lambda: next(clock))
    monkeypatch.setattr(cli, "_process_peak_rss_bytes", lambda: None)
    monkeypatch.setattr(cli, "current_process_cumulative_write_bytes", lambda: None)
    heartbeat = cli._WholeCertificationHeartbeat(emit=lambda _payload: None)

    heartbeat.progress(
        {
            "phase": "capacity_ingest",
            "workload": "SYNTHETIC_CAPACITY_V1",
            "workload_profile": "GOLDEN_SHAPED",
            "workload_id": "capacity-100",
            "commits_completed": 20,
            "commits_total": 100,
                "logical_rows_completed": 40,
                "logical_rows_total": 200,
                "workload_elapsed_ns": 10_000_000_000,
                "raw_segment_count": 2,
                "paper_segment_count": 2,
                "segment_count": 4,
                "checkpoint_count": 2,
                "segment_checkpoint_status": "EXACT_DURABLE_PUBLICATION_COUNTS",
            }
    )
    first = heartbeat._payload()
    heartbeat.progress(
        {
            "phase": "capacity_ingest",
            "workload": "SYNTHETIC_CAPACITY_V1",
            "workload_profile": "GOLDEN_SHAPED",
            "workload_id": "capacity-100",
            "commits_completed": 30,
            "commits_total": 100,
                "logical_rows_completed": 60,
                "logical_rows_total": 200,
                "workload_elapsed_ns": 20_000_000_000,
                "raw_segment_count": 3,
                "paper_segment_count": 3,
                "segment_count": 6,
                "checkpoint_count": 3,
                "segment_checkpoint_status": "EXACT_DURABLE_PUBLICATION_COUNTS",
            }
    )
    second = heartbeat._payload()

    assert first["conservative_eta_ns"] is None
    assert first["conservative_eta_status"] == (
        "UNAVAILABLE_INSUFFICIENT_HEARTBEAT_WINDOW"
    )
    assert second["recent_commits_per_second"] == "1"
    assert second["recent_logical_rows_per_second"] == "2"
    assert second["conservative_eta_ns"] == 70_000_000_000
    assert second["conservative_eta_status"] == (
        "AVAILABLE_MAX_OF_COMMIT_ROW_RECENT_AND_OVERALL_RATES"
    )
