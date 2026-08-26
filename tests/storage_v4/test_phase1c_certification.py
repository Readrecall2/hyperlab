from __future__ import annotations

import hashlib
import inspect
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from hyperlab.paper.storage_v4 import candidate_tree as candidate_tree_module
from hyperlab.paper.storage_v4 import phase1c_certification as certification_module
from hyperlab.paper.storage_v4.canonical import canonical_json_bytes
from hyperlab.paper.storage_v4.capacity import CapacityProfile, StorageGrowthAssessment
from hyperlab.paper.storage_v4.golden_native import GoldenNativeError
from hyperlab.paper.storage_v4.phase1c_certification import (
    PHASE1C_CAPACITY_LEVELS,
    PHASE1C_HISTORICAL_ATTEMPT_INGESTION,
    PHASE1C_PROVEN,
    PHASE1C_TARGET_NOT_MET,
    Phase1CCertificationConfig,
    Phase1CCertificationError,
    Phase1CCertificationResult,
    Phase1CClosureWitness,
    Phase1CCommandWitness,
    Phase1CCommitAccounting,
    Phase1CMeasurementBundle,
    Phase1CV9ByteWitness,
    compute_phase1c_code_identity,
    make_semantic_gate,
    phase1c_test_source_witnesses,
    require_exact_capacity_levels,
    run_phase1c_certification,
    run_phase1c_measurements,
    witness_phase1c_candidate_tree,
)
from hyperlab.paper.storage_v4.phase1c_evidence import EvidenceSemanticGate
from hyperlab.paper.storage_v4.phase1c_progress import Phase1CHeartbeatWindow
from hyperlab.paper.storage_v4.phase1c_workloads import (
    TARGET_MET,
    TARGET_NOT_MET,
    Phase1CTargetDecisionRole,
    Phase1CWorkloadProgress,
    Phase1CWorkloadProgressStatus,
    decide_phase1c_target_verdict,
)

_SHA256_A = "a" * 64
_SHA256_B = "b" * 64
_CLOSURE_PURPOSES = (
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
)


def _write_minimal_repository(root: Path) -> None:
    files = {
        "pyproject.toml": b"[project]\nname = 'phase1c-test'\n",
        "requirements-runtime.lock": b"runtime==1\n",
        "scripts/capture_phase1c_successor_baseline.py": b"# capture\n",
        "scripts/certify_storage_v4_phase1c.py": b"# certifier\n",
        "scripts/certify_storage_v4_phase1c_successor.py": b"# successor\n",
        "scripts/generate_phase05_paper_evidence.py": b"# phase05 generator\n",
        "scripts/generate_phase12_live_paper_artifacts.py": b"# generator\n",
        "src/hyperlab/__init__.py": b"# package\n",
        "src/hyperlab/nested.py": b"VALUE = 1\n",
    }
    for relative_path, payload in files.items():
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)


def _command(purpose: str) -> Phase1CCommandWitness:
    return Phase1CCommandWitness(
        purpose=purpose,
        command=("python", "-m", purpose.lower()),
        exit_code=0,
        output_sha256=_SHA256_A,
        summary=f"{purpose} passed",
    )


def test_workload_manifest_progress_bridge_emits_canonical_exact_schema() -> None:
    event = Phase1CWorkloadProgress(
        manifest_label="GOLDEN_SHAPED_100000",
        profile=CapacityProfile.GOLDEN_SHAPED,
        processed_commits=10_000,
        total_commits=100_000,
        processed_logical_rows=10_123,
        total_logical_rows=101_234,
        workload_elapsed_ns=9_876_543,
        status=Phase1CWorkloadProgressStatus.IN_PROGRESS,
    )

    assert certification_module._workload_manifest_progress_payload(event) == {
        "checkpoint_count": 0,
        "commits_completed": 10_000,
        "commits_total": 100_000,
        "logical_rows_completed": 10_123,
        "logical_rows_total": 101_234,
        "manifest_label": "GOLDEN_SHAPED_100000",
        "paper_segment_count": 0,
        "phase": "phase1c_workload_manifest",
        "processed_commits": 10_000,
        "profile": "GOLDEN_SHAPED",
        "progress_metrics_scope": (
            "CURRENT_CERTIFIER_PROCESS_MANIFEST_BUILD_AT_COMPLETED_PROGRESS_BOUNDARY"
        ),
        "raw_segment_count": 0,
        "segment_checkpoint_status": (
            "EXACT_NOT_APPLICABLE_NO_STORAGE_PUBLICATION"
        ),
        "segment_count": 0,
        "status": "IN_PROGRESS",
        "total_commits": 100_000,
        "workload": "SYNTHETIC_CAPACITY_MANIFEST_BUILD",
        "workload_elapsed_ns": 9_876_543,
        "workload_id": "phase1c-manifest:GOLDEN_SHAPED_100000",
        "workload_profile": "GOLDEN_SHAPED",
    }

    second_event = Phase1CWorkloadProgress(
        manifest_label="GOLDEN_SHAPED_100000",
        profile=CapacityProfile.GOLDEN_SHAPED,
        processed_commits=20_000,
        total_commits=100_000,
        processed_logical_rows=20_246,
        total_logical_rows=101_234,
        workload_elapsed_ns=19_876_543,
        status=Phase1CWorkloadProgressStatus.IN_PROGRESS,
    )
    window = Phase1CHeartbeatWindow()
    window.render(
        certification_module._workload_manifest_progress_payload(event),
        observed_elapsed_ns=event.workload_elapsed_ns,
    )
    rendered = window.render(
        certification_module._workload_manifest_progress_payload(second_event),
        observed_elapsed_ns=second_event.workload_elapsed_ns,
    )
    assert rendered["recent_throughput_status"] == "AVAILABLE_SAME_WORKLOAD_WINDOW"
    assert rendered["conservative_eta_ns"] is not None
    assert rendered["conservative_eta_status"] == (
        "AVAILABLE_MAX_OF_COMMIT_ROW_RECENT_AND_OVERALL_RATES"
    )


def _closure() -> Phase1CClosureWitness:
    return Phase1CClosureWitness(
        commands=tuple(_command(purpose) for purpose in _CLOSURE_PURPOSES),
        v9=Phase1CV9ByteWitness(
            path="config/paper/phase08-v9-historical-attestation.json",
            size_bytes=2_833,
            before_sha256=_SHA256_B,
            after_sha256=_SHA256_B,
        ),
    )


def _available_assessment(*, gib_per_hour: str, passed: bool) -> StorageGrowthAssessment:
    return StorageGrowthAssessment(
        status="AVAILABLE",
        basis="LOGICAL_SPAN",
        gib_per_hour=gib_per_hour,
        bytes_per_hour="1",
        passed=passed,
    )


def _make_file_symlink(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"file symlinks are unavailable in this environment: {error}")


def test_code_identity_is_stable_canonical_and_detects_source_drift(
    tmp_path: Path,
) -> None:
    repository = (tmp_path / "repository").resolve()
    repository.mkdir()
    _write_minimal_repository(repository)

    first = compute_phase1c_code_identity(repository)
    second = compute_phase1c_code_identity(repository)

    assert first == second
    assert tuple(path for path, _size, _digest in first.files) == (
        "pyproject.toml",
        "requirements-runtime.lock",
        "scripts/capture_phase1c_successor_baseline.py",
        "scripts/certify_storage_v4_phase1c.py",
        "scripts/certify_storage_v4_phase1c_successor.py",
        "scripts/generate_phase05_paper_evidence.py",
        "scripts/generate_phase12_live_paper_artifacts.py",
        "src/hyperlab/__init__.py",
        "src/hyperlab/nested.py",
    )
    assert first.sha256 == hashlib.sha256(
        canonical_json_bytes(first.payload_without_sha256())
    ).hexdigest()

    (repository / "src/hyperlab/nested.py").write_bytes(b"VALUE = 2\n")
    drifted = compute_phase1c_code_identity(repository)

    assert drifted.sha256 != first.sha256
    assert dict((path, digest) for path, _size, digest in drifted.files)[
        "src/hyperlab/nested.py"
    ] != dict((path, digest) for path, _size, digest in first.files)[
        "src/hyperlab/nested.py"
    ]


def test_code_identity_refuses_a_link_in_the_selected_source_set(tmp_path: Path) -> None:
    repository = (tmp_path / "repository").resolve()
    repository.mkdir()
    _write_minimal_repository(repository)
    target = repository / "plain.py"
    target.write_bytes(b"VALUE = 1\n")
    _make_file_symlink(repository / "src/hyperlab/linked.py", target)

    with pytest.raises(Phase1CCertificationError, match="link/reparse"):
        compute_phase1c_code_identity(repository)


def test_targeted_test_source_witnesses_are_sorted_deduplicated_and_drift(
    tmp_path: Path,
) -> None:
    repository = (tmp_path / "repository").resolve()
    repository.mkdir()
    first_path = repository / "tests/a.py"
    second_path = repository / "tests/b.py"
    first_path.parent.mkdir()
    first_path.write_bytes(b"def test_a(): pass\n")
    second_path.write_bytes(b"def test_b(): pass\n")

    first = phase1c_test_source_witnesses(
        repository,
        ("tests/b.py", "tests/a.py", "tests/a.py"),
    )
    assert tuple(path for path, _digest in first) == ("tests/a.py", "tests/b.py")

    first_path.write_bytes(b"def test_a(): assert True\n")
    second = phase1c_test_source_witnesses(
        repository,
        ("tests/a.py", "tests/b.py"),
    )
    assert second != first

    with pytest.raises(Phase1CCertificationError, match="unsafe path"):
        phase1c_test_source_witnesses(repository, ("../outside.py",))


def test_closure_requires_every_command_once_in_exact_canonical_order() -> None:
    closure = _closure()

    assert tuple(item.purpose for item in closure.commands) == _CLOSURE_PURPOSES
    assert closure.payload()["global_pytest_runs"] == 1
    assert closure.sha256 == hashlib.sha256(
        canonical_json_bytes(closure.payload())
    ).hexdigest()

    for invalid in (
        closure.commands[:-1],
        tuple(reversed(closure.commands)),
        (*closure.commands, closure.commands[-1]),
    ):
        with pytest.raises(
            Phase1CCertificationError,
            match="missing, duplicated, or out of canonical order",
        ):
            Phase1CClosureWitness(commands=invalid, v9=closure.v9)


def test_failed_closure_command_and_v9_drift_are_blocking() -> None:
    with pytest.raises(Phase1CCertificationError, match="required closure command failed"):
        Phase1CCommandWitness(
            purpose="RUFF_GLOBAL_FINAL",
            command=("ruff", "check", "."),
            exit_code=1,
            output_sha256=_SHA256_A,
            summary="failed",
        )

    with pytest.raises(Phase1CCertificationError, match="V9 attestation changed"):
        Phase1CV9ByteWitness(
            path="config/paper/phase08-v9-historical-attestation.json",
            size_bytes=2_833,
            before_sha256=_SHA256_A,
            after_sha256=_SHA256_B,
        )


@pytest.mark.parametrize(
    "levels",
    (
        (),
        (100_000, 500_000),
        (500_000, 100_000, 1_000_000),
        (100_000, 500_000, 1_000_000, 2_000_000),
    ),
)
def test_capacity_level_contract_refuses_any_substitution(
    levels: tuple[int, ...],
) -> None:
    with pytest.raises(ValueError, match="exactly 100k, 500k, and 1m"):
        require_exact_capacity_levels(levels)


def test_capacity_level_contract_accepts_only_the_exact_production_staircase() -> None:
    assert require_exact_capacity_levels(list(PHASE1C_CAPACITY_LEVELS)) == (
        100_000,
        500_000,
        1_000_000,
    )


def test_measurement_driver_uses_imported_golden_and_one_cumulative_worker() -> None:
    source = inspect.getsource(run_phase1c_measurements)

    assert "run_golden_native_worker(" not in source
    assert "GoldenNativeWorkerRequest(" not in source
    assert "run_phase1c_capacity_worker(" not in source
    assert "Phase1CCapacityWorkerRequest(" not in source
    assert source.count("reattest_golden_native_candidate(") == 1
    assert source.count("run_phase1c_cumulative_capacity_worker(") == 1
    assert source.count("Phase1CCumulativeCapacityWorkerRequest(") == 1
    assert source.count("candidate_root=capacity_candidate_root") == 1
    assert "resume_existing=config.cumulative_resume_candidate_root is not None" in source
    assert 'candidate_root=mission_root / f"capacity-{level}"' not in source


def test_bundle_level_mappings_come_from_one_cumulative_root_and_config(
    tmp_path: Path,
) -> None:
    shared_root = (tmp_path / "capacity-cumulative").resolve()
    boundaries = tuple(
        SimpleNamespace(
            manifest=SimpleNamespace(commit_count=level),
            measurement=SimpleNamespace(commit_count=level),
            evidence=SimpleNamespace(
                candidate_root=shared_root,
                config_identity=_SHA256_A,
            ),
        )
        for level in PHASE1C_CAPACITY_LEVELS
    )
    bundle = Phase1CMeasurementBundle(
        preflight=cast(object, None),
        code_identity=cast(object, None),
        runtime_identity=cast(object, None),
        golden_shape=cast(object, None),
        workloads=cast(object, None),
        golden=cast(object, None),
        golden_byte_census=cast(object, None),
        tail=cast(object, None),
        adversarial_measurement=cast(object, None),
        adversarial_evidence=cast(object, None),
        cumulative_capacity=cast(
            object,
            SimpleNamespace(typed_boundaries=boundaries),
        ),
        accounting=cast(object, None),
    )

    assert tuple(dict(bundle.level_measurements)) == PHASE1C_CAPACITY_LEVELS
    evidence_by_level = dict(bundle.level_evidence)
    assert set(evidence_by_level) == set(PHASE1C_CAPACITY_LEVELS)
    assert {item.candidate_root for item in evidence_by_level.values()} == {
        shared_root
    }
    assert {item.config_identity for item in evidence_by_level.values()} == {
        _SHA256_A
    }


def test_commit_accounting_separates_imported_golden_and_historical_attempts() -> None:
    assert PHASE1C_HISTORICAL_ATTEMPT_INGESTION == (
        ("native-golden-03", 204_000, 216_000),
        ("native-golden-04", 352_267, 352_267),
        ("native-capacity-05", 1_120_005, 1_120_005),
    )
    certification_module._require_canonical_historical_attempt_ingestion(
        PHASE1C_HISTORICAL_ATTEMPT_INGESTION
    )
    with pytest.raises(
        Phase1CCertificationError,
        match="differs from authenticated prior-attempt counts",
    ):
        certification_module._require_canonical_historical_attempt_ingestion(
            (
                ("native-golden-03", 204_000, 204_000),
                ("native-golden-04", 352_267, 352_267),
                ("native-capacity-05", 1_120_005, 1_120_005),
            )
        )

    accounting = Phase1CCommitAccounting(
        golden_commits_audited=252_262,
        golden_commits_ingested=0,
        golden_prefix_commits_reingested=0,
        tail_commits_ingested=100_005,
        adversarial_commits_ingested=20_000,
        cumulative_commits_ingested=1_000_000,
        cumulative_prefix_commits_reingested=0,
        historical_attempt_ingestion=(
            ("native-golden-03", 204_000, 216_000),
            ("native-golden-04", 352_267, 352_267),
            ("native-capacity-05", 1_120_005, 1_120_005),
        ),
    )

    assert accounting.current_mission_paper_commits_sealed == 1_120_005
    assert accounting.current_mission_raw_records_published == 1_120_005
    assert accounting.historical_paper_commits_sealed == 1_676_272
    assert accounting.historical_raw_records_published == 1_688_272
    assert accounting.historical_raw_only_unsealed_records == 12_000
    assert accounting.all_attempts_paper_commits_sealed == 2_796_277
    assert accounting.all_attempts_raw_records_published == 2_808_277
    assert accounting.all_attempts_raw_only_unsealed_records == 12_000
    payload = accounting.payload()
    assert payload["current_mission"] == {
        "cumulative_prefix_commits_reingested": 0,
        "golden_commits_audited": 252_262,
        "golden_reattestation_paper_commits_sealed": 0,
        "golden_prefix_commits_reingested": 0,
        "golden_reattestation_raw_records_published": 0,
        "paper_commits_sealed": 1_120_005,
        "raw_only_unsealed_records": 0,
        "raw_records_published": 1_120_005,
        "workloads": {
            "adversarial_commits_ingested": 20_000,
            "cumulative_commits_ingested": 1_000_000,
            "tail_commits_ingested": 100_005,
        },
    }
    assert payload["historical_attempts"] == [
        {
            "label": "native-golden-03",
            "paper_commits_sealed": 204_000,
            "raw_only_unsealed_records": 12_000,
            "raw_records_published": 216_000,
        },
        {
            "label": "native-golden-04",
            "paper_commits_sealed": 352_267,
            "raw_only_unsealed_records": 0,
            "raw_records_published": 352_267,
        },
        {
            "label": "native-capacity-05",
            "paper_commits_sealed": 1_120_005,
            "raw_only_unsealed_records": 0,
            "raw_records_published": 1_120_005,
        },
    ]
    assert payload["historical_totals"] == {
        "paper_commits_sealed": 1_676_272,
        "raw_only_unsealed_records": 12_000,
        "raw_records_published": 1_688_272,
    }
    assert payload["all_attempts_totals"] == {
        "paper_commits_sealed": 2_796_277,
        "raw_only_unsealed_records": 12_000,
        "raw_records_published": 2_808_277,
    }

    with pytest.raises(
        Phase1CCertificationError,
        match="cumulative prefixes must not be reingested",
    ):
        Phase1CCommitAccounting(
            golden_commits_audited=252_262,
            golden_commits_ingested=0,
            golden_prefix_commits_reingested=0,
            tail_commits_ingested=100_005,
            adversarial_commits_ingested=20_000,
            cumulative_commits_ingested=1_000_000,
            cumulative_prefix_commits_reingested=1,
        )

    with pytest.raises(
        Phase1CCertificationError,
        match="raw publication count cannot trail sealed Paper commits",
    ):
        Phase1CCommitAccounting(
            golden_commits_audited=252_262,
            golden_commits_ingested=0,
            golden_prefix_commits_reingested=0,
            tail_commits_ingested=100_005,
            adversarial_commits_ingested=20_000,
            cumulative_commits_ingested=1_000_000,
            cumulative_prefix_commits_reingested=0,
            historical_attempt_ingestion=(("invalid", 2, 1),),
        )


def test_semantic_gate_digest_is_recomputable_and_does_not_mutate_result() -> None:
    result: dict[str, object] = {
        "counts": {"commits": 10, "rows": 20},
        "status": "EXACT",
    }
    original = {"counts": {"commits": 10, "rows": 20}, "status": "EXACT"}

    evidence = make_semantic_gate(
        EvidenceSemanticGate.EXACT_LOGICAL_MATCH,
        certifier_contract="STORAGE_V4_PHASE1C_TEST_ORACLE_V1",
        result=result,
        passed=True,
    )
    digest_payload = {
        **result,
        "certifier_contract": "STORAGE_V4_PHASE1C_TEST_ORACLE_V1",
        "passed": True,
    }

    assert result == original
    assert evidence.gate is EvidenceSemanticGate.EXACT_LOGICAL_MATCH
    assert evidence.as_dict() == {
        "certifier_contract": "STORAGE_V4_PHASE1C_TEST_ORACLE_V1",
        "certifier_result_sha256": hashlib.sha256(
            canonical_json_bytes(digest_payload)
        ).hexdigest(),
        "passed": True,
    }


def test_candidate_tree_hash_is_canonical_and_changes_on_later_drift(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "candidate").resolve()
    (root / "nested").mkdir(parents=True)
    (root / "a.bin").write_bytes(b"alpha")
    (root / "nested/b.bin").write_bytes(b"beta")

    first = witness_phase1c_candidate_tree(root)
    assert tuple(item.relative_path for item in first.files) == (
        "a.bin",
        "nested/b.bin",
    )
    assert first.total_bytes == len(b"alpha") + len(b"beta")
    assert first.tree_sha256 == hashlib.sha256(
        canonical_json_bytes(first.payload_without_sha256())
    ).hexdigest()
    assert witness_phase1c_candidate_tree(root) == first

    (root / "empty-directory").mkdir()
    with_empty_directory = witness_phase1c_candidate_tree(root)
    assert with_empty_directory.tree_sha256 != first.tree_sha256
    assert with_empty_directory.directories == ("empty-directory", "nested")

    (root / "a.bin").write_bytes(b"alpha-drifted")
    mutated = witness_phase1c_candidate_tree(root)
    assert mutated.tree_sha256 != with_empty_directory.tree_sha256

    (root / "new.bin").write_bytes(b"addition")
    extended = witness_phase1c_candidate_tree(root)
    assert extended.tree_sha256 != mutated.tree_sha256
    assert tuple(item.relative_path for item in extended.files) == (
        "a.bin",
        "nested/b.bin",
        "new.bin",
    )


def test_candidate_tree_refuses_mutation_after_a_file_was_hashed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "candidate").resolve()
    root.mkdir()
    first_path = root / "a.bin"
    first_path.write_bytes(b"before")
    (root / "b.bin").write_bytes(b"stable")
    original_hash = candidate_tree_module._hash_regular_file
    mutated = False

    def hash_then_mutate(path: Path) -> tuple[int, str]:
        nonlocal mutated
        result = original_hash(path)
        if path == first_path and not mutated:
            first_path.write_bytes(b"changed-after-individual-hash")
            mutated = True
        return result

    monkeypatch.setattr(candidate_tree_module, "_hash_regular_file", hash_then_mutate)

    with pytest.raises(Phase1CCertificationError, match=r"candidate.*changed"):
        witness_phase1c_candidate_tree(root)


def test_candidate_tree_refuses_addition_after_enumeration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "candidate").resolve()
    root.mkdir()
    first_path = root / "a.bin"
    first_path.write_bytes(b"first")
    (root / "b.bin").write_bytes(b"second")
    original_hash = candidate_tree_module._hash_regular_file
    added = False

    def hash_then_add(path: Path) -> tuple[int, str]:
        nonlocal added
        result = original_hash(path)
        if path == first_path and not added:
            (root / "added-during-tree-hash.bin").write_bytes(b"addition")
            added = True
        return result

    monkeypatch.setattr(candidate_tree_module, "_hash_regular_file", hash_then_add)

    with pytest.raises(Phase1CCertificationError, match=r"candidate.*changed"):
        witness_phase1c_candidate_tree(root)


def test_candidate_tree_refuses_links_or_reparse_points(tmp_path: Path) -> None:
    root = (tmp_path / "candidate").resolve()
    root.mkdir()
    (root / "regular.bin").write_bytes(b"regular")
    target = tmp_path / "outside.bin"
    target.write_bytes(b"outside")
    _make_file_symlink(root / "unsafe.bin", target)

    with pytest.raises(Phase1CCertificationError, match="link/reparse"):
        witness_phase1c_candidate_tree(root)


def test_candidate_tree_set_contains_one_shared_capacity_terminal_root(
    tmp_path: Path,
) -> None:
    roots = {
        name: (tmp_path / name).resolve()
        for name in ("golden", "tail", "adversarial", "capacity-cumulative")
    }
    bundle = cast(
        Phase1CMeasurementBundle,
        SimpleNamespace(
            golden=SimpleNamespace(candidate_root=roots["golden"]),
            tail=SimpleNamespace(candidate_root=roots["tail"]),
            adversarial_evidence=SimpleNamespace(
                candidate_root=roots["adversarial"]
            ),
            cumulative_capacity=SimpleNamespace(
                candidate_root=roots["capacity-cumulative"]
            ),
        ),
    )

    observed = certification_module._tree_roots(bundle)

    assert observed == (
        ("GOLDEN_NATIVE", roots["golden"]),
        ("BOUNDED_TAIL_RESTART", roots["tail"]),
        ("ADVERSARIAL_STORAGE", roots["adversarial"]),
        ("CAPACITY_CUMULATIVE", roots["capacity-cumulative"]),
    )
    assert sum(label == "CAPACITY_CUMULATIVE" for label, _root in observed) == 1
    assert all("GOLDEN_SHAPED_" not in label for label, _root in observed)


def test_terminal_postflight_precedes_last_candidate_rehash_and_complete() -> None:
    source = inspect.getsource(certification_module._publish_phase1c_evidence)

    final_postflight = source.rindex("verify_phase1c_postflight(")
    first_candidate_witness = source.index("_witness_bound_candidate_trees(")
    final_candidate_rehash = source.rindex("_witness_bound_candidate_trees(")
    terminal_complete = source.rindex("publisher.publish_terminal_complete(")

    assert source.count("_witness_bound_candidate_trees(") == 2
    assert first_candidate_witness < final_postflight
    assert final_postflight < final_candidate_rehash < terminal_complete


@pytest.mark.parametrize(
    ("mutated_candidate", "expected_label"),
    (
        ("golden", "GOLDEN_NATIVE"),
        ("tail", "BOUNDED_TAIL_RESTART"),
    ),
)
def test_mutation_after_measurements_before_first_publication_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutated_candidate: str,
    expected_label: str,
) -> None:
    def measured_candidate(name: str) -> tuple[Path, object]:
        root = (tmp_path / name).resolve()
        root.mkdir()
        (root / "data.bin").write_bytes(name.encode("utf-8"))
        return root, witness_phase1c_candidate_tree(root)

    golden_root, golden_tree = measured_candidate("golden")
    adversarial_root, adversarial_tree = measured_candidate("adversarial")
    tail_root, tail_tree = measured_candidate("tail")
    capacity_root, capacity_tree = measured_candidate("capacity-cumulative")
    bundle = cast(
        Phase1CMeasurementBundle,
        SimpleNamespace(
            preflight=SimpleNamespace(
                witness=SimpleNamespace(mission_root=tmp_path),
            ),
            golden=SimpleNamespace(
                candidate_root=golden_root,
                candidate_tree_before=golden_tree,
            ),
            tail=SimpleNamespace(
                candidate_root=tail_root,
                audited_candidate_tree=tail_tree,
                cases=(SimpleNamespace(candidate_root=tail_root / "case-0"),),
            ),
            adversarial_evidence=SimpleNamespace(
                candidate_root=adversarial_root,
                audited_candidate_tree=adversarial_tree,
            ),
            cumulative_capacity=SimpleNamespace(
                candidate_root=capacity_root,
                terminal_shared_candidate_tree=capacity_tree,
            ),
        ),
    )

    config = cast(
        Phase1CCertificationConfig,
        object.__new__(Phase1CCertificationConfig),
    )
    measurements_returned = False

    def return_measurements(
        received: Phase1CCertificationConfig,
        *,
        progress: object,
    ) -> Phase1CMeasurementBundle:
        nonlocal measurements_returned
        del progress
        assert received is config
        measurements_returned = True
        return bundle

    def closure_then_mutate(mission_root: Path) -> Phase1CClosureWitness:
        del mission_root
        assert measurements_returned
        mutated_root = {
            "golden": golden_root,
            "tail": tail_root,
        }[mutated_candidate]
        (mutated_root / "post-measurement-drift.bin").write_bytes(b"drift")
        return _closure()

    def publication_boundary(
        received_config: Phase1CCertificationConfig,
        received_bundle: Phase1CMeasurementBundle,
        received_closure: Phase1CClosureWitness,
        *,
        progress: object,
    ) -> Phase1CCertificationResult:
        del received_config, received_closure, progress
        certification_module._witness_bound_candidate_trees(
            received_bundle,
            progress=None,
            heartbeat_interval_seconds=30.0,
        )
        return cast(Phase1CCertificationResult, object())

    monkeypatch.setattr(certification_module, "run_phase1c_measurements", return_measurements)
    monkeypatch.setattr(
        certification_module,
        "_publish_phase1c_evidence",
        publication_boundary,
    )

    with pytest.raises(
        Phase1CCertificationError,
        match=f"{expected_label} candidate differs from its audit-bound tree witness",
    ):
        run_phase1c_certification(
            config,
            closure_runner=closure_then_mutate,
        )

    assert measurements_returned
    assert not (tmp_path / "evidence").exists()


def test_target_miss_is_an_authorized_terminal_characterization() -> None:
    verdict = decide_phase1c_target_verdict(
        golden_assessment=_available_assessment(gib_per_hour="0.10", passed=True),
        level_assessments={
            100_000: _available_assessment(gib_per_hour="0.10", passed=True),
            500_000: _available_assessment(gib_per_hour="0.15", passed=True),
            1_000_000: _available_assessment(gib_per_hour="0.25", passed=False),
        },
    )

    assert verdict.terminal_verdict == PHASE1C_TARGET_NOT_MET
    assert verdict.terminal_target_status == TARGET_NOT_MET
    assert verdict.diagnostics[-1].role is Phase1CTargetDecisionRole.TERMINAL_DECISION
    assert verdict.payload()["terminal_decision_basis"] == (
        "GOLDEN_SHAPED_1000000_TOTAL_RAW_PLUS_PAPER"
    )


def test_only_the_1m_level_controls_the_target_verdict() -> None:
    verdict = decide_phase1c_target_verdict(
        golden_assessment=_available_assessment(gib_per_hour="0.90", passed=False),
        level_assessments={
            100_000: _available_assessment(gib_per_hour="0.80", passed=False),
            500_000: _available_assessment(gib_per_hour="0.70", passed=False),
            1_000_000: _available_assessment(gib_per_hour="0.19", passed=True),
        },
    )

    assert verdict.terminal_verdict == PHASE1C_PROVEN
    assert verdict.terminal_target_status == TARGET_MET
    assert all(
        diagnostic.role is Phase1CTargetDecisionRole.DIAGNOSTIC_ONLY
        for diagnostic in verdict.diagnostics[:-1]
    )


def test_certification_sequences_measurement_closure_then_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = cast(
        Phase1CCertificationConfig,
        object.__new__(Phase1CCertificationConfig),
    )
    bundle = cast(
        Phase1CMeasurementBundle,
        SimpleNamespace(
            preflight=SimpleNamespace(
                witness=SimpleNamespace(mission_root=tmp_path),
            )
        ),
    )
    closure = _closure()
    published = cast(Phase1CCertificationResult, object())
    timeline: list[str] = []

    def fake_measurements(
        received: Phase1CCertificationConfig,
        *,
        progress: object,
    ) -> Phase1CMeasurementBundle:
        assert received is config
        assert progress is progress_callback
        timeline.append("measurements")
        return bundle

    def closure_runner(mission_root: Path) -> Phase1CClosureWitness:
        assert mission_root == tmp_path
        timeline.append("closure")
        return closure

    def fake_publish(
        received_config: Phase1CCertificationConfig,
        received_bundle: Phase1CMeasurementBundle,
        received_closure: Phase1CClosureWitness,
        *,
        progress: object,
    ) -> Phase1CCertificationResult:
        assert received_config is config
        assert received_bundle is bundle
        assert received_closure is closure
        assert progress is progress_callback
        timeline.append("publish")
        return published

    def progress_callback(event: object) -> None:
        assert isinstance(event, dict)
        timeline.append(f"{event['phase']}:{event['status']}")

    monkeypatch.setattr(certification_module, "run_phase1c_measurements", fake_measurements)
    monkeypatch.setattr(certification_module, "_publish_phase1c_evidence", fake_publish)

    result = run_phase1c_certification(
        config,
        closure_runner=closure_runner,
        progress=progress_callback,
    )

    assert result is published
    assert timeline == [
        "measurements",
        "phase1c_repository_closure:RUNNING",
        "closure",
        "phase1c_repository_closure:COMPLETE",
        "publish",
    ]


def test_golden_measurement_divergence_never_runs_closure_or_creates_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = cast(
        Phase1CCertificationConfig,
        object.__new__(Phase1CCertificationConfig),
    )
    closure_called = False
    publish_called = False

    def divergent_measurement(
        received: Phase1CCertificationConfig,
        *,
        progress: object,
    ) -> Phase1CMeasurementBundle:
        del progress
        assert received is config
        raise GoldenNativeError("synthetic Golden/V4 divergence")

    def closure_runner(mission_root: Path) -> Phase1CClosureWitness:
        nonlocal closure_called
        del mission_root
        closure_called = True
        return _closure()

    def forbidden_publish(*args: object, **kwargs: object) -> Phase1CCertificationResult:
        nonlocal publish_called
        del args, kwargs
        publish_called = True
        return cast(Phase1CCertificationResult, object())

    monkeypatch.setattr(certification_module, "run_phase1c_measurements", divergent_measurement)
    monkeypatch.setattr(certification_module, "_publish_phase1c_evidence", forbidden_publish)

    with pytest.raises(GoldenNativeError, match="Golden/V4 divergence"):
        run_phase1c_certification(config, closure_runner=closure_runner)

    assert closure_called is False
    assert publish_called is False
    assert not (tmp_path / "evidence").exists()
