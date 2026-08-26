from __future__ import annotations

import ast
import hashlib
import json
import sys
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

import scripts.certify_storage_v4_phase1c_successor as successor_cli

successor_module = successor_cli._SUCCESSOR_MODULE
canonical_json_bytes = successor_module.canonical_json_bytes
witness_candidate_tree = successor_module.witness_candidate_tree
CAPACITY_MARKERS = successor_module.CAPACITY_MARKERS
PHASE1C_TARGET_NOT_MET_VERDICT = (
    successor_module.PHASE1C_TARGET_NOT_MET_VERDICT
)
SUCCESSOR_COMPLETE_FORMAT = successor_module.SUCCESSOR_COMPLETE_FORMAT
SUCCESSOR_COMPLETE_NAME = successor_module.SUCCESSOR_COMPLETE_NAME
SUCCESSOR_MARKERS = successor_module.SUCCESSOR_MARKERS
SUCCESSOR_RECEIPT_NAME = successor_module.SUCCESSOR_RECEIPT_NAME
SUCCESSOR_TARGETED_LOG_NAME = successor_module.SUCCESSOR_TARGETED_LOG_NAME
SUCCESSOR_TARGETED_TEST_PATHS = successor_module.SUCCESSOR_TARGETED_TEST_PATHS
SUCCESSOR_V9_RELATIVE_PATH = successor_module.SUCCESSOR_V9_RELATIVE_PATH
SUCCESSOR_V9_SHA256 = successor_module.SUCCESSOR_V9_SHA256
SUCCESSOR_V9_SIZE_BYTES = successor_module.SUCCESSOR_V9_SIZE_BYTES
Phase1CSuccessorClosureWitness = (
    successor_module.Phase1CSuccessorClosureWitness
)
Phase1CSuccessorCommandWitness = (
    successor_module.Phase1CSuccessorCommandWitness
)
Phase1CSuccessorConfig = successor_module.Phase1CSuccessorConfig
Phase1CSuccessorError = successor_module.Phase1CSuccessorError
Phase1CSuccessorExpectations = successor_module.Phase1CSuccessorExpectations
Phase1CSuccessorTestWitness = successor_module.Phase1CSuccessorTestWitness
Phase1CSuccessorV9ByteWitness = (
    successor_module.Phase1CSuccessorV9ByteWitness
)
compute_phase1c_successor_verifier_state = (
    successor_module.compute_phase1c_successor_verifier_state
)
finalize_phase1c_successor_closure = (
    successor_module.finalize_phase1c_successor_closure
)
reattest_phase1c_successor = successor_module.reattest_phase1c_successor

_COUNTS = (2, 4, 6)
_PRODUCER_CODE = "1" * 64
_PRODUCER_RUNTIME = "2" * 64
_CONFIG_IDENTITY = "3" * 64
_PROFILE = "GOLDEN_SHAPED"
_GENERATOR = "storage-v4-synthetic-capacity-v3"
_BASELINE_PATH = Path(
    "config/paper/storage-v4-phase1c-successor-baseline-byte-witness.json"
)
_BASELINE_SHA256 = (
    "32c30490eb3a9934165a67fd76b0127fb698316710fdadd67b08be081335c740"
)
_BASELINE_SIZE = 32_222
_ACQUIRED_IDENTITY = (
    "fa0e55fb4a42488eaa52a69355909c578f45994c64e2849df3e859a0089c5936"
)
_BASELINE_COMMIT = "f6c34d3c1e37bccf7ae72ef26cb8d8797dda8ed5"
_CLOSURE_SHA256 = (
    "e8bc7f8f4e3fce05bbb5681b95963414cea6d26a0de813d3f22a39a30a0c9bb7"
)
_ENTRYPOINTS = (
    "hyperlab.paper.storage_v4.capacity_runner",
    "hyperlab.paper.storage_v4.phase1c_workers",
    "hyperlab.paper.storage_v4.phase1c_workloads",
)
_CLOSURE_PURPOSES = (
    "V10_CHECK",
    "PHASE05_CHECK",
    "RUFF_GLOBAL_FINAL",
    "MYPY_HYPERLAB_FINAL",
    "PYTEST_GLOBAL_FINAL_SINGLE_RUN",
    "GIT_DIFF_CHECK_FINAL",
)
_CLOSURE_LOG_NAMES = (
    "01-v10-check.log",
    "02-phase05-check.log",
    "03-ruff-global-final.log",
    "04-mypy-hyperlab-final.log",
    "05-pytest-global-final-single-run.log",
    "06-git-diff-check-final.log",
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _baseline_witness_for_repository(
    repository_root: Path,
    tmp_path: Path,
) -> tuple[Path, bytes, str]:
    source = (repository_root / _BASELINE_PATH).read_bytes()
    assert len(source) == _BASELINE_SIZE
    assert _sha256(source) == _BASELINE_SHA256
    payload = json.loads(source)
    assert isinstance(payload, dict)
    acquisition = payload["acquisition"]
    assert isinstance(acquisition, dict)
    acquisition["repository_root"] = str(repository_root)
    code_identity = payload["acquired_verifier_global_identity"]
    assert isinstance(code_identity, dict)
    assert code_identity["sha256"] == _ACQUIRED_IDENTITY
    code_identity["repository_root"] = str(repository_root)
    identity_material = dict(code_identity)
    del identity_material["sha256"]
    code_identity["sha256"] = _sha256(canonical_json_bytes(identity_material))
    relocated = canonical_json_bytes(payload)
    path = (tmp_path / "phase1c-successor-baseline-byte-witness.json").resolve()
    path.write_bytes(relocated)
    return path, relocated, str(code_identity["sha256"])


def _workload_manifest(commit_count: int) -> dict[str, object]:
    workload_sha256 = _sha256(f"workload-{commit_count}".encode())
    return {
        "activity_rates": {
            "alert_every_commits": 2,
            "incident_every_commits": None,
            "ledger_every_commits": 3,
            "market_gap_count": 0,
            "projection_every_commits": 2,
        },
        "artifact": "STORAGE_V4_SYNTHETIC_CAPACITY_WORKLOAD_MANIFEST_V1",
        "configuration": {
            "activity_payload_bytes": {
                "alert": 1,
                "incident": 0,
                "ledger": 1,
                "market_gap": 0,
                "projection": 1,
            },
            "adversarial_schedule": {
                "boundary_intervals": [],
                "funding_burst_period": None,
                "funding_burst_width": None,
            },
            "bounded_tail_max": None,
            "commit_count": commit_count,
            "start_time_ns": 1,
        },
        "expected": {
            "commit_count": commit_count,
            "logical_row_count": commit_count + 1,
            "workload_sha256": workload_sha256,
        },
        "generator_version": _GENERATOR,
        "golden_census_sha256": "4" * 64,
        "markers": list(CAPACITY_MARKERS),
        "payload_sizes": [
            {
                "payload_cardinality": 1,
                "payload_max_bytes": 1,
                "payload_min_bytes": 1,
                "record_type": "INPUT",
            }
        ],
        "profile": _PROFILE,
        "seed": 7,
        "strategies": ["fixture"],
        "tail_restart_sizes": [],
        "temporal_cadence": {
            "cadence_ns": 1,
            "start_time_ns": 1,
            "status": "DEFINED",
        },
        "type_distribution": [
            {
                "payload_cardinality": 1,
                "payload_max_bytes": 1,
                "payload_min_bytes": 1,
                "record_type": "INPUT",
                "stream": "INPUT",
                "weight": 1,
            }
        ],
    }


def _measurement(
    *, commit_count: int, manifest_sha256: str, workload_sha256: str
) -> dict[str, object]:
    return {
        "byte_census": {
            "anchors_witnesses": {"paper_bytes": 1, "raw_bytes": 1, "total_bytes": 2},
            "anchors_witnesses_bytes": 2,
            "category_shares_of_total": {},
            "category_shares_status": "AVAILABLE",
            "current_cache": {"paper_bytes": 1, "raw_bytes": 1, "total_bytes": 2},
            "current_cache_bytes": 2,
            "paper": {},
            "paper_incremental_bytes": 10,
            "raw": {},
            "raw_bytes": 20,
            "scratch_current_bytes": 0,
            "scratch_peak_bytes": 0,
            "total_bytes": 30,
            "total_excludes_scratch": True,
            "total_with_current_scratch_bytes": 30,
        },
        "counts": {
            "checkpoints": 1,
            "commits": commit_count,
            "logical_rows": commit_count + 1,
            "manifests": 1,
            "segments": 1,
        },
        "cpu_ns": 10,
        "durations": {
            "checkpoint": {"count": 1, "observations_ns": [1], "total_ns": 1},
            "manifest_publication": {
                "count": 1,
                "observations_ns": [1],
                "total_ns": 1,
            },
            "seal": {"count": 1, "observations_ns": [1], "total_ns": 1},
        },
        "full_history_audit_ns": 10,
        "markers": list(CAPACITY_MARKERS),
        "metadata_authentication_ns": 1,
        "observed_workload_sha256": workload_sha256,
        "retained_bytes_per_raw_input_byte": {"ratio": "1", "status": "AVAILABLE"},
        "rss": {"peak_bytes": 10, "status": "AVAILABLE"},
        "startup": {
            "duration_ns": 1,
            "historical_commits_replayed": 0,
            "historical_segments_read": 0,
            "tail_entries_replayed": 0,
        },
        "storage_growth_target": {
            "basis": "COMMITS_PER_HOUR",
            "bytes_per_hour": "1",
            "gib_per_hour": "1",
            "passed": False,
            "relation": "<",
            "status": "AVAILABLE",
            "target_gib_per_hour": "0.20",
        },
        "throughput": {},
        "wall_ns": 20,
        "workload_manifest_sha256": manifest_sha256,
        "write_amplification": {"ratio": "1", "status": "AVAILABLE"},
    }


def _evidence(
    *,
    candidate_root: Path,
    tree: dict[str, object],
    commit_count: int,
    workload_sha256: str,
) -> dict[str, object]:
    prefix = _sha256(f"prefix-{commit_count}".encode())
    return {
        "authority": {
            "candidate_root": str(candidate_root),
            "code_identity": _PRODUCER_CODE,
            "config_identity": _CONFIG_IDENTITY,
            "paper_store_id": "paper",
            "raw_lake_id": "lake",
            "raw_store_id": "raw",
            "run_id": "run",
            "runtime_identity": _PRODUCER_RUNTIME,
        },
        "batching": {
            "batch_count": 1,
            "max_batch_commits_observed": commit_count,
            "seal_count": 1,
        },
        "integrity": {
            "alignment_status": "PHASE1C_RAW_PAPER_ALIGNED",
            "audited_candidate_tree": tree,
            "commit_count": commit_count,
            "final_prefix_root": prefix,
            "market_gap_count": 0,
            "oracle_commit_count": commit_count,
            "oracle_final_prefix_root": prefix,
            "oracle_logical_row_count": commit_count + 1,
            "oracle_workload_sha256": workload_sha256,
            "raw_reference_count": commit_count,
            "raw_reference_prefix_root": _sha256(f"raw-{commit_count}".encode()),
        },
        "scopes": {
            "checkpoint": "fixture",
            "manifest": "fixture",
            "metadata": "fixture",
            "rss": "fixture",
            "scratch": "fixture",
            "scratch_status": "fixture",
            "storage_rate": "fixture",
            "wall": "fixture",
        },
        "startup": {
            "file_access_trace": {},
            "paper_checkpoint_used": True,
            "paper_historical_commits_not_read": True,
            "paper_segments_read": 0,
            "paper_tail_entries_replayed": 0,
            "raw_historical_segments_read": 0,
            "raw_manifest_namespace_entries_scanned": 1,
            "raw_manifests_opened": 1,
        },
    }


@dataclass(frozen=True)
class _Fixture:
    repository_root: Path
    source_root: Path
    candidate_root: Path
    boundary_root: Path
    producer_log: Path
    run06_root: Path
    receipt_root: Path
    config: Phase1CSuccessorConfig


def _build_fixture(tmp_path: Path) -> _Fixture:
    repository_root = Path(__file__).resolve().parents[2]
    baseline_path, baseline_bytes, baseline_identity = _baseline_witness_for_repository(
        repository_root,
        tmp_path,
    )
    phase1c_root = (tmp_path / "phase1c").resolve()
    source_root = phase1c_root / "native-capacity-05"
    candidate_root = source_root / "capacity-cumulative"
    boundary_root = source_root / ".capacity-cumulative.phase1c-boundaries"
    candidate_root.mkdir(parents=True)
    boundary_root.mkdir()
    (candidate_root / "segment.bin").write_bytes(b"immutable-producer-bytes")
    tree = witness_candidate_tree(candidate_root).payload()
    manifests = [_workload_manifest(count) for count in _COUNTS]
    manifest_hashes = [_sha256(canonical_json_bytes(item)) for item in manifests]
    previous: str | None = None
    terminal_certificate_sha256 = ""
    for count, manifest, manifest_sha256 in zip(
        _COUNTS, manifests, manifest_hashes, strict=True
    ):
        expected = manifest["expected"]
        assert isinstance(expected, dict)
        workload_sha256 = expected["workload_sha256"]
        assert isinstance(workload_sha256, str)
        payload = {
            "artifact": "STORAGE_V4_PHASE_1C_CUMULATIVE_BOUNDARY_CERTIFICATE_V1",
            "authority": {
                "checkpoint_root": _sha256(f"checkpoint-{count}".encode()),
                "paper_manifest_root": _sha256(f"paper-{count}".encode()),
                "raw_manifest_root": _sha256(f"raw-manifest-{count}".encode()),
            },
            "boundary_commit_count": count,
            "boundary_manifest": manifest,
            "boundary_manifest_sha256": manifest_sha256,
            "evidence": _evidence(
                candidate_root=candidate_root,
                tree=tree,
                commit_count=count,
                workload_sha256=workload_sha256,
            ),
            "measurement": _measurement(
                commit_count=count,
                manifest_sha256=manifest_sha256,
                workload_sha256=workload_sha256,
            ),
            "previous_certificate_sha256": previous,
            "terminal_manifest_sha256": manifest_hashes[-1],
            "workload_prefix": {
                "commit_count": count,
                "logical_row_count": count + 1,
                "sha256": workload_sha256,
            },
        }
        data = canonical_json_bytes(payload)
        certificate = boundary_root / f"{count:016d}-{manifest_sha256}.json"
        certificate.write_bytes(data)
        previous = _sha256(data)
        terminal_certificate_sha256 = previous
    producer_log = (tmp_path / "producer.jsonl").resolve()
    producer_log.write_bytes(b'{"phase":"terminal","status":"FAILED_AFTER_AUDIT"}\n')
    receipt_root = (tmp_path / "successor").resolve()
    expectations = Phase1CSuccessorExpectations(
        boundary_commit_counts=_COUNTS,
        terminal_certificate_sha256=terminal_certificate_sha256,
        terminal_manifest_sha256=manifest_hashes[-1],
        terminal_tree_sha256=str(tree["tree_sha256"]),
        producer_code_identity=_PRODUCER_CODE,
        producer_runtime_identity=_PRODUCER_RUNTIME,
        config_identity=_CONFIG_IDENTITY,
        producer_stdout_size_bytes=producer_log.stat().st_size,
        workload_profile=_PROFILE,
        workload_seed=7,
        generator_version=_GENERATOR,
        baseline_byte_witness_sha256=_sha256(baseline_bytes),
        baseline_byte_witness_size_bytes=len(baseline_bytes),
        acquired_verifier_baseline_identity=baseline_identity,
        acquired_verifier_file_count=138,
        baseline_commit=_BASELINE_COMMIT,
        producer_dependency_closure_sha256=_CLOSURE_SHA256,
        producer_dependency_closure_file_count=104,
        producer_dependency_entrypoints=_ENTRYPOINTS,
    )
    config = Phase1CSuccessorConfig(
        repository_root=repository_root,
        baseline_byte_witness_path=baseline_path,
        source_mission_root=source_root,
        capacity_candidate_root=candidate_root,
        boundary_certificate_root=boundary_root,
        producer_stdout_log=producer_log,
        producer_stdout_sha256=_sha256(producer_log.read_bytes()),
        run06_candidate_root=phase1c_root / "native-capacity-06",
        receipt_root=receipt_root,
        expectations=expectations,
    )
    return _Fixture(
        repository_root=repository_root,
        source_root=source_root,
        candidate_root=candidate_root,
        boundary_root=boundary_root,
        producer_log=producer_log,
        run06_root=config.run06_candidate_root,
        receipt_root=receipt_root,
        config=config,
    )


def _gate_witnesses(
    tmp_path: Path,
    repository_root: Path,
) -> tuple[Phase1CSuccessorTestWitness, Phase1CSuccessorClosureWitness]:
    gates = (tmp_path / "gates").resolve()
    logs = gates / "logs"
    targeted_basetemp = gates / "pytest" / "targeted"
    global_basetemp = gates / "pytest" / "global"
    logs.mkdir(parents=True)
    targeted_basetemp.mkdir(parents=True)
    global_basetemp.mkdir()
    targeted_log = logs / SUCCESSOR_TARGETED_LOG_NAME
    targeted_log.write_bytes(b"targeted passed\n")
    source_files = tuple(
        (
            relative,
            _sha256((repository_root / relative).read_bytes()),
        )
        for relative in SUCCESSOR_TARGETED_TEST_PATHS
    )
    targeted = Phase1CSuccessorTestWitness(
        command=(
            sys.executable,
            "-m",
            "pytest",
            *SUCCESSOR_TARGETED_TEST_PATHS,
            "-p",
            "no:cacheprovider",
            "--basetemp",
            str(targeted_basetemp),
        ),
        exit_code=0,
        output_sha256=_sha256(targeted_log.read_bytes()),
        source_files=source_files,
        summary="targeted passed",
        output_log_path=str(targeted_log),
        output_log_size_bytes=targeted_log.stat().st_size,
    )
    closure_commands = (
        (
            sys.executable,
            "scripts/generate_phase12_live_paper_artifacts.py",
            "--check",
        ),
        (
            sys.executable,
            "scripts/generate_phase05_paper_evidence.py",
            "--check",
        ),
        (sys.executable, "-m", "ruff", "check", "."),
        (sys.executable, "-m", "mypy", "src/hyperlab"),
        (
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "--basetemp",
            str(global_basetemp),
        ),
        ("git", "-c", "core.whitespace=cr-at-eol", "diff", "--check"),
    )
    commands: list[Phase1CSuccessorCommandWitness] = []
    for purpose, command, log_name in zip(
        _CLOSURE_PURPOSES,
        closure_commands,
        _CLOSURE_LOG_NAMES,
        strict=True,
    ):
        path = logs / log_name
        path.write_bytes(f"{purpose} passed\n".encode())
        commands.append(
            Phase1CSuccessorCommandWitness(
                purpose=purpose,
                command=command,
                exit_code=0,
                output_sha256=_sha256(path.read_bytes()),
                summary=f"{purpose} passed",
                output_log_path=str(path),
                output_log_size_bytes=path.stat().st_size,
            )
        )
    v9_path = repository_root / SUCCESSOR_V9_RELATIVE_PATH
    v9_sha256 = _sha256(v9_path.read_bytes())
    assert v9_path.stat().st_size == SUCCESSOR_V9_SIZE_BYTES
    assert v9_sha256 == SUCCESSOR_V9_SHA256
    closure = Phase1CSuccessorClosureWitness(
        commands=tuple(commands),
        v9=Phase1CSuccessorV9ByteWitness(
            path=SUCCESSOR_V9_RELATIVE_PATH,
            size_bytes=v9_path.stat().st_size,
            before_sha256=v9_sha256,
            after_sha256=v9_sha256,
        ),
    )
    return targeted, closure


def test_gate_witness_contract_refuses_arbitrary_sources_and_commands(
    tmp_path: Path,
) -> None:
    repository_root = Path(__file__).resolve().parents[2]
    targeted, closure = _gate_witnesses(tmp_path, repository_root)

    with pytest.raises(Phase1CSuccessorError, match="canonical list"):
        replace(targeted, source_files=(("tests/storage_v4/test_phase1c_successor.py", "0" * 64),))

    changed = replace(closure.commands[0], command=(sys.executable, "-c", "pass"))
    with pytest.raises(Phase1CSuccessorError, match="canonical commands"):
        replace(closure, commands=(changed, *closure.commands[1:]))


def test_reattest_publishes_receipt_only_with_exact_double_identity_and_accounting(
    tmp_path: Path,
) -> None:
    fixture = _build_fixture(tmp_path)

    receipt = reattest_phase1c_successor(fixture.config)

    assert {item.name for item in receipt.root.iterdir()} == {SUCCESSOR_RECEIPT_NAME}
    assert not (receipt.root / SUCCESSOR_COMPLETE_NAME).exists()
    payload = json.loads(receipt.path.read_text(encoding="utf-8"))["payload"]
    assert payload["producer_identity"] == {
        "code_identity": _PRODUCER_CODE,
        "runtime_identity": _PRODUCER_RUNTIME,
    }
    assert payload["acquired_verifier_baseline"][
        "acquired_verifier_global_identity"
    ]["sha256"] == fixture.config.expectations.acquired_verifier_baseline_identity
    assert payload["current_final_verifier_identity"][
        "successor_verifier_code_identity"
    ]["sha256"] != fixture.config.expectations.acquired_verifier_baseline_identity
    assert payload["producer_dependency_closure"]["closure_sha256"] == (
        _CLOSURE_SHA256
    )
    assert payload["work_accounting"] == {
        "candidate_05_unchanged": True,
        "commits_ingested_during_succession": 0,
        "prefix_reingested": 0,
        "run06_commits": 0,
    }
    assert payload["run06_absence_witness"]["run06_candidate_absent"] is True
    assert payload["terminal_tree"]["tree_sha256"] == (
        fixture.config.expectations.terminal_tree_sha256
    )


def test_finalize_consumes_receipt_without_reopening_source_and_preserves_verdict(
    tmp_path: Path,
) -> None:
    fixture = _build_fixture(tmp_path)
    receipt = reattest_phase1c_successor(fixture.config)
    targeted, closure = _gate_witnesses(tmp_path, fixture.repository_root)
    fixture.source_root.rename(tmp_path / "source-moved-after-receipt")

    publication = finalize_phase1c_successor_closure(
        repository_root=fixture.repository_root,
        receipt_root=receipt.root,
        expected_receipt_sha256=receipt.sha256,
        targeted_tests=targeted,
        closure=closure,
    )

    assert publication.verdict == PHASE1C_TARGET_NOT_MET_VERDICT
    for relative in (
        "successor-report.json",
        "manifest.json",
        "pin/certification.pin.json",
        "COMPLETE",
    ):
        artifact = json.loads((receipt.root / relative).read_text(encoding="utf-8"))
        assert artifact["markers"] == list(SUCCESSOR_MARKERS)
    complete = json.loads((receipt.root / "COMPLETE").read_text(encoding="utf-8"))
    assert complete["artifact"] == SUCCESSOR_COMPLETE_FORMAT
    assert complete["status"] == PHASE1C_TARGET_NOT_MET_VERDICT
    assert complete["payload"]["terminal_tree_sha256"] == (
        fixture.config.expectations.terminal_tree_sha256
    )


def test_reattest_rejects_wrong_old_producer_identity(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    expectations = replace(fixture.config.expectations, producer_code_identity="f" * 64)

    with pytest.raises(Phase1CSuccessorError, match="producer authority differs"):
        reattest_phase1c_successor(replace(fixture.config, expectations=expectations))


def test_reattest_rejects_wrong_acquired_verifier_identity(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    expectations = replace(
        fixture.config.expectations,
        acquired_verifier_baseline_identity="f" * 64,
    )

    with pytest.raises(Phase1CSuccessorError, match="acquired verifier identity"):
        reattest_phase1c_successor(replace(fixture.config, expectations=expectations))


def test_expectations_reject_sha256_in_baseline_git_commit(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)

    with pytest.raises(
        Phase1CSuccessorError,
        match="baseline commit must be a lowercase 40-hex Git commit",
    ):
        replace(fixture.config.expectations, baseline_commit="f" * 64)


def test_reattest_rejects_producer_dependency_closure_drift(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    expectations = replace(
        fixture.config.expectations,
        producer_dependency_closure_sha256="f" * 64,
    )

    with pytest.raises(Phase1CSuccessorError, match="acquired verifier identity/closure"):
        reattest_phase1c_successor(replace(fixture.config, expectations=expectations))


def test_reattest_rejects_missing_terminal_full_audit_proof(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    terminal_path = sorted(fixture.boundary_root.iterdir())[-1]
    payload = json.loads(terminal_path.read_text(encoding="utf-8"))
    del payload["measurement"]["full_history_audit_ns"]
    data = canonical_json_bytes(payload)
    terminal_path.write_bytes(data)
    expectations = replace(
        fixture.config.expectations, terminal_certificate_sha256=_sha256(data)
    )

    with pytest.raises(Phase1CSuccessorError, match="measurement fields differ"):
        reattest_phase1c_successor(replace(fixture.config, expectations=expectations))


def test_reattest_rejects_candidate_byte_drift(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    (fixture.candidate_root / "segment.bin").write_bytes(b"changed")

    with pytest.raises(Phase1CSuccessorError, match="current candidate bytes differ"):
        reattest_phase1c_successor(fixture.config)


def test_reattest_rejects_mission_namespace_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _build_fixture(tmp_path)
    original = successor_module.witness_candidate_tree

    def _mutating_witness(root: Path, **kwargs: object) -> object:
        result = original(root, **kwargs)
        (fixture.source_root / "unexpected-after-tree.txt").write_text("drift")
        return result

    monkeypatch.setattr(successor_module, "witness_candidate_tree", _mutating_witness)

    with pytest.raises(Phase1CSuccessorError, match="mission changed"):
        reattest_phase1c_successor(fixture.config)


def test_reattest_rejects_present_run06_candidate(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    fixture.run06_root.mkdir()

    with pytest.raises(Phase1CSuccessorError, match="run06 candidate exists"):
        reattest_phase1c_successor(fixture.config)


def test_finalize_rejects_gate_log_drift_without_complete(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    receipt = reattest_phase1c_successor(fixture.config)
    targeted, closure = _gate_witnesses(tmp_path, fixture.repository_root)
    Path(closure.commands[0].output_log_path or "").write_bytes(b"changed\n")

    with pytest.raises(Phase1CSuccessorError, match="output log differs"):
        finalize_phase1c_successor_closure(
            repository_root=fixture.repository_root,
            receipt_root=receipt.root,
            expected_receipt_sha256=receipt.sha256,
            targeted_tests=targeted,
            closure=closure,
        )

    assert not (receipt.root / SUCCESSOR_COMPLETE_NAME).exists()


def test_finalize_rejects_pin_deletion_before_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _build_fixture(tmp_path)
    receipt = reattest_phase1c_successor(fixture.config)
    targeted, closure = _gate_witnesses(tmp_path, fixture.repository_root)
    original_load_receipt = successor_module._load_receipt
    deleted = False

    def _delete_pin_after_second_receipt_read(
        root: Path,
        expected_sha256: str,
    ) -> tuple[dict[str, object], bytes]:
        nonlocal deleted
        result = original_load_receipt(root, expected_sha256)
        pin = root / "pin/certification.pin.json"
        if pin.is_file() and not deleted:
            pin.unlink()
            deleted = True
        return result

    monkeypatch.setattr(
        successor_module,
        "_load_receipt",
        _delete_pin_after_second_receipt_read,
    )

    with pytest.raises(Phase1CSuccessorError, match="pin directory is ambiguous"):
        finalize_phase1c_successor_closure(
            repository_root=fixture.repository_root,
            receipt_root=receipt.root,
            expected_receipt_sha256=receipt.sha256,
            targeted_tests=targeted,
            closure=closure,
        )

    assert deleted is True
    assert not (receipt.root / SUCCESSOR_COMPLETE_NAME).exists()


def test_finalize_rejects_report_mutation_before_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _build_fixture(tmp_path)
    receipt = reattest_phase1c_successor(fixture.config)
    targeted, closure = _gate_witnesses(tmp_path, fixture.repository_root)
    original_load_receipt = successor_module._load_receipt
    mutated = False

    def _mutate_report_after_second_receipt_read(
        root: Path,
        expected_sha256: str,
    ) -> tuple[dict[str, object], bytes]:
        nonlocal mutated
        result = original_load_receipt(root, expected_sha256)
        report = root / "successor-report.json"
        if report.is_file() and not mutated:
            payload = json.loads(report.read_text(encoding="utf-8"))
            payload["status"] = "TAMPERED_BEFORE_COMPLETE"
            report.write_bytes(canonical_json_bytes(payload))
            mutated = True
        return result

    monkeypatch.setattr(
        successor_module,
        "_load_receipt",
        _mutate_report_after_second_receipt_read,
    )

    with pytest.raises(
        Phase1CSuccessorError,
        match="successor report differs before COMPLETE publication",
    ):
        finalize_phase1c_successor_closure(
            repository_root=fixture.repository_root,
            receipt_root=receipt.root,
            expected_receipt_sha256=receipt.sha256,
            targeted_tests=targeted,
            closure=closure,
        )

    assert mutated is True
    assert not (receipt.root / SUCCESSOR_COMPLETE_NAME).exists()


def test_finalize_rejects_successor_script_identity_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _build_fixture(tmp_path)
    receipt = reattest_phase1c_successor(fixture.config)
    targeted, closure = _gate_witnesses(tmp_path, fixture.repository_root)
    script = fixture.repository_root / "scripts/certify_storage_v4_phase1c_successor.py"
    original_read = successor_module._read_stable_regular_file

    def _drifted_read(path: Path, *, label: str, maximum_bytes: int) -> bytes:
        data = original_read(path, label=label, maximum_bytes=maximum_bytes)
        return data + b"# drift\n" if path == script else data

    monkeypatch.setattr(successor_module, "_read_stable_regular_file", _drifted_read)

    with pytest.raises(Phase1CSuccessorError, match="verifier identity changed"):
        finalize_phase1c_successor_closure(
            repository_root=fixture.repository_root,
            receipt_root=receipt.root,
            expected_receipt_sha256=receipt.sha256,
            targeted_tests=targeted,
            closure=closure,
        )


def test_successor_verifier_identity_includes_executing_and_capture_scripts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository_root = Path(__file__).resolve().parents[2]
    first = compute_phase1c_successor_verifier_state(repository_root)
    paths = {path for path, _, _ in first.successor_code_identity.files}
    assert "scripts/certify_storage_v4_phase1c_successor.py" in paths
    assert "scripts/capture_phase1c_successor_baseline.py" in paths
    assert "scripts/generate_phase05_paper_evidence.py" in paths
    original_read = successor_module._read_stable_regular_file
    script = repository_root / "scripts/certify_storage_v4_phase1c_successor.py"

    def _drifted_read(path: Path, *, label: str, maximum_bytes: int) -> bytes:
        data = original_read(path, label=label, maximum_bytes=maximum_bytes)
        return data + b"# changed\n" if path == script else data

    monkeypatch.setattr(successor_module, "_read_stable_regular_file", _drifted_read)
    second = compute_phase1c_successor_verifier_state(repository_root)
    assert second.successor_code_identity.sha256 != (
        first.successor_code_identity.sha256
    )


def test_successor_module_and_cli_are_stdlib_only_and_have_no_relative_imports() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    for path in (
        repository_root / "src/hyperlab/paper/storage_v4/phase1c_successor.py",
        repository_root / "scripts/certify_storage_v4_phase1c_successor.py",
    ):
        tree = ast.parse(path.read_bytes(), filename=str(path))
        imported: list[str] = []
        relative_imports: list[ast.ImportFrom] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    relative_imports.append(node)
                if node.module:
                    imported.append(node.module)
        assert not relative_imports
        assert not any(
            name.startswith("hyperlab.paper.storage_v4") for name in imported
        )
        assert not any(
            token in name
            for name in imported
            for token in (
                "capacity_runner",
                "ingestion",
                "phase1c_workers",
                "phase1c_workloads",
                "phase1c_pipeline",
                "golden_runner",
                "writer",
            )
        )


def test_finalize_rejects_receipt_tamper_without_source_candidate(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    receipt = reattest_phase1c_successor(fixture.config)
    targeted, closure = _gate_witnesses(tmp_path, fixture.repository_root)
    fixture.source_root.rename(tmp_path / "source-moved-after-receipt")
    receipt.path.write_bytes(receipt.path.read_bytes() + b"\n")

    with pytest.raises(Phase1CSuccessorError, match="receipt SHA-256 differs"):
        finalize_phase1c_successor_closure(
            repository_root=fixture.repository_root,
            receipt_root=receipt.root,
            expected_receipt_sha256=receipt.sha256,
            targeted_tests=targeted,
            closure=closure,
        )

    assert not (receipt.root / SUCCESSOR_COMPLETE_NAME).exists()
