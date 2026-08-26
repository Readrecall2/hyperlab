from __future__ import annotations

import errno
import hashlib
import json
import os
from pathlib import Path

import pytest

import hyperlab.paper.storage_v4.phase1c_evidence as evidence_module
from hyperlab.paper.storage_v4.phase1c_evidence import (
    LEVEL_COMPLETE_STATUS,
    PHASE1C_EVIDENCE_MARKERS,
    PHASE1C_SYNTHETIC_MARKERS,
    CapacityEvidenceLevel,
    EvidenceAlreadyPublished,
    EvidenceArtifactName,
    EvidenceFaultHook,
    EvidenceFaultPoint,
    EvidenceIncomplete,
    EvidenceIntegrityError,
    EvidenceReportStatus,
    EvidenceSemanticGate,
    Phase1CEvidenceProvenance,
    Phase1CEvidencePublisher,
    Phase1CEvidenceReport,
    Phase1CGateEvidence,
    UnsafeEvidencePath,
)

_VERDICT = "STORAGE_V4_PHASE_1C_NATIVE_CAPACITY_CHARACTERIZED_TARGET_NOT_MET"

_STATUS_BY_NAME = {
    EvidenceArtifactName.WORKLOAD_MANIFEST: EvidenceReportStatus.WORKLOAD_MANIFEST_FROZEN,
    EvidenceArtifactName.NATIVE_LAYOUT_REPORT: EvidenceReportStatus.NATIVE_LAYOUT_VERIFIED,
    EvidenceArtifactName.GOLDEN_NATIVE_REPORT: EvidenceReportStatus.GOLDEN_NATIVE_EXACT,
    EvidenceArtifactName.CAPACITY_100K: EvidenceReportStatus.CAPACITY_LEVEL_VERIFIED,
    EvidenceArtifactName.CAPACITY_500K: EvidenceReportStatus.CAPACITY_LEVEL_VERIFIED,
    EvidenceArtifactName.CAPACITY_1M: EvidenceReportStatus.CAPACITY_LEVEL_VERIFIED,
    EvidenceArtifactName.SCALING_REPORT: EvidenceReportStatus.SCALING_CHARACTERIZED,
    EvidenceArtifactName.INTEGRITY_REPORT: (
        EvidenceReportStatus.INTEGRITY_AND_RECOVERY_VERIFIED
    ),
    EvidenceArtifactName.LIMITATIONS: EvidenceReportStatus.LIMITATIONS_RECORDED,
    EvidenceArtifactName.MEASUREMENTS: EvidenceReportStatus.MEASUREMENTS_VERIFIED,
}
_GATES_BY_NAME = {
    EvidenceArtifactName.WORKLOAD_MANIFEST: (EvidenceSemanticGate.CONFIG_BOUND,),
    EvidenceArtifactName.NATIVE_LAYOUT_REPORT: (
        EvidenceSemanticGate.INTEGRITY_VERIFIED,
    ),
    EvidenceArtifactName.GOLDEN_NATIVE_REPORT: (
        EvidenceSemanticGate.EXACT_LOGICAL_MATCH,
        EvidenceSemanticGate.GOLDEN_SOURCE_UNCHANGED,
        EvidenceSemanticGate.INTEGRITY_VERIFIED,
        EvidenceSemanticGate.STARTUP_BOUNDED,
        EvidenceSemanticGate.TAIL_VERIFIED,
    ),
    EvidenceArtifactName.CAPACITY_100K: (
        EvidenceSemanticGate.EXACT_LOGICAL_MATCH,
        EvidenceSemanticGate.INTEGRITY_VERIFIED,
        EvidenceSemanticGate.MEASUREMENTS_COMPLETE,
        EvidenceSemanticGate.STARTUP_BOUNDED,
        EvidenceSemanticGate.TAIL_VERIFIED,
    ),
    EvidenceArtifactName.CAPACITY_500K: (
        EvidenceSemanticGate.EXACT_LOGICAL_MATCH,
        EvidenceSemanticGate.INTEGRITY_VERIFIED,
        EvidenceSemanticGate.MEASUREMENTS_COMPLETE,
        EvidenceSemanticGate.STARTUP_BOUNDED,
        EvidenceSemanticGate.TAIL_VERIFIED,
    ),
    EvidenceArtifactName.CAPACITY_1M: (
        EvidenceSemanticGate.EXACT_LOGICAL_MATCH,
        EvidenceSemanticGate.INTEGRITY_VERIFIED,
        EvidenceSemanticGate.MEASUREMENTS_COMPLETE,
        EvidenceSemanticGate.STARTUP_BOUNDED,
        EvidenceSemanticGate.TAIL_VERIFIED,
    ),
    EvidenceArtifactName.SCALING_REPORT: (
        EvidenceSemanticGate.MEASUREMENTS_COMPLETE,
        EvidenceSemanticGate.SCALING_CHARACTERIZED,
    ),
    EvidenceArtifactName.INTEGRITY_REPORT: (
        EvidenceSemanticGate.FAULT_RECOVERY_VERIFIED,
        EvidenceSemanticGate.INTEGRITY_VERIFIED,
    ),
    EvidenceArtifactName.LIMITATIONS: (
        EvidenceSemanticGate.LIMITATIONS_RECORDED,
    ),
    EvidenceArtifactName.MEASUREMENTS: (
        EvidenceSemanticGate.MEASUREMENTS_COMPLETE,
    ),
}


def _payload(label: str) -> dict[str, object]:
    return {"label": label, "observed": True}


def _sha256(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _provenance(
    root: Path,
    *,
    candidate_id: str = "candidate-a",
    run_id: str = "run-a",
) -> Phase1CEvidenceProvenance:
    return Phase1CEvidenceProvenance(
        candidate_id=candidate_id,
        candidate_root=os.fspath(Path(os.path.abspath(os.fspath(root)))),
        run_id=run_id,
        raw_store_id="raw-store-a",
        raw_lake_id="raw-lake-a",
        paper_store_id="paper-store-a",
        config_identity=_sha256("config-a"),
        code_identity=_sha256("code-a"),
        runtime_identity=_sha256("runtime-a"),
        golden_source_root_sha256=_sha256("golden-source"),
        golden_pin_sha256=_sha256("golden-pin"),
        golden_certification_root_sha256=_sha256("golden-certification"),
    )


def _publisher(
    root: Path,
    *,
    candidate_id: str = "candidate-a",
    run_id: str = "run-a",
    fault_hook: EvidenceFaultHook | None = None,
) -> Phase1CEvidencePublisher:
    return Phase1CEvidencePublisher(
        root,
        provenance=_provenance(root, candidate_id=candidate_id, run_id=run_id),
        fault_hook=fault_hook,
    )


def _report(
    publisher: Phase1CEvidencePublisher,
    name: EvidenceArtifactName,
    *,
    failed_gate: EvidenceSemanticGate | None = None,
    payload: dict[str, object] | None = None,
) -> Phase1CEvidenceReport:
    gates = tuple(
        Phase1CGateEvidence(
            gate=gate,
            passed=gate is not failed_gate,
            certifier_contract=f"TEST_CERTIFIER_{gate.value.upper()}_V1",
            certifier_result_sha256=_sha256(
                f"{name.value}:{gate.value}:{gate is not failed_gate}"
            ),
        )
        for gate in _GATES_BY_NAME[name]
    )
    status = (
        EvidenceReportStatus.VERIFICATION_FAILED
        if failed_gate is not None
        else _STATUS_BY_NAME[name]
    )
    return publisher.make_report(
        status=status,
        gates=gates,
        payload=_payload(name.value) if payload is None else payload,
    )


def _publish_report(
    publisher: Phase1CEvidencePublisher,
    name: EvidenceArtifactName,
) -> None:
    publisher.publish_json(name, report=_report(publisher, name))


def _publish_complete_candidate(
    publisher: Phase1CEvidencePublisher,
    *,
    failed_measurements_gate: EvidenceSemanticGate | None = None,
) -> None:
    _publish_report(publisher, EvidenceArtifactName.WORKLOAD_MANIFEST)
    _publish_report(publisher, EvidenceArtifactName.NATIVE_LAYOUT_REPORT)
    _publish_report(publisher, EvidenceArtifactName.GOLDEN_NATIVE_REPORT)
    _publish_report(publisher, EvidenceArtifactName.CAPACITY_100K)
    publisher.publish_level_complete(CapacityEvidenceLevel.CAPACITY_100K)
    _publish_report(publisher, EvidenceArtifactName.CAPACITY_500K)
    publisher.publish_level_complete(CapacityEvidenceLevel.CAPACITY_500K)
    _publish_report(publisher, EvidenceArtifactName.CAPACITY_1M)
    publisher.publish_level_complete(CapacityEvidenceLevel.CAPACITY_1M)
    _publish_report(publisher, EvidenceArtifactName.SCALING_REPORT)
    _publish_report(publisher, EvidenceArtifactName.INTEGRITY_REPORT)
    _publish_report(publisher, EvidenceArtifactName.LIMITATIONS)
    publisher.publish_measurements(
        (
            {"commit_count": 100_000, "wall_ns": 1},
            {"commit_count": 500_000, "wall_ns": 2},
            {"commit_count": 1_000_000, "wall_ns": 3},
        ),
        report=_report(
            publisher,
            EvidenceArtifactName.MEASUREMENTS,
            failed_gate=failed_measurements_gate,
        ),
        synthetic=True,
    )


def test_fresh_root_no_overwrite_and_atomic_canonical_publication(tmp_path: Path) -> None:
    root = tmp_path / "candidate"
    publisher = _publisher(root)

    record = publisher.publish_json(
        EvidenceArtifactName.WORKLOAD_MANIFEST,
        report=_report(
            publisher,
            EvidenceArtifactName.WORKLOAD_MANIFEST,
            payload={"commits": 1_000_000, "seed": 17},
        ),
    )

    data = record.path.read_bytes()
    assert data == json.dumps(
        json.loads(data),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert record.sha256 == hashlib.sha256(data).hexdigest()
    assert json.loads(data)["links"] == {}
    assert json.loads(data)["markers"] == list(PHASE1C_SYNTHETIC_MARKERS)
    assert json.loads(data)["provenance"] == publisher.provenance.as_dict()
    assert json.loads(data)["provenance_sha256"] == publisher.provenance.sha256
    assert set(json.loads(data)["gates"]) == {"config_bound"}
    assert not tuple(root.glob(".*.tmp"))

    with pytest.raises(EvidenceAlreadyPublished):
        publisher.publish_json(
            EvidenceArtifactName.WORKLOAD_MANIFEST,
            report=_report(publisher, EvidenceArtifactName.WORKLOAD_MANIFEST),
        )
    with pytest.raises((EvidenceAlreadyPublished, UnsafeEvidencePath)):
        _publisher(root)


def test_racing_target_created_at_exclusive_publish_is_never_overwritten(
    tmp_path: Path,
) -> None:
    root = tmp_path / "candidate"
    target = root / EvidenceArtifactName.WORKLOAD_MANIFEST.value
    racing_bytes = b"racing publication must remain untouched"
    events: list[EvidenceFaultPoint] = []

    def create_racing_target(point: EvidenceFaultPoint, /) -> None:
        events.append(point)
        if point is EvidenceFaultPoint.BEFORE_REPLACE:
            assert not target.exists()
            target.write_bytes(racing_bytes)

    publisher = _publisher(root, fault_hook=create_racing_target)
    with pytest.raises(EvidenceIntegrityError, match="collision bytes differ"):
        publisher.publish_json(
            EvidenceArtifactName.WORKLOAD_MANIFEST,
            report=_report(publisher, EvidenceArtifactName.WORKLOAD_MANIFEST),
        )

    assert target.read_bytes() == racing_bytes
    assert EvidenceFaultPoint.AFTER_REPLACE not in events
    assert publisher.records == ()
    assert not tuple(root.glob(".*.tmp"))


def test_sha256_graph_level_markers_and_terminal_marker_are_exact(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "candidate")
    _publish_complete_candidate(publisher)

    capacity_500k = publisher.verify_artifact("capacity-500k.json")
    envelope = json.loads(capacity_500k.path.read_bytes())
    assert envelope["links"]["capacity-100k.json"] == publisher.verify_artifact(
        "capacity-100k.json"
    ).sha256

    for level in CapacityEvidenceLevel:
        marker = publisher.verify_artifact(f"capacity-{level.value}/COMPLETE")
        marker_payload = json.loads(marker.path.read_bytes())
        assert marker_payload["status"] == LEVEL_COMPLETE_STATUS
        assert marker_payload["markers"] == list(PHASE1C_SYNTHETIC_MARKERS)
        assert marker_payload["payload"] == {
            "level": level.value,
            "verified_report_status": EvidenceReportStatus.CAPACITY_LEVEL_VERIFIED.value,
        }
        assert marker_payload["provenance_sha256"] == publisher.provenance.sha256

    terminal = publisher.publish_terminal_complete(_VERDICT)
    terminal_payload = json.loads(terminal.path.read_bytes())
    assert terminal_payload["status"] == _VERDICT
    assert terminal_payload["markers"] == list(PHASE1C_EVIDENCE_MARKERS)
    assert set(terminal_payload["links"]) == {
        record.name for record in publisher.records if record.name != "COMPLETE"
    }
    assert publisher.verify_all()[-1] == terminal


def test_measurements_are_one_immutable_canonical_jsonl_artifact(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "candidate")
    _publish_complete_candidate(publisher)

    record = publisher.verify_artifact("measurements.jsonl")
    lines = record.path.read_bytes().splitlines()
    assert len(lines) == 3
    for ordinal, line in enumerate(lines, start=1):
        value = json.loads(line)
        assert value["ordinal"] == ordinal
        assert value["status"] == EvidenceReportStatus.MEASUREMENTS_VERIFIED.value
        assert value["markers"] == list(PHASE1C_SYNTHETIC_MARKERS)
        assert value["provenance"] == publisher.provenance.as_dict()
        assert value["links"]["limitations.json"] == publisher.verify_artifact(
            "limitations.json"
        ).sha256

    with pytest.raises(EvidenceAlreadyPublished):
        publisher.publish_measurements(
            ({"commit_count": 2_000_000},),
            report=_report(publisher, EvidenceArtifactName.MEASUREMENTS),
            synthetic=True,
        )


def test_measurement_cannot_shadow_common_run_identity(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "candidate")
    _publish_report(publisher, EvidenceArtifactName.WORKLOAD_MANIFEST)
    _publish_report(publisher, EvidenceArtifactName.NATIVE_LAYOUT_REPORT)
    _publish_report(publisher, EvidenceArtifactName.GOLDEN_NATIVE_REPORT)
    _publish_report(publisher, EvidenceArtifactName.CAPACITY_100K)
    _publish_report(publisher, EvidenceArtifactName.CAPACITY_500K)
    _publish_report(publisher, EvidenceArtifactName.CAPACITY_1M)
    _publish_report(publisher, EvidenceArtifactName.SCALING_REPORT)
    _publish_report(publisher, EvidenceArtifactName.INTEGRITY_REPORT)
    _publish_report(publisher, EvidenceArtifactName.LIMITATIONS)

    with pytest.raises(ValueError, match="measurement shadows"):
        publisher.publish_measurements(
            ({"commit_count": 100_000, "run_id": "different-run"},),
            report=_report(publisher, EvidenceArtifactName.MEASUREMENTS),
            synthetic=True,
        )
    assert not (publisher.output_root / "measurements.jsonl").exists()


def test_incomplete_level_never_gets_complete_marker(tmp_path: Path) -> None:
    root = tmp_path / "candidate"
    publisher = _publisher(root)
    _publish_report(publisher, EvidenceArtifactName.WORKLOAD_MANIFEST)
    _publish_report(publisher, EvidenceArtifactName.NATIVE_LAYOUT_REPORT)

    with pytest.raises(EvidenceIncomplete, match=r"capacity-100k\.json"):
        publisher.publish_level_complete(CapacityEvidenceLevel.CAPACITY_100K)

    assert not (root / "capacity-100k" / "COMPLETE").exists()
    assert not (root / "COMPLETE").exists()


def test_incomplete_candidate_never_gets_terminal_complete(tmp_path: Path) -> None:
    root = tmp_path / "candidate"
    publisher = _publisher(root)
    _publish_report(publisher, EvidenceArtifactName.WORKLOAD_MANIFEST)

    with pytest.raises(EvidenceIncomplete, match="terminal evidence is incomplete"):
        publisher.publish_terminal_complete(_VERDICT)
    with pytest.raises(ValueError, match="not an authorized"):
        publisher.publish_terminal_complete("STORAGE_V4_PHASE_1C_INTEGRITY_BLOCKED")

    assert not (root / "COMPLETE").exists()


def test_cross_candidate_report_and_shadowed_identity_are_rejected(
    tmp_path: Path,
) -> None:
    first = _publisher(
        tmp_path / "candidate-a",
        candidate_id="candidate-a",
        run_id="run-a",
    )
    second = _publisher(
        tmp_path / "candidate-b",
        candidate_id="candidate-b",
        run_id="run-b",
    )
    report_from_first = _report(first, EvidenceArtifactName.WORKLOAD_MANIFEST)

    with pytest.raises(EvidenceIntegrityError, match="report provenance differs"):
        second.publish_json(
            EvidenceArtifactName.WORKLOAD_MANIFEST,
            report=report_from_first,
        )
    assert not (second.output_root / "workload-manifest.json").exists()

    valid_second_report = _report(second, EvidenceArtifactName.WORKLOAD_MANIFEST)
    other_run_provenance = _provenance(
        second.output_root,
        candidate_id="candidate-b",
        run_id="run-a",
    )
    other_run_report = Phase1CEvidenceReport(
        provenance_sha256=other_run_provenance.sha256,
        status=valid_second_report.status,
        gates=valid_second_report.gates,
        payload=valid_second_report.payload,
    )
    with pytest.raises(EvidenceIntegrityError, match="report provenance differs"):
        second.publish_json(
            EvidenceArtifactName.WORKLOAD_MANIFEST,
            report=other_run_report,
        )
    assert not (second.output_root / "workload-manifest.json").exists()

    with pytest.raises(ValueError, match="shadows evidence envelope identity"):
        second.make_report(
            status=EvidenceReportStatus.WORKLOAD_MANIFEST_FROZEN,
            gates=(
                Phase1CGateEvidence(
                    gate=EvidenceSemanticGate.CONFIG_BOUND,
                    passed=True,
                    certifier_contract="TEST_CONFIG_CERTIFIER_V1",
                    certifier_result_sha256=_sha256("config-result"),
                ),
            ),
            payload={"run_id": "run-a"},
        )


def test_dummy_or_failed_level_report_cannot_publish_complete(tmp_path: Path) -> None:
    root = tmp_path / "candidate"
    publisher = _publisher(root)

    with pytest.raises(EvidenceIncomplete, match="semantic gate set differs"):
        publisher.publish_json(
            EvidenceArtifactName.WORKLOAD_MANIFEST,
            report=publisher.make_report(
                status=EvidenceReportStatus.WORKLOAD_MANIFEST_FROZEN,
                gates=(),
                payload={"observed": True},
            ),
        )
    assert not (root / "workload-manifest.json").exists()

    _publish_report(publisher, EvidenceArtifactName.WORKLOAD_MANIFEST)
    _publish_report(publisher, EvidenceArtifactName.NATIVE_LAYOUT_REPORT)
    publisher.publish_json(
        EvidenceArtifactName.CAPACITY_100K,
        report=_report(
            publisher,
            EvidenceArtifactName.CAPACITY_100K,
            failed_gate=EvidenceSemanticGate.EXACT_LOGICAL_MATCH,
        ),
    )

    with pytest.raises(EvidenceIncomplete, match="semantic status failed"):
        publisher.publish_level_complete(CapacityEvidenceLevel.CAPACITY_100K)
    assert not (root / "capacity-100k" / "COMPLETE").exists()
    assert not (root / "COMPLETE").exists()


def test_failed_terminal_gate_never_gets_terminal_complete(tmp_path: Path) -> None:
    root = tmp_path / "candidate"
    publisher = _publisher(root)
    _publish_complete_candidate(
        publisher,
        failed_measurements_gate=EvidenceSemanticGate.MEASUREMENTS_COMPLETE,
    )

    with pytest.raises(EvidenceIncomplete, match=r"measurements\.jsonl"):
        publisher.publish_terminal_complete(_VERDICT)
    assert not (root / "COMPLETE").exists()


def test_tamper_blocks_level_and_terminal_completion(tmp_path: Path) -> None:
    root = tmp_path / "candidate"
    publisher = _publisher(root)
    _publish_report(publisher, EvidenceArtifactName.WORKLOAD_MANIFEST)
    _publish_report(publisher, EvidenceArtifactName.NATIVE_LAYOUT_REPORT)
    _publish_report(publisher, EvidenceArtifactName.CAPACITY_100K)
    capacity = root / "capacity-100k.json"
    capacity.write_bytes(capacity.read_bytes() + b" ")

    with pytest.raises(EvidenceIntegrityError, match="SHA-256 differs"):
        publisher.publish_level_complete(CapacityEvidenceLevel.CAPACITY_100K)
    assert not (root / "capacity-100k" / "COMPLETE").exists()

    second = _publisher(tmp_path / "complete-candidate")
    _publish_complete_candidate(second)
    limitations = second.output_root / "limitations.json"
    limitations.write_bytes(limitations.read_bytes() + b"\n")
    with pytest.raises(EvidenceIntegrityError, match="SHA-256 differs"):
        second.publish_terminal_complete(_VERDICT)
    assert not (second.output_root / "COMPLETE").exists()


class _FailAt:
    def __init__(self, point: EvidenceFaultPoint) -> None:
        self.point = point
        self.events: list[EvidenceFaultPoint] = []
        self.armed = True

    def __call__(self, point: EvidenceFaultPoint, /) -> None:
        self.events.append(point)
        if self.armed and point is self.point:
            raise RuntimeError(f"fault at {point.value}")


def test_fault_before_level_complete_preserves_publication_order(tmp_path: Path) -> None:
    hook = _FailAt(EvidenceFaultPoint.BEFORE_LEVEL_COMPLETE)
    publisher = _publisher(tmp_path / "candidate", fault_hook=hook)
    _publish_report(publisher, EvidenceArtifactName.WORKLOAD_MANIFEST)
    _publish_report(publisher, EvidenceArtifactName.NATIVE_LAYOUT_REPORT)
    _publish_report(publisher, EvidenceArtifactName.CAPACITY_100K)

    with pytest.raises(RuntimeError, match="before_level_complete"):
        publisher.publish_level_complete(CapacityEvidenceLevel.CAPACITY_100K)

    before_verify = hook.events.index(EvidenceFaultPoint.BEFORE_REQUIRED_VERIFICATION)
    after_verify = hook.events.index(
        EvidenceFaultPoint.AFTER_REQUIRED_VERIFICATION,
        before_verify,
    )
    before_complete = hook.events.index(
        EvidenceFaultPoint.BEFORE_LEVEL_COMPLETE,
        after_verify,
    )
    assert before_verify < after_verify < before_complete
    assert not (publisher.output_root / "capacity-100k" / "COMPLETE").exists()


def test_fault_before_terminal_complete_leaves_verified_tree_incomplete(
    tmp_path: Path,
) -> None:
    hook = _FailAt(EvidenceFaultPoint.BEFORE_TERMINAL_COMPLETE)
    publisher = _publisher(tmp_path / "candidate", fault_hook=hook)
    _publish_complete_candidate(publisher)

    with pytest.raises(RuntimeError, match="before_terminal_complete"):
        publisher.publish_terminal_complete(_VERDICT)

    assert hook.events[-3:] == [
        EvidenceFaultPoint.BEFORE_REQUIRED_VERIFICATION,
        EvidenceFaultPoint.AFTER_REQUIRED_VERIFICATION,
        EvidenceFaultPoint.BEFORE_TERMINAL_COMPLETE,
    ]
    assert not (publisher.output_root / "COMPLETE").exists()


def test_unexpected_sidecar_blocks_terminal_complete(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "candidate")
    _publish_complete_candidate(publisher)
    (publisher.output_root / "progress.json").write_text("{}", encoding="utf-8")

    with pytest.raises(EvidenceIncomplete, match="root file set differs"):
        publisher.publish_terminal_complete(_VERDICT)
    assert not (publisher.output_root / "COMPLETE").exists()


def test_non_regular_artifact_and_symlink_parent_are_rejected(tmp_path: Path) -> None:
    publisher = _publisher(tmp_path / "candidate")
    (publisher.output_root / "workload-manifest.json").mkdir()
    with pytest.raises(UnsafeEvidencePath, match="not a regular file"):
        _publish_report(publisher, EvidenceArtifactName.WORKLOAD_MANIFEST)

    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked-parent"
    try:
        linked_parent.symlink_to(real_parent, target_is_directory=True)
    except OSError:
        pytest.skip("local policy does not permit an unprivileged directory symlink")
    with pytest.raises(UnsafeEvidencePath, match="symbolic link or reparse"):
        _publisher(linked_parent / "candidate")


def test_simulated_windows_reparse_artifact_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher = _publisher(tmp_path / "candidate")
    target = publisher.output_root / "workload-manifest.json"
    target.write_bytes(b"not evidence")
    target_stat = target.lstat()
    target_identity = (target_stat.st_dev, target_stat.st_ino)
    original = evidence_module._is_link_or_reparse

    def simulated_reparse(path_stat: os.stat_result) -> bool:
        identity = (
            int(path_stat.st_dev),
            int(path_stat.st_ino),
        )
        return identity == target_identity or original(path_stat)

    monkeypatch.setattr(evidence_module, "_is_link_or_reparse", simulated_reparse)
    with pytest.raises(UnsafeEvidencePath, match="symbolic link or reparse"):
        _publish_report(publisher, EvidenceArtifactName.WORKLOAD_MANIFEST)


def test_directory_fsync_unsupported_is_explicit_not_fabricated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unsupported(_path: Path) -> None:
        raise OSError(errno.ENOTSUP, "directory fsync unsupported")

    monkeypatch.setattr(evidence_module, "fsync_directory", unsupported)
    publisher = _publisher(tmp_path / "candidate")
    record = publisher.publish_json(
        EvidenceArtifactName.WORKLOAD_MANIFEST,
        report=_report(
            publisher,
            EvidenceArtifactName.WORKLOAD_MANIFEST,
            payload={"measured": True},
        ),
    )

    assert not publisher.directory_fsync_supported
    assert not record.directory_fsync_supported
    assert record.path.is_file()
