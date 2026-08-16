from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from hyperlab.paper.admission import (
    CANONICAL_CANDIDATE_STRATEGY_NAMES,
    AdmissionManifest,
    AdmissionManifestError,
    CandidateSemanticVerification,
    canonical_thresholds_for,
    validate_candidate_semantic_verification,
    verify_admission_manifest,
    verify_admission_manifest_file,
)


def _write_binding(root: Path, relative_path: str, payload: bytes) -> dict[str, str]:
    target = root / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    return {"path": relative_path, "sha256": hashlib.sha256(payload).hexdigest()}


def _manifest_payload(root: Path) -> dict[str, object]:
    bindings = {
        "gate_b": _write_binding(root, "reports/gate-b.json", b'{"passed":false}'),
        "gate_c": _write_binding(root, "reports/gate-c.json", b'{"passed":true}'),
        "data": _write_binding(root, "data/panel.manifest.json", b"data-manifest-v1"),
        "calibration": _write_binding(root, "calibration/execution.json", b"calibration-v1"),
        "strategy": _write_binding(root, "strategy/cash-and-carry.py", b"strategy-v1"),
        "split": _write_binding(root, "research/split-plan.json", b"split-v1"),
        "variants": _write_binding(root, "research/variants.jsonl", b"variant-v1\n"),
        "reveal": _write_binding(root, "research/final-reveal.json", b"reveal-v1"),
        "source": _write_binding(root, "identities/public-source.json", b"source-v1"),
        "cost": _write_binding(root, "identities/cost-schedule.json", b"cost-v1"),
        "config": _write_binding(root, "identities/frozen-config.json", b"config-v1"),
    }
    return {
        "schema_version": 1,
        "candidate_id": "cash_and_carry",
        "gate_thresholds": canonical_thresholds_for("cash_and_carry"),
        "evidence": {
            "gate_b_report": bindings["gate_b"],
            "gate_c_report": bindings["gate_c"],
            "data": [bindings["data"]],
            "calibration": [bindings["calibration"]],
            "strategy": [bindings["strategy"]],
        },
        "research": {
            "split_plan": bindings["split"],
            "variant_registry": bindings["variants"],
            "final_reveal": bindings["reveal"],
        },
        "identities": {
            "market_source": {
                "identity": "hyperliquid-public-normalized-v1",
                "artifact": bindings["source"],
            },
            "cost_schedule": {
                "identity": "hyperliquid-public-conservative-v1",
                "artifact": bindings["cost"],
            },
            "frozen_config": {
                "identity": "cash-and-carry-paper-v1",
                "artifact": bindings["config"],
            },
        },
    }


def _codes(manifest: AdmissionManifest, root: Path) -> set[str]:
    return {item.code for item in verify_admission_manifest(manifest, evidence_root=root).blockers}


def _semantic_fixture(
    root: Path,
) -> tuple[AdmissionManifest, CandidateSemanticVerification, str, str]:
    frozen_config_hash = "c" * 64
    payload = _manifest_payload(root)
    identities = payload["identities"]
    assert isinstance(identities, dict)
    frozen_identity = identities["frozen_config"]
    assert isinstance(frozen_identity, dict)
    frozen_identity["identity"] = frozen_config_hash
    manifest = AdmissionManifest.from_mapping(payload)
    data_hash = manifest.identities.market_source.artifact.sha256
    receipt = CandidateSemanticVerification(
        candidate_id="cash_and_carry",
        strategy_name="cash_and_carry",
        gate_b_recomputed_pass=True,
        gate_c_recomputed_pass=True,
        gate_b_report_sha256=manifest.evidence.gate_b_report.sha256,
        gate_c_report_sha256=manifest.evidence.gate_c_report.sha256,
        frozen_config_hash=frozen_config_hash,
        data_hash=data_hash,
    )
    return manifest, receipt, frozen_config_hash, data_hash


def test_canonical_manifest_recomputes_every_byte_without_trusting_passed_flags(
    tmp_path: Path,
) -> None:
    evidence_root = tmp_path / "evidence"
    manifest = AdmissionManifest.from_mapping(_manifest_payload(evidence_root))
    manifest_path = tmp_path / "admission.json"
    manifest_path.write_bytes(manifest.canonical_json_bytes())

    result = verify_admission_manifest_file(manifest_path, evidence_root=evidence_root)

    assert result.evidence_bound
    assert result.blockers == ()
    assert result.manifest_sha256 == hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    # The gate reports deliberately disagree. Integrity verification does not trust either boolean.
    assert json.loads((evidence_root / "reports/gate-b.json").read_text())["passed"] is False


def test_changed_or_missing_artifacts_return_precise_blockers(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    manifest = AdmissionManifest.from_mapping(_manifest_payload(evidence_root))
    (evidence_root / "calibration/execution.json").write_bytes(b"changed")
    (evidence_root / "strategy/cash-and-carry.py").unlink()

    result = verify_admission_manifest(manifest, evidence_root=evidence_root)

    by_location = {item.location: item.code for item in result.blockers}
    assert by_location["evidence.calibration[0].sha256"] == "ARTIFACT_HASH_MISMATCH"
    assert by_location["evidence.strategy[0].path"] == "ARTIFACT_MISSING"


def test_weakened_or_extra_thresholds_never_match_canonical_policy(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    payload = _manifest_payload(evidence_root)
    thresholds = payload["gate_thresholds"]
    assert isinstance(thresholds, dict)
    gate_b = thresholds["gate_b"]
    assert isinstance(gate_b, dict)
    gate_b["minimum_history_hours"] = 72
    gate_b["caller_override"] = True
    manifest = AdmissionManifest.from_mapping(payload)

    result = verify_admission_manifest(manifest, evidence_root=evidence_root)

    by_location = {item.location: item.code for item in result.blockers}
    assert (
        by_location["gate_thresholds.gate_b.minimum_history_hours"]
        == "CANONICAL_THRESHOLD_MISMATCH"
    )
    assert by_location["gate_thresholds.gate_b.caller_override"] == "UNEXPECTED_THRESHOLD"


@pytest.mark.parametrize("relative_path", ["../outside.json", "/absolute.json", "C:/escape.json"])
def test_non_portable_or_escaping_paths_are_refused(
    tmp_path: Path,
    relative_path: str,
) -> None:
    evidence_root = tmp_path / "evidence"
    payload = _manifest_payload(evidence_root)
    evidence = payload["evidence"]
    assert isinstance(evidence, dict)
    gate_b = evidence["gate_b_report"]
    assert isinstance(gate_b, dict)
    gate_b["path"] = relative_path
    manifest = AdmissionManifest.from_mapping(payload)

    assert "INVALID_ARTIFACT_PATH" in _codes(manifest, evidence_root)


def test_duplicate_binding_and_invalid_hash_shape_are_refused(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    payload = _manifest_payload(evidence_root)
    evidence = payload["evidence"]
    assert isinstance(evidence, dict)
    data = evidence["data"]
    strategy = evidence["strategy"]
    assert isinstance(data, list) and isinstance(strategy, list)
    strategy[0] = dict(data[0])
    calibration = evidence["calibration"]
    assert isinstance(calibration, list) and isinstance(calibration[0], dict)
    calibration[0]["sha256"] = "A" * 64
    manifest = AdmissionManifest.from_mapping(payload)

    codes = _codes(manifest, evidence_root)

    assert "DUPLICATE_ARTIFACT_PATH" in codes
    assert "INVALID_SHA256" in codes


def test_loader_rejects_unknown_shape_duplicate_keys_and_noncanonical_bytes(
    tmp_path: Path,
) -> None:
    evidence_root = tmp_path / "evidence"
    payload = _manifest_payload(evidence_root)
    payload["trusted_passed"] = True
    with pytest.raises(AdmissionManifestError) as unexpected:
        AdmissionManifest.from_mapping(payload)
    assert unexpected.value.code == "INVALID_MANIFEST_SHAPE"

    with pytest.raises(AdmissionManifestError) as duplicate:
        AdmissionManifest.from_json_bytes(b'{"schema_version":1,"schema_version":1}')
    assert duplicate.value.code == "DUPLICATE_JSON_KEY"

    payload.pop("trusted_passed")
    manifest = AdmissionManifest.from_mapping(payload)
    pretty = json.dumps(manifest.to_dict(), indent=2, sort_keys=True).encode()
    manifest_path = tmp_path / "pretty.json"
    manifest_path.write_bytes(pretty)
    result = verify_admission_manifest_file(manifest_path, evidence_root=evidence_root)
    assert result.blockers[0].code == "NON_CANONICAL_MANIFEST"


def test_unknown_candidate_empty_artifact_sets_and_unstable_identities_block(
    tmp_path: Path,
) -> None:
    evidence_root = tmp_path / "evidence"
    payload = _manifest_payload(evidence_root)
    payload["candidate_id"] = "not_reviewed"
    evidence = payload["evidence"]
    identities = payload["identities"]
    assert isinstance(evidence, dict) and isinstance(identities, dict)
    evidence["data"] = []
    source = identities["market_source"]
    assert isinstance(source, dict)
    source["identity"] = "not stable"
    manifest = AdmissionManifest.from_mapping(payload)

    codes = _codes(manifest, evidence_root)

    assert "UNKNOWN_CANDIDATE" in codes
    assert "MISSING_REQUIRED_ARTIFACT_SET" in codes
    assert "INVALID_IDENTITY" in codes


def test_canonical_history_thresholds_are_not_cli_overrides() -> None:
    expected = {
        "cash_and_carry": 720,
        "funding_basket": 2160,
        "cross_exchange_funding": 720,
        "pairs_mean_reversion_phase08": 4320,
        "momentum_regime": 8760,
    }
    for candidate_id, minimum in expected.items():
        gate_b = canonical_thresholds_for(candidate_id)["gate_b"]
        assert isinstance(gate_b, dict)
        assert gate_b["minimum_history_hours"] == minimum
    market_making = canonical_thresholds_for("market_making_l2")["gate_b"]
    assert isinstance(market_making, dict)
    assert market_making["minimum_event_count"] == 10_000


def test_all_true_typed_semantic_receipt_remains_non_authorizing(
    tmp_path: Path,
) -> None:
    manifest, receipt, config_hash, data_hash = _semantic_fixture(tmp_path / "evidence")

    blockers = validate_candidate_semantic_verification(
        receipt,
        manifest=manifest,
        strategy_name="cash_and_carry",
        frozen_config_hash=config_hash,
        data_hash=data_hash,
    )

    assert {item.code for item in blockers} == {"SEMANTIC_RECEIPT_NON_AUTHORIZING"}
    assert json.loads((tmp_path / "evidence/reports/gate-b.json").read_text())["passed"] is False


def test_empty_tuple_cannot_stand_in_for_semantic_verification(tmp_path: Path) -> None:
    manifest, _receipt, config_hash, data_hash = _semantic_fixture(tmp_path / "evidence")

    blockers = validate_candidate_semantic_verification(
        (),
        manifest=manifest,
        strategy_name="cash_and_carry",
        frozen_config_hash=config_hash,
        data_hash=data_hash,
    )

    assert [item.code for item in blockers] == ["INVALID_SEMANTIC_RECEIPT"]


def test_gate_b_false_blocks_even_when_report_bytes_are_bound(tmp_path: Path) -> None:
    manifest, receipt, config_hash, data_hash = _semantic_fixture(tmp_path / "evidence")

    blockers = validate_candidate_semantic_verification(
        replace(receipt, gate_b_recomputed_pass=False),
        manifest=manifest,
        strategy_name="cash_and_carry",
        frozen_config_hash=config_hash,
        data_hash=data_hash,
    )

    assert "GATE_B_SEMANTIC_BLOCKED" in {item.code for item in blockers}


def test_candidate_policy_cannot_be_reused_by_a_different_strategy(tmp_path: Path) -> None:
    manifest, receipt, config_hash, data_hash = _semantic_fixture(tmp_path / "evidence")

    blockers = validate_candidate_semantic_verification(
        replace(receipt, strategy_name="momentum_regime"),
        manifest=manifest,
        strategy_name="momentum_regime",
        frozen_config_hash=config_hash,
        data_hash=data_hash,
    )

    assert "CANDIDATE_STRATEGY_MISMATCH" in {item.code for item in blockers}
    assert CANONICAL_CANDIDATE_STRATEGY_NAMES["pairs_mean_reversion_phase08"] == frozenset(
        {"pairs_mean_reversion"}
    )
