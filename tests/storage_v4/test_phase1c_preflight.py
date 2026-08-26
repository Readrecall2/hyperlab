from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from hyperlab.paper.golden_v3 import GOLDEN_STREAM_NAMES, GoldenVerification
from hyperlab.paper.golden_v3_certification import GoldenCertificationVerification
from hyperlab.paper.storage_v4 import phase1c_preflight as preflight_module
from hyperlab.paper.storage_v4.phase1b_certification import (
    PHASE1B_CERTIFICATION_FORMAT,
    PHASE1B_COMPLETE_FORMAT,
    PHASE1B_SUCCESS,
)
from hyperlab.paper.storage_v4.phase1c_preflight import (
    PHASE1C_POSTFLIGHT_STATUS,
    PHASE1C_PREFLIGHT_STATUS,
    PHASE1C_TARGET_PHRASE,
    Phase1BProofExpectations,
    Phase1CGoldenExpectations,
    Phase1CPreflightConfig,
    Phase1CPreflightError,
    run_phase1c_preflight,
    verify_phase1c_postflight,
)

_CERTIFICATION_ROOT = "1" * 64
_GOLDEN_ROOT = "2" * 64
_SOURCE = "3" * 64
_RUN = "4" * 64
_MANIFEST_ROOT = "5" * 64
_FINAL_PREFIX = "6" * 64


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


@dataclass(frozen=True)
class _Synthetic:
    config: Phase1CPreflightConfig
    golden: GoldenVerification
    certification: GoldenCertificationVerification
    calls: list[str]


def _synthetic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Synthetic:
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    certification_root = tmp_path / "golden-certification"
    export_root = certification_root / "corpus" / "extract-a"
    export_root.mkdir(parents=True)
    tiny_payload = b"{\"synthetic\":true}\n"
    (export_root / "tiny-shard.jsonl").write_bytes(tiny_payload)
    (export_root / "manifest.json").write_bytes(b"{\"control\":\"manifest\"}\n")
    (export_root / "COMPLETE").write_bytes(b"{\"control\":\"complete\"}\n")
    pin_path = certification_root / "pin" / "extract-a.pin.json"
    pin_path.parent.mkdir()
    pin_path.write_bytes(_canonical({"root_hash": _GOLDEN_ROOT, "run_id": _RUN}))
    pin_sha256 = hashlib.sha256(pin_path.read_bytes()).hexdigest()

    streams = {
        name: {
            "row_count": 1 if index < 2 else 0,
            "shards": ([{"physical_size": len(tiny_payload)}] if index == 0 else []),
        }
        for index, name in enumerate(GOLDEN_STREAM_NAMES)
    }
    census = {"alert_code_counts": {"MARKET_GAP": 1}, "commit_count": 2}
    golden_manifest: dict[str, Any] = {
        "census": census,
        "root_hash": _GOLDEN_ROOT,
        "run_id": _RUN,
        "source": {"sha256": _SOURCE, "stat": {"size": 1234}},
        "streams": streams,
    }
    golden = GoldenVerification(export_root.resolve(), _GOLDEN_ROOT, golden_manifest)
    certification_manifest: dict[str, object] = {
        "artifacts": {"pin/extract-a.pin.json": {"sha256": pin_sha256}},
        "census": census,
        "exports": {
            "a": {
                "path": "corpus/extract-a",
                "pin": "pin/extract-a.pin.json",
                "root_hash": _GOLDEN_ROOT,
            }
        },
        "run_id": _RUN,
        "source": {
            "after": {"bytes": 1234, "sha256": _SOURCE},
            "before": {"bytes": 1234, "sha256": _SOURCE},
        },
    }
    certification = GoldenCertificationVerification(
        candidate_root=certification_root.resolve(),
        certification_root_hash=_CERTIFICATION_ROOT,
        manifest=certification_manifest,
        tested={},
    )
    (certification_root / "certification-metadata.json").write_bytes(
        _canonical({"certification_root_hash": _CERTIFICATION_ROOT})
    )

    phase1b_root = tmp_path / "retry-02"
    phase1b_root.mkdir()
    report = {
        "audit": {
            "final_prefix_root": _FINAL_PREFIX,
            "manifest_root": _MANIFEST_ROOT,
        },
        "format": PHASE1B_CERTIFICATION_FORMAT,
        "golden": {
            "pin": {"sha256": pin_sha256},
            "root": _GOLDEN_ROOT,
            "run_id": _RUN,
            "source_sha256": _SOURCE,
        },
        "sizes": {
            "anchor_bytes": 12,
            "v3_compatibility_import_segment_bytes": 34,
            "v3_compatibility_import_storage_v4_store_bytes": 56,
        },
        "status": PHASE1B_SUCCESS,
    }
    report_payload = _canonical(report)
    report_sha256 = hashlib.sha256(report_payload).hexdigest()
    (phase1b_root / "report.json").write_bytes(report_payload)
    complete = {
        "certifier_code_sha256": "7" * 64,
        "certifier_configuration_sha256": "8" * 64,
        "certifier_runtime_environment_sha256": "9" * 64,
        "format": PHASE1B_COMPLETE_FORMAT,
        "golden_pin_sha256": pin_sha256,
        "golden_root": _GOLDEN_ROOT,
        "manifest_root": _MANIFEST_ROOT,
        "report_sha256": report_sha256,
        "status": "COMPLETE",
    }
    (phase1b_root / "COMPLETE").write_bytes(_canonical(complete))

    roadmap = tmp_path / "roadmap.html"
    roadmap.write_text(
        "\n".join(
            (
                "<div><b>&lt;0.20 GiB/h</b><span>Gate Storage v4.</span></div>",
                "<tr><td>Storage v4</td><td>budget &lt;0.2 GiB/h.</td></tr>",
                "<tr><td><strong>Storage v4</strong></td>"
                "<td><code>&lt;0.20 GiB/h</code> avec marge, "
                "differential logique exact.</td></tr>",
                "<p><strong>Gate V4 :</strong> extrapolation "
                "<code>&lt;0.20 GiB/h</code> avec marge.</p>",
            )
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    roadmap_sha256 = hashlib.sha256(roadmap.read_bytes()).hexdigest()
    config = Phase1CPreflightConfig(
        mission_root=allowed / "mission-01",
        allowed_parent=allowed,
        golden_certification_root=certification_root,
        golden_export_root=export_root,
        golden_pin_path=pin_path,
        phase1b_root=phase1b_root,
        roadmap_path=roadmap,
        golden=Phase1CGoldenExpectations(
            certification_root_hash=_CERTIFICATION_ROOT,
            golden_root_hash=_GOLDEN_ROOT,
            source_sha256=_SOURCE,
            run_id=_RUN,
            pin_sha256=pin_sha256,
            commit_count=2,
            row_count=2,
            stream_count=len(GOLDEN_STREAM_NAMES),
            market_gap_count=1,
            source_size_bytes=1234,
            export_physical_bytes=len(tiny_payload),
        ),
        phase1b=Phase1BProofExpectations(
            report_sha256=report_sha256,
            manifest_root=_MANIFEST_ROOT,
            final_prefix_root=_FINAL_PREFIX,
            storage_v4_store_bytes=56,
            anchor_bytes=12,
            compatibility_segment_bytes=34,
        ),
        minimum_free_bytes=1024,
        expected_roadmap_sha256=roadmap_sha256,
        expected_target_line_number=3,
    )
    calls: list[str] = []

    def verify_certification(root: Path | str) -> GoldenCertificationVerification:
        calls.append("certification")
        assert Path(root) == certification_root
        return certification

    def verify_export(
        root: Path | str,
        *,
        pin_path: Path | str | None = None,
    ) -> GoldenVerification:
        calls.append("export")
        assert Path(root) == export_root
        assert Path(pin_path) == pin_path_expected
        return golden

    pin_path_expected = pin_path
    monkeypatch.setattr(preflight_module, "verify_golden_v3_certification", verify_certification)
    monkeypatch.setattr(preflight_module, "verify_golden_v3", verify_export)
    monkeypatch.setattr(
        preflight_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=10_000),
    )
    return _Synthetic(config, golden, certification, calls)


def test_preflight_verifies_canonical_authorities_without_creating_mission_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)

    result = run_phase1c_preflight(synthetic.config)

    assert synthetic.calls == ["certification", "export"]
    assert not synthetic.config.mission_root.exists()
    assert result.golden_verification is synthetic.golden
    assert result.certification_verification is synthetic.certification
    assert result.witness.status == PHASE1C_PREFLIGHT_STATUS
    assert result.witness.mission_root_state == "ABSENT_FRESH"
    assert result.witness.observed_free_bytes == 10_000
    golden_witness = result.witness.external.golden
    assert golden_witness.export_physical_bytes == (
        synthetic.config.golden_export_root / "tiny-shard.jsonl"
    ).stat().st_size
    control_bytes = sum(
        (synthetic.config.golden_export_root / name).stat().st_size
        for name in ("manifest.json", "COMPLETE")
    )
    assert golden_witness.export_tree.total_bytes == (
        golden_witness.export_physical_bytes + control_bytes
    )
    target = result.witness.external.capacity_target
    assert target.phrase == PHASE1C_TARGET_PHRASE
    assert target.line_number == 3
    assert target.threshold_gib_per_hour == "0.20"
    assert target.comparator == "LT"
    assert target.margin_required is True
    assert tuple(line.line_number for line in target.corroborating_lines) == (1, 3, 4)
    assert target.consistent_numeric_target_line_numbers == (1, 2, 3, 4)
    payload = json.loads(result.witness.canonical_bytes())
    assert payload["status"] == PHASE1C_PREFLIGHT_STATUS
    assert result.witness.canonical_bytes().endswith(b"\n")


def test_postflight_reverifies_everything_and_accepts_new_mission_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    baseline = run_phase1c_preflight(synthetic.config).witness
    synthetic.config.mission_root.mkdir()
    (synthetic.config.mission_root / "measurement.json").write_bytes(b"{}\n")

    postflight = verify_phase1c_postflight(synthetic.config, baseline)

    assert synthetic.calls == ["certification", "export", "certification", "export"]
    assert postflight.status == PHASE1C_POSTFLIGHT_STATUS
    assert postflight.unchanged is True
    assert postflight.external == baseline.external
    assert json.loads(postflight.canonical_bytes())["unchanged"] is True


@pytest.mark.parametrize(
    "relative_path",
    ("tiny-shard.jsonl", "manifest.json", "COMPLETE"),
)
def test_postflight_rejects_any_external_tree_byte_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative_path: str,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    baseline = run_phase1c_preflight(synthetic.config).witness
    changed = synthetic.config.golden_export_root / relative_path
    changed.write_bytes(b"{\"synthetic\":\"changed\"}\n")

    with pytest.raises(Phase1CPreflightError, match=r"bytes (changed|differ)"):
        verify_phase1c_postflight(synthetic.config, baseline)


def test_existing_mission_root_is_rejected_before_authority_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    synthetic.config.mission_root.mkdir()

    with pytest.raises(Phase1CPreflightError, match="already exists or is ambiguous"):
        run_phase1c_preflight(synthetic.config)
    assert synthetic.calls == []


def test_insufficient_free_space_is_rejected_before_authority_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    monkeypatch.setattr(
        preflight_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=synthetic.config.minimum_free_bytes - 1),
    )

    with pytest.raises(Phase1CPreflightError, match="less free space"):
        run_phase1c_preflight(synthetic.config)
    assert synthetic.calls == []


def test_certification_failure_prevents_export_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)

    def fail_certification(_root: Path | str) -> GoldenCertificationVerification:
        synthetic.calls.append("certification-failed")
        raise ValueError("synthetic invalid certification")

    monkeypatch.setattr(
        preflight_module,
        "verify_golden_v3_certification",
        fail_certification,
    )
    with pytest.raises(ValueError, match="synthetic invalid certification"):
        run_phase1c_preflight(synthetic.config)
    assert synthetic.calls == ["certification-failed"]


def test_golden_census_mismatch_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    census = synthetic.golden.manifest["census"]
    assert isinstance(census, dict)
    census["commit_count"] = 3

    with pytest.raises(Phase1CPreflightError, match="Golden census differs"):
        run_phase1c_preflight(synthetic.config)


@pytest.mark.parametrize(
    "invalid_physical_size",
    (None, True, "20", 0, -1),
    ids=("missing", "bool", "string", "zero", "negative"),
)
def test_golden_shard_physical_size_is_strictly_validated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid_physical_size: object,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    streams = synthetic.golden.manifest["streams"]
    assert isinstance(streams, dict)
    stream = streams[GOLDEN_STREAM_NAMES[0]]
    assert isinstance(stream, dict)
    shards = stream["shards"]
    assert isinstance(shards, list)
    shard = shards[0]
    assert isinstance(shard, dict)
    if invalid_physical_size is None:
        shard.pop("physical_size")
    else:
        shard["physical_size"] = invalid_physical_size

    with pytest.raises(Phase1CPreflightError, match="must be a positive integer"):
        run_phase1c_preflight(synthetic.config)


@pytest.mark.parametrize(
    "invalid_shards",
    (None, {}, "not-a-list"),
    ids=("missing", "mapping", "string"),
)
def test_golden_shards_list_is_required(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid_shards: object,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    streams = synthetic.golden.manifest["streams"]
    assert isinstance(streams, dict)
    stream = streams[GOLDEN_STREAM_NAMES[0]]
    assert isinstance(stream, dict)
    if invalid_shards is None:
        stream.pop("shards")
    else:
        stream["shards"] = invalid_shards

    with pytest.raises(Phase1CPreflightError, match="shards are absent or malformed"):
        run_phase1c_preflight(synthetic.config)


def test_golden_shard_must_be_an_object(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    streams = synthetic.golden.manifest["streams"]
    assert isinstance(streams, dict)
    stream = streams[GOLDEN_STREAM_NAMES[0]]
    assert isinstance(stream, dict)
    stream["shards"] = [None]

    with pytest.raises(Phase1CPreflightError, match=r"shard 0.*absent or malformed"):
        run_phase1c_preflight(synthetic.config)


def test_golden_payload_physical_byte_mismatch_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    observed_bytes = synthetic.config.golden.export_physical_bytes
    expected_bytes = observed_bytes + 1
    wrong_golden = replace(
        synthetic.config.golden,
        export_physical_bytes=expected_bytes,
    )

    with pytest.raises(
        Phase1CPreflightError,
        match=(
            r"payload physical bytes differ.*"
            rf"observed={observed_bytes}, expected={expected_bytes}"
        ),
    ):
        run_phase1c_preflight(replace(synthetic.config, golden=wrong_golden))


def test_noncanonical_phase1b_report_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    report_path = synthetic.config.phase1b_root / "report.json"
    report = json.loads(report_path.read_bytes())
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    with pytest.raises(Phase1CPreflightError, match="not canonical JSON"):
        run_phase1c_preflight(synthetic.config)


def test_roadmap_sha_and_numeric_target_are_both_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    with pytest.raises(Phase1CPreflightError, match="roadmap SHA-256 differs"):
        run_phase1c_preflight(
            replace(synthetic.config, expected_roadmap_sha256="a" * 64)
        )

    synthetic.config.roadmap_path.write_text(
        synthetic.config.roadmap_path.read_text(encoding="utf-8")
        + "<p>Storage v4 target &lt;0.19 GiB/h.</p>\n",
        encoding="utf-8",
        newline="\n",
    )
    with pytest.raises(Phase1CPreflightError, match=r"inconsistent.*GiB/h targets"):
        run_phase1c_preflight(
            replace(synthetic.config, expected_roadmap_sha256=None)
        )


def test_hardlinked_roadmap_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    try:
        os.link(synthetic.config.roadmap_path, tmp_path / "roadmap-hardlink.html")
    except OSError as error:
        pytest.skip(f"hardlinks unavailable in this test environment: {error}")

    with pytest.raises(Phase1CPreflightError, match="hardlinks are forbidden"):
        run_phase1c_preflight(synthetic.config)


def test_config_rejects_lexical_path_traversal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    traversing = (
        synthetic.config.allowed_parent
        / ".."
        / synthetic.config.allowed_parent.name
        / "mission-02"
    )

    with pytest.raises(ValueError, match="path traversal"):
        replace(synthetic.config, mission_root=traversing)


@pytest.mark.parametrize("leaf", ("candidate:stream", "NUL", "CON.txt", "candidate."))
def test_windows_ambiguous_mission_leaf_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    leaf: str,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)

    with pytest.raises(Phase1CPreflightError, match="no safe leaf name"):
        run_phase1c_preflight(
            replace(
                synthetic.config,
                mission_root=synthetic.config.allowed_parent / leaf,
            )
        )


def test_reparse_allowed_parent_is_rejected_when_symlinks_are_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path, monkeypatch)
    linked_parent = tmp_path / "allowed-link"
    try:
        os.symlink(synthetic.config.allowed_parent, linked_parent, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"directory symlinks unavailable in this test environment: {error}")

    with pytest.raises(Phase1CPreflightError, match="symlink, junction, or reparse"):
        run_phase1c_preflight(
            replace(
                synthetic.config,
                allowed_parent=linked_parent,
                mission_root=linked_parent / "mission-02",
            )
        )
