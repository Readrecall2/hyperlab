from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from pathlib import Path

import pytest
from pytest import MonkeyPatch
from test_paper_golden_v3 import _build_source

import hyperlab.paper.golden_v3 as golden_v3
import hyperlab.paper.golden_v3_certification as certification
from hyperlab.paper.golden_v3_certification import (
    GoldenCertificationError,
    certify_golden_v3,
    verify_golden_v3_certification,
)


def _readonly_replayable_source(tmp_path: Path) -> tuple[Path, str, int, str]:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source, include_unlinked_alert=False)
    expected_size = source.stat().st_size
    expected_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    source.chmod(stat.S_IREAD)
    return source, run_id, expected_size, expected_sha256


def test_canonical_publication_fsyncs_parent_directory(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    observed: list[Path] = []
    monkeypatch.setattr(certification, "_fsync_directory", observed.append)
    artifact = tmp_path / "publication.json"

    certification._write_new_canonical_json(artifact, {"status": "EXACT"})

    assert artifact.read_bytes() == b'{"status":"EXACT"}\n'
    assert observed == [tmp_path]


def test_certification_candidate_chain_and_subdirs_fail_closed_on_fsync(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source, include_unlinked_alert=False)
    expected_size = source.stat().st_size
    expected_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    outer = tmp_path / "nested-root"
    candidate = outer / "candidate"
    source.chmod(stat.S_IREAD)
    parent_syncs: list[Path] = []
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)
    monkeypatch.setattr(golden_v3, "_fsync_directory", parent_syncs.append)

    def fail_candidate(parent: Path) -> None:
        assert parent == candidate
        assert {entry.name for entry in candidate.iterdir()} == {
            "corpus",
            "manifests",
            "pin",
            "results",
            "scratch",
        }
        raise OSError("synthetic candidate subdirs fsync failure")

    monkeypatch.setattr(certification, "_fsync_directory", fail_candidate)
    try:
        with pytest.raises(OSError, match="candidate subdirs fsync"):
            certify_golden_v3(
                source,
                candidate,
                run_id,
                sentinel_path=tmp_path / "forbidden-original.sqlite3",
                expected_source_size=expected_size,
                expected_source_sha256=expected_sha256,
                shard_rows=1_000,
                shard_bytes=1_000_000,
            )
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)

    assert parent_syncs == [tmp_path, outer]
    assert not (candidate / "corpus" / "extract-a").exists()
    assert not (candidate / "COMPLETE").exists()


def test_quarantine_uses_unique_destinations_and_preserves_existing_partial(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    manifest = tmp_path / "certification-manifest.json"
    pin = tmp_path / "certification.pin.json"
    complete = tmp_path / "COMPLETE"
    terminal_paths = (complete, pin, manifest)
    expected = {}
    for ordinal, path in enumerate(terminal_paths):
        payload = f"terminal-{ordinal}".encode()
        path.write_bytes(payload)
        expected[path.name] = payload
        path.with_name(f"{path.name}.partial").write_bytes(b"older partial")
    monkeypatch.setattr(certification, "_fsync_directory", lambda _path: None)

    certification._quarantine_final_markers(manifest, pin, complete)

    for path in terminal_paths:
        assert not path.exists()
        assert path.with_name(f"{path.name}.partial").read_bytes() == b"older partial"
        quarantined = list(tmp_path.glob(f"{path.name}.partial.*"))
        assert len(quarantined) == 1
        assert quarantined[0].read_bytes() == expected[path.name]


@pytest.mark.parametrize("failure", ["replace", "fsync"])
def test_quarantine_propagates_replace_and_fsync_failures(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    failure: str,
) -> None:
    manifest = tmp_path / "certification-manifest.json"
    pin = tmp_path / "certification.pin.json"
    complete = tmp_path / "COMPLETE"
    for path in (complete, pin, manifest):
        path.write_bytes(path.name.encode())

    if failure == "replace":
        real_replace = os.replace

        def fail_complete_replace(source: Path, target: Path) -> None:
            if source == complete:
                raise OSError("synthetic quarantine replace failure")
            real_replace(source, target)

        monkeypatch.setattr(certification.os, "replace", fail_complete_replace)
        monkeypatch.setattr(certification, "_fsync_directory", lambda _path: None)
    else:
        monkeypatch.setattr(
            certification,
            "_fsync_directory",
            lambda _path: (_ for _ in ()).throw(
                OSError("synthetic quarantine fsync failure")
            ),
        )

    with pytest.raises(OSError, match=failure):
        certification._quarantine_final_markers(manifest, pin, complete)

    if failure == "fsync":
        assert not any(path.exists() for path in (complete, pin, manifest))
    else:
        assert complete.exists()
        assert not pin.exists()
        assert not manifest.exists()


def test_certification_allows_nonblocking_behavioral_coverage_limits(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source, run_id, expected_size, expected_sha256 = _readonly_replayable_source(tmp_path)
    candidate = tmp_path / "candidate"
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)
    try:
        result = certify_golden_v3(
            source,
            candidate,
            run_id,
            sentinel_path=tmp_path / "forbidden-original.sqlite3",
            expected_source_size=expected_size,
            expected_source_sha256=expected_sha256,
            shard_rows=1_000,
            shard_bytes=1_000_000,
        )
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)

    assert result.status == "GOLDEN_V3_CERTIFIED"
    assert result.complete_path.is_file()
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["BLOCKING_INTEGRITY_GATES"] == []
    assert manifest["phase05_decision_coverage"] is False
    assert manifest["phase08_decision_coverage"] is False
    assert manifest["strategy_behavior_complete"] is False
    assert manifest["economic_evidence"] is False
    assert manifest["authorizes_real_money"] is False


def test_certification_manifest_binds_dual_exports_replay_results_and_pin(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source, run_id, expected_size, expected_sha256 = _readonly_replayable_source(tmp_path)
    candidate = tmp_path / "candidate"
    progress_events: list[dict[str, object]] = []
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)
    try:
        result = certify_golden_v3(
            source,
            candidate,
            run_id,
            sentinel_path=tmp_path / "forbidden-original.sqlite3",
            expected_source_size=expected_size,
            expected_source_sha256=expected_sha256,
            progress=lambda event: progress_events.append(dict(event)),
            shard_rows=1_000,
            shard_bytes=1_000_000,
        )
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)

    assert result.status == "GOLDEN_V3_CERTIFIED"
    verified = verify_golden_v3_certification(candidate)
    assert verified.certification_root_hash == result.certification_root_hash
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "GOLDEN_V3_CERTIFIED"
    assert manifest["gates"]["dual_extraction"] == "EXACT"
    assert manifest["gates"]["replay_differential"] == "EXACT"
    assert manifest["coverage_gaps"] == []
    assert manifest["census"]["sqlite_integrity_check"] == "ok"
    assert manifest["run_identity"]["config_hash"]
    assert manifest["exports"]["a"]["root_hash"] == manifest["exports"]["b"]["root_hash"]
    assert manifest["replay"]["status"] == "REPLAY_DIFFERENTIAL_EXACT"
    assert result.pin_path.stat().st_mode & stat.S_IWRITE == 0
    assert result.complete_path.is_file()
    validation_events = [
        event
        for event in progress_events
        if str(event.get("phase", "")).startswith("preserved_target_validation")
    ]
    assert [
        (event.get("phase"), event.get("validation_step"))
        for event in validation_events[:3]
    ] == [
        ("preserved_target_validation", "start"),
        ("preserved_target_validation", "fingerprint"),
        ("preserved_target_validation", "integrity"),
    ]
    assert {
        event.get("stream")
        for event in validation_events
        if event.get("validation_step") == "differential" and "stream" in event
    } == set(golden_v3.GOLDEN_STREAM_NAMES)
    assert validation_events[-2]["validation_step"] == "final_fingerprint"
    assert validation_events[-1]["phase"] == "preserved_target_validation_complete"
    assert validation_events[-1]["validation_step"] == "complete"
    assert all(
        "eta_seconds" not in event
        for event in validation_events
        if event.get("validation_step") in {"fingerprint", "final_fingerprint"}
    )
    final_validation_events = [
        event
        for event in progress_events
        if str(event.get("phase", "")).startswith(
            "final_preserved_target_validation"
        )
    ]
    assert [
        (event.get("phase"), event.get("validation_step"))
        for event in final_validation_events[:3]
    ] == [
        ("final_preserved_target_validation", "start"),
        ("final_preserved_target_validation", "fingerprint"),
        ("final_preserved_target_validation", "integrity"),
    ]
    assert final_validation_events[2]["bytes_completed"] == 0
    assert "eta_seconds" not in final_validation_events[2]
    assert {
        event.get("stream")
        for event in final_validation_events
        if event.get("validation_step") == "differential" and "stream" in event
    } == set(golden_v3.GOLDEN_STREAM_NAMES)
    assert final_validation_events[-2]["validation_step"] == "final_fingerprint"
    assert (
        final_validation_events[-1]["phase"]
        == "final_preserved_target_validation_complete"
    )
    assert final_validation_events[-1]["validation_step"] == "complete"

    pin_alias = candidate / "certification-pin-hardlink.json"
    os.link(result.pin_path, pin_alias)
    try:
        with pytest.raises(GoldenCertificationError, match=r"pin.*hardlink|hardlinks"):
            verify_golden_v3_certification(candidate)
    finally:
        pin_alias.chmod(stat.S_IREAD | stat.S_IWRITE)
        pin_alias.unlink()
        result.pin_path.chmod(stat.S_IREAD)

    manifest_path = candidate / "manifests" / "certification-manifest.json"
    original_reparse_check = certification._has_reparse_component

    def synthetic_manifest_parent_reparse(path: Path) -> bool:
        if certification._lexical_absolute(path) == certification._lexical_absolute(
            manifest_path
        ):
            return True
        return original_reparse_check(path)

    with monkeypatch.context() as reparse_patch:
        reparse_patch.setattr(
            certification,
            "_has_reparse_component",
            synthetic_manifest_parent_reparse,
        )
        with pytest.raises(GoldenCertificationError, match=r"symlink|junction|reparse"):
            verify_golden_v3_certification(candidate)

    replay_target = Path(str(manifest["replay"]["target_path"]))
    with sqlite3.connect(replay_target) as connection:
        connection.execute(
            "UPDATE paper_runs SET event_head_hash=? WHERE run_id=?",
            ("f" * 64, run_id),
        )
    with pytest.raises(GoldenCertificationError, match=r"target|hash|artifact|integrity"):
        verify_golden_v3_certification(candidate)

    replay_result = candidate / "results" / "replay.json"
    replay_result.write_text("{}\n", encoding="utf-8", newline="\n")
    with pytest.raises(GoldenCertificationError, match=r"result|hash|artifact"):
        verify_golden_v3_certification(candidate)


def test_terminal_callback_failure_preserves_partial_without_complete(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source, run_id, expected_size, expected_sha256 = _readonly_replayable_source(tmp_path)
    candidate = tmp_path / "candidate"
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)

    def fail_before_complete(record: object) -> None:
        if (
            isinstance(record, dict)
            and record.get("phase") == "certification_ready_to_publish"
        ):
            raise RuntimeError("synthetic terminal callback interruption")

    try:
        with pytest.raises(GoldenCertificationError, match=r"terminal callback"):
            certify_golden_v3(
                source,
                candidate,
                run_id,
                sentinel_path=tmp_path / "forbidden-original.sqlite3",
                expected_source_size=expected_size,
                expected_source_sha256=expected_sha256,
                progress=fail_before_complete,
                shard_rows=1_000,
                shard_bytes=1_000_000,
            )
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)

    assert not (candidate / "COMPLETE").exists()
    assert not (candidate / "manifests" / "certification-manifest.json").exists()
    assert not (candidate / "pin" / "certification.pin.json").exists()
    assert len(
        list((candidate / "manifests").glob("certification-manifest.json.partial.*"))
    ) == 1
    assert len(list((candidate / "pin").glob("certification.pin.json.partial.*"))) == 1


def test_interruption_after_complete_write_quarantines_all_terminal_names(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source, run_id, expected_size, expected_sha256 = _readonly_replayable_source(tmp_path)
    candidate = tmp_path / "candidate"
    complete = candidate / "COMPLETE"
    manifest = candidate / "manifests" / "certification-manifest.json"
    pin = candidate / "pin" / "certification.pin.json"
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)
    real_write = certification._write_new_canonical_json

    def interrupt_after_complete(path: Path, value: object) -> None:
        real_write(path, value)
        if path == complete:
            raise KeyboardInterrupt("synthetic interruption after COMPLETE write")

    monkeypatch.setattr(
        certification,
        "_write_new_canonical_json",
        interrupt_after_complete,
    )
    try:
        with pytest.raises(KeyboardInterrupt, match="after COMPLETE"):
            certify_golden_v3(
                source,
                candidate,
                run_id,
                sentinel_path=tmp_path / "forbidden-original.sqlite3",
                expected_source_size=expected_size,
                expected_source_sha256=expected_sha256,
                shard_rows=1_000,
                shard_bytes=1_000_000,
            )
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)

    assert not any(path.exists() for path in (complete, manifest, pin))
    for path in (complete, manifest, pin):
        quarantined = list(path.parent.glob(f"{path.name}.partial.*"))
        assert len(quarantined) == 1
        assert quarantined[0].stat().st_size > 0
    with pytest.raises(GoldenCertificationError, match=r"missing|unsafe|COMPLETE"):
        verify_golden_v3_certification(candidate)
