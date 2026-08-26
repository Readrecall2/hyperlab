from __future__ import annotations

import hashlib
import os
import sqlite3
from pathlib import Path

import pytest

from hyperlab.paper.storage_v4.phase1c_certification import (
    Phase1CCertificationError,
    _validate_startup_file_trace,
)
from hyperlab.paper.storage_v4.startup_trace import (
    STARTUP_TRACE_STATUS,
    StartupFileCategory,
    StartupTraceError,
    StartupTracePaths,
    trace_startup_file_access,
)


def _trace_paths(tmp_path: Path) -> StartupTracePaths:
    root = (tmp_path / "candidate").absolute()
    raw = root / "raw"
    paper = root / "paper"
    anchors = root / "anchors"
    for directory in (
        raw / "segments",
        raw / "manifests",
        paper / "segments",
        paper / "manifests",
        paper / "checkpoints",
        anchors,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    exact_files = {
        raw / "CURRENT": b"raw-current",
        paper / "CURRENT": b"paper-current",
        anchors / "raw.sqlite3": b"raw-anchor",
        anchors / "paper.sqlite3": b"paper-anchor",
        anchors / "raw.writer.lock": b"raw-lease",
        anchors / "paper.writer.lock": b"paper-anchor-lease",
        paper / "writer.lock": b"paper-writer-lease",
    }
    for path, value in exact_files.items():
        path.write_bytes(value)
    connection = sqlite3.connect(paper / "overlay.sqlite3")
    connection.execute("CREATE TABLE witness(value INTEGER NOT NULL)")
    connection.commit()
    connection.close()
    return StartupTracePaths(
        candidate_root=root,
        raw_root=raw,
        paper_root=paper,
        raw_anchor=anchors / "raw.sqlite3",
        paper_anchor=anchors / "paper.sqlite3",
        raw_anchor_writer_lease=anchors / "raw.writer.lock",
        paper_anchor_writer_lease=anchors / "paper.writer.lock",
        paper_writer_lease=paper / "writer.lock",
    )


def test_trace_records_ordered_python_level_opens_and_post_scope_hashes(
    tmp_path: Path,
) -> None:
    paths = _trace_paths(tmp_path)
    raw_manifest = paths.raw_root / "manifests" / "root.rawmanifest"
    paper_checkpoint = paths.paper_root / "checkpoints" / "root.checkpoint"
    raw_manifest.write_bytes(b"raw-manifest")
    paper_checkpoint.write_bytes(b"paper-checkpoint")

    with trace_startup_file_access(paths) as recorder:
        descriptor = os.open(raw_manifest, os.O_RDONLY)
        os.close(descriptor)
        with paper_checkpoint.open("rb") as stream:
            assert stream.read() == b"paper-checkpoint"
        connection = sqlite3.connect(paths.paper_root / "overlay.sqlite3")
        assert connection.execute("SELECT COUNT(*) FROM witness").fetchone() == (0,)
        connection.close()

    trace = recorder.result
    assert trace.status == STARTUP_TRACE_STATUS
    assert trace.historical_segment_open_count == 0
    assert [item.category for item in trace.opens] == [
        StartupFileCategory.RAW_MANIFEST,
        StartupFileCategory.PAPER_CHECKPOINT,
        StartupFileCategory.PAPER_OVERLAY,
    ]
    assert [item.relative_path for item in trace.opens] == [
        "raw/manifests/root.rawmanifest",
        "paper/checkpoints/root.checkpoint",
        "paper/overlay.sqlite3",
    ]
    assert trace.opens[0].sha256_after_scope == hashlib.sha256(b"raw-manifest").hexdigest()
    assert trace.opens[1].size_bytes_after_scope == len(b"paper-checkpoint")
    payload = trace.payload()
    assert payload["historical_segment_paths_opened"] == []
    assert payload["ordered_relative_paths"] == [
        "raw/manifests/root.rawmanifest",
        "paper/checkpoints/root.checkpoint",
        "paper/overlay.sqlite3",
    ]
    limitations = payload["limitations"]
    assert isinstance(limitations, list)
    assert any(
        "SQLITE_INTERNAL_IO_AND_SIDECARS_ARE_NOT" in limitation
        for limitation in limitations
    )


def test_trace_restores_all_hooks_after_exception(tmp_path: Path) -> None:
    paths = _trace_paths(tmp_path)
    original_os_open = os.open
    original_path_open = Path.open
    original_sqlite_connect = sqlite3.connect

    with pytest.raises(RuntimeError, match="boom"), trace_startup_file_access(paths):
        assert os.open is not original_os_open
        assert Path.open is not original_path_open
        assert sqlite3.connect is not original_sqlite_connect
        raise RuntimeError("boom")

    assert os.open is original_os_open
    assert Path.open is original_path_open
    assert sqlite3.connect is original_sqlite_connect


def test_trace_fails_closed_before_historical_segment_open(tmp_path: Path) -> None:
    paths = _trace_paths(tmp_path)
    historical = paths.raw_root / "segments" / "historical.rawseg"
    historical.write_bytes(b"must-not-open")
    original_path_open = Path.open

    with (
        pytest.raises(StartupTraceError, match="historical segment path"),
        trace_startup_file_access(paths),
    ):
        historical.open("rb")

    assert Path.open is original_path_open


def test_trace_fails_closed_on_unclassified_candidate_file(tmp_path: Path) -> None:
    paths = _trace_paths(tmp_path)
    unclassified = paths.candidate_root / "unclassified.txt"
    unclassified.write_bytes(b"not-authority")

    with (
        pytest.raises(StartupTraceError, match="unclassified file"),
        trace_startup_file_access(paths),
    ):
        unclassified.open("rb")


def test_trace_paths_reject_authority_outside_candidate(tmp_path: Path) -> None:
    paths = _trace_paths(tmp_path)
    with pytest.raises(StartupTraceError, match="raw_anchor escapes"):
        StartupTracePaths(
            candidate_root=paths.candidate_root,
            raw_root=paths.raw_root,
            paper_root=paths.paper_root,
            raw_anchor=(tmp_path / "outside.sqlite3").absolute(),
            paper_anchor=paths.paper_anchor,
            raw_anchor_writer_lease=paths.raw_anchor_writer_lease,
            paper_anchor_writer_lease=paths.paper_anchor_writer_lease,
            paper_writer_lease=paths.paper_writer_lease,
        )


def test_phase1c_certifier_requires_complete_candidate_bound_trace(
    tmp_path: Path,
) -> None:
    paths = _trace_paths(tmp_path)
    raw_manifest = paths.raw_root / "manifests" / "root.rawmanifest"
    paper_manifest = paths.paper_root / "manifests" / "root.manifest"
    paper_checkpoint = paths.paper_root / "checkpoints" / "root.checkpoint"
    for path, value in (
        (raw_manifest, b"raw-manifest"),
        (paper_manifest, b"paper-manifest"),
        (paper_checkpoint, b"paper-checkpoint"),
    ):
        path.write_bytes(value)

    with trace_startup_file_access(paths) as recorder:
        for path in (
            raw_manifest,
            paper_manifest,
            paper_checkpoint,
            paths.raw_anchor,
            paths.paper_anchor,
        ):
            with path.open("rb") as stream:
                assert stream.read(1)
        connection = sqlite3.connect(paths.paper_root / "overlay.sqlite3")
        connection.close()

    result = _validate_startup_file_trace(
        recorder.result,
        expected_candidate_root=paths.candidate_root,
        label="unit-candidate",
    )
    assert result["verified"] is True
    assert result["trace"] == recorder.result.payload()

    with pytest.raises(Phase1CCertificationError, match="candidate root differs"):
        _validate_startup_file_trace(
            recorder.result,
            expected_candidate_root=(tmp_path / "different-root").absolute(),
            label="unit-candidate",
        )
