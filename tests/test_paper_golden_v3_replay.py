from __future__ import annotations

import json
import os
from collections.abc import Mapping
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pytest import MonkeyPatch
from test_paper_golden_v3 import _START, _build_source, _config, _export, _market

import hyperlab.paper.golden_v3 as golden_v3_core
import hyperlab.paper.golden_v3_replay as golden_v3_replay
from hyperlab.paper import PaperEngine, PaperStore
from hyperlab.paper.golden_v3_replay import replay_golden_v3


def test_replay_reconstructs_and_compares_every_logical_row_including_revision_zero(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    config = _config()
    store = PaperStore(source)
    engine = PaperEngine(store, config)
    engine.start()
    for ordinal in range(5):
        engine.process_market(_market(ordinal))
    engine.post_funding(
        instrument="HYPERLIQUID:BTC:perp",
        amount=Decimal("0"),
        occurred_at=_START + timedelta(seconds=6),
        source_event_id="c" * 64,
    )
    engine.pause(
        as_of=_START + timedelta(seconds=7),
        reason="golden v3 synthetic committed alert fixture",
        operator_artifact_hash="d" * 64,
    )
    assert store.inspect_integrity_readonly(config.run_id).ok is True
    store.close()
    exported = tmp_path / "golden"
    scratch = tmp_path / "scratch"
    _export(source, exported, config.run_id)

    verify_calls = 0
    original_verify = golden_v3_core.verify_golden_v3

    def counted_verify(*args: object, **kwargs: object) -> object:
        nonlocal verify_calls
        verify_calls += 1
        return original_verify(*args, **kwargs)

    monkeypatch.setattr(golden_v3_core, "verify_golden_v3", counted_verify)
    monkeypatch.setattr(golden_v3_replay, "verify_golden_v3", counted_verify)
    progress_events: list[dict[str, object]] = []

    def capture_progress(record: Mapping[str, object]) -> None:
        progress_events.append(dict(record))

    result = replay_golden_v3(exported, scratch, progress=capture_progress)

    assert verify_calls == 1
    assert result["status"] == "REPLAY_DIFFERENTIAL_EXACT"
    assert Path(str(result["target_path"])).is_file()
    streams = result["differential"]["streams"]
    assert streams["inbox"]["rows_compared"] > 0
    assert streams["ledger_transactions"]["rows_compared"] > 0
    assert streams["commits"]["rows_compared"] > 0
    assert streams["projection_history"]["rows_compared"] > 1
    assert streams["projection_current"]["rows_compared"] == 1

    manifest = json.loads((exported / "manifest.json").read_text(encoding="utf-8"))
    stream_manifests = manifest["streams"]
    differential_events = [
        event
        for event in progress_events
        if event.get("phase")
        in {"differential", "differential_stream_complete"}
    ]
    assert differential_events
    assert any(float(event["eta_seconds"]) > 0 for event in differential_events)
    for event in differential_events:
        stream_name = str(event["stream"])
        rows_completed = int(event["rows_completed"])
        total_expected = int(event["total_expected"])
        assert total_expected == stream_manifests[stream_name]["row_count"]
        assert 0 <= rows_completed <= total_expected
        assert Path(str(event["target_path"])).name == "paper-replay.sqlite3"
        assert int(event["target_store_bytes"]) > 0
        assert float(event["elapsed_seconds"]) >= 0
        assert float(event["eta_seconds"]) >= 0
        if rows_completed == total_expected:
            assert event["eta_seconds"] == 0.0

    phases = [str(event["phase"]) for event in progress_events]
    assert phases.index("target_preservation") < phases.index(
        "target_preservation_complete"
    )
    assert phases.index("target_preservation_complete") < phases.index(
        "target_preserved"
    )
    assert phases.index("target_preserved") < phases.index("target_fingerprint")
    assert phases.index("target_fingerprint") < phases.index(
        "target_fingerprint_complete"
    )
    preservation_complete = next(
        event
        for event in progress_events
        if event["phase"] == "target_preservation_complete"
    )
    fingerprint_complete = next(
        event
        for event in progress_events
        if event["phase"] == "target_fingerprint_complete"
    )
    combined_seconds = float(result["timings"]["preserve_fingerprint_seconds"])
    assert combined_seconds >= (
        float(preservation_complete["elapsed_seconds"])
        + float(fingerprint_complete["elapsed_seconds"])
    )


def test_preserved_target_fsyncs_directory_after_atomic_publication(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "source.sqlite3"
    source.write_bytes(b"durable replay target")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    events: list[tuple[str, object]] = []
    real_fsync = os.fsync
    real_replace = os.replace

    def tracked_fsync(descriptor: int) -> None:
        events.append(("file_fsync", descriptor))
        real_fsync(descriptor)

    def tracked_replace(partial: Path, target: Path) -> None:
        events.append(("replace", target))
        real_replace(partial, target)

    def tracked_directory_fsync(path: Path) -> None:
        events.append(("directory_fsync", path))

    monkeypatch.setattr(golden_v3_replay.os, "fsync", tracked_fsync)
    monkeypatch.setattr(golden_v3_replay.os, "replace", tracked_replace)
    monkeypatch.setattr(
        golden_v3_replay,
        "_fsync_directory",
        tracked_directory_fsync,
    )

    preserved = golden_v3_replay._preserve_target_copy(
        source,
        scratch,
        target_filename="preserved.sqlite3",
    )

    assert preserved.read_bytes() == source.read_bytes()
    assert [name for name, _detail in events] == [
        "directory_fsync",
        "file_fsync",
        "replace",
        "directory_fsync",
    ]
    assert events[0] == ("directory_fsync", scratch)
    assert events[-1] == ("directory_fsync", preserved.parent)


def test_standalone_scratch_parent_fsync_failure_blocks_temp_store(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source, include_unlinked_alert=False)
    exported = tmp_path / "golden"
    _export(source, exported, run_id)
    scratch_parent = tmp_path / "standalone-scratch"
    scratch_parent.mkdir()
    scratch = scratch_parent / "replay"

    def fail_scratch_parent(parent: Path) -> None:
        assert parent == scratch_parent
        assert scratch.is_dir()
        assert list(scratch.iterdir()) == []
        raise OSError("synthetic scratch parent fsync failure")

    monkeypatch.setattr(golden_v3_core, "_fsync_directory", fail_scratch_parent)
    with pytest.raises(OSError, match="scratch parent fsync"):
        replay_golden_v3(exported, scratch)

    assert scratch.is_dir()
    assert list(scratch.iterdir()) == []


@pytest.mark.parametrize("failure_at", ["scratch", "preserved"])
def test_preserved_target_propagates_directory_fsync_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    failure_at: str,
) -> None:
    source = tmp_path / "source.sqlite3"
    source.write_bytes(b"fail-closed replay target")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    calls = 0

    def fail_directory_fsync(_path: Path) -> None:
        nonlocal calls
        calls += 1
        if failure_at == "scratch" or calls == 2:
            raise OSError(f"synthetic {failure_at} directory fsync failure")

    monkeypatch.setattr(
        golden_v3_replay,
        "_fsync_directory",
        fail_directory_fsync,
    )
    with pytest.raises(OSError, match=failure_at):
        golden_v3_replay._preserve_target_copy(
            source,
            scratch,
            target_filename="preserved.sqlite3",
        )


def test_target_preservation_reports_transition_before_first_read(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "source.sqlite3"
    source.write_bytes(b"0123456789")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    observed: list[dict[str, object]] = []
    real_open = Path.open

    class TrackedReader:
        def __init__(self, handle: object) -> None:
            self._handle = handle

        def __enter__(self) -> TrackedReader:
            return self

        def __exit__(
            self,
            exc_type: object,
            exc_value: object,
            traceback: object,
        ) -> None:
            self._handle.close()

        def read(self, size: int = -1) -> bytes:
            assert observed[0]["phase"] == "target_preservation"
            assert observed[0]["bytes_completed"] == 0
            return self._handle.read(size)

    def tracked_open(path: Path, *args: object, **kwargs: object) -> object:
        handle = real_open(path, *args, **kwargs)
        return TrackedReader(handle) if path == source else handle

    monkeypatch.setattr(Path, "open", tracked_open)
    monkeypatch.setattr(golden_v3_replay, "_COPY_CHUNK_BYTES", 4)
    monkeypatch.setattr(golden_v3_replay, "_COPY_PROGRESS_BYTES", 4)
    monkeypatch.setattr(golden_v3_replay, "_fsync_directory", lambda _path: None)
    preserved = golden_v3_replay._preserve_target_copy(
        source,
        scratch,
        target_filename="preserved.sqlite3",
        progress=lambda event: observed.append(dict(event)),
    )

    preservation = [
        event for event in observed if str(event["phase"]).startswith("target_preservation")
    ]
    assert preservation[0]["bytes_completed"] == 0
    assert [event["bytes_completed"] for event in preservation[1:-1]] == [4, 8, 10]
    assert preservation[-1]["phase"] == "target_preservation_complete"
    assert preservation[-1]["bytes_completed"] == source.stat().st_size
    assert all(event["total_expected"] == source.stat().st_size for event in preservation)
    assert preserved.read_bytes() == source.read_bytes()


def test_target_fingerprint_reports_start_before_hash_and_complete(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    target = tmp_path / "preserved.sqlite3"
    target.write_bytes(b"fingerprint target")
    observed: list[dict[str, object]] = []

    def observed_hash(path: Path) -> str:
        assert path == target
        assert observed[-1]["phase"] == "target_fingerprint"
        assert observed[-1]["bytes_completed"] == 0
        return "a" * 64

    monkeypatch.setattr(golden_v3_replay, "_sha256_file", observed_hash)
    digest, target_bytes = golden_v3_replay._fingerprint_target(
        target,
        progress=lambda event: observed.append(dict(event)),
    )

    assert digest == "a" * 64
    assert target_bytes == target.stat().st_size
    assert [event["phase"] for event in observed] == [
        "target_fingerprint",
        "target_fingerprint_complete",
    ]
    assert observed[-1]["bytes_completed"] == target_bytes
    assert observed[-1]["total_expected"] == target_bytes
