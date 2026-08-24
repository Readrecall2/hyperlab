from __future__ import annotations

import hashlib
from argparse import Namespace
from pathlib import Path

import pytest

import hyperlab.paper.golden_v3_replay as replay
import scripts.export_paper_golden_v3 as export_cli
import scripts.verify_paper_golden_v3 as verify_cli
from hyperlab.paper.golden_v3 import GoldenRefusal
from hyperlab.paper.golden_v3_replay import GoldenReplayError


def _export_args(tmp_path: Path, progress: Path) -> Namespace:
    source = tmp_path / "source.sqlite3"
    source.write_bytes(b"offline")
    return Namespace(
        source=source,
        output_root=tmp_path / "extract-a",
        run_id="0" * 64,
        sentinel=tmp_path / "original.sqlite3",
        expected_size=source.stat().st_size,
        expected_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        external_pin=tmp_path / "extract-a.pin.json",
        shard_rows=1_000,
        shard_bytes=1_000_000,
        progress_jsonl=progress,
    )


@pytest.mark.parametrize("collision", ["source-sidecar", "corpus"])
def test_export_progress_rejects_protected_paths(tmp_path: Path, collision: str) -> None:
    results = tmp_path / "results"
    results.mkdir()
    args = _export_args(tmp_path, results / "progress.jsonl")
    args.progress_jsonl = (
        Path(f"{args.source}-wal")
        if collision == "source-sidecar"
        else args.output_root / "progress.jsonl"
    )
    with pytest.raises(GoldenRefusal, match=r"collides|parent|suffix"):
        export_cli._validate_args(args)
    assert not args.progress_jsonl.exists()


@pytest.mark.parametrize("collision", ["sentinel", "journal", "shm", "wal"])
def test_export_pin_rejects_protected_paths(tmp_path: Path, collision: str) -> None:
    results = tmp_path / "results"
    results.mkdir()
    args = _export_args(tmp_path, results / "progress.jsonl")
    args.external_pin = (
        args.sentinel
        if collision == "sentinel"
        else Path(f"{args.source}-{collision}")
    )

    with pytest.raises(GoldenRefusal, match=r"collides|protected"):
        export_cli._validate_args(args)

    assert not args.external_pin.exists()


def test_replay_progress_may_not_mutate_export_or_scratch(tmp_path: Path) -> None:
    export_root = tmp_path / "golden"
    export_root.mkdir()
    scratch = tmp_path / "scratch"
    args = Namespace(
        export_root=export_root,
        scratch_root=scratch,
        target_filename="paper-replay.sqlite3",
        progress_jsonl=export_root / "progress.jsonl",
    )
    with pytest.raises(GoldenRefusal, match="collides"):
        verify_cli._replay(args)
    assert not args.progress_jsonl.exists()


def test_replay_scratch_rejects_reparse_before_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    export_root = tmp_path / "golden"
    export_root.mkdir()
    scratch = tmp_path / "synthetic-junction" / "scratch"

    monkeypatch.setattr(
        replay,
        "_has_reparse_component",
        lambda path: Path(path) == scratch,
        raising=False,
    )
    monkeypatch.setattr(
        replay,
        "verify_golden_v3",
        lambda *_args, **_kwargs: pytest.fail("export verification must not run"),
    )

    with pytest.raises(GoldenReplayError, match=r"symlink|junction|reparse"):
        replay.replay_golden_v3(export_root, scratch)

    assert not scratch.exists()


def test_progress_sink_never_creates_an_unvalidated_parent(tmp_path: Path) -> None:
    progress = tmp_path / "absent" / "progress.jsonl"
    sink = export_cli._ProgressJsonl(progress)
    with pytest.raises(FileNotFoundError):
        sink({"phase": "synthetic"})
    assert not progress.parent.exists()
