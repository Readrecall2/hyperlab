from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from pathlib import Path

import pytest
from pytest import MonkeyPatch
from test_paper_golden_v3 import _build_source, _export

import hyperlab.paper.golden_v3 as golden_v3
import hyperlab.paper.golden_v3_certification as certification
import hyperlab.paper.golden_v3_replay as replay
import scripts.certify_paper_golden_v3 as certify_cli
from hyperlab.paper.golden_v3 import (
    GoldenRefusal,
    GoldenVerificationError,
    verify_golden_v3,
    write_external_pin,
)


class _WindowsCall:
    def __init__(self, callback: Callable[..., object]) -> None:
        self._callback = callback
        self.argtypes: object = None
        self.restype: object = None

    def __call__(self, *args: object) -> object:
        return self._callback(*args)


class _Kernel32:
    def __init__(
        self,
        create_file: Callable[..., object],
        flush_file_buffers: Callable[..., object],
        close_handle: Callable[..., object],
    ) -> None:
        self.CreateFileW = _WindowsCall(create_file)
        self.FlushFileBuffers = _WindowsCall(flush_file_buffers)
        self.CloseHandle = _WindowsCall(close_handle)


def test_interrupted_extraction_retains_only_an_incomplete_candidate(tmp_path: Path) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    output = tmp_path / "interrupted"
    observed: list[Mapping[str, object]] = []

    def interrupt(progress: Mapping[str, object]) -> None:
        observed.append(progress)
        if progress["phase"] == "stream" and int(progress["rows_completed"]) >= 1:
            raise RuntimeError("synthetic extraction interruption")

    with pytest.raises(RuntimeError, match="synthetic extraction interruption"):
        _export(source, output, run_id, progress=interrupt, shard_rows=1)

    assert observed
    assert output.is_dir()
    assert not (output / "COMPLETE").exists()
    with pytest.raises(GoldenVerificationError, match=r"COMPLETE|incomplete"):
        verify_golden_v3(output)


def test_source_stat_change_after_snapshot_can_never_publish_complete(tmp_path: Path) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    output = tmp_path / "source-changed"
    changed = False

    def change_source_after_snapshot(progress: Mapping[str, object]) -> None:
        nonlocal changed
        if changed or progress["phase"] != "stream":
            return
        before = source.stat()
        os.utime(
            source,
            ns=(before.st_atime_ns, before.st_mtime_ns + 2_000_000_000),
        )
        changed = True

    with pytest.raises(GoldenRefusal, match=r"source.*changed|stat.*changed"):
        _export(
            source,
            output,
            run_id,
            progress=change_source_after_snapshot,
            shard_rows=1,
        )

    assert changed is True
    assert output.is_dir()
    assert not (output / "COMPLETE").exists()
    with pytest.raises(GoldenVerificationError, match=r"COMPLETE|incomplete"):
        verify_golden_v3(output)


def test_directory_fsync_has_one_canonical_implementation() -> None:
    assert certification._fsync_directory is golden_v3._fsync_directory
    assert replay._fsync_directory is golden_v3._fsync_directory
    assert certify_cli._fsync_directory is golden_v3._fsync_directory
    assert certification._mkdir_durable is golden_v3._mkdir_durable
    assert replay._mkdir_durable is golden_v3._mkdir_durable


def test_durable_mkdir_creates_each_level_then_fsyncs_its_parent(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    first = tmp_path / "first"
    second = first / "second"
    target = second / "target"
    observed: list[Path] = []

    def observe(parent: Path) -> None:
        observed.append(parent)
        expected_child = {
            tmp_path: first,
            first: second,
            second: target,
        }[parent]
        assert expected_child.is_dir()

    monkeypatch.setattr(golden_v3, "_fsync_directory", observe)

    assert golden_v3._mkdir_durable(target, exist_ok=False) is True
    assert observed == [tmp_path, first, second]
    assert golden_v3._mkdir_durable(target, exist_ok=True) is False
    assert observed == [tmp_path, first, second]
    with pytest.raises(FileExistsError):
        golden_v3._mkdir_durable(target, exist_ok=False)


def test_durable_mkdir_stops_at_first_parent_fsync_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    first = tmp_path / "first"
    second = first / "second"
    target = second / "target"
    observed: list[Path] = []

    def fail_second_parent(parent: Path) -> None:
        observed.append(parent)
        if parent == first:
            raise OSError("synthetic durable mkdir parent fsync failure")

    monkeypatch.setattr(golden_v3, "_fsync_directory", fail_second_parent)
    with pytest.raises(OSError, match="mkdir parent fsync"):
        golden_v3._mkdir_durable(target, exist_ok=False)

    assert observed == [tmp_path, first]
    assert first.is_dir()
    assert second.is_dir()
    assert not target.exists()


@pytest.mark.parametrize("failure", [None, "open", "fsync", "close"])
def test_directory_fsync_posix_closes_and_propagates(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    failure: str | None,
) -> None:
    descriptor = 654_321
    events: list[tuple[str, object]] = []
    monkeypatch.setattr(golden_v3.os, "name", "posix")

    def fake_open(path: Path, flags: int) -> int:
        events.append(("open", (path, flags)))
        if failure == "open":
            raise OSError("synthetic directory open failure")
        return descriptor

    def fake_fsync(observed: int) -> None:
        events.append(("fsync", observed))
        if failure == "fsync":
            raise OSError("synthetic directory fsync failure")

    def fake_close(observed: int) -> None:
        events.append(("close", observed))
        if failure == "close":
            raise OSError("synthetic directory close failure")

    monkeypatch.setattr(golden_v3.os, "open", fake_open)
    monkeypatch.setattr(golden_v3.os, "fsync", fake_fsync)
    monkeypatch.setattr(golden_v3.os, "close", fake_close)

    if failure is None:
        golden_v3._fsync_directory(tmp_path)
    else:
        with pytest.raises(OSError, match=failure):
            golden_v3._fsync_directory(tmp_path)

    expected_names = ["open"] if failure == "open" else ["open", "fsync", "close"]
    assert [name for name, _detail in events] == expected_names
    opened_path, opened_flags = events[0][1]
    assert opened_path == tmp_path
    assert opened_flags == os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0))


@pytest.mark.parametrize("failure", [None, "open", "fsync", "close"])
def test_directory_fsync_windows_closes_and_propagates(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    failure: str | None,
) -> None:
    handle = 123_456
    invalid_handle = golden_v3.wintypes.HANDLE(-1).value
    events: list[tuple[str, object]] = []
    monkeypatch.setattr(golden_v3.os, "name", "nt")

    def create_file(*args: object) -> object:
        events.append(("open", args))
        return invalid_handle if failure == "open" else handle

    def flush_file_buffers(observed: object) -> bool:
        events.append(("fsync", observed))
        return failure != "fsync"

    def close_handle(observed: object) -> bool:
        events.append(("close", observed))
        return failure != "close"

    kernel32 = _Kernel32(create_file, flush_file_buffers, close_handle)
    monkeypatch.setattr(
        golden_v3.ctypes,
        "WinDLL",
        lambda *_args, **_kwargs: kernel32,
        raising=False,
    )
    monkeypatch.setattr(
        golden_v3.ctypes,
        "get_last_error",
        lambda: 5,
        raising=False,
    )
    monkeypatch.setattr(
        golden_v3.ctypes,
        "WinError",
        lambda code: OSError(code, "synthetic Windows durability failure"),
        raising=False,
    )

    if failure is None:
        golden_v3._fsync_directory(tmp_path)
    else:
        with pytest.raises(OSError, match="Windows durability"):
            golden_v3._fsync_directory(tmp_path)

    expected_names = ["open"] if failure == "open" else ["open", "fsync", "close"]
    assert [name for name, _detail in events] == expected_names
    open_args = events[0][1]
    assert open_args[0] == str(tmp_path)
    assert open_args[1:3] == (0x40000000, 0x00000007)
    assert open_args[4:6] == (3, 0x02000000)


def test_stream_directory_is_fsynced_before_manifest_publication(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    output = corpus / "extract-a"
    observed: list[tuple[Path, bool, bool, bool]] = []

    def observe(path: Path) -> None:
        observed.append(
            (
                path,
                (output / "streams").exists(),
                (output / "manifest.json").exists(),
                (output / "COMPLETE").exists(),
            )
        )

    monkeypatch.setattr(golden_v3, "_fsync_directory", observe)
    _export(source, output, run_id)

    assert observed[0] == (corpus, False, False, False)
    assert (output / "streams", True, False, False) in observed


def test_export_root_parent_fsync_failure_blocks_streams_and_manifest(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    output = corpus / "extract-a"

    def fail_corpus(parent: Path) -> None:
        if parent == corpus:
            assert output.is_dir()
            assert not (output / "streams").exists()
            raise OSError("synthetic corpus fsync failure")

    monkeypatch.setattr(golden_v3, "_fsync_directory", fail_corpus)
    with pytest.raises(OSError, match="corpus fsync"):
        _export(source, output, run_id)

    assert not (output / "streams").exists()
    assert not (output / "manifest.json").exists()
    assert not (output / "COMPLETE").exists()


def test_stream_directory_fsync_failure_blocks_manifest_and_complete(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    output = tmp_path / "golden"

    def fail_streams(path: Path) -> None:
        if path == output / "streams":
            raise OSError("synthetic streams directory fsync failure")

    monkeypatch.setattr(golden_v3, "_fsync_directory", fail_streams)
    with pytest.raises(OSError, match="streams directory fsync"):
        _export(source, output, run_id)

    assert not (output / "manifest.json").exists()
    assert not (output / "COMPLETE").exists()


def test_external_pin_parent_chain_fsync_failure_blocks_pin_publication(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    exported = tmp_path / "golden"
    _export(source, exported, run_id)
    pins = tmp_path / "standalone-pins"
    nested = pins / "nested"
    pin = nested / "golden.pin.json"
    observed: list[Path] = []

    def fail_nested_parent(parent: Path) -> None:
        observed.append(parent)
        if parent == tmp_path:
            assert pins.is_dir()
            assert not nested.exists()
        if parent == pins:
            assert nested.is_dir()
            assert not pin.exists()
            raise OSError("synthetic pin parent fsync failure")

    monkeypatch.setattr(golden_v3, "_fsync_directory", fail_nested_parent)
    with pytest.raises(OSError, match="pin parent fsync"):
        write_external_pin(exported, pin)

    assert observed == [tmp_path, pins]
    assert not pin.exists()
