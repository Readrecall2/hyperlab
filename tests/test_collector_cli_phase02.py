from __future__ import annotations

import json
import signal
import socket
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest
import websocket
from typer.testing import CliRunner

import hyperlab.collector.replay as replay_module
import hyperlab.collector.storage as storage_module
from hyperlab import cli as cli_module
from hyperlab.cli import app
from hyperlab.collector.runtime import PublicCollector
from hyperlab.data.lake import validate_partition

runner = CliRunner()
FIXTURE_DIR = Path(__file__).parent / "fixtures" / "hyperliquid" / "replay"


class _FakeReplaySink:
    instances: ClassVar[list[_FakeReplaySink]] = []

    def __init__(
        self,
        _root: Path,
        *,
        batch_size: int,
        queue_capacity: int,
        persistent_dedup: bool,
    ) -> None:
        assert batch_size == 500
        assert queue_capacity == 10_000
        assert persistent_dedup is False
        self.pending = 0
        self.flush_rows: list[int] = []
        self.close_calls = 0
        self.instances.append(self)

    @property
    def should_flush(self) -> bool:
        return self.pending >= 2

    def add(self, _record: object) -> bool:
        self.pending += 1
        return True

    def flush(self) -> SimpleNamespace:
        rows = self.pending
        self.pending = 0
        self.flush_rows.append(rows)
        return SimpleNamespace(row_count=rows)

    def close(self) -> None:
        self.close_calls += 1


def _write_config(path: Path, data_dir: Path) -> Path:
    config = path / "research.toml"
    config.write_text(
        "\n".join(
            (
                "[app]",
                'network = "mainnet"',
                f'data_dir = "{data_dir.as_posix()}"',
                "request_timeout_seconds = 1.0",
                'mode = "readonly"',
                "",
            )
        ),
        encoding="utf-8",
    )
    return config


def _lake_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"), key=lambda item: item.as_posix())
        if path.is_file()
    }


def test_collect_help_exposes_phase_02_flags_and_removes_snapshot_loop_flags() -> None:
    result = runner.invoke(app, ["collect", "--help"], env={"COLUMNS": "160"})

    assert result.exit_code == 0
    for flag in (
        "--network",
        "--assets",
        "--candle-intervals",
        "--duration-seconds",
        "--max-messages",
        "--batch-size",
        "--history-lookback-hours",
    ):
        assert flag in result.output
    for obsolete_flag in ("--interval-seconds", "--samples"):
        assert obsolete_flag not in result.output


def test_cooperative_signal_handlers_request_stop_and_restore_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    managed = (int(signal.SIGTERM), int(signal.SIGINT))
    original = {signum: object() for signum in managed}
    current = dict(original)
    signal_calls: list[tuple[int, object]] = []
    stop_calls = 0

    def fake_getsignal(signum: int) -> object:
        return current[signum]

    def fake_signal(signum: int, handler: object) -> object:
        previous = current[signum]
        current[signum] = handler
        signal_calls.append((signum, handler))
        return previous

    def stop() -> None:
        nonlocal stop_calls
        stop_calls += 1

    monkeypatch.setattr(cli_module.signal, "getsignal", fake_getsignal)
    monkeypatch.setattr(cli_module.signal, "signal", fake_signal)

    with (
        pytest.raises(RuntimeError, match="simulated collector failure"),
        cli_module._cooperative_signal_handlers(stop),
    ):
        handler = current[int(signal.SIGTERM)]
        assert callable(handler)
        handler(int(signal.SIGTERM), None)
        raise RuntimeError("simulated collector failure")

    assert stop_calls == 1
    assert current == original
    assert [signum for signum, _handler in signal_calls] == [
        int(signal.SIGTERM),
        int(signal.SIGINT),
        int(signal.SIGINT),
        int(signal.SIGTERM),
    ]


def test_collect_keyboard_interrupt_stops_closes_and_restores_handlers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    monkeypatch.setattr(cli_module, "CONFIG", _write_config(tmp_path, data_dir))
    monkeypatch.delenv("HYPERLAB_DATA_DIR", raising=False)
    monkeypatch.delenv("HYPERLAB_MODE", raising=False)
    previous_handlers = {signum: signal.getsignal(signum) for signum in (signal.SIGTERM, signal.SIGINT)}

    class FakeCollector:
        def __init__(self) -> None:
            self.stop_calls = 0
            self.close_calls = 0
            self.metrics = SimpleNamespace(as_dict=lambda _now: {"state": "stopped"})

        def run(self, *, max_messages: int, duration_seconds: float | None) -> None:
            assert max_messages == 1
            assert duration_seconds is None
            raise KeyboardInterrupt

        def stop(self) -> None:
            self.stop_calls += 1

        def close(self) -> None:
            self.close_calls += 1

    collector = FakeCollector()
    monkeypatch.setattr(
        PublicCollector,
        "create_default",
        classmethod(lambda _cls, *_args, **_kwargs: collector),
    )

    result = runner.invoke(app, ["collect", "--max-messages", "1"])

    assert result.exit_code == 0, result.output
    assert collector.stop_calls == 1
    assert collector.close_calls == 1
    assert {
        signum: signal.getsignal(signum) for signum in (signal.SIGTERM, signal.SIGINT)
    } == previous_handlers


def test_collect_close_error_does_not_mask_primary_runtime_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    monkeypatch.setattr(cli_module, "CONFIG", _write_config(tmp_path, data_dir))
    monkeypatch.delenv("HYPERLAB_DATA_DIR", raising=False)
    monkeypatch.delenv("HYPERLAB_MODE", raising=False)

    class FakeCollector:
        def __init__(self) -> None:
            self.close_calls = 0
            self.metrics = SimpleNamespace(as_dict=lambda _now: {"state": "stopped"})

        def run(self, *, max_messages: int, duration_seconds: float | None) -> None:
            assert max_messages == 1
            assert duration_seconds is None
            raise ValueError("PRIMARY collector failure")

        def stop(self) -> None:
            pass

        def close(self) -> None:
            self.close_calls += 1
            raise RuntimeError("SECONDARY close failure")

    collector = FakeCollector()
    monkeypatch.setattr(
        PublicCollector,
        "create_default",
        classmethod(lambda _cls, *_args, **_kwargs: collector),
    )

    result = runner.invoke(app, ["collect", "--max-messages", "1"])

    assert result.exit_code == 1
    assert isinstance(result.exception, ValueError)
    assert str(result.exception) == "PRIMARY collector failure"
    assert collector.close_calls == 1
    assert any("SECONDARY close failure" in note for note in getattr(result.exception, "__notes__", ()))


def test_replay_is_network_forbidden_valid_and_byte_deterministic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def network_forbidden(*_args: object, **_kwargs: object) -> object:
        pytest.fail("replay attempted to construct a network socket")

    monkeypatch.setattr(socket, "socket", network_forbidden)
    monkeypatch.setattr(websocket, "create_connection", network_forbidden)
    summaries: list[dict[str, object]] = []
    lakes = (tmp_path / "first", tmp_path / "second")

    for lake in lakes:
        result = runner.invoke(
            app,
            [
                "replay",
                str(FIXTURE_DIR),
                "--output",
                str(lake),
                "--received-at",
                "2026-08-11T23:21:00Z",
            ],
        )

        assert result.exit_code == 0, result.output
        summary = json.loads(result.output)
        assert summary["network_enabled"] is False
        assert summary["fixture_count"] == 9
        assert summary["record_count"] == 19
        assert summary["rows_written"] == summary["record_count"]
        summaries.append(summary)

        manifests = sorted(lake.rglob("*.manifest.json"), key=lambda item: item.as_posix())
        assert manifests
        for manifest_path in manifests:
            validate_partition(manifest_path)

    assert summaries[0] == summaries[1]
    assert _lake_bytes(lakes[0]) == _lake_bytes(lakes[1])


@pytest.mark.parametrize("received_at", ["not-a-date", "2026-13-99T25:61:00Z"])
def test_replay_invalid_received_at_is_a_clean_bad_parameter(
    received_at: str,
) -> None:
    result = runner.invoke(
        app,
        ["replay", str(FIXTURE_DIR), "--received-at", received_at],
    )

    assert result.exit_code == 2
    assert "received-at must be a valid ISO 8601 timestamp" in result.output
    assert "Traceback" not in result.output


def test_replay_flushes_during_ingest_accumulates_rows_and_closes_sink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FakeReplaySink.instances.clear()
    records = [object(), object(), object(), object(), object()]

    def fake_replay(
        _path: Path,
        sink: object,
        _clock: object,
    ) -> dict[str, object]:
        callback = sink
        assert callable(callback)
        for record in records:
            callback(record)
        return {"fixture_count": 5, "record_count": 5}

    monkeypatch.setattr(storage_module, "BatchingLakeSink", _FakeReplaySink)
    monkeypatch.setattr(replay_module, "replay_fixture", fake_replay)

    result = runner.invoke(
        app,
        ["replay", str(FIXTURE_DIR), "--output", str(tmp_path / "lake")],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["rows_written"] == 5
    sink = _FakeReplaySink.instances[-1]
    assert sink.flush_rows == [2, 2, 1]
    assert sink.close_calls == 1


def test_replay_closes_sink_if_parser_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FakeReplaySink.instances.clear()

    def fail_replay(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise RuntimeError("simulated replay failure")

    monkeypatch.setattr(storage_module, "BatchingLakeSink", _FakeReplaySink)
    monkeypatch.setattr(replay_module, "replay_fixture", fail_replay)

    result = runner.invoke(
        app,
        ["replay", str(FIXTURE_DIR), "--output", str(tmp_path / "lake")],
    )

    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
    assert _FakeReplaySink.instances[-1].close_calls == 1


def test_status_exposes_the_atomic_runtime_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    runtime_status = {
        "state": "live",
        "connection_epoch": 3,
        "gaps": 1,
        "stale_channels": ["bbo:BTC"],
    }
    (data_dir / "runtime_status.json").write_text(
        json.dumps(runtime_status),
        encoding="utf-8",
    )
    monkeypatch.setattr(cli_module, "CONFIG", _write_config(tmp_path, data_dir))
    monkeypatch.delenv("HYPERLAB_DATA_DIR", raising=False)
    monkeypatch.delenv("HYPERLAB_MODE", raising=False)

    result = runner.invoke(app, ["status"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["runtime"] == runtime_status
    assert payload["snapshot_count"] == 0


@pytest.mark.parametrize(
    ("arguments", "expected_error"),
    [
        (["--assets", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"], "public market symbol"),
        (["--candle-intervals", "60m"], "unsupported candle intervals"),
        (["--candle-intervals", "1M"], "unsupported candle intervals: ['1M']"),
    ],
)
def test_collect_rejects_unsafe_configuration_before_network_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
    expected_error: str,
) -> None:
    data_dir = tmp_path / "data"
    monkeypatch.setattr(cli_module, "CONFIG", _write_config(tmp_path, data_dir))
    monkeypatch.delenv("HYPERLAB_DATA_DIR", raising=False)
    monkeypatch.delenv("HYPERLAB_MODE", raising=False)

    def collector_forbidden(*_args: object, **_kwargs: object) -> object:
        pytest.fail("invalid input reached collector/network construction")

    monkeypatch.setattr(PublicCollector, "create_default", collector_forbidden)

    result = runner.invoke(app, ["collect", *arguments])

    assert result.exit_code == 2
    assert expected_error in result.output
    assert "Traceback" not in result.output
