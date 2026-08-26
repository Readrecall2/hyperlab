from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
import websocket

import hyperlab.collector.websocket as websocket_module
from hyperlab.research_data.adapters import PublicHttpRequest
from hyperlab.research_data.envelope import (
    SYNTHETIC_FIXTURE_LABEL,
    CaptureProvenance,
    SessionEnvelopeFactory,
    Venue,
)
from hyperlab.research_data.probe import (
    ProbeConfig,
    _Counters,
    _execute_http,
    _hyperliquid_probe,
    recover_public_probe_output,
    run_public_probe,
)
from hyperlab.research_data.segments import (
    ResearchDataCapacityError,
    ResearchSegmentReader,
    ResearchSegmentWriter,
)


def _factory(collection_id: str) -> SessionEnvelopeFactory:
    return SessionEnvelopeFactory(
        venue=Venue.HYPERLIQUID,
        collector_identity="fixture-probe",
        session_identity="fixture-session",
        source_metadata_version="fixture-v1",
        provenance=CaptureProvenance(
            collection_id,
            "fixture://probe",
            "FIXTURE",
            SYNTHETIC_FIXTURE_LABEL,
        ),
    )


def test_hyperliquid_remote_websocket_close_is_reconnectable(
    tmp_path: Path, monkeypatch
) -> None:
    collection_id = "fixture-ws-close"
    factory = _factory(collection_id)
    writer = ResearchSegmentWriter(
        tmp_path / "raw",
        collection_id=collection_id,
        max_segment_bytes=4096,
        rotation_seconds=30,
        max_total_bytes=1_000_000,
    )

    class FakeSocket:
        def send_json(self, _payload):
            return None

        def receive(self, *, timeout_seconds):
            assert timeout_seconds == 1.0
            raise websocket.WebSocketConnectionClosedException("fixture remote close")

        def telemetry_snapshot(self):
            return {"queue_high_water": 0}

        def close(self):
            return None

    class FakeConnector:
        def __init__(self):
            self.connects = 0

        def connect(self, network, timeout):
            assert network == "public" and 0 < timeout <= 10.0
            self.connects += 1
            return FakeSocket()

    connector = FakeConnector()
    monkeypatch.setattr(websocket_module, "UrlWebsocketClientFactory", lambda *_a, **_k: connector)
    config = ProbeConfig(
        output_root=tmp_path / "unused",
        venue=Venue.HYPERLIQUID,
        feeds=("bbo",),
        instruments=("BTC",),
        census_limit=0,
        duration_seconds=1,
        max_bytes=1_000_000,
        max_segment_bytes=4096,
        rotation_seconds=30,
        progress_interval_seconds=1,
    )
    limitations = _hyperliquid_probe(
        config,
        factory=factory,
        writer=writer,
        counters=_Counters(),
        deadline=time.monotonic() + 1,
        stop_requested=lambda: connector.connects >= 1,
        progress=lambda _count: None,
        session=object(),
    )
    assert connector.connects == 1
    assert "TWAP_GLOBAL_PUBLIC_SOURCE_UNVERIFIED" in limitations
    writer.abort()


def test_offline_probe_recovery_publishes_only_authenticated_frames(tmp_path: Path) -> None:
    output = tmp_path / "probe-output"
    reports = output / "reports"
    reports.mkdir(parents=True)
    collection_id = "fixture-recovery"
    factory = _factory(collection_id)
    writer = ResearchSegmentWriter(
        output / "raw",
        collection_id=collection_id,
        max_segment_bytes=4096,
        rotation_seconds=30,
        max_total_bytes=1_000_000,
    )
    envelope = factory.make(
        feed_type="bbo",
        instrument_id="HL:BTC:perp",
        market_id=None,
        source_timestamp_ns=1,
        receive_timestamp_utc_ns=2,
        receive_monotonic_ns=3,
        raw_payload=b'{"fixture_label":"SYNTHETIC/FIXTURE"}',
        source_sequence=None,
    )
    writer.append(envelope)
    manifest = writer.close()
    assert manifest is not None

    report = recover_public_probe_output(
        output,
        venue=Venue.HYPERLIQUID,
        requested_duration_seconds=120,
        terminal_health="PUBLIC_SOURCE_UNAVAILABLE",
        error="WebSocketConnectionClosedException:fixture",
    )
    assert report.frames == 1
    assert report.manifest_sha256 == manifest.manifest_sha256
    assert "UNPUBLISHED_IN_MEMORY_TAIL_NOT_CLAIMED" in report.limitations
    saved = json.loads((reports / "result.json").read_text(encoding="utf-8"))
    assert saved["terminal_health"] == "PUBLIC_SOURCE_UNAVAILABLE"
    assert ResearchSegmentReader(
        output / "raw", manifest_sha256=report.manifest_sha256 or ""
    ).replay() == (envelope,)


def test_probe_publishes_running_health_before_collection(tmp_path: Path, monkeypatch) -> None:
    observed_health: list[dict[str, object]] = []

    class FakeSession:
        def close(self) -> None:
            return None

    def fake_probe(config, **kwargs):
        del config
        observed_health.append(
            json.loads((tmp_path / "probe" / "reports" / "health.json").read_text())
        )
        kwargs["progress"](0)
        return ()

    monkeypatch.setattr("hyperlab.research_data.probe._hyperliquid_probe", fake_probe)
    report = run_public_probe(
        ProbeConfig(
            output_root=tmp_path / "probe",
            venue=Venue.HYPERLIQUID,
            feeds=("bbo",),
            instruments=("BTC",),
            census_limit=0,
            duration_seconds=1,
            max_bytes=1_000_000,
            max_segment_bytes=4096,
            rotation_seconds=30,
            progress_interval_seconds=1,
            collection_id="fixture-running-health",
        ),
        http_session_factory=FakeSession,
    )
    assert observed_health[0]["terminal_health"] == "RUNNING"
    assert observed_health[0]["frames"] == 0
    assert report.terminal_health == "COMPLETE"


def test_probe_reports_backpressure_as_a_visible_gap(tmp_path: Path, monkeypatch) -> None:
    class FakeSession:
        def close(self) -> None:
            return None

    def overflow(*_args, **_kwargs):
        raise BufferError("SYNTHETIC/FIXTURE queue overflow")

    monkeypatch.setattr("hyperlab.research_data.probe._hyperliquid_probe", overflow)
    report = run_public_probe(
        ProbeConfig(
            output_root=tmp_path / "overflow",
            venue=Venue.HYPERLIQUID,
            feeds=("bbo",),
            instruments=("BTC",),
            census_limit=0,
            duration_seconds=1,
            max_bytes=1_000_000,
            max_segment_bytes=4096,
            rotation_seconds=30,
            progress_interval_seconds=1,
            collection_id="fixture-overflow-health",
        ),
        http_session_factory=FakeSession,
    )
    assert report.terminal_health == "BACKPRESSURE_LIMIT_REACHED"
    assert report.gaps == 1
    assert report.frames == 0


def test_public_http_body_is_streamed_under_a_hard_raw_bound() -> None:
    class Response:
        status_code = 200
        content = b""
        closed = False

        def iter_content(self, chunk_size):
            assert chunk_size == 64 * 1024
            yield b"123"
            yield b"4"

        def close(self):
            self.closed = True

    response = Response()

    class Session:
        def get(self, _url, **kwargs):
            assert kwargs["stream"] is True
            return response

    with pytest.raises(ResearchDataCapacityError, match="raw byte bound"):
        _execute_http(
            Session(),
            PublicHttpRequest(
                method="GET", url="https://gamma-api.polymarket.com/markets"
            ),
            deadline=time.monotonic() + 1,
            max_response_bytes=3,
        )
    assert response.closed
