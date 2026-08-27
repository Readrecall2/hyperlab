from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
import websocket

import hyperlab.collector.websocket as websocket_module
import hyperlab.research_data.probe as probe_module
from hyperlab.research_data.adapters import (
    POLYMARKET_CLOB_PUBLIC_URL,
    POLYMARKET_GAMMA_PUBLIC_URL,
    POLYMARKET_METADATA_VERSION,
    PolymarketPublicAdapter,
    PublicHttpRequest,
)
from hyperlab.research_data.canonical import canonical_json_bytes
from hyperlab.research_data.envelope import (
    SYNTHETIC_FIXTURE_LABEL,
    CaptureProvenance,
    SessionEnvelopeFactory,
    Venue,
)
from hyperlab.research_data.probe import (
    ProbeConfig,
    _append_probe_bounded,
    _Counters,
    _execute_http,
    _hyperliquid_probe,
    _polymarket_probe,
    _probe_binding_payload,
    _probe_binding_sha256,
    _ProbeBoundaryReached,
    _require_polymarket_websocket_selection,
    recover_public_probe_output,
    run_public_probe,
)
from hyperlab.research_data.segments import (
    ResearchDataCapacityError,
    ResearchSegmentReader,
    ResearchSegmentWriter,
)


def _factory(
    collection_id: str,
    *,
    session_identity: str = "fixture-session",
) -> SessionEnvelopeFactory:
    return SessionEnvelopeFactory(
        venue=Venue.HYPERLIQUID,
        collector_identity="fixture-probe",
        session_identity=session_identity,
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


def test_polymarket_reconnect_reauthenticates_before_subscription_and_book(
    tmp_path: Path,
    monkeypatch,
) -> None:
    collection_id = "fixture-polymarket-rebootstrap"
    factory = SessionEnvelopeFactory(
        venue=Venue.POLYMARKET,
        collector_identity="fixture-polymarket-probe",
        session_identity="fixture-polymarket-session",
        source_metadata_version=POLYMARKET_METADATA_VERSION,
        provenance=CaptureProvenance(
            collection_id,
            "fixture://polymarket-rebootstrap",
            "FIXTURE",
            SYNTHETIC_FIXTURE_LABEL,
        ),
    )
    writer = ResearchSegmentWriter(
        tmp_path / "raw",
        collection_id=collection_id,
        max_segment_bytes=100_000,
        rotation_seconds=30,
        max_total_bytes=1_000_000,
    )
    config = ProbeConfig(
        output_root=tmp_path / "unused",
        venue=Venue.POLYMARKET,
        feeds=("events", "fees", "order_book", "tick_size"),
        instruments=("fixture-token-yes", "fixture-token-no"),
        census_limit=0,
        duration_seconds=5,
        max_bytes=1_000_000,
        max_segment_bytes=100_000,
        rotation_seconds=30,
        progress_interval_seconds=30,
        max_frames=100,
        max_segments=10,
        max_network_calls=50,
    )
    gamma_market = {
        "clobTokenIds": ["fixture-token-yes", "fixture-token-no"],
        "conditionId": "fixture-condition",
        "events": [{"id": "fixture-event"}],
        "fixture_label": SYNTHETIC_FIXTURE_LABEL,
        "outcomes": ["YES", "NO"],
        "questionID": "fixture-question",
    }

    def fake_execute_http(
        _session,
        request,
        *,
        deadline,
        max_response_bytes,
        budget,
    ):
        assert probe_module.time.monotonic() < deadline
        assert max_response_bytes > 0
        budget.consume()
        query = dict(request.query)
        if "/markets-by-token/" in request.url:
            payload = {
                "condition_id": "fixture-condition",
                "fixture_label": SYNTHETIC_FIXTURE_LABEL,
            }
        elif request.url == f"{POLYMARKET_GAMMA_PUBLIC_URL}/markets/keyset":
            payload = {"markets": [gamma_market], "next_cursor": None}
        elif "/clob-markets/" in request.url:
            payload = {
                "condition_id": "fixture-condition",
                "fixture_label": SYNTHETIC_FIXTURE_LABEL,
                "tokens": [
                    {"outcome": "YES", "token_id": "fixture-token-yes"},
                    {"outcome": "NO", "token_id": "fixture-token-no"},
                ],
            }
        elif "/events/" in request.url:
            payload = {
                "fixture_label": SYNTHETIC_FIXTURE_LABEL,
                "id": "fixture-event",
                "markets": [{"id": "fixture-market"}],
            }
        elif request.url.endswith("/fee-rate"):
            payload = {
                "base_fee": "0",
                "fixture_label": SYNTHETIC_FIXTURE_LABEL,
                "market": "fixture-condition",
            }
        elif request.url.endswith("/tick-size"):
            payload = {
                "fixture_label": SYNTHETIC_FIXTURE_LABEL,
                "market": "fixture-condition",
                "minimum_tick_size": "0.01",
            }
        elif request.url.endswith("/book"):
            payload = {
                "asks": [["0.42", "2"]],
                "asset_id": query["token_id"],
                "bids": [["0.40", "2"]],
                "fixture_label": SYNTHETIC_FIXTURE_LABEL,
                "market": "fixture-condition",
            }
        else:  # pragma: no cover - makes an unexpected endpoint explicit
            raise AssertionError(request.url)
        return canonical_json_bytes(payload)

    stop = {"requested": False}
    websocket_attempts = {"count": 0}
    subscription_frame_counts: list[int] = []

    class FakeSocket:
        def getstatus(self) -> int:
            return 101

        def settimeout(self, _timeout: float) -> None:
            return None

        def send(self, _payload: str) -> None:
            subscription_frame_counts.append(writer.frame_count)

        def recv(self) -> str:
            stop["requested"] = True
            return canonical_json_bytes(
                {
                    "asks": [{"price": "0.42", "size": "2"}],
                    "asset_id": "fixture-token-yes",
                    "bids": [{"price": "0.40", "size": "2"}],
                    "event_type": "book",
                    "fixture_label": SYNTHETIC_FIXTURE_LABEL,
                    "market": "fixture-condition",
                    "timestamp": "1787688000000",
                }
            ).decode("utf-8")

        def close(self) -> None:
            return None

    def fake_create_connection(_url: str, **_kwargs):
        websocket_attempts["count"] += 1
        if websocket_attempts["count"] == 1:
            raise websocket.WebSocketException("SYNTHETIC/FIXTURE initial handshake failure")
        return FakeSocket()

    monkeypatch.setattr(probe_module, "_execute_http", fake_execute_http)
    monkeypatch.setattr(websocket, "create_connection", fake_create_connection)
    monkeypatch.setattr(probe_module.time, "sleep", lambda _seconds: None)
    counters = _Counters()
    limitations = _polymarket_probe(
        config,
        factory=factory,
        writer=writer,
        counters=counters,
        deadline=time.monotonic() + 5,
        stop_requested=lambda: stop["requested"],
        progress=lambda _count: None,
        session=object(),
    )
    assert limitations == ()
    assert websocket_attempts["count"] == 2
    assert counters.reconnects == 1
    manifest = writer.close()
    assert manifest is not None
    envelopes = ResearchSegmentReader(
        tmp_path / "raw",
        manifest_sha256=manifest.manifest_sha256,
    ).replay()
    current_session = factory.session_identity
    reconnected = tuple(item for item in envelopes if item.session_identity == current_session)
    websocket_index = next(
        index
        for index, item in enumerate(reconnected)
        if item.provenance.transport == "PUBLIC_WEBSOCKET" and item.feed_type != "heartbeat"
    )
    before_websocket = reconnected[:websocket_index]
    assert tuple(item.feed_type for item in before_websocket) == (
        "heartbeat",
        "metadata",
        "metadata",
        "events",
        "fees",
        "fees",
        "tick_size",
        "tick_size",
    )
    assert before_websocket[0].state.reconnect is True
    assert sum(item.state.reconnect for item in reconnected) == 1
    assert before_websocket[0].provenance.source_url == probe_module.POLYMARKET_PUBLIC_WEBSOCKET_URL
    assert before_websocket[0].state.reason == "RECONNECT_BOUNDARY"
    assert subscription_frame_counts == [len(envelopes) - 1]
    assert reconnected[websocket_index].feed_type == "order_book"

    class LateClock:
        def __init__(self) -> None:
            self.value = 100.0

        def monotonic(self) -> float:
            return self.value

        def monotonic_ns(self) -> int:
            return int(self.value * 1_000_000_000)

        def time_ns(self) -> int:
            return 1_800_000_000_000_000_000 + self.monotonic_ns()

        def sleep(self, seconds: float) -> None:
            self.value += seconds

    late_clock = LateClock()
    late_factory = SessionEnvelopeFactory(
        venue=Venue.POLYMARKET,
        collector_identity="fixture-polymarket-probe",
        session_identity="fixture-polymarket-late-session",
        source_metadata_version=POLYMARKET_METADATA_VERSION,
        provenance=CaptureProvenance(
            "fixture-polymarket-late-frame",
            "fixture://polymarket-late-frame",
            "FIXTURE",
            SYNTHETIC_FIXTURE_LABEL,
        ),
    )
    late_writer = ResearchSegmentWriter(
        tmp_path / "late-raw",
        collection_id="fixture-polymarket-late-frame",
        max_segment_bytes=100_000,
        rotation_seconds=30,
        max_total_bytes=1_000_000,
    )

    class LateSocket(FakeSocket):
        def send(self, _payload: str) -> None:
            return None

        def recv(self) -> str:
            late_clock.value = 101.001
            return canonical_json_bytes(
                {
                    "asks": [{"price": "0.42", "size": "2"}],
                    "asset_id": "fixture-token-yes",
                    "bids": [{"price": "0.40", "size": "2"}],
                    "event_type": "book",
                    "fixture_label": SYNTHETIC_FIXTURE_LABEL,
                    "market": "fixture-condition",
                    "timestamp": "1787688000000",
                }
            ).decode("utf-8")

    monkeypatch.setattr(probe_module, "time", late_clock)
    monkeypatch.setattr(websocket, "create_connection", lambda *_args, **_kwargs: LateSocket())
    with pytest.raises(_ProbeBoundaryReached) as late_boundary:
        _polymarket_probe(
            config,
            factory=late_factory,
            writer=late_writer,
            counters=_Counters(),
            deadline=101.0,
            stop_requested=lambda: False,
            progress=lambda _count: None,
            session=object(),
        )
    assert late_boundary.value.terminal_health == "MAX_DURATION_REACHED"
    late_manifest = late_writer.close()
    assert late_manifest is not None
    assert all(
        item.provenance.transport != "PUBLIC_WEBSOCKET"
        for item in ResearchSegmentReader(
            tmp_path / "late-raw",
            manifest_sha256=late_manifest.manifest_sha256,
        ).replay()
    )


def test_polymarket_websocket_selection_handles_repeated_and_unselected_events() -> None:
    factory = SessionEnvelopeFactory(
        venue=Venue.POLYMARKET,
        collector_identity="fixture-polymarket-selection",
        session_identity="fixture-polymarket-selection",
        source_metadata_version=POLYMARKET_METADATA_VERSION,
        provenance=CaptureProvenance(
            "fixture-polymarket-selection",
            "fixture://polymarket-selection",
            "FIXTURE",
            SYNTHETIC_FIXTURE_LABEL,
        ),
    )
    adapter = PolymarketPublicAdapter()
    repeated_books = adapter.envelope_from_websocket(
        canonical_json_bytes(
            [
                {"asset_id": "token-a", "event_type": "book", "market": "condition"},
                {"asset_id": "token-b", "event_type": "book", "market": "condition"},
            ]
        ),
        factory=factory,
        receive_timestamp_utc_ns=1,
        receive_monotonic_ns=1,
    )
    _require_polymarket_websocket_selection(repeated_books, {"order_book"})

    for event_type in ("last_trade_price", "future_unknown_event"):
        envelope = adapter.envelope_from_websocket(
            canonical_json_bytes(
                [{"asset_id": "token-a", "event_type": event_type, "market": "condition"}]
            ),
            factory=factory,
            receive_timestamp_utc_ns=2,
            receive_monotonic_ns=2,
        )
        with pytest.raises(ValueError, match="escaped the frozen feed selection"):
            _require_polymarket_websocket_selection(envelope, {"order_book"})


def test_probe_append_enforces_absolute_slot_cutoff_before_raw_write(tmp_path: Path) -> None:
    cutoff = 2_000_000_000_000_000_000
    config = ProbeConfig(
        output_root=tmp_path / "collection",
        venue=Venue.POLYMARKET,
        feeds=("order_book",),
        instruments=("token-a",),
        census_limit=0,
        duration_seconds=1,
        max_bytes=1_000_000,
        max_segment_bytes=100_000,
        rotation_seconds=30,
        progress_interval_seconds=30,
        max_frames=10,
        max_segments=10,
        max_network_calls=10,
        campaign_manifest_sha256="a" * 64,
        official_contract_sha256="b" * 64,
        candidate_config_sha256="c" * 64,
        collection_cutoff_utc_ns_exclusive=cutoff,
    )
    factory = SessionEnvelopeFactory(
        venue=Venue.POLYMARKET,
        collector_identity="fixture-cutoff",
        session_identity="fixture-cutoff",
        source_metadata_version=POLYMARKET_METADATA_VERSION,
        provenance=CaptureProvenance(
            "fixture-cutoff",
            "fixture://cutoff",
            "FIXTURE",
            SYNTHETIC_FIXTURE_LABEL,
        ),
    )
    writer = ResearchSegmentWriter(
        tmp_path / "raw",
        collection_id="fixture-cutoff",
        max_segment_bytes=100_000,
        rotation_seconds=30,
        max_total_bytes=1_000_000,
    )
    counters = _Counters()

    def envelope(received_ns: int):
        return factory.make(
            feed_type="order_book",
            instrument_id="PM:token-a",
            market_id="condition",
            source_timestamp_ns=None,
            receive_timestamp_utc_ns=received_ns,
            receive_monotonic_ns=received_ns,
            raw_payload=canonical_json_bytes({"event_type": "book"}),
            source_sequence=None,
        )

    _append_probe_bounded(writer, counters, envelope(cutoff - 1), config)
    with pytest.raises(_ProbeBoundaryReached) as reached:
        _append_probe_bounded(writer, counters, envelope(cutoff), config)
    assert reached.value.terminal_health == "MAX_DURATION_REACHED"
    manifest = writer.close()
    assert manifest.frame_count == 1


def test_polymarket_disconnect_and_partial_rebootstrap_fail_closed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class Clock:
        def __init__(self) -> None:
            self.value = 100.0

        def monotonic(self) -> float:
            return self.value

        def monotonic_ns(self) -> int:
            return int(self.value * 1_000_000_000)

        def time_ns(self) -> int:
            return 1_800_000_000_000_000_000 + self.monotonic_ns()

        def sleep(self, seconds: float) -> None:
            self.value += seconds

    def new_factory(collection_id: str) -> SessionEnvelopeFactory:
        return SessionEnvelopeFactory(
            venue=Venue.POLYMARKET,
            collector_identity="fixture-polymarket-continuity",
            session_identity=collection_id,
            source_metadata_version=POLYMARKET_METADATA_VERSION,
            provenance=CaptureProvenance(
                collection_id,
                "fixture://polymarket-continuity",
                "FIXTURE",
                SYNTHETIC_FIXTURE_LABEL,
            ),
        )

    def new_writer(path: Path, collection_id: str) -> ResearchSegmentWriter:
        return ResearchSegmentWriter(
            path,
            collection_id=collection_id,
            max_segment_bytes=100_000,
            rotation_seconds=30,
            max_total_bytes=1_000_000,
        )

    config = ProbeConfig(
        output_root=tmp_path / "unused",
        venue=Venue.POLYMARKET,
        feeds=("order_book",),
        instruments=("fixture-token",),
        census_limit=0,
        duration_seconds=2,
        max_bytes=1_000_000,
        max_segment_bytes=100_000,
        rotation_seconds=30,
        progress_interval_seconds=30,
        max_frames=100,
        max_segments=10,
        max_network_calls=50,
    )
    gamma_market = {
        "clobTokenIds": ["fixture-token"],
        "conditionId": "fixture-condition",
        "events": [{"id": "fixture-event"}],
        "outcomes": ["YES"],
        "questionID": "fixture-question",
    }

    def public_http(_session, request, *, deadline, max_response_bytes, budget):
        assert probe_module.time.monotonic() < deadline
        assert max_response_bytes > 0
        budget.consume()
        query = dict(request.query)
        if "/markets-by-token/" in request.url:
            payload = {"condition_id": "fixture-condition"}
        elif request.url == f"{POLYMARKET_GAMMA_PUBLIC_URL}/markets/keyset":
            payload = {"markets": [gamma_market], "next_cursor": None}
        elif "/clob-markets/" in request.url:
            payload = {
                "condition_id": "fixture-condition",
                "tokens": [{"outcome": "YES", "token_id": "fixture-token"}],
            }
        elif request.url.endswith("/book"):
            payload = {
                "asks": [["0.42", "2"]],
                "asset_id": query["token_id"],
                "bids": [["0.40", "2"]],
                "market": "fixture-condition",
            }
        else:  # pragma: no cover - unexpected public path must be explicit
            raise AssertionError(request.url)
        return canonical_json_bytes(payload)

    class DisconnectSocket:
        def __init__(self, clock: Clock) -> None:
            self.clock = clock

        def getstatus(self) -> int:
            return 101

        def settimeout(self, _timeout: float) -> None:
            return None

        def send(self, _payload: str) -> None:
            return None

        def recv(self) -> str:
            self.clock.value += 0.5
            raise websocket.WebSocketConnectionClosedException("SYNTHETIC/FIXTURE close")

        def close(self) -> None:
            return None

    disconnect_clock = Clock()
    disconnect_attempts = {"count": 0}

    def disconnect_then_fail(_url: str, **_kwargs):
        disconnect_attempts["count"] += 1
        if disconnect_attempts["count"] == 1:
            return DisconnectSocket(disconnect_clock)
        disconnect_clock.value = 102.0
        raise websocket.WebSocketException("SYNTHETIC/FIXTURE HTTP 403")

    monkeypatch.setattr(probe_module, "time", disconnect_clock)
    monkeypatch.setattr(probe_module, "_execute_http", public_http)
    monkeypatch.setattr(websocket, "create_connection", disconnect_then_fail)
    disconnected_writer = new_writer(tmp_path / "disconnected", "fixture-disconnected")
    with pytest.raises(_ProbeBoundaryReached) as disconnected:
        _polymarket_probe(
            config,
            factory=new_factory("fixture-disconnected"),
            writer=disconnected_writer,
            counters=_Counters(),
            deadline=102.0,
            stop_requested=lambda: False,
            progress=lambda _count: None,
            session=object(),
        )
    assert disconnected.value.terminal_health == "CONTINUITY_UNKNOWN_AFTER_RECONNECT_FROZEN"
    disconnected_writer.abort()

    never_clock = Clock()

    def never_connect(_url: str, **_kwargs):
        never_clock.value = 102.0
        raise websocket.WebSocketException("SYNTHETIC/FIXTURE HTTP 403")

    monkeypatch.setattr(probe_module, "time", never_clock)
    monkeypatch.setattr(websocket, "create_connection", never_connect)
    never_writer = new_writer(tmp_path / "never", "fixture-never")
    with pytest.raises(ConnectionError, match="WebSocketException: SYNTHETIC/FIXTURE HTTP 403"):
        _polymarket_probe(
            config,
            factory=new_factory("fixture-never"),
            writer=never_writer,
            counters=_Counters(),
            deadline=102.0,
            stop_requested=lambda: False,
            progress=lambda _count: None,
            session=object(),
        )
    never_writer.abort()

    partial_clock = Clock()
    partial_attempts = {"count": 0}
    gamma_calls = {"count": 0}

    class PartialSocket(DisconnectSocket):
        def recv(self) -> str:
            partial_clock.value += 0.5
            raise websocket.WebSocketConnectionClosedException("SYNTHETIC/FIXTURE close")

    def partial_connect(_url: str, **_kwargs):
        partial_attempts["count"] += 1
        return PartialSocket(partial_clock)

    def partial_http(_session, request, *, deadline, max_response_bytes, budget):
        if request.url == f"{POLYMARKET_GAMMA_PUBLIC_URL}/markets/keyset":
            gamma_calls["count"] += 1
            return public_http(
                _session,
                request,
                deadline=deadline,
                max_response_bytes=max_response_bytes,
                budget=budget,
            )
        if gamma_calls["count"] >= 2 and "/clob-markets/" in request.url:
            partial_clock.value = 102.0
            raise TimeoutError("SYNTHETIC/FIXTURE CLOB rebootstrap timeout")
        return public_http(
            _session,
            request,
            deadline=deadline,
            max_response_bytes=max_response_bytes,
            budget=budget,
        )

    monkeypatch.setattr(probe_module, "time", partial_clock)
    monkeypatch.setattr(probe_module, "_execute_http", partial_http)
    monkeypatch.setattr(websocket, "create_connection", partial_connect)
    partial_writer = new_writer(tmp_path / "partial", "fixture-partial")
    with pytest.raises(_ProbeBoundaryReached) as partial:
        _polymarket_probe(
            config,
            factory=new_factory("fixture-partial"),
            writer=partial_writer,
            counters=_Counters(),
            deadline=102.0,
            stop_requested=lambda: False,
            progress=lambda _count: None,
            session=object(),
        )
    assert partial.value.terminal_health == "CONTINUITY_UNKNOWN_AFTER_RECONNECT_FROZEN"
    partial_manifest = partial_writer.close()
    assert partial_manifest is not None
    partial_envelopes = ResearchSegmentReader(
        tmp_path / "partial",
        manifest_sha256=partial_manifest.manifest_sha256,
    ).replay()
    assert any(item.feed_type == "heartbeat" and item.state.reconnect for item in partial_envelopes)
    assert not any(
        item.feed_type == "metadata"
        and item.provenance.source_url.startswith(f"{POLYMARKET_CLOB_PUBLIC_URL}/")
        and item.session_identity == partial_envelopes[-1].session_identity
        for item in partial_envelopes
    )


def test_offline_probe_recovery_publishes_only_authenticated_frames(tmp_path: Path) -> None:
    output = tmp_path / "probe-output"
    reports = output / "reports"
    reports.mkdir(parents=True)
    collection_id = "fixture-recovery"
    config = ProbeConfig(
        output_root=output,
        venue=Venue.HYPERLIQUID,
        feeds=("bbo",),
        instruments=("BTC",),
        census_limit=0,
        duration_seconds=120,
        max_bytes=1_000_000,
        max_segment_bytes=4096,
        rotation_seconds=30,
        progress_interval_seconds=1,
        collection_id=collection_id,
    )
    binding_payload = _probe_binding_payload(config, collection_id=collection_id)
    binding_sha256 = _probe_binding_sha256(binding_payload)
    (reports / "probe-config.json").write_text(
        json.dumps(
            {**binding_payload, "probe_binding_sha256": binding_sha256},
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    factory = _factory(
        collection_id,
        session_identity=f"probe-binding-{binding_sha256}",
    )
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
        terminal_health="PUBLIC_SOURCE_UNAVAILABLE_RECOVERED",
        error="WebSocketConnectionClosedException:fixture",
    )
    assert report.frames == 1
    assert report.manifest_sha256 == manifest.manifest_sha256
    assert "UNPUBLISHED_IN_MEMORY_TAIL_NOT_CLAIMED" in report.limitations
    saved = json.loads((reports / "result.json").read_text(encoding="utf-8"))
    assert saved["terminal_health"] == "PUBLIC_SOURCE_UNAVAILABLE_RECOVERED"
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


def test_probe_elapsed_excludes_manifest_and_session_finalization(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class FakeClock:
        def __init__(self) -> None:
            self.value = 100.0

        def monotonic(self) -> float:
            return self.value

    clock = FakeClock()

    class FakeSession:
        def close(self) -> None:
            clock.value += 5.0

    def fake_probe(_config, **_kwargs):
        clock.value += 0.75
        return ()

    monkeypatch.setattr(probe_module, "time", clock)
    monkeypatch.setattr(probe_module, "_hyperliquid_probe", fake_probe)
    report = run_public_probe(
        ProbeConfig(
            output_root=tmp_path / "finalization-clock",
            venue=Venue.HYPERLIQUID,
            feeds=("bbo",),
            instruments=("BTC",),
            census_limit=0,
            duration_seconds=1,
            max_bytes=1_000_000,
            max_segment_bytes=4096,
            rotation_seconds=30,
            progress_interval_seconds=1,
            collection_id="fixture-finalization-clock",
        ),
        http_session_factory=FakeSession,
    )
    assert report.elapsed_ms == 750
    assert clock.value == 105.75


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
                method="GET", url="https://gamma-api.polymarket.com/markets/keyset"
            ),
            deadline=time.monotonic() + 1,
            max_response_bytes=3,
        )
    assert response.closed
