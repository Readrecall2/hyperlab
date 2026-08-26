from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from hyperlab.cli import app
from hyperlab.collector import websocket as websocket_module
from hyperlab.collector.websocket import UrlWebsocketClientFactory
from hyperlab.research_data import lighter as lighter_module
from hyperlab.research_data.adapters import (
    PublicHttpRequest,
    PublicWebsocketSubscription,
    all_public_route_specs,
)
from hyperlab.research_data.canonical import canonical_json_bytes
from hyperlab.research_data.envelope import (
    SYNTHETIC_FIXTURE_LABEL,
    CaptureProvenance,
    SessionEnvelopeFactory,
    Venue,
)
from hyperlab.research_data.lighter import (
    LIGHTER_DOCUMENTARY_CONTRACT,
    LIGHTER_METADATA_VERSION,
    LIGHTER_PUBLIC_HTTP_URL,
    LIGHTER_PUBLIC_WEBSOCKET_URL,
    LighterPublicAdapter,
    lighter_market_census,
)
from hyperlab.research_data.lighter_report import (
    LIGHTER_GREEN,
    build_lighter_probe_report,
)
from hyperlab.research_data.probe import (
    ProbeConfig,
    _append_lighter_bounded,
    _Counters,
    _default_lighter_http_session,
    _ProbeBoundaryReached,
)
from hyperlab.research_data.segments import (
    ResearchDataIntegrityError,
    ResearchSegmentWriter,
)

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "research_data"


def _raw(name: str) -> bytes:
    value = (FIXTURES / name).read_bytes()
    assert SYNTHETIC_FIXTURE_LABEL.encode() in value
    return value


def _factory() -> SessionEnvelopeFactory:
    return SessionEnvelopeFactory(
        venue=Venue.LIGHTER,
        collector_identity="fixture-lighter-public-probe-v1",
        session_identity="fixture-lighter-session",
        source_metadata_version=LIGHTER_METADATA_VERSION,
        provenance=CaptureProvenance(
            "fixture-lighter-collection",
            "fixture://lighter",
            "FIXTURE",
            SYNTHETIC_FIXTURE_LABEL,
        ),
    )


def _envelope(adapter: LighterPublicAdapter, factory: SessionEnvelopeFactory, name: str, at: int):
    return adapter.envelope_from_websocket(
        _raw(name),
        factory=factory,
        receive_timestamp_utc_ns=at,
        receive_monotonic_ns=at,
    )


def test_lighter_public_metadata_and_subscriptions_are_exactly_allowlisted() -> None:
    adapter = LighterPublicAdapter()
    requests = adapter.public_http_requests(feeds=("metadata",), market_indices=(0, 1))
    assert requests == (
        PublicHttpRequest(
            method="GET",
            url=f"{LIGHTER_PUBLIC_HTTP_URL}/orderBooks",
            query=(("filter", "all"),),
        ),
    )
    subscriptions = adapter.websocket_subscriptions(
        feeds=("order_book", "ticker", "market_stats", "trades"),
        market_indices=(0,),
    )
    assert {item.payload["channel"] for item in subscriptions} == {
        "order_book/0",
        "ticker/0",
        "market_stats/0",
        "trade/0",
    }
    assert all(item.url == LIGHTER_PUBLIC_WEBSOCKET_URL for item in subscriptions)
    assert all(set(item.payload) == {"channel", "type"} for item in subscriptions)
    assert all(item.payload["type"] == "subscribe" for item in subscriptions)


def test_lighter_market_census_preserves_public_precision_and_fee_strings() -> None:
    markets = lighter_market_census(_raw("lighter_order_books.json"), limit=2)
    assert [item.market_id for item in markets] == [0, 1]
    assert markets[0].symbol == "ETH"
    assert markets[0].supported_price_decimals == 2
    assert markets[0].supported_size_decimals == 4
    assert markets[0].maker_fee == "0.00000"
    assert markets[0].taker_fee == "0.00000"


def test_lighter_order_book_continuity_uses_begin_nonce_not_offset_arithmetic() -> None:
    adapter = LighterPublicAdapter()
    factory = _factory()
    first = _envelope(adapter, factory, "lighter_order_book_snapshot.json", 1774884082327000000)
    second = _envelope(adapter, factory, "lighter_order_book_delta.json", 1774884082427000000)

    assert first.feed_type == "order_book"
    assert first.source_sequence == 918
    assert first.source_cursor == "begin_nonce=900;offset=100"
    assert first.source_timestamp_ns == 1774884082309144000
    assert first.state.gap_detected is False
    assert second.source_sequence == 920
    assert second.source_cursor == "begin_nonce=918;offset=101"
    assert second.state.gap_detected is False

    gap = _envelope(adapter, factory, "lighter_order_book_gap.json", 1774884082527000000)
    assert gap.state.gap_detected is True
    assert gap.state.reason == "LIGHTER_BEGIN_NONCE_MISMATCH"
    assert adapter.frozen_markets == frozenset({0})
    with pytest.raises(ValueError, match="LIGHTER_CONTINUITY_FROZEN"):
        _envelope(adapter, factory, "lighter_order_book_delta.json", 1774884082627000000)


def test_lighter_duplicate_and_reconnect_epoch_are_explicit_without_invented_gap() -> None:
    adapter = LighterPublicAdapter()
    factory = _factory()
    first = _envelope(adapter, factory, "lighter_order_book_snapshot.json", 1774884082327000000)
    duplicate = _envelope(adapter, factory, "lighter_order_book_snapshot.json", 1774884082327000001)
    assert duplicate.state.duplicate is True
    assert duplicate.state.gap_detected is False
    assert duplicate.session_identity == first.session_identity

    factory.begin_reconnect()
    adapter.begin_connection_epoch()
    reconnected = _envelope(
        adapter,
        factory,
        "lighter_order_book_snapshot.json",
        1774884082327000002,
    )
    assert reconnected.state.reconnect is True
    assert reconnected.state.gap_detected is False
    assert reconnected.session_identity.endswith(":1")


@pytest.mark.parametrize(
    ("name", "feed", "source_sequence", "source_timestamp_ns"),
    (
        ("lighter_ticker.json", "ticker", 9182249734, 1774883844921166000),
        ("lighter_market_stats.json", "market_stats", None, 1786372092759000000),
        ("lighter_trades.json", "trades", 8630448841, 1773854156654000000),
    ),
)
def test_lighter_public_channel_envelopes_preserve_server_and_receive_times(
    name: str,
    feed: str,
    source_sequence: int | None,
    source_timestamp_ns: int,
) -> None:
    envelope = _envelope(LighterPublicAdapter(), _factory(), name, source_timestamp_ns + 10_000_000)
    assert envelope.feed_type == feed
    assert envelope.market_id == "LIGHTER:MARKET:0"
    assert envelope.source_sequence == source_sequence
    assert envelope.source_timestamp_ns == source_timestamp_ns
    assert envelope.receive_monotonic_ns == source_timestamp_ns + 10_000_000


def test_lighter_documentary_latency_values_are_scenarios_not_measurements() -> None:
    assert LIGHTER_DOCUMENTARY_CONTRACT["capture_date"] == "2026-08-26"
    assert LIGHTER_DOCUMENTARY_CONTRACT["scope"] == "DOCUMENTARY_NOT_OBSERVED_ACCOUNT_ACCESS"
    assert LIGHTER_DOCUMENTARY_CONTRACT["comparable_scenarios_ms"] == [100, 250, 500, 1000]
    assert LIGHTER_DOCUMENTARY_CONTRACT["scenario_classification"] == (
        "VERSIONED_HYPOTHETICAL_NOT_ACCOUNT_OBSERVATION"
    )
    contract_file = (ROOT / "config" / "lighter-public-contract-v1.json").read_bytes()
    assert contract_file.endswith(b"\n")
    assert canonical_json_bytes(json.loads(contract_file)) == canonical_json_bytes(
        LIGHTER_DOCUMENTARY_CONTRACT
    )


def test_lighter_capability_audit_has_no_private_or_transaction_surface() -> None:
    lighter_specs = [
        spec
        for spec in all_public_route_specs()
        if "zklighter.elliot.ai" in spec.url
    ]
    assert lighter_specs
    assert any(isinstance(spec, PublicHttpRequest) for spec in lighter_specs)
    assert any(isinstance(spec, PublicWebsocketSubscription) for spec in lighter_specs)
    for spec in lighter_specs:
        serialized = json.dumps(
            getattr(spec, "payload", {}), sort_keys=True
        ).lower()
        assert getattr(spec, "method", "GET") == "GET"
        assert "auth" not in serialized
        assert "account" not in serialized
        assert "position" not in serialized
        assert "sendtx" not in serialized

    tree = ast.parse(Path(lighter_module.__file__).read_text(encoding="utf-8"))
    imports = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    imported_names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert not any(module == "lighter" or module.startswith("lighter.") for module in imports)
    assert "lighter" not in imported_names
    assert "SignerClient" not in imported_names
    assert "Exchange" not in imported_names
    assert "?readonly=true" not in Path(lighter_module.__file__).read_text(encoding="utf-8")

    with pytest.raises(ValueError, match="restricted to documented public market metadata"):
        PublicHttpRequest(
            method="GET",
            url="https://mainnet.zklighter.elliot.ai/api/v1/accountActiveOrders",
        )
    with pytest.raises(ValueError, match="not an allowlisted public channel"):
        PublicWebsocketSubscription(
            url=LIGHTER_PUBLIC_WEBSOCKET_URL,
            payload={"channel": "account_all/1", "type": "subscribe"},
        )


def test_lighter_default_transports_disable_environment_proxies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class PausedConnection:
        def settimeout(self, _timeout_seconds: float) -> None:
            pass

        def close(self) -> None:
            observed["closed"] = True

    def open_direct(
        url: str,
        timeout_seconds: float,
        *,
        allow_environment_proxy: bool = True,
    ) -> PausedConnection:
        observed.update(
            url=url,
            timeout_seconds=timeout_seconds,
            allow_environment_proxy=allow_environment_proxy,
        )
        return PausedConnection()

    monkeypatch.setattr(websocket_module, "_open_public_websocket", open_direct)
    factory = UrlWebsocketClientFactory(
        LIGHTER_PUBLIC_WEBSOCKET_URL,
        allow_environment_proxy=False,
    )
    socket = factory.connect_paused("public", 1.0)
    socket.close()
    assert observed == {
        "allow_environment_proxy": False,
        "closed": True,
        "timeout_seconds": 1.0,
        "url": LIGHTER_PUBLIC_WEBSOCKET_URL,
    }

    session = _default_lighter_http_session()
    try:
        assert vars(session)["trust_env"] is False
    finally:
        session.close()


def _bounded_config(output_root: Path, *, max_frames: int = 5000, max_segments: int = 4) -> ProbeConfig:
    return ProbeConfig(
        output_root=output_root,
        venue=Venue.LIGHTER,
        feeds=("metadata", "order_book", "ticker", "market_stats", "trades"),
        instruments=("0",),
        census_limit=0,
        duration_seconds=600,
        max_bytes=64 * 1024 * 1024,
        max_segment_bytes=16 * 1024 * 1024,
        rotation_seconds=150,
        progress_interval_seconds=10,
        collection_id="fixture-lighter-bounded",
        max_frames=max_frames,
        max_segments=max_segments,
    )


def test_lighter_frame_and_segment_thresholds_stop_before_exceeding_bounds(
    tmp_path: Path,
) -> None:
    config = _bounded_config(tmp_path / "unused", max_frames=1, max_segments=1)
    factory = _factory()
    adapter = LighterPublicAdapter()
    writer = ResearchSegmentWriter(
        tmp_path / "frames",
        collection_id="fixture-lighter-collection",
        max_segment_bytes=16 * 1024 * 1024,
        rotation_seconds=150,
        max_total_bytes=64 * 1024 * 1024,
    )
    envelope = adapter.envelope_from_http(
        _raw("lighter_order_books.json"),
        factory=factory,
        receive_timestamp_utc_ns=1_800_000_000_000_000_000,
        receive_monotonic_ns=1,
    )
    with pytest.raises(_ProbeBoundaryReached) as reached:
        _append_lighter_bounded(writer, _Counters(), envelope, config)
    assert reached.value.terminal_health == "MAX_FRAMES_REACHED"
    assert writer.frame_count == 1
    assert writer.close() is not None

    segment_config = _bounded_config(
        tmp_path / "unused-segments", max_frames=5000, max_segments=1
    )
    segment_writer = ResearchSegmentWriter(
        tmp_path / "segments",
        collection_id="fixture-lighter-collection",
        max_segment_bytes=16 * 1024 * 1024,
        rotation_seconds=0.000000001,
        max_total_bytes=64 * 1024 * 1024,
    )
    first_factory = _factory()
    first = adapter.envelope_from_http(
        _raw("lighter_order_books.json"),
        factory=first_factory,
        receive_timestamp_utc_ns=1_800_000_000_000_000_000,
        receive_monotonic_ns=1,
    )
    _append_lighter_bounded(segment_writer, _Counters(), first, segment_config)
    second = _envelope(
        adapter,
        first_factory,
        "lighter_ticker.json",
        1_800_000_000_000_000_001,
    )
    with pytest.raises(_ProbeBoundaryReached) as reached_segment:
        _append_lighter_bounded(segment_writer, _Counters(), second, segment_config)
    assert reached_segment.value.terminal_health == "MAX_SEGMENTS_REACHED"
    assert segment_writer.segment_count == 1
    assert segment_writer.pending_frame_count == 0
    segment_writer.close()


def _complete_probe_output(tmp_path: Path) -> Path:
    output = tmp_path / "lighter-probe"
    reports = output / "reports"
    reports.mkdir(parents=True)
    raw_root = output / "raw"
    factory = _factory()
    adapter = LighterPublicAdapter()
    writer = ResearchSegmentWriter(
        raw_root,
        collection_id="fixture-lighter-collection",
        max_segment_bytes=1_000_000,
        rotation_seconds=150,
        max_total_bytes=64 * 1024 * 1024,
    )
    envelopes = [
        adapter.envelope_from_http(
            _raw("lighter_order_books.json"),
            factory=factory,
            receive_timestamp_utc_ns=1_800_000_000_000_000_000,
            receive_monotonic_ns=1,
        )
    ]
    for index, name in enumerate(
        (
            "lighter_order_book_snapshot.json",
            "lighter_ticker.json",
            "lighter_market_stats.json",
            "lighter_trades.json",
        ),
        start=2,
    ):
        envelopes.append(
            adapter.envelope_from_websocket(
                _raw(name),
                factory=factory,
                receive_timestamp_utc_ns=1_800_000_000_000_000_000 + index,
                receive_monotonic_ns=index,
            )
        )
    for envelope in envelopes:
        writer.append(envelope)
    manifest = writer.close()
    assert manifest is not None
    result = {
        "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        "bytes": manifest.stored_segment_bytes,
        "collection_id": "fixture-lighter-collection",
        "duplicates": 0,
        "elapsed_ms": 1000,
        "error": None,
        "frames": len(envelopes),
        "gaps": 0,
        "limitations": [],
        "manifest_sha256": manifest.manifest_sha256,
        "queue_high_water": 1,
        "reconnects": 0,
        "requested_duration_seconds": 600,
        "root_sha256": manifest.root_sha256,
        "schema_version": 1,
        "segments": len(manifest.segments),
        "source_timestamp_max_ns": 1786372092759000000,
        "source_timestamp_min_ns": 1773854156654000000,
        "terminal_health": "MAX_FRAMES_REACHED",
        "venue": "lighter",
    }
    config = {
        "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        "census_limit": 0,
        "duration_seconds": 600,
        "feeds": ["metadata", "order_book", "ticker", "market_stats", "trades"],
        "instruments": ["0"],
        "max_bytes": 64 * 1024 * 1024,
        "max_frames": 5000,
        "max_segment_bytes": 16 * 1024 * 1024,
        "max_segments": 4,
        "progress_interval_seconds": "10",
        "rotation_seconds": "150",
        "schema_version": 1,
        "venue": "lighter",
    }
    (reports / "result.json").write_bytes(canonical_json_bytes(result))
    (reports / "probe-config.json").write_bytes(canonical_json_bytes(config))
    return output


def test_lighter_report_authenticates_recovery_and_separates_observed_from_documentary(
    tmp_path: Path,
) -> None:
    output = _complete_probe_output(tmp_path)
    report = build_lighter_probe_report(output)
    assert report["verdict"] == LIGHTER_GREEN
    assert report["raw_evidence"]["offline_recovery"] == (
        "PASS_EXPLICIT_MANIFEST_FULL_REPLAY"
    )
    assert report["access"]["channels"]["trades"][
        "accessible_without_auth_in_this_probe"
    ] is True
    assert report["documentary_contract"]["account_tier_access_observed"] is False
    assert report["documentary_contract"]["frozen_contract"]["account_types"][
        "standard"
    ]["documented_taker_latency_ms"] == 300
    assert report["documentary_contract"]["versioned_comparable_scenarios_ms"] == [
        100,
        250,
        500,
        1000,
    ]
    assert report["metadata_and_public_fees_observed"][0]["symbol"] == "ETH"
    assert report["raw_evidence"]["replay_gap_count"] == 0


def test_lighter_report_fails_closed_on_terminal_counter_divergence(
    tmp_path: Path,
) -> None:
    output = _complete_probe_output(tmp_path)
    result_path = output / "reports" / "result.json"
    result = json.loads(result_path.read_bytes())
    result["duplicates"] = 1
    result_path.write_bytes(canonical_json_bytes(result))
    with pytest.raises(
        ValueError, match="offline recovery duplicate count differs"
    ):
        build_lighter_probe_report(output)


def test_lighter_report_fails_closed_on_raw_segment_corruption(tmp_path: Path) -> None:
    output = _complete_probe_output(tmp_path)
    segment = next((output / "raw" / "segments").glob("*.rdpseg"))
    damaged = bytearray(segment.read_bytes())
    damaged[-20] ^= 1
    segment.write_bytes(bytes(damaged))
    with pytest.raises(ResearchDataIntegrityError):
        build_lighter_probe_report(output)


def test_lighter_research_only_cli_exposes_all_four_probe_bounds_and_offline_report() -> None:
    runner = CliRunner()
    probe_help = runner.invoke(app, ["research-data", "probe", "--help"])
    assert probe_help.exit_code == 0
    for option in ("--duration-seconds", "--max-frames", "--max-bytes", "--max-segments"):
        assert option in probe_help.output
    report_help = runner.invoke(app, ["research-data", "lighter-report", "--help"])
    assert report_help.exit_code == 0
    assert "strictement offline" in report_help.output


def test_six_hundred_second_window_is_lighter_only(tmp_path: Path) -> None:
    lighter = _bounded_config(tmp_path / "lighter")
    assert lighter.duration_seconds == 600
    with pytest.raises(
        ValueError,
        match=r"hyperliquid probe duration must be within 1\.\.300 seconds",
    ):
        ProbeConfig(
            output_root=tmp_path / "hyperliquid",
            venue=Venue.HYPERLIQUID,
            feeds=("metadata",),
            instruments=("BTC",),
            census_limit=0,
            duration_seconds=600,
            max_bytes=64 * 1024 * 1024,
            max_segment_bytes=16 * 1024 * 1024,
            rotation_seconds=30,
            progress_interval_seconds=10,
        )
