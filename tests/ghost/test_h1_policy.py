from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from hyperlab.cli import app
from hyperlab.ghost.h1 import (
    ECONOMIC_NOT_AVAILABLE,
    H1_READY,
    H1PolicyConfig,
    replay_h1_research_manifest,
)
from hyperlab.research_data.adapters import (
    HYPERLIQUID_METADATA_VERSION,
    HYPERLIQUID_PUBLIC_HTTP_URL,
    HYPERLIQUID_PUBLIC_WEBSOCKET_URL,
    HyperliquidPublicAdapter,
)
from hyperlab.research_data.envelope import (
    SYNTHETIC_FIXTURE_LABEL,
    CaptureProvenance,
    SessionEnvelopeFactory,
    Venue,
)
from hyperlab.research_data.segments import ResearchSegmentWriter

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config" / "research" / "hyperliquid-h1-ghost-v1.json"
_SECOND = 1_000_000_000


def _raw(value: object) -> bytes:
    assert isinstance(value, dict)
    return json.dumps(
        {**value, "fixture_label": SYNTHETIC_FIXTURE_LABEL},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _manifest(
    tmp_path: Path,
    *,
    with_gap: bool = False,
    public_source_url: str | None = None,
) -> tuple[Path, str]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    collection_id = "h1-synthetic-collection"
    synthetic = public_source_url is None
    if synthetic:
        default_provenance = CaptureProvenance(
            collection_id=collection_id,
            source_url="fixture://hyperliquid/h1",
            transport="FIXTURE",
            fixture_label=SYNTHETIC_FIXTURE_LABEL,
        )
    else:
        assert public_source_url is not None
        default_provenance = CaptureProvenance(
            collection_id=collection_id,
            source_url=public_source_url,
            transport="PUBLIC_WEBSOCKET",
        )
    factory = SessionEnvelopeFactory(
        venue=Venue.HYPERLIQUID,
        collector_identity="h1-synthetic-collector",
        session_identity="h1-synthetic-session",
        source_metadata_version=(
            "h1-synthetic-metadata-v1" if synthetic else HYPERLIQUID_METADATA_VERSION
        ),
        provenance=default_provenance,
    )
    adapter = HyperliquidPublicAdapter()
    http_provenance = (
        None
        if synthetic
        else CaptureProvenance(
            collection_id=collection_id,
            source_url=HYPERLIQUID_PUBLIC_HTTP_URL,
            transport="PUBLIC_HTTP",
        )
    )
    envelopes = [
        adapter.envelope_from_http(
            _raw(
                {
                    "payload": [
                        {"universe": [{"name": "BTC", "szDecimals": 3}]},
                        [{}],
                    ]
                }
            ).replace(b'{"fixture_label":"SYNTHETIC/FIXTURE","payload":', b"")[:-1],
            feed_type="metadata",
            instrument=None,
            factory=factory,
            receive_timestamp_utc_ns=1 * _SECOND,
            receive_monotonic_ns=1 * _SECOND,
            provenance=http_provenance,
        ),
        adapter.envelope_from_websocket(
            _raw(
                {
                    "channel": "activeAssetCtx",
                    "data": {
                        "coin": "BTC",
                        "ctx": {
                            "dayNtlVlm": "100000000",
                            "funding": "0.00001",
                            "markPx": "100.1",
                            "midPx": "100.1",
                            "openInterest": "10000",
                            "oraclePx": "100.1",
                        },
                    },
                }
            ),
            factory=factory,
            receive_timestamp_utc_ns=2 * _SECOND,
            receive_monotonic_ns=2 * _SECOND,
        ),
        adapter.envelope_from_websocket(
            _raw(
                {
                    "channel": "bbo",
                    "data": {
                        "bbo": [
                            {"n": 5, "px": "100.0", "sz": "10"},
                            {"n": 2, "px": "100.2", "sz": "2"},
                        ],
                        "coin": "BTC",
                        "time": 3_000,
                    },
                }
            ),
            factory=factory,
            receive_timestamp_utc_ns=3 * _SECOND,
            receive_monotonic_ns=3 * _SECOND,
        ),
        adapter.envelope_from_websocket(
            _raw(
                {
                    "channel": "trades",
                    "data": [
                        {
                            "coin": "BTC",
                            "px": "100.2",
                            "side": "B",
                            "sz": "2",
                            "tid": 1,
                            "time": 3_500,
                        }
                    ],
                }
            ),
            factory=factory,
            receive_timestamp_utc_ns=3_500_000_000,
            receive_monotonic_ns=3_500_000_000,
        ),
    ]
    if with_gap:
        envelopes.append(
            factory.make(
                feed_type="heartbeat",
                instrument_id="HL:GLOBAL:public",
                market_id=None,
                source_timestamp_ns=None,
                receive_timestamp_utc_ns=3_750_000_000,
                receive_monotonic_ns=3_750_000_000,
                raw_payload=_raw({"channel": "pong"}),
                explicit_gap_detected=True,
                explicit_gap_reason="SYNTHETIC_SOURCE_GAP",
            )
        )
    envelopes.extend(
        [
            adapter.envelope_from_websocket(
                _raw(
                    {
                        "channel": "l2Book",
                        "data": {
                            "coin": "BTC",
                            "levels": [
                                [
                                    {"n": 5, "px": "100.0", "sz": "10"},
                                    {"n": 2, "px": "99.9", "sz": "4"},
                                ],
                                [
                                    {"n": 2, "px": "100.2", "sz": "2"},
                                    {"n": 3, "px": "100.3", "sz": "4"},
                                ],
                            ],
                            "time": 4_000,
                        },
                    }
                ),
                factory=factory,
                receive_timestamp_utc_ns=4 * _SECOND,
                receive_monotonic_ns=4 * _SECOND,
            ),
            adapter.envelope_from_websocket(
                _raw(
                    {
                        "channel": "trades",
                        "data": [
                            {
                                "coin": "BTC",
                                "px": "100.0",
                                "side": "A",
                                "sz": "11",
                                "tid": 2,
                                "time": 5_100,
                            }
                        ],
                    }
                ),
                factory=factory,
                receive_timestamp_utc_ns=5_100_000_000,
                receive_monotonic_ns=5_100_000_000,
            ),
            adapter.envelope_from_websocket(
                _raw(
                    {
                        "channel": "bbo",
                        "data": {
                            "bbo": [
                                {"n": 4, "px": "101.0", "sz": "5"},
                                {"n": 4, "px": "101.2", "sz": "5"},
                            ],
                            "coin": "BTC",
                            "time": 124_100,
                        },
                    }
                ),
                factory=factory,
                receive_timestamp_utc_ns=124_100_000_000,
                receive_monotonic_ns=124_100_000_000,
            ),
            adapter.envelope_from_websocket(
                _raw(
                    {
                        "channel": "l2Book",
                        "data": {
                            "coin": "BTC",
                            "levels": [
                                [{"n": 4, "px": "101.0", "sz": "5"}],
                                [{"n": 4, "px": "101.2", "sz": "5"}],
                            ],
                            "time": 124_100,
                        },
                    }
                ),
                factory=factory,
                receive_timestamp_utc_ns=124_100_000_001,
                receive_monotonic_ns=124_100_000_001,
            ),
        ]
    )
    raw_root = tmp_path / "raw"
    writer = ResearchSegmentWriter(
        raw_root,
        collection_id=collection_id,
        max_segment_bytes=1_000_000,
        rotation_seconds=300,
        max_total_bytes=2_000_000,
    )
    for envelope in envelopes:
        writer.append(envelope)
    manifest = writer.close()
    assert manifest is not None
    return raw_root, manifest.manifest_sha256


def test_h1_public_manifest_replay_is_deterministic_and_economically_honest(
    tmp_path: Path,
) -> None:
    raw_root, manifest = _manifest(tmp_path)
    config = H1PolicyConfig.from_path(CONFIG)

    first = replay_h1_research_manifest(raw_root, manifest, config=config)
    second = replay_h1_research_manifest(raw_root, manifest, config=config)

    assert first.canonical_bytes() == second.canonical_bytes()
    assert first.technical_verdict == H1_READY
    assert first.economic_status == ECONOMIC_NOT_AVAILABLE
    assert first.synthetic is True
    assert [item.latency_ms for item in first.latency_reports] == [100, 250, 500, 1_000]
    hurdle = next(item for item in first.latency_reports if item.latency_ms == 500)
    assert hurdle.role == "PRIMARY_HURDLE"
    assert hurdle.ghost.provenance.synthetic is True
    assert hurdle.ghost.pnl.reconciliation_difference == 0
    assert hurdle.attribution.reconciliation_difference == 0
    assert any(item.action.value == "BID_ONLY" for item in hurdle.decisions)
    assert all(len(item.markouts) == 6 for item in hurdle.decisions)
    assert 120_000 in {markout.horizon_ms for item in hurdle.decisions for markout in item.markouts}
    quoted = next(item for item in hurdle.decisions if item.action.value == "BID_ONLY")
    assert next(item for item in quoted.markouts if item.horizon_ms == 100).markout_bps is None
    assert next(item for item in quoted.markouts if item.horizon_ms == 120_000).markout_bps is not None
    assert hurdle.economic_gates["minimum_fills_5000"] is False
    assert hurdle.concentration.by_instrument
    assert hurdle.concentration.conservative_fills == 1
    assert hurdle.concentration.completed_inventory_matches == 1


def test_gap_closes_h1_action_window_fail_closed(tmp_path: Path) -> None:
    raw_root, manifest = _manifest(tmp_path, with_gap=True)
    report = replay_h1_research_manifest(
        raw_root,
        manifest,
        config=H1PolicyConfig.from_path(CONFIG),
    )
    hurdle = next(item for item in report.latency_reports if item.latency_ms == 500)
    assert all(item.action.value == "NO_QUOTE" for item in hurdle.decisions)
    assert "SOURCE_GAP_OR_RECONNECT" in {item.reason for item in hurdle.decisions}
    assert hurdle.ghost.orders == ()


def test_public_h1_requires_the_pinned_official_source_provenance(tmp_path: Path) -> None:
    official_root, official_manifest = _manifest(
        tmp_path / "official",
        public_source_url=HYPERLIQUID_PUBLIC_WEBSOCKET_URL,
    )
    report = replay_h1_research_manifest(
        official_root,
        official_manifest,
        config=H1PolicyConfig.from_path(CONFIG),
    )
    assert report.synthetic is False

    other_root, other_manifest = _manifest(
        tmp_path / "other",
        public_source_url="https://example.invalid/public-ws",
    )
    with pytest.raises(ValueError, match="official pinned Hyperliquid source"):
        replay_h1_research_manifest(
            other_root,
            other_manifest,
            config=H1PolicyConfig.from_path(CONFIG),
        )


def test_h1_config_freezes_primary_and_losing_variants_before_observation() -> None:
    config = H1PolicyConfig.from_path(CONFIG)
    assert config.primary_variant_id == "imbalance-flow-confirm-v1"
    assert {item.status for item in config.variants} == {
        "PRIMARY_FROZEN_UNOBSERVED",
        "REGISTERED_UNOBSERVED",
    }
    assert all(item.holdout_access == "SEALED" for item in config.variants)
    assert config.queue_cancellation_ahead_credit == ("0", "0.25", "0.50")
    assert config.latency_scenarios_ms == (100, 250, 500, 1_000)

    weakened = json.loads(CONFIG.read_text(encoding="utf-8"))
    weakened["economic_gates"]["minimum_fills"] = 1
    try:
        H1PolicyConfig.from_bytes(json.dumps(weakened).encode())
    except ValueError as error:
        assert "economic hurdles" in str(error)
    else:
        raise AssertionError("a weakened prospective hurdle must fail closed")


def test_h1_cli_is_offline_and_writes_the_same_canonical_report(tmp_path: Path) -> None:
    raw_root, manifest = _manifest(tmp_path / "input")
    output = tmp_path / "h1-report.json"
    result = CliRunner().invoke(
        app,
        [
            "ghost",
            "h1-replay",
            "--research-root",
            str(raw_root),
            "--manifest-sha256",
            manifest,
            "--config",
            str(CONFIG),
            "--output",
            str(output),
        ],
    )
    assert result.exit_code == 0, result.output
    assert H1_READY in result.output
    assert ECONOMIC_NOT_AVAILABLE in result.output
    expected = replay_h1_research_manifest(
        raw_root, manifest, config=H1PolicyConfig.from_path(CONFIG)
    )
    assert output.read_bytes() == expected.canonical_bytes() + b"\n"
