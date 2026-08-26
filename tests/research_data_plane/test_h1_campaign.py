from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import hyperlab.research_data.h1_campaign as campaign_module
from hyperlab.research_data.h1_campaign import collect_h1_campaign, prepare_h1_campaign
from hyperlab.research_data.segments import ResearchSegmentReader

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config" / "research" / "hyperliquid-h1-ghost-v1.json"
FEES = ROOT / "config" / "paper" / "hyperliquid-tier0-fees-2026-08-16.json"


def test_campaign_prepare_freezes_policy_fees_holdout_and_operator_blocks(
    tmp_path: Path, monkeypatch
) -> None:
    starts = datetime(2026, 9, 1, tzinfo=UTC)
    monkeypatch.setattr(campaign_module, "_utc_now", lambda: starts - timedelta(hours=1))
    result = prepare_h1_campaign(
        tmp_path / "campaign",
        config_path=CONFIG,
        fee_artifact_path=FEES,
        starts_at_utc=starts,
        fee_reviewed_at_utc=starts - timedelta(hours=1),
    )

    manifest_path = tmp_path / "campaign" / "campaign-manifest.json"
    raw = manifest_path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == result.manifest_sha256
    manifest = json.loads(raw)
    assert manifest["boundary"] == "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
    assert manifest["policy_config_sha256"] == result.policy_config_sha256
    assert manifest["fee_artifact_sha256"] == hashlib.sha256(FEES.read_bytes()).hexdigest()
    assert manifest["holdout"]["access"] == "SEALED_UNTIL_COLLECTION_COMPLETE"
    assert manifest["holdout"]["starts_at_utc"] == "2026-09-11T00:00:00Z"
    assert manifest["collection"]["minimum_days"] == 7
    assert manifest["collection"]["maximum_days"] == 14
    assert (tmp_path / "campaign" / "operator" / "windows-powershell.txt").is_file()
    assert (tmp_path / "campaign" / "operator" / "tabby-vps-bash.txt").is_file()
    assert "NO_VPS_COMMAND_EXECUTED_BY_CODEX" in (
        tmp_path / "campaign" / "operator" / "tabby-vps-bash.txt"
    ).read_text(encoding="utf-8")

    with pytest.raises(FileExistsError):
        prepare_h1_campaign(
            tmp_path / "campaign",
            config_path=CONFIG,
            fee_artifact_path=FEES,
            starts_at_utc=starts,
            fee_reviewed_at_utc=starts - timedelta(hours=1),
        )


def test_campaign_prepare_refuses_stale_or_post_start_fee_review(
    tmp_path: Path, monkeypatch
) -> None:
    starts = datetime(2026, 9, 1, tzinfo=UTC)
    monkeypatch.setattr(campaign_module, "_utc_now", lambda: starts)
    with pytest.raises(ValueError, match="fee review"):
        prepare_h1_campaign(
            tmp_path / "late-review",
            config_path=CONFIG,
            fee_artifact_path=FEES,
            starts_at_utc=starts,
            fee_reviewed_at_utc=starts + timedelta(seconds=1),
        )
    with pytest.raises(ValueError, match="24 hours"):
        prepare_h1_campaign(
            tmp_path / "stale-review",
            config_path=CONFIG,
            fee_artifact_path=FEES,
            starts_at_utc=starts,
            fee_reviewed_at_utc=starts - timedelta(days=2),
        )


def test_campaign_collection_resumes_the_same_authenticated_chain(
    tmp_path: Path, monkeypatch
) -> None:
    clock = [datetime(2026, 9, 1, tzinfo=UTC)]
    monkeypatch.setattr(campaign_module, "_utc_now", lambda: clock[0])
    starts = clock[0] + timedelta(hours=1)
    root = tmp_path / "resumable"
    prepare_h1_campaign(
        root,
        config_path=CONFIG,
        fee_artifact_path=FEES,
        starts_at_utc=starts,
        fee_reviewed_at_utc=clock[0],
    )
    clock[0] = starts + timedelta(hours=1)

    class Session:
        def close(self) -> None:
            return None

    def fake_public_collection(_config, **kwargs):
        factory = kwargs["factory"]
        writer = kwargs["writer"]
        envelope = factory.make(
            feed_type="heartbeat",
            instrument_id="HL:GLOBAL:public",
            market_id=None,
            source_timestamp_ns=None,
            receive_timestamp_utc_ns=writer.frame_count + 1,
            receive_monotonic_ns=writer.frame_count + 1,
            raw_payload=b'{"channel":"pong"}',
        )
        writer.append(envelope)
        kwargs["progress"](writer.frame_count)
        return ()

    monkeypatch.setattr(campaign_module, "_default_http_session", Session)
    monkeypatch.setattr(campaign_module, "_hyperliquid_probe", fake_public_collection)
    first = collect_h1_campaign(root, config_path=CONFIG, resume=False)
    second = collect_h1_campaign(root, config_path=CONFIG, resume=True)

    assert first["frames"] == 1
    assert second["frames"] == 2
    reader = ResearchSegmentReader(
        root / "raw", manifest_sha256=str(second["manifest_sha256"])
    )
    envelopes = reader.replay()
    assert [item.arrival_sequence for item in envelopes] == [1, 2]
    assert envelopes[1].state.reconnect is True
    assert len({item.provenance.collection_id for item in envelopes}) == 1


def test_campaign_prepare_refuses_naive_or_retroactive_timestamps(
    tmp_path: Path, monkeypatch
) -> None:
    now = datetime(2026, 9, 1, tzinfo=UTC)
    monkeypatch.setattr(campaign_module, "_utc_now", lambda: now)
    with pytest.raises(ValueError, match="timezone-aware"):
        prepare_h1_campaign(
            tmp_path / "naive",
            config_path=CONFIG,
            fee_artifact_path=FEES,
            starts_at_utc=datetime(2026, 9, 2),
            fee_reviewed_at_utc=now,
        )
    with pytest.raises(ValueError, match="frozen before"):
        prepare_h1_campaign(
            tmp_path / "retroactive",
            config_path=CONFIG,
            fee_artifact_path=FEES,
            starts_at_utc=now - timedelta(seconds=1),
            fee_reviewed_at_utc=now - timedelta(hours=1),
        )
