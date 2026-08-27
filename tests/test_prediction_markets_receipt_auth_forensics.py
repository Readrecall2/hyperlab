from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

import hyperlab.research_data.probe as probe_module
from hyperlab.research_data.canonical import canonical_json_bytes
from hyperlab.research_data.envelope import (
    SYNTHETIC_FIXTURE_LABEL,
    CaptureProvenance,
    Venue,
)
from hyperlab.research_data.prediction_candidate import CandidatePreregistration
from hyperlab.research_data.prediction_contracts import OfficialPublicContract
from hyperlab.research_data.segments import ResearchDataCapacityError
from ops.prediction_markets_launch_v1 import build_receipt_forensic_pack as pack_builder
from ops.prediction_markets_launch_v1 import receipt_auth_forensics as forensics

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE_PACK = (
    ROOT
    / "ops"
    / "prediction_markets_candidate_v1"
    / "prediction-markets-v1-20260901t000000z-aa60c0ff"
)
RUN_SLUG = "pm-20260827t131512z-6f59caae"
SOURCE_COMMIT = "6f59caae46e7f473cee9dec00103f4157920f8cb"
FORENSIC_SLUG = "receipt-auth-20260827t133835z-6f59caae"


def _tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix().encode()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        if path.is_file():
            raw = path.read_bytes()
            digest.update(len(raw).to_bytes(8, "big"))
            digest.update(raw)
        else:
            digest.update(b"DIR")
    return digest.hexdigest()


def _synthetic_capacity_transport(config, **kwargs):
    factory = kwargs["factory"]
    writer = kwargs["writer"]
    counters = kwargs["counters"]
    for index, feed in enumerate(config.feeds, start=1):
        payload = canonical_json_bytes(
            {
                "feed": feed,
                "fixture_label": SYNTHETIC_FIXTURE_LABEL,
                "venue": config.venue.value,
            }
        )
        envelope = factory.make(
            feed_type=feed,
            instrument_id=f"SYNTHETIC-{config.venue.value}-INSTRUMENT",
            market_id=f"SYNTHETIC-{config.venue.value}-MARKET",
            source_timestamp_ns=None,
            receive_timestamp_utc_ns=time.time_ns(),
            receive_monotonic_ns=time.monotonic_ns(),
            source_event_id=f"SYNTHETIC-FIXTURE-{feed}-{index}",
            raw_payload=payload,
            provenance=CaptureProvenance(
                factory.provenance.collection_id,
                f"fixture://prediction-markets/{config.venue.value.lower()}/{feed}",
                "FIXTURE",
                SYNTHETIC_FIXTURE_LABEL,
            ),
        )
        writer.append(envelope)
        counters.observe(envelope)
    raise ResearchDataCapacityError("SYNTHETIC/FIXTURE bounded capacity")


class _ClosedSession:
    def close(self) -> None:
        return None


def _materialize_failed_campaign(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, Path, Path]:
    volume = tmp_path / "hyperlab-prediction-markets"
    campaign = volume / "campaigns" / RUN_SLUG
    source = volume / "sources" / RUN_SLUG
    incoming = (
        tmp_path
        / "home"
        / "hyperlab-prediction-markets"
        / "incoming"
        / RUN_SLUG
    )
    for path in (campaign, source / "config" / "research", incoming):
        path.mkdir(parents=True)
    for name in ("campaign-manifest.json", "campaign-manifest.sha256"):
        (campaign / name).write_bytes((CANDIDATE_PACK / name).read_bytes())
    for name in (
        "prediction-markets-candidate-v1.json",
        "polymarket-public-contract-v1.json",
        "kalshi-public-contract-v1.json",
    ):
        (source / "config" / "research" / name).write_bytes(
            (ROOT / "config" / "research" / name).read_bytes()
        )
    handoff = {
        "boundary": forensics.BOUNDARY,
        "campaign_root": str(campaign),
        "incoming_root": str(incoming),
        "source_commit": SOURCE_COMMIT,
        "source_root": str(source),
    }
    handoff_raw = canonical_json_bytes(handoff) + b"\n"
    (incoming / "handoff.json").write_bytes(handoff_raw)
    (incoming / "handoff.sha256").write_text(
        f"{hashlib.sha256(handoff_raw).hexdigest()}  handoff.json\n",
        encoding="ascii",
    )
    source_rows = []
    for name in (
        "prediction-markets-candidate-v1.json",
        "polymarket-public-contract-v1.json",
        "kalshi-public-contract-v1.json",
    ):
        raw = (source / "config" / "research" / name).read_bytes()
        source_rows.append(
            {
                "blob_sha1": hashlib.sha1(
                    f"blob {len(raw)}\0".encode("ascii") + raw,
                    usedforsecurity=False,
                ).hexdigest(),
                "mode": "100644",
                "path": f"config/research/{name}",
                "size": len(raw),
            }
        )
    source_inventory_body = {
        "boundary": forensics.BOUNDARY,
        "commit": SOURCE_COMMIT,
        "files": source_rows,
        "schema_version": 1,
    }
    (incoming / "source-inventory.json").write_bytes(
        canonical_json_bytes(
            {
                **source_inventory_body,
                "inventory_sha256": hashlib.sha256(
                    canonical_json_bytes(source_inventory_body)
                ).hexdigest(),
            }
        )
        + b"\n"
    )
    candidate = CandidatePreregistration.from_path(
        source / "config" / "research" / "prediction-markets-candidate-v1.json"
    )
    contracts = {
        venue: OfficialPublicContract.from_path(
            source
            / "config"
            / "research"
            / f"{venue.value.lower()}-public-contract-v1.json"
        )
        for venue in (Venue.POLYMARKET, Venue.KALSHI)
    }
    manifest = json.loads((campaign / "campaign-manifest.json").read_text(encoding="utf-8"))
    start = datetime.fromisoformat(str(manifest["starts_at_utc"]).replace("Z", "+00:00")).astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    start_delta = start - epoch
    cutoff = (
        start_delta.days * 86_400_000_000_000
        + start_delta.seconds * 1_000_000_000
        + start_delta.microseconds * 1_000
        + candidate.prospective_shard_policy.cadence_seconds * 1_000_000_000
    )
    monkeypatch.setattr(probe_module, "_polymarket_probe", _synthetic_capacity_transport)
    monkeypatch.setattr(probe_module, "_kalshi_probe", _synthetic_capacity_transport)
    for venue in (Venue.POLYMARKET, Venue.KALSHI):
        label = venue.value.lower()
        plan = candidate.collection_plans[venue]
        shard = campaign / label / "runs" / "shard-0000-20260901T000000Z"
        probe_module.run_public_probe(
            probe_module.ProbeConfig(
                output_root=shard,
                venue=venue,
                feeds=plan.feeds,
                instruments=(),
                census_limit=plan.census_limit,
                duration_seconds=plan.duration_seconds,
                max_bytes=plan.max_bytes,
                max_segment_bytes=plan.max_segment_bytes,
                rotation_seconds=plan.rotation_seconds,
                progress_interval_seconds=plan.progress_interval_seconds,
                collection_id=candidate.prospective_shard_policy.collection_id(
                    base_collection_id=plan.collection_id(str(manifest["campaign_id"])),
                    campaign_manifest_sha256=str(manifest["manifest_sha256"]),
                    venue=venue,
                    ordinal=0,
                    scheduled_start=start,
                ),
                max_frames=plan.max_frames,
                max_segments=plan.max_segments,
                max_network_calls=plan.max_network_calls,
                campaign_manifest_sha256=str(manifest["manifest_sha256"]),
                official_contract_sha256=contracts[venue].contract_sha256,
                candidate_config_sha256=candidate.config_sha256,
                collection_cutoff_utc_ns_exclusive=cutoff,
            ),
            http_session_factory=_ClosedSession,
        )
        result_path = shard / "reports" / "result.json"
        result = json.loads(result_path.read_text(encoding="utf-8"))
        result["terminal_health"] = "PUBLIC_SOURCE_INVALID"
        result["error"] = "ValueError:SYNTHETIC/FIXTURE public record shape diverged"
        terminal = canonical_json_bytes(result)
        result_path.write_bytes(terminal)
        (shard / "reports" / "health.json").write_bytes(terminal)
        state_root = campaign / label
        (state_root / "state.json").write_bytes(
            canonical_json_bytes(
                {
                    "active_ordinal": None,
                    "boundary": forensics.BOUNDARY,
                    "error": "RunnerError:terminal collection receipt failed authentication: prediction terminal collection result is not admissible",
                    "lifecycle": "INTEGRITY_FAILED",
                    "recorded_slots": None,
                    "venue": venue.value,
                }
            )
            + b"\n"
        )
    forensic_root = incoming / "forensics" / FORENSIC_SLUG
    return campaign, incoming, source, forensic_root


def test_read_only_export_and_offline_diagnostic_identify_first_real_shape_divergence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign, incoming, source, forensic_root = _materialize_failed_campaign(
        tmp_path,
        monkeypatch,
    )
    before = _tree_digest(campaign)
    result = forensics.export_forensics(
        campaign_root=campaign,
        incoming_root=incoming,
        source_root=source,
        output_root=forensic_root,
        expected_source_commit=SOURCE_COMMIT,
    )
    assert result["status"] == "PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_EXPORT_GREEN"
    assert result["raw_segments_exported"] == 0
    assert _tree_digest(campaign) == before
    assert not list(forensic_root.rglob("*.rdpseg"))
    scope = json.loads((forensic_root / forensics.SCOPE_NAME).read_text(encoding="utf-8"))
    assert len(scope["raw_segment_metadata"]) == 2
    assert all(item["content_exported"] is False for item in scope["raw_segment_metadata"])
    diagnosis = forensics.diagnose_forensics(
        forensic_root,
        expected_source_commit=SOURCE_COMMIT,
    )
    assert diagnosis["status"] == "PREDICTION_MARKETS_RECEIPT_AUTH_DIVERGENCE_IDENTIFIED"
    assert diagnosis["raw_segments_read"] == 0
    for venue in ("polymarket", "kalshi"):
        report = diagnosis["reports"][venue]
        assert report["first_divergence"]["field"] == "terminal_health.accepted", report
        assert report["first_divergence"]["observed"] == "PUBLIC_SOURCE_INVALID"
        assert report["runtime_error_class"] == (
            "prediction terminal collection result is not admissible"
        )

    def _divergent_plan(*_args, **_kwargs) -> None:
        raise forensics.ForensicError("SYNTHETIC/FIXTURE frozen plan divergence")

    monkeypatch.setattr(forensics, "_verify_binding_plan", _divergent_plan)
    diagnosis_with_later_plan_divergence = forensics.diagnose_forensics(
        forensic_root,
        expected_source_commit=SOURCE_COMMIT,
    )
    for venue in ("polymarket", "kalshi"):
        report = diagnosis_with_later_plan_divergence["reports"][venue]
        assert report["first_divergence"]["field"] == "terminal_health.accepted"
        assert report["context_checks_after_result_stage"][0]["field"] == (
            "post_result_context.verify_collection_plan"
        )
        assert report["context_checks_after_result_stage"][0]["ok"] is False
    standalone = subprocess.run(
        [
            sys.executable,
            "-I",
            str(ROOT / "ops/prediction_markets_launch_v1/receipt_auth_forensics.py"),
            "diagnose",
            "--bundle-root",
            str(forensic_root),
            "--expected-source-commit",
            SOURCE_COMMIT,
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=15,
    )
    assert standalone.returncode == 0, standalone.stderr
    assert "PREDICTION_MARKETS_RECEIPT_AUTH_DIVERGENCE_IDENTIFIED" in standalone.stdout


def test_export_refuses_special_file_and_never_reuses_forensic_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign, incoming, source, forensic_root = _materialize_failed_campaign(
        tmp_path,
        monkeypatch,
    )
    state = campaign / "polymarket" / "state.json"
    state.unlink()
    state.mkdir()
    with pytest.raises(forensics.ForensicError, match="special"):
        forensics.export_forensics(
            campaign_root=campaign,
            incoming_root=incoming,
            source_root=source,
            output_root=forensic_root,
            expected_source_commit=SOURCE_COMMIT,
        )
    assert forensic_root.exists()
    with pytest.raises(forensics.ForensicError, match="must be a new"):
        forensics.export_forensics(
            campaign_root=campaign,
            incoming_root=incoming,
            source_root=source,
            output_root=forensic_root,
            expected_source_commit=SOURCE_COMMIT,
        )


def test_offline_diagnostic_refuses_archive_corruption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign, incoming, source, forensic_root = _materialize_failed_campaign(
        tmp_path,
        monkeypatch,
    )
    forensics.export_forensics(
        campaign_root=campaign,
        incoming_root=incoming,
        source_root=source,
        output_root=forensic_root,
        expected_source_commit=SOURCE_COMMIT,
    )
    archive = forensic_root / forensics.ARCHIVE_NAME
    archive.write_bytes(archive.read_bytes() + b"corruption")
    with pytest.raises(forensics.ForensicError, match="archive SHA-256 diverged"):
        forensics.diagnose_forensics(
            forensic_root,
            expected_source_commit=SOURCE_COMMIT,
        )


def test_generated_operator_blocks_are_exact_bounded_and_do_not_mutate_services() -> None:
    tool_raw = (
        ROOT / "ops/prediction_markets_launch_v1/receipt_auth_forensics.py"
    ).read_bytes()
    bash = pack_builder.render_tabby_export(tool_raw)
    fetch = pack_builder.render_windows_fetch()
    diagnose = pack_builder.render_windows_diagnose()
    encoded = bash.split(
        "HYPERLAB_PM_FORENSIC_TOOL_BASE64'\n",
        maxsplit=1,
    )[1].split("\nHYPERLAB_PM_FORENSIC_TOOL_BASE64\n", maxsplit=1)[0]
    import base64

    assert base64.b64decode(encoded) == tool_raw
    assert pack_builder.CAMPAIGN_ROOT in bash
    assert pack_builder.FORENSIC_ROOT in bash
    assert "sha256sum -c" in bash
    assert "PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_EXPORT_READY_FOR_TRANSFER" in bash
    assert "systemctl" not in bash
    assert "journalctl" not in bash
    assert "prediction-collect" not in bash
    assert "prediction-recover" not in bash
    assert '[[ -e "$FORENSICS_PARENT" || -L "$FORENSICS_PARENT" ]]' in bash
    assert '[[ -d "$FORENSICS_PARENT" && ! -L "$FORENSICS_PARENT" ]]' in bash
    assert "HYPERLAB_PM_SSH_KEY" in fetch
    assert "Local forensic root must be new" in fetch
    assert "receipt-auth-forensic.tar.sha256" in fetch
    assert "PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_FETCHED" in fetch
    assert "--expected-source-commit" in diagnose
    assert "aucun" not in fetch.lower() or "systemctl" not in fetch


def test_static_forensic_allowlist_excludes_raw_payloads_and_h1() -> None:
    source = (
        ROOT / "ops/prediction_markets_launch_v1/receipt_auth_forensics.py"
    ).read_text(encoding="utf-8")
    assert '"probe-config.json", "result.json", "health.json"' in source
    assert "campaign-manifest.sha256" in source
    assert "handoff.sha256" in source
    assert "source-inventory.json" in source
    assert "RAW_SEGMENT_EXCLUDED_FAILURE_PRECEDES_REPLAY" in source
    assert "content_exported\": False" in source
    assert "systemd_journal:NOT_QUERIED" in source
    assert "H1:OUT_OF_SCOPE" in source
