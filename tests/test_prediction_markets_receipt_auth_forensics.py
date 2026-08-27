from __future__ import annotations

import hashlib
import json
import os
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
POWERSHELL_51 = Path(
    r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
)
TRANSFER_NAMES = (
    "forensic-scope.json",
    "forensic-inventory.json",
    "forensic-inventory.json.sha256",
    "receipt-auth-forensic.tar",
    "receipt-auth-forensic.tar.sha256",
)


def _run_powershell_51(
    script: Path,
    *,
    cwd: Path,
    environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    if not POWERSHELL_51.is_file():
        raise AssertionError(f"Windows PowerShell 5.1 is absent: {POWERSHELL_51}")
    return subprocess.run(
        [
            str(POWERSHELL_51),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
        ],
        capture_output=True,
        check=False,
        cwd=cwd,
        env=environment,
        errors="replace",
        text=True,
        timeout=30,
    )


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


def _failed_source_blob(relative: str) -> bytes:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "show", f"{SOURCE_COMMIT}:{relative}"],
        capture_output=True,
        check=False,
        timeout=10,
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"failed source blob is unavailable: {relative}: "
            f"{completed.stderr.decode(errors='replace')}"
        )
    return completed.stdout


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
        relative = f"config/research/{name}"
        (source / "config" / "research" / name).write_bytes(
            _failed_source_blob(relative)
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


def _materialize_windows_followup(
    root: Path,
    *,
    campaign_root: Path,
    archive_sha256: str,
    file_count: int,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path]:
    operator = root / "operator"
    tools = root / "tools"
    operator.mkdir(parents=True)
    tools.mkdir()
    monkeypatch.setattr(pack_builder, "CAMPAIGN_ROOT", str(campaign_root))
    fetch = operator / "G-windows-fetch-receipt-auth-forensic.ps1"
    diagnose = operator / "H-windows-offline-diagnose.ps1"
    fetch.write_text(
        pack_builder.render_windows_fetch(
            expected_archive_sha256=archive_sha256,
            expected_file_count=file_count,
        ),
        encoding="utf-8",
    )
    diagnose.write_text(pack_builder.render_windows_diagnose(), encoding="utf-8")
    (tools / "receipt_auth_forensics.py").write_bytes(
        (ROOT / "ops/prediction_markets_launch_v1/receipt_auth_forensics.py").read_bytes()
    )
    return fetch, diagnose


def _install_strict_fake_scp(fake_bin: Path) -> Path:
    fake_bin.mkdir()
    stub = fake_bin / "strict_scp.py"
    stub.write_text(
        r"""from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

ALLOWED = {
    "forensic-scope.json",
    "forensic-inventory.json",
    "forensic-inventory.json.sha256",
    "receipt-auth-forensic.tar",
    "receipt-auth-forensic.tar.sha256",
}


def refuse(message: str) -> None:
    print(f"STRICT_FAKE_SCP_REFUSED:{message}", file=sys.stderr)
    raise SystemExit(91)


arguments = sys.argv[1:]
if len(arguments) != 5 or arguments[0] != "-i" or arguments[2] != "--":
    refuse("arguments")
key = Path(arguments[1]).resolve(strict=True)
if key != Path(os.environ["HYPERLAB_PM_TEST_KEY"]).resolve(strict=True):
    refuse("key")
remote_prefix = os.environ["HYPERLAB_PM_TEST_REMOTE_PREFIX"] + "/"
remote = arguments[3]
if not remote.startswith(remote_prefix):
    refuse("remote_prefix")
name = remote[len(remote_prefix):]
if name not in ALLOWED or "/" in name or "\\" in name:
    refuse("remote_name")
source_root = Path(os.environ["HYPERLAB_PM_TEST_REMOTE_FILES"]).resolve(strict=True)
source = source_root / name
if source.is_symlink() or not source.is_file():
    refuse("source")
local_root = Path(os.environ["HYPERLAB_PM_TEST_LOCAL_ROOT"]).resolve(strict=True)
destination = Path(arguments[4])
if destination.name != name or destination.parent.resolve(strict=True) != local_root:
    refuse("destination")
if destination.exists():
    refuse("destination_exists")
log = Path(os.environ["HYPERLAB_PM_TEST_SCP_LOG"])
seen = log.read_text(encoding="ascii").splitlines() if log.exists() else []
if name in seen:
    refuse("duplicate")
shutil.copyfile(source, destination)
with log.open("a", encoding="ascii", newline="\n") as handle:
    handle.write(name + "\n")
""",
        encoding="utf-8",
    )
    launcher = fake_bin / "scp.cmd"
    launcher.write_text(
        f'@echo off\r\n"{sys.executable}" "{stub}" %*\r\nexit /b %ERRORLEVEL%\r\n',
        encoding="ascii",
    )
    return launcher


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


@pytest.mark.skipif(
    os.name != "nt" or not POWERSHELL_51.is_file(),
    reason="Windows PowerShell 5.1 runtime required",
)
def test_materialized_g_then_h_execute_under_powershell_51_without_repo_cwd_or_ssh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign, incoming, source, forensic_root = _materialize_failed_campaign(
        tmp_path,
        monkeypatch,
    )
    exported = forensics.export_forensics(
        campaign_root=campaign,
        incoming_root=incoming,
        source_root=source,
        output_root=forensic_root,
        expected_source_commit=SOURCE_COMMIT,
    )
    assert sorted(path.name for path in forensic_root.iterdir() if path.is_file()) == sorted(
        TRANSFER_NAMES
    )
    pack_root = tmp_path / "materialized-windows-followup"
    fetch, diagnose = _materialize_windows_followup(
        pack_root,
        campaign_root=campaign,
        archive_sha256=str(exported["archive_sha256"]),
        file_count=int(exported["file_count"]),
        monkeypatch=monkeypatch,
    )
    fetch_text = fetch.read_text(encoding="utf-8")
    parser_start = fetch_text.index("function Assert-Pin {")
    parser_end = fetch_text.index("$null = Assert-Pin")
    legacy_fetch = fetch.with_name("G-legacy-invalid-pin-regex.ps1")
    legacy_fetch.write_text(
        fetch_text[:parser_start]
        + r"""function Assert-Pin {
    param([string] $PinName, [string] $TargetName)
    $Line = (Get-Content -LiteralPath (Join-Path $LocalRoot $PinName) -Raw).Trim()
    if ($Line -notmatch '^([0-9a-f]{64})  ([^/\]+)$' -or $Matches[2] -cne $TargetName) { throw "Malformed pin: $PinName" }
    if ((Get-Sha256Hex (Join-Path $LocalRoot $TargetName)) -cne $Matches[1]) { throw "SHA-256 diverged: $TargetName" }
}
"""
        + fetch_text[parser_end:],
        encoding="utf-8",
    )
    fake_bin = tmp_path / "strict-fake-bin"
    fake_scp = _install_strict_fake_scp(fake_bin)
    assert fake_scp.is_file()
    fake_key = tmp_path / "synthetic-fixture-ssh-key"
    fake_key.write_text("SYNTHETIC/FIXTURE NOT A REAL KEY\n", encoding="ascii")
    non_repo_cwd = Path(r"C:\Windows\System32")
    assert non_repo_cwd.is_dir()
    repository_probe = subprocess.run(
        ["git", "-C", str(non_repo_cwd), "rev-parse", "--is-inside-work-tree"],
        capture_output=True,
        check=False,
        text=True,
        timeout=10,
    )
    assert repository_probe.returncode != 0
    local_root = tmp_path / "receipt-auth-local-v2"
    assert not local_root.exists()
    scp_log = tmp_path / "strict-scp.log"
    environment = os.environ.copy()
    environment.update(
        {
            "HYPERLAB_PM_FORENSIC_LOCAL_ROOT": str(local_root),
            "HYPERLAB_PM_SSH_KEY": str(fake_key),
            "HYPERLAB_PM_SSH_TARGET": "fixture@127.0.0.1",
            "HYPERLAB_PM_TEST_KEY": str(fake_key),
            "HYPERLAB_PM_TEST_LOCAL_ROOT": str(local_root),
            "HYPERLAB_PM_TEST_REMOTE_FILES": str(forensic_root),
            "HYPERLAB_PM_TEST_REMOTE_PREFIX": (
                "fixture@127.0.0.1:" + pack_builder.FORENSIC_ROOT
            ),
            "HYPERLAB_PM_TEST_SCP_LOG": str(scp_log),
            "PATH": str(fake_bin),
        }
    )
    legacy_local_root = tmp_path / "receipt-auth-local-legacy-regex"
    legacy_scp_log = tmp_path / "strict-scp-legacy.log"
    legacy_environment = environment.copy()
    legacy_environment.update(
        {
            "HYPERLAB_PM_FORENSIC_LOCAL_ROOT": str(legacy_local_root),
            "HYPERLAB_PM_TEST_LOCAL_ROOT": str(legacy_local_root),
            "HYPERLAB_PM_TEST_SCP_LOG": str(legacy_scp_log),
        }
    )
    legacy_failed = _run_powershell_51(
        legacy_fetch,
        cwd=non_repo_cwd,
        environment=legacy_environment,
    )
    legacy_output = legacy_failed.stdout + legacy_failed.stderr
    assert legacy_failed.returncode != 0
    assert "System.ArgumentException" in legacy_output
    assert r"([^/\]+)" in legacy_output
    assert "PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_FETCHED" not in legacy_output
    assert legacy_scp_log.read_text(encoding="ascii").splitlines() == list(
        TRANSFER_NAMES
    )
    assert sorted(path.name for path in legacy_local_root.iterdir()) == sorted(
        TRANSFER_NAMES
    )

    fetched = _run_powershell_51(
        fetch,
        cwd=non_repo_cwd,
        environment=environment,
    )
    assert fetched.returncode == 0, fetched.stdout + fetched.stderr
    assert "PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_FETCHED" in fetched.stdout
    assert sorted(path.name for path in local_root.iterdir()) == sorted(TRANSFER_NAMES)
    assert scp_log.read_text(encoding="ascii").splitlines() == list(TRANSFER_NAMES)
    for name in TRANSFER_NAMES:
        assert (local_root / name).read_bytes() == (forensic_root / name).read_bytes()
    before_diagnostic = _tree_digest(local_root)

    refused_reuse = _run_powershell_51(
        fetch,
        cwd=non_repo_cwd,
        environment=environment,
    )
    assert refused_reuse.returncode != 0
    assert "Local forensic root must be new" in (
        refused_reuse.stdout + refused_reuse.stderr
    )

    diagnosed = _run_powershell_51(
        diagnose,
        cwd=non_repo_cwd,
        environment=environment,
    )
    assert diagnosed.returncode == 0, diagnosed.stdout + diagnosed.stderr
    assert "PREDICTION_MARKETS_RECEIPT_AUTH_DIVERGENCE_IDENTIFIED" in diagnosed.stdout
    diagnostic_json = next(
        json.loads(line)
        for line in diagnosed.stdout.splitlines()
        if line.startswith("{")
    )
    assert diagnostic_json["raw_segments_read"] == 0
    assert diagnostic_json["source_commit"] == SOURCE_COMMIT
    for venue in ("polymarket", "kalshi"):
        first = diagnostic_json["reports"][venue]["first_divergence"]
        assert first["field"] == "terminal_health.accepted"
        assert first["observed"] == "PUBLIC_SOURCE_INVALID"
    assert _tree_digest(local_root) == before_diagnostic
    assert sorted(path.name for path in local_root.iterdir()) == sorted(TRANSFER_NAMES)

    traversal_remote = tmp_path / "traversal-pin-remote"
    traversal_remote.mkdir()
    for name in TRANSFER_NAMES:
        (traversal_remote / name).write_bytes((forensic_root / name).read_bytes())
    archive_hash = str(exported["archive_sha256"])
    (traversal_remote / "receipt-auth-forensic.tar.sha256").write_bytes(
        f"{archive_hash}  ../receipt-auth-forensic.tar\n".encode("ascii")
    )
    traversal_root = tmp_path / "receipt-auth-local-traversal-refused"
    traversal_log = tmp_path / "strict-scp-traversal.log"
    traversal_environment = environment.copy()
    traversal_environment.update(
        {
            "HYPERLAB_PM_FORENSIC_LOCAL_ROOT": str(traversal_root),
            "HYPERLAB_PM_TEST_LOCAL_ROOT": str(traversal_root),
            "HYPERLAB_PM_TEST_REMOTE_FILES": str(traversal_remote),
            "HYPERLAB_PM_TEST_SCP_LOG": str(traversal_log),
        }
    )
    traversal_refused = _run_powershell_51(
        fetch,
        cwd=non_repo_cwd,
        environment=traversal_environment,
    )
    traversal_output = traversal_refused.stdout + traversal_refused.stderr
    assert traversal_refused.returncode != 0
    assert "Malformed pin layout" in traversal_output
    assert "PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_FETCHED" not in traversal_output


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
    assert pack_builder.ACQUIRED_REMOTE_ARCHIVE_SHA256 not in fetch
    assert "file count diverged from the acquired F evidence" not in fetch
    assert "$Matches" not in fetch
    assert "Malformed pin layout" in fetch
    assert "[Security.Cryptography.SHA256]::Create()" in fetch
    assert "[IO.Path]::IsPathRooted($LocalRootRaw)" in fetch
    assert "--expected-source-commit" in diagnose
    assert "$Matches" not in diagnose
    assert "[IO.Path]::GetFullPath($BundleRootRaw)" in diagnose
    assert "aucun" not in fetch.lower() or "systemctl" not in fetch


def test_windows_followup_pack_reuses_remote_forensic_and_omits_new_f(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tool_commit = "7" * 40

    def fake_git(_repo: Path, *arguments: str) -> str:
        values = {
            ("rev-parse", "HEAD"): tool_commit,
            ("branch", "--show-current"): pack_builder.EXPECTED_BRANCH,
            ("status", "--porcelain"): "",
        }
        return values[arguments]

    monkeypatch.setattr(pack_builder, "_git", fake_git)
    output = tmp_path / "windows-followup-pack"
    inventory = pack_builder.build_pack(
        repo_root=ROOT,
        output_root=output,
        tool_commit=tool_commit,
        windows_followup=True,
    )
    assert inventory["scope"] == "WINDOWS_FETCH_DIAG_ONLY_REMOTE_FORENSIC_REUSE"
    paths = {str(item["path"]) for item in inventory["files"]}
    assert "operator/F-tabby-export-receipt-auth-forensic.sh" not in paths
    assert paths == {
        "README.md",
        "operator/G-windows-fetch-receipt-auth-forensic.ps1",
        "operator/H-windows-offline-diagnose.ps1",
        "tools/receipt_auth_forensics.py",
    }
    fetch = (output / "operator/G-windows-fetch-receipt-auth-forensic.ps1").read_text(
        encoding="utf-8"
    )
    assert pack_builder.FORENSIC_ROOT in fetch
    assert pack_builder.ACQUIRED_REMOTE_ARCHIVE_SHA256 in fetch


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
