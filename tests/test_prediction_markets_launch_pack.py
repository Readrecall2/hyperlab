from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from ops.prediction_markets_launch_v1 import launch_pack, preflight

ROOT = Path(__file__).resolve().parents[1]
OPS = ROOT / "ops" / "prediction_markets_launch_v1"
PLAN = OPS / "launch-plan-v1.json"


def _plan() -> dict[str, object]:
    value = json.loads(PLAN.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _handoff() -> dict[str, object]:
    return {
        "boundary": launch_pack.BOUNDARY,
        "campaign_root": "/mnt/HC_Volume_106716684/hyperlab-prediction-markets/campaigns/pm-20260827t120000z-deadbeef",
        "dashboard_port": 18081,
        "incoming_root": "/home/hyperlab/hyperlab-prediction-markets/incoming/pm-20260827t120000z-deadbeef",
        "service_user": "hyperlab",
        "services": {
            "dashboard": "hyperlab-pm-20260827t120000z-deadbeef-dashboard.service",
            "kalshi": "hyperlab-pm-20260827t120000z-deadbeef-kalshi.service",
            "polymarket": "hyperlab-pm-20260827t120000z-deadbeef-polymarket.service",
        },
        "source_root": "/mnt/HC_Volume_106716684/hyperlab-prediction-markets/sources/pm-20260827t120000z-deadbeef",
    }


def _incoming_handoff(tmp_path: Path) -> Path:
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    wheelhouse = incoming / "wheelhouse"
    wheelhouse.mkdir()
    wheel = wheelhouse / "example-1.0-py3-none-any.whl"
    wheel.write_bytes(b"offline-wheel")
    wheel_manifest = f"{preflight.sha256_bytes(wheel.read_bytes())}  {wheel.name}\n".encode(
        "ascii"
    )
    (incoming / "wheelhouse.sha256").write_bytes(wheel_manifest)
    bundle = incoming / "launch.bundle"
    bundle.write_bytes(b"git-bundle-fixture")
    transfer = {
        "files": [
            {
                "path": "launch.bundle",
                "sha256": preflight.sha256_bytes(bundle.read_bytes()),
                "size": bundle.stat().st_size,
            },
            {
                "path": "wheelhouse.sha256",
                "sha256": preflight.sha256_bytes(wheel_manifest),
                "size": len(wheel_manifest),
            },
        ],
        "schema_version": 1,
    }
    transfer_payload = preflight.canonical_json_bytes(transfer) + b"\n"
    (incoming / "transfer-inventory.json").write_bytes(transfer_payload)
    volume_base = tmp_path / "volume-base"
    handoff = {
        "boundary": preflight.BOUNDARY,
        "bundle_filename": bundle.name,
        "bundle_sha256": preflight.sha256_bytes(bundle.read_bytes()),
        "campaign_root": str(volume_base / "campaigns" / "campaign-must-be-new"),
        "dashboard_port": 18081,
        "disk": {"required_free_bytes": 194347270144},
        "schema_version": 1,
        "service_user": "hyperlab",
        "services": _handoff()["services"],
        "source_root": str(volume_base / "sources" / "source-must-be-new"),
        "transfer_inventory_sha256": preflight.sha256_bytes(transfer_payload),
        "volume_base": str(volume_base),
        "volume_mount": "/mnt/HC_Volume_106716684",
        "wheelhouse_manifest_sha256": preflight.sha256_bytes(wheel_manifest),
    }
    payload = preflight.canonical_json_bytes(handoff) + b"\n"
    path = incoming / "handoff.json"
    path.write_bytes(payload)
    (incoming / "handoff.sha256").write_text(
        f"{preflight.sha256_bytes(payload)}  handoff.json\n",
        encoding="ascii",
    )
    return path


def _run_git(*arguments: str, cwd: Path | None = None) -> None:
    completed = subprocess.run(
        ["git", *arguments],
        capture_output=True,
        check=False,
        cwd=cwd,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr


def _create_real_git_bundle(tmp_path: Path, bundle: Path) -> None:
    source = tmp_path / "synthetic-bundle-source"
    _run_git("init", "--quiet", str(source))
    _run_git("config", "user.email", "synthetic-fixture@invalid.example", cwd=source)
    _run_git("config", "user.name", "Synthetic Fixture", cwd=source)
    (source / "SYNTHETIC_FIXTURE.txt").write_text(
        "SYNTHETIC/FIXTURE only; no economic evidence.\n",
        encoding="utf-8",
    )
    _run_git("add", "SYNTHETIC_FIXTURE.txt", cwd=source)
    _run_git("commit", "--quiet", "-m", "synthetic bundle fixture", cwd=source)
    _run_git("bundle", "create", str(bundle), "HEAD", cwd=source)


def _git_bash_path(path: Path) -> str:
    resolved = path.resolve()
    drive = resolved.drive.removesuffix(":").lower()
    assert len(drive) == 1
    return f"/{drive}{resolved.as_posix()[2:]}"


def _materialize_windows_transfer_layout(
    tmp_path: Path,
) -> tuple[Path, Path, Path, Path, Path, dict[str, object]]:
    pack = tmp_path / "materialized-pack"
    operator = pack / "operator"
    wheelhouse = pack / "wheelhouse"
    operator.mkdir(parents=True)
    wheelhouse.mkdir()
    bundle = pack / "hyperlab-prediction-markets-prospective-launch-v1.bundle"
    _create_real_git_bundle(tmp_path, bundle)
    handoff_json = pack / "handoff.json"
    handoff_json.write_bytes(b'{"fixture":"SYNTHETIC_LAYOUT_ONLY"}\n')
    wheel = wheelhouse / "fixture-1.0-py3-none-any.whl"
    wheel.write_bytes(b"real-wheel-hash-fixture")
    (pack / "handoff.sha256").write_bytes(
        f"{launch_pack.sha256_file(handoff_json)}  handoff.json\n".encode("ascii")
    )
    (pack / "wheelhouse.sha256").write_bytes(
        f"{launch_pack.sha256_file(wheel)}  {wheel.name}\n".encode("ascii")
    )
    handoff = _handoff()
    handoff.update(
        {
            "bundle_filename": bundle.name,
            "bundle_sha256": launch_pack.sha256_file(bundle),
            "incoming_root": _git_bash_path(pack),
        }
    )
    block_a = operator / "A-windows-bundle-verify-transfer.ps1"
    block_a.write_text(launch_pack.render_windows_transfer(handoff), encoding="utf-8")
    ssh_key = tmp_path / "hyperlab_hetzner"
    ssh_key.write_bytes(b"SYNTHETIC_PRIVATE_KEY_PATH_FIXTURE_NOT_A_KEY")
    return pack, block_a, bundle, handoff_json, ssh_key, handoff


def _invoke_windows_transfer_with_strict_fakes(
    tmp_path: Path,
    block_a: Path,
    ssh_key: Path,
    *,
    log_name: str,
) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    powershell = shutil.which("powershell.exe") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is unavailable for the materialized-layout regression")
    if os.name == "nt":
        program_files = Path(os.environ.get("PROGRAMFILES", r"C:\Program Files"))
        candidate = program_files / "Git" / "bin" / "bash.exe"
        git_bash = str(candidate) if candidate.is_file() else None
    else:
        git_bash = shutil.which("bash")
    if git_bash is None:
        pytest.skip("Git Bash is unavailable for the remote-command simulation")
    log = tmp_path / log_name
    wrapper = tmp_path / f"invoke-{log_name}.ps1"
    wrapper.write_text(
        """$ErrorActionPreference = 'Stop'
function global:ssh {
    Add-Content -LiteralPath $env:HYPERLAB_PM_FAKE_LOG -Value ('ssh|' + ($args -join '|'))
    $RemoteCommand = $args[-1]
    if ($RemoteCommand.Contains('git -C "$VERIFY_REPO" bundle verify "$BUNDLE_PATH"')) {
        & $env:HYPERLAB_PM_TEST_BASH -lc $RemoteCommand
        $global:LASTEXITCODE = $LASTEXITCODE
    } else {
        $global:LASTEXITCODE = 0
    }
}
function global:scp {
    Add-Content -LiteralPath $env:HYPERLAB_PM_FAKE_LOG -Value ('scp|' + ($args -join '|'))
    $global:LASTEXITCODE = 0
}
& $env:HYPERLAB_PM_A_SCRIPT
""",
        encoding="utf-8",
    )
    powershell_temp = tmp_path / "powershell-temp"
    powershell_temp.mkdir()
    non_git_cwd = tmp_path / "non-git-cwd"
    non_git_cwd.mkdir()
    environment = os.environ.copy()
    environment.update(
        {
            "HYPERLAB_PM_A_SCRIPT": str(block_a),
            "HYPERLAB_PM_FAKE_LOG": str(log),
            "HYPERLAB_PM_SSH_KEY": str(ssh_key),
            "HYPERLAB_PM_SSH_TARGET": "hyperlab@5.223.60.130",
            "HYPERLAB_PM_TEST_BASH": git_bash,
            "TEMP": str(powershell_temp),
            "TMP": str(powershell_temp),
        }
    )
    completed = subprocess.run(
        [
            powershell,
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(wrapper),
        ],
        capture_output=True,
        check=False,
        cwd=non_git_cwd,
        env=environment,
        text=True,
        timeout=30,
    )
    assert not (non_git_cwd / ".git").exists()
    return completed, log, powershell_temp


def _green_command(arguments: list[str] | tuple[str, ...]) -> preflight.CommandResult:
    if arguments[0] == "python3.12" or arguments[0].replace("\\", "/").endswith(
        "/.venv/bin/python"
    ):
        return preflight.CommandResult(0, "", "")
    if arguments[:4] == ["git", "init", "--bare", "--quiet"]:
        return preflight.CommandResult(0, "", "")
    if (
        len(arguments) >= 6
        and arguments[0] == "git"
        and arguments[1] == "-C"
        and arguments[3:5] == ["bundle", "verify"]
    ):
        return preflight.CommandResult(0, "bundle verified", "")
    if arguments[0] == "timedatectl":
        return preflight.CommandResult(0, "yes", "")
    if arguments[0] == "findmnt":
        return preflight.CommandResult(
            0,
            "/mnt/HC_Volume_106716684 /dev/sdb ext4 rw,relatime,discard",
            "",
        )
    if arguments[0] == "df":
        return preflight.CommandResult(
            0,
            "Filesystem 1-blocks Used Available Capacity Mounted on\n/dev/sdb 300000000000 1 200000000000 1% /mnt/HC_Volume_106716684",
            "",
        )
    if arguments[0] == "systemctl":
        return preflight.CommandResult(
            0,
            "LoadState=not-found\nActiveState=inactive\nSubState=dead\nMainPID=0",
            "",
        )
    raise AssertionError(arguments)


def test_plan_freezes_candidate_identities_and_conservative_h1_reservation() -> None:
    plan = launch_pack.validate_plan(_plan())
    assert plan["access_bundle_sha256"] == (
        "965a42f2169c16201323477c0eb1ba7a8b540b24109c1d9252d5d9fcce55bbe5"
    )
    assert plan["base_commit"] == "3f188b9c28c9fec406b904a9e3307b43f54243e8"
    assert plan["candidate_config_sha256"] == (
        "aa60c0ff0ef95813d79f56b6ea93a31952061b562905dc9729162f7b16e41964"
    )
    assert plan["campaign_manifest_sha256"] == (
        "c9eb654d077ba1c3cf4cf709c2633077f40fc55d5f9533ce82d498564cd66388"
    )
    disk = plan["disk"]
    assert isinstance(disk, dict)
    assert disk == {
        "h1_reserved_bytes": 144 * 1024**3,
        "prediction_maximum_raw_bytes": 2 * 672 * 16 * 1024**2,
        "safety_margin_bytes": 16 * 1024**3,
        "required_free_bytes": 194347270144,
    }
    assert plan["dashboard_port"] == 18081
    assert plan["python"] == {
        "implementation": "CPython",
        "major": 3,
        "minimum_glibc": "2.28",
        "minor": 12,
        "target_architecture": "x86_64",
    }
    assert "18080" not in json.dumps(plan)
    assert "hyperlab-h1" not in json.dumps(plan)


def test_plan_refuses_capacity_arithmetic_or_h1_path_drift() -> None:
    plan = _plan()
    disk = dict(plan["disk"])  # type: ignore[arg-type]
    disk["required_free_bytes"] = int(disk["required_free_bytes"]) - 1
    with pytest.raises(launch_pack.LaunchPackError, match="arithmetic"):
        launch_pack.validate_plan({**plan, "disk": disk})
    remote = dict(plan["remote"])  # type: ignore[arg-type]
    remote["volume_base"] = "/mnt/HC_Volume_106716684/hyperlab-h1"
    with pytest.raises(launch_pack.LaunchPackError):
        launch_pack.validate_plan({**plan, "remote": remote})


def test_new_slugs_produce_unique_incoming_source_campaign_and_service_identities() -> None:
    first = "pm-20260827t120000z-deadbeef"
    second = "pm-20260827t120001z-feedface"
    parents = (
        "/home/hyperlab/hyperlab-prediction-markets/incoming",
        "/mnt/HC_Volume_106716684/hyperlab-prediction-markets/sources",
        "/mnt/HC_Volume_106716684/hyperlab-prediction-markets/campaigns",
    )
    first_roots = {launch_pack._remote_path(parent, first) for parent in parents}
    second_roots = {launch_pack._remote_path(parent, second) for parent in parents}
    assert len(first_roots) == len(second_roots) == 3
    assert first_roots.isdisjoint(second_roots)
    assert set(launch_pack._service_names(first).values()).isdisjoint(
        launch_pack._service_names(second).values()
    )


def test_authoritative_base_remains_valid_for_causal_fix_descendants() -> None:
    base = "3f188b9c28c9fec406b904a9e3307b43f54243e8"
    head = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()
    assert launch_pack._git_is_ancestor(ROOT, base, head)
    assert not launch_pack._git_is_ancestor(ROOT, head, base)


def test_rendered_units_are_independent_hardened_and_path_isolated() -> None:
    units = launch_pack.render_units(_handoff())
    assert len(units) == 3
    polymarket = next(value for key, value in units.items() if "polymarket" in key)
    kalshi = next(value for key, value in units.items() if "kalshi" in key)
    dashboard = next(value for key, value in units.items() if "dashboard" in key)
    assert "--venue polymarket" in polymarket and "--venue kalshi" not in polymarket
    assert "--venue kalshi" in kalshi and "--venue polymarket" not in kalshi
    assert "ReadWritePaths=" in polymarket and "/polymarket" in polymarket
    assert "ReadWritePaths=" in kalshi and "/kalshi" in kalshi
    assert "ReadWritePaths=" not in dashboard
    assert "--host 127.0.0.1 --port 18081" in dashboard
    for collector in (polymarket, kalshi):
        assert "RestartPreventExitStatus=4" in collector
    assert "RestartPreventExitStatus=4" not in dashboard
    for unit in units.values():
        assert "User=hyperlab" in unit
        assert "NoNewPrivileges=yes" in unit
        assert "ProtectSystem=strict" in unit
        assert "CapabilityBoundingSet=" in unit
        assert "Restart=on-failure" in unit
        assert "hyperlab-h1" not in unit
        assert "18080" not in unit


def test_operator_blocks_are_shell_separated_bounded_and_h1_safe() -> None:
    handoff = _handoff()
    handoff.update(
        {
            "bundle_filename": "hyperlab-prediction-markets-prospective-launch-v1.bundle",
            "bundle_sha256": "a" * 64,
            "source_commit": "b" * 40,
            "volume_base": "/mnt/HC_Volume_106716684/hyperlab-prediction-markets",
        }
    )
    windows = launch_pack.render_windows_transfer(handoff)
    install = launch_pack.render_tabby_install(handoff)
    monitor = launch_pack.render_tabby_monitor(handoff)
    tunnel = launch_pack.render_windows_tunnel(handoff)
    recovery = launch_pack.render_recovery_rollback(handoff)
    assert "$ErrorActionPreference = 'Stop'" in windows
    assert "$BundleRoot = (Resolve-Path -LiteralPath (Join-Path $OperatorRoot '..')).Path" in windows
    assert "$BundleRoot = (Resolve-Path -LiteralPath $PSScriptRoot).Path" not in windows
    assert "Assert-Sha256Manifest" in windows
    assert "git -C $VerifyRoot bundle verify $Path" in windows
    assert "git bundle verify $BundlePath" not in windows
    assert 'git -C "$VERIFY_REPO" bundle verify "$BUNDLE_PATH"' in windows
    assert "cd '$IncomingRoot' && git bundle verify" not in windows
    assert "HYPERLAB_PM_SSH_KEY" in windows
    assert "PREDICTION_WINDOWS_TRANSFER_VERIFIED" in windows
    assert install.startswith("#!/usr/bin/env bash")
    assert "PREDICTION_INSTALL_ACTIVATION_GREEN" in install
    assert "PREDICTION_MONITOR_TRANSITION_OR_ALERT" in monitor
    assert "127.0.0.1:18081:127.0.0.1:18081" in tunnel
    assert "recovery|rollback" in recovery
    rendered = "\n".join((windows, install, monitor, tunnel, recovery))
    assert "df -P --output" not in rendered
    assert "systemctl" not in windows
    assert "hyperlab-h1" not in rendered
    assert "18080" not in rendered


def test_materialized_windows_a_uses_parent_pack_root_and_real_local_hashes(
    tmp_path: Path,
) -> None:
    pack, block_a, bundle, handoff_json, ssh_key, _handoff_data = (
        _materialize_windows_transfer_layout(tmp_path)
    )
    completed, log, powershell_temp = _invoke_windows_transfer_with_strict_fakes(
        tmp_path,
        block_a,
        ssh_key,
        log_name="successful-external-commands.log",
    )
    assert completed.returncode == 0, completed.stderr
    assert "PREDICTION_WINDOWS_TRANSFER_VERIFIED" in completed.stdout
    assert "PREDICTION_REMOTE_BUNDLE_VERIFIED" in completed.stdout
    lines = log.read_text(encoding="utf-8").splitlines()
    ssh_lines = [line for line in lines if line.startswith("ssh|")]
    scp_lines = [line for line in lines if line.startswith("scp|")]
    assert len(ssh_lines) == 2
    assert all(f"|-i|{ssh_key}|hyperlab@5.223.60.130|" in line for line in ssh_lines)
    assert len(scp_lines) == len(list(pack.iterdir()))
    assert all(f"|-i|{ssh_key}|-r|" in line for line in scp_lines)
    assert any(str(bundle) in line for line in scp_lines)
    assert any(str(handoff_json) in line for line in scp_lines)
    assert any(str(pack / "wheelhouse") in line for line in scp_lines)
    assert not any(str(pack / "operator" / bundle.name) in line for line in lines)
    assert list(powershell_temp.iterdir()) == []
    assert not list(pack.glob(".git-bundle-verify-*"))


def test_materialized_windows_a_rejects_sha_valid_non_bundle_with_real_git(
    tmp_path: Path,
) -> None:
    pack, block_a, bundle, _handoff_json, ssh_key, handoff = (
        _materialize_windows_transfer_layout(tmp_path)
    )
    bundle.write_bytes(b"SHA-valid but not a Git bundle")
    handoff["bundle_sha256"] = launch_pack.sha256_file(bundle)
    block_a.write_text(launch_pack.render_windows_transfer(handoff), encoding="utf-8")
    completed, log, powershell_temp = _invoke_windows_transfer_with_strict_fakes(
        tmp_path,
        block_a,
        ssh_key,
        log_name="invalid-bundle-external-commands.log",
    )
    assert completed.returncode != 0
    assert "git bundle verify failed" in completed.stderr
    assert not log.exists()
    assert list(powershell_temp.iterdir()) == []
    assert not list(pack.glob(".git-bundle-verify-*"))


@pytest.mark.parametrize(
    "relative_corruption",
    ["handoff.json", "wheelhouse/fixture-1.0-py3-none-any.whl"],
)
def test_materialized_windows_a_refuses_root_hash_divergence_before_external_commands(
    tmp_path: Path,
    relative_corruption: str,
) -> None:
    pack, block_a, _bundle, _handoff_json, ssh_key, _handoff_data = (
        _materialize_windows_transfer_layout(tmp_path)
    )
    (pack / relative_corruption).write_bytes(b"corrupted-after-manifest")
    completed, log, powershell_temp = _invoke_windows_transfer_with_strict_fakes(
        tmp_path,
        block_a,
        ssh_key,
        log_name="refused-external-commands.log",
    )
    assert completed.returncode != 0
    assert "SHA-256 diverged" in completed.stderr
    assert not log.exists()
    assert list(powershell_temp.iterdir()) == []


def test_target_preflight_verifies_real_bundle_from_non_git_cwd_and_cleans_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack, _block_a, _bundle, _handoff_json, _ssh_key, handoff = (
        _materialize_windows_transfer_layout(tmp_path)
    )
    non_git_cwd = tmp_path / "preflight-non-git-cwd"
    non_git_cwd.mkdir()
    monkeypatch.chdir(non_git_cwd)
    result = preflight.verify_git_bundle(pack, handoff, preflight._command)
    assert result == {
        "sha256": handoff["bundle_sha256"],
        "temporary_repository_removed": True,
        "verified": True,
    }
    assert not (non_git_cwd / ".git").exists()
    assert not list(pack.glob(".git-bundle-verify-*"))


def test_connectivity_preflight_is_per_venue_and_kalshi_never_uses_wss_credentials() -> None:
    green = preflight.probe_venue_connectivity(
        "polymarket",
        dns_probe=lambda host: [f"192.0.2.{len(host) % 100}"],
        tls_probe=lambda _host: "TLSv1.3",
        https_probe=lambda _url: 200,
        wss_probe=lambda _host, _path: 101,
    )
    assert green["verdict"] == "NETWORK_PREFLIGHT_GREEN"
    calls: list[tuple[str, str]] = []
    kalshi = preflight.probe_venue_connectivity(
        "kalshi",
        dns_probe=lambda _host: ["192.0.2.4"],
        tls_probe=lambda _host: "TLSv1.3",
        https_probe=lambda _url: 200,
        wss_probe=lambda host, path: calls.append((host, path)) or 101,
    )
    assert kalshi["verdict"] == "NETWORK_PREFLIGHT_GREEN"
    assert calls == []
    assert kalshi["wss"] == {
        "documented_status": "AUTHENTICATED_HANDSHAKE_REQUIRED",
        "probe": "NOT_EXECUTED_CREDENTIALS_FORBIDDEN",
        "status": None,
    }


@pytest.mark.parametrize("status", [403, 429])
def test_http_403_and_429_are_explicit_terminal_preflight_errors(status: int) -> None:
    result = preflight.probe_venue_connectivity(
        "kalshi",
        dns_probe=lambda _host: ["192.0.2.4"],
        tls_probe=lambda _host: "TLSv1.3",
        https_probe=lambda _url: status,
    )
    assert result["verdict"] == "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT"
    assert f"HTTPS_STATUS_{status}" in result["errors"]
    assert result["attempts"] == 1


def test_dns_failure_does_not_invent_http_or_hashes() -> None:
    def fail_dns(_host: str) -> list[str]:
        raise socket_error("DNS unavailable")

    class socket_error(OSError):
        pass

    result = preflight.probe_venue_connectivity("kalshi", dns_probe=fail_dns)
    assert result["verdict"] == "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT"
    assert result["https"]["status"] is None  # type: ignore[index]
    assert "sha256" not in json.dumps(result).lower()


def test_dns_uses_one_bounded_getent_process_without_public_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], float]] = []

    def completed(
        arguments: list[str],
        **kwargs: object,
    ) -> preflight.subprocess.CompletedProcess[str]:
        calls.append((arguments, float(kwargs["timeout"])))
        return preflight.subprocess.CompletedProcess(
            arguments,
            0,
            "192.0.2.8 STREAM fixture.example\n192.0.2.8 DGRAM fixture.example\n",
            "",
        )

    monkeypatch.setattr(preflight.subprocess, "run", completed)
    assert preflight._dns("fixture.example") == ["192.0.2.8"]
    assert calls == [(["getent", "ahosts", "fixture.example"], 3.0)]


def test_df_portable_parser_uses_df_pb1_column_four() -> None:
    output = "Filesystem 1-blocks Used Available Capacity Mounted on\n/dev/sdb 200000 1000 199000 1% /mnt/data\n"
    assert preflight._parse_df_available(output) == 199000
    with pytest.raises(preflight.PreflightError):
        preflight._parse_df_available("Filesystem only\n")


def test_host_preflight_synchronously_proves_ntp_capacity_port_services_and_isolates_venues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handoff = _incoming_handoff(tmp_path)
    monkeypatch.setenv("USER", "hyperlab")
    monkeypatch.setattr(preflight.Path, "home", classmethod(lambda _cls: Path("/home/hyperlab")))
    monkeypatch.setattr(preflight, "_required_command", lambda name: f"/usr/bin/{name}")
    result = preflight.host_preflight(
        handoff,
        run=_green_command,
        connectivity_probe=lambda venue: {
            "venue": venue,
            "verdict": (
                "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT"
                if venue == "polymarket"
                else "NETWORK_PREFLIGHT_GREEN"
            ),
        },
    )
    assert result["installation_admissible"] is True
    assert result["eligible_venues"] == ["kalshi"]
    assert result["checks"]["ntp"] == {"synchronized": True}  # type: ignore[index]
    assert result["checks"]["git_bundle"] == {  # type: ignore[index]
        "sha256": preflight.load_handoff(handoff)["bundle_sha256"],
        "temporary_repository_removed": True,
        "verified": True,
    }
    assert not list(handoff.parent.glob(".git-bundle-verify-*"))
    assert result["checks"]["loopback_port"] == {  # type: ignore[index]
        "free": True,
        "host": "127.0.0.1",
        "port": 18081,
    }
    assert len(result["checks"]["services"]) == 3  # type: ignore[index]


@pytest.mark.parametrize("failure", ["ntp", "capacity", "service"])
def test_host_preflight_refuses_ntp_capacity_or_service_collision(
    failure: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handoff = _incoming_handoff(tmp_path)
    monkeypatch.setenv("USER", "hyperlab")
    monkeypatch.setattr(preflight.Path, "home", classmethod(lambda _cls: Path("/home/hyperlab")))
    monkeypatch.setattr(preflight, "_required_command", lambda name: f"/usr/bin/{name}")

    def run(arguments: list[str] | tuple[str, ...]) -> preflight.CommandResult:
        if failure == "ntp" and arguments[0] == "timedatectl":
            return preflight.CommandResult(0, "no", "")
        if failure == "capacity" and arguments[0] == "df":
            return preflight.CommandResult(
                0,
                "Filesystem 1-blocks Used Available Capacity Mounted on\n/dev/sdb 300000000000 1 1000 99% /mnt/HC_Volume_106716684",
                "",
            )
        if failure == "service" and arguments[0] == "systemctl":
            return preflight.CommandResult(
                0,
                "LoadState=loaded\nActiveState=active\nSubState=running\nMainPID=123",
                "",
            )
        return _green_command(arguments)

    result = preflight.host_preflight(
        handoff,
        run=run,
        connectivity_probe=lambda venue: {
            "venue": venue,
            "verdict": "NETWORK_PREFLIGHT_GREEN",
        },
    )
    assert result["installation_admissible"] is False
    expected = {
        "ntp": "NTP is not synchronized",
        "capacity": "use a distinct host or ext4 volume",
        "service": "service collision",
    }[failure]
    assert expected in " ".join(result["errors"])  # type: ignore[arg-type]


def test_filesystem_probe_fsyncs_and_removes_only_its_exclusive_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handoff = _incoming_handoff(tmp_path)
    base = tmp_path / "volume-base"
    base.mkdir(mode=0o700)
    os.chmod(base, 0o700)
    original_stat = preflight.Path.stat

    def linux_mode_stat(path: Path, *args: object, **kwargs: object) -> os.stat_result:
        result = original_stat(path, *args, **kwargs)  # type: ignore[arg-type]
        if path == base:
            fields = list(result)
            fields[0] = (result.st_mode & ~0o777) | 0o700
            return os.stat_result(fields)
        return result

    monkeypatch.setattr(preflight.Path, "stat", linux_mode_stat)
    monkeypatch.setattr(preflight.os, "getuid", lambda: base.stat().st_uid, raising=False)
    original_open = preflight.os.open
    original_fsync = preflight.os.fsync
    original_close = preflight.os.close
    directory_descriptor = 987_654

    def linux_directory_open(path: object, flags: int, mode: int = 0o777) -> int:
        if Path(path) == base and flags == os.O_RDONLY:
            return directory_descriptor
        return original_open(path, flags, mode)  # type: ignore[arg-type]

    monkeypatch.setattr(preflight.os, "open", linux_directory_open)
    monkeypatch.setattr(
        preflight.os,
        "fsync",
        lambda descriptor: None
        if descriptor == directory_descriptor
        else original_fsync(descriptor),
    )
    monkeypatch.setattr(
        preflight.os,
        "close",
        lambda descriptor: None
        if descriptor == directory_descriptor
        else original_close(descriptor),
    )
    result = preflight.fsync_probe(handoff)
    assert result["terminal_signal"] == "PREDICTION_FILESYSTEM_FSYNC_GREEN"
    assert result["probe_removed"] is True
    assert list(base.iterdir()) == []


def test_network_recovery_preflight_returns_nonzero_for_only_the_failed_venue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reports = iter(
        (
            {"venue": "polymarket", "verdict": "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT"},
            {"venue": "kalshi", "verdict": "NETWORK_PREFLIGHT_GREEN"},
        )
    )
    monkeypatch.setattr(preflight, "probe_venue_connectivity", lambda _venue: next(reports))
    polymarket_report = tmp_path / "polymarket.json"
    kalshi_report = tmp_path / "kalshi.json"
    assert preflight.main(
        ["network", "--venue", "polymarket", "--report", str(polymarket_report)]
    ) == 4
    assert preflight.main(
        ["network", "--venue", "kalshi", "--report", str(kalshi_report)]
    ) == 0
    assert json.loads(polymarket_report.read_text(encoding="utf-8"))["venue"] == "polymarket"
    assert json.loads(kalshi_report.read_text(encoding="utf-8"))["venue"] == "kalshi"


def test_resume_preflight_revalidates_ntp_capacity_roots_and_offline_imports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handoff_path = _incoming_handoff(tmp_path)
    handoff = preflight.load_handoff(handoff_path)
    source = Path(str(handoff["source_root"]))
    campaign = Path(str(handoff["campaign_root"]))
    source.joinpath(".venv", "bin").mkdir(parents=True)
    source.joinpath("src").mkdir()
    source.joinpath(".venv", "bin", "python").write_bytes(b"offline-python-fixture")
    campaign.mkdir(parents=True)
    monkeypatch.setenv("USER", "hyperlab")
    monkeypatch.setattr(preflight.Path, "home", classmethod(lambda _cls: Path("/home/hyperlab")))
    result = preflight.resume_preflight(handoff_path, run=_green_command)
    assert result["resume_admissible"] is True
    assert result["terminal_signal"] == "PREDICTION_RESUME_PREFLIGHT_GREEN"
    assert result["checks"]["offline_imports"] == {"verified": True}  # type: ignore[index]


def test_wheelhouse_refuses_every_undeclared_entry(tmp_path: Path) -> None:
    handoff_path = _incoming_handoff(tmp_path)
    handoff = preflight.load_handoff(handoff_path)
    (handoff_path.parent / "wheelhouse" / "undeclared.whl").write_bytes(b"undeclared")
    with pytest.raises(preflight.PreflightError, match="undeclared"):
        preflight.verify_wheelhouse(handoff_path.parent, handoff)


def test_scripts_forbid_network_pip_and_target_only_prediction_services() -> None:
    bootstrap = (OPS / "bootstrap-offline.sh").read_text(encoding="utf-8")
    rollback = (OPS / "rollback.sh").read_text(encoding="utf-8")
    install = (OPS / "install.sh").read_text(encoding="utf-8")
    monitor = (OPS / "monitor.sh").read_text(encoding="utf-8")
    bundle = (OPS / "New-PredictionMarketsLaunchBundle.ps1").read_text(encoding="utf-8")
    assert "PIP_NO_INDEX=1" in bootstrap
    assert "--no-index" in bootstrap
    assert "--require-hashes" in bootstrap
    assert "include-system-site-packages = false" in bootstrap
    assert "hyperlab-pm-" in rollback
    assert "preflight.py\" network --venue \"$VENUE\"" in rollback
    assert "PREDICTION_RECOVERY_VENUE_REFUSED_PUBLIC_SOURCE_UNAVAILABLE" in rollback
    assert "preflight.py\" resume" in rollback
    assert "command_verified" in install and "MainPID" in install
    assert "manylinux_2_28_x86_64" in bundle
    assert "manylinux_2_17_x86_64" in bundle
    assert "git -C $RepoRoot bundle verify $BundlePath" in bundle
    assert "git bundle verify $BundlePath" not in bundle
    assert "glibc" in bootstrap
    assert "admission_required" in monitor and "eligible_venues" in monitor
    assert "CAPACITY_REFUSED" in monitor and "INTERRUPTED_RECOVERABLE" in monitor
    assert "hyperlab-h1" not in rollback + install + monitor
    assert "rm -rf" not in rollback + install
    assert "unlink" not in rollback + install
