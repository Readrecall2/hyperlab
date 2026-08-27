from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from ops.h1_dashboard_binding import binding_pack

ROOT = Path(__file__).resolve().parents[1]
OPS = ROOT / "ops" / "h1_dashboard_binding"
V8_INPUT = json.loads((OPS / "binding-input-v8.json").read_text(encoding="utf-8"))


def _plan() -> dict[str, object]:
    slug = "h1-synthetic-campaign"
    name = "h1-synthetic-dashboard-v1"
    campaign_id = "h1-" + "a" * 24
    return {
        "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        "campaign": {
            "campaign_id": campaign_id,
            "campaign_root": f"/mnt/HC_Volume_106716684/hyperlab-h1/campaigns/{slug}",
            "campaign_slug": slug,
            "collector_service": f"hyperlab-{slug}.service",
            "collector_source_root": f"/mnt/HC_Volume_106716684/hyperlab-h1/sources/{slug}",
            "manifest_checks": {
                "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
                "campaign_id": campaign_id,
                "schema_version": 1,
                "starts_at_utc": "2026-08-27T00:45:00Z",
            },
            "manifest_sha256": "1" * 64,
            "starts_at_utc": "2026-08-27T00:45:00Z",
        },
        "dashboard": {
            "bind_host": "127.0.0.1",
            "bind_port": 8765,
            "handoff_root": f"/etc/hyperlab-h1-dashboard/{name}",
            "incoming_root": f"/home/hyperlab/hyperlab-h1/dashboard-bindings/{name}",
            "policy_path": "config/research/hyperliquid-h1-ghost-v1.json",
            "remote_host": "203.0.113.42",
            "runtime_directory": name,
            "service_name": f"hyperlab-{name}.service",
            "source_root": f"/mnt/HC_Volume_106716684/hyperlab-h1/dashboard-sources/{name}",
            "user": "hyperlab",
        },
        "provenance": {
            "base_launch_commit": "1" * 40,
            "branch": "codex/h1-synthetic-dashboard-binding",
            "dashboard_integration_commit": "2" * 40,
            "dashboard_original_commit": "3" * 40,
            "source_commit": "4" * 40,
        },
        "schema_version": 1,
        "status": "H1_V8_DASHBOARD_BINDING_V1_GREEN_AWAITING_HUMAN_VPS_EXECUTION",
    }


def _v8_plan(source_commit: str = "f" * 40) -> dict[str, object]:
    return binding_pack.build_final_plan(V8_INPUT, source_commit)


def _handoff(source_commit: str = "f" * 40) -> dict[str, object]:
    return {
        "boundary": binding_pack.BOUNDARY,
        "bundle": {
            "filename": "hyperlab-h1-v8-dashboard-binding-v1.bundle",
            "ref": "refs/heads/codex/h1-v7-dashboard-binding-v1",
            "sha256": "1" * 64,
        },
        "inventory": {
            "pack_files": {path: "2" * 64 for path in binding_pack.PACK_FILES}
        },
        "plan": _v8_plan(source_commit),
        "schema_version": 1,
        "status": binding_pack.STATUS,
    }


def test_frozen_v8_input_has_authoritative_non_circular_identity() -> None:
    checked = binding_pack.validate_frozen_v8_input(V8_INPUT)
    campaign = checked["campaign"]
    dashboard = checked["dashboard"]
    provenance = checked["provenance"]
    assert isinstance(campaign, dict) and isinstance(dashboard, dict)
    assert isinstance(provenance, dict)
    assert "source_commit" not in provenance
    assert provenance == {
        "base_launch_commit": "926c878718c9f7d4095526061893e9f041d40c2b",
        "branch": "codex/h1-v7-dashboard-binding-v1",
        "dashboard_integration_commit": "cda0681b726fadba1a77bd72d2fca9f84dd14566",
        "dashboard_original_commit": "decb0e08aeabff71859fad052b84bff4af0ed990",
    }
    assert campaign["campaign_id"] == "h1-68c6493652abd667420b9a5b"
    assert campaign["campaign_slug"] == "h1-20260827t004500z-5973abde"
    assert campaign["manifest_sha256"] == (
        "3d8aeb91115ca7302266f85e55a2cd89404adbf4285991c35ad5c55b2647c2d5"
    )
    assert campaign["starts_at_utc"] == "2026-08-27T00:45:00Z"
    assert dashboard["bind_host"] == "127.0.0.1"
    assert dashboard["bind_port"] == 18080
    assert dashboard["remote_host"] == "5.223.60.130"
    assert checked["status"] == binding_pack.STATUS


def test_final_plan_injects_only_distinct_final_source_commit() -> None:
    plan = _v8_plan()
    provenance = plan["provenance"]
    assert isinstance(provenance, dict)
    assert provenance["source_commit"] == "f" * 40
    assert binding_pack.validate_handoff(_handoff())["status"] == binding_pack.STATUS


def test_frozen_v8_input_refuses_identity_drift() -> None:
    changed = copy.deepcopy(V8_INPUT)
    dashboard = changed["dashboard"]
    assert isinstance(dashboard, dict)
    dashboard["bind_port"] = 18081
    with pytest.raises(binding_pack.BindingPackError, match="frozen V8 dashboard"):
        binding_pack.validate_frozen_v8_input(changed)


@pytest.mark.parametrize(
    ("section", "key", "value", "message"),
    (
        ("dashboard", "bind_host", "0.0.0.0", "IPv4 loopback"),
        ("dashboard", "bind_port", 80, "unprivileged"),
        ("dashboard", "remote_host", "127.0.0.1", "remote host"),
        ("dashboard", "remote_host", "host.example'bad", "invalid format"),
        ("dashboard", "policy_path", "config/research/policy.json\nbad", "safe relative"),
        ("dashboard", "source_root", "/mnt/HC_Volume_106716684/hyperlab-h1/dashboard-sources/bad\nname", "exact leaf"),
        ("campaign", "manifest_sha256", "0" * 63, "invalid format"),
        ("campaign", "campaign_id", "h1-synthetic-id", "invalid format"),
        ("campaign", "campaign_slug", "wrong-slug", "exact slug"),
        ("campaign", "starts_at_utc", "not-utc", "invalid format"),
        ("provenance", "source_commit", "1" * 40, "distinct"),
    ),
)
def test_external_plan_refuses_identity_or_network_drift(
    section: str, key: str, value: object, message: str
) -> None:
    changed = copy.deepcopy(_plan())
    target = changed[section]
    assert isinstance(target, dict)
    target[key] = value
    with pytest.raises(binding_pack.BindingPackError, match=message):
        binding_pack.validate_plan(changed)


def test_systemd_renderer_is_loopback_only_readonly_and_hardened() -> None:
    plan = _plan()
    unit = binding_pack.render_systemd_unit(plan)
    campaign = plan["campaign"]
    dashboard = plan["dashboard"]
    provenance = plan["provenance"]
    assert isinstance(campaign, dict) and isinstance(dashboard, dict)
    assert isinstance(provenance, dict)
    assert "Environment=HYPERLAB_CONFIG=" in unit and "/config/research.toml" in unit
    assert "h1-dashboard-serve --port 8765" in unit and "--host" not in unit
    assert f"HYPERLAB_H1_DASHBOARD_SOURCE_COMMIT={provenance['source_commit']}" in unit
    assert f"HYPERLAB_H1_DASHBOARD_ORIGINAL_COMMIT={provenance['dashboard_original_commit']}" in unit
    assert f"HYPERLAB_H1_DASHBOARD_INTEGRATION_COMMIT={provenance['dashboard_integration_commit']}" in unit
    assert "ProtectSystem=strict" in unit and "NoNewPrivileges=yes" in unit
    assert "ProtectHome=yes" in unit
    assert "CapabilityBoundingSet=\n" in unit and "AmbientCapabilities=\n" in unit
    assert "IPAddressDeny=any" in unit and "IPAddressAllow=localhost" in unit
    assert "SocketBindDeny=any" in unit and "SocketBindAllow=ipv4:tcp:8765" in unit
    assert f"BindReadOnlyPaths={campaign['campaign_root']}" in unit
    assert "ReadWritePaths=" not in unit
    assert "EnvironmentFile=" not in unit
    assert "LoadCredential=" not in unit
    assert "PassEnvironment=" not in unit
    assert str(campaign["collector_service"]) in unit.split("[Service]", maxsplit=1)[0]


def test_v8_unit_binds_final_identity_and_port() -> None:
    unit = binding_pack.render_systemd_unit(_v8_plan())
    assert "h1-dashboard-serve --port 18080" in unit
    assert "SocketBindAllow=ipv4:tcp:18080" in unit
    assert "HYPERLAB_H1_DASHBOARD_SOURCE_COMMIT=" + "f" * 40 in unit
    assert "HYPERLAB_H1_COLLECTOR_SOURCE_COMMIT=926c878718c9f7d4095526061893e9f041d40c2b" in unit
    assert "BindReadOnlyPaths=/mnt/HC_Volume_106716684/hyperlab-h1/campaigns/" in unit


def test_windows_tunnel_is_strict_bounded_and_separate() -> None:
    tunnel = binding_pack.render_windows_tunnel(_plan())
    assert "LOCATION: Windows PowerShell on the Beelink" in tunnel
    assert "ssh.exe -N -T" in tunnel
    assert "ClearAllForwardings=yes" in tunnel
    assert "ExitOnForwardFailure=yes" in tunnel
    assert "-L '127.0.0.1:8765:127.0.0.1:8765'" in tunnel
    assert "ServerAliveInterval=30" in tunnel and "ServerAliveCountMax=3" in tunnel
    assert "CTRL+C:" in tunnel and "MAXIMUM_DURATION:" in tunnel and "PROMPTS:" in tunnel
    assert "systemctl" not in tunnel and "scp.exe" not in tunnel


def test_v8_operator_blocks_are_strictly_separated_and_ordered() -> None:
    handoff = _handoff()
    inventory_sha256 = "9" * 64
    transfer = binding_pack.render_windows_transfer(handoff, inventory_sha256)
    install = binding_pack.render_tabby_install(handoff, inventory_sha256)
    tunnel = binding_pack.render_windows_tunnel(_v8_plan())
    assert "STEP 01/03" in transfer and "scp.exe" in transfer
    assert f"$ExpectedInventorySha256 = '{inventory_sha256}'" in transfer
    assert transfer.index("Get-FileHash -LiteralPath $InventoryPath") < transfer.index(
        "Get-Content -LiteralPath $InventoryPath"
    )
    assert "Unsafe binding-files path" in transfer
    assert "systemctl" not in transfer and "ssh.exe -N -T" not in transfer
    assert "STEP 02/03" in install and "Tabby - VPS" in install
    assert f"printf '%s  %s\\n' '{inventory_sha256}' 'binding-files.sha256'" in install
    assert install.index("'binding-files.sha256' | sha256sum -c -") < install.index(
        "sha256sum -c binding-files.sha256"
    )
    assert 'systemctl show "$COLLECTOR_SERVICE"' in install
    for verb in ("start", "restart", "stop", "enable", "disable"):
        assert f'systemctl {verb} "$COLLECTOR_SERVICE"' not in install
    assert 'systemctl enable --now "$DASHBOARD_SERVICE"' in install
    assert "render-unit --plan" in install and "systemd-analyze verify" in install
    assert "sudo chown -R root:root \"$DASHBOARD_SOURCE_ROOT\"" in install
    assert "sudo chmod -R a-w \"$DASHBOARD_SOURCE_ROOT\"" in install
    assert "find \"$DASHBOARD_SOURCE_ROOT\" -type l" in install
    assert install.index("find \"$DASHBOARD_SOURCE_ROOT\" -type l") < install.index(
        "sudo chown -R root:root \"$DASHBOARD_SOURCE_ROOT\""
    )
    assert "LoadState=loaded" in install and "ActiveState=active" in install
    assert "SubState=running" in install and "NRestarts=0" in install
    assert "DASHBOARD_MAIN_PID =~ ^[1-9][0-9]*$" in install
    assert "--request HEAD" in install and "/api/h1/snapshot" in install
    assert 'payload["identity"]["campaign_manifest_sha256"]' in install
    assert 'payload["identity"]["collector_source_commit"]' in install
    assert 'payload["identity"]["dashboard_source_commit"]' in install
    assert "127.0.0.1:18080" in install and "PREPARED_NOT_STARTED" in install
    assert "RUNNING_HEALTHY" in install and "firewall" not in install.casefold()
    assert "STEP 03/03" in tunnel and "ssh.exe -N -T" in tunnel
    assert "-L '127.0.0.1:18080:127.0.0.1:18080'" in tunnel


def test_bootstrap_is_hash_locked_and_checks_binding_specific_cli() -> None:
    bootstrap = (OPS / "bootstrap-linux.sh").read_text(encoding="utf-8")
    assert "sys.version_info[:3] == (3, 12, 13)" in bootstrap
    assert "--require-hashes" in bootstrap and "--only-binary=:all:" in bootstrap
    assert "timeout --signal=INT --kill-after=60s 30m" in bootstrap
    assert "-m hyperlab h1-dashboard-serve --help" in bootstrap
    assert "research-data h1-collect" not in bootstrap


def test_windows_finalizer_is_post_commit_clean_branch_and_bundle_bound() -> None:
    script = (OPS / "New-H1V8DashboardBindingBundle.ps1").read_text(encoding="utf-8")
    assert "[ValidatePattern('^[0-9a-f]{40}$')]" in script
    assert "$ExpectedBranch = 'codex/h1-v7-dashboard-binding-v1'" in script
    assert "git -C $RepoRoot status --porcelain" in script
    assert "hyperlab-h1-v8-dashboard-binding-v1.bundle" in script
    assert "binding_pack.py') finalize" in script
    assert "--source-commit $Commit" in script
    assert "H1_V8_DASHBOARD_BINDING_WINDOWS_BUNDLE_FINALIZED_NOT_TRANSFERRED" in script


def test_git_helper_is_readonly_and_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, object] = {}

    def fake_run(command: list[str], **kwargs: object) -> SimpleNamespace:
        observed["command"] = command
        observed["environment"] = kwargs["env"]
        return SimpleNamespace(returncode=1, stdout="", stderr="refused")

    monkeypatch.setattr(binding_pack.subprocess, "run", fake_run)
    with pytest.raises(binding_pack.BindingPackError, match="refused"):
        binding_pack._git_output(Path("C:/synthetic"), "status", "--porcelain")
    assert observed["command"] == [
        "git",
        "-c",
        "safe.directory=C:\\synthetic",
        "-C",
        "C:\\synthetic",
        "status",
        "--porcelain",
    ]
    environment = observed["environment"]
    assert isinstance(environment, dict)
    assert environment["GIT_OPTIONAL_LOCKS"] == "0"


def test_source_causality_requires_direct_parent_ancestry_and_cherry_pick_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_commit = "f" * 40
    plan = _v8_plan(source_commit)
    expected = {
        ("rev-parse", "HEAD"): source_commit,
        ("branch", "--show-current"): "codex/h1-v7-dashboard-binding-v1",
        ("status", "--porcelain"): "",
        ("rev-parse", "cda0681b726fadba1a77bd72d2fca9f84dd14566^"): (
            "926c878718c9f7d4095526061893e9f041d40c2b"
        ),
        ("show", "-s", "--format=%B", "cda0681b726fadba1a77bd72d2fca9f84dd14566"): (
            "dashboard\n\n(cherry picked from commit "
            "decb0e08aeabff71859fad052b84bff4af0ed990)"
        ),
        ("bundle", "list-heads", str(OPS / "binding_pack.py")): (
            source_commit + " refs/heads/codex/h1-v7-dashboard-binding-v1"
        ),
    }

    def fake_git(_root: Path, *arguments: str) -> str:
        return expected[arguments]

    monkeypatch.setattr(binding_pack, "_git_output", fake_git)
    monkeypatch.setattr(
        binding_pack.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=b"", stderr=b""),
    )
    binding_pack._validate_source_causality(
        ROOT, plan=plan, bundle_path=OPS / "binding_pack.py"
    )


def test_collector_checkout_is_checked_readonly(monkeypatch: pytest.MonkeyPatch) -> None:
    root = ROOT
    expected_commit = "1" * 40
    calls: list[tuple[str, ...]] = []

    def fake_git(_root: Path, *arguments: str) -> str:
        calls.append(arguments)
        return expected_commit if arguments == ("rev-parse", "HEAD") else ""

    monkeypatch.setattr(binding_pack, "_git_output", fake_git)
    binding_pack._validate_collector_checkout(root, expected_commit)
    assert calls == [("rev-parse", "HEAD"), ("status", "--porcelain")]

    def dirty_git(_root: Path, *arguments: str) -> str:
        return expected_commit if arguments == ("rev-parse", "HEAD") else " M evidence.json"

    monkeypatch.setattr(binding_pack, "_git_output", dirty_git)
    with pytest.raises(binding_pack.BindingPackError, match="not clean"):
        binding_pack._validate_collector_checkout(root, expected_commit)


def test_pack_contains_no_order_route_secret_or_collector_mutation_surface() -> None:
    sources = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(OPS.iterdir()) if path.is_file()
    )
    assert "hyperliquid.exchange.Exchange" not in sources
    assert "PRIVATE_KEY=" not in sources and "MNEMONIC=" not in sources
    assert "0.0.0.0:18080" not in sources
    assert "BindReadOnlyPaths=" in sources and "ReadWritePaths=" not in sources
    for verb in ("start", "restart", "stop", "enable", "disable"):
        assert f"systemctl {verb} \"$COLLECTOR_SERVICE\"" not in sources


def test_pack_contains_no_stale_v7_campaign_identity() -> None:
    sources = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(OPS.iterdir()) if path.is_file()
    )
    for stale in (
        "e52a227b",
        "d56ea6960208968e3efa03c5",
        "44e7e163d65bce034aad49f87f9189bec25a00670bf016fa58937cc9b8f14ae5",
        "113548",
        "H1_V7",
    ):
        assert stale not in sources


def test_canonical_plan_bytes_are_reproducible() -> None:
    first = binding_pack.canonical_json_bytes(binding_pack.validate_plan(_plan())) + b"\n"
    second = binding_pack.canonical_json_bytes(binding_pack.validate_plan(_plan())) + b"\n"
    assert first == second
    assert hashlib.sha256(first).hexdigest() == hashlib.sha256(second).hexdigest()
