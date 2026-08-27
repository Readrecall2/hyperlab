from __future__ import annotations

import copy
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from ops.h1_dashboard_binding import binding_pack

ROOT = Path(__file__).resolve().parents[1]
OPS = ROOT / "ops" / "h1_dashboard_binding"


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
            },
            "manifest_sha256": "1" * 64,
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
    }


def test_generic_pack_ships_no_campaign_plan_and_requires_external_identity() -> None:
    assert not list(OPS.glob("*.json"))
    assert binding_pack.validate_plan(_plan())["schema_version"] == 1
    sources = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(OPS.iterdir()) if path.is_file()
    )
    assert "H1_DASHBOARD_GENERIC_INTEGRATION_READY_AWAITING_V8_IDENTITY" in sources


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


def test_bootstrap_is_hash_locked_and_checks_binding_specific_cli() -> None:
    bootstrap = (OPS / "bootstrap-linux.sh").read_text(encoding="utf-8")
    assert "sys.version_info[:3] == (3, 12, 13)" in bootstrap
    assert "--require-hashes" in bootstrap and "--only-binary=:all:" in bootstrap
    assert "timeout --signal=INT --kill-after=60s 30m" in bootstrap
    assert "-m hyperlab h1-dashboard-serve --help" in bootstrap
    assert "research-data h1-collect" not in bootstrap


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
        "-C",
        "C:\\synthetic",
        "status",
        "--porcelain",
    ]
    environment = observed["environment"]
    assert isinstance(environment, dict)
    assert environment["GIT_OPTIONAL_LOCKS"] == "0"


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
    assert "0.0.0.0:8765" not in sources
    assert "BindReadOnlyPaths=" in sources and "ReadWritePaths=" not in sources
    for verb in ("start", "restart", "stop", "enable", "disable"):
        assert f"systemctl {verb} \"$COLLECTOR_SERVICE\"" not in sources


def test_canonical_plan_bytes_are_reproducible() -> None:
    first = binding_pack.canonical_json_bytes(binding_pack.validate_plan(_plan())) + b"\n"
    second = binding_pack.canonical_json_bytes(binding_pack.validate_plan(_plan())) + b"\n"
    assert first == second
    assert hashlib.sha256(first).hexdigest() == hashlib.sha256(second).hexdigest()
