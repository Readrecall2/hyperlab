from __future__ import annotations

import copy
import hashlib
import json
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from ops.h1_dashboard_binding import binding_pack

ROOT = Path(__file__).resolve().parents[1]
OPS = ROOT / "ops" / "h1_dashboard_binding"
V8_INPUT_PATH = OPS / "binding-input-v8-v2.json"
V8_INPUT = json.loads(V8_INPUT_PATH.read_text(encoding="utf-8"))


def _plan() -> dict[str, object]:
    slug = "h1-synthetic-campaign"
    name = "h1-synthetic-dashboard-v2"
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
        "status": binding_pack.STATUS,
    }


def _v8_plan(source_commit: str = "f" * 40) -> dict[str, object]:
    return binding_pack.build_final_plan(V8_INPUT, source_commit)


def _handoff(source_commit: str = "f" * 40) -> dict[str, object]:
    return {
        "boundary": binding_pack.BOUNDARY,
        "bundle": {
            "filename": binding_pack.BUNDLE_FILENAME,
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


def _parent_snapshot() -> dict[str, object]:
    return {
        "parent_exists": True,
        "parent_is_directory": True,
        "parent_is_symlink": False,
        "parent_is_canonical": True,
        "parent_same_device": True,
        "parent_owner": "hyperlab:hyperlab",
        "parent_mode": "700",
        "children": [],
        "v1_is_directory": False,
        "v1_is_symlink": False,
        "v1_is_canonical": False,
        "v1_same_device": False,
        "v1_owner": "",
        "v1_mode": "",
        "v1_empty": False,
    }


def _exact_v1_residue_snapshot() -> dict[str, object]:
    snapshot = _parent_snapshot()
    snapshot.update(
        {
            "parent_owner": "root:root",
            "children": [binding_pack.EXPECTED_V1_BINDING_NAME],
            "v1_is_directory": True,
            "v1_is_canonical": True,
            "v1_same_device": True,
            "v1_owner": "hyperlab:hyperlab",
            "v1_mode": "700",
            "v1_empty": True,
        }
    )
    return snapshot


def _materialize_pack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, dict[str, object]]:
    output = tmp_path / "h1-v8-dashboard-binding-v2"
    output.mkdir()
    bundle = output / binding_pack.BUNDLE_FILENAME
    bundle.write_bytes(b"synthetic-test-bundle\n")
    monkeypatch.setattr(binding_pack, "_validate_source_causality", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        binding_pack,
        "portable_git_file_bytes",
        lambda root, relative: (root / relative).read_bytes().replace(b"\r\n", b"\n"),
    )
    monkeypatch.setattr(
        binding_pack,
        "build_handoff",
        lambda **kwargs: _handoff(str(kwargs["plan"]["provenance"]["source_commit"])),
    )
    result = binding_pack.finalize_binding_pack(
        repo_root=ROOT,
        input_path=V8_INPUT_PATH,
        bundle_path=bundle,
        output_root=output,
        source_commit="f" * 40,
    )
    return output, result


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
    assert "LOCATION: Windows PowerShell 5.1 on the Beelink" in tunnel
    assert "ssh.exe -N -T" in tunnel
    assert "ClearAllForwardings=yes" in tunnel
    assert "ExitOnForwardFailure=yes" in tunnel
    assert "-L '127.0.0.1:8765:127.0.0.1:8765'" in tunnel
    assert "ServerAliveInterval=30" in tunnel and "ServerAliveCountMax=3" in tunnel
    assert "CTRL+C:" in tunnel and "MAXIMUM_DURATION:" in tunnel and "PROMPTS:" in tunnel
    assert "systemctl" not in tunnel and "scp.exe" not in tunnel


def test_v8_operator_blocks_are_strictly_separated_and_ordered() -> None:
    handoff = _handoff()
    transfer = binding_pack.render_windows_transfer(handoff)
    install = binding_pack.render_tabby_install(handoff)
    tunnel = binding_pack.render_windows_tunnel(_v8_plan())
    assert "STEP A/3" in transfer and "scp.exe" in transfer
    assert "[ValidatePattern('^[0-9a-f]{64}$')]" in transfer
    assert "[string] $ExpectedInventorySha256" in transfer
    assert transfer.index("Get-FileHash -LiteralPath $InventoryPath") < transfer.index(
        "Get-Content -LiteralPath $InventoryPath"
    )
    assert "Unsafe binding-files path" in transfer
    assert "systemctl" not in transfer and "ssh.exe -N -T" not in transfer
    assert "STEP B/3" in install and "Tabby - VPS" in install
    assert "EXPECTED_INVENTORY_SHA256=$1" in install
    assert install.index("'binding-files.sha256' | sha256sum -c -") < install.index(
        "sha256sum -c binding-files.sha256"
    )
    assert 'systemctl show "$COLLECTOR_SERVICE"' in install
    for verb in ("start", "restart", "stop", "enable", "disable"):
        assert f'systemctl {verb} "$COLLECTOR_SERVICE"' not in install
    assert 'systemctl enable --now "$DASHBOARD_SERVICE"' in install
    assert "render-unit --plan" in install and "systemd-analyze verify" in install
    assert "chown -R" not in install and "chmod -R" not in install
    assert 'find "$DASHBOARD_SOURCE_ROOT" -xdev -type l' in install
    assert install.index('find "$DASHBOARD_SOURCE_ROOT" -xdev -type l') < install.index(
        'find "$DASHBOARD_SOURCE_ROOT" -xdev -exec chown'
    )
    assert "LoadState=loaded" in install and "ActiveState=active" in install
    assert "SubState=running" in install and "NRestarts=0" in install
    assert "DASHBOARD_MAIN_PID =~ ^[1-9][0-9]*$" in install
    assert "--request HEAD" in install and "/api/h1/snapshot" in install
    assert 'payload["identity"]["campaign_manifest_sha256"]' in install
    assert 'payload["identity"]["collector_source_commit"]' in install
    assert 'payload["identity"]["dashboard_source_commit"]' in install
    assert "127.0.0.1:18080" in install and "PREPARED_NOT_STARTED" in install
    assert "RUNNING_HEALTHY" in install and "INTERRUPTED_RECOVERABLE" in install
    assert "firewall" not in install.casefold()
    assert "STEP C/3" in tunnel and "ssh.exe -N -T" in tunnel
    assert "-L '127.0.0.1:18080:127.0.0.1:18080'" in tunnel


def test_v2_parent_creation_and_exact_v1_residue_repair_are_ordered() -> None:
    install = binding_pack.render_tabby_install(_handoff())
    parent_create = '"$DASHBOARD_SOURCE_PARENT"\n  PARENT_CREATED=1'
    leaf_create = '"$DASHBOARD_SOURCE_ROOT"\nassert_volume_dir "$DASHBOARD_SOURCE_ROOT"'
    repair = (
        "sudo chown --no-dereference 'hyperlab:hyperlab' "
        '"$DASHBOARD_SOURCE_PARENT"'
    )
    assert parent_create in install and leaf_create in install and repair in install
    pre_repair_v1 = "assert_volume_dir \"$V1_SOURCE_ROOT\" 'V1 dashboard source residue before parent repair'"
    assert pre_repair_v1 in install
    assert install.index(parent_create) < install.index(pre_repair_v1) < install.index(repair)
    assert install.index(repair) < install.index(leaf_create)
    assert "root-owned dashboard parent is not the exact V1 residue" in install
    assert "dashboard source parent contains foreign content" in install
    assert "V1 dashboard source residue contains foreign content" in install
    assert 'rm ' not in install and 'rmdir ' not in install
    assert 'findmnt -rn -T "$VOLUME_MOUNT" -o SOURCE' in install
    assert 'stat -c %d "$VOLUME_MOUNT"' in install


def test_parent_admission_accepts_absent_and_exact_root_owned_v1_residue() -> None:
    absent = _parent_snapshot()
    absent["parent_exists"] = False
    absent["parent_is_directory"] = False
    assert binding_pack.admit_dashboard_source_parent(absent) == "CREATE_PARENT_THEN_V2_LEAF"

    residue = _exact_v1_residue_snapshot()
    assert binding_pack.admit_dashboard_source_parent(residue) == (
        "REPAIR_PARENT_ONLY_THEN_CREATE_V2_LEAF"
    )


@pytest.mark.parametrize(
    ("updates", "message"),
    (
        ({"parent_is_symlink": True}, "unsafe"),
        ({"parent_same_device": False}, "unsafe"),
        ({"parent_owner": "other:other"}, "owner"),
        ({"parent_mode": "755"}, "mode"),
        ({"children": ["foreign"]}, "foreign"),
    ),
)
def test_parent_admission_refuses_symlink_device_owner_mode_or_foreign_content(
    updates: dict[str, object], message: str
) -> None:
    snapshot = _parent_snapshot()
    snapshot.update(updates)
    with pytest.raises(binding_pack.BindingPackError, match=message):
        binding_pack.admit_dashboard_source_parent(snapshot)


@pytest.mark.parametrize(
    "updates",
    (
        {"v1_is_symlink": True},
        {"v1_same_device": False},
        {"v1_owner": "root:root"},
        {"v1_mode": "755"},
        {"v1_empty": False},
    ),
)
def test_parent_repair_refuses_any_v1_residue_drift_before_mutation(
    updates: dict[str, object],
) -> None:
    snapshot = _exact_v1_residue_snapshot()
    snapshot.update(updates)
    with pytest.raises(binding_pack.BindingPackError, match="V1 dashboard source residue"):
        binding_pack.admit_dashboard_source_parent(snapshot)


def test_materialized_pack_inventories_native_lf_operator_scripts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output, result = _materialize_pack(tmp_path, monkeypatch)
    expected = {
        "operator/A-windows-transfer.ps1",
        "operator/B-tabby-vps-install.sh",
        "operator/C-windows-tunnel.ps1",
    }
    lines = (output / "binding-files.sha256").read_text(encoding="ascii").splitlines()
    inventory = {line.split("  ", maxsplit=1)[1]: line.split("  ", maxsplit=1)[0] for line in lines}
    assert expected <= set(inventory)
    assert result["inventory_sha256"] == hashlib.sha256(
        (output / "binding-files.sha256").read_bytes()
    ).hexdigest()
    for relative, expected_sha in inventory.items():
        payload = (output / relative).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == expected_sha
        if Path(relative).suffix in {".ps1", ".sh"}:
            assert payload and b"\r" not in payload and b"\x00" not in payload
            assert not payload.startswith((b"\xef\xbb\xbf", b"\xff\xfe", b"\xfe\xff"))


def test_materialized_bash_reproduces_crlf_refusal_and_lf_selfcheck(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output, _ = _materialize_pack(tmp_path, monkeypatch)
    candidate = Path("C:/Program Files/Git/bin/bash.exe")
    bash = str(candidate) if candidate.is_file() else shutil.which("bash")
    if bash is None:
        pytest.skip("Bash is unavailable")
    script = output / "operator" / "B-tabby-vps-install.sh"
    lf_run = subprocess.run(
        [bash, str(script), "--self-check"], check=False, capture_output=True, text=True
    )
    assert lf_run.returncode == 0
    assert "STEP_B_SELFCHECK_GREEN" in lf_run.stdout
    crlf = tmp_path / "B-tabby-vps-install-crlf.sh"
    crlf.write_bytes(script.read_bytes().replace(b"\n", b"\r\n"))
    assert b"set -Eeuo pipefail\r\n" in crlf.read_bytes()
    with pytest.raises(binding_pack.BindingPackError, match="LF-only shell bytes"):
        binding_pack.validate_lf_shell_bytes(crlf.read_bytes(), label=crlf.name)


def test_materialized_powershell_scripts_execute_from_another_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output, _ = _materialize_pack(tmp_path, monkeypatch)
    powershell = shutil.which("powershell.exe") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("Windows PowerShell is unavailable")
    unrelated = tmp_path / "unrelated-cwd"
    unrelated.mkdir()
    for name, arguments, signal in (
        (
            "A-windows-transfer.ps1",
            ["-ExpectedInventorySha256", "0" * 64, "-SelfCheck"],
            "STEP_A_SELFCHECK_GREEN",
        ),
        ("C-windows-tunnel.ps1", ["-SelfCheck"], "STEP_C_SELFCHECK_GREEN"),
    ):
        completed = subprocess.run(
            [powershell, "-NoLogo", "-NoProfile", "-File", str(output / "operator" / name), *arguments],
            cwd=unrelated,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, completed.stderr
        assert signal in completed.stdout


def test_bootstrap_is_hash_locked_and_checks_binding_specific_cli() -> None:
    bootstrap = (OPS / "bootstrap-linux-v2.sh").read_text(encoding="utf-8")
    assert "sys.version_info[:3] == (3, 12, 13)" in bootstrap
    assert "--require-hashes" in bootstrap and "--only-binary=:all:" in bootstrap
    assert "timeout --signal=INT --kill-after=60s 30m" in bootstrap
    assert "-m hyperlab h1-dashboard-serve --help" in bootstrap
    assert "research-data h1-collect" not in bootstrap


def test_windows_finalizer_is_post_commit_clean_branch_and_bundle_bound() -> None:
    script = (OPS / "New-H1V8DashboardBindingV2Bundle.ps1").read_text(encoding="utf-8")
    assert "[ValidatePattern('^[0-9a-f]{40}$')]" in script
    assert "$ExpectedBranch = 'codex/h1-v7-dashboard-binding-v1'" in script
    assert "git -C $RepoRoot status --porcelain" in script
    assert binding_pack.BUNDLE_FILENAME in script
    assert "binding_pack.py') finalize" in script
    assert "--source-commit $Commit" in script
    assert "H1_V8_DASHBOARD_BINDING_V2_WINDOWS_BUNDLE_FINALIZED_NOT_TRANSFERRED" in script


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
