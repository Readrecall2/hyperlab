from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
from enum import Enum
from pathlib import Path
from types import SimpleNamespace

import pytest

from ops.prediction_markets_launch_v1 import (
    launch_pack,
    preflight,
    superseded_compat_proof,
)

ROOT = Path(__file__).resolve().parents[1]
OPS = ROOT / "ops" / "prediction_markets_launch_v1"
OLD_COMMIT = "bcb5280f87393992e2aa4528188009186cd8bdc3"


def _git(*arguments: str, cwd: Path = ROOT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *arguments],
        cwd=cwd,
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )


def _blob_row(path: Path, relative: Path) -> dict[str, object]:
    raw = path.read_bytes()
    return {
        "blob_sha1": hashlib.sha1(
            f"blob {len(raw)}\0".encode("ascii") + raw
        ).hexdigest(),
        "mode": "100644",
        "path": relative.as_posix(),
        "size": len(raw),
    }


def test_real_historical_preflight_reproduces_argparse_incompatibility(
    tmp_path: Path,
) -> None:
    historical = _git(
        "show",
        f"{OLD_COMMIT}:ops/prediction_markets_launch_v1/preflight.py",
    )
    assert historical.returncode == 0, historical.stderr
    old_preflight = tmp_path / "historical-preflight.py"
    old_preflight.write_text(historical.stdout, encoding="utf-8", newline="\n")

    refused = subprocess.run(
        [sys.executable, "-I", str(old_preflight), "runtime-import-admission"],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    assert refused.returncode == 2
    assert "invalid choice: 'runtime-import-admission'" in refused.stderr
    assert "superseded-runtime-compatibility" not in refused.stderr

    candidate = subprocess.run(
        [
            sys.executable,
            "-I",
            str(OPS / "preflight.py"),
            "superseded-runtime-compatibility",
            "--help",
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    assert candidate.returncode == 0, candidate.stderr
    assert "--candidate-handoff" in candidate.stdout
    assert "--candidate-source-root" in candidate.stdout


def test_real_historical_checkout_inventory_is_exact_and_dirty_state_is_refused(
    tmp_path: Path,
) -> None:
    source = tmp_path / "historical-source"
    cloned = _git("clone", "--quiet", "--no-checkout", str(ROOT), str(source))
    assert cloned.returncode == 0, cloned.stderr
    configured = _git("config", "core.longpaths", "true", cwd=source)
    assert configured.returncode == 0, configured.stderr
    checked_out = _git("checkout", "--quiet", "--detach", OLD_COMMIT, cwd=source)
    assert checked_out.returncode == 0, checked_out.stderr
    inventory = launch_pack.build_source_inventory(source, OLD_COMMIT)
    assert len(inventory["files"]) == 681
    assert (
        inventory["inventory_sha256"]
        == preflight._SUPERSEDED_RUNTIME_INVENTORY_SHA256
    )
    incoming = tmp_path / "historical-incoming"
    incoming.mkdir()
    inventory_path = incoming / "source-inventory.json"
    inventory_path.write_bytes(preflight.canonical_json_bytes(inventory) + b"\n")

    authenticated = preflight._authenticated_runtime_checkout(
        source_root=source,
        inventory_path=inventory_path,
        expected_commit=OLD_COMMIT,
        expected_inventory_sha256=preflight._SUPERSEDED_RUNTIME_INVENTORY_SHA256,
        label="superseded target",
    )
    assert authenticated == inventory

    wrong_commit = {**inventory, "commit": "f" * 40}
    inventory_path.write_bytes(preflight.canonical_json_bytes(wrong_commit) + b"\n")
    with pytest.raises(preflight.PreflightError, match="source commit diverged"):
        preflight._authenticated_runtime_checkout(
            source_root=source,
            inventory_path=inventory_path,
            expected_commit="f" * 40,
            expected_inventory_sha256=preflight._SUPERSEDED_RUNTIME_INVENTORY_SHA256,
            label="superseded target",
        )

    wrong_inventory = {
        **inventory,
        "files": [
            {**inventory["files"][0], "size": inventory["files"][0]["size"] + 1},  # type: ignore[index,operator]
            *inventory["files"][1:],  # type: ignore[index]
        ],
    }
    inventory_path.write_bytes(preflight.canonical_json_bytes(wrong_inventory) + b"\n")
    with pytest.raises(preflight.PreflightError, match="Git inventory diverged"):
        preflight._authenticated_runtime_checkout(
            source_root=source,
            inventory_path=inventory_path,
            expected_commit=OLD_COMMIT,
            expected_inventory_sha256=preflight._SUPERSEDED_RUNTIME_INVENTORY_SHA256,
            label="superseded target",
        )

    inventory_path.write_bytes(preflight.canonical_json_bytes(inventory) + b"\n")
    (source / "UNTRACKED_RUNTIME_MUTATION.txt").write_text(
        "SYNTHETIC/FIXTURE dirty target\n", encoding="utf-8"
    )
    with pytest.raises(preflight.PreflightError, match="not clean"):
        preflight._authenticated_runtime_checkout(
            source_root=source,
            inventory_path=inventory_path,
            expected_commit=OLD_COMMIT,
            expected_inventory_sha256=preflight._SUPERSEDED_RUNTIME_INVENTORY_SHA256,
            label="superseded target",
        )


def test_candidate_owned_adapter_authenticates_distinct_historical_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_source = tmp_path / "candidate-source"
    candidate_tool_path = (
        candidate_source / "ops" / "prediction_markets_launch_v1" / "preflight.py"
    )
    candidate_tool_path.parent.mkdir(parents=True)
    candidate_tool_path.write_text("# candidate tool fixture\n", encoding="utf-8")
    candidate_incoming = tmp_path / "candidate-incoming"
    candidate_incoming.mkdir()
    candidate_handoff_path = candidate_incoming / "handoff.json"
    candidate_inventory_path = candidate_incoming / "source-inventory.json"

    target_source = tmp_path / "historical-source"
    target_incoming = tmp_path / "historical-incoming"
    target_campaign = tmp_path / "historical-campaign"
    target_incoming.mkdir()
    target_campaign.joinpath("state").mkdir(parents=True)
    target_campaign.joinpath("polymarket").mkdir()
    target_campaign.joinpath("kalshi").mkdir()
    (target_campaign / "campaign-manifest.json").write_text(
        '{"fixture":"SYNTHETIC/FIXTURE"}\n', encoding="utf-8"
    )
    (target_campaign / "state" / "activation-receipt.json").write_text(
        '{"fixture":"SYNTHETIC/FIXTURE"}\n', encoding="utf-8"
    )
    venv_root = target_source / ".venv"
    site_packages = venv_root / "lib" / "python3.12" / "site-packages"
    site_packages.mkdir(parents=True)
    target_python = venv_root / "bin" / "python"
    target_python.parent.mkdir()
    target_python.write_text("SYNTHETIC/FIXTURE executable\n", encoding="utf-8")
    stdlib_root = tmp_path / "stdlib"
    stdlib_root.mkdir()

    source_files: dict[str, Path] = {}
    for name, relative in preflight._SUPERSEDED_RUNTIME_SOURCE_RELATIVE_FILES.items():
        path = target_source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"# historical module fixture: {name}\n", encoding="utf-8")
        source_files[name] = path
    dependency_files: dict[str, Path] = {}
    for name in preflight._SUPERSEDED_RUNTIME_VENV_MODULES:
        path = site_packages / f"{name}.py"
        path.write_text(f"# venv dependency fixture: {name}\n", encoding="utf-8")
        dependency_files[name] = path

    candidate_commit = "c" * 40
    candidate_inventory_hash = "d" * 64
    candidate_handoff = {
        "boundary": preflight.BOUNDARY,
        "schema_version": 1,
        "source_commit": candidate_commit,
        "source_inventory_sha256": candidate_inventory_hash,
    }
    suffix = preflight._SUPERSEDED_RUNTIME_SLUG.removeprefix("pm-")
    target_contract = {
        "campaign_root": str(target_campaign),
        "dashboard_port": 18081,
        "incoming_root": str(target_incoming),
        "namespace_probe_services": {
            venue: f"hyperlab-pm-{suffix}-{venue}-namespace-probe.service"
            for venue in ("polymarket", "kalshi")
        },
        "run_slug": preflight._SUPERSEDED_RUNTIME_SLUG,
        "services": {
            venue: f"hyperlab-pm-{suffix}-{venue}.service"
            for venue in ("polymarket", "kalshi", "dashboard")
        },
        "source_commit": OLD_COMMIT,
        "source_root": str(target_source),
    }
    candidate_handoff["superseded_campaign"] = target_contract
    target_handoff = {
        **target_contract,
        "boundary": preflight.BOUNDARY,
        "schema_version": 1,
        "source_inventory_sha256": preflight._SUPERSEDED_RUNTIME_INVENTORY_SHA256,
    }
    candidate_inventory = {
        "files": [
            _blob_row(
                candidate_tool_path,
                Path("ops/prediction_markets_launch_v1/preflight.py"),
            )
        ]
    }
    target_inventory = {
        "files": [
            _blob_row(path, preflight._SUPERSEDED_RUNTIME_SOURCE_RELATIVE_FILES[name])
            for name, path in source_files.items()
        ]
    }

    def fake_load_handoff(path: Path) -> dict[str, object]:
        return candidate_handoff if path == candidate_handoff_path else target_handoff

    def fake_checkout(**kwargs: object) -> dict[str, object]:
        return (
            candidate_inventory
            if kwargs["source_root"] == candidate_source
            else target_inventory
        )

    def helper(*_args: object, **_kwargs: object) -> None:
        return None
    row = {
        "entry_sha256": "e" * 64,
        "ordinal": 0,
        "terminal_result_sha256": None,
    }
    fake_context = SimpleNamespace(manifest={"fixture": "SYNTHETIC/FIXTURE"})
    runner_module = SimpleNamespace(
        __file__=str(source_files["ops.prediction_markets_launch_v1.runner"]),
        _validate_result=helper,
        canonical_json_bytes=preflight.canonical_json_bytes,
        load_campaign_context=lambda *_args: fake_context,
        read_ledger=lambda *_args: [row],
        sha256_bytes=preflight.sha256_bytes,
        validate_service_ledger_against_manifest=helper,
    )
    for name in preflight._SUPERSEDED_RUNTIME_HELPERS[
        "ops.prediction_markets_launch_v1.runner"
    ]:
        if not hasattr(runner_module, name):
            setattr(runner_module, name, helper)
    cockpit_module = SimpleNamespace(
        __file__=str(source_files["ops.prediction_markets_launch_v1.cockpit"])
    )
    for name in preflight._SUPERSEDED_RUNTIME_HELPERS[
        "ops.prediction_markets_launch_v1.cockpit"
    ]:
        setattr(cockpit_module, name, helper)
    modules: dict[str, object] = {
        "hyperlab": SimpleNamespace(__file__=str(source_files["hyperlab"])),
        "ops.prediction_markets_launch_v1.cockpit": cockpit_module,
        "ops.prediction_markets_launch_v1.preflight": SimpleNamespace(
            __file__=str(source_files["ops.prediction_markets_launch_v1.preflight"])
        ),
        "ops.prediction_markets_launch_v1.runner": runner_module,
        **{
            name: SimpleNamespace(__file__=str(path))
            for name, path in dependency_files.items()
        },
    }

    class Venue(Enum):
        POLYMARKET = "polymarket"
        KALSHI = "kalshi"

    def fake_import(name: str) -> object:
        if name == "hyperlab.research_data.envelope":
            return SimpleNamespace(Venue=Venue)
        return modules[name]

    monkeypatch.setattr(preflight, "load_handoff", fake_load_handoff)
    monkeypatch.setattr(
        preflight,
        "validate_install_layout",
        lambda *_args, **_kwargs: {"source_commit": candidate_commit},
    )
    monkeypatch.setattr(preflight, "_superseded_contract", lambda _value: target_contract)
    monkeypatch.setattr(preflight, "_authenticated_runtime_checkout", fake_checkout)
    monkeypatch.setattr(
        preflight,
        "_superseded_runtime_environment",
        lambda **_kwargs: (venv_root, target_python, (stdlib_root,)),
    )
    monkeypatch.setattr(preflight.importlib, "import_module", fake_import)
    monkeypatch.setattr(preflight, "__file__", str(candidate_tool_path))
    monkeypatch.setattr(preflight.sys, "path", [str(stdlib_root)])

    removed = {
        name: sys.modules.pop(name)
        for name in preflight._SUPERSEDED_RUNTIME_SOURCE_MODULES
        if name in sys.modules
    }
    try:
        report = preflight.superseded_runtime_compatibility(
            candidate_handoff_path,
            candidate_source,
            candidate_inventory_path,
        )
    finally:
        sys.modules.update(removed)

    assert report["adapter_id"] == preflight._SUPERSEDED_RUNTIME_ADAPTER_ID
    assert report["candidate_commit"] == candidate_commit
    assert report["target_commit"] == OLD_COMMIT
    assert report["no_historical_new_cli_invoked"] is True
    assert report["terminal_signal"] == (
        "PREDICTION_SUPERSEDED_RUNTIME_COMPATIBILITY_RUNTIME_GREEN"
    )
    assert set(report["modules"]) == set(modules)


def test_unknown_adapter_and_external_module_are_refused(tmp_path: Path) -> None:
    candidate = {"superseded_campaign": {"source_commit": "f" * 40}}
    with pytest.raises(preflight.PreflightError, match="unknown or divergent"):
        preflight._superseded_contract(candidate)

    source = tmp_path / "source"
    venv_root = source / ".venv"
    stdlib_root = tmp_path / "stdlib"
    external = tmp_path / "external" / "foreign.py"
    for path in (source, venv_root, stdlib_root, external.parent):
        path.mkdir(parents=True, exist_ok=True)
    external.write_text("# foreign module\n", encoding="utf-8")
    with pytest.raises(preflight.PreflightError, match="escaped"):
        preflight._runtime_module_class(
            external,
            source_root=source,
            venv_root=venv_root,
            stdlib_roots=(stdlib_root,),
            name="foreign",
        )

    untracked = source / "ignored_runtime_module.py"
    untracked.write_text("# SYNTHETIC/FIXTURE untracked source module\n", encoding="utf-8")
    with pytest.raises(preflight.PreflightError, match="not uniquely inventoried"):
        preflight._inventoried_source_file(
            untracked,
            source_root=source,
            inventory={"files": []},
            relative_path=Path("ignored_runtime_module.py"),
            label="superseded loaded source module: ignored_runtime_module",
        )


def test_superseded_runtime_environment_refuses_wrong_venv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_source = tmp_path / "historical-source"
    venv_root = target_source / ".venv"
    expected_python = venv_root / "bin" / "python"
    wrong_python = tmp_path / "foreign-venv" / "bin" / "python"
    for path in (expected_python, wrong_python):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("SYNTHETIC/FIXTURE python executable\n", encoding="utf-8")
    (venv_root / "pyvenv.cfg").write_text(
        "include-system-site-packages = false\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(preflight.sys, "platform", "linux")
    monkeypatch.setattr(preflight.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(preflight.sys, "version_info", (3, 12, 0))
    monkeypatch.setattr(preflight.sys, "executable", str(wrong_python))
    monkeypatch.setattr(preflight.sys, "prefix", str(venv_root))
    monkeypatch.setattr(preflight.sys, "base_prefix", str(tmp_path / "system-python"))
    monkeypatch.setattr(
        preflight.sys,
        "flags",
        SimpleNamespace(isolated=1, no_user_site=1),
    )
    monkeypatch.setenv("PYTHONNOUSERSITE", "1")

    with pytest.raises(preflight.PreflightError, match="Python isolation diverged"):
        preflight._superseded_runtime_environment(target_source=target_source)


def test_cutover_uses_candidate_tool_and_preserves_transaction_order() -> None:
    cutover = (OPS / "cutover.sh").read_text(encoding="utf-8")
    launch = (OPS / "launch_pack.py").read_text(encoding="utf-8")
    assert (
        '"$OLD_PYTHON" -I "$NEW_SOURCE/ops/prediction_markets_launch_v1/preflight.py"'
        in cutover
    )
    assert "superseded-runtime-compatibility" in cutover
    assert (
        '"$OLD_SOURCE/ops/prediction_markets_launch_v1/preflight.py" '
        "runtime-import-admission"
    ) not in cutover
    assert "sudo -n test -f" not in cutover
    assert "sudo -n cmp" not in cutover
    helper_guard = "if [[ $MODE == disarm-old || $MODE == restore-old ]]; then"
    assert helper_guard in cutover
    assert cutover.index(helper_guard) < cutover.index(
        "bounded systemd helper is absent or unsafe"
    )
    assert launch.index('cutover.sh" verify-old') < launch.index(
        'cutover.sh" disarm-old'
    )
    assert launch.index('cutover.sh" disarm-old') < launch.index(
        'install.sh" "$INCOMING_ROOT"'
    )
    for token in (
        "props.get('ActiveState')!='active'",
        "int(props.get('MainPID','0') or '0')<=0",
        "listener_verified",
        "FragmentPath",
        "NRestarts",
    ):
        assert token in cutover


def test_superseded_compatibility_proof_pack_round_trip_is_read_only(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "candidate-repository"
    source_ops = repository / "ops" / "prediction_markets_launch_v1"
    source_ops.mkdir(parents=True)
    for name in ("launch_pack.py", "superseded_compat_proof.py"):
        shutil.copy2(OPS / name, source_ops / name)
    initialized = _git("init", "--quiet", cwd=repository)
    assert initialized.returncode == 0, initialized.stderr
    for key, value in (
        ("user.email", "synthetic-proof@invalid.example"),
        ("user.name", "Synthetic Proof"),
        ("core.autocrlf", "false"),
        ("core.longpaths", "true"),
    ):
        configured = _git("config", key, value, cwd=repository)
        assert configured.returncode == 0, configured.stderr
    assert _git("add", ".", cwd=repository).returncode == 0
    assert _git("commit", "--quiet", "-m", "proof fixture", cwd=repository).returncode == 0
    commit = _git("rev-parse", "HEAD", cwd=repository).stdout.strip()
    assert _git(
        "branch",
        "-M",
        superseded_compat_proof.EXPECTED_BRANCH,
        cwd=repository,
    ).returncode == 0

    slug = "pm-20260828t220000z-c0decafe"
    pack = tmp_path / "compatibility-pack"
    pack.mkdir()
    bundle = pack / superseded_compat_proof.BUNDLE_NAME
    bundled = _git(
        "bundle",
        "create",
        str(bundle),
        f"refs/heads/{superseded_compat_proof.EXPECTED_BRANCH}",
        cwd=repository,
    )
    assert bundled.returncode == 0, bundled.stderr
    handoff = superseded_compat_proof.finalize(
        repo_root=repository,
        output_root=pack,
        bundle_path=bundle,
        source_commit=commit,
        run_slug=slug,
    )
    verified = superseded_compat_proof.verify_input(
        pack,
        require_remote_layout=False,
    )
    assert verified["source_commit"] == commit
    b1 = (pack / "operator" / "B1-tabby-superseded-readonly-proof.sh").read_text(
        encoding="utf-8"
    )
    assert "cutover.sh\" verify-old" in b1
    cutover = (OPS / "cutover.sh").read_text(encoding="utf-8")
    assert "if [[ $MODE == disarm-old || $MODE == restore-old ]]; then" in cutover
    for forbidden in (
        "sudo -",
        "systemctl stop",
        "systemctl start",
        "systemctl restart",
        "systemctl enable",
        "systemctl disable",
        "disarm-old",
        "install.sh",
        "prediction-collect",
        "curl ",
        "wget ",
    ):
        assert forbidden not in b1

    runtime_body = {
        "adapter_id": superseded_compat_proof.ADAPTER_ID,
        "candidate_commit": commit,
        "candidate_inventory_sha256": handoff["source_inventory_sha256"],
        "no_historical_new_cli_invoked": True,
        "target_commit": superseded_compat_proof.TARGET_COMMIT,
        "target_inventory_sha256": superseded_compat_proof.TARGET_INVENTORY_SHA256,
        "terminal_signal": (
            "PREDICTION_SUPERSEDED_RUNTIME_COMPATIBILITY_RUNTIME_GREEN"
        ),
    }
    runtime = {
        **runtime_body,
        "compatibility_sha256": superseded_compat_proof.sha256_bytes(
            superseded_compat_proof.canonical_json_bytes(runtime_body)
        ),
    }
    output = pack / "superseded-runtime-compatibility-verify-old.stdout"
    output.write_bytes(
        superseded_compat_proof.canonical_json_bytes(runtime)
        + b"\nPREDICTION_OLD_RAW_RECEIPTS_LEDGER_AUTHENTICATED\n"
        + b"PREDICTION_OLD_CAMPAIGN_FIVE_UNITS_AUTHENTICATED\n"
        + b"PREDICTION_OLD_CAMPAIGN_PREMUTATION_AUTHENTICATED\n"
    )
    report = superseded_compat_proof.finalize_output(pack, output)
    assert report["no_cutover"] is True
    assert report["terminal_signal"] == (
        "PREDICTION_SUPERSEDED_RUNTIME_COMPATIBILITY_GREEN_NO_CUTOVER"
    )
    evidence = tmp_path / "compatibility-evidence"
    evidence.mkdir()
    for name in (
        superseded_compat_proof.RUNTIME_REPORT_NAME,
        superseded_compat_proof.REPORT_NAME,
        f"{superseded_compat_proof.REPORT_NAME}.sha256",
        superseded_compat_proof.OUTPUT_INVENTORY_NAME,
        f"{superseded_compat_proof.OUTPUT_INVENTORY_NAME}.sha256",
    ):
        shutil.copy2(pack / name, evidence / name)
    retrieved = superseded_compat_proof.verify_output(pack, evidence)
    assert retrieved["candidate_commit"] == commit
    assert retrieved["terminal_signal"] == (
        "PREDICTION_WINDOWS_SUPERSEDED_COMPATIBILITY_RETRIEVED_AUTHENTICATED"
    )
