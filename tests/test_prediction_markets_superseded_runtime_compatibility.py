from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import textwrap
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


def _write_pinned_object(path: Path, value: object) -> None:
    raw = preflight.canonical_json_bytes(value) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    path.with_name("handoff.sha256").write_text(
        f"{preflight.sha256_bytes(raw)}  handoff.json\n",
        encoding="ascii",
    )


def _commit_fixture(root: Path, message: str) -> str:
    assert _git("init", "--quiet", cwd=root).returncode == 0
    for key, value in (
        ("user.email", "superseded-alias-fixture@invalid.example"),
        ("user.name", "Superseded Alias Fixture"),
        ("core.autocrlf", "false"),
        ("core.longpaths", "true"),
    ):
        assert _git("config", key, value, cwd=root).returncode == 0
    assert _git("add", ".", cwd=root).returncode == 0
    committed = _git("commit", "--quiet", "-m", message, cwd=root)
    assert committed.returncode == 0, committed.stderr
    return _git("rev-parse", "HEAD", cwd=root).stdout.strip()


def _instrument_alias_candidate(
    source: str,
    *,
    target_commit: str,
    target_inventory_sha256: str,
    preload_alias: bool = False,
) -> str:
    marker = '\nif __name__ == "__main__":\n'
    assert source.count(marker) == 1
    preload = "import multiprocessing as _fixture_multiprocessing\n" if preload_alias else ""
    injection = f'''

# SYNTHETIC/FIXTURE: portable subprocess harness; production paths stay unchanged.
_SUPERSEDED_RUNTIME_COMMIT = {target_commit!r}
_SUPERSEDED_RUNTIME_INVENTORY_SHA256 = {target_inventory_sha256!r}

def _alias_fixture_install_layout(handoff, *, handoff_path, trusted_source_root=None):
    source_commit = handoff.get("source_commit")
    if not isinstance(source_commit, str):
        raise PreflightError("fixture candidate commit is absent")
    return {{"source_commit": source_commit}}

def _alias_fixture_contract(handoff):
    value = handoff.get("superseded_campaign")
    if not isinstance(value, dict):
        raise PreflightError("fixture superseded contract is absent")
    return dict(value)

def _alias_fixture_environment(*, target_source):
    venv_root = _runtime_exact_directory(
        target_source / ".venv", label="fixture superseded runtime virtual environment"
    )
    executable = _runtime_reported_file(
        str(Path(sys.executable)), label="fixture superseded runtime Python executable"
    )
    if (
        not _runtime_path_within(executable, venv_root)
        or Path(sys.prefix).resolve(strict=True) != venv_root
        or sys.base_prefix == sys.prefix
        or sys.flags.isolated != 1
        or sys.flags.no_user_site != 1
        or os.environ.get("PYTHONNOUSERSITE") != "1"
    ):
        raise PreflightError("fixture superseded runtime Python isolation diverged")
    return venv_root, executable, (Path(sys.base_prefix).resolve(strict=True),)

validate_install_layout = _alias_fixture_install_layout
_superseded_contract = _alias_fixture_contract
_superseded_runtime_environment = _alias_fixture_environment
{preload}'''
    return source.replace(marker, injection + marker, 1)


def _create_alias_target(tmp_path: Path) -> dict[str, object]:
    target_source = tmp_path / "target-historical-source"
    target_ops = target_source / "ops" / "prediction_markets_launch_v1"
    target_hyperlab = target_source / "src" / "hyperlab"
    target_research = target_hyperlab / "research_data"
    target_ops.mkdir(parents=True)
    target_research.mkdir(parents=True)
    (target_source / ".gitignore").write_text(".venv/\n", encoding="utf-8")
    (target_hyperlab / "__init__.py").write_text(
        "# SYNTHETIC/FIXTURE historical hyperlab\n", encoding="utf-8"
    )
    (target_research / "__init__.py").write_text(
        "# SYNTHETIC/FIXTURE research package\n", encoding="utf-8"
    )
    (target_research / "envelope.py").write_text(
        textwrap.dedent(
            """
            from enum import Enum
            class Venue(Enum):
                POLYMARKET = "polymarket"
                KALSHI = "kalshi"
            """
        ).lstrip(),
        encoding="utf-8",
    )
    (target_ops / "preflight.py").write_text(
        "# SYNTHETIC/FIXTURE historical preflight without new CLI\n",
        encoding="utf-8",
    )
    (target_ops / "cockpit.py").write_text(
        "\n".join(
            f"def {name}(*args, **kwargs): return None"
            for name in preflight._SUPERSEDED_RUNTIME_HELPERS[
                "ops.prediction_markets_launch_v1.cockpit"
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    runner_helpers = "\n".join(
        f"def {name}(*args, **kwargs): return None"
        for name in preflight._SUPERSEDED_RUNTIME_HELPERS[
            "ops.prediction_markets_launch_v1.runner"
        ]
        if name not in {"read_ledger", "validate_service_ledger_against_manifest"}
    )
    (target_ops / "runner.py").write_text(
        textwrap.dedent(
            f"""
            import hashlib
            import json
            from types import SimpleNamespace

            def canonical_json_bytes(value):
                return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()

            def sha256_bytes(value):
                return hashlib.sha256(value).hexdigest()

            def load_campaign_context(*args, **kwargs):
                return SimpleNamespace(manifest={{"fixture": "SYNTHETIC/FIXTURE"}})

            def read_ledger(*args, **kwargs):
                return [{{"entry_sha256": "{'e' * 64}", "ordinal": 0, "terminal_result_sha256": None}}]

            def validate_service_ledger_against_manifest(*args, **kwargs):
                return None

            def _validate_result(*args, **kwargs):
                return {{}}

            {runner_helpers}
            """
        ).lstrip(),
        encoding="utf-8",
    )
    target_commit = _commit_fixture(target_source, "historical target fixture")
    target_inventory = launch_pack.build_source_inventory(target_source, target_commit)

    target_venv = target_source / ".venv"
    created = subprocess.run(
        [sys.executable, "-m", "venv", "--copies", str(target_venv)],
        capture_output=True,
        check=False,
        text=True,
        timeout=120,
    )
    assert created.returncode == 0, created.stderr
    target_python = (
        target_venv / "Scripts" / "python.exe"
        if os.name == "nt"
        else target_venv / "bin" / "python"
    )
    purelib_result = subprocess.run(
        [str(target_python), "-I", "-c", "import sysconfig;print(sysconfig.get_path('purelib'))"],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    assert purelib_result.returncode == 0, purelib_result.stderr
    purelib = Path(purelib_result.stdout.strip())
    for name in ("fastapi", "requests", "websocket"):
        (purelib / f"{name}.py").write_text(
            f"# SYNTHETIC/FIXTURE {name}\n", encoding="utf-8"
        )
    for package_name in ("uvicorn", "click"):
        specification = importlib.util.find_spec(package_name)
        assert specification is not None and specification.submodule_search_locations
        package_source = Path(next(iter(specification.submodule_search_locations)))
        shutil.copytree(package_source, purelib / package_name)

    target_incoming = tmp_path / "target-historical-incoming"
    target_campaign = tmp_path / "target-historical-campaign"
    target_incoming.mkdir()
    target_campaign.joinpath("state").mkdir(parents=True)
    for venue in ("polymarket", "kalshi"):
        target_campaign.joinpath(venue).mkdir()
        target_campaign.joinpath(venue, "ledger.jsonl").write_text(
            '{"fixture":"SYNTHETIC/FIXTURE"}\n', encoding="utf-8"
        )
    target_campaign.joinpath("campaign-manifest.json").write_text(
        '{"fixture":"SYNTHETIC/FIXTURE"}\n', encoding="utf-8"
    )
    target_campaign.joinpath("state", "activation-receipt.json").write_text(
        '{"fixture":"SYNTHETIC/FIXTURE"}\n', encoding="utf-8"
    )
    target_contract = {
        "campaign_root": str(target_campaign.resolve()),
        "dashboard_port": 18081,
        "incoming_root": str(target_incoming.resolve()),
        "namespace_probe_services": {
            "kalshi": "fixture-kalshi-probe.service",
            "polymarket": "fixture-polymarket-probe.service",
        },
        "run_slug": "pm-20260828t000000z-11111111",
        "services": {
            "dashboard": "fixture-dashboard.service",
            "kalshi": "fixture-kalshi.service",
            "polymarket": "fixture-polymarket.service",
        },
        "source_commit": target_commit,
        "source_root": str(target_source.resolve()),
    }
    target_handoff = {
        **target_contract,
        "boundary": preflight.BOUNDARY,
        "schema_version": 1,
        "source_inventory_sha256": target_inventory["inventory_sha256"],
    }
    _write_pinned_object(target_incoming / "handoff.json", target_handoff)
    (target_incoming / "source-inventory.json").write_bytes(
        preflight.canonical_json_bytes(target_inventory) + b"\n"
    )
    return {
        "contract": target_contract,
        "inventory": target_inventory,
        "purelib": purelib,
        "python": target_python,
        "source": target_source,
    }


def _create_alias_candidate(
    root: Path,
    source: str,
    *,
    target: dict[str, object],
    preload_alias: bool = False,
) -> dict[str, Path | str]:
    candidate_tool = root / "ops" / "prediction_markets_launch_v1" / "preflight.py"
    candidate_tool.parent.mkdir(parents=True)
    candidate_src = root / "src"
    candidate_src.mkdir()
    (candidate_src / ".gitkeep").write_text("", encoding="utf-8")
    target_inventory = target["inventory"]
    assert isinstance(target_inventory, dict)
    candidate_tool.write_text(
        _instrument_alias_candidate(
            source,
            target_commit=str(target["contract"]["source_commit"]),  # type: ignore[index]
            target_inventory_sha256=str(target_inventory["inventory_sha256"]),
            preload_alias=preload_alias,
        ),
        encoding="utf-8",
        newline="\n",
    )
    candidate_other = candidate_tool.with_name("candidate-other.py")
    candidate_other.write_text(
        "# SYNTHETIC/FIXTURE alternate candidate file\n", encoding="utf-8"
    )
    candidate_commit = _commit_fixture(root, "candidate compatibility fixture")
    candidate_inventory = launch_pack.build_source_inventory(root, candidate_commit)
    incoming = root.parent / f"{root.name}-incoming"
    incoming.mkdir()
    handoff = {
        "boundary": preflight.BOUNDARY,
        "schema_version": 1,
        "source_commit": candidate_commit,
        "source_inventory_sha256": candidate_inventory["inventory_sha256"],
        "superseded_campaign": target["contract"],
    }
    handoff_path = incoming / "handoff.json"
    _write_pinned_object(handoff_path, handoff)
    inventory_path = incoming / "source-inventory.json"
    inventory_path.write_bytes(preflight.canonical_json_bytes(candidate_inventory) + b"\n")
    return {
        "commit": candidate_commit,
        "handoff": handoff_path,
        "inventory": inventory_path,
        "other": candidate_other,
        "source": root,
        "tool": candidate_tool,
    }


def _set_alias_uvicorn(
    purelib: Path,
    mode: str,
    *,
    candidate: dict[str, Path | str],
) -> None:
    tool = Path(candidate["tool"])
    other = Path(candidate["other"])
    real_package = purelib / "uvicorn"
    saved_package = purelib / "_fixture_real_uvicorn"
    stub = purelib / "uvicorn.py"
    if mode == "green":
        if saved_package.exists():
            saved_package.rename(real_package)
        if stub.exists():
            stub.unlink()
        return
    if real_package.exists():
        real_package.rename(saved_package)
    variants = {
        "absent": "# alias intentionally absent\n",
        "different": (
            "import multiprocessing,sys,types\n"
            "value=types.ModuleType('__mp_main__')\n"
            "value.__file__=__file__\n"
            "sys.modules['__mp_main__']=value\n"
        ),
        "other_file": (
            "import multiprocessing,sys\n"
            f"sys.modules['__main__'].__file__={str(other)!r}\n"
        ),
        "other_candidate_module": (
            "import multiprocessing,sys\n"
            "sys.modules['candidate_extra']=sys.modules['__main__']\n"
        ),
        "modified": (
            "import multiprocessing,sys\n"
            "from pathlib import Path\n"
            f"Path({str(tool)!r}).write_text('# mutated candidate tool\\n',encoding='utf-8')\n"
        ),
    }
    stub.write_text(variants[mode], encoding="utf-8")


def _run_alias_candidate(
    candidate: dict[str, Path | str],
    target: dict[str, object],
    *,
    cwd: Path,
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PYTHONNOUSERSITE"] = "1"
    return subprocess.run(
        [
            str(target["python"]),
            "-I",
            str(candidate["tool"]),
            "superseded-runtime-compatibility",
            "--candidate-handoff",
            str(candidate["handoff"]),
            "--candidate-source-root",
            str(candidate["source"]),
            "--candidate-source-inventory",
            str(candidate["inventory"]),
        ],
        cwd=cwd,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
        timeout=90,
    )


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


def test_distinct_git_roots_exact_subprocess_admits_only_candidate_tool_alias(
    tmp_path: Path,
) -> None:
    target = _create_alias_target(tmp_path)
    baseline = _git(
        "show",
        "98566fab0ce8405bf591041f7a789f94acf6757e:"
        "ops/prediction_markets_launch_v1/preflight.py",
    )
    assert baseline.returncode == 0, baseline.stderr
    before_patch = _create_alias_candidate(
        tmp_path / "candidate-before-patch",
        baseline.stdout,
        target=target,
    )
    fixed = _create_alias_candidate(
        tmp_path / "candidate-fixed",
        (OPS / "preflight.py").read_text(encoding="utf-8"),
        target=target,
    )
    neutral = tmp_path / "neutral-cwd"
    neutral.mkdir()
    assert Path(before_patch["source"]) != Path(target["source"])
    assert Path(fixed["source"]) != Path(target["source"])
    assert before_patch["commit"] != target["contract"]["source_commit"]  # type: ignore[index]
    assert fixed["commit"] != target["contract"]["source_commit"]  # type: ignore[index]

    purelib = Path(target["purelib"])
    _set_alias_uvicorn(purelib, "green", candidate=before_patch)
    reproduced = _run_alias_candidate(before_patch, target, cwd=neutral)
    assert reproduced.returncode == 4
    assert (
        "runtime module escaped source, venv, and stdlib roots: "
        f"__mp_main__:{before_patch['tool']}"
    ) in reproduced.stderr

    _set_alias_uvicorn(purelib, "green", candidate=fixed)
    admitted = _run_alias_candidate(fixed, target, cwd=neutral)
    assert admitted.returncode == 0, admitted.stderr
    report = json.loads(admitted.stdout)
    alias = report["modules"]["__mp_main__"]
    assert alias["class"] == "candidate_tool"
    assert alias["alias_of"] == "__main__"
    assert alias["file"] == str(fixed["tool"])
    assert report["loaded_module_files_validated"] >= 1

    refusals = {
        "absent": "candidate tool alias was not created",
        "different": "candidate tool alias does not reference __main__",
        "other_file": "candidate tool alias file diverged",
        "other_candidate_module": "candidate_extra:",
        "modified": "candidate compatibility tool alias diverged from its Git blob",
    }
    original_tool = Path(fixed["tool"]).read_bytes()
    for mode, expected_error in refusals.items():
        _set_alias_uvicorn(purelib, mode, candidate=fixed)
        refused = _run_alias_candidate(fixed, target, cwd=neutral)
        assert refused.returncode == 4, (mode, refused.stdout, refused.stderr)
        assert expected_error in refused.stderr
        if mode == "modified":
            Path(fixed["tool"]).write_bytes(original_tool)
            assert not _git(
                "status", "--porcelain", "--untracked-files=all", cwd=Path(fixed["source"])
            ).stdout.strip()

    preloaded = _create_alias_candidate(
        tmp_path / "candidate-preloaded-alias",
        (OPS / "preflight.py").read_text(encoding="utf-8"),
        target=target,
        preload_alias=True,
    )
    _set_alias_uvicorn(purelib, "green", candidate=preloaded)
    preloaded_result = _run_alias_candidate(preloaded, target, cwd=neutral)
    assert preloaded_result.returncode == 4
    assert "candidate tool alias was preloaded" in preloaded_result.stderr

    fixed_handoff_path = Path(fixed["handoff"])
    fixed_handoff = json.loads(fixed_handoff_path.read_text(encoding="utf-8"))
    conflated = json.loads(json.dumps(fixed_handoff))
    conflated["superseded_campaign"]["source_root"] = str(fixed["source"])
    _write_pinned_object(fixed_handoff_path, conflated)
    _set_alias_uvicorn(purelib, "green", candidate=fixed)
    conflated_result = _run_alias_candidate(fixed, target, cwd=neutral)
    assert conflated_result.returncode == 4
    assert "candidate tool and superseded target source roots collide" in conflated_result.stderr
    _write_pinned_object(fixed_handoff_path, fixed_handoff)


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
        if name == "uvicorn":
            sys.modules["__mp_main__"] = sys.modules["__main__"]
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
    previous_main = sys.modules.get("__main__")
    previous_alias = sys.modules.pop("__mp_main__", None)
    sys.modules["__main__"] = SimpleNamespace(__file__=str(candidate_tool_path))
    try:
        report = preflight.superseded_runtime_compatibility(
            candidate_handoff_path,
            candidate_source,
            candidate_inventory_path,
        )
    finally:
        sys.modules.pop("__mp_main__", None)
        if previous_alias is not None:
            sys.modules["__mp_main__"] = previous_alias
        if previous_main is None:
            sys.modules.pop("__main__", None)
        else:
            sys.modules["__main__"] = previous_main
        sys.modules.update(removed)

    assert report["adapter_id"] == preflight._SUPERSEDED_RUNTIME_ADAPTER_ID
    assert report["candidate_commit"] == candidate_commit
    assert report["target_commit"] == OLD_COMMIT
    assert report["no_historical_new_cli_invoked"] is True
    assert report["terminal_signal"] == (
        "PREDICTION_SUPERSEDED_RUNTIME_COMPATIBILITY_RUNTIME_GREEN"
    )
    assert set(report["modules"]) == {*modules, "__mp_main__"}
    assert report["modules"]["__mp_main__"]["class"] == "candidate_tool"  # type: ignore[index]
    assert report["modules"]["__mp_main__"]["alias_of"] == "__main__"  # type: ignore[index]


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

    candidate_tool = {
        "class": "candidate_tool",
        "file": f"{handoff['source_root']}/ops/prediction_markets_launch_v1/preflight.py",
        "git_blob_sha1": "a" * 40,
        "relative_path": "ops/prediction_markets_launch_v1/preflight.py",
        "sha256": "b" * 64,
        "size": 123,
    }
    runtime_body = {
        "adapter_id": superseded_compat_proof.ADAPTER_ID,
        "candidate_commit": commit,
        "candidate_inventory_sha256": handoff["source_inventory_sha256"],
        "candidate_tool": candidate_tool,
        "loaded_module_files_validated": 1,
        "modules": {
            "__mp_main__": {**candidate_tool, "alias_of": "__main__"},
        },
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
    invalid_body = {**runtime_body, "modules": {}}
    invalid_runtime = {
        **invalid_body,
        "compatibility_sha256": superseded_compat_proof.sha256_bytes(
            superseded_compat_proof.canonical_json_bytes(invalid_body)
        ),
    }
    output.write_bytes(
        superseded_compat_proof.canonical_json_bytes(invalid_runtime)
        + b"\nPREDICTION_OLD_RAW_RECEIPTS_LEDGER_AUTHENTICATED\n"
        + b"PREDICTION_OLD_CAMPAIGN_FIVE_UNITS_AUTHENTICATED\n"
        + b"PREDICTION_OLD_CAMPAIGN_PREMUTATION_AUTHENTICATED\n"
    )
    with pytest.raises(
        superseded_compat_proof.CompatibilityProofError,
        match="candidate tool alias report binding diverged",
    ):
        superseded_compat_proof.finalize_output(pack, output)
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
