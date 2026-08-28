from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import venv
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath

import pytest

from ops.prediction_markets_launch_v1 import cockpit, launch_pack, preflight, runner

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
        "run_slug": "pm-20260827t120000z-deadbeef",
        "service_user": "hyperlab",
        "services": {
            "dashboard": "hyperlab-pm-20260827t120000z-deadbeef-dashboard.service",
            "kalshi": "hyperlab-pm-20260827t120000z-deadbeef-kalshi.service",
            "polymarket": "hyperlab-pm-20260827t120000z-deadbeef-polymarket.service",
        },
        "source_root": "/mnt/HC_Volume_106716684/hyperlab-prediction-markets/sources/pm-20260827t120000z-deadbeef",
        "volume_base": "/mnt/HC_Volume_106716684/hyperlab-prediction-markets",
        "volume_mount": "/mnt/HC_Volume_106716684",
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
    scripts = incoming / "scripts"
    scripts.mkdir()
    launch_script = scripts / "launch_pack.py"
    launch_script.write_text("# SYNTHETIC/FIXTURE authenticated source verifier\n", encoding="utf-8")
    cutover_script = scripts / "cutover.sh"
    cutover_script.write_bytes(
        b"#!/usr/bin/env bash\nset -Eeuo pipefail\nprintf 'SYNTHETIC/FIXTURE cutover verifier\\n'\n"
    )
    source_inventory = incoming / "source-inventory.json"
    source_inventory.write_text('{"fixture":"SYNTHETIC/FIXTURE"}\n', encoding="utf-8")
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
            {
                "path": "scripts/launch_pack.py",
                "sha256": preflight.sha256_bytes(launch_script.read_bytes()),
                "size": launch_script.stat().st_size,
            },
            {
                "path": "scripts/cutover.sh",
                "sha256": preflight.sha256_bytes(cutover_script.read_bytes()),
                "size": cutover_script.stat().st_size,
            },
            {
                "path": "source-inventory.json",
                "sha256": preflight.sha256_bytes(source_inventory.read_bytes()),
                "size": source_inventory.stat().st_size,
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
        "incoming_root": str(incoming),
        "run_slug": "pm-20260827t120000z-deadbeef",
        "schema_version": 1,
        "service_user": "hyperlab",
        "services": _handoff()["services"],
        "source_commit": "b" * 40,
        "source_inventory_sha256": "c" * 64,
        "source_root": str(volume_base / "sources" / "source-must-be-new"),
        "superseded_campaign": launch_pack._superseded_campaign_contract(),
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
    scripts = source / "ops" / "prediction_markets_launch_v1"
    scripts.mkdir(parents=True)
    for name in launch_pack._SCRIPTS:
        payload = (OPS / name).read_bytes()
        if name.endswith(".sh"):
            payload = launch_pack.validate_posix_shell_payload(payload, label=name)
        (scripts / name).write_bytes(payload)
    _run_git("add", ".", cwd=source)
    _run_git("commit", "--quiet", "-m", "synthetic bundle fixture", cwd=source)
    _run_git("bundle", "create", str(bundle), "HEAD", cwd=source)


def test_launch_pack_branch_override_authenticates_exact_local_ref(
    tmp_path: Path,
) -> None:
    source = tmp_path / "synthetic-branch-source"
    _run_git("init", "--quiet", str(source))
    _run_git("config", "user.email", "synthetic-fixture@invalid.example", cwd=source)
    _run_git("config", "user.name", "Synthetic Fixture", cwd=source)
    (source / "SYNTHETIC_FIXTURE.txt").write_text(
        "SYNTHETIC/FIXTURE only; no economic evidence.\n",
        encoding="utf-8",
    )
    _run_git("add", ".", cwd=source)
    _run_git("commit", "--quiet", "-m", "synthetic branch fixture", cwd=source)
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        check=True,
        cwd=source,
        text=True,
        timeout=30,
    ).stdout.strip()
    branch = "codex/prediction-markets-v3-independent-audit"
    _run_git("branch", branch, commit, cwd=source)

    launch_pack._authenticate_source_branch(
        source,
        expected_branch=branch,
        source_commit=commit,
    )
    with pytest.raises(launch_pack.LaunchPackError, match="target branch differs"):
        launch_pack._authenticate_source_branch(
            source,
            expected_branch=branch,
            source_commit="f" * 40,
        )
    with pytest.raises(launch_pack.LaunchPackError, match="expected branch is invalid"):
        launch_pack._authenticate_source_branch(
            source,
            expected_branch="-unsafe",
            source_commit=commit,
        )


def test_python_isolated_mode_ignores_pythonpath_for_hyperlab(tmp_path: Path) -> None:
    fake = tmp_path / "fake-pythonpath"
    fake.mkdir()
    (fake / "hyperlab.py").write_text("SHOULD_NOT_IMPORT = True\n", encoding="utf-8")
    environment = {**os.environ, "PYTHONPATH": str(fake)}
    completed = subprocess.run(
        [sys.executable, "-I", "-c", "import hyperlab"],
        capture_output=True,
        check=False,
        cwd=tmp_path,
        env=environment,
        text=True,
        timeout=30,
    )
    assert completed.returncode != 0
    assert "No module named 'hyperlab'" in completed.stderr


def test_install_hyperlab_entrypoint_uses_explicit_isolated_source_root(
    tmp_path: Path,
) -> None:
    source = tmp_path / "authenticated-source"
    package = source / "src" / "hyperlab"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "__main__.py").write_text(
        """import json,sys
from pathlib import Path
Path(sys.argv[1]).write_text(json.dumps({'argv':sys.argv[1:],'file':__file__}),encoding='utf-8')
""",
        encoding="utf-8",
    )
    untrusted = tmp_path / "untrusted-cwd"
    (untrusted / "hyperlab").mkdir(parents=True)
    (untrusted / "hyperlab" / "__init__.py").write_text("", encoding="utf-8")
    (untrusted / "hyperlab" / "__main__.py").write_text(
        "raise SystemExit('UNTRUSTED_HYPERLAB_IMPORTED')\n",
        encoding="utf-8",
    )
    output = tmp_path / "entrypoint.json"
    bridge = (
        'import runpy,sys; source=sys.argv.pop(1); '
        'sys.path[:0]=[source+"/src",source]; '
        'runpy.run_module("hyperlab",run_name="__main__")'
    )
    install = (OPS / "install.sh").read_text(encoding="utf-8")
    assert f"'{bridge}'" in install
    environment = {**os.environ, "PYTHONPATH": str(untrusted)}
    completed = subprocess.run(
        [sys.executable, "-I", "-c", bridge, str(source), str(output), "sentinel"],
        capture_output=True,
        check=False,
        cwd=untrusted,
        env=environment,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    record = json.loads(output.read_text(encoding="utf-8"))
    assert record["argv"] == [str(output), "sentinel"]
    assert Path(record["file"]).resolve() == (package / "__main__.py").resolve()


def test_runtime_import_admission_isolated_fresh_venv_from_untrusted_cwd(
    tmp_path: Path,
) -> None:
    runtime, handoff_path, source, inventory_path, handoff = (
        _isolated_runtime_import_fixture(tmp_path)
    )
    untrusted_cwd = tmp_path / "old-source-cwd"
    untrusted_cwd.mkdir()
    (untrusted_cwd / "hyperlab.py").write_text(
        "raise RuntimeError('cwd hyperlab must be ignored')\n", encoding="utf-8"
    )
    user_base = tmp_path / "fake-user-base"
    user_site = user_base / "Python" / "site-packages"
    user_site.mkdir(parents=True)
    (user_site / "hyperlab.py").write_text(
        "raise RuntimeError('user-site hyperlab must be ignored')\n", encoding="utf-8"
    )
    report_path = handoff_path.parent / "runtime-import-admission.json"
    completed = subprocess.run(
        [
            str(runtime),
            "-I",
            str(handoff_path.parent / "scripts" / "preflight.py"),
            "runtime-import-admission",
            "--handoff",
            str(handoff_path),
            "--source-root",
            str(source),
            "--source-inventory",
            str(inventory_path),
            "--report",
            str(report_path),
        ],
        capture_output=True,
        check=False,
        cwd=untrusted_cwd,
        env={
            **os.environ,
            "PYTHONNOUSERSITE": "1",
            "PYTHONPATH": os.pathsep.join((str(untrusted_cwd), str(user_site))),
            "PYTHONUSERBASE": str(user_base),
        },
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout)
    assert report_path.read_bytes() == preflight.canonical_json_bytes(report) + b"\n"
    assert report["terminal_signal"] == "PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN"
    assert report["isolated"] is True
    assert report["no_user_site"] is True
    assert report["modules"]["hyperlab"] == {
        "class": "source",
        "file": str(source / "src" / "hyperlab" / "__init__.py"),
    }
    for module_name in preflight._RUNTIME_VENV_MODULES:
        module_record = report["modules"][module_name]
        assert module_record["class"] == "venv"
        assert Path(module_record["file"]).is_relative_to(source / ".venv")
    preflight.validate_runtime_import_admission(
        report,
        source_root=source,
        source_commit=str(handoff["source_commit"]),
        inventory_sha256=str(handoff["source_inventory_sha256"]),
    )
    outside = tmp_path / "outside-venv" / "fastapi.py"
    outside.parent.mkdir()
    outside.write_text("SYNTHETIC_FIXTURE = True\n", encoding="utf-8")
    forged = json.loads(json.dumps(report))
    forged["modules"]["fastapi"]["file"] = str(outside)
    forged_body = {
        key: value for key, value in forged.items() if key != "admission_sha256"
    }
    forged["admission_sha256"] = preflight.sha256_bytes(
        preflight.canonical_json_bytes(forged_body)
    )
    with pytest.raises(preflight.PreflightError, match="escaped venv site-packages"):
        preflight.validate_runtime_import_admission(
            forged,
            source_root=source,
            source_commit=str(handoff["source_commit"]),
            inventory_sha256=str(handoff["source_inventory_sha256"]),
        )


def test_runtime_import_admission_refuses_symlink_and_external_module_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = tmp_path / "real-source"
    real.mkdir()
    linked = tmp_path / "linked-source"
    try:
        linked.symlink_to(real, target_is_directory=True)
    except OSError:
        linked.mkdir()
        original_is_symlink = Path.is_symlink
        monkeypatch.setattr(
            Path,
            "is_symlink",
            lambda path: path == linked or original_is_symlink(path),
        )
    with pytest.raises(preflight.PreflightError, match="symlinked"):
        preflight._runtime_exact_directory(linked, label="synthetic source")

    venv_root = tmp_path / "venv"
    source_root = tmp_path / "source"
    stdlib_root = tmp_path / "stdlib"
    for root in (venv_root, source_root, stdlib_root):
        root.mkdir()
    outside = tmp_path / "outside" / "hyperlab.py"
    outside.parent.mkdir()
    outside.write_text("SYNTHETIC_FIXTURE = True\n", encoding="utf-8")
    with pytest.raises(preflight.PreflightError, match="escaped"):
        preflight._runtime_module_class(
            outside,
            source_root=source_root,
            venv_root=venv_root,
            stdlib_roots=(stdlib_root,),
        )


def _git_bash_path(path: Path) -> str:
    resolved = path.resolve()
    drive = resolved.drive.removesuffix(":").lower()
    assert len(drive) == 1
    return f"/{drive}{resolved.as_posix()[2:]}"


def _powershell_51() -> Path:
    system_root = Path(os.environ.get("SYSTEMROOT", r"C:\Windows"))
    executable = (
        system_root
        / "System32"
        / "WindowsPowerShell"
        / "v1.0"
        / "powershell.exe"
    )
    if not executable.is_file():
        pytest.skip("Windows PowerShell 5.1 is unavailable")
    return executable


def _git_bash() -> Path:
    program_files = Path(os.environ.get("PROGRAMFILES", r"C:\Program Files"))
    executable = program_files / "Git" / "bin" / "bash.exe"
    if not executable.is_file():
        pytest.skip("Git Bash is unavailable")
    return executable


def _write_fake_command(fake_bin: Path, name: str, body: str) -> None:
    path = fake_bin / name
    path.write_text("#!/usr/bin/bash\nset -Eeuo pipefail\n" + body, encoding="utf-8")
    path.chmod(0o700)


def _operator_handoff_for_git_bash(
    tmp_path: Path,
    suffix: str,
    *,
    materialize_source_runtime: bool = True,
) -> dict[str, object]:
    run_slug = str(_handoff()["run_slug"])
    incoming = tmp_path / f"incoming-{suffix}" / run_slug
    source = tmp_path / f"source-{suffix}" / run_slug
    campaign = tmp_path / f"campaign-{suffix}" / run_slug
    volume = tmp_path / f"volume-{suffix}"
    incoming.mkdir(parents=True)
    if materialize_source_runtime:
        runtime = source / ".venv" / "bin" / "python"
        runtime.parent.mkdir(parents=True)
        runtime.write_text(
            """#!/usr/bin/bash
if [[ ${1:-} == -I && ${2:-} == -c ]]; then exec \"$HYPERLAB_REAL_PYTHON\" \"$@\"; fi
printf '{\"dashboard\":{\"listener_verified\":true,\"nrestarts\":0},\"first_slots\":{\"kalshi\":{\"terminal_health\":\"COMPLETE\"},\"polymarket\":{\"terminal_health\":\"COMPLETE\"}}}\n'
""",
            encoding="utf-8",
        )
        runtime.chmod(0o700)
    handoff = _handoff()
    handoff.update(
        {
            "bundle_filename": "launch.bundle",
            "bundle_sha256": "a" * 64,
            "campaign_root": _git_bash_path(campaign),
            "incoming_root": _git_bash_path(incoming),
            "source_commit": "b" * 40,
            "source_root": _git_bash_path(source),
            "volume_base": _git_bash_path(volume),
        }
    )
    return handoff


def _operator_fake_environment(tmp_path: Path) -> tuple[dict[str, str], Path]:
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    log = tmp_path / "operator-fakes.log"
    prepared_runtime_wrapper = tmp_path / "prepared-runtime-python"
    prepared_runtime_wrapper.write_text(
        "#!/usr/bin/bash\n"
        "printf 'venv-python|%s\\n' \"$*\" >> \"$HYPERLAB_FAKE_LOG\"\n"
        "if [[ ${1:-} == -I && ${3:-} == runtime-import-admission ]]; then\n"
        "  admission_exit=${HYPERLAB_FAKE_VENV_IMPORT_EXIT:-0}\n"
        "  (( admission_exit == 0 )) || exit \"$admission_exit\"\n"
        "  report=''\n"
        "  for ((index=1; index<=$#; index++)); do\n"
        "    if [[ ${!index} == --report ]]; then next=$((index+1)); report=${!next}; fi\n"
        "  done\n"
        "  [[ -z $report ]] || printf '{\"terminal_signal\":\"SYNTHETIC_FIXTURE_SEQUENCE_ONLY\"}\\n' > \"$report\"\n"
        "  printf '{\"terminal_signal\":\"SYNTHETIC_FIXTURE_SEQUENCE_ONLY\"}\\n'\n"
        "  exit 0\n"
        "fi\n"
        "exit \"${HYPERLAB_FAKE_VENV_PYTHON_EXIT:-0}\"\n",
        encoding="utf-8",
    )
    prepared_runtime_wrapper.chmod(0o700)
    _write_fake_command(
        fake_bin,
        "id",
        "[[ ${1:-} == -un ]] || exit 97\nprintf 'hyperlab\\n'\n",
    )
    _write_fake_command(
        fake_bin,
        "python3.12",
        """printf 'python|%s\\n' "$*" >> "$HYPERLAB_FAKE_LOG"
if [[ ${HYPERLAB_FAKE_PYTHON_MODE:-success} == forward ]]; then
  exec "$HYPERLAB_REAL_PYTHON" "$@"
fi
exit "${HYPERLAB_FAKE_PYTHON_EXIT:-0}"
""",
    )
    _write_fake_command(
        fake_bin,
        "sudo",
        """printf 'sudo|%s\n' "$*" >> "$HYPERLAB_FAKE_LOG"
if [[ ${HYPERLAB_FAKE_SUDO_MODE:-green} == foreground-refused && ${1:-} == -v ]]; then exit 1; fi
if [[ ${HYPERLAB_FAKE_SUDO_MODE:-green} == cache-expired && ${1:-} == -n ]]; then exit 1; fi
""",
    )
    _write_fake_command(
        fake_bin,
        "git",
        """printf 'git|%s\\n' "$*" >> "$HYPERLAB_FAKE_LOG"
if [[ ${1:-} == clone && ${2:-} == --no-checkout ]]; then
  mkdir -p -- "$4"
  if [[ -n ${HYPERLAB_FAKE_CLONE_TEMPLATE:-} ]]; then
    cp -R -- "$HYPERLAB_FAKE_CLONE_TEMPLATE/." "$4/"
  fi
fi
""",
    )
    _write_fake_command(
        fake_bin,
        "sleep",
        "printf 'sleep|%s\\n' \"$*\" >> \"$HYPERLAB_FAKE_LOG\"\n[[ -z ${HYPERLAB_FAKE_SLEEP_SECONDS:-} ]] || /usr/bin/sleep \"$HYPERLAB_FAKE_SLEEP_SECONDS\"\n",
    )
    install_preflight_helper = tmp_path / "install-preflight-fixture.py"
    install_preflight_helper.write_text(
        """import hashlib,json,os,sys
args=sys.argv[1:]
command=args[0]
report=args[args.index('--report')+1]
if command=='install-admission':
 incoming=os.environ['HYPERLAB_INCOMING_WINDOWS']
 services=json.loads(os.environ['HYPERLAB_SERVICES_JSON'])
 unit_names=sorted(os.listdir(os.path.join(incoming,'systemd')))
 hashes={service:hashlib.sha256(open(os.path.join(incoming,'systemd',service),'rb').read()).hexdigest() for service in unit_names}
 value={'boundary':'PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY','errors':[],'evidence':{'unit_sha256':hashes},'install_admissible':True,'recorded_at_utc':'2026-08-27T18:00:00.000000Z','schema_version':1,'terminal_signal':'PREDICTION_INSTALL_ADMISSION_GREEN'}
else:
 value={'activation_admissible':True,'boundary':'PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY','errors':[],'live':{'capacity':{'admitted':True}},'recorded_at_utc':'2026-08-27T18:00:01.000000Z','schema_version':1,'terminal_signal':'PREDICTION_COLLECTOR_ACTIVATION_GUARD_GREEN'}
with open(report,'xb') as handle: handle.write(json.dumps(value,separators=(',',':'),sort_keys=True).encode()+b'\\n')
print(json.dumps(value,separators=(',',':'),sort_keys=True))
""",
        encoding="utf-8",
    )
    _write_fake_command(
        fake_bin,
        "bash",
        """printf 'bash|%s\\n' "$*" >> "$HYPERLAB_FAKE_LOG"
case "${HYPERLAB_FAKE_BASH_MODE:-install}" in
  install)
    if [[ ${1:-} == */bootstrap-offline.sh ]]; then
      bootstrap_exit=${HYPERLAB_FAKE_BOOTSTRAP_EXIT:-0}
      if (( bootstrap_exit == 0 )); then
        if [[ -n ${HYPERLAB_REAL_ADMISSION_BOOTSTRAP:-} ]]; then
          "$HYPERLAB_REAL_PYTHON" "$HYPERLAB_REAL_ADMISSION_BOOTSTRAP" "$2" "$HYPERLAB_FAKE_LOG"
        else
          mkdir -p -- "$2/.venv/bin"
          cp -- "$HYPERLAB_FAKE_VENV_WRAPPER" "$2/.venv/bin/python"
          chmod 0700 "$2/.venv/bin/python"
        fi
        printf 'PREDICTION_OFFLINE_BOOTSTRAP_GREEN:%s\\n' "$2/.venv/bin/python"
      fi
      exit "$bootstrap_exit"
    fi
    if [[ ${1:-} == */cutover.sh ]]; then
      case "${2:-}" in
        verify-old) printf 'PREDICTION_OLD_CAMPAIGN_PREMUTATION_AUTHENTICATED\\n' ;;
        disarm-old)
          : > "$(dirname -- "$3")/cutover-old-premutation.json"
          printf 'PREDICTION_OLD_CAMPAIGN_DISARMED_EVIDENCE_PRESERVED\\n'
          ;;
        *) exit 93 ;;
      esac
      exit 0
    fi
    if [[ ${1:-} == */install.sh ]]; then
      install_exit=${HYPERLAB_FAKE_INSTALL_EXIT:-0}
      if (( install_exit == 0 )); then
        printf 'PREDICTION_INSTALL_ACTIVATION_GREEN\n'
      fi
      exit "$install_exit"
    fi
    ;;
  monitor)
    count=0
    [[ ! -f $HYPERLAB_FAKE_COUNTER ]] || count=$(<"$HYPERLAB_FAKE_COUNTER")
    count=$((count + 1))
    printf '%s' "$count" > "$HYPERLAB_FAKE_COUNTER"
    if (( count == 1 )); then
      printf '{"alert":false,"semantic_fingerprint_sha256":"%064d"}\\n' 0
    else
      printf '{"alert":false,"semantic_fingerprint_sha256":"%064d"}\\n' 1
    fi
    ;;
  monitor-exit)
    exit 4
    ;;
  monitor-invalid)
    printf 'SYNTHETIC/FIXTURE invalid monitor payload\n'
    ;;
  recovery)
    [[ ${1:-} == */rollback.sh && ${2:-} == recovery ]] || exit 96
    printf 'PREDICTION_RECOVERY_GREEN\\n'
    ;;
  rollback-new)
    [[ ${1:-} == */rollback.sh && ${2:-} == rollback ]] || exit 95
    printf 'PREDICTION_ROLLBACK_GREEN\\n'
    ;;
  restore-old)
    [[ ${1:-} == */cutover.sh ]] || exit 95
    case ${2:-} in
      restore-old) printf 'PREDICTION_OLD_CAMPAIGN_RESTORED_NO_SLOT_RETRY\\n' ;;
      verify-restored) printf 'PREDICTION_OLD_CAMPAIGN_FINAL_STATE_AUTHENTICATED_NO_NEW_COLLECTOR\\n' ;;
      *) exit 93 ;;
    esac
    ;;
  *) exit 94 ;;
esac
        """,
    )
    bash_environment = tmp_path / "strict-fake-path.sh"
    bash_environment.write_text(
        f"export PATH='{_git_bash_path(fake_bin)}':\"$PATH\"\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment.update(
        {
            "BASH_ENV": _git_bash_path(bash_environment),
            "HYPERLAB_FAKE_COUNTER": _git_bash_path(tmp_path / "monitor-counter"),
            "HYPERLAB_FAKE_VENV_WRAPPER": _git_bash_path(prepared_runtime_wrapper),
            "HYPERLAB_FAKE_LOG": _git_bash_path(log),
            "HYPERLAB_REAL_PYTHON": _git_bash_path(Path(sys.executable)),
            "HYPERLAB_REAL_WINDOWS_PATH": environment["PATH"],
            "PATH": str(fake_bin) + os.pathsep + environment["PATH"],
        }
    )
    return environment, log


def _run_git_bash_script(
    script: Path,
    *,
    cwd: Path,
    environment: dict[str, str],
    arguments: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    assert not (cwd / ".git").exists()
    return subprocess.run(
        [
            str(_git_bash()),
            "--noprofile",
            "--norc",
            _git_bash_path(script),
            *arguments,
        ],
        capture_output=True,
        check=False,
        cwd=cwd,
        env=environment,
        text=True,
        timeout=90,
    )


def _internal_install_fixture(
    tmp_path: Path,
    *,
    suffix: str,
    eligible_venues: tuple[str, ...] = ("polymarket", "kalshi"),
) -> tuple[Path, dict[str, object], Path, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    handoff = _operator_handoff_for_git_bash(
        tmp_path, suffix, materialize_source_runtime=False
    )
    incoming = Path(str(handoff["incoming_root"]).replace("/c/", "C:/", 1))
    source = Path(str(handoff["source_root"]).replace("/c/", "C:/", 1))
    campaign = Path(str(handoff["campaign_root"]).replace("/c/", "C:/", 1))
    source.joinpath(".venv", "bin").mkdir(parents=True)
    source.joinpath("config", "research").mkdir(parents=True)
    source_ops = source / "ops" / "prediction_markets_launch_v1"
    source_ops.mkdir(parents=True)
    (source_ops / "install.sh").write_bytes((OPS / "install.sh").read_bytes())
    (source_ops / "preflight.py").write_bytes((OPS / "preflight.py").read_bytes())
    runtime = source / ".venv" / "bin" / "python"
    runtime.write_text(
        """#!/usr/bin/bash
set -Eeuo pipefail
if [[ ${1:-} == -I && ${2:-} == - && ${3:-} == */handoff.json && $# == 4 ]]; then
  "$HYPERLAB_REAL_PYTHON" -c 'import json,sys; d=json.load(open(sys.argv[1],encoding="utf-8")); suffix=d["run_slug"].removeprefix("pm-"); print(d["source_root"]); print(d["campaign_root"]); print(d["source_commit"]); print(d["services"]["polymarket"]); print(d["services"]["kalshi"]); print(d["services"]["dashboard"]); print(f"hyperlab-pm-{suffix}-polymarket-namespace-probe.service"); print(f"hyperlab-pm-{suffix}-kalshi-namespace-probe.service")' "$3" | tr -d '\\015'
  exit "${PIPESTATUS[0]}"
fi
if [[ ${1:-} == -I && ${2:-} == - && ${3:-} == */host-preflight-report.json && $# == 3 ]]; then
  "$HYPERLAB_REAL_PYTHON" "$@" | tr -d '\\015'
  exit "${PIPESTATUS[0]}"
fi
if [[ ${1:-} == -I && ${2:-} == - && $# == 3 ]]; then
  exit 0
fi
if [[ ${1:-} == -I && ${2:-} == */preflight.py && ${3:-} == runtime-import-admission ]]; then
  printf '{"terminal_signal":"SYNTHETIC_FIXTURE_INSTALL_SEQUENCE_ONLY"}\n'
  exit 0
fi
if [[ ${1:-} == -I && ${2:-} == */preflight.py && ${3:-} == authenticate-namespace-probe-completion ]]; then
  preflight_windows=$(cygpath -w "$2")
  export MSYS2_ARG_CONV_EXCL='*'
  exec "$HYPERLAB_REAL_PYTHON" -I "$preflight_windows" "${@:3}"
fi
if [[ ${1:-} == -I && ${2:-} == */preflight.py ]]; then
  exec "$HYPERLAB_REAL_PYTHON" "$HYPERLAB_INSTALL_PREFLIGHT_HELPER" "${@:3}"
fi
if [[ ${1:-} == -I && ${2:-} == -c && ${3:-} == *'runpy.run_module("hyperlab"'* && ${5:-} == research-data && ${6:-} == prediction-prepare ]]; then
  output=''
  while (($#)); do
    if [[ $1 == --output-root ]]; then output=$2; break; fi
    shift
  done
  [[ -n $output ]]
  mkdir -p -- "$output"
  cp -- "$HYPERLAB_FIXTURE_MANIFEST" "$output/campaign-manifest.json"
  cp -- "$HYPERLAB_FIXTURE_MANIFEST_PIN" "$output/campaign-manifest.sha256"
  mkdir -p -- "$output/polymarket"
  printf 'SYNTHETIC/FIXTURE raw preservation sentinel\n' > "$output/polymarket/SYNTHETIC_RAW_PRESERVED.bin"
  exit 0
fi
if [[ ${HYPERLAB_FAIL_DIAGNOSTIC_PYTHON:-0} == 1 && -n ${PROPERTIES+x} && -n ${JOURNAL+x} ]]; then
  exit 97
fi
if [[ ${1:-} == - && -e ${HYPERLAB_DASHBOARD_ENABLE_MARKER:-/absent} && ! -e ${HYPERLAB_FIRST_HEALTH_MARKER:-/absent} ]]; then
  : > "$HYPERLAB_FIRST_HEALTH_MARKER"
fi
exec "$HYPERLAB_REAL_PYTHON" "$@"
""",
        encoding="utf-8",
    )
    runtime.chmod(0o700)
    pack = (
        ROOT
        / "ops"
        / "prediction_markets_candidate_v1"
        / "prediction-markets-v1-20260901t000000z-aa60c0ff"
    )
    for name in (
        "polymarket-public-contract-v1.json",
        "kalshi-public-contract-v1.json",
        "prediction-markets-candidate-v1.json",
    ):
        (source / "config" / "research" / name).write_text("{}\n", encoding="utf-8")
    (incoming / "systemd").mkdir()
    services = handoff["services"]
    assert isinstance(services, dict)
    suffix_value = str(handoff["run_slug"]).removeprefix("pm-")
    all_units = [
        *services.values(),
        f"hyperlab-pm-{suffix_value}-polymarket-namespace-probe.service",
        f"hyperlab-pm-{suffix_value}-kalshi-namespace-probe.service",
    ]
    for service in all_units:
        (incoming / "systemd" / str(service)).write_text(
            "[Unit]\nDescription=SYNTHETIC/FIXTURE operator harness\n",
            encoding="utf-8",
        )
    preflight_report = {
        "boundary": launch_pack.BOUNDARY,
        "eligible_venues": list(eligible_venues),
        "installation_admissible": True,
        "network": {
            venue: {
                "verdict": (
                    "NETWORK_PREFLIGHT_GREEN"
                    if venue in eligible_venues
                    else "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT"
                )
            }
            for venue in ("polymarket", "kalshi")
        },
        "terminal_signal": "PREDICTION_HOST_PREFLIGHT_GREEN",
    }
    (incoming / "host-preflight-report.json").write_bytes(
        preflight.canonical_json_bytes(preflight_report) + b"\n"
    )
    (incoming / "filesystem-fsync-report.json").write_text(
        '{"fixture":"SYNTHETIC/FIXTURE"}\n', encoding="utf-8"
    )
    handoff.update({"dashboard_port": 18081})
    (incoming / "handoff.json").write_bytes(
        preflight.canonical_json_bytes(handoff) + b"\n"
    )
    (incoming / "handoff.sha256").write_text(
        f"{preflight.sha256_bytes((incoming / 'handoff.json').read_bytes())}  handoff.json\n",
        encoding="ascii",
    )
    return incoming, handoff, campaign, pack


def _namespace_probe_payload_fixture(
    handoff: dict[str, object],
    venue: str,
    *,
    admitted: bool = True,
) -> dict[str, object]:
    campaign = PurePosixPath(str(handoff["campaign_root"]))
    incoming = PurePosixPath(str(handoff["incoming_root"]))
    volume_mount = PurePosixPath(
        str(handoff.get("volume_mount") or "/SYNTHETIC_FIXTURE/VOLUME_MOUNT")
    )
    volume_base = PurePosixPath(
        str(handoff.get("volume_base") or "/SYNTHETIC_FIXTURE/VOLUME_BASE")
    )
    source = PurePosixPath(str(handoff["source_root"]))
    venue_root = campaign / venue

    def mount(
        logical: PurePosixPath,
        *,
        device: str,
        mode: str,
        target: PurePosixPath,
        filesystem_root: str,
        source_name: str,
    ) -> dict[str, object]:
        return {
            "device_major_minor": device,
            "filesystem": "ext4",
            "filesystem_root": filesystem_root,
            "logical_path": logical.as_posix(),
            "mount": target.as_posix(),
            "options": [mode, "nosuid", "relatime"],
            "source": source_name,
            "stat_device_major_minor": device,
        }

    parent = mount(
        volume_mount,
        device="8:16",
        mode="ro",
        target=volume_mount,
        filesystem_root="/",
        source_name="/dev/sdb",
    )
    base = mount(
        volume_base,
        device="8:16",
        mode="ro",
        target=volume_mount,
        filesystem_root="/",
        source_name="/dev/sdb",
    )
    source_mount = mount(
        source,
        device="8:16",
        mode="ro",
        target=volume_mount,
        filesystem_root="/",
        source_name="/dev/sdb",
    )
    campaign_mount = mount(
        campaign,
        device="8:16",
        mode="ro",
        target=volume_mount,
        filesystem_root="/",
        source_name="/dev/sdb",
    )
    try:
        relative_venue = venue_root.relative_to(volume_mount)
        venue_fsroot = (PurePosixPath("/") / relative_venue).as_posix()
    except ValueError:
        # The real pack always places campaigns below the admitted volume.  The
        # install harness deliberately materializes its campaign under a
        # Windows-owned temporary root, so keep that synthetic layout explicit
        # instead of pretending it is a real /dev/sdb subpath.
        venue_fsroot = (
            PurePosixPath("/SYNTHETIC_FIXTURE") / campaign.name / venue
        ).as_posix()
    venue_mount = mount(
        venue_root,
        device="8:16",
        mode="rw",
        target=venue_root,
        filesystem_root=venue_fsroot,
        source_name=f"/dev/sdb[{venue_fsroot}]",
    )
    home = mount(
        PurePosixPath("/home"),
        device="8:1",
        mode="ro",
        target=PurePosixPath("/"),
        filesystem_root="/",
        source_name="/dev/sda1",
    )
    incoming_mount = mount(
        incoming,
        device="8:1",
        mode="ro",
        target=PurePosixPath("/"),
        filesystem_root="/",
        source_name="/dev/sda1",
    )
    observed = {
        "campaign": campaign_mount,
        "home": home,
        "incoming": incoming_mount,
        "source": source_mount,
        "venue": venue_mount,
        "volume_base": base,
        "volume_parent": parent,
    }
    checks: dict[str, object] = {
        "admitted_device_major_minor": "8:16",
        "incoming_readonly": incoming_mount,
        "parent_mount": parent,
        "readonly_roots": {
            "campaign": campaign_mount,
            "source": source_mount,
        },
        "venue": venue,
        "venue_mount": venue_mount,
        "volume_base_readonly": base,
        "write_surface": {
            "directory_fsync": True,
            "exclusive_create": True,
            "file_fsync": True,
            "probe_removed": True,
            "root": venue_root.as_posix(),
        },
    }
    return {
        "boundary": launch_pack.BOUNDARY,
        "checks": checks if admitted else {},
        "errors": [] if admitted else ["SYNTHETIC/FIXTURE namespace refused"],
        "namespace_admissible": admitted,
        "observed_mounts": observed,
        "recorded_at_utc": "2026-08-28T01:35:00.000000Z",
        "schema_version": 1,
        "terminal_signal": (
            "PREDICTION_RUNNER_NAMESPACE_GREEN"
            if admitted
            else "PREDICTION_RUNNER_NAMESPACE_REFUSED"
        ),
        "venue": venue,
    }


def _internal_install_environment(
    tmp_path: Path,
    *,
    handoff: dict[str, object],
    pack: Path,
    failed_service: str | None = None,
    terminal_exit_service: str | None = None,
    clean_exit_service: str | None = None,
    namespace_probe_failure: str | None = None,
    namespace_probe_show_failure: str | None = None,
    namespace_probe_exec_main_code: str = "0",
    namespace_probe_property_mode: str = "green",
    namespace_probe_payload_mode: str | None = None,
    cleanup_failure_service: str | None = None,
    fail_diagnostic_encoding: bool = False,
    monitor_failure: str | None = None,
    collector_guard_failure: bool = False,
    monitor_eligible_venues: tuple[str, ...] = ("polymarket", "kalshi"),
    mutate_incoming_eligibility: bool = False,
) -> tuple[dict[str, str], Path]:
    fake_bin = tmp_path / "internal-fake-bin"
    fake_bin.mkdir()
    state_root = tmp_path / "systemd-state"
    state_root.mkdir()
    (state_root / "root-units").mkdir()
    log = tmp_path / "internal-operator.log"
    install_preflight_helper = tmp_path / "install-preflight-fixture.py"
    install_preflight_helper.write_text(
        """import hashlib,json,os,sys
args=sys.argv[1:]
command=args[0]
report=args[args.index('--report')+1]
if command=='install-admission':
 incoming=os.environ['HYPERLAB_INCOMING_WINDOWS']
 services=json.loads(os.environ['HYPERLAB_SERVICES_JSON'])
 unit_names=sorted(os.listdir(os.path.join(incoming,'systemd')))
 hashes={service:hashlib.sha256(open(os.path.join(incoming,'systemd',service),'rb').read()).hexdigest() for service in unit_names}
 value={'boundary':'PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY','errors':[],'evidence':{'unit_sha256':hashes},'install_admissible':True,'recorded_at_utc':'2026-08-27T18:00:00.000000Z','schema_version':1,'terminal_signal':'PREDICTION_INSTALL_ADMISSION_GREEN'}
else:
 refused=os.environ.get('HYPERLAB_COLLECTOR_GUARD_FAILURE')=='1'
 if os.environ.get('HYPERLAB_MUTATE_INCOMING_ELIGIBILITY')=='1':
  path=os.path.join(os.environ['HYPERLAB_INCOMING_WINDOWS'],'host-preflight-report.json')
  host=json.load(open(path,encoding='utf-8'))
  host['eligible_venues']=['polymarket','kalshi']
  host['network']={venue:{'verdict':'NETWORK_PREFLIGHT_GREEN'} for venue in ('polymarket','kalshi')}
  with open(path,'wb') as handle: handle.write(json.dumps(host,separators=(',',':'),sort_keys=True).encode()+b'\\n')
 value={'activation_admissible':not refused,'boundary':'PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY','errors':['SYNTHETIC/FIXTURE capacity reservation no longer proven'] if refused else [],'live':{'capacity':{'admitted':not refused}},'recorded_at_utc':'2026-08-27T18:00:01.000000Z','schema_version':1,'terminal_signal':'PREDICTION_COLLECTOR_ACTIVATION_GUARD_REFUSED' if refused else 'PREDICTION_COLLECTOR_ACTIVATION_GUARD_GREEN'}
with open(report,'xb') as handle: handle.write(json.dumps(value,separators=(',',':'),sort_keys=True).encode()+b'\\n')
print(json.dumps(value,separators=(',',':'),sort_keys=True))
if command!='install-admission' and refused: raise SystemExit(4)
""",
        encoding="utf-8",
    )
    _write_fake_command(fake_bin, "id", "[[ ${1:-} == -un ]]\nprintf 'hyperlab\\n'\n")
    _write_fake_command(
        fake_bin,
        "python3.12",
        "\"$HYPERLAB_REAL_PYTHON\" \"$@\" | tr -d '\\r'\nexit \"${PIPESTATUS[0]}\"\n",
    )
    _write_fake_command(
        fake_bin,
        "git",
        """printf 'git|%s\n' "$*" >> "$HYPERLAB_FAKE_LOG"
if [[ ${1:-} == rev-parse && ${2:-} == HEAD ]]; then
  printf '%s\n' "$HYPERLAB_SOURCE_COMMIT"
fi
""",
    )
    _write_fake_command(
        fake_bin,
        "systemd-analyze",
        "printf 'systemd-analyze|%s\\n' \"$*\" >> \"$HYPERLAB_FAKE_LOG\"\n",
    )
    _write_fake_command(
        fake_bin,
        "install",
        """printf 'install|%s\n' "$*" >> "$HYPERLAB_FAKE_LOG"
if [[ ${1:-} == -d && ${2:-} == -m ]]; then
  shift 3
  mkdir -p -- "$@"
else
  [[ ${1:-} == -m ]]
  shift 2
  cp -- "$1" "$2"
fi
""",
    )
    _write_fake_command(
        fake_bin,
        "sleep",
        "printf 'sleep|%s\\n' \"$*\" >> \"$HYPERLAB_FAKE_LOG\"\n[[ -z ${HYPERLAB_FAKE_SLEEP_SECONDS:-} ]] || /usr/bin/sleep \"$HYPERLAB_FAKE_SLEEP_SECONDS\"\n",
    )
    _write_fake_command(
        fake_bin,
        "systemctl",
        """printf 'systemctl|%s\n' "$*" >> "$HYPERLAB_FAKE_LOG"
if [[ ${1:-} == is-enabled ]]; then
  printf 'disabled\n'
  exit 1
fi
[[ ${1:-} == show ]] || exit 0
service=${2:-}
if [[ -n ${HYPERLAB_NAMESPACE_PROBE_SHOW_FAILURE:-} && $service == "$HYPERLAB_NAMESPACE_PROBE_SHOW_FAILURE" ]]; then exit 95; fi
if [[ $* == *'--property=ActiveState --value'* ]]; then
  if [[ -f "$HYPERLAB_SERVICE_STATE_DIR/$service" ]]; then printf 'active\n';
  elif [[ -f "$HYPERLAB_SERVICE_STATE_DIR/$service.failed" ]]; then printf 'failed\n';
  else printf 'inactive\n'; fi
  exit 0
fi
if [[ $service == *-namespace-probe.service ]]; then
  invocation=0123456789abcdef0123456789abcdef
  code=${HYPERLAB_NAMESPACE_PROBE_EXEC_MAIN_CODE:-0}
  active=inactive
  sub=dead
  result=success
  pid=0
  restarts=0
  status=0
  fragment=/etc/systemd/system/$service
  case ${HYPERLAB_NAMESPACE_PROBE_PROPERTY_MODE:-green} in
    green) ;;
    active) active=active; sub=exited ;;
    result-failed) result=failed ;;
    restart) restarts=1 ;;
    status4) status=4 ;;
    fragment) fragment=/etc/systemd/system/foreign.service ;;
    *) exit 94 ;;
  esac
  if [[ -f "$HYPERLAB_SERVICE_STATE_DIR/$service.probe-failed" ]]; then
    active=failed; sub=failed; result=exit-code; code=1; status=4
  fi
  if [[ -f "$HYPERLAB_SERVICE_STATE_DIR/$service.probe-success" ]]; then
    printf 'LoadState=loaded\nActiveState=%s\nSubState=%s\nResult=%s\nMainPID=%s\nNRestarts=%s\nExecMainCode=%s\nExecMainStatus=%s\nFragmentPath=%s\n' "$active" "$sub" "$result" "$pid" "$restarts" "$code" "$status" "$fragment"
  elif [[ -f "$HYPERLAB_SERVICE_STATE_DIR/$service.probe-failed" ]]; then
    printf 'LoadState=loaded\nActiveState=%s\nSubState=%s\nResult=%s\nMainPID=%s\nNRestarts=%s\nExecMainCode=%s\nExecMainStatus=%s\nFragmentPath=%s\n' "$active" "$sub" "$result" "$pid" "$restarts" "$code" "$status" "$fragment"
  else
    printf 'LoadState=loaded\nActiveState=inactive\nSubState=dead\nResult=unset\nMainPID=0\nNRestarts=0\nExecMainCode=\nExecMainStatus=\nFragmentPath=/etc/systemd/system/%s\n' "$service"
  fi
  exit 0
fi
if [[ -f "$HYPERLAB_SERVICE_STATE_DIR/$service.failed" ]]; then
  printf 'LoadState=loaded\nActiveState=failed\nSubState=failed\nResult=exit-code\nMainPID=0\nNRestarts=0\nExecMainCode=1\nExecMainStatus=4\nFragmentPath=/etc/systemd/system/%s\n' "$service"
  exit 0
fi
if [[ -f "$HYPERLAB_SERVICE_STATE_DIR/$service" ]]; then
  printf 'LoadState=loaded\nActiveState=active\nSubState=running\nResult=success\nMainPID=123\nNRestarts=0\nExecMainCode=0\nExecMainStatus=0\nFragmentPath=/etc/systemd/system/%s\n' "$service"
else
  printf 'LoadState=loaded\nActiveState=inactive\nSubState=dead\nResult=success\nMainPID=0\nNRestarts=0\nExecMainCode=1\nExecMainStatus=0\nFragmentPath=/etc/systemd/system/%s\n' "$service"
fi
""",
    )
    _write_fake_command(
        fake_bin,
        "sudo",
        """printf 'sudo|%s\n' "$*" >> "$HYPERLAB_FAKE_LOG"
[[ ${1:-} == -n ]] && shift
if [[ ${1:-} == timeout ]]; then
  shift
  while (($#)) && [[ ${1:-} != systemctl && ${1:-} != journalctl ]]; do shift; done
fi
if [[ ${1:-} == install ]]; then
  source=${@: -2:1}; target=${@: -1}
  cp -- "$source" "$HYPERLAB_ROOT_UNIT_STATE/$(basename -- "$target")"
  exit 0
fi
if [[ ${1:-} == sha256sum ]]; then
  target=${@: -1}; leaf=$(basename -- "$target")
  hash=$(sha256sum -- "$HYPERLAB_ROOT_UNIT_STATE/$leaf" | awk '{print $1}')
  printf '%s  %s\n' "$hash" "$target"
  exit 0
fi
if [[ ${1:-} == ln ]]; then
  source=${2}; target=${3}
  cp -- "$HYPERLAB_ROOT_UNIT_STATE/$(basename -- "$source")" "$HYPERLAB_ROOT_UNIT_STATE/$(basename -- "$target")"
  exit 0
fi
if [[ ${1:-} == rm ]]; then
  shift
  [[ ${1:-} == -- ]] && shift
  for target in "$@"; do rm -f -- "$HYPERLAB_ROOT_UNIT_STATE/$(basename -- "$target")"; done
  exit 0
fi
if [[ ${1:-} == journalctl ]]; then
  [[ $* == *'--no-pager -n 20 -o cat'* ]] || exit 92
  journal_service=''
  previous=''
  for argument in "$@"; do
    if [[ $previous == --unit ]]; then journal_service=$argument; break; fi
    previous=$argument
  done
  if [[ $journal_service == *-namespace-probe.service ]]; then
    payload_mode=${HYPERLAB_NAMESPACE_PROBE_PAYLOAD_MODE:-green}
    if [[ -f "$HYPERLAB_SERVICE_STATE_DIR/$journal_service.probe-failed" ]]; then
      payload_mode=refused
    fi
    case $payload_mode in
      absent) : ;;
      malformed) printf '%s\n' 'SYNTHETIC/FIXTURE malformed namespace payload' ;;
      refused)
        if [[ $journal_service == *-polymarket-* ]]; then
          printf '%s\n' "$HYPERLAB_NAMESPACE_PROBE_POLYMARKET_REFUSED_JSON"
        else
          printf '%s\n' "$HYPERLAB_NAMESPACE_PROBE_KALSHI_REFUSED_JSON"
        fi
        ;;
      green)
        if [[ $journal_service == *-polymarket-* ]]; then
          printf '%s\n' "$HYPERLAB_NAMESPACE_PROBE_POLYMARKET_JSON"
        else
          printf '%s\n' "$HYPERLAB_NAMESPACE_PROBE_KALSHI_JSON"
        fi
        ;;
      *) exit 91 ;;
    esac
  else
    printf '%s\n' 'PREDICTION_RUNNER_REFUSED:runner startup admission refused before slot selection: SYNTHETIC/FIXTURE terminal exit 4'
  fi
  exit 0
fi
if [[ ${1:-} != systemctl ]]; then exit 0; fi
shift
action=${1:-}; shift || true
if [[ $action == start ]]; then
  service=${1:-}
  if [[ -n ${HYPERLAB_FAIL_NAMESPACE_PROBE:-} && $service == "$HYPERLAB_FAIL_NAMESPACE_PROBE" ]]; then
    : > "$HYPERLAB_SERVICE_STATE_DIR/$service.probe-failed"
    exit 1
  fi
  : > "$HYPERLAB_SERVICE_STATE_DIR/$service.probe-success"
elif [[ $action == enable && ${1:-} == --now ]]; then
  service=$2
  if [[ -n ${HYPERLAB_FAIL_SERVICE:-} && $service == "$HYPERLAB_FAIL_SERVICE" ]]; then exit 1; fi
  if [[ -n ${HYPERLAB_CLEAN_EXIT_SERVICE:-} && $service == "$HYPERLAB_CLEAN_EXIT_SERVICE" ]]; then exit 0; fi
  if [[ -n ${HYPERLAB_TERMINAL_EXIT_SERVICE:-} && $service == "$HYPERLAB_TERMINAL_EXIT_SERVICE" ]]; then
    : > "$HYPERLAB_SERVICE_STATE_DIR/$service.failed"
    exit 0
  fi
  : > "$HYPERLAB_SERVICE_STATE_DIR/$service"
  if [[ $service == "$HYPERLAB_DASHBOARD_SERVICE" ]]; then
    : > "$HYPERLAB_DASHBOARD_ENABLE_MARKER"
  fi
elif [[ $action == stop ]]; then
  cleanup_failed=0
  for service in "$@"; do
    if [[ -n ${HYPERLAB_CLEANUP_FAILURE_SERVICE:-} && $service == "$HYPERLAB_CLEANUP_FAILURE_SERVICE" ]]; then
      cleanup_failed=1
    else
      rm -f -- "$HYPERLAB_SERVICE_STATE_DIR/$service" "$HYPERLAB_SERVICE_STATE_DIR/$service.failed" "$HYPERLAB_SERVICE_STATE_DIR/$service.probe-success" "$HYPERLAB_SERVICE_STATE_DIR/$service.probe-failed"
    fi
  done
  exit "$cleanup_failed"
elif [[ $action == disable ]]; then
  service=${1:-}
  if [[ -n ${HYPERLAB_CLEANUP_FAILURE_SERVICE:-} && $service == "$HYPERLAB_CLEANUP_FAILURE_SERVICE" ]]; then exit 1; fi
fi
""",
    )
    monitor_helper = tmp_path / "monitor-fixture.py"
    monitor_helper.write_text(
        """import json,os,sys
handoff=json.load(open(sys.argv[2],encoding='utf-8'))
mode=sys.argv[3] if len(sys.argv)>3 else 'full'
dashboard_only=mode in {'dashboard-only','recovery-dashboard'}
recovery_dashboard=mode=='recovery-dashboard'
forced=os.environ.get('HYPERLAB_MONITOR_FAILURE','')
eligible_list=json.loads(os.environ['HYPERLAB_MONITOR_ELIGIBLE_JSON'])
eligible=set(eligible_list)
services={}
failure=False
data_quality_alert=False
for name in ('polymarket','kalshi','dashboard'):
 service=handoff['services'][name]
 active=os.path.isfile(os.path.join(os.environ['HYPERLAB_SERVICE_STATE_DIR'],service))
 failed=os.path.isfile(os.path.join(os.environ['HYPERLAB_SERVICE_STATE_DIR'],service+'.failed'))
 required=name=='dashboard' or (not dashboard_only and name in eligible)
 properties={'ActiveState':'failed' if failed else ('active' if active else 'inactive'),'ExecMainCode':'1' if failed or not active else '0','ExecMainStatus':'4' if failed else '0','FragmentPath':'/etc/systemd/system/'+service,'LoadState':'loaded','MainPID':'123' if active else '0','NRestarts':'0','Result':'exit-code' if failed else 'success','SubState':'failed' if failed else ('running' if active else 'dead')}
 state=None if name=='dashboard' or not active else {'lifecycle':'WAITING_NEXT_SLOT'}
 status='RUNNING' if active else 'SERVICE_UNAVAILABLE'
 if forced=='pid-diverged' and name=='dashboard' and dashboard_only:
  properties['MainPID']='0'
 if forced=='command-diverged' and name=='dashboard' and dashboard_only:
  command_verified=False
 else:
  command_verified=active
 fragment_verified=True
 listener_verified=True if name=='dashboard' and active else None
 if forced=='dashboard-fragment' and name=='dashboard':
  properties['FragmentPath']='/etc/systemd/system/foreign.service'
  fragment_verified=False
 if forced=='dashboard-listener' and name=='dashboard' and active:
  listener_verified=False
 if forced=='collector-fragment' and name=='polymarket' and active and not dashboard_only:
  properties['FragmentPath']='/etc/systemd/system/foreign.service'
  fragment_verified=False
 if forced=='prepared-stale' and name in eligible and active and not dashboard_only:
  state={'lifecycle':'PREPARED'}
  status='PREPARED_STALE'
  failure=True
 if forced=='public-source-invalid-alert' and name=='polymarket' and active and not dashboard_only:
  state={'data_quality':{'alert':True,'economic_eligible':False,'error':'ValueError:SYNTHETIC/FIXTURE public source invalid','source_usable':False,'terminal_health':'PUBLIC_SOURCE_INVALID'},'last_terminal':{'terminal_health':'PUBLIC_SOURCE_INVALID'},'lifecycle':'WAITING_NEXT_SLOT'}
  status='PUBLIC_SOURCE_INVALID'
  data_quality_alert=True
 if required and (not active or properties['MainPID']=='0' or not command_verified or not fragment_verified or (name=='dashboard' and listener_verified is not True)): failure=True
 if not required and active and not recovery_dashboard: failure=True
 services[name]={'admission_required':required,'command_verified':command_verified,'data_quality_alert':status=='PUBLIC_SOURCE_INVALID','fragment_verified':fragment_verified,'listener_verified':listener_verified,'properties':properties,'state':state,'venue_status':status}
if forced=='final-operational-failure' and not dashboard_only and all(os.path.isfile(os.path.join(os.environ['HYPERLAB_SERVICE_STATE_DIR'],handoff['services'][venue])) for venue in ('polymarket','kalshi')):
 failure=True
failure_class=None
preflight_error=None
if forced in {'helper-import','initial-evidence'}:
 failure=True
 failure_class='MONITOR_RUNTIME_IMPORT_FAILED' if forced=='helper-import' else 'INITIAL_EVIDENCE_INVALID'
 preflight_error=('ModuleNotFoundError:SYNTHETIC/FIXTURE helper unavailable' if forced=='helper-import' else 'ValueError:SYNTHETIC/FIXTURE activation proof diverged')
value={'activation_admissible':not failure,'alert':failure or data_quality_alert,'eligible_venues':eligible_list,'failure_class':failure_class,'operational_failure':failure,'preflight_error':preflight_error,'services':services}
print(json.dumps(value,separators=(',',':'),sort_keys=True))
""",
        encoding="utf-8",
    )
    _write_fake_command(
        fake_bin,
        "bash",
        """printf 'bash|%s\n' "$*" >> "$HYPERLAB_FAKE_LOG"
[[ ${1:-} == */monitor.sh ]] || exit 93
exec "$HYPERLAB_REAL_PYTHON" "$HYPERLAB_MONITOR_HELPER" "$@"
""",
    )
    bash_environment = tmp_path / "internal-strict-fake-path.sh"
    bash_environment.write_text(
        f"export PATH='{_git_bash_path(fake_bin)}':\"$PATH\"\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    green_payloads = {
        venue: _namespace_probe_payload_fixture(handoff, venue)
        for venue in ("polymarket", "kalshi")
    }
    refused_payloads = {
        venue: _namespace_probe_payload_fixture(handoff, venue, admitted=False)
        for venue in ("polymarket", "kalshi")
    }
    environment.update(
        {
            "BASH_ENV": _git_bash_path(bash_environment),
            "HOME": "/home/hyperlab",
            "HYPERLAB_FAKE_LOG": _git_bash_path(log),
            "HYPERLAB_FAIL_SERVICE": failed_service or "",
            "HYPERLAB_TERMINAL_EXIT_SERVICE": terminal_exit_service or "",
            "HYPERLAB_CLEAN_EXIT_SERVICE": clean_exit_service or "",
            "HYPERLAB_FAIL_NAMESPACE_PROBE": namespace_probe_failure or "",
            "HYPERLAB_NAMESPACE_PROBE_SHOW_FAILURE": (
                namespace_probe_show_failure or ""
            ),
            "HYPERLAB_NAMESPACE_PROBE_EXEC_MAIN_CODE": (
                namespace_probe_exec_main_code
            ),
            "HYPERLAB_NAMESPACE_PROBE_PROPERTY_MODE": (
                namespace_probe_property_mode
            ),
            "HYPERLAB_NAMESPACE_PROBE_PAYLOAD_MODE": (
                namespace_probe_payload_mode or "green"
            ),
            "HYPERLAB_NAMESPACE_PROBE_POLYMARKET_JSON": json.dumps(
                green_payloads["polymarket"], separators=(",", ":"), sort_keys=True
            ),
            "HYPERLAB_NAMESPACE_PROBE_KALSHI_JSON": json.dumps(
                green_payloads["kalshi"], separators=(",", ":"), sort_keys=True
            ),
            "HYPERLAB_NAMESPACE_PROBE_POLYMARKET_REFUSED_JSON": json.dumps(
                refused_payloads["polymarket"],
                separators=(",", ":"),
                sort_keys=True,
            ),
            "HYPERLAB_NAMESPACE_PROBE_KALSHI_REFUSED_JSON": json.dumps(
                refused_payloads["kalshi"], separators=(",", ":"), sort_keys=True
            ),
            "HYPERLAB_CLEANUP_FAILURE_SERVICE": cleanup_failure_service or "",
            "HYPERLAB_FAIL_DIAGNOSTIC_PYTHON": (
                "1" if fail_diagnostic_encoding else "0"
            ),
            "HYPERLAB_DASHBOARD_ENABLE_MARKER": _git_bash_path(
                tmp_path / "dashboard-enabled"
            ),
            "HYPERLAB_FIRST_HEALTH_MARKER": _git_bash_path(
                tmp_path / "first-health-attempt"
            ),
            "HYPERLAB_DASHBOARD_SERVICE": str(
                dict(handoff["services"])["dashboard"]
            ),
            "HYPERLAB_MONITOR_FAILURE": monitor_failure or "",
            "HYPERLAB_MONITOR_ELIGIBLE_JSON": json.dumps(
                list(monitor_eligible_venues)
            ),
            "HYPERLAB_COLLECTOR_GUARD_FAILURE": (
                "1" if collector_guard_failure else "0"
            ),
            "HYPERLAB_MUTATE_INCOMING_ELIGIBILITY": (
                "1" if mutate_incoming_eligibility else "0"
            ),
            "HYPERLAB_FAKE_SLEEP_SECONDS": "0.03",
            "HYPERLAB_FIXTURE_MANIFEST": _git_bash_path(pack / "campaign-manifest.json"),
            "HYPERLAB_FIXTURE_MANIFEST_PIN": _git_bash_path(
                pack / "campaign-manifest.sha256"
            ),
            "HYPERLAB_INCOMING_WINDOWS": str(
                Path(str(handoff["incoming_root"]).replace("/c/", "C:/", 1))
            ),
            "HYPERLAB_INSTALL_PREFLIGHT_HELPER": _git_bash_path(
                install_preflight_helper
            ),
            "HYPERLAB_MONITOR_HELPER": _git_bash_path(monitor_helper),
            "HYPERLAB_REAL_PYTHON": _git_bash_path(Path(sys.executable)),
            "HYPERLAB_ROOT_UNIT_STATE": _git_bash_path(state_root / "root-units"),
            "HYPERLAB_SERVICE_STATE_DIR": _git_bash_path(state_root),
            "HYPERLAB_SERVICES_JSON": json.dumps(handoff["services"]),
            "HYPERLAB_SOURCE_COMMIT": str(handoff["source_commit"]),
            "PATH": str(fake_bin) + os.pathsep + environment["PATH"],
        }
    )
    return environment, log


def _materialize_windows_transfer_layout(
    tmp_path: Path,
) -> tuple[Path, Path, Path, Path, Path, dict[str, object]]:
    pack = tmp_path / "materialized-pack"
    operator = pack / "operator"
    scripts = pack / "scripts"
    wheelhouse = pack / "wheelhouse"
    operator.mkdir(parents=True)
    scripts.mkdir()
    wheelhouse.mkdir()
    bundle = pack / "hyperlab-prediction-markets-prospective-launch-v1.bundle"
    _create_real_git_bundle(tmp_path, bundle)
    wheel = wheelhouse / "fixture-1.0-py3-none-any.whl"
    wheel.write_bytes(b"real-wheel-hash-fixture")
    (pack / "wheelhouse.sha256").write_bytes(
        f"{launch_pack.sha256_file(wheel)}  {wheel.name}\n".encode("ascii")
    )
    handoff = _handoff()
    handoff.update(
        {
            "bundle_filename": bundle.name,
            "bundle_sha256": launch_pack.sha256_file(bundle),
            "incoming_root": _git_bash_path(pack),
            "source_commit": "b" * 40,
            "superseded_campaign": launch_pack._superseded_campaign_contract(),
        }
    )
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        check=True,
        text=True,
        timeout=30,
    ).stdout.strip()
    for name in launch_pack._SCRIPTS:
        target = scripts / name
        if name == "launch_pack.py":
            target.write_bytes(Path(launch_pack.__file__).read_bytes())
        elif name.endswith(".sh"):
            target.write_bytes(
                launch_pack._git_blob(
                    ROOT,
                    commit,
                    f"ops/prediction_markets_launch_v1/{name}",
                )
            )
        else:
            target.write_text("# SYNTHETIC/FIXTURE transferred Python payload\n", encoding="utf-8")
    (pack / "README.md").write_text("SYNTHETIC/FIXTURE transfer layout\n", encoding="utf-8")
    (pack / "source-inventory.json").write_text(
        '{"fixture":"SYNTHETIC/FIXTURE"}\n',
        encoding="utf-8",
    )
    blocks = {
        "A-windows-bundle-verify-transfer.ps1": launch_pack.render_windows_transfer(handoff),
        "B-tabby-preflight-install-activate.sh": launch_pack.render_tabby_install(handoff),
        "C-tabby-readonly-monitor.sh": launch_pack.render_tabby_monitor(handoff),
        "D-windows-dashboard-tunnel.ps1": launch_pack.render_windows_tunnel(handoff),
        "E-recovery-rollback.sh": launch_pack.render_recovery_rollback(handoff),
    }
    for name, content in blocks.items():
        (operator / name).write_text(content, encoding="utf-8", newline="\n")
    transfer_paths = [
        bundle.name,
        "README.md",
        "source-inventory.json",
        "wheelhouse.sha256",
        *[f"scripts/{name}" for name in launch_pack._SCRIPTS],
        *[f"operator/{name}" for name in blocks],
    ]
    transfer = launch_pack._transfer_inventory(pack, transfer_paths)
    transfer_raw = launch_pack.canonical_json_bytes(transfer) + b"\n"
    (pack / "transfer-inventory.json").write_bytes(transfer_raw)
    handoff["transfer_inventory_sha256"] = launch_pack.sha256_bytes(transfer_raw)
    handoff_json = pack / "handoff.json"
    handoff_json.write_bytes(launch_pack.canonical_json_bytes(handoff) + b"\n")
    (pack / "handoff.sha256").write_bytes(
        f"{launch_pack.sha256_file(handoff_json)}  handoff.json\n".encode("ascii")
    )
    block_a = operator / "A-windows-bundle-verify-transfer.ps1"
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
    powershell = _powershell_51()
    if os.name == "nt":
        program_files = Path(os.environ.get("PROGRAMFILES", r"C:\Program Files"))
        candidate = program_files / "Git" / "bin" / "bash.exe"
        git_bash = str(candidate) if candidate.is_file() else None
    else:
        git_bash = shutil.which("bash")
    if git_bash is None:
        pytest.skip("Git Bash is unavailable for the remote-command simulation")
    remote_fake_bin = tmp_path / "remote-fake-bin"
    remote_fake_bin.mkdir()
    _write_fake_command(
        remote_fake_bin,
        "python3.12",
        'exec "$HYPERLAB_PM_REAL_PYTHON" "$@"\n',
    )
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
            "HYPERLAB_PM_REAL_PYTHON": str(Path(sys.executable)),
            "PATH": str(remote_fake_bin) + os.pathsep + environment["PATH"],
            "TEMP": str(powershell_temp),
            "TMP": str(powershell_temp),
        }
    )
    completed = subprocess.run(
        [
            str(powershell),
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
    if arguments[0] == "bash" and "verify-old" in arguments:
        return preflight.CommandResult(
            0,
            "PREDICTION_OLD_RAW_RECEIPTS_LEDGER_AUTHENTICATED\n"
            "PREDICTION_OLD_CAMPAIGN_FIVE_UNITS_AUTHENTICATED\n"
            "PREDICTION_OLD_CAMPAIGN_PREMUTATION_AUTHENTICATED\n",
            "",
        )
    if arguments[0] == "python3.12" or arguments[0].replace("\\", "/").endswith(
        "/.venv/bin/python"
    ):
        if "verify-source" in arguments:
            expected_commit = arguments[arguments.index("--expected-commit") + 1]
            return preflight.CommandResult(
                0,
                json.dumps(
                    {
                        "commit": expected_commit,
                        "files": 1,
                        "inventory_sha256": "c" * 64,
                        "status": "PREDICTION_SOURCE_IDENTITY_GREEN",
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "",
            )
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
            "/mnt/HC_Volume_106716684 /dev/sdb ext4 rw,relatime,discard 8:16 /",
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


def _synthetic_runtime_import_result(**_kwargs: object) -> dict[str, object]:
    return {
        "admission_sha256": "d" * 64,
        "terminal_signal": "PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN",
    }


def test_plan_freezes_candidate_identities_and_conservative_h1_reservation() -> None:
    plan = launch_pack.validate_plan(_plan())
    assert plan["access_bundle_sha256"] == (
        "965a42f2169c16201323477c0eb1ba7a8b540b24109c1d9252d5d9fcce55bbe5"
    )
    assert plan["base_commit"] == "bcb5280f87393992e2aa4528188009186cd8bdc3"
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


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"\xef\xbb\xbf#!/usr/bin/env bash\ntrue\n", "UTF-8 BOM"),
        (b"#!/usr/bin/env bash\ntrue\x00\n", "contains NUL"),
        (b"#!/usr/bin/env bash\r\ntrue\r\n", "contains CR"),
        (b"#!/bin/sh\ntrue\n", "invalid Bash shebang"),
        (b"#!/usr/bin/env bash\ntrue", "final LF"),
    ],
)
def test_posix_shell_payload_validation_is_binary_and_fail_closed(
    payload: bytes,
    message: str,
) -> None:
    with pytest.raises(launch_pack.LaunchPackError, match=message):
        launch_pack.validate_posix_shell_payload(payload, label="synthetic.sh")


def test_commit_blob_materialization_ignores_forced_crlf_worktree_and_is_byte_stable(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "synthetic-crlf-checkout"
    _run_git("init", "--quiet", str(repo))
    _run_git("config", "user.email", "synthetic-fixture@invalid.example", cwd=repo)
    _run_git("config", "user.name", "Synthetic Fixture", cwd=repo)
    relative = "ops/prediction_markets_launch_v1/bootstrap-offline.sh"
    tracked = repo.joinpath(*PurePosixPath(relative).parts)
    tracked.parent.mkdir(parents=True)
    committed = b"#!/usr/bin/env bash\nset -Eeuo pipefail\nprintf 'BLOB_LF_GREEN\\n'\n"
    tracked.write_bytes(committed)
    _run_git("add", relative, cwd=repo)
    _run_git("commit", "--quiet", "-m", "synthetic LF blob", cwd=repo)
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        capture_output=True,
        check=True,
        text=True,
        timeout=30,
    ).stdout.strip()
    tracked.write_bytes(committed.replace(b"\n", b"\r\n"))
    assert b"\r\n" in tracked.read_bytes()
    first = tmp_path / "pack-one" / "scripts" / "bootstrap-offline.sh"
    second = tmp_path / "pack-two" / "scripts" / "bootstrap-offline.sh"
    for target in (first, second):
        launch_pack._materialize_commit_file(
            repo_root=repo,
            commit=commit,
            relative_path=relative,
            output_path=target,
        )
    assert first.read_bytes() == committed
    assert second.read_bytes() == committed
    assert first.read_bytes() == second.read_bytes()
    assert b"\r" not in first.read_bytes()


def test_materialized_bootstrap_executes_real_git_bash_beyond_set_builtin(
    tmp_path: Path,
) -> None:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        check=True,
        text=True,
        timeout=30,
    ).stdout.strip()
    script = tmp_path / "materialized-bootstrap-offline.sh"
    launch_pack._materialize_commit_file(
        repo_root=ROOT,
        commit=commit,
        relative_path="ops/prediction_markets_launch_v1/bootstrap-offline.sh",
        output_path=script,
    )
    completed = _run_git_bash_script(
        script,
        cwd=tmp_path,
        environment=os.environ.copy(),
        arguments=(
            _git_bash_path(tmp_path / "source"),
            _git_bash_path(tmp_path / "wheelhouse"),
        ),
    )
    assert completed.returncode == 4
    assert "PREDICTION_OFFLINE_BOOTSTRAP_REFUSED:run as hyperlab" in completed.stderr
    assert "invalid option name" not in completed.stderr


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


def test_materialized_pack_is_new_slug_bound_authenticated_and_probes_precede_persistent_services(
    tmp_path: Path,
) -> None:
    old_slug = "pm-20260827t234404z-73c6d2d2"
    new_slug = "pm-20260828t010203z-cafefeed"
    pack = tmp_path / new_slug
    pack.mkdir()
    (pack / "wheelhouse").mkdir()
    bundle = pack / "hyperlab-prediction-markets-prospective-launch-v1.bundle"
    _create_real_git_bundle(tmp_path, bundle)
    wheel = pack / "wheelhouse" / "fixture-1.0-py3-none-any.whl"
    wheel.write_bytes(b"SYNTHETIC/FIXTURE offline wheel")
    wheelhouse_payload = (
        f"{launch_pack.sha256_file(wheel)}  {wheel.name}\n".encode("ascii")
    )
    (pack / "wheelhouse.sha256").write_bytes(wheelhouse_payload)
    synthetic_source = tmp_path / "synthetic-bundle-source"
    synthetic_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        check=True,
        cwd=synthetic_source,
        text=True,
        timeout=30,
    ).stdout.strip()
    source_inventory = launch_pack.build_source_inventory(
        synthetic_source, synthetic_commit
    )
    (pack / "source-inventory.json").write_bytes(
        launch_pack.canonical_json_bytes(source_inventory) + b"\n"
    )
    handoff = json.loads(json.dumps(_handoff()).replace("pm-20260827t120000z-deadbeef", new_slug))
    plan = _plan()
    handoff.update(
        {
            "access_bundle_sha256": plan["access_bundle_sha256"],
            "base_commit": plan["base_commit"],
            "bundle_filename": bundle.name,
            "bundle_sha256": launch_pack.sha256_file(bundle),
            "candidate_config_sha256": plan["candidate_config_sha256"],
            "candidate_pack_manifest_sha256": plan["campaign_manifest_sha256"],
            "disk": plan["disk"],
            "economic_evidence_status": "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE",
            "pack_id": launch_pack.PACK_ID,
            "quick_start_default": "IMMEDIATE_AFTER_SUCCESSFUL_INSTALL",
            "schema_version": 1,
            "source_commit": synthetic_commit,
            "source_inventory_sha256": source_inventory["inventory_sha256"],
            "start_at_override": "HYPERLAB_PM_START_AT_UTC_OPTIONAL",
            "superseded_campaign": launch_pack._superseded_campaign_contract(),
            "wheelhouse_manifest_sha256": launch_pack.sha256_bytes(wheelhouse_payload),
        }
    )
    scripts_root = pack / "scripts"
    scripts_root.mkdir()
    for name in launch_pack._SCRIPTS:
        payload = launch_pack._git_blob(
            synthetic_source,
            synthetic_commit,
            f"ops/prediction_markets_launch_v1/{name}",
        )
        (scripts_root / name).write_bytes(payload)
    (pack / "README.md").write_text(
        launch_pack.render_operator_readme(handoff), encoding="utf-8"
    )
    units = launch_pack.render_units(handoff)
    for name, content in units.items():
        path = pack / "systemd" / name
        path.parent.mkdir(exist_ok=True)
        path.write_text(content, encoding="utf-8", newline="\n")
    blocks = {
        "A-windows-bundle-verify-transfer.ps1": launch_pack.render_windows_transfer(handoff),
        "B-tabby-preflight-install-activate.sh": launch_pack.render_tabby_install(handoff),
        "C-tabby-readonly-monitor.sh": launch_pack.render_tabby_monitor(handoff),
        "D-windows-dashboard-tunnel.ps1": launch_pack.render_windows_tunnel(handoff),
        "E-recovery-rollback.sh": launch_pack.render_recovery_rollback(handoff),
    }
    for name, content in blocks.items():
        path = pack / "operator" / name
        if name.endswith(".sh"):
            launch_pack._write_shell_new(path, content.encode("utf-8"))
        else:
            path.parent.mkdir(exist_ok=True)
            path.write_text(content, encoding="utf-8")
    transfer_paths = [
        bundle.name,
        "README.md",
        "source-inventory.json",
        "wheelhouse.sha256",
        *[f"scripts/{name}" for name in launch_pack._SCRIPTS],
        *[f"systemd/{name}" for name in units],
        *[f"operator/{name}" for name in blocks],
    ]
    transfer = launch_pack._transfer_inventory(pack, transfer_paths)
    transfer_payload = launch_pack.canonical_json_bytes(transfer) + b"\n"
    (pack / "transfer-inventory.json").write_bytes(transfer_payload)
    handoff["transfer_inventory_sha256"] = launch_pack.sha256_bytes(transfer_payload)
    handoff_payload = launch_pack.canonical_json_bytes(handoff) + b"\n"
    (pack / "handoff.json").write_bytes(handoff_payload)
    (pack / "handoff.sha256").write_text(
        f"{launch_pack.sha256_bytes(handoff_payload)}  handoff.json\n",
        encoding="ascii",
    )

    assert pack.name == new_slug and old_slug not in pack.as_posix()
    assert len(units) == 5 and len(blocks) == 5
    verify_repo = tmp_path / "materialized-pack-bundle-verifier.git"
    _run_git("init", "--quiet", "--bare", str(verify_repo))
    _run_git("bundle", "verify", str(bundle), cwd=verify_repo)
    for row in transfer["files"]:  # type: ignore[index]
        assert isinstance(row, dict)
        target = pack.joinpath(*PurePosixPath(str(row["path"])).parts)
        assert target.stat().st_size == row["size"]
        assert launch_pack.sha256_file(target) == row["sha256"]
    assert launch_pack.sha256_bytes((pack / "handoff.json").read_bytes()) in (
        pack / "handoff.sha256"
    ).read_text(encoding="ascii")
    transfer_proof = launch_pack.verify_materialized_transfer(
        pack.resolve(strict=True),
        (pack / "handoff.json").resolve(strict=True),
    )
    assert transfer_proof["terminal_signal"] == "PREDICTION_TRANSFER_SHELLS_LF_AUTHENTICATED"
    assert transfer_proof["shell_files"] == 8
    text_payload = "\n".join(
        path.read_text(encoding="utf-8")
        for path in pack.rglob("*")
        if path.is_file() and path.suffix not in {".bundle", ".whl"}
    )
    assert old_slug not in text_payload
    assert new_slug in blocks["B-tabby-preflight-install-activate.sh"]
    install_script = (scripts_root / "install.sh").read_text(encoding="utf-8")
    first_probe = install_script.index("for VENUE in polymarket kalshi")
    dashboard = install_script.index('systemctl enable --now "$DASHBOARD_SERVICE"')
    collector = install_script.index('systemctl enable --now "$SERVICE"')
    assert first_probe < dashboard < collector
    assert "PREDICTION_NAMESPACE_PROBE_DIAGNOSTIC=" in install_script
    assert "authenticate-namespace-probe-completion" in install_script
    assert "HYPERLAB_NAMESPACE_PROBE_JOURNAL" in install_script
    assert "properties.get('ExecMainCode')=='1'" not in install_script
    for field in ("TARGET", "SOURCE", "FSTYPE", "VFS_OPTIONS", "MAJ:MIN", "FSROOT", "LOGICAL_PATH"):
        assert f'"{field}"' in install_script


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
    assert len(units) == 5
    polymarket = units[
        "hyperlab-pm-20260827t120000z-deadbeef-polymarket.service"
    ]
    kalshi = units["hyperlab-pm-20260827t120000z-deadbeef-kalshi.service"]
    dashboard = next(value for key, value in units.items() if "dashboard" in key)
    polymarket_probe = units[
        "hyperlab-pm-20260827t120000z-deadbeef-polymarket-namespace-probe.service"
    ]
    kalshi_probe = units[
        "hyperlab-pm-20260827t120000z-deadbeef-kalshi-namespace-probe.service"
    ]
    assert "--venue polymarket" in polymarket and "--venue kalshi" not in polymarket
    assert "--venue kalshi" in kalshi and "--venue polymarket" not in kalshi
    assert "ReadWritePaths=" in polymarket and "/polymarket" in polymarket
    assert "ReadWritePaths=" in kalshi and "/kalshi" in kalshi
    assert "ReadWritePaths=" not in dashboard
    assert "--host 127.0.0.1 --port 18081" in dashboard
    for service in (polymarket, kalshi, dashboard):
        assert service.count("ExecStartPre=") == 1
        assert "runtime-import-admission" in service
        assert " --source-inventory " in service
        assert " -I " in service
    for collector in (polymarket, kalshi):
        assert "RestartPreventExitStatus=4" in collector
    for venue, probe in (("polymarket", polymarket_probe), ("kalshi", kalshi_probe)):
        assert "Type=oneshot" in probe
        assert "TimeoutStartSec=30" in probe
        assert "Restart=no" in probe
        assert "runner-namespace-guard" in probe
        assert f"--venue {venue}" in probe
        assert f"ReadWritePaths={_handoff()['campaign_root']}/{venue}" in probe
        collector = polymarket if venue == "polymarket" else kalshi
        filesystem_directives = (
            "PrivateTmp=",
            "PrivateDevices=",
            "ProtectSystem=",
            "ProtectHome=",
            "ReadOnlyPaths=",
            "ReadWritePaths=",
        )
        assert {
            line
            for line in probe.splitlines()
            if line.startswith(filesystem_directives)
        } == {
            line
            for line in collector.splitlines()
            if line.startswith(filesystem_directives)
        }
    assert "RestartPreventExitStatus=4" not in dashboard
    for unit in units.values():
        assert "User=hyperlab" in unit
        assert "Environment=USER=hyperlab" in unit
        assert "NoNewPrivileges=yes" in unit
        assert "ProtectSystem=strict" in unit
        assert "CapabilityBoundingSet=" in unit
        assert "Restart=on-failure" in unit or "Restart=no" in unit
        readonly_paths = unit.split("ReadOnlyPaths=", 1)[1].splitlines()[0].split()
        assert str(_handoff()["volume_mount"]) in readonly_paths
        assert str(_handoff()["volume_base"]) in readonly_paths
        assert str(_handoff()["incoming_root"]) in readonly_paths
        assert str(_handoff()["source_root"]) in readonly_paths
        assert str(_handoff()["campaign_root"]) in readonly_paths
        assert unit.count("ReadWritePaths=") <= 1
        assert "hyperlab-h1" not in unit
        assert "18080" not in unit


def _namespace_probe_properties(
    service: str,
    *,
    active_state: str = "inactive",
    exec_main_code: str = "0",
    exec_main_status: str = "0",
    fragment_path: str | None = None,
    load_state: str = "loaded",
    main_pid: str = "0",
    restarts: str = "0",
    result: str = "success",
    sub_state: str = "dead",
) -> str:
    fragment = fragment_path or f"/etc/systemd/system/{service}"
    return "\n".join(
        (
            f"LoadState={load_state}",
            f"ActiveState={active_state}",
            f"SubState={sub_state}",
            f"Result={result}",
            f"MainPID={main_pid}",
            f"NRestarts={restarts}",
            f"ExecMainCode={exec_main_code}",
            f"ExecMainStatus={exec_main_status}",
            f"FragmentPath={fragment}",
        )
    )


@pytest.mark.parametrize("exec_main_code", ("0", "1"))
def test_namespace_probe_completion_accepts_only_observed_success_code_forms(
    exec_main_code: str,
) -> None:
    handoff = _handoff()
    venue = "polymarket"
    service = (
        "hyperlab-pm-20260827t120000z-deadbeef-"
        "polymarket-namespace-probe.service"
    )
    payload = _namespace_probe_payload_fixture(handoff, venue)
    result = preflight.authenticate_namespace_probe_completion(
        properties_text=_namespace_probe_properties(
            service,
            exec_main_code=exec_main_code,
        ),
        journal_text=json.dumps(payload, separators=(",", ":"), sort_keys=True),
        service=service,
        venue=venue,
        campaign_root=Path(str(handoff["campaign_root"])),
        incoming_root=Path(str(handoff["incoming_root"])),
    )
    assert result["authenticated"] is True
    assert result["exec_main_code"] == exec_main_code
    assert result["terminal_signal"] == "PREDICTION_NAMESPACE_PROBE_COMPLETION_GREEN"


@pytest.mark.parametrize(
    ("property_overrides", "message"),
    (
        ({"active_state": "active", "sub_state": "exited"}, "active-state"),
        ({"active_state": "activating", "sub_state": "start"}, "active-state"),
        ({"active_state": "failed", "sub_state": "failed"}, "active-state"),
        ({"load_state": "not-found"}, "load-state"),
        ({"main_pid": "123"}, "main-pid"),
        ({"result": "failed"}, "result"),
        ({"restarts": "1"}, "restart-count"),
        ({"exec_main_code": "2"}, "exit-code-kind"),
        ({"exec_main_status": "4"}, "exit-status"),
        ({"fragment_path": "/etc/systemd/system/foreign.service"}, "fragment"),
    ),
)
def test_namespace_probe_completion_rejects_systemd_property_divergence(
    property_overrides: dict[str, str],
    message: str,
) -> None:
    handoff = _handoff()
    venue = "polymarket"
    service = (
        "hyperlab-pm-20260827t120000z-deadbeef-"
        "polymarket-namespace-probe.service"
    )
    payload = _namespace_probe_payload_fixture(handoff, venue)
    with pytest.raises(preflight.PreflightError, match=message):
        preflight.authenticate_namespace_probe_completion(
            properties_text=_namespace_probe_properties(
                service,
                **property_overrides,
            ),
            journal_text=json.dumps(payload, separators=(",", ":"), sort_keys=True),
            service=service,
            venue=venue,
            campaign_root=Path(str(handoff["campaign_root"])),
            incoming_root=Path(str(handoff["incoming_root"])),
        )


@pytest.mark.parametrize(
    ("payload_mode", "message"),
    (
        ("absent", "payload is absent"),
        ("malformed", "payload is absent or malformed"),
        ("refused", "payload refused"),
    ),
)
def test_namespace_probe_completion_rejects_missing_malformed_or_refused_payload(
    payload_mode: str,
    message: str,
) -> None:
    handoff = _handoff()
    venue = "polymarket"
    service = (
        "hyperlab-pm-20260827t120000z-deadbeef-"
        "polymarket-namespace-probe.service"
    )
    payload = _namespace_probe_payload_fixture(
        handoff,
        venue,
        admitted=payload_mode != "refused",
    )
    journal = {
        "absent": "",
        "malformed": "SYNTHETIC/FIXTURE not JSON",
        "refused": json.dumps(payload, separators=(",", ":"), sort_keys=True),
    }[payload_mode]
    with pytest.raises(preflight.PreflightError, match=message):
        preflight.authenticate_namespace_probe_completion(
            properties_text=_namespace_probe_properties(service),
            journal_text=journal,
            service=service,
            venue=venue,
            campaign_root=Path(str(handoff["campaign_root"])),
            incoming_root=Path(str(handoff["incoming_root"])),
        )


def test_home_mount_evidence_accepts_authenticated_root_and_records_stat_device(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stat_paths: list[Path] = []

    def stat_device(path: Path) -> str:
        stat_paths.append(path)
        return "8:1"

    monkeypatch.setattr(preflight, "_stat_device_major_minor", stat_device)
    evidence = preflight._mount_evidence(
        Path("/home"),
        run=lambda _arguments: preflight.CommandResult(
            0,
            "/ /dev/sda1 ext4 ro,nosuid,relatime 8:1 /",
            "",
        ),
        label="runner namespace /home root",
        target_may_be_ancestor=True,
        expected_fstype="ext4",
        required_mode="ro",
    )
    assert evidence == {
        "device_major_minor": "8:1",
        "filesystem": "ext4",
        "filesystem_root": "/",
        "logical_path": "/home",
        "mount": "/",
        "options": ["ro", "nosuid", "relatime"],
        "source": "/dev/sda1",
        "stat_device_major_minor": "8:1",
    }
    assert stat_paths == [Path("/home")]


@pytest.mark.parametrize(
    "target",
    (
        "/",
        "/home",
        "/home/hyperlab",
        "/home/hyperlab/hyperlab-prediction-markets",
        "/home/hyperlab/hyperlab-prediction-markets/incoming",
        "/home/hyperlab/hyperlab-prediction-markets/incoming/pm-20260827t120000z-deadbeef",
    ),
)
def test_incoming_namespace_accepts_authenticated_root_filesystem_ancestors(
    target: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming = Path(
        "/home/hyperlab/hyperlab-prediction-markets/incoming/"
        "pm-20260827t120000z-deadbeef"
    )
    canonical_paths: list[Path] = []

    def canonical(path: Path, *, label: str) -> tuple[int, int, int]:
        del label
        canonical_paths.append(path)
        return 8, 1, 0o40700

    monkeypatch.setattr(preflight, "_canonical_directory", canonical)
    monkeypatch.setattr(preflight, "_stat_device_major_minor", lambda _path: "8:1")
    filesystem_root = "/" if target == "/" else target
    source = "/dev/sda1" if target == "/" else f"/dev/sda1[{target}]"
    preflight._authenticate_incoming_namespace_target(
        {
            "device_major_minor": "8:1",
            "filesystem": "ext4",
            "filesystem_root": filesystem_root,
            "logical_path": incoming.as_posix(),
            "mount": target,
            "options": ["ro", "nosuid", "relatime"],
            "source": source,
            "stat_device_major_minor": "8:1",
        },
        home_evidence={
            "device_major_minor": "8:1",
            "filesystem": "ext4",
            "filesystem_root": "/",
            "logical_path": "/home",
            "mount": "/",
            "options": ["ro", "nosuid", "relatime"],
            "source": "/dev/sda1",
            "stat_device_major_minor": "8:1",
        },
        incoming=incoming,
        label="runner namespace incoming root",
    )
    assert canonical_paths[0] == Path("/home")
    assert canonical_paths[-1] == incoming


@pytest.mark.parametrize(
    "target",
    (
        "/home",
        "/home/hyperlab",
        "/home/hyperlab/hyperlab-prediction-markets",
        "/home/hyperlab/hyperlab-prediction-markets/incoming",
        "/home/hyperlab/hyperlab-prediction-markets/incoming/pm-20260827t120000z-deadbeef",
    ),
)
def test_incoming_namespace_accepts_only_authenticated_readonly_home_ancestors(
    target: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming = Path(
        "/home/hyperlab/hyperlab-prediction-markets/incoming/"
        "pm-20260827t120000z-deadbeef"
    )
    monkeypatch.setattr(
        preflight,
        "_canonical_directory",
        lambda _path, *, label: (254, 1, 0o40700),
    )
    monkeypatch.setattr(preflight, "_stat_device_major_minor", lambda _path: "254:0")
    evidence = {
        "device_major_minor": "254:0",
        "filesystem": "ext4",
        "filesystem_root": target,
        "logical_path": incoming.as_posix(),
        "mount": target,
        "options": ["ro", "relatime"],
        "source": f"/dev/root[{target}]",
        "stat_device_major_minor": "254:0",
    }
    home_evidence = {
        "device_major_minor": "254:0",
        "filesystem": "ext4",
        "filesystem_root": "/home",
        "logical_path": "/home",
        "mount": "/home",
        "options": ["ro", "relatime"],
        "source": "/dev/root[/home]",
        "stat_device_major_minor": "254:0",
    }
    preflight._authenticate_incoming_namespace_target(
        evidence,
        home_evidence=home_evidence,
        incoming=incoming,
        label="runner namespace incoming root",
    )


def test_incoming_namespace_accepts_authenticated_ancestor_on_separate_home_mount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming = Path(
        "/home/hyperlab/hyperlab-prediction-markets/incoming/"
        "pm-20260827t120000z-deadbeef"
    )
    target = "/home/hyperlab"
    monkeypatch.setattr(
        preflight,
        "_canonical_directory",
        lambda _path, *, label: (254, 1, 0o40700),
    )
    monkeypatch.setattr(preflight, "_stat_device_major_minor", lambda _path: "254:0")
    preflight._authenticate_incoming_namespace_target(
        {
            "device_major_minor": "254:0",
            "filesystem": "ext4",
            "filesystem_root": "/hyperlab",
            "logical_path": incoming.as_posix(),
            "mount": target,
            "options": ["ro", "relatime"],
            "source": "/dev/home[/hyperlab]",
            "stat_device_major_minor": "254:0",
        },
        home_evidence={
            "device_major_minor": "254:0",
            "filesystem": "ext4",
            "filesystem_root": "/",
            "logical_path": "/home",
            "mount": "/home",
            "options": ["ro", "relatime"],
            "source": "/dev/home[/]",
            "stat_device_major_minor": "254:0",
        },
        incoming=incoming,
        label="runner namespace incoming root",
    )


@pytest.mark.parametrize(
    ("home_mutation", "incoming_mutation", "message"),
    (
        ({"logical_path": "/srv"}, {}, "logical path"),
        ({"filesystem_root": "/wrong"}, {}, "root"),
        ({"source": "/dev/sda2"}, {}, "source"),
        ({"filesystem": "xfs"}, {}, "filesystem type"),
        ({"options": ["rw", "relatime"]}, {}, "mounted ro"),
        ({"stat_device_major_minor": "8:2"}, {}, "device"),
        ({}, {"filesystem_root": "/home"}, "root/bind"),
        ({}, {"source": "/dev/sdb"}, "source"),
        ({}, {"filesystem": "xfs"}, "filesystem type"),
        ({}, {"device_major_minor": "8:16"}, "device"),
        ({}, {"options": ["rw", "relatime"]}, "mounted ro"),
        ({}, {"logical_path": "/home/other"}, "logical path"),
    ),
)
def test_root_backed_incoming_namespace_rejects_identity_or_mode_divergence(
    home_mutation: dict[str, object],
    incoming_mutation: dict[str, object],
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming = Path(
        "/home/hyperlab/hyperlab-prediction-markets/incoming/"
        "pm-20260827t120000z-deadbeef"
    )
    monkeypatch.setattr(
        preflight,
        "_canonical_directory",
        lambda _path, *, label: (8, 1, 0o40700),
    )
    monkeypatch.setattr(preflight, "_stat_device_major_minor", lambda _path: "8:1")
    home_evidence: dict[str, object] = {
        "device_major_minor": "8:1",
        "filesystem": "ext4",
        "filesystem_root": "/",
        "logical_path": "/home",
        "mount": "/",
        "options": ["ro", "nosuid", "relatime"],
        "source": "/dev/sda1",
        "stat_device_major_minor": "8:1",
    }
    evidence: dict[str, object] = {
        "device_major_minor": "8:1",
        "filesystem": "ext4",
        "filesystem_root": "/",
        "logical_path": incoming.as_posix(),
        "mount": "/",
        "options": ["ro", "nosuid", "relatime"],
        "source": "/dev/sda1",
        "stat_device_major_minor": "8:1",
    }
    home_evidence.update(home_mutation)
    evidence.update(incoming_mutation)
    with pytest.raises(preflight.PreflightError, match=message):
        preflight._authenticate_incoming_namespace_target(
            evidence,
            home_evidence=home_evidence,
            incoming=incoming,
            label="runner namespace incoming root",
        )


def test_root_mount_target_remains_refused_for_volume_mapping() -> None:
    with pytest.raises(preflight.PreflightError, match="not allowlisted"):
        preflight._authenticate_volume_namespace_mapping(
            {
                "filesystem_root": "/",
                "mount": "/",
            },
            admitted_root="/",
            allowed_targets=(Path("/mnt/HC_Volume_106716684"),),
            label="runner namespace volume parent",
            volume_mount=Path("/mnt/HC_Volume_106716684"),
        )


def test_incoming_root_target_requires_home_to_share_the_root_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming = Path(
        "/home/hyperlab/hyperlab-prediction-markets/incoming/"
        "pm-20260827t120000z-deadbeef"
    )
    monkeypatch.setattr(
        preflight,
        "_canonical_directory",
        lambda _path, *, label: (8, 1, 0o40700),
    )
    monkeypatch.setattr(preflight, "_stat_device_major_minor", lambda _path: "8:1")
    with pytest.raises(preflight.PreflightError, match="home mount target"):
        preflight._authenticate_incoming_namespace_target(
            {
                "device_major_minor": "8:1",
                "filesystem": "ext4",
                "filesystem_root": "/",
                "logical_path": incoming.as_posix(),
                "mount": "/",
                "options": ["ro", "relatime"],
                "source": "/dev/sda1",
                "stat_device_major_minor": "8:1",
            },
            home_evidence={
                "device_major_minor": "8:1",
                "filesystem": "ext4",
                "filesystem_root": "/home",
                "logical_path": "/home",
                "mount": "/home",
                "options": ["ro", "relatime"],
                "source": "/dev/sda1[/home]",
                "stat_device_major_minor": "8:1",
            },
            incoming=incoming,
            label="runner namespace incoming root",
        )


@pytest.mark.parametrize("venue", ("polymarket", "kalshi"))
def test_runner_namespace_admits_root_backed_home_for_both_materialized_probes(
    venue: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slug = "pm-20260828t020000z-deadbeef"
    volume_mount = Path("/mnt/HC_Volume_106716684")
    volume_base = volume_mount / "hyperlab-prediction-markets"
    incoming = Path(
        "/home/hyperlab/hyperlab-prediction-markets/incoming/"
        f"{slug}"
    )
    source = volume_base / "sources" / slug
    campaign = volume_base / "campaigns" / slug
    layout = {
        "campaign_root": campaign.as_posix(),
        "incoming_root": incoming.as_posix(),
        "source_root": source.as_posix(),
        "volume_base": volume_base.as_posix(),
        "volume_mount": volume_mount.as_posix(),
    }
    handoff = {"service_user": "hyperlab"}
    admitted = {
        "device_major_minor": "8:16",
        "filesystem_root": "/",
    }
    monkeypatch.setenv("USER", "hyperlab")
    monkeypatch.setattr(
        preflight.Path,
        "home",
        classmethod(lambda _cls: Path("/home/hyperlab")),
    )
    monkeypatch.setattr(
        preflight,
        "_validated_install_admission",
        lambda *_args, **_kwargs: (handoff, layout, admitted),
    )
    monkeypatch.setattr(
        preflight,
        "_canonical_directory",
        lambda _path, *, label: (1, 2, 0o40700),
    )

    def stat_device(path: Path) -> str:
        logical = PurePosixPath(path.as_posix())
        home = PurePosixPath("/home")
        return "8:1" if logical == home or home in logical.parents else "8:16"

    monkeypatch.setattr(preflight, "_stat_device_major_minor", stat_device)

    def findmnt(arguments: list[str] | tuple[str, ...]) -> preflight.CommandResult:
        assert arguments[0] == "findmnt"
        target = Path(str(arguments[arguments.index("-T") + 1]))
        logical = PurePosixPath(target.as_posix())
        if logical == PurePosixPath("/home") or PurePosixPath("/home") in logical.parents:
            return preflight.CommandResult(
                0,
                "/ /dev/sda1 ext4 ro,nosuid,relatime 8:1 /",
                "",
            )
        venue_roots = {campaign / "polymarket", campaign / "kalshi"}
        if target in venue_roots:
            relative = logical.relative_to(PurePosixPath(volume_mount.as_posix()))
            filesystem_root = (PurePosixPath("/") / relative).as_posix()
            return preflight.CommandResult(
                0,
                f"{target.as_posix()} /dev/sdb[{filesystem_root}] "
                f"ext4 rw,relatime 8:16 {filesystem_root}",
                "",
            )
        return preflight.CommandResult(
            0,
            f"{volume_mount.as_posix()} /dev/sdb ext4 "
            "ro,nosuid,relatime 8:16 /",
            "",
        )

    report = preflight.runner_namespace_admission(
        Path("/fixture/handoff.json"),
        Path("/fixture/install-admission-report.json"),
        venue=venue,
        run=findmnt,
        write_probe=lambda root: {
            "probe_removed": True,
            "surface": root.as_posix(),
        },
    )
    assert report["namespace_admissible"] is True, report
    assert report["terminal_signal"] == "PREDICTION_RUNNER_NAMESPACE_GREEN"
    assert report["venue"] == venue
    observed = report["observed_mounts"]
    assert isinstance(observed, dict)
    for key in ("home", "incoming"):
        evidence = observed[key]
        assert isinstance(evidence, dict)
        assert evidence["mount"] == "/"
        assert evidence["device_major_minor"] == "8:1"
        assert evidence["stat_device_major_minor"] == "8:1"
        assert evidence["filesystem_root"] == "/"
        assert evidence["options"] == ["ro", "nosuid", "relatime"]


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            {"mount": "/", "filesystem_root": "/", "source": "/dev/root"},
            "home mount target",
        ),
        (
            {
                "mount": "/home/other",
                "filesystem_root": "/home/other",
                "source": "/dev/root[/home/other]",
            },
            "ancestor",
        ),
        (
            {
                "mount": "/home/hyperlab/cousin",
                "filesystem_root": "/home/hyperlab/cousin",
                "source": "/dev/root[/home/hyperlab/cousin]",
            },
            "ancestor",
        ),
        (
            {
                "mount": "/home/hyperlab/hyperlab-prediction-markets/incoming/pm-20260827t120000z-deadbeef/child",
                "filesystem_root": "/home/hyperlab/hyperlab-prediction-markets/incoming/pm-20260827t120000z-deadbeef/child",
                "source": "/dev/root[/home/hyperlab/hyperlab-prediction-markets/incoming/pm-20260827t120000z-deadbeef/child]",
            },
            "ancestor",
        ),
        ({"filesystem_root": "/wrong"}, "root/bind"),
        ({"device_major_minor": "8:16"}, "device"),
        ({"filesystem": "xfs"}, "filesystem type"),
        ({"options": ["rw", "relatime"]}, "mounted ro"),
        ({"source": "/dev/root[/wrong]"}, "source/root"),
    ),
)
def test_incoming_namespace_rejects_escape_or_identity_relaxation(
    mutation: dict[str, object],
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming = Path(
        "/home/hyperlab/hyperlab-prediction-markets/incoming/"
        "pm-20260827t120000z-deadbeef"
    )
    target = "/home/hyperlab/hyperlab-prediction-markets/incoming"
    monkeypatch.setattr(
        preflight,
        "_canonical_directory",
        lambda _path, *, label: (254, 1, 0o40700),
    )
    monkeypatch.setattr(preflight, "_stat_device_major_minor", lambda _path: "254:0")
    evidence: dict[str, object] = {
        "device_major_minor": "254:0",
        "filesystem": "ext4",
        "filesystem_root": target,
        "logical_path": incoming.as_posix(),
        "mount": target,
        "options": ["ro", "relatime"],
        "source": f"/dev/root[{target}]",
        "stat_device_major_minor": "254:0",
    }
    evidence.update(mutation)
    home_evidence = {
        "device_major_minor": "254:0",
        "filesystem": "ext4",
        "filesystem_root": "/home",
        "logical_path": "/home",
        "mount": "/home",
        "options": ["ro", "relatime"],
        "source": "/dev/root[/home]",
        "stat_device_major_minor": "254:0",
    }
    with pytest.raises(preflight.PreflightError, match=message):
        preflight._authenticate_incoming_namespace_target(
            evidence,
            home_evidence=home_evidence,
            incoming=incoming,
            label="runner namespace incoming root",
        )


def test_incoming_namespace_rejects_a_symlinked_authenticated_chain_member(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming = Path(
        "/home/hyperlab/hyperlab-prediction-markets/incoming/"
        "pm-20260827t120000z-deadbeef"
    )
    target = "/home/hyperlab"

    def canonical(path: Path, *, label: str) -> tuple[int, int, int]:
        if path == Path("/home/hyperlab/hyperlab-prediction-markets"):
            raise preflight.PreflightError(f"{label} is symlinked")
        return 254, 1, 0o40700

    monkeypatch.setattr(preflight, "_canonical_directory", canonical)
    monkeypatch.setattr(preflight, "_stat_device_major_minor", lambda _path: "254:0")
    with pytest.raises(preflight.PreflightError, match="symlinked"):
        preflight._authenticate_incoming_namespace_target(
            {
                "device_major_minor": "254:0",
                "filesystem": "ext4",
                "filesystem_root": target,
                "logical_path": incoming.as_posix(),
                "mount": target,
                "options": ["ro", "relatime"],
                "source": f"/dev/root[{target}]",
                "stat_device_major_minor": "254:0",
            },
            home_evidence={
                "device_major_minor": "254:0",
                "filesystem": "ext4",
                "filesystem_root": "/home",
                "logical_path": "/home",
                "mount": "/home",
                "options": ["ro", "relatime"],
                "source": "/dev/root[/home]",
                "stat_device_major_minor": "254:0",
            },
            incoming=incoming,
            label="runner namespace incoming root",
        )


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
    assert '! -L "$SOURCE_ROOT"' in install
    assert '! -L "$CAMPAIGN_ROOT"' in install
    assert (
        'bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/install.sh" '
        '"$INCOMING_ROOT"'
    ) in install
    assert 'bash "$INCOMING_ROOT/scripts/install.sh"' not in install
    assert "PREDICTION_INSTALL_ACTIVATION_GREEN" in install
    assert install.count("sudo -v") == 1
    assert "sudo -n true" in install
    assert "sudo -n -v" in install
    assert recovery.count("sudo -v") == 1
    assert "sudo -n true" in recovery
    assert "sudo -n -v" in recovery
    assert "verify-restored" in recovery
    assert "PREDICTION_OLD_CAMPAIGN_RESTORE_VERIFIED_NO_NEW_COLLECTOR" in recovery
    assert install.index('preflight.py" host') < install.index("bootstrap-offline.sh")
    assert install.index("PREDICTION_RUNTIME_PREPARED_BEFORE_CUTOVER") < install.index(
        'cutover.sh" verify-old'
    )
    assert install.index('cutover.sh" verify-old') < install.index(
        'cutover.sh" disarm-old'
    )
    assert install.index('cutover.sh" disarm-old') < install.index(
        'prediction_markets_launch_v1/install.sh'
    )
    assert "PREDICTION_NEW_ACTIVATION_FAILED_RUN_E_RESTORE_OLD" in install
    expected_monitor = (
        f"bash '{handoff['source_root']}/ops/prediction_markets_launch_v1/monitor.sh' "
        f"'{handoff['incoming_root']}/handoff.json'"
    )
    assert expected_monitor in monitor
    assert f"{handoff['incoming_root']}/scripts/monitor.sh" not in monitor
    assert "PREDICTION_MONITOR_FIRST_SLOTS_AUTHENTICATED" in monitor
    assert "first_slots" in monitor and "NRestarts" in monitor
    assert "$SshKeyRaw = $env:HYPERLAB_PM_SSH_KEY" in tunnel
    assert "Resolve-Path -LiteralPath $SshKeyRaw" in tunnel
    assert "ssh -i $SshKeyPath -N -o ExitOnForwardFailure=yes" in tunnel
    assert "127.0.0.1:18081:127.0.0.1:18081" in tunnel
    assert "recovery|rollback" in recovery
    rendered = "\n".join((windows, install, monitor, tunnel, recovery))
    assert "df -P --output" not in rendered
    assert "systemctl" not in windows
    assert "hyperlab-h1" not in rendered
    assert "rollback-new|restore-old" in recovery
    assert "18080" not in rendered


def test_rendered_operator_readme_is_attempt_bound_nonexpert_and_h1_safe() -> None:
    handoff = _handoff()
    handoff.update(
        {
            "run_slug": "pm-20260827t120000z-deadbeef",
            "source_commit": "b" * 40,
            "superseded_campaign": launch_pack._superseded_campaign_contract(),
        }
    )
    readme = launch_pack.render_operator_readme(handoff)
    assert "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY" in readme
    assert "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE" in readme
    assert "pm-20260827t120000z-deadbeef" in readme
    assert "/campaigns/pm-20260827t120000z-deadbeef" in readme
    assert "PUBLIC_SOURCE_INVALID" in readme
    assert "source_usable=false" in readme
    assert "INTEGRITY_FAILED" in readme
    assert "runtime-import-admission" in readme
    assert "PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN" in readme
    assert "python -I" in readme
    for name in (
        "A-windows-bundle-verify-transfer.ps1",
        "B-tabby-preflight-install-activate.sh",
        "C-tabby-readonly-monitor.sh",
        "D-windows-dashboard-tunnel.ps1",
        "E-recovery-rollback.sh",
    ):
        assert name in readme
    assert "hyperlab-h1" not in readme
    assert "18080" not in readme


def test_materialized_windows_d_is_powershell_51_and_ready_only_after_ssh_establishment(
    tmp_path: Path,
) -> None:
    operator = tmp_path / "pack" / "operator"
    operator.mkdir(parents=True)
    block_d = operator / "D-windows-dashboard-tunnel.ps1"
    block_d.write_text(launch_pack.render_windows_tunnel(_handoff()), encoding="utf-8")
    ssh_key = tmp_path / "hyperlab_hetzner"
    ssh_key.write_bytes(b"SYNTHETIC_KEY_PATH_ONLY_NOT_A_KEY")
    log = tmp_path / "ssh-arguments.log"
    wrapper = tmp_path / "invoke-d.ps1"
    wrapper.write_text(
        """$ErrorActionPreference = 'Stop'
function global:ssh {
    Add-Content -LiteralPath $env:HYPERLAB_FAKE_LOG -Value ($args -join '|')
    $Expected = @(
        '-i', $env:HYPERLAB_PM_SSH_KEY,
        '-N',
        '-o', 'ExitOnForwardFailure=yes',
        '-o', 'PermitLocalCommand=yes',
        '-o', 'LocalCommand=cmd.exe /d /c echo PREDICTION_TUNNEL_READY http://127.0.0.1:18081',
        '-L', '127.0.0.1:18081:127.0.0.1:18081',
        'hyperlab@5.223.60.130'
    )
    if ($args.Count -ne $Expected.Count) { throw 'fixture SSH argument count diverged' }
    for ($Index = 0; $Index -lt $Expected.Count; $Index++) {
        if (-not [StringComparer]::Ordinal.Equals([string]$args[$Index], [string]$Expected[$Index])) {
            throw ('fixture SSH argument diverged at index ' + $Index)
        }
    }
    if ($env:HYPERLAB_FAKE_SSH_RESULT -eq 'green') {
        Write-Output 'PREDICTION_TUNNEL_READY http://127.0.0.1:18081'
        $global:LASTEXITCODE = 0
    } else {
        $global:LASTEXITCODE = 255
    }
}
& $env:HYPERLAB_D_SCRIPT
""",
        encoding="utf-8",
    )
    non_git_cwd = tmp_path / "windows-system32-equivalent"
    non_git_cwd.mkdir()
    base_environment = os.environ.copy()
    base_environment.update(
        {
            "HYPERLAB_D_SCRIPT": str(block_d),
            "HYPERLAB_FAKE_LOG": str(log),
            "HYPERLAB_PM_SSH_KEY": str(ssh_key),
            "HYPERLAB_PM_SSH_TARGET": "hyperlab@5.223.60.130",
        }
    )
    command = [
        str(_powershell_51()),
        "-NoLogo",
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(wrapper),
    ]
    failed_environment = {**base_environment, "HYPERLAB_FAKE_SSH_RESULT": "failed"}
    failed = subprocess.run(
        command,
        capture_output=True,
        check=False,
        cwd=non_git_cwd,
        env=failed_environment,
        text=True,
        timeout=30,
    )
    assert failed.returncode != 0
    assert "PREDICTION_TUNNEL_READY" not in failed.stdout
    assert "Dashboard tunnel failed" in failed.stderr

    log.unlink()
    green_environment = {**base_environment, "HYPERLAB_FAKE_SSH_RESULT": "green"}
    green = subprocess.run(
        command,
        capture_output=True,
        check=False,
        cwd=non_git_cwd,
        env=green_environment,
        text=True,
        timeout=30,
    )
    assert green.returncode == 0, green.stderr
    assert green.stdout.count("PREDICTION_TUNNEL_READY") == 1
    arguments = log.read_text(encoding="utf-8")
    assert "-N" in arguments
    assert "ExitOnForwardFailure=yes" in arguments
    assert "PermitLocalCommand=yes" in arguments
    assert "LocalCommand=cmd.exe /d /c echo PREDICTION_TUNNEL_READY" in arguments
    assert "127.0.0.1:18081:127.0.0.1:18081" in arguments


def test_materialized_tabby_b_runs_under_git_bash_and_never_false_greens(
    tmp_path: Path,
) -> None:
    environment, log = _operator_fake_environment(tmp_path)
    non_git_cwd = tmp_path / "non-git-cwd"
    non_git_cwd.mkdir()

    failed_handoff = _operator_handoff_for_git_bash(
        tmp_path, "install-failed", materialize_source_runtime=False
    )
    failed_script = tmp_path / "B-install-failed.sh"
    failed_script.write_text(
        launch_pack.render_tabby_install(failed_handoff), encoding="utf-8"
    )
    failed_environment = {
        **environment,
        "HYPERLAB_FAKE_BASH_MODE": "install",
        "HYPERLAB_FAKE_INSTALL_EXIT": "4",
    }
    failed = _run_git_bash_script(
        failed_script,
        cwd=non_git_cwd,
        environment=failed_environment,
    )
    assert failed.returncode == 4
    assert "PREDICTION_INSTALL_ACTIVATION_GREEN" not in failed.stdout
    assert "PREDICTION_NEW_ACTIVATION_FAILED_RUN_E_RESTORE_OLD" in failed.stderr

    green_handoff = _operator_handoff_for_git_bash(
        tmp_path, "install-green", materialize_source_runtime=False
    )
    green_script = tmp_path / "B-install-green.sh"
    green_script.write_text(
        launch_pack.render_tabby_install(green_handoff), encoding="utf-8"
    )
    green_environment = {
        **environment,
        "HYPERLAB_FAKE_BASH_MODE": "install",
        "HYPERLAB_FAKE_INSTALL_EXIT": "0",
    }
    green = _run_git_bash_script(
        green_script,
        cwd=non_git_cwd,
        environment=green_environment,
    )
    assert green.returncode == 0, green.stderr
    assert green.stdout.count("PREDICTION_INSTALL_ACTIVATION_GREEN") == 1
    assert green.stdout.splitlines()[-1] == "PREDICTION_INSTALL_ACTIVATION_GREEN"
    lines = log.read_text(encoding="utf-8").splitlines()
    assert any(line.startswith("python|-I ") and " host " in line for line in lines)
    assert any(line.startswith("python|-I ") and " fsync " in line for line in lines)
    assert any(line.startswith("git|clone --no-checkout ") for line in lines)
    assert any("verify-source" in line for line in lines)
    assert any(line.startswith("bash|") and "bootstrap-offline.sh" in line for line in lines)
    expected_install = (
        f"bash|{green_handoff['source_root']}/ops/prediction_markets_launch_v1/"
        f"install.sh {green_handoff['incoming_root']}"
    )
    assert expected_install in lines
    assert not any("/scripts/install.sh" in line for line in lines)
    host_index = next(index for index, line in enumerate(lines) if "preflight.py host" in line)
    bootstrap_index = next(index for index, line in enumerate(lines) if "bootstrap-offline.sh" in line)
    import_index = next(
        index
        for index, line in enumerate(lines)
        if line.startswith("venv-python|-I ") and " runtime-import-admission " in line
    )
    verify_old_index = next(index for index, line in enumerate(lines) if "cutover.sh verify-old" in line)
    disarm_index = next(index for index, line in enumerate(lines) if "cutover.sh disarm-old" in line)
    install_index = lines.index(expected_install)
    assert host_index < bootstrap_index < import_index < verify_old_index < disarm_index < install_index


def test_materialized_tabby_b_preparation_failure_never_starts_cutover(
    tmp_path: Path,
) -> None:
    environment, log = _operator_fake_environment(tmp_path)
    non_git_cwd = tmp_path / "non-git-cwd-preparation-failure"
    non_git_cwd.mkdir()
    handoff = _operator_handoff_for_git_bash(
        tmp_path,
        "bootstrap-failed-before-cutover",
        materialize_source_runtime=False,
    )
    script = tmp_path / "B-bootstrap-failed-before-cutover.sh"
    script.write_text(launch_pack.render_tabby_install(handoff), encoding="utf-8")
    completed = _run_git_bash_script(
        script,
        cwd=non_git_cwd,
        environment={
            **environment,
            "HYPERLAB_FAKE_BASH_MODE": "install",
            "HYPERLAB_FAKE_BOOTSTRAP_EXIT": "4",
        },
    )
    assert completed.returncode == 4
    assert "PREDICTION_NEW_ACTIVATION_FAILED_RUN_E_RESTORE_OLD" not in completed.stderr
    lines = log.read_text(encoding="utf-8").splitlines()
    assert any("bootstrap-offline.sh" in line for line in lines)
    assert not any("cutover.sh verify-old" in line for line in lines)
    assert not any("cutover.sh disarm-old" in line for line in lines)


def test_materialized_tabby_b_import_failure_never_starts_cutover(
    tmp_path: Path,
) -> None:
    environment, log = _operator_fake_environment(tmp_path)
    non_git_cwd = tmp_path / "non-git-cwd-import-failure"
    non_git_cwd.mkdir()
    handoff = _operator_handoff_for_git_bash(
        tmp_path,
        "import-failed-before-cutover",
        materialize_source_runtime=False,
    )
    script = tmp_path / "B-import-failed-before-cutover.sh"
    script.write_text(launch_pack.render_tabby_install(handoff), encoding="utf-8")
    completed = _run_git_bash_script(
        script,
        cwd=non_git_cwd,
        environment={
            **environment,
            "HYPERLAB_FAKE_BASH_MODE": "install",
            "HYPERLAB_FAKE_VENV_IMPORT_EXIT": "4",
        },
    )
    assert completed.returncode == 4
    assert "PREDICTION_NEW_ACTIVATION_FAILED_RUN_E_RESTORE_OLD" not in completed.stderr
    lines = log.read_text(encoding="utf-8").splitlines()
    assert any(
        line.startswith("venv-python|-I ") and " runtime-import-admission " in line
        for line in lines
    )
    assert not any("cutover.sh verify-old" in line for line in lines)
    assert not any("cutover.sh disarm-old" in line for line in lines)


def test_materialized_tabby_b_executes_real_isolated_import_admission_before_cutover(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "real-admission-fixture"
    _runtime, handoff_path, source, _inventory_path, handoff = (
        _isolated_runtime_import_fixture(fixture_root)
    )
    shutil.rmtree(source / ".venv")
    template = tmp_path / "clone-template"
    _run_git(
        "-c",
        "core.longpaths=true",
        "clone",
        "--quiet",
        "--no-hardlinks",
        str(source),
        str(template),
    )
    _run_git("config", "core.longpaths", "true", cwd=template)
    incoming = handoff_path.parent
    attempt_mount = (tmp_path / "attempt-volume").resolve()
    volume_base = attempt_mount / "prediction-markets"
    volume_base.joinpath("sources").mkdir(parents=True)
    slug = str(handoff["run_slug"])
    source = volume_base / "sources" / slug
    campaign = volume_base / "campaigns" / slug
    handoff.update(
        {
            "bundle_filename": "launch.bundle",
            "campaign_root": campaign.as_posix(),
            "incoming_root": incoming.as_posix(),
            "source_root": source.as_posix(),
            "volume_base": volume_base.as_posix(),
            "volume_mount": attempt_mount.as_posix(),
        }
    )
    handoff_raw = preflight.canonical_json_bytes(handoff) + b"\n"
    handoff_path.write_bytes(handoff_raw)
    handoff_path.with_name("handoff.sha256").write_text(
        f"{preflight.sha256_bytes(handoff_raw)}  handoff.json\n", encoding="ascii"
    )

    builder = tmp_path / "build-fresh-offline-runtime.py"
    builder.write_text(
        """from pathlib import Path
import subprocess,sys,venv
source=Path(sys.argv[1]); log=Path(sys.argv[2])
venv.EnvBuilder(with_pip=False,symlinks=False).create(source/'.venv')
runtime=source/'.venv'/('Scripts/python.exe' if sys.platform=='win32' else 'bin/python')
query=subprocess.run([str(runtime),'-I','-c',"import site; print(next(p for p in site.getsitepackages() if p.lower().endswith('site-packages')))"],capture_output=True,check=True,text=True)
site_packages=Path(query.stdout.strip())
for name in ('fastapi','requests','uvicorn','websocket'):
 (site_packages/f'{name}.py').write_text('SYNTHETIC_FIXTURE = True\\n',encoding='utf-8')
wrapper=source/'.venv'/'bin'/'python'; wrapper.parent.mkdir(parents=True,exist_ok=True)
wrapper_payload=("#!/usr/bin/bash\\n"+"printf 'venv-python|%s\\\\n' \\"$*\\" >> \\"$HYPERLAB_FAKE_LOG\\"\\n"+f'PATH="$HYPERLAB_REAL_WINDOWS_PATH" exec "{runtime.as_posix()}" "$@"\\n')
wrapper.write_text(wrapper_payload,encoding='utf-8')
wrapper.chmod(0o700)
print('PREDICTION_TEST_FRESH_OFFLINE_VENV_GREEN')
""",
        encoding="utf-8",
    )
    operator_root = tmp_path / "operator-harness"
    operator_root.mkdir()
    environment, log = _operator_fake_environment(operator_root)
    environment.update(
        {
            "HYPERLAB_FAKE_BASH_MODE": "install",
            "HYPERLAB_FAKE_CLONE_TEMPLATE": _git_bash_path(template),
            "HYPERLAB_REAL_ADMISSION_BOOTSTRAP": _git_bash_path(builder),
        }
    )
    script = tmp_path / "B-real-runtime-import-admission.sh"
    script.write_text(launch_pack.render_tabby_install(handoff), encoding="utf-8")
    non_git_cwd = tmp_path / "untrusted-non-git-cwd"
    non_git_cwd.mkdir()
    completed = _run_git_bash_script(
        script,
        cwd=non_git_cwd,
        environment=environment,
    )
    assert completed.returncode == 0, completed.stderr
    assert "PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN" in completed.stdout
    assert "PREDICTION_RUNTIME_PREPARED_BEFORE_CUTOVER" in completed.stdout
    report = json.loads(
        (incoming / "runtime-import-admission.json").read_text(encoding="utf-8")
    )
    assert report["terminal_signal"] == "PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN"
    lines = log.read_text(encoding="utf-8").splitlines()
    admission_index = next(
        index
        for index, line in enumerate(lines)
        if line.startswith("venv-python|-I ") and " runtime-import-admission " in line
    )
    verify_old_index = next(
        index for index, line in enumerate(lines) if "cutover.sh verify-old" in line
    )
    disarm_index = next(
        index for index, line in enumerate(lines) if "cutover.sh disarm-old" in line
    )
    assert admission_index < verify_old_index < disarm_index


@pytest.mark.parametrize("sudo_mode", ["foreground-refused", "cache-expired"])
def test_materialized_tabby_b_sudo_refusal_is_preparation_only(
    tmp_path: Path,
    sudo_mode: str,
) -> None:
    environment, log = _operator_fake_environment(tmp_path)
    non_git_cwd = tmp_path / f"non-git-cwd-sudo-{sudo_mode}"
    non_git_cwd.mkdir()
    handoff = _operator_handoff_for_git_bash(
        tmp_path,
        f"sudo-{sudo_mode}",
        materialize_source_runtime=False,
    )
    script = tmp_path / f"B-sudo-{sudo_mode}.sh"
    script.write_text(launch_pack.render_tabby_install(handoff), encoding="utf-8")
    completed = _run_git_bash_script(
        script,
        cwd=non_git_cwd,
        environment={
            **environment,
            "HYPERLAB_FAKE_BASH_MODE": "install",
            "HYPERLAB_FAKE_SUDO_MODE": sudo_mode,
        },
    )
    assert completed.returncode == 4
    assert "PREDICTION_NEW_ACTIVATION_FAILED_RUN_E_RESTORE_OLD" not in completed.stderr
    lines = log.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "sudo|-v"
    assert not any("cutover.sh" in line for line in lines)


def test_internal_install_runs_real_script_handles_transient_503_and_isolates_failure(
    tmp_path: Path,
) -> None:
    class HealthHandler(BaseHTTPRequestHandler):
        ready_requests = 0

        def log_message(self, _format: str, *args: object) -> None:
            del args

        def do_GET(self) -> None:
            if self.path == "/health/live":
                status = 200
                value = {"mode": "readonly", "orders_enabled": False, "status": "alive"}
            elif self.path == "/health/ready":
                type(self).ready_requests += 1
                ready = type(self).ready_requests > 2
                status = 200 if ready else 503
                value = {
                    "mode": "readonly",
                    "orders_enabled": False,
                    "status": "ready" if ready else "not-ready",
                }
            else:
                status = 404
                value = {"status": "not-found"}
            payload = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server_started = threading.Event()
    server_error: list[OSError] = []
    servers: list[ThreadingHTTPServer] = []
    first_health_marker = tmp_path / "green" / "first-health-attempt"
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as availability_probe:
            availability_probe.bind(("127.0.0.1", 18081))
    except OSError as error:
        pytest.skip(f"loopback port 18081 unavailable for install harness: {error}")

    def delayed_dashboard() -> None:
        # The real-script harness performs five authenticated unit installs
        # before the first loopback request.  Give that setup its own bounded
        # window; the production readiness deadline remains unchanged.
        deadline = time.monotonic() + 60
        while not first_health_marker.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not first_health_marker.exists():
            server_error.append(OSError("first dashboard health attempt was not observed"))
            return
        time.sleep(1.2)
        try:
            server = ThreadingHTTPServer(("127.0.0.1", 18081), HealthHandler)
        except OSError as error:
            server_error.append(error)
            return
        servers.append(server)
        server_started.set()
        server.serve_forever()

    thread = threading.Thread(target=delayed_dashboard, daemon=True)
    non_git_cwd = tmp_path / "non-git-cwd"
    non_git_cwd.mkdir()
    try:
        incoming, handoff, campaign, pack = _internal_install_fixture(
            tmp_path / "green",
            suffix="internal-green",
        )
        environment, log = _internal_install_environment(
            tmp_path / "green",
            handoff=handoff,
            pack=pack,
        )
        # Start the delayed listener only after the comparatively expensive pack
        # fixture has been materialized.  The bounded delay is intended to model
        # the install-time bind race, not fixture construction time.
        thread.start()
        green_source = Path(str(handoff["source_root"]).replace("/c/", "C:/", 1))
        green = _run_git_bash_script(
            green_source / "ops" / "prediction_markets_launch_v1" / "install.sh",
            cwd=non_git_cwd,
            environment=environment,
            arguments=(_git_bash_path(incoming),),
        )
        assert green.returncode == 0, (
            f"server_started={server_started.is_set()} server_error={server_error} "
            f"marker={first_health_marker.exists()}\n"
            f"{log.read_text(encoding='utf-8')}\n{green.stderr}"
        )
        assert server_started.is_set(), server_error
        assert "PREDICTION_DASHBOARD_READINESS_WAIT=URLError" in green.stderr
        assert "Traceback (most recent call last)" not in green.stderr
        assert green.stdout.splitlines()[-1] == "PREDICTION_INSTALL_ACTIVATION_GREEN"
        assert HealthHandler.ready_requests == 3
        manifest = json.loads((pack / "campaign-manifest.json").read_bytes())
        activation = json.loads(
            (campaign / "state" / "activation-receipt.json").read_bytes()
        )
        assert activation["campaign_id"] == manifest["campaign_id"]
        assert activation["campaign_manifest_sha256"] == manifest["manifest_sha256"]
        green_log = log.read_text(encoding="utf-8")
        services = handoff["services"]
        assert isinstance(services, dict)
        dashboard_enable = f"systemctl enable --now {services['dashboard']}"
        polymarket_enable = f"systemctl enable --now {services['polymarket']}"
        kalshi_enable = f"systemctl enable --now {services['kalshi']}"
        suffix = str(handoff["run_slug"]).removeprefix("pm-")
        polymarket_probe = (
            f"hyperlab-pm-{suffix}-polymarket-namespace-probe.service"
        )
        kalshi_probe = f"hyperlab-pm-{suffix}-kalshi-namespace-probe.service"
        polymarket_probe_start = f"systemctl start {polymarket_probe}"
        kalshi_probe_start = f"systemctl start {kalshi_probe}"
        assert green_log.index(polymarket_probe_start) < green_log.index(
            kalshi_probe_start
        )
        assert green_log.index(kalshi_probe_start) < green_log.index(dashboard_enable)
        assert green_log.index(dashboard_enable) < green_log.index(polymarket_enable)
        assert green_log.index(kalshi_probe_start) < green_log.index(
            polymarket_enable
        )
        assert green_log.index(polymarket_enable) < green_log.index(kalshi_enable)

        refused_incoming, refused_handoff, _refused_campaign, refused_pack = (
            _internal_install_fixture(
                tmp_path / "namespace-refused",
                suffix="namespace-refused",
            )
        )
        refused_services = refused_handoff["services"]
        assert isinstance(refused_services, dict)
        refused_suffix = str(refused_handoff["run_slug"]).removeprefix("pm-")
        refused_probe = (
            f"hyperlab-pm-{refused_suffix}-kalshi-namespace-probe.service"
        )
        refused_environment, refused_log_path = _internal_install_environment(
            tmp_path / "namespace-refused",
            handoff=refused_handoff,
            pack=refused_pack,
            namespace_probe_failure=refused_probe,
        )
        refused_source = Path(
            str(refused_handoff["source_root"]).replace("/c/", "C:/", 1)
        )
        namespace_refused = _run_git_bash_script(
            refused_source / "ops" / "prediction_markets_launch_v1" / "install.sh",
            cwd=non_git_cwd,
            environment=refused_environment,
            arguments=(_git_bash_path(refused_incoming),),
        )
        refused_output = namespace_refused.stdout + namespace_refused.stderr
        assert namespace_refused.returncode == 4, refused_output
        assert "PREDICTION_NAMESPACE_PROBE_DIAGNOSTIC=" in refused_output
        assert "collector namespace probe refused before any persistent service activation: kalshi" in refused_output
        assert "PREDICTION_INSTALL_ACTIVATION_GREEN" not in refused_output
        assert '"TARGET":"/"' in refused_output
        assert '"SOURCE":"/dev/sda1"' in refused_output
        assert '"FSTYPE":"ext4"' in refused_output
        assert '"VFS_OPTIONS":["ro","nosuid","relatime"]' in refused_output
        assert '"MAJ:MIN":"8:1"' in refused_output
        assert '"FSROOT":"/"' in refused_output
        expected_refused_logical_path = str(refused_handoff["incoming_root"])
        assert (
            f'"LOGICAL_PATH":"{expected_refused_logical_path}"'
            in refused_output
        )
        refused_log = refused_log_path.read_text(encoding="utf-8")
        assert (
            f"systemctl start hyperlab-pm-{refused_suffix}-polymarket-namespace-probe.service"
            in refused_log
        )
        assert f"systemctl start {refused_probe}" in refused_log
        assert (
            f"systemctl enable --now {refused_services['dashboard']}"
            not in refused_log
        )
        for venue in ("polymarket", "kalshi"):
            assert (
                f"systemctl enable --now {refused_services[venue]}"
                not in refused_log
            )
        refused_cleanup = [
            *refused_services.values(),
            f"hyperlab-pm-{refused_suffix}-polymarket-namespace-probe.service",
            f"hyperlab-pm-{refused_suffix}-kalshi-namespace-probe.service",
        ]
        assert len(set(refused_cleanup)) == 5
        for service in refused_cleanup:
            assert f"systemctl stop {service}" in refused_log
            assert f"systemctl disable {service}" in refused_log

        cleanup_incoming, cleanup_handoff, _cleanup_campaign, cleanup_pack = (
            _internal_install_fixture(
                tmp_path / "namespace-show-cleanup-failure",
                suffix="namespace-show-cleanup-failure",
            )
        )
        cleanup_services = cleanup_handoff["services"]
        assert isinstance(cleanup_services, dict)
        cleanup_suffix = str(cleanup_handoff["run_slug"]).removeprefix("pm-")
        cleanup_probe = (
            f"hyperlab-pm-{cleanup_suffix}-polymarket-namespace-probe.service"
        )
        cleanup_environment, cleanup_log_path = _internal_install_environment(
            tmp_path / "namespace-show-cleanup-failure",
            handoff=cleanup_handoff,
            pack=cleanup_pack,
            namespace_probe_show_failure=cleanup_probe,
            cleanup_failure_service=str(cleanup_services["dashboard"]),
        )
        cleanup_source = Path(
            str(cleanup_handoff["source_root"]).replace("/c/", "C:/", 1)
        )
        cleanup_failed = _run_git_bash_script(
            cleanup_source / "ops" / "prediction_markets_launch_v1" / "install.sh",
            cwd=non_git_cwd,
            environment=cleanup_environment,
            arguments=(_git_bash_path(cleanup_incoming),),
        )
        cleanup_output = cleanup_failed.stdout + cleanup_failed.stderr
        assert cleanup_failed.returncode == 4, cleanup_output
        assert (
            "namespace probe properties unavailable and Prediction Markets cleanup also failed"
            in cleanup_output
        )
        assert "PREDICTION_INSTALL_ACTIVATION_GREEN" not in cleanup_output
        cleanup_log = cleanup_log_path.read_text(encoding="utf-8")
        for venue in ("polymarket", "kalshi"):
            assert (
                f"systemctl enable --now {cleanup_services[venue]}"
                not in cleanup_log
            )

        failed_incoming, failed_handoff, _failed_campaign, failed_pack = (
            _internal_install_fixture(
                tmp_path / "partial",
                suffix="internal-partial",
            )
        )
        failed_services = failed_handoff["services"]
        assert isinstance(failed_services, dict)
        failed_environment, failed_log_path = _internal_install_environment(
            tmp_path / "partial",
            handoff=failed_handoff,
            pack=failed_pack,
            failed_service=str(failed_services["polymarket"]),
        )
        failed_source = Path(
            str(failed_handoff["source_root"]).replace("/c/", "C:/", 1)
        )
        partial = _run_git_bash_script(
            failed_source / "ops" / "prediction_markets_launch_v1" / "install.sh",
            cwd=non_git_cwd,
            environment=failed_environment,
            arguments=(_git_bash_path(failed_incoming),),
        )
        assert partial.returncode == 4
        assert "PREDICTION_INSTALL_ACTIVATION_GREEN" not in partial.stdout + partial.stderr
        assert partial.stderr.splitlines()[-1] == (
            "PREDICTION_INSTALL_REFUSED:collector activation failed before readiness: polymarket"
        )
        failed_log = failed_log_path.read_text(encoding="utf-8")
        suffix = str(failed_handoff["run_slug"]).removeprefix("pm-")
        expected_cleanup = [
            *failed_services.values(),
            f"hyperlab-pm-{suffix}-polymarket-namespace-probe.service",
            f"hyperlab-pm-{suffix}-kalshi-namespace-probe.service",
        ]
        for service in expected_cleanup:
            assert f"systemctl stop {service}" in failed_log
            assert f"systemctl disable {service}" in failed_log
        assert f"systemctl enable --now {failed_services['kalshi']}" not in failed_log
    finally:
        if server_started.is_set() and servers:
            servers[0].shutdown()
        if servers:
            servers[0].server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize(
    ("property_mode", "payload_mode", "exit_four"),
    (
        ("active", "green", False),
        ("result-failed", "green", False),
        ("restart", "green", False),
        ("status4", "green", False),
        ("fragment", "green", False),
        ("green", "absent", False),
        ("green", "malformed", False),
        ("green", "refused", False),
        ("green", "refused", True),
    ),
)
def test_internal_install_refuses_probe_property_or_payload_divergence_before_activation(
    tmp_path: Path,
    property_mode: str,
    payload_mode: str,
    exit_four: bool,
) -> None:
    case = f"probe-{property_mode}-{payload_mode}-exit4-{int(exit_four)}"
    incoming, handoff, _campaign, pack = _internal_install_fixture(
        tmp_path / case,
        suffix=case,
    )
    services = handoff["services"]
    assert isinstance(services, dict)
    suffix = str(handoff["run_slug"]).removeprefix("pm-")
    polymarket_probe = (
        f"hyperlab-pm-{suffix}-polymarket-namespace-probe.service"
    )
    environment, log_path = _internal_install_environment(
        tmp_path / case,
        handoff=handoff,
        pack=pack,
        namespace_probe_failure=polymarket_probe if exit_four else None,
        namespace_probe_property_mode=property_mode,
        namespace_probe_payload_mode=payload_mode,
    )
    non_git_cwd = tmp_path / f"non-git-{case}"
    non_git_cwd.mkdir()
    source = Path(str(handoff["source_root"]).replace("/c/", "C:/", 1))
    completed = _run_git_bash_script(
        source / "ops" / "prediction_markets_launch_v1" / "install.sh",
        cwd=non_git_cwd,
        environment=environment,
        arguments=(_git_bash_path(incoming),),
    )
    output = completed.stdout + completed.stderr
    assert completed.returncode == 4, output
    assert "PREDICTION_INSTALL_ACTIVATION_GREEN" not in output
    assert "PREDICTION_NAMESPACE_PROBE_DIAGNOSTIC=" in output
    log = log_path.read_text(encoding="utf-8")
    assert f"systemctl start {polymarket_probe}" in log
    assert f"systemctl enable --now {services['dashboard']}" not in log
    for venue in ("polymarket", "kalshi"):
        assert f"systemctl enable --now {services[venue]}" not in log
    cleanup_services = [
        *services.values(),
        polymarket_probe,
        f"hyperlab-pm-{suffix}-kalshi-namespace-probe.service",
    ]
    assert len(set(cleanup_services)) == 5
    for service in cleanup_services:
        assert f"systemctl stop {service}" in log
        assert f"systemctl disable {service}" in log


def test_internal_install_reports_terminal_exit_four_before_state_and_disarms_all(
    tmp_path: Path,
) -> None:
    class HealthHandler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: object) -> None:
            del args

        def do_GET(self) -> None:
            value = {
                "mode": "readonly",
                "orders_enabled": False,
                "status": "ready" if self.path == "/health/ready" else "alive",
            }
            payload = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 18081), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    non_git_cwd = tmp_path / "non-git-cwd"
    non_git_cwd.mkdir()
    try:
        incoming, handoff, campaign, pack = _internal_install_fixture(
            tmp_path / "terminal-exit-four",
            suffix="terminal-exit-four",
        )
        services = handoff["services"]
        assert isinstance(services, dict)
        environment, log_path = _internal_install_environment(
            tmp_path / "terminal-exit-four",
            handoff=handoff,
            pack=pack,
            terminal_exit_service=str(services["polymarket"]),
        )
        source = Path(str(handoff["source_root"]).replace("/c/", "C:/", 1))
        completed = _run_git_bash_script(
            source / "ops" / "prediction_markets_launch_v1" / "install.sh",
            cwd=non_git_cwd,
            environment=environment,
            arguments=(_git_bash_path(incoming),),
        )
        output = completed.stdout + completed.stderr
        assert completed.returncode == 4, output
        assert "AssertionError" not in output
        assert "PREDICTION_COLLECTOR_READINESS_INVARIANT=state-object-present" in output
        assert "PREDICTION_COLLECTOR_TERMINAL_BEFORE_READINESS=polymarket" in output
        assert "PREDICTION_COLLECTOR_READINESS_DIAGNOSTIC=" in output
        assert "Result=exit-code" in output
        assert "ExecMainStatus=4" in output
        assert '"state_present":false' in output
        assert '"ledger_present":false' in output
        assert "PREDICTION_RUNNER_REFUSED:runner startup admission refused" in output
        assert "PREDICTION_INSTALL_ACTIVATION_GREEN" not in output
        assert not (campaign / "polymarket" / "state.json").exists()
        assert not (campaign / "polymarket" / "ledger.jsonl").exists()
        sentinel = campaign / "polymarket" / "SYNTHETIC_RAW_PRESERVED.bin"
        assert sentinel.read_bytes() == b"SYNTHETIC/FIXTURE raw preservation sentinel\n"
        log = log_path.read_text(encoding="utf-8")
        collector_enable = f"systemctl enable --now {services['polymarket']}"
        assert collector_enable in log
        after_enable = log.split(collector_enable, 1)[1]
        assert "sleep|0.5" not in after_enable
        assert f"systemctl enable --now {services['kalshi']}" not in log
        suffix = str(handoff["run_slug"]).removeprefix("pm-")
        cleanup_services = [
            *services.values(),
            f"hyperlab-pm-{suffix}-polymarket-namespace-probe.service",
            f"hyperlab-pm-{suffix}-kalshi-namespace-probe.service",
        ]
        for service in cleanup_services:
            assert f"systemctl stop {service}" in log
            assert f"systemctl disable {service}" in log
        service_state = Path(
            environment["HYPERLAB_SERVICE_STATE_DIR"].replace("/c/", "C:/", 1)
        )
        assert not list(service_state.glob("*.failed"))
        assert not list(service_state.glob("*.probe-success"))

        diagnostic_incoming, diagnostic_handoff, _diagnostic_campaign, diagnostic_pack = (
            _internal_install_fixture(
                tmp_path / "diagnostic-encoding-failure",
                suffix="diagnostic-encoding-failure",
            )
        )
        diagnostic_services = diagnostic_handoff["services"]
        assert isinstance(diagnostic_services, dict)
        diagnostic_environment, diagnostic_log_path = _internal_install_environment(
            tmp_path / "diagnostic-encoding-failure",
            handoff=diagnostic_handoff,
            pack=diagnostic_pack,
            terminal_exit_service=str(diagnostic_services["polymarket"]),
            fail_diagnostic_encoding=True,
        )
        diagnostic_source = Path(
            str(diagnostic_handoff["source_root"]).replace("/c/", "C:/", 1)
        )
        diagnostic_failed = _run_git_bash_script(
            diagnostic_source
            / "ops"
            / "prediction_markets_launch_v1"
            / "install.sh",
            cwd=non_git_cwd,
            environment=diagnostic_environment,
            arguments=(_git_bash_path(diagnostic_incoming),),
        )
        diagnostic_output = diagnostic_failed.stdout + diagnostic_failed.stderr
        assert diagnostic_failed.returncode == 4, diagnostic_output
        assert (
            "PREDICTION_COLLECTOR_READINESS_DIAGNOSTIC=ENCODING_FAILED"
            in diagnostic_output
        )
        diagnostic_log = diagnostic_log_path.read_text(encoding="utf-8")
        diagnostic_suffix = str(diagnostic_handoff["run_slug"]).removeprefix("pm-")
        diagnostic_cleanup = [
            *diagnostic_services.values(),
            f"hyperlab-pm-{diagnostic_suffix}-polymarket-namespace-probe.service",
            f"hyperlab-pm-{diagnostic_suffix}-kalshi-namespace-probe.service",
        ]
        for service in diagnostic_cleanup:
            assert f"systemctl stop {service}" in diagnostic_log
            assert f"systemctl disable {service}" in diagnostic_log
        assert (
            f"systemctl enable --now {diagnostic_services['kalshi']}"
            not in diagnostic_log
        )

        clean_incoming, clean_handoff, _clean_campaign, clean_pack = (
            _internal_install_fixture(
                tmp_path / "clean-exit-before-state",
                suffix="clean-exit-before-state",
            )
        )
        clean_services = clean_handoff["services"]
        assert isinstance(clean_services, dict)
        clean_environment, clean_log_path = _internal_install_environment(
            tmp_path / "clean-exit-before-state",
            handoff=clean_handoff,
            pack=clean_pack,
            clean_exit_service=str(clean_services["polymarket"]),
        )
        clean_source = Path(
            str(clean_handoff["source_root"]).replace("/c/", "C:/", 1)
        )
        clean_failed = _run_git_bash_script(
            clean_source / "ops" / "prediction_markets_launch_v1" / "install.sh",
            cwd=non_git_cwd,
            environment=clean_environment,
            arguments=(_git_bash_path(clean_incoming),),
        )
        clean_output = clean_failed.stdout + clean_failed.stderr
        assert clean_failed.returncode == 4, clean_output
        assert "PREDICTION_COLLECTOR_TERMINAL_BEFORE_READINESS=polymarket" in clean_output
        assert '"state_present":false' in clean_output
        clean_log = clean_log_path.read_text(encoding="utf-8")
        clean_enable = f"systemctl enable --now {clean_services['polymarket']}"
        assert clean_enable in clean_log
        assert "sleep|0.5" not in clean_log.split(clean_enable, 1)[1]
        assert f"systemctl enable --now {clean_services['kalshi']}" not in clean_log
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize(
    ("failure_mode", "expected_failure_class", "collector_guard_failure"),
    [
        ("helper-import", "MONITOR_RUNTIME_IMPORT_FAILED", False),
        ("initial-evidence", "INITIAL_EVIDENCE_INVALID", False),
        ("pid-diverged", None, False),
        ("command-diverged", None, False),
        ("dashboard-never", None, False),
        ("collector-guard", None, True),
        ("eligible-parser", None, False),
        ("prepared-stale", None, False),
    ],
)
def test_internal_install_refuses_bootstrap_readiness_and_stale_negatives_without_false_green(
    tmp_path: Path,
    failure_mode: str,
    expected_failure_class: str | None,
    collector_guard_failure: bool,
) -> None:
    class HealthHandler(BaseHTTPRequestHandler):
        unavailable = failure_mode == "dashboard-never"

        def log_message(self, _format: str, *args: object) -> None:
            del args

        def do_GET(self) -> None:
            status = 503 if type(self).unavailable else 200
            value = {
                "mode": "readonly",
                "orders_enabled": False,
                "status": "not-ready" if status == 503 else (
                    "ready" if self.path == "/health/ready" else "alive"
                ),
            }
            payload = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 18081), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    case_root = tmp_path / failure_mode
    non_git_cwd = tmp_path / f"non-git-{failure_mode}"
    non_git_cwd.mkdir()
    try:
        incoming, handoff, _campaign, pack = _internal_install_fixture(
            case_root,
            suffix=f"negative-{failure_mode}",
        )
        monitor_failure = (
            failure_mode
            if failure_mode
            in {
                "helper-import",
                "initial-evidence",
                "pid-diverged",
                "command-diverged",
                "prepared-stale",
            }
            else None
        )
        environment, log_path = _internal_install_environment(
            case_root,
            handoff=handoff,
            pack=pack,
            monitor_failure=monitor_failure,
            collector_guard_failure=collector_guard_failure,
            monitor_eligible_venues=(
                ("kalshi", "kalshi")
                if failure_mode == "eligible-parser"
                else ("polymarket", "kalshi")
            ),
        )
        environment["HYPERLAB_FAKE_SLEEP_SECONDS"] = ""
        source = Path(str(handoff["source_root"]).replace("/c/", "C:/", 1))
        completed = _run_git_bash_script(
            source / "ops" / "prediction_markets_launch_v1" / "install.sh",
            cwd=non_git_cwd,
            environment=environment,
            arguments=(_git_bash_path(incoming),),
        )
        output = completed.stdout + completed.stderr
        assert completed.returncode == 4, output
        assert "NameError" not in output
        assert "PREDICTION_INSTALL_ACTIVATION_GREEN" not in output
        if expected_failure_class is not None:
            assert expected_failure_class in output
        if failure_mode == "eligible-parser":
            assert "authenticated eligible venue parsing failed" in output
        services = handoff["services"]
        assert isinstance(services, dict)
        log = log_path.read_text(encoding="utf-8")
        dashboard = str(services["dashboard"])
        assert f"systemctl enable --now {dashboard}" in log
        if failure_mode == "prepared-stale":
            assert f"systemctl enable --now {services['polymarket']}" in log
            assert f"systemctl enable --now {services['kalshi']}" not in log
            suffix = str(handoff["run_slug"]).removeprefix("pm-")
            cleanup_services = [
                *services.values(),
                f"hyperlab-pm-{suffix}-polymarket-namespace-probe.service",
                f"hyperlab-pm-{suffix}-kalshi-namespace-probe.service",
            ]
            for service in cleanup_services:
                assert f"systemctl stop {service}" in log
                assert f"systemctl disable {service}" in log
            assert "collector readiness failed before authenticated state: polymarket" in output
        else:
            for venue in ("polymarket", "kalshi"):
                assert f"systemctl enable --now {services[venue]}" not in log
            assert f"systemctl stop {dashboard}" in log
            assert f"systemctl disable {dashboard}" in log
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_internal_install_binds_empty_or_partial_venue_selection_to_authenticated_monitor(
    tmp_path: Path,
) -> None:
    class HealthHandler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: object) -> None:
            del args

        def do_GET(self) -> None:
            value = {
                "mode": "readonly",
                "orders_enabled": False,
                "status": "ready" if self.path == "/health/ready" else "alive",
            }
            payload = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 18081), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    non_git_cwd = tmp_path / "non-git-cwd"
    non_git_cwd.mkdir()

    def run_case(
        name: str,
        *,
        eligible: tuple[str, ...],
        mutate_incoming: bool,
    ) -> tuple[subprocess.CompletedProcess[str], str, dict[str, object], Path]:
        case_root = tmp_path / name
        incoming, handoff, _campaign, pack = _internal_install_fixture(
            case_root,
            suffix=name,
            eligible_venues=eligible,
        )
        environment, log_path = _internal_install_environment(
            case_root,
            handoff=handoff,
            pack=pack,
            monitor_eligible_venues=eligible,
            mutate_incoming_eligibility=mutate_incoming,
        )
        environment["HYPERLAB_FAKE_SLEEP_SECONDS"] = ""
        source = Path(str(handoff["source_root"]).replace("/c/", "C:/", 1))
        completed = _run_git_bash_script(
            source / "ops" / "prediction_markets_launch_v1" / "install.sh",
            cwd=non_git_cwd,
            environment=environment,
            arguments=(_git_bash_path(incoming),),
        )
        return completed, log_path.read_text(encoding="utf-8"), handoff, incoming

    try:
        unavailable, unavailable_log, unavailable_handoff, _ = run_case(
            "both-unavailable",
            eligible=(),
            mutate_incoming=False,
        )
        assert unavailable.returncode == 0, unavailable.stderr
        assert "PREDICTION_ELIGIBLE_VENUES=NONE" in unavailable.stdout
        unavailable_services = unavailable_handoff["services"]
        assert isinstance(unavailable_services, dict)
        assert (
            f"systemctl enable --now {unavailable_services['dashboard']}"
            in unavailable_log
        )
        for venue in ("polymarket", "kalshi"):
            assert (
                f"systemctl enable --now {unavailable_services[venue]}"
                not in unavailable_log
            )

        partial, partial_log, partial_handoff, partial_incoming = run_case(
            "partial-immutable",
            eligible=("kalshi",),
            mutate_incoming=True,
        )
        assert partial.returncode == 0, partial.stderr
        assert "PREDICTION_ELIGIBLE_VENUES=kalshi" in partial.stdout
        assert json.loads(
            (partial_incoming / "host-preflight-report.json").read_bytes()
        )["eligible_venues"] == ["polymarket", "kalshi"]
        partial_services = partial_handoff["services"]
        assert isinstance(partial_services, dict)
        assert f"systemctl enable --now {partial_services['kalshi']}" in partial_log
        assert (
            f"systemctl enable --now {partial_services['polymarket']}"
            not in partial_log
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_materialized_tabby_c_stops_on_semantic_transition_with_real_json_parser(
    tmp_path: Path,
) -> None:
    environment, log = _operator_fake_environment(tmp_path)
    environment.update(
        {
            "HYPERLAB_FAKE_BASH_MODE": "monitor",
            "HYPERLAB_FAKE_PYTHON_MODE": "forward",
        }
    )
    handoff = _operator_handoff_for_git_bash(tmp_path, "monitor")
    script = tmp_path / "C-monitor.sh"
    script.write_text(launch_pack.render_tabby_monitor(handoff), encoding="utf-8")
    non_git_cwd = tmp_path / "monitor-non-git-cwd"
    non_git_cwd.mkdir()
    completed = _run_git_bash_script(
        script,
        cwd=non_git_cwd,
        environment=environment,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.count("PREDICTION_MONITOR_FIRST_SLOTS_AUTHENTICATED") == 1
    assert completed.stdout.count('"alert":false') == 1
    lines = log.read_text(encoding="utf-8").splitlines()
    expected_monitor = (
        "bash|"
        f"{handoff['source_root']}/ops/prediction_markets_launch_v1/monitor.sh "
        f"{handoff['incoming_root']}/handoff.json"
    )
    assert lines.count(expected_monitor) == 1
    assert not any(
        line.startswith("bash|")
        and f"{handoff['incoming_root']}/scripts/monitor.sh" in line
        for line in lines
    )
    assert sum(line.startswith("sleep|10") for line in lines) == 0


@pytest.mark.parametrize(
    ("mode", "diagnostic"),
    [
        ("monitor-exit", "PREDICTION_MONITOR_EXECUTION_FAILED"),
        ("monitor-invalid", "PREDICTION_MONITOR_JSON_INVALID"),
    ],
)
def test_materialized_tabby_c_always_signals_on_monitor_or_json_failure(
    tmp_path: Path,
    mode: str,
    diagnostic: str,
) -> None:
    environment, _log = _operator_fake_environment(tmp_path)
    environment.update(
        {
            "HYPERLAB_FAKE_BASH_MODE": mode,
            "HYPERLAB_FAKE_PYTHON_MODE": "forward",
        }
    )
    handoff = _operator_handoff_for_git_bash(tmp_path, mode)
    script = tmp_path / f"C-{mode}.sh"
    script.write_text(launch_pack.render_tabby_monitor(handoff), encoding="utf-8")
    non_git_cwd = tmp_path / f"{mode}-non-git-cwd"
    non_git_cwd.mkdir()
    completed = _run_git_bash_script(
        script,
        cwd=non_git_cwd,
        environment=environment,
    )
    assert completed.returncode == 4
    assert diagnostic in completed.stderr
    assert completed.stdout.count("PREDICTION_MONITOR_OPERATIONAL_FAILURE") == 1


@pytest.mark.parametrize(
    ("mode", "signal"),
    [
        ("recovery", "PREDICTION_RECOVERY_GREEN"),
        ("rollback-new", "PREDICTION_ROLLBACK_GREEN"),
    ],
)
def test_materialized_tabby_e_dispatches_only_the_selected_safe_mode(
    tmp_path: Path,
    mode: str,
    signal: str,
) -> None:
    environment, log = _operator_fake_environment(tmp_path)
    environment["HYPERLAB_FAKE_BASH_MODE"] = mode
    handoff = _operator_handoff_for_git_bash(tmp_path, mode)
    script = tmp_path / f"E-{mode}.sh"
    script.write_text(launch_pack.render_recovery_rollback(handoff), encoding="utf-8")
    non_git_cwd = tmp_path / f"{mode}-non-git-cwd"
    non_git_cwd.mkdir()
    completed = subprocess.run(
        [
            str(_git_bash()),
            "--noprofile",
            "--norc",
            _git_bash_path(script),
            mode,
        ],
        capture_output=True,
        check=False,
        cwd=non_git_cwd,
        env=environment,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.count(signal) == 1
    lines = log.read_text(encoding="utf-8").splitlines()
    dispatches = [line for line in lines if line.startswith("bash|")]
    assert len(dispatches) == 1
    expected_mode = "rollback" if mode == "rollback-new" else mode
    assert f"rollback.sh {expected_mode} " in dispatches[0]
    assert "hyperlab-h1" not in dispatches[0]


def test_materialized_tabby_e_restore_reauthenticates_final_old_state(
    tmp_path: Path,
) -> None:
    environment, log = _operator_fake_environment(tmp_path)
    environment["HYPERLAB_FAKE_BASH_MODE"] = "restore-old"
    handoff = _operator_handoff_for_git_bash(tmp_path, "restore-old")
    script = tmp_path / "E-restore-old.sh"
    script.write_text(launch_pack.render_recovery_rollback(handoff), encoding="utf-8")
    non_git_cwd = tmp_path / "restore-old-non-git-cwd"
    non_git_cwd.mkdir()
    completed = _run_git_bash_script(
        script,
        cwd=non_git_cwd,
        environment=environment,
        arguments=("restore-old",),
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines()[-1] == (
        "PREDICTION_OLD_CAMPAIGN_RESTORE_VERIFIED_NO_NEW_COLLECTOR"
    )
    lines = log.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "sudo|-v"
    assert lines[1] == "sudo|-n true"
    dispatches = [line for line in lines if line.startswith("bash|")]
    assert [line.split("cutover.sh ", 1)[1].split(" ", 1)[0] for line in dispatches] == [
        "restore-old",
        "verify-restored",
    ]


def _git_output(*arguments: str, cwd: Path) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        capture_output=True,
        check=False,
        cwd=cwd,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.strip()


def _isolated_runtime_import_fixture(
    tmp_path: Path,
) -> tuple[Path, Path, Path, Path, dict[str, object]]:
    slug = "pm-20260828t170000z-cafefeed"
    volume_mount = (tmp_path / "volume").resolve()
    volume_base = volume_mount / "prediction-markets"
    source = volume_base / "sources" / slug
    incoming = (tmp_path / "incoming" / slug).resolve()
    source.joinpath("src", "hyperlab").mkdir(parents=True)
    source.joinpath("ops", "prediction_markets_launch_v1").mkdir(parents=True)
    incoming.joinpath("scripts").mkdir(parents=True)
    (source / ".gitignore").write_text(".venv/\n__pycache__/\n", encoding="utf-8")
    (source / "src" / "hyperlab" / "__init__.py").write_text(
        "SYNTHETIC_FIXTURE = True\n", encoding="utf-8"
    )
    (source / "ops" / "__init__.py").write_text("", encoding="utf-8")
    (source / "ops" / "prediction_markets_launch_v1" / "__init__.py").write_text(
        "", encoding="utf-8"
    )
    helpers = "\n".join(
        f"def {name}(*args, **kwargs):\n    return None\n"
        for name in (
            "_validate_venue_state",
            "active_optional_service_is_admissible",
            "classify_monitored_service",
            "complete_service_is_admissible",
            "prepared_state_is_stale",
            "validate_activation_evidence",
        )
    )
    (source / "ops" / "prediction_markets_launch_v1" / "cockpit.py").write_text(
        helpers, encoding="utf-8"
    )
    runner_helpers = (
        "def read_ledger(*args, **kwargs):\n    return []\n\n"
        "def validate_service_ledger_against_manifest(*args, **kwargs):\n    return None\n"
    )
    (source / "ops" / "prediction_markets_launch_v1" / "runner.py").write_text(
        runner_helpers, encoding="utf-8"
    )
    (source / "ops" / "prediction_markets_launch_v1" / "preflight.py").write_text(
        "SYNTHETIC_FIXTURE = True\n", encoding="utf-8"
    )
    _run_git("init", "--quiet", str(source))
    _run_git("config", "core.longpaths", "true", cwd=source)
    _run_git("config", "user.email", "synthetic-fixture@invalid.example", cwd=source)
    _run_git("config", "user.name", "Synthetic Fixture", cwd=source)
    _run_git("add", ".", cwd=source)
    _run_git("commit", "--quiet", "-m", "isolated runtime fixture", cwd=source)
    commit = _git_output("rev-parse", "HEAD", cwd=source)
    inventory = launch_pack.build_source_inventory(source, commit)
    inventory_path = incoming / "source-inventory.json"
    inventory_path.write_bytes(preflight.canonical_json_bytes(inventory) + b"\n")
    shutil.copy2(OPS / "preflight.py", incoming / "scripts" / "preflight.py")
    shutil.copy2(OPS / "launch_pack.py", incoming / "scripts" / "launch_pack.py")

    venv.EnvBuilder(with_pip=False, symlinks=False).create(source / ".venv")
    runtime = source / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    site_query = subprocess.run(
        [
            str(runtime),
            "-I",
            "-c",
            (
                "import site; print(next(p for p in site.getsitepackages() "
                "if p.lower().endswith('site-packages')))"
            ),
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    assert site_query.returncode == 0, site_query.stderr
    site_packages = Path(site_query.stdout.strip())
    for module_name in preflight._RUNTIME_VENV_MODULES:
        (site_packages / f"{module_name}.py").write_text(
            "SYNTHETIC_FIXTURE = True\n", encoding="utf-8"
        )

    handoff = {
        "boundary": preflight.BOUNDARY,
        "campaign_root": str(volume_base / "campaigns" / slug),
        "incoming_root": str(incoming),
        "run_slug": slug,
        "schema_version": 1,
        "source_commit": commit,
        "source_inventory_sha256": inventory["inventory_sha256"],
        "source_root": str(source),
        "volume_base": str(volume_base),
        "volume_mount": str(volume_mount),
    }
    handoff_raw = preflight.canonical_json_bytes(handoff) + b"\n"
    handoff_path = incoming / "handoff.json"
    handoff_path.write_bytes(handoff_raw)
    (incoming / "handoff.sha256").write_text(
        f"{preflight.sha256_bytes(handoff_raw)}  handoff.json\n", encoding="ascii"
    )
    return runtime, handoff_path, source, inventory_path, handoff


@pytest.mark.parametrize("sudo_mode", ["foreground-refused", "cache-expired"])
def test_materialized_tabby_e_sudo_refusal_never_dispatches(
    tmp_path: Path,
    sudo_mode: str,
) -> None:
    environment, log = _operator_fake_environment(tmp_path)
    environment.update(
        {
            "HYPERLAB_FAKE_BASH_MODE": "restore-old",
            "HYPERLAB_FAKE_SUDO_MODE": sudo_mode,
        }
    )
    handoff = _operator_handoff_for_git_bash(tmp_path, f"restore-{sudo_mode}")
    script = tmp_path / f"E-restore-{sudo_mode}.sh"
    script.write_text(launch_pack.render_recovery_rollback(handoff), encoding="utf-8")
    non_git_cwd = tmp_path / f"restore-{sudo_mode}-non-git-cwd"
    non_git_cwd.mkdir()
    completed = _run_git_bash_script(
        script,
        cwd=non_git_cwd,
        environment=environment,
        arguments=("restore-old",),
    )
    assert completed.returncode == 4
    assert "PREDICTION_OLD_CAMPAIGN_RESTORE_VERIFIED_NO_NEW_COLLECTOR" not in completed.stdout
    assert not any(
        line.startswith("bash|") for line in log.read_text(encoding="utf-8").splitlines()
    )


def test_internal_rollback_runs_real_script_and_preserves_campaign_bytes(
    tmp_path: Path,
) -> None:
    slug = "pm-20260827t235959z-deadbeef"
    suffix = slug.removeprefix("pm-")
    virtual_home = tmp_path / "mounted-home"
    virtual_mnt = tmp_path / "mounted-mnt"
    incoming = (
        virtual_home
        / "hyperlab"
        / "hyperlab-prediction-markets"
        / "incoming"
        / slug
    )
    source = (
        virtual_mnt
        / "HC_Volume_106716684"
        / "hyperlab-prediction-markets"
        / "sources"
        / slug
    )
    campaign = (
        virtual_mnt
        / "HC_Volume_106716684"
        / "hyperlab-prediction-markets"
        / "campaigns"
        / slug
    )
    incoming.joinpath("scripts").mkdir(parents=True)
    source.mkdir(parents=True)
    campaign.joinpath("polymarket", "raw").mkdir(parents=True)
    campaign.joinpath("kalshi").mkdir()
    evidence = {
        campaign / "campaign-manifest.json": b'{"fixture":"SYNTHETIC/FIXTURE"}\n',
        campaign / "polymarket" / "raw" / "segment-fixture.rdpseg": b"immutable-raw-fixture",
        campaign / "kalshi" / "ledger.jsonl": b'{"fixture":"immutable-ledger"}\n',
    }
    for path, payload in evidence.items():
        path.write_bytes(payload)
    (incoming / "scripts" / "preflight.py").write_text(
        "# SYNTHETIC/FIXTURE path-presence guard\n", encoding="utf-8"
    )
    (incoming / "scripts" / "systemd_cutover.py").write_text(
        """# SYNTHETIC/FIXTURE only; bounded helper behavior has dedicated unit tests.
import os,sys
with open(os.environ['HYPERLAB_FAKE_LOG'],'a',encoding='utf-8') as handle:
 handle.write('helper|'+' '.join(sys.argv[1:])+'\\n')
print('PREDICTION_SYSTEMD_DISARM_GREEN:SYNTHETIC_FIXTURE')
""",
        encoding="utf-8",
    )
    services = {
        name: f"hyperlab-pm-{suffix}-{name}.service"
        for name in ("polymarket", "kalshi", "dashboard")
    }
    handoff = {
        "boundary": launch_pack.BOUNDARY,
        "campaign_root": campaign.resolve().as_posix(),
        "incoming_root": incoming.resolve().as_posix(),
        "run_slug": slug,
        "schema_version": 1,
        "services": services,
        "source_commit": "b" * 40,
        "source_root": source.resolve().as_posix(),
    }
    handoff_raw = preflight.canonical_json_bytes(handoff) + b"\n"
    (incoming / "handoff.json").write_bytes(handoff_raw)
    (incoming / "handoff.sha256").write_text(
        f"{preflight.sha256_bytes(handoff_raw)}  handoff.json\n",
        encoding="ascii",
    )
    fake_bin = tmp_path / "rollback-fake-bin"
    fake_bin.mkdir()
    log = tmp_path / "rollback-systemd.log"
    _write_fake_command(fake_bin, "id", "[[ ${1:-} == -un ]]\nprintf 'hyperlab\\n'\n")
    _write_fake_command(
        fake_bin,
        "python3.12",
        "\"$HYPERLAB_REAL_PYTHON\" \"$@\" | tr -d '\\r' | sed 's#^C:/#/c/#'\nexit \"${PIPESTATUS[0]}\"\n",
    )
    _write_fake_command(
        fake_bin,
        "systemctl",
        """printf 'systemctl|%s\n' "$*" >> "$HYPERLAB_FAKE_LOG"
if [[ ${1:-} == show && "$*" == *'--property=ActiveState --value'* ]]; then
  printf '%s\n' "${HYPERLAB_ROLLBACK_ACTIVE_STATE:-inactive}"
  exit 0
fi
if [[ ${1:-} == show ]]; then
  active=${HYPERLAB_ROLLBACK_ACTIVE_STATE:-inactive}
  pid=0; [[ $active == active || $active == deactivating ]] && pid=123
  printf 'LoadState=loaded\nActiveState=%s\nSubState=dead\nResult=success\nMainPID=%s\nNRestarts=0\n' "$active" "$pid"
  exit 0
fi
if [[ ${1:-} == is-enabled ]]; then printf 'disabled\n'; exit 1; fi
exit 98
""",
    )
    _write_fake_command(
        fake_bin,
        "sudo",
        """printf 'sudo|%s\n' "$*" >> "$HYPERLAB_FAKE_LOG"
[[ ${1:-} == -n && ${2:-} == timeout ]] || exit 98
exit 0
""",
    )
    bash_environment = tmp_path / "rollback-strict-path.sh"
    bash_environment.write_text(
        f"export PATH='{_git_bash_path(fake_bin)}':\"$PATH\"\n",
        encoding="utf-8",
    )
    production_rollback = (OPS / "rollback.sh").read_text(encoding="utf-8")
    root_substitutions = {
        "/home/hyperlab/hyperlab-prediction-markets/incoming": incoming.parent.resolve().as_posix(),
        "/mnt/HC_Volume_106716684/hyperlab-prediction-markets/sources": source.parent.resolve().as_posix(),
        "/mnt/HC_Volume_106716684/hyperlab-prediction-markets/campaigns": campaign.parent.resolve().as_posix(),
    }
    materialized_rollback = production_rollback
    for production_root, fixture_root in root_substitutions.items():
        assert materialized_rollback.count(production_root) == 1
        materialized_rollback = materialized_rollback.replace(
            production_root,
            fixture_root,
        )
    materialized_rollback = materialized_rollback.replace(
        "if str(incoming)!=expected_incoming or d.get('incoming_root')!=expected_incoming",
        "if incoming.as_posix()!=expected_incoming or d.get('incoming_root')!=expected_incoming",
    )
    rollback_script = tmp_path / "rollback-materialized-windows-harness.sh"
    rollback_script.write_text(materialized_rollback, encoding="utf-8")
    harness = tmp_path / "run-real-rollback.sh"
    harness.write_text(
        f"""#!/usr/bin/bash
set -Eeuo pipefail
/usr/bin/bash '{_git_bash_path(rollback_script)}' rollback '{_git_bash_path(incoming / 'handoff.json')}'
""",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment.update(
        {
            "BASH_ENV": _git_bash_path(bash_environment),
            "HOME": "/home/hyperlab",
            "HYPERLAB_FAKE_LOG": _git_bash_path(log),
            "HYPERLAB_REAL_PYTHON": _git_bash_path(Path(sys.executable)),
            "PATH": str(fake_bin) + os.pathsep + environment["PATH"],
        }
    )
    non_git_cwd = tmp_path / "rollback-non-git-cwd"
    non_git_cwd.mkdir()
    before = {path: path.read_bytes() for path in evidence}
    completed = _run_git_bash_script(
        harness,
        cwd=non_git_cwd,
        environment=environment,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines()[-1] == (
        "PREDICTION_ROLLBACK_DISARMED_RAW_PRESERVED"
    )
    assert {path: path.read_bytes() for path in evidence} == before
    systemd_log = log.read_text(encoding="utf-8")
    rollback_services = [
        *services.values(),
        f"hyperlab-pm-{suffix}-polymarket-namespace-probe.service",
        f"hyperlab-pm-{suffix}-kalshi-namespace-probe.service",
    ]
    for service in rollback_services:
        assert (
            f"systemctl|show {service} --property=ActiveState --value --no-pager"
            in systemd_log
        )
        assert f"systemctl|is-enabled {service}" in systemd_log
    assert "hyperlab-h1" not in systemd_log
    assert not any(line.startswith("rm|") for line in systemd_log.splitlines())

    environment["HYPERLAB_ROLLBACK_ACTIVE_STATE"] = "deactivating"
    refused = _run_git_bash_script(
        harness,
        cwd=non_git_cwd,
        environment=environment,
    )
    assert refused.returncode == 4
    assert "PREDICTION_ROLLBACK_SERVICE_NOT_DISARMED=" in refused.stderr
    assert "PREDICTION_ROLLBACK_DISARMED_RAW_PRESERVED" not in refused.stdout
    assert {path: path.read_bytes() for path in evidence} == before


def test_internal_recovery_runs_real_control_flow_and_isolates_start_failure(
    tmp_path: Path,
) -> None:
    class ReadyHandler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: object) -> None:
            del args

        def do_GET(self) -> None:
            if self.path not in {"/health/live", "/health/ready"}:
                self.send_error(404)
                return
            value = {
                "mode": "readonly",
                "orders_enabled": False,
                "status": "alive" if self.path.endswith("live") else "ready",
            }
            payload = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    try:
        server = ThreadingHTTPServer(("127.0.0.1", 18081), ReadyHandler)
    except OSError as error:
        pytest.skip(f"loopback port 18081 unavailable for recovery harness: {error}")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    pack = (
        ROOT
        / "ops"
        / "prediction_markets_candidate_v1"
        / "prediction-markets-v1-20260901t000000z-aa60c0ff"
    )

    def run_case(
        case_name: str,
        slug: str,
        *,
        fail_polymarket: bool,
        clean_exit_polymarket: bool = False,
        retry_after_partial: bool = False,
        monitor_mode: str | None = None,
    ):
        case_root = tmp_path / case_name
        incoming = case_root / "incoming" / slug
        source = case_root / "sources" / slug
        campaign = case_root / "campaigns" / slug
        incoming.joinpath("scripts").mkdir(parents=True)
        source.joinpath(".venv", "bin").mkdir(parents=True)
        for venue in ("polymarket", "kalshi"):
            campaign.joinpath(venue).mkdir(parents=True)
            (campaign / venue / "state.json").write_bytes(
                preflight.canonical_json_bytes(
                    {"lifecycle": "INTERRUPTED_RECOVERABLE"}
                )
                + b"\n"
            )
        evidence_path = campaign / "polymarket" / "raw-preserved.rdpseg"
        evidence_path.write_bytes(b"SYNTHETIC/FIXTURE immutable recovery evidence")
        (campaign / "campaign-manifest.json").write_bytes(
            (pack / "campaign-manifest.json").read_bytes()
        )
        (incoming / "scripts" / "preflight.py").write_text(
            "# SYNTHETIC/FIXTURE external-boundary stub target\n",
            encoding="utf-8",
        )
        (incoming / "scripts" / "systemd_cutover.py").write_text(
            """# SYNTHETIC/FIXTURE only; helper semantics have dedicated unit tests.
import os,sys
from pathlib import Path
args=sys.argv[1:]
command=args[0]; service=args[args.index('--service')+1]
state=Path(os.environ['HYPERLAB_SERVICE_STATE_DIR'])
with open(os.environ['HYPERLAB_FAKE_LOG'],'a',encoding='utf-8') as handle:
 handle.write('helper|'+' '.join(args)+'\\n')
if command=='disarm':
 for suffix in ('','.failed','.probe-success','.probe-failed'):
  (state/(service+suffix)).unlink(missing_ok=True)
elif command=='ensure-active':
 if os.environ.get('HYPERLAB_FAIL_SERVICE')==service: raise SystemExit(4)
 if os.environ.get('HYPERLAB_TERMINAL_EXIT_SERVICE')==service:
  (state/(service+'.failed')).touch()
 elif os.environ.get('HYPERLAB_CLEAN_EXIT_SERVICE')!=service:
  (state/service).touch()
else: raise SystemExit(97)
print('PREDICTION_SYSTEMD_HELPER_GREEN:SYNTHETIC_FIXTURE')
""",
            encoding="utf-8",
        )
        suffix = slug.removeprefix("pm-")
        services = {
            name: f"hyperlab-pm-{suffix}-{name}.service"
            for name in ("polymarket", "kalshi", "dashboard")
        }
        handoff = {
            "boundary": launch_pack.BOUNDARY,
            "campaign_root": campaign.resolve().as_posix(),
            "incoming_root": incoming.resolve().as_posix(),
            "run_slug": slug,
            "schema_version": 1,
            "services": services,
            "source_commit": "b" * 40,
            "source_root": source.resolve().as_posix(),
        }
        handoff_raw = preflight.canonical_json_bytes(handoff) + b"\n"
        (incoming / "handoff.json").write_bytes(handoff_raw)
        (incoming / "handoff.sha256").write_text(
            f"{preflight.sha256_bytes(handoff_raw)}  handoff.json\n",
            encoding="ascii",
        )
        runtime = source / ".venv" / "bin" / "python"
        runtime.write_text(
            """#!/usr/bin/bash
set -Eeuo pipefail
"$HYPERLAB_REAL_PYTHON" "$@" | tr -d '\r'
exit "${PIPESTATUS[0]}"
""",
            encoding="utf-8",
        )
        runtime.chmod(0o700)
        environment, systemd_log_path = _internal_install_environment(
            case_root,
            handoff=handoff,
            pack=pack,
            failed_service=(
                str(services["polymarket"])
                if fail_polymarket
                else None
            ),
            clean_exit_service=(
                str(services["polymarket"])
                if clean_exit_polymarket
                else None
            ),
            monitor_failure=monitor_mode,
        )
        preflight_helper = case_root / "recovery-preflight-fixture.py"
        preflight_helper.write_text(
            """import json,sys
args=sys.argv[1:]
command=args[0]
def option(name): return args[args.index(name)+1]
if command=='resume':
 value={'resume_admissible':True,'terminal_signal':'PREDICTION_RESUME_PREFLIGHT_GREEN'}
elif command=='recovery-admission':
 value={'admission_by_venue':{venue:{'eligible':True,'network_verdict':'NETWORK_PREFLIGHT_GREEN'} for venue in ('polymarket','kalshi')},'terminal_signal':'PREDICTION_RECOVERY_INITIAL_ADMISSION_AUTHENTICATED'}
elif command=='network':
 venue=option('--venue'); value={'errors':[],'venue':venue,'verdict':'NETWORK_PREFLIGHT_GREEN'}
else:
 raise SystemExit(97)
payload=json.dumps(value,ensure_ascii=False,allow_nan=False,separators=(',',':'),sort_keys=True).encode()+b'\\n'
open(option('--report'),'xb').write(payload)
print(payload[:-1].decode())
""",
            encoding="utf-8",
        )
        fake_python = case_root / "internal-fake-bin" / "python3.12"
        _write_fake_command(
            fake_python.parent,
            fake_python.name,
            """if [[ ${1:-} == -I && ${2:-} == */preflight.py ]]; then
  shift 2
  exec "$HYPERLAB_REAL_PYTHON" "$HYPERLAB_PREFLIGHT_HELPER" "$@"
fi
"$HYPERLAB_REAL_PYTHON" "$@" | tr -d '\r' | sed 's#^C:/#/c/#'
exit "${PIPESTATUS[0]}"
""",
        )
        production = (OPS / "rollback.sh").read_text(encoding="utf-8")
        substitutions = {
            "/home/hyperlab/hyperlab-prediction-markets/incoming": incoming.parent.resolve().as_posix(),
            "/mnt/HC_Volume_106716684/hyperlab-prediction-markets/sources": source.parent.resolve().as_posix(),
            "/mnt/HC_Volume_106716684/hyperlab-prediction-markets/campaigns": campaign.parent.resolve().as_posix(),
        }
        materialized = production
        for production_root, fixture_root in substitutions.items():
            assert materialized.count(production_root) == 1
            materialized = materialized.replace(production_root, fixture_root)
        materialized = materialized.replace(
            "if str(incoming)!=expected_incoming or d.get('incoming_root')!=expected_incoming",
            "if incoming.as_posix()!=expected_incoming or d.get('incoming_root')!=expected_incoming",
        )
        rollback_script = case_root / "rollback-recovery-materialized.sh"
        rollback_script.write_text(materialized, encoding="utf-8")
        environment["HYPERLAB_PREFLIGHT_HELPER"] = _git_bash_path(preflight_helper)
        non_git_cwd = case_root / "non-git-cwd"
        non_git_cwd.mkdir()
        before = evidence_path.read_bytes()
        completed = _run_git_bash_script(
            rollback_script,
            cwd=non_git_cwd,
            environment=environment,
            arguments=("recovery", _git_bash_path(incoming / "handoff.json")),
        )
        retried = None
        if retry_after_partial:
            assert completed.returncode == 4, completed.stderr
            environment["HYPERLAB_FAIL_SERVICE"] = ""
            retried = _run_git_bash_script(
                rollback_script,
                cwd=non_git_cwd,
                environment=environment,
                arguments=("recovery", _git_bash_path(incoming / "handoff.json")),
            )
        assert evidence_path.read_bytes() == before
        systemd_log = (
            systemd_log_path.read_text(encoding="utf-8")
            if systemd_log_path.exists()
            else ""
        )
        return completed, retried, systemd_log, services

    try:
        green, green_retry, green_log, green_services = run_case(
            "green",
            "pm-20260827t235950z-cafebabe",
            fail_polymarket=False,
        )
        assert green_retry is None
        assert green.returncode == 0, green.stderr
        assert green.stdout.splitlines()[-1] == (
            "PREDICTION_RECOVERY_RESUME_REQUESTED_NO_SLOT_RETRY"
        )
        for service in green_services.values():
            assert f"helper|ensure-active --service {service}" in green_log
        assert "prediction-collect" not in green_log

        partial, retried, partial_log, partial_services = run_case(
            "partial",
            "pm-20260827t235951z-feedface",
            fail_polymarket=True,
            retry_after_partial=True,
        )
        assert partial.returncode == 4
        assert partial.stderr.splitlines()[-1] == (
            "PREDICTION_RECOVERY_PARTIAL_OR_ALERT_NO_SLOT_RETRY"
        )
        assert f"helper|disarm --service {partial_services['polymarket']}" in partial_log
        assert f"helper|disarm --service {partial_services['kalshi']}" not in partial_log
        assert f"helper|disarm --service {partial_services['dashboard']}" not in partial_log
        assert retried is not None and retried.returncode == 0, (
            None if retried is None else retried.stderr
        )
        assert retried.stdout.splitlines()[-1] == (
            "PREDICTION_RECOVERY_RESUME_REQUESTED_NO_SLOT_RETRY"
        )
        assert partial_log.count(
            f"helper|ensure-active --service {partial_services['kalshi']}"
        ) == 2

        for case_name, slug, monitor_mode in (
            (
                "dashboard-fragment",
                "pm-20260827t235952z-deadbeef",
                "dashboard-fragment",
            ),
            (
                "dashboard-listener",
                "pm-20260827t235953z-abcd1234",
                "dashboard-listener",
            ),
        ):
            refused, refused_retry, refused_log, refused_services = run_case(
                case_name,
                slug,
                fail_polymarket=False,
                monitor_mode=monitor_mode,
            )
            assert refused_retry is None
            assert refused.returncode == 4
            assert "PREDICTION_RECOVERY_RESUME_REQUESTED_NO_SLOT_RETRY" not in (
                refused.stdout + refused.stderr
            )
            assert "dashboard recovery readiness failed before any venue start" in (
                refused.stderr
            )
            dashboard = refused_services["dashboard"]
            assert f"helper|disarm --service {dashboard}" in refused_log
            for venue in ("polymarket", "kalshi"):
                assert (
                    f"helper|ensure-active --service {refused_services[venue]}"
                    not in refused_log
                )

        fragment, _, fragment_log, fragment_services = run_case(
            "collector-fragment",
            "pm-20260827t235954z-0ddba11a",
            fail_polymarket=False,
            monitor_mode="collector-fragment",
        )
        assert fragment.returncode == 4
        assert "PREDICTION_RECOVERY_PARTIAL_OR_ALERT_NO_SLOT_RETRY" in fragment.stderr
        assert "PREDICTION_RECOVERY_RESUME_REQUESTED_NO_SLOT_RETRY" not in (
            fragment.stdout + fragment.stderr
        )
        assert f"helper|disarm --service {fragment_services['polymarket']}" in fragment_log

        clean_exit, _, clean_exit_log, clean_exit_services = run_case(
            "collector-clean-exit-before-state",
            "pm-20260827t235957z-0c1ea000",
            fail_polymarket=False,
            clean_exit_polymarket=True,
        )
        assert clean_exit.returncode == 4
        assert "PREDICTION_RECOVERY_SERVICE_TERMINAL=polymarket:" in clean_exit.stderr
        assert "ActiveState=inactive SubState=dead" in clean_exit.stderr
        assert "ExecMainStatus=0 MainPID=0" in clean_exit.stderr
        assert "PREDICTION_RECOVERY_PARTIAL_OR_ALERT_NO_SLOT_RETRY" in clean_exit.stderr
        assert f"helper|disarm --service {clean_exit_services['polymarket']}" in clean_exit_log
        assert "sleep|0.5" not in clean_exit_log

        final_failure, _, final_failure_log, final_failure_services = run_case(
            "final-operational-failure",
            "pm-20260827t235955z-f00dcafe",
            fail_polymarket=False,
            monitor_mode="final-operational-failure",
        )
        assert final_failure.returncode == 4
        assert "PREDICTION_RECOVERY_PARTIAL_OR_ALERT_NO_SLOT_RETRY" in (
            final_failure.stderr
        )
        assert "PREDICTION_RECOVERY_RESUME_REQUESTED_NO_SLOT_RETRY" not in (
            final_failure.stdout + final_failure.stderr
        )
        for venue in ("polymarket", "kalshi"):
            assert (
                f"helper|ensure-active --service {final_failure_services[venue]}"
                in final_failure_log
            )

        public_invalid, _, public_invalid_log, public_invalid_services = run_case(
            "public-source-invalid",
            "pm-20260827t235956z-c001d00d",
            fail_polymarket=False,
            monitor_mode="public-source-invalid-alert",
        )
        assert public_invalid.returncode == 0, public_invalid.stderr
        assert public_invalid.stdout.count(
            "PREDICTION_RECOVERY_RESUME_REQUESTED_NO_SLOT_RETRY"
        ) == 1
        assert "PREDICTION_RECOVERY_PARTIAL_OR_ALERT_NO_SLOT_RETRY" not in (
            public_invalid.stdout + public_invalid.stderr
        )
        for service in public_invalid_services.values():
            assert f"helper|ensure-active --service {service}" in public_invalid_log
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_materialized_monitor_shell_uses_attested_venv_and_never_masks_primary_error(
    tmp_path: Path,
) -> None:
    source = (tmp_path / "source").resolve()
    incoming = (tmp_path / "incoming").resolve()
    campaign = (tmp_path / "campaign").resolve()
    shutil.copytree(ROOT / "src" / "hyperlab", source / "src" / "hyperlab")
    shutil.copytree(OPS, source / "ops" / "prediction_markets_launch_v1")
    runtime = source / ".venv" / "bin" / "python"
    runtime.parent.mkdir(parents=True)
    injector = tmp_path / "monitor-injector.py"
    injector.write_text(
        r'''import sys
source=sys.stdin.read()
fake=r"""
_synthetic_mode=os.environ.get('HYPERLAB_MONITOR_PROBE_MODE','green')
class _SyntheticSystemctlResult:
 def __init__(self,name):
  active=name=='dashboard'; pid='703' if active else '0'
  if _synthetic_mode=='pid-diverged' and name=='dashboard': pid='0'
  fragment=f'/etc/systemd/system/{services[name]}'
  if _synthetic_mode=='fragment-diverged' and name=='dashboard': fragment='/etc/systemd/system/foreign.service'
  restarts='1' if _synthetic_mode=='restart-diverged' and name=='dashboard' else '0'
  self.returncode=0; self.stderr=''
  self.stdout=f'LoadState=loaded\nActiveState={"active" if active else "inactive"}\nSubState={"running" if active else "dead"}\nMainPID={pid}\nNRestarts={restarts}\nExecMainStatus=0\nFragmentPath={fragment}\n'
def _synthetic_systemctl(arguments,**_kwargs):
 name=next(key for key,value in services.items() if value==arguments[2])
 return _SyntheticSystemctlResult(name)
def _synthetic_cmdline(pid):
 assert pid==703
 command=list(expected['dashboard'])
 if _synthetic_mode=='command-diverged': command[-1]='18082'
 return b'\0'.join(part.encode('utf-8') for part in command)+b'\0'
_real_loopback_listener_owned_by_pid=loopback_listener_owned_by_pid
def _synthetic_listener(pid,port):
 if _synthetic_mode in {'proc-green','proc-foreign'}:
  proc_root=Path(os.environ['HYPERLAB_PROC_FIXTURE'])
  inode='4242' if _synthetic_mode=='proc-green' else '9999'
  class _SyntheticLinkMetadata: st_mode=stat.S_IFLNK
  return _real_loopback_listener_owned_by_pid(pid,port,proc_root=proc_root,readlink=lambda _path:f'socket:[{inode}]',lstat=lambda _path:_SyntheticLinkMetadata())
 return _synthetic_mode!='listener-diverged' and pid==703 and port==18081
subprocess.run=_synthetic_systemctl
bounded_proc_cmdline=_synthetic_cmdline
loopback_listener_owned_by_pid=_synthetic_listener
"""
marker="result={'alert':preflight_error is not None,"
if source.count(marker)!=1: raise SystemExit('monitor injection marker diverged')
source=source.replace(marker,fake+'\n'+marker,1)
exec(compile(source,'materialized-monitor-embedded','exec'),{'__name__':'__main__'})
''',
        encoding="utf-8",
    )
    runtime.write_text(
        """#!/usr/bin/bash
set -Eeuo pipefail
printf 'venv|%s\n' "$*" >> "$HYPERLAB_RUNTIME_LOG"
if [[ ${1:-} == -I && ${2:-} == */preflight.py && ${3:-} == runtime-import-admission ]]; then
  printf 'PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN\n'
  exit 0
fi
[[ ${1:-} == -I && ${2:-} == - ]]
shift 2
exec "$HYPERLAB_REAL_PYTHON" "$HYPERLAB_MONITOR_INJECTOR" "$@"
""",
        encoding="utf-8",
    )
    runtime.chmod(0o700)
    incoming.mkdir()
    campaign.joinpath("state").mkdir(parents=True)
    candidate_pack = (
        ROOT
        / "ops"
        / "prediction_markets_candidate_v1"
        / "prediction-markets-v1-20260901t000000z-aa60c0ff"
    )
    for name in ("campaign-manifest.json", "campaign-manifest.sha256"):
        (campaign / name).write_bytes((candidate_pack / name).read_bytes())
    manifest = json.loads((campaign / "campaign-manifest.json").read_bytes())
    preflight_value = {
        "boundary": cockpit.BOUNDARY,
        "eligible_venues": ["polymarket", "kalshi"],
        "errors": [],
        "host_admitted": True,
        "installation_admissible": True,
        "network": {
            venue: {"venue": venue, "verdict": "NETWORK_PREFLIGHT_GREEN"}
            for venue in ("polymarket", "kalshi")
        },
        "schema_version": 1,
        "terminal_signal": "PREDICTION_HOST_PREFLIGHT_GREEN",
    }
    preflight_raw = runner.canonical_json_bytes(preflight_value) + b"\n"
    (campaign / "state" / "preflight-report.json").write_bytes(preflight_raw)
    activation_body = {
        "boundary": cockpit.BOUNDARY,
        "campaign_id": manifest["campaign_id"],
        "campaign_manifest_sha256": manifest["manifest_sha256"],
        "campaign_root": str(campaign),
        "dashboard_port": 18081,
        "economic_evidence_status": cockpit.ECONOMIC_STATUS,
        "eligible_venues": ["polymarket", "kalshi"],
        "h1_actions": "NONE",
        "preflight_report_sha256": runner.sha256_bytes(preflight_raw),
        "quick_start": True,
        "recorded_at_utc": "2026-08-27T18:49:10.000000Z",
        "schema_version": 1,
        "source_commit": "b" * 40,
        "starts_at_utc": manifest["starts_at_utc"],
    }
    activation = {
        **activation_body,
        "receipt_sha256": runner.sha256_bytes(
            runner.canonical_json_bytes(activation_body)
        ),
    }
    activation_path = campaign / "state" / "activation-receipt.json"
    activation_raw = runner.canonical_json_bytes(activation) + b"\n"
    activation_path.write_bytes(activation_raw)
    services = {
        name: f"hyperlab-pm-20260827t190000z-cafefeed-{name}.service"
        for name in ("polymarket", "kalshi", "dashboard")
    }
    handoff = {
        "boundary": cockpit.BOUNDARY,
        "campaign_root": str(campaign),
        "dashboard_port": 18081,
        "incoming_root": str(incoming),
        "services": services,
        "source_commit": "b" * 40,
        "source_root": str(source),
    }
    handoff_raw = runner.canonical_json_bytes(handoff) + b"\n"
    handoff_path = incoming / "handoff.json"
    handoff_path.write_bytes(handoff_raw)
    (incoming / "handoff.sha256").write_text(
        f"{runner.sha256_bytes(handoff_raw)}  handoff.json\n",
        encoding="ascii",
    )
    bare_import = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            (
                "import sys;"
                f"sys.path[:0]=[{str(source / 'src')!r},{str(source)!r}];"
                "import ops.prediction_markets_launch_v1.cockpit"
            ),
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    assert bare_import.returncode != 0
    assert "No module named 'numpy'" in bare_import.stderr
    fake_bin = tmp_path / "monitor-fake-bin"
    fake_bin.mkdir()
    runtime_log = tmp_path / "runtime.log"
    _write_fake_command(
        fake_bin,
        "python3.12",
        "printf 'system-python|%s\\n' \"$*\" >> \"$HYPERLAB_RUNTIME_LOG\"\nexit 91\n",
    )
    environment = os.environ.copy()
    environment.update(
        {
            "HYPERLAB_MONITOR_INJECTOR": _git_bash_path(injector),
            "HYPERLAB_REAL_PYTHON": _git_bash_path(Path(sys.executable)),
            "HYPERLAB_RUNTIME_LOG": _git_bash_path(runtime_log),
            "PATH": str(fake_bin) + os.pathsep + environment["PATH"],
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    non_git_cwd = tmp_path / "non-git-cwd"
    non_git_cwd.mkdir()

    proc_fixture = tmp_path / "proc-fixture"
    proc_fixture.joinpath("net").mkdir(parents=True)
    proc_fixture.joinpath("703", "fd").mkdir(parents=True)
    proc_fixture.joinpath("703", "fd", "9").write_text(
        "SYNTHETIC/FIXTURE descriptor placeholder\n", encoding="utf-8"
    )
    proc_fixture.joinpath("net", "tcp").write_text(
        "  sl  local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"
        "0: 0100007F:46A1 00000000:0000 0A 00000000:00000000 00:00000000 00000000 1000 0 4242\n",
        encoding="ascii",
    )
    environment["HYPERLAB_PROC_FIXTURE"] = str(proc_fixture)

    def invoke(
        mode: str = "green",
    ) -> tuple[subprocess.CompletedProcess[str], dict[str, object]]:
        environment["HYPERLAB_MONITOR_PROBE_MODE"] = mode
        completed = _run_git_bash_script(
            source / "ops" / "prediction_markets_launch_v1" / "monitor.sh",
            cwd=non_git_cwd,
            environment=environment,
            arguments=(_git_bash_path(handoff_path), "dashboard-only"),
        )
        assert completed.returncode == 0, completed.stderr
        return completed, json.loads(completed.stdout)

    completed, green = invoke()
    assert green["preflight_error"] is None
    assert green["activation_admissible"] is True
    assert green["services"]["dashboard"]["listener_verified"] is True  # type: ignore[index]
    assert "NameError" not in completed.stderr
    assert "system-python" not in runtime_log.read_text(encoding="utf-8")
    assert "runtime-import-admission" in runtime_log.read_text(encoding="utf-8")
    assert "venv|-I -" in runtime_log.read_text(encoding="utf-8")

    proc_green_completed, proc_green = invoke("proc-green")
    assert proc_green_completed.returncode == 0
    assert proc_green["activation_admissible"] is True, json.dumps(
        proc_green["services"]["dashboard"],  # type: ignore[index]
        sort_keys=True,
    )
    assert proc_green["services"]["dashboard"]["listener_verified"] is True  # type: ignore[index]

    for mode, field in (
        ("pid-diverged", "command_verified"),
        ("command-diverged", "command_verified"),
        ("fragment-diverged", "fragment_verified"),
        ("listener-diverged", "listener_verified"),
        ("proc-foreign", "listener_verified"),
        ("restart-diverged", "restarts_verified"),
    ):
        refused_completed, refused = invoke(mode)
        assert refused_completed.returncode == 0
        assert refused["activation_admissible"] is False
        assert refused["operational_failure"] is True
        assert refused["services"]["dashboard"][field] is False  # type: ignore[index]
        assert "NameError" not in refused_completed.stderr + refused_completed.stdout

    campaign_alias = tmp_path / "campaign-linked"
    try:
        os.symlink(campaign, campaign_alias, target_is_directory=True)
    except OSError:
        linked = subprocess.run(
            [
                os.environ.get("COMSPEC", r"C:\Windows\System32\cmd.exe"),
                "/d",
                "/c",
                "mklink",
                "/J",
                str(campaign_alias),
                str(campaign),
            ],
            capture_output=True,
            check=False,
            text=True,
            timeout=30,
        )
        assert linked.returncode == 0, linked.stderr + linked.stdout
    linked_activation_body = {
        key: value for key, value in activation.items() if key != "receipt_sha256"
    }
    linked_activation_body["campaign_root"] = str(campaign_alias)
    linked_activation = {
        **linked_activation_body,
        "receipt_sha256": runner.sha256_bytes(
            runner.canonical_json_bytes(linked_activation_body)
        ),
    }
    activation_path.write_bytes(runner.canonical_json_bytes(linked_activation) + b"\n")
    linked_handoff = {**handoff, "campaign_root": str(campaign_alias)}
    linked_handoff_raw = runner.canonical_json_bytes(linked_handoff) + b"\n"
    handoff_path.write_bytes(linked_handoff_raw)
    (incoming / "handoff.sha256").write_text(
        f"{runner.sha256_bytes(linked_handoff_raw)}  handoff.json\n",
        encoding="ascii",
    )
    linked_completed, linked_failure = invoke()
    assert linked_failure["failure_class"] == "MONITOR_INITIAL_HANDOFF_INVALID"
    assert linked_failure["activation_admissible"] is False
    assert linked_failure["operational_failure"] is True
    assert "symlinked, special, or non-canonical" in str(
        linked_failure["preflight_error"]
    )
    assert "NameError" not in linked_completed.stderr + linked_completed.stdout
    activation_path.write_bytes(activation_raw)
    handoff_path.write_bytes(handoff_raw)
    (incoming / "handoff.sha256").write_text(
        f"{runner.sha256_bytes(handoff_raw)}  handoff.json\n",
        encoding="ascii",
    )

    cockpit_path = source / "ops" / "prediction_markets_launch_v1" / "cockpit.py"
    cockpit_raw = cockpit_path.read_bytes()
    cockpit_path.write_text(
        cockpit_raw.decode("utf-8").replace(
            "def prepared_state_is_stale(",
            "def prepared_state_is_stale_REMOVED_FOR_SYNTHETIC_REGRESSION(",
            1,
        ),
        encoding="utf-8",
    )
    helper_completed, helper_failure = invoke()
    assert helper_failure["failure_class"] == "MONITOR_RUNTIME_IMPORT_FAILED"
    assert "prepared_state_is_stale" in str(helper_failure["preflight_error"])
    assert helper_failure["operational_failure"] is True
    assert "NameError" not in helper_completed.stderr + helper_completed.stdout
    cockpit_path.write_bytes(cockpit_raw)

    corrupted = json.loads(activation_raw)
    corrupted["campaign_manifest_sha256"] = "f" * 64
    activation_path.write_bytes(runner.canonical_json_bytes(corrupted) + b"\n")
    evidence_completed, evidence_failure = invoke()
    assert evidence_failure["failure_class"] == "INITIAL_EVIDENCE_INVALID"
    assert "binding diverged" in str(evidence_failure["preflight_error"])
    assert evidence_failure["activation_admissible"] is False
    assert "NameError" not in evidence_completed.stderr + evidence_completed.stdout
    activation_path.write_bytes(activation_raw)

    runtime_backup = runtime.with_name("python.synthetic-backup")
    runtime.rename(runtime_backup)
    bootstrap = _run_git_bash_script(
        source / "ops" / "prediction_markets_launch_v1" / "monitor.sh",
        cwd=non_git_cwd,
        environment=environment,
        arguments=(_git_bash_path(handoff_path), "dashboard-only"),
    )
    bootstrap_value = json.loads(bootstrap.stdout)
    assert bootstrap.returncode == 0
    assert bootstrap_value["failure_class"] == "MONITOR_BOOTSTRAP_FAILED"
    assert bootstrap_value["alert"] is True
    assert "NameError" not in bootstrap.stderr + bootstrap.stdout


def test_materialized_monitor_authenticates_activation_and_rejects_rechained_ledger(
    tmp_path: Path,
) -> None:
    campaign = (tmp_path / "campaign").resolve()
    incoming = (tmp_path / "incoming").resolve()
    campaign.joinpath("state").mkdir(parents=True)
    incoming.mkdir()
    pack = (
        ROOT
        / "ops"
        / "prediction_markets_candidate_v1"
        / "prediction-markets-v1-20260901t000000z-aa60c0ff"
    )
    for name in ("campaign-manifest.json", "campaign-manifest.sha256"):
        (campaign / name).write_bytes((pack / name).read_bytes())
    manifest = json.loads((campaign / "campaign-manifest.json").read_bytes())
    preflight_value = {
        "boundary": cockpit.BOUNDARY,
        "eligible_venues": ["polymarket", "kalshi"],
        "errors": [],
        "host_admitted": True,
        "installation_admissible": True,
        "network": {
            venue: {
                "errors": ["SYNTHETIC/FIXTURE unavailable"],
                "venue": venue,
                "verdict": "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT",
            }
            for venue in ("polymarket", "kalshi")
        },
        "schema_version": 1,
        "terminal_signal": "PREDICTION_HOST_PREFLIGHT_GREEN",
    }
    preflight_raw = runner.canonical_json_bytes(preflight_value) + b"\n"
    (campaign / "state" / "preflight-report.json").write_bytes(preflight_raw)
    activation_body = {
        "boundary": cockpit.BOUNDARY,
        "campaign_id": manifest["campaign_id"],
        "campaign_manifest_sha256": manifest["manifest_sha256"],
        "campaign_root": str(campaign),
        "dashboard_port": 18081,
        "economic_evidence_status": cockpit.ECONOMIC_STATUS,
        "eligible_venues": ["polymarket", "kalshi"],
        "h1_actions": "NONE",
        "preflight_report_sha256": runner.sha256_bytes(preflight_raw),
        "quick_start": True,
        "recorded_at_utc": "2026-09-01T00:00:00.000000Z",
        "schema_version": 1,
        "source_commit": "b" * 40,
        "starts_at_utc": manifest["starts_at_utc"],
    }
    activation = {
        **activation_body,
        "receipt_sha256": runner.sha256_bytes(
            runner.canonical_json_bytes(activation_body)
        ),
    }
    (campaign / "state" / "activation-receipt.json").write_bytes(
        runner.canonical_json_bytes(activation) + b"\n"
    )
    corrupted_ledger = campaign / "polymarket" / "ledger.jsonl"
    corrupted_ledger.parent.mkdir()
    corrupted_ledger.write_bytes(b'{"entry_sha256":"' + b"0" * 64 + b'","ordinal":0}\n')
    services = {
        name: f"hyperlab-pm-monitor-fixture-{name}.service"
        for name in ("polymarket", "kalshi", "dashboard")
    }
    handoff = {
        "boundary": cockpit.BOUNDARY,
        "campaign_root": str(campaign),
        "dashboard_port": 18081,
        "incoming_root": str(incoming),
        "services": services,
        "source_commit": "b" * 40,
        "source_root": str(ROOT.resolve()),
    }
    handoff_raw = runner.canonical_json_bytes(handoff) + b"\n"
    handoff_path = incoming / "handoff.json"
    handoff_path.write_bytes(handoff_raw)
    (incoming / "handoff.sha256").write_text(
        f"{runner.sha256_bytes(handoff_raw)}  handoff.json\n",
        encoding="ascii",
    )
    production = (OPS / "monitor.sh").read_text(encoding="utf-8")
    marker = '"$VENV_PYTHON" -I - "$HANDOFF" "$MODE" "$SOURCE_ROOT" <<\'PY\'\n'
    assert production.count(marker) == 1
    embedded = production.split(marker, maxsplit=1)[1].rsplit("\nPY", maxsplit=1)[0]
    strict_external_fake = """
class _SyntheticSystemctlResult:
 returncode=0
 stderr=''
 stdout='LoadState=loaded\\nActiveState=inactive\\nSubState=dead\\nMainPID=0\\nNRestarts=0\\nExecMainStatus=0\\nFragmentPath=\\n'
def _synthetic_systemctl(arguments,**_kwargs):
 assert arguments[0]=='systemctl' and arguments[1]=='show'
 return _SyntheticSystemctlResult()
subprocess.run=_synthetic_systemctl
"""
    embedded = embedded.replace(
        "from pathlib import Path\n",
        "from pathlib import Path\n" + strict_external_fake,
        1,
    )
    monitor_python = tmp_path / "materialized-monitor.py"
    monitor_python.write_text(embedded, encoding="utf-8")
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            str(monitor_python),
            str(handoff_path),
            "full",
            str(ROOT.resolve()),
        ],
        capture_output=True,
        check=False,
        cwd=tmp_path,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    value = json.loads(completed.stdout)
    polymarket = value["services"]["polymarket"]
    assert value["alert"] is True
    assert value["activation_admissible"] is False
    assert polymarket["venue_status"] == "INTEGRITY_FAILED"
    assert "ledger" in polymarket["ledger_error"].lower()

    corrupted_ledger.unlink()
    old_start = "2026-01-01T00:00:00.000000Z"
    manifest_body = {
        **{key: item for key, item in manifest.items() if key != "manifest_sha256"},
        "starts_at_utc": old_start,
    }
    manifest = {
        **manifest_body,
        "manifest_sha256": runner.sha256_bytes(
            runner.canonical_json_bytes(manifest_body)
        ),
    }
    manifest_raw = runner.canonical_json_bytes(manifest) + b"\n"
    (campaign / "campaign-manifest.json").write_bytes(manifest_raw)
    (campaign / "campaign-manifest.sha256").write_text(
        f"{runner.sha256_bytes(manifest_raw)}  campaign-manifest.json\n",
        encoding="ascii",
    )
    activation_body.update(
        {
            "campaign_manifest_sha256": manifest["manifest_sha256"],
            "eligible_venues": ["polymarket", "kalshi"],
            "starts_at_utc": old_start,
        }
    )
    activation = {
        **activation_body,
        "receipt_sha256": runner.sha256_bytes(
            runner.canonical_json_bytes(activation_body)
        ),
    }
    (campaign / "state" / "activation-receipt.json").write_bytes(
        runner.canonical_json_bytes(activation) + b"\n"
    )
    required = 144 * 1024**3 + 21 * 1024**3 + 16 * 1024**3
    for venue in ("polymarket", "kalshi"):
        state = {
            "active_ordinal": None,
            "boundary": cockpit.BOUNDARY,
            "campaign_id": manifest["campaign_id"],
            "capacity": {
                "admitted": True,
                "available_bytes": required,
                "h1_reserved_bytes": 144 * 1024**3,
                "prediction_remaining_bytes": 21 * 1024**3,
                "required_free_bytes": required,
                "safety_margin_bytes": 16 * 1024**3,
            },
            "data_quality": None,
            "economic_evidence_status": cockpit.ECONOMIC_STATUS,
            "error": None,
            "expected_slots": manifest["prospective_shard_policy"][
                "expected_shards_per_venue"
            ],
            "holdout": {"access": "SEALED", "metrics_exposed": False},
            "last_terminal": None,
            "lifecycle": "PREPARED",
            "recorded_slots": 0,
            "schema_version": 1,
            "updated_at_utc": "2026-01-01T00:00:00.000000Z",
            "venue": venue,
        }
        venue_root = campaign / venue
        venue_root.mkdir(exist_ok=True)
        (venue_root / "state.json").write_bytes(
            runner.canonical_json_bytes(state) + b"\n"
        )

    active_fakes = r'''
class _SyntheticActiveResult:
 def __init__(self,name):
  pid={'polymarket':'101','kalshi':'102','dashboard':'103'}[name]
  self.returncode=0
  self.stderr=''
  self.stdout=f'LoadState=loaded\nActiveState=active\nSubState=running\nMainPID={pid}\nNRestarts=0\nExecMainStatus=0\nFragmentPath=/etc/systemd/system/{services[name]}\n'
def _synthetic_active_systemctl(arguments,**_kwargs):
 assert arguments[0]=='systemctl' and arguments[1]=='show'
 name=next(key for key,value in services.items() if value==arguments[2])
 return _SyntheticActiveResult(name)
def _synthetic_active_cmdline(pid):
 name={101:'polymarket',102:'kalshi',103:'dashboard'}[pid]
 return b'\0'.join(part.encode('utf-8') for part in expected[name])+b'\0'
subprocess.run=_synthetic_active_systemctl
bounded_proc_cmdline=_synthetic_active_cmdline
loopback_listener_owned_by_pid=lambda pid,port: pid==103 and port==18081
'''
    stale_embedded = embedded.replace(
        "result={'alert':preflight_error is not None,",
        active_fakes + "\nresult={'alert':preflight_error is not None,",
        1,
    )
    assert stale_embedded != embedded
    stale_monitor_python = tmp_path / "materialized-monitor-prepared-stale.py"
    stale_monitor_python.write_text(stale_embedded, encoding="utf-8")
    stale_completed = subprocess.run(
        [
            sys.executable,
            "-I",
            str(stale_monitor_python),
            str(handoff_path),
            "full",
            str(ROOT.resolve()),
        ],
        capture_output=True,
        check=False,
        cwd=tmp_path,
        text=True,
        timeout=30,
    )
    assert stale_completed.returncode == 0, stale_completed.stderr
    stale_value = json.loads(stale_completed.stdout)
    assert stale_value["alert"] is True
    assert stale_value["activation_admissible"] is False
    for venue in ("polymarket", "kalshi"):
        service = stale_value["services"][venue]
        assert service["command_verified"] is True
        assert service["venue_status"] == "PREPARED_STALE"


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
    block_a.write_text(
        launch_pack.render_windows_transfer(handoff),
        encoding="utf-8",
        newline="\n",
    )
    transfer_path = pack / "transfer-inventory.json"
    transfer = json.loads(transfer_path.read_bytes())
    bundle_rows = [
        row
        for row in transfer["files"]
        if row["path"] == bundle.name
    ]
    assert len(bundle_rows) == 1
    bundle_rows[0].update(
        {
            "sha256": launch_pack.sha256_file(bundle),
            "size": bundle.stat().st_size,
        }
    )
    block_rows = [
        row
        for row in transfer["files"]
        if row["path"] == "operator/A-windows-bundle-verify-transfer.ps1"
    ]
    assert len(block_rows) == 1
    block_rows[0].update(
        {
            "sha256": launch_pack.sha256_file(block_a),
            "size": block_a.stat().st_size,
        }
    )
    transfer_payload = launch_pack.canonical_json_bytes(transfer) + b"\n"
    transfer_path.write_bytes(transfer_payload)
    handoff["transfer_inventory_sha256"] = launch_pack.sha256_bytes(transfer_payload)
    handoff_payload = launch_pack.canonical_json_bytes(handoff) + b"\n"
    (pack / "handoff.json").write_bytes(handoff_payload)
    (pack / "handoff.sha256").write_text(
        f"{launch_pack.sha256_bytes(handoff_payload)}  handoff.json\n",
        encoding="ascii",
    )
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


def test_post_bootstrap_install_admission_reauthenticates_units_and_live_margin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        preflight,
        "_runtime_import_subprocess_admission",
        _synthetic_runtime_import_result,
    )
    slug = "pm-20260827t235959z-feedface"
    home_mount = tmp_path / "home"
    incoming_parent = home_mount / "incoming"
    volume_base = tmp_path / "volume" / "hyperlab-prediction-markets"
    volume_mount = tmp_path / "volume"
    incoming = incoming_parent / slug
    source_root = volume_base / "sources" / slug
    campaign_root = volume_base / "campaigns" / slug
    incoming.joinpath("systemd").mkdir(parents=True)
    incoming.joinpath("scripts").mkdir()
    launch_script = incoming / "scripts" / "launch_pack.py"
    launch_script.write_text(
        "# SYNTHETIC/FIXTURE source identity verifier target\n",
        encoding="utf-8",
    )
    source_inventory = incoming / "source-inventory.json"
    source_inventory.write_text(
        '{"fixture":"SYNTHETIC/FIXTURE source inventory"}\n',
        encoding="utf-8",
    )
    runtime = source_root / ".venv" / "bin" / "python"
    runtime.parent.mkdir(parents=True)
    runtime.write_text("SYNTHETIC/FIXTURE offline runtime\n", encoding="utf-8")
    services = {
        venue: f"hyperlab-pm-{slug.removeprefix('pm-')}-{venue}.service"
        for venue in ("polymarket", "kalshi", "dashboard")
    }
    namespace_probes = {
        venue: f"hyperlab-pm-{slug.removeprefix('pm-')}-{venue}-namespace-probe.service"
        for venue in ("polymarket", "kalshi")
    }
    unit_items: list[dict[str, object]] = []
    for service in [*services.values(), *namespace_probes.values()]:
        path = incoming / "systemd" / service
        path.write_text(
            "[Unit]\nDescription=SYNTHETIC/FIXTURE admission unit\n",
            encoding="utf-8",
        )
        unit_items.append(
            {
                "path": f"systemd/{service}",
                "sha256": preflight.sha256_bytes(path.read_bytes()),
                "size": path.stat().st_size,
            }
        )
    transfer = {
        "files": [
            *unit_items,
            {
                "path": "scripts/launch_pack.py",
                "sha256": preflight.sha256_bytes(launch_script.read_bytes()),
                "size": launch_script.stat().st_size,
            },
            {
                "path": "source-inventory.json",
                "sha256": preflight.sha256_bytes(source_inventory.read_bytes()),
                "size": source_inventory.stat().st_size,
            },
        ],
        "schema_version": 1,
    }
    transfer_raw = preflight.canonical_json_bytes(transfer) + b"\n"
    (incoming / "transfer-inventory.json").write_bytes(transfer_raw)
    handoff = {
        "boundary": preflight.BOUNDARY,
        "campaign_root": campaign_root.as_posix(),
        "dashboard_port": 18081,
        "disk": dict(preflight._DISK_RESERVATION),
        "incoming_root": incoming.as_posix(),
        "run_slug": slug,
        "schema_version": 1,
        "service_user": "hyperlab",
        "services": services,
        "source_commit": "b" * 40,
        "source_inventory_sha256": "c" * 64,
        "source_root": source_root.as_posix(),
        "transfer_inventory_sha256": preflight.sha256_bytes(transfer_raw),
        "volume_base": volume_base.as_posix(),
        "volume_mount": volume_mount.as_posix(),
    }
    handoff_raw = preflight.canonical_json_bytes(handoff) + b"\n"
    handoff_path = incoming / "handoff.json"
    handoff_path.write_bytes(handoff_raw)
    (incoming / "handoff.sha256").write_text(
        f"{preflight.sha256_bytes(handoff_raw)}  handoff.json\n",
        encoding="ascii",
    )
    host = {
        "boundary": preflight.BOUNDARY,
        "checks": {
            "filesystem": {
                "device_major_minor": "8:16",
                "filesystem": "ext4",
                "filesystem_root": "/",
                "mount": volume_mount.as_posix(),
                "options": ["rw", "relatime", "discard"],
                "source": "/dev/sdb",
                "stat_device_major_minor": "8:16",
            },
            "loopback_port": {
                "free": False,
                "host": "127.0.0.1",
                "occupied_by_authenticated_superseded_dashboard": True,
                "owner_service": (
                    "hyperlab-pm-20260828t024827z-bcb5280f-dashboard.service"
                ),
                "port": 18081,
            },
        },
        "eligible_venues": ["polymarket", "kalshi"],
        "errors": [],
        "host_admitted": True,
        "installation_admissible": True,
        "network": {
            venue: {"verdict": "NETWORK_PREFLIGHT_GREEN"}
            for venue in ("polymarket", "kalshi")
        },
        "schema_version": 1,
        "terminal_signal": "PREDICTION_HOST_PREFLIGHT_GREEN",
    }
    host_path = incoming / "host-preflight-report.json"
    host_path.write_bytes(preflight.canonical_json_bytes(host) + b"\n")
    fsync = {
        "boundary": preflight.BOUNDARY,
        "filesystem_write_surface": volume_base.as_posix(),
        "parent_roots": [
            volume_base.as_posix(),
            (volume_base / "sources").as_posix(),
            (volume_base / "campaigns").as_posix(),
        ],
        "probe_removed": True,
        "schema_version": 1,
        "terminal_signal": "PREDICTION_FILESYSTEM_FSYNC_GREEN",
    }
    fsync_path = incoming / "filesystem-fsync-report.json"
    fsync_path.write_bytes(preflight.canonical_json_bytes(fsync) + b"\n")
    monkeypatch.setattr(preflight, "_INCOMING_PARENT", PurePosixPath(incoming_parent.as_posix()))
    monkeypatch.setattr(preflight, "_HOME_MOUNT", PurePosixPath(home_mount.as_posix()))
    monkeypatch.setattr(preflight, "_VOLUME_BASE", PurePosixPath(volume_base.as_posix()))
    with pytest.raises(preflight.PreflightError, match="install handoff path"):
        preflight.validate_install_layout(
            {**handoff, "volume_mount": (tmp_path / "other-volume").as_posix()},
            handoff_path=handoff_path,
        )
    monkeypatch.setenv("USER", "hyperlab")
    monkeypatch.setattr(
        preflight.Path,
        "home",
        classmethod(lambda _cls: Path("/home/hyperlab")),
    )
    monkeypatch.setattr(
        preflight,
        "resume_preflight",
        lambda *_args, **_kwargs: {
            "checks": {"source_identity": {"commit": "b" * 40}},
            "errors": [],
            "resume_admissible": True,
        },
    )
    incoming_device = "254:0"
    monkeypatch.setattr(
        preflight,
        "_stat_device_major_minor",
        lambda path: (
            incoming_device
            if path == home_mount or home_mount in path.parents
            else "8:16"
        ),
    )
    available = 195_484_491_776
    ntp = "yes"
    filesystem_source = "/dev/sdb"
    filesystem_device = "8:16"
    namespace_fstype = "ext4"
    readonly_mode = "ro"
    venue_mode = "rw"
    source_mount_target_override: Path | None = None
    incoming_mount_target_override: Path | None = None
    readonly_fsroot_override: str | None = None
    venue_fsroot_override: str | None = None
    volume_mode = "rw"
    source_identity_green = True

    def live_command(arguments: list[str] | tuple[str, ...]) -> preflight.CommandResult:
        if str(arguments[0]) == str(runtime):
            if not source_identity_green:
                return preflight.CommandResult(4, "", "SYNTHETIC/FIXTURE source dirty")
            return preflight.CommandResult(
                0,
                json.dumps(
                    {
                        "commit": "b" * 40,
                        "files": 1,
                        "inventory_sha256": "c" * 64,
                        "status": "PREDICTION_SOURCE_IDENTITY_GREEN",
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "",
            )
        if arguments[0] == "timedatectl":
            return preflight.CommandResult(0, ntp, "")
        if arguments[0] == "findmnt":
            target = Path(str(arguments[arguments.index("-T") + 1]))
            if target == volume_mount:
                return preflight.CommandResult(
                    0,
                    f"{target.as_posix()} {filesystem_source} ext4 {volume_mode},relatime,discard {filesystem_device} /",
                    "",
                )
            if target == home_mount:
                return preflight.CommandResult(
                    0,
                    f"{home_mount.as_posix()} /dev/root[/home] ext4 ro,relatime {incoming_device} /home",
                    "",
                )
            if target == incoming:
                incoming_target = incoming_mount_target_override or home_mount
                incoming_relative = incoming_target.relative_to(home_mount)
                incoming_fsroot = (
                    PurePosixPath("/home") / PurePosixPath(incoming_relative.as_posix())
                ).as_posix()
                return preflight.CommandResult(
                    0,
                    f"{incoming_target.as_posix()} /dev/root[{incoming_fsroot}] ext4 ro,relatime {incoming_device} {incoming_fsroot}",
                    "",
                )
            relative = target.relative_to(volume_mount).as_posix()
            is_venue = target in {campaign_root / "polymarket", campaign_root / "kalshi"}
            mode = venue_mode if is_venue else readonly_mode
            mount_target = (
                target
                if is_venue
                else (
                    source_mount_target_override
                    if target == source_root and source_mount_target_override is not None
                    else volume_mount
                )
            )
            if is_venue:
                fsroot_value = venue_fsroot_override or f"/{relative}"
            else:
                mount_relative = mount_target.relative_to(volume_mount).as_posix()
                derived_fsroot = "/" if mount_relative == "." else f"/{mount_relative}"
                fsroot_value = readonly_fsroot_override or derived_fsroot
            return preflight.CommandResult(
                0,
                f"{mount_target.as_posix()} {filesystem_source} {namespace_fstype} {mode},relatime {filesystem_device} {fsroot_value}",
                "",
            )
        if arguments[0] == "df":
            return preflight.CommandResult(
                0,
                "Filesystem 1-blocks Used Available Capacity Mounted on\n"
                f"{filesystem_source} 300000000000 1 {available} 1% {volume_mount.as_posix()}",
                "",
            )
        if arguments[0] == "systemctl":
            return preflight.CommandResult(
                0,
                "LoadState=not-found\nActiveState=inactive\nSubState=dead\nMainPID=0",
                "",
            )
        raise AssertionError(arguments)

    green = preflight.install_admission_preflight(
        handoff_path,
        host_path,
        fsync_path,
        run=live_command,
    )
    assert green["install_admissible"] is True, green
    assert green["terminal_signal"] == "PREDICTION_INSTALL_ADMISSION_GREEN"
    live = green["evidence"]["live"]  # type: ignore[index]
    assert live["capacity"] == {  # type: ignore[index]
        "admitted": True,
        "available_bytes": available,
        "required_free_bytes": 194_347_270_144,
    }
    assert available - 194_347_270_144 == 1_137_221_632
    asserted_hashes = green["evidence"]["unit_sha256"]  # type: ignore[index]
    assert set(asserted_hashes) == {*services.values(), *namespace_probes.values()}
    install_admission_path = (
        volume_base / "campaigns" / slug / "state" / "install-admission-report.json"
    )
    install_admission_path.parent.mkdir(parents=True)
    (campaign_root / "polymarket").mkdir()
    (campaign_root / "kalshi").mkdir()
    install_admission_path.write_bytes(preflight.canonical_json_bytes(green) + b"\n")
    green_guard = preflight.collector_activation_guard(
        handoff_path,
        install_admission_path,
        run=live_command,
    )
    assert green_guard["activation_admissible"] is True
    volume_mode = "ro"
    old_parent_rw_predicate = "rw" in {volume_mode, "relatime"} and "ro" not in {
        volume_mode,
        "relatime",
    }
    assert old_parent_rw_predicate is False
    startup_green = preflight.runner_startup_admission(
        handoff_path,
        install_admission_path,
        venue="polymarket",
        run=live_command,
    )
    assert startup_green["startup_admissible"] is True
    assert startup_green["checks"]["capacity"] == {  # type: ignore[index]
        "deferred_to_ledger_accounted_runner_gate": True
    }
    assert startup_green["checks"]["namespace"]["parent_mount"]["options"][0] == "ro"  # type: ignore[index]
    assert "rw" not in startup_green["checks"]["namespace"]["parent_mount"]["options"]  # type: ignore[index]
    assert startup_green["checks"]["namespace"]["volume_base_readonly"]["mount"] == volume_mount.as_posix()  # type: ignore[index]
    assert startup_green["checks"]["namespace"]["incoming_readonly"]["mount"] == home_mount.as_posix()  # type: ignore[index]
    assert {
        row["mount"]
        for row in startup_green["checks"]["namespace"]["readonly_roots"].values()  # type: ignore[index,union-attr]
    } == {volume_mount.as_posix()}
    source_mount_target_override = volume_base / "sources"
    refused_unlisted_source_mount = preflight.runner_namespace_admission(
        handoff_path,
        install_admission_path,
        venue="polymarket",
        run=live_command,
    )
    assert refused_unlisted_source_mount["namespace_admissible"] is False
    assert "mount target is not allowlisted" in " ".join(  # type: ignore[arg-type]
        refused_unlisted_source_mount["errors"]
    )
    source_mount_target_override = None
    for incoming_mount_target_override in (home_mount, incoming.parent, incoming):
        coalesced_incoming_green = preflight.runner_namespace_admission(
            handoff_path,
            install_admission_path,
            venue="polymarket",
            run=live_command,
        )
        assert coalesced_incoming_green["namespace_admissible"] is True
        incoming_observed = coalesced_incoming_green["observed_mounts"]["incoming"]  # type: ignore[index]
        assert incoming_observed["mount"] == incoming_mount_target_override.as_posix()  # type: ignore[index]
        assert incoming_observed["logical_path"] == incoming.as_posix()  # type: ignore[index]
    incoming_mount_target_override = home_mount / "cousin"
    refused_unlisted_incoming_mount = preflight.runner_namespace_admission(
        handoff_path,
        install_admission_path,
        venue="polymarket",
        run=live_command,
    )
    assert refused_unlisted_incoming_mount["namespace_admissible"] is False
    assert "authenticated ancestor" in " ".join(  # type: ignore[arg-type]
        refused_unlisted_incoming_mount["errors"]
    )
    incoming_mount_target_override = None
    volume_mode = "rw"
    namespace_parent_rw_refused = preflight.runner_namespace_admission(
        handoff_path,
        install_admission_path,
        venue="polymarket",
        run=live_command,
    )
    assert namespace_parent_rw_refused["namespace_admissible"] is False
    assert "volume parent is not mounted ro" in " ".join(  # type: ignore[arg-type]
        namespace_parent_rw_refused["errors"]
    )
    volume_mode = "ro"
    namespace_fstype = "xfs"
    refused_fstype = preflight.runner_namespace_admission(
        handoff_path,
        install_admission_path,
        venue="polymarket",
        run=live_command,
    )
    assert refused_fstype["namespace_admissible"] is False
    assert "filesystem type diverged" in " ".join(refused_fstype["errors"])  # type: ignore[arg-type]
    namespace_fstype = "ext4"
    venue_mode = "ro"
    refused_readonly = preflight.runner_namespace_admission(
        handoff_path,
        install_admission_path,
        venue="polymarket",
        run=live_command,
    )
    assert refused_readonly["namespace_admissible"] is False
    assert "venue root is not mounted rw" in " ".join(  # type: ignore[arg-type]
        refused_readonly["errors"]
    )
    venue_mode = "rw"
    venue_fsroot_override = "/other/same-device-bind"
    refused_bind = preflight.runner_namespace_admission(
        handoff_path,
        install_admission_path,
        venue="polymarket",
        run=live_command,
    )
    assert refused_bind["namespace_admissible"] is False
    assert "root/bind identity diverged" in " ".join(refused_bind["errors"])  # type: ignore[arg-type]
    venue_fsroot_override = None
    readonly_fsroot_override = "/other/same-device-bind"
    refused_readonly_bind = preflight.runner_namespace_admission(
        handoff_path,
        install_admission_path,
        venue="polymarket",
        run=live_command,
    )
    assert refused_readonly_bind["namespace_admissible"] is False
    assert "root/bind identity diverged" in " ".join(  # type: ignore[arg-type]
        refused_readonly_bind["errors"]
    )
    readonly_fsroot_override = None

    def fsync_refused(_root: Path) -> dict[str, object]:
        raise preflight.PreflightError("SYNTHETIC/FIXTURE fsync impossible")

    refused_fsync = preflight.runner_namespace_admission(
        handoff_path,
        install_admission_path,
        venue="polymarket",
        run=live_command,
        write_probe=fsync_refused,
    )
    assert refused_fsync["namespace_admissible"] is False
    assert "fsync impossible" in " ".join(refused_fsync["errors"])  # type: ignore[arg-type]

    ntp = "no"
    startup_ntp_refused = preflight.runner_startup_admission(
        handoff_path,
        install_admission_path,
        venue="polymarket",
        run=live_command,
    )
    assert startup_ntp_refused["startup_admissible"] is False
    assert "NTP is not synchronized" in " ".join(  # type: ignore[arg-type]
        startup_ntp_refused["errors"]
    )
    ntp = "yes"

    source_identity_green = False
    startup_source_refused = preflight.runner_startup_admission(
        handoff_path,
        install_admission_path,
        venue="polymarket",
        run=live_command,
    )
    assert startup_source_refused["startup_admissible"] is False
    assert "source identity failed" in " ".join(  # type: ignore[arg-type]
        startup_source_refused["errors"]
    )
    source_identity_green = True

    refused_path = preflight.collector_activation_guard(
        handoff_path,
        install_admission_path.with_name("other-install-admission.json"),
        run=live_command,
    )
    assert refused_path["activation_admissible"] is False
    assert "install admission path diverged" in " ".join(refused_path["errors"])  # type: ignore[arg-type]

    install_admission_raw = install_admission_path.read_bytes()
    tampered_admission = json.loads(install_admission_raw)
    tampered_admission["evidence"]["handoff_sha256"] = "f" * 64
    install_admission_path.write_bytes(
        preflight.canonical_json_bytes(tampered_admission) + b"\n"
    )
    refused_binding = preflight.collector_activation_guard(
        handoff_path,
        install_admission_path,
        run=live_command,
    )
    assert refused_binding["activation_admissible"] is False
    assert "install admission binding diverged" in " ".join(  # type: ignore[arg-type]
        refused_binding["errors"]
    )

    tampered_admission = json.loads(install_admission_raw)
    tampered_admission["evidence"]["layout"]["source_root"] += "-diverged"
    install_admission_path.write_bytes(
        preflight.canonical_json_bytes(tampered_admission) + b"\n"
    )
    refused_layout = preflight.collector_activation_guard(
        handoff_path,
        install_admission_path,
        run=live_command,
    )
    assert refused_layout["activation_admissible"] is False
    assert "install admission binding diverged" in " ".join(  # type: ignore[arg-type]
        refused_layout["errors"]
    )

    install_admission_path.write_text(
        json.dumps(green, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    refused_noncanonical = preflight.collector_activation_guard(
        handoff_path,
        install_admission_path,
        run=live_command,
    )
    assert refused_noncanonical["activation_admissible"] is False
    assert "not canonical JSON" in " ".join(  # type: ignore[arg-type]
        refused_noncanonical["errors"]
    )
    install_admission_path.write_bytes(install_admission_raw)

    host_raw = host_path.read_bytes()
    for bad_eligible in ([{}], ["kalshi", "kalshi"]):
        invalid_host = json.loads(host_raw)
        invalid_host["eligible_venues"] = bad_eligible
        host_path.write_bytes(preflight.canonical_json_bytes(invalid_host) + b"\n")
        refused_host = preflight.install_admission_preflight(
            handoff_path,
            host_path,
            fsync_path,
            run=live_command,
        )
        assert refused_host["install_admissible"] is False
        assert "host preflight admission evidence diverged" in " ".join(  # type: ignore[arg-type]
            refused_host["errors"]
        )
    host_path.write_bytes(host_raw)

    volume_mode = "rw"
    filesystem_source = "/dev/sdc"
    filesystem_device = "8:17"
    startup_device_refused = preflight.runner_startup_admission(
        handoff_path,
        install_admission_path,
        venue="polymarket",
        run=live_command,
    )
    assert startup_device_refused["startup_admissible"] is False
    assert "device identities diverged" in " ".join(  # type: ignore[arg-type]
        startup_device_refused["errors"]
    )
    refused_device = preflight.collector_activation_guard(
        handoff_path,
        install_admission_path,
        run=live_command,
    )
    assert refused_device["activation_admissible"] is False
    assert "device identities diverged" in " ".join(  # type: ignore[arg-type]
        refused_device["errors"]
    )
    filesystem_source = "/dev/sdb"
    filesystem_device = "8:16"

    mutated = incoming / "systemd" / services["polymarket"]
    mutated.write_text("SYNTHETIC/FIXTURE unit mutated after inventory\n", encoding="utf-8")
    refused_unit = preflight.install_admission_preflight(
        handoff_path,
        host_path,
        fsync_path,
        run=live_command,
    )
    assert refused_unit["install_admissible"] is False
    assert "transfer file hash diverged" in " ".join(refused_unit["errors"])  # type: ignore[arg-type]

    available = 194_347_270_143
    refused_capacity = preflight.collector_activation_guard(
        handoff_path,
        install_admission_path,
        run=live_command,
    )
    assert refused_capacity["activation_admissible"] is False
    assert "enlarge or choose another ext4 volume" in " ".join(  # type: ignore[arg-type]
        refused_capacity["errors"]
    )
    ntp = "no"
    refused_ntp = preflight.collector_activation_guard(
        handoff_path,
        install_admission_path,
        run=live_command,
    )
    assert refused_ntp["activation_admissible"] is False
    assert "NTP is not synchronized" in " ".join(refused_ntp["errors"])  # type: ignore[arg-type]


def test_transfer_inventory_refuses_a_linked_parent_directory(tmp_path: Path) -> None:
    handoff_path = _incoming_handoff(tmp_path)
    incoming = handoff_path.parent
    scripts = incoming / "scripts"
    outside_scripts = tmp_path / "outside-scripts"
    scripts.rename(outside_scripts)
    try:
        try:
            os.symlink(outside_scripts, scripts, target_is_directory=True)
        except OSError:
            if os.name != "nt":
                pytest.skip("directory symlink creation is unavailable")
            linked = subprocess.run(
                [
                    os.environ.get("COMSPEC", r"C:\Windows\System32\cmd.exe"),
                    "/d",
                    "/c",
                    "mklink",
                    "/J",
                    str(scripts),
                    str(outside_scripts),
                ],
                capture_output=True,
                check=False,
                text=True,
                timeout=30,
            )
            assert linked.returncode == 0, linked.stderr + linked.stdout
        handoff = preflight.load_handoff(handoff_path)
        with pytest.raises(
            preflight.PreflightError,
            match="transfer inventory parent directory is unsafe",
        ):
            preflight.verify_transfer_inventory(incoming, handoff)
    finally:
        if scripts.is_symlink():
            scripts.unlink()
        elif scripts.exists():
            scripts.rmdir()


def test_host_preflight_synchronously_proves_ntp_capacity_port_services_and_isolates_venues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handoff = _incoming_handoff(tmp_path)
    monkeypatch.setenv("USER", "hyperlab")
    monkeypatch.setattr(preflight.Path, "home", classmethod(lambda _cls: Path("/home/hyperlab")))
    monkeypatch.setattr(preflight, "_required_command", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(preflight, "_stat_device_major_minor", lambda _path: "8:16")
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
        "free": False,
        "host": "127.0.0.1",
        "occupied_by_authenticated_superseded_dashboard": True,
        "owner_service": "hyperlab-pm-20260828t024827z-bcb5280f-dashboard.service",
        "port": 18081,
        "verification_signals": [
            "PREDICTION_OLD_RAW_RECEIPTS_LEDGER_AUTHENTICATED",
            "PREDICTION_OLD_CAMPAIGN_FIVE_UNITS_AUTHENTICATED",
            "PREDICTION_OLD_CAMPAIGN_PREMUTATION_AUTHENTICATED",
        ],
    }
    assert len(result["checks"]["services"]) == 5  # type: ignore[index]


def test_host_preflight_refuses_unauthenticated_superseded_dashboard_port_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handoff = _incoming_handoff(tmp_path)
    monkeypatch.setenv("USER", "hyperlab")
    monkeypatch.setattr(preflight.Path, "home", classmethod(lambda _cls: Path("/home/hyperlab")))
    monkeypatch.setattr(preflight, "_required_command", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(preflight, "_stat_device_major_minor", lambda _path: "8:16")

    def wrong_owner(arguments: list[str] | tuple[str, ...]) -> preflight.CommandResult:
        if arguments[0] == "bash" and "verify-old" in arguments:
            return preflight.CommandResult(4, "", "old dashboard listener diverged")
        return _green_command(arguments)

    result = preflight.host_preflight(
        handoff,
        run=wrong_owner,
        connectivity_probe=lambda venue: {
            "venue": venue,
            "verdict": "NETWORK_PREFLIGHT_GREEN",
        },
    )
    assert result["installation_admissible"] is False
    assert "superseded dashboard port owner authentication failed" in " ".join(
        result["errors"]  # type: ignore[arg-type]
    )


def test_host_preflight_refuses_dangling_attempt_root_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handoff_path = _incoming_handoff(tmp_path)
    handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
    source_link = tmp_path / "new-source-root"
    try:
        source_link.symlink_to(tmp_path / "absent-source-target", target_is_directory=True)
    except OSError:
        pytest.skip("directory symlink creation is unavailable")
    handoff["source_root"] = str(source_link)
    handoff["campaign_root"] = str(tmp_path / "new-campaign-root")
    handoff_raw = preflight.canonical_json_bytes(handoff) + b"\n"
    handoff_path.write_bytes(handoff_raw)
    handoff_path.with_name("handoff.sha256").write_text(
        f"{preflight.sha256_bytes(handoff_raw)}  handoff.json\n",
        encoding="ascii",
    )
    monkeypatch.setenv("USER", "hyperlab")
    monkeypatch.setattr(
        preflight.Path,
        "home",
        classmethod(lambda _cls: Path("/home/hyperlab")),
    )
    monkeypatch.setattr(preflight, "_required_command", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(preflight, "_stat_device_major_minor", lambda _path: "8:16")
    result = preflight.host_preflight(
        handoff_path,
        run=_green_command,
        connectivity_probe=lambda venue: {
            "venue": venue,
            "verdict": "NETWORK_PREFLIGHT_GREEN",
        },
    )
    assert result["installation_admissible"] is False
    assert "unique source_root already exists" in " ".join(result["errors"])  # type: ignore[arg-type]


def test_host_and_fsync_preflights_refuse_dangling_parent_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handoff_path = _incoming_handoff(tmp_path)
    handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
    base = tmp_path / "volume-base"
    base.mkdir(mode=0o700)
    (base / "campaigns").mkdir(mode=0o700)
    try:
        (base / "sources").symlink_to(
            tmp_path / "absent-sources-target",
            target_is_directory=True,
        )
    except OSError:
        pytest.skip("directory symlink creation is unavailable")
    handoff.update(
        {
            "volume_base": str(base),
            "source_root": str(base / "sources" / "new-source"),
            "campaign_root": str(base / "campaigns" / "new-campaign"),
        }
    )
    handoff_raw = preflight.canonical_json_bytes(handoff) + b"\n"
    handoff_path.write_bytes(handoff_raw)
    handoff_path.with_name("handoff.sha256").write_text(
        f"{preflight.sha256_bytes(handoff_raw)}  handoff.json\n",
        encoding="ascii",
    )
    monkeypatch.setenv("USER", "hyperlab")
    monkeypatch.setattr(
        preflight.Path,
        "home",
        classmethod(lambda _cls: Path("/home/hyperlab")),
    )
    monkeypatch.setattr(preflight, "_required_command", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(preflight, "_stat_device_major_minor", lambda _path: "8:16")
    result = preflight.host_preflight(
        handoff_path,
        run=_green_command,
        connectivity_probe=lambda venue: {
            "venue": venue,
            "verdict": "NETWORK_PREFLIGHT_GREEN",
        },
    )
    assert result["installation_admissible"] is False
    assert "source_parent is an unsafe existing path" in " ".join(result["errors"])  # type: ignore[arg-type]
    with pytest.raises(preflight.PreflightError, match="parent root is absent or unsafe"):
        preflight.fsync_probe(handoff_path)


@pytest.mark.parametrize(
    "failure",
    ["ntp", "capacity", "capacity-command", "service", "service-command"],
)
def test_host_preflight_refuses_ntp_capacity_or_service_collision(
    failure: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handoff = _incoming_handoff(tmp_path)
    monkeypatch.setenv("USER", "hyperlab")
    monkeypatch.setattr(preflight.Path, "home", classmethod(lambda _cls: Path("/home/hyperlab")))
    monkeypatch.setattr(preflight, "_required_command", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(preflight, "_stat_device_major_minor", lambda _path: "8:16")

    def run(arguments: list[str] | tuple[str, ...]) -> preflight.CommandResult:
        if failure == "ntp" and arguments[0] == "timedatectl":
            return preflight.CommandResult(0, "no", "")
        if failure == "capacity" and arguments[0] == "df":
            return preflight.CommandResult(
                0,
                "Filesystem 1-blocks Used Available Capacity Mounted on\n/dev/sdb 300000000000 1 1000 99% /mnt/HC_Volume_106716684",
                "",
            )
        if failure == "capacity-command" and arguments[0] == "df":
            return preflight.CommandResult(
                1,
                "Filesystem 1-blocks Used Available Capacity Mounted on\n/dev/sdb 300000000000 1 200000000000 1% /mnt/HC_Volume_106716684",
                "synthetic df failure",
            )
        if failure == "service" and arguments[0] == "systemctl":
            return preflight.CommandResult(
                0,
                "LoadState=loaded\nActiveState=active\nSubState=running\nMainPID=123",
                "",
            )
        if failure == "service-command" and arguments[0] == "systemctl":
            return preflight.CommandResult(1, "", "synthetic D-Bus failure")
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
        "capacity-command": "capacity check failed",
        "service": "service collision",
        "service-command": "systemd service identity check failed",
    }[failure]
    assert expected in " ".join(result["errors"])  # type: ignore[arg-type]


def test_filesystem_probe_fsyncs_and_removes_only_its_exclusive_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handoff = _incoming_handoff(tmp_path)
    base = tmp_path / "volume-base"
    base.mkdir(mode=0o700)
    roots = (base, base / "sources", base / "campaigns")
    for root in roots:
        root.mkdir(mode=0o700, exist_ok=True)
        os.chmod(root, 0o700)
    original_stat = preflight.Path.stat

    def linux_mode_stat(path: Path, *args: object, **kwargs: object) -> os.stat_result:
        result = original_stat(path, *args, **kwargs)  # type: ignore[arg-type]
        if path in roots:
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
    assert result["parent_roots"] == [str(root) for root in roots]
    assert set(base.iterdir()) == {base / "sources", base / "campaigns"}


def test_runner_write_surface_probe_is_exclusive_bounded_and_cleans_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    venue = tmp_path / "polymarket"
    venue.mkdir()
    sentinel = venue / "SYNTHETIC_FIXTURE_RAW_PRESERVED.bin"
    sentinel.write_bytes(b"SYNTHETIC/FIXTURE raw sentinel\n")
    before = sentinel.read_bytes()
    result = preflight._durable_write_surface_probe(venue)
    assert result["exclusive_create"] is True
    assert result["file_fsync"] is True
    assert result["probe_removed"] is True
    assert sentinel.read_bytes() == before
    assert not list(venue.glob(".prediction-write-surface-probe-*"))

    real_fsync = preflight.os.fsync
    calls = 0

    def refused_fsync(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("SYNTHETIC/FIXTURE file fsync refused")
        real_fsync(descriptor)

    monkeypatch.setattr(preflight.os, "fsync", refused_fsync)
    with pytest.raises(preflight.PreflightError, match="durable probe failed"):
        preflight._durable_write_surface_probe(venue)
    assert sentinel.read_bytes() == before
    assert not list(venue.glob(".prediction-write-surface-probe-*"))


def test_runner_write_surface_probe_posix_fsyncs_directory_before_and_after_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    venue = tmp_path / "polymarket"
    venue.mkdir()
    model_file = tmp_path / "SYNTHETIC_FIXTURE_REGULAR_FILE"
    model_file.write_bytes(b"SYNTHETIC/FIXTURE\n")
    root_stat = venue.lstat()
    file_stat = model_file.lstat()
    events: list[str] = []
    directory_descriptor = 700
    file_descriptor = 701
    real_stat = preflight.os.stat

    class ProbeHandle:
        def __enter__(self) -> ProbeHandle:
            return self

        def __exit__(self, *_args: object) -> None:
            events.append("close-file")

        def fileno(self) -> int:
            return file_descriptor

        def flush(self) -> None:
            events.append("flush-file")

        def write(self, value: bytes) -> int:
            assert value == b"PREDICTION_MARKETS_WRITE_SURFACE_PROBE_V1\n"
            events.append("write-file")
            return len(value)

    def fake_open(
        path: object,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        del mode
        if path == venue:
            assert dir_fd is None
            events.append("open-directory")
            return directory_descriptor
        assert isinstance(path, str)
        assert path.startswith(".prediction-write-surface-probe-")
        assert dir_fd == directory_descriptor
        assert flags & os.O_EXCL
        events.append("open-file-exclusive")
        return file_descriptor

    def fake_fstat(descriptor: int) -> os.stat_result:
        if descriptor == directory_descriptor:
            return root_stat
        assert descriptor == file_descriptor
        return file_stat

    def fake_stat(
        path: object,
        *args: object,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
        **kwargs: object,
    ) -> os.stat_result:
        if dir_fd == directory_descriptor:
            assert isinstance(path, str)
            assert path.startswith(".prediction-write-surface-probe-")
            assert follow_symlinks is False
            events.append("authenticate-file-before-unlink")
            return file_stat
        return real_stat(
            path,  # type: ignore[arg-type]
            *args,
            dir_fd=dir_fd,
            follow_symlinks=follow_symlinks,
            **kwargs,
        )

    def fake_fsync(descriptor: int) -> None:
        if descriptor == file_descriptor:
            events.append("fsync-file")
            return
        assert descriptor == directory_descriptor
        events.append("fsync-directory")

    def fake_unlink(path: object, *, dir_fd: int | None = None) -> None:
        assert isinstance(path, str)
        assert path.startswith(".prediction-write-surface-probe-")
        assert dir_fd == directory_descriptor
        events.append("unlink-file")

    monkeypatch.setattr(preflight.os, "name", "posix")
    monkeypatch.setattr(preflight.os, "open", fake_open)
    monkeypatch.setattr(preflight.os, "fstat", fake_fstat)
    monkeypatch.setattr(preflight.os, "stat", fake_stat)
    monkeypatch.setattr(preflight.os, "fdopen", lambda *_args, **_kwargs: ProbeHandle())
    monkeypatch.setattr(preflight.os, "fsync", fake_fsync)
    monkeypatch.setattr(preflight.os, "unlink", fake_unlink)
    monkeypatch.setattr(
        preflight.os,
        "close",
        lambda descriptor: events.append(f"close-descriptor-{descriptor}"),
    )

    result = preflight._durable_write_surface_probe(venue)
    assert result["directory_fsync"] is True
    assert events == [
        "open-directory",
        "open-file-exclusive",
        "write-file",
        "flush-file",
        "fsync-file",
        "close-file",
        "fsync-directory",
        "authenticate-file-before-unlink",
        "unlink-file",
        "fsync-directory",
        "close-descriptor-700",
    ]


def test_runner_namespace_directory_refuses_symlink_or_non_directory(
    tmp_path: Path,
) -> None:
    file_path = tmp_path / "SYNTHETIC_FIXTURE_NOT_A_DIRECTORY"
    file_path.write_text("SYNTHETIC/FIXTURE\n", encoding="utf-8")
    with pytest.raises(preflight.PreflightError, match="non-directory"):
        preflight._canonical_directory(file_path, label="runner venue root")
    real = tmp_path / "real-venue"
    real.mkdir()
    linked = tmp_path / "linked-venue"
    try:
        linked.symlink_to(real, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlink creation is unavailable")
    with pytest.raises(preflight.PreflightError, match="symlinked"):
        preflight._canonical_directory(linked, label="runner venue root")


def test_runner_write_surface_probe_never_removes_preexisting_collision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    venue = tmp_path / "kalshi"
    venue.mkdir()

    class FixedUuid:
        hex = "syntheticcollision"

    monkeypatch.setattr(preflight, "uuid4", lambda: FixedUuid())
    collision = venue / ".prediction-write-surface-probe-syntheticcollision"
    collision.write_bytes(b"SYNTHETIC/FIXTURE preexisting evidence\n")
    before = collision.read_bytes()
    with pytest.raises(preflight.PreflightError, match="durable probe failed"):
        preflight._durable_write_surface_probe(venue)
    assert collision.read_bytes() == before


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
    monkeypatch.setattr(
        preflight,
        "_runtime_import_subprocess_admission",
        _synthetic_runtime_import_result,
    )
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
    assert result["checks"]["offline_imports"] == {  # type: ignore[index]
        "admission_sha256": "d" * 64,
        "terminal_signal": "PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN",
        "verified": True,
    }
    assert result["checks"]["source_identity"] == {  # type: ignore[index]
        "commit": "b" * 40,
        "files": 1,
        "inventory_sha256": "c" * 64,
        "status": "PREDICTION_SOURCE_IDENTITY_GREEN",
    }

    def dirty_source(arguments: list[str] | tuple[str, ...]) -> preflight.CommandResult:
        if "verify-source" in arguments:
            return preflight.CommandResult(4, "", "detached source checkout is not clean")
        return _green_command(arguments)

    dirty = preflight.resume_preflight(handoff_path, run=dirty_source)
    assert dirty["resume_admissible"] is False
    assert "source checkout is not clean" in " ".join(dirty["errors"])

    def failed_df(arguments: list[str] | tuple[str, ...]) -> preflight.CommandResult:
        if arguments[0] == "df":
            return preflight.CommandResult(
                1,
                "Filesystem 1-blocks Used Available Capacity Mounted on\n/dev/sdb 300000000000 1 200000000000 1% /mnt/HC_Volume_106716684",
                "synthetic df failure",
            )
        return _green_command(arguments)

    refused = preflight.resume_preflight(handoff_path, run=failed_df)
    assert refused["resume_admissible"] is False
    assert "resume capacity check failed" in " ".join(refused["errors"])


def test_recovery_initial_admission_authenticates_absent_venue_reason_and_pin(
    tmp_path: Path,
) -> None:
    handoff_path = _incoming_handoff(tmp_path)
    handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
    handoff["source_commit"] = "b" * 40
    handoff_raw = preflight.canonical_json_bytes(handoff) + b"\n"
    handoff_path.write_bytes(handoff_raw)
    handoff_path.with_name("handoff.sha256").write_text(
        f"{preflight.sha256_bytes(handoff_raw)}  handoff.json\n",
        encoding="ascii",
    )
    campaign = Path(handoff["campaign_root"])
    state = campaign / "state"
    state.mkdir(parents=True)
    candidate_pack = (
        ROOT
        / "ops"
        / "prediction_markets_candidate_v1"
        / "prediction-markets-v1-20260901t000000z-aa60c0ff"
    )
    for name in ("campaign-manifest.json", "campaign-manifest.sha256"):
        (campaign / name).write_bytes((candidate_pack / name).read_bytes())
    manifest = json.loads((campaign / "campaign-manifest.json").read_bytes())
    initial = {
        "boundary": preflight.BOUNDARY,
        "eligible_venues": ["kalshi"],
        "installation_admissible": True,
        "network": {
            "polymarket": {"verdict": "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT"},
            "kalshi": {"verdict": "NETWORK_PREFLIGHT_GREEN"},
        },
        "terminal_signal": "PREDICTION_HOST_PREFLIGHT_GREEN",
    }
    initial_raw = preflight.canonical_json_bytes(initial) + b"\n"
    (state / "preflight-report.json").write_bytes(initial_raw)
    activation_body = {
        "boundary": preflight.BOUNDARY,
        "campaign_id": manifest["campaign_id"],
        "campaign_manifest_sha256": manifest["manifest_sha256"],
        "campaign_root": str(campaign),
        "dashboard_port": handoff["dashboard_port"],
        "eligible_venues": ["kalshi"],
        "economic_evidence_status": "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE",
        "h1_actions": "NONE",
        "preflight_report_sha256": preflight.sha256_bytes(initial_raw),
        "quick_start": True,
        "recorded_at_utc": "2026-08-27T12:00:00.000000Z",
        "schema_version": 1,
        "source_commit": handoff["source_commit"],
        "starts_at_utc": manifest["starts_at_utc"],
    }
    activation = {
        **activation_body,
        "receipt_sha256": preflight.sha256_bytes(
            preflight.canonical_json_bytes(activation_body)
        ),
    }
    (state / "activation-receipt.json").write_bytes(
        preflight.canonical_json_bytes(activation) + b"\n"
    )

    result = preflight.recovery_initial_admission(handoff_path)
    assert result["terminal_signal"] == (
        "PREDICTION_RECOVERY_INITIAL_ADMISSION_AUTHENTICATED"
    )
    assert result["admission_by_venue"] == {
        "polymarket": {
            "eligible": False,
            "network_verdict": "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT",
        },
        "kalshi": {
            "eligible": True,
            "network_verdict": "NETWORK_PREFLIGHT_GREEN",
        },
    }

    activation_path = state / "activation-receipt.json"
    activation_raw = activation_path.read_bytes()
    manifest_tampered = json.loads(activation_raw)
    manifest_tampered["campaign_manifest_sha256"] = "f" * 64
    manifest_tampered_body = {
        key: value
        for key, value in manifest_tampered.items()
        if key != "receipt_sha256"
    }
    manifest_tampered["receipt_sha256"] = preflight.sha256_bytes(
        preflight.canonical_json_bytes(manifest_tampered_body)
    )
    activation_path.write_bytes(
        preflight.canonical_json_bytes(manifest_tampered) + b"\n"
    )
    with pytest.raises(preflight.PreflightError, match="binding diverged"):
        preflight.recovery_initial_admission(handoff_path)
    activation_path.write_bytes(activation_raw)

    tampered = {**initial, "eligible_venues": ["polymarket", "kalshi"]}
    (state / "preflight-report.json").write_bytes(
        preflight.canonical_json_bytes(tampered) + b"\n"
    )
    with pytest.raises(preflight.PreflightError, match="report hash diverged"):
        preflight.recovery_initial_admission(handoff_path)


def test_recovery_network_admission_is_immutable_campaign_bound_and_reusable(
    tmp_path: Path,
) -> None:
    handoff_path = _incoming_handoff(tmp_path)
    handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
    handoff["source_commit"] = "b" * 40
    handoff_raw = preflight.canonical_json_bytes(handoff) + b"\n"
    handoff_path.write_bytes(handoff_raw)
    handoff_path.with_name("handoff.sha256").write_text(
        f"{preflight.sha256_bytes(handoff_raw)}  handoff.json\n",
        encoding="ascii",
    )
    campaign = Path(handoff["campaign_root"])
    state = campaign / "state"
    state.mkdir(parents=True)
    candidate_pack = (
        ROOT
        / "ops"
        / "prediction_markets_candidate_v1"
        / "prediction-markets-v1-20260901t000000z-aa60c0ff"
    )
    (campaign / "campaign-manifest.json").write_bytes(
        (candidate_pack / "campaign-manifest.json").read_bytes()
    )
    initial = {
        "boundary": preflight.BOUNDARY,
        "eligible_venues": ["kalshi"],
        "installation_admissible": True,
        "network": {
            "polymarket": {"verdict": "PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT"},
            "kalshi": {"verdict": "NETWORK_PREFLIGHT_GREEN"},
        },
        "terminal_signal": "PREDICTION_HOST_PREFLIGHT_GREEN",
    }
    initial_raw = preflight.canonical_json_bytes(initial) + b"\n"
    (state / "preflight-report.json").write_bytes(initial_raw)
    network = {
        "errors": [],
        "venue": "polymarket",
        "verdict": "NETWORK_PREFLIGHT_GREEN",
    }
    network_path = handoff_path.parent / "recovery-network-polymarket.json"
    network_path.write_bytes(preflight.canonical_json_bytes(network) + b"\n")
    output = state / "recovery-admission-polymarket.json"

    record = preflight.recovery_network_admission(
        handoff_path,
        network_path,
        venue="polymarket",
        output_path=output,
    )
    first_bytes = output.read_bytes()
    assert record["terminal_signal"] == (
        "PREDICTION_RECOVERY_NETWORK_ADMISSION_AUTHENTICATED"
    )
    assert record["network_report_sha256"] == preflight.sha256_bytes(
        network_path.read_bytes()
    )
    assert record["initial_preflight_report_sha256"] == preflight.sha256_bytes(
        initial_raw
    )
    assert preflight.recovery_network_admission(
        handoff_path,
        network_path,
        venue="polymarket",
        output_path=output,
    ) == record
    assert output.read_bytes() == first_bytes

    output.write_bytes(b" " + first_bytes)
    with pytest.raises(preflight.PreflightError, match="not canonical JSON with LF"):
        preflight.recovery_network_admission(
            handoff_path,
            network_path,
            venue="polymarket",
            output_path=output,
        )
    output.write_bytes(first_bytes)

    tampered = json.loads(first_bytes)
    tampered["source_commit"] = "c" * 40
    tampered_body = {
        key: value for key, value in tampered.items() if key != "receipt_sha256"
    }
    tampered["receipt_sha256"] = preflight.sha256_bytes(
        preflight.canonical_json_bytes(tampered_body)
    )
    output.write_bytes(preflight.canonical_json_bytes(tampered) + b"\n")
    with pytest.raises(preflight.PreflightError, match="binding diverged"):
        preflight.recovery_network_admission(
            handoff_path,
            network_path,
            venue="polymarket",
            output_path=output,
        )


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
    cutover = (OPS / "cutover.sh").read_text(encoding="utf-8")
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
    assert install.index("/health/live") < install.index('for VENUE in "${ELIGIBLE[@]}"')
    assert install.rindex("/health/ready") > install.index('for VENUE in "${ELIGIBLE[@]}"')
    assert rollback.index("/health/live") < rollback.index("for VENUE in polymarket kalshi")
    assert rollback.rindex("/health/ready") > rollback.index("for VENUE in polymarket kalshi")
    assert "RECOVERY_STARTED_VENUES" in rollback
    assert "RECOVERY_REFUSED_VENUES" in rollback
    assert "manylinux_2_28_x86_64" in bundle
    assert "manylinux_2_17_x86_64" in bundle
    assert "git -C $RepoRoot bundle verify $BundlePath" in bundle
    assert "git bundle verify $BundlePath" not in bundle
    assert "glibc" in bootstrap
    assert "admission_required" in monitor and "eligible_venues" in monitor
    assert "CAPACITY_REFUSED" in monitor and "INTERRUPTED_RECOVERABLE" in monitor
    assert "hyperlab-h1" not in rollback + install + monitor
    assert "pm-20260828t024827z-bcb5280f" in cutover
    assert "bcb5280f87393992e2aa4528188009186cd8bdc3" in cutover
    assert "_validate_result" in cutover and "read_ledger" in cutover
    assert "PREDICTION_OLD_CAMPAIGN_PREMUTATION_AUTHENTICATED" in cutover
    assert "PREDICTION_OLD_CAMPAIGN_DISARMED_EVIDENCE_PRESERVED" in cutover
    assert "PREDICTION_OLD_CAMPAIGN_RESTORED_NO_SLOT_RETRY" in cutover
    assert "hyperlab-h1" not in cutover
    assert '"$OLD_PYTHON" -I "$NEW_INCOMING/scripts/preflight.py"' in cutover
    assert "$INCOMING_ROOT" not in cutover
    assert "run_hyperlab_isolated research-data prediction-prepare" in install
    assert '"$VENV_PYTHON" -m hyperlab' not in install
    assert "rm -rf" not in cutover and "unlink" not in cutover
    assert "rm -rf" not in rollback + install
    assert "unlink" not in rollback + install
