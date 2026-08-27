from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
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
    scripts = incoming / "scripts"
    scripts.mkdir()
    launch_script = scripts / "launch_pack.py"
    launch_script.write_text("# SYNTHETIC/FIXTURE authenticated source verifier\n", encoding="utf-8")
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
        "schema_version": 1,
        "service_user": "hyperlab",
        "services": _handoff()["services"],
        "source_commit": "b" * 40,
        "source_inventory_sha256": "c" * 64,
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


def _operator_handoff_for_git_bash(tmp_path: Path, suffix: str) -> dict[str, object]:
    incoming = tmp_path / f"incoming-{suffix}"
    source = tmp_path / f"source-{suffix}"
    campaign = tmp_path / f"campaign-{suffix}"
    volume = tmp_path / f"volume-{suffix}"
    incoming.mkdir()
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
        "printf 'sudo|%s\\n' \"$*\" >> \"$HYPERLAB_FAKE_LOG\"\n",
    )
    _write_fake_command(
        fake_bin,
        "git",
        """printf 'git|%s\\n' "$*" >> "$HYPERLAB_FAKE_LOG"
if [[ ${1:-} == clone && ${2:-} == --no-checkout ]]; then
  mkdir -p -- "$4"
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
 hashes={service:hashlib.sha256(open(os.path.join(incoming,'systemd',service),'rb').read()).hexdigest() for service in services.values()}
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
  rollback)
    [[ ${1:-} == */rollback.sh && ${2:-} == rollback ]] || exit 95
    printf 'PREDICTION_ROLLBACK_GREEN\\n'
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
            "HYPERLAB_FAKE_LOG": _git_bash_path(log),
            "HYPERLAB_REAL_PYTHON": _git_bash_path(Path(sys.executable)),
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
        timeout=30,
    )


def _internal_install_fixture(
    tmp_path: Path,
    *,
    suffix: str,
    eligible_venues: tuple[str, ...] = ("polymarket", "kalshi"),
) -> tuple[Path, dict[str, object], Path, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    handoff = _operator_handoff_for_git_bash(tmp_path, suffix)
    incoming = Path(str(handoff["incoming_root"]).replace("/c/", "C:/", 1))
    source = Path(str(handoff["source_root"]).replace("/c/", "C:/", 1))
    campaign = Path(str(handoff["campaign_root"]).replace("/c/", "C:/", 1))
    source.joinpath(".venv", "bin").mkdir(parents=True)
    source.joinpath("config", "research").mkdir(parents=True)
    source_ops = source / "ops" / "prediction_markets_launch_v1"
    source_ops.mkdir(parents=True)
    (source_ops / "install.sh").write_bytes((OPS / "install.sh").read_bytes())
    runtime = source / ".venv" / "bin" / "python"
    runtime.write_text(
        """#!/usr/bin/bash
set -Eeuo pipefail
if [[ ${1:-} == -I && ${2:-} == - && ${3:-} == */handoff.json && $# == 4 ]]; then
  "$HYPERLAB_REAL_PYTHON" -c 'import json,sys; d=json.load(open(sys.argv[1],encoding="utf-8")); print(d["source_root"]); print(d["campaign_root"]); print(d["source_commit"]); print(d["services"]["polymarket"]); print(d["services"]["kalshi"]); print(d["services"]["dashboard"])' "$3" | tr -d '\\015'
  exit "${PIPESTATUS[0]}"
fi
if [[ ${1:-} == -I && ${2:-} == - && ${3:-} == */host-preflight-report.json && $# == 3 ]]; then
  "$HYPERLAB_REAL_PYTHON" "$@" | tr -d '\\015'
  exit "${PIPESTATUS[0]}"
fi
if [[ ${1:-} == -I && ${2:-} == - && $# == 3 ]]; then
  exit 0
fi
if [[ ${1:-} == -I && ${2:-} == */preflight.py ]]; then
  exec "$HYPERLAB_REAL_PYTHON" "$HYPERLAB_INSTALL_PREFLIGHT_HELPER" "${@:3}"
fi
if [[ ${1:-} == -m && ${4:-} == prediction-prepare ]]; then
  output=''
  while (($#)); do
    if [[ $1 == --output-root ]]; then output=$2; break; fi
    shift
  done
  [[ -n $output ]]
  mkdir -p -- "$output"
  cp -- "$HYPERLAB_FIXTURE_MANIFEST" "$output/campaign-manifest.json"
  cp -- "$HYPERLAB_FIXTURE_MANIFEST_PIN" "$output/campaign-manifest.sha256"
  exit 0
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
    for service in services.values():
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


def _internal_install_environment(
    tmp_path: Path,
    *,
    handoff: dict[str, object],
    pack: Path,
    failed_service: str | None = None,
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
 hashes={service:hashlib.sha256(open(os.path.join(incoming,'systemd',service),'rb').read()).hexdigest() for service in services.values()}
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
        "sudo",
        """printf 'sudo|%s\n' "$*" >> "$HYPERLAB_FAKE_LOG"
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
if [[ ${1:-} != systemctl ]]; then exit 0; fi
shift
action=${1:-}; shift || true
if [[ $action == enable && ${1:-} == --now ]]; then
  service=$2
  if [[ -n ${HYPERLAB_FAIL_SERVICE:-} && $service == "$HYPERLAB_FAIL_SERVICE" ]]; then exit 1; fi
  : > "$HYPERLAB_SERVICE_STATE_DIR/$service"
  if [[ $service == "$HYPERLAB_DASHBOARD_SERVICE" ]]; then
    : > "$HYPERLAB_DASHBOARD_ENABLE_MARKER"
  fi
elif [[ $action == stop ]]; then
  for service in "$@"; do rm -f -- "$HYPERLAB_SERVICE_STATE_DIR/$service"; done
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
 required=name=='dashboard' or (not dashboard_only and name in eligible)
 properties={'ActiveState':'active' if active else 'inactive','ExecMainStatus':'0','FragmentPath':'/etc/systemd/system/'+service,'LoadState':'loaded','MainPID':'123' if active else '0','NRestarts':'0','SubState':'running' if active else 'dead'}
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
    environment.update(
        {
            "BASH_ENV": _git_bash_path(bash_environment),
            "HOME": "/home/hyperlab",
            "HYPERLAB_FAKE_LOG": _git_bash_path(log),
            "HYPERLAB_FAIL_SERVICE": failed_service or "",
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
    powershell = _powershell_51()
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
        assert "Environment=USER=hyperlab" in unit
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
    assert '! -L "$SOURCE_ROOT"' in install
    assert '! -L "$CAMPAIGN_ROOT"' in install
    assert (
        'bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/install.sh" '
        '"$INCOMING_ROOT"'
    ) in install
    assert 'bash "$INCOMING_ROOT/scripts/install.sh"' not in install
    assert "PREDICTION_INSTALL_ACTIVATION_GREEN" in install
    expected_monitor = (
        f"bash '{handoff['source_root']}/ops/prediction_markets_launch_v1/monitor.sh' "
        f"'{handoff['incoming_root']}/handoff.json'"
    )
    assert expected_monitor in monitor
    assert f"{handoff['incoming_root']}/scripts/monitor.sh" not in monitor
    assert "PREDICTION_MONITOR_TRANSITION_OR_ALERT" in monitor
    assert "$SshKeyRaw = $env:HYPERLAB_PM_SSH_KEY" in tunnel
    assert "Resolve-Path -LiteralPath $SshKeyRaw" in tunnel
    assert "ssh -i $SshKeyPath -N -o ExitOnForwardFailure=yes" in tunnel
    assert "127.0.0.1:18081:127.0.0.1:18081" in tunnel
    assert "recovery|rollback" in recovery
    rendered = "\n".join((windows, install, monitor, tunnel, recovery))
    assert "df -P --output" not in rendered
    assert "systemctl" not in windows
    assert "hyperlab-h1" not in rendered
    assert "18080" not in rendered


def test_rendered_operator_readme_is_attempt_bound_nonexpert_and_h1_safe() -> None:
    handoff = _handoff()
    handoff.update(
        {
            "run_slug": "pm-20260827t120000z-deadbeef",
            "source_commit": "b" * 40,
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

    failed_handoff = _operator_handoff_for_git_bash(tmp_path, "install-failed")
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

    green_handoff = _operator_handoff_for_git_bash(tmp_path, "install-green")
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

    server_holder: list[ThreadingHTTPServer] = []
    server_started = threading.Event()
    server_error: list[OSError] = []
    first_health_marker = tmp_path / "green" / "first-health-attempt"

    def delayed_dashboard() -> None:
        deadline = time.monotonic() + 10
        while not first_health_marker.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not first_health_marker.exists():
            server_error.append(OSError("first dashboard health attempt was not observed"))
            return
        time.sleep(1.0)
        try:
            server = ThreadingHTTPServer(("127.0.0.1", 18081), HealthHandler)
        except OSError as error:
            server_error.append(error)
            return
        server_holder.append(server)
        server_started.set()
        server.serve_forever()

    thread = threading.Thread(target=delayed_dashboard, daemon=True)
    thread.start()
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
        green_source = Path(str(handoff["source_root"]).replace("/c/", "C:/", 1))
        green = _run_git_bash_script(
            green_source / "ops" / "prediction_markets_launch_v1" / "install.sh",
            cwd=non_git_cwd,
            environment=environment,
            arguments=(_git_bash_path(incoming),),
        )
        assert green.returncode == 0, green.stderr
        assert server_started.is_set(), server_error
        assert any(
            signature in green.stderr
            for signature in (
                "ConnectionRefusedError",
                "WinError 10061",
                "Errno 111",
                "urlopen error timed out",
            )
        ), green.stderr
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
        dashboard_enable = f"sudo|systemctl enable --now {services['dashboard']}"
        polymarket_enable = f"sudo|systemctl enable --now {services['polymarket']}"
        kalshi_enable = f"sudo|systemctl enable --now {services['kalshi']}"
        assert green_log.index(dashboard_enable) < green_log.index(polymarket_enable)
        assert green_log.index(polymarket_enable) < green_log.index(kalshi_enable)

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
            "PREDICTION_INSTALL_ACTIVATION_PARTIAL_OR_ALERT"
        )
        failed_log = failed_log_path.read_text(encoding="utf-8")
        assert f"sudo|systemctl stop {failed_services['polymarket']}" in failed_log
        assert f"sudo|systemctl disable {failed_services['polymarket']}" in failed_log
        assert f"sudo|systemctl stop {failed_services['kalshi']}" not in failed_log
        assert f"sudo|systemctl stop {failed_services['dashboard']}" not in failed_log
    finally:
        if server_started.is_set():
            server_holder[0].shutdown()
            server_holder[0].server_close()
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
        assert f"sudo|systemctl enable --now {dashboard}" in log
        if failure_mode == "prepared-stale":
            for venue in ("polymarket", "kalshi"):
                service = str(services[venue])
                assert f"sudo|systemctl enable --now {service}" in log
                assert f"sudo|systemctl stop {service}" in log
                assert f"sudo|systemctl disable {service}" in log
            assert "PREDICTION_INSTALL_ACTIVATION_PARTIAL_OR_ALERT" in output
        else:
            for venue in ("polymarket", "kalshi"):
                assert f"sudo|systemctl enable --now {services[venue]}" not in log
            assert f"sudo|systemctl stop {dashboard}" in log
            assert f"sudo|systemctl disable {dashboard}" in log
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
            f"sudo|systemctl enable --now {unavailable_services['dashboard']}"
            in unavailable_log
        )
        for venue in ("polymarket", "kalshi"):
            assert (
                f"sudo|systemctl enable --now {unavailable_services[venue]}"
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
        assert f"sudo|systemctl enable --now {partial_services['kalshi']}" in partial_log
        assert (
            f"sudo|systemctl enable --now {partial_services['polymarket']}"
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
    assert completed.stdout.count("PREDICTION_MONITOR_TRANSITION_OR_ALERT") == 1
    assert completed.stdout.count('"alert":false') == 2
    lines = log.read_text(encoding="utf-8").splitlines()
    expected_monitor = (
        "bash|"
        f"{handoff['source_root']}/ops/prediction_markets_launch_v1/monitor.sh "
        f"{handoff['incoming_root']}/handoff.json"
    )
    assert lines.count(expected_monitor) == 2
    assert not any(
        line.startswith("bash|")
        and f"{handoff['incoming_root']}/scripts/monitor.sh" in line
        for line in lines
    )
    assert sum(line.startswith("python|-I -c ") for line in lines) == 2
    assert sum(line.startswith("sleep|10") for line in lines) == 1


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
    assert completed.stdout.count("PREDICTION_MONITOR_TRANSITION_OR_ALERT") == 1


@pytest.mark.parametrize(
    ("mode", "signal"),
    [
        ("recovery", "PREDICTION_RECOVERY_GREEN"),
        ("rollback", "PREDICTION_ROLLBACK_GREEN"),
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
    assert f"rollback.sh {mode} " in dispatches[0]
    assert "hyperlab-h1" not in dispatches[0]


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
        "sudo",
        """printf 'sudo|%s\n' "$*" >> "$HYPERLAB_FAKE_LOG"
[[ ${1:-} == systemctl ]] || exit 98
if [[ ${2:-} == show ]]; then printf 'inactive\n'; fi
if [[ ${2:-} == is-enabled ]]; then printf 'disabled\n'; fi
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
    for service in services.values():
        assert f"sudo|systemctl stop {service}" in systemd_log
        assert f"sudo|systemctl disable {service}" in systemd_log
    assert "hyperlab-h1" not in systemd_log
    assert "rm " not in systemd_log


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
            assert f"sudo|systemctl enable --now {service}" in green_log
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
        assert f"sudo|systemctl stop {partial_services['polymarket']}" in partial_log
        assert f"sudo|systemctl disable {partial_services['polymarket']}" in partial_log
        assert f"sudo|systemctl stop {partial_services['kalshi']}" not in partial_log
        assert f"sudo|systemctl stop {partial_services['dashboard']}" not in partial_log
        assert retried is not None and retried.returncode == 0, (
            None if retried is None else retried.stderr
        )
        assert retried.stdout.splitlines()[-1] == (
            "PREDICTION_RECOVERY_RESUME_REQUESTED_NO_SLOT_RETRY"
        )
        assert partial_log.count(
            f"sudo|systemctl enable --now {partial_services['kalshi']}"
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
            assert f"sudo|systemctl stop {dashboard}" in refused_log
            assert f"sudo|systemctl disable {dashboard}" in refused_log
            for venue in ("polymarket", "kalshi"):
                assert (
                    f"sudo|systemctl enable --now {refused_services[venue]}"
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
        assert f"sudo|systemctl stop {fragment_services['polymarket']}" in fragment_log
        assert f"sudo|systemctl disable {fragment_services['polymarket']}" in fragment_log

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
                f"sudo|systemctl enable --now {final_failure_services[venue]}"
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
            assert f"sudo|systemctl enable --now {service}" in public_invalid_log
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
  self.returncode=0; self.stderr=''
  self.stdout=f'LoadState=loaded\nActiveState={"active" if active else "inactive"}\nSubState={"running" if active else "dead"}\nMainPID={pid}\nNRestarts=0\nExecMainStatus=0\nFragmentPath={fragment}\n'
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


def test_post_bootstrap_install_admission_reauthenticates_units_and_live_margin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slug = "pm-20260827t235959z-feedface"
    incoming_parent = tmp_path / "home" / "incoming"
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
    unit_items: list[dict[str, object]] = []
    for service in services.values():
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
        "checks": {"filesystem": {"source": "/dev/sdb"}},
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
    available = 196_391_251_968
    ntp = "yes"
    filesystem_source = "/dev/sdb"
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
            return preflight.CommandResult(
                0,
                f"{volume_mount.as_posix()} {filesystem_source} ext4 rw,relatime,discard",
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
    assert green["install_admissible"] is True
    assert green["terminal_signal"] == "PREDICTION_INSTALL_ADMISSION_GREEN"
    live = green["evidence"]["live"]  # type: ignore[index]
    assert live["capacity"] == {  # type: ignore[index]
        "admitted": True,
        "available_bytes": available,
        "required_free_bytes": 194_347_270_144,
    }
    assert available - 194_347_270_144 == 2_043_981_824
    asserted_hashes = green["evidence"]["unit_sha256"]  # type: ignore[index]
    assert set(asserted_hashes) == set(services.values())
    install_admission_path = (
        volume_base / "campaigns" / slug / "state" / "install-admission-report.json"
    )
    install_admission_path.parent.mkdir(parents=True)
    install_admission_path.write_bytes(preflight.canonical_json_bytes(green) + b"\n")
    green_guard = preflight.collector_activation_guard(
        handoff_path,
        install_admission_path,
        run=live_command,
    )
    assert green_guard["activation_admissible"] is True
    startup_green = preflight.runner_startup_admission(
        handoff_path,
        install_admission_path,
        run=live_command,
    )
    assert startup_green["startup_admissible"] is True
    assert startup_green["checks"]["capacity"] == {  # type: ignore[index]
        "deferred_to_ledger_accounted_runner_gate": True
    }

    ntp = "no"
    startup_ntp_refused = preflight.runner_startup_admission(
        handoff_path,
        install_admission_path,
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

    filesystem_source = "/dev/sdc"
    startup_device_refused = preflight.runner_startup_admission(
        handoff_path,
        install_admission_path,
        run=live_command,
    )
    assert startup_device_refused["startup_admissible"] is False
    assert "filesystem device" in " ".join(  # type: ignore[arg-type]
        startup_device_refused["errors"]
    )
    refused_device = preflight.collector_activation_guard(
        handoff_path,
        install_admission_path,
        run=live_command,
    )
    assert refused_device["activation_admissible"] is False
    assert "filesystem device diverged" in " ".join(  # type: ignore[arg-type]
        refused_device["errors"]
    )
    filesystem_source = "/dev/sdb"

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
    assert "rm -rf" not in rollback + install
    assert "unlink" not in rollback + install
