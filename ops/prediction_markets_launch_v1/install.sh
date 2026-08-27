#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

fail() {
  printf 'PREDICTION_INSTALL_REFUSED:%s\n' "$1" >&2
  exit 4
}
trap 'fail "line=$LINENO exit=$?"' ERR

if (($# != 1)); then
  fail 'usage: install.sh INCOMING_ROOT'
fi
INCOMING_ROOT=$1
[[ $(id -un) == hyperlab ]] || fail 'run as hyperlab'
[[ $HOME == /home/hyperlab ]] || fail 'HOME must be /home/hyperlab'
[[ -d "$INCOMING_ROOT" && ! -L "$INCOMING_ROOT" ]] || fail 'incoming root is absent or unsafe'
[[ $(readlink -f -- "$INCOMING_ROOT") == "$INCOMING_ROOT" ]] || fail 'incoming root real path differs'
[[ -f "$INCOMING_ROOT/host-preflight-report.json" ]] || fail 'host preflight report is absent'
[[ -f "$INCOMING_ROOT/filesystem-fsync-report.json" ]] || fail 'filesystem fsync report is absent'

SCRIPT_PATH=$(readlink -f -- "${BASH_SOURCE[0]}") || fail 'install script path is unavailable'
[[ ! -L ${BASH_SOURCE[0]} ]] || fail 'install script must not be a symlink'
SCRIPT_ROOT=$(dirname -- "$SCRIPT_PATH")
TRUSTED_SOURCE_ROOT=$(readlink -f -- "$SCRIPT_ROOT/../..") || fail 'install source root is unavailable'
[[ -d "$TRUSTED_SOURCE_ROOT" && ! -L "$TRUSTED_SOURCE_ROOT" ]] || fail 'install source root is unsafe'
VENV_PYTHON="$TRUSTED_SOURCE_ROOT/.venv/bin/python"
[[ -f "$VENV_PYTHON" && ! -L "$VENV_PYTHON" && -x "$VENV_PYTHON" ]] || fail 'offline runtime is absent or unsafe'

mapfile -t VALUES < <("$VENV_PYTHON" -I - "$INCOMING_ROOT/handoff.json" "$TRUSTED_SOURCE_ROOT" <<'PY'
from pathlib import Path
import sys
source=Path(sys.argv[2])
sys.path[:0]=[str(source/'src'),str(source)]
from ops.prediction_markets_launch_v1.preflight import load_handoff,validate_install_layout
handoff_path=Path(sys.argv[1])
context=validate_install_layout(load_handoff(handoff_path),handoff_path=handoff_path,trusted_source_root=source)
services=context['services']
print(context['source_root']); print(context['campaign_root']); print(context['source_commit'])
print(services['polymarket']); print(services['kalshi']); print(services['dashboard'])
probes=context['namespace_probe_services']
print(probes['polymarket']); print(probes['kalshi'])
PY
)
(( ${#VALUES[@]} == 8 )) || fail 'authenticated handoff context is incomplete'
for VALUE_INDEX in "${!VALUES[@]}"; do
  VALUES[$VALUE_INDEX]=${VALUES[$VALUE_INDEX]%$'\r'}
done
SOURCE_ROOT=${VALUES[0]}
CAMPAIGN_ROOT=${VALUES[1]}
SOURCE_COMMIT=${VALUES[2]}
POLYMARKET_SERVICE=${VALUES[3]}
KALSHI_SERVICE=${VALUES[4]}
DASHBOARD_SERVICE=${VALUES[5]}
POLYMARKET_NAMESPACE_PROBE_SERVICE=${VALUES[6]}
KALSHI_NAMESPACE_PROBE_SERVICE=${VALUES[7]}
[[ $SOURCE_ROOT == "$TRUSTED_SOURCE_ROOT" ]] \
  || fail "authenticated source root diverged handoff=$SOURCE_ROOT script=$TRUSTED_SOURCE_ROOT"
[[ $(pwd -P) == "$SOURCE_ROOT" ]] || cd "$SOURCE_ROOT"
[[ $(git rev-parse HEAD) == "$SOURCE_COMMIT" ]] || fail 'source commit diverged'
[[ -z $(git status --porcelain) ]] || fail 'source checkout is not clean'
[[ ! -e "$CAMPAIGN_ROOT" && ! -L "$CAMPAIGN_ROOT" ]] || fail 'campaign root must be new'
export HOME=/home/hyperlab
export PYTHONPATH="$SOURCE_ROOT/src:$SOURCE_ROOT"
export PYTHONNOUSERSITE=1
export PYTHONDONTWRITEBYTECODE=1
export PYTHONUNBUFFERED=1
export TZ=UTC

if ! "$VENV_PYTHON" -I - "$SOURCE_ROOT" <<'PY'
from pathlib import Path
import sys
source=Path(sys.argv[1]); sys.path[:0]=[str(source/'src'),str(source)]
from hyperlab.research_data.envelope import Venue
from ops.prediction_markets_launch_v1 import cockpit,runner
required=(cockpit._validate_venue_state,cockpit.active_optional_service_is_admissible,cockpit.classify_monitored_service,cockpit.complete_service_is_admissible,cockpit.prepared_state_is_stale,cockpit.validate_activation_evidence,runner.read_ledger,runner.validate_service_ledger_against_manifest)
if not Venue or not all(callable(value) for value in required):
 raise SystemExit('monitor-runtime-helper-self-check:required-symbol-unavailable')
PY
then
  fail 'monitor runtime helper import self-check failed before campaign preparation'
fi

START_AT_UTC=${HYPERLAB_PM_START_AT_UTC:-}
if [[ -z $START_AT_UTC ]]; then
  START_AT_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)
else
  if ! "$VENV_PYTHON" -I - "$START_AT_UTC" <<'PY'
from datetime import UTC,datetime,timedelta
import sys
try:
 value=datetime.fromisoformat(sys.argv[1].replace('Z','+00:00'))
except ValueError as error:
 raise SystemExit(f'explicit-start-at:invalid-iso8601:{error}') from error
if value.tzinfo is None or value.utcoffset() is None:
 raise SystemExit('explicit-start-at:timezone-required')
now=datetime.now(UTC)
if value.astimezone(UTC) < now-timedelta(seconds=5):
 raise SystemExit('explicit-start-at:already-stale')
if value.astimezone(UTC) > now+timedelta(days=1):
 raise SystemExit('explicit-start-at:too-far-in-future')
PY
  then
    fail 'explicit start_at UTC validation failed before campaign preparation'
  fi
fi
CAMPAIGN_ID=$(basename -- "$CAMPAIGN_ROOT")
"$VENV_PYTHON" -m hyperlab research-data prediction-prepare \
  --output-root "$CAMPAIGN_ROOT" \
  --campaign-id "$CAMPAIGN_ID" \
  --starts-at-utc "$START_AT_UTC" \
  --polymarket-contract "$SOURCE_ROOT/config/research/polymarket-public-contract-v1.json" \
  --kalshi-contract "$SOURCE_ROOT/config/research/kalshi-public-contract-v1.json" \
  --candidate-config "$SOURCE_ROOT/config/research/prediction-markets-candidate-v1.json"
install -d -m 0700 "$CAMPAIGN_ROOT/state" "$CAMPAIGN_ROOT/polymarket" "$CAMPAIGN_ROOT/kalshi"
install -m 0600 "$INCOMING_ROOT/host-preflight-report.json" "$CAMPAIGN_ROOT/state/preflight-report.json"
install -m 0600 "$INCOMING_ROOT/filesystem-fsync-report.json" "$CAMPAIGN_ROOT/state/filesystem-fsync-report.json"

"$VENV_PYTHON" - "$INCOMING_ROOT/handoff.json" "$INCOMING_ROOT/host-preflight-report.json" "$CAMPAIGN_ROOT/state/activation-receipt.json" "$CAMPAIGN_ROOT/campaign-manifest.json" <<'PY'
from datetime import UTC,datetime
from pathlib import Path
import hashlib,json,os,stat,sys
handoff=json.load(open(sys.argv[1],encoding='utf-8'))
preflight_path=Path(sys.argv[2]); before=preflight_path.lstat()
if preflight_path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size>8*1024*1024:
 raise ValueError('initial preflight report is unsafe')
preflight_raw=preflight_path.read_bytes(); after=preflight_path.lstat()
if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns) or len(preflight_raw)!=before.st_size:
 raise ValueError('initial preflight report changed during activation')
preflight=json.loads(preflight_raw.decode('utf-8'))
canonical=lambda v: json.dumps(v,ensure_ascii=False,allow_nan=False,separators=(',',':'),sort_keys=True).encode()
def bounded(path,maximum):
 before=path.lstat()
 if path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size>maximum:
  raise ValueError(f'unsafe or oversized activation input:{path}')
 raw=path.read_bytes(); after=path.lstat()
 if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns) or len(raw)!=before.st_size:
  raise ValueError(f'activation input changed during read:{path}')
 return raw
manifest_path=Path(sys.argv[4])
manifest_raw=bounded(manifest_path,8*1024*1024)
manifest=json.loads(manifest_raw.decode('utf-8'))
pin=bounded(manifest_path.with_suffix('.sha256'),256).decode('ascii').strip().split()
claimed_manifest=manifest.get('manifest_sha256') if isinstance(manifest,dict) else None
manifest_body={key:value for key,value in manifest.items() if key!='manifest_sha256'} if isinstance(manifest,dict) else {}
if (
 not isinstance(claimed_manifest,str) or len(claimed_manifest)!=64
 or any(character not in '0123456789abcdef' for character in claimed_manifest)
 or manifest_raw!=canonical(manifest)+b'\n'
 or hashlib.sha256(canonical(manifest_body)).hexdigest()!=claimed_manifest
 or len(pin)!=2 or pin[1]!='campaign-manifest.json'
 or hashlib.sha256(manifest_raw).hexdigest()!=pin[0]
):
 raise ValueError('campaign manifest authentication failed during activation')
body={
 'boundary':'PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY',
 'campaign_id':manifest['campaign_id'],
 'campaign_manifest_sha256':claimed_manifest,
 'campaign_root':handoff['campaign_root'],
 'dashboard_port':handoff['dashboard_port'],
 'eligible_venues':preflight['eligible_venues'],
 'economic_evidence_status':'ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE',
 'h1_actions':'NONE',
 'preflight_report_sha256':hashlib.sha256(preflight_raw).hexdigest(),
 'quick_start':os.environ.get('HYPERLAB_PM_START_AT_UTC') is None,
 'recorded_at_utc':datetime.now(UTC).isoformat(timespec='microseconds').replace('+00:00','Z'),
 'schema_version':1,
 'source_commit':handoff['source_commit'],
 'starts_at_utc':manifest['starts_at_utc'],
}
value={**body,'receipt_sha256':hashlib.sha256(canonical(body)).hexdigest()}
path=sys.argv[3]
fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'wb') as handle:
 handle.write(canonical(value)+b'\n'); handle.flush(); os.fsync(handle.fileno())
PY

INSTALL_ADMISSION_REPORT="$CAMPAIGN_ROOT/state/install-admission-report.json"
"$VENV_PYTHON" -I "$SOURCE_ROOT/ops/prediction_markets_launch_v1/preflight.py" install-admission \
  --handoff "$INCOMING_ROOT/handoff.json" \
  --host-report "$CAMPAIGN_ROOT/state/preflight-report.json" \
  --fsync-report "$CAMPAIGN_ROOT/state/filesystem-fsync-report.json" \
  --report "$INSTALL_ADMISSION_REPORT" \
  || fail 'post-bootstrap install admission refused before any systemd mutation'
mapfile -t EXPECTED_UNIT_SHA256 < <("$VENV_PYTHON" -I - "$INSTALL_ADMISSION_REPORT" "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" "$DASHBOARD_SERVICE" "$POLYMARKET_NAMESPACE_PROBE_SERVICE" "$KALSHI_NAMESPACE_PROBE_SERVICE" <<'PY'
import json,stat,sys
from pathlib import Path
path=Path(sys.argv[1]); before=path.lstat()
if path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size>8*1024*1024: raise ValueError('install admission report is unsafe')
raw=path.read_bytes(); after=path.lstat()
if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns) or len(raw)!=before.st_size: raise ValueError('install admission report changed during read')
value=json.loads(raw)
if value.get('install_admissible') is not True or value.get('terminal_signal')!='PREDICTION_INSTALL_ADMISSION_GREEN': raise ValueError('install admission report is not green')
unit_sha=value.get('evidence',{}).get('unit_sha256',{})
for service in sys.argv[2:]:
 digest=unit_sha.get(service)
 if not isinstance(digest,str) or len(digest)!=64 or any(character not in '0123456789abcdef' for character in digest): raise ValueError('authenticated unit hash is invalid')
 print(digest)
PY
)
(( ${#EXPECTED_UNIT_SHA256[@]} == 5 )) || fail 'authenticated unit hashes are incomplete'
for UNIT_HASH_INDEX in "${!EXPECTED_UNIT_SHA256[@]}"; do
  EXPECTED_UNIT_SHA256[$UNIT_HASH_INDEX]=${EXPECTED_UNIT_SHA256[$UNIT_HASH_INDEX]%$'\r'}
done

UNIT_INDEX=0
for SERVICE in "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" "$DASHBOARD_SERVICE" "$POLYMARKET_NAMESPACE_PROBE_SERVICE" "$KALSHI_NAMESPACE_PROBE_SERVICE"; do
  UNIT_SOURCE="$INCOMING_ROOT/systemd/$SERVICE"
  UNIT_TARGET="/etc/systemd/system/$SERVICE"
  UNIT_TEMP="/etc/systemd/system/.$SERVICE.$SOURCE_COMMIT.tmp"
  [[ -f "$UNIT_SOURCE" && ! -L "$UNIT_SOURCE" ]] || fail "rendered unit absent: $SERVICE"
  sudo test ! -e "$UNIT_TARGET"
  sudo test ! -L "$UNIT_TARGET"
  sudo test ! -e "$UNIT_TEMP"
  sudo test ! -L "$UNIT_TEMP"
  [[ $(sha256sum -- "$UNIT_SOURCE" | awk '{print $1}') == "${EXPECTED_UNIT_SHA256[$UNIT_INDEX]}" ]] \
    || fail "rendered unit hash diverged before privileged copy: $SERVICE"
  systemd-analyze verify "$UNIT_SOURCE"
  UNIT_INDEX=$((UNIT_INDEX + 1))
done
UNIT_INDEX=0
for SERVICE in "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" "$DASHBOARD_SERVICE" "$POLYMARKET_NAMESPACE_PROBE_SERVICE" "$KALSHI_NAMESPACE_PROBE_SERVICE"; do
  UNIT_SOURCE="$INCOMING_ROOT/systemd/$SERVICE"
  UNIT_TARGET="/etc/systemd/system/$SERVICE"
  UNIT_TEMP="/etc/systemd/system/.$SERVICE.$SOURCE_COMMIT.tmp"
  EXPECTED_UNIT_SHA=${EXPECTED_UNIT_SHA256[$UNIT_INDEX]}
  sudo install -o root -g root -m 0644 "$UNIT_SOURCE" "$UNIT_TEMP" \
    || fail "privileged unit staging failed: $SERVICE"
  if [[ $(sudo sha256sum -- "$UNIT_TEMP" | awk '{print $1}') != "$EXPECTED_UNIT_SHA" ]]; then
    sudo rm -- "$UNIT_TEMP" || true
    fail "privileged staged unit hash diverged: $SERVICE"
  fi
  if ! sudo ln "$UNIT_TEMP" "$UNIT_TARGET"; then
    sudo rm -- "$UNIT_TEMP" || true
    fail "privileged unit publication failed: $SERVICE"
  fi
  if [[ $(sudo sha256sum -- "$UNIT_TARGET" | awk '{print $1}') != "$EXPECTED_UNIT_SHA" ]] \
    || ! systemd-analyze verify "$UNIT_TARGET"; then
    sudo rm -- "$UNIT_TARGET" "$UNIT_TEMP" || true
    fail "published unit authentication failed: $SERVICE"
  fi
  sudo rm -- "$UNIT_TEMP" || fail "privileged unit staging cleanup failed: $SERVICE"
  UNIT_INDEX=$((UNIT_INDEX + 1))
done
sudo systemctl daemon-reload

cleanup_prediction_services() {
  local cleanup_errors=0 service active_state enabled_state
  for service in \
    "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" "$DASHBOARD_SERVICE" \
    "$POLYMARKET_NAMESPACE_PROBE_SERVICE" "$KALSHI_NAMESPACE_PROBE_SERVICE"; do
    sudo systemctl stop "$service" || cleanup_errors=1
    sudo systemctl disable "$service" || cleanup_errors=1
  done
  for service in \
    "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" "$DASHBOARD_SERVICE" \
    "$POLYMARKET_NAMESPACE_PROBE_SERVICE" "$KALSHI_NAMESPACE_PROBE_SERVICE"; do
    active_state=$(timeout 5 systemctl show "$service" --property=ActiveState --value --no-pager 2>&1) \
      || cleanup_errors=1
    case "$active_state" in
      inactive|failed) ;;
      *) cleanup_errors=1 ;;
    esac
    if enabled_state=$(timeout 5 systemctl is-enabled "$service" 2>&1); then
      [[ $enabled_state == disabled ]] || cleanup_errors=1
    else
      [[ $enabled_state == disabled ]] || cleanup_errors=1
    fi
  done
  return "$cleanup_errors"
}

if ! sudo systemctl enable --now "$DASHBOARD_SERVICE"; then
  cleanup_prediction_services || fail 'dashboard activation failed and Prediction Markets cleanup also failed'
  fail 'dashboard activation failed before any venue start'
fi

DASHBOARD_READY=no
DASHBOARD_LAST_MONITOR=''
for _attempt in {1..20}; do
  if "$VENV_PYTHON" - <<'PY' && \
     DASHBOARD_MONITOR=$(bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/monitor.sh" "$INCOMING_ROOT/handoff.json" dashboard-only) && \
     DASHBOARD_MONITOR="$DASHBOARD_MONITOR" "$VENV_PYTHON" - <<'PY'
import json,urllib.request
def require(condition,label):
 if not condition: raise SystemExit('dashboard-live:'+label)
with urllib.request.urlopen('http://127.0.0.1:18081/health/live',timeout=1) as response:
 raw=response.read(65537)
 require(len(raw)<=65536,'payload-size')
 value=json.loads(raw)
 require(response.status==200,'http-status')
 require(value.get('status')=='alive','live-status')
 require(value.get('mode')=='readonly','readonly-mode')
 require(value.get('orders_enabled') is False,'orders-disabled')
PY
import json,os
value=json.loads(os.environ['DASHBOARD_MONITOR'])
def require(condition,label):
 if not condition: raise SystemExit('dashboard-monitor:'+label)
require(value.get('preflight_error') is None,'preflight-error')
require(value.get('failure_class') is None,'failure-class')
require(value.get('activation_admissible') is True,'activation-admissible')
dashboard=value['services']['dashboard']
properties=dashboard['properties']
require(properties.get('ActiveState')=='active','active-state')
require(int(properties.get('MainPID','0') or '0')>0,'main-pid')
require(dashboard.get('command_verified') is True,'command')
require(dashboard.get('fragment_verified') is True,'fragment')
require(dashboard.get('listener_verified') is True,'listener')
for venue in ('polymarket','kalshi'):
 service=value['services'][venue]
 require(service['properties'].get('ActiveState')!='active',venue+'-inactive')
 require(int(service['properties'].get('MainPID','0') or '0')==0,venue+'-main-pid')
PY
  then
    DASHBOARD_READY=yes
    break
  fi
  DASHBOARD_LAST_MONITOR=${DASHBOARD_MONITOR:-}
  sleep 0.5
done
if [[ $DASHBOARD_READY != yes ]]; then
  if [[ -n $DASHBOARD_LAST_MONITOR ]]; then
    DASHBOARD_LAST_MONITOR="$DASHBOARD_LAST_MONITOR" "$VENV_PYTHON" -I -c 'import json,os; d=json.loads(os.environ["DASHBOARD_LAST_MONITOR"]); print("PREDICTION_DASHBOARD_READINESS_DIAGNOSTIC="+json.dumps({"failure_class":d.get("failure_class"),"preflight_error":d.get("preflight_error"),"dashboard":d.get("services",{}).get("dashboard")},ensure_ascii=False,separators=(",",":"),sort_keys=True)[:4096])' >&2 \
      || printf 'PREDICTION_DASHBOARD_READINESS_DIAGNOSTIC=UNPARSEABLE_MONITOR_OUTPUT\n' >&2
  else
    printf 'PREDICTION_DASHBOARD_READINESS_DIAGNOSTIC=NO_MONITOR_JSON\n' >&2
  fi
  cleanup_prediction_services || fail 'dashboard readiness failed and Prediction Markets cleanup also failed'
  fail 'dashboard loopback and command readiness did not become green'
fi

COLLECTOR_GUARD_REPORT="$CAMPAIGN_ROOT/state/collector-activation-guard.json"
if ! "$VENV_PYTHON" -I "$SOURCE_ROOT/ops/prediction_markets_launch_v1/preflight.py" collector-activation-guard \
  --handoff "$INCOMING_ROOT/handoff.json" \
  --install-admission-report "$INSTALL_ADMISSION_REPORT" \
  --report "$COLLECTOR_GUARD_REPORT"; then
  cleanup_prediction_services || fail 'collector guard refused and Prediction Markets cleanup also failed'
  fail 'collector activation guard refused; enlarge or choose another ext4 volume if capacity is insufficient'
fi

if ! ELIGIBLE_RAW=$(DASHBOARD_MONITOR="$DASHBOARD_MONITOR" "$VENV_PYTHON" -I - <<'PY'
import json,os
value=json.loads(os.environ['DASHBOARD_MONITOR'])
def require(condition,label):
 if not condition: raise SystemExit('eligible-venues:'+label)
require(value.get('preflight_error') is None,'preflight-error')
require(value.get('failure_class') is None,'failure-class')
require(value.get('operational_failure') is False,'operational-failure')
require(value.get('activation_admissible') is True,'activation-admissible')
eligible=value.get('eligible_venues')
require(isinstance(eligible,list),'list')
require(0<=len(eligible)<=2 and len(set(eligible))==len(eligible),'cardinality')
require(all(item in {'polymarket','kalshi'} for item in eligible),'allowlist')
for venue in eligible: print(venue)
PY
); then
  cleanup_prediction_services || fail 'eligible venue authentication failed and Prediction Markets cleanup also failed'
  fail 'authenticated eligible venue parsing failed before any collector start'
fi
ELIGIBLE=()
if [[ -n $ELIGIBLE_RAW ]]; then
  mapfile -t ELIGIBLE <<<"$ELIGIBLE_RAW"
  for ELIGIBLE_INDEX in "${!ELIGIBLE[@]}"; do
    ELIGIBLE[$ELIGIBLE_INDEX]=${ELIGIBLE[$ELIGIBLE_INDEX]%$'\r'}
  done
fi

namespace_probe_diagnostic() {
  local venue=$1 service=$2 properties journal
  properties=$(timeout 5 systemctl show "$service" \
    --property=LoadState,ActiveState,SubState,Result,MainPID,NRestarts,ExecMainCode,ExecMainStatus,FragmentPath \
    --no-pager 2>&1) || properties='SYSTEMCTL_SHOW_UNAVAILABLE'
  properties=${properties:0:4096}
  journal=$( { timeout 5 sudo journalctl --unit "$service" --no-pager -n 20 -o cat 2>&1 || true; } | head -c 4096 ) \
    || journal='JOURNAL_UNAVAILABLE'
  [[ -n $journal ]] || journal='JOURNAL_UNAVAILABLE'
  VENUE="$venue" SERVICE="$service" PROPERTIES="$properties" JOURNAL="$journal" \
    "$VENV_PYTHON" -I - <<'PY' >&2 \
    || printf 'PREDICTION_NAMESPACE_PROBE_DIAGNOSTIC=ENCODING_FAILED\n' >&2
import json,os
value={
 "journal":os.environ["JOURNAL"][:4096],
 "properties":os.environ["PROPERTIES"][:4096],
 "service":os.environ["SERVICE"],
 "venue":os.environ["VENUE"],
}
print("PREDICTION_NAMESPACE_PROBE_DIAGNOSTIC="+json.dumps(value,ensure_ascii=False,separators=(",",":"),sort_keys=True))
PY
  return 0
}

for VENUE in "${ELIGIBLE[@]}"; do
  case "$VENUE" in
    polymarket) NAMESPACE_PROBE_SERVICE=$POLYMARKET_NAMESPACE_PROBE_SERVICE ;;
    kalshi) NAMESPACE_PROBE_SERVICE=$KALSHI_NAMESPACE_PROBE_SERVICE ;;
    *)
      cleanup_prediction_services \
        || fail 'eligible venue invalid and Prediction Markets cleanup also failed'
      fail 'preflight eligible venue is invalid'
      ;;
  esac
  if ! sudo systemctl start "$NAMESPACE_PROBE_SERVICE"; then
    namespace_probe_diagnostic "$VENUE" "$NAMESPACE_PROBE_SERVICE"
    cleanup_prediction_services \
      || fail 'namespace probe refused and Prediction Markets cleanup also failed'
    fail "collector namespace probe refused before runner activation: $VENUE"
  fi
  NAMESPACE_PROBE_PROPERTIES=$(timeout 5 systemctl show "$NAMESPACE_PROBE_SERVICE" \
    --property=LoadState,ActiveState,SubState,Result,MainPID,NRestarts,ExecMainCode,ExecMainStatus,FragmentPath \
    --no-pager) || {
      namespace_probe_diagnostic "$VENUE" "$NAMESPACE_PROBE_SERVICE"
      cleanup_prediction_services \
        || fail 'namespace probe properties unavailable and Prediction Markets cleanup also failed'
      fail "collector namespace probe properties unavailable: $VENUE"
    }
  if ! NAMESPACE_PROBE_PROPERTIES="$NAMESPACE_PROBE_PROPERTIES" NAMESPACE_PROBE_SERVICE="$NAMESPACE_PROBE_SERVICE" "$VENV_PYTHON" -I - <<'PY'
import os
properties={}
for line in os.environ['NAMESPACE_PROBE_PROPERTIES'].splitlines():
 if '=' not in line: raise SystemExit('namespace-probe:malformed-property')
 key,value=line.split('=',1); properties[key]=value
def require(condition,label):
 if not condition: raise SystemExit('namespace-probe:'+label)
require(properties.get('LoadState')=='loaded','load-state')
require(properties.get('ActiveState')=='inactive','active-state')
require(properties.get('SubState')=='dead','sub-state')
require(properties.get('Result')=='success','result')
require(properties.get('MainPID')=='0','main-pid')
require(properties.get('NRestarts')=='0','restart-count')
require(properties.get('ExecMainCode')=='1','exit-code-kind')
require(properties.get('ExecMainStatus')=='0','exit-status')
require(properties.get('FragmentPath')=='/etc/systemd/system/'+os.environ['NAMESPACE_PROBE_SERVICE'],'fragment')
PY
  then
    namespace_probe_diagnostic "$VENUE" "$NAMESPACE_PROBE_SERVICE"
    cleanup_prediction_services \
      || fail 'namespace probe result diverged and Prediction Markets cleanup also failed'
    fail "collector namespace probe result diverged before runner activation: $VENUE"
  fi
  printf 'PREDICTION_NAMESPACE_PROBE_GREEN=%s\n' "$VENUE"
done

STARTED_VENUES=()
collector_ready() {
  local venue=$1 monitor_json
  monitor_json=$(bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/monitor.sh" "$INCOMING_ROOT/handoff.json") || return 1
  CAMPAIGN_ROOT="$CAMPAIGN_ROOT" MONITOR_JSON="$monitor_json" "$VENV_PYTHON" - "$venue" <<'PY'
from datetime import UTC,datetime
import json,os,sys
def require(condition,label):
 if not condition:
  print('PREDICTION_COLLECTOR_READINESS_INVARIANT='+label,file=sys.stderr)
  raise SystemExit(1)
try:
 value=json.loads(os.environ['MONITOR_JSON'])
 require(isinstance(value,dict),'monitor-object')
 require(value.get('preflight_error') is None,'monitor-preflight-error')
 require(value.get('failure_class') is None,'monitor-failure-class')
 venue=sys.argv[1]
 services=value.get('services'); require(isinstance(services,dict),'services-object')
 service=services.get(venue); require(isinstance(service,dict),'venue-service-object')
 properties=service.get('properties'); require(isinstance(properties,dict),'service-properties-object')
 state=service.get('state'); require(isinstance(state,dict),'state-object-present')
 require(service.get('admission_required') is True,'admission-required')
 lifecycle=state.get('lifecycle')
 require(lifecycle not in {None,'CAPACITY_REFUSED','INTEGRITY_FAILED','INTERRUPTED_RECOVERABLE'},'admissible-lifecycle')
 if service.get('venue_status')=='COMPLETE_WINDOW':
  require(lifecycle=='COMPLETE_WINDOW','complete-lifecycle')
  require(service.get('fragment_verified') is True,'complete-fragment')
  require(properties.get('ActiveState') in {'active','inactive'},'complete-active-state')
  if properties.get('ActiveState')=='inactive':
   require(properties.get('SubState')=='dead','complete-sub-state')
   require(int(properties.get('MainPID','0') or '0')==0,'complete-main-pid')
   require(properties.get('ExecMainStatus')=='0','complete-exit-status')
 else:
  require(service.get('venue_status') in {'RUNNING','PUBLIC_SOURCE_INVALID','PUBLIC_SOURCE_UNAVAILABLE_RUNTIME'},'venue-status')
  require(properties.get('ActiveState')=='active','active-state')
  require(int(properties.get('MainPID','0') or '0')>0,'main-pid')
  require(service.get('command_verified') is True,'command')
  require(service.get('fragment_verified') is True,'fragment')
 if lifecycle=='PREPARED':
  with open(os.path.join(os.environ['CAMPAIGN_ROOT'],'state','activation-receipt.json'),encoding='utf-8') as handle: activation=json.load(handle)
  require(activation.get('quick_start') is False,'prepared-explicit-start')
  starts=datetime.fromisoformat(str(activation.get('starts_at_utc')).replace('Z','+00:00'))
  require(starts.astimezone(UTC)>datetime.now(UTC),'prepared-future-start')
except (KeyError,TypeError,ValueError,json.JSONDecodeError) as error:
 print('PREDICTION_COLLECTOR_READINESS_INVARIANT=parse:'+type(error).__name__+':'+str(error)[:512],file=sys.stderr)
 raise SystemExit(1)
PY
}

collector_terminal() {
  local service=$1 properties
  properties=$(timeout 5 systemctl show "$service" \
    --property=LoadState,ActiveState,SubState,Result,MainPID,NRestarts,ExecMainCode,ExecMainStatus,FragmentPath \
    --no-pager 2>/dev/null) || return 1
  PROPERTIES="$properties" "$VENV_PYTHON" -I - <<'PY'
import os
p={}
for line in os.environ['PROPERTIES'].splitlines():
 if '=' in line:
  key,value=line.split('=',1); p[key]=value
terminal=(
 p.get('ActiveState')=='failed'
 or p.get('SubState')=='failed'
 or p.get('ExecMainStatus')=='4'
 or p.get('Result') not in {None,'','success'}
 or (
  p.get('ActiveState')=='inactive'
  and p.get('SubState')=='dead'
  and p.get('MainPID')=='0'
 )
)
raise SystemExit(0 if terminal else 1)
PY
}

collector_readiness_diagnostic() {
  local venue=$1 service=$2 properties journal monitor_json state_present=false ledger_present=false
  properties=$(timeout 5 systemctl show "$service" \
    --property=LoadState,ActiveState,SubState,Result,MainPID,NRestarts,ExecMainCode,ExecMainStatus,FragmentPath \
    --no-pager 2>&1) || properties='SYSTEMCTL_SHOW_UNAVAILABLE'
  properties=${properties:0:4096}
  monitor_json=$( { timeout 10 bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/monitor.sh" "$INCOMING_ROOT/handoff.json" 2>&1 || true; } | head -c 4096 ) \
    || monitor_json='MONITOR_UNAVAILABLE'
  [[ -n $monitor_json ]] || monitor_json='MONITOR_UNAVAILABLE'
  journal=$( { timeout 5 sudo journalctl --unit "$service" --no-pager -n 20 -o cat 2>&1 || true; } | head -c 4096 ) \
    || journal='JOURNAL_UNAVAILABLE'
  [[ -n $journal ]] || journal='JOURNAL_UNAVAILABLE'
  [[ -f "$CAMPAIGN_ROOT/$venue/state.json" && ! -L "$CAMPAIGN_ROOT/$venue/state.json" ]] \
    && state_present=true
  [[ -f "$CAMPAIGN_ROOT/$venue/ledger.jsonl" && ! -L "$CAMPAIGN_ROOT/$venue/ledger.jsonl" ]] \
    && ledger_present=true
  VENUE="$venue" SERVICE="$service" PROPERTIES="$properties" JOURNAL="$journal" \
    MONITOR_JSON="$monitor_json" STATE_PRESENT="$state_present" LEDGER_PRESENT="$ledger_present" \
    "$VENV_PYTHON" -I - <<'PY' >&2 \
    || printf 'PREDICTION_COLLECTOR_READINESS_DIAGNOSTIC=ENCODING_FAILED\n' >&2
import json,os
value={
 'journal':os.environ['JOURNAL'][:4096],
 'ledger_present':os.environ['LEDGER_PRESENT']=='true',
 'monitor_json':os.environ['MONITOR_JSON'][:4096],
 'properties':os.environ['PROPERTIES'][:4096],
 'service':os.environ['SERVICE'],
 'state_present':os.environ['STATE_PRESENT']=='true',
 'venue':os.environ['VENUE'],
}
print('PREDICTION_COLLECTOR_READINESS_DIAGNOSTIC='+json.dumps(value,ensure_ascii=False,separators=(',',':'),sort_keys=True))
PY
  return 0
}

for VENUE in "${ELIGIBLE[@]}"; do
  case "$VENUE" in
    polymarket) SERVICE=$POLYMARKET_SERVICE ;;
    kalshi) SERVICE=$KALSHI_SERVICE ;;
    *) fail 'preflight eligible venue is invalid' ;;
  esac
  if ! sudo systemctl enable --now "$SERVICE"; then
    collector_readiness_diagnostic "$VENUE" "$SERVICE"
    cleanup_prediction_services \
      || fail 'collector activation failed and Prediction Markets cleanup also failed'
    fail "collector activation failed before readiness: $VENUE"
  fi
  VENUE_READY=no
  for _attempt in {1..20}; do
    if collector_ready "$VENUE"; then
      VENUE_READY=yes
      break
    fi
    if collector_terminal "$SERVICE"; then
      printf 'PREDICTION_COLLECTOR_TERMINAL_BEFORE_READINESS=%s\n' "$VENUE" >&2
      break
    fi
    sleep 0.5
  done
  if [[ $VENUE_READY == yes ]]; then
    STARTED_VENUES+=("$VENUE")
  else
    collector_readiness_diagnostic "$VENUE" "$SERVICE"
    cleanup_prediction_services \
      || fail 'collector readiness failed and Prediction Markets cleanup also failed'
    fail "collector readiness failed before authenticated state: $VENUE"
  fi
done

ACTIVATION_READY=no
for _attempt in {1..20}; do
  if "$VENV_PYTHON" - <<'PY' && \
     MONITOR_JSON=$(bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/monitor.sh" "$INCOMING_ROOT/handoff.json") && \
     CAMPAIGN_ROOT="$CAMPAIGN_ROOT" MONITOR_JSON="$MONITOR_JSON" "$VENV_PYTHON" - "${ELIGIBLE[@]}" <<'PY'
import json,urllib.request
def require(condition,label):
 if not condition: raise SystemExit('final-dashboard:'+label)
with urllib.request.urlopen('http://127.0.0.1:18081/health/ready',timeout=1) as response:
 raw=response.read(65537)
 require(len(raw)<=65536,'payload-size')
 value=json.loads(raw)
 require(response.status==200 and value.get('status')=='ready','ready-status')
 require(value.get('mode')=='readonly' and value.get('orders_enabled') is False,'readonly-orders')
PY
from datetime import UTC,datetime
import json,os,sys
value=json.loads(os.environ['MONITOR_JSON'])
activation=json.load(open(os.path.join(os.environ['CAMPAIGN_ROOT'],'state','activation-receipt.json'),encoding='utf-8'))
def require(condition,label):
 if not condition: raise SystemExit('final-monitor:'+label)
require(value.get('preflight_error') is None,'preflight-error')
require(value.get('failure_class') is None,'failure-class')
require(value.get('activation_admissible') is True,'activation-admissible')
required={'dashboard',*sys.argv[1:]}
for name in required:
 service=value['services'][name]
 properties=service['properties']
 if name == 'dashboard':
  require(properties.get('ActiveState')=='active','dashboard-active')
  require(int(properties.get('MainPID','0') or '0') > 0,'dashboard-main-pid')
  require(service.get('command_verified') is True,'dashboard-command')
  require(service.get('fragment_verified') is True,'dashboard-fragment')
  require(service.get('listener_verified') is True,'dashboard-listener')
  continue
 state=service['state']
 require(isinstance(state,dict) and state.get('lifecycle') is not None,name+'-state')
 require(state.get('lifecycle') not in {'CAPACITY_REFUSED','INTEGRITY_FAILED','INTERRUPTED_RECOVERABLE'},name+'-lifecycle')
 if service.get('venue_status')=='COMPLETE_WINDOW':
  require(state.get('lifecycle')=='COMPLETE_WINDOW',name+'-complete')
  require(service.get('fragment_verified') is True,name+'-fragment')
 else:
  require(properties.get('ActiveState')=='active',name+'-active')
  require(int(properties.get('MainPID','0') or '0') > 0,name+'-main-pid')
  require(service.get('command_verified') is True,name+'-command')
  require(service.get('fragment_verified') is True,name+'-fragment')
 if state.get('lifecycle')=='PREPARED':
  require(activation.get('quick_start') is False,name+'-prepared-explicit-start')
  starts=datetime.fromisoformat(activation['starts_at_utc'].replace('Z','+00:00'))
  require(starts.astimezone(UTC) > datetime.now(UTC),name+'-prepared-future-start')
PY
  then
    ACTIVATION_READY=yes
    break
  fi
  FINAL_TERMINAL=no
  for VENUE in "${ELIGIBLE[@]}"; do
    case "$VENUE" in
      polymarket) SERVICE=$POLYMARKET_SERVICE ;;
      kalshi) SERVICE=$KALSHI_SERVICE ;;
      *) continue ;;
    esac
    if collector_terminal "$SERVICE"; then
      FINAL_TERMINAL=yes
      break
    fi
  done
  [[ $FINAL_TERMINAL != yes ]] || break
  sleep 0.5
done
if [[ $ACTIVATION_READY != yes ]]; then
  for VENUE in "${ELIGIBLE[@]}"; do
    case "$VENUE" in
      polymarket) SERVICE=$POLYMARKET_SERVICE ;;
      kalshi) SERVICE=$KALSHI_SERVICE ;;
      *) continue ;;
    esac
    collector_readiness_diagnostic "$VENUE" "$SERVICE"
  done
  cleanup_prediction_services \
    || fail 'final readiness failed and Prediction Markets cleanup also failed'
  fail 'final authenticated activation readiness failed; all new services disarmed'
fi
printf 'PREDICTION_STARTS_AT_UTC=%s\n' "$START_AT_UTC"
printf 'PREDICTION_ELIGIBLE_VENUES=%s\n' "${ELIGIBLE[*]:-NONE}"
printf 'PREDICTION_DASHBOARD=http://127.0.0.1:18081\n'
printf 'PREDICTION_INSTALL_ACTIVATION_GREEN\n'
