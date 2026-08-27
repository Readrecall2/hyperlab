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

mapfile -t VALUES < <(python3.12 -I - "$INCOMING_ROOT/handoff.json" <<'PY'
import json,sys
d=json.load(open(sys.argv[1],encoding='utf-8'))
print(d['source_root']); print(d['campaign_root']); print(d['source_commit'])
print(d['services']['polymarket']); print(d['services']['kalshi']); print(d['services']['dashboard'])
PY
)
(( ${#VALUES[@]} == 6 )) || fail 'handoff fields are incomplete'
SOURCE_ROOT=${VALUES[0]}
CAMPAIGN_ROOT=${VALUES[1]}
SOURCE_COMMIT=${VALUES[2]}
POLYMARKET_SERVICE=${VALUES[3]}
KALSHI_SERVICE=${VALUES[4]}
DASHBOARD_SERVICE=${VALUES[5]}
[[ $(pwd -P) == "$SOURCE_ROOT" ]] || cd "$SOURCE_ROOT"
[[ $(git rev-parse HEAD) == "$SOURCE_COMMIT" ]] || fail 'source commit diverged'
[[ -z $(git status --porcelain) ]] || fail 'source checkout is not clean'
[[ ! -e "$CAMPAIGN_ROOT" && ! -L "$CAMPAIGN_ROOT" ]] || fail 'campaign root must be new'
VENV_PYTHON="$SOURCE_ROOT/.venv/bin/python"
[[ -x "$VENV_PYTHON" ]] || fail 'offline runtime is absent'
export HOME=/home/hyperlab
export PYTHONPATH="$SOURCE_ROOT/src:$SOURCE_ROOT"
export PYTHONNOUSERSITE=1
export PYTHONDONTWRITEBYTECODE=1
export PYTHONUNBUFFERED=1
export TZ=UTC

START_AT_UTC=${HYPERLAB_PM_START_AT_UTC:-}
if [[ -z $START_AT_UTC ]]; then
  START_AT_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)
else
  python3.12 -I - "$START_AT_UTC" <<'PY'
from datetime import UTC,datetime,timedelta
import sys
value=datetime.fromisoformat(sys.argv[1].replace('Z','+00:00'))
assert value.tzinfo is not None and value.utcoffset() is not None
now=datetime.now(UTC)
assert value.astimezone(UTC) >= now-timedelta(seconds=5)
assert value.astimezone(UTC) <= now+timedelta(days=1)
PY
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

for SERVICE in "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" "$DASHBOARD_SERVICE"; do
  UNIT_SOURCE="$INCOMING_ROOT/systemd/$SERVICE"
  UNIT_TARGET="/etc/systemd/system/$SERVICE"
  UNIT_TEMP="/etc/systemd/system/.$SERVICE.$SOURCE_COMMIT.tmp"
  [[ -f "$UNIT_SOURCE" && ! -L "$UNIT_SOURCE" ]] || fail "rendered unit absent: $SERVICE"
  sudo test ! -e "$UNIT_TARGET"
  sudo test ! -e "$UNIT_TEMP"
  systemd-analyze verify "$UNIT_SOURCE"
done
for SERVICE in "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" "$DASHBOARD_SERVICE"; do
  UNIT_SOURCE="$INCOMING_ROOT/systemd/$SERVICE"
  UNIT_TARGET="/etc/systemd/system/$SERVICE"
  UNIT_TEMP="/etc/systemd/system/.$SERVICE.$SOURCE_COMMIT.tmp"
  sudo install -o root -g root -m 0644 "$UNIT_SOURCE" "$UNIT_TEMP"
  sudo ln "$UNIT_TEMP" "$UNIT_TARGET"
  sudo rm -- "$UNIT_TEMP"
done
sudo systemctl daemon-reload
if ! sudo systemctl enable --now "$DASHBOARD_SERVICE"; then
  DASHBOARD_CLEANUP_ERRORS=0
  sudo systemctl stop "$DASHBOARD_SERVICE" || DASHBOARD_CLEANUP_ERRORS=1
  sudo systemctl disable "$DASHBOARD_SERVICE" || DASHBOARD_CLEANUP_ERRORS=1
  (( DASHBOARD_CLEANUP_ERRORS == 0 )) || fail 'dashboard activation failed and targeted cleanup also failed'
  fail 'dashboard activation failed before any venue start'
fi

DASHBOARD_READY=no
for _attempt in {1..20}; do
  if "$VENV_PYTHON" - <<'PY' && \
     DASHBOARD_MONITOR=$(bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/monitor.sh" "$INCOMING_ROOT/handoff.json" dashboard-only) && \
     DASHBOARD_MONITOR="$DASHBOARD_MONITOR" "$VENV_PYTHON" - <<'PY'
import json,urllib.request
with urllib.request.urlopen('http://127.0.0.1:18081/health/live',timeout=1) as response:
 raw=response.read(65537)
 assert len(raw)<=65536
 value=json.loads(raw)
 assert response.status==200
 assert value.get('status')=='alive'
 assert value.get('mode')=='readonly'
 assert value.get('orders_enabled') is False
PY
import json,os
value=json.loads(os.environ['DASHBOARD_MONITOR'])
assert value.get('preflight_error') is None
assert value.get('activation_admissible') is True
dashboard=value['services']['dashboard']
properties=dashboard['properties']
assert properties.get('ActiveState')=='active'
assert int(properties.get('MainPID','0') or '0')>0
assert dashboard['command_verified'] is True
for venue in ('polymarket','kalshi'):
 service=value['services'][venue]
 assert service['properties'].get('ActiveState')!='active'
 assert int(service['properties'].get('MainPID','0') or '0')==0
PY
  then
    DASHBOARD_READY=yes
    break
  fi
  sleep 0.5
done
if [[ $DASHBOARD_READY != yes ]]; then
  DASHBOARD_CLEANUP_ERRORS=0
  sudo systemctl stop "$DASHBOARD_SERVICE" || DASHBOARD_CLEANUP_ERRORS=1
  sudo systemctl disable "$DASHBOARD_SERVICE" || DASHBOARD_CLEANUP_ERRORS=1
  (( DASHBOARD_CLEANUP_ERRORS == 0 )) || fail 'dashboard readiness failed and targeted cleanup also failed'
  fail 'dashboard loopback and command readiness did not become green'
fi

mapfile -t ELIGIBLE < <(python3.12 -I - "$INCOMING_ROOT/host-preflight-report.json" <<'PY'
import json,sys
for value in json.load(open(sys.argv[1],encoding='utf-8'))['eligible_venues']:
 print(value)
PY
)
STARTED_VENUES=()
FAILED_VENUES=()
cleanup_collector() {
  local service=$1 cleanup_errors=0
  sudo systemctl stop "$service" || cleanup_errors=1
  sudo systemctl disable "$service" || cleanup_errors=1
  return "$cleanup_errors"
}
collector_ready() {
  local venue=$1 monitor_json
  monitor_json=$(bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/monitor.sh" "$INCOMING_ROOT/handoff.json") || return 1
  CAMPAIGN_ROOT="$CAMPAIGN_ROOT" MONITOR_JSON="$monitor_json" "$VENV_PYTHON" - "$venue" <<'PY'
from datetime import UTC,datetime
import json,os,sys
value=json.loads(os.environ['MONITOR_JSON'])
assert value.get('preflight_error') is None
venue=sys.argv[1]
service=value['services'][venue]; properties=service['properties']; state=service['state']
assert service.get('admission_required') is True
assert isinstance(state,dict)
lifecycle=state.get('lifecycle')
assert lifecycle not in {None,'CAPACITY_REFUSED','INTEGRITY_FAILED','INTERRUPTED_RECOVERABLE'}
if service.get('venue_status')=='COMPLETE_WINDOW':
 assert lifecycle=='COMPLETE_WINDOW'
 assert properties.get('ActiveState') in {'active','inactive'}
 if properties.get('ActiveState')=='inactive':
  assert properties.get('SubState')=='dead'
  assert int(properties.get('MainPID','0') or '0')==0
  assert properties.get('ExecMainStatus')=='0'
else:
 assert service.get('venue_status') in {
  'RUNNING','PUBLIC_SOURCE_INVALID','PUBLIC_SOURCE_UNAVAILABLE_RUNTIME'
 }
 assert properties.get('ActiveState')=='active'
 assert int(properties.get('MainPID','0') or '0')>0
 assert service['command_verified'] is True
if lifecycle=='PREPARED':
 activation=json.load(open(os.path.join(os.environ['CAMPAIGN_ROOT'],'state','activation-receipt.json'),encoding='utf-8'))
 assert activation['quick_start'] is False
 starts=datetime.fromisoformat(activation['starts_at_utc'].replace('Z','+00:00'))
 assert starts.astimezone(UTC)>datetime.now(UTC)
PY
}
for VENUE in "${ELIGIBLE[@]}"; do
  case "$VENUE" in
    polymarket) SERVICE=$POLYMARKET_SERVICE ;;
    kalshi) SERVICE=$KALSHI_SERVICE ;;
    *) fail 'preflight eligible venue is invalid' ;;
  esac
  if ! sudo systemctl enable --now "$SERVICE"; then
    cleanup_collector "$SERVICE" || printf 'PREDICTION_INSTALL_COLLECTOR_CLEANUP_FAILED=%s\n' "$VENUE" >&2
    FAILED_VENUES+=("$VENUE")
    printf 'PREDICTION_INSTALL_COLLECTOR_ACTIVATION_FAILED=%s\n' "$VENUE" >&2
    continue
  fi
  VENUE_READY=no
  for _attempt in {1..20}; do
    if collector_ready "$VENUE"; then
      VENUE_READY=yes
      break
    fi
    sleep 0.5
  done
  if [[ $VENUE_READY == yes ]]; then
    STARTED_VENUES+=("$VENUE")
  else
    cleanup_collector "$SERVICE" || printf 'PREDICTION_INSTALL_COLLECTOR_CLEANUP_FAILED=%s\n' "$VENUE" >&2
    FAILED_VENUES+=("$VENUE")
    printf 'PREDICTION_INSTALL_COLLECTOR_READINESS_FAILED=%s\n' "$VENUE" >&2
  fi
done

if (( ${#FAILED_VENUES[@]} > 0 )); then
  printf 'PREDICTION_STARTED_VENUES=%s\n' "${STARTED_VENUES[*]:-NONE}" >&2
  printf 'PREDICTION_FAILED_VENUES=%s\n' "${FAILED_VENUES[*]}" >&2
  printf 'PREDICTION_DASHBOARD_PRESERVED=http://127.0.0.1:18081\n' >&2
  printf 'PREDICTION_INSTALL_ACTIVATION_PARTIAL_OR_ALERT\n' >&2
  exit 4
fi

ACTIVATION_READY=no
for _attempt in {1..20}; do
  if "$VENV_PYTHON" - <<'PY' && \
     MONITOR_JSON=$(bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/monitor.sh" "$INCOMING_ROOT/handoff.json") && \
     CAMPAIGN_ROOT="$CAMPAIGN_ROOT" MONITOR_JSON="$MONITOR_JSON" "$VENV_PYTHON" - "${ELIGIBLE[@]}" <<'PY'
import json,urllib.request
with urllib.request.urlopen('http://127.0.0.1:18081/health/ready',timeout=1) as response:
 raw=response.read(65537)
 assert len(raw)<=65536
 value=json.loads(raw)
 assert response.status==200 and value.get('status')=='ready'
 assert value.get('mode')=='readonly' and value.get('orders_enabled') is False
PY
from datetime import UTC,datetime
import json,os,sys
value=json.loads(os.environ['MONITOR_JSON'])
activation=json.load(open(os.path.join(os.environ['CAMPAIGN_ROOT'],'state','activation-receipt.json'),encoding='utf-8'))
assert value.get('preflight_error') is None
assert value.get('activation_admissible') is True
required={'dashboard',*sys.argv[1:]}
for name in required:
 service=value['services'][name]
 properties=service['properties']
 if name == 'dashboard':
  assert properties.get('ActiveState')=='active'
  assert int(properties.get('MainPID','0') or '0') > 0
  assert service['command_verified'] is True
  continue
 state=service['state']
 assert isinstance(state,dict) and state.get('lifecycle') is not None
 assert state.get('lifecycle') not in {'CAPACITY_REFUSED','INTEGRITY_FAILED','INTERRUPTED_RECOVERABLE'}
 if service.get('venue_status')=='COMPLETE_WINDOW':
  assert state.get('lifecycle')=='COMPLETE_WINDOW'
 else:
  assert properties.get('ActiveState')=='active'
  assert int(properties.get('MainPID','0') or '0') > 0
  assert service['command_verified'] is True
 if state.get('lifecycle')=='PREPARED':
  assert activation['quick_start'] is False
  starts=datetime.fromisoformat(activation['starts_at_utc'].replace('Z','+00:00'))
  assert starts.astimezone(UTC) > datetime.now(UTC)
PY
  then
    ACTIVATION_READY=yes
    break
  fi
  sleep 0.5
done
if [[ $ACTIVATION_READY != yes ]]; then
  printf 'PREDICTION_STARTED_VENUES_PRESERVED=%s\n' "${STARTED_VENUES[*]:-NONE}" >&2
  printf 'PREDICTION_DASHBOARD_PRESERVED=http://127.0.0.1:18081\n' >&2
  printf 'PREDICTION_INSTALL_ACTIVATION_PARTIAL_OR_ALERT\n' >&2
  exit 4
fi
printf 'PREDICTION_STARTS_AT_UTC=%s\n' "$START_AT_UTC"
printf 'PREDICTION_ELIGIBLE_VENUES=%s\n' "${ELIGIBLE[*]:-NONE}"
printf 'PREDICTION_DASHBOARD=http://127.0.0.1:18081\n'
printf 'PREDICTION_INSTALL_ACTIVATION_GREEN\n'
