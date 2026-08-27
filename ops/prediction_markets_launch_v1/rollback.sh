#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

fail() {
  printf 'PREDICTION_RECOVERY_ROLLBACK_REFUSED:%s\n' "$1" >&2
  exit 4
}
if (($# != 2)); then
  fail 'usage: rollback.sh recovery|rollback HANDOFF_JSON'
fi
MODE=$1
HANDOFF=$2
[[ $MODE == recovery || $MODE == rollback ]] || fail 'mode must be recovery or rollback'
[[ $(id -un) == hyperlab ]] || fail 'run as hyperlab'
[[ -f "$HANDOFF" && ! -L "$HANDOFF" ]] || fail 'handoff is absent or unsafe'
HANDOFF=$(readlink -f -- "$HANDOFF")
INCOMING_ROOT=$(dirname -- "$HANDOFF")
[[ -f "$INCOMING_ROOT/scripts/preflight.py" && ! -L "$INCOMING_ROOT/scripts/preflight.py" ]] || fail 'recovery preflight is absent or unsafe'

VALUES_RAW=$(python3.12 -I - "$HANDOFF" <<'PY'
import hashlib,json,re,stat,sys
from pathlib import Path

def bounded(path,maximum):
 before=path.lstat()
 if path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size>maximum:
  raise ValueError(f'unsafe or oversized file:{path}')
 raw=path.read_bytes(); after=path.lstat()
 if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns) or len(raw)!=before.st_size:
  raise ValueError(f'file changed during read:{path}')
 return raw

path=Path(sys.argv[1])
incoming=path.parent
if path.name!='handoff.json' or path.resolve(strict=True)!=path or incoming.resolve(strict=True)!=incoming:
 raise ValueError('handoff path is not canonical')
raw=bounded(path,4*1024*1024)
pin=bounded(incoming/'handoff.sha256',256).decode('ascii').strip().split()
if len(pin)!=2 or pin[1]!='handoff.json' or hashlib.sha256(raw).hexdigest()!=pin[0]:
 raise ValueError('handoff SHA-256 pin diverged')
d=json.loads(raw.decode('utf-8'))
if not isinstance(d,dict) or d.get('boundary')!='PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY' or d.get('schema_version')!=1:
 raise ValueError('handoff boundary or schema diverged')
slug=incoming.name
if re.fullmatch(r'pm-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}',slug) is None:
 raise ValueError('run slug is invalid')
suffix=slug.removeprefix('pm-')
expected_incoming=f'/home/hyperlab/hyperlab-prediction-markets/incoming/{slug}'
expected_source=f'/mnt/HC_Volume_106716684/hyperlab-prediction-markets/sources/{slug}'
expected_campaign=f'/mnt/HC_Volume_106716684/hyperlab-prediction-markets/campaigns/{slug}'
if str(incoming)!=expected_incoming or d.get('incoming_root')!=expected_incoming or d.get('source_root')!=expected_source or d.get('campaign_root')!=expected_campaign:
 raise ValueError('handoff roots do not match the authenticated run slug')
expected_services={name:f'hyperlab-pm-{suffix}-{name}.service' for name in ('polymarket','kalshi','dashboard')}
expected_probes={name:f'hyperlab-pm-{suffix}-{name}-namespace-probe.service' for name in ('polymarket','kalshi')}
if d.get('run_slug')!=slug or d.get('services')!=expected_services:
 raise ValueError('handoff services do not match the authenticated run slug')
for name in ('polymarket','kalshi','dashboard'):
 print(expected_services[name])
for name in ('polymarket','kalshi'):
 print(expected_probes[name])
print(expected_campaign); print(expected_source); print(expected_incoming); print(slug)
PY
) || fail 'handoff authentication failed'
mapfile -t VALUES <<<"$VALUES_RAW"
(( ${#VALUES[@]} == 9 )) || fail 'handoff authenticated fields are incomplete'
POLYMARKET_SERVICE=${VALUES[0]}
KALSHI_SERVICE=${VALUES[1]}
DASHBOARD_SERVICE=${VALUES[2]}
POLYMARKET_NAMESPACE_PROBE_SERVICE=${VALUES[3]}
KALSHI_NAMESPACE_PROBE_SERVICE=${VALUES[4]}
CAMPAIGN_ROOT=${VALUES[5]}
SOURCE_ROOT=${VALUES[6]}
AUTHENTICATED_INCOMING_ROOT=${VALUES[7]}
RUN_SLUG=${VALUES[8]}
[[ $INCOMING_ROOT == "$AUTHENTICATED_INCOMING_ROOT" ]] || fail 'incoming root changed after authentication'

SYSTEM_ERRORS=0
system_action() {
  if ! sudo systemctl "$@"; then
    printf 'PREDICTION_SYSTEM_ACTION_FAILED=%s\n' "$*" >&2
    SYSTEM_ERRORS=1
  fi
}

if [[ $MODE == rollback ]]; then
  for SERVICE in "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" "$DASHBOARD_SERVICE" "$POLYMARKET_NAMESPACE_PROBE_SERVICE" "$KALSHI_NAMESPACE_PROBE_SERVICE"; do
    system_action stop "$SERVICE"
    system_action disable "$SERVICE"
  done
  for SERVICE in "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" "$DASHBOARD_SERVICE" "$POLYMARKET_NAMESPACE_PROBE_SERVICE" "$KALSHI_NAMESPACE_PROBE_SERVICE"; do
    if ! ACTIVE=$(timeout 5 sudo systemctl show "$SERVICE" --property=ActiveState --value --no-pager); then
      printf 'PREDICTION_ROLLBACK_POSTCONDITION_UNREADABLE=%s\n' "$SERVICE" >&2
      SYSTEM_ERRORS=1
    else
      case "$ACTIVE" in
        inactive|failed) ;;
        *)
          printf 'PREDICTION_ROLLBACK_SERVICE_NOT_DISARMED=%s:%s\n' "$SERVICE" "${ACTIVE:-EMPTY}" >&2
          SYSTEM_ERRORS=1
          ;;
      esac
    fi
    set +e
    ENABLED=$(timeout 5 sudo systemctl is-enabled "$SERVICE" 2>&1)
    set -e
    if [[ $ENABLED != disabled ]]; then
      printf 'PREDICTION_ROLLBACK_SERVICE_NOT_DISABLED=%s:%s\n' "$SERVICE" "$ENABLED" >&2
      SYSTEM_ERRORS=1
    fi
  done
  (( SYSTEM_ERRORS == 0 )) || fail 'rollback systemd actions or postconditions failed; proofs remain preserved'
  printf 'PREDICTION_CAMPAIGN_ROOT_PRESERVED=%s\n' "$CAMPAIGN_ROOT"
  printf 'PREDICTION_ROLLBACK_DISARMED_RAW_PRESERVED\n'
  exit 0
fi

STAMP=$(date -u +%Y%m%dT%H%M%S%NZ)
RESUME_REPORT="$INCOMING_ROOT/resume-preflight-$STAMP.json"
python3.12 -I "$INCOMING_ROOT/scripts/preflight.py" resume --handoff "$HANDOFF" --report "$RESUME_REPORT" \
  || fail "resume preflight refused; inspect $RESUME_REPORT"
INITIAL_ADMISSION_REPORT="$INCOMING_ROOT/recovery-initial-admission-$STAMP.json"
python3.12 -I "$INCOMING_ROOT/scripts/preflight.py" recovery-admission --handoff "$HANDOFF" --report "$INITIAL_ADMISSION_REPORT" \
  || fail "initial venue admission authentication refused; inspect $INITIAL_ADMISSION_REPORT"
declare -A INITIAL_ELIGIBLE INITIAL_NETWORK_VERDICT
while IFS=$'\t' read -r VENUE ELIGIBLE VERDICT; do
  [[ $VENUE == polymarket || $VENUE == kalshi ]] || fail 'initial admission venue is invalid'
  [[ $ELIGIBLE == yes || $ELIGIBLE == no ]] || fail 'initial admission eligibility is invalid'
  [[ -n $VERDICT ]] || fail 'initial admission network verdict is absent'
  [[ -z ${INITIAL_ELIGIBLE[$VENUE]+x} ]] || fail 'initial admission venue is duplicated'
  INITIAL_ELIGIBLE[$VENUE]=$ELIGIBLE
  INITIAL_NETWORK_VERDICT[$VENUE]=$VERDICT
done < <(python3.12 -I - "$INITIAL_ADMISSION_REPORT" <<'PY'
import json,sys
value=json.load(open(sys.argv[1],encoding='utf-8'))
def require(condition,label):
 if not condition: raise SystemExit('recovery-initial-admission:'+label)
require(value.get('terminal_signal')=='PREDICTION_RECOVERY_INITIAL_ADMISSION_AUTHENTICATED','terminal-signal')
admission=value['admission_by_venue']
require(isinstance(admission,dict) and set(admission)=={'polymarket','kalshi'},'venue-set')
for venue in ('polymarket','kalshi'):
 row=admission[venue]
 require(isinstance(row,dict),'row-object:'+venue)
 require(isinstance(row.get('eligible'),bool),'eligible-type:'+venue)
 require(isinstance(row.get('network_verdict'),str) and bool(row['network_verdict']),'network-verdict:'+venue)
 print(venue,'yes' if row['eligible'] else 'no',row['network_verdict'],sep='\t')
PY
)
(( ${#INITIAL_ELIGIBLE[@]} == 2 )) || fail 'initial admission venue rows are incomplete'

if ! sudo systemctl enable --now "$DASHBOARD_SERVICE"; then
  DASHBOARD_CLEANUP_ERRORS=0
  sudo systemctl stop "$DASHBOARD_SERVICE" || DASHBOARD_CLEANUP_ERRORS=1
  sudo systemctl disable "$DASHBOARD_SERVICE" || DASHBOARD_CLEANUP_ERRORS=1
  (( DASHBOARD_CLEANUP_ERRORS == 0 )) || fail 'dashboard recovery activation and targeted cleanup both failed'
  fail 'dashboard recovery activation failed before any venue start'
fi
DASHBOARD_RECOVERY_READY=no
VENV_PYTHON="$SOURCE_ROOT/.venv/bin/python"
recovery_service_terminal() {
  local service=$1 label=$2 snapshot active_state sub_state result exec_status main_pid
  if ! snapshot=$(timeout 5 systemctl show "$service" \
    --property=ActiveState --property=SubState --property=Result \
    --property=ExecMainCode --property=ExecMainStatus --property=MainPID \
    --no-pager 2>&1); then
    printf 'PREDICTION_RECOVERY_SERVICE_STATUS_UNREADABLE=%s:%s\n' "$label" "$service" >&2
    return 0
  fi
  active_state=$(printf '%s\n' "$snapshot" | awk -F= '$1=="ActiveState" {print $2}')
  sub_state=$(printf '%s\n' "$snapshot" | awk -F= '$1=="SubState" {print $2}')
  result=$(printf '%s\n' "$snapshot" | awk -F= '$1=="Result" {print $2}')
  exec_status=$(printf '%s\n' "$snapshot" | awk -F= '$1=="ExecMainStatus" {print $2}')
  main_pid=$(printf '%s\n' "$snapshot" | awk -F= '$1=="MainPID" {print $2}')
  if [[ $active_state == failed || $exec_status == 4 \
    || ( $active_state == inactive && $sub_state == dead && $main_pid == 0 ) ]]; then
    printf 'PREDICTION_RECOVERY_SERVICE_TERMINAL=%s:%s ActiveState=%s SubState=%s Result=%s ExecMainStatus=%s MainPID=%s\n' \
      "$label" "$service" "${active_state:-NON_DISPONIBLE}" "${sub_state:-NON_DISPONIBLE}" \
      "${result:-NON_DISPONIBLE}" "${exec_status:-NON_DISPONIBLE}" "${main_pid:-NON_DISPONIBLE}" >&2
    return 0
  fi
  return 1
}
recovery_monitor_terminal() {
  local monitor_json=$1 context=$2
  MONITOR_JSON="$monitor_json" "$VENV_PYTHON" -I - "$context" <<'PY'
import json,os,sys
context=sys.argv[1]
try:
 value=json.loads(os.environ['MONITOR_JSON'])
except (KeyError,json.JSONDecodeError) as error:
 print(f'PREDICTION_RECOVERY_MONITOR_TERMINAL={context}:INVALID_JSON:{error}',file=sys.stderr)
 raise SystemExit(0) from error
if not isinstance(value,dict):
 print(f'PREDICTION_RECOVERY_MONITOR_TERMINAL={context}:NON_OBJECT',file=sys.stderr)
 raise SystemExit(0)
if (value.get('preflight_error') is not None or value.get('failure_class') is not None
    or value.get('operational_failure') is True or value.get('activation_admissible') is False):
 print(f'PREDICTION_RECOVERY_MONITOR_TERMINAL={context}:FAIL_CLOSED',file=sys.stderr)
 raise SystemExit(0)
raise SystemExit(1)
PY
}
for _attempt in {1..20}; do
  DASHBOARD_MONITOR=''
  if "$VENV_PYTHON" - <<'PY' && \
     DASHBOARD_MONITOR=$(bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/monitor.sh" "$HANDOFF" recovery-dashboard) && \
     DASHBOARD_MONITOR="$DASHBOARD_MONITOR" "$VENV_PYTHON" - "$DASHBOARD_SERVICE" <<'PY'
import json,urllib.request
def require(condition,label):
 if not condition: raise SystemExit('recovery-dashboard-live:'+label)
try:
 with urllib.request.urlopen('http://127.0.0.1:18081/health/live',timeout=1) as response:
  raw=response.read(65537)
  require(len(raw)<=65536,'payload-too-large')
  value=json.loads(raw)
  require(response.status==200,'http-status')
except Exception as error:
 raise SystemExit(f'recovery-dashboard-live:{type(error).__name__}:{error}') from error
require(value.get('status')=='alive','status')
require(value.get('mode')=='readonly','mode')
require(value.get('orders_enabled') is False,'orders-enabled')
PY
import json,os,sys
value=json.loads(os.environ['DASHBOARD_MONITOR'])
def require(condition,label):
 if not condition: raise SystemExit('recovery-dashboard-monitor:'+label)
service=value['services']['dashboard']; properties=service['properties']
require(value.get('preflight_error') is None,'preflight-error')
require(value.get('failure_class') is None,'failure-class')
require(value.get('operational_failure') is False,'operational-failure')
require(value.get('activation_admissible') is True,'activation-admissible')
require(properties.get('ActiveState')=='active','active-state')
require(int(properties.get('MainPID','0') or '0')>0,'main-pid')
require(service.get('command_verified') is True,'command')
require(service.get('fragment_verified') is True,'fragment')
require(service.get('listener_verified') is True,'listener')
require(properties.get('FragmentPath')==f'/etc/systemd/system/{sys.argv[1]}','fragment-path')
PY
  then
    DASHBOARD_RECOVERY_READY=yes
    break
  fi
  if [[ -n $DASHBOARD_MONITOR ]] \
    && recovery_monitor_terminal "$DASHBOARD_MONITOR" dashboard; then
    break
  fi
  if recovery_service_terminal "$DASHBOARD_SERVICE" dashboard; then
    break
  fi
  sleep 0.5
done
if [[ $DASHBOARD_RECOVERY_READY != yes ]]; then
  DASHBOARD_CLEANUP_ERRORS=0
  sudo systemctl stop "$DASHBOARD_SERVICE" || DASHBOARD_CLEANUP_ERRORS=1
  sudo systemctl disable "$DASHBOARD_SERVICE" || DASHBOARD_CLEANUP_ERRORS=1
  (( DASHBOARD_CLEANUP_ERRORS == 0 )) || fail 'dashboard recovery readiness and targeted cleanup both failed'
  fail 'dashboard recovery readiness failed before any venue start'
fi
RECOVERY_STARTED_VENUES=()
RECOVERY_REFUSED_VENUES=()
RECOVERY_COMPLETED_VENUES=()
RECOVERY_FAILED_VENUES=()
recovery_service_status() {
  local venue=$1 expected_service=$2 monitor_json
  monitor_json=$(bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/monitor.sh" "$HANDOFF") || return 1
  MONITOR_JSON="$monitor_json" "$VENV_PYTHON" - "$venue" "$expected_service" <<'PY'
import json,os,sys
value=json.loads(os.environ['MONITOR_JSON'])
def require(condition,label):
 if not condition: raise SystemExit('recovery-venue-monitor:'+label)
require(isinstance(value,dict),'monitor-object')
require(value.get('preflight_error') is None,'preflight-error')
if value.get('failure_class') is not None:
 print('recovery-venue-monitor:terminal-operational-failure',file=sys.stderr)
 print('OPERATIONAL_FAILURE')
 raise SystemExit(0)
venue=sys.argv[1]; expected_service=sys.argv[2]
services=value.get('services'); require(isinstance(services,dict),'services-object')
service=services.get(venue); require(isinstance(service,dict),'venue-service-object')
properties=service.get('properties'); require(isinstance(properties,dict),'properties-object')
state=service.get('state')
require(service.get('admission_required') is True,'admission-required')
if (service.get('fragment_verified') is not True
    or properties.get('FragmentPath')!=f'/etc/systemd/system/{expected_service}'):
 print('recovery-venue-monitor:terminal-fragment-divergence',file=sys.stderr)
 print('OPERATIONAL_FAILURE')
 raise SystemExit(0)
require(isinstance(state,dict),'state-object')
lifecycle=state.get('lifecycle')
require(lifecycle not in {None,'CAPACITY_REFUSED','INTEGRITY_FAILED','INTERRUPTED_RECOVERABLE'},'lifecycle')
if service.get('venue_status')=='COMPLETE_WINDOW':
 require(lifecycle=='COMPLETE_WINDOW','complete-lifecycle')
 require(properties.get('ActiveState') in {'active','inactive'},'complete-active-state')
 if properties.get('ActiveState')=='inactive':
  require(properties.get('SubState')=='dead','complete-sub-state')
  require(int(properties.get('MainPID','0') or '0')==0,'complete-main-pid')
  require(properties.get('ExecMainStatus')=='0','complete-exit-status')
 print('COMPLETE_WINDOW')
else:
 require(service.get('venue_status') in {
  'RUNNING','PUBLIC_SOURCE_INVALID','PUBLIC_SOURCE_UNAVAILABLE_RUNTIME'
 },'venue-status')
 require(properties.get('ActiveState')=='active','active-state')
 require(int(properties.get('MainPID','0') or '0')>0,'main-pid')
 require(service.get('command_verified') is True,'command')
 print('RUNNING')
PY
}
for VENUE in polymarket kalshi; do
  case "$VENUE" in
    polymarket) SERVICE=$POLYMARKET_SERVICE ;;
    kalshi) SERVICE=$KALSHI_SERVICE ;;
  esac
  STATE_PATH="$CAMPAIGN_ROOT/$VENUE/state.json"
  LEDGER_PATH="$CAMPAIGN_ROOT/$VENUE/ledger.jsonl"
  if [[ -L $STATE_PATH ]]; then
    printf 'PREDICTION_RECOVERY_VENUE_STATE_REFUSED=%s\n' "$VENUE" >&2
    system_action stop "$SERVICE"
    system_action disable "$SERVICE"
    RECOVERY_FAILED_VENUES+=("$VENUE")
    continue
  elif [[ ! -e $STATE_PATH ]]; then
    if [[ ${INITIAL_ELIGIBLE[$VENUE]} == no && ${INITIAL_NETWORK_VERDICT[$VENUE]} == PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT && ! -e $LEDGER_PATH && ! -L $LEDGER_PATH ]]; then
      LIFECYCLE=NOT_PREVIOUSLY_STARTED
      printf 'PREDICTION_RECOVERY_VENUE_NOT_PREVIOUSLY_STARTED=%s\n' "$VENUE"
    else
      printf 'PREDICTION_RECOVERY_VENUE_STATE_REFUSED=%s\n' "$VENUE" >&2
      system_action stop "$SERVICE"
      system_action disable "$SERVICE"
      RECOVERY_FAILED_VENUES+=("$VENUE")
      continue
    fi
  elif ! LIFECYCLE=$(python3.12 -I - "$STATE_PATH" <<'PY'
import json,stat,sys
from pathlib import Path
path=Path(sys.argv[1]); before=path.lstat()
if path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size>4*1024*1024: raise ValueError('unsafe venue state')
raw=path.read_bytes(); after=path.lstat()
if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns) or len(raw)!=before.st_size: raise ValueError('venue state changed')
value=json.loads(raw.decode('utf-8'))
if not isinstance(value,dict): raise ValueError('venue state root is invalid')
lifecycle=value.get('lifecycle')
if not isinstance(lifecycle,str) or not lifecycle: raise ValueError('venue lifecycle is absent')
print(lifecycle)
PY
  ); then
    printf 'PREDICTION_RECOVERY_VENUE_STATE_REFUSED=%s\n' "$VENUE" >&2
    system_action stop "$SERVICE"
    system_action disable "$SERVICE"
    RECOVERY_FAILED_VENUES+=("$VENUE")
    continue
  fi
  if [[ $LIFECYCLE == INTEGRITY_FAILED ]]; then
    printf 'PREDICTION_RECOVERY_VENUE_REFUSED_INTEGRITY_FAILED=%s\n' "$VENUE" >&2
    system_action stop "$SERVICE"
    system_action disable "$SERVICE"
    RECOVERY_FAILED_VENUES+=("$VENUE")
    continue
  fi
  if [[ $LIFECYCLE == COMPLETE_WINDOW ]]; then
    system_action stop "$SERVICE"
    system_action disable "$SERVICE"
    RECOVERY_COMPLETED_VENUES+=("$VENUE")
    printf 'PREDICTION_RECOVERY_VENUE_ALREADY_COMPLETE=%s\n' "$VENUE"
    continue
  fi
  REPORT="$INCOMING_ROOT/recovery-network-$VENUE-$STAMP.json"
  if python3.12 -I "$INCOMING_ROOT/scripts/preflight.py" network --venue "$VENUE" --report "$REPORT"; then
    if [[ ${INITIAL_ELIGIBLE[$VENUE]} == no ]]; then
      RECOVERY_ADMISSION="$CAMPAIGN_ROOT/state/recovery-admission-$VENUE.json"
      if ! python3.12 -I "$INCOMING_ROOT/scripts/preflight.py" recovery-network-admit \
        --handoff "$HANDOFF" --network-report "$REPORT" --venue "$VENUE" --report "$RECOVERY_ADMISSION"; then
        system_action stop "$SERVICE"
        system_action disable "$SERVICE"
        RECOVERY_FAILED_VENUES+=("$VENUE")
        printf 'PREDICTION_RECOVERY_VENUE_ADMISSION_FAILED=%s\n' "$VENUE" >&2
        continue
      fi
    fi
    if sudo systemctl enable --now "$SERVICE"; then
      VENUE_STATUS=''
      for _attempt in {1..20}; do
        if VENUE_STATUS=$(recovery_service_status "$VENUE" "$SERVICE"); then
          break
        fi
        if recovery_service_terminal "$SERVICE" "$VENUE"; then
          break
        fi
        sleep 0.5
      done
      if [[ $VENUE_STATUS == COMPLETE_WINDOW ]]; then
        system_action stop "$SERVICE"
        system_action disable "$SERVICE"
        RECOVERY_COMPLETED_VENUES+=("$VENUE")
        printf 'PREDICTION_RECOVERY_VENUE_COMPLETED=%s\n' "$VENUE"
      elif [[ $VENUE_STATUS == RUNNING ]]; then
        RECOVERY_STARTED_VENUES+=("$VENUE")
        printf 'PREDICTION_RECOVERY_VENUE_STARTED=%s\n' "$VENUE"
      else
        system_action stop "$SERVICE"
        system_action disable "$SERVICE"
        RECOVERY_FAILED_VENUES+=("$VENUE")
        printf 'PREDICTION_RECOVERY_VENUE_READINESS_FAILED=%s\n' "$VENUE" >&2
      fi
    else
      system_action stop "$SERVICE"
      system_action disable "$SERVICE"
      RECOVERY_FAILED_VENUES+=("$VENUE")
      printf 'PREDICTION_RECOVERY_VENUE_START_FAILED=%s\n' "$VENUE" >&2
    fi
  else
    system_action stop "$SERVICE"
    system_action disable "$SERVICE"
    RECOVERY_REFUSED_VENUES+=("$VENUE")
    printf 'PREDICTION_RECOVERY_VENUE_REFUSED_PUBLIC_SOURCE_UNAVAILABLE=%s REPORT=%s\n' "$VENUE" "$REPORT"
  fi
done
if (( SYSTEM_ERRORS != 0 || ${#RECOVERY_FAILED_VENUES[@]} > 0 || ${#RECOVERY_REFUSED_VENUES[@]} > 0 )); then
  printf 'PREDICTION_RECOVERY_STARTED_VENUES_PRESERVED=%s\n' "${RECOVERY_STARTED_VENUES[*]:-NONE}" >&2
  printf 'PREDICTION_RECOVERY_COMPLETED_VENUES=%s\n' "${RECOVERY_COMPLETED_VENUES[*]:-NONE}" >&2
  printf 'PREDICTION_RECOVERY_REFUSED_VENUES=%s\n' "${RECOVERY_REFUSED_VENUES[*]:-NONE}" >&2
  printf 'PREDICTION_RECOVERY_FAILED_VENUES=%s\n' "${RECOVERY_FAILED_VENUES[*]:-NONE}" >&2
  printf 'PREDICTION_RECOVERY_DASHBOARD_PRESERVED=http://127.0.0.1:18081\n' >&2
  printf 'PREDICTION_RECOVERY_PARTIAL_OR_ALERT_NO_SLOT_RETRY\n' >&2
  exit 4
fi
RECOVERY_READY=no
for _attempt in {1..20}; do
  RECOVERY_MONITOR=''
  if "$VENV_PYTHON" - <<'PY' && \
     RECOVERY_MONITOR=$(bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/monitor.sh" "$HANDOFF") && \
     RECOVERY_MONITOR="$RECOVERY_MONITOR" \
     STARTED_VENUES="${RECOVERY_STARTED_VENUES[*]}" \
     REFUSED_VENUES="${RECOVERY_REFUSED_VENUES[*]}" \
     COMPLETED_VENUES="${RECOVERY_COMPLETED_VENUES[*]}" \
     "$VENV_PYTHON" - "$DASHBOARD_SERVICE" "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" <<'PY'
import json,urllib.request
def require(condition,label):
 if not condition: raise SystemExit('recovery-dashboard-ready:'+label)
try:
 with urllib.request.urlopen('http://127.0.0.1:18081/health/ready',timeout=1) as response:
  raw=response.read(65537)
  require(len(raw)<=65536,'payload-too-large')
  value=json.loads(raw)
  require(response.status==200,'http-status')
except Exception as error:
 raise SystemExit(f'recovery-dashboard-ready:{type(error).__name__}:{error}') from error
require(value.get('status')=='ready','status')
require(value.get('mode')=='readonly','mode')
require(value.get('orders_enabled') is False,'orders-enabled')
PY
import json,os,sys
value=json.loads(os.environ['RECOVERY_MONITOR'])
def require(condition,label):
 if not condition: raise SystemExit('recovery-final-monitor:'+label)
require(isinstance(value,dict),'monitor-object')
require(value.get('preflight_error') is None,'preflight-error')
require(value.get('failure_class') is None,'failure-class')
require(value.get('operational_failure') is False,'operational-failure')
require(value.get('activation_admissible') is True,'activation-admissible')
expected_services={'dashboard':sys.argv[1],'polymarket':sys.argv[2],'kalshi':sys.argv[3]}
started=set(os.environ['STARTED_VENUES'].split())
refused=set(os.environ['REFUSED_VENUES'].split())
completed=set(os.environ['COMPLETED_VENUES'].split())
require(not (started & refused or started & completed or refused & completed),'venue-sets-overlap')
require(started | refused | completed == {'polymarket','kalshi'},'venue-sets-incomplete')
services=value.get('services'); require(isinstance(services,dict),'services-object')
dashboard=services.get('dashboard'); require(isinstance(dashboard,dict),'dashboard-object')
dashboard_properties=dashboard.get('properties'); require(isinstance(dashboard_properties,dict),'dashboard-properties')
require(dashboard_properties.get('ActiveState')=='active','dashboard-active-state')
require(int(dashboard_properties.get('MainPID','0') or '0')>0,'dashboard-main-pid')
require(dashboard.get('command_verified') is True,'dashboard-command')
require(dashboard.get('fragment_verified') is True,'dashboard-fragment')
require(dashboard.get('listener_verified') is True,'dashboard-listener')
require(dashboard_properties.get('FragmentPath')==f'/etc/systemd/system/{expected_services["dashboard"]}','dashboard-fragment-path')
for venue in started:
 service=services.get(venue); require(isinstance(service,dict),'started-service:'+venue)
 properties=service.get('properties'); require(isinstance(properties,dict),'started-properties:'+venue)
 state=service.get('state')
 require(service.get('fragment_verified') is True,'started-fragment:'+venue)
 require(properties.get('FragmentPath')==f'/etc/systemd/system/{expected_services[venue]}','started-fragment-path:'+venue)
 require(isinstance(state,dict) and state.get('lifecycle') not in {
   None,'CAPACITY_REFUSED','INTEGRITY_FAILED','INTERRUPTED_RECOVERABLE'
  },'started-lifecycle:'+venue)
 if service.get('venue_status')=='COMPLETE_WINDOW':
  require(state.get('lifecycle')=='COMPLETE_WINDOW','started-complete-lifecycle:'+venue)
  require(properties.get('ActiveState') in {'active','inactive'},'started-complete-active-state:'+venue)
  if properties.get('ActiveState')=='inactive':
   require(properties.get('SubState')=='dead','started-complete-sub-state:'+venue)
   require(int(properties.get('MainPID','0') or '0')==0,'started-complete-main-pid:'+venue)
   require(properties.get('ExecMainStatus')=='0','started-complete-exit-status:'+venue)
 else:
  require(properties.get('ActiveState')=='active','started-active-state:'+venue)
  require(int(properties.get('MainPID','0') or '0')>0,'started-main-pid:'+venue)
  require(service.get('command_verified') is True,'started-command:'+venue)
for venue in refused:
 service=services.get(venue); require(isinstance(service,dict),'refused-service:'+venue)
 properties=service.get('properties'); require(isinstance(properties,dict),'refused-properties:'+venue)
 require(service.get('fragment_verified') is True,'refused-fragment:'+venue)
 require(properties.get('FragmentPath')==f'/etc/systemd/system/{expected_services[venue]}','refused-fragment-path:'+venue)
 require(properties.get('ActiveState') in {'inactive','failed'},'refused-active-state:'+venue)
 require(int(properties.get('MainPID','0') or '0')==0,'refused-main-pid:'+venue)
for venue in completed:
 service=services.get(venue); require(isinstance(service,dict),'completed-service:'+venue)
 properties=service.get('properties'); require(isinstance(properties,dict),'completed-properties:'+venue)
 state=service.get('state')
 require(service.get('fragment_verified') is True,'completed-fragment:'+venue)
 require(properties.get('FragmentPath')==f'/etc/systemd/system/{expected_services[venue]}','completed-fragment-path:'+venue)
 require(service.get('venue_status')=='COMPLETE_WINDOW','completed-status:'+venue)
 require(isinstance(state,dict) and state.get('lifecycle')=='COMPLETE_WINDOW','completed-lifecycle:'+venue)
 require(properties.get('ActiveState') in {'inactive','failed'},'completed-active-state:'+venue)
 require(int(properties.get('MainPID','0') or '0')==0,'completed-main-pid:'+venue)
PY
  then
    RECOVERY_READY=yes
    break
  fi
  if [[ -n $RECOVERY_MONITOR ]] \
    && recovery_monitor_terminal "$RECOVERY_MONITOR" final; then
    break
  fi
  RECOVERY_TERMINAL=no
  if recovery_service_terminal "$DASHBOARD_SERVICE" dashboard; then
    RECOVERY_TERMINAL=yes
  fi
  for VENUE in "${RECOVERY_STARTED_VENUES[@]}"; do
    case "$VENUE" in
      polymarket) SERVICE=$POLYMARKET_SERVICE ;;
      kalshi) SERVICE=$KALSHI_SERVICE ;;
      *) RECOVERY_TERMINAL=yes; continue ;;
    esac
    if recovery_service_terminal "$SERVICE" "$VENUE"; then
      RECOVERY_TERMINAL=yes
    fi
  done
  [[ $RECOVERY_TERMINAL == no ]] || break
  sleep 0.5
done
if [[ $RECOVERY_READY != yes ]]; then
  printf 'PREDICTION_RECOVERY_STARTED_VENUES_PRESERVED=%s\n' "${RECOVERY_STARTED_VENUES[*]:-NONE}" >&2
  printf 'PREDICTION_RECOVERY_DASHBOARD_PRESERVED=http://127.0.0.1:18081\n' >&2
  printf 'PREDICTION_RECOVERY_PARTIAL_OR_ALERT_NO_SLOT_RETRY\n' >&2
  exit 4
fi
printf 'PREDICTION_RUN_SLUG=%s\n' "$RUN_SLUG"
printf 'PREDICTION_RECOVERY_RESUME_REQUESTED_NO_SLOT_RETRY\n'
