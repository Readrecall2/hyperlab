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
if d.get('run_slug')!=slug or d.get('services')!=expected_services:
 raise ValueError('handoff services do not match the authenticated run slug')
for name in ('polymarket','kalshi','dashboard'):
 print(expected_services[name])
print(expected_campaign); print(expected_source); print(expected_incoming); print(slug)
PY
) || fail 'handoff authentication failed'
mapfile -t VALUES <<<"$VALUES_RAW"
(( ${#VALUES[@]} == 7 )) || fail 'handoff authenticated fields are incomplete'
POLYMARKET_SERVICE=${VALUES[0]}
KALSHI_SERVICE=${VALUES[1]}
DASHBOARD_SERVICE=${VALUES[2]}
CAMPAIGN_ROOT=${VALUES[3]}
SOURCE_ROOT=${VALUES[4]}
AUTHENTICATED_INCOMING_ROOT=${VALUES[5]}
RUN_SLUG=${VALUES[6]}
[[ $INCOMING_ROOT == "$AUTHENTICATED_INCOMING_ROOT" ]] || fail 'incoming root changed after authentication'

SYSTEM_ERRORS=0
system_action() {
  if ! sudo systemctl "$@"; then
    printf 'PREDICTION_SYSTEM_ACTION_FAILED=%s\n' "$*" >&2
    SYSTEM_ERRORS=1
  fi
}

if [[ $MODE == rollback ]]; then
  for SERVICE in "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" "$DASHBOARD_SERVICE"; do
    system_action stop "$SERVICE"
    system_action disable "$SERVICE"
  done
  for SERVICE in "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" "$DASHBOARD_SERVICE"; do
    if ! ACTIVE=$(sudo systemctl show "$SERVICE" --property=ActiveState --value --no-pager); then
      printf 'PREDICTION_ROLLBACK_POSTCONDITION_UNREADABLE=%s\n' "$SERVICE" >&2
      SYSTEM_ERRORS=1
    elif [[ $ACTIVE == active || $ACTIVE == activating || $ACTIVE == reloading ]]; then
      printf 'PREDICTION_ROLLBACK_SERVICE_STILL_ACTIVE=%s:%s\n' "$SERVICE" "$ACTIVE" >&2
      SYSTEM_ERRORS=1
    fi
    set +e
    ENABLED=$(sudo systemctl is-enabled "$SERVICE" 2>&1)
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
assert value.get('terminal_signal')=='PREDICTION_RECOVERY_INITIAL_ADMISSION_AUTHENTICATED'
admission=value['admission_by_venue']
assert set(admission)=={'polymarket','kalshi'}
for venue in ('polymarket','kalshi'):
 row=admission[venue]
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
for _attempt in {1..20}; do
  if "$VENV_PYTHON" - <<'PY' && \
     DASHBOARD_MONITOR=$(bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/monitor.sh" "$HANDOFF" recovery-dashboard) && \
     DASHBOARD_MONITOR="$DASHBOARD_MONITOR" "$VENV_PYTHON" - "$DASHBOARD_SERVICE" <<'PY'
import json,urllib.request
with urllib.request.urlopen('http://127.0.0.1:18081/health/live',timeout=1) as response:
 raw=response.read(65537)
 assert len(raw)<=65536
 value=json.loads(raw)
 assert response.status==200 and value.get('status')=='alive'
 assert value.get('mode')=='readonly' and value.get('orders_enabled') is False
PY
import json,os,sys
value=json.loads(os.environ['DASHBOARD_MONITOR'])
service=value['services']['dashboard']; properties=service['properties']
assert value.get('preflight_error') is None and value.get('failure_class') is None
assert value.get('operational_failure') is False and value.get('activation_admissible') is True
assert properties.get('ActiveState')=='active'
assert int(properties.get('MainPID','0') or '0')>0 and service['command_verified'] is True
assert service['fragment_verified'] is True and service['listener_verified'] is True
assert properties.get('FragmentPath')==f'/etc/systemd/system/{sys.argv[1]}'
PY
  then
    DASHBOARD_RECOVERY_READY=yes
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
assert value.get('preflight_error') is None
venue=sys.argv[1]; expected_service=sys.argv[2]
service=value['services'][venue]; properties=service['properties']; state=service['state']
assert service.get('admission_required') is True
assert service.get('fragment_verified') is True
assert properties.get('FragmentPath')==f'/etc/systemd/system/{expected_service}'
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
 print('COMPLETE_WINDOW')
else:
 assert service.get('venue_status') in {
  'RUNNING','PUBLIC_SOURCE_INVALID','PUBLIC_SOURCE_UNAVAILABLE_RUNTIME'
 }
 assert properties.get('ActiveState')=='active'
 assert int(properties.get('MainPID','0') or '0')>0
 assert service['command_verified'] is True
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
  if "$VENV_PYTHON" - <<'PY' && \
     RECOVERY_MONITOR=$(bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/monitor.sh" "$HANDOFF") && \
     RECOVERY_MONITOR="$RECOVERY_MONITOR" \
     STARTED_VENUES="${RECOVERY_STARTED_VENUES[*]}" \
     REFUSED_VENUES="${RECOVERY_REFUSED_VENUES[*]}" \
     COMPLETED_VENUES="${RECOVERY_COMPLETED_VENUES[*]}" \
     "$VENV_PYTHON" - "$DASHBOARD_SERVICE" "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" <<'PY'
import json,urllib.request
with urllib.request.urlopen('http://127.0.0.1:18081/health/ready',timeout=1) as response:
 raw=response.read(65537)
 assert len(raw)<=65536
 value=json.loads(raw)
 assert response.status==200 and value.get('status')=='ready'
 assert value.get('mode')=='readonly' and value.get('orders_enabled') is False
PY
import json,os,sys
value=json.loads(os.environ['RECOVERY_MONITOR'])
assert value.get('preflight_error') is None
assert value.get('failure_class') is None
assert value.get('operational_failure') is False
assert value.get('activation_admissible') is True
expected_services={'dashboard':sys.argv[1],'polymarket':sys.argv[2],'kalshi':sys.argv[3]}
started=set(os.environ['STARTED_VENUES'].split())
refused=set(os.environ['REFUSED_VENUES'].split())
completed=set(os.environ['COMPLETED_VENUES'].split())
assert not (started & refused or started & completed or refused & completed)
assert started | refused | completed == {'polymarket','kalshi'}
dashboard=value['services']['dashboard']; dashboard_properties=dashboard['properties']
assert dashboard_properties.get('ActiveState')=='active'
assert int(dashboard_properties.get('MainPID','0') or '0')>0
assert dashboard['command_verified'] is True
assert dashboard['fragment_verified'] is True and dashboard['listener_verified'] is True
assert dashboard_properties.get('FragmentPath')==f'/etc/systemd/system/{expected_services["dashboard"]}'
for venue in started:
 service=value['services'][venue]; properties=service['properties']; state=service['state']
 assert service['fragment_verified'] is True
 assert properties.get('FragmentPath')==f'/etc/systemd/system/{expected_services[venue]}'
 assert isinstance(state,dict) and state.get('lifecycle') not in {
  None,'CAPACITY_REFUSED','INTEGRITY_FAILED','INTERRUPTED_RECOVERABLE'
 }
 if service.get('venue_status')=='COMPLETE_WINDOW':
  assert state.get('lifecycle')=='COMPLETE_WINDOW'
  assert properties.get('ActiveState') in {'active','inactive'}
  if properties.get('ActiveState')=='inactive':
   assert properties.get('SubState')=='dead'
   assert int(properties.get('MainPID','0') or '0')==0
   assert properties.get('ExecMainStatus')=='0'
 else:
  assert properties.get('ActiveState')=='active'
  assert int(properties.get('MainPID','0') or '0')>0
  assert service['command_verified'] is True
for venue in refused:
 service=value['services'][venue]; properties=service['properties']
 assert service['fragment_verified'] is True
 assert properties.get('FragmentPath')==f'/etc/systemd/system/{expected_services[venue]}'
 assert properties.get('ActiveState') not in {'active','activating','reloading'}
 assert int(properties.get('MainPID','0') or '0')==0
for venue in completed:
 service=value['services'][venue]; properties=service['properties']; state=service['state']
 assert service['fragment_verified'] is True
 assert properties.get('FragmentPath')==f'/etc/systemd/system/{expected_services[venue]}'
 assert service.get('venue_status')=='COMPLETE_WINDOW'
 assert isinstance(state,dict) and state.get('lifecycle')=='COMPLETE_WINDOW'
 assert properties.get('ActiveState') not in {'active','activating','reloading'}
 assert int(properties.get('MainPID','0') or '0')==0
PY
  then
    RECOVERY_READY=yes
    break
  fi
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
