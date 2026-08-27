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
[[ ! -e "$CAMPAIGN_ROOT" ]] || fail 'campaign root must be new'
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

"$VENV_PYTHON" - "$INCOMING_ROOT/handoff.json" "$INCOMING_ROOT/host-preflight-report.json" "$CAMPAIGN_ROOT/state/activation-receipt.json" "$START_AT_UTC" <<'PY'
from datetime import UTC,datetime
import hashlib,json,os,sys
handoff=json.load(open(sys.argv[1],encoding='utf-8'))
preflight=json.load(open(sys.argv[2],encoding='utf-8'))
body={
 'boundary':'PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY',
 'campaign_root':handoff['campaign_root'],
 'dashboard_port':handoff['dashboard_port'],
 'eligible_venues':preflight['eligible_venues'],
 'economic_evidence_status':'ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE',
 'h1_actions':'NONE',
 'quick_start':os.environ.get('HYPERLAB_PM_START_AT_UTC') is None,
 'recorded_at_utc':datetime.now(UTC).isoformat(timespec='microseconds').replace('+00:00','Z'),
 'schema_version':1,
 'source_commit':handoff['source_commit'],
 'starts_at_utc':sys.argv[4],
}
canonical=lambda v: json.dumps(v,ensure_ascii=False,allow_nan=False,separators=(',',':'),sort_keys=True).encode()
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
sudo systemctl enable --now "$DASHBOARD_SERVICE"

mapfile -t ELIGIBLE < <(python3.12 -I - "$INCOMING_ROOT/host-preflight-report.json" <<'PY'
import json,sys
for value in json.load(open(sys.argv[1],encoding='utf-8'))['eligible_venues']:
 print(value)
PY
)
for VENUE in "${ELIGIBLE[@]}"; do
  case "$VENUE" in
    polymarket) sudo systemctl enable --now "$POLYMARKET_SERVICE" ;;
    kalshi) sudo systemctl enable --now "$KALSHI_SERVICE" ;;
    *) fail 'preflight eligible venue is invalid' ;;
  esac
done

for _attempt in {1..20}; do
  if "$VENV_PYTHON" -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:18081/health/ready',timeout=1).read()"; then
    break
  fi
  sleep 0.5
done
"$VENV_PYTHON" -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:18081/health/ready',timeout=2).read()" \
  || fail 'dashboard loopback fail-closed readiness did not become green'

ACTIVATION_READY=no
for _attempt in {1..20}; do
  MONITOR_JSON=$(bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/monitor.sh" "$INCOMING_ROOT/handoff.json")
  if CAMPAIGN_ROOT="$CAMPAIGN_ROOT" MONITOR_JSON="$MONITOR_JSON" "$VENV_PYTHON" - "${ELIGIBLE[@]}" <<'PY'
from datetime import UTC,datetime
import json,os,sys
value=json.loads(os.environ['MONITOR_JSON'])
activation=json.load(open(os.path.join(os.environ['CAMPAIGN_ROOT'],'state','activation-receipt.json'),encoding='utf-8'))
assert value['alert'] is False
required={'dashboard',*sys.argv[1:]}
for name in required:
 service=value['services'][name]
 properties=service['properties']
 assert properties.get('ActiveState')=='active'
 assert int(properties.get('MainPID','0') or '0') > 0
 assert service['command_verified'] is True
 if name != 'dashboard':
  state=service['state']
  assert isinstance(state,dict) and state.get('lifecycle') is not None
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
[[ $ACTIVATION_READY == yes ]] || fail 'an admitted service lacks a verified PID, command, or published state'
printf 'PREDICTION_INSTALL_ACTIVATION_GREEN\n'
printf 'PREDICTION_STARTS_AT_UTC=%s\n' "$START_AT_UTC"
printf 'PREDICTION_ELIGIBLE_VENUES=%s\n' "${ELIGIBLE[*]:-NONE}"
printf 'PREDICTION_DASHBOARD=http://127.0.0.1:18081\n'
