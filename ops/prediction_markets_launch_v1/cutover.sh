#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

fail() {
  printf 'PREDICTION_CUTOVER_REFUSED:%s\n' "$1" >&2
  exit 4
}

if (($# != 2)); then
  fail 'usage: cutover.sh verify-old|disarm-old|restore-old NEW_HANDOFF_JSON'
fi
MODE=$1
NEW_HANDOFF=$2
[[ $MODE == verify-old || $MODE == disarm-old || $MODE == restore-old ]] \
  || fail 'mode must be verify-old, disarm-old, or restore-old'
[[ $(id -un) == hyperlab ]] || fail 'run as hyperlab'
[[ -f $NEW_HANDOFF && ! -L $NEW_HANDOFF ]] || fail 'new handoff is absent or unsafe'
NEW_HANDOFF=$(readlink -f -- "$NEW_HANDOFF")
NEW_INCOMING=$(dirname -- "$NEW_HANDOFF")

mapfile -t VALUES < <(python3.12 -I - "$NEW_HANDOFF" <<'PY'
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

def pinned(path):
 raw=bounded(path,4*1024*1024)
 pin=bounded(path.with_suffix('.sha256'),256).decode('ascii').strip().split()
 if len(pin)!=2 or pin[1]!=path.name or hashlib.sha256(raw).hexdigest()!=pin[0]:
  raise ValueError(f'pinned object diverged:{path}')
 value=json.loads(raw.decode('utf-8'))
 if not isinstance(value,dict) or raw!=json.dumps(value,ensure_ascii=False,allow_nan=False,separators=(',',':'),sort_keys=True).encode()+b'\n':
  raise ValueError(f'pinned object is not canonical:{path}')
 return value,raw

new_path=Path(sys.argv[1]); new,new_raw=pinned(new_path)
old=new.get('superseded_campaign')
if not isinstance(old,dict) or set(old)!={'campaign_root','dashboard_port','incoming_root','namespace_probe_services','run_slug','services','source_commit','source_root'}:
 raise ValueError('superseded campaign contract diverged')
expected_slug='pm-20260828t024827z-bcb5280f'
expected_source='bcb5280f87393992e2aa4528188009186cd8bdc3'
if old.get('run_slug')!=expected_slug or old.get('source_commit')!=expected_source or old.get('dashboard_port')!=18081:
 raise ValueError('superseded campaign identity diverged')
old_incoming=Path(old['incoming_root']); old_handoff,old_raw=pinned(old_incoming/'handoff.json')
for field in ('run_slug','source_commit','source_root','campaign_root','incoming_root','services','dashboard_port'):
 if old_handoff.get(field)!=old.get(field): raise ValueError(f'old handoff field diverged:{field}')
if old_handoff.get('boundary')!='PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY' or old_handoff.get('schema_version')!=1:
 raise ValueError('old handoff boundary or schema diverged')
campaign=Path(old['campaign_root']); manifest_path=campaign/'campaign-manifest.json'
manifest_raw=bounded(manifest_path,4*1024*1024)
pin=bounded(campaign/'campaign-manifest.sha256',256).decode('ascii').strip().split()
if len(pin)!=2 or pin[1]!='campaign-manifest.json' or hashlib.sha256(manifest_raw).hexdigest()!=pin[0]:
 raise ValueError('old campaign manifest pin diverged')
manifest=json.loads(manifest_raw.decode('utf-8')); claimed=manifest.get('manifest_sha256') if isinstance(manifest,dict) else None
body={key:value for key,value in manifest.items() if key!='manifest_sha256'} if isinstance(manifest,dict) else {}
canonical=lambda value: json.dumps(value,ensure_ascii=False,allow_nan=False,separators=(',',':'),sort_keys=True).encode()
if not isinstance(claimed,str) or hashlib.sha256(canonical(body)).hexdigest()!=claimed:
 raise ValueError('old campaign logical manifest diverged')
activation_path=campaign/'state'/'activation-receipt.json'; activation_raw=bounded(activation_path,4*1024*1024)
activation=json.loads(activation_raw.decode('utf-8'))
receipt=activation.get('receipt_sha256') if isinstance(activation,dict) else None
activation_body={key:value for key,value in activation.items() if key!='receipt_sha256'} if isinstance(activation,dict) else {}
if (not isinstance(receipt,str) or hashlib.sha256(canonical(activation_body)).hexdigest()!=receipt
    or activation.get('campaign_root')!=str(campaign) or activation.get('campaign_manifest_sha256')!=claimed
    or activation.get('source_commit')!=expected_source or activation.get('boundary')!='PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY'):
 raise ValueError('old activation receipt diverged')
services=old['services']; probes=old['namespace_probe_services']; new_services=new.get('services')
if not isinstance(services,dict) or not isinstance(probes,dict) or not isinstance(new_services,dict):
 raise ValueError('service maps diverged')
if set(services)!={'dashboard','kalshi','polymarket'} or set(new_services)!={'dashboard','kalshi','polymarket'} or set(probes)!={'kalshi','polymarket'}:
 raise ValueError('service map fields diverged')
old_suffix=expected_slug.removeprefix('pm-')
if services!={'dashboard':f'hyperlab-pm-{old_suffix}-dashboard.service','kalshi':f'hyperlab-pm-{old_suffix}-kalshi.service','polymarket':f'hyperlab-pm-{old_suffix}-polymarket.service'} or probes!={'kalshi':f'hyperlab-pm-{old_suffix}-kalshi-namespace-probe.service','polymarket':f'hyperlab-pm-{old_suffix}-polymarket-namespace-probe.service'}:
 raise ValueError('superseded service identities diverged')
new_slug=new.get('run_slug'); new_suffix=new_slug.removeprefix('pm-') if isinstance(new_slug,str) and re.fullmatch(r'pm-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}',new_slug) else ''
new_commit=new.get('source_commit')
if (not new_suffix or new_slug==expected_slug or not isinstance(new_commit,str) or not re.fullmatch(r'[0-9a-f]{40}',new_commit) or new_commit==expected_source
    or new.get('boundary')!='PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY' or new.get('schema_version')!=1
    or new_services!={'dashboard':f'hyperlab-pm-{new_suffix}-dashboard.service','kalshi':f'hyperlab-pm-{new_suffix}-kalshi.service','polymarket':f'hyperlab-pm-{new_suffix}-polymarket.service'}
    or new.get('source_root')==old['source_root'] or new.get('campaign_root')==old['campaign_root']):
 raise ValueError('new campaign isolation or service identities diverged')
for value in (
 old['incoming_root'],old['source_root'],old['campaign_root'],old['run_slug'],old['source_commit'],claimed,
 services['polymarket'],services['kalshi'],services['dashboard'],probes['polymarket'],probes['kalshi'],
 new.get('source_root'),new.get('campaign_root'),new_services.get('polymarket'),new_services.get('kalshi'),new_services.get('dashboard'),
): print(value)
print(hashlib.sha256(old_raw).hexdigest()); print(hashlib.sha256(activation_raw).hexdigest())
PY
) || fail 'new/old campaign identity authentication failed'
(( ${#VALUES[@]} == 18 )) || fail 'authenticated cutover context is incomplete'
OLD_INCOMING=${VALUES[0]}; OLD_SOURCE=${VALUES[1]}; OLD_CAMPAIGN=${VALUES[2]}; OLD_SLUG=${VALUES[3]}; OLD_COMMIT=${VALUES[4]}
OLD_MANIFEST_SHA=${VALUES[5]}; OLD_POLY=${VALUES[6]}; OLD_KALSHI=${VALUES[7]}; OLD_DASHBOARD=${VALUES[8]}
OLD_POLY_PROBE=${VALUES[9]}; OLD_KALSHI_PROBE=${VALUES[10]}; NEW_SOURCE=${VALUES[11]}; NEW_CAMPAIGN=${VALUES[12]}
NEW_POLY=${VALUES[13]}; NEW_KALSHI=${VALUES[14]}; NEW_DASHBOARD=${VALUES[15]}; OLD_HANDOFF_SHA=${VALUES[16]}; OLD_ACTIVATION_SHA=${VALUES[17]}

OLD_PYTHON="$OLD_SOURCE/.venv/bin/python"
[[ -x $OLD_PYTHON && ! -L $OLD_PYTHON ]] || fail 'old offline runtime is absent or unsafe'
[[ $(git -C "$OLD_SOURCE" rev-parse HEAD) == "$OLD_COMMIT" ]] || fail 'old source commit diverged'
[[ -z $(git -C "$OLD_SOURCE" status --porcelain) ]] || fail 'old source checkout is not clean'

authenticate_old_evidence() {
  PYTHONPATH="$OLD_SOURCE/src:$OLD_SOURCE" PYTHONNOUSERSITE=1 "$OLD_PYTHON" -I - "$OLD_CAMPAIGN" "$OLD_SOURCE" <<'PY'
from datetime import datetime
from pathlib import Path
import sys
source=Path(sys.argv[2]); sys.path[:0]=[str(source/'src'),str(source)]
from hyperlab.research_data.envelope import Venue
from ops.prediction_markets_launch_v1.runner import _validate_result,load_campaign_context,read_ledger,sha256_bytes,canonical_json_bytes,validate_service_ledger_against_manifest
root=Path(sys.argv[1]); context=load_campaign_context(root,source)
for venue in (Venue.POLYMARKET,Venue.KALSHI):
 rows=read_ledger(root/venue.value/'ledger.jsonl')
 validate_service_ledger_against_manifest(rows,campaign_manifest=context.manifest,venue=venue)
 for row in rows:
  if row.get('terminal_result_sha256') is None: continue
  scheduled=datetime.fromisoformat(str(row['scheduled_start_utc']).replace('Z','+00:00'))
  run=root/venue.value/'runs'/f"shard-{row['ordinal']:04d}-{scheduled.strftime('%Y%m%dT%H%M%SZ')}"
  result=_validate_result(run,context,venue,ordinal=row['ordinal'])
  if sha256_bytes(canonical_json_bytes(result))!=row['terminal_result_sha256']:
   raise ValueError('old terminal result and ledger diverged')
print('PREDICTION_OLD_RAW_RECEIPTS_LEDGER_AUTHENTICATED')
PY
}

unit_identity() {
  local service=$1 props fragment line_count load_count fragment_count restart_count
  fragment="/etc/systemd/system/$service"
  [[ -f "$OLD_INCOMING/systemd/$service" && ! -L "$OLD_INCOMING/systemd/$service" ]] || return 1
  sudo test -f "$fragment" || return 1
  sudo cmp --silent "$OLD_INCOMING/systemd/$service" "$fragment" || return 1
  props=$(timeout 5 sudo systemctl show "$service" --property=LoadState,FragmentPath,NRestarts --no-pager) || return 1
  line_count=$(printf '%s\n' "$props" | awk 'NF{count++}END{print count+0}')
  load_count=$(printf '%s\n' "$props" | awk '$0=="LoadState=loaded"{count++}END{print count+0}')
  fragment_count=$(printf '%s\n' "$props" | awk -v expected="FragmentPath=$fragment" '$0==expected{count++}END{print count+0}')
  restart_count=$(printf '%s\n' "$props" | awk '$0=="NRestarts=0"{count++}END{print count+0}')
  [[ $line_count == 3 && $load_count == 1 && $fragment_count == 1 && $restart_count == 1 ]]
}

verify_old_active() {
  authenticate_old_evidence || return 1
  for service in "$OLD_POLY" "$OLD_KALSHI" "$OLD_DASHBOARD" "$OLD_POLY_PROBE" "$OLD_KALSHI_PROBE"; do
    unit_identity "$service" || return 1
  done
  local monitor
  monitor=$(bash "$OLD_SOURCE/ops/prediction_markets_launch_v1/monitor.sh" "$OLD_INCOMING/handoff.json") \
    || return 1
  OLD_MONITOR="$monitor" OLD_PYTHON="$OLD_PYTHON" \
    "$OLD_PYTHON" -I - "$OLD_POLY" "$OLD_KALSHI" "$OLD_DASHBOARD" <<'PY'
import json,os,sys
value=json.loads(os.environ['OLD_MONITOR'])
if value.get('preflight_error') is not None or value.get('operational_failure') is not False: raise SystemExit('old monitor operational failure')
for name,expected in zip(('polymarket','kalshi','dashboard'),sys.argv[1:],strict=True):
 service=value['services'][name]; props=service['properties']
 if (service.get('command_verified') is not True or service.get('fragment_verified') is not True
     or props.get('FragmentPath')!=f'/etc/systemd/system/{expected}' or props.get('ActiveState')!='active'
     or int(props.get('MainPID','0') or '0')<=0 or props.get('NRestarts')!='0'):
  raise SystemExit(f'old active service diverged:{name}')
if value['services']['dashboard'].get('listener_verified') is not True: raise SystemExit('old dashboard listener diverged')
print('PREDICTION_OLD_CAMPAIGN_FIVE_UNITS_AUTHENTICATED')
PY
}

require_no_active_prediction_collectors() {
  local active
  active=$(sudo systemctl list-units --type=service --state=active --no-legend --no-pager \
    'hyperlab-pm-*-polymarket.service' 'hyperlab-pm-*-kalshi.service' \
    | awk 'NF{print $1}') || fail 'cannot enumerate active Prediction Markets collectors'
  [[ -z $active ]] || fail "Prediction Markets collectors remain active:$active"
}

disarm_service() {
  local service=$1
  sudo systemctl stop "$service"
  sudo systemctl disable "$service"
  local active enabled
  active=$(timeout 5 sudo systemctl show "$service" --property=ActiveState --value --no-pager)
  [[ $active == inactive || $active == failed ]] || fail "service still active:$service:$active"
  set +e; enabled=$(timeout 5 sudo systemctl is-enabled "$service" 2>&1); set -e
  [[ $enabled == disabled ]] || fail "service still enabled:$service:$enabled"
}

write_cutover_receipt() {
  local path=$1 terminal_signal=$2
  NEW_HANDOFF_SHA=$(sha256sum "$NEW_HANDOFF" | awk '{print $1}')
  python3.12 -I - "$path" "$OLD_SLUG" "$OLD_COMMIT" "$OLD_MANIFEST_SHA" "$OLD_HANDOFF_SHA" "$OLD_ACTIVATION_SHA" "$NEW_HANDOFF_SHA" "$terminal_signal" <<'PY'
from datetime import UTC,datetime
from pathlib import Path
import hashlib,json,os,sys
path=Path(sys.argv[1]); body={'boundary':'PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY','old_run_slug':sys.argv[2],'old_source_commit':sys.argv[3],'old_campaign_manifest_sha256':sys.argv[4],'old_handoff_sha256':sys.argv[5],'old_activation_receipt_sha256':sys.argv[6],'new_handoff_sha256':sys.argv[7],'recorded_at_utc':datetime.now(UTC).isoformat(timespec='microseconds').replace('+00:00','Z'),'schema_version':1,'terminal_signal':sys.argv[8]}
canonical=lambda v:json.dumps(v,ensure_ascii=False,allow_nan=False,separators=(',',':'),sort_keys=True).encode()
payload=canonical({**body,'receipt_sha256':hashlib.sha256(canonical(body)).hexdigest()})+b'\n'
fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'wb') as handle: handle.write(payload); handle.flush(); os.fsync(handle.fileno())
PY
}

authenticate_cutover_receipt() {
  local path=$1 terminal_signal=$2
  python3.12 -I - "$path" "$NEW_HANDOFF" "$OLD_SLUG" "$OLD_COMMIT" "$OLD_MANIFEST_SHA" "$OLD_HANDOFF_SHA" "$OLD_ACTIVATION_SHA" "$terminal_signal" <<'PY'
from pathlib import Path
import hashlib,json,stat,sys
def safe(path,maximum):
 before=path.lstat()
 if path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size>maximum: raise ValueError('unsafe cutover receipt input')
 raw=path.read_bytes(); after=path.lstat()
 if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns) or len(raw)!=before.st_size: raise ValueError('cutover receipt input changed')
 return raw
raw=safe(Path(sys.argv[1]),4096); value=json.loads(raw); body={key:item for key,item in value.items() if key!='receipt_sha256'}
canonical=lambda v:json.dumps(v,ensure_ascii=False,allow_nan=False,separators=(',',':'),sort_keys=True).encode()
if (raw!=canonical(value)+b'\n' or hashlib.sha256(canonical(body)).hexdigest()!=value.get('receipt_sha256')
    or value.get('old_run_slug')!=sys.argv[3] or value.get('old_source_commit')!=sys.argv[4]
    or value.get('old_campaign_manifest_sha256')!=sys.argv[5]
    or value.get('old_handoff_sha256')!=sys.argv[6] or value.get('old_activation_receipt_sha256')!=sys.argv[7]
    or value.get('new_handoff_sha256')!=hashlib.sha256(safe(Path(sys.argv[2]),4*1024*1024)).hexdigest()
    or value.get('terminal_signal')!=sys.argv[8]):
 raise ValueError('cutover receipt diverged')
PY
}

if [[ $MODE == verify-old ]]; then
  verify_old_active || fail 'old campaign active identity authentication failed'
  printf 'PREDICTION_OLD_CAMPAIGN_PREMUTATION_AUTHENTICATED\n'
  exit 0
fi

if [[ $MODE == disarm-old ]]; then
  verify_old_active || fail 'old campaign pre-mutation authentication failed'
  write_cutover_receipt "$NEW_INCOMING/cutover-old-premutation.json" 'PREDICTION_OLD_CAMPAIGN_PREMUTATION_AUTHENTICATED'
  for service in "$OLD_POLY" "$OLD_KALSHI" "$OLD_DASHBOARD" "$OLD_POLY_PROBE" "$OLD_KALSHI_PROBE"; do
    disarm_service "$service"
  done
  require_no_active_prediction_collectors
  if ss -H -ltn 'sport = :18081' | grep -q .; then fail 'dashboard port 18081 remains occupied'; fi
  authenticate_old_evidence || fail 'old evidence changed during disarm'
  write_cutover_receipt "$NEW_INCOMING/cutover-old-disarmed.json" 'PREDICTION_OLD_CAMPAIGN_DISARMED_EVIDENCE_PRESERVED'
  printf 'PREDICTION_OLD_CAMPAIGN_DISARMED_EVIDENCE_PRESERVED\n'
  exit 0
fi

[[ -f $NEW_INCOMING/cutover-old-premutation.json && ! -L $NEW_INCOMING/cutover-old-premutation.json ]] \
  || fail 'authenticated old pre-mutation receipt is absent'
authenticate_cutover_receipt "$NEW_INCOMING/cutover-old-premutation.json" 'PREDICTION_OLD_CAMPAIGN_PREMUTATION_AUTHENTICATED' \
  || fail 'old pre-mutation receipt diverged'
if [[ -e $NEW_INCOMING/cutover-old-disarmed.json || -L $NEW_INCOMING/cutover-old-disarmed.json ]]; then
  [[ -f $NEW_INCOMING/cutover-old-disarmed.json && ! -L $NEW_INCOMING/cutover-old-disarmed.json ]] \
    || fail 'old-disarm receipt is unsafe'
  authenticate_cutover_receipt "$NEW_INCOMING/cutover-old-disarmed.json" 'PREDICTION_OLD_CAMPAIGN_DISARMED_EVIDENCE_PRESERVED' \
    || fail 'old-disarm receipt diverged'
fi
disarm_new_service() {
  local service=$1 load active enabled
  load=$(timeout 5 sudo systemctl show "$service" --property=LoadState --value --no-pager 2>/dev/null || true)
  if [[ $load == not-found || -z $load ]]; then return 0; fi
  sudo systemctl stop "$service"
  sudo systemctl disable "$service"
  active=$(timeout 5 sudo systemctl show "$service" --property=ActiveState --value --no-pager)
  [[ $active == inactive || $active == failed ]] || fail "new service still active:$service:$active"
  set +e; enabled=$(timeout 5 sudo systemctl is-enabled "$service" 2>&1); set -e
  [[ $enabled == disabled ]] || fail "new service still enabled:$service:$enabled"
}
for service in "$NEW_POLY" "$NEW_KALSHI" "$NEW_DASHBOARD"; do disarm_new_service "$service"; done
NEW_SUFFIX=${NEW_POLY#hyperlab-pm-}; NEW_SUFFIX=${NEW_SUFFIX%-polymarket.service}
for service in "hyperlab-pm-$NEW_SUFFIX-polymarket-namespace-probe.service" "hyperlab-pm-$NEW_SUFFIX-kalshi-namespace-probe.service"; do disarm_new_service "$service"; done
for service in "$OLD_POLY" "$OLD_KALSHI" "$OLD_DASHBOARD" "$OLD_POLY_PROBE" "$OLD_KALSHI_PROBE"; do
  unit_identity "$service" || fail "old unit identity diverged before restore:$service"
  disarm_service "$service"
done
require_no_active_prediction_collectors
if ss -H -ltn 'sport = :18081' | grep -q .; then fail 'dashboard port 18081 is not free before old restore'; fi
authenticate_old_evidence || fail 'old evidence authentication failed before restore'
for service in "$OLD_POLY_PROBE" "$OLD_KALSHI_PROBE"; do
  sudo systemctl enable "$service"
  sudo systemctl start "$service"
  [[ $(timeout 5 sudo systemctl show "$service" --property=Result --value --no-pager) == success ]] \
    || fail "old namespace probe failed:$service"
done
sudo systemctl enable --now "$OLD_DASHBOARD"
sudo systemctl enable --now "$OLD_POLY"
sudo systemctl enable --now "$OLD_KALSHI"
for _attempt in {1..20}; do
  if verify_old_active; then
    printf 'PREDICTION_OLD_CAMPAIGN_RESTORED_NO_SLOT_RETRY\n'
    exit 0
  fi
  sleep 0.5
done
fail 'old campaign restore readiness failed; evidence remains preserved'
