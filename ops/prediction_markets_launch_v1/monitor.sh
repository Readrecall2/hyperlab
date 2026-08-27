#!/usr/bin/env bash
set -Eeuo pipefail

if (($# < 1 || $# > 2)) || [[ ${2:-} != '' && ${2:-} != dashboard-only && ${2:-} != recovery-dashboard ]]; then
  printf 'usage: monitor.sh HANDOFF_JSON [dashboard-only|recovery-dashboard]\n' >&2
  exit 4
fi
HANDOFF=$1
MODE=${2:-full}
python3.12 -I - "$HANDOFF" "$MODE" <<'PY'
import hashlib,json,os,stat,subprocess,sys,time
from datetime import UTC,datetime
from pathlib import Path

MAX_JSON=4*1024*1024
MAX_CMDLINE=64*1024

def bounded(path, maximum):
 before=path.lstat()
 if path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size>maximum:
  raise ValueError(f'unsafe or oversized file:{path}')
 raw=path.read_bytes(); after=path.lstat()
 if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns) or len(raw)!=before.st_size:
  raise ValueError(f'file changed during read:{path}')
 return raw

def bounded_proc_cmdline(pid):
 path=Path(f'/proc/{pid}/cmdline')
 before=path.lstat()
 if path.is_symlink() or not stat.S_ISREG(before.st_mode):
  raise ValueError(f'unsafe proc command line:{path}')
 fd=os.open(path,os.O_RDONLY|os.O_CLOEXEC)
 try:
  opened=os.fstat(fd)
  if (before.st_dev,before.st_ino)!=(opened.st_dev,opened.st_ino) or not stat.S_ISREG(opened.st_mode):
   raise ValueError(f'proc command line identity changed before read:{path}')
  raw=os.read(fd,MAX_CMDLINE+1)
  after=os.fstat(fd)
 finally:
  os.close(fd)
 if len(raw)>MAX_CMDLINE:
  raise ValueError(f'oversized proc command line:{path}')
 if (opened.st_dev,opened.st_ino)!=(after.st_dev,after.st_ino) or not stat.S_ISREG(after.st_mode):
  raise ValueError(f'proc command line identity changed during read:{path}')
 if not raw or not raw.endswith(b'\0') or b'\0\0' in raw:
  raise ValueError(f'proc command line framing is invalid:{path}')
 return raw

def object_at(path):
 value=json.loads(bounded(path,MAX_JSON).decode('utf-8'))
 if not isinstance(value,dict): raise ValueError(f'JSON root is not an object:{path}')
 return value

def canonical(value):
 return json.dumps(value,ensure_ascii=False,allow_nan=False,separators=(',',':'),sort_keys=True).encode()

def sha(value):
 return isinstance(value,str) and len(value)==64 and all(character in '0123456789abcdef' for character in value)

handoff_path=Path(sys.argv[1]); mode=sys.argv[2]; dashboard_only=mode!='full'; recovery_dashboard=mode=='recovery-dashboard'
handoff_raw=bounded(handoff_path,MAX_JSON)
pin=bounded(handoff_path.with_suffix('.sha256'),256).decode('ascii').strip().split()
if len(pin)!=2 or pin[1]!=handoff_path.name or hashlib.sha256(handoff_raw).hexdigest()!=pin[0]:
 raise ValueError('handoff SHA-256 pin diverged')
handoff=json.loads(handoff_raw.decode('utf-8'))
if not isinstance(handoff,dict): raise ValueError('handoff root is not an object')
services=handoff['services']; root=Path(handoff['campaign_root']); source=handoff['source_root']
sys.path[:0]=[str(Path(source)/'src'),source]
preflight_path=root/'state'/'preflight-report.json'; preflight_error=None; preflight={}; recovery_network={}; manifest={}
try:
 from hyperlab.research_data.envelope import Venue
 from ops.prediction_markets_launch_v1.cockpit import _validate_venue_state,active_optional_service_is_admissible,classify_monitored_service,complete_service_is_admissible,prepared_state_is_stale,validate_activation_evidence
 from ops.prediction_markets_launch_v1.runner import read_ledger,validate_service_ledger_against_manifest
 preflight_raw=bounded(preflight_path,MAX_JSON); preflight=json.loads(preflight_raw.decode('utf-8')); eligible=set(preflight['eligible_venues'])
 if not isinstance(preflight,dict): raise ValueError('preflight root is not an object')
 if not eligible.issubset({'polymarket','kalshi'}): raise ValueError('invalid eligible venue')
 manifest_path=root/'campaign-manifest.json'; manifest_raw=bounded(manifest_path,MAX_JSON); manifest=json.loads(manifest_raw.decode('utf-8'))
 if not isinstance(manifest,dict): raise ValueError('campaign manifest root is not an object')
 manifest_pin=bounded(root/'campaign-manifest.sha256',256).decode('ascii').strip().split()
 if len(manifest_pin)!=2 or manifest_pin[1]!='campaign-manifest.json' or hashlib.sha256(manifest_raw).hexdigest()!=manifest_pin[0]:
  raise ValueError('campaign manifest physical pin diverged')
 manifest_claimed=manifest.get('manifest_sha256'); manifest_body={key:value for key,value in manifest.items() if key!='manifest_sha256'}
 if not sha(manifest_claimed) or hashlib.sha256(canonical(manifest_body)).hexdigest()!=manifest_claimed:
  raise ValueError('campaign manifest logical SHA-256 diverged')
 activation_raw=bounded(root/'state'/'activation-receipt.json',MAX_JSON); activation=json.loads(activation_raw.decode('utf-8'))
 if not isinstance(activation,dict): raise ValueError('activation receipt root is not an object')
 validate_activation_evidence(activation,activation_raw=activation_raw,preflight=preflight,preflight_raw=preflight_raw,manifest=manifest,campaign_root=root,expected_source_commit=handoff.get('source_commit'))
 recovery_fields={'boundary','campaign_id','campaign_manifest_sha256','campaign_root','handoff_sha256','initial_preflight_report_sha256','network_report','network_report_sha256','receipt_sha256','recorded_at_utc','schema_version','source_commit','source_root','terminal_signal','venue'}
 for venue in ('polymarket','kalshi'):
  admission_path=root/'state'/f'recovery-admission-{venue}.json'
  try: admission_raw=bounded(admission_path,MAX_JSON)
  except FileNotFoundError: continue
  admission=json.loads(admission_raw.decode('utf-8'))
  if not isinstance(admission,dict) or set(admission)!=recovery_fields or admission_raw!=canonical(admission)+b'\n':
   raise ValueError(f'{venue} recovery admission schema or framing diverged')
  receipt=admission.get('receipt_sha256'); body={key:value for key,value in admission.items() if key!='receipt_sha256'}
  network=admission.get('network_report'); network_sha=admission.get('network_report_sha256')
  recorded=admission.get('recorded_at_utc')
  parsed=datetime.fromisoformat(recorded.replace('Z','+00:00')) if isinstance(recorded,str) else None
  initial=preflight.get('network',{}).get(venue,{}) if isinstance(preflight.get('network'),dict) else {}
  if (
   not sha(receipt) or hashlib.sha256(canonical(body)).hexdigest()!=receipt
   or not isinstance(network,dict) or network.get('venue')!=venue or network.get('verdict')!='NETWORK_PREFLIGHT_GREEN'
   or not sha(network_sha) or hashlib.sha256(canonical(network)+b'\n').hexdigest()!=network_sha
   or admission.get('boundary')!='PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY'
   or admission.get('campaign_id')!=manifest.get('campaign_id')
   or admission.get('campaign_manifest_sha256')!=manifest_claimed
   or admission.get('campaign_root')!=str(root)
   or admission.get('handoff_sha256')!=hashlib.sha256(handoff_raw).hexdigest()
   or admission.get('initial_preflight_report_sha256')!=hashlib.sha256(preflight_raw).hexdigest()
   or admission.get('schema_version')!=1 or admission.get('source_commit')!=handoff.get('source_commit')
   or admission.get('source_root')!=source or admission.get('terminal_signal')!='PREDICTION_RECOVERY_NETWORK_ADMISSION_AUTHENTICATED'
   or admission.get('venue')!=venue or parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None
   or not isinstance(initial,dict) or initial.get('verdict')!='PUBLIC_SOURCE_UNAVAILABLE_PREFLIGHT'
   or venue in set(preflight.get('eligible_venues',[]))
  ): raise ValueError(f'{venue} recovery admission binding diverged')
  eligible.add(venue); recovery_network[venue]=network
except Exception as error:
 eligible=set(); preflight_error=f'{type(error).__name__}:{error}'
expected={
 'polymarket':[source+'/.venv/bin/python',source+'/ops/prediction_markets_launch_v1/runner.py','--handoff',handoff['incoming_root']+'/handoff.json','--venue','polymarket'],
 'kalshi':[source+'/.venv/bin/python',source+'/ops/prediction_markets_launch_v1/runner.py','--handoff',handoff['incoming_root']+'/handoff.json','--venue','kalshi'],
 'dashboard':[source+'/.venv/bin/python',source+'/ops/prediction_markets_launch_v1/cockpit.py','--campaign-root',handoff['campaign_root'],'--host','127.0.0.1','--port','18081'],
}
result={'alert':preflight_error is not None,'boundary':'PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY','campaign_root':str(root),'eligible_venues':sorted(eligible),'operational_failure':preflight_error is not None,'preflight_error':preflight_error,'services':{},'source_commit':handoff['source_commit']}
for name in ('polymarket','kalshi','dashboard'):
 service=services[name]; props={}; system_error=None
 try:
  shown=subprocess.run(['systemctl','show',service,'--property=LoadState,ActiveState,SubState,MainPID,NRestarts,ExecMainStatus','--no-pager'],capture_output=True,text=True,check=False,timeout=5)
  for line in shown.stdout.splitlines():
   key,sep,value=line.partition('=')
   if sep: props[key]=value
  if shown.returncode!=0:
   system_error=f'systemctl show exit={shown.returncode}'; result['alert']=True
 except (OSError,subprocess.TimeoutExpired) as error:
  shown=None; system_error=f'{type(error).__name__}:{error}'; result['alert']=True
 try: pid=int(props.get('MainPID','0') or '0')
 except ValueError: pid=0; result['alert']=True
 command=None
 if pid>0:
  try:
   raw=bounded_proc_cmdline(pid)
   command=[part.decode('utf-8') for part in raw[:-1].split(b'\0')]
  except Exception as error:
   system_error=f'{type(error).__name__}:{error}'; result['alert']=True
 state=None; state_error=None; ledger_error=None; invalid_history=False
 network_verdict=None
 if name!='dashboard':
  network=recovery_network.get(name)
  if network is None: network=preflight.get('network',{}).get(name,{}) if isinstance(preflight,dict) else {}
  if isinstance(network,dict): network_verdict=network.get('verdict')
  if preflight_error is None:
   for attempt in range(3):
    try:
     rows=read_ledger(root/name/'ledger.jsonl')
     path=root/name/'state.json'
     try: state=object_at(path)
     except FileNotFoundError: state=None
     validate_service_ledger_against_manifest(rows,campaign_manifest=manifest,venue=Venue(name))
     _validate_venue_state(state,rows,manifest,venue=name)
     ledger_error=None
     break
    except Exception as error:
     ledger_error=f'{type(error).__name__}:{error}'
     if attempt==2: state_error=ledger_error
     else: time.sleep(0.05)
   if ledger_error is not None:
    result['alert']=True; result['operational_failure']=True
  else:
   state_error='UNAUTHENTICATED_STATE_DUE_TO_INITIAL_EVIDENCE_FAILURE'
  lifecycle=None if state is None else state.get('lifecycle')
  data_quality=None if state is None else state.get('data_quality')
  invalid_history=bool(isinstance(data_quality,dict) and data_quality.get('terminal_health')=='PUBLIC_SOURCE_INVALID')
  if lifecycle in {'CAPACITY_REFUSED','INTEGRITY_FAILED','INTERRUPTED_RECOVERABLE'} or invalid_history:
   result['alert']=True
 command_verified=bool(pid>0 and command==expected[name])
 required=name=='dashboard' or (not dashboard_only and name in eligible)
 complete=isinstance(state,dict) and state.get('lifecycle')=='COMPLETE_WINDOW'
 lifecycle=None if state is None else state.get('lifecycle')
 last_terminal=None if state is None else state.get('last_terminal')
 prepared_stale=prepared_state_is_stale(lifecycle=lifecycle,starts_at_utc=manifest.get('starts_at_utc'),now=datetime.now(UTC))
 complete_service_ok=complete_service_is_admissible(complete=complete,show_returncode=None if shown is None else shown.returncode,system_error=system_error,properties=props,pid=pid,command_verified=command_verified)
 venue_status,terminal_condition=classify_monitored_service(name=name,ledger_error=ledger_error,lifecycle=lifecycle,last_terminal=last_terminal,network_verdict=network_verdict,complete_service_ok=complete_service_ok,command_verified=command_verified,active_state=props.get('ActiveState'),prepared_stale=prepared_stale)
 if terminal_condition is not None: result['alert']=True
 if venue_status not in {'RUNNING','COMPLETE_WINDOW'}: result['alert']=True
 if required:
  if complete:
   if not complete_service_ok:
    result['alert']=True; result['operational_failure']=True
  else:
   if shown is None or shown.returncode!=0 or props.get('LoadState')!='loaded' or props.get('ActiveState')!='active' or pid<=0 or not command_verified:
    result['alert']=True; result['operational_failure']=True
   if name!='dashboard' and not isinstance(state,dict):
    result['alert']=True; result['operational_failure']=True
 if venue_status in {'INTEGRITY_FAILED','CAPACITY_REFUSED','INTERRUPTED_RECOVERABLE','PREPARED_STALE','SERVICE_UNAVAILABLE','COMPLETE_WINDOW_SERVICE_FAILED'} and required:
  result['operational_failure']=True
 if not required and (props.get('ActiveState')=='active' or pid>0):
  active_optional_ok=active_optional_service_is_admissible(recovery_dashboard=recovery_dashboard,name=name,eligible=name in eligible,show_returncode=None if shown is None else shown.returncode,load_state=props.get('LoadState'),active_state=props.get('ActiveState'),pid=pid,command_verified=command_verified,state_present=isinstance(state,dict),venue_status=venue_status)
  if not active_optional_ok:
   result['alert']=True; result['operational_failure']=True
 result['services'][name]={'admission_required':required,'command':command,'command_verified':command_verified,'data_quality_alert':invalid_history,'ledger_error':ledger_error,'network_verdict':network_verdict,'properties':props,'state':state,'state_error':state_error,'system_error':system_error,'terminal_condition':terminal_condition,'venue_status':venue_status}
result['activation_admissible']=preflight_error is None and result['operational_failure'] is False
semantic={'activation_admissible':result['activation_admissible'],'eligible_venues':result['eligible_venues'],'operational_failure':result['operational_failure'],'services':{}}
for name,service in result['services'].items():
 state=service['state'] if isinstance(service['state'],dict) else {}
 props=service['properties']
 capacity=state.get('capacity') if isinstance(state.get('capacity'),dict) else {}
 quality=state.get('data_quality') if isinstance(state.get('data_quality'),dict) else {}
 semantic['services'][name]={'ActiveState':props.get('ActiveState'),'ExecMainStatus':props.get('ExecMainStatus'),'NRestarts':props.get('NRestarts'),'SubState':props.get('SubState'),'capacity_admitted':capacity.get('admitted'),'command_verified':service['command_verified'],'data_quality_terminal':quality.get('terminal_health'),'last_terminal':state.get('last_terminal'),'lifecycle':state.get('lifecycle'),'recorded_slots':state.get('recorded_slots'),'terminal_condition':service['terminal_condition'],'venue_status':service['venue_status']}
result['semantic_fingerprint_sha256']=hashlib.sha256(json.dumps(semantic,ensure_ascii=False,separators=(',',':'),sort_keys=True).encode()).hexdigest()
print(json.dumps(result,ensure_ascii=False,separators=(',',':'),sort_keys=True))
PY
