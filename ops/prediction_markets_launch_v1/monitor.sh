#!/usr/bin/env bash
set -Eeuo pipefail

if (($# != 1)); then
  printf 'usage: monitor.sh HANDOFF_JSON\n' >&2
  exit 4
fi
HANDOFF=$1
python3.12 -I - "$HANDOFF" <<'PY'
import json,subprocess,sys
from pathlib import Path
handoff=json.load(open(sys.argv[1],encoding='utf-8'))
services=handoff['services']; root=Path(handoff['campaign_root'])
preflight_path=root/'state'/'preflight-report.json'
preflight_error=None
try:
 if preflight_path.is_symlink() or not preflight_path.is_file(): raise ValueError('unsafe or absent preflight report')
 preflight=json.load(open(preflight_path,encoding='utf-8'))
 eligible=set(preflight['eligible_venues'])
 if not eligible.issubset({'polymarket','kalshi'}): raise ValueError('invalid eligible venue')
except Exception as error:
 eligible=set(); preflight_error=f'{type(error).__name__}:{error}'
expected={
 'polymarket':'runner.py --handoff '+handoff['incoming_root']+'/handoff.json --venue polymarket',
 'kalshi':'runner.py --handoff '+handoff['incoming_root']+'/handoff.json --venue kalshi',
 'dashboard':'cockpit.py --campaign-root '+handoff['campaign_root']+' --host 127.0.0.1 --port 18081',
}
result={'alert':preflight_error is not None,'boundary':'PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY','campaign_root':str(root),'eligible_venues':sorted(eligible),'preflight_error':preflight_error,'services':{},'source_commit':handoff['source_commit']}
for name in ('polymarket','kalshi','dashboard'):
 service=services[name]
 shown=subprocess.run(['systemctl','show',service,'--property=LoadState,ActiveState,SubState,MainPID,NRestarts,ExecMainStatus','--no-pager'],capture_output=True,text=True,check=False)
 props={}
 for line in shown.stdout.splitlines():
  key,sep,value=line.partition('=')
  if sep: props[key]=value
 pid=int(props.get('MainPID','0') or '0')
 command=None
 if pid:
  try: command=Path(f'/proc/{pid}/cmdline').read_bytes().replace(b'\0',b' ').decode(errors='replace').strip()
  except OSError: command=None
 state=None
 if name != 'dashboard':
  path=root/name/'state.json'
  if path.is_file() and not path.is_symlink():
   try: state=json.load(open(path,encoding='utf-8'))
   except Exception: result['alert']=True
  if isinstance(state,dict) and state.get('lifecycle') in {'CAPACITY_REFUSED','INTEGRITY_FAILED','INTERRUPTED_RECOVERABLE'}: result['alert']=True
 command_verified=bool(pid and command and expected[name] in command)
 required=name=='dashboard' or name in eligible
 complete=isinstance(state,dict) and state.get('lifecycle')=='COMPLETE_WINDOW'
 if required and not complete:
  if shown.returncode != 0 or props.get('LoadState')!='loaded' or props.get('ActiveState')!='active' or pid <= 0 or not command_verified: result['alert']=True
  if name != 'dashboard' and not isinstance(state,dict): result['alert']=True
 if not required and (props.get('ActiveState')=='active' or pid > 0): result['alert']=True
 result['services'][name]={'admission_required':required,'command_verified':command_verified,'properties':props,'state':state}
print(json.dumps(result,ensure_ascii=False,separators=(',',':'),sort_keys=True))
PY
