#!/usr/bin/env bash
set -Eeuo pipefail

if (($# != 1)); then
  printf 'usage: monitor.sh HANDOFF_JSON\n' >&2
  exit 4
fi

HANDOFF=$1
mapfile -t VALUES < <(python3.12 -I - "$HANDOFF" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    handoff = json.load(handle)
print(handoff["service_name"])
print(handoff["remote"]["source_root"])
print(handoff["remote"]["campaign_root"])
PY
)
(( ${#VALUES[@]} == 3 )) || exit 4
SERVICE=${VALUES[0]}
SOURCE_ROOT=${VALUES[1]}
CAMPAIGN_ROOT=${VALUES[2]}

printf '=== systemd ===\n'
systemctl show "$SERVICE" \
  --property=LoadState,ActiveState,SubState,MainPID,NRestarts,ExecMainStatus \
  --no-pager
printf '=== journal (last 30 lines) ===\n'
journalctl -u "$SERVICE" --no-pager -n 30 --output=short-iso
printf '=== state/health.json ===\n'
cat "$CAMPAIGN_ROOT/state/health.json"
printf '\n=== identity check ===\n'
"$SOURCE_ROOT/.venv/bin/python" \
  "$SOURCE_ROOT/ops/h1_campaign/launch_pack.py" \
  monitor-check --handoff "$HANDOFF"
