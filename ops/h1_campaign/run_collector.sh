#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

fail() {
  printf 'H1_COLLECTOR_WRAPPER_REFUSED:%s\n' "$1" >&2
  exit 4
}

if (($# != 1)); then
  fail 'usage: run_collector.sh HANDOFF_JSON'
fi

HANDOFF=$1
[[ -f "$HANDOFF" ]] || fail 'handoff is absent'

mapfile -t VALUES < <(python3.12 -I - "$HANDOFF" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    handoff = json.load(handle)
remote = handoff["remote"]
print(remote["source_root"])
print(remote["campaign_root"])
print(handoff["starts_at_utc"])
PY
)
(( ${#VALUES[@]} == 3 )) || fail 'handoff fields are incomplete'
SOURCE_ROOT=${VALUES[0]}
CAMPAIGN_ROOT=${VALUES[1]}
STARTS_AT_UTC=${VALUES[2]}

case "$SOURCE_ROOT" in
  "$HOME"/hyperlab-h1/sources/*) ;;
  *) fail 'source root leaves admitted tree' ;;
esac
case "$CAMPAIGN_ROOT" in
  "$HOME"/hyperlab-h1/campaigns/*) ;;
  *) fail 'campaign root leaves admitted tree' ;;
esac

VENV_PYTHON="$SOURCE_ROOT/.venv/bin/python"
CONFIG="$SOURCE_ROOT/config/research/hyperliquid-h1-ghost-v1.json"
[[ -x "$VENV_PYTHON" && -f "$CONFIG" ]] || fail 'runtime or config is absent'

stop_waiting() {
  printf 'H1_SERVICE_WAIT_INTERRUPTED_BEFORE_COLLECTION\n' >&2
  exit 130
}
trap stop_waiting INT TERM

START_EPOCH=$(python3.12 -I - "$STARTS_AT_UTC" <<'PY'
from datetime import datetime
import sys

print(int(datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00")).timestamp()))
PY
)
while :; do
  NOW_EPOCH=$(date -u +%s)
  REMAINING=$((START_EPOCH - NOW_EPOCH))
  (( REMAINING > 0 )) || break
  if (( REMAINING > 60 )); then
    sleep 60
  else
    sleep "$REMAINING"
  fi
done
trap - INT TERM

export HOME=/home/hyperlab
export PYTHONPATH="$SOURCE_ROOT/src"
export PYTHONNOUSERSITE=1
export PYTHONDONTWRITEBYTECODE=1
export PYTHONUNBUFFERED=1
export TZ=UTC

if [[ -d "$CAMPAIGN_ROOT/raw" ]]; then
  exec "$VENV_PYTHON" -m hyperlab research-data h1-collect \
    --campaign-root "$CAMPAIGN_ROOT" \
    --config "$CONFIG" \
    --resume
fi

exec "$VENV_PYTHON" -m hyperlab research-data h1-collect \
  --campaign-root "$CAMPAIGN_ROOT" \
  --config "$CONFIG"
