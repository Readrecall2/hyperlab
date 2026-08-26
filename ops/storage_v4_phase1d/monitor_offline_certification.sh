#!/usr/bin/env bash
set -euo pipefail

if (( $# < 1 || $# > 2 )); then
  echo "usage: $0 RUN_ROOT [--follow]" >&2
  exit 64
fi

RUN_ROOT=$(realpath "$1")
FOLLOW=${2:-}
[[ -d "$RUN_ROOT" ]] || { echo "run root is missing" >&2; exit 65; }
PID=$(<"$RUN_ROOT/certifier.pid")
PROGRESS="$RUN_ROOT/certification/progress.jsonl"
COMPLETE="$RUN_ROOT/certification/COMPLETE.json"
STDERR_LOG="$RUN_ROOT/stderr.log"

snapshot() {
  if [[ -f "$PROGRESS" ]]; then
    tail -n 1 "$PROGRESS"
  else
    echo '{"phase":"LAUNCH","status":"WAITING_FOR_PROGRESS"}'
  fi
  if [[ -f "$COMPLETE" ]]; then
    cat "$COMPLETE"
    return 0
  fi
  if kill -0 "$PID" 2>/dev/null; then
    return 0
  fi
  echo "certifier process exited without COMPLETE.json" >&2
  tail -n 40 "$STDERR_LOG" >&2 || true
  return 1
}

if [[ "$FOLLOW" != "--follow" ]]; then
  snapshot || status=$?
  exit "${status:-0}"
fi

LAST_PROGRESS=
while kill -0 "$PID" 2>/dev/null; do
  if [[ -f "$PROGRESS" ]]; then
    CURRENT_PROGRESS=$(tail -n 1 "$PROGRESS")
    if [[ "$CURRENT_PROGRESS" != "$LAST_PROGRESS" ]]; then
      printf '%s\n' "$CURRENT_PROGRESS"
      LAST_PROGRESS=$CURRENT_PROGRESS
    fi
  fi
  sleep 15
done
snapshot
