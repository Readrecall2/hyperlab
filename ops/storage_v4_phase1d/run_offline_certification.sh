#!/usr/bin/env bash
set -euo pipefail

if (( $# < 3 )); then
  echo "usage: $0 REPOSITORY_ROOT PHASE1D_ROOT SOURCE_COMMIT [PAPER_SERVICE ...]" >&2
  exit 64
fi

REPOSITORY_ROOT=$1
PHASE1D_ROOT=$2
SOURCE_COMMIT=$3
shift 3

[[ ${#SOURCE_COMMIT} -eq 40 && "$SOURCE_COMMIT" != *[!0-9a-f]* ]] || {
  echo "SOURCE_COMMIT must be one lowercase 40-character Git SHA" >&2
  exit 64
}
case "${SOURCE_COMMIT:1}" in
  [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;;
  *) echo "SOURCE_COMMIT must be one lowercase 40-character Git SHA" >&2; exit 64 ;;
esac

REPOSITORY_ROOT=$(realpath "$REPOSITORY_ROOT")
PHASE1D_ROOT=$(realpath "$PHASE1D_ROOT")
[[ -d "$REPOSITORY_ROOT/.git" ]] || { echo "repository root is not a Git checkout" >&2; exit 65; }
[[ -d "$PHASE1D_ROOT" ]] || { echo "Phase 1D root must already exist" >&2; exit 65; }
[[ $(git -C "$REPOSITORY_ROOT" rev-parse HEAD) == "$SOURCE_COMMIT" ]] || {
  echo "repository HEAD differs from SOURCE_COMMIT" >&2
  exit 66
}
[[ -z $(git -C "$REPOSITORY_ROOT" status --porcelain --untracked-files=no) ]] || {
  echo "tracked repository source or index is dirty" >&2
  exit 66
}

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN_ROOT="$PHASE1D_ROOT/phase1d-launch-${SOURCE_COMMIT:0:12}-${STAMP}-$$"
WORKSPACE="$RUN_ROOT/certification"
VENV="$RUN_ROOT/venv"
STDOUT_LOG="$RUN_ROOT/stdout.log"
STDERR_LOG="$RUN_ROOT/stderr.log"
PID_FILE="$RUN_ROOT/certifier.pid"

umask 077
mkdir -m 0700 "$RUN_ROOT"
python3 -m venv --without-pip "$VENV"
PYTHON="$VENV/bin/python"
[[ -x "$PYTHON" ]] || { echo "fresh offline Python venv was not created" >&2; exit 67; }

COMMAND=(
  "$PYTHON"
  "$REPOSITORY_ROOT/scripts/certify_storage_v4_phase1d_linux.py"
  --workspace "$WORKSPACE"
  --repository-root "$REPOSITORY_ROOT"
  --source-commit "$SOURCE_COMMIT"
)
for service in "$@"; do
  COMMAND+=(--paper-service "$service")
done

nohup setsid env PYTHONPATH="$REPOSITORY_ROOT/src" "${COMMAND[@]}" \
  </dev/null >"$STDOUT_LOG" 2>"$STDERR_LOG" &
CERTIFIER_PID=$!
printf '%s\n' "$CERTIFIER_PID" >"$PID_FILE"
chmod 0600 "$PID_FILE" "$STDOUT_LOG" "$STDERR_LOG"

printf 'PHASE1D_RUN_ROOT=%s\n' "$RUN_ROOT"
printf 'PHASE1D_WORKSPACE=%s\n' "$WORKSPACE"
printf 'PHASE1D_PID=%s\n' "$CERTIFIER_PID"
printf 'MONITOR=%q %q\n' "$REPOSITORY_ROOT/ops/storage_v4_phase1d/monitor_offline_certification.sh" "$RUN_ROOT"
