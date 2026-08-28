#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

fail() {
  printf 'H1_DASHBOARD_BOOTSTRAP_V2_REFUSED:%s\n' "$1" >&2
  exit 4
}

if (($# != 2)); then
  fail 'usage: bootstrap-linux-v2.sh SOURCE_ROOT SOURCE_COMMIT'
fi

SOURCE_ROOT=$1
SOURCE_COMMIT=$2
case "$SOURCE_ROOT" in
  /mnt/HC_Volume_106716684/hyperlab-h1/dashboard-sources/*-dashboard-v2) ;;
  *) fail 'source root leaves the dedicated dashboard-sources V2 namespace' ;;
esac
[[ $SOURCE_COMMIT =~ ^[0-9a-f]{40}$ ]] || fail 'source commit is not an exact Git SHA'
[[ $(id -un) == hyperlab ]] || fail 'bootstrap user must be hyperlab'
[[ $HOME == /home/hyperlab ]] || fail 'HOME must be /home/hyperlab'
[[ -d "$SOURCE_ROOT/.git" && ! -L "$SOURCE_ROOT" ]] || fail 'source is not a real Git clone'
[[ $(readlink -f -- "$SOURCE_ROOT") == "$SOURCE_ROOT" ]] || fail 'source root real path differs'
[[ $(git -C "$SOURCE_ROOT" rev-parse HEAD) == "$SOURCE_COMMIT" ]] || fail 'source HEAD differs'
[[ -z $(GIT_OPTIONAL_LOCKS=0 git -C "$SOURCE_ROOT" status --porcelain) ]] \
  || fail 'source checkout is not clean'
[[ ! -e "$SOURCE_ROOT/.venv" ]] || fail 'source virtual environment already exists'

for command_name in python3.12 timeout sha256sum; do
  command -v "$command_name" >/dev/null 2>&1 || fail "missing command: $command_name"
done
python3.12 -I -c 'import sys; assert sys.version_info[:3] == (3, 12, 13)' \
  || fail 'CPython 3.12.13 is required'
python3.12 -m venv --copies "$SOURCE_ROOT/.venv"

VENV_PYTHON="$SOURCE_ROOT/.venv/bin/python"
[[ -x "$VENV_PYTHON" ]] || fail 'venv Python is absent'
grep -Eq '^include-system-site-packages = false$' "$SOURCE_ROOT/.venv/pyvenv.cfg" \
  || fail 'venv exposes system site packages'
export PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_NO_INPUT=1
export PYTHONNOUSERSITE=1
export PYTHONDONTWRITEBYTECODE=1
timeout --signal=INT --kill-after=60s 30m \
  "$VENV_PYTHON" -m pip install \
  --require-hashes --only-binary=:all: --retries 3 --timeout 30 \
  --requirement "$SOURCE_ROOT/requirements-runtime.lock" \
  || fail 'hash-locked dependency bootstrap failed'

export PYTHONPATH="$SOURCE_ROOT/src"
"$VENV_PYTHON" -I -c 'import site,sys; assert sys.prefix != sys.base_prefix; assert site.ENABLE_USER_SITE is False' \
  || fail 'venv isolation preflight failed'
"$VENV_PYTHON" -c 'import fastapi, hyperlab, uvicorn; print("H1_DASHBOARD_IMPORT_PREFLIGHT_GREEN")'
"$VENV_PYTHON" -m hyperlab h1-dashboard-serve --help >/dev/null
printf 'H1_DASHBOARD_BOOTSTRAP_V2_GREEN:%s\n' "$VENV_PYTHON"
