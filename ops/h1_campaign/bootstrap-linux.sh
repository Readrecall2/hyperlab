#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

fail() {
  printf 'H1_LINUX_BOOTSTRAP_REFUSED:%s\n' "$1" >&2
  exit 4
}

if (($# != 1)); then
  fail 'usage: bootstrap-linux.sh SOURCE_ROOT'
fi

SOURCE_ROOT=$1
case "$SOURCE_ROOT" in
  "$HOME"/hyperlab-h1/sources/*) ;;
  *) fail 'source root leaves $HOME/hyperlab-h1/sources' ;;
esac

[[ $(id -un) == hyperlab ]] || fail 'service user must be hyperlab'
[[ $HOME == /home/hyperlab ]] || fail 'HOME must be /home/hyperlab'
[[ -d "$SOURCE_ROOT/.git" ]] || fail 'source root is not a fresh Git clone'
[[ ! -e "$SOURCE_ROOT/.venv" ]] || fail 'source virtual environment already exists'

for command_name in python3.12 timeout sha256sum; do
  command -v "$command_name" >/dev/null 2>&1 || fail "missing command: $command_name"
done

python3.12 -I -c 'import sys; assert sys.version_info[:3] == (3, 12, 13)' \
  || fail 'CPython 3.12.13 is required by this launch pack'
python3.12 -m venv --copies "$SOURCE_ROOT/.venv"

VENV_PYTHON="$SOURCE_ROOT/.venv/bin/python"
[[ -x "$VENV_PYTHON" ]] || fail 'venv Python is absent'
grep -Eq '^include-system-site-packages = false$' "$SOURCE_ROOT/.venv/pyvenv.cfg" \
  || fail 'venv unexpectedly exposes system site packages'

export PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_NO_INPUT=1
export PYTHONNOUSERSITE=1
export PYTHONDONTWRITEBYTECODE=1

timeout --signal=INT --kill-after=60s 30m \
  "$VENV_PYTHON" -m pip install \
  --require-hashes \
  --only-binary=:all: \
  --retries 3 \
  --timeout 30 \
  --requirement "$SOURCE_ROOT/requirements-runtime.lock" \
  || fail 'hashed bounded dependency bootstrap failed'

export PYTHONPATH="$SOURCE_ROOT/src"
"$VENV_PYTHON" -I -c 'import site,sys; assert sys.prefix != sys.base_prefix; assert site.ENABLE_USER_SITE is False' \
  || fail 'venv isolation preflight failed'
"$VENV_PYTHON" -c 'import hyperlab, requests, websocket; print("H1_IMPORT_PREFLIGHT_GREEN")'
"$VENV_PYTHON" -m hyperlab --help >/dev/null
"$VENV_PYTHON" -m hyperlab research-data h1-collect --help >/dev/null

printf 'H1_LINUX_BOOTSTRAP_GREEN:%s\n' "$VENV_PYTHON"
