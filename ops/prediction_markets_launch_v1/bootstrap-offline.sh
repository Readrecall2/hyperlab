#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

fail() {
  printf 'PREDICTION_OFFLINE_BOOTSTRAP_REFUSED:%s\n' "$1" >&2
  exit 4
}

if (($# != 2)); then
  fail 'usage: bootstrap-offline.sh SOURCE_ROOT WHEELHOUSE_ROOT'
fi
SOURCE_ROOT=$1
WHEELHOUSE_ROOT=$2
[[ $(id -un) == hyperlab ]] || fail 'run as hyperlab'
[[ $HOME == /home/hyperlab ]] || fail 'HOME must be /home/hyperlab'
[[ -d "$SOURCE_ROOT/.git" && ! -L "$SOURCE_ROOT" ]] || fail 'source root is not a fresh real clone'
[[ $(readlink -f -- "$SOURCE_ROOT") == "$SOURCE_ROOT" ]] || fail 'source root real path differs'
[[ -d "$WHEELHOUSE_ROOT" && ! -L "$WHEELHOUSE_ROOT" ]] || fail 'wheelhouse is absent or unsafe'
[[ ! -e "$SOURCE_ROOT/.venv" ]] || fail 'source virtual environment must be new'
command -v python3.12 >/dev/null 2>&1 || fail 'python3.12 is absent'
command -v timeout >/dev/null 2>&1 || fail 'timeout is absent'
python3.12 -I -c 'import platform,ssl,sys,venv; assert sys.version_info[:2] == (3,12); assert platform.machine() == "x86_64"; libc,version=platform.libc_ver(); assert libc == "glibc" and tuple(map(int,version.split(".")[:2])) >= (2,28); assert ssl.OPENSSL_VERSION' \
  || fail 'CPython 3.12 x86_64 glibc>=2.28 stdlib preflight failed'
python3.12 -m venv --copies "$SOURCE_ROOT/.venv"
VENV_PYTHON="$SOURCE_ROOT/.venv/bin/python"
[[ -x "$VENV_PYTHON" ]] || fail 'venv Python is absent'
grep -Eq '^include-system-site-packages = false$' "$SOURCE_ROOT/.venv/pyvenv.cfg" \
  || fail 'venv exposes system site packages'
export PIP_CONFIG_FILE=/dev/null
export PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_NO_INPUT=1
export PIP_NO_INDEX=1
export PIP_NO_CACHE_DIR=1
export PYTHONNOUSERSITE=1
export PYTHONDONTWRITEBYTECODE=1
timeout --signal=INT --kill-after=60s 30m \
  "$VENV_PYTHON" -m pip install \
  --no-index \
  --find-links "$WHEELHOUSE_ROOT" \
  --require-hashes \
  --only-binary=:all: \
  --no-deps \
  --requirement "$SOURCE_ROOT/requirements-runtime.lock" \
  || fail 'offline hash-locked dependency installation failed'
"$VENV_PYTHON" -m pip check || fail 'offline dependency graph is inconsistent'
printf 'PREDICTION_OFFLINE_DEPENDENCY_GRAPH_GREEN\n'
printf 'PREDICTION_OFFLINE_BOOTSTRAP_GREEN:%s\n' "$VENV_PYTHON"
