#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

fail() {
  printf 'H1_VPS_INSTALL_REFUSED:%s\n' "$1" >&2
  exit 4
}
trap 'fail "line=$LINENO exit=$?"' ERR

if (($# != 1)); then
  fail 'usage: vps-install.sh INCOMING_ROOT'
fi

INCOMING_ROOT=$1
[[ $(id -un) == hyperlab ]] || fail 'run as hyperlab'
[[ $HOME == /home/hyperlab ]] || fail 'HOME must be /home/hyperlab'
case "$INCOMING_ROOT" in
  "$HOME"/hyperlab-h1/incoming/*) ;;
  *) fail 'incoming root leaves $HOME/hyperlab-h1/incoming' ;;
esac
[[ -d "$INCOMING_ROOT" && ! -L "$INCOMING_ROOT" ]] || fail 'incoming root is absent or a symlink'
HANDOFF="$INCOMING_ROOT/handoff.json"
[[ -f "$HANDOFF" && ! -L "$HANDOFF" ]] || fail 'handoff is absent or a symlink'
(cd "$INCOMING_ROOT" && sha256sum -c handoff.sha256 && sha256sum -c launch-files.sha256) \
  || fail 'incoming transfer hashes diverge'

mapfile -t VALUES < <(python3.12 -I - "$HANDOFF" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    handoff = json.load(handle)
remote = handoff["remote"]
print(remote["home_root"])
print(remote["source_root"])
print(remote["campaign_root"])
print(handoff["service_name"])
print(handoff["disk"]["required_free_bytes"])
print(handoff["source_commit"])
print(handoff["files"]["campaign_manifest_sha256"])
PY
)
(( ${#VALUES[@]} == 7 )) || fail 'handoff fields are incomplete'
HOME_ROOT=${VALUES[0]}
SOURCE_ROOT=${VALUES[1]}
CAMPAIGN_ROOT=${VALUES[2]}
SERVICE=${VALUES[3]}
REQUIRED_FREE_BYTES=${VALUES[4]}
SOURCE_COMMIT=${VALUES[5]}
CAMPAIGN_MANIFEST_SHA256=${VALUES[6]}

[[ $HOME_ROOT == "$HOME/hyperlab-h1" ]] || fail 'home root differs'
case "$SOURCE_ROOT" in "$HOME_ROOT"/sources/*) ;; *) fail 'source root leaves admitted tree' ;; esac
case "$CAMPAIGN_ROOT" in "$HOME_ROOT"/campaigns/*) ;; *) fail 'campaign root leaves admitted tree' ;; esac
[[ $(pwd -P) == "$SOURCE_ROOT" ]] || fail 'run from exact detached source root'
[[ $(git rev-parse HEAD) == "$SOURCE_COMMIT" ]] || fail 'source HEAD differs'
[[ -z $(git status --porcelain) ]] || fail 'source clone is not clean'

[[ $(timedatectl show --property=NTPSynchronized --value) == yes ]] \
  || fail 'H1_NTP_NOT_SYNCHRONIZED'
AVAILABLE_BYTES=$(df -PB1 "$HOME_ROOT" | awk 'NR == 2 {print $4}')
[[ $AVAILABLE_BYTES =~ ^[0-9]+$ ]] || fail 'cannot measure free bytes'
(( AVAILABLE_BYTES >= REQUIRED_FREE_BYTES )) \
  || fail "H1_DISK_CAPACITY_INSUFFICIENT available=$AVAILABLE_BYTES required=$REQUIRED_FREE_BYTES"

for path in "$HOME_ROOT" "$HOME_ROOT/sources" "$HOME_ROOT/campaigns" "$HOME_ROOT/incoming"; do
  [[ -d "$path" && ! -L "$path" ]] || fail "unsafe base path: $path"
  [[ $(stat -c %U "$path") == hyperlab ]] || fail "path is not owned by hyperlab: $path"
  [[ $(stat -c %a "$path") == 700 ]] || fail "path mode is not 0700: $path"
done
[[ ! -e "$CAMPAIGN_ROOT" ]] || fail 'campaign root must be new'
[[ ! -e "/etc/systemd/system/$SERVICE" ]] || fail 'systemd unit target already exists'
SERVICE_LOAD_STATE=$(systemctl show "$SERVICE" --property=LoadState --value 2>/dev/null || true)
[[ -z "$SERVICE_LOAD_STATE" || $SERVICE_LOAD_STATE == not-found ]] \
  || fail "systemd service name already has load state: $SERVICE_LOAD_STATE"
[[ $(systemctl is-active "$SERVICE" 2>/dev/null || true) != active ]] \
  || fail 'systemd service name is already active'

bash "$SOURCE_ROOT/ops/h1_campaign/bootstrap-linux.sh" "$SOURCE_ROOT"

mkdir "$CAMPAIGN_ROOT"
chmod 0700 "$CAMPAIGN_ROOT"
mkdir "$CAMPAIGN_ROOT/state" "$CAMPAIGN_ROOT/operator"
install -m 0600 "$INCOMING_ROOT/campaign-seed/campaign-manifest.json" "$CAMPAIGN_ROOT/campaign-manifest.json"
install -m 0600 "$INCOMING_ROOT/campaign-seed/campaign-manifest.sha256" "$CAMPAIGN_ROOT/campaign-manifest.sha256"
install -m 0600 "$INCOMING_ROOT/campaign-seed/state/health.json" "$CAMPAIGN_ROOT/state/health.json"

VENV_PYTHON="$SOURCE_ROOT/.venv/bin/python"
export PYTHONPATH="$SOURCE_ROOT/src"
export PYTHONNOUSERSITE=1
export PYTHONDONTWRITEBYTECODE=1
"$VENV_PYTHON" "$SOURCE_ROOT/ops/h1_campaign/launch_pack.py" \
  vps-preflight --handoff "$HANDOFF" --mode start

UNIT_RENDERED="$INCOMING_ROOT/rendered-$SERVICE"
[[ ! -e "$UNIT_RENDERED" ]] || fail 'rendered unit path already exists'
"$VENV_PYTHON" "$SOURCE_ROOT/ops/h1_campaign/launch_pack.py" \
  render-unit --handoff "$HANDOFF" --output "$UNIT_RENDERED"
systemd-analyze verify "$UNIT_RENDERED"

UNIT_TARGET="/etc/systemd/system/$SERVICE"
UNIT_TEMP="/etc/systemd/system/.$SERVICE.$SOURCE_COMMIT.tmp"
sudo test ! -e "$UNIT_TARGET"
sudo test ! -e "$UNIT_TEMP"
sudo install -o root -g root -m 0644 "$UNIT_RENDERED" "$UNIT_TEMP"
sudo ln "$UNIT_TEMP" "$UNIT_TARGET"
sudo rm -- "$UNIT_TEMP"
sudo systemctl daemon-reload
[[ $(systemctl show "$SERVICE" --property=FragmentPath --value) == "$UNIT_TARGET" ]] \
  || fail 'systemd fragment path differs from atomic unit target'
[[ $(sha256sum "$UNIT_RENDERED" | awk '{print $1}') == $(sha256sum "$UNIT_TARGET" | awk '{print $1}') ]] \
  || fail 'installed systemd unit bytes differ from rendered bytes'
sudo systemctl enable --now "$SERVICE"
sleep 3

"$VENV_PYTHON" "$SOURCE_ROOT/ops/h1_campaign/launch_pack.py" \
  monitor-check --handoff "$HANDOFF"

printf 'H1_SERVICE_ARMED_OR_RUNNING_GREEN\n'
printf 'H1_CAMPAIGN_RUN_ROOT=%s\n' "$CAMPAIGN_ROOT"
printf 'H1_CAMPAIGN_MANIFEST_SHA256=%s\n' "$CAMPAIGN_MANIFEST_SHA256"
printf 'H1_SERVICE_NAME=%s\n' "$SERVICE"
printf 'H1_SECOND_TABBY_COMMAND=watch -n 10 -- bash %q %q\n' \
  "$SOURCE_ROOT/ops/h1_campaign/monitor.sh" "$HANDOFF"
