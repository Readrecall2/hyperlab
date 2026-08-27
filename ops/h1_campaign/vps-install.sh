#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

fail() {
  printf 'H1_VPS_INSTALL_REFUSED:%s\n' "$1" >&2
  exit 4
}
trap 'fail "line=$LINENO exit=$?"' ERR

if (($# != 2)); then
  fail 'usage: vps-install.sh INCOMING_ROOT prepare|activate'
fi

INCOMING_ROOT=$1
MODE=$2
case "$MODE" in prepare|activate) ;; *) fail 'mode must be prepare or activate' ;; esac
[[ $(id -un) == hyperlab ]] || fail 'run as hyperlab'
[[ $HOME == /home/hyperlab ]] || fail 'HOME must be /home/hyperlab'
case "$INCOMING_ROOT" in
  "$HOME"/hyperlab-h1/incoming/*) ;;
  *) fail 'incoming root leaves $HOME/hyperlab-h1/incoming' ;;
esac
[[ -d "$INCOMING_ROOT" && ! -L "$INCOMING_ROOT" ]] || fail 'incoming root is absent or a symlink'
[[ $(readlink -f -- "$INCOMING_ROOT") == "$INCOMING_ROOT" ]] \
  || fail 'incoming root real path differs'
[[ -z $(find "$INCOMING_ROOT" -type l -print -quit) ]] || fail 'incoming tree contains a symlink'
[[ ! -d "$INCOMING_ROOT/raw" ]] || fail 'incoming staging must never contain raw campaign data'
[[ -z $(find "$INCOMING_ROOT" -type f -name '*.rdpseg' -print -quit) ]] \
  || fail 'incoming staging contains a raw segment'
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
volume = handoff["volume"]
disk = handoff["disk"]
print(remote["home_root"])
print(remote["volume_root"])
print(remote["source_root"])
print(remote["campaign_root"])
print(handoff["service_name"])
print(disk["required_free_bytes"])
print(disk["incoming_staging_max_bytes"])
print(handoff["source_commit"])
print(handoff["files"]["campaign_manifest_sha256"])
print(handoff["files"]["source_inventory_sha256"])
print(handoff["files"]["systemd_unit_sha256"])
print(volume["mount_point"])
print(volume["device"])
print(volume["filesystem"])
print(volume["model"])
print(volume["serial"])
print(handoff["arm_deadline_utc"])
PY
)
(( ${#VALUES[@]} == 17 )) || fail 'handoff fields are incomplete'
HOME_ROOT=${VALUES[0]}
VOLUME_ROOT=${VALUES[1]}
SOURCE_ROOT=${VALUES[2]}
CAMPAIGN_ROOT=${VALUES[3]}
SERVICE=${VALUES[4]}
REQUIRED_FREE_BYTES=${VALUES[5]}
INCOMING_STAGING_MAX_BYTES=${VALUES[6]}
SOURCE_COMMIT=${VALUES[7]}
CAMPAIGN_MANIFEST_SHA256=${VALUES[8]}
SOURCE_INVENTORY_SHA256=${VALUES[9]}
SYSTEMD_UNIT_SHA256=${VALUES[10]}
VOLUME_MOUNT=${VALUES[11]}
VOLUME_DEVICE=${VALUES[12]}
VOLUME_FS=${VALUES[13]}
VOLUME_MODEL=${VALUES[14]}
VOLUME_SERIAL=${VALUES[15]}
ARM_DEADLINE_UTC=${VALUES[16]}

[[ $HOME_ROOT == "$HOME/hyperlab-h1" ]] || fail 'home root differs'
[[ $VOLUME_ROOT == /mnt/HC_Volume_106716684/hyperlab-h1 ]] || fail 'volume root differs'
case "$SOURCE_ROOT" in "$VOLUME_ROOT"/sources/*) ;; *) fail 'source root leaves volume tree' ;; esac
case "$CAMPAIGN_ROOT" in "$VOLUME_ROOT"/campaigns/*) ;; *) fail 'campaign root leaves volume tree' ;; esac
[[ $(pwd -P) == "$SOURCE_ROOT" ]] || fail 'run from exact detached source root'
[[ $(readlink -f -- "$SOURCE_ROOT") == "$SOURCE_ROOT" ]] || fail 'source root real path differs'
[[ $(git rev-parse HEAD) == "$SOURCE_COMMIT" ]] || fail 'source HEAD differs'
[[ -z $(git status --porcelain) ]] || fail 'source clone is not clean'
if [[ $MODE == prepare ]]; then
  [[ ! -e "$CAMPAIGN_ROOT" ]] || fail 'campaign root must be new'
  [[ $(readlink -m -- "$CAMPAIGN_ROOT") == "$CAMPAIGN_ROOT" ]] \
    || fail 'campaign root is not canonical'
else
  [[ -d "$CAMPAIGN_ROOT" && ! -L "$CAMPAIGN_ROOT" ]] \
    || fail 'prepared campaign root is absent or unsafe'
  [[ $(readlink -f -- "$CAMPAIGN_ROOT") == "$CAMPAIGN_ROOT" ]] \
    || fail 'prepared campaign root real path differs'
fi

INCOMING_BYTES=$(du -sb -- "$INCOMING_ROOT" | awk '{print $1}')
[[ $INCOMING_BYTES =~ ^[0-9]+$ ]] || fail 'cannot measure incoming staging bytes'
(( INCOMING_BYTES <= INCOMING_STAGING_MAX_BYTES )) \
  || fail "incoming staging exceeds ceiling: actual=$INCOMING_BYTES maximum=$INCOMING_STAGING_MAX_BYTES"
[[ $(sha256sum "$INCOMING_ROOT/inventory/source-policy-readiness.json" | awk '{print $1}') == "$SOURCE_INVENTORY_SHA256" ]] \
  || fail 'source inventory differs from handoff'
for script_name in bootstrap-linux.sh launch_pack.py monitor.sh run_collector.sh vps-install.sh; do
  cmp --silent "$INCOMING_ROOT/scripts/$script_name" "$SOURCE_ROOT/ops/h1_campaign/$script_name" \
    || fail "transferred script differs from detached source: $script_name"
done

[[ $(timedatectl show --property=NTPSynchronized --value) == yes ]] \
  || fail 'H1_NTP_NOT_SYNCHRONIZED'
ARM_DEADLINE_EPOCH=$(date -u -d "$ARM_DEADLINE_UTC" +%s)
(( $(date -u +%s) <= ARM_DEADLINE_EPOCH )) || fail 'H1_ARM_DEADLINE_MISSED'
[[ -b "$VOLUME_DEVICE" ]] || fail 'expected volume block device is absent'
[[ $(readlink -f -- "$VOLUME_DEVICE") == "$VOLUME_DEVICE" ]] || fail 'volume device real path differs'
[[ -d "$VOLUME_MOUNT" && ! -L "$VOLUME_MOUNT" ]] || fail 'volume mount is absent or a symlink'
[[ $(readlink -f -- "$VOLUME_MOUNT") == "$VOLUME_MOUNT" ]] || fail 'volume mount real path differs'
FOUND_TARGET=$(findmnt -rn -T "$VOLUME_MOUNT" -o TARGET)
FOUND_SOURCE=$(findmnt -rn -T "$VOLUME_MOUNT" -o SOURCE)
FOUND_FS=$(findmnt -rn -T "$VOLUME_MOUNT" -o FSTYPE)
FOUND_OPTIONS=$(findmnt -rn -T "$VOLUME_MOUNT" -o OPTIONS)
[[ $FOUND_TARGET == "$VOLUME_MOUNT" ]] || fail "volume target differs: $FOUND_TARGET"
[[ $FOUND_SOURCE == "$VOLUME_DEVICE" ]] || fail "volume device differs: $FOUND_SOURCE"
[[ $FOUND_FS == "$VOLUME_FS" ]] || fail "volume filesystem differs: $FOUND_FS"
case ",$FOUND_OPTIONS," in *,rw,*) ;; *) fail "volume is not rw: $FOUND_OPTIONS" ;; esac
case ",$FOUND_OPTIONS," in *,ro,*) fail "volume is read-only: $FOUND_OPTIONS" ;; esac
FOUND_SERIAL=''
FOUND_MODEL=''
if command -v lsblk >/dev/null 2>&1; then
  FOUND_SERIAL=$(lsblk -dn -o SERIAL "$VOLUME_DEVICE" 2>/dev/null | awk '{gsub(/^[[:space:]]+|[[:space:]]+$/, ""); print}') || FOUND_SERIAL=''
  FOUND_MODEL=$(lsblk -dn -o MODEL "$VOLUME_DEVICE" 2>/dev/null | awk '{gsub(/^[[:space:]]+|[[:space:]]+$/, ""); print}') || FOUND_MODEL=''
fi
[[ -z $FOUND_SERIAL || $FOUND_SERIAL == "$VOLUME_SERIAL" ]] \
  || fail "stable serial differs: $FOUND_SERIAL"
[[ -z $FOUND_MODEL || $FOUND_MODEL == "$VOLUME_MODEL" ]] \
  || fail "stable model differs: $FOUND_MODEL"

AVAILABLE_BYTES=$(df -PB1 "$VOLUME_MOUNT" | awk 'NR == 2 {gsub(/[[:space:]]/, "", $4); print $4}')
[[ $AVAILABLE_BYTES =~ ^[0-9]+$ ]] || fail 'cannot measure volume free bytes'
(( AVAILABLE_BYTES >= REQUIRED_FREE_BYTES )) \
  || fail "H1_DISK_CAPACITY_INSUFFICIENT available=$AVAILABLE_BYTES required=$REQUIRED_FREE_BYTES"

for path in "$HOME_ROOT" "$HOME_ROOT/incoming" "$INCOMING_ROOT"; do
  [[ -d "$path" && ! -L "$path" ]] || fail "unsafe home staging path: $path"
  [[ $(stat -c %U "$path") == hyperlab ]] || fail "path is not owned by hyperlab: $path"
  [[ $(stat -c %a "$path") == 700 ]] || fail "path mode is not 0700: $path"
done
for path in "$VOLUME_ROOT" "$VOLUME_ROOT/sources" "$VOLUME_ROOT/campaigns" "$SOURCE_ROOT"; do
  [[ -d "$path" && ! -L "$path" ]] || fail "unsafe volume path: $path"
  [[ $(readlink -f -- "$path") == "$path" ]] || fail "volume path real path differs: $path"
  [[ $(stat -c %U "$path") == hyperlab ]] || fail "path is not owned by hyperlab: $path"
  [[ $(stat -c %a "$path") == 700 ]] || fail "path mode is not 0700: $path"
done
[[ ! -e "/etc/systemd/system/$SERVICE" ]] || fail 'systemd unit target already exists'
SERVICE_LOAD_STATE=$(systemctl show "$SERVICE" --property=LoadState --value 2>/dev/null || true)
[[ -z "$SERVICE_LOAD_STATE" || $SERVICE_LOAD_STATE == not-found ]] \
  || fail "systemd service name already has load state: $SERVICE_LOAD_STATE"
[[ $(systemctl is-active "$SERVICE" 2>/dev/null || true) != active ]] \
  || fail 'systemd service name is already active'

VENV_PYTHON="$SOURCE_ROOT/.venv/bin/python"
export PYTHONPATH="$SOURCE_ROOT/src"
export PYTHONNOUSERSITE=1
export PYTHONDONTWRITEBYTECODE=1
UNIT_RENDERED="$INCOMING_ROOT/rendered-$SERVICE"

if [[ $MODE == prepare ]]; then
  bash "$SOURCE_ROOT/ops/h1_campaign/bootstrap-linux.sh" "$SOURCE_ROOT"
  install -d -m 0700 "$CAMPAIGN_ROOT" "$CAMPAIGN_ROOT/state" "$CAMPAIGN_ROOT/operator"
  [[ $(readlink -f -- "$CAMPAIGN_ROOT") == "$CAMPAIGN_ROOT" ]] \
    || fail 'campaign root real path differs'
  install -m 0600 "$INCOMING_ROOT/campaign-seed/campaign-manifest.json" "$CAMPAIGN_ROOT/campaign-manifest.json"
  install -m 0600 "$INCOMING_ROOT/campaign-seed/campaign-manifest.sha256" "$CAMPAIGN_ROOT/campaign-manifest.sha256"
  install -m 0600 "$INCOMING_ROOT/campaign-seed/state/health.json" "$CAMPAIGN_ROOT/state/health.json"
  [[ ! -e "$UNIT_RENDERED" ]] || fail 'rendered unit path already exists'
fi

[[ -x "$VENV_PYTHON" ]] || fail 'prepared virtual environment is absent'
"$VENV_PYTHON" "$SOURCE_ROOT/ops/h1_campaign/launch_pack.py" \
  vps-preflight --handoff "$HANDOFF" --mode start

if [[ $MODE == prepare ]]; then
  "$VENV_PYTHON" "$SOURCE_ROOT/ops/h1_campaign/launch_pack.py" \
    render-unit --handoff "$HANDOFF" --output "$UNIT_RENDERED"
fi
[[ -f "$UNIT_RENDERED" && ! -L "$UNIT_RENDERED" ]] || fail 'rendered unit is absent or unsafe'
[[ $(sha256sum "$UNIT_RENDERED" | awk '{print $1}') == "$SYSTEMD_UNIT_SHA256" ]] \
  || fail 'rendered systemd unit differs from handoff'
cmp --silent "$UNIT_RENDERED" "$INCOMING_ROOT/systemd/$SERVICE" \
  || fail 'transferred and canonical systemd units differ'
systemd-analyze verify "$UNIT_RENDERED"

if [[ $MODE == prepare ]]; then
  printf 'H1_V8_PREPARED_FOR_CUTOVER_NO_SYSTEMD_CHANGE\n'
  exit 0
fi

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
printf 'H1_SOURCE_ROOT=%s\n' "$SOURCE_ROOT"
printf 'H1_CAMPAIGN_RUN_ROOT=%s\n' "$CAMPAIGN_ROOT"
printf 'H1_CAMPAIGN_MANIFEST_SHA256=%s\n' "$CAMPAIGN_MANIFEST_SHA256"
printf 'H1_SERVICE_NAME=%s\n' "$SERVICE"
printf 'H1_STARTS_AT_UTC=%s\n' "$(python3.12 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["starts_at_utc"])' "$HANDOFF")"
printf 'H1_SECOND_TABBY_COMMAND=watch -n 10 -- bash %q %q\n' \
  "$SOURCE_ROOT/ops/h1_campaign/monitor.sh" "$HANDOFF"
