#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

fail() {
  printf 'PREDICTION_RECOVERY_ROLLBACK_REFUSED:%s\n' "$1" >&2
  exit 4
}
if (($# != 2)); then
  fail 'usage: rollback.sh recovery|rollback HANDOFF_JSON'
fi
MODE=$1
HANDOFF=$2
[[ $MODE == recovery || $MODE == rollback ]] || fail 'mode must be recovery or rollback'
[[ $(id -un) == hyperlab ]] || fail 'run as hyperlab'
[[ -f "$HANDOFF" && ! -L "$HANDOFF" ]] || fail 'handoff is absent or unsafe'
HANDOFF=$(readlink -f -- "$HANDOFF")
INCOMING_ROOT=$(dirname -- "$HANDOFF")
[[ -f "$INCOMING_ROOT/scripts/preflight.py" && ! -L "$INCOMING_ROOT/scripts/preflight.py" ]] || fail 'recovery preflight is absent or unsafe'
mapfile -t VALUES < <(python3.12 -I - "$HANDOFF" <<'PY'
import json,re,sys
d=json.load(open(sys.argv[1],encoding='utf-8'))
for key in ('polymarket','kalshi','dashboard'):
 value=d['services'][key]
 assert re.fullmatch(r'hyperlab-pm-[a-z0-9-]+-(polymarket|kalshi|dashboard)\.service',value)
 print(value)
print(d['campaign_root'])
PY
)
(( ${#VALUES[@]} == 4 )) || fail 'handoff service fields are incomplete'
POLYMARKET_SERVICE=${VALUES[0]}
KALSHI_SERVICE=${VALUES[1]}
DASHBOARD_SERVICE=${VALUES[2]}
CAMPAIGN_ROOT=${VALUES[3]}
case "$CAMPAIGN_ROOT" in
  /mnt/HC_Volume_106716684/hyperlab-prediction-markets/campaigns/*) ;;
  *) fail 'campaign root leaves Prediction Markets tree' ;;
esac
if [[ $MODE == rollback ]]; then
  sudo systemctl stop "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" "$DASHBOARD_SERVICE" || true
  sudo systemctl disable "$POLYMARKET_SERVICE" "$KALSHI_SERVICE" "$DASHBOARD_SERVICE" || true
  printf 'PREDICTION_ROLLBACK_DISARMED_RAW_PRESERVED\n'
  printf 'PREDICTION_CAMPAIGN_ROOT_PRESERVED=%s\n' "$CAMPAIGN_ROOT"
  exit 0
fi
STAMP=$(date -u +%Y%m%dT%H%M%S%NZ)
RESUME_REPORT="$INCOMING_ROOT/resume-preflight-$STAMP.json"
python3.12 -I "$INCOMING_ROOT/scripts/preflight.py" resume --handoff "$HANDOFF" --report "$RESUME_REPORT" \
  || fail "resume preflight refused; inspect $RESUME_REPORT"
sudo systemctl enable --now "$DASHBOARD_SERVICE"
for VENUE in polymarket kalshi; do
  case "$VENUE" in
    polymarket) SERVICE=$POLYMARKET_SERVICE ;;
    kalshi) SERVICE=$KALSHI_SERVICE ;;
  esac
  REPORT="$INCOMING_ROOT/recovery-network-$VENUE-$STAMP.json"
  if python3.12 -I "$INCOMING_ROOT/scripts/preflight.py" network --venue "$VENUE" --report "$REPORT"; then
    sudo systemctl enable --now "$SERVICE"
    printf 'PREDICTION_RECOVERY_VENUE_STARTED=%s\n' "$VENUE"
  else
    sudo systemctl stop "$SERVICE" || true
    sudo systemctl disable "$SERVICE" || true
    printf 'PREDICTION_RECOVERY_VENUE_REFUSED_PUBLIC_SOURCE_UNAVAILABLE=%s REPORT=%s\n' "$VENUE" "$REPORT"
  fi
done
printf 'PREDICTION_RECOVERY_RESUME_REQUESTED_NO_SLOT_RETRY\n'
