# Prediction Markets Candidate V1 — future local campaign pack

The committed pack
`prediction-markets-v1-20260901t000000z-aa60c0ff/` is intentionally
`AWAITING_HUMAN_EXECUTION`. It was generated locally and never launched.

It is isolated from H1: `vps_or_h1_path=NONE`. No SSH, VPS, systemd, wallet,
account, credential or order route is part of this pack.

The schedule contains 672 hourly slots per venue across the frozen 28-day
train/validation/holdout horizon. Each slot admits at most 120 seconds and has a
deterministic collection identity. Slots cannot overlap; a missed slot becomes
an explicit gap and cannot be backfilled. A terminal result is never retried.

Before a future human run, read `campaign-manifest.json`, verify
`campaign-manifest.sha256`, select the exact scheduled ordinal, and copy the
corresponding command from `operator-commands.json` into local Windows
PowerShell. Every `--output-root` must be new. Safe monitoring is the shard's
`reports/health.json`; Ctrl+C closes only admitted local raw evidence. The
completion signal is `reports/result.json` with an explicit terminal health.

The commands are text-only and deliberately contain placeholders. No command
in this directory should be pointed at the active Hyperliquid H1 campaign.
