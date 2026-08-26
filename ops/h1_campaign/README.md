# H1 prospective campaign launch pack V1

This package prepares one human-operated Hyperliquid H1 campaign. It never
opens a collector connection during preparation and never authorizes private
data or order execution. The permanent boundary is
`PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY`.

## Frozen launch identity

- start: `2026-08-27T19:00:00Z`;
- operator arm deadline: `2026-08-27T18:30:00Z`;
- unique slug: `h1-20260827t190000z-7b91d4e2`;
- service: `hyperlab-h1-20260827t190000z-7b91d4e2.service`;
- raw ceiling: 137,438,953,472 bytes (128 GiB);
- reserved safety margin: 17,179,869,184 bytes (16 GiB);
- initial free-space admission: 154,618,822,656 bytes (144 GiB).

The launch must not silently reduce the 7–14 day window or the 128 GiB raw
ceiling. Insufficient free bytes stop the workflow before collection. On
resume, admission requires the unconsumed raw budget plus the same 16 GiB
margin.

## Fee review

`config/paper/hyperliquid-tier0-fee-review-2026-08-26.json` records the official
public Hyperliquid fee page retrieved at `2026-08-26T20:53:53Z`. Tier-0 base
perpetual fees remain maker 1.5 bps and taker 4.5 bps, so the historical fee
artifact and preregistered H1 policy remain unchanged. This is a point-in-time
technical cost input, not economic evidence.

## Reproducible dependency bootstrap

`bootstrap-linux.sh` creates a fresh CPython 3.12.13 virtual environment without
system site packages. It installs only `requirements-runtime.lock` with
`--require-hashes`, `--only-binary=:all:`, three retries, a 30-second request
timeout, no prompts, and a 30-minute outer timeout. HyperLab itself runs from
the detached, hash-verified source tree via an absolute `PYTHONPATH`; there is
no editable or ad hoc package install. The only runtime downloads are the
public wheel files whose acceptable SHA-256 values are already in the
canonical lock.

## Artifact flow

After the final commit, run `New-H1CampaignBundle.ps1` locally. It requires a
clean target branch, creates and verifies a Git bundle, invokes the canonical
H1 preparation function without network, and publishes a new output directory
containing:

- `hyperlab-h1-prospective-campaign-launch-v1.bundle`;
- `handoff.json` and `handoff.sha256`;
- `launch-files.sha256`;
- `campaign-seed/campaign-manifest.json` and its pin;
- `campaign-seed/state/health.json` in `PREPARED_NOT_STARTED`.

The handoff binds the final commit, bundle, launch plan, canonical policy hash,
raw policy file, fee artifact, fee review, runtime lock, campaign manifest,
remote roots, disk budget, and service name. The commit cannot be embedded in
its own Git bytes, so the separately hashed handoff is the non-circular binding
between source identity and the canonical campaign manifest.

## Persistent service and recovery

The rendered systemd unit runs as `hyperlab`, is read-only except for the one
campaign root, exposes no port, drops all capabilities, unsets common secret
variables, and permits only public IPv4/IPv6/Unix sockets. It waits locally
until the frozen start, then `exec`s only:

```text
python -m hyperlab research-data h1-collect --campaign-root ... --config ...
```

If authenticated raw data already exists, the wrapper adds only `--resume`.
`SIGINT` is the service stop signal; exit 130 is successful and non-restarting,
which preserves `INTERRUPTED_RECOVERABLE`. Other failures restart after 60
seconds, at most three starts per 30 minutes. The unit is never overwritten:
installation uses a new temporary inode and an atomic hard-link that fails if
the target already exists.

`monitor.sh` is read-only. It combines exact systemd state/MainPID identity,
the last 30 journal lines, `state/health.json`, and a process-command check. A
stale or forged PID, an active unit without admissible health, and a collector
that exits before health publication all fail closed.

## Human-only boundary

Codex does not execute `vps-install.sh`, SSH, SCP, SFTP, systemd, or the H1
collector. The exact Windows PowerShell and `Tabby - VPS` Bash blocks are
generated in the final handoff report only after the final commit and artifact
hashes exist. The Windows block uses SFTP only to create the unique incoming
directory and SCP for transfer; the Tabby block is the only Bash block.
