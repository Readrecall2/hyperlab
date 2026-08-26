# H1 prospective campaign launch pack V1

This package prepares one human-operated Hyperliquid H1 campaign. It never
opens a collector connection during preparation and never authorizes private
data or order execution. The permanent boundary is
`PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY`.

## Frozen launch identity

- start: `2026-08-27T21:00:00Z`;
- operator arm deadline: `2026-08-27T20:30:00Z`;
- unique slug: `h1-20260827t210000z-e52a227b`;
- service: `hyperlab-h1-20260827t210000z-e52a227b.service`;
- raw ceiling: 137,438,953,472 bytes (128 GiB);
- reserved safety margin: 17,179,869,184 bytes (16 GiB);
- initial free-space admission: 154,618,822,656 bytes (144 GiB).
- discovered free bytes on the admitted volume: 199,487,336,448;
- discovery margin above admission: 44,868,513,792 bytes;
- lightweight home incoming ceiling: 67,108,864 bytes (64 MiB).

The launch must not silently reduce the 7–14 day window or the 128 GiB raw
ceiling. Insufficient free bytes stop the workflow before collection. On
resume, admission requires the unconsumed raw budget plus the same 16 GiB
margin.

The exact authoritative volume contract is `/dev/sdb`, `ext4`, mounted read-write
at `/mnt/HC_Volume_106716684`, with Hetzner model `Volume` and stable serial
`106716684` when Linux exposes them. Incoming transfer remains under
`/home/hyperlab/hyperlab-h1/incoming/<slug>`. The detached source and campaign
roots are unique leaves under `/mnt/HC_Volume_106716684/hyperlab-h1`. Symlinks,
a different `findmnt` target/source/filesystem, read-only options, insufficient
free bytes, and pre-existing campaign leaves all refuse admission.
Free bytes are read from column four of portable `df -PB1` output. The GNU
coreutils-incompatible combination of `-P` with `--output` is forbidden by a
source-and-render regression test.

## Fee review

`config/paper/hyperliquid-tier0-fee-review-2026-08-26T223048Z.json` records the
official public Hyperliquid fee page retrieved at `2026-08-26T22:30:48Z`. Tier-0 base
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
- the byte-pinned systemd unit, operator scripts, source/policy inventory, and
  four shell-separated human blocks: V6 preservation/disable, V7 volume
  preparation, Windows transfer, and V7 installation/arming.

The handoff binds the final commit, bundle, launch plan, canonical policy hash,
raw policy file, fee artifact, fee review, runtime lock, campaign manifest,
remote roots, disk budget, and service name. The commit cannot be embedded in
its own Git bytes, so the separately hashed handoff is the non-circular binding
between source identity and the canonical campaign manifest.

Every byte-level identity for a Git-tracked policy, fee, readiness, launch-plan,
or source artifact hashes the exact `HEAD` blob rather than platform-materialized
worktree bytes. A Windows worktree may differ only by reversible CRLF expansion
of the canonical LF blob. Any other byte difference, untracked identity path,
missing file, or symlink fails generation locally before a handoff is created.
Consequently, a clean Linux checkout recomputes the same evidence as Windows.

## Persistent service and recovery

The rendered systemd unit runs as `hyperlab`, is read-only except for the one
campaign root on the admitted volume, exposes no port, drops all capabilities, unsets common secret
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

The host-side `vps-preflight` remains the authoritative physical-volume check:
it authenticates `/dev/sdb`, the exact ext4 mount and its read-write options,
model/serial, capacity, source identity, inventory, manifest, NTP, paths and
boundary before the unit is installed. The unit's distinct
`service-preflight` runs inside `ProtectSystem=strict`; it does not reinterpret
the expected read-only parent namespace as physical volume state. Instead it
re-authenticates the handoff, commit, inventory, manifest, paths, deadline and
public-only boundary, measures capacity from the campaign path, and proves the
only authorized write surface with an exclusive bounded probe in that campaign
root. Admission fsyncs the probe file and directory, removes only that probe,
then fsyncs the directory again. Any failure prevents `ExecStart`.

`monitor.sh` is read-only. It combines exact systemd state/MainPID identity,
the last 30 journal lines, `state/health.json`, and a process-command check. A
stale or forged PID, an active unit without admissible health, and a collector
that exits before health publication all fail closed.

## Human-only boundary

Codex does not execute `vps-install.sh`, SSH, SCP, SFTP, systemd, or the H1
collector. The V6 preservation/disable block is followed by the exact V7
volume-preparation Tabby block, Windows PowerShell transfer block, and
installation/arming Tabby block; all are generated only after the final commit
and artifact hashes exist. The volume block validates the already-mounted
volume and uses only `sudo install -d` to prepare its HyperLab base directories.
The Windows block uses SFTP only to create the unique home incoming directory
and SCP for transfer. The final Bash block revalidates the volume before any
campaign root is created.

The V2 slug `h1-20260827t190000z-7b91d4e2` remains byte-identical and was never
transferred or launched. Its append-only receipt marks it
`ABANDONED_BEFORE_TRANSFER_INSUFFICIENT_ROOT_DISK`; it is not reused by this
continuation.

The V4 slug `h1-20260827t200000z-21fa9dba` also remains byte-identical. Human
execution stopped before `sudo install` because GNU `df` rejects `-P` combined
with `--output`. Its append-only receipt marks it
`ABANDONED_BEFORE_VOLUME_PREPARATION_DF_OPTION_INCOMPATIBILITY` and records
`NO_DIRECTORY_PREPARATION_NO_TRANSFER_NO_SERVICE_NO_NETWORK_COLLECTION`.

The V5 slug `h1-20260827t180000z-a007df56` remains byte-identical and must not
be repaired or reused. Transfer, exact clone, venv bootstrap, import preflight,
and campaign-seed installation completed, then portable-identity verification
failed before systemd installation. Its append-only receipt marks it
`ABANDONED_AFTER_TRANSFER_BEFORE_SYSTEMD_PORTABLE_IDENTITY_MISMATCH` and records
that no service, collector, or network collection started.

The V6 slug `h1-20260827t210000z-c0043345` also remains byte-identical. Its
external physical-volume preflight was green, but its systemd `ExecCondition`
reused that host check inside `ProtectSystem=strict` and therefore saw the
expected read-only parent namespace. `ExecStart` never ran, MainPID and restart
count remained zero, health stayed `PREPARED_NOT_STARTED`, and no raw root was
created. The append-only receipt records
`SYSTEMD_EXEC_CONDITION_SANDBOX_FALSE_READ_ONLY_NO_EXECSTART_NO_COLLECTION`.
V7 includes a separate fail-closed operator block that authenticates this exact
state, disables V6, and preserves its unit and all three V6 roots.
