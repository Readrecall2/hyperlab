# H1 Prospective Campaign Launch Pack V1

Technical readiness target:
`H1_PROSPECTIVE_CAMPAIGN_LAUNCH_PACK_V1_GREEN_SYSTEMD_PREFLIGHT_AWAITING_HUMAN_EXECUTION`.

Economic status remains `ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE`. Nothing in this
pack demonstrates alpha, profitability, capacity, or permission for real
trading.

## Scope and frozen campaign

The pack stages the first prospective H1 observation window under the existing
`PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY` policy. It preserves the frozen
BTC/ETH/SOL/HYPE universe, all registered variants, train days 0–7, validation
days 7–10, sealed holdout days 10–14, and every existing economic gate.

The V7 replacement campaign is armed for `2026-08-27T22:00:00Z`. Complete the human transfer
and systemd installation no later than `2026-08-27T21:30:00Z`. Before the
frozen start the service only waits locally; it does not open the public
collector. A first launch after the frozen start is refused. A later restart is
allowed only when the same authenticated raw root already exists.

The official Hyperliquid fee page was reviewed at
`2026-08-26T20:53:53Z`, less than 24 hours before the start. Base tier-0
standard perpetual fees remain 1.5 bps maker and 4.5 bps taker. The review
receipt is append-only; the historical 2026-08-16 artifact and H1 policy were
not rewritten.

## Admission sequence

The human workflow must pass these gates in order:

1. local clean-branch and exact-commit checks, Git bundle creation and bundle
   verification;
2. local offline canonical H1 preparation in a new seed root;
3. SHA-256 verification of every transfer file;
4. unique lightweight incoming root under `/home/hyperlab/hyperlab-h1`, with
   source and campaign roots under `/mnt/HC_Volume_106716684/hyperlab-h1`;
5. exact detached commit and clean clone;
6. exact `/dev/sdb` -> `/mnt/HC_Volume_106716684` `ext4` read-write identity,
   stable serial `106716684` when exposed, synchronized NTP, safe ownership, and free bytes greater than the remaining
   128 GiB raw budget plus the fixed 16 GiB margin;
7. fresh CPython 3.12.13 venv, hash-locked wheels, import and CLI preflights;
8. exact policy, fee artifact, fee review, lock, handoff, and campaign-manifest
   hashes;
9. no live Research writer and no pre-existing unit target;
10. `systemd-analyze verify`, atomic unit install, then an admissible systemd
    MainPID plus health state.

No step deletes, reuses, or overwrites an earlier incoming, source, campaign,
raw, manifest, or unit path.

The human discovery measured 199,487,336,448 free bytes on the admitted volume
against 154,618,822,656 required bytes, leaving 44,868,513,792 bytes of measured
margin. The workflow does not format, partition, resize, remount, edit `fstab`,
or move prior data. It uses `sudo install -d` only for the owned `0700` volume
base directories. A missing/read-only/different mount or insufficient capacity
stops before campaign creation.

The full host preflight and systemd `ExecCondition` are deliberately distinct.
The host gate proves the physical mount/device/ext4/read-write/model/serial and
all identities. Inside `ProtectSystem=strict`, the service gate re-authenticates
handoff, commit, inventory, manifest, paths, deadline and the public-only
boundary, but tests write access only in the sole `ReadWritePaths` campaign
root. Its exclusive bounded probe fsyncs its file and containing directory,
removes only that probe, and fsyncs the directory again. It measures remaining
capacity through the campaign root. A missing permission, stale identity,
missed deadline, active writer or failed fsync stops before `ExecStart`; neither
the volume root nor source root is added to `ReadWritePaths`.

The earlier V2 package `h1-20260827t190000z-7b91d4e2` remains byte-identical,
was neither transferred nor launched, and is append-only marked
`ABANDONED_BEFORE_TRANSFER_INSUFFICIENT_ROOT_DISK` with
`NO_TRANSFER_NO_LAUNCH_NO_NETWORK_COLLECTION`. Its identity and paths are never
reused.

V6 `h1-20260827t210000z-c0043345` passed the physical-volume and identity gates,
then its reused host preflight saw the intentionally read-only parent namespace
created by `ProtectSystem=strict`. The unit remained loaded and enabled but
inactive/dead with MainPID 0, NRestarts 0, initial health and no raw root;
`ExecStart` and collection never occurred. V7 first authenticates those exact
facts, disables V6 without deleting its unit or roots, and records
`SYSTEMD_EXEC_CONDITION_SANDBOX_FALSE_READ_ONLY_NO_EXECSTART_NO_COLLECTION`.

## Runtime and observability

The service survives SSH disconnection. It uses `SIGINT`, a 180-second stop
allowance, and no forced SIGKILL. The collector closes the authenticated tail
and publishes `INTERRUPTED_RECOVERABLE`; manual start of the same service then
uses `--resume` against the same campaign manifest. Automatic retries are
limited to three starts per 30 minutes with 60 seconds between failures.

The second Tabby tab runs the read-only monitor every ten seconds. It reports
systemd state, MainPID, restart count, the last 30 journal lines, and the exact
health JSON. It refuses a false PID/service identity and never creates or
changes campaign state.

Expected operator signals are distinct:

- `H1_SERVICE_ARMED_PREPARED_NOT_STARTED`: service alive, waiting locally;
- `H1_SERVICE_RUNNING_HEALTH_GREEN`: public collection active after start;
- `INTERRUPTED_RECOVERABLE`: authenticated tail closed, manual resume allowed;
- `COMPLETE_COLLECTION_WINDOW` or `COMPLETE_VERIFIED_THRESHOLDS`: terminal
  collection result, still requiring offline and human review;
- any `*_REFUSED`, fail-closed health, inactive service with nonterminal health,
  or hash divergence: no launch success.

## Absolute exclusions

The pack has no private API, account channel, wallet, signer, seed, private
key, order creation/modification/cancellation, or real-money route. It does not
import or instantiate `hyperliquid.exchange.Exchange`, expose an Internet
port, or create any live/trade/mainnet command.
