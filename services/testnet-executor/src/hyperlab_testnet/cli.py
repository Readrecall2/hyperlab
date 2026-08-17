"""Fail-closed operator CLI for the isolated Hyperliquid Testnet executor.

The command group intentionally has no endpoint selector and no Mainnet/live
alias.  Every network-capable command reconstructs the exact build identity,
loads an exact TESTNET/TESTNET_EXECUTION receipt, and derives a dedicated API
wallet signer before the first public Testnet read.  Only ``smoke-order``,
``cancel``, ``run`` (dead-man switch only), and ``kill`` can sign actions.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated, NoReturn, TypeVar, cast

import typer

from .adapter import (
    ActionOutcome,
    HyperliquidTestnetAdapter,
    OutcomeKind,
    RequestsJsonTransport,
    perp_constraints_from_meta,
    read_testnet_meta,
    testnet_signer_from_secret,
)
from .authorization import TestnetAuthorizationBundle, load_testnet_authorization_bundle
from .build_identity import (
    current_testnet_build_identity,
    validate_runtime_identity,
    validate_runtime_process_boundary,
)
from .canonical import (
    JsonValue,
    canonical_json,
    decimal_text,
    decimal_value,
    deterministic_id,
    utc_text,
)
from .config import TestnetConfig
from .credentials import load_testnet_credentials
from .engine import ExecutionResult, TestnetExecutionEngine
from .evidence import write_testnet_readiness_bundle
from .models import (
    OrderSide,
    RuntimeState,
    TestnetOrderIntent,
    TimeInForce,
    deterministic_cloid,
    validate_cloid,
)
from .reconciliation import ExchangeFirstReconciler, ReconciliationPlan, RunScopedStore
from .runtime import (
    PreflightReport,
    TestnetPreflight,
    TestnetRuntime,
    TestnetUserEventSource,
    WebsocketClientConnector,
)
from .store import TestnetStore, WalletLeaseError
from .validation import run_testnet_software_validation

_MAX_CONFIG_BYTES = 1024 * 1024
_HTTP_TIMEOUT_SECONDS = 10.0
_WEBSOCKET_TIMEOUT_SECONDS = 5.0
_ACTION_TTL_MS = 30_000
_PAUSE_CONFIRMATION = "TESTNET-PAUSE"
_KILL_CONFIRMATION = "TESTNET-KILL"
_ORDER_CONFIRMATION = "TESTNET-ORDER"
_CANCEL_CONFIRMATION = "TESTNET-CANCEL"

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    help=(
        "HyperLab 0.3 Testnet-only executor. No endpoint selection, Mainnet route, "
        "or autonomous order generation exists."
    ),
)


class CliError(RuntimeError):
    """A bounded operator input or command precondition failed closed."""


@dataclass(slots=True)
class _OnlineContext:
    config: TestnetConfig
    store: TestnetStore
    scoped_store: RunScopedStore
    adapter: HyperliquidTestnetAdapter
    reconciler: ExchangeFirstReconciler
    engine: TestnetExecutionEngine
    preflight: TestnetPreflight
    runtime: TestnetRuntime

    def close(self) -> None:
        self.store.close()


_T = TypeVar("_T")


def _now() -> datetime:
    return datetime.now(UTC)


def _now_ms() -> int:
    return int(_now().timestamp() * 1_000)


def _json(payload: dict[str, JsonValue]) -> None:
    typer.echo(canonical_json(payload))


def _fail(message: str, *, exit_code: int = 2) -> NoReturn:
    typer.echo(f"ERROR: {message}", err=True)
    raise typer.Exit(exit_code)


def _guard(action: Callable[[], _T]) -> _T:
    """Render only reviewed, secret-free errors and suppress tracebacks."""

    try:
        return action()
    except typer.Exit:
        raise
    except (
        CliError,
        FileNotFoundError,
        IsADirectoryError,
        NotADirectoryError,
        PermissionError,
        TypeError,
        ValueError,
    ) as error:
        _fail(f"{type(error).__name__}: {error}")
    except Exception as error:
        # Unexpected third-party errors may embed request objects.  Report the
        # class only; adapter/domain exceptions deliberately sanitize details.
        _fail(f"FAIL_CLOSED ({type(error).__name__})")


def _read_regular_file(path: Path, *, label: str, byte_limit: int) -> bytes:
    if not isinstance(path, Path):
        raise TypeError(f"{label} path must be a Path")
    if path.is_symlink() or not path.is_file():
        raise CliError(f"{label} must be an existing regular file")
    size = path.stat().st_size
    if size > byte_limit:
        raise CliError(f"{label} exceeds the compiled byte limit")
    return path.read_bytes()


def _database_path(path: Path, *, must_exist: bool) -> Path:
    if not isinstance(path, Path):
        raise TypeError("database path must be a Path")
    if not path.is_absolute():
        raise CliError("database path must be absolute")
    target = path.resolve(strict=False)
    if path.is_symlink() or target.is_symlink():
        raise CliError("symbolic-link database paths are refused")
    if must_exist and not target.is_file():
        raise FileNotFoundError(f"Testnet database does not exist: {target}")
    if target.exists() and not target.is_file():
        raise CliError("database path must identify a regular file")
    if target.parent.exists() and (
        not target.parent.is_dir() or target.parent.is_symlink()
    ):
        raise CliError("database parent must be a regular directory")
    return target


def _load_config(path: Path) -> TestnetConfig:
    config = TestnetConfig.from_json_bytes(
        _read_regular_file(path, label="Testnet config", byte_limit=_MAX_CONFIG_BYTES)
    )
    validate_runtime_identity(config)
    return config


def _load_authorization(
    receipt_path: Path,
    *,
    manifest_path: Path,
    evidence_root: Path,
    validation_report_path: Path,
    config: TestnetConfig,
) -> TestnetAuthorizationBundle:
    return load_testnet_authorization_bundle(
        receipt_path,
        manifest_path=manifest_path,
        evidence_root=evidence_root,
        validation_report_path=validation_report_path,
        config=config,
    )


def _build_adapter(config: TestnetConfig) -> HyperliquidTestnetAdapter:
    credentials = load_testnet_credentials(config, environ=os.environ)
    revealed = credentials.private_key.reveal_for_signer()
    try:
        signer = testnet_signer_from_secret(revealed)
    finally:
        revealed = "[REDACTED]"
    credentials.validate_derived_api_wallet_address(signer.address)
    transport = RequestsJsonTransport()
    meta = read_testnet_meta(
        origin=config.http_endpoint,
        http=transport,
        timeout_seconds=_HTTP_TIMEOUT_SECONDS,
    )
    constraints = perp_constraints_from_meta(meta)
    adapter = HyperliquidTestnetAdapter(
        origin=config.http_endpoint,
        account_address=config.account_address,
        api_wallet_address=config.api_wallet_address,
        signer=signer,
        asset_constraints_by_coin=constraints,
        http=transport,
        timeout_seconds=_HTTP_TIMEOUT_SECONDS,
        action_ttl_ms=_ACTION_TTL_MS,
    )
    del credentials
    return adapter


def _open_mutable_store(path: Path) -> TestnetStore:
    return TestnetStore(_database_path(path, must_exist=False))


def _open_existing_mutable_store(path: Path) -> TestnetStore:
    return TestnetStore(_database_path(path, must_exist=True))


def _open_readonly_store(path: Path) -> TestnetStore:
    return TestnetStore.open_existing_readonly(_database_path(path, must_exist=True))


def _build_online_context(
    *,
    config_path: Path,
    receipt_path: Path,
    manifest_path: Path,
    evidence_root: Path,
    validation_report_path: Path,
    database_path: Path,
) -> _OnlineContext:
    validate_runtime_process_boundary()
    config = _load_config(config_path)
    authorization = _load_authorization(
        receipt_path,
        manifest_path=manifest_path,
        evidence_root=evidence_root,
        validation_report_path=validation_report_path,
        config=config,
    )
    adapter = _build_adapter(config)
    store = _open_mutable_store(database_path)
    try:
        durable = store.create_run(config)
        if dict(durable.config) != config.to_dict():
            raise CliError("durable run configuration differs from the reviewed config")
        scoped = RunScopedStore(store, run_id=config.run_id, clock=_now)
        reconciler = ExchangeFirstReconciler(adapter, scoped)
        engine = TestnetExecutionEngine(
            adapter=adapter,
            store=scoped,
            limits=config.risk_limits,
            clock=_now,
            reconciler=reconciler,
        )
        preflight = TestnetPreflight(
            config=config,
            adapter=adapter,
            store=scoped,
            authorization_check=lambda: authorization.receipt,
            authorization_evidence={
                "validation_id": authorization.validation.validation_id,
                "validation_report_sha256": authorization.validation_report_sha256,
            },
            clock_ms=_now_ms,
        )
        event_source = TestnetUserEventSource(
            endpoint=config.ws_endpoint,
            account_address=config.account_address,
            connector=WebsocketClientConnector(),
            timeout_seconds=_WEBSOCKET_TIMEOUT_SECONDS,
        )
        runtime = TestnetRuntime(
            config=config,
            store=scoped,
            preflight=preflight,
            reconciler=reconciler,
            engine=engine,
            event_source=event_source,
            clock=_now,
        )
    except BaseException:
        store.close()
        raise
    return _OnlineContext(
        config=config,
        store=store,
        scoped_store=scoped,
        adapter=adapter,
        reconciler=reconciler,
        engine=engine,
        preflight=preflight,
        runtime=runtime,
    )


def _preflight_payload(report: PreflightReport) -> dict[str, JsonValue]:
    return {
        "api_wallet_address": report.api_wallet_address,
        "api_wallet_valid_until_ms": report.api_wallet_valid_until_ms,
        "asset_count": report.asset_count,
        "config_hash": report.config_hash,
        "mark_count": report.mark_count,
        "mode": "PREFLIGHT_ONLY_NO_SIGNED_ACTION",
        "ready": True,
    }


def _plan_payload(plan: ReconciliationPlan) -> dict[str, JsonValue]:
    return {
        "action_resolution_count": len(plan.action_resolutions),
        "clean": plan.clean,
        "event_count": len(plan.events),
        "issue_codes": [issue.code for issue in plan.issues],
        "required_runtime_state": plan.required_runtime_state.value,
        "snapshot_hash": plan.snapshot_hash,
    }


def _execution_payload(result: ExecutionResult) -> dict[str, JsonValue]:
    outcome: ActionOutcome | None = result.outcome
    return {
        "action_id": result.action.action_id,
        "action_status": result.action.status.value,
        "cloid": result.order.intent.cloid if result.order is not None else None,
        "code": outcome.code if outcome is not None else result.action.code,
        "order_status": result.order.status.value if result.order is not None else None,
        "outcome": outcome.kind.value if outcome is not None else None,
        "reused": result.reused,
        "venue_order_id": (
            result.order.venue_order_id if result.order is not None else None
        ),
    }


def _select_run(store: TestnetStore, run_id: str | None) -> str:
    runs = store.list_runs()
    if run_id is not None:
        candidate = run_id.lower()
        store.get_run(candidate)
        return candidate
    if len(runs) != 1:
        raise CliError("--run-id is required unless the database contains exactly one run")
    return runs[0]


def _require_confirmation(observed: str, expected: str) -> None:
    if observed != expected:
        raise CliError(f"confirmation must be exactly {expected!r}")


@app.command("build-identity")
def build_identity() -> None:
    """Print the current secret-free build/source/strategy identity."""

    def execute() -> None:
        identity = current_testnet_build_identity()
        _json(cast(dict[str, JsonValue], identity.to_dict()))

    _guard(execute)


@app.command("evidence")
def evidence(
    config: Annotated[Path, typer.Option("--config")],
    validation_report: Annotated[Path, typer.Option("--validation-report")],
    evidence_root: Annotated[Path, typer.Option("--evidence-root")],
    manifest: Annotated[Path, typer.Option("--manifest")],
    receipt: Annotated[Path, typer.Option("--receipt")],
) -> None:
    """Create a new compiled TESTNET/TESTNET_EXECUTION evidence bundle."""

    def execute() -> None:
        loaded = _load_config(config)
        bundle = write_testnet_readiness_bundle(
            loaded,
            evidence_root=evidence_root,
            manifest_path=manifest,
            receipt_path=receipt,
            validation_report_path=validation_report,
        )
        _json(
            {
                "config_hash": loaded.config_hash,
                "evidence_count": len(bundle.manifest.evidence),
                "environment": bundle.manifest.environment.value,
                "manifest": str(bundle.manifest_path.resolve()),
                "purpose": bundle.manifest.purpose.value,
                "ready": True,
                "receipt": str(bundle.receipt_path.resolve()),
                "validation_report_sha256": bundle.validation_report_sha256,
            }
        )

    _guard(execute)


@app.command("validate-software")
def validate_software(
    config: Annotated[Path, typer.Option("--config")],
    output_directory: Annotated[Path, typer.Option("--output-directory")],
) -> None:
    """Run the fixed offline software gates and create immutable local evidence."""

    def execute() -> None:
        loaded = _load_config(config)
        report = run_testnet_software_validation(loaded, output_directory)
        _json(
            {
                "check_count": len(report.checks),
                "output_root": report.output_root,
                "passed": report.passed,
                "report": str(Path(report.output_root) / "software-validation.json"),
                "validation_id": report.validation_id,
            }
        )

    _guard(execute)


@app.command("preflight")
def preflight(
    config: Annotated[Path, typer.Option("--config")],
    receipt: Annotated[Path, typer.Option("--receipt")],
    manifest: Annotated[Path, typer.Option("--manifest")],
    evidence_root: Annotated[Path, typer.Option("--evidence-root")],
    validation_report: Annotated[Path, typer.Option("--validation-report")],
    database: Annotated[Path, typer.Option("--database")],
) -> None:
    """Validate identity, credentials, receipt, persistence and reads; sign nothing."""

    def execute() -> None:
        context = _build_online_context(
            config_path=config,
            receipt_path=receipt,
            manifest_path=manifest,
            evidence_root=evidence_root,
            validation_report_path=validation_report,
            database_path=database,
        )
        try:
            report = context.runtime.start(dry_run=True)
            _json(_preflight_payload(report))
        finally:
            context.close()

    _guard(execute)


@app.command("status")
def status(
    database: Annotated[Path, typer.Option("--database")],
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
) -> None:
    """Inspect an existing database read-only, with no credential or network access."""

    def execute() -> None:
        store = _open_readonly_store(database)
        try:
            selected = _select_run(store, run_id)
            run = store.get_run(selected)
            integrity = store.inspect_integrity_readonly(selected)
            orders = store.list_orders(selected)
            fills = store.list_fills(selected)
            ambiguous = store.list_ambiguous_actions(selected)
            counts: dict[str, int] = {}
            for order in orders:
                counts[order.status.value] = counts.get(order.status.value, 0) + 1
            _json(
                {
                    "ambiguous_action_count": len(ambiguous),
                    "audit_count": run.audit_count,
                    "fill_count": len(fills),
                    "integrity_issue_codes": [issue.code for issue in integrity.issues],
                    "integrity_ok": integrity.ok,
                    "last_reconciled_at": (
                        utc_text(run.last_reconciled_at)
                        if run.last_reconciled_at is not None
                        else None
                    ),
                    "order_count": len(orders),
                    "order_status_counts": cast(dict[str, JsonValue], counts),
                    "run_id": selected,
                    "runtime_reason": run.state_reason,
                    "runtime_state": run.runtime_state.value,
                }
            )
        finally:
            store.close()

    _guard(execute)


@app.command("reconcile")
def reconcile(
    config: Annotated[Path, typer.Option("--config")],
    receipt: Annotated[Path, typer.Option("--receipt")],
    manifest: Annotated[Path, typer.Option("--manifest")],
    evidence_root: Annotated[Path, typer.Option("--evidence-root")],
    validation_report: Annotated[Path, typer.Option("--validation-report")],
    database: Annotated[Path, typer.Option("--database")],
) -> None:
    """Run preflight plus one exchange-first reconciliation; sign nothing."""

    def execute() -> None:
        context = _build_online_context(
            config_path=config,
            receipt_path=receipt,
            manifest_path=manifest,
            evidence_root=evidence_root,
            validation_report_path=validation_report,
            database_path=database,
        )
        lease_held = False
        try:
            context.scoped_store.acquire_writer_lease()
            lease_held = True
            context.scoped_store.set_runtime_state(
                RuntimeState.STARTING,
                reason="OPERATOR_RECONCILE",
            )
            context.preflight.run()
            plan = context.reconciler.reconcile(captured_at_ms=_now_ms())
            if plan.clean:
                context.scoped_store.set_runtime_state(
                    RuntimeState.STOPPED,
                    reason="RECONCILIATION_COMPLETE",
                )
            _json(_plan_payload(plan))
            if not plan.clean:
                raise typer.Exit(3)
        except typer.Exit:
            raise
        except Exception:
            state = context.scoped_store.runtime_state()
            if state not in {
                RuntimeState.KILLED,
                RuntimeState.MANUAL_REVIEW,
                RuntimeState.PAUSED,
            }:
                context.scoped_store.set_runtime_state(
                    RuntimeState.PAUSED,
                    reason="RECONCILIATION_COMMAND_FAILED",
                )
            raise
        finally:
            if lease_held:
                context.scoped_store.release_writer_lease()
            context.close()

    _guard(execute)


@app.command("run")
def run_service(
    config: Annotated[Path, typer.Option("--config")],
    receipt: Annotated[Path, typer.Option("--receipt")],
    manifest: Annotated[Path, typer.Option("--manifest")],
    evidence_root: Annotated[Path, typer.Option("--evidence-root")],
    validation_report: Annotated[Path, typer.Option("--validation-report")],
    database: Annotated[Path, typer.Option("--database")],
) -> None:
    """Run reconciliation/WS/DMS supervision; never generate orders autonomously."""

    def execute() -> None:
        context = _build_online_context(
            config_path=config,
            receipt_path=receipt,
            manifest_path=manifest,
            evidence_root=evidence_root,
            validation_report_path=validation_report,
            database_path=database,
        )
        started = False
        try:
            report = context.runtime.start(dry_run=False)
            started = True
            _json(
                {
                    **_preflight_payload(report),
                    "mode": "SUSTAINED_TESTNET_RUNTIME_NO_AUTONOMOUS_ORDERS",
                }
            )
            while True:
                context.runtime.poll_once()
        except KeyboardInterrupt:
            if started:
                context.runtime.stop(reason="OPERATOR_STOP")
                started = False
            _json({"runtime_state": "STOPPED", "stopped": True})
        except Exception:
            if started:
                with suppress(Exception):
                    context.runtime.stop(reason="RUNTIME_COMMAND_FAILED")
            raise
        finally:
            context.close()

    _guard(execute)


@app.command("pause")
def pause(
    database: Annotated[Path, typer.Option("--database")],
    run_id: Annotated[str, typer.Option("--run-id")],
    confirm: Annotated[str, typer.Option("--confirm")],
) -> None:
    """Persist an emergency local PAUSED state without touching the network."""

    def execute() -> None:
        _require_confirmation(confirm, _PAUSE_CONFIRMATION)
        store = _open_existing_mutable_store(database)
        try:
            record = store.get_run(run_id.lower())
            if record.runtime_state in {RuntimeState.MANUAL_REVIEW, RuntimeState.KILLED}:
                result = record
            else:
                result = store.pause(
                    record.run_id,
                    reason="OPERATOR_PAUSE",
                    at=_now(),
                )
            _json(
                {
                    "network_action_attempted": False,
                    "run_id": result.run_id,
                    "runtime_reason": result.state_reason,
                    "runtime_state": result.runtime_state.value,
                }
            )
        finally:
            store.close()

    _guard(execute)


@app.command("kill")
def kill(
    database: Annotated[Path, typer.Option("--database")],
    run_id: Annotated[str, typer.Option("--run-id")],
    confirm: Annotated[str, typer.Option("--confirm")],
    config: Annotated[Path | None, typer.Option("--config")] = None,
    receipt: Annotated[Path | None, typer.Option("--receipt")] = None,
    manifest: Annotated[Path | None, typer.Option("--manifest")] = None,
    evidence_root: Annotated[Path | None, typer.Option("--evidence-root")] = None,
    validation_report: Annotated[
        Path | None,
        typer.Option("--validation-report"),
    ] = None,
) -> None:
    """Latch account KILLED first, then best-effort arm the emergency DMS."""

    def execute() -> None:
        _require_confirmation(confirm, _KILL_CONFIRMATION)
        authorization_paths = (
            config,
            receipt,
            manifest,
            evidence_root,
            validation_report,
        )
        if any(path is not None for path in authorization_paths) and not all(
            path is not None for path in authorization_paths
        ):
            raise CliError(
                "--config, --receipt, --manifest, --evidence-root, and "
                "--validation-report must be supplied together"
            )
        store = _open_existing_mutable_store(database)
        scoped: RunScopedStore | None = None
        lease_held = False
        try:
            selected = run_id.lower()
            killed = store.kill(selected, reason="OPERATOR_KILL", at=_now())
            if not store.account_kill_latched(selected):
                raise CliError("account-scoped kill latch was not durably proven")
            if any(path is None for path in authorization_paths):
                _json(
                    {
                        "account_kill_latched": True,
                        "deadman_confirmed": False,
                        "deadman_not_attempted_reason": "CONFIG_AND_RECEIPT_NOT_SUPPLIED",
                        "run_id": killed.run_id,
                        "runtime_state": killed.runtime_state.value,
                    }
                )
                raise typer.Exit(3)
            validate_runtime_process_boundary()
            assert config is not None
            assert receipt is not None
            assert manifest is not None
            assert evidence_root is not None
            assert validation_report is not None
            loaded = _load_config(config)
            if loaded.run_id != selected or dict(killed.config) != loaded.to_dict():
                raise CliError("kill config does not exactly match the durable run")
            _load_authorization(
                receipt,
                manifest_path=manifest,
                evidence_root=evidence_root,
                validation_report_path=validation_report,
                config=loaded,
            )
            scoped = RunScopedStore(store, run_id=selected, clock=_now)
            try:
                scoped.acquire_writer_lease()
                lease_held = True
            except WalletLeaseError:
                _json(
                    {
                        "account_kill_latched": True,
                        "deadman_confirmed": False,
                        "deadman_owner": "EXISTING_RUNTIME_WILL_ENFORCE_LATCH",
                        "run_id": selected,
                        "runtime_state": RuntimeState.KILLED.value,
                    }
                )
                raise typer.Exit(3) from None
            adapter = _build_adapter(loaded)
            reconciler = ExchangeFirstReconciler(adapter, scoped)
            engine = TestnetExecutionEngine(
                adapter=adapter,
                store=scoped,
                limits=loaded.risk_limits,
                clock=_now,
                reconciler=reconciler,
            )
            cancel_at_ms = (
                _now_ms() + loaded.risk_limits.deadman_interval_seconds * 1_000
            )
            result = engine.enforce_persisted_kill(cancel_at_ms=cancel_at_ms)
            outcome = result.outcome
            confirmed = outcome is not None and outcome.kind is OutcomeKind.DEADMAN_ARMED
            _json(
                {
                    "account_kill_latched": True,
                    "deadman_confirmed": confirmed,
                    "deadman_outcome": outcome.kind.value if outcome is not None else None,
                    "run_id": selected,
                    "runtime_state": RuntimeState.KILLED.value,
                }
            )
            if not confirmed:
                raise typer.Exit(3)
        finally:
            if lease_held and scoped is not None:
                scoped.release_writer_lease()
            store.close()

    _guard(execute)


def _manual_intent(
    context: _OnlineContext,
    *,
    instrument: str,
    side: OrderSide,
    quantity: Decimal,
    limit_price: Decimal,
    time_in_force: TimeInForce,
    reduce_only: bool,
    ordinal: int,
) -> TestnetOrderIntent:
    decision_id = deterministic_id(
        "hyperliquid_testnet_manual_smoke_decision_v1",
        context.config.run_id,
        instrument,
        side.value,
        decimal_text(quantity),
        decimal_text(limit_price),
        time_in_force.value,
        reduce_only,
        ordinal,
    )
    order_id = TestnetOrderIntent.identifier(
        run_id=context.config.run_id,
        decision_id=decision_id,
        instrument=instrument,
        side=side,
        quantity=quantity,
        limit_price=limit_price,
        time_in_force=time_in_force,
        reduce_only=reduce_only,
        ordinal=ordinal,
    )
    cloid = deterministic_cloid(context.config.run_id, order_id)
    existing = context.scoped_store.get_order(cloid)
    if existing is not None:
        return existing.intent
    return TestnetOrderIntent.create(
        run_id=context.config.run_id,
        decision_id=decision_id,
        instrument=instrument,
        side=side,
        quantity=quantity,
        limit_price=limit_price,
        time_in_force=time_in_force,
        reduce_only=reduce_only,
        created_at=_now(),
        ordinal=ordinal,
    )


@app.command("smoke-order")
def smoke_order(
    config: Annotated[Path, typer.Option("--config")],
    receipt: Annotated[Path, typer.Option("--receipt")],
    manifest: Annotated[Path, typer.Option("--manifest")],
    evidence_root: Annotated[Path, typer.Option("--evidence-root")],
    validation_report: Annotated[Path, typer.Option("--validation-report")],
    database: Annotated[Path, typer.Option("--database")],
    instrument: Annotated[str, typer.Option("--instrument")],
    side: Annotated[OrderSide, typer.Option("--side", case_sensitive=True)],
    quantity: Annotated[str, typer.Option("--quantity")],
    limit_price: Annotated[str, typer.Option("--limit-price")],
    time_in_force: Annotated[
        TimeInForce,
        typer.Option("--time-in-force", case_sensitive=True),
    ],
    confirm: Annotated[str, typer.Option("--confirm")],
    reduce_only: Annotated[
        bool,
        typer.Option("--reduce-only/--allow-increase"),
    ] = True,
    ordinal: Annotated[int, typer.Option("--ordinal", min=0)] = 0,
) -> None:
    """Submit exactly one deterministic manual limit intent after full startup."""

    def execute() -> None:
        _require_confirmation(confirm, _ORDER_CONFIRMATION)
        parsed_quantity = decimal_value(quantity, label="quantity", positive=True)
        parsed_price = decimal_value(limit_price, label="limit_price", positive=True)
        context = _build_online_context(
            config_path=config,
            receipt_path=receipt,
            manifest_path=manifest,
            evidence_root=evidence_root,
            validation_report_path=validation_report,
            database_path=database,
        )
        started = False
        try:
            context.runtime.start(dry_run=False)
            started = True
            intent = _manual_intent(
                context,
                instrument=instrument,
                side=side,
                quantity=parsed_quantity,
                limit_price=parsed_price,
                time_in_force=time_in_force,
                reduce_only=reduce_only,
                ordinal=ordinal,
            )
            result = context.engine.submit(intent, market_received_at=_now())
            _json(_execution_payload(result))
        finally:
            if started:
                with suppress(Exception):
                    context.runtime.stop(reason="SMOKE_ORDER_COMMAND_COMPLETE")
            context.close()

    _guard(execute)


@app.command("cancel")
def cancel(
    config: Annotated[Path, typer.Option("--config")],
    receipt: Annotated[Path, typer.Option("--receipt")],
    manifest: Annotated[Path, typer.Option("--manifest")],
    evidence_root: Annotated[Path, typer.Option("--evidence-root")],
    validation_report: Annotated[Path, typer.Option("--validation-report")],
    database: Annotated[Path, typer.Option("--database")],
    cloid: Annotated[str, typer.Option("--cloid")],
    confirm: Annotated[str, typer.Option("--confirm")],
) -> None:
    """Cancel one known deterministic CLOID after startup reconciliation."""

    def execute() -> None:
        _require_confirmation(confirm, _CANCEL_CONFIRMATION)
        normalized = validate_cloid(cloid)
        context = _build_online_context(
            config_path=config,
            receipt_path=receipt,
            manifest_path=manifest,
            evidence_root=evidence_root,
            validation_report_path=validation_report,
            database_path=database,
        )
        started = False
        try:
            context.runtime.start(dry_run=False)
            started = True
            result = context.engine.cancel(cloid=normalized)
            _json(_execution_payload(result))
        finally:
            if started:
                with suppress(Exception):
                    context.runtime.stop(reason="CANCEL_COMMAND_COMPLETE")
            context.close()

    _guard(execute)


if __name__ == "__main__":  # pragma: no cover - console-script convenience
    app()
