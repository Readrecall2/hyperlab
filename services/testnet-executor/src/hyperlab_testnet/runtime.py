"""Fail-closed Testnet startup, private event monitoring, and reconnect loop."""

from __future__ import annotations

import json
import re
import socket
import ssl
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from http import HTTPStatus
from typing import Protocol, cast

from hyperlab.environment_authorization import (
    REAL_MONEY_EXECUTION_ENABLED_IN_BUILD,
    AuthorizationPurpose,
    EnvironmentAuthorizationReceipt,
    EnvironmentClass,
    receipt_scope_blockers,
)

from .adapter import (
    TESTNET_API_ORIGIN,
    TESTNET_WEBSOCKET_URL,
    HyperliquidTestnetAdapter,
    VerifiedExtraAgent,
    parse_all_mids,
    perp_constraints_from_meta,
    verify_extra_agent_scope,
    verify_user_role,
)
from .build_identity import validate_runtime_identity
from .canonical import JsonValue
from .config import TestnetConfig, normalize_testnet_address
from .engine import ExecutionResult, TestnetExecutionEngine
from .models import RuntimeState
from .reconciliation import ReconciliationPlan


class RuntimeErrorClosed(RuntimeError):
    pass


class EventSourceError(RuntimeErrorClosed):
    pass


class EventSourceDisconnected(EventSourceError):
    pass


_MAX_WS_FRAME_BYTES = 4 * 1024 * 1024
_MAX_SUBSCRIPTION_ACK_FRAMES = 4
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate WebSocket JSON key")
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    del value
    raise ValueError("non-finite WebSocket JSON number")


def _loads_message(raw: str | bytes) -> object:
    if isinstance(raw, bytes):
        if len(raw) > _MAX_WS_FRAME_BYTES:
            raise ValueError("WebSocket frame exceeds the compiled size limit")
        raw = raw.decode("utf-8")
    if not isinstance(raw, str):
        raise ValueError("WebSocket frame must be text or UTF-8 bytes")
    if len(raw) > _MAX_WS_FRAME_BYTES or len(raw.encode("utf-8")) > _MAX_WS_FRAME_BYTES:
        raise ValueError("WebSocket frame exceeds the compiled size limit")
    return json.loads(
        raw,
        object_pairs_hook=_strict_object,
        parse_constant=_reject_constant,
    )


class WebSocketConnection(Protocol):
    @property
    def handshake_status(self) -> int: ...

    def send(self, payload: str) -> object: ...

    def recv(self) -> object: ...

    def close(self) -> object: ...


class WebSocketConnector(Protocol):
    def connect(self, *, url: str, timeout_seconds: float) -> WebSocketConnection: ...


class _ClientConnection:
    def __init__(self, connection: object) -> None:
        self._connection = connection

    @property
    def handshake_status(self) -> int:
        response = getattr(self._connection, "handshake_response", None)
        return int(getattr(response, "status", 0))

    def send(self, payload: str) -> object:
        method = getattr(self._connection, "send", None)
        if not callable(method):
            raise EventSourceDisconnected("WebSocket connection has no send method")
        return method(payload)

    def recv(self) -> object:
        method = getattr(self._connection, "recv", None)
        if not callable(method):
            raise EventSourceDisconnected("WebSocket connection has no recv method")
        return method()

    def close(self) -> object:
        method = getattr(self._connection, "close", None)
        return method() if callable(method) else None


class WebsocketClientConnector:
    """Production connector with no proxy and zero redirect allowance."""

    def connect(self, *, url: str, timeout_seconds: float) -> WebSocketConnection:
        if url != TESTNET_WEBSOCKET_URL:
            raise EventSourceError("only the compiled Testnet WebSocket URL is allowed")
        if not 0 < timeout_seconds <= 60:
            raise ValueError("WebSocket timeout must be in (0, 60]")
        import websocket

        raw_socket: socket.socket | None = None
        tls_socket: ssl.SSLSocket | None = None
        try:
            raw_socket = socket.create_connection(
                ("api.hyperliquid-testnet.xyz", 443),
                timeout=timeout_seconds,
            )
            tls_socket = ssl.create_default_context().wrap_socket(
                raw_socket,
                server_hostname="api.hyperliquid-testnet.xyz",
            )
            raw_socket = None
            connection = websocket.create_connection(
                url,
                timeout=timeout_seconds,
                enable_multithread=True,
                redirect_limit=0,
                socket=tls_socket,
            )
            tls_socket = None
        except Exception as error:
            if tls_socket is not None:
                tls_socket.close()
            if raw_socket is not None:
                raw_socket.close()
            raise EventSourceDisconnected(
                f"Testnet WebSocket connection failed ({type(error).__name__})"
            ) from None
        wrapped = _ClientConnection(connection)
        if wrapped.handshake_status != HTTPStatus.SWITCHING_PROTOCOLS:
            wrapped.close()
            raise EventSourceError("redirected or non-upgrade Testnet WebSocket refused")
        return wrapped


@dataclass(frozen=True, slots=True)
class UserStreamEvent:
    channel: str


class TestnetUserEventSource:
    """A dedicated account stream used only as a trigger for REST reconciliation."""

    def __init__(
        self,
        *,
        endpoint: str,
        account_address: str,
        connector: WebSocketConnector,
        timeout_seconds: float,
    ) -> None:
        if endpoint != TESTNET_WEBSOCKET_URL:
            raise EventSourceError("only the compiled Testnet WebSocket URL is allowed")
        normalized_account = normalize_testnet_address(
            account_address,
            label="account_address",
        )
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not 0 < float(timeout_seconds) <= 60
        ):
            raise ValueError("WebSocket timeout must be numeric and in (0, 60]")
        self._endpoint = endpoint
        self._account_address = normalized_account
        self._connector = connector
        self._timeout_seconds = float(timeout_seconds)
        self._connection: WebSocketConnection | None = None

    @property
    def endpoint(self) -> str:
        return self._endpoint

    def connect(self) -> None:
        if self._connection is not None:
            raise EventSourceError("Testnet user event source is already connected")
        connection = self._connector.connect(
            url=self._endpoint,
            timeout_seconds=self._timeout_seconds,
        )
        if connection.handshake_status != HTTPStatus.SWITCHING_PROTOCOLS:
            connection.close()
            raise EventSourceError("Testnet WebSocket handshake identity was not proven")
        try:
            pending = {"orderUpdates", "userFills"}
            for subscription_type in ("orderUpdates", "userFills"):
                payload = json.dumps(
                    {
                        "method": "subscribe",
                        "subscription": {
                            "type": subscription_type,
                            "user": self._account_address,
                        },
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                connection.send(payload)
            for _ in range(_MAX_SUBSCRIPTION_ACK_FRAMES):
                raw = connection.recv()
                if raw == "Websocket connection established.":
                    continue
                decoded = _loads_message(cast(str | bytes, raw))
                if (
                    not isinstance(decoded, Mapping)
                    or set(decoded) != {"channel", "data"}
                    or decoded.get("channel") != "subscriptionResponse"
                    or not isinstance(decoded.get("data"), Mapping)
                ):
                    raise EventSourceError("Testnet WebSocket subscription acknowledgement is invalid")
                data = cast(Mapping[str, object], decoded["data"])
                subscription = data.get("subscription")
                if (
                    set(data) != {"method", "subscription"}
                    or data.get("method") != "subscribe"
                    or not isinstance(subscription, Mapping)
                    or set(subscription) != {"type", "user"}
                ):
                    raise EventSourceError("Testnet WebSocket subscription acknowledgement is invalid")
                acknowledged_type = subscription.get("type")
                user = subscription.get("user")
                if (
                    not isinstance(acknowledged_type, str)
                    or acknowledged_type not in pending
                    or user != self._account_address
                ):
                    raise EventSourceError("Testnet WebSocket acknowledgement scope differs")
                pending.remove(acknowledged_type)
                if not pending:
                    break
            if pending:
                raise EventSourceError("Testnet WebSocket subscription acknowledgements are incomplete")
        except Exception as error:
            connection.close()
            if isinstance(error, EventSourceError):
                raise
            raise EventSourceDisconnected(
                f"Testnet WebSocket subscription failed ({type(error).__name__})"
            ) from None
        self._connection = connection

    def poll(self) -> UserStreamEvent | None:
        if self._connection is None:
            raise EventSourceDisconnected("Testnet user event source is disconnected")
        try:
            raw = self._connection.recv()
        except Exception as error:
            if type(error).__name__ in {"TimeoutError", "WebSocketTimeoutException"}:
                return None
            raise EventSourceDisconnected(
                f"Testnet WebSocket receive failed ({type(error).__name__})"
            ) from None
        if raw in {None, "", b""}:
            raise EventSourceDisconnected("Testnet WebSocket closed without a frame")
        if raw == "Websocket connection established.":
            return None
        try:
            decoded = _loads_message(cast(str | bytes, raw))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            raise EventSourceError("Testnet WebSocket returned malformed JSON") from None
        if not isinstance(decoded, Mapping):
            raise EventSourceError("Testnet WebSocket message must be an object")
        channel = decoded.get("channel")
        if channel in {"pong", "subscriptionResponse"}:
            return None
        if channel not in {"orderUpdates", "userFills"} or "data" not in decoded:
            raise EventSourceError("unexpected Testnet WebSocket channel refused")
        data = decoded.get("data")
        if channel == "userFills":
            if not isinstance(data, Mapping) or str(data.get("user", "")).lower() != self._account_address:
                raise EventSourceError("userFills event account scope differs")
        elif not isinstance(data, (Mapping, list)):
            raise EventSourceError("orderUpdates event has an invalid shape")
        return UserStreamEvent(str(channel))

    def close(self) -> None:
        connection, self._connection = self._connection, None
        if connection is not None:
            try:
                connection.close()
            except Exception:
                return


class RuntimeStore(Protocol):
    def runtime_state(self) -> RuntimeState: ...

    def verify_integrity(self) -> object: ...

    def set_runtime_state(self, state: RuntimeState, *, reason: str) -> None: ...

    def append_audit(self, kind: str, payload: Mapping[str, JsonValue]) -> None: ...

    def account_kill_latched(self) -> bool: ...

    def acquire_writer_lease(self) -> None: ...

    def renew_writer_lease(self) -> None: ...

    def release_writer_lease(self) -> None: ...


class RuntimeReconciler(Protocol):
    def reconcile(self, *, captured_at_ms: int) -> ReconciliationPlan: ...


@dataclass(frozen=True, slots=True)
class PreflightReport:
    config_hash: str
    asset_count: int
    api_wallet_address: str
    api_wallet_valid_until_ms: int
    mark_count: int


class TestnetPreflight:
    def __init__(
        self,
        *,
        config: TestnetConfig,
        adapter: HyperliquidTestnetAdapter,
        store: RuntimeStore,
        authorization_check: Callable[[], EnvironmentAuthorizationReceipt],
        authorization_evidence: Mapping[str, str],
        clock_ms: Callable[[], int],
    ) -> None:
        if set(authorization_evidence) != {
            "validation_id",
            "validation_report_sha256",
        }:
            raise ValueError("authorization_evidence requires the exact validation hash fields")
        for label, value in authorization_evidence.items():
            if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                raise ValueError(f"authorization_evidence.{label} must be a lowercase SHA-256")
        self._config = config
        self._adapter = adapter
        self._store = store
        self._authorization_check = authorization_check
        self._authorization_evidence = dict(authorization_evidence)
        self._clock_ms = clock_ms

    def run(self) -> PreflightReport:
        validate_runtime_identity(self._config)
        if (
            self._config.http_endpoint != TESTNET_API_ORIGIN
            or self._config.ws_endpoint != TESTNET_WEBSOCKET_URL
            or self._adapter.origin != TESTNET_API_ORIGIN
            or self._adapter.account_address != self._config.account_address
            or self._adapter.api_wallet_address != self._config.api_wallet_address
        ):
            raise RuntimeErrorClosed("Testnet endpoint/account identity preflight failed")
        integrity = self._store.verify_integrity()
        if not bool(integrity):
            raise RuntimeErrorClosed("Testnet durable-store integrity preflight failed")
        authorization = self._authorization_check()
        if not isinstance(authorization, EnvironmentAuthorizationReceipt):
            raise RuntimeErrorClosed("Testnet authorization receipt has the wrong type")
        blockers = receipt_scope_blockers(
            authorization,
            environment=EnvironmentClass.TESTNET,
            purpose=AuthorizationPurpose.TESTNET_EXECUTION,
            config_hash=self._config.config_hash,
        )
        if blockers or authorization.subject.to_dict() != self._config.to_readiness_subject():
            raise RuntimeErrorClosed("Testnet authorization receipt scope differs from config")
        if REAL_MONEY_EXECUTION_ENABLED_IN_BUILD or authorization.authorizes_real_money:
            raise RuntimeErrorClosed("real-money authorization is forbidden in this build")
        verifier_identities: list[JsonValue] = [
            cast(JsonValue, identity.to_dict()) for identity in authorization.verifier_identities
        ]
        self._store.append_audit(
            "ENVIRONMENT_AUTHORIZATION_ACCEPTED",
            {
                "manifest_sha256": authorization.manifest_sha256,
                "profile_sha256": authorization.profile_sha256,
                "receipt_sha256": authorization.receipt_sha256,
                "required_check_count": len(authorization.required_checks),
                "validation_id": self._authorization_evidence["validation_id"],
                "validation_report_sha256": self._authorization_evidence["validation_report_sha256"],
                "verifier_identities": verifier_identities,
                "verifier_set_sha256": authorization.verifier_set_sha256,
            },
        )
        live_constraints = perp_constraints_from_meta(self._adapter.read_meta())
        if dict(live_constraints) != dict(self._adapter.constraints_by_coin):
            raise RuntimeErrorClosed("live Testnet meta differs from the frozen perp constraints")
        marks = parse_all_mids(self._adapter.read_all_mids())
        if not marks or not all(f"HL:{coin}:perp" in marks for coin in live_constraints):
            raise RuntimeErrorClosed("live Testnet marks do not cover the frozen perp universe")
        verify_user_role(self._adapter.read_user_role())
        agent: VerifiedExtraAgent = verify_extra_agent_scope(
            self._adapter.read_extra_agents(),
            expected_api_wallet_address=self._config.api_wallet_address,
            now_ms=self._clock_ms(),
        )
        report = PreflightReport(
            config_hash=self._config.config_hash,
            asset_count=len(live_constraints),
            api_wallet_address=agent.address,
            api_wallet_valid_until_ms=agent.valid_until_ms,
            mark_count=len(marks),
        )
        self._store.append_audit(
            "TESTNET_PREFLIGHT_PASSED",
            {
                "api_wallet_address": report.api_wallet_address,
                "api_wallet_valid_until_ms": report.api_wallet_valid_until_ms,
                "asset_count": report.asset_count,
                "config_hash": report.config_hash,
                "mark_count": report.mark_count,
            },
        )
        return report


class TestnetRuntime:
    def __init__(
        self,
        *,
        config: TestnetConfig,
        store: RuntimeStore,
        preflight: TestnetPreflight,
        reconciler: RuntimeReconciler,
        engine: TestnetExecutionEngine,
        event_source: TestnetUserEventSource,
        clock: Callable[[], datetime],
    ) -> None:
        if event_source.endpoint != config.ws_endpoint:
            raise RuntimeErrorClosed("runtime event source is outside the config endpoint")
        self._config = config
        self._store = store
        self._preflight = preflight
        self._reconciler = reconciler
        self._engine = engine
        self._event_source = event_source
        self._clock = clock
        self._last_reconcile_ms: int | None = None
        self._last_deadman_ms: int | None = None

    def _now_ms(self) -> int:
        return int(self._clock().timestamp() * 1000)

    def _manual_review(self, reason: str) -> None:
        state = self._store.runtime_state()
        if state not in {RuntimeState.KILLED, RuntimeState.MANUAL_REVIEW}:
            self._store.set_runtime_state(RuntimeState.MANUAL_REVIEW, reason=reason)

    def _reconcile(self) -> ReconciliationPlan:
        captured_at_ms = self._now_ms()
        plan = self._reconciler.reconcile(captured_at_ms=captured_at_ms)
        if not plan.clean:
            raise RuntimeErrorClosed("Testnet reconciliation requires manual review")
        self._last_reconcile_ms = captured_at_ms
        return plan

    def _arm_deadman(self) -> ExecutionResult:
        now_ms = self._now_ms()
        cancel_at_ms = now_ms + self._config.risk_limits.deadman_interval_seconds * 1_000
        result = self._engine.arm_deadman(cancel_at_ms=cancel_at_ms)
        outcome = result.outcome
        if outcome is None or outcome.kind.value != "DEADMAN_ARMED":
            raise RuntimeErrorClosed("Testnet dead-man switch was not confirmed")
        self._last_deadman_ms = now_ms
        return result

    def start(self, *, dry_run: bool = False) -> PreflightReport:
        self._store.acquire_writer_lease()
        try:
            self._store.set_runtime_state(RuntimeState.STARTING, reason="RUNTIME_START")
            self._store.append_audit(
                "TESTNET_RUNTIME_STARTING",
                {"dry_run": dry_run, "run_id": self._config.run_id},
            )
            report = self._preflight.run()
            if dry_run:
                self._store.append_audit(
                    "TESTNET_PREFLIGHT_ONLY_STOP",
                    {"config_hash": self._config.config_hash},
                )
                self._store.set_runtime_state(RuntimeState.STOPPED, reason="PREFLIGHT_ONLY")
                self._store.release_writer_lease()
                return report
            self._reconcile()
            self._event_source.connect()
            self._arm_deadman()
            self._store.set_runtime_state(RuntimeState.RUNNING, reason="STARTUP_RECONCILED")
            self._store.append_audit(
                "TESTNET_RUNTIME_STARTED",
                {"run_id": self._config.run_id},
            )
            return report
        except Exception:
            self._event_source.close()
            self._manual_review("RUNTIME_START_FAILED")
            self._store.release_writer_lease()
            raise RuntimeErrorClosed("Testnet runtime startup failed closed") from None

    def _reconnect(self) -> None:
        self._store.set_runtime_state(RuntimeState.PAUSED, reason="WEBSOCKET_LOST")
        self._store.set_runtime_state(RuntimeState.STARTING, reason="WEBSOCKET_RECONNECT")
        self._store.append_audit(
            "TESTNET_WEBSOCKET_RECONNECTING",
            {"endpoint": TESTNET_WEBSOCKET_URL},
        )
        self._event_source.close()
        try:
            self._store.renew_writer_lease()
            self._event_source.connect()
            self._reconcile()
            self._arm_deadman()
            self._store.set_runtime_state(
                RuntimeState.RUNNING,
                reason="WEBSOCKET_RECONNECTED_AND_RECONCILED",
            )
            self._store.append_audit(
                "TESTNET_WEBSOCKET_RECONNECTED",
                {"endpoint": TESTNET_WEBSOCKET_URL},
            )
        except Exception:
            self._event_source.close()
            self._manual_review("WEBSOCKET_RECOVERY_FAILED")
            raise RuntimeErrorClosed("Testnet WebSocket recovery failed closed") from None

    def poll_once(self) -> UserStreamEvent | None:
        self._store.renew_writer_lease()
        state = self._store.runtime_state()
        if self._store.account_kill_latched() and state is not RuntimeState.KILLED:
            self._store.set_runtime_state(
                RuntimeState.KILLED,
                reason="ACCOUNT_KILL_LATCHED",
            )
            state = RuntimeState.KILLED
        if state is RuntimeState.KILLED:
            cancel_at_ms = self._now_ms() + self._config.risk_limits.deadman_interval_seconds * 1_000
            protection_confirmed = False
            try:
                protection = self._engine.enforce_persisted_kill(
                    cancel_at_ms=cancel_at_ms
                )
                outcome = protection.outcome
                protection_confirmed = (
                    outcome is not None and outcome.kind.value == "DEADMAN_ARMED"
                )
                if not protection_confirmed:
                    self._store.append_audit(
                        "TESTNET_EXTERNAL_KILL_DMS_UNCONFIRMED",
                        {"cancel_at_ms": cancel_at_ms},
                    )
            finally:
                self._event_source.close()
                self._store.release_writer_lease()
            if not protection_confirmed:
                raise RuntimeErrorClosed(
                    "durable external kill latched; emergency dead-man was not confirmed"
                )
            raise RuntimeErrorClosed("durable external kill was enforced")
        if state is not RuntimeState.RUNNING:
            raise RuntimeErrorClosed("polling requires RUNNING Testnet state")
        try:
            event = self._event_source.poll()
        except (EventSourceDisconnected, EventSourceError):
            self._reconnect()
            return None
        now_ms = self._now_ms()
        reconcile_interval_ms = min(
            30_000,
            self._config.risk_limits.reconciliation_stale_after_seconds * 500,
        )
        if (
            event is not None
            or self._last_reconcile_ms is None
            or now_ms - self._last_reconcile_ms >= reconcile_interval_ms
        ):
            try:
                self._reconcile()
            except Exception:
                self._manual_review("SUSTAINED_RECONCILIATION_FAILED")
                raise RuntimeErrorClosed("sustained Testnet reconciliation failed") from None
        refresh_ms = self._config.risk_limits.deadman_interval_seconds * 500
        if self._last_deadman_ms is None or now_ms - self._last_deadman_ms >= refresh_ms:
            try:
                self._arm_deadman()
            except Exception:
                self._manual_review("DEADMAN_REFRESH_FAILED")
                raise RuntimeErrorClosed("Testnet dead-man refresh failed") from None
        return event

    def stop(self, *, reason: str = "OPERATOR_STOP") -> None:
        self._store.append_audit("TESTNET_RUNTIME_STOP_REQUESTED", {"reason": reason})
        self._event_source.close()
        if self._store.runtime_state() not in {
            RuntimeState.KILLED,
            RuntimeState.MANUAL_REVIEW,
            RuntimeState.PAUSED,
        }:
            self._store.set_runtime_state(RuntimeState.STOPPED, reason=reason)
        self._store.append_audit("TESTNET_RUNTIME_STOPPED", {"reason": reason})
        self._store.release_writer_lease()

    def emergency_kill(
        self,
        *,
        reason: str = "OPERATOR_KILL",
    ) -> ExecutionResult:
        cancel_at_ms = self._now_ms() + self._config.risk_limits.deadman_interval_seconds * 1_000
        try:
            return self._engine.kill(cancel_at_ms=cancel_at_ms, reason=reason)
        finally:
            self._event_source.close()
            self._store.release_writer_lease()


__all__ = [
    "EventSourceDisconnected",
    "EventSourceError",
    "PreflightReport",
    "RuntimeErrorClosed",
    "RuntimeReconciler",
    "RuntimeStore",
    "TestnetPreflight",
    "TestnetRuntime",
    "TestnetUserEventSource",
    "UserStreamEvent",
    "WebSocketConnection",
    "WebSocketConnector",
    "WebsocketClientConnector",
]
