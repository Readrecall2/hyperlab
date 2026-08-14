from __future__ import annotations

import json
import socket
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import requests
from typer.testing import CliRunner

import hyperlab.cli as cli_module
import hyperlab.venues.binance as binance_module
from hyperlab.cli import app
from hyperlab.venues.binance import (
    BinancePublicRestClient,
    RequestsJsonTransport,
    diagnose_binance_http_paths,
)
from hyperlab.venues.http_observability import resolve_dns_snapshot

BASE = datetime(2026, 8, 12, 12, tzinfo=UTC)
runner = CliRunner()


class ControlledClock:
    def __init__(self) -> None:
        self.current = BASE

    def now(self) -> datetime:
        return self.current

    def monotonic(self) -> float:
        return (self.current - BASE).total_seconds()

    def advance(self, milliseconds: int) -> None:
        self.current += timedelta(milliseconds=milliseconds)


class InspectableSocket:
    family = socket.AF_INET6
    session_reused = False

    def getpeername(self) -> tuple[str, int, int, int]:
        return ("2001:db8::42", 443, 0, 0)


class FakeSession:
    def __init__(self, clock: ControlledClock, raw: object) -> None:
        self.clock = clock
        self.raw = raw
        self.close_calls = 0
        self.trust_env = True
        self.auth: object = ("ambient", "credential")
        self.headers = {"Authorization": "must-not-leak"}
        self.cookies = {"session": "must-not-leak"}
        self.params = {"apiKey": "must-not-leak"}
        self.proxies = {"https": "https://ambient.invalid"}
        self.cert: object = "ambient-client-cert"
        self.verify = False

    def get_adapter(self, _url: str) -> Any:
        raise AttributeError("test session has no urllib3 pool")

    def get(self, _url: str, **kwargs: object) -> requests.Response:
        response = requests.Response()
        response.status_code = 200
        response.elapsed = timedelta(milliseconds=20)
        response.headers["X-Amz-Cf-Pop"] = "SIN2-P11"
        response.headers["X-Cache"] = "Miss from cloudfront"
        response.raw = self.raw  # type: ignore[assignment]
        hooks = kwargs.get("hooks")
        assert isinstance(hooks, dict)
        response_hooks = hooks.get("response")
        assert isinstance(response_hooks, list)
        for hook in response_hooks:
            response = hook(response)
        self.clock.advance(24)
        response._content = b'{"serverTime":1786536000000}'
        response._content_consumed = True
        return response

    def close(self) -> None:
        self.close_calls += 1


class ExplodingRaw:
    @property
    def connection(self) -> object:
        raise RuntimeError("socket introspection unavailable")


def _client(clock: ControlledClock, raw: object) -> BinancePublicRestClient:
    session = FakeSession(clock, raw)
    return BinancePublicRestClient(
        transport=RequestsJsonTransport(
            session=session,  # type: ignore[arg-type]
            monotonic=clock.monotonic,
        ),
        clock=clock.now,
    )


def test_response_hook_reports_selected_peer_family_pop_and_existing_reuse_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("runtime observation must not resolve DNS")
        ),
    )
    clock = ControlledClock()
    raw = SimpleNamespace(connection=SimpleNamespace(sock=InspectableSocket()))
    client = _client(clock, raw)

    try:
        first = client.clock_measurement()
        second = client.clock_measurement()
    finally:
        client.close()

    first_diagnostics = first.http_diagnostics
    second_diagnostics = second.http_diagnostics
    assert first_diagnostics is not None
    assert second_diagnostics is not None
    assert first.server_time == BASE
    assert first.round_trip_latency_ms == Decimal("24")
    assert first.drift_uncertainty_ms == Decimal("12")
    assert first_diagnostics.peer_ip == "2001:db8::42"
    assert first_diagnostics.peer_port == 443
    assert first_diagnostics.socket_family == "AF_INET6"
    assert first_diagnostics.response_cloudfront_pop == "SIN2-P11"
    assert first_diagnostics.response_cache == "Miss from cloudfront"
    assert first_diagnostics.urllib3_connection_identity is not None
    assert first_diagnostics.tls_socket_identity is not None
    assert first_diagnostics.requests_adapter_header_elapsed_ms == pytest.approx(20)
    assert second_diagnostics.requests_session_reused is True
    assert second_diagnostics.urllib3_connection_reused is True
    assert second_diagnostics.tls_socket_reused is True


def test_response_path_introspection_is_observational_and_fail_open() -> None:
    clock = ControlledClock()
    client = _client(clock, ExplodingRaw())

    try:
        measurement = client.clock_measurement()
    finally:
        client.close()

    assert measurement.server_time == BASE
    assert measurement.round_trip_latency_ms == Decimal("24")
    assert measurement.drift_uncertainty_ms == Decimal("12")
    diagnostics = measurement.http_diagnostics
    assert diagnostics is not None
    assert diagnostics.peer_ip is None
    assert diagnostics.peer_port is None
    assert diagnostics.socket_family is None
    assert diagnostics.response_cloudfront_pop == "SIN2-P11"
    assert diagnostics.response_cache == "Miss from cloudfront"


def test_standalone_dns_snapshot_is_deterministic_for_a_and_aaaa_answers() -> None:
    clock = ControlledClock()

    def resolver(host: str, port: int, **kwargs: object) -> list[tuple[object, ...]]:
        assert host == "fapi.binance.com"
        assert port == 443
        assert kwargs == {"type": socket.SOCK_STREAM}
        clock.advance(3)
        return [
            (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("2001:db8::2", 443, 0, 0)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.9", 443)),
            (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("2001:db8::2", 443, 0, 0)),
        ]

    payload = resolve_dns_snapshot(
        "fapi.binance.com",
        443,
        resolver=resolver,
        monotonic=clock.monotonic,
    ).as_dict()

    assert payload["duration_ms"] == pytest.approx(3)
    assert payload["addresses"] == [
        {"family": "AF_INET", "ip": "192.0.2.9", "port": 443},
        {"family": "AF_INET6", "ip": "2001:db8::2", "port": 443},
    ]
    assert payload["error_type"] is None


def test_standalone_probe_uses_one_bounded_fresh_client_then_one_persistent_client() -> None:
    created: list[FakeClient] = []

    class Diagnostics:
        peer_ip = "192.0.2.9"
        peer_port = 443
        socket_family = "AF_INET"
        response_cloudfront_pop = "SIN2-P11"
        response_cache = "Miss from cloudfront"
        requests_adapter_header_elapsed_ms = 10.0
        session_get_total_ms = 12.0

        def __init__(self, identity: str, reused: bool) -> None:
            self.requests_session_reused = reused
            self.urllib3_connection_identity = identity
            self.urllib3_connection_reused = reused
            self.tls_socket_identity = f"tls:{identity}"
            self.tls_socket_reused = reused
            self.tls_session_reused = None

    class Measurement:
        request_sent_time = BASE
        response_received_time = BASE + timedelta(milliseconds=24)
        round_trip_latency_ms = Decimal("24")
        drift_uncertainty_ms = Decimal("12")
        estimated_clock_drift_ms = Decimal("0")

        def __init__(self, diagnostics: Diagnostics) -> None:
            self.http_diagnostics = diagnostics

    class FakeClient:
        def __init__(self, identity: str) -> None:
            self.identity = identity
            self.calls = 0
            self.closed = False

        def clock_measurement(self) -> Measurement:
            self.calls += 1
            return Measurement(Diagnostics(self.identity, self.calls > 1))

        def close(self) -> None:
            self.closed = True

    def factory() -> Any:
        client = FakeClient(f"connection:{len(created) + 1}")
        created.append(client)
        return client

    def resolver(_host: str, _port: int, **_kwargs: object) -> list[tuple[object, ...]]:
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.9", 443))]

    result = diagnose_binance_http_paths(
        samples=2,
        interval_seconds=0,
        client_factory=factory,
        resolver=resolver,
        sleeper=lambda _seconds: None,
        runtime_status={
            "clock_observability": {
                "latest": {
                    "peer_ip": "192.0.2.9",
                    "response_cloudfront_pop": "SIN2-P11",
                }
            }
        },
    )

    assert len(created) == 2
    assert all(client.closed for client in created)
    assert [sample["urllib3_connection_identity"] for sample in result["fresh_samples"]] == [
        "connection:1"
    ]
    assert [
        sample["urllib3_connection_identity"] for sample in result["persistent_samples"]
    ] == ["connection:2", "connection:2"]
    assert result["comparison"] == {
        "runtime_selected_peer_ip": "192.0.2.9",
        "runtime_cloudfront_pop": "SIN2-P11",
        "fresh_selected_peer_ips": ["192.0.2.9"],
        "persistent_selected_peer_ips": ["192.0.2.9"],
        "fresh_cloudfront_pops": ["SIN2-P11"],
        "persistent_cloudfront_pops": ["SIN2-P11"],
        "runtime_peer_matches_any_fresh": True,
        "runtime_peer_matches_persistent": True,
        "runtime_pop_matches_any_fresh": True,
        "runtime_pop_matches_persistent": True,
        "same_peer_or_pop_is_not_proof_of_same_edge": True,
    }


def test_cli_is_bounded_and_reads_latest_runtime_peer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_status = {
        "clock_observability": {
            "latest": {
                "peer_ip": "192.0.2.9",
                "response_cloudfront_pop": "SIN2-P11",
            }
        }
    }
    runtime_path = tmp_path / "runtime_status_binance_usdm.json"
    runtime_path.write_text(json.dumps(runtime_status), encoding="utf-8")
    monkeypatch.setattr(
        cli_module,
        "_settings",
        lambda: SimpleNamespace(
            app=SimpleNamespace(
                mode="readonly",
                data_dir=tmp_path,
                request_timeout_seconds=15.0,
            )
        ),
    )
    calls: list[dict[str, object]] = []

    def diagnostic(**kwargs: object) -> dict[str, object]:
        calls.append(dict(kwargs))
        return {"comparison": {"runtime_selected_peer_ip": "192.0.2.9"}}

    monkeypatch.setattr(binance_module, "diagnose_binance_http_paths", diagnostic)

    result = runner.invoke(
        app,
        [
            "diagnose-binance-http",
            "--persistent-samples",
            "2",
            "--fresh-samples",
            "1",
            "--interval-seconds",
            "0",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        {
            "samples": 2,
            "fresh_sample_count": 1,
            "interval_seconds": 0.0,
            "timeout_seconds": 15.0,
            "runtime_status": runtime_status,
        }
    ]
    payload = json.loads(result.stdout)
    assert payload["comparison"]["runtime_selected_peer_ip"] == "192.0.2.9"
    assert payload["runtime_status_read_error"] is None
