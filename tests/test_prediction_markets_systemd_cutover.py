from __future__ import annotations

import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest

from ops.prediction_markets_launch_v1 import systemd_cutover

SERVICE = "hyperlab-pm-20260828t160000z-deadbeef-polymarket.service"
PROBE = "hyperlab-pm-20260828t160000z-deadbeef-polymarket-namespace-probe.service"


class FakeSystemd:
    def __init__(
        self,
        *,
        active: bool,
        enabled: bool,
        probe: bool = False,
        fail_once: str | None = None,
        failure: str = "python-timeout",
    ) -> None:
        self.active = active
        self.enabled = enabled
        self.probe = probe
        self.fail_once = fail_once
        self.failure = failure
        self.calls: list[tuple[tuple[str, ...], int]] = []

    def __call__(
        self,
        arguments: Sequence[str],
        timeout_seconds: int,
    ) -> systemd_cutover.CommandResult:
        args = tuple(arguments)
        self.calls.append((args, timeout_seconds))
        if args[:2] == ("systemctl", "show"):
            if self.probe:
                active = "inactive"
                sub = "dead"
                result = "success"
                pid = "0"
            else:
                active = "active" if self.active else "inactive"
                sub = "running" if self.active else "dead"
                result = "success"
                pid = "123" if self.active else "0"
            output = (
                "LoadState=loaded\n"
                f"ActiveState={active}\n"
                f"SubState={sub}\n"
                f"Result={result}\n"
                f"MainPID={pid}\n"
                "NRestarts=0\n"
            )
            return systemd_cutover.CommandResult(0, output, "")
        if args[:2] == ("systemctl", "is-enabled"):
            state = "enabled" if self.enabled else "disabled"
            return systemd_cutover.CommandResult(0 if self.enabled else 1, state, "")
        assert args[:7] == (
            "sudo",
            "-n",
            "timeout",
            "--signal=TERM",
            "--kill-after=5s",
            args[5],
            "systemctl",
        )
        operation = args[7]
        if self.fail_once == operation:
            self.fail_once = None
            if self.failure == "python-timeout":
                raise subprocess.TimeoutExpired(args, timeout_seconds)
            if self.failure == "coreutils-timeout":
                return systemd_cutover.CommandResult(124, "", "")
            return systemd_cutover.CommandResult(
                1,
                "",
                "sudo: a password is required",
            )
        if operation == "stop":
            self.active = False
        elif operation == "disable":
            self.enabled = False
        elif operation == "enable":
            self.enabled = True
        elif operation == "start" and not self.probe:
            self.active = True
        return systemd_cutover.CommandResult(0, "", "")


@pytest.mark.parametrize(
    ("operation", "scenario"),
    [
        ("stop", "disarm-active"),
        ("disable", "disarm-enabled"),
        ("enable", "activate-disabled"),
        ("start", "activate-inactive"),
        ("start", "oneshot"),
    ],
)
def test_every_restore_mutation_hang_is_bounded_and_names_the_operation(
    operation: str,
    scenario: str,
) -> None:
    fake = FakeSystemd(
        active=scenario == "disarm-active",
        enabled=scenario not in {"activate-disabled"},
        probe=scenario == "oneshot",
        fail_once=operation,
    )
    messages: list[str] = []
    expected_service = PROBE if scenario == "oneshot" else SERVICE
    with pytest.raises(
        systemd_cutover.SystemdCutoverError,
        match=rf"PREDICTION_SYSTEMD_OPERATION_TIMEOUT:operation={operation}:service={expected_service}",
    ):
        if scenario.startswith("disarm"):
            systemd_cutover.disarm_service(
                SERVICE,
                allow_absent=False,
                run=fake,
                emit=messages.append,
            )
        elif scenario == "oneshot":
            systemd_cutover.ensure_probe_success(PROBE, run=fake, emit=messages.append)
        else:
            systemd_cutover.ensure_active_service(SERVICE, run=fake, emit=messages.append)
    assert messages[-1] == (
        f"PREDICTION_SYSTEMD_OPERATION_BEGIN:operation={operation}:"
        f"service={expected_service}"
    )
    assert not any("OPERATION_GREEN" in message for message in messages)


def test_privileged_timeout_is_inside_sudo_and_sudo_refusal_cannot_prompt() -> None:
    fake = FakeSystemd(
        active=True,
        enabled=True,
        fail_once="stop",
        failure="sudo-refused",
    )
    with pytest.raises(systemd_cutover.SystemdCutoverError, match="password is required"):
        systemd_cutover.disarm_service(SERVICE, allow_absent=False, run=fake)
    mutation = next(args for args, _timeout in fake.calls if args[0] == "sudo")
    assert mutation[:3] == ("sudo", "-n", "timeout")
    assert mutation[6:8] == ("systemctl", "stop")


def test_stopped_or_expired_privileged_timeout_is_terminal_not_green() -> None:
    fake = FakeSystemd(
        active=True,
        enabled=True,
        fail_once="stop",
        failure="coreutils-timeout",
    )
    messages: list[str] = []
    with pytest.raises(systemd_cutover.SystemdCutoverError, match="exit=124"):
        systemd_cutover.disarm_service(
            SERVICE,
            allow_absent=False,
            run=fake,
            emit=messages.append,
        )
    assert messages == [
        f"PREDICTION_SYSTEMD_OPERATION_BEGIN:operation=stop:service={SERVICE}"
    ]


def test_partial_disarm_can_resume_without_repeating_terminal_stop() -> None:
    fake = FakeSystemd(active=True, enabled=True, fail_once="disable")
    first_messages: list[str] = []
    with pytest.raises(systemd_cutover.SystemdCutoverError, match="operation=disable"):
        systemd_cutover.disarm_service(
            SERVICE,
            allow_absent=False,
            run=fake,
            emit=first_messages.append,
        )
    assert fake.active is False and fake.enabled is True

    second_messages: list[str] = []
    systemd_cutover.disarm_service(
        SERVICE,
        allow_absent=False,
        run=fake,
        emit=second_messages.append,
    )
    assert second_messages[0] == (
        "PREDICTION_SYSTEMD_OPERATION_SKIPPED_ALREADY_TERMINAL:"
        f"operation=stop:service={SERVICE}:state=inactive"
    )
    assert second_messages[-1] == (
        f"PREDICTION_SYSTEMD_DISARM_GREEN:service={SERVICE}:state=inactive-disabled"
    )
    stop_mutations = [
        args for args, _timeout in fake.calls if args[0] == "sudo" and args[7] == "stop"
    ]
    assert len(stop_mutations) == 1


def test_ctrl_c_reports_exact_operation_and_is_retriable(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def interrupt(_service: str, *, allow_absent: bool) -> None:
        assert allow_absent is False
        systemd_cutover._CURRENT_OPERATION = ("stop", SERVICE)
        raise KeyboardInterrupt

    monkeypatch.setattr(systemd_cutover, "disarm_service", interrupt)
    monkeypatch.setattr(systemd_cutover.signal, "signal", lambda *_args: None)
    assert systemd_cutover.main(["disarm", "--service", SERVICE]) == 130
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.strip() == (
        "PREDICTION_SYSTEMD_OPERATION_INTERRUPTED_RETRY_SAME_MODE:"
        f"operation=stop:service={SERVICE}"
    )


def test_read_only_queries_never_use_sudo_or_a_command_substitution_prompt() -> None:
    fake = FakeSystemd(active=False, enabled=False)
    systemd_cutover.disarm_service(SERVICE, allow_absent=False, run=fake)
    read_only = [args for args, _timeout in fake.calls if args[0] == "systemctl"]
    assert read_only
    assert all(args[0] == "systemctl" for args in read_only)
    cutover = (
        Path(__file__).resolve().parents[1]
        / "ops"
        / "prediction_markets_launch_v1"
        / "cutover.sh"
    ).read_text(encoding="utf-8")
    assert "timeout 5 sudo" not in cutover
    assert "sudo systemctl" not in cutover
    assert "$(sudo" not in cutover
