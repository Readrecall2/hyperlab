from __future__ import annotations

import argparse
import re
import signal
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import NoReturn

_SERVICE = re.compile(
    r"^hyperlab-pm-[a-z0-9-]+-(?:polymarket|kalshi|dashboard)(?:-namespace-probe)?\.service$"
)
_PROPERTY_NAMES = (
    "LoadState",
    "ActiveState",
    "SubState",
    "Result",
    "MainPID",
    "NRestarts",
)
_MUTATION_TIMEOUT_SECONDS = {
    "disable": 30,
    "enable": 30,
    "start": 45,
    "stop": 195,
}
_QUERY_TIMEOUT_SECONDS = 10
_CURRENT_OPERATION: tuple[str, str] | None = None


class SystemdCutoverError(RuntimeError):
    """Bounded systemd cutover refusal with an operator-safe diagnostic."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[Sequence[str], int], CommandResult]
Emitter = Callable[[str], None]


def _command(arguments: Sequence[str], timeout_seconds: int) -> CommandResult:
    completed = subprocess.run(
        list(arguments),
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout_seconds,
    )
    return CommandResult(completed.returncode, completed.stdout.strip(), completed.stderr.strip())


def _service(value: str) -> str:
    if _SERVICE.fullmatch(value) is None or "hyperlab-h1" in value:
        raise SystemdCutoverError(f"invalid Prediction Markets service identity:{value}")
    return value


def _run_bounded(
    arguments: Sequence[str],
    *,
    operation: str,
    service: str,
    timeout_seconds: int,
    run: CommandRunner,
) -> CommandResult:
    global _CURRENT_OPERATION
    _CURRENT_OPERATION = (operation, service)
    try:
        result = run(arguments, timeout_seconds)
    except subprocess.TimeoutExpired as error:
        raise SystemdCutoverError(
            "PREDICTION_SYSTEMD_OPERATION_TIMEOUT:"
            f"operation={operation}:service={service}:timeout_seconds={timeout_seconds}"
        ) from error
    except KeyboardInterrupt:
        raise
    else:
        _CURRENT_OPERATION = None
        return result


def _properties(service: str, *, run: CommandRunner) -> dict[str, str]:
    result = _run_bounded(
        [
            "systemctl",
            "show",
            service,
            "--property=" + ",".join(_PROPERTY_NAMES),
            "--no-pager",
        ],
        operation="show",
        service=service,
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        run=run,
    )
    if result.returncode != 0:
        raise SystemdCutoverError(
            f"systemd state query failed:service={service}:diagnostic={result.stderr[:512]}"
        )
    values: dict[str, str] = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key] = value
    if set(values) != set(_PROPERTY_NAMES):
        raise SystemdCutoverError(f"systemd state query is incomplete:service={service}")
    if values["MainPID"] and not values["MainPID"].isdigit():
        raise SystemdCutoverError(f"systemd MainPID is invalid:service={service}")
    if not values["NRestarts"].isdigit():
        raise SystemdCutoverError(f"systemd restart count is invalid:service={service}")
    return values


def _enabled_state(service: str, *, run: CommandRunner) -> str:
    result = _run_bounded(
        ["systemctl", "is-enabled", service, "--no-pager"],
        operation="is-enabled",
        service=service,
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        run=run,
    )
    state = result.stdout.strip()
    if state not in {"disabled", "enabled"} or result.returncode not in {0, 1}:
        raise SystemdCutoverError(
            f"systemd enabled-state query diverged:service={service}:state={state}:exit={result.returncode}"
        )
    return state


def _mutate(
    operation: str,
    service: str,
    *,
    run: CommandRunner,
    emit: Emitter,
) -> None:
    timeout_seconds = _MUTATION_TIMEOUT_SECONDS[operation]
    emit(f"PREDICTION_SYSTEMD_OPERATION_BEGIN:operation={operation}:service={service}")
    result = _run_bounded(
        [
            "sudo",
            "-n",
            "timeout",
            "--signal=TERM",
            "--kill-after=5s",
            f"{timeout_seconds}s",
            "systemctl",
            operation,
            service,
        ],
        operation=operation,
        service=service,
        timeout_seconds=timeout_seconds + 10,
        run=run,
    )
    if result.returncode in {124, 137}:
        raise SystemdCutoverError(
            "PREDICTION_SYSTEMD_OPERATION_TIMEOUT:"
            f"operation={operation}:service={service}:timeout_seconds={timeout_seconds}:"
            f"exit={result.returncode}"
        )
    if result.returncode != 0:
        raise SystemdCutoverError(
            "PREDICTION_SYSTEMD_OPERATION_FAILED:"
            f"operation={operation}:service={service}:exit={result.returncode}:"
            f"diagnostic={result.stderr[:512]}"
        )


def _green(operation: str, service: str, *, emit: Emitter) -> None:
    emit(f"PREDICTION_SYSTEMD_OPERATION_GREEN:operation={operation}:service={service}")


def _skip(operation: str, service: str, state: str, *, emit: Emitter) -> None:
    emit(
        "PREDICTION_SYSTEMD_OPERATION_SKIPPED_ALREADY_TERMINAL:"
        f"operation={operation}:service={service}:state={state}"
    )


def disarm_service(
    service: str,
    *,
    allow_absent: bool,
    run: CommandRunner = _command,
    emit: Emitter = print,
) -> None:
    service = _service(service)
    properties = _properties(service, run=run)
    if properties["LoadState"] == "not-found":
        if not allow_absent:
            raise SystemdCutoverError(f"required unit is absent:service={service}")
        _skip("stop", service, "not-found", emit=emit)
        _skip("disable", service, "not-found", emit=emit)
        emit(f"PREDICTION_SYSTEMD_DISARM_GREEN:service={service}:state=not-found")
        return
    if properties["LoadState"] != "loaded":
        raise SystemdCutoverError(f"unit load state diverged:service={service}")
    if properties["ActiveState"] in {"inactive", "failed"} and properties["MainPID"] == "0":
        _skip("stop", service, properties["ActiveState"], emit=emit)
    else:
        _mutate("stop", service, run=run, emit=emit)
        properties = _properties(service, run=run)
        if properties["ActiveState"] not in {"inactive", "failed"} or properties["MainPID"] != "0":
            raise SystemdCutoverError(f"unit did not stop:service={service}")
        _green("stop", service, emit=emit)
    if _enabled_state(service, run=run) == "disabled":
        _skip("disable", service, "disabled", emit=emit)
    else:
        _mutate("disable", service, run=run, emit=emit)
        if _enabled_state(service, run=run) != "disabled":
            raise SystemdCutoverError(f"unit did not disable:service={service}")
        _green("disable", service, emit=emit)
    emit(f"PREDICTION_SYSTEMD_DISARM_GREEN:service={service}:state=inactive-disabled")


def _ensure_enabled(service: str, *, run: CommandRunner, emit: Emitter) -> None:
    if _enabled_state(service, run=run) == "enabled":
        _skip("enable", service, "enabled", emit=emit)
        return
    _mutate("enable", service, run=run, emit=emit)
    if _enabled_state(service, run=run) != "enabled":
        raise SystemdCutoverError(f"unit did not enable:service={service}")
    _green("enable", service, emit=emit)


def ensure_active_service(
    service: str,
    *,
    run: CommandRunner = _command,
    emit: Emitter = print,
) -> None:
    service = _service(service)
    properties = _properties(service, run=run)
    if properties["LoadState"] != "loaded":
        raise SystemdCutoverError(f"required unit is not loaded:service={service}")
    if properties["NRestarts"] != "0":
        raise SystemdCutoverError(f"systemd restart count diverged:service={service}")
    _ensure_enabled(service, run=run, emit=emit)
    if properties["ActiveState"] == "active" and int(properties["MainPID"]) > 0:
        _skip("start", service, "active", emit=emit)
    else:
        _mutate("start", service, run=run, emit=emit)
        properties = _properties(service, run=run)
        if properties["ActiveState"] != "active" or int(properties["MainPID"]) <= 0:
            raise SystemdCutoverError(f"persistent unit did not become active:service={service}")
        _green("start", service, emit=emit)
    emit(f"PREDICTION_SYSTEMD_ACTIVE_GREEN:service={service}")


def ensure_probe_success(
    service: str,
    *,
    run: CommandRunner = _command,
    emit: Emitter = print,
) -> None:
    service = _service(service)
    properties = _properties(service, run=run)
    if properties["LoadState"] != "loaded":
        raise SystemdCutoverError(f"required probe unit is not loaded:service={service}")
    if properties["NRestarts"] != "0":
        raise SystemdCutoverError(f"systemd restart count diverged:service={service}")
    _ensure_enabled(service, run=run, emit=emit)
    _mutate("start", service, run=run, emit=emit)
    properties = _properties(service, run=run)
    if not (
        properties["ActiveState"] == "inactive"
        and properties["SubState"] == "dead"
        and properties["Result"] == "success"
        and properties["MainPID"] == "0"
    ):
        raise SystemdCutoverError(f"oneshot unit did not complete successfully:service={service}")
    _green("start", service, emit=emit)
    emit(f"PREDICTION_SYSTEMD_ONESHOT_GREEN:service={service}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bounded Prediction Markets systemd cutover")
    subparsers = parser.add_subparsers(dest="command", required=True)
    disarm = subparsers.add_parser("disarm")
    disarm.add_argument("--service", required=True)
    disarm.add_argument("--allow-absent", action="store_true")
    active = subparsers.add_parser("ensure-active")
    active.add_argument("--service", required=True)
    probe = subparsers.add_parser("ensure-probe")
    probe.add_argument("--service", required=True)
    return parser


def _interrupt(_signum: int, _frame: object) -> NoReturn:
    raise KeyboardInterrupt


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    signal.signal(signal.SIGINT, _interrupt)
    signal.signal(signal.SIGTERM, _interrupt)
    try:
        if arguments.command == "disarm":
            disarm_service(arguments.service, allow_absent=arguments.allow_absent)
        elif arguments.command == "ensure-active":
            ensure_active_service(arguments.service)
        else:
            ensure_probe_success(arguments.service)
        return 0
    except KeyboardInterrupt:
        operation, service = _CURRENT_OPERATION or (arguments.command, arguments.service)
        print(
            "PREDICTION_SYSTEMD_OPERATION_INTERRUPTED_RETRY_SAME_MODE:"
            f"operation={operation}:service={service}",
            file=sys.stderr,
        )
        return 130
    except (OSError, SystemdCutoverError) as error:
        print(f"PREDICTION_SYSTEMD_CUTOVER_REFUSED:{error}", file=sys.stderr)
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
