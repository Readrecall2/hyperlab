"""Run the complete Golden V3 certification under a bounded supervisor.

This command enforces the sole 7,200-second outer safety ceiling in-process.
The limit interrupts the main thread so normal fail-closed unwinding retains
the partial candidate and never publishes COMPLETE.
"""

from __future__ import annotations

import _thread
import argparse
import json
import os
import re
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import IO, Any, Self

from hyperlab.paper.golden_v3 import (
    GoldenDifferentialError,
    GoldenRefusal,
    GoldenVerificationError,
    _fsync_directory,
)
from hyperlab.paper.golden_v3_certification import (
    GoldenCertificationError,
    GoldenReplayDivergenceError,
    _discover_partial_candidate,
    certify_golden_v3,
    resume_golden_v3_certification,
)

_SHA256_RE = re.compile(r"[0-9a-fA-F]{64}\Z")
_HEARTBEAT_INTERVAL_SECONDS = 25.0
_MAX_SAFETY_SECONDS = 7_200.0
_GENUINE_INTEGRITY_BLOCKED = "GOLDEN_V3_CERTIFICATION_GENUINE_INTEGRITY_BLOCKED"
_REPLAY_DIVERGED = "GOLDEN_V3_CERTIFICATION_REPLAY_DIVERGED"


def _utc_now_text() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _canonical_json(payload: Mapping[str, object]) -> str:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    )


class _JsonConsole:
    """Serialize concurrent progress and heartbeat output one line at a time."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    def emit(self, payload: Mapping[str, object], *, error: bool = False) -> None:
        stream = sys.stderr if error else sys.stdout
        line = _canonical_json(payload)
        with self._lock:
            print(line, file=stream, flush=True)


class _ProgressJsonl:
    """Durable, thread-safe JSONL progress sink with lazy exclusive creation."""

    def __init__(self, path: Path, console: _JsonConsole) -> None:
        self._path = path
        self._console = console
        self._lock = threading.Lock()
        self._async_error_lock = threading.Lock()
        self._handle: IO[str] | None = None
        self._last_progress: dict[str, object] = {}
        self._progress_epoch_started_at = time.monotonic()
        self._stream_totals: dict[str, int] = {}
        self._heartbeat: _Heartbeat | None = None
        self._deadline: _SafetyDeadline | None = None
        self._async_error: BaseException | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.close()

    def __call__(self, payload: Mapping[str, object]) -> None:
        self._raise_async_error()
        event: dict[str, object] = dict(payload)
        event.setdefault("timestamp_utc", _utc_now_text())
        with self._lock:
            if event.get("event") != "heartbeat":
                stream = event.get("stream")
                total_expected = event.get("total_expected")
                if (
                    isinstance(stream, str)
                    and isinstance(total_expected, int)
                    and not isinstance(total_expected, bool)
                    and total_expected >= 0
                ):
                    self._stream_totals[stream] = total_expected
                elif isinstance(stream, str) and stream in self._stream_totals:
                    event["total_expected"] = self._stream_totals[stream]

            line = _canonical_json(event)
            created = False
            if self._handle is None:
                # The certifier creates candidate_root before its first event.
                # Mode "x" preserves the no-overwrite contract even after the
                # command-line preflight race window.
                if not self._path.parent.is_dir():
                    raise FileNotFoundError(
                        f"progress JSONL parent is not ready: {self._path.parent}"
                    )
                self._handle = self._path.open("x", encoding="utf-8", newline="\n")
                created = True
            self._handle.write(line)
            self._handle.write("\n")
            self._handle.flush()
            os.fsync(self._handle.fileno())
            if created:
                _fsync_directory(self._path.parent)
            if event.get("event") != "heartbeat":
                phase_or_stream_changed = (
                    event.get("phase") != self._last_progress.get("phase")
                    or (
                        "stream" in event
                        and event.get("stream") != self._last_progress.get("stream")
                    )
                    or (
                        "validation_step" in event
                        and event.get("validation_step")
                        != self._last_progress.get("validation_step")
                    )
                )
                if phase_or_stream_changed:
                    latest = {
                        key: self._last_progress[key]
                        for key in (
                            "total_expected",
                            "target_path",
                            "target_store_bytes",
                        )
                        if key in self._last_progress
                    }
                    self._progress_epoch_started_at = time.monotonic()
                else:
                    latest = dict(self._last_progress)
                latest.update({
                    key: event[key]
                    for key in (
                        "phase",
                        "stream",
                        "validation_step",
                        "rows_completed",
                        "commits_completed",
                        "bytes_completed",
                        "total_expected",
                        "files_completed",
                        "files_total",
                        "last_identity",
                        "last_input_id",
                        "target_path",
                        "target_store_bytes",
                        "eta_seconds",
                    )
                    if key in event
                })
                self._last_progress = latest
        self._console.emit(event)
        self._raise_async_error()
        if event.get("phase") == "certification_ready_to_publish":
            if self._heartbeat is not None:
                self._heartbeat.stop()
            if self._deadline is not None:
                self._deadline.finish_before_complete()

    def bind_heartbeat(self, heartbeat: _Heartbeat) -> None:
        if self._heartbeat is not None:
            raise RuntimeError("heartbeat is already bound")
        self._heartbeat = heartbeat

    def bind_deadline(self, deadline: _SafetyDeadline) -> None:
        if self._deadline is not None:
            raise RuntimeError("safety deadline is already bound")
        self._deadline = deadline

    def record_async_failure(self, error: BaseException) -> None:
        with self._async_error_lock:
            if self._async_error is None:
                self._async_error = error

    def _raise_async_error(self) -> None:
        with self._async_error_lock:
            error = self._async_error
        if error is not None:
            raise OSError(
                "durable Golden V3 heartbeat failed; certification is blocked"
            ) from error

    def heartbeat(self, *, elapsed_seconds: float, cpu_seconds: float) -> None:
        with self._lock:
            latest = dict(self._last_progress)
            progress_elapsed = time.monotonic() - self._progress_epoch_started_at
        total_expected = latest.get("total_expected")
        completed = next(
            (
                latest[key]
                for key in (
                    "commits_completed",
                    "rows_completed",
                    "bytes_completed",
                    "files_completed",
                )
                if isinstance(latest.get(key), int)
                and not isinstance(latest[key], bool)
            ),
            None,
        )
        if (
            isinstance(total_expected, int)
            and not isinstance(total_expected, bool)
            and isinstance(completed, int)
            and total_expected >= completed
        ):
            if completed == total_expected:
                latest["eta_seconds"] = 0.0
            elif completed > 0:
                latest["eta_seconds"] = round(
                    progress_elapsed * (total_expected - completed) / completed,
                    3,
                )
        self(
            {
                **latest,
                "cpu_seconds": round(cpu_seconds, 3),
                "elapsed_seconds": round(elapsed_seconds, 3),
                "event": "heartbeat",
                "status": "RUNNING",
            }
        )

    def close(self) -> None:
        with self._lock:
            if self._handle is None:
                return
            self._handle.close()
            self._handle = None

    def close_quietly(self) -> None:
        with suppress(OSError):
            self.close()


class _Heartbeat:
    """Emit durable JSONL liveness records while certification is running."""

    def __init__(
        self,
        progress: _ProgressJsonl,
        *,
        interval_seconds: float = _HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("heartbeat interval must be positive")
        self._progress = progress
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._failure_lock = threading.Lock()
        self._failure: BaseException | None = None
        self._started_at = time.monotonic()
        self._thread = threading.Thread(
            target=self._run,
            name="golden-v3-heartbeat",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    @property
    def failed(self) -> bool:
        with self._failure_lock:
            return self._failure is not None

    def stop(self, *, raise_on_error: bool = True) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=self._interval_seconds + 1.0)
        if self._thread.is_alive():
            error = TimeoutError("heartbeat thread did not stop cleanly")
            self._record_failure(error)
        if raise_on_error:
            self._progress._raise_async_error()

    def _run(self) -> None:
        try:
            while not self._stop.wait(self._interval_seconds):
                self._progress.heartbeat(
                    elapsed_seconds=time.monotonic() - self._started_at,
                    cpu_seconds=time.process_time(),
                )
        except BaseException as error:
            self._record_failure(error)

    def _record_failure(self, error: BaseException) -> None:
        with self._failure_lock:
            if self._failure is None:
                self._failure = error
        self._progress.record_async_failure(error)


class _SafetyDeadline:
    """Interrupt the main thread at the sole 7,200-second safety ceiling."""

    def __init__(self, *, seconds: float = _MAX_SAFETY_SECONDS) -> None:
        if seconds <= 0:
            raise ValueError("safety deadline must be positive")
        self._seconds = seconds
        self._stop = threading.Event()
        self._expired = threading.Event()
        self._started_at: float | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="golden-v3-safety-deadline",
            daemon=True,
        )

    @property
    def expired(self) -> bool:
        return self._expired.is_set()

    def start(self) -> None:
        self._started_at = time.monotonic()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def finish_before_complete(self) -> None:
        self.stop()
        started_at = self._started_at
        if (
            self.expired
            or started_at is None
            or time.monotonic() - started_at >= self._seconds
        ):
            self._expired.set()
            raise KeyboardInterrupt

    def _run(self) -> None:
        if not self._stop.wait(self._seconds):
            self._expired.set()
            _thread.interrupt_main()


@dataclass(frozen=True)
class _Arguments:
    source: Path
    candidate_root: Path
    golden_root: Path | None
    resume_existing_a: bool
    run_id: str
    sentinel: Path
    expected_size: int
    expected_sha256: str
    shard_rows: int
    shard_bytes: int
    progress_jsonl: Path
    expected_export_a_root_hash: str | None
    expected_export_a_file_count: int | None
    expected_export_a_bytes: int | None


def _hash_text(value: str, *, label: str) -> str:
    if _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be exactly 64 hexadecimal characters")
    return value.lower()


def _positive_int(value: Any, *, label: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{label} must be greater than zero")
    return parsed


def _lexical_absolute(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path.expanduser())))


def _same_or_contains(left: Path, right: Path) -> bool:
    try:
        right.relative_to(left)
        return True
    except ValueError:
        pass
    try:
        left.relative_to(right)
        return True
    except ValueError:
        return False


def _validated_arguments(namespace: argparse.Namespace) -> _Arguments:
    source = _lexical_absolute(Path(namespace.source))
    if not source.is_file():
        raise ValueError(f"source is not a regular file: {source}")

    requested_root = _lexical_absolute(Path(namespace.candidate_root))
    resume_existing_a = bool(getattr(namespace, "resume_existing_a", False))
    golden_root: Path | None = None
    if resume_existing_a:
        golden_root = requested_root
        candidate_root = _discover_partial_candidate(golden_root)
    else:
        candidate_root = requested_root
    sentinel = _lexical_absolute(Path(namespace.sentinel))
    progress_jsonl = _lexical_absolute(Path(namespace.progress_jsonl))

    if not resume_existing_a and (
        candidate_root.exists() or candidate_root.is_symlink()
    ):
        raise FileExistsError(f"candidate root already exists: {candidate_root}")
    if progress_jsonl.exists() or progress_jsonl.is_symlink():
        raise FileExistsError(f"progress JSONL already exists: {progress_jsonl}")
    if _same_or_contains(source, candidate_root):
        raise ValueError("candidate root must be distinct from and outside the source")
    if _same_or_contains(source, sentinel):
        raise ValueError("sentinel must be distinct from and outside the source")
    if _same_or_contains(candidate_root, sentinel):
        raise ValueError("candidate root and sentinel collide or contain one another")
    expected_progress_parent = candidate_root / "results"
    if (
        progress_jsonl.parent != expected_progress_parent
        or progress_jsonl.suffix.lower() != ".jsonl"
    ):
        raise ValueError(
            "progress JSONL must be a new .jsonl file directly below candidate_root/results"
        )

    raw_export_a_root = getattr(namespace, "expected_export_a_root_hash", None)
    raw_export_a_files = getattr(namespace, "expected_export_a_file_count", None)
    raw_export_a_bytes = getattr(namespace, "expected_export_a_bytes", None)
    if resume_existing_a:
        if (
            raw_export_a_root is None
            or raw_export_a_files is None
            or raw_export_a_bytes is None
        ):
            raise ValueError(
                "resume requires expected export A root hash, file count, and bytes"
            )
        expected_export_a_root_hash = _hash_text(
            str(raw_export_a_root),
            label="expected export A root hash",
        )
        expected_export_a_file_count = _positive_int(
            raw_export_a_files,
            label="expected export A file count",
        )
        expected_export_a_bytes = _positive_int(
            raw_export_a_bytes,
            label="expected export A bytes",
        )
    else:
        if any(
            value is not None
            for value in (raw_export_a_root, raw_export_a_files, raw_export_a_bytes)
        ):
            raise ValueError(
                "export A reuse expectations require --resume-existing-a"
            )
        expected_export_a_root_hash = None
        expected_export_a_file_count = None
        expected_export_a_bytes = None

    return _Arguments(
        source=source,
        candidate_root=candidate_root,
        golden_root=golden_root,
        resume_existing_a=resume_existing_a,
        run_id=_hash_text(str(namespace.run_id), label="run id"),
        sentinel=sentinel,
        expected_size=_positive_int(namespace.expected_size, label="expected size"),
        expected_sha256=_hash_text(
            str(namespace.expected_sha256), label="expected SHA-256"
        ),
        shard_rows=_positive_int(namespace.shard_rows, label="shard rows"),
        shard_bytes=_positive_int(namespace.shard_bytes, label="shard bytes"),
        progress_jsonl=progress_jsonl,
        expected_export_a_root_hash=expected_export_a_root_hash,
        expected_export_a_file_count=expected_export_a_file_count,
        expected_export_a_bytes=expected_export_a_bytes,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Certify a complete, immutable Golden V3 replay oracle.",
        epilog=(
            "The command enforces the sole 7200-second safety ceiling in-process; "
            "an interrupted partial candidate is retained. Explicit resume is "
            "allowed only for one uniquely discovered, exhaustively reattested A."
        ),
    )
    parser.add_argument("source", help="read-only source SQLite path")
    parser.add_argument(
        "candidate_root",
        help=(
            "new absent candidate directory, or Golden root containing exactly "
            "one partial candidate with --resume-existing-a"
        ),
    )
    parser.add_argument("--run-id", required=True, help="expected 64-hex run identity")
    parser.add_argument(
        "--sentinel",
        required=True,
        help="forbidden original path; it may exist but must not alias the source",
    )
    parser.add_argument("--expected-size", required=True, help="expected source size in bytes")
    parser.add_argument(
        "--expected-sha256", required=True, help="expected source SHA-256"
    )
    parser.add_argument("--shard-rows", required=True, help="maximum rows per shard")
    parser.add_argument("--shard-bytes", required=True, help="maximum bytes per shard")
    parser.add_argument(
        "--progress-jsonl",
        required=True,
        help="new .jsonl file directly below candidate_root/results",
    )
    parser.add_argument(
        "--resume-existing-a",
        action="store_true",
        help="reuse and exhaustively reattest the unique existing export A",
    )
    parser.add_argument("--expected-export-a-root-hash")
    parser.add_argument("--expected-export-a-file-count")
    parser.add_argument("--expected-export-a-bytes")
    return parser


def _failure_verdict(exc: BaseException) -> str:
    pending = [exc]
    visited: set[int] = set()
    while pending:
        current = pending.pop()
        identity = id(current)
        if identity in visited:
            continue
        visited.add(identity)
        if isinstance(current, GoldenReplayDivergenceError):
            return _REPLAY_DIVERGED
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
    return _GENUINE_INTEGRITY_BLOCKED


def _blocked_payload(exc: BaseException) -> dict[str, object]:
    return {
        "error": str(exc),
        "error_type": type(exc).__name__,
        "status": _failure_verdict(exc),
        "timestamp_utc": _utc_now_text(),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    namespace = parser.parse_args(argv)
    console = _JsonConsole()

    try:
        args = _validated_arguments(namespace)
    except (GoldenCertificationError, OSError, TypeError, ValueError) as exc:
        console.emit(_blocked_payload(exc), error=True)
        return 2

    progress = _ProgressJsonl(args.progress_jsonl, console)
    heartbeat = _Heartbeat(progress)
    deadline = _SafetyDeadline()
    progress.bind_heartbeat(heartbeat)
    progress.bind_deadline(deadline)
    console.emit(
        {
            "candidate_root": str(args.candidate_root),
            "phase": "certification",
            "source": str(args.source),
            "status": "STARTED",
            "timestamp_utc": _utc_now_text(),
        }
    )
    try:
        deadline.start()
        heartbeat.start()
        if args.resume_existing_a:
            if (
                args.golden_root is None
                or args.expected_export_a_root_hash is None
                or args.expected_export_a_file_count is None
                or args.expected_export_a_bytes is None
            ):
                raise GoldenCertificationError(
                    "validated resume arguments are incomplete"
                )
            result = resume_golden_v3_certification(
                args.source,
                args.golden_root,
                args.run_id,
                sentinel_path=args.sentinel,
                expected_source_size=args.expected_size,
                expected_source_sha256=args.expected_sha256,
                expected_export_a_root_hash=args.expected_export_a_root_hash,
                expected_export_a_file_count=args.expected_export_a_file_count,
                expected_export_a_bytes=args.expected_export_a_bytes,
                progress=progress,
                shard_rows=args.shard_rows,
                shard_bytes=args.shard_bytes,
            )
        else:
            result = certify_golden_v3(
                args.source,
                args.candidate_root,
                args.run_id,
                sentinel_path=args.sentinel,
                expected_source_size=args.expected_size,
                expected_source_sha256=args.expected_sha256,
                progress=progress,
                shard_rows=args.shard_rows,
                shard_bytes=args.shard_bytes,
            )
        heartbeat.stop()
        deadline.stop()
        progress.close()
        console.emit(result.to_dict())
        return 0
    except KeyboardInterrupt:
        timed_out = deadline.expired
        console.emit(
            {
                "status": "TIMEOUT" if timed_out else "INTERRUPTED",
                "timestamp_utc": _utc_now_text(),
            },
            error=True,
        )
        return 124 if timed_out else 130
    except (
        GoldenCertificationError,
        GoldenRefusal,
        GoldenVerificationError,
        GoldenDifferentialError,
        OSError,
        TypeError,
        ValueError,
    ) as exc:
        console.emit(_blocked_payload(exc), error=True)
        return 2
    finally:
        deadline.stop()
        heartbeat.stop(raise_on_error=False)
        progress.close_quietly()


if __name__ == "__main__":
    raise SystemExit(main())
