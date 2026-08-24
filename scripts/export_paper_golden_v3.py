from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import TextIO

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPOSITORY_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from hyperlab.paper.golden_v3 import (  # noqa: E402
    GoldenDifferentialError,
    GoldenRefusal,
    GoldenVerificationError,
    export_golden_v3,
    validate_new_auxiliary_path,
    write_external_pin,
)

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


def _json_line(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


class _ProgressJsonl:
    def __init__(self, path: Path | None) -> None:
        self._path = path
        self._stream: TextIO | None = None
        if path is not None and path.exists():
            raise FileExistsError(f"progress JSONL already exists: {path}")

    def __call__(self, record: Mapping[str, object]) -> None:
        if self._path is None:
            return
        if self._stream is None:
            self._stream = self._path.open("x", encoding="utf-8", newline="\n")
        self._stream.write(_json_line(dict(record)) + "\n")
        self._stream.flush()

    def close(self) -> None:
        if self._stream is not None:
            self._stream.close()
            self._stream = None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export one immutable logical Golden V3 candidate from an offline PaperStore v3 copy."
    )
    parser.add_argument("source", type=Path, help="Offline SQLite copy opened read-only by the exporter")
    parser.add_argument("output_root", type=Path, help="New, absent extraction directory")
    parser.add_argument("--run-id", required=True, help="Exact 64-hex Paper run identity")
    parser.add_argument("--sentinel", required=True, type=Path, help="Forbidden-original sentinel path")
    parser.add_argument("--expected-size", required=True, type=int, help="Expected source size in bytes")
    parser.add_argument("--expected-sha256", required=True, help="Expected lowercase source SHA-256")
    parser.add_argument("--external-pin", required=True, type=Path, help="New local pin outside the corpus")
    parser.add_argument("--shard-rows", type=int, default=100_000)
    parser.add_argument("--shard-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--progress-jsonl", type=Path, help="New append-only progress JSONL path")
    return parser.parse_args()


def _validate_args(args: argparse.Namespace) -> None:
    if args.output_root.exists():
        raise GoldenRefusal(f"output root already exists: {args.output_root}")
    if args.external_pin.exists():
        raise GoldenRefusal(f"external pin already exists: {args.external_pin}")
    if args.expected_size < 0:
        raise GoldenRefusal("expected source size must be non-negative")
    if _SHA256_RE.fullmatch(args.expected_sha256) is None:
        raise GoldenRefusal("expected source SHA-256 must be exactly 64 lowercase hex characters")
    if _SHA256_RE.fullmatch(args.run_id) is None:
        raise GoldenRefusal("run ID must be exactly 64 lowercase hex characters")
    if args.shard_rows <= 0 or args.shard_bytes <= 0:
        raise GoldenRefusal("shard bounds must be positive")
    source_sidecars = tuple(
        Path(f"{args.source}{suffix}") for suffix in ("-journal", "-shm", "-wal")
    )
    args.external_pin = validate_new_auxiliary_path(
        args.external_pin,
        forbidden_paths=(
            args.source,
            *source_sidecars,
            args.sentinel,
            args.output_root,
        ),
        label="external pin",
        required_suffix=None,
        require_existing_parent=False,
    )
    if args.progress_jsonl is not None:
        args.progress_jsonl = validate_new_auxiliary_path(
            args.progress_jsonl,
            forbidden_paths=(
                args.source,
                *source_sidecars,
                args.sentinel,
                args.output_root,
                args.external_pin,
            ),
            label="export progress JSONL",
        )


def main() -> int:
    args = _parse_args()
    progress: _ProgressJsonl | None = None
    try:
        _validate_args(args)
        progress = _ProgressJsonl(args.progress_jsonl)
        result = export_golden_v3(
            args.source,
            args.output_root,
            args.run_id,
            sentinel_path=args.sentinel,
            expected_source_size=args.expected_size,
            expected_source_sha256=args.expected_sha256,
            require_readonly=True,
            shard_rows=args.shard_rows,
            shard_bytes=args.shard_bytes,
            progress=progress,
        )
        source_sidecars = tuple(
            Path(f"{args.source}{suffix}") for suffix in ("-journal", "-shm", "-wal")
        )
        pin_path = write_external_pin(
            args.output_root,
            args.external_pin,
            forbidden_paths=(
                args.source,
                *source_sidecars,
                args.sentinel,
            ),
        )
        payload = result.to_dict()
        payload["external_pin"] = str(pin_path)
        print(_json_line(payload))
        return 0
    except KeyboardInterrupt:
        print(_json_line({"complete": False, "status": "INTERRUPTED"}))
        return 130
    except (
        GoldenRefusal,
        GoldenVerificationError,
        GoldenDifferentialError,
        FileExistsError,
        OSError,
        TypeError,
        ValueError,
    ) as error:
        print(
            _json_line(
                {
                    "complete": False,
                    "error": str(error),
                    "error_type": type(error).__name__,
                    "status": "BLOCKED",
                }
            )
        )
        return 2
    finally:
        if progress is not None:
            progress.close()


if __name__ == "__main__":
    raise SystemExit(main())
