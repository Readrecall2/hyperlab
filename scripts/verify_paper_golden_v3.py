from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from pathlib import Path, PurePath
from typing import TextIO

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPOSITORY_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from hyperlab.paper.golden_v3 import (  # noqa: E402
    GoldenDifferentialError,
    GoldenRefusal,
    GoldenVerificationError,
    compare_golden_exports,
    validate_new_auxiliary_path,
    verify_golden_v3,
)


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
        description="Verify, compare, or exhaustively replay a complete logical Golden V3 export."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    verify_parser = subparsers.add_parser("verify", help="Verify one complete export without writes")
    verify_parser.add_argument("export_root", type=Path)
    verify_parser.add_argument("--pin", type=Path, help="External local pin to verify")

    compare_parser = subparsers.add_parser("compare", help="Compare two exports by logical identity")
    compare_parser.add_argument("expected_root", type=Path)
    compare_parser.add_argument("actual_root", type=Path)

    replay_parser = subparsers.add_parser(
        "replay",
        help="Replay every canonical input into one new disposable target and compare all streams",
    )
    replay_parser.add_argument("export_root", type=Path)
    replay_parser.add_argument("scratch_root", type=Path, help="New, absent disposable replay directory")
    replay_parser.add_argument("--target-filename", default="paper-replay.sqlite3")
    replay_parser.add_argument("--progress-jsonl", type=Path, help="New append-only progress JSONL path")
    return parser.parse_args()


def _verify(args: argparse.Namespace) -> dict[str, object]:
    return verify_golden_v3(args.export_root, pin_path=args.pin).to_dict()


def _compare(args: argparse.Namespace) -> dict[str, object]:
    return compare_golden_exports(args.expected_root, args.actual_root).to_dict()


def _replay(args: argparse.Namespace) -> dict[str, object]:
    if args.scratch_root.exists():
        raise GoldenRefusal(f"scratch root already exists: {args.scratch_root}")
    target = PurePath(args.target_filename)
    if target.name != args.target_filename or target.name in {"", ".", ".."}:
        raise GoldenRefusal("target filename must be one plain filename")
    if args.progress_jsonl is not None:
        args.progress_jsonl = validate_new_auxiliary_path(
            args.progress_jsonl,
            forbidden_paths=(
                args.export_root,
                args.scratch_root,
                args.scratch_root / args.target_filename,
            ),
            label="replay progress JSONL",
        )
    progress = _ProgressJsonl(args.progress_jsonl)
    try:
        from hyperlab.paper.golden_v3_replay import replay_golden_v3

        return replay_golden_v3(
            args.export_root,
            args.scratch_root,
            progress=progress,
            target_filename=args.target_filename,
        )
    finally:
        progress.close()


def main() -> int:
    args = _parse_args()
    try:
        if args.command == "verify":
            payload = _verify(args)
        elif args.command == "compare":
            payload = _compare(args)
        else:
            payload = _replay(args)
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


if __name__ == "__main__":
    raise SystemExit(main())
