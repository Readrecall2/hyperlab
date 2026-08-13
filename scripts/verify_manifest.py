from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
from pathlib import Path

MANIFEST_NAME = "MANIFEST_SHA256.txt"


def canonical_bytes(path: Path) -> bytes:
    """Return platform-independent bytes for a source file.

    Git may materialize text files with CRLF on Windows. The release manifest hashes
    UTF-8 text with LF endings and hashes binary files byte-for-byte.
    """

    content = path.read_bytes()
    if b"\0" in content:
        return content
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return content
    return text.replace("\r\n", "\n").encode("utf-8")


def sha256_file(path: Path) -> str:
    return hashlib.sha256(canonical_bytes(path)).hexdigest()


def release_paths(root: Path) -> tuple[str, ...]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    paths = []
    for raw_path in result.stdout.split(b"\0"):
        if not raw_path:
            continue
        path = raw_path.decode("utf-8")
        candidate = root / path
        if path == MANIFEST_NAME or not candidate.is_file():
            continue
        paths.append(path.replace("\\", "/"))
    return tuple(sorted(set(paths)))


def build_manifest(root: Path, paths: tuple[str, ...] | None = None) -> str:
    selected = paths if paths is not None else release_paths(root)
    lines = [f"{sha256_file(root / path)}  {path}" for path in selected]
    return "\n".join(lines) + "\n"


def verify_manifest(root: Path) -> list[str]:
    manifest_path = root / MANIFEST_NAME
    if not manifest_path.is_file():
        return [f"missing {MANIFEST_NAME}"]
    expected = build_manifest(root)
    actual = manifest_path.read_text(encoding="utf-8").replace("\r\n", "\n")
    if actual == expected:
        return []

    actual_lines = set(actual.splitlines())
    expected_lines = set(expected.splitlines())
    errors = [f"{MANIFEST_NAME} is stale or incomplete"]
    missing = sorted(expected_lines - actual_lines)
    extra = sorted(actual_lines - expected_lines)
    errors.extend(f"missing/current: {line}" for line in missing[:20])
    errors.extend(f"stale/extra: {line}" for line in extra[:20])
    if len(missing) > 20 or len(extra) > 20:
        errors.append(f"additional differences omitted: missing={len(missing)}, extra={len(extra)}")
    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify or regenerate the canonical source manifest.")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--write",
        action="store_true",
        help="Regenerate MANIFEST_SHA256.txt. Review and stage it explicitly afterwards.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    if args.write:
        (root / MANIFEST_NAME).write_text(build_manifest(root), encoding="utf-8", newline="\n")
        print(f"updated {root / MANIFEST_NAME}")
        return 0
    errors = verify_manifest(root)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        print("Run: python scripts/verify_manifest.py --write", file=sys.stderr)
        return 1
    print(f"verified {MANIFEST_NAME}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
