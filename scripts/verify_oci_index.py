from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

FULL_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
REQUIRED_PLATFORMS = {("linux", "amd64"), ("linux", "arm64")}


def exact_platform_digests(payload: Any) -> dict[str, str]:
    if not isinstance(payload, dict):
        raise ValueError("OCI index must be a JSON object")
    manifests = payload.get("manifests")
    if not isinstance(manifests, list) or len(manifests) != len(REQUIRED_PLATFORMS):
        raise ValueError("OCI index must contain exactly two child manifests")

    observed: dict[tuple[str, str], str] = {}
    for item in manifests:
        if not isinstance(item, dict):
            raise ValueError("each child manifest must be a JSON object")
        platform = item.get("platform")
        if not isinstance(platform, dict):
            raise ValueError("each child manifest must declare one explicit platform")
        key = (platform.get("os"), platform.get("architecture"))
        if key not in REQUIRED_PLATFORMS:
            raise ValueError(f"unexpected child platform: {key!r}")
        if key in observed:
            raise ValueError(f"duplicate child platform: {key!r}")
        digest = item.get("digest")
        if not isinstance(digest, str) or FULL_DIGEST.fullmatch(digest) is None:
            raise ValueError(f"invalid child digest for {key!r}")
        observed[key] = digest

    if set(observed) != REQUIRED_PLATFORMS:
        raise ValueError("OCI index does not contain exactly linux/amd64 and linux/arm64")
    return {architecture: observed[("linux", architecture)] for architecture in ("amd64", "arm64")}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fail closed unless an OCI index has exactly the Phase 15 platforms."
    )
    parser.add_argument("manifest", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        payload = json.loads(args.manifest.read_text(encoding="utf-8"))
        digests = exact_platform_digests(payload)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"OCI index verification FAILED: {exc}", file=sys.stderr)
        return 1
    for architecture, digest in digests.items():
        print(f"{architecture}_digest={digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
