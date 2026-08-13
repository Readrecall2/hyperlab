from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

FULL_SHA = re.compile(r"[0-9a-f]{40}")
REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
RELEASE_TAG = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+")

JsonFetcher = Callable[[str], dict[str, Any]]


def resolve_tag_commit(fetch_json: JsonFetcher, tag: str) -> str:
    """Resolve lightweight or nested annotated tag objects to one commit SHA."""

    payload = fetch_json(f"git/ref/tags/{urllib.parse.quote(tag, safe='')}")
    target = payload.get("object")
    for _ in range(8):
        if not isinstance(target, dict):
            raise ValueError("GitHub tag response has no object")
        object_type = target.get("type")
        object_sha = target.get("sha")
        if not isinstance(object_sha, str) or FULL_SHA.fullmatch(object_sha) is None:
            raise ValueError("GitHub tag response contains an invalid object SHA")
        if object_type == "commit":
            return object_sha
        if object_type != "tag":
            raise ValueError(f"GitHub tag resolves to unsupported object type {object_type!r}")
        annotated = fetch_json(f"git/tags/{object_sha}")
        target = annotated.get("object")
    raise ValueError("GitHub tag annotation depth exceeds the fail-closed limit")


def verify_tag(fetch_json: JsonFetcher, *, tag: str, expected_commit: str) -> None:
    if RELEASE_TAG.fullmatch(tag) is None:
        raise ValueError("release tag must be exact vMAJOR.MINOR.PATCH")
    if FULL_SHA.fullmatch(expected_commit) is None:
        raise ValueError("expected commit must be a full lowercase Git SHA")
    observed = resolve_tag_commit(fetch_json, tag)
    if observed != expected_commit:
        raise ValueError(
            f"release tag moved or does not match the event commit: expected {expected_commit}, "
            f"observed {observed}"
        )


def _github_fetcher(repository: str, token: str) -> JsonFetcher:
    if REPOSITORY.fullmatch(repository) is None:
        raise ValueError("repository must be OWNER/REPOSITORY")
    if not token:
        raise ValueError("GITHUB_TOKEN is required to resolve the release tag fail closed")
    api_root = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
    base = f"{api_root}/repos/{repository}/"

    def fetch_json(relative_url: str) -> dict[str, Any]:
        request = urllib.request.Request(
            base + relative_url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                payload = json.load(response)
        except (OSError, urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError) as exc:
            raise ValueError("GitHub tag lookup failed closed") from exc
        if not isinstance(payload, dict):
            raise ValueError("GitHub tag lookup returned a non-object response")
        return payload

    return fetch_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify that a GitHub release tag still targets one commit.")
    parser.add_argument("--repository", required=True, help="GitHub OWNER/REPOSITORY")
    parser.add_argument("--tag", required=True, help="Exact vMAJOR.MINOR.PATCH release tag")
    parser.add_argument("--expected-commit", required=True, help="Full commit SHA frozen by preflight")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        fetch_json = _github_fetcher(args.repository, os.environ.get("GITHUB_TOKEN", ""))
        verify_tag(fetch_json, tag=args.tag, expected_commit=args.expected_commit)
    except ValueError as exc:
        print(f"GitHub release-tag verification FAILED: {exc}", file=sys.stderr)
        return 1
    print(f"GitHub release tag {args.tag} still targets {args.expected_commit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
