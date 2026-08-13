from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any

GITHUB_OWNER = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,37}[a-z0-9])?")
REPOSITORY = re.compile(r"[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?")
SEMVER = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
DIGEST = re.compile(r"[0-9a-f]{64}")
FULL_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
FULL_SHA = re.compile(r"[0-9a-f]{40}")
IMAGE_LINE = re.compile(r"(?m)^(\s*image:\s*)ghcr\.io/[^\s]+$")
MANIFEST_VERSION = re.compile(r'(?m)^version:\s*"[^"]+"\s*$')
RAW_ICON = re.compile(r"https://raw\.githubusercontent\.com/[^/\s]+/[^/\s]+/[^/\s]+/jjlab-hyperlab/icon\.svg")
TEMPLATE_IMAGE = (
    "ghcr.io/REPLACE_WITH_GITHUB_USER/REPLACE_WITH_REPOSITORY:"
    "REPLACE_WITH_IMAGE_VERSION@sha256:REPLACE_WITH_IMAGE_DIGEST"
)
TEMPLATE_ICON = (
    "https://raw.githubusercontent.com/REPLACE_WITH_GITHUB_USER/"
    "REPLACE_WITH_REPOSITORY/vREPLACE_WITH_IMAGE_VERSION/jjlab-hyperlab/icon.svg"
)
CommandRunner = Callable[[list[str]], bytes]


def _project_version(root: Path) -> str:
    with (root / "pyproject.toml").open("rb") as handle:
        return str(tomllib.load(handle)["project"]["version"])


def _validate_inputs(
    root: Path,
    github_user: str,
    repository: str,
    image_version: str,
    image_digest: str,
) -> None:
    if GITHUB_OWNER.fullmatch(github_user) is None:
        raise ValueError("github_user must be a lowercase GitHub account or organization name")
    if REPOSITORY.fullmatch(repository) is None or repository in {".", ".."}:
        raise ValueError("repository must be a lowercase plain GitHub repository name")
    if SEMVER.fullmatch(image_version) is None:
        raise ValueError("image version must be an exact MAJOR.MINOR.PATCH version")
    if image_version != _project_version(root):
        raise ValueError(
            f"image version {image_version} does not match project version {_project_version(root)}"
        )
    if DIGEST.fullmatch(image_digest) is None:
        raise ValueError("image digest must be exactly 64 lowercase hexadecimal characters")


def _render_files(
    root: Path,
    *,
    github_user: str,
    repository: str,
    image_version: str,
    image_digest: str,
) -> dict[Path, str]:
    image = f"ghcr.io/{github_user}/{repository}:{image_version}@sha256:{image_digest}"
    replacements = {
        "REPLACE_WITH_GITHUB_USER": github_user,
        "REPLACE_WITH_REPOSITORY": repository,
        "REPLACE_WITH_IMAGE_VERSION": image_version,
        "REPLACE_WITH_IMAGE_DIGEST": image_digest,
        "REPLACE_WITH_TAG": image_version,
    }
    candidates = [root / "umbrel-app-store.yml", *(root / "jjlab-hyperlab").rglob("*")]
    rendered: dict[Path, str] = {}
    for path in candidates:
        if not path.is_file() or path.suffix not in {".yml", ".yaml", ".md"}:
            continue
        current = path.read_text(encoding="utf-8")
        updated = current
        for old, new in replacements.items():
            updated = updated.replace(old, new)
        if path.name == "docker-compose.yml":
            updated = IMAGE_LINE.sub(lambda match: f"{match.group(1)}{image}", updated)
        if path.name == "umbrel-app.yml":
            updated = MANIFEST_VERSION.sub(f'version: "{image_version}"', updated)
            icon = f"https://raw.githubusercontent.com/{github_user}/{repository}/v{image_version}/jjlab-hyperlab/icon.svg"
            updated = RAW_ICON.sub(icon, updated)
        rendered[path] = updated

    combined = "\n".join(rendered.values())
    if "REPLACE_WITH_" in combined:
        raise ValueError("prepared package would retain unresolved placeholders")
    compose = rendered[root / "jjlab-hyperlab/docker-compose.yml"]
    if compose.count(f"image: {image}") != 2:
        raise ValueError("prepared package must pin dashboard and collector to the same digest")
    manifest = rendered[root / "jjlab-hyperlab/umbrel-app.yml"]
    if f'version: "{image_version}"' not in manifest or "/main/" in manifest:
        raise ValueError("prepared manifest version or immutable icon reference is invalid")
    return rendered


def _default_command_runner(command: list[str]) -> bytes:
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
        )
    except FileNotFoundError as exc:
        raise ValueError(f"required release verifier is unavailable: {command[0]}") from exc
    except subprocess.CalledProcessError as exc:
        raise ValueError(f"release verification failed closed: {command[0]}") from exc
    return completed.stdout


def _read_receipt(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("release receipt is unavailable or invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("release receipt must contain one JSON object")
    return payload


def _verify_release_evidence(
    *,
    github_user: str,
    repository: str,
    image_version: str,
    image_digest: str,
    release_receipt: Path,
    receipt_bundle: Path,
    command_runner: CommandRunner,
) -> None:
    receipt_path = release_receipt.resolve()
    bundle_path = receipt_bundle.resolve()
    if not bundle_path.is_file():
        raise ValueError("signed receipt bundle is unavailable")
    receipt = _read_receipt(receipt_path)
    image = f"ghcr.io/{github_user}/{repository}"
    index_digest = f"sha256:{image_digest}"
    source_commit = receipt.get("source_commit")
    expected_scalars = {
        "schema": "hyperlab-phase15-release-receipt-v1",
        "image": image,
        "version": image_version,
        "release_tag": f"v{image_version}",
        "index_digest": index_digest,
        "workflow": ".github/workflows/container.yml",
    }
    for field, expected in expected_scalars.items():
        if receipt.get(field) != expected:
            raise ValueError(f"release receipt {field} does not match the requested release")
    if not isinstance(source_commit, str) or FULL_SHA.fullmatch(source_commit) is None:
        raise ValueError("release receipt source_commit must be a full lowercase commit SHA")

    platforms = receipt.get("platform_digests")
    if not isinstance(platforms, dict) or set(platforms) != {"linux/amd64", "linux/arm64"}:
        raise ValueError("release receipt must contain exactly amd64 and arm64 child digests")
    child_digests = list(platforms.values())
    if any(not isinstance(value, str) or FULL_DIGEST.fullmatch(value) is None for value in child_digests):
        raise ValueError("release receipt contains an invalid child digest")
    if len(set(child_digests)) != 2:
        raise ValueError("release receipt child digests must be distinct")

    sbom_hashes = receipt.get("sbom_sha256")
    expected_sboms = {"sbom-amd64.spdx.json", "sbom-arm64.spdx.json"}
    if not isinstance(sbom_hashes, dict) or set(sbom_hashes) != expected_sboms:
        raise ValueError("release receipt must bind both exact-platform SBOM files")
    if any(
        not isinstance(value, str) or FULL_DIGEST.fullmatch(value) is None for value in sbom_hashes.values()
    ):
        raise ValueError("release receipt contains an invalid SBOM digest")

    gate = receipt.get("vulnerability_gate")
    expected_gate = {
        "scanner": "trivy",
        "scanner_version": "0.72.0",
        "scanner_archive_sha256": "bbb64b9695866ce4a7a8f5c9592002c5961cab378577fa3f8a040df362b9b2ea",
        "severity": ["HIGH", "CRITICAL"],
        "ignore_unfixed": False,
        "result": "pass",
    }
    if gate != expected_gate:
        raise ValueError("release receipt does not prove the required vulnerability gate")

    owner_repository = f"{github_user}/{repository}"
    tag_ref = f"refs/tags/v{image_version}"
    certificate_identity = f"https://github.com/{owner_repository}/.github/workflows/container.yml@{tag_ref}"
    signer_workflow = f"{owner_repository}/.github/workflows/container.yml"
    image_by_digest = f"{image}@{index_digest}"

    command_runner(
        [
            "cosign",
            "verify-blob",
            "--bundle",
            str(bundle_path),
            "--certificate-identity",
            certificate_identity,
            "--certificate-oidc-issuer",
            "https://token.actions.githubusercontent.com",
            str(receipt_path),
        ]
    )
    command_runner(
        [
            "cosign",
            "verify",
            "--certificate-identity",
            certificate_identity,
            "--certificate-oidc-issuer",
            "https://token.actions.githubusercontent.com",
            image_by_digest,
        ]
    )

    attestation_policy = [
        "--repo",
        owner_repository,
        "--signer-workflow",
        signer_workflow,
        "--signer-digest",
        source_commit,
        "--source-ref",
        tag_ref,
        "--source-digest",
        source_commit,
        "--cert-identity",
        certificate_identity,
        "--cert-oidc-issuer",
        "https://token.actions.githubusercontent.com",
        "--deny-self-hosted-runners",
    ]
    command_runner(["gh", "attestation", "verify", f"oci://{image_by_digest}", *attestation_policy])
    for child_digest in child_digests:
        command_runner(
            [
                "gh",
                "attestation",
                "verify",
                f"oci://{image}@{child_digest}",
                *attestation_policy,
                "--predicate-type",
                "https://spdx.dev/Document/v2.3",
            ]
        )

    semantic_ref = f"{image}:{image_version}"
    manifest = command_runner(["docker", "buildx", "imagetools", "inspect", semantic_ref, "--raw"])
    observed = f"sha256:{hashlib.sha256(manifest).hexdigest()}"
    if observed != index_digest:
        raise ValueError("semantic-version registry tag does not resolve to the signed index digest")


def _atomic_replace_many(rendered: dict[Path, str]) -> int:
    pending: dict[Path, Path] = {}
    changed = {path: text for path, text in rendered.items() if path.read_text(encoding="utf-8") != text}
    try:
        for path, text in changed.items():
            temporary = path.with_name(f".{path.name}.phase15-{os.getpid()}.tmp")
            temporary.write_text(text, encoding="utf-8", newline="\n")
            pending[path] = temporary
        for path, temporary in pending.items():
            os.replace(temporary, path)
    finally:
        for temporary in pending.values():
            temporary.unlink(missing_ok=True)
    return len(changed)


def reset_store_template(root: Path, *, dry_run: bool = False) -> int:
    """Restore the non-deployable release template without touching runtime code."""

    root = root.resolve()
    version = _project_version(root)
    paths = (
        root / "jjlab-hyperlab/docker-compose.yml",
        root / "jjlab-hyperlab/umbrel-app.yml",
    )
    rendered: dict[Path, str] = {}
    for path in paths:
        current = path.read_text(encoding="utf-8")
        updated = current
        if path.name == "docker-compose.yml":
            updated = IMAGE_LINE.sub(lambda match: f"{match.group(1)}{TEMPLATE_IMAGE}", updated)
        else:
            updated = MANIFEST_VERSION.sub(f'version: "{version}"', updated)
            updated = RAW_ICON.sub(TEMPLATE_ICON, updated)
        rendered[path] = updated
    compose = rendered[paths[0]]
    if compose.count(f"image: {TEMPLATE_IMAGE}") != 2:
        raise ValueError("release template must contain two exact image placeholders")
    if TEMPLATE_ICON not in rendered[paths[1]]:
        raise ValueError("release template must contain an immutable-version icon placeholder")
    if dry_run:
        return sum(path.read_text(encoding="utf-8") != text for path, text in rendered.items())
    return _atomic_replace_many(rendered)


def prepare_store(
    root: Path,
    *,
    github_user: str,
    repository: str,
    image_version: str,
    image_digest: str,
    release_receipt: Path,
    receipt_bundle: Path,
    dry_run: bool = False,
    command_runner: CommandRunner | None = None,
) -> int:
    root = root.resolve()
    _validate_inputs(root, github_user, repository, image_version, image_digest)
    _verify_release_evidence(
        github_user=github_user,
        repository=repository,
        image_version=image_version,
        image_digest=image_digest,
        release_receipt=release_receipt,
        receipt_bundle=receipt_bundle,
        command_runner=command_runner or _default_command_runner,
    )
    rendered = _render_files(
        root,
        github_user=github_user,
        repository=repository,
        image_version=image_version,
        image_digest=image_digest,
    )
    if dry_run:
        return sum(path.read_text(encoding="utf-8") != text for path, text in rendered.items())
    return _atomic_replace_many(rendered)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare a digest-pinned Umbrel package.")
    parser.add_argument("github_user", nargs="?", help="Lowercase GitHub account or organization")
    parser.add_argument("--repository", default="hyperlab")
    parser.add_argument("--image-version", "--tag", dest="image_version", default="0.2.1")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--image-digest", help="Multi-architecture manifest digest, 64 hex")
    mode.add_argument(
        "--reset-template",
        action="store_true",
        help="Restore non-deployable placeholders on a release branch.",
    )
    parser.add_argument(
        "--release-receipt",
        type=Path,
        help="Signed workflow release-receipt.json matching the requested digest",
    )
    parser.add_argument(
        "--receipt-bundle",
        type=Path,
        help="Sigstore bundle for release-receipt.json",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    if args.reset_template:
        touched = reset_store_template(root, dry_run=args.dry_run)
        action = "would restore" if args.dry_run else "restored"
        print(f"Umbrel release template {action} in {touched} file(s)")
        print("NON-DEPLOYABLE: publish and verify a new digest before updating the store")
        return
    if (
        args.github_user is None
        or args.image_digest is None
        or args.release_receipt is None
        or args.receipt_bundle is None
    ):
        raise SystemExit(
            "github_user, --image-digest, --release-receipt and --receipt-bundle "
            "are required for a deployable package"
        )
    touched = prepare_store(
        root,
        github_user=args.github_user,
        repository=args.repository,
        image_version=args.image_version,
        image_digest=args.image_digest,
        release_receipt=args.release_receipt,
        receipt_bundle=args.receipt_bundle,
        dry_run=args.dry_run,
    )
    action = "would update" if args.dry_run else "updated"
    print(f"Umbrel package {action} {touched} file(s)")
    print(
        f"Pinned image: ghcr.io/{args.github_user}/{args.repository}:"
        f"{args.image_version}@sha256:{args.image_digest}"
    )


if __name__ == "__main__":
    main()
