from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

import yaml

try:
    from verify_manifest import verify_manifest
except ModuleNotFoundError:  # Imported as scripts.verify_release by the test suite.
    from scripts.verify_manifest import verify_manifest

ACTION_REF = re.compile(r"\buses:\s*([^\s@]+)@([^\s#]+)")
FULL_SHA = re.compile(r"[0-9a-f]{40}")
DIGEST = re.compile(r"[0-9a-f]{64}")
SEMVER = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
IMAGE_TEMPLATE = re.compile(
    r"^ghcr\.io/REPLACE_WITH_GITHUB_USER/REPLACE_WITH_REPOSITORY:"
    r"REPLACE_WITH_IMAGE_VERSION@sha256:REPLACE_WITH_IMAGE_DIGEST$"
)
IMAGE_RELEASE = re.compile(
    r"^ghcr\.io/([a-z0-9](?:[a-z0-9-]{0,37}[a-z0-9])?)/"
    r"([a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?):"
    r"([0-9]+\.[0-9]+\.[0-9]+)@sha256:([0-9a-f]{64})$"
)
TEMPLATE_ICON = (
    "https://raw.githubusercontent.com/REPLACE_WITH_GITHUB_USER/"
    "REPLACE_WITH_REPOSITORY/vREPLACE_WITH_IMAGE_VERSION/jjlab-hyperlab/icon.svg"
)
PINNED_ACTIONS = {
    "actions/attest": "508db95dd578ae2727ebd6217d5ba78e4fbda05d",
    "actions/checkout": "3d3c42e5aac5ba805825da76410c181273ba90b1",
    "actions/setup-python": "5fda3b95a4ea91299a34e894583c3862153e4b97",
    "actions/upload-artifact": "043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
    "docker/build-push-action": "53b7df96c91f9c12dcc8a07bcb9ccacbed38856a",
    "docker/login-action": "dbcb813823bdd20940b903addbd779551569679f",
    "docker/setup-buildx-action": "bb05f3f5519dd87d3ba754cc423b652a5edd6d2c",
    "docker/setup-qemu-action": "96fe6ef7f33517b61c61be40b68a1882f3264fb8",
    "github/codeql-action/upload-sarif": "ff2f1c621b7f889edc0d3c761ac2e6a3f8cdb0dd",
    "sigstore/cosign-installer": "6f9f17788090df1f26f669e9d70d6ae9567deba6",
}


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return payload


def _canonical_name(requirement: str) -> str:
    match = re.match(r"[A-Za-z0-9_.-]+", requirement)
    if match is None:
        raise ValueError(f"cannot extract requirement name from {requirement!r}")
    return re.sub(r"[-_.]+", "-", match.group(0)).lower()


def _locked_names(path: Path) -> tuple[set[str], list[str]]:
    text = path.read_text(encoding="utf-8")
    errors: list[str] = []
    names: set[str] = set()
    lines = text.splitlines()
    requirement_indices = [
        index
        for index, line in enumerate(lines)
        if line and not line[0].isspace() and not line.startswith(("#", "--"))
    ]
    for position, index in enumerate(requirement_indices):
        line = lines[index]
        if "==" not in line or " @ " in line or line.startswith("-e "):
            errors.append(f"{path.name}:{index + 1} is not an exact registry pin: {line}")
            continue
        name = _canonical_name(line)
        names.add(name)
        end = requirement_indices[position + 1] if position + 1 < len(requirement_indices) else len(lines)
        block = "\n".join(lines[index:end])
        if "--hash=sha256:" not in block:
            errors.append(f"{path.name}:{index + 1} has no SHA-256 artifact hash")
    if not names:
        errors.append(f"{path.name} contains no locked dependencies")
    return names, errors


def _workflow_errors(root: Path) -> list[str]:
    errors: list[str] = []
    workflows = [root / ".github/workflows/ci.yml", root / ".github/workflows/container.yml"]
    observed_actions: set[str] = set()
    for path in workflows:
        text = path.read_text(encoding="utf-8")
        refs = ACTION_REF.findall(text)
        if not refs:
            errors.append(f"{path} contains no external actions")
        for action, reference in refs:
            if action.startswith("./"):
                continue
            observed_actions.add(action)
            if FULL_SHA.fullmatch(reference) is None:
                errors.append(f"{path}: {action}@{reference} is not pinned to a full commit SHA")
                continue
            expected = PINNED_ACTIONS.get(action)
            if expected is None:
                errors.append(f"{path}: unreviewed external action {action}@{reference}")
            elif reference != expected:
                errors.append(f"{path}: {action} must use reviewed commit {expected}, got {reference}")
    for missing in sorted(set(PINNED_ACTIONS) - observed_actions):
        errors.append(f"reviewed release action is missing from workflows: {missing}")

    ci = workflows[0].read_text(encoding="utf-8")
    container = workflows[1].read_text(encoding="utf-8")
    for fragment in (
        "--require-hashes --requirement requirements-ci.lock",
        "python scripts/verify_release.py --auto --check-manifest",
        "ruff check .",
        "mypy src/hyperlab",
        "pytest --cov=hyperlab",
        "gitleaks_8.30.1_linux_x64.tar.gz",
        "551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb",
        "sha256sum --check --strict",
        "gitleaks git --redact --exit-code 1",
    ):
        if fragment not in ci:
            errors.append(f"CI workflow is missing required gate: {fragment}")
    for fragment in (
        "needs: [preflight, secret-scan, image-scan]",
        "environment: signed-release",
        "Refuse to overwrite an existing semantic-version image tag",
        "candidate-${{ github.run_id }}-${{ github.run_attempt }}",
        "load: true",
        "push: false",
        "name: Pre-publish vulnerability gate (${{ matrix.arch }})",
        "platforms: linux/amd64,linux/arm64",
        "tonistiigi/binfmt@sha256:400a4873b838d1b89194d982c45e5fb3cda4593fbfd7e08a02e76b03b21166f0",
        "buildx-v0.34.1.linux-amd64",
        "f1332ddb9010bd0b72628266c3a906d9a6979848033df4c8d9bd2cd113bae12b",
        "docker buildx version | grep --fixed-strings 'v0.34.1'",
        "moby/buildkit@sha256:28a898719c18a33f4e8000685287fa36fd0dd9560c6440227d3a732d79bb41d8",
        "buildkitd-flags: --debug=false",
        "trivy_0.72.0_Linux-64bit.tar.gz",
        "bbb64b9695866ce4a7a8f5c9592002c5961cab378577fa3f8a040df362b9b2ea",
        "syft_1.51.0_linux_amd64.tar.gz",
        "2a2e837a2c8d59ec9af5472ee22d3b04ee463c4e44476ecf993fd1e5ab6ebc7f",
        "github/codeql-action/upload-sarif@",
        "--severity HIGH,CRITICAL",
        "--exit-code 1",
        "actions/attest@",
        "Verify the published multi-architecture manifest platforms",
        "python scripts/verify_oci_index.py manifest-index.json",
        "Block vulnerabilities on the exact published amd64 child digest",
        "Block vulnerabilities on the exact published arm64 child digest",
        "trivy-published-amd64.sarif",
        "trivy-published-arm64.sarif",
        "Generate reviewable SPDX JSON SBOMs for both exact child digests",
        "sbom-amd64.spdx.json",
        "sbom-arm64.spdx.json",
        "Create a digest-bound release receipt",
        '"schema": "hyperlab-phase15-release-receipt-v1"',
        "cosign sign-blob --yes --bundle release-receipt.sigstore.json",
        "cosign sign --yes",
        "cosign verify",
        "python scripts/verify_github_tag.py",
        "ref: ${{ needs.preflight.outputs.release_sha }}",
        "REF_TYPE: ${{ github.ref_type }}",
        "REF_PROTECTED: ${{ github.ref_protected }}",
        '[[ "$REF_TYPE" != "tag" || "$REF_PROTECTED" != "true" ]]',
        "Promote the verified digest to the immutable semantic-version tag",
        "docker buildx imagetools create --tag",
    ):
        if fragment not in container:
            errors.append(f"container workflow is missing required gate: {fragment}")
    for forbidden in (
        "type=raw,value=latest",
        "docker/metadata-action@",
        "aquasecurity/trivy-action@",
        "anchore/sbom-action@",
        "raw.githubusercontent.com/anchore/syft/main/install.sh",
        "labels: ${{ steps.meta.outputs.labels }}",
        "version: v0.34.1",
    ):
        if forbidden in container:
            errors.append(f"container workflow contains forbidden mutable release input: {forbidden}")
    if container.count("actions/attest@") < 3:
        errors.append("container workflow must attest provenance and both exact-platform SBOMs")
    signing = container.find("name: Verify the just-created keyless signature")
    receipt_signing = container.find("name: Sign and verify the digest-bound release receipt")
    tag_revalidation = container.find("name: Revalidate the source tag")
    promotion = container.find("name: Promote the verified digest")
    if min(signing, receipt_signing, tag_revalidation, promotion) < 0 or not (
        signing < receipt_signing < tag_revalidation < promotion
    ):
        errors.append("semantic-version promotion must follow signature, signed receipt and tag revalidation")
    amd64_scan = container.find("name: Block vulnerabilities on the exact published amd64")
    arm64_scan = container.find("name: Block vulnerabilities on the exact published arm64")
    sbom_generation = container.find("name: Generate reviewable SPDX JSON SBOMs")
    first_attestation = container.find("name: Attach GitHub build provenance")
    if min(amd64_scan, arm64_scan, sbom_generation, first_attestation) < 0 or not (
        amd64_scan < arm64_scan < sbom_generation < first_attestation
    ):
        errors.append("both exact child digests must be scanned before SBOM and attestations")
    candidate_build = container.find(
        "name: Build and push the vulnerability-gated multi-architecture candidate"
    )
    candidate_block = container[candidate_build:amd64_scan]
    if "provenance: false" not in candidate_block or "sbom: false" not in candidate_block:
        errors.append("candidate build must defer provenance and SBOM until after its digest scan")
    index_verifier = (root / "scripts/verify_oci_index.py").read_text(encoding="utf-8")
    for fragment in (
        "len(manifests) != len(REQUIRED_PLATFORMS)",
        "if key in observed:",
        "FULL_DIGEST.fullmatch(digest)",
        'REQUIRED_PLATFORMS = {("linux", "amd64"), ("linux", "arm64")}',
    ):
        if fragment not in index_verifier:
            errors.append(f"OCI index verifier is missing fail-closed invariant: {fragment}")
    dependabot = (root / ".github/dependabot.yml").read_text(encoding="utf-8")
    for ecosystem in ("pip", "github-actions", "docker"):
        if f"package-ecosystem: {ecosystem}" not in dependabot:
            errors.append(f"Dependabot is not monitoring {ecosystem}")
    return errors


def _release_preparation_errors(root: Path) -> list[str]:
    errors: list[str] = []
    preparer = (root / "scripts/prepare_umbrel_store.py").read_text(encoding="utf-8")
    for fragment in (
        '"cosign",\n            "verify-blob"',
        '"cosign",\n            "verify"',
        '"gh", "attestation", "verify"',
        '"--predicate-type",\n                "https://spdx.dev/Document/v2.3"',
        '"--deny-self-hosted-runners"',
        '"--source-digest"',
        '"--source-ref"',
        '"--signer-workflow"',
        '"docker", "buildx", "imagetools", "inspect"',
        '"--release-receipt"',
        '"--receipt-bundle"',
        "semantic-version registry tag does not resolve to the signed index digest",
    ):
        if fragment not in preparer:
            errors.append(f"Umbrel release preparation is missing evidence gate: {fragment}")
    return errors


def _packaging_errors(root: Path, *, template: bool, version: str) -> list[str]:
    errors: list[str] = []
    compose_path = root / "jjlab-hyperlab/docker-compose.yml"
    manifest_path = root / "jjlab-hyperlab/umbrel-app.yml"
    compose_text = compose_path.read_text(encoding="utf-8")
    manifest_text = manifest_path.read_text(encoding="utf-8")
    compose = _load_yaml(compose_path)
    manifest = _load_yaml(manifest_path)

    if str(manifest.get("version")) != version:
        errors.append(f"Umbrel manifest version {manifest.get('version')!r} does not match {version}")
    icon = str(manifest.get("icon", ""))
    if "/main/" in icon:
        errors.append("Umbrel icon must use the immutable release ref, not main")
    if template and icon != TEMPLATE_ICON:
        errors.append("Umbrel template icon must retain the exact release placeholders")

    services = compose.get("services")
    if not isinstance(services, dict):
        return [*errors, "Umbrel compose has no services mapping"]
    images = []
    for service_name in ("dashboard", "collector"):
        service = services.get(service_name)
        if not isinstance(service, dict):
            errors.append(f"Umbrel compose is missing {service_name}")
            continue
        image = service.get("image")
        if not isinstance(image, str):
            errors.append(f"Umbrel {service_name} has no image")
            continue
        images.append(image)
        if template:
            if IMAGE_TEMPLATE.fullmatch(image) is None:
                errors.append(f"Umbrel {service_name} must retain the exact release placeholders")
        else:
            match = IMAGE_RELEASE.fullmatch(image)
            if match is None:
                errors.append(f"Umbrel {service_name} image is not tag+digest pinned: {image}")
            elif match.group(3) != version:
                errors.append(f"Umbrel {service_name} image tag does not match {version}")
    if images and len(set(images)) != 1:
        errors.append("dashboard and collector must use the identical multi-architecture digest")
    if not template and images:
        match = IMAGE_RELEASE.fullmatch(images[0])
        if match is not None:
            expected_icon = (
                f"https://raw.githubusercontent.com/{match.group(1)}/{match.group(2)}/"
                f"v{version}/jjlab-hyperlab/icon.svg"
            )
            if icon != expected_icon:
                errors.append("Umbrel icon does not match the immutable image owner/repository/version")
    if not template and "REPLACE_WITH_" in compose_text + manifest_text:
        errors.append("prepared Umbrel package contains unresolved placeholders")
    return errors


def verify_release(
    root: Path,
    *,
    template: bool,
    tag: str | None = None,
    check_manifest: bool = False,
) -> list[str]:
    errors: list[str] = []
    pyproject = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    version = str(pyproject["project"]["version"])
    if SEMVER.fullmatch(version) is None:
        errors.append(f"project version must be exact semantic version, got {version!r}")
    if tag is not None and tag != f"v{version}":
        errors.append(f"release tag {tag!r} does not match project version v{version}")

    runtime_names, runtime_errors = _locked_names(root / "requirements-runtime.lock")
    ci_names, ci_errors = _locked_names(root / "requirements-ci.lock")
    errors.extend(runtime_errors + ci_errors)
    forbidden_private_graph = {
        "eth-abi",
        "eth-account",
        "eth-keyfile",
        "eth-keys",
        "hyperliquid-python-sdk",
    }
    for forbidden in sorted((runtime_names | ci_names) & forbidden_private_graph):
        errors.append(f"forbidden exchange/private dependency remains locked: {forbidden}")
    if "duckdb" in runtime_names:
        errors.append("runtime lock must exclude optional DuckDB from the multi-architecture service image")
    if "duckdb" not in ci_names:
        errors.append("CI lock must retain optional DuckDB for research and catalog tests")
    runtime_requirements = {_canonical_name(value) for value in pyproject["project"].get("dependencies", [])}
    optional_requirements = {
        _canonical_name(value)
        for group in pyproject["project"].get("optional-dependencies", {}).values()
        for value in group
    }
    for missing in sorted(runtime_requirements - runtime_names):
        errors.append(f"runtime lock is missing direct dependency {missing}")
    for missing in sorted((runtime_requirements | optional_requirements) - ci_names):
        errors.append(f"CI lock is missing direct dependency {missing}")

    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    first_line = dockerfile.splitlines()[0]
    if re.search(r"@sha256:[0-9a-f]{64}\s+AS\s+runtime$", first_line) is None:
        errors.append("Dockerfile base image must be pinned by a 64-hex SHA-256 digest")
    for fragment in (
        "COPY requirements-runtime.lock",
        "--require-hashes --requirement requirements-runtime.lock",
        "PYTHONPATH=/app/src",
        'ENTRYPOINT ["python", "-m", "hyperlab.cli"]',
    ):
        if fragment not in dockerfile:
            errors.append(f"Dockerfile is missing reproducibility boundary: {fragment}")
    if "pip install ." in dockerfile or "pip install --upgrade" in dockerfile:
        errors.append("Dockerfile must not resolve the local package or upgrade pip during the build")
    if f'org.opencontainers.image.version="{version}"' not in dockerfile:
        errors.append("Dockerfile OCI version label does not match the project version")

    package_init = (root / "src/hyperlab/__init__.py").read_text(encoding="utf-8")
    if re.search(rf'^__version__\s*=\s*"{re.escape(version)}"\s*$', package_init, re.MULTILINE) is None:
        errors.append("Python package __version__ does not match the project version")

    config_text = (root / "config/research.toml").read_text(encoding="utf-8").casefold()
    for forbidden in ("userfees", "account-specific rates", "private endpoint"):
        if forbidden in config_text:
            errors.append(f"research config references forbidden private fee input: {forbidden}")

    errors.extend(_workflow_errors(root))
    errors.extend(_release_preparation_errors(root))
    errors.extend(_packaging_errors(root, template=template, version=version))
    security_review = (root / "docs/PHASE15_SECURITY_REVIEW.md").read_text(encoding="utf-8")
    for fragment in (
        "Phase 10",
        "Phase 11",
        "Phase 12",
        "Phase 13",
        "Phase 14",
        "BLOCKED",
        "rollback",
        "Cosign",
        "SBOM",
    ):
        if fragment not in security_review:
            errors.append(f"Phase 15 security review is missing required evidence: {fragment}")
    if check_manifest:
        errors.extend(verify_manifest(root))
    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fail closed on Phase 15 release-policy violations.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--template", action="store_true", help="Validate repository placeholders.")
    mode.add_argument("--prepared", action="store_true", help="Validate a deployable Umbrel package.")
    mode.add_argument(
        "--auto",
        action="store_true",
        help="Accept a source template or a fully digest-pinned store, never a mixed state.",
    )
    parser.add_argument("--tag", help="Require an exact v-prefixed tag matching the package version.")
    parser.add_argument("--check-manifest", action="store_true")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    template = args.template
    if args.auto:
        compose = (args.root.resolve() / "jjlab-hyperlab/docker-compose.yml").read_text(encoding="utf-8")
        template = "REPLACE_WITH_" in compose
    errors = verify_release(
        args.root.resolve(),
        template=template,
        tag=args.tag,
        check_manifest=args.check_manifest,
    )
    if errors:
        print("Phase 15 release verification FAILED:", file=sys.stderr)
        print("\n".join(f"- {error}" for error in errors), file=sys.stderr)
        return 1
    print("Phase 15 release verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
