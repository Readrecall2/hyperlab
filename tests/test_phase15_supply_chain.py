from __future__ import annotations

import hashlib
import json
import re
import shutil
from pathlib import Path

import pytest
import yaml

from scripts.prepare_umbrel_store import TEMPLATE_IMAGE, prepare_store, reset_store_template
from scripts.verify_github_tag import verify_tag
from scripts.verify_manifest import sha256_file
from scripts.verify_oci_index import exact_platform_digests
from scripts.verify_release import PINNED_ACTIONS, _packaging_errors, verify_release


def _root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_phase15_release_policy_passes_for_the_repository_template() -> None:
    assert verify_release(_root(), template=True) == []


def test_workflow_actions_are_immutable_and_publish_is_gated() -> None:
    root = _root()
    workflows = "\n".join(
        path.read_text(encoding="utf-8") for path in (root / ".github/workflows").glob("*.yml")
    )
    references = re.findall(r"\buses:\s*([^\s@]+)@([^\s#]+)", workflows)

    assert references
    assert all(
        action.startswith("./") or (re.fullmatch(r"[0-9a-f]{40}", ref) and PINNED_ACTIONS[action] == ref)
        for action, ref in references
    )
    container = (root / ".github/workflows/container.yml").read_text(encoding="utf-8")
    assert isinstance(yaml.safe_load(container), dict)
    assert "needs: [preflight, secret-scan, image-scan]" in container
    assert "environment: signed-release" in container
    assert "load: true" in container
    assert "push: false" in container
    assert container.index("name: Pre-publish vulnerability gate") < container.index(
        "name: Multi-architecture"
    )
    assert "platforms: linux/amd64,linux/arm64" in container
    assert "Verify the published multi-architecture manifest platforms" in container
    assert (
        container.index("name: Block vulnerabilities on the exact published amd64")
        < container.index("name: Block vulnerabilities on the exact published arm64")
        < container.index("name: Generate reviewable SPDX JSON SBOMs")
        < container.index("name: Attach GitHub build provenance")
    )
    assert "trivy-published-amd64.sarif" in container
    assert "trivy-published-arm64.sarif" in container
    assert "sbom-amd64.spdx.json" in container
    assert "sbom-arm64.spdx.json" in container
    assert "actions/attest@" in container
    assert "cosign sign --yes" in container
    assert "--severity HIGH,CRITICAL" in container
    assert "--exit-code 1" in container
    assert "type=raw,value=latest" not in container
    assert "docker/metadata-action@" not in container
    assert "anchore/sbom-action@" not in container
    assert "aquasecurity/trivy-action@" not in container
    assert "labels: ${{ steps.meta.outputs.labels }}" not in container
    assert "candidate-${{ github.run_id }}-${{ github.run_attempt }}" in container
    assert (
        container.index("name: Verify the just-created keyless signature")
        < container.index("name: Sign and verify the digest-bound release receipt")
        < container.index("name: Revalidate the source tag")
        < container.index("name: Promote the verified digest")
    )
    assert "Refuse to overwrite an existing semantic-version image tag" in container
    assert "SOURCE_REF: ${{ github.ref }}" in container
    assert '"refs/tags/$tag"' in container
    assert "REF_TYPE: ${{ github.ref_type }}" in container
    assert "REF_PROTECTED: ${{ github.ref_protected }}" in container
    assert '[[ "$REF_TYPE" != "tag" || "$REF_PROTECTED" != "true" ]]' in container
    assert "python scripts/verify_oci_index.py manifest-index.json" in container
    assert (
        "tonistiigi/binfmt@sha256:400a4873b838d1b89194d982c45e5fb3cda4593fbfd7e08a02e76b03b21166f0"
        in container
    )
    assert (
        "moby/buildkit@sha256:28a898719c18a33f4e8000685287fa36fd0dd9560c6440227d3a732d79bb41d8" in container
    )
    assert "buildx-v0.34.1.linux-amd64" in container
    assert "f1332ddb9010bd0b72628266c3a906d9a6979848033df4c8d9bd2cd113bae12b" in container
    assert "version: v0.34.1" not in container


def test_oci_index_rejects_duplicate_extra_unknown_and_invalid_children() -> None:
    amd64 = "sha256:" + "a" * 64
    arm64 = "sha256:" + "b" * 64
    valid = {
        "manifests": [
            {"digest": amd64, "platform": {"os": "linux", "architecture": "amd64"}},
            {"digest": arm64, "platform": {"os": "linux", "architecture": "arm64"}},
        ]
    }
    assert exact_platform_digests(valid) == {"amd64": amd64, "arm64": arm64}

    duplicate = {"manifests": [valid["manifests"][0], valid["manifests"][0]]}
    with pytest.raises(ValueError, match="duplicate"):
        exact_platform_digests(duplicate)
    with pytest.raises(ValueError, match="exactly two"):
        exact_platform_digests({"manifests": [*valid["manifests"], valid["manifests"][0]]})
    unknown = {
        "manifests": [
            valid["manifests"][0],
            {"digest": arm64, "platform": {"os": "unknown", "architecture": "unknown"}},
        ]
    }
    with pytest.raises(ValueError, match="unexpected"):
        exact_platform_digests(unknown)
    invalid = {
        "manifests": [
            valid["manifests"][0],
            {"digest": "sha256:not-a-digest", "platform": {"os": "linux", "architecture": "arm64"}},
        ]
    }
    with pytest.raises(ValueError, match="invalid child digest"):
        exact_platform_digests(invalid)


def test_github_tag_verifier_handles_lightweight_and_annotated_tags_fail_closed() -> None:
    commit = "a" * 40
    tag_object = "b" * 40

    def lightweight(path: str) -> dict[str, object]:
        assert path == "git/ref/tags/v0.2.1"
        return {"object": {"type": "commit", "sha": commit}}

    verify_tag(lightweight, tag="v0.2.1", expected_commit=commit)

    responses = {
        "git/ref/tags/v0.2.1": {"object": {"type": "tag", "sha": tag_object}},
        f"git/tags/{tag_object}": {"object": {"type": "commit", "sha": commit}},
    }
    verify_tag(responses.__getitem__, tag="v0.2.1", expected_commit=commit)

    with pytest.raises(ValueError, match="moved"):
        verify_tag(lightweight, tag="v0.2.1", expected_commit="c" * 40)


def test_hash_locks_exclude_the_exchange_sdk_and_private_dependency_graph() -> None:
    root = _root()
    combined = "\n".join(
        (root / name).read_text(encoding="utf-8").casefold()
        for name in ("requirements-runtime.lock", "requirements-ci.lock")
    )
    for forbidden in (
        "hyperliquid-python-sdk",
        "eth-account",
        "eth-abi",
        "eth-keyfile",
        "eth-keys",
    ):
        assert forbidden not in combined
    assert "--hash=sha256:" in combined
    assert " @ " not in combined
    runtime = (root / "requirements-runtime.lock").read_text(encoding="utf-8").casefold()
    ci = (root / "requirements-ci.lock").read_text(encoding="utf-8").casefold()
    assert "duckdb==" not in runtime
    assert "duckdb==" in ci


def test_manifest_hash_is_independent_of_checkout_line_endings(tmp_path: Path) -> None:
    lf = tmp_path / "lf.txt"
    crlf = tmp_path / "crlf.txt"
    lf.write_bytes(b"first\nsecond\n")
    crlf.write_bytes(b"first\r\nsecond\r\n")

    assert sha256_file(lf) == sha256_file(crlf)


def _release_evidence(tmp_path: Path) -> tuple[str, Path, Path, list[list[str]], object]:
    manifest = b'{"schemaVersion":2,"manifests":[]}'
    digest = hashlib.sha256(manifest).hexdigest()
    receipt = tmp_path / "release-receipt.json"
    bundle = tmp_path / "release-receipt.sigstore.json"
    receipt.write_text(
        json.dumps(
            {
                "schema": "hyperlab-phase15-release-receipt-v1",
                "image": "ghcr.io/example-owner/hyperlab",
                "version": "0.2.1",
                "release_tag": "v0.2.1",
                "source_commit": "c" * 40,
                "workflow": ".github/workflows/container.yml",
                "index_digest": f"sha256:{digest}",
                "platform_digests": {
                    "linux/amd64": "sha256:" + "a" * 64,
                    "linux/arm64": "sha256:" + "b" * 64,
                },
                "sbom_sha256": {
                    "sbom-amd64.spdx.json": "sha256:" + "d" * 64,
                    "sbom-arm64.spdx.json": "sha256:" + "e" * 64,
                },
                "vulnerability_gate": {
                    "scanner": "trivy",
                    "scanner_version": "0.72.0",
                    "scanner_archive_sha256": "bbb64b9695866ce4a7a8f5c9592002c5961cab378577fa3f8a040df362b9b2ea",
                    "severity": ["HIGH", "CRITICAL"],
                    "ignore_unfixed": False,
                    "result": "pass",
                },
            }
        ),
        encoding="utf-8",
    )
    bundle.write_text("{}\n", encoding="utf-8")
    commands: list[list[str]] = []

    def command_runner(command: list[str]) -> bytes:
        commands.append(command)
        if command[:4] == ["docker", "buildx", "imagetools", "inspect"]:
            return manifest
        return b"verified"

    return digest, receipt, bundle, commands, command_runner


def test_prepare_store_requires_verified_release_and_renders_a_repeatable_package(tmp_path: Path) -> None:
    root = _root()
    shutil.copy2(root / "pyproject.toml", tmp_path / "pyproject.toml")
    shutil.copy2(root / "umbrel-app-store.yml", tmp_path / "umbrel-app-store.yml")
    package = tmp_path / "jjlab-hyperlab"
    package.mkdir()
    shutil.copy2(root / "jjlab-hyperlab/docker-compose.yml", package / "docker-compose.yml")
    shutil.copy2(root / "jjlab-hyperlab/umbrel-app.yml", package / "umbrel-app.yml")
    digest, receipt, bundle, commands, command_runner = _release_evidence(tmp_path)
    evidence = {
        "release_receipt": receipt,
        "receipt_bundle": bundle,
        "command_runner": command_runner,
    }

    with pytest.raises(ValueError, match="exactly 64 lowercase"):
        prepare_store(
            tmp_path,
            github_user="example-owner",
            repository="hyperlab",
            image_version="0.2.1",
            image_digest="abc",
            **evidence,
        )
    with pytest.raises(ValueError, match="lowercase GitHub"):
        prepare_store(
            tmp_path,
            github_user="Example-Owner",
            repository="hyperlab",
            image_version="0.2.1",
            image_digest=digest,
            **evidence,
        )

    touched = prepare_store(
        tmp_path,
        github_user="example-owner",
        repository="hyperlab",
        image_version="0.2.1",
        image_digest=digest,
        **evidence,
    )
    assert touched >= 2
    compose = (package / "docker-compose.yml").read_text(encoding="utf-8")
    manifest = (package / "umbrel-app.yml").read_text(encoding="utf-8")
    image = f"ghcr.io/example-owner/hyperlab:0.2.1@sha256:{digest}"
    assert compose.count(f"image: {image}") == 2
    assert "REPLACE_WITH_" not in compose + manifest
    assert "/v0.2.1/jjlab-hyperlab/icon.svg" in manifest
    assert _packaging_errors(tmp_path, template=False, version="0.2.1") == []
    assert (
        prepare_store(
            tmp_path,
            github_user="example-owner",
            repository="hyperlab",
            image_version="0.2.1",
            image_digest=digest,
            **evidence,
        )
        == 0
    )
    assert [command[0] for command in commands].count("cosign") == 4
    assert [command[0] for command in commands].count("gh") == 6
    assert [command[0] for command in commands].count("docker") == 2
    assert all("candidate-" not in " ".join(command) for command in commands)
    assert any("https://spdx.dev/Document/v2.3" in command for command in commands)
    assert any("ghcr.io/example-owner/hyperlab:0.2.1" in command for command in commands)
    assert reset_store_template(tmp_path) == 2
    reset_compose = (package / "docker-compose.yml").read_text(encoding="utf-8")
    reset_manifest = (package / "umbrel-app.yml").read_text(encoding="utf-8")
    assert reset_compose.count(f"image: {TEMPLATE_IMAGE}") == 2
    assert "vREPLACE_WITH_IMAGE_VERSION/jjlab-hyperlab/icon.svg" in reset_manifest
    assert _packaging_errors(tmp_path, template=True, version="0.2.1") == []


def test_prepare_store_fails_closed_on_receipt_or_promoted_tag_mismatch(tmp_path: Path) -> None:
    root = _root()
    shutil.copy2(root / "pyproject.toml", tmp_path / "pyproject.toml")
    shutil.copy2(root / "umbrel-app-store.yml", tmp_path / "umbrel-app-store.yml")
    shutil.copytree(root / "jjlab-hyperlab", tmp_path / "jjlab-hyperlab")
    digest, receipt, bundle, _, command_runner = _release_evidence(tmp_path)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["vulnerability_gate"]["result"] = "fail"
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="vulnerability gate"):
        prepare_store(
            tmp_path,
            github_user="example-owner",
            repository="hyperlab",
            image_version="0.2.1",
            image_digest=digest,
            release_receipt=receipt,
            receipt_bundle=bundle,
            command_runner=command_runner,
        )

    payload["vulnerability_gate"]["result"] = "pass"
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    def wrong_manifest_runner(command: list[str]) -> bytes:
        if command[:4] == ["docker", "buildx", "imagetools", "inspect"]:
            return b"different promoted index"
        return b"verified"

    with pytest.raises(ValueError, match="semantic-version registry tag"):
        prepare_store(
            tmp_path,
            github_user="example-owner",
            repository="hyperlab",
            image_version="0.2.1",
            image_digest=digest,
            release_receipt=receipt,
            receipt_bundle=bundle,
            command_runner=wrong_manifest_runner,
        )


def test_prepare_store_accepts_valid_lowercase_github_repository_punctuation(tmp_path: Path) -> None:
    root = _root()
    shutil.copy2(root / "pyproject.toml", tmp_path / "pyproject.toml")
    shutil.copy2(root / "umbrel-app-store.yml", tmp_path / "umbrel-app-store.yml")
    shutil.copytree(root / "jjlab-hyperlab", tmp_path / "jjlab-hyperlab")
    digest, receipt, bundle, _, command_runner = _release_evidence(tmp_path)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["image"] = "ghcr.io/example-owner/hyperlab.phase_15"
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    assert (
        prepare_store(
            tmp_path,
            github_user="example-owner",
            repository="hyperlab.phase_15",
            image_version="0.2.1",
            image_digest=digest,
            release_receipt=receipt,
            receipt_bundle=bundle,
            command_runner=command_runner,
        )
        >= 2
    )


def test_secret_files_are_excluded_from_git_and_build_context() -> None:
    root = _root()
    gitignore = (root / ".gitignore").read_text(encoding="utf-8")
    dockerignore = (root / ".dockerignore").read_text(encoding="utf-8")
    for pattern in (
        ".env.*",
        "*.p12",
        "*.pfx",
        "*keystore*",
        "*wallet*",
        "*seed*",
        "*.sqlite",
        "*.sqlite3",
        "*.db",
    ):
        assert pattern in gitignore
        assert pattern in dockerignore


def test_phase15_docs_require_external_backup_before_umbrel_uninstall() -> None:
    root = _root()
    documents = {
        name: (root / name).read_text(encoding="utf-8")
        for name in (
            "README.md",
            "docs/UMBREL_SETUP.md",
            "docs/UMBREL_PACKAGE_NOTES.md",
            "docs/GUIDE_COMPLET_FR.md",
            "docs/GUIDE_COMPLET_FR.html",
            "docs/PHASE15_SECURITY_REVIEW.md",
        )
    }
    assert all("0.2.0" not in text for text in documents.values())
    setup = documents["docs/UMBREL_SETUP.md"]
    for fragment in (
        "ops check-layout",
        "ops backup",
        "ops verify-backup",
        "ops restore",
        "ops export-parquet",
        "hors de `${APP_DATA_DIR}`",
        "supprime `${APP_DATA_DIR}`",
    ):
        assert fragment in setup
    review = documents["docs/PHASE15_SECURITY_REVIEW.md"]
    assert "Aucune conservation automatique" in review
