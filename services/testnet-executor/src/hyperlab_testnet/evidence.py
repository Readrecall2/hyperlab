"""Canonical, secret-free readiness evidence for the isolated Testnet service."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

from hyperlab.environment_authorization import (
    AUTHORIZATION_MANIFEST_SCHEMA_VERSION,
    EnvironmentAuthorizationReceipt,
    EnvironmentClass,
    EnvironmentReadinessManifest,
    EvidenceCheck,
    ReadinessArtifactBinding,
    ReadinessSubject,
    issue_environment_receipt,
    profile_for,
    testnet_evidence_payload,
    verify_environment_readiness,
)

from .build_identity import validate_runtime_identity
from .canonical import canonical_json
from .config import TestnetConfig
from .validation import load_testnet_software_validation


class TestnetEvidenceError(RuntimeError):
    """The local readiness bundle could not be created or verified exactly."""


@dataclass(frozen=True, slots=True)
class TestnetEvidenceBundle:
    manifest: EnvironmentReadinessManifest
    receipt: EnvironmentAuthorizationReceipt
    manifest_path: Path
    receipt_path: Path
    evidence_root: Path
    validation_report_path: Path
    validation_report_sha256: str


def _write_new_durable(path: Path, payload: bytes) -> None:
    if path.is_symlink():
        raise TestnetEvidenceError(f"refusing symbolic-link output: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.parent.is_symlink():
        raise TestnetEvidenceError(f"refusing symbolic-link parent: {path.parent}")
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        raise TestnetEvidenceError(f"refusing to overwrite readiness artifact: {path}") from None
    except OSError as error:
        raise TestnetEvidenceError(
            f"cannot durably write readiness artifact ({type(error).__name__})"
        ) from None


def write_testnet_readiness_bundle(
    config: TestnetConfig,
    *,
    evidence_root: Path,
    manifest_path: Path,
    receipt_path: Path,
    validation_report_path: Path,
) -> TestnetEvidenceBundle:
    """Write and immediately verify all compiled TESTNET/TESTNET_EXECUTION evidence."""

    if not isinstance(config, TestnetConfig):
        raise TypeError("config must be a TestnetConfig")
    if not all(
        isinstance(path, Path)
        for path in (evidence_root, manifest_path, receipt_path, validation_report_path)
    ):
        raise TypeError(
            "evidence_root, manifest_path, receipt_path, and validation_report_path "
            "must be Path objects"
        )
    if evidence_root.exists() and not evidence_root.is_dir():
        raise TestnetEvidenceError("evidence_root must be a directory")
    if evidence_root.is_symlink() or manifest_path.is_symlink() or receipt_path.is_symlink():
        raise TestnetEvidenceError("symbolic-link readiness paths are refused")

    # A caller cannot mint a receipt for arbitrary hashes. The subject must
    # describe the source and dependency graph that is executing this command.
    validate_runtime_identity(config)
    validation = load_testnet_software_validation(validation_report_path, config)
    validation_report_sha256 = hashlib.sha256(validation_report_path.read_bytes()).hexdigest()

    profile = profile_for(EnvironmentClass.TESTNET)
    subject = ReadinessSubject(**config.to_readiness_subject())
    bindings: dict[EvidenceCheck, ReadinessArtifactBinding] = {}
    for check in sorted(profile.required_checks, key=lambda item: item.value):
        payload = (
            canonical_json(
                testnet_evidence_payload(
                    check,
                    subject,
                    risk_limits=config.risk_limits.to_dict(),
                    validation_id=validation.validation_id,
                    validation_report_sha256=validation_report_sha256,
                )
            ).encode("utf-8")
        )
        relative_path = f"testnet/{check.value.lower()}.json"
        _write_new_durable(evidence_root / Path(relative_path), payload)
        bindings[check] = ReadinessArtifactBinding.from_bytes(relative_path, payload)

    manifest = EnvironmentReadinessManifest(
        schema_version=AUTHORIZATION_MANIFEST_SCHEMA_VERSION,
        environment=profile.environment,
        purpose=profile.purpose,
        environment_identity=profile.environment.value,
        execution_network=profile.execution_network,
        credential_scope=profile.credential_scope,
        order_capability=profile.order_capability,
        subject=subject,
        evidence=bindings,
    )
    _write_new_durable(manifest_path, manifest.canonical_json_bytes())
    decision = verify_environment_readiness(manifest, evidence_root=evidence_root)
    if not decision.ready:
        codes = ",".join(blocker.code for blocker in decision.blockers)
        raise TestnetEvidenceError(f"compiled readiness verification failed closed: {codes}")
    receipt = issue_environment_receipt(decision)
    _write_new_durable(receipt_path, receipt.canonical_json_bytes())
    return TestnetEvidenceBundle(
        manifest=manifest,
        receipt=receipt,
        manifest_path=manifest_path,
        receipt_path=receipt_path,
        evidence_root=evidence_root,
        validation_report_path=validation_report_path,
        validation_report_sha256=validation_report_sha256,
    )


__all__ = [
    "TestnetEvidenceBundle",
    "TestnetEvidenceError",
    "write_testnet_readiness_bundle",
]
