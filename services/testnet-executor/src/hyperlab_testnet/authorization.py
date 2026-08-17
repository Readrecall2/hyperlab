"""Exact receipt loading and scope validation for TESTNET/TESTNET_EXECUTION."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from hyperlab.environment_authorization import (
    REAL_MONEY_EXECUTION_ENABLED_IN_BUILD,
    AuthorizationPurpose,
    EnvironmentAuthorizationReceipt,
    EnvironmentClass,
    EnvironmentReadinessManifest,
    issue_environment_receipt,
    receipt_scope_blockers,
    verify_environment_readiness,
)

from .config import TestnetConfig
from .validation import SoftwareValidationReport, load_testnet_software_validation

_MAX_RECEIPT_BYTES = 1024 * 1024
_MAX_MANIFEST_BYTES = 1024 * 1024


class TestnetAuthorizationError(RuntimeError):
    """A receipt is unreadable, stale, or outside the exact Testnet scope."""


@dataclass(frozen=True, slots=True)
class TestnetAuthorizationBundle:
    receipt: EnvironmentAuthorizationReceipt
    manifest: EnvironmentReadinessManifest
    validation: SoftwareValidationReport
    validation_report_sha256: str


def _read_regular(path: Path, *, label: str, maximum: int) -> bytes:
    if not isinstance(path, Path):
        raise TypeError(f"{label} path must be a Path")
    if path.is_symlink() or not path.is_file():
        raise TestnetAuthorizationError(f"{label} must be a regular file")
    try:
        if path.stat().st_size > maximum:
            raise TestnetAuthorizationError(f"{label} exceeds the size limit")
        return path.read_bytes()
    except TestnetAuthorizationError:
        raise
    except OSError as error:
        raise TestnetAuthorizationError(
            f"{label} cannot be read ({type(error).__name__})"
        ) from None


def _load_receipt_bytes(
    path: Path,
    *,
    config: TestnetConfig,
) -> tuple[EnvironmentAuthorizationReceipt, bytes]:
    payload = _read_regular(path, label="authorization receipt", maximum=_MAX_RECEIPT_BYTES)
    try:
        receipt = EnvironmentAuthorizationReceipt.from_json_bytes(
            payload,
            require_canonical=True,
        )
    except (TypeError, ValueError) as error:
        raise TestnetAuthorizationError(
            f"authorization receipt refused ({type(error).__name__})"
        ) from None

    blockers = receipt_scope_blockers(
        receipt,
        environment=EnvironmentClass.TESTNET,
        purpose=AuthorizationPurpose.TESTNET_EXECUTION,
        config_hash=config.config_hash,
    )
    if receipt.subject.to_dict() != config.to_readiness_subject():
        raise TestnetAuthorizationError("authorization receipt subject differs from config")
    if blockers:
        codes = ",".join(blocker.code for blocker in blockers)
        raise TestnetAuthorizationError(f"authorization receipt scope refused: {codes}")
    if REAL_MONEY_EXECUTION_ENABLED_IN_BUILD or receipt.authorizes_real_money:
        raise TestnetAuthorizationError("real-money authorization is forbidden in this build")
    return receipt, payload


def _assert_manifest_validation_binding(
    manifest: EnvironmentReadinessManifest,
    *,
    evidence_root: Path,
    validation_id: str,
    validation_report_sha256: str,
) -> None:
    try:
        root = evidence_root.resolve(strict=True)
    except OSError as error:
        raise TestnetAuthorizationError(
            f"authorization evidence root refused ({type(error).__name__})"
        ) from None
    expected = {
        "report_sha256": validation_report_sha256,
        "validation_id": validation_id,
    }
    for binding in manifest.evidence.values():
        try:
            path = (root / binding.relative_path).resolve(strict=True)
            path.relative_to(root)
        except (OSError, ValueError):
            raise TestnetAuthorizationError(
                "authorization validation binding escapes or is unavailable"
            ) from None
        payload = _read_regular(
            path,
            label="semantic authorization artifact",
            maximum=1024 * 1024,
        )
        if hashlib.sha256(payload).hexdigest() != binding.sha256:
            raise TestnetAuthorizationError("semantic authorization artifact hash changed")
        try:
            decoded = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise TestnetAuthorizationError(
                "semantic authorization artifact validation binding is invalid"
            ) from None
        if (
            not isinstance(decoded, Mapping)
            or cast(Mapping[str, object], decoded).get("validation") != expected
        ):
            raise TestnetAuthorizationError(
                "semantic authorization artifact binds a different software validation"
            )


def load_testnet_authorization_bundle(
    receipt_path: Path,
    *,
    manifest_path: Path,
    evidence_root: Path,
    validation_report_path: Path,
    config: TestnetConfig,
) -> TestnetAuthorizationBundle:
    """Reverify validation, semantic evidence, manifest, and receipt as one authority."""

    if not isinstance(config, TestnetConfig):
        raise TypeError("config must be a TestnetConfig")
    validation = load_testnet_software_validation(validation_report_path, config)
    manifest_payload = _read_regular(
        manifest_path,
        label="authorization manifest",
        maximum=_MAX_MANIFEST_BYTES,
    )
    try:
        manifest = EnvironmentReadinessManifest.from_json_bytes(
            manifest_payload,
            require_canonical=True,
        )
    except (TypeError, ValueError) as error:
        raise TestnetAuthorizationError(
            f"authorization manifest refused ({type(error).__name__})"
        ) from None
    if manifest.subject.to_dict() != config.to_readiness_subject():
        raise TestnetAuthorizationError("authorization manifest subject differs from config")
    decision = verify_environment_readiness(manifest, evidence_root=evidence_root)
    if not decision.ready:
        codes = ",".join(blocker.code for blocker in decision.blockers)
        raise TestnetAuthorizationError(f"authorization evidence refused: {codes}")
    validation_payload = _read_regular(
        validation_report_path,
        label="software validation report",
        maximum=8 * 1024 * 1024,
    )
    validation_report_sha256 = hashlib.sha256(validation_payload).hexdigest()
    _assert_manifest_validation_binding(
        manifest,
        evidence_root=evidence_root,
        validation_id=validation.validation_id,
        validation_report_sha256=validation_report_sha256,
    )
    expected = issue_environment_receipt(decision)
    receipt, receipt_payload = _load_receipt_bytes(receipt_path, config=config)
    if (
        receipt_payload != expected.canonical_json_bytes()
        or receipt.receipt_sha256 != expected.receipt_sha256
    ):
        raise TestnetAuthorizationError(
            "authorization receipt does not derive from the supplied verified manifest"
        )
    return TestnetAuthorizationBundle(
        receipt=receipt,
        manifest=manifest,
        validation=validation,
        validation_report_sha256=validation_report_sha256,
    )


def load_testnet_authorization_receipt(
    path: Path,
    *,
    manifest_path: Path,
    evidence_root: Path,
    validation_report_path: Path,
    config: TestnetConfig,
) -> EnvironmentAuthorizationReceipt:
    return load_testnet_authorization_bundle(
        path,
        manifest_path=manifest_path,
        evidence_root=evidence_root,
        validation_report_path=validation_report_path,
        config=config,
    ).receipt


__all__ = [
    "TestnetAuthorizationBundle",
    "TestnetAuthorizationError",
    "load_testnet_authorization_bundle",
    "load_testnet_authorization_receipt",
]
