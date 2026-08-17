from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from hyperlab.environment_authorization import (
    EnvironmentClass,
    profile_for,
    verify_environment_readiness,
)

import hyperlab_testnet.authorization as authorization_module
import hyperlab_testnet.evidence as evidence_module
from hyperlab_testnet.authorization import (
    TestnetAuthorizationError as _TestnetAuthorizationError,
)
from hyperlab_testnet.authorization import (
    load_testnet_authorization_receipt,
)
from hyperlab_testnet.build_identity import current_testnet_build_identity
from hyperlab_testnet.config import TestnetConfig as _TestnetConfig
from hyperlab_testnet.evidence import (
    TestnetEvidenceError as _TestnetEvidenceError,
)
from hyperlab_testnet.evidence import (
    write_testnet_readiness_bundle,
)
from hyperlab_testnet.validation import SoftwareValidationReport


@pytest.fixture(autouse=True)
def _synthetic_validation_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    """Validation semantics are exercised independently in test_validation.py."""

    def accepted(*args: object, **kwargs: object) -> SoftwareValidationReport:
        return cast(SoftwareValidationReport, SimpleNamespace(validation_id="f" * 64))

    monkeypatch.setattr(evidence_module, "load_testnet_software_validation", accepted)
    monkeypatch.setattr(authorization_module, "load_testnet_software_validation", accepted)


def _validation_report(tmp_path: Path) -> Path:
    path = tmp_path / "software-validation.json"
    path.write_bytes(b"{}")
    return path


def _config() -> _TestnetConfig:
    identity = current_testnet_build_identity()
    return _TestnetConfig(
        candidate_id="phase13-synthetic",
        account_address="0x" + "11" * 20,
        api_wallet_address="0x" + "22" * 20,
        strategy_name=identity.strategy_name,
        strategy_hash=identity.strategy_hash,
        build_hash=identity.build_hash,
        source_identity=identity.source_identity,
        source_hash=identity.source_hash,
    )


def test_readiness_bundle_is_canonical_complete_and_ready(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    manifest_path = tmp_path / "manifest.json"
    receipt_path = tmp_path / "receipt.json"
    validation_report = _validation_report(tmp_path)

    bundle = write_testnet_readiness_bundle(
        _config(),
        evidence_root=evidence_root,
        manifest_path=manifest_path,
        receipt_path=receipt_path,
        validation_report_path=validation_report,
    )

    profile = profile_for(EnvironmentClass.TESTNET)
    assert bundle.manifest.environment is EnvironmentClass.TESTNET
    assert set(bundle.manifest.evidence) == set(profile.required_checks)
    assert manifest_path.read_bytes() == bundle.manifest.canonical_json_bytes()
    assert receipt_path.read_bytes() == bundle.receipt.canonical_json_bytes()
    assert not bundle.receipt.authorizes_real_money
    assert len(tuple((evidence_root / "testnet").glob("*.json"))) == len(
        profile.required_checks
    )
    assert verify_environment_readiness(
        bundle.manifest,
        evidence_root=evidence_root,
    ).ready


def test_readiness_bundle_refuses_to_overwrite_evidence(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    manifest_path = tmp_path / "manifest.json"
    receipt_path = tmp_path / "receipt.json"
    validation_report = _validation_report(tmp_path)
    write_testnet_readiness_bundle(
        _config(),
        evidence_root=evidence_root,
        manifest_path=manifest_path,
        receipt_path=receipt_path,
        validation_report_path=validation_report,
    )

    with pytest.raises(_TestnetEvidenceError, match="refusing to overwrite"):
        write_testnet_readiness_bundle(
            _config(),
            evidence_root=evidence_root,
            manifest_path=manifest_path,
            receipt_path=receipt_path,
            validation_report_path=validation_report,
        )


def test_receipt_loader_requires_exact_config_scope_and_canonical_bytes(
    tmp_path: Path,
) -> None:
    config = _config()
    evidence_root = tmp_path / "evidence"
    manifest_path = tmp_path / "manifest.json"
    receipt_path = tmp_path / "receipt.json"
    validation_report = _validation_report(tmp_path)
    bundle = write_testnet_readiness_bundle(
        config,
        evidence_root=evidence_root,
        manifest_path=manifest_path,
        receipt_path=receipt_path,
        validation_report_path=validation_report,
    )

    loaded = load_testnet_authorization_receipt(
        receipt_path,
        manifest_path=manifest_path,
        evidence_root=evidence_root,
        validation_report_path=validation_report,
        config=config,
    )
    assert loaded.receipt_sha256 == bundle.receipt.receipt_sha256

    swapped_validation = tmp_path / "swapped-software-validation.json"
    swapped_validation.write_bytes(b'{"different":true}')
    with pytest.raises(_TestnetAuthorizationError, match="different software validation"):
        load_testnet_authorization_receipt(
            receipt_path,
            manifest_path=manifest_path,
            evidence_root=evidence_root,
            validation_report_path=swapped_validation,
            config=config,
        )

    with pytest.raises(_TestnetAuthorizationError, match="subject differs"):
        load_testnet_authorization_receipt(
            receipt_path,
            manifest_path=manifest_path,
            evidence_root=evidence_root,
            validation_report_path=validation_report,
            config=replace(config, candidate_id="different-candidate"),
        )

    noncanonical = tmp_path / "noncanonical-receipt.json"
    noncanonical.write_bytes(bundle.receipt.canonical_json_bytes() + b"\n")
    with pytest.raises(_TestnetAuthorizationError, match="receipt refused"):
        load_testnet_authorization_receipt(
            noncanonical,
            manifest_path=manifest_path,
            evidence_root=evidence_root,
            validation_report_path=validation_report,
            config=config,
        )
