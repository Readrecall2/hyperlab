"""Synthetic-only tests for canonical local Testnet software validation evidence."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import zipfile
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import hyperlab_testnet.validation as validation
from hyperlab_testnet.build_identity import TestnetBuildIdentity
from hyperlab_testnet.config import TestnetConfig
from hyperlab_testnet.validation import (
    PHASE13_BASELINE_BRANCH,
    PHASE13_BASELINE_HEAD,
    PytestCounts,
    SoftwareValidationError,
    SoftwareValidationReport,
    ValidationCheckRecord,
    load_testnet_software_validation,
)

_REAL_BUILD_ADDITIONAL_ARTIFACTS = validation._build_additional_artifacts

TestnetBuildIdentity.__test__ = False  # type: ignore[attr-defined]
TestnetConfig.__test__ = False  # type: ignore[attr-defined]

NOW = datetime(2026, 8, 17, 12, 0, tzinfo=UTC)
CURRENT_HEAD = "1" * 40


@dataclass(frozen=True)
class _Bundle:
    root: Path
    output: Path
    report_path: Path
    config: TestnetConfig
    identity: TestnetBuildIdentity
    report: SoftwareValidationReport


def _config(identity: TestnetBuildIdentity) -> TestnetConfig:
    return TestnetConfig(
        candidate_id="synthetic-validation",
        account_address="0x" + "1" * 40,
        api_wallet_address="0x" + "2" * 40,
        strategy_name=identity.strategy_name,
        strategy_hash=identity.strategy_hash,
        build_hash=identity.build_hash,
        source_identity=identity.source_identity,
        source_hash=identity.source_hash,
    )


def _write_wheel(
    path: Path,
    *,
    distribution: str,
    version: str,
    requirements: tuple[str, ...] = (),
) -> None:
    metadata = [
        "Metadata-Version: 2.4",
        f"Name: {distribution}",
        f"Version: {version}",
        *(f"Requires-Dist: {item}" for item in requirements),
        "",
        "",
    ]
    dist_info = distribution.replace("-", "_")
    with zipfile.ZipFile(path, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            f"{dist_info}-{version}.dist-info/METADATA",
            chr(10).join(metadata).encode("utf-8"),
        )


def _write_build_provenance(
    bundle: _Bundle,
    *,
    service_requirements: tuple[str, ...] = (),
) -> Path:
    build_output = bundle.output / "runtime" / "build-isolation"
    wheels = build_output / "wheels"
    wheels.mkdir(parents=True, exist_ok=True)
    root_wheel = wheels / "hyperlab-0.2.1-py3-none-any.whl"
    service_wheel = wheels / "hyperlab_testnet_executor-0.3.0.dev0-py3-none-any.whl"
    _write_wheel(root_wheel, distribution="hyperlab", version="0.2.1")
    _write_wheel(
        service_wheel,
        distribution="hyperlab-testnet-executor",
        version="0.3.0.dev0",
        requirements=service_requirements,
    )
    wheelhouse = bundle.root / validation._WHEELHOUSE_RELATIVE
    wheelhouse.mkdir(parents=True)
    _write_wheel(
        wheelhouse / "synthetic_dependency-1.0-py3-none-any.whl",
        distribution="synthetic-dependency",
        version="1.0",
    )
    smoke_prefix = build_output / "smoke-env"
    root_origin = smoke_prefix / "Lib" / "site-packages" / "hyperlab" / "__init__.py"
    service_origin = (
        smoke_prefix / "Lib" / "site-packages" / "hyperlab_testnet" / "__init__.py"
    )
    root_origin.parent.mkdir(parents=True)
    service_origin.parent.mkdir(parents=True)
    root_origin.write_text("__version__ = '0.2.1'", encoding="utf-8")
    service_origin.write_text("__version__ = '0.3.0.dev0'", encoding="utf-8")
    import_path = build_output / "import-verification.json"
    import_path.write_text(
        validation.canonical_json(
            {
                "distributions": {
                    "hyperlab": {
                        "origin": str(root_origin.resolve()),
                        "version": "0.2.1",
                    },
                    "hyperlab-testnet-executor": {
                        "origin": str(service_origin.resolve()),
                        "version": "0.3.0.dev0",
                    },
                },
                "python_prefix": str(smoke_prefix.resolve()),
                "schema_version": 1,
            }
        ),
        encoding="utf-8",
    )
    root_record = validation._wheel_record(root_wheel, output=build_output)
    service_record = validation._wheel_record(service_wheel, output=build_output)
    service = bundle.root / "services" / "testnet-executor"
    operation_specs = (
        *validation._prebuild_operation_specs(
            bundle.root,
            service,
            build_output,
            wheelhouse,
        ),
        *validation._postbuild_operation_specs(
            bundle.root,
            service,
            build_output,
            wheelhouse,
            root_wheel,
            service_wheel,
        ),
    )
    import_size, import_sha256 = validation._sha256_file(import_path)
    payload = {
        "build_lock_sha256": "e" * 64,
        "external_lock_sha256": "d" * 64,
        "import_verification": {
            "relative_path": import_path.relative_to(build_output).as_posix(),
            "sha256": import_sha256,
            "size_bytes": import_size,
        },
        "operations": [
            {
                "argv": list(argv),
                "cwd": str(cwd),
                "exit_code": 0,
                "operation_id": operation_id,
            }
            for operation_id, argv, cwd in operation_specs
        ],
        "root_wheel": root_record,
        "schema_version": 2,
        "service_has_hyperlab_dependency": False,
        "service_wheel": service_record,
        "wheelhouse": {
            "relative_path": validation._WHEELHOUSE_RELATIVE.as_posix(),
            "wheels": list(validation._wheelhouse_inventory(wheelhouse)),
        },
    }
    provenance = build_output / "build-provenance.json"
    provenance.write_text(validation.canonical_json(payload), encoding="utf-8")
    return provenance


@pytest.fixture
def valid_bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Bundle:
    root = tmp_path / "repo"
    service = root / "services" / "testnet-executor"
    output = service / "evidence" / "synthetic-run"
    for path in (
        root / ".git",
        service,
        output / "artifacts",
        output / "runtime" / "home",
        output / "runtime" / "tmp",
        output / "runtime" / "cache",
        output / "runtime" / "pytest-basetemp",
    ):
        path.mkdir(parents=True, exist_ok=True)
    (service / "pyproject.toml").write_text("[project]\nname='synthetic'\n", encoding="utf-8")

    identity = TestnetBuildIdentity(
        build_hash="a" * 64,
        source_hash="b" * 64,
        strategy_hash="c" * 64,
    )
    config = _config(identity)
    monkeypatch.setattr(validation, "current_testnet_build_identity", lambda: identity)
    monkeypatch.setattr(validation, "external_lock_sha256", lambda: "d" * 64)
    monkeypatch.setattr(validation, "build_lock_sha256", lambda: "e" * 64)
    monkeypatch.setattr(
        validation,
        "_repository_identity",
        lambda repository, git, env: (CURRENT_HEAD, PHASE13_BASELINE_BRANCH),
    )
    monkeypatch.setattr(validation, "_baseline_is_ancestor", lambda *args: True)
    monkeypatch.setattr(validation, "_assert_output_ignored", lambda *args: None)
    monkeypatch.setattr(
        validation,
        "_worktree_inventory_sha256",
        lambda *args: "6" * 64,
    )
    monkeypatch.setattr(validation, "_executable_version", lambda executable, env: "synthetic-version")

    python = Path(validation.sys.executable).resolve()
    monkeypatch.setattr(validation, "_repository_gate_python", lambda repository: python)
    git = validation._resolve_executable("git")
    specs = validation._expected_check_specs(root, output, python, python, git)
    executable_hashes = {
        executable: validation._sha256_file(executable)[1]
        for executable in {Path(spec.argv[0]) for spec in specs}
    }
    build_extra_path = output / "runtime" / "build-isolation" / "synthetic-provenance.json"
    build_extra_path.parent.mkdir(parents=True)
    build_extra_path.write_bytes(b"synthetic-build-provenance")
    build_extra = validation._artifact(
        output,
        "runtime/build-isolation/synthetic-provenance.json",
    )
    monkeypatch.setattr(
        validation,
        "_build_additional_artifacts",
        lambda repository_root, output_root, relative: (build_extra,),
    )

    records: list[ValidationCheckRecord] = []
    for spec in specs:
        stdout_path = output / spec.stdout_relative
        stderr_path = output / spec.stderr_relative
        stdout_path.write_text(f"synthetic {spec.check_id} pass\n", encoding="utf-8")
        stderr_path.write_bytes(b"")
        junit = None
        audit = None
        counts = None
        if spec.junit_relative is not None and spec.pytest_audit_relative is not None:
            (output / spec.junit_relative).write_text(
                '<testsuites><testsuite tests="1" failures="0" errors="0" '
                'skipped="0"><testcase classname="synthetic" name="pass" />'
                '</testsuite></testsuites>',
                encoding="utf-8",
            )
            (output / spec.pytest_audit_relative).write_text(
                '{"deselected":0,"external_network_attempts":0,"xfailed":0,"xpassed":0}',
                encoding="utf-8",
            )
            junit = validation._artifact(output, spec.junit_relative)
            audit = validation._artifact(output, spec.pytest_audit_relative)
            counts = PytestCounts(1, 0, 0, 0, 0, 0, 0, 0)
        executable = Path(spec.argv[0])
        records.append(
            ValidationCheckRecord(
                check_id=spec.check_id,
                argv=spec.argv,
                cwd=str(spec.cwd),
                executable_path=str(executable),
                executable_sha256=executable_hashes[executable],
                executable_version="synthetic-version",
                started_at=NOW,
                ended_at=NOW,
                exit_code=0,
                stdout=validation._artifact(output, spec.stdout_relative),
                stderr=validation._artifact(output, spec.stderr_relative),
                junit=junit,
                pytest_audit=audit,
                pytest_counts=counts,
                additional_artifacts=(
                    (build_extra,)
                    if spec.check_id == "DEPENDENCY_BUILD_ISOLATION"
                    else ()
                ),
                passed=True,
            )
        )
    report = SoftwareValidationReport(
        validation_id=validation.canonical_sha256(
            {
                "build_hash": identity.build_hash,
                "config_hash": config.config_hash,
                "created_at": validation.utc_text(NOW),
                "repository_head": CURRENT_HEAD,
            }
        ),
        created_at=NOW,
        repository_root=str(root.resolve()),
        output_root=str(output.resolve()),
        baseline_head=PHASE13_BASELINE_HEAD,
        repository_head=CURRENT_HEAD,
        branch=PHASE13_BASELINE_BRANCH,
        config_subject=config.to_readiness_subject(),
        build_identity_before=identity.to_dict(),
        build_identity_after=identity.to_dict(),
        worktree_inventory_before_sha256="6" * 64,
        worktree_inventory_after_sha256="6" * 64,
        external_lock_sha256="d" * 64,
        build_lock_sha256="e" * 64,
        python_identity=validation._python_identity(python),
        checks=tuple(records),
        passed=True,
    )
    report_path = output / validation.REPORT_NAME
    validation._write_report(report, report_path)
    return _Bundle(root, output, report_path, config, identity, report)


def test_strict_loader_accepts_exact_current_report(valid_bundle: _Bundle) -> None:
    loaded = load_testnet_software_validation(
        valid_bundle.report_path,
        valid_bundle.config,
        repository_root=valid_bundle.root,
    )
    assert loaded == valid_bundle.report


def test_strict_loader_defaults_to_report_repository_root(
    valid_bundle: _Bundle,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unrelated = tmp_path / "unrelated-working-directory"
    unrelated.mkdir()
    monkeypatch.chdir(unrelated)

    loaded = load_testnet_software_validation(
        valid_bundle.report_path,
        valid_bundle.config,
    )

    assert loaded == valid_bundle.report
    assert loaded.repository_head == CURRENT_HEAD
    assert loaded.baseline_head == PHASE13_BASELINE_HEAD
    assert tuple(check.check_id for check in loaded.checks) == validation.CHECK_IDS


def test_gate_interpreter_mutation_fails_closed(
    valid_bundle: _Bundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        validation,
        "_repository_gate_python",
        lambda repository: validation._resolve_executable("git"),
    )

    with pytest.raises(SoftwareValidationError, match="compiled check differs"):
        load_testnet_software_validation(
            valid_bundle.report_path,
            valid_bundle.config,
            repository_root=valid_bundle.root,
        )


def test_repository_gate_interpreter_must_be_regular(tmp_path: Path) -> None:
    relative = (
        Path(".venv") / "Scripts" / "python.exe"
        if validation.os.name == "nt"
        else Path(".venv") / "bin" / "python"
    )
    candidate = tmp_path / relative
    candidate.mkdir(parents=True)

    with pytest.raises(SoftwareValidationError, match="non-regular"):
        validation._repository_gate_python(tmp_path)


def test_missing_artifact_fails_closed(valid_bundle: _Bundle) -> None:
    artifact = valid_bundle.output / valid_bundle.report.checks[0].stdout.relative_path
    artifact.unlink()
    with pytest.raises(SoftwareValidationError, match="missing"):
        load_testnet_software_validation(
            valid_bundle.report_path,
            valid_bundle.config,
            repository_root=valid_bundle.root,
        )


def test_modified_artifact_fails_closed(valid_bundle: _Bundle) -> None:
    artifact = valid_bundle.output / valid_bundle.report.checks[0].stdout.relative_path
    with artifact.open("ab") as stream:
        stream.write(b"modified")
    with pytest.raises(SoftwareValidationError, match="changed"):
        load_testnet_software_validation(
            valid_bundle.report_path,
            valid_bundle.config,
            repository_root=valid_bundle.root,
        )


def test_fake_artifact_record_fails_closed(valid_bundle: _Bundle) -> None:
    first = valid_bundle.report.checks[0]
    fake_stdout = replace(first.stdout, sha256="0" * 64)
    fake_check = replace(first, stdout=fake_stdout)
    fake_report = replace(
        valid_bundle.report,
        checks=(fake_check, *valid_bundle.report.checks[1:]),
    )
    valid_bundle.report_path.unlink()
    validation._write_report(fake_report, valid_bundle.report_path)
    with pytest.raises(SoftwareValidationError, match="changed"):
        load_testnet_software_validation(
            valid_bundle.report_path,
            valid_bundle.config,
            repository_root=valid_bundle.root,
        )


def test_modified_or_fake_report_fails_closed(valid_bundle: _Bundle) -> None:
    with valid_bundle.report_path.open("ab") as stream:
        stream.write(b"\n")
    with pytest.raises(SoftwareValidationError, match="canonical"):
        load_testnet_software_validation(
            valid_bundle.report_path,
            valid_bundle.config,
            repository_root=valid_bundle.root,
        )


def test_fake_report_hash_fails_closed(valid_bundle: _Bundle) -> None:
    envelope = json.loads(valid_bundle.report_path.read_text(encoding="utf-8"))
    envelope["report_sha256"] = "0" * 64
    valid_bundle.report_path.write_text(
        validation.canonical_json(envelope),
        encoding="utf-8",
    )
    with pytest.raises(SoftwareValidationError, match="report hash differs"):
        load_testnet_software_validation(
            valid_bundle.report_path,
            valid_bundle.config,
            repository_root=valid_bundle.root,
        )


def test_missing_report_fails_closed(valid_bundle: _Bundle) -> None:
    valid_bundle.report_path.unlink()
    with pytest.raises(SoftwareValidationError, match="missing"):
        load_testnet_software_validation(
            valid_bundle.report_path,
            valid_bundle.config,
            repository_root=valid_bundle.root,
        )


def test_fake_validation_id_is_rejected(valid_bundle: _Bundle) -> None:
    with pytest.raises(SoftwareValidationError, match="validation_id"):
        replace(valid_bundle.report, validation_id="0" * 64)


def test_baseline_must_be_ancestor_of_resulting_head(
    valid_bundle: _Bundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(validation, "_baseline_is_ancestor", lambda *args: False)
    with pytest.raises(SoftwareValidationError, match="baseline HEAD or branch"):
        load_testnet_software_validation(
            valid_bundle.report_path,
            valid_bundle.config,
            repository_root=valid_bundle.root,
        )


def test_current_worktree_inventory_mismatch_fails_closed(
    valid_bundle: _Bundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        validation,
        "_worktree_inventory_sha256",
        lambda *args: "7" * 64,
    )
    with pytest.raises(SoftwareValidationError, match="worktree inventory"):
        load_testnet_software_validation(
            valid_bundle.report_path,
            valid_bundle.config,
            repository_root=valid_bundle.root,
        )


def test_current_source_mismatch_fails_closed(
    valid_bundle: _Bundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    changed = replace(valid_bundle.identity, build_hash="9" * 64)
    monkeypatch.setattr(validation, "current_testnet_build_identity", lambda: changed)
    with pytest.raises(SoftwareValidationError, match="build identity"):
        load_testnet_software_validation(
            valid_bundle.report_path,
            valid_bundle.config,
            repository_root=valid_bundle.root,
        )


def test_sanitized_environment_drops_secrets_and_poisons_network(
    valid_bundle: _Bundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HYPERLAB_TESTNET_PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setenv("GITHUB_TOKEN", "synthetic-secret")
    monkeypatch.setenv("HTTPS_PROXY", "http://real-proxy.invalid")
    environment = validation._sanitized_environment(
        repository_root=valid_bundle.root,
        output_root=valid_bundle.output,
        create_directories=False,
    )
    assert "HYPERLAB_TESTNET_PRIVATE_KEY" not in environment
    assert "GITHUB_TOKEN" not in environment
    assert environment["HTTPS_PROXY"] == "http://127.0.0.1:9"
    assert environment["PIP_NO_INDEX"] == "1"


def test_sanitized_environment_manages_pytest_basetemp_parent(
    valid_bundle: _Bundle,
) -> None:
    parent = valid_bundle.output / "runtime" / "pytest-basetemp"
    parent.rmdir()

    validation._sanitized_environment(
        repository_root=valid_bundle.root,
        output_root=valid_bundle.output,
    )
    assert parent.is_dir()
    assert not parent.is_symlink()

    parent.rmdir()
    with pytest.raises(SoftwareValidationError, match="runtime directory"):
        validation._sanitized_environment(
            repository_root=valid_bundle.root,
            output_root=valid_bundle.output,
            create_directories=False,
        )


def test_validation_network_audit_allows_loopback_and_rejects_external() -> None:
    previous_attempts = validation._PYTEST_EXTERNAL_NETWORK_ATTEMPTS
    try:
        validation._PYTEST_EXTERNAL_NETWORK_ATTEMPTS = 0
        for host in ("localhost", "localhost.", "127.0.0.1", "127.42.0.9", "::1"):
            validation._pytest_network_audit_hook(
                "socket.getaddrinfo",
                (host, 8080),
            )
        validation._pytest_network_audit_hook(
            "socket.connect",
            (object(), ("127.0.0.1", 8080)),
        )

        with pytest.raises(RuntimeError, match="forbids external network"):
            validation._pytest_network_audit_hook(
                "socket.getaddrinfo",
                ("example.invalid", 443),
            )
        assert validation._PYTEST_EXTERNAL_NETWORK_ATTEMPTS == 1
    finally:
        validation._PYTEST_EXTERNAL_NETWORK_ATTEMPTS = previous_attempts


def test_pytest_audit_artifact_refuses_external_network_attempt(
    tmp_path: Path,
) -> None:
    junit = tmp_path / "junit.xml"
    audit = tmp_path / "audit.json"
    junit.write_text(
        '<testsuites><testsuite tests="1" failures="0" errors="0" '
        'skipped="0"><testcase classname="synthetic" name="pass" />'
        '</testsuite></testsuites>',
        encoding="utf-8",
    )
    audit.write_text(
        validation.canonical_json(
            {
                "deselected": 0,
                "external_network_attempts": 1,
                "xfailed": 0,
                "xpassed": 0,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SoftwareValidationError, match="external network"):
        validation._parse_pytest_counts(junit, audit)


def test_pytest_counts_accepts_direct_testsuite_root(tmp_path: Path) -> None:
    junit = tmp_path / "junit.xml"
    audit = tmp_path / "audit.json"
    junit.write_text(
        '<testsuite tests="2" failures="0" errors="0" skipped="0">'
        '<testcase classname="synthetic" name="one" />'
        '<testcase classname="synthetic" name="two" />'
        '</testsuite>',
        encoding="utf-8",
    )
    audit.write_text(
        '{"deselected":0,"external_network_attempts":0,"xfailed":0,"xpassed":0}',
        encoding="utf-8",
    )

    assert validation._parse_pytest_counts(junit, audit) == PytestCounts(
        2,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    )


@pytest.mark.parametrize(
    "junit_xml",
    (
        "<testsuites></testsuites>",
        (
            '<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0" />'
            '<testsuite tests="1" failures="0" errors="0" skipped="0" /></testsuites>'
        ),
    ),
)
def test_pytest_counts_refuses_missing_or_multiple_direct_suites(
    tmp_path: Path,
    junit_xml: str,
) -> None:
    junit = tmp_path / "junit.xml"
    audit = tmp_path / "audit.json"
    junit.write_text(junit_xml, encoding="utf-8")
    audit.write_text(
        '{"deselected":0,"external_network_attempts":0,"xfailed":0,"xpassed":0}',
        encoding="utf-8",
    )

    with pytest.raises(SoftwareValidationError, match="exactly one direct testsuite"):
        validation._parse_pytest_counts(junit, audit)


def _write_clean_pytest_audit(path: Path) -> None:
    path.write_text(
        '{"deselected":0,"external_network_attempts":0,"xfailed":0,"xpassed":0}',
        encoding="utf-8",
    )


def test_pytest_counts_accepts_real_wrapper_and_matching_wrapper_counts(
    tmp_path: Path,
) -> None:
    junit = tmp_path / "junit.xml"
    audit = tmp_path / "audit.json"
    junit.write_text(
        '<testsuites name="pytest tests" tests="2" failures="0" errors="0" '
        'skipped="0"><testsuite name="pytest" errors="0" failures="0" '
        'skipped="0" tests="2" time="0.001" timestamp="2026-08-17T00:00:00Z" '
        'hostname="synthetic"><testcase classname="synthetic" name="one" '
        'time="0.000" /><testcase classname="synthetic" name="two" '
        'time="0.000" /></testsuite></testsuites>',
        encoding="utf-8",
    )
    _write_clean_pytest_audit(audit)

    assert validation._parse_pytest_counts(junit, audit) == PytestCounts(
        2,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    )


@pytest.mark.parametrize(
    "wrapper",
    (
        'tests="1"',
        'tests="2" failures="0" errors="0" skipped="0"',
    ),
)
def test_pytest_counts_refuses_partial_or_divergent_wrapper_counts(
    tmp_path: Path,
    wrapper: str,
) -> None:
    junit = tmp_path / "junit.xml"
    audit = tmp_path / "audit.json"
    junit.write_text(
        f'<testsuites {wrapper}><testsuite tests="1" failures="0" errors="0" '
        'skipped="0"><testcase classname="synthetic" name="pass" />'
        '</testsuite></testsuites>',
        encoding="utf-8",
    )
    _write_clean_pytest_audit(audit)

    with pytest.raises(SoftwareValidationError, match="wrapper counts"):
        validation._parse_pytest_counts(junit, audit)


@pytest.mark.parametrize("raw", ("01", "+1", "\u0661", "1000001"))
def test_pytest_counts_refuses_noncanonical_or_unbounded_ascii_counts(
    tmp_path: Path,
    raw: str,
) -> None:
    junit = tmp_path / "junit.xml"
    audit = tmp_path / "audit.json"
    junit.write_text(
        f'<testsuite tests="{raw}" failures="0" errors="0" skipped="0">'
        '<testcase classname="synthetic" name="pass" /></testsuite>',
        encoding="utf-8",
    )
    _write_clean_pytest_audit(audit)

    with pytest.raises(SoftwareValidationError, match=r"noncanonical|exceeds policy"):
        validation._parse_pytest_counts(junit, audit)


@pytest.mark.parametrize(
    ("junit_xml", "message"),
    (
        (
            '<testsuite tests="1" failures="1" errors="1" skipped="0">'
            '<testcase classname="synthetic" name="bad"><failure /><error />'
            '</testcase></testsuite>',
            "outcome counts are inconsistent",
        ),
        (
            '<testsuite tests="2" failures="0" errors="0" skipped="0">'
            '<testcase classname="synthetic" name="one" /></testsuite>',
            "testcase count differs",
        ),
        (
            '<testsuite tests="1" failures="0" errors="0" skipped="0">'
            '<testcase classname="synthetic" name="bad"><failure />'
            '</testcase></testsuite>',
            "testcase outcomes differ",
        ),
    ),
)
def test_pytest_counts_refuses_inconsistent_outcomes_and_testcases(
    tmp_path: Path,
    junit_xml: str,
    message: str,
) -> None:
    junit = tmp_path / "junit.xml"
    audit = tmp_path / "audit.json"
    junit.write_text(junit_xml, encoding="utf-8")
    _write_clean_pytest_audit(audit)

    with pytest.raises(SoftwareValidationError, match=message):
        validation._parse_pytest_counts(junit, audit)


def test_pytest_counts_refuses_malformed_junit(tmp_path: Path) -> None:
    junit = tmp_path / "junit.xml"
    audit = tmp_path / "audit.json"
    junit.write_text("<testsuites><testsuite>", encoding="utf-8")
    _write_clean_pytest_audit(audit)

    with pytest.raises(SoftwareValidationError, match="malformed"):
        validation._parse_pytest_counts(junit, audit)


def test_pytest_counts_refuses_oversized_junit_before_xml_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    junit = tmp_path / "junit.xml"
    audit = tmp_path / "audit.json"
    with junit.open("wb") as stream:
        stream.seek(validation._MAX_JUNIT_BYTES)
        stream.write(b"x")
    _write_clean_pytest_audit(audit)
    monkeypatch.setattr(
        validation.ElementTree,
        "fromstring",
        lambda _: pytest.fail("oversized JUnit reached XML parsing"),
    )

    with pytest.raises(SoftwareValidationError, match="oversized"):
        validation._parse_pytest_counts(junit, audit)


def test_pytest_counts_refuses_oversized_audit_before_json_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    junit = tmp_path / "junit.xml"
    audit = tmp_path / "audit.json"
    junit.write_text(
        '<testsuite tests="1" failures="0" errors="0" skipped="0">'
        '<testcase classname="synthetic" name="pass" /></testsuite>',
        encoding="utf-8",
    )
    with audit.open("wb") as stream:
        stream.seek(validation._MAX_PYTEST_AUDIT_BYTES)
        stream.write(b"x")
    monkeypatch.setattr(
        validation,
        "_strict_json",
        lambda *_args, **_kwargs: pytest.fail("oversized audit reached JSON parsing"),
    )

    with pytest.raises(SoftwareValidationError, match="pytest audit is oversized"):
        validation._parse_pytest_counts(junit, audit)


def test_pytest_counts_refuses_non_regular_junit(tmp_path: Path) -> None:
    audit = tmp_path / "audit.json"
    _write_clean_pytest_audit(audit)
    directory = tmp_path / "junit-directory"
    directory.mkdir()
    with pytest.raises(SoftwareValidationError, match="regular non-symlink"):
        validation._parse_pytest_counts(directory, audit)


def test_pytest_counts_refuses_symlink_junit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit = tmp_path / "audit.json"
    _write_clean_pytest_audit(audit)
    target = tmp_path / "target.xml"
    target.write_text(
        '<testsuite tests="1" failures="0" errors="0" skipped="0">'
        '<testcase classname="synthetic" name="pass" /></testsuite>',
        encoding="utf-8",
    )
    link = tmp_path / "link.xml"
    try:
        link.symlink_to(target)
    except OSError:
        original_lstat = Path.lstat

        def synthetic_lstat(path: Path) -> object:
            if path == link:
                return SimpleNamespace(st_mode=stat.S_IFLNK, st_size=0)
            return original_lstat(path)

        monkeypatch.setattr(Path, "lstat", synthetic_lstat)
    with pytest.raises(SoftwareValidationError, match="regular non-symlink"):
        validation._parse_pytest_counts(link, audit)


def test_report_refuses_nonzero_pytest_outcomes() -> None:
    assert not PytestCounts(1, 0, 0, 1, 0, 0, 0, 0).clean
    assert not PytestCounts(1, 0, 0, 0, 1, 0, 0, 0).clean
    assert not PytestCounts(1, 0, 0, 0, 0, 1, 0, 0).clean
    assert not PytestCounts(1, 0, 0, 0, 0, 0, 1, 0).clean
    assert not PytestCounts(1, 0, 0, 0, 0, 0, 0, 1).clean


def test_phase13_service_check_runs_the_complete_test_directory(
    valid_bundle: _Bundle,
) -> None:
    check = next(
        item
        for item in valid_bundle.report.checks
        if item.check_id == "PHASE13_SERVICE_PYTEST"
    )
    assert check.argv[-1] == "tests"


def test_git_diff_check_binds_crlf_safe_whitespace_policy(valid_bundle: _Bundle) -> None:
    check = next(
        item for item in valid_bundle.report.checks if item.check_id == "GIT_DIFF_CHECK"
    )

    assert check.argv == (
        check.executable_path,
        "-c",
        "core.whitespace=blank-at-eol,blank-at-eof,space-before-tab,cr-at-eol",
        "diff",
        "--check",
        "--",
    )


def test_git_diff_check_allows_crlf_but_rejects_space_before_crlf(
    tmp_path: Path,
) -> None:
    git = validation._resolve_executable("git")
    repository = tmp_path / "repository"
    repository.mkdir()
    environment = {
        key: value
        for key, value in os.environ.items()
        if key.upper() in {"COMSPEC", "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR"}
    }
    environment.update(
        {
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_TERMINAL_PROMPT": "0",
        }
    )
    subprocess.run(
        (str(git), "init", "--quiet"),
        cwd=repository,
        check=True,
        capture_output=True,
        env=environment,
    )
    source = repository / "sample.txt"
    source.write_bytes(b"first\n")
    subprocess.run(
        (str(git), "-c", "core.autocrlf=false", "add", "--", source.name),
        cwd=repository,
        check=True,
        capture_output=True,
        env=environment,
    )
    command = (
        str(git),
        "-c",
        "core.whitespace=blank-at-eol,blank-at-eof,space-before-tab,cr-at-eol",
        "diff",
        "--check",
        "--",
    )

    source.write_bytes(b"first\r\nsecond\r\n")
    crlf = subprocess.run(
        command,
        cwd=repository,
        check=False,
        capture_output=True,
        env=environment,
    )
    assert crlf.returncode == 0
    assert crlf.stdout == b""
    assert crlf.stderr == b""

    source.write_bytes(b"first \r\nsecond\r\n")
    trailing = subprocess.run(
        command,
        cwd=repository,
        check=False,
        capture_output=True,
        env=environment,
    )
    assert trailing.returncode != 0
    assert b"trailing whitespace" in trailing.stdout
    assert trailing.stderr == b""


def test_git_diff_whitespace_policy_mutation_fails_closed(
    valid_bundle: _Bundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        validation,
        "_GIT_DIFF_WHITESPACE_POLICY",
        "core.whitespace=blank-at-eol",
    )

    with pytest.raises(SoftwareValidationError, match="compiled check differs"):
        load_testnet_software_validation(
            valid_bundle.report_path,
            valid_bundle.config,
            repository_root=valid_bundle.root,
        )


def test_build_provenance_binds_wheels_wheelhouse_and_imports(
    valid_bundle: _Bundle,
) -> None:
    provenance = _write_build_provenance(valid_bundle)
    artifacts = _REAL_BUILD_ADDITIONAL_ARTIFACTS(
        valid_bundle.root,
        valid_bundle.output,
        provenance.relative_to(valid_bundle.output).as_posix(),
    )
    assert len(artifacts) == 4
    assert artifacts[0].relative_path.endswith("build-provenance.json")
    assert {Path(item.relative_path).suffix for item in artifacts[1:3]} == {".whl"}
    assert artifacts[3].relative_path.endswith("import-verification.json")


def test_service_wheel_hyperlab_registry_dependency_fails_closed(
    valid_bundle: _Bundle,
) -> None:
    provenance = _write_build_provenance(
        valid_bundle,
        service_requirements=("hyperlab==0.2.1",),
    )
    with pytest.raises(SoftwareValidationError, match="HyperLab dependency"):
        _REAL_BUILD_ADDITIONAL_ARTIFACTS(
            valid_bundle.root,
            valid_bundle.output,
            provenance.relative_to(valid_bundle.output).as_posix(),
        )


def test_modified_built_wheel_fails_closed(valid_bundle: _Bundle) -> None:
    provenance = _write_build_provenance(valid_bundle)
    service_wheel = (
        valid_bundle.output
        / "runtime"
        / "build-isolation"
        / "wheels"
        / "hyperlab_testnet_executor-0.3.0.dev0-py3-none-any.whl"
    )
    with service_wheel.open("ab") as stream:
        stream.write(b"modified")
    with pytest.raises(SoftwareValidationError, match="wheel differs"):
        _REAL_BUILD_ADDITIONAL_ARTIFACTS(
            valid_bundle.root,
            valid_bundle.output,
            provenance.relative_to(valid_bundle.output).as_posix(),
        )


def test_modified_wheelhouse_inventory_fails_closed(valid_bundle: _Bundle) -> None:
    provenance = _write_build_provenance(valid_bundle)
    wheelhouse = valid_bundle.root / validation._WHEELHOUSE_RELATIVE
    _write_wheel(
        wheelhouse / "unexpected-1.0-py3-none-any.whl",
        distribution="unexpected",
        version="1.0",
    )
    with pytest.raises(SoftwareValidationError, match="wheelhouse inventory differs"):
        _REAL_BUILD_ADDITIONAL_ARTIFACTS(
            valid_bundle.root,
            valid_bundle.output,
            provenance.relative_to(valid_bundle.output).as_posix(),
        )


def test_nested_build_operations_cannot_import_checkout_pythonpath(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    observed: dict[str, str] = {}

    def fake_run(*args: object, **kwargs: object) -> SimpleNamespace:
        del args
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        observed.update(environment)
        return SimpleNamespace(returncode=0)

    monkeypatch.setenv("PYTHONPATH", "synthetic-checkout-source")
    monkeypatch.setattr(validation.subprocess, "run", fake_run)
    record, exit_code = validation._run_build_operation(
        "SYNTHETIC",
        ("python", "--version"),
        tmp_path,
    )
    assert exit_code == 0
    assert record["exit_code"] == 0
    assert "PYTHONPATH" not in observed
