"""Canonical, local-only software validation for the Testnet executor.

The report is evidence, not a test launcher shortcut.  Every check has a
compiled identifier and argv, runs with a scrubbed offline environment, and
binds immutable stdout/stderr (plus JUnit data for pytest) by size and SHA-256.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email.parser import BytesParser
from email.policy import compat32
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, Self, cast
from xml.etree import ElementTree

from .build_identity import (
    BuildIdentityError,
    TestnetBuildIdentity,
    build_lock_sha256,
    current_testnet_build_identity,
    external_lock_sha256,
    validate_runtime_identity,
)
from .canonical import canonical_json, canonical_sha256, parse_utc, utc_text
from .config import TestnetConfig

SOFTWARE_VALIDATION_SCHEMA_VERSION = 1
PHASE13_BASELINE_HEAD = "88e8224797684b9bf44be426b79ee01cccbe6e46"
PHASE13_BASELINE_BRANCH = "phase-13-testnet"
REPORT_NAME = "software-validation.json"
_WHEELHOUSE_RELATIVE = Path("services/testnet-executor/.wheelhouse")

CHECK_IDS = (
    "RUFF_ROOT",
    "RUFF_SERVICE",
    "MYPY_ROOT",
    "MYPY_SERVICE",
    "PHASE13_ROOT_PYTEST",
    "PHASE13_SERVICE_PYTEST",
    "TESTNET_AUTH_PYTEST",
    "PHASE12_REGRESSION_PYTEST",
    "FULL_ROOT_PYTEST",
    "GIT_DIFF_CHECK",
    "CONFLICT_MARKER_SCAN",
    "MANIFEST_VERIFY",
    "RELEASE_POLICY_VERIFY",
    "DEPENDENCY_BUILD_ISOLATION",
)
_PYTEST_CHECK_IDS = frozenset(
    {
        "PHASE13_ROOT_PYTEST",
        "PHASE13_SERVICE_PYTEST",
        "TESTNET_AUTH_PYTEST",
        "PHASE12_REGRESSION_PYTEST",
        "FULL_ROOT_PYTEST",
    }
)
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_GIT_OID_RE = re.compile(r"[0-9a-f]{40}\Z")
_CHECK_ID_RE = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_SECRET_ENV_MARKERS = (
    "API_KEY",
    "AUTH",
    "COOKIE",
    "CREDENTIAL",
    "MNEMONIC",
    "PASSWORD",
    "PRIVATE",
    "SEED",
    "SECRET",
    "SIGNATURE",
    "TOKEN",
    "WALLET",
)
_SAFE_INHERITED_ENV = frozenset(
    {
        "COMSPEC",
        "LANG",
        "LC_ALL",
        "NUMBER_OF_PROCESSORS",
        "OS",
        "PATHEXT",
        "PATH",
        "PROCESSOR_ARCHITECTURE",
        "SYSTEMDRIVE",
        "SYSTEMROOT",
        "TZ",
        "WINDIR",
    }
)
_PROXY_POISON = "http://127.0.0.1:9"
_MAX_ARTIFACT_BYTES = 512 * 1024 * 1024
_MAX_REPORT_BYTES = 8 * 1024 * 1024
_MAX_CONFLICT_FILE_BYTES = 16 * 1024 * 1024
_MAX_JUNIT_BYTES = 16 * 1024 * 1024
_MAX_PYTEST_AUDIT_BYTES = 4 * 1024
_MAX_PYTEST_COUNT = 1_000_000
_PYTEST_COUNT_RE = re.compile(r"(?:0|[1-9][0-9]{0,6})\Z")
_GIT_DIFF_WHITESPACE_POLICY = (
    "core.whitespace=blank-at-eol,blank-at-eof,space-before-tab,cr-at-eol"
)


class SoftwareValidationError(RuntimeError):
    """Software-validation evidence is missing, divergent, or unsafe."""


def _require_text(value: object, *, label: str, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise SoftwareValidationError(f"{label} must be non-empty canonical text")
    if len(value) > maximum or any(ord(character) < 32 for character in value):
        raise SoftwareValidationError(f"{label} contains invalid text")
    return value


def _require_sha256(value: object, *, label: str) -> str:
    text = _require_text(value, label=label, maximum=64)
    if _SHA256_RE.fullmatch(text) is None:
        raise SoftwareValidationError(f"{label} must be lowercase SHA-256")
    return text


def _require_git_oid(value: object, *, label: str) -> str:
    text = _require_text(value, label=label, maximum=40)
    if _GIT_OID_RE.fullmatch(text) is None:
        raise SoftwareValidationError(f"{label} must be a lowercase full Git object ID")
    return text


def _require_int(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise SoftwareValidationError(f"{label} must be an integer >= {minimum}")
    return value


def _exact_mapping(
    value: object,
    keys: frozenset[str],
    *,
    label: str,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise SoftwareValidationError(f"{label} must be an object with string keys")
    observed = frozenset(cast(Mapping[str, object], value))
    if observed != keys:
        raise SoftwareValidationError(f"{label} has missing or unexpected fields")
    return cast(Mapping[str, object], value)


def _strict_json(raw: bytes, *, label: str) -> object:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise SoftwareValidationError(f"{label} is not UTF-8") from error

    def pairs(values: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, item in values:
            if key in result:
                raise SoftwareValidationError(f"{label} contains duplicate JSON keys")
            result[key] = item
        return result

    try:
        return json.loads(text, object_pairs_hook=pairs)
    except (json.JSONDecodeError, TypeError, ValueError) as error:
        if isinstance(error, SoftwareValidationError):
            raise
        raise SoftwareValidationError(f"{label} is not strict JSON") from error


def _mapping_of_text(value: object, *, label: str) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise SoftwareValidationError(f"{label} must be a string mapping")
    result: dict[str, str] = {}
    for raw_key, raw_value in cast(Mapping[str, object], value).items():
        key = _require_text(raw_key, label=f"{label} key", maximum=128)
        result[key] = _require_text(raw_value, label=f"{label}.{key}")
    return MappingProxyType(dict(sorted(result.items())))


@dataclass(frozen=True, slots=True)
class ValidationArtifact:
    relative_path: str
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        path = _require_text(self.relative_path, label="artifact relative_path", maximum=512)
        candidate = Path(path)
        if (
            candidate.is_absolute()
            or "\\" in path
            or path != candidate.as_posix()
            or any(part in {"", ".", ".."} for part in candidate.parts)
        ):
            raise SoftwareValidationError("artifact path must be a canonical relative POSIX path")
        object.__setattr__(self, "relative_path", path)
        object.__setattr__(
            self,
            "size_bytes",
            _require_int(self.size_bytes, label="artifact size_bytes"),
        )
        object.__setattr__(self, "sha256", _require_sha256(self.sha256, label="artifact sha256"))

    def to_dict(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }

    @classmethod
    def from_dict(cls, value: object, *, label: str) -> Self:
        raw = _exact_mapping(
            value,
            frozenset({"relative_path", "sha256", "size_bytes"}),
            label=label,
        )
        return cls(
            relative_path=cast(str, raw["relative_path"]),
            size_bytes=cast(int, raw["size_bytes"]),
            sha256=cast(str, raw["sha256"]),
        )


@dataclass(frozen=True, slots=True)
class PytestCounts:
    tests: int
    failures: int
    errors: int
    skipped: int
    deselected: int
    xfailed: int
    xpassed: int
    external_network_attempts: int

    def __post_init__(self) -> None:
        for field_name in (
            "tests",
            "failures",
            "errors",
            "skipped",
            "deselected",
            "xfailed",
            "xpassed",
            "external_network_attempts",
        ):
            object.__setattr__(
                self,
                field_name,
                _require_int(getattr(self, field_name), label=f"pytest {field_name}"),
            )

    @property
    def clean(self) -> bool:
        return self.tests > 0 and not any(
            (
                self.failures,
                self.errors,
                self.skipped,
                self.deselected,
                self.xfailed,
                self.xpassed,
                self.external_network_attempts,
            )
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "deselected": self.deselected,
            "errors": self.errors,
            "external_network_attempts": self.external_network_attempts,
            "failures": self.failures,
            "skipped": self.skipped,
            "tests": self.tests,
            "xfailed": self.xfailed,
            "xpassed": self.xpassed,
        }

    @classmethod
    def from_dict(cls, value: object, *, label: str) -> Self:
        keys = frozenset(
            {
                "tests",
                "failures",
                "errors",
                "skipped",
                "deselected",
                "xfailed",
                "xpassed",
                "external_network_attempts",
            }
        )
        raw = _exact_mapping(value, keys, label=label)
        return cls(**{key: cast(int, raw[key]) for key in keys})


@dataclass(frozen=True, slots=True)
class ValidationCheckRecord:
    check_id: str
    argv: tuple[str, ...]
    cwd: str
    executable_path: str
    executable_sha256: str
    executable_version: str
    started_at: datetime
    ended_at: datetime
    exit_code: int
    stdout: ValidationArtifact
    stderr: ValidationArtifact
    junit: ValidationArtifact | None = None
    pytest_audit: ValidationArtifact | None = None
    pytest_counts: PytestCounts | None = None
    additional_artifacts: tuple[ValidationArtifact, ...] = ()
    passed: bool = False

    def __post_init__(self) -> None:
        check_id = _require_text(self.check_id, label="check_id", maximum=64)
        if _CHECK_ID_RE.fullmatch(check_id) is None:
            raise SoftwareValidationError("check_id is not canonical")
        object.__setattr__(self, "check_id", check_id)
        if not self.argv or any(not isinstance(item, str) or not item for item in self.argv):
            raise SoftwareValidationError("check argv must be non-empty canonical strings")
        cwd = Path(_require_text(self.cwd, label=f"{check_id} cwd"))
        executable = Path(_require_text(self.executable_path, label=f"{check_id} executable"))
        if not cwd.is_absolute() or not executable.is_absolute():
            raise SoftwareValidationError("check cwd and executable must be absolute")
        if self.argv[0] != str(executable):
            raise SoftwareValidationError("check argv[0] must equal executable_path")
        object.__setattr__(self, "executable_sha256", _require_sha256(self.executable_sha256, label="executable_sha256"))
        object.__setattr__(self, "executable_version", _require_text(self.executable_version, label="executable_version"))
        start = parse_utc(utc_text(self.started_at), label="check started_at")
        end = parse_utc(utc_text(self.ended_at), label="check ended_at")
        if end < start:
            raise SoftwareValidationError("check ended_at precedes started_at")
        object.__setattr__(self, "started_at", start)
        object.__setattr__(self, "ended_at", end)
        if isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int):
            raise SoftwareValidationError("check exit_code must be an integer")
        is_pytest = check_id in _PYTEST_CHECK_IDS
        if is_pytest != all(
            item is not None for item in (self.junit, self.pytest_audit, self.pytest_counts)
        ):
            raise SoftwareValidationError("pytest artifacts/counts must exist exactly for pytest checks")
        expected_pass = self.exit_code == 0 and (
            not is_pytest or cast(PytestCounts, self.pytest_counts).clean
        )
        if not isinstance(self.passed, bool) or self.passed is not expected_pass:
            raise SoftwareValidationError("check passed flag differs from its durable result")
        paths = [self.stdout.relative_path, self.stderr.relative_path]
        paths.extend(item.relative_path for item in self.additional_artifacts)
        if self.junit is not None:
            paths.append(self.junit.relative_path)
        if self.pytest_audit is not None:
            paths.append(self.pytest_audit.relative_path)
        if len(paths) != len(set(paths)):
            raise SoftwareValidationError("check artifacts contain duplicate paths")

    def to_dict(self) -> dict[str, object]:
        return {
            "additional_artifacts": [item.to_dict() for item in self.additional_artifacts],
            "argv": list(self.argv),
            "check_id": self.check_id,
            "cwd": self.cwd,
            "ended_at": utc_text(self.ended_at),
            "executable_path": self.executable_path,
            "executable_sha256": self.executable_sha256,
            "executable_version": self.executable_version,
            "exit_code": self.exit_code,
            "junit": self.junit.to_dict() if self.junit is not None else None,
            "passed": self.passed,
            "pytest_audit": self.pytest_audit.to_dict() if self.pytest_audit is not None else None,
            "pytest_counts": self.pytest_counts.to_dict() if self.pytest_counts is not None else None,
            "started_at": utc_text(self.started_at),
            "stderr": self.stderr.to_dict(),
            "stdout": self.stdout.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object, *, label: str) -> Self:
        keys = frozenset(
            {
                "additional_artifacts",
                "argv",
                "check_id",
                "cwd",
                "ended_at",
                "executable_path",
                "executable_sha256",
                "executable_version",
                "exit_code",
                "junit",
                "passed",
                "pytest_audit",
                "pytest_counts",
                "started_at",
                "stderr",
                "stdout",
            }
        )
        raw = _exact_mapping(value, keys, label=label)
        raw_argv = raw["argv"]
        raw_additional = raw["additional_artifacts"]
        if not isinstance(raw_argv, list) or not isinstance(raw_additional, list):
            raise SoftwareValidationError(f"{label} argv/artifacts must be arrays")
        junit = raw["junit"]
        audit = raw["pytest_audit"]
        counts = raw["pytest_counts"]
        return cls(
            check_id=cast(str, raw["check_id"]),
            argv=tuple(cast(list[str], raw_argv)),
            cwd=cast(str, raw["cwd"]),
            executable_path=cast(str, raw["executable_path"]),
            executable_sha256=cast(str, raw["executable_sha256"]),
            executable_version=cast(str, raw["executable_version"]),
            started_at=parse_utc(cast(str, raw["started_at"]), label="started_at"),
            ended_at=parse_utc(cast(str, raw["ended_at"]), label="ended_at"),
            exit_code=cast(int, raw["exit_code"]),
            stdout=ValidationArtifact.from_dict(raw["stdout"], label=f"{label}.stdout"),
            stderr=ValidationArtifact.from_dict(raw["stderr"], label=f"{label}.stderr"),
            junit=(
                ValidationArtifact.from_dict(junit, label=f"{label}.junit")
                if junit is not None
                else None
            ),
            pytest_audit=(
                ValidationArtifact.from_dict(audit, label=f"{label}.pytest_audit")
                if audit is not None
                else None
            ),
            pytest_counts=(
                PytestCounts.from_dict(counts, label=f"{label}.pytest_counts")
                if counts is not None
                else None
            ),
            additional_artifacts=tuple(
                ValidationArtifact.from_dict(item, label=f"{label}.additional")
                for item in raw_additional
            ),
            passed=cast(bool, raw["passed"]),
        )


@dataclass(frozen=True, slots=True)
class SoftwareValidationReport:
    validation_id: str
    created_at: datetime
    repository_root: str
    output_root: str
    baseline_head: str
    repository_head: str
    branch: str
    config_subject: Mapping[str, str]
    build_identity_before: Mapping[str, str]
    build_identity_after: Mapping[str, str]
    worktree_inventory_before_sha256: str
    worktree_inventory_after_sha256: str
    external_lock_sha256: str
    build_lock_sha256: str
    python_identity: Mapping[str, str]
    checks: tuple[ValidationCheckRecord, ...]
    passed: bool
    schema_version: int = SOFTWARE_VALIDATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        schema_version = _require_int(self.schema_version, label="schema_version", minimum=1)
        if schema_version != SOFTWARE_VALIDATION_SCHEMA_VERSION:
            raise SoftwareValidationError("unsupported software-validation schema")
        object.__setattr__(self, "schema_version", schema_version)
        validation_id = _require_sha256(self.validation_id, label="validation_id")
        created = parse_utc(utc_text(self.created_at), label="report created_at")
        object.__setattr__(self, "created_at", created)
        root = Path(_require_text(self.repository_root, label="repository_root"))
        output = Path(_require_text(self.output_root, label="output_root"))
        if not root.is_absolute() or not output.is_absolute():
            raise SoftwareValidationError("report roots must be absolute")
        object.__setattr__(self, "baseline_head", _require_git_oid(self.baseline_head, label="baseline_head"))
        object.__setattr__(self, "repository_head", _require_git_oid(self.repository_head, label="repository_head"))
        object.__setattr__(self, "branch", _require_text(self.branch, label="branch", maximum=128))
        object.__setattr__(self, "external_lock_sha256", _require_sha256(self.external_lock_sha256, label="external_lock_sha256"))
        object.__setattr__(self, "build_lock_sha256", _require_sha256(self.build_lock_sha256, label="build_lock_sha256"))
        object.__setattr__(self, "config_subject", _mapping_of_text(self.config_subject, label="config_subject"))
        object.__setattr__(self, "build_identity_before", _mapping_of_text(self.build_identity_before, label="build_identity_before"))
        object.__setattr__(self, "build_identity_after", _mapping_of_text(self.build_identity_after, label="build_identity_after"))
        object.__setattr__(
            self,
            "worktree_inventory_before_sha256",
            _require_sha256(
                self.worktree_inventory_before_sha256,
                label="worktree_inventory_before_sha256",
            ),
        )
        object.__setattr__(
            self,
            "worktree_inventory_after_sha256",
            _require_sha256(
                self.worktree_inventory_after_sha256,
                label="worktree_inventory_after_sha256",
            ),
        )
        object.__setattr__(self, "python_identity", _mapping_of_text(self.python_identity, label="python_identity"))
        expected_validation_id = canonical_sha256(
            {
                "build_hash": self.build_identity_before.get("build_hash"),
                "config_hash": self.config_subject.get("config_hash"),
                "created_at": utc_text(created),
                "repository_head": self.repository_head,
            }
        )
        if validation_id != expected_validation_id:
            raise SoftwareValidationError("validation_id differs from the canonical report identity")
        object.__setattr__(self, "validation_id", validation_id)
        ids = tuple(item.check_id for item in self.checks)
        if ids != CHECK_IDS:
            raise SoftwareValidationError("report must contain the exact fixed check set in order")
        artifact_paths = [
            artifact.relative_path
            for check in self.checks
            for artifact in _check_artifacts(check)
        ]
        if len(artifact_paths) != len(set(artifact_paths)):
            raise SoftwareValidationError("report reuses an artifact across checks")
        expected_pass = (
            all(item.passed for item in self.checks)
            and self.baseline_head == PHASE13_BASELINE_HEAD
            and self.branch == PHASE13_BASELINE_BRANCH
            and dict(self.build_identity_before) == dict(self.build_identity_after)
            and self.worktree_inventory_before_sha256
            == self.worktree_inventory_after_sha256
        )
        if not isinstance(self.passed, bool) or self.passed is not expected_pass:
            raise SoftwareValidationError("report passed flag differs from its evidence")

    def to_dict(self) -> dict[str, object]:
        return {
            "baseline_head": self.baseline_head,
            "branch": self.branch,
            "build_identity_after": dict(self.build_identity_after),
            "build_identity_before": dict(self.build_identity_before),
            "build_lock_sha256": self.build_lock_sha256,
            "checks": [item.to_dict() for item in self.checks],
            "config_subject": dict(self.config_subject),
            "created_at": utc_text(self.created_at),
            "external_lock_sha256": self.external_lock_sha256,
            "output_root": self.output_root,
            "passed": self.passed,
            "python_identity": dict(self.python_identity),
            "repository_head": self.repository_head,
            "repository_root": self.repository_root,
            "schema_version": self.schema_version,
            "validation_id": self.validation_id,
            "worktree_inventory_after_sha256": self.worktree_inventory_after_sha256,
            "worktree_inventory_before_sha256": self.worktree_inventory_before_sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        keys = frozenset(
            {
                "baseline_head",
                "branch",
                "build_identity_after",
                "build_identity_before",
                "build_lock_sha256",
                "checks",
                "config_subject",
                "created_at",
                "external_lock_sha256",
                "output_root",
                "passed",
                "python_identity",
                "repository_head",
                "repository_root",
                "schema_version",
                "validation_id",
                "worktree_inventory_after_sha256",
                "worktree_inventory_before_sha256",
            }
        )
        raw = _exact_mapping(value, keys, label="report")
        raw_checks = raw["checks"]
        if not isinstance(raw_checks, list):
            raise SoftwareValidationError("report.checks must be an array")
        return cls(
            validation_id=cast(str, raw["validation_id"]),
            created_at=parse_utc(cast(str, raw["created_at"]), label="created_at"),
            repository_root=cast(str, raw["repository_root"]),
            output_root=cast(str, raw["output_root"]),
            baseline_head=cast(str, raw["baseline_head"]),
            repository_head=cast(str, raw["repository_head"]),
            branch=cast(str, raw["branch"]),
            config_subject=_mapping_of_text(raw["config_subject"], label="config_subject"),
            build_identity_before=_mapping_of_text(raw["build_identity_before"], label="build_identity_before"),
            build_identity_after=_mapping_of_text(raw["build_identity_after"], label="build_identity_after"),
            worktree_inventory_before_sha256=cast(
                str, raw["worktree_inventory_before_sha256"]
            ),
            worktree_inventory_after_sha256=cast(
                str, raw["worktree_inventory_after_sha256"]
            ),
            external_lock_sha256=cast(str, raw["external_lock_sha256"]),
            build_lock_sha256=cast(str, raw["build_lock_sha256"]),
            python_identity=_mapping_of_text(raw["python_identity"], label="python_identity"),
            checks=tuple(
                ValidationCheckRecord.from_dict(item, label=f"checks[{index}]")
                for index, item in enumerate(raw_checks)
            ),
            passed=cast(bool, raw["passed"]),
            schema_version=cast(int, raw["schema_version"]),
        )


@dataclass(frozen=True, slots=True)
class _CheckSpec:
    check_id: str
    argv: tuple[str, ...]
    cwd: Path
    stdout_relative: str
    stderr_relative: str
    junit_relative: str | None = None
    pytest_audit_relative: str | None = None
    build_provenance_relative: str | None = None


def _check_artifacts(check: ValidationCheckRecord) -> tuple[ValidationArtifact, ...]:
    values = [check.stdout, check.stderr, *check.additional_artifacts]
    if check.junit is not None:
        values.append(check.junit)
    if check.pytest_audit is not None:
        values.append(check.pytest_audit)
    return tuple(values)


def _identity_dict(identity: TestnetBuildIdentity) -> Mapping[str, str]:
    return MappingProxyType(identity.to_dict())


def _python_identity(executable: Path) -> Mapping[str, str]:
    implementation = sys.implementation
    return MappingProxyType(
        {
            "abi_cache_tag": implementation.cache_tag or "none",
            "executable": str(executable),
            "implementation": implementation.name,
            "machine": platform.machine() or "unknown",
            "platform": sys.platform,
            "platform_release": platform.release() or "unknown",
            "version": platform.python_version(),
        }
    )


def _sha256_file(path: Path, *, maximum: int = _MAX_ARTIFACT_BYTES) -> tuple[int, str]:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise SoftwareValidationError(f"artifact is missing: {path}") from error
    if path.is_symlink() or not path.is_file():
        raise SoftwareValidationError(f"artifact is not a regular file: {path}")
    size = metadata.st_size
    if size < 0 or size > maximum:
        raise SoftwareValidationError(f"artifact size is outside the compiled bound: {path}")
    digest = hashlib.sha256()
    observed = 0
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                observed += len(chunk)
                if observed > maximum:
                    raise SoftwareValidationError("artifact grew beyond the compiled bound")
                digest.update(chunk)
    except OSError as error:
        raise SoftwareValidationError(f"artifact cannot be read: {path}") from error
    if observed != size:
        raise SoftwareValidationError(f"artifact changed while hashing: {path}")
    return size, digest.hexdigest()


def _artifact(output_root: Path, relative: str) -> ValidationArtifact:
    candidate = output_root / relative
    _assert_no_symlinks(output_root, candidate)
    size, digest = _sha256_file(candidate)
    return ValidationArtifact(relative, size, digest)


def _assert_no_symlinks(root: Path, candidate: Path) -> None:
    try:
        relative = candidate.relative_to(root)
    except ValueError as error:
        raise SoftwareValidationError("artifact escapes the validation output root") from error
    current = root
    if current.is_symlink():
        raise SoftwareValidationError("validation output root cannot be a symlink")
    for part in relative.parts:
        current /= part
        if os.path.lexists(current) and current.is_symlink():
            raise SoftwareValidationError("validation artifact path contains a symlink")


def _repository_paths(repository_root: Path) -> tuple[Path, Path]:
    root = repository_root.resolve()
    service = root / "services" / "testnet-executor"
    if root.is_symlink() or not (root / ".git").is_dir():
        raise SoftwareValidationError("repository_root is not the HyperLab Git worktree")
    if service.is_symlink() or not (service / "pyproject.toml").is_file():
        raise SoftwareValidationError("Testnet service root is unavailable")
    return root, service


def _resolve_executable(name: str) -> Path:
    raw = shutil.which(name)
    if raw is None:
        raise SoftwareValidationError(f"required executable is unavailable: {name}")
    path = Path(raw).resolve()
    if path.is_symlink() or not path.is_file():
        raise SoftwareValidationError(f"required executable is not a regular file: {name}")
    return path


def _repository_gate_python(repository_root: Path) -> Path:
    relative = (
        Path(".venv") / "Scripts" / "python.exe"
        if os.name == "nt"
        else Path(".venv") / "bin" / "python"
    )
    candidate = repository_root / relative
    if not os.path.lexists(candidate) or candidate.is_symlink() or not candidate.is_file():
        raise SoftwareValidationError(
            "repository validation interpreter is missing, symlinked, or non-regular"
        )
    return candidate.resolve()


def _pytest_spec(
    check_id: str,
    *,
    python: Path,
    cwd: Path,
    output_root: Path,
    arguments: Sequence[str],
) -> _CheckSpec:
    slug = check_id.casefold().replace("_", "-")
    junit_relative = f"artifacts/{slug}.junit.xml"
    audit_relative = f"artifacts/{slug}.pytest-audit.json"
    basetemp = output_root / "runtime" / "pytest-basetemp" / slug
    return _CheckSpec(
        check_id,
        (
            str(python),
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "-p",
            "hyperlab_testnet.validation",
            f"--junitxml={output_root / junit_relative}",
            f"--basetemp={basetemp}",
            *arguments,
        ),
        cwd,
        f"artifacts/{slug}.stdout.txt",
        f"artifacts/{slug}.stderr.txt",
        junit_relative,
        audit_relative,
    )


def _expected_check_specs(
    repository_root: Path,
    output_root: Path,
    operator_python: Path,
    gate_python: Path,
    git: Path,
) -> tuple[_CheckSpec, ...]:
    root, service = _repository_paths(repository_root)

    def ordinary(
        check_id: str,
        argv: Sequence[str],
        cwd: Path,
        *,
        build_provenance: str | None = None,
    ) -> _CheckSpec:
        slug = check_id.casefold().replace("_", "-")
        return _CheckSpec(
            check_id,
            tuple(argv),
            cwd,
            f"artifacts/{slug}.stdout.txt",
            f"artifacts/{slug}.stderr.txt",
            build_provenance_relative=build_provenance,
        )

    phase12 = (
        "tests/test_paper_engine_phase12.py",
        "tests/test_paper_runtime_phase12.py",
        "tests/test_paper_gate_snapshot_phase12.py",
        "tests/test_paper_gate_cli_phase12.py",
        "tests/test_paper_admission_cli_phase12.py",
        "tests/test_phase12_fee_artifact.py",
    )
    build_output = output_root / "runtime" / "build-isolation"
    wheelhouse = root / _WHEELHOUSE_RELATIVE
    specs = (
        ordinary(
            "RUFF_ROOT",
            (str(gate_python), "-m", "ruff", "check", "--no-cache", "."),
            root,
        ),
        ordinary(
            "RUFF_SERVICE",
            (str(gate_python), "-m", "ruff", "check", "--no-cache", "."),
            service,
        ),
        ordinary(
            "MYPY_ROOT",
            (
                str(gate_python),
                "-m",
                "mypy",
                "--cache-dir",
                str(output_root / "runtime" / "mypy-root"),
                "src/hyperlab",
            ),
            root,
        ),
        ordinary(
            "MYPY_SERVICE",
            (
                str(gate_python),
                "-m",
                "mypy",
                "--cache-dir",
                str(output_root / "runtime" / "mypy-service"),
                "src/hyperlab_testnet",
            ),
            service,
        ),
        _pytest_spec(
            "PHASE13_ROOT_PYTEST",
            python=gate_python,
            cwd=root,
            output_root=output_root,
            arguments=(
                "tests/test_testnet_core_phase13.py",
                "tests/test_testnet_service_isolation_phase13.py",
            ),
        ),
        _pytest_spec(
            "PHASE13_SERVICE_PYTEST",
            python=gate_python,
            cwd=service,
            output_root=output_root,
            arguments=("tests",),
        ),
        _pytest_spec(
            "TESTNET_AUTH_PYTEST",
            python=gate_python,
            cwd=root,
            output_root=output_root,
            arguments=("tests/test_testnet_authorization_phase13.py",),
        ),
        _pytest_spec(
            "PHASE12_REGRESSION_PYTEST",
            python=gate_python,
            cwd=root,
            output_root=output_root,
            arguments=phase12,
        ),
        _pytest_spec(
            "FULL_ROOT_PYTEST",
            python=gate_python,
            cwd=root,
            output_root=output_root,
            arguments=(),
        ),
        ordinary(
            "GIT_DIFF_CHECK",
            (str(git), "-c", _GIT_DIFF_WHITESPACE_POLICY, "diff", "--check", "--"),
            root,
        ),
        ordinary(
            "CONFLICT_MARKER_SCAN",
            (
                str(operator_python),
                "-m",
                "hyperlab_testnet.validation",
                "--conflict-scan",
                "--root",
                str(root),
            ),
            root,
        ),
        ordinary(
            "MANIFEST_VERIFY",
            (str(gate_python), "scripts/verify_manifest.py", "--root", str(root)),
            root,
        ),
        ordinary(
            "RELEASE_POLICY_VERIFY",
            (
                str(gate_python),
                "scripts/verify_release.py",
                "--auto",
                "--check-manifest",
                "--root",
                str(root),
            ),
            root,
        ),
        ordinary(
            "DEPENDENCY_BUILD_ISOLATION",
            (
                str(operator_python),
                "-m",
                "hyperlab_testnet.validation",
                "--build-isolation",
                "--root",
                str(root),
                "--output",
                str(build_output),
                "--wheelhouse",
                str(wheelhouse),
            ),
            root,
            build_provenance="runtime/build-isolation/build-provenance.json",
        ),
    )
    if tuple(spec.check_id for spec in specs) != CHECK_IDS:
        raise AssertionError("compiled software-validation check order diverged")
    return specs


def _value_looks_secret(value: str) -> bool:
    upper = value.upper()
    return (
        "BEGIN PRIVATE KEY" in upper
        or re.fullmatch(r"0x[0-9A-Fa-f]{64}", value) is not None
        or re.fullmatch(r"[0-9A-Fa-f]{64}", value) is not None
    )


def _sanitized_environment(
    *,
    repository_root: Path,
    output_root: Path,
    create_directories: bool = True,
) -> dict[str, str]:
    runtime = output_root / "runtime"
    home = runtime / "home"
    temporary = runtime / "tmp"
    cache = runtime / "cache"
    for path in (
        runtime,
        home,
        temporary,
        cache,
        runtime / "pytest-basetemp",
        output_root / "artifacts",
    ):
        if create_directories:
            path.mkdir(parents=True, exist_ok=True)
        elif not path.is_dir() or path.is_symlink():
            raise SoftwareValidationError("validation runtime directory is missing or unsafe")
    result: dict[str, str] = {}
    for key in sorted(_SAFE_INHERITED_ENV):
        value = os.environ.get(key)
        if value is None:
            continue
        upper_key = key.upper()
        if any(marker in upper_key for marker in _SECRET_ENV_MARKERS):
            continue
        if _value_looks_secret(value):
            continue
        result[key] = value
    result.update(
        {
            "ALL_PROXY": _PROXY_POISON,
            "APPDATA": str(home / "AppData" / "Roaming"),
            "COVERAGE_FILE": str(runtime / ".coverage"),
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_TERMINAL_PROMPT": "0",
            "HOME": str(home),
            "HTTP_PROXY": _PROXY_POISON,
            "HTTPS_PROXY": _PROXY_POISON,
            "LOCALAPPDATA": str(home / "AppData" / "Local"),
            "NO_PROXY": "",
            "PIP_DISABLE_PIP_VERSION_CHECK": "1",
            "PIP_NO_INDEX": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONHASHSEED": "0",
            "PYTHONPATH": os.pathsep.join(
                (
                    str(repository_root / "services" / "testnet-executor" / "src"),
                    str(repository_root / "src"),
                )
            ),
            "RUFF_CACHE_DIR": str(cache / "ruff"),
            "TEMP": str(temporary),
            "TMP": str(temporary),
            "USERPROFILE": str(home),
            "UV_OFFLINE": "1",
            "all_proxy": _PROXY_POISON,
            "http_proxy": _PROXY_POISON,
            "https_proxy": _PROXY_POISON,
            "no_proxy": "",
        }
    )
    for key, value in result.items():
        if key != "PYTHONHASHSEED" and any(
            marker in key.upper() for marker in _SECRET_ENV_MARKERS
        ):
            raise SoftwareValidationError("sanitized environment retained a secret-like key")
        if _value_looks_secret(value):
            raise SoftwareValidationError("sanitized environment retained a secret-like value")
    return result


def _executable_version(path: Path, env: Mapping[str, str]) -> str:
    result = subprocess.run(
        [str(path), "--version"],
        check=False,
        capture_output=True,
        env=dict(env),
        timeout=30,
    )
    raw = (result.stdout + result.stderr).decode("utf-8", errors="strict").strip()
    if result.returncode != 0:
        raise SoftwareValidationError(f"cannot identify executable version: {path}")
    return _require_text(raw, label="executable version", maximum=1024)


def _read_bounded_regular_file(
    path: Path,
    *,
    label: str,
    maximum_bytes: int,
) -> bytes:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise SoftwareValidationError(f"{label} is missing or unreadable") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise SoftwareValidationError(f"{label} must be a regular non-symlink file")
    if metadata.st_size > maximum_bytes:
        raise SoftwareValidationError(f"{label} is oversized")
    try:
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            if not stat.S_ISREG(opened.st_mode) or not os.path.samestat(metadata, opened):
                raise SoftwareValidationError(f"{label} changed before it was read")
            payload = stream.read(maximum_bytes + 1)
            finished = os.fstat(stream.fileno())
    except SoftwareValidationError:
        raise
    except OSError as error:
        raise SoftwareValidationError(f"{label} is unreadable") from error
    if (
        len(payload) > maximum_bytes
        or not os.path.samestat(opened, finished)
        or opened.st_size != finished.st_size
        or len(payload) != finished.st_size
    ):
        raise SoftwareValidationError(f"{label} changed while it was read or is oversized")
    return payload


def _parse_pytest_counts(junit_path: Path, audit_path: Path) -> PytestCounts:
    junit_raw = _read_bounded_regular_file(
        junit_path,
        label="JUnit artifact",
        maximum_bytes=_MAX_JUNIT_BYTES,
    )
    if b"<!DOCTYPE" in junit_raw or b"<!ENTITY" in junit_raw:
        raise SoftwareValidationError("JUnit artifact is unsafe")
    try:
        root = ElementTree.fromstring(junit_raw)
    except ElementTree.ParseError as error:
        raise SoftwareValidationError("JUnit artifact is malformed") from error
    if root.tag not in {"testsuite", "testsuites"}:
        raise SoftwareValidationError("JUnit root must be testsuite(s)")
    summary = root
    if root.tag == "testsuites":
        children = list(root)
        if len(children) != 1 or children[0].tag != "testsuite":
            raise SoftwareValidationError(
                "JUnit testsuites root must contain exactly one direct testsuite"
            )
        summary = children[0]
    elif any(child.tag == "testsuite" for child in summary):
        raise SoftwareValidationError("JUnit testsuite must not contain nested suites")

    count_names = ("tests", "failures", "errors", "skipped")

    def attribute(element: ElementTree.Element, name: str, *, label: str) -> int:
        raw = element.attrib.get(name)
        if raw is None or _PYTEST_COUNT_RE.fullmatch(raw) is None:
            raise SoftwareValidationError(
                f"JUnit {label} {name} count is missing or noncanonical"
            )
        value = int(raw)
        if value > _MAX_PYTEST_COUNT:
            raise SoftwareValidationError(f"JUnit {label} {name} count exceeds policy")
        return value

    counts = {name: attribute(summary, name, label="testsuite") for name in count_names}
    if any(counts[name] > counts["tests"] for name in count_names[1:]) or sum(
        counts[name] for name in count_names[1:]
    ) > counts["tests"]:
        raise SoftwareValidationError("JUnit testsuite outcome counts are inconsistent")

    if root.tag == "testsuites":
        wrapper_fields = tuple(name in root.attrib for name in count_names)
        if any(wrapper_fields):
            if not all(wrapper_fields):
                raise SoftwareValidationError("JUnit wrapper counts must be all present or absent")
            wrapper_counts = {
                name: attribute(root, name, label="testsuites wrapper")
                for name in count_names
            }
            if wrapper_counts != counts:
                raise SoftwareValidationError("JUnit wrapper counts differ from its direct suite")

    allowed_suite_children = frozenset(
        {"properties", "testcase", "system-out", "system-err"}
    )
    allowed_case_children = frozenset(
        {"properties", "failure", "error", "skipped", "system-out", "system-err"}
    )
    testcases: list[ElementTree.Element] = []
    observed_outcomes = {"failures": 0, "errors": 0, "skipped": 0}
    outcome_field = {"failure": "failures", "error": "errors", "skipped": "skipped"}
    for child in summary:
        if child.tag not in allowed_suite_children:
            raise SoftwareValidationError("JUnit testsuite contains an unexpected direct child")
        if child.tag != "testcase":
            continue
        testcases.append(child)
        outcomes = 0
        for detail in child:
            if detail.tag not in allowed_case_children:
                raise SoftwareValidationError("JUnit testcase contains an unexpected direct child")
            field = outcome_field.get(detail.tag)
            if field is not None:
                outcomes += 1
                observed_outcomes[field] += 1
        if outcomes > 1:
            raise SoftwareValidationError("JUnit testcase contains multiple terminal outcomes")
    if len(testcases) != counts["tests"]:
        raise SoftwareValidationError("JUnit testcase count differs from testsuite count")
    if any(observed_outcomes[name] != counts[name] for name in observed_outcomes):
        raise SoftwareValidationError("JUnit testcase outcomes differ from testsuite counts")

    audit_raw = _read_bounded_regular_file(
        audit_path,
        label="pytest audit",
        maximum_bytes=_MAX_PYTEST_AUDIT_BYTES,
    )
    audit_value = _strict_json(audit_raw, label="pytest audit")
    if audit_raw != canonical_json(audit_value).encode("utf-8"):
        raise SoftwareValidationError("pytest audit is not canonical JSON")
    audit = _exact_mapping(
        audit_value,
        frozenset(
            {"deselected", "external_network_attempts", "xfailed", "xpassed"}
        ),
        label="pytest audit",
    )
    external_network_attempts = _require_int(
        audit["external_network_attempts"],
        label="external_network_attempts",
    )
    if external_network_attempts:
        raise SoftwareValidationError("pytest attempted external network access")
    return PytestCounts(
        tests=counts["tests"],
        failures=counts["failures"],
        errors=counts["errors"],
        skipped=counts["skipped"],
        deselected=_require_int(audit["deselected"], label="deselected"),
        xfailed=_require_int(audit["xfailed"], label="xfailed"),
        xpassed=_require_int(audit["xpassed"], label="xpassed"),
        external_network_attempts=external_network_attempts,
    )


_PYTEST_DESELECTED = 0
_PYTEST_XFAILED = 0
_PYTEST_XPASSED = 0
_PYTEST_EXTERNAL_NETWORK_ATTEMPTS = 0
_PYTEST_NETWORK_HOOK_INSTALLED = False


def _is_loopback_host(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, bytes):
        try:
            value = value.decode("ascii")
        except UnicodeDecodeError:
            return False
    if not isinstance(value, str):
        return True
    normalized = value.rstrip(".").casefold()
    if normalized == "localhost":
        return True
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return False
    if address.is_loopback:
        return True
    mapped = getattr(address, "ipv4_mapped", None)
    return bool(mapped is not None and mapped.is_loopback)


def _pytest_network_audit_hook(event: str, args: tuple[object, ...]) -> None:
    global _PYTEST_EXTERNAL_NETWORK_ATTEMPTS
    host: object
    if event in {"socket.connect", "socket.connect_ex"}:
        if len(args) < 2 or not isinstance(args[1], tuple) or not args[1]:
            return
        host = args[1][0]
    elif event in {
        "socket.getaddrinfo",
        "socket.gethostbyaddr",
        "socket.gethostbyname",
    }:
        if not args:
            return
        host = args[0]
    else:
        return
    if _is_loopback_host(host):
        return
    _PYTEST_EXTERNAL_NETWORK_ATTEMPTS += 1
    raise RuntimeError("software validation forbids external network access")


def pytest_configure(config: object) -> None:
    del config
    global _PYTEST_DESELECTED, _PYTEST_EXTERNAL_NETWORK_ATTEMPTS
    global _PYTEST_NETWORK_HOOK_INSTALLED, _PYTEST_XFAILED, _PYTEST_XPASSED
    _PYTEST_DESELECTED = 0
    _PYTEST_EXTERNAL_NETWORK_ATTEMPTS = 0
    _PYTEST_XFAILED = 0
    _PYTEST_XPASSED = 0
    if (
        os.environ.get("HYPERLAB_VALIDATION_PYTEST_AUDIT_PATH") is not None
        and not _PYTEST_NETWORK_HOOK_INSTALLED
    ):
        sys.addaudithook(_pytest_network_audit_hook)
        _PYTEST_NETWORK_HOOK_INSTALLED = True


def pytest_deselected(items: Sequence[object]) -> None:
    global _PYTEST_DESELECTED
    _PYTEST_DESELECTED += len(items)


def pytest_runtest_logreport(report: object) -> None:
    global _PYTEST_XFAILED, _PYTEST_XPASSED
    if getattr(report, "when", None) != "call" or not hasattr(report, "wasxfail"):
        return
    if bool(getattr(report, "skipped", False)):
        _PYTEST_XFAILED += 1
    elif bool(getattr(report, "passed", False)):
        _PYTEST_XPASSED += 1


def pytest_sessionfinish(session: object, exitstatus: object) -> None:
    del session, exitstatus
    raw_path = os.environ.get("HYPERLAB_VALIDATION_PYTEST_AUDIT_PATH")
    if raw_path is None:
        return
    path = Path(raw_path)
    if not path.is_absolute() or path.is_symlink() or path.exists():
        raise SoftwareValidationError("pytest audit path is not fresh and absolute")
    payload = canonical_json(
        {
            "deselected": _PYTEST_DESELECTED,
            "external_network_attempts": _PYTEST_EXTERNAL_NETWORK_ATTEMPTS,
            "xfailed": _PYTEST_XFAILED,
            "xpassed": _PYTEST_XPASSED,
        }
    ).encode("utf-8")
    try:
        with path.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except OSError as error:
        raise SoftwareValidationError("cannot persist pytest audit counters") from error


def _wheel_metadata(path: Path) -> Mapping[str, object]:
    try:
        with zipfile.ZipFile(path) as archive:
            metadata_names = [
                name
                for name in archive.namelist()
                if name.endswith(".dist-info/METADATA") and ".." not in Path(name).parts
            ]
            if len(metadata_names) != 1:
                raise SoftwareValidationError("wheel must contain exactly one METADATA file")
            raw = archive.read(metadata_names[0])
    except (OSError, zipfile.BadZipFile, KeyError) as error:
        raise SoftwareValidationError(f"wheel is unreadable: {path.name}") from error
    if not raw or len(raw) > 4 * 1024 * 1024:
        raise SoftwareValidationError("wheel METADATA size is outside the compiled bound")
    message = BytesParser(policy=compat32).parsebytes(raw)
    name = message.get("Name")
    version = message.get("Version")
    if not isinstance(name, str) or not isinstance(version, str):
        raise SoftwareValidationError("wheel METADATA lacks Name or Version")
    requirements = tuple(sorted(message.get_all("Requires-Dist", [])))
    if any(not isinstance(item, str) or not item.strip() for item in requirements):
        raise SoftwareValidationError("wheel contains a noncanonical Requires-Dist")
    return MappingProxyType(
        {
            "name": _require_text(name, label="wheel Name", maximum=128),
            "requires_dist": list(requirements),
            "version": _require_text(version, label="wheel Version", maximum=128),
        }
    )


def _wheel_record(path: Path, *, output: Path) -> dict[str, object]:
    size, digest = _sha256_file(path)
    return {
        "metadata": dict(_wheel_metadata(path)),
        "relative_path": path.relative_to(output).as_posix(),
        "sha256": digest,
        "size_bytes": size,
    }


_BUILD_OPERATION_IDS = (
    "CREATE_BUILD_ENV",
    "INSTALL_BUILD_LOCK",
    "BUILD_ROOT_WHEEL",
    "BUILD_SERVICE_WHEEL",
    "CREATE_SMOKE_ENV",
    "INSTALL_EXTERNAL_LOCK",
    "INSTALL_LOCAL_WHEELS",
    "VERIFY_IMPORTS",
    "CLI_SMOKE",
)
_IMPORT_VERIFICATION_CODE = (
    "import importlib.metadata as m,json,os,pathlib,sys;"
    "import hyperlab,hyperlab_testnet;"
    "prefix=pathlib.Path(sys.prefix).resolve();"
    "modules={'hyperlab':hyperlab,'hyperlab-testnet-executor':hyperlab_testnet};"
    "records={name:{'origin':str(pathlib.Path(module.__file__).resolve()),"
    "'version':m.version(name)} for name,module in modules.items()};"
    "assert all(pathlib.Path(value['origin']).is_relative_to(prefix) "
    "for value in records.values());"
    "payload={'distributions':records,'python_prefix':str(prefix),'schema_version':1};"
    "target=pathlib.Path(sys.argv[1]);"
    "stream=target.open('x',encoding='utf-8');"
    "stream.write(json.dumps(payload,sort_keys=True,separators=(',',':')));"
    "stream.flush();os.fsync(stream.fileno());stream.close()"
)


def _venv_python_path(environment: Path) -> Path:
    return environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _wheelhouse_inventory(wheelhouse: Path) -> tuple[dict[str, object], ...]:
    if wheelhouse.is_symlink() or not wheelhouse.is_dir():
        raise SoftwareValidationError("the fixed offline validation wheelhouse is unavailable")
    paths = tuple(sorted(wheelhouse.iterdir(), key=lambda item: item.name))
    if not paths or len(paths) > 1_024:
        raise SoftwareValidationError("wheelhouse file count is outside the compiled bound")
    records: list[dict[str, object]] = []
    total_bytes = 0
    for path in paths:
        if path.is_symlink() or not path.is_file() or path.suffix.casefold() != ".whl":
            raise SoftwareValidationError("wheelhouse must contain only regular wheel files")
        size_bytes, digest = _sha256_file(path)
        total_bytes += size_bytes
        if total_bytes > 16 * 1024 * 1024 * 1024:
            raise SoftwareValidationError("wheelhouse exceeds the compiled byte bound")
        records.append(
            {
                "filename": _require_text(path.name, label="wheelhouse filename", maximum=256),
                "sha256": digest,
                "size_bytes": size_bytes,
            }
        )
    return tuple(records)


def _prebuild_operation_specs(
    root: Path,
    service: Path,
    output: Path,
    wheelhouse: Path,
) -> tuple[tuple[str, tuple[str, ...], Path], ...]:
    build_environment = output / "build-env"
    smoke_environment = output / "smoke-env"
    build_python = _venv_python_path(build_environment)
    wheels = output / "wheels"
    common_install = (
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "--no-cache-dir",
        "--no-input",
        "--no-index",
        "--only-binary=:all:",
        "--require-hashes",
        "--no-deps",
        "--find-links",
        str(wheelhouse),
    )
    common_build = (
        "-m",
        "pip",
        "wheel",
        "--disable-pip-version-check",
        "--no-cache-dir",
        "--no-input",
        "--no-index",
        "--no-deps",
        "--no-build-isolation",
        "--wheel-dir",
        str(wheels),
    )
    return (
        (
            "CREATE_BUILD_ENV",
            (sys.executable, "-m", "venv", "--copies", str(build_environment)),
            root,
        ),
        (
            "INSTALL_BUILD_LOCK",
            (
                str(build_python),
                *common_install,
                "--requirement",
                str(service / "requirements-build.lock"),
            ),
            root,
        ),
        ("BUILD_ROOT_WHEEL", (str(build_python), *common_build, str(root)), root),
        (
            "BUILD_SERVICE_WHEEL",
            (str(build_python), *common_build, str(service)),
            root,
        ),
        (
            "CREATE_SMOKE_ENV",
            (sys.executable, "-m", "venv", "--copies", str(smoke_environment)),
            root,
        ),
    )


def _postbuild_operation_specs(
    root: Path,
    service: Path,
    output: Path,
    wheelhouse: Path,
    root_wheel: Path,
    service_wheel: Path,
) -> tuple[tuple[str, tuple[str, ...], Path], ...]:
    smoke_python = _venv_python_path(output / "smoke-env")
    import_result = output / "import-verification.json"
    return (
        (
            "INSTALL_EXTERNAL_LOCK",
            (
                str(smoke_python),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--no-cache-dir",
                "--no-input",
                "--no-index",
                "--only-binary=:all:",
                "--require-hashes",
                "--no-deps",
                "--find-links",
                str(wheelhouse),
                "--requirement",
                str(service / "requirements-external.lock"),
            ),
            root,
        ),
        (
            "INSTALL_LOCAL_WHEELS",
            (
                str(smoke_python),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--no-cache-dir",
                "--no-input",
                "--no-index",
                "--no-deps",
                str(root_wheel),
                str(service_wheel),
            ),
            root,
        ),
        (
            "VERIFY_IMPORTS",
            (str(smoke_python), "-c", _IMPORT_VERIFICATION_CODE, str(import_result)),
            root,
        ),
        (
            "CLI_SMOKE",
            (str(smoke_python), "-m", "hyperlab_testnet.cli", "build-identity"),
            root,
        ),
    )


def _run_build_operation(
    operation_id: str,
    argv: tuple[str, ...],
    cwd: Path,
) -> tuple[dict[str, object], int]:
    print(f"software-validation build operation: {operation_id}", flush=True)
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    result = subprocess.run(argv, cwd=cwd, check=False, env=environment)
    record = {
        "argv": list(argv),
        "cwd": str(cwd),
        "exit_code": result.returncode,
        "operation_id": operation_id,
    }
    return record, result.returncode


def _run_build_isolation(
    repository_root: Path,
    output: Path,
    wheelhouse: Path,
) -> int:
    root, service = _repository_paths(repository_root)
    if output.exists() or output.is_symlink():
        raise SoftwareValidationError("build-isolation output must not already exist")
    expected_wheelhouse = (root / _WHEELHOUSE_RELATIVE).resolve()
    if wheelhouse.resolve() != expected_wheelhouse:
        raise SoftwareValidationError("build isolation requires the compiled wheelhouse path")
    wheelhouse_before = _wheelhouse_inventory(wheelhouse)
    wheels = output / "wheels"
    wheels.mkdir(parents=True, exist_ok=False)
    operations: list[dict[str, object]] = []
    for operation_id, argv, cwd in _prebuild_operation_specs(
        root, service, output, wheelhouse
    ):
        record, exit_code = _run_build_operation(operation_id, argv, cwd)
        operations.append(record)
        if exit_code != 0:
            return exit_code
    wheel_paths = tuple(sorted(wheels.glob("*.whl"), key=lambda item: item.name))
    if (
        len(wheel_paths) != 2
        or any(path.is_symlink() or not path.is_file() for path in wheel_paths)
    ):
        raise SoftwareValidationError("isolated build must create exactly two regular wheels")
    records = [_wheel_record(path, output=output) for path in wheel_paths]
    by_name = {
        cast(str, cast(Mapping[str, object], record["metadata"])["name"]): record
        for record in records
    }
    if len(by_name) != 2 or set(by_name) != {"hyperlab", "hyperlab-testnet-executor"}:
        raise SoftwareValidationError("isolated build produced unexpected distributions")
    service_metadata = cast(
        Mapping[str, object], by_name["hyperlab-testnet-executor"]["metadata"]
    )
    requirements = cast(list[str], service_metadata["requires_dist"])
    if any(
        re.match(r"(?i)^hyperlab(?:\s|\[|@|=|<|>|!|~|;|$)", item) is not None
        for item in requirements
    ):
        raise SoftwareValidationError("service wheel must not declare HyperLab as a dependency")
    root_wheel = output / cast(str, by_name["hyperlab"]["relative_path"])
    service_wheel = output / cast(
        str, by_name["hyperlab-testnet-executor"]["relative_path"]
    )
    for operation_id, argv, cwd in _postbuild_operation_specs(
        root,
        service,
        output,
        wheelhouse,
        root_wheel,
        service_wheel,
    ):
        record, exit_code = _run_build_operation(operation_id, argv, cwd)
        operations.append(record)
        if exit_code != 0:
            return exit_code
    if tuple(record["operation_id"] for record in operations) != _BUILD_OPERATION_IDS:
        raise AssertionError("build-isolation operation order diverged")
    wheelhouse_after = _wheelhouse_inventory(wheelhouse)
    if wheelhouse_after != wheelhouse_before:
        raise SoftwareValidationError("offline wheelhouse changed during validation")
    import_result = output / "import-verification.json"
    import_size, import_digest = _sha256_file(import_result)
    payload = {
        "build_lock_sha256": build_lock_sha256(),
        "external_lock_sha256": external_lock_sha256(),
        "import_verification": {
            "relative_path": import_result.relative_to(output).as_posix(),
            "sha256": import_digest,
            "size_bytes": import_size,
        },
        "operations": operations,
        "root_wheel": by_name["hyperlab"],
        "schema_version": 2,
        "service_has_hyperlab_dependency": False,
        "service_wheel": by_name["hyperlab-testnet-executor"],
        "wheelhouse": {
            "relative_path": _WHEELHOUSE_RELATIVE.as_posix(),
            "wheels": list(wheelhouse_after),
        },
    }
    provenance = output / "build-provenance.json"
    with provenance.open("xb") as stream:
        stream.write(canonical_json(payload).encode("utf-8"))
        stream.flush()
        os.fsync(stream.fileno())
    return 0


def _git_inventory(root: Path) -> tuple[Path, ...]:
    git = _resolve_executable("git")
    result = subprocess.run(
        (str(git), "ls-files", "--cached", "--others", "--exclude-standard", "-z"),
        cwd=root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise SoftwareValidationError("cannot enumerate repository files for conflict scan")
    paths: list[Path] = []
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        try:
            relative = raw.decode("utf-8")
        except UnicodeDecodeError as error:
            raise SoftwareValidationError("Git inventory contains a non-UTF-8 path") from error
        path = root / relative
        if path.is_file() and not path.is_symlink():
            paths.append(path)
    return tuple(paths)


def _conflict_marker_scan(root: Path) -> int:
    findings: list[str] = []
    marker = re.compile(r"^(<<<<<<<|=======|>>>>>>>)")
    for path in _git_inventory(root):
        metadata = path.stat()
        if metadata.st_size > _MAX_CONFLICT_FILE_BYTES:
            continue
        raw = path.read_bytes()
        if b"\0" in raw:
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        for number, line in enumerate(text.splitlines(), start=1):
            if marker.match(line) is not None:
                findings.append(f"{path.relative_to(root).as_posix()}:{number}:{line[:80]}")
    if findings:
        print("\n".join(findings))
        return 1
    print("no conflict markers in tracked or untracked non-ignored text files")
    return 0


def _build_additional_artifacts(
    repository_root: Path,
    output_root: Path,
    provenance_relative: str,
) -> tuple[ValidationArtifact, ...]:
    root, service = _repository_paths(repository_root)
    build_output = output_root / "runtime" / "build-isolation"
    wheelhouse = root / _WHEELHOUSE_RELATIVE
    provenance_path = output_root / provenance_relative
    raw = provenance_path.read_bytes()
    value = _strict_json(raw, label="build provenance")
    if raw != canonical_json(value).encode("utf-8"):
        raise SoftwareValidationError("build provenance is not canonical JSON")
    top = _exact_mapping(
        value,
        frozenset(
            {
                "build_lock_sha256",
                "external_lock_sha256",
                "import_verification",
                "operations",
                "root_wheel",
                "schema_version",
                "service_has_hyperlab_dependency",
                "service_wheel",
                "wheelhouse",
            }
        ),
        label="build provenance",
    )
    if (
        _require_int(top["schema_version"], label="build provenance schema", minimum=1)
        != 2
        or top["service_has_hyperlab_dependency"] is not False
    ):
        raise SoftwareValidationError("build provenance policy flags are invalid")
    if (
        _require_sha256(top["external_lock_sha256"], label="provenance external lock")
        != external_lock_sha256()
        or _require_sha256(top["build_lock_sha256"], label="provenance build lock")
        != build_lock_sha256()
    ):
        raise SoftwareValidationError("build provenance lock hashes differ")
    wheelhouse_value = _exact_mapping(
        top["wheelhouse"],
        frozenset({"relative_path", "wheels"}),
        label="build provenance wheelhouse",
    )
    if (
        _require_text(
            wheelhouse_value["relative_path"],
            label="wheelhouse relative_path",
            maximum=256,
        )
        != _WHEELHOUSE_RELATIVE.as_posix()
        or wheelhouse.resolve() != (root / _WHEELHOUSE_RELATIVE).resolve()
    ):
        raise SoftwareValidationError("build provenance wheelhouse path differs")
    recorded_wheelhouse = wheelhouse_value["wheels"]
    if not isinstance(recorded_wheelhouse, list):
        raise SoftwareValidationError("build provenance wheelhouse inventory is not an array")
    actual_wheelhouse = list(_wheelhouse_inventory(wheelhouse))
    if recorded_wheelhouse != actual_wheelhouse:
        raise SoftwareValidationError("build provenance wheelhouse inventory differs")
    result = [_artifact(output_root, provenance_relative)]
    observed_names: list[str] = []
    built_paths: dict[str, Path] = {}
    built_metadata: dict[str, Mapping[str, object]] = {}
    for key, expected_name in (
        ("root_wheel", "hyperlab"),
        ("service_wheel", "hyperlab-testnet-executor"),
    ):
        wheel = _exact_mapping(
            top[key],
            frozenset({"metadata", "relative_path", "sha256", "size_bytes"}),
            label=key,
        )
        relative = _require_text(wheel["relative_path"], label=f"{key}.relative_path")
        artifact = _artifact(output_root, f"runtime/build-isolation/{relative}")
        if (
            artifact.sha256 != _require_sha256(wheel["sha256"], label=f"{key}.sha256")
            or artifact.size_bytes != _require_int(wheel["size_bytes"], label=f"{key}.size")
        ):
            raise SoftwareValidationError("wheel differs from build provenance")
        metadata = _exact_mapping(
            wheel["metadata"],
            frozenset({"name", "requires_dist", "version"}),
            label=f"{key}.metadata",
        )
        actual_metadata = dict(_wheel_metadata(output_root / artifact.relative_path))
        if dict(metadata) != actual_metadata or metadata["name"] != expected_name:
            raise SoftwareValidationError("wheel METADATA differs from build provenance")
        requirements = metadata["requires_dist"]
        if not isinstance(requirements, list) or any(not isinstance(item, str) for item in requirements):
            raise SoftwareValidationError("wheel Requires-Dist inventory is malformed")
        if key == "service_wheel" and any(
            re.match(r"(?i)^hyperlab(?:\s|\[|@|=|<|>|!|~|;|$)", item) is not None
            for item in requirements
        ):
            raise SoftwareValidationError("service wheel declares a HyperLab dependency")
        observed_names.append(metadata["name"])
        built_paths[expected_name] = output_root / artifact.relative_path
        built_metadata[expected_name] = metadata
        result.append(artifact)
    if observed_names != ["hyperlab", "hyperlab-testnet-executor"]:
        raise SoftwareValidationError("build provenance wheel identities differ")
    import_record = _exact_mapping(
        top["import_verification"],
        frozenset({"relative_path", "sha256", "size_bytes"}),
        label="import verification artifact",
    )
    import_relative = _require_text(
        import_record["relative_path"],
        label="import verification relative_path",
        maximum=256,
    )
    import_artifact = _artifact(
        output_root,
        f"runtime/build-isolation/{import_relative}",
    )
    if (
        import_artifact.sha256
        != _require_sha256(import_record["sha256"], label="import verification sha256")
        or import_artifact.size_bytes
        != _require_int(import_record["size_bytes"], label="import verification size")
    ):
        raise SoftwareValidationError("import verification artifact differs")
    import_path = output_root / import_artifact.relative_path
    import_raw = import_path.read_bytes()
    import_value = _strict_json(import_raw, label="import verification")
    if import_raw != canonical_json(import_value).encode("utf-8"):
        raise SoftwareValidationError("import verification is not canonical JSON")
    import_top = _exact_mapping(
        import_value,
        frozenset({"distributions", "python_prefix", "schema_version"}),
        label="import verification",
    )
    if _require_int(import_top["schema_version"], label="import schema", minimum=1) != 1:
        raise SoftwareValidationError("import verification schema differs")
    smoke_prefix = (build_output / "smoke-env").resolve()
    if (
        _require_text(import_top["python_prefix"], label="import python_prefix")
        != str(smoke_prefix)
    ):
        raise SoftwareValidationError("import verification Python prefix differs")
    distributions = _exact_mapping(
        import_top["distributions"],
        frozenset({"hyperlab", "hyperlab-testnet-executor"}),
        label="import distributions",
    )
    for name in ("hyperlab", "hyperlab-testnet-executor"):
        imported = _exact_mapping(
            distributions[name],
            frozenset({"origin", "version"}),
            label=f"import distribution {name}",
        )
        origin = Path(_require_text(imported["origin"], label=f"{name} origin"))
        try:
            origin.relative_to(smoke_prefix)
        except ValueError as error:
            raise SoftwareValidationError("import origin escapes the fresh smoke venv") from error
        _assert_no_symlinks(smoke_prefix, origin)
        if origin.is_symlink() or not origin.is_file():
            raise SoftwareValidationError("import origin is no longer a regular venv file")
        if imported["version"] != built_metadata[name]["version"]:
            raise SoftwareValidationError("imported distribution version differs from wheel")
    operations_value = top["operations"]
    if not isinstance(operations_value, list):
        raise SoftwareValidationError("build provenance operations must be an array")
    expected_specs = (
        *_prebuild_operation_specs(root, service, build_output, wheelhouse),
        *_postbuild_operation_specs(
            root,
            service,
            build_output,
            wheelhouse,
            built_paths["hyperlab"],
            built_paths["hyperlab-testnet-executor"],
        ),
    )
    expected_operations = [
        {
            "argv": list(argv),
            "cwd": str(cwd),
            "exit_code": 0,
            "operation_id": operation_id,
        }
        for operation_id, argv, cwd in expected_specs
    ]
    if operations_value != expected_operations:
        raise SoftwareValidationError("build provenance operations differ from compiled plan")
    result.append(import_artifact)
    return tuple(result)


def _prepare_output_directory(repository_root: Path, output_directory: Path | str) -> Path:
    _, service = _repository_paths(repository_root)
    evidence_root = service / "evidence"
    if os.path.lexists(evidence_root):
        if evidence_root.is_symlink() or not evidence_root.is_dir():
            raise SoftwareValidationError("service evidence root is unsafe")
    else:
        evidence_root.mkdir(mode=0o700)
    requested = Path(output_directory)
    target = requested if requested.is_absolute() else evidence_root / requested
    if target.parent.resolve() != evidence_root.resolve():
        raise SoftwareValidationError("validation output must be one fresh directory under service evidence")
    if os.path.lexists(target):
        raise FileExistsError(f"validation output already exists: {target}")
    target.mkdir(mode=0o700)
    return target.resolve()


def _git_output(
    git: Path,
    root: Path,
    arguments: Sequence[str],
    env: Mapping[str, str],
) -> str:
    result = subprocess.run(
        (str(git), *arguments),
        cwd=root,
        check=False,
        capture_output=True,
        env=dict(env),
        timeout=30,
    )
    if result.returncode != 0 or result.stderr:
        raise SoftwareValidationError(f"Git identity command failed: {' '.join(arguments)}")
    try:
        value = result.stdout.decode("utf-8").strip()
    except UnicodeDecodeError as error:
        raise SoftwareValidationError("Git identity output is not UTF-8") from error
    return _require_text(value, label="Git identity", maximum=256)


def _repository_identity(
    root: Path,
    git: Path,
    env: Mapping[str, str],
) -> tuple[str, str]:
    head = _require_git_oid(
        _git_output(git, root, ("rev-parse", "HEAD"), env),
        label="repository HEAD",
    )
    branch = _git_output(git, root, ("branch", "--show-current"), env)
    return head, branch


def _assert_output_ignored(
    repository_root: Path,
    output_root: Path,
    git: Path,
    env: Mapping[str, str],
) -> None:
    try:
        relative = output_root.relative_to(repository_root).as_posix()
    except ValueError as error:
        raise SoftwareValidationError("validation output is outside the repository") from error
    result = subprocess.run(
        (str(git), "check-ignore", "--quiet", "--", relative),
        cwd=repository_root,
        check=False,
        capture_output=True,
        env=dict(env),
        timeout=30,
    )
    if result.returncode != 0 or result.stderr:
        raise SoftwareValidationError(
            "validation output must be under the compiled ignored evidence directory"
        )


def _worktree_inventory_sha256(
    repository_root: Path,
    output_root: Path,
    git: Path,
    env: Mapping[str, str],
) -> str:
    result = subprocess.run(
        (
            str(git),
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ),
        cwd=repository_root,
        check=False,
        capture_output=True,
        env=dict(env),
        timeout=120,
    )
    if result.returncode != 0 or result.stderr:
        raise SoftwareValidationError("cannot enumerate the complete validation worktree")
    excluded = output_root.resolve()
    entries: list[dict[str, object]] = []
    observed_paths: set[str] = set()
    observed_files: set[Path] = set()
    for raw_path in result.stdout.split(b"\0"):
        if not raw_path:
            continue
        try:
            relative = raw_path.decode("utf-8")
        except UnicodeDecodeError as error:
            raise SoftwareValidationError("worktree inventory contains a non-UTF-8 path") from error
        canonical = Path(relative)
        if (
            not relative
            or chr(92) in relative
            or canonical.is_absolute()
            or ".." in canonical.parts
            or canonical.as_posix() != relative
            or relative in observed_paths
        ):
            raise SoftwareValidationError("worktree inventory contains a noncanonical path")
        candidate = repository_root / canonical
        try:
            resolved = candidate.resolve(strict=True)
        except OSError as error:
            raise SoftwareValidationError("worktree inventory contains a missing path") from error
        try:
            resolved.relative_to(excluded)
        except ValueError:
            pass
        else:
            continue
        try:
            resolved.relative_to(repository_root)
        except ValueError as error:
            raise SoftwareValidationError("worktree inventory path escapes the repository") from error
        metadata_before = candidate.lstat()
        if (
            candidate.is_symlink()
            or not stat.S_ISREG(metadata_before.st_mode)
            or resolved in observed_files
        ):
            raise SoftwareValidationError(
                "worktree inventory contains a link, duplicate, or non-regular file"
            )
        size_bytes, digest = _sha256_file(candidate)
        metadata_after = candidate.lstat()
        if (
            metadata_before.st_mode != metadata_after.st_mode
            or metadata_before.st_size != metadata_after.st_size
            or metadata_before.st_mtime_ns != metadata_after.st_mtime_ns
        ):
            raise SoftwareValidationError("worktree file changed while inventorying")
        observed_paths.add(relative)
        observed_files.add(resolved)
        entries.append(
            {
                "mode": format(stat.S_IMODE(metadata_after.st_mode), "04o"),
                "path": relative,
                "sha256": digest,
                "size_bytes": size_bytes,
            }
        )
    entries.sort(key=lambda item: cast(str, item["path"]))
    return canonical_sha256({"entries": entries, "schema_version": 1})


def _baseline_is_ancestor(
    root: Path,
    git: Path,
    head: str,
    env: Mapping[str, str],
) -> bool:
    result = subprocess.run(
        (str(git), "merge-base", "--is-ancestor", PHASE13_BASELINE_HEAD, head),
        cwd=root,
        check=False,
        capture_output=True,
        env=dict(env),
        timeout=30,
    )
    if result.stderr or result.returncode not in {0, 1}:
        raise SoftwareValidationError("cannot prove Phase 13 baseline ancestry")
    return result.returncode == 0


def _run_check(
    spec: _CheckSpec,
    *,
    output_root: Path,
    base_env: Mapping[str, str],
    executable_cache: dict[Path, tuple[str, str]],
    timeout_seconds: int,
) -> ValidationCheckRecord:
    executable = Path(spec.argv[0])
    cached = executable_cache.get(executable)
    if cached is None:
        _, executable_hash = _sha256_file(executable)
        cached = (executable_hash, _executable_version(executable, base_env))
        executable_cache[executable] = cached
    stdout_path = output_root / spec.stdout_relative
    stderr_path = output_root / spec.stderr_relative
    environment = dict(base_env)
    if spec.pytest_audit_relative is not None:
        environment["HYPERLAB_VALIDATION_PYTEST_AUDIT_PATH"] = str(
            output_root / spec.pytest_audit_relative
        )
    started = datetime.now(UTC)
    try:
        with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
            result = subprocess.run(
                spec.argv,
                cwd=spec.cwd,
                check=False,
                stdout=cast(BinaryIO, stdout),
                stderr=cast(BinaryIO, stderr),
                env=environment,
                timeout=timeout_seconds,
            )
        exit_code = result.returncode
    except subprocess.TimeoutExpired:
        exit_code = 124
        with stderr_path.open("ab") as stderr:
            stderr.write(b"\nsoftware validation timeout\n")
    except OSError as error:
        exit_code = 125
        with stderr_path.open("ab") as stderr:
            stderr.write(f"\nlocal execution error: {type(error).__name__}\n".encode("ascii"))
    ended = datetime.now(UTC)
    stdout_artifact = _artifact(output_root, spec.stdout_relative)
    stderr_artifact = _artifact(output_root, spec.stderr_relative)
    junit_artifact: ValidationArtifact | None = None
    audit_artifact: ValidationArtifact | None = None
    counts: PytestCounts | None = None
    if spec.junit_relative is not None and spec.pytest_audit_relative is not None:
        junit_path = output_root / spec.junit_relative
        audit_path = output_root / spec.pytest_audit_relative
        counts = _parse_pytest_counts(junit_path, audit_path)
        junit_artifact = _artifact(output_root, spec.junit_relative)
        audit_artifact = _artifact(output_root, spec.pytest_audit_relative)
    additional = (
        _build_additional_artifacts(
            spec.cwd,
            output_root,
            spec.build_provenance_relative,
        )
        if spec.build_provenance_relative is not None and exit_code == 0
        else ()
    )
    passed = exit_code == 0 and (counts is None or counts.clean)
    return ValidationCheckRecord(
        check_id=spec.check_id,
        argv=spec.argv,
        cwd=str(spec.cwd),
        executable_path=str(executable),
        executable_sha256=cached[0],
        executable_version=cached[1],
        started_at=started,
        ended_at=ended,
        exit_code=exit_code,
        stdout=stdout_artifact,
        stderr=stderr_artifact,
        junit=junit_artifact,
        pytest_audit=audit_artifact,
        pytest_counts=counts,
        additional_artifacts=additional,
        passed=passed,
    )


def _write_report(report: SoftwareValidationReport, path: Path) -> None:
    payload = report.to_dict()
    envelope = {
        "report": payload,
        "report_sha256": canonical_sha256(payload),
    }
    encoded = canonical_json(envelope).encode("utf-8")
    try:
        with path.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
    except OSError as error:
        raise SoftwareValidationError("cannot persist software-validation report") from error


def run_testnet_software_validation(
    config: TestnetConfig,
    output_directory: Path | str,
    *,
    repository_root: Path | str | None = None,
    timeout_seconds: int = 7_200,
) -> SoftwareValidationReport:
    """Run the exact compiled local validation plan and persist its canonical report."""

    if not isinstance(config, TestnetConfig):
        raise TypeError("config must be a TestnetConfig")
    if isinstance(timeout_seconds, bool) or not 60 <= timeout_seconds <= 28_800:
        raise ValueError("timeout_seconds must be an integer from 60 through 28800")
    requested_root = Path(repository_root) if repository_root is not None else Path.cwd()
    root, _ = _repository_paths(requested_root)
    python = Path(sys.executable).resolve()
    if python.is_symlink() or not python.is_file():
        raise SoftwareValidationError("current Python executable is not a regular file")
    gate_python = _repository_gate_python(root)
    git = _resolve_executable("git")
    try:
        identity_before = current_testnet_build_identity()
        validate_runtime_identity(config, observed=identity_before)
    except BuildIdentityError as error:
        raise SoftwareValidationError(f"current build identity is invalid: {error}") from error
    _wheelhouse_inventory(root / _WHEELHOUSE_RELATIVE)
    output_root = _prepare_output_directory(root, output_directory)
    base_env = _sanitized_environment(repository_root=root, output_root=output_root)
    _assert_output_ignored(root, output_root, git, base_env)
    repository_head, branch = _repository_identity(root, git, base_env)
    if branch != PHASE13_BASELINE_BRANCH or not _baseline_is_ancestor(
        root,
        git,
        repository_head,
        base_env,
    ):
        raise SoftwareValidationError("validation requires the Phase 13 branch and baseline ancestry")
    inventory_before = _worktree_inventory_sha256(root, output_root, git, base_env)
    started = datetime.now(UTC)
    specs = _expected_check_specs(root, output_root, python, gate_python, git)
    executable_cache: dict[Path, tuple[str, str]] = {}
    checks = tuple(
        _run_check(
            spec,
            output_root=output_root,
            base_env=base_env,
            executable_cache=executable_cache,
            timeout_seconds=timeout_seconds,
        )
        for spec in specs
    )
    identity_after = current_testnet_build_identity()
    ending_head, ending_branch = _repository_identity(root, git, base_env)
    ending_external_lock = external_lock_sha256()
    ending_build_lock = build_lock_sha256()
    inventory_after = _worktree_inventory_sha256(root, output_root, git, base_env)
    identity_stable = identity_after == identity_before
    repository_stable = ending_head == repository_head and ending_branch == branch
    inventory_stable = inventory_after == inventory_before
    passed = (
        all(item.passed for item in checks)
        and identity_stable
        and repository_stable
        and inventory_stable
    )
    validation_id = canonical_sha256(
        {
            "build_hash": identity_before.build_hash,
            "config_hash": config.config_hash,
            "created_at": utc_text(started),
            "repository_head": repository_head,
        }
    )
    report = SoftwareValidationReport(
        validation_id=validation_id,
        created_at=started,
        repository_root=str(root),
        output_root=str(output_root),
        baseline_head=PHASE13_BASELINE_HEAD,
        repository_head=repository_head,
        branch=branch,
        config_subject=config.to_readiness_subject(),
        build_identity_before=_identity_dict(identity_before),
        build_identity_after=_identity_dict(identity_after),
        worktree_inventory_before_sha256=inventory_before,
        worktree_inventory_after_sha256=inventory_after,
        external_lock_sha256=ending_external_lock,
        build_lock_sha256=ending_build_lock,
        python_identity=_python_identity(python),
        checks=checks,
        passed=passed,
    )
    _write_report(report, output_root / REPORT_NAME)
    return report


def _verify_recorded_artifact(output_root: Path, expected: ValidationArtifact) -> None:
    observed = _artifact(output_root, expected.relative_path)
    if observed != expected:
        raise SoftwareValidationError(f"validation artifact changed: {expected.relative_path}")


def _load_envelope(report_path: Path) -> SoftwareValidationReport:
    size, _ = _sha256_file(report_path, maximum=_MAX_REPORT_BYTES)
    if size == 0:
        raise SoftwareValidationError("software-validation report is empty")
    raw = report_path.read_bytes()
    value = _strict_json(raw, label="software-validation report")
    if raw != canonical_json(value).encode("utf-8"):
        raise SoftwareValidationError("software-validation report is not canonical JSON")
    envelope = _exact_mapping(
        value,
        frozenset({"report", "report_sha256"}),
        label="report envelope",
    )
    report_payload = envelope["report"]
    expected_hash = _require_sha256(envelope["report_sha256"], label="report_sha256")
    if canonical_sha256(report_payload) != expected_hash:
        raise SoftwareValidationError("software-validation report hash differs")
    return SoftwareValidationReport.from_dict(report_payload)


def load_testnet_software_validation(
    report_path: Path | str,
    config: TestnetConfig,
    *,
    repository_root: Path | str | None = None,
) -> SoftwareValidationReport:
    """Strictly reload and re-verify passing evidence against current code/config."""

    if not isinstance(config, TestnetConfig):
        raise TypeError("config must be a TestnetConfig")
    path = Path(report_path)
    if path.name != REPORT_NAME or not path.is_absolute() or path.is_symlink():
        raise SoftwareValidationError("report path must be the absolute canonical report path")
    report = _load_envelope(path)
    if not report.passed:
        raise SoftwareValidationError("software-validation report is not a passing report")
    requested_root = (
        Path(repository_root)
        if repository_root is not None
        else Path(report.repository_root)
    )
    root, service = _repository_paths(requested_root)
    output_root = path.parent.resolve()
    if (
        report.repository_root != str(root)
        or report.output_root != str(output_root)
        or output_root.parent.resolve() != (service / "evidence").resolve()
    ):
        raise SoftwareValidationError("report repository/output binding differs")
    python = Path(sys.executable).resolve()
    if python.is_symlink() or not python.is_file():
        raise SoftwareValidationError("current Python executable is not a regular file")
    gate_python = _repository_gate_python(root)
    git = _resolve_executable("git")
    environment = _sanitized_environment(
        repository_root=root,
        output_root=output_root,
        create_directories=False,
    )
    _assert_output_ignored(root, output_root, git, environment)
    head, branch = _repository_identity(root, git, environment)
    if (
        report.baseline_head != PHASE13_BASELINE_HEAD
        or report.repository_head != head
        or report.branch != branch
        or branch != PHASE13_BASELINE_BRANCH
        or not _baseline_is_ancestor(root, git, head, environment)
    ):
        raise SoftwareValidationError("report baseline HEAD or branch differs")
    current_inventory = _worktree_inventory_sha256(root, output_root, git, environment)
    if (
        report.worktree_inventory_before_sha256 != current_inventory
        or report.worktree_inventory_after_sha256 != current_inventory
    ):
        raise SoftwareValidationError("report worktree inventory differs from current")
    try:
        current_identity = current_testnet_build_identity()
        validate_runtime_identity(config, observed=current_identity)
    except BuildIdentityError as error:
        raise SoftwareValidationError(f"current build identity is invalid: {error}") from error
    if (
        dict(report.config_subject) != config.to_readiness_subject()
        or dict(report.build_identity_before) != current_identity.to_dict()
        or dict(report.build_identity_after) != current_identity.to_dict()
    ):
        raise SoftwareValidationError("report source/build/config subject differs from current")
    if (
        report.external_lock_sha256 != external_lock_sha256()
        or report.build_lock_sha256 != build_lock_sha256()
        or dict(report.python_identity) != dict(_python_identity(python))
    ):
        raise SoftwareValidationError("report lock or Python identity differs from current")
    specs = _expected_check_specs(root, output_root, python, gate_python, git)
    executable_cache: dict[Path, tuple[str, str]] = {}
    for check, spec in zip(report.checks, specs, strict=True):
        if (
            check.check_id != spec.check_id
            or check.argv != spec.argv
            or check.cwd != str(spec.cwd)
            or check.stdout.relative_path != spec.stdout_relative
            or check.stderr.relative_path != spec.stderr_relative
            or (check.junit.relative_path if check.junit is not None else None)
            != spec.junit_relative
            or (check.pytest_audit.relative_path if check.pytest_audit is not None else None)
            != spec.pytest_audit_relative
        ):
            raise SoftwareValidationError(f"compiled check differs: {check.check_id}")
        executable = Path(check.executable_path)
        cached = executable_cache.get(executable)
        if cached is None:
            _, digest = _sha256_file(executable)
            cached = (digest, _executable_version(executable, environment))
            executable_cache[executable] = cached
        if check.executable_sha256 != cached[0] or check.executable_version != cached[1]:
            raise SoftwareValidationError("recorded executable identity differs from current")
        for artifact in _check_artifacts(check):
            _verify_recorded_artifact(output_root, artifact)
        if check.pytest_counts is not None:
            assert check.junit is not None and check.pytest_audit is not None
            observed_counts = _parse_pytest_counts(
                output_root / check.junit.relative_path,
                output_root / check.pytest_audit.relative_path,
            )
            if observed_counts != check.pytest_counts or not observed_counts.clean:
                raise SoftwareValidationError("pytest counts differ or are not clean")
        if spec.build_provenance_relative is None:
            if check.additional_artifacts:
                raise SoftwareValidationError("unexpected additional check artifacts")
        else:
            expected_additional = _build_additional_artifacts(
                root,
                output_root,
                spec.build_provenance_relative,
            )
            if check.additional_artifacts != expected_additional:
                raise SoftwareValidationError("build-isolation artifacts differ")
    if any(check.started_at < report.created_at for check in report.checks):
        raise SoftwareValidationError("check timestamp precedes report validation start")
    return report


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Internal HyperLab Testnet validation helpers")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--conflict-scan", action="store_true")
    mode.add_argument("--build-isolation", action="store_true")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--wheelhouse", type=Path)
    return parser.parse_args(argv)


def _main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    root = args.root.resolve()
    if args.conflict_scan:
        if args.output is not None or args.wheelhouse is not None:
            raise SoftwareValidationError(
                "conflict scan does not accept build-isolation paths"
            )
        return _conflict_marker_scan(root)
    if (
        args.output is None
        or not args.output.is_absolute()
        or args.wheelhouse is None
        or not args.wheelhouse.is_absolute()
    ):
        raise SoftwareValidationError(
            "build isolation requires absolute --output and --wheelhouse paths"
        )
    return _run_build_isolation(root, args.output, args.wheelhouse)


__all__ = [
    "CHECK_IDS",
    "PHASE13_BASELINE_BRANCH",
    "PHASE13_BASELINE_HEAD",
    "REPORT_NAME",
    "SOFTWARE_VALIDATION_SCHEMA_VERSION",
    "PytestCounts",
    "SoftwareValidationError",
    "SoftwareValidationReport",
    "ValidationArtifact",
    "ValidationCheckRecord",
    "load_testnet_software_validation",
    "run_testnet_software_validation",
]


if __name__ == "__main__":
    raise SystemExit(_main())
