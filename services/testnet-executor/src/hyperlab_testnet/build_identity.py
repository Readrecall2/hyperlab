"""Runtime-verifiable identity for the isolated Testnet executor build.

The authorization subject must describe the code that is actually running.  A
caller-provided hash is therefore insufficient: this module hashes every Python
source file in both the dedicated executor and the shared HyperLab package, plus
the exact versions of the small dependency set that can affect signing or
transport behavior.
"""

from __future__ import annotations

import hashlib
import importlib
import os
import platform
import re
import site
import stat
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from types import MappingProxyType

import hyperlab
from hyperlab.environment_authorization import REAL_MONEY_EXECUTION_ENABLED_IN_BUILD

from .canonical import canonical_sha256
from .config import (
    TESTNET_CHAIN_IDENTITY,
    TESTNET_HTTP_ENDPOINT,
    TESTNET_WS_ENDPOINT,
    TestnetConfig,
)

TESTNET_SOURCE_IDENTITY = "hyperliquid-testnet-public"
TESTNET_MANUAL_STRATEGY_NAME = "manual-testnet-smoke"

_DEPENDENCIES = (
    "eth-account",
    "hyperlab",
    "hyperliquid-python-sdk",
    "requests",
    "rich",
    "typer",
    "websocket-client",
)
_LOCK_PIN_RE = re.compile(
    r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s\\]+)\s*\\$"
)
_MAX_SOURCE_FILES = 2_000
_MAX_SOURCE_BYTES = 8 * 1024 * 1024
_MAX_DEPENDENCY_FILES = 20_000
_MAX_DEPENDENCY_BYTES = 256 * 1024 * 1024
_RUNTIME_IMPORTS = (
    ("eth_account", "eth-account"),
    ("hyperliquid.utils.signing", "hyperliquid-python-sdk"),
    ("hyperliquid.utils.types", "hyperliquid-python-sdk"),
    ("requests", "requests"),
    ("typer", "typer"),
    ("websocket", "websocket-client"),
)
_SHARED_RUNTIME_IMPORTS = (
    "hyperlab.environment_authorization",
)
_LOCK_NAMES = frozenset({"requirements-build.lock", "requirements-external.lock"})


class BuildIdentityError(RuntimeError):
    """The running implementation cannot be bound to an exact authorization subject."""


@dataclass(frozen=True, slots=True)
class TestnetBuildIdentity:
    build_hash: str
    source_hash: str
    strategy_hash: str
    source_identity: str = TESTNET_SOURCE_IDENTITY
    strategy_name: str = TESTNET_MANUAL_STRATEGY_NAME

    def to_dict(self) -> dict[str, str]:
        return {
            "build_hash": self.build_hash,
            "source_hash": self.source_hash,
            "source_identity": self.source_identity,
            "strategy_hash": self.strategy_hash,
            "strategy_name": self.strategy_name,
        }


def _source_root(module_file: str | None, *, label: str) -> Path:
    if module_file is None:
        raise BuildIdentityError(f"{label} source root is unavailable")
    root = Path(module_file).resolve().parent
    if not root.is_dir() or root.is_symlink():
        raise BuildIdentityError(f"{label} source root is not a regular directory")
    return root


def _hash_python_tree(root: Path, *, namespace: str) -> Mapping[str, str]:
    result: dict[str, str] = {}
    total_bytes = 0
    paths = sorted(root.rglob("*.py"), key=lambda item: item.as_posix())
    if not paths or len(paths) > _MAX_SOURCE_FILES:
        raise BuildIdentityError(f"{namespace} source file count is outside the compiled bound")
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise BuildIdentityError(f"{namespace} source contains a non-regular Python file")
        try:
            payload = path.read_bytes()
        except OSError as error:
            raise BuildIdentityError(
                f"{namespace} source cannot be read ({type(error).__name__})"
            ) from None
        total_bytes += len(payload)
        if total_bytes > _MAX_SOURCE_BYTES:
            raise BuildIdentityError(f"{namespace} source exceeds the compiled byte bound")
        relative = path.relative_to(root).as_posix()
        result[f"{namespace}/{relative}"] = hashlib.sha256(payload).hexdigest()
    return MappingProxyType(result)


def _dependency_versions() -> Mapping[str, str]:
    versions: dict[str, str] = {}
    for distribution in _DEPENDENCIES:
        try:
            value = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            raise BuildIdentityError(
                f"required Testnet distribution is unavailable: {distribution}"
            ) from None
        if not value or value != value.strip():
            raise BuildIdentityError("a Testnet dependency version is not canonical")
        versions[distribution] = value
    return MappingProxyType(versions)


def _required_lock_path(name: str) -> Path:
    if name not in _LOCK_NAMES:
        raise BuildIdentityError("the requested dependency lock name is not compiled")
    module_path = Path(__file__).resolve()
    candidates = (
        module_path.parents[2] / name,
        module_path.parent / "locks" / name,
    )
    for path in candidates:
        if not os.path.lexists(path):
            continue
        try:
            metadata = path.lstat()
        except OSError as error:
            raise BuildIdentityError(
                f"the hash-locked dependency file cannot be inspected: {name}"
            ) from error
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise BuildIdentityError(
                f"the hash-locked dependency file is not a regular file: {name}"
            )
        return path
    raise BuildIdentityError(f"the hash-locked dependency file is unavailable: {name}")


def external_lock_path() -> Path:
    return _required_lock_path("requirements-external.lock")


def build_lock_path() -> Path:
    return _required_lock_path("requirements-build.lock")


def _lock_sha256(path: Path) -> str:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise BuildIdentityError(
            f"the dependency lock cannot be read ({type(error).__name__})"
        ) from None
    if not payload or len(payload) > 8 * 1024 * 1024:
        raise BuildIdentityError("the dependency lock size is outside the compiled bound")
    return hashlib.sha256(payload).hexdigest()


def external_lock_sha256() -> str:
    return _lock_sha256(external_lock_path())


def build_lock_sha256() -> str:
    return _lock_sha256(build_lock_path())


def _canonical_distribution_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _locked_dependency_versions() -> Mapping[str, str]:
    try:
        lines = external_lock_path().read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise BuildIdentityError(
            f"the dependency lock cannot be parsed ({type(error).__name__})"
        ) from None
    result: dict[str, str] = {}
    for line in lines:
        match = _LOCK_PIN_RE.fullmatch(line)
        if match is None:
            if line and not line[0].isspace() and "==" in line:
                raise BuildIdentityError("the dependency lock contains a noncanonical pin")
            continue
        raw_name, version = match.groups()
        name = _canonical_distribution_name(raw_name)
        if name in result:
            raise BuildIdentityError("the dependency lock contains a duplicate distribution")
        result[name] = version
    if not 10 <= len(result) <= 200:
        raise BuildIdentityError("the dependency lock distribution count is outside the bound")
    return MappingProxyType(result)


def _dependency_artifacts(locked: Mapping[str, str]) -> Mapping[str, str]:
    artifacts: dict[str, str] = {}
    total_bytes = 0
    file_count = 0
    for name, expected_version in sorted(locked.items()):
        try:
            distribution = metadata.distribution(name)
        except metadata.PackageNotFoundError:
            raise BuildIdentityError(
                f"locked Testnet distribution is unavailable: {name}"
            ) from None
        if distribution.version != expected_version:
            raise BuildIdentityError(
                f"locked Testnet distribution version differs: {name}"
            )
        files = distribution.files
        if files is None:
            raise BuildIdentityError(f"locked distribution has no file inventory: {name}")
        included = 0
        for package_path in sorted(files, key=lambda item: str(item).replace("\\", "/")):
            relative = str(package_path).replace("\\", "/")
            parts = tuple(part for part in relative.split("/") if part)
            lowered = relative.lower()
            if (
                not parts
                or ".." in parts
                or "__pycache__" in parts
                or lowered.endswith(".pyc")
                or parts[-1] in {"INSTALLER", "RECORD", "REQUESTED", "direct_url.json"}
            ):
                continue
            path = Path(str(distribution.locate_file(package_path)))
            if path.is_symlink() or not path.is_file():
                raise BuildIdentityError(
                    f"locked distribution contains a non-regular artifact: {name}"
                )
            try:
                payload = path.read_bytes()
            except OSError as error:
                raise BuildIdentityError(
                    f"locked dependency artifact cannot be read ({type(error).__name__})"
                ) from None
            file_count += 1
            included += 1
            total_bytes += len(payload)
            if file_count > _MAX_DEPENDENCY_FILES or total_bytes > _MAX_DEPENDENCY_BYTES:
                raise BuildIdentityError("dependency artifacts exceed the compiled bound")
            artifacts[f"{name}/{relative}"] = hashlib.sha256(payload).hexdigest()
        if included == 0:
            raise BuildIdentityError(f"locked distribution has no bindable artifacts: {name}")
    return MappingProxyType(artifacts)


def _distribution_regular_files(name: str) -> frozenset[Path]:
    try:
        distribution = metadata.distribution(name)
    except metadata.PackageNotFoundError:
        raise BuildIdentityError(f"runtime distribution is unavailable: {name}") from None
    files = distribution.files
    if files is None:
        raise BuildIdentityError(f"runtime distribution has no file inventory: {name}")
    result: set[Path] = set()
    for package_path in files:
        relative = str(package_path).replace("\\", "/")
        if ".." in relative.split("/"):
            continue
        path = Path(str(distribution.locate_file(package_path))).resolve()
        if path.is_file() and not path.is_symlink():
            result.add(path)
    return frozenset(result)


def _runtime_import_artifacts() -> Mapping[str, str]:
    artifacts: dict[str, str] = {}
    inventories: dict[str, frozenset[Path]] = {}
    for module_name, distribution_name in _RUNTIME_IMPORTS:
        try:
            module = importlib.import_module(module_name)
        except Exception as error:
            raise BuildIdentityError(
                f"runtime import is unavailable ({module_name}: {type(error).__name__})"
            ) from None
        module_file = getattr(module, "__file__", None)
        if not isinstance(module_file, str) or not module_file:
            raise BuildIdentityError(f"runtime import has no regular origin: {module_name}")
        path = Path(module_file).resolve()
        if path.is_symlink() or not path.is_file():
            raise BuildIdentityError(f"runtime import origin is not regular: {module_name}")
        inventory = inventories.setdefault(
            distribution_name,
            _distribution_regular_files(distribution_name),
        )
        if path not in inventory:
            raise BuildIdentityError(
                f"runtime import origin is outside its locked distribution: {module_name}"
            )
        try:
            payload = path.read_bytes()
        except OSError as error:
            raise BuildIdentityError(
                f"runtime import origin cannot be read ({type(error).__name__})"
            ) from None
        artifacts[module_name] = hashlib.sha256(payload).hexdigest()
    shared_root = _source_root(hyperlab.__file__, label="HyperLab")
    for module_name in _SHARED_RUNTIME_IMPORTS:
        module = importlib.import_module(module_name)
        module_file = getattr(module, "__file__", None)
        if not isinstance(module_file, str) or not module_file:
            raise BuildIdentityError(f"shared runtime import has no origin: {module_name}")
        raw_path = Path(module_file)
        path = raw_path.resolve()
        try:
            path.relative_to(shared_root)
        except ValueError:
            raise BuildIdentityError(
                f"shared runtime import is outside the bound HyperLab source: {module_name}"
            ) from None
        if raw_path.is_symlink() or not path.is_file():
            raise BuildIdentityError(f"shared runtime import origin is not regular: {module_name}")
        artifacts[module_name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return MappingProxyType(artifacts)


def validate_runtime_process_boundary() -> Mapping[str, str]:
    """Reject common interpreter/path injection before any credential is read."""

    for name in ("PYTHONHOME", "PYTHONPATH", "PYTHONUSERBASE"):
        if os.environ.get(name):
            raise BuildIdentityError(f"runtime interpreter injection variable is forbidden: {name}")
    if sys.prefix == sys.base_prefix or site.ENABLE_USER_SITE is not False:
        raise BuildIdentityError("Testnet execution requires a dedicated venv with user site disabled")
    if "sitecustomize" in sys.modules or "usercustomize" in sys.modules:
        raise BuildIdentityError("runtime customization modules are forbidden")
    for value in sys.path:
        if not isinstance(value, str) or not value or not Path(value).is_absolute():
            raise BuildIdentityError("runtime sys.path must contain only explicit absolute paths")
    return _runtime_import_artifacts()


def _python_runtime_identity() -> Mapping[str, str]:
    implementation = sys.implementation
    return MappingProxyType(
        {
            "abi_cache_tag": implementation.cache_tag or "",
            "implementation": implementation.name,
            "machine": platform.machine(),
            "platform": sys.platform,
            "version": platform.python_version(),
        }
    )


def expected_source_hash() -> str:
    return canonical_sha256(
        {
            "chain_identity": TESTNET_CHAIN_IDENTITY,
            "http_endpoint": TESTNET_HTTP_ENDPOINT,
            "info_reads": [
                "allMids",
                "clearinghouseState",
                "extraAgents",
                "meta",
                "openOrders",
                "orderStatus",
                "spotClearinghouseState",
                "userFillsByTime",
                "userRole",
            ],
            "schema_version": 1,
            "source_identity": TESTNET_SOURCE_IDENTITY,
            "websocket_endpoint": TESTNET_WS_ENDPOINT,
        }
    )


def expected_strategy_hash() -> str:
    return canonical_sha256(
        {
            "autonomous_order_generation": False,
            "confirmation_literal": "TESTNET-ORDER",
            "mode": "MANUAL_SINGLE_INTENT_ONLY",
            "order_type": "LIMIT",
            "schema_version": 1,
            "strategy_name": TESTNET_MANUAL_STRATEGY_NAME,
        }
    )


def current_testnet_build_identity() -> TestnetBuildIdentity:
    if REAL_MONEY_EXECUTION_ENABLED_IN_BUILD:
        raise BuildIdentityError("real-money execution is forbidden in the Testnet build")
    service_root = Path(__file__).resolve().parent
    if service_root.is_symlink():
        raise BuildIdentityError("Testnet executor source root cannot be a symbolic link")
    shared_root = _source_root(hyperlab.__file__, label="HyperLab")
    sources = {
        **_hash_python_tree(shared_root, namespace="hyperlab"),
        **_hash_python_tree(service_root, namespace="hyperlab_testnet"),
    }
    locked_dependencies = _locked_dependency_versions()
    direct_versions = _dependency_versions()
    for distribution, expected_version in direct_versions.items():
        if distribution == "hyperlab":
            continue
        locked_version = locked_dependencies.get(
            _canonical_distribution_name(distribution)
        )
        if locked_version != expected_version:
            raise BuildIdentityError(
                f"direct Testnet dependency is not pinned exactly: {distribution}"
            )
    build_hash = canonical_sha256(
        {
            "dependencies": dict(direct_versions),
            "build_lock_sha256": build_lock_sha256(),
            "dependency_artifacts": dict(_dependency_artifacts(locked_dependencies)),
            "dependency_lock_sha256": external_lock_sha256(),
            "locked_dependencies": dict(locked_dependencies),
            "python_runtime": dict(_python_runtime_identity()),
            "real_money_execution_enabled": False,
            "runtime_import_artifacts": dict(_runtime_import_artifacts()),
            "schema_version": 2,
            "sources": sources,
        }
    )
    return TestnetBuildIdentity(
        build_hash=build_hash,
        source_hash=expected_source_hash(),
        strategy_hash=expected_strategy_hash(),
    )


def validate_runtime_identity(
    config: TestnetConfig,
    *,
    observed: TestnetBuildIdentity | None = None,
) -> TestnetBuildIdentity:
    if not isinstance(config, TestnetConfig):
        raise TypeError("config must be a TestnetConfig")
    identity = current_testnet_build_identity() if observed is None else observed
    expected = identity.to_dict()
    actual = {
        "build_hash": config.build_hash,
        "source_hash": config.source_hash,
        "source_identity": config.source_identity,
        "strategy_hash": config.strategy_hash,
        "strategy_name": config.strategy_name,
    }
    if actual != expected:
        raise BuildIdentityError("Testnet configuration does not match the running build identity")
    return identity


__all__ = [
    "TESTNET_MANUAL_STRATEGY_NAME",
    "TESTNET_SOURCE_IDENTITY",
    "BuildIdentityError",
    "TestnetBuildIdentity",
    "build_lock_path",
    "build_lock_sha256",
    "current_testnet_build_identity",
    "expected_source_hash",
    "expected_strategy_hash",
    "external_lock_path",
    "external_lock_sha256",
    "validate_runtime_identity",
    "validate_runtime_process_boundary",
]
