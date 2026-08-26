from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
from uuid import uuid4

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_CERTIFIER_MODULE = "hyperlab.paper.storage_v4.phase1d_linux_certification"


def _venv_python(root: Path) -> Path:
    if os.name == "nt":
        return root / "Scripts" / "python.exe"
    return root / "bin" / "python"


@contextmanager
def _fresh_venv_parent() -> Iterator[Path]:
    if os.name != "nt":
        with TemporaryDirectory(prefix="hyperlab-phase1d-import-test-") as temporary:
            yield Path(temporary)
        return
    root = Path(gettempdir()) / f"hyperlab-phase1d-import-test-{uuid4().hex}"
    powershell = Path(os.environ["SYSTEMROOT"]) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
    powershell_environment = os.environ.copy()
    powershell_environment["HYPERLAB_PHASE1D_TEST_ROOT"] = str(root)
    subprocess.run(
        [
            str(powershell),
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "New-Item -ItemType Directory -Path $env:HYPERLAB_PHASE1D_TEST_ROOT | Out-Null",
        ],
        check=True,
        env=powershell_environment,
        timeout=30,
    )
    try:
        yield root
    finally:
        subprocess.run(
            [
                str(powershell),
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                (
                    "$target=[IO.Path]::GetFullPath($env:HYPERLAB_PHASE1D_TEST_ROOT);"
                    "$temp=[IO.Path]::GetFullPath([IO.Path]::GetTempPath());"
                    "if(-not $target.StartsWith($temp,[StringComparison]::OrdinalIgnoreCase)){exit 65};"
                    "Remove-Item -LiteralPath $target -Recurse -Force"
                ),
            ],
            check=True,
            env=powershell_environment,
            timeout=30,
        )


def test_phase1d_certifier_imports_in_fresh_without_pip_venv() -> None:
    with _fresh_venv_parent() as temporary:
        environment_root = temporary / "stdlib-only-venv"
        created = subprocess.run(
            [sys.executable, "-m", "venv", "--without-pip", str(environment_root)],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert created.returncode == 0, created.stdout + created.stderr
        python = _venv_python(environment_root)
        environment = os.environ.copy()
        environment.pop("PYTHONHOME", None)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        environment["PYTHONNOUSERSITE"] = "1"
        environment["PYTHONPATH"] = str(_REPOSITORY_ROOT / "src")
        probe = "\n".join(
            (
                "import importlib.util",
                "import sys",
                "assert importlib.util.find_spec('pip') is None",
                "assert importlib.util.find_spec('pandas') is None",
                f"from {_CERTIFIER_MODULE} import PHASE1D_ARTIFACT, main",
                "assert PHASE1D_ARTIFACT == "
                "'STORAGE_V4_PHASE1D_LINUX_EXT4_OFFLINE_CERTIFICATION_V1'",
                "assert callable(main)",
                "assert 'hyperlab.paper.carry_strategy' not in sys.modules",
                "assert 'hyperlab.backtest' not in sys.modules",
                "print('PHASE1D_STDLIB_IMPORT_OK')",
            )
        )

        completed = subprocess.run(
            [str(python), "-c", probe],
            cwd=_REPOSITORY_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert completed.stdout.strip() == "PHASE1D_STDLIB_IMPORT_OK"


def test_existing_paper_package_exports_remain_resolvable() -> None:
    import hyperlab.paper as paper

    assert set(paper.__all__) <= set(dir(paper))
    for name in paper.__all__:
        assert getattr(paper, name) is not None


def test_existing_storage_v4_package_exports_remain_resolvable() -> None:
    import hyperlab.paper.storage_v4 as storage_v4

    assert set(storage_v4.__all__) <= set(dir(storage_v4))
    for name in storage_v4.__all__:
        assert getattr(storage_v4, name) is not None


def test_launcher_import_preflight_precedes_detached_process() -> None:
    launcher = (
        _REPOSITORY_ROOT / "ops" / "storage_v4_phase1d" / "run_offline_certification.sh"
    ).read_text(encoding="utf-8")

    marker = "PHASE1D_IMPORT_PREFLIGHT"
    assert marker in launcher
    assert launcher.index(marker) < launcher.index("nohup setsid")
    assert "no certifier process was launched" in launcher
    assert f"from {_CERTIFIER_MODULE} import main" in launcher
