"""Bounded regression checks for the one-shot successor baseline capture."""

from __future__ import annotations

import ast
import hashlib
import json
import subprocess
import sys
from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _REPOSITORY_ROOT / "scripts" / "capture_phase1c_successor_baseline.py"
_WITNESS = (
    _REPOSITORY_ROOT
    / "config"
    / "paper"
    / "storage-v4-phase1c-successor-baseline-byte-witness.json"
)
_WITNESS_SHA256 = (
    "32c30490eb3a9934165a67fd76b0127fb698316710fdadd67b08be081335c740"
)
_GLOBAL_SHA256 = (
    "fa0e55fb4a42488eaa52a69355909c578f45994c64e2849df3e859a0089c5936"
)
_CLOSURE_SHA256 = (
    "e8bc7f8f4e3fce05bbb5681b95963414cea6d26a0de813d3f22a39a30a0c9bb7"
)


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def test_baseline_capture_imports_only_the_standard_library() -> None:
    source = _SCRIPT.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(_SCRIPT))
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported.update(
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    )
    assert not {
        name for name in imported if name == "hyperlab" or name.startswith("hyperlab.")
    }

    probe = (
        "from importlib.abc import MetaPathFinder;"
        "import runpy,sys;"
        "R=type('R',(MetaPathFinder,),"
        "{'find_spec':lambda self,name,path=None,target=None:"
        "(_ for _ in ()).throw(RuntimeError(name))"
        " if name=='hyperlab' or name.startswith('hyperlab.') else None});"
        "sys.meta_path.insert(0,R());"
        "runpy.run_path(sys.argv[1],run_name='phase1c_capture_import_probe')"
    )
    result = subprocess.run(
        (sys.executable, "-I", "-S", "-c", probe, str(_SCRIPT)),
        check=False,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_acquired_baseline_witness_remains_exact_and_canonical() -> None:
    data = _WITNESS.read_bytes()
    payload = json.loads(data)
    assert data == _canonical_json_bytes(payload)
    assert hashlib.sha256(data).hexdigest() == _WITNESS_SHA256

    global_identity = payload["acquired_verifier_global_identity"]
    global_without_sha256 = {
        key: value for key, value in global_identity.items() if key != "sha256"
    }
    assert len(global_identity["files"]) == 138
    assert (
        hashlib.sha256(_canonical_json_bytes(global_without_sha256)).hexdigest()
        == _GLOBAL_SHA256
    )

    closure = payload["producer_dependency_closure"]
    assert closure["file_count"] == len(closure["files"]) == 104
    assert (
        hashlib.sha256(_canonical_json_bytes(closure["files"])).hexdigest()
        == _CLOSURE_SHA256
    )
    global_files = global_identity["files"]
    assert all(
        global_files[item["path"]]["sha256"] == item["sha256"]
        for item in closure["files"]
    )
