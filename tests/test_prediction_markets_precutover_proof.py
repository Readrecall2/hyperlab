from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

from ops.prediction_markets_launch_v1 import launch_pack, precutover_proof

ROOT = Path(__file__).resolve().parents[1]
OPS = ROOT / "ops" / "prediction_markets_launch_v1"
AUDIT_BRANCH = "codex/prediction-markets-v3-independent-audit"


def _run_git(*arguments: str, cwd: Path | None = None) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        capture_output=True,
        check=False,
        cwd=cwd,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.strip()


def _write_command(root: Path, name: str, body: str) -> None:
    path = root / name
    path.write_text(f"#!/usr/bin/bash\n{body}", encoding="utf-8", newline="\n")
    path.chmod(0o700)


def _git_bash() -> Path:
    candidate = Path(os.environ.get("PROGRAMFILES", r"C:\Program Files")) / "Git" / "bin" / "bash.exe"
    if not candidate.is_file():
        pytest.skip("Git Bash is unavailable")
    return candidate


def _bash_path(path: Path) -> str:
    resolved = path.resolve(strict=False)
    if os.name == "nt":
        drive = resolved.drive.rstrip(":").lower()
        return f"/{drive}/{resolved.as_posix().split(':', 1)[1].lstrip('/')}"
    return resolved.as_posix()


def _synthetic_runtime_pack(tmp_path: Path) -> tuple[Path, Path, str]:
    repository = tmp_path / "synthetic-source"
    proof_target = repository / "ops" / "prediction_markets_launch_v1" / "precutover_proof.py"
    proof_target.parent.mkdir(parents=True)
    shutil.copy2(OPS / "precutover_proof.py", proof_target)
    (repository / "SYNTHETIC_FIXTURE.txt").write_text(
        "SYNTHETIC/FIXTURE only; no Linux or economic evidence.\n",
        encoding="utf-8",
    )
    _run_git("init", "--quiet", cwd=repository)
    _run_git("config", "user.email", "synthetic-fixture@invalid.example", cwd=repository)
    _run_git("config", "user.name", "Synthetic Fixture", cwd=repository)
    _run_git("add", ".", cwd=repository)
    _run_git("commit", "--quiet", "-m", "synthetic proof source", cwd=repository)
    commit = _run_git("rev-parse", "HEAD", cwd=repository)
    _run_git("branch", AUDIT_BRANCH, commit, cwd=repository)

    pack = tmp_path / "synthetic-runtime-pack"
    scripts = pack / "scripts"
    operator = pack / "operator"
    wheelhouse = pack / "wheelhouse"
    scripts.mkdir(parents=True)
    operator.mkdir()
    wheelhouse.mkdir()
    bundle = pack / precutover_proof._BUNDLE_NAME
    _run_git("bundle", "create", str(bundle), f"refs/heads/{AUDIT_BRANCH}", cwd=repository)
    wheel = wheelhouse / "fixture-1.0-py3-none-any.whl"
    wheel.write_bytes(b"SYNTHETIC/FIXTURE wheel bytes")
    wheelhouse_payload = (
        f"{launch_pack.sha256_file(wheel)}  {wheel.name}\n".encode("ascii")
    )
    (pack / "wheelhouse.sha256").write_bytes(wheelhouse_payload)
    source_inventory = launch_pack.build_source_inventory(repository, commit)
    (pack / "source-inventory.json").write_bytes(
        launch_pack.canonical_json_bytes(source_inventory) + b"\n"
    )
    for name in launch_pack._SCRIPTS:
        source = OPS / name
        target = scripts / name
        if source.is_file():
            shutil.copy2(source, target)
        else:
            target.write_text("# SYNTHETIC/FIXTURE\n", encoding="utf-8")
    for name in (
        "B-tabby-preflight-install-activate.sh",
        "C-tabby-readonly-monitor.sh",
        "E-recovery-rollback.sh",
    ):
        (operator / name).write_text(
            "#!/usr/bin/env bash\nprintf 'SYNTHETIC/FIXTURE only\\n'\n",
            encoding="utf-8",
            newline="\n",
        )
    slug = "pm-20260828t210000z-cafefeed"
    volume = "/mnt/HC_Volume_106716684/hyperlab-prediction-markets"
    incoming = f"/home/hyperlab/hyperlab-prediction-markets/incoming/{slug}"
    handoff: dict[str, object] = {
        "boundary": precutover_proof.BOUNDARY,
        "bundle_filename": bundle.name,
        "bundle_sha256": launch_pack.sha256_file(bundle),
        "campaign_root": f"{volume}/campaigns/{slug}",
        "disk": {"required_free_bytes": 194_347_270_144},
        "incoming_root": incoming,
        "run_slug": slug,
        "schema_version": 1,
        "source_commit": commit,
        "source_inventory_sha256": source_inventory["inventory_sha256"],
        "source_root": f"{volume}/sources/{slug}",
        "volume_base": volume,
        "volume_mount": "/mnt/HC_Volume_106716684",
        "wheelhouse_manifest_sha256": launch_pack.sha256_bytes(wheelhouse_payload),
    }
    transfer_paths = [
        bundle.name,
        "source-inventory.json",
        "wheelhouse.sha256",
        *[f"scripts/{name}" for name in launch_pack._SCRIPTS],
        *[f"operator/{name}" for name in (
            "B-tabby-preflight-install-activate.sh",
            "C-tabby-readonly-monitor.sh",
            "E-recovery-rollback.sh",
        )],
    ]
    transfer = launch_pack._transfer_inventory(pack, transfer_paths)
    transfer_raw = launch_pack.canonical_json_bytes(transfer) + b"\n"
    (pack / "transfer-inventory.json").write_bytes(transfer_raw)
    handoff["transfer_inventory_sha256"] = launch_pack.sha256_bytes(transfer_raw)
    handoff_raw = launch_pack.canonical_json_bytes(handoff) + b"\n"
    (pack / "handoff.json").write_bytes(handoff_raw)
    (pack / "handoff.sha256").write_text(
        f"{launch_pack.sha256_bytes(handoff_raw)}  handoff.json\n",
        encoding="ascii",
    )
    return repository, pack, commit


def test_proof_pack_finalizes_authenticates_and_verifies_retrieved_output(
    tmp_path: Path,
) -> None:
    repository, runtime_pack, commit = _synthetic_runtime_pack(tmp_path)
    proof_pack = tmp_path / "proof-pack"
    result = precutover_proof.finalize(
        repo_root=repository.resolve(strict=True),
        runtime_pack=runtime_pack.resolve(strict=True),
        output_root=proof_pack,
        source_commit=commit,
        expected_branch=AUDIT_BRANCH,
    )
    assert result["terminal_signal"] == (
        "PREDICTION_MARKETS_LINUX_PRECUTOVER_PROOF_PACK_V2_GREEN_"
        "OS_RELEASE_LAYOUT_FIXED_AWAITING_HUMAN_EXECUTION"
    )
    verified = precutover_proof.verify_input(proof_pack.resolve(strict=True))
    assert verified["source_commit"] == commit
    assert verified["wheels"] == 1
    assert not (proof_pack / "systemd").exists()
    assert not (proof_pack / "scripts" / "cutover.sh").exists()
    assert not (proof_pack / "scripts" / "install.sh").exists()
    production_b0 = (
        proof_pack / "operator" / "B0-linux-precutover-proof.sh"
    ).read_text(encoding="utf-8")
    assert "/usr/bin/env -i HOME=/home/hyperlab PATH=/usr/bin:/bin" in production_b0
    assert "GIT_ALLOW_PROTOCOL=file" in production_b0
    assert production_b0.index("verify-linux-environment") < production_b0.index(
        "git clone --no-checkout"
    )

    manifest, manifest_digest = precutover_proof._proof_manifest(proof_pack)
    evidence = tmp_path / "retrieved-evidence"
    evidence.mkdir()
    runtime = {"terminal_signal": "PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN"}
    runtime_raw = precutover_proof.canonical_json_bytes(runtime) + b"\n"
    (evidence / precutover_proof.RUNTIME_REPORT).write_bytes(runtime_raw)
    body: dict[str, object] = {
        "boundary": precutover_proof.BOUNDARY,
        "bundle": {},
        "campaign_root_absent": True,
        "command_contract": {},
        "environment": {"os_id": "ubuntu"},
        "expected_branch": AUDIT_BRANCH,
        "input_inventory_sha256": verified["input_inventory_sha256"],
        "manifest_sha256": manifest_digest,
        "neutral_cwd": f"{manifest['incoming_root']}/{precutover_proof.NEUTRAL_CWD}",
        "proof_id": precutover_proof.PROOF_ID,
        "recorded_at_utc": "2026-08-28T21:00:00.000000Z",
        "run_slug": manifest["run_slug"],
        "runtime_import": {
            "file_sha256": precutover_proof.sha256_bytes(runtime_raw),
            "terminal_signal": "PREDICTION_RUNTIME_IMPORT_ADMISSION_GREEN",
        },
        "schema_version": 1,
        "source_commit": commit,
        "source_root": manifest["source_root"],
        "source_verification": {"status": "PREDICTION_SOURCE_IDENTITY_GREEN"},
        "terminal_signal": precutover_proof.TERMINAL_SIGNAL,
    }
    report = {
        **body,
        "proof_sha256": precutover_proof.sha256_bytes(
            precutover_proof.canonical_json_bytes(body)
        ),
    }
    report_path = evidence / precutover_proof.REPORT
    report_path.write_bytes(precutover_proof.canonical_json_bytes(report) + b"\n")
    (evidence / precutover_proof.REPORT_PIN).write_text(
        f"{precutover_proof.sha256_file(report_path)}  {report_path.name}\n",
        encoding="ascii",
    )
    output_paths = [
        precutover_proof.REPORT,
        precutover_proof.REPORT_PIN,
        precutover_proof.RUNTIME_REPORT,
    ]
    inventory = {
        "files": [
            {
                "path": name,
                "sha256": precutover_proof.sha256_file(evidence / name),
                "size": (evidence / name).stat().st_size,
            }
            for name in sorted(output_paths)
        ],
        "schema_version": 1,
    }
    inventory_path = evidence / precutover_proof.OUTPUT_INVENTORY
    inventory_path.write_bytes(precutover_proof.canonical_json_bytes(inventory) + b"\n")
    (evidence / precutover_proof.OUTPUT_INVENTORY_PIN).write_text(
        f"{precutover_proof.sha256_file(inventory_path)}  {inventory_path.name}\n",
        encoding="ascii",
    )
    retrieved = precutover_proof.verify_output(proof_pack, evidence)
    assert retrieved["terminal_signal"] == precutover_proof.RETRIEVAL_SIGNAL

    (proof_pack / "operator" / "B0-linux-precutover-proof.sh").write_text(
        "tampered\n", encoding="utf-8"
    )
    with pytest.raises(precutover_proof.ProofError, match="inventory file identity diverged"):
        precutover_proof.verify_input(proof_pack)


def _os_release_layout(tmp_path: Path) -> tuple[Path, Path, int]:
    logical = tmp_path / "etc" / "os-release"
    target = tmp_path / "usr" / "lib" / "os-release"
    logical.parent.mkdir(parents=True)
    target.parent.mkdir(parents=True)
    target.write_text('ID=ubuntu\nVERSION_ID="24.04"\n', encoding="utf-8", newline="\n")
    target.chmod(0o644)
    return logical, target, target.lstat().st_uid


def _safe_test_file_identity(path: Path) -> precutover_proof._FileIdentity:
    value = precutover_proof._file_identity(path)
    return replace(value, mode=stat.S_IFREG | 0o644)


def _symlink_test_readers(
    logical: Path,
    target: Path,
    link: str,
    *,
    target_identity: precutover_proof._FileIdentity | None = None,
    resolved: Path | None = None,
) -> tuple[
    Callable[[Path], precutover_proof._FileIdentity],
    Callable[[Path], str],
    Callable[[Path], Path],
]:
    selected_identity = target_identity or _safe_test_file_identity(target)
    logical_identity = replace(
        selected_identity,
        inode=selected_identity.inode + 1,
        mode=stat.S_IFLNK | 0o777,
        size=len(link.encode()),
    )

    def identity(path: Path) -> precutover_proof._FileIdentity:
        return logical_identity if path == logical else selected_identity

    def readlink(path: Path) -> str:
        assert path == logical
        return link

    def resolve(path: Path) -> Path:
        assert path == logical
        return resolved or target

    return identity, readlink, resolve


def test_os_release_accepts_safe_regular_file_and_authenticates_content(
    tmp_path: Path,
) -> None:
    logical, _target, uid = _os_release_layout(tmp_path)
    logical.write_text('ID=ubuntu\nVERSION_ID="24.04"\n', encoding="utf-8", newline="\n")
    logical.chmod(0o644)
    values, evidence = precutover_proof._read_os_release(
        logical,
        tmp_path / "usr" / "lib" / "os-release",
        required_uid=uid,
        identity_reader=_safe_test_file_identity,
    )
    assert values == {"ID": "ubuntu", "VERSION_ID": "24.04"}
    assert evidence["layout"] == "regular"
    assert evidence["logical_path"] == str(logical)
    assert evidence["resolved_path"] == str(logical)
    assert evidence["content_sha256"] == precutover_proof.sha256_file(logical)


@pytest.mark.parametrize("absolute", [False, True])
def test_os_release_accepts_only_direct_canonical_ubuntu_symlink(
    tmp_path: Path,
    absolute: bool,
) -> None:
    logical, target, uid = _os_release_layout(tmp_path)
    link = str(target) if absolute else os.path.relpath(target, logical.parent)
    identity, readlink, resolve = _symlink_test_readers(logical, target, link)
    values, evidence = precutover_proof._read_os_release(
        logical,
        target,
        required_uid=uid,
        identity_reader=identity,
        link_reader=readlink,
        resolve_reader=resolve,
    )
    assert values["ID"] == "ubuntu"
    assert evidence["layout"] == (
        "canonical_absolute_symlink" if absolute else "canonical_relative_symlink"
    )
    assert evidence["symlink_target"] == link
    assert evidence["resolved_path"] == str(target)
    assert evidence["target_metadata"]["uid"] == uid  # type: ignore[index]


def test_os_release_refuses_wrong_target_and_symlink_chain(tmp_path: Path) -> None:
    logical, target, uid = _os_release_layout(tmp_path)
    wrong = tmp_path / "wrong-os-release"
    wrong.write_text("ID=ubuntu\n", encoding="utf-8", newline="\n")
    wrong.chmod(0o644)
    wrong_link = str(wrong)
    identity, readlink, resolve = _symlink_test_readers(logical, target, wrong_link)
    with pytest.raises(precutover_proof.ProofError, match="not the canonical Ubuntu target"):
        precutover_proof._read_os_release(
            logical,
            target,
            required_uid=uid,
            identity_reader=identity,
            link_reader=readlink,
            resolve_reader=resolve,
        )

    ultimate = tmp_path / "ultimate-os-release"
    ultimate.write_text("ID=ubuntu\n", encoding="utf-8", newline="\n")
    ultimate.chmod(0o644)
    canonical_link = os.path.relpath(target, logical.parent)
    identity, readlink, resolve = _symlink_test_readers(
        logical,
        target,
        canonical_link,
        resolved=ultimate,
    )
    with pytest.raises(precutover_proof.ProofError, match="ambiguous"):
        precutover_proof._read_os_release(
            logical,
            target,
            required_uid=uid,
            identity_reader=identity,
            link_reader=readlink,
            resolve_reader=resolve,
        )


def test_os_release_refuses_absent_or_linked_canonical_target(tmp_path: Path) -> None:
    logical, target, uid = _os_release_layout(tmp_path)
    link = os.path.relpath(target, logical.parent)
    safe_target = _safe_test_file_identity(target)
    identity, readlink, resolve = _symlink_test_readers(logical, target, link)

    def absent_target(path: Path) -> precutover_proof._FileIdentity:
        if path == target:
            raise FileNotFoundError(target)
        return identity(path)

    with pytest.raises(precutover_proof.ProofError, match="absent or unreadable"):
        precutover_proof._read_os_release(
            logical,
            target,
            required_uid=uid,
            identity_reader=absent_target,
            link_reader=readlink,
            resolve_reader=resolve,
        )

    linked_target = replace(safe_target, mode=stat.S_IFLNK | 0o777)
    identity, readlink, resolve = _symlink_test_readers(
        logical,
        target,
        link,
        target_identity=linked_target,
    )
    with pytest.raises(precutover_proof.ProofError, match="not a regular file"):
        precutover_proof._read_os_release(
            logical,
            target,
            required_uid=uid,
            identity_reader=identity,
            link_reader=readlink,
            resolve_reader=resolve,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("group_writable", "group/world writable"),
        ("non_regular", "not a regular file"),
        ("oversized", "oversized"),
        ("wrong_uid", "not owned by root"),
    ],
)
def test_os_release_refuses_unsafe_target_metadata(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    logical, _target, uid = _os_release_layout(tmp_path)
    target = tmp_path / "usr" / "lib" / "os-release"
    target_value = _safe_test_file_identity(target)
    if mutation == "group_writable":
        target_value = replace(target_value, mode=target_value.mode | stat.S_IWGRP)
    elif mutation == "non_regular":
        target_value = replace(target_value, mode=stat.S_IFDIR | 0o755)
    elif mutation == "oversized":
        target_value = replace(
            target_value,
            size=precutover_proof._OS_RELEASE_MAXIMUM_BYTES + 1,
        )
    else:
        target_value = replace(target_value, uid=uid + 1)
    link = os.path.relpath(target, logical.parent)
    identity, readlink, resolve = _symlink_test_readers(
        logical,
        target,
        link,
        target_identity=target_value,
    )

    with pytest.raises(precutover_proof.ProofError, match=message):
        precutover_proof._read_os_release(
            logical,
            target,
            required_uid=uid,
            identity_reader=identity,
            link_reader=readlink,
            resolve_reader=resolve,
        )


def test_os_release_refuses_toctou_mutation(tmp_path: Path) -> None:
    logical, _target, uid = _os_release_layout(tmp_path)
    logical.write_text("ID=ubuntu\n", encoding="utf-8", newline="\n")
    logical.chmod(0o644)

    def mutating_read(path: Path) -> bytes:
        raw = path.read_bytes()
        path.write_bytes(raw + b"MUTATED=1\n")
        return raw

    with pytest.raises(precutover_proof.ProofError, match="mutated during authentication"):
        precutover_proof._read_os_release(
            logical,
            tmp_path / "usr" / "lib" / "os-release",
            required_uid=uid,
            bytes_reader=mutating_read,
            identity_reader=_safe_test_file_identity,
        )


def test_readonly_preflight_accumulates_all_independent_failures() -> None:
    def refuse(detail: str) -> dict[str, object]:
        raise precutover_proof.ProofError(detail)

    checks = {
        "os_release": ("OS_RELEASE_REFUSED", lambda: refuse("bad os-release")),
        "capacity": ("CAPACITY_REFUSED", lambda: refuse("low capacity")),
        "wheelhouse": ("WHEELHOUSE_TAGS_REFUSED", lambda: refuse("wrong tags")),
        "independent_green": ("UNUSED", lambda: {"value": "green"}),
    }
    results, incompatibilities = precutover_proof._collect_readonly_preflight(checks)
    assert [row["code"] for row in incompatibilities] == [
        "OS_RELEASE_REFUSED",
        "CAPACITY_REFUSED",
        "WHEELHOUSE_TAGS_REFUSED",
    ]
    assert results["independent_green"]["status"] == "green"  # type: ignore[index]


def test_wheelhouse_names_are_linux_cpython312_compatible_or_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheelhouse = tmp_path / "wheelhouse"
    wheelhouse.mkdir()
    for name in (
        "pure-1.0-py3-none-any.whl",
        "native-1.0-cp312-cp312-manylinux_2_28_x86_64.whl",
    ):
        (wheelhouse / name).write_bytes(b"SYNTHETIC/FIXTURE")
    monkeypatch.setattr(precutover_proof.platform, "libc_ver", lambda: ("glibc", "2.39"))
    assert precutover_proof._wheelhouse_compatibility(tmp_path)["count"] == 2
    (wheelhouse / "wrong-1.0-cp312-cp312-win_amd64.whl").write_bytes(
        b"SYNTHETIC/FIXTURE"
    )
    with pytest.raises(precutover_proof.ProofError, match=r"wrong-1\.0.*win_amd64"):
        precutover_proof._wheelhouse_compatibility(tmp_path)


def _b0_fixture(tmp_path: Path, *, fail_linux: bool) -> tuple[Path, dict[str, str], Path, Path]:
    incoming = tmp_path / "incoming" / "pm-20260828t220000z-deadbeef"
    source = tmp_path / "volume" / "sources" / incoming.name
    campaign = tmp_path / "volume" / "campaigns" / incoming.name
    (incoming / "operator").mkdir(parents=True)
    (incoming / "scripts").mkdir()
    source.parent.mkdir(parents=True)
    campaign.parent.mkdir(parents=True)
    manifest = {
        "incoming_root": _bash_path(incoming),
        "source_root": _bash_path(source),
        "campaign_root": _bash_path(campaign),
        "source_commit": "d" * 40,
        "bundle_filename": precutover_proof._BUNDLE_NAME,
    }
    b0 = incoming / "operator" / "B0-linux-precutover-proof.sh"
    b0.write_text(precutover_proof.render_b0(manifest), encoding="utf-8", newline="\n")
    (incoming / "scripts" / "precutover_proof.py").write_text(
        "# SYNTHETIC/FIXTURE\n", encoding="utf-8"
    )
    (incoming / "scripts" / "launch_pack.py").write_text(
        "# SYNTHETIC/FIXTURE\n", encoding="utf-8"
    )
    (incoming / "scripts" / "preflight.py").write_text(
        "# SYNTHETIC/FIXTURE\n", encoding="utf-8"
    )
    log = tmp_path / "b0.log"
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    harness_b0 = b0.read_text(encoding="utf-8")
    harness_b0 = harness_b0.replace("/usr/bin/readlink", "readlink")
    harness_b0 = harness_b0.replace("exec /usr/bin/timeout", "exec timeout")
    harness_b0 = harness_b0.replace(
        "/usr/bin/env -i HOME=/home/hyperlab PATH=/usr/bin:/bin",
        f"env HOME=/home/hyperlab PATH={_bash_path(fake_bin)}:/usr/bin:/bin",
    )
    harness_b0 = harness_b0.replace("/usr/bin/bash", "bash")
    harness_b0 = harness_b0.replace(
        '[[ $(command -v "$command") == "/usr/bin/$command" ]]',
        'command -v "$command" >/dev/null 2>&1',
    )
    b0.write_text(harness_b0, encoding="utf-8", newline="\n")
    _write_command(
        fake_bin,
        "id",
        "[[ ${1:-} == -un ]] || exit 97\nprintf 'hyperlab\\n'\n",
    )
    _write_command(
        fake_bin,
        "python3.12",
        "printf 'python|%s\\n' \"$*\" >> \"$HYPERLAB_FAKE_LOG\"\n"
        "if [[ $* == *verify-linux-environment* && ${HYPERLAB_FAIL_LINUX:-0} == 1 ]]; then\n"
        "  printf '%s\\n' '{\"clone_started\":false,\"compatible\":false,\"incompatibilities\":[{\"code\":\"OS_RELEASE_REFUSED\"},{\"code\":\"CAPACITY_REFUSED\"},{\"code\":\"WHEELHOUSE_TAGS_REFUSED\"}],\"mutation_performed\":false,\"phase\":\"BEFORE_CLONE\"}'\n"
        "  printf '%s\\n' 'PREDICTION_LINUX_PRECUTOVER_PREFLIGHT_REFUSED:codes=OS_RELEASE_REFUSED,CAPACITY_REFUSED,WHEELHOUSE_TAGS_REFUSED' >&2\n"
        "  exit 4\n"
        "fi\n",
    )
    _write_command(
        fake_bin,
        "findmnt",
        "printf '/mnt/fixture /dev/fixture ext4 rw 8:16 /\\n'\n",
    )
    _write_command(
        fake_bin,
        "git",
        "printf 'git|%s\\n' \"$*\" >> \"$HYPERLAB_FAKE_LOG\"\n"
        "if [[ ${1:-} == clone ]]; then mkdir -p -- \"$4\"; exit 0; fi\n"
        "if [[ ${1:-} == -C && ${3:-} == symbolic-ref ]]; then exit 1; fi\n"
        "exit 0\n",
    )
    venv_wrapper = tmp_path / "venv-python"
    venv_wrapper.write_text(
        "#!/usr/bin/bash\n"
        "printf 'venv|%s\\n' \"$*\" >> \"$HYPERLAB_FAKE_LOG\"\n"
        "if [[ $* == *runtime-import-admission* ]]; then printf '{}\\n' > \"$HYPERLAB_INCOMING_WINDOWS/runtime-import-admission.json\"; fi\n"
        "if [[ $* == *write-report* ]]; then\n"
        "  for name in linux-precutover-proof-report.json linux-precutover-proof-report.sha256 linux-precutover-proof-output-inventory.json linux-precutover-proof-output-inventory.sha256; do printf '{}\\n' > \"$HYPERLAB_INCOMING_WINDOWS/$name\"; done\n"
        "fi\n",
        encoding="utf-8",
        newline="\n",
    )
    venv_wrapper.chmod(0o700)
    bootstrap = incoming / "scripts" / "bootstrap-offline.sh"
    bootstrap.write_text(
        "#!/usr/bin/env bash\n"
        "printf 'bootstrap|%s\\n' \"$*\" >> \"$HYPERLAB_FAKE_LOG\"\n"
        "mkdir -p -- \"$1/.venv/bin\"\n"
        "cp -- \"$HYPERLAB_VENV_WRAPPER\" \"$1/.venv/bin/python\"\n"
        "chmod 0700 \"$1/.venv/bin/python\"\n",
        encoding="utf-8",
        newline="\n",
    )
    bash_env = tmp_path / "bash-env"
    bash_env.write_text(
        f"export PATH='{_bash_path(fake_bin)}:/usr/bin:/bin'\n",
        encoding="utf-8",
        newline="\n",
    )
    environment = os.environ.copy()
    environment.update(
        {
            "HOME": "/home/hyperlab",
            "BASH_ENV": _bash_path(bash_env),
            "HYPERLAB_FAIL_LINUX": "1" if fail_linux else "0",
            "HYPERLAB_FAKE_LOG": _bash_path(log),
            "HYPERLAB_INCOMING_WINDOWS": _bash_path(incoming),
            "HYPERLAB_VENV_WRAPPER": _bash_path(venv_wrapper),
            "PATH": _bash_path(fake_bin) + ":/usr/bin:/bin",
        }
    )
    return b0, environment, log, campaign


def test_b0_executes_real_bounded_control_flow_and_stops_before_every_cutover(
    tmp_path: Path,
) -> None:
    b0, environment, log, campaign = _b0_fixture(tmp_path, fail_linux=False)
    completed = subprocess.run(
        [str(_git_bash()), _bash_path(b0)],
        capture_output=True,
        check=False,
        cwd=tmp_path,
        env=environment,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert precutover_proof.TERMINAL_SIGNAL in completed.stdout
    assert not campaign.exists()
    lines = log.read_text(encoding="utf-8").splitlines()
    ordered = (
        "verify-input",
        "verify-linux-environment",
        "git|clone --no-checkout",
        "git|-C",
        "verify-source",
        "bootstrap|",
        "runtime-import-admission",
        "write-report",
    )
    positions = [next(i for i, line in enumerate(lines) if token in line) for token in ordered]
    assert positions == sorted(positions)
    executable = "\n".join(
        line
        for line in b0.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )
    for forbidden in (
        "sudo",
        "systemctl",
        "systemd-run",
        "verify-old",
        "disarm-old",
        "restore-old",
        "cutover.sh",
        "install.sh",
        "curl",
        "wget",
        "ssh",
        "scp",
        "nc",
        "ss",
        "lsof",
        "fuser",
    ):
        assert re.search(
            rf"(?<![A-Za-z0-9_-]){re.escape(forbidden)}(?![A-Za-z0-9_-])",
            executable,
        ) is None
    assert "timeout --signal=TERM --kill-after=30s 35m" in executable
    campaign_lines = [line.strip() for line in executable.splitlines() if "CAMPAIGN_ROOT" in line]
    assert campaign_lines[0].startswith("CAMPAIGN_ROOT=")
    assert all(
        line.startswith("[[ ! -e $CAMPAIGN_ROOT") for line in campaign_lines[1:]
    )
    assert executable.index("write-report") < executable.index(
        f"printf '{precutover_proof.TERMINAL_SIGNAL}"
    )


def test_b0_linux_refusal_never_clones_or_false_greens(tmp_path: Path) -> None:
    b0, environment, log, campaign = _b0_fixture(tmp_path, fail_linux=True)
    completed = subprocess.run(
        [str(_git_bash()), _bash_path(b0)],
        capture_output=True,
        check=False,
        cwd=tmp_path,
        env=environment,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 4
    lines = log.read_text(encoding="utf-8").splitlines()
    assert any("verify-input" in line for line in lines)
    assert any("verify-linux-environment" in line for line in lines)
    assert not any(line.startswith("git|") for line in lines)
    for code in (
        "OS_RELEASE_REFUSED",
        "CAPACITY_REFUSED",
        "WHEELHOUSE_TAGS_REFUSED",
    ):
        assert code in completed.stdout
        assert code in completed.stderr
    assert '"clone_started":false' in completed.stdout
    assert '"mutation_performed":false' in completed.stdout
    assert completed.stderr.count(precutover_proof.PREFLIGHT_REFUSED) == 1
    assert precutover_proof.TERMINAL_SIGNAL not in completed.stdout
    assert not campaign.exists()


@pytest.mark.parametrize(
    "renderer",
    [precutover_proof.render_windows_transfer, precutover_proof.render_windows_retrieve],
)
def test_windows_blocks_are_power_shell_51_parseable_and_announce_safety_contract(
    tmp_path: Path,
    renderer: object,
) -> None:
    manifest = {
        "incoming_root": "/home/hyperlab/hyperlab-prediction-markets/incoming/pm-20260828t230000z-deadbeef",
        "run_slug": "pm-20260828t230000z-deadbeef",
    }
    assert callable(renderer)
    content = renderer(manifest)  # type: ignore[operator]
    script = tmp_path / "operator.ps1"
    script.write_text(content, encoding="utf-8")
    powershell = Path(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe")
    completed = subprocess.run(
        [
            str(powershell),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            (
                "$tokens=$null;$errors=$null;"
                "[void][System.Management.Automation.Language.Parser]::ParseFile("
                f"'{script}',[ref]$tokens,[ref]$errors);"
                "if($errors.Count){$errors|ForEach-Object{$_.Message};exit 4}"
            ),
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    for phrase in ("Lieu:", "Durée moyenne:", "maximum opérateur:", "Prompts:", "Ctrl+C", "Signal terminal exact:"):
        assert phrase in content
