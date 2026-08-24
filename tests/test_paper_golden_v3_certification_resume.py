from __future__ import annotations

import hashlib
import shutil
import stat
from argparse import Namespace
from dataclasses import dataclass
from pathlib import Path

import pytest
from pytest import MonkeyPatch
from test_paper_golden_v3 import _build_source

import hyperlab.paper.golden_v3_certification as certification
import scripts.certify_paper_golden_v3 as cli
from hyperlab.paper.golden_v3 import (
    GoldenExportResult,
    GoldenVerification,
    export_golden_v3,
    verify_golden_v3,
    write_external_pin,
)
from hyperlab.paper.golden_v3_certification import GoldenCertificationError


@dataclass(frozen=True)
class _PartialCandidate:
    source: Path
    golden_root: Path
    candidate: Path
    export_a: Path
    pin_a: Path
    run_id: str
    source_size: int
    source_sha256: str
    export_a_root_hash: str
    export_a_file_count: int
    export_a_bytes: int
    sentinel: Path


def _tree_fingerprint(root: Path) -> tuple[tuple[str, int, str], ...]:
    records: list[tuple[str, int, str]] = []
    for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file()):
        payload = path.read_bytes()
        records.append(
            (
                path.relative_to(root).as_posix(),
                len(payload),
                hashlib.sha256(payload).hexdigest(),
            )
        )
    return tuple(records)


def _export_measurement(root: Path) -> tuple[int, int]:
    files = [candidate for candidate in root.rglob("*") if candidate.is_file()]
    return len(files), sum(path.stat().st_size for path in files)


def _build_partial_candidate(tmp_path: Path) -> _PartialCandidate:
    source = tmp_path / "source" / "paper-source.sqlite3"
    source.parent.mkdir()
    run_id = _build_source(source, market_count=2, include_unlinked_alert=False)
    source_size = source.stat().st_size
    source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    source.chmod(stat.S_IREAD)

    golden_root = tmp_path / "golden-v3"
    candidate = golden_root / "candidate-synthetic-01"
    corpus = candidate / "corpus"
    manifests = candidate / "manifests"
    results = candidate / "results"
    scratch = candidate / "scratch"
    pins = candidate / "pin"
    for directory in (corpus, manifests, results, scratch, pins):
        directory.mkdir(parents=True, exist_ok=False)

    sentinel = source.parent / "forbidden-original.sqlite3"
    export_a_result = export_golden_v3(
        source,
        corpus / "extract-a",
        run_id,
        sentinel_path=sentinel,
        require_readonly=True,
        shard_rows=1_000,
        shard_bytes=1_000_000,
        expected_source_size=source_size,
        expected_source_sha256=source_sha256,
    )
    verification_a = verify_golden_v3(export_a_result.output_root)
    pin_a = write_external_pin(
        export_a_result.output_root,
        pins / "extract-a.pin.json",
        verification=verification_a,
    )
    certification._write_new_canonical_json(
        results / "extract-a.json",
        {
            "export": export_a_result.to_dict(),
            "verification": verification_a.to_dict(),
        },
    )
    export_a_file_count, export_a_bytes = _export_measurement(export_a_result.output_root)
    return _PartialCandidate(
        source=source,
        golden_root=golden_root,
        candidate=candidate,
        export_a=export_a_result.output_root,
        pin_a=pin_a,
        run_id=run_id,
        source_size=source_size,
        source_sha256=source_sha256,
        export_a_root_hash=verification_a.root_hash,
        export_a_file_count=export_a_file_count,
        export_a_bytes=export_a_bytes,
        sentinel=sentinel,
    )


@pytest.fixture
def partial_candidate(tmp_path: Path, monkeypatch: MonkeyPatch) -> _PartialCandidate:
    monkeypatch.setattr(certification, "_MINIMUM_FREE_BYTES", 0)
    partial = _build_partial_candidate(tmp_path)
    try:
        yield partial
    finally:
        for path in sorted(tmp_path.rglob("*"), reverse=True):
            if path.is_file():
                path.chmod(stat.S_IREAD | stat.S_IWRITE)


def _resume(
    partial: _PartialCandidate,
    *,
    source: Path | None = None,
    expected_export_a_root_hash: str | None = None,
    expected_export_a_file_count: int | None = None,
    expected_export_a_bytes: int | None = None,
) -> object:
    selected_source = source or partial.source
    return certification.resume_golden_v3_certification(
        selected_source,
        partial.golden_root,
        partial.run_id,
        sentinel_path=partial.sentinel,
        expected_source_size=selected_source.stat().st_size,
        expected_source_sha256=hashlib.sha256(selected_source.read_bytes()).hexdigest(),
        expected_export_a_root_hash=(
            expected_export_a_root_hash or partial.export_a_root_hash
        ),
        expected_export_a_file_count=(
            partial.export_a_file_count
            if expected_export_a_file_count is None
            else expected_export_a_file_count
        ),
        expected_export_a_bytes=(
            partial.export_a_bytes
            if expected_export_a_bytes is None
            else expected_export_a_bytes
        ),
        shard_rows=1_000,
        shard_bytes=1_000_000,
    )


def test_discover_partial_candidate_requires_exactly_one_regular_child(
    tmp_path: Path,
) -> None:
    golden_root = tmp_path / "golden-v3"
    golden_root.mkdir()

    with pytest.raises(GoldenCertificationError, match=r"candidate|exactly|unique|found"):
        certification._discover_partial_candidate(golden_root)

    first = golden_root / "candidate-01"
    first.mkdir()
    assert certification._discover_partial_candidate(golden_root) == first.resolve(strict=True)

    (golden_root / "candidate-02").mkdir()
    with pytest.raises(GoldenCertificationError, match=r"ambiguous|candidate|exactly|unique"):
        certification._discover_partial_candidate(golden_root)


def test_cli_resume_discovers_candidate_and_requires_new_progress(
    partial_candidate: _PartialCandidate,
) -> None:
    partial = partial_candidate
    progress = partial.candidate / "results" / "resume-01.jsonl"
    namespace = Namespace(
        source=partial.source,
        candidate_root=partial.golden_root,
        resume_existing_a=True,
        run_id=partial.run_id,
        sentinel=partial.sentinel,
        expected_size=partial.source_size,
        expected_sha256=partial.source_sha256,
        shard_rows=1_000,
        shard_bytes=1_000_000,
        progress_jsonl=progress,
        expected_export_a_root_hash=partial.export_a_root_hash,
        expected_export_a_file_count=partial.export_a_file_count,
        expected_export_a_bytes=partial.export_a_bytes,
    )

    args = cli._validated_arguments(namespace)

    assert args.resume_existing_a is True
    assert args.golden_root == partial.golden_root.resolve(strict=True)
    assert args.candidate_root == partial.candidate.resolve(strict=True)
    assert args.progress_jsonl == progress.resolve(strict=False)


def test_resume_reuses_export_a_and_exports_only_new_b(
    partial_candidate: _PartialCandidate,
    monkeypatch: MonkeyPatch,
) -> None:
    partial = partial_candidate
    export_a_before = _tree_fingerprint(partial.export_a)
    original_export = certification.export_golden_v3
    exported_roots: list[Path] = []

    def recording_export(*args: object, **kwargs: object) -> GoldenExportResult:
        output_root = Path(args[1])
        exported_roots.append(output_root)
        return original_export(*args, **kwargs)

    monkeypatch.setattr(certification, "export_golden_v3", recording_export)
    result = _resume(partial)

    assert result.status == "GOLDEN_V3_CERTIFIED"
    assert exported_roots == [partial.candidate / "corpus" / "extract-b"]
    assert _tree_fingerprint(partial.export_a) == export_a_before
    assert (partial.candidate / "COMPLETE").is_file()


@pytest.mark.parametrize(
    "fault",
    [
        "export-a-incomplete",
        "export-a-modified",
        "expected-root-mismatch",
        "expected-file-count-mismatch",
        "expected-byte-count-mismatch",
        "pin-missing",
        "pin-mutable",
        "source-identity-mismatch",
        "final-artifact-present",
        "export-b-present",
    ],
)
def test_resume_refuses_ambiguous_or_mismatched_partial_candidate(
    partial_candidate: _PartialCandidate,
    monkeypatch: MonkeyPatch,
    fault: str,
) -> None:
    partial = partial_candidate
    source_override: Path | None = None
    expected_root: str | None = None
    expected_files: int | None = None
    expected_bytes: int | None = None

    if fault == "export-a-incomplete":
        (partial.export_a / "COMPLETE").unlink()
    elif fault == "export-a-modified":
        shard = next((partial.export_a / "streams").glob("*.jsonl"))
        shard.write_bytes(shard.read_bytes() + b"{}\n")
    elif fault == "expected-root-mismatch":
        expected_root = "0" * 64
    elif fault == "expected-file-count-mismatch":
        expected_files = partial.export_a_file_count + 1
    elif fault == "expected-byte-count-mismatch":
        expected_bytes = partial.export_a_bytes + 1
    elif fault == "pin-missing":
        partial.pin_a.chmod(stat.S_IREAD | stat.S_IWRITE)
        partial.pin_a.unlink()
    elif fault == "pin-mutable":
        partial.pin_a.chmod(stat.S_IREAD | stat.S_IWRITE)
    elif fault == "source-identity-mismatch":
        source_override = partial.source.with_name("different-copy.sqlite3")
        shutil.copyfile(partial.source, source_override)
        source_override.chmod(stat.S_IREAD)
    elif fault == "final-artifact-present":
        (partial.candidate / "COMPLETE").write_bytes(b"{}\n")
    elif fault == "export-b-present":
        (partial.candidate / "corpus" / "extract-b").mkdir()
    else:  # pragma: no cover - the parametrization is intentionally exhaustive.
        raise AssertionError(f"unknown synthetic fault: {fault}")

    export_calls: list[Path] = []

    def unexpected_export(*args: object, **kwargs: object) -> GoldenExportResult:
        del kwargs
        export_calls.append(Path(args[1]))
        raise AssertionError("resume attempted an export before rejecting invalid reuse state")

    monkeypatch.setattr(certification, "export_golden_v3", unexpected_export)
    with pytest.raises(GoldenCertificationError):
        _resume(
            partial,
            source=source_override,
            expected_export_a_root_hash=expected_root,
            expected_export_a_file_count=expected_files,
            expected_export_a_bytes=expected_bytes,
        )
    assert export_calls == []
    assert not (partial.candidate / "COMPLETE").exists() or fault == "final-artifact-present"


def test_resume_is_idempotently_fail_closed_after_success(
    partial_candidate: _PartialCandidate,
    monkeypatch: MonkeyPatch,
) -> None:
    partial = partial_candidate
    original_export = certification.export_golden_v3
    exported_roots: list[Path] = []

    def recording_export(*args: object, **kwargs: object) -> GoldenExportResult:
        exported_roots.append(Path(args[1]))
        return original_export(*args, **kwargs)

    monkeypatch.setattr(certification, "export_golden_v3", recording_export)
    _resume(partial)
    completed_before = _tree_fingerprint(partial.candidate)

    with pytest.raises(GoldenCertificationError, match=r"COMPLETE|complete|final|resume"):
        _resume(partial)

    assert exported_roots == [partial.candidate / "corpus" / "extract-b"]
    assert _tree_fingerprint(partial.candidate) == completed_before


def test_fixture_builds_authenticated_result_without_forging_verification(
    partial_candidate: _PartialCandidate,
) -> None:
    partial = partial_candidate
    verified = verify_golden_v3(partial.export_a, pin_path=partial.pin_a)

    assert isinstance(verified, GoldenVerification)
    assert verified.root_hash == partial.export_a_root_hash
    assert _export_measurement(partial.export_a) == (
        partial.export_a_file_count,
        partial.export_a_bytes,
    )
