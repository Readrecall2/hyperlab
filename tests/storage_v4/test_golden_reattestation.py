from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

import hyperlab.paper.storage_v4.golden_reattestation as reattestation_module
from hyperlab.paper.golden_v3 import GoldenVerification
from hyperlab.paper.storage_v4.anchor import (
    AnchorError,
    AnchorErrorCode,
    LocalAnchor,
)
from hyperlab.paper.storage_v4.candidate_tree import (
    CandidateTreeWitness,
    witness_candidate_tree,
)
from hyperlab.paper.storage_v4.canonical import canonical_json_bytes
from hyperlab.paper.storage_v4.golden_import import GoldenCommitAssembler
from hyperlab.paper.storage_v4.golden_reattestation import (
    GOLDEN_NATIVE_IMPORTED_REATTESTATION_V1,
    GOLDEN_NATIVE_REATTESTATION_METRICS_STATUS,
    GoldenNativeReattestationConfig,
    GoldenNativeReattestationError,
    GoldenNativeReattestationResult,
    reattest_golden_native_candidate,
)
from hyperlab.paper.storage_v4.golden_runner import OfflineGoldenNativeRunner
from hyperlab.paper.storage_v4.manifest import (
    OpaqueIdentity,
    manifest_from_bytes,
    manifest_to_bytes,
)
from hyperlab.paper.storage_v4.overlay import (
    OverlayError,
    OverlayErrorCode,
    SQLiteOverlay,
)
from hyperlab.paper.storage_v4.repository import _overlay_identity
from hyperlab.paper.storage_v4.types import Hash32
from tests.storage_v4.test_golden_native import (
    _expectations,
    _fixture,
    _verification,
)

SYNTHETIC_STORAGE_V4_WORKLOAD = True


@dataclass(frozen=True, slots=True)
class _BuiltCandidate:
    config: GoldenNativeReattestationConfig
    verification: GoldenVerification
    streams: dict[str, list[dict[str, object]]]
    progress: tuple[dict[str, object], ...]

    def stream_factory(
        self,
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        return self.streams[name]


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _build_candidate(tmp_path: Path) -> _BuiltCandidate:
    streams = _fixture()
    verification = _verification(tmp_path, streams)
    expected_rows = sum(len(rows) for rows in streams.values())
    progress: list[dict[str, object]] = []
    code_identity = Hash32(hashlib.sha256(b"producer-code").digest())
    runtime_identity = Hash32(hashlib.sha256(b"producer-runtime").digest())

    def assembler_factory(ignored: GoldenVerification) -> GoldenCommitAssembler:
        del ignored
        return GoldenCommitAssembler(streams, _expectations(streams))

    def stream_factory(
        ignored: GoldenVerification,
        name: str,
    ) -> Iterable[Mapping[str, object]]:
        del ignored
        return streams[name]

    runner = OfflineGoldenNativeRunner(
        candidate_root=(tmp_path / "producer-candidate").absolute(),
        code_identity=code_identity,
        runtime_identity=runtime_identity,
        batch_size=2,
        expected_commits=3,
        expected_rows=expected_rows,
        expected_streams=13,
        expected_market_gaps=1,
        assembler_factory=assembler_factory,
        stream_factory=stream_factory,
        rss_probe=lambda: 123_456,
        progress=lambda payload: progress.append(dict(payload)),
    )
    runner.run(verification)
    stdout = (tmp_path / "producer-stdout.jsonl").absolute()
    stderr = (tmp_path / "producer-stderr.log").absolute()
    stdout.write_bytes(
        b"".join(canonical_json_bytes(record) + b"\r\n" for record in progress)
    )
    stderr.write_bytes(b"")
    config = GoldenNativeReattestationConfig(
        candidate_root=(tmp_path / "producer-candidate").absolute(),
        producer_stdout_log=stdout,
        producer_stderr_log=stderr,
        producer_stdout_sha256=_digest(stdout),
        producer_stderr_sha256=_digest(stderr),
        reattestor_code_identity=Hash32(
            hashlib.sha256(b"reattestor-code").digest()
        ),
        reattestor_runtime_identity=Hash32(
            hashlib.sha256(b"reattestor-runtime").digest()
        ),
        expected_commits=3,
        expected_rows=expected_rows,
        expected_streams=13,
        expected_market_gaps=1,
    )
    return _BuiltCandidate(
        config=config,
        verification=verification,
        streams=streams,
        progress=tuple(progress),
    )


def _reattest(built: _BuiltCandidate) -> GoldenNativeReattestationResult:
    return reattest_golden_native_candidate(
        built.config,
        built.verification,
        stream_factory=built.stream_factory,
    )


def test_imported_golden_reattestation_is_exact_read_only_and_zero_ingest(
    tmp_path: Path,
) -> None:
    built = _build_candidate(tmp_path)
    before = witness_candidate_tree(built.config.candidate_root)

    result = _reattest(built)
    payload = result.payload()

    assert result.candidate_tree_before == before == result.candidate_tree_after
    assert payload["status"] == GOLDEN_NATIVE_IMPORTED_REATTESTATION_V1
    assert payload["counts"] == {
        "audited_commits": 3,
        "ingested_commits": 0,
        "prefix_reingested_commits": 0,
    }
    assert payload["measurements"]["status"] == (
        GOLDEN_NATIVE_REATTESTATION_METRICS_STATUS
    )
    assert result.raw_audit.records_read == 3
    assert result.paper_audit.commits_read == 3
    assert result.native_audit.commit_count == 3
    assert result.differential.report["checkpoint_states_verified"] == 2
    assert result.startup_file_trace.historical_segment_open_count == 0
    assert result.paper_startup.segments_read == 0
    assert result.paper_startup.tail_entries_replayed == 0
    assert result.producer.inferred_batch_size == 2

    raw_anchor = LocalAnchor.open_existing_read_only(
        built.config.candidate_root / "anchors" / "raw.sqlite3",
        store_id=result.raw_config.store_id,
    )
    raw_record = raw_anchor.read()
    assert raw_record is not None
    with pytest.raises(AnchorError) as lease_error:
        raw_anchor.acquire_writer_lease()
    assert lease_error.value.code is AnchorErrorCode.READ_ONLY
    with pytest.raises(AnchorError) as cas_error:
        raw_anchor.compare_and_swap(raw_record, raw_record)
    assert cas_error.value.code is AnchorErrorCode.READ_ONLY

    overlay = SQLiteOverlay.open_existing_read_only(
        built.config.candidate_root / "paper" / "overlay.sqlite3",
        expected_identity=_overlay_identity(result.paper_config),
    )
    try:
        assert overlay.read_only is True
        assert overlay.durability_settings().journal_mode == "delete"
        state = overlay.state
        with pytest.raises(OverlayError) as mutation_error:
            overlay.advance_base(
                manifest_generation=state.base_manifest_generation,
                manifest_root=state.base_manifest_root,
                base_commit_sequence=state.base_commit_sequence,
                base_prefix_root=state.base_prefix_root,
            )
        assert mutation_error.value.code is OverlayErrorCode.READ_ONLY
    finally:
        overlay.close()
    assert witness_candidate_tree(built.config.candidate_root) == before


def test_imported_golden_reattestation_rejects_extra_paper_artifact(
    tmp_path: Path,
) -> None:
    built = _build_candidate(tmp_path)
    extra = built.config.candidate_root / "paper" / "segments" / f"{'0' * 64}.hl4s"
    extra.write_bytes(b"extra")

    with pytest.raises(GoldenNativeReattestationError):
        _reattest(built)


def test_imported_golden_reattestation_rejects_same_generation_manifest_fork(
    tmp_path: Path,
) -> None:
    built = _build_candidate(tmp_path)
    current = json.loads(
        (built.config.candidate_root / "paper" / "CURRENT").read_text(
            encoding="utf-8"
        )
    )
    manifests = built.config.candidate_root / "paper" / "manifests"
    authority = manifest_from_bytes(
        (manifests / f"{current['manifest_root']}.hl4m").read_bytes()
    )
    fork = replace(
        authority,
        runtime_identity=OpaqueIdentity(Hash32(hashlib.sha256(b"fork").digest())),
    )
    (manifests / f"{fork.identity.root.hex()}.hl4m").write_bytes(
        manifest_to_bytes(fork)
    )

    with pytest.raises(GoldenNativeReattestationError):
        _reattest(built)


def test_imported_golden_reattestation_rejects_ambiguous_terminal_log(
    tmp_path: Path,
) -> None:
    built = _build_candidate(tmp_path)
    stdout = built.config.producer_stdout_log
    stdout.write_bytes(
        stdout.read_bytes() + canonical_json_bytes(built.progress[-1]) + b"\r\n"
    )
    ambiguous = replace(
        built.config,
        producer_stdout_sha256=_digest(stdout),
    )

    with pytest.raises(GoldenNativeReattestationError, match="unique Golden"):
        reattest_golden_native_candidate(
            ambiguous,
            built.verification,
            stream_factory=built.stream_factory,
        )


def test_imported_golden_reattestation_rejects_tree_drift_after_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built = _build_candidate(tmp_path)
    calls = 0

    def mutate_before_second_witness(root: Path) -> CandidateTreeWitness:
        nonlocal calls
        calls += 1
        if calls == 2:
            (root / "staging" / "post-audit-drift.txt").write_bytes(b"drift")
        return witness_candidate_tree(root)

    monkeypatch.setattr(
        reattestation_module,
        "witness_candidate_tree",
        mutate_before_second_witness,
    )
    with pytest.raises(GoldenNativeReattestationError, match="candidate changed"):
        _reattest(built)
