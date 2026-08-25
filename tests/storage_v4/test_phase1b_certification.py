from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import hyperlab.paper.storage_v4.phase1b_certification as certification_module
from hyperlab.paper.golden_v3 import GOLDEN_STREAM_NAMES, GoldenVerification
from hyperlab.paper.storage_v4.canonical import build_commit_logical
from hyperlab.paper.storage_v4.checkpoint import CheckpointState
from hyperlab.paper.storage_v4.golden_import import (
    GoldenCommitAssembler,
    GoldenImportExpectations,
)
from hyperlab.paper.storage_v4.phase1b_certification import (
    DEFAULT_DEPENDENCIES,
    PHASE1B_DIVERGED,
    PHASE1B_SUCCESS,
    Phase1BCertificationConfig,
    Phase1BCertificationError,
    Phase1BGoldenDivergenceError,
    _CertificationExpectations,
    _Dependencies,
    _Progress,
    certify_storage_v4_phase1b,
    failure_verdict,
)
from hyperlab.paper.storage_v4.repository import (
    AuditIntegrityStatus,
    StartupIntegrityStatus,
)
from hyperlab.paper.storage_v4.types import (
    CommitFrame,
    CommitSequence,
    Hash32,
    RunId,
    StoreId,
    StreamId,
)

_ROOT = "a" * 64
_RUN = "b" * 64
_SOURCE = "1" * 64
_CONFIG = "c" * 64
_CODE = "d" * 64
_RUNTIME = "e" * 64
_CERTIFIER_CODE = "4" * 64
_CERTIFIER_RUNTIME = "5" * 64
_MANIFEST = Hash32.from_hex("f" * 64)
_CHECKPOINT = Hash32.from_hex("0" * 64)


def _pin_bytes(*, annotation: str | None = None) -> bytes:
    value: dict[str, object] = {
        "export_root": _ROOT,
        "run_id": _RUN,
        "source_sha256": _SOURCE,
    }
    if annotation is not None:
        value["annotation"] = annotation
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        + b"\n"
    )


def _line(row: dict[str, object]) -> bytes:
    return (
        json.dumps(
            row,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        + b"\n"
    )


def _progress_records(path: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
    ]


@dataclass(slots=True)
class _State:
    frames: list[CommitFrame] = field(default_factory=list)
    generation: int = 0
    tail_commits: int = 0
    tail_rows: int = 0
    verify_calls: int = 0
    seal_counts: list[tuple[tuple[StreamId, int], ...]] = field(default_factory=list)
    checkpoint_state: object | None = None
    checkpoint_state_witnesses: list[tuple[int, str]] = field(default_factory=list)
    persisted_checkpoint_state_overrides: dict[int, CheckpointState] = field(
        default_factory=dict
    )
    checkpoint_builds: int = 0
    seal_required_after_commits: int = 1
    close_calls: int = 0
    fail_close_call: int | None = None
    close_failure_raised: bool = False


class _FakeRepository:
    def __init__(self, root: Path, state: _State, *, create: bool) -> None:
        self._root = root
        self._state = state
        self._closed = False
        if create:
            root.mkdir(exist_ok=False)

    @property
    def overlay_state(self) -> SimpleNamespace:
        return SimpleNamespace(
            seal_required=(
                self._state.tail_commits >= self._state.seal_required_after_commits
            ),
            tail_commit_count=self._state.tail_commits,
            tail_row_count=self._state.tail_rows,
        )

    @property
    def startup_report(self) -> SimpleNamespace:
        return self.startup()

    def append(self, frame: CommitFrame) -> bool:
        self._state.frames.append(frame)
        self._state.tail_commits += 1
        self._state.tail_rows += len(frame.rows)
        return True

    def seal(
        self,
        *,
        checkpoint_state: object,
        cumulative_stream_counts: tuple[tuple[StreamId, int], ...],
        historical_commit_count: int,
    ) -> SimpleNamespace:
        self._state.checkpoint_builds += 1
        assert historical_commit_count == len(self._state.frames)
        assert cumulative_stream_counts
        assert all(count > 0 for _, count in cumulative_stream_counts)
        self._state.seal_counts.append(cumulative_stream_counts)
        persisted_state = self._state.persisted_checkpoint_state_overrides.get(
            historical_commit_count,
            checkpoint_state,
        )
        assert isinstance(persisted_state, CheckpointState)
        self._state.checkpoint_state = persisted_state
        self._state.checkpoint_state_witnesses.append(
            (
                historical_commit_count,
                certification_module._checkpoint_state_sha256(persisted_state),
            )
        )
        self._state.generation += 1
        self._state.tail_commits = 0
        self._state.tail_rows = 0
        segment_path = self._root / f"segment-{self._state.generation}.bin"
        checkpoint_path = self._root / f"checkpoint-{self._state.generation}.bin"
        manifest_path = self._root / f"manifest-{self._state.generation}.bin"
        segment_path.write_bytes(b"s" * 100)
        checkpoint_path.write_bytes(b"c" * 20)
        manifest_path.write_bytes(b"m" * 20)
        return SimpleNamespace(
            checkpoint=SimpleNamespace(state=checkpoint_state),
            segment=SimpleNamespace(physical_size=100),
            segment_path=segment_path,
            checkpoint_path=checkpoint_path,
            manifest_path=manifest_path,
        )

    def startup(self) -> SimpleNamespace:
        final_root = build_commit_logical(self._state.frames[-1]).prefix_root
        return SimpleNamespace(
            integrity_status=StartupIntegrityStatus.AUTHENTICATED_CHECKPOINT_PLUS_TAIL,
            manifest_generation=self._state.generation,
            manifest_root=_MANIFEST,
            checkpoint_root=_CHECKPOINT,
            checkpoint_state=self._state.checkpoint_state,
            base_commit_sequence=CommitSequence(len(self._state.frames)),
            base_prefix_root=final_root,
            tail_frames=(),
            tail_entries_replayed=0,
            tail_rows_replayed=0,
            segments_read=0,
            historical_segments_not_read=self._state.generation,
            historical_commits_not_read=len(self._state.frames),
            historical_rows_not_read=sum(len(frame.rows) for frame in self._state.frames),
            checkpoint_used=True,
        )

    def full_audit(self) -> SimpleNamespace:
        return SimpleNamespace(
            integrity_status=AuditIntegrityStatus.FULL_HISTORY_AUTHENTICATED,
            manifest_generation=self._state.generation,
            manifest_root=_MANIFEST,
            checkpoint_root=_CHECKPOINT,
            manifests_read=self._state.generation,
            checkpoints_read=self._state.generation,
            segments_read=self._state.generation,
            commits_read=len(self._state.frames),
            rows_read=sum(len(frame.rows) for frame in self._state.frames),
            physical_segment_bytes=self._state.generation * 100,
            cumulative_stream_counts=(),
            checkpoint_state_witnesses=tuple(
                SimpleNamespace(
                    covered_commit_sequence=CommitSequence(sequence),
                    state_sha256=Hash32.from_hex(digest),
                )
                for sequence, digest in self._state.checkpoint_state_witnesses
            ),
        )

    def iter_historical_frames(self) -> Any:
        return iter(self._state.frames)

    def close(self) -> None:
        self._state.close_calls += 1
        if (
            self._state.fail_close_call == self._state.close_calls
            and not self._state.close_failure_raised
        ):
            self._state.close_failure_raised = True
            raise RuntimeError("synthetic repository close failed")
        self._closed = True


@dataclass(frozen=True, slots=True)
class _Synthetic:
    config: Phase1BCertificationConfig
    verification: GoldenVerification
    expectations: GoldenImportExpectations
    streams: dict[str, tuple[dict[str, object], ...]]
    state: _State
    dependencies: _Dependencies
    required: _CertificationExpectations


def _synthetic(tmp_path: Path) -> _Synthetic:
    golden = tmp_path / "golden"
    golden.mkdir()
    pin = tmp_path / "golden.pin"
    pin.write_bytes(_pin_bytes())
    output = tmp_path / "phase1b-result"
    state = _State()

    commit_hashes = ("2" * 64, "3" * 64)
    event_hash = "4" * 64
    projection_hashes = ("5" * 64, "6" * 64, "7" * 64)
    rows: dict[str, tuple[dict[str, object], ...]] = {
        "schema": ({"kind": "metadata", "name": "paper_schema"},),
        "run": (
            {
                "commit_count": 2,
                "config": {
                    "release_code_sha256": _CODE,
                    "runtime_environment_sha256": _RUNTIME,
                },
                "config_hash": _CONFIG,
                "run_id": _RUN,
            },
        ),
        "inbox": (
            {
                "commit_hash": commit_hashes[0],
                "commit_sequence": 1,
                "input_id": "input-1",
                "run_id": _RUN,
            },
            {
                "commit_hash": commit_hashes[1],
                "commit_sequence": 2,
                "input_id": "input-2",
                "run_id": _RUN,
            },
        ),
        "events": (
            {
                "event_hash": event_hash,
                "input_id": "input-1",
                "run_id": _RUN,
                "sequence": 1,
            },
        ),
        "ledger_transactions": (
            {
                "commit_sequence": 1,
                "run_id": _RUN,
                "transaction_hash": "8" * 64,
            },
        ),
        "ledger_entries": (
            {
                "account": "cash",
                "amount_text": "10.25",
                "commit_sequence": 1,
                "entry_hash": "9" * 64,
                "run_id": _RUN,
                "unit": "USD",
            },
        ),
        "alerts": (
            {"code": "MARKET_GAP", "commit_sequence": 2, "run_id": _RUN},
        ),
        "commits": (
            {
                "commit_hash": commit_hashes[0],
                "commit_sequence": 1,
                "event_hashes": [event_hash],
                "first_event_sequence": 1,
                "input_id": "input-1",
                "last_event_sequence": 1,
                "projection_hash": projection_hashes[1],
                "projection_revision": 1,
                "run_id": _RUN,
            },
            {
                "commit_hash": commit_hashes[1],
                "commit_sequence": 2,
                "event_hashes": [],
                "first_event_sequence": None,
                "input_id": "input-2",
                "last_event_sequence": None,
                "projection_hash": projection_hashes[2],
                "projection_revision": 2,
                "run_id": _RUN,
            },
        ),
        "projection_history": (
            {
                "created_at": "2026-08-18T12:00:00Z",
                "event_head_hash": "0" * 64,
                "event_sequence": 0,
                "input_id": "run-start",
                "payload": {"state": "INITIAL"},
                "payload_codec": "zlib-json-v1",
                "projection_hash": projection_hashes[0],
                "revision": 0,
                "run_id": _RUN,
                "status": "INITIAL",
                "utc_date": "2026-08-18",
            },
            {
                "created_at": "2026-08-18T12:00:01Z",
                "event_head_hash": event_hash,
                "event_sequence": 1,
                "input_id": "input-1",
                "payload": {"state": "RUNNING"},
                "payload_codec": "zlib-json-v1",
                "projection_hash": projection_hashes[1],
                "revision": 1,
                "run_id": _RUN,
                "status": "RUNNING",
                "utc_date": "2026-08-18",
            },
            {
                "created_at": "2026-08-18T12:00:02Z",
                "event_head_hash": event_hash,
                "event_sequence": 1,
                "input_id": "input-2",
                "payload": {"state": "PAUSED"},
                "payload_codec": "zlib-json-v1",
                "projection_hash": projection_hashes[2],
                "revision": 2,
                "run_id": _RUN,
                "status": "PAUSED",
                "utc_date": "2026-08-18",
            },
        ),
        "projection_current": (),
        "runtime_sessions": (
            {"commit_sequence": 2, "run_id": _RUN, "session_id": "session-1"},
        ),
        "incidents": (
            {
                "code": "MARKET_GAP",
                "commit_sequence": 2,
                "run_id": _RUN,
            },
        ),
        "heads": ({"commit_count": 2, "run_id": _RUN},),
    }
    rows["projection_current"] = (
        {
            "effective_status": "PAUSED",
            "event_head_hash": event_hash,
            "event_sequence": 1,
            "payload": {"state": "PAUSED"},
            "projection_hash": projection_hashes[2],
            "revision": 2,
            "run_id": _RUN,
            "status": "PAUSED",
            "updated_at": "2026-08-18T12:00:02Z",
        },
    )

    descriptors: dict[str, object] = {}
    for name in GOLDEN_STREAM_NAMES:
        payload = b"".join(_line(row) for row in rows[name])
        descriptors[name] = {
            "logical_sha256": hashlib.sha256(payload).hexdigest(),
            "logical_size": len(payload),
            "row_count": len(rows[name]),
            "shards": [{"physical_size": len(payload)}],
        }
    manifest: dict[str, object] = {
        "census": {
            "alert_code_counts": {"MARKET_GAP": 1},
            "coverage_gaps": ["PHASE05_PHASE08_DECISIONS_NOT_BOTH_OBSERVED"],
            "strategy_decision_counts": {"phase08_robust_pairs": 1},
        },
        "root_hash": _ROOT,
        "run_id": _RUN,
        "source": {"sha256": _SOURCE},
        "streams": descriptors,
    }
    verification = GoldenVerification(golden.resolve(), _ROOT, manifest)
    expectations = GoldenImportExpectations(
        run_id=RunId(_RUN),
        export_root=Hash32.from_hex(_ROOT),
        commit_count=2,
        row_count=sum(len(rows[name]) for name in GOLDEN_STREAM_NAMES),
        stream_row_counts=tuple(
            (name, len(rows[name])) for name in GOLDEN_STREAM_NAMES
        ),
    )

    def verify(root: Path, external_pin: Path) -> GoldenVerification:
        assert root == golden
        assert external_pin == pin
        state.verify_calls += 1
        return verification

    def assemble(
        candidate: GoldenVerification,
    ) -> tuple[GoldenImportExpectations, GoldenCommitAssembler]:
        assert candidate is verification
        return expectations, GoldenCommitAssembler(rows, expectations)

    def stream(
        candidate: GoldenVerification,
        name: str,
    ) -> tuple[dict[str, object], ...]:
        assert candidate is verification
        return rows[name]

    def anchor_create(path: Path, store_id: StoreId) -> object:
        assert store_id == StoreId("synthetic-store")
        path.write_bytes(b"anchor")
        return object()

    def anchor_open(path: Path, store_id: StoreId) -> object:
        assert path.read_bytes() == b"anchor"
        assert store_id == StoreId("synthetic-store")
        return object()

    dependencies = _Dependencies(
        verify=verify,
        assemble=assemble,
        stream=stream,
        create_repository=lambda root, anchor, config: _FakeRepository(
            root, state, create=True
        ),
        open_repository=lambda root, anchor, config: _FakeRepository(
            root, state, create=False
        ),
        anchor_create=anchor_create,
        anchor_open=anchor_open,
        current_certifier_code_sha256=lambda: _CERTIFIER_CODE,
        current_certifier_runtime_environment_sha256=lambda: _CERTIFIER_RUNTIME,
    )
    config = Phase1BCertificationConfig(
        golden_root=golden,
        golden_pin=pin,
        output_root=output,
        expected_golden_root=_ROOT,
        expected_source_sha256=_SOURCE,
        expected_run_id=_RUN,
        config_hash=_CONFIG,
        release_code_sha256=_CODE,
        runtime_environment_sha256=_RUNTIME,
        certifier_code_sha256=_CERTIFIER_CODE,
        certifier_runtime_environment_sha256=_CERTIFIER_RUNTIME,
        store_id="synthetic-store",
        seal_rows=1,
        seal_bytes=1,
        heartbeat_seconds=30.0,
        safety_seconds=30.0,
    )
    return _Synthetic(
        config=config,
        verification=verification,
        expectations=expectations,
        streams=rows,
        state=state,
        dependencies=dependencies,
        required=_CertificationExpectations(
            commits=2,
            rows=expectations.row_count,
            streams=len(GOLDEN_STREAM_NAMES),
            market_gap_alert_rows=1,
        ),
    )


def test_synthetic_pipeline_publishes_complete_only_after_exact_equivalence(
    tmp_path: Path,
) -> None:
    synthetic = _synthetic(tmp_path)
    assert set(synthetic.streams["projection_current"][0]) != set(
        synthetic.streams["projection_history"][-1]
    )

    result = certify_storage_v4_phase1b(
        synthetic.config,
        _dependencies=synthetic.dependencies,
        _test_expectations=synthetic.required,
    )

    assert result.status == PHASE1B_SUCCESS
    assert synthetic.state.verify_calls == 2
    assert len(synthetic.state.seal_counts) == 2
    assert all(
        count > 0
        for seal_counts in synthetic.state.seal_counts
        for _, count in seal_counts
    )
    report_bytes = result.report_path.read_bytes()
    report = json.loads(report_bytes)
    assert report["format"] == "hyperlab-storage-v4-phase1b-certification-v2"
    assert report["golden"]["source_sha256"] == _SOURCE
    assert report["golden"]["pin"]["sha256"] == hashlib.sha256(
        synthetic.config.golden_pin.read_bytes()
    ).hexdigest()
    assert report["golden"]["pin"]["stable_during_each_verification"] is True
    assert report["golden"]["pin"]["unchanged_across_certification"] is True
    assert report["identities"]["golden_source"] == {
        "config_hash": _CONFIG,
        "release_code_sha256": _CODE,
        "run_id": _RUN,
        "runtime_environment_sha256": _RUNTIME,
    }
    certifier = report["identities"]["storage_v4_certifier"]
    assert certifier["candidate_id"] == "phase08-phase05-multistrategy-paper-v1"
    assert certifier["code_sha256"] == _CERTIFIER_CODE
    assert certifier["runtime_environment_sha256"] == _CERTIFIER_RUNTIME
    assert certifier["configuration_sha256"] == (
        certification_module._certifier_configuration_sha256(synthetic.config)
    )
    assert certifier["configuration"]["golden_source"]["source_sha256"] == _SOURCE
    assert report["comparison"]["rows"] == synthetic.expectations.row_count
    assert report["comparison"]["market_gap_rows"] == 1
    assert report["comparison"]["frame_ownership_exact"] is True
    assert report["comparison"]["checkpoint_states_verified"] == 2
    assert report["comparison"]["legacy_v3_identity_exact"] is True
    assert report["comparison"]["row_order_exact"] is True
    assert [
        witness["covered_commit_sequence"]
        for witness in report["audit"]["checkpoint_state_witnesses"]
    ] == [1, 2]
    assert all(
        len(witness["state_sha256"]) == 64
        for witness in report["audit"]["checkpoint_state_witnesses"]
    )
    assert report["coverage_metadata_non_blocking"] == {
        "coverage_gaps": ["PHASE05_PHASE08_DECISIONS_NOT_BOTH_OBSERVED"],
        "economic_evidence": False,
        "market_gap_alert_count": 1,
        "non_blocking": True,
        "phase05_decision_coverage": False,
        "phase08_decision_coverage": True,
        "strategy_decision_counts": {"phase08_robust_pairs": 1},
    }
    assert report["startup"] == {
        "checkpoint_state_after_tail_exact": True,
        "checkpoint_state_exact": True,
        "checkpoint_used": True,
        "historical_commits_not_read": 2,
        "historical_payload_replay_complexity": "O(tail)",
        "historical_rows_not_read": synthetic.expectations.row_count,
        "historical_segments_not_read": 2,
        "integrity_result": "AUTHENTICATED_CHECKPOINT_PLUS_TAIL",
        "manifest_generation": 2,
        "metadata_authentication_complexity": (
            "O(current_manifest + checkpoint + tail)"
        ),
        "segments_read": 0,
        "tail_entries_replayed": 0,
    }
    complete = json.loads(result.complete_path.read_bytes())
    assert complete["format"] == "hyperlab-storage-v4-phase1b-complete-v2"
    assert complete["report_sha256"] == hashlib.sha256(report_bytes).hexdigest()
    assert complete["golden_pin_sha256"] == report["golden"]["pin"]["sha256"]
    assert complete["certifier_code_sha256"] == _CERTIFIER_CODE
    assert complete["certifier_configuration_sha256"] == (
        certifier["configuration_sha256"]
    )
    assert (
        complete["certifier_runtime_environment_sha256"]
        == _CERTIFIER_RUNTIME
    )
    assert complete["status"] == "COMPLETE"
    assert synthetic.state.checkpoint_builds == 2
    rss_status = report["metrics"]["peak_rss_status"]
    assert rss_status in {"AVAILABLE", "UNAVAILABLE"}
    if rss_status == "AVAILABLE":
        assert report["metrics"]["peak_rss_bytes"] > 0
    else:
        assert "peak_rss_bytes" not in report["metrics"]
        assert report["metrics"]["peak_rss_unavailable_reason"]
    progress = _progress_records(synthetic.config.progress_path)
    assert progress[-1]["status"] == "RUNNING"
    assert progress[-1]["event"] == "certification_gates_passed"
    assert all(item.get("status") != "VERIFIED" for item in progress)


@pytest.mark.parametrize(
    ("field", "divergent_value"),
    (
        ("revision", 3),
        ("event_sequence", 2),
        ("event_head_hash", "8" * 64),
        ("projection_hash", "9" * 64),
        ("payload", {"state": "DIVERGED"}),
    ),
)
def test_terminal_projection_semantic_divergence_never_publishes_complete(
    tmp_path: Path,
    field: str,
    divergent_value: object,
) -> None:
    synthetic = _synthetic(tmp_path)
    synthetic.streams["projection_current"][0][field] = divergent_value

    with pytest.raises(Phase1BGoldenDivergenceError, match=field):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=synthetic.dependencies,
            _test_expectations=synthetic.required,
        )

    assert not synthetic.config.report_path.exists()
    assert not synthetic.config.complete_path.exists()


def test_pipeline_integrates_with_real_repository_and_local_anchor(tmp_path: Path) -> None:
    synthetic = _synthetic(tmp_path)
    dependencies = _Dependencies(
        verify=synthetic.dependencies.verify,
        assemble=synthetic.dependencies.assemble,
        stream=synthetic.dependencies.stream,
        create_repository=DEFAULT_DEPENDENCIES.create_repository,
        open_repository=DEFAULT_DEPENDENCIES.open_repository,
        anchor_create=DEFAULT_DEPENDENCIES.anchor_create,
        anchor_open=DEFAULT_DEPENDENCIES.anchor_open,
        current_certifier_code_sha256=(
            synthetic.dependencies.current_certifier_code_sha256
        ),
        current_certifier_runtime_environment_sha256=(
            synthetic.dependencies.current_certifier_runtime_environment_sha256
        ),
    )

    result = certify_storage_v4_phase1b(
        synthetic.config,
        _dependencies=dependencies,
        _test_expectations=synthetic.required,
    )

    report = json.loads(result.report_path.read_bytes())
    assert report["audit"]["commits"] == 2
    assert report["audit"]["rows"] == synthetic.expectations.row_count
    assert report["audit"]["segments"] == 2
    assert report["startup"]["segments_read"] == 0
    assert (
        report["startup"]["historical_rows_not_read"]
        == synthetic.expectations.row_count
    )
    assert result.complete_path.is_file()


def test_nested_market_gap_text_outside_alert_code_is_not_double_counted(
    tmp_path: Path,
) -> None:
    synthetic = _synthetic(tmp_path)
    result = certify_storage_v4_phase1b(
        synthetic.config,
        _dependencies=synthetic.dependencies,
        _test_expectations=synthetic.required,
    )
    report = json.loads(result.report_path.read_bytes())
    assert report["comparison"]["market_gap_rows"] == 1


def test_logical_divergence_never_publishes_complete(tmp_path: Path) -> None:
    synthetic = _synthetic(tmp_path)

    def divergent_stream(
        verification: GoldenVerification,
        name: str,
    ) -> tuple[dict[str, object], ...]:
        values = synthetic.dependencies.stream(verification, name)
        if name == "events":
            return ({**values[0], "diverged": True},)
        return tuple(values)

    dependencies = _Dependencies(
        verify=synthetic.dependencies.verify,
        assemble=synthetic.dependencies.assemble,
        stream=divergent_stream,
        create_repository=synthetic.dependencies.create_repository,
        open_repository=synthetic.dependencies.open_repository,
        anchor_create=synthetic.dependencies.anchor_create,
        anchor_open=synthetic.dependencies.anchor_open,
        current_certifier_code_sha256=(
            synthetic.dependencies.current_certifier_code_sha256
        ),
        current_certifier_runtime_environment_sha256=(
            synthetic.dependencies.current_certifier_runtime_environment_sha256
        ),
    )
    with pytest.raises(Phase1BGoldenDivergenceError, match="events") as raised:
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=dependencies,
            _test_expectations=synthetic.required,
        )
    assert failure_verdict(raised.value) == PHASE1B_DIVERGED
    assert not synthetic.config.complete_path.exists()
    assert not synthetic.config.report_path.exists()


def test_source_identity_mismatch_refuses_before_creating_output(tmp_path: Path) -> None:
    synthetic = _synthetic(tmp_path)
    config = replace(synthetic.config, expected_source_sha256="9" * 64)
    with pytest.raises(Phase1BCertificationError, match="source SHA-256"):
        certify_storage_v4_phase1b(
            config,
            _dependencies=synthetic.dependencies,
            _test_expectations=synthetic.required,
        )
    assert not config.output_root.exists()


@pytest.mark.parametrize(
    ("field", "message"),
    (
        ("certifier_code_sha256", "current certifier code SHA-256 differs"),
        (
            "certifier_runtime_environment_sha256",
            "current certifier runtime environment SHA-256 differs",
        ),
    ),
)
def test_current_certifier_provenance_mismatch_refuses_before_creating_output(
    tmp_path: Path,
    field: str,
    message: str,
) -> None:
    synthetic = _synthetic(tmp_path)
    config = replace(synthetic.config, **{field: "6" * 64})

    with pytest.raises(Phase1BCertificationError, match=message):
        certify_storage_v4_phase1b(
            config,
            _dependencies=synthetic.dependencies,
            _test_expectations=synthetic.required,
        )

    assert synthetic.state.verify_calls == 0
    assert not config.output_root.exists()


def test_certifier_provenance_is_rechecked_before_publication(tmp_path: Path) -> None:
    synthetic = _synthetic(tmp_path)
    calls = 0

    def current_code() -> str:
        nonlocal calls
        calls += 1
        return _CERTIFIER_CODE if calls == 1 else "6" * 64

    dependencies = replace(
        synthetic.dependencies,
        current_certifier_code_sha256=current_code,
    )
    with pytest.raises(
        Phase1BCertificationError,
        match="current certifier code SHA-256 differs",
    ):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=dependencies,
            _test_expectations=synthetic.required,
        )

    assert calls == 2
    assert not synthetic.config.report_path.exists()
    assert not synthetic.config.complete_path.exists()


def test_existing_output_is_never_reused(tmp_path: Path) -> None:
    synthetic = _synthetic(tmp_path)
    synthetic.config.output_root.mkdir()
    sentinel = synthetic.config.output_root / "user.txt"
    sentinel.write_bytes(b"untouched")

    with pytest.raises(Phase1BCertificationError, match="already exists"):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=synthetic.dependencies,
            _test_expectations=synthetic.required,
        )
    assert sentinel.read_bytes() == b"untouched"
    assert not synthetic.config.complete_path.exists()


def test_existing_output_is_rejected_before_golden_verification(tmp_path: Path) -> None:
    synthetic = _synthetic(tmp_path)
    synthetic.config.output_root.mkdir()

    with pytest.raises(Phase1BCertificationError, match="already exists"):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=synthetic.dependencies,
            _test_expectations=synthetic.required,
        )

    assert synthetic.state.verify_calls == 0


def test_only_sealed_commits_build_complete_checkpoint_state(tmp_path: Path) -> None:
    synthetic = _synthetic(tmp_path)
    synthetic.state.seal_required_after_commits = 100

    with pytest.raises(Phase1BCertificationError, match="multiple seal/checkpoint cycles"):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=synthetic.dependencies,
            _test_expectations=synthetic.required,
        )

    assert synthetic.state.checkpoint_builds == 1
    assert not synthetic.config.complete_path.exists()


def _dependencies_with_anchor_open(
    synthetic: _Synthetic,
    callback: Any,
) -> _Dependencies:
    def anchor_open(path: Path, store_id: StoreId) -> object:
        callback()
        return synthetic.dependencies.anchor_open(path, store_id)

    return _Dependencies(
        verify=synthetic.dependencies.verify,
        assemble=synthetic.dependencies.assemble,
        stream=synthetic.dependencies.stream,
        create_repository=synthetic.dependencies.create_repository,
        open_repository=synthetic.dependencies.open_repository,
        anchor_create=synthetic.dependencies.anchor_create,
        anchor_open=anchor_open,
        current_certifier_code_sha256=(
            synthetic.dependencies.current_certifier_code_sha256
        ),
        current_certifier_runtime_environment_sha256=(
            synthetic.dependencies.current_certifier_runtime_environment_sha256
        ),
    )


def test_wrong_authenticated_checkpoint_state_never_publishes_complete(
    tmp_path: Path,
) -> None:
    synthetic = _synthetic(tmp_path)

    def corrupt_checkpoint_state() -> None:
        synthetic.state.checkpoint_state = CheckpointState(
            adapter={"wrong": True},
            ledger={"wrong": True},
            projection={"wrong": True},
            sessions={"wrong": True},
            incidents={"wrong": True},
            cursors={"wrong": True},
            stream_heads={"wrong": True},
        )

    with pytest.raises(Phase1BCertificationError, match="checkpoint state differs"):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=_dependencies_with_anchor_open(
                synthetic,
                corrupt_checkpoint_state,
            ),
            _test_expectations=synthetic.required,
        )
    assert not synthetic.config.complete_path.exists()


def test_wrong_intermediate_checkpoint_with_correct_final_never_completes(
    tmp_path: Path,
) -> None:
    synthetic = _synthetic(tmp_path)
    synthetic.state.persisted_checkpoint_state_overrides[1] = CheckpointState(
        adapter={"wrong": True},
        ledger={"wrong": True},
        projection={"wrong": True},
        sessions={"wrong": True},
        incidents={"wrong": True},
        cursors={"wrong": True},
        stream_heads={"wrong": True},
    )
    with pytest.raises(
        Phase1BGoldenDivergenceError,
        match="checkpoint state differs at seal boundary 1",
    ):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=synthetic.dependencies,
            _test_expectations=synthetic.required,
        )

    assert synthetic.state.checkpoint_builds == 2
    assert synthetic.state.checkpoint_state_witnesses[0][1] != (
        synthetic.state.checkpoint_state_witnesses[1][1]
    )
    assert not synthetic.config.complete_path.exists()


def test_full_audit_checkpoint_witness_count_must_match_all_chain_counts(
    tmp_path: Path,
) -> None:
    synthetic = _synthetic(tmp_path)

    def drop_intermediate_witness() -> None:
        synthetic.state.checkpoint_state_witnesses.pop(0)

    with pytest.raises(
        Phase1BCertificationError,
        match="manifest/checkpoint/segment/witness counts differ",
    ):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=_dependencies_with_anchor_open(
                synthetic,
                drop_intermediate_witness,
            ),
            _test_expectations=synthetic.required,
        )
    assert not synthetic.config.complete_path.exists()


def test_full_audit_checkpoint_witness_boundaries_must_be_strictly_ordered(
    tmp_path: Path,
) -> None:
    synthetic = _synthetic(tmp_path)

    def reverse_witness_boundaries() -> None:
        synthetic.state.checkpoint_state_witnesses.reverse()

    with pytest.raises(
        Phase1BCertificationError,
        match="boundaries are not strictly ordered",
    ):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=_dependencies_with_anchor_open(
                synthetic,
                reverse_witness_boundaries,
            ),
            _test_expectations=synthetic.required,
        )
    assert not synthetic.config.complete_path.exists()


def test_wrong_legacy_v3_identity_never_publishes_complete(tmp_path: Path) -> None:
    synthetic = _synthetic(tmp_path)

    def corrupt_final_identity() -> None:
        final = synthetic.state.frames[-1]
        synthetic.state.frames[-1] = replace(
            final,
            legacy_v3_identity=Hash32.from_hex("a" * 64),
        )

    with pytest.raises(Phase1BGoldenDivergenceError, match="legacy V3 identity"):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=_dependencies_with_anchor_open(
                synthetic,
                corrupt_final_identity,
            ),
            _test_expectations=synthetic.required,
        )
    assert not synthetic.config.complete_path.exists()


def test_row_moved_between_frames_never_publishes_complete(tmp_path: Path) -> None:
    synthetic = _synthetic(tmp_path)

    def move_alert_to_wrong_frame() -> None:
        first, final = synthetic.state.frames
        alert = next(row for row in final.rows if row.stream_id == StreamId("alerts"))
        positions = {
            name: position for position, name in enumerate(GOLDEN_STREAM_NAMES)
        }
        first_rows = tuple(
            sorted(
                (*first.rows, alert),
                key=lambda row: (positions[row.stream_id.value], int(row.ordinal)),
            )
        )
        new_first = replace(first, rows=first_rows)
        new_final = replace(
            final,
            previous_prefix_root=build_commit_logical(new_first).prefix_root,
            rows=tuple(row for row in final.rows if row is not alert),
        )
        synthetic.state.frames[:] = [new_first, new_final]

    with pytest.raises(Phase1BGoldenDivergenceError, match=r"misowns.*alerts"):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=_dependencies_with_anchor_open(
                synthetic,
                move_alert_to_wrong_frame,
            ),
            _test_expectations=synthetic.required,
        )
    assert not synthetic.config.complete_path.exists()


def test_unknown_coverage_gap_never_publishes_complete(tmp_path: Path) -> None:
    synthetic = _synthetic(tmp_path)
    census = synthetic.verification.manifest["census"]
    assert isinstance(census, dict)
    gaps = census["coverage_gaps"]
    assert isinstance(gaps, list)
    gaps.append("UNEXPECTED_INTEGRITY_GAP")

    with pytest.raises(Phase1BCertificationError, match="non-authorized coverage gaps"):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=synthetic.dependencies,
            _test_expectations=synthetic.required,
        )
    assert not synthetic.config.complete_path.exists()
    assert all(
        record.get("status") != "VERIFIED"
        for record in _progress_records(synthetic.config.progress_path)
    )


@pytest.mark.parametrize("target_attribute", ("report_path", "complete_path"))
def test_publication_failure_never_records_verified_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_attribute: str,
) -> None:
    synthetic = _synthetic(tmp_path)
    target = getattr(synthetic.config, target_attribute)
    original_publish = certification_module.durable_publish_immutable

    def fail_selected_publication(path: Path, payload: bytes) -> None:
        if path == target:
            raise OSError("synthetic publication failure")
        original_publish(path, payload)

    monkeypatch.setattr(
        certification_module,
        "durable_publish_immutable",
        fail_selected_publication,
    )
    with pytest.raises(OSError, match="synthetic publication failure"):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=synthetic.dependencies,
            _test_expectations=synthetic.required,
        )
    assert not synthetic.config.complete_path.exists()
    assert all(
        record.get("status") != "VERIFIED"
        for record in _progress_records(synthetic.config.progress_path)
    )


def test_canonical_pin_replacement_with_same_bindings_is_rejected(
    tmp_path: Path,
) -> None:
    synthetic = _synthetic(tmp_path)

    def replace_pin_without_changing_verifier_bindings() -> None:
        synthetic.config.golden_pin.write_bytes(
            _pin_bytes(annotation="same-authority-bindings-different-bytes")
        )

    with pytest.raises(
        Phase1BCertificationError,
        match="authority or pin changed",
    ):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=_dependencies_with_anchor_open(
                synthetic,
                replace_pin_without_changing_verifier_bindings,
            ),
            _test_expectations=synthetic.required,
        )
    assert synthetic.state.verify_calls == 2
    assert not synthetic.config.complete_path.exists()


def test_deadline_covers_initial_golden_verification(tmp_path: Path) -> None:
    synthetic = _synthetic(tmp_path)

    def slow_verify(root: Path, pin: Path) -> GoldenVerification:
        del root, pin
        time.sleep(0.1)
        return synthetic.verification

    dependencies = _Dependencies(
        verify=slow_verify,
        assemble=synthetic.dependencies.assemble,
        stream=synthetic.dependencies.stream,
        create_repository=synthetic.dependencies.create_repository,
        open_repository=synthetic.dependencies.open_repository,
        anchor_create=synthetic.dependencies.anchor_create,
        anchor_open=synthetic.dependencies.anchor_open,
        current_certifier_code_sha256=(
            synthetic.dependencies.current_certifier_code_sha256
        ),
        current_certifier_runtime_environment_sha256=(
            synthetic.dependencies.current_certifier_runtime_environment_sha256
        ),
    )
    config = replace(synthetic.config, safety_seconds=0.02)
    with pytest.raises(TimeoutError, match="safety deadline"):
        certify_storage_v4_phase1b(
            config,
            _dependencies=dependencies,
            _test_expectations=synthetic.required,
        )
    assert not config.output_root.exists()


def test_deadline_failure_after_complete_publication_removes_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synthetic = _synthetic(tmp_path)
    original = certification_module._Deadline.finish_after_complete

    def expire_after_publication(deadline: Any) -> None:
        deadline._expired.set()
        original(deadline)

    monkeypatch.setattr(
        certification_module._Deadline,
        "finish_after_complete",
        expire_after_publication,
    )
    with pytest.raises(TimeoutError, match="safety deadline"):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=synthetic.dependencies,
            _test_expectations=synthetic.required,
        )
    assert synthetic.config.report_path.is_file()
    assert not synthetic.config.complete_path.exists()


def test_final_repository_close_failure_never_publishes_complete(
    tmp_path: Path,
) -> None:
    synthetic = _synthetic(tmp_path)
    synthetic.state.fail_close_call = 2

    with pytest.raises(RuntimeError, match="repository close failed"):
        certify_storage_v4_phase1b(
            synthetic.config,
            _dependencies=synthetic.dependencies,
            _test_expectations=synthetic.required,
        )

    assert synthetic.state.close_failure_raised is True
    assert not synthetic.config.report_path.exists()
    assert not synthetic.config.complete_path.exists()


def test_progress_heartbeat_contains_required_operational_metrics(tmp_path: Path) -> None:
    root = tmp_path / "progress"
    root.mkdir()
    path = root / "progress.jsonl"
    progress = _Progress(path)
    progress.emit(
        {
            "bytes_written": 123,
            "checkpoints": 2,
            "commits_completed": 4,
            "commits_expected": 8,
            "event": "progress",
            "rows_completed": 10,
            "rows_expected": 20,
            "segments": 2,
            "timestamp_utc": "2000-01-01T00:00:00Z",
        }
    )
    progress.heartbeat(
        {
            "bytes_written": 123,
            "checkpoints": 2,
            "commits_completed": 4,
            "commits_expected": 8,
            "cpu_us": 7,
            "elapsed_us": 11,
            "peak_rss_bytes": 99,
            "peak_rss_source": "WINDOWS_PROCESS_MEMORY_COUNTERS",
            "peak_rss_status": "AVAILABLE",
            "rows_completed": 10,
            "rows_expected": 20,
            "segments": 2,
        }
    )
    progress.close()
    records = [json.loads(line) for line in path.read_text().splitlines()]
    heartbeat = records[-1]
    assert heartbeat["event"] == "heartbeat"
    assert heartbeat["commits_completed"] == 4
    assert heartbeat["rows_expected"] == 20
    assert heartbeat["segments"] == 2
    assert heartbeat["checkpoints"] == 2
    assert heartbeat["bytes_written"] == 123
    assert heartbeat["elapsed_us"] == 11
    assert heartbeat["cpu_us"] == 7
    assert heartbeat["peak_rss_bytes"] == 99
    assert heartbeat["peak_rss_source"] == "WINDOWS_PROCESS_MEMORY_COUNTERS"
    assert heartbeat["peak_rss_status"] == "AVAILABLE"
    assert heartbeat["timestamp_utc"] != "2000-01-01T00:00:00Z"


def test_windows_peak_rss_reports_real_bytes_when_process_query_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(certification_module.sys, "platform", "win32")
    monkeypatch.setattr(
        certification_module,
        "_windows_peak_rss_bytes",
        lambda: 123_456,
    )

    payload = certification_module._peak_rss_measurement().payload()

    assert payload == {
        "peak_rss_bytes": 123_456,
        "peak_rss_source": "WINDOWS_PROCESS_MEMORY_COUNTERS",
        "peak_rss_status": "AVAILABLE",
    }


def test_windows_peak_rss_failure_is_explicitly_unavailable_without_null(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(certification_module.sys, "platform", "win32")

    def unavailable() -> int:
        raise OSError("synthetic process-memory query failure")

    monkeypatch.setattr(
        certification_module,
        "_windows_peak_rss_bytes",
        unavailable,
    )

    payload = certification_module._peak_rss_measurement().payload()

    assert payload == {
        "peak_rss_source": "WINDOWS_PROCESS_MEMORY_COUNTERS",
        "peak_rss_status": "UNAVAILABLE",
        "peak_rss_unavailable_reason": "WINDOWS_PROCESS_MEMORY_QUERY_FAILED",
    }
    assert "peak_rss_bytes" not in payload
    assert None not in payload.values()


def test_config_requires_digest_run_identity_and_bounded_heartbeat(tmp_path: Path) -> None:
    synthetic = _synthetic(tmp_path)
    with pytest.raises(ValueError, match="expected_run_id"):
        replace(synthetic.config, expected_run_id="run")
    with pytest.raises(ValueError, match="heartbeat"):
        replace(synthetic.config, heartbeat_seconds=29.0)
