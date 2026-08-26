from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import replace
from pathlib import Path

import pytest

import hyperlab.paper.storage_v4.phase1c_workers as workers_module
from hyperlab.paper.storage_v4.canonical import canonical_json_bytes
from hyperlab.paper.storage_v4.capacity import (
    CapacityProfile,
    CapacityTypeSpec,
    CapacityWorkloadConfig,
    CapacityWorkloadHasher,
    CapacityWorkloadManifest,
    iter_capacity_commits,
)
from hyperlab.paper.storage_v4.phase1c_certification import (
    run_phase1c_measurements,
)
from hyperlab.paper.storage_v4.phase1c_worker_result import (
    Phase1CCumulativeWorkerResultQuery,
    Phase1CWorkerResultError,
    close_phase1c_cumulative_worker_result_from_authority,
    load_phase1c_cumulative_worker_receipt,
    load_phase1c_cumulative_worker_receipt_authority,
    load_phase1c_cumulative_worker_result_query,
    load_phase1c_promoted_cumulative_worker_result,
    phase1c_cumulative_worker_receipt_authority_path,
    phase1c_cumulative_worker_result_paths,
    promote_phase1c_cumulative_worker_result,
)
from hyperlab.paper.storage_v4.phase1c_workers import (
    Phase1CCumulativeCapacityWorkerRequest,
    Phase1CWorkerError,
    Phase1CWorkerErrorCode,
    run_phase1c_cumulative_capacity_worker,
)
from hyperlab.paper.storage_v4.raw_segment import RawSegmentThresholds
from hyperlab.paper.storage_v4.types import Hash32

SYNTHETIC_STORAGE_V4_WORKLOAD = True


def _manifests() -> tuple[CapacityWorkloadManifest, ...]:
    terminal = CapacityWorkloadConfig(
        profile=CapacityProfile.GOLDEN_SHAPED,
        seed=731,
        commit_count=4,
        start_time_ns=1_700_000_000_000_000_000,
        cadence_ns=250_000_000,
        type_distribution=(
            CapacityTypeSpec(
                record_type="PUBLIC_BBO",
                stream="inbox",
                weight=1,
                payload_min_bytes=8,
                payload_max_bytes=16,
                payload_cardinality=2,
            ),
        ),
        strategies=("phase05_cash_and_carry", "phase08_cross_venue"),
        alert_every_commits=None,
        incident_every_commits=None,
        ledger_every_commits=None,
        market_gap_count=0,
        alert_payload_bytes=5,
        incident_payload_bytes=6,
        ledger_payload_bytes=7,
        market_gap_payload_bytes=8,
        golden_census_sha256="a" * 64,
    )
    configs = {
        commit_count: replace(terminal, commit_count=commit_count)
        for commit_count in (2, 4)
    }
    hasher = CapacityWorkloadHasher()
    manifests: list[CapacityWorkloadManifest] = []
    for commit in iter_capacity_commits(terminal):
        hasher.update(commit)
        if commit.sequence in configs:
            manifests.append(
                CapacityWorkloadManifest(
                    config=configs[commit.sequence],
                    digest=hasher.snapshot(),
                )
            )
    return tuple(manifests)


def _request(tmp_path: Path, *, name: str) -> Phase1CCumulativeCapacityWorkerRequest:
    return Phase1CCumulativeCapacityWorkerRequest(
        manifests=_manifests(),
        candidate_root=(tmp_path / name).absolute(),
        code_identity=Hash32(b"\x91" * 32),
        runtime_identity=Hash32(b"\x92" * 32),
        batch_size=2,
    )


def test_live_queue_result_is_promoted_before_callback_and_reloads_safely(
    tmp_path: Path,
) -> None:
    request = _request(tmp_path, name="promoted")
    timeline: list[tuple[str, object]] = []
    captured = []

    def progress(payload: dict[str, object]) -> None:
        if payload.get("worker_result_event") == "RECEIPT_DURABLE":
            authority_path = Path(str(payload["receipt_authority_path"]))
            assert authority_path.is_file()
            timeline.append(("authority", dict(payload)))
        elif payload.get("worker_result_event") == "RESULT_PROMOTED":
            timeline.append(("promotion", dict(payload)))

    def capture(value: object) -> None:
        timeline.append(("callback", value))
        captured.append(value)

    result = run_phase1c_cumulative_capacity_worker(
        request,
        progress=progress,
        durable_result_callback=capture,
    )

    assert result.accounting.commits_ingested == 4
    assert [item[0] for item in timeline] == [
        "authority",
        "promotion",
        "callback",
    ]
    assert len(captured) == 1
    durable = captured[0]
    promotion = durable.promotion
    assert promotion is not None
    event = timeline[1][1]
    assert isinstance(event, dict)
    assert event["promotion_sha256"] == promotion.sha256
    assert durable.terminal_shared_candidate_tree.root == request.candidate_root
    assert durable.accounting.payload()["generator_emissions"] == 4
    assert durable.receipt.path.parent == request.candidate_root.parent
    assert request.candidate_root not in durable.receipt.path.parents
    reloaded = load_phase1c_promoted_cumulative_worker_result(
        Phase1CCumulativeWorkerResultQuery.from_request(request),
        expected_promotion_sha256=promotion.sha256,
    )
    assert reloaded.authority_payload() == durable.authority_payload()
    with pytest.raises(Phase1CWorkerResultError, match="external pin"):
        load_phase1c_promoted_cumulative_worker_result(
            reloaded.query,
            expected_promotion_sha256="0" * 64,
        )

    promotion_bytes = promotion.path.read_bytes()
    promotion_mapping = json.loads(promotion_bytes)
    assert isinstance(promotion_mapping, dict)
    promotion_mapping["unexpected"] = True
    tampered_promotion_bytes = canonical_json_bytes(promotion_mapping)
    promotion.path.write_bytes(tampered_promotion_bytes)
    with pytest.raises(Phase1CWorkerResultError, match="external pin"):
        load_phase1c_promoted_cumulative_worker_result(
            reloaded.query,
            expected_promotion_sha256=promotion.sha256,
        )
    tampered_promotion_sha256 = hashlib.sha256(tampered_promotion_bytes).hexdigest()
    with pytest.raises(Phase1CWorkerResultError, match="unexpected key set"):
        load_phase1c_promoted_cumulative_worker_result(
            reloaded.query,
            expected_promotion_sha256=tampered_promotion_sha256,
        )
    promotion.path.write_bytes(promotion_bytes)

    boundary_root = (
        request.candidate_root.parent
        / f".{request.candidate_root.name}.phase1c-boundaries"
    )
    fork = boundary_root / "fork.json"
    fork.write_bytes(b"{}")
    with pytest.raises(Phase1CWorkerResultError, match="forked or ambiguous"):
        load_phase1c_promoted_cumulative_worker_result(
            reloaded.query,
            expected_promotion_sha256=promotion.sha256,
        )
    fork.unlink()

    reloaded.receipt.path.unlink()
    with pytest.raises(Phase1CWorkerResultError, match="cannot be lstat-ed"):
        load_phase1c_promoted_cumulative_worker_result(
            reloaded.query,
            expected_promotion_sha256=promotion.sha256,
        )


class _SimulatedParentLoss(RuntimeError):
    pass


def test_receipt_pin_recovers_after_parent_loss_without_source_reingestion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(tmp_path, name="orphaned-receipt")
    durable_events: list[dict[str, object]] = []

    def lose_parent_after_receipt(payload: dict[str, object]) -> None:
        if payload.get("worker_result_event") == "RECEIPT_DURABLE":
            durable_events.append(dict(payload))
            raise _SimulatedParentLoss("simulated loss after durable receipt event")

    with pytest.raises(_SimulatedParentLoss):
        run_phase1c_cumulative_capacity_worker(
            request,
            progress=lose_parent_after_receipt,
        )

    receipt_path, promotion_path = phase1c_cumulative_worker_result_paths(
        request.candidate_root
    )
    authority_path = phase1c_cumulative_worker_receipt_authority_path(
        request.candidate_root
    )
    assert receipt_path.is_file()
    assert authority_path.is_file()
    assert not promotion_path.exists()
    assert len(durable_events) == 1
    durable_event = durable_events[0]
    assert durable_event["worker_result_event"] == "RECEIPT_DURABLE"
    assert durable_event["receipt_authority_path"] == str(authority_path)
    assert durable_event["receipt_authority_sha256"] == hashlib.sha256(
        authority_path.read_bytes()
    ).hexdigest()
    expected_receipt_sha256 = durable_event["receipt_sha256"]
    assert isinstance(expected_receipt_sha256, str)
    assert expected_receipt_sha256 == hashlib.sha256(
        receipt_path.read_bytes()
    ).hexdigest()
    authority = load_phase1c_cumulative_worker_receipt_authority(
        request.candidate_root,
        expected_receipt_sha256=expected_receipt_sha256,
    )
    query = load_phase1c_cumulative_worker_result_query(
        request.candidate_root,
        expected_receipt_sha256=expected_receipt_sha256,
    )
    assert replace(query, raw_thresholds=request.raw_thresholds) == (
        Phase1CCumulativeWorkerResultQuery.from_request(request)
    )
    assert query.raw_thresholds == RawSegmentThresholds()
    durable = load_phase1c_cumulative_worker_receipt(
        query,
        expected_receipt_sha256=expected_receipt_sha256,
    )
    with pytest.raises(Phase1CWorkerResultError, match="external pin"):
        close_phase1c_cumulative_worker_result_from_authority(
            query.candidate_root,
            expected_receipt_sha256="0" * 64,
        )
    assert not promotion_path.exists()

    receipt_bytes = receipt_path.read_bytes()
    receipt_mapping = json.loads(receipt_bytes)
    assert isinstance(receipt_mapping, dict)
    receipt_mapping["unexpected"] = True
    tampered_receipt_bytes = canonical_json_bytes(receipt_mapping)
    receipt_path.write_bytes(tampered_receipt_bytes)
    with pytest.raises(Phase1CWorkerResultError, match="external pin"):
        promote_phase1c_cumulative_worker_result(durable)
    assert not promotion_path.exists()
    tampered_receipt_sha256 = hashlib.sha256(tampered_receipt_bytes).hexdigest()
    with pytest.raises(Phase1CWorkerResultError, match="unexpected key set"):
        load_phase1c_cumulative_worker_receipt(
            query,
            expected_receipt_sha256=tampered_receipt_sha256,
        )
    assert not promotion_path.exists()
    receipt_path.unlink()
    with pytest.raises(Phase1CWorkerResultError, match="cannot be lstat-ed"):
        promote_phase1c_cumulative_worker_result(durable)
    assert not promotion_path.exists()
    receipt_path.write_bytes(receipt_bytes)

    authority_bytes = authority_path.read_bytes()
    authority_mapping = json.loads(authority_bytes)
    assert isinstance(authority_mapping, dict)
    authority_mapping["unexpected"] = True
    authority_path.write_bytes(canonical_json_bytes(authority_mapping))
    with pytest.raises(Phase1CWorkerResultError, match="unexpected key set"):
        close_phase1c_cumulative_worker_result_from_authority(
            query.candidate_root,
            expected_receipt_sha256=expected_receipt_sha256,
        )
    assert not promotion_path.exists()
    authority_path.unlink()
    with pytest.raises(Phase1CWorkerResultError, match="cannot be lstat-ed"):
        close_phase1c_cumulative_worker_result_from_authority(
            query.candidate_root,
            expected_receipt_sha256=expected_receipt_sha256,
        )
    assert not promotion_path.exists()
    authority_path.write_bytes(authority_bytes)

    def source_bomb(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("closure-only recovery must not regenerate commits")

    monkeypatch.setattr(workers_module, "iter_capacity_commits", source_bomb)
    monkeypatch.setattr(
        workers_module,
        "run_phase1c_cumulative_capacity_worker",
        source_bomb,
    )
    candidate_file = (
        query.candidate_root
        / durable.terminal_shared_candidate_tree.files[0].relative_path
    )
    candidate_bytes = candidate_file.read_bytes()
    candidate_file.write_bytes(candidate_bytes + b"tamper")
    with pytest.raises(
        Phase1CWorkerResultError,
        match="changed after receipt publication",
    ):
        close_phase1c_cumulative_worker_result_from_authority(
            query.candidate_root,
            expected_receipt_sha256=expected_receipt_sha256,
        )
    assert not promotion_path.exists()
    candidate_file.write_bytes(candidate_bytes)
    closure = close_phase1c_cumulative_worker_result_from_authority(
        query.candidate_root,
        expected_receipt_sha256=expected_receipt_sha256,
    )
    assert closure.authority == authority
    assert closure.durable.promotion is not None
    assert closure.payload() == receipt_mapping["result"]
    assert hashlib.sha256(canonical_json_bytes(closure.payload())).hexdigest() == (
        closure.durable.receipt.result_sha256
    )
    assert closure.accounting.commits_ingested == 4
    assert closure.accounting.prefix_commits_reingested == 0
    assert closure.durable.terminal_shared_candidate_tree.root == request.candidate_root
    repeated = close_phase1c_cumulative_worker_result_from_authority(
        query.candidate_root,
        expected_receipt_sha256=expected_receipt_sha256,
    )
    assert repeated.payload() == closure.payload()
    assert repeated.durable.promotion == closure.durable.promotion


def test_request_preflight_rejects_stale_terminal_sidecars(
    tmp_path: Path,
) -> None:
    manifests = _manifests()
    candidate_root = (tmp_path / "stale-fresh").absolute()
    receipt_path, promotion_path = phase1c_cumulative_worker_result_paths(
        candidate_root
    )
    authority_path = phase1c_cumulative_worker_receipt_authority_path(
        candidate_root
    )
    receipt_path.write_bytes(b"{}")
    with pytest.raises(Phase1CWorkerError) as caught:
        Phase1CCumulativeCapacityWorkerRequest(
            manifests=manifests,
            candidate_root=candidate_root,
            code_identity=Hash32(b"\x91" * 32),
            runtime_identity=Hash32(b"\x92" * 32),
            batch_size=2,
        )
    assert caught.value.code is Phase1CWorkerErrorCode.INPUT_INVALID
    receipt_path.unlink()
    promotion_path.write_bytes(b"{}")
    with pytest.raises(Phase1CWorkerError) as caught:
        Phase1CCumulativeCapacityWorkerRequest(
            manifests=manifests,
            candidate_root=candidate_root,
            code_identity=Hash32(b"\x91" * 32),
            runtime_identity=Hash32(b"\x92" * 32),
            batch_size=2,
        )
    assert caught.value.code is Phase1CWorkerErrorCode.INPUT_INVALID

    promotion_path.unlink()
    authority_path.write_bytes(b"{}")
    with pytest.raises(Phase1CWorkerError) as caught:
        Phase1CCumulativeCapacityWorkerRequest(
            manifests=manifests,
            candidate_root=candidate_root,
            code_identity=Hash32(b"\x91" * 32),
            runtime_identity=Hash32(b"\x92" * 32),
            batch_size=2,
        )
    assert caught.value.code is Phase1CWorkerErrorCode.INPUT_INVALID

    authority_path.unlink()
    candidate_root.mkdir()
    receipt_path.write_bytes(b"{}")
    with pytest.raises(Phase1CWorkerError, match="closure-only"):
        Phase1CCumulativeCapacityWorkerRequest(
            manifests=manifests,
            candidate_root=candidate_root,
            code_identity=Hash32(b"\x91" * 32),
            runtime_identity=Hash32(b"\x92" * 32),
            batch_size=2,
            resume_existing=True,
        )


def test_certification_keeps_one_cumulative_worker_source_call() -> None:
    source = inspect.getsource(run_phase1c_measurements)
    assert source.count("run_phase1c_cumulative_capacity_worker(") == 1
