from __future__ import annotations

import json

import pytest

from hyperlab.paper.golden_v3 import GOLDEN_STREAM_NAMES
from hyperlab.paper.storage_v4.canonical import build_commit_logical
from hyperlab.paper.storage_v4.contracts import rematerialize_compatibility_record
from hyperlab.paper.storage_v4.golden_import import (
    GoldenCommitAssembler,
    GoldenImportError,
    GoldenImportExpectations,
)
from hyperlab.paper.storage_v4.types import Hash32, RunId, StreamId

SYNTHETIC_STORAGE_V4_WORKLOAD = True
_RUN = "SYNTHETIC_STORAGE_V4_WORKLOAD/golden-import"


def _hash(marker: int) -> str:
    return f"{marker:064x}"


def _canonical(row: dict[str, object]) -> bytes:
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


def _fixture() -> dict[str, list[dict[str, object]]]:
    commit_hashes = (_hash(101), _hash(102), _hash(103))
    projection_hashes = (_hash(201), _hash(202), _hash(203), _hash(204))
    event_hashes = (_hash(301), _hash(302), _hash(303))
    input_ids = ("input-1", "input-2", "input-3")
    commits = [
        {
            "commit_hash": commit_hashes[0],
            "commit_sequence": 1,
            "event_hashes": [event_hashes[0]],
            "first_event_sequence": 1,
            "input_id": input_ids[0],
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
            "input_id": input_ids[1],
            "last_event_sequence": None,
            "projection_hash": projection_hashes[2],
            "projection_revision": 2,
            "run_id": _RUN,
        },
        {
            "commit_hash": commit_hashes[2],
            "commit_sequence": 3,
            "event_hashes": [event_hashes[1], event_hashes[2]],
            "first_event_sequence": 2,
            "input_id": input_ids[2],
            "last_event_sequence": 3,
            "projection_hash": projection_hashes[3],
            "projection_revision": 3,
            "run_id": _RUN,
        },
    ]
    inbox = [
        {
            "commit_hash": commit_hashes[index],
            "commit_sequence": index + 1,
            "input_id": input_ids[index],
            "payload": {"type": "RUN_START" if index == 0 else "TIMER"},
            "run_id": _RUN,
        }
        for index in range(3)
    ]
    events = [
        {
            "event_hash": event_hashes[0],
            "input_id": input_ids[0],
            "payload": {"event": 1},
            "run_id": _RUN,
            "sequence": 1,
        },
        {
            "event_hash": event_hashes[1],
            "input_id": input_ids[2],
            "payload": {"event": 2},
            "run_id": _RUN,
            "sequence": 2,
        },
        {
            "event_hash": event_hashes[2],
            "input_id": input_ids[2],
            "payload": {"event": 3},
            "run_id": _RUN,
            "sequence": 3,
        },
    ]
    projections = [
        {
            "payload": {"state": "INITIAL"},
            "projection_hash": projection_hashes[0],
            "revision": 0,
            "run_id": _RUN,
        },
        {
            "payload": {"state": "RUNNING", "ratio": 0.65},
            "projection_hash": projection_hashes[1],
            "revision": 1,
            "run_id": _RUN,
        },
        {
            "payload": {"state": "PAUSED"},
            "projection_hash": projection_hashes[2],
            "revision": 2,
            "run_id": _RUN,
        },
        {
            "payload": {"state": "PAUSED"},
            "projection_hash": projection_hashes[3],
            "revision": 3,
            "run_id": _RUN,
        },
    ]
    return {
        "schema": [{"kind": "metadata", "name": "paper_schema"}],
        "run": [{"config": {"maker_probability": 0.65}, "run_id": _RUN}],
        "inbox": inbox,
        "events": events,
        "ledger_transactions": [
            {
                "commit_sequence": 1,
                "run_id": _RUN,
                "transaction_hash": _hash(401),
            }
        ],
        "ledger_entries": [
            {
                "account": "cash",
                "amount_text": "10.25",
                "commit_sequence": 1,
                "entry_hash": _hash(402),
                "run_id": _RUN,
                "unit": "USD",
            }
        ],
        "alerts": [
            {
                "code": "MARKET_GAP",
                "commit_sequence": 2,
                "run_id": _RUN,
            }
        ],
        "commits": commits,
        "projection_history": projections,
        "projection_current": [
            {
                "effective_status": "PAUSED",
                "event_head_hash": event_hashes[-1],
                "event_sequence": 3,
                "payload": {"state": "PAUSED"},
                "projection_hash": projection_hashes[-1],
                "revision": 3,
                "run_id": _RUN,
                "status": "PAUSED",
                "updated_at": "2026-08-18T12:00:03Z",
            }
        ],
        "runtime_sessions": [
            {"commit_sequence": 3, "input_id": input_ids[2], "run_id": _RUN}
        ],
        "incidents": [
            {"code": "MARKET_GAP", "commit_sequence": 2, "run_id": _RUN}
        ],
        "heads": [{"commit_count": 3, "run_id": _RUN}],
    }


def _expectations(streams: dict[str, list[dict[str, object]]]) -> GoldenImportExpectations:
    counts = tuple((name, len(streams[name])) for name in GOLDEN_STREAM_NAMES)
    return GoldenImportExpectations(
        run_id=RunId(_RUN),
        export_root=Hash32(b"\x77" * 32),
        commit_count=3,
        row_count=sum(count for _, count in counts),
        stream_row_counts=counts,
    )


def test_assembler_routes_all_13_streams_and_rematerializes_exact_jsonl() -> None:
    streams = _fixture()
    iterator = iter(GoldenCommitAssembler(streams, _expectations(streams)))
    first = next(iterator)
    second = next(iterator)
    final = next(iterator)
    final_checkpoint = final.build_checkpoint_sections()
    with pytest.raises(StopIteration):
        next(iterator)
    assembled = (first, second, final)

    assert len(assembled) == 3
    assert assembled[0].frame.previous_prefix_root == Hash32(b"\x77" * 32)
    assert assembled[1].frame.previous_prefix_root == build_commit_logical(
        assembled[0].frame
    ).prefix_root
    assert assembled[-1].cumulative_rows == 22
    assert final_checkpoint.adapter_state["processed_commits"] == 3
    assert final_checkpoint.ledger_state["balances"] == {
        "cash": {"USD": "10.25"}
    }

    actual = {name: bytearray() for name in GOLDEN_STREAM_NAMES}
    for item in assembled:
        for row in item.frame.rows:
            actual[row.stream_id.value].extend(rematerialize_compatibility_record(row))
    expected = {
        name: b"".join(_canonical(row) for row in streams[name])
        for name in GOLDEN_STREAM_NAMES
    }
    assert {name: bytes(value) for name, value in actual.items()} == expected
    assert b'"maker_probability":0.65' in bytes(actual["run"])
    assert b'"code":"MARKET_GAP"' in bytes(actual["alerts"])
    assert sum(len(item.frame.rows) for item in assembled) == 22
    assert {row.stream_id for row in assembled[0].frame.rows} >= {
        StreamId("schema"), StreamId("projection_history")
    }
    assert StreamId("run") not in {
        row.stream_id for row in assembled[0].frame.rows
    }
    assert StreamId("events") not in {
        row.stream_id for row in assembled[1].frame.rows
    }
    assert {row.stream_id for row in assembled[-1].frame.rows} >= {
        StreamId("heads"), StreamId("projection_current"), StreamId("run")
    }


def test_checkpoint_snapshot_is_lazy_and_stale_builders_fail_closed() -> None:
    streams = _fixture()
    iterator = iter(GoldenCommitAssembler(streams, _expectations(streams)))
    first = next(iterator)
    assert first.build_checkpoint_sections().adapter_state["processed_commits"] == 1

    second = next(iterator)
    with pytest.raises(GoldenImportError, match="after assembler advanced"):
        first.build_checkpoint_sections()
    assert second.build_checkpoint_sections().adapter_state["processed_commits"] == 2


def test_checkpoint_ledger_sum_has_no_hundred_digit_rounding_ceiling() -> None:
    streams = _fixture()
    large = "1" + ("0" * 120)
    streams["ledger_entries"] = [
        {
            "account": "cash",
            "amount_text": large,
            "commit_sequence": 1,
            "entry_hash": _hash(501),
            "run_id": _RUN,
            "unit": "USD",
        },
        {
            "account": "cash",
            "amount_text": "1",
            "commit_sequence": 2,
            "entry_hash": _hash(502),
            "run_id": _RUN,
            "unit": "USD",
        },
        {
            "account": "cash",
            "amount_text": "-" + large,
            "commit_sequence": 3,
            "entry_hash": _hash(503),
            "run_id": _RUN,
            "unit": "USD",
        },
    ]
    iterator = iter(GoldenCommitAssembler(streams, _expectations(streams)))
    next(iterator)
    next(iterator)
    final = next(iterator)

    assert final.build_checkpoint_sections().ledger_state["balances"] == {
        "cash": {"USD": "1"}
    }


def test_assembler_refuses_uncommitted_alert_in_certified_import() -> None:
    streams = _fixture()
    streams["alerts"][0]["commit_sequence"] = None

    with pytest.raises(GoldenImportError, match="uncommitted"):
        tuple(GoldenCommitAssembler(streams, _expectations(streams)))


def test_assembler_refuses_event_hash_or_range_divergence() -> None:
    streams = _fixture()
    streams["events"][0]["event_hash"] = _hash(999)

    with pytest.raises(GoldenImportError, match="event hash"):
        tuple(GoldenCommitAssembler(streams, _expectations(streams)))
