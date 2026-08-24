from __future__ import annotations

import hashlib
import json
import shutil
import stat
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from test_paper_golden_v3 import (
    _build_source,
    _export,
    _manifest,
    _read_stream_rows,
    _stream_paths,
)

import hyperlab.paper.golden_v3 as golden_v3_core
from hyperlab.backtest.protocol import canonical_json, canonical_sha256
from hyperlab.paper.golden_v3 import (
    GoldenDifferentialError,
    GoldenVerificationError,
    compare_golden_exports,
    verify_golden_v3,
    write_external_pin,
)


def _rewrite_single_shard(
    export_root: Path,
    stream_name: str,
    mutate: Callable[[list[dict[str, Any]]], None],
) -> None:
    paths = _stream_paths(export_root, stream_name)
    assert len(paths) == 1
    rows = _read_stream_rows(export_root, stream_name)
    mutate(rows)
    paths[0].write_text(
        "".join(f"{canonical_json(row)}\n" for row in rows),
        encoding="utf-8",
        newline="\n",
    )


def _reauthenticate_single_shard(export_root: Path, stream_name: str) -> None:
    manifest = _manifest(export_root)
    streams = manifest["streams"]
    assert isinstance(streams, dict)
    stream = streams[stream_name]
    assert isinstance(stream, dict)
    shards = stream["shards"]
    assert isinstance(shards, list) and len(shards) == 1
    shard = shards[0]
    assert isinstance(shard, dict)
    path = export_root / str(shard["path"])
    rows = _read_stream_rows(export_root, stream_name)
    logical = b"".join(golden_v3_core._canonical_line(row) for row in rows)
    replay = b"".join(
        golden_v3_core._canonical_line(
            golden_v3_core.golden_replay_semantic_row(stream_name, row)
        )
        for row in rows
    )
    identities = [golden_v3_core._row_identity(stream_name, row) for row in rows]
    first = list(identities[0]) if identities else None
    last = list(identities[-1]) if identities else None
    logical_hash = hashlib.sha256(logical).hexdigest()
    shard.update(
        {
            "first_identity": first,
            "last_identity": last,
            "logical_sha256": logical_hash,
            "logical_size": len(logical),
            "physical_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "physical_size": path.stat().st_size,
            "row_count": len(rows),
        }
    )
    stream.update(
        {
            "first_identity": first,
            "last_identity": last,
            "logical_sha256": logical_hash,
            "logical_size": len(logical),
            "replay_sha256": hashlib.sha256(replay).hexdigest(),
            "replay_size": len(replay),
            "row_count": len(rows),
        }
    )
    root_hash = golden_v3_core._manifest_root(manifest)
    manifest["root_hash"] = root_hash
    manifest_payload = golden_v3_core._canonical_line(manifest)
    (export_root / "manifest.json").write_bytes(manifest_payload)
    complete = {
        "format": manifest["format"],
        "manifest_sha256": hashlib.sha256(manifest_payload).hexdigest(),
        "root_hash": root_hash,
        "run_id": manifest["run_id"],
        "status": "COMPLETE",
    }
    (export_root / "COMPLETE").write_bytes(
        golden_v3_core._canonical_line(complete)
    )


def _omit_row(rows: list[dict[str, Any]]) -> None:
    assert len(rows) >= 3
    rows.pop(1)


def _duplicate_row(rows: list[dict[str, Any]]) -> None:
    assert len(rows) >= 2
    rows.insert(1, dict(rows[0]))


def _reorder_rows(rows: list[dict[str, Any]]) -> None:
    assert len(rows) >= 2
    rows[0], rows[1] = rows[1], rows[0]


def _alter_payload(rows: list[dict[str, Any]]) -> None:
    payload = rows[0]["payload"]
    assert isinstance(payload, dict)
    payload["synthetic_tamper"] = "payload altered"


def _same_id_different_payload(rows: list[dict[str, Any]]) -> None:
    input_id = rows[0]["input_id"]
    payload = rows[0]["payload"]
    assert isinstance(payload, dict)
    payload["synthetic_tamper"] = "same input_id, different payload"
    assert rows[0]["input_id"] == input_id


def _alter_commit_previous(rows: list[dict[str, Any]]) -> None:
    assert len(rows) >= 2
    rows[1]["previous_commit_hash"] = "f" * 64


def _alter_ledger_transaction(rows: list[dict[str, Any]]) -> None:
    assert rows
    rows[0]["transaction_hash"] = "e" * 64


def _omit_nonzero_projection_revision(rows: list[dict[str, Any]]) -> None:
    nonzero = next(index for index, row in enumerate(rows) if int(row["revision"]) > 0)
    rows.pop(nonzero)


def _omit_projection_revision_zero(rows: list[dict[str, Any]]) -> None:
    zero = next(index for index, row in enumerate(rows) if int(row["revision"]) == 0)
    rows.pop(zero)


def _omit_unlinked_alert(rows: list[dict[str, Any]]) -> None:
    unlinked = next(
        index for index, row in enumerate(rows) if row.get("commit_sequence") is None
    )
    rows.pop(unlinked)


@pytest.mark.parametrize(
    ("stream_name", "mutate"),
    [
        pytest.param("events", _omit_row, id="row-omitted"),
        pytest.param("events", _duplicate_row, id="row-duplicated"),
        pytest.param("events", _reorder_rows, id="row-reordered"),
        pytest.param("events", _alter_payload, id="payload-altered"),
        pytest.param("inbox", _same_id_different_payload, id="same-id-different-payload"),
        pytest.param("commits", _alter_commit_previous, id="commit-previous-altered"),
        pytest.param(
            "ledger_transactions",
            _alter_ledger_transaction,
            id="ledger-transaction-altered",
        ),
        pytest.param(
            "projection_history",
            _omit_nonzero_projection_revision,
            id="projection-revision-missing",
        ),
        pytest.param(
            "projection_history",
            _omit_projection_revision_zero,
            id="projection-revision-zero-missing",
        ),
        pytest.param("alerts", _omit_unlinked_alert, id="unlinked-alert-omitted"),
    ],
)
def test_differential_rejects_every_logical_history_divergence(
    tmp_path: Path,
    stream_name: str,
    mutate: Callable[[list[dict[str, Any]]], None],
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    expected = tmp_path / "expected"
    actual = tmp_path / "actual"
    _export(source, expected, run_id)
    _export(source, actual, run_id)
    _rewrite_single_shard(actual, stream_name, mutate)

    with pytest.raises(GoldenDifferentialError):
        compare_golden_exports(expected, actual)


@pytest.mark.parametrize(
    ("stream_name", "mutate", "error_pattern"),
    [
        pytest.param("events", _omit_row, r"sequence.*gap", id="omitted-event"),
        pytest.param(
            "events", _duplicate_row, r"order|duplicated", id="duplicated-event"
        ),
        pytest.param("events", _reorder_rows, r"order|reordered", id="reordered-event"),
        pytest.param(
            "projection_history",
            _omit_projection_revision_zero,
            r"revision zero|gap",
            id="missing-revision-zero",
        ),
    ],
)
def test_verifier_rejects_reauthenticated_logical_order_or_gap_corruption(
    tmp_path: Path,
    stream_name: str,
    mutate: Callable[[list[dict[str, Any]]], None],
    error_pattern: str,
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    exported = tmp_path / "golden"
    _export(source, exported, run_id)
    _rewrite_single_shard(exported, stream_name, mutate)
    _reauthenticate_single_shard(exported, stream_name)

    with pytest.raises(GoldenVerificationError, match=error_pattern):
        verify_golden_v3(exported)


def _alter_ledger_entry(rows: list[dict[str, Any]]) -> None:
    payload = rows[0]["payload"]
    assert isinstance(payload, dict)
    payload["synthetic_tamper"] = "reauthenticated ledger mutation"


def _alter_alert(rows: list[dict[str, Any]]) -> None:
    payload = rows[0]["payload"]
    assert isinstance(payload, dict)
    payload["synthetic_tamper"] = "reauthenticated alert mutation"


def _alter_head(rows: list[dict[str, Any]]) -> None:
    rows[0]["projection_hash"] = "0" * 64


@pytest.mark.parametrize(
    ("stream_name", "mutate"),
    [
        pytest.param("ledger_entries", _alter_ledger_entry, id="ledger-entry"),
        pytest.param("alerts", _alter_alert, id="alert"),
        pytest.param("commits", _alter_commit_previous, id="commit-components"),
        pytest.param("projection_current", _alter_payload, id="projection-current"),
        pytest.param("heads", _alter_head, id="heads"),
    ],
)
def test_differential_rejects_reauthenticated_stable_identity_mutation(
    tmp_path: Path,
    stream_name: str,
    mutate: Callable[[list[dict[str, Any]]], None],
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    expected = tmp_path / "expected"
    actual = tmp_path / "actual"
    _export(source, expected, run_id)
    _export(source, actual, run_id)
    _rewrite_single_shard(actual, stream_name, mutate)
    _reauthenticate_single_shard(actual, stream_name)

    verify_golden_v3(actual)
    with pytest.raises(GoldenDifferentialError, match=r"logical histories differ"):
        compare_golden_exports(expected, actual)


def test_differential_rejects_two_valid_complete_but_distinct_histories(
    tmp_path: Path,
) -> None:
    expected_source = tmp_path / "expected-source.sqlite3"
    actual_source = tmp_path / "actual-source.sqlite3"
    run_id = _build_source(expected_source, market_count=5)
    assert _build_source(actual_source, market_count=6) == run_id
    expected = tmp_path / "expected"
    actual = tmp_path / "actual"
    _export(expected_source, expected, run_id)
    _export(actual_source, actual, run_id)
    verify_golden_v3(expected)
    verify_golden_v3(actual)

    with pytest.raises(GoldenDifferentialError, match=r"logical histories differ"):
        compare_golden_exports(expected, actual)


def test_verifier_rejects_truncated_shard(tmp_path: Path) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    exported = tmp_path / "golden"
    _export(source, exported, run_id)
    shard = _stream_paths(exported, "events")[0]
    original = shard.read_bytes()
    assert original.endswith(b"\n")
    shard.write_bytes(original[:-7])

    with pytest.raises(GoldenVerificationError, match=r"shard|physical|truncated|hash"):
        verify_golden_v3(exported)


def test_verifier_rejects_manifest_replaced_by_another_complete_export(
    tmp_path: Path,
) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    victim = tmp_path / "victim"
    other = tmp_path / "other"
    _export(source, victim, run_id)
    _export(source, other, run_id)
    other_manifest = _manifest(other)
    other_manifest["root_hash"] = "9" * 64
    (other / "manifest.json").write_text(
        f"{canonical_json(other_manifest)}\n",
        encoding="utf-8",
        newline="\n",
    )
    shutil.copyfile(other / "manifest.json", victim / "manifest.json")

    with pytest.raises(GoldenVerificationError, match=r"manifest|COMPLETE|root"):
        verify_golden_v3(victim)


def test_verifier_rejects_reauthenticated_numeric_string_counts(tmp_path: Path) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    exported = tmp_path / "golden"
    _export(source, exported, run_id)
    manifest = _manifest(exported)
    streams = manifest["streams"]
    assert isinstance(streams, dict)
    events = streams["events"]
    assert isinstance(events, dict)
    events["row_count"] = str(events["row_count"])
    root_material = dict(manifest)
    root_material.pop("root_hash")
    root_hash = canonical_sha256(
        {"domain": "hyperlab-paper-golden-v3-root-v1", "manifest": root_material}
    )
    manifest["root_hash"] = root_hash
    manifest_payload = f"{canonical_json(manifest)}\n".encode()
    (exported / "manifest.json").write_bytes(manifest_payload)
    complete = {
        "format": manifest["format"],
        "manifest_sha256": hashlib.sha256(manifest_payload).hexdigest(),
        "root_hash": root_hash,
        "run_id": run_id,
        "status": "COMPLETE",
    }
    (exported / "COMPLETE").write_text(
        f"{canonical_json(complete)}\n",
        encoding="utf-8",
        newline="\n",
    )

    with pytest.raises(GoldenVerificationError, match=r"row_count.*integer"):
        verify_golden_v3(exported)


def test_verifier_rejects_external_root_mismatch(tmp_path: Path) -> None:
    source = tmp_path / "paper-source.sqlite3"
    run_id = _build_source(source)
    exported = tmp_path / "golden"
    pin_path = tmp_path / "golden.pin.json"
    _export(source, exported, run_id)
    write_external_pin(exported, pin_path)
    pin_path.chmod(stat.S_IREAD | stat.S_IWRITE)
    pin = json.loads(pin_path.read_text(encoding="utf-8"))
    pin["root_hash"] = "0" * 64
    pin_path.write_text(
        f"{canonical_json(pin)}\n",
        encoding="utf-8",
        newline="\n",
    )

    with pytest.raises(GoldenVerificationError, match=r"external|pin|root"):
        verify_golden_v3(exported, pin_path=pin_path)
