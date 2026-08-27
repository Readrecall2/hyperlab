from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from hyperlab.research_data.envelope import Venue
from hyperlab.research_data.prediction_bundle import (
    CAMPAIGN_BOUND_EXCLUDED_SLOT_RECEIPT,
    PredictionPublicSourceInvalidReceipt,
)
from hyperlab.research_data.prediction_candidate import CandidatePreregistration

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/fixtures/prediction_markets/real_receipt_auth_20260827"
EXPECTED = {
    "kalshi": {
        "bytes": 508_740,
        "config_sha256": "ad462480fca9d9878b2f17891a2a1115e38ad73f6c27b5fe53a2d24ea3531d28",
        "error": "ValueError:source timestamp must be absent or a non-negative UTC epoch value",
        "frames": 10,
        "manifest_sha256": "db18b1cc74f9b2c7b78c458bf3bd358ec365e35a2d28699940fcf2507929ad82",
        "network_calls": 11,
        "result_sha256": "e9a5147132e8318ba52e00b4c62090c1b92e01f466c38de5ad4786471a635e7f",
        "root_sha256": "aacf0d449b4bedb6e49c83f3f3eaf8051baedd584cd9777bc3ff1cd031d768da",
        "segments": 2,
    },
    "polymarket": {
        "bytes": 17_053,
        "config_sha256": "f9422f16c6c04bbf90d8204448aabcd8a60306172973c3b4acc8916b73957cb8",
        "error": "ValueError:Polymarket CLOB market-info identity graph diverged",
        "frames": 1,
        "manifest_sha256": "ebde9477cecbca07aa69cef75adc68d3a1a797b895141ac0b9995f0c931226de",
        "network_calls": 2,
        "result_sha256": "98c1f7c53c25533ce61e6f5e692551e76db009cd331a42c54d201d28b1ec3b85",
        "root_sha256": "4e3447b1773b72038c7d24acfa197db5d384ae1c896c91af5391c1ea429e7231",
        "segments": 1,
    },
}


def _forensic_bytes(path: Path) -> bytes:
    raw = path.read_bytes()
    return raw[:-1] if raw.endswith(b"\n") else raw


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


@pytest.mark.parametrize("venue", ["kalshi", "polymarket"])
def test_real_forensic_public_source_invalid_receipt_is_admitted_as_metadata_only(
    venue: str,
) -> None:
    expected = EXPECTED[venue]
    venue_root = FIXTURE / venue
    config_raw = _forensic_bytes(venue_root / "probe-config.json")
    result_raw = _forensic_bytes(venue_root / "result.json")
    manifest_raw = _forensic_bytes(venue_root / "manifest.json")

    assert hashlib.sha256(config_raw).hexdigest() == expected["config_sha256"]
    assert hashlib.sha256(result_raw).hexdigest() == expected["result_sha256"]
    assert hashlib.sha256(manifest_raw).hexdigest() == expected["manifest_sha256"]
    provenance = json.loads(_forensic_bytes(FIXTURE / "fixture-provenance.json"))
    assert provenance == {
        "archive_sha256": "6e7c094dfb45d901f2f1b77bde8e53958075e9e67349e5cb9f4d125b1c031ea8",
        "boundary": "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY",
        "forensic_inventory_sha256": "4f44b2f151e9ed28c2a6ac10dd719de5a096759b3f0d3cc07b8c0f9a29bdae16",
        "fixture_classification": "AUTHENTICATED_REAL_PUBLIC_RECEIPT_METADATA_NO_RAW_SEGMENTS_NOT_ECONOMIC_EVIDENCE",
        "raw_segments_included": 0,
        "schema_version": 1,
        "source_commit": "6f59caae46e7f473cee9dec00103f4157920f8cb",
    }

    receipt = PredictionPublicSourceInvalidReceipt.from_report_bytes(
        probe_config_raw=config_raw,
        terminal_result_raw=result_raw,
    )
    preregistration = CandidatePreregistration.from_path(
        ROOT / "config/research/prediction-markets-candidate-v1.json"
    )
    plan = preregistration.collection_plans[Venue(venue)]
    receipt.binding.verify_collection_plan(plan)
    manifest = json.loads(manifest_raw)
    assert receipt.classification == CAMPAIGN_BOUND_EXCLUDED_SLOT_RECEIPT
    assert receipt.source_usable is False
    assert receipt.economic_eligible is False
    assert receipt.terminal_error == expected["error"]
    assert receipt.frame_count == expected["frames"] == manifest["frame_count"]
    assert receipt.byte_count == expected["bytes"] == manifest["stored_segment_bytes"]
    assert receipt.segment_count == expected["segments"] == manifest["segment_count"]
    assert receipt.network_calls == expected["network_calls"]
    assert receipt.raw_manifest_sha256 == expected["manifest_sha256"]
    assert receipt.raw_root_sha256 == expected["root_sha256"] == manifest["root_sha256"]
    assert receipt.terminal_result_sha256 == expected["result_sha256"]
    with pytest.raises(ValueError, match="frozen plan"):
        receipt.binding.verify_collection_plan(
            replace(plan, max_network_calls=plan.max_network_calls - 1)
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("error", None, "not admissible"),
        ("error", "", "not admissible"),
        ("error", "x" * 2_049, "not admissible"),
        ("terminal_health", "COMPLETE", "not admissible"),
        ("manifest_sha256", "0" * 63, "not admissible"),
        ("campaign_manifest_sha256", "0" * 64, "not admissible"),
        ("frames", 501, "not admissible"),
        ("source_timestamp_min_ns", -1, "not admissible"),
    ],
)
def test_real_forensic_invalid_receipt_corruption_stays_fail_closed(
    field: str,
    value: object,
    message: str,
) -> None:
    root = FIXTURE / "polymarket"
    config_raw = _forensic_bytes(root / "probe-config.json")
    result = json.loads(_forensic_bytes(root / "result.json"))
    result[field] = value
    with pytest.raises(ValueError, match=message):
        PredictionPublicSourceInvalidReceipt.from_report_bytes(
            probe_config_raw=config_raw,
            terminal_result_raw=_canonical(result),
        )


def test_real_forensic_invalid_receipt_rejects_inverted_timestamp_bounds() -> None:
    root = FIXTURE / "kalshi"
    result = json.loads(_forensic_bytes(root / "result.json"))
    result["source_timestamp_min_ns"] = 2
    result["source_timestamp_max_ns"] = 1
    with pytest.raises(ValueError, match="not admissible"):
        PredictionPublicSourceInvalidReceipt.from_report_bytes(
            probe_config_raw=_forensic_bytes(root / "probe-config.json"),
            terminal_result_raw=_canonical(result),
        )
