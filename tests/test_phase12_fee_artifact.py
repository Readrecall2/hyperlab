from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from fnmatch import fnmatchcase
from pathlib import Path

from hyperlab.paper.public_source import PublicRecordMarketEventAdapter


def test_hyperliquid_public_fee_artifact_is_conservative_and_non_promotable() -> None:
    root = Path(__file__).resolve().parents[1]
    artifact = json.loads(
        (root / "config/paper/hyperliquid-tier0-fees-2026-08-16.json").read_text(
            encoding="utf-8"
        )
    )

    assert artifact["artifact_version"] == 2
    assert artifact["scope"] == "OFFICIAL_PUBLIC_FEES_ONLY"
    assert artifact["economic_eligibility"] is False
    assert artifact["status"] == "BLOCKED_INCOMPLETE_EXECUTION_CALIBRATION"
    provenance = artifact["provenance"]
    assert provenance["publisher"] == "Hyperliquid"
    assert provenance["retrieved_at_utc"] == "2026-08-16T21:06:42Z"
    assert provenance["publisher_effective_from"] is None
    assert provenance["publisher_effective_to"] is None
    receipt_path = root / provenance["source_receipt_path"]
    receipt_bytes = receipt_path.read_bytes()
    assert hashlib.sha256(receipt_bytes).hexdigest() == provenance["source_receipt_sha256"]
    receipt = json.loads(receipt_bytes)
    assert (
        receipt["capture"]["decoded_content_sha256"]
        == provenance["source_content_sha256"]
    )
    assert receipt["capture"]["http_etag"]
    assert receipt["capture"]["retrieved_at_utc"] == provenance["retrieved_at_utc"]
    assert receipt["effective_interval"]["historical_use_before_observation"] is False
    assert receipt["capture"]["raw_response_bytes_stored"] is False
    capture_binding = receipt["content_addressed_table_capture"]
    capture_bytes = (root / capture_binding["path"]).read_bytes()
    assert hashlib.sha256(capture_bytes).hexdigest() == capture_binding["sha256"]
    capture = json.loads(capture_bytes)
    assert capture["source_content_sha256"] == provenance["source_content_sha256"]
    assert capture["limitations"]["full_decoded_page_stored"] is False
    captured_percent = {
        row["market"]: (row["maker_fee_percent"], row["taker_fee_percent"])
        for row in capture["table_rows"]
    }
    assert captured_percent == {
        "Perps": ("0.015", "0.045"),
        "Spot": ("0.040", "0.070"),
    }

    rules = {
        rule["instrument_pattern"]: (
            rule["maker_fee_bps"],
            rule["taker_fee_bps"],
        )
        for rule in artifact["official_fee_rules"]
    }
    assert rules == {
        "HL:*:perp": ("1.5", "4.5"),
        "HL:*:spot": ("4.0", "7.0"),
    }
    assert tuple(Decimal(value) * 100 for value in captured_percent["Perps"]) == tuple(
        Decimal(value) for value in rules["HL:*:perp"]
    )
    assert tuple(Decimal(value) * 100 for value in captured_percent["Spot"]) == tuple(
        Decimal(value) for value in rules["HL:*:spot"]
    )
    source = PublicRecordMarketEventAdapter(
        instruments={
            ("hyperliquid", "BTC"): "HL:BTC:perp",
            ("hyperliquid", "UBTC"): "HL:UBTC:spot",
        },
        queue_capacity=128,
    )
    for instrument in source.instruments.values():
        assert sum(fnmatchcase(instrument, pattern) for pattern in rules) == 1

    attributes = (root / ".gitattributes").read_text(encoding="utf-8").splitlines()
    for relative_path in (
        "config/paper/hyperliquid-tier0-fees-2026-08-16.json",
        "config/paper/hyperliquid-fees-source-receipt-2026-08-16.json",
        "config/paper/hyperliquid-tier0-fee-table-capture-2026-08-16.json",
    ):
        assert f"{relative_path} text eol=lf" in attributes
    policy = artifact["policy"]
    assert policy["tier"] == "public tier 0"
    assert policy["account_or_private_data_used"] is False
    for discount in (
        "aligned_quote_discount_assumed",
        "maker_rebate_assumed",
        "referral_discount_assumed",
        "staking_discount_assumed",
        "volume_discount_assumed",
    ):
        assert policy[discount] is False
    assert artifact["unresolved_calibration"]
