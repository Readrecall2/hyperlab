from __future__ import annotations

import json
from pathlib import Path

from scripts.generate_phase05_paper_evidence import build_evidence

_ROOT = Path(__file__).resolve().parents[1]
_EVIDENCE = _ROOT / "reports" / "phase12-phase05" / "technical-evidence.json"


def test_phase05_phase08_technical_evidence_is_exact_and_non_authorizing() -> None:
    expected = json.dumps(build_evidence(_ROOT), indent=2, sort_keys=True) + "\n"

    assert _EVIDENCE.read_text(encoding="utf-8") == expected
    evidence = json.loads(expected)
    assert evidence["evidence_contract"] == (
        "PHASE12_PHASE05_MULTISTRATEGY_TECHNICAL_EVIDENCE_V1"
    )
    assert evidence["economic_status"] == {
        "data_calibration_status": "UNCALIBRATED",
        "economic_prerequisites_satisfied": False,
        "economically_eligible": False,
        "execution_calibration_status": "UNCALIBRATED",
        "status": "TECHNICAL_ONLY_UNCALIBRATED",
    }
    assert evidence["authorization"] == {
        "authorizes_real_money": False,
        "credential_scope": "NONE",
        "environment": "PAPER",
        "execution_network": "NONE",
        "mode": "PAPER_ONLY",
        "orders_enabled": False,
    }
    assert evidence["scope"] == {
        "deployed_phase08_v9_artifacts_regenerated": False,
        "live_database_accessed": False,
        "network_source_started": False,
        "private_api_added": False,
        "real_order_route_added": False,
        "vps_accessed": False,
    }
    assert [item["strategy_id"] for item in evidence["strategies"]] == [
        "phase05_cash_and_carry",
        "phase08_robust_pairs",
    ]
    assert evidence["source"]["one_shared_collector"] is True
    assert evidence["source"]["public_read_only"] is True
    assert evidence["benchmark"]["synthetic_warning"].startswith(
        "SYNTHETIC TECHNICAL THROUGHPUT ONLY"
    )
