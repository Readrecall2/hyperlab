from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scripts.generate_phase05_paper_evidence import build_evidence, canonical_lf_text_bytes

_ROOT = Path(__file__).resolve().parents[1]
_EVIDENCE = _ROOT / "reports" / "phase12-phase05" / "technical-evidence.json"
_BENCHMARK = _ROOT / "reports" / "phase12-phase05" / "benchmark.json"
_CANONICAL_BENCHMARK_SHA256 = "c42b11557e437306f549a26d778657faa183bd0d1dfc870fb68dbf591a86dfff"


def test_phase05_release_inputs_are_lf_canonical_and_checkout_independent(tmp_path: Path) -> None:
    benchmark_bytes = _BENCHMARK.read_bytes()
    assert b"\r" not in benchmark_bytes
    assert hashlib.sha256(benchmark_bytes).hexdigest() == _CANONICAL_BENCHMARK_SHA256

    crlf_root = tmp_path / "crlf-checkout"
    crlf_benchmark = crlf_root / "reports" / "phase12-phase05" / "benchmark.json"
    crlf_benchmark.parent.mkdir(parents=True)
    crlf_benchmark.write_bytes(benchmark_bytes.replace(b"\n", b"\r\n"))
    assert build_evidence(crlf_root)["benchmark"] == build_evidence(_ROOT)["benchmark"]
    assert canonical_lf_text_bytes(crlf_benchmark.read_bytes()) == benchmark_bytes

    attributes = (_ROOT / ".gitattributes").read_text(encoding="utf-8").splitlines()
    for relative_path in (
        "config/paper/phase08-v9-historical-attestation.json",
        "reports/phase12-phase05/benchmark.json",
        "reports/phase12-phase05/technical-evidence.json",
    ):
        assert f"{relative_path} text eol=lf" in attributes


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
