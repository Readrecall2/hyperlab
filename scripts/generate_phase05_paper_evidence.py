from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from hyperlab.paper.phase05_portfolio import build_phase05_phase08_paper_foundation

_EVIDENCE_TIME = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)


def canonical_lf_text_bytes(payload: bytes) -> bytes:
    """Return strict UTF-8 text with checkout-independent LF endings."""

    return payload.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")


def build_evidence(repository_root: Path) -> dict[str, object]:
    report_root = repository_root / "reports" / "phase12-phase05"
    benchmark_path = report_root / "benchmark.json"
    if not benchmark_path.is_file():
        raise FileNotFoundError("run benchmark_paper_phase05_portfolio.py before evidence generation")
    benchmark_bytes = canonical_lf_text_bytes(benchmark_path.read_bytes())
    benchmark = json.loads(benchmark_bytes)
    warning = benchmark.get("synthetic_warning")
    if not isinstance(warning, str) or "NOT ECONOMIC" not in warning:
        raise ValueError("benchmark artifact lacks the mandatory synthetic warning")

    foundation = build_phase05_phase08_paper_foundation(
        runtime_status_path=report_root / "never-started-source-status.json",
        validation_started_at=_EVIDENCE_TIME,
    )
    try:
        config = foundation.config
        source_identity_bytes = foundation.source.identity_artifact_bytes
        source_identity = json.loads(source_identity_bytes)
        strategies = [
            {**strategy.to_dict(), "strategy_config_hash": strategy.strategy_config_hash}
            for strategy in config.strategy_configs
        ]
        return {
            "authorization": {
                "authorizes_real_money": False,
                "credential_scope": "NONE",
                "environment": "PAPER",
                "execution_network": "NONE",
                "mode": "PAPER_ONLY",
                "orders_enabled": False,
            },
            "benchmark": {
                "contract": benchmark["benchmark_contract"],
                "path": "reports/phase12-phase05/benchmark.json",
                "sha256": hashlib.sha256(benchmark_bytes).hexdigest(),
                "synthetic_warning": warning,
            },
            "economic_status": {
                "data_calibration_status": config.data_calibration_status,
                "economic_prerequisites_satisfied": config.economic_prerequisites_satisfied,
                "economically_eligible": config.economically_eligible,
                "execution_calibration_status": config.execution.calibration_status,
                "status": "TECHNICAL_ONLY_UNCALIBRATED",
            },
            "evidence_contract": "PHASE12_PHASE05_MULTISTRATEGY_TECHNICAL_EVIDENCE_V1",
            "identity": {
                "config_hash": config.config_hash,
                "engine_build_hash": config.engine_build_hash,
                "portfolio_id": config.portfolio_id,
                "release_code_sha256": config.release_code_sha256,
                "run_id": config.run_id,
                "runtime_environment_sha256": config.runtime_environment_sha256,
                "source_data_hash": config.data_hash,
            },
            "paper_configuration": config.to_dict(),
            "risk_hierarchy": {
                "portfolio": config.risk.to_dict(),
                "strategies": {
                    strategy.strategy_id: strategy.risk.to_dict() for strategy in config.strategy_configs
                },
            },
            "scope": {
                "deployed_phase08_v9_artifacts_regenerated": False,
                "live_database_accessed": False,
                "network_source_started": foundation.source.started,
                "private_api_added": False,
                "real_order_route_added": False,
                "vps_accessed": False,
            },
            "source": {
                "collector_config": asdict(foundation.source.collector_config),
                "data_hash": foundation.source.descriptor.data_hash,
                "identity_artifact": source_identity,
                "identity_artifact_sha256": hashlib.sha256(source_identity_bytes).hexdigest(),
                "name": foundation.source.descriptor.source,
                "one_shared_collector": True,
                "public_read_only": True,
                "required_instruments": list(config.required_instruments),
            },
            "strategies": strategies,
            "validation_started_at": _EVIDENCE_TIME.isoformat(timespec="microseconds"),
        }
    finally:
        foundation.source.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate fail-closed Phase 05 + Phase 08 technical Paper evidence."
    )
    parser.add_argument(
        "--repository-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    repository_root = args.repository_root.resolve()
    output = args.output or (repository_root / "reports" / "phase12-phase05" / "technical-evidence.json")
    rendered = (json.dumps(build_evidence(repository_root), indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )
    if args.check:
        if not output.is_file() or output.read_bytes() != rendered:
            raise SystemExit("Phase 05 technical evidence differs from current source/runtime")
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(rendered)
    print(rendered.decode("utf-8"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
