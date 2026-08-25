"""Certify one new Storage v4 Phase 1B import from a pinned Golden V3 export."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from hyperlab.paper.storage_v4.phase1b_certification import (
    DEFAULT_HEARTBEAT_SECONDS,
    MAX_SAFETY_SECONDS,
    Phase1BCertificationConfig,
    certify_storage_v4_phase1b,
    failure_verdict,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Stream a pinned Golden V3 export into a new checkpointed Storage v4 "
            "repository and publish COMPLETE only after exhaustive equivalence."
        )
    )
    parser.add_argument("--golden-root", type=Path, required=True)
    parser.add_argument("--golden-pin", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--expected-golden-root", required=True)
    parser.add_argument("--expected-source-sha256", required=True)
    parser.add_argument("--expected-run-id", required=True)
    parser.add_argument("--config-hash", required=True)
    parser.add_argument("--release-code-sha256", required=True)
    parser.add_argument("--runtime-environment-sha256", required=True)
    parser.add_argument("--certifier-code-sha256", required=True)
    parser.add_argument("--certifier-runtime-environment-sha256", required=True)
    parser.add_argument("--store-id", default="golden-v3-storage-v4-phase1b")
    parser.add_argument("--seal-rows", type=int, default=50_000)
    parser.add_argument("--seal-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--codec-level", type=int, default=6)
    parser.add_argument(
        "--heartbeat-seconds",
        type=float,
        default=DEFAULT_HEARTBEAT_SECONDS,
        help="durable heartbeat interval; must remain between 30 and 60 seconds",
    )
    parser.add_argument(
        "--safety-seconds",
        type=float,
        default=MAX_SAFETY_SECONDS,
        help="strict offline safety ceiling; may not exceed 7200 seconds",
    )
    return parser


def _config(namespace: argparse.Namespace) -> Phase1BCertificationConfig:
    return Phase1BCertificationConfig(
        golden_root=namespace.golden_root,
        golden_pin=namespace.golden_pin,
        output_root=namespace.output_root,
        expected_golden_root=namespace.expected_golden_root,
        expected_source_sha256=namespace.expected_source_sha256,
        expected_run_id=namespace.expected_run_id,
        config_hash=namespace.config_hash,
        release_code_sha256=namespace.release_code_sha256,
        runtime_environment_sha256=namespace.runtime_environment_sha256,
        certifier_code_sha256=namespace.certifier_code_sha256,
        certifier_runtime_environment_sha256=(
            namespace.certifier_runtime_environment_sha256
        ),
        store_id=namespace.store_id,
        seal_rows=namespace.seal_rows,
        seal_bytes=namespace.seal_bytes,
        codec_level=namespace.codec_level,
        heartbeat_seconds=namespace.heartbeat_seconds,
        safety_seconds=namespace.safety_seconds,
    )


def _emit(payload: dict[str, object], *, error: bool = False) -> None:
    print(
        json.dumps(payload, sort_keys=True, separators=(",", ":")),
        file=sys.stderr if error else sys.stdout,
        flush=True,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    namespace = parser.parse_args(argv)
    try:
        config = _config(namespace)
        result = certify_storage_v4_phase1b(config)
    except KeyboardInterrupt:
        _emit({"status": "INTERRUPTED"}, error=True)
        return 130
    except TimeoutError as error:
        _emit(
            {
                "error": str(error),
                "error_type": type(error).__name__,
                "status": failure_verdict(error),
                "timed_out": True,
            },
            error=True,
        )
        return 124
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        _emit(
            {
                "error": str(error),
                "error_type": type(error).__name__,
                "status": failure_verdict(error),
            },
            error=True,
        )
        return 2
    _emit(
        {
            "complete_path": str(result.complete_path),
            "final_prefix_root": result.final_prefix_root,
            "manifest_root": result.manifest_root,
            "output_root": str(result.output_root),
            "report_path": str(result.report_path),
            "report_sha256": result.report_sha256,
            "status": result.status,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
