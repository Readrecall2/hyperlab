from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from pathlib import Path

from hyperlab.analysis.streaming_stress import run_production_component_stress


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the synthetic-only Phase 10-2 production-component "
            "streaming benchmark."
        )
    )
    parser.add_argument("--manifests", type=int, default=60_001)
    parser.add_argument("--source-rows", type=int, default=2_000_000)
    parser.add_argument("--minimum-output-events", type=int, default=2_000_000)
    parser.add_argument("--writer-buffer-rows", type=int, default=16_384)
    parser.add_argument("--quantile-run-rows", type=int, default=250_000)
    parser.add_argument("--scratch-parent", type=Path)
    args = parser.parse_args()

    scratch = Path(
        tempfile.mkdtemp(
            prefix="hyperlab-phase10-streaming-stress-",
            dir=args.scratch_parent,
        )
    )
    target = scratch / "scratch"
    try:
        result = run_production_component_stress(
            target,
            manifest_count=args.manifests,
            source_rows=args.source_rows,
            minimum_output_events=args.minimum_output_events,
            writer_buffer_rows=args.writer_buffer_rows,
            quantile_run_rows=args.quantile_run_rows,
        )
        print(json.dumps(result.as_dict(), sort_keys=True, separators=(",", ":")))
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
