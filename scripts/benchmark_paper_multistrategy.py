from __future__ import annotations

import argparse
import gc
import json
import statistics
import tempfile
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.models import (
    DecisionIntent,
    MarketEvent,
    PaperExecutionConfig,
    PaperRiskLimits,
    PaperRunConfig,
    PaperStrategyConfig,
    deterministic_id,
)
from hyperlab.paper.runner import PaperStrategyView, PortfolioRunner
from hyperlab.paper.store import PaperStore

_START = datetime(2026, 8, 18, tzinfo=UTC)
_INSTRUMENT = "HYPERLIQUID:BTC:perp"
_DATA_HASH = deterministic_id("paper_multistrategy_benchmark_data_v1")


class _HoldStrategy:
    def __init__(self, config: PaperStrategyConfig) -> None:
        self.strategy_id = config.strategy_id
        self.strategy_name = config.strategy_name
        self.strategy_hash = config.strategy_hash
        self.strategy_config_hash = config.strategy_config_hash
        self.observations = 0

    def decide(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None:
        if view.strategy_id != self.strategy_id or tuple(markets) != (_INSTRUMENT,):
            raise AssertionError("benchmark strategy received a divergent shared frame")
        self.observations += 1
        return None


def _strategy(index: int) -> PaperStrategyConfig:
    strategy_id = f"hold_{index:02d}"
    return PaperStrategyConfig(
        strategy_id=strategy_id,
        strategy_name="synthetic_hold",
        strategy_hash=deterministic_id("paper_multistrategy_benchmark", strategy_id),
        parameters={"fixture": "SYNTHETIC_BENCHMARK_V1"},
        risk=PaperRiskLimits(),
        required_instruments=(_INSTRUMENT,),
    )


def _config(strategies: tuple[PaperStrategyConfig, ...]) -> PaperRunConfig:
    primary = strategies[0]
    return PaperRunConfig(
        strategy_name=primary.strategy_name,
        strategy_hash=primary.strategy_hash,
        parameters=primary.parameters,
        data_hash=_DATA_HASH,
        execution=PaperExecutionConfig(
            calibration_status="SYNTHETIC",
            source="deterministic-multistrategy-benchmark",
        ),
        risk=PaperRiskLimits(),
        seed=18,
        initial_cash=Decimal("100000"),
        validation_started_at=_START,
        run_kind="DEMO",
        data_calibration_status="SYNTHETIC",
        data_source="deterministic-multistrategy-benchmark",
        required_instruments=(_INSTRUMENT,),
        schema_version=3,
        strategies=strategies,
    )


def _markets(frames: int) -> tuple[dict[str, MarketEvent], ...]:
    return tuple(
        {
            _INSTRUMENT: MarketEvent.create(
                received_at=_START + timedelta(milliseconds=index),
                instrument=_INSTRUMENT,
                bid_price=Decimal("100"),
                ask_price=Decimal("101"),
                bid_depth=Decimal("1000"),
                ask_depth=Decimal("1000"),
                source_sequence=index + 1,
            )
        }
        for index in range(frames)
    )


def _measure(root: Path, *, strategy_count: int, frames: tuple[dict[str, MarketEvent], ...]) -> float:
    configs = tuple(_strategy(index) for index in range(strategy_count))
    adapters = tuple(_HoldStrategy(config) for config in reversed(configs))
    engine = PaperEngine(PaperStore(root / "paper.sqlite3"), _config(configs))
    engine.start()
    runner = PortfolioRunner(engine, adapters)
    started = time.perf_counter()
    for frame in frames:
        received_at = frame[_INSTRUMENT].received_at
        runner.process_frame(frame, processed_at=received_at)
    elapsed = time.perf_counter() - started
    if any(adapter.observations != len(frames) for adapter in adapters):
        raise AssertionError("a strategy did not consume every shared observation")
    if len(tuple(engine.store.iter_inputs(engine.run_id, input_type="PUBLIC_MARKET_EVENT"))) != len(frames):
        raise AssertionError("shared market observations were duplicated durably")
    if engine.replay().to_dict() != engine.projection().to_dict():
        raise AssertionError("benchmark replay was not exact")
    engine.store.close()
    del runner, engine
    gc.collect()
    return elapsed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Measure deterministic durable Paper frames with 1, 2, and 4 hold strategies."
    )
    parser.add_argument("--frames", type=int, default=200)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    if args.frames < 1 or args.repetitions < 1:
        parser.error("--frames and --repetitions must be positive")
    frames = _markets(args.frames)
    samples: dict[int, list[float]] = {1: [], 2: [], 4: []}
    with tempfile.TemporaryDirectory(prefix="hyperlab-multistrategy-benchmark-") as temporary:
        benchmark_root = Path(temporary)
        for strategy_count in samples:
            for repetition in range(args.repetitions):
                sample_root = benchmark_root / f"s{strategy_count}-r{repetition}"
                sample_root.mkdir()
                samples[strategy_count].append(
                    _measure(sample_root, strategy_count=strategy_count, frames=frames)
                )
    medians = {count: statistics.median(values) for count, values in samples.items()}
    baseline = medians[1]
    payload = {
        "contract": "SHARED_CANONICAL_MARKET_SEQUENTIAL_STRATEGY_ID_ORDER_V1",
        "frames_per_repetition": args.frames,
        "repetitions": args.repetitions,
        "results": {
            str(count): {
                "frames_per_second_median": args.frames / median,
                "relative_elapsed_to_one_strategy": median / baseline,
                "seconds_median": median,
                "seconds_samples": samples[count],
            }
            for count, median in medians.items()
        },
        "synthetic_warning": "SYNTHETIC TECHNICAL THROUGHPUT ONLY; NOT ECONOMIC EVIDENCE",
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
