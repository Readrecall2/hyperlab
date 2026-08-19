from __future__ import annotations

import argparse
import gc
import json
import platform
import statistics
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from hyperlab.collector.models import ParsedRecord
from hyperlab.data.schema import RecordType, latest_schema_for
from hyperlab.paper.collector_source import (
    PHASE12_PHASE05_PRODUCT_IDENTITY_HASHES,
    PHASE12_PHASE05_PUBLIC_INSTRUMENTS,
    HyperliquidPaperPublicSource,
)
from hyperlab.paper.engine import PaperEngine
from hyperlab.paper.models import DecisionIntent, MarketEvent, PaperRunConfig, PaperStrategyConfig
from hyperlab.paper.pairs_strategy import (
    FrozenRobustPairsPaperConfig,
    FrozenRobustPairsPaperStrategy,
    make_phase08_paper_strategy_config,
)
from hyperlab.paper.phase05_portfolio import (
    build_phase05_phase08_paper_foundation,
    default_phase05_phase08_risk_allocation,
)
from hyperlab.paper.public_source import (
    BoundedPublicRecordSource,
    PublicRecordMarketEventAdapter,
)
from hyperlab.paper.runner import FrozenPaperStrategy, PaperStrategyView, PortfolioRunner
from hyperlab.paper.runtime import PublicSourceDescriptor
from hyperlab.paper.store import PaperStore

_START = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)
_SYNTHETIC_WARNING = "SYNTHETIC TECHNICAL THROUGHPUT ONLY; NOT ECONOMIC OR DEPLOYMENT EVIDENCE"


@dataclass(frozen=True, slots=True)
class _Case:
    config: PaperRunConfig
    strategies: tuple[FrozenPaperStrategy, ...]


class _CountingAdapter:
    def __init__(
        self,
        delegate: FrozenPaperStrategy,
        config: PaperStrategyConfig,
    ) -> None:
        self._delegate = delegate
        self.strategy_id = config.strategy_id
        self.strategy_name = config.strategy_name
        self.strategy_hash = config.strategy_hash
        self.strategy_config_hash = config.strategy_config_hash
        self.evaluations = 0

    def decide(
        self,
        markets: Mapping[str, MarketEvent],
        view: PaperStrategyView,
    ) -> DecisionIntent | None:
        self.evaluations += 1
        return self._delegate.decide(markets, view)


def _cases(root: Path) -> Mapping[str, _Case]:
    combined = build_phase05_phase08_paper_foundation(
        runtime_status_path=root / "combined-source-status.json",
        validation_started_at=_START,
    )
    combined.source.close()

    allocation = default_phase05_phase08_risk_allocation()
    pairs_config = FrozenRobustPairsPaperConfig()
    pairs_identity = make_phase08_paper_strategy_config(
        config=pairs_config,
        risk=allocation.phase08,
    )
    pairs_source = HyperliquidPaperPublicSource.create_mainnet(
        runtime_status_path=root / "phase08-source-status.json"
    )
    phase08_config = replace(
        combined.config,
        strategy_name=pairs_identity.strategy_name,
        strategy_hash=pairs_identity.strategy_hash,
        parameters=pairs_identity.parameters,
        data_hash=pairs_source.descriptor.data_hash,
        data_source=pairs_source.descriptor.source,
        risk=allocation.phase08,
        required_instruments=pairs_identity.required_instruments,
        strategies=(pairs_identity,),
    )
    pairs_source.close()
    return {
        "phase08_only": _Case(
            config=phase08_config,
            strategies=(
                FrozenRobustPairsPaperStrategy(
                    pairs_config,
                    strategy_config=pairs_identity,
                ),
            ),
        ),
        "phase08_plus_phase05": _Case(
            config=combined.config,
            strategies=combined.strategies,
        ),
    }


def _context(instrument: str, received_at: datetime) -> Mapping[str, object]:
    if not instrument.startswith("HL:HYPE:"):
        return {}
    is_spot = instrument.endswith(":spot")
    return {
        "instrument_kind": "spot" if is_spot else "perp",
        "notional_volume_24h": "100000000",
        "observation_id": f"synthetic-{instrument}-{received_at.isoformat()}",
        "open_interest_notional": None if is_spot else "1000000000",
        "product_identity_sha256": PHASE12_PHASE05_PRODUCT_IDENTITY_HASHES[instrument],
        "received_at": received_at,
        "source_asset": "@107" if is_spot else "HYPE",
    }


def _mid(instrument: str) -> Decimal:
    return {
        "HL:BTC:perp": Decimal("100000"),
        "HL:ETH:perp": Decimal("2000"),
        "HL:HYPE:perp": Decimal("10.05"),
        "HL:HYPE:spot": Decimal("10"),
    }[instrument]


def _frames(config: PaperRunConfig, count: int) -> tuple[dict[str, MarketEvent], ...]:
    result: list[dict[str, MarketEvent]] = []
    for index in range(count):
        received_at = _START + timedelta(milliseconds=index)
        frame: dict[str, MarketEvent] = {}
        for ordinal, instrument in enumerate(config.required_instruments, start=1):
            mid = _mid(instrument)
            frame[instrument] = MarketEvent.create(
                received_at=received_at,
                instrument=instrument,
                bid_price=mid - Decimal("0.005"),
                ask_price=mid + Decimal("0.005"),
                bid_depth=Decimal("1000000"),
                ask_depth=Decimal("1000000"),
                capture_ordinal=ordinal,
                source_sequence=index + 1,
                context=_context(instrument, received_at),
            )
        result.append(frame)
    return tuple(result)


def _measure(root: Path, case: _Case, frames: int) -> Mapping[str, object]:
    adapters = tuple(
        _CountingAdapter(strategy, config)
        for config, strategy in zip(
            case.config.strategy_configs,
            case.strategies,
            strict=True,
        )
    )
    market_frames = _frames(case.config, frames)
    store = PaperStore(root / "paper.sqlite3")
    engine = PaperEngine(store, case.config)
    engine.start()
    runner = PortfolioRunner(engine, adapters)
    started = time.perf_counter()
    for frame in market_frames:
        runner.process_frame(frame, processed_at=max(event.received_at for event in frame.values()))
    elapsed = time.perf_counter() - started
    durable_commits = len(tuple(store.iter_inputs(engine.run_id, input_type="PUBLIC_MARKET_EVENT")))
    evaluations = sum(adapter.evaluations for adapter in adapters)
    expected_commits = frames * len(case.config.required_instruments)
    expected_evaluations = frames * len(adapters)
    if durable_commits != expected_commits:
        raise AssertionError("durable market commit count differs from canonical input")
    if evaluations != expected_evaluations:
        raise AssertionError("real strategy evaluation count differs from scheduler contract")
    if engine.replay().to_dict() != engine.projection().to_dict():
        raise AssertionError("benchmark replay was not exact")
    store.verify_integrity(engine.run_id)
    store.close()
    del runner, engine, adapters
    gc.collect()
    return {
        "durable_commits": durable_commits,
        "elapsed_seconds": elapsed,
        "strategy_evaluations": evaluations,
    }


def _common_row(record_type: RecordType, received_at: datetime, asset: str) -> dict[str, object]:
    schema = latest_schema_for(record_type)
    row = {name: None for name in schema.schema.names}
    row.update(
        {
            "asset": asset,
            "connection_id": "synthetic-queue-probe",
            "event_time": received_at,
            "exchange_time": received_at,
            "received_time": received_at,
            "record_type": record_type.value,
            "schema_version": schema.version,
            "source_sequence": None,
            "venue": "hyperliquid",
        }
    )
    return row


def _queue_probe(bursts: int) -> Mapping[str, object]:
    capacity = 32
    adapter = PublicRecordMarketEventAdapter(
        instruments=PHASE12_PHASE05_PUBLIC_INSTRUMENTS,
        queue_capacity=capacity,
        identity_context={"fixture": "SYNTHETIC_PHASE05_QUEUE_PROBE_V1"},
        include_market_context=True,
        product_identity_hashes=PHASE12_PHASE05_PRODUCT_IDENTITY_HASHES,
    )
    source = BoundedPublicRecordSource(
        descriptor=PublicSourceDescriptor(
            source="synthetic-phase05-queue-probe",
            data_hash=adapter.identity_hash,
        ),
        adapter=adapter,
        capacity=capacity,
    )
    record_count = 0
    for burst in range(bursts):
        received_at = _START + timedelta(milliseconds=burst)
        for _venue, asset in sorted(PHASE12_PHASE05_PUBLIC_INSTRUMENTS):
            row = _common_row(RecordType.BBO, received_at, asset)
            row.update(
                {
                    "ask_price": Decimal("101"),
                    "ask_quantity": Decimal("1000"),
                    "bid_price": Decimal("100"),
                    "bid_quantity": Decimal("1000"),
                    "update_id": f"synthetic-{asset}-{burst}",
                }
            )
            source.feed(ParsedRecord(record_type=RecordType.BBO, asset=asset, row=row))
            record_count += 1
    before_drain = source.queue_snapshot(as_of=_START + timedelta(milliseconds=bursts))
    drained = 0
    while source.poll(timeout_seconds=0) is not None:
        drained += 1
    after_drain = source.queue_snapshot(as_of=_START + timedelta(milliseconds=bursts))
    source.close()
    expected_pending = len(PHASE12_PHASE05_PUBLIC_INSTRUMENTS)
    if before_drain["pending_frames"] != expected_pending:
        raise AssertionError("same-minute BBOs did not remain bounded by instrument")
    if after_drain["pending_frames"] != 0 or drained != expected_pending:
        raise AssertionError("queue probe did not drain completely")
    return {
        "after_drain": after_drain,
        "before_drain": before_drain,
        "input_bbo_records": record_count,
        "persistent_backlog": False,
        "production_capacity_frames": 4096,
        "probe_capacity_frames": capacity,
    }


def _summarize(samples: list[Mapping[str, object]], frames: int) -> Mapping[str, object]:
    elapsed_samples = [float(sample["elapsed_seconds"]) for sample in samples]
    elapsed = statistics.median(elapsed_samples)
    evaluations = int(samples[0]["strategy_evaluations"])
    commits = int(samples[0]["durable_commits"])
    return {
        "durable_commits_per_repetition": commits,
        "durable_commits_per_second_median": commits / elapsed,
        "elapsed_seconds_median": elapsed,
        "elapsed_seconds_samples": elapsed_samples,
        "frames_per_second_median": frames / elapsed,
        "strategy_evaluations_per_repetition": evaluations,
        "strategy_evaluations_per_second_median": evaluations / elapsed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark real Phase 08 versus real Phase 08 + Phase 05 Paper adapters."
    )
    parser.add_argument("--frames", type=int, default=200)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.frames < 1 or args.repetitions < 1:
        parser.error("--frames and --repetitions must be positive")

    samples: dict[str, list[Mapping[str, object]]] = {
        "phase08_only": [],
        "phase08_plus_phase05": [],
    }
    identities: dict[str, Mapping[str, object]] = {}
    with tempfile.TemporaryDirectory(prefix="hyperlab-phase05-paper-benchmark-") as temporary:
        benchmark_root = Path(temporary)
        cases = _cases(benchmark_root)
        for name, case in cases.items():
            identities[name] = {
                "config_hash": case.config.config_hash,
                "portfolio_id": case.config.portfolio_id,
                "required_instruments": list(case.config.required_instruments),
                "run_id": case.config.run_id,
                "strategy_ids": [item.strategy_id for item in case.config.strategy_configs],
            }
            for repetition in range(args.repetitions):
                sample_root = benchmark_root / f"{name}-{repetition}"
                sample_root.mkdir()
                fresh_case = _cases(sample_root)[name]
                samples[name].append(_measure(sample_root, fresh_case, args.frames))
        queue = _queue_probe(max(args.frames, 2))

    results = {name: _summarize(case_samples, args.frames) for name, case_samples in samples.items()}
    baseline = float(results["phase08_only"]["elapsed_seconds_median"])
    combined = float(results["phase08_plus_phase05"]["elapsed_seconds_median"])
    payload = {
        "benchmark_contract": "REAL_PHASE08_VS_REAL_PHASE08_PLUS_PHASE05_DURABLE_V1",
        "comparison": {
            "combined_elapsed_relative_to_phase08_only": combined / baseline,
            "combined_frames_per_second_relative_to_phase08_only": baseline / combined,
        },
        "frames_per_repetition": args.frames,
        "identities": identities,
        "input": "DETERMINISTIC_SYNTHETIC_CANONICAL_BBO",
        "platform": platform.platform(),
        "queue_behavior": queue,
        "repetitions": args.repetitions,
        "results": results,
        "synthetic_warning": _SYNTHETIC_WARNING,
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(rendered.encode("utf-8"))
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
