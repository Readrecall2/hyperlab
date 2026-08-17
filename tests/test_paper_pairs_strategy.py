from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import MappingProxyType

from hyperlab.paper.models import (
    DecisionAction,
    MarketEvent,
    OrderSide,
    PaperOrderType,
    PaperState,
    TimeInForce,
)
from hyperlab.paper.pairs_strategy import (
    FrozenRobustPairsPaperConfig,
    FrozenRobustPairsPaperStrategy,
)
from hyperlab.paper.runner import PaperStrategyView

_START = datetime(2026, 8, 17, 0, 0, tzinfo=UTC)
_RUN_ID = "1" * 64
_CONFIG_HASH = "2" * 64
_ASSET_A = "HL:ETH:perp"
_ASSET_B = "HL:BTC:perp"


def _config() -> FrozenRobustPairsPaperConfig:
    return FrozenRobustPairsPaperConfig(
        model_method="cointegration",
        lookback_bars=12,
        bar_seconds=30,
        enter_z=2.0,
        exit_z=0.6,
        stop_z=20.0,
        max_holding_bars=12,
        cooldown_bars=2,
        volatility_lookback_bars=12,
        target_spread_volatility=0.02,
        maximum_pair_gross=0.50,
        maximum_gross_notional=Decimal("100"),
        asset_a_quantity_step=Decimal("0.0001"),
        asset_b_quantity_step=Decimal("0.0001"),
        asset_a_max_quantity=Decimal("10"),
        asset_b_max_quantity=Decimal("10"),
        retained_bars=64,
    )


def _view(
    state: PaperState,
    positions: dict[str, Decimal] | None = None,
) -> PaperStrategyView:
    return PaperStrategyView(
        run_id=_RUN_ID,
        config_hash=_CONFIG_HASH,
        state=state,
        positions=MappingProxyType(positions or {}),
        marks=MappingProxyType({}),
        completed_cycles=0,
    )


def _frame(
    index: int,
    spread: float,
    *,
    sequence_base: int | None = None,
    stale_asset: str | None = None,
    gap_asset: str | None = None,
    execution_shift: float = 0.0,
) -> dict[str, MarketEvent]:
    """Build a visibly synthetic bilateral BBO frame for deterministic tests."""

    received_at = _START + timedelta(seconds=index * 30 + 10)
    btc_mid = Decimal(str(100.0 + 0.15 * index + execution_shift))
    eth_mid = Decimal(str(float(btc_mid) * math.exp(spread)))
    sequence = sequence_base if sequence_base is not None else 10_000 + index * 10

    def event(instrument: str, mid: Decimal, ordinal: int) -> MarketEvent:
        half_spread = Decimal("0.01")
        return MarketEvent.create(
            received_at=received_at,
            instrument=instrument,
            bid_price=mid - half_spread,
            ask_price=mid + half_spread,
            bid_depth=Decimal("100"),
            ask_depth=Decimal("100"),
            source_sequence=sequence + ordinal,
            stale=instrument == stale_asset,
            gap=instrument == gap_asset,
            tradable=instrument not in {stale_asset, gap_asset},
        )

    return {
        _ASSET_A: event(_ASSET_A, eth_mid, 1),
        _ASSET_B: event(_ASSET_B, btc_mid, 2),
    }


def _baseline_spread(index: int) -> float:
    # Synthetic stationary history; no result from this fixture is economic evidence.
    return 0.006 * math.sin(index * 0.71)


def _warm_to_open_shock(
    strategy: FrozenRobustPairsPaperStrategy,
) -> tuple[list[MarketEvent], int]:
    durable: list[MarketEvent] = []
    shock_index = 38
    for index in range(shock_index):
        frame = _frame(index, _baseline_spread(index))
        durable.extend(frame.values())
        assert strategy.decide(frame, _view(PaperState.PAUSED)) is None
    shock = _frame(shock_index, 0.055)
    durable.extend(shock.values())
    assert strategy.decide(shock, _view(PaperState.PAUSED)) is None
    return durable, shock_index


def test_signal_uses_only_completed_bar_and_intent_is_deterministic() -> None:
    first = FrozenRobustPairsPaperStrategy(_config())
    second = FrozenRobustPairsPaperStrategy(_config())
    durable, shock_index = _warm_to_open_shock(first)
    second.restore(durable)

    crossing = _frame(shock_index + 1, _baseline_spread(shock_index + 1))
    first_intent = first.decide(crossing, _view(PaperState.FLAT))
    second_intent = second.decide(crossing, _view(PaperState.FLAT))

    assert first_intent is not None
    assert second_intent is not None
    assert first_intent.to_dict() == second_intent.to_dict()
    assert first_intent.action is DecisionAction.ENTRY
    assert len(first_intent.orders) == 2
    assert {order.order_type for order in first_intent.orders} == {PaperOrderType.TAKER}
    assert {order.time_in_force for order in first_intent.orders} == {TimeInForce.IOC}
    assert all(not order.reduce_only for order in first_intent.orders)
    assert len(first_intent.observed_event_ids) > 40
    entry_notional = sum(
        order.quantity
        * (
            crossing[order.instrument].ask_price
            if order.side is OrderSide.BUY
            else crossing[order.instrument].bid_price
        )
        for order in first_intent.orders
    )
    assert entry_notional <= _config().maximum_gross_notional
    assert all(order.quantity <= Decimal("10") for order in first_intent.orders)

    assert first.diagnostic_snapshot["bar_ended_at"] == (
        _START + timedelta(seconds=(shock_index + 1) * 30)
    ).isoformat(timespec="microseconds").replace("+00:00", "Z")
    assert first.strategy_hash == second.strategy_hash

def test_intent_identity_is_independent_of_mapping_insertion_order() -> None:
    first = FrozenRobustPairsPaperStrategy(_config())
    second = FrozenRobustPairsPaperStrategy(_config())
    durable, shock_index = _warm_to_open_shock(first)
    second.restore(durable)
    crossing = _frame(
        shock_index + 1,
        _baseline_spread(shock_index + 1),
        sequence_base=32_000,
    )

    canonical = first.decide(crossing, _view(PaperState.FLAT))
    reversed_mapping = second.decide(
        {_ASSET_B: crossing[_ASSET_B], _ASSET_A: crossing[_ASSET_A]},
        _view(PaperState.FLAT),
    )

    assert canonical is not None and reversed_mapping is not None
    assert canonical.to_dict() == reversed_mapping.to_dict()


def test_signal_waits_for_complete_post_bar_pair_execution_snapshot() -> None:
    strategy = FrozenRobustPairsPaperStrategy(_config())
    _, shock_index = _warm_to_open_shock(strategy)
    prior = _frame(shock_index, 0.055)
    crossing = _frame(
        shock_index + 1,
        _baseline_spread(shock_index + 1),
        sequence_base=35_000,
    )

    incomplete = {_ASSET_A: crossing[_ASSET_A], _ASSET_B: prior[_ASSET_B]}
    assert strategy.decide(incomplete, _view(PaperState.FLAT)) is None

    intent = strategy.decide(crossing, _view(PaperState.FLAT))

    assert intent is not None
    assert intent.action is DecisionAction.ENTRY
    assert {order.instrument for order in intent.orders} == {_ASSET_A, _ASSET_B}
    assert strategy.diagnostic_snapshot["status"] == "SIGNAL_EXECUTION_READY"


def test_future_open_bucket_prices_cannot_rewrite_completed_bar_signal() -> None:
    base = FrozenRobustPairsPaperStrategy(_config())
    changed_future = FrozenRobustPairsPaperStrategy(_config())
    _, shock_index = _warm_to_open_shock(base)
    _warm_to_open_shock(changed_future)

    normal = _frame(
        shock_index + 1,
        _baseline_spread(shock_index + 1),
        sequence_base=40_000,
    )
    rewritten = _frame(
        shock_index + 1,
        -0.20,
        sequence_base=50_000,
        execution_shift=25.0,
    )
    assert base.decide(normal, _view(PaperState.FLAT)) is not None
    assert changed_future.decide(rewritten, _view(PaperState.FLAT)) is not None

    base_diagnostic = base.diagnostic_snapshot
    changed_diagnostic = changed_future.diagnostic_snapshot
    assert base_diagnostic["zscore"] == changed_diagnostic["zscore"]
    assert base_diagnostic["spread"] == changed_diagnostic["spread"]
    assert base_diagnostic["target_weights"] == changed_diagnostic["target_weights"]

def test_repeated_one_leg_updates_accept_unchanged_older_peer_snapshot() -> None:
    strategy = FrozenRobustPairsPaperStrategy(_config())
    initial = _frame(0, _baseline_spread(0))
    assert strategy.decide(initial, _view(PaperState.PAUSED)) is None

    previous_peer = initial[_ASSET_B]
    for ordinal, offset in enumerate((1, 1), start=1):
        prior = initial[_ASSET_A]
        updated = MarketEvent.create(
            received_at=prior.received_at + timedelta(seconds=offset),
            instrument=_ASSET_A,
            bid_price=prior.bid_price + Decimal(ordinal) / Decimal("1000"),
            ask_price=prior.ask_price + Decimal(ordinal) / Decimal("1000"),
            bid_depth=prior.bid_depth,
            ask_depth=prior.ask_depth,
            source_sequence=20_000 + ordinal,
        )

        assert (
            strategy.decide(
                {_ASSET_A: updated, _ASSET_B: previous_peer},
                _view(PaperState.PAUSED),
            )
            is None
        )
        initial[_ASSET_A] = updated

    next_bucket = _frame(1, _baseline_spread(1), sequence_base=21_000)
    assert strategy.decide(next_bucket, _view(PaperState.PAUSED)) is None
    assert strategy.diagnostic_snapshot["bars_retained"] == 1
    assert strategy.diagnostic_snapshot["status"] == "SIGNAL_EXECUTION_READY"


def test_stale_gap_and_skewed_execution_frames_fail_closed() -> None:
    for unsafe in (
        {"stale_asset": _ASSET_A},
        {"gap_asset": _ASSET_B},
    ):
        strategy = FrozenRobustPairsPaperStrategy(_config())
        _, shock_index = _warm_to_open_shock(strategy)
        crossing = _frame(shock_index + 1, 0.0, sequence_base=60_000, **unsafe)
        assert strategy.decide(crossing, _view(PaperState.FLAT)) is None
        assert strategy.diagnostic_snapshot["status"] == "UNSAFE_EXECUTION_FRAME"

    strategy = FrozenRobustPairsPaperStrategy(_config())
    _, shock_index = _warm_to_open_shock(strategy)
    crossing = _frame(shock_index + 1, 0.0, sequence_base=70_000)
    later_btc = MarketEvent.create(
        received_at=crossing[_ASSET_B].received_at + timedelta(seconds=3),
        instrument=_ASSET_B,
        bid_price=crossing[_ASSET_B].bid_price,
        ask_price=crossing[_ASSET_B].ask_price,
        bid_depth=Decimal("100"),
        ask_depth=Decimal("100"),
        source_sequence=70_099,
    )
    crossing[_ASSET_B] = later_btc
    assert strategy.decide(crossing, _view(PaperState.FLAT)) is None
    assert strategy.diagnostic_snapshot["status"] == "EXECUTION_FRAME_SKEW"


def test_multi_leg_entry_then_mean_reversion_exit_is_reduce_only() -> None:
    strategy = FrozenRobustPairsPaperStrategy(_config())
    _, shock_index = _warm_to_open_shock(strategy)
    reversion = _frame(shock_index + 1, 0.0, sequence_base=80_000)
    entry = strategy.decide(reversion, _view(PaperState.FLAT))
    assert entry is not None
    assert entry.action is DecisionAction.ENTRY
    positions = {order.instrument: order.quantity * order.side.sign for order in entry.orders}
    assert set(positions) == {_ASSET_A, _ASSET_B}
    assert {order.side for order in entry.orders} == {OrderSide.BUY, OrderSide.SELL}

    next_frame = _frame(shock_index + 2, 0.0, sequence_base=81_000)
    exit_intent = strategy.decide(next_frame, _view(PaperState.HEDGED, positions))
    assert exit_intent is not None
    assert exit_intent.action is DecisionAction.EXIT
    assert len(exit_intent.orders) == 2
    assert all(order.reduce_only for order in exit_intent.orders)
    for order in exit_intent.orders:
        assert order.quantity == abs(positions[order.instrument])
        expected_sign = Decimal(-1) if positions[order.instrument] > 0 else Decimal(1)
        assert order.side.sign == expected_sign


def test_restart_reconstruction_is_streamable_exact_and_memory_bounded() -> None:
    uninterrupted = FrozenRobustPairsPaperStrategy(_config())
    durable, shock_index = _warm_to_open_shock(uninterrupted)

    restarted = FrozenRobustPairsPaperStrategy(_config())
    restarted.restore(iter(durable), _view(PaperState.FLAT))
    crossing = _frame(shock_index + 1, 0.0, sequence_base=90_000)
    expected = uninterrupted.decide(crossing, _view(PaperState.FLAT))
    actual = restarted.decide(crossing, _view(PaperState.FLAT))

    assert expected is not None and actual is not None
    assert expected.to_dict() == actual.to_dict()
    assert uninterrupted.diagnostic_snapshot == restarted.diagnostic_snapshot
    for index in range(shock_index + 2, shock_index + 80):
        restarted.decide(_frame(index, _baseline_spread(index)), _view(PaperState.PAUSED))
    assert restarted.diagnostic_snapshot["bars_retained"] == _config().retained_bars

    assert int(restarted.diagnostic_snapshot["bars_retained"]) <= _config().retained_bars
