from __future__ import annotations

from decimal import Decimal

import pytest

from hyperlab.ghost.models import (
    BookLevel,
    ClockedObservation,
    ExecutableBook,
    InstrumentGridVersion,
    Side,
    VenueHealth,
)


def _grid() -> InstrumentGridVersion:
    return InstrumentGridVersion(
        grid_id="grid-v1",
        venue="lighter",
        instrument_id="LIGHTER:BTC:perp",
        effective_from_ns=0,
        effective_to_ns=None,
        tick_size=Decimal("0.5"),
        lot_size=Decimal("0.1"),
    )


def test_point_in_time_intervals_refuse_lookahead_and_clock_overlap() -> None:
    observation = ClockedObservation(source_ns=90, receive_ns=100, clock_uncertainty_ns=2)
    observation.assert_known_at(103, decision_clock_uncertainty_ns=1)
    with pytest.raises(ValueError, match="CLOCK_UNCERTAINTY_OVERLAP"):
        observation.assert_known_at(102, decision_clock_uncertainty_ns=1)
    with pytest.raises(ValueError, match="LOOKAHEAD_RECEIVE_AFTER_DECISION"):
        observation.assert_known_at(99, decision_clock_uncertainty_ns=0)


def test_versioned_grid_rejects_off_grid_prices_and_quantities() -> None:
    grid = _grid()
    grid.assert_effective_at(10)
    grid.assert_price(Decimal("100.5"))
    grid.assert_quantity(Decimal("1.2"))
    with pytest.raises(ValueError, match="PRICE_OFF_TICK_GRID"):
        grid.assert_price(Decimal("100.25"))
    with pytest.raises(ValueError, match="QUANTITY_OFF_LOT_GRID"):
        grid.assert_quantity(Decimal("1.25"))


def test_executable_book_has_real_bbo_and_finite_level_by_level_depth() -> None:
    book = ExecutableBook(
        venue="lighter",
        instrument_id="LIGHTER:BTC:perp",
        observation=ClockedObservation(source_ns=90, receive_ns=100, clock_uncertainty_ns=1),
        health=VenueHealth.FRESH,
        grid=_grid(),
        bids=(BookLevel(Decimal("99.5"), Decimal("1.0")),),
        asks=(
            BookLevel(Decimal("100.5"), Decimal("0.5")),
            BookLevel(Decimal("101.0"), Decimal("0.7")),
        ),
    )

    assert book.best_bid.price == Decimal("99.5")
    assert book.best_ask.price == Decimal("100.5")
    assert book.spread == Decimal("1.0")
    consumed = book.consume(side=Side.BUY, quantity=Decimal("2.0"), limit_price=Decimal("101.0"))
    assert consumed.filled_quantity == Decimal("1.2")
    assert consumed.unfilled_quantity == Decimal("0.8")
    assert consumed.notional == Decimal("120.95")
    assert consumed.levels == 2
    assert consumed.midpoint_used is False


def test_book_refuses_crossed_or_unsorted_depth() -> None:
    with pytest.raises(ValueError, match="BOOK_CROSSED"):
        ExecutableBook(
            venue="lighter",
            instrument_id="LIGHTER:BTC:perp",
            observation=ClockedObservation(90, 100, 1),
            health=VenueHealth.FRESH,
            grid=_grid(),
            bids=(BookLevel(Decimal("101"), Decimal("1")),),
            asks=(BookLevel(Decimal("100.5"), Decimal("1")),),
        )
