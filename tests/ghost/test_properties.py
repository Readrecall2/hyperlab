from __future__ import annotations

from decimal import Decimal

from hyperlab.ghost.models import (
    BookLevel,
    ClockedObservation,
    ExecutableBook,
    InstrumentGridVersion,
    Side,
    VenueHealth,
)


def test_finite_depth_and_notional_conservation_property_grid() -> None:
    grid = InstrumentGridVersion(
        "g", "kalshi", "K:A", 0, None, Decimal("0.01"), Decimal("1")
    )
    for depth in range(1, 20):
        asks = tuple(
            BookLevel(Decimal("0.50") + Decimal(index) / 100, Decimal(index + 1))
            for index in range(depth)
        )
        book = ExecutableBook(
            "kalshi",
            "K:A",
            ClockedObservation(1, 2, 0),
            VenueHealth.FRESH,
            grid,
            (BookLevel(Decimal("0.49"), Decimal("100")),),
            asks,
        )
        requested = Decimal(depth * (depth + 1) // 2 + 7)
        fill = book.consume(Side.BUY, requested, asks[-1].price)
        assert fill.filled_quantity == sum(
            (level.quantity for level in asks), Decimal("0")
        )
        assert fill.filled_quantity + fill.unfilled_quantity == requested
        assert fill.notional == sum(
            (level.price * level.quantity for level in asks), Decimal("0")
        )
        assert fill.midpoint_used is False
