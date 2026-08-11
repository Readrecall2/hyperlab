from __future__ import annotations

from typing import Protocol

from hyperlab.models import MarketPanel, StrategyOutput


class Strategy(Protocol):
    name: str
    risk_tier: str

    def generate(self, panel: MarketPanel) -> StrategyOutput: ...
