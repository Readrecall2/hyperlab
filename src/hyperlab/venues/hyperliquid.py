from __future__ import annotations

from collections.abc import Mapping

from hyperlab.collector.models import ParsedMessage, WireEnvelope
from hyperlab.collector.parser import parse_websocket_message
from hyperlab.venues.base import NormalizedInstrument


class HyperliquidPublicConnector:
    """Adapter exposing the Phase 02 parser through the common venue boundary."""

    venue = "hyperliquid"

    def __init__(self, instruments: Mapping[str, NormalizedInstrument]) -> None:
        self._instruments = {asset.upper(): item for asset, item in instruments.items()}
        if not self._instruments:
            raise ValueError("Hyperliquid connector requires reviewed instrument metadata")
        if any(item.venue != self.venue for item in self._instruments.values()):
            raise ValueError("Hyperliquid connector received another venue's instrument")

    def websocket_url(self, assets: tuple[str, ...], candle_intervals: tuple[str, ...]) -> str:
        for asset in assets:
            self.instrument_for_asset(asset)
        if not candle_intervals:
            raise ValueError("at least one candle interval is required")
        return "wss://api.hyperliquid.xyz/ws"

    def parse_message(self, envelope: WireEnvelope) -> ParsedMessage:
        return parse_websocket_message(envelope)

    def instrument_for_asset(self, asset: str) -> NormalizedInstrument:
        try:
            return self._instruments[asset.upper()]
        except KeyError:
            raise ValueError(f"asset is not configured for Hyperliquid: {asset}") from None
