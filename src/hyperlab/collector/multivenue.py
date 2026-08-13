from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from threading import Barrier
from typing import Protocol


class _Collector(Protocol):
    def stop(self) -> None: ...

    def close(self) -> None: ...


class _HyperliquidCollector(_Collector, Protocol):
    def run(self, *, max_messages: int, duration_seconds: float | None) -> object: ...


class _BinanceCollector(_Collector, Protocol):
    def run(self, *, duration_seconds: float | None, max_messages: int | None) -> object: ...


class _Writer(Protocol):
    def close(self) -> None: ...


class MultiVenueCollector:
    """Run both public collectors together and stop all venues on any exit."""

    def __init__(
        self,
        *,
        hyperliquid: _HyperliquidCollector,
        binance: _BinanceCollector,
        writer: _Writer,
    ) -> None:
        self.hyperliquid = hyperliquid
        self.binance = binance
        self.writer = writer
        self._closed = False

    def stop(self) -> None:
        self.hyperliquid.stop()
        self.binance.stop()

    def run(self, *, duration_seconds: float | None) -> None:
        if duration_seconds is not None and duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive when provided")
        start = Barrier(3)

        def synchronized(target: Callable[[], object]) -> object:
            start.wait()
            return target()

        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="hyperlab-venue") as executor:
            futures: tuple[Future[object], Future[object]] = (
                executor.submit(
                    synchronized,
                    lambda: self.hyperliquid.run(
                        max_messages=0,
                        duration_seconds=duration_seconds,
                    ),
                ),
                executor.submit(
                    synchronized,
                    lambda: self.binance.run(
                        duration_seconds=duration_seconds,
                        max_messages=None,
                    ),
                ),
            )
            start.wait()
            try:
                wait(futures, return_when=FIRST_COMPLETED)
            finally:
                # A bounded completion, fatal error, signal, or interrupt must
                # never leave the other venue collecting alone.
                self.stop()
            wait(futures)

        errors: list[BaseException] = []
        for future in futures:
            try:
                future.result()
            except BaseException as exc:
                errors.append(exc)
        if errors:
            primary = errors[0]
            for secondary in errors[1:]:
                primary.add_note(
                    "other venue also failed: "
                    f"{type(secondary).__name__}: {secondary}"
                )
            raise primary

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.stop()
        errors: list[tuple[str, BaseException]] = []
        for label, close in (
            ("Hyperliquid collector close", self.hyperliquid.close),
            ("Binance collector close", self.binance.close),
            ("coordinated lake writer close", self.writer.close),
        ):
            try:
                close()
            except BaseException as exc:
                errors.append((label, exc))
        if errors:
            first_label, primary = errors[0]
            primary.add_note(f"cleanup action: {first_label}")
            for label, secondary in errors[1:]:
                primary.add_note(
                    f"{label} also failed: {type(secondary).__name__}: {secondary}"
                )
            raise primary
