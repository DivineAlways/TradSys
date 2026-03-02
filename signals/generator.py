"""
Signal Generator: consumes market data, runs strategies, emits Signals.

Included example strategy: EMA Crossover Momentum
  - Fast EMA crosses above Slow EMA → LONG signal
  - Fast EMA crosses below Slow EMA → SHORT / FLAT signal

To add your own strategy:
  1. Subclass BaseStrategy and implement `on_bar(bar) -> Optional[Signal]`
  2. Register it in SignalGenerator.__init__
"""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections import deque
from typing import Optional

import pandas as pd

from core.events import Event, EventBus, EventType
from core.models import Bar, Signal
from core.enums import SignalDirection
from market_data.feed_manager import FeedManager

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Strategy Interface
# ─────────────────────────────────────────────────────────────────────────────

class BaseStrategy(ABC):
    """Base class all strategies must implement."""

    @property
    @abstractmethod
    def strategy_id(self) -> str:
        ...

    @property
    @abstractmethod
    def symbols(self) -> list[str]:
        ...

    @abstractmethod
    def on_bar(self, bar: Bar) -> Optional[Signal]:
        """
        Called on each new bar for a subscribed symbol.
        Return a Signal to trigger execution, or None to do nothing.
        """
        ...


# ─────────────────────────────────────────────────────────────────────────────
# Example Strategy: EMA Crossover
# ─────────────────────────────────────────────────────────────────────────────

class MomentumStrategy(BaseStrategy):
    """
    EMA Crossover strategy.

    Long when fast_ema > slow_ema (bullish momentum).
    Flat when fast_ema < slow_ema.

    Parameters:
        symbols:    List of symbols to trade
        fast_period: Fast EMA period (default 9)
        slow_period: Slow EMA period (default 21)
        min_bars:    Minimum bars before generating signals
    """

    def __init__(
        self,
        symbols: list[str],
        fast_period: int = 9,
        slow_period: int = 21,
        min_bars: int = 30,
    ) -> None:
        self._symbols = symbols
        self._fast = fast_period
        self._slow = slow_period
        self._min_bars = max(min_bars, slow_period + 5)
        # Ring buffer of closes per symbol
        self._closes: dict[str, deque[float]] = {
            s: deque(maxlen=self._min_bars * 2) for s in symbols
        }
        # Last known position direction to avoid duplicate signals
        self._last_direction: dict[str, SignalDirection] = {}

    @property
    def strategy_id(self) -> str:
        return "ema_crossover"

    @property
    def symbols(self) -> list[str]:
        return self._symbols

    def on_bar(self, bar: Bar) -> Optional[Signal]:
        if bar.symbol not in self._closes:
            return None

        self._closes[bar.symbol].append(bar.close)
        closes = list(self._closes[bar.symbol])

        if len(closes) < self._min_bars:
            return None  # Not enough data yet

        series = pd.Series(closes)
        fast_ema = series.ewm(span=self._fast, adjust=False).mean().iloc[-1]
        slow_ema = series.ewm(span=self._slow, adjust=False).mean().iloc[-1]

        prev_fast = series.ewm(span=self._fast, adjust=False).mean().iloc[-2]
        prev_slow = series.ewm(span=self._slow, adjust=False).mean().iloc[-2]

        # Crossover detection
        bullish_cross = fast_ema > slow_ema and prev_fast <= prev_slow
        bearish_cross = fast_ema < slow_ema and prev_fast >= prev_slow

        last = self._last_direction.get(bar.symbol)

        if bullish_cross and last != SignalDirection.LONG:
            self._last_direction[bar.symbol] = SignalDirection.LONG
            strength = min(1.0, abs(fast_ema - slow_ema) / slow_ema * 100)
            return Signal(
                symbol=bar.symbol,
                direction=SignalDirection.LONG,
                strength=round(strength, 4),
                strategy_id=self.strategy_id,
                metadata={
                    "fast_ema": round(fast_ema, 4),
                    "slow_ema": round(slow_ema, 4),
                    "close": bar.close,
                },
            )

        if bearish_cross and last == SignalDirection.LONG:
            self._last_direction[bar.symbol] = SignalDirection.FLAT
            return Signal(
                symbol=bar.symbol,
                direction=SignalDirection.FLAT,
                strength=1.0,
                strategy_id=self.strategy_id,
                metadata={
                    "fast_ema": round(fast_ema, 4),
                    "slow_ema": round(slow_ema, 4),
                    "close": bar.close,
                },
            )

        return None


# ─────────────────────────────────────────────────────────────────────────────
# Signal Generator Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

class SignalGenerator:
    """
    Runs registered strategies on incoming Bar events.
    Publishes Signal events to the EventBus for the ExecutionEngine to consume.
    """

    def __init__(
        self,
        event_bus: EventBus,
        strategies: Optional[list[BaseStrategy]] = None,
    ) -> None:
        self._bus = event_bus
        self._strategies: list[BaseStrategy] = strategies or []
        self._task: Optional[asyncio.Task] = None
        self._queue = event_bus.subscribe({EventType.BAR})

    def add_strategy(self, strategy: BaseStrategy) -> None:
        self._strategies.append(strategy)
        logger.info(
            "Strategy registered",
            extra={
                "strategy_id": strategy.strategy_id,
                "symbols": strategy.symbols,
            },
        )

    async def start(self) -> None:
        self._task = asyncio.create_task(
            self._event_loop(), name="signal_generator"
        )
        logger.info(
            "SignalGenerator started",
            extra={"n_strategies": len(self._strategies)},
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("SignalGenerator stopped")

    async def _event_loop(self) -> None:
        while True:
            try:
                event: Event = await self._queue.get()
                if event.type == EventType.BAR:
                    bar: Bar = event.data
                    self._run_strategies(bar)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "SignalGenerator error",
                    extra={"error": str(exc)},
                    exc_info=True,
                )

    def _run_strategies(self, bar: Bar) -> None:
        for strategy in self._strategies:
            if bar.symbol not in strategy.symbols:
                continue
            try:
                signal = strategy.on_bar(bar)
                if signal:
                    self._bus.publish(Event(
                        type=EventType.SIGNAL,
                        data=signal,
                        source=strategy.strategy_id,
                    ))
                    logger.info(
                        "Signal generated",
                        extra={
                            "strategy": strategy.strategy_id,
                            "symbol": signal.symbol,
                            "direction": signal.direction,
                            "strength": signal.strength,
                        },
                    )
            except Exception as exc:
                logger.error(
                    "Strategy error",
                    extra={
                        "strategy": strategy.strategy_id,
                        "symbol": bar.symbol,
                        "error": str(exc),
                    },
                    exc_info=True,
                )
