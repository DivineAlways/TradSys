"""
FeedManager: high-level coordinator for all market data feeds.
Provides a unified interface for the rest of the system to query
latest market data without knowing which feed is the source.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from core.events import EventBus
from core.models import Bar, Quote
from market_data.alpaca_feed import AlpacaDataFeed

logger = logging.getLogger(__name__)


class FeedManager:
    """
    Manages one or more data feed connections.
    Currently supports Alpaca; can be extended with other feeds.

    Usage:
        fm = FeedManager(bus, api_key, secret, symbols=["AAPL"])
        await fm.start()
        price = fm.get_mid_price("AAPL")
        await fm.stop()
    """

    def __init__(
        self,
        event_bus: EventBus,
        api_key: str,
        secret_key: str,
        symbols: list[str],
    ) -> None:
        self._bus = event_bus
        self._feed = AlpacaDataFeed(
            event_bus=event_bus,
            api_key=api_key,
            secret_key=secret_key,
            symbols=symbols,
        )
        self._symbols = symbols
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        await self._feed.start()
        self._started = True
        logger.info("FeedManager started", extra={"symbols": self._symbols})

    async def stop(self) -> None:
        if not self._started:
            return
        await self._feed.stop()
        self._started = False
        logger.info("FeedManager stopped")

    def get_latest_quote(self, symbol: str) -> Optional[Quote]:
        return self._feed.latest_quotes.get(symbol)

    def get_latest_bar(self, symbol: str) -> Optional[Bar]:
        return self._feed.latest_bars.get(symbol)

    def get_mid_price(self, symbol: str) -> Optional[float]:
        """Return latest mid price or close if no quote available."""
        quote = self._feed.latest_quotes.get(symbol)
        if quote and quote.bid > 0 and quote.ask > 0:
            return quote.mid
        bar = self._feed.latest_bars.get(symbol)
        if bar:
            return bar.close
        return None

    def get_all_quotes(self) -> dict[str, Quote]:
        return dict(self._feed.latest_quotes)

    async def wait_for_data(
        self,
        symbols: list[str],
        timeout: float = 30.0,
    ) -> bool:
        """
        Wait until we have at least one quote or bar for every requested symbol.
        Returns True if data arrived within timeout, False otherwise.
        """
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if all(
                s in self._feed.latest_quotes or s in self._feed.latest_bars
                for s in symbols
            ):
                return True
            await asyncio.sleep(0.5)
        logger.warning(
            "Timeout waiting for market data",
            extra={"missing": [s for s in symbols if s not in self._feed.latest_quotes]},
        )
        return False
