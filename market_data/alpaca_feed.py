"""
Alpaca market data WebSocket feed.

Subscribes to real-time quotes and bars for a list of symbols using the
Alpaca Data Stream (IEX feed – free, no subscription needed).

Reconnection uses exponential backoff via tenacity.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

import websockets
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from core.events import Event, EventBus, EventType
from core.models import Bar, Quote

logger = logging.getLogger(__name__)

# Alpaca IEX feed (free): real-time during market hours in paper mode
_WS_URL = "wss://stream.data.alpaca.markets/v2/iex"


class AlpacaDataFeed:
    """
    Subscribes to Alpaca WS data stream and publishes Quote / Bar events.

    Usage:
        feed = AlpacaDataFeed(bus, api_key, secret_key, symbols=["AAPL"])
        await feed.start()
        ...
        await feed.stop()
    """

    def __init__(
        self,
        event_bus: EventBus,
        api_key: str,
        secret_key: str,
        symbols: list[str],
        ws_url: str = _WS_URL,
    ) -> None:
        self._bus = event_bus
        self._api_key = api_key
        self._secret_key = secret_key
        self._symbols = symbols
        self._ws_url = ws_url
        self._task: Optional[asyncio.Task] = None
        self._running = False

        # Last known quote per symbol (used by execution engine for mid-price)
        self.latest_quotes: dict[str, Quote] = {}
        self.latest_bars: dict[str, Bar] = {}

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(
            self._run_with_retry(), name="alpaca_data_feed"
        )
        logger.info(
            "Alpaca data feed starting",
            extra={"symbols": self._symbols},
        )

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Alpaca data feed stopped")

    @retry(
        retry=retry_if_exception_type((
            websockets.ConnectionClosed,
            ConnectionResetError,
            OSError,
        )),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(20),
        reraise=True,
    )
    async def _run_with_retry(self) -> None:
        """Connect to Alpaca WS, authenticate, subscribe, and consume messages."""
        if not self._running:
            return

        logger.info(
            "Connecting to Alpaca data stream",
            extra={"url": self._ws_url},
        )
        async with websockets.connect(
            self._ws_url,
            ping_interval=20,
            ping_timeout=10,
        ) as ws:
            # Step 1: receive connected message
            msg = json.loads(await ws.recv())
            if not (isinstance(msg, list) and msg[0].get("T") == "success"):
                raise ConnectionError(f"Unexpected connect msg: {msg}")

            # Step 2: authenticate
            await ws.send(json.dumps({
                "action": "auth",
                "key": self._api_key,
                "secret": self._secret_key,
            }))
            auth_resp = json.loads(await ws.recv())
            if not (
                isinstance(auth_resp, list)
                and auth_resp[0].get("T") == "success"
                and auth_resp[0].get("msg") == "authenticated"
            ):
                raise ConnectionError(f"Auth failed: {auth_resp}")

            # Step 3: subscribe
            await ws.send(json.dumps({
                "action": "subscribe",
                "quotes": self._symbols,
                "bars": self._symbols,
            }))
            sub_resp = json.loads(await ws.recv())
            logger.info(
                "Subscribed to Alpaca data",
                extra={"response": sub_resp},
            )

            # Step 4: consume messages
            async for raw in ws:
                if not self._running:
                    break
                try:
                    messages = json.loads(raw)
                    if isinstance(messages, list):
                        for msg in messages:
                            self._dispatch(msg)
                except json.JSONDecodeError:
                    logger.warning("Bad JSON from data stream", extra={"raw": raw[:200]})
                except Exception as exc:
                    logger.error(
                        "Error processing data message",
                        extra={"error": str(exc)},
                        exc_info=True,
                    )

    def _dispatch(self, msg: dict) -> None:
        msg_type = msg.get("T")
        if msg_type == "q":
            self._handle_quote(msg)
        elif msg_type == "b":
            self._handle_bar(msg)
        elif msg_type == "error":
            logger.error("Data stream error", extra={"msg": msg})
            self._bus.publish(Event(
                type=EventType.SYSTEM_ERROR,
                data={"component": "alpaca_data_feed", "msg": msg},
                source="alpaca_feed",
            ))

    def _handle_quote(self, msg: dict) -> None:
        try:
            quote = Quote(
                symbol=msg["S"],
                bid=float(msg.get("bp", 0)),
                ask=float(msg.get("ap", 0)),
                bid_size=float(msg.get("bs", 0)),
                ask_size=float(msg.get("as", 0)),
            )
            self.latest_quotes[quote.symbol] = quote
            self._bus.publish(Event(
                type=EventType.QUOTE,
                data=quote,
                source="alpaca_feed",
            ))
        except Exception as exc:
            logger.warning(
                "Quote parse error",
                extra={"error": str(exc), "msg": msg},
            )

    def _handle_bar(self, msg: dict) -> None:
        try:
            bar = Bar(
                symbol=msg["S"],
                open=float(msg.get("o", 0)),
                high=float(msg.get("h", 0)),
                low=float(msg.get("l", 0)),
                close=float(msg.get("c", 0)),
                volume=float(msg.get("v", 0)),
                vwap=float(msg.get("vw", 0)),
            )
            self.latest_bars[bar.symbol] = bar
            self._bus.publish(Event(
                type=EventType.BAR,
                data=bar,
                source="alpaca_feed",
            ))
        except Exception as exc:
            logger.warning(
                "Bar parse error",
                extra={"error": str(exc), "msg": msg},
            )
