"""
Real-time position and portfolio tracker.

Listens for fill events and updates positions / P&L accordingly.
Also refreshes current prices from market data quotes.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from core.enums import OrderSide
from core.events import Event, EventBus, EventType
from core.models import Fill, Portfolio, Position, Quote

logger = logging.getLogger(__name__)


class PositionTracker:
    """
    Maintains real-time portfolio state:
    - Positions per symbol (qty, avg_cost, unrealised P&L)
    - Realised P&L (daily running total)
    - Portfolio-level metrics (equity, drawdown %)

    Thread-safe via asyncio.Lock (single-threaded event loop assumption).
    """

    def __init__(self, event_bus: EventBus, initial_cash: float = 0.0) -> None:
        self._bus = event_bus
        self._positions: dict[str, Position] = {}
        self._cash = initial_cash
        self._equity = initial_cash
        self._peak_equity = initial_cash
        self._realised_pnl_today = 0.0
        self._buying_power = initial_cash
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._queue = event_bus.subscribe({
            EventType.ORDER_FILLED,
            EventType.ORDER_PARTIAL_FILL,
            EventType.QUOTE,
            EventType.PORTFOLIO_UPDATE,
        })

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._task = asyncio.create_task(
            self._event_loop(), name="position_tracker"
        )
        logger.info("PositionTracker started")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("PositionTracker stopped")

    def sync_from_broker(self, portfolio: Portfolio) -> None:
        """
        Override internal state from a broker reconciliation snapshot.
        Called at startup and after reconciliation.
        """
        self._cash = portfolio.cash
        self._equity = portfolio.equity
        self._buying_power = portfolio.buying_power
        self._positions = dict(portfolio.positions)
        if portfolio.equity > self._peak_equity:
            self._peak_equity = portfolio.equity
        logger.info(
            "PositionTracker synced from broker",
            extra={"equity": self._equity, "n_positions": len(self._positions)},
        )

    # ── Event Loop ────────────────────────────────────────────────────────────

    async def _event_loop(self) -> None:
        while True:
            try:
                event: Event = await self._queue.get()
                await self._handle_event(event)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "PositionTracker event error",
                    extra={"error": str(exc)},
                    exc_info=True,
                )

    async def _handle_event(self, event: Event) -> None:
        if event.type in (EventType.ORDER_FILLED, EventType.ORDER_PARTIAL_FILL):
            fill: Optional[Fill] = None
            if isinstance(event.data, dict):
                fill = event.data.get("fill")
            if fill:
                await self._apply_fill(fill)

        elif event.type == EventType.QUOTE:
            quote: Quote = event.data
            await self._update_price(quote.symbol, quote.mid)

        elif event.type == EventType.PORTFOLIO_UPDATE:
            portfolio: Portfolio = event.data
            async with self._lock:
                self._cash = portfolio.cash
                self._equity = portfolio.equity
                self._buying_power = portfolio.buying_power
                if self._equity > self._peak_equity:
                    self._peak_equity = self._equity

    async def _apply_fill(self, fill: Fill) -> None:
        async with self._lock:
            symbol = fill.symbol
            pos = self._positions.get(symbol) or Position(symbol=symbol)

            if fill.side == OrderSide.BUY:
                new_qty = pos.qty + fill.qty
                if new_qty == 0:
                    self._positions.pop(symbol, None)
                    return
                # Weighted average cost
                new_avg = (
                    (pos.avg_cost * pos.qty + fill.price * fill.qty) / new_qty
                    if new_qty > 0 else fill.price
                )
                pos = pos.model_copy(update={
                    "qty": new_qty,
                    "avg_cost": new_avg,
                    "last_updated": datetime.now(tz=timezone.utc),
                })
                self._cash -= fill.gross_value + fill.commission

            else:  # SELL
                realised = (fill.price - pos.avg_cost) * fill.qty
                self._realised_pnl_today += realised
                new_qty = pos.qty - fill.qty
                if abs(new_qty) < 1e-9:
                    self._positions.pop(symbol, None)
                    self._cash += fill.gross_value - fill.commission
                    self._publish_portfolio()
                    return
                pos = pos.model_copy(update={
                    "qty": new_qty,
                    "last_updated": datetime.now(tz=timezone.utc),
                })
                self._cash += fill.gross_value - fill.commission

            self._positions[symbol] = pos
            self._recalculate_equity()
            self._publish_portfolio()

            logger.info(
                "Position updated",
                extra={
                    "symbol": symbol,
                    "qty": pos.qty,
                    "avg_cost": pos.avg_cost,
                    "unrealised_pnl": pos.unrealised_pnl,
                },
            )

    async def _update_price(self, symbol: str, price: float) -> None:
        if symbol not in self._positions:
            return
        async with self._lock:
            pos = self._positions[symbol]
            self._positions[symbol] = pos.model_copy(update={
                "current_price": price,
                "last_updated": datetime.now(tz=timezone.utc),
            })
            self._recalculate_equity()

    def _recalculate_equity(self) -> None:
        """Recompute equity from cash + mark-to-market positions."""
        market_value = sum(p.market_value for p in self._positions.values())
        self._equity = self._cash + market_value
        if self._equity > self._peak_equity:
            self._peak_equity = self._equity

    def _publish_portfolio(self) -> None:
        portfolio = self.get_portfolio()
        self._bus.publish(Event(
            type=EventType.PORTFOLIO_UPDATE,
            data=portfolio,
            source="position_tracker",
        ))
        # Drawdown breach check
        if portfolio.drawdown_pct > 0:
            from config.settings import get_settings
            if portfolio.drawdown_pct >= get_settings().max_drawdown_pct:
                self._bus.publish(Event(
                    type=EventType.DRAWDOWN_BREACH,
                    data={
                        "drawdown_pct": portfolio.drawdown_pct,
                        "equity": portfolio.equity,
                        "peak_equity": portfolio.peak_equity,
                    },
                    source="position_tracker",
                ))

    # ── Query API ─────────────────────────────────────────────────────────────

    def get_position(self, symbol: str) -> Optional[Position]:
        return self._positions.get(symbol)

    def get_all_positions(self) -> dict[str, Position]:
        return dict(self._positions)

    def get_portfolio(self) -> Portfolio:
        return Portfolio(
            cash=self._cash,
            equity=self._equity,
            buying_power=self._buying_power,
            positions=dict(self._positions),
            realised_pnl_today=self._realised_pnl_today,
            unrealised_pnl=sum(p.unrealised_pnl for p in self._positions.values()),
            peak_equity=self._peak_equity,
        )

    @property
    def drawdown_pct(self) -> float:
        if self._peak_equity <= 0:
            return 0.0
        return (self._peak_equity - self._equity) / self._peak_equity * 100.0

    @property
    def gross_exposure(self) -> float:
        return sum(abs(p.market_value) for p in self._positions.values())
