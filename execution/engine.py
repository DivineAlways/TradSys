"""
Execution Engine.

Receives Signal events from the EventBus, applies sizing / risk checks,
converts signals to Orders, and submits them to the OMS.

Signal → [Sizing] → [Risk Pre-Check] → Order → OMS → Broker
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Optional

from config.settings import get_settings
from core.enums import OrderSide, OrderType, SignalDirection, TimeInForce
from core.events import Event, EventBus, EventType
from core.exceptions import KillSwitchActive, RiskLimitBreached
from core.models import Order, Signal
from market_data.feed_manager import FeedManager
from oms.order_manager import OrderManager
from oms.position_tracker import PositionTracker

logger = logging.getLogger(__name__)


class ExecutionEngine:
    """
    Signal-to-order pipeline with inline risk pre-checks:

    1. Signal arrives via EventBus
    2. Size the order (fixed notional or signal.target_qty)
    3. Pre-flight risk: position limits, order rate limit, kill switch
    4. Build Order object
    5. Submit to OMS → broker
    """

    def __init__(
        self,
        event_bus: EventBus,
        oms: OrderManager,
        position_tracker: PositionTracker,
        feed_manager: FeedManager,
    ) -> None:
        self._bus = event_bus
        self._oms = oms
        self._positions = position_tracker
        self._feed = feed_manager
        self._settings = get_settings()
        self._kill_active = False
        self._task: Optional[asyncio.Task] = None

        # Rate-limit tracking: sliding window of order submission timestamps
        self._order_times: deque[float] = deque()

        self._queue = event_bus.subscribe({
            EventType.SIGNAL,
            EventType.KILL_SWITCH_TRIGGERED,
        })

    async def start(self) -> None:
        self._task = asyncio.create_task(
            self._event_loop(), name="execution_engine"
        )
        logger.info("ExecutionEngine started")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("ExecutionEngine stopped")

    # ── Event Loop ────────────────────────────────────────────────────────────

    async def _event_loop(self) -> None:
        while True:
            try:
                event: Event = await self._queue.get()
                if event.type == EventType.KILL_SWITCH_TRIGGERED:
                    self._kill_active = True
                    logger.warning("ExecutionEngine: kill switch active, rejecting all signals")
                elif event.type == EventType.SIGNAL:
                    await self._process_signal(event.data)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "ExecutionEngine event loop error",
                    extra={"error": str(exc)},
                    exc_info=True,
                )

    # ── Signal Processing ─────────────────────────────────────────────────────

    async def _process_signal(self, signal: Signal) -> None:
        if self._kill_active:
            logger.warning(
                "Signal rejected: kill switch active",
                extra={"signal_id": signal.id, "symbol": signal.symbol},
            )
            return

        logger.info(
            "Processing signal",
            extra={
                "signal_id": signal.id,
                "symbol": signal.symbol,
                "direction": signal.direction,
                "strength": signal.strength,
            },
        )

        try:
            order = self._build_order(signal)
            if order is None:
                return
            self._pre_flight_risk_check(order, signal)
            submitted = await self._oms.submit_order(order)
            logger.info(
                "Order created from signal",
                extra={
                    "signal_id": signal.id,
                    "order_id": submitted.id,
                    "symbol": submitted.symbol,
                    "side": submitted.side,
                    "qty": submitted.qty,
                },
            )
        except (KillSwitchActive, RiskLimitBreached) as exc:
            logger.warning(
                "Order blocked by risk",
                extra={"signal_id": signal.id, "reason": str(exc)},
            )
        except Exception as exc:
            logger.error(
                "Signal processing error",
                extra={"signal_id": signal.id, "error": str(exc)},
                exc_info=True,
            )

    def _build_order(self, signal: Signal) -> Optional[Order]:
        """Convert signal to Order, determining qty and side."""
        symbol = signal.symbol
        direction = signal.direction

        # Determine order side and qty
        if direction == SignalDirection.FLAT:
            # Close existing position
            pos = self._positions.get_position(symbol)
            if not pos or pos.is_flat():
                logger.debug("FLAT signal but no position", extra={"symbol": symbol})
                return None
            side = OrderSide.SELL if pos.qty > 0 else OrderSide.BUY
            qty = abs(pos.qty)

        elif direction == SignalDirection.LONG:
            side = OrderSide.BUY
            qty = self._size_order(signal)
            if qty <= 0:
                return None

        elif direction == SignalDirection.SHORT:
            side = OrderSide.SELL
            qty = self._size_order(signal)
            if qty <= 0:
                return None

        else:
            logger.warning("Unknown signal direction", extra={"direction": direction})
            return None

        # Use limit order if price hint provided, else market
        if signal.price_hint:
            return Order(
                symbol=symbol,
                side=side,
                order_type=OrderType.LIMIT,
                qty=qty,
                limit_price=signal.price_hint,
                time_in_force=TimeInForce.DAY,
                signal_id=signal.id,
                strategy_id=signal.strategy_id,
            )

        return Order(
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            qty=qty,
            time_in_force=TimeInForce.DAY,
            signal_id=signal.id,
            strategy_id=signal.strategy_id,
        )

    def _size_order(self, signal: Signal) -> float:
        """
        Determine order quantity.
        Priority: signal.target_qty → fixed notional sizing.
        """
        if signal.target_qty > 0:
            return signal.target_qty

        # Fixed notional sizing: scale by signal strength
        mid = self._feed.get_mid_price(signal.symbol)
        if not mid or mid <= 0:
            logger.warning(
                "No price for sizing",
                extra={"symbol": signal.symbol},
            )
            return 0.0

        notional = self._settings.max_position_size_usd * signal.strength
        qty = notional / mid

        # Round to nearest share (equity minimum 1)
        qty = max(1.0, round(qty, 0))
        return qty

    # ── Risk Pre-Flight ───────────────────────────────────────────────────────

    def _pre_flight_risk_check(self, order: Order, signal: Signal) -> None:
        """Raise RiskLimitBreached if any pre-trade risk limit is hit."""

        # 1. Kill switch
        if self._kill_active:
            raise KillSwitchActive("Kill switch active")

        # 2. Order rate limit (max N orders per minute)
        now = time.monotonic()
        self._order_times.append(now)
        # Purge orders older than 60 seconds
        while self._order_times and (now - self._order_times[0]) > 60:
            self._order_times.popleft()
        if len(self._order_times) > self._settings.max_orders_per_minute:
            raise RiskLimitBreached(
                f"Order rate limit: {self._settings.max_orders_per_minute}/min",
                limit_name="order_rate",
            )

        # 3. Max position size
        mid = self._feed.get_mid_price(order.symbol) or 0
        order_value = order.qty * mid
        if order_value > self._settings.max_position_size_usd * 1.1:  # 10% tolerance
            raise RiskLimitBreached(
                f"Order value ${order_value:.0f} exceeds max "
                f"${self._settings.max_position_size_usd}",
                limit_name="position_size",
            )

        # 4. Daily loss limit
        portfolio = self._positions.get_portfolio()
        if portfolio.realised_pnl_today < -self._settings.max_daily_loss_usd:
            raise RiskLimitBreached(
                f"Daily loss limit breached: "
                f"${portfolio.realised_pnl_today:.0f}",
                limit_name="daily_loss",
            )
