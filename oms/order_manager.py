"""
Order Management System (OMS).

Tracks every order from creation to terminal state using a strict state machine.
Subscribes to broker order update events and applies transitions atomically.

State machine:
    NEW → SUBMITTED → ACCEPTED → PARTIAL_FILL → FILLED
                               ↘ CANCELLED
                               ↘ REJECTED
                               ↘ EXPIRED
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from threading import Lock
from typing import Optional

from brokers.base import BrokerProtocol
from core.enums import ORDER_TRANSITIONS, TERMINAL_STATUSES, OrderStatus
from core.events import Event, EventBus, EventType
from core.exceptions import BrokerOrderError, InvalidOrderTransitionError, KillSwitchActive
from core.models import Fill, Order

logger = logging.getLogger(__name__)


class OrderManager:
    """
    Central registry for all orders.

    Responsibilities:
    - Persist order state in memory (append to audit DB via events)
    - Enforce state machine transitions
    - Apply fill updates and update avg fill price
    - Listen to EventBus for broker updates
    - Expose query API (get_order, get_open_orders, etc.)
    """

    def __init__(self, broker: BrokerProtocol, event_bus: EventBus) -> None:
        self._broker = broker
        self._bus = event_bus
        self._orders: dict[str, Order] = {}                 # internal id → Order
        self._broker_id_map: dict[str, str] = {}            # broker_id → internal id
        self._lock = asyncio.Lock()
        self._kill_switch_active = False
        self._listener_task: Optional[asyncio.Task] = None
        self._queue = event_bus.subscribe({
            EventType.ORDER_ACCEPTED,
            EventType.ORDER_FILLED,
            EventType.ORDER_PARTIAL_FILL,
            EventType.ORDER_CANCELLED,
            EventType.ORDER_REJECTED,
            EventType.KILL_SWITCH_TRIGGERED,
        })

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._listener_task = asyncio.create_task(
            self._event_loop(), name="oms_event_loop"
        )
        logger.info("OMS started")

    async def stop(self) -> None:
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        logger.info("OMS stopped")

    # ── Order Submission ──────────────────────────────────────────────────────

    async def submit_order(self, order: Order) -> Order:
        """
        Register, validate, and send an order to the broker.
        Returns the updated Order with broker_order_id set.
        """
        if self._kill_switch_active:
            raise KillSwitchActive("Kill switch is active; order rejected")

        async with self._lock:
            self._orders[order.id] = order

        self._bus.publish(Event(
            type=EventType.ORDER_CREATED,
            data=order,
            source="oms",
        ))

        try:
            submitted = await self._broker.place_order(order)
            async with self._lock:
                submitted = self._transition(submitted, OrderStatus.SUBMITTED)
                self._orders[submitted.id] = submitted
                if submitted.broker_order_id:
                    self._broker_id_map[submitted.broker_order_id] = submitted.id

            self._bus.publish(Event(
                type=EventType.ORDER_SUBMITTED,
                data=submitted,
                source="oms",
            ))
            logger.info(
                "Order submitted",
                extra={
                    "order_id": submitted.id,
                    "broker_order_id": submitted.broker_order_id,
                    "symbol": submitted.symbol,
                    "side": submitted.side,
                    "qty": submitted.qty,
                    "type": submitted.order_type,
                },
            )
            return submitted

        except (BrokerOrderError, Exception) as exc:
            async with self._lock:
                failed = self._orders[order.id].model_copy(update={
                    "status": OrderStatus.REJECTED,
                    "reject_reason": str(exc),
                })
                self._orders[order.id] = failed
            self._bus.publish(Event(
                type=EventType.ORDER_REJECTED,
                data={"update": {"status": OrderStatus.REJECTED}, "order": failed},
                source="oms",
            ))
            logger.error(
                "Order submission failed",
                extra={"order_id": order.id, "error": str(exc)},
            )
            raise

    async def cancel_order(self, order_id: str) -> Optional[Order]:
        """Request cancellation of an order by internal ID."""
        async with self._lock:
            order = self._orders.get(order_id)
        if not order:
            logger.warning("cancel_order: unknown order", extra={"order_id": order_id})
            return None
        if order.is_terminal:
            logger.info(
                "cancel_order: already terminal",
                extra={"order_id": order_id, "status": order.status},
            )
            return order
        return await self._broker.cancel_order(order)

    # ── State Machine ─────────────────────────────────────────────────────────

    def _transition(self, order: Order, new_status: OrderStatus) -> Order:
        """Apply a state transition; raise InvalidOrderTransitionError if illegal."""
        allowed = ORDER_TRANSITIONS.get(order.status, set())
        if new_status not in allowed:
            raise InvalidOrderTransitionError(order.status, new_status)
        return order.model_copy(update={"status": new_status})

    def _apply_fill(self, order: Order, fill: Fill) -> Order:
        """Update order with fill data; recalculate avg fill price."""
        new_filled_qty = order.filled_qty + fill.qty
        # Weighted average
        if new_filled_qty > 0:
            new_avg_price = (
                (order.avg_fill_price * order.filled_qty) + (fill.price * fill.qty)
            ) / new_filled_qty
        else:
            new_avg_price = fill.price

        new_status = (
            OrderStatus.FILLED
            if new_filled_qty >= order.qty
            else OrderStatus.PARTIAL_FILL
        )

        return order.model_copy(update={
            "filled_qty": new_filled_qty,
            "avg_fill_price": new_avg_price,
            "fills": order.fills + [fill],
            "status": new_status,
            "filled_at": datetime.now(tz=timezone.utc) if new_status == OrderStatus.FILLED else None,
        })

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
                    "OMS event loop error",
                    extra={"error": str(exc)},
                    exc_info=True,
                )

    async def _handle_event(self, event: Event) -> None:
        if event.type == EventType.KILL_SWITCH_TRIGGERED:
            self._kill_switch_active = True
            return

        data = event.data
        update = data.get("update", {}) if isinstance(data, dict) else {}
        broker_order_id = update.get("broker_order_id")
        if not broker_order_id:
            return

        async with self._lock:
            internal_id = self._broker_id_map.get(broker_order_id)
            if not internal_id:
                logger.warning(
                    "OMS: received update for unknown broker order",
                    extra={"broker_order_id": broker_order_id},
                )
                return
            order = self._orders[internal_id]

        try:
            async with self._lock:
                if event.type in (EventType.ORDER_FILLED, EventType.ORDER_PARTIAL_FILL):
                    fill: Fill = data.get("fill")
                    if fill:
                        order = self._apply_fill(order, fill)
                    self._orders[internal_id] = order

                elif event.type == EventType.ORDER_CANCELLED:
                    order = order.model_copy(update={
                        "status": OrderStatus.CANCELLED,
                        "cancelled_at": datetime.now(tz=timezone.utc),
                    })
                    self._orders[internal_id] = order

                elif event.type == EventType.ORDER_REJECTED:
                    order = order.model_copy(update={
                        "status": OrderStatus.REJECTED,
                        "reject_reason": data.get("reason", ""),
                    })
                    self._orders[internal_id] = order

                elif event.type == EventType.ORDER_ACCEPTED:
                    if order.status == OrderStatus.SUBMITTED:
                        order = order.model_copy(update={"status": OrderStatus.ACCEPTED})
                        self._orders[internal_id] = order

            logger.info(
                "Order state updated",
                extra={
                    "order_id": internal_id,
                    "broker_order_id": broker_order_id,
                    "status": order.status,
                    "filled_qty": order.filled_qty,
                    "avg_price": order.avg_fill_price,
                },
            )

        except InvalidOrderTransitionError as exc:
            logger.warning("Invalid order transition", extra={"error": str(exc)})
        except Exception as exc:
            logger.error(
                "Error handling order event",
                extra={"error": str(exc), "event_type": event.type},
                exc_info=True,
            )

    # ── Query API ─────────────────────────────────────────────────────────────

    def get_order(self, order_id: str) -> Optional[Order]:
        return self._orders.get(order_id)

    def get_order_by_broker_id(self, broker_order_id: str) -> Optional[Order]:
        internal_id = self._broker_id_map.get(broker_order_id)
        return self._orders.get(internal_id) if internal_id else None

    def get_open_orders(self) -> list[Order]:
        return [o for o in self._orders.values() if not o.is_terminal]

    def get_orders_for_symbol(self, symbol: str) -> list[Order]:
        return [o for o in self._orders.values() if o.symbol == symbol]

    def get_all_orders(self) -> list[Order]:
        return list(self._orders.values())

    def order_count(self) -> int:
        return len(self._orders)

    def open_order_count(self) -> int:
        return sum(1 for o in self._orders.values() if not o.is_terminal)
