"""
Async event bus.  Components publish typed events; subscribers receive them
via asyncio.Queue.  Decouples signal generator, OMS, execution engine, etc.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import auto, Enum
from typing import Any, Optional

from core.models import Bar, Fill, Order, Portfolio, Quote, Signal

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    # Market data
    QUOTE = "quote"
    BAR = "bar"

    # Signals
    SIGNAL = "signal"

    # Orders
    ORDER_CREATED = "order_created"
    ORDER_SUBMITTED = "order_submitted"
    ORDER_ACCEPTED = "order_accepted"
    ORDER_FILLED = "order_filled"
    ORDER_PARTIAL_FILL = "order_partial_fill"
    ORDER_CANCELLED = "order_cancelled"
    ORDER_REJECTED = "order_rejected"

    # Position / portfolio
    POSITION_UPDATE = "position_update"
    PORTFOLIO_UPDATE = "portfolio_update"

    # Risk / system
    KILL_SWITCH_TRIGGERED = "kill_switch_triggered"
    DRAWDOWN_BREACH = "drawdown_breach"
    RECONCILIATION_DISCREPANCY = "reconciliation_discrepancy"
    SYSTEM_ERROR = "system_error"
    SYSTEM_SHUTDOWN = "system_shutdown"

    # Alerts
    ALERT = "alert"


@dataclass
class Event:
    type: EventType
    data: Any
    source: str = "system"
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )


class EventBus:
    """
    Simple async pub/sub.  Subscribers register for one or more EventTypes.
    Publishing puts the event onto every matching subscriber's asyncio.Queue.

    Usage:
        bus = EventBus()
        q = bus.subscribe({EventType.QUOTE, EventType.BAR})
        bus.publish(Event(EventType.QUOTE, quote_data))
        event = await q.get()
    """

    def __init__(self, maxsize: int = 10_000) -> None:
        self._maxsize = maxsize
        # event_type → list of queues
        self._subscribers: dict[EventType, list[asyncio.Queue[Event]]] = {
            et: [] for et in EventType
        }

    def subscribe(
        self,
        event_types: set[EventType],
        maxsize: Optional[int] = None,
    ) -> asyncio.Queue[Event]:
        """Register interest in a set of event types. Returns a Queue."""
        q: asyncio.Queue[Event] = asyncio.Queue(
            maxsize=maxsize or self._maxsize
        )
        for et in event_types:
            self._subscribers[et].append(q)
        return q

    def publish(self, event: Event) -> None:
        """
        Non-blocking publish.  If a subscriber queue is full the event is
        dropped and a warning is logged (never block the publishing thread).
        """
        for q in self._subscribers.get(event.type, []):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning(
                    "EventBus: subscriber queue full, dropping event",
                    extra={"event_type": event.type, "source": event.source},
                )

    async def publish_async(self, event: Event) -> None:
        """Async publish – awaits if any queue is full."""
        for q in self._subscribers.get(event.type, []):
            await q.put(event)

    def unsubscribe(self, q: asyncio.Queue[Event]) -> None:
        """Remove a queue from all subscriber lists."""
        for qs in self._subscribers.values():
            try:
                qs.remove(q)
            except ValueError:
                pass
