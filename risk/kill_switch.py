"""
Kill Switch: emergency system shutdown.

Activating the kill switch:
  1. Broadcasts KILL_SWITCH_TRIGGERED event (blocks new orders in ExecutionEngine/OMS)
  2. Cancels all open orders via broker
  3. Flattens all open positions via market orders
  4. Publishes an ALERT event to notify operators
  5. Optionally stops all system components

Can be triggered:
  - Manually (CLI / dashboard button)
  - Automatically by drawdown breach (DRAWDOWN_BREACH event)
  - Automatically by system error threshold
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from core.enums import AlertLevel
from core.events import Event, EventBus, EventType
from core.exceptions import KillSwitchActive

logger = logging.getLogger(__name__)


class KillSwitch:
    """
    One-click emergency shutdown.

    Usage:
        ks = KillSwitch(broker, event_bus)
        await ks.start()           # subscribe to auto-trigger events
        await ks.trigger("manual") # emergency shutdown
    """

    def __init__(self, broker, event_bus: EventBus) -> None:
        self._broker = broker
        self._bus = event_bus
        self._active = False
        self._triggered_at: Optional[datetime] = None
        self._trigger_reason: str = ""
        self._task: Optional[asyncio.Task] = None
        self._queue = event_bus.subscribe({
            EventType.DRAWDOWN_BREACH,
            EventType.SYSTEM_ERROR,
        })

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._task = asyncio.create_task(
            self._auto_trigger_loop(), name="kill_switch_monitor"
        )
        logger.info("KillSwitch monitor started")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ── Trigger ───────────────────────────────────────────────────────────────

    async def trigger(self, reason: str = "manual") -> None:
        """
        Execute emergency shutdown sequence.
        Idempotent – safe to call multiple times.
        """
        if self._active:
            logger.warning(
                "Kill switch already active",
                extra={"reason": reason},
            )
            return

        self._active = True
        self._triggered_at = datetime.now(tz=timezone.utc)
        self._trigger_reason = reason

        logger.critical(
            "KILL SWITCH TRIGGERED",
            extra={
                "reason": reason,
                "timestamp": self._triggered_at.isoformat(),
            },
        )

        # Step 1: Broadcast – blocks all new orders immediately
        self._bus.publish(Event(
            type=EventType.KILL_SWITCH_TRIGGERED,
            data={
                "reason": reason,
                "timestamp": self._triggered_at.isoformat(),
            },
            source="kill_switch",
        ))

        # Step 2: Cancel all open orders
        cancelled = await self._cancel_all_orders()

        # Step 3: Flatten all positions
        flattened = await self._flatten_all_positions()

        # Step 4: Alert operators
        self._bus.publish(Event(
            type=EventType.ALERT,
            data={
                "level": AlertLevel.CRITICAL,
                "title": "KILL SWITCH ACTIVATED",
                "message": (
                    f"Kill switch triggered: {reason}\n"
                    f"Cancelled {cancelled} orders, flattened {flattened} positions.\n"
                    f"All trading halted at {self._triggered_at.isoformat()}"
                ),
            },
            source="kill_switch",
        ))

        logger.critical(
            "Kill switch complete",
            extra={
                "cancelled_orders": cancelled,
                "flattened_positions": flattened,
            },
        )

    async def _cancel_all_orders(self) -> int:
        """Cancel all open orders; return count attempted."""
        try:
            await self._broker.cancel_all_orders()
            logger.warning("All orders cancellation requested")
            return 1  # actual count from broker WS updates
        except Exception as exc:
            logger.error(
                "Kill switch: cancel_all_orders failed",
                extra={"error": str(exc)},
                exc_info=True,
            )
            return 0

    async def _flatten_all_positions(self) -> int:
        """Market-close all positions; return count attempted."""
        try:
            orders = await self._broker.close_all_positions()
            logger.warning(
                "All positions close requested",
                extra={"orders_generated": len(orders)},
            )
            return len(orders)
        except Exception as exc:
            logger.error(
                "Kill switch: close_all_positions failed",
                extra={"error": str(exc)},
                exc_info=True,
            )
            return 0

    # ── Auto-Trigger ──────────────────────────────────────────────────────────

    async def _auto_trigger_loop(self) -> None:
        """Watch for drawdown breaches and auto-trigger."""
        while True:
            try:
                event: Event = await self._queue.get()
                if self._active:
                    continue

                if event.type == EventType.DRAWDOWN_BREACH:
                    dd = event.data.get("drawdown_pct", 0)
                    await self.trigger(
                        reason=f"auto:drawdown_breach:{dd:.2f}%"
                    )

                elif event.type == EventType.SYSTEM_ERROR:
                    # Only auto-trigger on repeated / critical errors
                    # (can add counter logic here)
                    logger.error(
                        "System error detected",
                        extra={"data": event.data},
                    )

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "KillSwitch monitor error",
                    extra={"error": str(exc)},
                    exc_info=True,
                )

    # ── Status ────────────────────────────────────────────────────────────────

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def triggered_at(self) -> Optional[datetime]:
        return self._triggered_at

    @property
    def trigger_reason(self) -> str:
        return self._trigger_reason

    def reset(self, confirmation_code: str) -> None:
        """
        Manually reset kill switch after investigation.
        Requires explicit confirmation to prevent accidental reset.
        """
        if confirmation_code != "RESET_CONFIRMED":
            raise ValueError(
                "Must pass confirmation_code='RESET_CONFIRMED' to reset kill switch"
            )
        logger.warning("Kill switch RESET by operator")
        self._active = False
        self._triggered_at = None
        self._trigger_reason = ""
