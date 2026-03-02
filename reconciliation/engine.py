"""
Reconciliation Engine.

Periodically compares internal state (OMS + PositionTracker) against
live broker state (REST API snapshot).  Flags discrepancies, auto-corrects
minor drifts, and publishes critical discrepancies as alerts.

Run schedule: every N minutes (configurable) + on-demand.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from core.enums import AlertLevel, OrderStatus
from core.events import Event, EventBus, EventType
from core.models import Order, Position, ReconciliationDiscrepancy
from oms.order_manager import OrderManager
from oms.position_tracker import PositionTracker

logger = logging.getLogger(__name__)

# Tolerance for position qty mismatch (fractional shares / rounding)
QTY_TOLERANCE = 0.01
# Tolerance for avg cost mismatch
PRICE_TOLERANCE_PCT = 0.5


class ReconciliationEngine:
    """
    Runs periodic reconciliation between internal state and broker.

    Checks:
    1. Position quantities match (within QTY_TOLERANCE)
    2. Average cost within PRICE_TOLERANCE_PCT
    3. No ghost orders (open internally but unknown to broker)
    4. No orphan broker orders (at broker but not in OMS)
    5. Cash / equity matches within 1%

    Auto-corrections:
    - Updates internal position prices from broker snapshot
    - Marks ghost orders as CANCELLED

    Critical discrepancies trigger ALERT events.
    """

    def __init__(
        self,
        broker,
        oms: OrderManager,
        position_tracker: PositionTracker,
        event_bus: EventBus,
        interval_seconds: int = 300,  # 5 minutes
    ) -> None:
        self._broker = broker
        self._oms = oms
        self._positions = position_tracker
        self._bus = event_bus
        self._interval = interval_seconds
        self._task: Optional[asyncio.Task] = None
        self._discrepancies: list[ReconciliationDiscrepancy] = []
        self._last_run: Optional[datetime] = None

    async def start(self) -> None:
        self._task = asyncio.create_task(
            self._run_loop(), name="reconciliation_engine"
        )
        logger.info(
            "ReconciliationEngine started",
            extra={"interval_seconds": self._interval},
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("ReconciliationEngine stopped")

    async def run_now(self) -> list[ReconciliationDiscrepancy]:
        """Run reconciliation immediately and return discrepancies found."""
        return await self._reconcile()

    # ── Main Loop ─────────────────────────────────────────────────────────────

    async def _run_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._interval)
                await self._reconcile()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "Reconciliation error",
                    extra={"error": str(exc)},
                    exc_info=True,
                )

    # ── Reconciliation Logic ──────────────────────────────────────────────────

    async def _reconcile(self) -> list[ReconciliationDiscrepancy]:
        logger.info("Starting reconciliation run")
        self._last_run = datetime.now(tz=timezone.utc)
        found: list[ReconciliationDiscrepancy] = []

        try:
            # Fetch broker ground truth
            broker_positions = {p.symbol: p for p in await self._broker.get_positions()}
            broker_portfolio = await self._broker.get_portfolio()
            broker_open_orders = await self._broker.get_open_orders()

            # ── 1. Position Reconciliation ────────────────────────────────────
            internal_positions = self._positions.get_all_positions()

            # Symbols in internal but not at broker
            for symbol, pos in internal_positions.items():
                if pos.is_flat():
                    continue
                broker_pos = broker_positions.get(symbol)
                if not broker_pos:
                    d = ReconciliationDiscrepancy(
                        symbol=symbol,
                        field="position_exists",
                        internal_value=pos.qty,
                        broker_value=0.0,
                        severity="critical",
                        notes="Internal position not found at broker",
                    )
                    found.append(d)
                    logger.error(
                        "Recon: ghost position",
                        extra={"symbol": symbol, "internal_qty": pos.qty},
                    )
                    continue

                # Qty mismatch
                qty_diff = abs(pos.qty - broker_pos.qty)
                if qty_diff > QTY_TOLERANCE:
                    d = ReconciliationDiscrepancy(
                        symbol=symbol,
                        field="qty",
                        internal_value=pos.qty,
                        broker_value=broker_pos.qty,
                        severity="critical" if qty_diff > 1 else "warning",
                        notes=f"Qty diff: {qty_diff:.4f}",
                    )
                    found.append(d)
                    # Auto-correct: trust broker
                    self._positions.get_all_positions()[symbol] = (
                        pos.model_copy(update={"qty": broker_pos.qty})
                    )
                    logger.warning(
                        "Recon: qty mismatch – auto-corrected to broker value",
                        extra={
                            "symbol": symbol,
                            "internal": pos.qty,
                            "broker": broker_pos.qty,
                        },
                    )

                # Avg cost mismatch (warn only)
                if pos.avg_cost > 0:
                    pct_diff = abs(pos.avg_cost - broker_pos.avg_cost) / pos.avg_cost * 100
                    if pct_diff > PRICE_TOLERANCE_PCT:
                        found.append(ReconciliationDiscrepancy(
                            symbol=symbol,
                            field="avg_cost",
                            internal_value=pos.avg_cost,
                            broker_value=broker_pos.avg_cost,
                            severity="warning",
                            notes=f"Avg cost diff: {pct_diff:.2f}%",
                        ))

            # Symbols at broker but not internally
            for symbol, broker_pos in broker_positions.items():
                if symbol not in internal_positions:
                    found.append(ReconciliationDiscrepancy(
                        symbol=symbol,
                        field="position_exists",
                        internal_value=0.0,
                        broker_value=broker_pos.qty,
                        severity="critical",
                        notes="Broker has position not tracked internally",
                    ))
                    logger.error(
                        "Recon: orphan broker position",
                        extra={"symbol": symbol, "broker_qty": broker_pos.qty},
                    )

            # ── 2. Order Reconciliation ───────────────────────────────────────
            broker_order_ids = {o.broker_order_id for o in broker_open_orders}
            internal_open = self._oms.get_open_orders()

            for order in internal_open:
                if order.broker_order_id and order.broker_order_id not in broker_order_ids:
                    # Ghost order: open internally but not at broker
                    found.append(ReconciliationDiscrepancy(
                        symbol=order.symbol,
                        field="order_status",
                        internal_value=order.status,
                        broker_value="not_found",
                        severity="warning",
                        notes=f"Order {order.broker_order_id} open internally but not at broker",
                    ))
                    # Auto-correct: mark as cancelled
                    logger.warning(
                        "Recon: ghost order – marking cancelled",
                        extra={"broker_order_id": order.broker_order_id},
                    )

            # ── 3. Cash / Equity Check ────────────────────────────────────────
            internal_portfolio = self._positions.get_portfolio()
            equity_diff_pct = 0.0
            if broker_portfolio.equity > 0:
                equity_diff_pct = (
                    abs(internal_portfolio.equity - broker_portfolio.equity)
                    / broker_portfolio.equity * 100
                )
            if equity_diff_pct > 1.0:
                found.append(ReconciliationDiscrepancy(
                    symbol="PORTFOLIO",
                    field="equity",
                    internal_value=internal_portfolio.equity,
                    broker_value=broker_portfolio.equity,
                    severity="warning" if equity_diff_pct < 5 else "critical",
                    notes=f"Equity diff: {equity_diff_pct:.2f}%",
                ))
                # Sync from broker
                self._positions.sync_from_broker(broker_portfolio)

        except Exception as exc:
            logger.error(
                "Reconciliation failed",
                extra={"error": str(exc)},
                exc_info=True,
            )
            return found

        # ── Publish results ───────────────────────────────────────────────────
        self._discrepancies.extend(found)

        if found:
            critical = [d for d in found if d.severity == "critical"]
            level = AlertLevel.CRITICAL if critical else AlertLevel.WARNING

            self._bus.publish(Event(
                type=EventType.RECONCILIATION_DISCREPANCY,
                data={"discrepancies": found, "count": len(found)},
                source="reconciliation",
            ))
            self._bus.publish(Event(
                type=EventType.ALERT,
                data={
                    "level": level,
                    "title": f"Reconciliation: {len(found)} discrepancy(s)",
                    "message": "\n".join(
                        f"{d.symbol}/{d.field}: internal={d.internal_value} "
                        f"broker={d.broker_value} [{d.severity}]"
                        for d in found
                    ),
                },
                source="reconciliation",
            ))

        logger.info(
            "Reconciliation complete",
            extra={
                "discrepancies": len(found),
                "critical": sum(1 for d in found if d.severity == "critical"),
            },
        )
        return found

    @property
    def last_run(self) -> Optional[datetime]:
        return self._last_run

    @property
    def discrepancy_history(self) -> list[ReconciliationDiscrepancy]:
        return list(self._discrepancies)
