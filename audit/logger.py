"""
Audit Logger: immutable, append-only event journal.

Persists every trading decision, order lifecycle event, fill, and
system event to SQLite for post-trade analysis and compliance.

Schema:
  audit_events(id, ts, event_type, source, data_json)
  orders(id, broker_order_id, symbol, side, qty, status, fill_qty,
         avg_price, created_at, updated_at, signal_id, strategy_id)
  fills(id, order_id, symbol, side, qty, price, commission, broker_exec_id, ts)
  signals(id, symbol, direction, strength, strategy_id, ts, metadata_json)
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiosqlite

from core.enums import AlertLevel
from core.events import Event, EventBus, EventType
from core.models import Fill, Order, Signal

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS audit_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    source      TEXT,
    data_json   TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    id              TEXT PRIMARY KEY,
    broker_order_id TEXT,
    symbol          TEXT NOT NULL,
    side            TEXT NOT NULL,
    order_type      TEXT NOT NULL,
    qty             REAL NOT NULL,
    limit_price     REAL,
    stop_price      REAL,
    status          TEXT NOT NULL,
    filled_qty      REAL DEFAULT 0,
    avg_fill_price  REAL DEFAULT 0,
    signal_id       TEXT,
    strategy_id     TEXT,
    reject_reason   TEXT,
    created_at      TEXT,
    submitted_at    TEXT,
    filled_at       TEXT,
    cancelled_at    TEXT,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fills (
    id              TEXT PRIMARY KEY,
    order_id        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    side            TEXT NOT NULL,
    qty             REAL NOT NULL,
    price           REAL NOT NULL,
    commission      REAL DEFAULT 0,
    broker_exec_id  TEXT,
    ts              TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
    id          TEXT PRIMARY KEY,
    symbol      TEXT NOT NULL,
    direction   TEXT NOT NULL,
    strength    REAL,
    strategy_id TEXT,
    ts          TEXT NOT NULL,
    metadata    TEXT
);

CREATE INDEX IF NOT EXISTS idx_orders_symbol    ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_status    ON orders(status);
CREATE INDEX IF NOT EXISTS idx_fills_order_id   ON fills(order_id);
CREATE INDEX IF NOT EXISTS idx_signals_symbol   ON signals(symbol);
CREATE INDEX IF NOT EXISTS idx_events_type      ON audit_events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_ts        ON audit_events(ts);
"""


class AuditLogger:
    """
    Async SQLite-backed audit trail.

    Listens to all relevant EventBus events and writes them to SQLite.
    All writes are batched through an asyncio.Queue for performance.
    """

    def __init__(self, event_bus: EventBus, db_path: str = "data/audit.db") -> None:
        self._bus = event_bus
        self._db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None
        self._task: Optional[asyncio.Task] = None
        self._write_queue: asyncio.Queue = asyncio.Queue(maxsize=50_000)
        self._queue = event_bus.subscribe({
            EventType.SIGNAL,
            EventType.ORDER_CREATED,
            EventType.ORDER_SUBMITTED,
            EventType.ORDER_ACCEPTED,
            EventType.ORDER_FILLED,
            EventType.ORDER_PARTIAL_FILL,
            EventType.ORDER_CANCELLED,
            EventType.ORDER_REJECTED,
            EventType.KILL_SWITCH_TRIGGERED,
            EventType.DRAWDOWN_BREACH,
            EventType.RECONCILIATION_DISCREPANCY,
            EventType.ALERT,
            EventType.SYSTEM_ERROR,
        })

    async def start(self) -> None:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(_SCHEMA)
        await self._db.commit()
        self._task = asyncio.create_task(
            self._event_loop(), name="audit_logger"
        )
        logger.info("AuditLogger started", extra={"db": self._db_path})

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._db:
            await self._db.close()
        logger.info("AuditLogger stopped")

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
                    "AuditLogger error",
                    extra={"error": str(exc)},
                    exc_info=True,
                )

    async def _handle_event(self, event: Event) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()

        # Always write to raw audit_events table
        await self._insert_audit_event(now, event)

        # Type-specific structured writes
        if event.type == EventType.SIGNAL:
            signal: Signal = event.data
            await self._upsert_signal(signal, now)

        elif event.type in {
            EventType.ORDER_CREATED, EventType.ORDER_SUBMITTED,
            EventType.ORDER_ACCEPTED, EventType.ORDER_FILLED,
            EventType.ORDER_PARTIAL_FILL, EventType.ORDER_CANCELLED,
            EventType.ORDER_REJECTED,
        }:
            await self._upsert_order_from_event(event, now)

        await self._db.commit()

    # ── DB Writers ────────────────────────────────────────────────────────────

    async def _insert_audit_event(self, now: str, event: Event) -> None:
        try:
            data_str = json.dumps(
                event.data if isinstance(event.data, dict)
                else (event.data.model_dump() if hasattr(event.data, "model_dump") else str(event.data)),
                default=str,
            )
        except Exception:
            data_str = str(event.data)

        await self._db.execute(
            "INSERT INTO audit_events(ts, event_type, source, data_json) VALUES(?,?,?,?)",
            (now, event.type.value, event.source, data_str),
        )

    async def _upsert_signal(self, signal: Signal, now: str) -> None:
        await self._db.execute(
            """INSERT OR REPLACE INTO signals
               (id, symbol, direction, strength, strategy_id, ts, metadata)
               VALUES(?,?,?,?,?,?,?)""",
            (
                signal.id, signal.symbol, signal.direction.value,
                signal.strength, signal.strategy_id, now,
                json.dumps(signal.metadata, default=str),
            ),
        )

    async def _upsert_order_from_event(self, event: Event, now: str) -> None:
        order: Optional[Order] = None
        fill: Optional[Fill] = None

        if isinstance(event.data, Order):
            order = event.data
        elif isinstance(event.data, dict):
            order = event.data.get("order") or event.data.get("submitted")
            fill = event.data.get("fill")

        if order:
            await self._db.execute(
                """INSERT OR REPLACE INTO orders
                   (id, broker_order_id, symbol, side, order_type, qty,
                    limit_price, stop_price, status, filled_qty, avg_fill_price,
                    signal_id, strategy_id, reject_reason,
                    created_at, submitted_at, filled_at, cancelled_at, updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    order.id, order.broker_order_id, order.symbol,
                    order.side.value, order.order_type.value, order.qty,
                    order.limit_price, order.stop_price, order.status.value,
                    order.filled_qty, order.avg_fill_price,
                    order.signal_id, order.strategy_id, order.reject_reason,
                    _dt(order.created_at), _dt(order.submitted_at),
                    _dt(order.filled_at), _dt(order.cancelled_at), now,
                ),
            )

        if fill:
            await self._db.execute(
                """INSERT OR IGNORE INTO fills
                   (id, order_id, symbol, side, qty, price, commission,
                    broker_exec_id, ts)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    fill.id, fill.order_id, fill.symbol, fill.side.value,
                    fill.qty, fill.price, fill.commission,
                    fill.broker_exec_id, now,
                ),
            )

    # ── Query API (for reporting / dashboard) ─────────────────────────────────

    async def get_fills_today(self) -> list[dict]:
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        async with self._db.execute(
            "SELECT * FROM fills WHERE ts >= ? ORDER BY ts DESC",
            (today,),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_orders(
        self,
        symbol: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        sql = "SELECT * FROM orders WHERE 1=1"
        params: list[Any] = []
        if symbol:
            sql += " AND symbol=?"
            params.append(symbol)
        if status:
            sql += " AND status=?"
            params.append(status)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        async with self._db.execute(sql, params) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]


def _dt(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None
