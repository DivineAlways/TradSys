"""
Core Pydantic data models.  All fields are immutable after creation unless
explicitly updated through the OMS state machine.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from core.enums import (
    AssetClass, OrderSide, OrderStatus, OrderType,
    SignalDirection, TimeInForce,
)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _new_id() -> str:
    return str(uuid.uuid4())


# ─────────────────────────────────────────────────────────────────────────────
# Market Data
# ─────────────────────────────────────────────────────────────────────────────

class Quote(BaseModel):
    """Best bid/ask snapshot."""
    symbol: str
    bid: float
    ask: float
    bid_size: float = 0.0
    ask_size: float = 0.0
    timestamp: datetime = Field(default_factory=_utcnow)

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid


class Bar(BaseModel):
    """OHLCV bar."""
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float = 0.0
    timestamp: datetime = Field(default_factory=_utcnow)
    timeframe: str = "1Min"


# ─────────────────────────────────────────────────────────────────────────────
# Signal
# ─────────────────────────────────────────────────────────────────────────────

class Signal(BaseModel):
    """Trading signal emitted by a strategy."""
    id: str = Field(default_factory=_new_id)
    symbol: str
    direction: SignalDirection
    strength: float = Field(default=1.0, ge=0.0, le=1.0)  # 0–1 confidence
    target_qty: float = 0.0          # desired quantity; 0 = strategy decides
    price_hint: Optional[float] = None  # optional limit price hint
    strategy_id: str = "default"
    metadata: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=_utcnow)


# ─────────────────────────────────────────────────────────────────────────────
# Orders & Fills
# ─────────────────────────────────────────────────────────────────────────────

class Fill(BaseModel):
    """A single execution / partial fill."""
    id: str = Field(default_factory=_new_id)
    order_id: str
    symbol: str
    side: OrderSide
    qty: float
    price: float
    commission: float = 0.0
    timestamp: datetime = Field(default_factory=_utcnow)
    broker_exec_id: str = ""

    @property
    def gross_value(self) -> float:
        return self.qty * self.price

    @property
    def net_value(self) -> float:
        return self.gross_value + self.commission


class Order(BaseModel):
    """Full lifecycle of a single order."""
    # Identity
    id: str = Field(default_factory=_new_id)
    broker_order_id: Optional[str] = None   # assigned after submission
    client_order_id: str = Field(default_factory=_new_id)

    # Instrument
    symbol: str
    asset_class: AssetClass = AssetClass.US_EQUITY

    # Order parameters
    side: OrderSide
    order_type: OrderType = OrderType.MARKET
    qty: float
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    time_in_force: TimeInForce = TimeInForce.DAY
    extended_hours: bool = False

    # State machine
    status: OrderStatus = OrderStatus.NEW

    # Execution tracking
    filled_qty: float = 0.0
    avg_fill_price: float = 0.0
    fills: list[Fill] = Field(default_factory=list)

    # Source
    signal_id: Optional[str] = None
    strategy_id: str = "default"

    # Timestamps
    created_at: datetime = Field(default_factory=_utcnow)
    submitted_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None

    # Rejection / cancel reason
    reject_reason: str = ""
    cancel_reason: str = ""

    @property
    def remaining_qty(self) -> float:
        return self.qty - self.filled_qty

    @property
    def is_terminal(self) -> bool:
        from core.enums import TERMINAL_STATUSES
        return self.status in TERMINAL_STATUSES

    @property
    def is_buy(self) -> bool:
        return self.side == OrderSide.BUY

    @field_validator("qty")
    @classmethod
    def qty_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("Order qty must be positive")
        return v

    @model_validator(mode="after")
    def limit_price_required_for_limit(self) -> "Order":
        if self.order_type == OrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit_price required for LIMIT orders")
        return self


# ─────────────────────────────────────────────────────────────────────────────
# Position & Portfolio
# ─────────────────────────────────────────────────────────────────────────────

class Position(BaseModel):
    """Real-time position for one symbol."""
    symbol: str
    qty: float = 0.0              # positive = long, negative = short
    avg_cost: float = 0.0         # average cost basis
    current_price: float = 0.0
    asset_class: AssetClass = AssetClass.US_EQUITY
    strategy_id: str = "default"
    opened_at: datetime = Field(default_factory=_utcnow)
    last_updated: datetime = Field(default_factory=_utcnow)

    @property
    def market_value(self) -> float:
        return self.qty * self.current_price

    @property
    def cost_basis(self) -> float:
        return abs(self.qty) * self.avg_cost

    @property
    def unrealised_pnl(self) -> float:
        return (self.current_price - self.avg_cost) * self.qty

    @property
    def unrealised_pnl_pct(self) -> float:
        if self.cost_basis == 0:
            return 0.0
        return self.unrealised_pnl / self.cost_basis * 100.0

    @property
    def side(self) -> str:
        if self.qty > 0:
            return "long"
        if self.qty < 0:
            return "short"
        return "flat"

    def is_flat(self) -> bool:
        return abs(self.qty) < 1e-9


class Portfolio(BaseModel):
    """Aggregate portfolio snapshot."""
    cash: float = 0.0
    equity: float = 0.0                  # cash + market value of all positions
    buying_power: float = 0.0
    positions: dict[str, Position] = Field(default_factory=dict)
    realised_pnl_today: float = 0.0
    unrealised_pnl: float = 0.0
    peak_equity: float = 0.0
    timestamp: datetime = Field(default_factory=_utcnow)

    @property
    def drawdown_pct(self) -> float:
        if self.peak_equity <= 0:
            return 0.0
        return (self.peak_equity - self.equity) / self.peak_equity * 100.0

    @property
    def gross_exposure(self) -> float:
        return sum(abs(p.market_value) for p in self.positions.values())

    @property
    def net_exposure(self) -> float:
        return sum(p.market_value for p in self.positions.values())


# ─────────────────────────────────────────────────────────────────────────────
# Reconciliation
# ─────────────────────────────────────────────────────────────────────────────

class ReconciliationDiscrepancy(BaseModel):
    """A mismatch found during reconciliation."""
    symbol: str
    field: str                   # "qty", "avg_cost", "status", etc.
    internal_value: Any
    broker_value: Any
    severity: str = "warning"   # "info" | "warning" | "critical"
    detected_at: datetime = Field(default_factory=_utcnow)
    resolved: bool = False
    notes: str = ""
