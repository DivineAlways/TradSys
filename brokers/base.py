"""
Broker protocol / interface.
All broker implementations must satisfy this Protocol so the rest of the
system can swap between Alpaca, IB, etc. without changing upstream code.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from core.models import Fill, Order, Portfolio, Position


@runtime_checkable
class BrokerProtocol(Protocol):
    """Structural protocol defining the broker adapter interface."""

    async def connect(self) -> None:
        """Establish connection / authenticate."""
        ...

    async def disconnect(self) -> None:
        """Gracefully disconnect."""
        ...

    async def place_order(self, order: Order) -> Order:
        """
        Submit order to broker.
        Returns the Order with broker_order_id populated and status=SUBMITTED.
        """
        ...

    async def cancel_order(self, order: Order) -> Order:
        """Request cancellation of an open order."""
        ...

    async def cancel_all_orders(self) -> list[Order]:
        """Cancel all open orders.  Used by kill switch."""
        ...

    async def get_order(self, broker_order_id: str) -> Order:
        """Fetch current order state from broker."""
        ...

    async def get_open_orders(self) -> list[Order]:
        """Return all open orders from broker."""
        ...

    async def get_positions(self) -> list[Position]:
        """Return current positions from broker."""
        ...

    async def get_portfolio(self) -> Portfolio:
        """Return account / portfolio snapshot."""
        ...

    async def close_position(self, symbol: str) -> Order:
        """Market-close an entire position. Used by kill switch."""
        ...

    async def close_all_positions(self) -> list[Order]:
        """Market-close every open position. Used by kill switch."""
        ...

    async def subscribe_order_updates(self) -> None:
        """
        Start listening to broker order update stream.
        Implementations should publish fill/status events to the EventBus.
        """
        ...

    @property
    def is_connected(self) -> bool:
        ...

    @property
    def is_paper(self) -> bool:
        ...
