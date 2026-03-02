"""
Alpaca broker adapter.

Uses alpaca-py SDK for REST calls and websocket order updates.
Paper trading is the default (TRADING_MODE=paper in .env).

Free tier:
  - Paper trading: unlimited, no approval needed
  - Live trading: free brokerage account at alpaca.markets
  - Market data: IEX feed (free, 15-min delayed for stocks; real-time in paper)
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from core.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from core.events import Event, EventBus, EventType
from core.exceptions import BrokerConnectionError, BrokerOrderError
from core.models import Fill, Order, Portfolio, Position

logger = logging.getLogger(__name__)


class AlpacaBroker:
    """
    Alpaca broker adapter wrapping alpaca-py.
    Implements BrokerProtocol structurally.
    """

    def __init__(self, event_bus: EventBus, api_key: str, secret_key: str,
                 paper: bool = True) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._paper = paper
        self._bus = event_bus
        self._connected = False
        self._trading_client = None
        self._data_client = None
        self._stream = None
        self._stream_task: Optional[asyncio.Task] = None

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def is_paper(self) -> bool:
        return self._paper

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Initialise alpaca-py clients."""
        try:
            from alpaca.trading.client import TradingClient
            from alpaca.data.historical import StockHistoricalDataClient
            from alpaca.trading.stream import TradingStream

            self._trading_client = TradingClient(
                api_key=self._api_key,
                secret_key=self._secret_key,
                paper=self._paper,
            )
            self._data_client = StockHistoricalDataClient(
                api_key=self._api_key,
                secret_key=self._secret_key,
            )
            self._stream = TradingStream(
                api_key=self._api_key,
                secret_key=self._secret_key,
                paper=self._paper,
            )
            self._connected = True
            logger.info(
                "Alpaca broker connected",
                extra={"paper": self._paper},
            )
        except Exception as exc:
            raise BrokerConnectionError(f"Alpaca connect failed: {exc}") from exc

    async def disconnect(self) -> None:
        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass
        self._connected = False
        logger.info("Alpaca broker disconnected")

    # ── Order Management ─────────────────────────────────────────────────────

    async def place_order(self, order: Order) -> Order:
        """Place order via Alpaca REST API."""
        from alpaca.trading.requests import (
            MarketOrderRequest, LimitOrderRequest,
            StopOrderRequest, StopLimitOrderRequest,
        )
        from alpaca.trading.enums import (
            OrderSide as AlpacaSide,
            TimeInForce as AlpacaTIF,
            OrderType as AlpacaType,
        )

        side = AlpacaSide.BUY if order.side == OrderSide.BUY else AlpacaSide.SELL
        tif_map = {
            TimeInForce.DAY: AlpacaTIF.DAY,
            TimeInForce.GTC: AlpacaTIF.GTC,
            TimeInForce.IOC: AlpacaTIF.IOC,
            TimeInForce.FOK: AlpacaTIF.FOK,
            TimeInForce.OPG: AlpacaTIF.OPG,
            TimeInForce.CLS: AlpacaTIF.CLS,
        }
        tif = tif_map.get(order.time_in_force, AlpacaTIF.DAY)

        try:
            if order.order_type == OrderType.MARKET:
                req = MarketOrderRequest(
                    symbol=order.symbol,
                    qty=order.qty,
                    side=side,
                    time_in_force=tif,
                )
            elif order.order_type == OrderType.LIMIT:
                req = LimitOrderRequest(
                    symbol=order.symbol,
                    qty=order.qty,
                    side=side,
                    time_in_force=tif,
                    limit_price=order.limit_price,
                )
            elif order.order_type == OrderType.STOP:
                req = StopOrderRequest(
                    symbol=order.symbol,
                    qty=order.qty,
                    side=side,
                    time_in_force=tif,
                    stop_price=order.stop_price,
                )
            elif order.order_type == OrderType.STOP_LIMIT:
                req = StopLimitOrderRequest(
                    symbol=order.symbol,
                    qty=order.qty,
                    side=side,
                    time_in_force=tif,
                    limit_price=order.limit_price,
                    stop_price=order.stop_price,
                )
            else:
                raise BrokerOrderError(
                    f"Unsupported order type: {order.order_type}",
                    order_id=order.id,
                )

            # Run sync alpaca-py call in executor to keep async loop free
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(
                None, self._trading_client.submit_order, req
            )

            order = order.model_copy(update={
                "broker_order_id": str(resp.id),
                "status": OrderStatus.SUBMITTED,
                "submitted_at": datetime.now(tz=timezone.utc),
            })
            logger.info(
                "Order submitted to Alpaca",
                extra={
                    "order_id": order.id,
                    "broker_order_id": order.broker_order_id,
                    "symbol": order.symbol,
                    "side": order.side,
                    "qty": order.qty,
                },
            )
            return order

        except BrokerOrderError:
            raise
        except Exception as exc:
            raise BrokerOrderError(
                f"Alpaca order submission failed: {exc}",
                order_id=order.id,
            ) from exc

    async def cancel_order(self, order: Order) -> Order:
        if not order.broker_order_id:
            raise BrokerOrderError(
                "No broker_order_id to cancel", order_id=order.id
            )
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                self._trading_client.cancel_order_by_id,
                order.broker_order_id,
            )
            logger.info(
                "Cancel request sent",
                extra={"broker_order_id": order.broker_order_id},
            )
            return order.model_copy(update={"status": OrderStatus.PENDING_CANCEL})
        except Exception as exc:
            raise BrokerOrderError(
                f"Cancel failed: {exc}", order_id=order.id
            ) from exc

    async def cancel_all_orders(self) -> list[Order]:
        loop = asyncio.get_running_loop()
        try:
            cancel_statuses = await loop.run_in_executor(
                None, self._trading_client.cancel_orders
            )
            logger.warning(
                "All orders cancel requested",
                extra={"count": len(cancel_statuses)},
            )
            return []  # OMS will reconcile actual status via WS stream
        except Exception as exc:
            raise BrokerOrderError(f"cancel_all_orders failed: {exc}") from exc

    async def get_order(self, broker_order_id: str) -> Order:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            None, self._trading_client.get_order_by_id, broker_order_id
        )
        return self._alpaca_order_to_order(resp)

    async def get_open_orders(self) -> list[Order]:
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus

        loop = asyncio.get_running_loop()
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN)
        orders = await loop.run_in_executor(
            None, lambda: self._trading_client.get_orders(req)
        )
        return [self._alpaca_order_to_order(o) for o in orders]

    async def get_positions(self) -> list[Position]:
        loop = asyncio.get_running_loop()
        positions = await loop.run_in_executor(
            None, self._trading_client.get_all_positions
        )
        return [self._alpaca_position_to_position(p) for p in positions]

    async def get_portfolio(self) -> Portfolio:
        loop = asyncio.get_running_loop()
        acct = await loop.run_in_executor(
            None, self._trading_client.get_account
        )
        positions = await self.get_positions()
        return Portfolio(
            cash=float(acct.cash),
            equity=float(acct.equity),
            buying_power=float(acct.buying_power),
            positions={p.symbol: p for p in positions},
        )

    async def close_position(self, symbol: str) -> Order:
        loop = asyncio.get_running_loop()
        try:
            resp = await loop.run_in_executor(
                None,
                lambda: self._trading_client.close_position(symbol),
            )
            logger.warning("Position closed", extra={"symbol": symbol})
            return self._alpaca_order_to_order(resp)
        except Exception as exc:
            raise BrokerOrderError(
                f"close_position({symbol}) failed: {exc}"
            ) from exc

    async def close_all_positions(self) -> list[Order]:
        loop = asyncio.get_running_loop()
        try:
            responses = await loop.run_in_executor(
                None,
                lambda: self._trading_client.close_all_positions(
                    cancel_orders=True
                ),
            )
            logger.warning(
                "All positions close requested",
                extra={"count": len(responses)},
            )
            return []
        except Exception as exc:
            raise BrokerOrderError(f"close_all_positions failed: {exc}") from exc

    # ── Order Update Stream ───────────────────────────────────────────────────

    async def subscribe_order_updates(self) -> None:
        """Start the WS trade update stream in a background task."""
        self._stream.subscribe_trade_updates(self._handle_trade_update)
        self._stream_task = asyncio.create_task(
            self._run_stream(), name="alpaca_order_stream"
        )
        logger.info("Alpaca order update stream started")

    async def _run_stream(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._stream.run)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error(
                "Order stream error",
                extra={"error": str(exc)},
                exc_info=True,
            )
            self._bus.publish(Event(
                type=EventType.SYSTEM_ERROR,
                data={"component": "alpaca_order_stream", "error": str(exc)},
                source="alpaca_broker",
            ))

    async def _handle_trade_update(self, data) -> None:
        """Convert Alpaca trade update to internal events."""
        try:
            event_type = str(data.event).lower()
            order_data = data.order

            status_map = {
                "fill": OrderStatus.FILLED,
                "partial_fill": OrderStatus.PARTIAL_FILL,
                "canceled": OrderStatus.CANCELLED,
                "cancelled": OrderStatus.CANCELLED,
                "rejected": OrderStatus.REJECTED,
                "expired": OrderStatus.EXPIRED,
                "new": OrderStatus.ACCEPTED,
                "accepted": OrderStatus.ACCEPTED,
                "pending_cancel": OrderStatus.PENDING_CANCEL,
            }
            status = status_map.get(event_type, OrderStatus.ACCEPTED)

            update_payload = {
                "broker_order_id": str(order_data.id),
                "status": status,
                "filled_qty": float(order_data.filled_qty or 0),
                "avg_fill_price": float(order_data.filled_avg_price or 0),
            }

            if event_type in ("fill", "partial_fill") and data.price:
                fill = Fill(
                    order_id=str(order_data.id),
                    symbol=order_data.symbol,
                    side=OrderSide.BUY if order_data.side.value == "buy" else OrderSide.SELL,
                    qty=float(data.qty or 0),
                    price=float(data.price),
                    broker_exec_id=str(getattr(data, "execution_id", "")),
                )
                bus_event_type = (
                    EventType.ORDER_FILLED
                    if event_type == "fill"
                    else EventType.ORDER_PARTIAL_FILL
                )
                self._bus.publish(Event(
                    type=bus_event_type,
                    data={"update": update_payload, "fill": fill},
                    source="alpaca_broker",
                ))
            elif event_type in ("canceled", "cancelled"):
                self._bus.publish(Event(
                    type=EventType.ORDER_CANCELLED,
                    data={"update": update_payload},
                    source="alpaca_broker",
                ))
            elif event_type == "rejected":
                self._bus.publish(Event(
                    type=EventType.ORDER_REJECTED,
                    data={
                        "update": update_payload,
                        "reason": str(getattr(order_data, "reason", "")),
                    },
                    source="alpaca_broker",
                ))
            else:
                self._bus.publish(Event(
                    type=EventType.ORDER_ACCEPTED,
                    data={"update": update_payload},
                    source="alpaca_broker",
                ))

        except Exception as exc:
            logger.error(
                "Error processing trade update",
                extra={"error": str(exc)},
                exc_info=True,
            )

    # ── Conversion Helpers ────────────────────────────────────────────────────

    def _alpaca_order_to_order(self, resp) -> Order:
        from alpaca.trading.enums import OrderSide as AlpacaSide

        status_map = {
            "new": OrderStatus.ACCEPTED,
            "accepted": OrderStatus.ACCEPTED,
            "partially_filled": OrderStatus.PARTIAL_FILL,
            "filled": OrderStatus.FILLED,
            "canceled": OrderStatus.CANCELLED,
            "cancelled": OrderStatus.CANCELLED,
            "rejected": OrderStatus.REJECTED,
            "expired": OrderStatus.EXPIRED,
            "pending_cancel": OrderStatus.PENDING_CANCEL,
        }
        side = (
            OrderSide.BUY
            if resp.side == AlpacaSide.BUY
            else OrderSide.SELL
        )
        return Order(
            broker_order_id=str(resp.id),
            client_order_id=str(resp.client_order_id),
            symbol=resp.symbol,
            side=side,
            qty=float(resp.qty or 0),
            filled_qty=float(resp.filled_qty or 0),
            avg_fill_price=float(resp.filled_avg_price or 0),
            status=status_map.get(str(resp.status), OrderStatus.ACCEPTED),
            order_type=OrderType.MARKET,  # simplified
        )

    def _alpaca_position_to_position(self, resp) -> Position:
        return Position(
            symbol=resp.symbol,
            qty=float(resp.qty),
            avg_cost=float(resp.avg_entry_price),
            current_price=float(resp.current_price),
        )
