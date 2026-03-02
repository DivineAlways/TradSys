"""
Rich terminal dashboard.

Displays a live-updating table with:
  - Portfolio summary (equity, cash, P&L, drawdown)
  - Open positions (symbol, qty, avg cost, current price, unrealised P&L)
  - Recent orders (last 10)
  - System status (all component health)
  - Kill switch status

Run standalone or embedded in main.py.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from core.events import Event, EventBus, EventType
from core.models import Order, Portfolio, Position
from oms.order_manager import OrderManager
from oms.position_tracker import PositionTracker
from risk.kill_switch import KillSwitch

logger = logging.getLogger(__name__)


class Dashboard:
    """
    Live Rich terminal dashboard.

    Usage:
        dash = Dashboard(bus, oms, positions, kill_switch)
        await dash.start()   # starts refresh loop in background
        ...
        await dash.stop()
    """

    def __init__(
        self,
        event_bus: EventBus,
        oms: OrderManager,
        position_tracker: PositionTracker,
        kill_switch: KillSwitch,
        refresh_seconds: float = 1.0,
    ) -> None:
        self._bus = event_bus
        self._oms = oms
        self._positions = position_tracker
        self._ks = kill_switch
        self._refresh = refresh_seconds
        self._console = Console()
        self._task: Optional[asyncio.Task] = None
        self._latest_portfolio: Optional[Portfolio] = None
        self._recent_orders: list[Order] = []
        self._system_errors: list[str] = []
        self._start_time = datetime.now(tz=timezone.utc)
        self._queue = event_bus.subscribe({
            EventType.PORTFOLIO_UPDATE,
            EventType.ORDER_FILLED,
            EventType.ORDER_CANCELLED,
            EventType.ORDER_REJECTED,
            EventType.KILL_SWITCH_TRIGGERED,
            EventType.SYSTEM_ERROR,
        })

    async def start(self) -> None:
        self._task = asyncio.create_task(
            self._run(), name="dashboard"
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        # Background event consumer
        event_task = asyncio.create_task(self._consume_events())
        try:
            with Live(
                self._render(),
                console=self._console,
                refresh_per_second=1 / self._refresh,
                screen=False,
            ) as live:
                while True:
                    await asyncio.sleep(self._refresh)
                    live.update(self._render())
        except asyncio.CancelledError:
            pass
        finally:
            event_task.cancel()

    async def _consume_events(self) -> None:
        while True:
            try:
                event: Event = await self._queue.get()
                if event.type == EventType.PORTFOLIO_UPDATE:
                    self._latest_portfolio = event.data
                elif event.type in (
                    EventType.ORDER_FILLED,
                    EventType.ORDER_CANCELLED,
                    EventType.ORDER_REJECTED,
                ):
                    # Refresh recent orders list
                    self._recent_orders = self._oms.get_all_orders()[-10:]
                elif event.type == EventType.SYSTEM_ERROR:
                    err = str(event.data.get("error", ""))[:80]
                    self._system_errors.append(err)
                    self._system_errors = self._system_errors[-5:]
            except asyncio.CancelledError:
                break

    def _render(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(self._header_panel(), size=3),
            Layout(name="body"),
            Layout(self._footer_panel(), size=4),
        )
        layout["body"].split_row(
            Layout(self._portfolio_panel(), name="left"),
            Layout(self._orders_panel(), name="right"),
        )
        return layout

    def _header_panel(self) -> Panel:
        mode = "[bold red]LIVE[/]" if not True else "[bold green]PAPER[/]"
        ks_status = (
            "[bold red]KILL SWITCH ACTIVE[/]"
            if self._ks.is_active
            else "[green]OK[/]"
        )
        now = datetime.now(tz=timezone.utc).strftime("%H:%M:%S UTC")
        title = Text.assemble(
            "TradSys  ", mode, f"  |  {now}  |  KillSwitch: ", ks_status
        )
        return Panel(title, style="blue")

    def _portfolio_panel(self) -> Panel:
        p = self._latest_portfolio or self._positions.get_portfolio()

        # Portfolio summary table
        summary = Table(show_header=False, box=None, padding=(0, 1))
        summary.add_column("Key", style="cyan")
        summary.add_column("Value", style="white")

        color = "green" if p.unrealised_pnl >= 0 else "red"
        summary.add_row("Equity", f"${p.equity:>12,.2f}")
        summary.add_row("Cash", f"${p.cash:>12,.2f}")
        summary.add_row("Unrealised P&L", f"[{color}]${p.unrealised_pnl:>+12,.2f}[/]")
        summary.add_row("Realised P&L", f"${p.realised_pnl_today:>+12,.2f}")
        dd_color = "red" if p.drawdown_pct > 2 else "yellow" if p.drawdown_pct > 1 else "green"
        summary.add_row("Drawdown", f"[{dd_color}]{p.drawdown_pct:>11.2f}%[/]")
        summary.add_row("Gross Exposure", f"${p.gross_exposure:>12,.2f}")

        # Positions table
        pos_table = Table(
            title="Positions",
            show_header=True,
            header_style="bold magenta",
            box=None,
        )
        pos_table.add_column("Symbol")
        pos_table.add_column("Qty", justify="right")
        pos_table.add_column("Avg Cost", justify="right")
        pos_table.add_column("Price", justify="right")
        pos_table.add_column("Unreal P&L", justify="right")
        pos_table.add_column("Side")

        for sym, pos in p.positions.items():
            pnl_color = "green" if pos.unrealised_pnl >= 0 else "red"
            pos_table.add_row(
                sym,
                f"{pos.qty:.2f}",
                f"${pos.avg_cost:.2f}",
                f"${pos.current_price:.2f}",
                f"[{pnl_color}]${pos.unrealised_pnl:+,.2f}[/]",
                f"[cyan]{pos.side}[/]",
            )

        from rich.columns import Columns
        body = Table.grid()
        body.add_row(summary)
        body.add_row(pos_table)
        return Panel(body, title="Portfolio", border_style="green")

    def _orders_panel(self) -> Panel:
        orders = self._oms.get_all_orders()[-10:]
        tbl = Table(
            show_header=True,
            header_style="bold cyan",
            box=None,
        )
        tbl.add_column("Symbol")
        tbl.add_column("Side")
        tbl.add_column("Qty", justify="right")
        tbl.add_column("Type")
        tbl.add_column("Status")
        tbl.add_column("Fill", justify="right")

        status_colors = {
            "filled": "green",
            "partial_fill": "yellow",
            "cancelled": "dim",
            "rejected": "red",
            "submitted": "cyan",
            "accepted": "blue",
            "new": "white",
        }

        for order in reversed(orders):
            color = status_colors.get(order.status.value, "white")
            tbl.add_row(
                order.symbol,
                f"[{'green' if order.is_buy else 'red'}]{order.side.value.upper()}[/]",
                f"{order.qty:.1f}",
                order.order_type.value,
                f"[{color}]{order.status.value}[/]",
                f"{order.filled_qty:.1f}@${order.avg_fill_price:.2f}"
                if order.filled_qty > 0 else "-",
            )

        return Panel(tbl, title="Recent Orders (last 10)", border_style="cyan")

    def _footer_panel(self) -> Panel:
        uptime = datetime.now(tz=timezone.utc) - self._start_time
        open_orders = self._oms.open_order_count()
        errors = self._system_errors[-1] if self._system_errors else "None"
        text = (
            f"Uptime: {str(uptime).split('.')[0]}  |  "
            f"Open Orders: {open_orders}  |  "
            f"Total Orders: {self._oms.order_count()}  |  "
            f"Last Error: {errors}"
        )
        return Panel(Text(text, style="dim"), style="grey37")
