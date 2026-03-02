"""
TradSys – Live Algorithmic Trading System
==========================================

Entry point.  Boots all components in dependency order, connects to broker,
starts market data feed, runs signal generator and execution engine.

Usage:
    # Paper trading (safe default):
    python main.py

    # Live trading (requires TRADING_MODE=live in .env):
    python main.py --live

    # Emergency kill (from separate terminal):
    python main.py --kill

    # Run reconciliation now:
    python main.py --recon

Architecture:
    MarketData Feed
         │ BAR / QUOTE events
         ▼
    SignalGenerator  ──► EventBus ──► ExecutionEngine
                                           │ Order
                                           ▼
                                      OrderManager ──► Broker (Alpaca)
                                           │                │
                                           │        WS Order Updates
                                           ▼                │
                                    PositionTracker ◄────────
                                           │
                                    KillSwitch (monitors drawdown)
                                    ReconciliationEngine (every 5 min)
                                    AuditLogger (all events → SQLite)
                                    Alerter (Telegram / Email)
                                    Dashboard (Rich terminal)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

# ── Install uvloop for faster async I/O (Linux/macOS) ─────────────────────────
try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

from config.logging_config import setup_logging
from config.settings import get_settings


async def main() -> None:
    """Boot and run the trading system."""
    settings = get_settings()
    setup_logging(level=settings.log_level, log_dir=settings.log_dir)

    logger = logging.getLogger("tradsys.main")
    logger.info(
        "TradSys starting",
        extra={
            "mode": settings.trading_mode,
            "symbols": settings.symbols,
        },
    )

    if settings.trading_mode == "live":
        logger.warning("=" * 60)
        logger.warning("LIVE TRADING MODE – REAL MONEY AT RISK")
        logger.warning("=" * 60)
        await asyncio.sleep(3)  # pause to allow cancellation

    # ── Validate configuration ────────────────────────────────────────────────
    _validate_config(settings)

    # ── Build component graph ─────────────────────────────────────────────────
    from core.events import EventBus
    from brokers.alpaca_broker import AlpacaBroker
    from market_data.feed_manager import FeedManager
    from oms.order_manager import OrderManager
    from oms.position_tracker import PositionTracker
    from execution.engine import ExecutionEngine
    from signals.generator import MomentumStrategy, SignalGenerator
    from risk.kill_switch import KillSwitch
    from reconciliation.engine import ReconciliationEngine
    from audit.logger import AuditLogger
    from alerts.alerter import Alerter
    from monitoring.dashboard import Dashboard

    bus = EventBus()

    broker = AlpacaBroker(
        event_bus=bus,
        api_key=settings.alpaca_key,
        secret_key=settings.alpaca_secret,
        paper=(settings.trading_mode == "paper"),
    )

    oms = OrderManager(broker=broker, event_bus=bus)
    position_tracker = PositionTracker(event_bus=bus)
    feed_manager = FeedManager(
        event_bus=bus,
        api_key=settings.alpaca_key,
        secret_key=settings.alpaca_secret,
        symbols=settings.symbols,
    )
    signal_gen = SignalGenerator(event_bus=bus)
    signal_gen.add_strategy(MomentumStrategy(symbols=settings.symbols))

    execution_engine = ExecutionEngine(
        event_bus=bus,
        oms=oms,
        position_tracker=position_tracker,
        feed_manager=feed_manager,
    )
    kill_switch = KillSwitch(broker=broker, event_bus=bus)
    recon_engine = ReconciliationEngine(
        broker=broker,
        oms=oms,
        position_tracker=position_tracker,
        event_bus=bus,
        interval_seconds=300,
    )
    audit_logger = AuditLogger(event_bus=bus, db_path=settings.audit_db_path)
    alerter = Alerter(event_bus=bus)
    dashboard = Dashboard(
        event_bus=bus,
        oms=oms,
        position_tracker=position_tracker,
        kill_switch=kill_switch,
    )

    # ── Start all components ──────────────────────────────────────────────────
    logger.info("Starting components...")

    await audit_logger.start()
    await alerter.start()
    await broker.connect()
    await broker.subscribe_order_updates()
    await oms.start()
    await kill_switch.start()

    # Sync portfolio state from broker
    try:
        portfolio = await broker.get_portfolio()
        position_tracker.sync_from_broker(portfolio)
        logger.info(
            "Portfolio synced",
            extra={"equity": portfolio.equity, "cash": portfolio.cash},
        )
    except Exception as exc:
        logger.warning("Could not sync portfolio from broker", extra={"error": str(exc)})

    await position_tracker.start()
    await feed_manager.start()

    # Wait for initial market data
    logger.info("Waiting for market data...")
    got_data = await feed_manager.wait_for_data(settings.symbols, timeout=30)
    if not got_data:
        logger.warning("Timeout waiting for market data – proceeding anyway")

    await signal_gen.start()
    await execution_engine.start()
    await recon_engine.start()
    await dashboard.start()

    logger.info("All components started. TradSys is running.")

    # ── Register shutdown handlers ────────────────────────────────────────────
    shutdown_event = asyncio.Event()

    def _request_shutdown(sig_name: str) -> None:
        logger.info("Shutdown signal received", extra={"signal": sig_name})
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig.name: _request_shutdown(s))

    # ── Wait for shutdown ─────────────────────────────────────────────────────
    await shutdown_event.wait()
    logger.info("Shutting down TradSys...")

    # ── Graceful shutdown sequence ────────────────────────────────────────────
    await dashboard.stop()
    await execution_engine.stop()
    await signal_gen.stop()
    await recon_engine.stop()
    await feed_manager.stop()
    await oms.stop()
    await kill_switch.stop()
    await position_tracker.stop()
    await broker.disconnect()
    await alerter.stop()
    await audit_logger.stop()

    logger.info("TradSys shutdown complete")


def _validate_config(settings) -> None:
    """Fail fast on missing critical config."""
    from core.exceptions import ConfigurationError

    if not settings.alpaca_key or not settings.alpaca_secret:
        raise ConfigurationError(
            "ALPACA_API_KEY and ALPACA_SECRET_KEY must be set in .env\n"
            "Get free paper trading keys at: https://app.alpaca.markets"
        )

    if settings.trading_mode == "live":
        if not settings.alpaca_live_api_key or not settings.alpaca_live_secret_key:
            raise ConfigurationError(
                "ALPACA_LIVE_API_KEY and ALPACA_LIVE_SECRET_KEY required for live mode"
            )


# ── CLI ────────────────────────────────────────────────────────────────────────

def cli() -> None:
    parser = argparse.ArgumentParser(description="TradSys – Algorithmic Trading System")
    parser.add_argument("--live", action="store_true", help="Override mode to live trading")
    parser.add_argument("--kill", action="store_true", help="Trigger kill switch via API")
    parser.add_argument("--recon", action="store_true", help="Run reconciliation and exit")
    parser.add_argument("--status", action="store_true", help="Print system status and exit")
    args = parser.parse_args()

    if args.live:
        os.environ["TRADING_MODE"] = "live"
        print("WARNING: LIVE mode enabled. Real money at risk.")

    if args.kill:
        print("Kill switch trigger not yet implemented via CLI. "
              "Use Ctrl+C to stop the system, which will run graceful shutdown.")
        sys.exit(0)

    asyncio.run(main())


if __name__ == "__main__":
    cli()
