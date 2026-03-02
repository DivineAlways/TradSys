<div align="center">

```
████████╗██████╗  █████╗ ██████╗ ███████╗██╗   ██╗███████╗
╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗██╔════╝╚██╗ ██╔╝██╔════╝
   ██║   ██████╔╝███████║██║  ██║███████╗ ╚████╔╝ ███████╗
   ██║   ██╔══██╗██╔══██║██║  ██║╚════██║  ╚██╔╝  ╚════██║
   ██║   ██║  ██║██║  ██║██████╔╝███████║   ██║   ███████║
   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ ╚══════╝   ╚═╝   ╚══════╝
```

### Institutional-Grade Live Algorithmic Trading System

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Alpaca](https://img.shields.io/badge/Broker-Alpaca-FFCD00?style=for-the-badge&logo=alpaca&logoColor=black)](https://alpaca.markets)
[![AsyncIO](https://img.shields.io/badge/Async-AsyncIO%20%2B%20uvloop-00C7B7?style=for-the-badge&logo=python&logoColor=white)](https://docs.python.org/3/library/asyncio.html)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)
[![Paper Trading](https://img.shields.io/badge/Default%20Mode-Paper%20Trading-blue?style=for-the-badge)](https://app.alpaca.markets)

*Market data → Strategy signal → Risk check → Order → Fill → Alert. All in under 500ms.*

</div>

---

## What Is This?

TradSys is a **production-ready, fully automated trading system** built the way institutional desks build them — modular, async, event-driven, and obsessively safe by default.

You run it, it connects to the market, watches prices in real time, runs your strategy, places orders, tracks every position and dollar of P&L, protects your account with a kill switch, reconciles its own records against the broker, and sends you Telegram alerts for every fill — all while writing an immutable audit trail to SQLite.

**Paper trading is the default.** You need to explicitly opt into live trading. Your keys and data never leave your machine.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│   Alpaca WebSocket (IEX free feed)                               │
│         │                                                        │
│         ▼                                                        │
│   ┌─────────────┐    BAR/QUOTE     ┌──────────────────┐         │
│   │ FeedManager │ ──────────────► │ SignalGenerator   │         │
│   └─────────────┘                 │ (your strategy)   │         │
│                                   └────────┬─────────┘         │
│                                            │ SIGNAL             │
│                                            ▼                    │
│                                   ┌──────────────────┐          │
│                                   │ ExecutionEngine  │          │
│                                   │ sizing + risk    │          │
│                                   └────────┬─────────┘          │
│                                            │ Order              │
│                                            ▼                    │
│                                   ┌──────────────────┐          │
│                                   │  OrderManager    │ ──────► Alpaca REST
│                                   │  (state machine) │          │
│                                   └────────┬─────────┘          │
│                                            │                    │
│                              WS fills ◄────┘◄──── Alpaca WS    │
│                                            │                    │
│                                            ▼                    │
│                                   ┌──────────────────┐          │
│                                   │ PositionTracker  │          │
│                                   │ P&L + drawdown   │          │
│                                   └────────┬─────────┘          │
│                                            │                    │
│                                   ┌────────▼─────────┐          │
│                                   │   KillSwitch     │          │
│                                   │ auto + manual    │          │
│                                   └──────────────────┘          │
│                                                                  │
│  ──── Cross-cutting (always running) ──────────────────────────  │
│  ReconciliationEngine │ AuditLogger │ Alerter │ Dashboard        │
│  EventBus (async pub/sub backbone)                               │
└──────────────────────────────────────────────────────────────────┘
```

Every component communicates through a central **EventBus** — no direct dependencies between layers. Adding a new subscriber (second strategy, extra alert channel) requires zero changes to existing code.

---

## Features

| Component | What It Does |
|---|---|
| **Signal Generator** | Pluggable strategy interface. Ships with EMA crossover. Add your own in 10 lines. |
| **Execution Engine** | Converts signals to orders with notional sizing scaled by signal confidence (0–1) |
| **Order Manager** | Strict state machine — `NEW → SUBMITTED → ACCEPTED → FILLED`. Invalid transitions raise, never silently corrupt. |
| **Position Tracker** | Real-time qty, avg cost, unrealised P&L, realised P&L, drawdown % — updated on every fill and tick |
| **Kill Switch** | One call flattens everything. Auto-fires on drawdown breach. Requires `"RESET_CONFIRMED"` to re-enable. |
| **Reconciliation** | Every 5 minutes: compares internal state to broker snapshot. Auto-corrects drift, alerts on critical mismatch. |
| **Alerting** | Telegram push (free) + Gmail SMTP (free). Rate-limited to prevent floods. Kill switch alerts always go through. |
| **Audit Logger** | Append-only SQLite journal. Every signal, order, fill, and system event. Queryable with SQL after market close. |
| **Dashboard** | Rich terminal live display — portfolio, positions, orders, drawdown, uptime. Refreshes every second. |
| **Paper Mode** | Default. Alpaca paper endpoint — identical API, zero real money. Flip one env var to go live. |

---

## Risk Controls

```
Pre-trade (blocks the order before it leaves):
  ✓ Kill switch active?            → reject immediately
  ✓ Order rate > 30/min?           → reject (sliding window)
  ✓ Order value > max notional?    → reject
  ✓ Daily loss > limit?            → reject

Reactive (fires after the fact):
  ✓ Drawdown > threshold?          → auto-trigger kill switch
  ✓ Kill switch triggered          → cancel all orders + flatten all positions
```

---

## Project Structure

```
TradSys/
├── main.py                    ← Boot + graceful shutdown orchestrator
├── .env.example               ← Copy to .env, fill in keys
├── pyproject.toml             ← Dependencies
│
├── config/
│   ├── settings.py            ← All config via pydantic-settings
│   └── logging_config.py      ← Structured JSON logs, daily rotation
│
├── core/
│   ├── enums.py               ← OrderStatus, SignalDirection, AlertLevel, ...
│   ├── models.py              ← Order, Position, Fill, Signal, Portfolio, Quote, Bar
│   ├── events.py              ← EventBus + all 16 EventTypes
│   └── exceptions.py          ← Typed exception hierarchy
│
├── brokers/
│   ├── base.py                ← BrokerProtocol (swap Alpaca for IB with no upstream changes)
│   └── alpaca_broker.py       ← REST order placement + WebSocket fill stream
│
├── market_data/
│   ├── alpaca_feed.py         ← IEX WebSocket feed, exponential backoff reconnect
│   └── feed_manager.py        ← Unified price lookup for all components
│
├── oms/
│   ├── order_manager.py       ← Central order registry + state machine enforcement
│   └── position_tracker.py   ← Real-time P&L, drawdown monitoring, peak equity
│
├── execution/
│   └── engine.py              ← Signal→Order pipeline, 4 pre-trade risk checks
│
├── signals/
│   └── generator.py           ← BaseStrategy interface + EMA crossover example
│
├── risk/
│   └── kill_switch.py         ← Emergency shutdown, manual + auto on drawdown
│
├── reconciliation/
│   └── engine.py              ← Internal vs broker comparison, auto-correction
│
├── alerts/
│   └── alerter.py             ← Telegram + Gmail dispatcher, rate-limited
│
├── audit/
│   └── logger.py              ← SQLite: audit_events, orders, fills, signals
│
└── monitoring/
    └── dashboard.py           ← Rich terminal live UI
```

---

## Quick Start

### 1. Get Free API Keys

**Alpaca (required — free paper trading):**
1. Sign up at [app.alpaca.markets](https://app.alpaca.markets)
2. Go to **Paper Trading** → **API Keys** → Generate
3. Copy the key and secret

**Telegram (optional — free instant alerts):**
1. Message [@BotFather](https://t.me/BotFather) on Telegram → `/newbot`
2. Copy the token it gives you
3. Message your new bot once, then visit:
   `https://api.telegram.org/bot<TOKEN>/getUpdates`
4. Find `"chat": {"id": YOUR_CHAT_ID}` in the response

**Gmail (optional — free email alerts):**
1. Enable 2-step verification on your Google account
2. Go to [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords)
3. Create an App Password → copy the 16-character password

### 2. Configure

```bash
git clone https://github.com/DivineAlways/TradSys.git
cd TradSys
cp .env.example .env
```

Edit `.env`:
```env
# Required
ALPACA_API_KEY=PKxxxxxxxxxxxxxxxxxxxxxxxx
ALPACA_SECRET_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# Optional but recommended
TELEGRAM_BOT_TOKEN=1234567890:ABCdef...
TELEGRAM_CHAT_ID=987654321

# Optional
SMTP_USER=you@gmail.com
SMTP_PASSWORD=xxxx-xxxx-xxxx-xxxx
ALERT_EMAIL_TO=you@gmail.com

# Symbols to trade
STRATEGY_SYMBOLS=AAPL,MSFT,SPY

# Risk limits
MAX_POSITION_SIZE_USD=10000
MAX_DRAWDOWN_PCT=5.0
MAX_DAILY_LOSS_USD=500
```

### 3. Install & Run

```bash
pip install alpaca-py aiosqlite rich tenacity websockets httpx pandas pydantic pydantic-settings uvloop

# Paper trading (safe — no real money)
python main.py

# Live trading (real money — set TRADING_MODE=live in .env first)
python main.py --live
```

### 4. Emergency Stop

```bash
# Graceful shutdown (runs shutdown sequence)
Ctrl+C

# Or from another terminal
kill -SIGTERM <pid>
```

---

## Writing Your Own Strategy

```python
# signals/my_strategy.py
from signals.generator import BaseStrategy
from core.models import Bar, Signal
from core.enums import SignalDirection
from typing import Optional

class MyStrategy(BaseStrategy):

    @property
    def strategy_id(self) -> str:
        return "my_strategy"

    @property
    def symbols(self) -> list[str]:
        return ["AAPL", "MSFT", "SPY"]

    def on_bar(self, bar: Bar) -> Optional[Signal]:
        # Your logic here. Return Signal to trade, None to do nothing.
        if bar.close > bar.open * 1.005:   # 0.5% green candle
            return Signal(
                symbol=bar.symbol,
                direction=SignalDirection.LONG,
                strength=0.6,              # 60% of max position size
                strategy_id=self.strategy_id,
            )
        return None
```

Register it in `main.py`:
```python
signal_gen.add_strategy(MyStrategy())
```

That's it. The system handles sizing, risk checks, order placement, fill tracking, position updates, alerts, and audit logging automatically.

---

## Order State Machine

```
         ┌─────────┐
         │   NEW   │  ← created locally
         └────┬────┘
              │ broker.place_order()
              ▼
       ┌────────────┐
       │ SUBMITTED  │  ← sent to Alpaca REST
       └─────┬──────┘
             │ broker ACK via WebSocket
             ▼
       ┌────────────┐
       │  ACCEPTED  │
       └──────┬─────┘
       ┌──────┴──────┐
       ▼             ▼
 ┌──────────┐  ┌──────────────┐
 │  FILLED  │  │ PARTIAL_FILL │ ──► FILLED / CANCELLED
 └──────────┘  └──────────────┘
      ✓ terminal

 CANCELLED  REJECTED  EXPIRED  ← also terminal, no further transitions
```

Illegal transitions (`FILLED → NEW`, `CANCELLED → FILLED`) raise `InvalidOrderTransitionError` and are logged. State never silently corrupts.

---

## Audit Database

After every trading session, query your SQLite database at `data/audit.db`:

```sql
-- Fills today
SELECT symbol, side, qty, price, qty*price as value, ts
FROM fills WHERE ts >= date('now') ORDER BY ts DESC;

-- Total P&L by symbol
SELECT symbol,
       SUM(CASE WHEN side='sell' THEN qty*price ELSE -qty*price END) as pnl
FROM fills GROUP BY symbol;

-- Signals that converted to fills
SELECT s.symbol, s.direction, s.strength, f.price, f.qty
FROM signals s
JOIN orders o ON s.id = o.signal_id
JOIN fills  f ON o.id = f.order_id;

-- Order rejection reasons
SELECT symbol, reject_reason, created_at
FROM orders WHERE status = 'rejected';
```

---

## A Trade, Step by Step

```
 1. AAPL bar closes at $151.00
 2. EMA crossover detected → Signal(AAPL, LONG, strength=0.65)
 3. Execution engine sizes: ($10,000 × 0.65) / $151 = 43 shares
 4. 4 risk checks pass → Order(AAPL, BUY, MARKET, qty=43)
 5. OMS submits → Alpaca REST POST /v2/orders
 6. Alpaca assigns broker_order_id, status = SUBMITTED
 7. ~200ms later: Alpaca WS sends fill confirmation
 8. Fill(qty=43, price=$151.02) processed
 9. Position updated: AAPL 43 shares @ $151.02, cash −$6,493.86
10. Telegram: "Filled 43 AAPL @ $151.02 — Gross: $6,493.86"
11. SQLite: order + fill written to audit.db
12. Dashboard: refreshes with new position and P&L
```

Total end-to-end: **< 500ms**

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| Async runtime | `asyncio` + `uvloop` (2–4× faster event loop) |
| Broker | Alpaca (`alpaca-py` SDK) |
| Market data | Alpaca IEX WebSocket (free) |
| Data models | Pydantic v2 (self-validating, immutable) |
| Config | pydantic-settings (`.env` → typed settings) |
| WebSocket | `websockets` with `tenacity` exponential backoff |
| HTTP | `httpx` async client |
| Technical analysis | `pandas` + `pandas-ta` |
| Audit storage | SQLite via `aiosqlite` (async, ACID) |
| Terminal UI | `rich` (live layout, tables, panels) |
| Alerts | Telegram Bot API + Gmail SMTP |
| Logging | Structured JSON, daily rotation, 30-day retention |

---

## Extending to Interactive Brokers

```bash
pip install ib_insync
```

Create `brokers/ib_broker.py` implementing `BrokerProtocol`, then swap in `main.py`:

```python
from brokers.ib_broker import IBBroker
broker = IBBroker(event_bus=bus, host="127.0.0.1", port=7497, client_id=1)
```

IB TWS or IB Gateway must be running locally. Everything else — OMS, signals, execution engine, kill switch — works unchanged.

---

## Safety Defaults

- `TRADING_MODE=paper` is the default — you cannot accidentally trade live
- `.env` is in `.gitignore` — your keys will never be committed
- `logs/` and `data/` are in `.gitignore` — your trade history stays local
- Kill switch requires `"RESET_CONFIRMED"` to re-enable after triggering
- Reconciliation auto-corrects internal state from broker truth every 5 minutes
- All Pydantic models validate on creation — bad data fails fast, not silently

---

<div align="center">

**Built for traders who want to automate without losing control.**

[Read the full architecture →](ARCHITECTURE.md) · [How it works, layer by layer →](How%20It%20Works.md)

</div>
