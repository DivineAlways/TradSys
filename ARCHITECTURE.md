# TradSys – Live Algorithmic Trading System Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        TradSys Component Graph                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Alpaca WS Data ──► FeedManager ──► BAR/QUOTE events                   │
│  (IEX free feed)       │                │                               │
│                        │                ▼                               │
│                        │         SignalGenerator                        │
│                        │         (EMA Crossover + your strategies)      │
│                        │                │ SIGNAL event                  │
│                        │                ▼                               │
│                        └──────► ExecutionEngine                        │
│                                  │ sizing + risk pre-check              │
│                                  │ ORDER (internal)                     │
│                                  ▼                                      │
│                           OrderManager (OMS)                            │
│                           state machine: NEW→SUBMITTED→FILLED          │
│                                  │                                      │
│                    ┌─────────────┤                                      │
│                    │             │ REST place_order()                   │
│                    │             ▼                                      │
│                    │       AlpacaBroker ─── WS Order Updates ────┐     │
│                    │       (paper or live)                        │     │
│                    │                                              │     │
│              FILL events ◄─────────────────────────────────────── │    │
│                    │                                                    │
│                    ▼                                                    │
│             PositionTracker                                             │
│             (qty, avg_cost, unrealised P&L, portfolio equity)           │
│                    │                                                    │
│                    │ drawdown_pct > threshold?                          │
│                    ▼                                                    │
│              KillSwitch ────► cancel_all_orders()                       │
│              (auto + manual)   close_all_positions()                    │
│                                                                         │
│  ────────────── Cross-cutting Services ──────────────────────────────   │
│                                                                         │
│  ReconciliationEngine  (every 5 min: internal vs broker state)         │
│  AuditLogger           (SQLite: all events, orders, fills, signals)    │
│  Alerter               (Telegram free + Gmail SMTP free)               │
│  Dashboard             (Rich terminal live display)                     │
│  EventBus              (async pub/sub connecting all components)        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## File Structure

```
TradSys/
├── main.py                          ← Entry point / orchestrator
├── pyproject.toml                   ← Dependencies
├── .env.example                     ← Configuration template
│
├── config/
│   ├── settings.py                  ← Pydantic settings (all env vars)
│   └── logging_config.py            ← JSON structured logging
│
├── core/
│   ├── enums.py                     ← OrderStatus, OrderType, SignalDirection, ...
│   ├── models.py                    ← Order, Position, Fill, Signal, Portfolio, Quote, Bar
│   ├── events.py                    ← EventBus + all EventType definitions
│   └── exceptions.py                ← Domain exception hierarchy
│
├── brokers/
│   ├── base.py                      ← BrokerProtocol (structural Protocol)
│   └── alpaca_broker.py             ← Alpaca REST + WS adapter
│
├── market_data/
│   ├── alpaca_feed.py               ← Alpaca IEX WS consumer (free)
│   └── feed_manager.py              ← Unified data access layer
│
├── oms/
│   ├── order_manager.py             ← OMS: state machine, fill tracking
│   └── position_tracker.py         ← Real-time positions + P&L
│
├── execution/
│   └── engine.py                    ← Signal→Order pipeline + risk pre-check
│
├── signals/
│   └── generator.py                 ← BaseStrategy + EMA Crossover example
│
├── risk/
│   └── kill_switch.py               ← Emergency shutdown (manual + auto)
│
├── reconciliation/
│   └── engine.py                    ← Internal vs broker state comparison
│
├── alerts/
│   └── alerter.py                   ← Telegram + Gmail dispatch
│
├── audit/
│   └── logger.py                    ← SQLite append-only audit trail
│
├── monitoring/
│   └── dashboard.py                 ← Rich terminal live dashboard
│
├── logs/                            ← Rotating JSON log files
└── data/
    └── audit.db                     ← SQLite audit database
```

---

## Order State Machine

```
             ┌──────────┐
             │   NEW    │  (created locally)
             └────┬─────┘
                  │ submit_order()
                  ▼
           ┌────────────┐
           │ SUBMITTED  │  (sent to broker)
           └─────┬──────┘
                 │ broker ACK
                 ▼
           ┌────────────┐
           │  ACCEPTED  │
           └──────┬─────┘
          ┌───────┴────────┐
          ▼                ▼
   ┌─────────────┐   ┌──────────┐
   │PARTIAL_FILL │   │  FILLED  │ ✓ terminal
   └──────┬──────┘   └──────────┘
          │
          ▼
   ┌──────────┐    ┌───────────┐    ┌─────────┐
   │CANCELLED │    │ REJECTED  │    │ EXPIRED │  ✓ terminal
   └──────────┘    └───────────┘    └─────────┘
```

---

## Event Bus Topics

| EventType                   | Publisher              | Subscriber(s)                          |
|----------------------------|------------------------|----------------------------------------|
| `QUOTE`                    | AlpacaDataFeed         | PositionTracker, SignalGenerator        |
| `BAR`                      | AlpacaDataFeed         | SignalGenerator                         |
| `SIGNAL`                   | SignalGenerator        | ExecutionEngine                         |
| `ORDER_CREATED`            | OMS                    | AuditLogger, Dashboard                  |
| `ORDER_SUBMITTED`          | OMS                    | AuditLogger                             |
| `ORDER_FILLED`             | AlpacaBroker (WS)      | OMS, PositionTracker, AuditLogger, Alerter |
| `ORDER_CANCELLED`          | AlpacaBroker (WS)      | OMS, AuditLogger, Alerter              |
| `ORDER_REJECTED`           | AlpacaBroker (WS)      | OMS, AuditLogger, Alerter              |
| `POSITION_UPDATE`          | PositionTracker        | Dashboard                               |
| `PORTFOLIO_UPDATE`         | PositionTracker        | Dashboard, KillSwitch monitor           |
| `KILL_SWITCH_TRIGGERED`    | KillSwitch             | OMS, ExecutionEngine, Alerter, AuditLogger |
| `DRAWDOWN_BREACH`          | PositionTracker        | KillSwitch, Alerter                    |
| `RECONCILIATION_DISCREPANCY` | ReconciliationEngine | Alerter, AuditLogger                   |
| `ALERT`                    | Multiple               | Alerter                                |
| `SYSTEM_ERROR`             | Multiple               | KillSwitch, Alerter, AuditLogger, Dashboard |

---

## Broker API: Alpaca

### Why Alpaca?
- **Free paper trading** with full API access, no approval needed
- Real-time market data via IEX feed (free)
- REST + WebSocket APIs
- Sign up: https://app.alpaca.markets

### Endpoints Used

| Operation              | Method            | Endpoint                            |
|-----------------------|-------------------|-------------------------------------|
| Place order           | POST              | `/v2/orders`                        |
| Cancel order          | DELETE            | `/v2/orders/{order_id}`             |
| Cancel all orders     | DELETE            | `/v2/orders`                        |
| Get order             | GET               | `/v2/orders/{order_id}`             |
| Get open orders       | GET               | `/v2/orders?status=open`            |
| Get positions         | GET               | `/v2/positions`                     |
| Close position        | DELETE            | `/v2/positions/{symbol}`            |
| Close all positions   | DELETE            | `/v2/positions`                     |
| Get account           | GET               | `/v2/account`                       |
| Order updates (WS)    | WebSocket         | `wss://paper-api.alpaca.markets/stream` |
| Market data (WS)      | WebSocket         | `wss://stream.data.alpaca.markets/v2/iex` |

---

## Risk Controls

### Pre-Trade Checks (ExecutionEngine)
1. **Kill switch active** → reject all orders
2. **Order rate limit** → max N orders/minute (sliding window)
3. **Position size limit** → max $X notional per order
4. **Daily loss limit** → halt if realised P&L < -$X today

### Reactive Controls (KillSwitch)
1. **Drawdown breach** → auto-trigger if drawdown_pct ≥ MAX_DRAWDOWN_PCT
2. **Manual trigger** → `await kill_switch.trigger("manual")`
3. **Sequence**: block new orders → cancel all open → flatten all positions → alert

---

## Alerting Setup (Free)

### Telegram Bot (Recommended – Free, Instant)
```bash
# 1. Message @BotFather on Telegram
#    /newbot → follow prompts → copy token

# 2. Start a conversation with your new bot

# 3. Get your chat_id:
curl "https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates"
# Look for "chat":{"id": <YOUR_CHAT_ID>}

# 4. Add to .env:
TELEGRAM_BOT_TOKEN=1234567890:ABCdef...
TELEGRAM_CHAT_ID=987654321
```

### Gmail SMTP (Free)
```bash
# 1. Enable 2-step verification on your Google account
# 2. Go to: myaccount.google.com/apppasswords
# 3. Create App Password for "Mail"
# 4. Add to .env:
SMTP_USER=you@gmail.com
SMTP_PASSWORD=xxxx-xxxx-xxxx-xxxx  # 16-char app password
ALERT_EMAIL_TO=you@gmail.com
```

---

## Quick Start

```bash
# 1. Install dependencies
pip install -e ".[ib]"

# 2. Configure
cp .env.example .env
# Edit .env: add Alpaca paper keys + Telegram/email

# 3. Run in paper mode (safe)
python main.py

# 4. Emergency stop
Ctrl+C   # triggers graceful shutdown
# OR from another terminal:
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
        return ["AAPL", "MSFT"]

    def on_bar(self, bar: Bar) -> Optional[Signal]:
        # Your logic here
        # Return a Signal to trade, None to do nothing
        if bar.close > bar.open * 1.001:  # price up 0.1%
            return Signal(
                symbol=bar.symbol,
                direction=SignalDirection.LONG,
                strength=0.5,
                strategy_id=self.strategy_id,
            )
        return None

# Register in main.py:
signal_gen.add_strategy(MyStrategy())
```

---

## Audit Database Schema

```sql
-- Every event (immutable log)
audit_events(id, ts, event_type, source, data_json)

-- Structured order lifecycle
orders(id, broker_order_id, symbol, side, order_type, qty,
       limit_price, stop_price, status, filled_qty, avg_fill_price,
       signal_id, strategy_id, reject_reason,
       created_at, submitted_at, filled_at, cancelled_at, updated_at)

-- Individual fills
fills(id, order_id, symbol, side, qty, price, commission,
      broker_exec_id, ts)

-- Signals generated
signals(id, symbol, direction, strength, strategy_id, ts, metadata)
```

---

## Extending to Interactive Brokers

Install: `pip install ib_insync`

Create `brokers/ib_broker.py` implementing `BrokerProtocol`.
Swap in `main.py`:
```python
from brokers.ib_broker import IBBroker
broker = IBBroker(event_bus=bus, host="127.0.0.1", port=7497, client_id=1)
```
IB TWS or IB Gateway must be running locally.
