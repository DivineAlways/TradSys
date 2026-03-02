# TradSys: Complete System Explanation

---

## The Big Picture: What Is This?

This is a **fully automated trading machine**. You start it, it connects to the stock market, watches prices, decides when to buy and sell based on a strategy, places real orders (or fake ones in paper mode), tracks your money, and protects you from blowing up your account — all while keeping records of every single thing it does.

Think of it like hiring a trading desk that never sleeps:

```
Market prices come in
       ↓
Strategy brain decides: buy, sell, or do nothing
       ↓
Risk officer approves or blocks the trade
       ↓
Order is sent to the broker (Alpaca)
       ↓
Fill comes back: you now own X shares at $Y
       ↓
Portfolio is updated: new P&L calculated
       ↓
Everything is logged. You get a Telegram alert.
```

---

## Layer 1: The Foundation — `core/`

Before anything can trade, you need shared vocabulary. This layer defines the language every other component speaks.

### `core/enums.py` — The Vocabulary

Defines all the named constants in the system.

**OrderStatus** — the states an order can be in:
```
NEW → SUBMITTED → ACCEPTED → PARTIAL_FILL → FILLED
                            ↘ CANCELLED
                            ↘ REJECTED
                            ↘ EXPIRED
```
`ORDER_TRANSITIONS` is a dictionary that says "from status X, you are only allowed to move to these statuses". This prevents impossible things like an order going from `FILLED` back to `NEW`.

`TERMINAL_STATUSES` = `{FILLED, CANCELLED, REJECTED, EXPIRED}` — once an order reaches these, it is permanently done. No further changes allowed.

**OrderSide** = `BUY` or `SELL`
**OrderType** = `MARKET`, `LIMIT`, `STOP`, `STOP_LIMIT`
**SignalDirection** = `LONG` (go long/buy), `SHORT` (go short/sell), `FLAT` (close position)
**AlertLevel** = `INFO`, `WARNING`, `ERROR`, `CRITICAL`

---

### `core/models.py` — The Data Structures

Every piece of data in the system is a **Pydantic model** — a class that validates its own data. If you try to create an `Order` with quantity = -5, it will immediately raise an error.

**Quote** — a real-time bid/ask snapshot:
```python
Quote(symbol="AAPL", bid=150.00, ask=150.02)
quote.mid     # → 150.01 (midpoint price)
quote.spread  # → 0.02 (bid-ask spread)
```

**Bar** — an OHLCV candlestick (open, high, low, close, volume for one time period):
```python
Bar(symbol="AAPL", open=149.5, high=151.0, low=149.2, close=150.8, volume=1_200_000)
```

**Signal** — the output of a strategy. Says "I want to go long AAPL with 80% confidence":
```python
Signal(symbol="AAPL", direction=SignalDirection.LONG, strength=0.8)
```
`strength` is 0.0–1.0 and controls position sizing — higher confidence = larger position.

**Fill** — proof that an execution happened at the broker:
```python
Fill(order_id="...", symbol="AAPL", side=BUY, qty=10, price=150.05)
fill.gross_value  # → 1500.50
```

**Order** — the full lifecycle record of one order:
```python
Order(symbol="AAPL", side=BUY, order_type=LIMIT, qty=10, limit_price=150.00)
order.remaining_qty    # how many shares still unfilled
order.is_terminal      # True if FILLED/CANCELLED/REJECTED/EXPIRED
order.is_buy           # True if side == BUY
```

**Position** — your current holding in one stock:
```python
Position(symbol="AAPL", qty=10, avg_cost=150.00, current_price=155.00)
position.unrealised_pnl      # → $50.00  (10 shares × $5 gain)
position.unrealised_pnl_pct  # → 3.33%
position.side                # → "long"
```

**Portfolio** — the full account snapshot:
```python
portfolio.equity          # total account value (cash + positions)
portfolio.drawdown_pct    # how far below your peak equity you are
portfolio.gross_exposure  # total dollar value of all positions
```

**ReconciliationDiscrepancy** — when internal records don't match the broker:
```python
ReconciliationDiscrepancy(
    symbol="AAPL", field="qty",
    internal_value=10.0, broker_value=9.0,
    severity="critical"
)
```

---

### `core/events.py` — The Nervous System

This is the **EventBus** — a pub/sub message system that lets all components talk to each other without knowing about each other directly.

**How it works:**

```
Component A publishes an event:
    bus.publish(Event(type=EventType.ORDER_FILLED, data=fill))

Component B subscribed to ORDER_FILLED gets it automatically:
    queue = bus.subscribe({EventType.ORDER_FILLED})
    event = await queue.get()  # blocks until an event arrives
```

This is why the system is decoupled. The `AlpacaBroker` doesn't know the `PositionTracker` exists — it just says "a fill happened" and broadcasts it. The `PositionTracker` is listening and reacts. The `AuditLogger` is also listening and records it. The `Alerter` is also listening and sends a Telegram message. All three react to the same event with zero coordination.

**All EventTypes:**

| Event | Meaning |
|---|---|
| `QUOTE` | New real-time bid/ask price arrived |
| `BAR` | New candlestick completed |
| `SIGNAL` | Strategy says to trade |
| `ORDER_CREATED` | Order exists in memory |
| `ORDER_SUBMITTED` | Sent to broker |
| `ORDER_ACCEPTED` | Broker acknowledged |
| `ORDER_FILLED` | Fully executed |
| `ORDER_PARTIAL_FILL` | Partially executed |
| `ORDER_CANCELLED` | Cancelled |
| `ORDER_REJECTED` | Broker refused |
| `PORTFOLIO_UPDATE` | Balance/positions changed |
| `KILL_SWITCH_TRIGGERED` | Emergency shutdown |
| `DRAWDOWN_BREACH` | Account lost too much |
| `RECONCILIATION_DISCREPANCY` | Internal ≠ broker |
| `ALERT` | Send a notification |
| `SYSTEM_ERROR` | Something crashed |

---

### `core/exceptions.py` — Typed Errors

Custom exception hierarchy so you can catch specific types of failures:
```
TradSysError
  ├── BrokerError
  │     ├── BrokerConnectionError   (can't connect to Alpaca)
  │     └── BrokerOrderError        (order placement failed)
  ├── InvalidOrderTransitionError   (tried illegal state change)
  ├── RiskLimitBreached             (order blocked by risk check)
  ├── KillSwitchActive              (all trading halted)
  ├── ReconciliationError           (unresolvable mismatch)
  └── ConfigurationError            (missing API keys, etc.)
```

---

## Layer 2: Configuration — `config/`

### `config/settings.py` — All Settings in One Place

Uses **pydantic-settings** to load configuration from your `.env` file. Every other module calls `get_settings()` — nobody reads environment variables directly.

Key properties:
```python
settings = get_settings()
settings.trading_mode     # "paper" or "live"
settings.alpaca_key       # auto-selects paper or live key
settings.alpaca_base_url  # paper-api.alpaca.markets or api.alpaca.markets
settings.symbols          # ["AAPL", "MSFT", "SPY"] parsed from "AAPL,MSFT,SPY"
settings.max_drawdown_pct # 5.0 → kill switch fires at 5% drawdown
settings.max_daily_loss_usd  # $500 → halt if you lose more than this today
```

`@lru_cache(maxsize=1)` on `get_settings()` means the `.env` file is only parsed once — every call after that gets a cached instance. Fast and consistent.

### `config/logging_config.py` — Structured JSON Logs

Every log line is a JSON object:
```json
{"ts": "2026-03-02T14:23:01Z", "level": "INFO", "logger": "oms.order_manager",
 "msg": "Order submitted", "order_id": "abc123", "symbol": "AAPL", "qty": 10}
```
This means you can query logs with tools like `jq`, import them into a database, or feed them to a monitoring tool. Logs rotate daily and keep 30 days of history.

---

## Layer 3: Market Data — `market_data/`

### `market_data/alpaca_feed.py` — Price Stream

Connects to Alpaca's **WebSocket** (a persistent real-time connection, unlike HTTP which is request-response). Subscribes to quotes and bars for your symbols.

**Connection sequence:**
```
1. Connect to wss://stream.data.alpaca.markets/v2/iex
2. Receive: {"T": "success", "msg": "connected"}
3. Send: {"action": "auth", "key": "...", "secret": "..."}
4. Receive: {"T": "success", "msg": "authenticated"}
5. Send: {"action": "subscribe", "quotes": ["AAPL"], "bars": ["AAPL"]}
6. Now receive continuous stream of quote and bar messages
```

**Auto-reconnection:** Uses `tenacity` library with exponential backoff — if the connection drops, it waits 1 second and retries, then 2 seconds, then 4 seconds, up to 30 seconds, and keeps trying up to 20 times. This handles network blips automatically.

**What it publishes:**
- Every incoming quote → `EventType.QUOTE` event on the bus
- Every completed bar → `EventType.BAR` event on the bus
- Also stores the latest quote/bar per symbol locally for instant lookup

### `market_data/feed_manager.py` — Data Access Layer

A clean wrapper that the rest of the system uses. Instead of "get the Alpaca feed and call its internal dictionary", you call:

```python
feed_manager.get_mid_price("AAPL")     # → 150.01 (latest)
feed_manager.get_latest_quote("AAPL")  # → Quote object
feed_manager.get_latest_bar("AAPL")    # → Bar object
await feed_manager.wait_for_data(["AAPL", "MSFT"], timeout=30)
# Waits up to 30 seconds until we have at least one data point per symbol
```

This abstraction means if you switch from Alpaca data to Bloomberg data, nothing else in the system needs to change — only the feed manager internals.

---

## Layer 4: The Broker — `brokers/`

### `brokers/base.py` — The Contract (Protocol)

Defines what every broker adapter must be able to do, using Python's `Protocol` type. This is like an interface in Java/C#. It says: any class claiming to be a broker must implement these methods:

```python
async def connect()               # authenticate
async def place_order(order)      # send an order
async def cancel_order(order)     # cancel one order
async def cancel_all_orders()     # cancel everything (kill switch)
async def get_positions()         # what do I own?
async def get_portfolio()         # account value and cash
async def close_all_positions()   # flatten everything (kill switch)
async def subscribe_order_updates() # start listening for fills via WS
```

This means you can have `AlpacaBroker`, `IBBroker`, `CoinbaseBroker` — the rest of the system doesn't care which one is running. It just calls these methods.

### `brokers/alpaca_broker.py` — Alpaca Implementation

Two connection types run simultaneously:

**REST API** (for placing/cancelling orders):
- `alpaca-py` SDK calls are **blocking** (synchronous) — they freeze the program while waiting for a response
- Solution: `await loop.run_in_executor(None, blocking_call)` — runs the blocking call in a thread pool so the async event loop stays free
- Returns updated Order objects with `broker_order_id` populated

**WebSocket stream** (for receiving fills):
- Persistent connection to `wss://paper-api.alpaca.markets/stream`
- Receives messages whenever an order state changes at Alpaca
- `_handle_trade_update()` converts Alpaca's format to internal events and publishes them to the EventBus

**Paper vs Live switching:**
```python
# In .env:
TRADING_MODE=paper  → connects to paper-api.alpaca.markets (fake money)
TRADING_MODE=live   → connects to api.alpaca.markets (real money)
```
The only difference is the URL — all code is identical.

---

## Layer 5: Order Management — `oms/`

### `oms/order_manager.py` — The State Machine

This is the central registry and traffic controller for orders. Every order in the system lives here.

**How an order flows through:**

```
1. ExecutionEngine calls oms.submit_order(order)
2. OMS registers it internally with status NEW
3. Publishes ORDER_CREATED event (AuditLogger records it)
4. Calls broker.place_order(order)
5. Broker returns it with broker_order_id set, status = SUBMITTED
6. OMS updates its internal record
7. Publishes ORDER_SUBMITTED event

[Later, via WebSocket from Alpaca:]
8. Broker WS sends "fill" message
9. AlpacaBroker publishes ORDER_FILLED event to bus
10. OMS is subscribed, catches it
11. Validates the transition (ACCEPTED → FILLED is legal)
12. Updates filled_qty and avg_fill_price using weighted average
13. Publishes updated state
```

**Fill weighted average price calculation:**
If you bought 5 shares at $100 and then 5 more at $110:
```
avg = ($100 × 5 + $110 × 5) / 10 = $105
```
The OMS tracks this precisely across partial fills.

**State machine enforcement:**
```python
# Somewhere in core/enums.py:
ORDER_TRANSITIONS = {
    OrderStatus.NEW:       {SUBMITTED, CANCELLED},
    OrderStatus.SUBMITTED: {ACCEPTED, REJECTED, CANCELLED, FILLED},
    OrderStatus.ACCEPTED:  {PARTIAL_FILL, FILLED, PENDING_CANCEL, CANCELLED},
    OrderStatus.FILLED:    set()  # terminal — nothing allowed
}
```
If the OMS receives a fill update for an already-cancelled order, it sees that `CANCELLED → FILLED` is not in the transitions and raises `InvalidOrderTransitionError`. This prevents corrupt state.

**Query methods for the dashboard and reconciliation:**
```python
oms.get_order("order-id")               # find by internal ID
oms.get_order_by_broker_id("alpaca-id") # find by Alpaca's ID
oms.get_open_orders()                   # all non-terminal orders
oms.get_orders_for_symbol("AAPL")       # all AAPL orders
oms.open_order_count()                  # how many open right now
```

### `oms/position_tracker.py` — Your Portfolio in Real-Time

Listens to fills and quotes. Maintains the truth of what you own right now.

**When a BUY fill arrives:**
```
1. Find existing position for the symbol (or create new)
2. Add fill.qty to position.qty
3. Recalculate avg_cost: (old_avg × old_qty + fill_price × fill_qty) / new_qty
4. Subtract fill value from cash
5. Recalculate equity: cash + sum(position.qty × current_price for all positions)
6. Update peak_equity if equity is a new high
7. Publish PORTFOLIO_UPDATE event
```

**When a SELL fill arrives:**
```
1. Calculate realised P&L: (fill_price - avg_cost) × fill_qty
2. Add to realised_pnl_today running total
3. Reduce position qty
4. Add sale proceeds to cash
```

**When a QUOTE arrives:**
```
1. Update current_price for that symbol's position
2. Recalculate unrealised P&L
3. Check drawdown: (peak_equity - current_equity) / peak_equity
4. If drawdown > MAX_DRAWDOWN_PCT → publish DRAWDOWN_BREACH event
```

**Drawdown example:**
- Start with $100,000 (peak)
- Lose $3,000 → equity = $97,000
- Drawdown = (100,000 - 97,000) / 100,000 = 3%
- Config says MAX_DRAWDOWN_PCT = 5%
- Not triggered yet

- Lose another $3,000 → equity = $94,000
- Drawdown = 6% > 5% → DRAWDOWN_BREACH event fires → KillSwitch triggers

---

## Layer 6: Execution Engine — `execution/engine.py`

This is the **signal-to-order pipeline**. It bridges the strategy brain and the order management system.

**Flow for each incoming SIGNAL:**

```
1. Check kill switch — if active, drop the signal immediately

2. Build the order:
   - LONG signal  → BUY order
   - SHORT signal → SELL order
   - FLAT signal  → close existing position (market order for remaining qty)

3. Size the order:
   - If signal.target_qty > 0: use that exact quantity
   - Otherwise: notional sizing
     qty = (MAX_POSITION_SIZE_USD × signal.strength) / current_price
     e.g.: $10,000 × 0.8 strength / $150 price = 53 shares

4. Run 4 pre-trade risk checks:
   a. Kill switch active? → reject
   b. Order rate: more than 30 orders in last 60 seconds? → reject
   c. Order value > MAX_POSITION_SIZE_USD? → reject
   d. Realised P&L today < -MAX_DAILY_LOSS_USD? → reject

5. Submit to OMS → OMS sends to broker
```

**Order type decision:**
- Signal has `price_hint` → create a LIMIT order at that price
- No price hint → create a MARKET order (fills immediately at current price)

**Rate limiting implementation:**
Uses a sliding window with a `deque` (double-ended queue):
```python
# Every time an order is submitted, timestamp is added
order_times.append(time.monotonic())
# Remove timestamps older than 60 seconds
while order_times[0] < now - 60:
    order_times.popleft()
# Count remaining = orders in last 60 seconds
if len(order_times) > MAX_ORDERS_PER_MINUTE:
    raise RiskLimitBreached(...)
```

---

## Layer 7: Signal Generator — `signals/generator.py`

### BaseStrategy — The Interface

Every strategy must implement:
```python
@property
def strategy_id(self) -> str: ...   # unique name
@property
def symbols(self) -> list[str]: ... # which stocks to watch
def on_bar(self, bar: Bar) -> Optional[Signal]: ...
```
`on_bar` is called every time a new bar arrives. Return a Signal to trade, return `None` to do nothing.

### MomentumStrategy (EMA Crossover) — The Example

**What is an EMA?**
An Exponential Moving Average weights recent prices more heavily than old prices. A 9-period EMA tracks price more tightly than a 21-period EMA.

**The signal logic:**
```
Fast EMA (9 periods) crosses ABOVE Slow EMA (21 periods)
→ momentum is turning bullish → LONG signal

Fast EMA crosses BELOW Slow EMA
→ momentum is turning bearish → FLAT signal (close the position)
```

**Implementation:**
```python
closes = deque(maxlen=60)   # ring buffer of last 60 close prices

series = pd.Series(closes)
fast_ema = series.ewm(span=9, adjust=False).mean().iloc[-1]   # today
slow_ema = series.ewm(span=21, adjust=False).mean().iloc[-1]
prev_fast = ...mean().iloc[-2]   # yesterday
prev_slow = ...mean().iloc[-2]

# Cross detected: yesterday fast was below slow, today it's above
bullish_cross = fast_ema > slow_ema and prev_fast <= prev_slow
```

Signal strength = how far apart the EMAs are (as a percentage). Wider gap = higher conviction = larger position size.

**Deduplication:** The strategy tracks `_last_direction` per symbol. If the last signal was `LONG` and another `LONG` signal would fire, it is suppressed. Only transitions generate signals.

**How to add your own strategy:**
```python
class MyStrategy(BaseStrategy):
    @property
    def strategy_id(self): return "my_strategy"
    @property
    def symbols(self): return ["AAPL"]

    def on_bar(self, bar: Bar) -> Optional[Signal]:
        if some_condition(bar):
            return Signal(symbol=bar.symbol, direction=SignalDirection.LONG)
        return None
```
That is literally it. Register it in `main.py` and the system handles the rest.

---

## Layer 8: Risk — `risk/kill_switch.py`

The kill switch is a **one-way door**. Once triggered, it cannot be accidentally reversed.

**Two ways to trigger:**

1. **Manual:** `await kill_switch.trigger("manual")` — call this from a script, button, or command line
2. **Automatic:** the kill switch subscribes to `DRAWDOWN_BREACH` events — if the position tracker reports your drawdown exceeds the configured threshold, the kill switch fires automatically

**What happens when triggered (in order):**

```
Step 1: Set internal flag  →  all future orders immediately rejected
Step 2: Publish KILL_SWITCH_TRIGGERED to EventBus
         → OMS sees it → blocks new orders
         → ExecutionEngine sees it → drops all incoming signals
Step 3: Call broker.cancel_all_orders()  → REST DELETE /v2/orders
Step 4: Call broker.close_all_positions() → REST DELETE /v2/positions
         This places market orders to sell everything you own immediately
Step 5: Publish ALERT with level=CRITICAL
         → Alerter sends Telegram + Email: "KILL SWITCH ACTIVATED"
```

**Reset (intentionally hard):**
```python
kill_switch.reset("RESET_CONFIRMED")
# Must pass the exact string — prevents accidental reactivation
```

---

## Layer 9: Reconciliation — `reconciliation/engine.py`

Runs every 5 minutes and asks: **does our internal state match what the broker actually shows?**

This is necessary because:
- WS messages can be lost or arrive out of order
- Software bugs can corrupt internal state
- Network timeouts can cause order status to be missed

**What it checks:**

**Position quantities:**
```
Internal says: AAPL qty = 10
Broker says:   AAPL qty = 9
→ CRITICAL discrepancy! Log it, alert, auto-correct to broker value
```

**Average cost:**
```
Internal says: AAPL avg_cost = $150.00
Broker says:   AAPL avg_cost = $150.03
→ 0.02% difference < 0.5% threshold → ignore (rounding)
```

**Ghost orders** (open internally, not at broker):
```
OMS has order X as SUBMITTED
Broker has never heard of order X
→ Something went wrong. Mark X as CANCELLED internally. Alert.
```

**Orphan positions** (at broker, not internally tracked):
```
Broker shows 100 shares of TSLA
Internal tracker has no TSLA position
→ CRITICAL — we own something we don't know about
```

**Portfolio equity drift:**
```
Internal equity = $100,000
Broker equity =  $98,500
Difference = 1.5% > 1% threshold
→ Sync internal state from broker snapshot. Warn operator.
```

**Auto-corrections** are intentionally conservative: the system always trusts the broker as ground truth and updates internal state to match, never the other way around.

---

## Layer 10: Alerting — `alerts/alerter.py`

Subscribes to several EventTypes and routes them to notification channels.

**Events that trigger alerts:**

| Event | Alert message |
|---|---|
| `ORDER_FILLED` | "Filled 10 AAPL @ $150.05 — Gross: $1,500.50" |
| `ORDER_REJECTED` | "Order Rejected — Reason: insufficient buying power" |
| `KILL_SWITCH_TRIGGERED` | "KILL SWITCH ACTIVATED — Reason: auto:drawdown:5.2%" |
| `DRAWDOWN_BREACH` | "Portfolio drawdown: 5.2% — Equity: $94,800 — Peak: $100,000" |
| `SYSTEM_ERROR` | "System Error: alpaca_order_stream — Connection lost" |
| `RECONCILIATION_DISCREPANCY` | "Recon: AAPL qty internal=10 broker=9 [critical]" |

**Rate limiting:** Maximum one alert per channel per alert level per 60 seconds. This prevents getting 1,000 Telegram messages if something repeatedly fails. Kill switch alerts bypass rate limiting — they always go through.

**Telegram (free, instant push to your phone):**
```python
POST https://api.telegram.org/bot{TOKEN}/sendMessage
{"chat_id": "...", "text": "[CRITICAL] KILL SWITCH ACTIVATED..."}
```

**Gmail SMTP (free with App Password):**
```python
smtp = smtplib.SMTP("smtp.gmail.com", 587)
smtp.starttls()
smtp.login(user, app_password)
smtp.sendmail(from, to, message)
```
Because `smtplib` is synchronous (blocking), it runs in a thread executor so it doesn't freeze the async event loop.

---

## Layer 11: Audit Logger — `audit/logger.py`

Everything that happens is written to SQLite. This is your **compliance record and post-trade analysis database**.

**Four tables:**

**`audit_events`** — raw dump of every single event:
```sql
id | ts                    | event_type    | source        | data_json
1  | 2026-03-02T14:23:01Z  | signal        | ema_crossover | {"symbol":"AAPL",...}
2  | 2026-03-02T14:23:02Z  | order_created | oms           | {"id":"abc",...}
3  | 2026-03-02T14:23:03Z  | order_filled  | alpaca_broker | {"fill":{"qty":10,...}}
```

**`orders`** — structured order lifecycle (easier to query than raw JSON):
```sql
id | symbol | side | qty | status | filled_qty | avg_fill_price | created_at | filled_at
```

**`fills`** — individual executions:
```sql
id | order_id | symbol | side | qty | price | commission | ts
```

**`signals`** — what the strategy decided:
```sql
id | symbol | direction | strength | strategy_id | ts | metadata
```

**How writes work:** All events go through an `asyncio.Queue` with capacity 50,000. The logger pulls from this queue and commits to SQLite. This means a burst of 1,000 events won't block the trading system — they queue up and get written at the database's pace.

**Example queries after a trading day:**
```sql
-- How many fills today?
SELECT COUNT(*) FROM fills WHERE ts >= date('now');

-- Total commission paid?
SELECT SUM(commission) FROM fills WHERE ts >= date('now');

-- All AAPL orders this week?
SELECT * FROM orders WHERE symbol='AAPL' AND created_at >= date('now', '-7 days');

-- Signals that led to fills?
SELECT s.*, f.price, f.qty FROM signals s
JOIN orders o ON s.id = o.signal_id
JOIN fills f ON o.id = f.order_id;
```

---

## Layer 12: Dashboard — `monitoring/dashboard.py`

A **Rich library terminal UI** that refreshes every second:

```
TradSys  [PAPER]  |  14:23:45 UTC  |  KillSwitch: OK
────────────────────────────────────────────────────────
Portfolio                  │  Recent Orders (last 10)
                           │
Equity:      $102,350.00   │  AAPL  BUY   10  MKT  filled   10@150.05
Cash:         $86,848.00   │  MSFT  BUY    5  LMT  accepted  -
Unrealised:   +$1,502.00   │  SPY   SELL  20  MKT  cancelled -
Realised:       +$500.00   │
Drawdown:          1.2%    │
Gross Exposure: $15,502.00 │
                           │
Positions:                 │
AAPL  10.00  $150.05  $155.07  +$50.20  long
MSFT   5.00  $410.10  $415.50  +$27.00  long
────────────────────────────────────────────────────────
Uptime: 0:02:34  |  Open Orders: 1  |  Total Orders: 3  |  Last Error: None
```

The dashboard runs in a background task. It gets updates from the EventBus (specifically PORTFOLIO_UPDATE events from PositionTracker) so the numbers are live.

---

## Layer 13: Orchestrator — `main.py`

The entry point that wires everything together and starts it in the correct order.

**Boot sequence (order matters):**
```
1.  AuditLogger.start()       — log from day one, before anything trades
2.  Alerter.start()           — alerts ready before any errors can occur
3.  broker.connect()          — establish connection to Alpaca
4.  broker.subscribe_order_updates() — start WS fill stream
5.  OMS.start()               — ready to receive orders
6.  KillSwitch.start()        — emergency protection online
7.  broker.get_portfolio()    — sync current state from broker
8.  PositionTracker.start()   — now we know our starting balance
9.  FeedManager.start()       — begin receiving market data
10. wait_for_data(30s)        — don't trade until we have prices
11. SignalGenerator.start()   — strategies start processing bars
12. ExecutionEngine.start()   — orders can now flow
13. ReconciliationEngine.start() — periodic verification starts
14. Dashboard.start()         — live terminal view
```

**Shutdown sequence (reverse order, graceful):**
```
1.  Dashboard.stop()           — stop updating UI
2.  ExecutionEngine.stop()     — no more orders from signals
3.  SignalGenerator.stop()     — no more signals
4.  ReconciliationEngine.stop()
5.  FeedManager.stop()         — disconnect market data WS
6.  OMS.stop()
7.  KillSwitch.stop()
8.  PositionTracker.stop()
9.  broker.disconnect()        — clean WS close
10. Alerter.stop()
11. AuditLogger.stop()         — flush final records to SQLite
```

`Ctrl+C` sends `SIGINT` → the signal handler sets a `shutdown_event` → the `await shutdown_event.wait()` unblocks → graceful shutdown runs.

---

## How a Complete Trade Flows End-to-End

Here's the full journey from price movement to filled order:

```
1. AAPL tick arrives on Alpaca WS data feed
   └─► AlpacaDataFeed._handle_bar() called
   └─► Bar(symbol="AAPL", close=151.00) created
   └─► bus.publish(Event(BAR, bar))

2. SignalGenerator receives BAR event
   └─► Runs MomentumStrategy.on_bar(bar)
   └─► Detects EMA bullish crossover
   └─► Returns Signal(AAPL, LONG, strength=0.65)
   └─► bus.publish(Event(SIGNAL, signal))

3. ExecutionEngine receives SIGNAL event
   └─► _build_order(signal): side=BUY, qty = ($10,000 × 0.65) / $151 = 43 shares
   └─► _pre_flight_risk_check(): 4 checks pass
   └─► Creates Order(AAPL, BUY, MARKET, qty=43)
   └─► Calls oms.submit_order(order)

4. OrderManager.submit_order()
   └─► Registers order: status=NEW
   └─► bus.publish(Event(ORDER_CREATED, order))
   └─► Calls broker.place_order(order)

5. AlpacaBroker.place_order()
   └─► Calls alpaca_client.submit_order(MarketOrderRequest(...))  [in thread]
   └─► Alpaca REST POST /v2/orders
   └─► Returns with broker_order_id = "alpaca-uuid-123"
   └─► Returns order with status=SUBMITTED

6. OMS updates order → status=SUBMITTED
   └─► bus.publish(Event(ORDER_SUBMITTED, order))
   └─► AuditLogger records it to SQLite

7. [~200ms later] Alpaca WS sends fill message:
   {"event": "fill", "price": "151.02", "qty": "43", ...}

8. AlpacaBroker._handle_trade_update()
   └─► Creates Fill(order_id="alpaca-uuid-123", qty=43, price=151.02)
   └─► bus.publish(Event(ORDER_FILLED, {update: {...}, fill: fill}))

9. OrderManager receives ORDER_FILLED
   └─► Looks up internal order by broker_order_id
   └─► Calls _apply_fill(order, fill): filled_qty=43, avg_price=151.02
   └─► Status → FILLED (valid transition: ACCEPTED → FILLED)
   └─► Updates internal record

10. PositionTracker receives ORDER_FILLED
    └─► Finds or creates Position("AAPL")
    └─► qty += 43, avg_cost = 151.02
    └─► cash -= 43 × 151.02 = -$6,493.86
    └─► equity recalculated
    └─► bus.publish(Event(PORTFOLIO_UPDATE, portfolio))

11. AuditLogger receives ORDER_FILLED
    └─► Writes to orders table: status=filled, filled_qty=43, avg_price=151.02
    └─► Writes to fills table: qty=43, price=151.02

12. Alerter receives ORDER_FILLED
    └─► Sends Telegram: "Filled 43 AAPL @ $151.02 — Gross: $6,493.86"
    └─► (if email configured, sends email too)

13. Dashboard receives PORTFOLIO_UPDATE
    └─► Refreshes terminal: shows AAPL 43 shares, +$0.00 P&L
```

Total latency: typically under 500ms from bar to filled order confirmation.

---

## Summary of Design Decisions

| Decision | Why |
|---|---|
| **AsyncIO everywhere** | Single thread, no locks needed for most operations. Thousands of events per second with no overhead. |
| **EventBus pub/sub** | Components are decoupled. Adding a new subscriber (e.g., a second alerter) requires zero changes to publishers. |
| **Pydantic models** | Self-validating data. Impossible to create an Order with qty=-5 or a Fill with price=0. Bugs caught at data creation, not hours later. |
| **State machine with transition table** | Impossible for an order to go FILLED → SUBMITTED. Bugs like "order updated after cancellation" are caught and logged, not silently ignored. |
| **Broker as executor** | Alpaca's SDK is synchronous. Running it in a thread executor keeps the event loop free. The rest of the system never stalls waiting for an HTTP response. |
| **Paper mode default** | You must explicitly set `TRADING_MODE=live` to risk real money. Default is always safe. |
| **Broker is ground truth** | During reconciliation, if internal state disagrees with broker, we always trust the broker. The broker is the authority — our internal state is a derived view. |
| **Kill switch is irreversible** | Requires explicit string `"RESET_CONFIRMED"` to re-enable. Makes it impossible to accidentally reset after triggering. |
| **Audit to SQLite, not files** | SQLite is queryable. Post-trade analysis with SQL beats grepping through log files. It also survives crashes — SQLite is ACID compliant. |
