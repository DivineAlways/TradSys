# TradSys Parameters Guide

Here are all the parameters you can tune, all in your `.env` file:

---

## Trading Mode
```env
TRADING_MODE=paper      # "paper" = fake money | "live" = real money
```

---

## Symbols to Trade
```env
STRATEGY_SYMBOLS=AAPL,MSFT,SPY    # comma separated, any US stock/ETF
```
Examples:
```env
STRATEGY_SYMBOLS=AAPL,MSFT,SPY,TSLA,NVDA,AMZN,GOOGL,META
STRATEGY_SYMBOLS=SPY,QQQ,IWM          # ETFs only
STRATEGY_SYMBOLS=NVDA,AMD,INTC        # semiconductors
```

---

## Risk Limits
```env
MAX_POSITION_SIZE_USD=10000    # max dollar value per single order
                               # e.g. if AAPL is $150 → max 66 shares

MAX_DRAWDOWN_PCT=5.0           # kill switch auto-fires when portfolio
                               # drops this % from its peak equity
                               # e.g. 5.0 = fires at $95,000 from $100k peak

MAX_DAILY_LOSS_USD=500         # halts all trading when realised losses
                               # today exceed this dollar amount

MAX_ORDERS_PER_MINUTE=30       # rate limit — blocks orders if you exceed
                               # this many per 60 second window
```

---

## Strategy (EMA Crossover)
These live in `signals/generator.py` — not in `.env`. Open the file and change the numbers:

```python
# signals/generator.py  line ~100
signal_gen.add_strategy(MomentumStrategy(
    symbols=settings.symbols,
    fast_period=9,     # fast EMA period  ← change this
    slow_period=21,    # slow EMA period  ← change this
    min_bars=30,       # bars needed before first signal fires
))
```

Common configurations:
```python
# Faster / more signals (scalping style)
fast_period=5,  slow_period=13,  min_bars=20

# Default (swing style)
fast_period=9,  slow_period=21,  min_bars=30

# Slower / fewer signals (position trading)
fast_period=20, slow_period=50,  min_bars=60

# Classic golden cross (very slow, big moves only)
fast_period=50, slow_period=200, min_bars=210
```

---

## Position Sizing
Controlled by `MAX_POSITION_SIZE_USD` combined with signal `strength` (0.0–1.0):

```
qty = (MAX_POSITION_SIZE_USD × signal_strength) / current_price
```

Examples with `MAX_POSITION_SIZE_USD=10000`:

| Signal Strength | AAPL @ $150 | SPY @ $500 | NVDA @ $800 |
|---|---|---|---|
| 1.0 (max conviction) | 66 shares | 20 shares | 12 shares |
| 0.7 | 46 shares | 14 shares | 8 shares |
| 0.5 | 33 shares | 10 shares | 6 shares |
| 0.3 (low conviction) | 20 shares | 6 shares | 3 shares |

So if you want **bigger positions**, raise `MAX_POSITION_SIZE_USD`. If you want **smaller**, lower it.

---

## Order Type
In `signals/generator.py` — the strategy returns a `Signal` with an optional `price_hint`:

```python
# Market order (fills immediately at current price) — default
return Signal(symbol=bar.symbol, direction=SignalDirection.LONG, strength=0.8)

# Limit order (only fills at your price or better)
return Signal(symbol=bar.symbol, direction=SignalDirection.LONG, strength=0.8,
              price_hint=bar.close * 0.999)   # 0.1% below close
```

---

## Alerting
```env
# How often the same alert level can fire per channel (built into code)
# Default = 60 seconds between same-level alerts
# Kill switch alerts ALWAYS go through regardless

TELEGRAM_CHAT_ID=7166362256             # your personal chat
TELEGRAM_GROUP_ID_TOKEN=-1002544660979  # your group
```

---

## Logging
```env
LOG_LEVEL=INFO      # DEBUG = everything | INFO = normal | WARNING = problems only
LOG_DIR=logs        # where log files are written
AUDIT_DB_PATH=data/audit.db   # SQLite audit database location
```

---

## Reconciliation
Hardcoded in `reconciliation/engine.py` line 70 — change the interval:
```python
ReconciliationEngine(
    ...
    interval_seconds=300,   # 300 = every 5 minutes ← change this
)
```

---

## Quick Reference Card

```env
# ── The ones you'll actually tune ──────────────────────────────
TRADING_MODE=paper
STRATEGY_SYMBOLS=AAPL,MSFT,SPY
MAX_POSITION_SIZE_USD=10000     ← bigger = bigger positions
MAX_DRAWDOWN_PCT=5.0            ← lower = tighter protection
MAX_DAILY_LOSS_USD=500          ← daily stop loss in dollars
MAX_ORDERS_PER_MINUTE=30        ← rarely need to change
```

After changing anything in `.env` — restart the bot:
```bash
Ctrl+C
python3 main.py
```

After changing anything in `signals/generator.py` — also restart.
