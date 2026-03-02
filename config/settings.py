"""
Centralised configuration loaded from environment / .env file.
All other modules import `get_settings()` – never read env vars directly.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Trading mode ─────────────────────────────────────────────────────────
    trading_mode: Literal["paper", "live"] = "paper"

    # ── Alpaca ────────────────────────────────────────────────────────────────
    alpaca_api_key: str = ""
    alpaca_secret_key: str = ""
    alpaca_live_api_key: str = ""
    alpaca_live_secret_key: str = ""

    @property
    def alpaca_key(self) -> str:
        return self.alpaca_live_api_key if self.trading_mode == "live" else self.alpaca_api_key

    @property
    def alpaca_secret(self) -> str:
        return self.alpaca_live_secret_key if self.trading_mode == "live" else self.alpaca_secret_key

    @property
    def alpaca_base_url(self) -> str:
        if self.trading_mode == "live":
            return "https://api.alpaca.markets"
        return "https://paper-api.alpaca.markets"

    @property
    def alpaca_data_ws_url(self) -> str:
        return "wss://stream.data.alpaca.markets/v2/iex"

    @property
    def alpaca_trade_ws_url(self) -> str:
        if self.trading_mode == "live":
            return "wss://api.alpaca.markets/stream"
        return "wss://paper-api.alpaca.markets/stream"

    # ── Telegram alerts ───────────────────────────────────────────────────────
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # ── Email alerts ──────────────────────────────────────────────────────────
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    alert_email_to: str = ""

    # ── Twilio SMS (optional) ─────────────────────────────────────────────────
    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_from_number: str = ""
    twilio_to_number: str = ""

    # ── Risk limits ───────────────────────────────────────────────────────────
    max_position_size_usd: float = 10_000.0
    max_drawdown_pct: float = 5.0
    max_orders_per_minute: int = 30
    max_daily_loss_usd: float = 500.0

    # ── Strategy ──────────────────────────────────────────────────────────────
    strategy_symbols: str = "AAPL,MSFT,SPY"
    signal_interval_seconds: int = 60

    @property
    def symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.strategy_symbols.split(",") if s.strip()]

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = "INFO"
    log_dir: str = "logs"
    audit_db_path: str = "data/audit.db"

    @field_validator("trading_mode")
    @classmethod
    def validate_mode(cls, v: str) -> str:
        if v not in ("paper", "live"):
            raise ValueError("trading_mode must be 'paper' or 'live'")
        return v


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
