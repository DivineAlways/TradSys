"""
Alerting System.

Channels (all free or optional paid):
  - Telegram Bot   → FREE, instant push notifications (recommended)
  - Email (SMTP)   → FREE with Gmail App Password
  - Twilio SMS     → Paid (optional, commented out unless configured)

Listens to EventBus ALERT events and routes to configured channels.
Supports rate-limiting to prevent alert floods (max 1 per channel per 60s
for same alert type).

Setup:
  Telegram (FREE):
    1. Message @BotFather on Telegram → /newbot → copy token
    2. Message your bot once, then visit:
       https://api.telegram.org/bot<TOKEN>/getUpdates
       to get your chat_id
    3. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env

  Gmail SMTP (FREE):
    1. Enable 2-step verification on Google account
    2. Go to myaccount.google.com/apppasswords → create App Password
    3. Set SMTP_USER, SMTP_PASSWORD, ALERT_EMAIL_TO in .env
"""
from __future__ import annotations

import asyncio
import logging
import smtplib
import time
from collections import defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

import httpx

from config.settings import get_settings
from core.enums import AlertLevel
from core.events import Event, EventBus, EventType

logger = logging.getLogger(__name__)

# Rate limit: minimum seconds between same-level alerts per channel
_RATE_LIMIT_SECONDS = 60


class Alerter:
    """
    Multi-channel alert dispatcher.

    Subscribes to ALERT, ORDER_FILLED, ORDER_REJECTED, KILL_SWITCH_TRIGGERED,
    DRAWDOWN_BREACH, and SYSTEM_ERROR events.
    """

    def __init__(self, event_bus: EventBus) -> None:
        self._bus = event_bus
        self._settings = get_settings()
        self._task: Optional[asyncio.Task] = None
        self._http: Optional[httpx.AsyncClient] = None
        # Rate limiting: channel → level → last_sent timestamp
        self._last_sent: dict[str, dict[str, float]] = defaultdict(dict)
        self._queue = event_bus.subscribe({
            EventType.ALERT,
            EventType.ORDER_FILLED,
            EventType.ORDER_REJECTED,
            EventType.KILL_SWITCH_TRIGGERED,
            EventType.DRAWDOWN_BREACH,
            EventType.SYSTEM_ERROR,
        })

    async def start(self) -> None:
        self._http = httpx.AsyncClient(timeout=10.0)
        self._task = asyncio.create_task(
            self._event_loop(), name="alerter"
        )
        logger.info("Alerter started")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._http:
            await self._http.aclose()
        logger.info("Alerter stopped")

    # ── Event Loop ────────────────────────────────────────────────────────────

    async def _event_loop(self) -> None:
        while True:
            try:
                event: Event = await self._queue.get()
                await self._route_event(event)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "Alerter event loop error",
                    extra={"error": str(exc)},
                    exc_info=True,
                )

    async def _route_event(self, event: Event) -> None:
        """Convert events to alert messages."""
        if event.type == EventType.ALERT:
            data = event.data
            await self.send_alert(
                level=data.get("level", AlertLevel.INFO),
                title=data.get("title", "TradSys Alert"),
                message=data.get("message", ""),
            )

        elif event.type == EventType.ORDER_FILLED:
            data = event.data or {}
            update = data.get("update", {})
            fill = data.get("fill")
            if fill:
                await self.send_alert(
                    level=AlertLevel.INFO,
                    title=f"Fill: {fill.symbol}",
                    message=(
                        f"Filled {fill.qty} {fill.symbol} @ ${fill.price:.2f}\n"
                        f"Side: {fill.side.value.upper()}\n"
                        f"Gross: ${fill.gross_value:,.2f}"
                    ),
                )

        elif event.type == EventType.ORDER_REJECTED:
            data = event.data or {}
            update = data.get("update", {})
            await self.send_alert(
                level=AlertLevel.WARNING,
                title="Order Rejected",
                message=f"Reason: {data.get('reason', 'unknown')}",
            )

        elif event.type == EventType.KILL_SWITCH_TRIGGERED:
            data = event.data or {}
            await self.send_alert(
                level=AlertLevel.CRITICAL,
                title="🚨 KILL SWITCH TRIGGERED 🚨",
                message=f"Reason: {data.get('reason', 'unknown')}\nAll orders cancelled, positions flattened.",
                bypass_rate_limit=True,  # always send this
            )

        elif event.type == EventType.DRAWDOWN_BREACH:
            data = event.data or {}
            await self.send_alert(
                level=AlertLevel.CRITICAL,
                title="Drawdown Breach",
                message=(
                    f"Portfolio drawdown: {data.get('drawdown_pct', 0):.2f}%\n"
                    f"Equity: ${data.get('equity', 0):,.2f}\n"
                    f"Peak: ${data.get('peak_equity', 0):,.2f}"
                ),
            )

        elif event.type == EventType.SYSTEM_ERROR:
            data = event.data or {}
            await self.send_alert(
                level=AlertLevel.ERROR,
                title=f"System Error: {data.get('component', 'unknown')}",
                message=str(data.get("error", "")),
            )

    # ── Public Interface ──────────────────────────────────────────────────────

    async def send_alert(
        self,
        level: AlertLevel,
        title: str,
        message: str,
        bypass_rate_limit: bool = False,
    ) -> None:
        """
        Dispatch alert to all configured channels.
        Rate-limited per channel per level (unless bypass_rate_limit=True).
        """
        level_str = level.value if hasattr(level, "value") else str(level)
        full_message = f"[{level_str.upper()}] {title}\n{message}"

        tasks = []

        if self._settings.telegram_bot_token and self._settings.telegram_chat_id:
            if bypass_rate_limit or self._check_rate_limit("telegram", level_str):
                tasks.append(self._send_telegram(full_message, self._settings.telegram_chat_id))

        if self._settings.telegram_bot_token and self._settings.telegram_group_id_token:
            if bypass_rate_limit or self._check_rate_limit("telegram_group", level_str):
                tasks.append(self._send_telegram(full_message, self._settings.telegram_group_id_token))

        if self._settings.smtp_user and self._settings.alert_email_to:
            if bypass_rate_limit or self._check_rate_limit("email", level_str):
                tasks.append(self._send_email(title, full_message))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    logger.error("Alert delivery failed", extra={"error": str(r)})

    # ── Telegram ──────────────────────────────────────────────────────────────

    async def _send_telegram(self, text: str, chat_id: str) -> None:
        """Send message to a specific Telegram chat_id or group_id."""
        url = (
            f"https://api.telegram.org/bot"
            f"{self._settings.telegram_bot_token}/sendMessage"
        )
        payload = {
            "chat_id": chat_id,
            "text": text[:4096],  # Telegram max length
            "parse_mode": "HTML",
        }
        try:
            resp = await self._http.post(url, json=payload)
            resp.raise_for_status()
            logger.debug("Telegram alert sent", extra={"chat_id": chat_id})
        except Exception as exc:
            logger.error("Telegram send failed", extra={"chat_id": chat_id, "error": str(exc)})
            raise

    # ── Email ─────────────────────────────────────────────────────────────────

    async def _send_email(self, subject: str, body: str) -> None:
        """Send email via SMTP in thread executor (smtplib is blocking)."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._send_email_sync, subject, body)

    def _send_email_sync(self, subject: str, body: str) -> None:
        s = self._settings
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"TradSys | {subject}"
        msg["From"] = s.smtp_user
        msg["To"] = s.alert_email_to
        msg.attach(MIMEText(body, "plain"))

        try:
            with smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=10) as server:
                server.ehlo()
                server.starttls()
                server.login(s.smtp_user, s.smtp_password)
                server.sendmail(s.smtp_user, s.alert_email_to, msg.as_string())
            logger.debug("Email alert sent")
        except Exception as exc:
            logger.error("Email send failed", extra={"error": str(exc)})
            raise

    # ── Rate Limiting ─────────────────────────────────────────────────────────

    def _check_rate_limit(self, channel: str, level: str) -> bool:
        """Return True (allowed) and update timestamp if rate limit not hit."""
        key = f"{channel}:{level}"
        now = time.monotonic()
        last = self._last_sent[channel].get(level, 0)
        if now - last >= _RATE_LIMIT_SECONDS:
            self._last_sent[channel][level] = now
            return True
        return False
