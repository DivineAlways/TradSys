"""
Structured JSON logging setup.
Call `setup_logging()` once at startup before any other module logs.
"""
from __future__ import annotations

import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class JSONFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        log: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }
        if record.exc_info:
            log["exc"] = self.formatException(record.exc_info)
        # Merge any extra fields the caller passed
        for key, val in record.__dict__.items():
            if key not in {
                "args", "asctime", "created", "exc_info", "exc_text", "filename",
                "funcName", "id", "levelname", "levelno", "lineno", "module",
                "msecs", "message", "msg", "name", "pathname", "process",
                "processName", "relativeCreated", "stack_info", "thread",
                "threadName", "taskName",
            }:
                log[key] = val
        return json.dumps(log, default=str)


def setup_logging(level: str = "INFO", log_dir: str = "logs") -> None:
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # ── Console handler (human-readable for dev, JSON in prod) ───────────────
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(JSONFormatter())
    root.addHandler(console)

    # ── Rotating file handler ─────────────────────────────────────────────────
    today = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(log_dir, f"tradsys_{today}.log"),
        when="midnight",
        backupCount=30,
        utc=True,
    )
    file_handler.setFormatter(JSONFormatter())
    root.addHandler(file_handler)

    # Suppress noisy third-party loggers
    for noisy in ("urllib3", "asyncio", "websockets.client", "hpack"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
