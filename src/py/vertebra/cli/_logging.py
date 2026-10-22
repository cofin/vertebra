"""Centralized log configuration for Vertebra CLI.

Configures logging based on mode (dev/prod) and format (auto/plain/json).
"""

import logging
import sys
from datetime import UTC, datetime
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from vertebra.utils.serializers import encode_json

__all__ = ("JsonFormatter", "StructuredFormatter", "configure_logging")


class StructuredFormatter(logging.Formatter):
    """Plain-text structured formatter for production logs.

    Format: ``YYYY-MM-DD HH:MM:SS.mmm | LEVEL    | logger.name | message``
    """

    _LEVEL_WIDTH = 8

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record into structured plain text."""
        ts = datetime.fromtimestamp(record.created, tz=UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        level = record.levelname.ljust(self._LEVEL_WIDTH)
        return f"{ts} | {level} | {record.name} | {record.getMessage()}"


class JsonFormatter(logging.Formatter):
    """JSON structured formatter for production log ingestion."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a JSON string."""
        ts = datetime.fromtimestamp(record.created, tz=UTC).isoformat()
        entry: dict[str, Any] = {
            "ts": ts,
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1] is not None:
            entry["exception"] = self.formatException(record.exc_info)
        return encode_json(entry)


def configure_logging(
    *,
    level: str = "info",
    mode: str = "dev",
    log_format: str = "auto",
    tracebacks_show_locals: bool = False,
) -> None:
    """Configure the root logger for Vertebra.

    Args:
        level: Log level name (debug, info, warning, error).
        mode: ``"dev"`` for Rich output, ``"prod"`` for structured text.
        log_format: ``"auto"`` (mode-based), ``"plain"`` (structured text),
            or ``"json"`` (JSON lines).
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()

    effective_format = log_format
    if effective_format == "auto":
        effective_format = "rich" if mode == "dev" else "plain"

    if effective_format == "rich":
        handler = RichHandler(
            console=Console(stderr=True, highlight=False),
            show_path=False,
            show_time=True,
            rich_tracebacks=True,
            tracebacks_show_locals=tracebacks_show_locals,
            markup=True,
            log_time_format="[%H:%M:%S]",
        )
        handler.setLevel(log_level)
        root.addHandler(handler)
    elif effective_format == "json":
        handler_stream = logging.StreamHandler(sys.stderr)
        handler_stream.setFormatter(JsonFormatter())
        handler_stream.setLevel(log_level)
        root.addHandler(handler_stream)
    else:
        handler_stream = logging.StreamHandler(sys.stderr)
        handler_stream.setFormatter(StructuredFormatter())
        handler_stream.setLevel(log_level)
        root.addHandler(handler_stream)
