"""Logging configuration helpers for Vertebra plugin."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from litestar.config.app import AppConfig

__all__ = ("VERTEBRA_LOGGERS", "configure_logging")

VERTEBRA_LOGGERS = ("vertebra", "vertebra.supervisor", "vertebra.broker")


def configure_logging(app_config: AppConfig, log_level: str) -> None:
    """Configure logging for Vertebra-related loggers.

    This function sets up logging handlers and formatters for Vertebra's
    internal loggers. It integrates with Litestar's logging configuration
    and supports structlog if the StructlogPlugin is present.

    Args:
        app_config: The Litestar application configuration.
        log_level: The log level to set for Vertebra loggers.
    """
    level = _normalize_log_level(log_level)
    logging_config = getattr(app_config, "logging_config", None)

    if logging_config is None:
        _configure_basic_logging(level)
        return

    structlog_plugin = _get_structlog_plugin(app_config)
    if structlog_plugin is not None:
        _configure_structlog_logging(structlog_plugin, level)
    elif _is_logging_config(logging_config):
        _configure_litestar_logging(logging_config, level)
    else:
        _configure_basic_logging(level)


def _normalize_log_level(log_level: str) -> int:
    """Convert string log level to logging constant."""
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    return level_map.get(log_level.lower(), logging.INFO)


def _configure_basic_logging(level: int) -> None:
    """Configure basic Python logging for Vertebra loggers."""
    for logger_name in VERTEBRA_LOGGERS:
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(level)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)


def _get_structlog_plugin(app_config: AppConfig) -> Any | None:
    """Get StructlogPlugin from app plugins if present."""
    plugins = getattr(app_config, "plugins", None)
    if plugins is None:
        return None

    for plugin in plugins:
        if type(plugin).__name__ == "StructlogPlugin":
            return plugin
    return None


def _is_logging_config(config: Any) -> bool:
    """Type guard for LoggingConfig."""
    return type(config).__name__ == "LoggingConfig"


def _configure_structlog_logging(structlog_plugin: Any, level: int) -> None:
    """Configure structlog-based logging for Vertebra loggers."""
    # When structlog is in use, we just need to ensure the loggers exist
    # at the right level. Structlog handles formatting.
    for logger_name in VERTEBRA_LOGGERS:
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)


def _configure_litestar_logging(logging_config: Any, level: int) -> None:
    """Configure Litestar LoggingConfig for Vertebra loggers."""
    loggers = getattr(logging_config, "loggers", None)
    if loggers is None:
        loggers = {}
        setattr(logging_config, "loggers", loggers)

    level_name = logging.getLevelName(level)
    for logger_name in VERTEBRA_LOGGERS:
        if logger_name not in loggers:
            loggers[logger_name] = {"level": level_name, "handlers": ["queue_listener"], "propagate": False}
