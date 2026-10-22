"""Tests for Vertebra CLI logging configuration."""

from __future__ import annotations

import logging
import sys
from collections.abc import Generator

import pytest

from vertebra.cli._logging import JsonFormatter, StructuredFormatter, configure_logging
from vertebra.utils.serializers import decode_json


@pytest.fixture(autouse=True)
def _reset_root_logger() -> Generator[None, None, None]:
    """Save and restore root logger handlers and level around each test."""
    root = logging.getLogger()
    old_handlers = root.handlers[:]
    old_level = root.level
    yield
    root.handlers = old_handlers
    root.level = old_level


def test_structured_formatter_includes_timestamp_and_fields() -> None:
    """Verify ``StructuredFormatter`` includes timestamp, level, logger, and message."""
    fmt = StructuredFormatter()
    record = logging.LogRecord("test", logging.INFO, "", 0, "hello", (), None)
    output = fmt.format(record)
    assert " | INFO" in output
    assert "| test |" in output
    assert "hello" in output


def test_structured_formatter_level_alignment() -> None:
    """Verify ``StructuredFormatter`` pads level names to a fixed width."""
    fmt = StructuredFormatter()
    info = logging.LogRecord("x", logging.INFO, "", 0, "msg", (), None)
    warn = logging.LogRecord("x", logging.WARNING, "", 0, "msg", (), None)
    info_out = fmt.format(info)
    warn_out = fmt.format(warn)
    assert "INFO    " in info_out
    assert "WARNING " in warn_out


def test_json_formatter_outputs_valid_json() -> None:
    """Verify ``JsonFormatter`` serializes log records as valid JSON."""
    fmt = JsonFormatter()
    record = logging.LogRecord("myapp", logging.ERROR, "", 0, "oops", (), None)
    output = fmt.format(record)
    data = decode_json(output)
    assert data["level"] == "ERROR"
    assert data["logger"] == "myapp"
    assert data["msg"] == "oops"
    assert "ts" in data


def test_json_formatter_includes_exception() -> None:
    """Verify ``JsonFormatter`` includes formatted traceback when ``exc_info`` is present."""
    fmt = JsonFormatter()
    try:
        msg = "test error"
        raise ValueError(msg)
    except ValueError:
        record = logging.LogRecord("x", logging.ERROR, "", 0, "failed", (), sys.exc_info())
    output = fmt.format(record)
    data = decode_json(output)
    assert "exception" in data
    assert "ValueError" in data["exception"]


def test_configure_logging_dev_mode_installs_rich_handler() -> None:
    """Verify dev mode installs a ``RichHandler`` on the root logger."""
    configure_logging(level="info", mode="dev")
    root = logging.getLogger()
    handler_types = [type(h).__name__ for h in root.handlers]
    assert "RichHandler" in handler_types


def test_configure_logging_prod_mode_installs_structured_formatter() -> None:
    """Verify prod mode installs a ``StructuredFormatter`` by default."""
    configure_logging(level="info", mode="prod")
    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, StructuredFormatter)


def test_configure_logging_json_format_installs_json_formatter() -> None:
    """Verify ``log_format='json'`` installs a ``JsonFormatter``."""
    configure_logging(level="info", mode="prod", log_format="json")
    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, JsonFormatter)


def test_configure_logging_plain_format_overrides_dev_mode() -> None:
    """Verify ``log_format='plain'`` overrides dev mode with ``StructuredFormatter``."""
    configure_logging(level="info", mode="dev", log_format="plain")
    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, StructuredFormatter)


def test_configure_logging_sets_level() -> None:
    """Verify ``configure_logging()`` sets the requested log level."""
    configure_logging(level="debug", mode="dev")
    root = logging.getLogger()
    assert root.level == logging.DEBUG


def test_configure_logging_clears_existing_handlers() -> None:
    """Verify ``configure_logging()`` clears pre-existing handlers to avoid duplicates."""
    root = logging.getLogger()
    root.addHandler(logging.StreamHandler())
    root.addHandler(logging.StreamHandler())
    assert len(root.handlers) >= 2
    configure_logging(level="info", mode="dev")
    assert len(root.handlers) == 1
