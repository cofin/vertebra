"""Tests for the Vertebra dev TUI components."""

from __future__ import annotations

import pytest

from vertebra.cli.tui import DevLogFormatter, LogBuffer, LogLine


def test_log_buffer_append_and_get() -> None:
    """Verify appending and retrieving lines from ``LogBuffer``."""
    buf = LogBuffer(capacity=10)
    buf.append(LogLine(raw="hello"))
    assert len(buf) == 1
    lines = buf.get_lines()
    assert lines[0].raw == "hello"


def test_log_buffer_capacity_overflow() -> None:
    """Verify ``LogBuffer`` evicts oldest lines when capacity is exceeded."""
    buf = LogBuffer(capacity=3)
    for i in range(5):
        buf.append(LogLine(raw=f"line {i}"))
    assert len(buf) == 3
    lines = buf.get_lines()
    assert lines[0].raw == "line 2"
    assert lines[2].raw == "line 4"


def test_log_buffer_get_last_n() -> None:
    """Verify ``get_lines(n)`` returns the most recent ``n`` lines."""
    buf = LogBuffer(capacity=10)
    for i in range(5):
        buf.append(LogLine(raw=f"line {i}"))
    lines = buf.get_lines(2)
    assert len(lines) == 2
    assert lines[0].raw == "line 3"
    assert lines[1].raw == "line 4"


def test_log_buffer_empty() -> None:
    """Verify an empty ``LogBuffer`` has length 0 and returns an empty list."""
    buf = LogBuffer()
    assert len(buf) == 0
    assert buf.get_lines() == []


@pytest.mark.parametrize(
    ("raw", "expected_source", "expected_level", "expected_message"),
    [
        ("Starting server on port 8000", "", "info", "Starting server on port 8000"),
        ("[vertebra worker 2] connected to ring 'test'", "worker.2", "info", "connected to ring 'test'"),
        ("[vertebra] wsgi handler error for request 123", "server", "error", "wsgi handler error for request 123"),
        ("ERROR: connection failed", "", "error", "ERROR: connection failed"),
        ("WARNING: deprecated function called", "", "warning", "WARNING: deprecated function called"),
        ("debug: loading config", "", "debug", "debug: loading config"),
    ],
)
def test_dev_log_formatter_parse(
    raw: str,
    expected_source: str,
    expected_level: str,
    expected_message: str,
) -> None:
    """Verify ``DevLogFormatter.parse()`` extracts source, level, and message."""
    line = DevLogFormatter.parse(raw)
    assert line.source == expected_source
    assert line.level == expected_level
    assert line.message == expected_message


def test_dev_log_formatter_render_produces_text() -> None:
    """Verify ``DevLogFormatter.render()`` includes source and message."""
    line = LogLine(raw="test", level="info", source="worker.0", message="hello")
    rendered = DevLogFormatter.render(line)
    assert "hello" in rendered.plain
    assert "worker.0" in rendered.plain


def test_dev_log_formatter_render_error_line() -> None:
    """Verify ``DevLogFormatter.render()`` formats error lines."""
    line = LogLine(raw="err", level="error", source="", message="something broke")
    rendered = DevLogFormatter.render(line)
    assert "something broke" in rendered.plain
