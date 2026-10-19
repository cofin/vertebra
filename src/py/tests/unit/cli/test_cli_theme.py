"""Tests for Vertebra CLI theme module."""

from __future__ import annotations

import io
from collections.abc import Generator

import pytest

from vertebra.cli import theme
from vertebra.cli.theme import (
    ACCENT,
    ERROR,
    MUTED,
    SECONDARY,
    SUCCESS,
    SYM_BULLET,
    SYM_ELLIPSIS,
    SYM_ERROR,
    SYM_INFO,
    SYM_SUCCESS,
    SYM_WARNING,
    WARNING,
    VertebraConsole,
    console,
    get_shutdown_blocks,
    get_startup_blocks,
    register_shutdown_block,
    register_startup_block,
    render_shutdown_blocks,
    render_startup_blocks,
)


def _make_console() -> tuple[VertebraConsole, io.StringIO]:
    """Create a ``VertebraConsole`` that writes to a ``StringIO`` buffer."""
    buf = io.StringIO()
    vc = VertebraConsole(file=buf, force_terminal=True)
    return vc, buf


@pytest.fixture(autouse=True)
def _clear_registries() -> Generator[None, None, None]:
    """Clear content registries before and after each test."""
    theme._STARTUP_BLOCKS.clear()
    theme._SHUTDOWN_BLOCKS.clear()
    yield
    theme._STARTUP_BLOCKS.clear()
    theme._SHUTDOWN_BLOCKS.clear()


@pytest.mark.parametrize(
    ("constant", "expected"),
    [
        (ACCENT, "cyan"),
        (SUCCESS, "green"),
        (WARNING, "yellow"),
        (ERROR, "red"),
        (SECONDARY, "dim"),
        (MUTED, "dim italic"),
    ],
)
def test_color_palette_constants(constant: str, expected: str) -> None:
    """Verify color palette constants match expected Rich styles."""
    assert constant == expected


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [
        (SYM_SUCCESS, "✓"),
        (SYM_ERROR, "✗"),
        (SYM_INFO, "▸"),
        (SYM_WARNING, "⚠"),
        (SYM_BULLET, "·"),
        (SYM_ELLIPSIS, "…"),
    ],
)
def test_symbol_vocabulary_constants(symbol: str, expected: str) -> None:
    """Verify symbol vocabulary constants match expected glyphs."""
    assert symbol == expected


def test_console_success_contains_checkmark() -> None:
    """Verify ``success()`` outputs a checkmark and message."""
    vc, buf = _make_console()
    vc.success("build complete")
    output = buf.getvalue()
    assert "✓" in output
    assert "build complete" in output


def test_console_error_contains_cross() -> None:
    """Verify ``error()`` outputs a cross and message."""
    vc, buf = _make_console()
    vc.error("something failed")
    output = buf.getvalue()
    assert "✗" in output
    assert "something failed" in output


def test_console_error_with_hint() -> None:
    """Verify ``error()`` outputs both message and hint when provided."""
    vc, buf = _make_console()
    vc.error("something failed", hint="try this instead")
    output = buf.getvalue()
    assert "✗" in output
    assert "something failed" in output
    assert "try this instead" in output


def test_console_warning_contains_symbol() -> None:
    """Verify ``warning()`` outputs a warning symbol and message."""
    vc, buf = _make_console()
    vc.warning("careful here")
    output = buf.getvalue()
    assert "⚠" in output
    assert "careful here" in output


def test_console_info_contains_arrow() -> None:
    """Verify ``info()`` outputs an arrow symbol and message."""
    vc, buf = _make_console()
    vc.info("starting server")
    output = buf.getvalue()
    assert "▸" in output
    assert "starting server" in output


def test_console_step_contains_bullet() -> None:
    """Verify ``step()`` outputs a bullet and message."""
    vc, buf = _make_console()
    vc.step("loading config")
    output = buf.getvalue()
    assert "·" in output
    assert "loading config" in output


def test_console_blank_outputs_empty_line() -> None:
    """Verify ``blank()`` outputs an empty line."""
    vc, buf = _make_console()
    vc.blank()
    output = buf.getvalue()
    assert output.strip() == ""


def test_console_raw_passes_through() -> None:
    """Verify ``raw()`` passes output directly to the underlying console."""
    vc, buf = _make_console()
    vc.raw("hello world")
    output = buf.getvalue()
    assert "hello world" in output


def test_module_console_singleton_exists() -> None:
    """Verify the module-level ``console`` singleton is a ``VertebraConsole``."""
    assert isinstance(console, VertebraConsole)


def test_no_color_suppresses_ansi(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify ``NO_COLOR`` suppresses ANSI escape sequences."""
    monkeypatch.setenv("NO_COLOR", "1")
    buf = io.StringIO()
    vc = VertebraConsole(file=buf)
    vc.success("done")
    output = buf.getvalue()
    assert "✓" in output
    assert "done" in output
    assert "\x1b[" not in output


def test_no_color_property(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify ``no_color`` is ``True`` when ``NO_COLOR`` is set."""
    monkeypatch.setenv("NO_COLOR", "1")
    vc = VertebraConsole()
    assert vc.no_color is True


def test_color_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify ``no_color`` is ``False`` when ``NO_COLOR`` is unset."""
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    vc = VertebraConsole()
    assert vc.no_color is False


def test_register_startup_block() -> None:
    """Verify registering and retrieving a startup block."""
    register_startup_block("test-plugin", lambda: "hello")
    blocks = get_startup_blocks()
    assert len(blocks) == 1
    assert blocks[0][0] == "test-plugin"
    assert blocks[0][1]() == "hello"


def test_register_shutdown_block() -> None:
    """Verify registering and retrieving a shutdown block."""
    register_shutdown_block("cleanup", lambda: "done")
    blocks = get_shutdown_blocks()
    assert len(blocks) == 1
    assert blocks[0][0] == "cleanup"
    assert blocks[0][1]() == "done"


def test_multiple_blocks_preserve_order() -> None:
    """Verify registered startup blocks preserve insertion order."""
    register_startup_block("first", lambda: "1")
    register_startup_block("second", lambda: "2")
    register_startup_block("third", lambda: "3")
    blocks = get_startup_blocks()
    assert [name for name, _ in blocks] == ["first", "second", "third"]


def test_get_returns_copy() -> None:
    """Verify ``get_startup_blocks()`` returns a shallow copy."""
    register_startup_block("a", lambda: "x")
    blocks1 = get_startup_blocks()
    blocks2 = get_startup_blocks()
    assert blocks1 is not blocks2


def test_render_startup_blocks() -> None:
    """Verify rendering startup blocks outputs provider content."""
    register_startup_block("db", lambda: "PostgreSQL connected")
    vc, buf = _make_console()
    render_startup_blocks(vc)
    output = buf.getvalue()
    assert "db" in output
    assert "PostgreSQL connected" in output


def test_render_shutdown_blocks() -> None:
    """Verify rendering shutdown blocks outputs provider content."""
    register_shutdown_block("cache", lambda: "flushed")
    vc, buf = _make_console()
    render_shutdown_blocks(vc)
    output = buf.getvalue()
    assert "cache" in output
    assert "flushed" in output


def test_render_handles_exception_gracefully() -> None:
    """Verify failing block providers do not raise and log a fallback step."""

    def broken() -> str:
        msg = "kaboom"
        raise RuntimeError(msg)

    register_startup_block("broken-plugin", broken)
    vc, buf = _make_console()
    render_startup_blocks(vc)
    output = buf.getvalue()
    assert "broken-plugin" in output
    assert "failed to render" in output


def test_render_skips_empty_content() -> None:
    """Verify empty block content skips rendering the block header."""
    register_startup_block("empty", lambda: "")
    vc, buf = _make_console()
    render_startup_blocks(vc)
    output = buf.getvalue()
    assert "▸" not in output


def test_status_context_manager_exists() -> None:
    """Verify ``status()`` can be used as a context manager."""
    vc, _ = _make_console()
    with vc.status("working…"):
        pass


def test_status_non_tty_prints_message() -> None:
    """Verify ``status()`` prints a static message in non-TTY mode."""
    buf = io.StringIO()
    vc = VertebraConsole(file=buf, force_terminal=False)
    with vc.status("building…"):
        pass
    output = buf.getvalue()
    assert "building" in output
