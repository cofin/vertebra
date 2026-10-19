"""Tests for the Vertebra CLI banner."""

from __future__ import annotations

import io

import pytest

from vertebra.cli import theme
from vertebra.cli.banner.logos import LOGO_COMPACT, LOGO_FULL, get_logo
from vertebra.cli.banner.renderer import render_banner, render_shutdown
from vertebra.cli.theme import Console


def _make_tty_console() -> tuple[Console, io.StringIO]:
    """Create a TTY-enabled ``Console`` backed by ``StringIO``."""
    buf = io.StringIO()
    return Console(file=buf, force_terminal=True), buf


def _make_pipe_console() -> tuple[Console, io.StringIO]:
    """Create a non-TTY ``Console`` backed by ``StringIO``."""
    buf = io.StringIO()
    return Console(file=buf, force_terminal=False), buf


def test_full_logo_is_two_lines() -> None:
    """Verify the full logo spans two lines."""
    assert len(LOGO_FULL.splitlines()) == 2


def test_compact_logo_is_single_line() -> None:
    """Verify the compact logo is a single line."""
    assert "\n" not in LOGO_COMPACT
    assert LOGO_COMPACT == "▜▄▛"


@pytest.mark.parametrize(
    ("width", "expected"),
    [
        (120, LOGO_FULL),
        (80, LOGO_FULL),
        (79, LOGO_COMPACT),
        (60, LOGO_COMPACT),
    ],
)
def test_get_logo_by_width(width: int, expected: str) -> None:
    """Verify ``get_logo()`` selects full vs compact logo at the 80-column threshold."""
    assert get_logo(width) == expected


def test_banner_includes_version() -> None:
    """Verify ``render_banner()`` includes the version string."""
    vc, buf = _make_tty_console()
    render_banner(vc=vc)
    output = buf.getvalue()
    assert "v" in output


def test_banner_includes_mode() -> None:
    """Verify ``render_banner()`` includes the operating mode."""
    vc, buf = _make_tty_console()
    render_banner(mode="dev", vc=vc)
    output = buf.getvalue()
    assert "dev" in output


def test_banner_includes_host_port() -> None:
    """Verify ``render_banner()`` includes the bound host and port."""
    vc, buf = _make_tty_console()
    render_banner(host="0.0.0.0", port=3000, vc=vc)
    output = buf.getvalue()
    assert "0.0.0.0:3000" in output


def test_banner_includes_python_version() -> None:
    """Verify ``render_banner()`` includes the Python runtime version."""
    vc, buf = _make_tty_console()
    render_banner(vc=vc)
    output = buf.getvalue()
    assert "Python" in output


def test_banner_skips_non_tty() -> None:
    """Verify ``render_banner()`` produces no output in non-TTY mode."""
    vc, buf = _make_pipe_console()
    render_banner(vc=vc)
    output = buf.getvalue()
    assert output == ""


def test_banner_shows_worker_count() -> None:
    """Verify ``render_banner()`` pluralizes worker count when greater than 1."""
    vc, buf = _make_tty_console()
    render_banner(workers=4, vc=vc)
    output = buf.getvalue()
    assert "4 workers" in output


def test_banner_singular_worker() -> None:
    """Verify ``render_banner()`` uses singular worker label when count is 1."""
    vc, buf = _make_tty_console()
    render_banner(workers=1, vc=vc)
    output = buf.getvalue()
    assert "1 worker" in output
    assert "1 workers" not in output


def test_shutdown_includes_message() -> None:
    """Verify ``render_shutdown()`` prints shutdown progress and completion."""
    vc, buf = _make_tty_console()
    render_shutdown(vc=vc)
    output = buf.getvalue()
    assert "shutting down" in output
    assert "✓" in output
    assert "shutdown complete" in output


def test_shutdown_skips_non_tty() -> None:
    """Verify ``render_shutdown()`` produces no output in non-TTY mode."""
    vc, buf = _make_pipe_console()
    render_shutdown(vc=vc)
    output = buf.getvalue()
    assert output == ""


def test_shutdown_renders_plugin_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify ``render_shutdown()`` renders registered shutdown blocks."""
    monkeypatch.setattr(theme, "_SHUTDOWN_BLOCKS", [("test-db", lambda: "connections closed")])
    vc, buf = _make_tty_console()
    render_shutdown(vc=vc)
    output = buf.getvalue()
    assert "test-db" in output
    assert "connections closed" in output
