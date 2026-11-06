"""Centralized theme module for Vertebra CLI.

Single source of truth for colors, symbols, and styled console output.
All CLI output should go through the ``console`` singleton or ``Console``.
"""

import os
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

from rich.console import Console as RichConsole
from rich.console import RenderableType

__all__ = (
    "ACCENT",
    "Console",
    "ContentProvider",
    "ERROR",
    "MUTED",
    "SECONDARY",
    "SUCCESS",
    "SYM_BULLET",
    "SYM_ELLIPSIS",
    "SYM_ERROR",
    "SYM_INFO",
    "SYM_SUCCESS",
    "SYM_WARNING",
    "WARNING",
    "console",
    "get_shutdown_blocks",
    "get_startup_blocks",
    "register_shutdown_block",
    "register_startup_block",
    "render_shutdown_blocks",
    "render_startup_blocks",
)

ACCENT = "cyan"
SUCCESS = "green"
WARNING = "yellow"
ERROR = "red"
SECONDARY = "dim"
MUTED = "dim italic"

SYM_SUCCESS = "✓"
SYM_ERROR = "✗"
SYM_INFO = "▸"
SYM_WARNING = "⚠"
SYM_BULLET = "·"
SYM_ELLIPSIS = "…"

ContentProvider = Callable[[], str | RenderableType]


class Console:
    """Semantic console wrapper for Vertebra CLI output.

    Wraps ``rich.console.Console`` with semantic methods (``success``, ``error``, etc.)
    that produce consistent, symbol-driven output. All output goes to stderr
    to keep stdout clean for piping.
    """

    __slots__ = ("_console",)

    def __init__(
        self,
        *,
        file: Any = None,
        force_terminal: bool | None = None,
    ) -> None:
        no_color = os.environ.get("NO_COLOR")
        if no_color is not None:
            self._console = RichConsole(
                file=file,
                stderr=file is None,
                no_color=True,
                force_terminal=force_terminal,
                highlight=False,
            )
        else:
            self._console = RichConsole(
                file=file,
                stderr=file is None,
                force_terminal=force_terminal,
                highlight=False,
            )

    @property
    def is_tty(self) -> bool:
        """Return whether the output target is an interactive terminal."""
        return self._console.is_terminal

    @property
    def no_color(self) -> bool:
        """Return whether color output is disabled via the ``NO_COLOR`` env var."""
        return os.environ.get("NO_COLOR") is not None

    def success(self, msg: str) -> None:
        """Print a success message in green."""
        self._console.print(f"  [{SUCCESS}]{SYM_SUCCESS}[/{SUCCESS}] {msg}")

    def error(self, msg: str, *, hint: str | None = None) -> None:
        """Print an error message in red with an optional muted hint."""
        self._console.print(f"  [{ERROR}]{SYM_ERROR}[/{ERROR}] [{ERROR}]{msg}[/{ERROR}]")
        if hint:
            self._console.print(f"    [{MUTED}]{hint}[/{MUTED}]")

    def warning(self, msg: str) -> None:
        """Print a warning message in yellow."""
        self._console.print(f"  [{WARNING}]{SYM_WARNING}[/{WARNING}] {msg}")

    def info(self, msg: str) -> None:
        """Print an informational message in cyan."""
        self._console.print(f"  [{ACCENT}]{SYM_INFO}[/{ACCENT}] {msg}")

    def step(self, msg: str) -> None:
        """Print a secondary step/detail message in dim style."""
        self._console.print(f"    [{SECONDARY}]{SYM_BULLET} {msg}[/{SECONDARY}]")

    def blank(self) -> None:
        """Print an empty line."""
        self._console.print()

    def raw(self, *args: Any, **kwargs: Any) -> None:
        """Pass-through to the underlying ``RichConsole.print``."""
        self._console.print(*args, **kwargs)

    def status(self, msg: str) -> AbstractContextManager[Any]:
        """Return a context manager showing a spinner in TTY or a static message otherwise."""
        if self._console.is_terminal:
            return self._console.status(
                f"  [{ACCENT}]{SYM_ELLIPSIS}[/{ACCENT}] {msg}",
                spinner="dots",
            )
        self._console.print(f"  [{ACCENT}]{SYM_ELLIPSIS}[/{ACCENT}] {msg}")
        return _NoopContext()


def register_startup_block(name: str, provider: Callable[[], Any]) -> None:
    """Register a content provider for the startup banner."""
    _STARTUP_BLOCKS.append((name, provider))


def register_shutdown_block(name: str, provider: Callable[[], Any]) -> None:
    """Register a content provider for the shutdown display."""
    _SHUTDOWN_BLOCKS.append((name, provider))


def get_startup_blocks() -> list[tuple[str, Callable[[], Any]]]:
    """Return a copy of all registered startup content providers."""
    return list(_STARTUP_BLOCKS)


def get_shutdown_blocks() -> list[tuple[str, Callable[[], Any]]]:
    """Return a copy of all registered shutdown content providers."""
    return list(_SHUTDOWN_BLOCKS)


def render_startup_blocks(vc: Console | None = None) -> None:
    """Render all registered startup content blocks."""
    target = vc or console
    for name, provider in _STARTUP_BLOCKS:
        try:
            content = provider()
            if content:
                target.info(name)
                target.raw(f"    {content}")
        except Exception:
            target.step(f"{name}: failed to render")


def render_shutdown_blocks(vc: Console | None = None) -> None:
    """Render all registered shutdown content blocks."""
    target = vc or console
    for name, provider in _SHUTDOWN_BLOCKS:
        try:
            content = provider()
            if content:
                target.step(f"{name}: {content}")
        except Exception:
            target.step(f"{name}: failed to render")


class _NoopContext:
    """Minimal context manager that performs no action."""

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, *args: Any) -> None:
        pass


_STARTUP_BLOCKS: list[tuple[str, Callable[[], Any]]] = []
_SHUTDOWN_BLOCKS: list[tuple[str, Callable[[], Any]]] = []
console = Console()
