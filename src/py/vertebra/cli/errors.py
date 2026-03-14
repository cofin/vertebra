"""CLI error handling utilities."""

from __future__ import annotations

from typing import NoReturn

import click
from rich.console import Console
from rich.panel import Panel

__all__ = [
    "EXIT_CONFIG_ERROR",
    "EXIT_INTERNAL_ERROR",
    "EXIT_SUCCESS",
    "EXIT_USER_ERROR",
    "CLIError",
    "error_console",
    "format_error",
    "print_cli_error",
    "raise_cli_error",
]

EXIT_SUCCESS = 0
EXIT_USER_ERROR = 1
EXIT_CONFIG_ERROR = 2
EXIT_INTERNAL_ERROR = 3

error_console = Console(stderr=True)


class CLIError(click.ClickException):
    """Vertebra CLI exception with Rich formatting support."""

    def __init__(self, message: str, *, hint: str | None = None, exit_code: int = EXIT_USER_ERROR) -> None:
        super().__init__(message)
        self.hint = hint
        self.exit_code = exit_code

    def show(self, file: object = None) -> None:
        from rich.text import Text

        _ = file
        content = Text()
        content.append(self.message, style="bold")
        if self.hint:
            content.append("\n\n")
            content.append("Hint: ", style="dim")
            content.append(self.hint, style="dim italic")
        panel = Panel(content, title="Error", title_align="left", border_style="red", padding=(0, 1))
        error_console.print(panel)


def format_error(exc: Exception) -> tuple[str, str | None, int]:
    """Extract message, hint, and exit code from an exception."""
    message = str(exc)
    hint: str | None = None
    exit_code = EXIT_INTERNAL_ERROR
    if isinstance(exc, (FileNotFoundError, PermissionError, ModuleNotFoundError, ImportError)):
        exit_code = EXIT_CONFIG_ERROR
    elif isinstance(exc, (ValueError, TypeError, KeyError)):
        exit_code = EXIT_USER_ERROR
    return message, hint, exit_code


def print_cli_error(message: str, *, hint: str | None = None) -> None:
    """Print a formatted error message to stderr."""
    error = CLIError(message, hint=hint)
    error.show()


def raise_cli_error(exc: Exception) -> NoReturn:
    """Convert an exception to CLIError and raise it."""
    message, hint, exit_code = format_error(exc)
    raise CLIError(message, hint=hint, exit_code=exit_code) from exc
