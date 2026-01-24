"""CLI error handling utilities for clean, professional error output.

Provides a CLIError exception class with Rich formatting support,
exit code constants, and utility functions for error handling.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn

import rich_click as click
from rich.console import Console
from rich.panel import Panel

if TYPE_CHECKING:
    from rich.console import RenderableType

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

# Exit codes
EXIT_SUCCESS = 0
EXIT_USER_ERROR = 1  # User input/validation errors (recoverable by user)
EXIT_CONFIG_ERROR = 2  # Configuration/setup errors (requires system changes)
EXIT_INTERNAL_ERROR = 3  # Unexpected internal errors (report as bug)

# Console for stderr output
error_console = Console(stderr=True)


def _get_hint_for_exception(exc: Exception) -> str | None:
    """Get a helpful hint message based on exception type."""
    hint_map: dict[type[Exception], str] = {
        FileNotFoundError: "Check that the file path exists and you have read permissions.",
        PermissionError: "Run with elevated permissions or check file/directory ownership.",
        ConnectionError: "Verify the server is running and the address/port are correct.",
        TimeoutError: "The operation timed out. Try again or increase timeout settings.",
        ValueError: "Check the input format matches the expected type.",
        ModuleNotFoundError: "Ensure the module is installed and the import path is correct.",
        ImportError: "Check that all required dependencies are installed.",
    }

    for exc_type, hint in hint_map.items():
        if isinstance(exc, exc_type):
            return hint
    return None


def _get_exit_code_for_exception(exc: Exception) -> int:
    """Get appropriate exit code based on exception type."""
    config_errors = (FileNotFoundError, PermissionError, ModuleNotFoundError, ImportError)
    user_errors = (ValueError, TypeError, KeyError)

    if isinstance(exc, config_errors):
        return EXIT_CONFIG_ERROR
    if isinstance(exc, user_errors):
        return EXIT_USER_ERROR
    return EXIT_INTERNAL_ERROR


class CLIError(click.ClickException):
    """Vertebra CLI exception with Rich formatting support.

    Provides consistent error display with styled panels and optional hints.
    """

    def __init__(self, message: str, *, hint: str | None = None, exit_code: int = EXIT_USER_ERROR) -> None:
        """Initialize CLI error.

        Args:
            message: The error message to display.
            hint: Optional helpful hint for the user.
            exit_code: Exit code to use (default: 1).
        """
        super().__init__(message)
        self.hint = hint
        self.exit_code = exit_code

    def _build_content(self) -> RenderableType:
        """Build the Rich content for the error panel."""
        from rich.text import Text

        content = Text()
        content.append(self.message, style="bold")

        if self.hint:
            content.append("\n\n")
            content.append("Hint: ", style="dim")
            content.append(self.hint, style="dim italic")

        return content

    def show(self, file: object = None) -> None:
        """Display the error using Rich formatting.

        Overrides Click's default show() to use Rich Panel.

        Args:
            file: Unused, kept for Click compatibility.
        """
        from vertebra.cli.theme import COLORS

        _ = file  # Unused, Rich uses stderr Console instead
        panel = Panel(
            self._build_content(), title="Error", title_align="left", border_style=COLORS["error"], padding=(0, 1)
        )
        error_console.print(panel)


def format_error(exc: Exception) -> tuple[str, str | None, int]:
    """Extract message, hint, and exit code from an exception.

    Args:
        exc: The exception to format.

    Returns:
        Tuple of (message, hint, exit_code).
    """
    message = str(exc)
    hint = _get_hint_for_exception(exc)
    exit_code = _get_exit_code_for_exception(exc)
    return message, hint, exit_code


def print_cli_error(message: str, *, hint: str | None = None) -> None:
    """Print a formatted error message to stderr.

    Utility function for displaying errors outside of Click's exception handling.

    Args:
        message: The error message to display.
        hint: Optional helpful hint for the user.
    """
    error = CLIError(message, hint=hint)
    error.show()


def raise_cli_error(exc: Exception) -> NoReturn:
    """Convert an exception to CLIError and raise it.

    Args:
        exc: The exception to convert.

    Raises:
        CLIError: Always raises with formatted error details.
    """
    message, hint, exit_code = format_error(exc)
    raise CLIError(message, hint=hint, exit_code=exit_code) from exc
