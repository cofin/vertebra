"""CLI error handling utilities."""

from typing import NoReturn

import click

from vertebra.cli.theme import Console, console
from vertebra.typing import ImproperConfigurationError, MissingDependencyError, VertebraError

__all__ = (
    "CLIError",
    "ConfigResolverError",
    "EXIT_CONFIG_ERROR",
    "EXIT_INTERNAL_ERROR",
    "EXIT_SUCCESS",
    "EXIT_USER_ERROR",
    "ImproperConfigurationError",
    "MissingDependencyError",
    "VertebraError",
    "format_error",
    "print_cli_error",
    "raise_cli_error",
)

EXIT_SUCCESS = 0
EXIT_USER_ERROR = 1
EXIT_CONFIG_ERROR = 2
EXIT_INTERNAL_ERROR = 99


class CLIError(click.ClickException, VertebraError):
    """Vertebra CLI exception with Rich formatting support."""

    def __init__(self, message: str, *, hint: str | None = None, exit_code: int = EXIT_USER_ERROR) -> None:
        super().__init__(message)
        self.hint = hint
        self.__dict__["exit_code"] = exit_code

    def show(self, file: object = None) -> None:
        """Render the CLI error to the themed console."""
        target_console = Console(file=file) if file is not None else console
        target_console.error(self.message, hint=self.hint)


class ConfigResolverError(click.ClickException, VertebraError):
    """Raised when configuration resolution fails."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


def format_error(exc: Exception) -> tuple[str, str | None, int]:
    """Extract message, hint, and exit code from an exception."""
    message = str(exc)
    hint: str | None = None
    exit_code = EXIT_INTERNAL_ERROR

    if isinstance(exc, CLIError):
        hint = exc.hint
        exit_code = exc.exit_code
    elif isinstance(exc, MissingDependencyError):
        hint = exc.hint
        exit_code = EXIT_CONFIG_ERROR
    elif isinstance(exc, FileNotFoundError):
        exit_code = EXIT_CONFIG_ERROR
    elif isinstance(exc, (ValueError, TypeError)):
        exit_code = EXIT_USER_ERROR

    return message, hint, exit_code


def print_cli_error(exc: Exception) -> None:
    """Print a formatted error message to stderr."""
    if isinstance(exc, (CLIError, MissingDependencyError)):
        exc.show()
    else:
        message, hint, _ = format_error(exc)
        CLIError(message, hint=hint).show()


def raise_cli_error(exc: Exception) -> NoReturn:
    """Convert an exception to ``CLIError`` and raise it."""
    message, hint, exit_code = format_error(exc)
    raise CLIError(message, hint=hint, exit_code=exit_code) from exc
