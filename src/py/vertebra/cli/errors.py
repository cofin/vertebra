"""CLI error handling utilities."""

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
    "ConfigResolverError",
    "ImproperConfigurationError",
    "MissingDependencyError",
    "error_console",
    "format_error",
    "print_cli_error",
    "raise_cli_error",
]

EXIT_SUCCESS = 0
EXIT_USER_ERROR = 1
EXIT_CONFIG_ERROR = 2
EXIT_INTERNAL_ERROR = 99

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


class MissingDependencyError(click.ClickException):
    """Raised when an optional dependency is required but unavailable."""

    def __init__(self, package: str, *, install_package: str | None = None) -> None:
        self.package = package
        self.install_package = install_package or package
        message = f"Optional dependency '{package}' is required but not installed."
        self.hint = f"Install it with: pip install {self.install_package}"
        super().__init__(message)

    def show(self, file: object = None) -> None:
        from rich.text import Text

        _ = file
        content = Text()
        content.append(str(self), style="bold")
        content.append("\n\n")
        content.append("Hint: ", style="dim")
        content.append(self.hint, style="dim italic")
        panel = Panel(content, title="Dependency Missing", title_align="left", border_style="yellow", padding=(0, 1))
        error_console.print(panel)


class ConfigResolverError(click.ClickException):
    """Raised when configuration resolution fails."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class ImproperConfigurationError(click.ClickException):
    """Raised when configuration is invalid."""

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
    """Convert an exception to CLIError and raise it."""
    message, hint, exit_code = format_error(exc)
    raise CLIError(message, hint=hint, exit_code=exit_code) from exc
