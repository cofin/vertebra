"""Tests for CLI error handling module."""

from vertebra.cli.errors import (
    EXIT_CONFIG_ERROR,
    EXIT_INTERNAL_ERROR,
    EXIT_SUCCESS,
    EXIT_USER_ERROR,
    CLIError,
    format_error,
)


class TestExitCodes:
    """Test exit code constants."""

    def test_exit_codes_are_distinct(self) -> None:
        """All exit codes should be distinct."""
        codes = [EXIT_SUCCESS, EXIT_USER_ERROR, EXIT_CONFIG_ERROR, EXIT_INTERNAL_ERROR]
        assert len(codes) == len(set(codes))

    def test_exit_success_is_zero(self) -> None:
        """Success exit code should be 0."""
        assert EXIT_SUCCESS == 0

    def test_exit_user_error_is_one(self) -> None:
        """User error exit code should be 1."""
        assert EXIT_USER_ERROR == 1


class TestCLIError:
    """Test CLIError exception class."""

    def test_cli_error_basic(self) -> None:
        """CLIError should store message and default exit code."""
        error = CLIError("Test error")
        assert error.message == "Test error"
        assert error.exit_code == EXIT_USER_ERROR
        assert error.hint is None

    def test_cli_error_with_hint(self) -> None:
        """CLIError should store hint when provided."""
        error = CLIError("Test error", hint="Try this instead")
        assert error.hint == "Try this instead"

    def test_cli_error_with_exit_code(self) -> None:
        """CLIError should use provided exit code."""
        error = CLIError("Test error", exit_code=EXIT_CONFIG_ERROR)
        assert error.exit_code == EXIT_CONFIG_ERROR

    def test_cli_error_is_click_exception(self) -> None:
        """CLIError should be a click.ClickException."""
        import click

        error = CLIError("Test error")
        assert isinstance(error, click.ClickException)


class TestFormatError:
    """Test error formatting function."""

    def test_format_file_not_found_error(self) -> None:
        """FileNotFoundError should map to config error with hint."""
        exc = FileNotFoundError("test.txt not found")
        message, hint, exit_code = format_error(exc)
        assert "test.txt not found" in message
        assert hint is not None
        assert "path" in hint.lower() or "permission" in hint.lower()
        assert exit_code == EXIT_CONFIG_ERROR

    def test_format_permission_error(self) -> None:
        """PermissionError should map to config error with hint."""
        exc = PermissionError("Access denied")
        message, hint, exit_code = format_error(exc)
        assert "Access denied" in message
        assert hint is not None
        assert exit_code == EXIT_CONFIG_ERROR

    def test_format_value_error(self) -> None:
        """ValueError should map to user error with hint."""
        exc = ValueError("Invalid input")
        message, hint, exit_code = format_error(exc)
        assert "Invalid input" in message
        assert hint is not None
        assert exit_code == EXIT_USER_ERROR

    def test_format_generic_exception(self) -> None:
        """Generic exceptions should map to internal error."""
        exc = RuntimeError("Something went wrong")
        message, _hint, exit_code = format_error(exc)
        assert "Something went wrong" in message
        assert exit_code == EXIT_INTERNAL_ERROR
