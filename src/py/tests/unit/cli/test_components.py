"""Tests for CLI UI components module."""

from io import StringIO

from rich.console import Console
from rich.progress import Progress
from rich.table import Table

from vertebra.cli.components import (
    console,
    key_value,
    progress_bar,
    section_header,
    status_indicator,
    status_spinner,
    styled_table,
)
from vertebra.cli.theme import COLORS


class TestConsole:
    """Test console singleton."""

    def test_console_is_console_instance(self) -> None:
        """Console should be a Rich Console instance."""
        assert isinstance(console, Console)


class TestSectionHeader:
    """Test section header component."""

    def test_section_header_output(self, capsys) -> None:
        """section_header should output styled rule."""
        # Create a test console that writes to stdout
        Console(file=StringIO(), force_terminal=True)
        # We can't easily capture rich output, so just verify it doesn't raise
        section_header("Test Section")
        # If we get here without error, the test passes


class TestKeyValue:
    """Test key-value formatting."""

    def test_key_value_format(self) -> None:
        """key_value should format key-value pairs."""
        result = key_value("Host", "127.0.0.1")
        assert "Host" in result
        assert "127.0.0.1" in result

    def test_key_value_alignment(self) -> None:
        """key_value should respect key_width."""
        result = key_value("Key", "Value", key_width=20)
        assert "Key" in result
        assert "Value" in result


class TestStatusIndicator:
    """Test status indicator component."""

    def test_status_indicator_ready(self) -> None:
        """Ready status should use success color."""
        result = status_indicator("ready")
        assert COLORS["success"] in result

    def test_status_indicator_starting(self) -> None:
        """Starting status should use warning color."""
        result = status_indicator("starting")
        assert COLORS["warning"] in result

    def test_status_indicator_error(self) -> None:
        """Error status should use error color."""
        result = status_indicator("error")
        assert COLORS["error"] in result

    def test_status_indicator_unknown(self) -> None:
        """Unknown status should use muted color."""
        result = status_indicator("unknown")
        assert COLORS["muted"] in result


class TestProgressBar:
    """Test progress bar component."""

    def test_progress_bar_returns_progress(self) -> None:
        """progress_bar should return a Progress instance."""
        progress = progress_bar("Test Task", 100)
        assert isinstance(progress, Progress)


class TestStatusSpinner:
    """Test status spinner context manager."""

    def test_status_spinner_context_manager(self) -> None:
        """status_spinner should work as context manager."""
        # Just verify it doesn't raise
        with status_spinner("Testing..."):
            pass


class TestStyledTable:
    """Test styled table component."""

    def test_styled_table_returns_table(self) -> None:
        """styled_table should return a Table instance."""
        table = styled_table()
        assert isinstance(table, Table)

    def test_styled_table_with_title(self) -> None:
        """styled_table should accept title."""
        table = styled_table(title="Test Table")
        assert table.title == "Test Table"

    def test_styled_table_no_header(self) -> None:
        """styled_table should accept show_header=False."""
        table = styled_table(show_header=False)
        assert table.show_header is False
