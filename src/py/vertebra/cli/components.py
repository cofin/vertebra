"""Reusable UI components for Vertebra CLI.

Provides styled components for consistent CLI output across all commands:
- section_header: Styled rule with left-aligned title
- key_value: Formatted key-value pair
- status_indicator: Colored status circle
- progress_bar: Pre-configured Rich progress bar
- status_spinner: Context manager for spinners during operations
- styled_table: Pre-configured Rich table
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.rule import Rule
from rich.table import Table

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = [
    "console",
    "key_value",
    "progress_bar",
    "section_header",
    "status_indicator",
    "status_spinner",
    "styled_table",
]

# Shared console instance
console = Console()


def section_header(title: str, style: str | None = None) -> None:
    """Render a styled section header.

    Creates a left-aligned rule with the given title.

    Args:
        title: The section title to display.
        style: Optional style override. Defaults to slate (primary color).
    """
    from vertebra.cli.theme import COLORS

    rule_style = style or COLORS["slate"]
    rule = Rule(title, style=rule_style, align="left")
    console.print(rule)


def key_value(key: str, value: str, key_width: int = 15) -> str:
    """Format a key-value pair with consistent alignment.

    Args:
        key: The label/key to display.
        value: The value to display.
        key_width: Width for the key column (default: 15).

    Returns:
        Formatted string with arrow prefix and aligned columns.
    """
    from vertebra.cli.theme import CHARS, COLORS

    arrow = CHARS.get("arrow", ">")
    return f"  [{COLORS['dim']}]{arrow}[/] {key:<{key_width}} {value}"


def status_indicator(status: str) -> str:
    """Return a colored status indicator character.

    Maps status strings to appropriate colors:
    - ready/connected/healthy -> green
    - starting/degraded/warning -> yellow
    - error/failed/disconnected -> red
    - other -> gray

    Args:
        status: Status string to map.

    Returns:
        Rich markup string with colored status indicator.
    """
    from vertebra.cli.theme import CHARS, COLORS, get_status_color

    color_name = get_status_color(status)
    color = COLORS.get(color_name, COLORS["muted"])
    indicator = CHARS.get("status_ready", "*")
    return f"[{color}]{indicator}[/{color}]"


def progress_bar(task_name: str, total: int, *, transient: bool = True) -> Progress:
    """Create a pre-configured Rich progress bar.

    Args:
        task_name: Description of the task being tracked.
        total: Total number of steps.
        transient: Whether to remove the progress bar when complete (default: True).

    Returns:
        Configured Progress instance. Use as context manager.
    """
    from vertebra.cli.theme import COLORS

    progress = Progress(
        SpinnerColumn(),
        TextColumn(f"[{COLORS['text']}]{{task.description}}[/]"),
        BarColumn(complete_style=COLORS["success"], finished_style=COLORS["success"]),
        TextColumn(f"[{COLORS['muted']}]{{task.percentage:>3.0f}}%[/]"),
        TimeElapsedColumn(),
        console=console,
        transient=transient,
    )
    progress.add_task(task_name, total=total)
    return progress


@contextmanager
def status_spinner(message: str) -> Iterator[None]:
    """Context manager for displaying a spinner during operations.

    Args:
        message: Status message to display alongside spinner.

    Yields:
        None. Use in a with statement.
    """
    from vertebra.cli.theme import COLORS

    with console.status(f"[{COLORS['warning']}]{message}[/]", spinner="dots"):
        yield


def styled_table(title: str | None = None, *, show_header: bool = True) -> Table:
    """Create a pre-configured Rich table with Vertebra styling.

    Args:
        title: Optional table title.
        show_header: Whether to show column headers (default: True).

    Returns:
        Configured Table instance. Add columns and rows as needed.
    """
    from vertebra.cli.theme import COLORS

    return Table(
        title=title,
        show_header=show_header,
        header_style=f"bold {COLORS['slate']}",
        border_style=f"dim {COLORS['slate']}",
        title_style=f"bold {COLORS['text']}",
        padding=(0, 1),
    )
