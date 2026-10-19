"""Rich-click configuration for Vertebra CLI styling.

Imported by ``vertebra.cli.__init__``.
"""

import rich_click as click

from vertebra.cli.theme import ACCENT, SUCCESS, WARNING

__all__ = ("configure_rich_click",)


def configure_rich_click() -> None:
    """Apply Vertebra styling defaults to rich-click."""
    click.rich_click.USE_RICH_MARKUP = True
    click.rich_click.USE_MARKDOWN = True
    click.rich_click.SHOW_ARGUMENTS = True
    click.rich_click.WIDTH = 120
    click.rich_click.MAX_WIDTH = 120
    click.rich_click.STYLE_COMMANDS_TABLE_COLUMN_WIDTH_RATIO = (1, 3)
    click.rich_click.STYLE_OPTION = ACCENT
    click.rich_click.STYLE_SWITCH = SUCCESS
    click.rich_click.STYLE_ARGUMENT = WARNING
    click.rich_click.STYLE_HEADER_TEXT = "bold"
    click.rich_click.STYLE_ERRORS_SUGGESTION = "dim italic"
    click.rich_click.ERRORS_SUGGESTION = "Try 'vtb --help' for more information."
