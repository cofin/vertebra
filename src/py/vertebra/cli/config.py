"""Rich-click configuration for Vertebra CLI styling."""

import rich_click as click

click.rich_click.USE_RICH_MARKUP = True
click.rich_click.USE_MARKDOWN = True
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.WIDTH = 120
click.rich_click.MAX_WIDTH = 120
click.rich_click.STYLE_COMMANDS_TABLE_COLUMN_WIDTH_RATIO = (1, 3)
click.rich_click.STYLE_OPTION = "bold cyan"
click.rich_click.STYLE_SWITCH = "bold green"
click.rich_click.STYLE_ARGUMENT = "bold yellow"
click.rich_click.STYLE_ERRORS_SUGGESTION = "dim"
