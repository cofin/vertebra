"""Rich-click configuration for Vertebra CLI.

This module configures rich-click styling using the Vertebra color palette.
Import this module for side effects before defining CLI commands.
"""

from __future__ import annotations

import rich_click as click

from vertebra.cli.theme import COLORS

# Enable rich features
click.rich_click.USE_RICH_MARKUP = True
click.rich_click.USE_MARKDOWN = True

# Style definitions using Vertebra brand colors
click.rich_click.STYLE_COMMAND = f"bold {COLORS['slate']}"
click.rich_click.STYLE_OPTION = f"bold {COLORS['success']}"
click.rich_click.STYLE_SWITCH = f"bold {COLORS['success']}"
click.rich_click.STYLE_ARGUMENT = "bold cyan"
click.rich_click.STYLE_METAVAR = COLORS["gold"]

click.rich_click.STYLE_USAGE = "bold"
click.rich_click.STYLE_USAGE_COMMAND = f"bold {COLORS['slate']}"
click.rich_click.STYLE_HELPTEXT = "dim"
click.rich_click.STYLE_HELPTEXT_FIRST_LINE = ""
click.rich_click.STYLE_OPTION_HELP = ""

click.rich_click.STYLE_ERRORS_SUGGESTION = f"italic {COLORS['warning']}"
click.rich_click.STYLE_REQUIRED_SHORT = COLORS["error"]
click.rich_click.STYLE_REQUIRED_LONG = f"dim {COLORS['error']}"
click.rich_click.STYLE_ERRORS_PANEL_BORDER = COLORS["error"]
click.rich_click.STYLE_ABORTED = COLORS["error"]

click.rich_click.STYLE_OPTIONS_PANEL_BORDER = f"dim {COLORS['slate']}"
click.rich_click.STYLE_COMMANDS_PANEL_BORDER = f"dim {COLORS['slate']}"

# Layout constraints
click.rich_click.WIDTH = 120
click.rich_click.MAX_WIDTH = 120
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
click.rich_click.SHOW_METAVARS_COLUMN = True
click.rich_click.APPEND_METAVARS_HELP = False

# Command groups for organized help output
click.rich_click.COMMAND_GROUPS = {
    "vertebra": [
        {"name": "Server", "commands": ["run"]},
        {"name": "Build", "commands": ["build"]},
        {"name": "Project", "commands": ["init"]},
    ],
    "vtb": [
        {"name": "Server", "commands": ["run"]},
        {"name": "Build", "commands": ["build"]},
        {"name": "Project", "commands": ["init"]},
    ],
}
