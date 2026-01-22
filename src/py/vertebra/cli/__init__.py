"""Command line interface for Vertebra."""

import click
import rich_click

from .build import build
from .init import init
from .serve import serve

# Configure rich-click
rich_click.rich_click.USE_RICH_MARKUP = True
rich_click.rich_click.SHOW_ARGUMENTS = True
rich_click.rich_click.GROUP_ARGUMENTS_OPTIONS = True


@click.group()
@click.version_option(package_name="vertebra")
def main() -> None:
    """Vertebra: High-performance polyglot application backbone."""


main.add_command(serve)
main.add_command(build)
main.add_command(init)

if __name__ == "__main__":
    main()
