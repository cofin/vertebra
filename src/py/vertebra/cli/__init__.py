"""Command line interface for Vertebra."""

import click

# Import config module for side effects (rich-click configuration)
from vertebra.cli import config as _config

from .build import build
from .init import init
from .run import run

__all__ = ["cli", "main"]


@click.group()
@click.version_option(package_name="vertebra")  # type: ignore[call-arg]
def cli() -> None:
    """Vertebra: High-performance polyglot application backbone.

    Python-first architecture with Rust IPC and Bun server layer.
    """


cli.add_command(run)
cli.add_command(build)
cli.add_command(init)

# Alias for backwards compatibility
main = cli

if __name__ == "__main__":
    cli()
