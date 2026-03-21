"""Command line interface for Vertebra."""

import click

from vertebra.cli import config as _config  # noqa: F401
from vertebra.cli.build import build
from vertebra.cli.dev import dev
from vertebra.cli.init import init
from vertebra.cli.js import js
from vertebra.cli.run import run

__all__ = ["cli", "main"]


@click.group()
@click.version_option(package_name="vertebra")
def cli() -> None:
    """Vertebra: High-performance polyglot application backbone.

    Python-first architecture with Rust IPC primitives.
    """


cli.add_command(run)
cli.add_command(dev)
cli.add_command(js)
cli.add_command(init)
cli.add_command(build)

vtb = cli
main = cli

if __name__ == "__main__":
    cli()
