"""Command line interface for Vertebra."""

import click

from vertebra.cli.config import configure_rich_click
from vertebra.cli.dev import dev as _dev_cmd
from vertebra.cli.init import init as _init_cmd
from vertebra.cli.js import js as _js_cmd
from vertebra.cli.mcp import mcp as _mcp_cmd
from vertebra.cli.run import run as _run_cmd
from vertebra.cli.workers import workers as _workers_cmd

__all__ = ("cli", "main", "vtb")

configure_rich_click()


@click.group()
@click.version_option(package_name="vertebra")
def cli() -> None:
    """Vertebra: High-performance polyglot application backbone.

    Python-first architecture with Rust IPC primitives.
    """


cli.add_command(_run_cmd)
cli.add_command(_dev_cmd)
cli.add_command(_js_cmd)
cli.add_command(_init_cmd)
cli.add_command(_mcp_cmd)
cli.add_command(_workers_cmd)

vtb = cli
main = cli

if __name__ == "__main__":
    cli()
