"""Init command for project scaffolding."""

import click

from vertebra.cli.theme import console

__all__ = ("init",)


@click.command()
@click.argument("name")
@click.option("--template", default="default", help="Project template: default, web")
def init(name: str, template: str) -> None:
    """Initialize a new Vertebra project."""
    console.info(f"Initializing project '{name}' with template '{template}'…")
