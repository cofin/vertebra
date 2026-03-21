"""Init command for project scaffolding."""

import click


@click.command()
@click.argument("name")
@click.option("--template", default="default", help="Project template: default, web")
def init(name: str, template: str) -> None:
    """Initialize a new Vertebra project."""
    click.echo(f"Initializing project '{name}' with template '{template}'...")
