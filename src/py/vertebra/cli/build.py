"""Build command."""

from __future__ import annotations

import click


@click.command()
@click.option("--target", default="frontend", help="Build target: frontend, backend, all")
@click.option("--profile", default="release", help="Build profile: debug, release")
def build(target: str, profile: str) -> None:
    """Build Vertebra application assets."""
    click.echo(f"Building {target} ({profile})...")
