"""Build command."""

import asyncio
import os
import sys
from pathlib import Path

import click


@click.command()
@click.argument("app_dir", type=click.Path(exists=True, file_okay=False), default=".")
@click.option("--one-file", is_flag=True, help="Build a standalone single-file executable")
@click.option("--target", help="Target platform (e.g., linux-x64, darwin-arm64)", required=True)
@click.option("--profile", type=click.Choice(["baseline", "modern"]), default="modern", help="Optimization profile")
@click.option("--output", "-o", type=click.Path(), help="Output path")
@click.option("--clean", is_flag=True, help="Clean output directory before building")
def build(app_dir: str, one_file: bool, target: str, profile: str, output: str | None, clean: bool) -> None:
    """Build Vertebra application artifacts."""
    if not one_file:
        click.secho("Only --one-file builds are currently supported.", fg="yellow")
        return

    output_path = Path(output) if output else Path("dist/standalone")
    project_path = Path(app_dir).resolve()

    click.echo(f"Building single-file executable for {project_path}")
    click.echo(f"  Target:  {target}")
    click.echo(f"  Profile: {profile}")
    click.echo(f"  Output:  {output_path}")

    try:
        from vertebra.build.bundler import Bundler

        bundler = Bundler(
            project_dir=project_path,
            output_dir=output_path,
            target=target,
            profile=profile,  # type: ignore
            clean=clean,
        )

        asyncio.run(bundler.build())

        click.secho("Build completed successfully.", fg="green")
    except Exception as e:
        click.secho(f"Build failed: {e}", fg="red", err=True)
        if os.environ.get("VERTEBRA_DEBUG"):
            raise
        sys.exit(1)
