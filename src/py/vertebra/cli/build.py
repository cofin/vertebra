"""Build command."""

import asyncio
import os
from pathlib import Path

import click

from vertebra.cli.components import console, section_header
from vertebra.cli.errors import EXIT_INTERNAL_ERROR, CLIError
from vertebra.cli.theme import COLORS


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
        console.print(f"[{COLORS['warning']}]Only --one-file builds are currently supported.[/]")
        return

    output_path = Path(output) if output else Path("dist/standalone")
    project_path = Path(app_dir).resolve()

    section_header("Build Configuration")
    console.print(f"  Project: {project_path}")
    console.print(f"  Target:  {target}")
    console.print(f"  Profile: {profile}")
    console.print(f"  Output:  {output_path}")
    console.print()

    try:
        from vertebra.build.bundler import Bundler

        bundler = Bundler(
            project_dir=project_path,
            output_dir=output_path,
            target=target,
            profile=profile,  # type: ignore[arg-type]
            clean=clean,
        )

        asyncio.run(bundler.build())

        console.print(f"[{COLORS['success']}]Build completed successfully.[/]")
    except Exception as e:
        if os.environ.get("VERTEBRA_DEBUG"):
            raise
        msg = f"Build failed: {e}"
        raise CLIError(msg, hint="Set VERTEBRA_DEBUG=1 for full traceback.", exit_code=EXIT_INTERNAL_ERROR) from e
