"""Build command."""

import asyncio
import os
import subprocess
from pathlib import Path

import click

from vertebra.cli.components import console, section_header
from vertebra.cli.errors import EXIT_INTERNAL_ERROR, CLIError
from vertebra.cli.theme import COLORS


@click.command()
@click.argument("app_dir", type=click.Path(exists=True, file_okay=False), default=".")
@click.option("--onefile", is_flag=True, help="Build a standalone single-file executable")
@click.option("--target", help="Target platform (e.g., linux-x64, darwin-arm64)")
@click.option("--profile", type=click.Choice(["baseline", "modern"]), default="modern", help="Optimization profile")
@click.option("--output", "-o", type=click.Path(), help="Output path")
@click.option("--clean", is_flag=True, help="Clean output directory before building")
def build(app_dir: str, onefile: bool, target: str | None, profile: str, output: str | None, clean: bool) -> None:
    """Build Vertebra application artifacts."""
    project_path = Path(app_dir).resolve()
    
    # 1. Build Frontend if present
    package_json = project_path / "package.json"
    if package_json.exists():
        section_header("Building Frontend")
        try:
            # Install deps if needed (basic check)
            if not (project_path / "node_modules").exists():
                console.print(f"[{COLORS['info']}]Installing frontend dependencies...[/]")
                subprocess.run(["npm", "install"], cwd=project_path, check=True)
            
            console.print(f"[{COLORS['info']}]Building frontend assets...[/]")
            subprocess.run(["npm", "run", "build"], cwd=project_path, check=True)
            console.print(f"[{COLORS['success']}]Frontend build complete.[/]")
        except Exception as e:
            console.print(f"[{COLORS['error']}]Frontend build failed: {e}[/]")
            # Don't fail the whole build? Or should we?
            # For now, let's fail to ensure correctness.
            raise CLIError(str(e), exit_code=1) from e

    # 2. Build Backend (Standalone)
    if onefile:
        if not target:
             console.print(f"[{COLORS['error']}]--target is required for --onefile builds[/]")
             return

        output_path = Path(output) if output else Path("dist/standalone")

        section_header("Build Configuration")
        console.print(f"  Project: {project_path}")
        console.print(f"  Target:  {target}")
        console.print(f"  Profile: {profile}")
        console.print(f"  Output:  {output_path}")
        console.print()

        try:
            from vertebra.core.build.onefile import OneFileBuilder

            builder = OneFileBuilder(
                project_dir=project_path,
                output_path=output_path,
                target=target,
                profile=profile,
            )

            asyncio.run(builder.build())

            console.print(f"[{COLORS['success']}]Standalone build completed successfully.[/]")
        except Exception as e:
            if os.environ.get("VERTEBRA_DEBUG"):
                raise
            msg = f"Build failed: {e}"
            raise CLIError(msg, hint="Set VERTEBRA_DEBUG=1 for full traceback.", exit_code=EXIT_INTERNAL_ERROR) from e
    else:
        # Distribution directory build
        output_path = Path(output) if output else Path("dist/distribution")
        target_platform = target if target else "native"

        section_header("Build Configuration (Directory)")
        console.print(f"  Project: {project_path}")
        console.print(f"  Target:  {target_platform}")
        console.print(f"  Profile: {profile}")
        console.print(f"  Output:  {output_path}")
        console.print()

        try:
            from vertebra.core.build.bundler import Bundler

            bundler = Bundler(
                project_dir=project_path,
                output_dir=output_path,
                target=target_platform,
                profile=profile,  # type: ignore[arg-type]
                clean=clean,
            )

            asyncio.run(bundler.build())

            console.print(f"[{COLORS['success']}]Distribution build completed successfully.[/]")
        except Exception as e:
            if os.environ.get("VERTEBRA_DEBUG"):
                raise
            msg = f"Build failed: {e}"
            raise CLIError(msg, hint="Set VERTEBRA_DEBUG=1 for full traceback.", exit_code=EXIT_INTERNAL_ERROR) from e