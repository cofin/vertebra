"""Build command."""

from pathlib import Path

import click

from vertebra.builder._orchestrator import BuildOrchestrator
from vertebra.cli.theme import console

__all__ = ("build",)


@click.command()
@click.option("--target", default="frontend", help="Build target: frontend, backend, all")
@click.option("--profile", default="release", help="Build profile: debug, release")
@click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("."),
    help="Project directory containing pyproject.toml and any frontend workspace",
)
def build(target: str, profile: str, project_dir: Path) -> None:
    """Build Vertebra application assets."""
    _ = profile

    if target not in {"frontend", "all", "backend"}:
        msg = f"Unsupported build target: {target}"
        raise click.ClickException(msg)

    if target in {"frontend", "all"}:
        orchestrator = BuildOrchestrator(project_dir.resolve())
        with console.status("Building frontend…"):
            built = orchestrator.build()
        if built:
            console.success("Frontend build complete")
        else:
            console.info("No frontend project detected — skipped frontend build")

    if target in {"backend", "all"}:
        console.warning("Backend build is not implemented yet")
