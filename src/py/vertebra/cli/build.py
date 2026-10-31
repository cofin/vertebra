"""Build command for frontend assets, standalone directories, onefile binaries, and PGO."""

from pathlib import Path

import click

from vertebra.build import BuildOrchestrator
from vertebra.build.packaging import (
    BuildTarget,
    OnefileBuilder,
    StandaloneBuilder,
)
from vertebra.cli.theme import console

__all__ = ("build",)

_LEGACY_TARGETS: frozenset[str] = frozenset({"frontend", "backend", "all"})


@click.command()
@click.option(
    "--target",
    default="frontend",
    help="Build target: frontend, backend, all, or platform triple (e.g. linux-x86_64, darwin-aarch64)",
)
@click.option("--profile", default="release", help="Build profile: debug, release")
@click.option(
    "--standalone",
    is_flag=True,
    default=False,
    help="Assemble a relocatable standalone distribution directory",
)
@click.option(
    "--onefile",
    is_flag=True,
    default=False,
    help="Package application into a self-extracting single-file executable",
)
@click.option(
    "--pgo",
    is_flag=True,
    default=False,
    hidden=True,
    help="Enable Profile-Guided Optimization metadata during packaging",
)
@click.option(
    "--profdata",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    hidden=True,
    help="Merged LLVM .profdata file for Rust -Cprofile-use packaging metadata",
)
@click.option(
    "--server-binary",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Compiled vertebra-server executable to bundle under bin/ in the staged distribution",
)
@click.option(
    "--entrypoint",
    default="app:app",
    help="Application entrypoint module:attribute (default: app:app)",
)
@click.option(
    "--python-version",
    default="3.12",
    help="Target Python runtime version (3.12, 3.13, 3.14, 3.14t, 3.15, 3.15t)",
)
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    default=None,
    help="Output directory (for --standalone) or binary path (for --onefile)",
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("."),
    help="Project directory containing pyproject.toml and any frontend workspace",
)
def build(
    target: str,
    profile: str,
    standalone: bool,
    onefile: bool,
    pgo: bool,
    profdata: Path | None,
    server_binary: Path | None,
    entrypoint: str,
    python_version: str,
    output: Path | None,
    project_dir: Path,
) -> None:
    """Build Vertebra application assets, standalone distributions, or onefile binaries."""
    resolved_project = project_dir.resolve()
    effective_pgo = pgo or (profdata is not None)

    if standalone or onefile or target not in _LEGACY_TARGETS:
        platform_spec = None if target in _LEGACY_TARGETS else target
        try:
            target_tuple = BuildTarget.resolve(platform_spec, python_version=python_version)
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc

        dist_dir = (
            output
            if (standalone and not onefile and output is not None)
            else (resolved_project / "dist" / f"standalone-{target_tuple.rust_triple}")
        )
        standalone_builder = StandaloneBuilder(
            project_dir=resolved_project,
            target=target_tuple,
            entrypoint=entrypoint,
            pgo_enabled=effective_pgo,
            profdata_path=profdata,
            server_binary=server_binary,
            profile=profile,
        )
        staged_dir = standalone_builder.build(dist_dir)
        console.success(f"Standalone build staged -> {staged_dir}")

        if onefile:
            binary_out = output or (resolved_project / "dist" / f"{resolved_project.name}-{target_tuple.rust_triple}")
            onefile_builder = OnefileBuilder(dist_dir=staged_dir, target=target_tuple)
            created = onefile_builder.build(binary_out)
            console.success(f"Onefile executable created -> {created}")
        return

    if target in {"frontend", "all"}:
        orchestrator = BuildOrchestrator(resolved_project)
        with console.status("Building frontend…"):
            built = orchestrator.build()
        if built:
            console.success("Frontend build complete")
        else:
            console.info("No frontend project detected — skipped frontend build")

    if target in {"backend", "all"}:
        try:
            target_tuple = BuildTarget.resolve(None, python_version=python_version)
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc
        dist_dir = output or (resolved_project / "dist" / "backend")
        standalone_builder = StandaloneBuilder(
            project_dir=resolved_project,
            target=target_tuple,
            entrypoint=entrypoint,
            pgo_enabled=effective_pgo,
            profdata_path=profdata,
            server_binary=server_binary,
            profile=profile,
        )
        staged = standalone_builder.build(dist_dir)
        console.success(f"Backend build complete -> {staged}")
