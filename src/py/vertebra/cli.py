"""Command line interface for Vertebra."""

import asyncio
import os
import shutil
import sys
from pathlib import Path

import click
import rich_click

from vertebra.supervisor import build_broker_command, run_supervisor

# Configure rich-click
rich_click.rich_click.USE_RICH_MARKUP = True
rich_click.rich_click.SHOW_ARGUMENTS = True
rich_click.rich_click.GROUP_ARGUMENTS_OPTIONS = True

BANNER = r"""
   __      __          __          __
  / /   __/ /____ ____/ /____ ____/ /_
 / /__ / _  / _ `/ _  / __/ _ `/ _  / /
/____//_,_/\_,_/\_,_/\__/\_,_/\_,_/_/
"""


@click.group()
@click.version_option(package_name="vertebra")
def main() -> None:
    """Vertebra: High-performance polyglot application backbone."""


@main.command()
@click.argument("app")
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", type=int, default=8000, help="Port to bind to")
@click.option("--workers", type=int, default=1, help="Number of worker processes")
@click.option("--config", "config_path", type=click.Path(exists=True), help="Path to application config")
@click.option("--log-level", default="info", help="Log level")
@click.option("--broker-config", "broker_config_path", type=click.Path(exists=True), help="Path to broker config")
@click.option("--drain-timeout", type=int, default=5000, help="Drain timeout in milliseconds")
@click.option("--shutdown-timeout", type=int, default=30000, help="Shutdown timeout in milliseconds")
def serve(
    app: str,
    host: str,
    port: int,
    workers: int,
    config_path: str | None,
    log_level: str,
    broker_config_path: str | None,
    drain_timeout: int,
    shutdown_timeout: int,
) -> None:
    """Run the Bun-backed ASGI server."""
    bun = shutil.which("bun")
    if not bun:
        click.secho("Error: bun binary not found; install Bun or add it to PATH", fg="red", err=True)
        sys.exit(1)

    env = os.environ.copy()
    env["VERTEBRA_PYTHON_APP"] = app
    env["VERTEBRA_HOST"] = host
    env["VERTEBRA_PORT"] = str(port)
    env["VERTEBRA_WORKERS"] = str(workers)
    env["VERTEBRA_LOG_LEVEL"] = log_level
    if config_path:
        env["VERTEBRA_CONFIG"] = config_path
    broker_config = broker_config_path or env.get("VERTEBRA_BROKER_CONFIG") or "vertebra-broker.toml"
    env["VERTEBRA_BROKER_CONFIG"] = broker_config

    engine = find_engine()
    engine_path = Path(engine)

    # If the engine is a binary, use it directly. Otherwise use bun to run the source.
    bun_cmd = (
        [str(engine_path)]
        if engine_path.suffix in (".exe", "") and os.access(engine_path, os.X_OK)
        else [bun, str(engine_path)]
    )

    broker_cmd = build_broker_command(broker_config)

    print_banner(host=host, port=port, workers=workers, log_level=log_level)

    exit_code = run_supervisor(
        bun_cmd=bun_cmd,
        broker_cmd=broker_cmd,
        broker_config=broker_config,
        env=env,
        drain_timeout_ms=drain_timeout,
        shutdown_timeout_ms=shutdown_timeout,
    )
    sys.exit(exit_code)


@main.command()
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


def find_engine() -> str:
    package_root = Path(__file__).resolve().parent
    bundled = package_root / "bin" / "vertebra-engine"
    if bundled.exists():
        return str(bundled)
    repo_root = find_repo_root(package_root)
    if repo_root:
        entry = repo_root / "src" / "ts" / "src" / "server" / "serve.ts"
        if entry.exists():
            return str(entry)
    msg = "Unable to locate vertebra engine; expected bundled binary or source entrypoint"
    raise SystemExit(msg)


def find_repo_root(start: Path) -> Path | None:
    for parent in [start, *start.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return None


def print_banner(*, host: str, port: int, workers: int, log_level: str) -> None:
    click.secho(BANNER, fg="cyan")
    click.echo("  Version: 0.1.0")
    click.echo(f"  Host: {host}")
    click.echo(f"  Port: {port}")
    click.echo(f"  Workers: {workers}")
    click.echo(f"  Log Level: {log_level}")
    click.echo("-" * 40)


if __name__ == "__main__":
    main()
