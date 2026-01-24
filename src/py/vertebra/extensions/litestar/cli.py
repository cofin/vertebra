"""CLI commands for Vertebra Litestar plugin."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

import click

if TYPE_CHECKING:
    from litestar import Litestar

__all__ = ("run_command",)


@click.command(name="run")
@click.pass_context
@click.option(
    "--host", "-h", default="127.0.0.1", show_default=True, envvar="VERTEBRA_HOST", help="Host address to bind to."
)
@click.option(
    "--port", "-p", default=8000, type=int, show_default=True, envvar="VERTEBRA_PORT", help="Port to bind to."
)
@click.option(
    "--workers",
    "-w",
    default=1,
    type=int,
    show_default=True,
    envvar="VERTEBRA_WORKERS",
    help="Number of worker processes.",
)
@click.option(
    "--log-level",
    default="info",
    type=click.Choice(["debug", "info", "warning", "error", "critical"]),
    show_default=True,
    envvar="VERTEBRA_LOG_LEVEL",
    help="Log level for the application.",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True),
    envvar="VERTEBRA_CONFIG",
    help="Path to application configuration file.",
)
@click.option(
    "--broker-config",
    "broker_config_path",
    type=click.Path(exists=True),
    envvar="VERTEBRA_BROKER_CONFIG",
    help="Path to broker configuration file.",
)
@click.option(
    "--drain-timeout", default=5000, type=int, show_default=True, help="Drain timeout in milliseconds before shutdown."
)
@click.option(
    "--shutdown-timeout", default=30000, type=int, show_default=True, help="Shutdown timeout in milliseconds."
)
def run_command(
    ctx: click.Context,
    host: str,
    port: int,
    workers: int,
    log_level: str,
    config_path: str | None,
    broker_config_path: str | None,
    drain_timeout: int,
    shutdown_timeout: int,
) -> None:
    r"""Run the Vertebra server with the Litestar application.

    This command starts the Bun-backed ASGI server with shared-memory
    broker for high-performance IPC. It provides the same functionality
    as the standalone ``vertebra serve`` command.

    \b
    Examples:
        # Start with default settings
        $ litestar run

        # Start with multiple workers on all interfaces
        $ litestar run --workers 4 --host 0.0.0.0

        # Use a custom broker configuration
        $ litestar run --broker-config custom-broker.toml

        # Debug mode with custom port
        $ litestar run --log-level debug --port 8080

    \b
    Environment Variables:
        VERTEBRA_HOST          Host to bind to
        VERTEBRA_PORT          Port to bind to
        VERTEBRA_WORKERS       Number of workers
        VERTEBRA_LOG_LEVEL     Log level
        VERTEBRA_CONFIG        Path to app config
        VERTEBRA_BROKER_CONFIG Path to broker config
    """
    app: Litestar = ctx.obj.app
    app_path = _get_app_path(app)

    _run_vertebra(
        app_path=app_path,
        host=host,
        port=port,
        workers=workers,
        log_level=log_level,
        config_path=config_path,
        broker_config_path=broker_config_path,
        drain_timeout=drain_timeout,
        shutdown_timeout=shutdown_timeout,
    )


def _get_app_path(app: Any) -> str:
    """Get the import path for the Litestar application.

    Attempts to reconstruct the app path from the module and name.
    Falls back to a default if not determinable.
    """
    module = getattr(app, "__module__", None)
    name = getattr(app, "__name__", None) or "app"

    if module and module != "__main__":
        return f"{module}:{name}"

    # Fallback: try to get from Click context
    return name


def _run_vertebra(
    app_path: str,
    host: str,
    port: int,
    workers: int,
    log_level: str,
    config_path: str | None,
    broker_config_path: str | None,
    drain_timeout: int,
    shutdown_timeout: int,
) -> None:
    """Run the Vertebra server.

    This function delegates to vertebra.cli.serve which handles:
    - Bun binary validation
    - Environment setup
    - Supervisor lifecycle (broker + engine)
    """
    import os
    import shutil
    from pathlib import Path

    from vertebra.supervisor import run_supervisor

    bun = shutil.which("bun")
    if not bun:
        click.secho("Error: bun binary not found; install Bun or add it to PATH", fg="red", err=True)
        sys.exit(1)

    env = os.environ.copy()
    env["VERTEBRA_PYTHON_APP"] = app_path
    env["VERTEBRA_HOST"] = host
    env["VERTEBRA_PORT"] = str(port)
    env["VERTEBRA_WORKERS"] = str(workers)
    env["VERTEBRA_LOG_LEVEL"] = log_level
    if config_path:
        env["VERTEBRA_CONFIG"] = config_path
    broker_config = broker_config_path or env.get("VERTEBRA_BROKER_CONFIG") or "vertebra-broker.toml"
    env["VERTEBRA_BROKER_CONFIG"] = broker_config

    engine = _find_engine()
    engine_path = Path(engine)

    # If the engine is a binary, use it directly. Otherwise use bun to run the source.
    bun_cmd = (
        [str(engine_path)]
        if engine_path.suffix in (".exe", "") and os.access(engine_path, os.X_OK)
        else [bun, str(engine_path)]
    )

    _print_banner(host=host, port=port, workers=workers, log_level=log_level)

    exit_code = run_supervisor(
        bun_cmd=bun_cmd, env=env, drain_timeout_ms=drain_timeout, shutdown_timeout_ms=shutdown_timeout
    )
    sys.exit(exit_code)


def _find_engine() -> str:
    """Locate the vertebra engine binary or source."""
    from pathlib import Path

    package_root = Path(__file__).resolve().parent.parent.parent
    bundled = package_root / "bin" / "vertebra-engine"
    if bundled.exists():
        return str(bundled)
    bundled_exe = package_root / "bin" / "vertebra-engine.exe"
    if bundled_exe.exists():
        return str(bundled_exe)
    repo_root = _find_repo_root(package_root)
    if repo_root:
        entry = repo_root / "src" / "ts" / "src" / "server" / "serve.ts"
        if entry.exists():
            return str(entry)
    msg = "Unable to locate vertebra engine; expected bundled binary or source entrypoint"
    raise SystemExit(msg)


def _find_repo_root(start: Any) -> Any:
    """Find the repository root by looking for pyproject.toml."""
    from pathlib import Path

    start = Path(start)
    for parent in [start, *start.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return None


def _print_banner(*, host: str, port: int, workers: int, log_level: str) -> None:
    """Print the Vertebra startup banner."""
    banner = r"""
   __      __          __          __
  / /   __/ /____ ____/ /____ ____/ /_
 / /__ / _  / _ `/ _  / __/ _ `/ _  / /
/____//_,_/\_,_/\_,_/\__/\_,_/\_,_/_/
"""
    click.secho(banner, fg="cyan")
    click.echo("  Version: 0.1.0")
    click.echo(f"  Host: {host}")
    click.echo(f"  Port: {port}")
    click.echo(f"  Workers: {workers}")
    click.echo(f"  Log Level: {log_level}")
    click.echo("-" * 40)
