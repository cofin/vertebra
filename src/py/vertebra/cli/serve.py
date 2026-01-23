"""Serve command."""

import logging
import os
import sys

import click

from vertebra.supervisor import build_bun_command, run_supervisor

BANNER = r"""
   __      __          __          __
  / /   __/ /____ ____/ /____ ____/ /_
 / /__ / _  / _ `/ _  / __/ _ `/ _  / /
/____//_,_/\_,_/\_,_/\__/\_,_/\_,_/_/
"""


@click.command()
@click.argument("app")
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", type=int, default=8000, help="Port to bind to")
@click.option("--workers", type=int, default=1, help="Number of worker processes")
@click.option("--config", "config_path", type=click.Path(exists=True), help="Path to application config")
@click.option("--log-level", default="info", help="Log level")
@click.option("--drain-timeout", type=int, default=5000, help="Drain timeout in milliseconds")
@click.option("--shutdown-timeout", type=int, default=30000, help="Shutdown timeout in milliseconds")
@click.option("--native/--no-native", default=True, help="Enable/disable native Litestar integration")
def serve(
    app: str,
    host: str,
    port: int,
    workers: int,
    config_path: str | None,
    log_level: str,
    drain_timeout: int,
    shutdown_timeout: int,
    native: bool,
) -> None:
    """Run the Bun-backed ASGI server."""
    # Configure logging
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    env = os.environ.copy()
    env["VERTEBRA_PYTHON_APP"] = app
    env["VERTEBRA_HOST"] = host
    env["VERTEBRA_PORT"] = str(port)
    env["VERTEBRA_WORKERS"] = str(workers)
    env["VERTEBRA_LOG_LEVEL"] = log_level
    env["VERTEBRA_NATIVE"] = str(int(native))
    if config_path:
        env["VERTEBRA_CONFIG"] = config_path

    # Build the bun command (automatically finds 'bun' and entrypoint)
    try:
        bun_cmd = build_bun_command()
    except SystemExit as e:
        click.secho(str(e), fg="red", err=True)
        sys.exit(1)

    print_banner(host=host, port=port, workers=workers, log_level=log_level)

    exit_code = run_supervisor(
        bun_cmd=bun_cmd,
        env=env,
        workers=workers,
        drain_timeout_ms=drain_timeout,
        shutdown_timeout_ms=shutdown_timeout,
        # TODO: Load config object from config_path if needed for supervisor
    )
    sys.exit(exit_code)


def print_banner(*, host: str, port: int, workers: int, log_level: str) -> None:
    click.secho(BANNER, fg="cyan")
    click.echo("  Version: 0.1.0")
    click.echo(f"  Host: {host}")
    click.echo(f"  Port: {port}")
    click.echo(f"  Workers: {workers}")
    click.echo(f"  Log Level: {log_level}")
    click.echo("-" * 40)
