"""Run command."""

import logging
import os
import sys
import tomllib
from pathlib import Path
from typing import Any

import click

from vertebra.cli.banner import render_banner
from vertebra.cli.errors import EXIT_CONFIG_ERROR, CLIError
from vertebra.common.serializers import decode_json
from vertebra.supervisor import build_bun_command, run_supervisor


@click.command()
@click.argument("app")
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", type=int, default=8000, help="Port to bind to")
@click.option("--workers", type=int, default=1, help="Number of worker processes")
@click.option("--config", "config_path", type=click.Path(exists=True), help="Path to application config")
@click.option("--log-level", default="info", help="Log level")
@click.option("--drain-timeout", type=int, default=5000, help="Drain timeout in milliseconds")
@click.option("--shutdown-timeout", type=int, default=30000, help="Shutdown timeout in milliseconds")
@click.option("--asgi", is_flag=True, default=False, help="Run in ASGI mode (disable native pipeline)")
@click.option("--no-banner", is_flag=True, help="Disable startup banner")
def run(
    app: str,
    host: str,
    port: int,
    workers: int,
    config_path: str | None,
    log_level: str,
    drain_timeout: int,
    shutdown_timeout: int,
    asgi: bool,
    no_banner: bool,
) -> None:
    """Run the Vertebra server."""
    # Configure logging
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

    env = os.environ.copy()
    env["VERTEBRA_PYTHON_APP"] = app
    env["VERTEBRA_HOST"] = host
    env["VERTEBRA_PORT"] = str(port)
    env["VERTEBRA_WORKERS"] = str(workers)
    env["VERTEBRA_LOG_LEVEL"] = log_level
    env["VERTEBRA_NATIVE"] = "0" if asgi else "1"
    config_data: dict[str, Any] | None = None
    if config_path:
        env["VERTEBRA_CONFIG"] = config_path
        config_data = load_config(Path(config_path))

    # Build the bun command (automatically finds 'bun' and entrypoint)
    try:
        bun_cmd = build_bun_command()
    except SystemExit as e:
        raise CLIError(
            str(e),
            hint="Ensure Bun is installed and the vertebra-engine entrypoint is available.",
            exit_code=EXIT_CONFIG_ERROR,
        ) from e

    # Render startup banner (unless disabled)
    if not no_banner:
        render_banner(mode="serve", host=host, port=port, workers=workers, log_level=log_level)

    exit_code = run_supervisor(
        bun_cmd=bun_cmd,
        env=env,
        workers=workers,
        drain_timeout_ms=drain_timeout,
        shutdown_timeout_ms=shutdown_timeout,
        config=config_data,
    )
    sys.exit(exit_code)


def load_config(path: Path) -> dict[str, Any]:
    """Load a Vertebra config file into a dict for supervisor use."""
    if not path.exists():
        msg = f"Config file not found: {path}"
        raise FileNotFoundError(msg)
    if path.suffix in {".toml", ".tml"}:
        with path.open("rb") as f:
            return tomllib.load(f)
    if path.suffix == ".json":
        return decode_json(path.read_bytes(), type_hint=dict[str, Any])
    msg = f"Unsupported config format: {path.suffix}"
    raise ValueError(msg)
