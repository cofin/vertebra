"""Dev command for Vertebra development mode."""

import logging
import os
import sys
import threading
from pathlib import Path
from typing import Any

import click

from vertebra.builder._config import BuildConfig
from vertebra.builder._orchestrator import BuildOrchestrator
from vertebra.cli._logging import configure_logging
from vertebra.cli.banner import render_banner
from vertebra.cli.errors import EXIT_CONFIG_ERROR, CLIError
from vertebra.supervisor._supervisor import build_server_command, run_supervisor

__all__ = ("dev",)


@click.command()
@click.argument("app")
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", type=int, default=8000, help="Port to bind to")
@click.option("--log-level", default="info", help="Log level")
@click.option("--asgi", is_flag=True, default=False, help="Run in ASGI mode")
@click.option("--no-reload", is_flag=True, default=False, help="Disable auto-reload")
@click.option("--no-banner", is_flag=True, help="Disable startup banner")
@click.option("--no-tui", is_flag=True, help="Disable dev TUI (use standard scrolling output)")
@click.option("--verbose", is_flag=True, help="Show full tracebacks in dev output")
def dev(
    app: str,
    host: str,
    port: int,
    log_level: str,
    asgi: bool,
    no_reload: bool,
    no_banner: bool,
    no_tui: bool,
    verbose: bool,
) -> None:
    """Run Vertebra in development mode.

    Single worker, auto-reload enabled, frontend HMR active.

    APP is the application import string (e.g., myapp:app).
    """
    _ = verbose
    configure_logging(level=log_level, mode="dev")

    os.environ["VERTEBRA_DEV"] = "1"

    env: dict[str, Any] = os.environ.copy()
    env["VERTEBRA_APP"] = app
    env["VERTEBRA_PYTHON_APP"] = app
    env["VERTEBRA_PYTHON_BIN"] = sys.executable
    env["VERTEBRA_HOST"] = host
    env["VERTEBRA_PORT"] = str(port)
    env["VERTEBRA_WORKERS"] = "1"
    env["VERTEBRA_LOG_LEVEL"] = log_level
    env["VERTEBRA_NATIVE"] = "0" if asgi else "1"

    if not no_reload:
        env["VERTEBRA_RELOAD"] = "1"

    _start_frontend_dev(Path.cwd(), app)

    try:
        server_cmd = build_server_command()
    except SystemExit as e:
        raise CLIError(
            str(e), hint="Ensure vertebra-server binary is available/built.", exit_code=EXIT_CONFIG_ERROR
        ) from e

    if not no_banner:
        render_banner(mode="dev", host=host, port=port, workers=1, log_level=log_level)

    use_tui = sys.stderr.isatty() and not no_tui

    exit_code = run_supervisor(
        server_cmd=server_cmd,
        env=env,
        drain_timeout_ms=2000,
        shutdown_timeout_ms=5000,
        tui=use_tui,
    )
    sys.exit(exit_code)


def _start_frontend_dev(project_dir: Path, app_path: str) -> threading.Thread | None:
    """Start the frontend build orchestrator in dev mode on a background thread."""
    try:
        config = BuildConfig.load(project_dir)
    except Exception:
        return None

    if not config.entry:
        return None

    def _run_dev() -> None:
        try:
            orchestrator = BuildOrchestrator(project_dir, config, app_path=app_path)
            orchestrator.dev()
        except Exception:
            logger = logging.getLogger("vertebra.cli.dev")
            logger.warning("Frontend dev server failed to start", exc_info=True)

    thread = threading.Thread(target=_run_dev, daemon=True, name="vertebra-frontend-dev")
    thread.start()
    return thread
