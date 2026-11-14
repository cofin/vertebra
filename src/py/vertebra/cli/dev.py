"""Dev command for single-worker development mode."""

import logging
import sys
import threading
from pathlib import Path

import click

from vertebra.build import BuildConfig, BuildOrchestrator
from vertebra.cli._launch import (
    ServerLaunchEnv,
    load_workers_pyproject_config,
    parse_lane_concurrency_map,
)
from vertebra.cli._logging import configure_logging
from vertebra.cli.banner.renderer import render_banner
from vertebra.cli.errors import EXIT_CONFIG_ERROR, CLIError
from vertebra.cli.plugin_registry import enter_app_server_lifespan, resolve_cli_app_path
from vertebra.server._loops import LOOP_BACKEND_CHOICES
from vertebra.supervisor._supervisor import build_server_command, run_supervisor

__all__ = ("dev",)

logger = logging.getLogger("vertebra.cli.dev")


@click.command()
@click.argument("app", required=False, default=None)
@click.option("--host", default="127.0.0.1", help="Bind host")
@click.option("--port", default=8000, type=int, help="Bind port")
@click.option("--log-level", default="info", help="Log level")
@click.option("--no-reload", is_flag=True, help="Disable file watching and auto-reload")
@click.option("--asgi", is_flag=True, help="Use standard ASGI instead of native C ABI offload")
@click.option(
    "--interface",
    type=click.Choice(["auto", "asgi", "rsgi", "wsgi"], case_sensitive=False),
    default="auto",
    help="Gateway protocol interface (auto, asgi, rsgi, or wsgi)",
)
@click.option(
    "--loop",
    type=click.Choice(LOOP_BACKEND_CHOICES, case_sensitive=False),
    default="auto",
    help="Event loop backend (auto, uvloop, winloop, rloop, zuvloop, or asyncio)",
)
@click.option("--no-banner", is_flag=True, help="Suppress startup banner")
@click.option("--no-tui", is_flag=True, help="Disable interactive TUI dashboard")
@click.option("--verbose", is_flag=True, help="Enable verbose tracebacks with local variables")
@click.option(
    "--with-workers/--no-workers",
    default=None,
    help="Co-locate background WorkerRunner lanes inside the dev server process",
)
@click.option(
    "--queues",
    "worker_queues",
    default=None,
    help="Comma-separated queue lane names for co-located workers (e.g. default,embeddings)",
)
@click.option(
    "--lane-concurrency",
    "lane_concurrency",
    default=None,
    help="Comma-separated lane=limit assignments (e.g. default=4,embeddings=1)",
)
def dev(
    app: str | None,
    host: str,
    port: int,
    log_level: str,
    no_reload: bool,
    asgi: bool,
    interface: str,
    loop: str,
    no_banner: bool,
    no_tui: bool,
    verbose: bool,
    with_workers: bool | None,
    worker_queues: str | None,
    lane_concurrency: str | None,
) -> None:
    """Run Vertebra in development mode.

    Single worker, auto-reload enabled, frontend HMR active.

    APP is the application import string (e.g., myapp:app).
    """
    configure_logging(level=log_level, mode="dev", tracebacks_show_locals=verbose)
    resolved_app = resolve_cli_app_path(app)
    cfg_enabled, cfg_queues, cfg_lanes = load_workers_pyproject_config(Path.cwd())
    effective_with_workers = cfg_enabled if with_workers is None else with_workers
    effective_queues = tuple(q.strip() for q in worker_queues.split(",") if q.strip()) if worker_queues else cfg_queues
    effective_lanes = dict(cfg_lanes)
    if lane_concurrency:
        effective_lanes.update(parse_lane_concurrency_map(lane_concurrency))

    launch_env = ServerLaunchEnv(
        app_path=resolved_app,
        host=host,
        port=port,
        workers=1,
        log_level=log_level,
        dev=True,
        native=not asgi,
        loop=loop,
        with_workers=effective_with_workers,
        worker_queues=effective_queues or ("default",),
        worker_lane_concurrency=effective_lanes,
    )
    env = launch_env.to_environ()
    resolved_interface = "asgi" if asgi and interface.lower() == "auto" else interface.lower()
    env["VERTEBRA_INTERFACE"] = resolved_interface

    if not no_reload:
        env["VERTEBRA_RELOAD"] = "1"

    frontend_orch, _ = _start_frontend_dev(Path.cwd(), resolved_app)

    try:
        server_cmd = build_server_command()
    except SystemExit as e:
        if frontend_orch is not None:
            frontend_orch.stop()
        raise CLIError(
            str(e), hint="Ensure vertebra-server binary is available/built.", exit_code=EXIT_CONFIG_ERROR
        ) from e

    if not no_banner:
        render_banner(mode="dev", host=host, port=port, workers=1)

    use_tui = sys.stderr.isatty() and not no_tui

    with enter_app_server_lifespan(resolved_app, app_path=resolved_app, native=not asgi) as offload_json:
        if offload_json:
            env["VERTEBRA_OFFLOAD_CONFIG"] = offload_json
        try:
            exit_code = run_supervisor(
                server_cmd=server_cmd,
                env=env,
                drain_timeout_ms=2000,
                shutdown_timeout_ms=5000,
                tui=use_tui,
            )
        except SystemExit as e:
            if frontend_orch is not None:
                frontend_orch.stop()
            raise CLIError(
                str(e),
                hint="Ensure shared libpython is available or set VERTEBRA_SERVER_BINARY.",
                exit_code=EXIT_CONFIG_ERROR,
            ) from e
    if frontend_orch is not None:
        frontend_orch.stop()
    sys.exit(exit_code)


def _start_frontend_dev(project_dir: Path, app_path: str) -> tuple[BuildOrchestrator | None, threading.Thread | None]:
    """Start the frontend build orchestrator in dev mode if configured."""
    config = BuildConfig.load(project_dir)
    if not config.has_frontend:
        return None, None

    logger.info("Starting frontend dev server…")
    orchestrator = BuildOrchestrator(project_dir, config, app_path=app_path)
    thread = threading.Thread(
        target=orchestrator.dev,
        name="vertebra-frontend-dev",
        daemon=True,
    )
    thread.start()
    return orchestrator, thread
