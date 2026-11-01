"""Run command."""

import asyncio
import logging
import os
import sys
import tomllib
from pathlib import Path
from typing import Any

import click

from vertebra.build import BuildConfig
from vertebra.cli._launch import (
    ServerLaunchEnv,
    is_free_threaded_runtime,
    load_workers_pyproject_config,
    parse_lane_concurrency_map,
)
from vertebra.cli._logging import configure_logging
from vertebra.cli.banner import render_banner
from vertebra.cli.errors import EXIT_CONFIG_ERROR, CLIError
from vertebra.extensions.litestar.plugin import enter_litestar_server_lifespan
from vertebra.framework.queues import WorkerRunner
from vertebra.supervisor._supervisor import build_server_command, run_supervisor
from vertebra.utils.module_loader import import_app
from vertebra.utils.serializers import decode_json

__all__ = ("load_config", "run")

logger = logging.getLogger("vertebra.cli.run")


@click.command()
@click.argument("app")
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", type=int, default=8000, help="Port to bind to")
@click.option("--workers", type=int, default=None, help="Number of worker processes")
@click.option("--threads", type=int, default=None, help="Number of threads per process")
@click.option(
    "--topology",
    type=click.Choice(["auto", "embedded", "shm"], case_sensitive=False),
    default=None,
    help="Runtime topology (auto, embedded, or shm)",
)
@click.option(
    "--jit/--no-jit",
    default=None,
    help="Enable or disable CPython experimental JIT (PYTHON_JIT)",
)
@click.option("--config", "config_path", type=click.Path(exists=True), help="Path to application config")
@click.option("--log-level", default="info", help="Log level")
@click.option("--drain-timeout", type=int, default=5000, help="Drain timeout in milliseconds")
@click.option("--shutdown-timeout", type=int, default=30000, help="Shutdown timeout in milliseconds")
@click.option("--asgi", is_flag=True, default=False, help="Run in ASGI mode (disable native pipeline)")
@click.option(
    "--interface",
    type=click.Choice(["auto", "asgi", "rsgi", "wsgi"], case_sensitive=False),
    default="auto",
    help="Gateway protocol interface (auto, asgi, rsgi, or wsgi)",
)
@click.option("--tls-cert", type=click.Path(exists=True), help="Path to PEM certificate for HTTPS")
@click.option("--tls-key", type=click.Path(exists=True), help="Path to PEM private key for HTTPS")
@click.option(
    "--worker-only",
    is_flag=True,
    default=False,
    help="Run background worker lanes with health probes only",
)
@click.option(
    "--oneshot-job",
    default=None,
    help="Execute a single registered job inside the app lifespan and exit",
)
@click.option(
    "--with-workers/--no-workers",
    default=None,
    help="Co-locate background WorkerRunner lanes inside the server process",
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
@click.option("--no-banner", is_flag=True, help="Disable startup banner")
def run(
    app: str,
    host: str,
    port: int,
    workers: int | None,
    threads: int | None,
    topology: str | None,
    jit: bool | None,
    config_path: str | None,
    log_level: str,
    drain_timeout: int,
    shutdown_timeout: int,
    asgi: bool,
    interface: str,
    tls_cert: str | None,
    tls_key: str | None,
    worker_only: bool,
    oneshot_job: str | None,
    with_workers: bool | None,
    worker_queues: str | None,
    lane_concurrency: str | None,
    no_banner: bool,
) -> None:
    """Run the Vertebra server."""
    configure_logging(level=log_level, mode="prod")

    if oneshot_job:
        sys.exit(_execute_oneshot_cli(app, oneshot_job))

    resolved_jit = _resolve_jit_setting(jit)
    cfg_enabled, cfg_queues, cfg_lanes = load_workers_pyproject_config(Path.cwd())
    effective_with_workers = cfg_enabled if with_workers is None else with_workers
    effective_queues = tuple(q.strip() for q in worker_queues.split(",") if q.strip()) if worker_queues else cfg_queues
    effective_lanes = dict(cfg_lanes)
    if lane_concurrency:
        effective_lanes.update(parse_lane_concurrency_map(lane_concurrency))

    launch_env = ServerLaunchEnv(
        app_path=app,
        host=host,
        port=port,
        workers=workers,
        log_level=log_level,
        dev=False,
        native=not asgi,
        topology=topology,
        threads=threads,
        jit=resolved_jit,
        with_workers=effective_with_workers,
        worker_queues=effective_queues or ("default",),
        worker_lane_concurrency=effective_lanes,
    )
    env = launch_env.to_environ()
    resolved_interface = "asgi" if asgi and interface.lower() == "auto" else interface.lower()
    env["VERTEBRA_INTERFACE"] = resolved_interface
    env["VERTEBRA_WORKER_ONLY"] = "1" if worker_only else "0"

    if tls_cert and tls_key:
        env["VERTEBRA_TLS_CERT"] = str(Path(tls_cert).resolve())
        env["VERTEBRA_TLS_KEY"] = str(Path(tls_key).resolve())
    elif tls_cert or tls_key:
        raise CLIError(
            "Both --tls-cert and --tls-key must be provided together",
            exit_code=EXIT_CONFIG_ERROR,
        )

    static_dir = _resolve_static_dir(Path.cwd())
    if static_dir:
        env["VERTEBRA_STATIC_DIR"] = str(static_dir)

    if config_path:
        env["VERTEBRA_CONFIG"] = config_path
        load_config(Path(config_path))

    try:
        server_cmd = build_server_command()
    except SystemExit as e:
        raise CLIError(
            str(e),
            hint="Ensure vertebra-server binary is available/built.",
            exit_code=EXIT_CONFIG_ERROR,
        ) from e

    if not no_banner:
        banner_workers = _parse_positive_int(env.get("VERTEBRA_WORKERS"), default=1)
        render_banner(mode="serve", host=host, port=port, workers=banner_workers)

    exit_code = run_supervisor(
        server_cmd=server_cmd,
        env=env,
        drain_timeout_ms=drain_timeout,
        shutdown_timeout_ms=shutdown_timeout,
    )
    sys.exit(exit_code)


def load_config(path: Path) -> dict[str, Any]:
    """Load a Vertebra config file into a dict."""
    if not path.exists():
        msg = f"Config file not found: {path}"
        raise FileNotFoundError(msg)
    if path.suffix in {".toml", ".tml"}:
        with path.open("rb") as f:
            return tomllib.load(f)
    if path.suffix == ".json":
        data: dict[str, Any] = decode_json(path.read_bytes())
        return data
    msg = f"Unsupported config format: {path.suffix}"
    raise ValueError(msg)


def _resolve_jit_setting(jit_flag: bool | None) -> bool | None:
    """Resolve the requested JIT setting from CLI flag or VERTEBRA_JIT, skipping free-threaded builds."""
    requested = jit_flag
    if requested is None:
        raw = os.environ.get("VERTEBRA_JIT", "").strip().lower()
        if raw in {"1", "true", "yes", "on"}:
            requested = True
        elif raw in {"0", "false", "no", "off"}:
            requested = False

    if requested is True and is_free_threaded_runtime():
        logger.warning("CPython JIT is unavailable on free-threaded builds; skipping PYTHON_JIT=1")
        return False
    return requested


def _parse_positive_int(raw: str | None, *, default: int) -> int:
    """Parse an integer string or fall back to default when missing or zero."""
    if raw is not None:
        try:
            parsed = int(raw)
            return parsed if parsed > 0 else default
        except ValueError:
            return default
    return default


def _execute_oneshot_cli(app_path: str, job_name: str) -> int:
    """Import app_path, enter its server lifespan, and execute a single named oneshot job."""
    os.environ["LITESTAR_APP"] = app_path
    os.environ["VERTEBRA_APP"] = app_path
    target_app = import_app(app_path)
    with enter_litestar_server_lifespan(target_app, app_path=app_path):
        return asyncio.run(WorkerRunner.run_oneshot(target_app, job_name))


def _resolve_static_dir(project_dir: Path) -> Path | None:
    """Resolve the static assets directory for production frontend serving."""
    try:
        config = BuildConfig.load(project_dir)
        if config.outdir:
            candidate = project_dir / config.outdir
            if candidate.is_dir():
                return candidate
    except Exception:
        pass

    for name in ("dist/static", "dist"):
        candidate = project_dir / name
        if candidate.is_dir():
            return candidate

    return None
