"""Run command."""

import os
import sys
import tomllib
from pathlib import Path
from typing import Any

import click

from vertebra.builder._config import BuildConfig
from vertebra.cli._logging import configure_logging
from vertebra.cli.banner import render_banner
from vertebra.cli.errors import EXIT_CONFIG_ERROR, CLIError
from vertebra.supervisor._supervisor import build_server_command, run_supervisor
from vertebra.utils.serializers import decode_json

__all__ = ("load_config", "run")


@click.command()
@click.argument("app")
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", type=int, default=8000, help="Port to bind to")
@click.option("--workers", type=int, default=None, help="Number of worker processes")
@click.option("--threads", type=int, default=None, help="Number of threads per process")
@click.option(
    "--topology",
    type=click.Choice(["embedded", "shm"], case_sensitive=False),
    default="embedded",
    help="Runtime topology",
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
@click.option("--no-banner", is_flag=True, help="Disable startup banner")
def run(
    app: str,
    host: str,
    port: int,
    workers: int | None,
    threads: int | None,
    topology: str,
    config_path: str | None,
    log_level: str,
    drain_timeout: int,
    shutdown_timeout: int,
    asgi: bool,
    interface: str,
    tls_cert: str | None,
    tls_key: str | None,
    no_banner: bool,
) -> None:
    """Run the Vertebra server."""
    configure_logging(level=log_level, mode="prod")

    if workers is None:
        workers = 1 if topology.lower() == "shm" else 0

    if threads is None:
        threads = os.cpu_count() or 1

    env = os.environ.copy()
    env["VERTEBRA_APP"] = app
    env["VERTEBRA_PYTHON_BIN"] = sys.executable

    cwd = os.getcwd()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{cwd}{os.pathsep}{existing}" if existing else cwd
    env["VERTEBRA_HOST"] = host
    env["VERTEBRA_PORT"] = str(port)
    env["VERTEBRA_WORKERS"] = str(workers)
    env["VERTEBRA_THREADS"] = str(threads)
    env["VERTEBRA_TOPOLOGY"] = topology.lower()
    env["VERTEBRA_LOG_LEVEL"] = log_level
    resolved_interface = "asgi" if asgi and interface.lower() == "auto" else interface.lower()
    env["VERTEBRA_INTERFACE"] = resolved_interface
    env["VERTEBRA_NATIVE"] = "0" if asgi else "1"

    if tls_cert and tls_key:
        env["VERTEBRA_TLS_CERT"] = str(Path(tls_cert).resolve())
        env["VERTEBRA_TLS_KEY"] = str(Path(tls_key).resolve())
    elif tls_cert or tls_key:
        raise CLIError("Both --tls-cert and --tls-key must be provided together", exit_code=EXIT_CONFIG_ERROR)

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
            str(e), hint="Ensure vertebra-server binary is available/built.", exit_code=EXIT_CONFIG_ERROR
        ) from e

    if not no_banner:
        render_banner(mode="serve", host=host, port=port, workers=workers, log_level=log_level)

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
