"""CLI commands for multi-lane background worker execution (vtb workers run)."""

import asyncio
import os
import sys

import click

from vertebra.extensions.litestar.plugin import enter_litestar_server_lifespan
from vertebra.extensions.worker.runner import (
    MultiLaneWorkerRunner,
    parse_worker_lanes,
)
from vertebra.utils.module_loader import import_app

__all__ = ("execute_worker_cli", "run_workers", "workers")


def execute_worker_cli(
    app_path: str,
    *,
    queues: str = "default",
    concurrency: str = "10",
    drain_timeout_s: float = 30.0,
    run_until_stopped: bool = False,
) -> int:
    """Load the target application, enter its CLI server lifespan, and configure worker lanes."""
    os.environ["LITESTAR_APP"] = app_path
    os.environ["VERTEBRA_APP"] = app_path
    app = import_app(app_path)
    lanes = parse_worker_lanes(queues, concurrency, drain_timeout_s=drain_timeout_s)
    runner = MultiLaneWorkerRunner(lanes)
    state = getattr(app, "state", None)
    if isinstance(state, dict):
        state["vertebra_worker_runner"] = runner
    with enter_litestar_server_lifespan(app, app_path=app_path):
        try:
            if run_until_stopped:
                asyncio.run(asyncio.Event().wait())
        except KeyboardInterrupt:
            pass
        finally:
            asyncio.run(runner.stop())
        return 0


@click.group()
def workers() -> None:
    """Manage multi-lane background task workers."""


@workers.command("run")
@click.argument("app")
@click.option("--queues", default="default", help="Comma-separated queue lane names (e.g. default,etl)")
@click.option(
    "--concurrency",
    default="10",
    help="Comma-separated per-lane concurrency limits (e.g. 10,2)",
)
@click.option(
    "--drain-timeout",
    "drain_timeout_s",
    type=float,
    default=30.0,
    help="Graceful drain timeout in seconds",
)
def run_workers(app: str, queues: str, concurrency: str, drain_timeout_s: float) -> None:
    """Run admission-controlled background worker lanes for APP."""
    exit_code = execute_worker_cli(
        app,
        queues=queues,
        concurrency=concurrency,
        drain_timeout_s=drain_timeout_s,
    )
    if exit_code != 0:
        sys.exit(exit_code)
