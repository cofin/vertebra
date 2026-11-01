"""CLI commands for multi-lane background worker execution (vtb workers run)."""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

import click
import msgspec

from vertebra.extensions.litestar.plugin import enter_litestar_server_lifespan
from vertebra.server.lanes import WorkerRunner
from vertebra.server.platform import LameDuckController
from vertebra.utils.module_loader import import_app
from vertebra.utils.serializers import decode_json

__all__ = ("execute_worker_cli", "run_workers", "workers")


def execute_worker_cli(
    app_path: str,
    *,
    queues: str = "default",
    concurrency: str = "10",
    drain_timeout_s: float = 30.0,
    run_until_stopped: bool = False,
    oneshot_job: str | None = None,
    payload: Any = None,
) -> int:
    """Load the target application, enter its CLI server lifespan, and configure worker lanes."""
    os.environ["LITESTAR_APP"] = app_path
    os.environ["VERTEBRA_APP"] = app_path
    app = import_app(app_path)
    if oneshot_job:
        with enter_litestar_server_lifespan(app, app_path=app_path):
            return asyncio.run(WorkerRunner.run_oneshot(app, oneshot_job, payload=payload))

    lanes = WorkerRunner.parse_lanes(queues, concurrency, drain_timeout_s=drain_timeout_s)
    lame_duck = LameDuckController()
    runner = WorkerRunner(lanes, lame_duck=lame_duck)
    state = getattr(app, "state", None)
    if isinstance(state, dict):
        state["vertebra_worker_runner"] = runner
        state["vertebra_lame_duck"] = lame_duck
    with enter_litestar_server_lifespan(app, app_path=app_path):
        try:
            if run_until_stopped:
                asyncio.run(runner.run(install_signal_handlers=True))
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
@click.option(
    "--oneshot",
    "oneshot_job",
    default=None,
    help="Execute a single registered background job and exit",
)
@click.option(
    "--payload",
    "payload_json",
    default=None,
    help="Optional JSON payload passed to --oneshot job",
)
def run_workers(
    app: str,
    queues: str,
    concurrency: str,
    drain_timeout_s: float,
    oneshot_job: str | None,
    payload_json: str | None,
) -> None:
    """Run admission-controlled background worker lanes or a single --oneshot job for APP."""
    parsed_payload: Any = None
    if payload_json is not None:
        try:
            parsed_payload = decode_json(payload_json.encode("utf-8"))
        except msgspec.DecodeError as exc:
            msg = f"Invalid JSON passed to --payload: {exc}"
            raise click.ClickException(msg) from exc

    exit_code = execute_worker_cli(
        app,
        queues=queues,
        concurrency=concurrency,
        drain_timeout_s=drain_timeout_s,
        oneshot_job=oneshot_job,
        payload=parsed_payload,
    )
    if exit_code != 0:
        sys.exit(exit_code)
