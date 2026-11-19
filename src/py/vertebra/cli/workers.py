"""CLI commands for multi-lane background worker execution (vtb workers run)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
import time
from collections.abc import Callable, MutableMapping
from pathlib import Path
from typing import Any, cast

import click
import msgspec

from vertebra.cli.plugin_registry import enter_app_server_lifespan, resolve_cli_app_path
from vertebra.framework._app import App
from vertebra.server.lanes import WorkerRunner
from vertebra.server.platform import HealthProbeServer, LameDuckController, ZPagesRegistry
from vertebra.server.worker import Worker
from vertebra.telemetry.otel import EventLoopMetricSample, EventLoopRecorder
from vertebra.utils.module_loader import import_app, import_optional
from vertebra.utils.serializers import decode_json

__all__ = ("execute_worker_cli", "run_workers", "workers")

logger = logging.getLogger(__name__)


def _detect_loop_backend_name(loop: asyncio.AbstractEventLoop) -> str:
    """Return the canonical backend name for a running event loop instance."""
    module = type(loop).__module__
    for name in ("zuvloop", "uvloop", "winloop", "rloop"):
        if module == name or module.startswith(f"{name}."):
            return name
    return "asyncio"


def _start_worker_loop_telemetry(
    loop: asyncio.AbstractEventLoop,
    zpages: ZPagesRegistry,
) -> tuple[EventLoopRecorder, Callable[[], None]] | None:
    """Start periodic scheduling-lag sampling on loop when VERTEBRA_LOOP_TELEMETRY is enabled."""
    if not Worker._loop_telemetry_enabled():
        return None
    interval = Worker._loop_telemetry_interval()
    recorder = EventLoopRecorder(emit_otel=import_optional("opentelemetry.metrics") is not None)
    backend = _detect_loop_backend_name(loop)
    if backend == "zuvloop":
        recorder.configure_zuvloop(loop, metrics_interval_ms=max(1, round(interval * 1000.0)))
    zpages.register_loop_recorder("worker", recorder)

    stopped = False
    timer_handle: asyncio.TimerHandle | None = None

    def _arm() -> None:
        nonlocal timer_handle
        if stopped or loop.is_closed():
            return
        timer_handle = loop.call_later(interval, _sample, time.monotonic() + interval)

    def _sample(expected_at: float) -> None:
        nonlocal timer_handle
        timer_handle = None
        if stopped:
            return
        lag_ms = max(0.0, (time.monotonic() - expected_at) * 1000.0)
        try:
            recorder.record(
                EventLoopMetricSample(
                    shard_id=0,
                    backend=backend,
                    lag_ms=lag_ms,
                    active_tasks=len(asyncio.all_tasks(loop)),
                )
            )
        except Exception:
            logger.exception("Worker event loop telemetry sample failed")
        _arm()

    def _stop() -> None:
        nonlocal stopped, timer_handle
        stopped = True
        if timer_handle is not None:
            timer_handle.cancel()
            timer_handle = None

    _arm()
    return recorder, _stop


def execute_worker_cli(
    app_path: str,
    *,
    queues: str = "default",
    concurrency: str = "10",
    drain_timeout_s: float = 30.0,
    run_until_stopped: bool = False,
    oneshot_job: str | None = None,
    payload: Any = None,
    health_probe: tuple[str, int] | None = None,
) -> int:
    """Load the target application, enter its lifespan, bind QueueService, and run worker lanes."""
    prev_litestar_app = os.environ.get("LITESTAR_APP")
    prev_vertebra_app = os.environ.get("VERTEBRA_APP")
    cwd_str = str(Path.cwd().resolve())
    if cwd_str not in sys.path:
        sys.path.insert(0, cwd_str)
    os.environ["LITESTAR_APP"] = app_path
    os.environ["VERTEBRA_APP"] = app_path
    try:
        app = import_app(app_path)
        if oneshot_job:
            with enter_app_server_lifespan(app, app_path=app_path):
                return asyncio.run(WorkerRunner.run_oneshot(app, oneshot_job, payload=payload))

        lanes = WorkerRunner.parse_lanes(queues, concurrency, drain_timeout_s=drain_timeout_s)
        lame_duck = LameDuckController()
        zpages = ZPagesRegistry("vertebra-worker", lame_duck=lame_duck)

        async def _serve_workers() -> int:
            async with contextlib.AsyncExitStack() as async_stack:
                if not isinstance(app, App):
                    lifespan_fn = getattr(app, "lifespan", None)
                    if callable(lifespan_fn):
                        cm = lifespan_fn()
                        if hasattr(cm, "__aenter__"):
                            await async_stack.enter_async_context(
                                cast("contextlib.AbstractAsyncContextManager[Any]", cm)
                            )

                queue_service = WorkerRunner.resolve_queue_service(app)
                runner = WorkerRunner(lanes, queue_service=queue_service, lame_duck=lame_duck)
                running_loop = asyncio.get_running_loop()
                zpages.register_event_loop("worker", running_loop)
                telemetry_binding = _start_worker_loop_telemetry(running_loop, zpages)
                stop_loop_telemetry: Callable[[], None] | None = None
                state = getattr(app, "state", None)
                if isinstance(state, MutableMapping):
                    state["vertebra_worker_runner"] = runner
                    state["vertebra_lame_duck"] = lame_duck
                    state["vertebra_zpages"] = zpages
                if telemetry_binding is not None:
                    loop_recorder, stop_loop_telemetry = telemetry_binding
                    if isinstance(state, MutableMapping):
                        state["vertebra_loop_recorder"] = loop_recorder

                probe_server: HealthProbeServer | None = None
                try:
                    if health_probe is not None:
                        probe_host, probe_port = health_probe
                        probe_server = HealthProbeServer(zpages, host=probe_host, port=probe_port)
                        await probe_server.start()
                        if isinstance(state, MutableMapping):
                            state["vertebra_health_probe_server"] = probe_server
                            state["vertebra_health_probe_port"] = probe_server.bound_port

                    zpages.set_var("worker_state", "ready")
                    try:
                        if run_until_stopped:
                            await runner.run(install_signal_handlers=True)
                    finally:
                        zpages.set_var("worker_state", "draining")
                        await runner.stop()
                        if probe_server is not None:
                            await probe_server.stop()
                        zpages.set_var("worker_state", "stopped")
                finally:
                    if stop_loop_telemetry is not None:
                        stop_loop_telemetry()
            return 0

        with enter_app_server_lifespan(app, app_path=app_path):
            try:
                return asyncio.run(_serve_workers())
            except KeyboardInterrupt:
                return 0
    finally:
        if prev_litestar_app is None:
            os.environ.pop("LITESTAR_APP", None)
        else:
            os.environ["LITESTAR_APP"] = prev_litestar_app
        if prev_vertebra_app is None:
            os.environ.pop("VERTEBRA_APP", None)
        else:
            os.environ["VERTEBRA_APP"] = prev_vertebra_app


@click.group()
def workers() -> None:
    """Manage multi-lane background task workers."""


@workers.command("run")
@click.argument("app", required=False, default=None)
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
    app: str | None,
    queues: str,
    concurrency: str,
    drain_timeout_s: float,
    oneshot_job: str | None,
    payload_json: str | None,
) -> None:
    """Run admission-controlled background worker lanes or a single --oneshot job for APP."""
    resolved_app = resolve_cli_app_path(app)
    parsed_payload: Any = None
    if payload_json is not None:
        try:
            parsed_payload = decode_json(payload_json.encode("utf-8"))
        except msgspec.DecodeError as exc:
            msg = f"Invalid JSON passed to --payload: {exc}"
            raise click.ClickException(msg) from exc

    exit_code = execute_worker_cli(
        resolved_app,
        queues=queues,
        concurrency=concurrency,
        drain_timeout_s=drain_timeout_s,
        run_until_stopped=oneshot_job is None,
        oneshot_job=oneshot_job,
        payload=parsed_payload,
    )
    if exit_code != 0:
        sys.exit(exit_code)
