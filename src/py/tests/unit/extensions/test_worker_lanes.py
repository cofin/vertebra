"""Unit tests for multi-lane admission-controlled worker runner and CLI commands."""

import asyncio
import contextlib
import importlib
import inspect
from collections.abc import AsyncIterator, Callable, Sequence
from types import SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner

from vertebra.cli.run import run
from vertebra.cli.workers import workers
from vertebra.extensions.worker import (
    MultiLaneWorkerRunner,
    WorkerLaneConfig,
    parse_worker_lanes,
    run_oneshot_job,
)


def _load_litestar_worker_test_primitives() -> tuple[Any, Any]:
    """Load Litestar and get or fallback doubles when optional extra is absent."""
    try:
        mod = importlib.import_module("litestar")
        return mod.Litestar, mod.get
    except ImportError:

        def get_stub(path: str, **_kwargs: Any) -> Callable[[Callable[..., Any]], Any]:
            def wrap(fn: Callable[..., Any]) -> Any:
                return SimpleNamespace(paths={path}, http_methods={"GET"}, fn=fn)

            return wrap

        class LitestarStub:
            def __init__(
                self,
                route_handlers: Sequence[Any] | None = None,
                *,
                on_startup: Sequence[Callable[..., Any]] | None = None,
                on_shutdown: Sequence[Callable[..., Any]] | None = None,
                **_kwargs: Any,
            ) -> None:
                self.routes = list(route_handlers or ())
                self._on_startup = tuple(on_startup or ())
                self._on_shutdown = tuple(on_shutdown or ())
                self.state: dict[str, Any] = {}

            @contextlib.asynccontextmanager
            async def lifespan(self) -> AsyncIterator[None]:
                for hook in self._on_startup:
                    res = hook()
                    if inspect.isawaitable(res):
                        await res
                try:
                    yield
                finally:
                    for hook in self._on_shutdown:
                        res = hook()
                        if inspect.isawaitable(res):
                            await res

        return LitestarStub, get_stub


Litestar, get = _load_litestar_worker_test_primitives()


def test_parse_worker_lanes_matches_queues_and_concurrency() -> None:
    """Verify parse_worker_lanes parses comma-separated queues and concurrency limits."""
    lanes = parse_worker_lanes("default,etl,notifications", "10,2,5", drain_timeout_s=15.0)
    assert len(lanes) == 3
    assert lanes[0] == WorkerLaneConfig(queue_name="default", concurrency=10, drain_timeout_s=15.0)
    assert lanes[1] == WorkerLaneConfig(queue_name="etl", concurrency=2, drain_timeout_s=15.0)
    assert lanes[2] == WorkerLaneConfig(queue_name="notifications", concurrency=5, drain_timeout_s=15.0)

    single_conc = parse_worker_lanes("default,etl", "4")
    assert [lane.concurrency for lane in single_conc] == [4, 4]


@pytest.mark.anyio
async def test_multi_lane_worker_runner_isolates_lane_concurrency() -> None:
    """Verify MultiLaneWorkerRunner enforces independent per-lane concurrency semaphores and drains cleanly."""
    lanes = parse_worker_lanes("default,etl", "3,1", drain_timeout_s=5.0)
    runner = MultiLaneWorkerRunner(lanes)

    active_by_lane: dict[str, int] = {"default": 0, "etl": 0}
    peak_by_lane: dict[str, int] = {"default": 0, "etl": 0}
    completed: list[str] = []

    async def make_job(lane_name: str, job_id: str) -> None:
        active_by_lane[lane_name] += 1
        peak_by_lane[lane_name] = max(peak_by_lane[lane_name], active_by_lane[lane_name])
        await asyncio.sleep(0.01)
        active_by_lane[lane_name] -= 1
        completed.append(f"{lane_name}:{job_id}")

    tasks = [
        runner.submit("etl", lambda: make_job("etl", "e1")),
        runner.submit("etl", lambda: make_job("etl", "e2")),
        runner.submit("default", lambda: make_job("default", "d1")),
        runner.submit("default", lambda: make_job("default", "d2")),
        runner.submit("default", lambda: make_job("default", "d3")),
    ]
    await asyncio.gather(*tasks)
    await runner.stop()

    assert peak_by_lane["etl"] == 1
    assert peak_by_lane["default"] <= 3
    assert len(completed) == 5


@pytest.mark.anyio
async def test_run_oneshot_job_executes_inside_app_lifespan() -> None:
    """Verify run_oneshot_job enters app lifespan, runs the named job, and returns exit code 0."""
    events: list[str] = []

    async def on_startup() -> None:
        events.append("startup")

    async def on_shutdown() -> None:
        events.append("shutdown")

    async def rebuild_search_index() -> None:
        events.append("job:rebuild_search_index")

    @get("/healthz", sync_to_thread=False)
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    app = Litestar(
        route_handlers=[healthz],
        on_startup=[on_startup],
        on_shutdown=[on_shutdown],
    )
    app.state["vertebra_jobs"] = {"rebuild_search_index": rebuild_search_index}

    code = await run_oneshot_job(app, "rebuild_search_index")
    assert code == 0
    assert events == ["startup", "job:rebuild_search_index", "shutdown"]

    missing_code = await run_oneshot_job(app, "unknown_job")
    assert missing_code == 1


def test_vtb_workers_run_and_vtb_run_oneshot_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify vtb workers run and vtb run --oneshot-job CLI commands dispatch cleanly."""
    recorded: dict[str, Any] = {}

    def fake_execute_worker_cli(
        app_path: str,
        *,
        queues: str,
        concurrency: str,
        drain_timeout_s: float,
    ) -> int:
        recorded["app_path"] = app_path
        recorded["queues"] = queues
        recorded["concurrency"] = concurrency
        recorded["drain_timeout_s"] = drain_timeout_s
        return 0

    monkeypatch.setattr("vertebra.cli.workers.execute_worker_cli", fake_execute_worker_cli)
    cli_runner = CliRunner()
    res = cli_runner.invoke(
        workers,
        [
            "run",
            "example.app:app",
            "--queues",
            "default,etl",
            "--concurrency",
            "10,2",
            "--drain-timeout",
            "20",
        ],
    )
    assert res.exit_code == 0
    assert recorded == {
        "app_path": "example.app:app",
        "queues": "default,etl",
        "concurrency": "10,2",
        "drain_timeout_s": 20.0,
    }

    oneshot_recorded: dict[str, str] = {}

    def fake_oneshot_cli(app_path: str, job_name: str) -> int:
        oneshot_recorded["app_path"] = app_path
        oneshot_recorded["job_name"] = job_name
        return 0

    monkeypatch.setattr("vertebra.cli.run._execute_oneshot_cli", fake_oneshot_cli)
    run_res = cli_runner.invoke(run, ["example.app:app", "--oneshot-job", "sync_catalog", "--no-banner"])
    assert run_res.exit_code == 0
    assert oneshot_recorded == {"app_path": "example.app:app", "job_name": "sync_catalog"}
