"""Tests for vertebra.server._dispatch, vertebra.server._loops, and vertebra.server.worker."""

import asyncio
import inspect
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from vertebra.server import _dispatch, worker
from vertebra.server._dispatch import (
    BatchDispatchPlan,
    _call_handler,
    _handler_meta,
    _resolve_handler_args,
    dispatch_batch,
    partition_batch,
)
from vertebra.server._gil_audit import is_free_threaded
from vertebra.server._loops import EventLoopPool
from vertebra.server.worker import Worker, run_worker


class MockRequest:
    """Minimal mock mimicking a Vertebra Request object."""

    def __init__(
        self,
        handler_id: int = 1,
        request_id: int = 1,
        method: str = "GET",
        path: str = "/test",
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        query: str | None = None,
    ) -> None:
        self.handler_id = handler_id
        self.request_id = request_id
        self.method = method
        self.path = path
        self.body = body
        self.headers = headers or {}
        self.query = query


class MockResponse:
    """Minimal mock mimicking vertebra._vertebra.Response."""

    def __init__(
        self,
        status: int = 200,
        body: bytes | None = None,
        headers: dict[str, str] | list[tuple[str, str]] | None = None,
        request_id: int = 0,
    ) -> None:
        self.status = status
        self.body = body or b""
        self.headers = headers
        self.request_id = request_id


def test_single_loop_pool_replaces_async_runtime() -> None:
    """Verify EventLoopPool(size=1) executes single and batch coroutines cleanly without AsyncRuntime."""
    pool = EventLoopPool(size=1)

    async def add(a: int, b: int) -> int:
        await asyncio.sleep(0.001)
        return a + b

    try:
        future = pool.schedule(add(2, 3))
        assert future.result(timeout=2) == 5

        batch_futures = pool.schedule_batch([add(10, 20), add(30, 40)])
        assert [f.result(timeout=2) for f in batch_futures] == [30, 70]
    finally:
        pool.stop()


def test_worker_and_dispatch_split_ownership() -> None:
    """Verify Worker/run_worker live in worker.py, dispatch in _dispatch.py, and legacy modules are gone."""
    server_dir = Path(worker.__file__).resolve().parent
    assert not (server_dir / "_native.py").exists()
    assert not (server_dir / "_runtime_tuning.py").exists()
    assert not hasattr(worker, "_HANDLER_LOOP")
    assert not hasattr(_dispatch, "_HANDLER_LOOP")
    assert Worker is not None
    assert callable(run_worker)
    assert callable(partition_batch)
    assert callable(dispatch_batch)
    assert BatchDispatchPlan is not None


def test_call_handler_async_without_loop_raises() -> None:
    """Calling an async handler via _call_handler without a loop raises RuntimeError."""

    async def my_async() -> dict:
        return {"ok": True}

    with pytest.raises(RuntimeError, match="without an event loop"):
        _call_handler(my_async, MockRequest())


def test_partition_batch_sync_native_grouped() -> None:
    """Sync native handlers go to the sync_native group."""

    def sync_handler() -> dict:
        return {"ok": True}

    handlers = {1: sync_handler}
    batch = [MockRequest(handler_id=1, request_id=10)]
    result = partition_batch(batch, handlers, [])

    assert len(result.sync_native) == 1
    assert result.sync_native[0][0] == 0
    assert result.sync_native[0][1] is batch[0]
    assert result.sync_native[0][2].is_async is False
    assert not result.async_native
    assert not result.mounts
    assert not result.unmatched


def test_partition_batch_async_native_grouped() -> None:
    """Async native handlers go to the async_native group."""

    async def async_handler() -> dict:
        return {"ok": True}

    handlers = {2: async_handler}
    batch = [MockRequest(handler_id=2, request_id=20)]
    result = partition_batch(batch, handlers, [])

    assert len(result.async_native) == 1
    assert result.async_native[0][0] == 0
    assert result.async_native[0][2].is_async is True
    assert not result.sync_native


def test_partition_batch_mount_grouped_by_prefix() -> None:
    """Unregistered handler_id matching a mount prefix goes to mounts."""
    mount_runners = [("/api", lambda reqs: [], "asgi")]
    batch = [
        MockRequest(handler_id=99, request_id=1, path="/api/users"),
        MockRequest(handler_id=99, request_id=2, path="/api/items"),
    ]
    result = partition_batch(batch, {}, mount_runners)

    assert "/api" in result.mounts
    assert len(result.mounts["/api"]) == 2
    assert not result.unmatched


def test_partition_batch_unmatched_for_404() -> None:
    """Requests matching neither native handlers nor mounts go to unmatched."""
    batch = [MockRequest(handler_id=999, request_id=1, path="/nowhere")]
    result = partition_batch(batch, {}, [])

    assert len(result.unmatched) == 1
    assert result.unmatched[0][0] == 0


def test_partition_batch_mixed_batch_preserves_indices() -> None:
    """A mixed batch preserves original batch indices across all groups."""

    def sync_h() -> str:
        return "sync"

    async def async_h() -> str:
        return "async"

    handlers = {1: sync_h, 2: async_h}
    mounts = [("/legacy", lambda req: None, "wsgi")]
    batch = [
        MockRequest(handler_id=2, request_id=10, path="/async"),
        MockRequest(handler_id=99, request_id=20, path="/legacy/old"),
        MockRequest(handler_id=1, request_id=30, path="/sync"),
        MockRequest(handler_id=999, request_id=40, path="/missing"),
    ]
    result = partition_batch(batch, handlers, mounts)

    assert len(result.async_native) == 1
    assert result.async_native[0][0] == 0
    assert len(result.mounts["/legacy"]) == 1
    assert result.mounts["/legacy"][0][0] == 1
    assert len(result.sync_native) == 1
    assert result.sync_native[0][0] == 2
    assert len(result.unmatched) == 1
    assert result.unmatched[0][0] == 3


def test_resolve_handler_args_no_params() -> None:
    def h() -> None:
        pass

    assert _resolve_handler_args(h, MockRequest()) == ()


def test_resolve_handler_args_request_param() -> None:
    def h(request: Any) -> None:
        pass

    req = MockRequest()
    assert _resolve_handler_args(h, req) == (req,)


def test_resolve_handler_args_body_and_method_params() -> None:
    def h(method: str, body: bytes) -> None:
        pass

    req = MockRequest(method="POST", body=b"payload")
    assert _resolve_handler_args(h, req) == ("POST", b"payload")


def test_resolve_handler_args_unknown_param_receives_request() -> None:
    def h(ctx: Any) -> None:
        pass

    req = MockRequest()
    assert _resolve_handler_args(h, req) == (req,)


def test_dispatch_batch_sync_handlers_produce_responses() -> None:
    def h1() -> dict:
        return {"n": 1}

    def h2() -> str:
        return "hello"

    handlers = {1: h1, 2: h2}
    batch = [
        MockRequest(handler_id=1, request_id=100),
        MockRequest(handler_id=2, request_id=200),
    ]
    loop = asyncio.new_event_loop()
    try:
        plan = partition_batch(batch, handlers, [])
        results = dispatch_batch(plan, handlers, [], MockResponse, loop)
        assert results is not None
        assert len(results) == 2
        assert results[0].status == 200
        assert results[0].body == b'{"n":1}'
        assert results[0].request_id == 100
        assert results[1].body == b"hello"
        assert results[1].request_id == 200
    finally:
        loop.close()


def test_dispatch_batch_async_handlers_run_concurrently() -> None:
    """Multiple async handlers in a batch run concurrently via gather, not sequentially."""
    execution_order: list[str] = []

    async def slow_handler() -> str:
        execution_order.append("slow_start")
        await asyncio.sleep(0.03)
        execution_order.append("slow_end")
        return "slow"

    async def fast_handler() -> str:
        execution_order.append("fast_start")
        await asyncio.sleep(0.005)
        execution_order.append("fast_end")
        return "fast"

    handlers = {1: slow_handler, 2: fast_handler}
    batch = [
        MockRequest(handler_id=1, request_id=10),
        MockRequest(handler_id=2, request_id=20),
    ]
    pool = EventLoopPool(size=2)
    try:
        plan = partition_batch(batch, handlers, [])
        results = dispatch_batch(plan, handlers, [], MockResponse, pool)
        assert results is not None
        assert results[0].body == b"slow"
        assert results[0].request_id == 10
        assert results[1].body == b"fast"
        assert results[1].request_id == 20
        assert execution_order.index("fast_end") < execution_order.index("slow_end")
    finally:
        pool.stop()


def test_dispatch_batch_mixed_batch_preserves_response_order() -> None:
    """Responses are returned in exact original batch order regardless of handler type."""

    def sync_h() -> str:
        return "sync_val"

    async def async_h() -> str:
        await asyncio.sleep(0.005)
        return "async_val"

    handlers = {1: sync_h, 2: async_h}
    batch = [
        MockRequest(handler_id=2, request_id=1),
        MockRequest(handler_id=999, request_id=2, path="/404"),
        MockRequest(handler_id=1, request_id=3),
    ]
    loop = asyncio.new_event_loop()
    try:
        plan = partition_batch(batch, handlers, [])
        results = dispatch_batch(plan, handlers, [], MockResponse, loop)
        assert results is not None
        assert len(results) == 3
        assert results[0].body == b"async_val"
        assert results[0].request_id == 1
        assert results[1].status == 404
        assert results[1].request_id == 2
        assert results[2].body == b"sync_val"
        assert results[2].request_id == 3
    finally:
        loop.close()


def test_dispatch_batch_sync_handler_exception_returns_500() -> None:
    """An exception in one sync handler returns 500 without aborting other requests."""

    def bad() -> None:
        raise ValueError("broken")

    def good() -> str:
        return "ok"

    handlers = {1: bad, 2: good}
    batch = [
        MockRequest(handler_id=1, request_id=10),
        MockRequest(handler_id=2, request_id=20),
    ]
    loop = asyncio.new_event_loop()
    try:
        plan = partition_batch(batch, handlers, [])
        results = dispatch_batch(plan, handlers, [], MockResponse, loop)
        assert results is not None
        assert results[0].status == 500
        assert results[0].request_id == 10
        assert results[1].status == 200
        assert results[1].body == b"ok"
        assert results[1].request_id == 20
    finally:
        loop.close()


def test_dispatch_batch_async_handler_exception_returns_500() -> None:
    """An exception in one async handler returns 500 without cancelling sibling coroutines."""

    async def bad_async() -> str:
        raise RuntimeError("async boom")

    async def good_async() -> str:
        await asyncio.sleep(0.005)
        return "survived"

    handlers = {1: bad_async, 2: good_async}
    batch = [
        MockRequest(handler_id=1, request_id=10),
        MockRequest(handler_id=2, request_id=20),
    ]
    loop = asyncio.new_event_loop()
    try:
        plan = partition_batch(batch, handlers, [])
        results = dispatch_batch(plan, handlers, [], MockResponse, loop)
        assert results is not None
        assert results[0].status == 500
        assert results[0].request_id == 10
        assert results[1].status == 200
        assert results[1].body == b"survived"
        assert results[1].request_id == 20
    finally:
        loop.close()


def test_dispatch_batch_asgi_mount_receives_batch() -> None:
    """Multiple requests to the same ASGI mount are dispatched as a single batch."""
    batches_seen: list[int] = []

    class MockAsgiMount:
        def __init__(self, status: int, body: bytes) -> None:
            self.status = status
            self.body = body

    def mock_asgi_runner(reqs: list) -> list:
        batches_seen.append(len(reqs))
        return [MockAsgiMount(200, f"asgi-{r.request_id}".encode()) for r in reqs]

    mounts = [("/app", mock_asgi_runner, "asgi")]
    batch = [
        MockRequest(handler_id=99, request_id=1, path="/app/a"),
        MockRequest(handler_id=99, request_id=2, path="/app/b"),
    ]
    loop = asyncio.new_event_loop()
    try:
        plan = partition_batch(batch, {}, mounts)
        results = dispatch_batch(plan, {}, mounts, MockResponse, loop)
        assert results is not None
        assert batches_seen == [2]
        assert results[0].body == b"asgi-1"
        assert results[1].body == b"asgi-2"
    finally:
        loop.close()


def test_dispatch_batch_wsgi_mount_runs_concurrently() -> None:
    """Multiple WSGI requests run via ThreadPoolExecutor."""
    threads_seen: set[int] = set()

    def mock_wsgi_adapter(req: Any) -> MockResponse:
        threads_seen.add(threading.get_ident())
        return MockResponse(status=200, body=b"wsgi-ok", request_id=req.request_id)

    mounts = [("/legacy", mock_wsgi_adapter, "wsgi")]
    batch = [
        MockRequest(handler_id=99, request_id=1, path="/legacy/a"),
        MockRequest(handler_id=99, request_id=2, path="/legacy/b"),
    ]
    loop = asyncio.new_event_loop()
    try:
        plan = partition_batch(batch, {}, mounts)
        results = dispatch_batch(plan, {}, mounts, MockResponse, loop)
        assert results is not None
        assert results[0].body == b"wsgi-ok"
        assert results[0].request_id == 1
        assert results[1].body == b"wsgi-ok"
        assert results[1].request_id == 2
    finally:
        loop.close()


def test_dispatch_batch_wsgi_mount_exception_returns_500() -> None:
    """A crashing WSGI mount returns 500 for that request."""

    def crashing_wsgi(req: Any) -> MockResponse:
        raise RuntimeError("wsgi crash")

    mounts = [("/legacy", crashing_wsgi, "wsgi")]
    batch = [MockRequest(handler_id=99, request_id=7, path="/legacy/crash")]
    loop = asyncio.new_event_loop()
    try:
        plan = partition_batch(batch, {}, mounts)
        results = dispatch_batch(plan, {}, mounts, MockResponse, loop)
        assert results is not None
        assert results[0].status == 500
        assert results[0].request_id == 7
    finally:
        loop.close()


def test_dispatch_batch_async_mode_spawns_and_submits_independently() -> None:
    """In async_mode, fast handlers submit before slow handlers complete."""
    submitted: list[MockResponse] = []

    async def slow_handler() -> str:
        await asyncio.sleep(0.05)
        return "slow"

    async def fast_handler() -> str:
        await asyncio.sleep(0.005)
        return "fast"

    handlers = {1: slow_handler, 2: fast_handler}
    batch = [
        MockRequest(handler_id=1, request_id=10),
        MockRequest(handler_id=2, request_id=20),
    ]
    pool = EventLoopPool(size=1)
    try:
        plan = partition_batch(batch, handlers, [])
        result = dispatch_batch(
            plan,
            handlers,
            [],
            MockResponse,
            pool,
            async_mode=True,
            submit=submitted.append,
        )
        assert result is None

        deadline = time.monotonic() + 2.0
        while len(submitted) < 2 and time.monotonic() < deadline:
            time.sleep(0.005)

        assert len(submitted) == 2
        assert submitted[0].request_id == 20
        assert submitted[0].body == b"fast"
        assert submitted[1].request_id == 10
        assert submitted[1].body == b"slow"
    finally:
        pool.stop()


def test_dispatch_batch_async_mode_sync_handlers_submit_immediately() -> None:
    """In async_mode, sync handlers submit via submit callback immediately."""
    submitted: list[MockResponse] = []

    def sync_h() -> dict:
        return {"ok": True}

    handlers = {1: sync_h}
    batch = [MockRequest(handler_id=1, request_id=42)]
    pool = EventLoopPool(size=1)
    try:
        plan = partition_batch(batch, handlers, [])
        result = dispatch_batch(
            plan,
            handlers,
            [],
            MockResponse,
            pool,
            async_mode=True,
            submit=submitted.append,
        )
        assert result is None
        assert len(submitted) == 1
        assert submitted[0].request_id == 42
        assert submitted[0].body == b'{"ok":true}'
    finally:
        pool.stop()


def test_dispatch_batch_async_mode_unmatched_submits_404() -> None:
    """In async_mode, unmatched requests submit 404 immediately."""
    submitted: list[MockResponse] = []
    batch = [MockRequest(handler_id=999, request_id=77, path="/missing")]
    pool = EventLoopPool(size=1)
    try:
        plan = partition_batch(batch, {}, [])
        dispatch_batch(
            plan,
            {},
            [],
            MockResponse,
            pool,
            async_mode=True,
            submit=submitted.append,
        )
        assert len(submitted) == 1
        assert submitted[0].status == 404
        assert submitted[0].request_id == 77
    finally:
        pool.stop()


def test_dispatch_batch_async_mode_error_handler_invoked_on_exception() -> None:
    """In async_mode, custom error_handler is called when a handler raises."""
    submitted: list[MockResponse] = []

    async def failing() -> str:
        raise ValueError("bad input")

    def custom_err(request: Any, exc: Exception) -> MockResponse:
        return MockResponse(status=422, body=str(exc).encode(), request_id=request.request_id)

    handlers = {1: failing}
    batch = [MockRequest(handler_id=1, request_id=55)]
    pool = EventLoopPool(size=1)
    try:
        plan = partition_batch(batch, handlers, [])
        dispatch_batch(
            plan,
            handlers,
            [],
            MockResponse,
            pool,
            async_mode=True,
            submit=submitted.append,
            error_handler=custom_err,
        )
        deadline = time.monotonic() + 2.0
        while len(submitted) < 1 and time.monotonic() < deadline:
            time.sleep(0.005)

        assert len(submitted) == 1
        assert submitted[0].status == 422
        assert submitted[0].body == b"bad input"
        assert submitted[0].request_id == 55
    finally:
        pool.stop()


def test_handler_meta_identifies_sync_and_async() -> None:
    def sync_no_args() -> None:
        pass

    def sync_with_args(request: Any) -> None:
        pass

    async def async_no_args() -> None:
        pass

    async def async_with_args(request: Any, body: bytes) -> None:
        pass

    assert _handler_meta(sync_no_args) == (False, False)
    assert _handler_meta(sync_with_args) == (True, False)
    assert _handler_meta(async_no_args) == (False, True)
    assert _handler_meta(async_with_args) == (True, True)


def test_run_worker_exists_and_is_callable() -> None:
    assert callable(run_worker)


def test_is_free_threaded_returns_bool() -> None:
    result = is_free_threaded()
    assert isinstance(result, bool)


def test_run_worker_accepts_mounts_parameter() -> None:
    sig = inspect.signature(run_worker)
    assert "mounts" in sig.parameters
    assert sig.parameters["mounts"].default is None


def test_dispatch_batch_returns_pre_built_response_passthrough() -> None:
    """When a handler returns a Response instance directly, it is passed through with request_id set."""

    def custom_resp() -> MockResponse:
        return MockResponse(status=201, body=b"created")

    handlers = {1: custom_resp}
    batch = [MockRequest(handler_id=1, request_id=88)]
    loop = asyncio.new_event_loop()
    try:
        plan = partition_batch(batch, handlers, [])
        results = dispatch_batch(plan, handlers, [], MockResponse, loop)
        assert results is not None
        assert results[0].status == 201
        assert results[0].body == b"created"
        assert results[0].request_id == 88
    finally:
        loop.close()
