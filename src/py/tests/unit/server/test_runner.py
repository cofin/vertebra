"""Tests for vertebra.server._dispatch, vertebra.server._loops, and vertebra.server.worker."""

import asyncio
import gc
import inspect
import os
import threading
import weakref
from collections.abc import AsyncIterator, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Annotated, Any

import msgspec
import pytest

from vertebra._vertebra import Request, init_transport
from vertebra.build.codegen.markers import FromCookie, FromHeader, FromQuery, MsgPackBody, Parameter
from vertebra.framework._app import App
from vertebra.framework._router import Controller, Router, get, post
from vertebra.framework.di import Inject, Provide
from vertebra.framework.security import Principal, requires_role
from vertebra.server import _dispatch, worker
from vertebra.server._asgi._runner import AsgiRunner
from vertebra.server._dispatch import (
    _JSON_CONTENT_TYPE_COMPACT,
    _MOUNT_HANDLER_MASK,
    _ZPAGES_HANDLER_ID,
    BatchDispatchPlan,
    _call_handler,
    _handler_meta,
    _resolve_handler_args,
    compile_handler_table,
    dispatch_batch,
    get_worker_executor,
    partition_batch,
    shutdown_worker_executor,
)
from vertebra.server._gil_audit import is_free_threaded
from vertebra.server._loops import _TASKZ_PROBE_TASK_NAME, EventLoopPool
from vertebra.server.platform import ZPagesRegistry
from vertebra.server.worker import Worker, WorkerStartupError, _AsgiMountRunner, _instantiate_mount_runners, run_worker
from vertebra.telemetry.otel import EventLoopRecorder
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json, encode_json_bytes, encode_msgpack_bytes


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


class _SubmitCollector:
    """Thread-safe submission collector using a condition variable instead of polling."""

    def __init__(self) -> None:
        self.items: list[MockResponse] = []
        self._cond = threading.Condition()

    def append(self, response: MockResponse) -> None:
        with self._cond:
            self.items.append(response)
            self._cond.notify_all()

    def wait_for(self, count: int, timeout: float = 2.0) -> list[MockResponse]:
        with self._cond:
            if not self._cond.wait_for(lambda: len(self.items) >= count, timeout=timeout):
                msg = f"Timed out waiting for {count} submissions; got {len(self.items)}"
                raise TimeoutError(msg)
            return self.items


def test_single_loop_pool_replaces_async_runtime() -> None:
    """Verify EventLoopPool(size=1) executes single and batch coroutines cleanly without AsyncRuntime."""
    pool = EventLoopPool(size=1)

    async def add(a: int, b: int) -> int:
        await asyncio.sleep(0)
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

    async def my_async() -> dict[str, bool]:
        return {"ok": True}

    with pytest.raises(RuntimeError, match="without an event loop"):
        _call_handler(my_async, MockRequest())


def test_partition_batch_sync_native_grouped() -> None:
    """Sync native handlers go to the sync_native group."""

    def sync_handler() -> dict[str, bool]:
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

    async def async_handler() -> dict[str, bool]:
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
    mount_runners: list[tuple[str, Any, str]] = [("/api", lambda reqs: [], "asgi")]
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
    def h1() -> dict[str, int]:
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
        await asyncio.sleep(0)
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
        await asyncio.sleep(0)
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
        await asyncio.sleep(0)
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

    def mock_asgi_runner(reqs: list[Any]) -> list[MockAsgiMount]:
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
    collector = _SubmitCollector()

    async def slow_handler() -> str:
        await asyncio.sleep(0.05)
        return "slow"

    async def fast_handler() -> str:
        await asyncio.sleep(0)
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
            submit=collector.append,
        )
        assert result is None

        submitted = collector.wait_for(2)
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

    def sync_h() -> dict[str, bool]:
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
    collector = _SubmitCollector()

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
            submit=collector.append,
            error_handler=custom_err,
        )
        submitted = collector.wait_for(1)
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


class _CountingExecutor(ThreadPoolExecutor):
    """ThreadPoolExecutor test double recording submit call counts."""

    def __init__(self) -> None:
        super().__init__(max_workers=2, thread_name_prefix="test-exec")
        self.submit_calls = 0

    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Future[Any]:
        self.submit_calls += 1
        return super().submit(fn, *args, **kwargs)


class _NoPathAccessRequest:
    """Request test double that fails if path prefix scanning is attempted."""

    def __init__(self, handler_id: int, request_id: int) -> None:
        self.handler_id = handler_id
        self.request_id = request_id

    @property
    def path(self) -> str:
        msg = "path must not be accessed when handler_id encodes a mount index"
        raise AssertionError(msg)


def test_sync_native_batch_runs_inline_without_executor_or_double_execution() -> None:
    """Default sync native routes run inline without ThreadPoolExecutor or single-item double execution."""
    init_transport()
    caller_tid = threading.get_ident()
    seen_tids: list[int] = []
    dict_calls = 0

    def h_bytes() -> bytes:
        seen_tids.append(threading.get_ident())
        return b"ok"

    def h_dict() -> dict[str, int]:
        nonlocal dict_calls
        dict_calls += 1
        seen_tids.append(threading.get_ident())
        return {"count": dict_calls}

    compiled = compile_handler_table({1: h_bytes, 2: h_dict})
    exec_spy = _CountingExecutor()
    pool = EventLoopPool(size=1)
    w = Worker({1: h_bytes, 2: h_dict})
    try:
        single_dict_batch = [Request.from_parts("GET", "/json", b"", request_id=901, handler_id=2)]
        w._process_batch(
            single_dict_batch,
            compiled_handlers=compiled,
            mount_runners=[],
            mount_lookup={},
            all_sync_native=True,
            loop_pool=pool,
            executor=exec_spy,
        )
        assert dict_calls == 1
        assert exec_spy.submit_calls == 0

        multi_batch = [
            Request.from_parts("GET", "/b1", b"", request_id=902, handler_id=1),
            Request.from_parts("GET", "/b2", b"", request_id=903, handler_id=1),
            Request.from_parts("GET", "/j1", b"", request_id=904, handler_id=2),
        ]
        w._process_batch(
            multi_batch,
            compiled_handlers=compiled,
            mount_runners=[],
            mount_lookup={},
            all_sync_native=True,
            loop_pool=pool,
            executor=exec_spy,
        )
        assert dict_calls == 2
        assert exec_spy.submit_calls == 0
        assert all(tid == caller_tid for tid in seen_tids)
    finally:
        exec_spy.shutdown(wait=True)
        pool.stop()


def test_sync_to_thread_route_executes_once_on_executor() -> None:
    """Routes decorated with sync_to_thread=True offload once to ThreadPoolExecutor while defaults stay inline."""
    app = App()
    caller_tid = threading.get_ident()
    fast_tids: list[int] = []
    blocking_tids: list[int] = []

    @app.get("/fast")
    def fast_route() -> bytes:
        fast_tids.append(threading.get_ident())
        return b"fast"

    @app.get("/blocking", sync_to_thread=True)
    def blocking_route() -> bytes:
        blocking_tids.append(threading.get_ident())
        return b"blocking"

    router = Router(prefix="/v1")

    @get("/slow", sync_to_thread=True)
    def router_blocking() -> bytes:
        blocking_tids.append(threading.get_ident())
        return b"router-slow"

    router.register(router_blocking)
    app.register(router)

    compiled = compile_handler_table(app._handlers)
    fast_id = next(hid for hid, info in app.handler_index.items() if info.path == "/fast")
    block_id = next(hid for hid, info in app.handler_index.items() if info.path == "/blocking")
    router_id = next(hid for hid, info in app.handler_index.items() if info.path == "/v1/slow")

    assert compiled[fast_id].sync_to_thread is False
    assert compiled[block_id].sync_to_thread is True
    assert compiled[router_id].sync_to_thread is True

    collector = _SubmitCollector()
    exec_spy = _CountingExecutor()
    pool = EventLoopPool(size=1)
    try:
        batch = [
            MockRequest(handler_id=fast_id, request_id=101, path="/fast"),
            MockRequest(handler_id=block_id, request_id=102, path="/blocking"),
            MockRequest(handler_id=router_id, request_id=103, path="/v1/slow"),
        ]
        plan = partition_batch(batch, compiled, [])
        dispatch_batch(
            plan,
            compiled,
            [],
            MockResponse,
            pool,
            async_mode=True,
            submit=collector.append,
            executor=exec_spy,
        )
        submitted = collector.wait_for(3)
        assert len(submitted) == 3
        assert exec_spy.submit_calls == 2
        assert fast_tids == [caller_tid]
        assert len(blocking_tids) == 2
        assert all(tid != caller_tid for tid in blocking_tids)
    finally:
        exec_spy.shutdown(wait=True)
        pool.stop()


def test_memoryview_and_bytearray_responses_serialize_zero_copy() -> None:
    """Handlers returning req.body_view (memoryview) or bytearray serialize directly without TypeError."""
    init_transport()

    def echo_view(req: Request) -> memoryview:
        return req.body_view

    def echo_bytearray() -> bytearray:
        return bytearray(b"mutable-bytes")

    handlers = compile_handler_table({1: echo_view, 2: echo_bytearray})
    req1 = Request.from_parts("POST", "/echo", b"zero-copy-body", request_id=910, handler_id=1)
    req2 = Request.from_parts("GET", "/ba", b"", request_id=911, handler_id=2)

    raw_calls: list[tuple[int, int, bytes]] = []

    def capture_raw(req_id: int, status: int, body: Any, headers: Any = None) -> None:
        del headers
        raw_calls.append((req_id, status, bytes(body)))

    plan = partition_batch([req1, req2], handlers, [])
    dispatch_batch(
        plan,
        handlers,
        [],
        MockResponse,
        None,
        async_mode=True,
        submit_raw=capture_raw,
    )
    assert raw_calls == [
        (910, 200, b"zero-copy-body"),
        (911, 200, b"mutable-bytes"),
    ]


def test_async_dispatch_unpins_request_before_await() -> None:
    """Async handlers that do not retain Request release their Request reference before suspending."""
    entered = threading.Event()
    release_gate: asyncio.Event | None = None
    loop_holder: list[asyncio.AbstractEventLoop] = []
    collector = _SubmitCollector()

    async def waiting_handler() -> bytes:
        nonlocal release_gate
        loop_holder.append(asyncio.get_running_loop())
        release_gate = asyncio.Event()
        entered.set()
        await release_gate.wait()
        return b"done"

    compiled = compile_handler_table({1: waiting_handler})
    pool = EventLoopPool(size=1)
    try:
        req = MockRequest(handler_id=1, request_id=707)
        weak_req = weakref.ref(req)
        plan = partition_batch([req], compiled, [])
        dispatch_batch(
            plan,
            compiled,
            [],
            MockResponse,
            pool,
            async_mode=True,
            submit=collector.append,
        )
        del req
        del plan
        assert entered.wait(timeout=2.0)
        gc.collect()
        assert weak_req() is None
        assert release_gate is not None
        loop_holder[0].call_soon_threadsafe(release_gate.set)
        submitted = collector.wait_for(1)
        assert len(submitted) == 1
        assert submitted[0].body == b"done"
    finally:
        pool.stop()


def test_partition_batch_o1_mount_handler_id_without_path_scan() -> None:
    """Mount handler IDs (both negative and MOUNT_HANDLER_BASE-tagged) resolve in O(1) without reading path."""
    mounts = [
        ("/first", lambda reqs: reqs, "asgi"),
        ("/second", lambda reqs: reqs, "rsgi"),
    ]
    req_first = _NoPathAccessRequest(handler_id=-1, request_id=1)
    req_second = _NoPathAccessRequest(handler_id=0xFFFF_0000_0000_0000 | 1, request_id=2)
    plan = partition_batch([req_first, req_second], {}, mounts)
    assert [r.request_id for _i, r in plan.mounts["/first"]] == [1]
    assert [r.request_id for _i, r in plan.mounts["/second"]] == [2]
    assert not plan.unmatched


def test_worker_inline_event_loop_add_reader_drains_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same-thread uvloop add_reader loop drains SHM batches on a single thread and honours max_batches."""
    init_transport()
    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    seen_threads: list[int] = []
    pending_batches: list[list[Request]] = [
        [Request.from_parts("GET", "/a", b"", request_id=990, handler_id=1)],
    ]

    async def async_route() -> bytes:
        seen_threads.append(threading.get_ident())
        return b"inline-loop"

    monkeypatch.setattr(worker, "try_get_batch", lambda: pending_batches.pop(0) if pending_batches else None)
    monkeypatch.setattr(worker, "clear_request_notification", lambda: None)

    pool = EventLoopPool(size=1, inline_single_loop=True)
    w = Worker({1: async_route}, max_batches=1, batch_timeout_ms=10)
    compiled = compile_handler_table({1: async_route})
    try:
        os.write(write_fd, b"\x01")
        w._run_inline_event_loop(
            notify_fd=read_fd,
            timeout_sec=0.01,
            compiled_handlers=compiled,
            mount_runners=[],
            mount_lookup={},
            all_sync_native=False,
            single_root_asgi=None,
            single_root_rsgi=None,
            loop_pool=pool,
            executor=None,
        )
        assert seen_threads == [threading.get_ident()]
    finally:
        pool.stop()
        os.close(read_fd)
        os.close(write_fd)


def test_worker_process_batch_single_root_asgi_dispatches_via_fastpath_wrapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Worker._process_batch passes single_root_asgi.dispatch_app so __vertebra_fastpath__ executes."""

    class _FakeFastPath:
        async def dispatch(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def wrap_asgi(self, fallback_app: Any) -> Any:
            async def _wrapped(scope: Any, receive: Any, send: Any) -> None:
                await fallback_app(scope, receive, send)

            return _wrapped

    class _RootAsgiApp:
        def __init__(self) -> None:
            self.__vertebra_fastpath__ = _FakeFastPath()

        async def __call__(self, _scope: Any, _receive: Any, send: Any) -> None:
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok", "more_body": False})

    class _RequestBatchStub:
        def __len__(self) -> int:
            return 1

        def __getitem__(self, _index: int) -> MockRequest:
            return MockRequest(handler_id=0)

    app = _RootAsgiApp()
    asgi_runner = AsgiRunner(app)
    captured_apps: list[Any] = []

    def _spy_drive_asgi_batch(target_app: Any, batch_obj: Any, **_kwargs: Any) -> None:
        _ = batch_obj
        captured_apps.append(target_app)

    monkeypatch.setattr(worker, "RequestBatch", _RequestBatchStub)
    monkeypatch.setattr(worker, "drive_asgi_batch", _spy_drive_asgi_batch)

    pool = EventLoopPool(size=1)
    w = Worker({}, mounts=[("/", asgi_runner, "asgi")])
    try:
        assert asgi_runner.dispatch_app is not app
        w._process_batch(
            _RequestBatchStub(),
            compiled_handlers={},
            mount_runners=[("/", asgi_runner, "asgi")],
            mount_lookup={"/": (asgi_runner, "asgi")},
            all_sync_native=False,
            single_root_asgi=asgi_runner,
            single_root_rsgi=None,
            loop_pool=pool,
            executor=None,
        )
        assert captured_apps == [asgi_runner.dispatch_app]
    finally:
        asgi_runner.close()
        pool.stop()


class _OrderInput(msgspec.Struct, frozen=True, gc=False):
    """Typed request payload for native dispatch tests."""

    sku: str
    qty: int


class _OrderOutput(msgspec.Struct, frozen=True, gc=False):
    """Typed response payload for native dispatch tests."""

    order_id: int
    sku: str
    qty: int
    tenant: str
    source: str


class _TenantService:
    """Service resolved via hierarchical DI."""

    def __init__(self, prefix: str = "acme") -> None:
        self.prefix = prefix

    def format_tenant(self, raw_tenant: str) -> str:
        return f"{self.prefix}:{raw_tenant}"


def test_native_dispatch_typed_params_json_msgpack_di_guards_and_generator_teardown() -> None:
    """Verify native dispatch with typed path/query/header/cookie, Struct bodies, DI, generators, and guards."""
    cleanup_log: list[str] = []

    def provide_sync_tx() -> Iterator[str]:
        cleanup_log.append("tx_open")
        try:
            yield "sync-tx"
        finally:
            cleanup_log.append("tx_close")

    async def provide_async_audit(request: Any) -> AsyncIterator[str]:
        cleanup_log.append(f"audit_open:{request.path}")
        try:
            yield "async-audit"
        finally:
            cleanup_log.append(f"audit_close:{request.path}")

    def provide_principal(request: Any) -> Principal:
        role = (request.headers or {}).get("x-role", "guest")
        return Principal(subject="u-1", roles=frozenset({role}))

    class OrderController(Controller):
        path = "/orders"
        dependencies = {"tenant_svc": _TenantService("ctrl")}

        @post("/{order_id:int}", status_code=201)
        def create_order(
            self,
            order_id: int,
            payload: _OrderInput,
            tenant_svc: _TenantService,
            tx: Annotated[str, Inject("sync_tx")],
            source: FromQuery[str] = "web",
            x_tenant: Annotated[str, Parameter(header="x-tenant")] = "default",
            session_id: FromCookie[str] = "anon",
        ) -> _OrderOutput:
            return _OrderOutput(
                order_id=order_id,
                sku=payload.sku,
                qty=payload.qty,
                tenant=f"{tenant_svc.format_tenant(x_tenant)}:{session_id}",
                source=f"{source}:{tx}",
            )

        @post("/msgpack/{order_id:int}")
        def create_order_msgpack(
            self,
            order_id: int,
            payload: MsgPackBody[_OrderInput],
            x_trace: FromHeader[str] = "none",
        ) -> dict[str, Any]:
            return {"id": order_id, "sku": payload.sku, "qty": payload.qty, "trace": x_trace}

        @get("/async-promoted/{order_id:int}")
        def sync_handler_with_async_dep(
            self,
            order_id: int,
            audit: Annotated[str, Inject("async_audit")],
        ) -> dict[str, Any]:
            return {"id": order_id, "audit": audit}

        @get("/admin", guards=(requires_role("admin"),))
        def admin_only(self) -> dict[str, bool]:
            return {"admin": True}

    app = App(
        route_handlers=[OrderController],
        dependencies={
            "sync_tx": Provide(provide_sync_tx, scope="request"),
            "async_audit": Provide(provide_async_audit, scope="request"),
            "principal": Provide(provide_principal, scope="request"),
        },
    )
    compiled = compile_handler_table(app._handlers)

    create_hid = next(hid for hid, info in app.handler_index.items() if info.path == "/orders/{order_id:int}")
    msgpack_hid = next(hid for hid, info in app.handler_index.items() if info.path == "/orders/msgpack/{order_id:int}")
    promoted_hid = next(
        hid for hid, info in app.handler_index.items() if info.path == "/orders/async-promoted/{order_id:int}"
    )
    admin_hid = next(hid for hid, info in app.handler_index.items() if info.path == "/orders/admin")

    assert compiled[create_hid].is_async is False
    assert compiled[create_hid].status_code == 201
    assert compiled[promoted_hid].is_async is True

    batch = [
        MockRequest(
            handler_id=create_hid,
            request_id=1,
            method="POST",
            path="/orders/42",
            query="source=mobile",
            headers={"x-tenant": "eu", "Cookie": "session_id=s-99; theme=dark"},
            body=encode_json_bytes(_OrderInput(sku="SKU-7", qty=3)),
        ),
        MockRequest(
            handler_id=msgpack_hid,
            request_id=2,
            method="POST",
            path="/orders/msgpack/77",
            headers={"x-trace": "tr-1"},
            body=encode_msgpack_bytes(_OrderInput(sku="MP-1", qty=5)),
        ),
        MockRequest(
            handler_id=promoted_hid,
            request_id=3,
            method="GET",
            path="/orders/async-promoted/15",
        ),
        MockRequest(
            handler_id=admin_hid,
            request_id=4,
            method="GET",
            path="/orders/admin",
            headers={"x-role": "guest"},
        ),
        MockRequest(
            handler_id=admin_hid,
            request_id=5,
            method="GET",
            path="/orders/admin",
            headers={"x-role": "admin"},
        ),
    ]

    loop = asyncio.new_event_loop()
    try:
        plan = partition_batch(batch, compiled, [])
        assert len(plan.sync_native) == 4
        assert len(plan.async_native) == 1
        results = dispatch_batch(plan, compiled, [], MockResponse, loop)
        assert results is not None
        assert results[0].status == 201
        assert decode_json(results[0].body, type=_OrderOutput) == _OrderOutput(
            order_id=42,
            sku="SKU-7",
            qty=3,
            tenant="ctrl:eu:s-99",
            source="mobile:sync-tx",
        )
        assert results[1].status == 200
        assert decode_json(results[1].body) == {"id": 77, "sku": "MP-1", "qty": 5, "trace": "tr-1"}
        assert results[2].status == 200
        assert decode_json(results[2].body) == {"id": 15, "audit": "async-audit"}
        assert results[3].status == 403
        assert results[4].status == 200
        assert decode_json(results[4].body) == {"admin": True}
        assert cleanup_log == [
            "tx_open",
            "tx_close",
            "audit_open:/orders/async-promoted/15",
            "audit_close:/orders/async-promoted/15",
        ]
    finally:
        loop.close()


def test_event_loop_pool_stop_drains_tasks_and_async_generators() -> None:
    """stop() delivers task cancellation, finalizes async generators, and closes loops after threads exit."""
    pool = EventLoopPool(size=2)
    started = threading.Event()
    cancelled = threading.Event()
    finalized = threading.Event()
    generators: list[AsyncIterator[int]] = []

    async def wait_forever() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def ticker() -> AsyncIterator[int]:
        try:
            yield 1
            yield 2
        finally:
            finalized.set()

    async def start_generator() -> None:
        gen = ticker()
        generators.append(gen)
        await anext(gen)

    pool.spawn(wait_forever(), worker_index=0)
    pool.schedule(start_generator(), worker_index=1).result(timeout=2.0)
    assert started.wait(timeout=2.0)
    pool.stop()
    assert cancelled.is_set()
    assert finalized.is_set()
    assert all(loop.is_closed() for loop in pool.loops)


def test_event_loop_pool_raises_when_shard_does_not_start(monkeypatch: pytest.MonkeyPatch) -> None:
    """A shard thread that never signals readiness makes the constructor raise."""
    monkeypatch.setattr("vertebra.server._loops._SHARD_START_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(EventLoopPool, "_run_loop_shard", staticmethod(lambda loop, ready: None))
    with pytest.raises(RuntimeError, match="did not start"):
        EventLoopPool(size=2)


def test_asgi_mount_runner_times_out_and_cancels_task() -> None:
    """A request past batch_timeout returns 504 and its asyncio task is cancelled."""
    pool = EventLoopPool(size=2)
    cancelled = threading.Event()

    async def never_responds(scope: Any, receive: Any, send: Any) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    try:
        runner = _AsgiMountRunner(never_responds, pool, batch_timeout=0.05)
        responses = runner([MockRequest(handler_id=99, request_id=7, path="/slow")])
        assert [(r.status, r.body, r.request_id) for r in responses] == [(504, b"Gateway Timeout", 7)]
        assert cancelled.wait(timeout=2.0)
    finally:
        pool.stop()


@pytest.mark.parametrize("raise_on_cancel", [False, True])
def test_asgi_mount_runner_batch_timeout_cancels_scheduled_task_without_warnings(
    raise_on_cancel: bool,
) -> None:
    """Timed-out _AsgiMountRunner batches cancel the scheduled asyncio.Task with zero unretrieved warnings."""
    pool = EventLoopPool(size=2)
    scheduled_tasks: list[asyncio.Task[Any]] = []
    loop_errors: list[dict[str, Any]] = []

    for shard_loop in pool.loops:
        shard_loop.set_exception_handler(lambda _loop, ctx: loop_errors.append(dict(ctx)))

    async def slow_asgi(scope: Any, receive: Any, send: Any) -> None:
        _ = (scope, receive, send)
        task = asyncio.current_task()
        assert task is not None
        scheduled_tasks.append(task)
        try:
            await asyncio.Event().wait()
        finally:
            if raise_on_cancel:
                raise RuntimeError("cleanup boom")

    try:
        runner = _AsgiMountRunner(slow_asgi, pool, batch_timeout=0.05)
        responses = runner([MockRequest(handler_id=101, request_id=19, path="/hang")])
        assert [(r.status, r.body, r.request_id) for r in responses] == [(504, b"Gateway Timeout", 19)]
        assert len(scheduled_tasks) == 1
        task = scheduled_tasks[0]
        assert task.done()
        if not raise_on_cancel:
            assert task.cancelled()
        gc.collect()
    finally:
        pool.stop()
    assert all(not task_set for task_set in pool._task_sets)
    assert loop_errors == []


async def _ok_asgi(scope: Any, receive: Any, send: Any) -> None:
    """Minimal ASGI app answering 200."""
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, 30.0), ("", 30.0), ("2.5", 2.5), ("0", None), ("none", None), ("NONE", None)],
    ids=["unset", "empty", "seconds", "zero-disables", "none-disables", "none-uppercase"],
)
def test_asgi_mount_runner_reads_batch_timeout_from_environment(
    monkeypatch: pytest.MonkeyPatch,
    raw: str | None,
    expected: float | None,
) -> None:
    """VERTEBRA_ASGI_BATCH_TIMEOUT sets the buffered mount timeout; 0 or none disables it; unset means 30 s."""
    if raw is None:
        monkeypatch.delenv("VERTEBRA_ASGI_BATCH_TIMEOUT", raising=False)
    else:
        monkeypatch.setenv("VERTEBRA_ASGI_BATCH_TIMEOUT", raw)
    pool = EventLoopPool(size=2)
    try:
        mounts = _instantiate_mount_runners([("/api", _ok_asgi, "asgi")], pool)
        runner = mounts[0][1]
        assert isinstance(runner, _AsgiMountRunner)
        assert runner.batch_timeout == expected
    finally:
        pool.stop()


@pytest.mark.parametrize("raw", ["abc", "-1"])
def test_asgi_mount_batch_timeout_rejects_invalid_values(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    """Non-numeric or negative VERTEBRA_ASGI_BATCH_TIMEOUT values are configuration errors."""
    monkeypatch.setenv("VERTEBRA_ASGI_BATCH_TIMEOUT", raw)
    pool = EventLoopPool(size=2)
    try:
        with pytest.raises(ImproperConfigurationError, match="VERTEBRA_ASGI_BATCH_TIMEOUT"):
            _instantiate_mount_runners([("/api", _ok_asgi, "asgi")], pool)
    finally:
        pool.stop()


def test_dispatch_batch_buffered_async_native_times_out_with_504_and_cancels_task() -> None:
    """Buffered dispatch_batch(async_mode=False) returns 504 and cancels timed-out async_native tasks."""
    pool = EventLoopPool(size=2)
    cancelled = threading.Event()

    async def slow_handler() -> bytes:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return b"never"

    handlers = {1: slow_handler}
    batch = [MockRequest(handler_id=1, request_id=19, path="/slow-native")]
    try:
        plan = partition_batch(batch, handlers, [])
        results = dispatch_batch(
            plan,
            handlers,
            [],
            MockResponse,
            pool,
            async_mode=False,
            batch_timeout=0.05,
        )
        assert results is not None
        assert [(r.status, r.body, r.request_id) for r in results] == [(504, b"Gateway Timeout", 19)]
        assert cancelled.wait(timeout=2.0)
    finally:
        pool.stop()


@pytest.mark.parametrize("failure_mode", ["startup_failed_event", "startup_exception", "startup_timeout", "rsgi_init"])
def test_worker_run_raises_worker_startup_error_on_lifespan_or_rsgi_failure(
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    """Worker.run raises WorkerStartupError on lifespan failure, exception, timeout, or RSGI init error."""
    monkeypatch.setenv("VERTEBRA_LIFESPAN_TIMEOUT", "0.05")
    monkeypatch.setattr("vertebra.server.worker.get_request_notify_fd", lambda: -1)
    monkeypatch.setattr("vertebra.server.worker.get_batch", lambda _timeout: [])

    async def failing_event_asgi(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "lifespan":
            await receive()
            await send({"type": "lifespan.startup.failed", "message": "db down"})

    async def raising_asgi(scope: dict[str, Any], receive: Any, _send: Any) -> None:
        if scope["type"] == "lifespan":
            await receive()
            msg = "lifespan exploded"
            raise RuntimeError(msg)

    async def timeout_asgi(scope: dict[str, Any], receive: Any, _send: Any) -> None:
        if scope["type"] == "lifespan":
            await receive()
            await asyncio.Event().wait()

    class FailingRsgiApp:
        def __rsgi_init__(self, _loop: Any) -> None:
            msg = "rsgi init exploded"
            raise RuntimeError(msg)

        async def __rsgi__(self, _scope: Any, _proto: Any) -> None:
            return None

    mount_map: dict[str, tuple[str, Any, str]] = {
        "startup_failed_event": ("/api", failing_event_asgi, "asgi"),
        "startup_exception": ("/api", raising_asgi, "asgi"),
        "startup_timeout": ("/api", timeout_asgi, "asgi"),
        "rsgi_init": ("/rsgi", FailingRsgiApp(), "rsgi"),
    }
    prefix, mount_app, proto = mount_map[failure_mode]
    expected_stage = "rsgi_init" if failure_mode == "rsgi_init" else "lifespan"

    w = Worker({}, mounts=[(prefix, mount_app, proto)], max_batches=1)
    with pytest.raises(WorkerStartupError) as exc_info:
        w.run()

    assert exc_info.value.prefix == prefix
    assert exc_info.value.phase == expected_stage


def test_worker_run_unwinds_prior_mounts_on_later_mount_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When mount 2 fails startup, mount 1 lifespan shutdown unwinds cleanly before WorkerStartupError."""
    monkeypatch.setattr("vertebra.server.worker.get_request_notify_fd", lambda: -1)
    monkeypatch.setattr("vertebra.server.worker.get_batch", lambda _timeout: [])
    events: list[str] = []

    async def first_ok_asgi(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "lifespan":
            return
        msg = await receive()
        if msg["type"] == "lifespan.startup":
            events.append("m1_startup")
            await send({"type": "lifespan.startup.complete"})
        msg2 = await receive()
        if msg2["type"] == "lifespan.shutdown":
            events.append("m1_shutdown")
            await send({"type": "lifespan.shutdown.complete"})

    async def second_fail_asgi(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "lifespan":
            await receive()
            events.append("m2_startup_failed")
            await send({"type": "lifespan.startup.failed", "message": "boom"})

    w = Worker({}, mounts=[("/m1", first_ok_asgi, "asgi"), ("/m2", second_fail_asgi, "asgi")], max_batches=1)
    with pytest.raises(WorkerStartupError) as exc_info:
        w.run()

    assert exc_info.value.prefix == "/m2"
    assert events == ["m1_startup", "m2_startup_failed", "m1_shutdown"]


def test_worker_run_unsupported_lifespan_mount_stays_non_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mounts that do not support the ASGI lifespan protocol do not trigger WorkerStartupError."""
    monkeypatch.setattr("vertebra.server.worker.get_request_notify_fd", lambda: -1)
    monkeypatch.setattr("vertebra.server.worker.get_batch", lambda _timeout: [object()])
    monkeypatch.setattr(Worker, "_process_batch", lambda *_args, **_kwargs: None)

    async def stateless_asgi(scope: dict[str, Any], _receive: Any, _send: Any) -> None:
        if scope["type"] == "lifespan":
            msg = "lifespan not supported"
            raise NotImplementedError(msg)

    w = Worker({}, mounts=[("/stateless", stateless_asgi, "asgi")], max_batches=1)
    w.run()


@pytest.mark.parametrize("workload", ["pure_async", "sync_to_thread", "wsgi_concurrent"])
def test_dispatch_batch_worker_executor_lazy_init_and_reuse(
    monkeypatch: pytest.MonkeyPatch,
    workload: str,
) -> None:
    """Worker executor initializes lazily only when needed, reuses one pool across batches, and joins cleanly."""
    shutdown_worker_executor(wait=True)
    created_kwargs: list[dict[str, Any]] = []
    shutdown_waits: list[bool] = []
    seen_none_during_shutdown: list[bool] = []

    class _TrackedExecutor(ThreadPoolExecutor):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            created_kwargs.append(dict(kwargs))
            super().__init__(*args, **kwargs)

        def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
            seen_none_during_shutdown.append(_dispatch._WORKER_EXECUTOR is None)
            shutdown_waits.append(wait)
            super().shutdown(wait=wait, cancel_futures=cancel_futures)

    monkeypatch.setattr(_dispatch, "ThreadPoolExecutor", _TrackedExecutor)
    pool = EventLoopPool(size=1)
    try:
        if workload == "pure_async":
            collector = _SubmitCollector()

            async def async_route() -> bytes:
                return b"async-ok"

            compiled = compile_handler_table({1: async_route})
            for req_id in (1, 2, 3):
                plan = partition_batch([MockRequest(handler_id=1, request_id=req_id)], compiled, [])
                dispatch_batch(
                    plan,
                    compiled,
                    [],
                    MockResponse,
                    pool,
                    async_mode=True,
                    submit=collector.append,
                )

            submitted = collector.wait_for(3)
            assert len(submitted) == 3
            assert created_kwargs == []
            assert shutdown_waits == []
        elif workload == "sync_to_thread":
            sync_collector = _SubmitCollector()
            app = App()

            @app.get("/work", sync_to_thread=True)
            def blocking_route() -> bytes:
                return b"sync-ok"

            compiled = compile_handler_table(app._handlers)
            handler_id = next(iter(compiled))
            for req_id in (10, 11, 12):
                plan = partition_batch(
                    [MockRequest(handler_id=handler_id, request_id=req_id, path="/work")], compiled, []
                )
                dispatch_batch(
                    plan,
                    compiled,
                    [],
                    MockResponse,
                    pool,
                    async_mode=True,
                    submit=sync_collector.append,
                )

            submitted_sync = sync_collector.wait_for(3)
            assert len(submitted_sync) == 3
            assert len(created_kwargs) == 1
            assert created_kwargs[0]["thread_name_prefix"] == "vertebra-worker"

            shutdown_worker_executor(wait=True)
            assert shutdown_waits == [True]
            assert seen_none_during_shutdown == [True]
            shutdown_worker_executor(wait=True)
            assert shutdown_waits == [True]
        else:

            def mock_wsgi(req: Any) -> MockResponse:
                return MockResponse(status=200, body=b"wsgi-ok", request_id=req.request_id)

            mounts = [("/legacy", mock_wsgi, "wsgi")]
            for offset in (0, 2):
                batch = [
                    MockRequest(handler_id=99, request_id=20 + offset, path="/legacy/a"),
                    MockRequest(handler_id=99, request_id=21 + offset, path="/legacy/b"),
                ]
                plan = partition_batch(batch, {}, mounts)
                results = dispatch_batch(plan, {}, mounts, MockResponse, pool.primary_loop)
                assert results is not None
                assert [r.body for r in results] == [b"wsgi-ok", b"wsgi-ok"]

            assert len(created_kwargs) == 1
            assert created_kwargs[0]["thread_name_prefix"] == "vertebra-worker"
            shutdown_worker_executor(wait=True)
            assert shutdown_waits == [True]
            assert seen_none_during_shutdown == [True]
    finally:
        shutdown_worker_executor(wait=True)
        pool.stop()


@pytest.mark.parametrize("exit_mode", ["normal_loop_exit", "exception_exit"])
def test_worker_run_joins_worker_and_dispatch_executors_on_exit(
    monkeypatch: pytest.MonkeyPatch,
    exit_mode: str,
) -> None:
    """Worker.run calls shutdown(wait=True) for both its own executor and cached dispatch executor on exit."""
    shutdown_worker_executor(wait=True)
    worker_shutdowns: list[bool] = []
    dispatch_shutdowns: list[bool] = []

    class _WorkerTrackedExecutor(ThreadPoolExecutor):
        def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
            worker_shutdowns.append(wait)
            super().shutdown(wait=wait, cancel_futures=cancel_futures)

    class _DispatchTrackedExecutor(ThreadPoolExecutor):
        def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
            dispatch_shutdowns.append(wait)
            super().shutdown(wait=wait, cancel_futures=cancel_futures)

    app = App()

    @app.get("/block", sync_to_thread=True)
    def blocking_handler() -> bytes:
        return b"ok"

    monkeypatch.setattr(worker, "ThreadPoolExecutor", _WorkerTrackedExecutor)
    monkeypatch.setattr(_dispatch, "ThreadPoolExecutor", _DispatchTrackedExecutor)
    monkeypatch.setattr(worker, "get_request_notify_fd", lambda: -1)
    monkeypatch.setattr(worker, "get_batch", lambda _timeout: [object()])

    def fake_process_batch(*_args: Any, **_kwargs: Any) -> None:
        get_worker_executor()
        if exit_mode == "exception_exit":
            msg = "batch exploded"
            raise RuntimeError(msg)

    monkeypatch.setattr(Worker, "_process_batch", fake_process_batch)

    w = Worker(app._handlers, max_batches=1, batch_timeout_ms=10)
    try:
        if exit_mode == "exception_exit":
            with pytest.raises(RuntimeError, match="batch exploded"):
                w.run()
        else:
            w.run()
        assert worker_shutdowns == [True]
        assert dispatch_shutdowns == [True]
        assert _dispatch._WORKER_EXECUTOR is None
    finally:
        shutdown_worker_executor(wait=True)


def test_zpages_handler_id_distinct_from_mount_mask() -> None:
    """Verify _ZPAGES_HANDLER_ID matches Rust ZPAGES_HANDLER_ID and never collides with mount masks."""
    assert _ZPAGES_HANDLER_ID == 0xFFFE_0000_0000_0001
    assert (_ZPAGES_HANDLER_ID & _MOUNT_HANDLER_MASK) != _MOUNT_HANDLER_MASK


def test_worker_process_batch_intercepts_zpages_before_all_sync_native(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Worker._process_batch intercepts _ZPAGES_HANDLER_ID ahead of all_sync_native handlers."""
    called_handlers: list[int] = []

    def sync_handler() -> bytes:
        called_handlers.append(1)
        return b"ok"

    compiled = compile_handler_table({1: sync_handler})
    raw_submissions: list[tuple[int, int, bytes, bytes | None]] = []

    def fake_submit_response_raw(
        request_id: int,
        status: int,
        body: bytes,
        content_type: bytes | None = None,
    ) -> None:
        raw_submissions.append((request_id, status, body, content_type))

    monkeypatch.setattr(worker, "submit_response_raw", fake_submit_response_raw)
    zpages = ZPagesRegistry("sync-native-zpages")
    w = Worker({1: sync_handler}, zpages=zpages)
    pool = EventLoopPool(size=1, inline_single_loop=True)
    try:
        w._process_batch(
            [MockRequest(handler_id=_ZPAGES_HANDLER_ID, request_id=41, path="/statusz")],
            compiled_handlers=compiled,
            mount_runners=[],
            mount_lookup={},
            all_sync_native=True,
            loop_pool=pool,
        )
    finally:
        pool.stop()

    assert called_handlers == []
    assert len(raw_submissions) == 1
    req_id, status, body, content_type = raw_submissions[0]
    assert req_id == 41
    assert status == 200
    assert content_type == _JSON_CONTENT_TYPE_COMPACT
    payload = decode_json(body)
    assert payload["service"] == "sync-native-zpages"


@pytest.mark.parametrize("fastpath", ["asgi", "rsgi", "wsgi"])
def test_worker_process_batch_intercepts_zpages_before_single_root_fastpaths(
    monkeypatch: pytest.MonkeyPatch,
    fastpath: str,
) -> None:
    """Verify Worker._process_batch intercepts _ZPAGES_HANDLER_ID ahead of single_root_{asgi,rsgi,wsgi}."""

    class _FakeRequestBatch(list[Any]):
        pass

    driver_calls: list[str] = []

    def fail_driver(name: str) -> Any:
        def _driver(*_args: Any, **_kwargs: Any) -> None:
            driver_calls.append(name)

        return _driver

    raw_submissions: list[tuple[int, int, bytes, bytes | None]] = []

    def fake_submit_response_raw(
        request_id: int,
        status: int,
        body: bytes,
        content_type: bytes | None = None,
    ) -> None:
        raw_submissions.append((request_id, status, body, content_type))

    monkeypatch.setattr(worker, "RequestBatch", _FakeRequestBatch)
    monkeypatch.setattr(worker, "drive_asgi_batch", fail_driver("asgi"))
    monkeypatch.setattr(worker, "drive_rsgi_batch", fail_driver("rsgi"))
    monkeypatch.setattr(worker, "drive_wsgi_batch", fail_driver("wsgi"))
    monkeypatch.setattr(worker, "submit_response_raw", fake_submit_response_raw)

    zpages = ZPagesRegistry(f"single-root-{fastpath}")
    w = Worker({}, zpages=zpages)
    pool = EventLoopPool(size=1, inline_single_loop=True)
    sentinel_adapter: Any = object()
    try:
        w._process_batch(
            _FakeRequestBatch([MockRequest(handler_id=_ZPAGES_HANDLER_ID, request_id=42, path="/statusz")]),
            compiled_handlers={},
            mount_runners=[],
            mount_lookup={},
            all_sync_native=False,
            loop_pool=pool,
            single_root_asgi=sentinel_adapter if fastpath == "asgi" else None,
            single_root_rsgi=sentinel_adapter if fastpath == "rsgi" else None,
            single_root_wsgi=sentinel_adapter if fastpath == "wsgi" else None,
        )
    finally:
        pool.stop()

    assert driver_calls == []
    assert len(raw_submissions) == 1
    req_id, status, body, content_type = raw_submissions[0]
    assert req_id == 42
    assert status == 200
    assert content_type == _JSON_CONTENT_TYPE_COMPACT
    assert decode_json(body)["service"] == f"single-root-{fastpath}"


def test_worker_handle_zpage_taskz_background_shard_pool_uses_async(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify /taskz on a multi-shard EventLoopPool runs asynchronously and excludes the probe task."""
    submitted = threading.Event()
    raw_submissions: list[tuple[int, int, bytes, bytes | None]] = []

    def fake_submit_response_raw(
        request_id: int,
        status: int,
        body: bytes,
        content_type: bytes | None = None,
    ) -> None:
        raw_submissions.append((request_id, status, body, content_type))
        submitted.set()

    monkeypatch.setattr(worker, "submit_response_raw", fake_submit_response_raw)
    zpages = ZPagesRegistry("sharded-taskz")
    w = Worker({}, zpages=zpages)
    pool = EventLoopPool(size=2)
    zpages.register_loop_pool("worker", pool)
    started = threading.Event()
    release = threading.Event()

    async def active_background_job() -> None:
        started.set()
        while not release.is_set():
            await asyncio.sleep(0.01)

    try:
        pool.spawn(active_background_job(), worker_index=0, name="shard-active-job")
        assert started.wait(timeout=2.0)

        w._handle_zpage_request(
            MockRequest(handler_id=_ZPAGES_HANDLER_ID, request_id=55, path="/taskz"),
            pool,
        )
        assert submitted.wait(timeout=2.0)
    finally:
        release.set()
        pool.stop()

    assert len(raw_submissions) == 1
    req_id, status, body, content_type = raw_submissions[0]
    assert req_id == 55
    assert status == 200
    assert content_type == _JSON_CONTENT_TYPE_COMPACT
    payload = decode_json(body)
    task_names = {entry["task_name"] for entry in payload["tasks"]}
    assert "shard-active-job" in task_names
    assert _TASKZ_PROBE_TASK_NAME not in task_names


def test_worker_handle_zpage_taskz_inline_single_loop_uses_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify /taskz on an inline single loop with no remote loops submits synchronously."""
    raw_submissions: list[tuple[int, int, bytes, bytes | None]] = []

    def fake_submit_response_raw(
        request_id: int,
        status: int,
        body: bytes,
        content_type: bytes | None = None,
    ) -> None:
        raw_submissions.append((request_id, status, body, content_type))

    monkeypatch.setattr(worker, "submit_response_raw", fake_submit_response_raw)
    zpages = ZPagesRegistry("inline-taskz")
    w = Worker({}, zpages=zpages)
    pool = EventLoopPool(size=1, inline_single_loop=True)
    zpages.register_loop_pool("worker", pool)
    try:
        w._handle_zpage_request(
            MockRequest(handler_id=_ZPAGES_HANDLER_ID, request_id=56, path="/taskz"),
            pool,
        )
    finally:
        pool.stop()

    assert len(raw_submissions) == 1
    req_id, status, body, content_type = raw_submissions[0]
    assert req_id == 56
    assert status == 200
    assert content_type == _JSON_CONTENT_TYPE_COMPACT
    payload = decode_json(body)
    assert "worker.shard.0" in payload["loops"]


def test_worker_custom_loop_pool_name_exports_loop_telemetry_and_lazy_active_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify loop_pool_name namespaces /taskz and /varz loop_telemetry with zero set_var calls in _process_batch."""
    app = App()

    @app.get("/ping")
    async def ping() -> bytes:
        return b"pong"

    recorder = EventLoopRecorder(emit_otel=False)
    zpages = ZPagesRegistry("named-worker")
    set_var_calls_during_batch: list[tuple[str, Any]] = []
    in_batch = False
    orig_set_var = ZPagesRegistry.set_var

    def tracked_set_var(self: ZPagesRegistry, name: str, value: Any) -> None:
        if in_batch:
            set_var_calls_during_batch.append((name, value))
        orig_set_var(self, name, value)

    monkeypatch.setattr(ZPagesRegistry, "set_var", tracked_set_var)
    monkeypatch.setattr(worker, "get_request_notify_fd", lambda: -1)
    monkeypatch.setattr(
        worker,
        "get_batch",
        lambda _timeout: [MockRequest(handler_id=1, request_id=10, path="/ping")],
    )
    monkeypatch.setattr(worker, "submit_response_raw", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(worker, "submit_response", lambda *_args, **_kwargs: None)

    captured_taskz: dict[str, Any] = {}
    captured_varz: dict[str, Any] = {}
    orig_process_batch = Worker._process_batch

    def wrapped_process_batch(self: Worker, batch: Any, **kwargs: Any) -> None:
        nonlocal in_batch
        in_batch = True
        try:
            orig_process_batch(self, batch, **kwargs)
        finally:
            in_batch = False
        captured_taskz.update(zpages.get_tasks_payload())
        captured_varz.update(zpages.get_vars_payload())

    monkeypatch.setattr(Worker, "_process_batch", wrapped_process_batch)

    w = Worker(
        app._handlers,
        zpages=zpages,
        loop_recorder=recorder,
        loop_pool_name="worker-2",
        max_batches=1,
        batch_timeout_ms=10,
    )
    w.run()

    assert set_var_calls_during_batch == []
    assert "worker-2.shard.0" in captured_taskz["loops"]
    assert "loop_telemetry" in captured_varz
    assert "worker-2" in captured_varz["loop_telemetry"]
    assert captured_varz["active_requests"] == 0
    assert captured_varz["worker_state"] == "ready"
    assert zpages.get_vars_payload()["worker_state"] == "stopped"


def test_worker_handle_zpage_broken_broker_returns_500_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a failing custom broker provider returns 500 JSON instead of crashing the worker."""
    raw_submissions: list[tuple[int, int, bytes, bytes | None]] = []

    def fake_submit_response_raw(
        request_id: int,
        status: int,
        body: bytes,
        content_type: bytes | None = None,
    ) -> None:
        raw_submissions.append((request_id, status, body, content_type))

    def broken_broker() -> dict[str, object]:
        msg = "broker snapshot failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(worker, "submit_response_raw", fake_submit_response_raw)
    zpages = ZPagesRegistry("broken-broker")
    zpages.register_broker("bad", broken_broker)
    w = Worker({}, zpages=zpages)
    pool = EventLoopPool(size=1, inline_single_loop=True)
    try:
        w._handle_zpage_request(
            MockRequest(handler_id=_ZPAGES_HANDLER_ID, request_id=77, path="/brokerz"),
            pool,
        )
    finally:
        pool.stop()

    assert raw_submissions == [
        (77, 500, b'{"error":"zpages_internal_error"}', _JSON_CONTENT_TYPE_COMPACT),
    ]
